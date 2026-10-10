//! 3.1 dual writer, rebuilding a node whose disk was replaced (CONTRACT D-16,
//! S7), on local PostgreSQL. Node A's database is copied physically, with
//! `CREATE DATABASE ... TEMPLATE` standing in for the pair's
//! `pg_basebackup`, and `node-identity repersonalise` makes the copy node
//! B's. The table inventory it resets by classifies every table.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    ledger::{
        table_inventory::{self, LocalPart, Reset, TableClass, TABLE_INVENTORY},
        IdentityCheck, Ledger, LineageEvidence, PeerSyncCursor, RepersonaliseGuard, TableRows,
    },
    node_identity::NodeIndex,
    peer_sync::COPIED_TABLES,
};
use qbit_prism_test_gate as gate;
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool};

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;

use ledger_database::FixtureDatabase;

fn share(id: u64) -> qbit_prism::AcceptedShare {
    qbit_prism::AcceptedShare {
        share_seq: 0,
        share_id: format!("worker:{id:064x}"),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

/// Appends `ids` through the ledger and returns each row's `share_seq` and
/// `origin_node`, and its header mapping's `origin_node`.
async fn append(ledger: &Ledger, ids: std::ops::Range<u64>) -> Result<Vec<(i64, i16, i16)>> {
    let mut rows = Vec::new();
    for id in ids {
        ensure!(
            ledger.append(share(id), None).await?.inserted,
            "share {id} was not appended"
        );
        rows.push(
            sqlx::query_as(
                "SELECT s.share_seq,s.origin_node,h.origin_node FROM qbit_share_ledger s \
                 JOIN qbit_prism_share_hashes h ON h.share_id=s.share_id WHERE s.share_id=$1",
            )
            .bind(format!("worker:{id:064x}"))
            .fetch_one(&ledger.pool)
            .await?,
        );
    }
    Ok(rows)
}

fn hex(byte: &str) -> String {
    byte.repeat(32)
}

const BLOCK_A: &str = "a1";
const BLOCK_B: &str = "b1";
const CANDIDATE: &str = "c1";

/// A fanout artifact of node A's block, with an optional claim.
async fn fanout(
    pool: &PgPool,
    chunk: i32,
    next_attempt: &str,
    claim: Option<(&str, i64)>,
) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,\
         manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,\
         commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,\
         parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,covenant_output_value_sats,\
         fanout_output_sum_sats,settlement_status,next_broadcast_attempt_at,\
         broadcast_attempt_count,claim_token,claim_instance_id,claim_expires_at,claim_renewals,\
         claim_lease_seconds) VALUES ($1,$2,$3,'{{}}','{{}}',$3,$3,$3,'00',$4,3,$3,0,'00','00',0,0,\
         'broadcastable',{next_attempt},2,$5,$6,$7,$8,$9)"
    ))
    .bind(fanout_txid(chunk))
    .bind(hex(BLOCK_A))
    .bind(hex("5a"))
    .bind(chunk)
    .bind(claim.map(|(token, _)| token))
    .bind(claim.map(|_| "node-a"))
    .bind(claim.map(|_| chrono::Utc::now() + chrono::Duration::minutes(1)))
    .bind(claim.map_or(0, |(_, renewals)| renewals))
    .bind(claim.map(|_| 60i32))
    .execute(pool)
    .await?;
    Ok(())
}

fn fanout_txid(chunk: i32) -> String {
    format!("{chunk:064x}")
}

/// Node A at work: personalised, appending its own shares, holding rows it
/// pulled from node B (origin 1, B's odd keys), and the local state of its
/// frontends, sessions, candidates, claims and wallet, as a physical copy
/// would carry them.
async fn node_a_at_work(a: &Ledger) -> Result<()> {
    let pool = &a.pool;
    a.set_node_identity(NodeIndex::A, "test").await?;
    ensure!(append(a, 1..4).await? == [(4, 0, 0), (6, 0, 0), (8, 0, 0)]);
    // Shares A pulled from B, then A's sequence raised above them (D-14).
    for (seq, id) in [(9i64, 1u64), (11, 2), (13, 3)] {
        let share_id = format!("b-worker:{id:064x}");
        sqlx::query(
            "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,\
             p2mr_program,share_difficulty,network_difficulty,template_height,job_id,\
             job_issued_at,ntime,writer_id,writer_epoch,origin_node) VALUES($1,$2,'miner-b',\
             'miner-b',decode(repeat('22',32),'hex'),1,100,100,'job-b',to_timestamp(1),\
             1800000000,'node-b',0,1)",
        )
        .bind(seq)
        .bind(&share_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO qbit_prism_share_hashes(header_hash,share_id,origin_node) VALUES($1,$2,1)",
        )
        .bind(format!("{:064x}", 0xb000 + id))
        .bind(&share_id)
        .execute(pool)
        .await?;
    }
    sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',14,true)")
        .execute(pool)
        .await?;
    ensure!(append(a, 4..6).await? == [(16, 0, 0), (18, 0, 0)]);
    // How far A has pulled B's streams: past the rows it holds.
    sqlx::query(
        "INSERT INTO qbit_prism_peer_sync_cursors(stream,peer_node,scanned_through,\
         ingested_through) VALUES('shares',1,41,13),('blocks',1,1500,1000),\
         ('prepared',1,2500,2000)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_prism_peer_sync_conflicts(source_table,row_key,origin_node,detail) \
         VALUES('qbit_share_ledger','b-worker:x',1,'test')",
    )
    .execute(pool)
    .await?;
    // A block of each origin, with its payout and carry rows, confirmed; A's
    // also mature.
    for (block, height, origin) in [(BLOCK_A, 100i64, None), (BLOCK_B, 101, Some(1i16))] {
        sqlx::query(
            "INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,\
             payout_manifest_sha256,origin_node,sync_seq) VALUES($1,$2,$3,$3,$3,\
             COALESCE($4,0),CASE WHEN $4 IS NULL THEN qbit_prism_next_sync_seq() ELSE 1000 END)",
        )
        .bind(hex(block))
        .bind(height)
        .bind(hex("ee"))
        .bind(origin)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,\
         p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,100,\
         'miner-a','miner-a',decode(repeat('11',32),'hex'),0,1000,'accrued')",
    )
    .bind(hex(BLOCK_A))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_pool_payout_entries(payout_entry_seq,block_hash,block_height,miner_id,\
         payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action,\
         origin_node) VALUES(101,$1,101,'miner-b','miner-b',decode(repeat('22',32),'hex'),0,500,\
         'accrued',1)",
    )
    .bind(hex(BLOCK_B))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,\
         p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,\
         onchain_amount_sats,carry_forward_balance_sats,action) VALUES(100,$1,'miner-a',\
         'miner-a',decode(repeat('11',32),'hex'),1000,0,1000,0,1000,'accrued')",
    )
    .bind(hex(BLOCK_A))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_payout_carry_forward(carry_forward_seq,block_height,block_hash,\
         miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,\
         candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action,\
         origin_node) VALUES(201,101,$1,'miner-b','miner-b',decode(repeat('22',32),'hex'),500,0,\
         500,0,500,'accrued',1)",
    )
    .bind(hex(BLOCK_B))
    .execute(pool)
    .await?;
    sqlx::query("UPDATE qbit_pool_blocks SET chain_state='confirmed'")
        .execute(pool)
        .await?;
    sqlx::query(
        "UPDATE qbit_pool_blocks SET maturity_state='mature',matured_at=clock_timestamp() \
         WHERE block_hash=$1",
    )
    .bind(hex(BLOCK_A))
    .execute(pool)
    .await?;
    // A's block's fanouts: one claimed and scheduled later, one claimed and
    // held, one unclaimed; a broadcast attempt, and CPFP funding from A's
    // wallet.
    sqlx::query(
        "INSERT INTO qbit_ctv_fanout_sets(block_hash,manifest_set_json,manifest_set,\
         manifest_set_sha256,settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,\
         fanout_count,fanout_output_sum_sats,covenant_output_value_sats) VALUES($1,'{}','{}',$2,\
         'ctv_fanout',$2,'00',3,0,0)",
    )
    .bind(hex(BLOCK_A))
    .bind(hex("5a"))
    .execute(pool)
    .await?;
    fanout(
        pool,
        0,
        "clock_timestamp()+interval '1 hour'",
        Some(("claim-0", 3)),
    )
    .await?;
    fanout(pool, 1, "'infinity'", Some(("claim-1", 0))).await?;
    fanout(pool, 2, "clock_timestamp()+interval '1 hour'", None).await?;
    sqlx::query(
        "INSERT INTO qbit_ctv_fanout_broadcast_attempts(fanout_txid,attempt_status) \
         VALUES($1,'submitted')",
    )
    .bind(fanout_txid(0))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_prism_cpfp_packages(fanout_txid,funding_txid,funding_vout,\
         funding_value_sats,wallet_name) VALUES($1,$2,0,1000,'wallet-a')",
    )
    .bind(fanout_txid(0))
    .bind(hex("fa"))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_prism_cpfp_retired_funding(funding_txid,funding_vout,fanout_txid,\
         funding_value_sats,wallet_name,retirement_reason) VALUES($1,1,$2,1000,'wallet-a','test')",
    )
    .bind(hex("fa"))
    .bind(fanout_txid(0))
    .execute(pool)
    .await?;
    // A's found-block candidate and its deferred solving share.
    sqlx::query(
        "INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256) \
         VALUES($1,'{}',$2)",
    )
    .bind(hex(CANDIDATE))
    .bind(hex("cd"))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_prism_deferred_shares(block_hash,share,share_sha256) VALUES($1,'{}',$2)",
    )
    .bind(hex(CANDIDATE))
    .bind(hex("cd"))
    .execute(pool)
    .await?;
    // A session's reservation, a stopped frontend beside A's running one
    // (Ledger::connect registered "node-a"), an issued job, and prepared
    // records of both origins.
    sqlx::query(
        "INSERT INTO qbit_prism_session_reservations(extranonce1,instance_id,owner_token,\
         reservation_token) VALUES(7,'node-a','owner-a','reservation-a')",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_prism_instances(instance_id,status) VALUES('node-a-old','{\"state\":\"stopped\"}')",
    )
    .execute(pool)
    .await?;
    a.save_job(
        "node-a-issued-1",
        &json!({"extranonce1": "00000007"}),
        0,
        &hex("ee"),
        600,
    )
    .await?;
    sqlx::query(
        "INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,\
         expires_at,origin_node,sync_seq) VALUES('prepared:node-a:1','node-a',$1,0,'{}',\
         clock_timestamp()+interval '1 hour',0,qbit_prism_next_sync_seq()),\
         ('prepared:node-b:1','node-b',$1,0,'{}',clock_timestamp()+interval '1 hour',1,2000)",
    )
    .bind(hex("ee"))
    .execute(pool)
    .await?;
    // Derived state and history the copy keeps.
    sqlx::query(
        "INSERT INTO qbit_prism_payout_divergences(block_hash,block_height,offer_decision,\
         offer_decided_at,overpay_ceiling_sats,offer_observed_payout_revision) \
         VALUES($1,100,'disabled',clock_timestamp(),0,0)",
    )
    .bind(hex(BLOCK_A))
    .execute(pool)
    .await?;
    sqlx::raw_sql(
        "INSERT INTO qbit_hashrate_rollup_progress(last_share_seq) VALUES(18); \
         INSERT INTO qbit_hashrate_rollup_pool VALUES(300,0,5,5); \
         INSERT INTO qbit_worker_difficulty(listener,worker_username,difficulty,evidence_at) \
         VALUES('main','miner.worker',8,clock_timestamp()); \
         INSERT INTO qbit_prism_node_roles(origin_node,epoch,carry_owner,action,recorded_by) \
         VALUES(0,0,true,'seed','node-a'),(1,1,false,'seed','node-b'); \
         UPDATE qbit_prism_submission_hold SET reason='held for the test',\
         set_at=clock_timestamp(),set_by='test'",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// A table's rows (or `columns` of those matching `filter`), counted and
/// digested in a stable order.
async fn digest(pool: &PgPool, table: &str, columns: &str, filter: &str) -> Result<(i64, String)> {
    Ok(sqlx::query_as(&format!(
        "SELECT count(*),md5(COALESCE(string_agg(r::text,E'\\n' ORDER BY r::text),'')) \
         FROM (SELECT {columns} FROM {table} WHERE {filter}) r"
    ))
    .fetch_one(pool)
    .await?)
}

/// `table`'s columns other than `excluded`, as a select list.
async fn columns_but(pool: &PgPool, table: &str, excluded: &[&str]) -> Result<String> {
    Ok(sqlx::query_scalar(
        "SELECT string_agg(quote_ident(attname),',' ORDER BY attnum) FROM pg_attribute \
         WHERE attrelid=$1::regclass AND attnum>0 AND NOT attisdropped AND attname<>ALL($2)",
    )
    .bind(table)
    .bind(excluded)
    .fetch_one(pool)
    .await?)
}

async fn count(pool: &PgPool, table: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(pool)
        .await?)
}

async fn exists(pool: &PgPool, table: &str) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(table)
        .fetch_one(pool)
        .await?)
}

/// A sequence's step, bounds and whether it cycles.
async fn sequence(pool: &PgPool, name: &str) -> Result<(i64, i64, i64, bool)> {
    Ok(sqlx::query_as(
        "SELECT seqincrement,seqmin,seqmax,seqcycle FROM pg_sequence WHERE seqrelid=$1::regclass",
    )
    .bind(name)
    .fetch_one(pool)
    .await?)
}

async fn next_value(pool: &PgPool, name: &str) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT nextval($1::regclass)")
        .bind(name)
        .fetch_one(pool)
        .await?)
}

fn rows(table: &str, rows: u64) -> TableRows {
    TableRows {
        table: table.into(),
        rows,
    }
}

async fn error_of<T>(attempt: impl std::future::Future<Output = Result<T>>) -> String {
    attempt
        .await
        .err()
        .map(|error| format!("{error:#}"))
        .unwrap_or_default()
}

/// The copy of node A's database, re-personalised as node B: checked
/// against A's own database, which the rebuild never touches.
async fn rebuild_from(a: &PgPool, copy: &FixtureDatabase) -> Result<()> {
    let b = Ledger::connect_operator(&copy.url, false).await?;
    let result = async {
        let pool = &b.pool;
        // The copy carries A's running frontend's row, its heartbeat as fresh
        // as the moment the copy stopped following A: refused, as a frontend
        // running on this database would be.
        sqlx::query("UPDATE qbit_prism_instances SET heartbeat_at=clock_timestamp() WHERE instance_id='node-a'")
            .execute(pool)
            .await?;
        let error = error_of(b.repersonalise_node_identity(NodeIndex::B, "test", &RepersonaliseGuard::default())).await;
        ensure!(
            error.contains("refused, nothing was changed")
                && error.contains("a frontend may be running on this database: node-a (starting")
                && !error.contains("node-a-old"),
            "{error}"
        );
        ensure!(b.recorded_node_identity().await?.map(|r| r.node) == Some(NodeIndex::A));
        // A minute later by the copy's clock, the row is only A's history.
        sqlx::query("UPDATE qbit_prism_instances SET heartbeat_at=clock_timestamp()-interval '61 seconds' WHERE instance_id='node-a'")
            .execute(pool)
            .await?;
        let lineage_a: (i64, i64) = sqlx::query_as(
            "SELECT share_seq_floor,sync_seq_floor FROM qbit_prism_node_lineage",
        )
        .fetch_one(a)
        .await?;
        ensure!(lineage_a == (2, 0), "node A's floors: {lineage_a:?}");

        // A physical copy promoted for B runs on a new timeline: A's last
        // verification, carried in the copy, names A's own.
        let LineageEvidence {
            system_identifier,
            timeline,
        } = b.lineage_evidence().await?;
        sqlx::query(
            "UPDATE qbit_prism_node_lineage SET verified_system_identifier=$1,verified_timeline=$2,\
             verified_at=clock_timestamp()",
        )
        .bind(system_identifier)
        .bind(timeline - 1)
        .execute(pool)
        .await?;
        let done = b.repersonalise_node_identity(NodeIndex::B, "test-rebuild", &RepersonaliseGuard::default()).await?;
        ensure!(done.copied_identity.node == NodeIndex::A, "{done:?}");
        ensure!(
            done.identity.node == NodeIndex::B && done.identity.recorded_by == "test-rebuild",
            "{done:?}"
        );
        ensure!(b.recorded_node_identity().await? == Some(done.identity.clone()));
        ensure!(
            b.check_node_identity(NodeIndex::B).await? == IdentityCheck::Ready(done.identity.clone())
        );
        // The peer's in-flight work, deleted; every row reported.
        ensure!(
            done.cleared
                == [
                    rows("qbit_prism_jobs", 1),
                    rows("qbit_prism_deferred_shares", 1),
                    rows("qbit_block_candidate_outbox", 1),
                    rows("qbit_prism_session_reservations", 1),
                    rows("qbit_prism_instances", 2),
                    rows("qbit_prism_cpfp_packages", 1),
                    rows("qbit_prism_cpfp_retired_funding", 1),
                    rows("qbit_prism_peer_sync_conflicts", 1),
                ],
            "{:?}",
            done.cleared
        );
        ensure!(done.reset == [rows("qbit_ctv_fanout_artifacts", 2)], "{:?}", done.reset);
        // B's keys: above every key held and every position A reached in B's
        // old streams (shares 41; blocks and prepared 2500).
        ensure!(
            (done.share_seq_floor, done.sync_seq_floor) == (41, 2500),
            "{done:?}"
        );
        let lineage = b.node_lineage().await?.context("the lineage is recorded")?;
        ensure!(
            (lineage.share_seq_floor, lineage.sync_seq_floor) == (41, 2500)
                && lineage.verified.is_none(),
            "{lineage:?}"
        );
        // B's first pull of each of A's streams: every A share up to A's
        // newest (18) is in the copy; A's blocks and prepared records are
        // read again from A's floor.
        let cursors: Vec<(String, i16, i64, Option<i64>)> = sqlx::query_as(
            "SELECT stream,peer_node,scanned_through,ingested_through \
             FROM qbit_prism_peer_sync_cursors ORDER BY stream",
        )
        .fetch_all(pool)
        .await?;
        ensure!(
            cursors
                == [
                    ("blocks".into(), 0, 0, None),
                    ("prepared".into(), 0, 0, None),
                    ("shares".into(), 0, 18, Some(18)),
                ],
            "{cursors:?}"
        );
        let cursor = |stream: &str, scanned_through, ingested_through| PeerSyncCursor {
            stream: stream.into(),
            peer_node: NodeIndex::A,
            scanned_through,
            ingested_through,
        };
        ensure!(
            done.cursors
                == [
                    cursor("shares", 18, Some(18)),
                    cursor("blocks", 0, None),
                    cursor("prepared", 0, None),
                ],
            "{:?}",
            done.cursors
        );
        let mark: Option<i64> = sqlx::query_scalar("SELECT qbit_prism_peer_share_mark()")
            .fetch_one(pool)
            .await?;
        ensure!(mark == Some(18), "the safe peer mark is {mark:?}");

        // Every table, as the inventory decided: cleared tables are empty
        // (and were not in A), the rewritten ones are checked above, and every
        // other row and column is A's, byte for byte: the copied rows of both
        // origins, the carry-owner journal, the balance summary, the cluster
        // singleton, the divergences, rollups and journals.
        for entry in TABLE_INVENTORY {
            if entry.optional && !exists(a, entry.table).await? {
                continue;
            }
            let (columns, filter) = match entry.class {
                TableClass::Local(Reset::Rewrite) => continue,
                TableClass::Local(Reset::Clear) => {
                    ensure!(
                        count(pool, entry.table).await? == 0 && count(a, entry.table).await? > 0,
                        "{} is cleared",
                        entry.table
                    );
                    continue;
                }
                TableClass::Local(Reset::Keep) | TableClass::PairWide => {
                    ("*".to_string(), "true".to_string())
                }
                TableClass::Copied(parts) => {
                    let mut excluded = Vec::new();
                    let mut filter = "true".to_string();
                    for part in parts {
                        match part {
                            LocalPart::Rows { predicate } => filter = format!("NOT ({predicate})"),
                            LocalPart::Reset { columns, .. } => excluded.extend(*columns),
                            LocalPart::Kept(_) => {}
                        }
                    }
                    (columns_but(a, entry.table, &excluded).await?, filter)
                }
            };
            let theirs = digest(a, entry.table, &columns, &filter).await?;
            let ours = digest(pool, entry.table, &columns, &filter).await?;
            ensure!(theirs == ours, "{}: A {theirs:?}, the copy {ours:?}", entry.table);
        }
        // The issued job went; the prepared records of both origins stayed.
        let jobs: Vec<String> = sqlx::query_scalar("SELECT job_id FROM qbit_prism_jobs ORDER BY 1")
            .fetch_all(pool)
            .await?;
        ensure!(jobs == ["prepared:node-a:1", "prepared:node-b:1"], "{jobs:?}");
        // The fanout claims were handed back: the later one due at once, the
        // held one still held, the unclaimed one untouched.
        let fanouts: Vec<(String, bool, bool, bool)> = sqlx::query_as(
            "SELECT fanout_txid,num_nulls(claim_token,claim_instance_id,claim_expires_at,\
             claim_lease_seconds)=4 AND claim_renewals=0,\
             next_broadcast_attempt_at='infinity'::timestamptz,\
             next_broadcast_attempt_at<=clock_timestamp() \
             FROM qbit_ctv_fanout_artifacts ORDER BY chunk_index",
        )
        .fetch_all(pool)
        .await?;
        ensure!(
            fanouts
                == [
                    (fanout_txid(0), true, false, true),
                    (fanout_txid(1), true, true, false),
                    (fanout_txid(2), true, false, false),
                ],
            "{fanouts:?}"
        );
        let unclaimed = "SELECT next_broadcast_attempt_at::text||updated_at::text FROM \
                         qbit_ctv_fanout_artifacts WHERE chunk_index=2";
        let theirs: String = sqlx::query_scalar(unclaimed).fetch_one(a).await?;
        let ours: String = sqlx::query_scalar(unclaimed).fetch_one(pool).await?;
        ensure!(theirs == ours, "the unclaimed fanout was touched");
        // The kept balance summary still equals its recomputation.
        let (drift, balances): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM qbit_carry_forward_current_drift()),\
             (SELECT count(*) FROM qbit_current_carry_forward_balances())",
        )
        .fetch_one(pool)
        .await?;
        ensure!((drift, balances) == (0, 2), "{drift} {balances}");

        // B's keys and sessions from here on.
        for table in COPIED_TABLES {
            let default: Option<String> = sqlx::query_scalar(
                "SELECT pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a JOIN pg_attrdef d \
                 ON d.adrelid=a.attrelid AND d.adnum=a.attnum \
                 WHERE a.attrelid=$1::regclass AND a.attname='origin_node'",
            )
            .bind(table)
            .fetch_optional(pool)
            .await?;
            ensure!(default.as_deref() == Some("1"), "{table}: {default:?}");
        }
        for name in [
            "qbit_share_ledger_share_seq_seq",
            "qbit_pool_payout_entries_payout_entry_seq_seq",
            "qbit_payout_carry_forward_carry_forward_seq_seq",
        ] {
            ensure!(sequence(pool, name).await?.0 == 2, "{name} steps by 2");
        }
        ensure!(
            next_value(pool, "qbit_pool_payout_entries_payout_entry_seq_seq").await? == 103
                && next_value(pool, "qbit_payout_carry_forward_carry_forward_seq_seq").await?
                    == 203
                && next_value(pool, "qbit_prism_sync_seq").await? == 2501,
            "B's payout, carry and sync keys continue above everything held"
        );
        ensure!(
            sequence(pool, "qbit_prism_session_sequence").await?
                == (1, 0x8000_0000, 0xffff_ffff, true)
        );
        // Setting the same node again changes nothing; repersonalising again
        // is refused.
        ensure!(b.set_node_identity(NodeIndex::B, "again").await? == done.identity);
        let error = error_of(b.repersonalise_node_identity(NodeIndex::B, "again", &RepersonaliseGuard::default())).await;
        ensure!(error.contains("already dual-writer node B"), "{error}");

        // B's frontend appends B's shares, odd and after every row held.
        let frontend = Ledger::connect(&copy.url, "node-b".into(), 4, false).await?;
        let appended = async {
            ensure!(append(&frontend, 100..102).await? == [(43, 1, 1), (45, 1, 1)]);
            let session = frontend.new_session_id().await?;
            ensure!(NodeIndex::B.extranonce1_range().contains(&session.value()));
            drop(session);
            Ok(())
        }
        .await;
        frontend.pool.close().await;
        appended
    }
    .await;
    b.pool.close().await;
    result
}

/// CONTRACT D-16: a physical copy of node A's database, rebuilt for node B,
/// becomes node B's. Its identity, key sequences (odd, above every key held
/// and every position A reached in B's old streams), session range, floors
/// and cursors are B's, with no own-log verification; A's in-flight work
/// (candidates and their deferred shares, sessions, frontends, fanout claims,
/// CPFP funding, issued jobs, sync conflicts) is gone; every other row,
/// copied of either origin or kept as the inventory decides, is A's.
#[tokio::test]
async fn repersonalise_makes_a_physical_copy_of_node_a_node_b() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let a_db = FixtureDatabase::open(&raw, "rebuild_a_").await?;
    let result = async {
        let a = Ledger::connect(&a_db.url, "node-a".into(), 4, true).await?;
        let seeded = node_a_at_work(&a).await;
        // No session may be connected to A while it is copied.
        a.pool.close().await;
        seeded?;
        let copy = a_db.copy(&raw).await?;
        let result = async {
            let reader = PgPoolOptions::new()
                .max_connections(2)
                .connect(&a_db.url)
                .await?;
            let result = rebuild_from(&reader, &copy).await;
            reader.close().await;
            result
        }
        .await;
        copy.close(result).await
    }
    .await;
    a_db.close(result).await
}

/// Each refusal names its reason and changes nothing: no identity, already
/// this node, a frontend running on the database, the peer's own database
/// (where it last proved its own log), and a table the inventory does not
/// classify.
#[tokio::test]
async fn repersonalise_refuses_a_database_that_is_no_copy_of_the_peer() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = FixtureDatabase::open(&raw, "rebuild_refusals_").await?;
    let ledger = match Ledger::connect_operator(&db.url, true).await {
        Ok(ledger) => ledger,
        Err(error) => return db.close(Err(error)).await,
    };
    let result = async {
        let ledger = &ledger;
        let pool = &ledger.pool;
        let guard = RepersonaliseGuard::default();
        let repersonalise = || ledger.repersonalise_node_identity(NodeIndex::B, "test", &guard);
        let error = error_of(repersonalise()).await;
        ensure!(error.contains("has no dual-writer node identity"), "{error}");
        ledger.set_node_identity(NodeIndex::A, "test").await?;
        let unchanged = || still_node_a(ledger);
        // A frontend heartbeating on this database, whatever its health
        // payload; a drained or stopped one, or a stale row, does not count.
        sqlx::query(
            "INSERT INTO qbit_prism_instances(instance_id,status,heartbeat_at) VALUES\
             ('live',$1,clock_timestamp()),('drained','{\"state\":\"drained\"}',clock_timestamp()),\
             ('stopped','{\"state\":\"stopped\"}',clock_timestamp()),\
             ('stale','{}',clock_timestamp()-interval '61 seconds')",
        )
        .bind(json!({"schema": "qbit.prism.audit-health.v1", "ready": true}))
        .execute(pool)
        .await?;
        let error = error_of(repersonalise()).await;
        ensure!(
            error.contains("a frontend may be running on this database: live (running, heartbeat")
                && !error.contains("drained (")
                && !error.contains("stopped (")
                && !error.contains("stale ("),
            "{error}"
        );
        unchanged().await?;
        sqlx::query("UPDATE qbit_prism_instances SET status='{\"state\":\"stopped\"}' WHERE instance_id='live'")
            .execute(pool)
            .await?;
        // No verification: nothing tells A's own database from a copy.
        let error = error_of(repersonalise()).await;
        ensure!(error.contains("carries no own-log verification of node A's"), "{error}");
        unchanged().await?;
        // Overridden, but the peer's server, read over its URL, is this one.
        let here = ledger.lineage_evidence().await?;
        let error = error_of(ledger.repersonalise_node_identity(
            NodeIndex::B,
            "test",
            &RepersonaliseGuard {
                unverified_copy: true,
                peer_server: Some(here),
            },
        ))
        .await;
        ensure!(
            error.contains("PRISM_PEER_DATABASE_URL reports this database's system identifier")
                && !error.contains("carries no own-log verification"),
            "{error}"
        );
        unchanged().await?;
        // The server node A last proved its own log on is A's own database.
        let LineageEvidence {
            system_identifier,
            timeline,
        } = ledger.lineage_evidence().await?;
        sqlx::query(
            "UPDATE qbit_prism_node_lineage SET verified_system_identifier=$1,verified_timeline=$2,\
             verified_at=clock_timestamp()",
        )
        .bind(system_identifier)
        .bind(timeline)
        .execute(pool)
        .await?;
        let error = error_of(repersonalise()).await;
        ensure!(
            error.contains("node A's own database, not a copy promoted for node B")
                && !error.contains("a frontend may be running"),
            "{error}"
        );
        unchanged().await?;
        // A promoted copy is on a new timeline.
        sqlx::query("UPDATE qbit_prism_node_lineage SET verified_timeline=verified_timeline+1")
            .execute(pool)
            .await?;
        sqlx::query("CREATE TABLE operator_notes (note text)")
            .execute(pool)
            .await?;
        let error = error_of(repersonalise()).await;
        ensure!(
            error.contains("does not classify operator_notes")
                && !error.contains("own database"),
            "{error}"
        );
        unchanged().await?;
        sqlx::query("DROP TABLE operator_notes").execute(pool).await?;
        let done = repersonalise().await?;
        ensure!(done.identity.node == NodeIndex::B, "{done:?}");
        ensure!(
            ledger.node_lineage().await?.is_some_and(|lineage| lineage.verified.is_none()),
            "a repersonalised node proves its own log again"
        );
        let error = error_of(repersonalise()).await;
        ensure!(error.contains("already dual-writer node B"), "{error}");
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    db.close(result).await
}

/// A refused repersonalisation left the database node A's, with no cursor.
async fn still_node_a(ledger: &Ledger) -> Result<()> {
    ensure!(
        ledger
            .recorded_node_identity()
            .await?
            .map(|record| record.node)
            == Some(NodeIndex::A)
            && count(&ledger.pool, "qbit_prism_peer_sync_cursors").await? == 0,
        "a refusal changed the database"
    );
    Ok(())
}

/// CONTRACT D-16: every table of a migrated schema is classified, and every
/// table and column the inventory names exists. A detached share partition
/// is its parent's; a table nobody classified, a renamed table or a renamed
/// local column is reported.
#[tokio::test]
async fn the_table_inventory_classifies_every_table_of_the_migrated_schema() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = FixtureDatabase::open(&raw, "rebuild_inventory_").await?;
    let ledger = match Ledger::connect_operator(&db.url, true).await {
        Ok(ledger) => ledger,
        Err(error) => return db.close(Err(error)).await,
    };
    let result = async {
        let pool = &ledger.pool;
        let coverage = table_inventory::schema_coverage(&mut *pool.acquire().await?).await?;
        ensure!(coverage.is_complete(), "{coverage:?}");
        let partitions: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_inherits WHERE inhparent='qbit_share_ledger'::regclass",
        )
        .fetch_one(pool)
        .await?;
        ensure!(partitions > 0, "the share ledger has no partitions");
        // A detached partition stays a table of the schema, named in the
        // partition catalog.
        sqlx::raw_sql(
            "CREATE TABLE qbit_share_ledger_p99 (LIKE qbit_share_ledger INCLUDING DEFAULTS); \
             INSERT INTO qbit_prism_share_partitions(partition_name,lower_seq,upper_seq,state,\
             archive_uri,archive_manifest_sha256,archived_at,archive_verified_at,detached_at) \
             VALUES('qbit_share_ledger_p99',9000000000,9100000000,'detached','archive:p99',\
             repeat('a',64),clock_timestamp(),clock_timestamp(),clock_timestamp())",
        )
        .execute(pool)
        .await?;
        let coverage = table_inventory::schema_coverage(&mut *pool.acquire().await?).await?;
        ensure!(coverage.is_complete(), "{coverage:?}");
        // What the inventory does not know, and what it names that is gone.
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(
            "CREATE TABLE operator_notes (note text); \
             ALTER TABLE qbit_worker_difficulty RENAME TO qbit_worker_difficulty_old; \
             ALTER TABLE qbit_pool_blocks RENAME COLUMN inactive_since TO inactive_since_old",
        )
        .execute(&mut *tx)
        .await?;
        let coverage = table_inventory::schema_coverage(&mut tx).await?;
        tx.rollback().await?;
        ensure!(
            coverage.unclassified == ["operator_notes", "qbit_worker_difficulty_old"]
                && coverage.missing_tables == ["qbit_worker_difficulty"]
                && coverage.missing_columns == ["qbit_pool_blocks.inactive_since"],
            "{coverage:?}"
        );
        // Every table the peer sync copies is classified copied.
        for table in COPIED_TABLES {
            ensure!(
                matches!(
                    table_inventory::entry(table).map(|entry| entry.class),
                    Some(TableClass::Copied(_))
                ),
                "{table}"
            );
        }
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    db.close(result).await
}
