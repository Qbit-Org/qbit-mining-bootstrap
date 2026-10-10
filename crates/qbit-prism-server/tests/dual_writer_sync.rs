//! 3.1 dual writer, the peer sync (D1), on two local PostgreSQL databases:
//! node A and node B, each a fixture database personalised by
//! `Ledger::set_node_identity`, each pulling the other's rows by identity.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    config::DualWriterConfig,
    ledger::Ledger,
    metrics::Metrics,
    node_identity::{NodeIdentity, NodeIndex},
    peer_sync::{PassReport, PeerSync, Refusal},
};
use qbit_prism_test_gate as gate;
use serde_json::Value;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

const ORDER_LOCK: i64 = 0x505249534d000002;

/// Two personalised nodes on two fixture databases.
struct Pair {
    a: Ledger,
    b: Ledger,
    a_url: String,
    b_url: String,
}

/// Open A and B, personalise them, run `test`, and drop both databases
/// whatever happens.
async fn pair(raw: &str, test: impl AsyncFnOnce(&Pair) -> Result<()>) -> Result<()> {
    let a_db = FixtureDatabase::open(raw, "sync_a_").await?;
    let b_db = match FixtureDatabase::open(raw, "sync_b_").await {
        Ok(db) => db,
        Err(error) => return a_db.close(Err(error)).await,
    };
    let result = async {
        let a = Ledger::connect(&a_db.url, "node-a".into(), 8, true).await?;
        let b = Ledger::connect(&b_db.url, "node-b".into(), 8, true).await?;
        a.set_node_identity(NodeIndex::A, "test").await?;
        b.set_node_identity(NodeIndex::B, "test").await?;
        let pair = Pair {
            a,
            b,
            a_url: a_db.url.clone(),
            b_url: b_db.url.clone(),
        };
        let result = test(&pair).await;
        pair.a.pool.close().await;
        pair.b.pool.close().await;
        result
    }
    .await;
    let result = b_db.close(result).await;
    a_db.close(result).await
}

fn config(node: NodeIndex, peer: &str, fallback: Option<&str>) -> DualWriterConfig {
    DualWriterConfig {
        identity: NodeIdentity {
            node,
            carry_owner: node == NodeIndex::A,
        },
        peer_database_url: peer.to_owned(),
        peer_database_url_fallback: fallback.map(str::to_owned),
        peer_sync_interval: Duration::from_millis(10),
        peer_sync_batch_rows: 5000,
        peer_ingest_wait: Duration::from_millis(250),
    }
}

fn sync(ledger: &Ledger, node: NodeIndex, peer: &str) -> (PeerSync, Arc<Metrics>) {
    let metrics = Arc::new(Metrics::default());
    let (sync, _) = PeerSync::new(
        ledger.clone(),
        &config(node, peer, None),
        Some(metrics.clone()),
    );
    (sync, metrics)
}

/// Pass until `done` holds, at most a few seconds: the `sync_seq` streams
/// wait for the peer's older transactions, which concurrent tests share.
async fn pass_until(
    sync: &mut PeerSync,
    mut done: impl AsyncFnMut(&PassReport) -> Result<bool>,
) -> Result<PassReport> {
    let mut last = PassReport::default();
    for _ in 0..200 {
        last = sync.pass().await?;
        if done(&last).await? {
            return Ok(last);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("the condition never held; last pass {last:?}")
}

fn hex(seed: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(seed.as_bytes()))
}

fn share(id: &str) -> qbit_prism::AcceptedShare {
    qbit_prism::AcceptedShare {
        share_seq: 0,
        share_id: format!("worker:{}", hex(id)),
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

/// Append `ids` through the native append; returns their share_seq values.
async fn append(ledger: &Ledger, ids: &[&str]) -> Result<Vec<i64>> {
    let mut seqs = Vec::new();
    for id in ids {
        let appended = ledger.append(share(id), None).await?;
        ensure!(appended.inserted, "{id} was not appended");
        seqs.push(i64::try_from(appended.share.share_seq)?);
    }
    Ok(seqs)
}

/// Land a block the way a landing writes it, in one transaction: the block
/// row, its audit snapshot and bundle, one payout and one carry row, and a
/// CTV fanout set with one artifact. Returns the block hash.
async fn land(ledger: &Ledger, seed: &str, height: i64) -> Result<String> {
    let block = hex(&format!("block {seed}"));
    let snapshot = hex(&format!("snapshot {seed}"));
    let mut tx = ledger.pool.begin().await?;
    land_in(
        &mut tx,
        seed,
        height,
        &block,
        &snapshot,
        &hex(&format!("coinbase {seed}")),
    )
    .await?;
    tx.commit().await?;
    Ok(block)
}

async fn land_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    seed: &str,
    height: i64,
    block: &str,
    snapshot: &str,
    coinbase: &str,
) -> Result<()> {
    let audit = hex(&format!("audit {seed}"));
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,as_issued_audit_sha256) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(block).bind(height).bind(hex(&format!("parent {seed}"))).bind(coinbase)
        .bind(hex(&format!("manifest {seed}"))).bind(&audit)
        .execute(&mut **tx).await?;
    sqlx::query("INSERT INTO qbit_prism_audit_snapshots(snapshot_sha256,first_share_seq,last_share_seq,anchor_ms,share_count) VALUES($1,1,3,1000,3) ON CONFLICT DO NOTHING")
        .bind(snapshot).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,share_snapshot_sha256) VALUES($1,'{}'::jsonb,$2,'00',$3)")
        .bind(block).bind(&audit).bind(snapshot)
        .execute(&mut **tx).await?;
    sqlx::query("INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,$2,'miner','miner',decode(repeat('11',32),'hex'),0,10,'accrued')")
        .bind(block).bind(height).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO qbit_payout_carry_forward(block_hash,block_height,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,$2,'miner','miner',decode(repeat('11',32),'hex'),10,0,10,0,10,'accrued')")
        .bind(block).bind(height).execute(&mut **tx).await?;
    let set = hex(&format!("set {seed}"));
    sqlx::query("INSERT INTO qbit_ctv_fanout_sets(block_hash,manifest_set_json,manifest_set,manifest_set_sha256,settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,fanout_count,fanout_output_sum_sats,covenant_output_value_sats) VALUES($1,'{}','{}'::jsonb,$2,'ctv_fanout',$3,'00',1,1,1)")
        .bind(block).bind(&set).bind(coinbase).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,covenant_output_value_sats,fanout_output_sum_sats) VALUES($1,$2,$3,'{}','{}'::jsonb,$4,$5,$6,'00',0,1,$7,0,'00','00',1,1)")
        .bind(hex(&format!("fanout {seed}"))).bind(block).bind(&set)
        .bind(hex(&format!("manifest {seed}"))).bind(hex(&format!("pre {seed}"))).bind(hex(&format!("ctv {seed}"))).bind(coinbase)
        .execute(&mut **tx).await?;
    Ok(())
}

/// Save a prepared job with its template and balance blobs, as a
/// publication does; returns its job id.
async fn prepare(ledger: &Ledger, seed: &str) -> Result<String> {
    let job_id = format!("prepared:{}:{}", ledger.instance_id, hex(seed));
    let template = hex(&format!("template {seed}"));
    let balances = hex(&format!("balances {seed}"));
    let mut tx = ledger.pool.begin().await?;
    sqlx::query("INSERT INTO qbit_prism_templates(template_sha256,template_bytes) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(&template).bind(seed.as_bytes()).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(&balances).bind(format!("[{seed}]").as_bytes()).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at,window_anchor_ms,window_prior_balances_sha256,template_sha256) VALUES($1,$2,$3,0,'{\"compact\":true}'::jsonb,clock_timestamp()+interval '1 hour',1000,$4,$5)")
        .bind(&job_id).bind(&ledger.instance_id).bind(hex(&format!("parent {seed}")))
        .bind(&balances).bind(&template).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(job_id)
}

async fn count(pool: &PgPool, sql: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(sql).fetch_one(pool).await?)
}

/// The share rows of `origin` as `(share_seq, share_id)`, in order.
async fn shares_of(pool: &PgPool, origin: i16) -> Result<Vec<(i64, String)>> {
    Ok(sqlx::query_as(
        "SELECT share_seq,share_id FROM qbit_share_ledger WHERE origin_node=$1 ORDER BY share_seq",
    )
    .bind(origin)
    .fetch_all(pool)
    .await?)
}

async fn share_rows(pool: &PgPool, origin: i16) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(s) FROM qbit_share_ledger s WHERE origin_node=$1 ORDER BY share_seq",
    )
    .bind(origin)
    .fetch_all(pool)
    .await?)
}

async fn last_share_seq(pool: &PgPool) -> Result<i64> {
    count(
        pool,
        "SELECT last_value FROM qbit_share_ledger_share_seq_seq",
    )
    .await
}

/// Steady state (S1): B pulls everything A originated, by identity, with
/// derived columns at B's own defaults, and A pulls B's back. Shares keep
/// their share_seq and every column, the safe peer mark covers them, and
/// each node's sequence rises above the peer rows it holds with its own
/// parity. A second pass applies nothing.
#[tokio::test]
async fn steady_sync_copies_each_nodes_rows_to_the_other() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        let a_seqs = append(&pair.a, &["a1", "a2", "a3"]).await?;
        let block = land(&pair.a, "x", 100).await?;
        let job = prepare(&pair.a, "p").await?;
        sqlx::query("INSERT INTO qbit_prism_node_roles(epoch,carry_owner,action,recorded_by) VALUES(0,true,'seed','test')")
            .execute(&pair.a.pool)
            .await?;
        let (mut on_b, metrics) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        let report = pass_until(&mut on_b, async |_| {
            Ok(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_jobs WHERE job_id LIKE 'prepared:%'").await? == 1
                && count(&pair.b.pool, "SELECT count(*) FROM qbit_pool_blocks").await? == 1)
        })
        .await?;
        ensure!(report.own_log_caught_up && report.reached_peer, "{report:?}");
        // Shares, identical to A's, under the safe peer mark.
        ensure!(share_rows(&pair.b.pool, 0).await? == share_rows(&pair.a.pool, 0).await?);
        ensure!(
            shares_of(&pair.b.pool, 0).await?.iter().map(|(seq, _)| *seq).collect::<Vec<_>>() == a_seqs
        );
        let mark: Option<i64> = sqlx::query_scalar("SELECT qbit_prism_peer_share_mark()")
            .fetch_one(&pair.b.pool)
            .await?;
        ensure!(mark >= a_seqs.last().copied(), "mark {mark:?} below {a_seqs:?}");
        ensure!(
            count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_share_hashes WHERE origin_node=0").await? == 3
        );
        let b_last = last_share_seq(&pair.b.pool).await?;
        ensure!(b_last > *a_seqs.last().unwrap() && b_last % 2 == 1, "B's sequence {b_last}");
        // The landing, inert until B confirms it, with every child row.
        let (chain, maturity, publication): (String, String, Option<i64>) = sqlx::query_as(
            "SELECT chain_state,maturity_state,audit_publication_sequence FROM qbit_pool_blocks WHERE block_hash=$1 AND origin_node=0",
        )
        .bind(&block)
        .fetch_one(&pair.b.pool)
        .await?;
        ensure!((chain.as_str(), maturity.as_str(), publication) == ("prepared", "immature", None));
        for table in [
            "qbit_pool_audit_bundles",
            "qbit_pool_payout_entries",
            "qbit_payout_carry_forward",
            "qbit_ctv_fanout_sets",
            "qbit_ctv_fanout_artifacts",
        ] {
            let rows = count(&pair.b.pool, &format!("SELECT count(*) FROM {table} WHERE block_hash='{block}' AND origin_node=0")).await?;
            ensure!(rows == 1, "{table} holds {rows} rows of the block");
        }
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_audit_snapshots").await? == 1);
        let status: String = sqlx::query_scalar("SELECT settlement_status FROM qbit_ctv_fanout_artifacts")
            .fetch_one(&pair.b.pool)
            .await?;
        ensure!(status == "awaiting_maturity");
        ensure!(
            count(&pair.b.pool, "SELECT count(*) FROM qbit_payout_carry_forward_current").await? == 0,
            "an unconfirmed peer landing must not count on B's balances"
        );
        // The prepared job with both its blobs, and the journal row.
        ensure!(count(&pair.b.pool, &format!("SELECT count(*) FROM qbit_prism_jobs WHERE job_id='{job}' AND origin_node=0")).await? == 1);
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_templates").await? == 1);
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_balance_snapshots").await? == 1);
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_node_roles WHERE origin_node=0").await? == 1);
        // Idempotent: another pass applies nothing and finds no conflict.
        let again = on_b.pass().await?;
        ensure!(again.applied.inserted.is_empty() && again.applied.conflicts.is_empty(), "{again:?}");
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_peer_sync_conflicts").await? == 0);
        // Metrics and status.
        let scrape = metrics.render();
        for line in [
            "qbit_prism_peer_sync_peer_reachable 1",
            "qbit_prism_peer_sync_own_log_caught_up 1",
            "qbit_prism_peer_sync_lag_rows{stream=\"shares\"} 0",
            "qbit_prism_peer_sync_rows_total{table=\"qbit_share_ledger\"} 3",
            "qbit_prism_peer_sync_path{path=\"primary\"} 1",
        ] {
            ensure!(scrape.contains(line), "{line} missing from {scrape}");
        }
        // And back: B's own rows reach A, odd, with A's sequence above them.
        let b_seqs = append(&pair.b, &["b1", "b2"]).await?;
        ensure!(b_seqs.iter().all(|seq| seq % 2 == 1 && *seq > *a_seqs.last().unwrap()));
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        pass_until(&mut on_a, async |_| Ok(shares_of(&pair.a.pool, 1).await?.len() == 2)).await?;
        ensure!(share_rows(&pair.a.pool, 1).await? == share_rows(&pair.b.pool, 1).await?);
        let a_last = last_share_seq(&pair.a.pool).await?;
        ensure!(a_last > *b_seqs.last().unwrap() && a_last % 2 == 0, "A's sequence {a_last}");
        // A's own rows were never pulled back as the peer's.
        ensure!(shares_of(&pair.a.pool, 0).await?.len() == 3);
        let next = append(&pair.a, &["a4"]).await?;
        ensure!(next[0] > *b_seqs.last().unwrap(), "A's new share orders after B's");
        Ok(())
    })
    .await
}

/// The pulls never skip a row that commits out of order. A's share append
/// in flight holds ORDER_LOCK, so B's scan stops below it and takes it once
/// it commits. A landing whose transaction is still open when a later
/// landing commits is not passed over: B's safe mark waits for it.
#[tokio::test]
async fn out_of_order_commits_are_never_skipped() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        let first = append(&pair.a, &["s0"]).await?;
        // An append in flight: ORDER_LOCK held, its share_seq drawn, not committed.
        let mut in_flight = pair.a.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(ORDER_LOCK)
            .execute(&mut *in_flight)
            .await?;
        let late: i64 = sqlx::query_scalar("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch) VALUES('worker:late','miner','miner',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(0.001),1,clock_timestamp(),'node-a',0) RETURNING share_seq")
            .fetch_one(&mut *in_flight)
            .await?;
        ensure!(late > first[0]);
        let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        on_b.pass().await?;
        let mark: Option<i64> = sqlx::query_scalar("SELECT qbit_prism_peer_share_mark()")
            .fetch_one(&pair.b.pool)
            .await?;
        ensure!(mark.is_some_and(|mark| mark < late), "the mark {mark:?} passed the uncommitted {late}");
        in_flight.commit().await?;
        let after = append(&pair.a, &["s2"]).await?;
        pass_until(&mut on_b, async |_| Ok(shares_of(&pair.b.pool, 0).await?.len() == 3)).await?;
        ensure!(
            shares_of(&pair.b.pool, 0).await?.iter().map(|(seq, _)| *seq).collect::<Vec<_>>()
                == [first[0], late, after[0]]
        );
        // Landings: X1's transaction draws its sync_seq first and stays open
        // while X2 commits.
        let mut open = pair.a.pool.begin().await?;
        land_in(&mut open, "x1", 101, &hex("block x1"), &hex("snapshot x1"), &hex("coinbase x1")).await?;
        let x2 = land(&pair.a, "x2", 102).await?;
        for _ in 0..10 {
            on_b.pass().await?;
            ensure!(
                count(&pair.b.pool, "SELECT count(*) FROM qbit_pool_blocks").await? == 0,
                "B pulled a landing past one still in flight"
            );
        }
        open.commit().await?;
        pass_until(&mut on_b, async |_| {
            Ok(count(&pair.b.pool, "SELECT count(*) FROM qbit_pool_blocks").await? == 2)
        })
        .await?;
        let held: Vec<String> = sqlx::query_scalar("SELECT block_hash FROM qbit_pool_blocks ORDER BY sync_seq")
            .fetch_all(&pair.b.pool)
            .await?;
        ensure!(held == [hex("block x1"), x2], "{held:?}");
        Ok(())
    })
    .await
}

/// A node restored from an old backup (S6) pulls back its own rows the peer
/// holds before its latch is set, raises its sequences above everything the
/// peer has seen of it, and then appends without colliding; the peer takes
/// the new rows as usual. A recovery cut short leaves the latch down, even
/// with the backup on the server and timeline it was verified on.
#[tokio::test]
async fn a_restored_node_recovers_its_own_log_from_the_peer() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        let early = append(&pair.a, &["e1", "e2"]).await?;
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        pass_until(&mut on_a, async |report| Ok(report.own_log_caught_up)).await?;
        // The backup: a copy of A as it is now.
        let backup = FixtureDatabase::open(&raw, "sync_backup_").await?;
        let result = async {
            pg_copy(&pair.a.pool, &backup).await?;
            // A goes on, and B pulls everything.
            let later = append(&pair.a, &["l1", "l2", "l3"]).await?;
            let block = land(&pair.a, "after-backup", 200).await?;
            let job = prepare(&pair.a, "after-backup").await?;
            sqlx::query("INSERT INTO qbit_prism_node_roles(epoch,carry_owner,action,recorded_by) VALUES(1,true,'seed','test')")
                .execute(&pair.a.pool)
                .await?;
            let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
            pass_until(&mut on_b, async |_| {
                Ok(shares_of(&pair.b.pool, 0).await?.len() == 5
                    && count(&pair.b.pool, "SELECT count(*) FROM qbit_pool_blocks").await? == 1
                    && count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_jobs").await? == 1)
            })
            .await?;
            // A is restored from the backup: its later rows are gone.
            let restored = Ledger::connect(&backup.url, "node-a".into(), 8, true).await?;
            ensure!(shares_of(&restored.pool, 0).await?.len() == 2);
            ensure!(restored.recorded_node_identity().await?.map(|r| r.node) == Some(NodeIndex::A));
            // The backup kept A's last verification, on this server and
            // timeline: a restore that kept the timeline. B answers, but
            // recovery fails partway (B's journal is locked). The node found
            // its own rows missing, so it must not latch on that evidence.
            let mut lock = pair.b.pool.begin().await?;
            sqlx::raw_sql("LOCK TABLE qbit_prism_node_roles IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *lock)
                .await?;
            let (mut on_restored, metrics) = sync(&restored, NodeIndex::A, &pair.b_url);
            let cut_short = on_restored.pass().await;
            lock.rollback().await?;
            ensure!(cut_short.is_err(), "recovery read a locked journal: {cut_short:?}");
            ensure!(metrics
                .render()
                .contains("qbit_prism_peer_sync_own_log_caught_up 0"));
            let verified: Option<i64> =
                sqlx::query_scalar("SELECT verified_system_identifier FROM qbit_prism_node_lineage")
                    .fetch_one(&restored.pool)
                    .await?;
            ensure!(verified.is_none(), "the verification outlived missing own rows");
            let report = on_restored.pass().await?;
            ensure!(report.own_log_caught_up, "{report:?}");
            ensure!(
                shares_of(&restored.pool, 0).await?.iter().map(|(seq, _)| *seq).collect::<Vec<_>>()
                    == [early.clone(), later.clone()].concat()
            );
            ensure!(share_rows(&restored.pool, 0).await? == share_rows(&pair.a.pool, 0).await?);
            ensure!(count(&restored.pool, &format!("SELECT count(*) FROM qbit_pool_blocks WHERE block_hash='{block}' AND origin_node=0")).await? == 1);
            ensure!(count(&restored.pool, &format!("SELECT count(*) FROM qbit_prism_jobs WHERE job_id='{job}'")).await? == 1);
            ensure!(count(&restored.pool, "SELECT count(*) FROM qbit_prism_node_roles WHERE origin_node=0").await? == 1);
            // The restored sequences rose above everything B saw of A.
            let restored_last = last_share_seq(&restored.pool).await?;
            ensure!(restored_last >= *later.last().unwrap() && restored_last % 2 == 0);
            let fresh = append(&restored, &["after-restore"]).await?;
            ensure!(fresh[0] > *later.last().unwrap());
            let block_seq: i64 = count(&pair.b.pool, "SELECT max(sync_seq) FROM qbit_pool_blocks").await?;
            let new_block = land(&restored, "after-restore", 201).await?;
            let new_seq: i64 = count(&restored.pool, &format!("SELECT sync_seq FROM qbit_pool_blocks WHERE block_hash='{new_block}'")).await?;
            ensure!(new_seq > block_seq, "the restored node reused a sync_seq B has passed");
            // B, now pulling from the restored A, takes the new rows and finds no conflict.
            let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &backup.url);
            pass_until(&mut on_b, async |_| {
                Ok(shares_of(&pair.b.pool, 0).await?.len() == 6
                    && count(&pair.b.pool, "SELECT count(*) FROM qbit_pool_blocks").await? == 2)
            })
            .await?;
            ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_peer_sync_conflicts").await? == 0);
            restored.pool.close().await;
            Ok(())
        }
        .await;
        backup.close(result).await
    })
    .await
}

/// Copy `source`'s database into the empty fixture `target` through
/// pg_dump and pg_restore, the way a backup is restored.
async fn pg_copy(source: &PgPool, target: &FixtureDatabase) -> Result<()> {
    let bin = gate::pg_bin_dir(gate::site!())?.context("PRISM_TEST_PG_BIN_DIR is required")?;
    let (database, schema): (String, String) =
        sqlx::query_as("SELECT current_database()::text,current_schema()::text")
            .fetch_one(source)
            .await?;
    let source_url = {
        let mut url = url::Url::parse(&target.url)?;
        url.set_path(&format!("/{database}"));
        url.set_query(None);
        url.to_string()
    };
    let target_url = {
        let mut url = url::Url::parse(&target.url)?;
        url.set_query(None);
        url.to_string()
    };
    let dump = tempfile::NamedTempFile::new()?;
    let status = std::process::Command::new(std::path::Path::new(&bin).join("pg_dump"))
        .args(["--format=custom", "--schema", &schema, "--file"])
        .arg(dump.path())
        .arg(&source_url)
        .status()?;
    ensure!(status.success(), "pg_dump failed");
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", target.schema))
        .execute(&PgPool::connect(&target_url).await?)
        .await?;
    let status = std::process::Command::new(std::path::Path::new(&bin).join("pg_restore"))
        .args(["--no-owner", "--dbname", &target_url])
        .arg(dump.path())
        .status()?;
    ensure!(status.success(), "pg_restore failed");
    if schema != target.schema {
        sqlx::query(&format!(
            "ALTER SCHEMA {schema} RENAME TO {}",
            target.schema
        ))
        .execute(&PgPool::connect(&target_url).await?)
        .await?;
    }
    Ok(())
}

/// CONTRACT D-8 and D-17: the own-log latch. With the peer reachable it is
/// set once the own log is verified. Starting without the peer it is set
/// when the database is on the server its own log was last verified on, and
/// stays unset, with the rollback gauge raised, when the verification is
/// missing or names another timeline; the peer coming back sets it. Losing
/// the peer later never clears it.
#[tokio::test]
async fn the_own_log_latch_without_and_with_the_peer() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        let dead = dead_url(&pair.b_url)?;
        // 1. No verification recorded, the peer unreachable: not caught up.
        let (mut cut_off, metrics) = sync(&pair.a, NodeIndex::A, &dead);
        let report = cut_off.pass().await?;
        ensure!(!report.reached_peer && !report.own_log_caught_up, "{report:?}");
        ensure!(metrics.render().contains("qbit_prism_peer_sync_rollback_evidence 1"));
        // 2. With the peer: verified, caught up, evidence recorded.
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        let report = on_a.pass().await?;
        ensure!(report.reached_peer && report.own_log_caught_up, "{report:?}");
        let lineage = pair.a.node_lineage().await?.context("lineage")?;
        let (evidence, _) = lineage.verified.context("the verification was not recorded")?;
        ensure!(evidence == pair.a.lineage_evidence().await?);
        // 3. A restart without the peer, on the same server: caught up at once.
        let (mut restarted, metrics) = sync(&pair.a, NodeIndex::A, &dead);
        let report = restarted.pass().await?;
        ensure!(!report.reached_peer && report.own_log_caught_up, "{report:?}");
        ensure!(metrics.render().contains("qbit_prism_peer_sync_rollback_evidence 0"));
        // 4. Another timeline recorded (a restore or promotion): not caught up
        //    without the peer, caught up once the peer confirms the own log.
        sqlx::query("UPDATE qbit_prism_node_lineage SET verified_timeline=verified_timeline+1")
            .execute(&pair.a.pool)
            .await?;
        let (mut rolled_back, _) = sync(&pair.a, NodeIndex::A, &dead);
        ensure!(!rolled_back.pass().await?.own_log_caught_up);
        // While a frontend whose database was restored under it stops (the
        // engine sets this when the evidence changes as it runs), no share is
        // appended.
        pair.a.set_own_log_lost(true);
        let refused = append(&pair.a, &["while-lost"]).await;
        ensure!(
            refused.as_ref().is_err_and(|error| error
                .downcast_ref::<qbit_prism_server::ledger::OwnLogLost>()
                .is_some()),
            "{refused:?}"
        );
        // The frontend stops with the flag set; its restart has a fresh one.
        pair.a.set_own_log_lost(false);
        append(&pair.a, &["after-restart"]).await?;
        let (mut recovering, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        ensure!(recovering.pass().await?.own_log_caught_up);
        // 5. The peer lost later never clears the latch.
        sqlx::query(&format!(
            "ALTER DATABASE {} ALLOW_CONNECTIONS false",
            database_of(&pair.b_url)?
        ))
        .execute(&pair.a.pool)
        .await?;
        sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()")
            .bind(database_of(&pair.b_url)?)
            .execute(&pair.a.pool)
            .await?;
        // A pooled connection the peer dropped fails one pass; the next finds
        // the peer unreachable. Neither clears the latch.
        let status = recovering.subscribe();
        let mut unreachable = false;
        for _ in 0..3 {
            if let Ok(report) = recovering.pass().await {
                ensure!(report.own_log_caught_up, "{report:?}");
                if !report.reached_peer {
                    unreachable = true;
                    break;
                }
            }
            ensure!(status.borrow().own_log_caught_up);
        }
        sqlx::query(&format!(
            "ALTER DATABASE {} ALLOW_CONNECTIONS true",
            database_of(&pair.b_url)?
        ))
        .execute(&pair.a.pool)
        .await?;
        ensure!(unreachable, "the peer never became unreachable");
        ensure!(status.borrow().own_log_caught_up && !status.borrow().peer_reachable);
        Ok(())
    })
    .await
}

fn database_of(url: &str) -> Result<String> {
    Ok(url::Url::parse(url)?
        .path()
        .trim_start_matches('/')
        .to_owned())
}

/// A URL on the same server whose database does not exist.
fn dead_url(url: &str) -> Result<String> {
    let mut url = url::Url::parse(url)?;
    url.set_path("/prism_no_such_database");
    Ok(url.to_string())
}

/// Rows held under an identity with other content are recorded as
/// conflicts and kept out, and the stream moves past them: a share whose
/// header this node credits to another share, a block held with other
/// landing facts. A share that breaks two rules is one conflict. A block
/// held with the same facts (an adoption, S8) is skipped whole, its
/// children never added twice (D-10).
#[tokio::test]
async fn conflicts_are_recorded_and_never_written_over() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        // B already credits A's next share's header to another share id, and
        // the share after it, under its own share_seq, to another header.
        let header = hex("contested");
        let doubled = share("doubled").share_id;
        let theirs = append(&pair.a, &["contested", "after", "doubled"]).await?;
        let mut tx = pair.b.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(ORDER_LOCK).execute(&mut *tx).await?;
        for (share_id, header) in [(format!("other:{header}"), header.clone()), (doubled.clone(), hex("other doubled header"))] {
            sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch) VALUES($1,'miner','miner',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(0.001),1,clock_timestamp(),'node-b',0)")
                .bind(&share_id).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO qbit_prism_share_hashes(header_hash,share_id) VALUES($1,$2)")
                .bind(&header).bind(&share_id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        // B holds one of A's blocks with other facts, and another adopted with the same.
        let conflicting = land(&pair.a, "conflicting", 300).await?;
        let adopted = land(&pair.a, "adopted", 301).await?;
        let later = land(&pair.a, "later", 302).await?;
        let mut tx = pair.b.pool.begin().await?;
        land_in(&mut tx, "conflicting", 300, &conflicting, &hex("snapshot conflicting"), &hex("another coinbase")).await?;
        land_in(&mut tx, "adopted", 301, &adopted, &hex("snapshot adopted"), &hex("coinbase adopted")).await?;
        // And one of A's prepared jobs, under its id, built on another parent.
        let contested_job = prepare(&pair.a, "contested-job").await?;
        sqlx::query("INSERT INTO qbit_prism_templates(template_sha256,template_bytes) VALUES($1,$2)")
            .bind(hex("template contested-job")).bind("contested-job".as_bytes()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2)")
            .bind(hex("balances contested-job")).bind("[contested-job]".as_bytes()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at,window_anchor_ms,window_prior_balances_sha256,template_sha256) VALUES($1,'node-a',$2,0,'{\"compact\":true}'::jsonb,clock_timestamp()+interval '1 hour',1000,$3,$4)")
            .bind(&contested_job).bind(hex("another parent")).bind(hex("balances contested-job")).bind(hex("template contested-job"))
            .execute(&mut *tx).await?;
        tx.commit().await?;
        let carries_before = count(&pair.b.pool, "SELECT count(*) FROM qbit_payout_carry_forward").await?;
        let (mut on_b, metrics) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        pass_until(&mut on_b, async |_| {
            Ok(count(&pair.b.pool, &format!("SELECT count(*) FROM qbit_pool_blocks WHERE block_hash='{later}'")).await? == 1
                && count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_peer_sync_conflicts WHERE source_table='qbit_prism_jobs'").await? == 1)
        })
        .await?;
        // The contested share is refused whole; the one after it arrives.
        let held = shares_of(&pair.b.pool, 0).await?;
        ensure!(held == [(theirs[1], share("after").share_id)], "{held:?}");
        ensure!(
            count(&pair.b.pool, &format!("SELECT count(*) FROM qbit_prism_share_hashes WHERE header_hash='{header}' AND share_id='other:{header}'")).await? == 1
        );
        // The conflicting block is B's own still, the adopted one was skipped
        // whole, the later one applied.
        let coinbase: String = sqlx::query_scalar("SELECT coinbase_txid FROM qbit_pool_blocks WHERE block_hash=$1")
            .bind(&conflicting).fetch_one(&pair.b.pool).await?;
        ensure!(coinbase == hex("another coinbase"));
        ensure!(
            count(&pair.b.pool, "SELECT count(*) FROM qbit_payout_carry_forward").await? == carries_before + 1,
            "only the later block's carry row may be added"
        );
        let conflicts: Vec<(String, String)> = sqlx::query_as(
            "SELECT source_table,row_key FROM qbit_prism_peer_sync_conflicts ORDER BY source_table,row_key",
        )
        .fetch_all(&pair.b.pool)
        .await?;
        let mut expected = vec![
            ("qbit_pool_blocks".to_owned(), conflicting.clone()),
            ("qbit_prism_jobs".to_owned(), contested_job.clone()),
            ("qbit_share_ledger".to_owned(), share("contested").share_id),
            ("qbit_share_ledger".to_owned(), doubled.clone()),
        ];
        expected.sort();
        ensure!(conflicts == expected, "{conflicts:?}");
        let scrape = metrics.render();
        ensure!(scrape.contains("qbit_prism_peer_sync_conflicts_total{table=\"qbit_pool_blocks\"} 1"), "{scrape}");
        ensure!(scrape.contains("qbit_prism_peer_sync_conflicts_total{table=\"qbit_share_ledger\"} 2"), "{scrape}");
        // The doubled share broke two rules: one sighting, both reasons.
        let (seen, detail): (i64, String) = sqlx::query_as(
            "SELECT seen_count,detail FROM qbit_prism_peer_sync_conflicts WHERE row_key=$1",
        )
        .bind(&doubled)
        .fetch_one(&pair.b.pool)
        .await?;
        ensure!(seen == 1 && detail.contains("maps header") && detail.contains("another share_seq"), "{seen} {detail}");
        // The refused rows are quarantined for good. Each stream keeps its
        // highest refused key, and later clean pulls keep it. The safe peer
        // mark only rises, and no peer share is ever inserted at or below a
        // mark already passed: a window cut there stays whole.
        let mark = async || -> Result<i64> {
            let mark: Option<i64> = sqlx::query_scalar("SELECT qbit_prism_peer_share_mark()")
                .fetch_one(&pair.b.pool)
                .await?;
            mark.context("no safe peer mark")
        };
        let first_mark = mark().await?;
        let held_below = async || -> Result<Vec<String>> {
            Ok(sqlx::query_scalar(
                "SELECT share_id FROM qbit_share_ledger WHERE origin_node=0 AND share_seq<=$1 ORDER BY share_seq",
            )
            .bind(first_mark)
            .fetch_all(&pair.b.pool)
            .await?)
        };
        let below_first_mark = held_below().await?;
        let job_seq = count(&pair.a.pool, &format!("SELECT sync_seq FROM qbit_prism_jobs WHERE job_id='{contested_job}'")).await?;
        let later_job = prepare(&pair.a, "later-job").await?;
        let fresh = append(&pair.a, &["fresh"]).await?[0];
        let mut marks = vec![first_mark];
        pass_until(&mut on_b, async |_| {
            marks.push(mark().await?);
            Ok(count(&pair.b.pool, &format!("SELECT count(*) FROM qbit_prism_jobs WHERE job_id='{later_job}'")).await? == 1
                && shares_of(&pair.b.pool, 0).await?.iter().any(|(seq, _)| *seq == fresh))
        })
        .await?;
        for _ in 0..3 {
            on_b.pass().await?;
            marks.push(mark().await?);
        }
        ensure!(marks.windows(2).all(|pair| pair[0] <= pair[1]), "the safe peer mark stepped back: {marks:?}");
        let below_after = held_below().await?;
        ensure!(below_after == below_first_mark, "a peer share was inserted below a passed mark: {below_after:?}");
        let held = shares_of(&pair.b.pool, 0).await?;
        ensure!(held == [(theirs[1], share("after").share_id), (fresh, share("fresh").share_id)], "{held:?}");
        let later_seq = count(&pair.a.pool, &format!("SELECT sync_seq FROM qbit_prism_jobs WHERE job_id='{later_job}'")).await?;
        let refused: Vec<(String, Option<i64>)> = sqlx::query_as(
            "SELECT stream,ingested_through FROM qbit_prism_peer_sync_cursors ORDER BY stream",
        )
        .fetch_all(&pair.b.pool)
        .await?;
        ensure!(
            refused
                == [
                    ("blocks".to_owned(), None),
                    ("prepared".to_owned(), Some(job_seq)),
                    ("shares".to_owned(), Some(theirs[2])),
                ],
            "{refused:?}"
        );
        // A's found-block wait confirms only a window that starts above the
        // highest quarantined share, and a prepared record above the
        // quarantined one.
        {
            use qbit_prism_server::peer_sync::{AdoptionNeeds, PeerIngest, PeerIngestWait};
            let wait = PeerIngestWait::new(&config(NodeIndex::A, &pair.b_url, None))?
                .context("the wait is on by default")?;
            let needs = |first: Option<i64>, last: Option<i64>, prepared: Option<i64>| AdoptionNeeds {
                share_seq: last,
                first_share_seq: first,
                prepared_sync_seq: prepared,
            };
            for (case, confirmed) in [
                (needs(Some(fresh), Some(fresh), Some(later_seq)), true),
                (needs(Some(theirs[1]), Some(fresh), None), false),
                (needs(None, None, Some(job_seq)), false),
            ] {
                let (_, outcome, _) = wait.wait(std::future::ready(Ok(case))).await;
                ensure!(
                    (outcome == PeerIngest::Confirmed) == confirmed && (confirmed || outcome == PeerIngest::TimedOut),
                    "{case:?}: {outcome:?}"
                );
            }
        }
        // Seen again, each conflict is counted, not duplicated.
        on_b.pass().await?;
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_peer_sync_conflicts").await? == 4);
        Ok(())
    })
    .await
}

/// A peer row far above this node's attached partitions is inserted after
/// the sync raises this node's sequence above it and attaches the lead it
/// needs.
#[tokio::test]
async fn peer_rows_beyond_the_attached_partitions_get_partitions() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        let (rows, covered): (i64, i64) = sqlx::query_as(
            "SELECT partition_rows,(SELECT max(upper_seq) FROM qbit_prism_share_partitions WHERE state='attached') FROM qbit_prism_share_partitioning",
        )
        .fetch_one(&pair.b.pool)
        .await?;
        // A's sequence jumps past B's coverage, keeping A's parity.
        let far = covered + 3 * rows;
        let far = far + far % 2;
        sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1,true)").bind(far).execute(&pair.a.pool).await?;
        sqlx::query("SELECT qbit_prism_share_partition_ensure()").execute(&pair.a.pool).await?;
        let seqs = append(&pair.a, &["far"]).await?;
        ensure!(seqs[0] > covered + 2 * rows);
        let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        pass_until(&mut on_b, async |_| Ok(shares_of(&pair.b.pool, 0).await?.len() == 1)).await?;
        let holding: String = sqlx::query_scalar("SELECT tableoid::regclass::text FROM qbit_share_ledger WHERE share_seq=$1")
            .bind(seqs[0]).fetch_one(&pair.b.pool).await?;
        ensure!(holding.starts_with("qbit_share_ledger_p"), "{holding}");
        let b_last = last_share_seq(&pair.b.pool).await?;
        ensure!(b_last > seqs[0] && b_last % 2 == 1);
        let next = append(&pair.b, &["b-after-far"]).await?;
        ensure!(next[0] > seqs[0]);
        Ok(())
    })
    .await
}

/// A dead first path fails over to PRISM_PEER_DATABASE_URL_FALLBACK, which
/// then carries the pulls.
#[tokio::test]
async fn a_dead_first_path_falls_back() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        append(&pair.a, &["via-fallback"]).await?;
        let metrics = Arc::new(Metrics::default());
        let (mut on_b, _) = PeerSync::new(
            pair.b.clone(),
            &config(NodeIndex::B, &dead_url(&pair.a_url)?, Some(&pair.a_url)),
            Some(metrics.clone()),
        );
        let first = on_b.pass().await?;
        ensure!(!first.reached_peer, "{first:?}");
        let report = pass_until(&mut on_b, async |_| {
            Ok(shares_of(&pair.b.pool, 0).await?.len() == 1)
        })
        .await?;
        ensure!(report.path == Some(1), "{report:?}");
        let scrape = metrics.render();
        ensure!(
            scrape.contains("qbit_prism_peer_sync_path{path=\"fallback\"} 1"),
            "{scrape}"
        );
        ensure!(
            scrape.contains("qbit_prism_peer_sync_failures_total{path=\"primary\"} 1"),
            "{scrape}"
        );
        Ok(())
    })
    .await
}

/// The sync refuses to run on a database that is not this node's (D-9), and
/// refuses a peer that is not the other node or runs another cluster
/// fingerprint (D-6), and either database without a valid 031 index (D-14);
/// nothing is pulled.
#[tokio::test]
async fn the_sync_refuses_wrong_identities_and_fingerprints() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        append(&pair.a, &["refused"]).await?;
        // This frontend configured as A on B's database.
        let (mut wrong_local, _) = sync(&pair.b, NodeIndex::A, &pair.a_url);
        let report = wrong_local.pass().await?;
        ensure!(
            matches!(report.refused, Some(Refusal::LocalIdentity(_))),
            "{report:?}"
        );
        // B pointed at its own database as the peer.
        let (mut wrong_peer, _) = sync(&pair.b, NodeIndex::B, &pair.b_url);
        let report = wrong_peer.pass().await?;
        ensure!(
            matches!(report.refused, Some(Refusal::PeerIdentity(_))),
            "{report:?}"
        );
        // A peer on another cluster fingerprint.
        sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint='other' WHERE singleton")
            .execute(&pair.a.pool)
            .await?;
        let (mut other_cluster, metrics) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        let report = other_cluster.pass().await?;
        ensure!(report.refused == Some(Refusal::Fingerprint), "{report:?}");
        ensure!(metrics
            .render()
            .contains("qbit_prism_peer_sync_refused{reason=\"fingerprint\"} 1"));
        ensure!(shares_of(&pair.b.pool, 0).await?.is_empty());
        // Without a valid 031 index, on the peer and then here (D-14).
        sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=$1 WHERE singleton")
            .bind(
                sqlx::query_scalar::<_, Option<String>>(
                    "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton",
                )
                .fetch_one(&pair.b.pool)
                .await?,
            )
            .execute(&pair.a.pool)
            .await?;
        sqlx::raw_sql("DROP INDEX qbit_share_ledger_origin_seq_idx")
            .execute(&pair.a.pool)
            .await?;
        let (mut no_peer_index, metrics) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        let report = no_peer_index.pass().await?;
        ensure!(
            report.refused == Some(Refusal::OriginIndex { local: false }),
            "{report:?}"
        );
        ensure!(metrics
            .render()
            .contains("qbit_prism_peer_sync_refused{reason=\"schema\"} 1"));
        sqlx::raw_sql("DROP INDEX qbit_share_ledger_origin_seq_idx")
            .execute(&pair.b.pool)
            .await?;
        let (mut no_index, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        let report = no_index.pass().await?;
        ensure!(
            report.refused == Some(Refusal::OriginIndex { local: true }),
            "{report:?}"
        );
        ensure!(shares_of(&pair.b.pool, 0).await?.is_empty());
        Ok(())
    })
    .await
}

/// CONTRACT D-19: the wait before a found block's offer confirms once the
/// peer's cursors cover this node's shares through the window and the
/// prepared record the block was built on, times out within its bound when
/// they do not, and reports an unreachable peer; it never holds the block.
#[tokio::test]
async fn the_offer_wait_reads_the_peers_cursors() -> Result<()> {
    use qbit_prism_server::peer_sync::{AdoptionNeeds, PeerIngest, PeerIngestWait};
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        let seqs = append(&pair.a, &["w1", "w2"]).await?;
        prepare(&pair.a, "w").await?;
        let prepared = pair
            .a
            .own_prepared_sync_seq(NodeIndex::A, 1000, &hex("balances w"))
            .await?;
        ensure!(
            prepared.is_some(),
            "the prepared record was not found by its window"
        );
        let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        pass_until(&mut on_b, async |_| {
            Ok(shares_of(&pair.b.pool, 0).await?.len() == 2
                && count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_jobs").await? == 1)
        })
        .await?;
        let on_a = config(NodeIndex::A, &pair.b_url, None);
        let wait = PeerIngestWait::new(&on_a)?.context("the wait is on by default")?;
        let bound = wait.bound();
        let held = AdoptionNeeds {
            share_seq: Some(seqs[1]),
            first_share_seq: Some(seqs[0]),
            prepared_sync_seq: prepared,
        };
        let (waited, outcome, _) = wait.wait(std::future::ready(Ok(held))).await;
        ensure!(
            outcome == PeerIngest::Confirmed && waited < bound,
            "{outcome:?} after {waited:?}"
        );
        let beyond = AdoptionNeeds {
            share_seq: Some(seqs[1] + 1000),
            first_share_seq: Some(seqs[0]),
            prepared_sync_seq: prepared,
        };
        let (waited, outcome, _) = wait.wait(std::future::ready(Ok(beyond))).await;
        ensure!(
            outcome == PeerIngest::TimedOut && waited >= bound.mul_f32(0.9),
            "{outcome:?} after {waited:?}"
        );
        ensure!(waited < bound * 4, "the wait overran its bound: {waited:?}");
        let dead = PeerIngestWait::new(&config(NodeIndex::A, &dead_url(&pair.b_url)?, None))?
            .context("the wait is on by default")?;
        let (_, outcome, _) = dead.wait(std::future::ready(Ok(held))).await;
        ensure!(matches!(outcome, PeerIngest::Unreachable(_)), "{outcome:?}");
        // A local read that fails leaves the offer unconfirmed at once.
        let started = std::time::Instant::now();
        let (_, outcome, needs) = wait
            .wait(std::future::ready(Err(anyhow::anyhow!(
                "the local read failed"
            ))))
            .await;
        ensure!(
            matches!(outcome, PeerIngest::Unreachable(_)) && needs.is_none(),
            "{outcome:?}"
        );
        ensure!(started.elapsed() < bound / 2, "it waited for nothing");
        // This node's own read of the needs is inside the bound too.
        let (waited, outcome, needs) = wait
            .wait(async {
                tokio::time::sleep(bound * 2).await;
                Ok(held)
            })
            .await;
        ensure!(
            outcome == PeerIngest::TimedOut && needs.is_none() && waited < bound * 2,
            "{outcome:?} after {waited:?}"
        );
        // A first path that accepts connections and never answers does not
        // use up the fallback's time.
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let mut hung = url::Url::parse(&pair.b_url)?;
        hung.set_ip_host("127.0.0.1".parse()?)
            .ok()
            .context("the silent path's host")?;
        hung.set_port(Some(silent.local_addr()?.port()))
            .ok()
            .context("the silent path's port")?;
        let held_sockets = tokio::spawn(async move {
            let mut sockets = Vec::new();
            while let Ok((socket, _)) = silent.accept().await {
                sockets.push(socket);
            }
        });
        let fallback =
            PeerIngestWait::new(&config(NodeIndex::A, hung.as_str(), Some(&pair.b_url)))?
                .context("the wait is on by default")?;
        let (waited, outcome, _) = fallback.wait(std::future::ready(Ok(held))).await;
        held_sockets.abort();
        ensure!(
            outcome == PeerIngest::Confirmed && waited < bound,
            "{outcome:?} after {waited:?} with a silent first path"
        );
        let mut off = on_a.clone();
        off.peer_ingest_wait = Duration::ZERO;
        ensure!(PeerIngestWait::new(&off)?.is_none(), "0 turns the wait off");
        Ok(())
    })
    .await
}

/// Every column of each copied table, sorted, as this release's migrations
/// leave them (CONTRACT D-2's list). A new column on a copied table fails
/// here until someone decides what the sync does with it: carry it, or keep
/// it local (ledger/table_inventory.rs, and `carried()` in
/// ledger/peer_sync.rs), and then pins it here.
const COPIED_COLUMNS: [(&str, &str); 13] = [
    (
        "qbit_share_ledger",
        "accepted,accepted_at,credit_policy,job_id,job_issued_at,miner_id,\
         network_difficulty,ntime,origin_node,p2mr_program,payout_order_key,\
         reject_reason,share_difficulty,share_id,share_seq,template_height,\
         writer_epoch,writer_id",
    ),
    (
        "qbit_prism_share_hashes",
        "header_hash,origin_node,share_id",
    ),
    (
        "qbit_pool_blocks",
        "as_issued_audit_sha256,audit_publication_sequence,block_hash,\
         block_height,chain_state,coinbase_txid,disconnected_at,found_at,\
         inactive_since,matured_at,maturity_state,origin_node,parent_hash,\
         payout_manifest_sha256,solver_miner_id,solver_network_difficulty,\
         solver_share_difficulty,solver_share_id,sync_seq",
    ),
    (
        "qbit_prism_audit_snapshots",
        "anchor_ms,created_at,first_share_seq,inline_shares,last_share_seq,\
         origin_node,share_count,snapshot_sha256",
    ),
    (
        "qbit_pool_audit_bundles",
        "audit_body_byte_len,audit_bundle,audit_bundle_sha256,\
         audit_commitment_leaves_hex,block_hash,body_uri,canonical_audit_bytes,\
         coinbase_tx_hex,created_at,found_block_bits,\
         found_block_coinbase_value_sats,found_block_network_difficulty,\
         origin_node,schema_version,share_snapshot_sha256,\
         witness_merkle_leaves_hex",
    ),
    (
        "qbit_pool_payout_entries",
        "action,block_hash,block_height,carry_forward_balance_sats,created_at,\
         maturity_state,miner_id,onchain_amount_sats,origin_node,p2mr_program,\
         payout_entry_seq,payout_order_key",
    ),
    (
        "qbit_payout_carry_forward",
        "action,block_hash,block_height,candidate_balance_sats,\
         carry_forward_balance_sats,carry_forward_seq,created_at,\
         gross_amount_sats,maturity_state,miner_id,onchain_amount_sats,\
         origin_node,p2mr_program,payout_order_key,prior_balance_sats,\
         settlement_fee_sats",
    ),
    (
        "qbit_ctv_fanout_sets",
        "block_hash,covenant_output_value_sats,created_at,fanout_count,\
         fanout_output_sum_sats,manifest_set,manifest_set_json,\
         manifest_set_sha256,origin_node,parent_coinbase_tx_hex,\
         parent_coinbase_txid,settlement_mode",
    ),
    (
        "qbit_ctv_fanout_artifacts",
        "anchor_vout,block_hash,broadcast_attempt_count,\
         broadcast_attempt_detail_count,broadcast_attempt_status_counts,\
         broadcast_retry_backoff_seconds,chunk_count,chunk_index,\
         claim_expires_at,claim_instance_id,claim_lease_seconds,claim_renewals,\
         claim_token,commitment_witness_leaf_hex,confirmed_block_hash,\
         confirmed_block_height,confirmed_depth,covenant_output_value_sats,\
         ctv_hash,fanout_output_sum_sats,fanout_tx_hex,fanout_tx_template_hex,\
         fanout_txid,first_broadcast_attempt_at,last_broadcast_attempt_at,\
         last_broadcast_attempt_status,last_broadcast_error,\
         last_broadcast_package_tx_hexes,last_broadcast_package_txids,\
         last_broadcast_submit_result,manifest,manifest_json,\
         manifest_set_sha256,manifest_sha256,next_broadcast_attempt_at,\
         origin_node,parent_coinbase_txid,parent_coinbase_vout,\
         precommitment_sha256,settlement_status,spend_scan_anchor_hash,\
         spend_scan_anchor_height,spend_scan_next_height,updated_at",
    ),
    (
        "qbit_prism_templates",
        "origin_node,template_bytes,template_sha256",
    ),
    (
        "qbit_prism_balance_snapshots",
        "balances,origin_node,prior_balances_digest",
    ),
    (
        "qbit_prism_jobs",
        "created_at,expires_at,instance_id,job_id,origin_node,parent_hash,\
         payload,payout_revision,sync_seq,template_sha256,window_anchor_ms,\
         window_first_share_seq,window_last_share_seq,\
         window_prior_balances_sha256,window_share_count,\
         window_snapshot_sha256",
    ),
    (
        "qbit_prism_node_roles",
        "action,carry_owner,detail,epoch,origin_node,recorded_at,recorded_by",
    ),
];

/// The sync carries exactly the columns of each copied table that the table
/// inventory does not name as a node's own (ledger/table_inventory.rs), but
/// a prepared job's `expires_at`, which it copies as the peer held it, so
/// that a column a migration adds to a copied table is either carried or
/// named local, never both and never neither.
#[tokio::test]
async fn the_sync_carries_every_column_the_inventory_does_not_keep_local() -> Result<()> {
    use qbit_prism_server::{ledger::table_inventory, peer_sync::COPIED_TABLES};
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = FixtureDatabase::open(&raw, "sync_columns_").await?;
    let ledger = Ledger::connect(&db.url, "columns".into(), 4, true).await?;
    let result = async {
        let carried = ledger.carried_columns().await?;
        ensure!(COPIED_COLUMNS.len() == COPIED_TABLES.len());
        for table in COPIED_TABLES {
            let all: Vec<String> = sqlx::query_scalar(
                "SELECT attname::text FROM pg_attribute WHERE attrelid=$1::regclass AND attnum>0 AND NOT attisdropped ORDER BY attname",
            )
            .bind(table)
            .fetch_all(&ledger.pool)
            .await?;
            let pinned = COPIED_COLUMNS
                .iter()
                .find(|(pinned, _)| pinned == table)
                .map(|(_, columns)| columns.split(',').collect::<Vec<_>>())
                .with_context(|| format!("{table} has no pinned column list"))?;
            ensure!(
                all == pinned,
                "{table}'s columns changed: decide whether the sync carries each new one, then pin them; now {all:?}"
            );
            let local = table_inventory::local_columns(table);
            for column in &all {
                let is_carried = carried.of(table).contains(column);
                let is_local = local.contains(&column.as_str());
                let copied_as_held = *table == "qbit_prism_jobs" && column == "expires_at";
                ensure!(
                    is_carried != is_local || copied_as_held,
                    "{table}.{column} is carried {is_carried} and local {is_local}"
                );
            }
        }
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    db.close(result).await
}

/// The sync barrier is one statement: a puller that called it and then
/// stalls, its connection open and idle, holds nothing a writer waits on,
/// and leaves no advisory lock behind. While a writer that drew a sync_seq
/// is still open the barrier is not taken, and the last mark stands.
#[tokio::test]
async fn a_stalled_puller_never_holds_the_sync_barrier() -> Result<()> {
    use qbit_prism_server::ledger::peer_sync::SYNC_BARRIER_LOCK;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        let puller = PgPool::connect(&pair.a_url).await?;
        let mut stalled = puller.acquire().await?;
        let (taken, position): (bool, Option<i64>) =
            sqlx::query_as("SELECT taken,sync_position FROM qbit_prism_sync_barrier()")
                .fetch_one(&mut *stalled)
                .await?;
        ensure!(taken && position.is_none(), "{taken} {position:?}");
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *stalled)
            .await?;
        let held: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory'",
        )
        .bind(pid)
        .fetch_one(&pair.a.pool)
        .await?;
        ensure!(held == 0, "the puller still holds {held} advisory lock(s)");
        // The puller stalls with its session open; the writers go on at once.
        let started = std::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(5), async {
            land(&pair.a, "beside a stalled puller", 400).await?;
            prepare(&pair.a, "beside a stalled puller").await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("a writer waited on a stalled puller")??;
        ensure!(started.elapsed() < Duration::from_secs(5));
        // A writer still open with its sync_seq: the barrier is not taken.
        let mut open = pair.a.pool.begin().await?;
        sqlx::query("SELECT qbit_prism_next_sync_seq()")
            .execute(&mut *open)
            .await?;
        let (taken, _): (bool, Option<i64>) =
            sqlx::query_as("SELECT taken,sync_position FROM qbit_prism_sync_barrier()")
                .fetch_one(&mut *stalled)
                .await?;
        ensure!(!taken, "the barrier was taken past an open sync_seq");
        open.rollback().await?;
        let (taken, position): (bool, Option<i64>) =
            sqlx::query_as("SELECT taken,sync_position FROM qbit_prism_sync_barrier()")
                .fetch_one(&mut *stalled)
                .await?;
        ensure!(taken && position.is_some(), "{taken} {position:?}");
        ensure!(SYNC_BARRIER_LOCK == 0x505249534d000008);
        drop(stalled);
        puller.close().await;
        Ok(())
    })
    .await
}

/// CONTRACT D-8: a peer that accepts the connection but whose reads fail is
/// as good as unreachable. The pass fails, and the latch follows the local
/// rollback evidence: set when the database is on the server its own log
/// was last verified on, unset when it is not.
#[tokio::test]
async fn a_peer_that_answers_but_cannot_be_read_leaves_the_latch_to_the_evidence() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        ensure!(on_a.pass().await?.own_log_caught_up);
        // A database that accepts the sync's connection and has no PRISM schema.
        let empty = FixtureDatabase::open(&raw, "sync_unreadable_").await?;
        let result = async {
            let (mut unreadable, _) = sync(&pair.a, NodeIndex::A, &empty.url);
            let status = unreadable.subscribe();
            ensure!(
                unreadable.pass().await.is_err(),
                "reading an empty database succeeded"
            );
            ensure!(
                status.borrow().own_log_caught_up,
                "the latch stayed down although the evidence matches"
            );
            sqlx::query("UPDATE qbit_prism_node_lineage SET verified_timeline=verified_timeline+1")
                .execute(&pair.a.pool)
                .await?;
            let (mut rolled_back, _) = sync(&pair.a, NodeIndex::A, &empty.url);
            let status = rolled_back.subscribe();
            ensure!(rolled_back.pass().await.is_err());
            ensure!(
                !status.borrow().own_log_caught_up,
                "the latch was set against rollback evidence"
            );
            Ok(())
        }
        .await;
        empty.close(result).await
    })
    .await
}

/// A block whose insert fails for a passing reason (here a lock wait on its
/// audit snapshot, which a local transaction is inserting) is never skipped:
/// only its stream fails, with the cursor where it was and nothing recorded
/// as a conflict, while the pass and the other streams go on; the block
/// lands once the wait clears.
#[tokio::test]
async fn a_block_that_fails_to_apply_for_a_passing_reason_is_retried() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        // Shares first: a block waits until the share mark covers its window.
        append(&pair.a, &["c1", "c2"]).await?;
        let block = land(&pair.a, "contended", 500).await?;
        let snapshot = hex("snapshot contended");
        // B is inserting the same snapshot row, and has not committed.
        let mut blocker = pair.b.pool.begin().await?;
        sqlx::query("INSERT INTO qbit_prism_audit_snapshots(snapshot_sha256,first_share_seq,last_share_seq,anchor_ms,share_count) VALUES($1,1,3,1000,3)")
            .bind(&snapshot)
            .execute(&mut *blocker)
            .await?;
        let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        let failed = pass_until(&mut on_b, async |report| {
            Ok(report.failed_streams.contains(&"blocks"))
        })
        .await?;
        ensure!(failed.reached_peer && failed.failed_streams == ["blocks"], "{failed:?}");
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_pool_blocks").await? == 0);
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_peer_sync_conflicts").await? == 0);
        let cursor: Option<i64> = sqlx::query_scalar(
            "SELECT scanned_through FROM qbit_prism_peer_sync_cursors WHERE stream='blocks'",
        )
        .fetch_optional(&pair.b.pool)
        .await?;
        let sync_seq = count(&pair.a.pool, &format!("SELECT sync_seq FROM qbit_pool_blocks WHERE block_hash='{block}'")).await?;
        ensure!(cursor.is_none_or(|cursor| cursor < sync_seq), "the cursor passed the block: {cursor:?}");
        // The other streams go on while the block waits.
        append(&pair.a, &["c3"]).await?;
        let job = prepare(&pair.a, "while the block waits").await?;
        pass_until(&mut on_b, async |report| {
            ensure!(report.failed_streams == ["blocks"], "{report:?}");
            Ok(count(&pair.b.pool, &format!("SELECT count(*) FROM qbit_prism_jobs WHERE job_id='{job}'")).await? == 1
                && count(&pair.b.pool, "SELECT count(*) FROM qbit_share_ledger WHERE origin_node=0").await? == 3)
        })
        .await?;
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_pool_blocks").await? == 0);
        blocker.rollback().await?;
        pass_until(&mut on_b, async |_| {
            Ok(count(&pair.b.pool, &format!("SELECT count(*) FROM qbit_pool_blocks WHERE block_hash='{block}'")).await? == 1)
        })
        .await?;
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_payout_carry_forward").await? == 1);
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_peer_sync_conflicts").await? == 0);
        Ok(())
    })
    .await
}

/// The grants of the peer's sync role on one database, exactly as
/// docs/prism-ledger-ops.md lists them for the pair's provisioning.
async fn grant_peer_role(ledger: &Ledger, role: &str) -> Result<()> {
    let (database, schema): (String, String) =
        sqlx::query_as("SELECT current_database()::text,current_schema()::text")
            .fetch_one(&ledger.pool)
            .await?;
    sqlx::raw_sql(&format!(
        "GRANT CONNECT ON DATABASE \"{database}\" TO {role}; \
         GRANT USAGE ON SCHEMA \"{schema}\" TO {role}; \
         GRANT SELECT ON qbit_share_ledger, qbit_prism_share_hashes, qbit_pool_blocks, \
             qbit_prism_audit_snapshots, qbit_pool_audit_bundles, qbit_pool_payout_entries, \
             qbit_payout_carry_forward, qbit_ctv_fanout_sets, qbit_ctv_fanout_artifacts, \
             qbit_prism_templates, qbit_prism_balance_snapshots, qbit_prism_jobs, \
             qbit_prism_node_roles TO {role}; \
         GRANT SELECT ON qbit_prism_node_identity, qbit_prism_node_lineage, \
             qbit_prism_peer_sync_cursors TO {role}; \
         GRANT SELECT (config_fingerprint) ON qbit_prism_cluster TO {role}; \
         GRANT SELECT ON SEQUENCE qbit_prism_sync_seq, qbit_share_ledger_share_seq_seq TO {role}"
    ))
    .execute(&ledger.pool)
    .await?;
    Ok(())
}

/// Every read the peer sync makes on the peer runs as the peer's sync role,
/// which holds the documented grants and nothing more (D5 provisions it):
/// here each node pulls the other's rows, recovers its own log and waits for
/// the peer's ingest as such a role, so a read that needs any other right
/// fails this test rather than a pair's first start.
#[tokio::test]
async fn the_sync_needs_only_the_peer_roles_documented_grants() -> Result<()> {
    use qbit_prism_server::peer_sync::{AdoptionNeeds, PeerIngest, PeerIngestWait};
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let role = format!("peer_sync_{}", uuid::Uuid::new_v4().simple());
    let result = pair(&raw, async |pair| {
        sqlx::raw_sql(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD 'peer-sync' NOSUPERUSER NOCREATEDB NOCREATEROLE \
             NOREPLICATION NOBYPASSRLS NOINHERIT"
        ))
        .execute(&pair.a.pool)
        .await?;
        grant_peer_role(&pair.a, &role).await?;
        grant_peer_role(&pair.b, &role).await?;
        {
            let as_role = |url: &str| -> Result<String> {
                let mut url = url::Url::parse(url)?;
                url.set_username(&role)
                    .ok()
                    .context("the role as the URL's user")?;
                url.set_password(Some("peer-sync"))
                    .ok()
                    .context("the role's password in the URL")?;
                Ok(url.to_string())
            };
            let (a_as_role, b_as_role) = (as_role(&pair.a_url)?, as_role(&pair.b_url)?);
            append(&pair.a, &["granted-a1", "granted-a2"]).await?;
            let block = land(&pair.a, "granted", 300).await?;
            let job = prepare(&pair.a, "granted").await?;
            sqlx::query("INSERT INTO qbit_prism_node_roles(epoch,carry_owner,action,recorded_by) VALUES(0,true,'seed','test')")
                .execute(&pair.a.pool)
                .await?;
            // B pulls A's rows, its own-log check reading A first.
            let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &a_as_role);
            let report = pass_until(&mut on_b, async |report| {
                ensure!(report.refused.is_none() && report.failed_streams.is_empty(), "{report:?}");
                Ok(count(&pair.b.pool, &format!("SELECT count(*) FROM qbit_pool_blocks WHERE block_hash='{block}'")).await? == 1
                    && count(&pair.b.pool, &format!("SELECT count(*) FROM qbit_prism_jobs WHERE job_id='{job}'")).await? == 1
                    && count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_node_roles WHERE origin_node=0").await? == 1)
            })
            .await?;
            ensure!(report.own_log_caught_up && report.reached_peer, "{report:?}");
            ensure!(shares_of(&pair.b.pool, 0).await?.len() == 2);
            // A pulls B's rows the same way.
            append(&pair.b, &["granted-b1"]).await?;
            let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &b_as_role);
            pass_until(&mut on_a, async |report| {
                ensure!(report.refused.is_none() && report.failed_streams.is_empty(), "{report:?}");
                Ok(report.own_log_caught_up && shares_of(&pair.a.pool, 1).await?.len() == 1)
            })
            .await?;
            // The offer wait reads B's cursors over A's streams as the role.
            let wait = PeerIngestWait::new(&config(NodeIndex::A, &b_as_role, None))?
                .context("the offer wait is configured")?;
            let a_last = shares_of(&pair.a.pool, 0).await?.last().map(|(seq, _)| *seq);
            let (_, ingest, _) = wait
                .wait(std::future::ready(Ok(AdoptionNeeds {
                    share_seq: a_last,
                    first_share_seq: a_last,
                    prepared_sync_seq: None,
                })))
                .await;
            ensure!(ingest == PeerIngest::Confirmed, "{ingest:?}");
            Ok(())
        }
    })
    .await;
    // The role's grants went with the two databases, so it holds nothing.
    let admin = sqlx::PgPool::connect(&raw).await?;
    let dropped = sqlx::raw_sql(&format!("DROP ROLE IF EXISTS {role}"))
        .execute(&admin)
        .await;
    admin.close().await;
    result?;
    dropped?;
    Ok(())
}

/// Shares the hashrate rollups have folded, over every 5-minute bucket.
async fn rolled_up(pool: &PgPool) -> Result<i64> {
    count(
        pool,
        "SELECT COALESCE(sum(accepted_share_count),0)::bigint FROM qbit_hashrate_rollup_pool WHERE grain_seconds=300",
    )
    .await
}

async fn rollup_watermark(pool: &PgPool) -> Result<i64> {
    count(
        pool,
        "SELECT COALESCE((SELECT last_share_seq FROM qbit_hashrate_rollup_progress WHERE singleton),0)",
    )
    .await
}

/// A dual writer's hashrate rollups sweep as a single writer's do, and a
/// peer share pulled after this node's own newer ones, with a share_seq
/// below the sweep's watermark, is folded once, by the pull that inserts it.
/// A peer share above the watermark is the sweep's.
#[tokio::test]
async fn a_late_peer_share_below_the_local_max_is_still_rolled_up() -> Result<()> {
    use qbit_prism_server::rollups::advance_dual_writer;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        let b_seqs = append(&pair.b, &["late-b"]).await?;
        let a_seqs = append(&pair.a, &["late-a1", "late-a2", "late-a3"]).await?;
        ensure!(
            b_seqs[0] < a_seqs[2],
            "B's share {b_seqs:?} is not below A's {a_seqs:?}"
        );
        // A's own shares fold as a single writer's do, peer or no peer.
        advance_dual_writer(&pair.a.pool, 1000).await?;
        ensure!(rollup_watermark(&pair.a.pool).await? == a_seqs[2]);
        ensure!(rolled_up(&pair.a.pool).await? == 3);
        // B's share arrives below that watermark: the pull folds it.
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        pass_until(&mut on_a, async |_| {
            Ok(shares_of(&pair.a.pool, 1).await?.len() == 1)
        })
        .await?;
        ensure!(
            rolled_up(&pair.a.pool).await? == 4,
            "{} folded",
            rolled_up(&pair.a.pool).await?
        );
        // The sweep never counts it again.
        advance_dual_writer(&pair.a.pool, 1000).await?;
        ensure!(rolled_up(&pair.a.pool).await? == 4);
        // A share of B's above A's watermark is left to the sweep.
        let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        pass_until(&mut on_b, async |_| {
            Ok(shares_of(&pair.b.pool, 0).await?.len() == 3)
        })
        .await?;
        let high = append(&pair.b, &["late-b2"]).await?;
        ensure!(high[0] > a_seqs[2], "{high:?}");
        pass_until(&mut on_a, async |_| {
            Ok(shares_of(&pair.a.pool, 1).await?.len() == 2)
        })
        .await?;
        ensure!(rolled_up(&pair.a.pool).await? == 4);
        advance_dual_writer(&pair.a.pool, 1000).await?;
        ensure!(rolled_up(&pair.a.pool).await? == 5);
        Ok(())
    })
    .await
}

/// A share-ledger partition of a dual writer leaves only once the safe peer
/// mark has passed it: the peer's rows can still land in it until then, even
/// with every row it holds folded and this node's sequence past its bound.
/// An operator's declaration that the peer's unpulled rows are lost lifts
/// the condition.
#[tokio::test]
async fn a_partition_the_peer_mark_has_not_passed_never_leaves() -> Result<()> {
    use qbit_prism_server::{ledger::archive, rollups::advance_dual_writer};
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let options = archive::PlanOptions {
        network_difficulty: "1".into(),
        retention_days: 30,
        window_multiple: 4,
        check_duplicates: false,
    };
    // The first partition's rollup condition on A.
    let condition = async |ledger: &Ledger| -> Result<(String, String)> {
        let report = archive::plan(ledger, &options).await?;
        let first = report
            .partitions
            .iter()
            .min_by_key(|plan| plan.record.upper_seq)
            .context("no partition")?;
        let rollup = first
            .conditions
            .iter()
            .find(|condition| condition.name == "rollup_watermark")
            .context("no rollup condition")?;
        Ok((rollup.status.to_owned(), rollup.detail.clone()))
    };
    pair(&raw, async |pair| {
        let a_seqs = append(&pair.a, &["leave-a1", "leave-a2"]).await?;
        let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        pass_until(&mut on_b, async |_| {
            Ok(shares_of(&pair.b.pool, 0).await?.len() == 2)
        })
        .await?;
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        pass_until(&mut on_a, async |_| {
            let mark: Option<i64> = sqlx::query_scalar("SELECT qbit_prism_peer_share_mark()")
                .fetch_one(&pair.a.pool)
                .await?;
            Ok(mark >= Some(a_seqs[1]))
        })
        .await?;
        advance_dual_writer(&pair.a.pool, 1000).await?;
        ensure!(rollup_watermark(&pair.a.pool).await? == a_seqs[1]);
        // A's sequence past the first partition's bound: on a single writer
        // its rollup condition would now clear.
        let upper = count(
            &pair.a.pool,
            "SELECT min(upper_seq) FROM qbit_prism_share_partitions",
        )
        .await?;
        let past = upper + 10 - (upper + 10) % 2;
        sqlx::query("SELECT setval(pg_get_serial_sequence('qbit_share_ledger','share_seq'),$1)")
            .bind(past)
            .execute(&pair.a.pool)
            .await?;
        sqlx::raw_sql("SELECT qbit_prism_share_partition_ensure()")
            .execute(&pair.a.pool)
            .await?;
        let (status, detail) = condition(&pair.a).await?;
        ensure!(
            status == "blocked" && detail.contains("safe peer mark"),
            "{status}: {detail}"
        );
        // Declared lost, the peer's rows no longer hold it.
        sqlx::query("UPDATE qbit_prism_node_lineage SET peer_tail_lost_at=clock_timestamp()")
            .execute(&pair.a.pool)
            .await?;
        let (status, detail) = condition(&pair.a).await?;
        ensure!(status == "clear", "{status}: {detail}");
        sqlx::query("UPDATE qbit_prism_node_lineage SET peer_tail_lost_at=NULL")
            .execute(&pair.a.pool)
            .await?;
        // A peer share above the bound moves A's mark past it.
        let past_b = past + 1;
        sqlx::query("SELECT setval(pg_get_serial_sequence('qbit_share_ledger','share_seq'),$1)")
            .bind(past_b)
            .execute(&pair.b.pool)
            .await?;
        let b_seqs = append(&pair.b, &["leave-b"]).await?;
        ensure!(b_seqs[0] >= upper, "{b_seqs:?}");
        pass_until(&mut on_a, async |_| {
            Ok(shares_of(&pair.a.pool, 1).await?.len() == 1)
        })
        .await?;
        advance_dual_writer(&pair.a.pool, 1000).await?;
        let (status, detail) = condition(&pair.a).await?;
        ensure!(status == "clear", "{status}: {detail}");
        Ok(())
    })
    .await
}

/// Every plan node, depth first, init plans and CTEs included.
fn plan_nodes<'a>(node: &'a Value, all: &mut Vec<&'a Value>) {
    all.push(node);
    for child in node["Plans"].as_array().into_iter().flatten() {
        plan_nodes(child, all);
    }
}

/// D-14, with D2: the peer sync's share reads by origin take the shape only
/// the (origin_node, share_seq) index of 031 can serve, so none walks the
/// ledger through the other node's rows. Here node A's rows lie under a long
/// run of node B's, where a plain `origin_node = $1` read walks the primary
/// key backward even with the index present (D2 measured it). Each read runs
/// prepared, as sqlx runs it, under PostgreSQL's default random_page_cost
/// and under 1.1, the SSD value, through custom and generic plans. What is
/// asserted is the shape: no sequential scan that reads a ledger row or a
/// header mapping (the empty lead partitions may be scanned, as they read
/// nothing), the origin reads through the index, and the ledger rows and
/// mappings each read visits bounded by the rows it returns or scans by
/// design, plus a probe per partition, whatever the other node's run holds.
#[tokio::test]
async fn the_origin_share_reads_never_walk_the_other_nodes_rows() -> Result<()> {
    use qbit_prism_server::ledger::peer_sync::{peer, HIGHEST_SHARE};
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    /// A's rows, and B's run above them.
    const OWN: i64 = 2_000;
    const RUN: i64 = 18_000;
    let db = FixtureDatabase::open(&raw, "sync_plan_").await?;
    let result = async {
        let a = Ledger::connect(&db.url, "node-a".into(), 4, true).await?;
        a.set_node_identity(NodeIndex::A, "test").await?;
        let pool = &a.pool;
        let base: i64 = sqlx::query_scalar("SELECT last_value FROM qbit_share_ledger_share_seq_seq")
            .fetch_one(pool)
            .await?;
        sqlx::query(
            "INSERT INTO qbit_share_ledger(share_seq,origin_node,share_id,miner_id,payout_order_key,p2mr_program,\
             share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,writer_id,writer_epoch) \
             SELECT $1+g,CASE WHEN g<=$2 THEN 0 ELSE 1 END,'plan-'||g,'miner','miner',decode(repeat('11',32),'hex'),\
             1,100,100,'job',clock_timestamp(),0,'test',1 FROM generate_series(1,$2+$3) g",
        )
        .bind(base)
        .bind(OWN)
        .bind(RUN)
        .execute(pool)
        .await?;
        // Their header mappings, which the share reads return beside them.
        sqlx::query(
            "INSERT INTO qbit_prism_share_hashes(header_hash,share_id,origin_node) \
             SELECT md5('plan-h'||g)||md5('plan-x'||g),'plan-'||g,CASE WHEN g<=$1 THEN 0 ELSE 1 END \
             FROM generate_series(1,$1+$2) g",
        )
        .bind(OWN)
        .bind(RUN)
        .execute(pool)
        .await?;
        for table in ["qbit_share_ledger", "qbit_prism_share_hashes"] {
            // One statement each: VACUUM refuses a transaction block.
            sqlx::raw_sql(&format!("VACUUM ANALYZE {table}"))
                .execute(pool)
                .await?;
        }
        let partitions = count(
            pool,
            "SELECT count(*) FROM pg_inherits WHERE inhparent='qbit_share_ledger'::regclass",
        )
        .await?;
        let last_own = base + OWN;
        // Its own connection, never returned to the pool, so the plan mode
        // and costs set below reach no other query.
        let mut conn = pool.acquire().await?.detach();
        sqlx::raw_sql(&format!(
            "PREPARE highest(smallint) AS {HIGHEST_SHARE}; \
             PREPARE scanned(bigint,bigint,smallint) AS {}; \
             PREPARE shares_of(smallint,bigint,bigint) AS {}; \
             PREPARE beyond(smallint,bigint,bigint) AS {}",
            peer::shares_scanned_sql(),
            peer::shares_of_sql(),
            peer::SHARES_BEYOND,
        ))
        .execute(&mut conn)
        .await?;
        // Each read, whether it reads by origin, and the ledger rows it
        // visits by design.
        let reads = [
            // A's newest, under B's whole run; B's newest.
            ("highest(0::smallint)".to_owned(), true, 1),
            ("highest(1::smallint)".to_owned(), true, 1),
            // A's last ten rows, then B's run: the read must stop there.
            (format!("shares_of(0::smallint,{},5000)", last_own - 10), true, 10),
            (format!("shares_of(0::smallint,{base},500)"), true, 500),
            (format!("beyond(0::smallint,{},1000000)", last_own - 10), true, 10),
            // 5000 rows of any origin by the primary key, and A's ten among
            // them, which the pick may read again by either index.
            (format!("scanned({},5000,0::smallint)", last_own - 10), false, 2 * 5000 + 10),
        ];
        let mut walks = Vec::new();
        for cost in ["4", "1.1"] {
            for mode in ["force_custom_plan", "force_generic_plan"] {
                sqlx::raw_sql(&format!(
                    "SET random_page_cost={cost}; SET plan_cache_mode={mode}"
                ))
                .execute(&mut conn)
                .await?;
                for (execute, by_origin, rows) in &reads {
                    let plan: Value = sqlx::query_scalar(&format!(
                        "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) EXECUTE {execute}"
                    ))
                    .fetch_one(&mut conn)
                    .await?;
                    let mut all = Vec::new();
                    plan_nodes(&plan[0]["Plan"], &mut all);
                    // Rows per loop: a lookup loops once per row it serves.
                    let read = |node: &&Value| {
                        let rows = |key: &str| node[key].as_f64().unwrap_or(0.0);
                        (rows("Actual Rows")
                            + rows("Rows Removed by Filter")
                            + rows("Rows Removed by Index Recheck"))
                            * node["Actual Loops"].as_f64().unwrap_or(1.0)
                    };
                    let of = |table: &'static str| {
                        all.iter()
                            .filter(move |node| {
                                node["Relation Name"]
                                    .as_str()
                                    .is_some_and(|name| name.starts_with(table))
                            })
                            .copied()
                            .collect::<Vec<&Value>>()
                    };
                    let (ledger, mappings) = (of("qbit_share_ledger"), of("qbit_prism_share_hashes"));
                    let at = format!("random_page_cost {cost}, {mode}, {execute}");
                    if ledger
                        .iter()
                        .chain(&mappings)
                        .any(|node| node["Node Type"] == "Seq Scan" && read(node) > 0.0)
                    {
                        walks.push(format!("{at}: a sequential scan that reads rows: {plan}"));
                    }
                    if *by_origin
                        && !all.iter().any(|node| {
                            node["Index Name"]
                                .as_str()
                                .is_some_and(|name| name.ends_with("_origin_seq_idx"))
                        })
                    {
                        walks.push(format!("{at}: not through the origin index: {plan}"));
                    }
                    let bound = (rows + 2 * partitions) as f64;
                    for (what, nodes) in [("ledger rows", &ledger), ("header mappings", &mappings)] {
                        let visited: f64 = nodes.iter().map(read).sum();
                        if visited > bound {
                            walks.push(format!(
                                "{at}: visited {visited} {what}, at most {bound}: {plan}"
                            ));
                        }
                    }
                }
            }
        }
        drop(conn);
        a.pool.close().await;
        ensure!(
            walks.is_empty(),
            "the origin share reads walk the ledger:\n{}",
            walks.join("\n")
        );
        Ok(())
    }
    .await;
    db.close(result).await
}

/// D-9: a database whose identity changes under the running frontend stops
/// it, reporting it not ready and refusing every share until it has exited:
/// an origin default that drifts, and an identity row rewritten as the other
/// node's, each after the sync saw the database ready.
#[tokio::test]
async fn a_frontend_whose_identity_changes_while_it_runs_stops() -> Result<()> {
    use qbit_prism_server::{ledger::OwnLogLost, peer_sync::FrontendStop};
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        // A drifted origin default on A.
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        let status = on_a.subscribe();
        ensure!(on_a.pass().await?.own_log_caught_up);
        sqlx::raw_sql("ALTER TABLE qbit_pool_blocks ALTER COLUMN origin_node SET DEFAULT 1")
            .execute(&pair.a.pool)
            .await?;
        on_a.recheck_identity();
        let stopped = on_a.pass().await;
        ensure!(
            stopped.as_ref().is_err_and(|error| matches!(
                error.downcast_ref::<FrontendStop>(),
                Some(FrontendStop::IdentityChanged(_))
            )),
            "{stopped:?}"
        );
        ensure!(
            !status.borrow().own_log_caught_up,
            "a stopping node still reports ready"
        );
        let refused = append(&pair.a, &["drifted"]).await;
        ensure!(
            refused
                .as_ref()
                .is_err_and(|error| error.downcast_ref::<OwnLogLost>().is_some()),
            "{refused:?}"
        );
        // B's identity row rewritten as the other node's.
        let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        let status = on_b.subscribe();
        ensure!(on_b.pass().await?.own_log_caught_up);
        sqlx::raw_sql("UPDATE qbit_prism_node_identity SET node_index=0")
            .execute(&pair.b.pool)
            .await?;
        on_b.recheck_identity();
        let stopped = on_b.pass().await;
        ensure!(
            stopped.as_ref().is_err_and(|error| matches!(
                error.downcast_ref::<FrontendStop>(),
                Some(FrontendStop::IdentityChanged(_))
            )),
            "{stopped:?}"
        );
        ensure!(!status.borrow().own_log_caught_up);
        ensure!(pair.b.own_log_lost());
        Ok(())
    })
    .await
}

/// Landed blocks commit out of sync_seq order, so a backup can hold a block
/// whose earlier-numbered sibling was still open. Own-log recovery reads
/// every own block the peer holds, not only those above the highest held
/// here, and the restored node gets the earlier one back.
#[tokio::test]
async fn recovery_finds_an_own_block_committed_after_a_later_numbered_one() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        append(&pair.a, &["o1", "o2", "o3"]).await?;
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        pass_until(&mut on_a, async |report| Ok(report.own_log_caught_up)).await?;
        // The first block draws its sync_seq and stays open while the second
        // lands and commits.
        let first = hex("block out-of-order-1");
        let mut open = pair.a.pool.begin().await?;
        land_in(
            &mut open,
            "out-of-order-1",
            400,
            &first,
            &hex("snapshot out-of-order-1"),
            &hex("coinbase out-of-order-1"),
        )
        .await?;
        let second = land(&pair.a, "out-of-order-2", 401).await?;
        let (first_seq, second_seq): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT sync_seq FROM qbit_pool_blocks WHERE block_hash=$1),\
             (SELECT sync_seq FROM qbit_pool_blocks WHERE block_hash=$2)",
        )
        .bind(&first)
        .bind(&second)
        .fetch_one(&mut *open)
        .await?;
        ensure!(first_seq < second_seq, "{first_seq} {second_seq}");
        let backup = FixtureDatabase::open(&raw, "sync_backup_").await?;
        let result = async {
            pg_copy(&pair.a.pool, &backup).await?;
            open.commit().await?;
            let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
            pass_until(&mut on_b, async |_| {
                Ok(count(&pair.b.pool, "SELECT count(*) FROM qbit_pool_blocks").await? == 2)
            })
            .await?;
            // A is restored: it holds the second block, not the first.
            let restored = Ledger::connect(&backup.url, "node-a".into(), 8, true).await?;
            let held = |hash: String| {
                let pool = restored.pool.clone();
                async move {
                    count(
                        &pool,
                        &format!("SELECT count(*) FROM qbit_pool_blocks WHERE block_hash='{hash}'"),
                    )
                    .await
                }
            };
            ensure!(held(second.clone()).await? == 1 && held(first.clone()).await? == 0);
            let (mut on_restored, _) = sync(&restored, NodeIndex::A, &pair.b_url);
            pass_until(
                &mut on_restored,
                async |report| Ok(report.own_log_caught_up),
            )
            .await?;
            ensure!(
                held(first.clone()).await? == 1,
                "recovery skipped the earlier-numbered block"
            );
            ensure!(
                count(
                    &restored.pool,
                    "SELECT count(*) FROM qbit_prism_peer_sync_conflicts"
                )
                .await?
                    == 0
            );
            restored.pool.close().await;
            Ok(())
        }
        .await;
        backup.close(result).await
    })
    .await
}

/// A node prunes its own prepared jobs at expiry, while the peer may keep
/// its copy longer. Own-log recovery reads only the unexpired ones, so a job
/// pruned here is neither missing, which would forget the verification, nor
/// put back.
#[tokio::test]
async fn recovery_leaves_own_prepared_jobs_pruned_at_expiry() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        let job = prepare(&pair.a, "pruned").await?;
        let held = |pool: PgPool| {
            let job = job.clone();
            async move {
                count(
                    &pool,
                    &format!("SELECT count(*) FROM qbit_prism_jobs WHERE job_id='{job}'"),
                )
                .await
            }
        };
        let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        pass_until(&mut on_b, async |_| Ok(held(pair.b.pool.clone()).await? == 1)).await?;
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        pass_until(&mut on_a, async |report| Ok(report.own_log_caught_up)).await?;
        // The job expires on both nodes; A prunes it, B keeps its copy.
        for pool in [&pair.a.pool, &pair.b.pool] {
            sqlx::query(
                "UPDATE qbit_prism_jobs SET expires_at=clock_timestamp()-interval '1 minute' WHERE job_id=$1",
            )
            .bind(&job)
            .execute(pool)
            .await?;
        }
        ensure!(pair.a.prune_expired_jobs().await? == 1);
        ensure!(held(pair.b.pool.clone()).await? == 1);
        // A restarts while B's journal is locked: recovery reads A's rows the
        // peer holds, finds none missing, and stops at the journal, leaving
        // the verification in place.
        let mut lock = pair.b.pool.begin().await?;
        sqlx::raw_sql("LOCK TABLE qbit_prism_node_roles IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await?;
        let (mut restarted, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        let cut_short = restarted.pass().await;
        lock.rollback().await?;
        ensure!(cut_short.is_err(), "recovery read a locked journal: {cut_short:?}");
        ensure!(
            pair.a.node_lineage().await?.and_then(|lineage| lineage.verified).is_some(),
            "a job pruned here at expiry counted as a missing own row"
        );
        // A full recovery leaves it pruned.
        let (mut recovered, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        pass_until(&mut recovered, async |report| Ok(report.own_log_caught_up)).await?;
        ensure!(
            held(pair.a.pool.clone()).await? == 0,
            "recovery put back a job pruned at expiry"
        );
        Ok(())
    })
    .await
}

/// Own-log recovery takes back this node's rows however far above its own
/// sequence they lie, up to the peer's cursor over its shares, which every
/// own row the peer holds came through. An own row beyond that and the pull
/// headroom (a damaged peer) is a conflict: the own log has diverged, the
/// latch stays down, and nothing is raised or attached for it.
#[tokio::test]
async fn recovery_refuses_an_own_row_beyond_the_peers_cursor() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        append(&pair.a, &["near"]).await?;
        let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        pass_until(&mut on_b, async |_| Ok(shares_of(&pair.b.pool, 0).await?.len() == 1)).await?;
        // B holds a row of A's 200 partitions beyond either node's reach.
        let (rows, covered): (i64, i64) = sqlx::query_as(
            "SELECT partition_rows,(SELECT max(upper_seq) FROM qbit_prism_share_partitions WHERE state='attached') FROM qbit_prism_share_partitioning",
        )
        .fetch_one(&pair.a.pool)
        .await?;
        let b_covered = count(
            &pair.b.pool,
            "SELECT max(upper_seq) FROM qbit_prism_share_partitions WHERE state='attached'",
        )
        .await?;
        let lower = (covered.max(b_covered) / rows + 200) * rows;
        let far = lower + 2;
        let far_id = format!("worker:{}", hex("far beyond"));
        let mut tx = pair.b.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(ORDER_LOCK).execute(&mut *tx).await?;
        sqlx::query("SELECT qbit_prism_share_partition_create('qbit_share_ledger_far',$1,$2)")
            .bind(lower).bind(lower + rows).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch,origin_node) VALUES($1,$2,'miner','miner',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(0.001),1,clock_timestamp(),'node-a',0,0)")
            .bind(far).bind(&far_id).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO qbit_prism_share_hashes(header_hash,share_id,origin_node) VALUES($1,$2,0)")
            .bind(hex("far beyond")).bind(&far_id).execute(&mut *tx).await?;
        tx.commit().await?;
        sqlx::query("SELECT qbit_prism_share_partition_ensure()").execute(&pair.a.pool).await?;
        let partitions = "SELECT count(*) FROM qbit_prism_share_partitions";
        let partitions_before = count(&pair.a.pool, partitions).await?;
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        let refused = on_a.pass().await;
        ensure!(
            refused.as_ref().is_err_and(|error| format!("{error:#}").contains("diverged")),
            "{refused:?}"
        );
        ensure!(!on_a.subscribe().borrow().own_log_caught_up);
        let detail: String = sqlx::query_scalar(
            "SELECT detail FROM qbit_prism_peer_sync_conflicts WHERE row_key=$1",
        )
        .bind(&far_id)
        .fetch_one(&pair.a.pool)
        .await?;
        ensure!(detail.contains("beyond any key"), "{detail}");
        ensure!(last_share_seq(&pair.a.pool).await? < lower);
        ensure!(count(&pair.a.pool, partitions).await? == partitions_before);
        Ok(())
    })
    .await
}

/// Own-log recovery compares each own landed block the peer holds and this
/// database holds too by a digest of its immutable facts: one held here
/// with other facts is read whole and recorded as a conflict, the own log
/// has diverged, and the latch stays down. The verification goes in the
/// transaction that records the conflict, so a restart without the peer
/// stays down as well, even after a crash right after it.
#[tokio::test]
async fn recovery_finds_an_own_block_held_with_other_facts() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        append(&pair.a, &["d1", "d2", "d3"]).await?;
        let block = land(&pair.a, "diverged", 500).await?;
        let (mut on_b, _) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        pass_until(&mut on_b, async |_| {
            Ok(count(&pair.b.pool, "SELECT count(*) FROM qbit_pool_blocks").await? == 1)
        })
        .await?;
        let (mut on_a, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        pass_until(&mut on_a, async |report| Ok(report.own_log_caught_up)).await?;
        // A's own row of the block now names another coinbase.
        sqlx::query("UPDATE qbit_pool_blocks SET coinbase_txid=$2 WHERE block_hash=$1")
            .bind(&block)
            .bind(hex("another coinbase"))
            .execute(&pair.a.pool)
            .await?;
        // Recording the conflict clears the verification in the same
        // transaction, so a crash right after it cannot leave A trusting it.
        {
            use sqlx::Connection;
            let mut on_b_database = sqlx::PgConnection::connect(&pair.b_url).await?;
            let bundles = qbit_prism_server::ledger::peer_sync::peer::blocks_with_hashes(
                &mut on_b_database,
                NodeIndex::A,
                std::slice::from_ref(&block),
            )
            .await?;
            on_b_database.close().await?;
            ensure!(bundles.len() == 1, "{}", bundles.len());
            let applied = pair.a.apply_block(&bundles[0], None).await?;
            ensure!(applied.total_conflicts() == 1, "{applied:?}");
            ensure!(
                pair.a.node_lineage().await?.and_then(|lineage| lineage.verified).is_none(),
                "the conflict was recorded without clearing the verification"
            );
        }
        let (mut restarted, _) = sync(&pair.a, NodeIndex::A, &pair.b_url);
        let refused = restarted.pass().await;
        ensure!(
            refused.as_ref().is_err_and(|error| format!("{error:#}").contains("diverged")),
            "{refused:?}"
        );
        ensure!(!restarted.subscribe().borrow().own_log_caught_up);
        ensure!(
            count(
                &pair.a.pool,
                &format!("SELECT count(*) FROM qbit_prism_peer_sync_conflicts WHERE source_table='qbit_pool_blocks' AND row_key='{block}'"),
            )
            .await?
                == 1
        );
        ensure!(
            pair.a.node_lineage().await?.and_then(|lineage| lineage.verified).is_none(),
            "the verification outlived a diverged own log"
        );
        let (mut cut_off, _) = sync(&pair.a, NodeIndex::A, &dead_url(&pair.b_url)?);
        ensure!(!cut_off.pass().await?.own_log_caught_up);
        Ok(())
    })
    .await
}
