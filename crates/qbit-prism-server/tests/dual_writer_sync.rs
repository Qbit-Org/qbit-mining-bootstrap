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
/// the new rows as usual.
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
            let (mut on_restored, _) = sync(&restored, NodeIndex::A, &pair.b_url);
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
/// landing facts. A block held with the same facts (an adoption, S8) is
/// skipped whole, its children never added twice (D-10).
#[tokio::test]
async fn conflicts_are_recorded_and_never_written_over() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    pair(&raw, async |pair| {
        // B already credits A's next share's header to another share id.
        let header = hex("contested");
        let theirs = append(&pair.a, &["contested", "after"]).await?;
        let mut tx = pair.b.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(ORDER_LOCK).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch) VALUES($1,'miner','miner',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(0.001),1,clock_timestamp(),'node-b',0)")
            .bind(format!("other:{header}")).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO qbit_prism_share_hashes(header_hash,share_id) VALUES($1,$2)")
            .bind(&header).bind(format!("other:{header}")).execute(&mut *tx).await?;
        tx.commit().await?;
        // B holds one of A's blocks with other facts, and another adopted with the same.
        let conflicting = land(&pair.a, "conflicting", 300).await?;
        let adopted = land(&pair.a, "adopted", 301).await?;
        let later = land(&pair.a, "later", 302).await?;
        let mut tx = pair.b.pool.begin().await?;
        land_in(&mut tx, "conflicting", 300, &conflicting, &hex("snapshot conflicting"), &hex("another coinbase")).await?;
        land_in(&mut tx, "adopted", 301, &adopted, &hex("snapshot adopted"), &hex("coinbase adopted")).await?;
        tx.commit().await?;
        let carries_before = count(&pair.b.pool, "SELECT count(*) FROM qbit_payout_carry_forward").await?;
        let (mut on_b, metrics) = sync(&pair.b, NodeIndex::B, &pair.a_url);
        pass_until(&mut on_b, async |_| {
            Ok(count(&pair.b.pool, &format!("SELECT count(*) FROM qbit_pool_blocks WHERE block_hash='{later}'")).await? == 1)
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
        ensure!(
            conflicts
                == [
                    ("qbit_pool_blocks".to_owned(), conflicting.clone()),
                    ("qbit_share_ledger".to_owned(), share("contested").share_id),
                ],
            "{conflicts:?}"
        );
        let scrape = metrics.render();
        ensure!(scrape.contains("qbit_prism_peer_sync_conflicts_total{table=\"qbit_pool_blocks\"} 1"), "{scrape}");
        // Seen again, each conflict is counted, not duplicated.
        on_b.pass().await?;
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_peer_sync_conflicts").await? == 2);
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
/// fingerprint (D-6); nothing is pulled.
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
        let wait = PeerIngestWait::new(&on_a).context("the wait is on by default")?;
        let bound = wait.bound();
        let held = AdoptionNeeds {
            share_seq: Some(seqs[1]),
            prepared_sync_seq: prepared,
        };
        let (waited, outcome) = wait.wait(held).await;
        ensure!(
            outcome == PeerIngest::Confirmed && waited < bound,
            "{outcome:?} after {waited:?}"
        );
        let beyond = AdoptionNeeds {
            share_seq: Some(seqs[1] + 1000),
            prepared_sync_seq: prepared,
        };
        let (waited, outcome) = wait.wait(beyond).await;
        ensure!(
            outcome == PeerIngest::TimedOut && waited >= bound.mul_f32(0.9),
            "{outcome:?} after {waited:?}"
        );
        ensure!(waited < bound * 4, "the wait overran its bound: {waited:?}");
        let dead = PeerIngestWait::new(&config(NodeIndex::A, &dead_url(&pair.b_url)?, None))
            .context("the wait is on by default")?;
        let (_, outcome) = dead.wait(held).await;
        ensure!(matches!(outcome, PeerIngest::Unreachable(_)), "{outcome:?}");
        let mut off = on_a.clone();
        off.peer_ingest_wait = Duration::ZERO;
        ensure!(PeerIngestWait::new(&off).is_none(), "0 turns the wait off");
        Ok(())
    })
    .await
}

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
        for table in COPIED_TABLES {
            let all: Vec<String> = sqlx::query_scalar(
                "SELECT attname::text FROM pg_attribute WHERE attrelid=$1::regclass AND attnum>0 AND NOT attisdropped ORDER BY attname",
            )
            .bind(table)
            .fetch_all(&ledger.pool)
            .await?;
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
/// the pass fails with the cursor where it was, nothing is recorded as a
/// conflict, and the block lands once the wait clears.
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
        let mut failed = false;
        for _ in 0..200 {
            match on_b.pass().await {
                Err(_) => {
                    failed = true;
                    break;
                }
                Ok(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        ensure!(failed, "the contended block never failed to apply");
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_pool_blocks").await? == 0);
        ensure!(count(&pair.b.pool, "SELECT count(*) FROM qbit_prism_peer_sync_conflicts").await? == 0);
        let cursor: Option<i64> = sqlx::query_scalar(
            "SELECT scanned_through FROM qbit_prism_peer_sync_cursors WHERE stream='blocks'",
        )
        .fetch_optional(&pair.b.pool)
        .await?;
        let sync_seq = count(&pair.a.pool, &format!("SELECT sync_seq FROM qbit_pool_blocks WHERE block_hash='{block}'")).await?;
        ensure!(cursor.is_none_or(|cursor| cursor < sync_seq), "the cursor passed the block: {cursor:?}");
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
