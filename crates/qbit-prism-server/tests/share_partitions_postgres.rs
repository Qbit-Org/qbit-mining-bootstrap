//! The readers, probes and maintenance of the partitioned share ledger (#144).
//! PRISM_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55483/postgres cargo test -p qbit-prism-server --test share_partitions_postgres
use anyhow::{ensure, Context, Result};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, verify_audit_bundle_with_ledger_public_key, AcceptedShare, AuditBundle,
    FoundBlock, PayoutPolicy,
};
use qbit_prism_server::{
    api::{router, ApiConfig, ApiState},
    ledger::{Candidate, Ledger, SignerKeys, Snapshot, WindowRef},
    metrics::Metrics,
    partitions, rollups,
};
use qbit_prism_test_gate as gate;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

struct Database {
    admin: PgPool,
    schema: String,
    ledger: Ledger,
}

impl Database {
    async fn open(prefix: &str) -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_{prefix}_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"))
            .append_pair("application_name", &schema);
        let ledger = Ledger::connect(url.as_str(), "partition-test".into(), 8, true).await?;
        Ok(Some(Self {
            admin,
            schema,
            ledger,
        }))
    }

    fn pool(&self) -> &PgPool {
        &self.ledger.pool
    }

    /// The exclusive upper bound of the release partition, where the grid of
    /// lead cells starts.
    async fn first_bound(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT upper_seq FROM qbit_prism_share_partitions WHERE partition_name='qbit_share_ledger_p0'",
        )
        .fetch_one(self.pool())
        .await?)
    }

    async fn partition_rows(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT partition_rows FROM qbit_prism_share_partitioning WHERE singleton",
        )
        .fetch_one(self.pool())
        .await?)
    }

    /// What PostgreSQL holds as attached, which is the authority.
    async fn attached(&self) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT child.relname::text FROM pg_inherits i JOIN pg_class child ON child.oid=i.inhrelid WHERE i.inhparent=to_regclass('qbit_share_ledger') ORDER BY 1",
        )
        .fetch_all(self.pool())
        .await?)
    }

    /// What the catalog records as attached, which is what the maintenance
    /// function and the lead gauge read.
    async fn cataloged(&self) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT partition_name FROM qbit_prism_share_partitions WHERE state='attached' ORDER BY 1",
        )
        .fetch_all(self.pool())
        .await?)
    }

    /// Make the sequence hand out `next` as the next `share_seq`. Sequences are
    /// nontransactional, which is why a test can move one under a live ledger
    /// without touching a row.
    async fn set_next_seq(&self, next: i64) -> Result<()> {
        sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1)")
            .bind(next - 1)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Take a partition out of the ledger, out of the schema and out of the
    /// catalog: `qbit_prism_share_partition_ensure()` reads the catalog for
    /// what is covered and refuses to adopt a relation already holding a name.
    async fn remove_partition(&self, name: &str) -> Result<()> {
        sqlx::raw_sql(&format!(
            "ALTER TABLE qbit_share_ledger DETACH PARTITION {name}; DROP TABLE {name}"
        ))
        .execute(self.pool())
        .await?;
        sqlx::query("DELETE FROM qbit_prism_share_partitions WHERE partition_name=$1")
            .bind(name)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn close(self) -> Result<()> {
        self.ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

fn keys() -> (ManifestSigningKey, ManifestSigningKey) {
    (
        ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap(),
        ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap(),
    )
}

fn share(id: u64, miner: &str) -> AcceptedShare {
    share_with_id(format!("{miner}.rig:{id:064x}"), miner)
}

/// A share whose header hash is `block_hash`, which is what makes it the
/// block's solving share: the suffix of `share_id` is the expression both the
/// landing and migration 016's backfill attribute a block by.
fn solving_share(block_hash: &str, miner: &str) -> AcceptedShare {
    share_with_id(format!("{miner}.rig:{block_hash}"), miner)
}

fn share_with_id(share_id: String, miner: &str) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id,
        miner_id: miner.into(),
        order_key: miner.into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 7,
        network_difficulty: 100,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

fn candidate(snapshot: &Snapshot, nonce: u32) -> Result<(Candidate, AuditBundle)> {
    let (coinbase_key, ledger_key) = keys();
    let bundle = build_audit_bundle(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        &coinbase_key,
        &ledger_key,
    )?;
    let report = verify_audit_bundle_with_ledger_public_key(&bundle, &ledger_key.public_key_hex())?;
    let mut block = vec![0u8; 80];
    block[..4].copy_from_slice(&0x2000_0000u32.to_le_bytes());
    block[4..36].fill(0x22);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&1_800_000_000u32.to_le_bytes());
    block[72..76].copy_from_slice(&0x207f_ffffu32.to_le_bytes());
    block[76..80].copy_from_slice(&nonce.to_le_bytes());
    let mut hash = Sha256::digest(Sha256::digest(&block)).to_vec();
    hash.reverse();
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    Ok((
        Candidate {
            block_hash: hex::encode(hash),
            block_sha256: Candidate::block_digest_hex(&block),
            job_id: "job".into(),
            payout_revision: snapshot.payout_revision,
            window: WindowRef::from_snapshot(snapshot)?,
            bootstrap_share: None,
            found_block: bundle.found_block.clone(),
            payout_policy: bundle.payout_policy.clone(),
            ctv: None,
            audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
            signer_keys: SignerKeys::of(&coinbase_key, &ledger_key),
            leased: false,
            coinbase_suffix_hex: bundle
                .coinbase_script_sig_suffix_hex
                .clone()
                .unwrap_or_else(|| "00".repeat(12)),
            deferred_share: None,
            block_bytes: block,
            as_issued_balances: Vec::new(),
        },
        bundle,
    ))
}

/// Land one confirmed block solved by `miner`, returning its hash and the
/// `share_seq` of the solving share. The solving share is appended after the
/// candidate is built because only then is the block hash its `share_id` has
/// to end with known.
async fn land_block(
    ledger: &Ledger,
    miner: &str,
    nonce: u32,
    filler: u64,
) -> Result<(String, u64)> {
    ledger.append(share(filler, miner), None).await?;
    let snapshot = ledger.snapshot(100).await?;
    let (candidate, bundle) = candidate(&snapshot, nonce)?;
    let hash = candidate.block_hash.clone();
    let solving = ledger
        .append(solving_share(&hash, miner), None)
        .await?
        .share
        .share_seq;
    ledger.enqueue_candidate(candidate).await?;
    let claim = ledger
        .claim_candidate(60)
        .await?
        .context("claimed candidate missing")?
        .with_bundle(bundle);
    ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    ledger.finish_candidate(&claim, true, None).await?;
    Ok((hash, solving))
}

/// Everything the four dashboard read models say about one block's solver.
/// Deliberately without any clock-derived field, so two observations of the
/// same data compare equal.
#[derive(Debug, PartialEq, Eq)]
struct SolverView {
    block_recipient: Value,
    block_worker: Value,
    block_share_difficulty: Value,
    latest_block_recipient: Value,
    latest_block_worker: Value,
    blocks_found_3h: Value,
    blocks_found_reward: Value,
    reward_recipient: Value,
}

async fn solver_view(pool: &PgPool, hash: &str) -> Result<SolverView> {
    let blocks: Value = sqlx::query_scalar(include_str!("../src/api/queries/dashboard_blocks.sql"))
        .bind(50i64)
        .bind(0i64)
        .bind("all")
        .fetch_one(pool)
        .await?;
    let block = blocks["rows"]
        .as_array()
        .context("the blocks page has no rows array")?
        .iter()
        .find(|row| row["hash"] == hash)
        .context("the landed block is absent from the blocks page")?
        .clone();
    let snapshot: Value = sqlx::query_scalar(include_str!(
        "../src/api/queries/dashboard_pool_snapshot.sql"
    ))
    .bind("100")
    .fetch_one(pool)
    .await?;
    let three_hour: Value =
        sqlx::query_scalar(include_str!("../src/api/queries/dashboard_leaderboard.sql"))
            .bind(None::<String>)
            .bind(50i64)
            .bind(0i64)
            .fetch_one(pool)
            .await?;
    let reward: Value = sqlx::query_scalar(include_str!(
        "../src/api/queries/dashboard_reward_leaderboard.sql"
    ))
    .bind("100")
    .bind(None::<String>)
    .bind(None::<String>)
    .bind(50i64)
    .bind(0i64)
    .fetch_one(pool)
    .await?;
    Ok(SolverView {
        block_recipient: block["solver_recipient_id"].clone(),
        block_worker: block["solver_worker_name"].clone(),
        block_share_difficulty: block["solver_share_difficulty"].clone(),
        latest_block_recipient: snapshot["latest_block"]["solver_recipient_id"].clone(),
        latest_block_worker: snapshot["latest_block"]["solver_worker_name"].clone(),
        blocks_found_3h: three_hour["rows"][0]["blocks_found"].clone(),
        blocks_found_reward: reward["rows"][0]["blocks_found_total"].clone(),
        reward_recipient: reward["rows"][0]["recipient_id"].clone(),
    })
}

async fn clear_solver_columns(pool: &PgPool, hash: &str) -> Result<()> {
    sqlx::query("UPDATE qbit_pool_blocks SET solver_miner_id=NULL,solver_share_id=NULL,solver_share_difficulty=NULL,solver_network_difficulty=NULL WHERE block_hash=$1")
        .bind(hash)
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn landing_attributes_the_solver_on_the_block_row_and_readers_survive_a_detach() -> Result<()>
{
    let Some(db) = Database::open("solver").await? else {
        return Ok(());
    };
    let result = async {
        let (hash, solving_seq) = land_block(&db.ledger, "alice", 401, 1).await?;
        let stored: (Option<String>, Option<String>, Option<String>, Option<String>) =
            sqlx::query_as("SELECT solver_miner_id,solver_share_id,solver_share_difficulty::text,solver_network_difficulty::text FROM qbit_pool_blocks WHERE block_hash=$1")
                .bind(&hash)
                .fetch_one(db.pool())
                .await?;
        ensure!(
            stored
                == (
                    Some("alice".into()),
                    Some(format!("alice.rig:{hash}")),
                    Some("7".into()),
                    Some("100".into())
                ),
            "landing did not record the solver on the block row: {stored:?}"
        );

        // Move the sequence into the first lead partition and append there, so
        // the miner still has online shares after the partition holding the
        // solving share is detached. Without them the leaderboards would have
        // no row at all to carry a block count.
        let bound = db.first_bound().await?;
        ensure!(
            i64::try_from(solving_seq)? < bound,
            "the solving share is not in the release partition"
        );
        db.set_next_seq(bound + 1).await?;
        for id in 10..13 {
            db.ledger.append(share(id, "alice"), None).await?;
        }
        let leaf: Option<String> = sqlx::query_scalar(
            "SELECT tableoid::regclass::text FROM qbit_share_ledger WHERE share_seq=$1",
        )
        .bind(bound + 1)
        .fetch_optional(db.pool())
        .await?;
        ensure!(
            leaf.as_deref() == Some("qbit_share_ledger_p1"),
            "the share above the first bound did not land in the lead partition: {leaf:?}"
        );

        let landed = solver_view(db.pool(), &hash).await?;
        ensure!(
            landed.block_recipient == "alice"
                && landed.block_worker == "rig"
                && landed.block_share_difficulty == "7"
                && landed.latest_block_recipient == "alice"
                && landed.latest_block_worker == "rig"
                && landed.blocks_found_3h == 1
                && landed.blocks_found_reward == 1
                && landed.reward_recipient == "alice",
            "the readers do not agree on the solver of a freshly landed block: {landed:?}"
        );

        // EP-COMPAT: a block row whose columns were never written, which is
        // what a pre-016 landing missed by the backfill would look like, is
        // still served by the ledger fallback the columns replaced.
        clear_solver_columns(db.pool(), &hash).await?;
        let fallback = solver_view(db.pool(), &hash).await?;
        ensure!(
            fallback == landed,
            "the ledger fallback does not reproduce the recorded solver: {fallback:?} against {landed:?}"
        );
        sqlx::query("UPDATE qbit_pool_blocks SET solver_miner_id='alice',solver_share_id=$2,solver_share_difficulty=7,solver_network_difficulty=100 WHERE block_hash=$1")
            .bind(&hash)
            .bind(format!("alice.rig:{hash}"))
            .execute(db.pool())
            .await?;

        sqlx::query("ALTER TABLE qbit_share_ledger DETACH PARTITION qbit_share_ledger_p0")
            .execute(db.pool())
            .await?;
        let online: Option<i64> =
            sqlx::query_scalar("SELECT share_seq FROM qbit_share_ledger WHERE share_seq=$1")
                .bind(i64::try_from(solving_seq)?)
                .fetch_optional(db.pool())
                .await?;
        ensure!(online.is_none(), "the detach left the solving share online");
        let detached = solver_view(db.pool(), &hash).await?;
        ensure!(
            detached == landed,
            "detaching the solving share's partition changed what the readers report: {detached:?} against {landed:?}"
        );

        // And the columns are what saved it: without them, with the partition
        // gone, there is nothing left to attribute the block by.
        clear_solver_columns(db.pool(), &hash).await?;
        let lost = solver_view(db.pool(), &hash).await?;
        ensure!(
            lost.block_recipient == "" && lost.block_worker.is_null() && lost.blocks_found_3h == 0,
            "the fixture does not actually remove the solver from the ledger: {lost:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    db.close().await?;
    result
}

async fn latest_evidence_counts(app: &Router) -> Result<(i64, i64)> {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/audit/latest")
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 10_000_000).await?)?;
    ensure!(status == StatusCode::OK, "/audit/latest: {status} {body}");
    Ok((
        body["accepted_share_count"]
            .as_i64()
            .context("accepted_share_count is not an integer")?,
        body["distinct_miner_count"]
            .as_i64()
            .context("distinct_miner_count is not an integer")?,
    ))
}

async fn raw_lifetime_counts(pool: &PgPool) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT count(*),count(DISTINCT miner_id) FROM qbit_share_ledger WHERE accepted",
    )
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn latest_evidence_counts_lifetime_shares_across_the_rollup_watermark() -> Result<()> {
    let Some(db) = Database::open("evidence").await? else {
        return Ok(());
    };
    let result = async {
        land_block(&db.ledger, "alice", 402, 1).await?;
        for id in 20..24 {
            let miner = if id % 2 == 0 { "bob" } else { "alice" };
            db.ledger.append(share(id, miner), None).await?;
        }
        let app = router(ApiState::new(
            db.pool().clone(),
            ApiConfig {
                cache_enabled: false,
                ..ApiConfig::default()
            },
            Arc::new(Metrics::default()),
        ));

        // Before the first sweep there is no watermark row, so both counts are
        // the raw lifetime ones, as they were before #144.
        let raw = raw_lifetime_counts(db.pool()).await?;
        let before = latest_evidence_counts(&app).await?;
        ensure!(
            before == raw,
            "counts before the first rollup sweep are not the raw lifetime counts: {before:?} against {raw:?}"
        );

        // Public-read databases may lack the optional rollup schema. Any
        // missing rollup table must preserve the raw-ledger count fallback.
        for table in [
            "qbit_hashrate_rollup_progress",
            "qbit_hashrate_rollup_pool",
            "qbit_hashrate_rollup_miner",
        ] {
            sqlx::raw_sql(&format!("ALTER TABLE {table} RENAME TO absent_rollup"))
                .execute(db.pool())
                .await?;
            let without_rollups = latest_evidence_counts(&app).await;
            sqlx::raw_sql(&format!("ALTER TABLE absent_rollup RENAME TO {table}"))
                .execute(db.pool())
                .await?;
            ensure!(
                without_rollups? == raw,
                "counts without {table} did not fall back to the raw ledger"
            );
        }

        // After it every share is folded and the raw tail is empty.
        ensure!(
            rollups::advance(db.pool(), 1000).await?.advanced,
            "the rollup sweep did not advance its watermark"
        );
        let swept = latest_evidence_counts(&app).await?;
        ensure!(
            swept == raw,
            "counts after the rollup sweep are not the raw lifetime counts: {swept:?} against {raw:?}"
        );

        // And once the tail moves again, with a miner the rollups have never
        // seen, so the union of rolled and raw miner ids is exercised.
        for id in 30..33 {
            db.ledger.append(share(id, "carol"), None).await?;
        }
        let grown = raw_lifetime_counts(db.pool()).await?;
        ensure!(
            grown == (raw.0 + 3, raw.1 + 1),
            "the fixture did not grow as expected: {grown:?} against {raw:?}"
        );
        let tail = latest_evidence_counts(&app).await?;
        ensure!(
            tail == grown,
            "counts with a raw tail above the watermark are wrong: {tail:?} against {grown:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    db.close().await?;
    result
}

async fn bounded_probe_finds(pool: &PgPool, share_id: &str) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1 AND share_seq>=qbit_prism_share_probe_floor())")
        .bind(share_id)
        .fetch_one(pool)
        .await?)
}

#[tokio::test]
async fn share_id_probes_are_bounded_and_the_vardiff_lookup_is_still_exact() -> Result<()> {
    let Some(db) = Database::open("probe").await? else {
        return Ok(());
    };
    let result = async {
        let fresh = db.ledger.append(share(1, "alice"), None).await?.share;
        // A share appended moments ago is inside the floor, which is what the
        // block-only reconciliation probes in the coordinator rely on.
        ensure!(
            bounded_probe_finds(db.pool(), &fresh.share_id).await?,
            "a share appended moments ago is below the probe floor"
        );
        ensure!(
            db.ledger.share_accepted_at_ms(&fresh.share_id).await? == Some(fresh.accepted_at_ms),
            "the vardiff lookup missed a share appended moments ago"
        );

        // Move the sequence into the fourth cell, p3, and attach the lead the
        // moved sequence needs: the floor is the lower bound of the cell two
        // below the sequence's, p1, which is above that share.
        let width = db.partition_rows().await?;
        db.set_next_seq(3 * width + 10).await?;
        ensure!(
            partitions::ensure(db.pool()).await? > 0,
            "no lead was attached for the moved sequence"
        );
        let floor: i64 = sqlx::query_scalar("SELECT qbit_prism_share_probe_floor()")
            .fetch_one(db.pool())
            .await?;
        ensure!(
            floor == width && floor > i64::try_from(fresh.share_seq)?,
            "the probe floor {floor} is not the lower bound of p1, two cells below the sequence, above share_seq {}",
            fresh.share_seq
        );
        ensure!(
            !bounded_probe_finds(db.pool(), &fresh.share_id).await?,
            "the bounded probe still reaches below the floor, so this fixture proves nothing"
        );
        // An online row below the floor is still answered exactly: the vardiff
        // lookup retries unbounded rather than reporting no evidence.
        ensure!(
            db.ledger.share_accepted_at_ms(&fresh.share_id).await? == Some(fresh.accepted_at_ms),
            "the vardiff lookup lost a share below the probe floor"
        );
        ensure!(
            db.ledger
                .share_accepted_at_ms("alice.rig:absent")
                .await?
                .is_none(),
            "the vardiff lookup invented evidence for an absent share"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    db.close().await?;
    result
}

/// The leaves the bounded probe descends, by name, and how many attached
/// partitions executor-startup pruning removed before it ran.
async fn bounded_probe_plan(pool: &PgPool, share_id: &str) -> Result<(Vec<String>, i64)> {
    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT 1 FROM qbit_share_ledger WHERE share_id=$1 AND share_seq>=qbit_prism_share_probe_floor()",
    )
    .bind(share_id)
    .fetch_all(pool)
    .await?;
    let mut leaves = Vec::new();
    let mut removed = 0;
    for line in &plan {
        if let Some(rest) = line.trim().strip_prefix("Subplans Removed: ") {
            removed = rest.parse()?;
        }
        if let Some(rest) = line.split(" on qbit_share_ledger_p").nth(1) {
            let number: String = rest.chars().take_while(char::is_ascii_digit).collect();
            leaves.push(format!("qbit_share_ledger_p{number}"));
        }
    }
    ensure!(
        !leaves.is_empty(),
        "the probe plan names no partition: {plan:?}"
    );
    // A bitmap scan names a leaf twice, once per node.
    leaves.sort();
    leaves.dedup();
    Ok((leaves, removed))
}

/// The probe floor is the lower bound of the attached partition two below
/// the one the sequence is in, read from the catalog, so a probe descends
/// at most three leaves holding rows plus the empty lead whatever
/// `partition_rows` was when each partition was created. Two current widths
/// below the sequence would not do: after the width grows, that floor spans
/// every narrower partition still attached below the sequence.
#[tokio::test]
async fn the_probe_floor_tracks_the_attached_bounds_after_the_width_grows() -> Result<()> {
    let Some(db) = Database::open("floor").await? else {
        return Ok(());
    };
    let result = async {
        let width = db.partition_rows().await?;
        let start = db.attached().await?;
        ensure!(start.len() == 5, "unexpected attached set: {start:?}");
        // Nothing to prune while the sequence is in the release partition:
        // every leaf is in range, the lead included.
        let early = db.ledger.append(share(1, "alice"), None).await?.share;
        let (leaves, removed) = bounded_probe_plan(db.pool(), &early.share_id).await?;
        ensure!(
            leaves == start && removed == 0,
            "a probe with the sequence in p0 pruned {removed} of {start:?}: {leaves:?}"
        );

        // Quadruple the width, then move the sequence into the first cell of
        // the new grid, p5 = [5W, 9W), and attach its lead.
        sqlx::query(
            "UPDATE qbit_prism_share_partitioning SET partition_rows=partition_rows*4,updated_at=clock_timestamp() WHERE singleton",
        )
        .execute(db.pool())
        .await?;
        db.set_next_seq(5 * width + 10).await?;
        ensure!(
            partitions::ensure(db.pool()).await? > 0,
            "no lead was attached above the widened grid"
        );
        let attached = db.attached().await?;
        ensure!(
            attached.len() > 6 && attached[5] == "qbit_share_ledger_p5",
            "the widened lead is not attached above p0..p4: {attached:?}"
        );
        let landed = db.ledger.append(share(2, "alice"), None).await?.share;
        let seq = i64::try_from(landed.share_seq)?;
        ensure!(seq == 5 * width + 10, "the share landed at {seq}");

        // The floor is the lower bound of p3, two cells below p5. Two
        // current widths below the sequence, 5W + 11 - 8W, is below every
        // bound: that floor would have left the probe descending p0, p1
        // and p2 as well, and more with every narrower partition retained.
        let floor: i64 = sqlx::query_scalar("SELECT qbit_prism_share_probe_floor()")
            .fetch_one(db.pool())
            .await?;
        ensure!(
            floor == 3 * width,
            "the probe floor {floor} is not the lower bound of p3, {}",
            3 * width
        );
        ensure!(
            bounded_probe_finds(db.pool(), &landed.share_id).await?,
            "the share just appended is below the probe floor"
        );
        ensure!(
            !bounded_probe_finds(db.pool(), &early.share_id).await?
                && db.ledger.share_accepted_at_ms(&early.share_id).await?
                    == Some(early.accepted_at_ms),
            "the row in p0 is inside the floor, or the unbounded fallback lost it"
        );
        // Executor-startup pruning removes exactly the three leaves below
        // the floor; the probe descends p3, p4, p5 and the empty lead.
        let (leaves, removed) = bounded_probe_plan(db.pool(), &landed.share_id).await?;
        ensure!(
            removed == 3 && leaves == attached[3..],
            "the probe over {attached:?} pruned {removed} leaves and descends {leaves:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    db.close().await?;
    result
}

#[tokio::test]
async fn an_append_past_the_last_bound_attaches_the_lead_and_lands_the_share() -> Result<()> {
    let Some(db) = Database::open("retry").await? else {
        return Ok(());
    };
    let result = async {
        let bound = db.first_bound().await?;
        for name in db.cataloged().await? {
            if name != "qbit_share_ledger_p0" {
                db.remove_partition(&name).await?;
            }
        }
        ensure!(
            db.attached().await? == vec!["qbit_share_ledger_p0".to_owned()],
            "the release partition is not the only one left"
        );
        db.set_next_seq(bound + 1).await?;
        // The parent has no DEFAULT partition, so PostgreSQL refuses this
        // append until the lead is attached. The append attaches it and
        // retries itself.
        let landed = db.ledger.append(share(1, "alice"), None).await?;
        // The refused attempt drew its `share_seq` from the sequence, and
        // PostgreSQL sequences do not roll back, so the retry lands on the
        // next value above the bound rather than reusing the refused one.
        let seq = i64::try_from(landed.share.share_seq)?;
        ensure!(
            landed.inserted && seq > bound,
            "the retried append did not land the share above the last bound: {landed:?}"
        );
        let cataloged = db.cataloged().await?;
        ensure!(
            cataloged.len() > 1 && cataloged.contains(&"qbit_share_ledger_p1".to_owned()),
            "the retry did not record the partitions it created: {cataloged:?}"
        );
        ensure!(
            db.attached().await? == cataloged,
            "the catalog and pg_inherits disagree after the retry"
        );
        let leaf: String = sqlx::query_scalar(
            "SELECT tableoid::regclass::text FROM qbit_share_ledger WHERE share_seq=$1",
        )
        .bind(seq)
        .fetch_one(db.pool())
        .await?;
        ensure!(
            leaf == "qbit_share_ledger_p1",
            "the retried share landed in {leaf}"
        );
        // The refused attempt wrote nothing, so the retry credits the share
        // once and the sequence is not consumed twice.
        let credited: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
                .bind(&landed.share.share_id)
                .fetch_one(db.pool())
                .await?;
        ensure!(
            credited == 1,
            "the retry credited the share {credited} times"
        );
        ensure!(
            partitions::lead_rows(db.pool())
                .await?
                .context("no lead after the retry")?
                > 0,
            "the retry left no headroom above the sequence"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    db.close().await?;
    result
}

#[tokio::test]
async fn partition_ensure_restores_the_lead_and_creates_nothing_when_it_is_intact() -> Result<()> {
    let Some(db) = Database::open("ensure").await? else {
        return Ok(());
    };
    let result = async {
        // The conversion attaches the release partition and its lead, so a
        // freshly migrated schema has nothing left to create.
        let start = db.attached().await?;
        ensure!(
            start.len() == 5,
            "a converted ledger should carry the release partition and four lead partitions: {start:?}"
        );
        ensure!(
            partitions::ensure(db.pool()).await? == 0,
            "ensure created partitions over an intact lead"
        );
        ensure!(
            db.attached().await? == start,
            "an idempotent ensure changed the attached set"
        );

        let width = db.partition_rows().await?;
        let lead = partitions::lead_rows(db.pool())
            .await?
            .context("a converted ledger reports no lead")?;
        ensure!(
            lead == 5 * width - 1,
            "the lead gauge reads {lead}, not the five attached cells above the first share_seq"
        );

        let last = start.last().context("no partitions")?.clone();
        db.remove_partition(&last).await?;
        ensure!(
            partitions::ensure(db.pool()).await? == 1,
            "ensure did not restore the one missing lead partition"
        );
        ensure!(
            db.attached().await? == start,
            "the restored lead is not the set the conversion left"
        );
        ensure!(
            partitions::ensure(db.pool()).await? == 0,
            "ensure is not idempotent once the lead is restored"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    db.close().await?;
    result
}

/// `partition_rows` is documented as changeable after the conversion. The
/// next partition always starts where the last attached one ends, so the
/// bounds stay contiguous whatever the width; its name comes from the
/// catalog's counter, never from the bound divided by the width, which after
/// a change maps back onto a name that is already taken.
#[tokio::test]
async fn changing_the_partition_width_keeps_names_unique_and_bounds_contiguous() -> Result<()> {
    let Some(db) = Database::open("width").await? else {
        return Ok(());
    };
    let result = async {
        let width = db.partition_rows().await?;
        let start = db.attached().await?;
        ensure!(start.len() == 5, "unexpected attached set: {start:?}");
        let covered: i64 = sqlx::query_scalar(
            "SELECT max(upper_seq) FROM qbit_prism_share_partitions WHERE state='attached'",
        )
        .fetch_one(db.pool())
        .await?;
        ensure!(
            covered == 5 * width,
            "the conversion did not leave five cells of width {width}: covered {covered}"
        );
        // Doubling the width makes the next bound, 5 widths, the second
        // cell of the new grid: 5W / 2W = 2, and qbit_share_ledger_p2 is
        // attached. The lead of four new-width partitions above a sequence
        // still at 1 is 8W, so two partitions are created above 5W.
        sqlx::query(
            "UPDATE qbit_prism_share_partitioning SET partition_rows=partition_rows*2,updated_at=clock_timestamp() WHERE singleton",
        )
        .execute(db.pool())
        .await?;
        let next: i64 = sqlx::query_scalar("SELECT qbit_prism_share_partition_next_number()")
            .fetch_one(db.pool())
            .await?;
        ensure!(next == 5, "the name counter reads {next} over p0..p4");
        ensure!(
            partitions::ensure(db.pool()).await? == 2,
            "ensure did not create the two partitions the wider lead needs"
        );
        let mut expected = start.clone();
        expected.push("qbit_share_ledger_p5".to_owned());
        expected.push("qbit_share_ledger_p6".to_owned());
        ensure!(
            db.attached().await? == expected && db.cataloged().await? == expected,
            "the wider partitions were not named past the existing ones: {:?}",
            db.attached().await?
        );
        let bounds: Vec<(String, Option<i64>, i64)> = sqlx::query_as(
            "SELECT partition_name,lower_seq,upper_seq FROM qbit_prism_share_partitions WHERE state='attached' ORDER BY upper_seq",
        )
        .fetch_all(db.pool())
        .await?;
        for pair in bounds.windows(2) {
            ensure!(
                pair[1].1 == Some(pair[0].2),
                "a gap or an overlap between {} ending at {} and {} starting at {:?}",
                pair[0].0,
                pair[0].2,
                pair[1].0,
                pair[1].1
            );
        }
        ensure!(
            bounds[5] == ("qbit_share_ledger_p5".to_owned(), Some(5 * width), 7 * width)
                && bounds[6] == ("qbit_share_ledger_p6".to_owned(), Some(7 * width), 9 * width),
            "the new partitions are not two double-width cells above the old grid: {bounds:?}"
        );
        ensure!(
            partitions::ensure(db.pool()).await? == 0,
            "ensure is not idempotent after the width change"
        );
        // The parent routes an append past the old grid into the wider cell.
        db.set_next_seq(5 * width + 1).await?;
        let landed = db.ledger.append(share(1, "alice"), None).await?;
        let leaf: String = sqlx::query_scalar(
            "SELECT tableoid::regclass::text FROM qbit_share_ledger WHERE share_seq=$1",
        )
        .bind(i64::try_from(landed.share.share_seq)?)
        .fetch_one(db.pool())
        .await?;
        ensure!(
            leaf == "qbit_share_ledger_p5",
            "the share past the old grid landed in {leaf}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    db.close().await?;
    result
}
