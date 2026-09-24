//! A266's migration 011 and the offer-before-landing lifecycle it installs,
//! against a real PostgreSQL: what 011 does to the rows an older binary left
//! (quarantine, refusals, the row shapes its CHECKs accept), the ledger's
//! lifecycle transitions and lanes, the pre-offer balance fence a pending
//! landing keeps and a post-offer landing relaxes, and the as-issued
//! integrity validator with every finding it reports.
//!
//! Run through test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test offer_lifecycle.
use anyhow::{ensure, Context, Result};
use futures_util::future::LocalBoxFuture;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, verify_audit_bundle_with_ledger_public_key, AcceptedShare, AuditBundle,
    FoundBlock, PayoutPolicy,
};
use qbit_prism_server::ledger::{
    Candidate, CandidateClaim, CandidateState, Ledger, OfferOutcome, SignerKeys, Snapshot,
    WindowRef,
};
use qbit_prism_server::metrics::{collectors, Metrics};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::sync::Mutex;
use tracing::instrument::WithSubscriber;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

const ANCHOR: i64 = 1_700_000_000_000;
const BASE_SCHEMA: &str = include_str!("../../qbit-prism/sql/001_share_ledger.sql");
/// Every native migration before 011, in the runner's order.
const PRE_011: [(i32, &str); 10] = [
    (2, include_str!("../migrations/002_multi_instance.sql")),
    (3, include_str!("../migrations/003_2x_compatibility.sql")),
    (
        4,
        include_str!("../migrations/004_cpfp_retired_funding.sql"),
    ),
    (5, include_str!("../migrations/005_candidate_dispatch.sql")),
    (6, include_str!("../migrations/006_source_schema.sql")),
    (
        7,
        include_str!("../migrations/007_candidate_window_reference.sql"),
    ),
    (
        8,
        include_str!("../migrations/008_prepared_window_reference.sql"),
    ),
    (9, include_str!("../migrations/009_wrap_safe_sessions.sql")),
    (
        10,
        include_str!("../migrations/010_fatal_state_recovery.sql"),
    ),
    (14, include_str!("../migrations/014_policy_transition.sql")),
];
const ALL_VERSIONS: [i32; 19] = [
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
];
/// 011 itself, for the one test that applies its SQL without the runner.
const MIGRATION_011: &str = include_str!("../migrations/011_offer_before_landing.sql");
/// The proof time the lifecycle test enqueues with, and the call time it
/// records: wall clocks in UNIX milliseconds, far outside `i32`.
const PROOF_MS: i64 = 1_700_000_000_123;
const OFFERED_MS: i64 = 1_700_000_000_456;

struct Database {
    fixture: FixtureDatabase,
    pool: PgPool,
    url: String,
    ledgers: Mutex<Vec<PgPool>>,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        // Migration and settlement advisory locks are database-wide, so
        // separate schemas would still make independent tests block each other.
        let fixture = FixtureDatabase::open(&raw, "prism_offer_lifecycle_").await?;
        let pool = match PgPool::connect(&fixture.url).await {
            Ok(pool) => pool,
            Err(error) => return Err(fixture.abandon(error.into()).await),
        };
        Ok(Some(Self {
            url: fixture.url.clone(),
            fixture,
            pool,
            ledgers: Mutex::new(Vec::new()),
        }))
    }

    /// The real schema a 3.x.x database is in before 011 exists: 001 and
    /// every native migration through 010 applied and recorded.
    async fn apply_pre_011(&self) -> Result<()> {
        sqlx::raw_sql(BASE_SCHEMA).execute(&self.pool).await?;
        let mut tx = self.pool.begin().await?;
        sqlx::raw_sql("CREATE TABLE qbit_prism_schema_migrations(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())")
            .execute(&mut *tx).await?;
        for (version, migration) in PRE_011 {
            sqlx::raw_sql(migration).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES($1)")
                .bind(version)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("INSERT INTO qbit_prism_migration_source(source_state,prior_schema_version,migrated_by) VALUES('native',10,'offer-lifecycle-test') ON CONFLICT (singleton) DO NOTHING")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn ledger(&self, id: &str) -> Result<Ledger> {
        let ledger = Ledger::connect(&self.url, id.to_owned(), 4, true).await?;
        self.ledgers.lock().unwrap().push(ledger.pool.clone());
        Ok(ledger)
    }

    async fn versions(&self) -> Result<Vec<i32>> {
        Ok(
            sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    async fn has_column(&self, table: &str, column: &str) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2)")
            .bind(table).bind(column).fetch_one(&self.pool).await?)
    }

    async fn row(&self, block_hash: &str) -> Result<Value> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(o) - 'block_bytes' || jsonb_build_object('has_block',block_bytes IS NOT NULL) FROM qbit_block_candidate_outbox o WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn integrity(&self) -> Result<(u64, u64, Vec<String>)> {
        let report: Value = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
            .fetch_one(&self.pool)
            .await?;
        let reasons: Vec<String> = sqlx::query_scalar(
            "SELECT mismatch_reason FROM qbit_carry_forward_integrity_mismatches()",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok((
            report["mismatch_count"]
                .as_u64()
                .context("mismatch_count")?,
            report["current_drift_count"]
                .as_u64()
                .context("current_drift_count")?,
            reasons,
        ))
    }

    async fn close(self) -> Result<()> {
        for pool in self.ledgers.into_inner().unwrap() {
            pool.close().await;
        }
        self.pool.close().await;
        self.fixture.close(Ok(())).await
    }
}

/// Expire a row's claim lease and make it due again through the fixture pool,
/// as an abandoned lease looks to the next claim.
///
/// The row lock is taken first. A ledger call these tests expect to be
/// refused drops its transaction on the error path, and sqlx only queues that
/// transaction's `ROLLBACK`: it is sent when the pooled connection is
/// returned, from a task of its own, so for a moment after the refusal has
/// returned the transaction can still hold its row lock (`FOR NO KEY UPDATE`
/// from the reservation and outcome writes, `FOR KEY SHARE` from the claim
/// fence of an abandonment). A claim lane selects `FOR UPDATE SKIP LOCKED`
/// and passes over the row while that lock stands, and the plain `UPDATE`
/// alone would not wait for a `FOR KEY SHARE` lock. `FOR UPDATE` conflicts
/// with every row lock, so this commits only once no earlier transaction on
/// the row is open, and the claim that follows finds nothing to skip.
async fn expire_lease(pool: &PgPool, hash: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR UPDATE",
    )
    .bind(hash)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second',next_attempt_at=clock_timestamp() WHERE block_hash=$1")
        .bind(hash)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn run(
    body: impl for<'a> FnOnce(&'a Database) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = body(&db).await;
    let cleanup = db.close().await;
    match (result, cleanup) {
        (Ok(()), cleanup) => cleanup,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("schema cleanup also failed: {cleanup}")))
        }
    }
}

fn keys() -> (ManifestSigningKey, ManifestSigningKey) {
    (
        ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap(),
        ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap(),
    )
}

fn signer_keys() -> SignerKeys {
    let (manifest_key, ledger_key) = keys();
    SignerKeys::of(&manifest_key, &ledger_key)
}

fn other_signer_keys() -> SignerKeys {
    SignerKeys::of(
        &ManifestSigningKey::from_seed_hex(&"52".repeat(32)).unwrap(),
        &ManifestSigningKey::from_seed_hex(&"53".repeat(32)).unwrap(),
    )
}

/// The share `seed_share` writes for `share_seq`, and the one a hand-built
/// snapshot holds.
fn seeded_share(share_seq: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq,
        share_id: format!("share-{share_seq}"),
        miner_id: "miner-a".into(),
        order_key: "a".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: ANCHOR - 1,
        accepted_at_ms: ANCHOR,
        ntime: 100,
        credit_policy: None,
    }
}

async fn seed_share(pool: &PgPool, share_seq: i64) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,accepted,writer_id,writer_epoch) VALUES($1,'share-'||$1,'miner-a','a',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(($2-1)::double precision/1000),100,to_timestamp($2::double precision/1000),NULL,true,'offer-lifecycle-test',0)")
        .bind(share_seq).bind(ANCHOR).execute(pool).await?;
    Ok(())
}

fn appended_share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("worker:{id:064x}"),
        miner_id: "miner-a".into(),
        order_key: "a".into(),
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

/// Two recipients with carried balances from a confirmed block at height 100.
async fn seed_carry(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES(repeat('aa',32),100,repeat('00',32),repeat('ab',32),repeat('ac',32),'confirmed'); INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES(100,repeat('aa',32),'miner-b','b',decode(repeat('22',32),'hex'),500,0,500,0,500,'accrued'),(100,repeat('aa',32),'miner-a','a',decode(repeat('11',32),'hex'),1000,0,1000,0,1000,'accrued');")
        .execute(pool).await?;
    Ok(())
}

/// A slim candidate for `snapshot`'s window beside the bundle it was found
/// on; the candidate carries the snapshot's balances as its as-issued set.
fn candidate_for(snapshot: &Snapshot, nonce: u32) -> Result<(Candidate, AuditBundle)> {
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
    block[..4].copy_from_slice(&0x20000000u32.to_le_bytes());
    block[4..36].fill(0x22);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&1_800_000_000u32.to_le_bytes());
    block[72..76].copy_from_slice(&0x207fffffu32.to_le_bytes());
    block[76..80].copy_from_slice(&nonce.to_le_bytes());
    let mut hash = Sha256::digest(Sha256::digest(&block)).to_vec();
    hash.reverse();
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    let candidate = Candidate {
        block_hash: hex::encode(hash),
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "job".into(),
        payout_revision: snapshot.payout_revision,
        window: WindowRef::from_snapshot(snapshot)?,
        bootstrap_share: None,
        found_block: bundle.found_block.clone(),
        payout_policy: PayoutPolicy::day_one_default(),
        ctv: None,
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: signer_keys(),
        leased: false,
        coinbase_suffix_hex: "00".repeat(12),
        deferred_share: None,
        block_bytes: block,
        as_issued_balances: snapshot.prior_balances.clone(),
    };
    Ok((candidate, bundle))
}

/// Write a pending row exactly as a post-007, pre-011 frontend left it:
/// the document, its digest, the block bytes and the six window columns,
/// with the attempt history and claim the case describes.
async fn insert_pre_011_pending(
    pool: &PgPool,
    candidate: &Candidate,
    attempts: i32,
    last_error: Option<&str>,
    live_claim: bool,
    with_block: bool,
) -> Result<()> {
    let range = candidate.window.shares.context("range")?;
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,window_last_share_seq,window_share_count,window_snapshot_sha256,attempt_count,last_error,claim_token,claim_instance_id,claim_expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,CASE WHEN $13 IS NULL THEN NULL ELSE clock_timestamp()+interval '60 seconds' END)")
        .bind(&candidate.block_hash)
        .bind(serde_json::to_value(candidate)?)
        .bind(hex::encode(Sha256::digest(serde_json::to_vec(candidate)?)))
        .bind(with_block.then(|| candidate.block_bytes.clone()))
        .bind(candidate.window.anchor_ms)
        .bind(hex::encode(candidate.window.prior_balances_digest))
        .bind(range.first_share_seq as i64)
        .bind(range.last_share_seq as i64)
        .bind(range.share_count as i64)
        .bind(hex::encode(range.snapshot_sha256))
        .bind(attempts)
        .bind(last_error)
        .bind(live_claim.then(|| "old-frontend-token".to_string()))
        .bind(live_claim.then(|| "old-frontend".to_string()))
        .execute(pool)
        .await?;
    Ok(())
}

/// A hand-built snapshot over the one seeded share, the reference a pre-011
/// row can carry without any ledger API.
fn seeded_snapshot() -> Snapshot {
    Snapshot {
        anchor_ms: ANCHOR,
        share_seq: 1,
        payout_revision: 0,
        shares: vec![seeded_share(1)],
        prior_balances: Vec::new(),
    }
}

/// The frozen 2.x.x v2.0.2 release files (see tests/fixtures/schema_2x): a
/// #258 source is built from these, never from the live in-tree files.
const FROZEN_2X_001: &str = include_str!("fixtures/schema_2x/001_share_ledger.sql");
const FROZEN_2X_002: &str = include_str!("fixtures/schema_2x/002_candidate_bodies.sql");

/// The pre-011 database an upgrade test starts from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    /// 001 and every native migration through 010, as `apply_pre_011`
    /// builds it: 001's two inline rules, under the names PostgreSQL gave
    /// them.
    Native,
    /// A 2.x.x #258 source migrated by this binary, with 011 undone exactly
    /// as `tests/support/ledger_2x.rs` undoes it: #258's dual-format rule and
    /// 001's state rule are back; the offer columns, the recovery index, the
    /// marker, the version row and the capability are gone. The database a
    /// pre-011 build left on such a source.
    Applied258,
}

/// Operator CHECKs on a pre-011 outbox that 011 must keep byte for byte:
/// neither references `state` or `completed_at`, the columns 011's
/// conservative catalog policy refuses on; the second is NOT VALID and keeps
/// that state. Keeping them proves nothing about later writes: one that
/// rejects a lifecycle write fails that write's transaction as any CHECK
/// would.
const PRESERVED_OPERATOR_CHECKS: [(&str, &str); 2] = [
    (
        "operator_last_error_bounded",
        "CHECK (last_error IS NULL OR length(last_error) < 65536)",
    ),
    (
        "operator_updated_after_created",
        "CHECK (updated_at >= created_at) NOT VALID",
    ),
];

/// 001's state rule under an operator's name: a known rule, replaced by
/// definition whatever it is called.
const RENAMED_LEGACY_RULE: (&str, &str) = (
    "operator_legacy_states",
    "CHECK (state IN ('pending', 'submitted', 'abandoned'))",
);

/// The reviewer's operator CHECKs that reference a lifecycle column: 011
/// does not know them and refuses them by name rather than executing or
/// dropping them.
const LIFECYCLE_OPERATOR_CHECKS: [(&str, &str); 2] = [
    (
        "operator_completed_after_created",
        "CHECK (completed_at IS NULL OR completed_at >= created_at)",
    ),
    (
        "operator_pending_attempts_bounded",
        "CHECK (state <> 'pending' OR attempt_count < 1000000)",
    ),
];

async fn add_checks(pool: &PgPool, checks: &[(&str, &str)]) -> Result<()> {
    for (name, definition) in checks {
        sqlx::query(&format!(
            "ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT {name} {definition}"
        ))
        .execute(pool)
        .await?;
    }
    Ok(())
}

impl Database {
    async fn apply_pre_011_from(&self, source: Source) -> Result<()> {
        match source {
            Source::Native => self.apply_pre_011().await,
            Source::Applied258 => {
                sqlx::raw_sql(FROZEN_2X_001).execute(&self.pool).await?;
                sqlx::raw_sql(FROZEN_2X_002).execute(&self.pool).await?;
                let earlier = self.ledger("earlier-build").await?;
                earlier
                    .heartbeat(qbit_prism_server::ledger::HeartbeatStatus::Stopped)
                    .await?;
                ensure!(
                    self.versions().await? == ALL_VERSIONS,
                    "the runner did not migrate the #258 source"
                );
                // Undo epoch authority with the later lifecycle metadata:
                // this fixture models an old writer, not a mixed-version one.
                sqlx::raw_sql("DELETE FROM qbit_prism_schema_migrations WHERE version=18; DELETE FROM qbit_prism_schema_capabilities WHERE capability='chain_observation_epoch'; ALTER TABLE qbit_prism_cluster DROP COLUMN chain_epoch")
                    .execute(&self.pool).await?;
                sqlx::raw_sql(
                    "ALTER TABLE qbit_prism_instances DROP CONSTRAINT qbit_prism_instances_offer_startup; DELETE FROM qbit_prism_schema_capabilities WHERE capability='instance_offer_startup'; DELETE FROM qbit_prism_schema_migrations WHERE version IN (11,12,15); \
                     DELETE FROM qbit_prism_schema_capabilities WHERE capability IN ('candidate_offer_lifecycle','candidate_orphan_disposition'); \
                     DROP INDEX qbit_block_candidate_outbox_unfinished_idx; \
                     ALTER TABLE qbit_block_candidate_outbox \
                         DROP CONSTRAINT qbit_block_candidate_outbox_offer_check, \
                         DROP CONSTRAINT qbit_block_candidate_outbox_lifecycle_state_check, \
                         DROP CONSTRAINT qbit_block_candidate_outbox_lifecycle_payload_check, \
                         DROP COLUMN proof_observed_at_ms, DROP COLUMN offer_reserved_at, DROP COLUMN offer_reserved_by, \
                         DROP COLUMN offered_at_ms, DROP COLUMN offer_outcome, DROP COLUMN offer_reply; \
                     ALTER TABLE qbit_pool_blocks DROP COLUMN as_issued_audit_sha256; \
                     ALTER TABLE qbit_block_candidate_outbox ADD CHECK (state IN ('pending', 'submitted', 'abandoned')); \
                     ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT qbit_block_candidate_outbox_dual_format_check CHECK ((state = 'pending' AND completed_at IS NULL AND ((storage_version = 1 AND candidate IS NOT NULL AND body_id IS NULL) OR (storage_version = 2 AND candidate IS NULL AND body_id IS NOT NULL))) OR (state IN ('submitted', 'abandoned') AND completed_at IS NOT NULL AND candidate IS NULL AND body_id IS NULL))",
                )
                .execute(&self.pool)
                .await?;
                ensure!(!self.versions().await?.contains(&11));
                Ok(())
            }
        }
    }

    /// Every CHECK on the outbox with its definition, validation state
    /// included, by name.
    async fn checks(&self) -> Result<Vec<(String, String)>> {
        Ok(sqlx::query_as(
            "SELECT conname::text,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='qbit_block_candidate_outbox'::regclass AND contype='c' ORDER BY 1",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn outbox_rows(&self) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o ORDER BY block_hash",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn outbox_columns(&self) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT column_name::text FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='qbit_block_candidate_outbox' ORDER BY ordinal_position",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn offer_capability_declared(&self) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_prism_schema_capabilities WHERE capability='candidate_offer_lifecycle')",
        )
        .fetch_one(&self.pool)
        .await?)
    }
}

// ---------------------------------------------------------------------------
// Migration 011 on the rows an older binary left
// ---------------------------------------------------------------------------

/// A pending row an old frontend attempted may have been offered and lost its
/// reply: 011 quarantines it as a reconciliation row with an unknown outcome,
/// due at once for the recovery lane, which decodes it and never offers it. A
/// never-attempted row stays pending, untouched. Terminal rows are unchanged,
/// the capability is declared, and the lifecycle constraints replace 001's.
#[tokio::test]
async fn migration_011_quarantines_attempted_pending_rows_and_keeps_never_attempted_ones(
) -> Result<()> {
    run(|db| {
        Box::pin(async move {
            db.apply_pre_011().await?;
            seed_share(&db.pool, 1).await?;
            let snapshot = seeded_snapshot();
            let (attempted, _) = candidate_for(&snapshot, 1)?;
            let (fresh, _) = candidate_for(&snapshot, 2)?;
            insert_pre_011_pending(&db.pool, &attempted, 2, Some("window read timed out"), false, true)
                .await?;
            insert_pre_011_pending(&db.pool, &fresh, 0, None, false, true).await?;
            for (tag, state) in [("b2", "submitted"), ("c3", "abandoned")] {
                sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,attempt_count,completed_at) VALUES($1,NULL,$2,$3,1,clock_timestamp()-interval '1 hour')")
                    .bind(tag.repeat(32)).bind(format!("{tag}{}", "0".repeat(62))).bind(state)
                    .execute(&db.pool).await?;
            }
            let before_fresh = db.row(&fresh.block_hash).await?;
            let before_constraints: Vec<(String, String)> = sqlx::query_as(
                "SELECT conname::text,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='qbit_block_candidate_outbox'::regclass AND contype='c' ORDER BY 1",
            )
            .fetch_all(&db.pool)
            .await?;
            let before_terminal: Vec<Value> = sqlx::query_scalar(
                "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o WHERE state IN ('submitted','abandoned') ORDER BY block_hash",
            )
            .fetch_all(&db.pool)
            .await?;

            let ledger = db.ledger("upgrade").await?;
            ensure!(db.versions().await? == ALL_VERSIONS, "011 did not join the applied set");

            let quarantined = db.row(&attempted.block_hash).await?;
            ensure!(quarantined["state"] == "reconciliation", "{quarantined}");
            ensure!(quarantined["offer_outcome"] == "unknown", "{quarantined}");
            ensure!(
                quarantined["offer_reserved_by"] == "migration-011-legacy-quarantine",
                "{quarantined}"
            );
            ensure!(quarantined["offered_at_ms"].is_null(), "a call time was fabricated");
            ensure!(quarantined["claim_token"].is_null(), "the quarantine kept a claim");
            let reason = quarantined["last_error"].as_str().context("no reason")?;
            ensure!(
                reason.contains("quarantined by migration 011")
                    && reason.contains("2 time(s)")
                    && reason.contains("window read timed out"),
                "{reason}"
            );
            ensure!(
                quarantined["has_block"] == true && quarantined["candidate"].is_object(),
                "the quarantine dropped evidence"
            );
            let due: bool = sqlx::query_scalar(
                "SELECT next_attempt_at<=clock_timestamp() FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&attempted.block_hash)
            .fetch_one(&db.pool)
            .await?;
            ensure!(due, "the quarantined row is not due for recovery");

            let after_fresh = db.row(&fresh.block_hash).await?;
            for (column, value) in before_fresh.as_object().context("row")? {
                ensure!(
                    after_fresh.get(column) == Some(value),
                    "011 rewrote {column} of a never-attempted row: {value} became {:?}",
                    after_fresh.get(column)
                );
            }
            ensure!(after_fresh["state"] == "pending" && after_fresh["offer_reserved_at"].is_null());
            let after_terminal: Vec<Value> = sqlx::query_scalar(
                "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o WHERE state IN ('submitted','abandoned') ORDER BY block_hash",
            )
            .fetch_all(&db.pool)
            .await?;
            for (before, after) in before_terminal.iter().zip(&after_terminal) {
                for (column, value) in before.as_object().context("row")? {
                    ensure!(after.get(column) == Some(value), "011 rewrote {column} of a terminal row");
                }
            }
            let capability: i32 = sqlx::query_scalar(
                "SELECT capability_value FROM qbit_prism_schema_capabilities WHERE capability='candidate_offer_lifecycle'",
            )
            .fetch_one(&db.pool)
            .await?;
            ensure!(capability == 1);
            // 001's two inline rules (state, and payload by state) are gone;
            // its column CHECKs on candidate_sha256 and attempt_count and
            // 007's window CHECK are exactly as they were (006 adds
            // storage_version without a CHECK), beside 011's three.
            let constraints: Vec<(String, String)> = sqlx::query_as(
                "SELECT conname::text,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='qbit_block_candidate_outbox'::regclass AND contype='c' ORDER BY 1",
            )
            .fetch_all(&db.pool)
            .await?;
            let names: Vec<&str> = constraints.iter().map(|(name, _)| name.as_str()).collect();
            ensure!(
                names
                    == [
                        "qbit_block_candidate_outbox_attempt_count_check",
                        "qbit_block_candidate_outbox_candidate_sha256_check",
                        "qbit_block_candidate_outbox_lifecycle_payload_check",
                        "qbit_block_candidate_outbox_lifecycle_state_check",
                        "qbit_block_candidate_outbox_offer_check",
                        "qbit_block_candidate_outbox_window_check",
                    ],
                "{constraints:?}"
            );
            for (name, definition) in &constraints {
                let kept = before_constraints.iter().find(|(before, _)| before == name);
                if name.ends_with("_attempt_count_check")
                    || name.ends_with("_candidate_sha256_check")
                    || name.ends_with("_window_check")
                {
                    ensure!(
                        kept.map(|(_, before)| before) == Some(definition),
                        "011 changed {name}: {kept:?} became {definition}"
                    );
                } else {
                    ensure!(kept.is_none(), "{name} predates 011");
                }
            }
            ensure!(db.has_column("qbit_pool_blocks", "as_issued_audit_sha256").await?);

            // The recovery lane decodes the quarantined row, with its
            // lifecycle, before the never-attempted one, which the offer
            // lane still serves as pending. Neither carries an offer time.
            let mut states = Vec::new();
            for _ in 0..2 {
                let claim = ledger
                    .claim_candidate(60)
                    .await?
                    .context("a row was not claimable after 011")?;
                ensure!(claim.lifecycle.proof_observed_at_ms.is_none());
                ensure!(claim.lifecycle.offer.offered_at_ms.is_none());
                states.push((claim.candidate.block_hash.clone(), claim.lifecycle.state, claim.lifecycle.offer.outcome));
            }
            states.sort_by(|a, b| a.0.cmp(&b.0));
            let mut expected = vec![
                (attempted.block_hash.clone(), CandidateState::Reconciliation, Some(OfferOutcome::Unknown)),
                (fresh.block_hash.clone(), CandidateState::Pending, None),
            ];
            expected.sort_by(|a, b| a.0.cmp(&b.0));
            ensure!(states == expected, "{states:?}");
            // A restart applies nothing further and quarantines nothing more.
            let _restarted = db.ledger("restart").await?;
            ensure!(db.versions().await? == ALL_VERSIONS);
            Ok(())
        })
    })
    .await
}

/// An empty outbox does not prove that an old frontend has stopped. Even
/// an arbitrarily old heartbeat may belong to a paused process that resumes.
#[tokio::test]
async fn migration_011_refuses_idle_instances_until_explicitly_stopped() -> Result<()> {
    for status in [
        json!({}),
        Value::Null,
        json!({"state": "starting"}),
        json!({"state": "unknown"}),
        json!({"schema": "qbit.prism.audit-health.v1", "ready": true}),
        json!({"schema": "qbit.prism.audit-health.v1", "ready": false}),
    ] {
        run(move |db| {
            Box::pin(async move {
                db.apply_pre_011().await?;
                sqlx::query("INSERT INTO qbit_prism_instances(instance_id,heartbeat_at,status) VALUES('idle-pre-011',clock_timestamp()-interval '1 day',$1)")
                    .bind(&status).execute(&db.pool).await?;
                let count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_block_candidate_outbox")
                    .fetch_one(&db.pool).await?;
                ensure!(count == 0, "the regression requires no candidate claim");
                let error = db.ledger("refused-idle").await.err()
                    .context("migration 011 accepted an idle pre-011 instance without a stopped marker")?;
                let text = format!("{error:#}");
                ensure!(text.contains("idle-pre-011") && text.contains("drained or stopped"), "{text}");
                ensure!(!db.versions().await?.contains(&11));
                ensure!(!db.has_column("qbit_block_candidate_outbox", "offer_outcome").await?);
                ensure!(!db.has_column("qbit_pool_blocks", "as_issued_audit_sha256").await?);
                let capability: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_prism_schema_capabilities WHERE capability='candidate_offer_lifecycle')")
                    .fetch_one(&db.pool).await?;
                ensure!(!capability, "a refused migration published its capability");
                let retained: Value = sqlx::query_scalar("SELECT status FROM qbit_prism_instances WHERE instance_id='idle-pre-011'")
                    .fetch_one(&db.pool).await?;
                ensure!(retained == status, "refusal changed the instance evidence");
                // Simulate the old frontend's graceful-shutdown heartbeat.
                sqlx::query(r#"UPDATE qbit_prism_instances SET status='{"state":"stopped"}'::jsonb WHERE instance_id='idle-pre-011'"#)
                    .execute(&db.pool).await?;
                sqlx::query(r#"INSERT INTO qbit_prism_instances(instance_id,status) VALUES('drained-pre-011','{"state":"drained"}'::jsonb)"#)
                    .execute(&db.pool).await?;
                let _ledger = db.ledger("post-011").await?;
                ensure!(db.versions().await? == ALL_VERSIONS);
                // Once 011 is applied, ordinary concurrent frontend starts remain allowed.
                let _other = db.ledger("another-post-011").await?;
                Ok(())
            })
        }).await?;
    }
    Ok(())
}

/// A heartbeat that has not committed yet must not be invisible to the
/// migration's instance scan. The table lock waits for that writer first.
#[tokio::test]
async fn migration_011_waits_for_in_flight_instance_registration() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            db.apply_pre_011().await?;
            let mut heartbeat = db.pool.begin().await?;
            sqlx::query(r#"INSERT INTO qbit_prism_instances(instance_id,status) VALUES('registering-pre-011','{"state":"starting"}'::jsonb)"#)
                .execute(&mut *heartbeat).await?;
            let url = db.url.clone();
            let registration = async {
                // The scratch replay every migrate runs first (001 and every native
                // migration) takes seconds under load, so the lock wait is watched
                // for well past it.
                tokio::time::timeout(std::time::Duration::from_secs(20), async {
                    loop {
                        let waiting: bool = sqlx::query_scalar(
                            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='qbit_prism_instances'::regclass AND mode='ShareRowExclusiveLock' AND NOT granted)",
                        ).fetch_one(&db.pool).await?;
                        if waiting { return Ok::<_, anyhow::Error>(()); }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }).await.context("migration did not wait for in-flight instance registration")??;
                heartbeat.commit().await?;
                Ok::<_, anyhow::Error>(())
            };
            let (migration, registration) = tokio::join!(
                Ledger::connect(&url, "migration-waiter".to_owned(), 4, true),
                registration,
            );
            registration?;
            let error = migration.err()
                .context("migration missed the newly committed pre-011 instance")?;
            ensure!(format!("{error:#}").contains("registering-pre-011"), "{error:#}");
            ensure!(!db.versions().await?.contains(&11));
            ensure!(!db.has_column("qbit_block_candidate_outbox", "offer_outcome").await?);
            Ok(())
        })
    }).await
}

/// An old startup can pass its capability check before 011, then queue its
/// initial heartbeat behind the migration. It must fail after the cutover.
#[tokio::test]
async fn migration_011_rejects_pre_011_registration_queued_behind_cutover() -> Result<()> {
    for reuse_stopped_id in [false, true] {
        run(move |db| {
        Box::pin(async move {
            db.apply_pre_011().await?;
            if reuse_stopped_id {
                sqlx::query(r#"INSERT INTO qbit_prism_instances(instance_id,status) VALUES('queued-pre-011','{"state":"stopped"}'::jsonb)"#)
                    .execute(&db.pool).await?;
            }
            // This is the last successful capability read of a pre-011 binary.
            let declared: Vec<(String, i32)> = sqlx::query_as("SELECT capability,capability_value FROM qbit_prism_schema_capabilities")
                .fetch_all(&db.pool).await?;
            ensure!(declared == vec![("candidate_storage_version".into(), 1)]);
            let mut blocker = db.pool.begin().await?;
            sqlx::query("LOCK TABLE qbit_prism_instances IN SHARE MODE")
                .execute(&mut *blocker).await?;
            let wait_for = |mode: &'static str| async move {
                // The scratch replay every migrate runs first (001 and every native
                // migration) takes seconds under load, so the lock wait is watched
                // for well past it.
                tokio::time::timeout(std::time::Duration::from_secs(20), async {
                    loop {
                        let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='qbit_prism_instances'::regclass AND mode=$1 AND NOT granted)")
                            .bind(mode).fetch_one(&db.pool).await?;
                        if waiting { return Ok::<_, anyhow::Error>(()); }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }).await.with_context(|| format!("no queued {mode}"))?
            };
            let late_start = async {
                wait_for("ShareRowExclusiveLock").await?;
                let registration = sqlx::query(r#"INSERT INTO qbit_prism_instances(instance_id,status) VALUES('queued-pre-011','{"state":"starting"}'::jsonb) ON CONFLICT(instance_id) DO UPDATE SET heartbeat_at=clock_timestamp(),status=EXCLUDED.status"#)
                    .execute(&db.pool);
                let release = async {
                    wait_for("RowExclusiveLock").await?;
                    blocker.commit().await?;
                    Ok::<_, anyhow::Error>(())
                };
                let (registration, release) = tokio::join!(registration, release);
                release?;
                let error = registration.err().context("pre-011 registration succeeded after migration committed")?;
                ensure!(error.to_string().contains("qbit_prism_instances_offer_startup"), "{error}");
                Ok::<_, anyhow::Error>(())
            };
            let (migration, late_start) = tokio::join!(db.ledger("post-cutover"), late_start);
            migration?;
            late_start?;
            let old: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_instances WHERE instance_id='queued-pre-011'")
                .fetch_one(&db.pool).await?;
            ensure!(old == i64::from(reuse_stopped_id), "a refused startup changed the instance set");
            if reuse_stopped_id {
                let state: String = sqlx::query_scalar("SELECT status->>'state' FROM qbit_prism_instances WHERE instance_id='queued-pre-011'").fetch_one(&db.pool).await?;
                ensure!(state == "stopped", "failed upsert overwrote shutdown evidence");
            }
            let _other = db.ledger("post-cutover-restart").await?;
            Ok(())
        })
    }).await?;
    }
    Ok(())
}

/// Databases already migrated by an earlier 011 build get the same fence,
/// without rewriting 011 or accepting a selectively restored declaration.
#[tokio::test]
async fn migration_012_fences_existing_011_and_requires_its_declaration() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            db.apply_pre_011().await?;
            sqlx::raw_sql(MIGRATION_011).execute(&db.pool).await?;
            sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(11)")
                .execute(&db.pool).await?;
            sqlx::query(r#"INSERT INTO qbit_prism_instances(instance_id,status) VALUES('earlier-011','{"state":"starting"}'::jsonb)"#)
                .execute(&db.pool).await?;
            let error = db.ledger("blocked-012").await.err().context("012 accepted an earlier live frontend")?;
            ensure!(format!("{error:#}").contains("migration 012 requires"), "{error:#}");
            ensure!(!db.versions().await?.contains(&12));
            let installed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='qbit_prism_instances'::regclass AND conname='qbit_prism_instances_offer_startup')")
                .fetch_one(&db.pool).await?;
            ensure!(!installed, "refusal installed part of the startup fence");
            sqlx::query(r#"UPDATE qbit_prism_instances SET status='{"state":"stopped"}'::jsonb WHERE instance_id='earlier-011'"#)
                .execute(&db.pool).await?;
            let _ledger = db.ledger("upgraded-012").await?;
            ensure!(db.versions().await? == ALL_VERSIONS);
            let marker: Value = sqlx::query_scalar("SELECT status->'candidate_offer_lifecycle' FROM qbit_prism_instances WHERE instance_id='upgraded-012'")
                .fetch_one(&db.pool).await?;
            ensure!(marker == json!(1));
            for marker in [Value::Null, json!(0), json!(2), json!("1")] {
                let error = sqlx::query("INSERT INTO qbit_prism_instances(instance_id,status) VALUES('invalid-startup',$1)")
                    .bind(json!({"state":"starting", "candidate_offer_lifecycle":marker}))
                    .execute(&db.pool).await.err().context("invalid startup protocol was accepted")?;
                ensure!(error.to_string().contains("qbit_prism_instances_offer_startup"), "{error}");
            }
            for value in [None, Some(0), Some(2)] {
                sqlx::query("DELETE FROM qbit_prism_schema_capabilities WHERE capability='instance_offer_startup'")
                    .execute(&db.pool).await?;
                if let Some(value) = value {
                    sqlx::query("INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('instance_offer_startup',$1)")
                        .bind(value).execute(&db.pool).await?;
                }
                for initialize in [false, true] {
                    let error = Ledger::connect(&db.url, "missing-fence-declaration".into(), 4, initialize)
                        .await.err().context("startup accepted an invalid 012 declaration")?;
                    ensure!(format!("{error:#}").contains("instance_offer_startup"), "{error:#}");
                }
            }
            sqlx::query("UPDATE qbit_prism_schema_capabilities SET capability_value=1 WHERE capability='instance_offer_startup'")
                .execute(&db.pool).await?;
            let _restart = db.ledger("repaired-012").await?;
            Ok(())
        })
    }).await
}

/// 011 refuses, before any DDL, a pending row whose claim is still live (an
/// old frontend may be mid-offer) and an attempted pending row without the
/// evidence a quarantined row must carry; once the claim expired or the row
/// was drained, the same binary applies it.
#[tokio::test]
async fn migration_011_refuses_a_live_claim_and_an_attempted_row_without_evidence() -> Result<()> {
    for case in ["live-claim", "bare-attempt"] {
        run(move |db| {
            Box::pin(async move {
                db.apply_pre_011().await?;
                seed_share(&db.pool, 1).await?;
                let (candidate, _) = candidate_for(&seeded_snapshot(), 3)?;
                match case {
                    "live-claim" => {
                        insert_pre_011_pending(&db.pool, &candidate, 1, None, true, true).await?
                    }
                    _ => insert_pre_011_pending(&db.pool, &candidate, 1, None, false, false).await?,
                }
                let error = db
                    .ledger("refused")
                    .await
                    .err()
                    .with_context(|| format!("{case}: 011 was applied"))?;
                let text = format!("{error:#}");
                ensure!(text.contains(&candidate.block_hash), "{case}: the refusal did not name the row: {text}");
                let expected = if case == "live-claim" {
                    "hold a live claim"
                } else {
                    "without a block and window reference"
                };
                ensure!(text.contains(expected), "{case}: {text}");
                ensure!(text.contains("nothing was changed"), "{case}: {text}");
                ensure!(!db.versions().await?.contains(&11), "{case}: version 11 was recorded");
                ensure!(
                    !db.has_column("qbit_block_candidate_outbox", "offer_outcome").await?,
                    "{case}: 011's columns survived the refusal"
                );
                ensure!(
                    !db.has_column("qbit_pool_blocks", "as_issued_audit_sha256").await?,
                    "{case}: 011's marker survived the refusal"
                );
                let capability: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM qbit_prism_schema_capabilities WHERE capability='candidate_offer_lifecycle')",
                )
                .fetch_one(&db.pool)
                .await?;
                ensure!(!capability, "{case}: the capability was declared");
                let row = db.row(&candidate.block_hash).await?;
                ensure!(row["state"] == "pending", "{case}: {row}");
                // The operator's remedy, then the same binary applies 011.
                match case {
                    "live-claim" => {
                        sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE block_hash=$1")
                            .bind(&candidate.block_hash).execute(&db.pool).await?;
                    }
                    _ => {
                        sqlx::query("UPDATE qbit_block_candidate_outbox SET state='submitted',candidate=NULL,window_anchor_ms=NULL,window_prior_balances_sha256=NULL,window_first_share_seq=NULL,window_last_share_seq=NULL,window_share_count=NULL,window_snapshot_sha256=NULL,completed_at=clock_timestamp() WHERE block_hash=$1")
                            .bind(&candidate.block_hash).execute(&db.pool).await?;
                    }
                }
                let _ledger = db.ledger("applied").await?;
                ensure!(db.versions().await? == ALL_VERSIONS, "{case}: 011 did not apply after the remedy");
                let row = db.row(&candidate.block_hash).await?;
                let expected_state = if case == "live-claim" { "reconciliation" } else { "submitted" };
                ensure!(row["state"] == expected_state, "{case}: {row}");
                Ok(())
            })
        })
        .await
        .with_context(|| format!("case {case}"))?;
    }
    Ok(())
}

/// 011 replaces only the lifecycle rules it knows, by definition rather
/// than by name: 001's two inline rules on a native database, #258's
/// dual-format rule on a database that came through 002, and 001's state
/// rule even under an operator's name. Every other CHECK on the outbox that
/// references neither `state` nor `completed_at` survives byte for byte,
/// validation state included: the operator CHECKs here, 001's column CHECKs,
/// 007's window CHECK and #258's storage_version CHECK. The quarantine, the
/// untouched rows and the lifecycle itself work under these kept
/// constraints, and no probe constraint or row is left behind.
#[tokio::test]
async fn migration_011_replaces_only_the_known_lifecycle_rules_and_keeps_operator_checks(
) -> Result<()> {
    for source in [Source::Native, Source::Applied258] {
        run(move |db| {
            Box::pin(async move {
                db.apply_pre_011_from(source).await?;
                seed_share(&db.pool, 1).await?;
                let snapshot = seeded_snapshot();
                let (attempted, _) = candidate_for(&snapshot, 1)?;
                let (fresh, _) = candidate_for(&snapshot, 2)?;
                insert_pre_011_pending(&db.pool, &attempted, 2, Some("window read timed out"), false, true)
                    .await?;
                insert_pre_011_pending(&db.pool, &fresh, 0, None, false, true).await?;
                for (tag, state) in [("b2", "submitted"), ("c3", "abandoned")] {
                    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,attempt_count,created_at,updated_at,completed_at) VALUES($1,NULL,$2,$3,1,clock_timestamp()-interval '2 hours',clock_timestamp()-interval '1 hour',clock_timestamp()-interval '1 hour')")
                        .bind(tag.repeat(32)).bind(format!("{tag}{}", "0".repeat(62))).bind(state)
                        .execute(&db.pool).await?;
                }
                add_checks(&db.pool, &PRESERVED_OPERATOR_CHECKS).await?;
                add_checks(&db.pool, &[RENAMED_LEGACY_RULE]).await?;
                let before = db.checks().await?;
                let before_rows = db.outbox_rows().await?;
                let replaced: &[&str] = match source {
                    Source::Native => &[
                        RENAMED_LEGACY_RULE.0,
                        "qbit_block_candidate_outbox_check",
                        "qbit_block_candidate_outbox_state_check",
                    ],
                    Source::Applied258 => &[
                        RENAMED_LEGACY_RULE.0,
                        "qbit_block_candidate_outbox_dual_format_check",
                        "qbit_block_candidate_outbox_state_check",
                    ],
                };
                for name in replaced {
                    ensure!(
                        before.iter().any(|(found, _)| found == name),
                        "{source:?}: {name} is not on the pre-011 outbox: {before:?}"
                    );
                }

                let ledger = db.ledger("upgrade").await?;
                ensure!(db.versions().await? == ALL_VERSIONS, "{source:?}: 011 did not join the applied set");
                let after = db.checks().await?;
                for (name, definition) in &before {
                    if replaced.contains(&name.as_str()) {
                        ensure!(
                            !after.iter().any(|(found, _)| found == name),
                            "{source:?}: 011 kept the known rule {name}"
                        );
                    } else {
                        ensure!(
                            after.iter().any(|(found, kept)| found == name && kept == definition),
                            "{source:?}: 011 changed or dropped {name} ({definition}): {after:?}"
                        );
                    }
                }
                let added: Vec<&str> = after
                    .iter()
                    .filter(|(name, _)| !before.iter().any(|(known, _)| known == name))
                    .map(|(name, _)| name.as_str())
                    .collect();
                ensure!(
                    added
                        == [
                            "qbit_block_candidate_outbox_lifecycle_payload_check",
                            "qbit_block_candidate_outbox_lifecycle_state_check",
                            "qbit_block_candidate_outbox_offer_check",
                        ],
                    "{source:?}: 011 added {added:?}"
                );
                ensure!(after.len() == before.len(), "{source:?}: {after:?}");
                let payload = after
                    .iter()
                    .find(|(name, _)| name == "qbit_block_candidate_outbox_lifecycle_payload_check")
                    .map(|(_, definition)| definition.as_str())
                    .context("no payload rule")?;
                ensure!(
                    payload.contains("body_id") == (source == Source::Applied258),
                    "{source:?}: the payload rule is not the one for this source: {payload}"
                );
                let not_valid = after
                    .iter()
                    .find(|(name, _)| name == "operator_updated_after_created")
                    .context("the NOT VALID operator CHECK is gone")?;
                ensure!(not_valid.1.ends_with("NOT VALID"), "{source:?}: 011 validated {not_valid:?}");

                let after_rows = db.outbox_rows().await?;
                ensure!(after_rows.len() == before_rows.len(), "{source:?}: 011 changed the row count");
                for (before_row, after_row) in before_rows.iter().zip(&after_rows) {
                    ensure!(before_row["block_hash"] == after_row["block_hash"]);
                    if before_row["block_hash"] == attempted.block_hash {
                        ensure!(
                            after_row["state"] == "reconciliation" && after_row["offer_outcome"] == "unknown",
                            "{source:?}: the attempted row was not quarantined: {after_row}"
                        );
                        continue;
                    }
                    for (column, value) in before_row.as_object().context("row")? {
                        ensure!(
                            after_row.get(column) == Some(value),
                            "{source:?}: 011 rewrote {column} of {}: {value} became {:?}",
                            before_row["block_hash"],
                            after_row.get(column)
                        );
                    }
                }

                // The lifecycle runs under the kept constraints: the fresh row
                // is reserved, offered and reconciled, and a state 011 does
                // not know is still refused.
                let mut pending = None;
                for _ in 0..2 {
                    let claim = ledger.claim_candidate(60).await?.context("a row was not claimable after 011")?;
                    if claim.lifecycle.state == CandidateState::Pending {
                        pending = Some(claim);
                    } else {
                        ensure!(claim.candidate.block_hash == attempted.block_hash);
                    }
                }
                let claim = pending.context("the never-attempted row was not claimed pending")?;
                ensure!(claim.candidate.block_hash == fresh.block_hash);
                ledger.reserve_offer(&claim).await?;
                ledger.record_offer(&claim, OFFERED_MS, OfferOutcome::Accepted, None).await?;
                ledger.reconcile_candidate(&claim, "landing failed after acceptance").await?;
                let row = db.row(&fresh.block_hash).await?;
                ensure!(row["state"] == "reconciliation" && row["offer_outcome"] == "accepted", "{row}");
                let foreign = sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,completed_at) VALUES(repeat('d4',32),NULL,repeat('d4',32),'rejected',clock_timestamp())")
                    .execute(&db.pool)
                    .await;
                ensure!(foreign.is_err(), "{source:?}: a state 011 does not know was accepted");
                Ok(())
            })
        })
        .await
        .with_context(|| format!("source {source:?}"))?;
    }
    Ok(())
}

/// A CHECK 011 does not know that references `state` or `completed_at` is
/// refused by name, every offender at once, and so is a constraint that
/// carries one of 011's names with another definition (the runner's
/// gap-collision check refuses that one before 011's own SQL runs). The
/// refusal changes nothing: no version, column, capability, marker or
/// quarantine, and every constraint and row exactly as it was. Once the
/// operator removes the offenders, the same binary applies 011 and keeps
/// the unrelated operator CHECK byte for byte.
#[tokio::test]
async fn migration_011_refuses_unknown_checks_the_lifecycle_cannot_satisfy_and_changes_nothing(
) -> Result<()> {
    for case in ["lifecycle-columns", "reused-name"] {
        run(move |db| {
            Box::pin(async move {
                db.apply_pre_011().await?;
                seed_share(&db.pool, 1).await?;
                let (attempted, _) = candidate_for(&seeded_snapshot(), 3)?;
                insert_pre_011_pending(
                    &db.pool,
                    &attempted,
                    2,
                    Some("window read timed out"),
                    false,
                    true,
                )
                .await?;
                let (control, _) = PRESERVED_OPERATOR_CHECKS[0];
                let offenders: &[(&str, &str)] = match case {
                    "lifecycle-columns" => &LIFECYCLE_OPERATOR_CHECKS,
                    _ => &[(
                        "qbit_block_candidate_outbox_offer_check",
                        "CHECK (attempt_count < 10)",
                    )],
                };
                add_checks(&db.pool, &PRESERVED_OPERATOR_CHECKS[..1]).await?;
                add_checks(&db.pool, offenders).await?;
                let before = db.checks().await?;
                let before_rows = db.outbox_rows().await?;
                let before_columns = db.outbox_columns().await?;
                let before_versions = db.versions().await?;

                let error = db
                    .ledger("refused")
                    .await
                    .err()
                    .with_context(|| format!("{case}: 011 was applied"))?;
                let text = format!("{error:#}");
                for (name, _) in offenders {
                    ensure!(
                        text.contains(name),
                        "{case}: the refusal did not name {name}: {text}"
                    );
                }
                ensure!(
                    !text.contains(control),
                    "{case}: the refusal blamed {control}: {text}"
                );
                ensure!(
                    text.to_lowercase().contains("nothing was changed"),
                    "{case}: {text}"
                );
                let detail = if case == "lifecycle-columns" {
                    "reference its lifecycle columns state or completed_at"
                } else {
                    "already holds objects this missing migration creates"
                };
                ensure!(text.contains(detail), "{case}: {text}");
                ensure!(
                    db.versions().await? == before_versions,
                    "{case}: the versions changed"
                );
                ensure!(
                    !before_versions.contains(&11),
                    "{case}: version 11 was recorded"
                );
                ensure!(
                    db.outbox_columns().await? == before_columns,
                    "{case}: 011's columns survived the refusal"
                );
                ensure!(
                    !db.has_column("qbit_pool_blocks", "as_issued_audit_sha256")
                        .await?,
                    "{case}: 011's marker survived the refusal"
                );
                ensure!(
                    !db.offer_capability_declared().await?,
                    "{case}: the capability was declared"
                );
                ensure!(
                    db.checks().await? == before,
                    "{case}: the constraints changed: {:?}",
                    db.checks().await?
                );
                ensure!(
                    db.outbox_rows().await? == before_rows,
                    "{case}: the rows changed"
                );

                // The operator's remedy, then the same binary applies 011.
                for (name, _) in offenders {
                    sqlx::query(&format!(
                        "ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT {name}"
                    ))
                    .execute(&db.pool)
                    .await?;
                }
                let _ledger = db.ledger("applied").await?;
                ensure!(
                    db.versions().await? == ALL_VERSIONS,
                    "{case}: 011 did not apply after the remedy"
                );
                let after = db.checks().await?;
                let kept = before
                    .iter()
                    .find(|(name, _)| name == control)
                    .context("control")?;
                ensure!(
                    after.contains(kept),
                    "{case}: 011 dropped or changed {kept:?}: {after:?}"
                );
                ensure!(
                    !after.iter().any(|(name, _)| name.contains("011_probe")),
                    "{case}: {after:?}"
                );
                let row = db.row(&attempted.block_hash).await?;
                ensure!(row["state"] == "reconciliation", "{case}: {row}");
                ensure!(
                    db.outbox_rows().await?.len() == before_rows.len(),
                    "{case}: 011 changed the row count"
                );
                Ok(())
            })
        })
        .await
        .with_context(|| format!("case {case}"))?;
    }
    Ok(())
}

/// 011's own SQL, applied directly, upholds its stated order: a constraint
/// under one of 011's names is judged by that name first, so a legacy rule
/// hiding under it is refused as conflicting rather than recognised and
/// dropped, and the transaction changes nothing. Through the runner the
/// gap-collision gate refuses such a database earlier; this pins the SQL.
#[tokio::test]
async fn migration_011_sql_refuses_a_legacy_rule_under_its_own_name_before_matching_it(
) -> Result<()> {
    run(|db| {
        Box::pin(async move {
            db.apply_pre_011().await?;
            sqlx::raw_sql(
                "ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT qbit_block_candidate_outbox_state_check; \
                 ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT qbit_block_candidate_outbox_lifecycle_state_check CHECK (state IN ('pending', 'submitted', 'abandoned'))",
            )
            .execute(&db.pool)
            .await?;
            let before = db.checks().await?;
            let before_columns = db.outbox_columns().await?;
            let mut tx = db.pool.begin().await?;
            let error = sqlx::raw_sql(MIGRATION_011)
                .execute(&mut *tx)
                .await
                .err()
                .context("011 accepted a legacy rule under its own name")?;
            tx.rollback().await.ok();
            let text = format!("{error:#}");
            ensure!(
                text.contains("qbit_block_candidate_outbox_lifecycle_state_check")
                    && text.contains("exists with a definition 011 did not create")
                    && text.contains("nothing was changed"),
                "{text}"
            );
            ensure!(db.checks().await? == before, "{:?}", db.checks().await?);
            ensure!(db.outbox_columns().await? == before_columns);
            ensure!(!db.versions().await?.contains(&11));
            Ok(())
        })
    })
    .await
}

/// One row shape the lifecycle CHECKs are tried on: `(state, candidate,
/// block, window, completed, reserved, offered_at, outcome, reply,
/// last_error)` and whether PostgreSQL must accept it.
type Shape = (
    &'static str,
    bool,
    bool,
    bool,
    bool,
    bool,
    bool,
    Option<&'static str>,
    Option<&'static str>,
    Option<&'static str>,
    bool,
);

/// The lifecycle CHECKs accept exactly the lifecycle's shapes. Counts are
/// used in the constraints because a CHECK that evaluates to NULL passes:
/// each NULL-where-required case here is a row PostgreSQL must refuse.
#[tokio::test]
async fn outbox_lifecycle_checks_accept_exactly_the_lifecycle_shapes() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let _ledger = db.ledger("checks").await?;
            let sha = "11".repeat(32);
            // (state, candidate, block, window, completed, reserved, offered_at, outcome, reply, last_error) -> accepted?
            let cases: Vec<Shape> = vec![
                ("pending", true, false, false, false, false, false, None, None, None, true),
                ("pending", true, true, true, false, false, false, None, None, None, true),
                ("pending", false, true, true, false, false, false, None, None, None, false),
                ("pending", true, true, true, false, true, false, None, None, None, false),
                ("pending", true, true, true, true, false, false, None, None, None, false),
                ("offer_reserved", true, true, true, false, true, false, None, None, None, true),
                ("offer_reserved", true, false, true, false, true, false, None, None, None, false),
                ("offer_reserved", true, true, false, false, true, false, None, None, None, false),
                ("offer_reserved", true, true, true, false, false, false, None, None, None, false),
                ("offer_reserved", true, true, true, false, true, false, Some("accepted"), None, None, false),
                ("offered", true, true, true, false, true, true, Some("accepted"), None, None, true),
                ("offered", true, true, true, false, true, true, Some("rejected"), Some("duplicate"), None, true),
                ("offered", true, true, true, false, true, true, Some("rejected"), None, None, false),
                ("offered", true, true, true, false, true, true, None, None, None, false),
                ("offered", true, true, true, false, true, false, Some("accepted"), None, None, false),
                ("offered", true, true, true, false, true, true, Some("maybe"), None, None, false),
                ("reconciliation", true, true, true, false, true, false, Some("unknown"), None, Some("delivery unknown"), true),
                ("reconciliation", true, true, true, false, true, true, Some("accepted"), None, Some("landing failed"), true),
                ("reconciliation", true, true, true, false, true, false, None, None, Some("reason"), false),
                ("reconciliation", true, true, true, false, true, false, Some("unknown"), None, None, false),
                ("reconciliation", true, true, true, false, true, false, Some("unknown"), None, Some("   "), false),
                ("submitted", false, false, false, true, false, false, None, None, None, true),
                ("submitted", false, false, false, true, true, true, Some("accepted"), None, None, true),
                ("submitted", true, false, false, true, false, false, None, None, None, false),
                ("submitted", false, true, false, true, false, false, None, None, None, false),
                ("abandoned", false, false, false, true, false, false, None, None, Some("superseded"), true),
                ("abandoned", false, false, false, true, true, false, None, None, None, false),
                // 015: an orphaned row is terminal, its payload cleared like
                // every terminal row's, and keeps its offer record and reason.
                ("orphaned", false, false, false, true, true, true, Some("accepted"), None, Some("proven orphan"), true),
                ("orphaned", false, false, false, true, true, true, Some("rejected"), Some("duplicate"), Some("proven orphan"), true),
                ("orphaned", false, false, false, true, true, true, Some("unknown"), None, Some("proven orphan"), true),
                // A reservation whose call was lost stays unknown without a call time.
                ("orphaned", false, false, false, true, true, false, Some("unknown"), None, Some("proven orphan"), true),
                // No payload survives the disposition.
                ("orphaned", true, false, false, true, true, true, Some("accepted"), None, Some("proven orphan"), false),
                ("orphaned", false, true, false, true, true, true, Some("accepted"), None, Some("proven orphan"), false),
                ("orphaned", false, false, true, true, true, true, Some("accepted"), None, Some("proven orphan"), false),
                ("orphaned", true, true, true, true, true, true, Some("accepted"), None, Some("proven orphan"), false),
                ("orphaned", false, false, false, false, true, true, Some("accepted"), None, Some("proven orphan"), false),
                // The offer record and the reason are required.
                ("orphaned", false, false, false, true, false, false, None, None, Some("proven orphan"), false),
                ("orphaned", false, false, false, true, true, false, None, None, Some("proven orphan"), false),
                ("orphaned", false, false, false, true, true, true, Some("accepted"), None, None, false),
                ("orphaned", false, false, false, true, true, true, Some("accepted"), None, Some("   "), false),
                // A node answer without its call time would be an invented submission.
                ("orphaned", false, false, false, true, true, false, Some("accepted"), None, Some("proven orphan"), false),
                ("orphaned", false, false, false, true, true, false, Some("rejected"), Some("duplicate"), Some("proven orphan"), false),
                ("rejected", false, false, false, true, false, false, None, None, None, false),
            ];
            for (index, (state, candidate, block, window, completed, reserved, offered_at, outcome, reply, last_error, accepted)) in cases.iter().enumerate() {
                let hash = format!("{index:064x}");
                let result = sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256,state,completed_at,offer_reserved_at,offer_reserved_by,offered_at_ms,offer_outcome,offer_reply,last_error) VALUES($1,CASE WHEN $2 THEN '{}'::jsonb END,$3,CASE WHEN $4 THEN '\\x00'::bytea END,CASE WHEN $5 THEN 1::bigint END,CASE WHEN $5 THEN repeat('00',32) END,$6,CASE WHEN $7 THEN clock_timestamp() END,CASE WHEN $8 THEN clock_timestamp() END,CASE WHEN $8 THEN 'frontend' END,CASE WHEN $9 THEN 1::bigint END,$10,$11,$12)")
                    .bind(&hash).bind(candidate).bind(&sha).bind(block).bind(window).bind(state).bind(completed).bind(reserved).bind(offered_at).bind(outcome).bind(reply).bind(last_error)
                    .execute(&db.pool).await;
                ensure!(
                    result.is_ok() == *accepted,
                    "case {index} ({state}, candidate={candidate}, block={block}, window={window}, completed={completed}, reserved={reserved}, offered_at={offered_at}, outcome={outcome:?}, reply={reply:?}, last_error={last_error:?}): {result:?}"
                );
            }
            Ok(())
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// The lifecycle through the ledger
// ---------------------------------------------------------------------------

/// Every unfinished state is claimable once its lease is gone, keeps its
/// evidence and its window reference (the retention predicate and the
/// candidates gauge count it), can only be reserved from pending, can never
/// be abandoned after the reservation, blocks a signer rotation, and backs
/// off longer in reconciliation.
#[tokio::test]
async fn every_unfinished_state_is_claimable_retained_and_reserved_only_once() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("lifecycle").await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            let (candidate, bundle) = candidate_for(&snapshot, 10)?;
            let hash = candidate.block_hash.clone();
            ensure!(ledger.enqueue_candidate_observed(candidate.clone(), Some(PROOF_MS)).await?);
            let metrics = Metrics::default();
            let live_rows = || async {
                let (retained, gauge): (i64, i64) = sqlx::query_as(
                    "SELECT (SELECT count(*) FROM qbit_block_candidate_outbox WHERE window_anchor_ms IS NOT NULL),(SELECT count(*) FROM qbit_block_candidate_outbox WHERE state IN ('pending','offer_reserved','offered','reconciliation'))",
                )
                .fetch_one(&db.pool)
                .await?;
                let collected = collectors::database(&db.pool, &metrics).await?.candidates;
                Ok::<_, anyhow::Error>((retained, gauge, collected))
            };

            // pending
            let claim = ledger.claim_candidate(60).await?.context("pending not claimable")?;
            ensure!(claim.lifecycle.state == CandidateState::Pending);
            ensure!(claim.lifecycle.proof_observed_at_ms == Some(PROOF_MS));
            ensure!(ledger.record_offer(&claim, 1, OfferOutcome::Accepted, None).await.is_err(), "an outcome was recorded without a reservation");
            ensure!(ledger.reconcile_candidate(&claim, "reason").await.is_err(), "a pending row was reconciled");
            ensure!(live_rows().await? == (1, 1, 1));

            // offer_reserved
            ledger.reserve_offer(&claim).await?;
            ensure!(ledger.reserve_offer(&claim).await.is_err(), "the row was reserved twice");
            let row = db.row(&hash).await?;
            ensure!(row["state"] == "offer_reserved" && row["offer_reserved_by"] == "lifecycle", "{row}");
            ensure!(ledger.finish_candidate(&claim, false, Some("superseded")).await.is_err(), "a reserved row was abandoned");
            ensure!(live_rows().await? == (1, 1, 1));
            expire_lease(&db.pool, &hash).await?;
            let claim = ledger.claim_candidate(60).await?.context("reserved not claimable")?;
            ensure!(claim.lifecycle.state == CandidateState::OfferReserved);
            ensure!(claim.lifecycle.offer.reserved_by.as_deref() == Some("lifecycle"));
            ensure!(claim.lifecycle.offer.outcome.is_none() && claim.lifecycle.offer.offered_at_ms.is_none());
            ensure!(ledger.reserve_offer(&claim).await.is_err(), "a recovered reservation was reserved again");

            // offered
            ledger.record_offer(&claim, OFFERED_MS, OfferOutcome::Rejected, Some("duplicate")).await?;
            ensure!(ledger.record_offer(&claim, 2, OfferOutcome::Accepted, None).await.is_err(), "the outcome was recorded twice");
            ensure!(ledger.finish_candidate(&claim, false, Some("superseded")).await.is_err(), "an offered row was abandoned");
            expire_lease(&db.pool, &hash).await?;
            let claim = ledger.claim_candidate(60).await?.context("offered not claimable")?;
            ensure!(claim.lifecycle.state == CandidateState::Offered);
            ensure!(claim.lifecycle.offer.outcome == Some(OfferOutcome::Rejected));
            ensure!(claim.lifecycle.offer.reply.as_deref() == Some("duplicate"));
            ensure!(claim.lifecycle.offer.offered_at_ms == Some(OFFERED_MS));
            ensure!(live_rows().await? == (1, 1, 1));
            // A rotation to other keys is refused while the offered row
            // stores this frontend's.
            let error = ledger.configure("fingerprint", &other_signer_keys()).await.err().context("rotation accepted")?;
            ensure!(format!("{error:#}").contains(&hash), "{error:#}");

            // reconciliation, with the longer backoff
            ledger.reconcile_candidate(&claim, "node rejected the offer: duplicate").await?;
            let row = db.row(&hash).await?;
            ensure!(row["state"] == "reconciliation" && row["offer_outcome"] == "rejected" && row["claim_token"].is_null(), "{row}");
            let (attempts, backoff): (i32, f64) = sqlx::query_as(
                "SELECT attempt_count,extract(epoch FROM next_attempt_at-clock_timestamp())::double precision FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            ).bind(&hash).fetch_one(&db.pool).await?;
            ensure!(attempts == 3, "{attempts}");
            ensure!(backoff > 25.0 && backoff <= 30.0, "reconciliation backoff was {backoff} s, not 10 * {attempts}");
            ensure!(live_rows().await? == (1, 1, 1));
            expire_lease(&db.pool, &hash).await?;
            let claim = ledger.claim_candidate(60).await?.context("reconciliation not claimable")?;
            ensure!(claim.lifecycle.state == CandidateState::Reconciliation);
            ensure!(ledger.finish_candidate(&claim, false, Some("superseded")).await.is_err(), "a reconciliation row was abandoned");
            ledger.retry_candidate(&claim, "still not active").await?;
            let (attempts, backoff): (i32, f64) = sqlx::query_as(
                "SELECT attempt_count,extract(epoch FROM next_attempt_at-clock_timestamp())::double precision FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            ).bind(&hash).fetch_one(&db.pool).await?;
            ensure!(attempts == 4 && backoff > 35.0 && backoff <= 40.0, "retry backoff {backoff} s at attempt {attempts}");

            // submitted, keeping the offer record and dropping the payload
            expire_lease(&db.pool, &hash).await?;
            let claim = ledger.claim_candidate(60).await?.context("not claimable")?.with_bundle(bundle);
            let revision = ledger.payout_revision().await?;
            ledger.land_candidate_at_revision(&claim, &keys().1.public_key_hex(), revision).await?;
            ledger.finish_candidate_at_revision(&claim, true, None, revision).await?;
            let row = db.row(&hash).await?;
            ensure!(row["state"] == "submitted" && row["candidate"].is_null() && row["has_block"] == false && row["window_anchor_ms"].is_null(), "{row}");
            ensure!(row["offered_at_ms"] == OFFERED_MS && row["offer_outcome"] == "rejected" && row["proof_observed_at_ms"] == PROOF_MS, "the offer record was lost: {row}");
            ensure!(live_rows().await? == (0, 0, 0));
            ensure!(ledger.claim_candidate(60).await?.is_none(), "a terminal row was claimed");
            Ok(())
        })
    })
    .await
}

/// Whether a `Database::row` has the payload every terminal row has: no
/// document, no block, none of the six window columns, and no body where the
/// outbox has a body column.
fn terminal_payload_cleared(row: &Value) -> bool {
    row["candidate"].is_null()
        && row["has_block"] == false
        && [
            "window_anchor_ms",
            "window_prior_balances_sha256",
            "window_first_share_seq",
            "window_last_share_seq",
            "window_share_count",
            "window_snapshot_sha256",
            "body_id",
        ]
        .iter()
        .all(|column| row.get(*column).is_none_or(Value::is_null))
}

/// The terminal orphan disposition (#415, migration 015) through the ledger:
/// reachable from the offer states only and never from `pending`, written at
/// the revision the observation was taken at and refused at any other, and
/// refused without a reason. `orphaned` is a processing disposition, so the
/// settled row is cleared like every terminal row (no document, block bytes
/// or window reference, so retention releases it) and keeps its offer record
/// and reason; a reservation whose call was lost stays `unknown`, with no
/// call time or reply invented for it. The row leaves the pending gauge and
/// the collector, is never claimable, can be moved by no later settlement,
/// and, like every other terminal row, blocks no signer rotation.
#[tokio::test]
async fn orphaned_rows_are_terminal_keep_their_evidence_and_leave_the_pending_gauge() -> Result<()>
{
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("orphan").await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            let metrics = Metrics::default();
            let live_rows = || async {
                let (retained, gauge): (i64, i64) = sqlx::query_as(
                    "SELECT (SELECT count(*) FROM qbit_block_candidate_outbox WHERE window_anchor_ms IS NOT NULL),(SELECT count(*) FROM qbit_block_candidate_outbox WHERE state IN ('pending','offer_reserved','offered','reconciliation'))",
                )
                .fetch_one(&db.pool)
                .await?;
                let collected = collectors::database(&db.pool, &metrics).await?.candidates;
                Ok::<_, anyhow::Error>((retained, gauge, collected))
            };
            let (candidate, bundle) = candidate_for(&snapshot, 50)?;
            let hash = candidate.block_hash.clone();
            ensure!(ledger.enqueue_candidate_observed(candidate, Some(PROOF_MS)).await?);
            let claim = ledger.claim_candidate(60).await?.context("pending not claimable")?;
            let revision = ledger.payout_revision().await?;

            // Never from pending: a block that was never offered is abandoned, not orphaned.
            let error = ledger.orphan_candidate_at_revision(&claim, "proven orphan", revision).await.err().context("a pending row was orphaned")?;
            ensure!(format!("{error:#}").contains("never offered"), "{error:#}");
            ensure!(db.row(&hash).await?["state"] == "pending");

            // From an offered row: the fences first, then the settlement.
            ledger.reserve_offer(&claim).await?;
            ledger.record_offer(&claim, OFFERED_MS, OfferOutcome::Accepted, None).await?;
            // The post-offer landing ran first: the audit and the prepared
            // pool block are evidence the disposition must keep.
            let claim = claim.with_bundle(bundle);
            ledger.land_candidate_at_revision(&claim, &keys().1.public_key_hex(), revision).await?;
            let revision = ledger.payout_revision().await?;
            let audit_evidence = || async {
                Ok::<_, anyhow::Error>(sqlx::query_as::<_, (i64, Option<String>)>(
                    "SELECT (SELECT count(*) FROM qbit_pool_audit_bundles WHERE block_hash=$1),(SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1)",
                )
                .bind(&hash)
                .fetch_one(&db.pool)
                .await?)
            };
            ensure!(audit_evidence().await? == (1, Some("prepared".into())), "the landing left no audit: {:?}", audit_evidence().await?);
            ensure!(ledger.orphan_candidate_at_revision(&claim, "   ", revision).await.is_err(), "an orphan was settled without a reason");
            let error = ledger.orphan_candidate_at_revision(&claim, "proven orphan", revision + 1).await.err().context("an orphan was settled at a revision never observed")?;
            ensure!(format!("{error:#}").contains("payout revision changed"), "{error:#}");
            ensure!(db.row(&hash).await?["state"] == "offered", "a refused settlement moved the row");
            ensure!(live_rows().await? == (1, 1, 1));
            ledger.orphan_candidate_at_revision(&claim, "proven orphan: another block is active at height 101 with 6 confirmations", revision).await?;
            let row = db.row(&hash).await?;
            ensure!(row["state"] == "orphaned" && !row["completed_at"].is_null() && row["claim_token"].is_null(), "{row}");
            ensure!(terminal_payload_cleared(&row), "the orphaned row kept a payload: {row}");
            ensure!(row["offered_at_ms"] == OFFERED_MS && row["offer_outcome"] == "accepted" && row["offer_reply"].is_null() && row["offer_reserved_by"] == "orphan" && !row["offer_reserved_at"].is_null() && row["proof_observed_at_ms"] == PROOF_MS, "the offer record was lost: {row}");
            ensure!(row["last_error"].as_str().is_some_and(|reason| reason.contains("proven orphan")), "{row}");
            ensure!(audit_evidence().await? == (1, Some("inactive".into())), "the landed audit or pool block was lost: {:?}", audit_evidence().await?);

            // Released by retention, counted by neither the gauge predicate
            // nor the collector, and never claimed or moved again.
            ensure!(live_rows().await? == (0, 0, 0));
            sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1").bind(&hash).execute(&db.pool).await?;
            ensure!(ledger.claim_candidate(60).await?.is_none(), "a terminal orphan was claimed");
            ensure!(ledger.reconcile_candidate(&claim, "again").await.is_err(), "a terminal orphan was reconciled");
            ensure!(ledger.finish_candidate(&claim, true, None).await.is_err(), "a terminal orphan was finished");
            ensure!(ledger.orphan_candidate_at_revision(&claim, "again", ledger.payout_revision().await?).await.is_err(), "a terminal orphan was orphaned twice");
            ensure!(db.row(&hash).await?["state"] == "orphaned");

            // A reservation whose submitblock call was lost: the disposition
            // records the outcome as unknown and invents no call time or
            // reply, and the row is cleared the same way.
            let (reserved, _) = candidate_for(&snapshot, 51)?;
            let reserved_hash = reserved.block_hash.clone();
            ensure!(ledger.enqueue_candidate_observed(reserved, Some(PROOF_MS)).await?);
            let claim = ledger.claim_candidate(60).await?.context("second pending not claimable")?;
            ensure!(claim.candidate.block_hash == reserved_hash);
            ledger.reserve_offer(&claim).await?;
            ledger.orphan_candidate_at_revision(&claim, "proven orphan: reservation lost, another block is active", ledger.payout_revision().await?).await?;
            let row = db.row(&reserved_hash).await?;
            ensure!(row["state"] == "orphaned" && terminal_payload_cleared(&row), "{row}");
            ensure!(row["offer_outcome"] == "unknown" && row["offered_at_ms"].is_null() && row["offer_reply"].is_null() && row["offer_reserved_by"] == "orphan", "a submission was invented for a lost reservation: {row}");
            ensure!(live_rows().await? == (0, 0, 0));

            // A terminal row blocks no signer rotation.
            ledger.configure("rotated", &other_signer_keys()).await.context("rotation refused by an orphaned row")?;
            Ok(())
        })
    })
    .await
}

/// A pending row keeps the pre-offer balance fence: with the balances moved
/// since its job was issued, its landing is refused and nothing is written.
/// The same claim, once reserved and offered, lands the same parts on the
/// balances that moved: the block is marked as issued, its carry rows are
/// the issued accounts, the current balances add its deltas, and the
/// validator is clean.
#[tokio::test]
async fn pending_divergent_landing_refuses_while_the_post_offer_landing_succeeds() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("fence").await?;
            seed_carry(&db.pool).await?;
            ledger.append(appended_share(1), None).await?;
            let issued = ledger.snapshot(100).await?;
            ensure!(issued.prior_balances.len() == 2, "the window carries two balances");
            let (candidate, bundle) = candidate_for(&issued, 20)?;
            let hash = candidate.block_hash.clone();
            ensure!(ledger.enqueue_candidate_observed(candidate, None).await?);
            let claim = ledger.claim_candidate(60).await?.context("not claimable")?.with_bundle(bundle.clone());
            // An intervening block moves miner-a's balance.
            sqlx::raw_sql("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES(repeat('ee',32),101,repeat('aa',32),repeat('ef',32),repeat('e0',32),'confirmed'); INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES(101,repeat('ee',32),'miner-a','a',decode(repeat('11',32),'hex'),700,1000,1700,0,1700,'accrued');")
                .execute(&db.pool).await?;
            let key = keys().1.public_key_hex();
            let error = ledger.land_candidate(&claim, &key).await.err().context("a divergent pending landing was accepted")?;
            ensure!(format!("{error:#}").contains("differ from current canonical balances"), "{error:#}");
            let landed: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_pool_blocks WHERE block_hash=$1").bind(&hash).fetch_one(&db.pool).await?;
            ensure!(landed == 0, "the refused landing wrote rows");

            ledger.reserve_offer(&claim).await?;
            ledger.record_offer(&claim, 1_700_000_000_789, OfferOutcome::Accepted, None).await?;
            let revision = ledger.payout_revision().await?;
            let report = ledger.land_candidate_at_revision(&claim, &key, revision).await?;
            let (marker, prior_a): (Option<String>, String) = sqlx::query_as(
                "SELECT b.as_issued_audit_sha256,c.prior_balance_sats::text FROM qbit_pool_blocks b JOIN qbit_payout_carry_forward c USING(block_hash) WHERE b.block_hash=$1 AND c.p2mr_program=decode(repeat('11',32),'hex')",
            ).bind(&hash).fetch_one(&db.pool).await?;
            ensure!(marker.as_deref() == Some(report.audit_bundle_sha256_hex.as_str()), "{marker:?}");
            ensure!(prior_a == "1000", "the landed carry row is not the issued account: prior {prior_a}");
            ledger.finish_candidate_at_revision(&claim, true, None, revision).await?;
            let mut expected: Vec<(String, i128)> = vec![("11".repeat(32), 1700), ("22".repeat(32), 500)];
            for account in bundle.payout_policy_manifest.accounts.iter().filter(|a| a.account_type == qbit_prism::PayoutPolicyAccountType::Miner) {
                let delta = i128::from(account.gross_amount_sats) - i128::from(account.onchain_amount_sats);
                match expected.iter_mut().find(|(program, _)| *program == account.p2mr_program_hex) {
                    Some((_, balance)) => *balance += delta,
                    None => expected.push((account.p2mr_program_hex.clone(), delta)),
                }
            }
            expected.retain(|(_, balance)| *balance != 0);
            expected.sort();
            let current: Vec<(String, String)> = sqlx::query_as("SELECT encode(p2mr_program,'hex'),balance_sats::text FROM qbit_current_carry_forward_balances() ORDER BY 1")
                .fetch_all(&db.pool).await?;
            let current: Vec<(String, i128)> = current.into_iter().map(|(p, b)| Ok::<_, anyhow::Error>((p, b.parse()?))).collect::<Result<_>>()?;
            ensure!(current == expected, "{current:?} != {expected:?}");
            let (mismatches, drift, reasons) = db.integrity().await?;
            ensure!(mismatches == 0 && drift == 0, "{reasons:?} drift {drift}");
            Ok(())
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// The as-issued integrity validator
// ---------------------------------------------------------------------------

/// The manifest miner-a's carry row of the marked block M is checked against.
fn manifest(block_height: u64, prior: i64, gross: i64, onchain: i64, fee: Option<i64>) -> Value {
    let mut account = json!({
        "recipient_id": "miner-a",
        "order_key": "a",
        "p2mr_program_hex": "11".repeat(32),
        "gross_amount_sats": gross,
        "prior_balance_sats": prior,
        "candidate_balance_sats": prior + gross,
        "onchain_amount_sats": onchain,
        "carry_forward_balance_sats": prior + gross - onchain,
        "action": "onchain"
    });
    if let Some(fee) = fee {
        account["settlement_fee_sats"] = json!(fee);
    }
    json!({"payout_policy_manifest": {
        "schema": "qbit.prism.payout-policy-manifest.v1",
        "block_height": block_height,
        "coinbase_value_sats": 500_000_000,
        "min_output_sats": 1,
        "floor_formula": "test",
        "accounts": [account, {
            "account_type": "pool_fee",
            "recipient_id": "pool",
            "order_key": "pool",
            "p2mr_program_hex": "33".repeat(32),
            "gross_amount_sats": 10,
            "prior_balance_sats": 0,
            "candidate_balance_sats": 10,
            "onchain_amount_sats": 10,
            "carry_forward_balance_sats": 0,
            "action": "onchain"
        }],
        "onchain_entitlements": []
    }})
}

/// The amounts of a block's carry row for miner-a: prior balance, gross,
/// paid on chain, settlement fee.
struct Amounts {
    prior: i64,
    gross: i64,
    onchain: i64,
    fee: i64,
}

/// Insert a confirmed block with its carry row for miner-a and (for a marked
/// block) its audit, marker and payout entries.
async fn insert_block(
    pool: &PgPool,
    hash: &str,
    height: i64,
    marked: Option<&Value>,
    amounts: Amounts,
) -> Result<()> {
    let Amounts {
        prior,
        gross,
        onchain,
        fee,
    } = amounts;
    let digest = marked.map(|_| format!("{:0>64}", hash.chars().take(4).collect::<String>()));
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,as_issued_audit_sha256) VALUES($1,$2,repeat('00',32),repeat('ab',32),repeat('ac',32),'confirmed',$3)")
        .bind(hash).bind(height).bind(&digest).execute(pool).await?;
    sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,settlement_fee_sats,carry_forward_balance_sats,action) VALUES($1,$2,'miner-a','a',decode(repeat('11',32),'hex'),$3,$4,$4+$3,$5,$6,$4+$3-$5,'onchain')")
        .bind(height).bind(hash).bind(gross).bind(prior).bind(onchain).bind(fee).execute(pool).await?;
    if let Some(body) = marked {
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex) VALUES($1,$2,$3,'00')")
            .bind(hash).bind(body).bind(&digest).execute(pool).await?;
        sqlx::query("INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,$2,'miner-a','a',decode(repeat('11',32),'hex'),$3,$4,'onchain'),($1,$2,'pool','pool',decode(repeat('33',32),'hex'),10,0,'onchain')")
            .bind(hash).bind(height).bind(onchain).bind(prior + gross - onchain).execute(pool).await?;
    }
    Ok(())
}

/// The validator keeps 001's sequential rule for unmarked rows and validates
/// marked rows against their immutable manifest: as-issued priors that the
/// sequential rule would refuse are accepted, the settlement fee is checked
/// but never enters the balance, blocks landed at a lower height or
/// confirmed in another order stay clean, and every corruption, a changed
/// amount, a missing required manifest amount, a missing audit with or
/// without the rest of the evidence, duplicated or missing rows, is a
/// finding with its name.
#[tokio::test]
async fn integrity_validator_accepts_marked_as_issued_rows_and_names_every_corruption() -> Result<()>
{
    run(|db| {
        Box::pin(async move {
            let _ledger = db.ledger("validator").await?;
            let pool = &db.pool;
            let clean = |label: &'static str| async move {
                let (mismatches, drift, reasons) = db.integrity().await?;
                ensure!(mismatches == 0 && drift == 0, "{label}: {reasons:?} drift {drift}");
                Ok::<_, anyhow::Error>(())
            };
            let finding = |label: &'static str, expected: &'static [&'static str]| async move {
                let (mismatches, drift, reasons) = db.integrity().await?;
                ensure!(drift == 0, "{label}: drift {drift}");
                ensure!(mismatches >= 1, "{label}: no finding");
                for reason in expected {
                    ensure!(
                        reasons.iter().any(|found| found.split(',').any(|part| part == *reason)),
                        "{label}: {reasons:?} lacks {reason}"
                    );
                }
                Ok::<_, anyhow::Error>(())
            };
            let balance = || async {
                let balance: Option<String> = sqlx::query_scalar(
                    "SELECT balance_sats::text FROM qbit_current_carry_forward_balances() WHERE p2mr_program=decode(repeat('11',32),'hex')",
                )
                .fetch_optional(pool)
                .await?;
                Ok::<_, anyhow::Error>(balance.map(|b| b.parse::<i64>()).transpose()?.unwrap_or(0))
            };

            // L: a legacy block, sequential from zero.
            let legacy = "1a".repeat(32);
            insert_block(pool, &legacy, 101, None, Amounts { prior: 0, gross: 100, onchain: 0, fee: 0 }).await?;
            clean("legacy").await?;
            ensure!(balance().await? == 100);

            // M: marked, issued on a prior of 60 that the sequential rule
            // (which expects 100 after L) would refuse, with a fee.
            let marked = "2b".repeat(32);
            let m = manifest(102, 60, 100, 150, Some(7));
            insert_block(pool, &marked, 102, Some(&m), Amounts { prior: 60, gross: 100, onchain: 150, fee: 7 }).await?;
            clean("marked as issued").await?;
            ensure!(balance().await? == 100 + 100 - 150, "the fee entered the balance");

            // K: marked, at a lower height than L, confirmed after it. The
            // marked row is clean; the legacy rule, kept as it was, now sees
            // K's delta before L and reports L's stored prior.
            let lower = "3c".repeat(32);
            let k = manifest(99, 0, 50, 20, None);
            insert_block(pool, &lower, 99, Some(&k), Amounts { prior: 0, gross: 50, onchain: 20, fee: 0 }).await?;
            {
                let (_, _, reasons) = db.integrity().await?;
                let marked_findings: Vec<String> = sqlx::query_scalar(
                    "SELECT mismatch_reason FROM qbit_carry_forward_integrity_mismatches() WHERE block_hash IN ($1,$2)",
                ).bind(&marked).bind(&lower).fetch_all(pool).await?;
                ensure!(marked_findings.is_empty(), "marked rows reported: {marked_findings:?}");
                let legacy_findings: Vec<String> = sqlx::query_scalar(
                    "SELECT mismatch_reason FROM qbit_carry_forward_integrity_mismatches() WHERE block_hash=$1",
                ).bind(&legacy).fetch_all(pool).await?;
                ensure!(legacy_findings == ["prior_balance,candidate_balance,carry_forward_balance"], "{legacy_findings:?} ({reasons:?})");
            }
            sqlx::query("DELETE FROM qbit_payout_carry_forward WHERE block_hash=$1; ").bind(&lower).execute(pool).await?;
            sqlx::query("DELETE FROM qbit_pool_payout_entries WHERE block_hash=$1").bind(&lower).execute(pool).await?;
            sqlx::query("DELETE FROM qbit_pool_audit_bundles WHERE block_hash=$1").bind(&lower).execute(pool).await?;
            sqlx::query("DELETE FROM qbit_pool_blocks WHERE block_hash=$1").bind(&lower).execute(pool).await?;
            clean("after removing K").await?;

            // Corruptions of M, each restored afterwards.
            sqlx::query("UPDATE qbit_payout_carry_forward SET onchain_amount_sats=onchain_amount_sats+1 WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            finding("changed amount", &["onchain_amount", "carry_arithmetic"]).await?;
            sqlx::query("UPDATE qbit_payout_carry_forward SET onchain_amount_sats=onchain_amount_sats-1 WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            clean("restored amount").await?;

            sqlx::query("UPDATE qbit_payout_carry_forward SET settlement_fee_sats=8 WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            finding("changed fee", &["settlement_fee"]).await?;
            ensure!(balance().await? == 50, "the fee entered the balance");
            sqlx::query("UPDATE qbit_payout_carry_forward SET settlement_fee_sats=7 WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            clean("restored fee").await?;

            sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=jsonb_set(audit_bundle,'{payout_policy_manifest,accounts,0}',(audit_bundle->'payout_policy_manifest'->'accounts'->0)-'prior_balance_sats') WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            finding("missing manifest amount", &["manifest_field_missing", "prior_balance"]).await?;
            sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2 WHERE block_hash=$1").bind(&marked).bind(&m).execute(pool).await?;
            clean("restored manifest").await?;

            sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=jsonb_set(audit_bundle,'{payout_policy_manifest,block_height}','103') WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            finding("manifest height", &["manifest_block_height"]).await?;
            sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2 WHERE block_hash=$1").bind(&marked).bind(&m).execute(pool).await?;
            clean("restored height").await?;

            sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,settlement_fee_sats,carry_forward_balance_sats,action) SELECT block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,settlement_fee_sats,carry_forward_balance_sats,action FROM qbit_payout_carry_forward WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            finding("duplicate evidence", &["duplicate_evidence"]).await?;
            sqlx::query("DELETE FROM qbit_payout_carry_forward WHERE carry_forward_seq=(SELECT max(carry_forward_seq) FROM qbit_payout_carry_forward WHERE block_hash=$1)").bind(&marked).execute(pool).await?;
            clean("deduplicated").await?;

            sqlx::query("INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,102,'miner-z','z',decode(repeat('44',32),'hex'),5,0,'onchain')").bind(&marked).execute(pool).await?;
            finding("unexpected payout entry", &["payout_entry_unexpected"]).await?;
            sqlx::query("DELETE FROM qbit_pool_payout_entries WHERE block_hash=$1 AND miner_id='miner-z'").bind(&marked).execute(pool).await?;
            sqlx::query("DELETE FROM qbit_pool_payout_entries WHERE block_hash=$1 AND miner_id='miner-a'").bind(&marked).execute(pool).await?;
            finding("missing payout entry", &["payout_entry_missing"]).await?;
            sqlx::query("INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,102,'miner-a','a',decode(repeat('11',32),'hex'),150,10,'onchain')").bind(&marked).execute(pool).await?;
            clean("restored payout entry").await?;

            let carry: Value = sqlx::query_scalar("SELECT to_jsonb(c) FROM qbit_payout_carry_forward c WHERE block_hash=$1").bind(&marked).fetch_one(pool).await?;
            sqlx::query("DELETE FROM qbit_payout_carry_forward WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            finding("missing carry evidence", &["evidence_missing"]).await?;
            sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,settlement_fee_sats,carry_forward_balance_sats,action) VALUES($1,$2,'miner-a','a',decode(repeat('11',32),'hex'),$3,$4,$5,$6,$7,$8,'onchain')")
                .bind(102i64).bind(&marked)
                .bind(carry["gross_amount_sats"].as_i64().context("gross")?)
                .bind(carry["prior_balance_sats"].as_i64().or_else(|| carry["prior_balance_sats"].as_str().and_then(|v| v.parse().ok())).context("prior")?)
                .bind(carry["candidate_balance_sats"].as_i64().or_else(|| carry["candidate_balance_sats"].as_str().and_then(|v| v.parse().ok())).context("candidate")?)
                .bind(carry["onchain_amount_sats"].as_i64().context("onchain")?)
                .bind(carry["settlement_fee_sats"].as_i64().context("fee")?)
                .bind(carry["carry_forward_balance_sats"].as_i64().or_else(|| carry["carry_forward_balance_sats"].as_str().and_then(|v| v.parse().ok())).context("carry")?)
                .execute(pool).await?;
            clean("restored carry evidence").await?;

            // The audit the marker names is gone: a finding by itself, and
            // still a finding once every other piece of evidence is gone too.
            sqlx::query("DELETE FROM qbit_pool_audit_bundles WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            finding("missing audit", &["audit_missing"]).await?;
            sqlx::query("DELETE FROM qbit_payout_carry_forward WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            sqlx::query("DELETE FROM qbit_pool_payout_entries WHERE block_hash=$1").bind(&marked).execute(pool).await?;
            finding("whole evidence set missing", &["audit_missing"]).await?;
            // A wrong digest under the marker is as missing as no row.
            sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex) VALUES($1,$2,repeat('ff',32),'00')").bind(&marked).bind(&m).execute(pool).await?;
            finding("audit under another digest", &["audit_missing"]).await?;
            Ok(())
        })
    })
    .await
}

/// The issued arithmetic of every manifest account is validated, not only
/// of the accounts that have a carry row: a fee recipient is paid on chain
/// and carries nothing, so a marked block whose pool-fee entry does not add
/// up (prior 0 + gross 9 is not candidate 10) is a finding by itself even
/// though its payout entry matches the entry's on-chain amount and no carry
/// row exists to disagree with. The same entry with consistent amounts is
/// clean, and the miner rows are untouched either way.
#[tokio::test]
async fn integrity_validator_checks_the_issued_arithmetic_of_every_manifest_account() -> Result<()>
{
    run(|db| {
        Box::pin(async move {
            let _ledger = db.ledger("validator-fee").await?;
            let pool = &db.pool;
            let marked = "4d".repeat(32);
            // The pool-fee account of the manifest carries gross 9 but a
            // candidate balance of 10, paid on chain in full, with no carry
            // row (fee recipients never have one) and a matching payout
            // entry of 10.
            let mut malformed = manifest(102, 0, 100, 100, None);
            malformed["payout_policy_manifest"]["accounts"][1]["gross_amount_sats"] = json!(9);
            insert_block(pool, &marked, 102, Some(&malformed), Amounts { prior: 0, gross: 100, onchain: 100, fee: 0 }).await?;
            let (mismatches, drift, reasons) = db.integrity().await?;
            ensure!(drift == 0, "drift {drift}");
            ensure!(mismatches == 1, "{reasons:?}");
            let (miner_id, reason): (Option<String>, String) = sqlx::query_as(
                "SELECT miner_id,mismatch_reason FROM qbit_carry_forward_integrity_mismatches()",
            )
            .fetch_one(pool)
            .await?;
            ensure!(miner_id.as_deref() == Some("pool"), "{miner_id:?}: {reason}");
            ensure!(reason == "manifest_candidate_arithmetic", "{reason}");
            // The same entry, adding up, is clean; the carry side of the
            // arithmetic is checked the same way.
            sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=jsonb_set(audit_bundle,'{payout_policy_manifest,accounts,1,gross_amount_sats}','10') WHERE block_hash=$1")
                .bind(&marked).execute(pool).await?;
            let (mismatches, drift, reasons) = db.integrity().await?;
            ensure!(mismatches == 0 && drift == 0, "{reasons:?} drift {drift}");
            sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=jsonb_set(audit_bundle,'{payout_policy_manifest,accounts,1,carry_forward_balance_sats}','1') WHERE block_hash=$1")
                .bind(&marked).execute(pool).await?;
            let reasons: Vec<(Option<String>, String)> = sqlx::query_as(
                "SELECT miner_id,mismatch_reason FROM qbit_carry_forward_integrity_mismatches()",
            )
            .fetch_all(pool)
            .await?;
            ensure!(
                reasons.iter().any(|(miner, reason)| miner.as_deref() == Some("pool")
                    && reason.split(',').any(|part| part == "manifest_carry_arithmetic")),
                "{reasons:?}"
            );
            Ok(())
        })
    })
    .await
}

/// The fence is decided by the row's state as the database holds it under
/// the claim lock, never by the claim's memory of it: the claim that was
/// taken pending lands after its own reservation on moved balances.
#[tokio::test]
async fn landing_reads_the_lifecycle_state_under_the_claim_lock_not_from_the_claim() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("state").await?;
            seed_carry(&db.pool).await?;
            ledger.append(appended_share(1), None).await?;
            let issued = ledger.snapshot(100).await?;
            let (candidate, bundle) = candidate_for(&issued, 30)?;
            ensure!(ledger.enqueue_candidate_observed(candidate, None).await?);
            // Claimed pending: the in-memory lifecycle says pending for good.
            let claim = ledger.claim_candidate(60).await?.context("not claimable")?.with_bundle(bundle);
            ensure!(claim.lifecycle.state == CandidateState::Pending);
            sqlx::raw_sql("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES(repeat('ee',32),101,repeat('aa',32),repeat('ef',32),repeat('e0',32),'confirmed'); INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES(101,repeat('ee',32),'miner-b','b',decode(repeat('22',32),'hex'),1,500,501,0,501,'accrued');")
                .execute(&db.pool).await?;
            let key = keys().1.public_key_hex();
            ensure!(ledger.land_candidate(&claim, &key).await.is_err(), "a pending divergent landing was accepted");
            ledger.reserve_offer(&claim).await?;
            // Reserved, with the same claim value: the landing reads the
            // reservation from the row and accepts the divergence.
            let revision = ledger.payout_revision().await?;
            ledger.land_candidate_at_revision(&claim, &key, revision).await?;
            let state: String = sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(&claim.candidate.block_hash).fetch_one(&db.pool).await?;
            ensure!(state == "offer_reserved", "{state}");
            let marked: Option<String> = sqlx::query_scalar("SELECT as_issued_audit_sha256 FROM qbit_pool_blocks WHERE block_hash=$1")
                .bind(&claim.candidate.block_hash).fetch_one(&db.pool).await?;
            ensure!(marked.is_some(), "the block is not marked as issued");
            Ok(())
        })
    })
    .await
}

/// Writing a fingerprint onto a reset one with other keys is a rotation, and
/// it must be refused, naming the row, while `row` is the only unfinished
/// row and is in `state`; a refusal pins nothing.
async fn rotation_refused(
    ledger: &Ledger,
    pool: &PgPool,
    label: &str,
    state: &str,
    hash: &str,
) -> Result<()> {
    let (unfinished, current): (i64, String) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM qbit_block_candidate_outbox WHERE state IN ('pending','offer_reserved','offered','reconciliation')),(SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1)",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?;
    ensure!(
        unfinished == 1 && current == state,
        "{label}: {unfinished} unfinished rows and {hash} is {current}; the probe is not isolated"
    );
    let error = ledger
        .configure("rotated", &other_signer_keys())
        .await
        .err()
        .with_context(|| format!("rotation accepted at {label}"))?;
    ensure!(
        format!("{error:#}").contains(hash),
        "{label}: the refusal did not name {hash}: {error:#}"
    );
    let pinned: Option<String> =
        sqlx::query_scalar("SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(pool)
            .await?;
    ensure!(
        pinned.is_none(),
        "{label}: the refused rotation pinned {pinned:?}"
    );
    Ok(())
}

/// A signer rotation is refused while the only unfinished row stores other
/// keys, whichever unfinished state it is in: `pending`, `offer_reserved`,
/// `offered`, `reconciliation` after an offer, and `reconciliation` by
/// adoption of an active block without any offer. Each state is probed with
/// that one row and nothing else unfinished, so no other state can stand in
/// for an omitted one; a refusal pins nothing; and once every row is
/// terminal the same rotation is accepted.
#[tokio::test]
async fn signer_rotation_is_refused_at_every_unfinished_state_alone_and_accepted_once_all_are_terminal(
) -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("rotation").await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            let key = keys().1.public_key_hex();

            // Row A, through the offer states to its landing.
            let (candidate, bundle) = candidate_for(&snapshot, 40)?;
            let hash = candidate.block_hash.clone();
            ensure!(
                ledger
                    .enqueue_candidate_observed(candidate, Some(PROOF_MS))
                    .await?
            );
            rotation_refused(&ledger, &db.pool, "pending", "pending", &hash).await?;
            let claim = ledger
                .claim_candidate(60)
                .await?
                .context("pending not claimable")?;
            ledger.reserve_offer(&claim).await?;
            rotation_refused(&ledger, &db.pool, "offer_reserved", "offer_reserved", &hash).await?;
            ledger
                .record_offer(&claim, OFFERED_MS, OfferOutcome::Accepted, None)
                .await?;
            rotation_refused(&ledger, &db.pool, "offered", "offered", &hash).await?;
            ledger
                .reconcile_candidate(&claim, "landing failed after acceptance")
                .await?;
            rotation_refused(&ledger, &db.pool, "reconciliation", "reconciliation", &hash).await?;
            expire_lease(&db.pool, &hash).await?;
            let claim = ledger
                .claim_candidate(60)
                .await?
                .context("reconciliation not claimable")?
                .with_bundle(bundle);
            let revision = ledger.payout_revision().await?;
            ledger
                .land_candidate_at_revision(&claim, &key, revision)
                .await?;
            ledger
                .finish_candidate_at_revision(&claim, true, None, revision)
                .await?;
            ensure!(db.row(&hash).await?["state"] == "submitted");

            // Row B, adopted from pending into reconciliation without an offer.
            let (candidate, bundle) = candidate_for(&snapshot, 41)?;
            let hash = candidate.block_hash.clone();
            ensure!(
                ledger
                    .enqueue_candidate_observed(candidate, Some(PROOF_MS))
                    .await?
            );
            let claim = ledger
                .claim_candidate(60)
                .await?
                .context("pending not claimable")?
                .with_bundle(bundle);
            ledger
                .adopt_active_candidate(
                    &claim,
                    "node: block active at height 101",
                    "already on the active chain before any offer",
                )
                .await?;
            rotation_refused(
                &ledger,
                &db.pool,
                "adopted reconciliation",
                "reconciliation",
                &hash,
            )
            .await?;
            let revision = ledger.payout_revision().await?;
            ledger
                .land_candidate_at_revision(&claim, &key, revision)
                .await?;
            ledger
                .finish_candidate_at_revision(&claim, true, None, revision)
                .await?;
            ensure!(db.row(&hash).await?["state"] == "submitted");

            // The control: every row terminal, the same rotation is accepted.
            ledger
                .configure("rotated", &other_signer_keys())
                .await
                .context("rotation refused with every row terminal")?;
            let pinned: Option<String> = sqlx::query_scalar(
                "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton",
            )
            .fetch_one(&db.pool)
            .await?;
            ensure!(pinned.as_deref() == Some("rotated"), "{pinned:?}");
            Ok(())
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// The declaration 011 makes, and the index it builds
// ---------------------------------------------------------------------------

/// Every relation, sequence, index and function of the schema, by name: the
/// witness that a refused start or migrate changed nothing.
async fn schema_objects(pool: &PgPool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT relkind::text||' '||relname::text FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relkind IN ('r','S','i') UNION ALL SELECT 'f '||oid::regprocedure::text FROM pg_proc WHERE pronamespace=current_schema()::regnamespace ORDER BY 1",
    )
    .fetch_all(pool)
    .await?)
}

async fn offer_capability(pool: &PgPool) -> Result<Option<i32>> {
    Ok(sqlx::query_scalar(
        "SELECT capability_value FROM qbit_prism_schema_capabilities WHERE capability='candidate_offer_lifecycle'",
    )
    .fetch_optional(pool)
    .await?)
}

/// A database at 11 declares `candidate_offer_lifecycle = 1`: 011 declared
/// it once every object of the lifecycle existed, and nothing native removes
/// or edits it. Without the row, or with another value, the database can no
/// longer say that every unfinished outbox row belongs to the offer
/// lifecycle, so a start refuses it in both connect modes and a migrate
/// refuses it before any DDL, with every row, every object and the
/// declaration itself left exactly as found: the server never repairs a
/// selectively restored declaration. A genuine pre-011 database has no such
/// row to lose and is declared by 011; declared again as 011 declares it, a
/// refused database starts and migrates.
#[tokio::test]
async fn migrated_database_without_its_offer_lifecycle_declaration_is_refused_at_connect_and_at_migrate(
) -> Result<()> {
    run(|db| {
        Box::pin(async move {
            // The genuine upgrade: a pre-011 database is not refused for the
            // row it cannot have, and 011 declares it while it quarantines
            // the attempted row and keeps the never-attempted one.
            db.apply_pre_011().await?;
            ensure!(offer_capability(&db.pool).await?.is_none());
            seed_share(&db.pool, 1).await?;
            let snapshot = seeded_snapshot();
            let (attempted, _) = candidate_for(&snapshot, 41)?;
            let (fresh, _) = candidate_for(&snapshot, 42)?;
            insert_pre_011_pending(&db.pool, &attempted, 1, Some("node timed out"), false, true)
                .await?;
            insert_pre_011_pending(&db.pool, &fresh, 0, None, false, true).await?;
            sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,attempt_count,completed_at) VALUES($1,NULL,$2,'submitted',1,clock_timestamp()-interval '1 hour')")
                .bind("d4".repeat(32)).bind(format!("d4{}", "0".repeat(62)))
                .execute(&db.pool).await?;
            let upgraded = db.ledger("upgrade").await?;
            ensure!(db.versions().await? == ALL_VERSIONS);
            ensure!(
                offer_capability(&db.pool).await? == Some(1),
                "011 did not declare the lifecycle"
            );
            ensure!(db.row(&attempted.block_hash).await?["state"] == "reconciliation");
            ensure!(db.row(&fresh.block_hash).await?["state"] == "pending");
            upgraded.pool.close().await;
            let rows = db.outbox_rows().await?;
            let objects = schema_objects(&db.pool).await?;
            // Healthy controls: both connect modes start the declared database.
            for initialize in [false, true] {
                Ledger::connect(&db.url, format!("healthy-{initialize}"), 4, initialize)
                    .await
                    .with_context(|| {
                        format!("connect(initialize={initialize}) refused a declared database")
                    })?
                    .pool
                    .close()
                    .await;
            }
            for (case, statement, message, remedy) in [
                (
                    "deleted",
                    "DELETE FROM qbit_prism_schema_capabilities WHERE capability='candidate_offer_lifecycle'",
                    "qbit_prism_schema_capabilities has no candidate_offer_lifecycle row",
                    "VALUES('candidate_offer_lifecycle',1)",
                ),
                (
                    "edited",
                    "UPDATE qbit_prism_schema_capabilities SET capability_value=0 WHERE capability='candidate_offer_lifecycle'",
                    "declares candidate_offer_lifecycle = 0",
                    "nothing native writes another value",
                ),
                (
                    "newer",
                    "UPDATE qbit_prism_schema_capabilities SET capability_value=2 WHERE capability='candidate_offer_lifecycle'",
                    "declares candidate_offer_lifecycle = 2",
                    "newer PRISM release",
                ),
            ] {
                sqlx::raw_sql(statement).execute(&db.pool).await?;
                let declared = offer_capability(&db.pool).await?;
                for initialize in [false, true] {
                    let error = Ledger::connect(&db.url, "cold".into(), 4, initialize)
                        .await
                        .err()
                        .with_context(|| format!("{case}: connect(initialize={initialize}) accepted a database at 11 without its lifecycle declaration"))?;
                    let text = format!("{error:#}");
                    ensure!(
                        text.contains("database is at schema migration 11 but")
                            && text.contains(message)
                            && text.contains(remedy),
                        "{case}: {text}"
                    );
                    if initialize {
                        ensure!(
                            text.contains("refusing to migrate a native database at schema migrations 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 before any DDL"),
                            "{case}: {text}"
                        );
                    }
                    ensure!(
                        db.versions().await? == ALL_VERSIONS,
                        "{case}: the migration record changed"
                    );
                    ensure!(
                        db.outbox_rows().await? == rows,
                        "{case}: a refused start rewrote outbox rows"
                    );
                    ensure!(
                        schema_objects(&db.pool).await? == objects,
                        "{case}: a refused start changed the schema"
                    );
                    ensure!(
                        offer_capability(&db.pool).await? == declared,
                        "{case}: the refusal repaired the declaration itself"
                    );
                }
                sqlx::raw_sql("DELETE FROM qbit_prism_schema_capabilities WHERE capability='candidate_offer_lifecycle'; INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('candidate_offer_lifecycle',1)")
                    .execute(&db.pool).await?;
            }
            // Before any DDL: with 009 undone as well, migrate refuses the
            // missing declaration and applies nothing above it. Declared
            // again, 009 is applied and the database starts in both modes.
            sqlx::raw_sql("DELETE FROM qbit_prism_schema_migrations WHERE version=9; DROP TABLE qbit_prism_session_reservations; DROP INDEX qbit_prism_jobs_extranonce1_expiry_idx; ALTER SEQUENCE qbit_prism_session_sequence NO CYCLE; DELETE FROM qbit_prism_schema_capabilities WHERE capability='candidate_offer_lifecycle'")
                .execute(&db.pool).await?;
            let objects = schema_objects(&db.pool).await?;
            let error = db
                .ledger("this-build")
                .await
                .err()
                .context("migrate applied 009 above a missing lifecycle declaration")?;
            let text = format!("{error:#}");
            ensure!(
                text.contains("refusing to migrate a native database at schema migrations 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 before any DDL")
                    && text.contains("has no candidate_offer_lifecycle row"),
                "{text}"
            );
            ensure!(db.versions().await? == [2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);
            ensure!(
                schema_objects(&db.pool).await? == objects,
                "a refused migrate changed the schema"
            );
            ensure!(
                db.outbox_rows().await? == rows,
                "a refused migrate rewrote outbox rows"
            );
            sqlx::raw_sql("INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('candidate_offer_lifecycle',1)")
                .execute(&db.pool).await?;
            let migrated = db.ledger("this-build").await?;
            ensure!(
                db.versions().await? == ALL_VERSIONS,
                "009 was not applied after the remedy"
            );
            migrated.pool.close().await;
            Ledger::connect(&db.url, "follower".into(), 4, false)
                .await?
                .pool
                .close()
                .await;
            ensure!(
                db.outbox_rows().await? == rows,
                "the remedy rewrote outbox rows"
            );
            // A record with 11 and not 6 was restored selectively: the fence
            // still runs before any DDL, ahead of 006's own refusals, and the
            // remedy is the same.
            sqlx::raw_sql("DELETE FROM qbit_prism_schema_migrations WHERE version=6; DELETE FROM qbit_prism_schema_capabilities WHERE capability='candidate_offer_lifecycle'")
                .execute(&db.pool).await?;
            let objects = schema_objects(&db.pool).await?;
            let error = db.ledger("this-build").await.err().context(
                "migrate accepted a record with 11 and not 6 without the lifecycle declaration",
            )?;
            let text = format!("{error:#}");
            ensure!(
                text.contains("refusing to migrate a native database at schema migrations 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 before any DDL")
                    && text.contains("has no candidate_offer_lifecycle row"),
                "{text}"
            );
            ensure!(
                schema_objects(&db.pool).await? == objects && db.outbox_rows().await? == rows,
                "a refused migrate changed the database"
            );
            sqlx::raw_sql("INSERT INTO qbit_prism_schema_migrations(version) VALUES(6); INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('candidate_offer_lifecycle',1)")
                .execute(&db.pool).await?;
            Ledger::connect(&db.url, "restored".into(), 4, true)
                .await?
                .pool
                .close()
                .await;
            ensure!(db.versions().await? == ALL_VERSIONS && db.outbox_rows().await? == rows);
            Ok(())
        })
    })
    .await
}

/// Retained terminal history beside one unfinished row in each state, all
/// due, the pending rows attempted so that only the oldest-due lane can
/// serve them. Every row satisfies 011's lifecycle, payload and offer rules.
async fn seed_retained_history(pool: &PgPool, terminal_rows: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,attempt_count,created_at,next_attempt_at,completed_at) \
         SELECT lpad(to_hex(i),64,'0'),NULL,lpad(to_hex(i),64,'0'),CASE WHEN i%7=0 THEN 'abandoned' ELSE 'submitted' END,1+(i%3), \
                clock_timestamp()-(i||' seconds')::interval,clock_timestamp()-(i||' seconds')::interval,clock_timestamp()-(i||' seconds')::interval \
         FROM generate_series(1,$1::bigint) AS g(i)",
    )
    .bind(terminal_rows)
    .execute(pool)
    .await?;
    sqlx::raw_sql(
        r#"INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256,state,attempt_count,created_at,next_attempt_at,offer_reserved_at,offer_reserved_by,offered_at_ms,offer_outcome,last_error) VALUES
           (repeat('a1',32),'{"k":1}',repeat('a1',32),decode('00','hex'),1,repeat('b1',32),'pending',2,clock_timestamp()-interval '10 minutes',clock_timestamp()-interval '10 minutes',NULL,NULL,NULL,NULL,'window read timed out'),
           (repeat('a2',32),'{"k":2}',repeat('a2',32),decode('00','hex'),1,repeat('b2',32),'pending',3,clock_timestamp()-interval '3 hours',clock_timestamp()-interval '1 minute',NULL,NULL,NULL,NULL,'window read timed out'),
           (repeat('a3',32),'{"k":3}',repeat('a3',32),decode('00','hex'),1,repeat('b3',32),'offer_reserved',1,clock_timestamp()-interval '5 minutes',clock_timestamp()-interval '4 minutes',clock_timestamp()-interval '5 minutes','fe-1',NULL,NULL,NULL),
           (repeat('a4',32),'{"k":4}',repeat('a4',32),decode('00','hex'),1,repeat('b4',32),'offered',1,clock_timestamp()-interval '6 minutes',clock_timestamp()-interval '5 minutes',clock_timestamp()-interval '6 minutes','fe-1',1700000000456,'accepted',NULL),
           (repeat('a5',32),'{"k":5}',repeat('a5',32),decode('00','hex'),1,repeat('b5',32),'reconciliation',4,clock_timestamp()-interval '2 hours',clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '2 hours','fe-2',NULL,'unknown','delivery unknown')"#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Every node of an `EXPLAIN (FORMAT JSON)` plan tree, depth first.
fn plan_nodes<'a>(plan: &'a Value, nodes: &mut Vec<&'a Value>) {
    nodes.push(plan);
    if let Some(children) = plan["Plans"].as_array() {
        for child in children {
            plan_nodes(child, nodes);
        }
    }
}

/// The oldest-due lane selects every unfinished state, the pending rows
/// included, and takes the first due row in its own order; the dispatch
/// probe asks whether any such row is due at all. Over an outbox that keeps
/// its terminal history, both are served by 011's partial index, the lane in
/// the index's order, under the planner's defaults: no claim scans or sorts
/// the retained rows. The fresh lane keeps 005's index. The plans are those
/// of the statements the server issues, and the lane's first row is the
/// earliest due unfinished row whatever its state.
#[tokio::test]
async fn oldest_due_lane_and_dispatch_probe_use_the_unfinished_index_over_retained_history(
) -> Result<()> {
    run(|db| {
        Box::pin(async move {
            db.apply_pre_011().await?;
            let ledger = db.ledger("plan").await?;
            let forced: Vec<String> = sqlx::query_scalar(
                "SELECT name FROM pg_settings WHERE name IN ('enable_seqscan','enable_sort','enable_indexscan','enable_bitmapscan') AND setting<>'on'",
            )
            .fetch_all(&db.pool)
            .await?;
            ensure!(forced.is_empty(), "planner settings are forced: {forced:?}");
            seed_retained_history(&db.pool, 50_000).await?;
            sqlx::raw_sql("ANALYZE qbit_block_candidate_outbox")
                .execute(&db.pool)
                .await?;
            for (name, statement, index, ordered) in [
                (
                    "oldest-due lane",
                    Ledger::claim_lane_sql(false),
                    "qbit_block_candidate_outbox_unfinished_idx",
                    true,
                ),
                (
                    "dispatch probe",
                    Ledger::due_work_probe_sql(),
                    "qbit_block_candidate_outbox_unfinished_idx",
                    false,
                ),
                (
                    "fresh lane",
                    Ledger::claim_lane_sql(true),
                    "qbit_prism_candidate_fresh_idx",
                    true,
                ),
            ] {
                let plan: Value =
                    sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {statement}"))
                        .fetch_one(&db.pool)
                        .await?;
                let plan = &plan[0]["Plan"];
                let mut nodes = Vec::new();
                plan_nodes(plan, &mut nodes);
                let scans: Vec<(&str, &str)> = nodes
                    .iter()
                    .filter(|node| {
                        node["Relation Name"] == "qbit_block_candidate_outbox"
                            || node["Node Type"] == "Bitmap Index Scan"
                    })
                    .map(|node| {
                        (
                            node["Node Type"].as_str().unwrap_or(""),
                            node["Index Name"].as_str().unwrap_or(""),
                        )
                    })
                    .collect();
                println!("{name} over 50000 retained rows: {scans:?}");
                ensure!(
                    scans.iter().any(|(kind, used)| *used == index
                        && matches!(*kind, "Index Scan" | "Index Only Scan" | "Bitmap Index Scan")),
                    "{name}: not served by {index}: {plan}"
                );
                ensure!(
                    !scans.iter().any(|(kind, _)| *kind == "Seq Scan"),
                    "{name}: scans the whole outbox: {plan}"
                );
                if ordered {
                    ensure!(
                        !nodes.iter().any(|node| node["Node Type"] == "Sort"),
                        "{name}: sorts the outbox: {plan}"
                    );
                    ensure!(
                        scans
                            .iter()
                            .any(|(kind, used)| *used == index && *kind == "Index Scan"),
                        "{name}: not an ordered index scan: {plan}"
                    );
                }
            }
            // The lane over that history: the earliest due unfinished row, a
            // pending one, comes first, and the probe sees the due work.
            let mut tx = db.pool.begin().await?;
            let first: String = sqlx::query_scalar(&Ledger::claim_lane_sql(false))
                .fetch_one(&mut *tx)
                .await?;
            ensure!(
                first == "a1".repeat(32),
                "the oldest-due lane did not take the earliest due pending row: {first}"
            );
            let slot: Option<i64> = sqlx::query_scalar(&Ledger::due_work_probe_sql())
                .fetch_optional(&mut *tx)
                .await?;
            ensure!(slot.is_some(), "the dispatch probe saw no due work");
            tx.rollback().await?;
            ledger.pool.close().await;
            Ok(())
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// The divergence alert a post-offer landing raises
// ---------------------------------------------------------------------------

/// The start of the warning `land_candidate` raises when it lands an offered
/// block whose as-issued prior balances are no longer the canonical ones.
const DIVERGENCE_ALERT: &str =
    "ALERT: landing an as-issued audit whose prior balances differ from the current canonical balances";

/// Collects one landing's events. `offer_lifecycle` is a many-test binary, so
/// the subscriber is attached to the landing future alone (never installed
/// globally) and the capture belongs to that one test.
#[derive(Clone, Default)]
struct LogCapture(std::sync::Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl LogCapture {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }

    fn events(&self) -> Result<Vec<Value>> {
        self.text()
            .lines()
            .map(|line| Ok(serde_json::from_str(line)?))
            .collect()
    }
}

/// miner-c, whose only share of the divergence window is a quarter of its
/// weight and who carries no seeded balance.
const MINER_C_PROGRAM: &str = "33333333333333333333333333333333333333333333333333333333333333cc";

/// The window both divergence tests are issued on. miner-a takes all but a
/// rounding sliver of the coinbase; miner-c's sliver lands under the payout
/// policy's dust floor, so it accrues instead of being paid on chain and
/// miner-a absorbs it against its seeded balance. That accrual is the one
/// lever in this fixture that moves the canonical balances the way production
/// moves them — `SUM(gross - onchain)` over a landed and confirmed block's
/// own carry rows — rather than by writing balance rows behind the ledger's
/// back. The window is read at a difficulty wide enough to hold both shares.
const WINDOW_DIFFICULTY: u128 = 1_000_000;

async fn seed_divergence_window(ledger: &Ledger) -> Result<()> {
    let mut major = appended_share(1);
    major.share_difficulty = 500_000;
    ledger.append(major, None).await?;
    let mut minor = appended_share(2);
    minor.miner_id = "miner-c".into();
    minor.order_key = "c".into();
    minor.p2mr_program_hex = MINER_C_PROGRAM.into();
    ledger.append(minor, None).await?;
    Ok(())
}

/// A subscriber that records warnings as JSON into `capture`, for
/// `.with_subscriber(..)` on a single future.
fn capturing_subscriber(capture: &LogCapture) -> impl tracing::Subscriber + Send + Sync {
    let writer = capture.clone();
    tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(move || writer.clone())
        .finish()
}

/// Land `claim` with `subscriber` attached to that future alone. The digest
/// comparison runs on a blocking thread, but the warning is emitted on the
/// async task after that await, so the per-future subscriber sees it.
async fn land_capturing(
    ledger: &Ledger,
    claim: &CandidateClaim,
    key: &str,
    capture: &LogCapture,
) -> Result<qbit_prism::AuditVerificationReport> {
    let revision = ledger.payout_revision().await?;
    ledger
        .land_candidate_at_revision(claim, key, revision)
        .with_subscriber(capturing_subscriber(capture))
        .await
}

/// Claim the one candidate the outbox is expected to hand out next, with the
/// bundle its parts are rebuilt from.
async fn claim_expecting(
    ledger: &Ledger,
    hash: &str,
    bundle: AuditBundle,
) -> Result<CandidateClaim> {
    let claim = ledger
        .claim_candidate(60)
        .await?
        .context("the candidate is not claimable")?;
    ensure!(
        claim.candidate.block_hash == hash,
        "claimed {} instead of {hash}",
        claim.candidate.block_hash
    );
    Ok(claim.with_bundle(bundle))
}

async fn offer(ledger: &Ledger, claim: &CandidateClaim) -> Result<()> {
    ledger.reserve_offer(claim).await?;
    ledger
        .record_offer(claim, OFFERED_MS, OfferOutcome::Accepted, None)
        .await
}

/// The prior balance `block_hash`'s carry row records for `program`: the
/// amount the block's audit was issued against, which for a divergent landing
/// is the as-issued balance and not the current one.
async fn landed_prior_balance(pool: &PgPool, block_hash: &str, program: &str) -> Result<i128> {
    let prior: String = sqlx::query_scalar("SELECT prior_balance_sats::text FROM qbit_payout_carry_forward WHERE block_hash=$1 AND p2mr_program=decode($2,'hex')")
        .bind(block_hash).bind(program).fetch_one(pool).await?;
    Ok(prior.parse()?)
}

/// `program`'s canonical balance, or zero when it carries none.
async fn current_balance(pool: &PgPool, program: &str) -> Result<i128> {
    let balance: Option<String> = sqlx::query_scalar("SELECT balance_sats::text FROM qbit_current_carry_forward_balances() WHERE p2mr_program=decode($1,'hex')")
        .bind(program).fetch_optional(pool).await?;
    Ok(balance
        .map(|balance| balance.parse())
        .transpose()?
        .unwrap_or_default())
}

async fn as_issued_marker(pool: &PgPool, block_hash: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT as_issued_audit_sha256 FROM qbit_pool_blocks WHERE block_hash=$1",
    )
    .bind(block_hash)
    .fetch_one(pool)
    .await?)
}

/// An offered block lands on balances another block of the same window moved
/// out from under it. The landing is allowed — the node has or may have the
/// block — but it is reported: one WARN naming the block, its lifecycle state
/// and both digests. The alert is not the whole contract, so the same test
/// holds the landing to the rest of it: the provenance marker records the
/// audit the block was issued with, the carry rows keep the as-issued prior
/// balance rather than the moved one, and the validator that reads marked
/// rows against their manifest stays clean once the block is confirmed.
#[tokio::test]
async fn a_divergent_offered_landing_warns_with_both_digests_and_keeps_its_as_issued_accounts(
) -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("divergence").await?;
            seed_carry(&db.pool).await?;
            seed_divergence_window(&ledger).await?;
            let issued = ledger.snapshot(WINDOW_DIFFICULTY).await?;
            let key = keys().1.public_key_hex();

            // The victim is claimed on the issued balances and held, unlanded,
            // while another block of the same window settles beneath it.
            let (victim, victim_bundle) = candidate_for(&issued, 20)?;
            let victim_hash = victim.block_hash.clone();
            ensure!(ledger.enqueue_candidate_observed(victim, None).await?);
            let victim_claim = claim_expecting(&ledger, &victim_hash, victim_bundle).await?;

            // The mover moves the canonical balances the way production does:
            // it lands and is confirmed, and the carry triggers do the rest.
            // An UPDATE or a hand-written carry row would move the digest
            // without moving the balances the landing is judged against.
            let (mover, mover_bundle) = candidate_for(&issued, 21)?;
            let mover_hash = mover.block_hash.clone();
            ensure!(ledger.enqueue_candidate_observed(mover, None).await?);
            let mover_claim = claim_expecting(&ledger, &mover_hash, mover_bundle).await?;
            offer(&ledger, &mover_claim).await?;
            let revision = ledger.payout_revision().await?;
            ledger
                .land_candidate_at_revision(&mover_claim, &key, revision)
                .await?;
            ledger
                .finish_candidate_at_revision(&mover_claim, true, None, revision)
                .await?;

            // Without this the fixture could pass the control's assertions by
            // accident: a mover that moved nothing raises no alert either.
            let as_issued = qbit_prism::prior_balances_digest(&issued.prior_balances);
            let current = qbit_prism::prior_balances_digest(
                &ledger.snapshot(WINDOW_DIFFICULTY).await?.prior_balances,
            );
            ensure!(
                current != as_issued,
                "the mover left the canonical balances where the victim was issued on"
            );
            ensure!(
                victim_claim.candidate.window.prior_balances_digest == as_issued,
                "the victim does not carry the issued digest"
            );
            // miner-c is the account the mover moved: no seeded balance, and
            // the mover's accrual gives it one.
            let moved = current_balance(&db.pool, MINER_C_PROGRAM).await?;
            ensure!(moved > 0, "the mover accrued nothing to miner-c");

            offer(&ledger, &victim_claim).await?;
            let capture = LogCapture::default();
            let report = land_capturing(&ledger, &victim_claim, &key, &capture).await?;

            // (i) The divergence is reported once, with both digests.
            let events = capture.events()?;
            let alerts: Vec<&Value> = events
                .iter()
                .filter(|event| {
                    event["level"] == "WARN"
                        && event["fields"]["message"]
                            .as_str()
                            .is_some_and(|message| message.starts_with(DIVERGENCE_ALERT))
                })
                .collect();
            ensure!(
                alerts.len() == 1,
                "expected one divergence alert, captured {}",
                capture.text()
            );
            let fields = &alerts[0]["fields"];
            ensure!(fields["block"] == victim_hash.as_str(), "{fields}");
            ensure!(fields["state"] == "offered", "{fields}");
            ensure!(
                fields["as_issued_balances"] == hex::encode(as_issued),
                "{fields}"
            );
            ensure!(
                fields["current_balances"] == hex::encode(current),
                "{fields}"
            );
            ensure!(
                fields["as_issued_balances"] != fields["current_balances"],
                "the alert reported one digest twice: {fields}"
            );

            // (ii) The provenance marker records the audit that was issued.
            let marker = as_issued_marker(&db.pool, &victim_hash).await?;
            ensure!(
                marker.as_deref() == Some(report.audit_bundle_sha256_hex.as_str()),
                "the divergent landing did not record its as-issued provenance: {marker:?}"
            );

            // (iii) The carry rows are the issued accounts, not the moved
            // ones: the victim was issued when miner-c carried nothing, and
            // that is the prior it lands with even though miner-c now carries
            // the mover's accrual.
            let victim_prior =
                landed_prior_balance(&db.pool, &victim_hash, MINER_C_PROGRAM).await?;
            ensure!(
                victim_prior == 0,
                "the landed carry row is not the issued account: miner-c prior \
                 {victim_prior}, current {moved}"
            );
            ensure!(
                landed_prior_balance(&db.pool, &victim_hash, &"11".repeat(32)).await? == 1_000,
                "the victim's seeded account did not land on its issued prior either"
            );

            // (iv) The as-issued validator accepts the pair it just created.
            let revision = ledger.payout_revision().await?;
            ledger
                .finish_candidate_at_revision(&victim_claim, true, None, revision)
                .await?;
            let (mismatches, drift, reasons) = db.integrity().await?;
            ensure!(mismatches == 0 && drift == 0, "{reasons:?} drift {drift}");
            Ok(())
        })
    })
    .await
}

/// The control the alert has to be read against: the same fixture with no
/// mover. The landing is not divergent, so nothing is reported — but the
/// provenance marker is written all the same, because it records how the
/// block was issued and not whether the balances moved.
#[tokio::test]
async fn an_undisturbed_offered_landing_is_marked_without_a_divergence_alert() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("no-divergence").await?;
            seed_carry(&db.pool).await?;
            seed_divergence_window(&ledger).await?;
            let issued = ledger.snapshot(WINDOW_DIFFICULTY).await?;
            let key = keys().1.public_key_hex();
            let (victim, bundle) = candidate_for(&issued, 20)?;
            let hash = victim.block_hash.clone();
            ensure!(ledger.enqueue_candidate_observed(victim, None).await?);
            let claim = claim_expecting(&ledger, &hash, bundle).await?;
            offer(&ledger, &claim).await?;

            let capture = LogCapture::default();
            let report = land_capturing(&ledger, &claim, &key, &capture).await?;

            ensure!(
                !capture.text().contains(DIVERGENCE_ALERT),
                "an undisturbed landing reported a divergence: {}",
                capture.text()
            );
            let marker = as_issued_marker(&db.pool, &hash).await?;
            ensure!(
                marker.as_deref() == Some(report.audit_bundle_sha256_hex.as_str()),
                "every fresh landing records its provenance, divergent or not: {marker:?}"
            );
            let prior = landed_prior_balance(&db.pool, &hash, &"11".repeat(32)).await?;
            ensure!(
                prior == 1_000,
                "the landed carry row is not the issued account: prior {prior}"
            );

            let revision = ledger.payout_revision().await?;
            ledger
                .finish_candidate_at_revision(&claim, true, None, revision)
                .await?;
            let (mismatches, drift, reasons) = db.integrity().await?;
            ensure!(mismatches == 0 && drift == 0, "{reasons:?} drift {drift}");
            Ok(())
        })
    })
    .await
}
