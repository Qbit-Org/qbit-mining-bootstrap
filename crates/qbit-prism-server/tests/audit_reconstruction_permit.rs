//! The permit that bounds native audit reconstruction (#460).
//!
//! `materialize_audit_row` verifies the share snapshot, rebuilds the counted
//! window and recomputes the canonical digest on one blocking thread, and
//! hands that job the caller's permit rather than holding it in the caller's
//! future: Tokio keeps running a queued blocking job after the future that
//! awaited it is dropped, and the permit must bound that job. In production
//! the permit comes from one process-wide semaphore sized by
//! `PRISM_POSTGRES_READ_CONCURRENCY`; these tests drive the function directly
//! with permits from a one-permit semaphore of their own.
//!
//! Ordering here is a fact, not a race: the runtime has a single blocking
//! thread, the test parks it, and every wait is a bounded poll of
//! `Semaphore::available_permits`, the checkout counter or the pool's only
//! connection. No assertion depends on how long anything takes.
//!
//! The imported-audit twin of this contract, `decode_canonical_audit_body`,
//! is covered by `tests/support/ledger_2x.rs`.
//!
//! ```text
//! test/prism-native-tests.sh cargo-args --locked -p qbit-prism-server \
//!   --test audit_reconstruction_permit
//! ```

use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, canonical_audit_bundle_bytes, verify_audit_bundle_with_ledger_public_key,
    AcceptedShare, AuditBundle, FoundBlock, PayoutPolicy,
};
use qbit_prism_server::ledger::{materialize_audit_row, Candidate, Ledger, SignerKeys, WindowRef};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::task::AbortOnDropHandle;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// Bounds every wait. Nothing is asserted from how long a wait took.
const WAIT: Duration = Duration::from_secs(10);
/// How long "the permit has not come back" is held open before it is believed.
/// A release the job did not perform would be immediate, so this only has to
/// outlast the scheduling of the callers that were just dropped or queued.
const HOLD: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// Fixture: one database per test, and the landed native block to reconstruct
// ---------------------------------------------------------------------------

/// A fixture database with one schema on `search_path`. `url` copies the
/// fixture's, for the ledger and the read pool built on it.
struct Database {
    fixture: FixtureDatabase,
    url: String,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let fixture = FixtureDatabase::open(&raw, "prism_audit_permit_").await?;
        Ok(Some(Self {
            url: fixture.url.clone(),
            fixture,
        }))
    }

    async fn ledger(&self, id: &str) -> Result<Ledger> {
        Ledger::connect(&self.url, id.to_owned(), 8, true).await
    }

    /// The database goes away on success and on failure alike.
    async fn close(self, ledgers: Vec<Ledger>) -> Result<()> {
        for ledger in ledgers {
            ledger.pool.close().await;
        }
        self.fixture.close(Ok(())).await
    }
}

fn keys() -> (ManifestSigningKey, ManifestSigningKey) {
    (
        ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap(),
        ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap(),
    )
}

fn ledger_public_key() -> String {
    keys().1.public_key_hex()
}

fn share(id: u64) -> AcceptedShare {
    AcceptedShare {
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

/// The `candidate_with_bundle` recipe from `tests/audit_body_normalization.rs`:
/// an 80-byte header whose double SHA-256 is the candidate's `block_hash`,
/// with the verified coinbase transaction as the block's first transaction.
fn candidate(
    bundle: &AuditBundle,
    window: WindowRef,
    payout_revision: i64,
    nonce: u32,
) -> Result<Candidate> {
    let report = verify_audit_bundle_with_ledger_public_key(bundle, &ledger_public_key())?;
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
    let (coinbase_key, ledger_key) = keys();
    Ok(Candidate {
        block_hash: hex::encode(hash),
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "audit-permit-job".into(),
        payout_revision,
        window,
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
    })
}

/// One landed native block and the bundle it was landed from.
struct Landed {
    hash: String,
    /// The bundle as it was signed, before landing dropped its share copy.
    logical: Value,
}

/// Append one share, snapshot, build a signed candidate, enqueue, claim and
/// land it through the production path. Then append a second share, so the
/// ledger has moved past the block's anchored range before anything reads the
/// row back: a reconstruction that rebuilt the window from present-day ledger
/// state instead of the snapshot range would otherwise be indistinguishable
/// from the correct one.
async fn land_small_block(ledger: &Ledger, nonce: u32) -> Result<Landed> {
    ledger.append(share(1), None).await?;
    let snapshot = ledger.snapshot(100).await?;
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
    let logical = serde_json::to_value(&bundle)?;
    let candidate = candidate(
        &bundle,
        WindowRef::from_snapshot(&snapshot)?,
        snapshot.payout_revision,
        nonce,
    )?;
    let hash = candidate.block_hash.clone();
    ledger.enqueue_candidate(candidate).await?;
    let claim = ledger
        .claim_candidate(60)
        .await?
        .context("no pending candidate to claim")?
        .with_bundle(bundle);
    ledger.land_candidate(&claim, &ledger_public_key()).await?;
    ledger.append(share(2), None).await?;
    Ok(Landed { hash, logical })
}

impl Landed {
    /// The logical row a caller hands `materialize_audit_row`, built from the
    /// stored row exactly as `audit_canonical_bytes` builds it.
    async fn row(&self, pool: &PgPool) -> Result<Value> {
        let row = sqlx::query("SELECT audit_bundle,audit_bundle_sha256,share_snapshot_sha256 FROM qbit_pool_audit_bundles WHERE block_hash=$1")
            .bind(&self.hash)
            .fetch_one(pool)
            .await?;
        let body: Option<Value> = row.try_get("audit_bundle")?;
        let expected: String = row.try_get("audit_bundle_sha256")?;
        let snapshot: Option<String> = row.try_get("share_snapshot_sha256")?;
        ensure!(
            snapshot.is_some(),
            "the landed row carries no share snapshot to reconstruct from"
        );
        // The window is rebuilt, not copied out of the row (#267).
        ensure!(
            body.as_ref().is_some_and(|body| body["shares"].is_null()),
            "the landed row already holds a share copy"
        );
        Ok(json!({
            "audit_bundle": body,
            "audit_bundle_sha256": expected,
            "share_snapshot_sha256": snapshot,
        }))
    }

    /// The materialized row carries the signed body back, counted window and
    /// all, and its canonical digest is the one the row declares.
    fn assert_hydrated(&self, row: &Value) -> Result<()> {
        assert_eq!(row["audit_bundle"], self.logical, "materialized audit body");
        let bundle: AuditBundle = serde_json::from_value(row["audit_bundle"].clone())?;
        assert_eq!(
            hex::encode(Sha256::digest(canonical_audit_bundle_bytes(&bundle)?)),
            row["audit_bundle_sha256"]
                .as_str()
                .context("the row declares no canonical digest")?,
            "canonical digest of the materialized body"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Runtime, blocking thread and the read pool the tests observe
// ---------------------------------------------------------------------------

/// One runtime per test: a single blocking thread is what serializes the
/// reconstruction jobs, and two worker threads keep the driving task running
/// while a reconstruction waits.
fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?)
}

/// The runtime's only blocking thread, parked until this is released or
/// dropped, so a reconstruction's job stays queued. Everything the test needs
/// from PostgreSQL is connected before the park: a new connection would
/// otherwise queue behind this thread.
struct ParkedBlockingThread {
    release: std::sync::mpsc::Sender<()>,
    worker: tokio::task::JoinHandle<()>,
}

impl ParkedBlockingThread {
    async fn park() -> Result<Self> {
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let (started, running) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            // A disconnected sender also unparks the thread on a failed
            // assertion, so a panicking test never hangs the runtime.
            let _ = blocked.recv();
        });
        tokio::time::timeout(WAIT, running).await??;
        Ok(Self { release, worker })
    }

    /// Release the thread and wait for it to leave, so the jobs queued behind
    /// it start from a free blocking pool.
    async fn release(self) -> Result<()> {
        let Self { release, worker } = self;
        drop(release);
        Ok(tokio::time::timeout(WAIT, worker).await??)
    }
}

/// A one-connection read pool that counts its checkouts. A reconstruction
/// takes one for the snapshot row and one for the share range; the count says
/// which statement it is on, and re-acquiring the only connection afterwards
/// proves the second one finished.
struct ReadPool {
    pool: PgPool,
    checkouts: Arc<AtomicUsize>,
}

impl ReadPool {
    async fn open(url: &str) -> Result<Self> {
        let checkouts = Arc::new(AtomicUsize::new(0));
        // connect() leaves a warm idle connection, so every later checkout is
        // a reuse and calls the hook.
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(WAIT)
            .before_acquire({
                let checkouts = checkouts.clone();
                move |_, _| {
                    checkouts.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(true) })
                }
            })
            .connect(url)
            .await?;
        Ok(Self { pool, checkouts })
    }

    fn checkouts(&self) -> usize {
        self.checkouts.load(Ordering::SeqCst)
    }
}

/// A bounded poll. `what` names the condition for the timeout message.
async fn wait_for(mut ready: impl FnMut() -> bool, what: &str) -> Result<()> {
    tokio::time::timeout(WAIT, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {what}"))
}

/// The absence of a release cannot be awaited, so it is held open: re-check
/// over [`HOLD`] while the blocking thread stays parked. A caller that
/// released the permit itself would have done so before the first check.
async fn holds_at_zero(semaphore: &Semaphore, what: &str) {
    let checks = 20;
    for _ in 0..checks {
        assert_eq!(semaphore.available_permits(), 0, "{what}");
        tokio::time::sleep(HOLD / checks).await;
    }
}

/// Start a reconstruction of `row` holding `permit`, and return once it has
/// queued its blocking job: the checkout counter shows the range statement
/// began, re-acquiring the pool's only connection shows it finished, and
/// everything between that statement and `spawn_blocking` is synchronous.
async fn queued_reconstruction(
    read: &ReadPool,
    mut row: Value,
    permit: OwnedSemaphorePermit,
) -> Result<AbortOnDropHandle<Result<Value>>> {
    let before = read.checkouts();
    let pool = read.pool.clone();
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        materialize_audit_row(&pool, &mut row, Some(permit)).await?;
        Ok(row)
    }));
    wait_for(
        || read.checkouts() == before + 2,
        "the reconstruction's share range statement",
    )
    .await?;
    let connection = tokio::time::timeout(WAIT, read.pool.acquire()).await??;
    assert!(
        !task.is_finished(),
        "the reconstruction ran its blocking job on the parked thread"
    );
    drop(connection);
    Ok(task)
}

// ---------------------------------------------------------------------------
// The contract
// ---------------------------------------------------------------------------

/// A caller that goes away mid-flight does not release the bound: the job it
/// queued keeps running, and keeps the permit until it ends.
#[test]
fn a_queued_reconstruction_keeps_its_permit_after_its_caller_is_dropped() -> Result<()> {
    runtime()?.block_on(async {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let ledger = db.ledger("audit-permit-retention").await?;
        let result = retention_case(&db, &ledger).await;
        result.and(db.close(vec![ledger]).await)
    })
}

async fn retention_case(db: &Database, ledger: &Ledger) -> Result<()> {
    let landed = land_small_block(ledger, 46001).await?;
    let read = ReadPool::open(&db.url).await?;
    let row = landed.row(&read.pool).await?;
    let semaphore = Arc::new(Semaphore::new(1));
    let parked = ParkedBlockingThread::park().await?;

    let permit = semaphore.clone().acquire_owned().await?;
    assert_eq!(semaphore.available_permits(), 0);
    let task = queued_reconstruction(&read, row.clone(), permit).await?;

    // The caller goes away with its job already queued.
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    holds_at_zero(
        &semaphore,
        "a dropped caller released the permit its queued job still holds",
    )
    .await;

    // Only the job's own end returns it.
    parked.release().await?;
    wait_for(
        || semaphore.available_permits() == 1,
        "the finished job to release its permit",
    )
    .await?;

    // The bound is intact afterwards: the same permit reconstructs the row.
    let mut hydrated = row;
    materialize_audit_row(
        &read.pool,
        &mut hydrated,
        Some(semaphore.clone().acquire_owned().await?),
    )
    .await?;
    landed.assert_hydrated(&hydrated)?;
    assert_eq!(semaphore.available_permits(), 1);
    read.pool.close().await;
    Ok(())
}

/// Two reconstructions of one permit run one after the other, and both hydrate
/// their row. The second cannot acquire before the first job ends, so it never
/// reaches a statement of its own meanwhile.
#[test]
fn a_second_reconstruction_waits_for_the_permit_and_both_rows_hydrate() -> Result<()> {
    runtime()?.block_on(async {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let ledger = db.ledger("audit-permit-serialization").await?;
        let result = serialization_case(&db, &ledger).await;
        result.and(db.close(vec![ledger]).await)
    })
}

async fn serialization_case(db: &Database, ledger: &Ledger) -> Result<()> {
    let landed = land_small_block(ledger, 46002).await?;
    let read = ReadPool::open(&db.url).await?;
    let row = landed.row(&read.pool).await?;
    let semaphore = Arc::new(Semaphore::new(1));
    let parked = ParkedBlockingThread::park().await?;

    let permit = semaphore.clone().acquire_owned().await?;
    let first = queued_reconstruction(&read, row.clone(), permit).await?;

    let before = read.checkouts();
    let second = AbortOnDropHandle::new(tokio::spawn({
        let semaphore = semaphore.clone();
        let pool = read.pool.clone();
        let mut row = row;
        async move {
            let permit = semaphore.acquire_owned().await?;
            materialize_audit_row(&pool, &mut row, Some(permit)).await?;
            Ok::<_, anyhow::Error>(row)
        }
    }));
    holds_at_zero(
        &semaphore,
        "the waiting caller took a permit the queued job still holds",
    )
    .await;
    assert!(!second.is_finished());
    assert_eq!(
        read.checkouts(),
        before,
        "the waiting caller read before its turn"
    );

    parked.release().await?;
    landed.assert_hydrated(&tokio::time::timeout(WAIT, first).await???)?;
    landed.assert_hydrated(&tokio::time::timeout(WAIT, second).await???)?;
    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(
        read.checkouts(),
        before + 2,
        "the second reconstruction read the same two statements"
    );
    read.pool.close().await;
    Ok(())
}

/// A reconstruction that fails or is cancelled returns the permit
/// exactly once, whichever side of the blocking job it ends on: with the
/// future when no job was queued, and with the job when one was.
#[test]
fn failed_and_cancelled_reconstructions_release_the_permit() -> Result<()> {
    runtime()?.block_on(async {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let ledger = db.ledger("audit-permit-release").await?;
        let result = release_cases(&db, &ledger).await;
        result.and(db.close(vec![ledger]).await)
    })
}

async fn release_cases(db: &Database, ledger: &Ledger) -> Result<()> {
    let landed = land_small_block(ledger, 46003).await?;
    let read = ReadPool::open(&db.url).await?;
    let row = landed.row(&read.pool).await?;
    let semaphore = Arc::new(Semaphore::new(1));
    // Parked for the whole case: a permit that comes back while no job can run
    // was released with its future, not by a job.
    let parked = ParkedBlockingThread::park().await?;

    // Cancelled before the job is queued. The only connection is held, so the
    // snapshot lookup has not returned and there is no job to hold anything.
    let held = tokio::time::timeout(WAIT, read.pool.acquire()).await??;
    let permit = semaphore.clone().acquire_owned().await?;
    let cancelled = AbortOnDropHandle::new(tokio::spawn({
        let pool = read.pool.clone();
        let mut row = row.clone();
        async move { materialize_audit_row(&pool, &mut row, Some(permit)).await }
    }));
    holds_at_zero(
        &semaphore,
        "a reconstruction waiting on its first statement released its permit",
    )
    .await;
    assert!(!cancelled.is_finished());
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    assert_eq!(
        semaphore.available_permits(),
        1,
        "a cancelled reconstruction kept a permit no job holds"
    );
    drop(held);

    // Failure before the job: the snapshot the row names does not exist, so
    // the reconstruction ends at its first statement.
    let mut missing = row.clone();
    missing["share_snapshot_sha256"] = json!("0".repeat(64));
    let permit = semaphore.clone().acquire_owned().await?;
    assert_eq!(semaphore.available_permits(), 0);
    let error = materialize_audit_row(&read.pool, &mut missing, Some(permit))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::RowNotFound)
        ),
        "{error:#}"
    );
    assert_eq!(
        semaphore.available_permits(),
        1,
        "a reconstruction that failed before its job kept the permit"
    );

    // Failure inside the job: the row declares a digest the materialized body
    // cannot have. The permit is the job's until the job ends.
    let mut mismatched = row;
    mismatched["audit_bundle_sha256"] = json!("0".repeat(64));
    let permit = semaphore.clone().acquire_owned().await?;
    let failing = queued_reconstruction(&read, mismatched, permit).await?;
    holds_at_zero(
        &semaphore,
        "the failing job released its permit before it ran",
    )
    .await;
    parked.release().await?;
    let error = tokio::time::timeout(WAIT, failing).await??.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("materialized audit body digest mismatch"),
        "{error:#}"
    );
    wait_for(
        || semaphore.available_permits() == 1,
        "the failed job to release its permit",
    )
    .await?;
    read.pool.close().await;
    Ok(())
}
