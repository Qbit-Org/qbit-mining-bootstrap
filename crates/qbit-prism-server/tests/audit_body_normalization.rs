//! Native audit bodies without a share copy (#267).
//!
//! A landed native block stores its audit body minus the top-level `shares`
//! and minus `reward_manifest.shares`. The counted window is rebuilt on read
//! from the immutable share ledger and proven byte-identical by the canonical
//! digest. These tests cover the stored shape, the read path for both native
//! shapes (normalized and pre-#267), imported rows, which never enter the
//! native reconstruction, and the paged durable-range proof that runs before
//! the settlement lock. An `#[ignore]` harness measures the stored body and
//! the settlement-lock hold at a configurable window size.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test audit_body_normalization
//! ```
//!
//! Measurement, at `PRISM_AUDIT_BODY_MEASURE_SHARES` shares (default 20,000;
//! the count must divide the 8,000,000 window weight):
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=... PRISM_AUDIT_BODY_MEASURE_SHARES=100000 \
//!   cargo test --locked -p qbit-prism-server --test audit_body_normalization \
//!   -- --ignored --nocapture measure_landing_body_and_settlement_lock_hold
//! ```

use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, canonical_audit_bundle_bytes, verify_audit_bundle_with_ledger_public_key,
    AcceptedShare, AuditBundle, FoundBlock, PayoutPolicy,
};
use qbit_prism_server::ledger::{
    audit_canonical_bytes, Candidate, CandidateClaim, Ledger, ShareRange, SignerKeys, Snapshot,
    WindowRef,
};
use qbit_prism_test_gate as gate;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[path = "support/window_fixture.rs"]
mod window_fixture;
use window_fixture::WindowPlan;

#[path = "support/audit_acquire_metrics.rs"]
mod acquire_metrics;

#[path = "support/audit_reconstruction_metrics.rs"]
mod reconstruction_metrics;

// ---------------------------------------------------------------------------
// Per-test database and the small fixture from `tests/ledger_postgres.rs`
// ---------------------------------------------------------------------------

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// A fixture database with one schema on `search_path`. `url` copies the
/// fixture's, for the included modules that read it. `admin` is a
/// default-size pool on that same database, separate from the ledgers' pools;
/// the measurement watches `pg_locks` through it.
struct Database {
    fixture: FixtureDatabase,
    admin: PgPool,
    url: String,
}

impl Database {
    async fn open_raw(raw: &str) -> Result<Self> {
        let fixture = FixtureDatabase::open(raw, "prism_audit_body_").await?;
        let admin = match PgPool::connect(&fixture.url).await {
            Ok(admin) => admin,
            Err(error) => return Err(fixture.abandon(error.into()).await),
        };
        Ok(Self {
            admin,
            url: fixture.url.clone(),
            fixture,
        })
    }

    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        Ok(Some(Self::open_raw(&raw).await?))
    }

    /// The `#[ignore]` harness is asked for explicitly, so a missing URL fails
    /// it whatever the switch says: a skip that reported `ok` would be the
    /// vacuous pass the gate exists to prevent.
    async fn open_required() -> Result<Self> {
        Self::open_raw(&gate::required_database_url(gate::site!())?).await
    }

    async fn ledger(&self, id: &str) -> Result<Ledger> {
        Ledger::connect(&self.url, id.to_owned(), 8, true).await
    }

    /// EP-ERRORS: the database goes away on success and on failure alike; a
    /// fixture that never reaches `close` is dropped by the helper's fallback.
    async fn close(self, ledgers: Vec<Ledger>) -> Result<()> {
        for ledger in ledgers {
            ledger.pool.close().await;
        }
        self.admin.close().await;
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

/// The `candidate_with_bundle` recipe from `tests/ledger_postgres.rs`: an
/// 80-byte header whose double SHA-256 is the candidate's `block_hash`, with
/// the verified coinbase transaction as the block's first transaction.
/// A candidate and the assembled bundle it was built from. Since #265 the
/// stored candidate holds a window reference rather than the bundle, so a test
/// that needs both keeps them side by side, as `tests/ledger_postgres.rs` does.
struct TestCandidate {
    candidate: Candidate,
    bundle: AuditBundle,
}

impl TestCandidate {
    /// Attach the parts a landing needs, which the claim no longer carries.
    fn claim(&self, claim: CandidateClaim) -> CandidateClaim {
        claim.with_bundle(self.bundle.clone())
    }
}

fn candidate_with_bundle(
    bundle: AuditBundle,
    window: WindowRef,
    payout_revision: i64,
    nonce: u32,
) -> Result<TestCandidate> {
    let report = verify_audit_bundle_with_ledger_public_key(&bundle, &ledger_public_key())?;
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
    let candidate = Candidate {
        block_hash: hex::encode(hash),
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "audit-body-job".into(),
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
    };
    Ok(TestCandidate { candidate, bundle })
}

/// The reference a candidate carries for `shares`, derived from those shares
/// rather than from the snapshot. The negative cases below hand the claim a
/// window the ledger does not hold; their reference has to describe the window
/// they actually carry, or landing would refuse them at the reference digest
/// before the durable-range proof ever runs, and the proof is what is under
/// test.
fn window_ref_for(shares: &[AcceptedShare], snapshot: &Snapshot) -> Result<WindowRef> {
    let range = match (shares.first(), shares.last()) {
        (Some(first), Some(last)) => Some(ShareRange {
            first_share_seq: first.share_seq,
            last_share_seq: last.share_seq,
            share_count: u64::try_from(shares.len())?,
            snapshot_sha256: Sha256::digest(serde_json::to_vec(shares)?).into(),
        }),
        _ => None,
    };
    Ok(WindowRef {
        anchor_ms: snapshot.anchor_ms,
        prior_balances_digest: qbit_prism::prior_balances_digest(&snapshot.prior_balances),
        shares: range,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// One landed block and the artifact it was landed from: the canonical bytes
/// and the logical value of the candidate bundle, both taken before landing.
struct Landed {
    claim: CandidateClaim,
    bundle: AuditBundle,
    canonical: Vec<u8>,
    logical: Value,
}

impl Landed {
    fn hash(&self) -> &str {
        &self.claim.candidate.block_hash
    }

    fn bundle(&self) -> &AuditBundle {
        &self.bundle
    }
}

/// Append one share, snapshot, build a signed candidate, enqueue, claim and
/// land it through the production path. Then append a second share, so the
/// ledger has moved past the block's anchored range before anything reads
/// the row back: a read that rebuilt the window from present-day ledger
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
    let canonical = canonical_audit_bundle_bytes(&bundle)?;
    let logical = serde_json::to_value(&bundle)?;
    let block = candidate_with_bundle(
        bundle,
        WindowRef::from_snapshot(&snapshot)?,
        snapshot.payout_revision,
        nonce,
    )?;
    ledger.enqueue_candidate(block.candidate.clone()).await?;
    let claim = block.claim(
        ledger
            .claim_candidate(60)
            .await?
            .context("no pending candidate to claim")?,
    );
    ledger.land_candidate(&claim, &ledger_public_key()).await?;
    ledger.append(share(2), None).await?;
    Ok(Landed {
        claim,
        bundle: block.bundle,
        canonical,
        logical,
    })
}

/// What the row holds, read straight from the table.
#[derive(Debug)]
struct StoredRow {
    top_shares: bool,
    manifest_shares: bool,
    native: bool,
    imported: bool,
    /// `pg_column_size(audit_bundle)`: what the row occupies, TOAST-compressed.
    stored_bytes: i64,
    /// `octet_length(audit_bundle::text)`: the JSON text form.
    text_len: i64,
    audit_body_byte_len: Option<i64>,
    /// The `audit_commitment_leaves_hex` member the GIN index is built on.
    commitment_index_key: bool,
}

async fn stored_row(pool: &PgPool, hash: &str) -> Result<StoredRow> {
    let row = sqlx::query(
        "SELECT COALESCE(audit_bundle ? 'shares',false) AS top_shares,\
         COALESCE(audit_bundle->'reward_manifest' ? 'shares',false) AS manifest_shares,\
         share_snapshot_sha256 IS NOT NULL AS native,\
         canonical_audit_bytes IS NOT NULL AS imported,\
         COALESCE(pg_column_size(audit_bundle),0)::bigint AS stored_bytes,\
         COALESCE(octet_length(audit_bundle::text),0)::bigint AS text_len,\
         audit_body_byte_len,\
         COALESCE(audit_bundle ? 'audit_commitment_leaves_hex',false) AS commitment_index_key \
         FROM qbit_pool_audit_bundles WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?;
    Ok(StoredRow {
        top_shares: row.try_get("top_shares")?,
        manifest_shares: row.try_get("manifest_shares")?,
        native: row.try_get("native")?,
        imported: row.try_get("imported")?,
        stored_bytes: row.try_get("stored_bytes")?,
        text_len: row.try_get("text_len")?,
        audit_body_byte_len: row.try_get("audit_body_byte_len")?,
        commitment_index_key: row.try_get("commitment_index_key")?,
    })
}

async fn stored_body(pool: &PgPool, hash: &str) -> Result<Value> {
    Ok(
        sqlx::query_scalar("SELECT audit_bundle FROM qbit_pool_audit_bundles WHERE block_hash=$1")
            .bind(hash)
            .fetch_one(pool)
            .await?,
    )
}

async fn assert_serves(ledger: &Ledger, landed: &Landed, what: &str) -> Result<()> {
    let served = audit_canonical_bytes(&ledger.pool, landed.hash())
        .await
        .with_context(|| format!("{what}: canonical bytes"))?;
    ensure!(
        served.as_deref() == Some(landed.canonical.as_slice()),
        "{what}: served canonical bytes differ from the candidate's"
    );
    let hydrated = ledger
        .audit_bundle(landed.hash())
        .await
        .with_context(|| format!("{what}: logical body"))?;
    ensure!(
        hydrated.as_ref() == Some(&landed.logical),
        "{what}: hydrated logical body differs from the candidate bundle"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Criterion 3: the row stores neither share copy, `audit_body_byte_len`
/// stays the full canonical length, and the served bytes equal the
/// pre-landing canonical bytes. A tampered stored header is refused by name,
/// and an older reader cannot mistake the row for a full bundle.
#[tokio::test]
async fn landed_native_body_stores_no_share_copy_and_serves_identical_bytes() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("a").await?;
    let landed = land_small_block(&ledger, 2671).await?;
    let row = stored_row(&ledger.pool, landed.hash()).await?;
    ensure!(
        row.native && !row.imported,
        "landed row is not native: {row:?}"
    );
    ensure!(
        !row.top_shares,
        "stored body still carries the top-level shares"
    );
    ensure!(
        !row.manifest_shares,
        "stored body still carries reward_manifest.shares"
    );
    ensure!(
        row.commitment_index_key,
        "the GIN-indexed commitment leaves left the stored body"
    );
    let canonical_len = i64::try_from(landed.canonical.len())?;
    ensure!(
        row.audit_body_byte_len == Some(canonical_len),
        "audit_body_byte_len must stay the full canonical artifact length: {row:?}"
    );
    ensure!(
        row.text_len < canonical_len,
        "stored body is not smaller than the canonical artifact: {row:?}"
    );
    assert_serves(&ledger, &landed, "normalized row").await?;

    // EP-COMPAT, forward: a reader from before this change would insert the
    // top-level shares and decode the body as a bundle. That decode fails
    // explicitly; it can never serve a manifest with an empty window.
    let mut older = stored_body(&ledger.pool, landed.hash()).await?;
    older["shares"] = serde_json::to_value(&landed.bundle().shares)?;
    let error = AuditBundle::deserialize(&older)
        .expect_err("a pre-#267 reader decoded the normalized row as a bundle");
    ensure!(
        error.to_string().contains("missing field `shares`"),
        "unexpected decode error: {error}"
    );

    // A stored header the rebuild does not reproduce is refused as the
    // reward_manifest mismatch, not only later at the canonical digest.
    let original_digest: String = sqlx::query_scalar(
        "SELECT audit_bundle->'reward_manifest'->>'share_slice_digest_hex' FROM qbit_pool_audit_bundles WHERE block_hash=$1",
    )
    .bind(landed.hash())
    .fetch_one(&ledger.pool)
    .await?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=jsonb_set(audit_bundle,'{reward_manifest,share_slice_digest_hex}',to_jsonb($2::text)) WHERE block_hash=$1")
        .bind(landed.hash())
        .bind("00".repeat(32))
        .execute(&ledger.pool)
        .await?;
    for (what, error) in [
        (
            "canonical bytes",
            audit_canonical_bytes(&ledger.pool, landed.hash())
                .await
                .err(),
        ),
        (
            "logical body",
            ledger.audit_bundle(landed.hash()).await.err(),
        ),
    ] {
        let error = error.with_context(|| format!("{what}: a tampered header was served"))?;
        let text = format!("{error:#}");
        ensure!(
            text.contains("reward_manifest"),
            "{what}: tampered header failed without naming reward_manifest: {text}"
        );
    }
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=jsonb_set(audit_bundle,'{reward_manifest,share_slice_digest_hex}',to_jsonb($2::text)) WHERE block_hash=$1")
        .bind(landed.hash())
        .bind(&original_digest)
        .execute(&ledger.pool)
        .await?;
    assert_serves(&ledger, &landed, "restored row").await?;
    db.close(vec![ledger]).await
}

/// Criterion 4 (EP-COMPAT, existing data): a native row written before this
/// change, with `reward_manifest.shares` still inline, serves identical bytes
/// through the unchanged legacy arm, and that arm still verifies the body.
#[tokio::test]
async fn legacy_native_rows_with_manifest_shares_still_serve_identical_bytes() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("a").await?;
    let landed = land_small_block(&ledger, 2672).await?;
    // Reshape the row into exactly what `land_candidate_checked` wrote before
    // this change: the logical bundle minus the top-level `shares` only.
    let manifest_shares = serde_json::to_value(&landed.bundle().reward_manifest.shares)?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=jsonb_set(audit_bundle,'{reward_manifest,shares}',$2) WHERE block_hash=$1")
        .bind(landed.hash())
        .bind(&manifest_shares)
        .execute(&ledger.pool)
        .await?;
    let row = stored_row(&ledger.pool, landed.hash()).await?;
    ensure!(
        row.native && !row.imported && row.manifest_shares && !row.top_shares,
        "legacy reshaping did not produce the pre-#267 shape: {row:?}"
    );
    let mut legacy = stored_body(&ledger.pool, landed.hash()).await?;
    legacy["shares"] = serde_json::to_value(&landed.bundle().shares)?;
    ensure!(
        legacy == landed.logical,
        "legacy shape plus the window is not the logical bundle"
    );
    assert_serves(&ledger, &landed, "legacy row").await?;

    // The legacy arm keeps its own proof: a changed counted share fails the
    // canonical digest instead of being served.
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=jsonb_set(audit_bundle,'{reward_manifest,shares,0,counted_difficulty}','2') WHERE block_hash=$1")
        .bind(landed.hash())
        .execute(&ledger.pool)
        .await?;
    ensure!(
        audit_canonical_bytes(&ledger.pool, landed.hash())
            .await
            .is_err(),
        "a tampered legacy counted share was served"
    );
    ensure!(
        ledger.audit_bundle(landed.hash()).await.is_err(),
        "a tampered legacy counted share was hydrated"
    );
    db.close(vec![ledger]).await
}

/// Criterion 5: an imported row serves from its canonical bytes and never
/// enters the native reconstruction, even when its inline body would fail it.
#[tokio::test]
async fn imported_rows_serve_canonical_bytes_and_never_enter_native_reconstruction() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("a").await?;
    let landed = land_small_block(&ledger, 2673).await?;
    // Turn the row into a pre-native filesystem row and import it, as
    // `tests/support/ledger_2x.rs` does.
    let dir = tempfile::tempdir()?;
    let body_path = dir.path().join("legacy-audit.json");
    std::fs::write(&body_path, &landed.canonical)?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=NULL,share_snapshot_sha256=NULL,body_uri=$2 WHERE block_hash=$1")
        .bind(landed.hash())
        .bind(body_path.to_str().context("utf-8 temp path")?)
        .execute(&ledger.pool)
        .await?;
    ensure!(
        audit_canonical_bytes(&ledger.pool, landed.hash())
            .await?
            .is_none(),
        "a filesystem row has no canonical bytes before import"
    );
    ensure!(
        ledger
            .import_legacy_audits(Some(dir.path()), &ledger_public_key())
            .await?
            == 1
    );
    let row = stored_row(&ledger.pool, landed.hash()).await?;
    ensure!(
        row.imported && !row.native,
        "import did not produce an imported row: {row:?}"
    );
    assert_serves(&ledger, &landed, "imported row").await?;

    // Poison the only input the native path could read. Serving still
    // succeeds, so the row never entered that path.
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle='{\"schema\":\"not-an-audit\",\"reward_manifest\":{}}'::jsonb WHERE block_hash=$1")
        .bind(landed.hash())
        .execute(&ledger.pool)
        .await?;
    assert_serves(&ledger, &landed, "imported row with a poisoned inline body").await?;
    db.close(vec![ledger]).await
}

#[path = "support/audit_durable_range.rs"]
mod audit_durable_range;
// Keep these fixture names available to the sibling acquisition-metrics suite.
#[allow(unused_imports)]
use audit_durable_range::{claim_enqueued, signed_candidate, PROOF_WINDOW_SHARES};

// ---------------------------------------------------------------------------
// Measurement harness
// ---------------------------------------------------------------------------

/// The settlement advisory lock key, as `ledger.rs` defines it.
const SETTLEMENT_LOCK: i64 = 0x505249534d000003;
/// `pg_locks` sampling interval; every reported hold is a lower bound short
/// by at most one interval on each end.
const LOCK_POLL: Duration = Duration::from_millis(1);

/// Watch `pg_locks` from a second backend and record every interval during
/// which some backend held the settlement lock. A 64-bit advisory key shows
/// as its upper 32 bits in `classid` and its lower 32 bits in `objid`, with
/// `objsubid = 1`.
async fn watch_settlement_lock(
    pool: PgPool,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<Vec<Duration>> {
    let classid = SETTLEMENT_LOCK >> 32;
    let objid = SETTLEMENT_LOCK & 0xffff_ffff;
    let mut held_since: Option<Instant> = None;
    let mut intervals = Vec::new();
    loop {
        let held: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted \
             AND classid::bigint=$1 AND objid::bigint=$2 AND objsubid=1)",
        )
        .bind(classid)
        .bind(objid)
        .fetch_one(&pool)
        .await?;
        let now = Instant::now();
        match (held, held_since) {
            (true, None) => held_since = Some(now),
            (false, Some(since)) => {
                intervals.push(now - since);
                held_since = None;
            }
            _ => {}
        }
        if *stop.borrow_and_update() {
            if let Some(since) = held_since {
                intervals.push(now - since);
            }
            return Ok(intervals);
        }
        tokio::time::sleep(LOCK_POLL).await;
    }
}

/// One `/proc/self/status` field in KiB; `None` when the kernel does not
/// report it, never zero.
fn status_kib(field: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.trim().strip_suffix("kB"))
        .and_then(|kib| kib.trim().parse().ok())
}

/// Writing `5` to `clear_refs` resets `VmHWM`, so the next reading covers
/// only the phase that follows. It measures the test process, not the
/// PostgreSQL backend. Returns whether the reset took.
fn reset_peak_rss() -> bool {
    std::fs::write("/proc/self/clear_refs", "5").is_ok()
}

/// `VmHWM` since the matching `reset_peak_rss`; `None` when that reset
/// failed, so a reading would span earlier phases too.
fn peak_rss_kib(reset: bool) -> Option<u64> {
    if !reset {
        return None;
    }
    status_kib("VmHWM:")
}

fn cell(kib: Option<u64>) -> String {
    kib.map_or("unmeasured".to_owned(), |kib| {
        format!("{:.1} MiB", kib as f64 / 1024.0)
    })
}

fn measurement_shares() -> Result<u64> {
    match std::env::var("PRISM_AUDIT_BODY_MEASURE_SHARES") {
        Ok(raw) => raw.trim().parse().with_context(|| {
            format!("PRISM_AUDIT_BODY_MEASURE_SHARES={raw:?} is not a share count")
        }),
        Err(std::env::VarError::NotPresent) => Ok(20_000),
        Err(error) => Err(error).context("PRISM_AUDIT_BODY_MEASURE_SHARES is not readable"),
    }
}

async fn measure(db: &Database, ledger: &Ledger, plan: &WindowPlan) -> Result<()> {
    let n = plan.share_count();
    let pool = ledger.pool.clone();
    let load = plan.load(&pool, "audit-body-measure").await?;
    eprintln!(
        "[measure n={n}] loaded {} shares in {:.2} s",
        load.rows, load.seconds
    );
    let snapshot = ledger.snapshot(plan.window_network_difficulty()).await?;
    ensure!(
        snapshot.shares.len() as u64 == n,
        "snapshot window is {} shares, expected exactly {n}",
        snapshot.shares.len()
    );
    let payout_revision = snapshot.payout_revision;
    let anchor_ms = snapshot.anchor_ms;
    let reference = window_ref_for(&snapshot.shares, &snapshot)?;
    let (coinbase_key, ledger_key) = keys();
    let bundle = build_audit_bundle(
        snapshot.shares,
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 5_000_000_000,
            network_difficulty: plan.window_network_difficulty(),
            anchor_job_issued_at_ms: anchor_ms,
        },
        snapshot.prior_balances,
        PayoutPolicy::day_one_default(),
        &coinbase_key,
        &ledger_key,
    )?;
    let canonical = canonical_audit_bundle_bytes(&bundle)?;
    let canonical_len = canonical.len();
    let expected_sha = sha256_hex(&canonical);
    drop(canonical);
    let candidate = candidate_with_bundle(bundle, reference, payout_revision, 0x0267)?;
    let hash = candidate.candidate.block_hash.clone();
    let mut winning = plan.share(n);
    winning.share_seq = 0;
    winning.share_id = "audit-body-measure:winning-share".into();
    winning.job_issued_at_ms = 1;
    winning.accepted_at_ms = 0;
    ledger
        .append(winning, Some(candidate.candidate.clone()))
        .await?;
    let claim = candidate.claim(
        ledger
            .claim_candidate(600)
            .await?
            .context("claim found no pending candidate")?,
    );

    // The window-sized computations `land_candidate` still performs inside
    // the settlement transaction, timed on the claim's own bundle so the
    // residual lock hold can be attributed. They are pure, so timing them here
    // first changes nothing about the landing that follows.
    let bundle = &candidate.bundle;
    let clock = Instant::now();
    let _ = Sha256::digest(serde_json::to_vec(&bundle.shares)?);
    let snapshot_digest_seconds = clock.elapsed().as_secs_f64();
    let clock = Instant::now();
    let logical = serde_json::to_value(bundle)?;
    let to_value_seconds = clock.elapsed().as_secs_f64();
    drop(logical);
    let clock = Instant::now();
    let _ = canonical_audit_bundle_bytes(bundle)?.len();
    let canonical_len_seconds = clock.elapsed().as_secs_f64();
    eprintln!(
        "[measure n={n}] in-transaction pure work on the claim bundle: snapshot digest {:.3} s, \
         serde_json::to_value {:.3} s, canonical length {:.3} s",
        snapshot_digest_seconds, to_value_seconds, canonical_len_seconds
    );

    let (stop, watch) = tokio::sync::watch::channel(false);
    let watcher = tokio::spawn(watch_settlement_lock(db.admin.clone(), watch));
    // Let the watcher take its first sample before the lock can be taken.
    tokio::time::sleep(LOCK_POLL * 20).await;
    // Resident memory just before landing is the baseline the landing peak is
    // measured against: the claim, and nothing else of the window, is live.
    let rss_reset = reset_peak_rss();
    let rss_before = status_kib("VmRSS:");
    let clock = Instant::now();
    let landed = ledger
        .land_candidate(&claim, &ledger_key.public_key_hex())
        .await;
    let landing = clock.elapsed();
    let rss_peak = peak_rss_kib(rss_reset);
    // Observe the release before stopping.
    tokio::time::sleep(LOCK_POLL * 20).await;
    stop.send(true)?;
    let intervals = watcher.await??;
    landed.context("landing failed")?;
    let hold = intervals.iter().max().copied();

    let row = stored_row(&pool, &hash).await?;
    ensure!(
        row.native && !row.top_shares,
        "landed row is not a native range-backed body: {row:?}"
    );
    ensure!(
        row.audit_body_byte_len == Some(i64::try_from(canonical_len)?),
        "audit_body_byte_len is not the canonical artifact length: {row:?}"
    );
    let clock = Instant::now();
    let served = audit_canonical_bytes(&pool, &hash)
        .await?
        .context("landed block has no canonical bytes")?;
    let read_back = clock.elapsed();
    ensure!(
        served.len() == canonical_len && sha256_hex(&served) == expected_sha,
        "served canonical bytes differ from the candidate's"
    );

    eprintln!(
        "[measure n={n}] stored audit_bundle: {} B on disk (pg_column_size), {} B as JSON text \
         ({:.1} B/share); audit_body_byte_len {} B (full canonical artifact); \
         top-level shares {}, reward_manifest.shares {}",
        row.stored_bytes,
        row.text_len,
        row.text_len as f64 / n as f64,
        canonical_len,
        row.top_shares,
        row.manifest_shares,
    );
    eprintln!(
        "[measure n={n}] land_candidate wall clock {:.3} s; settlement lock held {} \
         (longest of {} interval(s), {} ms poll, lower bound)",
        landing.as_secs_f64(),
        hold.map_or("unmeasured".to_owned(), |hold| format!(
            "{:.3} s",
            hold.as_secs_f64()
        )),
        intervals.len(),
        LOCK_POLL.as_millis(),
    );
    eprintln!(
        "[measure n={n}] landing path RSS: {} resident before land_candidate, {} peak during it \
         (test process, VmHWM reset before the call)",
        cell(rss_before),
        cell(rss_peak),
    );
    eprintln!(
        "[measure n={n}] canonical read-back {:.3} s, sha256 matches",
        read_back.as_secs_f64(),
    );
    Ok(())
}

/// Stored body and settlement-lock hold for one landed block at a configurable
/// window size. Prints its numbers; run with `--nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement harness; loads PRISM_AUDIT_BODY_MEASURE_SHARES shares (default 20,000) and prints the numbers"]
async fn measure_landing_body_and_settlement_lock_hold() -> Result<()> {
    let db = Database::open_required().await?;
    let plan = WindowPlan::new(measurement_shares()?)?;
    let ledger = db.ledger("audit-body-measure").await?;
    let outcome = measure(&db, &ledger, &plan).await;
    let closed = db.close(vec![ledger]).await;
    outcome?;
    closed
}
