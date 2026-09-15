//! The candidate switch to the window reference (#265, slice 3): what the
//! enqueue writes, what the claim refuses, and what landing and the terminal
//! update do with the reference, against a real PostgreSQL.
//!
//! Run through test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test candidate_window_switch.
use anyhow::{anyhow, ensure, Context, Result};
use futures_util::future::LocalBoxFuture;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, verify_audit_bundle_with_ledger_public_key, AcceptedShare, AuditBundle,
    CarryForwardBalance, FoundBlock, PayoutPolicy,
};
use qbit_prism_server::ledger::{
    authenticate_landed_audit, BalanceSource, Candidate, CandidateClaim, Ledger, ShareRange,
    SignerKeys, Snapshot, WindowError, WindowRef,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{
    io::Write,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

/// The ledger's advisory locks are cluster-wide constants, not schema-scoped,
/// and one test here holds `ORDER_LOCK` deliberately, so the tests of this
/// binary run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// 2023-11-14T22:13:20Z: every seeded share sits before any snapshot anchor.
const ANCHOR: i64 = 1_700_000_000_000;
const ORDER_LOCK: i64 = 0x505249534d000002;
const SETTLEMENT_LOCK: i64 = 0x505249534d000003;
const WINDOW_COLUMNS: [&str; 6] = [
    "window_anchor_ms",
    "window_prior_balances_sha256",
    "window_first_share_seq",
    "window_last_share_seq",
    "window_share_count",
    "window_snapshot_sha256",
];

struct Database {
    admin: PgPool,
    pool: PgPool,
    url: String,
    schema: String,
    ledgers: Mutex<Vec<PgPool>>,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_candidate_switch_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let pool = PgPool::connect(url.as_str()).await?;
        Ok(Some(Self {
            admin,
            pool,
            url: url.to_string(),
            schema,
            ledgers: Mutex::new(Vec::new()),
        }))
    }

    async fn ledger(&self, id: &str) -> Result<Ledger> {
        let ledger = Ledger::connect(&self.url, id.to_owned(), 4, true).await?;
        self.ledgers.lock().unwrap().push(ledger.pool.clone());
        Ok(ledger)
    }

    async fn close(self) -> Result<()> {
        for pool in self.ledgers.into_inner().unwrap() {
            pool.close().await;
        }
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

async fn run(
    body: impl for<'a> FnOnce(&'a Database) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let _serial = SERIAL.lock().await;
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

fn appended_share(id: u64) -> AcceptedShare {
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

/// Write `first..=last` straight into the ledger, before any anchor, so a
/// window of thousands of rows costs one statement.
async fn seed_shares(pool: &PgPool, first: i64, last: i64) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,accepted,writer_id,writer_epoch)
        SELECT i,'share-'||i,'miner-'||(i%3),'order-'||(i%3),decode(repeat('11',32),'hex'),1,1000,100,'job',
            to_timestamp(($3-1)::double precision/1000),100,to_timestamp($3::double precision/1000),
            NULL,true,'candidate-switch-test',0
        FROM generate_series($1::bigint,$2::bigint) AS g(i)")
        .bind(first).bind(last).bind(ANCHOR).execute(pool).await?;
    Ok(())
}

/// Two recipients with carried balances, so `qbit_current_carry_forward_balances()`
/// is not empty and a balance snapshot has something to hold.
async fn seed_carry(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES(repeat('aa',32),100,repeat('00',32),repeat('ab',32),repeat('ac',32),'confirmed'); INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES(100,repeat('aa',32),'miner-b','b',decode(repeat('22',32),'hex'),500,0,500,0,500,'accrued'),(100,repeat('aa',32),'miner-a','a',decode(repeat('11',32),'hex'),1000,0,1000,0,1000,'accrued');")
        .execute(pool).await?;
    Ok(())
}

/// A slim candidate for `snapshot`'s window beside the bundle it was found on.
struct Found {
    candidate: Candidate,
    bundle: AuditBundle,
}

impl Found {
    fn claim(&self, claim: CandidateClaim) -> CandidateClaim {
        claim.with_bundle(self.bundle.clone())
    }
}

fn found(snapshot: &Snapshot, nonce: u32) -> Result<Found> {
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
        as_issued_balances: Vec::new(),
    };
    Ok(Found { candidate, bundle })
}

/// The outbox row as a JSON object, for column assertions.
async fn outbox_row(pool: &PgPool, block_hash: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(o) - 'block_bytes' || jsonb_build_object('block_bytes_hex',encode(block_bytes,'hex'),'candidate_text_len',octet_length(candidate::text),'candidate_has_array',jsonb_path_exists(candidate,'$.** ? (@.type() == \"array\")'),'candidate_has_share_rows',position('\"share_id\"' in candidate::text)>0) FROM qbit_block_candidate_outbox o WHERE block_hash=$1",
    )
    .bind(block_hash)
    .fetch_one(pool)
    .await?)
}

/// Write a row exactly as production would, bypassing `prepare_candidate`'s
/// checks, so a claim can be handed a row the enqueue would have refused.
/// `digest` is what `candidate_sha256` holds; production digests the struct's
/// own serialization, whose key order `serde_json::Value` does not keep.
async fn insert_raw(
    pool: &PgPool,
    candidate: &Candidate,
    document: &Value,
    digest: &str,
) -> Result<()> {
    let range = candidate.window.shares;
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,window_last_share_seq,window_share_count,window_snapshot_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind(&candidate.block_hash).bind(document).bind(digest).bind(&candidate.block_bytes)
        .bind(candidate.window.anchor_ms).bind(hex::encode(candidate.window.prior_balances_digest))
        .bind(range.map(|r| r.first_share_seq as i64)).bind(range.map(|r| r.last_share_seq as i64))
        .bind(range.map(|r| r.share_count as i64)).bind(range.map(|r| hex::encode(r.snapshot_sha256)))
        .execute(pool).await?;
    Ok(())
}

/// `candidate_sha256` as production computes it.
fn digest_of(candidate: &Candidate) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(candidate)?)))
}

async fn delete_row(pool: &PgPool, block_hash: &str) -> Result<()> {
    sqlx::query("DELETE FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(block_hash)
        .execute(pool)
        .await?;
    Ok(())
}

/// The error a claim of the one pending row produces, or a failure if it
/// decoded.
async fn claim_error(ledger: &Ledger) -> Result<String> {
    match ledger.claim_candidate(60).await {
        Ok(Some(claim)) => Err(anyhow!(
            "the tampered row decoded as candidate {}",
            claim.candidate.block_hash
        )),
        Ok(None) => Err(anyhow!("no pending row was claimable")),
        Err(error) => Ok(format!("{error:#}")),
    }
}

/// The outbox columns a parking may change. Everything else in the row (the
/// document, its digest, the block bytes, the window columns, the lifecycle
/// state and offer record) is evidence it must leave exactly as it was.
const PARKING_COLUMNS: [&str; 7] = [
    "claim_token",
    "claim_instance_id",
    "claim_expires_at",
    "last_error",
    "next_attempt_at",
    "updated_at",
    "attempt_count",
];

/// Claim the one tampered row and check that the claim parked it (#387): the
/// error keeps the diagnosis under the parked outcome, `last_error` names the
/// row's database hash and the validation kind, the claim token, instance and
/// expiry are cleared, `next_attempt_at` is infinity, the evidence is
/// unchanged, and neither frontend's next poll attempts the row again.
async fn claim_parks(
    db: &Database,
    ledger: &Ledger,
    other: &Ledger,
    hash: &str,
    kind: &str,
    expected: &str,
) -> Result<()> {
    let evidence = |mut row: Value| {
        let columns = row.as_object_mut().expect("an outbox row is an object");
        for column in PARKING_COLUMNS {
            columns.remove(column);
        }
        row
    };
    let before = outbox_row(&db.pool, hash).await?;
    let error = claim_error(ledger).await?;
    ensure!(error.contains(expected), "claim error: {error}");
    ensure!(
        error.starts_with(&format!(
            "candidate {hash} failed validation and was parked"
        )),
        "claim error: {error}"
    );
    let after = outbox_row(&db.pool, hash).await?;
    let reason = after["last_error"]
        .as_str()
        .context("the parked row records no reason")?;
    ensure!(
        reason.starts_with(&format!("candidate {hash}: validation {kind}: ")),
        "reason: {reason}"
    );
    ensure!(
        reason.contains(expected) && reason.len() <= 1024,
        "reason: {reason}"
    );
    ensure!(
        after["claim_token"].is_null()
            && after["claim_instance_id"].is_null()
            && after["claim_expires_at"].is_null(),
        "the parked row keeps its claim: {after}"
    );
    ensure!(
        after["next_attempt_at"] == json!("infinity"),
        "the row was not parked: {after}"
    );
    ensure!(after["attempt_count"] == json!(1), "{after}");
    ensure!(
        evidence(after) == evidence(before),
        "parking changed the row's evidence"
    );
    for poller in [ledger, other] {
        ensure!(
            poller.claim_candidate(60).await?.is_none(),
            "a parked row was claimed again"
        );
    }
    ensure!(
        outbox_row(&db.pool, hash).await?["attempt_count"] == json!(1),
        "a parked row was attempted again"
    );
    Ok(())
}

/// Captured `tracing` output for one test body, on the runtime thread.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Logs {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

// ---------------------------------------------------------------------------
// Enqueue
// ---------------------------------------------------------------------------

/// The row is O(1) in the window: a two-page window enqueues a document with
/// no share array anywhere and well under 1 MB, the six typed columns equal
/// the document's reference, and the block is stored as bytes.
#[tokio::test]
async fn enqueue_stores_a_reference_row_beside_the_block_bytes() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("enqueue").await?;
            // 9,000 rows of difficulty 1; a network difficulty of 1,000 selects
            // the newest 8,000, which is two keyset pages.
            seed_shares(&db.pool, 1, 9_000).await?;
            let snapshot = ledger.snapshot(1_000).await?;
            ensure!(snapshot.shares.len() == 8_000, "fixture window is wrong");
            let block = found(&snapshot, 1)?;
            let range = block.candidate.window.shares.context("range")?;
            ensure!(range.first_share_seq == 1_001 && range.last_share_seq == 9_000);
            ensure!(
                ledger
                    .enqueue_candidate_once(block.candidate.clone())
                    .await?
            );
            let row = outbox_row(&db.pool, &block.candidate.block_hash).await?;
            // The reference's `shares` member is the range, an object of four
            // scalars; no array of any kind and no share row is in the document.
            ensure!(
                row["candidate_has_array"] == false,
                "the document carries an array"
            );
            ensure!(
                row["candidate_has_share_rows"] == false,
                "the document carries a share row"
            );
            ensure!(row["candidate"]["window"]["shares"]["share_count"] == json!(8_000));
            ensure!(
                row["candidate_text_len"].as_i64().unwrap() < 1 << 20,
                "the document is {} bytes",
                row["candidate_text_len"]
            );
            ensure!(row["candidate"]["window"] == serde_json::to_value(block.candidate.window)?);
            ensure!(row["window_anchor_ms"] == json!(block.candidate.window.anchor_ms));
            ensure!(
                row["window_prior_balances_sha256"]
                    == json!(hex::encode(block.candidate.window.prior_balances_digest))
            );
            ensure!(row["window_first_share_seq"] == json!(range.first_share_seq));
            ensure!(row["window_last_share_seq"] == json!(range.last_share_seq));
            ensure!(row["window_share_count"] == json!(range.share_count));
            ensure!(row["window_snapshot_sha256"] == json!(hex::encode(range.snapshot_sha256)));
            ensure!(row["block_bytes_hex"] == json!(hex::encode(&block.candidate.block_bytes)));
            ensure!(row["candidate"]["block_sha256"] == json!(block.candidate.block_sha256));
            ensure!(row["candidate"]["leased"] == false);
            ensure!(row["candidate"].get("bundle").is_none());
            ensure!(row["candidate"].get("block_hex").is_none());
            let digest = hex::encode(Sha256::digest(serde_json::to_vec(&block.candidate)?));
            ensure!(row["candidate_sha256"] == json!(digest));
            // The claim decodes it back, block included, and the reference
            // still reads the two pages it names.
            let claim = ledger
                .claim_candidate(60)
                .await?
                .context("the row was not claimable")?;
            ensure!(claim.candidate.block_bytes == block.candidate.block_bytes);
            ensure!(claim.candidate.window == block.candidate.window);
            ensure!(
                claim.parts.is_none(),
                "a claim carries no parts before its rebuild"
            );
            let window = ledger
                .read_window(&claim.candidate.window, BalanceSource::Current)
                .await?;
            ensure!(window.shares == snapshot.shares);
            Ok(())
        })
    })
    .await
}

/// The writer fence: a frontend that pinned a fingerprint refuses to enqueue,
/// in the same transaction and before any insert, while the stored one is
/// reset or different. A share-path enqueue rolls the share back with it.
#[tokio::test]
async fn enqueue_refuses_a_fingerprint_the_writer_did_not_pin() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("fence").await?;
            ledger.configure("fingerprint-one", &signer_keys()).await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            let block = found(&snapshot, 2)?;
            for stored in [None, Some("fingerprint-two")] {
                sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=$1 WHERE singleton")
                    .bind(stored)
                    .execute(&db.pool)
                    .await?;
                let error = ledger
                    .enqueue_candidate_once(block.candidate.clone())
                    .await
                    .err()
                    .context("a reset fingerprint did not refuse the enqueue")?;
                ensure!(
                    format!("{error:#}").contains("fingerprint"),
                    "unexpected refusal: {error:#}"
                );
                let error = ledger
                    .append(appended_share(2), Some(block.candidate.clone()))
                    .await
                    .err()
                    .context("a reset fingerprint did not refuse the share-path enqueue")?;
                ensure!(format!("{error:#}").contains("fingerprint"));
                let (rows, shares): (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM qbit_block_candidate_outbox),(SELECT count(*) FROM qbit_share_ledger)")
                    .fetch_one(&db.pool).await?;
                ensure!(
                    (rows, shares) == (0, 1),
                    "a refused enqueue left rows behind: {rows} candidates, {shares} shares"
                );
            }
            sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint='fingerprint-one' WHERE singleton")
                .execute(&db.pool)
                .await?;
            ledger
                .append(appended_share(2), Some(block.candidate.clone()))
                .await?;
            // A frontend that never pinned one (tooling, tests) still takes the
            // `FOR SHARE` read and writes.
            let bare = db.ledger("bare").await?;
            sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint='fingerprint-three' WHERE singleton")
                .execute(&db.pool)
                .await?;
            ensure!(bare.enqueue_candidate_once(found(&snapshot, 3)?.candidate).await?);
            Ok(())
        })
    })
    .await
}

/// A window whose prefix row is gone still publishes: the block must reach the
/// node. The enqueue raises an alert instead, and the claim's read then meets
/// `Incomplete`.
#[tokio::test]
async fn enqueue_alerts_but_publishes_when_the_window_prefix_row_is_missing() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("probe").await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            let mut block = found(&snapshot, 4)?;
            // Share rows are immutable, so a missing prefix can only be a
            // reference to rows that were never there.
            block.candidate.window.shares = Some(ShareRange {
                first_share_seq: 5_000_000,
                last_share_seq: 5_000_010,
                share_count: 11,
                snapshot_sha256: [7; 32],
            });
            let logs = Logs::default();
            let subscriber = tracing_subscriber::fmt()
                .with_writer({
                    let logs = logs.clone();
                    move || logs.clone()
                })
                .with_ansi(false)
                .finish();
            let enqueued = {
                let _guard = tracing::subscriber::set_default(subscriber);
                ledger
                    .enqueue_candidate_once(block.candidate.clone())
                    .await?
            };
            ensure!(enqueued, "the candidate was not published");
            let text = logs.text();
            ensure!(
                text.contains("ALERT") && text.contains("prefix row is missing"),
                "no alert was raised: {text}"
            );
            let claim = ledger
                .claim_candidate(60)
                .await?
                .context("the published row was not claimable")?;
            let error = ledger
                .read_window(&claim.candidate.window, BalanceSource::Current)
                .await
                .err()
                .context("the missing range read as a window")?;
            ensure!(matches!(error, WindowError::Incomplete { .. }), "{error:?}");
            Ok(())
        })
    })
    .await
}

/// A `leased` enqueue re-establishes the balance snapshot its reference names,
/// digest-checked, so the leased claim can read `AsIssued` after a prune; a
/// candidate that is not leased writes none, and a leased one whose balances
/// do not hash to its reference is refused before the lock.
#[tokio::test]
async fn leased_enqueue_reinserts_the_as_issued_balance_snapshot() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("leased").await?;
            seed_carry(&db.pool).await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            ensure!(snapshot.prior_balances.len() == 2, "fixture carries no balances");
            let digest = hex::encode(snapshot_digest(&snapshot));
            let count = |pool: &PgPool, digest: &str| {
                let pool = pool.clone();
                let digest = digest.to_owned();
                async move {
                    sqlx::query_scalar::<_, i64>(
                        "SELECT count(*) FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
                    )
                    .bind(digest)
                    .fetch_one(&pool)
                    .await
                }
            };
            let plain = found(&snapshot, 5)?;
            ledger.enqueue_candidate(plain.candidate).await?;
            ensure!(count(&db.pool, &digest).await? == 0, "an unleased enqueue wrote a snapshot");

            let mut wrong = found(&snapshot, 6)?;
            wrong.candidate.leased = true;
            wrong.candidate.as_issued_balances = vec![snapshot.prior_balances[0].clone()];
            let error = ledger
                .enqueue_candidate(wrong.candidate)
                .await
                .err()
                .context("mismatched as-issued balances were accepted")?;
            ensure!(format!("{error:#}").contains("do not hash"), "{error:#}");
            ensure!(count(&db.pool, &digest).await? == 0);

            let mut leased = found(&snapshot, 7)?;
            leased.candidate.leased = true;
            // Any order of the same set: the writer sorts.
            let mut reversed = snapshot.prior_balances.clone();
            reversed.reverse();
            leased.candidate.as_issued_balances = reversed;
            ledger.enqueue_candidate(leased.candidate.clone()).await?;
            ensure!(count(&db.pool, &digest).await? == 1, "the leased enqueue wrote no snapshot");
            let stored: Vec<u8> = sqlx::query_scalar(
                "SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
            )
            .bind(&digest)
            .fetch_one(&db.pool)
            .await?;
            let mut canonical = snapshot.prior_balances.clone();
            canonical.sort_by(|a, b| {
                a.order_key
                    .cmp(&b.order_key)
                    .then_with(|| a.recipient_id.cmp(&b.recipient_id))
                    .then_with(|| a.p2mr_program_hex.cmp(&b.p2mr_program_hex))
            });
            ensure!(stored == serde_json::to_vec(&canonical)?, "the stored set is not canonical");
            // A prune between submission and enqueue costs nothing: the next
            // leased enqueue writes it back, and the claim reads it as issued.
            sqlx::query("DELETE FROM qbit_prism_balance_snapshots")
                .execute(&db.pool)
                .await?;
            let mut again = found(&snapshot, 8)?;
            again.candidate.leased = true;
            again.candidate.as_issued_balances = snapshot.prior_balances.clone();
            ledger.enqueue_candidate(again.candidate.clone()).await?;
            ensure!(count(&db.pool, &digest).await? == 1);
            let window = ledger
                .read_window(&again.candidate.window, BalanceSource::AsIssued)
                .await?;
            ensure!(window.prior_balances == canonical);
            let row = outbox_row(&db.pool, &again.candidate.block_hash).await?;
            ensure!(row["candidate"]["leased"] == true, "the leased flag was not stored");
            Ok(())
        })
    })
    .await
}

fn snapshot_digest(snapshot: &Snapshot) -> [u8; 32] {
    qbit_prism::prior_balances_digest(&snapshot.prior_balances)
}

/// Serialization, digest and validation happen before the append transaction
/// opens: a candidate the preparation refuses is rejected while another
/// session holds `ORDER_LOCK`, without ever waiting for the lock, and the
/// share it came with is never written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn candidate_preparation_never_waits_for_the_order_lock() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("prepare").await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            let mut holder = db.pool.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(ORDER_LOCK)
                .execute(&mut *holder)
                .await?;
            let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *holder)
                .await?;
            let mut header_mismatch = found(&snapshot, 9)?.candidate;
            header_mismatch.block_hash = "00".repeat(32);
            let mut digest_mismatch = found(&snapshot, 10)?.candidate;
            digest_mismatch.block_sha256 = "11".repeat(32);
            let mut anchor_mismatch = found(&snapshot, 11)?.candidate;
            anchor_mismatch.found_block.anchor_job_issued_at_ms += 1;
            let mut bootstrap_mismatch = found(&snapshot, 12)?.candidate;
            bootstrap_mismatch.bootstrap_share = Some(appended_share(99));
            let mut empty_suffix = found(&snapshot, 13)?.candidate;
            empty_suffix.coinbase_suffix_hex.clear();
            for (name, candidate) in [
                ("header hash", header_mismatch),
                ("block digest", digest_mismatch),
                ("anchor", anchor_mismatch),
                ("bootstrap share", bootstrap_mismatch),
                ("coinbase suffix", empty_suffix),
            ] {
                let outcome = tokio::time::timeout(
                    Duration::from_secs(2),
                    ledger.append(appended_share(2), Some(candidate.clone())),
                )
                .await
                .map_err(|_| {
                    anyhow!("the {name} candidate waited for ORDER_LOCK: it was prepared inside the transaction")
                })?;
                ensure!(outcome.is_err(), "the {name} candidate was accepted");
                let outcome = tokio::time::timeout(
                    Duration::from_secs(2),
                    ledger.enqueue_candidate_once(candidate),
                )
                .await
                .map_err(|_| anyhow!("the {name} candidate waited for ORDER_LOCK on enqueue"))?;
                ensure!(outcome.is_err(), "the {name} candidate was enqueued");
            }
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))",
            )
            .bind(holder_pid)
            .fetch_one(&db.admin)
            .await?;
            ensure!(waiting == 0, "{waiting} sessions queued behind ORDER_LOCK");
            let (rows, shares): (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM qbit_block_candidate_outbox),(SELECT count(*) FROM qbit_share_ledger)")
                .fetch_one(&db.pool).await?;
            ensure!((rows, shares) == (0, 1), "a refused candidate wrote {rows} rows and {shares} shares");
            holder.rollback().await?;
            ledger
                .append(appended_share(2), Some(found(&snapshot, 14)?.candidate))
                .await?;
            Ok(())
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// Claim decode
// ---------------------------------------------------------------------------

/// Every disagreement between a pending row and its document is refused at
/// claim: the typed columns, the document digest, the block bytes, the header,
/// a NULL anchor (a pre-007 row), a missing suffix, and an inline pre-007
/// document. After 007 there is no compatibility decode. Each refusal parks
/// the row (#387) with a durable reason naming it and the validation kind,
/// leaves its evidence untouched, and is not attempted again by a later poll.
#[tokio::test]
async fn claim_refuses_every_row_that_disagrees_with_its_document() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("decode").await?;
            let other = db.ledger("decode-other").await?;
            for id in 1..=3 {
                ledger.append(appended_share(id), None).await?;
            }
            let snapshot = ledger.snapshot(100).await?;
            let block = found(&snapshot, 20)?;
            let candidate = &block.candidate;
            let hash = candidate.block_hash.clone();
            let document = serde_json::to_value(candidate)?;

            // Column tampering on a row the enqueue wrote.
            let tampered: Vec<(&str, &str, &str, &str)> = vec![
                (
                    "anchor column",
                    "UPDATE qbit_block_candidate_outbox SET window_anchor_ms=window_anchor_ms+1 WHERE block_hash=$1",
                    "window_columns",
                    "anchor column disagrees",
                ),
                (
                    "balances digest column",
                    "UPDATE qbit_block_candidate_outbox SET window_prior_balances_sha256=repeat('0',64) WHERE block_hash=$1",
                    "window_columns",
                    "balances digest column disagrees",
                ),
                (
                    "range column",
                    "UPDATE qbit_block_candidate_outbox SET window_last_share_seq=window_last_share_seq+1 WHERE block_hash=$1",
                    "window_columns",
                    "range columns disagree",
                ),
                (
                    "range columns on an empty-window document",
                    "UPDATE qbit_block_candidate_outbox SET window_first_share_seq=NULL,window_last_share_seq=NULL,window_share_count=NULL,window_snapshot_sha256=NULL WHERE block_hash=$1",
                    "window_columns",
                    "range columns disagree",
                ),
                (
                    "document digest",
                    "UPDATE qbit_block_candidate_outbox SET candidate_sha256=repeat('0',64) WHERE block_hash=$1",
                    "document_digest",
                    "digest mismatch",
                ),
                (
                    "block bytes",
                    "UPDATE qbit_block_candidate_outbox SET block_bytes=block_bytes||'\\x00'::bytea WHERE block_hash=$1",
                    "block",
                    "do not hash to the document's block_sha256",
                ),
                (
                    "missing block bytes",
                    "UPDATE qbit_block_candidate_outbox SET block_bytes=NULL WHERE block_hash=$1",
                    "block",
                    "carries no block bytes",
                ),
                (
                    "NULL anchor on a pending row",
                    "UPDATE qbit_block_candidate_outbox SET window_anchor_ms=NULL,window_prior_balances_sha256=NULL,window_first_share_seq=NULL,window_last_share_seq=NULL,window_share_count=NULL,window_snapshot_sha256=NULL WHERE block_hash=$1",
                    "window_reference",
                    "pre-007 frontend",
                ),
            ];
            for (name, statement, kind, expected) in tampered {
                ledger.enqueue_candidate(candidate.clone()).await?;
                sqlx::query(statement).bind(&hash).execute(&db.pool).await?;
                claim_parks(db, &ledger, &other, &hash, kind, expected)
                    .await
                    .with_context(|| name.to_owned())?;
                delete_row(&db.pool, &hash).await?;
            }

            // Rows the enqueue would have refused, written directly.
            let mut header_forged = candidate.clone();
            header_forged.block_bytes[76] ^= 1;
            header_forged.block_sha256 = Candidate::block_digest_hex(&header_forged.block_bytes);
            insert_raw(
                &db.pool,
                &header_forged,
                &serde_json::to_value(&header_forged)?,
                &digest_of(&header_forged)?,
            )
            .await?;
            claim_parks(db, &ledger, &other, &hash, "block", "header does not hash to block_hash")
                .await
                .context("forged header")?;
            delete_row(&db.pool, &hash).await?;

            let mut truncated = candidate.clone();
            truncated.block_bytes.truncate(80);
            truncated.block_sha256 = Candidate::block_digest_hex(&truncated.block_bytes);
            insert_raw(
                &db.pool,
                &truncated,
                &serde_json::to_value(&truncated)?,
                &digest_of(&truncated)?,
            )
            .await?;
            claim_parks(db, &ledger, &other, &hash, "block", "candidate block is truncated")
                .await
                .context("truncated block")?;
            delete_row(&db.pool, &hash).await?;

            let mut without_suffix = document.clone();
            without_suffix.as_object_mut().unwrap().remove("coinbase_suffix_hex");
            insert_raw(&db.pool, candidate, &without_suffix, &digest_of(candidate)?).await?;
            claim_parks(db, &ledger, &other, &hash, "document", "invalid persisted candidate")
                .await
                .context("missing suffix")?;
            delete_row(&db.pool, &hash).await?;

            let mut empty_suffix = candidate.clone();
            empty_suffix.coinbase_suffix_hex = String::new();
            insert_raw(
                &db.pool,
                &empty_suffix,
                &serde_json::to_value(&empty_suffix)?,
                &digest_of(&empty_suffix)?,
            )
            .await?;
            claim_parks(db, &ledger, &other, &hash, "coinbase_suffix", "suffix must be non-empty hex")
                .await
                .context("empty suffix")?;
            delete_row(&db.pool, &hash).await?;

            // The document, digested as production would, names another block
            // than the row it is stored in.
            let mut renamed = candidate.clone();
            renamed.block_hash = "ff".repeat(32);
            insert_raw(
                &db.pool,
                candidate,
                &serde_json::to_value(&renamed)?,
                &digest_of(&renamed)?,
            )
            .await?;
            claim_parks(db, &ledger, &other, &hash, "document_identity", &format!("names block {} in row {hash}", "ff".repeat(32)))
                .await
                .context("renamed document")?;
            delete_row(&db.pool, &hash).await?;

            // A document range the bigint columns cannot hold, beside valid columns.
            let mut unrepresentable = candidate.clone();
            unrepresentable
                .window
                .shares
                .as_mut()
                .context("the candidate references a range")?
                .last_share_seq = u64::MAX;
            insert_raw(
                &db.pool,
                candidate,
                &serde_json::to_value(&unrepresentable)?,
                &digest_of(&unrepresentable)?,
            )
            .await?;
            claim_parks(db, &ledger, &other, &hash, "window_columns", "not representable in the range columns")
                .await
                .context("unrepresentable range")?;
            delete_row(&db.pool, &hash).await?;

            let mut inconsistent = candidate.clone();
            inconsistent.found_block.anchor_job_issued_at_ms += 1;
            insert_raw(
                &db.pool,
                &inconsistent,
                &serde_json::to_value(&inconsistent)?,
                &digest_of(&inconsistent)?,
            )
            .await?;
            claim_parks(db, &ledger, &other, &hash, "reference_invariants", "anchor disagrees with its window reference")
                .await
                .context("anchor invariant")?;
            delete_row(&db.pool, &hash).await?;

            // A legacy inline candidate on the post-007 schema, with columns a
            // tampered migration could have filled in: refused, never decoded.
            let legacy = json!({
                "block_hash": hash,
                "block_hex": hex::encode(&candidate.block_bytes),
                "job_id": "job",
                "payout_revision": candidate.payout_revision,
                "bundle": serde_json::to_value(&block.bundle)?,
                "coinbase_suffix_hex": "00".repeat(12),
            });
            insert_raw(&db.pool, candidate, &legacy, &digest_of(candidate)?).await?;
            claim_parks(db, &ledger, &other, &hash, "document", "inline pre-007 document")
                .await
                .context("inline document")?;
            delete_row(&db.pool, &hash).await?;

            // The untouched row decodes.
            ledger.enqueue_candidate(candidate.clone()).await?;
            let claim = ledger
                .claim_candidate(60)
                .await
                .context("the untouched row did not decode")?
                .context("the untouched row was not claimable")?;
            ensure!(claim.candidate.block_hash == hash);
            ensure!(claim.candidate.coinbase_suffix_hex == "00".repeat(12));
            Ok(())
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// Landing and the terminal row
// ---------------------------------------------------------------------------

/// Landing takes the claim's parts and refuses parts that are not the window
/// the candidate references; a claim without parts cannot land at all.
#[tokio::test]
async fn landing_refuses_parts_that_are_not_the_referenced_window() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("landing").await?;
            for id in 1..=3 {
                ledger.append(appended_share(id), None).await?;
            }
            let snapshot = ledger.snapshot(100).await?;
            let block = found(&snapshot, 30)?;
            ledger.enqueue_candidate(block.candidate.clone()).await?;
            let claim = ledger
                .claim_candidate(60)
                .await?
                .context("the row was not claimable")?;
            let error = ledger
                .land_candidate(&claim, &keys().1.public_key_hex())
                .await
                .err()
                .context("a claim without parts landed")?;
            ensure!(format!("{error:#}").contains("no rebuilt audit parts"), "{error:#}");
            // A bundle over a narrower window than the reference names.
            let narrower = Snapshot {
                shares: snapshot.shares[1..].to_vec(),
                ..snapshot.clone()
            };
            let other = found(&narrower, 31)?;
            let error = ledger
                .land_candidate(
                    &claim.clone().with_bundle(other.bundle),
                    &keys().1.public_key_hex(),
                )
                .await
                .err()
                .context("parts over a different window landed")?;
            ensure!(
                format!("{error:#}").contains("differs from the candidate's window reference"),
                "{error:#}"
            );
            let landed: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_pool_blocks")
                .fetch_one(&db.pool)
                .await?;
            ensure!(landed == 0, "a refused landing wrote a block row");
            ledger
                .land_candidate(&block.claim(claim), &keys().1.public_key_hex())
                .await?;
            let stored: (Option<Value>, String) = sqlx::query_as("SELECT audit_bundle,share_snapshot_sha256 FROM qbit_pool_audit_bundles WHERE block_hash=$1")
                .bind(&block.candidate.block_hash).fetch_one(&db.pool).await?;
            let range = block.candidate.window.shares.context("range")?;
            ensure!(stored.1 == hex::encode(range.snapshot_sha256));
            let body = stored.0.context("no body")?;
            ensure!(body.get("shares").is_none(), "the stored body carries the window");
            ensure!(
                ledger.audit_bundle(&block.candidate.block_hash).await?
                    == Some(serde_json::to_value(&block.bundle)?),
                "the hydrated audit is not the bundle"
            );
            Ok(())
        })
    })
    .await
}

/// The terminal UPDATE NULLs the six window columns and the block bytes with
/// the document, for a submitted and for an abandoned row.
#[tokio::test]
async fn terminal_update_nulls_the_window_columns_and_block_bytes() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("terminal").await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            for (nonce, submitted) in [(40, true), (41, false)] {
                let block = found(&snapshot, nonce)?;
                ledger.enqueue_candidate(block.candidate.clone()).await?;
                let claim = block.claim(
                    ledger
                        .claim_candidate(60)
                        .await?
                        .context("the row was not claimable")?,
                );
                if submitted {
                    ledger
                        .land_candidate(&claim, &keys().1.public_key_hex())
                        .await?;
                }
                let row = outbox_row(&db.pool, &block.candidate.block_hash).await?;
                ensure!(row["window_anchor_ms"].is_i64() && row["block_bytes_hex"].is_string());
                ledger
                    .finish_candidate(&claim, submitted, (!submitted).then_some("rejected"))
                    .await?;
                let row = outbox_row(&db.pool, &block.candidate.block_hash).await?;
                ensure!(row["state"] == json!(if submitted { "submitted" } else { "abandoned" }));
                ensure!(row["candidate"].is_null(), "the document survived the terminal update");
                ensure!(
                    row["block_bytes_hex"].is_null(),
                    "the block survived the terminal update: {}",
                    row["block_bytes_hex"]
                );
                for column in WINDOW_COLUMNS {
                    ensure!(
                        row[column].is_null(),
                        "{column} survived the terminal update: {}",
                        row[column]
                    );
                }
                // The retention predicate sees exactly the live rows.
                let live: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM qbit_block_candidate_outbox WHERE window_anchor_ms IS NOT NULL",
                )
                .fetch_one(&db.pool)
                .await?;
                ensure!(live == 0, "{live} terminal rows still hold a window reference");
            }
            Ok(())
        })
    })
    .await
}

/// A recovered claim authenticates the landed audit row against its block:
/// the coinbase must be the block's, the audit root must be the coinbase's
/// witness reserved value, the share snapshot must be the reference's, and the
/// recorded bits must be the header's. Any forgery is refused.
#[tokio::test]
async fn landed_audit_is_authenticated_against_the_block() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("landed").await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            let block = found(&snapshot, 50)?;
            let hash = block.candidate.block_hash.clone();
            ensure!(ledger.landed_audit(&hash).await?.is_none());
            ledger.enqueue_candidate(block.candidate.clone()).await?;
            let claim = block.claim(
                ledger
                    .claim_candidate(60)
                    .await?
                    .context("the row was not claimable")?,
            );
            ledger
                .land_candidate(&claim, &keys().1.public_key_hex())
                .await?;
            let landed = ledger
                .landed_audit(&hash)
                .await?
                .context("the landed audit was not read back")?;
            authenticate_landed_audit(&block.candidate, &landed)?;

            let mut forged_coinbase = landed.clone();
            let mut coinbase = hex::decode(&forged_coinbase.coinbase_tx_hex)?;
            let last = coinbase.len() - 1;
            coinbase[last] ^= 1;
            forged_coinbase.coinbase_tx_hex = hex::encode(coinbase);
            let error = authenticate_landed_audit(&block.candidate, &forged_coinbase)
                .err()
                .context("a forged coinbase authenticated")?;
            ensure!(
                format!("{error:#}").contains("coinbase differs"),
                "{error:#}"
            );

            let mut forged_root = landed.clone();
            forged_root.audit_commitment_leaves_hex = vec!["ab".repeat(32)];
            let error = authenticate_landed_audit(&block.candidate, &forged_root)
                .err()
                .context("a forged audit root authenticated")?;
            ensure!(
                format!("{error:#}").contains("witness reserved value"),
                "{error:#}"
            );

            let mut forged_snapshot = landed.clone();
            forged_snapshot.share_snapshot_sha256 = Some("cd".repeat(32));
            let error = authenticate_landed_audit(&block.candidate, &forged_snapshot)
                .err()
                .context("a forged share snapshot authenticated")?;
            ensure!(format!("{error:#}").contains("share snapshot"), "{error:#}");

            let mut forged_bits = landed.clone();
            forged_bits.found_block_bits = Some("1d00ffff".into());
            let error = authenticate_landed_audit(&block.candidate, &forged_bits)
                .err()
                .context("forged bits authenticated")?;
            ensure!(format!("{error:#}").contains("bits"), "{error:#}");

            // Bits that predate the column are accepted and can be recorded.
            let mut unrecorded = landed.clone();
            unrecorded.found_block_bits = None;
            authenticate_landed_audit(&block.candidate, &unrecorded)?;
            Ok(())
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// Configure
// ---------------------------------------------------------------------------

/// Pinning a fingerprint onto a reset one is a rotation: it is refused while a
/// pending candidate stores other signer keys, and allowed once that row is
/// terminal or was signed with the local pair.
#[tokio::test]
async fn configure_refuses_a_rotation_over_pending_foreign_signed_candidates() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("rotation").await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            let block = found(&snapshot, 60)?;
            ledger.enqueue_candidate(block.candidate.clone()).await?;
            let error = ledger
                .configure("fingerprint-new", &other_signer_keys())
                .await
                .err()
                .context("a rotation over a foreign-signed pending candidate was accepted")?;
            ensure!(
                format!("{error:#}").contains(&block.candidate.block_hash),
                "the refusal does not name the row: {error:#}"
            );
            let stored: Option<String> = sqlx::query_scalar(
                "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton",
            )
            .fetch_one(&db.pool)
            .await?;
            ensure!(stored.is_none(), "a refused configure pinned {stored:?}");
            // The local pair signed it: not a rotation.
            let same = db.ledger("same-keys").await?;
            same.configure("fingerprint-new", &signer_keys()).await?;
            sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=NULL WHERE singleton")
                .execute(&db.pool)
                .await?;
            // A terminal row no longer holds any keys.
            let claim = block.claim(
                ledger
                    .claim_candidate(60)
                    .await?
                    .context("the row was not claimable")?,
            );
            ledger
                .finish_candidate(&claim, false, Some("rotation test"))
                .await?;
            let rotated = db.ledger("rotated").await?;
            rotated
                .configure("fingerprint-rotated", &other_signer_keys())
                .await?;
            ensure!(rotated.config_fingerprint() == Some("fingerprint-rotated"));
            Ok(())
        })
    })
    .await
}

/// The full-equality durable-range proof runs before the settlement lock is
/// taken, and only a row count runs under it.
///
/// #267 moved that proof out of the settlement transaction so the lock is never
/// held for a window-sized read; this slice made the read paged so it never
/// occupies a runtime thread either. Reconciling the two is the step that can
/// quietly undo the first: keep this slice's version of `persist_audit_snapshot`
/// through a rebase and the full read goes back under the lock. The result
/// stays correct, every other test still passes, and only the lock hold grows,
/// so nothing else in the suite can tell.
///
/// This pins the placement without measuring anything. A second connection
/// holds `SETTLEMENT_LOCK`, so the landing blocks on it after its proof has
/// run. While it waits, one share's `ntime` is altered: full equality notices
/// that, a `count(*)` over the same predicate cannot. The landing must then
/// succeed, which it can only do if it read and compared the shares *before*
/// taking the lock. Move the full read back under the lock and this fails with
/// "audit share snapshot differs from canonical database history".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_durable_range_proof_runs_before_the_settlement_lock() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("proof-before-lock").await?;
            seed_carry(&ledger.pool).await?;
            for id in 1..=6 {
                ledger.append(appended_share(id), None).await?;
            }
            let snapshot = ledger.snapshot(100).await?;
            let block = found(&snapshot, 61)?;
            ledger.enqueue_candidate(block.candidate.clone()).await?;
            let claim = block.claim(
                ledger
                    .claim_candidate(60)
                    .await?
                    .context("the row was not claimable")?,
            );

            // Hold the settlement lock on a connection of its own, so the
            // landing reaches it and waits.
            let mut holder = db.pool.acquire().await?;
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(SETTLEMENT_LOCK)
                .execute(&mut *holder)
                .await?;

            let landing = tokio::spawn({
                let ledger = ledger.clone();
                async move {
                    ledger
                        .land_candidate(&claim, &keys().1.public_key_hex())
                        .await
                }
            });

            // Wait until the landing is genuinely blocked on that lock rather
            // than merely slow, so the alteration below lands between its proof
            // and its transaction.
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=$1 AND objid=$2)",
                )
                .bind((SETTLEMENT_LOCK >> 32) as i32)
                .bind((SETTLEMENT_LOCK & 0xffff_ffff) as i32)
                .fetch_one(&db.pool)
                .await?;
                if waiting {
                    break;
                }
                ensure!(
                    std::time::Instant::now() < deadline,
                    "the landing never waited on the settlement lock"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            // `ntime` is not part of the counted share, so this leaves the row
            // count, the reward manifest and every signature alone: only a full
            // field-by-field comparison can see it. Accepted history is
            // immutable in production and a trigger enforces that, which is the
            // property being relied on everywhere else; it is lifted here for
            // one statement, inside this test's own disposable schema, because
            // simulating a ledger that disagrees with the claim is the whole
            // point. It goes straight back on.
            sqlx::query("ALTER TABLE qbit_share_ledger DISABLE TRIGGER qbit_prism_immutable_share_history")
                .execute(&ledger.pool)
                .await?;
            let altered = sqlx::query("UPDATE qbit_share_ledger SET ntime=ntime+1 WHERE share_seq=$1")
                .bind(3i64)
                .execute(&ledger.pool)
                .await?
                .rows_affected();
            sqlx::query("ALTER TABLE qbit_share_ledger ENABLE TRIGGER qbit_prism_immutable_share_history")
                .execute(&ledger.pool)
                .await?;
            ensure!(altered == 1, "the test altered {altered} shares, expected 1");

            sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(SETTLEMENT_LOCK)
                .execute(&mut *holder)
                .await?;
            drop(holder);

            tokio::time::timeout(Duration::from_secs(60), landing)
                .await
                .map_err(|_| anyhow!("the landing did not finish once the lock was released"))??
                .context(
                    "the landing read share payloads under the settlement lock: it saw an \
                     alteration made after its proof had already run",
                )?;
            Ok(())
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// As-issued snapshot preparation
// ---------------------------------------------------------------------------

/// The stored order of a balance set, the digest's own comparator.
fn canonical_sort(set: &mut [CarryForwardBalance]) {
    set.sort_by(|a, b| {
        a.order_key
            .cmp(&b.order_key)
            .then_with(|| a.recipient_id.cmp(&b.recipient_id))
            .then_with(|| a.p2mr_program_hex.cmp(&b.p2mr_program_hex))
    });
}

/// `n` recipients with distinct keys in a scrambled order, so a canonical
/// sort does real work. Deterministic in `salt`, so a set can be rebuilt
/// for comparison instead of being kept.
fn scrambled_as_issued_set(n: usize, salt: u64) -> Vec<CarryForwardBalance> {
    let mut set: Vec<CarryForwardBalance> = (0..n)
        .map(|i| CarryForwardBalance {
            recipient_id: format!("miner-{i:07}"),
            order_key: format!("order-{:07}", (i * 7919) % n),
            p2mr_program_hex: format!("{:064x}", i as u128 + u128::from(salt)),
            balance_sats: i as i128 * 1000 + i128::from(salt),
        })
        .collect();
    // Fisher-Yates over a fixed linear congruential sequence.
    let mut state = salt | 1;
    for i in (1..set.len()).rev() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let j = (state >> 33) as usize % (i + 1);
        set.swap(i, j);
    }
    set
}

/// The longest gap between consecutive 1 ms timer ticks, observed by a task
/// that shares the runtime's thread with the code under test.
struct Ticker {
    max_gap_ns: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl Ticker {
    async fn start() -> Result<Self> {
        let max_gap_ns = Arc::new(AtomicU64::new(0));
        let gaps = max_gap_ns.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut previous = Instant::now();
            ready.send(()).ok();
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let now = Instant::now();
                gaps.fetch_max(
                    now.duration_since(previous).as_nanos() as u64,
                    Ordering::Relaxed,
                );
                previous = now;
            }
        });
        started.await?;
        Ok(Self { max_gap_ns, task })
    }

    /// The longest gap since the previous reading.
    fn take_max_gap(&self) -> Duration {
        Duration::from_nanos(self.max_gap_ns.swap(0, Ordering::Relaxed))
    }

    async fn stop(self) -> Result<()> {
        self.task.abort();
        ensure!(
            self.task.await.unwrap_err().is_cancelled(),
            "ticker did not stop"
        );
        Ok(())
    }
}

/// The as-issued balance set a candidate carries is whole-set work at
/// enqueue: a canonical sort, a digest and an encoding over every recipient
/// before the snapshot can be written back. On both paths that write a
/// candidate, the direct enqueue and the block-solving share's append, that
/// work runs off the runtime and before the transaction opens: on a
/// single-threaded runtime, where the code under test and a 1 ms ticker
/// share the one thread, the ticker keeps ticking while a 300 000-recipient
/// set is prepared, and the set reaches the database once, as its canonical
/// encoding, readable as issued and no longer carried by the claim.
#[tokio::test]
async fn as_issued_snapshot_preparation_runs_off_the_runtime_on_both_write_paths() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            const RECIPIENTS: usize = 300_000;
            let ledger = db.ledger("prepare-off-runtime").await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            let sets = [
                scrambled_as_issued_set(RECIPIENTS, 1),
                scrambled_as_issued_set(RECIPIENTS, 2),
            ];
            let digests = [
                qbit_prism::prior_balances_digest(&sets[0]),
                qbit_prism::prior_balances_digest(&sets[1]),
            ];
            // What the preparation costs here: the same sort, digest and
            // encoding over the same set, measured off the runtime.
            let sample = sets[0].clone();
            let work = tokio::task::spawn_blocking(move || {
                let started = Instant::now();
                let mut sorted = sample;
                canonical_sort(&mut sorted);
                let digest = qbit_prism::prior_balances_digest(&sorted);
                let bytes = serde_json::to_vec(&sorted).expect("a balance set encodes");
                std::hint::black_box((digest, bytes));
                started.elapsed()
            })
            .await?;
            ensure!(
                work >= Duration::from_millis(50),
                "{RECIPIENTS} recipients prepare in {work:?}: too little work to observe a stall"
            );
            let [enqueued_set, appended_set] = sets;
            let mut enqueued = found(&snapshot, 21)?.candidate;
            enqueued.leased = true;
            enqueued.window.prior_balances_digest = digests[0];
            enqueued.as_issued_balances = enqueued_set;
            let mut appended = found(&snapshot, 22)?.candidate;
            appended.leased = true;
            appended.window.prior_balances_digest = digests[1];
            appended.as_issued_balances = appended_set;
            let references = [
                (enqueued.block_hash.clone(), enqueued.window),
                (appended.block_hash.clone(), appended.window),
            ];

            let ticker = Ticker::start().await?;
            ledger.enqueue_candidate(enqueued).await?;
            let enqueue_gap = ticker.take_max_gap();
            ledger.append(appended_share(2), Some(appended)).await?;
            let append_gap = ticker.take_max_gap();
            ticker.stop().await?;
            println!(
                "as_issued_preparation recipients={RECIPIENTS} work_ms={} enqueue_max_tick_gap_ms={} append_max_tick_gap_ms={}",
                work.as_millis(),
                enqueue_gap.as_millis(),
                append_gap.as_millis()
            );
            ensure!(
                enqueue_gap < work / 2,
                "the direct enqueue stalled the runtime for {enqueue_gap:?} while {work:?} of balances were prepared"
            );
            ensure!(
                append_gap < work / 2,
                "the share append stalled the runtime for {append_gap:?} while {work:?} of balances were prepared"
            );

            // Each set reached the database once, as its canonical encoding,
            // and is read back as issued.
            for (salt, (block_hash, window)) in references.iter().enumerate() {
                let mut canonical = scrambled_as_issued_set(RECIPIENTS, salt as u64 + 1);
                canonical_sort(&mut canonical);
                let stored: Vec<u8> = sqlx::query_scalar(
                    "SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
                )
                .bind(hex::encode(window.prior_balances_digest))
                .fetch_one(&db.pool)
                .await?;
                ensure!(
                    stored == serde_json::to_vec(&canonical)?,
                    "the stored set of {block_hash} is not the canonical encoding"
                );
                let read = ledger.read_window(window, BalanceSource::AsIssued).await?;
                ensure!(
                    read.prior_balances == canonical,
                    "AsIssued did not return the stored set of {block_hash}"
                );
                let row = outbox_row(&db.pool, block_hash).await?;
                ensure!(
                    row["candidate"]["leased"] == true && row["state"] == "pending",
                    "{row}"
                );
            }
            let snapshots: i64 =
                sqlx::query_scalar("SELECT count(*) FROM qbit_prism_balance_snapshots")
                    .fetch_one(&db.pool)
                    .await?;
            ensure!(snapshots == 2, "{snapshots} snapshot rows for two sets");
            // The claim carries the reference, never the set.
            let claim = ledger
                .claim_candidate(60)
                .await?
                .context("no candidate claimable")?;
            ensure!(claim.candidate.as_issued_balances.is_empty() && claim.candidate.leased);
            Ok(())
        })
    })
    .await
}

/// A balance snapshot row is immutable evidence keyed by its digest. When
/// the row a candidate's as-issued set would write is already there under
/// another encoding, the write is refused as corruption and the candidate
/// with it: the direct enqueue writes no outbox row, and the share append
/// writes neither the share nor the row. A candidate that carries no
/// as-issued set never touches the row: it is enqueued and reads the current
/// balances for as long as they still hash to its reference. Once the
/// foreign row is gone, the leased enqueue writes the canonical encoding.
#[tokio::test]
async fn a_stored_snapshot_that_is_not_the_canonical_encoding_refuses_both_write_paths_atomically(
) -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("snapshot-authenticity").await?;
            seed_carry(&db.pool).await?;
            ledger.append(appended_share(1), None).await?;
            let snapshot = ledger.snapshot(100).await?;
            ensure!(snapshot.prior_balances.len() == 2, "fixture carries no balances");
            let digest = hex::encode(snapshot_digest(&snapshot));
            let mut canonical = snapshot.prior_balances.clone();
            canonical_sort(&mut canonical);
            let canonical_bytes = serde_json::to_vec(&canonical)?;
            // The same set under another encoding: it decodes to the same
            // balances and still is not the stored form.
            let foreign = serde_json::to_vec_pretty(&canonical)?;
            ensure!(foreign != canonical_bytes);
            sqlx::query("INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2)")
                .bind(&digest).bind(&foreign).execute(&db.pool).await?;
            let counts = |pool: &PgPool| {
                let pool = pool.clone();
                async move {
                    sqlx::query_as::<_, (i64, i64)>("SELECT (SELECT count(*) FROM qbit_block_candidate_outbox),(SELECT count(*) FROM qbit_share_ledger)")
                        .fetch_one(&pool)
                        .await
                }
            };
            let stored = |pool: &PgPool| {
                let pool = pool.clone();
                let digest = digest.clone();
                async move {
                    sqlx::query_scalar::<_, Vec<u8>>(
                        "SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
                    )
                    .bind(digest)
                    .fetch_one(&pool)
                    .await
                }
            };
            for (nonce, leased) in [(30, true), (31, false)] {
                let mut candidate = found(&snapshot, nonce)?.candidate;
                candidate.leased = leased;
                candidate.as_issued_balances = snapshot.prior_balances.clone();
                let error = ledger
                    .enqueue_candidate(candidate)
                    .await
                    .err()
                    .with_context(|| {
                        format!("leased={leased}: a foreign encoding under the set's digest was accepted")
                    })?;
                ensure!(
                    format!("{error:#}").contains("immutable balance snapshot"),
                    "{error:#}"
                );
                ensure!(counts(&db.pool).await? == (0, 1), "a refused enqueue wrote rows");
                ensure!(
                    stored(&db.pool).await? == foreign,
                    "a refused enqueue rewrote the snapshot row"
                );
            }
            let mut candidate = found(&snapshot, 32)?.candidate;
            candidate.leased = true;
            candidate.as_issued_balances = snapshot.prior_balances.clone();
            let error = ledger
                .append(appended_share(2), Some(candidate))
                .await
                .err()
                .context("the share append accepted a foreign encoding")?;
            ensure!(
                format!("{error:#}").contains("immutable balance snapshot"),
                "{error:#}"
            );
            ensure!(
                counts(&db.pool).await? == (0, 1),
                "a refused append wrote the share or the row"
            );
            ensure!(stored(&db.pool).await? == foreign);
            // The missing-snapshot fallback: a candidate without the set is
            // enqueued and reads the current balances.
            let plain = found(&snapshot, 33)?.candidate;
            ensure!(plain.as_issued_balances.is_empty() && !plain.leased);
            ledger.enqueue_candidate(plain.clone()).await?;
            ensure!(counts(&db.pool).await? == (1, 1));
            let window = ledger
                .read_window(&plain.window, BalanceSource::Current)
                .await?;
            ensure!(window.prior_balances.len() == 2);
            // The foreign row gone, the leased enqueue writes the canonical
            // encoding.
            sqlx::query("DELETE FROM qbit_prism_balance_snapshots")
                .execute(&db.pool)
                .await?;
            let mut leased = found(&snapshot, 34)?.candidate;
            leased.leased = true;
            leased.as_issued_balances = snapshot.prior_balances.clone();
            ledger.enqueue_candidate(leased).await?;
            ensure!(
                stored(&db.pool).await? == canonical_bytes,
                "the leased enqueue did not store the canonical encoding"
            );
            ensure!(counts(&db.pool).await? == (2, 1));
            Ok(())
        })
    })
    .await
}
