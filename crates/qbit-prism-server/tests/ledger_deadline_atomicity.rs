//! Native ledger regressions for late publication failures, per-item migration
//! atomicity, native audit completeness and the issued-job absolute deadline.
//! Assertions read public `Ledger` results and persisted rows only. Faults are
//! triggers created inside each disposable schema; production SQL is unchanged.
//! PRISM_TEST_DATABASE_URL=<disposable> cargo test -p qbit-prism-server --test ledger_deadline_atomicity
use anyhow::{bail, ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    canonical_audit_bundle_bytes, verify_audit_bundle_with_ledger_public_key, AcceptedShare,
    AuditBundle, FoundBlock, PayoutPolicy,
};
use qbit_prism_server::ledger::{
    audit_canonical_bytes, BalanceSource, Candidate, CandidateClaim, IssuedJobSave, Ledger,
    PreparedDependency, ShareRange, SignerKeys, Snapshot, WindowError, WindowRef,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Ceiling for every barrier, lock wait and task join in this binary.
const WAIT: Duration = Duration::from_secs(10);
const FAULT: &str = "ledger-atomicity-injected-fault";
const MINERS: [(&str, u8); 3] = [("alice", 0x11), ("bob", 0x22), ("carol", 0x33)];
const PUBLICATION_TABLES: [&str; 6] = [
    "qbit_pool_blocks",
    "qbit_pool_audit_bundles",
    "qbit_pool_payout_entries",
    "qbit_payout_carry_forward",
    "qbit_ctv_fanout_sets",
    "qbit_ctv_fanout_artifacts",
];

struct Database {
    admin: PgPool,
    schema: String,
    ledger: Ledger,
}

impl Database {
    /// Must run on the test thread: the gate names the test from it.
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_atomicity_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        match Ledger::connect(url.as_str(), "ledger-atomicity".into(), 8, true).await {
            Ok(ledger) => Ok(Some(Self {
                admin,
                schema,
                ledger,
            })),
            Err(error) => {
                let _ = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
                    .execute(&admin)
                    .await;
                admin.close().await;
                Err(error)
            }
        }
    }

    async fn close(self, result: Result<()>) -> Result<()> {
        self.ledger.pool.close().await;
        let cleanup = sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await;
        self.admin.close().await;
        match (result, cleanup) {
            (Ok(()), cleanup) => cleanup.map(|_| ()).map_err(Into::into),
            (Err(error), Ok(_)) => Err(error),
            (Err(error), Err(cleanup)) => {
                Err(error.context(format!("schema cleanup also failed: {cleanup}")))
            }
        }
    }
}

/// Aborts the spawned save if the test leaves before joining it.
struct Running<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Running<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn keys() -> (ManifestSigningKey, ManifestSigningKey) {
    (
        ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap(),
        ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap(),
    )
}

fn ledger_key() -> String {
    keys().1.public_key_hex()
}

fn share(id: u64, (miner, program): (&str, u8)) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("worker:{id:064x}"),
        miner_id: miner.into(),
        order_key: miner.into(),
        p2mr_program_hex: hex::encode([program; 32]),
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

/// Accept one share per miner through the production append path.
async fn accept_shares(ledger: &Ledger, first_id: u64, miners: &[(&str, u8)]) -> Result<()> {
    for (offset, miner) in (0u64..).zip(miners) {
        ledger
            .append(share(first_id + offset, *miner), None)
            .await?;
    }
    Ok(())
}

fn found_block(snapshot: &Snapshot) -> FoundBlock {
    FoundBlock {
        block_height: 101,
        coinbase_value_sats: 500_000_000,
        network_difficulty: 100,
        anchor_job_issued_at_ms: snapshot.anchor_ms,
    }
}

fn direct_bundle(snapshot: &Snapshot) -> Result<AuditBundle> {
    let (coinbase_key, ledger_key) = keys();
    Ok(qbit_prism::build_audit_bundle(
        snapshot.shares.clone(),
        found_block(snapshot),
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        &coinbase_key,
        &ledger_key,
    )?)
}

/// One CTV fanout per recipient, so publication writes several fanout rows.
fn fanout_bundle(snapshot: &Snapshot) -> Result<AuditBundle> {
    let (coinbase_key, ledger_key) = keys();
    Ok(qbit_prism::build_audit_bundle_with_ctv_settlement_options(
        snapshot.shares.clone(),
        found_block(snapshot),
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        u64::MAX,
        qbit_prism::SettlementModeConfig {
            max_fanout_recipients_per_transaction: 1,
            ..Default::default()
        },
        Some(qbit_prism::FanoutFeeRatePolicy::new(1000, 12000)),
        None,
        vec![],
        &coinbase_key,
        &ledger_key,
    )?)
}

/// Keep the signed oracle separate from the public candidate representation.
/// When that representation changes, only construction and claim setup change.
struct TestCandidate {
    candidate: Candidate,
    bundle: AuditBundle,
}

impl std::ops::Deref for TestCandidate {
    type Target = Candidate;
    fn deref(&self) -> &Candidate {
        &self.candidate
    }
}

impl TestCandidate {
    fn conflicting_claim(self, hash: &str, token: String) -> CandidateClaim {
        CandidateClaim {
            candidate: Candidate {
                block_hash: hash.into(),
                ..self.candidate
            },
            claim_token: token,
            // A conflicting claim is never landed, so it needs no rebuilt parts.
            parts: None,
            lifecycle: Default::default(),
        }
    }
}

/// A serialized block whose header commits to the verified audit coinbase.
fn candidate(bundle: AuditBundle, payout_revision: i64, nonce: u32) -> Result<TestCandidate> {
    let report = verify_audit_bundle_with_ledger_public_key(&bundle, &ledger_key())?;
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
    let range = match (bundle.shares.first(), bundle.shares.last()) {
        (Some(first), Some(last)) => Some(ShareRange {
            first_share_seq: first.share_seq,
            last_share_seq: last.share_seq,
            share_count: u64::try_from(bundle.shares.len())?,
            snapshot_sha256: Sha256::digest(serde_json::to_vec(&bundle.shares)?).into(),
        }),
        _ => None,
    };
    let candidate = Candidate {
        block_hash: hex::encode(hash),
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "job".into(),
        payout_revision,
        window: WindowRef {
            anchor_ms: bundle.found_block.anchor_job_issued_at_ms,
            prior_balances_digest: qbit_prism::prior_balances_digest(&bundle.prior_balances),
            shares: range,
        },
        bootstrap_share: None,
        found_block: bundle.found_block.clone(),
        payout_policy: bundle.payout_policy.clone(),
        ctv: None,
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys {
            manifest_key_hex: bundle
                .signed_coinbase_manifest
                .signature
                .public_key_hex
                .clone(),
            ledger_key_hex: bundle
                .ledger_window_attestation
                .signature
                .public_key_hex
                .clone(),
        },
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

async fn claim(ledger: &Ledger, block: &TestCandidate) -> Result<CandidateClaim> {
    ledger.enqueue_candidate(block.candidate.clone()).await?;
    let claim = ledger
        .claim_candidate(60)
        .await?
        .context("enqueued candidate was not claimable")?;
    ensure!(claim.candidate.block_hash == block.candidate.block_hash);
    Ok(claim.with_bundle(block.bundle.clone()))
}

async fn land_confirmed(ledger: &Ledger, block: &TestCandidate) -> Result<()> {
    let claim = claim(ledger, block).await?;
    ledger.land_candidate(&claim, &ledger_key()).await?;
    ledger.finish_candidate(&claim, true, None).await
}

/// Build, land and confirm a block over every share accepted so far.
async fn landed_block(
    ledger: &Ledger,
    nonce: u32,
    build: fn(&Snapshot) -> Result<AuditBundle>,
) -> Result<TestCandidate> {
    let snapshot = ledger.snapshot(100).await?;
    let block = candidate(build(&snapshot)?, snapshot.payout_revision, nonce)?;
    land_confirmed(ledger, &block).await?;
    Ok(block)
}

/// Every persisted row a landing publishes for one block hash.
async fn block_state(pool: &PgPool, hash: &str, tables: &[&str]) -> Result<Value> {
    let mut state = serde_json::Map::new();
    for table in tables {
        let rows: Value = sqlx::query_scalar(&format!("SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM {table} t WHERE t.block_hash=$1"))
            .bind(hash).fetch_one(pool).await?;
        state.insert((*table).into(), rows);
    }
    Ok(Value::Object(state))
}

fn row_counts(state: &Value) -> Value {
    Value::Object(
        state
            .as_object()
            .into_iter()
            .flatten()
            .map(|(table, rows)| (table.clone(), json!(rows.as_array().map_or(0, Vec::len))))
            .collect(),
    )
}

fn snapshot_digest(bundle: &AuditBundle) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(
        &bundle.shares,
    )?)))
}

async fn snapshot_row(pool: &PgPool, digest: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(s) FROM qbit_prism_audit_snapshots s WHERE snapshot_sha256=$1",
    )
    .bind(digest)
    .fetch_optional(pool)
    .await?)
}

async fn carry_balances(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY to_jsonb(c)::text),'[]'::jsonb) FROM qbit_payout_carry_forward_current c")
        .fetch_one(pool).await?)
}

/// The served audit body must be the exact signed bundle.
async fn ensure_serves(ledger: &Ledger, block: &TestCandidate) -> Result<()> {
    let body = ledger
        .audit_bundle(&block.block_hash)
        .await?
        .with_context(|| format!("no audit body for {}", block.block_hash))?;
    let served: AuditBundle = serde_json::from_value(body)?;
    ensure!(
        canonical_audit_bundle_bytes(&served)? == canonical_audit_bundle_bytes(&block.bundle)?,
        "served audit differs from the signed bundle for {}",
        block.block_hash
    );
    Ok(())
}

/// Install a row trigger that raises `FAULT` followed by `detail` (a text SQL
/// expression evaluated inside the failing transaction) when `condition` holds.
async fn inject_fault(
    pool: &PgPool,
    table: &str,
    event: &str,
    condition: &str,
    detail: &str,
) -> Result<()> {
    sqlx::raw_sql(&format!("CREATE FUNCTION ledger_atomicity_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF {condition} THEN RAISE EXCEPTION '{FAULT} %', {detail}; END IF; RETURN NEW; END $$; CREATE TRIGGER ledger_atomicity_fault BEFORE {event} ON {table} FOR EACH ROW EXECUTE FUNCTION ledger_atomicity_fault()"))
        .execute(pool).await?;
    Ok(())
}

async fn remove_fault(pool: &PgPool, table: &str) -> Result<()> {
    sqlx::raw_sql(&format!(
        "DROP TRIGGER ledger_atomicity_fault ON {table}; DROP FUNCTION ledger_atomicity_fault()"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Require that an operation failed at the injected fault; return its detail.
fn injected<T: std::fmt::Debug>(result: Result<T>) -> Result<Value> {
    let error = match result {
        Ok(value) => bail!("operation succeeded despite the injected fault: {value:?}"),
        Err(error) => error,
    };
    let text = format!("{error:#}");
    let detail = text
        .find(FAULT)
        .map(|start| &text[start + FAULT.len()..])
        .with_context(|| format!("failure did not come from the injected fault: {text}"))?;
    let open = detail
        .find('{')
        .with_context(|| format!("injected fault carried no detail: {text}"))?;
    // The error chain may repeat the database message after the detail.
    serde_json::Deserializer::from_str(&detail[open..])
        .into_iter::<Value>()
        .next()
        .with_context(|| format!("injected fault carried no detail: {text}"))?
        .with_context(|| format!("injected fault detail is not JSON: {text}"))
}

/// Simulate out-of-band loss of immutable share history. The immutability
/// trigger is suspended only inside this transaction; rows are kept aside so
/// `restore_shares` can put back exactly what was lost.
async fn lose_shares(pool: &PgPool, seqs: &[u64]) -> Result<()> {
    let seqs: Vec<i64> = seqs
        .iter()
        .map(|&s| i64::try_from(s))
        .collect::<Result<_, _>>()?;
    let mut tx = pool.begin().await?;
    sqlx::raw_sql("CREATE TABLE IF NOT EXISTS ledger_atomicity_lost_shares (LIKE qbit_share_ledger); CREATE TABLE IF NOT EXISTS ledger_atomicity_lost_hashes (LIKE qbit_prism_share_hashes)")
        .execute(&mut *tx).await?;
    sqlx::query("INSERT INTO ledger_atomicity_lost_shares SELECT * FROM qbit_share_ledger WHERE share_seq=ANY($1)")
        .bind(&seqs).execute(&mut *tx).await?;
    sqlx::query("WITH lost AS (DELETE FROM qbit_prism_share_hashes h USING qbit_share_ledger s WHERE h.share_id=s.share_id AND s.share_seq=ANY($1) RETURNING h.*) INSERT INTO ledger_atomicity_lost_hashes SELECT * FROM lost")
        .bind(&seqs).execute(&mut *tx).await?;
    sqlx::raw_sql(
        "ALTER TABLE qbit_share_ledger DISABLE TRIGGER qbit_prism_immutable_share_history",
    )
    .execute(&mut *tx)
    .await?;
    let deleted = sqlx::query("DELETE FROM qbit_share_ledger WHERE share_seq=ANY($1)")
        .bind(&seqs)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    sqlx::raw_sql(
        "ALTER TABLE qbit_share_ledger ENABLE TRIGGER qbit_prism_immutable_share_history",
    )
    .execute(&mut *tx)
    .await?;
    ensure!(
        deleted == seqs.len() as u64,
        "fixture lost {deleted} of {seqs:?}"
    );
    tx.commit().await?;
    Ok(())
}

async fn restore_shares(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql("INSERT INTO qbit_share_ledger SELECT * FROM ledger_atomicity_lost_shares; INSERT INTO qbit_prism_share_hashes SELECT * FROM ledger_atomicity_lost_hashes; DELETE FROM ledger_atomicity_lost_hashes; DELETE FROM ledger_atomicity_lost_shares")
        .execute(pool).await?;
    Ok(())
}

#[tokio::test]
async fn landing_rows_roll_back_on_late_failure() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = landing_rows_roll_back(&db.ledger).await;
    db.close(result).await
}

async fn landing_rows_roll_back(ledger: &Ledger) -> Result<()> {
    let pool = &ledger.pool;
    accept_shares(ledger, 100, &MINERS).await?;
    let snapshot = ledger.snapshot(100).await?;
    let block = candidate(fanout_bundle(&snapshot)?, snapshot.payout_revision, 100)?;
    let hash = &block.block_hash;
    let set = block
        .bundle
        .ctv_fanout_manifest_set
        .as_ref()
        .context("bundle has no CTV fanout set")?;
    let fanouts: Vec<String> = set
        .manifests
        .iter()
        .map(|m| m.fanout_txid.clone())
        .collect();
    let recipients = block.bundle.payout_policy_manifest.accounts.len();
    ensure!(
        fanouts.len() >= 3 && recipients >= 3,
        "fixture must publish several recipients and fanouts: {recipients} recipients, {} fanouts",
        fanouts.len()
    );
    let digest = snapshot_digest(&block.bundle)?;
    let claim = claim(ledger, &block).await?;
    let revision = ledger.payout_revision().await?;
    let carry = carry_balances(pool).await?;
    let empty = block_state(pool, hash, &PUBLICATION_TABLES).await?;
    ensure!(row_counts(&empty)
        .as_object()
        .unwrap()
        .values()
        .all(|n| n == 0));
    ensure!(snapshot_row(pool, &digest).await?.is_none());

    // Fail the last fanout row, after the block, audit, snapshot, payout,
    // carry, set and earlier fanout writes of the same landing.
    let count =
        |table: &str| format!("'{table}',(SELECT count(*) FROM {table} WHERE block_hash='{hash}')");
    let detail = format!(
        "jsonb_build_object({},'qbit_prism_audit_snapshots',(SELECT count(*) FROM qbit_prism_audit_snapshots WHERE snapshot_sha256='{digest}'))::text",
        PUBLICATION_TABLES.map(count).join(",")
    );
    let last = fanouts.last().unwrap();
    inject_fault(
        pool,
        "qbit_ctv_fanout_artifacts",
        "INSERT",
        &format!("NEW.fanout_txid='{last}'"),
        &detail,
    )
    .await?;
    let in_flight = injected(ledger.land_candidate(&claim, &ledger_key()).await)?;
    ensure!(
        block_state(pool, hash, &PUBLICATION_TABLES).await? == empty,
        "failed landing left publication rows for {hash}"
    );
    ensure!(
        snapshot_row(pool, &digest).await?.is_none(),
        "failed landing left its share snapshot"
    );
    ensure!(ledger.audit_bundle(hash).await?.is_none());
    ensure!(ledger.payout_revision().await? == revision);
    ensure!(
        carry_balances(pool).await? == carry,
        "failed landing changed current carry balances"
    );

    // Control: the same live claim lands the complete set once the fault is gone.
    remove_fault(pool, "qbit_ctv_fanout_artifacts").await?;
    ledger.land_candidate(&claim, &ledger_key()).await?;
    let landed = block_state(pool, hash, &PUBLICATION_TABLES).await?;
    let mut expected = row_counts(&landed);
    expected["qbit_prism_audit_snapshots"] = json!(1);
    ensure!(
        expected["qbit_pool_blocks"] == 1
            && expected["qbit_pool_audit_bundles"] == 1
            && expected["qbit_pool_payout_entries"] == recipients
            && expected["qbit_payout_carry_forward"].as_u64() > Some(0)
            && expected["qbit_ctv_fanout_sets"] == 1
            && expected["qbit_ctv_fanout_artifacts"] == fanouts.len(),
        "successful landing did not persist the complete set: {expected}"
    );
    ensure!(snapshot_row(pool, &digest).await?.is_some());
    // The fault fired late: everything but the final fanout already existed
    // inside the transaction that was then rolled back.
    let mut late = expected.clone();
    late["qbit_ctv_fanout_artifacts"] = json!(fanouts.len() - 1);
    ensure!(
        in_flight == late,
        "fault did not fire after the other publication writes: {in_flight} vs {late}"
    );
    let stored: Vec<String> = sqlx::query_scalar(
        "SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1 ORDER BY chunk_index",
    )
    .bind(hash)
    .fetch_all(pool)
    .await?;
    ensure!(stored == fanouts);
    ensure_serves(ledger, &block).await?;
    // A retry of the landed hash is a no-op.
    ledger.land_candidate(&claim, &ledger_key()).await?;
    ensure!(block_state(pool, hash, &PUBLICATION_TABLES).await? == landed);
    Ok(())
}

/// Reshape a landed native row as a 2.x externalized row: its body lives only
/// in the `body_uri` file, as the legacy import finds it.
async fn externalize(pool: &PgPool, block: &TestCandidate, dir: &std::path::Path) -> Result<()> {
    let path = dir.join(format!("legacy-audit-{}.json", block.block_hash));
    std::fs::write(&path, serde_json::to_vec(&block.bundle)?)?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=NULL,share_snapshot_sha256=NULL,body_uri=$2 WHERE block_hash=$1")
        .bind(&block.block_hash).bind(path.to_str().context("non-UTF-8 path")?).execute(pool).await?;
    Ok(())
}

async fn audit_row(pool: &PgPool, hash: &str) -> Result<Value> {
    sqlx::query_scalar("SELECT to_jsonb(a) FROM qbit_pool_audit_bundles a WHERE block_hash=$1")
        .bind(hash)
        .fetch_optional(pool)
        .await?
        .with_context(|| format!("audit row {hash} disappeared"))
}

async fn ensure_imported(ledger: &Ledger, block: &TestCandidate) -> Result<()> {
    let expected = canonical_audit_bundle_bytes(&block.bundle)?;
    ensure!(
        audit_canonical_bytes(&ledger.pool, &block.block_hash).await? == Some(expected),
        "{} does not hold its canonical imported bytes",
        block.block_hash
    );
    ensure_serves(ledger, block).await
}

#[tokio::test]
async fn import_item_failure_preserves_completed_items() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = import_item_failure(&db.ledger).await;
    db.close(result).await
}

async fn import_item_failure(ledger: &Ledger) -> Result<()> {
    let pool = &ledger.pool;
    let mut blocks = Vec::new();
    for nonce in 0..3u32 {
        accept_shares(ledger, 200 + u64::from(nonce) * 10, &MINERS[..2]).await?;
        blocks.push(landed_block(ledger, 200 + nonce, direct_bundle).await?);
    }
    // The import walks legacy rows in block-hash order.
    blocks.sort_by(|a, b| a.block_hash.cmp(&b.block_hash));
    let dir = tempfile::tempdir()?;
    let other_tables = &PUBLICATION_TABLES[..1]
        .iter()
        .chain(&PUBLICATION_TABLES[2..])
        .copied()
        .collect::<Vec<_>>();
    let mut effects = Vec::new();
    let mut legacy = Vec::new();
    for block in &blocks {
        externalize(pool, block, dir.path()).await?;
        ensure!(ledger.audit_bundle(&block.block_hash).await?.is_none());
        effects.push(block_state(pool, &block.block_hash, other_tables).await?);
        legacy.push(audit_row(pool, &block.block_hash).await?);
    }
    let failing = &blocks[1].block_hash;
    inject_fault(
        pool,
        "qbit_pool_audit_bundles",
        "UPDATE",
        &format!("NEW.block_hash='{failing}' AND NEW.canonical_audit_bytes IS NOT NULL"),
        "'{}'",
    )
    .await?;
    injected(
        ledger
            .import_legacy_audits(Some(dir.path()), &ledger_key())
            .await,
    )?;
    ensure_imported(ledger, &blocks[0]).await?;
    let completed = audit_row(pool, &blocks[0].block_hash).await?;
    for (block, before) in blocks.iter().zip(&legacy).skip(1) {
        ensure!(
            audit_row(pool, &block.block_hash).await? == *before,
            "import failure changed unfinished item {}",
            block.block_hash
        );
        ensure!(ledger.audit_bundle(&block.block_hash).await?.is_none());
    }

    // Restart after the fault is removed: exactly the unfinished items import.
    remove_fault(pool, "qbit_pool_audit_bundles").await?;
    ensure!(
        ledger
            .import_legacy_audits(Some(dir.path()), &ledger_key())
            .await?
            == 2,
        "restart did not import exactly the two unfinished items"
    );
    ensure!(
        audit_row(pool, &blocks[0].block_hash).await? == completed,
        "restart rewrote the item completed before the failure"
    );
    for block in &blocks {
        ensure_imported(ledger, block).await?;
    }
    ensure!(
        ledger
            .import_legacy_audits(Some(dir.path()), &ledger_key())
            .await?
            == 0
    );
    let audits: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_pool_audit_bundles")
        .fetch_one(pool)
        .await?;
    ensure!(audits == 3, "import duplicated audit rows: {audits}");
    for (block, before) in blocks.iter().zip(&effects) {
        ensure!(
            block_state(pool, &block.block_hash, other_tables).await? == *before,
            "import changed non-audit state of {}",
            block.block_hash
        );
    }
    Ok(())
}

async fn fanout_rows(pool: &PgPool, hash: &str) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object('set',(SELECT jsonb_build_object('manifest_set',manifest_set,'digest',manifest_set_sha256,'count',fanout_count,'mode',settlement_mode) FROM qbit_ctv_fanout_sets WHERE block_hash=$1),'fanouts',(SELECT COALESCE(jsonb_agg(jsonb_build_object('txid',fanout_txid,'manifest',manifest,'manifest_sha256',manifest_sha256,'set',manifest_set_sha256,'chunk',chunk_index,'chunks',chunk_count,'tx',fanout_tx_hex,'status',settlement_status,'claim',claim_token) ORDER BY chunk_index),'[]'::jsonb) FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1))")
        .bind(hash).fetch_one(pool).await?)
}

#[tokio::test]
async fn backfill_item_failure_is_atomic() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = backfill_item_failure(&db.ledger).await;
    db.close(result).await
}

async fn backfill_item_failure(ledger: &Ledger) -> Result<()> {
    let pool = &ledger.pool;
    accept_shares(ledger, 300, &MINERS).await?;
    let first = landed_block(ledger, 300, fanout_bundle).await?;
    accept_shares(ledger, 310, &MINERS).await?;
    let second = landed_block(ledger, 301, fanout_bundle).await?;
    let original = [
        fanout_rows(pool, &first.block_hash).await?,
        fanout_rows(pool, &second.block_hash).await?,
    ];
    let chunks = original[1]["fanouts"].as_array().map_or(0, Vec::len);
    ensure!(
        chunks >= 3,
        "fixture needs several fanouts per block: {chunks}"
    );
    ensure!(ledger.backfill_ctv(&ledger_key()).await? == 0);

    // The earlier block misses one fanout; the later one lost its whole set,
    // so its repair writes the set and every fanout in one transaction.
    sqlx::query("DELETE FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1 AND chunk_index=1")
        .bind(&first.block_hash)
        .execute(pool)
        .await?;
    sqlx::raw_sql(&format!("DELETE FROM qbit_ctv_fanout_artifacts WHERE block_hash='{0}'; DELETE FROM qbit_ctv_fanout_sets WHERE block_hash='{0}'", second.block_hash))
        .execute(pool).await?;
    let missing = fanout_rows(pool, &second.block_hash).await?;
    ensure!(missing["set"].is_null() && missing["fanouts"] == json!([]));
    inject_fault(
        pool,
        "qbit_ctv_fanout_artifacts",
        "INSERT",
        &format!("NEW.block_hash='{}' AND NEW.chunk_index={}", second.block_hash, chunks - 1),
        "jsonb_build_object('sets',(SELECT count(*) FROM qbit_ctv_fanout_sets WHERE block_hash=NEW.block_hash),'fanouts',(SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE block_hash=NEW.block_hash))::text",
    )
    .await?;
    let in_flight = injected(ledger.backfill_ctv(&ledger_key()).await)?;
    ensure!(
        in_flight == json!({"sets": 1, "fanouts": chunks - 1}),
        "fault did not fire after the other rows of the block's repair: {in_flight}"
    );
    ensure!(
        fanout_rows(pool, &first.block_hash).await? == original[0],
        "the earlier block's completed repair did not survive the later failure"
    );
    ensure!(
        fanout_rows(pool, &second.block_hash).await? == missing,
        "a failed block repair committed part of its fanout set"
    );

    remove_fault(pool, "qbit_ctv_fanout_artifacts").await?;
    ensure!(
        ledger.backfill_ctv(&ledger_key()).await? == 1,
        "restart did not repair exactly the failed block"
    );
    ensure!(fanout_rows(pool, &first.block_hash).await? == original[0]);
    ensure!(fanout_rows(pool, &second.block_hash).await? == original[1]);
    ensure!(ledger.backfill_ctv(&ledger_key()).await? == 0);
    Ok(())
}

fn window_ref(anchor_ms: i64, shares: &[AcceptedShare], snapshot: &Snapshot) -> Result<WindowRef> {
    let mut balances = snapshot.prior_balances.clone();
    balances.sort_by(|a, b| {
        (&a.order_key, &a.recipient_id, &a.p2mr_program_hex).cmp(&(
            &b.order_key,
            &b.recipient_id,
            &b.p2mr_program_hex,
        ))
    });
    Ok(WindowRef {
        anchor_ms,
        prior_balances_digest: qbit_prism::prior_balances_digest(&balances),
        shares: Some(ShareRange {
            first_share_seq: shares.first().context("empty window")?.share_seq,
            last_share_seq: shares.last().unwrap().share_seq,
            share_count: shares.len() as u64,
            snapshot_sha256: Sha256::digest(serde_json::to_vec(shares)?).into(),
        }),
    })
}

#[tokio::test]
async fn incomplete_window_is_typed_error() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = incomplete_window(&db.ledger).await;
    db.close(result).await
}

async fn incomplete_window(ledger: &Ledger) -> Result<()> {
    accept_shares(ledger, 400, &MINERS).await?;
    accept_shares(ledger, 410, &MINERS[..2]).await?;
    let snapshot = ledger.snapshot(100).await?;
    let mut shares = snapshot.shares.clone();
    shares.sort_by_key(|share| share.share_seq);
    ensure!(shares.len() == 5, "fixture window: {}", shares.len());
    let head = window_ref(snapshot.anchor_ms, &shares[..3], &snapshot)?;
    let tail = window_ref(snapshot.anchor_ms, &shares[2..], &snapshot)?;
    for (window, expected) in [(&head, &shares[..3]), (&tail, &shares[2..])] {
        let read = ledger.read_window(window, BalanceSource::Current).await?;
        ensure!(read.shares == expected, "control window differs");
    }

    // Lose the head's first endpoint and an interior row of the tail.
    lose_shares(&ledger.pool, &[shares[0].share_seq, shares[3].share_seq]).await?;
    let head_read = ledger.read_window(&head, BalanceSource::Current).await;
    ensure!(
        matches!(
            head_read,
            Err(WindowError::Incomplete {
                expected: 3,
                got
            }) if got < 3
        ),
        "missing first endpoint was not a typed incomplete window: {head_read:?}"
    );
    let tail_read = ledger.read_window(&tail, BalanceSource::Current).await;
    ensure!(
        matches!(
            tail_read,
            Err(WindowError::Incomplete {
                expected: 3,
                got: 2
            })
        ),
        "missing interior row was not a typed incomplete window: {tail_read:?}"
    );
    Ok(())
}

#[tokio::test]
async fn incomplete_audit_snapshot_returns_no_body() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = incomplete_audit_snapshot(&db.ledger).await;
    db.close(result).await
}

async fn incomplete_audit_snapshot(ledger: &Ledger) -> Result<()> {
    let pool = &ledger.pool;
    accept_shares(ledger, 500, &MINERS).await?;
    let block = landed_block(ledger, 500, direct_bundle).await?;
    let hash = &block.block_hash;
    let seqs: Vec<u64> = block.bundle.shares.iter().map(|s| s.share_seq).collect();
    ensure!(seqs.len() == 3, "fixture snapshot: {seqs:?}");
    let digest = snapshot_digest(&block.bundle)?;
    ensure_serves(ledger, &block).await?;
    let durable = json!({
        "block": block_state(pool, hash, &PUBLICATION_TABLES).await?,
        "snapshot": snapshot_row(pool, &digest).await?.context("native audit has no snapshot")?,
    });
    let state = || async {
        Ok::<_, anyhow::Error>(json!({
            "block": block_state(pool, hash, &PUBLICATION_TABLES).await?,
            "snapshot": snapshot_row(pool, &digest).await?,
        }))
    };

    for (case, lost) in [
        ("interior share", vec![seqs[1]]),
        ("every remaining share", vec![seqs[0], seqs[2]]),
    ] {
        lose_shares(pool, &lost).await?;
        match ledger.audit_bundle(hash).await {
            Err(_) => {}
            Ok(Some(_)) => bail!("{case}: served a body from incomplete share history"),
            Ok(None) => bail!("{case}: incomplete snapshot was reported as an absent audit"),
        }
        ensure!(
            audit_canonical_bytes(pool, hash).await.is_err(),
            "{case}: canonical bytes were produced from incomplete share history"
        );
        ensure!(
            state().await? == durable,
            "{case}: refusal changed durable audit state"
        );
    }

    // Nothing was repaired or rewritten: restoring the lost history alone
    // makes the original audit servable again.
    restore_shares(pool).await?;
    ensure_serves(ledger, &block).await?;
    ensure!(state().await? == durable);
    let absent = "00".repeat(32);
    ensure!(ledger.audit_bundle(&absent).await?.is_none());
    ensure!(audit_canonical_bytes(pool, &absent).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn conflicting_audit_preserves_original() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = conflicting_audit(&db.ledger).await;
    db.close(result).await
}

async fn conflicting_audit(ledger: &Ledger) -> Result<()> {
    let pool = &ledger.pool;
    accept_shares(ledger, 600, &MINERS[..2]).await?;
    let snapshot = ledger.snapshot(100).await?;
    let block = candidate(fanout_bundle(&snapshot)?, snapshot.payout_revision, 600)?;
    let hash = &block.block_hash;
    let claim = claim(ledger, &block).await?;
    ledger.land_candidate(&claim, &ledger_key()).await?;
    let digest = snapshot_digest(&block.bundle)?;
    let durable = json!({
        "block": block_state(pool, hash, &PUBLICATION_TABLES).await?,
        "snapshot": snapshot_row(pool, &digest).await?,
        "carry": carry_balances(pool).await?,
        "revision": ledger.payout_revision().await?,
    });

    // A different, validly signed audit and serialized block presented for the
    // already-landed hash under the same live claim.
    accept_shares(ledger, 610, &MINERS[2..]).await?;
    let other_snapshot = ledger.snapshot(100).await?;
    let other = candidate(
        direct_bundle(&other_snapshot)?,
        other_snapshot.payout_revision,
        601,
    )?;
    ensure!(
        canonical_audit_bundle_bytes(&other.bundle)?
            != canonical_audit_bundle_bytes(&block.bundle)?
    );
    let conflicting = other.conflicting_claim(hash, claim.claim_token.clone());
    ensure!(
        ledger
            .land_candidate(&conflicting, &ledger_key())
            .await
            .is_err(),
        "a conflicting audit landed over an existing block hash"
    );
    let after = json!({
        "block": block_state(pool, hash, &PUBLICATION_TABLES).await?,
        "snapshot": snapshot_row(pool, &digest).await?,
        "carry": carry_balances(pool).await?,
        "revision": ledger.payout_revision().await?,
    });
    ensure!(
        after == durable,
        "conflicting audit changed the original block state"
    );
    ensure_serves(ledger, &block).await?;
    // The claim itself stayed valid: the original candidate still retries cleanly.
    ledger.land_candidate(&claim, &ledger_key()).await?;
    ensure!(block_state(pool, hash, &PUBLICATION_TABLES).await? == durable["block"]);
    Ok(())
}

const PREPARED: &str = "prepared:late-expiry";
const ISSUED: &str = "issued:late-expiry";

async fn job_row(pool: &PgPool, id: &str) -> Result<Option<Value>> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(j) FROM qbit_prism_jobs j WHERE job_id=$1")
            .bind(id)
            .fetch_optional(pool)
            .await?,
    )
}

fn issued_payload(expiry: i64) -> Value {
    json!({"prepared_key":PREPARED,"expires_at_ms":expiry,
        "worker":{"username":"late.worker"},"extranonce1":"01020304"})
}

async fn database_deadline(pool: &PgPool, after_ms: i64) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint + $1")
            .bind(after_ms)
            .fetch_one(pool)
            .await?,
    )
}

#[tokio::test]
async fn issued_job_late_expiry_rolls_back() -> Result<()> {
    let Some(mut db) = Database::open().await? else {
        return Ok(());
    };
    // This test holds a row lock across the absolute job deadline. Remove
    // competing server timeouts on every connection in this fixture pool;
    // WAIT still bounds the barriers and task join. Other tests keep defaults.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(WAIT)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SELECT set_config('lock_timeout','0',false),set_config('statement_timeout','0',false)")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(db.ledger.pool.connect_options().as_ref().clone())
        .await;
    let result = match pool {
        Ok(pool) => {
            db.ledger.pool.close().await;
            db.ledger.pool = pool;
            issued_job_late_expiry(&db.ledger).await
        }
        Err(error) => Err(error.into()),
    };
    db.close(result).await
}

async fn issued_job_late_expiry(ledger: &Ledger) -> Result<()> {
    let pool = &ledger.pool;
    let parent = "11".repeat(32);
    let revision = ledger.observe_chain_view(&parent, 100, "01").await?;
    let prepared = json!({"snapshot":{"payout_revision":revision,"anchor_ms":1234},
        "template":{"previousblockhash":parent},"coinbase_suffix":"late-expiry-entropy"});
    ledger
        .save_job(PREPARED, &prepared, revision, &parent, 60)
        .await?;
    // An aged dependency: a successful save must renew it.
    sqlx::query("UPDATE qbit_prism_jobs SET expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1")
        .bind(PREPARED).execute(pool).await?;
    let before = job_row(pool, PREPARED)
        .await?
        .context("prepared row missing")?;

    // Hold the dependency row so the save waits on its row lock after the
    // early deadline check, then let the absolute deadline pass during the wait.
    let mut holder = pool.begin().await?;
    sqlx::query("SELECT job_id FROM qbit_prism_jobs WHERE job_id=$1 FOR UPDATE")
        .bind(PREPARED)
        .execute(&mut *holder)
        .await?;
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await?;
    let expiry = database_deadline(pool, 1_500).await?;
    let mut saver = {
        let ledger = ledger.clone();
        let parent = parent.clone();
        Running(tokio::spawn(async move {
            let dependency = PreparedDependency {
                key: PREPARED,
                original_revision: revision,
                parent: &parent,
            };
            ledger
                .save_issued_job(
                    ISSUED,
                    &issued_payload(expiry),
                    revision,
                    &parent,
                    expiry,
                    dependency,
                    None,
                )
                .await
        }))
    };
    timeout(WAIT, async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))")
                .bind(holder_pid).fetch_one(pool).await?;
            if waiting {
                return Ok::<_, anyhow::Error>(());
            }
            if saver.0.is_finished() {
                let early = (&mut saver.0).await?;
                bail!("save finished before waiting on the dependency row lock: {early:?}");
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("save never waited on the held dependency row")??;
    timeout(WAIT, async {
        loop {
            let elapsed: bool = sqlx::query_scalar(
                "SELECT to_timestamp($1::double precision/1000) <= clock_timestamp()",
            )
            .bind(expiry)
            .fetch_one(pool)
            .await?;
            if elapsed {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("issued deadline did not elapse")??;
    ensure!(
        !saver.0.is_finished(),
        "save stopped waiting before the row lock was released"
    );
    holder.rollback().await?;
    let outcome = timeout(WAIT, &mut saver.0)
        .await
        .context("save did not finish after the row lock was released")??;
    ensure!(
        outcome.is_err(),
        "save published after its absolute deadline elapsed during a row-lock wait: {outcome:?}"
    );
    let error = outcome.unwrap_err();
    ensure!(
        format!("{error:#}").contains("issued job deadline elapsed"),
        "save failed for a different reason than its absolute deadline: {error:#}"
    );
    ensure!(
        job_row(pool, ISSUED).await?.is_none(),
        "expired issued job was published"
    );
    ensure!(
        job_row(pool, PREPARED).await? == Some(before.clone()),
        "expired save left a partial dependency renewal"
    );

    // Control: the same save with a live deadline publishes and renews.
    let expiry = database_deadline(pool, 30_000).await?;
    let saved = ledger
        .save_issued_job(
            ISSUED,
            &issued_payload(expiry),
            revision,
            &parent,
            expiry,
            PreparedDependency {
                key: PREPARED,
                original_revision: revision,
                parent: &parent,
            },
            None,
        )
        .await?;
    ensure!(saved == IssuedJobSave::Saved);
    ensure!(ledger.job(ISSUED).await? == Some(issued_payload(expiry)));
    ensure!(job_row(pool, PREPARED).await? != Some(before));
    ensure!(ledger.job(PREPARED).await? == Some(prepared));
    let covers_issued_deadline: bool = sqlx::query_scalar(
        "SELECT expires_at >= to_timestamp($2::double precision/1000) FROM qbit_prism_jobs WHERE job_id=$1",
    )
    .bind(PREPARED)
    .bind(expiry)
    .fetch_one(pool)
    .await?;
    ensure!(
        covers_issued_deadline,
        "renewed dependency expires before the issued job"
    );
    Ok(())
}
