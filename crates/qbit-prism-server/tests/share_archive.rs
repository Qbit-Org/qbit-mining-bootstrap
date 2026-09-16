//! Share ledger retention (#144): seal, archive, verify, detach, drop and
//! restore a partition, and serve an archived block from its stored bytes.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test share_archive -- --test-threads=4
//! ```
//!
//! The partition grid is the shipped one (2^24 rows per cell), so these tests
//! place rows by explicit `share_seq` instead of writing 16 million of them:
//! the online horizon is moved by inserting a later partition's rows and
//! advancing the sequence, exactly as an operator's ledger reaches the next
//! cell on its own.
use anyhow::{ensure, Context, Result};
use axum::http::StatusCode;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, canonical_audit_bundle_bytes, verify_audit_bundle_with_ledger_public_key,
    AcceptedShare, AuditBundle, FoundBlock, PayoutPolicy,
};
use qbit_prism_server::{
    api::{router, ApiConfig, ApiState},
    ledger::{
        archive, audit_canonical_bytes, audit_completeness, Candidate, CandidateClaim, Ledger,
        SignerKeys, WindowRef,
    },
};
use qbit_prism_test_gate as gate;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::io::Read;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const P0: &str = "qbit_share_ledger_p0";
const P1: &str = "qbit_share_ledger_p1";
const P2: &str = "qbit_share_ledger_p2";

// ---------------------------------------------------------------------------
// Per-test schema, the fixture of tests/audit_body_normalization.rs
// ---------------------------------------------------------------------------

struct Database {
    admin: PgPool,
    schema: String,
    url: String,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_share_archive_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        Ok(Some(Self {
            admin,
            schema,
            url: url.to_string(),
        }))
    }

    async fn ledger(&self, id: &str) -> Result<Ledger> {
        Ledger::connect(&self.url, id.to_owned(), 8, true).await
    }

    /// EP-ERRORS: the schema goes away on success and on failure alike.
    async fn close(self, ledgers: Vec<Ledger>) -> Result<()> {
        for ledger in ledgers {
            ledger.pool.close().await;
        }
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

fn ledger_public_key() -> String {
    keys().1.public_key_hex()
}

// ---------------------------------------------------------------------------
// Ledger rows placed by share_seq
// ---------------------------------------------------------------------------

/// Write `[first, last]` directly, with the shape a native append produces and
/// deliberate variety in every column the archive encodes: a non-ASCII
/// `share_id`, a `credit_policy` on some rows, a rejected row with a
/// `reject_reason` on others, and microsecond-resolution timestamps.
async fn insert_shares(
    pool: &PgPool,
    first: i64,
    last: i64,
    difficulty: i64,
    writer: &str,
    age_seconds: f64,
) -> Result<()> {
    insert_shares_into(
        pool,
        "qbit_share_ledger",
        first,
        last,
        difficulty,
        writer,
        age_seconds,
    )
    .await
}

/// The same rows written into one relation by name: a detached partition,
/// which the parent no longer routes to.
async fn insert_shares_into(
    pool: &PgPool,
    table: &str,
    first: i64,
    last: i64,
    difficulty: i64,
    writer: &str,
    age_seconds: f64,
) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO {table}(share_seq,share_id,miner_id,payout_order_key,p2mr_program,\
         share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,\
         accepted,reject_reason,credit_policy,writer_id,writer_epoch) \
         SELECT i,'miner-'||(i%5)::text||':é'||lpad(i::text,16,'0'),'m'||(i%5)::text,'k',\
         decode(repeat(lpad(to_hex(170+(i%5)::int),2,'0'),32),'hex'),\
         $3::text::numeric,1000,100,'job-a',\
         statement_timestamp()-make_interval(secs=>$5+($2-i)::double precision/1000),\
         1700000000,\
         statement_timestamp()-make_interval(secs=>$5+($2-i)::double precision/1000),\
         i%11<>0,CASE WHEN i%11=0 THEN 'stale-job' END,\
         CASE WHEN i%7=0 THEN 'stale-grace' END,$4,0 \
         FROM generate_series($1::bigint,$2::bigint) AS g(i)"
    ))
    .bind(first)
    .bind(last)
    .bind(difficulty.to_string())
    .bind(writer)
    .bind(age_seconds)
    .execute(pool)
    .await
    .with_context(|| format!("inserting shares {first}..={last}"))?;
    Ok(())
}

/// Move the share sequence, as the ledger does by handing out values, so the
/// next append lands in the partition the fixture just wrote into.
async fn set_sequence(pool: &PgPool, value: i64) -> Result<()> {
    sqlx::query("SELECT setval(pg_get_serial_sequence('qbit_share_ledger','share_seq'),$1)")
        .bind(value)
        .execute(pool)
        .await?;
    Ok(())
}

async fn bounds(pool: &PgPool, partition: &str) -> Result<(Option<i64>, i64)> {
    let row = sqlx::query(
        "SELECT lower_seq,upper_seq FROM qbit_prism_share_partitions WHERE partition_name=$1",
    )
    .bind(partition)
    .fetch_one(pool)
    .await?;
    Ok((row.try_get("lower_seq")?, row.try_get("upper_seq")?))
}

/// Fold every share into the permanent rollup tables, which is the third
/// condition of the online horizon.
async fn advance_rollups(pool: &PgPool) -> Result<i64> {
    loop {
        let progress = qbit_prism_server::rollups::advance(pool, 50_000).await?;
        if progress.scanned == 0 {
            return Ok(progress.last_share_seq);
        }
    }
}

/// Put the newest history, and with it the payout window, in `p1`, and leave
/// the rollup watermark past `p0`. `difficulty` must exceed the requested
/// window weight so the window stops inside `p1`.
async fn move_horizon_past_p0(pool: &PgPool) -> Result<i64> {
    let (_, p0_upper) = bounds(pool, P0).await?;
    insert_shares(pool, p0_upper, p0_upper + 4, 1_000_000, "server-b", 1.0).await?;
    set_sequence(pool, p0_upper + 4).await?;
    advance_rollups(pool).await
}

fn retention(days: i64) -> archive::PlanOptions {
    archive::PlanOptions {
        network_difficulty: "1".into(),
        retention_days: days,
        window_multiple: 4,
        check_duplicates: false,
    }
}

// ---------------------------------------------------------------------------
// Landing one block through the production path
// ---------------------------------------------------------------------------

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

/// The `candidate_with_bundle` recipe of `tests/audit_body_normalization.rs`.
fn candidate_with_bundle(
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
        job_id: "share-archive-job".into(),
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

/// One landed block and the artifact it was landed from.
struct Landed {
    block_hash: String,
    audit_bundle_sha256: String,
    canonical: Vec<u8>,
    logical: Value,
}

/// Append two shares into the first partition, land a block on the window
/// they form, and return the artifact the block committed to.
async fn land_block(ledger: &Ledger, nonce: u32) -> Result<Landed> {
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
    let audit_bundle_sha256 = hex::encode(Sha256::digest(&canonical));
    let logical = serde_json::to_value(&bundle)?;
    let candidate = candidate_with_bundle(
        &bundle,
        WindowRef::from_snapshot(&snapshot)?,
        snapshot.payout_revision,
        nonce,
    )?;
    let block_hash = candidate.block_hash.clone();
    ledger.enqueue_candidate(candidate).await?;
    let claim: CandidateClaim = ledger
        .claim_candidate(60)
        .await?
        .context("no pending candidate to claim")?;
    let claim = claim.with_bundle(bundle);
    ledger.land_candidate(&claim, &ledger_public_key()).await?;
    ledger.finish_candidate(&claim, true, None).await?;
    ledger.append(share(2), None).await?;
    Ok(Landed {
        block_hash,
        audit_bundle_sha256,
        canonical,
        logical,
    })
}

// ---------------------------------------------------------------------------
// Reading the archive back independently of the module that wrote it
// ---------------------------------------------------------------------------

struct ArchiveOnDisk {
    manifest: archive::ArchiveManifest,
    manifest_sha256: String,
    rows_gz_sha256: String,
    rows_sha256: String,
    lines: Vec<String>,
}

fn read_archive(root: &Path, partition: &str) -> Result<ArchiveOnDisk> {
    let directory = root.join("qbit_share_ledger").join(partition);
    let manifest_bytes = std::fs::read(directory.join("manifest.json"))?;
    let manifest: archive::ArchiveManifest = serde_json::from_slice(&manifest_bytes)?;
    let gz = std::fs::read(directory.join("rows.ndjson.gz"))?;
    let mut rows = Vec::new();
    flate2::read::MultiGzDecoder::new(gz.as_slice()).read_to_end(&mut rows)?;
    Ok(ArchiveOnDisk {
        manifest,
        manifest_sha256: hex::encode(Sha256::digest(&manifest_bytes)),
        rows_gz_sha256: hex::encode(Sha256::digest(&gz)),
        rows_sha256: hex::encode(Sha256::digest(&rows)),
        lines: String::from_utf8(rows)?
            .lines()
            .map(str::to_owned)
            .collect(),
    })
}

fn condition<'a>(plan: &'a archive::PartitionPlan, name: &str) -> &'a archive::Condition {
    plan.conditions
        .iter()
        .find(|condition| condition.name == name)
        .unwrap_or_else(|| panic!("plan has no {name} condition: {:?}", plan.conditions))
}

fn entry<'a>(report: &'a archive::PlanReport, partition: &str) -> &'a archive::PartitionPlan {
    report
        .partitions
        .iter()
        .find(|entry| entry.record.partition_name == partition)
        .unwrap_or_else(|| panic!("plan has no {partition} entry"))
}

async fn catalog(pool: &PgPool, partition: &str) -> Result<sqlx::postgres::PgRow> {
    Ok(sqlx::query(
        "SELECT state,sealed_at,archived_at,archive_verified_at,detached_at,dropped_at,archive_uri,archive_rows,archive_rows_sha256,archive_manifest_sha256 FROM qbit_prism_share_partitions WHERE partition_name=$1",
    )
    .bind(partition)
    .fetch_one(pool)
    .await?)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Archive format v1, round trip: the manifest's fields and both digests are
/// what the files on disk actually hold, the second archive chains to the
/// first, and a single flipped byte in `rows.ndjson.gz` fails `verify` by
/// name instead of being decompressed into something plausible.
#[tokio::test]
async fn archive_and_verify_round_trip_chain_and_tamper_detection() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let ledger = db.ledger("archive-a").await?;
        let root = tempfile::tempdir()?;
        let (p0_lower, p0_upper) = bounds(&ledger.pool, P0).await?;
        let (p1_lower, p1_upper) = bounds(&ledger.pool, P1).await?;
        let (p2_lower, p2_upper) = bounds(&ledger.pool, P2).await?;
        ensure!(p0_lower.is_none(), "the first partition is not MINVALUE");
        ensure!(
            p1_lower == Some(p0_upper) && p2_lower == Some(p1_upper),
            "the grid is not contiguous: {p1_lower:?} after {p0_upper}, {p2_lower:?} after {p1_upper}"
        );
        insert_shares(&ledger.pool, 1, 300, 7, "server-a", 3600.0).await?;
        insert_shares(&ledger.pool, p0_upper, p0_upper + 9, 5, "server-b", 60.0).await?;
        set_sequence(&ledger.pool, p0_upper + 9).await?;

        let written =
            archive::archive(&ledger, P0, root.path(), false, "operator-a").await?;
        let disk = read_archive(root.path(), P0)?;
        ensure!(
            disk.manifest.schema == archive::ARCHIVE_SCHEMA_V1,
            "unexpected archive schema {}",
            disk.manifest.schema
        );
        ensure!(
            disk.manifest.partition_name == P0
                && disk.manifest.lower_seq.is_none()
                && disk.manifest.upper_seq == p0_upper,
            "manifest bounds differ from the catalog: {:?}",
            disk.manifest
        );
        ensure!(
            disk.manifest.row_count == 300
                && disk.lines.len() == 300
                && disk.manifest.first_share_seq == Some(1)
                && disk.manifest.last_share_seq == Some(300),
            "manifest counts differ from the file: {:?}",
            disk.manifest
        );
        ensure!(
            disk.manifest.rows_sha256 == disk.rows_sha256
                && disk.manifest.rows_gz_sha256 == disk.rows_gz_sha256,
            "manifest digests differ from the file"
        );
        ensure!(
            disk.manifest.previous_manifest_sha256.is_none()
                && disk.manifest.previous_upper_seq.is_none(),
            "the first archived partition must not chain to anything"
        );
        ensure!(
            disk.manifest.schema_versions.contains(&16),
            "the manifest does not record the schema it was taken at: {:?}",
            disk.manifest.schema_versions
        );
        ensure!(
            written["archive_manifest_sha256"] == serde_json::json!(disk.manifest_sha256),
            "the recorded manifest digest is not the file's"
        );
        // The line is the design record's row, byte for byte: every column in
        // the record's key order, microsecond timestamps, difficulties as
        // decimal strings, the program as hex, and the exact values the ledger
        // holds. The expected text is built from the row read straight out of
        // PostgreSQL, so nothing here is copied from the encoder.
        let expected = sqlx::query(
            "SELECT share_id,miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,\
             share_difficulty::text AS difficulty,network_difficulty::text AS network,\
             template_height,job_id,\
             floor(extract(epoch FROM job_issued_at)*1000000)::bigint AS issued_us,\
             floor(extract(epoch FROM accepted_at)*1000000)::bigint AS accepted_us,\
             ntime,accepted,writer_id,writer_epoch \
             FROM qbit_share_ledger WHERE share_seq=1",
        )
        .fetch_one(&ledger.pool)
        .await?;
        let expected_line = format!(
            "{{\"share_seq\":1,\"share_id\":{},\"miner_id\":{},\"payout_order_key\":{},\"p2mr_program_hex\":{},\"share_difficulty\":{},\"network_difficulty\":{},\"template_height\":{},\"job_id\":{},\"job_issued_at_us\":{},\"accepted_at_us\":{},\"ntime\":{},\"accepted\":{},\"reject_reason\":null,\"credit_policy\":null,\"writer_id\":{},\"writer_epoch\":{}}}",
            serde_json::to_string(&expected.try_get::<String, _>("share_id")?)?,
            serde_json::to_string(&expected.try_get::<String, _>("miner_id")?)?,
            serde_json::to_string(&expected.try_get::<String, _>("payout_order_key")?)?,
            serde_json::to_string(&expected.try_get::<String, _>("program")?)?,
            serde_json::to_string(&expected.try_get::<String, _>("difficulty")?)?,
            serde_json::to_string(&expected.try_get::<String, _>("network")?)?,
            expected.try_get::<i64, _>("template_height")?,
            serde_json::to_string(&expected.try_get::<String, _>("job_id")?)?,
            expected.try_get::<i64, _>("issued_us")?,
            expected.try_get::<i64, _>("accepted_us")?,
            expected.try_get::<i64, _>("ntime")?,
            expected.try_get::<bool, _>("accepted")?,
            serde_json::to_string(&expected.try_get::<String, _>("writer_id")?)?,
            expected.try_get::<i64, _>("writer_epoch")?,
        );
        ensure!(
            disk.lines[0] == expected_line,
            "the archived row is not the v1 encoding of the ledger row:\n{}\n{expected_line}",
            disk.lines[0]
        );
        // Both nullable columns appear on every row, as null or as a value.
        ensure!(
            disk.lines
                .iter()
                .filter(|line| line.contains("\"accepted\":false,\"reject_reason\":\"stale-job\""))
                .count()
                == 27,
            "the archive did not carry every rejected share with its reason"
        );
        ensure!(
            disk.lines.iter().any(|line| line.contains("\"credit_policy\":\"stale-grace\""))
                && disk.lines.iter().any(|line| line.contains("\"credit_policy\":null")),
            "the archive did not carry both credit_policy shapes"
        );

        let verified = archive::verify(&ledger, P0, root.path()).await?;
        ensure!(
            verified["live_rows_compared"] == true
                && verified["live"]["rows_sha256"] == serde_json::json!(disk.rows_sha256)
                && verified["chain_previous_upper_seq"].is_null(),
            "verify did not compare the live rows: {verified}"
        );
        let row = catalog(&ledger.pool, P0).await?;
        ensure!(
            row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("archive_verified_at")?
                .is_some()
                && row.try_get::<Option<i64>, _>("archive_rows")? == Some(300)
                && row.try_get::<Option<String>, _>("archive_rows_sha256")?
                    == Some(disk.rows_sha256.clone()),
            "the catalog did not record the verified archive"
        );

        // A partition the sequence has not passed can still receive rows, so
        // neither an archive nor a verification of it can be complete.
        let error = archive::archive(&ledger, P1, root.path(), false, "operator-a")
            .await
            .expect_err("archived a partition the sequence is still inside")
            .to_string();
        ensure!(
            error.contains("share sequence stands at") && error.contains("still land in it"),
            "{error}"
        );
        set_sequence(&ledger.pool, p2_upper - 1).await?;

        // The chain stays contiguous: a partition whose nearest archived
        // predecessor does not end where it starts is refused until the
        // partitions between them are archived.
        let error = archive::archive(&ledger, P2, root.path(), false, "operator-a")
            .await
            .expect_err("archived past a partition that is not archived")
            .to_string();
        ensure!(
            error.contains("would not be adjacent")
                && error.contains(P0)
                && error.contains(&format!("ends at {p0_upper}"))
                && error.contains(&format!("starts at {p1_upper}")),
            "{error}"
        );

        // The chain: the second archive links to the first by manifest digest
        // and starts exactly where it ended.
        archive::archive(&ledger, P1, root.path(), false, "operator-a").await?;
        let next = read_archive(root.path(), P1)?;
        ensure!(
            next.manifest.previous_manifest_sha256 == Some(disk.manifest_sha256.clone())
                && next.manifest.previous_upper_seq == Some(p0_upper)
                && next.manifest.lower_seq == Some(p0_upper)
                && next.manifest.upper_seq == p1_upper
                && next.manifest.row_count == 10,
            "the second archive does not chain to the first: {:?}",
            next.manifest
        );
        ensure!(
            archive::chain_is_adjacent(next.manifest.previous_upper_seq, next.manifest.lower_seq),
            "the chain link is not adjacent"
        );
        let verified = archive::verify(&ledger, P1, root.path()).await?;
        ensure!(
            verified["chain_previous_upper_seq"] == serde_json::json!(p0_upper),
            "{verified}"
        );
        // A verification is refused the same way whenever the sequence is
        // found below the partition, whatever put it there.
        set_sequence(&ledger.pool, p0_upper + 9).await?;
        let error = archive::verify(&ledger, P1, root.path())
            .await
            .expect_err("verified a partition the sequence is inside")
            .to_string();
        ensure!(error.contains("share sequence stands at"), "{error}");
        set_sequence(&ledger.pool, p2_upper - 1).await?;

        // Re-archiving is refused without --force, and --force clears the
        // recorded verification: a new archive has not been verified.
        let error = archive::archive(&ledger, P0, root.path(), false, "operator-a")
            .await
            .expect_err("re-archived without --force")
            .to_string();
        ensure!(error.contains("--force"), "{error}");
        let rewritten = archive::archive(&ledger, P0, root.path(), true, "operator-a").await?;
        ensure!(
            catalog(&ledger.pool, P0)
                .await?
                .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("archive_verified_at")?
                .is_none(),
            "--force kept the verification of the archive it replaced"
        );
        // The second manifest links to the digest that was just replaced, so
        // its verification goes with the first's, and it cannot be certified
        // again until it is written again.
        ensure!(
            rewritten["verification_cleared"] == serde_json::json!([P1]),
            "{rewritten}"
        );
        ensure!(
            catalog(&ledger.pool, P1)
                .await?
                .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("archive_verified_at")?
                .is_none(),
            "--force kept the verification of an archive chained to the one it replaced"
        );
        let error = archive::verify(&ledger, P1, root.path())
            .await
            .expect_err("certified a manifest chained to a replaced one")
            .to_string();
        ensure!(error.contains("chains to"), "{error}");
        archive::verify(&ledger, P0, root.path()).await?;
        // Written again in order, the chain is whole again.
        archive::archive(&ledger, P1, root.path(), true, "operator-a").await?;
        ensure!(
            read_archive(root.path(), P1)?.manifest.previous_manifest_sha256
                == Some(read_archive(root.path(), P0)?.manifest_sha256),
            "the second archive was not relinked to the rewritten first"
        );
        archive::verify(&ledger, P1, root.path()).await?;
        // Once a later archive has left the ledger it cannot be written again
        // to follow a new manifest, so the manifest it chains to is fixed.
        sqlx::raw_sql(&format!(
            "ALTER TABLE qbit_share_ledger DETACH PARTITION {P1} CONCURRENTLY"
        ))
        .execute(&ledger.pool)
        .await?;
        let reconciled = archive::detach(&ledger, P1, &retention(0)).await?;
        ensure!(reconciled["action"] == "reconciled", "{reconciled}");
        let error = archive::archive(&ledger, P0, root.path(), true, "operator-a")
            .await
            .expect_err("rewrote a manifest a detached partition chains to")
            .to_string();
        ensure!(
            error.contains("have left the ledger") && error.contains(P1),
            "{error}"
        );
        ensure!(
            catalog(&ledger.pool, P0)
                .await?
                .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("archive_verified_at")?
                .is_some(),
            "the refused rewrite cleared the verification it left in place"
        );

        // Tamper: one byte of the compressed file.
        let rows_path = root.path().join("qbit_share_ledger").join(P0).join("rows.ndjson.gz");
        let mut bytes = std::fs::read(&rows_path)?;
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0x40;
        std::fs::write(&rows_path, &bytes)?;
        let error = archive::verify(&ledger, P0, root.path())
            .await
            .expect_err("verify accepted a tampered archive")
            .to_string();
        ensure!(
            error.contains("rows_gz_sha256") && error.contains("altered"),
            "verify did not name the digest that differed: {error}"
        );
        Ok(ledger)
    }
    .await;
    match result {
        Ok(ledger) => db.close(vec![ledger]).await,
        Err(error) => {
            db.close(Vec::new()).await?;
            Err(error)
        }
    }
}

/// `plan` names every blocker of decision D6, and distinguishes an input it
/// does not have from a condition that is clear.
#[tokio::test]
async fn plan_names_each_blocker_and_separates_unknown_from_clear() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let ledger = db.ledger("plan-a").await?;
        let landed = land_block(&ledger, 1441).await?;
        insert_shares(&ledger.pool, 3, 120, 7, "server-a", 30.0).await?;
        set_sequence(&ledger.pool, 120).await?;

        // No rollup sweep has ever run: the watermark is unknown, not clear.
        let report = archive::plan(&ledger, &retention(30)).await?;
        let p0 = entry(&report, P0);
        ensure!(
            report
                .unknowns
                .iter()
                .any(|line| line.contains("qbit_hashrate_rollup_progress")),
            "a missing rollup watermark was not reported: {:?}",
            report.unknowns
        );
        let rollup = condition(p0, "rollup_watermark");
        ensure!(
            rollup.status == "unknown" && rollup.detail.contains("never run"),
            "a missing watermark was not unknown: {rollup:?}"
        );
        ensure!(report.rollup_last_share_seq.is_none(), "{report:?}");
        ensure!(!p0.eligible && report.eligible.is_empty(), "{p0:?}");
        ensure!(
            p0.live_rows == Some(120) && p0.newest_accepted_at.is_some(),
            "plan did not count the live rows: {p0:?}"
        );
        ensure!(
            report.attached_count == 5 && report.lead_rows_ahead.is_some(),
            "plan did not report the attached set and the lead: {report:?}"
        );
        // The command prints this report, so the JSON encoding is part of the
        // contract: one object per partition plus the summary, scriptable.
        let encoded = serde_json::to_string(&report)?;
        ensure!(
            !encoded.contains("serde_json::private"),
            "the plan report does not encode as plain JSON: {encoded}"
        );
        let printed: Value = serde_json::from_str(&encoded)?;
        let object = &printed["partitions"][0];
        ensure!(
            printed["schema"] == "qbit.prism.share-archive-plan.v1"
                && printed["partitions"].as_array().map(Vec::len) == Some(5)
                && object["partition_name"].is_string()
                && object["upper_seq"].is_number()
                && object["attached"] == true
                && object["conditions"].as_array().map(Vec::len) == Some(5)
                && object["conditions"][0]["status"].is_string(),
            "the printed plan is not one object per partition plus a summary: {printed}"
        );

        let window = condition(p0, "payout_window");
        ensure!(
            window.status == "blocked" && window.detail.contains("still reaches share_seq"),
            "the window floor was not a blocker: {window:?}"
        );
        let age = condition(p0, "retention_age");
        ensure!(
            age.status == "blocked" && age.detail.contains("inside the 30 day retention age"),
            "the retention age was not a blocker: {age:?}"
        );
        let sealed = condition(p0, "audits_sealed");
        ensure!(
            sealed.status == "blocked"
                && sealed.detail.contains("canonical_audit_bytes")
                && sealed.detail.contains("share-archive seal"),
            "the unsealed audit was not a blocker: {sealed:?}"
        );
        let references = condition(p0, "pending_references");
        ensure!(
            references.status == "clear",
            "the landed block's outbox row is terminal and must not block: {references:?}"
        );

        // A watermark that exists but has not reached the partition is a
        // blocker with its position named, not an unknown.
        sqlx::query(
            "INSERT INTO qbit_hashrate_rollup_progress(singleton,last_share_seq) VALUES(true,5)",
        )
        .execute(&ledger.pool)
        .await?;
        let report = archive::plan(&ledger, &retention(30)).await?;
        let rollup = condition(entry(&report, P0), "rollup_watermark");
        ensure!(
            rollup.status == "blocked" && rollup.detail.contains("folded share_seq up to 5"),
            "a watermark behind the partition was not a blocker: {rollup:?}"
        );

        // An unfinished block candidate that names a share in the partition
        // pins it. The solve path writes share_id with the candidate; this
        // fixture enqueues a block-only candidate and attaches the share_id
        // the same row would carry.
        let snapshot = ledger.snapshot(100).await?;
        let (coinbase_key, ledger_key) = keys();
        let bundle = build_audit_bundle(
            snapshot.shares.clone(),
            FoundBlock {
                block_height: 102,
                coinbase_value_sats: 500_000_000,
                network_difficulty: 100,
                anchor_job_issued_at_ms: snapshot.anchor_ms,
            },
            snapshot.prior_balances.clone(),
            PayoutPolicy::day_one_default(),
            &coinbase_key,
            &ledger_key,
        )?;
        let pending = candidate_with_bundle(
            &bundle,
            WindowRef::from_snapshot(&snapshot)?,
            snapshot.payout_revision,
            1442,
        )?;
        let pending_hash = pending.block_hash.clone();
        ledger.enqueue_candidate(pending).await?;
        let share_id: String =
            sqlx::query_scalar("SELECT share_id FROM qbit_share_ledger WHERE share_seq=60")
                .fetch_one(&ledger.pool)
                .await?;
        sqlx::query("UPDATE qbit_block_candidate_outbox SET share_id=$2 WHERE block_hash=$1")
            .bind(&pending_hash)
            .bind(&share_id)
            .execute(&ledger.pool)
            .await?;
        let report = archive::plan(&ledger, &retention(30)).await?;
        let references = condition(entry(&report, P0), "pending_references");
        ensure!(
            references.status == "blocked"
                && references.detail.contains("1 unfinished block candidate"),
            "an unfinished outbox row did not pin the partition: {references:?}"
        );

        // Sealing clears exactly one of the five and leaves the rest.
        archive::seal(&ledger, P0).await?;
        let report = archive::plan(&ledger, &retention(30)).await?;
        let p0 = entry(&report, P0);
        ensure!(
            condition(p0, "audits_sealed").status == "clear",
            "sealing did not clear the audit condition: {p0:?}"
        );
        ensure!(
            p0.blockers.len() == 4 && !p0.eligible,
            "sealing cleared more or fewer conditions than its own: {:?}",
            p0.blockers
        );
        ensure!(
            landed.audit_bundle_sha256.len() == 64,
            "the landed artifact has no digest"
        );
        Ok(ledger)
    }
    .await;
    match result {
        Ok(ledger) => db.close(vec![ledger]).await,
        Err(error) => {
            db.close(Vec::new()).await?;
            Err(error)
        }
    }
}

#[tokio::test]
async fn unfinished_candidate_window_pins_an_earlier_partition() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let ledger = db.ledger("window-pin-a").await?;
        insert_shares(&ledger.pool, 1, 120, 7, "server-a", 7200.0).await?;
        set_sequence(&ledger.pool, 120).await?;
        let snapshot = ledger.snapshot(100).await?;
        let (coinbase_key, ledger_key) = keys();
        let bundle = build_audit_bundle(
            snapshot.shares.clone(),
            FoundBlock {
                block_height: 102,
                coinbase_value_sats: 500_000_000,
                network_difficulty: 100,
                anchor_job_issued_at_ms: snapshot.anchor_ms,
            },
            snapshot.prior_balances.clone(),
            PayoutPolicy::day_one_default(),
            &coinbase_key,
            &ledger_key,
        )?;
        let pending = candidate_with_bundle(
            &bundle,
            WindowRef::from_snapshot(&snapshot)?,
            snapshot.payout_revision,
            1443,
        )?;
        let pending_hash = pending.block_hash.clone();
        ledger.enqueue_candidate(pending).await?;
        move_horizon_past_p0(&ledger.pool).await?;
        let (_, p0_upper) = bounds(&ledger.pool, P0).await?;
        // The solving share belongs to p1, but recovery still reads the
        // persisted payout window from p0 after the online horizon moves on.
        sqlx::query("UPDATE qbit_block_candidate_outbox SET share_id=(SELECT share_id FROM qbit_share_ledger WHERE share_seq=$2) WHERE block_hash=$1")
            .bind(&pending_hash)
            .bind(p0_upper)
            .execute(&ledger.pool)
            .await?;
        let report = archive::plan(&ledger, &retention(0)).await?;
        let p0 = entry(&report, P0);
        let references = condition(p0, "pending_references");
        ensure!(
            references.status == "blocked"
                && references.detail.contains("1 unfinished block candidate")
                && p0.blockers.len() == 1,
            "the candidate's earlier payout window did not pin p0: {p0:?}"
        );

        // Move the whole window without changing its count. With neither
        // the solving share nor the window in p0, this candidate releases it.
        sqlx::query("UPDATE qbit_block_candidate_outbox SET window_first_share_seq=window_first_share_seq+$2,window_last_share_seq=window_last_share_seq+$2 WHERE block_hash=$1")
            .bind(&pending_hash)
            .bind(p0_upper)
            .execute(&ledger.pool)
            .await?;
        let report = archive::plan(&ledger, &retention(0)).await?;
        let p0 = entry(&report, P0);
        ensure!(
            condition(p0, "pending_references").status == "clear" && p0.eligible,
            "a non-overlapping candidate window still pinned p0: {p0:?}"
        );
        Ok(ledger)
    }
    .await;
    match result {
        Ok(ledger) => db.close(vec![ledger]).await,
        Err(error) => {
            db.close(Vec::new()).await?;
            Err(error)
        }
    }
}

#[tokio::test]
async fn verification_refuses_a_concurrent_archive_rewrite() -> Result<()> {
    // Replacing either the predecessor or the archive being verified must
    // keep the stale verifier from restoring the invalidated timestamp.
    for rewritten in [P0, P1] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let result = async {
            let ledger = db.ledger("verify-race-a").await?;
            let verifier = db.ledger("verify-race-b").await?;
            let root = tempfile::tempdir()?;
            insert_shares(&ledger.pool, 1, 250, 7, "server-a", 7200.0).await?;
            let (_, p0_upper) = bounds(&ledger.pool, P0).await?;
            let (_, p1_upper) = bounds(&ledger.pool, P1).await?;
            insert_shares(&ledger.pool, p0_upper, p0_upper + 4, 7, "server-a", 7200.0)
                .await?;
            set_sequence(&ledger.pool, p1_upper).await?;
            for partition in [P0, P1] {
                archive::archive(&ledger, partition, root.path(), false, "operator-a").await?;
                archive::verify(&ledger, partition, root.path()).await?;
            }

            // Hold the catalog changes an archive rewrite makes uncommitted,
            // so the verifier first reads the old manifest and chain link.
            let mut writer = ledger.pool.begin().await?;
            let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *writer)
                .await?;
            sqlx::query("UPDATE qbit_prism_share_partitions SET archive_manifest_sha256=$2 WHERE partition_name=$1")
                .bind(rewritten)
                .bind("ab".repeat(32))
                .execute(&mut *writer)
                .await?;
            sqlx::query("UPDATE qbit_prism_share_partitions SET archive_verified_at=NULL WHERE partition_name=$1")
                .bind(P1)
                .execute(&mut *writer)
                .await?;
            let verification = tokio::spawn({
                let root = root.path().to_path_buf();
                async move {
                    let result = archive::verify(&verifier, P1, &root).await;
                    (verifier, result)
                }
            });
            // Observe the actual lock wait instead of relying on scheduling
            // the verifier within an arbitrary sleep.
            let waiting = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))")
                        .bind(writer_pid)
                        .fetch_one(&db.admin)
                        .await?;
                    if blocked {
                        return Ok::<(), anyhow::Error>(());
                    }
                    ensure!(!verification.is_finished(), "verification did not wait for the rewrite of {rewritten}");
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await;
            writer.commit().await?;
            let (verifier, verified) = verification.await?;
            waiting.context("verification never waited for the catalog rewrite")??;
            let error = verified
                .expect_err("a stale verification survived an archive rewrite")
                .to_string();
            ensure!(error.contains("rewritten during verification"), "{error}");
            ensure!(
                catalog(&ledger.pool, P1)
                    .await?
                    .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("archive_verified_at")?
                    .is_none(),
                "verification restored the timestamp invalidated by rewriting {rewritten}"
            );
            Ok(vec![ledger, verifier])
        }
        .await;
        match result {
            Ok(ledgers) => db.close(ledgers).await?,
            Err(error) => {
                db.close(Vec::new()).await?;
                return Err(error);
            }
        }
    }
    Ok(())
}

/// Acceptance criterion 5 of #144: a block whose payout window lay inside a
/// partition keeps serving its advertised artifact after that partition has
/// been sealed, archived, verified, detached and dropped. Nothing can rebuild
/// it from the ledger any more; the stored canonical bytes are what is served,
/// through the ledger API, the operator tool and the public artifact route.
#[tokio::test]
async fn an_archived_blocks_audit_is_served_from_its_sealed_bytes() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let ledger = db.ledger("sealed-a").await?;
        let root = tempfile::tempdir()?;
        let landed = land_block(&ledger, 1445).await?;
        let (_, p0_upper) = bounds(&ledger.pool, P0).await?;
        let watermark = move_horizon_past_p0(&ledger.pool).await?;
        ensure!(watermark >= p0_upper - 1, "the rollup sweep stopped at {watermark}");

        // Everything but the seal is clear, and the seal is named.
        let report = archive::plan(&ledger, &retention(0)).await?;
        let p0 = entry(&report, P0);
        ensure!(
            p0.blockers.len() == 1 && condition(p0, "audits_sealed").status == "blocked",
            "unexpected blockers before the seal: {:?}",
            p0.blockers
        );
        let error = archive::detach(&ledger, P0, &retention(0))
            .await
            .expect_err("detached an unsealed partition")
            .to_string();
        ensure!(error.contains("seal"), "{error}");

        let sealed = archive::seal(&ledger, P0).await?;
        ensure!(
            sealed["sealed_now"] == 1
                && sealed["unsealed_remaining"] == 0
                && sealed["sealed_bytes"].as_i64().unwrap_or(0) > 0,
            "the seal did not store the artifact: {sealed}"
        );
        let report = archive::plan(&ledger, &retention(0)).await?;
        ensure!(
            report.eligible.contains(&P0.to_owned()),
            "the partition is still blocked: {:?}",
            entry(&report, P0)
        );

        archive::archive(&ledger, P0, root.path(), false, "operator-a").await?;
        archive::verify(&ledger, P0, root.path()).await?;
        let detached = archive::detach(&ledger, P0, &retention(0)).await?;
        ensure!(detached["action"] == "detached", "{detached}");
        let attached: Vec<String> = sqlx::query_scalar(
            "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid WHERE i.inhparent=to_regclass('qbit_share_ledger') ORDER BY 1",
        )
        .fetch_all(&ledger.pool)
        .await?;
        ensure!(!attached.contains(&P0.to_owned()), "{P0} is still attached");
        let dropped = archive::drop_partition(&ledger, P0).await?;
        ensure!(dropped["relation_dropped"] == true, "{dropped}");
        let present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(P0)
            .fetch_one(&ledger.pool)
            .await?;
        ensure!(!present, "{P0} still exists after the drop");
        let row = catalog(&ledger.pool, P0).await?;
        ensure!(
            row.try_get::<String, _>("state")? == "dropped"
                && row
                    .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("dropped_at")?
                    .is_some(),
            "the catalog does not record the drop"
        );

        // The shares the block paid on are gone; the snapshot metadata stays.
        let remaining: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_seq<$1")
                .bind(p0_upper)
                .fetch_one(&ledger.pool)
                .await?;
        ensure!(remaining == 0, "{remaining} archived shares are still online");
        let snapshots: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_prism_audit_snapshots")
                .fetch_one(&ledger.pool)
                .await?;
        ensure!(snapshots >= 1, "the snapshot metadata row was removed");

        // Every reader still serves the block, with its advertised digest.
        let served = audit_canonical_bytes(&ledger.pool, &landed.block_hash)
            .await?
            .context("the archived block serves no canonical bytes")?;
        ensure!(
            served == landed.canonical,
            "the served canonical bytes differ from the artifact the block committed to"
        );
        ensure!(
            hex::encode(Sha256::digest(&served)) == landed.audit_bundle_sha256,
            "the served bytes do not hash to the advertised audit_bundle_sha256"
        );
        let hydrated = ledger
            .audit_bundle(&landed.block_hash)
            .await?
            .context("the archived block serves no logical body")?;
        ensure!(
            hydrated == landed.logical,
            "the hydrated body differs from the artifact the block committed to"
        );
        audit_completeness(&ledger.pool).await?.require_complete()?;

        // The public API: the artifact route serves the exact bytes and the
        // bundle route serves the same body.
        let (app, rpc) = api(&ledger.pool).await?;
        let (status, bytes) = raw(
            &app,
            &format!("/public/v1/artifacts/{}", landed.audit_bundle_sha256),
        )
        .await;
        ensure!(status == StatusCode::OK, "artifact route: {status}");
        ensure!(
            bytes == landed.canonical,
            "the artifact route served {} bytes, not the {} the block committed to",
            bytes.len(),
            landed.canonical.len()
        );
        let (status, bundle) = json(
            &app,
            &format!("/audit/blocks/{}/bundle", landed.block_hash),
        )
        .await;
        ensure!(status == StatusCode::OK, "bundle route: {status} {bundle}");
        ensure!(
            bundle["audit_bundle"] == landed.logical
                && bundle["audit_bundle_sha256"] == serde_json::json!(landed.audit_bundle_sha256),
            "the bundle route served a different body"
        );
        rpc.abort();
        Ok(ledger)
    }
    .await;
    match result {
        Ok(ledger) => db.close(vec![ledger]).await,
        Err(error) => {
            db.close(Vec::new()).await?;
            Err(error)
        }
    }
}

/// A restore rebuilds the partition byte for byte, with the shape the ledger
/// would have created, and `--attach` puts it back in front of every reader.
#[tokio::test]
async fn restore_rebuilds_the_partition_and_attach_returns_it_to_the_parent() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let ledger = db.ledger("restore-a").await?;
        let root = tempfile::tempdir()?;
        insert_shares(&ledger.pool, 1, 250, 7, "server-a", 7200.0).await?;
        move_horizon_past_p0(&ledger.pool).await?;
        archive::archive(&ledger, P0, root.path(), false, "operator-a").await?;
        archive::verify(&ledger, P0, root.path()).await?;
        archive::seal(&ledger, P0).await?;
        // EP-ERRORS: a run whose DETACH succeeded and whose catalog update did
        // not is repaired by the next run, which reads pg_inherits as the
        // truth rather than detaching a second time.
        sqlx::raw_sql(&format!(
            "ALTER TABLE qbit_share_ledger DETACH PARTITION {P0} CONCURRENTLY"
        ))
        .execute(&ledger.pool)
        .await?;
        let reconciled = archive::detach(&ledger, P0, &retention(0)).await?;
        ensure!(
            reconciled["action"] == "reconciled",
            "an interrupted detach was not reconciled: {reconciled}"
        );
        ensure!(
            catalog(&ledger.pool, P0).await?.try_get::<String, _>("state")? == "detached",
            "the catalog was not brought in line with pg_inherits"
        );
        archive::drop_partition(&ledger, P0).await?;
        let disk = read_archive(root.path(), P0)?;
        let manifest_path: PathBuf = root
            .path()
            .join("qbit_share_ledger")
            .join(P0)
            .join("manifest.json");

        // Restored, not attached: the same rows, the same digest, the shape
        // qbit_prism_share_partition_create builds.
        let restored = archive::restore(&ledger, &manifest_path, root.path(), false).await?;
        ensure!(
            restored["row_count"] == 250
                && restored["rows_sha256"] == serde_json::json!(disk.rows_sha256),
            "the restored partition does not re-stream to the archived digest: {restored}"
        );
        let shape = sqlx::query(
            "SELECT (SELECT count(*) FROM pg_index WHERE indrelid=to_regclass($1))::bigint AS indexes,\
             (SELECT count(*) FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal)::bigint AS triggers,\
             (SELECT count(*) FROM pg_constraint WHERE conrelid=to_regclass($1) AND conname=$1||'_bound')::bigint AS bound,\
             (SELECT count(*) FROM pg_constraint WHERE conrelid=to_regclass($1) AND conname=$1||'_share_id_key')::bigint AS unique_share_id,\
             (SELECT count(*) FROM pg_inherits WHERE inhrelid=to_regclass($1))::bigint AS parents",
        )
        .bind(P0)
        .fetch_one(&ledger.pool)
        .await?;
        ensure!(
            shape.try_get::<i64, _>("indexes")? == 6,
            "the restored partition has {} indexes, not the release set of six",
            shape.try_get::<i64, _>("indexes")?
        );
        ensure!(
            shape.try_get::<i64, _>("triggers")? == 1
                && shape.try_get::<i64, _>("bound")? == 1
                && shape.try_get::<i64, _>("unique_share_id")? == 1,
            "the restored partition is missing its trigger, bound or share_id uniqueness"
        );
        ensure!(
            shape.try_get::<i64, _>("parents")? == 0,
            "the restore attached the partition without being asked to"
        );
        // Nothing that lives in the parent sees it yet.
        let online: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_seq<=250")
                .fetch_one(&ledger.pool)
                .await?;
        ensure!(online == 0, "an unattached restore is visible through the parent");
        // The immutability trigger is live on the restored table.
        let error = sqlx::query(&format!("DELETE FROM {P0} WHERE share_seq=1"))
            .execute(&ledger.pool)
            .await
            .expect_err("the restored partition accepted a DELETE")
            .to_string();
        ensure!(error.contains("immutable"), "{error}");
        // A second restore refuses rather than adopting the relation.
        let error = archive::restore(&ledger, &manifest_path, root.path(), false)
            .await
            .expect_err("restored over an existing relation")
            .to_string();
        ensure!(error.contains("already holds that name"), "{error}");

        sqlx::raw_sql(&format!("DROP TABLE {P0}"))
            .execute(&ledger.pool)
            .await?;
        // A self-consistent archive with matching bounds is not sufficient:
        // only the catalog's copy of record may return a departed partition.
        sqlx::query("UPDATE qbit_prism_share_partitions SET archive_manifest_sha256=$2 WHERE partition_name=$1")
            .bind(P0)
            .bind("ab".repeat(32))
            .execute(&ledger.pool)
            .await?;
        let error = archive::restore(&ledger, &manifest_path, root.path(), true)
            .await
            .expect_err("re-attached an archive other than the catalog's copy of record")
            .to_string();
        ensure!(error.contains("catalog records another archive as the copy of record"), "{error}");
        let absent: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NULL")
            .bind(P0)
            .fetch_one(&ledger.pool)
            .await?;
        ensure!(absent, "a refused re-attach did not roll back the restored table");
        let row = catalog(&ledger.pool, P0).await?;
        ensure!(
            row.try_get::<String, _>("state")? == "dropped"
                && row.try_get::<String, _>("archive_manifest_sha256")? == "ab".repeat(32),
            "a refused re-attach changed the catalog"
        );
        sqlx::query("UPDATE qbit_prism_share_partitions SET archive_manifest_sha256=$2 WHERE partition_name=$1")
            .bind(P0)
            .bind(&disk.manifest_sha256)
            .execute(&ledger.pool)
            .await?;
        let restored = archive::restore(&ledger, &manifest_path, root.path(), true).await?;
        ensure!(restored["attached"] == true, "{restored}");
        let online: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_seq<=250")
                .fetch_one(&ledger.pool)
                .await?;
        ensure!(online == 250, "the re-attached partition holds {online} rows, not 250");
        let oldest: Option<i64> = sqlx::query_scalar(
            "SELECT min(share_seq) FROM qbit_prism_window(clock_timestamp(),1000000000::numeric)",
        )
        .fetch_one(&ledger.pool)
        .await?;
        ensure!(
            oldest == Some(1),
            "the payout window does not reach the re-attached rows: {oldest:?}"
        );
        let range: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM qbit_share_ledger WHERE share_seq BETWEEN 100 AND 200",
        )
        .fetch_one(&ledger.pool)
        .await?;
        ensure!(range == 101, "a range read returned {range} rows, not 101");
        let row = catalog(&ledger.pool, P0).await?;
        ensure!(
            row.try_get::<String, _>("state")? == "attached"
                && row
                    .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("detached_at")?
                    .is_none()
                && row
                    .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("dropped_at")?
                    .is_none(),
            "the catalog was not returned to the attached state"
        );
        // The restore never moves the sequence live appends draw from.
        let (_, p0_upper) = bounds(&ledger.pool, P0).await?;
        let next: i64 = sqlx::query_scalar("SELECT qbit_prism_share_next_seq()")
            .fetch_one(&ledger.pool)
            .await?;
        ensure!(
            next == p0_upper + 5,
            "the restore moved the share sequence to {next}"
        );
        Ok(ledger)
    }
    .await;
    match result {
        Ok(ledger) => db.close(vec![ledger]).await,
        Err(error) => {
            db.close(Vec::new()).await?;
            Err(error)
        }
    }
}

/// The verification is a proof at the instant it was taken. A row that lands
/// after it, whether an append that committed late or one written into the
/// standalone relation by name, is caught by the count the detach and the
/// drop each take against the archive, so no row leaves the ledger unarchived.
#[tokio::test]
async fn detach_and_drop_refuse_a_partition_that_outgrew_its_verified_archive() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let ledger = db.ledger("recount-a").await?;
        let root = tempfile::tempdir()?;
        insert_shares(&ledger.pool, 1, 40, 7, "server-a", 7200.0).await?;
        move_horizon_past_p0(&ledger.pool).await?;
        archive::seal(&ledger, P0).await?;
        archive::archive(&ledger, P0, root.path(), false, "operator-a").await?;
        archive::verify(&ledger, P0, root.path()).await?;
        let report = archive::plan(&ledger, &retention(0)).await?;
        ensure!(entry(&report, P0).eligible, "{:?}", entry(&report, P0));

        // An append that drew its share_seq before the sequence passed the
        // partition and committed only after the comparison.
        insert_shares(&ledger.pool, 41, 41, 7, "server-a", 7200.0).await?;
        let error = archive::detach(&ledger, P0, &retention(0))
            .await
            .expect_err("detached a partition holding a row its archive does not")
            .to_string();
        ensure!(
            error.contains("41 live rows") && error.contains("records 40"),
            "{error}"
        );
        let attached: Option<bool> = sqlx::query_scalar(
            "SELECT true FROM pg_inherits i WHERE i.inhrelid=to_regclass($1) AND i.inhparent=to_regclass('qbit_share_ledger')",
        )
        .bind(P0)
        .fetch_optional(&ledger.pool)
        .await?;
        ensure!(
            attached == Some(true)
                && catalog(&ledger.pool, P0).await?.try_get::<String, _>("state")? == "attached",
            "the refused detach changed something"
        );
        // Archiving again, and verifying again, is the recovery.
        archive::archive(&ledger, P0, root.path(), true, "operator-a").await?;
        archive::verify(&ledger, P0, root.path()).await?;
        let detached = archive::detach(&ledger, P0, &retention(0)).await?;
        ensure!(detached["action"] == "detached", "{detached}");

        // A row written into the standalone relation by name, between the
        // detach and the drop, is the last thing the drop checks for.
        insert_shares_into(&ledger.pool, P0, 42, 42, 7, "server-a", 7200.0).await?;
        let error = archive::drop_partition(&ledger, P0)
            .await
            .expect_err("dropped a relation holding a row its archive does not")
            .to_string();
        ensure!(
            error.contains("holds 42 rows") && error.contains("records 41"),
            "{error}"
        );
        let present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(P0)
            .fetch_one(&ledger.pool)
            .await?;
        ensure!(
            present && catalog(&ledger.pool, P0).await?.try_get::<String, _>("state")? == "detached",
            "the refused drop changed something"
        );
        Ok(ledger)
    }
    .await;
    match result {
        Ok(ledger) => db.close(vec![ledger]).await,
        Err(error) => {
            db.close(Vec::new()).await?;
            Err(error)
        }
    }
}

/// EP-ERRORS: a `DETACH PARTITION ... CONCURRENTLY` that was interrupted
/// after PostgreSQL marked the partition detach-pending is finished with
/// FINALIZE, never restarted, and the catalog records it once it is out.
#[tokio::test]
async fn an_interrupted_concurrent_detach_is_finalized() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let ledger = db.ledger("finalize-a").await?;
        let root = tempfile::tempdir()?;
        insert_shares(&ledger.pool, 1, 40, 7, "server-a", 7200.0).await?;
        move_horizon_past_p0(&ledger.pool).await?;
        archive::archive(&ledger, P0, root.path(), false, "operator-a").await?;
        archive::verify(&ledger, P0, root.path()).await?;
        archive::seal(&ledger, P0).await?;

        // A reader holding a snapshot of the parent stops the second phase of
        // the concurrent detach; the first phase has already committed the
        // detach-pending mark, so cancelling here leaves exactly the state an
        // interrupted operator run leaves behind.
        let mut reader = ledger.pool.begin().await?;
        sqlx::query("SELECT count(*) FROM qbit_share_ledger")
            .fetch_one(&mut *reader)
            .await?;
        let mut ddl = ledger.pool.acquire().await?;
        sqlx::query("SELECT set_config('statement_timeout','2000',false)")
            .execute(&mut *ddl)
            .await?;
        let interrupted = sqlx::raw_sql(&format!(
            "ALTER TABLE qbit_share_ledger DETACH PARTITION {P0} CONCURRENTLY"
        ))
        .execute(&mut *ddl)
        .await;
        ensure!(
            interrupted.is_err(),
            "the concurrent detach was not interrupted by the open reader"
        );
        drop(ddl);
        reader.rollback().await?;
        let pending: Option<bool> = sqlx::query_scalar(
            "SELECT i.inhdetachpending FROM pg_inherits i WHERE i.inhrelid=to_regclass($1) AND i.inhparent=to_regclass('qbit_share_ledger')",
        )
        .bind(P0)
        .fetch_optional(&ledger.pool)
        .await?
        .flatten();
        ensure!(
            pending == Some(true),
            "the interrupted detach did not leave the partition detach-pending: {pending:?}"
        );

        // plan reports the interrupted attempt as an unknown, and detach
        // finalizes it instead of issuing a second CONCURRENTLY.
        let report = archive::plan(&ledger, &retention(0)).await?;
        ensure!(
            entry(&report, P0)
                .unknowns
                .iter()
                .any(|line| line.contains("inhdetachpending")),
            "plan did not report the interrupted detach: {:?}",
            entry(&report, P0).unknowns
        );
        let finalized = archive::detach(&ledger, P0, &retention(0)).await?;
        ensure!(
            finalized["action"] == "finalized"
                && finalized["statement"]
                    .as_str()
                    .is_some_and(|statement| statement.ends_with("FINALIZE")),
            "the interrupted detach was not finalized: {finalized}"
        );
        let row = catalog(&ledger.pool, P0).await?;
        ensure!(
            row.try_get::<String, _>("state")? == "detached",
            "the catalog was not updated after the finalize"
        );
        let still: Option<bool> = sqlx::query_scalar(
            "SELECT true FROM pg_inherits i WHERE i.inhrelid=to_regclass($1) AND i.inhparent=to_regclass('qbit_share_ledger')",
        )
        .bind(P0)
        .fetch_optional(&ledger.pool)
        .await?;
        ensure!(still.is_none(), "{P0} is still a partition after the finalize");
        Ok(ledger)
    }
    .await;
    match result {
        Ok(ledger) => db.close(vec![ledger]).await,
        Err(error) => {
            db.close(Vec::new()).await?;
            Err(error)
        }
    }
}

/// Sealing is idempotent and safe to run twice at once: each artifact is
/// stored exactly once, whichever run gets there first, and `sealed_at` is
/// recorded once and not moved.
#[tokio::test]
async fn seal_is_idempotent_and_safe_to_run_concurrently() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let ledger = db.ledger("seal-a").await?;
        land_block(&ledger, 1447).await?;
        land_block(&ledger, 1448).await?;
        let (first, second) = tokio::join!(archive::seal(&ledger, P0), archive::seal(&ledger, P0));
        let (first, second) = (first?, second?);
        let stored = first["sealed_now"].as_i64().unwrap_or(-1)
            + second["sealed_now"].as_i64().unwrap_or(-1);
        ensure!(
            stored == 2,
            "two concurrent seals stored {stored} artifacts, not two: {first} {second}"
        );
        ensure!(
            first["unsealed_remaining"] == 0 && second["unsealed_remaining"] == 0,
            "a concurrent seal left work behind: {first} {second}"
        );
        let sealed_at: Option<chrono::DateTime<chrono::Utc>> =
            catalog(&ledger.pool, P0).await?.try_get("sealed_at")?;
        let sealed_at = sealed_at.context("sealed_at was not recorded")?;

        let again = archive::seal(&ledger, P0).await?;
        ensure!(
            again["sealed_now"] == 0 && again["unsealed_remaining"] == 0,
            "a repeated seal did work: {again}"
        );
        let after: Option<chrono::DateTime<chrono::Utc>> =
            catalog(&ledger.pool, P0).await?.try_get("sealed_at")?;
        ensure!(
            after == Some(sealed_at),
            "a repeated seal moved sealed_at from {sealed_at} to {after:?}"
        );
        let bytes: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM qbit_pool_audit_bundles WHERE canonical_audit_bytes IS NOT NULL",
        )
        .fetch_one(&ledger.pool)
        .await?;
        ensure!(bytes == 2, "{bytes} rows hold canonical bytes, not two");
        Ok(ledger)
    }
    .await;
    match result {
        Ok(ledger) => db.close(vec![ledger]).await,
        Err(error) => {
            db.close(Vec::new()).await?;
            Err(error)
        }
    }
}

// ---------------------------------------------------------------------------
// The public API, driven against the same database
// ---------------------------------------------------------------------------

async fn api(pool: &PgPool) -> Result<(axum::Router, tokio::task::JoinHandle<()>)> {
    use axum::{routing::post, Json, Router};

    async fn rpc(Json(input): Json<Value>) -> Json<Value> {
        let result = match input["method"].as_str().unwrap_or_default() {
            "getblockchaininfo" => {
                serde_json::json!({"chain":"regtest","blocks":10,"bestblockhash":"b".repeat(64),"initialblockdownload":false})
            }
            "getblocktemplate" => {
                serde_json::json!({"bits":"207fffff","coinbasevalue":5000000000u64})
            }
            "getnetworkinfo" => serde_json::json!({"connections":2}),
            _ => Value::Null,
        };
        Json(serde_json::json!({"result":result,"error":null,"id":"prism-share-archive"}))
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, Router::new().route("/", post(rpc))).await;
    });
    let config = ApiConfig {
        rpc_url: format!("http://{address}/"),
        cache_enabled: false,
        instance_id: "operator-a".into(),
        ..ApiConfig::default()
    };
    Ok((
        router(ApiState::new(
            pool.clone(),
            config,
            std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
        )),
        server,
    ))
}

async fn raw(app: &axum::Router, path: &str) -> (StatusCode, Vec<u8>) {
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tower::ServiceExt;

    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 50_000_000).await.unwrap();
    (status, bytes.to_vec())
}

async fn json(app: &axum::Router, path: &str) -> (StatusCode, Value) {
    let (status, bytes) = raw(app, path).await;
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Sequences are nontransactional: an append can hold a `share_seq` below the
/// partition's bound in a transaction that has not committed when the
/// sequence reports the bound passed. The archive waits for it under the
/// ledger's ordering lock and then holds every row, rather than writing a copy
/// that misses the row the detach would take with the partition.
#[tokio::test]
async fn archive_waits_for_an_append_that_drew_its_share_seq_before_the_bound() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let ledger = db.ledger("drain-a").await?;
        let archiver = db.ledger("drain-b").await?;
        let root = tempfile::tempdir()?;
        insert_shares(&ledger.pool, 1, 250, 7, "server-a", 7200.0).await?;
        let (_, p0_upper) = bounds(&ledger.pool, P0).await?;
        // The sequence has handed out the partition's last value and stands
        // past the bound, while the append that drew it is still open: the
        // ordering lock held, the row written, nothing committed.
        set_sequence(&ledger.pool, p0_upper).await?;
        let mut writer = ledger.pool.begin().await?;
        // `ORDER_LOCK` in `ledger.rs`, the advisory key every append holds
        // from before its insert until its transaction ends.
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(0x505249534d000002_i64)
            .execute(&mut *writer)
            .await?;
        sqlx::query(
            "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,\
             share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,\
             accepted,writer_id,writer_epoch) VALUES($1,'late:0001','m1','k',decode(repeat('aa',32),'hex'),7,1000,100,'job-a',\
             clock_timestamp(),1700000000,clock_timestamp(),true,'server-a',0)",
        )
        .bind(p0_upper - 1)
        .execute(&mut *writer)
        .await?;
        let archive = tokio::spawn({
            let root = root.path().to_path_buf();
            async move { archive::archive(&archiver, P0, &root, false, "operator-a").await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        ensure!(
            !archive.is_finished(),
            "the archive finished while an append below the bound was still open"
        );
        writer.commit().await?;
        let written = archive.await??;
        ensure!(
            written["manifest"]["row_count"] == 251
                && written["manifest"]["last_share_seq"] == p0_upper - 1,
            "the archive does not hold the late append: {}",
            written["manifest"]
        );
        let verified = archive::verify(&ledger, P0, root.path()).await?;
        ensure!(
            verified["live_rows_compared"] == true && verified["row_count"] == 251,
            "the archive did not verify against the live rows: {verified}"
        );
        Ok(ledger)
    }
    .await;
    match result {
        Ok(ledger) => db.close(vec![ledger]).await,
        Err(error) => {
            db.close(Vec::new()).await?;
            Err(error)
        }
    }
}
