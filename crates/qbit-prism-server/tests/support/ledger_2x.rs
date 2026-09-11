use super::*;
use qbit_prism_server::ledger::audit_canonical_bytes;
use serde_json::Value;
use sqlx::Row;
use std::io::Write;

const LEGACY_SCHEMA: &str = include_str!("../../../qbit-prism/sql/001_share_ledger.sql");

async fn seed_legacy_carry(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES(repeat('aa',32),100,repeat('00',32),repeat('ab',32),repeat('ac',32),'confirmed'),(repeat('ee',32),101,repeat('aa',32),repeat('ef',32),repeat('e0',32),'prepared'); INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES(100,repeat('aa',32),'miner-a','a',decode(repeat('11',32),'hex'),1000,0,1000,0,1000,'accrued'),(101,repeat('ee',32),'miner-b','b',decode(repeat('22',32),'hex'),500,0,500,0,500,'accrued'); DELETE FROM qbit_payout_carry_forward_current; UPDATE qbit_pool_blocks SET chain_state='confirmed' WHERE block_hash=repeat('ee',32);")
        .execute(pool).await?;
    Ok(())
}

async fn carry_summary(pool: &PgPool) -> Result<serde_json::Value> {
    Ok(sqlx::query_scalar("SELECT coalesce(jsonb_agg(jsonb_build_object('miner',miner_id,'balance',balance_sats::text,'count',active_row_count) ORDER BY miner_id),'[]'::jsonb) FROM qbit_payout_carry_forward_current")
        .fetch_one(pool).await?)
}

#[tokio::test]
async fn legacy_2x_upgrade_repairs_partial_carry_seed_and_preserves_shared_state() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    // This is the standalone 2.x schema, with its own BEGIN/COMMIT and no
    // native tables. Reproduce the interrupted legacy apply's partial seed.
    sqlx::raw_sql(LEGACY_SCHEMA).execute(&pool).await?;
    seed_legacy_carry(&pool).await?;
    assert_eq!(
        carry_summary(&pool).await?,
        json!([{"miner":"miner-b","balance":"500","count":1}])
    );
    sqlx::raw_sql("INSERT INTO qbit_worker_difficulty(listener,worker_username,difficulty,evidence_at) VALUES('primary','miner.worker',42,clock_timestamp()); INSERT INTO qbit_hashrate_rollup_progress(singleton,last_share_seq) VALUES(true,123);")
        .execute(&pool).await?;
    let ordinals: Vec<i64> = sqlx::query_scalar(
        "SELECT audit_publication_sequence FROM qbit_pool_blocks ORDER BY block_hash",
    )
    .fetch_all(&pool)
    .await?;
    let ledger = db.ledger("native-upgrade").await?;
    assert_eq!(
        carry_summary(&pool).await?,
        json!([{"miner":"miner-a","balance":"1000","count":1},{"miner":"miner-b","balance":"500","count":1}])
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_carry_forward_current_drift()")
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT max(version) FROM qbit_prism_schema_migrations")
            .fetch_one(&pool)
            .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT last_share_seq FROM qbit_hashrate_rollup_progress")
            .fetch_one(&pool)
            .await?,
        123
    );
    assert_eq!(
        ledger
            .worker_difficulty("primary", "miner.worker", 60)
            .await?
            .unwrap()
            .difficulty,
        42.0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT audit_publication_sequence FROM qbit_pool_blocks ORDER BY block_hash"
        )
        .fetch_all(&pool)
        .await?,
        ordinals
    );
    assert!(sqlx::query("INSERT INTO qbit_ledger_writer_lease(singleton,writer_id,writer_epoch,writer_session_token,lease_expires_at) VALUES(true,'python',1,'token',clock_timestamp()+interval '1 hour')").execute(&pool).await.is_err());
    pool.close().await;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn legacy_2x_failed_native_migration_rolls_back_seed_repair_and_all_ddl() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    sqlx::raw_sql(LEGACY_SCHEMA).execute(&pool).await?;
    seed_legacy_carry(&pool).await?;
    // Balance-only damage exercises the guard's drift comparison even when
    // active row counts match. Failure is deliberately after the base schema.
    sqlx::raw_sql("SELECT qbit_rebuild_carry_forward_current_balances(); UPDATE qbit_payout_carry_forward_current SET balance_sats=balance_sats+1 WHERE miner_id='miner-a'; CREATE TABLE qbit_prism_share_hashes(unexpected text);").execute(&pool).await?;
    let poisoned = carry_summary(&pool).await?;
    assert!(db.ledger("failed-upgrade").await.is_err());
    assert_eq!(
        carry_summary(&pool).await?,
        poisoned,
        "base schema committed before the native migration failed"
    );
    assert!(!sqlx::query_scalar::<_,bool>("SELECT to_regclass('qbit_prism_cluster') IS NOT NULL OR to_regclass('qbit_prism_schema_migrations') IS NOT NULL").fetch_one(&pool).await?);
    sqlx::query("DROP TABLE qbit_prism_share_hashes")
        .execute(&pool)
        .await?;
    let ledger = db.ledger("retry-upgrade").await?;
    assert_eq!(
        carry_summary(&pool).await?,
        json!([{"miner":"miner-a","balance":"1000","count":1},{"miner":"miner-b","balance":"500","count":1}])
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_carry_forward_current_drift()")
            .fetch_one(&pool)
            .await?,
        0
    );
    pool.close().await;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn native_publication_ordinal_survives_reactivation_and_excludes_rejected_candidates(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("a").await?;
    ledger.append(share(1), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 3001)?;
    ledger.enqueue_candidate(block.clone()).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT audit_publication_sequence FROM qbit_pool_blocks WHERE block_hash=$1"
        )
        .bind(&block.block_hash)
        .fetch_one(&ledger.pool)
        .await?,
        None
    );
    ledger.finish_candidate(&claim, true, None).await?;
    let ordinal: i64 = sqlx::query_scalar(
        "SELECT audit_publication_sequence FROM qbit_pool_blocks WHERE block_hash=$1",
    )
    .bind(&block.block_hash)
    .fetch_one(&ledger.pool)
    .await?;
    ledger
        .reconcile_blocks(
            &[BlockObservation {
                block_hash: block.block_hash.clone(),
                active: false,
            }],
            101,
        )
        .await?;
    let inactive=sqlx::query("SELECT audit_publication_sequence,inactive_since IS NOT NULL AS disconnected,chain_state,maturity_state FROM qbit_pool_blocks WHERE block_hash=$1").bind(&block.block_hash).fetch_one(&ledger.pool).await?;
    assert_eq!(
        inactive.try_get::<i64, _>("audit_publication_sequence")?,
        ordinal
    );
    assert!(inactive.try_get::<bool, _>("disconnected")?);
    assert_eq!(inactive.try_get::<String, _>("chain_state")?, "inactive");
    assert_eq!(inactive.try_get::<String, _>("maturity_state")?, "immature");
    ledger
        .reconcile_blocks(
            &[BlockObservation {
                block_hash: block.block_hash.clone(),
                active: true,
            }],
            101,
        )
        .await?;
    assert!(sqlx::query_scalar::<_,bool>("SELECT inactive_since IS NULL AND audit_publication_sequence=$2 FROM qbit_pool_blocks WHERE block_hash=$1").bind(&block.block_hash).bind(ordinal).fetch_one(&ledger.pool).await?);
    let rejected = candidate(&ledger.snapshot(100).await?, 3002)?;
    ledger.enqueue_candidate(rejected.clone()).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    ledger
        .finish_candidate(&claim, false, Some("node rejected"))
        .await?;
    assert!(sqlx::query_scalar::<_,bool>("SELECT audit_publication_sequence IS NULL AND inactive_since IS NULL FROM qbit_pool_blocks WHERE block_hash=$1").bind(&rejected.block_hash).fetch_one(&ledger.pool).await?);
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn canonical_2x_sidecar_import_preserves_exact_bytes_and_fails_closed() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("a").await?;
    ledger.append(share(1), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 4001)?;
    ledger.enqueue_candidate(block.clone()).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    let report = ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    let expected = qbit_prism::canonical_audit_bundle_bytes(&block.bundle)?;
    assert_eq!(
        audit_canonical_bytes(&ledger.pool, &block.block_hash).await?,
        Some(expected.clone())
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT canonical_audit_bytes IS NULL FROM qbit_pool_audit_bundles WHERE block_hash=$1"
        )
        .bind(&block.block_hash)
        .fetch_one(&ledger.pool)
        .await?,
        "native range snapshots must not duplicate their full share window"
    );
    let dir = tempfile::tempdir()?;
    let body_path = dir.path().join("legacy-audit.json");
    std::fs::write(&body_path, serde_json::to_vec(&block.bundle)?)?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2,share_snapshot_sha256=NULL,body_uri=$3 WHERE block_hash=$1").bind(&block.block_hash).bind(serde_json::to_value(&block.bundle)?).bind(body_path.to_str().unwrap()).execute(&ledger.pool).await?;
    assert_eq!(
        audit_canonical_bytes(&ledger.pool, &block.block_hash).await?,
        None
    );
    let path = dir.path().join(format!(
        "prism-audit-bundle-canonical-{}-{}.json.gz",
        block.block_hash, report.audit_bundle_sha256_hex
    ));
    std::fs::write(&path, b"corrupt gzip")?;
    assert!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await
            .is_err(),
        "corrupt present sidecar fell back to a valid body"
    );
    let write_gzip = |bytes: &[u8]| -> Result<()> {
        let mut encoder = flate2::GzBuilder::new()
            .mtime(0)
            .write(std::fs::File::create(&path)?, flate2::Compression::best());
        encoder.write_all(bytes)?;
        encoder.finish()?;
        Ok(())
    };
    write_gzip(b"{}")?;
    assert!(ledger
        .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
        .await
        .is_err());
    write_gzip(&expected)?;
    assert!(ledger
        .import_legacy_audits(Some(dir.path()), &keys().0.public_key_hex())
        .await
        .is_err());
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await?,
        1
    );
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await?,
        0
    );
    assert_eq!(
        audit_canonical_bytes(&ledger.pool, &block.block_hash).await?,
        Some(expected)
    );
    assert!(path.exists() && body_path.exists());
    sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2 WHERE block_hash=$1")
        .bind(&block.block_hash)
        .bind(b"{}".to_vec())
        .execute(&ledger.pool)
        .await?;
    assert!(audit_canonical_bytes(&ledger.pool, &block.block_hash)
        .await
        .is_err());
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn durable_worker_hints_order_by_shared_evidence_and_keep_original_ttl() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let accepted = share(8001);
    a.append(accepted.clone(), None).await?;
    let evidence = a.share_accepted_at_ms(&accepted.share_id).await?.unwrap();
    assert!(
        a.record_worker_difficulty("primary", "alice.worker", 32.0, evidence)
            .await?
    );
    assert!(
        !b.record_worker_difficulty("primary", "alice.worker", 64.0, evidence - 1)
            .await?
    );
    assert!(
        b.record_worker_difficulty("primary", "alice.worker", 48.0, evidence)
            .await?
    );
    assert!(
        a.lower_worker_difficulty("primary", "alice.worker", 8.0)
            .await?
    );
    assert!(
        !a.lower_worker_difficulty("primary", "alice.worker", 16.0)
            .await?
    );
    let hint = b
        .worker_difficulty("primary", "alice.worker", 60)
        .await?
        .unwrap();
    assert_eq!(hint.difficulty, 8.0);
    assert_eq!(hint.evidence_at_ms, evidence);
    assert!(hint.age_ms < 60_000);
    assert!(b
        .worker_difficulty("highdiff", "alice.worker", 60)
        .await?
        .is_none());
    assert!(b
        .worker_difficulty("primary", "Alice.worker", 60)
        .await?
        .is_none());
    for invalid in [f64::NAN, f64::INFINITY, 0.0, -1.0] {
        assert!(a
            .record_worker_difficulty("primary", "invalid", invalid, evidence)
            .await
            .is_err());
    }
    assert!(
        a.record_worker_difficulty("primary", "expired-a", 64.0, evidence - 120_000)
            .await?
    );
    assert!(
        a.record_worker_difficulty("primary", "expired-b", 64.0, evidence - 120_000)
            .await?
    );
    assert!(
        a.lower_worker_difficulty("primary", "expired-a", 1.0)
            .await?
    );
    assert!(a
        .worker_difficulty("primary", "expired-a", 60)
        .await?
        .is_none());
    assert_eq!(a.prune_worker_difficulties(60, 1).await?, 1);
    assert_eq!(b.prune_worker_difficulties(60, 1).await?, 1);
    assert_eq!(a.prune_worker_difficulties(60, 1).await?, 0);
    db.close(vec![a, b]).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_range_query_uses_deadline_remaining_after_delayed_snapshot() -> Result<()> {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use qbit_prism_server::api::{self, ApiConfig, ApiState};
    use std::time::{Duration, Instant};
    use tower::ServiceExt;
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("audit-deadline").await?;
    ledger.append(share(9001), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 9001)?;
    ledger.enqueue_candidate(block.clone()).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    let report = ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    let app = api::router(ApiState::new(
        ledger.pool.clone(),
        ApiConfig {
            cache_enabled: false,
            read_timeout: Duration::from_millis(800),
            ..Default::default()
        },
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    ));
    let path = format!("/public/v1/artifacts/{}", report.audit_bundle_sha256_hex);
    let mut snapshot_lock = ledger.pool.begin().await?;
    let snapshot_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *snapshot_lock)
        .await?;
    sqlx::query("LOCK TABLE qbit_prism_audit_snapshots IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *snapshot_lock)
        .await?;
    let mut share_lock = ledger.pool.begin().await?;
    let share_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *share_lock)
        .await?;
    sqlx::query("LOCK TABLE qbit_share_ledger IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *share_lock)
        .await?;
    let request = Request::builder().uri(&path).body(Body::empty())?;
    let started = Instant::now();
    let response = tokio::spawn(app.clone().oneshot(request));
    // Wait until the real public query spends its first part of the budget
    // behind the snapshot lock, then move it onto the independently held
    // share table lock. A reused transaction would still have ~800ms there.
    tokio::time::timeout(Duration::from_millis(500),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='prism-public-read' AND state='active' AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)))").bind(snapshot_pid).fetch_one(&db.admin).await?;
            if waiting {return Ok::<_,anyhow::Error>(())}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        started + Duration::from_millis(500),
    ))
    .await;
    snapshot_lock.rollback().await?;
    tokio::time::timeout(Duration::from_millis(250),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='prism-public-read' AND state='active' AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)))").bind(share_pid).fetch_one(&db.admin).await?;
            if waiting {return Ok::<_,anyhow::Error>(())}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    let response = response.await??;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(error["error"]["code"], "read_timeout");
    assert!(started.elapsed() < Duration::from_millis(1100));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name='prism-public-read' AND state='active' AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid))").bind(share_pid).fetch_one(&db.admin).await?;
    assert_eq!(
        waiting, 0,
        "late audit range SQL outlived the HTTP deadline and occupied the public pool"
    );
    share_lock.rollback().await?;
    let response = app
        .oneshot(Request::builder().uri(&path).body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    assert_eq!(
        bytes.as_ref(),
        qbit_prism::canonical_audit_bundle_bytes(&block.bundle)?
    );
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn compact_bits_metadata_comes_from_durable_header_and_recovers_without_audit_changes(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("bits-a").await?;
    let b = db.ledger("bits-b").await?;
    a.append(share(9101), None).await?;
    let mut block = candidate(&a.snapshot(100).await?, 9101)?;
    let canonical = qbit_prism::canonical_audit_bundle_bytes(&block.bundle)?;
    // Deliberately asymmetric compact bytes prove display endianness. Their
    // value is independent of the audit's scaled network-difficulty integer.
    let mut bytes = hex::decode(&block.block_hex)?;
    bytes[72..76].copy_from_slice(&0x1d00ffffu32.to_le_bytes());
    let mut hash = Sha256::digest(Sha256::digest(&bytes[..80])).to_vec();
    hash.reverse();
    block.block_hash = hex::encode(hash);
    block.block_hex = hex::encode(bytes);
    a.enqueue_candidate(block.clone()).await?;
    let claim = a.claim_candidate(60).await?.unwrap();
    let report = a.land_candidate(&claim, &keys().1.public_key_hex()).await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT found_block_bits FROM qbit_pool_audit_bundles WHERE block_hash=$1"
        )
        .bind(&block.block_hash)
        .fetch_one(&a.pool)
        .await?,
        "1d00ffff"
    );
    assert_eq!(
        report.audit_bundle_sha256_hex,
        hex::encode(Sha256::digest(&canonical))
    );
    // Simulate a pre-fix prepared row and an owner crash. The next physical
    // instance recovers the same old candidate format with its original header.
    sqlx::query("UPDATE qbit_pool_audit_bundles SET found_block_bits=NULL WHERE block_hash=$1")
        .bind(&block.block_hash)
        .execute(&a.pool)
        .await?;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE block_hash=$1").bind(&block.block_hash).execute(&a.pool).await?;
    let recovered = b.claim_candidate(60).await?.unwrap();
    assert_eq!(recovered.candidate.block_hex, block.block_hex);
    let recovered_report = b
        .land_candidate(&recovered, &keys().1.public_key_hex())
        .await?;
    assert_eq!(
        recovered_report.audit_bundle_sha256_hex,
        report.audit_bundle_sha256_hex
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT found_block_bits FROM qbit_pool_audit_bundles WHERE block_hash=$1"
        )
        .bind(&block.block_hash)
        .fetch_one(&b.pool)
        .await?,
        "1d00ffff"
    );
    assert_eq!(
        audit_canonical_bytes(&b.pool, &block.block_hash).await?,
        Some(canonical)
    );
    sqlx::query(
        "UPDATE qbit_pool_audit_bundles SET found_block_bits='207fffff' WHERE block_hash=$1",
    )
    .bind(&block.block_hash)
    .execute(&b.pool)
    .await?;
    assert!(
        b.land_candidate(&recovered, &keys().1.public_key_hex())
            .await
            .is_err(),
        "idempotent landing accepted contradictory header metadata"
    );
    db.close(vec![a, b]).await
}

// ---------------------------------------------------------------------------
// Imported legacy audits are served from their canonical bytes (#265).
// ---------------------------------------------------------------------------

fn imported_audit_router(pool: &PgPool) -> axum::Router {
    use qbit_prism_server::api::{self, ApiConfig, ApiState};
    api::router(ApiState::new(
        pool.clone(),
        ApiConfig {
            cache_enabled: false,
            ..Default::default()
        },
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    ))
}

async fn api_get(app: &axum::Router, path: &str) -> Result<(axum::http::StatusCode, Value)> {
    use tower::ServiceExt;
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(path)
                .body(axum::body::Body::empty())?,
        )
        .await?;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

async fn land_confirmed(ledger: &Ledger, block: &Candidate) -> Result<()> {
    ledger.enqueue_candidate(block.clone()).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    ledger.finish_candidate(&claim, true, None).await
}

/// Reshape a landed native row as a 2.x externalized row: its body lives only
/// in the `body_uri` file, as the legacy import finds it.
async fn externalize(
    pool: &PgPool,
    block: &Candidate,
    dir: &std::path::Path,
) -> Result<std::path::PathBuf> {
    let path = dir.join(format!("legacy-audit-{}.json", block.block_hash));
    std::fs::write(&path, serde_json::to_vec(&block.bundle)?)?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=NULL,share_snapshot_sha256=NULL,body_uri=$2 WHERE block_hash=$1")
        .bind(&block.block_hash).bind(path.to_str().unwrap()).execute(pool).await?;
    Ok(path)
}

/// The metadata columns the import writes, plus the stored bits it must not.
async fn audit_metadata(pool: &PgPool, hash: &str) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object('schema_version',schema_version,'difficulty',found_block_network_difficulty::text,'value',found_block_coinbase_value_sats,'commitments',audit_commitment_leaves_hex,'witness',witness_merkle_leaves_hex,'bits',found_block_bits) FROM qbit_pool_audit_bundles WHERE block_hash=$1")
        .bind(hash).fetch_one(pool).await?)
}

async fn canonical_state(pool: &PgPool, hash: &str) -> Result<(bool, bool, bool)> {
    let row = sqlx::query("SELECT audit_bundle IS NULL AS body_null,canonical_audit_bytes IS NOT NULL AS canonical,encode(sha256(canonical_audit_bytes),'hex') IS NOT DISTINCT FROM audit_bundle_sha256 AS digest FROM qbit_pool_audit_bundles WHERE block_hash=$1")
        .bind(hash).fetch_one(pool).await?;
    Ok((
        row.try_get("body_null")?,
        row.try_get("canonical")?,
        row.try_get("digest")?,
    ))
}

async fn dashboard_row(app: &axum::Router, hash: &str) -> Result<Value> {
    let (status, blocks) = api_get(app, "/public/v1/blocks?chain_state=all&limit=100").await?;
    ensure_ok(status, &blocks)?;
    blocks["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|row| row["hash"] == hash)
        .cloned()
        .with_context(|| format!("dashboard is missing {hash}: {blocks}"))
}

fn ensure_ok(status: axum::http::StatusCode, body: &Value) -> Result<()> {
    anyhow::ensure!(status == axum::http::StatusCode::OK, "{status}: {body}");
    Ok(())
}

/// The dashboard as the pre-#265 import left it: the same metadata columns
/// plus the inline logical body. Restores the row's current body afterwards.
async fn dashboard_row_with_inline_import(
    app: &axum::Router,
    pool: &PgPool,
    block: &Candidate,
) -> Result<Value> {
    let current: Option<Value> =
        sqlx::query_scalar("SELECT audit_bundle FROM qbit_pool_audit_bundles WHERE block_hash=$1")
            .bind(&block.block_hash)
            .fetch_one(pool)
            .await?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2 WHERE block_hash=$1")
        .bind(&block.block_hash)
        .bind(serde_json::to_value(&block.bundle)?)
        .execute(pool)
        .await?;
    let row = dashboard_row(app, &block.block_hash).await;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2 WHERE block_hash=$1")
        .bind(&block.block_hash)
        .bind(current)
        .execute(pool)
        .await?;
    row
}

/// A bundle that records its settlement decision, as the coordinator builds it.
fn settled_bundle(
    snapshot: &Snapshot,
    direct_floor_sats: u64,
    config: qbit_prism::SettlementModeConfig,
) -> Result<qbit_prism::AuditBundle> {
    let (coinbase_key, ledger_key) = keys();
    Ok(qbit_prism::build_audit_bundle_with_ctv_settlement_options(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        direct_floor_sats,
        config,
        Some(qbit_prism::FanoutFeeRatePolicy::new(1000, 12000)),
        None,
        vec![],
        &coinbase_key,
        &ledger_key,
    )?)
}

#[tokio::test]
async fn imported_external_audit_is_served_from_canonical_bytes_without_the_legacy_file(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-external").await?;
    ledger.append(share(5001), None).await?;
    let snapshot = ledger.snapshot(100).await?;
    let block = candidate_with_bundle(
        settled_bundle(&snapshot, 0, Default::default())?,
        snapshot.payout_revision,
        5001,
    )?;
    let hash = block.block_hash.clone();
    let logical = serde_json::to_value(&block.bundle)?;
    let commitment = block.bundle.audit_commitment_leaves_hex[0].clone();
    land_confirmed(&ledger, &block).await?;
    let metadata = audit_metadata(&ledger.pool, &hash).await?;
    assert_eq!(metadata["bits"], "207fffff");
    let dir = tempfile::tempdir()?;
    let file = externalize(&ledger.pool, &block, dir.path()).await?;
    let app = imported_audit_router(&ledger.pool);
    let block_path = format!("/audit/blocks/{hash}/bundle");
    let commitment_path = format!("/audit/commitments/{commitment}/bundle");
    let settlement_path = format!("/public/v1/blocks/{hash}/settlement-artifacts");
    // Before the import, the legacy file serves the same logical body the
    // pre-#265 import stored inline.
    let (status, before_block) = api_get(&app, &block_path).await?;
    ensure_ok(status, &before_block)?;
    assert_eq!(before_block["audit_bundle"], logical);
    let (status, before_commitment) = api_get(&app, &commitment_path).await?;
    ensure_ok(status, &before_commitment)?;
    let (status, mut before_settlement) = api_get(&app, &settlement_path).await?;
    ensure_ok(status, &before_settlement)?;
    let before_dashboard = dashboard_row(&app, &hash).await?;
    // 2.x wrote no metadata for some rows. The import restores all of it
    // except the bits, which no audit body carries and which stay as stored.
    sqlx::query("UPDATE qbit_pool_audit_bundles SET schema_version=NULL,found_block_network_difficulty=NULL,found_block_coinbase_value_sats=NULL,audit_commitment_leaves_hex=NULL,witness_merkle_leaves_hex=NULL WHERE block_hash=$1")
        .bind(&hash).execute(&ledger.pool).await?;
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await?,
        1
    );
    assert_eq!(
        canonical_state(&ledger.pool, &hash).await?,
        (true, true, true),
        "import must store digest-checked canonical bytes and no inline body"
    );
    assert_eq!(audit_metadata(&ledger.pool, &hash).await?, metadata);
    std::fs::remove_file(&file)?;
    assert_eq!(ledger.audit_bundle(&hash).await?, Some(logical.clone()));
    let (status, after_block) = api_get(&app, &block_path).await?;
    ensure_ok(status, &after_block)?;
    assert_eq!(after_block, before_block);
    assert!(
        after_block.get("body_uri").is_none() && after_block.get("share_snapshot_sha256").is_none()
    );
    let (status, after_commitment) = api_get(&app, &commitment_path).await?;
    ensure_ok(status, &after_commitment)?;
    assert_eq!(after_commitment, before_commitment);
    // Direct-coinbase blocks take their settlement payload from the body.
    let (status, mut after_settlement) = api_get(&app, &settlement_path).await?;
    ensure_ok(status, &after_settlement)?;
    for payload in [&mut before_settlement, &mut after_settlement] {
        payload.as_object_mut().unwrap().remove("generated_at");
    }
    assert_eq!(after_settlement, before_settlement);
    assert_eq!(after_settlement["settlement_mode"], "direct_coinbase");
    assert!(
        after_settlement["artifact_links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|link| link["kind"] == "audit_bundle"),
        "{after_settlement}"
    );
    let dashboard = dashboard_row(&app, &hash).await?;
    assert_eq!(dashboard, before_dashboard);
    assert_eq!(
        dashboard,
        dashboard_row_with_inline_import(&app, &ledger.pool, &block).await?
    );
    assert_eq!(dashboard["bits"], "207fffff");
    let (status, latest) = api_get(&app, "/audit/latest").await?;
    ensure_ok(status, &latest)?;
    assert_eq!(latest["job_share_count"], 1);
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn corrupt_imported_canonical_bytes_are_refused_not_served_from_the_legacy_file() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-corrupt").await?;
    ledger.append(share(5101), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 5101)?;
    let hash = block.block_hash.clone();
    let commitment = block.bundle.audit_commitment_leaves_hex[0].clone();
    land_confirmed(&ledger, &block).await?;
    let dir = tempfile::tempdir()?;
    let file = externalize(&ledger.pool, &block, dir.path()).await?;
    ledger
        .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
        .await?;
    let (bytes, digest): (Vec<u8>, String) = sqlx::query_as(
        "SELECT canonical_audit_bytes,audit_bundle_sha256 FROM qbit_pool_audit_bundles WHERE block_hash=$1",
    )
    .bind(&hash)
    .fetch_one(&ledger.pool)
    .await?;
    let app = imported_audit_router(&ledger.pool);
    let paths = [
        format!("/audit/blocks/{hash}/bundle"),
        format!("/audit/commitments/{commitment}/bundle"),
    ];
    let mut flipped = bytes.clone();
    flipped[bytes.len() / 2] ^= 0x01;
    let digest_of = |bytes: &[u8]| hex::encode(Sha256::digest(bytes));
    // A well-formed inline body-ref envelope the shared parser would resolve.
    let envelope = {
        let mut body = serde_json::to_value(&block.bundle)?;
        let shares = body.as_object_mut().unwrap().remove("shares").unwrap();
        let seq = block.bundle.shares[0].share_seq;
        serde_json::to_vec(
            &json!({"schema":qbit_prism::AUDIT_BODY_REF_SCHEMA,"audit_bundle_sha256":digest,"share_count":1,"bundle_without_shares":body,"share_parts":[{"kind":"inline","first_share_seq":seq,"last_share_seq":seq,"share_count":1,"shares":shares}]}),
        )?
    };
    for (case, stored, declared) in [
        ("one flipped byte", flipped.clone(), digest.clone()),
        ("declared digest mismatch", bytes.clone(), "00".repeat(32)),
        (
            "digest-valid empty object",
            b"{}".to_vec(),
            digest_of(b"{}"),
        ),
        ("digest-valid non-object", b"[]".to_vec(), digest_of(b"[]")),
        (
            "digest-valid non-JSON",
            b"not json".to_vec(),
            digest_of(b"not json"),
        ),
        (
            "digest-valid body-ref envelope",
            envelope.clone(),
            digest_of(&envelope),
        ),
    ] {
        sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2,audit_bundle_sha256=$3 WHERE block_hash=$1")
            .bind(&hash).bind(&stored).bind(&declared).execute(&ledger.pool).await?;
        assert!(file.exists());
        assert!(
            ledger.audit_bundle(&hash).await.is_err(),
            "{case}: Ledger::audit_bundle served a corrupt imported body"
        );
        for path in &paths {
            let (status, body) = api_get(&app, path).await?;
            assert_eq!(
                status,
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "{case}: {path} served {body}"
            );
            assert!(body.get("audit_bundle").is_none(), "{case}: {path}: {body}");
        }
    }
    sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2,audit_bundle_sha256=$3 WHERE block_hash=$1")
        .bind(&hash).bind(&bytes).bind(&digest).execute(&ledger.pool).await?;
    assert_eq!(
        ledger.audit_bundle(&hash).await?,
        Some(serde_json::to_value(&block.bundle)?)
    );
    for path in &paths {
        let (status, body) = api_get(&app, path).await?;
        ensure_ok(status, &body)?;
    }
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn imported_ctv_audit_backfills_and_links_without_the_legacy_file() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-ctv").await?;
    ledger.append(share(5201), None).await?;
    let mut second = share(5202);
    second.miner_id = "other".into();
    second.order_key = "other".into();
    second.p2mr_program_hex = "22".repeat(32);
    ledger.append(second, None).await?;
    let snapshot = ledger.snapshot(100).await?;
    let ledger_key = keys().1;
    let bundle = settled_bundle(
        &snapshot,
        u64::MAX,
        qbit_prism::SettlementModeConfig {
            max_fanout_recipients_per_transaction: 1,
            ..Default::default()
        },
    )?;
    let block = candidate_with_bundle(bundle, snapshot.payout_revision, 5201)?;
    let hash = block.block_hash.clone();
    land_confirmed(&ledger, &block).await?;
    let dir = tempfile::tempdir()?;
    let file = externalize(&ledger.pool, &block, dir.path()).await?;
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &ledger_key.public_key_hex())
            .await?,
        1
    );
    std::fs::remove_file(&file)?;
    assert_eq!(ledger.backfill_ctv(&ledger_key.public_key_hex()).await?, 0);
    sqlx::query("DELETE FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1 AND chunk_index=1")
        .bind(&hash)
        .execute(&ledger.pool)
        .await?;
    assert_eq!(ledger.backfill_ctv(&ledger_key.public_key_hex()).await?, 1);
    assert_eq!(ledger.backfill_ctv(&ledger_key.public_key_hex()).await?, 0);
    let app = imported_audit_router(&ledger.pool);
    // CTV payloads come from the fanout tables; the audit link comes from the
    // metadata shortcut rather than a whole-body decode.
    let (status, settlement) = api_get(
        &app,
        &format!("/public/v1/blocks/{hash}/settlement-artifacts"),
    )
    .await?;
    ensure_ok(status, &settlement)?;
    assert_eq!(settlement["settlement_mode"], "ctv_fanout");
    let audit_sha = settlement["audit_bundle_sha256"].as_str().unwrap();
    assert!(
        settlement["artifact_links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|link| link["kind"] == "audit_bundle" && link["sha256"] == audit_sha),
        "{settlement}"
    );
    assert_eq!(settlement["fanouts"].as_array().unwrap().len(), 2);
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn inline_only_legacy_audit_import_keeps_its_body_and_serves_canonical_bytes() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-inline").await?;
    ledger.append(share(5301), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 5301)?;
    let hash = block.block_hash.clone();
    let logical = serde_json::to_value(&block.bundle)?;
    land_confirmed(&ledger, &block).await?;
    let digest: String = sqlx::query_scalar(
        "SELECT audit_bundle_sha256 FROM qbit_pool_audit_bundles WHERE block_hash=$1",
    )
    .bind(&hash)
    .fetch_one(&ledger.pool)
    .await?;
    // A 2.x row from before externalization: an inline body-ref envelope, no
    // body_uri, and none of the metadata columns, including the bits.
    let mut body = logical.clone();
    let shares = body.as_object_mut().unwrap().remove("shares").unwrap();
    let seq = block.bundle.shares[0].share_seq;
    let envelope = json!({"schema":"qbit.prism.audit-body-ref.v1","audit_bundle_sha256":digest,"share_count":1,"bundle_without_shares":body,"share_parts":[{"kind":"inline","first_share_seq":seq,"last_share_seq":seq,"share_count":1,"shares":shares}]});
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2,share_snapshot_sha256=NULL,body_uri=NULL,schema_version=NULL,found_block_network_difficulty=NULL,found_block_coinbase_value_sats=NULL,audit_commitment_leaves_hex=NULL,witness_merkle_leaves_hex=NULL,found_block_bits=NULL WHERE block_hash=$1")
        .bind(&hash).bind(&envelope).execute(&ledger.pool).await?;
    let app = imported_audit_router(&ledger.pool);
    let block_path = format!("/audit/blocks/{hash}/bundle");
    let (status, before) = api_get(&app, &block_path).await?;
    ensure_ok(status, &before)?;
    assert_eq!(before["audit_bundle"], envelope);
    assert_eq!(
        ledger
            .import_legacy_audits(None, &keys().1.public_key_hex())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>(
            "SELECT audit_bundle FROM qbit_pool_audit_bundles WHERE block_hash=$1"
        )
        .bind(&hash)
        .fetch_one(&ledger.pool)
        .await?,
        envelope,
        "option A keeps an inline-only row's stored body unchanged"
    );
    assert_eq!(
        canonical_state(&ledger.pool, &hash).await?,
        (false, true, true)
    );
    let metadata = audit_metadata(&ledger.pool, &hash).await?;
    assert_eq!(metadata["difficulty"], "100");
    assert_eq!(metadata["value"], 500_000_000);
    assert_eq!(
        metadata["commitments"],
        json!(block.bundle.audit_commitment_leaves_hex)
    );
    assert!(metadata["bits"].is_null(), "the import invented bits");
    // Both readers serve the canonical bytes' logical body, not the envelope.
    assert_eq!(ledger.audit_bundle(&hash).await?, Some(logical.clone()));
    let (status, after) = api_get(&app, &block_path).await?;
    ensure_ok(status, &after)?;
    assert_eq!(after["audit_bundle"], logical);
    let commitment = &block.bundle.audit_commitment_leaves_hex[0];
    let (status, by_commitment) =
        api_get(&app, &format!("/audit/commitments/{commitment}/bundle")).await?;
    ensure_ok(status, &by_commitment)?;
    assert_eq!(by_commitment["audit_bundle"], logical);
    let dashboard = dashboard_row(&app, &hash).await?;
    assert_eq!(
        dashboard,
        dashboard_row_with_inline_import(&app, &ledger.pool, &block).await?
    );
    assert_eq!(dashboard["bits"], "00000000");
    // A second run finds nothing left to import and leaves the row alone.
    assert_eq!(
        ledger
            .import_legacy_audits(None, &keys().1.public_key_hex())
            .await?,
        0
    );
    db.close(vec![ledger]).await
}

/// The `audit_bundle` member each public bundle query loads for a row.
async fn bundle_query_bodies(pool: &PgPool, hash: &str, commitment: &str) -> Result<[Value; 2]> {
    let mut bodies = [Value::Null, Value::Null];
    for (body, (sql, id)) in bodies.iter_mut().zip([
        (include_str!("../../src/api/queries/audit_bundle.sql"), hash),
        (
            include_str!("../../src/api/queries/audit_bundle_by_commitment.sql"),
            commitment,
        ),
    ]) {
        let mut row: Value = sqlx::query_scalar(sql).bind(id).fetch_one(pool).await?;
        anyhow::ensure!(
            row["block_hash"] == hash && row.get("audit_bundle").is_some(),
            "{row}"
        );
        *body = row["audit_bundle"].take();
    }
    Ok(bodies)
}

#[tokio::test]
async fn superseded_inline_audit_body_is_not_loaded_for_imported_rows() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-superseded").await?;
    ledger.append(share(5401), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 5401)?;
    let hash = block.block_hash.clone();
    let logical = serde_json::to_value(&block.bundle)?;
    let commitment = block.bundle.audit_commitment_leaves_hex[0].clone();
    land_confirmed(&ledger, &block).await?;
    let dir = tempfile::tempdir()?;
    let file = externalize(&ledger.pool, &block, dir.path()).await?;
    let app = imported_audit_router(&ledger.pool);
    let paths = [
        format!("/audit/blocks/{hash}/bundle"),
        format!("/audit/commitments/{commitment}/bundle"),
    ];
    let mut before = Vec::new();
    for path in &paths {
        let (status, body) = api_get(&app, path).await?;
        ensure_ok(status, &body)?;
        assert_eq!(body["audit_bundle"], logical);
        before.push(body);
    }
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await?,
        1
    );
    std::fs::remove_file(&file)?;
    // The row as the pre-change import left it: the full inline logical body
    // beside the canonical bytes that supersede it, its body_uri, and no
    // snapshot.
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2 WHERE block_hash=$1")
        .bind(&hash)
        .bind(&logical)
        .execute(&ledger.pool)
        .await?;
    assert_eq!(
        canonical_state(&ledger.pool, &hash).await?,
        (false, true, true)
    );
    assert!(sqlx::query_scalar::<_, bool>("SELECT body_uri IS NOT NULL AND share_snapshot_sha256 IS NULL FROM qbit_pool_audit_bundles WHERE block_hash=$1")
        .bind(&hash).fetch_one(&ledger.pool).await?);
    assert_eq!(
        bundle_query_bodies(&ledger.pool, &hash, &commitment).await?,
        [Value::Null, Value::Null],
        "a bundle query loaded the superseded inline body"
    );
    for (path, before) in paths.iter().zip(&before) {
        let (status, body) = api_get(&app, path).await?;
        ensure_ok(status, &body)?;
        assert_eq!(&body, before, "{path}");
    }
    assert_eq!(ledger.audit_bundle(&hash).await?, Some(logical.clone()));
    // Without canonical bytes, the inline body is the served representation.
    sqlx::query(
        "UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=NULL WHERE block_hash=$1",
    )
    .bind(&hash)
    .execute(&ledger.pool)
    .await?;
    assert_eq!(
        bundle_query_bodies(&ledger.pool, &hash, &commitment).await?,
        [logical.clone(), logical.clone()]
    );
    for (path, before) in paths.iter().zip(&before) {
        let (status, body) = api_get(&app, path).await?;
        ensure_ok(status, &body)?;
        assert_eq!(&body, before, "{path}");
    }
    db.close(vec![ledger]).await
}

fn ensure_read_timeout((status, body): (axum::http::StatusCode, Value)) -> Result<()> {
    anyhow::ensure!(
        status == axum::http::StatusCode::SERVICE_UNAVAILABLE
            && body["error"]["code"] == "read_timeout",
        "{status}: {body}"
    );
    Ok(())
}

/// Imported audit decodes run after their read connection is released. The
/// runtime has one blocking thread and the test occupies it, so a decode the
/// API starts stays queued, holding its permit, until the test lets it run.
#[test]
fn imported_audit_decode_limit_outlives_a_dropped_request() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?
        .block_on(decode_limit_outlives_a_dropped_request())
}

async fn decode_limit_outlives_a_dropped_request() -> Result<()> {
    use qbit_prism_server::api::{self, ApiConfig, ApiState};
    use std::time::Duration;
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-decode-limit").await?;
    ledger.append(share(5501), None).await?;
    let snapshot = ledger.snapshot(100).await?;
    let block = candidate_with_bundle(
        settled_bundle(&snapshot, 0, Default::default())?,
        snapshot.payout_revision,
        5501,
    )?;
    let hash = block.block_hash.clone();
    land_confirmed(&ledger, &block).await?;
    let dir = tempfile::tempdir()?;
    let file = externalize(&ledger.pool, &block, dir.path()).await?;
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await?,
        1
    );
    std::fs::remove_file(&file)?;
    let state = ApiState::new(
        ledger.pool.clone(),
        ApiConfig {
            cache_enabled: false,
            read_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
    .with_read_concurrency(1);
    let limit = state.audit_decode_limit();
    let app = api::router(state);
    // A direct-coinbase settlement is read from one decode of the body.
    let path = format!("/public/v1/blocks/{hash}/settlement-artifacts");
    let settlement = |app: axum::Router| {
        let path = path.clone();
        async move {
            let (status, mut body) = api_get(&app, &path).await?;
            ensure_ok(status, &body)?;
            body.as_object_mut().unwrap().remove("generated_at");
            Ok::<_, anyhow::Error>(body)
        }
    };
    let baseline = settlement(app.clone()).await?;
    assert_eq!(baseline["settlement_mode"], "direct_coinbase");
    assert_eq!(limit.available_permits(), 1);
    // While another holder has the only permit, no decode starts: the
    // request spends its own deadline waiting instead.
    let held = limit.clone().acquire_owned().await?;
    ensure_read_timeout(api_get(&app, &path).await?)?;
    drop(held);
    assert_eq!(settlement(app.clone()).await?, baseline);

    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let (started, running) = tokio::sync::oneshot::channel();
    let blocker = tokio::task::spawn_blocking(move || {
        let _ = started.send(());
        let _ = blocked.recv();
    });
    running.await?;
    // The request takes the permit and queues its decode; the deadline then
    // drops the request while it awaits that decode.
    ensure_read_timeout(api_get(&app, &path).await?)?;
    assert_eq!(
        limit.available_permits(),
        0,
        "a dropped request released its permit before its decode finished"
    );
    // A second request cannot start another decode meanwhile.
    ensure_read_timeout(api_get(&app, &path).await?)?;
    assert_eq!(limit.available_permits(), 0);
    release.send(())?;
    blocker.await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while limit.available_permits() != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("the finished decode kept its permit")?;
    assert_eq!(settlement(app).await?, baseline);
    db.close(vec![ledger]).await
}
