use super::*;
use qbit_prism_server::ledger::audit_canonical_bytes;
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
        4
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
