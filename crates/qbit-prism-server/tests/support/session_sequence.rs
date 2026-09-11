use super::*;
use qbit_prism_server::ledger::SessionAllocationExhausted;

async fn wrap(pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT setval('qbit_prism_session_sequence',4294967295,true)")
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn stopped_guard_blocks_cleanup_and_owner_filter_preserves_other_reservations() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("shutdown-guard").await?;
    let other = db.ledger("other-owner").await?;
    let held = ledger.new_session_id().await?;
    let owner_token: String = sqlx::query_scalar(
        "SELECT status->>'session_owner_token' FROM qbit_prism_instances WHERE instance_id='shutdown-guard'",
    )
    .fetch_one(&ledger.pool)
    .await?;
    sqlx::query("INSERT INTO qbit_prism_session_reservations(extranonce1,instance_id,owner_token,reservation_token) VALUES(99,'shutdown-guard',$1,'leftover')")
        .bind(owner_token)
        .execute(&ledger.pool)
        .await?;
    let other_token: String = sqlx::query_scalar("SELECT status->>'session_owner_token' FROM qbit_prism_instances WHERE instance_id='other-owner'")
        .fetch_one(&other.pool).await?;
    sqlx::query("INSERT INTO qbit_prism_session_reservations(extranonce1,instance_id,owner_token,reservation_token) VALUES(100,'other-owner',$1,'other')")
        .bind(other_token).execute(&other.pool).await?;
    let stopped = ledger.heartbeat(json!({"state":"stopped"})).await;
    assert!(
        stopped.is_err(),
        "stopped must fail while a guard is active"
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_session_reservations")
        .fetch_one(&ledger.pool)
        .await?;
    assert_eq!(count, 3, "failed shutdown retains all reservations");
    held.release().await?;
    ledger.heartbeat(json!({"state":"stopped"})).await?;
    ledger.release_session_owner_reservations().await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_session_reservations")
        .fetch_one(&ledger.pool)
        .await?;
    assert_eq!(count, 1, "successful stopped path cleans only this owner");
    db.close(vec![ledger, other]).await
}

#[tokio::test]
async fn initialize_false_rejects_pre009_schema() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    sqlx::raw_sql("CREATE TABLE qbit_prism_schema_migrations(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp()); INSERT INTO qbit_prism_schema_migrations(version) VALUES(2),(3),(4),(5)")
        .execute(&pool).await?;
    let error = match Ledger::connect(&db.url, "pre009".into(), 4, false).await {
        Ok(_) => panic!("pre-009 schema must fail when initialization is disabled"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("migration 009"));
    pool.close().await;
    db.close(vec![]).await
}

async fn seed_job(pool: &PgPool, id: &str, extra: &str, live: bool) -> Result<()> {
    sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at) VALUES($1,'pre-009','parent',0,$2,clock_timestamp()+$3*interval '1 hour')")
        .bind(id).bind(json!({"extranonce1":extra,"old-field":"retained"}))
        .bind(if live {1_i64} else {-1_i64}).execute(pool).await?;
    Ok(())
}

#[tokio::test]
async fn wrap_reuses_expired_jobs_and_skips_live_jobs_and_sessions() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    seed_job(&a.pool, "expired", "00000001", false).await?;
    seed_job(&a.pool, "live", "00000002", true).await?;
    wrap(&a.pool).await?;
    let first = a.new_session_id().await?;
    assert_eq!(first.value(), 1, "an expired job must not prevent wrap");
    wrap(&a.pool).await?;
    let next = b.new_session_id().await?;
    assert_eq!(
        next.value(),
        3,
        "the live reservation and live job both block reuse"
    );
    first.release().await?;
    next.release().await?;
    seed_job(&a.pool, "live-next", "00000001", true).await?;
    wrap(&a.pool).await?;
    let next = b.new_session_id().await?;
    assert_eq!(
        next.value(),
        3,
        "a live job at the wrap boundary must be skipped"
    );
    next.release().await?;
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn migration_009_preserves_preexisting_jobs_and_runs_once_for_two_frontends() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    // Build the actual pre-009 schema, not an already-migrated approximation.
    sqlx::raw_sql(include_str!("../../../qbit-prism/sql/001_share_ledger.sql"))
        .execute(&pool)
        .await?;
    for sql in [
        include_str!("../../migrations/002_multi_instance.sql"),
        include_str!("../../migrations/003_2x_compatibility.sql"),
        include_str!("../../migrations/004_cpfp_retired_funding.sql"),
        include_str!("../../migrations/005_candidate_dispatch.sql"),
    ] {
        sqlx::raw_sql(sql).execute(&pool).await?;
    }
    sqlx::raw_sql("CREATE TABLE qbit_prism_schema_migrations(version integer PRIMARY KEY,applied_at timestamptz NOT NULL DEFAULT clock_timestamp()); INSERT INTO qbit_prism_schema_migrations(version) VALUES(2),(3),(4),(5)")
        .execute(&pool).await?;
    seed_job(&pool, "expired", "00000001", false).await?;
    seed_job(&pool, "old-arbitrary", "0000000A", true).await?;
    let before: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(j) FROM qbit_prism_jobs j ORDER BY job_id")
            .fetch_all(&pool)
            .await?;
    let (a, b) = tokio::try_join!(db.ledger("a"), db.ledger("b"))?;
    let after: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(j) FROM qbit_prism_jobs j ORDER BY job_id")
            .fetch_all(&pool)
            .await?;
    assert_eq!(before, after, "migration must not rewrite pre-009 rows");
    let versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(&pool)
            .await?;
    assert_eq!(versions, vec![2, 3, 4, 5, 9]);
    let cycled: bool = sqlx::query_scalar(
        "SELECT seqcycle FROM pg_sequence WHERE seqrelid='qbit_prism_session_sequence'::regclass",
    )
    .fetch_one(&pool)
    .await?;
    assert!(cycled);
    wrap(&pool).await?;
    let first = a.new_session_id().await?;
    assert_eq!(first.value(), 1);
    sqlx::query("SELECT setval('qbit_prism_session_sequence',9,true)")
        .execute(&pool)
        .await?;
    let next = b.new_session_id().await?;
    assert_eq!(
        next.value(),
        11,
        "pre-009 uppercase hexadecimal is protected"
    );
    first.release().await?;
    next.release().await?;
    pool.close().await;
    db.close(vec![a, b]).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_frontends_race_across_wrap_without_duplicate_allocations() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let a_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&a.pool)
        .await?;
    let b_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&b.pool)
        .await?;
    assert_ne!(a_pid, b_pid, "must use two real PostgreSQL connections");
    sqlx::raw_sql("ALTER SEQUENCE qbit_prism_session_sequence MAXVALUE 32; SELECT setval('qbit_prism_session_sequence',32,true)")
        .execute(&a.pool).await?;
    let ids = futures_util::future::try_join_all((0..32).map(|n| {
        let ledger = if n % 2 == 0 { &a } else { &b };
        ledger.new_session_id()
    }))
    .await?;
    let unique: std::collections::HashSet<_> = ids.iter().map(|id| id.value()).collect();
    assert_eq!(unique.len(), 32);
    for id in ids {
        id.release().await?;
    }

    // Only one free value forces both frontends to attempt the SAME candidate,
    // including after nextval cycles, rather than merely testing nextval itself.
    seed_job(&a.pool, "occupied-second-value", "00000002", true).await?;
    sqlx::raw_sql("ALTER SEQUENCE qbit_prism_session_sequence RESTART WITH 1 MAXVALUE 2")
        .execute(&a.pool)
        .await?;
    let (left, right) = tokio::join!(a.new_session_id(), b.new_session_id());
    let (winner, error) = match (left, right) {
        (Ok(id), Err(error)) | (Err(error), Ok(id)) => (id, error),
        other => panic!("exactly one concurrent allocation must win: {other:?}"),
    };
    assert_eq!(winner.value(), 1);
    assert!(error.is::<SessionAllocationExhausted>(), "{error:#}");
    assert!(!error.to_string().contains("database unavailable"));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_session_reservations")
        .fetch_one(&a.pool)
        .await?;
    assert_eq!(count, 1, "failed contender must not release the winner");
    winner.release().await?;
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn only_stopped_owners_are_reclaimed_and_old_cleanup_cannot_release_a_replacement(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let held = a.new_session_id().await?;
    assert_eq!(held.value(), 1);
    sqlx::query("UPDATE qbit_prism_instances SET heartbeat_at=clock_timestamp()-interval '2 days' WHERE instance_id='a'")
        .execute(&a.pool).await?;
    wrap(&a.pool).await?;
    let skipped = b.new_session_id().await?;
    assert_eq!(
        skipped.value(),
        2,
        "stale heartbeats do not prove session exit"
    );
    skipped.release().await?;
    sqlx::query("DELETE FROM qbit_prism_instances WHERE instance_id='a'")
        .execute(&a.pool)
        .await?;
    wrap(&a.pool).await?;
    let skipped = b.new_session_id().await?;
    assert_eq!(
        skipped.value(),
        2,
        "absent owners do not prove session exit"
    );
    skipped.release().await?;
    let (owner_token, old_token): (String, String) = sqlx::query_as("SELECT owner_token,reservation_token FROM qbit_prism_session_reservations WHERE extranonce1=1")
        .fetch_one(&a.pool).await?;
    assert!(a.heartbeat(json!({"state":"stopped"})).await.is_err());
    held.release().await?;
    // Recreate a missed cleanup after the actual guard has ended. A stopped
    // heartbeat must never be published while that live guard still exists.
    sqlx::query("INSERT INTO qbit_prism_session_reservations(extranonce1,instance_id,owner_token,reservation_token) VALUES(1,'a',$1,$2)")
        .bind(owner_token).bind(&old_token).execute(&a.pool).await?;
    a.heartbeat(json!({"state":"stopped"})).await?;
    seed_job(&a.pool, "stopped-live-job", "00000001", true).await?;
    wrap(&a.pool).await?;
    let skipped = b.new_session_id().await?;
    assert_eq!(
        skipped.value(),
        2,
        "stopped-owner reclaim must still check jobs"
    );
    skipped.release().await?;
    sqlx::query("UPDATE qbit_prism_jobs SET expires_at=clock_timestamp()-interval '1 second'")
        .execute(&a.pool)
        .await?;
    wrap(&a.pool).await?;
    let replacement = b.new_session_id().await?;
    assert_eq!(replacement.value(), 1);
    let late_cleanup = sqlx::query(
        "DELETE FROM qbit_prism_session_reservations WHERE extranonce1=1 AND reservation_token=$1",
    )
    .bind(old_token)
    .execute(&a.pool)
    .await?
    .rows_affected();
    assert_eq!(late_cleanup, 0);
    let owner: String = sqlx::query_scalar(
        "SELECT instance_id FROM qbit_prism_session_reservations WHERE extranonce1=1",
    )
    .fetch_one(&b.pool)
    .await?;
    assert_eq!(
        owner, "b",
        "late cleanup cannot delete a replacement's token"
    );
    replacement.release().await?;
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn stopped_requires_no_pending_or_live_sessions_and_cannot_reclaim_another_incarnation(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("reused-instance-id").await?;
    let held = a.new_session_id().await?;
    let other_incarnation = db.ledger("reused-instance-id").await?;
    let allocator = db.ledger("allocator").await?;
    assert!(
        a.heartbeat(json!({"state":"stopped"})).await.is_err(),
        "a live session prevents stopped"
    );
    assert!(
        a.clone()
            .heartbeat(json!({"state":"stopped"}))
            .await
            .is_err(),
        "Ledger clones share admission state"
    );
    other_incarnation
        .heartbeat(json!({"state":"stopped"}))
        .await?;
    wrap(&a.pool).await?;
    let next = allocator.new_session_id().await?;
    assert_eq!(next.value(),2,"stopped from a second process with the same instance ID cannot reclaim the first process's session");
    next.release().await?;
    held.release().await?;

    // Poll allocation until it waits on a saturated real PostgreSQL pool,
    // retaining the pending future across timeout instead of cancelling it.
    let mut connections = Vec::new();
    for _ in 0..8 {
        connections.push(a.pool.acquire().await?);
    }
    let mut pending = Box::pin(a.new_session_id());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), pending.as_mut())
            .await
            .is_err()
    );
    assert!(
        a.heartbeat(json!({"state":"stopped"})).await.is_err(),
        "an allocation awaiting a connection also prevents stopped"
    );
    drop(pending);
    drop(connections);
    a.heartbeat(json!({"state":"stopped"})).await?;
    let error = a.new_session_id().await.unwrap_err();
    assert!(
        error.to_string().contains("allocator is stopped"),
        "stopped closes future admission"
    );
    db.close(vec![a, other_incarnation, allocator]).await
}

#[tokio::test]
async fn cancelling_a_session_releases_only_its_reservation() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let held = b.new_session_id().await?;
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let worker = a.clone();
    let session = tokio::spawn(async move {
        let id = worker.new_session_id().await.unwrap();
        ready_tx.send(id.value()).unwrap();
        std::future::pending::<()>().await;
        drop(id);
    });
    let cancelled = ready_rx.await?;
    session.abort();
    assert!(session.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM qbit_prism_session_reservations WHERE extranonce1=$1)",
            )
            .bind(i64::from(cancelled))
            .fetch_one(&a.pool)
            .await?;
            if !exists {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let remaining: Vec<i64> =
        sqlx::query_scalar("SELECT extranonce1 FROM qbit_prism_session_reservations")
            .fetch_all(&a.pool)
            .await?;
    assert_eq!(remaining, vec![i64::from(held.value())]);
    held.release().await?;
    db.close(vec![a, b]).await
}
