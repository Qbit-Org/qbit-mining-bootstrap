//! Exercise the actual heartbeat branches with one pool slot and inert work.
use super::*;
use futures_util::{future::LocalBoxFuture, FutureExt};
use sqlx::postgres::PgPoolOptions;
use std::panic::{resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};

const BOUND: Duration = Duration::from_secs(5);
const LEASE: CandidateLease = CandidateLease {
    seconds: 10,
    interval: Duration::from_millis(100),
    timeout: Duration::from_secs(2),
    rebuild_deadline: Duration::from_secs(60),
};

fn counts(metrics: &crate::metrics::Metrics) -> (f64, f64) {
    let body = metrics.render();
    let count = |result| {
        let key = format!("qbit_prism_database_pool_acquire_seconds_count{{result=\"{result}\"}} ");
        body.lines()
            .find_map(|line| line.strip_prefix(&key))
            .unwrap()
            .parse()
            .unwrap()
    };
    (count("success"), count("failure"))
}

struct Attempt(JoinHandle<Result<()>>, Arc<AtomicBool>);
impl Drop for Attempt {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct WorkDropped(Arc<AtomicBool>);
impl Drop for WorkDropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn start(fixture: &Fixture) -> Result<Attempt> {
    let ready = Arc::new(Notify::new());
    let entered = ready.clone();
    let coordinator = fixture.coordinator.clone();
    let claim = fixture.claim.clone();
    let dropped = Arc::new(AtomicBool::new(false));
    let observed = dropped.clone();
    let mut attempt = Attempt(
        tokio::spawn(async move {
            coordinator
                .with_candidate_heartbeat(&claim, LEASE, async {
                    let _dropped = WorkDropped(dropped);
                    entered.notify_one();
                    std::future::pending::<Result<()>>().await
                })
                .await
        }),
        observed,
    );
    tokio::select! {
        result = &mut attempt.0 => panic!("initial renewal failed: {result:?}"),
        result = tokio::time::timeout(BOUND, ready.notified()) => result?,
    }
    Ok(attempt)
}

async fn with_fixture<F>(case: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a Fixture) -> LocalBoxFuture<'a, Result<()>>,
{
    let _serial = TEST_LOCK.lock().await;
    let Some(mut fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = AssertUnwindSafe(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(3))
            .after_connect(|connection, _| {
                Box::pin(async move {
                    // Test-only SQL contention returns 55P03 before the lease
                    // timeout; the live-token reconciliation must then run.
                    sqlx::query("SET lock_timeout='500ms'")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(
                fixture
                    .coordinator
                    .ledger
                    .pool
                    .connect_options()
                    .as_ref()
                    .clone(),
            )
            .await?;
        let coordinator = Arc::get_mut(&mut fixture.coordinator)
            .context("fixture coordinator is shared before test")?;
        let ledger = Arc::make_mut(&mut coordinator.ledger);
        let old = std::mem::replace(&mut ledger.pool, pool);
        old.close().await;
        case(&fixture).await
    })
    .catch_unwind()
    .await;
    let cleanup = fixture.close().await;
    match result {
        Ok(result) => result.and(cleanup),
        Err(panic) => {
            if let Err(error) = cleanup {
                eprintln!("candidate checkout fixture cleanup failed: {error}");
            }
            resume_unwind(panic)
        }
    }
}

async fn block_renewal(fixture: &Fixture) -> Result<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut blocker = fixture.successor.pool.begin().await?;
    sqlx::query(
        "SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR UPDATE",
    )
    .bind(&fixture.claim.candidate.block_hash)
    .execute(&mut *blocker)
    .await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    tokio::time::timeout(BOUND, async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%FOR NO KEY UPDATE%')")
                .bind(pid).fetch_one(&fixture.admin).await?;
            if waiting { return Ok::<_, anyhow::Error>(()); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.context("heartbeat did not reach renewal row lock")??;
    Ok(blocker)
}

async fn reusable(pool: &PgPool) -> Result<()> {
    let value: i32 =
        tokio::time::timeout(BOUND, sqlx::query_scalar("SELECT 42").fetch_one(pool)).await??;
    assert_eq!(value, 42);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_token_checkout_cancel_and_closed_pool_keep_renewal_failure() -> Result<()> {
    for close in [false, true] {
        with_fixture(|fixture| {
            Box::pin(async move {
                let pool = &fixture.coordinator.ledger.pool;
                let metrics = &fixture.coordinator.metrics;
                let before = counts(metrics);
                let mut attempt = start(fixture).await?;
                let blocker = block_renewal(fixture).await?;
                // Queue ahead of the live-token probe while the renewal owns the
                // only slot. Its SQL lock timeout releases that slot to this waiter.
                let mut next = Box::pin(pool.acquire());
                assert!(futures_util::poll!(&mut next).is_pending());
                let held = tokio::time::timeout(BOUND, next).await??;
                tokio::time::sleep(Duration::from_millis(40)).await;
                assert!(!attempt.0.is_finished());
                assert!(
                    !attempt.1.load(Ordering::SeqCst),
                    "live-token probe already abandoned work"
                );
                assert_eq!(counts(metrics), (before.0 + 2., before.1));
                if close {
                    let mut closing = Box::pin(pool.close());
                    assert!(futures_util::poll!(&mut closing).is_pending());
                    let error = tokio::time::timeout(BOUND, &mut attempt.0)
                        .await??
                        .unwrap_err();
                    assert_eq!(
                        error
                            .downcast_ref::<sqlx::Error>()
                            .and_then(sqlx::Error::as_database_error)
                            .and_then(|error| error.code())
                            .as_deref(),
                        Some("55P03")
                    );
                    // Live-token checkout fails, then terminal checkout fails;
                    // neither replaces the original renewal-contention error.
                    assert_eq!(counts(metrics), (before.0 + 2., before.1 + 2.));
                    drop(held);
                    tokio::time::timeout(BOUND, closing).await?;
                } else {
                    attempt.0.abort();
                    assert!(tokio::time::timeout(BOUND, &mut attempt.0)
                        .await?
                        .unwrap_err()
                        .is_cancelled());
                    assert_eq!(counts(metrics), (before.0 + 2., before.1 + 1.));
                    drop(held);
                    reusable(pool).await?;
                }
                blocker.rollback().await?;
                let state: String = sqlx::query_scalar(
                    "SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1",
                )
                .bind(&fixture.claim.candidate.block_hash)
                .fetch_one(&fixture.successor.pool)
                .await?;
                assert_eq!(state, "pending");
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_checkout_cancel_releases_slot_without_resuming_work() -> Result<()> {
    with_fixture(|fixture| Box::pin(async move {
        let pool = &fixture.coordinator.ledger.pool;
        let metrics = &fixture.coordinator.metrics;
        let before = counts(metrics);
        let mut attempt = start(fixture).await?;
        let mut blocker = block_renewal(fixture).await?;
        let mut next = Box::pin(pool.acquire());
        assert!(futures_util::poll!(&mut next).is_pending());
        sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_token='replacement-token' WHERE block_hash=$1")
            .bind(&fixture.claim.candidate.block_hash).execute(&mut *blocker).await?;
        blocker.commit().await?;
        let held = tokio::time::timeout(BOUND, next).await??;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(!attempt.0.is_finished());
        assert!(attempt.1.load(Ordering::SeqCst), "terminal probe has not dropped the lost work");
        assert_eq!(counts(metrics), (before.0 + 2., before.1));
        attempt.0.abort();
        assert!(tokio::time::timeout(BOUND, &mut attempt.0).await?.unwrap_err().is_cancelled());
        assert_eq!(counts(metrics), (before.0 + 2., before.1 + 1.));
        drop(held);
        reusable(pool).await?;
        let row = fixture.row().await?;
        assert_eq!(row.state, "pending");
        assert_eq!(row.token.as_deref(), Some("replacement-token"));
        assert_eq!(fixture.submissions().await, 0);
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_sql_failure_preserves_successful_checkouts() -> Result<()> {
    with_fixture(|fixture| {
        Box::pin(async move {
            let metrics = &fixture.coordinator.metrics;
            let before = counts(metrics);
            let mut attempt = start(fixture).await?;
            sqlx::query("DROP TABLE qbit_block_candidate_outbox CASCADE")
                .execute(&fixture.successor.pool)
                .await?;
            let error = tokio::time::timeout(BOUND, &mut attempt.0)
                .await??
                .unwrap_err();
            assert_eq!(
                error
                    .downcast_ref::<sqlx::Error>()
                    .and_then(sqlx::Error::as_database_error)
                    .and_then(|error| error.code())
                    .as_deref(),
                Some("42P01")
            );
            assert_eq!(counts(metrics), (before.0 + 3., before.1));
            reusable(&fixture.coordinator.ledger.pool).await?;
            assert_eq!(fixture.submissions().await, 0);
            Ok(())
        })
    })
    .await
}
