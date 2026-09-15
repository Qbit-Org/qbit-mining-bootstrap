//! Checkout observations at ledger-owned job reads and expiry (#352).
use super::test_support::{counts, family, sample, with_database};
use super::*;
use crate::metrics::Metrics;
use anyhow::{Context, Result};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{sync::Arc, time::Duration};
use tokio_util::task::AbortOnDropHandle;

const WAIT: Duration = Duration::from_secs(10);

struct Database {
    side: PgPool,
    ledger: Ledger,
    metrics: Arc<Metrics>,
}

impl Database {
    async fn open(url: &str, pools: &mut Vec<PgPool>) -> Result<Self> {
        let metrics = Arc::new(Metrics::default());
        let mut ledger =
            Ledger::connect_with_metrics(url, "job-acquire".into(), 2, true, Some(metrics.clone()))
                .await?;
        pools.push(ledger.pool.clone());
        // Isolate checkout timing. Production after_connect deadlines are
        // covered elsewhere; table-lock waits here are explicitly controlled.
        let single = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(WAIT)
            .connect(url)
            .await?;
        pools.push(single.clone());
        let old = std::mem::replace(&mut ledger.pool, single);
        old.close().await;
        let side = PgPool::connect(url).await?;
        pools.push(side.clone());
        Ok(Self {
            side,
            ledger,
            metrics,
        })
    }
}

// An assertion in the paused-clock block must resume time before the shared
// fixture awaits database cleanup while propagating the panic.
struct ResumeClock;

impl Drop for ResumeClock {
    fn drop(&mut self) {
        tokio::time::resume();
    }
}

#[derive(Clone, Copy, Debug)]
enum Caller {
    Job,
    Compact,
    Prune,
}

impl Caller {
    const ALL: [Self; 3] = [Self::Job, Self::Compact, Self::Prune];

    async fn run(self, ledger: &Ledger) -> Result<()> {
        match self {
            Self::Job => assert!(ledger.job("missing").await?.is_none()),
            Self::Compact => assert!(ledger.compact_prepared("missing").await?.is_none()),
            Self::Prune => assert_eq!(ledger.prune_expired_jobs().await?, 0),
        }
        Ok(())
    }
}

async fn wait_for_counts(metrics: &Metrics, expected: (f64, f64)) -> Result<()> {
    tokio::time::timeout(WAIT, async {
        while counts(metrics) != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("caller did not reach checkout boundary")
}

#[tokio::test]
async fn postgres_job_checkouts_start_on_first_poll_and_recover() -> Result<()> {
    with_database(|url, pools| {
        Box::pin(async move { checkout_cases(&Database::open(url, pools).await?).await })
    })
    .await
}

async fn checkout_cases(db: &Database) -> Result<()> {
    let (ledger, metrics) = (&db.ledger, db.metrics.as_ref());
    for caller in Caller::ALL {
        let held = ledger.pool.acquire().await?;
        let before = counts(metrics);
        let failure_sum = sample(metrics, "failure", "sum");
        // Exercise the real caller while controlling only its checkout clock.
        // Dropping the async caller unpolled checks its lazy outer boundary;
        // the polled cancellation below checks the actual checkout clock.
        tokio::time::pause();
        let resume_clock = ResumeClock;
        let unpolled = caller.run(ledger);
        tokio::time::advance(Duration::from_secs(60)).await;
        drop(unpolled);
        assert_eq!(counts(metrics), before, "{caller:?}: unpolled");
        let mut acquiring = Box::pin(caller.run(ledger));
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(futures_util::poll!(&mut acquiring).is_pending());
        assert_eq!(counts(metrics), before, "{caller:?}: pending");
        tokio::time::advance(Duration::from_millis(75)).await;
        drop(acquiring);
        drop(resume_clock);
        assert_eq!(
            counts(metrics),
            (before.0, before.1 + 1.),
            "{caller:?}: cancellation"
        );
        let elapsed = sample(metrics, "failure", "sum") - failure_sum;
        assert!((elapsed - 0.075).abs() < 0.000001, "{caller:?}: {elapsed}");
        drop(held);
        tokio::time::timeout(WAIT, caller.run(ledger)).await??;
        assert_eq!(
            counts(metrics),
            (before.0 + 1., before.1 + 1.),
            "{caller:?}: recovery"
        );

        let held = ledger.pool.acquire().await?;
        let before = counts(metrics);
        let success_sum = sample(metrics, "success", "sum");
        let mut acquiring = Box::pin(caller.run(ledger));
        assert!(futures_util::poll!(&mut acquiring).is_pending());
        let started = std::time::Instant::now();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let waited = started.elapsed();
        assert_eq!(counts(metrics), before);
        drop(held);
        tokio::time::timeout(WAIT, acquiring).await??;
        assert_eq!(
            counts(metrics),
            (before.0 + 1., before.1),
            "{caller:?}: success"
        );
        assert!(sample(metrics, "success", "sum") - success_sum >= waited.as_secs_f64());
        // The only slot is returned at the statement boundary.
        let tx = tokio::time::timeout(WAIT, ledger.begin()).await??;
        tx.rollback().await?;
        assert_eq!(counts(metrics), (before.0 + 2., before.1));
    }
    // Intentional failure injection, not fixture cleanup. The shared owner
    // later closes every registered pool (close is idempotent).
    ledger.pool.close().await;
    for caller in Caller::ALL {
        let before = counts(metrics);
        let error = caller.run(ledger).await.unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<sqlx::Error>(),
                Some(sqlx::Error::PoolClosed)
            ),
            "{caller:?}: {error:#}"
        );
        assert_eq!(
            counts(metrics),
            (before.0, before.1 + 1.),
            "{caller:?}: pool failure"
        );
    }
    Ok(())
}

#[tokio::test]
async fn postgres_job_sql_wait_errors_and_cancellation_do_not_change_checkout_samples() -> Result<()>
{
    with_database(|url, pools| {
        Box::pin(async move { sql_cases(&Database::open(url, pools).await?).await })
    })
    .await
}

async fn sql_cases(db: &Database) -> Result<()> {
    let (ledger, metrics) = (&db.ledger, db.metrics.as_ref());
    for caller in Caller::ALL {
        for cancel in [false, true] {
            let mut lock = db.side.begin().await?;
            sqlx::query("LOCK TABLE qbit_prism_jobs IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *lock)
                .await?;
            let before = counts(metrics);
            let task = AbortOnDropHandle::new(tokio::spawn({
                let ledger = ledger.clone();
                async move { caller.run(&ledger).await }
            }));
            wait_for_counts(metrics, (before.0 + 1., before.1)).await?;
            let acquired = family(metrics);
            tokio::time::sleep(Duration::from_millis(75)).await;
            assert!(!task.is_finished(), "{caller:?}: SQL must still be blocked");
            assert_eq!(family(metrics), acquired, "{caller:?}: SQL wait");
            if cancel {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
                lock.rollback().await?;
            } else {
                lock.rollback().await?;
                tokio::time::timeout(WAIT, task).await???;
            }
            assert_eq!(family(metrics), acquired, "{caller:?}: SQL outcome");
            tokio::time::timeout(WAIT, caller.run(ledger)).await??;
            assert_eq!(
                counts(metrics),
                (before.0 + 2., before.1),
                "{caller:?}: recovery"
            );
        }
        // Keep the original SQL error and its already-successful checkout.
        sqlx::query("ALTER TABLE qbit_prism_jobs RENAME COLUMN expires_at TO hidden_expiry")
            .execute(&db.side)
            .await?;
        let before = counts(metrics);
        let error = caller.run(ledger).await.unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<sqlx::Error>()
                .and_then(sqlx::Error::as_database_error)
                .and_then(|error| error.code())
                .as_deref(),
            Some("42703"),
            "{caller:?}: {error:#}"
        );
        assert_eq!(
            counts(metrics),
            (before.0 + 1., before.1),
            "{caller:?}: SQL error"
        );
        sqlx::query("ALTER TABLE qbit_prism_jobs RENAME COLUMN hidden_expiry TO expires_at")
            .execute(&db.side)
            .await?;
        tokio::time::timeout(WAIT, caller.run(ledger)).await??;
        assert_eq!(counts(metrics), (before.0 + 2., before.1));
    }
    Ok(())
}

#[test]
fn postgres_compact_read_releases_checkout_before_blocking_decode() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    runtime.block_on(with_database(|url, pools| {
        Box::pin(async move { decode_case(&Database::open(url, pools).await?).await })
    }))
}

async fn decode_case(db: &Database) -> Result<()> {
    let (ledger, metrics) = (&db.ledger, db.metrics.as_ref());
    let payload = serde_json::json!({"snapshot": {}, "template": {}});
    ledger.save_job("legacy", &payload, 0, "parent", 60).await?;
    // Occupy the sole blocking thread so the real reader must await decode.
    // Dropping the sender unblocks it even if a later assertion fails.
    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let (ready, started) = tokio::sync::oneshot::channel();
    let blocker = tokio::task::spawn_blocking(move || {
        let _ = ready.send(());
        let _ = blocked.recv_timeout(WAIT);
    });
    started.await?;
    let before = counts(metrics);
    let task = AbortOnDropHandle::new(tokio::spawn({
        let ledger = ledger.clone();
        async move { ledger.compact_prepared("legacy").await }
    }));
    wait_for_counts(metrics, (before.0 + 1., before.1)).await?;
    let acquired = family(metrics);
    let mut connection =
        tokio::time::timeout(Duration::from_secs(2), ledger.pool.acquire()).await??;
    sqlx::query("SELECT 1").execute(&mut *connection).await?;
    assert!(
        !task.is_finished(),
        "reader must still await the blocked decoder"
    );
    assert_eq!(family(metrics), acquired);
    drop(connection);
    drop(release);
    blocker.await?;
    assert!(tokio::time::timeout(WAIT, task).await???.is_none());
    assert_eq!(family(metrics), acquired);
    assert_eq!(
        tokio::time::timeout(WAIT, ledger.job("legacy")).await??,
        Some(payload)
    );
    assert_eq!(counts(metrics), (before.0 + 2., before.1));
    Ok(())
}
