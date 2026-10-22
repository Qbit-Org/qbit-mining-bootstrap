//! Session reservation cleanup checkout boundaries (#352), on disposable PG16.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{ledger::Ledger, metrics::Metrics};
use qbit_prism_test_gate as gate;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{sync::Arc, time::Duration};

#[allow(dead_code)]
#[path = "support/ledger_database.rs"]
mod ledger_database;
use ledger_database::FixtureDatabase;

const WAIT: Duration = Duration::from_secs(10);
const CANCEL: Duration = Duration::from_millis(100);

fn sample(metrics: &Metrics, result: &str, suffix: &str) -> f64 {
    let key = format!("qbit_prism_database_pool_acquire_seconds_{suffix}{{result=\"{result}\"}} ");
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&key))
        .expect("pool metric series")
        .parse()
        .unwrap()
}

fn counts(metrics: &Metrics) -> (f64, f64) {
    (
        sample(metrics, "success", "count"),
        sample(metrics, "failure", "count"),
    )
}

async fn wait_counts(metrics: &Metrics, expected: (f64, f64)) -> Result<()> {
    tokio::time::timeout(WAIT, async {
        while counts(metrics) != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("cleanup did not reach expected checkout boundary")
}

async fn present(pool: &PgPool, id: u32) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT FROM qbit_prism_session_reservations WHERE extranonce1=$1)",
    )
    .bind(i64::from(id))
    .fetch_one(pool)
    .await?)
}

async fn gone(pool: &PgPool, id: u32) -> Result<()> {
    tokio::time::timeout(WAIT, async {
        while present(pool, id).await? {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        anyhow::Ok(())
    })
    .await
    .context("reservation cleanup did not finish")?
}

async fn ledger(db: &FixtureDatabase, metrics: Option<Arc<Metrics>>) -> Result<Ledger> {
    let mut ledger =
        Ledger::connect_with_metrics(&db.url, "session-metrics".into(), 2, true, metrics).await?;
    // One slot makes retained connections and pending cleanups observable.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(WAIT)
        .connect(&db.url)
        .await?;
    let old = std::mem::replace(&mut ledger.pool, pool);
    old.close().await;
    Ok(ledger)
}

async fn database(raw: &str) -> Result<FixtureDatabase> {
    let db = FixtureDatabase::open(raw, "prism_session_metrics_").await?;
    let (version, fsync, full_page_writes): (i32, String, String) = sqlx::query_as(
        "SELECT current_setting('server_version_num')::int,current_setting('fsync'),current_setting('full_page_writes')")
        .fetch_one(&db.admin).await?;
    ensure!(
        version / 10_000 == 16 && fsync == "on" && full_page_writes == "on",
        "requires durable PostgreSQL 16"
    );
    Ok(db)
}

#[tokio::test]
async fn release_and_drop_count_once_and_preserve_replacement_tokens() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = database(&raw).await?;
    let result = async {
        let metrics = Arc::new(Metrics::default());
        let ledger = ledger(&db, Some(metrics.clone())).await?;
        for explicit in [true, false] {
            let session = ledger.new_session_id().await?;
            let id = session.value();
            let other = ledger.new_session_id().await?;
            let before = counts(&metrics);
            if explicit { session.release().await?; } else { drop(session); }
            gone(&ledger.pool, id).await?;
            assert_eq!(counts(&metrics), (before.0 + 1., before.1));
            assert!(present(&ledger.pool, other.value()).await?);
            other.release().await?;

            let stale = ledger.new_session_id().await?;
            let id = stale.value();
            sqlx::query("UPDATE qbit_prism_session_reservations SET reservation_token='replacement' WHERE extranonce1=$1")
                .bind(i64::from(id)).execute(&ledger.pool).await?;
            let before = counts(&metrics);
            if explicit { stale.release().await?; } else { drop(stale); }
            wait_counts(&metrics, (before.0 + 1., before.1)).await?;
            // Queue behind the cleanup's single checkout, so its DELETE has finished.
            let connection = ledger.pool.acquire().await?;
            drop(connection);
            assert!(present(&ledger.pool, id).await?, "old token released replacement");
        }
        let before = counts(&metrics);
        ledger.release_session_owner_reservations().await?;
        assert_eq!(counts(&metrics), (before.0 + 1., before.1), "owner release is already timed once");
        ledger.pool.close().await;
        Ok(())
    }.await;
    db.close(result).await
}

#[tokio::test]
async fn unpolled_and_cancelled_release_keep_the_drop_fallback_and_recover() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = database(&raw).await?;
    let result = async {
        let metrics = Arc::new(Metrics::default());
        let ledger = ledger(&db, Some(metrics.clone())).await?;
        let side = PgPool::connect(&db.url).await?;
        for cancel in [false, true] {
            let session = ledger.new_session_id().await?;
            let id = session.value();
            let held = ledger.pool.acquire().await?;
            let before = counts(&metrics);
            let sum = sample(&metrics, "failure", "sum");
            let release = session.release();
            // This runtime is current-thread: no spawned cleanup can poll between assertions.
            assert_eq!(counts(&metrics), before);
            if cancel {
                assert!(tokio::time::timeout(CANCEL, release).await.is_err());
                assert!(sample(&metrics, "failure", "sum") - sum >= 0.05);
            } else {
                drop(release);
            }
            let failures = before.1 + if cancel { 1. } else { 0. };
            assert_eq!(counts(&metrics), (before.0, failures));
            assert!(
                present(&side, id).await?,
                "waiting cleanup released a live reservation"
            );
            drop(held);
            gone(&side, id).await?;
            assert_eq!(
                counts(&metrics),
                (before.0 + 1., failures),
                "fallback owns a separate checkout"
            );
            let recovered = ledger.new_session_id().await?;
            recovered.release().await?;
        }
        side.close().await;
        ledger.pool.close().await;
        Ok(())
    }
    .await;
    db.close(result).await
}

#[tokio::test]
async fn sql_wait_and_errors_remain_successful_checkouts() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = database(&raw).await?;
    let result = async {
        let metrics = Arc::new(Metrics::default());
        let ledger = ledger(&db, Some(metrics.clone())).await?;
        let side = PgPool::connect(&db.url).await?;
        for explicit in [true, false] {
            let session = ledger.new_session_id().await?;
            let id = session.value();
            let mut lock = side.begin().await?;
            sqlx::query("LOCK TABLE qbit_prism_session_reservations IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await?;
            let before = counts(&metrics);
            let task = if explicit {
                Some(tokio_util::task::AbortOnDropHandle::new(tokio::spawn(session.release())))
            } else { drop(session); None };
            wait_counts(&metrics, (before.0 + 1., before.1)).await?;
            let measured = sample(&metrics, "success", "sum");
            tokio::time::sleep(CANCEL).await;
            assert_eq!(sample(&metrics, "success", "sum"), measured, "SQL lock wait entered checkout duration");
            if let Some(task) = &task { assert!(!task.is_finished()); }
            lock.rollback().await?;
            if let Some(task) = task { task.await??; }
            gone(&side, id).await?;
            assert_eq!(counts(&metrics), (before.0 + 1., before.1));
            assert_eq!(sample(&metrics, "success", "sum"), measured);

            let session = ledger.new_session_id().await?;
            let id = session.value();
            // A real SQL error after checkout, shared by explicit and fallback DELETEs.
            sqlx::query("ALTER TABLE qbit_prism_session_reservations RENAME COLUMN reservation_token TO hidden_token").execute(&side).await?;
            let before = counts(&metrics);
            if explicit {
                let error = session.release().await.unwrap_err();
                assert_eq!(error.downcast_ref::<sqlx::Error>().and_then(sqlx::Error::as_database_error).and_then(|e| e.code()).as_deref(), Some("42703"));
            } else { drop(session); }
            let expected = (before.0 + if explicit { 2. } else { 1. }, before.1);
            wait_counts(&metrics, expected).await?;
            // Wait for the last SQL attempt, before restoring the column.
            let connection = ledger.pool.acquire().await?;
            drop(connection);
            assert!(present(&side, id).await?);
            assert_eq!(counts(&metrics), expected, "SQL failure relabelled a checkout");
            sqlx::query("ALTER TABLE qbit_prism_session_reservations RENAME COLUMN hidden_token TO reservation_token").execute(&side).await?;
            ledger.release_session_owner_reservations().await?;
            assert!(!present(&side, id).await?);
        }
        side.close().await;
        ledger.pool.close().await;
        Ok(())
    }.await;
    db.close(result).await
}

#[tokio::test]
async fn closed_pool_failures_retain_reservations_and_unattached_cleanup_recovers() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = database(&raw).await?;
    let result = async {
        let metrics = Arc::new(Metrics::default());
        let mut ledger = ledger(&db, Some(metrics.clone())).await?;
        let explicit = ledger.new_session_id().await?;
        let dropped = ledger.new_session_id().await?;
        let ids = [explicit.value(), dropped.value()];
        ledger.pool.close().await;
        let before = counts(&metrics);
        let error = explicit.release().await.unwrap_err();
        assert!(matches!(
            error.downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::PoolClosed)
        ));
        drop(dropped);
        // Explicit error, its fallback, and ordinary Drop each attempt a checkout.
        wait_counts(&metrics, (before.0, before.1 + 3.)).await?;
        ledger.pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&db.url)
            .await?;
        for id in ids {
            assert!(present(&ledger.pool, id).await?);
        }
        ledger.release_session_owner_reservations().await?;
        for id in ids {
            assert!(!present(&ledger.pool, id).await?);
        }
        ledger.pool.close().await;

        // No-metrics control exercises both results and cancellation fallback.
        let ledger = self::ledger(&db, None).await?;
        for explicit in [true, false] {
            let session = ledger.new_session_id().await?;
            let id = session.value();
            if explicit {
                session.release().await?;
            } else {
                drop(session);
            }
            gone(&ledger.pool, id).await?;
        }
        let session = ledger.new_session_id().await?;
        let id = session.value();
        let held = ledger.pool.acquire().await?;
        assert!(tokio::time::timeout(CANCEL, session.release())
            .await
            .is_err());
        drop(held);
        gone(&ledger.pool, id).await?;
        let session = ledger.new_session_id().await?;
        ledger.pool.close().await;
        assert!(matches!(
            session
                .release()
                .await
                .unwrap_err()
                .downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::PoolClosed)
        ));
        Ok(())
    }
    .await;
    db.close(result).await
}

#[test]
fn runtime_shutdown_counts_only_polled_background_cleanup() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
    };
    let outer = runtime()?;
    let db = outer.block_on(database(&raw))?;
    let result = (|| -> Result<()> {
        for polled in [false, true] {
            let cleanup_runtime = runtime()?;
            let metrics = Arc::new(Metrics::default());
            let mut ledger = cleanup_runtime.block_on(ledger(&db, Some(metrics.clone())))?;
            let session = cleanup_runtime.block_on(ledger.new_session_id())?;
            let id = session.value();
            let held = cleanup_runtime.block_on(ledger.pool.acquire())?;
            let before = counts(&metrics);
            let sum = sample(&metrics, "failure", "sum");
            {
                let _entered = cleanup_runtime.enter();
                drop(session);
            }
            if polled {
                cleanup_runtime.block_on(async {
                    tokio::time::sleep(CANCEL).await;
                });
            }
            assert_eq!(
                counts(&metrics),
                before,
                "pending or unpolled cleanup counted early"
            );
            // Shutting down aborts the private runtime's spawned cleanup.
            drop(cleanup_runtime);
            assert_eq!(
                counts(&metrics),
                (before.0, before.1 + if polled { 1. } else { 0. })
            );
            if polled {
                assert!(sample(&metrics, "failure", "sum") - sum >= 0.05);
            }
            outer.block_on(async {
                // Its socket belonged to the stopped reactor; discard it instead
                // of spawning SQLx's return-to-pool validation on a new runtime.
                drop(held.detach());
                // A new pool recovers without recycling the retained reservation.
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&db.url)
                    .await?;
                let old = std::mem::replace(&mut ledger.pool, pool);
                tokio::time::timeout(WAIT, old.close()).await?;
                assert!(present(&ledger.pool, id).await?);
                ledger.release_session_owner_reservations().await?;
                assert!(!present(&ledger.pool, id).await?);
                ledger.pool.close().await;
                anyhow::Ok(())
            })?;
        }
        Ok(())
    })();
    outer.block_on(db.close(result))
}
