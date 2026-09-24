//! The idle-gated checkout probe against a real PostgreSQL: a connection whose
//! backend died while idle is replaced silently once the gap reaches the
//! threshold, and inside the threshold it fails exactly one statement and is
//! then discarded, so the following checkout gets a live connection.
use super::test_support::with_database;
use super::*;
use anyhow::{ensure, Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

async fn backend_pid(pool: &PgPool) -> Result<i32> {
    Ok(sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(pool)
        .await?)
}

/// Terminate the pooled backend from a separate session and wait until the
/// server no longer lists it, so the client-side socket has been closed.
async fn terminate(admin: &PgPool, pid: i32) -> Result<()> {
    let killed: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(admin)
        .await?;
    ensure!(killed, "pg_terminate_backend({pid}) returned false");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let alive: Option<i32> =
                sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE pid=$1")
                    .bind(pid)
                    .fetch_optional(admin)
                    .await?;
            if alive.is_none() {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("terminated backend still listed after 5 s")??;
    // Let the FIN reach the client socket before the next checkout.
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

/// Wait until the pool's one connection is back on the idle queue, so the
/// backend can be terminated while the socket is idle rather than mid-ping.
///
/// A dropped `PoolConnection` is returned by a task SQLx spawns, which pings
/// the server first and only then makes the connection idle. Right after
/// [`backend_pid`] that ping can still be in flight; a backend terminated then
/// fails the ping, SQLx discards the connection on the spot, and the checkout
/// under test opens a fresh one instead of receiving the dead socket, so the
/// statement succeeds on a new pid.
async fn settled(pool: &PgPool) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.num_idle() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("the pooled connection was not returned to the idle queue within 5 s")?;
    ensure!(
        pool.size() == 1,
        "the pool holds {} connections, not the one under test",
        pool.size()
    );
    Ok(())
}

/// One slot, so the checkout under test is the only connection the pool has.
fn probed_pool(idle_at_least: Duration) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .test_before_acquire(false)
        .before_acquire(move |connection, metadata| {
            probe_before_acquire(connection, metadata, idle_at_least)
        })
}

#[tokio::test]
async fn a_dead_idle_connection_is_replaced_when_the_idle_gap_reaches_the_threshold() -> Result<()>
{
    with_database(|url, pools| {
        Box::pin(async move {
            let pool = probed_pool(Duration::ZERO).connect(url).await?;
            pools.push(pool.clone());
            let admin = PgPool::connect(url).await?;
            pools.push(admin.clone());
            let first = backend_pid(&pool).await?;
            settled(&pool).await?;
            terminate(&admin, first).await?;
            ensure!(
                pool.num_idle() == 1,
                "the dead connection left the pool before the checkout"
            );
            // Threshold zero: every checkout probes, so the dead connection is
            // discarded and the statement runs on a fresh one without an error.
            let second = tokio::time::timeout(Duration::from_secs(5), backend_pid(&pool))
                .await
                .context("checkout after a terminated backend hung")??;
            ensure!(
                second != first,
                "the dead backend {first} was handed out again"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn a_dead_idle_connection_inside_the_probe_window_fails_one_statement_then_recovers(
) -> Result<()> {
    with_database(|url, pools| {
        Box::pin(async move {
            let pool = probed_pool(Duration::from_secs(3600)).connect(url).await?;
            pools.push(pool.clone());
            let admin = PgPool::connect(url).await?;
            pools.push(admin.clone());
            let first = backend_pid(&pool).await?;
            settled(&pool).await?;
            terminate(&admin, first).await?;
            ensure!(
                pool.num_idle() == 1,
                "the dead connection left the pool before the checkout"
            );
            // Inside the window no probe runs: the first statement on the dead
            // socket fails as an error, never as a hang or a silent success.
            let failed = tokio::time::timeout(Duration::from_secs(5), backend_pid(&pool))
                .await
                .context("statement on a dead idle socket hung")?;
            let error = match failed {
                Ok(pid) => anyhow::bail!(
                    "a statement on the terminated backend {first} returned pid {pid}"
                ),
                Err(error) => error,
            };
            let root = error.root_cause().to_string();
            ensure!(
                !root.contains("statement_timeout"),
                "the failure must be the closed socket, not a timeout: {root}"
            );
            // The release-time ping discards the broken connection, so the
            // next checkout opens a live one.
            let recovered = tokio::time::timeout(Duration::from_secs(5), backend_pid(&pool))
                .await
                .context("checkout after the failed statement hung")??;
            ensure!(recovered != first);
            Ok(())
        })
    })
    .await
}

#[test]
fn probe_idle_setting_default_bounds_and_rejections() -> Result<()> {
    assert_eq!(probe_idle_from(None)?, Duration::from_millis(100));
    assert_eq!(probe_idle_from(Some("0"))?, Duration::ZERO);
    assert_eq!(
        probe_idle_from(Some("600000"))?,
        Duration::from_millis(600_000)
    );
    assert!(probe_idle_from(Some("600001")).is_err());
    assert!(probe_idle_from(Some("-1")).is_err());
    assert!(probe_idle_from(Some("fast")).is_err());
    Ok(())
}
