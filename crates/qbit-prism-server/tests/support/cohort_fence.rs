//! The cluster-row authority contract (#479), driven through production
//! writers: job persistence fences authority with `FOR KEY SHARE` on the
//! cluster row, which a bare non-key `UPDATE` of `payout_revision`,
//! `config_fingerprint` or `fatal_error` passes. Every authority writer must
//! therefore take the row `FOR UPDATE` before it writes. A test holds the
//! fence the way a job cohort does and requires the writer to block on it,
//! then releases it and lets the writer finish.
use anyhow::{bail, Context, Result};
use sqlx::PgPool;
use std::future::Future;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// The fence a compact job cohort, repair or prepared save takes.
pub const COHORT_FENCE: &str =
    "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton FOR KEY SHARE";

/// Hold [`COHORT_FENCE`] on a connection from `pool` (a pool on the ledger's
/// database), start `writer`, and require it to block on the fence holder
/// rather than finish; then release the fence and return the writer's own
/// result, which the caller checks (a halting reconcile returns an error
/// after committing its halt).
pub async fn waits_for_the_cohort_fence<T: Send + 'static>(
    pool: &PgPool,
    what: &str,
    writer: impl Future<Output = Result<T>> + Send + 'static,
) -> Result<Result<T>> {
    let mut cohort = pool.begin().await?;
    sqlx::query(COHORT_FENCE).fetch_one(&mut *cohort).await?;
    let cohort_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *cohort)
        .await?;
    let mut writer = tokio::spawn(writer);
    let blocked = timeout(Duration::from_secs(10), async {
        loop {
            if writer.is_finished() {
                return Ok::<_, anyhow::Error>(false);
            }
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
            )
            .bind(cohort_pid)
            .fetch_one(pool)
            .await?;
            if blocked {
                return Ok(true);
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| format!("{what} neither finished nor waited for a job cohort's fence"))??;
    if !blocked {
        let outcome = (&mut writer).await?;
        cohort.rollback().await?;
        bail!(
            "{what} finished under a held job cohort fence ({:?}); an authority write must take the cluster row FOR UPDATE first",
            outcome.map(|_| ())
        );
    }
    cohort.commit().await?;
    timeout(Duration::from_secs(30), writer)
        .await
        .with_context(|| format!("{what} never proceeded after the cohort fence was released"))?
        .context("writer task")
}
