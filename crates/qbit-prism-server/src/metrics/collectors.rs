//! Background observations. HTTP rendering performs no external I/O.
use super::{DatabaseMetrics, Metrics, ProcessMetrics};
use anyhow::{Context, Result};
use sqlx::{Connection, PgPool};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::watch;

/// Read from one procfs process directory; the explicit path also permits
/// deterministic tests of the production parser and failure behavior.
pub fn process(proc_path: &Path) -> Result<ProcessMetrics> {
    let status = std::fs::read_to_string(proc_path.join("status"))?;
    let field = |name: &str, unit: Option<&str>| -> Result<u64> {
        let line = status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .context("missing procfs field")?;
        let mut words = line.split_whitespace();
        let value: u64 = words.next().context("missing procfs value")?.parse()?;
        anyhow::ensure!(
            words.next() == unit && words.next().is_none(),
            "unexpected procfs field units"
        );
        Ok(value)
    };
    let resident_bytes = field("VmRSS:", Some("kB"))?
        .checked_mul(1024)
        .context("procfs RSS overflow")?;
    Ok(ProcessMetrics { resident_bytes })
}

/// One bounded read-only MVCC snapshot over pending candidate metadata.
/// No share-table scan, candidate JSON decode, or accounting lock.
/// A/#266 must extend the pending predicate when new outbox states land.
pub async fn database(pool: &PgPool, metrics: &Metrics) -> Result<DatabaseMetrics> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let started = std::time::Instant::now();
        let acquired = pool.acquire().await;
        metrics.observe_pool_acquire(if acquired.is_ok() { super::Outcome::Success } else { super::Outcome::Failure }, started.elapsed());
        let mut connection = acquired?;
        let mut tx = connection.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx).await?;
        sqlx::query("SELECT set_config('statement_timeout','2000',true),set_config('lock_timeout','500',true)")
            .execute(&mut *tx).await?;
        let (candidates, candidate_age): (i64, f64) = sqlx::query_as(
            "SELECT count(*), COALESCE(GREATEST(0,extract(epoch FROM transaction_timestamp()-min(created_at))),0)::double precision FROM qbit_block_candidate_outbox WHERE state='pending'"
        ).fetch_one(&mut *tx).await?;
        let snapshot = DatabaseMetrics { candidates: candidates.try_into()?, candidate_oldest: seconds(candidate_age)? };
        tx.commit().await?;
        Ok::<_, anyhow::Error>(snapshot)
    }).await.context("metrics database collection deadline exceeded")?
}
fn seconds(value: f64) -> Result<Duration> {
    Duration::try_from_secs_f64(value).context("invalid measured database age")
}

pub async fn run(
    metrics: Arc<Metrics>,
    pool: PgPool,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! { _ = shutdown.changed() => break, _ = interval.tick() => {} }
        let process_attempt = metrics.begin_collection(super::Collector::Process);
        let process = tokio::task::spawn_blocking(|| process(Path::new("/proc/self"))).await;
        process_attempt.publish_process(process.ok().and_then(Result::ok));
        let database_attempt = metrics.begin_collection(super::Collector::Database);
        tokio::select! {
            _ = shutdown.changed() => break,
            result = database(&pool, &metrics) => {
                if let Err(error) = &result { tracing::warn!(%error, "metrics database collection unavailable"); }
                database_attempt.publish_database(result.ok());
            }
        }
    }
    Ok(())
}
