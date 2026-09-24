//! Incremental dashboard history, independently maintained by any frontend.
use crate::metrics::{time_pool_acquire, Metrics};
use anyhow::{ensure, Result};
use sqlx::{PgPool, Transaction};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy)]
pub struct Progress {
    pub scanned: i64,
    pub last_share_seq: i64,
    pub advanced: bool,
}

impl Progress {
    /// A pass that advanced the watermark without filling its batch bound left
    /// no unfolded share behind, so this frontend is caught up. A lost race or
    /// a full batch proves nothing about the lag and leaves the stamp alone.
    fn caught_up(&self, batch: u32) -> bool {
        self.advanced && self.scanned < i64::from(batch)
    }
}

pub struct Settings {
    batch: u32,
    interval: Duration,
}

pub fn settings_from_env() -> Result<Option<Settings>> {
    if !crate::config::flag("PRISM_HASHRATE_ROLLUP_ENABLED", true)? {
        return Ok(None);
    }
    let batch = crate::config::number("PRISM_HASHRATE_ROLLUP_BATCH_SHARES", 50_000u32)?;
    ensure!(
        batch > 0 && batch <= 100_000,
        "PRISM_HASHRATE_ROLLUP_BATCH_SHARES must be 1..100000"
    );
    let seconds = crate::config::number("PRISM_HASHRATE_ROLLUP_INTERVAL_SECONDS", 15.0f64)?;
    ensure!(
        seconds.is_finite() && seconds > 0.0 && seconds <= 86400.0,
        "PRISM_HASHRATE_ROLLUP_INTERVAL_SECONDS must be positive and at most 86400"
    );
    let interval = Duration::from_secs_f64(seconds);
    ensure!(
        !interval.is_zero(),
        "PRISM_HASHRATE_ROLLUP_INTERVAL_SECONDS is below timer precision"
    );
    Ok(Some(Settings { batch, interval }))
}

/// One MVCC snapshot and guarded watermark advance atomically fold a bounded
/// batch into all grains. A competing frontend can win; its loser adds nothing.
/// This needs no lease or share-order lock and never changes accounting rows.
pub async fn advance(pool: &PgPool, batch: u32) -> Result<Progress> {
    advance_with_metrics(pool, batch, None).await
}

async fn advance_with_metrics(
    pool: &PgPool,
    batch: u32,
    metrics: Option<&Metrics>,
) -> Result<Progress> {
    ensure!(
        batch > 0 && batch <= 100_000,
        "rollup batch must be 1..100000"
    );
    let mut transaction =
        Transaction::begin(time_pool_acquire(metrics, pool.acquire()).await?, None).await?;
    sqlx::query("SET LOCAL statement_timeout = '10s'")
        .execute(&mut *transaction)
        .await?;
    let (scanned, last_share_seq, advanced): (i64, i64, bool) =
        sqlx::query_as(include_str!("rollups.sql"))
            .bind(i64::from(batch))
            .fetch_one(&mut *transaction)
            .await?;
    transaction.commit().await?;
    Ok(Progress {
        scanned,
        last_share_seq,
        advanced,
    })
}

pub async fn run(pool: PgPool, settings: Settings, shutdown: watch::Receiver<bool>) -> Result<()> {
    run_with_metrics(pool, settings, shutdown, None).await
}

pub(crate) async fn run_with_metrics(
    pool: PgPool,
    settings: Settings,
    mut shutdown: watch::Receiver<bool>,
    metrics: Option<Arc<Metrics>>,
) -> Result<()> {
    if let Some(metrics) = metrics.as_deref() {
        metrics.start_hashrate_rollup();
    }
    let mut tick = tokio::time::interval(settings.interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = tick.tick() => {}
        }
        tokio::select! {
            _ = shutdown.changed() => break,
            result = advance_with_metrics(&pool, settings.batch, metrics.as_deref()) => match result {
                Ok(progress) => {
                    if let Some(metrics) = metrics.as_deref() {
                        metrics.record_hashrate_rollup_pass(progress.caught_up(settings.batch));
                    }
                    tracing::debug!(scanned=progress.scanned,
                        last_share_seq=progress.last_share_seq, advanced=progress.advanced,
                        "hashrate rollup maintenance")
                }
                Err(error) => tracing::warn!(%error, "hashrate rollup maintenance unavailable"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
