//! Incremental dashboard history, independently maintained by any frontend.
use crate::metrics::{time_pool_acquire, Metrics};
use anyhow::{ensure, Result};
use sqlx::{PgPool, Transaction};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

/// 3.0's sweep, which a 3.1 dual writer runs too.
const SWEEP: &str = include_str!("rollups.sql");

/// The sweep on a 3.1 dual-writer database (a dual writer's, or a single
/// writer's on a personalised database) takes the progress row first, so
/// its own statement's snapshot starts after any transaction that holds it:
/// the peer sync's, which folds the late peer shares it inserts
/// ([`LATE_PEER_SHARES`]). A share that commits after a sweep passed its
/// `share_seq` is therefore either seen by the next sweep or folded by the
/// transaction that inserted it, never neither.
pub(crate) const PROGRESS_LOCK: &str =
    "SELECT last_share_seq FROM qbit_hashrate_rollup_progress WHERE singleton FOR UPDATE";

/// Fold the peer shares a pull inserts at or below the sweep's watermark,
/// which the sweep has passed and never reads again, into the rollup
/// buckets, in the inserting transaction and under [`PROGRESS_LOCK`]: the
/// same grains and buckets as `rollups.sql`'s `pool_rollup` and
/// `miner_rollup`. Peer shares arrive below shares this node already holds,
/// so the watermark can be past them; a share above it is the sweep's. `$1`
/// is the pulled rows as JSON, `$2` the share IDs inserted. Returns the
/// shares folded.
pub(crate) const LATE_PEER_SHARES: &str = "WITH progress AS (SELECT last_share_seq FROM qbit_hashrate_rollup_progress WHERE singleton FOR UPDATE),\
 batch AS (SELECT i.accepted_at,i.miner_id,i.share_difficulty FROM jsonb_populate_recordset(NULL::qbit_share_ledger,$1) i \
   WHERE i.share_id=ANY($2) AND i.accepted AND i.share_seq<=(SELECT last_share_seq FROM progress)),\
 grains AS (SELECT grain_seconds FROM (VALUES (300), (3600), (86400)) AS grain(grain_seconds)),\
 pool_rollup AS (INSERT INTO qbit_hashrate_rollup_pool (grain_seconds,bucket_epoch,accepted_share_count,accepted_share_difficulty) \
   SELECT grains.grain_seconds,floor(extract(epoch FROM batch.accepted_at) / grains.grain_seconds)::bigint * grains.grain_seconds AS bucket_epoch,\
   count(*),sum(batch.share_difficulty) FROM batch, grains GROUP BY grains.grain_seconds, bucket_epoch \
   ON CONFLICT (grain_seconds, bucket_epoch) DO UPDATE SET accepted_share_count = qbit_hashrate_rollup_pool.accepted_share_count + EXCLUDED.accepted_share_count,\
   accepted_share_difficulty = qbit_hashrate_rollup_pool.accepted_share_difficulty + EXCLUDED.accepted_share_difficulty RETURNING 1),\
 miner_rollup AS (INSERT INTO qbit_hashrate_rollup_miner (grain_seconds,bucket_epoch,miner_id,accepted_share_count,accepted_share_difficulty) \
   SELECT grains.grain_seconds,floor(extract(epoch FROM batch.accepted_at) / grains.grain_seconds)::bigint * grains.grain_seconds AS bucket_epoch,\
   batch.miner_id,count(*),sum(batch.share_difficulty) FROM batch, grains GROUP BY grains.grain_seconds, bucket_epoch, batch.miner_id \
   ON CONFLICT (grain_seconds, bucket_epoch, miner_id) DO UPDATE SET accepted_share_count = qbit_hashrate_rollup_miner.accepted_share_count + EXCLUDED.accepted_share_count,\
   accepted_share_difficulty = qbit_hashrate_rollup_miner.accepted_share_difficulty + EXCLUDED.accepted_share_difficulty RETURNING 1) \
 SELECT (SELECT count(*) FROM batch)+0*(SELECT count(*) FROM pool_rollup)+0*(SELECT count(*) FROM miner_rollup)";

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
    dual_writer: bool,
}

impl Settings {
    /// The sweep on a 3.1 dual-writer database, which takes the progress row
    /// first ([`PROGRESS_LOCK`]).
    pub fn for_dual_writer(self) -> Self {
        Self {
            dual_writer: true,
            ..self
        }
    }
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
    Ok(Some(Settings {
        batch,
        interval,
        dual_writer: false,
    }))
}

/// One MVCC snapshot and guarded watermark advance atomically fold a bounded
/// batch into all grains. A competing frontend can win; its loser adds nothing.
/// This needs no lease or share-order lock and never changes accounting rows.
pub async fn advance(pool: &PgPool, batch: u32) -> Result<Progress> {
    advance_with_metrics(pool, batch, false, None).await
}

/// [`advance`] for a 3.1 dual-writer database: the progress row first
/// ([`PROGRESS_LOCK`]), then 3.0's sweep.
pub async fn advance_dual_writer(pool: &PgPool, batch: u32) -> Result<Progress> {
    advance_with_metrics(pool, batch, true, None).await
}

async fn advance_with_metrics(
    pool: &PgPool,
    batch: u32,
    dual_writer: bool,
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
    if dual_writer {
        sqlx::query(PROGRESS_LOCK)
            .execute(&mut *transaction)
            .await?;
    }
    let (scanned, last_share_seq, advanced): (i64, i64, bool) = sqlx::query_as(SWEEP)
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
            result = advance_with_metrics(&pool, settings.batch, settings.dual_writer, metrics.as_deref()) => match result {
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
