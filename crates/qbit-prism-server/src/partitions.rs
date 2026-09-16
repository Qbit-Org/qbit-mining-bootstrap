//! Attached headroom for the partitioned share ledger.
//!
//! `qbit_share_ledger` is RANGE partitioned on `share_seq` with no DEFAULT
//! partition (migration 017), so an append whose sequence value runs past the
//! last attached bound is refused outright: PostgreSQL raises SQLSTATE 23514,
//! "no partition of relation ... found for row". The database keeps that from
//! happening through `qbit_prism_share_partition_ensure()`, which attaches
//! empty partitions until `lead_partitions` of them cover the sequence, and
//! which every frontend calls once at startup and then on this task's tick.
//! The call is serialized per schema by the online migration runner's advisory
//! lock class, so several frontends ticking together create each partition
//! once, and it creates nothing while the lead is intact.
use anyhow::{ensure, Result};
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::watch;

/// One call cannot be allowed to hold the parent's SHARE UPDATE EXCLUSIVE lock
/// indefinitely: an ATTACH that cannot take it is retried on the next tick,
/// and the sequence is many partition widths away from the last bound.
const ENSURE_STATEMENT_TIMEOUT: &str = "SET LOCAL statement_timeout = '30s'";

pub struct Settings {
    interval: Duration,
}

/// The tick of the maintenance task, whole seconds.
///
/// The default of 60 s is a small fraction of the time the lead covers (four
/// partitions of 2^24 rows is about nine hours at 500 shares/s), so a missed
/// tick or a temporarily unreachable database cannot exhaust it.
pub fn settings_from_env() -> Result<Settings> {
    let seconds = crate::config::number("PRISM_SHARE_PARTITION_ENSURE_INTERVAL_SECONDS", 60u64)?;
    ensure!(
        (1..=86_400).contains(&seconds),
        "PRISM_SHARE_PARTITION_ENSURE_INTERVAL_SECONDS must be 1..86400"
    );
    Ok(Settings {
        interval: Duration::from_secs(seconds),
    })
}

/// Attach whatever the lead is missing, returning how many partitions were
/// created. Idempotent, and zero whenever the lead is already intact.
pub async fn ensure(pool: &PgPool) -> Result<i32> {
    let mut transaction = pool.begin().await?;
    sqlx::query(ENSURE_STATEMENT_TIMEOUT)
        .execute(&mut *transaction)
        .await?;
    let created: i32 = sqlx::query_scalar("SELECT qbit_prism_share_partition_ensure()")
        .fetch_one(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(created)
}

/// Rows of attached partition headroom above the next `share_seq`, or `None`
/// when the ledger has not been converted yet and there is nothing to report.
///
/// Read from the catalog rather than `pg_inherits` because a partition that a
/// retention run has detached must stop counting as headroom the moment the
/// catalog records it, whatever the visibility of the DETACH itself.
pub async fn lead_rows(pool: &PgPool) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT max(upper_seq)-qbit_prism_share_next_seq() FROM qbit_prism_share_partitions WHERE state='attached'",
    )
    .fetch_one(pool)
    .await?)
}

/// Keep the lead attached for as long as this frontend runs.
///
/// A failure is never fatal: the lead is many hours of appends wide, the next
/// tick retries, and the append path runs `ensure` itself if it ever does find
/// no partition. Ending the task would instead leave the instance serving with
/// nothing maintaining its partitions.
pub async fn run(
    pool: PgPool,
    settings: Settings,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut tick = tokio::time::interval(settings.interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = tick.tick() => {}
        }
        tokio::select! {
            _ = shutdown.changed() => break,
            result = ensure(&pool) => match result {
                // An intact lead is the ordinary outcome, once a minute.
                Ok(0) => tracing::debug!("share ledger partition lead intact"),
                Ok(created) => tracing::info!(created, "share ledger partitions attached"),
                Err(error) => tracing::warn!(%error, "share ledger partition maintenance unavailable"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The environment is process-wide; these three cases share one lock so
    /// they cannot observe each other's variable.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    const NAME: &str = "PRISM_SHARE_PARTITION_ENSURE_INTERVAL_SECONDS";

    fn with_value<T>(value: Option<&str>, body: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        match value {
            Some(value) => std::env::set_var(NAME, value),
            None => std::env::remove_var(NAME),
        }
        let result = body();
        std::env::remove_var(NAME);
        result
    }

    #[test]
    fn unset_interval_ticks_once_a_minute() {
        let settings = with_value(None, || settings_from_env().unwrap());
        assert_eq!(settings.interval, Duration::from_secs(60));
    }

    #[test]
    fn interval_accepts_its_documented_bounds() {
        for (value, seconds) in [("1", 1u64), ("86400", 86_400)] {
            let settings = with_value(Some(value), || settings_from_env().unwrap());
            assert_eq!(settings.interval, Duration::from_secs(seconds));
        }
    }

    #[test]
    fn an_empty_interval_is_the_unset_default_like_every_other_setting() {
        for value in ["", "   "] {
            let settings = with_value(Some(value), || settings_from_env().unwrap());
            assert_eq!(settings.interval, Duration::from_secs(60));
        }
    }

    #[test]
    fn interval_outside_its_bounds_or_unparsable_is_refused() {
        for value in ["0", "86401", "-1", "sixty", "60.5"] {
            let refused = with_value(Some(value), settings_from_env);
            assert!(
                refused.is_err(),
                "{NAME}={value:?} was accepted: {:?}",
                refused.map(|settings| settings.interval)
            );
        }
    }
}
