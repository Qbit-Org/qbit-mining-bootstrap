//! One checkout boundary for ledger transactions and direct queries.
use super::{Ledger, Postgres};
use crate::metrics::time_pool_acquire;
use futures_util::future::BoxFuture;
use sqlx::pool::{PoolConnection, PoolConnectionMetadata};
use sqlx::PgConnection;
use std::time::Duration;

/// `PRISM_DATABASE_ACQUIRE_PROBE_IDLE_MS`: how long a pooled connection must
/// have sat idle before a checkout probes it with a protocol round trip.
///
/// Every checkout used to ping the connection first (SQLx's default), and every
/// return still pings it, so a connection that was handed back moments ago has
/// just been proved live twice. Under a work fan-out the pool never idles for
/// more than a few milliseconds and the acquire ping was a third of every
/// checkout's round trips. A connection idle for at least this long is probed
/// as before; one that died sooner after its release ping surfaces on its first
/// statement as the same I/O error any mid-statement disconnect already
/// produces, and the pool discards it. `0` probes every checkout.
pub(crate) const PROBE_IDLE_VAR: &str = "PRISM_DATABASE_ACQUIRE_PROBE_IDLE_MS";
const PROBE_IDLE_DEFAULT_MS: u64 = 100;
const PROBE_IDLE_MAX_MS: u64 = 600_000;

pub(crate) fn probe_idle_setting() -> anyhow::Result<Duration> {
    probe_idle_from(std::env::var(PROBE_IDLE_VAR).ok().as_deref())
}

pub(crate) fn probe_idle_from(value: Option<&str>) -> anyhow::Result<Duration> {
    let millis = match value {
        Some(raw) => raw
            .parse::<u64>()
            .map_err(|error| anyhow::anyhow!("{PROBE_IDLE_VAR} must be an integer: {error}"))?,
        None => PROBE_IDLE_DEFAULT_MS,
    };
    anyhow::ensure!(
        millis <= PROBE_IDLE_MAX_MS,
        "{PROBE_IDLE_VAR} must be between 0 and {PROBE_IDLE_MAX_MS} milliseconds"
    );
    Ok(Duration::from_millis(millis))
}

/// The pool's `before_acquire` hook: ping only after an idle gap of at least
/// `idle_at_least`. A failed ping makes SQLx discard the connection and open
/// another, exactly as its default acquire-time ping did.
pub(crate) fn probe_before_acquire(
    connection: &mut PgConnection,
    metadata: PoolConnectionMetadata,
    idle_at_least: Duration,
) -> BoxFuture<'_, sqlx::Result<bool>> {
    Box::pin(async move {
        if metadata.idle_for >= idle_at_least {
            sqlx::Connection::ping(connection).await?;
        }
        Ok(true)
    })
}

impl Ledger {
    /// Acquire a connection and record only its checkout duration, including
    /// connection establishment/validation. SQL and BEGIN run after the timer.
    /// Keep the returned connection scoped to the original query's lifetime.
    pub(crate) async fn acquire(&self) -> sqlx::Result<PoolConnection<Postgres>> {
        time_pool_acquire(self.metrics.as_deref(), self.pool.acquire()).await
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod job_tests;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod probe_tests;
