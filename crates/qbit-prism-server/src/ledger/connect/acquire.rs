//! One checkout boundary for ledger transactions and direct queries.
use super::{Ledger, Metrics, PgPool, Postgres};
use crate::metrics::collectors::observe_pool_acquire;
use sqlx::pool::PoolConnection;

impl Ledger {
    /// Acquire a connection and record only its checkout duration, including
    /// connection establishment/validation. SQL and BEGIN run after the timer.
    /// Keep the returned connection scoped to the original query's lifetime.
    pub(crate) async fn acquire(&self) -> sqlx::Result<PoolConnection<Postgres>> {
        acquire(&self.pool, self.metrics.as_deref()).await
    }
}

pub(super) async fn acquire(
    pool: &PgPool,
    metrics: Option<&Metrics>,
) -> sqlx::Result<PoolConnection<Postgres>> {
    observe_pool_acquire(metrics, pool.acquire()).await
}

#[cfg(test)]
mod tests;
