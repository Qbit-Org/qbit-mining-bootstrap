//! One checkout boundary for ledger transactions and direct queries.
use super::{Ledger, Postgres};
use crate::metrics::time_pool_acquire;
use sqlx::pool::PoolConnection;

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
