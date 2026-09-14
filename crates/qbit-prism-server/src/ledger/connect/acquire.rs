//! One checkout boundary for ledger transactions and direct queries.
use super::{Ledger, Metrics, Outcome, PgPool, Postgres, WaitGuard, WaitKind};
use sqlx::pool::PoolConnection;
use std::future::Future;

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
    observe(metrics, pool.acquire()).await
}

async fn observe<T>(
    metrics: Option<&Metrics>,
    acquisition: impl Future<Output = sqlx::Result<T>>,
) -> sqlx::Result<T> {
    // Arm on first poll, not future construction. Drop records one failure if
    // cancellation interrupts acquisition; complete disarms before SQL runs.
    let guard = metrics.map(|metrics| WaitGuard::arm(metrics, WaitKind::PoolAcquire));
    let acquired = acquisition.await;
    if let Some(guard) = guard {
        guard.complete(if acquired.is_ok() {
            Outcome::Success
        } else {
            Outcome::Failure
        });
    }
    acquired
}

#[cfg(test)]
mod tests;
