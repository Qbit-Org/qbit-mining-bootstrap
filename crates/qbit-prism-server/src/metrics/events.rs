//! Event recording hooks and the shared pool checkout timing boundary.
use super::*;
use std::future::Future;

/// Records exactly once, including when the acquisition future is cancelled.
/// Its lifetime ends before SQL or BEGIN, so only checkout is measured.
struct PoolAcquireObservation<'a> {
    metrics: &'a Metrics,
    started: tokio::time::Instant,
    outcome: Outcome,
}

impl Drop for PoolAcquireObservation<'_> {
    fn drop(&mut self) {
        self.metrics
            .observe_pool_acquire(self.outcome, self.started.elapsed());
    }
}

/// The shared ledger/collector checkout boundary. Tokio's monotonic clock
/// measures real elapsed time normally and controlled time in a paused runtime.
/// Neither the guard nor its clock is read before first poll or without metrics.
pub(crate) async fn time_pool_acquire<T>(
    metrics: Option<&Metrics>,
    acquire: impl Future<Output = sqlx::Result<T>>,
) -> sqlx::Result<T> {
    let mut observation = metrics.map(|metrics| PoolAcquireObservation {
        metrics,
        started: tokio::time::Instant::now(),
        outcome: Outcome::Failure,
    });
    let result = acquire.await;
    if let Some(observation) = &mut observation {
        observation.outcome = if result.is_ok() {
            Outcome::Success
        } else {
            Outcome::Failure
        };
    }
    result
}

#[cfg(test)]
mod tests;

impl Metrics {
    pub fn observe_share_ack(&self, result: AckResult, elapsed: Duration) {
        self.observe(
            Family::ShareAck,
            Labels::One(("result", result.as_str())),
            elapsed,
        );
    }
    /// Record stale-grace credit only after durable acceptance.
    pub fn record_grace_credit(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(Family::Grace, Labels::Empty);
    }
    /// Record a share accepted after its commit deadline, once the ledger
    /// confirmed the in-flight commit.
    pub fn record_late_confirmation(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(Family::LateConfirmed, vec![]);
    }
    pub fn record_rejection(&self, reason: RejectReason) {
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        registry.increment(
            Family::Rejections,
            Labels::One(("reason_id", reason.as_str())),
        );
        let aggregate = match reason {
            RejectReason::StaleJob | RejectReason::UnknownJob => Some(Family::Stale),
            RejectReason::DuplicateShare => Some(Family::Duplicate),
            RejectReason::LowDifficulty => Some(Family::LowDifficulty),
            _ => None,
        };
        if let Some(family) = aggregate {
            registry.increment(family, Labels::Empty);
        }
    }
    /// Exactly one observation at the actual first-offer boundary. A/#266
    /// owns timestamp transport and recovery semantics across processes.
    pub fn observe_first_offer(&self, elapsed: Duration) {
        self.observe(Family::FirstOffer, Labels::Empty, elapsed);
    }
    pub fn observe_pool_acquire(&self, result: Outcome, elapsed: Duration) {
        self.observe(
            Family::PoolAcquire,
            Labels::One(("result", result.as_str())),
            elapsed,
        );
    }
    pub fn observe_advisory_lock(&self, lock: LockKind, result: Outcome, elapsed: Duration) {
        self.observe(
            Family::LockWait,
            Labels::Two(("lock", lock.as_str()), ("result", result.as_str())),
            elapsed,
        );
    }
    fn observe(&self, family: Family, labels: Labels, elapsed: Duration) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .observe(family, labels, elapsed.as_secs_f64());
    }
}
