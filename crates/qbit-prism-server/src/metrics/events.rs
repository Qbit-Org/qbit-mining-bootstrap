//! Small synchronous hooks; producer code owns the event boundaries.
use super::*;

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
