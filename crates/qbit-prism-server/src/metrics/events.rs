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
    pub(crate) fn record_ctv_tip_refresh_yield(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(Family::CtvTipRefreshYields, Labels::Empty);
    }

    pub(crate) fn observe_ctv_chunk(&self, elapsed: Duration) {
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        registry.observe(Family::CtvChunkRows, Labels::Empty, 1.);
        registry.observe(
            Family::CtvChunkSeconds,
            Labels::Empty,
            elapsed.as_secs_f64(),
        );
    }
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
    /// Record one offered candidate settled as a proven orphan (#415), once
    /// the ledger committed the terminal disposition. Attributed to the
    /// settlement event, never to an observation that failed to settle.
    /// #478: one offer decision for a pending block on the current tip whose
    /// payout revision was superseded.
    pub fn record_capture_decision(&self, decision: CaptureDecision) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(
                Family::CaptureOfferDecisions,
                Labels::One(("decision", decision.as_str())),
            );
    }
    /// #478: one committed divergent confirmation (a landed block's rows
    /// started to count on balances other than its as-issued ones) and the
    /// debt it created.
    pub fn record_divergent_landing(&self, overpay_sats: u64) {
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        registry.increment(Family::DivergentLandings, vec![]);
        registry.add(Family::DivergentOverpay, vec![], overpay_sats as f64);
    }
    /// The pool's total carry-forward debt, read from the canonical balances
    /// after a balance change or a full refresh this process committed.
    pub fn record_carry_forward_debt(&self, pool_debt_sats: u64) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).set(
            Family::CarryForwardDebt,
            vec![],
            pool_debt_sats as f64,
        );
    }
    pub fn record_candidate_orphaned(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(Family::CandidatesOrphaned, vec![]);
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
    /// Exactly once, at the branch that refused admission under a configured
    /// limit or ended a session for exceeding a per-session budget.
    pub fn record_connection_refusal(&self, reason: ConnectionRefusalReason) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(
                Family::ConnectionRefusals,
                Labels::One(("reason", reason.as_str())),
            );
    }
    /// The configured global ceiling, never the permits left after admissions.
    pub fn set_stratum_connection_limit(&self, limit: usize) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).set(
            Family::ConnectionLimit,
            Labels::Empty,
            limit as f64,
        );
    }
    /// Exactly once, at the stale-job branch that refused the share. The coarse
    /// `stale-job` reason is still counted separately by the share observation.
    pub fn record_stale_job_rejection(&self, cause: StaleJobCause) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(
                Family::StaleJobRejections,
                Labels::One(("cause", cause.as_str())),
            );
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
    /// Exactly once per refresh snapshot, at the ledger's acquisition
    /// decision: the delta path advanced, or the full scan ran for the named
    /// reason. Zero samples make the first event visible to `increase()`.
    pub fn record_window_acquisition(&self, outcome: WindowAcquisition) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(
                Family::WindowAcquisitions,
                Labels::One(("outcome", outcome.as_str())),
            );
    }
    /// Exactly once per refresh that published rebuilt work, from the
    /// refresh's entry to its publication. A refresh that reused the
    /// published work unchanged, or failed, records nothing.
    pub fn observe_refresh(
        &self,
        trigger: RefreshTrigger,
        acquisition: RefreshAcquisition,
        elapsed: Duration,
    ) {
        self.observe(
            Family::RefreshSeconds,
            Labels::Two(
                ("trigger", trigger.as_str()),
                ("acquisition", acquisition.as_str()),
            ),
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
