//! Event recording hooks and the shared pool checkout timing boundary.
use super::*;
use std::future::Future;

/// What the accepted-publication gauge observed: a failed derivation, a
/// successful observation that nothing is pending, or the oldest pending
/// block's age. Unknown is not zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingAge {
    Unknown,
    None,
    Oldest(Duration),
}

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
    /// Exactly once, at the branch that refused admission under an existing limit.
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
    /// One sample per accepted pool block, at the publication that first
    /// carried its landed payout revision. The interval starts at the durable
    /// `offered_at_ms` the offering frontend recorded immediately before its
    /// one `submitblock` call and ends at this frontend's wall clock when the
    /// publication returned: two hosts' wall clocks, so a negative interval is
    /// skew and the caller records nothing rather than clamping it to zero.
    /// `superseded` is the same measurement for a landing an earlier revision
    /// already carried when this publication caught up, never a failure.
    pub fn observe_accepted_publication(&self, result: PublicationResult, elapsed: Duration) {
        self.observe(
            Family::AcceptedPublication,
            Labels::One(("result", result.as_str())),
            elapsed,
        );
    }
    /// The oldest accepted block this frontend has not published a landed
    /// revision for, recomputed from durable rows on every health tick. A
    /// derivation that could not read the ledger is unknown (-1), never zero:
    /// zero is the successful observation that nothing is pending.
    pub fn set_accepted_pending_age(&self, age: PendingAge) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).set(
            Family::AcceptedPendingAge,
            Labels::Empty,
            match age {
                PendingAge::Unknown => -1.,
                PendingAge::None => 0.,
                PendingAge::Oldest(age) => age.as_secs_f64(),
            },
        );
    }
    /// Exactly once, at the `build_job` arm that refused work because the
    /// published payout snapshot no longer matches the cluster's revision.
    pub fn record_stale_revision_refusal(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(Family::StaleRevisionRefusals, Labels::Empty);
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
