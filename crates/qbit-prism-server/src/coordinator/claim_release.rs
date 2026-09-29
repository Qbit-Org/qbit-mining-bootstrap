//! #529: the release of a failed attempt's candidate claim, retried when the
//! database does not answer the first time.
//!
//! Every write under a claim is fenced on its token and on a live database
//! lease, and a failed attempt releases its claim so the row can be retried
//! at once. When that release fails too, as it does when a PostgreSQL
//! failover takes the pool's connections with the old primary mid-landing,
//! no frontend can take the row until the lease expires, up to
//! `CANDIDATE_LEASE` (120 s) later. The holder is the one party that knows
//! its attempt is over, so it keeps retrying the same release until the
//! database answers or the lease's local end passes, and once more at
//! shutdown. The retries keep their schedule while the loop processes other
//! rows, and claiming new work waits for at most one bounded retry.
//!
//! Why this is safe: an [`Attempt`] owns its claim, the future of
//! [`Attempt::run`] borrows it, and the only ways to a release consume it:
//! [`Attempt::failed`] (the [`FailedAttempt`] a release takes) and
//! [`Attempt::hand_back_at_shutdown`]. No release can overlap the work whose
//! claim it frees, so nothing this process started under the claim (a
//! `submitblock` call included) can still run after the row is released. The release itself is
//! [`Ledger::release_candidate_claim`], fenced on the token and the live
//! lease, or at shutdown [`Ledger::release_recovery_claim`], fenced on the
//! token: once another frontend has taken the row it changes nothing. A
//! holder that died with the old primary still leaves its claim to the
//! lease.
use super::*;

/// How often a deferred release is retried, at most: once a second, or at
/// the lease's heartbeat interval when that is shorter.
const RETRY_SPACING: Duration = Duration::from_secs(1);

/// The local instant a claim's heartbeat treats as its lease's end: the
/// start of the last renewal (or of the claim) plus the lease, which is never
/// later than the database's own expiry.
pub(super) struct LeaseHorizon(std::sync::Mutex<tokio::time::Instant>);

impl LeaseHorizon {
    pub(super) fn set(&self, until: tokio::time::Instant) {
        *self.0.lock().unwrap() = until;
    }

    fn get(&self) -> tokio::time::Instant {
        *self.0.lock().unwrap()
    }
}

/// A claimed row's processing, owning the claim while it runs.
pub(super) struct Attempt {
    claim: CandidateClaim,
    horizon: LeaseHorizon,
}

/// A failed attempt whose work has finished: the only thing a release takes.
pub(super) struct FailedAttempt {
    claim: CandidateClaim,
    reason: String,
    lease_end: tokio::time::Instant,
}

impl Attempt {
    /// `claimed_at` is taken before the claim statement ran, so the claim's
    /// database expiry is at least `lease.seconds` after it.
    pub(super) fn new(
        claim: CandidateClaim,
        claimed_at: tokio::time::Instant,
        lease: CandidateLease,
    ) -> Self {
        let horizon = claimed_at + Duration::from_secs(lease.seconds as u64);
        Self {
            claim,
            horizon: LeaseHorizon(std::sync::Mutex::new(horizon)),
        }
    }

    /// Process the claim under its heartbeat. The future borrows the
    /// attempt, so the claim can be released (by [`Attempt::failed`] or
    /// [`Attempt::hand_back_at_shutdown`], which consume it) only once the
    /// future is gone.
    pub(super) async fn run(&self, coordinator: &Coordinator, lease: CandidateLease) -> Result<()> {
        coordinator
            .with_tracked_heartbeat(
                &self.claim,
                lease,
                Some(&self.horizon),
                coordinator.process_candidate_inner(&self.claim, lease),
            )
            .await
    }

    /// The attempt's run failed with `error`.
    pub(super) fn failed(self, error: anyhow::Error) -> Box<FailedAttempt> {
        tracing::warn!(%error,block=%self.claim.candidate.block_hash,"candidate remains recoverable");
        Box::new(FailedAttempt {
            reason: error.to_string(),
            lease_end: self.horizon.get(),
            claim: self.claim,
        })
    }

    /// #573: the shutdown dropped the attempt's work mid-attempt; hand the
    /// claim back instead of making another frontend wait out the lease on
    /// every rolling restart. The release keeps the state and schedule, so
    /// it changes only when a successor may take the row, not what it then
    /// does: exactly what it would do after the expiry. Fenced on the token
    /// and on an unfinished state, so a terminal commit that won the race is
    /// left alone. A failed release falls back to the expiry.
    pub(super) async fn hand_back_at_shutdown(self, ledger: &Ledger, lease: CandidateLease) {
        let block = &self.claim.candidate.block_hash;
        match tokio::time::timeout(
            lease.timeout,
            ledger.release_recovery_claim(&self.claim, "claim released at shutdown"),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(release)) => {
                tracing::warn!(%release,%block,"candidate claim release at shutdown failed; the claim waits for its expiry")
            }
            Err(_) => {
                tracing::warn!(%block,"candidate claim release at shutdown timed out; the claim waits for its expiry")
            }
        }
    }
}

impl FailedAttempt {
    fn block(&self) -> &str {
        &self.claim.candidate.block_hash
    }

    fn lease_remaining_ms(&self) -> u128 {
        self.lease_end
            .saturating_duration_since(tokio::time::Instant::now())
            .as_millis()
    }

    /// The bound of one release attempt: the lease's own, and never past
    /// the lease's local end.
    fn budget(&self, lease: CandidateLease) -> Duration {
        lease.timeout.min(
            self.lease_end
                .saturating_duration_since(tokio::time::Instant::now()),
        )
    }

    /// One bounded release: `Ok(true)` released, `Ok(false)` no longer held
    /// by this claim, an error when the database did not answer in time.
    async fn try_release(&self, ledger: &Ledger, lease: CandidateLease) -> Result<bool> {
        tokio::time::timeout(
            self.budget(lease),
            ledger.release_candidate_claim(&self.claim, &self.reason),
        )
        .await
        .context("candidate claim release deadline exceeded")?
    }

    /// The shutdown's release, the submit loop's token-fenced claim release
    /// at shutdown (`Ledger::release_recovery_claim`): the row keeps its
    /// schedule, and is claimable at once by another frontend.
    async fn try_release_at_shutdown(
        &self,
        ledger: &Ledger,
        lease: CandidateLease,
    ) -> Result<bool> {
        tokio::time::timeout(
            self.budget(lease),
            ledger.release_recovery_claim(&self.claim, &self.reason),
        )
        .await
        .context("candidate claim release deadline exceeded")?
    }
}

struct Deferred {
    failed: Box<FailedAttempt>,
    next_try: tokio::time::Instant,
}

/// The failed attempts whose claims this loop has not managed to release.
#[derive(Default)]
pub(super) struct Unreleased(Vec<Deferred>);

impl Unreleased {
    /// Release a failed attempt's claim now, and defer it when the database
    /// does not answer.
    pub(super) async fn release(
        &mut self,
        ledger: &Ledger,
        failed: Box<FailedAttempt>,
        lease: CandidateLease,
    ) {
        match failed.try_release(ledger, lease).await {
            Ok(true) => {}
            Ok(false) => tracing::warn!(
                block = %failed.block(),
                "candidate claim was already lost or expired; nothing to release"
            ),
            Err(error) => {
                tracing::warn!(
                    %error,
                    block = %failed.block(),
                    lease_remaining_ms = failed.lease_remaining_ms(),
                    "candidate claim release deferred: retrying until the database answers or the lease ends"
                );
                self.0.push(Deferred {
                    failed,
                    next_try: tokio::time::Instant::now() + lease.interval.min(RETRY_SPACING),
                });
            }
        }
    }

    /// Drop the deferred releases whose lease has ended, then retry at most
    /// one that is due, so claiming new work is never held up by more than
    /// one bounded attempt.
    pub(super) async fn retry_one(&mut self, ledger: &Ledger, lease: CandidateLease) {
        let now = tokio::time::Instant::now();
        self.0.retain(|deferred| {
            let live = now < deferred.failed.lease_end;
            if !live {
                tracing::warn!(
                    block = %deferred.failed.block(),
                    "deferred candidate claim release expired with its lease; the row is claimable again once the database's lease expires"
                );
            }
            live
        });
        let Some(index) = self.0.iter().position(|deferred| deferred.next_try <= now) else {
            return;
        };
        match self.0[index].failed.try_release(ledger, lease).await {
            Ok(released) => {
                let deferred = self.0.remove(index);
                if released {
                    tracing::warn!(
                        block = %deferred.failed.block(),
                        lease_remaining_ms = deferred.failed.lease_remaining_ms(),
                        "deferred candidate claim release succeeded; the row is claimable again"
                    );
                } else {
                    tracing::warn!(
                        block = %deferred.failed.block(),
                        "deferred candidate claim release found the claim no longer held (released, taken over or expired)"
                    );
                }
            }
            Err(error) => {
                tracing::debug!(%error, block = %self.0[index].failed.block(), "deferred candidate claim release failed; retrying");
                self.0[index].next_try =
                    tokio::time::Instant::now() + lease.interval.min(RETRY_SPACING);
            }
        }
    }

    /// Wait until the next deferred release is due, or its lease ends, and
    /// then [`Unreleased::retry_one`]; never completes while none is
    /// deferred.
    pub(super) async fn retry_when_due(&mut self, ledger: &Ledger, lease: CandidateLease) {
        let Some(due) = self
            .0
            .iter()
            .map(|deferred| deferred.next_try.min(deferred.failed.lease_end))
            .min()
        else {
            return std::future::pending().await;
        };
        tokio::time::sleep_until(due).await;
        self.retry_one(ledger, lease).await;
    }

    /// At shutdown, one bounded attempt for each deferred release whose lease
    /// has not ended; whatever is left waits for its lease.
    pub(super) async fn release_at_shutdown(&mut self, ledger: &Ledger, lease: CandidateLease) {
        for deferred in self.0.drain(..) {
            let failed = deferred.failed;
            if tokio::time::Instant::now() >= failed.lease_end {
                tracing::warn!(block = %failed.block(), "deferred candidate claim release expired with its lease at shutdown");
                continue;
            }
            match failed.try_release_at_shutdown(ledger, lease).await {
                Ok(true) => tracing::warn!(
                    block = %failed.block(),
                    "deferred candidate claim release succeeded at shutdown"
                ),
                Ok(false) => tracing::warn!(
                    block = %failed.block(),
                    "deferred candidate claim release at shutdown found the claim no longer held"
                ),
                Err(error) => tracing::warn!(
                    %error,
                    block = %failed.block(),
                    lease_remaining_ms = failed.lease_remaining_ms(),
                    "deferred candidate claim release failed at shutdown; the row waits for its lease"
                ),
            }
        }
    }
}
