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
//!
//! An attempt that took an offer reservation and provably sent nothing
//! ([`UnsentOffer`]) releases differently: its release is
//! [`Ledger::release_unsent_offer`], which returns the reservation to
//! `pending` for another offer, fenced on the token, `offer_reserved` and an
//! empty offer record, and is retried, deferred and attempted at shutdown
//! exactly like the ordinary release. A release that keeps the reservation
//! would make every later claim settle the block as delivery unknown and
//! never offer it, although the holder knows the call never ran.
use super::*;

/// How often a deferred release is retried, at most: once a second, or at
/// the lease's heartbeat interval when that is shorter.
const RETRY_SPACING: Duration = Duration::from_secs(1);

/// Everything the submit loop does after a shutdown signal (an offer in
/// flight, #578; the hand-back, #573; the deferred releases) finishes
/// within this. The server joins its tasks for 30 s; this leaves room for
/// one bounded retry already running when the signal came.
pub(super) const SHUTDOWN_BUDGET: Duration = Duration::from_secs(20);

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

/// Whether an attempt holds an offer reservation that it has provably not
/// sent, and what its release then records. The offer sets it once the
/// reservation has committed, clears it synchronously immediately before the
/// `submitblock` request can leave this process
/// (`Coordinator::submit_block`), and sets it again once the node's answer
/// proves the call did not run (`offer_not_executed`), before it writes the
/// row back to `pending`. It lives in the [`Attempt`], outside the work's
/// future, so it survives that future being dropped by the heartbeat or by a
/// shutdown, and the work can no longer change it once it is gone.
#[derive(Default)]
pub(super) struct UnsentOffer(std::sync::Mutex<Option<String>>);

impl UnsentOffer {
    /// The reservation is held and nothing of its call ran on the node;
    /// `why` says so in the reason the release records.
    pub(super) fn hold(&self, why: String) {
        *self.0.lock().unwrap() = Some(why);
    }

    /// From here the request may leave: the reservation is no longer
    /// provably unsent.
    pub(super) fn sending(&self) {
        *self.0.lock().unwrap() = None;
    }

    fn take(&self) -> Option<String> {
        self.0.lock().unwrap().take()
    }
}

/// A claimed row's processing, owning the claim while it runs.
pub(super) struct Attempt {
    claim: CandidateClaim,
    horizon: LeaseHorizon,
    unsent: UnsentOffer,
}

/// A failed attempt whose work has finished: the only thing a release takes.
pub(super) struct FailedAttempt {
    claim: CandidateClaim,
    reason: String,
    lease_end: tokio::time::Instant,
    /// It held an offer reservation that it provably never sent: the release
    /// returns the reservation to `pending` ([`Ledger::release_unsent_offer`]),
    /// and `reason` starts with `OFFER_NOT_SENT_REASON_PREFIX`.
    unsent: bool,
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
            unsent: UnsentOffer::default(),
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
                coordinator.process_candidate_inner(&self.claim, lease, &self.unsent),
            )
            .await
    }

    /// The attempt's run failed with `error`. When it held an offer
    /// reservation it never sent, the release returns it to `pending`.
    pub(super) fn failed(self, error: anyhow::Error) -> Box<FailedAttempt> {
        let block = &self.claim.candidate.block_hash;
        let unsent = self.unsent.take();
        let reason = match &unsent {
            Some(why) => {
                tracing::warn!(%error,%block,"the attempt failed before its block offer was sent; returning the offer reservation to pending");
                unsent_reason(why, &error)
            }
            None => {
                tracing::warn!(%error,%block,"candidate remains recoverable");
                error.to_string()
            }
        };
        Box::new(FailedAttempt {
            reason,
            lease_end: self.horizon.get(),
            unsent: unsent.is_some(),
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
    ///
    /// Dropped while it held an offer reservation it never sent (a shutdown
    /// that stopped waiting for the offer section before the send), the
    /// attempt returns the reservation to `pending` instead, in the same one
    /// bounded try ([`return_unsent_reservation`]).
    pub(super) async fn hand_back_at_shutdown(self, ledger: &Ledger, budget: Duration) {
        let block = &self.claim.candidate.block_hash;
        let release = async {
            match self.unsent.take() {
                Some(why) => {
                    tracing::warn!(%block,"the shutdown stopped the attempt before its block offer was sent; returning the offer reservation to pending");
                    let reason = unsent_reason(&why, &anyhow::anyhow!("stopped at shutdown"));
                    return_unsent_reservation(ledger, &self.claim, &reason).await
                }
                None => {
                    ledger
                        .release_recovery_claim(&self.claim, "claim released at shutdown")
                        .await
                }
            }
        };
        match tokio::time::timeout(budget, release).await {
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
    /// the lease's local end or the loop's stop.
    fn budget(&self, lease: CandidateLease, stop_by: Option<tokio::time::Instant>) -> Duration {
        let now = tokio::time::Instant::now();
        let bound = lease
            .timeout
            .min(self.lease_end.saturating_duration_since(now));
        stop_by.map_or(bound, |stop| bound.min(stop.saturating_duration_since(now)))
    }

    /// One bounded release: `Ok(true)` released, `Ok(false)` no longer held
    /// by this claim, an error when the database did not answer in time. An
    /// unsent reservation goes back to `pending` instead
    /// ([`return_unsent_reservation`]).
    async fn try_release(
        &self,
        ledger: &Ledger,
        lease: CandidateLease,
        stop_by: Option<tokio::time::Instant>,
    ) -> Result<bool> {
        let release = async {
            if self.unsent {
                return return_unsent_reservation(ledger, &self.claim, &self.reason).await;
            }
            ledger
                .release_candidate_claim(&self.claim, &self.reason)
                .await
        };
        tokio::time::timeout(self.budget(lease, stop_by), release)
            .await
            .context("candidate claim release deadline exceeded")?
    }

    /// The shutdown's release, the submit loop's token-fenced claim release
    /// at shutdown (`Ledger::release_recovery_claim`): the row keeps its
    /// schedule, and is claimable at once by another frontend. An unsent
    /// reservation goes back to `pending` instead, as in
    /// [`FailedAttempt::try_release`].
    async fn try_release_at_shutdown(
        &self,
        ledger: &Ledger,
        lease: CandidateLease,
        stop_by: Option<tokio::time::Instant>,
    ) -> Result<bool> {
        let release = async {
            if self.unsent {
                return return_unsent_reservation(ledger, &self.claim, &self.reason).await;
            }
            ledger
                .release_recovery_claim(&self.claim, &self.reason)
                .await
        };
        tokio::time::timeout(self.budget(lease, stop_by), release)
            .await
            .context("candidate claim release deadline exceeded")?
    }
}

/// The `last_error` of an unsent reservation returned by its owner after the
/// attempt failed with `error`; `why` is what [`UnsentOffer::hold`] recorded.
/// It starts with `OFFER_NOT_SENT_REASON_PREFIX`, as
/// [`Ledger::release_unsent_offer`] requires.
fn unsent_reason(why: &str, error: &anyhow::Error) -> String {
    format!(
        "{}: {why} ({error}); the reservation was returned to pending for another offer",
        crate::ledger::OFFER_NOT_SENT_REASON_PREFIX
    )
}

/// Return `claim`'s offer reservation, which its attempt provably never
/// sent, to `pending` ([`Ledger::release_unsent_offer`]): one transaction,
/// fenced on the token, `offer_reserved`, no call time and no outcome, so a
/// row another claim has taken, or one that has moved on, is never touched.
/// `Ok(true)` returned, `Ok(false)` the row is no longer this claim's unsent
/// reservation, an error when the database did not answer.
///
/// `release_unsent_offer` reports a row its fence did not match as an error,
/// like a database that did not answer, so a failed write is followed by a
/// read of the same fence: only a row still held as this claim's unsent
/// reservation is retried. If the read fails too, the write's error is kept
/// and retried. A write whose COMMIT committed although its reply was lost
/// reads as no longer held, which is the case. The caller's deadline bounds
/// both: a statement it cuts off is either committed on the server already
/// or rolled back when its connection is next used or closed, and its row
/// lock is held until then, so the next try waits for it within its own
/// bound.
async fn return_unsent_reservation(
    ledger: &Ledger,
    claim: &CandidateClaim,
    reason: &str,
) -> Result<bool> {
    let Err(error) = ledger.release_unsent_offer(claim, reason).await else {
        return Ok(true);
    };
    let held = async {
        sqlx::query_scalar::<_, bool>(
            "SELECT claim_token IS NOT DISTINCT FROM $2 AND state='offer_reserved' AND offered_at_ms IS NULL AND offer_outcome IS NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&claim.candidate.block_hash)
        .bind(&claim.claim_token)
        .fetch_optional(&mut *ledger.acquire().await?)
        .await
    };
    match held.await {
        Ok(Some(false) | None) => Ok(false),
        Ok(Some(true)) | Err(_) => Err(error),
    }
}

struct Deferred {
    failed: Box<FailedAttempt>,
    next_try: tokio::time::Instant,
}

/// The failed attempts whose claims this loop has not managed to release.
#[derive(Default)]
pub(super) struct Unreleased {
    deferred: Vec<Deferred>,
    /// Set once the loop is stopping: nothing it does after the shutdown
    /// signal runs past this.
    stop_by: Option<tokio::time::Instant>,
}

impl Unreleased {
    /// Release a failed attempt's claim now, and defer it when the database
    /// does not answer.
    pub(super) async fn release(
        &mut self,
        ledger: &Ledger,
        failed: Box<FailedAttempt>,
        lease: CandidateLease,
    ) {
        match failed.try_release(ledger, lease, self.stop_by).await {
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
                self.deferred.push(Deferred {
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
        self.deferred.retain(|deferred| {
            let live = now < deferred.failed.lease_end;
            if !live {
                tracing::warn!(
                    block = %deferred.failed.block(),
                    "deferred candidate claim release expired with its lease; the row is claimable again once the database's lease expires"
                );
            }
            live
        });
        let Some(index) = self
            .deferred
            .iter()
            .position(|deferred| deferred.next_try <= now)
        else {
            return;
        };
        match self.deferred[index]
            .failed
            .try_release(ledger, lease, self.stop_by)
            .await
        {
            Ok(released) => {
                let deferred = self.deferred.remove(index);
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
                tracing::debug!(%error, block = %self.deferred[index].failed.block(), "deferred candidate claim release failed; retrying");
                self.deferred[index].next_try =
                    tokio::time::Instant::now() + lease.interval.min(RETRY_SPACING);
            }
        }
    }

    /// The loop is stopping: the instant, fixed by the first call, that
    /// everything it still does must finish by ([`SHUTDOWN_BUDGET`]).
    pub(super) fn stop_deadline(&mut self) -> tokio::time::Instant {
        *self
            .stop_by
            .get_or_insert_with(|| tokio::time::Instant::now() + SHUTDOWN_BUDGET)
    }

    /// Wait until the next deferred release is due, or its lease ends, and
    /// then [`Unreleased::retry_one`]; never completes while none is
    /// deferred.
    pub(super) async fn retry_when_due(&mut self, ledger: &Ledger, lease: CandidateLease) {
        let Some(due) = self
            .deferred
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
        let stop_by = Some(self.stop_deadline());
        for deferred in self.deferred.drain(..) {
            let failed = deferred.failed;
            if tokio::time::Instant::now() >= failed.lease_end {
                tracing::warn!(block = %failed.block(), "deferred candidate claim release expired with its lease at shutdown");
                continue;
            }
            match failed.try_release_at_shutdown(ledger, lease, stop_by).await {
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
