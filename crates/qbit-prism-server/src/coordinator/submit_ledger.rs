//! The two storage boundaries used by ordinary share admission.
//!
//! Tests replace I/O here, not Coordinator's classification/accounting logic.
//! Production appends retain the transaction-scoped payout revision check.
use super::publication_authority::{LeaseCommitFence, LeaseCommitRefusal};
use super::*;
use crate::ledger::MovedRevision;
use futures_util::future::BoxFuture;
use std::sync::{atomic::AtomicU8, OnceLock};

const OPEN: u8 = 0;
const COMMITTING: u8 = 1;
/// #716: the hook refused only because its lease's authority was contended.
/// Nothing was written and nothing is decided: the append waits for the
/// authority and runs again, and the deadline can still close the gate.
const CONTENDED: u8 = 6;

/// Stored atomically with closure: later observations cannot change why the
/// first refusal won, or replace an already-started COMMIT with staleness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum GateClosure {
    DeadlineOrCancelled = 2,
    AuthorityUnavailable = 3,
    StaleAuthority = 4,
    /// #716: the deadline closed the gate while the append was waiting out
    /// contention for its lease's authority. The authority held the share
    /// back, not the ledger.
    AuthorityContended = 5,
}

impl GateClosure {
    /// The `closure` field of a refusal's log line, which operators read.
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::DeadlineOrCancelled => "deadline-or-cancelled",
            Self::AuthorityUnavailable => "authority-unavailable",
            Self::StaleAuthority => "stale-authority",
            Self::AuthorityContended => "authority-contended",
        }
    }
}

impl From<LeaseCommitRefusal> for GateClosure {
    fn from(refusal: LeaseCommitRefusal) -> Self {
        match refusal {
            LeaseCommitRefusal::Stale => Self::StaleAuthority,
            LeaseCommitRefusal::Unavailable => Self::AuthorityUnavailable,
            LeaseCommitRefusal::Contended => Self::AuthorityContended,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GateState {
    Open,
    /// #716: refused for contention; the append waits for the authority.
    Contended,
    Committing,
    Closed,
}

/// One-shot arbiter between an append's COMMIT and its acknowledgement
/// deadline. Both transitions leave `Open` by compare-and-swap, so exactly one
/// of them wins: an append sends COMMIT only after `begin_commit` succeeds,
/// and once `close` succeeds it never will. A lease fence that finds its
/// authority contended moves the gate to `Contended` instead (#716), which
/// decides nothing: `retry_after_contention` reopens it for the append's next
/// attempt, under the same deadline, or closes it, and the deadline can close
/// it meanwhile.
#[derive(Default)]
pub(super) struct CommitGate {
    state: AtomicU8,
    committing_since: OnceLock<Instant>,
    lease: Option<LeaseCommitFence>,
}

impl CommitGate {
    pub(super) fn with_lease(lease: Option<LeaseCommitFence>) -> Self {
        Self {
            lease,
            ..Self::default()
        }
    }

    /// Whether a lease fence guards this gate, the only kind under which a
    /// refused append may run again (#716).
    pub(super) fn fenced(&self) -> bool {
        self.lease.is_some()
    }

    /// The append's pre-commit hook. `true` lets COMMIT be sent, and records
    /// when it started.
    pub(super) fn begin_commit(&self) -> bool {
        if let Some(lease) = &self.lease {
            return match lease.with_authority(|| self.begin_authorized_commit()) {
                Ok(won) => won,
                // #716: contention proves nothing, so it closes nothing. The
                // append rolls back and waits in `retry_after_contention`,
                // outside ORDER_LOCK, before it runs again. A gate the
                // deadline already closed stays closed, and the retry finds
                // it so.
                Err(LeaseCommitRefusal::Contended) => {
                    self.contend();
                    false
                }
                Err(refusal) => {
                    self.close_for(refusal.into());
                    false
                }
            };
        }
        self.begin_authorized_commit()
    }

    /// After an append refused at its hook: whether it may run again under
    /// this gate (#716). Only a `Contended` gate allows that. This waits for
    /// the authority, outside `ORDER_LOCK` and any transaction, and checks the
    /// lease against it: `true` when the lease still authorizes the share and
    /// the gate reopened, so the next attempt decides at its own hook.
    /// Otherwise `false`, with the gate closed by the lease's own refusal or
    /// already by the deadline.
    pub(super) async fn retry_after_contention(&self) -> bool {
        let Some(lease) = &self.lease else {
            return false;
        };
        if self.state() != GateState::Contended {
            return false;
        }
        match lease.await_authority().await {
            Ok(()) => self.reopen(),
            Err(refusal) => {
                self.close_for(refusal.into());
                false
            }
        }
    }

    /// The hook found its lease's authority contended. A gate the deadline
    /// already closed stays closed.
    fn contend(&self) -> bool {
        self.state
            .compare_exchange(OPEN, CONTENDED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Reopen a contended gate for the append's next attempt, unless the
    /// deadline closed it first.
    fn reopen(&self) -> bool {
        self.state
            .compare_exchange(CONTENDED, OPEN, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn begin_authorized_commit(&self) -> bool {
        let now = Instant::now();
        let won = self
            .state
            .compare_exchange(OPEN, COMMITTING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if won {
            let _ = self.committing_since.set(now);
        }
        won
    }

    /// The acknowledgement deadline. `true` means COMMIT was not sent and never
    /// will be; `false` means the gate was already `Committing`.
    pub(super) fn close(&self) -> bool {
        self.close_for(GateClosure::DeadlineOrCancelled)
    }

    /// Close the gate for `reason`, as the deadline or a lease fence does.
    #[cfg(test)]
    pub(super) fn close_for_test(&self, reason: GateClosure) -> bool {
        self.close_for(reason)
    }

    /// Close an open or contended gate. The deadline, or a cancellation, that
    /// finds the append waiting out contention closes the gate for that
    /// contention, not for ledger latency (#716); a lease's own refusal after
    /// the wait closes it for that refusal.
    fn close_for(&self, reason: GateClosure) -> bool {
        let mut current = OPEN;
        loop {
            let closure = match (current, reason) {
                (CONTENDED, GateClosure::DeadlineOrCancelled) => GateClosure::AuthorityContended,
                _ => reason,
            };
            match self.state.compare_exchange(
                current,
                closure as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual @ (OPEN | CONTENDED)) => current = actual,
                Err(actual) => return actual != COMMITTING,
            }
        }
    }

    pub(super) fn closure(&self) -> Option<GateClosure> {
        match self.state.load(Ordering::Acquire) {
            2 => Some(GateClosure::DeadlineOrCancelled),
            3 => Some(GateClosure::AuthorityUnavailable),
            4 => Some(GateClosure::StaleAuthority),
            5 => Some(GateClosure::AuthorityContended),
            _ => None,
        }
    }

    pub(super) fn state(&self) -> GateState {
        match self.state.load(Ordering::Acquire) {
            OPEN => GateState::Open,
            CONTENDED => GateState::Contended,
            COMMITTING => GateState::Committing,
            _ => GateState::Closed,
        }
    }

    /// When COMMIT started, once the gate is `Committing`.
    pub(super) fn committing_since(&self) -> Option<Instant> {
        self.committing_since.get().copied()
    }
}

/// What a committed share append wrote.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Appended {
    /// The share, and its candidate if it carried one.
    Inserted,
    /// Nothing: the immutable row was already durable (a duplicate).
    Recorded,
    /// #657: the payout revision moved after the submit check, so the
    /// append captured its block instead of crediting the share. The block
    /// is enqueued as a #478 capture, the share deferred until it confirms.
    Captured,
}

pub(super) trait SubmitLedger: Send + Sync {
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>>;
    /// Append at `revision`, sending COMMIT only if `gate.begin_commit()`
    /// succeeds immediately before it.
    fn append_at_revision(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        revision: i64,
        gate: Arc<CommitGate>,
    ) -> BoxFuture<'_, Result<Appended>>;
    /// [`SubmitLedger::append_at_revision`], recording when the candidate's
    /// locally validated block proof was observed (a wall clock, UNIX ms),
    /// and capturing a candidate-bearing append's block if the revision
    /// moved when `moved` allows it (#657). A store without a durable
    /// candidate row has nowhere to keep either and keeps the plain append.
    fn append_at_revision_observed(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        _proof_observed_at_ms: Option<i64>,
        revision: i64,
        _moved: MovedRevision,
        gate: Arc<CommitGate>,
    ) -> BoxFuture<'_, Result<Appended>> {
        self.append_at_revision(share, candidate, revision, gate)
    }
}

impl SubmitLedger for Ledger {
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>> {
        Box::pin(Ledger::payout_revision(self))
    }

    fn append_at_revision(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        revision: i64,
        gate: Arc<CommitGate>,
    ) -> BoxFuture<'_, Result<Appended>> {
        self.append_at_revision_observed(
            share,
            candidate,
            None,
            revision,
            MovedRevision::Refuse,
            gate,
        )
    }

    fn append_at_revision_observed(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        proof_observed_at_ms: Option<i64>,
        revision: i64,
        moved: MovedRevision,
        gate: Arc<CommitGate>,
    ) -> BoxFuture<'_, Result<Appended>> {
        Box::pin(async move {
            let pre_commit = || gate.begin_commit();
            let appended = Ledger::append_at_revision_gated_observed(
                self,
                share,
                candidate,
                proof_observed_at_ms,
                revision,
                &pre_commit,
                moved,
            )
            .await?;
            Ok(if appended.captured {
                Appended::Captured
            } else if appended.inserted {
                Appended::Inserted
            } else {
                Appended::Recorded
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_gate_closure_wins_and_cannot_replace_commit() {
        let causes = [
            GateClosure::DeadlineOrCancelled,
            GateClosure::AuthorityUnavailable,
            GateClosure::StaleAuthority,
            GateClosure::AuthorityContended,
        ];
        for first in causes {
            for later in causes {
                let gate = CommitGate::default();
                assert!(gate.close_for(first));
                assert!(gate.close_for(later));
                assert!(gate.close());
                assert!(!gate.contend());
                assert!(!gate.reopen());
                assert!(!gate.begin_commit());
                assert_eq!(gate.closure(), Some(first));
                assert_eq!(gate.state(), GateState::Closed);
                assert!(gate.committing_since().is_none());
            }
            let gate = CommitGate::default();
            assert!(gate.begin_commit());
            assert!(!gate.close_for(first));
            assert!(!gate.contend());
            assert_eq!(gate.closure(), None);
            assert_eq!(gate.state(), GateState::Committing);
        }
    }

    /// #716: contention decides nothing. A contended gate is neither
    /// committing nor closed; it reopens for the append's next attempt, or
    /// closes, and the deadline closes it for the contention, never for
    /// ledger latency.
    #[test]
    fn a_contended_gate_reopens_or_closes_for_its_contention() {
        let gate = CommitGate::default();
        assert!(gate.contend());
        assert_eq!(gate.state(), GateState::Contended);
        assert_eq!(gate.closure(), None);
        assert!(!gate.begin_commit(), "a contended gate admits no COMMIT");
        assert!(gate.reopen());
        assert_eq!(gate.state(), GateState::Open);
        assert!(!gate.reopen());
        assert!(gate.begin_commit(), "the reopened gate admits the retry");
        assert!(!gate.close(), "the retry's COMMIT is in flight");
        assert_eq!(gate.state(), GateState::Committing);

        let gate = CommitGate::default();
        assert!(gate.contend());
        assert!(gate.close(), "the deadline closes a contended gate");
        assert_eq!(gate.closure(), Some(GateClosure::AuthorityContended));
        assert!(
            !gate.reopen(),
            "an attempt after the deadline cannot reopen it"
        );
        assert!(!gate.begin_commit());
        assert!(gate.committing_since().is_none());

        for refusal in [LeaseCommitRefusal::Stale, LeaseCommitRefusal::Unavailable] {
            let gate = CommitGate::default();
            assert!(gate.contend());
            assert!(gate.close_for(refusal.into()));
            assert_eq!(gate.closure(), Some(refusal.into()));
            assert!(gate.close(), "the deadline cannot replace the refusal");
            assert_eq!(gate.closure(), Some(refusal.into()));
            assert!(!gate.reopen());
        }
    }
}
