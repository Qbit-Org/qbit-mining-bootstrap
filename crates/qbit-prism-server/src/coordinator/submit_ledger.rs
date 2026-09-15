//! The two storage boundaries used by ordinary share admission.
//!
//! Tests replace I/O here, not Coordinator's classification/accounting logic.
//! Production appends retain the transaction-scoped payout revision check.
use super::publication_authority::LeaseCommitFence;
use super::*;
use futures_util::future::BoxFuture;
use std::sync::{atomic::AtomicU8, OnceLock};

const OPEN: u8 = 0;
const COMMITTING: u8 = 1;
const CLOSED: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GateState {
    Open,
    Committing,
    Closed,
}

/// One-shot arbiter between an append's COMMIT and its acknowledgement
/// deadline. Both transitions leave `Open` by compare-and-swap, so exactly one
/// of them wins: an append sends COMMIT only after `begin_commit` succeeds,
/// and once `close` succeeds it never will.
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
    /// The append's pre-commit hook. `true` lets COMMIT be sent, and records
    /// when it started.
    pub(super) fn begin_commit(&self) -> bool {
        if let Some(lease) = &self.lease {
            let won = lease.with_authority(|| self.begin_authorized_commit());
            if !won {
                self.close();
            }
            return won;
        }
        self.begin_authorized_commit()
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
        match self
            .state
            .compare_exchange(OPEN, CLOSED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => true,
            Err(current) => current == CLOSED,
        }
    }

    pub(super) fn state(&self) -> GateState {
        match self.state.load(Ordering::Acquire) {
            OPEN => GateState::Open,
            COMMITTING => GateState::Committing,
            _ => GateState::Closed,
        }
    }

    /// When COMMIT started, once the gate is `Committing`.
    pub(super) fn committing_since(&self) -> Option<Instant> {
        self.committing_since.get().copied()
    }
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
    ) -> BoxFuture<'_, Result<bool>>;
    /// [`SubmitLedger::append_at_revision`], recording when the candidate's
    /// locally validated block proof was observed (a wall clock, UNIX ms).
    /// A store without a durable candidate row has nowhere to keep it and
    /// keeps the plain append.
    fn append_at_revision_observed(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        _proof_observed_at_ms: Option<i64>,
        revision: i64,
        gate: Arc<CommitGate>,
    ) -> BoxFuture<'_, Result<bool>> {
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
    ) -> BoxFuture<'_, Result<bool>> {
        self.append_at_revision_observed(share, candidate, None, revision, gate)
    }

    fn append_at_revision_observed(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        proof_observed_at_ms: Option<i64>,
        revision: i64,
        gate: Arc<CommitGate>,
    ) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            let pre_commit = || gate.begin_commit();
            Ok(Ledger::append_at_revision_gated_observed(
                self,
                share,
                candidate,
                proof_observed_at_ms,
                revision,
                &pre_commit,
            )
            .await?
            .inserted)
        })
    }
}
