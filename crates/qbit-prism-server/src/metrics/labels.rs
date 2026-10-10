//! Every label dimension has a finite set of values.
use std::cmp::Ordering;

/// Event keys stay inline; snapshot callers may still supply arbitrary values.
/// Compare the pairs, not their storage, so both forms address the same sample.
#[derive(Clone)]
pub(super) enum Labels {
    Empty,
    One((&'static str, &'static str)),
    Two((&'static str, &'static str), (&'static str, &'static str)),
    Owned(Vec<(&'static str, String)>),
}

impl Labels {
    pub(super) fn iter(&self) -> impl Iterator<Item = (&'static str, &str)> {
        let (inline, owned): (_, &[(&'static str, String)]) = match self {
            Self::Empty => ([None, None], &[]),
            Self::One(pair) => ([Some(*pair), None], &[]),
            Self::Two(first, second) => ([Some(*first), Some(*second)], &[]),
            Self::Owned(pairs) => ([None, None], pairs),
        };
        inline
            .into_iter()
            .flatten()
            .chain(owned.iter().map(|(key, value)| (*key, value.as_str())))
    }
}

impl From<Vec<(&'static str, String)>> for Labels {
    fn from(pairs: Vec<(&'static str, String)>) -> Self {
        Self::Owned(pairs)
    }
}

impl PartialEq for Labels {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Labels {}
impl PartialOrd for Labels {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Labels {
    fn cmp(&self, other: &Self) -> Ordering {
        self.iter().cmp(other.iter())
    }
}

macro_rules! labels {
    ($name:ident { $($variant:ident => $label:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
        pub enum $name { $($variant),+ }
        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $label),+ }
            }
        }
    };
}

labels!(AckResult { Accepted => "accepted", Rejected => "rejected" });
labels!(Outcome { Success => "success", Failure => "failure" });
labels!(RevisionWorkResult {
    Published => "published", Degraded => "degraded", Superseded => "superseded"
});
labels!(LockKind { Migration => "migration", Order => "order", Settlement => "settlement" });
labels!(Collector { Database => "database", Process => "process" });
labels!(TaskKind {
    Refresh => "refresh", Submit => "submit", BlockWait => "block_wait",
    Broadcast => "broadcast", Rollup => "rollup", HealthPublisher => "health_publisher",
    StratumListener => "stratum_listener", StratumSession => "stratum_session",
    Collector => "collector", SharePartitions => "share_partitions"
});
labels!(RejectReason {
    StaleJob => "stale-job", DuplicateShare => "duplicate-share", LowDifficulty => "low-difficulty",
    MalformedSubmit => "malformed-submit", UnauthorizedWorker => "unauthorized-worker",
    UnknownJob => "unknown-job", InvalidExtranonce => "invalid-extranonce",
    InvalidNtimeOrNonce => "invalid-ntime-or-nonce",
    BackendRpcUnavailable => "backend-rpc-unavailable",
    BackendDatabaseUnavailable => "backend-database-unavailable",
    InternalError => "internal-error", PoolClosed => "pool-closed",
    LedgerConfirmationFailed => "ledger-confirmation-failed",
    LedgerOutcomeUnknown => "ledger-outcome-unknown", Unrecognised => "unrecognised"
});
// Admission limits and per-session budgets only; never a peer address or username.
labels!(ConnectionRefusalReason {
    GlobalLimit => "global_limit", UsernameLimit => "username_limit", IpLimit => "ip_limit",
    MalformedFrameBudget => "malformed_frame_budget", UnknownJobBudget => "unknown_job_budget",
    AuthorizeBudget => "authorize_budget"
});
// How a refresh snapshot acquired its payout window: the delta path advanced
// the retired window, or the reason the newest-first full scan ran instead.
labels!(WindowAcquisition {
    Advanced => "advanced", NoPrior => "no_prior", EmptyPrior => "empty_prior",
    TailMismatch => "tail_mismatch", AnchorRegressed => "anchor_regressed",
    CutoffRegressed => "cutoff_regressed", DeltaTooLarge => "delta_too_large",
    NoEvidence => "no_evidence", LeafChanged => "leaf_changed",
    MarginTooLarge => "margin_too_large", Partial => "partial",
    WitnessChanged => "witness_changed", CountMismatch => "count_mismatch",
    Invariant => "invariant"
});
// What invalidated the published work and made a refresh rebuild it.
labels!(RefreshTrigger {
    Initial => "initial", Tip => "tip", Revision => "revision", Balances => "balances",
    Reanchor => "reanchor", Shares => "shares", Template => "template", Fee => "fee",
    WriterTimeline => "writer_timeline"
});
// Where the rebuilt work's window came from: the delta path, the full scan,
// or the cached window the previous refresh captured.
labels!(RefreshAcquisition { Delta => "delta", Full => "full", Cached => "cached" });
// The stale-job decision that refused a share. The wire reason stays `stale-job`.
labels!(StaleJobCause {
    ResumeExpired => "resume_expired", FeeFloor => "fee_floor",
    ParentGrace => "parent_grace", PayoutRevision => "payout_revision",
    WindowNotHeld => "window_not_held"
});
// #622: the first check that refused a job preparation. Tip authority's own
// refusals come first, so a stale poll masks retired work behind it.
labels!(JobDeferral {
    TipPollingStale => "tip_polling_stale", TipPollingUnavailable => "tip_polling_unavailable",
    NewTipPending => "new_tip_pending", WorkRetired => "work_retired",
    FeeFloor => "fee_floor", Other => "other"
});
// #478: what the offer did with a pending block on the current tip whose
// payout revision was superseded.
labels!(CaptureDecision {
    Offered => "offered", AbandonedCeiling => "abandoned_ceiling",
    AbandonedDisabled => "abandoned_disabled"
});
// #529: how a found block's wait for the failover standby's flush ended
// before its offer; the block is offered in every case.
labels!(StandbyWaitOutcome {
    Confirmed => "confirmed", Absent => "absent", Lagging => "lagging", Failed => "failed"
});
// #574: which wait a block-bearing submission's acknowledgement cap ended: the
// share-pass append that carries the block, or a block-only, deferred or
// captured proof's wait for its enqueue and landing.
labels!(BlockAckPath { Share => "share", BlockOnly => "block_only" });
// #602: the transaction that held ORDER_LOCK, one value per kind of holder:
// the share append, a block-only candidate insert, the offering frontend's
// settlement (first_confirmation when that transaction confirmed the block
// for the first time, settlement otherwise, including an abandon), the reorg
// reconciler, the orphan disposition, the refresh's window snapshot that
// prepared work is built from, blob cleanup, fatal-state recovery, the
// operator commands (policy and signing transitions, archive verify and
// restore), and the 3.1 dual writer's identity upkeep (a node's
// personalisation, and the peer sync raising the share sequence above the
// peer's rows). The schema cutover's hold is not observed.
labels!(OrderLockHolder {
    Append => "append", CandidateInsert => "candidate_insert",
    FirstConfirmation => "first_confirmation", Settlement => "settlement",
    Reconcile => "reconcile", Orphan => "orphan", Prepared => "prepared",
    Cleanup => "cleanup", FatalState => "fatal_state", Operator => "operator",
    PeerSync => "peer_sync"
});
// #602: the share acknowledgement p99 bound, in seconds, a landing window is
// judged against: 2 s tickets after three windows in a row, 10 s warns.
labels!(LandingAckBound { Ticket => "2", Warning => "10" });

impl RejectReason {
    /// Metrics normalization must not alter the existing protocol response.
    /// Missing reasons retain the legacy internal-error classification; present
    /// but unrecognised IDs (including empty strings) signal label drift.
    pub fn from_reason_id(reason: Option<&str>) -> Self {
        let Some(reason) = reason else {
            return Self::InternalError;
        };
        Self::ALL
            .iter()
            .copied()
            .find(|value| value.as_str() == reason)
            .unwrap_or(Self::Unrecognised)
    }
}
