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
// The stale-job decision that refused a share. The wire reason stays `stale-job`.
labels!(StaleJobCause {
    ResumeExpired => "resume_expired", FeeFloor => "fee_floor",
    ParentGrace => "parent_grace", PayoutRevision => "payout_revision"
});

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
