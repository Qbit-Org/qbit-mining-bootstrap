//! Every label dimension has a finite set of values.
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
    Collector => "collector"
});
labels!(RejectReason {
    StaleJob => "stale-job", DuplicateShare => "duplicate-share", LowDifficulty => "low-difficulty",
    MalformedSubmit => "malformed-submit", UnauthorizedWorker => "unauthorized-worker",
    UnknownJob => "unknown-job", InvalidExtranonce => "invalid-extranonce",
    InvalidNtimeOrNonce => "invalid-ntime-or-nonce",
    BackendRpcUnavailable => "backend-rpc-unavailable",
    InternalError => "internal-error", PoolClosed => "pool-closed",
    LedgerConfirmationFailed => "ledger-confirmation-failed"
});

impl RejectReason {
    /// Metrics normalization must not alter the existing protocol response.
    pub fn from_reason_id(reason: Option<&str>) -> Self {
        Self::ALL
            .iter()
            .copied()
            .find(|value| Some(value.as_str()) == reason)
            .unwrap_or(Self::InternalError)
    }
}
