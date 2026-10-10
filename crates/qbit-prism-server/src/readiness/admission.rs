//! Whether this frontend admits miners (3.1): the decision behind the
//! readiness endpoint the Hashbalancer checks and, in dual-writer mode, behind
//! the Stratum listeners, which accept connections only while it admits.
//!
//! `ready` flips false on every new tip and payout revision until replacement
//! work is published, so admission tolerates a dip shorter than the grace
//! (`PRISM_READINESS_GRACE_SECONDS`, ten seconds by default: the bound of the
//! operator readiness contract in `docs/prism-ha-reference-architecture.md`).
//! The grace runs from the last observation that found the frontend ready,
//! not from the first that did not: a publisher stalled for longer than the
//! grace, as by a dead database, withdraws the frontend at its next
//! publication instead of granting it a fresh grace. A hard fault, one that
//! says this node must not take miners at all, withdraws at once. A frontend
//! that has never been ready admits nothing.
use std::time::{Duration, Instant};

/// The default `PRISM_READINESS_GRACE_SECONDS`.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(10);
/// The longest grace `PRISM_READINESS_GRACE_SECONDS` accepts.
const MAX_GRACE_SECONDS: u64 = 120;

/// `PRISM_READINESS_GRACE_SECONDS`: 0 to 120, ten by default.
pub fn grace_from_env() -> anyhow::Result<Duration> {
    let seconds = crate::config::number("PRISM_READINESS_GRACE_SECONDS", DEFAULT_GRACE.as_secs())?;
    anyhow::ensure!(
        seconds <= MAX_GRACE_SECONDS,
        "PRISM_READINESS_GRACE_SECONDS must be 0..{MAX_GRACE_SECONDS}"
    );
    Ok(Duration::from_secs(seconds))
}

/// Why a frontend does not admit miners.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Withdrawal {
    /// Dual mode: this node's own share log is not yet caught up from the
    /// peer, as after a restore from an old backup.
    OwnLogBehind,
    /// Dual mode: the connected database is not this node's writable
    /// primary, or has not answered for longer than the writer probe allows.
    WriterNotLocal,
    /// Readiness stayed false for longer than the grace.
    NotReady,
}

impl Withdrawal {
    pub const ALL: [Self; 3] = [Self::OwnLogBehind, Self::WriterNotLocal, Self::NotReady];

    pub fn label(self) -> crate::metrics::WithdrawalReason {
        use crate::metrics::WithdrawalReason;
        match self {
            Self::OwnLogBehind => WithdrawalReason::OwnLogBehind,
            Self::WriterNotLocal => WithdrawalReason::WriterNotLocal,
            Self::NotReady => WithdrawalReason::NotReady,
        }
    }

    /// The metric, health and log label of the reason.
    pub fn as_str(self) -> &'static str {
        self.label().as_str()
    }

    /// A fault that says this node must not serve at all, whatever its work:
    /// it withdraws at once, and closes the sessions already accepted too.
    pub fn is_hard(self) -> bool {
        !matches!(self, Self::NotReady)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionState {
    /// Never admitted since the process started.
    Starting,
    Admitting,
    /// Still admitting, with readiness false since the observation after
    /// `since`, the last that found the frontend ready, which the grace runs
    /// from.
    Grace {
        since: Instant,
    },
    /// Admitted once, then withdrawn; `reason` is what blocks it now.
    Withdrawn {
        reason: Withdrawal,
    },
}

impl AdmissionState {
    pub fn admits(self) -> bool {
        matches!(self, Self::Admitting | Self::Grace { .. })
    }

    pub fn label(self) -> crate::metrics::AdmissionStateLabel {
        use crate::metrics::AdmissionStateLabel;
        match self {
            Self::Starting => AdmissionStateLabel::Starting,
            Self::Admitting => AdmissionStateLabel::Admitting,
            Self::Grace { .. } => AdmissionStateLabel::Grace,
            Self::Withdrawn { .. } => AdmissionStateLabel::Withdrawn,
        }
    }

    /// What blocks a withdrawn frontend now.
    pub fn reason(self) -> Option<Withdrawal> {
        match self {
            Self::Withdrawn { reason } => Some(reason),
            _ => None,
        }
    }

    /// The metric and health label of the state.
    pub fn as_str(self) -> &'static str {
        self.label().as_str()
    }
}

/// A change of whether the frontend admits, for its log line and counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionChange {
    Admitted,
    Withdrawn(Withdrawal),
}

#[derive(Debug)]
pub struct Admission {
    grace: Duration,
    state: AdmissionState,
    /// The last observation that found the frontend ready without a hard
    /// fault.
    last_ready: Option<Instant>,
}

impl Admission {
    pub fn new(grace: Duration) -> Self {
        Self {
            grace,
            state: AdmissionState::Starting,
            last_ready: None,
        }
    }

    pub fn state(&self) -> AdmissionState {
        self.state
    }

    pub fn grace(&self) -> Duration {
        self.grace
    }

    /// Fold in one readiness observation: `ready` is the frontend's
    /// instantaneous readiness, and `hard` a fault that forbids serving
    /// whatever its work. Returns the change of admission it caused, if any.
    pub fn observe(
        &mut self,
        now: Instant,
        ready: bool,
        hard: Option<Withdrawal>,
    ) -> Option<AdmissionChange> {
        let before = self.state;
        self.state = match (before, hard) {
            (AdmissionState::Starting, None) if ready => AdmissionState::Admitting,
            (AdmissionState::Starting, _) => AdmissionState::Starting,
            (_, Some(reason)) => AdmissionState::Withdrawn { reason },
            (_, None) if ready => AdmissionState::Admitting,
            (AdmissionState::Withdrawn { .. }, None) => AdmissionState::Withdrawn {
                reason: Withdrawal::NotReady,
            },
            (AdmissionState::Admitting, None) => {
                self.within_grace(self.last_ready.unwrap_or(now), now)
            }
            (AdmissionState::Grace { since }, None) => self.within_grace(since, now),
        };
        if ready && hard.is_none() {
            self.last_ready = Some(now);
        }
        match (before.admits(), self.state) {
            (false, now) if now.admits() => Some(AdmissionChange::Admitted),
            (true, AdmissionState::Withdrawn { reason }) => {
                Some(AdmissionChange::Withdrawn(reason))
            }
            _ => None,
        }
    }

    /// Ready last at `since`: admitting until the grace from then runs out.
    fn within_grace(&self, since: Instant, now: Instant) -> AdmissionState {
        if now.saturating_duration_since(since) >= self.grace {
            AdmissionState::Withdrawn {
                reason: Withdrawal::NotReady,
            }
        } else {
            AdmissionState::Grace { since }
        }
    }
}

/// The health publisher's latest admission decision, as the Stratum
/// listeners and the readiness endpoint read it. A decision older than the
/// health freshness budget admits nothing, so a publisher that stops
/// publishing closes the listeners and fails the endpoint, as `/healthz`
/// reports such a snapshot stale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionSignal {
    admits: bool,
    decided_at: Option<tokio::time::Instant>,
    /// The hard withdrawal this decision carries, if any.
    hard: Option<Withdrawal>,
}

impl AdmissionSignal {
    /// Before the first decision: nothing is admitted.
    pub const UNDECIDED: Self = Self {
        admits: false,
        decided_at: None,
        hard: None,
    };

    pub fn decided(admits: bool, at: tokio::time::Instant) -> Self {
        Self {
            admits,
            decided_at: Some(at),
            hard: None,
        }
    }

    /// The decision `state` makes at `at`.
    pub fn of(state: AdmissionState, at: tokio::time::Instant) -> Self {
        Self {
            admits: state.admits(),
            decided_at: Some(at),
            hard: state.reason().filter(|reason| reason.is_hard()),
        }
    }

    /// The hard withdrawal this decision carries: the gated listeners close
    /// their accepted sessions on it, not only their listening sockets.
    pub fn hard_withdrawal(&self) -> Option<Withdrawal> {
        self.hard
    }

    /// Whether the decision admits at `now`: it says so and is younger than
    /// `stale_after`, so it admits strictly before [`Self::stale_at`] and not
    /// at it. The readiness endpoint and the gated listeners both ask this.
    pub fn admits_at(&self, now: tokio::time::Instant, stale_after: Duration) -> bool {
        self.stale_at(stale_after).is_some_and(|stale| now < stale)
    }

    /// When an admitting decision goes stale; `None` when it admits nothing.
    pub fn stale_at(&self, stale_after: Duration) -> Option<tokio::time::Instant> {
        self.decided_at
            .filter(|_| self.admits)
            .map(|at| at + stale_after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GRACE: Duration = Duration::from_secs(10);

    #[test]
    fn a_signal_carries_only_a_hard_withdrawal() {
        let at = tokio::time::Instant::now();
        for reason in [Withdrawal::OwnLogBehind, Withdrawal::WriterNotLocal] {
            let signal = AdmissionSignal::of(AdmissionState::Withdrawn { reason }, at);
            assert_eq!(signal.hard_withdrawal(), Some(reason));
            assert!(!signal.admits_at(at, Duration::from_secs(15)));
        }
        let soft = AdmissionSignal::of(
            AdmissionState::Withdrawn {
                reason: Withdrawal::NotReady,
            },
            at,
        );
        assert_eq!(soft.hard_withdrawal(), None);
        for state in [
            AdmissionState::Starting,
            AdmissionState::Admitting,
            AdmissionState::Grace {
                since: Instant::now(),
            },
        ] {
            assert_eq!(AdmissionSignal::of(state, at).hard_withdrawal(), None);
        }
        assert!(AdmissionSignal::of(AdmissionState::Admitting, at)
            .admits_at(at, Duration::from_secs(15)));
    }

    #[test]
    fn a_signal_admits_only_while_its_decision_is_fresh() {
        let budget = Duration::from_secs(15);
        let at = tokio::time::Instant::now();
        assert!(!AdmissionSignal::UNDECIDED.admits_at(at, budget));
        assert_eq!(AdmissionSignal::UNDECIDED.stale_at(budget), None);
        let refused = AdmissionSignal::decided(false, at);
        assert!(!refused.admits_at(at, budget));
        assert_eq!(refused.stale_at(budget), None);
        let admitted = AdmissionSignal::decided(true, at);
        assert!(admitted.admits_at(at, budget));
        assert!(admitted.admits_at(at + budget - Duration::from_millis(1), budget));
        // Stale at exactly `stale_at`: a listener that sleeps until then
        // closes instead of computing the same deadline again.
        assert_eq!(admitted.stale_at(budget), Some(at + budget));
        assert!(!admitted.admits_at(at + budget, budget));
        assert!(!admitted.admits_at(at + budget + Duration::from_millis(1), budget));
    }

    fn at(start: Instant, seconds: u64) -> Instant {
        start + Duration::from_secs(seconds)
    }

    #[test]
    fn a_frontend_never_ready_admits_nothing_whatever_the_time() {
        let start = Instant::now();
        let mut admission = Admission::new(GRACE);
        for seconds in [0, 5, 60, 3600] {
            assert_eq!(admission.observe(at(start, seconds), false, None), None);
            assert_eq!(admission.state(), AdmissionState::Starting);
        }
        // Ready work behind a hard fault is still not admitted.
        for hard in Withdrawal::ALL {
            assert_eq!(admission.observe(at(start, 3601), true, Some(hard)), None);
            assert_eq!(admission.state(), AdmissionState::Starting);
            assert!(!admission.state().admits());
        }
        assert_eq!(
            admission.observe(at(start, 3602), true, None),
            Some(AdmissionChange::Admitted)
        );
        assert_eq!(admission.state(), AdmissionState::Admitting);
    }

    #[test]
    fn a_dip_shorter_than_the_grace_keeps_admitting() {
        let start = Instant::now();
        let mut admission = Admission::new(GRACE);
        admission.observe(start, true, None);
        assert_eq!(admission.observe(at(start, 1), false, None), None);
        // The grace runs from the last ready observation.
        assert_eq!(admission.state(), AdmissionState::Grace { since: start });
        assert_eq!(admission.observe(at(start, 9), false, None), None);
        assert!(admission.state().admits());
        assert_eq!(admission.observe(at(start, 9), true, None), None);
        assert_eq!(admission.state(), AdmissionState::Admitting);
    }

    #[test]
    fn a_publication_later_than_the_grace_after_the_last_ready_one_withdraws_at_once() {
        // A publisher stalled by a dead database publishes again 30 s after
        // it last found the frontend ready: no fresh grace.
        let start = Instant::now();
        let mut admission = Admission::new(GRACE);
        admission.observe(start, true, None);
        assert_eq!(
            admission.observe(at(start, 30), false, None),
            Some(AdmissionChange::Withdrawn(Withdrawal::NotReady))
        );
        assert!(!admission.state().admits());
    }

    #[test]
    fn a_dip_after_readiness_returns_measures_from_the_new_ready_observation() {
        let start = Instant::now();
        let mut admission = Admission::new(GRACE);
        admission.observe(start, true, None);
        admission.observe(at(start, 5), false, None);
        assert_eq!(admission.observe(at(start, 8), true, None), None);
        assert_eq!(admission.observe(at(start, 15), false, None), None);
        assert_eq!(
            admission.state(),
            AdmissionState::Grace {
                since: at(start, 8)
            }
        );
        assert_eq!(admission.observe(at(start, 17), false, None), None);
        assert!(admission.state().admits());
        assert_eq!(
            admission.observe(at(start, 18), false, None),
            Some(AdmissionChange::Withdrawn(Withdrawal::NotReady))
        );
    }

    #[test]
    fn readiness_false_for_the_whole_grace_withdraws_and_ready_readmits() {
        let start = Instant::now();
        let mut admission = Admission::new(GRACE);
        admission.observe(start, true, None);
        admission.observe(at(start, 2), false, None);
        assert_eq!(admission.observe(at(start, 9), false, None), None);
        assert_eq!(
            admission.observe(at(start, 10), false, None),
            Some(AdmissionChange::Withdrawn(Withdrawal::NotReady))
        );
        assert_eq!(
            admission.state(),
            AdmissionState::Withdrawn {
                reason: Withdrawal::NotReady
            }
        );
        assert_eq!(admission.observe(at(start, 13), false, None), None);
        assert_eq!(
            admission.observe(at(start, 14), true, None),
            Some(AdmissionChange::Admitted)
        );
        assert_eq!(admission.state(), AdmissionState::Admitting);
    }

    #[test]
    fn a_hard_fault_withdraws_at_once_from_admitting_and_from_the_grace() {
        for hard in [Withdrawal::OwnLogBehind, Withdrawal::WriterNotLocal] {
            let start = Instant::now();
            let mut admission = Admission::new(GRACE);
            admission.observe(start, true, None);
            assert_eq!(
                admission.observe(at(start, 1), true, Some(hard)),
                Some(AdmissionChange::Withdrawn(hard)),
                "{hard:?} with ready work"
            );
            assert_eq!(
                admission.state(),
                AdmissionState::Withdrawn { reason: hard }
            );
            admission.observe(at(start, 2), true, None);
            admission.observe(at(start, 3), false, None);
            assert!(matches!(admission.state(), AdmissionState::Grace { .. }));
            assert_eq!(
                admission.observe(at(start, 4), false, Some(hard)),
                Some(AdmissionChange::Withdrawn(hard)),
                "{hard:?} within the grace"
            );
        }
    }

    #[test]
    fn a_withdrawn_frontend_reports_what_blocks_it_now() {
        let start = Instant::now();
        let mut admission = Admission::new(GRACE);
        admission.observe(start, true, None);
        admission.observe(at(start, 1), true, Some(Withdrawal::WriterNotLocal));
        // The writer is local again but the work is not yet current.
        assert_eq!(admission.observe(at(start, 2), false, None), None);
        assert_eq!(
            admission.state(),
            AdmissionState::Withdrawn {
                reason: Withdrawal::NotReady
            }
        );
        assert_eq!(
            admission.observe(at(start, 3), false, Some(Withdrawal::OwnLogBehind)),
            None
        );
        assert_eq!(
            admission.state(),
            AdmissionState::Withdrawn {
                reason: Withdrawal::OwnLogBehind
            }
        );
    }

    #[test]
    fn a_zero_grace_withdraws_on_the_first_false_readiness() {
        let start = Instant::now();
        let mut admission = Admission::new(Duration::ZERO);
        admission.observe(start, true, None);
        assert_eq!(
            admission.observe(start, false, None),
            Some(AdmissionChange::Withdrawn(Withdrawal::NotReady))
        );
    }
}
