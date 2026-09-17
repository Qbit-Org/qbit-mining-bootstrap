//! Bounded, credential-safe events for completed public readiness probes.
//!
//! Decide under the snapshot lock; emit only after publishing and unlocking it.
use super::ProbeFailure;
use std::time::{Duration, Instant};

const REMINDER_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct ReadinessEvents {
    last_warning: Option<(ProbeFailure, Instant)>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ReadinessEvent {
    Failed(ProbeFailure, &'static str),
    Recovered,
}

impl ReadinessEvents {
    pub(super) fn observe(
        &mut self,
        failure: Option<(ProbeFailure, &'static str)>,
        healthy: bool,
        now: Instant,
    ) -> Option<ReadinessEvent> {
        if let Some((failure, phase)) = failure {
            let warn = self.last_warning.is_none_or(|(previous, warned_at)| {
                failure != previous || now.saturating_duration_since(warned_at) >= REMINDER_INTERVAL
            });
            if warn {
                // Track the decision even if the subscriber filters the event.
                self.last_warning = Some((failure, now));
                return Some(ReadinessEvent::Failed(failure, phase));
            }
        } else if healthy && self.last_warning.take().is_some() {
            return Some(ReadinessEvent::Recovered);
        }
        None
    }
}

impl ReadinessEvent {
    pub(super) fn emit(self) {
        match self {
            Self::Failed(failure, phase) => failure.log(phase),
            Self::Recovered => tracing::info!("public readiness probe recovered"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_failure_and_unchanged_reminders_use_exact_monotonic_boundaries() {
        let mut events = ReadinessEvents::default();
        let start = Instant::now();
        let failure = Some((ProbeFailure::Connection, "schema"));
        assert_eq!(
            events.observe(failure, false, start),
            Some(ReadinessEvent::Failed(ProbeFailure::Connection, "schema"))
        );
        for elapsed in [
            Duration::ZERO,
            Duration::from_secs(5),
            REMINDER_INTERVAL - Duration::from_nanos(1),
        ] {
            assert_eq!(events.observe(failure, false, start + elapsed), None);
        }
        assert_eq!(
            events.observe(failure, false, start + REMINDER_INTERVAL),
            Some(ReadinessEvent::Failed(ProbeFailure::Connection, "schema"))
        );
        assert_eq!(
            events.observe(failure, false, start + REMINDER_INTERVAL),
            None
        );
        assert_eq!(
            events.observe(
                failure,
                false,
                start + REMINDER_INTERVAL * 2 - Duration::from_nanos(1)
            ),
            None
        );
        assert_eq!(
            events.observe(failure, false, start + REMINDER_INTERVAL * 2),
            Some(ReadinessEvent::Failed(ProbeFailure::Connection, "schema"))
        );
        // A long gap emits one event and resets the budget, without catch-up.
        let later = start + REMINDER_INTERVAL * 20;
        assert!(events.observe(failure, false, later).is_some());
        assert_eq!(events.observe(failure, false, later), None);
    }

    #[test]
    fn category_changes_warn_immediately_and_restart_the_reminder_budget() {
        let mut events = ReadinessEvents::default();
        let start = Instant::now();
        events.observe(Some((ProbeFailure::Connection, "schema")), false, start);
        assert_eq!(
            events.observe(Some((ProbeFailure::Connection, "replica")), false, start),
            None,
            "phase alone is not a category change"
        );
        let changed = start + Duration::from_secs(59);
        let failure = Some((ProbeFailure::Timeout, "probe"));
        assert_eq!(
            events.observe(failure, false, changed),
            Some(ReadinessEvent::Failed(ProbeFailure::Timeout, "probe"))
        );
        assert_eq!(
            events.observe(failure, false, start + REMINDER_INTERVAL),
            None
        );
        assert_eq!(
            events.observe(
                failure,
                false,
                changed + REMINDER_INTERVAL - Duration::from_nanos(1)
            ),
            None
        );
        assert!(events
            .observe(failure, false, changed + REMINDER_INTERVAL)
            .is_some());
        assert_eq!(
            events.observe(
                Some((ProbeFailure::Connection, "replica")),
                false,
                changed + REMINDER_INTERVAL
            ),
            Some(ReadinessEvent::Failed(ProbeFailure::Connection, "replica"))
        );
    }

    #[test]
    fn recovery_is_once_per_episode_and_flapping_keeps_each_transition() {
        let mut events = ReadinessEvents::default();
        let now = Instant::now();
        assert_eq!(events.observe(None, true, now), None, "healthy startup");
        let failure = Some((ProbeFailure::Schema, "schema"));
        for _ in 0..3 {
            assert_eq!(
                events.observe(failure, false, now),
                Some(ReadinessEvent::Failed(ProbeFailure::Schema, "schema"))
            );
            assert_eq!(events.observe(failure, false, now), None);
            assert_eq!(events.observe(None, false, now), None, "still unhealthy");
            assert_eq!(
                events.observe(None, true, now),
                Some(ReadinessEvent::Recovered)
            );
            assert_eq!(events.observe(None, true, now), None, "already healthy");
        }
    }
}
