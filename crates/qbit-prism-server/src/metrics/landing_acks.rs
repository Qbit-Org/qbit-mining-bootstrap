//! Share acknowledgements around pool block landings (#602 R4, R7).
//!
//! A landing window opens when this frontend first observes a pool block's
//! acceptance: its own `submitblock` acceptance, or a peer frontend's block on
//! the active chain, seen before it is confirmed or already confirmed at the
//! tip (a peer's settlement can finish before this frontend's reconciler
//! looks). Settlement follows acceptance and holds `ORDER_LOCK`, which every
//! share append takes, so the acknowledgements of submissions that arrive
//! within [`WINDOW`] after it are the ones a slow settlement delays. They are
//! recorded twice: in `share_ack_seconds` as always, and in
//! `share_ack_landing_window_seconds`.
//!
//! The hot path pays one atomic load outside a window: the arrival instant is
//! the one the acknowledgement's own elapsed time is measured from, and the
//! window's end is published in an atomic. Only an acknowledgement inside a
//! window takes the window's mutex.
//!
//! Each window also gets a verdict once its acknowledgements have had
//! [`SETTLE`] to complete: whether its p99 exceeded each [`LandingAckBound`].
//! A p99 above a bound is exactly "more than one percent of the window's
//! acknowledgements took longer than the bound" (nearest rank), so the verdict
//! needs two counters, not the samples. The gauge publishes, per bound, how
//! many consecutive most recent windows exceeded it. An acceptance while a
//! window still admits arrivals extends that window rather than opening a
//! second one, so two landings seconds apart are one window and one verdict.
//! A window no submission arrived in has no verdict and leaves the streaks
//! as they were: a quiet landing proves nothing either way.
use super::LandingAckBound;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::Duration,
};
use tokio::time::Instant;

/// How long after an acceptance a submission's arrival still counts.
pub(super) const WINDOW: Duration = Duration::from_secs(30);

/// How long past the end of its admissions a window waits before its verdict,
/// so a submission that arrived at the last moment can still complete. The
/// share-commit timeout (15 s by default) plus its grace bound an
/// acknowledgement; a later one is still in the histogram, not the verdict.
pub(super) const SETTLE: Duration = Duration::from_secs(30);

impl LandingAckBound {
    const fn threshold(self) -> Duration {
        match self {
            Self::Ticket => Duration::from_secs(2),
            Self::Warning => Duration::from_secs(10),
        }
    }
}

pub(super) struct LandingAcks {
    epoch: Instant,
    /// Nanoseconds after `epoch` before which an arrival is admitted to the
    /// current window; zero before the first acceptance.
    admits_until: AtomicU64,
    windows: Mutex<Windows>,
}

#[derive(Default)]
struct Windows {
    open: Option<Window>,
    /// Per bound, in [`LandingAckBound::ALL`] order; `None` before a verdict.
    streaks: Option<[u32; 2]>,
}

struct Window {
    admits_until: Instant,
    acks: u64,
    over: [u64; 2],
}

impl Default for LandingAcks {
    fn default() -> Self {
        Self {
            epoch: Instant::now(),
            admits_until: AtomicU64::new(0),
            windows: Mutex::default(),
        }
    }
}

impl LandingAcks {
    fn offset(&self, at: Instant) -> u64 {
        // Zero is reserved for "no window"; an instant at the epoch itself
        // still admits nothing it should not, because it precedes every end.
        u64::try_from(at.saturating_duration_since(self.epoch).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1)
    }

    /// A pool block acceptance observed at `now` opens a window, or extends
    /// the one still admitting arrivals.
    pub(super) fn opened(&self, now: Instant) {
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        let until = now + WINDOW;
        match windows.open.as_mut() {
            Some(window) if now < window.admits_until => window.admits_until = until,
            _ => {
                windows.close();
                windows.open = Some(Window {
                    admits_until: until,
                    acks: 0,
                    over: [0; 2],
                });
            }
        }
        self.admits_until
            .store(self.offset(until), Ordering::Release);
    }

    /// Whether a submission that arrived at `received_at` falls in a window.
    /// The acknowledgement completes after any acceptance observed before it,
    /// so an arrival before the latest window's end is one that was in flight
    /// during that window.
    pub(super) fn admits(&self, received_at: Instant) -> bool {
        let until = self.admits_until.load(Ordering::Acquire);
        until != 0 && self.offset(received_at) < until
    }

    /// Count one admitted acknowledgement toward its window's verdict.
    pub(super) fn record(&self, received_at: Instant, elapsed: Duration) {
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(window) = windows
            .open
            .as_mut()
            .filter(|window| received_at < window.admits_until)
        {
            window.acks += 1;
            for (over, bound) in window.over.iter_mut().zip(LandingAckBound::ALL) {
                *over += u64::from(elapsed > bound.threshold());
            }
        }
    }

    /// Per bound, the consecutive most recent windows whose p99 exceeded it,
    /// or `None` before the first verdict. A window whose settle time has
    /// passed by `now` gets its verdict first.
    pub(super) fn streaks(&self, now: Instant) -> Option<[u32; 2]> {
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        if windows
            .open
            .as_ref()
            .is_some_and(|window| now >= window.admits_until + SETTLE)
        {
            windows.close();
        }
        windows.streaks
    }
}

impl Windows {
    fn close(&mut self) {
        let Some(window) = self.open.take() else {
            return;
        };
        if window.acks == 0 {
            return;
        }
        let streaks = self.streaks.get_or_insert([0; 2]);
        for (streak, over) in streaks.iter_mut().zip(window.over) {
            // p99 > bound iff more than 1% of the acknowledgements exceeded it.
            *streak = if over * 100 > window.acks {
                streak.saturating_add(1)
            } else {
                0
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn acks(state: &LandingAcks, arrived: Instant, elapsed: &[Duration]) {
        for elapsed in elapsed {
            assert!(state.admits(arrived));
            state.record(arrived, *elapsed);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_is_admitted_before_an_acceptance_or_after_its_window() {
        let state = LandingAcks::default();
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(!state.admits(Instant::now()));
        assert_eq!(state.streaks(Instant::now()), None);
        let before = Instant::now();
        tokio::time::advance(MS).await;
        state.opened(Instant::now());
        // In flight at the acceptance: admitted.
        assert!(state.admits(before));
        tokio::time::advance(WINDOW - MS).await;
        assert!(state.admits(Instant::now() - MS));
        tokio::time::advance(MS).await;
        assert!(!state.admits(Instant::now()));
    }

    #[tokio::test(start_paused = true)]
    async fn the_verdict_is_the_nearest_rank_p99_and_waits_for_settle() {
        let state = LandingAcks::default();
        state.opened(Instant::now());
        let arrived = Instant::now();
        // 100 acknowledgements, one over 2 s: p99 is the 99th, under 2 s.
        acks(&state, arrived, &[MS; 99]);
        acks(&state, arrived, &[Duration::from_secs(3)]);
        tokio::time::advance(WINDOW + SETTLE - MS).await;
        assert_eq!(state.streaks(Instant::now()), None, "verdict before settle");
        tokio::time::advance(MS).await;
        assert_eq!(state.streaks(Instant::now()), Some([0, 0]));

        // 100 more, two over 2 s and one over 10 s: p99 over 2 s, not 10 s.
        state.opened(Instant::now());
        let arrived = Instant::now();
        acks(&state, arrived, &[MS; 97]);
        acks(
            &state,
            arrived,
            &[Duration::from_secs(3), Duration::from_secs(3)],
        );
        acks(&state, arrived, &[Duration::from_secs(11)]);
        tokio::time::advance(WINDOW + SETTLE).await;
        assert_eq!(state.streaks(Instant::now()), Some([1, 0]));
    }

    #[tokio::test(start_paused = true)]
    async fn streaks_count_consecutive_slow_windows_and_quiet_windows_are_skipped() {
        let state = LandingAcks::default();
        let window = |slow: Option<Duration>| {
            state.opened(Instant::now());
            if let Some(elapsed) = slow {
                acks(&state, Instant::now(), &[elapsed]);
            }
        };
        for _ in 0..3 {
            window(Some(Duration::from_millis(2_500)));
            tokio::time::advance(WINDOW + SETTLE).await;
        }
        assert_eq!(state.streaks(Instant::now()), Some([3, 0]));
        // No submission arrived: no verdict, the streak stands.
        window(None);
        tokio::time::advance(WINDOW + SETTLE).await;
        assert_eq!(state.streaks(Instant::now()), Some([3, 0]));
        window(Some(Duration::from_secs(12)));
        tokio::time::advance(WINDOW + SETTLE).await;
        assert_eq!(state.streaks(Instant::now()), Some([4, 1]));
        // Exactly the bound is not over it.
        window(Some(Duration::from_secs(2)));
        tokio::time::advance(WINDOW + SETTLE).await;
        assert_eq!(state.streaks(Instant::now()), Some([0, 0]));
    }

    #[tokio::test(start_paused = true)]
    async fn an_acceptance_inside_a_window_extends_it_and_a_later_one_closes_it() {
        let state = LandingAcks::default();
        state.opened(Instant::now());
        acks(&state, Instant::now(), &[Duration::from_secs(3)]);
        tokio::time::advance(Duration::from_secs(20)).await;
        state.opened(Instant::now());
        tokio::time::advance(Duration::from_secs(25)).await;
        // 45 s after the first acceptance, 25 s after the second: admitted.
        acks(&state, Instant::now(), &[MS]);
        assert_eq!(
            state.streaks(Instant::now()),
            None,
            "one window, still open"
        );
        // Past the extended window's admissions but before its settle, a new
        // acceptance takes the verdict now: one of two over 2 s.
        tokio::time::advance(Duration::from_secs(10)).await;
        state.opened(Instant::now());
        assert_eq!(state.streaks(Instant::now()), Some([1, 0]));
    }
}
