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
//! A later acceptance opens a new window and leaves the previous one settling:
//! its late acknowledgements still count toward its own verdict.
//! A window no submission arrived in has no verdict and leaves the streaks
//! as they were: a quiet landing proves nothing either way.
//!
//! A block opens at most one window, whichever observation of it comes
//! first, so the reconciler seeing a block this frontend offered does not
//! open a second one. Blocks are told apart by hash, not height: a reorg's
//! replacement pool block at the same or a lower height is a new settlement. This state is independent of the revision-work
//! tracker in `landing.rs`: that tracker's capacity limits never stop a
//! window from opening.
use super::LandingAckBound;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::Duration,
};
use tokio::time::Instant;

/// Blocks remembered as having opened a window: far more than can land
/// within any observation's reach of each other.
const REMEMBERED: usize = 64;

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
    /// The previous window, no longer admitting but still settling when a
    /// later acceptance opened `open`: its late acknowledgements still count
    /// toward its own verdict.
    settling: Option<Window>,
    /// The most recent blocks that opened a window, oldest first, at most
    /// [`REMEMBERED`].
    opened_by: std::collections::VecDeque<String>,
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

    /// The first observation of pool block `hash`'s acceptance, at `now`,
    /// opens a window, or extends the one still admitting arrivals. A block
    /// that already opened one opens nothing.
    pub(super) fn opened(&self, now: Instant, hash: &str) {
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        let hash = hash.to_ascii_lowercase();
        if windows.opened_by.contains(&hash) {
            return;
        }
        if windows.opened_by.len() == REMEMBERED {
            windows.opened_by.pop_front();
        }
        windows.opened_by.push_back(hash);
        let until = now + WINDOW;
        match windows.open.as_mut() {
            Some(window) if now < window.admits_until => window.admits_until = until,
            _ => {
                // At most one window settles at a time: the older one takes
                // its verdict now, so verdicts stay in acceptance order.
                if let Some(settling) = windows.settling.take() {
                    windows.judge(settling);
                }
                windows.settling = windows.open.take();
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

    /// Count one admitted acknowledgement toward its window's verdict: the
    /// settling window's when it admitted the arrival, else the open one's.
    pub(super) fn record(&self, received_at: Instant, elapsed: Duration) {
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        let Windows { open, settling, .. } = &mut *windows;
        if let Some(window) = settling
            .as_mut()
            .filter(|window| received_at < window.admits_until)
            .or(open.as_mut())
            .filter(|window| received_at < window.admits_until)
        {
            window.acks += 1;
            for (over, bound) in window.over.iter_mut().zip(LandingAckBound::ALL) {
                *over += u64::from(elapsed > bound.threshold());
            }
        }
    }

    /// Per bound, the consecutive most recent windows whose p99 exceeded it,
    /// or `None` before the first verdict. Windows whose settle time has
    /// passed by `now` get their verdicts first, oldest first.
    pub(super) fn streaks(&self, now: Instant) -> Option<[u32; 2]> {
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        let settled = |window: &Option<Window>| {
            window
                .as_ref()
                .is_some_and(|window| now >= window.admits_until + SETTLE)
        };
        if settled(&windows.settling) {
            let window = windows.settling.take().expect("checked above");
            windows.judge(window);
        }
        if windows.settling.is_none() && settled(&windows.open) {
            let window = windows.open.take().expect("checked above");
            windows.judge(window);
        }
        windows.streaks
    }
}

impl Windows {
    fn judge(&mut self, window: Window) {
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
        state.opened(Instant::now(), "01");
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
        state.opened(Instant::now(), "0a");
        let arrived = Instant::now();
        // 100 acknowledgements, one over 2 s: p99 is the 99th, under 2 s.
        acks(&state, arrived, &[MS; 99]);
        acks(&state, arrived, &[Duration::from_secs(3)]);
        tokio::time::advance(WINDOW + SETTLE - MS).await;
        assert_eq!(state.streaks(Instant::now()), None, "verdict before settle");
        tokio::time::advance(MS).await;
        assert_eq!(state.streaks(Instant::now()), Some([0, 0]));

        // 100 more, two over 2 s and one over 10 s: p99 over 2 s, not 10 s.
        state.opened(Instant::now(), "0b");
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
        let mut height = 100;
        let mut window = |slow: Option<Duration>| {
            height += 1;
            state.opened(Instant::now(), &format!("{height:x}"));
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
    async fn an_acceptance_inside_a_window_extends_it_and_a_later_one_lets_it_settle() {
        let state = LandingAcks::default();
        let start = Instant::now();
        state.opened(start, "0d");
        acks(&state, start, &[Duration::from_secs(3)]);
        tokio::time::advance(Duration::from_secs(20)).await;
        state.opened(Instant::now(), "0e");
        tokio::time::advance(Duration::from_secs(25)).await;
        // 45 s after the first acceptance, 25 s after the second: admitted.
        acks(&state, Instant::now(), &[MS]);
        assert_eq!(
            state.streaks(Instant::now()),
            None,
            "one window, still open"
        );
        // Past the extended window's admissions, before its settle: a new
        // acceptance opens another window and leaves this one settling.
        tokio::time::advance(Duration::from_secs(10)).await;
        state.opened(Instant::now(), "0f");
        assert_eq!(
            state.streaks(Instant::now()),
            None,
            "the first window still settles"
        );
        // A submission in flight since 49 s, answered 8 s late, counts
        // toward the first window's verdict, not the second's.
        state.record(start + Duration::from_secs(49), Duration::from_secs(8));
        tokio::time::advance(Duration::from_secs(25)).await;
        assert_eq!(state.streaks(Instant::now()), Some([1, 0]));
        // The second window saw no acknowledgement: no verdict of its own.
        tokio::time::advance(WINDOW + SETTLE).await;
        assert_eq!(state.streaks(Instant::now()), Some([1, 0]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_block_opens_one_window_whichever_observation_comes_first() {
        let state = LandingAcks::default();
        state.opened(Instant::now(), "aa");
        tokio::time::advance(WINDOW - MS).await;
        // The reconciler's later observation of the same block, in either
        // case, neither extends nor reopens the window.
        state.opened(Instant::now(), "AA");
        tokio::time::advance(MS).await;
        assert!(!state.admits(Instant::now()));
        // Another block does, even at the same or a lower height, as a
        // reorg's replacement would.
        state.opened(Instant::now(), "bb");
        assert!(state.admits(Instant::now()));
    }

    /// Through the acceptance hooks: a reorg's replacement pool blocks at the
    /// same height and at a lower one each open a window, while the same
    /// block seen again, as its own offer and then by the reconciler at the
    /// tip, opens one.
    #[tokio::test(start_paused = true)]
    async fn reorg_replacements_open_their_own_windows_and_a_block_opens_one() {
        let metrics = crate::metrics::Metrics::default();
        let open = |metrics: &crate::metrics::Metrics| metrics.landing_acks.admits(Instant::now());
        let (a, b, c) = ("aa".repeat(32), "bb".repeat(32), "cc".repeat(32));
        metrics.accepted_unlanded_block(&a, 10);
        assert!(open(&metrics));
        tokio::time::advance(WINDOW).await;
        metrics.accepted_landed_block(&a, 10, true);
        assert!(!open(&metrics), "the same block opened a second window");
        metrics.accepted_block(&b, 10);
        assert!(
            open(&metrics),
            "a replacement at the same height opened none"
        );
        tokio::time::advance(WINDOW).await;
        metrics.accepted_block(&c, 9);
        assert!(
            open(&metrics),
            "a replacement at a lower height opened none"
        );
    }
}
