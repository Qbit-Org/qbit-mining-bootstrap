//! Observe critical futures without relying on a heartbeat on another worker.
//! Completed-poll maxima use 61 fixed one-second buckets per task kind. Each
//! sample is retained for at least 60 and at most 61 seconds; conservative
//! bucket expiry never drops a sample still within the last 60 seconds.
use super::{
    registry::{Family, Registry},
    TaskKind,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::watch;

const RETAIN_LAG: Duration = Duration::from_secs(60);
const POLL_BUCKET_COUNT: usize = RETAIN_LAG.as_secs() as usize + 1;
pub const DEFAULT_POLL_BUDGET: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
struct PollBucket {
    second: u64,
    maximum: Duration,
}
struct RecentPolls {
    origin: Instant,
    buckets: [Option<PollBucket>; POLL_BUCKET_COUNT],
}
impl RecentPolls {
    fn new(origin: Instant) -> Self {
        Self {
            origin,
            buckets: [None; POLL_BUCKET_COUNT],
        }
    }
    fn observe(&mut self, now: Instant, elapsed: Duration) {
        let second = now.saturating_duration_since(self.origin).as_secs();
        let slot = &mut self.buckets[(second % POLL_BUCKET_COUNT as u64) as usize];
        if let Some(bucket) = slot.as_mut().filter(|bucket| bucket.second == second) {
            bucket.maximum = bucket.maximum.max(elapsed);
        } else {
            *slot = Some(PollBucket {
                second,
                maximum: elapsed,
            });
        }
    }
    fn maximum(&self, now: Instant) -> Duration {
        let second = now.saturating_duration_since(self.origin).as_secs();
        self.buckets
            .iter()
            .flatten()
            .filter(|bucket| {
                bucket.second <= second && second - bucket.second <= RETAIN_LAG.as_secs()
            })
            .map(|bucket| bucket.maximum)
            .max()
            .unwrap_or_default()
    }
}

struct Entry {
    task: TaskKind,
    poll_started: Option<Instant>,
    progress: Option<(Instant, Duration)>,
}
#[derive(Default)]
struct State {
    next_id: u64,
    entries: BTreeMap<u64, Entry>,
    recent: BTreeMap<TaskKind, RecentPolls>,
    wake_lag: Duration,
}
impl State {
    fn record_completed_poll(&mut self, task: TaskKind, now: Instant, elapsed: Duration) {
        self.recent
            .entry(task)
            .or_insert_with(|| RecentPolls::new(now))
            .observe(now, elapsed);
    }
}
pub struct RuntimeMonitor {
    state: Mutex<State>,
    poll_budget: Duration,
}
impl Default for RuntimeMonitor {
    fn default() -> Self {
        Self::new(DEFAULT_POLL_BUDGET)
    }
}
impl RuntimeMonitor {
    pub fn new(poll_budget: Duration) -> Self {
        assert!(
            !poll_budget.is_zero(),
            "runtime poll budget must be positive"
        );
        Self {
            state: Mutex::new(State::default()),
            poll_budget,
        }
    }
    fn register(
        self: &Arc<Self>,
        task: TaskKind,
        progress: Option<(Instant, Duration)>,
    ) -> Registration {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .expect("runtime registration sequence exhausted");
        state.entries.insert(
            id,
            Entry {
                task,
                poll_started: None,
                progress,
            },
        );
        Registration {
            monitor: self.clone(),
            id,
        }
    }
    /// A poll that never returns is visible from a surviving runtime worker.
    /// Registration follows the future's lifetime, including cancellation.
    pub fn track<F: Future>(self: &Arc<Self>, task: TaskKind, future: F) -> Monitored<F> {
        Monitored {
            future: Box::pin(future),
            registration: self.register(task, None),
        }
    }
    /// Only start this guard when required work exists. Long polls/idle loops
    /// should not manufacture a progress deadline. Dropping the guard ends it.
    pub fn start_operation(self: &Arc<Self>, task: TaskKind, budget: Duration) -> ProgressGuard {
        assert!(
            !budget.is_zero(),
            "operation progress budget must be positive"
        );
        ProgressGuard(self.register(task, Some((Instant::now(), budget))))
    }
    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.snapshot_at(Instant::now())
    }
    fn snapshot_at(&self, now: Instant) -> RuntimeSnapshot {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut tasks: BTreeMap<_, _> = TaskKind::ALL
            .iter()
            .map(|kind| (*kind, TaskSnapshot::default()))
            .collect();
        for (kind, recent) in &state.recent {
            tasks.get_mut(kind).unwrap().poll_lag = recent.maximum(now).as_secs_f64();
        }
        for entry in state.entries.values() {
            let task = tasks.get_mut(&entry.task).unwrap();
            if let Some(at) = entry.poll_started {
                let elapsed = now.saturating_duration_since(at);
                task.poll_lag = task.poll_lag.max(elapsed.as_secs_f64());
                task.stalled |= elapsed > self.poll_budget;
            }
            if let Some((at, budget)) = entry.progress {
                let elapsed = now.saturating_duration_since(at);
                task.progress_age = task.progress_age.max(elapsed.as_secs_f64());
                task.stalled |= elapsed > budget;
            }
        }
        RuntimeSnapshot {
            tasks,
            wake_lag: state.wake_lag.as_secs_f64(),
        }
    }
    /// This measures scheduling delay; critical-future poll observation above
    /// remains necessary when only one of several workers is blocked.
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                _ = shutdown.changed() => break,
                scheduled = interval.tick() => {
                    self.state.lock().unwrap_or_else(|e| e.into_inner()).wake_lag =
                        Instant::now().saturating_duration_since(scheduled.into_std());
                }
            }
        }
        Ok(())
    }
}

struct Registration {
    monitor: Arc<RuntimeMonitor>,
    id: u64,
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.monitor
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .remove(&self.id);
    }
}
pub struct Monitored<F> {
    future: Pin<Box<F>>,
    registration: Registration,
}
impl<F: Future> Future for Monitored<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let started = Instant::now();
        {
            let mut state = this
                .registration
                .monitor
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            state
                .entries
                .get_mut(&this.registration.id)
                .unwrap()
                .poll_started = Some(started);
        }
        // No monitor lock is held while application code runs or unwinds.
        let result = this.future.as_mut().poll(cx);
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(started);
        let mut state = this
            .registration
            .monitor
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let entry = state.entries.get_mut(&this.registration.id).unwrap();
        entry.poll_started = None;
        let task = entry.task;
        state.record_completed_poll(task, now, elapsed);
        result
    }
}

pub struct ProgressGuard(Registration);
impl ProgressGuard {
    /// Call only after this operation makes real progress; a different task's
    /// timer cannot refresh this registration.
    pub fn progress(&self) {
        let mut state = self
            .0
            .monitor
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some((at, _)) = &mut state.entries.get_mut(&self.0.id).unwrap().progress {
            *at = Instant::now();
        }
    }
}

#[derive(Clone, Default)]
struct TaskSnapshot {
    poll_lag: f64,
    progress_age: f64,
    stalled: bool,
}
#[derive(Clone)]
pub struct RuntimeSnapshot {
    tasks: BTreeMap<TaskKind, TaskSnapshot>,
    wake_lag: f64,
}
impl RuntimeSnapshot {
    pub fn stalled(&self) -> bool {
        self.tasks.values().any(|task| task.stalled)
    }
    pub fn apply_health(&self, payload: &mut Value) {
        if self.stalled() {
            payload["ok"] = json!(false);
            payload["ready"] = json!(false);
            payload["status"] = json!("runtime-stalled");
            payload["error"] = json!("a critical runtime task is not making progress");
        }
    }
    pub fn render(&self) -> String {
        let mut registry = Registry::default();
        registry.set(Family::RuntimeLag, vec![], self.wake_lag);
        for (kind, task) in &self.tasks {
            let labels = vec![("task", kind.as_str().into())];
            registry.set(Family::PollLag, labels.clone(), task.poll_lag);
            registry.set(Family::ProgressAge, labels.clone(), task.progress_age);
            registry.set(Family::TaskStalled, labels, u8::from(task.stalled).into());
        }
        registry.render()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completed_poll_survives_expiry_of_a_larger_predecessor() {
        let monitor = RuntimeMonitor::default();
        let start = Instant::now();
        {
            let mut state = monitor.state.lock().unwrap();
            state.record_completed_poll(TaskKind::Refresh, start, Duration::from_secs(3));
            state.record_completed_poll(
                TaskKind::Refresh,
                start + Duration::from_secs(50),
                Duration::from_millis(2500),
            );
        }
        let snapshot = monitor.snapshot_at(start + Duration::from_secs(61));
        assert_eq!(snapshot.tasks[&TaskKind::Refresh].poll_lag, 2.5);
        assert!(
            !snapshot.stalled(),
            "completed polls do not create active stalls"
        );
    }

    #[test]
    fn completed_poll_buckets_expire_with_at_most_one_extra_second() {
        let start = Instant::now();
        let mut recent = RecentPolls::new(start);
        recent.observe(start, Duration::from_secs(3));
        recent.observe(start + Duration::from_millis(900), Duration::from_secs(2));
        assert_eq!(recent.maximum(start + RETAIN_LAG), Duration::from_secs(3));
        assert_eq!(
            recent.maximum(start + RETAIN_LAG + Duration::from_millis(999)),
            Duration::from_secs(3),
            "one-second buckets conservatively retain their maximum"
        );
        assert_eq!(
            recent.maximum(start + RETAIN_LAG + Duration::from_secs(1)),
            Duration::ZERO
        );
    }

    #[test]
    fn high_poll_rate_uses_fixed_storage_and_preserves_other_task_maxima() {
        let start = Instant::now();
        let monitor = RuntimeMonitor::default();
        {
            let mut state = monitor.state.lock().unwrap();
            state.record_completed_poll(TaskKind::Submit, start, Duration::from_secs(4));
            for micros in 0..100_000 {
                state.record_completed_poll(
                    TaskKind::Refresh,
                    start + Duration::from_micros(micros),
                    Duration::from_millis(5),
                );
            }
            let recent = &state.recent[&TaskKind::Refresh];
            assert_eq!(recent.buckets.len(), 61);
            assert_eq!(recent.buckets.iter().flatten().count(), 1);
        }
        let snapshot = monitor.snapshot_at(start + Duration::from_secs(1));
        assert_eq!(snapshot.tasks[&TaskKind::Refresh].poll_lag, 0.005);
        assert_eq!(snapshot.tasks[&TaskKind::Submit].poll_lag, 4.);
        assert!(!snapshot.stalled());
        {
            let mut state = monitor.state.lock().unwrap();
            for second in 1..=300 {
                state.record_completed_poll(
                    TaskKind::Refresh,
                    start + Duration::from_secs(second),
                    Duration::from_millis(second),
                );
            }
            let recent = &state.recent[&TaskKind::Refresh];
            assert_eq!(recent.buckets.iter().flatten().count(), 61);
            assert_eq!(
                recent.maximum(start + Duration::from_secs(300)),
                Duration::from_millis(300)
            );
            assert_eq!(
                recent.maximum(start + Duration::from_secs(361)),
                Duration::ZERO
            );
        }
    }

    #[test]
    fn progress_is_monotonic_per_operation_and_idle_is_not_stalled() {
        let monitor = Arc::new(RuntimeMonitor::default());
        assert!(!monitor.snapshot().stalled());
        let guard = monitor.start_operation(TaskKind::Refresh, Duration::from_secs(2));
        let future = Instant::now() + Duration::from_secs(3);
        assert!(monitor.snapshot_at(future).stalled());
        let other = monitor.start_operation(TaskKind::Rollup, Duration::from_secs(10));
        other.progress();
        assert!(monitor.snapshot_at(future).stalled());
        guard.progress();
        assert!(!monitor.snapshot().stalled());
        drop(guard);
        drop(other);
        assert!(!monitor
            .snapshot_at(future + Duration::from_secs(100))
            .stalled());
    }
}
