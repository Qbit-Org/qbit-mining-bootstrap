//! The difficulty-hint writer (#856): vardiff resume hints leave the session
//! that made them and are written beside the listener, one at a time.
//!
//! A session used to await each hint (up to three ledger statements under a
//! 500 ms bound) before it read its next request, so a slow write held up the
//! miner's next share. A session now only offers the hint to its listener's
//! queue, which never waits. The queue holds one entry per worker, and a
//! newer hint for a worker already queued is combined with its entry (see
//! [`Pending`]); a hint for another worker when the queue is full is dropped
//! and counted. A decrease that has waited longer than [`MAX_DECREASE_WAIT`]
//! is not written. The writer is its listener's own task (or, when a
//! connection is served on its own, that connection's): it stops when they
//! do, letting a listener's write in flight finish within its bound rather
//! than cancelling it mid-statement, and whatever it still holds then is
//! dropped and counted.
use super::{retain_difficulty, MiningBackend, StratumConfig, Worker};
use crate::metrics::{DifficultyHintOutcome as Outcome, Metrics, TaskKind};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::Notify, task::JoinHandle, time::timeout};

/// Workers one listener's writer holds hints for unless the configuration
/// says otherwise. About 2,000 sessions offer one each per 30 s at a steady
/// difficulty, so this absorbs every session of a reconnect storm at once.
pub(super) const DEFAULT_QUEUE_CAPACITY: usize = 4096;

/// One write's bound, its read-back included, as when sessions awaited it.
pub(super) const DEFAULT_WRITE_TIMEOUT_SECONDS: f64 = 0.5;

/// The longest a decrease may wait in the queue and still be written,
/// checked as its write is about to run. A decrease carries no share, and
/// the ledger lowers any higher row whatever evidence it holds, so one
/// written late can pull down evidence that another frontend recorded
/// meanwhile: a worker reconnects through the balancer, proves a higher
/// difficulty there, and then resumes at this frontend's stale, lower one.
/// When sessions awaited their hints, a decrease reached the ledger within
/// its 500 ms write bound (the default `difficulty_hint_timeout_seconds`) of
/// the retarget. Waiting as long again keeps that within about a second: a
/// decrease is written if its write starts within 500 ms of its offer. A
/// healthy queue waits milliseconds, and one offered during a single write
/// waits at most the rest of that write's bound, even if it times out.
/// Behind other entries, or behind earlier hints in its own entry, it can
/// wait longer and age out; the row then keeps its older, higher difficulty
/// until a share at the lowered one records evidence, and a miner that
/// reconnects meanwhile resumes from it. Evidence never ages: the ledger
/// keeps the row with the newer evidence, so late evidence cannot overwrite
/// newer.
pub(super) const MAX_DECREASE_WAIT: Duration = Duration::from_millis(500);

/// Whether a decrease offered at `offered` has waited longer than
/// [`MAX_DECREASE_WAIT`] by `now`, both on the writer's clock.
fn waited_too_long(offered: tokio::time::Instant, now: tokio::time::Instant) -> bool {
    now.saturating_duration_since(offered) > MAX_DECREASE_WAIT
}

/// Accepted-share evidence: the difficulty the share proved, the share, and
/// when this frontend accepted it.
struct Evidence {
    difficulty: f64,
    share_id: String,
    accepted: Instant,
}

/// An unproven decrease: the difficulty to lower to, and when a session
/// offered it, on the writer's clock.
struct Decrease {
    difficulty: f64,
    offered: tokio::time::Instant,
}

/// One hint as a session offered it.
enum Hint {
    Evidence(Evidence),
    Decrease(Decrease),
}

impl Hint {
    /// Whether the ledger would take its difficulty: positive and finite, as
    /// its `valid_difficulty` and the fuzz backend's check require.
    fn takes_difficulty(&self) -> bool {
        let difficulty = match self {
            Hint::Evidence(evidence) => evidence.difficulty,
            Hint::Decrease(decrease) => decrease.difficulty,
        };
        difficulty.is_finite() && difficulty > 0.0
    }

    /// Whether the ledger would take it for `worker`: a difficulty it takes
    /// and, for evidence, a share that is the worker's own
    /// (`<username>:<block hash>`), as the coordinator requires before it
    /// looks the share up.
    fn valid(&self, worker: &Worker) -> bool {
        let own = match self {
            Hint::Evidence(evidence) => evidence
                .share_id
                .strip_prefix(worker.username.as_str())
                .is_some_and(|rest| rest.starts_with(':')),
            Hint::Decrease(_) => true,
        };
        own && self.takes_difficulty()
    }

    /// Its write, which never ages: a refused hint changes no row however
    /// late it is written (see [`Pending::refused`]).
    fn refused_step(self) -> Step {
        let (difficulty, share_id) = match self {
            Hint::Evidence(evidence) => (evidence.difficulty, Some(evidence.share_id)),
            Hint::Decrease(decrease) => (decrease.difficulty, None),
        };
        Step {
            difficulty,
            share_id,
            ages_from: None,
        }
    }
}

impl Evidence {
    fn step(self) -> Step {
        Step {
            difficulty: self.difficulty,
            share_id: Some(self.share_id),
            ages_from: None,
        }
    }
}

impl Decrease {
    fn step(self) -> Step {
        Step {
            difficulty: self.difficulty,
            share_id: None,
            ages_from: Some(self.offered),
        }
    }
}

/// One write: a record of evidence (with its share) or a decrease (without).
struct Step {
    difficulty: f64,
    share_id: Option<String>,
    /// When the decrease was offered: it is not written once it has waited
    /// longer than [`MAX_DECREASE_WAIT`]. `None` for evidence, which never
    /// ages, and for a refused hint (see [`Pending::refused`]).
    ages_from: Option<tokio::time::Instant>,
}

/// What combining a hint into an entry left never to be written.
#[derive(Default)]
struct Unwritten {
    /// Hints combined into another.
    superseded: u64,
    /// Decreases that had already waited too long.
    aged_out: u64,
}

/// Fold a newer decrease into the one held on its side of the evidence.
/// Lowering twice lowers to the lower of the two, so the lower is kept, with
/// the time it was itself offered (the newer one's on a tie), and ages as it
/// would have alone. A held decrease that has already waited too long would
/// never be written, so the newer one replaces it whatever its value, as
/// their separate writes would have left the row.
fn fold(
    held: &mut Option<Decrease>,
    newer: Decrease,
    now: tokio::time::Instant,
    unwritten: &mut Unwritten,
) {
    match held {
        Some(old) if waited_too_long(old.offered, now) => {
            unwritten.aged_out += 1;
            *old = newer;
        }
        Some(old) => {
            unwritten.superseded += 1;
            if newer.difficulty <= old.difficulty {
                *old = newer;
            }
        }
        None => *held = Some(newer),
    }
}

/// What the writer will do for one worker, in order: what the hints it was
/// offered would have done as separate writes, published against the worker
/// and the shares they were made for, never against whatever the sessions
/// that made them hold by the time it is written.
///
/// Decreases fold into the lowest (see [`fold`]). Of two pieces of evidence
/// only the one this frontend accepted later is kept: the ledger keeps the
/// row with the newer evidence, so the older record would change nothing
/// after it, and before it only what the newer one overwrites. Several
/// sessions may share a username, so that is not always the one offered
/// last.
///
/// Against separate writes under the age rule, one fold differs: a held
/// decrease, still within its bound when a higher one is folded into it,
/// that passes the bound before the write. Neither is then written, although
/// the higher one alone would have been: the fold errs toward leaving the
/// row as it is, never toward a stale decrease.
struct Pending {
    worker: Worker,
    /// One hint offered for the worker that the ledger refuses (see
    /// [`Hint::valid`]), kept apart from the others. One kind is a
    /// difficulty that is not positive and finite: folded in, it would
    /// vanish (`f64::min` keeps the other operand of a NaN, and the finite
    /// one beside an infinite decrease; newer evidence replaces it), and a
    /// backend that checks every difficulty it is handed, as the fuzz
    /// targets' does, would never see it. The other is evidence whose share
    /// is another worker's, which a session offers at a retarget after it
    /// re-authorized under this username and then had a share accepted on a
    /// job of its earlier one: as the later-accepted evidence it would
    /// replace evidence the ledger would have taken, and then be refused
    /// itself, losing both. The first refused hint is the one held, except
    /// that a refused difficulty takes the place of another worker's share
    /// held at a difficulty the ledger takes, so that backend still sees it;
    /// any other one for the worker while one is held is superseded. It is
    /// written last, so it never delays the entry's other writes, whatever
    /// its age, and changes no row: the ledger refuses it (`failed`), or,
    /// for evidence whose share it cannot find, records nothing.
    refused: Option<Hint>,
    /// The lowest decrease offered before `evidence` (or with none).
    lower_before: Option<Decrease>,
    evidence: Option<Evidence>,
    /// The lowest decrease offered after `evidence`.
    lower_after: Option<Decrease>,
}

impl Pending {
    fn new(worker: &Worker, hint: Hint, now: tokio::time::Instant) -> Self {
        let mut pending = Self {
            worker: worker.clone(),
            refused: None,
            lower_before: None,
            evidence: None,
            lower_after: None,
        };
        // Nothing is held yet, so nothing is left unwritten.
        pending.absorb(worker, hint, now);
        pending
    }

    /// Combine a newer hint, offered at `now`, into this entry.
    fn absorb(&mut self, worker: &Worker, hint: Hint, now: tokio::time::Instant) -> Unwritten {
        let mut unwritten = Unwritten::default();
        if !hint.valid(worker) {
            match &self.refused {
                // A difficulty the ledger refuses is what a backend that
                // checks every difficulty must see, so it takes the place of
                // another worker's share held at a difficulty the ledger
                // takes.
                Some(held) if held.takes_difficulty() && !hint.takes_difficulty() => {
                    unwritten.superseded += 1;
                    self.refused = Some(hint);
                }
                Some(_) => unwritten.superseded += 1,
                None => self.refused = Some(hint),
            }
            return unwritten;
        }
        match hint {
            Hint::Evidence(newer) => match &self.evidence {
                Some(kept) if kept.accepted > newer.accepted => unwritten.superseded += 1,
                kept => {
                    unwritten.superseded += u64::from(kept.is_some());
                    if let Some(after) = self.lower_after.take() {
                        fold(&mut self.lower_before, after, now, &mut unwritten);
                    }
                    self.worker = worker.clone();
                    self.evidence = Some(newer);
                }
            },
            Hint::Decrease(decrease) => {
                let side = match self.evidence {
                    Some(_) => &mut self.lower_after,
                    None => &mut self.lower_before,
                };
                fold(side, decrease, now, &mut unwritten);
            }
        }
        unwritten
    }

    /// The writes, in order.
    fn steps(self) -> (Worker, Vec<Step>) {
        let steps = [
            self.lower_before.map(Decrease::step),
            self.evidence.map(Evidence::step),
            self.lower_after.map(Decrease::step),
            self.refused.map(Hint::refused_step),
        ];
        (self.worker, steps.into_iter().flatten().collect())
    }
}

#[derive(Default)]
struct Queue {
    /// Usernames in the order their entry was first queued.
    order: VecDeque<String>,
    pending: HashMap<String, Pending>,
    /// Set once nothing will write later offers, to what they count as:
    /// `abandoned` once the listener or connection has stopped,
    /// `writer_panicked` once the writer has. The first reason stays.
    closed: Option<Outcome>,
    /// Set once a listener's work has ended: the writer lets the write in
    /// flight finish and starts no other.
    stopping: bool,
}

struct Shared {
    queue: Mutex<Queue>,
    queued: Notify,
}

/// How a writer ends once the work it runs beside has ended.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum WriterEnd {
    /// A listener's: the write in flight finishes within its own bound, as
    /// when a session awaited it, instead of being cancelled mid-statement;
    /// what is still queued, and the rest of that write's entry, is dropped
    /// and counted as abandoned. A writer that panics only closes the queue
    /// (what it held and every later offer count as `writer_panicked`).
    Abandon,
    /// A connection served on its own (the fuzz targets, the property
    /// tests): what it queued is written before it returns, as when its
    /// session awaited each hint (bar a decrease past its wait bound, aged
    /// out as anywhere), and a panic in the writer is its own.
    Drain,
}

/// One listener's queue of difficulty hints, shared by its sessions and its
/// writer. The listener is the writer's own, so a worker's username keys it.
#[derive(Clone)]
pub(super) struct DifficultyHints {
    shared: Arc<Shared>,
}

impl DifficultyHints {
    pub(super) fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                queue: Mutex::new(Queue::default()),
                queued: Notify::new(),
            }),
        }
    }

    /// A queue nothing will ever write, for a session a test builds on its
    /// own: every hint it is offered counts as abandoned at once, rather than
    /// waiting forever uncounted.
    #[cfg(test)]
    pub(super) fn closed() -> Self {
        let hints = Self::new();
        hints.queue().closed = Some(Outcome::Abandoned);
        hints
    }

    fn queue(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.shared
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Queue a hint for the writer and return at once. `evidence` is the
    /// accepted share and when this frontend accepted it. The checks that
    /// made a hint a no-op before #856 still make it one, before anything is
    /// queued or counted.
    pub(super) fn offer(
        &self,
        config: &StratumConfig,
        metrics: &Metrics,
        worker: &Worker,
        difficulty: f64,
        evidence: Option<(&str, Instant)>,
        downward_only: bool,
    ) {
        if !config.resume_enabled
            || config.resume_ttl_seconds == 0
            || config.resume_max_entries == 0
        {
            return;
        }
        // The writer ages decreases on the clock that timed them.
        let now = tokio::time::Instant::now();
        let hint = match (downward_only, evidence) {
            (true, _) => Hint::Decrease(Decrease {
                difficulty,
                offered: now,
            }),
            (false, Some((share_id, accepted))) => Hint::Evidence(Evidence {
                difficulty,
                share_id: share_id.to_owned(),
                accepted,
            }),
            (false, None) => return,
        };
        let (refused, unwritten) = {
            let mut queue = self.queue();
            if let Some(outcome) = queue.closed {
                (Some(outcome), Unwritten::default())
            } else if let Some(queued) = queue.pending.get_mut(&worker.username) {
                // The entry keeps its place, so a busy worker cannot starve
                // the others.
                (None, queued.absorb(worker, hint, now))
            } else if queue.pending.len() >= config.difficulty_hint_queue_capacity {
                (Some(Outcome::Dropped), Unwritten::default())
            } else {
                queue.order.push_back(worker.username.clone());
                queue
                    .pending
                    .insert(worker.username.clone(), Pending::new(worker, hint, now));
                drop(queue);
                self.shared.queued.notify_one();
                return;
            }
        };
        if let Some(outcome) = refused {
            metrics.record_difficulty_hints(outcome, 1);
        }
        metrics.record_difficulty_hints(Outcome::Superseded, unwritten.superseded);
        metrics.record_difficulty_hints(Outcome::AgedOut, unwritten.aged_out);
    }

    /// A listener's work has ended: drop what is queued, counted as
    /// abandoned, refuse later offers, and have the writer stop once the
    /// write in flight, if any, has finished.
    fn stop_after_write(&self, metrics: &Metrics) {
        self.queue().stopping = true;
        self.close(metrics, Outcome::Abandoned);
        self.shared.queued.notify_one();
    }

    /// Refuse every later offer, counting it as `outcome` unless the queue
    /// had closed already, and drop what is queued, counting it as `outcome`.
    fn close(&self, metrics: &Metrics, outcome: Outcome) {
        let dropped: u64 = {
            let mut queue = self.queue();
            queue.closed.get_or_insert(outcome);
            queue.order.clear();
            std::mem::take(&mut queue.pending)
                .into_values()
                .map(|pending| pending.steps().1.len() as u64)
                .sum()
        };
        metrics.record_difficulty_hints(outcome, dropped);
    }

    /// The writer task ended with `error` before its queue was closed and
    /// empty: nothing writes this queue any more, so it closes. A panic
    /// counts what was left, and every later offer, as `writer_panicked`, so
    /// an operator can tell a dead writer from a stopping listener; it is
    /// logged as an error, or, for a connection served on its own, is that
    /// connection's panic. A writer cancelled while its queue is open (only a
    /// runtime shutting down does that) leaves its hints abandoned.
    fn writer_ended(
        &self,
        metrics: &Metrics,
        listener: &str,
        end: WriterEnd,
        error: tokio::task::JoinError,
    ) {
        if !error.is_panic() {
            self.close(metrics, Outcome::Abandoned);
            return;
        }
        self.close(metrics, Outcome::WriterPanicked);
        if end == WriterEnd::Drain {
            std::panic::resume_unwind(error.into_panic());
        }
        tracing::error!(listener, error = %error, "Stratum difficulty hint writer panicked; hints are no longer written");
    }

    /// Run `work` with this queue's writer running beside it as a task of
    /// its own; later offers are refused once `work` ends. If this future is
    /// dropped first, a write still in flight is cancelled and whatever is
    /// still queued is dropped, both counted as abandoned (besides a write's
    /// own bound, the only way a write is cancelled mid-statement). Should
    /// the writer panic, see
    /// [`Self::writer_ended`]. When `work` ends,
    /// `end` says what becomes of what is left, and the writer task has
    /// finished before this returns.
    pub(super) async fn beside<B: MiningBackend, T>(
        &self,
        backend: Arc<B>,
        config: StratumConfig,
        metrics: Arc<Metrics>,
        end: WriterEnd,
        work: impl Future<Output = T>,
    ) -> T {
        let listener = config.listener_name.clone();
        let writer = {
            let (hints, metrics) = (self.clone(), metrics.clone());
            let runtime = metrics.runtime();
            tokio::spawn(runtime.track(TaskKind::StratumListener, async move {
                hints.write(backend.as_ref(), &config, &metrics).await
            }))
        };
        let mut stop = Stop {
            hints: self,
            metrics: &metrics,
            writer: Some(writer),
        };
        tokio::pin!(work);
        let output = tokio::select! {
            biased;
            output = &mut work => output,
            ended = stop.writer.as_mut().expect("the writer runs until stopped") => {
                stop.writer = None;
                // The queue is open, so the writer cannot have returned.
                if let Err(error) = ended {
                    self.writer_ended(&metrics, &listener, end, error);
                }
                work.await
            }
        };
        // The guard keeps the handle until the writer has finished: if this
        // future is dropped while it waits, as a cancelled connection is
        // mid-drain, the guard aborts the writer instead of detaching it.
        if let Some(writer) = stop.writer.as_mut() {
            match end {
                // EP-ERRORS: a write cancelled mid-statement leaves it running
                // on the server after its connection is gone; the one in
                // flight finishes within its bound instead.
                // A writer that has already ended (a panic the work's end
                // raced) is left to `writer_ended` below to count.
                WriterEnd::Abandon if !writer.is_finished() => self.stop_after_write(&metrics),
                WriterEnd::Abandon => {}
                WriterEnd::Drain => {
                    self.queue().closed.get_or_insert(Outcome::Abandoned);
                    self.shared.queued.notify_one();
                }
            }
            let joined = writer.await;
            stop.writer = None;
            // Cancelled only by a runtime shutting down, which leaves what it
            // held abandoned; a panic is still its own.
            if let Err(error) = joined {
                if error.is_panic() {
                    self.writer_ended(&metrics, &listener, end, error);
                }
            }
        }
        output
    }

    /// The oldest queued entry, waiting for one; `None` once the queue is
    /// closed and empty.
    async fn next(&self) -> Option<Pending> {
        loop {
            {
                let mut queue = self.queue();
                while let Some(username) = queue.order.pop_front() {
                    if let Some(pending) = queue.pending.remove(&username) {
                        return Some(pending);
                    }
                }
                if queue.closed.is_some() {
                    return None;
                }
            }
            // A single writer: an offer made since the check above left a
            // permit, so this cannot miss it.
            self.shared.queued.notified().await;
        }
    }

    /// Write queued entries one at a time, oldest first, until the queue is
    /// closed and empty, or the writer is cancelled. Writes never overlap, so
    /// an older write's read-back always lands in the retained-difficulty
    /// cache before a newer one's.
    async fn write<B: MiningBackend>(
        &self,
        backend: &B,
        config: &StratumConfig,
        metrics: &Metrics,
    ) {
        let bound = Duration::try_from_secs_f64(config.difficulty_hint_timeout_seconds)
            .ok()
            .filter(|bound| !bound.is_zero())
            .unwrap_or(Duration::from_secs_f64(DEFAULT_WRITE_TIMEOUT_SECONDS));
        while let Some(pending) = self.next().await {
            let (worker, steps) = pending.steps();
            let mut in_flight = InFlight {
                metrics,
                unsettled: steps.len() as u64,
            };
            // Each step is one hint's write, exactly as a session awaited it
            // before #856: its own bound, its own read-back, and the cache
            // updated from that read-back.
            for step in steps {
                // A stopping listener's writer starts no further write; the
                // rest of the entry is abandoned. What the steps already
                // written leave is what separate writes would have left had
                // the listener stopped between those hints.
                if self.queue().stopping {
                    break;
                }
                // A decrease is aged as its write is about to run, on the
                // clock that timed its offer, and an aged one touches neither
                // the ledger nor the cache.
                if step
                    .ages_from
                    .is_some_and(|offered| waited_too_long(offered, tokio::time::Instant::now()))
                {
                    in_flight.settle(Outcome::AgedOut);
                    continue;
                }
                let Step {
                    difficulty,
                    share_id,
                    ..
                } = step;
                let downward_only = share_id.is_none();
                let persisted = timeout(bound, async {
                    backend
                        .remember_worker_difficulty(
                            &config.listener_name,
                            &worker,
                            difficulty,
                            share_id.as_deref(),
                            downward_only,
                        )
                        .await?;
                    // A different frontend may already have newer accepted
                    // evidence. Cache the canonical retained value and its
                    // database age, rather than treating this optional write
                    // as new evidence on this host.
                    backend
                        .worker_difficulty(
                            &config.listener_name,
                            &worker,
                            config.resume_ttl_seconds,
                        )
                        .await
                })
                .await;
                in_flight.settle(match persisted {
                    Ok(Ok(Some((difficulty, age)))) => {
                        retain_difficulty(config, &worker, difficulty, age, !downward_only);
                        Outcome::Written
                    }
                    Ok(Ok(None)) => {
                        config
                            .retained_difficulties
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .remove(&(config.listener_name.clone(), worker.username.clone()));
                        Outcome::Written
                    }
                    Ok(Err(error)) => {
                        tracing::debug!(listener = %config.listener_name, error = %format_args!("{error:#}"), "Stratum difficulty hint not written");
                        Outcome::Failed
                    }
                    Err(_) => Outcome::TimedOut,
                });
            }
        }
    }
}

/// Stops a queue's writer and closes the queue, however `beside` ends.
struct Stop<'a> {
    hints: &'a DifficultyHints,
    metrics: &'a Metrics,
    writer: Option<JoinHandle<()>>,
}

impl Drop for Stop<'_> {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            writer.abort();
        }
        self.hints.close(self.metrics, Outcome::Abandoned);
    }
}

/// The hints of an entry being written: those not yet settled are counted
/// as abandoned if the writer is cancelled first, or as `writer_panicked` if
/// it panics.
struct InFlight<'a> {
    metrics: &'a Metrics,
    unsettled: u64,
}

impl InFlight<'_> {
    fn settle(&mut self, outcome: Outcome) {
        self.unsettled -= 1;
        self.metrics.record_difficulty_hints(outcome, 1);
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        // Dropped while unwinding: the writer panicked mid-entry. (A
        // current-thread runtime dropped by a panicking thread, a test's
        // failure, would count its cancelled writer here too.)
        let outcome = if std::thread::panicking() {
            Outcome::WriterPanicked
        } else {
            Outcome::Abandoned
        };
        self.metrics
            .record_difficulty_hints(outcome, self.unsettled);
    }
}
