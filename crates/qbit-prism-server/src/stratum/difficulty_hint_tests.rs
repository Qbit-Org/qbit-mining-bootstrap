//! The difficulty-hint writer (#856): bounded, one entry per worker, never
//! awaited by a session, and gone with its listener.
use super::*;
use crate::metrics::{DifficultyHintOutcome, Metrics};
use difficulty_hints::MAX_DECREASE_WAIT;
use tokio::sync::Notify;

/// Held once armed: `entered` fires when a write reaches it, and the write
/// waits for `release`.
#[derive(Default)]
struct Gate {
    entered: Notify,
    release: Notify,
}

/// One remembered hint, as the backend received it.
type Written = (String, f64, Option<String>, bool);

/// Stores hints like the ledger's worker-difficulty rows, minus the evidence
/// clock (a record sets the row, a lowering lowers an existing row), refuses
/// the difficulties the ledger refuses and evidence that is another worker's
/// share, as the coordinator does, and holds or fails writes on demand.
#[derive(Default)]
struct HintStore {
    /// Every difficulty it was handed, by worker, as the fuzz targets'
    /// backend checks them: refused ones too.
    seen: Mutex<Vec<(String, f64)>>,
    gates: Mutex<VecDeque<Arc<Gate>>>,
    fail: std::sync::atomic::AtomicBool,
    /// Fail only records of evidence, never decreases.
    fail_records: std::sync::atomic::AtomicBool,
    /// Panic in the next write, as a bug in the writer's path would.
    panic: std::sync::atomic::AtomicBool,
    written: Mutex<Vec<Written>>,
    rows: Mutex<HashMap<String, f64>>,
}

impl HintStore {
    fn hold_next(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        self.gates.lock().unwrap().push_back(gate.clone());
        gate
    }
    fn written(&self) -> Vec<Written> {
        self.written.lock().unwrap().clone()
    }
}

impl MiningBackend for HintStore {
    type Context = ();
    async fn worker_difficulty(
        &self,
        _listener: &str,
        worker: &Worker,
        _ttl_seconds: u64,
    ) -> Result<Option<(f64, Duration)>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(&worker.username)
            .map(|difficulty| (*difficulty, Duration::ZERO)))
    }
    async fn remember_worker_difficulty(
        &self,
        _listener: &str,
        worker: &Worker,
        difficulty: f64,
        share_id: Option<&str>,
        downward_only: bool,
    ) -> Result<()> {
        assert!(
            !self.panic.swap(false, Ordering::SeqCst),
            "a bug in the hint path"
        );
        self.seen
            .lock()
            .unwrap()
            .push((worker.username.clone(), difficulty));
        let gate = self.gates.lock().unwrap().pop_front();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        anyhow::ensure!(
            !self.fail.load(Ordering::SeqCst)
                && (downward_only || !self.fail_records.load(Ordering::SeqCst)),
            "difficulty evidence belongs to another worker"
        );
        anyhow::ensure!(
            share_id.is_none_or(|share| share.starts_with(&format!("{}:", worker.username))),
            "difficulty evidence belongs to another worker"
        );
        anyhow::ensure!(
            difficulty.is_finite() && difficulty > 0.0,
            "worker difficulty must be positive and finite"
        );
        self.written.lock().unwrap().push((
            worker.username.clone(),
            difficulty,
            share_id.map(str::to_owned),
            downward_only,
        ));
        let mut rows = self.rows.lock().unwrap();
        if downward_only {
            if let Some(row) = rows.get_mut(&worker.username) {
                *row = row.min(difficulty);
            }
        } else {
            rows.insert(worker.username.clone(), difficulty);
        }
        Ok(())
    }
    async fn new_session_id(&self) -> std::result::Result<SessionId, StratumError> {
        Err(StratumError::backend("no sessions in this test"))
    }
    async fn authorize(&self, _username: &str) -> std::result::Result<Worker, StratumError> {
        Err(StratumError::backend("no sessions in this test"))
    }
    async fn build_job(
        &self,
        _worker: &Worker,
        _extranonce1: &str,
        _difficulty: f64,
        _minimum: f64,
    ) -> std::result::Result<MiningJob<()>, StratumError> {
        Err(StratumError::backend("no work in this test"))
    }
    async fn submit(
        &self,
        _worker: &Worker,
        _job: &MiningJob<()>,
        _submission: Submission,
        _grace: StaleGrace,
    ) -> std::result::Result<(), StratumError> {
        Err(StratumError::backend("no work in this test"))
    }
}

fn worker(username: &str) -> Worker {
    Worker {
        username: username.into(),
        payout_address: String::new(),
        worker_name: None,
        p2mr_program_hex: String::new(),
    }
}

fn count(metrics: &Metrics, outcome: DifficultyHintOutcome) -> f64 {
    let name = format!(
        "qbit_prism_stratum_difficulty_hints_total{{outcome=\"{}\"}} ",
        outcome.as_str()
    );
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&name))
        .unwrap()
        .parse()
        .unwrap()
}

fn cached(config: &StratumConfig, username: &str) -> Option<f64> {
    config
        .retained_difficulties
        .lock()
        .unwrap()
        .get(&(config.listener_name.clone(), username.to_owned()))
        .map(|(difficulty, _)| *difficulty)
}

/// Offer the hint of a share accepted now for `username`.
fn offer(
    hints: &DifficultyHints,
    config: &StratumConfig,
    metrics: &Metrics,
    username: &str,
    difficulty: f64,
) {
    offer_accepted(hints, config, metrics, username, difficulty, Instant::now());
}

/// Offer the hint of a share this frontend accepted at `accepted`.
fn offer_accepted(
    hints: &DifficultyHints,
    config: &StratumConfig,
    metrics: &Metrics,
    username: &str,
    difficulty: f64,
    accepted: Instant,
) {
    let share = format!("{username}:{difficulty}");
    hints.offer(
        config,
        metrics,
        &worker(username),
        difficulty,
        Some((&share, accepted)),
        false,
    );
}

/// Offer an unproven decrease for `username`.
fn lower(
    hints: &DifficultyHints,
    config: &StratumConfig,
    metrics: &Metrics,
    username: &str,
    difficulty: f64,
) {
    hints.offer(config, metrics, &worker(username), difficulty, None, true);
}

/// Poll until `done`, at most five seconds of the test's clock. Sleeping,
/// not yielding, lets a paused clock advance meanwhile.
async fn until(mut done: impl FnMut() -> bool) {
    timeout(Duration::from_secs(5), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the writer never got there");
}

/// Yield until `done`, for a test on a paused clock: nothing here sleeps, so
/// the clock moves only when the test advances it.
async fn settled(mut done: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the writer never got there");
}

fn row(store: &HintStore, username: &str) -> Option<f64> {
    store.rows.lock().unwrap().get(username).copied()
}

struct Fixture {
    store: Arc<HintStore>,
    metrics: Arc<Metrics>,
    hints: DifficultyHints,
}

fn fixture() -> Fixture {
    Fixture {
        store: Arc::default(),
        metrics: Arc::default(),
        hints: DifficultyHints::new(),
    }
}

impl Fixture {
    /// As a listener runs its writer.
    async fn beside<T>(&self, config: &StratumConfig, work: impl Future<Output = T>) -> T {
        self.beside_ending(WriterEnd::Abandon, config, work).await
    }

    async fn beside_ending<T>(
        &self,
        end: WriterEnd,
        config: &StratumConfig,
        work: impl Future<Output = T>,
    ) -> T {
        self.hints
            .beside(
                self.store.clone(),
                config.clone(),
                self.metrics.clone(),
                end,
                work,
            )
            .await
    }
}

#[tokio::test]
async fn a_full_queue_drops_new_workers_and_a_queued_worker_keeps_its_newest_hint_and_place() {
    let config = StratumConfig {
        difficulty_hint_queue_capacity: 2,
        ..Default::default()
    };
    let f = fixture();
    offer(&f.hints, &config, &f.metrics, "first", 1.0);
    offer(&f.hints, &config, &f.metrics, "second", 2.0);
    // Full: a hint for a third worker is dropped and counted, at once.
    offer(&f.hints, &config, &f.metrics, "third", 3.0);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Dropped), 1.);
    // A worker already queued is not a new entry: its newest evidence
    // replaces the queued one and keeps its place, full or not.
    offer(&f.hints, &config, &f.metrics, "first", 4.0);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Superseded), 1.);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Dropped), 1.);
    f.beside(&config, until(|| f.store.written().len() == 2))
        .await;
    assert_eq!(
        f.store.written(),
        [
            ("first".into(), 4.0, Some("first:4".into()), false),
            ("second".into(), 2.0, Some("second:2".into()), false),
        ],
        "the newest hint for the oldest queued worker first; the dropped one never"
    );
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Written), 2.);
    assert_eq!(cached(&config, "first"), Some(4.0));
    assert_eq!(cached(&config, "second"), Some(2.0));
    assert_eq!(cached(&config, "third"), None);
}

#[tokio::test]
async fn an_offer_never_waits_for_a_held_write() {
    let config = StratumConfig {
        difficulty_hint_queue_capacity: 1,
        ..Default::default()
    };
    let f = fixture();
    let gate = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "held", 1.0);
        gate.entered.notified().await;
        // The writer is stuck in the held write: the queue takes one more
        // and refuses the next, and neither offer waits.
        offer(&f.hints, &config, &f.metrics, "queued", 2.0);
        offer(&f.hints, &config, &f.metrics, "refused", 3.0);
        assert_eq!(count(&f.metrics, DifficultyHintOutcome::Dropped), 1.);
        assert!(f.store.written().is_empty());
        gate.release.notify_one();
        until(|| f.store.written().len() == 2).await;
    };
    f.beside(&config, work).await;
    let workers: Vec<_> = f.store.written().into_iter().map(|w| w.0).collect();
    assert_eq!(workers, ["held", "queued"]);
}

// Before #856 every hint was a write of its own, in the order offered. A
// queued entry still writes what those writes would have left: decreases
// fold to the lowest, each on its own side of the evidence, and of two
// pieces of evidence the one accepted later is kept, whichever session
// offered it last.
#[tokio::test]
async fn hints_for_a_queued_worker_combine_into_what_their_separate_writes_left() {
    let config = StratumConfig::default();
    let f = fixture();
    let gate = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "blocker", 1.0);
        gate.entered.notified().await;
        // Evidence, then two decreases: recorded, then lowered to the lower.
        offer(&f.hints, &config, &f.metrics, "rig", 8.0);
        lower(&f.hints, &config, &f.metrics, "rig", 4.0);
        lower(&f.hints, &config, &f.metrics, "rig", 6.0);
        // A decrease, then evidence: lowered first, then recorded.
        lower(&f.hints, &config, &f.metrics, "other", 2.0);
        offer(&f.hints, &config, &f.metrics, "other", 5.0);
        // Two sessions of one username: the newer share, offered first,
        // outlives an older one offered after it.
        let older = Instant::now() - Duration::from_secs(1);
        offer(&f.hints, &config, &f.metrics, "shared", 64.0);
        offer_accepted(&f.hints, &config, &f.metrics, "shared", 6.0, older);
        // Newer evidence replaces older; the decrease between them now
        // comes before it.
        offer(&f.hints, &config, &f.metrics, "moved", 10.0);
        lower(&f.hints, &config, &f.metrics, "moved", 5.0);
        offer(&f.hints, &config, &f.metrics, "moved", 20.0);
        // ...and joins one already there, folding into the lower.
        lower(&f.hints, &config, &f.metrics, "joined", 9.0);
        offer(&f.hints, &config, &f.metrics, "joined", 10.0);
        lower(&f.hints, &config, &f.metrics, "joined", 5.0);
        offer(&f.hints, &config, &f.metrics, "joined", 20.0);
        assert_eq!(count(&f.metrics, DifficultyHintOutcome::Superseded), 5.);
        gate.release.notify_one();
        until(|| f.store.written().len() == 10).await;
    };
    f.beside(&config, work).await;
    assert_eq!(
        f.store.written()[1..],
        [
            ("rig".into(), 8.0, Some("rig:8".into()), false),
            ("rig".into(), 4.0, None, true),
            ("other".into(), 2.0, None, true),
            ("other".into(), 5.0, Some("other:5".into()), false),
            ("shared".into(), 64.0, Some("shared:64".into()), false),
            ("moved".into(), 5.0, None, true),
            ("moved".into(), 20.0, Some("moved:20".into()), false),
            ("joined".into(), 5.0, None, true),
            ("joined".into(), 20.0, Some("joined:20".into()), false),
        ]
    );
    assert_eq!(cached(&config, "rig"), Some(4.0));
    assert_eq!(cached(&config, "other"), Some(5.0));
    assert_eq!(cached(&config, "shared"), Some(64.0));
    assert_eq!(cached(&config, "moved"), Some(20.0));
    // Every offered hint counted once: 10 written, 5 superseded.
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Written), 10.);
}

// Each step of an entry is a write of its own, with its own bound: a record
// that fails still lets the decrease after it land, as the separate writes
// did, and each is counted for what became of it.
#[tokio::test]
async fn a_failed_record_still_lets_the_decrease_after_it_land() {
    let config = StratumConfig::default();
    let f = fixture();
    f.store.rows.lock().unwrap().insert("rig".into(), 16.0);
    let gate = f.store.hold_next();
    let work = async {
        lower(&f.hints, &config, &f.metrics, "blocker", 1.0);
        gate.entered.notified().await;
        offer(&f.hints, &config, &f.metrics, "rig", 8.0);
        lower(&f.hints, &config, &f.metrics, "rig", 4.0);
        f.store.fail_records.store(true, Ordering::SeqCst);
        gate.release.notify_one();
        until(|| count(&f.metrics, DifficultyHintOutcome::Written) == 2.).await;
    };
    f.beside(&config, work).await;
    assert_eq!(f.store.rows.lock().unwrap()["rig"], 4.0);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Failed), 1.);
    assert_eq!(
        f.store.written().last(),
        Some(&("rig".into(), 4.0, None, true))
    );
}

// EP-STATE: writes never overlap, so an older write's read-back reaches the
// retained-difficulty cache before a newer one's and never after it. A hint
// superseded in the queue is never written, so it never reaches the cache;
// a newer write that fails leaves the older write's committed value.
#[tokio::test]
async fn writes_never_overlap_and_only_committed_read_backs_reach_the_cache() {
    let config = StratumConfig::default();
    let f = fixture();
    let older = f.store.hold_next();
    let newer = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "rig", 1.0);
        older.entered.notified().await;
        offer(&f.hints, &config, &f.metrics, "rig", 2.0);
        offer(&f.hints, &config, &f.metrics, "rig", 3.0);
        assert_eq!(count(&f.metrics, DifficultyHintOutcome::Superseded), 1.);
        older.release.notify_one();
        // The newer write starts only once the older one has landed.
        newer.entered.notified().await;
        assert_eq!(f.store.written().len(), 1);
        assert_eq!(cached(&config, "rig"), Some(1.0));
        f.store.fail.store(true, Ordering::SeqCst);
        newer.release.notify_one();
        until(|| count(&f.metrics, DifficultyHintOutcome::Failed) == 1.).await;
        assert_eq!(cached(&config, "rig"), Some(1.0));
        f.store.fail.store(false, Ordering::SeqCst);
        offer(&f.hints, &config, &f.metrics, "rig", 4.0);
        until(|| cached(&config, "rig") == Some(4.0)).await;
    };
    f.beside(&config, work).await;
    let difficulties: Vec<_> = f.store.written().into_iter().map(|w| w.1).collect();
    assert_eq!(difficulties, [1.0, 4.0], "the superseded 2.0 was written");
}

#[tokio::test(start_paused = true)]
async fn a_write_past_its_bound_is_timed_out_and_a_refused_one_failed_never_dropped() {
    let config = StratumConfig::default();
    let f = fixture();
    // Held and never released: the 500 ms bound ends it.
    let _held = f.store.hold_next();
    let work = async {
        let started = tokio::time::Instant::now();
        offer(&f.hints, &config, &f.metrics, "slow", 1.0);
        until(|| count(&f.metrics, DifficultyHintOutcome::TimedOut) == 1.).await;
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_millis(500) && waited < Duration::from_millis(510),
            "{waited:?}"
        );
        f.store.fail.store(true, Ordering::SeqCst);
        offer(&f.hints, &config, &f.metrics, "refused", 2.0);
        until(|| count(&f.metrics, DifficultyHintOutcome::Failed) == 1.).await;
    };
    f.beside(&config, work).await;
    assert_eq!(cached(&config, "slow"), None);
    assert_eq!(cached(&config, "refused"), None);
    for outcome in [
        DifficultyHintOutcome::Written,
        DifficultyHintOutcome::Dropped,
        DifficultyHintOutcome::Abandoned,
    ] {
        assert_eq!(count(&f.metrics, outcome), 0., "{}", outcome.as_str());
    }
}

// EP-ERRORS: a write cancelled mid-statement would leave its statement
// running on the server after the connection is gone. When the listener's
// work ends, the write in flight finishes, within its bound, before the
// listener returns; what is queued is dropped and counted, and no other
// write starts.
#[tokio::test]
async fn a_stopping_listener_lets_the_write_in_flight_finish_and_abandons_what_is_queued() {
    // The write is held as long as the test needs, never cut short by its
    // bound.
    let config = StratumConfig {
        difficulty_hint_timeout_seconds: 60.0,
        ..Default::default()
    };
    let f = fixture();
    let gate = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "in-flight", 1.0);
        gate.entered.notified().await;
        offer(&f.hints, &config, &f.metrics, "queued", 2.0);
    };
    let release = async {
        // The work has ended and the queued hint is gone; the held write
        // has not been cancelled.
        until(|| count(&f.metrics, DifficultyHintOutcome::Abandoned) == 1.).await;
        assert!(f.store.written().is_empty());
        gate.release.notify_one();
    };
    tokio::join!(f.beside(&config, work), release);
    assert_eq!(
        f.store.written(),
        [("in-flight".into(), 1.0, Some("in-flight:1".into()), false)]
    );
    assert_eq!(cached(&config, "in-flight"), Some(1.0));
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Written), 1.);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Abandoned), 1.);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::WriterPanicked), 0.);
    // A session still holding the queue after that cannot queue again.
    offer(&f.hints, &config, &f.metrics, "late", 3.0);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Abandoned), 2.);
}

// ...but no longer than its own bound: a stopping listener waits for a hung
// write until it times out, and the rest of that write's entry is abandoned,
// not started.
#[tokio::test(start_paused = true)]
async fn a_stopping_listener_waits_no_longer_than_the_write_bound() {
    let config = StratumConfig::default();
    let f = fixture();
    // Held and never released.
    let _held = f.store.hold_next();
    let started = tokio::time::Instant::now();
    let work = async {
        // Offered before the writer first runs: one entry, two writes.
        offer(&f.hints, &config, &f.metrics, "in-flight", 8.0);
        lower(&f.hints, &config, &f.metrics, "in-flight", 4.0);
        settled(|| f.store.seen.lock().unwrap().len() == 1).await;
        offer(&f.hints, &config, &f.metrics, "queued", 2.0);
    };
    f.beside(&config, work).await;
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(500) && waited < Duration::from_millis(510),
        "{waited:?}"
    );
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::TimedOut), 1.);
    // The queued hint and the decrease after the timed-out write.
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Abandoned), 2.);
    assert_eq!(f.store.seen.lock().unwrap().len(), 1, "a write started");
    assert!(f.store.written().is_empty());
}

// A listener aborted mid-flight (a frontend stop, or the shutdown timeout)
// drops `beside` without finishing it: the queue still closes and counts
// what it held, and the writer task is aborted with it.
#[tokio::test]
async fn an_aborted_listener_still_stops_its_writer_and_counts_what_it_held() {
    let config = StratumConfig::default();
    let f = fixture();
    let gate = f.store.hold_next();
    let (abort, aborted) = tokio::sync::oneshot::channel::<()>();
    let listener = f.beside(&config, std::future::pending::<()>());
    let driver = async {
        offer(&f.hints, &config, &f.metrics, "in-flight", 1.0);
        gate.entered.notified().await;
        offer(&f.hints, &config, &f.metrics, "queued", 2.0);
        abort.send(()).unwrap();
        std::future::pending::<()>().await;
    };
    tokio::select! {
        () = listener => unreachable!("the listener's work never ends"),
        _ = aborted => {}
        () = driver => unreachable!(),
    }
    // Dropped: the queued hint is counted at once, the cancelled write once
    // its aborted task is dropped.
    assert!(count(&f.metrics, DifficultyHintOutcome::Abandoned) >= 1.);
    until(|| count(&f.metrics, DifficultyHintOutcome::Abandoned) == 2.).await;
    offer(&f.hints, &config, &f.metrics, "late", 3.0);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Abandoned), 3.);
    gate.release.notify_one();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        f.store.written().is_empty(),
        "a write outlived its listener"
    );
}

// A writer that panics writes nothing more, so its queue closes: what it
// held and every later offer count as `writer_panicked`, never as a stopping
// listener's `abandoned`, and the listener goes on.
#[tokio::test]
async fn a_writer_that_panics_closes_its_queue_and_the_listener_carries_on() {
    let config = StratumConfig::default();
    let f = fixture();
    f.store.panic.store(true, Ordering::SeqCst);
    let panicked = || count(&f.metrics, DifficultyHintOutcome::WriterPanicked);
    let work = async {
        offer(&f.hints, &config, &f.metrics, "boom", 1.0);
        until(|| panicked() == 1.).await;
        // Counted at once if the queue has closed already, or when it does.
        offer(&f.hints, &config, &f.metrics, "later", 2.0);
        until(|| panicked() == 2.).await;
        offer(&f.hints, &config, &f.metrics, "closed", 3.0);
        assert_eq!(panicked(), 3.);
        "carried on"
    };
    assert_eq!(f.beside(&config, work).await, "carried on");
    // The listener has stopped since, but nothing wrote hints before that
    // either: an offer still counts as the writer's.
    offer(&f.hints, &config, &f.metrics, "stopped", 4.0);
    assert_eq!(panicked(), 4.);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Abandoned), 0.);
    assert!(f.store.written().is_empty());
}

// A connection served on its own writes what its session queued before it
// returns, as when the session awaited each hint, so a fuzz target's backend
// still sees every hint.
#[tokio::test]
async fn a_connection_served_on_its_own_writes_what_it_queued_before_it_returns() {
    let config = StratumConfig::default();
    let f = fixture();
    let gate = f.store.hold_next();
    let release = async {
        gate.entered.notified().await;
        gate.release.notify_one();
    };
    let work = async {
        offer(&f.hints, &config, &f.metrics, "held", 1.0);
        offer(&f.hints, &config, &f.metrics, "queued", 2.0);
    };
    tokio::join!(f.beside_ending(WriterEnd::Drain, &config, work), release);
    assert_eq!(f.store.written().len(), 2);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Written), 2.);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Abandoned), 0.);
    offer(&f.hints, &config, &f.metrics, "late", 3.0);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Abandoned), 1.);
}

// A connection cancelled while it drains takes its writer with it: the
// write in flight is cancelled, not left running detached.
#[tokio::test]
async fn a_connection_cancelled_mid_drain_stops_its_writer() {
    let config = StratumConfig::default();
    let f = fixture();
    let gate = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "held", 1.0);
        offer(&f.hints, &config, &f.metrics, "queued", 2.0);
    };
    // The session is over; the connection is draining, held in the first
    // write, when it is cancelled.
    let draining = f.beside_ending(WriterEnd::Drain, &config, work);
    tokio::select! {
        _ = draining => unreachable!("the drain is held"),
        () = gate.entered.notified() => {}
    }
    until(|| count(&f.metrics, DifficultyHintOutcome::Abandoned) == 2.).await;
    gate.release.notify_one();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        f.store.written().is_empty(),
        "a write outlived its connection"
    );
}

// ...and a panic in its writer is the connection's, as it was the session's.
#[tokio::test]
async fn a_writer_panic_is_a_connection_served_on_its_owns_panic() {
    let config = StratumConfig::default();
    let f = Arc::new(fixture());
    f.store.panic.store(true, Ordering::SeqCst);
    let connection = tokio::spawn({
        let f = f.clone();
        async move {
            let work = async {
                offer(&f.hints, &config, &f.metrics, "boom", 1.0);
                std::future::pending::<()>().await
            };
            f.beside_ending(WriterEnd::Drain, &config, work).await
        }
    });
    let ended = timeout(Duration::from_secs(5), connection).await.unwrap();
    assert!(ended.unwrap_err().is_panic());
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::WriterPanicked), 1.);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Abandoned), 0.);
}

// The same when the panic comes while the connection drains, after its
// session has ended.
#[tokio::test]
async fn a_writer_panic_while_a_connection_drains_is_the_connections_panic() {
    let config = StratumConfig::default();
    let f = Arc::new(fixture());
    f.store.panic.store(true, Ordering::SeqCst);
    let connection = tokio::spawn({
        let f = f.clone();
        async move {
            let work = async {
                offer(&f.hints, &config, &f.metrics, "boom", 1.0);
                offer(&f.hints, &config, &f.metrics, "behind", 2.0);
            };
            f.beside_ending(WriterEnd::Drain, &config, work).await
        }
    });
    let ended = timeout(Duration::from_secs(5), connection).await.unwrap();
    assert!(ended.unwrap_err().is_panic());
    // The write it panicked in, and the hint still queued behind it.
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::WriterPanicked), 2.);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Abandoned), 0.);
}

#[tokio::test]
async fn offers_that_were_never_writes_queue_nothing() {
    let config = StratumConfig {
        difficulty_hint_queue_capacity: 1,
        ..Default::default()
    };
    let f = fixture();
    // An upward or unchanged hint without evidence never wrote anything.
    f.hints
        .offer(&config, &f.metrics, &worker("rig"), 1.0, None, false);
    let disabled = StratumConfig {
        resume_enabled: false,
        ..config.clone()
    };
    offer(&f.hints, &disabled, &f.metrics, "rig", 1.0);
    // Nothing took the one slot.
    offer(&f.hints, &config, &f.metrics, "rig", 1.0);
    for outcome in DifficultyHintOutcome::ALL {
        assert_eq!(count(&f.metrics, *outcome), 0., "{}", outcome.as_str());
    }
    f.beside(&config, until(|| f.store.written().len() == 1))
        .await;
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Written), 1.);
}

// A decrease carries no share, and the ledger lowers any higher row, so one
// written late could pull down newer evidence another frontend recorded
// meanwhile. One that waited longer than its bound when its write is about
// to run is not written, touches neither the row nor the cache, and is
// counted as aged out; one just inside it is written. Evidence never ages.
#[tokio::test(start_paused = true)]
async fn a_decrease_is_written_within_its_wait_bound_and_aged_out_past_it_and_evidence_never_is() {
    // Writes are held as long as the test needs, never cut short by their own
    // bound: only the queue wait is aged.
    let config = StratumConfig {
        difficulty_hint_timeout_seconds: 60.0,
        ..Default::default()
    };
    let f = fixture();
    for rig in ["late", "inside"] {
        f.store.rows.lock().unwrap().insert(rig.into(), 8.0);
        config.retained_difficulties.lock().unwrap().insert(
            (config.listener_name.clone(), rig.into()),
            (8.0, Instant::now()),
        );
    }
    let gate = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "blocker", 1.0);
        gate.entered.notified().await;
        lower(&f.hints, &config, &f.metrics, "late", 2.0);
        offer(&f.hints, &config, &f.metrics, "evidence", 3.0);
        tokio::time::advance(Duration::from_millis(2)).await;
        lower(&f.hints, &config, &f.metrics, "inside", 4.0);
        // The writer reaches them 1 ms past the bound for "late" and the
        // evidence, 1 ms inside it for "inside".
        tokio::time::advance(MAX_DECREASE_WAIT - Duration::from_millis(1)).await;
        assert_eq!(count(&f.metrics, DifficultyHintOutcome::AgedOut), 0.);
        gate.release.notify_one();
        settled(|| count(&f.metrics, DifficultyHintOutcome::Written) == 3.).await;
    };
    f.beside(&config, work).await;
    assert_eq!(
        f.store.written()[1..],
        [
            ("evidence".into(), 3.0, Some("evidence:3".into()), false),
            ("inside".into(), 4.0, None, true),
        ]
    );
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::AgedOut), 1.);
    assert_eq!(row(&f.store, "late"), Some(8.0), "an aged decrease lowered");
    assert_eq!(cached(&config, "late"), Some(8.0));
    assert_eq!(row(&f.store, "inside"), Some(4.0));
    assert_eq!(cached(&config, "inside"), Some(4.0));
    assert_eq!(cached(&config, "evidence"), Some(3.0));
}

// A held decrease already past its bound would never be written, so a fresh
// one folded into it replaces it whatever their values, as their separate
// writes would have left the row: the stale one is aged out, the fresh one
// written.
#[tokio::test(start_paused = true)]
async fn a_decrease_past_its_bound_is_replaced_by_a_fresh_one_folded_into_it() {
    let config = StratumConfig {
        difficulty_hint_timeout_seconds: 60.0,
        ..Default::default()
    };
    let f = fixture();
    for rig in ["stale-lower", "stale-higher", "moved"] {
        f.store.rows.lock().unwrap().insert(rig.into(), 16.0);
    }
    let gate = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "blocker", 1.0);
        gate.entered.notified().await;
        lower(&f.hints, &config, &f.metrics, "stale-lower", 2.0);
        lower(&f.hints, &config, &f.metrics, "stale-higher", 8.0);
        lower(&f.hints, &config, &f.metrics, "moved", 2.0);
        offer(&f.hints, &config, &f.metrics, "moved", 10.0);
        tokio::time::advance(MAX_DECREASE_WAIT + Duration::from_millis(1)).await;
        lower(&f.hints, &config, &f.metrics, "stale-lower", 4.0);
        lower(&f.hints, &config, &f.metrics, "stale-higher", 4.0);
        // The same when newer evidence moves a fresh decrease from after the
        // old evidence to before the new one, onto a stale decrease there.
        lower(&f.hints, &config, &f.metrics, "moved", 4.0);
        offer(&f.hints, &config, &f.metrics, "moved", 20.0);
        assert_eq!(count(&f.metrics, DifficultyHintOutcome::AgedOut), 3.);
        // Only the replaced evidence.
        assert_eq!(count(&f.metrics, DifficultyHintOutcome::Superseded), 1.);
        gate.release.notify_one();
        settled(|| count(&f.metrics, DifficultyHintOutcome::Written) == 5.).await;
    };
    f.beside(&config, work).await;
    assert_eq!(
        f.store.written()[1..],
        [
            ("stale-lower".into(), 4.0, None, true),
            ("stale-higher".into(), 4.0, None, true),
            ("moved".into(), 4.0, None, true),
            ("moved".into(), 20.0, Some("moved:20".into()), false),
        ]
    );
    assert_eq!(row(&f.store, "stale-lower"), Some(4.0));
    assert_eq!(row(&f.store, "stale-higher"), Some(4.0));
    assert_eq!(row(&f.store, "moved"), Some(20.0));
}

// Folded while both are within the bound, the lower decrease is kept with the
// time it was itself offered, so it ages as it would have alone. If it passes
// the bound before the write, the higher one folded into it is not written
// either, although alone it would have been: the fold errs toward leaving the
// row as it is, never toward a stale decrease.
#[tokio::test(start_paused = true)]
async fn a_folded_decrease_ages_from_the_lowest_ones_offer() {
    let config = StratumConfig {
        difficulty_hint_timeout_seconds: 60.0,
        ..Default::default()
    };
    let f = fixture();
    for rig in ["lowest-first", "lowest-last"] {
        f.store.rows.lock().unwrap().insert(rig.into(), 16.0);
    }
    let gate = f.store.hold_next();
    let half = MAX_DECREASE_WAIT / 2;
    let work = async {
        offer(&f.hints, &config, &f.metrics, "blocker", 1.0);
        gate.entered.notified().await;
        lower(&f.hints, &config, &f.metrics, "lowest-first", 2.0);
        lower(&f.hints, &config, &f.metrics, "lowest-last", 8.0);
        tokio::time::advance(half).await;
        lower(&f.hints, &config, &f.metrics, "lowest-first", 4.0);
        lower(&f.hints, &config, &f.metrics, "lowest-last", 4.0);
        assert_eq!(count(&f.metrics, DifficultyHintOutcome::Superseded), 2.);
        // Past the bound for the first offers, inside it for the second.
        tokio::time::advance(MAX_DECREASE_WAIT - half + Duration::from_millis(1)).await;
        gate.release.notify_one();
        settled(|| count(&f.metrics, DifficultyHintOutcome::AgedOut) == 1.).await;
        settled(|| count(&f.metrics, DifficultyHintOutcome::Written) == 2.).await;
    };
    f.beside(&config, work).await;
    assert_eq!(
        f.store.written()[1..],
        [("lowest-last".into(), 4.0, None, true)]
    );
    assert_eq!(row(&f.store, "lowest-first"), Some(16.0));
    assert_eq!(row(&f.store, "lowest-last"), Some(4.0));
}

// Folding would hide a difficulty the ledger refuses: `f64::min` keeps the
// other operand of a NaN and the finite one beside an infinite decrease, and
// newer evidence replaces older. The first refused difficulty per queued
// worker is kept apart and written as offered, after the valid hints beside
// it, so a backend that checks every difficulty (the fuzz targets') still
// sees it and the valid hints are written as before.
#[tokio::test]
async fn a_difficulty_the_ledger_refuses_is_never_folded_away() {
    let config = StratumConfig::default();
    let f = fixture();
    let gate = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "blocker", 1.0);
        gate.entered.notified().await;
        lower(&f.hints, &config, &f.metrics, "nan", 4.0);
        lower(&f.hints, &config, &f.metrics, "nan", f64::NAN);
        lower(&f.hints, &config, &f.metrics, "infinite", f64::INFINITY);
        lower(&f.hints, &config, &f.metrics, "infinite", 4.0);
        let older = Instant::now() - Duration::from_secs(1);
        offer_accepted(&f.hints, &config, &f.metrics, "evidence", -1.0, older);
        offer(&f.hints, &config, &f.metrics, "evidence", 8.0);
        // One refused hint queued per worker is enough to be seen.
        lower(&f.hints, &config, &f.metrics, "nan", f64::NEG_INFINITY);
        assert_eq!(count(&f.metrics, DifficultyHintOutcome::Superseded), 1.);
        gate.release.notify_one();
        until(|| f.store.seen.lock().unwrap().len() == 7).await;
        until(|| count(&f.metrics, DifficultyHintOutcome::Failed) == 3.).await;
    };
    f.beside(&config, work).await;
    let seen: Vec<_> = f
        .store
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|(worker, difficulty)| format!("{worker} {difficulty}"))
        .collect();
    assert_eq!(
        seen[1..],
        [
            "nan 4",
            "nan NaN",
            "infinite 4",
            "infinite inf",
            "evidence 8",
            "evidence -1"
        ]
    );
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Written), 4.);
    assert_eq!(cached(&config, "evidence"), Some(8.0));
}

// A session that re-authorized under another username and then had a share
// accepted on a job of its earlier one offers its next retarget with that
// share as the new worker's evidence. The ledger refuses it, as another
// worker's, so it is not evidence for this one: kept as the later-accepted
// evidence it would replace evidence the ledger would have taken, and both
// would be lost. It is kept apart and written last, whichever was offered
// first, as their separate writes would have left the row.
#[tokio::test]
async fn another_workers_share_never_replaces_the_workers_own_evidence() {
    let config = StratumConfig::default();
    let f = fixture();
    let gate = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "blocker", 1.0);
        gate.entered.notified().await;
        let older = Instant::now() - Duration::from_secs(1);
        let foreign = |username: &str, accepted| {
            let evidence = Some(("other:2", accepted));
            f.hints
                .offer(&config, &f.metrics, &worker(username), 2.0, evidence, false);
        };
        // The worker's own evidence first, then another worker's share,
        // accepted later.
        offer_accepted(&f.hints, &config, &f.metrics, "own-first", 8.0, older);
        foreign("own-first", Instant::now());
        // Another worker's share first, then the worker's own evidence,
        // accepted earlier.
        foreign("own-last", Instant::now());
        offer_accepted(&f.hints, &config, &f.metrics, "own-last", 8.0, older);
        assert_eq!(count(&f.metrics, DifficultyHintOutcome::Superseded), 0.);
        gate.release.notify_one();
        until(|| count(&f.metrics, DifficultyHintOutcome::Failed) == 2.).await;
    };
    f.beside(&config, work).await;
    let seen: Vec<_> = f
        .store
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|(worker, difficulty)| format!("{worker} {difficulty}"))
        .collect();
    assert_eq!(
        seen[1..],
        ["own-first 8", "own-first 2", "own-last 8", "own-last 2"]
    );
    let own = |username: &str| {
        (
            username.to_owned(),
            8.0,
            Some(format!("{username}:8")),
            false,
        )
    };
    assert_eq!(f.store.written()[1..], [own("own-first"), own("own-last")]);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Written), 3.);
    assert_eq!(cached(&config, "own-first"), Some(8.0));
    assert_eq!(cached(&config, "own-last"), Some(8.0));
}

// One refused hint is held per queued worker. Another worker's share at a
// difficulty the ledger takes gives its place to a difficulty the ledger
// refuses, so a backend that checks every difficulty still sees that one;
// nothing takes the place of a refused difficulty.
#[tokio::test]
async fn a_refused_difficulty_is_seen_though_another_workers_share_was_refused_first() {
    let config = StratumConfig::default();
    let f = fixture();
    let gate = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "blocker", 1.0);
        gate.entered.notified().await;
        let foreign = || {
            let evidence = Some(("other:2", Instant::now()));
            f.hints
                .offer(&config, &f.metrics, &worker("rig"), 2.0, evidence, false);
        };
        foreign();
        lower(&f.hints, &config, &f.metrics, "rig", f64::NAN);
        lower(&f.hints, &config, &f.metrics, "rig", f64::INFINITY);
        foreign();
        assert_eq!(count(&f.metrics, DifficultyHintOutcome::Superseded), 3.);
        gate.release.notify_one();
        until(|| count(&f.metrics, DifficultyHintOutcome::Failed) == 1.).await;
    };
    f.beside(&config, work).await;
    let seen = f.store.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!(seen[1].1.is_nan(), "{seen:?}");
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Written), 1.);
}

// A refused difficulty changes no row however late it is written, so it never
// ages: a decrease past its wait bound that the ledger would refuse still
// reaches the backend, where the fuzz targets' check sees it.
#[tokio::test(start_paused = true)]
async fn a_refused_difficulty_is_written_however_long_it_waited() {
    let config = StratumConfig {
        difficulty_hint_timeout_seconds: 60.0,
        ..Default::default()
    };
    let f = fixture();
    let gate = f.store.hold_next();
    let work = async {
        offer(&f.hints, &config, &f.metrics, "blocker", 1.0);
        gate.entered.notified().await;
        lower(&f.hints, &config, &f.metrics, "nan", f64::NAN);
        tokio::time::advance(MAX_DECREASE_WAIT + Duration::from_millis(1)).await;
        gate.release.notify_one();
        settled(|| count(&f.metrics, DifficultyHintOutcome::Failed) == 1.).await;
    };
    f.beside(&config, work).await;
    let seen = f.store.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert!(seen[1].1.is_nan(), "{seen:?}");
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::AgedOut), 0.);
    assert_eq!(count(&f.metrics, DifficultyHintOutcome::Written), 1.);
}
