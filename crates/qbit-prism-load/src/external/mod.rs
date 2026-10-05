//! External-target mode: real Stratum sessions against a deployed frontend,
//! or the operator TCP load balancer in front of several, instead of
//! frontends the harness launches itself (#291's failover drill,
//! load-balancer exercise and soak).
//!
//! It is the harness's own client, not a second load generator (#474): the
//! session task in [`crate::client`] connects, subscribes, authorizes, mines
//! real proof of work on the jobs the target sends and reconnects when its
//! connection goes, and the open-loop token bucket the phases use places the
//! offers. What changes is what an external target cannot give it. Nothing
//! is launched, seeded or read from a database, so there is no artifact and
//! no reconciliation: the stats are what the client saw, and `--share-log`
//! keeps every share id for reconciling against the target's ledger. The
//! target's share difficulty is not the harness's to configure, so each job
//! is mined at the difficulty the target advertised for it
//! ([`crate::client::DifficultySource::Advertised`]), up to a ceiling.
//!
//! One process per client machine. Each writes a document whose counts,
//! histograms and per-second timeline add exactly ([`stats`]), and
//! `qbit-prism-load external-merge` adds several into one.

pub mod histogram;
pub mod stats;

use crate::client::{self, Control, DifficultySource, SessionConfig, SessionHandle, SessionShared};
use crate::run::{EXIT_ABORTED, EXIT_BLOCKED, EXIT_OK};
use anyhow::{ensure, Context, Result};
use clap::Parser;
use serde_json::Value;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// `qbit-prism-load external ...`.
pub const RUN_COMMAND: &str = "external";
/// `qbit-prism-load external-merge ...`.
pub const MERGE_COMMAND: &str = "external-merge";
/// The flag without which the mode refuses to send anything.
pub const GUARD_FLAG: &str = "--i-understand-external-target";
/// The phase stamp every offer carries.
pub const PHASE: &str = "external";

/// The default ceiling, 2^-16: about 65,536 hashes a share, tens of
/// milliseconds of one core in a release build.
pub const DEFAULT_MAX_DIFFICULTY: f64 = 1.0 / 65_536.0;
/// The highest ceiling accepted, 2^-14: a share search covers 2^22 nonces,
/// which finds a share at this difficulty with probability 1 - e^-16.
pub const MAX_DIFFICULTY_LIMIT: f64 = 1.0 / 16_384.0;
/// The longest connect attempt; `--work-timeout-seconds` bounds the rest of
/// a handshake.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// How long `--target` may take to resolve at entry.
pub const RESOLVE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a stopped session may take to finish its last search and exit.
pub const STOP_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the stats may take to take in the last events once every session
/// has stopped. A search an aborted session left running still holds a
/// sender until it ends.
pub const COLLECTOR_TIMEOUT: Duration = Duration::from_secs(30);
/// Session events that may wait for the stats. A collector this far behind
/// is losing ground, and the queue would grow without bound: the run stops
/// itself, as a signal would stop it, and says why.
pub const EVENT_BACKLOG_LIMIT: usize = 250_000;
/// The highest `--rate`: a share costs thousands of hashes, so one client
/// machine mines far fewer.
pub const MAX_RATE: f64 = 100_000.0;

/// Which subcommand a command line names, if any.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Command {
    Run,
    Merge,
}

impl Command {
    /// The subcommand `argv[1]` names. Checked before the harness's own
    /// parser and preset expansion see the line, so the mode's flags are not
    /// harness flags and no preset has to pin them.
    pub fn of(argv: &[OsString]) -> Option<Self> {
        match argv.get(1).and_then(|word| word.to_str()) {
            Some(RUN_COMMAND) => Some(Self::Run),
            Some(MERGE_COMMAND) => Some(Self::Merge),
            _ => None,
        }
    }
}

#[derive(Parser, Clone, Debug)]
#[command(
    name = "qbit-prism-load external",
    bin_name = "qbit-prism-load external",
    about = "Drive real Stratum sessions against an external frontend or load balancer and \
             write mergeable client-side stats",
    version
)]
pub struct ExternalArgs {
    /// The Stratum endpoint to drive, as `host:port`: one frontend's
    /// listener, or the load balancer in front of several. Every connection
    /// resolves it again.
    #[arg(long)]
    pub target: String,
    /// Confirm that `--target` is a rehearsal or test deployment configured
    /// for low-difficulty load, not a pool serving miners. Required.
    #[arg(long)]
    pub i_understand_external_target: bool,
    /// The payout address every session authorizes as, as
    /// `<address>.<worker>`. The target validates it against its own chain
    /// and credits every share it accepts to it.
    #[arg(long)]
    pub address: String,
    /// Worker names are `<prefix>-s<index>`. Give each client machine its
    /// own. Default: `pload-<run tag>`.
    #[arg(long)]
    pub worker_prefix: Option<String>,
    /// This process's name in the stats and a merge, e.g. the machine's.
    /// Default: the host name.
    #[arg(long)]
    pub label: Option<String>,
    /// Stratum sessions this process holds, one connection each.
    #[arg(long, default_value_t = 100)]
    pub sessions: usize,
    /// Offered shares per second across this process's sessions.
    #[arg(long, default_value_t = 50.0)]
    pub rate: f64,
    /// Seconds of load, counted from when the sessions hold work.
    #[arg(long, default_value_t = 60)]
    pub duration_seconds: u64,
    /// Ask the target for this share difficulty with `d=` in the Stratum
    /// password. The target clamps it to its own bounds; the client mines
    /// whatever it advertises.
    #[arg(long)]
    pub difficulty: Option<f64>,
    /// The highest advertised share difficulty this process mines; an offer
    /// on a job above it is counted and not mined. At most 2^-14.
    #[arg(long, default_value_t = DEFAULT_MAX_DIFFICULTY)]
    pub max_difficulty: f64,
    /// Seconds to wait for the sessions to hold work before the load starts,
    /// and for any one handshake, first job included.
    #[arg(long, default_value_t = 60)]
    pub work_timeout_seconds: u64,
    /// Seconds to wait after the load for answers still outstanding; what
    /// is still unanswered is recorded as no-response `run ended`.
    #[arg(long, default_value_t = 25)]
    pub drain_seconds: u64,
    /// The stats document.
    #[arg(long, default_value = "external-load.json")]
    pub out: PathBuf,
    /// Also write one JSON line per submit (share id, outcome, times) here,
    /// to reconcile offered, acknowledged, rejected and unanswered ids
    /// against the target's ledger.
    #[arg(long)]
    pub share_log: Option<PathBuf>,
    /// Seconds between progress lines on stderr; 0 prints none.
    #[arg(long, default_value_t = 10)]
    pub progress_seconds: u64,
}

impl ExternalArgs {
    /// EP-VALIDATION: the guard first, then every input against what its
    /// consumer does with it.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.i_understand_external_target,
            "qbit-prism-load external drives real Stratum load at {}: every session \
             authorizes as {}.<worker> and submits real proof-of-work shares that the \
             target credits to that address. Point it only at a rehearsal or test \
             deployment configured for low-difficulty load (crates/qbit-prism-load/README.md, \
             External-target mode), never at a pool serving miners, and pass {GUARD_FLAG} to \
             confirm",
            self.target,
            self.address
        );
        check_target(&self.target)?;
        ensure!(
            !self.address.is_empty()
                && self.address.len() <= 256
                && self
                    .address
                    .chars()
                    .all(|c| c.is_ascii_graphic() && c != '.'),
            "--address must be 1..256 printable characters with no '.', which the server reads \
             as the start of the worker name"
        );
        if let Some(prefix) = &self.worker_prefix {
            ensure!(
                !prefix.is_empty()
                    && prefix.len() <= 64
                    && prefix
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
                "--worker-prefix must be 1..64 of [A-Za-z0-9_-]"
            );
        }
        if let Some(label) = &self.label {
            ensure!(
                !label.is_empty() && label.len() <= 128 && !label.chars().any(char::is_control),
                "--label must be 1..128 characters with no control characters"
            );
        }
        ensure!(
            (1..=100_000).contains(&self.sessions),
            "--sessions must be 1..100000"
        );
        ensure!(
            self.rate.is_finite() && self.rate > 0.0 && self.rate <= MAX_RATE,
            "--rate must be finite and in (0, {MAX_RATE}]: a share costs thousands of hashes, and \
             a faster load needs more client machines, one process each"
        );
        ensure!(
            (1..=604_800).contains(&self.duration_seconds),
            "--duration-seconds must be 1..604800"
        );
        ensure!(
            self.max_difficulty.is_finite()
                && self.max_difficulty > 0.0
                && self.max_difficulty <= MAX_DIFFICULTY_LIMIT,
            "--max-difficulty must be positive and at most {MAX_DIFFICULTY_LIMIT} (2^-14): a \
             share search covers 2^22 nonces, which does not reliably hold a share at a higher \
             difficulty"
        );
        if let Some(difficulty) = self.difficulty {
            ensure!(
                difficulty.is_finite() && difficulty > 0.0 && difficulty <= self.max_difficulty,
                "--difficulty must be positive and at most --max-difficulty ({}): the client \
                 would not mine what it asked for",
                self.max_difficulty
            );
        }
        ensure!(
            (1..=3600).contains(&self.work_timeout_seconds),
            "--work-timeout-seconds must be 1..3600"
        );
        ensure!(
            self.drain_seconds <= 3600,
            "--drain-seconds must be 0..3600"
        );
        ensure!(
            self.progress_seconds <= 3600,
            "--progress-seconds must be 0..3600"
        );
        Ok(())
    }

    /// What each session sends as its Stratum password.
    pub fn password(&self) -> String {
        match self.difficulty {
            Some(difficulty) => format!("x,d={difficulty}"),
            None => "x".into(),
        }
    }
}

/// `host:port`, with a bracketed IPv6 host allowed.
fn check_target(target: &str) -> Result<()> {
    let (host, port) = target
        .rsplit_once(':')
        .with_context(|| format!("--target {target:?} must be host:port"))?;
    ensure!(
        !host.is_empty() && !host.chars().any(|c| c.is_whitespace() || c.is_control()),
        "--target {target:?} has no host"
    );
    let port: u16 = port
        .parse()
        .with_context(|| format!("--target {target:?} has no valid port"))?;
    ensure!(port > 0, "--target {target:?} has port 0");
    Ok(())
}

#[derive(Parser, Clone, Debug)]
#[command(
    name = "qbit-prism-load external-merge",
    bin_name = "qbit-prism-load external-merge",
    about = "Add several external-target stats documents (one per client machine) into one",
    version
)]
pub struct MergeArgs {
    /// Documents written by `qbit-prism-load external` (or earlier merges).
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,
    /// Where to write the merged document.
    #[arg(long, default_value = "external-load-merged.json")]
    pub out: PathBuf,
}

/// Ask the run to end early. The first request ends the load window and
/// drains; a second one skips what is left of the drain.
#[derive(Clone, Default)]
pub struct Shutdown {
    requests: Arc<AtomicUsize>,
    reason: Arc<Mutex<Option<String>>>,
    notify: Arc<tokio::sync::Notify>,
}

impl Shutdown {
    /// One nobody asks of.
    pub fn never() -> Self {
        Self::default()
    }

    /// One that SIGINT and SIGTERM ask of. Both handlers are installed
    /// before this returns, so from then on neither signal can end the
    /// process without its stats; one that cannot be installed is an error,
    /// not a silent default.
    pub fn on_signals() -> Result<Self> {
        use tokio::signal::unix::{signal, SignalKind};
        let mut interrupt =
            signal(SignalKind::interrupt()).context("installing the SIGINT handler")?;
        let mut terminate =
            signal(SignalKind::terminate()).context("installing the SIGTERM handler")?;
        let shutdown = Self::default();
        let asked = shutdown.clone();
        tokio::spawn(async move {
            loop {
                let name = tokio::select! {
                    _ = interrupt.recv() => "interrupted",
                    _ = terminate.recv() => "terminated",
                };
                asked.request(name);
            }
        });
        Ok(shutdown)
    }

    pub fn request(&self, reason: &str) {
        {
            let mut held = self.reason.lock().expect("shutdown reason lock");
            held.get_or_insert_with(|| reason.to_owned());
        }
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Why the first request was made, once one has been.
    pub fn requested(&self) -> Option<String> {
        if self.requests.load(Ordering::SeqCst) == 0 {
            return None;
        }
        self.reason.lock().expect("shutdown reason lock").clone()
    }

    fn forced(&self) -> bool {
        self.requests.load(Ordering::SeqCst) >= 2
    }

    /// Resolve once at least `count` requests have been made.
    async fn after(&self, count: usize) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.requests.load(Ordering::SeqCst) >= count {
                return;
            }
            notified.await;
        }
    }
}

/// How a run ended, with its document.
pub struct Outcome {
    pub exit_code: i32,
    pub document: Value,
}

/// Run `external` or `external-merge` from a full command line
/// (`argv[1]` is the subcommand) and return the exit code.
pub async fn main(command: Command, argv: Vec<OsString>) -> Result<i32> {
    let program = OsString::from(match command {
        Command::Run => "qbit-prism-load external",
        Command::Merge => "qbit-prism-load external-merge",
    });
    let rest = std::iter::once(program).chain(argv.into_iter().skip(2));
    match command {
        Command::Run => {
            let args = ExternalArgs::parse_from(rest);
            let outcome = run(&args, &Shutdown::on_signals()?).await?;
            Ok(outcome.exit_code)
        }
        Command::Merge => {
            let args = MergeArgs::parse_from(rest);
            let document = merge_files(&args.inputs)?;
            write_document(&args.out, &document)?;
            eprintln!(
                "qbit-prism-load external-merge: {} documents, {}; written to {}",
                args.inputs.len(),
                headline(&document),
                args.out.display()
            );
            Ok(EXIT_OK)
        }
    }
}

/// Read and add the documents at `paths`.
pub fn merge_files(paths: &[PathBuf]) -> Result<Value> {
    let mut inputs = Vec::with_capacity(paths.len());
    for path in paths {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let value: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        inputs.push((path.clone(), value));
    }
    stats::merge(&inputs)
}

/// Write `document` to `path` through a temporary file in the same
/// directory, so a reader never sees half a document.
pub fn write_document(path: &Path, document: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(document)?;
    bytes.push(b'\n');
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .with_context(|| format!("{} names no file", path.display()))?;
    let temporary = partial_path(directory, name);
    std::fs::write(&temporary, &bytes)
        .with_context(|| format!("writing {}", temporary.display()))?;
    std::fs::rename(&temporary, path)
        .with_context(|| format!("renaming {} to {}", temporary.display(), path.display()))?;
    Ok(())
}

/// Where [`write_document`] writes `name` before renaming it into place.
fn partial_path(directory: &Path, name: &std::ffi::OsStr) -> PathBuf {
    directory.join(format!(".{}.partial", name.to_string_lossy()))
}

/// The run's result in one line.
fn headline(document: &Value) -> String {
    let summary = &document["summary"];
    let shares = &summary["shares"];
    let millis = |value: &Value| {
        value
            .as_f64()
            .map_or_else(|| "n/a".to_owned(), |ms| format!("{ms:.1} ms"))
    };
    let rate = summary["rates"]["accepted_per_second"]
        .as_f64()
        .map_or_else(|| "n/a".to_owned(), |rate| format!("{rate:.1}/s"));
    format!(
        "{} sessions: {} accepted ({rate}), {} rejected, {} without an answer; ACK p50 {}, p99 \
         {}; {} disconnects, {} reconnects",
        summary["sessions"],
        shares["accepted"],
        shares["rejected"],
        shares["no_response"],
        millis(&summary["ack_latency"]["p50"]),
        millis(&summary["ack_latency"]["p99"]),
        summary["reconnects"]["disconnects"],
        summary["reconnects"]["completed"],
    )
}

fn host_name() -> Option<String> {
    let mut buffer = [0u8; 256];
    let result = unsafe { libc::gethostname(buffer.as_mut_ptr().cast(), buffer.len()) };
    if result != 0 {
        return None;
    }
    let end = buffer.iter().position(|b| *b == 0).unwrap_or(buffer.len());
    let name = String::from_utf8_lossy(&buffer[..end]).trim().to_owned();
    (!name.is_empty()).then_some(name)
}

/// Drive the load `args` describe and write its document to `args.out`.
pub async fn run(args: &ExternalArgs, shutdown: &Shutdown) -> Result<Outcome> {
    args.validate()?;
    // A name that does not resolve is refused here, before any session
    // spins against it.
    let resolved: Vec<String> = tokio::time::timeout(
        RESOLVE_TIMEOUT,
        tokio::net::lookup_host(args.target.as_str()),
    )
    .await
    .with_context(|| {
        format!(
            "--target {} did not resolve within {RESOLVE_TIMEOUT:?}",
            args.target
        )
    })?
    .with_context(|| format!("resolving --target {}", args.target))?
    .map(|address| address.to_string())
    .collect();
    ensure!(
        !resolved.is_empty(),
        "--target {} resolved to no address",
        args.target
    );
    let (_, descriptors) =
        crate::measure::raise_file_descriptor_limit(args.sessions as u64 * 2 + 256)?;
    ensure!(
        descriptors >= args.sessions as u64 + 64,
        "--sessions {} needs about {} file descriptors and the hard limit allows {descriptors}; \
         raise it (ulimit -Hn) or run fewer sessions per process",
        args.sessions,
        args.sessions + 64
    );
    let run_id = uuid::Uuid::new_v4();
    let run_tag = run_id.simple().to_string()[..8].to_owned();
    check_outputs(args, &run_tag)?;
    // Created before any load, so a log that cannot be written refuses the
    // run instead of losing the ids.
    let share_log = args
        .share_log
        .as_deref()
        .map(stats::ShareLog::create)
        .transpose()?;

    let worker_prefix = args
        .worker_prefix
        .clone()
        .unwrap_or_else(|| format!("pload-{run_tag}"));
    let host = host_name();
    let label = args
        .label
        .clone()
        .or_else(|| host.clone())
        .unwrap_or_else(|| run_tag.clone());
    let started_at = chrono::Utc::now();
    let anchor = stats::Anchor::now();
    eprintln!(
        "qbit-prism-load external: {label}: {} sessions at {} ({}) as {}.{worker_prefix}-s*, \
         {} offers/s for {} s",
        args.sessions,
        args.target,
        resolved.join(", "),
        args.address,
        args.rate,
        args.duration_seconds
    );

    let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
    let collector = Arc::new(Mutex::new(stats::Collector::new(
        anchor,
        share_log,
        args.sessions,
    )));
    // Which sessions hold work, kept by the collector from the sessions' own
    // events: the scheduler offers only to these.
    let holding = collector.lock().expect("collector lock").holding();
    let mut collector_task = {
        let collector = collector.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut behind = false;
            while let Some(event) = inbox.recv().await {
                let waiting = inbox.len();
                let mut collector = collector.lock().expect("collector lock");
                collector.apply(event);
                collector.event_backlog_max = collector.event_backlog_max.max(waiting);
                if waiting >= EVENT_BACKLOG_LIMIT && !behind {
                    behind = true;
                    shutdown.request(&format!(
                        "the stats fell {waiting} events behind the sessions, so this machine \
                         drives more than it can count; run fewer sessions or a lower rate per \
                         process"
                    ));
                }
            }
        })
    };
    let shared = Arc::new(SessionShared {
        phase: std::sync::RwLock::new(PHASE.to_owned()),
        events,
        // Every job is counted.
        record_notifies: AtomicBool::new(true),
        kill_fence: Arc::new(AtomicU64::new(0)),
        stopping: Arc::new(AtomicBool::new(false)),
    });
    // Kept apart from `shared`, which goes with the sessions.
    let stopping = shared.stopping.clone();
    let work_timeout = Duration::from_secs(args.work_timeout_seconds);
    let drain = Duration::from_secs(args.drain_seconds);
    let sessions: Vec<SessionHandle> = (0..args.sessions)
        .map(|index| {
            client::spawn_session(
                SessionConfig {
                    index,
                    username: format!("{}.{worker_prefix}-s{index:05}", args.address),
                    password: args.password(),
                    difficulty: DifficultySource::Advertised {
                        ceiling: args.max_difficulty,
                    },
                    version_rolling_mask: qbit_prism_server::codec::VERSION_ROLLING_MASK,
                    connect_timeout: CONNECT_TIMEOUT.min(work_timeout),
                    handshake_timeout: work_timeout,
                    // The mode never closes a connection on purpose before
                    // the stop, which comes after the drain.
                    quiesce_limit: drain,
                    // An outage is shortfall, never a burst on recovery.
                    drop_offers_held_while_disconnected: true,
                },
                0,
                args.target.clone(),
                shared.clone(),
                1,
            )
        })
        .collect();
    // The sessions hold the only other references: once they have all
    // stopped, the collector's queue closes.
    drop(shared);

    // --- the sessions take work ------------------------------------------
    let ready_deadline = Instant::now() + work_timeout;
    let holding_at_start = loop {
        let holding = collector
            .lock()
            .expect("collector lock")
            .sessions_holding_work();
        if holding == args.sessions
            || Instant::now() >= ready_deadline
            || shutdown.requested().is_some()
        {
            break holding;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            _ = shutdown.after(1) => {}
        }
    };
    let mut window = None;
    let mut minted = Offers::default();
    let ended = if let Some(reason) = shutdown.requested() {
        format!("interrupted: {reason} before the load started")
    } else if holding_at_start == 0 {
        format!(
            "blocked: no session held work within --work-timeout-seconds ({})",
            args.work_timeout_seconds
        )
    } else {
        if holding_at_start < args.sessions {
            eprintln!(
                "qbit-prism-load external: {label}: {holding_at_start} of {} sessions hold work \
                 after {} s; starting the load with them, the rest keep trying",
                args.sessions, args.work_timeout_seconds
            );
        }
        let (driven, interrupted) = drive(
            args, &label, &sessions, &holding, &collector, anchor, shutdown,
        )
        .await;
        window = Some(driven.window);
        minted = driven.offers;
        match interrupted {
            Some(reason) => format!("interrupted: {reason}"),
            None => "completed".to_owned(),
        }
    };

    // --- drain, then stop ------------------------------------------------
    // Only a session with a connection can still be answered; an offer held
    // by one without is waited for by nothing.
    let outstanding = |connected: bool| -> usize {
        sessions
            .iter()
            .zip(holding.iter())
            .filter(|(_, holds)| holds.load(Ordering::Relaxed) == connected)
            .map(|(session, _)| session.outstanding.load(Ordering::Relaxed))
            .sum()
    };
    let drain_deadline = Instant::now() + drain;
    while outstanding(true) > 0 && Instant::now() < drain_deadline && !shutdown.forced() {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            _ = shutdown.after(2) => {}
        }
    }
    let outstanding_at_drain_end = outstanding(true);
    let held_without_a_connection_at_drain_end = outstanding(false);
    // First the flag, which a search running or queued for the blocking pool
    // reads, then the stop each session reads between searches.
    stopping.store(true, Ordering::SeqCst);
    for session in &sessions {
        let _ = session.control.send(Control::Stop);
    }
    // One deadline for all of them: a session in the middle of a handshake
    // reads its stop only once the handshake ends, and is aborted instead.
    let stop_deadline = tokio::time::Instant::now() + STOP_TIMEOUT;
    let mut sessions_aborted_at_stop = 0usize;
    let (mut discarded_at_stop, mut unknown_at_abort) = (0u64, 0u64);
    for session in sessions {
        let mut task = session.task;
        let stopped = tokio::time::timeout_at(stop_deadline, &mut task)
            .await
            .is_ok();
        // What a session still counts once it is gone is what it took and
        // never reported. One that stopped had no connection when its stop
        // came, so its offer was never sent. One that had to be aborted may
        // have been sending it.
        let left = session.outstanding.load(Ordering::Relaxed) as u64;
        if stopped {
            discarded_at_stop += left;
        } else {
            task.abort();
            sessions_aborted_at_stop += 1;
            unknown_at_abort += left;
        }
    }
    // Every sender goes with its session, so the queue drains and ends. If it
    // has not within the limit, the stats are written with what arrived and
    // say so, rather than lost.
    let events_cut_off = match tokio::time::timeout(COLLECTOR_TIMEOUT, &mut collector_task).await {
        Ok(_) => None,
        Err(_) => {
            collector_task.abort();
            Some(format!(
                "events were still arriving {COLLECTOR_TIMEOUT:?} after the sessions stopped; \
                 the stats hold what had arrived by then"
            ))
        }
    };
    let (mut totals, share_log, event_backlog_max) = {
        let mut collector = collector
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            std::mem::take(&mut collector.totals),
            collector.take_share_log(),
            collector.event_backlog_max,
        )
    };
    // Its writer may still be writing what it was sent: waited for off the
    // runtime.
    let share_log = match share_log {
        Some(log) => Some(
            tokio::task::spawn_blocking(move || log.finish())
                .await
                .context("finishing the share log")?,
        ),
        None => None,
    };
    totals.offers_discarded += discarded_at_stop;
    totals.offers_unknown_at_abort += unknown_at_abort;
    totals.offers_minted = minted.minted;
    totals.offers_dispatched = minted.dispatched;
    totals.offers_shortfall = minted.shortfall;
    for (unix_second, (offered, dispatched)) in &minted.per_second {
        let second = totals.timeline.second(*unix_second);
        second.offered += offered;
        second.dispatched += dispatched;
        second.shortfall += offered - dispatched;
    }

    let exit_code = if ended.starts_with("interrupted") {
        EXIT_ABORTED
    } else if ended.starts_with("blocked") {
        EXIT_BLOCKED
    } else {
        EXIT_OK
    };
    let process = stats::Process {
        run_id: run_id.to_string(),
        label: label.clone(),
        host,
        harness_version: env!("CARGO_PKG_VERSION").to_owned(),
        target: args.target.clone(),
        target_resolved: resolved,
        address: args.address.clone(),
        worker_prefix,
        sessions: args.sessions,
        rate: args.rate,
        duration_seconds: args.duration_seconds,
        requested_difficulty: args.difficulty,
        max_difficulty: args.max_difficulty,
        work_timeout_seconds: args.work_timeout_seconds,
        drain_seconds: args.drain_seconds,
        started_at: started_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        ended_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        window,
        sessions_holding_work_at_start: holding_at_start,
        ended: ended.clone(),
        exit_code,
        offers_minted: totals.offers_minted,
        accepted: totals.accepted,
        rejected: totals.rejected,
        no_response: totals.no_response(),
        outstanding_at_drain_end,
        held_without_a_connection_at_drain_end,
        sessions_aborted_at_stop,
        events_cut_off,
        event_backlog_max,
        client_cpu_seconds: crate::measure::process_cpu_seconds(std::process::id()),
        available_parallelism: std::thread::available_parallelism().ok().map(usize::from),
        file_descriptor_limit: Some(descriptors),
        share_log,
    };
    let document = stats::document("process", &[process], &totals)?;
    write_document(&args.out, &document)?;
    eprintln!(
        "qbit-prism-load external: {label}: {ended}: {}; stats in {}",
        headline(&document),
        args.out.display()
    );
    Ok(Outcome {
        exit_code,
        document,
    })
}

/// `path` as its file name in its directory's canonical path, then each file
/// its final component's symlinks lead to, followed whether or not the last
/// one exists.
fn file_names(path: &Path, flag: &str) -> Result<Vec<PathBuf>> {
    let mut names = vec![in_real_directory(path, flag)?];
    // Linux follows at most 40 links in a path.
    for _ in 0..40 {
        let current = names.last().expect("one name").clone();
        let is_link = std::fs::symlink_metadata(&current)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false);
        if !is_link {
            return Ok(names);
        }
        let target = std::fs::read_link(&current)
            .with_context(|| format!("{flag} {}: reading {}", path.display(), current.display()))?;
        let next = match current.parent() {
            Some(directory) if target.is_relative() => directory.join(target),
            _ => target,
        };
        names.push(in_real_directory(&next, flag)?);
    }
    anyhow::bail!(
        "{flag} {}: too many levels of symbolic links",
        path.display()
    )
}

/// `path`'s file name in its directory's canonical path. The directory has
/// to exist: the file is created in it.
fn in_real_directory(path: &Path, flag: &str) -> Result<PathBuf> {
    let name = path
        .file_name()
        .with_context(|| format!("{flag} {} names no file", path.display()))?;
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let directory = std::fs::canonicalize(directory).with_context(|| {
        format!(
            "{flag} {}: {} is not a directory that exists",
            path.display(),
            directory.display()
        )
    })?;
    Ok(directory.join(name))
}

/// The output paths, checked before the load: `--out` is written only after
/// it, so a path it cannot be written to is refused now rather than found
/// after hours. The check writes a probe beside it and never touches the
/// file itself, which may hold an earlier run's stats. `--share-log` must be
/// another file, or the document would replace the ids it reports.
fn check_outputs(args: &ExternalArgs, run_tag: &str) -> Result<()> {
    ensure!(
        !args.out.is_dir(),
        "--out {} is a directory; name the file to write",
        args.out.display()
    );
    if let Some(log) = &args.share_log {
        // Every name each path can reach a file by: itself in its directory's
        // real path, and whatever its final symlinks lead to, dangling or
        // not. The log is created through its symlinks and the document is
        // renamed over its own name, so any name the two share is a log the
        // document would replace.
        let log_names = file_names(log, "--share-log")?;
        // The document is written to a partial file first and renamed: a log
        // there would be overwritten as surely.
        let out_name = args
            .out
            .file_name()
            .with_context(|| format!("--out {} names no file", args.out.display()))?;
        let out_directory = args
            .out
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut out_names = file_names(&args.out, "--out")?;
        out_names.extend(file_names(&partial_path(out_directory, out_name), "--out")?);
        // And the same file under another name, a hard link: by device and
        // inode, for the names that exist.
        let identity = |name: &PathBuf| {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(name)
                .ok()
                .map(|metadata| (metadata.dev(), metadata.ino()))
        };
        let out_files: Vec<(u64, u64)> = out_names.iter().filter_map(identity).collect();
        let same = out_names.iter().any(|name| log_names.contains(name))
            || log_names
                .iter()
                .filter_map(identity)
                .any(|file| out_files.contains(&file));
        ensure!(
            !same,
            "--share-log and --out name the same file, {}; the stats would replace the share ids",
            args.out.display()
        );
    }
    let directory = args
        .out
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = args
        .out
        .file_name()
        .with_context(|| format!("--out {} names no file", args.out.display()))?;
    let probe = directory.join(format!(".{}.{run_tag}.probe", name.to_string_lossy()));
    std::fs::write(&probe, b"").with_context(|| {
        format!(
            "--out {}: cannot write in {}",
            args.out.display(),
            directory.display()
        )
    })?;
    std::fs::remove_file(&probe).with_context(|| format!("removing {}", probe.display()))?;
    Ok(())
}

/// What the token bucket minted, by wall-clock second: (offered,
/// dispatched).
#[derive(Default)]
struct Offers {
    minted: u64,
    dispatched: u64,
    shortfall: u64,
    per_second: std::collections::BTreeMap<i64, (u64, u64)>,
}

struct Driven {
    window: stats::Window,
    offers: Offers,
}

/// Give one offer to the next session, in turn from `cursor`, that holds
/// work and has nothing outstanding. A session without a connection is not
/// offered anything: it would hold the offer until it reconnected, so an
/// outage would read as dispatched load rather than shortfall, and the held
/// offers would go out as a burst on recovery.
fn place(
    sessions: &[SessionHandle],
    holding: &[AtomicBool],
    cursor: &mut usize,
    phase: &Arc<str>,
) -> bool {
    for _ in 0..sessions.len() {
        let index = *cursor % sessions.len();
        *cursor = cursor.wrapping_add(1);
        if holding[index].load(Ordering::Relaxed) && sessions[index].try_offer(1, phase) {
            return true;
        }
    }
    false
}

/// The load window: the harness's open-loop token bucket, round-robin over
/// the sessions that hold work and have nothing outstanding. Returns the
/// window and, when a shutdown cut it short, why.
#[allow(clippy::too_many_arguments)]
async fn drive(
    args: &ExternalArgs,
    label: &str,
    sessions: &[SessionHandle],
    holding: &[AtomicBool],
    collector: &Arc<Mutex<stats::Collector>>,
    anchor: stats::Anchor,
    shutdown: &Shutdown,
) -> (Driven, Option<String>) {
    let started = Instant::now();
    let duration = Duration::from_secs(args.duration_seconds);
    let mut cursor = 0usize;
    let phase: Arc<str> = Arc::from(PHASE);
    let mut offers = Offers::default();
    let mut interrupted = None;
    let mut next_sample = started;
    let progress = Duration::from_secs(args.progress_seconds);
    let mut next_progress = started + progress;
    let mut last_progress = (started, 0u64, 0u64);
    let mut ticker = tokio::time::interval(Duration::from_millis(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.after(1) => {
                interrupted = shutdown.requested();
                break;
            }
            _ = ticker.tick() => {}
        }
        let now = Instant::now();
        let elapsed = now.duration_since(started);
        // The window's last tick still mints what is due at its end, so a
        // run makes every offer its rate and length promise, however late
        // that tick comes.
        let last = elapsed >= duration;
        // The clock decides how many offers were made; any no session can
        // take is shortfall, never a backlog.
        let want = (elapsed.min(duration).as_secs_f64() * args.rate).floor() as u64;
        let second = anchor.unix_second(now);
        // Once a scan finds no session free, the rest of this tick's offers
        // are shortfall without scanning again, so an outage costs one pass
        // over the sessions a tick rather than one per offer.
        let mut saturated = false;
        while offers.minted < want {
            offers.minted += 1;
            let placed = !saturated && place(sessions, holding, &mut cursor, &phase);
            saturated = !placed;
            let tally = offers.per_second.entry(second).or_insert((0, 0));
            tally.0 += 1;
            if placed {
                offers.dispatched += 1;
                tally.1 += 1;
            } else {
                offers.shortfall += 1;
            }
        }
        if last {
            break;
        }
        if now >= next_sample {
            collector
                .lock()
                .expect("collector lock")
                .sample_holding_work(second);
            next_sample += Duration::from_secs(1);
        }
        if !progress.is_zero() && now >= next_progress {
            let (accepted, rejected, no_response, holding) = {
                let collector = collector.lock().expect("collector lock");
                (
                    collector.totals.accepted,
                    collector.totals.rejected,
                    collector.totals.no_response(),
                    collector.sessions_holding_work(),
                )
            };
            let interval = now.duration_since(last_progress.0).as_secs_f64().max(1e-9);
            eprintln!(
                "qbit-prism-load external: {label}: {:.0} s: offered {} ({:.1}/s), accepted {} \
                 ({:.1}/s), rejected {rejected}, without an answer {no_response}, shortfall {}, \
                 sessions holding work {holding}/{}",
                elapsed.as_secs_f64(),
                offers.minted,
                (offers.minted - last_progress.1) as f64 / interval,
                accepted,
                (accepted - last_progress.2) as f64 / interval,
                offers.shortfall,
                sessions.len()
            );
            last_progress = (now, offers.minted, accepted);
            next_progress += progress;
        }
    }
    // A window that ran its length ends where its offers were minted to,
    // not where a late last tick noticed; one a shutdown cut short ends now.
    let ended = match interrupted {
        None => started + duration,
        Some(_) => Instant::now(),
    };
    let window = stats::Window {
        started_unix_ms: anchor.unix_ms(started),
        ended_unix_ms: anchor.unix_ms(ended),
        seconds: ended.duration_since(started).as_secs_f64(),
    };
    (Driven { window, offers }, interrupted)
}
