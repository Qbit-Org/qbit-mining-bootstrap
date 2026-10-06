//! The frontend faults (#554): SIGTERM of a frontend with a found block's
//! offer in flight, a rolling restart of every frontend, SIGKILL of the
//! frontend holding a found block the node has already accepted, and a
//! reconnect storm.
//!
//! Each is a state machine the fault driver polls from the scheduling loop,
//! as the drained restart and the mid-flight kill are: every `poll` returns
//! after bounded, synchronous work, and anything slow runs in a spawned task
//! (EP-STATE).

use super::{
    rpc_relay::{Arm, Seen},
    CandidateRow, FaultEnv, FaultTools, Spawned,
};
use crate::{client, kill::KillDriver, restart::ReadyWait};
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

/// How long a fault waits for a scheduled block's `submitblock` to reach the
/// relay before it records that the offer never came.
pub const OFFER_WAIT: Duration = Duration::from_secs(30);
/// How long the relay holds a drained frontend's `submitblock` before
/// forwarding it: long enough that the SIGTERM (sent the moment the call
/// arrives) lands inside the offer, and half the server's default
/// `PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS` (1 s), so the call is answered
/// rather than timed out into an unknown outcome.
pub const OFFER_DELAY: Duration = Duration::from_millis(500);
/// The server's default `PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS`: a frontend
/// still alive this long after it sent a `submitblock` has recorded the call
/// as an unknown outcome, so a kill after it no longer loses the answer.
pub const SUBMIT_RPC_TIMEOUT: Duration = Duration::from_secs(1);
/// The server joins its tasks for 30 s after a shutdown signal; this adds
/// the time to reap.
pub const EXIT_LIMIT: Duration = Duration::from_secs(40);
/// The log line #585 writes when a shutdown waits for an offer in flight.
pub const SHUTDOWN_WAITS_LINE: &str = "shutdown waits for a found block's offer in flight";
/// The ALERT it writes when the shutdown budget ran out first.
pub const SHUTDOWN_GAVE_UP_LINE: &str =
    "shutdown stopped waiting for a found block's offer in flight";
/// How long, once the drained frontend serves again, its block has to land
/// and every frontend to serve work at the payout revision the landing
/// committed (#686). It normally takes a few seconds.
pub const DRAIN_SETTLE_WAIT: Duration = Duration::from_secs(60);
/// How often the drain's settle reads the row, and a [`WorkCurrentWait`] the
/// revision and every `/healthz`.
const SETTLE_READ_INTERVAL: Duration = Duration::from_millis(250);
/// Each `/healthz` read's limit: the server answers from its last published
/// snapshot, so a slow answer means a frontend that is not serving.
const HEALTH_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Why the drain's settle stops, once its bound has passed.
fn past_settle_bound(deadline: Instant) -> Option<String> {
    (Instant::now() >= deadline).then(|| {
        format!(
            "not settled within {} s of the frontend serving again",
            DRAIN_SETTLE_WAIT.as_secs()
        )
    })
}

/// SIGTERM, the signal a service manager sends to stop a process.
pub fn terminate(pid: u32) {
    // SAFETY: a plain signal to a child this process spawned.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }
}

fn sessions_on(env: &FaultEnv<'_>, index: usize) -> Vec<usize> {
    env.sessions
        .iter()
        .filter(|session| session.frontend.load(Ordering::Relaxed) == index)
        .map(|session| session.index)
        .collect()
}

fn pause(env: &FaultEnv<'_>, sessions: &[usize]) {
    for &index in sessions {
        let session = &env.sessions[index];
        session.paused.store(true, Ordering::Relaxed);
        let _ = session.control.send(client::Control::Pause);
    }
}

fn retarget(env: &FaultEnv<'_>, sessions: &[usize], to: usize, reconnect: bool) {
    let address = env.frontends[to].stratum_address();
    for &index in sessions {
        let session = &env.sessions[index];
        let _ = session.control.send(client::Control::Retarget {
            frontend: to,
            address: address.clone(),
            reconnect,
        });
        session.paused.store(false, Ordering::Relaxed);
    }
}

/// A session on `preferred` holding work, else any session, to find the
/// fault's block.
fn block_finder(env: &FaultEnv<'_>, preferred: usize) -> Option<usize> {
    env.sessions
        .iter()
        .find(|session| {
            session.frontend.load(Ordering::Relaxed) == preferred
                && !session.paused.load(Ordering::Relaxed)
        })
        .or_else(|| env.sessions.first())
        .map(|session| session.index)
}

/// Arm every frontend's port and ask a session for a block, so whichever
/// frontend's submit loop claims it is the one the fault acts on.
fn arm_and_find(
    env: &FaultEnv<'_>,
    tools: &FaultTools,
    arm: Arm,
    preferred: usize,
) -> Vec<oneshot::Receiver<Seen>> {
    let receivers = (0..env.frontends.len())
        .map(|index| tools.relay.arm(index, arm))
        .collect();
    if let Some(session) = block_finder(env, preferred) {
        let _ = env.sessions[session]
            .control
            .send(client::Control::ScheduledBlock);
    }
    receivers
}

fn poll_seen(receivers: &mut [oneshot::Receiver<Seen>]) -> Option<Seen> {
    receivers
        .iter_mut()
        .find_map(|receiver| receiver.try_recv().ok())
}

fn log_count(text: &str, line: &str) -> usize {
    text.matches(line).count()
}

// --- SIGTERM drain with an offer in flight --------------------------------

enum DrainStage {
    Arming,
    AwaitingOffer {
        receivers: Vec<oneshot::Receiver<Seen>>,
        deadline: Instant,
    },
    AwaitingExit {
        index: usize,
        deadline: Instant,
    },
    ReadingRow {
        index: usize,
        row: Spawned<Result<Option<CandidateRow>>>,
    },
    Relaunching {
        index: usize,
        wait: ReadyWait,
    },
    Done,
}

pub struct SigtermDrain {
    stage: DrainStage,
    pub seen: Option<Seen>,
    /// The frontend that got the SIGTERM, with or without an offer seen.
    pub signalled: Option<usize>,
    pub signalled_at: Option<Instant>,
    pub exited_at: Option<Instant>,
    pub exit_status: Option<String>,
    pub exit_success: Option<bool>,
    pub forced_kill: bool,
    pub waits_logged: Option<bool>,
    pub gave_up_logged: Option<bool>,
    pub row_after_exit: Option<CandidateRow>,
    pub row_error: Option<String>,
    pub moved: Vec<usize>,
    pub ready_at: Option<Instant>,
    /// The settle (#686), from the frontend serving again: the block's row
    /// until it is `submitted`, then the payout revision and every
    /// frontend's `/healthz` until each serves work at it.
    settle_deadline: Option<Instant>,
    landing: Option<Spawned<Result<Option<CandidateRow>>>>,
    next_settle_read: Instant,
    /// Once the block has landed: every frontend's work at its revision.
    work: WorkCurrentWait,
    pub landed_at: Option<Instant>,
    pub current_at: Option<Instant>,
    /// Why the settle stopped without current work: the bound, or a row
    /// that went terminal without landing.
    pub unsettled: Option<String>,
    pub last_row: Option<CandidateRow>,
    /// The latest read of the row's failure; a later successful read clears
    /// it. The work's reads keep their own.
    pub settle_error: Option<String>,
    pub problems: Vec<String>,
    log_before: (usize, usize),
}

impl Default for SigtermDrain {
    fn default() -> Self {
        Self::new()
    }
}

impl SigtermDrain {
    pub fn new() -> Self {
        Self {
            stage: DrainStage::Arming,
            seen: None,
            signalled: None,
            signalled_at: None,
            exited_at: None,
            exit_status: None,
            exit_success: None,
            forced_kill: false,
            waits_logged: None,
            gave_up_logged: None,
            row_after_exit: None,
            row_error: None,
            moved: Vec::new(),
            ready_at: None,
            settle_deadline: None,
            landing: None,
            next_settle_read: Instant::now(),
            work: WorkCurrentWait::new(),
            landed_at: None,
            current_at: None,
            unsettled: None,
            last_row: None,
            settle_error: None,
            problems: Vec::new(),
            log_before: (0, 0),
        }
    }

    /// The frontend that is down on purpose right now.
    pub fn outage(&self) -> Option<usize> {
        match &self.stage {
            DrainStage::AwaitingExit { index, .. }
            | DrainStage::ReadingRow { index, .. }
            | DrainStage::Relaunching { index, .. } => Some(*index),
            _ => None,
        }
    }

    /// `Ok(true)` once the frontend is back and serving.
    pub fn poll(&mut self, env: &mut FaultEnv<'_>, tools: &FaultTools) -> Result<bool> {
        match &mut self.stage {
            DrainStage::Arming => {
                let receivers = arm_and_find(env, tools, Arm::DelayForward(OFFER_DELAY), 0);
                self.stage = DrainStage::AwaitingOffer {
                    receivers,
                    deadline: Instant::now() + OFFER_WAIT,
                };
                Ok(false)
            }
            DrainStage::AwaitingOffer {
                receivers,
                deadline,
            } => {
                let seen = poll_seen(receivers);
                let index = match &seen {
                    Some(seen) => seen.frontend,
                    None if Instant::now() < *deadline => return Ok(false),
                    None => {
                        // Drained anyway, so the rest of the fault still
                        // runs; the verdict says the offer never came.
                        self.problems.push(format!(
                            "no found block's submitblock reached the relay within {OFFER_WAIT:?}"
                        ));
                        0
                    }
                };
                tools.relay.disarm_all();
                self.seen = seen;
                let text = env.frontends[index].read_stderr();
                self.log_before = (
                    log_count(&text, SHUTDOWN_WAITS_LINE),
                    log_count(&text, SHUTDOWN_GAVE_UP_LINE),
                );
                let pid = env.frontends[index]
                    .pid()
                    .context("the drained frontend has no process")?;
                terminate(pid);
                self.signalled = Some(index);
                self.signalled_at = Some(Instant::now());
                // A miner whose pool is going away stops being offered
                // work; what it has in flight is the drain's to answer.
                self.moved = sessions_on(env, index);
                pause(env, &self.moved);
                self.stage = DrainStage::AwaitingExit {
                    index,
                    deadline: Instant::now() + EXIT_LIMIT,
                };
                Ok(false)
            }
            DrainStage::AwaitingExit { index, deadline } => {
                let index = *index;
                match env.frontends[index].exited() {
                    Some(status) => {
                        self.exited_at = Some(Instant::now());
                        self.exit_success = Some(status.success());
                        self.exit_status = Some(status.to_string());
                    }
                    None if Instant::now() < *deadline => return Ok(false),
                    None => {
                        self.forced_kill = true;
                        self.problems.push(format!(
                            "{} had not exited {EXIT_LIMIT:?} after SIGTERM; killed",
                            env.frontends[index].spec.instance_id
                        ));
                        env.frontends[index].kill();
                        self.exited_at = Some(Instant::now());
                    }
                }
                let text = env.frontends[index].read_stderr();
                self.waits_logged = Some(log_count(&text, SHUTDOWN_WAITS_LINE) > self.log_before.0);
                self.gave_up_logged =
                    Some(log_count(&text, SHUTDOWN_GAVE_UP_LINE) > self.log_before.1);
                // The row as the stopped process left it, before anything
                // else can touch it.
                let row = match &self.seen {
                    Some(seen) => Spawned::spawn(CandidateRow::read(
                        tools.side.clone(),
                        seen.block_hash.clone(),
                    )),
                    None => Spawned::ready(Ok(None)),
                };
                self.stage = DrainStage::ReadingRow { index, row };
                Ok(false)
            }
            DrainStage::ReadingRow { index, row } => {
                let Some(result) = row.poll() else {
                    return Ok(false);
                };
                match result {
                    Ok(row) => self.row_after_exit = row.clone(),
                    Err(error) => self.row_error = Some(format!("{error:#}")),
                }
                let index = *index;
                env.frontends[index].restart()?;
                if let Some(sampler) = env.samplers.get(index) {
                    sampler.set_pid(env.frontends[index].pid());
                }
                self.stage = DrainStage::Relaunching {
                    index,
                    wait: ReadyWait::start(tools.ready_limit, "its relaunch after SIGTERM"),
                };
                Ok(false)
            }
            DrainStage::Relaunching { index, wait } => {
                let index = *index;
                if !wait.poll(&mut env.frontends[index])? {
                    return Ok(false);
                }
                self.ready_at = Some(Instant::now());
                retarget(env, &self.moved, index, false);
                self.stage = DrainStage::Done;
                Ok(true)
            }
            DrainStage::Done => Ok(true),
        }
    }

    /// Wait, once the drained frontend serves again, for its block to land
    /// and for every frontend to serve work at the payout revision the
    /// landing committed (#686). The relaunched process normally lands the
    /// block it offered; the confirmation bumps the revision, and until work
    /// at the new revision is published every share on the old work is
    /// refused `stale-job`. A fault started before then measures the drain
    /// rather than itself: a settlement-lock holder even blocks that
    /// publication, so all the shares in the first half of its hold were
    /// refused. `true` once settled, or once it cannot settle: the row went
    /// terminal without landing, or [`DRAIN_SETTLE_WAIT`] passed. The verdict
    /// fails both.
    pub fn poll_settle(&mut self, env: &FaultEnv<'_>, tools: &FaultTools) -> bool {
        let Some(seen) = &self.seen else {
            // Nothing was offered, so nothing lands; the verdict fails that.
            return true;
        };
        if self.current_at.is_some() || self.unsettled.is_some() {
            return true;
        }
        let deadline = *self
            .settle_deadline
            .get_or_insert_with(|| Instant::now() + DRAIN_SETTLE_WAIT);
        if self.landed_at.is_none()
            && self
                .row_after_exit
                .as_ref()
                .is_some_and(CandidateRow::landed)
        {
            // The shutdown landed it before the process exited.
            self.landed_at = self.exited_at;
        }
        if let Some(read) = self.landing.as_mut() {
            let finished = read.poll().map(|result| match result {
                Ok(value) => Ok(value.clone()),
                Err(error) => Err(format!("{error:#}")),
            });
            match finished {
                // A read still in flight at the bound is abandoned with the
                // wait; one whose task was lost is replaced.
                None if !read.lost() => {
                    self.unsettled = past_settle_bound(deadline);
                    return self.unsettled.is_some();
                }
                None => self.settle_error = Some("a read of the candidate was lost".into()),
                Some(Err(error)) => {
                    self.settle_error = Some(format!("reading the candidate: {error}"));
                }
                Some(Ok(row)) => {
                    self.settle_error = None;
                    match &row {
                        Some(row) if row.landed() => self.landed_at = Some(Instant::now()),
                        Some(row) if super::backlog::terminal(&row.state) => {
                            self.unsettled =
                                Some(format!("the candidate went {} without landing", row.state));
                        }
                        Some(_) => {}
                        None => self.unsettled = Some("the candidate row is gone".into()),
                    }
                    self.last_row = row;
                }
            }
            self.landing = None;
            if self.unsettled.is_some() {
                return true;
            }
        }
        if self.landed_at.is_some() {
            if !self.work.poll(deadline, env, tools) {
                return false;
            }
            match self.work.current_at {
                Some(at) => self.current_at = Some(at),
                None => self.unsettled = past_settle_bound(deadline),
            }
            return true;
        }
        self.unsettled = past_settle_bound(deadline);
        if self.unsettled.is_some() {
            return true;
        }
        if Instant::now() >= self.next_settle_read {
            self.next_settle_read = Instant::now() + SETTLE_READ_INTERVAL;
            self.landing = Some(Spawned::spawn(CandidateRow::read(
                tools.side.clone(),
                seen.block_hash.clone(),
            )));
        }
        false
    }

    /// The settle's line in the verdict: when the block landed and every
    /// frontend served work at its revision, counted from the frontend
    /// serving again, or why it did not settle and what it last read.
    pub fn settle_detail(&self) -> String {
        if self.seen.is_none() {
            return "no offer was seen, so no landing to wait for".into();
        }
        let since_ready = |at: Instant| {
            self.ready_at.map_or(0.0, |ready| {
                at.saturating_duration_since(ready).as_secs_f64()
            })
        };
        let landed = match self.landed_at {
            _ if self
                .row_after_exit
                .as_ref()
                .is_some_and(CandidateRow::landed) =>
            {
                "before the process exited".to_owned()
            }
            Some(at) => format!("{:.1} s after the frontend served again", since_ready(at)),
            None => "no".to_owned(),
        };
        match self.current_at {
            Some(at) => format!(
                "landed {landed}; every frontend served work at its revision {:.1} s after the \
                 frontend served again (bound {} s)",
                since_ready(at),
                DRAIN_SETTLE_WAIT.as_secs()
            ),
            None => format!(
                "{}; landed: {landed}; candidate {:?}; last reading {:?}{}",
                self.unsettled
                    .as_deref()
                    .unwrap_or("the settle did not finish"),
                self.last_row,
                self.work.last,
                self.settle_error()
                    .map(|error| format!(" ({error})"))
                    .unwrap_or_default()
            ),
        }
    }

    /// The latest read's failure, of the row or, once the block landed, of
    /// the work.
    fn settle_error(&self) -> Option<&str> {
        self.settle_error.as_deref().or(self.work.error.as_deref())
    }

    pub fn evidence(&self, origin: Instant) -> Value {
        let at = |instant: Option<Instant>| {
            instant.map(|at| at.saturating_duration_since(origin).as_secs_f64())
        };
        json!({
            "offer_seen": self.seen.as_ref().map(|seen| json!({
                "frontend": seen.frontend,
                "block_hash": seen.block_hash,
            })),
            "relay_delay_seconds": OFFER_DELAY.as_secs_f64(),
            "sigterm_after_seconds": at(self.signalled_at),
            "exit_after_sigterm_seconds": match (self.signalled_at, self.exited_at) {
                (Some(signalled), Some(exited)) => Some(exited.saturating_duration_since(signalled).as_secs_f64()),
                _ => None,
            },
            "exit_status": self.exit_status,
            "forced_kill": self.forced_kill,
            "shutdown_waited_for_the_offer_logged": self.waits_logged,
            "shutdown_gave_up_logged": self.gave_up_logged,
            "candidate_after_exit": self.row_after_exit,
            "candidate_read_error": self.row_error,
            "sessions_paused": self.moved.len(),
            "serving_again_after_seconds": at(self.ready_at),
            "block_landed_after_seconds": at(self.landed_at),
            "work_current_after_seconds": at(self.current_at),
            "settle_wait_seconds": DRAIN_SETTLE_WAIT.as_secs(),
            "candidate_at_settle": self.last_row,
            "work_at_settle": self.work.last,
            "settle_unsettled": self.unsettled,
            "settle_error": self.settle_error(),
            "problems": self.problems,
        })
    }
}

/// One reading of the work every frontend serves, as #640's gate takes it:
/// the cluster's payout revision and the settlements under way, then each
/// frontend's published `/healthz`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WorkReading {
    pub payout_revision: i64,
    /// Backends holding or waiting for `SETTLEMENT_LOCK`: a rebuild, a job
    /// build or a landing the server is still settling.
    pub settlements_in_progress: i64,
    /// One per frontend, in order; `None` where `/healthz` gave no JSON.
    pub frontends: Vec<Option<FrontendWork>>,
}

/// What one frontend's `/healthz` says about its work.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FrontendWork {
    /// The server's readiness: its work is on the tip it observed and at
    /// the payout revision it last read.
    pub ok: bool,
    pub payout_state_generation: Option<i64>,
    pub authorized_missing_current_work: Option<u64>,
}

impl WorkReading {
    /// The revision first, then every frontend's `/healthz` at once, so a
    /// bump between the two shows as a frontend behind it and is read again.
    async fn read(pool: PgPool, client: reqwest::Client, health_urls: Vec<String>) -> Result<Self> {
        let (payout_revision, settlements_in_progress): (i64, i64) = sqlx::query_as(
            "SELECT payout_revision, \
               (SELECT count(*) FROM pg_locks l WHERE l.locktype = 'advisory' \
                  AND l.database = (SELECT oid FROM pg_database \
                                    WHERE datname = current_database()) \
                  AND l.classid = $1::bigint::oid AND l.objid = $2::bigint::oid \
                  AND l.objsubid = 1) \
             FROM qbit_prism_cluster WHERE singleton",
        )
        .bind(crate::measure::PRISM_LOCK_CLASSID)
        .bind(crate::measure::SETTLEMENT_LOCK_OBJID)
        .fetch_one(&pool)
        .await?;
        let reads: Vec<_> = health_urls
            .into_iter()
            .map(|url| {
                let client = client.clone();
                tokio::spawn(async move {
                    // A frontend that is not ready answers 503 with the same
                    // body.
                    let response = client.get(&url).send().await.ok()?;
                    response.json::<Value>().await.ok()
                })
            })
            .collect();
        let mut frontends = Vec::with_capacity(reads.len());
        for read in reads {
            let health = read.await.ok().flatten();
            frontends.push(health.map(|health| FrontendWork {
                ok: health["ok"] == true,
                payout_state_generation: health["payout_state_generation"].as_i64(),
                authorized_missing_current_work:
                    health["stratum"]["authorized_missing_current_work"].as_u64(),
            }));
        }
        Ok(Self {
            payout_revision,
            settlements_in_progress,
            frontends,
        })
    }

    /// Every frontend is ready, serves work at the revision read before its
    /// `/healthz`, and has delivered that work to every authorized session.
    pub fn current(&self) -> bool {
        !self.frontends.is_empty()
            && self.frontends.iter().all(|work| {
                work.as_ref().is_some_and(|work| {
                    work.ok
                        && work.payout_state_generation == Some(self.payout_revision)
                        && work.authorized_missing_current_work == Some(0)
                })
            })
    }

    /// Current, with no settlement under way: a lock taken now is not queued
    /// behind a rebuild, a job build or a landing the server is still
    /// settling.
    pub fn quiet(&self) -> bool {
        self.current() && self.settlements_in_progress == 0
    }

    /// Every frontend ready and publishing work at `revision`, as a reading
    /// taken under `SETTLEMENT_LOCK` must show for the lock to keep retained
    /// work current. A session still waiting for its first job has nothing
    /// to submit, so it is not counted.
    pub fn published_at(&self, revision: i64) -> bool {
        !self.frontends.is_empty()
            && self.frontends.iter().all(|work| {
                work.as_ref()
                    .is_some_and(|work| work.ok && work.payout_state_generation == Some(revision))
            })
    }
}

/// A spawned [`WorkReading`] of every frontend.
fn reading(
    client: reqwest::Client,
    env: &FaultEnv<'_>,
    tools: &FaultTools,
) -> Spawned<Result<WorkReading>> {
    Spawned::spawn(WorkReading::read(
        tools.side.clone(),
        client,
        env.frontends
            .iter()
            .map(|frontend| frontend.health_url())
            .collect(),
    ))
}

/// A wait, up to a deadline, for every frontend to serve current work, read
/// as [`WorkReading`] every [`SETTLE_READ_INTERVAL`]. The drain's settle
/// waits for it once its block has landed (#686), and the settlement-lock
/// fault for current work on a quiet cluster before it takes the lock
/// (#692).
pub struct WorkCurrentWait {
    /// Wait for [`WorkReading::quiet`], not only [`WorkReading::current`].
    quiet: bool,
    read: Option<Spawned<Result<WorkReading>>>,
    next_read: Instant,
    client: Option<reqwest::Client>,
    /// When a reading showed every frontend's work current, before the
    /// deadline.
    pub current_at: Option<Instant>,
    /// The deadline passed first.
    pub timed_out: bool,
    pub last: Option<WorkReading>,
    /// The latest read's failure; a later successful read clears it.
    pub error: Option<String>,
}

impl Default for WorkCurrentWait {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkCurrentWait {
    pub fn new() -> Self {
        Self {
            quiet: false,
            read: None,
            next_read: Instant::now(),
            client: None,
            current_at: None,
            timed_out: false,
            last: None,
            error: None,
        }
    }

    /// A wait for current work on a quiet cluster: a lock queued behind a
    /// settlement under way would start its hold in the middle of it.
    pub fn quiet() -> Self {
        Self {
            quiet: true,
            ..Self::new()
        }
    }

    /// `true` once every frontend serves current work, or once `deadline`
    /// passed first.
    pub fn poll(&mut self, deadline: Instant, env: &FaultEnv<'_>, tools: &FaultTools) -> bool {
        self.poll_with(deadline, |client| reading(client, env, tools))
    }

    /// One more reading, outside the wait: the settlement-lock fault's
    /// reading under the lock.
    pub fn read_once(
        &mut self,
        env: &FaultEnv<'_>,
        tools: &FaultTools,
    ) -> Spawned<Result<WorkReading>> {
        reading(self.client(), env, tools)
    }

    fn client(&mut self) -> reqwest::Client {
        self.client
            .get_or_insert_with(|| {
                reqwest::Client::builder()
                    .timeout(HEALTH_READ_TIMEOUT)
                    .build()
                    .unwrap_or_default()
            })
            .clone()
    }

    /// [`Self::poll`], with each read started by `read`.
    fn poll_with(
        &mut self,
        deadline: Instant,
        read: impl FnOnce(reqwest::Client) -> Spawned<Result<WorkReading>>,
    ) -> bool {
        if self.current_at.is_some() || self.timed_out {
            return true;
        }
        if let Some(pending) = self.read.as_mut() {
            let finished = pending.poll().map(|result| match result {
                Ok(value) => Ok(value.clone()),
                Err(error) => Err(format!("{error:#}")),
            });
            match finished {
                // A read still in flight at the bound is abandoned with the
                // wait; one whose task was lost is replaced.
                None if !pending.lost() => {
                    if Instant::now() < deadline {
                        return false;
                    }
                    pending.abort();
                    self.read = None;
                    self.timed_out = true;
                    return true;
                }
                None => self.error = Some("a read of the frontends' work was lost".into()),
                Some(Err(error)) => {
                    self.error = Some(format!("reading the frontends' work: {error}"));
                }
                Some(Ok(reading)) => {
                    self.error = None;
                    let done = if self.quiet {
                        reading.quiet()
                    } else {
                        reading.current()
                    };
                    // A reading that finished past the bound does not count.
                    if done && Instant::now() < deadline {
                        self.current_at = Some(Instant::now());
                    }
                    self.last = Some(reading);
                }
            }
            self.read = None;
            if self.current_at.is_some() {
                return true;
            }
        }
        if Instant::now() >= deadline {
            self.timed_out = true;
            return true;
        }
        if Instant::now() >= self.next_read {
            self.next_read = Instant::now() + SETTLE_READ_INTERVAL;
            let client = self.client();
            self.read = Some(read(client));
        }
        false
    }

    /// Cancel a read still in flight, for a caller that stopped waiting.
    pub fn abort(&mut self) {
        if let Some(mut read) = self.read.take() {
            read.abort();
        }
    }

    /// What the wait last read, and the latest read's failure.
    pub fn last_read(&self) -> String {
        format!(
            "last reading {:?}{}",
            self.last,
            self.error
                .as_deref()
                .map(|error| format!(" ({error})"))
                .unwrap_or_default()
        )
    }
}

// --- rolling restart --------------------------------------------------------

enum RollingStage {
    Next,
    AwaitingExit { index: usize, deadline: Instant },
    Relaunching { index: usize, wait: ReadyWait },
    Done,
}

/// One frontend's turn in the rolling restart.
#[derive(Clone, Debug)]
pub struct Turn {
    pub index: usize,
    pub moved_to: usize,
    pub sessions_moved: Vec<usize>,
    pub signalled_at: Instant,
    pub exited_at: Option<Instant>,
    pub exit_status: Option<String>,
    pub exit_success: Option<bool>,
    pub forced_kill: bool,
    pub ready_at: Option<Instant>,
}

pub struct RollingRestart {
    stage: RollingStage,
    next: usize,
    /// Each session's frontend before the restart, so the topology is put
    /// back once every frontend has been restarted.
    home: Vec<usize>,
    pub turns: Vec<Turn>,
    pub rebalanced_at: Option<Instant>,
}

impl Default for RollingRestart {
    fn default() -> Self {
        Self::new()
    }
}

impl RollingRestart {
    pub fn new() -> Self {
        Self {
            stage: RollingStage::Next,
            next: 0,
            home: Vec::new(),
            turns: Vec::new(),
            rebalanced_at: None,
        }
    }

    pub fn outage(&self) -> Option<usize> {
        match &self.stage {
            RollingStage::AwaitingExit { index, .. } | RollingStage::Relaunching { index, .. } => {
                Some(*index)
            }
            _ => None,
        }
    }

    pub fn poll(&mut self, env: &mut FaultEnv<'_>, tools: &FaultTools) -> Result<bool> {
        loop {
            match &mut self.stage {
                RollingStage::Next => {
                    if self.home.is_empty() {
                        self.home = env
                            .sessions
                            .iter()
                            .map(|session| session.frontend.load(Ordering::Relaxed))
                            .collect();
                    }
                    if self.next >= env.frontends.len() {
                        // Every frontend has had its turn: put each session
                        // back on its own frontend, as a balancer would
                        // spread returning miners.
                        for (session, &home) in self.home.iter().enumerate() {
                            if env.sessions[session].frontend.load(Ordering::Relaxed) != home {
                                retarget(env, &[session], home, true);
                            }
                        }
                        self.rebalanced_at = Some(Instant::now());
                        self.stage = RollingStage::Done;
                        return Ok(true);
                    }
                    let index = self.next;
                    self.next += 1;
                    let moved_to = (index + 1) % env.frontends.len();
                    let pid = env.frontends[index]
                        .pid()
                        .context("a frontend in the rolling restart has no process")?;
                    terminate(pid);
                    let signalled_at = Instant::now();
                    // The balancer sends this frontend's miners to the one
                    // still serving; each finishes what it has in flight
                    // first (the retarget quiesces before it reconnects).
                    let sessions = sessions_on(env, index);
                    retarget(env, &sessions, moved_to, true);
                    self.turns.push(Turn {
                        index,
                        moved_to,
                        sessions_moved: sessions,
                        signalled_at,
                        exited_at: None,
                        exit_status: None,
                        exit_success: None,
                        forced_kill: false,
                        ready_at: None,
                    });
                    self.stage = RollingStage::AwaitingExit {
                        index,
                        deadline: signalled_at + EXIT_LIMIT,
                    };
                    return Ok(false);
                }
                RollingStage::AwaitingExit { index, deadline } => {
                    let index = *index;
                    let turn = self.turns.last_mut().expect("a turn is in progress");
                    match env.frontends[index].exited() {
                        Some(status) => {
                            turn.exited_at = Some(Instant::now());
                            turn.exit_success = Some(status.success());
                            turn.exit_status = Some(status.to_string());
                        }
                        None if Instant::now() < *deadline => return Ok(false),
                        None => {
                            turn.forced_kill = true;
                            env.frontends[index].kill();
                            turn.exited_at = Some(Instant::now());
                        }
                    }
                    env.frontends[index].restart()?;
                    if let Some(sampler) = env.samplers.get(index) {
                        sampler.set_pid(env.frontends[index].pid());
                    }
                    self.stage = RollingStage::Relaunching {
                        index,
                        wait: ReadyWait::start(tools.ready_limit, "its rolling restart"),
                    };
                    return Ok(false);
                }
                RollingStage::Relaunching { index, wait } => {
                    let index = *index;
                    if !wait.poll(&mut env.frontends[index])? {
                        return Ok(false);
                    }
                    self.turns
                        .last_mut()
                        .expect("a turn is in progress")
                        .ready_at = Some(Instant::now());
                    self.stage = RollingStage::Next;
                    continue;
                }
                RollingStage::Done => return Ok(true),
            }
        }
    }

    /// The windows each frontend was down on purpose.
    pub fn outages(&self, end: Instant) -> Vec<(usize, Instant, Instant)> {
        self.turns
            .iter()
            .map(|turn| (turn.index, turn.signalled_at, turn.ready_at.unwrap_or(end)))
            .collect()
    }

    pub fn evidence(&self, origin: Instant) -> Value {
        let at = |instant: Option<Instant>| {
            instant.map(|at| at.saturating_duration_since(origin).as_secs_f64())
        };
        json!({
            "turns": self.turns.iter().map(|turn| json!({
                "frontend": turn.index,
                "sessions_moved_to": turn.moved_to,
                "sessions_moved": turn.sessions_moved.len(),
                "sigterm_after_seconds": at(Some(turn.signalled_at)),
                "exit_after_sigterm_seconds": turn.exited_at
                    .map(|exited| exited.saturating_duration_since(turn.signalled_at).as_secs_f64()),
                "exit_status": turn.exit_status,
                "forced_kill": turn.forced_kill,
                "serving_again_after_sigterm_seconds": turn.ready_at
                    .map(|ready| ready.saturating_duration_since(turn.signalled_at).as_secs_f64()),
            })).collect::<Vec<_>>(),
            "rebalanced_after_seconds": at(self.rebalanced_at),
        })
    }
}

// --- SIGKILL of the holder of an accepted block ----------------------------

enum KillStage {
    Arming,
    AwaitingOffer {
        receivers: Vec<oneshot::Receiver<Seen>>,
        deadline: Instant,
    },
    AwaitingNode {
        deadline: Instant,
    },
    Killing(Box<KillDriver>),
    Done,
}

pub struct FrontendSigkill {
    stage: KillStage,
    pub seen: Option<Seen>,
    pub node_answered_at: Option<Instant>,
    pub killed_index: Option<usize>,
    pub kill_started_at: Option<Instant>,
    pub relaunched_at: Option<Instant>,
    pub outstanding_at_kill: Option<usize>,
    pub indeterminate: Vec<client::SubmitRecord>,
    /// The landing wait: polled after the relaunch until the block's row
    /// settles or the lease wait passes.
    landing: Option<Spawned<Result<Option<CandidateRow>>>>,
    next_landing_read: Instant,
    pub landed: Option<(Instant, CandidateRow)>,
    pub last_row: Option<CandidateRow>,
    pub landing_deadline: Option<Instant>,
    pub problems: Vec<String>,
}

impl Default for FrontendSigkill {
    fn default() -> Self {
        Self::new()
    }
}

impl FrontendSigkill {
    pub fn new() -> Self {
        Self {
            stage: KillStage::Arming,
            seen: None,
            node_answered_at: None,
            killed_index: None,
            kill_started_at: None,
            relaunched_at: None,
            outstanding_at_kill: None,
            indeterminate: Vec::new(),
            landing: None,
            next_landing_read: Instant::now(),
            landed: None,
            last_row: None,
            landing_deadline: None,
            problems: Vec::new(),
        }
    }

    pub fn outage(&self) -> Option<usize> {
        match &self.stage {
            KillStage::Killing(driver) => Some(driver.index()),
            _ => None,
        }
    }

    /// `Ok(true)` once the killed frontend is back and its census is done.
    pub fn poll(&mut self, env: &mut FaultEnv<'_>, tools: &FaultTools) -> Result<bool> {
        loop {
            match &mut self.stage {
                KillStage::Arming => {
                    let preferred = env.frontends.len().saturating_sub(1).min(1);
                    let receivers = arm_and_find(env, tools, Arm::WithholdReply, preferred);
                    self.stage = KillStage::AwaitingOffer {
                        receivers,
                        deadline: Instant::now() + OFFER_WAIT,
                    };
                    return Ok(false);
                }
                KillStage::AwaitingOffer {
                    receivers,
                    deadline,
                } => {
                    let seen = poll_seen(receivers);
                    if seen.is_none() && Instant::now() < *deadline {
                        return Ok(false);
                    }
                    tools.relay.disarm_all();
                    if seen.is_none() {
                        self.problems.push(format!(
                            "no found block's submitblock reached the relay within {OFFER_WAIT:?}; \
                             the kill ran without a block in flight"
                        ));
                    }
                    self.seen = seen;
                    self.stage = KillStage::AwaitingNode {
                        deadline: Instant::now() + Duration::from_secs(10),
                    };
                    continue;
                }
                KillStage::AwaitingNode { deadline } => {
                    // Kill only once the node has the block: the reply is
                    // what is lost, not the block.
                    if let Some(seen) = &self.seen {
                        let answered = tools.relay.submits().iter().any(|submit| {
                            submit.block_hash == seen.block_hash && submit.result.is_some()
                        });
                        if !answered && Instant::now() < *deadline {
                            return Ok(false);
                        }
                        if answered {
                            self.node_answered_at = Some(Instant::now());
                        } else {
                            self.problems.push(
                                "the node did not answer the withheld submitblock within 10 s"
                                    .into(),
                            );
                        }
                    }
                    let index = self
                        .seen
                        .as_ref()
                        .map(|seen| seen.frontend)
                        .unwrap_or_else(|| env.frontends.len().saturating_sub(1).min(1));
                    self.killed_index = Some(index);
                    self.kill_started_at = Some(Instant::now());
                    // At once, not after the mid-flight kill's wait for
                    // outstanding shares: the frontend has to die inside its
                    // own submitblock deadline, or it records the call as an
                    // unknown outcome and the answer is not what is lost.
                    self.stage = KillStage::Killing(Box::new(
                        KillDriver::start(
                            index,
                            tools.ready_limit,
                            tools.drain_limit,
                            env.kill_fence.clone(),
                        )
                        .without_work_wait(),
                    ));
                    continue;
                }
                KillStage::Killing(driver) => {
                    match driver.poll(env.sessions, env.frontends, env.samplers, env.collected)? {
                        None => return Ok(false),
                        Some(record) => {
                            self.outstanding_at_kill = Some(record.outstanding_at_kill);
                            self.indeterminate = record.indeterminate;
                            self.relaunched_at = Some(Instant::now());
                            self.landing_deadline = Some(
                                Instant::now() + Duration::from_secs(tools.lease_wait_seconds),
                            );
                            self.stage = KillStage::Done;
                            return Ok(true);
                        }
                    }
                }
                KillStage::Done => return Ok(true),
            }
        }
    }

    /// Wait for the killed holder's block to land through the candidate
    /// lease. `Ok(true)` once it did, or once the lease wait has passed.
    pub fn poll_landing(&mut self, tools: &FaultTools) -> Result<bool> {
        let Some(seen) = &self.seen else {
            return Ok(true);
        };
        if self.landed.is_some() {
            return Ok(true);
        }
        if let Some(read) = self.landing.as_mut() {
            let Some(result) = read.poll() else {
                return Ok(false);
            };
            match result {
                Ok(Some(row)) => {
                    self.last_row = Some(row.clone());
                    if row.landed() {
                        self.landed = Some((Instant::now(), row.clone()));
                        return Ok(true);
                    }
                }
                Ok(None) => self.last_row = None,
                Err(error) => self
                    .problems
                    .push(format!("reading the candidate: {error:#}")),
            }
            self.landing = None;
        }
        if self
            .landing_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Ok(true);
        }
        if Instant::now() >= self.next_landing_read {
            self.next_landing_read = Instant::now() + Duration::from_millis(500);
            self.landing = Some(Spawned::spawn(CandidateRow::read(
                tools.side.clone(),
                seen.block_hash.clone(),
            )));
        }
        Ok(false)
    }

    pub fn evidence(&self, origin: Instant, lease_wait_seconds: u64) -> Value {
        let at = |instant: Option<Instant>| {
            instant.map(|at| at.saturating_duration_since(origin).as_secs_f64())
        };
        let lease_recovery = match (self.kill_started_at, &self.landed) {
            (Some(killed), Some((landed, _))) => {
                Some(landed.saturating_duration_since(killed).as_secs_f64())
            }
            _ => None,
        };
        json!({
            "offer_seen": self.seen.as_ref().map(|seen| json!({
                "frontend": seen.frontend,
                "block_hash": seen.block_hash,
            })),
            "node_had_the_block_before_the_kill": self.node_answered_at.is_some(),
            "killed_frontend": self.killed_index,
            "kill_after_seconds": at(self.kill_started_at),
            "submits_outstanding_at_kill": self.outstanding_at_kill,
            "indeterminate_shares": self.indeterminate.len(),
            "relaunched_after_seconds": at(self.relaunched_at),
            "landed": self.landed.as_ref().map(|(_, row)| row),
            "last_candidate_row": self.last_row,
            "lease_recovery_seconds": lease_recovery,
            "lease_wait_seconds": lease_wait_seconds,
            "lease_recovery_note": "Measured, not gated. A holder that dies after the node \
                accepted its block leaves the claim to the candidate lease: no frontend may \
                take the row until the lease expires, then recovery reconciles it against the \
                chain. That wait is #529's option-A gap (a takeover keyed on the holder's \
                death), which #585 left open.",
            "problems": self.problems,
        })
    }
}

// --- reconnect storm ---------------------------------------------------------

pub struct ReconnectStorm {
    pub stormed: Vec<usize>,
    pub departed_at: Option<Instant>,
    /// Each stormed session's return delay.
    pub returns: Vec<Duration>,
}

/// The longest a stormed session stays away.
pub const STORM_RETURN: Duration = Duration::from_secs(5);
/// How long after its return a stormed session may take to hold work again.
pub const STORM_WORK_BUDGET: Duration = Duration::from_secs(10);

impl Default for ReconnectStorm {
    fn default() -> Self {
        Self::new()
    }
}

impl ReconnectStorm {
    pub fn new() -> Self {
        Self {
            stormed: Vec::new(),
            departed_at: None,
            returns: Vec::new(),
        }
    }

    /// Drop `fraction` of the sessions at once, each returning after a
    /// seeded delay of up to [`STORM_RETURN`].
    pub fn start(&mut self, env: &FaultEnv<'_>, fraction: f64, seed: u64, ordinal: usize) {
        let mut rng = crate::realism::Rng::new(seed, &format!("fault-storm:{ordinal}"));
        let count =
            ((env.sessions.len() as f64 * fraction).round() as usize).clamp(1, env.sessions.len());
        let mut order: Vec<usize> = (0..env.sessions.len()).collect();
        for index in (1..order.len()).rev() {
            let other = (rng.next_f64() * (index + 1) as f64) as usize;
            order.swap(index, other.min(index));
        }
        order.truncate(count);
        order.sort_unstable();
        self.departed_at = Some(Instant::now());
        for &session in &order {
            let delay = STORM_RETURN.mul_f64(rng.next_f64());
            self.returns.push(delay);
            let _ = env.sessions[session].control.send(client::Control::Depart {
                reason: "fault reconnect storm".into(),
                reconnect_after: Some(delay),
            });
        }
        self.stormed = order;
    }

    /// Every stormed session holds work again, read from the connections
    /// the collector has recorded since the storm.
    pub fn all_returned(&self, env: &FaultEnv<'_>) -> bool {
        let Some(departed) = self.departed_at else {
            return false;
        };
        let collected = env.collected.lock().expect("collector lock");
        self.stormed.iter().all(|session| {
            collected
                .opened
                .iter()
                .any(|opened| opened.session == *session && opened.ready > departed)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn work(ok: bool, generation: Option<i64>, missing: Option<u64>) -> Option<FrontendWork> {
        Some(FrontendWork {
            ok,
            payout_state_generation: generation,
            authorized_missing_current_work: missing,
        })
    }

    #[test]
    fn work_is_current_once_every_frontend_serves_the_revision_to_every_session() {
        let reading = |frontends| WorkReading {
            payout_revision: 7,
            settlements_in_progress: 0,
            frontends,
        };
        assert!(reading(vec![
            work(true, Some(7), Some(0)),
            work(true, Some(7), Some(0))
        ])
        .current());
        // The landing's bump is committed, but one frontend still serves the
        // work it published before it.
        assert!(!reading(vec![
            work(true, Some(7), Some(0)),
            work(true, Some(6), Some(0))
        ])
        .current());
        // Published, but not yet delivered to every authorized session.
        assert!(!reading(vec![work(true, Some(7), Some(3))]).current());
        // Not ready: work on another tip, or a stale snapshot.
        assert!(!reading(vec![work(false, Some(7), Some(0))]).current());
        // A `/healthz` without JSON, or without the fields, proves nothing.
        assert!(!reading(vec![None]).current());
        assert!(!reading(vec![work(true, None, Some(0))]).current());
        assert!(!reading(vec![work(true, Some(7), None)]).current());
        assert!(!reading(Vec::new()).current());
    }

    fn at_revision(generation: i64) -> WorkReading {
        WorkReading {
            payout_revision: 7,
            settlements_in_progress: 0,
            frontends: vec![work(true, Some(generation), Some(0))],
        }
    }

    /// A read that has already finished with `reading`.
    fn read(reading: WorkReading) -> impl FnOnce(reqwest::Client) -> Spawned<Result<WorkReading>> {
        move |_| Spawned::ready(Ok(reading))
    }

    fn no_read(_: reqwest::Client) -> Spawned<Result<WorkReading>> {
        panic!("the wait started a read it should not have")
    }

    fn hold_off_reads(wait: &mut WorkCurrentWait) {
        wait.next_read = Instant::now() + Duration::from_secs(3600);
    }

    #[test]
    fn the_wait_reads_until_the_work_is_current_then_stops() {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut wait = WorkCurrentWait::new();
        // The first poll starts a read; the next takes it. A frontend still
        // behind the revision is read again, but only after the interval.
        assert!(!wait.poll_with(deadline, read(at_revision(6))));
        hold_off_reads(&mut wait);
        assert!(!wait.poll_with(deadline, no_read));
        assert_eq!(wait.last, Some(at_revision(6)));
        wait.next_read = Instant::now();
        assert!(!wait.poll_with(deadline, read(at_revision(7))));
        assert!(wait.poll_with(deadline, no_read));
        assert!(wait.current_at.is_some() && !wait.timed_out);
        assert_eq!(wait.last, Some(at_revision(7)));
        // Done: no more reads.
        wait.next_read = Instant::now();
        assert!(wait.poll_with(deadline, no_read));
    }

    #[test]
    fn a_failed_or_lost_read_is_replaced_and_a_later_reading_clears_it() {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut wait = WorkCurrentWait::new();
        assert!(!wait.poll_with(deadline, |_| {
            Spawned::ready(Err(anyhow::anyhow!("connection refused")))
        }));
        hold_off_reads(&mut wait);
        assert!(!wait.poll_with(deadline, no_read));
        assert!(wait
            .error
            .as_deref()
            .is_some_and(|error| error.contains("connection refused")));
        assert!(!wait.timed_out, "a failed read does not end the wait");
        // A read whose task panicked or was cancelled yields nothing.
        wait.read = Some(Spawned {
            task: None,
            value: None,
        });
        assert!(!wait.poll_with(deadline, no_read));
        assert_eq!(
            wait.error.as_deref(),
            Some("a read of the frontends' work was lost")
        );
        wait.next_read = Instant::now();
        assert!(!wait.poll_with(deadline, read(at_revision(7))));
        assert!(wait.poll_with(deadline, no_read));
        assert!(wait.current_at.is_some());
        assert_eq!(wait.error, None);
    }

    #[test]
    fn the_bound_ends_the_wait_and_a_reading_past_it_does_not_count() {
        let mut wait = WorkCurrentWait::new();
        assert!(!wait.poll_with(
            Instant::now() + Duration::from_secs(60),
            read(at_revision(7))
        ));
        // Current, but only once the bound had passed.
        assert!(wait.poll_with(Instant::now(), no_read));
        assert!(wait.timed_out && wait.current_at.is_none());
        assert_eq!(wait.last, Some(at_revision(7)), "kept for the verdict");
        assert!(wait.poll_with(Instant::now(), no_read));
        assert!(wait.last_read().contains("payout_revision: 7"));
    }

    #[test]
    fn work_published_at_the_locked_revision_ignores_sessions_without_a_first_job() {
        let reading = |frontends| WorkReading {
            payout_revision: 7,
            settlements_in_progress: 1,
            frontends,
        };
        // Our own holder is the settlement under way, and a session that
        // has just authorized waits for its first job behind it.
        assert!(reading(vec![work(true, Some(7), Some(1))]).published_at(7));
        // A tip that reached a frontend before the lock: unready, or still
        // publishing the previous revision's work.
        assert!(!reading(vec![
            work(true, Some(7), Some(0)),
            work(false, Some(7), Some(0))
        ])
        .published_at(7));
        assert!(!reading(vec![work(true, Some(6), Some(0))]).published_at(7));
        assert!(!reading(vec![None]).published_at(7));
        assert!(!reading(Vec::new()).published_at(7));
    }

    #[test]
    fn a_settlement_under_way_holds_only_a_quiet_wait() {
        let deadline = Instant::now() + Duration::from_secs(60);
        let settling = WorkReading {
            settlements_in_progress: 1,
            ..at_revision(7)
        };
        assert!(settling.current() && !settling.quiet());
        let mut plain = WorkCurrentWait::new();
        assert!(!plain.poll_with(deadline, read(settling.clone())));
        assert!(plain.poll_with(deadline, no_read));
        assert!(plain.current_at.is_some());
        let mut quiet = WorkCurrentWait::quiet();
        assert!(!quiet.poll_with(deadline, read(settling.clone())));
        hold_off_reads(&mut quiet);
        assert!(!quiet.poll_with(deadline, no_read));
        assert_eq!(quiet.last, Some(settling));
        quiet.next_read = Instant::now();
        assert!(!quiet.poll_with(deadline, read(at_revision(7))));
        assert!(quiet.poll_with(deadline, no_read));
        assert!(quiet.current_at.is_some());
    }

    #[tokio::test]
    async fn an_abort_cancels_a_read_in_flight() {
        let mut wait = WorkCurrentWait::quiet();
        assert!(
            !wait.poll_with(Instant::now() + Duration::from_secs(60), |_| {
                Spawned::spawn(std::future::pending::<Result<WorkReading>>())
            })
        );
        wait.abort();
        assert!(wait.read.is_none());
    }

    #[tokio::test]
    async fn the_bound_abandons_a_read_still_in_flight() {
        let mut wait = WorkCurrentWait::new();
        let deadline = Instant::now() + Duration::from_secs(60);
        assert!(!wait.poll_with(deadline, |_| {
            Spawned::spawn(std::future::pending::<Result<WorkReading>>())
        }));
        assert!(!wait.poll_with(deadline, no_read), "still within the bound");
        assert!(wait.poll_with(Instant::now(), no_read));
        assert!(wait.timed_out && wait.current_at.is_none() && wait.last.is_none());
        assert!(
            wait.read.is_none(),
            "the read is cancelled, not left running"
        );
    }
}
