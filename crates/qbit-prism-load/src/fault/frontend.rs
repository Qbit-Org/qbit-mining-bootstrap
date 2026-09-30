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
use serde_json::{json, Value};
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
            "problems": self.problems,
        })
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
