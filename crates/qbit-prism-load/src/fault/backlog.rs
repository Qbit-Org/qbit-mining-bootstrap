//! Both frontends restarted over a pending candidate backlog (#554, fault
//! 10; #187's class).
//!
//! The relay answers every `submitblock` with qbitd's warmup error, which the
//! server knows was never run (#526): each found block's candidate goes back
//! to `pending` and is retried after a backoff. While it refuses, the fault
//! asks `backlog` sessions for a found block each, until PostgreSQL holds at
//! least that many unfinished candidates. Every frontend is then stopped
//! (SIGTERM, its sessions paused) and relaunched with the relay still
//! refusing, so the new processes' recovery meets the refusal too; then the
//! relay heals. Every backlog row must reach a terminal state, none may reach
//! the node twice, and the rows counted before the restart must all be there
//! after it.

use super::{frontend::terminate, FaultEnv, FaultTools, Spawned};
use crate::{client, restart::ReadyWait};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

/// How long the backlog may take to build.
pub const BUILD_WAIT: Duration = Duration::from_secs(90);
/// How long after the heal every backlog row may take to settle: the
/// server's pending backoff grows by a second per attempt, to 60 s.
pub const SETTLE_WAIT: Duration = Duration::from_secs(180);

/// The server's default `PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS` (#415): a
/// backlog block that lost the tip race to another backlog block (the node
/// answers `inconclusive`) is kept for chain reconciliation until the winner
/// has this many confirmations, then settled as a proven orphan. The fault
/// mints that many blocks once a row is waiting for it, as the chain would.
pub const ORPHAN_CONFIRMATIONS: usize = 6;

/// The unfinished candidate states (`CandidateState::UNFINISHED_SQL`).
pub(super) const UNFINISHED: [&str; 4] = ["pending", "offer_reserved", "offered", "reconciliation"];

enum Stage {
    Start,
    Building {
        deadline: Instant,
        read: Option<Spawned<Result<BTreeMap<String, String>>>>,
        next_ask: Instant,
    },
    AwaitingExits {
        deadline: Instant,
    },
    Relaunching {
        waits: Vec<Option<ReadyWait>>,
    },
    Done,
}

/// How one frontend's stop went.
#[derive(Clone, Debug)]
pub struct Exit {
    pub index: usize,
    pub status: Option<String>,
    pub success: Option<bool>,
    pub forced_kill: bool,
    pub after_sigterm_seconds: Option<f64>,
}

pub struct CandidateBacklog {
    stage: Stage,
    target: usize,
    asked: usize,
    /// Blocks refused before this fault began (an earlier backlog in the
    /// same plan), which are not this backlog's.
    earlier: BTreeSet<String>,
    /// The latest read of this backlog's rows while it builds.
    last_rows: BTreeMap<String, String>,
    next_read: Instant,
    /// Every frontend's sessions, paused for the restart.
    home: Vec<(usize, Vec<usize>)>,
    pub built_at: Option<Instant>,
    /// The backlog: each refused block's row state before the restart.
    pub before: BTreeMap<String, String>,
    pub signalled_at: Option<Instant>,
    pub exits: Vec<Exit>,
    pub ready_at: Option<Instant>,
    pub healed_at: Option<Instant>,
    settle: Option<Spawned<Result<BTreeMap<String, String>>>>,
    next_settle_read: Instant,
    pub after: BTreeMap<String, String>,
    pub settled_at: Option<Instant>,
    /// Blocks minted so a lost race's winner reaches the orphan proof.
    pub mints: usize,
    next_mint: Instant,
    pub problems: Vec<String>,
}

/// The state of each named candidate row.
pub(super) async fn states(pool: PgPool, hashes: Vec<String>) -> Result<BTreeMap<String, String>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT block_hash, state FROM qbit_block_candidate_outbox WHERE block_hash = ANY($1)",
    )
    .bind(&hashes)
    .fetch_all(&pool)
    .await?;
    Ok(rows.into_iter().collect())
}

pub fn terminal(state: &str) -> bool {
    !UNFINISHED.contains(&state)
}

impl CandidateBacklog {
    pub fn new(target: usize) -> Self {
        Self {
            stage: Stage::Start,
            target: target.max(1),
            asked: 0,
            earlier: BTreeSet::new(),
            last_rows: BTreeMap::new(),
            next_read: Instant::now(),
            home: Vec::new(),
            built_at: None,
            before: BTreeMap::new(),
            signalled_at: None,
            exits: Vec::new(),
            ready_at: None,
            healed_at: None,
            settle: None,
            next_settle_read: Instant::now(),
            after: BTreeMap::new(),
            settled_at: None,
            mints: 0,
            next_mint: Instant::now(),
            problems: Vec::new(),
        }
    }

    /// Every frontend, while it is down on purpose.
    pub fn outages(&self) -> Vec<usize> {
        match &self.stage {
            Stage::AwaitingExits { .. } | Stage::Relaunching { .. } => {
                self.home.iter().map(|(index, _)| *index).collect()
            }
            _ => Vec::new(),
        }
    }

    /// The windows each frontend was down on purpose.
    pub fn outage_windows(&self, end: Instant) -> Vec<(usize, Instant, Instant)> {
        match self.signalled_at {
            Some(from) => self
                .home
                .iter()
                .map(|(index, _)| (*index, from, self.ready_at.unwrap_or(end)))
                .collect(),
            None => Vec::new(),
        }
    }

    /// The blocks the relay refused, by hash.
    fn refused(tools: &FaultTools) -> BTreeSet<String> {
        tools
            .relay
            .submits()
            .into_iter()
            .filter(|submit| submit.armed == Some("refused-warmup"))
            .map(|submit| submit.block_hash)
            .collect()
    }

    /// `Ok(true)` once every frontend is back and serving, with the relay
    /// healed.
    pub fn poll(&mut self, env: &mut FaultEnv<'_>, tools: &FaultTools) -> Result<bool> {
        loop {
            match &mut self.stage {
                Stage::Start => {
                    self.earlier = Self::refused(tools);
                    tools.relay.set_refusing(true);
                    self.stage = Stage::Building {
                        deadline: Instant::now() + BUILD_WAIT,
                        read: None,
                        next_ask: Instant::now(),
                    };
                    continue;
                }
                Stage::Building {
                    deadline,
                    read,
                    next_ask,
                } => {
                    // One found block per session asked, a second apart, so
                    // each is its own candidate row, until this backlog's
                    // refused blocks less those already settled (a stale
                    // one, say) reach the target.
                    let refused: Vec<String> = Self::refused(tools)
                        .difference(&self.earlier)
                        .cloned()
                        .collect();
                    let settled = self
                        .last_rows
                        .values()
                        .filter(|state| terminal(state))
                        .count();
                    if refused.len().saturating_sub(settled) < self.target
                        && Instant::now() >= *next_ask
                    {
                        *next_ask = Instant::now() + Duration::from_secs(1);
                        let working: Vec<usize> = env
                            .sessions
                            .iter()
                            .filter(|session| !session.paused.load(Ordering::Relaxed))
                            .map(|session| session.index)
                            .collect();
                        if !working.is_empty() {
                            let session = working[self.asked % working.len()];
                            self.asked += 1;
                            let _ = env.sessions[session]
                                .control
                                .send(client::Control::ScheduledBlock);
                        }
                    }
                    if let Some(task) = read.as_mut() {
                        if let Some(result) = task.poll() {
                            match result {
                                Ok(rows) => {
                                    self.last_rows = rows.clone();
                                    let unfinished =
                                        rows.values().filter(|state| !terminal(state)).count();
                                    if unfinished >= self.target {
                                        self.before = rows.clone();
                                        self.built_at = Some(Instant::now());
                                        self.begin_restart(env)?;
                                        continue;
                                    }
                                }
                                Err(error) => self
                                    .problems
                                    .push(format!("reading the backlog: {error:#}")),
                            }
                            *read = None;
                        }
                    }
                    if read.is_none() && !refused.is_empty() && Instant::now() >= self.next_read {
                        self.next_read = Instant::now() + Duration::from_millis(250);
                        *read = Some(Spawned::spawn(states(tools.side.clone(), refused.clone())));
                    }
                    if Instant::now() >= *deadline {
                        self.problems.push(format!(
                            "{} of {} found blocks unfinished within {BUILD_WAIT:?} ({} refused at \
                             the relay); the frontends were restarted over what there was",
                            self.last_rows.values().filter(|state| !terminal(state)).count(),
                            self.target,
                            refused.len()
                        ));
                        self.before = self.last_rows.clone();
                        self.built_at = Some(Instant::now());
                        self.begin_restart(env)?;
                        continue;
                    }
                    return Ok(false);
                }
                Stage::AwaitingExits { deadline } => {
                    let mut waiting = false;
                    for (index, _) in &self.home {
                        let index = *index;
                        if self.exits.iter().any(|exit| exit.index == index) {
                            continue;
                        }
                        let signalled = self.signalled_at.unwrap_or_else(Instant::now);
                        match env.frontends[index].exited() {
                            Some(status) => self.exits.push(Exit {
                                index,
                                status: Some(status.to_string()),
                                success: Some(status.success()),
                                forced_kill: false,
                                after_sigterm_seconds: Some(signalled.elapsed().as_secs_f64()),
                            }),
                            None if Instant::now() < *deadline => waiting = true,
                            None => {
                                env.frontends[index].kill();
                                self.exits.push(Exit {
                                    index,
                                    status: None,
                                    success: None,
                                    forced_kill: true,
                                    after_sigterm_seconds: None,
                                });
                            }
                        }
                    }
                    if waiting {
                        return Ok(false);
                    }
                    let mut waits = Vec::new();
                    for (index, _) in &self.home {
                        env.frontends[*index].restart()?;
                        if let Some(sampler) = env.samplers.get(*index) {
                            sampler.set_pid(env.frontends[*index].pid());
                        }
                        waits.push(Some(ReadyWait::start(
                            tools.ready_limit,
                            "its relaunch over a candidate backlog",
                        )));
                    }
                    self.stage = Stage::Relaunching { waits };
                    return Ok(false);
                }
                Stage::Relaunching { waits } => {
                    for (slot, (index, _)) in waits.iter_mut().zip(&self.home) {
                        if let Some(wait) = slot.as_mut() {
                            if wait.poll(&mut env.frontends[*index])? {
                                *slot = None;
                            }
                        }
                    }
                    if waits.iter().any(Option::is_some) {
                        return Ok(false);
                    }
                    self.ready_at = Some(Instant::now());
                    for (index, sessions) in &self.home {
                        let address = env.frontends[*index].stratum_address();
                        for &session in sessions {
                            let handle = &env.sessions[session];
                            let _ = handle.control.send(client::Control::Retarget {
                                frontend: *index,
                                address: address.clone(),
                                reconnect: false,
                            });
                            handle.paused.store(false, Ordering::Relaxed);
                        }
                    }
                    tools.relay.set_refusing(false);
                    self.healed_at = Some(Instant::now());
                    self.stage = Stage::Done;
                    return Ok(true);
                }
                Stage::Done => return Ok(true),
            }
        }
    }

    /// The phase ended mid-fault: heal the relay, relaunch any frontend
    /// still down and give every paused session its frontend back, so what
    /// follows the phase has a pool to reach.
    pub fn abandon(&mut self, env: &mut FaultEnv<'_>, tools: &FaultTools) {
        tools.relay.set_refusing(false);
        if !matches!(
            self.stage,
            Stage::AwaitingExits { .. } | Stage::Relaunching { .. }
        ) {
            return;
        }
        for (index, sessions) in &self.home {
            let frontend = &mut env.frontends[*index];
            if frontend.pid().is_none() || frontend.exited().is_some() {
                if frontend.exited().is_none() {
                    frontend.kill();
                }
                if frontend.restart().is_ok() {
                    if let Some(sampler) = env.samplers.get(*index) {
                        sampler.set_pid(frontend.pid());
                    }
                }
            }
            let address = env.frontends[*index].stratum_address();
            for &session in sessions {
                let handle = &env.sessions[session];
                let _ = handle.control.send(client::Control::Retarget {
                    frontend: *index,
                    address: address.clone(),
                    reconnect: true,
                });
                handle.paused.store(false, Ordering::Relaxed);
            }
        }
        self.stage = Stage::Done;
    }

    fn begin_restart(&mut self, env: &mut FaultEnv<'_>) -> Result<()> {
        self.home = (0..env.frontends.len())
            .map(|index| {
                let sessions: Vec<usize> = env
                    .sessions
                    .iter()
                    .filter(|session| session.frontend.load(Ordering::Relaxed) == index)
                    .map(|session| session.index)
                    .collect();
                (index, sessions)
            })
            .collect();
        for (_, sessions) in &self.home {
            for &session in sessions {
                let handle = &env.sessions[session];
                handle.paused.store(true, Ordering::Relaxed);
                let _ = handle.control.send(client::Control::Pause);
            }
        }
        for (index, _) in &self.home {
            let pid = env.frontends[*index]
                .pid()
                .context("a frontend in the backlog restart has no process")?;
            terminate(pid);
        }
        self.signalled_at = Some(Instant::now());
        self.stage = Stage::AwaitingExits {
            deadline: Instant::now() + super::frontend::EXIT_LIMIT,
        };
        Ok(())
    }

    /// After the heal: every backlog row read until each is terminal or the
    /// settle wait passes. `true` once settled either way.
    pub fn poll_settle(&mut self, env: &FaultEnv<'_>, tools: &FaultTools) -> bool {
        if self.settled_at.is_some() {
            return true;
        }
        let hashes: Vec<String> = self.before.keys().cloned().collect();
        if hashes.is_empty() {
            self.settled_at = Some(Instant::now());
            return true;
        }
        if let Some(read) = self.settle.as_mut() {
            let Some(result) = read.poll() else {
                return false;
            };
            match result {
                Ok(rows) => {
                    self.after = rows.clone();
                    if hashes
                        .iter()
                        .all(|hash| self.after.get(hash).is_some_and(|state| terminal(state)))
                    {
                        self.settled_at = Some(Instant::now());
                        return true;
                    }
                    // A lost race waits for its winner's confirmations.
                    if self.after.values().any(|state| state == "reconciliation")
                        && self.mints < ORPHAN_CONFIRMATIONS
                        && Instant::now() >= self.next_mint
                    {
                        env.node.mint_external(crate::node::MintPurpose::Fault);
                        self.mints += 1;
                        self.next_mint = Instant::now() + Duration::from_secs(1);
                    }
                }
                Err(error) => self
                    .problems
                    .push(format!("reading the backlog after the heal: {error:#}")),
            }
            self.settle = None;
        }
        if self
            .healed_at
            .is_some_and(|healed| healed.elapsed() >= SETTLE_WAIT)
        {
            return true;
        }
        if Instant::now() >= self.next_settle_read {
            self.next_settle_read = Instant::now() + Duration::from_millis(500);
            self.settle = Some(Spawned::spawn(states(tools.side.clone(), hashes)));
        }
        false
    }

    pub fn evidence(&self, origin: Instant) -> Value {
        let at = |instant: Option<Instant>| {
            instant.map(|at| at.saturating_duration_since(origin).as_secs_f64())
        };
        json!({
            "backlog_target": self.target,
            "found_blocks_asked": self.asked,
            "backlog_built_after_seconds": at(self.built_at),
            "backlog_before_restart": self.before,
            "frontends_signalled_after_seconds": at(self.signalled_at),
            "exits": self.exits.iter().map(|exit| json!({
                "frontend": exit.index,
                "exit_status": exit.status,
                "exit_success": exit.success,
                "forced_kill": exit.forced_kill,
                "exit_after_sigterm_seconds": exit.after_sigterm_seconds,
            })).collect::<Vec<_>>(),
            "serving_again_after_seconds": at(self.ready_at),
            "relay_healed_after_seconds": at(self.healed_at),
            "backlog_after_heal": self.after,
            "blocks_minted_for_orphan_proofs": self.mints,
            "settled_after_heal_seconds": self.settled_at.zip(self.healed_at)
                .map(|(settled, healed)| settled.saturating_duration_since(healed).as_secs_f64()),
            "problems": self.problems,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_unfinished_states_are_the_servers() {
        let server = qbit_prism_server::ledger::CandidateState::UNFINISHED_SQL;
        let listed: Vec<&str> = server
            .trim_matches(|c| c == '(' || c == ')')
            .split(',')
            .map(|state| state.trim_matches('\''))
            .collect();
        assert_eq!(listed, UNFINISHED);
        assert!(!terminal("pending") && terminal("submitted") && terminal("orphaned"));
    }
}
