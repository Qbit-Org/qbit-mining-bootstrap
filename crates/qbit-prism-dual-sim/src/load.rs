//! Stratum load: real sessions mining real proof of work through the
//! balancer, and one block finder per node that finds an own block on that
//! node when a scenario asks.
//!
//! The sessions are `qbit-prism-load`'s client (`client::spawn_session`):
//! each connects, subscribes, authorizes, mines each job at the configured
//! share difficulty, and reconnects 250 ms after its connection goes, as a
//! miner behind a balancer does. It never submits a hash that also meets the
//! network target, so the load alone finds no block and every block in a
//! scenario is one it asked for. An open-loop scheduler offers shares at the
//! plan's rate, round robin over the sessions that hold work.
//!
//! Every submit is recorded with its `share_id` (`<user>.<worker>:<header
//! hash>`, the ledger's own identity), the job it was mined on (whose id
//! names the frontend that issued it), its outcome and when it was sent and
//! answered on the run's clock. The pool answers `true` only after the
//! share's ledger transaction committed (`stratum.rs`), so an accepted
//! record is a share some node's database holds.
//!
//! Block finders bypass the balancer: a block "found by B" is one solved on
//! B's work and submitted to B, whatever the balancer prefers.

use crate::frontend::Node;
use anyhow::{bail, ensure, Context, Result};
use qbit_prism_load::client::{
    self, Control, DifficultySource, Event, Outcome, SessionConfig, SessionHandle, SessionShared,
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

const PHASE: &str = "dual-sim";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// The server bounds session allocation by its 30 s initial-job timeout.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(35);
/// The share commit timeout (15 s) plus its grace (5 s), so a deliberate
/// close never turns a share the server may still commit into "no answer".
const QUIESCE_LIMIT: Duration = Duration::from_secs(25);

/// One payout identity and how many sessions mine for it.
#[derive(Clone, Debug, Serialize)]
pub struct Account {
    pub label: String,
    pub address: String,
    /// Its 32-byte P2MR program, hex.
    pub program_hex: String,
    pub sessions: usize,
}

impl Account {
    /// An account paying a derived regtest P2MR address nobody holds a key
    /// for: a valid output that nothing spends.
    pub fn derived(label: &str, sessions: usize) -> Self {
        let (address, program_hex) =
            qbit_prism_load::qbitd::derived_address(&format!("dual-sim-miner-{label}"));
        Self {
            label: label.to_owned(),
            address,
            program_hex,
            sessions,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct LoadPlan {
    pub accounts: Vec<Account>,
    /// Offered shares per second across every session.
    pub rate: f64,
    /// The frontends' configured share difficulty.
    pub share_difficulty: f64,
}

/// One submit as the scenario report and the checker read it. Times are
/// milliseconds on the run's clock.
#[derive(Clone, Debug, Serialize)]
pub struct ShareRecord {
    pub share_id: String,
    pub session: usize,
    pub account: String,
    pub job_id: String,
    /// The node whose frontend issued the job, from the job id's
    /// `<instance>-<uuid>` prefix; `None` for an unrecognised prefix.
    pub issuer: Option<Node>,
    pub outcome: String,
    pub reason: Option<String>,
    pub sent_ms: u64,
    pub answered_ms: Option<u64>,
    pub scheduled_block: bool,
}

impl ShareRecord {
    pub fn accepted(&self) -> bool {
        self.outcome == "accepted"
    }

    /// An ordinary share (not a scheduled block) accepted on `node`'s jobs
    /// and answered after `after_ms`: what the scenarios count as `node`
    /// taking miners.
    pub fn accepted_on(&self, node: Node, after_ms: u64) -> bool {
        self.accepted()
            && !self.scheduled_block
            && self.issuer == Some(node)
            && self.answered_ms.is_some_and(|at| at > after_ms)
    }

    /// The header hash the `share_id` ends with: the block hash when the
    /// share was also a block.
    pub fn header_hash(&self) -> &str {
        self.share_id
            .rsplit_once(':')
            .map_or(self.share_id.as_str(), |(_, hash)| hash)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionRecord {
    pub session: usize,
    pub event: String,
    pub at_ms: u64,
    pub cause: String,
}

/// The run's clock: milliseconds since the scenario started.
#[derive(Clone, Copy, Debug)]
pub struct RunClock {
    pub started: Instant,
}

impl RunClock {
    pub fn ms(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.started).as_millis() as u64
    }

    pub fn now_ms(&self) -> u64 {
        self.ms(Instant::now())
    }
}

#[derive(Default)]
struct Log {
    shares: Vec<ShareRecord>,
    connections: Vec<ConnectionRecord>,
    discarded_block_solutions: u64,
    unplaced_offers: u64,
}

/// Which node issued a job, from its id (`coordinator.rs`: `{instance}-{uuid}`).
pub fn issuer_of(job_id: &str) -> Option<Node> {
    Node::BOTH
        .into_iter()
        .find(|node| job_id.starts_with(&format!("{}-", node.instance_id())))
}

pub struct Load {
    clock: RunClock,
    sessions: Vec<SessionHandle>,
    /// The account each load session mines for, by session index.
    account_of: Vec<String>,
    /// Session index of each node's block finder.
    finders: BTreeMap<Node, usize>,
    holding: Arc<Vec<AtomicBool>>,
    /// Connections each session has opened (reached work on), so a caller
    /// can tell a fresh connection from the one it had.
    opened: Arc<Vec<AtomicU64>>,
    log: Arc<Mutex<Log>>,
    paused: Arc<AtomicBool>,
    stopping: Arc<AtomicBool>,
    scheduler: Option<JoinHandle<()>>,
    collector: Option<JoinHandle<()>>,
    blocks_asked: AtomicU64,
}

impl Load {
    /// Start every session against the balancer at `balancer_port`, and a
    /// block finder against each node's own Stratum port in `finders`. The
    /// load is offered once [`Load::resume`] is called.
    pub fn start(
        plan: &LoadPlan,
        balancer_port: u16,
        finders: &[(Node, u16)],
        clock: RunClock,
    ) -> Result<Self> {
        ensure!(plan.rate > 0.0, "the load rate must be positive");
        let load_sessions: usize = plan.accounts.iter().map(|account| account.sessions).sum();
        ensure!(load_sessions > 0, "the load has no sessions");
        let total = load_sessions + finders.len();
        let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let stopping = Arc::new(AtomicBool::new(false));
        let shared = Arc::new(SessionShared {
            phase: std::sync::RwLock::new(PHASE.to_owned()),
            events,
            record_notifies: AtomicBool::new(false),
            kill_fence: Arc::new(AtomicU64::new(0)),
            stopping: stopping.clone(),
        });
        let config = |index: usize, username: String| SessionConfig {
            index,
            username,
            password: "x".into(),
            difficulty: DifficultySource::Configured(plan.share_difficulty),
            version_rolling_mask: qbit_prism_server::codec::VERSION_ROLLING_MASK,
            connect_timeout: CONNECT_TIMEOUT,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            quiesce_limit: QUIESCE_LIMIT,
            drop_offers_held_while_disconnected: true,
        };
        let mut sessions = Vec::with_capacity(total);
        let mut account_of = Vec::with_capacity(total);
        for account in &plan.accounts {
            for worker in 0..account.sessions {
                let index = sessions.len();
                let username = format!("{}.{}-{worker:03}", account.address, account.label);
                sessions.push(client::spawn_session(
                    config(index, username),
                    0,
                    format!("127.0.0.1:{balancer_port}"),
                    shared.clone(),
                    1,
                ));
                account_of.push(account.label.clone());
            }
        }
        let finder_account = plan
            .accounts
            .first()
            .context("the load has no account for the block finders")?;
        let mut finder_index = BTreeMap::new();
        for (node, port) in finders {
            let index = sessions.len();
            let username = format!("{}.finder-{}", finder_account.address, node.label());
            sessions.push(client::spawn_session(
                config(index, username),
                node.index(),
                format!("127.0.0.1:{port}"),
                shared.clone(),
                1,
            ));
            account_of.push(finder_account.label.clone());
            finder_index.insert(*node, index);
        }
        drop(shared);
        let holding: Arc<Vec<AtomicBool>> =
            Arc::new((0..total).map(|_| AtomicBool::new(false)).collect());
        let opened: Arc<Vec<AtomicU64>> = Arc::new((0..total).map(|_| AtomicU64::new(0)).collect());
        let log = Arc::new(Mutex::new(Log::default()));
        let collector = {
            let (holding, opened, log, account_of) = (
                holding.clone(),
                opened.clone(),
                log.clone(),
                account_of.clone(),
            );
            tokio::spawn(async move {
                while let Some(event) = inbox.recv().await {
                    let Ok(mut log) = log.lock() else { return };
                    match event {
                        Event::Opened(connection) => {
                            holding[connection.session].store(true, Ordering::SeqCst);
                            opened[connection.session].fetch_add(1, Ordering::SeqCst);
                            log.connections.push(ConnectionRecord {
                                session: connection.session,
                                event: "opened".into(),
                                at_ms: clock.ms(connection.ready),
                                cause: connection.cause,
                            });
                        }
                        Event::Closed(closed) => {
                            holding[closed.session].store(false, Ordering::SeqCst);
                            log.connections.push(ConnectionRecord {
                                session: closed.session,
                                event: "closed".into(),
                                at_ms: clock.ms(closed.at),
                                cause: closed.cause,
                            });
                        }
                        Event::Submit(record) => {
                            let (outcome, reason) = match &record.outcome {
                                Outcome::Accepted => ("accepted", None),
                                Outcome::Rejected(rejection) => (
                                    "rejected",
                                    Some(
                                        rejection
                                            .reason_id
                                            .clone()
                                            .unwrap_or_else(|| rejection.message.clone()),
                                    ),
                                ),
                                Outcome::NoResponse { reason } => {
                                    ("no-response", Some(reason.clone()))
                                }
                            };
                            log.shares.push(ShareRecord {
                                issuer: issuer_of(&record.job_id),
                                share_id: record.share_id.clone(),
                                session: record.session,
                                account: account_of
                                    .get(record.session)
                                    .cloned()
                                    .unwrap_or_default(),
                                job_id: record.job_id.clone(),
                                outcome: outcome.to_owned(),
                                reason,
                                sent_ms: clock.ms(record.sent),
                                answered_ms: record.responded.map(|at| clock.ms(at)),
                                scheduled_block: record.scheduled_block,
                            });
                        }
                        Event::DiscardedBlockSolution { .. } => {
                            log.discarded_block_solutions += 1;
                        }
                        _ => {}
                    }
                }
            })
        };
        let paused = Arc::new(AtomicBool::new(true));
        let load_indices: Vec<usize> = (0..load_sessions).collect();
        let scheduler = {
            let (holding, paused, log) = (holding.clone(), paused.clone(), log.clone());
            let offers: Vec<(
                mpsc_work::Sender,
                Arc<std::sync::atomic::AtomicUsize>,
                Arc<AtomicBool>,
            )> = load_indices
                .iter()
                .map(|index| {
                    let session = &sessions[*index];
                    (
                        session.work.clone(),
                        session.outstanding.clone(),
                        session.paused.clone(),
                    )
                })
                .collect();
            let gap = Duration::from_secs_f64(1.0 / plan.rate);
            tokio::spawn(async move {
                let phase: Arc<str> = Arc::from(PHASE);
                let mut ticker = tokio::time::interval(gap);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut next = 0usize;
                loop {
                    ticker.tick().await;
                    if paused.load(Ordering::SeqCst) {
                        continue;
                    }
                    let mut placed = false;
                    for step in 0..offers.len() {
                        let slot = (next + step) % offers.len();
                        if !holding[slot].load(Ordering::SeqCst) {
                            continue;
                        }
                        let (work, outstanding, session_paused) = &offers[slot];
                        if session_paused.load(Ordering::Relaxed)
                            || outstanding.load(Ordering::Relaxed) >= 1
                        {
                            continue;
                        }
                        if work
                            .try_send(client::Work::Submit {
                                phase: phase.clone(),
                            })
                            .is_ok()
                        {
                            outstanding.fetch_add(1, Ordering::Relaxed);
                            next = slot + 1;
                            placed = true;
                            break;
                        }
                    }
                    if !placed {
                        if let Ok(mut log) = log.lock() {
                            log.unplaced_offers += 1;
                        }
                    }
                }
            })
        };
        Ok(Self {
            clock,
            sessions,
            account_of,
            finders: finder_index,
            holding,
            opened,
            log,
            paused,
            stopping,
            scheduler: Some(scheduler),
            collector: Some(collector),
            blocks_asked: AtomicU64::new(0),
        })
    }

    /// Start (or restart) offering shares.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    /// Stop offering shares; sessions stay connected.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    /// How many load sessions hold work now.
    pub fn holding_work(&self) -> usize {
        let load_sessions = self.sessions.len() - self.finders.len();
        self.holding[..load_sessions]
            .iter()
            .filter(|holds| holds.load(Ordering::SeqCst))
            .count()
    }

    /// Wait until at least `count` load sessions hold work.
    pub async fn wait_holding(&self, count: usize, limit: Duration) -> Result<()> {
        let started = Instant::now();
        while self.holding_work() < count {
            ensure!(
                started.elapsed() < limit,
                "only {} of {count} sessions held work after {limit:?}",
                self.holding_work()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// Wait until no load share is outstanding: every offered share has its
    /// answer or was recorded as unanswered.
    pub async fn wait_answered(&self, limit: Duration) -> Result<()> {
        let started = Instant::now();
        let load_sessions = self.sessions.len() - self.finders.len();
        loop {
            let outstanding: usize = self.sessions[..load_sessions]
                .iter()
                .map(|session| session.outstanding.load(Ordering::SeqCst))
                .sum();
            if outstanding == 0 {
                return Ok(());
            }
            ensure!(
                started.elapsed() < limit,
                "{outstanding} shares were still unanswered after {limit:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Reconnect `node`'s block finder and wait until the new connection
    /// holds work: a job built now, on the node's current tip and payout
    /// revision, rather than one an earlier tip or revision superseded.
    pub async fn refresh_finder(&self, node: Node, limit: Duration) -> Result<()> {
        let index = *self
            .finders
            .get(&node)
            .with_context(|| format!("no block finder for node {node:?}"))?;
        let before = self.opened[index].load(Ordering::SeqCst);
        self.sessions[index]
            .control
            .send(Control::Reconnect {
                reason: "fresh work for a scheduled block".into(),
                phase: Arc::from(PHASE),
            })
            .map_err(|_| anyhow::anyhow!("node {node:?}'s block finder has stopped"))?;
        let started = Instant::now();
        while self.opened[index].load(Ordering::SeqCst) <= before
            || !self.holding[index].load(Ordering::SeqCst)
        {
            ensure!(
                started.elapsed() < limit,
                "node {node:?}'s block finder held no fresh work within {limit:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }

    /// Have `node`'s block finder solve a block on the work it holds and
    /// submit it to that node. Returns the submit's record once answered.
    /// The block's hash is [`ShareRecord::header_hash`]; that it lands is
    /// for the caller to wait for.
    pub async fn find_block(&self, node: Node, limit: Duration) -> Result<ShareRecord> {
        let index = *self
            .finders
            .get(&node)
            .with_context(|| format!("no block finder for node {node:?}"))?;
        let started = Instant::now();
        while !self.holding[index].load(Ordering::SeqCst) {
            ensure!(
                started.elapsed() < limit,
                "node {node:?}'s block finder held no work within {limit:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let before = self.records().len();
        self.blocks_asked.fetch_add(1, Ordering::SeqCst);
        self.sessions[index]
            .control
            .send(Control::ScheduledBlock)
            .map_err(|_| anyhow::anyhow!("node {node:?}'s block finder has stopped"))?;
        loop {
            if let Some(record) = self.records()[before..]
                .iter()
                .find(|record| record.session == index && record.scheduled_block)
            {
                return Ok(record.clone());
            }
            if started.elapsed() > limit {
                bail!("node {node:?}'s scheduled block got no answer within {limit:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Every submit so far.
    pub fn records(&self) -> Vec<ShareRecord> {
        self.log
            .lock()
            .map(|log| log.shares.clone())
            .unwrap_or_default()
    }

    /// How many ordinary shares were accepted on `node`'s jobs and answered
    /// after `after_ms`, counted in place rather than through a copy of the
    /// whole log.
    pub fn accepted_on(&self, node: Node, after_ms: u64) -> usize {
        self.log
            .lock()
            .map(|log| {
                log.shares
                    .iter()
                    .filter(|r| r.accepted_on(node, after_ms))
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn connections(&self) -> Vec<ConnectionRecord> {
        self.log
            .lock()
            .map(|log| log.connections.clone())
            .unwrap_or_default()
    }

    pub fn unplaced_offers(&self) -> u64 {
        self.log.lock().map(|log| log.unplaced_offers).unwrap_or(0)
    }

    pub fn clock(&self) -> RunClock {
        self.clock
    }

    pub fn account_labels(&self) -> Vec<String> {
        let mut labels = self.account_of.clone();
        labels.dedup();
        labels
    }

    /// Stop offering, wait for every answer, then stop every session.
    pub async fn stop(&mut self) -> Result<()> {
        self.pause();
        let answered = self.wait_answered(QUIESCE_LIMIT).await;
        self.stopping.store(true, Ordering::SeqCst);
        for session in &self.sessions {
            let _ = session.control.send(Control::Stop);
        }
        if let Some(scheduler) = self.scheduler.take() {
            scheduler.abort();
        }
        for session in self.sessions.drain(..) {
            let _ = tokio::time::timeout(Duration::from_secs(30), session.task).await;
        }
        if let Some(collector) = self.collector.take() {
            let _ = tokio::time::timeout(Duration::from_secs(30), collector).await;
        }
        answered
    }
}

impl Drop for Load {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
        if let Some(scheduler) = self.scheduler.take() {
            scheduler.abort();
        }
        for session in &self.sessions {
            session.task.abort();
        }
    }
}

/// The `Sender` type of a session's work queue.
mod mpsc_work {
    pub type Sender = tokio::sync::mpsc::Sender<qbit_prism_load::client::Work>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_job_id_names_the_node_that_issued_it() {
        assert_eq!(issuer_of("dual-sim-a-0f1e2d3c"), Some(Node::A));
        assert_eq!(issuer_of("dual-sim-b-0f1e2d3c"), Some(Node::B));
        assert_eq!(issuer_of("live-0-0f1e2d3c"), None);
    }

    #[test]
    fn a_share_id_ends_with_its_header_hash() {
        let record = ShareRecord {
            share_id: "qbrt1xyz.big-000:00ab".into(),
            session: 0,
            account: "big".into(),
            job_id: String::new(),
            issuer: None,
            outcome: "accepted".into(),
            reason: None,
            sent_ms: 0,
            answered_ms: Some(1),
            scheduled_block: false,
        };
        assert_eq!(record.header_hash(), "00ab");
        assert!(record.accepted());
    }
}
