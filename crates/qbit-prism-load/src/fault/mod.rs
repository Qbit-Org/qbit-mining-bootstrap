//! Fault injection under load (#554, #487 S16).
//!
//! `--faults` adds a `faults` side phase, after every other phase, in which
//! the faults its plan names are injected one at a time while the sessions
//! keep mining. Each fault gets a baseline window, its injection (held for
//! `hold` seconds, or, for an action such as a restart, until the frontend
//! serves again), its removal and a recovery window. The driver is polled
//! from the phase's scheduling loop and never awaited there, so the other
//! sessions keep being offered load throughout (EP-STATE).
//!
//! What a fault must show is evaluated after the run, from the same records
//! the reconciliation reads: every fault row states its windows, what it
//! measured, and each check with its pass line. A check whose figure could
//! not be measured fails, with the reason (EP-OBSERVABILITY). The rest of
//! the run's contract is unchanged: every acknowledged share must be in
//! PostgreSQL, and a committed share whose submit went unanswered mid-run
//! is still exit 5. A failed fault check is exit 9.
//!
//! The pass criteria are the ones #554's plan set, per fault, written
//! against #585's shutdown and lease behaviour; `test/e2e-scenarios.toml`
//! names them.

pub mod backlog;
pub mod database;
pub mod disk;
pub mod endpoint;
pub mod failover;
pub mod frontend;
pub mod plan;
pub mod read_tier;
pub mod rpc_relay;
mod verdict;

pub use verdict::{Check, EvalInputs, WindowShares};

use crate::{
    client::{Outcome, SessionHandle, SubmitRecord},
    frontend::Frontend,
    measure::{self, ProcessSampler},
    node::{ExternalMint, MintPurpose, TipChange},
    proxy::DelayProxy,
    run::Collected,
};
use anyhow::Result;
use backlog::CandidateBacklog;
use database::{Exhauster, Exhaustion, LockHolder};
use disk::WalDiskFull;
use failover::{Failover, FailoverControl};
use frontend::{FrontendSigkill, ReconnectStorm, RollingRestart, SigtermDrain, WorkCurrentWait};
use plan::{FaultKind, FaultPlan, Scheduled};
use rpc_relay::RpcFaultRelay;
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

/// The phase's name.
pub const PHASE: &str = "faults";

/// How long a saturated server is given to show a frontend backend
/// terminated into it before the exhaustion counts as in effect anyway.
const TERMINATION_WAIT: Duration = Duration::from_secs(5);

/// How long the settlement-lock fault waits, from its injection's start, for
/// every frontend to serve current work on a quiet cluster before it takes
/// the lock (#692). A steady baseline is there at once; after a fault that
/// landed a block it takes a few seconds.
pub const LOCK_WORK_WAIT: Duration = Duration::from_secs(60);

/// What the fault driver acts on, lent to it by the scheduling loop on each
/// poll.
pub struct FaultEnv<'a> {
    pub sessions: &'a [SessionHandle],
    pub frontends: &'a mut [Frontend],
    pub samplers: &'a [ProcessSampler],
    pub collected: &'a Arc<Mutex<Collected>>,
    pub kill_fence: &'a Arc<AtomicU64>,
    pub node: &'a dyn ExternalMint,
    pub delay_proxy: &'a DelayProxy,
}

/// What the fault driver owns for the whole phase.
pub struct FaultTools {
    /// Straight to the primary, never through the delay proxy.
    pub direct_url: String,
    /// The frontends' URL, through the delay proxy.
    pub proxied_url: String,
    /// The harness's side pool, for reads.
    pub side: PgPool,
    pub relay: Arc<RpcFaultRelay>,
    pub ready_limit: Duration,
    pub drain_limit: Duration,
    pub slow_db_delay_ms: u64,
    pub lease_wait_seconds: u64,
    pub storm_fraction: f64,
    pub seed: u64,
    /// The managed cluster and the writer endpoint, for the database
    /// failover and the full WAL volume; `None` against a `--database-url`.
    pub failover: Option<Arc<FailoverControl>>,
    pub cut_seconds: u64,
    pub backlog: usize,
}

/// A spawned task whose result the driver polls without awaiting.
pub struct Spawned<T> {
    task: Option<JoinHandle<T>>,
    value: Option<T>,
}

impl<T: Send + 'static> Spawned<T> {
    /// Wait for the task, whatever it has reached: for a cleanup that must
    /// see it finished. A task that panicked or was cancelled yields none.
    pub async fn join(self) -> Option<T> {
        match (self.value, self.task) {
            (Some(value), _) => Some(value),
            (None, Some(task)) => task.await.ok(),
            (None, None) => None,
        }
    }

    pub fn spawn(future: impl Future<Output = T> + Send + 'static) -> Self {
        Self {
            task: Some(tokio::spawn(future)),
            value: None,
        }
    }

    pub fn ready(value: T) -> Self {
        Self {
            task: None,
            value: Some(value),
        }
    }

    /// Cancel the task, for a caller that stopped waiting for it.
    pub fn abort(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }

    /// The task ended without a result: it panicked or was cancelled, and
    /// `poll` will never yield one.
    pub fn lost(&self) -> bool {
        self.task.is_none() && self.value.is_none()
    }

    /// The result, once the task has finished. A task that panicked or was
    /// cancelled never yields one; the caller's own deadline covers it, or
    /// `lost` tells it apart from one still running.
    pub fn poll(&mut self) -> Option<&T> {
        if self.value.is_none() {
            let task = self.task.as_ref()?;
            if !task.is_finished() {
                return None;
            }
            let result = finished(self.task.as_mut().expect("checked above"))?;
            self.task = None;
            if let Ok(value) = result {
                self.value = Some(value);
            }
        }
        self.value.as_ref()
    }
}

/// The result of a task that `is_finished` already said is ready. Polled in
/// place: a `Pending` (the runtime's task budget) leaves the handle to be
/// polled again, rather than dropping it and the result with it.
pub(crate) fn finished<T>(task: &mut JoinHandle<T>) -> Option<Result<T, tokio::task::JoinError>> {
    use std::task::{Context, Poll};
    let mut context = Context::from_waker(std::task::Waker::noop());
    match std::pin::Pin::new(task).poll(&mut context) {
        Poll::Ready(result) => Some(result),
        Poll::Pending => None,
    }
}

/// A found block's outbox row, as far as a fault's verdict reads it.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CandidateRow {
    pub state: String,
    pub offer_outcome: Option<String>,
    pub claim_instance_id: Option<String>,
}

impl CandidateRow {
    pub async fn read(pool: PgPool, block_hash: String) -> Result<Option<Self>> {
        let row: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT state, offer_outcome, claim_instance_id \
             FROM qbit_block_candidate_outbox WHERE block_hash = $1",
        )
        .bind(&block_hash)
        .fetch_optional(&pool)
        .await?;
        Ok(row.map(|(state, offer_outcome, claim_instance_id)| Self {
            state,
            offer_outcome,
            claim_instance_id,
        }))
    }

    /// The landing finished: the audit is landed and the block submitted.
    pub fn landed(&self) -> bool {
        self.state == "submitted"
    }
}

/// One fault's injector.
enum Action {
    SlowDatabase {
        applied: bool,
        observed: Option<Spawned<Result<f64>>>,
        observed_ms: Option<f64>,
        error: Option<String>,
    },
    SettlementLock {
        /// Every frontend's work and what the cluster is still settling,
        /// read until the work is current and the cluster quiet: the lock is
        /// taken only then (#692).
        work: WorkCurrentWait,
        holder: Option<LockHolder>,
        mint_at: Option<Instant>,
        minted: Option<Instant>,
        tip: Option<TipChange>,
        stall: Option<StallSampler>,
    },
    PoolExhaustion {
        warm: Option<Spawned<()>>,
        exhauster: Option<Exhauster>,
        outcome: Option<Exhaustion>,
        acquires_before: Option<Spawned<Vec<(String, String)>>>,
        acquires_after: Option<Spawned<Vec<(String, String)>>>,
    },
    FrontendSigkill(Box<FrontendSigkill>),
    SigtermDrain(Box<SigtermDrain>),
    RollingRestart(Box<RollingRestart>),
    ReconnectStorm(ReconnectStorm),
    Failover(Box<Failover>),
    WalDiskFull(Box<WalDiskFull>),
    CandidateBacklog(Box<CandidateBacklog>),
}

impl Action {
    /// Stop a fault's own scrapers once its recovery window has closed, so
    /// they do not go on loading every frontend's `/metrics` through the
    /// faults that follow.
    fn stop_sampling(&mut self) {
        match self {
            Self::SettlementLock {
                stall: Some(stall), ..
            } => stall.stop(),
            Self::WalDiskFull(full) => full.stop_sampling(),
            _ => {}
        }
    }

    fn new(kind: FaultKind, tools: &FaultTools) -> Result<Self> {
        Ok(match kind {
            FaultKind::SlowDatabase => Self::SlowDatabase {
                applied: false,
                observed: None,
                observed_ms: None,
                error: None,
            },
            FaultKind::SettlementLock => Self::SettlementLock {
                work: WorkCurrentWait::quiet(),
                holder: None,
                mint_at: None,
                minted: None,
                tip: None,
                stall: None,
            },
            FaultKind::PoolExhaustion => Self::PoolExhaustion {
                warm: None,
                exhauster: None,
                outcome: None,
                acquires_before: None,
                acquires_after: None,
            },
            FaultKind::FrontendSigkill => Self::FrontendSigkill(Box::default()),
            FaultKind::SigtermDrain => Self::SigtermDrain(Box::default()),
            FaultKind::RollingRestart => Self::RollingRestart(Box::default()),
            FaultKind::ReconnectStorm => Self::ReconnectStorm(ReconnectStorm::new()),
            FaultKind::PrimaryKill => {
                Self::Failover(Box::new(Failover::new(failover::Mode::Async, tools)?))
            }
            FaultKind::PrimarySwitch => {
                Self::Failover(Box::new(Failover::new(failover::Mode::Fenced, tools)?))
            }
            FaultKind::BlockFailover => {
                Self::Failover(Box::new(Failover::new(failover::Mode::Block, tools)?))
            }
            FaultKind::WalDiskFull => Self::WalDiskFull(Box::new(WalDiskFull::new(tools)?)),
            FaultKind::CandidateBacklog => {
                Self::CandidateBacklog(Box::new(CandidateBacklog::new(tools.backlog)))
            }
        })
    }
}

enum Stage {
    Gap { until: Instant },
    Baseline { until: Instant },
    Injecting,
    Holding { until: Instant },
    Removing,
    Recovering { until: Instant },
}

/// One fault as it ran.
pub struct FaultRun {
    pub ordinal: usize,
    pub kind: FaultKind,
    pub gap_seconds: u64,
    pub baseline_start: Option<Instant>,
    pub inject_start: Option<Instant>,
    /// When the fault was in effect: the lock taken, the slots filled, the
    /// delay observed; for an action, when the frontend served again.
    pub injected_at: Option<Instant>,
    pub removed_at: Option<Instant>,
    pub recovery_end: Option<Instant>,
    action: Action,
    /// Why the injector could not do what the fault asks, if it could not.
    pub problems: Vec<String>,
}

/// The run's fault driver: the plan's faults, one at a time.
pub struct FaultDriver {
    plan: FaultPlan,
    schedule: Vec<Scheduled>,
    tools: FaultTools,
    next: usize,
    current: Option<(FaultRun, Stage)>,
    pub runs: Vec<FaultRun>,
    pub started: Instant,
}

impl FaultDriver {
    pub fn new(plan: FaultPlan, tools: FaultTools) -> Self {
        let schedule = plan.schedule();
        Self {
            plan,
            schedule,
            tools,
            next: 0,
            current: None,
            runs: Vec::new(),
            started: Instant::now(),
        }
    }

    pub fn plan(&self) -> &FaultPlan {
        &self.plan
    }

    pub fn relay(&self) -> &Arc<RpcFaultRelay> {
        &self.tools.relay
    }

    /// Every fault has run and recovered.
    pub fn finished(&self) -> bool {
        self.current.is_none() && self.next >= self.schedule.len()
    }

    /// The frontends a fault has taken down on purpose, which the loop's
    /// exit check must not read as a crash.
    pub fn planned_outages(&self) -> Vec<usize> {
        match &self.current {
            Some((run, _)) => match &run.action {
                Action::SigtermDrain(drain) => drain.outage().into_iter().collect(),
                Action::RollingRestart(rolling) => rolling.outage().into_iter().collect(),
                Action::FrontendSigkill(kill) => kill.outage().into_iter().collect(),
                Action::CandidateBacklog(backlog) => backlog.outages(),
                _ => Vec::new(),
            },
            None => Vec::new(),
        }
    }

    /// Advance without blocking. An error means the run cannot go on (a
    /// frontend that would not come back); the caller aborts the phase.
    pub fn poll(&mut self, env: &mut FaultEnv<'_>) -> Result<()> {
        let now = Instant::now();
        if self.current.is_none() {
            let Some(scheduled) = self.schedule.get(self.next).copied() else {
                return Ok(());
            };
            let ordinal = self.next;
            self.next += 1;
            self.current = Some((
                FaultRun {
                    ordinal,
                    kind: scheduled.kind,
                    gap_seconds: scheduled.gap_seconds,
                    baseline_start: None,
                    inject_start: None,
                    injected_at: None,
                    removed_at: None,
                    recovery_end: None,
                    // Built when its injection starts, so a lock holder
                    // connects only then.
                    action: Action::ReconnectStorm(ReconnectStorm::new()),
                    problems: Vec::new(),
                },
                Stage::Gap {
                    until: now + Duration::from_secs(scheduled.gap_seconds),
                },
            ));
        }
        let (run, stage) = self.current.as_mut().expect("set above");
        let tools = &self.tools;
        let plan = &self.plan;
        loop {
            match stage {
                Stage::Gap { until } => {
                    if Instant::now() < *until {
                        return Ok(());
                    }
                    run.baseline_start = Some(Instant::now());
                    *stage = Stage::Baseline {
                        until: Instant::now() + Duration::from_secs(plan.baseline_seconds),
                    };
                }
                Stage::Baseline { until } => {
                    if Instant::now() < *until {
                        return Ok(());
                    }
                    run.inject_start = Some(Instant::now());
                    run.action = Action::new(run.kind, tools)?;
                    *stage = Stage::Injecting;
                }
                Stage::Injecting => {
                    if !inject(run, env, tools, plan)? {
                        return Ok(());
                    }
                    let injected = run.injected_at.unwrap_or_else(Instant::now);
                    run.injected_at = Some(injected);
                    *stage = if run.kind.is_action() {
                        Stage::Removing
                    } else {
                        Stage::Holding {
                            until: injected + Duration::from_secs(plan.hold_seconds),
                        }
                    };
                }
                Stage::Holding { until } => {
                    hold(run, env);
                    if Instant::now() < *until {
                        return Ok(());
                    }
                    *stage = Stage::Removing;
                }
                Stage::Removing => {
                    if !remove(run, env)? {
                        return Ok(());
                    }
                    let removed = run.removed_at.unwrap_or_else(Instant::now);
                    run.removed_at = Some(removed);
                    *stage = Stage::Recovering {
                        until: removed + Duration::from_secs(plan.recovery_seconds),
                    };
                }
                Stage::Recovering { until } => {
                    let settled = settle(run, env, tools)?;
                    if Instant::now() < *until || !settled {
                        return Ok(());
                    }
                    run.recovery_end = Some(Instant::now());
                    let (mut run, _) = self.current.take().expect("in progress");
                    run.action.stop_sampling();
                    self.runs.push(run);
                    return Ok(());
                }
            }
        }
    }

    /// Stop whatever is still running at the phase's end: release a lock,
    /// return the slots, take the delay off. The fault's row then says it
    /// did not finish.
    pub fn abandon(&mut self, env: &mut FaultEnv<'_>) {
        if let Some((mut run, _)) = self.current.take() {
            match &mut run.action {
                Action::SlowDatabase { .. } => env.delay_proxy.set_delay_millis(0),
                Action::SettlementLock {
                    holder: Some(holder),
                    ..
                } => holder.release(),
                Action::PoolExhaustion {
                    exhauster: Some(exhauster),
                    ..
                } => exhauster.stop(),
                Action::WalDiskFull(full) => full.abandon(),
                Action::Failover(failover) => failover.abandon(&self.tools),
                Action::CandidateBacklog(backlog) => backlog.abandon(env, &self.tools),
                _ => {}
            }
            run.action.stop_sampling();
            self.tools.relay.disarm_all();
            self.tools.relay.release_held();
            self.tools.relay.set_refusing(false);
            run.problems
                .push("the phase ended before this fault finished its recovery window".into());
            self.runs.push(run);
        }
        // Faults the phase never reached are listed, so a truncated plan
        // cannot read as a complete one.
        while let Some(scheduled) = self.schedule.get(self.next).copied() {
            self.runs.push(FaultRun {
                ordinal: self.next,
                kind: scheduled.kind,
                gap_seconds: scheduled.gap_seconds,
                baseline_start: None,
                inject_start: None,
                injected_at: None,
                removed_at: None,
                recovery_end: None,
                action: Action::ReconnectStorm(ReconnectStorm::new()),
                problems: vec!["never started: the phase ended first".into()],
            });
            self.next += 1;
        }
    }

    /// The shares whose fate a database fault's verdict explains, which the
    /// reconciliation excuses as that fault's own: every acknowledged share a
    /// failover's verdict proves lies in the replication gap (each listed in
    /// its row), and every share answered `ledger-outcome-unknown` while the
    /// primary was going or down, which is the honest answer to a commit
    /// whose reply was lost with it. Any other loss is still a durability
    /// finding.
    pub fn excused_shares(
        &self,
        submits: &[SubmitRecord],
        committed: &BTreeSet<String>,
    ) -> BTreeSet<String> {
        let mut excused = BTreeSet::new();
        let mut windows = Vec::new();
        for run in &self.runs {
            match &run.action {
                Action::Failover(failover) => {
                    if let Some(from) = run.baseline_start {
                        excused.extend(failover.excused(submits, committed, from));
                    }
                    windows.extend(failover.outage());
                }
                Action::WalDiskFull(full) => {
                    if let Some(down) = full.fill_started_at() {
                        windows.push((
                            down,
                            full.up_at.unwrap_or_else(Instant::now) + failover::SERVE_BOUND,
                        ));
                    }
                }
                _ => {}
            }
        }
        for record in submits {
            if let (Outcome::Rejected(rejection), Some(answered)) =
                (&record.outcome, record.responded)
            {
                if crate::classify::is_outcome_unknown(rejection)
                    && windows
                        .iter()
                        .any(|(from, to)| answered >= *from && answered <= *to)
                {
                    excused.insert(record.share_id.clone());
                }
            }
        }
        excused
    }

    /// The indeterminate shares every SIGKILL's census found, which the
    /// reconciliation excuses as the kill's own.
    pub fn kill_indeterminate(&self) -> Vec<SubmitRecord> {
        self.runs
            .iter()
            .filter_map(|run| match &run.action {
                Action::FrontendSigkill(kill) => Some(kill.indeterminate.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }
}

fn inject(
    run: &mut FaultRun,
    env: &mut FaultEnv<'_>,
    tools: &FaultTools,
    plan: &FaultPlan,
) -> Result<bool> {
    match &mut run.action {
        Action::SlowDatabase {
            applied,
            observed,
            observed_ms,
            error,
        } => {
            if !*applied {
                env.delay_proxy.set_delay_millis(tools.slow_db_delay_ms);
                *applied = true;
                let url = tools.proxied_url.clone();
                *observed = Some(Spawned::spawn(async move {
                    crate::proxy::measure_select1_millis(&url, 5).await
                }));
            }
            let Some(result) = observed.as_mut().and_then(Spawned::poll) else {
                return Ok(false);
            };
            match result {
                Ok(median) => *observed_ms = Some(*median),
                Err(failure) => *error = Some(format!("{failure:#}")),
            }
            run.injected_at = Some(Instant::now());
            Ok(true)
        }
        Action::SettlementLock {
            work,
            holder,
            mint_at,
            stall,
            ..
        } => {
            let holder = match holder {
                Some(holder) => holder,
                None => {
                    // Only on current work (#692). A fault before this one
                    // that landed a block bumped the payout revision, and
                    // until every frontend publishes work at it, shares on
                    // the old work are refused stale-job. A hold taken first
                    // blocks that publication too, so it would measure the
                    // landing, not the lock; so would a landing still to
                    // come, or a hold queued behind a settlement under way.
                    // The verdict fails a hold that had to start without it.
                    let started = run.inject_start.unwrap_or_else(Instant::now);
                    if !work.poll(started + LOCK_WORK_WAIT, env, tools) {
                        return Ok(false);
                    }
                    holder.insert(LockHolder::start(
                        tools.direct_url.clone(),
                        database::SETTLEMENT_LOCK_KEY,
                    ))
                }
            };
            match holder.poll_acquired() {
                Ok(None) => Ok(false),
                Ok(Some(at)) => {
                    run.injected_at = Some(at);
                    *mint_at = Some(at + Duration::from_secs(plan.hold_seconds) / 2);
                    *stall = Some(StallSampler::start(
                        env.frontends
                            .iter()
                            .map(|child| (child.spec.instance_id.clone(), child.metrics_url()))
                            .collect(),
                    ));
                    Ok(true)
                }
                Err(error) => {
                    run.problems.push(format!("{error:#}"));
                    Ok(true)
                }
            }
        }
        Action::PoolExhaustion {
            warm,
            exhauster,
            outcome,
            acquires_before,
            ..
        } => {
            if warm.is_none() {
                // The side pool keeps its connections through the
                // exhaustion, so the harness can still read.
                let side = tools.side.clone();
                *warm = Some(Spawned::spawn(async move {
                    let mut held = Vec::new();
                    for _ in 0..side.options().get_max_connections() {
                        if let Ok(connection) = side.acquire().await {
                            held.push(connection);
                        }
                    }
                }));
                *acquires_before = Some(Spawned::spawn(scrape_acquires(env)));
                return Ok(false);
            }
            if warm.as_mut().and_then(Spawned::poll).is_none()
                || acquires_before.as_mut().and_then(Spawned::poll).is_none()
            {
                return Ok(false);
            }
            let exhauster = exhauster
                .get_or_insert_with(|| Exhauster::start(tools.direct_url.clone(), "load-fe-"));
            let progress = exhauster.progress();
            if let Some(saturated) = progress.saturated_at {
                // In effect once the frontends' idle backends were
                // terminated into the full server. A server with none idle
                // to terminate is still full: the fault goes on, and its
                // check says no frontend was displaced.
                if progress.terminated.is_empty()
                    && progress.error.is_none()
                    && saturated.elapsed() < TERMINATION_WAIT
                {
                    return Ok(false);
                }
                run.injected_at = Some(saturated);
                return Ok(true);
            }
            if let Some(finished) = exhauster.poll_finished() {
                run.problems.push(format!(
                    "the server was never saturated: {}",
                    finished
                        .error
                        .clone()
                        .unwrap_or_else(|| "no refusal".into())
                ));
                *outcome = Some(finished);
                return Ok(true);
            }
            Ok(false)
        }
        Action::FrontendSigkill(kill) => kill.poll(env, tools),
        Action::SigtermDrain(drain) => drain.poll(env, tools),
        Action::RollingRestart(rolling) => rolling.poll(env, tools),
        Action::Failover(failover) => {
            let done = failover.poll(env, tools)?;
            if done {
                run.injected_at = failover.killed_at;
            }
            Ok(done)
        }
        Action::WalDiskFull(full) => {
            let done = full.poll_inject(env)?;
            if done {
                run.injected_at = full.down_at.or(full.filled_at);
            }
            Ok(done)
        }
        Action::CandidateBacklog(backlog) => {
            let done = backlog.poll(env, tools)?;
            if done {
                run.injected_at = backlog.signalled_at;
            }
            Ok(done)
        }
        Action::ReconnectStorm(storm) => {
            storm.start(env, tools.storm_fraction, tools.seed, run.ordinal);
            run.injected_at = storm.departed_at;
            Ok(true)
        }
    }
}

fn hold(run: &mut FaultRun, env: &mut FaultEnv<'_>) {
    if let Action::WalDiskFull(full) = &mut run.action {
        full.poll_hold();
    }
    if let Action::SettlementLock {
        mint_at,
        minted,
        tip,
        ..
    } = &mut run.action
    {
        if minted.is_none() && mint_at.is_some_and(|at| Instant::now() >= at) {
            *minted = Some(Instant::now());
            *tip = env.node.mint_external(MintPurpose::Fault);
        }
    }
}

fn remove(run: &mut FaultRun, env: &mut FaultEnv<'_>) -> Result<bool> {
    match &mut run.action {
        Action::SlowDatabase { .. } => {
            env.delay_proxy.set_delay_millis(0);
            run.removed_at = Some(Instant::now());
            Ok(true)
        }
        Action::SettlementLock { holder, .. } => {
            let Some(holder) = holder else {
                // The injection ends only once the holder has started.
                run.problems
                    .push("removed without a lock holder ever started".into());
                run.removed_at = Some(Instant::now());
                return Ok(true);
            };
            holder.release();
            match holder.poll_released()? {
                Some(at) => {
                    if let Some(error) = holder.error() {
                        run.problems.push(format!("lock holder: {error}"));
                    }
                    run.removed_at = Some(at);
                    Ok(true)
                }
                None => Ok(false),
            }
        }
        Action::PoolExhaustion {
            exhauster,
            outcome,
            acquires_after,
            ..
        } => {
            if let Some(active) = exhauster.as_mut() {
                active.stop();
                match active.poll_finished() {
                    Some(finished) => {
                        *outcome = Some(finished);
                        *exhauster = None;
                        run.removed_at = Some(Instant::now());
                    }
                    None => return Ok(false),
                }
            }
            // The frontends' acquire counters, read once they can connect
            // again, for the evidence.
            let scrape = acquires_after.get_or_insert_with(|| Spawned::spawn(scrape_acquires(env)));
            Ok(scrape.poll().is_some())
        }
        // An action is over once the frontend serves again: for a failover,
        // once the endpoint has moved and a new standby streams; for the
        // backlog, once every frontend is back and the relay healed.
        Action::FrontendSigkill(_)
        | Action::SigtermDrain(_)
        | Action::RollingRestart(_)
        | Action::Failover(_)
        | Action::CandidateBacklog(_) => {
            run.removed_at = Some(Instant::now());
            Ok(true)
        }
        Action::WalDiskFull(full) => {
            if !full.poll_remove(env)? {
                return Ok(false);
            }
            run.removed_at = Some(full.up_at.unwrap_or_else(Instant::now));
            Ok(true)
        }
        Action::ReconnectStorm(storm) => {
            // The storm is over once every stormed session is back, or once
            // it had its return and work budgets.
            let limit = storm.departed_at.unwrap_or_else(Instant::now)
                + frontend::STORM_RETURN
                + frontend::STORM_WORK_BUDGET;
            if storm.all_returned(env) || Instant::now() >= limit {
                run.removed_at = Some(Instant::now());
                return Ok(true);
            }
            Ok(false)
        }
    }
}

/// Anything a fault still waits for once its recovery window has passed:
/// the SIGKILL's landing through the candidate lease, the drained block's
/// landing and every frontend's work at the revision it committed (#686), a
/// failover's census of the new primary and its block's landing, the
/// backlog's settling. The next fault's gap starts only after it.
fn settle(run: &mut FaultRun, env: &FaultEnv<'_>, tools: &FaultTools) -> Result<bool> {
    match &mut run.action {
        Action::FrontendSigkill(kill) => kill.poll_landing(tools),
        Action::SigtermDrain(drain) => Ok(drain.poll_settle(env, tools)),
        Action::Failover(failover) => failover.poll_settle(env, tools),
        Action::CandidateBacklog(backlog) => Ok(backlog.poll_settle(env, tools)),
        _ => Ok(true),
    }
}

/// `database_pool_acquire_seconds_count` by outcome, per frontend, from its
/// `/metrics`.
fn scrape_acquires(
    env: &FaultEnv<'_>,
) -> impl Future<Output = Vec<(String, String)>> + Send + 'static {
    let targets: Vec<(String, String)> = env
        .frontends
        .iter()
        .map(|child| (child.spec.instance_id.clone(), child.metrics_url()))
        .collect();
    async move {
        // Evidence only: a client that cannot be built reads as no scrape.
        let Ok(client) = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
        else {
            return Vec::new();
        };
        let mut texts = Vec::new();
        for (instance, url) in targets {
            let text = match client.get(&url).send().await {
                Ok(response) => response.text().await.unwrap_or_default(),
                Err(_) => String::new(),
            };
            texts.push((instance, text));
        }
        texts
    }
}

/// The sum of every sample of `metric` whose labels contain `label`, from a
/// Prometheus text exposition. `None` when no such sample is exposed.
pub fn metric_sum(text: &str, metric: &str, label: Option<&str>) -> Option<f64> {
    let mut found = None;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some((name_and_labels, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let name = name_and_labels.split('{').next().unwrap_or(name_and_labels);
        if !name.ends_with(metric) {
            continue;
        }
        if let Some(label) = label {
            if !name_and_labels.contains(label) {
                continue;
            }
        }
        if let Ok(value) = value.parse::<f64>() {
            *found.get_or_insert(0.0) += value;
        }
    }
    found
}

/// Samples each frontend's `work_refresh_stalled_seconds` gauge every second.
/// Each sample: the frontend, when it was taken, and the gauge's value
/// (`None` when the scrape failed or did not expose it).
type StallSamples = Arc<Mutex<Vec<(String, Instant, Option<f64>)>>>;

pub struct StallSampler {
    samples: StallSamples,
    task: JoinHandle<()>,
}

impl Drop for StallSampler {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub const STALL_GAUGE: &str = "work_refresh_stalled_seconds";

/// The pool-acquire outcomes the evidence reads from `/metrics`.
const ACQUIRE_OUTCOMES: [&str; 3] = ["success", "timeout", "error"];

impl StallSampler {
    fn start(targets: Vec<(String, String)>) -> Self {
        let samples = Arc::new(Mutex::new(Vec::new()));
        let task = {
            let samples = samples.clone();
            tokio::spawn(async move {
                let Ok(client) = reqwest::Client::builder()
                    .timeout(Duration::from_secs(2))
                    .build()
                else {
                    return;
                };
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tick.tick().await;
                    for (instance, url) in &targets {
                        let at = Instant::now();
                        let value = match client.get(url).send().await {
                            Ok(response) => response
                                .text()
                                .await
                                .ok()
                                .and_then(|text| metric_sum(&text, STALL_GAUGE, None)),
                            Err(_) => None,
                        };
                        samples.lock().expect("stall samples lock").push((
                            instance.clone(),
                            at,
                            value,
                        ));
                    }
                }
            })
        };
        Self { samples, task }
    }

    fn samples(&self) -> Vec<(String, Instant, Option<f64>)> {
        self.samples.lock().expect("stall samples lock").clone()
    }

    /// Stop scraping; the samples taken so far are kept for the verdict.
    fn stop(&self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_gate_shows_one_row_per_fault_and_fails_the_one_that_missed() {
        let report = json!({
            "faults": {
                "faults": [
                    {"ordinal": 0, "fault": "sigterm-drain", "pass": true,
                     "checks": [{"name": "exited 0", "pass": true, "detail": ""}]},
                    {"ordinal": 1, "fault": "settlement-lock", "pass": false,
                     "checks": [{"name": "no share waited on the lock holder", "pass": false,
                                 "detail": ""}]},
                ],
                "read_tier_over_the_phase": null,
                "read_tier_error": "public-api exited",
            },
        });
        let checks = crate::gate::evaluate(
            &report,
            Some(0),
            &crate::gate::Budgets {
                phases: None,
                max_shortfall: 0,
                max_rejected_valid_shares: None,
                max_unanswered_submits: None,
                tip_last_notify_p99_ms: None,
                d1_verdict_table: false,
                churn_tip_last_notify_p99_ms: None,
                new_session_first_job_p99_ms: None,
            },
        );
        let row = |name: &str| {
            checks
                .iter()
                .find(|check| check.name == name)
                .unwrap_or_else(|| panic!("no {name} row"))
        };
        assert_eq!(row("fault 0: sigterm-drain").pass, Some(true));
        let failed = row("fault 1: settlement-lock");
        assert_eq!(failed.pass, Some(false));
        assert!(failed
            .observed
            .contains("no share waited on the lock holder"));
        let tier = row("read tier throughout the fault phase");
        assert_eq!(tier.pass, Some(false));
        assert!(tier.observed.contains("public-api exited"));
    }

    #[test]
    fn a_failover_gap_is_gated_in_its_fault_row_not_as_a_missing_share() {
        let report = |missing: u64, in_gap: u64| {
            json!({
                "durability_findings": [],
                "phases": [{"name": crate::fault::PHASE, "reconciliation": {
                    "missing": missing, "unexpected": 0,
                    crate::run::MISSING_IN_A_FAILOVER_GAP: in_gap,
                }}],
            })
        };
        let budgets = crate::gate::Budgets {
            phases: None,
            max_shortfall: 0,
            max_rejected_valid_shares: None,
            max_unanswered_submits: None,
            tip_last_notify_p99_ms: None,
            d1_verdict_table: false,
            churn_tip_last_notify_p99_ms: None,
            new_session_first_job_p99_ms: None,
        };
        let row = |missing, in_gap| {
            crate::gate::evaluate(&report(missing, in_gap), Some(0), &budgets)
                .into_iter()
                .find(|check| {
                    check.name == "reconciliation: acknowledged shares missing from PostgreSQL"
                })
                .expect("the missing-share row")
        };
        let gap_only = row(150, 150);
        assert_eq!(gap_only.pass, Some(true));
        assert!(gap_only
            .observed
            .contains("150 in a failover's replication gap"));
        assert_eq!(row(151, 150).pass, Some(false), "a loss outside the gap");
        assert_eq!(row(1, 0).pass, Some(false));
    }

    #[test]
    fn a_metric_is_summed_over_its_matching_samples() {
        let text = "# HELP x\n\
                    qbit_prism_database_pool_acquire_seconds_count{outcome=\"success\",pool=\"a\"} 7\n\
                    qbit_prism_database_pool_acquire_seconds_count{outcome=\"timeout\",pool=\"a\"} 2\n\
                    qbit_prism_database_pool_acquire_seconds_count{outcome=\"success\",pool=\"b\"} 3\n\
                    qbit_prism_work_refresh_stalled_seconds 12.5\n";
        assert_eq!(
            metric_sum(
                text,
                "database_pool_acquire_seconds_count",
                Some("outcome=\"success\"")
            ),
            Some(10.0)
        );
        assert_eq!(metric_sum(text, STALL_GAUGE, None), Some(12.5));
        assert_eq!(metric_sum(text, "absent_total", None), None);
    }
}
