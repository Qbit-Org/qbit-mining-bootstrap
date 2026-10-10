//! Bounded, concurrent Stratum v1 connections on Tokio's multithread runtime.
use crate::{
    codec::{self, Job, Submission},
    ledger::SessionId,
    vardiff::{password_difficulties, Vardiff, VardiffConfig},
    waiting::Dependency,
};
use anyhow::{ensure, Context, Result};
use futures_util::future::{Fuse, FusedFuture, FutureExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    net::IpAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{interval, timeout, MissedTickBehavior},
};
// The in-crate session tests drive `request` and `deliver_job` over a socket.
#[cfg(test)]
use tokio::net::tcp::OwnedWriteHalf;

mod retained_jobs;
mod stale_grace;
pub use stale_grace::{RetentionTip, StaleGrace};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Worker {
    pub username: String,
    pub payout_address: String,
    pub worker_name: Option<String>,
    pub p2mr_program_hex: String,
}

pub struct MiningJob<C> {
    pub wire: Job,
    pub context: Arc<C>,
}

impl<C> Clone for MiningJob<C> {
    fn clone(&self) -> Self {
        Self {
            wire: self.wire.clone(),
            context: self.context.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StratumError {
    pub code: i32,
    pub message: String,
    pub reason_id: Option<String>,
}

impl StratumError {
    pub fn new(code: i32, message: impl Into<String>, reason_id: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            reason_id: Some(reason_id.into()),
        }
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(20, message, "internal-error")
    }
    pub fn backend(message: impl Into<String>) -> Self {
        Self::new(20, message, "backend-rpc-unavailable")
    }
    /// A refusal whose cause lies with the ledger database, not the node
    /// (#581).
    pub fn database(message: impl Into<String>) -> Self {
        Self::new(20, message, "backend-database-unavailable")
    }
    /// One of the session's own deadlines passed while a backend call was
    /// still waiting (#655): the database's when the call was waiting on the
    /// ledger database then, otherwise the node's, as every such timeout was
    /// before.
    pub fn timed_out(message: impl Into<String>, waiting_on: Dependency) -> Self {
        match waiting_on {
            Dependency::Database => Self::database(message),
            Dependency::Unattributed => Self::backend(message),
        }
    }
    pub fn malformed(message: impl Into<String>) -> Self {
        Self::new(20, message, "malformed-submit")
    }
    pub fn response(&self, id: Value) -> Value {
        json!({"id":id,"result":null,"error":[self.code,self.message,self.reason_id.as_ref().map(|r|json!({"reason_id":r}))]})
    }
}

impl std::fmt::Display for StratumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for StratumError {}

pub trait MiningBackend: Send + Sync + 'static {
    type Context: Send + Sync + 'static;
    /// In-memory retention hint only. Authoritative submit checks stay in the backend.
    fn observed_tip_hint(&self) -> impl Future<Output = Option<RetentionTip>> + Send {
        async { None }
    }
    /// The published work's parent and payout revision, an admission hint
    /// only (#604): a session whose newest job matches it holds current work
    /// and rebuilds through the rebuild lane. `None` when unknown, which
    /// sends every delivery to the shared admission, as before #604.
    fn published_work_hint(&self) -> impl Future<Output = Option<(String, i64)>> + Send {
        async { None }
    }
    fn health_ready(&self) -> impl Future<Output = bool> + Send {
        async { true }
    }
    fn worker_difficulty(
        &self,
        _listener: &str,
        _worker: &Worker,
        _ttl_seconds: u64,
    ) -> impl Future<Output = Result<Option<(f64, Duration)>>> + Send {
        async { Ok(None) }
    }
    fn remember_worker_difficulty(
        &self,
        _listener: &str,
        _worker: &Worker,
        _difficulty: f64,
        _share_id: Option<&str>,
        _downward_only: bool,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
    fn new_session_id(
        &self,
    ) -> impl Future<Output = std::result::Result<SessionId, StratumError>> + Send;
    fn authorize(
        &self,
        username: &str,
    ) -> impl Future<Output = std::result::Result<Worker, StratumError>> + Send;
    fn build_job(
        &self,
        worker: &Worker,
        extranonce1: &str,
        difficulty: f64,
        minimum_difficulty: f64,
    ) -> impl Future<Output = std::result::Result<MiningJob<Self::Context>, StratumError>> + Send;
    fn persist_issued_job(
        &self,
        _worker: &Worker,
        _job: &MiningJob<Self::Context>,
        _version_mask: u32,
        _ttl: Duration,
    ) -> impl Future<Output = std::result::Result<(), StratumError>> + Send {
        async { Ok(()) }
    }
    fn resume_job(
        &self,
        _worker: &Worker,
        _job_id: &str,
    ) -> impl Future<Output = std::result::Result<Option<MiningJob<Self::Context>>, StratumError>> + Send
    {
        async { Ok(None) }
    }
    fn submit(
        &self,
        worker: &Worker,
        job: &MiningJob<Self::Context>,
        submission: Submission,
        stale_grace: StaleGrace,
    ) -> impl Future<Output = std::result::Result<(), StratumError>> + Send;
}

mod share_observation;

pub type RetainedDifficulties = Arc<Mutex<HashMap<(String, String), (f64, Instant)>>>;

#[derive(Clone, Debug)]
pub struct StratumConfig {
    pub listener_name: String,
    pub resume_enabled: bool,
    pub resume_ttl_seconds: u64,
    pub resume_max_entries: usize,
    pub resume_max_start_factor: f64,
    pub retained_difficulties: RetainedDifficulties,
    pub startup_difficulty: f64,
    pub minimum_difficulty: f64,
    pub vardiff: VardiffConfig,
    pub extranonce2_size: usize,
    pub version_rolling_mask: u32,
    pub max_message_bytes: usize,
    pub max_jobs_per_connection: usize,
    pub job_retention_seconds: f64,
    pub stale_grace_seconds: f64,
    pub initial_job_timeout_seconds: f64,
    pub write_timeout_seconds: f64,
    /// Connections the kernel queues for each Stratum listener before the
    /// accept loop takes them (`PRISM_STRATUM_LISTEN_BACKLOG`); a burst
    /// beyond it has its SYNs dropped. The kernel caps it at
    /// `net.core.somaxconn`.
    pub listen_backlog: u32,
    pub connection_limit: ConnectionLimit,
    pub initial_job_limit: Arc<Semaphore>,
    /// The rebuild lane (#604): a session that already holds current work
    /// (its newest job, for its current worker, is on the published parent
    /// and payout revision) takes one of these before `initial_job_limit`
    /// and keeps it through persistence, so rebuilds hold at most a quarter of the
    /// initial-job permits (one, below four) and a first job never queues
    /// behind a whole fan-out. Sized from
    /// `PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS` by [`rebuild_lane_permits`].
    pub rebuild_job_limit: Arc<Semaphore>,
    pub max_connections_per_username: usize,
    pub username_connections: Arc<Mutex<HashMap<String, Weak<Semaphore>>>>,
    pub max_connections_per_ip: usize,
    pub ip_connections: Arc<Mutex<HashMap<IpAddr, Weak<Semaphore>>>>,
    pub session_budget_interval_seconds: f64,
    pub max_malformed_frames_per_interval: u32,
    pub max_unknown_jobs_per_interval: u32,
    pub max_authorize_attempts_per_interval: u32,
    pub stats: Arc<StratumStats>,
}

impl Default for StratumConfig {
    fn default() -> Self {
        Self {
            listener_name: "default".into(),
            resume_enabled: true,
            resume_ttl_seconds: 900,
            resume_max_entries: 8192,
            resume_max_start_factor: 1024.0,
            retained_difficulties: Arc::new(Mutex::new(HashMap::new())),
            startup_difficulty: 0.000000001,
            minimum_difficulty: 0.0,
            vardiff: VardiffConfig::default(),
            extranonce2_size: 8,
            version_rolling_mask: codec::VERSION_ROLLING_MASK,
            max_message_bytes: 16 * 1024,
            max_jobs_per_connection: 64,
            job_retention_seconds: 30.0,
            stale_grace_seconds: 3.0,
            initial_job_timeout_seconds: 30.0,
            write_timeout_seconds: 20.0,
            listen_backlog: 4096,
            connection_limit: ConnectionLimit::new(384),
            initial_job_limit: Arc::new(Semaphore::new(128)),
            rebuild_job_limit: Arc::new(Semaphore::new(rebuild_lane_permits(128))),
            max_connections_per_username: 0,
            username_connections: Arc::new(Mutex::new(HashMap::new())),
            max_connections_per_ip: 0,
            ip_connections: Arc::new(Mutex::new(HashMap::new())),
            session_budget_interval_seconds: 60.0,
            max_malformed_frames_per_interval: 0,
            max_unknown_jobs_per_interval: 0,
            max_authorize_attempts_per_interval: 0,
            stats: Arc::new(StratumStats::default()),
        }
    }
}

/// Rebuild-lane permits for `initial` initial-job permits: a quarter, at
/// least one (#604). At the default 128 that is 32, enough to keep the build
/// workers and the database pool busy with rebuilds while at least 96
/// initial-job permits stay free for sessions with no work yet. A first job
/// waits for about this many rebuilds, so a larger lane is slower for it.
/// The lane also bounds how many rebuilds share one issued-job batch
/// transaction, so while commits are slow, rebuilds complete at about the
/// lane per commit. Revisit the quarter if
/// `qbit_prism_stratum_rebuild_lane_waiters` stays high while first jobs
/// are not queueing, which would mean the lane, not first-job admission, is
/// the bottleneck.
pub const fn rebuild_lane_permits(initial: usize) -> usize {
    if initial < 4 {
        1
    } else {
        initial / 4
    }
}

/// The global Stratum admission ceiling, kept with the capacity it was created
/// with so observation never has to infer configuration from free permits.
#[derive(Clone, Debug)]
pub struct ConnectionLimit {
    permits: Arc<Semaphore>,
    capacity: usize,
}

impl ConnectionLimit {
    pub fn new(capacity: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.permits.clone().try_acquire_owned().ok()
    }
}

/// The largest value any per-session rate budget accepts. Budgets bound a
/// misbehaving session, not a legitimate one, so the ceiling only has to stay
/// far above any honest per-interval rate.
const MAX_SESSION_BUDGET: u32 = 1_000_000;

/// The per-source admission limit was already spent for this address.
struct IpLimitExceeded;

/// A per-session rate budget over a fixed window. Zero disables it, which is
/// the default and today's behavior. A budget is a rate, never a session
/// lifetime total: a whole window's worth of burst is always admitted and the
/// allowance refills in full at the next window.
#[derive(Debug)]
struct SessionBudget {
    limit: u32,
    interval: Duration,
    window_start: Instant,
    charged: u32,
}

impl SessionBudget {
    fn new(limit: u32, interval: Duration) -> Self {
        Self {
            limit,
            interval,
            window_start: Instant::now(),
            charged: 0,
        }
    }
    /// Charge exactly one event. False once this window's allowance is spent.
    fn charge(&mut self) -> bool {
        if self.limit == 0 {
            return true;
        }
        if self.window_start.elapsed() >= self.interval {
            self.window_start = Instant::now();
            self.charged = 0;
        }
        self.charged = self.charged.saturating_add(1);
        self.charged <= self.limit
    }
}

#[derive(Debug, Default)]
pub struct StratumStats {
    connections: AtomicUsize,
    authorized: AtomicUsize,
    pending_builds: AtomicUsize,
    rebuild_lane_waiters: AtomicUsize,
    job_delivery_successes: AtomicU64,
    job_delivery_failures: AtomicU64,
    job_delivery_cancellations: AtomicU64,
    accepted_submissions: AtomicU64,
    rejected_submissions: AtomicU64,
    delivered_generations: Mutex<HashMap<u64, usize>>,
    last_delivery_progress: Mutex<Option<Instant>>,
    next_observation: AtomicU64,
    initial_jobs: Mutex<HashMap<u64, Instant>>,
}

#[derive(Debug, Serialize)]
pub struct StratumStatsSnapshot {
    pub connections: usize,
    pub authorized: usize,
    pub pending_builds: usize,
    pub rebuild_lane_waiters: usize,
    pub job_delivery_successes: u64,
    pub job_delivery_failures: u64,
    pub job_delivery_cancellations: u64,
    pub accepted_submissions: u64,
    pub rejected_submissions: u64,
    pub current_generation: u64,
    pub authorized_with_current_work: usize,
    pub authorized_missing_current_work: usize,
    pub last_delivery_progress_age_seconds: Option<f64>,
}

impl StratumStats {
    /// Shares the listeners have accepted, as `snapshot` reports them; read
    /// at every metrics scrape (#581).
    pub fn accepted_submissions(&self) -> u64 {
        self.accepted_submissions.load(Ordering::Relaxed)
    }

    pub fn delivery_metrics(&self) -> crate::metrics::DeliveryMetrics {
        let jobs = self.initial_jobs.lock().unwrap();
        crate::metrics::DeliveryMetrics {
            pending_initial_jobs: Some(jobs.len() as u64),
            oldest_initial_job: Some(jobs.values().min().map_or(Duration::ZERO, Instant::elapsed)),
        }
    }

    pub fn snapshot(&self, current_generation: u64) -> StratumStatsSnapshot {
        let authorized = self.authorized.load(Ordering::Relaxed);
        let covered = self
            .delivered_generations
            .lock()
            .unwrap()
            .get(&current_generation)
            .copied()
            .unwrap_or(0)
            .min(authorized);
        StratumStatsSnapshot {
            connections: self.connections.load(Ordering::Relaxed),
            authorized,
            pending_builds: self.pending_builds.load(Ordering::Relaxed),
            rebuild_lane_waiters: self.rebuild_lane_waiters.load(Ordering::Relaxed),
            job_delivery_successes: self.job_delivery_successes.load(Ordering::Relaxed),
            job_delivery_failures: self.job_delivery_failures.load(Ordering::Relaxed),
            job_delivery_cancellations: self.job_delivery_cancellations.load(Ordering::Relaxed),
            accepted_submissions: self.accepted_submissions.load(Ordering::Relaxed),
            rejected_submissions: self.rejected_submissions.load(Ordering::Relaxed),
            current_generation,
            authorized_with_current_work: covered,
            authorized_missing_current_work: authorized - covered,
            last_delivery_progress_age_seconds: self
                .last_delivery_progress
                .lock()
                .unwrap()
                .map(|at| at.elapsed().as_secs_f64()),
        }
    }
}

struct SessionObservation {
    stats: Arc<StratumStats>,
    authorized: bool,
    generation: Option<u64>,
    observation_id: u64,
}
impl SessionObservation {
    fn new(stats: Arc<StratumStats>) -> Self {
        stats.connections.fetch_add(1, Ordering::Relaxed);
        let observation_id = stats.next_observation.fetch_add(1, Ordering::Relaxed);
        Self {
            stats,
            observation_id,
            authorized: false,
            generation: None,
        }
    }
    fn authorize(&mut self) {
        self.stats
            .initial_jobs
            .lock()
            .unwrap()
            .entry(self.observation_id)
            .or_insert_with(Instant::now);
        if let Some(previous) = self.generation.take() {
            let mut generations = self.stats.delivered_generations.lock().unwrap();
            if let Some(count) = generations.get_mut(&previous) {
                *count -= 1;
                if *count == 0 {
                    generations.remove(&previous);
                }
            }
        }
        if !self.authorized {
            self.stats.authorized.fetch_add(1, Ordering::Relaxed);
            self.authorized = true;
        }
    }
    fn delivered(&mut self, generation: u64) {
        self.stats
            .initial_jobs
            .lock()
            .unwrap()
            .remove(&self.observation_id);
        if self.generation == Some(generation) {
            return;
        }
        let mut generations = self.stats.delivered_generations.lock().unwrap();
        if let Some(previous) = self.generation {
            if let Some(count) = generations.get_mut(&previous) {
                *count -= 1;
                if *count == 0 {
                    generations.remove(&previous);
                }
            }
        }
        *generations.entry(generation).or_default() += 1;
        self.generation = Some(generation);
        *self.stats.last_delivery_progress.lock().unwrap() = Some(Instant::now());
    }
}
impl Drop for SessionObservation {
    fn drop(&mut self) {
        self.stats
            .initial_jobs
            .lock()
            .unwrap()
            .remove(&self.observation_id);
        self.stats.connections.fetch_sub(1, Ordering::Relaxed);
        if self.authorized {
            self.stats.authorized.fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(generation) = self.generation {
            let mut generations = self.stats.delivered_generations.lock().unwrap();
            if let Some(count) = generations.get_mut(&generation) {
                *count -= 1;
                if *count == 0 {
                    generations.remove(&generation);
                }
            }
        }
    }
}

/// How a job delivery ended, counted once when its observation drops.
#[derive(Clone, Copy)]
enum DeliveryOutcome {
    Success,
    Failure,
    /// Abandoned unannounced because its session ended (#621).
    Cancelled,
}

struct DeliveryObservation {
    stats: Arc<StratumStats>,
    outcome: DeliveryOutcome,
}
impl DeliveryObservation {
    fn new(stats: Arc<StratumStats>) -> Self {
        stats.pending_builds.fetch_add(1, Ordering::Relaxed);
        // Until it settles, a delivery that drops was abandoned by its
        // session (#621): a miner that leaves, or a shutdown, is not a
        // failed delivery.
        Self {
            stats,
            outcome: DeliveryOutcome::Cancelled,
        }
    }
    /// Settle as failed, handing back the error that failed it.
    fn failed(mut self, error: StratumError) -> StratumError {
        self.outcome = DeliveryOutcome::Failure;
        error
    }
}
impl Drop for DeliveryObservation {
    fn drop(&mut self) {
        self.stats.pending_builds.fetch_sub(1, Ordering::Relaxed);
        let counter = match self.outcome {
            DeliveryOutcome::Success => &self.stats.job_delivery_successes,
            DeliveryOutcome::Failure => &self.stats.job_delivery_failures,
            DeliveryOutcome::Cancelled => &self.stats.job_delivery_cancellations,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Counts one rebuild waiting for the rebuild lane (#604) until it holds a
/// permit, times out or its session goes away.
struct RebuildLaneWait(Arc<StratumStats>);
impl RebuildLaneWait {
    fn new(stats: Arc<StratumStats>) -> Self {
        stats.rebuild_lane_waiters.fetch_add(1, Ordering::Relaxed);
        Self(stats)
    }
}
impl Drop for RebuildLaneWait {
    fn drop(&mut self) {
        self.0.rebuild_lane_waiters.fetch_sub(1, Ordering::Relaxed);
    }
}

impl StratumConfig {
    pub fn from_env() -> Result<Self> {
        fn value<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
        where
            T::Err: std::fmt::Display,
        {
            match std::env::var(name).ok().filter(|s| !s.trim().is_empty()) {
                None => Ok(default),
                Some(v) => v
                    .parse()
                    .map_err(|e| anyhow::anyhow!("invalid {name}: {e}")),
            }
        }
        let mut config = Self::default();
        config.resume_enabled = crate::config::flag("PRISM_STRATUM_VARDIFF_RESUME", true)?;
        config.resume_ttl_seconds = value(
            "PRISM_STRATUM_VARDIFF_RESUME_TTL_SECONDS",
            config.resume_ttl_seconds,
        )?;
        config.resume_max_entries = value(
            "PRISM_STRATUM_VARDIFF_RESUME_MAX_ENTRIES",
            config.resume_max_entries,
        )?;
        config.resume_max_start_factor = value(
            "PRISM_STRATUM_VARDIFF_RESUME_MAX_START_FACTOR",
            config.resume_max_start_factor,
        )?;
        let share = value("PRISM_STRATUM_SHARE_DIFF", config.startup_difficulty)?;
        config.vardiff.enabled = match std::env::var("PRISM_STRATUM_VARDIFF").as_deref() {
            Ok("0" | "false" | "no" | "off") => false,
            Ok("1" | "true" | "yes" | "on") | Err(_) => true,
            Ok(_) => anyhow::bail!("invalid PRISM_STRATUM_VARDIFF boolean"),
        };
        config.startup_difficulty = if config.vardiff.enabled {
            value("PRISM_STRATUM_VARDIFF_START_DIFF", share)?
        } else {
            share
        };
        config.vardiff.minimum = value("PRISM_STRATUM_VARDIFF_MIN_DIFF", share)?;
        config.vardiff.maximum = value("PRISM_STRATUM_VARDIFF_MAX_DIFF", config.vardiff.maximum)?;
        config.vardiff.target_seconds = value(
            "PRISM_STRATUM_VARDIFF_TARGET_SECONDS",
            config.vardiff.target_seconds,
        )?;
        config.vardiff.retarget_seconds = value(
            "PRISM_STRATUM_VARDIFF_RETARGET_SECONDS",
            config.vardiff.retarget_seconds,
        )?;
        config.vardiff.max_step_up = value(
            "PRISM_STRATUM_VARDIFF_MAX_STEP_UP",
            config.vardiff.max_step_up,
        )?;
        config.vardiff.max_step_down = value(
            "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN",
            config.vardiff.max_step_down,
        )?;
        config.vardiff.ewma_alpha = value(
            "PRISM_STRATUM_VARDIFF_EWMA_ALPHA",
            config.vardiff.ewma_alpha,
        )?;
        config.vardiff.tolerance = value(
            "PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE",
            config.vardiff.tolerance,
        )?;
        config.vardiff.initial_enabled =
            crate::config::flag("PRISM_STRATUM_VARDIFF_INITIAL_CONVERGENCE", true)?;
        config.vardiff.initial_max_step_up = 64.0f64.max(config.vardiff.max_step_up);
        if config.vardiff.initial_enabled {
            config.vardiff.initial_max_step_up = value(
                "PRISM_STRATUM_VARDIFF_INITIAL_MAX_STEP_UP",
                config.vardiff.initial_max_step_up,
            )?;
            config.vardiff.initial_min_shares =
                value("PRISM_STRATUM_VARDIFF_INITIAL_MIN_SHARES", 8)?;
            config.vardiff.initial_min_step_up =
                value("PRISM_STRATUM_VARDIFF_INITIAL_MIN_STEP_UP", 4.0)?;
            config.vardiff.initial_min_seconds =
                value("PRISM_STRATUM_VARDIFF_INITIAL_MIN_SECONDS", 1.0)?;
        }
        config.extranonce2_size = value("PRISM_STRATUM_EXTRANONCE2_SIZE", config.extranonce2_size)?;
        config.max_message_bytes =
            value("PRISM_STRATUM_MAX_MESSAGE_BYTES", config.max_message_bytes)?;
        config.max_jobs_per_connection = value(
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION",
            config.max_jobs_per_connection,
        )?;
        config.job_retention_seconds = value(
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS",
            config.job_retention_seconds,
        )?;
        config.stale_grace_seconds = value(
            "PRISM_STRATUM_STALE_GRACE_SECONDS",
            config.stale_grace_seconds,
        )?;
        config.initial_job_timeout_seconds = value(
            "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS",
            config.initial_job_timeout_seconds,
        )?;
        config.write_timeout_seconds = value(
            "PRISM_STRATUM_SEND_TIMEOUT_SECONDS",
            config.write_timeout_seconds,
        )?;
        config.listen_backlog = value("PRISM_STRATUM_LISTEN_BACKLOG", config.listen_backlog)?;
        let connections = value("PRISM_STRATUM_MAX_CONNECTIONS", 384usize)?;
        let initial = value("PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS", 128usize)?;
        ensure!(connections > 0 && connections <= Semaphore::MAX_PERMITS && initial > 0 && initial <= connections,
            "Stratum pending initial job limit must be positive and no greater than connection limit");
        config.connection_limit = ConnectionLimit::new(connections);
        config.initial_job_limit = Arc::new(Semaphore::new(initial));
        config.rebuild_job_limit = Arc::new(Semaphore::new(rebuild_lane_permits(initial)));
        config.max_connections_per_username =
            value("PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME", 0usize)?;
        config.max_connections_per_ip = value("PRISM_STRATUM_MAX_CONNECTIONS_PER_IP", 0usize)?;
        config.session_budget_interval_seconds = value(
            "PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS",
            config.session_budget_interval_seconds,
        )?;
        config.max_malformed_frames_per_interval =
            value("PRISM_STRATUM_MAX_MALFORMED_FRAMES_PER_INTERVAL", 0u32)?;
        config.max_unknown_jobs_per_interval =
            value("PRISM_STRATUM_MAX_UNKNOWN_JOBS_PER_INTERVAL", 0u32)?;
        config.max_authorize_attempts_per_interval =
            value("PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL", 0u32)?;
        if let Ok(mask) = std::env::var("PRISM_VERSION_ROLLING_MASK") {
            config.version_rolling_mask = codec::version_mask_from_template(
                &json!({"versionrollingmask":mask}),
                codec::VERSION_ROLLING_MASK,
            )?;
        }
        config.validate()?;
        ensure!(
            config.startup_difficulty >= config.vardiff.minimum
                && config.startup_difficulty <= config.vardiff.maximum,
            "startup difficulty must be within vardiff bounds"
        );
        Ok(config)
    }

    pub fn highdiff_config(&self) -> Result<Option<Self>> {
        let Some(port) = std::env::var("PRISM_STRATUM_HIGHDIFF_PORT")
            .ok()
            .filter(|s| !s.trim().is_empty())
        else {
            return Ok(None);
        };
        ensure!(
            port.parse::<u16>().is_ok_and(|n| n > 0),
            "invalid PRISM_STRATUM_HIGHDIFF_PORT"
        );
        fn difficulty(name: &str, default: f64) -> Result<f64> {
            match std::env::var(name).ok().filter(|s| !s.trim().is_empty()) {
                None => Ok(default),
                Some(v) => v.parse().with_context(|| format!("invalid {name}")),
            }
        }
        let mut config = self.clone();
        config.listener_name = "highdiff".into();
        config.minimum_difficulty = difficulty("PRISM_STRATUM_HIGHDIFF_MIN_DIFF", 500_000.0)?;
        config.vardiff.minimum = config.minimum_difficulty;
        config.vardiff.maximum = difficulty("PRISM_STRATUM_HIGHDIFF_MAX_DIFF", 4_294_967_296.0)?;
        let start = difficulty("PRISM_STRATUM_HIGHDIFF_START_DIFF", 500_000.0)?;
        let share = difficulty("PRISM_STRATUM_HIGHDIFF_SHARE_DIFF", start)?;
        ensure!(
            start.is_finite() && start >= config.vardiff.minimum && start <= config.vardiff.maximum,
            "highdiff startup outside bounds"
        );
        ensure!(
            share.is_finite() && share >= config.vardiff.minimum && share <= config.vardiff.maximum,
            "highdiff share difficulty outside bounds"
        );
        config.startup_difficulty = if config.vardiff.enabled { start } else { share };
        config.validate()?;
        Ok(Some(config))
    }

    pub fn validate(&self) -> Result<()> {
        self.vardiff.validate()?;
        ensure!(
            self.resume_ttl_seconds <= 86400 && self.resume_max_entries <= 1_000_000,
            "vardiff resume retention exceeds supported bounds"
        );
        ensure!(
            self.resume_max_start_factor.is_finite() && self.resume_max_start_factor >= 1.0,
            "PRISM_STRATUM_VARDIFF_RESUME_MAX_START_FACTOR must be finite and at least 1"
        );
        ensure!(
            (1..=crate::listen::MAX_LISTEN_BACKLOG).contains(&self.listen_backlog),
            "PRISM_STRATUM_LISTEN_BACKLOG must be between 1 and {}",
            crate::listen::MAX_LISTEN_BACKLOG
        );
        ensure!(
            self.max_connections_per_username <= Semaphore::MAX_PERMITS,
            "per-username connection limit exceeds semaphore capacity"
        );
        ensure!(
            self.max_connections_per_ip <= Semaphore::MAX_PERMITS,
            "PRISM_STRATUM_MAX_CONNECTIONS_PER_IP exceeds semaphore capacity"
        );
        ensure!(
            self.session_budget_interval_seconds.is_finite()
                && self.session_budget_interval_seconds > 0.0
                && self.session_budget_interval_seconds <= 3600.0,
            "PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS must be finite, positive and at most 3600"
        );
        for (name, budget) in [
            (
                "PRISM_STRATUM_MAX_MALFORMED_FRAMES_PER_INTERVAL",
                self.max_malformed_frames_per_interval,
            ),
            (
                "PRISM_STRATUM_MAX_UNKNOWN_JOBS_PER_INTERVAL",
                self.max_unknown_jobs_per_interval,
            ),
            (
                "PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL",
                self.max_authorize_attempts_per_interval,
            ),
        ] {
            ensure!(
                budget <= MAX_SESSION_BUDGET,
                "{name} must be at most {MAX_SESSION_BUDGET}"
            );
        }
        ensure!(
            self.startup_difficulty.is_finite() && self.startup_difficulty > 0.0,
            "startup difficulty must be positive"
        );
        ensure!(
            self.minimum_difficulty.is_finite() && self.minimum_difficulty >= 0.0,
            "minimum difficulty must be nonnegative"
        );
        ensure!(
            self.minimum_difficulty <= self.vardiff.maximum,
            "listener difficulty floor exceeds maximum"
        );
        ensure!(
            self.extranonce2_size > 0 && self.extranonce2_size <= 32,
            "extranonce2 size must be between 1 and 32"
        );
        ensure!(
            self.max_message_bytes >= 256
                && self.max_message_bytes <= 1024 * 1024
                && self.max_jobs_per_connection > 0,
            "invalid Stratum message/job bounds"
        );
        for n in [
            self.job_retention_seconds,
            self.initial_job_timeout_seconds,
            self.write_timeout_seconds,
        ] {
            ensure!(
                n.is_finite() && n > 0.0,
                "Stratum timeouts must be positive"
            );
            ensure!(
                Duration::try_from_secs_f64(n).is_ok(),
                "Stratum timeout exceeds supported duration"
            );
        }
        ensure!(
            self.stale_grace_seconds.is_finite() && self.stale_grace_seconds >= 0.0,
            "invalid stale grace interval"
        );
        ensure!(
            Duration::try_from_secs_f64(self.stale_grace_seconds).is_ok(),
            "stale grace exceeds supported duration"
        );
        Ok(())
    }
}

impl StratumConfig {
    /// Per-source admission for one accepted socket, decided from the address
    /// this listener observed. `Ok(None)` means the limit is disabled. The
    /// registry holds weak references; dead keys are dropped only when a new
    /// key is inserted, so an accept from a known address stays O(1) while the
    /// map stays bounded by the live connections at the last insertion, not by
    /// every address observed. Both listeners share one registry, so an
    /// address spends one budget across the ordinary and high-difficulty ports.
    fn try_acquire_ip(
        &self,
        ip: IpAddr,
    ) -> std::result::Result<Option<OwnedSemaphorePermit>, IpLimitExceeded> {
        if self.max_connections_per_ip == 0 {
            return Ok(None);
        }
        let semaphore = {
            let mut registry = self
                .ip_connections
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match registry.get(&ip).and_then(Weak::upgrade) {
                Some(semaphore) => semaphore,
                None => {
                    registry.retain(|_, entry| entry.strong_count() > 0);
                    let semaphore = Arc::new(Semaphore::new(self.max_connections_per_ip));
                    registry.insert(ip, Arc::downgrade(&semaphore));
                    semaphore
                }
            }
        };
        semaphore
            .try_acquire_owned()
            .map(Some)
            .map_err(|_| IpLimitExceeded)
    }
}

struct IssuedJob<C> {
    job: MiningJob<C>,
    worker: Worker,
    authorization_permit: Option<Arc<OwnedSemaphorePermit>>,
    version_mask: u32,
    retired_at: Option<Instant>,
}

struct Session<C> {
    worker: Option<Worker>,
    extranonce1: Option<String>,
    _session_id: Option<SessionId>,
    miner_version_mask: Option<u32>,
    advertised_version_mask: u32,
    difficulty: f64,
    requested: Option<f64>,
    requested_minimum: Option<f64>,
    suggested: Option<f64>,
    vardiff: Vardiff,
    jobs: VecDeque<IssuedJob<C>>,
    retained: retained_jobs::RetainedJobs<C>,
    tip_work_delivered: Option<(String, Instant)>,
    retry_job: bool,
    /// A job delivery is being built and persisted while the session keeps
    /// answering submits (#621).
    delivery_in_flight: bool,
    authorization_permit: Option<Arc<OwnedSemaphorePermit>>,
    observation: SessionObservation,
    pending_retarget: Option<(f64, Vardiff)>,
    last_accepted_share: Option<(String, f64)>,
    last_hint: Option<(f64, Instant)>,
    malformed_frames: SessionBudget,
    unknown_jobs: SessionBudget,
    authorize_attempts: SessionBudget,
    budget_exceeded: Option<crate::metrics::ConnectionRefusalReason>,
}

impl<C> Session<C> {
    fn new(config: &StratumConfig, observation: SessionObservation) -> Self {
        // Validation keeps this convertible; a zero-limit budget ignores it.
        let interval = Duration::try_from_secs_f64(config.session_budget_interval_seconds)
            .unwrap_or(Duration::from_secs(60));
        Self {
            worker: None,
            extranonce1: None,
            _session_id: None,
            miner_version_mask: None,
            advertised_version_mask: 0,
            difficulty: config
                .startup_difficulty
                .clamp(config.vardiff.minimum, config.vardiff.maximum),
            requested: None,
            requested_minimum: None,
            suggested: None,
            vardiff: Vardiff::new(config.vardiff.clone()),
            jobs: VecDeque::new(),
            retained: retained_jobs::RetainedJobs::default(),
            tip_work_delivered: None,
            retry_job: false,
            delivery_in_flight: false,
            authorization_permit: None,
            observation,
            pending_retarget: None,
            last_accepted_share: None,
            last_hint: None,
            malformed_frames: SessionBudget::new(
                config.max_malformed_frames_per_interval,
                interval,
            ),
            unknown_jobs: SessionBudget::new(config.max_unknown_jobs_per_interval, interval),
            authorize_attempts: SessionBudget::new(
                config.max_authorize_attempts_per_interval,
                interval,
            ),
            budget_exceeded: None,
        }
    }

    /// Charge one budgeted event and name the breach for the request loop, the
    /// refusal counter and the operator's log. A disabled budget never
    /// breaches, so the default configuration keeps today's behavior.
    fn charge(
        &mut self,
        reason: crate::metrics::ConnectionRefusalReason,
        metrics: &crate::metrics::Metrics,
    ) -> bool {
        use crate::metrics::ConnectionRefusalReason as Reason;
        let budget = match reason {
            Reason::MalformedFrameBudget => &mut self.malformed_frames,
            Reason::UnknownJobBudget => &mut self.unknown_jobs,
            Reason::AuthorizeBudget => &mut self.authorize_attempts,
            // Admission limits are decided before a session exists.
            Reason::GlobalLimit | Reason::UsernameLimit | Reason::IpLimit => return true,
        };
        if budget.charge() {
            return true;
        }
        self.budget_exceeded = Some(reason);
        metrics.record_connection_refusal(reason);
        tracing::warn!(
            reason = reason.as_str(),
            limit = budget.limit,
            interval_seconds = budget.interval.as_secs_f64(),
            "Stratum session exceeded a per-session budget"
        );
        false
    }

    fn apply_requests(&mut self, config: &StratumConfig) {
        let floor = self
            .requested_minimum
            .unwrap_or(0.0)
            .max(config.vardiff.minimum)
            .max(config.minimum_difficulty)
            .min(config.vardiff.maximum);
        self.vardiff.config.minimum = floor;
        self.difficulty = self
            .requested
            .or(self.suggested)
            .unwrap_or(self.difficulty)
            .clamp(floor, config.vardiff.maximum);
        self.vardiff.reset();
        self.pending_retarget = None;
    }

    fn retarget(&mut self) {
        // A delivery in flight was built at the current difficulty, so a
        // retarget now would be announced with that job (#621). It waits for
        // the announcement; the next delivery or timer tick retargets.
        if self.pending_retarget.is_some() || self.delivery_in_flight {
            return;
        }
        let previous = self.vardiff.clone();
        if let Some(next) = self.vardiff.retarget(self.difficulty) {
            self.pending_retarget = Some((self.difficulty, previous));
            self.difficulty = next;
            self.retry_job = self.worker.is_some() && self.extranonce1.is_some();
        }
    }

    fn restore_retarget(&mut self) {
        if let Some((difficulty, vardiff)) = self.pending_retarget.take() {
            self.difficulty = difficulty;
            self.vardiff = vardiff;
        }
    }
}

fn retain_difficulty(
    config: &StratumConfig,
    worker: &Worker,
    difficulty: f64,
    age: Duration,
    evidence: bool,
) {
    if !config.resume_enabled || config.resume_max_entries == 0 || config.resume_ttl_seconds == 0 {
        return;
    }
    let now = Instant::now();
    let Some(recorded) = now.checked_sub(age) else {
        return;
    };
    let mut retained = config.retained_difficulties.lock().unwrap();
    let key = (config.listener_name.clone(), worker.username.clone());
    if let Some((old, at)) = retained.get_mut(&key) {
        if evidence {
            *at = recorded;
            *old = difficulty;
        } else {
            *old = old.min(difficulty);
        }
        return;
    }
    if !evidence {
        return;
    }
    if retained.len() >= config.resume_max_entries {
        if let Some(oldest) = retained
            .iter()
            .min_by_key(|(_, (_, at))| *at)
            .map(|(key, _)| key.clone())
        {
            retained.remove(&oldest);
        }
    }
    retained.insert(key, (difficulty, recorded));
}

async fn remember_difficulty<B: MiningBackend>(
    backend: &B,
    config: &StratumConfig,
    worker: &Worker,
    difficulty: f64,
    share_id: Option<&str>,
    downward_only: bool,
) {
    if !config.resume_enabled || config.resume_ttl_seconds == 0 || config.resume_max_entries == 0 {
        return;
    }
    if !downward_only && share_id.is_none() {
        return;
    }
    let persisted = timeout(Duration::from_millis(500), async {
        backend
            .remember_worker_difficulty(
                &config.listener_name,
                worker,
                difficulty,
                share_id,
                downward_only,
            )
            .await?;
        // A different frontend may already have newer accepted evidence.
        // Cache the canonical retained value and its database age, rather
        // than treating this optional write as new evidence on this host.
        backend
            .worker_difficulty(&config.listener_name, worker, config.resume_ttl_seconds)
            .await
    })
    .await;
    match persisted {
        Ok(Ok(Some((difficulty, age)))) => {
            retain_difficulty(config, worker, difficulty, age, !downward_only);
        }
        Ok(Ok(None)) => {
            config
                .retained_difficulties
                .lock()
                .unwrap()
                .remove(&(config.listener_name.clone(), worker.username.clone()));
        }
        _ => {}
    }
}

async fn write_json(
    writer: &mut (impl AsyncWrite + Unpin),
    payload: Value,
    config: &StratumConfig,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(&payload)?;
    bytes.push(b'\n');
    timeout(
        Duration::from_secs_f64(config.write_timeout_seconds),
        writer.write_all(&bytes),
    )
    .await
    .context("Stratum write timeout")??;
    Ok(())
}

async fn result(
    writer: &mut (impl AsyncWrite + Unpin),
    id: Value,
    value: Value,
    config: &StratumConfig,
) -> Result<()> {
    write_json(writer, json!({"id":id,"result":value,"error":null}), config).await
}

/// Whether `prior`, the parent and payout revision of the session's newest
/// live job for its current worker, is the published work (#604). An
/// unknown publication counts nothing as current. An admission hint only.
async fn holds_current_work<B: MiningBackend>(backend: &B, prior: Option<&(String, i64)>) -> bool {
    let Some((prior_parent, prior_revision)) = prior else {
        return false;
    };
    backend
        .published_work_hint()
        .await
        .is_some_and(|(parent, revision)| {
            // Jobs carry the template's hash lowercased; the hint may not.
            prior_parent.eq_ignore_ascii_case(&parent) && *prior_revision == revision
        })
}

/// What a job delivery is built from, read from the session as it starts.
/// Building and persisting the job borrow nothing else from the session, so
/// the session keeps answering submits meanwhile (#621). The requests it
/// answers then cannot change any of these: see [`answered_during_delivery`].
struct DeliveryInputs {
    worker: Worker,
    extranonce1: String,
    difficulty: f64,
    miner_version_mask: Option<u32>,
    /// The parent and payout revision of the session's newest live job for
    /// this worker, owned, for the rebuild lane's hint (#604). A job resumed
    /// after a reconnect is retired and is not current work.
    prior: Option<(String, i64)>,
}

impl DeliveryInputs {
    fn of<C>(session: &Session<C>) -> Option<Self> {
        let worker = session.worker.clone()?;
        let prior = session
            .jobs
            .back()
            .filter(|prior| prior.worker.username == worker.username && prior.retired_at.is_none())
            .map(|prior| {
                (
                    prior.job.wire.previousblockhash.clone(),
                    prior.job.wire.payout_revision,
                )
            });
        Some(Self {
            extranonce1: session.extranonce1.clone()?,
            difficulty: session.difficulty,
            miner_version_mask: session.miner_version_mask,
            prior,
            worker,
        })
    }
}

/// A built job, durable but not yet announced to the miner.
struct PreparedJob<C> {
    job: MiningJob<C>,
    worker: Worker,
    mask: u32,
    observation: DeliveryObservation,
}

/// Requests a session answers while its job delivery is in flight (#621):
/// `mining.submit` and `mining.get_health`, neither of which changes what
/// that job is built from or how it is announced. A submit's own retarget
/// waits for the delivery (`Session::retarget`). Every other object frame
/// (`mining.subscribe`, `mining.authorize`, `mining.configure`,
/// `mining.suggest_difficulty`, `mining.extranonce.subscribe`, any other
/// method, or a method that is missing or not a string) waits for the
/// delivery, and the session reads nothing after it until it is handled, so
/// responses keep request order and those requests see the session exactly
/// as before #621. A frame that is not a JSON object is answered at once.
fn answered_during_delivery(request: &Value) -> bool {
    matches!(
        request.get("method").and_then(Value::as_str),
        Some("mining.submit" | "mining.get_health")
    )
}

/// Handle `work`, a request answered during a delivery, while that delivery
/// keeps being polled, so it never stalls behind the request (#621). A
/// delivery that finishes meanwhile is kept in `finished` and announced
/// after the request's answer.
async fn alongside<T, D: FusedFuture>(
    work: impl Future<Output = T>,
    mut delivery: Pin<&mut D>,
    finished: &mut Option<D::Output>,
) -> T {
    tokio::pin!(work);
    loop {
        tokio::select! {
            biased;
            output = &mut work => return output,
            prepared = delivery.as_mut(), if !delivery.is_terminated() => *finished = Some(prepared),
        }
    }
}

/// Await a backend call under one of the session's own deadlines (#655): its
/// output, or, once `seconds` have passed, what the call was waiting on then,
/// which names the refusal ([`StratumError::timed_out`]). The backend marks
/// the steps it waits on the ledger database (`crate::waiting`); a call that
/// marks none keeps the node's label.
async fn within_session_deadline<F: Future>(
    seconds: f64,
    call: F,
) -> std::result::Result<F::Output, Dependency> {
    crate::waiting::timeout(Duration::from_secs_f64(seconds), call).await
}

/// Admit, build and persist one job. The session loop polls this while it
/// keeps answering submits (#621) and announces the job with
/// [`announce_job`] once it is durable. `refresh` is this delivery's own
/// receiver, for the rebuild lane's re-check (#604).
async fn prepare_job<B: MiningBackend>(
    backend: &B,
    inputs: DeliveryInputs,
    config: &StratumConfig,
    metrics: &crate::metrics::Metrics,
    mut refresh: watch::Receiver<u64>,
) -> std::result::Result<PreparedJob<B::Context>, StratumError> {
    let DeliveryInputs {
        worker,
        extranonce1,
        difficulty,
        miner_version_mask,
        prior,
    } = inputs;
    let prior = prior.as_ref();
    let observation = DeliveryObservation::new(config.stats.clone());
    // #604: a session that holds current work (its newest job, for this
    // worker, is on the published parent and payout revision) can keep
    // mining meanwhile, so its rebuild (a same-tip republication, a
    // retarget) queues in the rebuild lane first and holds that permit
    // through persistence. Rebuilds of current work therefore occupy at most
    // the lane's permits anywhere between admission and a durable job, and a
    // session without current work (a first job, a new worker, a tip change
    // or a payout revision landing) waits for those, not for every session's
    // rebuild. It takes only the shared initial-job admission, as before.
    let build = async {
        let lane = if holds_current_work(backend, prior).await {
            match config.rebuild_job_limit.try_acquire() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    let _waiting = RebuildLaneWait::new(config.stats.clone());
                    // A publication that supersedes the work while it waits
                    // (a new tip, a payout revision landing) sends it to the
                    // shared admission instead. The acquire stays pinned, so
                    // a same-tip publication keeps its place in the lane.
                    let acquire = config.rebuild_job_limit.acquire();
                    tokio::pin!(acquire);
                    loop {
                        tokio::select! {
                            permit = &mut acquire => break Some(permit
                                .map_err(|_| StratumError::internal("pool is shutting down"))?),
                            changed = refresh.changed() => {
                                if changed.is_err() || !holds_current_work(backend, prior).await {
                                    break None;
                                }
                            }
                        }
                    }
                }
            }
        } else {
            None
        };
        let _admission = config
            .initial_job_limit
            .acquire()
            .await
            .map_err(|_| StratumError::internal("pool is shutting down"))?;
        backend
            .build_job(&worker, &extranonce1, difficulty, config.minimum_difficulty)
            .await
            .map(|job| (job, lane))
    };
    let revision_build = metrics.revision_work_build();
    let (job, lane) = match within_session_deadline(config.initial_job_timeout_seconds, build).await
    {
        Ok(Ok(built)) => built,
        Ok(Err(error)) => return Err(observation.failed(error)),
        Err(waiting_on) => {
            revision_build.deadline_hit();
            return Err(observation.failed(StratumError::timed_out(
                "initial job delivery timed out",
                waiting_on,
            )));
        }
    };
    let mask = miner_version_mask.map_or(0, |miner| {
        miner & config.version_rolling_mask & job.wire.version_mask
    });
    match within_session_deadline(
        config.initial_job_timeout_seconds,
        backend.persist_issued_job(
            &worker,
            &job,
            mask,
            Duration::from_secs_f64(config.job_retention_seconds.max(config.stale_grace_seconds)),
        ),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(observation.failed(error)),
        Err(waiting_on) => {
            return Err(observation.failed(StratumError::timed_out(
                "job persistence timed out",
                waiting_on,
            )))
        }
    }
    // The job is durable. Neither a slow miner's socket nor a submit the
    // session answers before announcing it (#621) may hold the lane.
    drop(lane);
    Ok(PreparedJob {
        job,
        worker,
        mask,
        observation,
    })
}

/// Build, persist and announce one job in turn, as the session loop did
/// before #621; the in-crate session tests drive a delivery this way.
#[cfg(test)]
async fn deliver_job<B: MiningBackend>(
    backend: &B,
    session: &mut Session<B::Context>,
    writer: &mut (impl AsyncWrite + Unpin),
    config: &StratumConfig,
    metrics: &crate::metrics::Metrics,
    refresh: watch::Receiver<u64>,
) -> Result<()> {
    let Some(inputs) = DeliveryInputs::of(session) else {
        return Ok(());
    };
    session.retry_job = false;
    match prepare_job(backend, inputs, config, metrics, refresh).await {
        Ok(prepared) => announce_job(backend, session, writer, config, metrics, prepared).await,
        Err(error) => {
            session.retry_job = true;
            Err(error.into())
        }
    }
}

/// Announce a prepared job: its version mask and difficulty, then the work,
/// and retire what it replaces.
async fn announce_job<B: MiningBackend>(
    backend: &B,
    session: &mut Session<B::Context>,
    writer: &mut (impl AsyncWrite + Unpin),
    config: &StratumConfig,
    metrics: &crate::metrics::Metrics,
    prepared: PreparedJob<B::Context>,
) -> Result<()> {
    let PreparedJob {
        mut job,
        worker,
        mask,
        mut observation,
    } = prepared;
    // A failed write from here ends the session and counts as a failed
    // delivery, as it did before #621.
    observation.outcome = DeliveryOutcome::Failure;
    let worker = &worker;
    // Against the work the miner holds now: a submit answered while this job
    // was built may have resumed or pruned some (#621).
    let work_invalidated = session.jobs.back().is_none_or(|prior| {
        prior.job.wire.previousblockhash != job.wire.previousblockhash
            || prior.job.wire.payout_revision != job.wire.payout_revision
    });
    job.wire.clean_jobs = work_invalidated;
    if session.miner_version_mask.is_some() && mask != session.advertised_version_mask {
        write_json(
            writer,
            json!({"id":null,"method":"mining.set_version_mask","params":[format!("{mask:08x}")]}),
            config,
        )
        .await?;
        session.advertised_version_mask = mask;
    }
    // Difficulty precedes the work to which it applies; older jobs keep their
    // own immutable target and remain creditable during a retarget.
    write_json(
        writer,
        json!({"id":null,"method":"mining.set_difficulty","params":[job.wire.share_difficulty]}),
        config,
    )
    .await?;
    write_json(writer, job.wire.notify(), config).await?;
    metrics.revision_work_delivered(job.wire.payout_revision);
    session.observation.delivered(job.wire.refresh_generation);
    let hint = session.pending_retarget.take().map(|(previous, _)| {
        let difficulty = job.wire.share_difficulty;
        let evidence = session
            .last_accepted_share
            .as_ref()
            .filter(|(_, proved)| {
                (!session.vardiff.proposed_initial && session.vardiff.proposed_share_backed)
                    || *proved == difficulty
            })
            .map(|(share_id, _)| share_id.clone());
        let downward_only = difficulty < previous && evidence.is_none();
        session.vardiff.delivered_retarget();
        (difficulty, evidence, downward_only)
    });
    session.note_delivery(&job.wire.previousblockhash);
    let now = tokio::time::Instant::now().into_std();
    for prior in &mut session.jobs {
        prior.retired_at.get_or_insert(now);
    }
    // Publication may have advanced while persistence or notification waited.
    let retention_tip = backend.observed_tip_hint().await;
    // #478 block capture: a same-parent payout replacement no longer discards
    // the superseded jobs. They are retired to block-only work, so a block
    // found on one can still be offered while its parent is the active tip;
    // `submit_share` refuses block-only work every credit path. Previous-parent
    // jobs keep their separate, notification-anchored grace deadline.
    session.bury_superseded_same_parent(&job.wire, config, retention_tip.as_ref());
    session.make_job_room(config, retention_tip.as_ref());
    session.jobs.push_back(IssuedJob {
        job,
        worker: worker.clone(),
        authorization_permit: session.authorization_permit.clone(),
        version_mask: mask,
        retired_at: None,
    });
    // `retry_job` was consumed when this delivery started: a publication or
    // retarget seen since asks for the next one.
    observation.outcome = DeliveryOutcome::Success;
    if let Some((difficulty, evidence, downward_only)) = hint {
        remember_difficulty(
            backend,
            config,
            worker,
            difficulty,
            evidence.as_deref(),
            downward_only,
        )
        .await;
    }
    Ok(())
}

async fn request<B: MiningBackend>(
    backend: &B,
    session: &mut Session<B::Context>,
    writer: &mut (impl AsyncWrite + Unpin),
    config: &StratumConfig,
    request: Value,
    received_at: tokio::time::Instant,
    metrics: &crate::metrics::Metrics,
) -> Result<()> {
    let observed_tip = backend.observed_tip_hint().await;
    session.prune_jobs(config, observed_tip.as_ref());
    let is_submit = request.get("method").and_then(Value::as_str) == Some("mining.submit");
    let share_observation =
        share_observation::ShareObservation::begin(metrics, is_submit, received_at);
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let dispatch = async {
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| StratumError::malformed("missing method"))?;
        let empty = Vec::new();
        let params = match request.get("params") {
            None => &empty,
            Some(v) => v
                .as_array()
                .ok_or_else(|| StratumError::malformed("params must be an array"))?,
        };
        match method {
            "mining.get_health" => {
                if !params.is_empty() {
                    return Err(
                        StratumError::malformed("mining.get_health takes no parameters").into(),
                    );
                }
                let ready = timeout(Duration::from_secs(3), backend.health_ready())
                    .await
                    .unwrap_or(false);
                result(writer, id.clone(), json!({"ready":ready}), config).await?;
            }
            "mining.subscribe" => {
                if session.extranonce1.is_none() {
                    let id = within_session_deadline(
                        config.initial_job_timeout_seconds,
                        backend.new_session_id(),
                    )
                    .await
                    .map_err(|waiting_on| {
                        StratumError::timed_out("session allocation timed out", waiting_on)
                    })??;
                    // Requests are serial within a session. Publish only a
                    // successful allocation; later subscribes reuse this ID.
                    session.extranonce1 = Some(format!("{id:08x}"));
                    session._session_id = Some(id);
                }
                result(
                    writer,
                    id.clone(),
                    json!([[], session.extranonce1, config.extranonce2_size]),
                    config,
                )
                .await?;
                session.retry_job = session.worker.is_some();
            }
            "mining.authorize" => {
                let username = params.first().and_then(Value::as_str).unwrap_or("");
                // Charged before the address validation RPC: username cycling
                // on one connection is what this budget bounds. Re-authorizing
                // within the budget keeps working.
                if !session.charge(
                    crate::metrics::ConnectionRefusalReason::AuthorizeBudget,
                    metrics,
                ) {
                    return Err(StratumError {
                        code: 20,
                        message: "too many authorization attempts".into(),
                        reason_id: None,
                    }
                    .into());
                }
                let worker = within_session_deadline(
                    config.initial_job_timeout_seconds,
                    backend.authorize(username),
                )
                .await
                .map_err(|waiting_on| {
                    StratumError::timed_out("payout address validation timed out", waiting_on)
                })??;
                let same_username = session
                    .worker
                    .as_ref()
                    .is_some_and(|old| old.username == worker.username);
                let new_permit = if config.max_connections_per_username > 0 && !same_username {
                    // A retained job may still credit this username. Reuse its
                    // permit when switching back, so the session cannot either
                    // bypass admission or reject itself at a limit of one.
                    let retained = session
                        .jobs
                        .iter()
                        .find(|issued| issued.worker.username == worker.username)
                        .and_then(|issued| issued.authorization_permit.clone())
                        .or_else(|| session.retained.permit(&worker.username));
                    if let Some(permit) = retained {
                        Some(permit)
                    } else {
                        let semaphore = {
                            let mut registry =
                                config.username_connections.lock().map_err(|_| {
                                    StratumError::internal("username admission unavailable")
                                })?;
                            registry.retain(|_, entry| entry.strong_count() > 0);
                            match registry.get(&worker.username).and_then(Weak::upgrade) {
                                Some(semaphore) => semaphore,
                                None => {
                                    let semaphore = Arc::new(Semaphore::new(
                                        config.max_connections_per_username,
                                    ));
                                    registry.insert(
                                        worker.username.clone(),
                                        Arc::downgrade(&semaphore),
                                    );
                                    semaphore
                                }
                            }
                        };
                        Some(Arc::new(semaphore.try_acquire_owned().map_err(|_| {
                            metrics.record_connection_refusal(
                                crate::metrics::ConnectionRefusalReason::UsernameLimit,
                            );
                            StratumError {
                                code: 20,
                                message: "too many connections for username".into(),
                                reason_id: None,
                            }
                        })?))
                    }
                } else {
                    None
                };
                let password = params.get(1).and_then(Value::as_str).unwrap_or("");
                if session.worker.is_none()
                    && config.resume_enabled
                    && config.vardiff.enabled
                    && config.resume_ttl_seconds > 0
                    && config.resume_max_entries > 0
                {
                    let loaded = timeout(
                        Duration::from_secs(1),
                        backend.worker_difficulty(
                            &config.listener_name,
                            &worker,
                            config.resume_ttl_seconds,
                        ),
                    )
                    .await;
                    let retained = match loaded {
                        Ok(Ok(Some((difficulty, age)))) => {
                            retain_difficulty(config, &worker, difficulty, age, true);
                            Some(difficulty)
                        }
                        Ok(Ok(None)) => {
                            config
                                .retained_difficulties
                                .lock()
                                .unwrap()
                                .remove(&(config.listener_name.clone(), worker.username.clone()));
                            None
                        }
                        _ => config
                            .retained_difficulties
                            .lock()
                            .unwrap()
                            .get(&(config.listener_name.clone(), worker.username.clone()))
                            .filter(|(_, at)| {
                                at.elapsed() <= Duration::from_secs(config.resume_ttl_seconds)
                            })
                            .map(|(difficulty, _)| *difficulty),
                    };
                    if let Some(retained) =
                        retained.filter(|difficulty| difficulty.is_finite() && *difficulty > 0.0)
                    {
                        session.difficulty = retained.clamp(
                            config.vardiff.minimum,
                            config
                                .vardiff
                                .maximum
                                .min(config.startup_difficulty * config.resume_max_start_factor)
                                .max(config.vardiff.minimum),
                        );
                    }
                }
                (session.requested, session.requested_minimum) = password_difficulties(password);
                session.apply_requests(config);
                if !same_username {
                    session.authorization_permit = new_permit;
                    session.last_accepted_share = None;
                    session.last_hint = None;
                }
                session.worker = Some(worker);
                session.observation.authorize();
                session.retry_job = session.extranonce1.is_some();
                result(writer, id.clone(), json!(true), config).await?;
            }
            "mining.configure" => {
                let extensions = params.first().and_then(Value::as_array);
                let options = params.get(1).and_then(Value::as_object);
                let mut response = serde_json::Map::new();
                if let Some(extensions) = extensions {
                    for extension in extensions {
                        let Some(extension) = extension.as_str() else {
                            continue;
                        };
                        if extension == "version-rolling" {
                            let miner_mask = if let Some(value) =
                                options.and_then(|o| o.get("version-rolling.mask"))
                            {
                                codec::parse_u32_hex(value.as_str().ok_or_else(|| {
                                    StratumError::malformed("version-rolling.mask must be hex")
                                })?)
                                .map_err(|e| StratumError::malformed(e.to_string()))?
                            } else {
                                u32::MAX
                            };
                            session.miner_version_mask = Some(miner_mask);
                            let server_mask = session
                                .jobs
                                .back()
                                .map_or(config.version_rolling_mask, |j| {
                                    j.job.wire.version_mask & config.version_rolling_mask
                                });
                            let mask = miner_mask & server_mask;
                            session.advertised_version_mask = mask;
                            response.insert("version-rolling".into(), json!(mask != 0));
                            response.insert(
                                "version-rolling.mask".into(),
                                json!(format!("{mask:08x}")),
                            );
                            session.retry_job =
                                session.worker.is_some() && session.extranonce1.is_some();
                        } else {
                            response.insert(extension.into(), json!(false));
                        }
                    }
                }
                result(writer, id.clone(), Value::Object(response), config).await?;
            }
            "mining.extranonce.subscribe" => {
                result(writer, id.clone(), json!(true), config).await?
            }
            "mining.suggest_difficulty" => {
                let suggestion = params
                    .first()
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
                    .filter(|n| n.is_finite() && *n > 0.0);
                if let Some(suggestion) = suggestion {
                    session.suggested = Some(suggestion);
                    session.apply_requests(config);
                    session.retry_job = session.worker.is_some() && session.extranonce1.is_some();
                }
                result(writer, id.clone(), json!(true), config).await?;
            }
            "mining.submit" => {
                let worker = session
                    .worker
                    .as_ref()
                    .filter(|_| session.extranonce1.is_some())
                    .cloned()
                    .ok_or_else(|| {
                        StratumError::new(
                            20,
                            "worker is not authorized and subscribed",
                            "unauthorized-worker",
                        )
                    })?;
                if params.len() < 5 {
                    return Err(StratumError::malformed("submit params are incomplete").into());
                }
                let fields: Vec<&str> = params
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .ok_or_else(|| StratumError::malformed("submit params must be strings"))
                    })
                    .collect::<std::result::Result<_, _>>()?;
                if fields[0] != worker.username {
                    return Err(StratumError::new(
                        20,
                        "submit username does not match authorized username",
                        "unauthorized-worker",
                    )
                    .into());
                }
                if fields[2].len() != config.extranonce2_size * 2 {
                    return Err(StratumError::new(
                        20,
                        "unexpected extranonce2 size",
                        "invalid-extranonce",
                    )
                    .into());
                }
                if fields[3].len() != 8 || fields[4].len() != 8 {
                    return Err(StratumError::new(
                        20,
                        "ntime and nonce must be 4-byte hex strings",
                        "invalid-ntime-or-nonce",
                    )
                    .into());
                }
                if !session.jobs.iter().any(|j| j.job.wire.job_id == fields[1])
                    && session.retained.get(fields[1]).is_none()
                {
                    if let Some(job) = within_session_deadline(
                        config.initial_job_timeout_seconds,
                        backend.resume_job(&worker, fields[1]),
                    )
                    .await
                    .map_err(|waiting_on| {
                        StratumError::timed_out("job resume timed out", waiting_on)
                    })?? {
                        if job.wire.job_id != fields[1] {
                            return Err(StratumError::internal("restored job ID mismatch").into());
                        }
                        let original_mask = job.wire.version_mask;
                        // The request-entry hint can predate this resume await.
                        let retention_tip = backend.observed_tip_hint().await;
                        session.make_job_room(config, retention_tip.as_ref());
                        session.jobs.push_front(IssuedJob {
                            job,
                            worker: worker.clone(),
                            authorization_permit: session.authorization_permit.clone(),
                            version_mask: original_mask,
                            retired_at: Some(tokio::time::Instant::now().into_std()),
                        });
                    } else {
                        // Every miss cost one ledger lookup, so every miss is
                        // charged. A miss is not proof the ID is bogus: honest
                        // staleness and transient resume races land here too,
                        // and the next submit must still re-query.
                        if !session.charge(
                            crate::metrics::ConnectionRefusalReason::UnknownJobBudget,
                            metrics,
                        ) {
                            return Err(StratumError {
                                code: 20,
                                message: "too many unknown job submissions".into(),
                                reason_id: None,
                            }
                            .into());
                        }
                    }
                }
                let issued = session
                    .jobs
                    .iter()
                    .find(|j| j.job.wire.job_id == fields[1])
                    .or_else(|| session.retained.get(fields[1]))
                    .ok_or_else(|| StratumError::new(21, "stale job", "unknown-job"))?;
                // Resume may complete after the absolute lease expired. This
                // check must follow the await and is independent of grace.
                if issued
                    .job
                    .wire
                    .resume_expires_at
                    .is_some_and(|expires| tokio::time::Instant::now().into_std() >= expires)
                {
                    metrics
                        .record_stale_job_rejection(crate::metrics::StaleJobCause::ResumeExpired);
                    return Err(StratumError::new(21, "stale job", "stale-job").into());
                }
                let grace = if issued.job.wire.resume_expires_at.is_none() {
                    session.stale_grace(config)
                } else {
                    false.into()
                };
                let submission = issued
                    .job
                    .wire
                    .assemble_submission(
                        fields[2],
                        fields[3],
                        fields[4],
                        fields.get(5).copied(),
                        issued.version_mask,
                    )
                    .map_err(|e| StratumError::malformed(format!("malformed submit: {e}")))?;
                let share_id = format!("{}:{}", issued.worker.username, submission.block_hash_hex);
                let proved_difficulty = if submission.share_pass {
                    issued.job.wire.share_difficulty
                } else {
                    codec::target_difficulty(&issued.job.wire.network_target)
                        .map_err(|error| StratumError::internal(error.to_string()))?
                };
                backend
                    .submit(&issued.worker, &issued.job, submission, grace)
                    .await?;
                session.vardiff.accepted(proved_difficulty);
                // Also in the state a failed retarget delivery restores, so a
                // share answered while it is in flight (#621) counts either way.
                if let Some((_, previous)) = &mut session.pending_retarget {
                    previous.accepted(proved_difficulty);
                }
                session.last_accepted_share = Some((share_id.clone(), proved_difficulty));
                config
                    .stats
                    .accepted_submissions
                    .fetch_add(1, Ordering::Relaxed);
                result(writer, id.clone(), json!(true), config).await?;
                share_observation.acknowledged(crate::metrics::AckResult::Accepted);
                #[cfg(feature = "soak-leak-mutant")]
                soak_leak_mutant::retain();
                if session
                    .jobs
                    .back()
                    .is_some_and(|current| current.job.wire.share_difficulty == proved_difficulty)
                    && session.last_hint.is_none_or(|(difficulty, at)| {
                        difficulty != issued.job.wire.share_difficulty
                            || at.elapsed() >= Duration::from_secs(30)
                    })
                {
                    remember_difficulty(
                        backend,
                        config,
                        &issued.worker,
                        issued.job.wire.share_difficulty,
                        Some(&share_id),
                        false,
                    )
                    .await;
                    session.last_hint = Some((issued.job.wire.share_difficulty, Instant::now()));
                }
                session.retarget();
            }
            _ => {
                return Err(StratumError::malformed(format!("unsupported method {method}")).into())
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(error) = dispatch {
        let Some(error) = error.downcast_ref::<StratumError>() else {
            return Err(error);
        };
        if is_submit {
            config
                .stats
                .rejected_submissions
                .fetch_add(1, Ordering::Relaxed);
            share_observation.rejected(error);
        }
        let malformed = error.reason_id.as_deref() == Some("malformed-submit");
        write_json(writer, error.response(id), config).await?;
        share_observation.acknowledged(crate::metrics::AckResult::Rejected);
        // Malformed requests were answered and forgotten. One frame is one
        // charge, whatever it weighs: a junk flood is a frame rate.
        if malformed {
            session.charge(
                crate::metrics::ConnectionRefusalReason::MalformedFrameBudget,
                metrics,
            );
        }
    }
    Ok(())
}

async fn session<B: MiningBackend>(
    stream: TcpStream,
    backend: Arc<B>,
    config: StratumConfig,
    refresh: watch::Receiver<u64>,
    shutdown: watch::Receiver<bool>,
    metrics: Arc<crate::metrics::Metrics>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let (reader, writer) = stream.into_split();
    serve_connection(reader, writer, backend, config, refresh, shutdown, metrics).await
}

/// Whether the session breached a per-session budget. The breach was
/// recorded and answered; the session closes after the response.
fn budget_breached<C>(session: &Session<C>) -> bool {
    let Some(reason) = session.budget_exceeded else {
        return false;
    };
    tracing::warn!(
        reason = reason.as_str(),
        "Stratum session disconnected by a per-session budget"
    );
    true
}

/// One Stratum connection's request loop over any byte stream (#575).
/// `run_listener` calls it with an accepted socket's halves; the fuzz targets
/// and property tests call it over an in-memory pipe, so the line framing and
/// session state machine they exercise are exactly the production ones.
/// Admission (the global, per-source and per-username limits taken at
/// accept) and `StratumConfig::validate` stay with the caller.
#[doc(hidden)]
pub async fn serve_connection<B: MiningBackend>(
    reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    backend: Arc<B>,
    config: StratumConfig,
    mut refresh: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<crate::metrics::Metrics>,
) -> Result<()> {
    let observation = SessionObservation::new(config.stats.clone());
    let mut session = Session::new(&config, observation);
    let mut reader = BufReader::new(reader);
    let mut buffer = Vec::new();
    let mut timer = interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // An unauthenticated peer must not retain admission indefinitely.
    let connected = Instant::now();
    // The session's job delivery in flight (#621): built and persisted while
    // the loop below keeps answering submits, then announced from it.
    let delivery = Fuse::terminated();
    tokio::pin!(delivery);
    // That delivery once it finished, until it is announced.
    let mut finished = None;
    // A request that waits for that delivery (see `answered_during_delivery`)
    // and when its frame completed. Nothing after it is read until it is
    // handled.
    let mut held: Option<(Value, tokio::time::Instant)> = None;
    // A frame over the size limit ends the session; its rejection is the
    // session's last write.
    let mut oversized = false;
    loop {
        if *shutdown.borrow() {
            break;
        }
        // A failed delivery is retried after the next event, the timer at the
        // latest, never in the pass that saw it fail.
        let mut delivery_failed = false;
        let remaining = config.max_message_bytes + 1 - buffer.len();
        let mut bounded_reader = (&mut reader).take(remaining as u64);
        // Reads come last (#621). Every other branch is ready at most once per
        // tick, publication or delivery and returns promptly, so none starves
        // a pending request, and a backlog of requests cannot hold off a
        // finished delivery, a new publication or the unauthenticated-peer
        // deadline.
        tokio::select! {
            biased;
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            prepared = &mut delivery, if !delivery.is_terminated() => finished = Some(prepared),
            changed = refresh.changed() => {
                if changed.is_err() { break; }
                session.retry_job = session.worker.is_some() && session.extranonce1.is_some();
            }
            _ = timer.tick() => {
                // A delivery in flight ends at its own deadlines first.
                if delivery.is_terminated() && session.jobs.is_empty() && session.retained.is_empty() && connected.elapsed().as_secs_f64() > config.initial_job_timeout_seconds { break; }
                let observed_tip = backend.observed_tip_hint().await;
                session.prune_jobs(&config, observed_tip.as_ref());
                session.retarget();
            }
            read = bounded_reader.read_until(b'\n',&mut buffer), if held.is_none() => {
                if read? == 0 { break; }
                if buffer.len() > config.max_message_bytes { oversized = true; break; }
                if buffer.last() != Some(&b'\n') { continue; }
                let received_at = tokio::time::Instant::now();
                let frame = std::mem::take(&mut buffer);
                match serde_json::from_slice::<Value>(&frame) {
                    Ok(value) if value.is_object() && !delivery.is_terminated() && !answered_during_delivery(&value) => held = Some((value, received_at)),
                    Ok(value) if value.is_object() => alongside(request(backend.as_ref(),&mut session,&mut writer,&config,value,received_at,&metrics), delivery.as_mut(), &mut finished).await?,
                    _ => {
                        alongside(write_json(&mut writer,StratumError::malformed("invalid JSON request").response(Value::Null),&config), delivery.as_mut(), &mut finished).await?;
                        session.charge(crate::metrics::ConnectionRefusalReason::MalformedFrameBudget,&metrics);
                    }
                }
            }
        }
        // A breach closes the session right after its answer, before any
        // work that finished meanwhile is announced (#621).
        if budget_breached(&session) {
            break;
        }
        if let Some(prepared) = finished.take() {
            session.delivery_in_flight = false;
            match prepared {
                Ok(prepared) => {
                    announce_job(
                        backend.as_ref(),
                        &mut session,
                        &mut writer,
                        &config,
                        &metrics,
                        prepared,
                    )
                    .await?
                }
                Err(_) => {
                    // Template/RPC trouble is retried on the timer while existing
                    // work remains usable. Do not disconnect a healthy miner.
                    session.restore_retarget();
                    session.retry_job = true;
                    delivery_failed = true;
                }
            }
        }
        if delivery.is_terminated() {
            if let Some((value, received_at)) = held.take() {
                metrics.observe_request_delivery_wait(received_at.elapsed());
                request(
                    backend.as_ref(),
                    &mut session,
                    &mut writer,
                    &config,
                    value,
                    received_at,
                    &metrics,
                )
                .await?;
            }
        }
        // A held request handled above may breach a budget too.
        if budget_breached(&session) {
            break;
        }
        if session.retry_job && delivery.is_terminated() && !delivery_failed {
            // A retarget that waited for the delivery just announced rides
            // this one, so back-to-back deliveries never starve vardiff.
            session.retarget();
            if let Some(inputs) = DeliveryInputs::of(&session) {
                session.retry_job = false;
                session.delivery_in_flight = true;
                delivery.set(
                    prepare_job(backend.as_ref(), inputs, &config, &metrics, refresh.clone())
                        .fuse(),
                );
            }
        }
    }
    // The session is ending: abandon a delivery still in flight before the
    // last writes, so a peer that is not reading cannot hold its admission
    // or lane permit through them (#621).
    delivery.set(Fuse::terminated());
    drop(finished);
    if oversized {
        write_json(
            &mut writer,
            StratumError::malformed("Stratum message exceeds size limit").response(Value::Null),
            &config,
        )
        .await?;
    }
    writer.shutdown().await?;
    Ok(())
}

pub async fn run_listener<B: MiningBackend>(
    listener: TcpListener,
    config: StratumConfig,
    backend: Arc<B>,
    refresh: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<crate::metrics::Metrics>,
) -> Result<()> {
    config.validate()?;
    // Both listeners share this one limit, so either may report its capacity.
    metrics.set_stratum_connection_limit(config.connection_limit.capacity());
    let mut connections = JoinSet::new();
    let sessions = Sessions {
        config: config.clone(),
        backend: backend.clone(),
        refresh: refresh.clone(),
        shutdown: shutdown.clone(),
        metrics: metrics.clone(),
    };
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            _ = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => sessions.start(accepted?, &mut connections),
        }
    }
    drain_sessions(connections).await;
    Ok(())
}

/// A dual-writer Stratum listener (3.1): bound from startup, listening only
/// while the frontend admits miners. Before the first admission and after a
/// withdrawal the address refuses every connection, so no handshake, check
/// or balancer sees a frontend that must not serve as up; a decision older
/// than `stale_after` admits nothing, so a stalled health publisher closes
/// it too. Withdrawing resets connections still queued unaccepted; sessions
/// already accepted carry on until they end or the balancer closes them.
#[allow(clippy::too_many_arguments)]
pub async fn run_gated_listener<B: MiningBackend>(
    mut address: crate::listen::ReservedAddress,
    config: StratumConfig,
    backend: Arc<B>,
    refresh: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<crate::metrics::Metrics>,
    mut admission: watch::Receiver<crate::readiness::admission::AdmissionSignal>,
    stale_after: Duration,
) -> Result<()> {
    config.validate()?;
    metrics.set_stratum_connection_limit(config.connection_limit.capacity());
    metrics.publish_stratum_listener_accepting(&config.listener_name, false);
    let mut connections = JoinSet::new();
    let sessions = Sessions {
        config: config.clone(),
        backend: backend.clone(),
        refresh: refresh.clone(),
        shutdown: shutdown.clone(),
        metrics: metrics.clone(),
    };
    // The health publisher holds the sender for the life of the process; if
    // it is gone nothing can admit again, so the listener stays closed.
    let mut decisions = true;
    'gate: loop {
        if *shutdown.borrow() {
            break;
        }
        let admits = admission
            .borrow_and_update()
            .admits_at(tokio::time::Instant::now(), stale_after);
        if !admits {
            tokio::select! {
                changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
                _ = connections.join_next(), if !connections.is_empty() => {}
                changed = admission.changed(), if decisions => decisions = changed.is_ok(),
            }
            continue;
        }
        let listener = address.listen(config.listen_backlog).with_context(|| {
            format!(
                "listen on the {} Stratum address {}",
                config.listener_name,
                address.local_addr()
            )
        })?;
        metrics.publish_stratum_listener_accepting(&config.listener_name, true);
        tracing::info!(listener = %config.listener_name, address = %address.local_addr(), "Stratum listener accepting: the frontend admits miners");
        loop {
            let stale_at = admission
                .borrow()
                .stale_at(stale_after)
                .unwrap_or_else(tokio::time::Instant::now);
            tokio::select! {
                changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break 'gate; } }
                _ = connections.join_next(), if !connections.is_empty() => {}
                changed = admission.changed(), if decisions => {
                    decisions = changed.is_ok();
                    if !decisions || !admission.borrow_and_update().admits_at(tokio::time::Instant::now(), stale_after) {
                        break;
                    }
                }
                () = tokio::time::sleep_until(stale_at) => {
                    if !admission.borrow().admits_at(tokio::time::Instant::now(), stale_after) {
                        break;
                    }
                }
                accepted = listener.accept() => sessions.start(accepted?, &mut connections),
            }
        }
        drop(listener);
        address.reserve().with_context(|| {
            format!(
                "hold the {} Stratum address {} after closing its listener",
                config.listener_name,
                address.local_addr()
            )
        })?;
        metrics.publish_stratum_listener_accepting(&config.listener_name, false);
        tracing::warn!(listener = %config.listener_name, address = %address.local_addr(), "Stratum listener refusing connections: the frontend withdrew");
    }
    metrics.publish_stratum_listener_accepting(&config.listener_name, false);
    drain_sessions(connections).await;
    Ok(())
}

/// What every accepted Stratum connection's session needs from its listener.
struct Sessions<B> {
    config: StratumConfig,
    backend: Arc<B>,
    refresh: watch::Receiver<u64>,
    shutdown: watch::Receiver<bool>,
    metrics: Arc<crate::metrics::Metrics>,
}

impl<B: MiningBackend> Sessions<B> {
    /// Admit one accepted connection under the per-source and global limits
    /// and start its session, or refuse it.
    fn start(
        &self,
        (stream, peer): (TcpStream, std::net::SocketAddr),
        connections: &mut JoinSet<()>,
    ) {
        let (config, metrics) = (&self.config, &self.metrics);
        // Decided from the observed peer address at accept, before any
        // permit, task or allocation, so a refused source costs one
        // in-memory lookup and never reaches the database.
        let ip_permit = match config.try_acquire_ip(peer.ip()) {
            Ok(permit) => permit,
            Err(IpLimitExceeded) => {
                metrics.record_connection_refusal(crate::metrics::ConnectionRefusalReason::IpLimit);
                // The address belongs in the log, never in a label.
                tracing::warn!(peer = %peer, limit = config.max_connections_per_ip, listener = %config.listener_name, "Stratum connection refused by the per-source limit");
                drop(stream);
                return;
            }
        };
        let Some(permit) = config.connection_limit.try_acquire() else {
            metrics.record_connection_refusal(crate::metrics::ConnectionRefusalReason::GlobalLimit);
            drop(stream);
            return;
        };
        let (backend, config, refresh, shutdown) = (
            self.backend.clone(),
            config.clone(),
            self.refresh.clone(),
            self.shutdown.clone(),
        );
        let metrics = metrics.clone();
        let runtime = metrics.runtime();
        connections.spawn(
            runtime.track(crate::metrics::TaskKind::StratumSession, async move {
                let _permit = permit;
                let _ip_permit = ip_permit;
                if let Err(error) =
                    session(stream, backend, config, refresh, shutdown, metrics).await
                {
                    tracing::warn!(error = %format_args!("{error:#}"), "Stratum connection ended");
                }
            }),
        );
    }
}

/// Let sessions finish after the listener stops, for at most ten seconds.
async fn drain_sessions(mut connections: JoinSet<()>) {
    if timeout(Duration::from_secs(10), async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
}

/// Subscribe and authorize exactly like a miner, then inspect the first
/// advertised difficulty. Useful for checking a rental-market listener floor.
pub async fn probe_first_difficulty(
    address: &str,
    username: &str,
    deadline: Duration,
) -> Result<f64> {
    timeout(deadline, async {
        let stream = TcpStream::connect(address).await?;
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        for payload in [
            json!({"id":1,"method":"mining.subscribe","params":["prism-self-check"]}),
            json!({"id":2,"method":"mining.authorize","params":[username,"x"]}),
        ] {
            writer.write_all(format!("{payload}\n").as_bytes()).await?;
        }
        loop {
            let mut frame = Vec::new();
            ensure!(
                (&mut reader)
                    .take(1024 * 1024 + 1)
                    .read_until(b'\n', &mut frame)
                    .await?
                    > 0,
                "Stratum probe disconnected before difficulty"
            );
            ensure!(
                frame.len() <= 1024 * 1024,
                "oversized Stratum probe response"
            );
            let value: Value = serde_json::from_slice(&frame)?;
            ensure!(
                value.get("error").is_none_or(Value::is_null),
                "Stratum probe rejected: {}",
                value["error"]
            );
            if value["method"] == "mining.set_difficulty" {
                let difficulty = value["params"][0]
                    .as_f64()
                    .context("invalid advertised difficulty")?;
                ensure!(
                    difficulty.is_finite() && difficulty > 0.0,
                    "invalid advertised difficulty"
                );
                return Ok(difficulty);
            }
        }
    })
    .await
    .context("Stratum difficulty probe timed out")?
}

#[cfg(test)]
mod stale_grace_tests;

/// Test-only (#575): keeps 16 KiB per accepted share for the life of the
/// process, so the load harness's soak can be shown to fail its resident
/// memory gate within a short run. Compiled only with the `soak-leak-mutant`
/// feature, which no build of this repository enables.
#[cfg(feature = "soak-leak-mutant")]
mod soak_leak_mutant {
    static RETAINED: std::sync::Mutex<Vec<Vec<u8>>> = std::sync::Mutex::new(Vec::new());

    pub(super) fn retain() {
        // Written, not merely allocated, so the pages are resident.
        let block = vec![0x5a_u8; 16 * 1024];
        RETAINED
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(block);
    }
}
