//! Bounded, concurrent Stratum v1 connections on Tokio's multithread runtime.
use crate::{
    codec::{self, Job, Submission},
    ledger::SessionId,
    vardiff::{password_difficulties, Vardiff, VardiffConfig},
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{interval, timeout, MissedTickBehavior},
};

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
        stale_grace_eligible: bool,
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
    pub connection_limit: Arc<Semaphore>,
    pub initial_job_limit: Arc<Semaphore>,
    pub max_connections_per_username: usize,
    pub username_connections: Arc<Mutex<HashMap<String, Weak<Semaphore>>>>,
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
            connection_limit: Arc::new(Semaphore::new(384)),
            initial_job_limit: Arc::new(Semaphore::new(128)),
            max_connections_per_username: 0,
            username_connections: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(StratumStats::default()),
        }
    }
}

#[derive(Debug, Default)]
pub struct StratumStats {
    connections: AtomicUsize,
    authorized: AtomicUsize,
    pending_builds: AtomicUsize,
    job_delivery_successes: AtomicU64,
    job_delivery_failures: AtomicU64,
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
    pub job_delivery_successes: u64,
    pub job_delivery_failures: u64,
    pub accepted_submissions: u64,
    pub rejected_submissions: u64,
    pub current_generation: u64,
    pub authorized_with_current_work: usize,
    pub authorized_missing_current_work: usize,
    pub last_delivery_progress_age_seconds: Option<f64>,
}

impl StratumStats {
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
            job_delivery_successes: self.job_delivery_successes.load(Ordering::Relaxed),
            job_delivery_failures: self.job_delivery_failures.load(Ordering::Relaxed),
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

struct DeliveryObservation {
    stats: Arc<StratumStats>,
    success: bool,
}
impl DeliveryObservation {
    fn new(stats: Arc<StratumStats>) -> Self {
        stats.pending_builds.fetch_add(1, Ordering::Relaxed);
        Self {
            stats,
            success: false,
        }
    }
}
impl Drop for DeliveryObservation {
    fn drop(&mut self) {
        self.stats.pending_builds.fetch_sub(1, Ordering::Relaxed);
        if self.success {
            self.stats
                .job_delivery_successes
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .job_delivery_failures
                .fetch_add(1, Ordering::Relaxed);
        }
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
        let connections = value("PRISM_STRATUM_MAX_CONNECTIONS", 384usize)?;
        let initial = value("PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS", 128usize)?;
        ensure!(connections > 0 && connections <= Semaphore::MAX_PERMITS && initial > 0 && initial <= connections,
            "Stratum pending initial job limit must be positive and no greater than connection limit");
        config.connection_limit = Arc::new(Semaphore::new(connections));
        config.initial_job_limit = Arc::new(Semaphore::new(initial));
        config.max_connections_per_username =
            value("PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME", 0usize)?;
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
            self.max_connections_per_username <= Semaphore::MAX_PERMITS,
            "per-username connection limit exceeds semaphore capacity"
        );
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

struct IssuedJob<C> {
    job: MiningJob<C>,
    worker: Worker,
    authorization_permit: Option<Arc<OwnedSemaphorePermit>>,
    version_mask: u32,
    retired_at: Option<Instant>,
    tip_replaced_at: Option<Instant>,
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
    retry_job: bool,
    authorization_permit: Option<Arc<OwnedSemaphorePermit>>,
    observation: SessionObservation,
    pending_retarget: Option<(f64, Vardiff)>,
    last_accepted_share: Option<(String, f64)>,
    last_hint: Option<(f64, Instant)>,
}

impl<C> Session<C> {
    fn new(config: &StratumConfig, observation: SessionObservation) -> Self {
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
            retry_job: false,
            authorization_permit: None,
            observation,
            pending_retarget: None,
            last_accepted_share: None,
            last_hint: None,
        }
    }

    fn prune_jobs(&mut self, config: &StratumConfig) {
        let now = Instant::now();
        self.jobs.retain(|issued| {
            issued
                .job
                .wire
                .resume_expires_at
                .is_none_or(|expires| now < expires)
                && issued.retired_at.is_none_or(|when| {
                    now.duration_since(when).as_secs_f64()
                        <= config.job_retention_seconds.max(config.stale_grace_seconds)
                })
        });
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
        if self.pending_retarget.is_some() {
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
    writer: &mut OwnedWriteHalf,
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
    writer: &mut OwnedWriteHalf,
    id: Value,
    value: Value,
    config: &StratumConfig,
) -> Result<()> {
    write_json(writer, json!({"id":id,"result":value,"error":null}), config).await
}

async fn deliver_job<B: MiningBackend>(
    backend: &B,
    session: &mut Session<B::Context>,
    writer: &mut OwnedWriteHalf,
    config: &StratumConfig,
) -> Result<()> {
    let Some(extranonce1) = session.extranonce1.as_deref() else {
        return Ok(());
    };
    let Some(worker) = session.worker.as_ref() else {
        return Ok(());
    };
    let mut observation = DeliveryObservation::new(config.stats.clone());
    let build = async {
        let _admission = config
            .initial_job_limit
            .acquire()
            .await
            .map_err(|_| StratumError::internal("pool is shutting down"))?;
        backend
            .build_job(
                worker,
                extranonce1,
                session.difficulty,
                config.minimum_difficulty,
            )
            .await
    };
    let mut job = match timeout(
        Duration::from_secs_f64(config.initial_job_timeout_seconds),
        build,
    )
    .await
    {
        Ok(Ok(job)) => job,
        Ok(Err(error)) => {
            session.retry_job = true;
            return Err(error.into());
        }
        Err(_) => {
            session.retry_job = true;
            return Err(StratumError::backend("initial job delivery timed out").into());
        }
    };
    let work_invalidated = session.jobs.back().is_none_or(|prior| {
        prior.job.wire.previousblockhash != job.wire.previousblockhash
            || prior.job.wire.payout_revision != job.wire.payout_revision
    });
    job.wire.clean_jobs = work_invalidated;
    let mask = session.miner_version_mask.map_or(0, |miner| {
        miner & config.version_rolling_mask & job.wire.version_mask
    });
    match timeout(
        Duration::from_secs_f64(config.initial_job_timeout_seconds),
        backend.persist_issued_job(
            worker,
            &job,
            mask,
            Duration::from_secs_f64(config.job_retention_seconds.max(config.stale_grace_seconds)),
        ),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            session.retry_job = true;
            return Err(error.into());
        }
        Err(_) => {
            session.retry_job = true;
            return Err(StratumError::backend("job persistence timed out").into());
        }
    }
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
    let now = Instant::now();
    for prior in &mut session.jobs {
        prior.retired_at.get_or_insert(now);
        if prior.job.wire.previousblockhash != job.wire.previousblockhash {
            prior.tip_replaced_at.get_or_insert(now);
        }
    }
    session.jobs.retain(|prior| {
        // A same-parent payout replacement has no stale grace. Previous-parent
        // jobs retain their separate, notification-anchored grace deadline.
        (prior.job.wire.previousblockhash != job.wire.previousblockhash
            || prior.job.wire.payout_revision == job.wire.payout_revision)
            && prior.retired_at.is_none_or(|when| {
                when.elapsed().as_secs_f64()
                    <= config.job_retention_seconds.max(config.stale_grace_seconds)
            })
    });
    while session.jobs.len() >= config.max_jobs_per_connection {
        session.jobs.pop_front();
    }
    session.jobs.push_back(IssuedJob {
        job,
        worker: worker.clone(),
        authorization_permit: session.authorization_permit.clone(),
        version_mask: mask,
        retired_at: None,
        tip_replaced_at: None,
    });
    session.retry_job = false;
    observation.success = true;
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
    writer: &mut OwnedWriteHalf,
    config: &StratumConfig,
    request: Value,
    received_at: tokio::time::Instant,
    metrics: &crate::metrics::Metrics,
) -> Result<()> {
    let is_submit = request.get("method").and_then(Value::as_str) == Some("mining.submit");
    let share_observation =
        share_observation::ShareObservation::begin(metrics, is_submit, received_at);
    session.prune_jobs(config);
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
                    let id = timeout(
                        Duration::from_secs_f64(config.initial_job_timeout_seconds),
                        backend.new_session_id(),
                    )
                    .await
                    .map_err(|_| StratumError::backend("session allocation timed out"))??;
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
                let worker = timeout(
                    Duration::from_secs_f64(config.initial_job_timeout_seconds),
                    backend.authorize(username),
                )
                .await
                .map_err(|_| StratumError::backend("payout address validation timed out"))??;
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
                        .and_then(|issued| issued.authorization_permit.clone());
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
                if !session.jobs.iter().any(|j| j.job.wire.job_id == fields[1]) {
                    if let Some(job) = timeout(
                        Duration::from_secs_f64(config.initial_job_timeout_seconds),
                        backend.resume_job(worker, fields[1]),
                    )
                    .await
                    .map_err(|_| StratumError::backend("job resume timed out"))??
                    {
                        if job.wire.job_id != fields[1] {
                            return Err(StratumError::internal("restored job ID mismatch").into());
                        }
                        let original_mask = job.wire.version_mask;
                        while session.jobs.len() >= config.max_jobs_per_connection {
                            session.jobs.pop_front();
                        }
                        session.jobs.push_front(IssuedJob {
                            job,
                            worker: worker.clone(),
                            authorization_permit: session.authorization_permit.clone(),
                            version_mask: original_mask,
                            retired_at: Some(Instant::now()),
                            tip_replaced_at: None,
                        });
                    }
                }
                let issued = session
                    .jobs
                    .iter()
                    .find(|j| j.job.wire.job_id == fields[1])
                    .ok_or_else(|| StratumError::new(21, "stale job", "unknown-job"))?;
                if issued
                    .job
                    .wire
                    .resume_expires_at
                    .is_some_and(|expires| Instant::now() >= expires)
                    || issued.retired_at.is_some_and(|when| {
                        when.elapsed().as_secs_f64()
                            > config.job_retention_seconds.max(config.stale_grace_seconds)
                    })
                {
                    return Err(StratumError::new(21, "stale job", "stale-job").into());
                }
                let grace = issued.job.wire.resume_expires_at.is_none()
                    && config.stale_grace_seconds > 0.0
                    && issued.tip_replaced_at.is_none_or(|when| {
                        when.elapsed().as_secs_f64() <= config.stale_grace_seconds
                    });
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
                session.last_accepted_share = Some((share_id.clone(), proved_difficulty));
                config
                    .stats
                    .accepted_submissions
                    .fetch_add(1, Ordering::Relaxed);
                result(writer, id.clone(), json!(true), config).await?;
                share_observation.acknowledged(crate::metrics::AckResult::Accepted);
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
        write_json(writer, error.response(id), config).await?;
        share_observation.acknowledged(crate::metrics::AckResult::Rejected);
    }
    Ok(())
}

async fn session<B: MiningBackend>(
    stream: TcpStream,
    backend: Arc<B>,
    config: StratumConfig,
    mut refresh: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<crate::metrics::Metrics>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let observation = SessionObservation::new(config.stats.clone());
    let mut session = Session::new(&config, observation);
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut buffer = Vec::new();
    let mut timer = interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // An unauthenticated peer must not retain admission indefinitely.
    let connected = Instant::now();
    loop {
        if *shutdown.borrow() {
            break;
        }
        let remaining = config.max_message_bytes + 1 - buffer.len();
        let mut bounded_reader = (&mut reader).take(remaining as u64);
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            changed = refresh.changed() => {
                if changed.is_err() { break; }
                session.retry_job = session.worker.is_some() && session.extranonce1.is_some();
            }
            _ = timer.tick() => {
                if session.jobs.is_empty() && connected.elapsed().as_secs_f64() > config.initial_job_timeout_seconds { break; }
                session.prune_jobs(&config);
                session.retarget();
            }
            read = bounded_reader.read_until(b'\n',&mut buffer) => {
                if read? == 0 { break; }
                if buffer.len() > config.max_message_bytes {
                    write_json(&mut writer,StratumError::malformed("Stratum message exceeds size limit").response(Value::Null),&config).await?;
                    break;
                }
                if buffer.last() != Some(&b'\n') { continue; }
                let received_at = tokio::time::Instant::now();
                let frame = std::mem::take(&mut buffer);
                match serde_json::from_slice::<Value>(&frame) {
                    Ok(value) if value.is_object() => request(backend.as_ref(),&mut session,&mut writer,&config,value,received_at,&metrics).await?,
                    _ => write_json(&mut writer,StratumError::malformed("invalid JSON request").response(Value::Null),&config).await?,
                }
            }
        }
        if session.retry_job {
            if let Err(error) =
                deliver_job(backend.as_ref(), &mut session, &mut writer, &config).await
            {
                session.restore_retarget();
                if error.downcast_ref::<StratumError>().is_none() {
                    return Err(error);
                }
                // Template/RPC trouble is retried on the timer while existing
                // work remains usable. Do not disconnect a healthy miner.
            }
        }
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
    let mut connections = JoinSet::new();
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            _ = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let (stream,_) = accepted?;
                let Ok(permit) = config.connection_limit.clone().try_acquire_owned() else { drop(stream); continue; };
                let (backend,config,refresh,shutdown) = (backend.clone(),config.clone(),refresh.clone(),shutdown.clone());
                let metrics = metrics.clone();
                let runtime = metrics.runtime();
                connections.spawn(runtime.track(crate::metrics::TaskKind::StratumSession, async move {
                    let _permit = permit;
                    if let Err(error) = session(stream,backend,config,refresh,shutdown,metrics).await {
                        tracing::warn!(error = %format_args!("{error:#}"), "Stratum connection ended");
                    }
                }));
            }
        }
    }
    if timeout(Duration::from_secs(10), async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    Ok(())
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
