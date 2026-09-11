//! Measurement: latency summaries, per-frontend CPU and RSS, ORDER_LOCK wait
//! sampling, server histogram scrapes and host facts.
//!
//! EP-OBSERVABILITY: every quantity records its units and its clock, and an
//! unknown or failed measurement is `None` with a reason, never 0.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// `ORDER_LOCK` is `0x505249534d000002` in
/// `crates/qbit-prism-server/src/ledger.rs`. `pg_advisory_xact_lock(bigint)`
/// stores the high 32 bits as `classid` and the low 32 as `objid`.
pub const ORDER_LOCK_CLASSID: i64 = 0x5052_4953;
pub const ORDER_LOCK_OBJID: i64 = 0x4d00_0002;
/// The predicate is derived from the hex key, not typed in decimal: the high
/// half of `0x505249534d000002` is `0x50524953`, which is 1347570003, and the
/// low half `0x4d000002` is 1291845634.
pub const ORDER_LOCK_KEY_NOTE: &str =
    "classid is the high 32 bits of 0x505249534d000002 (0x50524953 = 1347570003) and objid the \
     low 32 bits (0x4d000002 = 1291845634), with objsubid = 1";

/// Raise this process's file-descriptor soft limit to what the session count
/// needs. Child frontends inherit it.
pub fn raise_file_descriptor_limit(required: u64) -> Result<(u64, u64)> {
    unsafe {
        let mut limit: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return Err(std::io::Error::last_os_error()).context("getrlimit(RLIMIT_NOFILE)");
        }
        let before = limit.rlim_cur as u64;
        let hard = limit.rlim_max as u64;
        let wanted = required.max(before).min(hard);
        if wanted > before {
            limit.rlim_cur = wanted as libc::rlim_t;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                return Err(std::io::Error::last_os_error()).context("setrlimit(RLIMIT_NOFILE)");
            }
        }
        Ok((before, wanted))
    }
}

/// Percentile summary of a latency sample, in milliseconds on the client's
/// monotonic clock.
#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencySummary {
    pub unit: &'static str,
    pub clock: &'static str,
    pub samples: usize,
    pub p50: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
    /// Why the percentiles are absent, when they are.
    pub unavailable_reason: Option<String>,
}

pub fn summarize(mut values: Vec<f64>, clock: &'static str) -> LatencySummary {
    if values.is_empty() {
        return LatencySummary {
            unit: "milliseconds",
            clock,
            samples: 0,
            unavailable_reason: Some("no samples were recorded".into()),
            ..Default::default()
        };
    }
    values.sort_by(f64::total_cmp);
    let quantile = |q: f64| -> f64 {
        // Nearest-rank, so a reported percentile is always an observed value.
        let rank = (q * values.len() as f64).ceil().max(1.0) as usize;
        values[rank.min(values.len()) - 1]
    };
    let sum: f64 = values.iter().sum();
    LatencySummary {
        unit: "milliseconds",
        clock,
        samples: values.len(),
        p50: Some(quantile(0.50)),
        p99: Some(quantile(0.99)),
        max: values.last().copied(),
        mean: Some(sum / values.len() as f64),
        unavailable_reason: None,
    }
}

/// One process sample from procfs.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct ProcessSample {
    pub elapsed_seconds: f64,
    pub cpu_seconds: Option<f64>,
    pub rss_kib: Option<u64>,
    pub peak_rss_kib: Option<u64>,
}

/// What one frontend's process sampler saw over a phase or a whole run.
#[derive(Clone, Debug, Serialize)]
pub struct ProcessSummary {
    pub instance_id: String,
    pub pid: Option<u32>,
    pub platform: String,
    pub sample_interval_seconds: f64,
    pub samples: usize,
    pub cpu_seconds: Option<f64>,
    pub cpu_cores_mean: Option<f64>,
    pub rss_kib_max: Option<u64>,
    pub rss_kib_last: Option<u64>,
    /// `VmHWM` on Linux, which is a kernel peak, not a sampled maximum.
    pub peak_rss_kib: Option<u64>,
    pub peak_rss_source: &'static str,
    pub unavailable_reason: Option<String>,
}

#[cfg(target_os = "linux")]
pub fn clock_ticks_per_second() -> f64 {
    unsafe { libc::sysconf(libc::_SC_CLK_TCK) as f64 }
}

/// `utime + stime`, in seconds. The `comm` field can contain spaces and
/// parentheses, so the fields are read after the last `)`.
#[cfg(target_os = "linux")]
pub fn process_cpu_seconds(pid: u32) -> Option<f64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let tail = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = tail.split_whitespace().collect();
    // After `)` the first field is `state`, which is field 3, so `utime` (14)
    // and `stime` (15) are at offsets 11 and 12.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) as f64 / clock_ticks_per_second())
}

#[cfg(target_os = "linux")]
fn status_kib(pid: u32, field: &str) -> Option<u64> {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

#[cfg(target_os = "linux")]
pub fn process_rss_kib(pid: u32) -> Option<u64> {
    status_kib(pid, "VmRSS:")
}

#[cfg(target_os = "linux")]
pub fn process_peak_rss_kib(pid: u32) -> Option<u64> {
    status_kib(pid, "VmHWM:")
}

/// macOS exposes cumulative CPU and resident size through `proc_pid_rusage`.
/// It has no `VmHWM` equivalent, so a peak there is a sampled maximum and is
/// labelled as one. `RUSAGE_INFO_V2`'s layout is declared here rather than
/// taken from `libc`, so the harness does not depend on which Apple structs a
/// given `libc` release happens to export.
#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Clone, Copy)]
struct RusageInfoV2 {
    ri_uuid: [u8; 16],
    ri_user_time: u64,
    ri_system_time: u64,
    ri_pkg_idle_wkups: u64,
    ri_interrupt_wkups: u64,
    ri_pageins: u64,
    ri_wired_size: u64,
    ri_resident_size: u64,
    ri_phys_footprint: u64,
    ri_proc_start_abstime: u64,
    ri_proc_exit_abstime: u64,
    ri_child_user_time: u64,
    ri_child_system_time: u64,
    ri_child_pkg_idle_wkups: u64,
    ri_child_interrupt_wkups: u64,
    ri_child_pageins: u64,
    ri_child_elapsed_abstime: u64,
    ri_diskio_bytesread: u64,
    ri_diskio_byteswritten: u64,
}

#[cfg(target_os = "macos")]
extern "C" {
    fn proc_pid_rusage(
        pid: libc::c_int,
        flavor: libc::c_int,
        buffer: *mut libc::c_void,
    ) -> libc::c_int;
}

#[cfg(target_os = "macos")]
fn rusage(pid: u32) -> Option<RusageInfoV2> {
    const RUSAGE_INFO_V2: libc::c_int = 2;
    unsafe {
        let mut info: RusageInfoV2 = std::mem::zeroed();
        let code = proc_pid_rusage(
            pid as libc::c_int,
            RUSAGE_INFO_V2,
            &mut info as *mut RusageInfoV2 as *mut libc::c_void,
        );
        (code == 0).then_some(info)
    }
}

/// `ri_user_time` and `ri_system_time` are nanoseconds.
#[cfg(target_os = "macos")]
pub fn process_cpu_seconds(pid: u32) -> Option<f64> {
    rusage(pid).map(|info| (info.ri_user_time + info.ri_system_time) as f64 / 1e9)
}

#[cfg(target_os = "macos")]
pub fn process_rss_kib(pid: u32) -> Option<u64> {
    rusage(pid).map(|info| info.ri_resident_size / 1024)
}

/// No kernel high-water mark exists on macOS; the run reports the sampled
/// maximum instead, never a fabricated peak.
#[cfg(target_os = "macos")]
pub fn process_peak_rss_kib(_pid: u32) -> Option<u64> {
    None
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn process_cpu_seconds(_pid: u32) -> Option<f64> {
    None
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn process_rss_kib(_pid: u32) -> Option<u64> {
    None
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn process_peak_rss_kib(_pid: u32) -> Option<u64> {
    None
}

pub const PEAK_RSS_SOURCE: &str = if cfg!(target_os = "linux") {
    "kernel VmHWM"
} else if cfg!(target_os = "macos") {
    "sampled max"
} else {
    "unavailable"
};

/// A per-second CPU and RSS sampler for one child process.
pub struct ProcessSampler {
    pub instance_id: String,
    pid: Arc<std::sync::Mutex<Option<u32>>>,
    samples: Arc<std::sync::Mutex<Vec<ProcessSample>>>,
    stop: Arc<AtomicBool>,
    interval: Duration,
    started: Instant,
}

impl ProcessSampler {
    pub fn start(instance_id: String, pid: Option<u32>, interval: Duration) -> Self {
        let sampler = Self {
            instance_id,
            pid: Arc::new(std::sync::Mutex::new(pid)),
            samples: Arc::new(std::sync::Mutex::new(Vec::new())),
            stop: Arc::new(AtomicBool::new(false)),
            interval,
            started: Instant::now(),
        };
        let pid_handle = sampler.pid.clone();
        let samples = sampler.samples.clone();
        let stop = sampler.stop.clone();
        let started = sampler.started;
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let pid = *pid_handle.lock().expect("sampler pid lock");
                let sample = ProcessSample {
                    elapsed_seconds: started.elapsed().as_secs_f64(),
                    cpu_seconds: pid.and_then(process_cpu_seconds),
                    rss_kib: pid.and_then(process_rss_kib),
                    peak_rss_kib: pid.and_then(process_peak_rss_kib),
                };
                samples.lock().expect("sampler lock").push(sample);
                tokio::time::sleep(interval).await;
            }
        });
        sampler
    }

    /// A restart gives the frontend a new pid; CPU counters restart with it,
    /// so the summary reports the sum across process lifetimes.
    pub fn set_pid(&self, pid: Option<u32>) {
        *self.pid.lock().expect("sampler pid lock") = pid;
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Map an `Instant` onto this sampler's own elapsed clock.
    pub fn elapsed_of(&self, at: Instant) -> f64 {
        at.saturating_duration_since(self.started).as_secs_f64()
    }

    pub fn samples(&self) -> Vec<ProcessSample> {
        self.samples.lock().expect("sampler lock").clone()
    }

    pub fn summarize(&self, since_elapsed: f64, until_elapsed: f64) -> ProcessSummary {
        let all = self.samples();
        let window: Vec<ProcessSample> = all
            .into_iter()
            .filter(|s| s.elapsed_seconds >= since_elapsed && s.elapsed_seconds <= until_elapsed)
            .collect();
        let platform = std::env::consts::OS.to_owned();
        if window.is_empty() {
            return ProcessSummary {
                instance_id: self.instance_id.clone(),
                pid: *self.pid.lock().expect("sampler pid lock"),
                platform,
                sample_interval_seconds: self.interval.as_secs_f64(),
                samples: 0,
                cpu_seconds: None,
                cpu_cores_mean: None,
                rss_kib_max: None,
                rss_kib_last: None,
                peak_rss_kib: None,
                peak_rss_source: PEAK_RSS_SOURCE,
                unavailable_reason: Some("no samples fell inside the window".into()),
            };
        }
        // CPU counters reset when a frontend is restarted, so the delta is
        // accumulated segment by segment and a decrease starts a new segment.
        let mut cpu_total = 0.0f64;
        let mut previous: Option<f64> = None;
        let mut saw_cpu = false;
        for sample in &window {
            if let Some(current) = sample.cpu_seconds {
                saw_cpu = true;
                if let Some(last) = previous {
                    if current >= last {
                        cpu_total += current - last;
                    } else {
                        cpu_total += current;
                    }
                }
                previous = Some(current);
            } else {
                previous = None;
            }
        }
        let span = (window.last().unwrap().elapsed_seconds
            - window.first().unwrap().elapsed_seconds)
            .max(f64::MIN_POSITIVE);
        ProcessSummary {
            instance_id: self.instance_id.clone(),
            pid: *self.pid.lock().expect("sampler pid lock"),
            platform,
            sample_interval_seconds: self.interval.as_secs_f64(),
            samples: window.len(),
            cpu_seconds: saw_cpu.then_some(cpu_total),
            cpu_cores_mean: saw_cpu.then(|| cpu_total / span),
            rss_kib_max: window.iter().filter_map(|s| s.rss_kib).max(),
            rss_kib_last: window.last().and_then(|s| s.rss_kib),
            peak_rss_kib: window.iter().filter_map(|s| s.peak_rss_kib).max(),
            peak_rss_source: PEAK_RSS_SOURCE,
            unavailable_reason: (!saw_cpu)
                .then(|| format!("no CPU accounting on {}", std::env::consts::OS)),
        }
    }
}

/// One `pg_locks` sample of the ORDER_LOCK waiter set.
#[derive(Clone, Debug)]
pub struct LockSample {
    pub monotonic: Instant,
    pub server_time: chrono::DateTime<chrono::Utc>,
    pub waiters: Vec<(i32, Option<chrono::DateTime<chrono::Utc>>)>,
    pub query_millis: f64,
}

/// The ORDER_LOCK picture over a phase.
#[derive(Clone, Debug, Serialize)]
pub struct LockSummary {
    pub lock: &'static str,
    pub key: &'static str,
    pub classid: i64,
    pub objid: i64,
    pub key_note: &'static str,
    pub sample_interval_milliseconds: f64,
    pub samples: usize,
    pub samples_with_waiters: usize,
    pub max_waiters: usize,
    pub mean_waiters: Option<f64>,
    /// Riemann estimate of the total time backends spent waiting, in
    /// waiter-seconds. Waits shorter than the sampling interval can be missed
    /// entirely, so this is a lower bound.
    pub waiter_seconds_estimate: Option<f64>,
    /// Distinct `(pid, waitstart)` pairs seen. Reported as "at least".
    pub episodes_at_least: usize,
    pub longest_observed_wait_seconds: Option<f64>,
    /// The sampler's own cost, so its perturbation of the thing it measures is
    /// visible.
    pub sampler_query_millis_mean: Option<f64>,
    pub sampler_query_millis_max: Option<f64>,
    pub unavailable_reason: Option<String>,
    /// `pg_stat_statements` aggregate for the advisory-lock statement, when
    /// the extension is loaded. The three PRISM locks share one query text.
    pub advisory_lock_statement: Option<AdvisoryStatementStats>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AdvisoryStatementStats {
    pub calls: i64,
    pub total_exec_milliseconds: f64,
    pub note: &'static str,
}

/// Side-connection sampler for ORDER_LOCK waits.
pub struct LockSampler {
    samples: Arc<std::sync::Mutex<Vec<LockSample>>>,
    stop: Arc<AtomicBool>,
    interval: Duration,
    failure: Arc<std::sync::Mutex<Option<String>>>,
}

impl LockSampler {
    pub fn start(pool: PgPool, interval: Duration) -> Self {
        let sampler = Self {
            samples: Arc::new(std::sync::Mutex::new(Vec::new())),
            stop: Arc::new(AtomicBool::new(false)),
            interval,
            failure: Arc::new(std::sync::Mutex::new(None)),
        };
        let samples = sampler.samples.clone();
        let stop = sampler.stop.clone();
        let failure = sampler.failure.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let started = Instant::now();
                let query = sqlx::query(
                    "SELECT a.pid, l.waitstart, clock_timestamp() AS sampled_at \
                     FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
                     WHERE l.locktype = 'advisory' \
                       AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
                       AND l.classid = $1::bigint::oid AND l.objid = $2::bigint::oid \
                       AND l.objsubid = 1 AND NOT l.granted",
                )
                .bind(ORDER_LOCK_CLASSID)
                .bind(ORDER_LOCK_OBJID)
                .fetch_all(&pool)
                .await;
                match query {
                    Ok(rows) => {
                        let server_time = rows
                            .first()
                            .and_then(|row| row.try_get("sampled_at").ok())
                            .unwrap_or_else(chrono::Utc::now);
                        let waiters = rows
                            .iter()
                            .map(|row| {
                                (
                                    row.try_get::<i32, _>("pid").unwrap_or_default(),
                                    row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(
                                        "waitstart",
                                    )
                                    .unwrap_or(None),
                                )
                            })
                            .collect();
                        samples.lock().expect("lock sampler").push(LockSample {
                            monotonic: Instant::now(),
                            server_time,
                            waiters,
                            query_millis: started.elapsed().as_secs_f64() * 1000.0,
                        });
                    }
                    Err(error) => {
                        *failure.lock().expect("lock sampler failure") = Some(error.to_string());
                    }
                }
                tokio::time::sleep(interval).await;
            }
        });
        sampler
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn summarize(&self, since: Instant, until: Instant) -> LockSummary {
        let all = self.samples.lock().expect("lock sampler").clone();
        let failure = self.failure.lock().expect("lock sampler failure").clone();
        let window: Vec<LockSample> = all
            .into_iter()
            .filter(|s| s.monotonic >= since && s.monotonic <= until)
            .collect();
        let mut summary = LockSummary {
            lock: "ORDER_LOCK",
            key: "0x505249534d000002",
            classid: ORDER_LOCK_CLASSID,
            objid: ORDER_LOCK_OBJID,
            key_note: ORDER_LOCK_KEY_NOTE,
            sample_interval_milliseconds: self.interval.as_secs_f64() * 1000.0,
            samples: window.len(),
            samples_with_waiters: 0,
            max_waiters: 0,
            mean_waiters: None,
            waiter_seconds_estimate: None,
            episodes_at_least: 0,
            longest_observed_wait_seconds: None,
            sampler_query_millis_mean: None,
            sampler_query_millis_max: None,
            unavailable_reason: failure,
            advisory_lock_statement: None,
        };
        if window.is_empty() {
            summary
                .unavailable_reason
                .get_or_insert_with(|| "no samples fell inside the phase".into());
            return summary;
        }
        let mut episodes: HashMap<
            (i32, String),
            (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
        > = HashMap::new();
        let mut waiter_seconds = 0.0f64;
        let mut total_waiters = 0usize;
        let mut previous: Option<Instant> = None;
        let mut query_millis = Vec::with_capacity(window.len());
        for sample in &window {
            query_millis.push(sample.query_millis);
            let count = sample.waiters.len();
            total_waiters += count;
            summary.max_waiters = summary.max_waiters.max(count);
            if count > 0 {
                summary.samples_with_waiters += 1;
            }
            if let Some(last) = previous {
                let delta = sample
                    .monotonic
                    .saturating_duration_since(last)
                    .as_secs_f64();
                waiter_seconds += count as f64 * delta;
            }
            previous = Some(sample.monotonic);
            for (pid, waitstart) in &sample.waiters {
                let key = (*pid, waitstart.map(|w| w.to_rfc3339()).unwrap_or_default());
                let entry = episodes
                    .entry(key)
                    .or_insert((waitstart.unwrap_or(sample.server_time), sample.server_time));
                entry.1 = sample.server_time;
            }
        }
        summary.mean_waiters = Some(total_waiters as f64 / window.len() as f64);
        summary.waiter_seconds_estimate = Some(waiter_seconds);
        summary.episodes_at_least = episodes.len();
        summary.longest_observed_wait_seconds = episodes
            .values()
            .map(|(start, last)| (*last - *start).num_milliseconds() as f64 / 1000.0)
            .max_by(f64::total_cmp);
        summary.sampler_query_millis_mean =
            Some(query_millis.iter().sum::<f64>() / query_millis.len() as f64);
        summary.sampler_query_millis_max = query_millis.iter().copied().max_by(f64::total_cmp);
        summary
    }
}

/// Reset `pg_stat_statements`, if it is loaded.
pub async fn reset_statement_stats(pool: &PgPool) -> bool {
    sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(pool)
        .await
        .is_ok()
}

/// `calls` and `total_exec_time` of the advisory-lock statement. All three
/// PRISM advisory locks share one normalized query text, so this mixes them.
pub async fn advisory_lock_statement(pool: &PgPool) -> Option<AdvisoryStatementStats> {
    let row = sqlx::query(
        "SELECT sum(calls)::bigint AS calls, sum(total_exec_time)::double precision AS total \
         FROM pg_stat_statements WHERE query LIKE '%pg_advisory_xact_lock%'",
    )
    .fetch_one(pool)
    .await
    .ok()?;
    let calls: Option<i64> = row.try_get("calls").ok()?;
    let total: Option<f64> = row.try_get("total").ok()?;
    Some(AdvisoryStatementStats {
        calls: calls?,
        total_exec_milliseconds: total?,
        note: "MIGRATION, ORDER and SETTLEMENT locks share one normalized query text",
    })
}

/// One scrape of a frontend's `/metrics`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MetricsScrape {
    pub instance_id: String,
    pub ok: bool,
    pub error: Option<String>,
    /// `qbit_prism_share_ack_seconds_bucket{result=...,le=...}`.
    pub ack_buckets: BTreeMap<String, BTreeMap<String, f64>>,
    pub ack_counts: BTreeMap<String, f64>,
    pub ack_sums: BTreeMap<String, f64>,
}

pub async fn scrape_metrics(instance_id: &str, url: &str) -> MetricsScrape {
    let mut scrape = MetricsScrape {
        instance_id: instance_id.to_owned(),
        ..Default::default()
    };
    let response = reqwest::Client::new()
        .get(url)
        .timeout(Duration::from_secs(10))
        .send()
        .await;
    let body = match response {
        Ok(response) if response.status().is_success() => response.text().await,
        Ok(response) => {
            scrape.error = Some(format!("HTTP {}", response.status()));
            return scrape;
        }
        Err(error) => {
            scrape.error = Some(error.to_string());
            return scrape;
        }
    };
    let body = match body {
        Ok(body) => body,
        Err(error) => {
            scrape.error = Some(error.to_string());
            return scrape;
        }
    };
    parse_share_ack(&body, &mut scrape);
    scrape.ok = true;
    scrape
}

fn label(line: &str, name: &str) -> Option<String> {
    let start = line.find('{')?;
    let end = line.find('}')?;
    for pair in line[start + 1..end].split(',') {
        let (key, value) = pair.split_once('=')?;
        if key.trim() == name {
            return Some(value.trim().trim_matches('"').to_owned());
        }
    }
    None
}

fn sample_value(line: &str) -> Option<f64> {
    line.rsplit_once(' ')?.1.trim().parse().ok()
}

pub fn parse_share_ack(body: &str, scrape: &mut MetricsScrape) {
    for line in body.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let Some(value) = sample_value(line) else {
            continue;
        };
        if line.starts_with("qbit_prism_share_ack_seconds_bucket") {
            let (Some(result), Some(le)) = (label(line, "result"), label(line, "le")) else {
                continue;
            };
            scrape
                .ack_buckets
                .entry(result)
                .or_default()
                .insert(le, value);
        } else if line.starts_with("qbit_prism_share_ack_seconds_count") {
            if let Some(result) = label(line, "result") {
                scrape.ack_counts.insert(result, value);
            }
        } else if line.starts_with("qbit_prism_share_ack_seconds_sum") {
            if let Some(result) = label(line, "result") {
                scrape.ack_sums.insert(result, value);
            }
        }
    }
}

/// Bucket deltas between two scrapes of the same frontend.
#[derive(Clone, Debug, Serialize)]
pub struct ServerAckDelta {
    pub instance_id: String,
    pub metric: &'static str,
    pub boundary: &'static str,
    pub buckets_seconds: Vec<f64>,
    pub counts: BTreeMap<String, f64>,
    pub sums: BTreeMap<String, f64>,
    pub bucket_deltas: BTreeMap<String, BTreeMap<String, f64>>,
    pub unavailable_reason: Option<String>,
}

pub fn ack_delta(before: &MetricsScrape, after: &MetricsScrape) -> ServerAckDelta {
    let mut delta = ServerAckDelta {
        instance_id: after.instance_id.clone(),
        metric: "qbit_prism_share_ack_seconds",
        boundary: "server-side: complete submit frame receipt to completed response write",
        buckets_seconds: qbit_prism_server::metrics::BUCKETS.to_vec(),
        counts: BTreeMap::new(),
        sums: BTreeMap::new(),
        bucket_deltas: BTreeMap::new(),
        unavailable_reason: None,
    };
    if !before.ok || !after.ok {
        delta.unavailable_reason = Some(
            after
                .error
                .clone()
                .or_else(|| before.error.clone())
                .unwrap_or_else(|| "a boundary scrape failed".into()),
        );
        return delta;
    }
    for (result, after_count) in &after.ack_counts {
        let before_count = before.ack_counts.get(result).copied().unwrap_or(0.0);
        delta
            .counts
            .insert(result.clone(), after_count - before_count);
    }
    for (result, after_sum) in &after.ack_sums {
        let before_sum = before.ack_sums.get(result).copied().unwrap_or(0.0);
        delta.sums.insert(result.clone(), after_sum - before_sum);
    }
    for (result, buckets) in &after.ack_buckets {
        let empty = BTreeMap::new();
        let before_buckets = before.ack_buckets.get(result).unwrap_or(&empty);
        let entry = delta.bucket_deltas.entry(result.clone()).or_default();
        for (le, value) in buckets {
            entry.insert(
                le.clone(),
                value - before_buckets.get(le).copied().unwrap_or(0.0),
            );
        }
    }
    delta
}

/// `MemAvailable`, in kibibytes.
pub fn mem_available_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// CPU model, core count, RAM and OS, for the report header.
pub fn host_facts() -> Value {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let model = cpuinfo
        .lines()
        .find_map(|line| line.strip_prefix("model name"))
        .and_then(|rest| rest.split_once(':'))
        .map(|(_, value)| value.trim().to_owned());
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let total_kib = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok());
    let swap_kib = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("SwapTotal:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok());
    serde_json::json!({
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "kernel": std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .map(|value| value.trim().to_owned()).ok(),
        "cpu_model": model,
        "cpu_cores": std::thread::available_parallelism().map(usize::from).ok(),
        "memory_total_kib": total_kib,
        "swap_total_kib": swap_kib,
        "cgroup_memory_max": read_cgroup("memory.max"),
        "cgroup_cpu_max": read_cgroup("cpu.max"),
    })
}

fn read_cgroup(name: &str) -> Option<String> {
    std::fs::read_to_string(format!("/sys/fs/cgroup/{name}"))
        .ok()
        .map(|value| value.trim().to_owned())
}
