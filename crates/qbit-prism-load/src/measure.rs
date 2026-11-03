//! Measurement: latency summaries, per-frontend CPU and RSS, PRISM
//! advisory-lock wait sampling, server histogram scrapes and host facts.
//!
//! EP-OBSERVABILITY: every quantity records its units and its clock, and an
//! unknown or failed measurement is `None` with a reason, never 0.

use anyhow::{ensure, Context, Result};
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// Every PRISM advisory lock is `0x505249534d0000xx` in
/// `crates/qbit-prism-server/src/ledger.rs`. `pg_advisory_xact_lock(bigint)`
/// stores the high 32 bits as `classid` and the low 32 as `objid`, so the three
/// locks share one `classid` and differ only in `objid`.
pub const PRISM_LOCK_CLASSID: i64 = 0x5052_4953;
/// Kept under its old name because `order_lock.classid` publishes it.
pub const ORDER_LOCK_CLASSID: i64 = PRISM_LOCK_CLASSID;
pub const ORDER_LOCK_OBJID: i64 = 0x4d00_0002;
pub const SETTLEMENT_LOCK_OBJID: i64 = 0x4d00_0003;
/// The predicate is derived from the hex key, not typed in decimal: the high
/// half of `0x505249534d000002` is `0x50524953`, which is 1347570003, and the
/// low half `0x4d000002` is 1291845634.
pub const ORDER_LOCK_KEY_NOTE: &str =
    "classid is the high 32 bits of 0x505249534d000002 (0x50524953 = 1347570003) and objid the \
     low 32 bits (0x4d000002 = 1291845634), with objsubid = 1";
/// The same derivation for `0x505249534d000003`: the low half `0x4d000003` is
/// 1291845635.
pub const SETTLEMENT_LOCK_KEY_NOTE: &str =
    "classid is the high 32 bits of 0x505249534d000003 (0x50524953 = 1347570003) and objid the \
     low 32 bits (0x4d000003 = 1291845635), with objsubid = 1";

/// One advisory lock the sampler watches, and which server path takes it, so
/// each reported block names its own source (EP-OBSERVABILITY).
#[derive(Clone, Copy, Debug)]
pub struct SampledLock {
    pub lock: &'static str,
    pub key: &'static str,
    pub objid: i64,
    pub key_note: &'static str,
    pub taken_by: &'static str,
}

pub const ORDER_LOCK: SampledLock = SampledLock {
    lock: "ORDER_LOCK",
    key: "0x505249534d000002",
    objid: ORDER_LOCK_OBJID,
    key_note: ORDER_LOCK_KEY_NOTE,
    taken_by: "the share append. Window::append_checked (ledger/window.rs) takes this lock and \
               no other, so this block is the queueing a share pays for. The rebuild paths take \
               it too, but second, after SETTLEMENT_LOCK.",
};

pub const SETTLEMENT_LOCK: SampledLock = SampledLock {
    lock: "SETTLEMENT_LOCK",
    key: "0x505249534d000003",
    objid: SETTLEMENT_LOCK_OBJID,
    key_note: SETTLEMENT_LOCK_KEY_NOTE,
    taken_by: "the rebuild after a landing. observe_chain_view (ledger/window.rs), the job build \
               (ledger/jobs.rs) and candidate confirmation or abandonment (ledger/blocks.rs) take \
               this lock first and ORDER_LOCK second, so a landing's rebuild queues here before \
               it queues on ORDER_LOCK.",
};

/// Both locks the sampler watches, in the order they are taken.
pub const SAMPLED_LOCKS: [SampledLock; 2] = [SETTLEMENT_LOCK, ORDER_LOCK];

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

/// Unit labels for `summarize`. A consumer generic over `LatencySummary`
/// reads `unit`, so a count summarized as milliseconds renders as a time:
/// a carried unit that is wrong is worse than none (EP-OBSERVABILITY).
pub const MILLISECONDS: &str = "milliseconds";
pub const COUNT: &str = "count";

/// Percentile summary of a sample, in the unit and on the clock it names.
#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencySummary {
    pub unit: &'static str,
    pub clock: &'static str,
    pub samples: usize,
    pub min: Option<f64>,
    pub p50: Option<f64>,
    pub p90: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
    /// Why the percentiles are absent, when they are.
    pub unavailable_reason: Option<String>,
}

pub fn summarize(mut values: Vec<f64>, unit: &'static str, clock: &'static str) -> LatencySummary {
    if values.is_empty() {
        return LatencySummary {
            unit,
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
        unit,
        clock,
        samples: values.len(),
        min: values.first().copied(),
        p50: Some(quantile(0.50)),
        p90: Some(quantile(0.90)),
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

/// Declared here for the same reason `RusageInfoV2` is.
#[cfg(target_os = "macos")]
#[repr(C)]
struct MachTimebaseInfo {
    numer: u32,
    denom: u32,
}

#[cfg(target_os = "macos")]
extern "C" {
    fn mach_timebase_info(info: *mut MachTimebaseInfo) -> libc::c_int;
}

/// The Mach timebase as `(numer, denom)`: one tick is `numer / denom`
/// nanoseconds. `None` when the kernel would not say, or said a zero
/// denominator; a CPU figure is then unknown rather than wrong.
#[cfg(target_os = "macos")]
fn mach_timebase() -> Option<(u32, u32)> {
    let mut info = MachTimebaseInfo { numer: 0, denom: 0 };
    let code = unsafe { mach_timebase_info(&mut info) };
    (code == 0 && info.numer != 0 && info.denom != 0).then_some((info.numer, info.denom))
}

/// Mach absolute-time ticks to seconds. Not gated on the platform, so the
/// arithmetic is tested wherever the tests run.
pub fn mach_ticks_to_seconds(ticks: u64, numer: u32, denom: u32) -> f64 {
    ticks as f64 * f64::from(numer) / f64::from(denom) / 1e9
}

/// `ri_user_time` and `ri_system_time` are Mach absolute-time ticks, not
/// nanoseconds. The two coincide on Intel, where the timebase is 1/1. On Apple
/// silicon it is 125/3, and reading ticks as nanoseconds under-reported every
/// frontend's CPU by about 42 times (#447).
#[cfg(target_os = "macos")]
pub fn process_cpu_seconds(pid: u32) -> Option<f64> {
    let (numer, denom) = mach_timebase()?;
    rusage(pid)
        .map(|info| mach_ticks_to_seconds(info.ri_user_time + info.ri_system_time, numer, denom))
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

/// One backend's PRISM advisory-lock row at one instant: waiting when
/// `granted` is false, holding when it is true.
///
/// The holder matters as much as the waiter. A foreign process that *holds*
/// one of these locks stalls every frontend, and the frontends then queue up as
/// waiters and are correctly recognised as this run's own -- so a sampler that
/// only selected ungranted rows billed the whole stall to them and reported
/// that nothing foreign was involved.
#[derive(Clone, Debug)]
pub struct LockRow {
    pub pid: i32,
    /// Which PRISM lock this row is on. Every sample carries rows for both
    /// sampled locks, and each summary reads only its own.
    pub objid: i64,
    pub granted: bool,
    pub waitstart: Option<chrono::DateTime<chrono::Utc>>,
    pub application_name: String,
    /// Whether `pg_stat_activity` showed this backend to the sampler's role.
    /// A row whose activity the sampler cannot read cannot be shown to be one
    /// of this run's frontends, so it is counted as foreign: that is the safe
    /// direction, and it matches the floor test.
    pub activity_visible: bool,
}

/// What a foreign row with no readable `pg_stat_activity` is called in the
/// foreign name lists, so an unreadable row is never reported as an empty
/// `application_name`.
pub const UNREADABLE_ACTIVITY: &str = "(activity row not visible to the sampler's role)";

/// One `pg_locks` sample of every sampled PRISM advisory-lock row in this
/// database. One poll covers both locks, so the two summaries share a sample
/// count and a sampler cost.
#[derive(Clone, Debug)]
pub struct LockSample {
    pub monotonic: Instant,
    pub server_time: chrono::DateTime<chrono::Utc>,
    pub rows: Vec<LockRow>,
    pub query_millis: f64,
}

/// One sampled lock's picture over a phase.
#[derive(Clone, Debug, Serialize)]
pub struct LockSummary {
    pub lock: &'static str,
    pub key: &'static str,
    pub classid: i64,
    pub objid: i64,
    pub key_note: &'static str,
    /// Which server path takes this lock, so the block says what it measures
    /// rather than leaving a reader to infer it from the name.
    pub taken_by: &'static str,
    pub sample_interval_milliseconds: f64,
    /// How waiters were attributed to this run's frontends.
    pub attribution: String,
    /// Frontends whose connections the sampler recognised.
    pub attributed_application_names: Vec<String>,
    /// Polls that fell inside the phase. This is the one count that is a
    /// measurement even at zero: it says how much the sampler saw.
    pub samples: usize,
    /// Every count below is `None` when `samples` is 0: with no poll inside
    /// the phase the sampler saw no queue, which is not the same as seeing
    /// an empty one, and a plain 0 beside `unavailable_reason` read as a
    /// measured zero (#483). A phase the sampler did poll and found nothing
    /// waiting reports `Some(0)`, a real measured zero.
    pub samples_with_waiters: Option<usize>,
    pub max_waiters: Option<usize>,
    pub mean_waiters: Option<f64>,
    /// A PRISM advisory lock is database-wide, so anything else holding or
    /// waiting on it in the same database would distort every number here.
    /// These are the waiters that were not this run's frontends.
    pub foreign_waiter_samples: Option<usize>,
    pub foreign_application_names: Vec<String>,
    pub foreign_waiter_seconds_estimate: Option<f64>,
    /// A foreign *holder* is what a foreign stall looks like from here: it
    /// blocks every frontend, and the frontends then queue up as waiters that
    /// are correctly this run's own. Without these three, that stall would be
    /// billed to them.
    pub foreign_holder_samples: Option<usize>,
    pub foreign_holder_application_names: Vec<String>,
    pub foreign_holder_seconds_estimate: Option<f64>,
    /// One answer for a reader who should not have to notice a zero in the
    /// right field. `None` means the sampler could not attribute rows at all,
    /// so it cannot say: unknown, not false.
    pub foreign_contention_observed: Option<bool>,
    /// Riemann estimate of the total time backends spent waiting, in
    /// waiter-seconds. Waits shorter than the sampling interval can be missed
    /// entirely, so this is a lower bound.
    pub waiter_seconds_estimate: Option<f64>,
    /// Distinct `(pid, waitstart)` pairs seen. Reported as "at least";
    /// `None` when there were no samples to see them in.
    pub episodes_at_least: Option<usize>,
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

/// One sample's rows, split by whose they are and whether they are granted.
///
/// `own_holding` is the normal case -- a frontend in the middle of an append
/// -- and deliberately stays out of every foreign counter.
pub struct LockRowSplit<'a> {
    pub own_waiting: Vec<&'a LockRow>,
    pub own_holding: Vec<&'a LockRow>,
    pub foreign_waiting: Vec<&'a LockRow>,
    pub foreign_holding: Vec<&'a LockRow>,
}

/// Whether a row belongs to one of this run's frontends.
///
/// With no attribution -- the driver did not carry `application_name`, or
/// `pg_stat_activity` did not show it -- no row can be called foreign, so
/// every row counts as this run's and the summary says so rather than
/// pretending the set is clean.
pub fn is_own_row(row: &LockRow, frontends: &[String]) -> bool {
    // An unreadable activity row is foreign even with no attribution at all:
    // nothing about it can be shown to belong to this run.
    row.activity_visible && (frontends.is_empty() || frontends.contains(&row.application_name))
}

/// How a row appears in a foreign name list.
pub fn row_label(row: &LockRow) -> String {
    if row.activity_visible {
        row.application_name.clone()
    } else {
        UNREADABLE_ACTIVITY.to_owned()
    }
}

/// Split one sample's rows. Pure, so the attribution it decides is testable
/// without a database.
pub fn split_lock_rows<'a>(rows: &'a [LockRow], frontends: &[String]) -> LockRowSplit<'a> {
    split_rows(rows.iter(), frontends)
}

/// The same split, restricted to one lock. A sample carries rows for every
/// sampled lock, and a summary must never mix two locks' queues: they are
/// different queues, taken by different server paths.
pub fn split_lock_rows_for<'a>(
    rows: &'a [LockRow],
    objid: i64,
    frontends: &[String],
) -> LockRowSplit<'a> {
    split_rows(rows.iter().filter(move |row| row.objid == objid), frontends)
}

fn split_rows<'a>(
    rows: impl Iterator<Item = &'a LockRow>,
    frontends: &[String],
) -> LockRowSplit<'a> {
    let mut split = LockRowSplit {
        own_waiting: Vec::new(),
        own_holding: Vec::new(),
        foreign_waiting: Vec::new(),
        foreign_holding: Vec::new(),
    };
    for row in rows {
        match (is_own_row(row, frontends), row.granted) {
            (true, false) => split.own_waiting.push(row),
            (true, true) => split.own_holding.push(row),
            (false, false) => split.foreign_waiting.push(row),
            (false, true) => split.foreign_holding.push(row),
        }
    }
    split
}

/// Both sampled locks over one phase, reported side by side.
///
/// The dense-cadence scenario measures the rebuild after a landing, and the
/// rebuild queues on `SETTLEMENT_LOCK` before it queues on `ORDER_LOCK`; a
/// harness that watched only `ORDER_LOCK` reported part of the queueing and
/// gave a reader no way to tell (EP-OBSERVABILITY).
#[derive(Clone, Debug, Serialize)]
pub struct PhaseLocks {
    pub order: LockSummary,
    pub settlement: LockSummary,
}

/// Side-connection sampler for PRISM advisory-lock waits.
pub struct LockSampler {
    samples: Arc<std::sync::Mutex<Vec<LockSample>>>,
    stop: Arc<AtomicBool>,
    interval: Duration,
    failure: Arc<std::sync::Mutex<Option<String>>>,
    /// `application_name` of each frontend, when the driver carried it.
    frontends: Vec<String>,
    attribution: String,
}

impl LockSampler {
    pub fn start(
        pool: PgPool,
        interval: Duration,
        frontends: Vec<String>,
        attribution: String,
    ) -> Self {
        let sampler = Self {
            samples: Arc::new(std::sync::Mutex::new(Vec::new())),
            stop: Arc::new(AtomicBool::new(false)),
            interval,
            failure: Arc::new(std::sync::Mutex::new(None)),
            frontends,
            attribution,
        };
        let samples = sampler.samples.clone();
        let stop = sampler.stop.clone();
        let failure = sampler.failure.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let started = Instant::now();
                // Granted rows are selected too: the holder of one of these
                // locks is what a foreign stall looks like, and filtering it
                // out made one invisible.
                // Both sampled locks come back from one poll, tagged with
                // their objid, so the two summaries cover exactly the same
                // instants and can be compared without an alignment argument.
                // `pg_stat_activity` is joined on the left, so a lock row
                // whose backend the sampler's role cannot read still appears
                // and is counted as foreign rather than disappearing. The
                // server's `clock_timestamp()` comes from the outer one-row
                // source, so every poll carries it, including a poll that
                // finds no lock at all: a sample never mixes the harness's
                // clock with the server's.
                let query = sqlx::query(
                    "WITH locks AS ( \
                       SELECT l.pid AS lock_pid, l.objid::bigint AS lock_objid, l.granted, \
                              l.waitstart, a.pid AS activity_pid, \
                              COALESCE(a.application_name,'') AS application_name \
                       FROM pg_locks l LEFT JOIN pg_stat_activity a ON a.pid = l.pid \
                       WHERE l.locktype = 'advisory' \
                         AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
                         AND l.classid = $1::bigint::oid \
                         AND l.objid IN ($2::bigint::oid, $3::bigint::oid) \
                         AND l.objsubid = 1 \
                     ) \
                     SELECT stamp.sampled_at, locks.lock_pid, locks.lock_objid, locks.granted, \
                            locks.waitstart, locks.activity_pid, locks.application_name \
                     FROM (SELECT clock_timestamp() AS sampled_at) stamp \
                     LEFT JOIN locks ON true",
                )
                .bind(PRISM_LOCK_CLASSID)
                .bind(ORDER_LOCK_OBJID)
                .bind(SETTLEMENT_LOCK_OBJID)
                .fetch_all(&pool)
                .await;
                match query {
                    Ok(rows) => {
                        // The outer source guarantees one row, so a missing
                        // server timestamp is a sampler failure, not a licence
                        // to substitute the harness's clock.
                        let server_time: Option<chrono::DateTime<chrono::Utc>> =
                            rows.first().and_then(|row| row.try_get("sampled_at").ok());
                        match server_time {
                            None => {
                                let mut state = failure.lock().expect("lock sampler failure");
                                state.get_or_insert_with(|| {
                                    "the sampler read no server clock_timestamp(), so its                                      samples would have mixed clocks"
                                        .to_owned()
                                });
                            }
                            Some(server_time) => {
                                let rows = rows
                                    .iter()
                                    // A poll that found no lock still returns
                                    // one row, with a null lock pid.
                                    .filter_map(|row| {
                                        let pid = row.try_get::<Option<i32>, _>("lock_pid").ok()??;
                                        let objid =
                                            row.try_get::<Option<i64>, _>("lock_objid").ok()??;
                                        Some(LockRow {
                                            pid,
                                            objid,
                                            granted: row
                                                .try_get::<Option<bool>, _>("granted")
                                                .ok()
                                                .flatten()
                                                .unwrap_or(false),
                                            waitstart: row
                                                .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(
                                                    "waitstart",
                                                )
                                                .unwrap_or(None),
                                            application_name: row
                                                .try_get::<String, _>("application_name")
                                                .unwrap_or_default(),
                                            activity_visible: row
                                                .try_get::<Option<i32>, _>("activity_pid")
                                                .ok()
                                                .flatten()
                                                .is_some(),
                                        })
                                    })
                                    .collect();
                                samples.lock().expect("lock sampler").push(LockSample {
                                    monotonic: Instant::now(),
                                    server_time,
                                    rows,
                                    query_millis: started.elapsed().as_secs_f64() * 1000.0,
                                });
                            }
                        }
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

    /// Both sampled locks over the same window, from the same polls.
    pub fn summarize(&self, since: Instant, until: Instant) -> PhaseLocks {
        let all = self.samples.lock().expect("lock sampler").clone();
        let failure = self.failure.lock().expect("lock sampler failure").clone();
        let window: Vec<LockSample> = all
            .into_iter()
            .filter(|s| s.monotonic >= since && s.monotonic <= until)
            .collect();
        PhaseLocks {
            order: self.summarize_lock(ORDER_LOCK, &window, failure.clone()),
            settlement: self.summarize_lock(SETTLEMENT_LOCK, &window, failure),
        }
    }

    /// One lock's picture, over rows already restricted to that lock.
    fn summarize_lock(
        &self,
        lock: SampledLock,
        window: &[LockSample],
        failure: Option<String>,
    ) -> LockSummary {
        summarize_lock_window(
            lock,
            window,
            failure,
            &self.frontends,
            &self.attribution,
            self.interval,
        )
    }
}

/// One lock's summary over `window`, the polls that fell inside a phase.
///
/// Pure, so the empty-window and real-zero cases are testable without a
/// database. An empty window is unknown, not quiet: every waiter count is
/// `None` and `unavailable_reason` says why. A window with polls and no
/// waiters is a measured zero and says so with `Some(0)` (EP-OBSERVABILITY).
pub fn summarize_lock_window(
    lock: SampledLock,
    window: &[LockSample],
    failure: Option<String>,
    frontends: &[String],
    attribution: &str,
    interval: Duration,
) -> LockSummary {
    let attributing = !frontends.is_empty();
    let mut summary = LockSummary {
        lock: lock.lock,
        key: lock.key,
        classid: PRISM_LOCK_CLASSID,
        objid: lock.objid,
        key_note: lock.key_note,
        taken_by: lock.taken_by,
        sample_interval_milliseconds: interval.as_secs_f64() * 1000.0,
        attribution: attribution.to_owned(),
        attributed_application_names: frontends.to_vec(),
        samples: window.len(),
        samples_with_waiters: None,
        max_waiters: None,
        mean_waiters: None,
        foreign_waiter_samples: None,
        foreign_application_names: Vec::new(),
        foreign_waiter_seconds_estimate: None,
        foreign_holder_samples: None,
        foreign_holder_application_names: Vec::new(),
        foreign_holder_seconds_estimate: None,
        foreign_contention_observed: None,
        waiter_seconds_estimate: None,
        episodes_at_least: None,
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
    let mut foreign_seconds = 0.0f64;
    let mut foreign_holder_seconds = 0.0f64;
    let mut foreign_names: BTreeMap<String, usize> = BTreeMap::new();
    let mut foreign_holder_names: BTreeMap<String, usize> = BTreeMap::new();
    let mut total_waiters = 0usize;
    let mut samples_with_waiters = 0usize;
    let mut max_waiters = 0usize;
    let mut foreign_waiter_samples = 0usize;
    let mut foreign_holder_samples = 0usize;
    let mut previous: Option<Instant> = None;
    let mut query_millis = Vec::with_capacity(window.len());
    for sample in window {
        query_millis.push(sample.query_millis);
        let split = split_lock_rows_for(&sample.rows, lock.objid, frontends);
        // Every waiter number stays over ungranted rows belonging to this
        // run, exactly as before; the granted rows are new information
        // beside them, never folded into them.
        let foreign = split.foreign_waiting.len();
        let foreign_holding = split.foreign_holding.len();
        let count = split.own_waiting.len();
        total_waiters += count;
        max_waiters = max_waiters.max(count);
        if count > 0 {
            samples_with_waiters += 1;
        }
        if foreign > 0 {
            foreign_waiter_samples += 1;
            for row in &split.foreign_waiting {
                *foreign_names.entry(row_label(row)).or_insert(0) += 1;
            }
        }
        if foreign_holding > 0 {
            foreign_holder_samples += 1;
            for row in &split.foreign_holding {
                *foreign_holder_names.entry(row_label(row)).or_insert(0) += 1;
            }
        }
        if let Some(last) = previous {
            let delta = sample
                .monotonic
                .saturating_duration_since(last)
                .as_secs_f64();
            waiter_seconds += count as f64 * delta;
            foreign_seconds += foreign as f64 * delta;
            foreign_holder_seconds += foreign_holding as f64 * delta;
        }
        previous = Some(sample.monotonic);
        for waiter in split.own_waiting {
            let key = (
                waiter.pid,
                waiter.waitstart.map(|w| w.to_rfc3339()).unwrap_or_default(),
            );
            let entry = episodes.entry(key).or_insert((
                waiter.waitstart.unwrap_or(sample.server_time),
                sample.server_time,
            ));
            entry.1 = sample.server_time;
        }
    }
    summary.samples_with_waiters = Some(samples_with_waiters);
    summary.max_waiters = Some(max_waiters);
    summary.mean_waiters = Some(total_waiters as f64 / window.len() as f64);
    summary.waiter_seconds_estimate = Some(waiter_seconds);
    summary.foreign_waiter_samples = Some(foreign_waiter_samples);
    summary.foreign_waiter_seconds_estimate = Some(foreign_seconds);
    summary.foreign_application_names = foreign_names.into_keys().collect();
    summary.foreign_holder_samples = Some(foreign_holder_samples);
    summary.foreign_holder_seconds_estimate = Some(foreign_holder_seconds);
    summary.foreign_holder_application_names = foreign_holder_names.into_keys().collect();
    summary.foreign_contention_observed =
        attributing.then_some(foreign_waiter_samples > 0 || foreign_holder_samples > 0);
    summary.episodes_at_least = Some(episodes.len());
    summary.longest_observed_wait_seconds = episodes
        .values()
        .map(|(start, last)| (*last - *start).num_milliseconds() as f64 / 1000.0)
        .max_by(f64::total_cmp);
    summary.sampler_query_millis_mean =
        Some(query_millis.iter().sum::<f64>() / query_millis.len() as f64);
    summary.sampler_query_millis_max = query_millis.iter().copied().max_by(f64::total_cmp);
    summary
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
    /// `_count` and `_sum` of each [`ACCOUNTED_SERIES`], keyed by
    /// [`AccountedSeries::key`]; see [`SeriesSample`] for when a missing
    /// series is a zero and when it is unknown.
    pub series: BTreeMap<String, SeriesSample>,
}

/// One histogram series' `_count` and `_sum` in one scrape; either is `None`
/// when that line was not exported. `declared` is whether the scrape carried
/// the family's `# TYPE` line: the server declares the lock families at start
/// and samples a series only once it observes it, so a declared family with
/// no line for the series is a real zero for that process, while an
/// undeclared one (a server without the family) is unknown.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct SeriesSample {
    pub declared: bool,
    pub count: Option<f64>,
    pub sum: Option<f64>,
}

impl SeriesSample {
    /// `(count, sum)`, zeros for a declared series not yet observed, `None`
    /// when unknown.
    fn values(&self) -> Option<(f64, f64)> {
        match (self.count, self.sum) {
            (Some(count), Some(sum)) => Some((count, sum)),
            (None, None) if self.declared => Some((0.0, 0.0)),
            _ => None,
        }
    }
}

/// A frontend histogram series the share-ack accounting reads (#829): its
/// family and the labels that select the series.
#[derive(Clone, Copy, Debug)]
pub struct AccountedSeries {
    pub key: &'static str,
    pub family: &'static str,
    pub labels: &'static [(&'static str, &'static str)],
}

pub const ORDER_LOCK_HOLD_APPEND: &str = "order_lock_hold_append";
pub const ORDER_LOCK_WAIT_GRANTED: &str = "order_lock_wait_granted";
pub const POOL_ACQUIRE_SUCCEEDED: &str = "pool_acquire_succeeded";

/// The server's existing histograms the accounting reads; the harness adds
/// no server metric (#829).
pub const ACCOUNTED_SERIES: [AccountedSeries; 3] = [
    AccountedSeries {
        key: ORDER_LOCK_HOLD_APPEND,
        family: "qbit_prism_database_order_lock_hold_seconds",
        labels: &[("holder", "append")],
    },
    AccountedSeries {
        key: ORDER_LOCK_WAIT_GRANTED,
        family: "qbit_prism_database_advisory_lock_wait_seconds",
        labels: &[("lock", "order"), ("result", "success")],
    },
    AccountedSeries {
        key: POOL_ACQUIRE_SUCCEEDED,
        family: "qbit_prism_database_pool_acquire_seconds",
        labels: &[("result", "success")],
    },
];

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
    parse_accounted_series(&body, &mut scrape);
    scrape.ok = true;
    scrape
}

/// Read each [`ACCOUNTED_SERIES`]' `_count` and `_sum` lines from a
/// `/metrics` body into `scrape.series`.
pub fn parse_accounted_series(body: &str, scrape: &mut MetricsScrape) {
    for line in body.lines() {
        let line = line.trim();
        if let Some(declaration) = line.strip_prefix("# TYPE ") {
            let family = declaration.split_whitespace().next().unwrap_or_default();
            for series in ACCOUNTED_SERIES
                .iter()
                .filter(|series| series.family == family)
            {
                scrape
                    .series
                    .entry(series.key.to_owned())
                    .or_default()
                    .declared = true;
            }
            continue;
        }
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let Some(value) = sample_value(line) else {
            continue;
        };
        let name = line.split(['{', ' ']).next().unwrap_or_default();
        for series in &ACCOUNTED_SERIES {
            let Some(suffix) = name.strip_prefix(series.family) else {
                continue;
            };
            if !series
                .labels
                .iter()
                .all(|(key, wanted)| label(line, key).as_deref() == Some(*wanted))
            {
                continue;
            }
            let sample = scrape.series.entry(series.key.to_owned()).or_default();
            match suffix {
                "_count" => sample.count = Some(value),
                "_sum" => sample.sum = Some(value),
                _ => {}
            }
        }
    }
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

/// The two scrapes that bracket a frontend restart: the old process's last
/// counters and the new process's first. The counters between them are a
/// reset, not traffic.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AckSplit {
    pub end_of_previous: MetricsScrape,
    pub start_of_next: MetricsScrape,
}

/// Bucket deltas over a phase for one frontend.
///
/// A frontend restart between the boundary scrapes resets its in-process
/// counters, so a plain `after - before` is negative or, worse, a plausible
/// small number. The delta is therefore assembled from one segment per
/// process, each bracketed by its own scrapes, and a reset that no segment
/// covers leaves the delta unknown -- never zero (EP-OBSERVABILITY).
#[derive(Clone, Debug, Serialize)]
pub struct ServerAckDelta {
    pub instance_id: String,
    pub metric: &'static str,
    pub boundary: &'static str,
    pub buckets_seconds: Vec<f64>,
    pub counts: BTreeMap<String, f64>,
    pub sums: BTreeMap<String, f64>,
    pub bucket_deltas: BTreeMap<String, BTreeMap<String, f64>>,
    /// Times the frontend's process was replaced during the phase, each of
    /// which reset its counters.
    pub counter_resets: usize,
    /// Segments summed into `counts`: one per process that ran during the
    /// phase. Zero when the delta is unavailable.
    pub segments: usize,
    /// Set when `counts` could not be assembled; `counts` is then empty,
    /// which means unknown, not zero.
    pub unavailable_reason: Option<String>,
    pub note: Option<String>,
}

impl ServerAckDelta {
    fn empty(instance_id: &str, counter_resets: usize) -> Self {
        Self {
            instance_id: instance_id.to_owned(),
            metric: "qbit_prism_share_ack_seconds",
            boundary: "server-side: complete submit frame receipt to completed response write",
            buckets_seconds: qbit_prism_server::metrics::BUCKETS.to_vec(),
            counts: BTreeMap::new(),
            sums: BTreeMap::new(),
            bucket_deltas: BTreeMap::new(),
            counter_resets,
            segments: 0,
            unavailable_reason: None,
            note: None,
        }
    }
}

/// One process's contribution: `to - from`, refused if either scrape failed
/// or any counter went backwards, which is a reset nobody recorded.
struct Segment {
    counts: BTreeMap<String, f64>,
    sums: BTreeMap<String, f64>,
    buckets: BTreeMap<String, BTreeMap<String, f64>>,
}

fn segment(from: &MetricsScrape, to: &MetricsScrape) -> std::result::Result<Segment, String> {
    if !from.ok || !to.ok {
        return Err(to
            .error
            .clone()
            .or_else(|| from.error.clone())
            .unwrap_or_else(|| "a boundary scrape failed".into()));
    }
    let backwards = |what: &str, result: &str, before: f64, after: f64| {
        format!(
            "{what} for result {result:?} went from {before} to {after} between two scrapes \
             without a recorded restart: the process was replaced, or the counters reset, and \
             the delta cannot be attributed"
        )
    };
    let mut counts = BTreeMap::new();
    for (result, after_count) in &to.ack_counts {
        let before_count = from.ack_counts.get(result).copied().unwrap_or(0.0);
        if *after_count < before_count {
            return Err(backwards("count", result, before_count, *after_count));
        }
        counts.insert(result.clone(), after_count - before_count);
    }
    let mut sums = BTreeMap::new();
    for (result, after_sum) in &to.ack_sums {
        let before_sum = from.ack_sums.get(result).copied().unwrap_or(0.0);
        if *after_sum < before_sum {
            return Err(backwards("sum", result, before_sum, *after_sum));
        }
        sums.insert(result.clone(), after_sum - before_sum);
    }
    let mut buckets: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for (result, after_buckets) in &to.ack_buckets {
        let empty = BTreeMap::new();
        let before_buckets = from.ack_buckets.get(result).unwrap_or(&empty);
        let entry = buckets.entry(result.clone()).or_default();
        for (le, value) in after_buckets {
            let before = before_buckets.get(le).copied().unwrap_or(0.0);
            if *value < before {
                return Err(backwards(
                    &format!("bucket le={le}"),
                    result,
                    before,
                    *value,
                ));
            }
            entry.insert(le.clone(), value - before);
        }
    }
    Ok(Segment {
        counts,
        sums,
        buckets,
    })
}

/// The `(from, to)` scrapes of each process that ran during a phase: the
/// phase's first scrape to the first restart's last, and so on to the
/// phase's last. `Err` when a restart has no scrape on each side.
fn segment_pairs<'a>(
    before: &'a MetricsScrape,
    splits: &[&'a AckSplit],
    after: &'a MetricsScrape,
    restarts: usize,
) -> std::result::Result<Vec<(&'a MetricsScrape, &'a MetricsScrape)>, String> {
    if splits.len() != restarts {
        return Err(format!(
            "the frontend restarted {restarts} time(s) during the phase and its counters reset \
             each time; {} of those restarts have a scrape on each side",
            splits.len()
        ));
    }
    let mut starts: Vec<&MetricsScrape> = vec![before];
    let mut ends: Vec<&MetricsScrape> = Vec::new();
    for split in splits {
        ends.push(&split.end_of_previous);
        starts.push(&split.start_of_next);
    }
    ends.push(after);
    Ok(starts.into_iter().zip(ends).collect())
}

/// The phase delta for a frontend that was not restarted.
pub fn ack_delta(before: &MetricsScrape, after: &MetricsScrape) -> ServerAckDelta {
    ack_delta_across_restarts(before, &[], after, 0)
}

/// The phase delta for a frontend restarted `restarts` times during the
/// phase, with `splits` bracketing each restart in order. A restart without a
/// split leaves the delta unavailable and says so.
pub fn ack_delta_across_restarts(
    before: &MetricsScrape,
    splits: &[&AckSplit],
    after: &MetricsScrape,
    restarts: usize,
) -> ServerAckDelta {
    let mut delta = ServerAckDelta::empty(&after.instance_id, restarts);
    let pairs = match segment_pairs(before, splits, after, restarts) {
        Ok(pairs) => pairs,
        Err(reason) => {
            delta.unavailable_reason = Some(format!(
                "{reason}, so the delta cannot be assembled and is unknown"
            ));
            return delta;
        }
    };
    for (from, to) in pairs {
        match segment(from, to) {
            Ok(part) => {
                for (result, value) in part.counts {
                    *delta.counts.entry(result).or_insert(0.0) += value;
                }
                for (result, value) in part.sums {
                    *delta.sums.entry(result).or_insert(0.0) += value;
                }
                for (result, buckets) in part.buckets {
                    let entry = delta.bucket_deltas.entry(result).or_default();
                    for (le, value) in buckets {
                        *entry.entry(le).or_insert(0.0) += value;
                    }
                }
                delta.segments += 1;
            }
            Err(reason) => {
                delta.counts.clear();
                delta.sums.clear();
                delta.bucket_deltas.clear();
                delta.segments = 0;
                delta.unavailable_reason = Some(reason);
                return delta;
            }
        }
    }
    if restarts > 0 {
        delta.note = Some(format!(
            "the frontend restarted {restarts} time(s) during the phase; the counters were \
             scraped on each side of every restart and the {} per-process segments summed",
            delta.segments
        ));
    }
    delta
}

/// One [`AccountedSeries`]' observations and seconds over a phase on one
/// frontend; both `None`, with the reason, when they cannot be assembled.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct SeriesDelta {
    pub count: Option<f64>,
    pub seconds: Option<f64>,
    pub unavailable_reason: Option<String>,
}

impl SeriesDelta {
    fn unknown(reason: String) -> Self {
        Self {
            count: None,
            seconds: None,
            unavailable_reason: Some(reason),
        }
    }
}

/// [`ack_delta_across_restarts`] for one accounted series: one segment per
/// process, each bracketed by its own scrapes. A failed scrape, a series a
/// scrape lacks, a counter that went backwards or an unbracketed restart
/// leaves it unknown, never zero (EP-OBSERVABILITY).
pub fn series_delta_across_restarts(
    key: &str,
    before: &MetricsScrape,
    splits: &[&AckSplit],
    after: &MetricsScrape,
    restarts: usize,
) -> SeriesDelta {
    let pairs = match segment_pairs(before, splits, after, restarts) {
        Ok(pairs) => pairs,
        Err(reason) => return SeriesDelta::unknown(reason),
    };
    let (mut count, mut seconds) = (0.0, 0.0);
    for (from, to) in pairs {
        if !from.ok || !to.ok {
            return SeriesDelta::unknown(
                to.error
                    .clone()
                    .or_else(|| from.error.clone())
                    .unwrap_or_else(|| "a boundary scrape failed".into()),
            );
        }
        let read = |scrape: &MetricsScrape| scrape.series.get(key).and_then(SeriesSample::values);
        let (Some((from_count, from_sum)), Some((to_count, to_sum))) = (read(from), read(to))
        else {
            return SeriesDelta::unknown(format!(
                "{key}: the frontend's /metrics did not export this series"
            ));
        };
        if to_count < from_count || to_sum < from_sum {
            return SeriesDelta::unknown(format!(
                "{key}: the counters went backwards between two scrapes without a recorded \
                 restart"
            ));
        }
        count += to_count - from_count;
        seconds += to_sum - from_sum;
    }
    SeriesDelta {
        count: Some(count),
        seconds: Some(seconds),
        unavailable_reason: None,
    }
}

/// `pg_stat_statements` for the share append's one server-side call
/// (migration 032's `qbit_prism_append_share`, #711) over a phase.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AppendCallStats {
    pub calls: i64,
    pub total_seconds: f64,
    /// `None` when the phase made no call.
    pub max_seconds: Option<f64>,
}

/// The append call's `pg_stat_statements` row since the phase's reset. No
/// row at all is a real zero: the extension answered and nothing called it.
pub async fn append_call_statement(pool: &PgPool) -> std::result::Result<AppendCallStats, String> {
    let row = sqlx::query(
        "SELECT coalesce(sum(calls), 0)::bigint AS calls, \
                coalesce(sum(total_exec_time), 0)::double precision AS total, \
                max(max_exec_time)::double precision AS max \
         FROM pg_stat_statements WHERE query LIKE '%qbit_prism_append_share(%'",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("pg_stat_statements could not be read: {error}"))?;
    let read = |error: sqlx::Error| format!("pg_stat_statements row unreadable: {error}");
    let calls: i64 = row.try_get("calls").map_err(read)?;
    let total: f64 = row.try_get("total").map_err(read)?;
    let max: Option<f64> = row.try_get("max").map_err(read)?;
    Ok(AppendCallStats {
        calls,
        total_seconds: total / 1000.0,
        max_seconds: max.map(|max| max / 1000.0),
    })
}

/// `pg_stat_wal` and the checkpointer's counters at one instant.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct WalCounters {
    pub wal_records: i64,
    pub wal_fpi: i64,
    pub wal_bytes: f64,
    pub wal_write: i64,
    pub wal_sync: i64,
    pub wal_write_time_ms: f64,
    pub wal_sync_time_ms: f64,
    /// `track_wal_io_timing` as the server had it; the two times are
    /// zeros it never measured when this is off.
    pub io_timing: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct CheckpointCounters {
    pub timed: i64,
    pub requested: i64,
    pub write_time_ms: f64,
    pub sync_time_ms: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DatabaseCounters {
    /// `pg_postmaster_start_time()`: counters read from a server that
    /// restarted, or from a promoted standby, are not one server's delta.
    pub server_started: std::result::Result<String, String>,
    pub wal: std::result::Result<WalCounters, String>,
    pub checkpoints: std::result::Result<CheckpointCounters, String>,
}

/// Read [`DatabaseCounters`]. PostgreSQL 16 keeps the checkpoint counters in
/// `pg_stat_bgwriter`, 17 in `pg_stat_checkpointer`, chosen by the server's
/// version; either half failing leaves it unknown with the error (18 moved
/// pg_stat_wal's write and sync columns to pg_stat_io, so its WAL half reads
/// unknown).
pub async fn database_counters(pool: &PgPool) -> DatabaseCounters {
    let server = sqlx::query(
        "SELECT pg_postmaster_start_time()::text AS started, \
                current_setting('server_version_num')::int AS version",
    )
    .fetch_one(pool)
    .await
    .and_then(|row| {
        Ok((
            row.try_get::<String, _>("started")?,
            row.try_get::<i32, _>("version")?,
        ))
    })
    .map_err(|error| format!("the server's start time could not be read: {error}"));
    let wal = async {
        let row = sqlx::query(
            "SELECT wal_records, wal_fpi, wal_bytes::double precision AS wal_bytes, wal_write, \
                    wal_sync, wal_write_time, wal_sync_time, \
                    current_setting('track_wal_io_timing')::bool AS io_timing \
             FROM pg_stat_wal",
        )
        .fetch_one(pool)
        .await?;
        Ok::<_, sqlx::Error>(WalCounters {
            wal_records: row.try_get("wal_records")?,
            wal_fpi: row.try_get("wal_fpi")?,
            wal_bytes: row.try_get("wal_bytes")?,
            wal_write: row.try_get("wal_write")?,
            wal_sync: row.try_get("wal_sync")?,
            wal_write_time_ms: row.try_get("wal_write_time")?,
            wal_sync_time_ms: row.try_get("wal_sync_time")?,
            io_timing: row.try_get("io_timing")?,
        })
    };
    // PostgreSQL 17 moved the checkpoint counters to pg_stat_checkpointer.
    let checkpoint_query = match &server {
        Ok((_, version)) if *version >= 170_000 => {
            "SELECT num_timed AS timed, num_requested AS requested, write_time, sync_time \
             FROM pg_stat_checkpointer"
        }
        _ => {
            "SELECT checkpoints_timed AS timed, checkpoints_req AS requested, \
                    checkpoint_write_time AS write_time, checkpoint_sync_time AS sync_time \
             FROM pg_stat_bgwriter"
        }
    };
    let checkpoints = async {
        let row = sqlx::query(checkpoint_query).fetch_one(pool).await?;
        Ok::<_, sqlx::Error>(CheckpointCounters {
            timed: row.try_get("timed")?,
            requested: row.try_get("requested")?,
            write_time_ms: row.try_get("write_time")?,
            sync_time_ms: row.try_get("sync_time")?,
        })
    };
    let (wal, checkpoints) = tokio::join!(wal, checkpoints);
    DatabaseCounters {
        server_started: server.map(|(started, _)| started),
        wal: wal.map_err(|error| format!("pg_stat_wal could not be read: {error}")),
        checkpoints: checkpoints
            .map_err(|error| format!("the checkpoint counters could not be read: {error}")),
    }
}

/// [`WalCounters`] over a phase. The times are `None` unless
/// `track_wal_io_timing` was on at both ends.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct WalDelta {
    pub records: Option<i64>,
    pub full_page_images: Option<i64>,
    pub bytes: Option<f64>,
    pub writes: Option<i64>,
    pub syncs: Option<i64>,
    pub write_seconds: Option<f64>,
    pub sync_seconds: Option<f64>,
    pub sync_mean_milliseconds: Option<f64>,
    pub unavailable_reason: Option<String>,
    pub timing_unavailable_reason: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct CheckpointDelta {
    /// Checkpoints the checkpointer counted during the phase. Timed ones
    /// include any it skipped for want of WAL activity (PostgreSQL 16 and 17
    /// count those too), and a checkpoint is counted when the checkpointer
    /// reports its statistics, so the log lines time them exactly.
    pub timed: Option<i64>,
    pub requested: Option<i64>,
    pub write_seconds: Option<f64>,
    pub sync_seconds: Option<f64>,
    pub unavailable_reason: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct DatabaseDelta {
    pub definition: &'static str,
    pub wal: WalDelta,
    pub checkpoints: CheckpointDelta,
}

pub const DATABASE_COUNTERS_DEFINITION: &str =
    "pg_stat_wal and the checkpointer's counters, read on the primary at the phase's start and \
     end; times are PostgreSQL's. These are cumulative statistics that backends publish at most \
     about once a second, so each boundary blurs by about that much";

const COUNTERS_WENT_BACKWARDS: &str =
    "the counters went backwards: the statistics were reset or the primary was replaced \
     during the phase";

/// Why two reads of `pg_postmaster_start_time()` are not one server's, or
/// `None` when they are: the primary restarted or a failover replaced it
/// between them, or either read failed.
pub fn server_replaced(
    before: &std::result::Result<String, String>,
    after: &std::result::Result<String, String>,
) -> Option<String> {
    match (before, after) {
        (Ok(from), Ok(to)) if from == to => None,
        (Ok(_), Ok(_)) => Some(
            "the primary restarted or was replaced during the phase, so its statistics are not \
             one server's"
                .to_owned(),
        ),
        (Err(error), _) | (_, Err(error)) => Some(error.clone()),
    }
}

pub fn database_delta(before: &DatabaseCounters, after: &DatabaseCounters) -> DatabaseDelta {
    if let Some(reason) = server_replaced(&before.server_started, &after.server_started) {
        return DatabaseDelta {
            definition: DATABASE_COUNTERS_DEFINITION,
            wal: WalDelta {
                unavailable_reason: Some(reason.clone()),
                ..Default::default()
            },
            checkpoints: CheckpointDelta {
                unavailable_reason: Some(reason),
                ..Default::default()
            },
        };
    }
    let wal = match (&before.wal, &after.wal) {
        (Err(error), _) | (_, Err(error)) => WalDelta {
            unavailable_reason: Some(error.clone()),
            ..Default::default()
        },
        (Ok(from), Ok(to))
            if to.wal_records < from.wal_records
                || to.wal_fpi < from.wal_fpi
                || to.wal_bytes < from.wal_bytes
                || to.wal_write < from.wal_write
                || to.wal_sync < from.wal_sync
                || to.wal_write_time_ms < from.wal_write_time_ms
                || to.wal_sync_time_ms < from.wal_sync_time_ms =>
        {
            WalDelta {
                unavailable_reason: Some(COUNTERS_WENT_BACKWARDS.into()),
                ..Default::default()
            }
        }
        (Ok(from), Ok(to)) => {
            let syncs = to.wal_sync - from.wal_sync;
            let timed = from.io_timing && to.io_timing;
            let sync_seconds =
                timed.then(|| (to.wal_sync_time_ms - from.wal_sync_time_ms) / 1000.0);
            WalDelta {
                records: Some(to.wal_records - from.wal_records),
                full_page_images: Some(to.wal_fpi - from.wal_fpi),
                bytes: Some(to.wal_bytes - from.wal_bytes),
                writes: Some(to.wal_write - from.wal_write),
                syncs: Some(syncs),
                write_seconds: timed
                    .then(|| (to.wal_write_time_ms - from.wal_write_time_ms) / 1000.0),
                sync_seconds,
                sync_mean_milliseconds: sync_seconds
                    .filter(|_| syncs > 0)
                    .map(|seconds| seconds * 1000.0 / syncs as f64),
                unavailable_reason: None,
                timing_unavailable_reason: (!timed).then(|| {
                    "track_wal_io_timing was off on the database, so PostgreSQL did not time \
                     WAL writes or syncs"
                        .into()
                }),
            }
        }
    };
    let checkpoints = match (&before.checkpoints, &after.checkpoints) {
        (Err(error), _) | (_, Err(error)) => CheckpointDelta {
            unavailable_reason: Some(error.clone()),
            ..Default::default()
        },
        (Ok(from), Ok(to))
            if to.timed < from.timed
                || to.requested < from.requested
                || to.write_time_ms < from.write_time_ms
                || to.sync_time_ms < from.sync_time_ms =>
        {
            CheckpointDelta {
                unavailable_reason: Some(COUNTERS_WENT_BACKWARDS.into()),
                ..Default::default()
            }
        }
        (Ok(from), Ok(to)) => CheckpointDelta {
            timed: Some(to.timed - from.timed),
            requested: Some(to.requested - from.requested),
            write_seconds: Some((to.write_time_ms - from.write_time_ms) / 1000.0),
            sync_seconds: Some((to.sync_time_ms - from.sync_time_ms) / 1000.0),
            unavailable_reason: None,
        },
    };
    DatabaseDelta {
        definition: DATABASE_COUNTERS_DEFINITION,
        wal,
        checkpoints,
    }
}

/// One number of the share-ack accounting: its value, or why it is unknown.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Accounted {
    pub seconds: Option<f64>,
    pub count: Option<f64>,
    /// `measured`, or `derived` by [`Self::formula`] from measured inputs.
    pub kind: &'static str,
    pub clock: &'static str,
    pub source: &'static str,
    pub formula: Option<&'static str>,
    pub unavailable_reason: Option<String>,
}

impl Accounted {
    fn measured(clock: &'static str, source: &'static str) -> Self {
        Self {
            seconds: None,
            count: None,
            kind: "measured",
            clock,
            source,
            formula: None,
            unavailable_reason: None,
        }
    }
}

/// Where a phase's accepted share acknowledgements spent their time (#829),
/// summed over every frontend. Measured numbers come from the frontends'
/// histograms and `pg_stat_statements`; the derived ones are named by their
/// formula and are unknown when any input is.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ShareAckAccounting {
    pub definition: &'static str,
    pub accepted_acks: Accounted,
    pub order_lock_wait: Accounted,
    pub order_lock_hold_append: Accounted,
    pub append_call: Accounted,
    pub append_call_max_seconds: Option<f64>,
    pub pool_acquire: Accounted,
    pub in_call_after_grant: Accounted,
    pub after_call: Accounted,
    pub before_lock_and_response: Accounted,
    pub commit_not_in_pg_stat_statements: &'static str,
}

pub const ACCOUNTING_DEFINITION: &str =
    "accepted share acknowledgements' summed seconds, split at ORDER_LOCK. Every frontend's \
     histogram deltas are summed; pg_stat_statements covers every frontend of the cluster. The \
     populations differ slightly (holds and waits include appends that were refused, waits \
     include the rebuild paths' few), so compare the counts beside each figure.";

/// `pg_stat_statements` cannot time a share's COMMIT: PostgreSQL commits the
/// transaction, and flushes its WAL, in `finish_xact_command` after the
/// extension's utility hook has stopped timing the `COMMIT` statement, so its
/// row reads about zero however long the flush took. The flush is read from
/// `pg_stat_wal` instead.
pub const COMMIT_NOT_IN_PG_STAT_STATEMENTS: &str =
    "COMMIT is deliberately not read from pg_stat_statements: PostgreSQL commits and flushes \
     WAL in finish_xact_command, after the extension stops timing the COMMIT statement, so its \
     row reads about zero whatever the flush took; the phase's database_counters.wal times \
     the flush instead";

fn sum_over_frontends(deltas: &[&SeriesDelta]) -> (Option<f64>, Option<f64>, Option<String>) {
    let mut count = 0.0;
    let mut seconds = 0.0;
    for delta in deltas {
        match (delta.count, delta.seconds) {
            (Some(c), Some(s)) => {
                count += c;
                seconds += s;
            }
            _ => {
                return (
                    None,
                    None,
                    delta
                        .unavailable_reason
                        .clone()
                        .or_else(|| Some("a frontend's delta is unknown".into())),
                )
            }
        }
    }
    if deltas.is_empty() {
        return (None, None, Some("no frontend was scraped".into()));
    }
    (Some(count), Some(seconds), None)
}

/// `minuend - subtrahends` when every input is known, else unknown, naming
/// the first unknown input and its reason.
fn derived(
    minuend: (&str, &Accounted),
    subtrahends: &[(&str, &Accounted)],
    formula: &'static str,
    meaning: &'static str,
) -> Accounted {
    let unknown = |(name, part): (&str, &Accounted)| {
        part.seconds.is_none().then(|| {
            format!(
                "{name} is unknown: {}",
                part.unavailable_reason
                    .as_deref()
                    .unwrap_or("no reason was given")
            )
        })
    };
    let mut seconds = minuend.1.seconds;
    let mut reason = unknown(minuend);
    for part in subtrahends {
        seconds = seconds.zip(part.1.seconds).map(|(a, b)| a - b);
        if reason.is_none() {
            reason = unknown(*part);
        }
    }
    // The inputs come from different populations (holds and waits include
    // refused appends, waits the rebuild paths') and different clocks. A
    // difference below zero is their disagreement, not a time.
    if let Some(negative) = seconds.filter(|value| *value < 0.0) {
        seconds = None;
        reason = Some(format!(
            "the inputs disagree: the formula gives {negative:.3} s; their populations or clocks \
             differ more than this part's own time"
        ));
    }
    Accounted {
        seconds,
        count: None,
        kind: "derived",
        clock: "mixed; each input's clock is named in the formula",
        source: meaning,
        formula: Some(formula),
        unavailable_reason: if seconds.is_some() {
            None
        } else {
            reason.or_else(|| Some("an input is unknown".into()))
        },
    }
}

/// Build a phase's [`ShareAckAccounting`] from each frontend's accepted-ack
/// delta and series deltas, and the phase's append-call statement.
pub fn share_ack_accounting(
    acks: &[ServerAckDelta],
    series: &[BTreeMap<String, SeriesDelta>],
    append_call: &std::result::Result<AppendCallStats, String>,
) -> ShareAckAccounting {
    let mut accepted_acks = Accounted::measured(
        "frontend monotonic",
        "qbit_prism_share_ack_seconds{result=\"accepted\"}: complete submit frame to completed \
         response write",
    );
    let accepted: Vec<SeriesDelta> = acks
        .iter()
        .map(|delta| {
            match (
                delta.counts.get("accepted"),
                delta.sums.get("accepted"),
                &delta.unavailable_reason,
            ) {
                (Some(count), Some(sum), None) => SeriesDelta {
                    count: Some(*count),
                    seconds: Some(*sum),
                    unavailable_reason: None,
                },
                (_, _, unavailable) => SeriesDelta::unknown(
                    unavailable
                        .clone()
                        .unwrap_or_else(|| "no accepted acknowledgement series".into()),
                ),
            }
        })
        .collect();
    (
        accepted_acks.count,
        accepted_acks.seconds,
        accepted_acks.unavailable_reason,
    ) = sum_over_frontends(&accepted.iter().collect::<Vec<_>>());
    let series_of = |key: &str, clock: &'static str, source: &'static str| {
        let missing = SeriesDelta::unknown(format!("{key}: no delta was recorded"));
        let deltas: Vec<&SeriesDelta> = series
            .iter()
            .map(|frontend| frontend.get(key).unwrap_or(&missing))
            .collect();
        let (count, seconds, unavailable_reason) = sum_over_frontends(&deltas);
        Accounted {
            seconds,
            count,
            unavailable_reason,
            ..Accounted::measured(clock, source)
        }
    };
    let order_lock_wait = series_of(
        ORDER_LOCK_WAIT_GRANTED,
        "PostgreSQL clock_timestamp() for share appends (the call's lock_waited_us); frontend \
         monotonic for the rebuild paths' lock statement",
        "qbit_prism_database_advisory_lock_wait_seconds{lock=\"order\",result=\"success\"}",
    );
    let order_lock_hold_append = series_of(
        ORDER_LOCK_HOLD_APPEND,
        "frontend monotonic, from the grant (the call's lock_held_us back-dated from its reply) \
         to the end of COMMIT",
        "qbit_prism_database_order_lock_hold_seconds{holder=\"append\"}",
    );
    let pool_acquire = series_of(
        POOL_ACQUIRE_SUCCEEDED,
        "frontend monotonic",
        "qbit_prism_database_pool_acquire_seconds{result=\"success\"}: every caller of the \
         ledger pool, each share's payout-revision read included; context only, not subtracted \
         below",
    );
    let mut append_call_seconds = Accounted::measured(
        "PostgreSQL executor (pg_stat_statements total_exec_time)",
        "pg_stat_statements, SELECT * FROM qbit_prism_append_share(...): the one-call append, \
         its ORDER_LOCK wait included",
    );
    let mut append_call_max_seconds = None;
    match append_call {
        Ok(stats) => {
            append_call_seconds.seconds = Some(stats.total_seconds);
            append_call_seconds.count = Some(stats.calls as f64);
            append_call_max_seconds = stats.max_seconds;
        }
        Err(reason) => append_call_seconds.unavailable_reason = Some(reason.clone()),
    }
    let in_call_after_grant = derived(
        ("append_call", &append_call_seconds),
        &[("order_lock_wait", &order_lock_wait)],
        "append_call (PostgreSQL executor) - order_lock_wait (PostgreSQL clock for appends)",
        "the call's time holding ORDER_LOCK",
    );
    let after_call = derived(
        ("order_lock_hold_append", &order_lock_hold_append),
        &[("in_call_after_grant", &in_call_after_grant)],
        "order_lock_hold_append (frontend monotonic) - in_call_after_grant (PostgreSQL)",
        "the call's reply in transit, the frontend runtime's turnaround to COMMIT, and the \
         COMMIT round trip with its WAL flush; the phase's database_counters.wal times the \
         flush itself",
    );
    let before_lock_and_response = derived(
        ("accepted_acks", &accepted_acks),
        &[
            ("order_lock_wait", &order_lock_wait),
            ("order_lock_hold_append", &order_lock_hold_append),
        ],
        "accepted_acks (frontend monotonic) - order_lock_wait (PostgreSQL clock for appends) - \
         order_lock_hold_append (frontend monotonic)",
        "not measured separately: the stratum's request handling, the submit checks (the \
         payout-revision read and its pool checkout among them), the append-admission queue, \
         the append's pool checkout and BEGIN, and the response write",
    );
    ShareAckAccounting {
        definition: ACCOUNTING_DEFINITION,
        accepted_acks,
        order_lock_wait,
        order_lock_hold_append,
        append_call: append_call_seconds,
        append_call_max_seconds,
        pool_acquire,
        in_call_after_grant,
        after_call,
        before_lock_and_response,
        commit_not_in_pg_stat_statements: COMMIT_NOT_IN_PG_STAT_STATEMENTS,
    }
}

/// Where `MemAvailable` is read from. Linux only: macOS has no `/proc`.
pub const MEMINFO_PATH: &str = "/proc/meminfo";

/// `MemAvailable`, in kibibytes, or `None` when [`MEMINFO_PATH`] cannot be
/// read or holds no such line.
pub fn mem_available_kib() -> Option<u64> {
    mem_available_kib_at(Path::new(MEMINFO_PATH))
}

/// [`mem_available_kib`] from a given file, so an unreadable source can be
/// tested on a host whose own `/proc/meminfo` is fine.
pub fn mem_available_kib_at(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path)
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// Refuse a `--min-mem-available-mib` floor the host cannot measure.
///
/// The floor is checked once a second against `MemAvailable`; when that
/// cannot be read, the check used to do nothing and the run proceeded
/// unguarded, with only a `null` `min_mem_available_kib` in the side report to
/// show for it -- on macOS, where there is no `/proc/meminfo`, on every run
/// (#485). A floor the run cannot enforce is refused at entry instead of
/// being passed through (EP-CONFIG); `--min-mem-available-mib 0` asks for no
/// floor and is accepted anywhere. `available_kib` is the entry read.
pub fn verify_memory_floor(floor_mib: u64, available_kib: Option<u64>) -> Result<()> {
    ensure!(
        floor_mib == 0 || available_kib.is_some(),
        "--min-mem-available-mib {floor_mib} cannot be enforced: MemAvailable could not be read \
         from {MEMINFO_PATH}, so the floor would never operate. Run on a Linux host, or pass \
         --min-mem-available-mib 0 to run without a memory floor"
    );
    Ok(())
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
