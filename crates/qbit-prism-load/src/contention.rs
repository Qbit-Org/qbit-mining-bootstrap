//! Host contention over a run (#842): the CPU time the hypervisor took from
//! this machine (steal), the CPU time spent waiting on IO (iowait), and the
//! kernel's pressure stall information (PSI) for CPU, IO and memory. Read
//! about once a second, then summarised per phase and over each tip's
//! delivery, so a slow tip can be set against what the host was doing while
//! it was delivered.
//!
//! EP-OBSERVABILITY: a counter the host does not expose -- no steal field in
//! `/proc/stat`, no `/proc/pressure` (a kernel without PSI, or macOS) -- is
//! unknown with its reason, never 0. Each figure names what it divides:
//! steal and iowait are shares of the CPU time the kernel accounted over
//! every CPU between two reads (USER_HZ ticks); the PSI figures are shares of
//! the harness's monotonic time from before the first of two reads to after
//! the second (the kernel's cumulative stall microseconds). CPU `full` is not
//! read: at the system level the kernel reports it as zero for compatibility,
//! which is not a measurement.

use serde::Serialize;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// What the `host_contention` blocks mean; the run-level block carries it.
pub const DEFINITION: &str =
    "Read from /proc/stat and /proc/pressure/{cpu,io,memory} about once per \
     sample_interval_seconds. steal and iowait: the counter's increase over the CPU time the \
     kernel accounted across every CPU between two reads (user, nice, system, idle, iowait, \
     irq, softirq, steal; USER_HZ ticks), in percent. cpu_some, io_some, io_full, memory_some, \
     memory_full: the PSI total's increase (microseconds some or all non-idle tasks were \
     stalled) over the harness's monotonic time from before the first of the two reads to after \
     the second, in percent: the time the totals can cover at most, so time the sampler lost \
     around a read lowers a share and never raises it. percent is \
     over every measured interval in the window, max_interval_percent the largest single \
     interval's; an interval overlapping the window counts, each figure's bounded by its own \
     source's reads, and covered_from_milliseconds and covered_to_milliseconds (relative to the \
     window's start) say what the intervals actually span. A figure the host does not expose is \
     null with unavailable_reason, never 0.";

/// The steal counter reads 0 both on a host whose hypervisor never took CPU
/// and on one whose hypervisor does not report it, so a zero steal figure is
/// read beside `steal_counter_at_start_ticks`.
pub const STEAL_NOTE: &str =
    "steal is what the hypervisor reports through the guest kernel. A counter still at 0 since \
     boot (steal_counter_at_start_ticks) cannot tell a hypervisor that never took CPU from one \
     that does not report steal, so a 0 steal figure on such a host is not evidence of an \
     uncontended host; a nonzero counter at start shows the hypervisor reports it.";

/// The aggregate `cpu` line of `/proc/stat`, in USER_HZ ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CpuTimes {
    /// user + nice + system + idle + iowait + irq + softirq + steal, as many
    /// of them as the line has (guest time is already inside user and nice).
    pub total: u64,
    pub iowait: Option<u64>,
    pub steal: Option<u64>,
}

/// Parse the aggregate `cpu` line of `/proc/stat`.
///
/// The first four fields (user, nice, system, idle) are required; iowait
/// (5th) and steal (8th) are `None` on a kernel whose line stops before
/// them. A field that is not a number refuses the line.
pub fn parse_proc_stat(text: &str) -> Result<CpuTimes, String> {
    let line = text
        .lines()
        .find(|line| line.split_whitespace().next() == Some("cpu"))
        .ok_or("/proc/stat has no aggregate cpu line")?;
    let fields: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .map(|field| {
            field
                .parse::<u64>()
                .map_err(|_| format!("/proc/stat cpu line has a non-numeric field {field:?}"))
        })
        .collect::<Result<_, _>>()?;
    if fields.len() < 4 {
        return Err(format!(
            "/proc/stat cpu line has {} fields; user, nice, system and idle are needed",
            fields.len()
        ));
    }
    let accounted = &fields[..fields.len().min(8)];
    let total = accounted
        .iter()
        .try_fold(0u64, |sum, value| sum.checked_add(*value))
        .ok_or("/proc/stat cpu fields overflow their sum")?;
    Ok(CpuTimes {
        total,
        iowait: fields.get(4).copied(),
        steal: fields.get(7).copied(),
    })
}

/// One `/proc/pressure/<resource>` file's cumulative totals, microseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PressureTotals {
    pub some_us: u64,
    /// `None` when the file has no `full` line (CPU before 5.13).
    pub full_us: Option<u64>,
}

/// Parse a PSI file: a `some` line, and a `full` line where the kernel
/// has one, each ending in `total=<microseconds>`.
pub fn parse_pressure(text: &str) -> Result<PressureTotals, String> {
    let total = |kind: &str| -> Result<Option<u64>, String> {
        let Some(line) = text
            .lines()
            .find(|line| line.split_whitespace().next() == Some(kind))
        else {
            return Ok(None);
        };
        let value = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("total="))
            .ok_or_else(|| format!("the {kind} line has no total= field"))?;
        value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("the {kind} line's total={value:?} is not a number"))
    };
    Ok(PressureTotals {
        some_us: total("some")?.ok_or("no some line")?,
        full_us: total("full")?,
    })
}

/// One PSI file's totals and the harness instants its read lay between.
///
/// The totals are the kernel's as of some moment inside the read, and the
/// harness cannot tell which: the sampler can lose the CPU before the read,
/// or between the read and the stamp after it. The stall between two reads
/// of a file therefore covers at most the time from `before` of the first to
/// `after` of the second. A share divides by that time, so a sampler
/// preempted around a read reports a lower share, never one above what the
/// kernel counted. Each file has its own instants, so a sampler preempted
/// between two files skews neither.
#[derive(Clone, Copy, Debug)]
pub struct PressureRead {
    /// Taken before the file was read.
    pub before: Instant,
    /// Taken after its text came back.
    pub after: Instant,
    pub totals: PressureTotals,
}

pub type Pressure = Result<PressureRead, String>;

/// One read of every source. `at` is when `/proc/stat` was read, and places
/// the read; each pressure file carries its own instants.
#[derive(Clone, Debug)]
pub struct HostReading {
    pub at: Instant,
    pub cpu: Result<CpuTimes, String>,
    pub cpu_pressure: Pressure,
    pub io_pressure: Pressure,
    pub memory_pressure: Pressure,
}

/// Read every source under `proc_root` (`/proc` on a real host).
pub fn read_host_at(proc_root: &Path) -> HostReading {
    read_host_with(|relative| {
        let path = proc_root.join(relative);
        std::fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))
    })
}

/// Read every source through `read`, which takes a path relative to `/proc`.
/// A pressure file is stamped on both sides of its read.
pub fn read_host_with(read: impl Fn(&str) -> Result<String, String>) -> HostReading {
    let pressure = |resource: &str| -> Pressure {
        let before = Instant::now();
        let text = read(&format!("pressure/{resource}"));
        let after = Instant::now();
        parse_pressure(&text?).map(|totals| PressureRead {
            before,
            after,
            totals,
        })
    };
    let cpu = read("stat").and_then(|text| parse_proc_stat(&text));
    let at = Instant::now();
    HostReading {
        at,
        cpu,
        cpu_pressure: pressure("cpu"),
        io_pressure: pressure("io"),
        memory_pressure: pressure("memory"),
    }
}

/// One figure over a window.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Share {
    /// Over every measured interval in the window, in percent.
    pub percent: Option<f64>,
    /// The largest single interval's share, in percent.
    pub max_interval_percent: Option<f64>,
    /// Intervals between consecutive reads of this figure's own source that
    /// overlap the window: a pressure file's interval runs from before its
    /// first read to after its second, so a sampler preempted between its
    /// reads moves no file's interval onto another's. Two neighbouring
    /// intervals of a file therefore share the read between them, and a
    /// window edge that falls inside that read counts both.
    pub intervals: usize,
    /// Of those, the ones the figure was measured over.
    pub measured_intervals: usize,
    /// What the measured intervals span, in milliseconds relative to the
    /// window's start (harness monotonic).
    pub covered_from_milliseconds: Option<f64>,
    pub covered_to_milliseconds: Option<f64>,
    pub unavailable_reason: Option<String>,
}

/// The host over one window: a phase, or one tip's delivery.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct HostContention {
    /// Intervals between consecutive `/proc/stat` reads that overlap the
    /// window, which place the reads; each figure counts its own.
    pub intervals: usize,
    /// What those intervals span, in milliseconds relative to the window's
    /// start (harness monotonic): the first can begin before it and the
    /// last end after it.
    pub covered_from_milliseconds: Option<f64>,
    pub covered_to_milliseconds: Option<f64>,
    pub steal: Share,
    pub iowait: Share,
    pub cpu_some: Share,
    pub io_some: Share,
    pub io_full: Share,
    pub memory_some: Share,
    pub memory_full: Share,
}

/// One interval between two reads of a source: its bounds on the harness's
/// monotonic clock, and its numerator and denominator or why it has none.
struct Span {
    start: Instant,
    end: Instant,
    part: Result<(f64, f64), String>,
}

/// Milliseconds from `from` to `at`, negative before it.
fn offset(from: Instant, at: Instant) -> f64 {
    if at >= from {
        at.duration_since(from).as_secs_f64() * 1000.0
    } else {
        -(from.duration_since(at).as_secs_f64() * 1000.0)
    }
}

/// Accumulate one figure's intervals in a window into a [`Share`].
fn share(spans: &[Span], from: Instant) -> Share {
    let intervals = spans.len();
    let unknown = |reason: String| Share {
        percent: None,
        max_interval_percent: None,
        intervals,
        measured_intervals: 0,
        covered_from_milliseconds: None,
        covered_to_milliseconds: None,
        unavailable_reason: Some(reason),
    };
    if intervals == 0 {
        return unknown("no two reads span the window".into());
    }
    let mut numerator = 0.0;
    let mut denominator = 0.0;
    let mut max: Option<f64> = None;
    let mut measured = 0;
    let mut covered: Option<(Instant, Instant)> = None;
    let mut reason = None;
    for span in spans {
        match &span.part {
            Ok((value, over)) if *over > 0.0 => {
                numerator += value;
                denominator += over;
                measured += 1;
                let percent = value / over * 100.0;
                max = Some(max.map_or(percent, |m: f64| m.max(percent)));
                covered = Some(covered.map_or((span.start, span.end), |(first, last)| {
                    (first.min(span.start), last.max(span.end))
                }));
            }
            Ok(_) => {
                reason.get_or_insert_with(|| "an interval accounted no time".to_owned());
            }
            Err(error) => {
                reason.get_or_insert_with(|| error.clone());
            }
        }
    }
    if measured == 0 {
        return unknown(reason.unwrap_or_else(|| "no interval was measured".into()));
    }
    Share {
        percent: Some(numerator / denominator * 100.0),
        max_interval_percent: max,
        intervals,
        measured_intervals: measured,
        covered_from_milliseconds: covered.map(|(first, _)| offset(from, first)),
        covered_to_milliseconds: covered.map(|(_, last)| offset(from, last)),
        unavailable_reason: (measured < intervals).then(|| {
            format!(
                "measured over {measured} of {intervals} intervals: {}",
                reason.unwrap_or_default()
            )
        }),
    }
}

/// A counter's increase between two reads, or why there is none: `missing`
/// when a read lacks it, and a named refusal when it went backwards.
fn increase(
    before: Option<u64>,
    after: Option<u64>,
    name: &str,
    missing: &str,
) -> Result<f64, String> {
    match (before, after) {
        (Some(before), Some(after)) if after >= before => Ok((after - before) as f64),
        (Some(_), Some(_)) => Err(format!("the {name} counter went backwards")),
        _ => Err(missing.to_owned()),
    }
}

/// The host over `[from, until]`, from every pair of consecutive reads whose
/// interval overlaps it, each figure's interval bounded by its own source's
/// reads. Pure, so the unknown and zero cases are testable.
pub fn summarize(readings: &[HostReading], from: Instant, until: Instant) -> HostContention {
    let overlaps = |start: Instant, end: Instant| end > from && start <= until && end > start;
    let pairs: Vec<(&HostReading, &HostReading)> = readings
        .windows(2)
        .map(|pair| (&pair[0], &pair[1]))
        .filter(|(a, b)| overlaps(a.at, b.at))
        .collect();
    let cpu = |pick: fn(&CpuTimes) -> Option<u64>, name: &str, missing: &str| -> Share {
        let spans: Vec<Span> = pairs
            .iter()
            .map(|(a, b)| Span {
                start: a.at,
                end: b.at,
                part: match (&a.cpu, &b.cpu) {
                    (Ok(x), Ok(y)) => increase(pick(x), pick(y), name, missing).and_then(|value| {
                        let total =
                            increase(Some(x.total), Some(y.total), "CPU total", "no CPU total")?;
                        Ok((value, total))
                    }),
                    (Err(error), _) | (_, Err(error)) => Err(error.clone()),
                },
            })
            .collect();
        share(&spans, from)
    };
    let psi =
        |pick: fn(&HostReading) -> &Pressure, full: bool, name: &str, missing: &str| -> Share {
            let spans: Vec<Span> = readings
                .windows(2)
                .filter_map(|pair| {
                    let (a, b) = (&pair[0], &pair[1]);
                    let span = match (pick(a), pick(b)) {
                        (Ok(x), Ok(y)) => {
                            let (before, after) = if full {
                                (x.totals.full_us, y.totals.full_us)
                            } else {
                                (Some(x.totals.some_us), Some(y.totals.some_us))
                            };
                            // The longest the two totals can be apart: see
                            // `PressureRead`.
                            let (start, end) = (x.before, y.after);
                            Span {
                                start,
                                end,
                                part: increase(before, after, name, missing).map(|stalled| {
                                    let wall =
                                        end.saturating_duration_since(start).as_secs_f64() * 1e6;
                                    (stalled, wall)
                                }),
                            }
                        }
                        // An unreadable file has no instant of its own; the
                        // /proc/stat reads place the interval it left unmeasured.
                        (Err(error), _) | (_, Err(error)) => Span {
                            start: a.at,
                            end: b.at,
                            part: Err(error.clone()),
                        },
                    };
                    overlaps(span.start, span.end).then_some(span)
                })
                .collect();
            share(&spans, from)
        };
    HostContention {
        intervals: pairs.len(),
        covered_from_milliseconds: pairs.first().map(|(a, _)| offset(from, a.at)),
        covered_to_milliseconds: pairs.last().map(|(_, b)| offset(from, b.at)),
        steal: cpu(
            |c| c.steal,
            "steal",
            "this kernel's /proc/stat has no steal field",
        ),
        iowait: cpu(
            |c| c.iowait,
            "iowait",
            "this kernel's /proc/stat has no iowait field",
        ),
        cpu_some: psi(|r| &r.cpu_pressure, false, "cpu some", "no cpu some total"),
        io_some: psi(|r| &r.io_pressure, false, "io some", "no io some total"),
        io_full: psi(
            |r| &r.io_pressure,
            true,
            "io full",
            "/proc/pressure/io has no full line",
        ),
        memory_some: psi(
            |r| &r.memory_pressure,
            false,
            "memory some",
            "no memory some total",
        ),
        memory_full: psi(
            |r| &r.memory_pressure,
            true,
            "memory full",
            "/proc/pressure/memory has no full line",
        ),
    }
}

/// The run-level block: what was read, how often, and whether the steal
/// counter is known to be live on this host.
pub fn run_block(readings: &[HostReading], interval: Duration) -> Value {
    let first = readings.first();
    let source = |pick: fn(&HostReading) -> Option<String>| -> Value {
        match first {
            None => json!("no read was taken"),
            Some(reading) => match pick(reading) {
                None => json!("read at start"),
                Some(error) => json!(error),
            },
        }
    };
    json!({
        "definition": DEFINITION,
        "clock": "harness monotonic between reads; the counters are the kernel's",
        "sources_note": "each source as the first read found it; a later failure is in the \
                         figures' own unavailable_reason",
        "sample_interval_seconds": interval.as_secs_f64(),
        "reads": readings.len(),
        "sources": {
            "proc_stat": source(|r| r.cpu.as_ref().err().cloned()),
            "pressure_cpu": source(|r| r.cpu_pressure.as_ref().err().cloned()),
            "pressure_io": source(|r| r.io_pressure.as_ref().err().cloned()),
            "pressure_memory": source(|r| r.memory_pressure.as_ref().err().cloned()),
        },
        "steal_counter_at_start_ticks": first
            .and_then(|r| r.cpu.as_ref().ok())
            .and_then(|cpu| cpu.steal),
        "steal_note": STEAL_NOTE,
    })
}

/// Reads the host about once per interval until stopped.
pub struct HostSampler {
    readings: Arc<std::sync::Mutex<Vec<HostReading>>>,
    stop: Arc<AtomicBool>,
    interval: Duration,
    proc_root: PathBuf,
}

impl HostSampler {
    /// Start reading `proc_root` (`/proc`); the first read is taken at once.
    pub fn start(proc_root: PathBuf, interval: Duration) -> Self {
        let sampler = Self {
            readings: Arc::new(std::sync::Mutex::new(Vec::new())),
            stop: Arc::new(AtomicBool::new(false)),
            interval,
            proc_root: proc_root.clone(),
        };
        let readings = sampler.readings.clone();
        let stop = sampler.stop.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let reading = read_host_at(&proc_root);
                readings.lock().expect("host sampler").push(reading);
                tokio::time::sleep(interval).await;
            }
        });
        sampler
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Stop, and take one last read now, so the run's final interval and
    /// a tip stamped after the last periodic read still have a read after
    /// them however short the teardown was.
    pub fn finish(&self) {
        self.stop();
        let reading = read_host_at(&self.proc_root);
        self.readings.lock().expect("host sampler").push(reading);
    }

    pub fn readings(&self) -> Vec<HostReading> {
        self.readings.lock().expect("host sampler").clone()
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }
}

/// A run that returns early still stops reading the host.
impl Drop for HostSampler {
    fn drop(&mut self) {
        self.stop();
    }
}
