//! Long soak (#575 item 2): what a soak samples, and what it is held to.
//!
//! A soak is one server lifetime under hours of realistic load. Two drivers
//! produce the same per-sample record ([`Sample`], one JSON line each):
//!
//! - the load harness's `--plan soak` ([`crate::soak_driver`]), on regtest,
//!   every `sample_seconds`, reading the frontends' `/proc` entries and the
//!   database directly;
//! - `qbit-prism-soak-report` (testnet4 and any other live deployment), every
//!   hour, reading Prometheus and a read-only database URL.
//!
//! [`evaluate`] holds either series to one [`Gates`] block, so the regtest
//! soak and a multi-day testnet4 soak are judged by the same code: resident
//! memory and open descriptors must stop growing after a warm-up, database
//! connections must not drift, WAL must stay bounded, and the share ledger
//! must have rolled over into new partitions and archived old ones as often
//! as the soak asks.
//!
//! EP-OBSERVABILITY: a figure a sample could not read is `null` with the
//! reason beside it, never zero, and a gate whose inputs are unknown fails
//! with that reason rather than passing on the samples that happened to read.

use crate::gate::Check;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::Path;

pub const SAMPLE_SCHEMA: &str = "qbit.prism.soak-sample.v1";
pub const SAMPLES_FILE: &str = "soak-samples.jsonl";
pub const EVENTS_FILE: &str = "soak-events.jsonl";
pub const REPORT_FILE: &str = "soak-report.md";

/// The flags a looped preset contributes to its segment of a soak: what the
/// load looks like, never what the server or the cluster is. Everything
/// else -- frontends, sessions, window, pool fee, database and runtime
/// sizes, template bits, the population -- is the soak preset's own, because
/// one server lifetime has one configuration.
pub const WORKLOAD_FLAGS: &[&str] = &[
    "--plan",
    "--rate",
    "--arrival",
    "--background-shares-per-second",
    "--warmup-seconds",
    "--steady-state-seconds",
    "--steady-state-rate",
    "--burst-seconds",
    "--burst-rate",
    "--reconnect-seconds",
    "--reconnect-target",
    "--slow-database-seconds",
    "--slow-db-delay-ms",
    "--external-tips",
    "--scheduled-blocks",
    "--cadence",
    "--cadence-seconds",
    "--cadence-rate",
    "--cadence-gaps",
    "--churn-seconds",
    "--churn-rate",
    "--churn-tips",
    "--rental-bursts",
    "--rental-burst-window-seconds",
    "--rental-burst-interval-seconds",
    "--rental-lifetime",
    "--rental-hashrate",
    "--reconnect-storms",
    "--storm-interval-seconds",
    "--storm-reconnect-seconds",
    "--mid-flight-kill",
];

/// A soak preset's `soak` block. Every key is required.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    /// Scheduled load, in minutes. Whole cycles of the looped presets are
    /// planned while they fit; nothing is cut short.
    pub minutes: u64,
    /// The presets whose workload is looped, in order, by name.
    pub presets: Vec<String>,
    /// Seconds between samples.
    pub sample_seconds: u64,
    /// Minutes into the soak at which the share ledger is rolled into its
    /// next partition. See [`crate::soak_driver`].
    pub rollover_minutes: Vec<f64>,
    /// Rows left below the partition bound when a rollover advances the share
    /// sequence, so the live load itself crosses the bound.
    pub rollover_margin_rows: i64,
    /// Minutes between retention passes (`share-archive` seal, archive,
    /// verify, then detach and drop once `plan` clears the partition).
    pub archive_every_minutes: f64,
    /// `share-archive --window-multiple` for those passes.
    pub archive_window_multiple: i64,
    /// `share-archive --retention-days` for those passes.
    pub archive_retention_days: i64,
    /// The `PRISM_SHARE_PARTITION_ENSURE_INTERVAL_SECONDS` every frontend runs
    /// with, so a new lead partition is attached within seconds of a
    /// rollover rather than a minute.
    pub partition_ensure_interval_seconds: u64,
    pub gates: Gates,
}

/// What a soak is held to. Every key is required, `null` included.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Gates {
    /// Samples before this many minutes are the warm-up: reported, never
    /// fitted.
    pub warmup_minutes: f64,
    /// Fewest post-warm-up samples a trend may be fitted over, per process.
    pub min_gated_samples: u64,
    /// Least-squares slope of resident memory's envelope after the warm-up,
    /// per process, MiB per hour: the peak of each `rss_trend_window_minutes`
    /// window, fitted over the windows. A block landing's audit build lifts
    /// the resident set by hundreds of MiB for minutes at a time on a 400k
    /// window; a window at least as long as the landing cadence holds one in
    /// every peak alike, so landings shift the envelope and a leak tilts it.
    pub rss_slope_mib_per_hour_max: f64,
    /// The window the resident-memory envelope is taken over, minutes.
    pub rss_trend_window_minutes: f64,
    /// Fewest whole windows the envelope may be fitted over.
    pub min_trend_windows: u64,
    /// The resident-set bound of `docs/prism-capacity-readiness.md`: after
    /// the warm-up, resident memory stays within this multiple of the
    /// warm-up's peak. `null` does not gate on it.
    pub rss_warmup_peak_multiple_max: Option<f64>,
    /// Least-squares slope of open file descriptors after the warm-up, per
    /// process, per hour.
    pub fd_slope_per_hour_max: f64,
    /// Most database connections one server process may hold at any sample.
    pub pool_connections_max: u64,
    /// Mean connections over the last quarter of the post-warm-up samples
    /// minus the mean over the first quarter, per process.
    pub pool_connections_drift_max: f64,
    /// Largest `pg_wal` the database may hold at any sample, bytes.
    pub wal_bytes_max: u64,
    /// Partition bounds the share sequence must cross during the soak.
    pub min_rollovers: u64,
    /// Partitions that must leave the ledger (detached and dropped after a
    /// verified archive) during the soak.
    pub min_archive_cycles: u64,
    /// Whether a sampled process whose PID changes fails the soak: a regtest
    /// soak is one server lifetime by construction; a deployment may be
    /// restarted by its operator, and a restart restarts the trend.
    pub one_lifetime: bool,
}

impl Spec {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.minutes > 0, "soak.minutes must be positive");
        ensure!(!self.presets.is_empty(), "soak.presets names no preset");
        ensure!(
            (5..=3600).contains(&self.sample_seconds),
            "soak.sample_seconds must be 5..3600"
        );
        let mut last = 0.0;
        for minute in &self.rollover_minutes {
            ensure!(
                minute.is_finite() && *minute > last,
                "soak.rollover_minutes must be positive, finite and increasing"
            );
            last = *minute;
        }
        ensure!(
            self.rollover_margin_rows >= 100,
            "soak.rollover_margin_rows must be at least 100"
        );
        ensure!(
            self.archive_every_minutes.is_finite() && self.archive_every_minutes > 0.0,
            "soak.archive_every_minutes must be positive"
        );
        ensure!(
            (1..=1024).contains(&self.archive_window_multiple),
            "soak.archive_window_multiple must be 1..1024, as share-archive's is"
        );
        ensure!(
            (0..=36_500).contains(&self.archive_retention_days),
            "soak.archive_retention_days must be 0..36500, as share-archive's is"
        );
        ensure!(
            (1..=86_400).contains(&self.partition_ensure_interval_seconds),
            "soak.partition_ensure_interval_seconds must be 1..86400, as the server's is"
        );
        self.gates.validate()
    }
}

impl Gates {
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("warmup_minutes", self.warmup_minutes),
            ("rss_trend_window_minutes", self.rss_trend_window_minutes),
            (
                "rss_slope_mib_per_hour_max",
                self.rss_slope_mib_per_hour_max,
            ),
            ("fd_slope_per_hour_max", self.fd_slope_per_hour_max),
            (
                "pool_connections_drift_max",
                self.pool_connections_drift_max,
            ),
        ] {
            ensure!(
                value.is_finite() && value >= 0.0,
                "soak gate {name} must be finite and not negative"
            );
        }
        ensure!(
            self.min_gated_samples >= 3,
            "soak gate min_gated_samples must be at least 3: a slope needs points"
        );
        ensure!(
            self.min_trend_windows >= 3 && self.rss_trend_window_minutes > 0.0,
            "soak gates min_trend_windows must be at least 3 and rss_trend_window_minutes \
             positive: an envelope slope needs windows"
        );
        ensure!(
            self.wal_bytes_max > 0,
            "soak gate wal_bytes_max must be positive"
        );
        if let Some(multiple) = self.rss_warmup_peak_multiple_max {
            ensure!(
                multiple.is_finite() && multiple >= 1.0,
                "soak gate rss_warmup_peak_multiple_max must be at least 1"
            );
        }
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let gates: Self =
            serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        gates.validate()?;
        Ok(gates)
    }
}

// --- samples ---------------------------------------------------------------

/// One server process at one sample.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ProcessPoint {
    pub instance: String,
    /// The process ID; for a deployment read through Prometheus, which does
    /// not export it, the ordinal of the process's lifetime (1, 2, ...) as
    /// the resets of its acknowledged-share counter reveal them.
    pub pid: Option<u32>,
    pub rss_bytes: Option<u64>,
    pub open_fds: Option<u64>,
    pub threads: Option<u64>,
    /// The process's acknowledged-share counter, where it is read from
    /// Prometheus (`qbit_prism_accepted_shares_total`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_total: Option<u64>,
    /// Why a `null` above is unknown.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unknown: Vec<String>,
}

/// One share ledger partition as the catalog and PostgreSQL held it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PartitionPoint {
    pub name: String,
    pub state: String,
    pub lower_seq: Option<i64>,
    pub upper_seq: i64,
    /// `pg_total_relation_size`; `null` once the relation is gone.
    pub bytes: Option<u64>,
}

/// The database at one sample.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct DatabasePoint {
    /// Connections to this database by client ([`CONNECTION_KEY`]); `null`
    /// when `pg_stat_activity` could not be read. An empty map is a read
    /// that found no client.
    pub connections: Option<BTreeMap<String, u64>>,
    /// Connections idle inside a transaction for over a minute; `null` when
    /// the sampling role cannot see other roles' session states.
    pub idle_in_transaction_over_60s: Option<u64>,
    pub wal_bytes: Option<u64>,
    pub next_share_seq: Option<i64>,
    pub database_bytes: Option<u64>,
    #[serde(default)]
    pub partitions: Vec<PartitionPoint>,
    /// The largest relations of the schema, total size in bytes.
    #[serde(default)]
    pub relations: BTreeMap<String, u64>,
    /// Payout divergence rows (#478), for a deployment soak's reconciliation.
    pub payout_divergences: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unknown: Vec<String>,
}

/// Share acknowledgement latency over the interval that ended at the sample.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct LatencyPoint {
    /// Where the figure came from: the harness's own clients, or the servers'
    /// `share_ack_seconds` histogram.
    pub source: String,
    pub interval_seconds: f64,
    pub acknowledged: Option<u64>,
    pub p50_ms: Option<f64>,
    pub p99_ms: Option<f64>,
}

/// Acknowledged against committed shares, for a soak that cannot see its
/// miners' side (a deployment). Cumulative from the first sample.
///
/// A Prometheus counter is as old as its last scrape, so the acknowledged
/// count lags the moment it is read, never leads it: committed rows are
/// counted up to the sample's own time, and from `tolerance_seconds` before
/// the first sample, which covers the first reading's lag. Every share a
/// server acknowledged inside the span is then a committed row inside it,
/// and `committed >= acknowledged` is the check. A counter that could not be
/// read, or restarted between two readings, drops that stretch from the
/// acknowledged side only, which can hide a loss but never invent one.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct LedgerPoint {
    /// Shares the servers counted as accepted (acknowledged to the miner)
    /// since the first sample, summed over processes and across restarts.
    pub acknowledged_since_start: Option<u64>,
    /// Samples whose counters could not be read, so their stretch is missing
    /// from `acknowledged_since_start`.
    #[serde(default)]
    pub acknowledged_gaps: u64,
    /// Accepted rows the ledger holds with `accepted_at` in the span.
    pub committed_since_start: Option<u64>,
    /// Of those, the rows from the `tolerance_seconds` before the first
    /// sample.
    pub tolerance_rows: Option<u64>,
    pub tolerance_seconds: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Sample {
    pub schema: String,
    pub at: DateTime<Utc>,
    /// Seconds since the soak's first sample.
    pub elapsed_seconds: f64,
    /// The harness phase the sample was taken in, when there is one.
    pub phase: Option<String>,
    /// Whether the session population was the base population (no rental
    /// churn in flight), so descriptor and memory counts compare across
    /// samples. A deployment's samples are all steady.
    pub steady: bool,
    pub processes: Vec<ProcessPoint>,
    pub database: DatabasePoint,
    pub latency: Option<LatencyPoint>,
    pub ledger: Option<LedgerPoint>,
}

pub fn append_line<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    writeln!(file, "{}", serde_json::to_string(value)?)?;
    file.sync_data()?;
    Ok(())
}

pub fn read_samples(path: &Path) -> Result<Vec<Sample>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut samples = Vec::new();
    for (number, line) in std::io::BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let sample: Sample = serde_json::from_str(&line)
            .with_context(|| format!("{}:{}", path.display(), number + 1))?;
        ensure!(
            sample.schema == SAMPLE_SCHEMA,
            "{}:{}: schema {:?}, not {SAMPLE_SCHEMA}",
            path.display(),
            number + 1,
            sample.schema
        );
        samples.push(sample);
    }
    Ok(samples)
}

// --- process sampling (Linux /proc) ----------------------------------------

/// Resident memory, open descriptors and threads of `pid`, each `None` with
/// the reason when it cannot be read.
pub fn process_point(instance: &str, pid: Option<u32>) -> ProcessPoint {
    let mut point = ProcessPoint {
        instance: instance.to_owned(),
        pid,
        ..ProcessPoint::default()
    };
    let Some(pid) = pid else {
        point.unknown.push("the process is not running".into());
        return point;
    };
    match std::fs::read_to_string(format!("/proc/{pid}/status")) {
        Ok(status) => {
            point.rss_bytes = status_field_kib(&status, "VmRSS:").map(|kib| kib * 1024);
            point.threads = status_field_kib(&status, "Threads:");
            if point.rss_bytes.is_none() {
                point.unknown.push("VmRSS missing from /proc status".into());
            }
        }
        Err(error) => point.unknown.push(format!("/proc/{pid}/status: {error}")),
    }
    match std::fs::read_dir(format!("/proc/{pid}/fd")) {
        Ok(entries) => point.open_fds = Some(entries.count() as u64),
        Err(error) => point.unknown.push(format!("/proc/{pid}/fd: {error}")),
    }
    point
}

fn status_field_kib(status: &str, key: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|value| value.parse().ok())
}

// --- database sampling -----------------------------------------------------

/// How connections are attributed to a client: the `application_name` when
/// the client set one (the harness names each frontend's), else the role
/// and address it connected from.
pub const CONNECTION_KEY: &str = "CASE WHEN application_name <> '' THEN application_name \
     ELSE usename || '@' || COALESCE(host(client_addr), 'local') END";

/// Everything [`DatabasePoint`] holds, each part independently: a part the
/// role may not read is `null` with its reason, and the rest still reads.
/// `only`, when given, keeps the connections of those clients and no others
/// (the harness's frontends, not its own side pools).
pub async fn database_point(pool: &PgPool, only: Option<&[String]>) -> DatabasePoint {
    let mut point = DatabasePoint::default();
    let connections = sqlx::query(&format!(
        "SELECT {CONNECTION_KEY} AS client, count(*)::bigint AS n, \
         count(*) FILTER (WHERE state IS NULL)::bigint AS hidden, \
         count(*) FILTER (WHERE state LIKE 'idle in transaction%' \
             AND clock_timestamp() - state_change > interval '60 seconds')::bigint AS stuck \
         FROM pg_stat_activity WHERE datname = current_database() AND backend_type = 'client backend' \
         AND pid <> pg_backend_pid() GROUP BY 1"
    ))
    .fetch_all(pool)
    .await;
    match connections {
        Ok(rows) => {
            let mut stuck = 0u64;
            let mut hidden = 0u64;
            let mut clients = BTreeMap::new();
            for row in rows {
                let client: String = row.try_get("client").unwrap_or_default();
                if only.is_some_and(|keep| !keep.contains(&client)) {
                    continue;
                }
                let n: i64 = row.try_get("n").unwrap_or(0);
                hidden += row.try_get::<i64, _>("hidden").unwrap_or(0) as u64;
                stuck += row.try_get::<i64, _>("stuck").unwrap_or(0) as u64;
                clients.insert(client, n as u64);
            }
            point.connections = Some(clients);
            if hidden == 0 {
                point.idle_in_transaction_over_60s = Some(stuck);
            } else {
                point.unknown.push(format!(
                    "idle_in_transaction_over_60s: {hidden} session state(s) are hidden from \
                     this role (grant pg_read_all_stats)"
                ));
            }
        }
        Err(error) => point.unknown.push(format!("connections: {error}")),
    }
    match sqlx::query_scalar::<_, Option<i64>>("SELECT sum(size)::bigint FROM pg_ls_waldir()")
        .fetch_one(pool)
        .await
    {
        Ok(bytes) => point.wal_bytes = bytes.map(|b| b as u64),
        Err(error) => point.unknown.push(format!(
            "wal_bytes: pg_ls_waldir() refused ({error}); grant pg_monitor"
        )),
    }
    match sqlx::query_scalar::<_, i64>("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await
    {
        Ok(bytes) => point.database_bytes = Some(bytes as u64),
        Err(error) => point.unknown.push(format!("database_bytes: {error}")),
    }
    match sqlx::query_scalar::<_, i64>("SELECT qbit_prism_share_next_seq()")
        .fetch_one(pool)
        .await
    {
        Ok(next) => point.next_share_seq = Some(next),
        Err(error) => point.unknown.push(format!("next_share_seq: {error}")),
    }
    match sqlx::query(
        "SELECT partition_name, state, lower_seq, upper_seq, \
         CASE WHEN to_regclass(partition_name) IS NULL THEN NULL \
              ELSE pg_total_relation_size(to_regclass(partition_name)) END AS bytes \
         FROM qbit_prism_share_partitions ORDER BY upper_seq",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            for row in rows {
                point.partitions.push(PartitionPoint {
                    name: row.try_get("partition_name").unwrap_or_default(),
                    state: row.try_get("state").unwrap_or_default(),
                    lower_seq: row.try_get("lower_seq").unwrap_or(None),
                    upper_seq: row.try_get("upper_seq").unwrap_or(0),
                    bytes: row
                        .try_get::<Option<i64>, _>("bytes")
                        .unwrap_or(None)
                        .map(|b| b as u64),
                });
            }
        }
        Err(error) => point.unknown.push(format!("partitions: {error}")),
    }
    // Partitions are reported above; every other relation of the schema by
    // total size, the largest twenty.
    match sqlx::query(
        "SELECT c.relname::text AS name, pg_total_relation_size(c.oid)::bigint AS bytes \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r','p') AND NOT c.relispartition \
         AND n.nspname = current_schema() AND c.relname NOT LIKE 'qbit_share_ledger_p%' \
         ORDER BY 2 DESC LIMIT 20",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            for row in rows {
                let name: String = row.try_get("name").unwrap_or_default();
                let bytes: i64 = row.try_get("bytes").unwrap_or(0);
                point.relations.insert(name, bytes as u64);
            }
        }
        Err(error) => point.unknown.push(format!("relations: {error}")),
    }
    match sqlx::query_scalar::<_, i64>("SELECT count(*)::bigint FROM qbit_prism_payout_divergences")
        .fetch_one(pool)
        .await
    {
        Ok(rows) => point.payout_divergences = Some(rows as u64),
        Err(error) => point.unknown.push(format!("payout_divergences: {error}")),
    }
    point
}

// --- evaluation ------------------------------------------------------------

/// Least-squares slope of `(x, y)` in y-units per x-unit; `None` under two
/// distinct x values.
pub fn slope(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    let n = points.len() as f64;
    let mean_x = points.iter().map(|p| p.0).sum::<f64>() / n;
    let mean_y = points.iter().map(|p| p.1).sum::<f64>() / n;
    let sxx: f64 = points.iter().map(|p| (p.0 - mean_x).powi(2)).sum();
    if sxx <= 0.0 {
        return None;
    }
    let sxy: f64 = points.iter().map(|p| (p.0 - mean_x) * (p.1 - mean_y)).sum();
    Some(sxy / sxx)
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn fail(name: impl Into<String>, observed: String, budget: String) -> Check {
    Check {
        name: name.into(),
        observed,
        budget,
        pass: Some(false),
    }
}

fn verdict(name: impl Into<String>, observed: String, budget: String, pass: bool) -> Check {
    Check {
        name: name.into(),
        observed,
        budget,
        pass: Some(pass),
    }
}

fn info(name: impl Into<String>, observed: String) -> Check {
    Check {
        name: name.into(),
        observed,
        budget: String::new(),
        pass: None,
    }
}

/// Every process instance any sample names, in first-seen order.
fn instances(samples: &[Sample]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for sample in samples {
        for process in &sample.processes {
            if !names.contains(&process.instance) {
                names.push(process.instance.clone());
            }
        }
    }
    names
}

/// What one trend gate reads and holds: a process figure, divided by
/// `scale` into `unit`, whose slope per hour must be at most `max`. With a
/// `window`, the slope is fitted over each whole window's peak instead of
/// over every sample.
struct Trend<'a, F> {
    label: &'a str,
    unit: &'a str,
    max: f64,
    scale: f64,
    window: Option<(f64, u64)>,
    value: F,
}

/// The peak of each whole `window_hours` window of `(hours, value)` points,
/// at the window's middle. Windows start at the first point; a trailing
/// partial window is left out, since its peak would be read over less time.
pub fn window_peaks(points: &[(f64, f64)], window_hours: f64) -> Vec<(f64, f64)> {
    let Some(&(start, _)) = points.first() else {
        return Vec::new();
    };
    let end = points.last().map_or(start, |p| p.0);
    let whole = ((end - start) / window_hours).floor() as usize;
    (0..whole)
        .filter_map(|index| {
            let from = start + index as f64 * window_hours;
            let to = from + window_hours;
            points
                .iter()
                .filter(|p| p.0 >= from && p.0 < to)
                .map(|p| p.1)
                .reduce(f64::max)
                .map(|peak| (from + window_hours / 2.0, peak))
        })
        .collect()
}

/// A per-process trend over the steady post-warm-up samples, in units per
/// hour, held to the trend's `max`.
fn trend_check<F: Fn(&ProcessPoint) -> Option<u64>>(
    trend: Trend<'_, F>,
    instance: &str,
    gated: &[&Sample],
    gates: &Gates,
) -> Check {
    let Trend {
        label,
        unit,
        max,
        scale,
        window,
        value,
    } = trend;
    let name = format!("{label} slope after warm-up, {instance}");
    let budget = match window {
        Some((minutes, windows)) => {
            format!("<= {max} {unit}/h over the peaks of >= {windows} whole {minutes} min windows")
        }
        None => format!(
            "<= {max} {unit}/h over >= {} samples",
            gates.min_gated_samples
        ),
    };
    let mut points = Vec::new();
    let mut unknown = 0usize;
    for sample in gated {
        let Some(process) = sample.processes.iter().find(|p| p.instance == instance) else {
            unknown += 1;
            continue;
        };
        match value(process) {
            Some(v) => points.push((sample.elapsed_seconds / 3600.0, v as f64 / scale)),
            None => unknown += 1,
        }
    }
    if (points.len() as u64) < gates.min_gated_samples {
        return fail(
            name,
            format!(
                "unknown: {} readable steady sample(s) after warm-up ({unknown} unreadable)",
                points.len()
            ),
            budget,
        );
    }
    let (fitted, what) = match window {
        Some((minutes, windows)) => {
            let peaks = window_peaks(&points, minutes / 60.0);
            if (peaks.len() as u64) < windows {
                return fail(
                    name,
                    format!(
                        "unknown: {} whole window(s) after warm-up from {} sample(s)",
                        peaks.len(),
                        points.len()
                    ),
                    budget,
                );
            }
            let what = format!("window peaks of {} samples", points.len());
            (peaks, what)
        }
        None => (points.clone(), "samples".to_owned()),
    };
    match slope(&fitted) {
        Some(per_hour) => {
            let first = fitted.first().map(|p| p.1).unwrap_or_default();
            let last = fitted.last().map(|p| p.1).unwrap_or_default();
            verdict(
                name,
                format!(
                    "{per_hour:+.2} {unit}/h over {} {what} ({first:.1} -> {last:.1} {unit}){}",
                    fitted.len(),
                    if unknown > 0 {
                        format!(", {unknown} unreadable")
                    } else {
                        String::new()
                    }
                ),
                budget,
                per_hour <= max,
            )
        }
        None => fail(name, "unknown: the samples span no time".into(), budget),
    }
}

/// Partition bounds the share sequence crossed between the first and the last
/// sample that read it.
pub fn rollovers(samples: &[Sample]) -> Option<(u64, i64, i64)> {
    let first = samples.iter().find_map(|s| s.database.next_share_seq)?;
    let last = samples
        .iter()
        .rev()
        .find_map(|s| s.database.next_share_seq)?;
    let mut bounds: Vec<i64> = samples
        .iter()
        .flat_map(|s| s.database.partitions.iter().map(|p| p.upper_seq))
        .collect();
    bounds.sort_unstable();
    bounds.dedup();
    let crossed = bounds
        .iter()
        .filter(|bound| first < **bound && **bound <= last)
        .count() as u64;
    Some((crossed, first, last))
}

/// Partitions that were in the ledger at the first sample that listed any
/// and were dropped by the last.
pub fn archive_cycles(samples: &[Sample]) -> Option<Vec<String>> {
    let first = samples.iter().find(|s| !s.database.partitions.is_empty())?;
    let last = samples
        .iter()
        .rev()
        .find(|s| !s.database.partitions.is_empty())?;
    let before: BTreeMap<&str, &str> = first
        .database
        .partitions
        .iter()
        .map(|p| (p.name.as_str(), p.state.as_str()))
        .collect();
    Some(
        last.database
            .partitions
            .iter()
            .filter(|p| p.state == "dropped")
            .filter(|p| {
                before
                    .get(p.name.as_str())
                    .is_none_or(|state| *state != "dropped")
            })
            .map(|p| p.name.clone())
            .collect(),
    )
}

/// Hold a soak's samples to its gates. Checks only one driver can make (the
/// harness's retention commands, say) are added by that driver's caller:
/// see [`gate_harness_run`].
pub fn evaluate(samples: &[Sample], gates: &Gates) -> Vec<Check> {
    let mut checks = Vec::new();
    let Some(last) = samples.last() else {
        checks.push(fail(
            "soak samples",
            "unknown: no sample was recorded".into(),
            "at least one".into(),
        ));
        return checks;
    };
    let warmup = gates.warmup_minutes * 60.0;
    checks.push(info(
        "soak span",
        format!(
            "{} samples over {:.2} h ({:.2} h after a {:.0} min warm-up)",
            samples.len(),
            last.elapsed_seconds / 3600.0,
            ((last.elapsed_seconds - warmup) / 3600.0).max(0.0),
            gates.warmup_minutes
        ),
    ));
    let after: Vec<&Sample> = samples
        .iter()
        .filter(|s| s.elapsed_seconds >= warmup)
        .collect();
    let steady: Vec<&Sample> = after.iter().copied().filter(|s| s.steady).collect();
    let names = instances(samples);
    if names.is_empty() {
        checks.push(fail(
            "server processes",
            "unknown: no sample names a server process".into(),
            "at least one".into(),
        ));
    }
    for instance in &names {
        let mut pids: Vec<u32> = samples
            .iter()
            .flat_map(|s| s.processes.iter())
            .filter(|p| p.instance == *instance)
            .filter_map(|p| p.pid)
            .collect();
        pids.dedup();
        let lifetimes = pids.len();
        if gates.one_lifetime {
            checks.push(verdict(
                format!("one server lifetime, {instance}"),
                format!("{lifetimes} PID(s): {pids:?}"),
                "1".into(),
                lifetimes == 1,
            ));
        } else {
            checks.push(info(
                format!("server lifetimes, {instance}"),
                format!("{lifetimes} PID(s): {pids:?}"),
            ));
        }
        // A restart restarts every trend: only the last lifetime is fitted.
        let current = pids.last().copied();
        let lifetime: Vec<&Sample> = steady
            .iter()
            .copied()
            .filter(|s| {
                s.processes
                    .iter()
                    .find(|p| p.instance == *instance)
                    .is_some_and(|p| p.pid == current)
            })
            .collect();
        if let Some(multiple) = gates.rss_warmup_peak_multiple_max {
            let peak = |early: bool| {
                samples
                    .iter()
                    .filter(|s| (s.elapsed_seconds < warmup) == early)
                    .filter_map(|s| s.processes.iter().find(|p| p.instance == *instance))
                    .filter(|p| p.pid == current)
                    .filter_map(|p| p.rss_bytes)
                    .max()
            };
            let name = format!("resident memory against the warm-up peak, {instance}");
            let budget = format!("<= {multiple}x the warm-up peak");
            checks.push(match (peak(true), peak(false)) {
                (Some(warm), Some(after)) => verdict(
                    name,
                    format!(
                        "peak {:.1} MiB after, {:.1} MiB in the warm-up ({:.2}x)",
                        after as f64 / 1048576.0,
                        warm as f64 / 1048576.0,
                        after as f64 / warm.max(1) as f64
                    ),
                    budget,
                    after as f64 <= multiple * warm as f64,
                ),
                _ => fail(
                    name,
                    "unknown: no readable sample on one side of the warm-up".into(),
                    budget,
                ),
            });
        }
        checks.push(trend_check(
            Trend {
                label: "resident memory",
                unit: "MiB",
                max: gates.rss_slope_mib_per_hour_max,
                scale: 1024.0 * 1024.0,
                window: Some((gates.rss_trend_window_minutes, gates.min_trend_windows)),
                value: |p: &ProcessPoint| p.rss_bytes,
            },
            instance,
            &lifetime,
            gates,
        ));
        checks.push(trend_check(
            Trend {
                label: "open file descriptors",
                unit: "fds",
                max: gates.fd_slope_per_hour_max,
                scale: 1.0,
                window: None,
                value: |p: &ProcessPoint| p.open_fds,
            },
            instance,
            &lifetime,
            gates,
        ));
    }

    // Connections are per client key; a server process's key is its
    // instance name when the samples come from the harness, and whatever
    // the deployment's clients connect as otherwise.
    let clients: Vec<String> = {
        let mut keys: Vec<String> = Vec::new();
        for sample in samples {
            for key in sample.database.connections.iter().flat_map(|c| c.keys()) {
                if !keys.contains(key) {
                    keys.push(key.clone());
                }
            }
        }
        keys
    };
    let unread = samples
        .iter()
        .filter(|s| s.database.connections.is_none())
        .count();
    if unread > 0 {
        checks.push(fail(
            "database connections",
            format!(
                "unknown: {unread} of {} sample(s) could not read pg_stat_activity",
                samples.len()
            ),
            format!("<= {} per client", gates.pool_connections_max),
        ));
    }
    for client in &clients {
        let peak = samples
            .iter()
            .filter_map(|s| s.database.connections.as_ref()?.get(client))
            .max()
            .copied()
            .unwrap_or(0);
        checks.push(verdict(
            format!("database connections, {client}"),
            format!("peak {peak}"),
            format!("<= {}", gates.pool_connections_max),
            peak <= gates.pool_connections_max,
        ));
        // A read that did not list the client is a real zero for it.
        let series: Vec<f64> = after
            .iter()
            .filter_map(|s| s.database.connections.as_ref())
            .map(|clients| *clients.get(client).unwrap_or(&0) as f64)
            .collect();
        let quarter = series.len() / 4;
        let name = format!("database connection drift, {client}");
        let budget = format!(
            "<= {} (last quarter mean - first quarter mean)",
            gates.pool_connections_drift_max
        );
        if quarter == 0 {
            checks.push(fail(
                name,
                format!("unknown: {} post-warm-up sample(s)", series.len()),
                budget,
            ));
        } else {
            let first = mean(&series[..quarter]).unwrap_or_default();
            let last = mean(&series[series.len() - quarter..]).unwrap_or_default();
            checks.push(verdict(
                name,
                format!("{:+.2} ({first:.2} -> {last:.2})", last - first),
                budget,
                last - first <= gates.pool_connections_drift_max,
            ));
        }
    }
    let stuck: Vec<Option<u64>> = samples
        .iter()
        .map(|s| s.database.idle_in_transaction_over_60s)
        .collect();
    let unknown_stuck = stuck.iter().filter(|v| v.is_none()).count();
    let worst_stuck = stuck.iter().flatten().max().copied();
    checks.push(match worst_stuck {
        _ if unknown_stuck > 0 => fail(
            "idle in transaction over 60 s",
            format!("unknown in {unknown_stuck} of {} sample(s)", samples.len()),
            "0".into(),
        ),
        Some(worst) => verdict(
            "idle in transaction over 60 s",
            format!("peak {worst}"),
            "0".into(),
            worst == 0,
        ),
        None => fail(
            "idle in transaction over 60 s",
            "unknown: no sample".into(),
            "0".into(),
        ),
    });

    let wal: Vec<Option<u64>> = samples.iter().map(|s| s.database.wal_bytes).collect();
    let unknown_wal = wal.iter().filter(|v| v.is_none()).count();
    let peak_wal = wal.iter().flatten().max().copied();
    let wal_budget = format!("<= {} MiB", gates.wal_bytes_max / (1024 * 1024));
    checks.push(match peak_wal {
        _ if unknown_wal > 0 => fail(
            "WAL size",
            format!("unknown in {unknown_wal} of {} sample(s)", samples.len()),
            wal_budget,
        ),
        Some(peak) => verdict(
            "WAL size",
            format!("peak {} MiB", peak / (1024 * 1024)),
            wal_budget,
            peak <= gates.wal_bytes_max,
        ),
        None => fail("WAL size", "unknown: no sample".into(), wal_budget),
    });

    checks.push(match rollovers(samples) {
        Some((crossed, first, last)) => verdict(
            "share partition rollovers",
            format!("{crossed} bound(s) crossed (share_seq {first} -> {last})"),
            format!(">= {}", gates.min_rollovers),
            crossed >= gates.min_rollovers,
        ),
        None => fail(
            "share partition rollovers",
            "unknown: no sample read the share sequence".into(),
            format!(">= {}", gates.min_rollovers),
        ),
    });
    checks.push(match archive_cycles(samples) {
        Some(dropped) => verdict(
            "share partitions archived and dropped",
            if dropped.is_empty() {
                "none".into()
            } else {
                format!("{}: {}", dropped.len(), dropped.join(", "))
            },
            format!(">= {}", gates.min_archive_cycles),
            dropped.len() as u64 >= gates.min_archive_cycles,
        ),
        None => fail(
            "share partitions archived and dropped",
            "unknown: no sample read the partition catalog".into(),
            format!(">= {}", gates.min_archive_cycles),
        ),
    });

    // Deployment soaks: acknowledged against committed, and payout
    // divergences recorded during the soak.
    if let Some(ledger) = samples.iter().rev().find_map(|s| s.ledger.as_ref()) {
        checks.push(
            match (
                ledger.acknowledged_since_start,
                ledger.committed_since_start,
            ) {
                (Some(acked), Some(committed)) => verdict(
                    "acknowledged shares in the ledger",
                    format!(
                        "{committed} committed ({} from the {} s before the start) of {acked} \
                         acknowledged{}",
                        ledger
                            .tolerance_rows
                            .map_or("unknown".into(), |rows| rows.to_string()),
                        ledger.tolerance_seconds,
                        if ledger.acknowledged_gaps > 0 {
                            format!(
                                "; {} sample(s) could not read the counters",
                                ledger.acknowledged_gaps
                            )
                        } else {
                            String::new()
                        }
                    ),
                    "committed >= acknowledged".into(),
                    committed >= acked,
                ),
                _ => fail(
                    "acknowledged shares in the ledger",
                    "unknown: the last sample could not read both counts".into(),
                    "committed >= acknowledged".into(),
                ),
            },
        );
    }
    let divergences: Vec<u64> = samples
        .iter()
        .filter_map(|s| s.database.payout_divergences)
        .collect();
    if let (Some(first), Some(last)) = (divergences.first(), divergences.last()) {
        checks.push(verdict(
            "payout divergences recorded during the soak",
            format!("{}", last.saturating_sub(*first)),
            "0".into(),
            last <= first,
        ));
    }

    // Latency is reported, not gated: the harness's per-phase gates and the
    // deployment's alerting hold it; a soak shows whether it drifted.
    let latencies: Vec<(f64, f64)> = after
        .iter()
        .filter_map(|s| {
            s.latency
                .as_ref()
                .and_then(|l| l.p99_ms)
                .map(|p99| (s.elapsed_seconds / 3600.0, p99))
        })
        .collect();
    if let Some(per_hour) = slope(&latencies) {
        let worst = latencies.iter().map(|p| p.1).fold(0.0, f64::max);
        checks.push(info(
            "share ACK p99 per sample after warm-up",
            format!(
                "slope {per_hour:+.1} ms/h over {} samples, worst {worst:.1} ms",
                latencies.len()
            ),
        ));
    }
    checks
}

/// The Markdown report: the verdict table, then the trend of each process.
pub fn markdown(title: &str, samples: &[Sample], checks: &[Check]) -> String {
    let mut text = crate::gate::markdown(title, checks);
    text.push_str("\n#### Samples\n\n| elapsed h | phase | ");
    let names = instances(samples);
    for name in &names {
        text.push_str(&format!("{name} RSS MiB | {name} fds | "));
    }
    text.push_str("connections | WAL MiB | next share_seq |\n|---|---|");
    for _ in &names {
        text.push_str("---|---|");
    }
    text.push_str("---|---|---|\n");
    // At most 48 rows: evenly spaced, always including the last.
    let step = samples.len().div_ceil(48).max(1);
    for (index, sample) in samples.iter().enumerate() {
        if index % step != 0 && index + 1 != samples.len() {
            continue;
        }
        text.push_str(&format!(
            "| {:.2} | {} | ",
            sample.elapsed_seconds / 3600.0,
            sample.phase.as_deref().unwrap_or("")
        ));
        for name in &names {
            let process = sample.processes.iter().find(|p| p.instance == *name);
            let rss = process
                .and_then(|p| p.rss_bytes)
                .map(|b| format!("{:.1}", b as f64 / 1048576.0))
                .unwrap_or_else(|| "unknown".into());
            let fds = process
                .and_then(|p| p.open_fds)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".into());
            text.push_str(&format!("{rss} | {fds} | "));
        }
        let connections = sample
            .database
            .connections
            .as_ref()
            .map(|clients| clients.values().sum::<u64>().to_string())
            .unwrap_or_else(|| "unknown".into());
        text.push_str(&format!(
            "{connections} | {} | {} |\n",
            sample
                .database
                .wal_bytes
                .map(|b| format!("{:.0}", b as f64 / 1048576.0))
                .unwrap_or_else(|| "unknown".into()),
            sample
                .database
                .next_share_seq
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".into()),
        ));
    }
    text
}

/// The soak block of a preset file, or `None` for a preset without one.
pub fn from_preset_value(value: Option<&serde_json::Value>) -> Result<Option<Spec>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let spec: Spec = serde_json::from_value(value.clone()).context("parsing the soak block")?;
    spec.validate()?;
    Ok(Some(spec))
}

/// The gate over a harness soak: the samples beside the report, held to the
/// preset's soak gates, plus the retention driver's own record. The
/// harness's exit code and reconciliation are the ordinary gate's.
/// Writes the Markdown report beside the samples and returns the checks.
pub fn gate_harness_run(
    report: &serde_json::Value,
    report_dir: &Path,
    spec: &Spec,
    title: &str,
) -> Vec<Check> {
    let soak = &report["soak"];
    let mut checks = Vec::new();
    if !soak.is_object() || soak.get("samples").is_none() {
        checks.push(fail(
            "soak driver",
            format!(
                "unknown: the report has no soak record ({})",
                soak["reason"].as_str().unwrap_or("not a --plan soak run")
            ),
            "a completed soak".into(),
        ));
        return checks;
    }
    // Read from beside the report, wherever the run's directory now is.
    let path = report_dir.join(SAMPLES_FILE);
    let samples = match read_samples(&path) {
        Ok(samples) => samples,
        Err(error) => {
            checks.push(fail(
                "soak samples",
                format!("unknown: {error:#}"),
                "readable".into(),
            ));
            return checks;
        }
    };
    let errors = soak["retention_errors"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    checks.push(verdict(
        "rollover and retention commands",
        if errors.is_empty() {
            format!(
                "{} retention step(s), {} rollover(s), no error",
                soak["retention_steps"],
                soak["rollovers"].as_array().map_or(0, Vec::len)
            )
        } else {
            format!(
                "{} error(s); first: {}",
                errors.len(),
                errors[0]["error"].as_str().unwrap_or("unknown")
            )
        },
        "no error".into(),
        errors.is_empty(),
    ));
    checks.extend(evaluate(&samples, &spec.gates));
    let text = markdown(title, &samples, &checks);
    let _ = std::fs::write(report_dir.join(REPORT_FILE), &text);
    checks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gates() -> Gates {
        Gates {
            warmup_minutes: 10.0,
            min_gated_samples: 5,
            rss_slope_mib_per_hour_max: 16.0,
            rss_trend_window_minutes: 20.0,
            min_trend_windows: 3,
            rss_warmup_peak_multiple_max: Some(2.0),
            fd_slope_per_hour_max: 2.0,
            pool_connections_max: 20,
            pool_connections_drift_max: 2.0,
            wal_bytes_max: 1 << 30,
            min_rollovers: 1,
            min_archive_cycles: 1,
            one_lifetime: true,
        }
    }

    /// One sample a minute for `minutes`, RSS growing by `leak_mib_per_hour`,
    /// the share sequence crossing one bound and one partition dropped.
    fn series(minutes: usize, leak_mib_per_hour: f64) -> Vec<Sample> {
        (0..minutes)
            .map(|minute| {
                let hours = minute as f64 / 60.0;
                let rss = (200.0 + leak_mib_per_hour * hours) * 1048576.0;
                let dropped = minute > minutes / 2;
                Sample {
                    schema: SAMPLE_SCHEMA.into(),
                    at: Utc::now(),
                    elapsed_seconds: minute as f64 * 60.0,
                    phase: Some("c01.x.steady_state".into()),
                    steady: true,
                    processes: vec![ProcessPoint {
                        instance: "load-fe-0".into(),
                        pid: Some(42),
                        rss_bytes: Some(rss as u64),
                        open_fds: Some(300 + (minute % 3) as u64),
                        threads: Some(8),
                        ..ProcessPoint::default()
                    }],
                    database: DatabasePoint {
                        connections: Some(
                            [("load-fe-0".to_owned(), 10 + (minute % 2) as u64)]
                                .into_iter()
                                .collect(),
                        ),
                        idle_in_transaction_over_60s: Some(0),
                        wal_bytes: Some(64 << 20),
                        next_share_seq: Some(1000 + 100 * minute as i64),
                        partitions: vec![
                            PartitionPoint {
                                name: "qbit_share_ledger_p0".into(),
                                state: if dropped { "dropped" } else { "attached" }.into(),
                                lower_seq: None,
                                upper_seq: 2000,
                                bytes: (!dropped).then_some(1 << 20),
                            },
                            PartitionPoint {
                                name: "qbit_share_ledger_p1".into(),
                                state: "attached".into(),
                                lower_seq: Some(2000),
                                upper_seq: 1 << 40,
                                bytes: Some(1 << 20),
                            },
                        ],
                        payout_divergences: Some(0),
                        ..DatabasePoint::default()
                    },
                    latency: None,
                    ledger: None,
                }
            })
            .collect()
    }

    fn failed(checks: &[Check]) -> Vec<String> {
        checks
            .iter()
            .filter(|check| check.pass == Some(false))
            .map(|check| format!("{}: {}", check.name, check.observed))
            .collect()
    }

    #[test]
    fn slope_is_least_squares_per_unit() {
        assert_eq!(slope(&[(0.0, 1.0), (1.0, 3.0), (2.0, 5.0)]), Some(2.0));
        assert_eq!(slope(&[(1.0, 1.0), (1.0, 2.0)]), None);
        assert_eq!(slope(&[(1.0, 1.0)]), None);
    }

    #[test]
    fn window_peaks_take_each_whole_window_and_drop_the_tail() {
        let points: Vec<(f64, f64)> = (0..10).map(|i| (i as f64, i as f64 % 3.0)).collect();
        assert_eq!(
            window_peaks(&points, 3.0),
            vec![(1.5, 2.0), (4.5, 2.0), (7.5, 2.0)]
        );
        assert!(window_peaks(&[], 3.0).is_empty());
    }

    #[test]
    fn landing_steps_pass_the_envelope_and_a_leak_under_them_does_not() {
        // A 700 MiB step for 12 of every 20 minutes, as a 400k window's
        // block landing makes, over a flat 500 MiB.
        let landing = |minute: usize| if minute % 20 < 12 { 700.0 } else { 0.0 };
        let mut samples = series(240, 0.0);
        for (minute, sample) in samples.iter_mut().enumerate() {
            sample.processes[0].rss_bytes = Some(((500.0 + landing(minute)) * 1048576.0) as u64);
        }
        assert!(failed(&evaluate(&samples, &gates())).is_empty());
        // The same landings over a 64 MiB/h leak.
        for (minute, sample) in samples.iter_mut().enumerate() {
            let leak = 64.0 * minute as f64 / 60.0;
            sample.processes[0].rss_bytes =
                Some(((500.0 + landing(minute) + leak) * 1048576.0) as u64);
        }
        let failures = failed(&evaluate(&samples, &gates()));
        assert!(
            failures
                .iter()
                .any(|line| line.starts_with("resident memory slope")),
            "{failures:?}"
        );
    }

    #[test]
    fn a_flat_soak_passes_every_gate() {
        let checks = evaluate(&series(120, 0.0), &gates());
        assert!(failed(&checks).is_empty(), "{:?}", failed(&checks));
    }

    #[test]
    fn a_leak_fails_the_resident_memory_gate_and_nothing_else() {
        let checks = evaluate(&series(120, 64.0), &gates());
        let failed = failed(&checks);
        assert_eq!(failed.len(), 1, "{failed:?}");
        assert!(failed[0].starts_with("resident memory slope"), "{failed:?}");
        assert!(failed[0].contains("+64.00 MiB/h"), "{failed:?}");
        // The documented 2x bound is the coarser of the two: a leak that
        // doubles the warm-up peak fails it too.
        let failed = super::tests::failed(&evaluate(&series(120, 400.0), &gates()));
        assert!(
            failed
                .iter()
                .any(|line| line.starts_with("resident memory against the warm-up peak")),
            "{failed:?}"
        );
    }

    #[test]
    fn the_warm_up_is_not_fitted() {
        let mut samples = series(120, 0.0);
        // Growth confined to the warm-up (the first 10 minutes) is allowed,
        // within the documented 2x of its peak.
        for sample in samples.iter_mut().take(10) {
            sample.processes[0].rss_bytes = Some(120 << 20);
        }
        assert!(failed(&evaluate(&samples, &gates())).is_empty());
    }

    #[test]
    fn unknown_readings_fail_rather_than_pass() {
        let mut samples = series(120, 0.0);
        for sample in &mut samples {
            sample.processes[0].rss_bytes = None;
            sample.database.wal_bytes = None;
            sample.database.idle_in_transaction_over_60s = None;
        }
        let failures = failed(&evaluate(&samples, &gates()));
        for name in ["resident memory", "WAL size", "idle in transaction"] {
            assert!(
                failures
                    .iter()
                    .any(|line| line.starts_with(name) && line.contains("unknown")),
                "{name}: {failures:?}"
            );
        }
        assert!(failed(&evaluate(&[], &gates()))[0].contains("no sample"));
    }

    #[test]
    fn drift_restarts_rollovers_and_retention_are_each_gated() {
        let mut samples = series(120, 0.0);
        let len = samples.len();
        for sample in samples.iter_mut().skip(len * 3 / 4) {
            if let Some(clients) = sample.database.connections.as_mut() {
                clients.insert("load-fe-0".into(), 19);
            }
        }
        samples[60].processes[0].pid = Some(43);
        for sample in &mut samples {
            sample.database.next_share_seq = Some(1000);
            sample.database.partitions[0].state = "attached".into();
        }
        samples[70].database.wal_bytes = Some(2 << 30);
        samples[80].database.idle_in_transaction_over_60s = Some(1);
        let failed = failed(&evaluate(&samples, &gates()));
        for name in [
            "database connection drift",
            "one server lifetime",
            "share partition rollovers",
            "share partitions archived and dropped",
            "WAL size",
            "idle in transaction",
        ] {
            assert!(
                failed.iter().any(|line| line.starts_with(name)),
                "{name}: {failed:?}"
            );
        }
    }

    #[test]
    fn a_deployment_ledger_must_hold_every_acknowledged_share() {
        let mut samples = series(120, 0.0);
        let point = |acked, committed| LedgerPoint {
            acknowledged_since_start: Some(acked),
            acknowledged_gaps: 0,
            committed_since_start: Some(committed),
            tolerance_rows: Some(3),
            tolerance_seconds: 120.0,
        };
        samples.last_mut().unwrap().ledger = Some(point(1000, 1000));
        assert!(failed(&evaluate(&samples, &gates())).is_empty());
        samples.last_mut().unwrap().ledger = Some(point(1000, 999));
        let failed = failed(&evaluate(&samples, &gates()));
        assert!(
            failed[0].starts_with("acknowledged shares in the ledger"),
            "{failed:?}"
        );
    }

    #[test]
    fn no_client_is_a_real_zero_and_an_unread_activity_view_is_unknown() {
        let mut samples = series(120, 0.0);
        for sample in &mut samples {
            sample.database.connections = Some(BTreeMap::new());
        }
        assert!(failed(&evaluate(&samples, &gates())).is_empty());
        samples[30].database.connections = None;
        let failures = failed(&evaluate(&samples, &gates()));
        assert!(
            failures[0].starts_with("database connections: unknown: 1 of 120"),
            "{failures:?}"
        );
    }

    #[test]
    fn a_new_payout_divergence_fails_the_soak() {
        let mut samples = series(120, 0.0);
        samples.last_mut().unwrap().database.payout_divergences = Some(1);
        let failed = failed(&evaluate(&samples, &gates()));
        assert!(failed[0].starts_with("payout divergences"), "{failed:?}");
    }

    #[test]
    fn samples_round_trip_through_the_jsonl_file() {
        let dir = std::env::temp_dir().join(format!("soak-samples-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(SAMPLES_FILE);
        let samples = series(3, 0.0);
        for sample in &samples {
            append_line(&path, sample).unwrap();
        }
        assert_eq!(read_samples(&path).unwrap(), samples);
        std::fs::write(&path, "{\"schema\":\"other\"}\n").unwrap();
        assert!(read_samples(&path).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_checked_in_testnet4_gates_parse_and_validate() {
        let gates: Gates =
            serde_json::from_str(include_str!("../soak-gates/testnet4.json")).unwrap();
        gates.validate().unwrap();
        assert!(
            !gates.one_lifetime,
            "a deployment may be restarted by its operator"
        );
    }
}
