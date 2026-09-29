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

mod evaluate;
mod sample;
#[cfg(test)]
mod tests;

pub use evaluate::{
    archive_cycles, evaluate, gate_harness_run, markdown, rollovers, slope, window_peaks,
};
pub use sample::{
    append_line, database_point, process_point, read_samples, DatabasePoint, LatencyPoint,
    LedgerPoint, PartitionPoint, ProcessPoint, Sample, CONNECTION_KEY,
};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
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

/// The soak block of a preset file, or `None` for a preset without one.
pub fn from_preset_value(value: Option<&serde_json::Value>) -> Result<Option<Spec>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let spec: Spec = serde_json::from_value(value.clone()).context("parsing the soak block")?;
    spec.validate()?;
    Ok(Some(spec))
}
