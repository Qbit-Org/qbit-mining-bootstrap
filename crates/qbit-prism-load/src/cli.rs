//! Command-line surface.
//!
//! EP-VALIDATION: every numeric input is range-checked here, at the entry
//! boundary, against the semantics its consumer applies downstream.

use anyhow::{bail, ensure, Result};
use clap::Parser;
use std::path::PathBuf;

/// Named phase plans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Plan {
    /// Decision D1's shape: 500/s for 300 s, a 2,000/s burst, reconnect and
    /// slow-database phases.
    D1,
    /// Every artifact phase for 60 s at `--rate`.
    Short,
}

impl Plan {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "d1" => Ok(Self::D1),
            "short" => Ok(Self::Short),
            other => bail!("unknown plan {other:?}; use d1 or short"),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::D1 => "d1",
            Self::Short => "short",
        }
    }
}

#[derive(Parser, Clone, Debug)]
#[command(
    name = "qbit-prism-load",
    about = "Stratum-to-PostgreSQL load harness and capacity-evidence producer",
    version
)]
pub struct Args {
    /// `qbit-prism-server` executable. Defaults to the one beside this binary.
    #[arg(long)]
    pub server_bin: Option<PathBuf>,
    /// Run a server whose build profile is debug, or cannot be determined
    /// from its location, anyway. Neither is a capacity measurement.
    #[arg(long)]
    pub allow_debug_server: bool,
    /// Run with modified tracked files. Forces `artifact_kind: example`.
    #[arg(long)]
    pub allow_dirty_tree: bool,
    /// Emit `artifact_kind: example` even from a clean tree.
    #[arg(long)]
    pub example_artifact: bool,

    /// Directory holding `initdb`, `pg_ctl` and `pg_basebackup`. Defaults to
    /// `QBIT_PRISM_LOAD_PG_BIN_DIR`, then `pg_config --bindir`.
    #[arg(long)]
    pub pg_bin_dir: Option<PathBuf>,
    /// Use an existing database instead of managing clusters. No standby is
    /// created; the replication mode is detected.
    #[arg(long)]
    pub database_url: Option<String>,
    /// `async`, `sync` or `none`.
    #[arg(long, default_value = "async")]
    pub replication: String,

    /// Number of frontends (1, 2 or 4).
    #[arg(long, default_value_t = 1)]
    pub frontends: usize,
    /// Stratum sessions, spread round-robin across the frontends.
    #[arg(long, default_value_t = 100)]
    pub sessions: usize,
    /// Shares pre-seeded into the payout window.
    #[arg(long, default_value_t = 20_000)]
    pub window_shares: u64,
    /// Serialized size of one seeded share, in bytes.
    #[arg(long, default_value_t = crate::window::DEFAULT_SEED_SHARE_BYTES)]
    pub seed_share_bytes: usize,

    #[arg(long, default_value = "short")]
    pub plan: String,
    /// Offered share rate for the `short` plan, in shares per second.
    #[arg(long, default_value_t = 50.0)]
    pub rate: f64,
    /// Outstanding submits per session. The server answers one request per
    /// session at a time.
    #[arg(long, default_value_t = 1)]
    pub max_outstanding_per_session: usize,

    /// Warm-up seconds before the first artifact phase. Not in the artifact.
    #[arg(long, default_value_t = 30)]
    pub warmup_seconds: u64,
    #[arg(long)]
    pub steady_state_seconds: Option<u64>,
    #[arg(long)]
    pub steady_state_rate: Option<f64>,
    #[arg(long)]
    pub burst_seconds: Option<u64>,
    #[arg(long)]
    pub burst_rate: Option<f64>,
    #[arg(long)]
    pub reconnect_seconds: Option<u64>,
    #[arg(long)]
    pub slow_database_seconds: Option<u64>,
    /// Completed reconnects to drive in the reconnect phase.
    #[arg(long, default_value_t = 12)]
    pub reconnect_target: u64,
    /// One-way per-chunk proxy delay during `slow_database`, in milliseconds.
    #[arg(long, default_value_t = 10)]
    pub slow_db_delay_ms: u64,

    /// SIGKILL a frontend while submits are outstanding, in a side phase.
    #[arg(long)]
    pub mid_flight_kill: bool,
    /// Scheduled own-block submissions during the run.
    #[arg(long, default_value_t = 0)]
    pub scheduled_blocks: usize,
    /// External tips minted during warm-up, for time to usable work.
    #[arg(long, default_value_t = 3)]
    pub external_tips: usize,

    /// `none`, or `dense` for the #271 dense-cadence side phase: own blocks
    /// about 9 s apart and in 18-20 s pairs, measuring what each
    /// payout-revision bump costs every frontend.
    #[arg(long, default_value = "none")]
    pub cadence: String,
    /// Length of the `dense_cadence` phase, in seconds.
    #[arg(long, default_value_t = 240)]
    pub cadence_seconds: u64,
    /// Offered share rate during `dense_cadence`. Defaults to the
    /// steady-state rate.
    #[arg(long)]
    pub cadence_rate: Option<f64>,
    /// Seconds between own-block landings, repeated cyclically.
    #[arg(long, default_value = crate::cadence::DEFAULT_GAPS)]
    pub cadence_gaps: String,

    /// Seconds to wait for the first frontend to serve work.
    #[arg(long, default_value_t = 120)]
    pub work_timeout: u64,
    #[arg(long, default_value_t = 2000.0)]
    pub forecast_peak_shares_per_second: f64,
    #[arg(long, default_value_t = 1000.0)]
    pub ack_p99_limit_ms: f64,

    /// `PRISM_DATABASE_MAX_CONNECTIONS` for every frontend.
    #[arg(long, default_value_t = 16)]
    pub db_max_connections: u32,
    /// `PRISM_RUNTIME_WORKERS` for every frontend.
    #[arg(long, default_value_t = 2)]
    pub runtime_workers: usize,
    /// `PRISM_BLOCKPOLL_SECONDS` for every frontend.
    #[arg(long, default_value_t = 2.0)]
    pub blockpoll_seconds: f64,
    /// `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` for every frontend.
    #[arg(long, default_value_t = 15.0)]
    pub share_commit_timeout_seconds: f64,

    /// ORDER_LOCK sampling interval, in milliseconds (1..1000).
    #[arg(long, default_value_t = 10)]
    pub lock_sample_interval_ms: u64,
    /// Per-frontend CPU and RSS sampling interval, in milliseconds (50..60000).
    #[arg(long, default_value_t = 1000)]
    pub process_sample_interval_ms: u64,
    /// Stop the run if `MemAvailable` falls below this many mebibytes.
    #[arg(long, default_value_t = 4096)]
    pub min_mem_available_mib: u64,

    /// Output directory.
    #[arg(long, default_value = "load-out")]
    pub out: PathBuf,
    /// Keep cluster data directories and logs after the run.
    #[arg(long)]
    pub keep_artifacts: bool,
}

impl Args {
    pub fn plan(&self) -> Result<Plan> {
        Plan::parse(&self.plan)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.frontends, 1 | 2 | 4),
            "--frontends must be 1, 2 or 4"
        );
        ensure!(self.sessions > 0, "--sessions must be positive");
        ensure!(
            self.sessions >= self.frontends,
            "--sessions must be at least --frontends so every frontend is driven"
        );
        ensure!(self.window_shares > 0, "--window-shares must be positive");
        ensure!(
            self.seed_share_bytes >= 200,
            "--seed-share-bytes must leave room for the share payload"
        );
        ensure!(
            self.rate.is_finite() && self.rate > 0.0,
            "--rate must be finite and positive"
        );
        for (name, value) in [
            ("--steady-state-rate", self.steady_state_rate),
            ("--burst-rate", self.burst_rate),
            ("--cadence-rate", self.cadence_rate),
        ] {
            if let Some(value) = value {
                ensure!(
                    value.is_finite() && value > 0.0,
                    "{name} must be finite and positive"
                );
            }
        }
        ensure!(
            self.max_outstanding_per_session > 0,
            "--max-outstanding-per-session must be positive"
        );
        ensure!(
            self.forecast_peak_shares_per_second.is_finite()
                && self.forecast_peak_shares_per_second > 0.0,
            "--forecast-peak-shares-per-second must be finite and positive"
        );
        ensure!(
            self.ack_p99_limit_ms.is_finite() && self.ack_p99_limit_ms > 0.0,
            "--ack-p99-limit-ms must be finite and positive"
        );
        ensure!(
            self.share_commit_timeout_seconds.is_finite()
                && self.share_commit_timeout_seconds > 0.0,
            "--share-commit-timeout-seconds must be finite and positive"
        );
        // The consumer refuses an ACK limit above the commit timeout
        // (`capacity.rs`), so the harness refuses it here rather than writing
        // an artifact that cannot validate.
        ensure!(
            self.ack_p99_limit_ms <= self.share_commit_timeout_seconds * 1000.0,
            "--ack-p99-limit-ms ({}) cannot exceed --share-commit-timeout-seconds x 1000 ({})",
            self.ack_p99_limit_ms,
            self.share_commit_timeout_seconds * 1000.0
        );
        ensure!(
            self.slow_db_delay_ms >= 10,
            "--slow-db-delay-ms must be at least 10; the artifact phase requires it"
        );
        ensure!(
            self.reconnect_target >= 10,
            "--reconnect-target must be at least 10; the artifact phase requires it"
        );
        ensure!(
            (4..=1024).contains(&self.db_max_connections),
            "--db-max-connections must be 4..1024, as PRISM_DATABASE_MAX_CONNECTIONS is"
        );
        ensure!(
            (1..=1024).contains(&self.runtime_workers),
            "--runtime-workers must be 1..1024"
        );
        ensure!(
            self.blockpoll_seconds.is_finite() && self.blockpoll_seconds > 0.0,
            "--blockpoll-seconds must be finite and positive"
        );
        // A sampling interval has to be bounded above as well as below: an
        // interval longer than a phase measures nothing, and one longer than
        // the phase's own length would stretch the run.
        ensure!(
            (1..=1000).contains(&self.lock_sample_interval_ms),
            "--lock-sample-interval-ms must be 1..1000"
        );
        ensure!(
            (50..=60_000).contains(&self.process_sample_interval_ms),
            "--process-sample-interval-ms must be 50..60000"
        );
        ensure!(self.work_timeout > 0, "--work-timeout must be positive");
        for (name, value) in [
            ("--steady-state-seconds", self.steady_state_seconds),
            ("--reconnect-seconds", self.reconnect_seconds),
            ("--slow-database-seconds", self.slow_database_seconds),
        ] {
            if let Some(value) = value {
                ensure!(
                    value >= 60,
                    "{name} must be at least 60; the artifact refuses a shorter phase"
                );
            }
        }
        self.plan()?;
        crate::cluster::Replication::parse(&self.replication)?;
        // The gap pattern and the phase length are checked against each other
        // here, at the entry boundary, because a pattern that cannot hold ten
        // landings measures nothing and the run must say so before it starts
        // (EP-VALIDATION).
        if self.cadence()?.is_dense() {
            crate::cadence::validate(&self.cadence_gaps, self.cadence_seconds)?;
        }
        Ok(())
    }

    pub fn cadence(&self) -> Result<crate::cadence::Cadence> {
        crate::cadence::Cadence::parse(&self.cadence)
    }

    /// The gap pattern, for a run that asked for one. Empty otherwise.
    pub fn cadence_gaps(&self) -> Result<Vec<f64>> {
        if self.cadence()?.is_dense() {
            crate::cadence::parse_gaps(&self.cadence_gaps)
        } else {
            Ok(Vec::new())
        }
    }
}

/// One phase of the run.
#[derive(Clone, Debug)]
pub struct PhasePlan {
    pub name: String,
    pub seconds: u64,
    pub rate: f64,
    /// Only the three required phases go into the artifact.
    pub in_artifact: bool,
    /// Drive reconnects during this phase.
    pub reconnects: bool,
    /// One-way proxy delay, in milliseconds.
    pub database_delay_ms: u64,
    /// SIGKILL a frontend with submits outstanding.
    pub mid_flight_kill: bool,
    /// Drive own-block landings on the `--cadence-gaps` pattern and measure
    /// the payout-revision bumps they cause.
    pub dense_cadence: bool,
}

pub fn phases(args: &Args) -> Result<Vec<PhasePlan>> {
    let plan = args.plan()?;
    let (steady_seconds, steady_rate, burst, reconnect_seconds, slow_seconds) = match plan {
        Plan::D1 => (
            args.steady_state_seconds.unwrap_or(300),
            args.steady_state_rate.unwrap_or(500.0),
            Some((
                args.burst_seconds.unwrap_or(60),
                args.burst_rate.unwrap_or(2000.0),
            )),
            args.reconnect_seconds.unwrap_or(60),
            args.slow_database_seconds.unwrap_or(60),
        ),
        Plan::Short => (
            args.steady_state_seconds.unwrap_or(60),
            args.steady_state_rate.unwrap_or(args.rate),
            args.burst_seconds
                .map(|seconds| (seconds, args.burst_rate.unwrap_or(args.rate))),
            args.reconnect_seconds.unwrap_or(60),
            args.slow_database_seconds.unwrap_or(60),
        ),
    };
    let mut plans = Vec::new();
    if args.warmup_seconds > 0 {
        plans.push(PhasePlan {
            name: "warm_up".into(),
            seconds: args.warmup_seconds,
            rate: steady_rate,
            in_artifact: false,
            reconnects: false,
            database_delay_ms: 0,
            mid_flight_kill: false,
            dense_cadence: false,
        });
    }
    plans.push(PhasePlan {
        name: "steady_state".into(),
        seconds: steady_seconds,
        rate: steady_rate,
        in_artifact: true,
        reconnects: false,
        database_delay_ms: 0,
        mid_flight_kill: false,
        dense_cadence: false,
    });
    if let Some((seconds, rate)) = burst {
        plans.push(PhasePlan {
            name: "burst".into(),
            seconds,
            rate,
            in_artifact: false,
            reconnects: false,
            database_delay_ms: 0,
            mid_flight_kill: false,
            dense_cadence: false,
        });
    }
    plans.push(PhasePlan {
        name: "reconnect".into(),
        seconds: reconnect_seconds,
        rate: steady_rate,
        in_artifact: true,
        reconnects: true,
        database_delay_ms: 0,
        mid_flight_kill: false,
        dense_cadence: false,
    });
    plans.push(PhasePlan {
        name: "slow_database".into(),
        seconds: slow_seconds,
        rate: steady_rate,
        in_artifact: true,
        reconnects: false,
        database_delay_ms: args.slow_db_delay_ms,
        mid_flight_kill: false,
        dense_cadence: false,
    });
    // The dense-cadence phase is a side phase, after `slow_database` and with
    // no proxy delay: the measurement is the frontends' rebuild latency, which
    // a delayed database would drown out.
    if args.cadence()?.is_dense() {
        plans.push(PhasePlan {
            name: crate::cadence::PHASE.into(),
            seconds: args.cadence_seconds,
            rate: args.cadence_rate.unwrap_or(steady_rate),
            in_artifact: false,
            reconnects: false,
            database_delay_ms: 0,
            mid_flight_kill: false,
            dense_cadence: true,
        });
    }
    if args.mid_flight_kill {
        plans.push(PhasePlan {
            name: "mid_flight_kill".into(),
            seconds: 60,
            rate: steady_rate,
            in_artifact: false,
            reconnects: false,
            // The kill has to land while submits really are outstanding. With
            // no delay an acknowledgement takes a few milliseconds, so at any
            // ordinary rate almost nothing is in flight and the scenario
            // silently does not happen. The proxy delay holds submits open
            // long enough for the kill to mean something.
            database_delay_ms: args.slow_db_delay_ms,
            mid_flight_kill: true,
            dense_cadence: false,
        });
    }
    Ok(plans)
}
