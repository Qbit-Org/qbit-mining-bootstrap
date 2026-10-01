//! Command-line surface.
//!
//! EP-VALIDATION: every numeric input is range-checked here, at the entry
//! boundary, against the semantics its consumer applies downstream.

use anyhow::{bail, ensure, Context, Result};
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
    /// Warm-up only: the external tips at `--background-shares-per-second`
    /// (or `--rate`) and no artifact phase, so no artifact. The per-PR smoke
    /// run's plan (#521): time to new-tip work and reconciliation in about a
    /// minute.
    Tips,
    /// The long soak (#575): the looped presets' phases from a soak
    /// preset's `soak` block, over one server lifetime. See
    /// [`crate::soak_driver`].
    Soak,
}

impl Plan {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "d1" => Ok(Self::D1),
            "short" => Ok(Self::Short),
            "tips" => Ok(Self::Tips),
            "soak" => Ok(Self::Soak),
            other => bail!("unknown plan {other:?}; use d1, short, tips or soak"),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::D1 => "d1",
            Self::Short => "short",
            Self::Tips => "tips",
            Self::Soak => "soak",
        }
    }
}

/// The native server's default for `PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS`
/// (`crates/qbit-prism-server/src/stratum.rs`): the admission a production
/// frontend runs unless its deployment says otherwise, and so the admission a
/// measurement runs unless the run says otherwise.
pub const PRODUCTION_MAX_PENDING_INITIAL_JOBS: usize = 128;

/// Where a run's initial-job admission came from, recorded so a reader of the
/// side report can tell a deliberate override from the default without
/// consulting the command line the run was started with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionSource {
    /// `--stratum-max-pending-initial-jobs` was omitted: the production
    /// default.
    Default,
    /// `--stratum-max-pending-initial-jobs` was given.
    Flag,
}

impl AdmissionSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Flag => "--stratum-max-pending-initial-jobs",
        }
    }
}

/// The Stratum listener limits derived from the run's shape, identical for
/// every frontend. See [`Args::stratum_limits`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StratumLimits {
    /// `--sessions` spread over `--frontends`, rounded up.
    pub sessions_per_frontend: usize,
    /// `PRISM_STRATUM_MAX_CONNECTIONS`: room for every session on the
    /// frontend to be reconnecting while its old socket is still closing,
    /// and never below the server's own default of 384.
    pub max_connections: usize,
    /// `PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS`.
    pub max_pending_initial_jobs: usize,
    pub admission_source: AdmissionSource,
}

impl StratumLimits {
    /// The admission every run of this shape used before
    /// `--stratum-max-pending-initial-jobs` existed: enough permits for
    /// every session on the frontend to build its first job at once, and
    /// never below the production default. Recorded beside the launched
    /// value so older evidence can be reproduced and compared honestly.
    pub fn pre_flag_max_pending_initial_jobs(&self) -> usize {
        (self.sessions_per_frontend + 16).max(PRODUCTION_MAX_PENDING_INITIAL_JOBS)
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
    /// from its location, anyway. Neither is a capacity measurement, so
    /// this forces `artifact_kind: example`.
    #[arg(long)]
    pub allow_debug_server: bool,
    /// Run with modified tracked files. Forces `artifact_kind: example`.
    #[arg(long)]
    pub allow_dirty_tree: bool,
    /// Run a server binary that cannot be tied to this checkout's HEAD:
    /// no Cargo dep-info beside it, or a source newer than it. Forces
    /// `artifact_kind: example`, because the artifact would otherwise name
    /// a revision that did not produce the measurements.
    #[arg(long)]
    pub allow_unverified_server_revision: bool,
    /// Emit `artifact_kind: example` even from a clean tree.
    #[arg(long)]
    pub example_artifact: bool,

    /// Directory holding `initdb`, `pg_ctl` and `pg_basebackup`. Defaults to
    /// `QBIT_PRISM_LOAD_PG_BIN_DIR`, then `pg_config --bindir`.
    #[arg(long)]
    pub pg_bin_dir: Option<PathBuf>,
    /// Use an existing database instead of managing clusters. No standby is
    /// created; the replication mode is detected and must be the one
    /// `--replication` declares, or the run exits 8.
    #[arg(long)]
    pub database_url: Option<String>,
    /// `async`, `sync` or `none`: the replication mode the run declares. A
    /// managed cluster is built to it; an external database is checked
    /// against it, at entry and after the load.
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
    /// Scheduled own-block submissions during the run: in `steady_state`, or
    /// in warm-up under `--plan tips`, whose only phase it is.
    #[arg(long, default_value_t = 0)]
    pub scheduled_blocks: usize,
    /// External tips minted during warm-up, for time to usable work.
    #[arg(long, default_value_t = 3)]
    pub external_tips: usize,
    /// Serve different template bits on every tip, as a per-block retarget
    /// does: a deterministic walk of about 0.8 percent per block around the
    /// stock bits, so no two consecutive tips share a network difficulty.
    /// Off by default; every existing comparison is against constant bits.
    #[arg(long)]
    pub retarget_bits: bool,
    /// Offered shares per second during warm-up, the phase the external tips
    /// are minted in, so the tips' refreshes run over a ledger that is still
    /// receiving shares. Defaults to the steady-state rate, as before.
    #[arg(long)]
    pub background_shares_per_second: Option<f64>,

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
    /// `PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS` for every frontend: how many
    /// authorized sessions may build their first job at once; the rest wait
    /// in the server's queue. Defaults to the server's own default, 128, so
    /// the measurement runs the admission production runs. Positive, and at
    /// most the connection cap the harness derives from `--sessions` and
    /// `--frontends`. Evidence taken before this flag existed ran
    /// `sessions_per_frontend + 16` (128 at least); pass that value to
    /// reproduce it, and see `frontend_environment[].stratum_admission` in
    /// its side report for what a run actually used.
    #[arg(long)]
    pub stratum_max_pending_initial_jobs: Option<usize>,
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

    /// Seed for every random draw the realism options make: recipient
    /// weights, window order, session hashrates, offer placement and arrival
    /// bursts. The same seed and flags generate the same population.
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Distinct payout addresses across the live sessions and the seeded
    /// window. Omitted, every live session uses one address and the window
    /// keeps its five round-robin recipients, as before.
    #[arg(long)]
    pub recipients: Option<usize>,
    /// How work is spread over the `--recipients` addresses: `uniform`,
    /// `zipf:<s>`, `pareto:<alpha>` or `whale:<fraction>+<one of those>`.
    /// Skews both how many sessions each address has and how many window
    /// shares it holds.
    #[arg(long, default_value = "uniform")]
    pub recipient_weights: String,
    /// Lognormal spread of hashrate between sessions (sigma of the log);
    /// 0 gives every session of an address the same hashrate.
    #[arg(long, default_value_t = 0.0)]
    pub session_hashrate_sigma: f64,
    /// `fixed`, or `vardiff:<max ratio>`: each session's share difficulty in
    /// proportion to its hashrate, as a converged vardiff sets it.
    #[arg(long, default_value = "fixed")]
    pub session_difficulty: String,
    /// `smooth`, or `bursty:cv1=<x>,cv60=<y>,max=<m>`: the offered rate varies
    /// per second and per minute around each phase's rate.
    #[arg(long, default_value = "smooth")]
    pub arrival: String,

    /// Compact network bits the fake node serves (before any retarget walk).
    /// The default is every earlier run's. A harder target (a smaller
    /// exponent or mantissa) raises the window's weight and with it every
    /// share's difficulty, which a wide `--session-difficulty` spread over a
    /// production-sized window needs: no share difficulty can go below
    /// 2^-32, whose target is already 2^256. At most 1,024 times the default
    /// difficulty, as each scheduled block costs that many more hashes.
    #[arg(long, default_value = crate::window::TEMPLATE_BITS)]
    pub template_bits: String,

    /// The node behind the frontends: `fake`, the in-process node every
    /// earlier run used, or `qbitd`, a managed real regtest pool node and
    /// peer whose chain is ramped to the fake node's difficulty first
    /// (#547). See `qbitd.rs`.
    #[arg(long, default_value = "fake")]
    pub node: String,
    /// The `qbitd` executable for `--node qbitd`. Defaults to `QBITD_BIN`,
    /// the variable the live fixtures read.
    #[arg(long)]
    pub qbitd_bin: Option<PathBuf>,

    /// Pool fee in basis points, as mainnet runs one: every frontend is
    /// launched with `PRISM_POOL_FEE_ENABLED=1`, this `PRISM_POOL_FEE_BPS` and
    /// a fee address of its own, and dust below the payout floor is swept to
    /// the fee as production sweeps it. 0 (the default) still enables the
    /// fee, which every server now requires (#535): it pays nothing and adds
    /// no output until dust must be swept, where a fee-off run would have
    /// stalled, so runs measured fee-off before #535 reproduce at 0.
    #[arg(long, default_value_t = 0)]
    pub pool_fee_bps: u16,

    /// Settle payouts through CTV fanout, as mainnet does (#548): every
    /// frontend is launched with `PRISM_CTV_SETTLEMENT_ENABLED=1` and the
    /// fanout fee policy `tests/fixtures/mainnet-compose.env` pins, with the
    /// broadcaster left off, so a block with more payable recipients than
    /// the direct-output cap builds fanout chunks. Off (the default) every
    /// frontend settles directly in the coinbase, as every earlier run did.
    /// The side report's `settlement` block counts each landed block's
    /// direct, fanout and carried recipients either way.
    #[arg(long)]
    pub ctv_settlement: bool,

    /// Length of the `churn` side phase, in seconds; 0 (the default) runs
    /// none. The phase follows `slow_database` (or warm-up, under `--plan
    /// tips`) with no proxy delay, and drives the rental bursts, lifetimes
    /// and reconnect storms below. See `churn.rs`.
    #[arg(long, default_value_t = 0)]
    pub churn_seconds: u64,
    /// Offered share rate during `churn`. Defaults to the steady-state rate.
    #[arg(long)]
    pub churn_rate: Option<f64>,
    /// External tips minted evenly through `churn`.
    #[arg(long, default_value_t = 0)]
    pub churn_tips: usize,
    /// Rental burst sizes, in order: `none`, a list (`100,500,2000`), or
    /// `pareto:xm=<x>,alpha=<a>,max=<m>;count=<n>` (or `lognormal:...`)
    /// seeded draws.
    #[arg(long, default_value = "none")]
    pub rental_bursts: String,
    /// Seconds over which a burst's sessions connect.
    #[arg(long, default_value_t = 10.0)]
    pub rental_burst_window_seconds: f64,
    /// Seconds between the starts of consecutive bursts; the first starts
    /// 5 s into the phase.
    #[arg(long, default_value_t = 60.0)]
    pub rental_burst_interval_seconds: f64,
    /// How long a rental session stays before its socket closes abruptly:
    /// `pareto:xm=<s>,alpha=<a>,max=<s>` or `lognormal:median=<s>,sigma=<x>,max=<s>`.
    #[arg(long, default_value = "pareto:xm=30,alpha=1.5,max=3600")]
    pub rental_lifetime: String,
    /// A rental session's hashrate over a mean base session's.
    #[arg(long, default_value_t = 20.0)]
    pub rental_hashrate: f64,
    /// Reconnect storms, in order: `none` or fractions of the connected
    /// sessions dropped abruptly (`0.1,0.25,0.5`).
    #[arg(long, default_value = "none")]
    pub reconnect_storms: String,
    /// Seconds between consecutive storms.
    #[arg(long, default_value_t = 60.0)]
    pub storm_interval_seconds: f64,
    /// A stormed session reconnects after a uniform delay up to this.
    #[arg(long, default_value_t = 5.0)]
    pub storm_reconnect_seconds: f64,

    /// Inject faults under load in a `faults` side phase after every other
    /// phase (#554): `<fault>[,<fault>...][;<key>=<value>...]`, e.g.
    /// `sigterm-drain,settlement-lock;hold=10;recovery=8`. Omitted, the run
    /// has no fault phase and its frontends talk to the node directly. See
    /// `fault/plan.rs` for the faults and keys.
    #[arg(long)]
    pub faults: Option<String>,

    /// A checked-in preset (`crates/qbit-prism-load/presets/*.json`) whose
    /// flags are added to the command line; any flag it sets cannot be given
    /// again. The side report records its name and SHA-256.
    #[arg(long)]
    pub preset: Option<PathBuf>,
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

    /// The `--faults` plan, parsed; `None` when the run injects none.
    pub fn fault_plan(&self) -> Result<Option<crate::fault::plan::FaultPlan>> {
        self.faults
            .as_deref()
            .map(crate::fault::plan::FaultPlan::parse)
            .transpose()
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(plan) = self.fault_plan()? {
            plan.check_against(self.frontends, self.database_url.is_none())?;
            plan.check_replication(&self.replication)?;
            // A soak's phases come from its looped presets, never this
            // flag's, so its fault verdict would silently be missing.
            ensure!(
                self.plan()? != Plan::Soak,
                "--faults does not run under --plan soak yet (#556)"
            );
        }
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
            (
                "--background-shares-per-second",
                self.background_shares_per_second,
            ),
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
        // The server refuses to start with more initial-job permits than
        // connections, so the harness refuses the same pair here, against the
        // connection cap it will in fact set (EP-VALIDATION).
        let limits = self.stratum_limits();
        ensure!(
            limits.max_pending_initial_jobs >= 1,
            "--stratum-max-pending-initial-jobs must be positive"
        );
        ensure!(
            limits.max_pending_initial_jobs <= limits.max_connections,
            "--stratum-max-pending-initial-jobs ({}) cannot exceed the frontends' connection cap \
             ({}, PRISM_STRATUM_MAX_CONNECTIONS for {} sessions per frontend), as the server \
             would refuse to start",
            limits.max_pending_initial_jobs,
            limits.max_connections,
            limits.sessions_per_frontend
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
        if self.plan()? == Plan::Tips {
            ensure!(
                self.warmup_seconds > 0 && self.external_tips > 0,
                "--plan tips mints its tips in warm-up, so it needs --warmup-seconds and \
                 --external-tips above 0"
            );
            ensure!(
                !self.mid_flight_kill && !self.cadence()?.is_dense(),
                "--plan tips runs warm-up only, so it cannot hold --mid-flight-kill or \
                 --cadence dense"
            );
        }
        crate::cluster::Replication::parse(&self.replication)?;
        self.template_bits()?;
        ensure!(
            self.pool_fee_bps <= 10_000,
            "--pool-fee-bps must be 0..10000, as PRISM_POOL_FEE_BPS is"
        );
        self.population_spec()?;
        self.arrival()?;
        self.churn_spec()?;
        // The gap pattern and the phase length are checked against each other
        // here, at the entry boundary, because a pattern that cannot hold ten
        // landings measures nothing and the run must say so before it starts
        // (EP-VALIDATION).
        if self.cadence()?.is_dense() {
            crate::cadence::validate(&self.cadence_gaps, self.cadence_seconds)?;
        }
        match self.node_mode()? {
            NodeMode::Fake => ensure!(
                self.qbitd_bin.is_none(),
                "--qbitd-bin names the node for --node qbitd; this run uses the fake node"
            ),
            NodeMode::Qbitd => self.validate_real_node()?,
        }
        Ok(())
    }

    pub fn node_mode(&self) -> Result<NodeMode> {
        NodeMode::parse(&self.node)
    }

    /// `--qbitd-bin`, else `QBITD_BIN`.
    pub fn qbitd_bin(&self) -> Result<PathBuf> {
        self.qbitd_bin
            .clone()
            .or_else(|| {
                std::env::var_os(qbit_prism_test_gate::Input::QbitdBin.name()).map(PathBuf::from)
            })
            .filter(|path| !path.as_os_str().is_empty())
            .context("--node qbitd needs --qbitd-bin or QBITD_BIN")
    }

    /// What `--node qbitd` can run, refused at entry (EP-VALIDATION): the
    /// ramp produces one difficulty, so the bits are the default's; the
    /// per-height walk is the fake node's; recipients need wallet-issued
    /// addresses (#553); every phase's rate must imply an own-block cadence
    /// inside the band; and the run must fit in the ramped epoch. Where the
    /// node's executable is, like `--server-bin`, is checked when the run
    /// starts, not here.
    fn validate_real_node(&self) -> Result<()> {
        ensure!(
            self.template_bits
                .trim()
                .eq_ignore_ascii_case(crate::window::TEMPLATE_BITS),
            "--node qbitd ramps the chain to the default difficulty ({}), so --template-bits \
             must be the default",
            crate::window::TEMPLATE_BITS
        );
        ensure!(
            !self.retarget_bits,
            "--retarget-bits walks the fake node's bits; a real node's bits are its chain's, so \
             --node qbitd refuses it"
        );
        ensure!(
            self.recipients.is_none(),
            "--node qbitd does not take --recipients yet: the addresses have to be issued by the \
             node and the share filter has to cover them (#553)"
        );
        // A soak's looped segments are parsed from the preset files, which
        // pin the fake node; its phases are not known here either (#575).
        ensure!(
            self.plan()? != Plan::Soak,
            "--node qbitd does not run --plan soak: a soak loops fake-node presets over one \
             server lifetime"
        );
        let plans = phases(self)?;
        crate::qbitd::check_cadence_band(
            self.window_shares,
            &plans
                .iter()
                .map(|plan| (plan.name.clone(), plan.rate))
                .collect::<Vec<_>>(),
        )?;
        let phase_seconds: u64 = plans.iter().map(|plan| plan.seconds).sum();
        crate::qbitd::check_headroom(crate::qbitd::planned_block_ceiling(
            (self.external_tips + self.churn_tips) as u64,
            self.scheduled_blocks as u64,
            phase_seconds + REAL_NODE_SETUP_ALLOWANCE_SECONDS,
        ))
    }

    /// The Stratum listener limits every frontend is launched with. This is
    /// the only place they are derived, so the value validated at entry, the
    /// one exported to the child and the one the side report records are the
    /// same value (EP-CONFIG).
    pub fn stratum_limits(&self) -> StratumLimits {
        // `validate` refuses zero frontends; the guard only keeps this
        // derivation total for a caller that asks before validating.
        // Sized for every session that can be connected at once, which is
        // `--sessions` unless a churn phase adds rentals.
        let sessions_per_frontend = self.peak_sessions().div_ceil(self.frontends.max(1));
        let (max_pending_initial_jobs, admission_source) =
            match self.stratum_max_pending_initial_jobs {
                Some(value) => (value, AdmissionSource::Flag),
                None => (
                    PRODUCTION_MAX_PENDING_INITIAL_JOBS,
                    AdmissionSource::Default,
                ),
            };
        StratumLimits {
            sessions_per_frontend,
            max_connections: (sessions_per_frontend * 2 + 64).max(384),
            max_pending_initial_jobs,
            admission_source,
        }
    }

    /// Rows the run seeds. Constant bits seed exactly the window, as every
    /// run before `--retarget-bits` did. A retargeting node also seeds a
    /// tenth more, older history below the window, so a tip whose target is
    /// harder than the last has older rows to reach into, as it always has
    /// in production; without them the harder window runs out of history
    /// and the refresh path measures a partial window that production never
    /// builds.
    pub fn seed_share_count(&self) -> u64 {
        if self.retarget_bits {
            self.window_shares + self.window_shares.div_ceil(10)
        } else {
            self.window_shares
        }
    }

    /// `--template-bits`, parsed and range-checked: 8 hex digits, no easier
    /// than the default and at most 1,024 times harder (EP-VALIDATION).
    pub fn template_bits(&self) -> Result<u32> {
        let text = self.template_bits.trim();
        ensure!(
            text.len() == 8 && text.bytes().all(|b| b.is_ascii_hexdigit()),
            "--template-bits must be 8 hex digits, not {text:?}"
        );
        let bits = qbit_prism_server::codec::parse_u32_hex(text)?;
        let difficulty = crate::window::scaled_network_difficulty(bits)?;
        let default = crate::window::scaled_network_difficulty(
            qbit_prism_server::codec::parse_u32_hex(crate::window::TEMPLATE_BITS)?,
        )?;
        ensure!(
            difficulty >= default && difficulty <= default.saturating_mul(1024),
            "--template-bits {text} must be at least the default {} and at most 1,024 times \
             harder",
            crate::window::TEMPLATE_BITS
        );
        Ok(bits)
    }

    /// The population the realism flags ask for, parsed and range-checked.
    pub fn population_spec(&self) -> Result<crate::realism::PopulationSpec> {
        let weights = crate::realism::WeightDist::parse(&self.recipient_weights)?;
        if self.recipients.is_none() {
            ensure!(
                weights.is_uniform(),
                "--recipient-weights skews work over --recipients addresses, so it needs \
                 --recipients"
            );
        }
        if let Some(count) = self.recipients {
            ensure!(
                (1..=crate::realism::MAX_RECIPIENTS).contains(&count),
                "--recipients must be 1..{}",
                crate::realism::MAX_RECIPIENTS
            );
        }
        ensure!(
            self.session_hashrate_sigma.is_finite()
                && (0.0..=5.0).contains(&self.session_hashrate_sigma),
            "--session-hashrate-sigma must be finite and 0..5"
        );
        Ok(crate::realism::PopulationSpec {
            recipients: self.recipients,
            weights,
            difficulty: crate::realism::SessionDifficulty::parse(&self.session_difficulty)?,
            hashrate_sigma: self.session_hashrate_sigma,
            sessions: self.sessions,
            seed: self.seed,
        })
    }

    /// The churn flags, parsed and range-checked (EP-VALIDATION).
    pub fn churn_spec(&self) -> Result<crate::churn::ChurnSpec> {
        let bursts = crate::churn::BurstSizes::parse(&self.rental_bursts)?;
        let storms = crate::churn::parse_storms(&self.reconnect_storms)?;
        let on = self.churn_seconds > 0;
        if on {
            ensure!(
                (30..=7200).contains(&self.churn_seconds),
                "--churn-seconds must be 0 (off) or 30..7200"
            );
        } else {
            ensure!(
                bursts == crate::churn::BurstSizes::None
                    && storms.is_empty()
                    && self.churn_tips == 0,
                "--rental-bursts, --reconnect-storms and --churn-tips drive the churn phase, so \
                 they need --churn-seconds"
            );
        }
        if let Some(rate) = self.churn_rate {
            ensure!(
                rate.is_finite() && rate > 0.0,
                "--churn-rate must be finite and positive"
            );
        }
        for (name, value, low, high) in [
            (
                "--rental-burst-window-seconds",
                self.rental_burst_window_seconds,
                0.0,
                600.0,
            ),
            (
                "--rental-burst-interval-seconds",
                self.rental_burst_interval_seconds,
                1.0,
                7200.0,
            ),
            (
                "--rental-hashrate",
                self.rental_hashrate,
                0.001,
                1_000_000.0,
            ),
            (
                "--storm-interval-seconds",
                self.storm_interval_seconds,
                1.0,
                7200.0,
            ),
            (
                "--storm-reconnect-seconds",
                self.storm_reconnect_seconds,
                0.0,
                600.0,
            ),
        ] {
            ensure!(
                value.is_finite() && (low..=high).contains(&value),
                "{name} must be finite and {low}..{high}"
            );
        }
        // A storm picks from the sessions it counts as connected: every one
        // not paused. A session still waiting out the previous storm's
        // reconnect delay is not paused, so it could be picked again, its
        // close would be ignored, and `dropped` would overstate the storm.
        // Refused here rather than measured wrong (#539 review).
        if storms.len() > 1 {
            ensure!(
                self.storm_reconnect_seconds < self.storm_interval_seconds,
                "--storm-reconnect-seconds ({}) must be shorter than --storm-interval-seconds \
                 ({}), or a storm could pick sessions the previous one left offline",
                self.storm_reconnect_seconds,
                self.storm_interval_seconds
            );
        }
        ensure!(
            bursts.upper_bound() <= crate::churn::MAX_RENTAL_SESSIONS,
            "--rental-bursts could add {} sessions, over {}",
            bursts.upper_bound(),
            crate::churn::MAX_RENTAL_SESSIONS
        );
        Ok(crate::churn::ChurnSpec {
            seconds: self.churn_seconds,
            tips: self.churn_tips,
            bursts,
            burst_window_seconds: self.rental_burst_window_seconds,
            burst_interval_seconds: self.rental_burst_interval_seconds,
            lifetime: crate::churn::Tail::parse(&self.rental_lifetime, "--rental-lifetime")?,
            rental_hashrate: self.rental_hashrate,
            storms,
            storm_interval_seconds: self.storm_interval_seconds,
            storm_reconnect_seconds: self.storm_reconnect_seconds,
            seed: self.seed,
        })
    }

    /// The most sessions the frontends can hold at once: `--sessions`, plus
    /// every rental the churn phase could have connected together.
    pub fn peak_sessions(&self) -> usize {
        let rentals = if self.churn_seconds > 0 {
            crate::churn::BurstSizes::parse(&self.rental_bursts)
                .map(|bursts| bursts.upper_bound())
                .unwrap_or(0)
        } else {
            0
        };
        self.sessions + rentals
    }

    pub fn arrival(&self) -> Result<crate::realism::Arrival> {
        crate::realism::Arrival::parse(&self.arrival)
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

/// Seconds of keepalive tips budgeted for a real-node run's setup and
/// teardown on top of its phases: seeding, frontend startup, the drain.
pub const REAL_NODE_SETUP_ALLOWANCE_SECONDS: u64 = 1_800;

/// The node behind the frontends (`--node`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeMode {
    Fake,
    Qbitd,
}

impl NodeMode {
    pub fn parse(text: &str) -> Result<Self> {
        match text {
            "fake" => Ok(Self::Fake),
            "qbitd" => Ok(Self::Qbitd),
            other => bail!("unknown --node {other:?}; use fake or qbitd"),
        }
    }
}

/// One phase of the run.
#[derive(Clone, Debug)]
pub struct PhasePlan {
    /// Unique within the run: every offer is stamped with it.
    pub name: String,
    /// What the phase is (`warm_up`, `steady_state`, `churn`, ...), which is
    /// what decides where tips are minted and blocks scheduled. The name
    /// outside a soak, whose phases are named per cycle.
    pub kind: String,
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
    /// With `reconnects` and two or more frontends, restart one frontend
    /// a third of the way in. Off in a soak, which is one server lifetime.
    pub restart_frontend: bool,
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
        Plan::Soak => bail!(
            "a soak's phases come from its preset's soak block (soak_driver::plan), not from \
             --plan alone"
        ),
        Plan::Short | Plan::Tips => (
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
            kind: "warm_up".into(),
            seconds: args.warmup_seconds,
            rate: args.background_shares_per_second.unwrap_or(steady_rate),
            in_artifact: false,
            reconnects: false,
            database_delay_ms: 0,
            mid_flight_kill: false,
            dense_cadence: false,
            restart_frontend: false,
        });
    }
    if plan == Plan::Tips {
        let rate = args.steady_state_rate.unwrap_or(args.rate);
        plans.extend(churn_phase(args, rate));
        plans.extend(faults_phase(args, rate)?);
        return Ok(plans);
    }
    plans.push(PhasePlan {
        name: "steady_state".into(),
        kind: "steady_state".into(),
        seconds: steady_seconds,
        rate: steady_rate,
        in_artifact: true,
        reconnects: false,
        database_delay_ms: 0,
        mid_flight_kill: false,
        dense_cadence: false,
        restart_frontend: false,
    });
    if let Some((seconds, rate)) = burst {
        plans.push(PhasePlan {
            name: "burst".into(),
            kind: "burst".into(),
            seconds,
            rate,
            in_artifact: false,
            reconnects: false,
            database_delay_ms: 0,
            mid_flight_kill: false,
            dense_cadence: false,
            restart_frontend: false,
        });
    }
    plans.push(PhasePlan {
        name: "reconnect".into(),
        kind: "reconnect".into(),
        seconds: reconnect_seconds,
        rate: steady_rate,
        in_artifact: true,
        reconnects: true,
        database_delay_ms: 0,
        mid_flight_kill: false,
        dense_cadence: false,
        restart_frontend: true,
    });
    plans.push(PhasePlan {
        name: "slow_database".into(),
        kind: "slow_database".into(),
        seconds: slow_seconds,
        rate: steady_rate,
        in_artifact: true,
        reconnects: false,
        database_delay_ms: args.slow_db_delay_ms,
        mid_flight_kill: false,
        dense_cadence: false,
        restart_frontend: false,
    });
    // The dense-cadence phase is a side phase, after `slow_database` and with
    // no proxy delay: the measurement is the frontends' rebuild latency, which
    // a delayed database would drown out.
    if args.cadence()?.is_dense() {
        plans.push(PhasePlan {
            name: crate::cadence::PHASE.into(),
            kind: crate::cadence::PHASE.into(),
            seconds: args.cadence_seconds,
            rate: args.cadence_rate.unwrap_or(steady_rate),
            in_artifact: false,
            reconnects: false,
            database_delay_ms: 0,
            mid_flight_kill: false,
            dense_cadence: true,
            restart_frontend: false,
        });
    }
    plans.extend(churn_phase(args, steady_rate));
    if args.mid_flight_kill {
        plans.push(PhasePlan {
            name: "mid_flight_kill".into(),
            kind: "mid_flight_kill".into(),
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
            restart_frontend: false,
        });
    }
    // Last of all, as documented: a fault the phase's bound cut short can
    // leave a frontend down, and no phase may follow that would read the
    // planned outage as a crash or aim its own kill at that frontend.
    plans.extend(faults_phase(args, steady_rate)?);
    Ok(plans)
}

/// The `churn` side phase, when `--churn-seconds` asks for one: no proxy
/// delay, outside the artifact.
fn churn_phase(args: &Args, steady_rate: f64) -> Option<PhasePlan> {
    (args.churn_seconds > 0).then(|| PhasePlan {
        name: crate::churn::PHASE.into(),
        kind: crate::churn::PHASE.into(),
        seconds: args.churn_seconds,
        rate: args.churn_rate.unwrap_or(steady_rate),
        in_artifact: false,
        reconnects: false,
        database_delay_ms: 0,
        mid_flight_kill: false,
        dense_cadence: false,
        restart_frontend: false,
    })
}

/// The `faults` side phase, when `--faults` asks for one (#554): no proxy
/// delay of its own, outside the artifact. Its length is the plan's upper
/// bound; the phase ends as soon as the last fault has recovered.
fn faults_phase(args: &Args, steady_rate: f64) -> Result<Option<PhasePlan>> {
    Ok(args.fault_plan()?.map(|plan| PhasePlan {
        name: crate::fault::PHASE.into(),
        kind: crate::fault::PHASE.into(),
        seconds: plan.phase_seconds_bound(args.frontends),
        rate: steady_rate,
        in_artifact: false,
        reconnects: false,
        database_delay_ms: 0,
        mid_flight_kill: false,
        dense_cadence: false,
        restart_frontend: false,
    }))
}
