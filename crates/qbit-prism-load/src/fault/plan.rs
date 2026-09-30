//! The `--faults` specification (#554): which faults the `faults` phase
//! injects, in what order, and with what timing.
//!
//! One flag carries the whole plan, so a preset pins it in one line:
//!
//! ```text
//! <fault>[,<fault>...][;<key>=<value>...]
//! ```
//!
//! Faults are listed by name ([`FaultKind::name`]). The keys:
//!
//! - `order`: `listed` (the default) injects each listed fault once, in
//!   order; `random` draws `count` faults from the list with the seeded
//!   stream, so #556's soak can fire one every few minutes and a failed soak
//!   replays from its report.
//! - `seed`: the draw's seed (default 1). `count`: draws under `random`
//!   (default: as many as are listed).
//! - `baseline`, `hold`, `recovery`: seconds before each injection, of the
//!   injection, and after its removal (defaults 10, 30, 20). A fault that is
//!   an action rather than a state (a restart) has no hold: its injection
//!   ends when the frontend serves again.
//! - `gap`: seconds between one fault's recovery and the next fault's
//!   baseline, `<s>` or `<min>..<max>` drawn per fault (default 0).
//! - `read-tier`: `on` (the default) scrapes the public API and every
//!   frontend's `/metrics` throughout; `off` does not.
//! - `storm`: the fraction of sessions a reconnect storm drops (default 0.5).
//! - `lease-wait`: seconds the SIGKILL fault waits for the killed holder's
//!   found block to land through the candidate lease (default 180).
//!
//! EP-VALIDATION: everything is checked here, at the entry boundary, with the
//! offending text in the error.

use crate::realism::Rng;
use anyhow::{bail, ensure, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};

/// One injectable fault.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FaultKind {
    /// The delay proxy holds every chunk between the frontends and
    /// PostgreSQL for `--slow-db-delay-ms`.
    SlowDatabase,
    /// Every free PostgreSQL connection slot is taken and the frontends'
    /// idle backends are terminated, so a frontend that needs a connection
    /// cannot get one.
    PoolExhaustion,
    /// An outside transaction holds `SETTLEMENT_LOCK`; an external tip is
    /// minted halfway through, so its jobs wait for the release.
    SettlementLock,
    /// SIGKILL of the frontend offering a found block, after the node
    /// accepted it and before its answer is recorded.
    FrontendSigkill,
    /// SIGTERM of a frontend while it offers a found block.
    SigtermDrain,
    /// SIGTERM and relaunch of every frontend in turn, its sessions moved to
    /// the one still serving.
    RollingRestart,
    /// A fraction of the sessions drop abruptly and return within seconds.
    ReconnectStorm,
}

pub const ALL: [FaultKind; 7] = [
    FaultKind::SlowDatabase,
    FaultKind::PoolExhaustion,
    FaultKind::SettlementLock,
    FaultKind::FrontendSigkill,
    FaultKind::SigtermDrain,
    FaultKind::RollingRestart,
    FaultKind::ReconnectStorm,
];

impl FaultKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::SlowDatabase => "slow-database",
            Self::PoolExhaustion => "pool-exhaustion",
            Self::SettlementLock => "settlement-lock",
            Self::FrontendSigkill => "frontend-sigkill",
            Self::SigtermDrain => "sigterm-drain",
            Self::RollingRestart => "rolling-restart",
            Self::ReconnectStorm => "reconnect-storm",
        }
    }

    pub fn parse(text: &str) -> Result<Self> {
        ALL.into_iter()
            .find(|kind| kind.name() == text)
            .with_context(|| {
                format!(
                    "unknown fault {text:?}; use {}",
                    ALL.map(Self::name).join(", ")
                )
            })
    }

    /// A fault that is one action (a signal and a relaunch) rather than a
    /// state held for `hold` seconds.
    pub fn is_action(self) -> bool {
        matches!(
            self,
            Self::FrontendSigkill | Self::SigtermDrain | Self::RollingRestart
        )
    }

    /// Faults that need two frontends: one to fail and one to serve.
    pub fn needs_two_frontends(self) -> bool {
        matches!(self, Self::RollingRestart)
    }

    /// Faults that act on the database server itself, which only a cluster
    /// the harness manages may be subjected to.
    pub fn needs_managed_cluster(self) -> bool {
        matches!(self, Self::PoolExhaustion)
    }
}

/// How the listed faults are sequenced.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Order {
    Listed,
    Random,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FaultPlan {
    /// The specification exactly as given, for the report.
    pub spec: String,
    pub listed: Vec<FaultKind>,
    pub order: Order,
    pub seed: u64,
    pub count: usize,
    pub baseline_seconds: u64,
    pub hold_seconds: u64,
    pub recovery_seconds: u64,
    pub gap_seconds: (u64, u64),
    pub read_tier: bool,
    pub storm_fraction: f64,
    pub lease_wait_seconds: u64,
}

/// One fault as drawn: the kind and the gap that precedes its baseline.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct Scheduled {
    pub kind: FaultKind,
    pub gap_seconds: u64,
}

/// Time the phase allows an action fault beyond its planned windows: the
/// drain (the server's 30 s task join), the relaunch and its readiness wait.
pub const ACTION_ALLOWANCE_SECONDS: u64 = 90;

impl FaultPlan {
    pub fn parse(spec: &str) -> Result<Self> {
        let mut sections = spec.split(';');
        let names = sections.next().unwrap_or_default().trim();
        ensure!(!names.is_empty(), "--faults {spec:?} lists no fault");
        let listed = names
            .split(',')
            .map(|name| FaultKind::parse(name.trim()))
            .collect::<Result<Vec<_>>>()
            .with_context(|| format!("--faults {spec:?}"))?;
        let mut plan = Self {
            spec: spec.to_owned(),
            count: listed.len(),
            listed,
            order: Order::Listed,
            seed: 1,
            baseline_seconds: 10,
            hold_seconds: 30,
            recovery_seconds: 20,
            gap_seconds: (0, 0),
            read_tier: true,
            storm_fraction: 0.5,
            lease_wait_seconds: 180,
        };
        let mut count_given = false;
        for option in sections {
            let option = option.trim();
            if option.is_empty() {
                continue;
            }
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("--faults {spec:?}: {option:?} is not key=value"))?;
            let (key, value) = (key.trim(), value.trim());
            let number = |what: &str| -> Result<u64> {
                value.parse::<u64>().with_context(|| {
                    format!("--faults {spec:?}: {what}={value:?} is not a whole number")
                })
            };
            match key {
                "order" => {
                    plan.order = match value {
                        "listed" => Order::Listed,
                        "random" => Order::Random,
                        other => bail!("--faults {spec:?}: order={other:?}; use listed or random"),
                    }
                }
                "seed" => plan.seed = number("seed")?,
                "count" => {
                    plan.count = number("count")? as usize;
                    count_given = true;
                }
                "baseline" => plan.baseline_seconds = number("baseline")?,
                "hold" => plan.hold_seconds = number("hold")?,
                "recovery" => plan.recovery_seconds = number("recovery")?,
                "gap" => {
                    plan.gap_seconds = match value.split_once("..") {
                        Some((low, high)) => {
                            let parse = |text: &str| {
                                text.trim().parse::<u64>().with_context(|| {
                                    format!("--faults {spec:?}: gap={value:?} is not <min>..<max>")
                                })
                            };
                            (parse(low)?, parse(high)?)
                        }
                        None => {
                            let seconds = number("gap")?;
                            (seconds, seconds)
                        }
                    };
                    ensure!(
                        plan.gap_seconds.0 <= plan.gap_seconds.1,
                        "--faults {spec:?}: gap={value:?} has its minimum above its maximum"
                    );
                }
                "read-tier" => {
                    plan.read_tier = match value {
                        "on" => true,
                        "off" => false,
                        other => bail!("--faults {spec:?}: read-tier={other:?}; use on or off"),
                    }
                }
                "storm" => {
                    let fraction: f64 = value.parse().with_context(|| {
                        format!("--faults {spec:?}: storm={value:?} is not a fraction")
                    })?;
                    ensure!(
                        fraction > 0.0 && fraction <= 1.0,
                        "--faults {spec:?}: storm={value} must be in (0, 1]"
                    );
                    plan.storm_fraction = fraction;
                }
                "lease-wait" => plan.lease_wait_seconds = number("lease-wait")?,
                other => bail!(
                    "--faults {spec:?}: unknown key {other:?}; use order, seed, count, baseline, \
                     hold, recovery, gap, read-tier, storm or lease-wait"
                ),
            }
        }
        ensure!(plan.count > 0, "--faults {spec:?}: count must be positive");
        ensure!(
            plan.order == Order::Random || !count_given || plan.count == plan.listed.len(),
            "--faults {spec:?}: count applies to order=random; listed faults run once each"
        );
        ensure!(
            plan.hold_seconds > 0 && plan.recovery_seconds >= 4,
            "--faults {spec:?}: hold must be positive and recovery at least 4 s, so the \
             recovery check has a second half to read"
        );
        Ok(plan)
    }

    /// The faults in the order they will run, each with the gap before it.
    /// Deterministic in the plan: the same specification draws the same
    /// sequence, which the report records.
    pub fn schedule(&self) -> Vec<Scheduled> {
        let mut rng = Rng::new(self.seed, "faults");
        let gap = |rng: &mut Rng| {
            let (low, high) = self.gap_seconds;
            low + (rng.next_f64() * (high - low + 1) as f64).floor() as u64
        };
        match self.order {
            Order::Listed => self
                .listed
                .iter()
                .map(|kind| Scheduled {
                    kind: *kind,
                    gap_seconds: gap(&mut rng),
                })
                .collect(),
            Order::Random => (0..self.count)
                .map(|_| {
                    let index = ((rng.next_f64() * self.listed.len() as f64) as usize)
                        .min(self.listed.len() - 1);
                    Scheduled {
                        kind: self.listed[index],
                        gap_seconds: gap(&mut rng),
                    }
                })
                .collect(),
        }
    }

    /// An upper bound on the `faults` phase: every planned window, every gap,
    /// and each action fault's drain and relaunch allowance, plus the lease
    /// wait for a SIGKILL. The phase ends as soon as the last fault's
    /// recovery window closes; this bound only stops a fault that never
    /// finishes from running forever.
    pub fn phase_seconds_bound(&self, frontends: usize) -> u64 {
        self.schedule()
            .iter()
            .map(|scheduled| {
                let mut seconds = scheduled.gap_seconds
                    + self.baseline_seconds
                    + self.hold_seconds
                    + self.recovery_seconds;
                if scheduled.kind.is_action() {
                    seconds += ACTION_ALLOWANCE_SECONDS;
                }
                if scheduled.kind == FaultKind::RollingRestart {
                    // One drain and relaunch per frontend, each its own turn.
                    seconds += ACTION_ALLOWANCE_SECONDS * (frontends as u64).saturating_sub(1);
                }
                if scheduled.kind == FaultKind::FrontendSigkill {
                    seconds += self.lease_wait_seconds;
                }
                seconds
            })
            .sum::<u64>()
            + 30
    }

    /// Refuse a plan the run cannot carry out.
    pub fn check_against(&self, frontends: usize, managed_cluster: bool) -> Result<()> {
        for kind in &self.listed {
            ensure!(
                !kind.needs_two_frontends() || frontends >= 2,
                "--faults: {} needs at least two frontends, one to restart while the other \
                 serves",
                kind.name()
            );
            ensure!(
                !kind.needs_managed_cluster() || managed_cluster,
                "--faults: {} acts on the PostgreSQL server itself, so it runs only against the \
                 cluster the harness manages, never a --database-url",
                kind.name()
            );
        }
        Ok(())
    }

    pub fn report(&self) -> Value {
        json!({
            "spec": self.spec,
            "listed": self.listed.iter().map(|kind| kind.name()).collect::<Vec<_>>(),
            "order": self.order,
            "seed": self.seed,
            "count": self.count,
            "baseline_seconds": self.baseline_seconds,
            "hold_seconds": self.hold_seconds,
            "recovery_seconds": self.recovery_seconds,
            "gap_seconds": [self.gap_seconds.0, self.gap_seconds.1],
            "read_tier": self.read_tier,
            "storm_fraction": self.storm_fraction,
            "lease_wait_seconds": self.lease_wait_seconds,
            "schedule": self.schedule().iter().map(|scheduled| json!({
                "fault": scheduled.kind.name(),
                "gap_seconds": scheduled.gap_seconds,
            })).collect::<Vec<_>>(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_listed_plan_runs_each_fault_once_in_order() {
        let plan = FaultPlan::parse("sigterm-drain,settlement-lock;hold=12;recovery=8").unwrap();
        assert_eq!(
            plan.schedule()
                .iter()
                .map(|scheduled| scheduled.kind)
                .collect::<Vec<_>>(),
            vec![FaultKind::SigtermDrain, FaultKind::SettlementLock]
        );
        assert_eq!((plan.hold_seconds, plan.recovery_seconds), (12, 8));
        assert!(plan.read_tier);
    }

    #[test]
    fn a_random_plan_is_seeded_and_draws_only_listed_faults() {
        let spec = "slow-database,reconnect-storm,settlement-lock;order=random;count=40;seed=9;gap=180..300";
        let first = FaultPlan::parse(spec).unwrap().schedule();
        let second = FaultPlan::parse(spec).unwrap().schedule();
        assert_eq!(first, second, "the same spec draws the same sequence");
        assert_eq!(first.len(), 40);
        assert!(first.iter().all(|scheduled| {
            (180..=300).contains(&scheduled.gap_seconds)
                && scheduled.kind != FaultKind::FrontendSigkill
        }));
        let other = FaultPlan::parse(&spec.replace("seed=9", "seed=10"))
            .unwrap()
            .schedule();
        assert_ne!(first, other, "another seed draws another sequence");
    }

    #[test]
    fn bad_specifications_are_refused_with_their_text() {
        for (spec, fragment) in [
            ("", "lists no fault"),
            ("slow-db", "unknown fault \"slow-db\""),
            ("slow-database;order=shuffled", "order=\"shuffled\""),
            ("slow-database;gap=9..3", "minimum above its maximum"),
            ("slow-database;storm=1.5", "must be in (0, 1]"),
            ("slow-database;count=3", "count applies to order=random"),
            ("slow-database;recovery=2", "recovery at least 4 s"),
            ("slow-database;hold", "is not key=value"),
            ("slow-database;speed=2", "unknown key \"speed\""),
        ] {
            let error = format!("{:#}", FaultPlan::parse(spec).unwrap_err());
            assert!(error.contains(fragment), "{spec:?}: {error}");
        }
    }

    #[test]
    fn a_plan_the_topology_cannot_carry_is_refused() {
        let rolling = FaultPlan::parse("rolling-restart").unwrap();
        assert!(rolling.check_against(1, true).is_err());
        assert!(rolling.check_against(2, true).is_ok());
        let exhaustion = FaultPlan::parse("pool-exhaustion").unwrap();
        assert!(exhaustion.check_against(2, false).is_err());
        use clap::Parser;
        let soak = crate::cli::Args::parse_from([
            "qbit-prism-load",
            "--plan",
            "soak",
            "--faults",
            "sigterm-drain",
        ]);
        let error = format!("{:#}", soak.validate().unwrap_err());
        assert!(error.contains("--plan soak"), "{error}");
    }

    #[test]
    fn the_phase_bound_covers_every_window_and_allowance() {
        let plan =
            FaultPlan::parse("frontend-sigkill;baseline=5;hold=10;recovery=20;lease-wait=150")
                .unwrap();
        assert_eq!(
            plan.phase_seconds_bound(2),
            5 + 10 + 20 + ACTION_ALLOWANCE_SECONDS + 150 + 30
        );
        let rolling = FaultPlan::parse("rolling-restart;baseline=5;hold=10;recovery=20").unwrap();
        assert_eq!(
            rolling.phase_seconds_bound(4),
            5 + 10 + 20 + 4 * ACTION_ALLOWANCE_SECONDS + 30
        );
    }

    #[test]
    fn the_faults_phase_runs_after_every_other_phase() {
        use clap::Parser;
        let side_phases = ["--churn-seconds", "30", "--frontends", "2"];
        for extra in [&["--mid-flight-kill"][..], &["--plan", "tips"][..]] {
            let args = crate::cli::Args::parse_from(
                ["qbit-prism-load", "--faults", "sigterm-drain"]
                    .iter()
                    .chain(&side_phases)
                    .chain(extra),
            );
            args.validate().unwrap();
            let names: Vec<String> = crate::cli::phases(&args)
                .unwrap()
                .into_iter()
                .map(|phase| phase.name)
                .collect();
            assert_eq!(
                names.last().map(String::as_str),
                Some(crate::fault::PHASE),
                "{names:?}"
            );
        }
    }
}
