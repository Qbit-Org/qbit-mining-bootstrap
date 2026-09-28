//! Checked-in run presets (#521).
//!
//! A preset pins every flag a run's result depends on, so a nightly number
//! never moves because a harness default did. `--preset <file>` adds the
//! preset's flags to the command line; clap then refuses any of them given
//! again, so the command line can add only the operational flags (where the
//! binaries and the output live), never change the measurement.
//!
//! EP-CONFIG: the harness reads the preset itself, through this one parser,
//! and refuses one that omits a result flag, so the file the workflow names
//! is the configuration that runs; the side report records its name and
//! SHA-256.

use anyhow::{bail, ensure, Context, Result};
use clap::CommandFactory;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

pub const SCHEMA: &str = "qbit.prism.load-preset.v1";

/// Flags that say where things are or how the output is kept, never what is
/// measured. A preset may not set them, and need not.
pub const OPERATIONAL_FLAGS: &[&str] = &[
    "--server-bin",
    "--pg-bin-dir",
    "--out",
    "--keep-artifacts",
    "--preset",
    "--allow-debug-server",
    "--allow-dirty-tree",
    "--allow-unverified-server-revision",
    "--example-artifact",
];

/// Result flags whose `null` (flag omitted) is itself an explicit choice
/// rather than a fall-back to a default: an external database is replaced
/// by a managed cluster, the short plan's burst phase is not run, and the
/// live sessions share one payout address as before `--recipients`.
/// Every other result flag must carry a value.
pub const NULL_MEANS_OFF: &[&str] = &[
    "--database-url",
    "--burst-seconds",
    "--burst-rate",
    "--recipients",
];

/// When the nightly workflow runs a preset.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Schedule {
    /// Every night, and on demand.
    Nightly,
    /// On demand only.
    Manual,
    /// The per-PR smoke test's preset; not run by the nightly workflow.
    Smoke,
}

/// What the gate holds a run of the preset to. See [`crate::gate`]. Every
/// key is required, `null` included, so a preset states what it does not
/// gate as plainly as what it does.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Gates {
    /// Phases the per-phase checks gate; `null` is every phase the run
    /// drove. A phase left out is still reported.
    pub phases: Option<Vec<String>>,
    /// Offers no session could take, per gated phase.
    pub max_shortfall: u64,
    /// #473's D1 rule: valid shares the server refused, per gated phase;
    /// `null` does not gate on it.
    pub max_rejected_valid_shares: Option<u64>,
    /// #473's D1 rule: submits with no answer, per gated phase; `null` does
    /// not gate on it.
    pub max_unanswered_submits: Option<u64>,
    /// Per tip, the slowest session's time to usable work on it; the p99
    /// over the run's tips (nearest rank). `null` reports it ungated.
    pub tip_last_notify_p99_budget_ms: Option<f64>,
    /// Print #473's D1 verdict table.
    pub d1_verdict_table: bool,
}

/// The keys of [`Gates`], every one required.
pub const GATE_KEYS: &[&str] = &[
    "phases",
    "max_shortfall",
    "max_rejected_valid_shares",
    "max_unanswered_submits",
    "tip_last_notify_p99_budget_ms",
    "d1_verdict_table",
];

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PresetFile {
    schema: String,
    name: String,
    description: String,
    issues: Vec<String>,
    schedule: Schedule,
    runner: String,
    timeout_minutes: u32,
    gates: serde_json::Map<String, Value>,
    args: BTreeMap<String, Value>,
}

/// One loaded preset.
#[derive(Clone, Debug)]
pub struct Preset {
    pub path: PathBuf,
    pub sha256: String,
    pub name: String,
    pub description: String,
    pub issues: Vec<String>,
    pub schedule: Schedule,
    pub runner: String,
    pub timeout_minutes: u32,
    pub gates: Gates,
    pub args: BTreeMap<String, Value>,
}

impl Preset {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let file: PresetFile = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing preset {}", path.display()))?;
        ensure!(
            file.schema == SCHEMA,
            "preset {} has schema {:?}, not {SCHEMA}",
            path.display(),
            file.schema
        );
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default();
        ensure!(
            file.name == stem,
            "preset {} is named {:?}; the name must be the file's stem",
            path.display(),
            file.name
        );
        let missing: Vec<&str> = GATE_KEYS
            .iter()
            .copied()
            .filter(|key| !file.gates.contains_key(*key))
            .collect();
        ensure!(
            missing.is_empty(),
            "preset {}: gates must state {} (null where it does not gate)",
            file.name,
            missing.join(", ")
        );
        let gates: Gates = serde_json::from_value(Value::Object(file.gates.clone()))
            .with_context(|| format!("preset {}: gates", file.name))?;
        if let Some(budget) = gates.tip_last_notify_p99_budget_ms {
            ensure!(
                budget.is_finite() && budget > 0.0,
                "preset {}: tip_last_notify_p99_budget_ms must be finite and positive",
                file.name
            );
        }
        if let Some(phases) = &gates.phases {
            ensure!(
                !phases.is_empty(),
                "preset {}: gates.phases is empty; use null for every phase",
                file.name
            );
        }
        ensure!(
            (1..=360).contains(&file.timeout_minutes),
            "preset {}: timeout_minutes must be 1..360",
            file.name
        );
        let preset = Self {
            path: path.to_owned(),
            sha256: hex::encode(Sha256::digest(&bytes)),
            name: file.name,
            description: file.description,
            issues: file.issues,
            schedule: file.schedule,
            runner: file.runner,
            timeout_minutes: file.timeout_minutes,
            gates,
            args: file.args,
        };
        preset.check_complete()?;
        Ok(preset)
    }

    /// The preset's flags as command-line words: a `true` flag bare, a
    /// `false` or `null` one omitted, anything else as `--flag value`.
    pub fn argv(&self) -> Result<Vec<String>> {
        let mut words = Vec::new();
        for (flag, value) in &self.args {
            match value {
                Value::Bool(true) => words.push(flag.clone()),
                Value::Bool(false) | Value::Null => {}
                Value::String(text) => {
                    words.push(flag.clone());
                    words.push(text.clone());
                }
                Value::Number(number) => {
                    words.push(flag.clone());
                    words.push(number.to_string());
                }
                other => bail!(
                    "preset {}: {flag} is {other}, not a string, number, boolean or null",
                    self.name
                ),
            }
        }
        Ok(words)
    }

    /// Every result flag the harness has, pinned; nothing else.
    pub fn check_complete(&self) -> Result<()> {
        let result_flags = result_flags();
        for flag in self.args.keys() {
            ensure!(
                !OPERATIONAL_FLAGS.contains(&flag.as_str()),
                "preset {} sets {flag}, which is operational: give it on the command line",
                self.name
            );
            ensure!(
                result_flags.contains(flag),
                "preset {} sets {flag}, which the harness does not have",
                self.name
            );
        }
        let missing: Vec<&String> = result_flags
            .iter()
            .filter(|flag| !self.args.contains_key(*flag))
            .collect();
        ensure!(
            missing.is_empty(),
            "preset {} does not pin {}; every flag a result depends on must be set explicitly \
             (null only for {})",
            self.name,
            missing
                .iter()
                .map(|flag| flag.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            NULL_MEANS_OFF.join(", ")
        );
        for (flag, value) in &self.args {
            if value.is_null() {
                ensure!(
                    NULL_MEANS_OFF.contains(&flag.as_str()),
                    "preset {}: {flag} is null, which would leave it to a harness default",
                    self.name
                );
            }
        }
        if self
            .args
            .get("--burst-seconds")
            .is_some_and(|v| !v.is_null())
        {
            ensure!(
                !self.args.get("--burst-rate").is_some_and(Value::is_null),
                "preset {}: --burst-seconds is set and --burst-rate is null, which would take \
                 --rate",
                self.name
            );
        }
        // The short plan's burst runs only when --burst-seconds is given, and
        // the tips plan has none; the D1 plan's runs either way, so there a
        // null would take the plan's default length and rate.
        let plan = self.args.get("--plan").and_then(Value::as_str);
        if !matches!(plan, Some("short" | "tips")) {
            for flag in ["--burst-seconds", "--burst-rate"] {
                ensure!(
                    !self.args.get(flag).is_some_and(Value::is_null),
                    "preset {}: {flag} is null under plan {plan:?}, which would take the \
                     plan's default",
                    self.name
                );
            }
        }
        Ok(())
    }
}

/// Every long flag of [`crate::cli::Args`] that is not operational.
pub fn result_flags() -> Vec<String> {
    crate::cli::Args::command()
        .get_arguments()
        .filter_map(|arg| arg.get_long())
        .map(|long| format!("--{long}"))
        .filter(|flag| {
            !OPERATIONAL_FLAGS.contains(&flag.as_str()) && flag != "--help" && flag != "--version"
        })
        .collect()
}

/// The presets directory checked in beside this crate.
pub fn presets_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("presets")
}

/// Every checked-in preset, by name.
pub fn load_all(dir: &Path) -> Result<Vec<Preset>> {
    let mut presets = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            presets.push(Preset::load(&path)?);
        }
    }
    presets.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(presets)
}

/// The command line with `--preset <file>`'s flags appended, and the preset.
/// A line with no `--preset` is returned unchanged.
pub fn expand_command_line(argv: Vec<OsString>) -> Result<(Vec<OsString>, Option<Preset>)> {
    let mut path: Option<PathBuf> = None;
    let mut words = argv.iter();
    while let Some(word) = words.next() {
        let text = word.to_string_lossy();
        let named = if text == "--preset" {
            Some(PathBuf::from(
                words.next().context("--preset needs a file")?.clone(),
            ))
        } else {
            text.strip_prefix("--preset=").map(PathBuf::from)
        };
        if let Some(named) = named {
            ensure!(path.is_none(), "--preset is given twice");
            path = Some(named);
        }
    }
    let Some(path) = path else {
        return Ok((argv, None));
    };
    let preset = Preset::load(&path)?;
    let mut expanded = argv;
    expanded.extend(preset.argv()?.into_iter().map(OsString::from));
    Ok((expanded, Some(preset)))
}
