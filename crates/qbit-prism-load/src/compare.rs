//! The A/B release comparison (#511): two builds' interleaved repeats of
//! one preset on one host, summarized against #473's D1 rule.
//!
//! `scripts/prism_load_ab.py` drives the runs and writes a manifest; this
//! module reads the manifest and each run's side report and prints one
//! table per D1 phase (`steady_state`, `burst`) with a row per build, and a
//! verdict: the candidate passes when it meets the rule in every phase the
//! preset gates.
//!
//! A build meets the rule in a phase when every one of its repeats exited 0
//! and, in every repeat, `shortfall == 0`, no valid share was refused, no
//! submit went unanswered, and the client's ACK p99 stayed within the
//! validator limit the run used. #473 defined "met" from the harness's own
//! accounting rather than `achieved >= target`, because a perfect 300 s phase
//! places 149,999 tokens and reads 499.997.
//!
//! EP-OBSERVABILITY: a figure a report does not carry is "n/a" and fails the
//! rule; it is never read as zero. A run whose report names another build's
//! revision is refused outright, never pooled.

use crate::gate::{evaluate, number, passed as gate_passed, phase_figures, Budgets};
use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

pub const MANIFEST_SCHEMA: &str = "qbit.prism.load-ab.v1";

/// The phases #473's D1 rows cover.
pub const D1_PHASES: &[&str] = &["steady_state", "burst"];

/// The reference host's flush cost: `pg_test_fsync`'s one-8 kB-write
/// fdatasync on the filesystem the harness builds its clusters on, as #479
/// STEP 1 recorded it (docs/prism-release-benchmark.md).
pub const REFERENCE_FDATASYNC_USECS: f64 = 288.0;

/// How far a host's flush cost may sit from the reference's, as a factor,
/// and still be read as the same flush class.
pub const REFERENCE_FLUSH_FACTOR: f64 = 2.0;

#[derive(Clone, Debug, Deserialize)]
pub struct Manifest {
    pub schema: String,
    pub preset: ManifestPreset,
    #[serde(default)]
    pub host: Value,
    #[serde(default)]
    pub pg_test_fsync: Value,
    /// The driver's settings; `repeats` is how many runs each build owes.
    #[serde(default)]
    pub settings: Value,
    pub builds: Vec<Build>,
    pub runs: Vec<Run>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManifestPreset {
    pub name: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Build {
    /// `base` or `candidate`.
    pub label: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub commit: String,
    #[serde(default)]
    pub dropped_legacy_flags: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Run {
    pub id: String,
    pub build: String,
    pub repeat: u32,
    /// The harness's `--out`, relative to the manifest.
    pub dir: String,
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub ceiling_hit: bool,
    pub load_before: Option<f64>,
    pub load_max: Option<f64>,
    pub mem_available_min_mib: Option<u64>,
}

/// One run with its side report, or the reason it is outside the medians.
#[derive(Clone, Debug)]
pub struct LoadedRun {
    pub run: Run,
    pub report: Option<Value>,
    pub excluded: Option<String>,
}

impl LoadedRun {
    /// The report's phase `name`, when the run has a report that drove it.
    pub fn phase(&self, name: &str) -> Option<&Value> {
        self.report
            .as_ref()?
            .get("phases")?
            .as_array()?
            .iter()
            .find(|phase| phase["name"] == name)
    }
}

pub fn read_manifest(path: &Path) -> Result<Manifest> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let manifest: Manifest =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    ensure!(
        manifest.schema == MANIFEST_SCHEMA,
        "{} has schema {:?}, not {MANIFEST_SCHEMA}",
        path.display(),
        manifest.schema
    );
    for label in ["base", "candidate"] {
        ensure!(
            manifest.builds.iter().filter(|b| b.label == label).count() == 1,
            "the manifest must name exactly one {label} build"
        );
    }
    // A series cut short, or resumed with fewer repeats, is not summarized:
    // a verdict over fewer runs than the series promised could pass a build
    // the missing repeats would fail.
    let counts: Vec<usize> = ["base", "candidate"]
        .iter()
        .map(|label| manifest.runs.iter().filter(|r| r.build == *label).count())
        .collect();
    let owed = manifest.settings["repeats"].as_u64();
    ensure!(
        counts[0] == counts[1] && counts[0] > 0,
        "the series is incomplete: {} base and {} candidate runs",
        counts[0],
        counts[1]
    );
    if let Some(owed) = owed {
        ensure!(
            counts[0] as u64 == owed,
            "the series is incomplete: {} of {owed} repeats per build",
            counts[0]
        );
    }
    for run in &manifest.runs {
        ensure!(
            manifest.builds.iter().any(|b| b.label == run.build),
            "run {} names build {:?}, which the manifest does not list",
            run.id,
            run.build
        );
    }
    Ok(manifest)
}

/// Every run's report, refusing one that names another build's revision.
pub fn load_runs(manifest: &Manifest, base_dir: &Path) -> Result<Vec<LoadedRun>> {
    let mut loaded = Vec::new();
    for run in &manifest.runs {
        let build = manifest
            .builds
            .iter()
            .find(|b| b.label == run.build)
            .context("run of an unlisted build")?;
        // A run that did not exit 0 fails its build whatever its report
        // says, and none of its figures are pooled; the harness's reduced
        // failure report carries no `versions` block, and a run killed at
        // the ceiling may leave a truncated one, so neither is required.
        let failed = if run.ceiling_hit {
            Some("killed at the run ceiling".to_owned())
        } else if run.exit_code != Some(0) {
            Some(format!(
                "exit {}",
                run.exit_code
                    .map_or("not captured".into(), |c| c.to_string())
            ))
        } else {
            None
        };
        let path = base_dir.join(&run.dir).join("load-harness-report.json");
        let report: Option<Value> = match std::fs::read_to_string(&path) {
            Ok(text) => match serde_json::from_str(&text) {
                Ok(report) => Some(report),
                Err(_) if failed.is_some() => None,
                Err(error) => bail!("parsing {}: {error}", path.display()),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => bail!("reading {}: {error}", path.display()),
        };
        if let Some(report) = &report {
            // Revision evidence is required of every report whose figures
            // count, and a report that names another build is refused
            // either way.
            let revision = report["versions"]["coordinator_revision"].as_str();
            ensure!(
                revision == Some(build.commit.as_str()) || (failed.is_some() && revision.is_none()),
                "run {}: its report names revision {revision:?}, not the {} build's {}; \
                 runs of another build are never pooled",
                run.id,
                build.label,
                build.commit
            );
        }
        let excluded = if failed.is_some() {
            failed
        } else if report.is_none() {
            Some("no side report".to_owned())
        } else if report.as_ref().is_some_and(|r| r["dirty"] == true) {
            Some("built from a dirty tree".to_owned())
        } else {
            None
        };
        loaded.push(LoadedRun {
            run: run.clone(),
            report,
            excluded,
        });
    }
    Ok(loaded)
}

/// The median; for an even count the mean of the middle two, as #473 read
/// a two-run cell.
pub fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}

fn spread(values: &[f64], target: Option<f64>) -> String {
    match median(values) {
        None => "n/a".into(),
        Some(mid) => {
            let min = values.iter().copied().fold(f64::INFINITY, f64::min);
            let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            format!(
                "{} ({}–{})",
                number(mid, target),
                number(min, target),
                number(max, target)
            )
        }
    }
}

fn per_run(values: &[Option<u64>]) -> String {
    if values.is_empty() {
        return "n/a".into();
    }
    values
        .iter()
        .map(|v| v.map_or("n/a".into(), |n| number(n as f64, None)))
        .collect::<Vec<_>>()
        .join(" / ")
}

/// One build's row for one phase.
#[derive(Clone, Debug)]
pub struct Row {
    pub build: String,
    pub met: bool,
    pub verdict: String,
    pub cells: Vec<String>,
    pub achieved_median: Option<f64>,
    pub ack_p99_median: Option<f64>,
}

pub const COLUMNS: &[&str] = &[
    "build",
    "commit",
    "n (in medians / run)",
    "target /s",
    "achieved /s, median (min–max)",
    "shortfall tokens, per run",
    "valid shares refused, per run",
    "submits unanswered, per run",
    "ACK p50 ms, median",
    "ACK p99 ms, median (min–max)",
    "ACK p99 limit ms",
    "ORDER_LOCK max / mean waiters, median",
    "verdict",
    "runs outside the medians",
];

/// One build's row for `phase`: the D1 rule over every repeat.
pub fn row(build: &Build, runs: &[&LoadedRun], phase: &str) -> Row {
    let total = runs.len();
    let included: Vec<&&LoadedRun> = runs.iter().filter(|r| r.excluded.is_none()).collect();
    let outside: Vec<String> = runs
        .iter()
        .filter_map(|r| {
            r.excluded
                .as_ref()
                .map(|why| format!("{} ({why})", r.run.id))
        })
        .collect();
    let phases: Vec<(&LoadedRun, Option<&Value>)> =
        included.iter().map(|r| (**r, r.phase(phase))).collect();
    let target = phases
        .iter()
        .find_map(|(_, p)| p.and_then(|p| p["target_rate_shares_per_second"].as_f64()));
    let floats = |key: &dyn Fn(&Value) -> Option<f64>| -> Vec<f64> {
        phases.iter().filter_map(|(_, p)| p.and_then(key)).collect()
    };
    let achieved = floats(&|p| p["achieved_rate_shares_per_second"].as_f64());
    let p50 = floats(&|p| p["client_ack_latency"]["p50"].as_f64());
    let p99 = floats(&|p| p["client_ack_latency"]["p99"].as_f64());
    let lock_max = floats(&|p| p["order_lock"]["max_waiters"].as_f64());
    let lock_mean = floats(&|p| p["order_lock"]["mean_waiters"].as_f64());
    let mut shortfall = Vec::new();
    let mut rejected = Vec::new();
    let mut unanswered = Vec::new();
    let mut limits = Vec::new();
    let mut reasons = Vec::new();
    for (run, phase_value) in &phases {
        let report = run.report.as_ref().expect("an included run has a report");
        let Some(phase_value) = phase_value else {
            reasons.push(format!("{}: phase absent", run.run.id));
            shortfall.push(None);
            rejected.push(None);
            unanswered.push(None);
            continue;
        };
        let figures = phase_figures(report, phase_value);
        shortfall.push(figures.shortfall);
        rejected.push(figures.rejected_valid_shares);
        unanswered.push(figures.unanswered);
        let limit = report["validator"]["ack_p99_limit_used_milliseconds"].as_f64();
        limits.extend(limit);
        let run_p99 = phase_value["client_ack_latency"]["p99"].as_f64();
        let id = &run.run.id;
        match figures.shortfall {
            Some(0) => {}
            Some(n) => reasons.push(format!("{id}: shortfall {}", number(n as f64, None))),
            None => reasons.push(format!("{id}: shortfall unreported")),
        }
        match figures.rejected_valid_shares {
            Some(0) => {}
            Some(n) => reasons.push(format!("{id}: {n} valid refused")),
            None => reasons.push(format!("{id}: valid refused unreported")),
        }
        match figures.unanswered {
            Some(0) => {}
            Some(n) => reasons.push(format!("{id}: {n} unanswered")),
            None => reasons.push(format!("{id}: unanswered unreported")),
        }
        match (run_p99, limit) {
            (Some(p), Some(l)) if p <= l => {}
            (Some(p), Some(l)) => reasons.push(format!(
                "{id}: ACK p99 {} ms over {} ms",
                number(p, None),
                number(l, None)
            )),
            _ => reasons.push(format!("{id}: ACK p99 or its limit unreported")),
        }
    }
    if total == 0 {
        reasons.push("no run".into());
    }
    for why in &outside {
        reasons.push(format!("{why} is outside the medians"));
    }
    let met = reasons.is_empty();
    let verdict = if met {
        format!("**met** ({total} of {total})")
    } else {
        format!("**not met**: {}", reasons.join("; "))
    };
    let limit_cell = match (
        limits.iter().copied().reduce(f64::min),
        limits.iter().copied().reduce(f64::max),
    ) {
        (Some(lo), Some(hi)) if lo == hi => number(lo, None),
        (Some(lo), Some(hi)) => format!("{}–{}", number(lo, None), number(hi, None)),
        _ => "n/a".into(),
    };
    let cells = vec![
        format!("{} (`{}`)", build.label, build.reference),
        format!("`{}`", &build.commit[..build.commit.len().min(8)]),
        format!("{} / {total}", included.len()),
        target.map_or("n/a".into(), |t| number(t, None)),
        spread(&achieved, target),
        per_run(&shortfall),
        per_run(&rejected),
        per_run(&unanswered),
        median(&p50).map_or("n/a".into(), |m| number(m, None)),
        spread(&p99, None),
        limit_cell,
        match (median(&lock_max), median(&lock_mean)) {
            (Some(max), Some(mean)) => format!("{} / {mean:.2}", number(max, None)),
            _ => "n/a".into(),
        },
        verdict.clone(),
        if outside.is_empty() {
            "–".into()
        } else {
            outside.join(", ")
        },
    ];
    Row {
        build: build.label.clone(),
        met,
        verdict,
        cells,
        achieved_median: median(&achieved),
        ack_p99_median: median(&p99),
    }
}

/// The comparison: the Markdown and whether the candidate passes.
pub struct Comparison {
    pub markdown: String,
    pub passed: bool,
}

fn host_line(manifest: &Manifest) -> String {
    let host = &manifest.host;
    let text = |key: &str| host[key].as_str().map(str::to_owned);
    let num = |key: &str| host[key].as_f64().map(|n| number(n, None));
    format!(
        "{} · {} vCPU · {} MiB · Linux {}",
        text("hostname").unwrap_or_else(|| "host n/a".into()),
        num("nproc").unwrap_or_else(|| "n/a".into()),
        num("mem_total_mib").unwrap_or_else(|| "n/a".into()),
        text("kernel").unwrap_or_else(|| "n/a".into()),
    )
}

fn flush_lines(manifest: &Manifest) -> String {
    let mut out = String::new();
    let mut costs = Vec::new();
    for when in ["before", "after"] {
        let usecs = manifest.pg_test_fsync[when]["fdatasync_usecs_per_op"].as_f64();
        costs.extend(usecs);
        out.push_str(&format!(
            "- `pg_test_fsync` fdatasync, one 8 kB write, {when} the series: {}\n",
            usecs.map_or("not recorded".into(), |u| format!("{} µs", number(u, None)))
        ));
    }
    let reference = REFERENCE_FDATASYNC_USECS;
    let class = if costs.len() < 2 {
        "a flush reading is missing, so this host cannot be placed against the reference \
         host"
            .to_owned()
    } else if costs.iter().all(|&u| {
        u >= reference / REFERENCE_FLUSH_FACTOR && u <= reference * REFERENCE_FLUSH_FACTOR
    }) {
        format!(
            "within {REFERENCE_FLUSH_FACTOR}× of the reference host's {} µs: the reference \
             flush class",
            number(reference, None)
        )
    } else {
        format!(
            "**outside {REFERENCE_FLUSH_FACTOR}× of the reference host's {} µs**: rates here \
             are not comparable with the reference host's (docs/prism-release-benchmark.md)",
            number(reference, None)
        )
    };
    out.push_str(&format!("- Flush class: {class}\n"));
    out
}

/// Every run held to the preset's own gates, as `qbit-prism-load-gate` holds
/// a nightly run: the D1 table covers only the D1 rule, and a preset may
/// gate more (tip delivery, other phases). One line per build, and whether
/// every one of its runs passed.
fn preset_gate_rows(
    builds: [&Build; 2],
    runs: &[LoadedRun],
    budgets: &Budgets,
) -> (String, [bool; 2]) {
    let mut out = String::from(
        "\n### The preset's own gates\n\n| build | runs passing | failed checks |\n|---|---|---|\n",
    );
    let mut passed = [true, true];
    for (index, build) in builds.iter().enumerate() {
        let mine: Vec<&LoadedRun> = runs.iter().filter(|r| r.run.build == build.label).collect();
        let mut failures = Vec::new();
        let mut passing = 0;
        for run in &mine {
            let exit_code = if run.run.ceiling_hit {
                None
            } else {
                run.run.exit_code
            };
            let report = run.report.clone().unwrap_or(Value::Null);
            let checks = evaluate(&report, exit_code, budgets);
            if gate_passed(&checks) {
                passing += 1;
            } else {
                failures.extend(
                    checks
                        .iter()
                        .filter(|check| check.pass == Some(false))
                        .map(|check| {
                            format!("{}: {} ({})", run.run.id, check.name, check.observed)
                        }),
                );
            }
        }
        passed[index] = !mine.is_empty() && passing == mine.len();
        out.push_str(&format!(
            "| {} | {passing} / {} | {} |\n",
            build.label,
            mine.len(),
            if failures.is_empty() {
                "–".to_owned()
            } else {
                failures.join("; ").replace('|', "\\|")
            }
        ));
    }
    (out, passed)
}

/// What a phase was planned to do, as every harness since #271 reports it:
/// every counted run of either build must report the same for each phase,
/// so no build skips the scheduled blocks, the database delay or the
/// restart a phase carries.
pub const PHASE_PLAN_FIELDS: &[&str] = &[
    "in_artifact",
    "database_delay_milliseconds_configured",
    "scheduled_blocks",
    "frontend_restarts",
];

/// How far a phase's measured length may spread across the counted runs of
/// both builds, as a fraction of the shortest: the harness times each phase
/// on the clock, so equal plans differ by milliseconds, while a build that
/// read a pinned duration differently would differ by far more.
pub const DURATION_TOLERANCE: f64 = 0.05;

/// Whether every counted run of either build drove `phase` as one workload:
/// `None` when they did (or none ran it), otherwise why not. A run that
/// left the phase out while another ran it, reported no target rate or no
/// length, a second distinct target, or lengths spread past
/// [`DURATION_TOLERANCE`] each fail. A phase reported only for information
/// still has to be the same workload on both sides. `frontends` is the
/// pinned count, which decides the planned restarts.
fn phase_workload(
    runs: &[LoadedRun],
    phase: &str,
    expected: Option<&crate::cli::PhasePlan>,
    frontends: usize,
) -> Option<String> {
    let planned = expected.is_some();
    let counted: Vec<&LoadedRun> = runs.iter().filter(|r| r.excluded.is_none()).collect();
    if !planned && !counted.iter().any(|run| run.phase(phase).is_some()) {
        return None;
    }
    let mut targets: Vec<f64> = Vec::new();
    let mut durations: Vec<f64> = Vec::new();
    let mut plan: Option<(&str, Vec<Value>)> = None;
    for run in counted {
        let id = &run.run.id;
        let Some(value) = run.phase(phase) else {
            return Some(format!(
                "{id} does not report the phase {}",
                if planned {
                    "the preset plans"
                } else {
                    "other runs ran"
                }
            ));
        };
        if let Some(expected) = expected {
            if let Some(why) = off_plan(id, value, expected, frontends) {
                return Some(format!(
                    "**the runs did not drive the pinned workload**: {why}"
                ));
            }
        }
        let Some(target) = value["target_rate_shares_per_second"].as_f64() else {
            return Some(format!("{id} reports no target rate"));
        };
        let Some(duration) = value["duration_seconds"].as_f64() else {
            return Some(format!("{id} reports no phase length"));
        };
        if !targets.contains(&target) {
            targets.push(target);
        }
        durations.push(duration);
        let fields: Vec<Value> = PHASE_PLAN_FIELDS
            .iter()
            .map(|key| value[*key].clone())
            .collect();
        match &plan {
            None => plan = Some((id, fields)),
            Some((first, expected)) => {
                if let Some((key, (a, b))) = PHASE_PLAN_FIELDS
                    .iter()
                    .zip(expected.iter().zip(&fields))
                    .find(|(_, (a, b))| !same_json(a, b))
                {
                    return Some(format!(
                        "**the runs did not drive one workload**: `{key}` is {a} in {first} and \
                         {b} in {id}"
                    ));
                }
            }
        }
    }
    if targets.len() > 1 {
        return Some(format!(
            "**the runs did not drive one workload**: target rates {}",
            targets
                .iter()
                .map(|t| number(*t, None))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let shortest = durations.iter().copied().fold(f64::INFINITY, f64::min);
    let longest = durations.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if !(shortest > 0.0 && longest <= shortest * (1.0 + DURATION_TOLERANCE)) {
        return Some(format!(
            "**the runs did not drive one workload**: phase lengths {}–{} s",
            number(shortest, None),
            number(longest, None)
        ));
    }
    None
}

/// Preset flags whose effect the side report states, and where: a counted
/// run must report the pinned value (every harness since #271 writes these).
pub const PINNED_REPORT_FIELDS: &[(&str, &str)] = &[
    ("--frontends", "/topology/frontends"),
    ("--sessions", "/topology/sessions"),
    (
        "--max-outstanding-per-session",
        "/topology/max_outstanding_per_session",
    ),
    ("--plan", "/topology/plan"),
    ("--window-shares", "/window/requested_window_shares"),
    ("--window-shares", "/window/computed_window_shares"),
    ("--replication", "/database/replication/declared"),
    ("--seed-share-bytes", "/window/seed/target_share_bytes"),
    (
        "--ack-p99-limit-ms",
        "/validator/ack_p99_limit_used_milliseconds",
    ),
    (
        "--forecast-peak-shares-per-second",
        "/validator/forecast_used",
    ),
];

/// Measured facts of the workload every counted run of both builds must
/// report alike, where the preset fixes them only through the harness: the
/// payout window's length as the database held it at the start (in every
/// harness since #271). A build that seeded a smaller ledger shows here even
/// when it echoes the pinned `--window-shares`.
pub const AGREED_REPORT_FIELDS: &[(&str, &str)] =
    &[("--window-shares", "/window/ledger_window_shares_at_start")];

/// Preset flags that set how many entries a report list holds, and the list:
/// a counted run must report exactly the pinned count (every harness since
/// #271 lists its external tips).
pub const PINNED_REPORT_COUNTS: &[(&str, &str)] =
    &[("--external-tips", "/time_to_usable_work/tips")];

/// Why a counted run's list at `pointer` does not hold `flag`'s pinned
/// number of entries, or `None` when every one does.
fn pinned_count_mismatch(
    runs: &[LoadedRun],
    flag: &str,
    pointer: &str,
    pinned: Option<&Value>,
) -> Option<String> {
    let wanted = pinned.and_then(Value::as_u64)?;
    for run in runs.iter().filter(|r| r.excluded.is_none()) {
        let count = run
            .report
            .as_ref()
            .and_then(|r| r.pointer(pointer))
            .and_then(Value::as_array)
            .map(Vec::len);
        match count {
            Some(count) if count as u64 == wanted => {}
            Some(count) => {
                return Some(format!(
                    "**the runs did not drive the pinned workload**: {} reports {count} entries \
                     at `{pointer}`, not the pinned `{flag}` {wanted}",
                    run.run.id
                ))
            }
            None => {
                return Some(format!(
                    "{} does not report `{pointer}`, so its `{flag}` cannot be checked",
                    run.run.id
                ))
            }
        }
    }
    None
}

/// Preset flags the harness passes to every frontend as a server setting,
/// and the variable: each frontend of each counted run must have been
/// launched with the pinned value (every harness since #271 records its
/// frontends' environment).
pub const PINNED_FRONTEND_ENV: &[(&str, &str)] = &[
    ("--runtime-workers", "PRISM_RUNTIME_WORKERS"),
    ("--db-max-connections", "PRISM_DATABASE_MAX_CONNECTIONS"),
    (
        "--share-commit-timeout-seconds",
        "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS",
    ),
    ("--blockpoll-seconds", "PRISM_BLOCKPOLL_SECONDS"),
    (
        "--stratum-max-pending-initial-jobs",
        "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS",
    ),
];

/// The churn flags: the `churn.parameters` block echoes them all.
const CHURN_FLAGS: &[&str] = &[
    "--churn-seconds",
    "--churn-tips",
    "--rental-bursts",
    "--rental-burst-window-seconds",
    "--rental-burst-interval-seconds",
    "--rental-lifetime",
    "--rental-hashrate",
    "--reconnect-storms",
    "--storm-interval-seconds",
    "--storm-reconnect-seconds",
    "--seed",
];

/// One report field a pinned realism or churn setting decides: the flags
/// behind it, where the report states it, and what it must read.
pub struct ExpectedSetting {
    pub flags: Vec<&'static str>,
    pub pointer: &'static str,
    pub value: Value,
}

/// The preset's flags as this harness parses them.
fn preset_args(pinned: &BTreeMap<String, Value>) -> Result<crate::cli::Args> {
    use clap::Parser;
    let words = crate::preset::argv_of("preset", pinned)?;
    let args = crate::cli::Args::try_parse_from(
        std::iter::once("qbit-prism-load".to_owned()).chain(words),
    )
    .context("parsing the preset's flags")?;
    // The harness's own entry checks (topology, rates, intervals,
    // replication, cadence), so a preset it would refuse is refused here.
    args.validate()
        .context("the harness would refuse the preset's flags")?;
    Ok(args)
}

/// The phases the preset plans, by this harness's own planner: every
/// counted run must report each of them, whatever the other runs did.
pub fn expected_phases(pinned: &BTreeMap<String, Value>) -> Result<Vec<crate::cli::PhasePlan>> {
    crate::cli::phases(&preset_args(pinned)?)
}

/// Why a counted run's report of `phase` does not match the preset's plan
/// for it, or `None` when it does: the target rate and whether the phase is
/// in the artifact exactly, the configured database delay (seen to be paid)
/// and the frontend restarts exactly (with `frontends` running), and the
/// length within [`DURATION_TOLERANCE`] of the planned seconds.
fn off_plan(
    id: &str,
    reported: &Value,
    plan: &crate::cli::PhasePlan,
    frontends: usize,
) -> Option<String> {
    let name = &plan.name;
    let Some(target) = reported["target_rate_shares_per_second"].as_f64() else {
        return Some(format!("{id} reports no target rate for `{name}`"));
    };
    if target != plan.rate {
        return Some(format!(
            "{id} ran `{name}` at {} shares/s, not the planned {}",
            number(target, None),
            number(plan.rate, None)
        ));
    }
    let seconds = plan.seconds as f64;
    let Some(duration) = reported["duration_seconds"].as_f64() else {
        return Some(format!("{id} reports no length for `{name}`"));
    };
    if (duration - seconds).abs() > seconds * DURATION_TOLERANCE {
        return Some(format!(
            "{id} ran `{name}` for {} s, not the planned {} s",
            number(duration, None),
            number(seconds, None)
        ));
    }
    if reported["in_artifact"].as_bool() != Some(plan.in_artifact) {
        return Some(format!(
            "{id} reports `{name}` with in_artifact {}, not the planned {}",
            reported["in_artifact"], plan.in_artifact
        ));
    }
    let delay = reported["database_delay_milliseconds_configured"].as_u64();
    if delay != Some(plan.database_delay_ms) {
        return Some(format!(
            "{id} ran `{name}` with a {} ms database delay, not the planned {} ms",
            delay.map_or("unreported".into(), |d| d.to_string()),
            plan.database_delay_ms
        ));
    }
    // A delayed phase's delay was seen to be paid before the phase, as the
    // harness itself requires (`run::check_delay_observed`, in every harness
    // since #271): a report that only states the configured delay proves
    // nothing about the database the phase ran against.
    if plan.database_delay_ms > 0 {
        let observed = reported["database_delay_observed_select1_median_milliseconds"].as_f64();
        match observed
            .map(|median| crate::run::check_delay_observed(plan.database_delay_ms, median))
        {
            Some(Ok(())) => {}
            Some(Err(error)) => {
                return Some(format!(
                    "{id} ran `{name}` without paying its delay: {error:#}"
                ))
            }
            None => {
                return Some(format!(
                    "{id} reports no observed database delay for `{name}`"
                ))
            }
        }
    }
    // A run that skipped the drained restart or the mid-flight kill ran an
    // easier phase, whatever the other runs did.
    let restarts = reported["frontend_restarts"].as_u64();
    let planned = plan.frontend_restarts(frontends);
    if restarts != Some(planned) {
        return Some(format!(
            "{id} restarted a frontend {} times in `{name}`, not the planned {planned}",
            restarts.map_or("an unreported number of".into(), |n| n.to_string())
        ));
    }
    None
}

/// What each realism and churn setting the preset pins must read as in a
/// side report, rendered by this harness's own parsers from the preset, so
/// a build that parsed a pinned value differently shows it.
pub fn expected_settings(pinned: &BTreeMap<String, Value>) -> Result<Vec<ExpectedSetting>> {
    let args = preset_args(pinned)?;
    let population = args.population_spec()?;
    let churn = args.churn_spec()?;
    let one = |flag: &'static str, pointer: &'static str, value: Value| ExpectedSetting {
        flags: vec![flag],
        pointer,
        value,
    };
    let mut expected = vec![
        one(
            "--arrival",
            "/arrival",
            Value::from(args.arrival()?.render()),
        ),
        one(
            "--recipients",
            "/population/recipients_flag",
            serde_json::json!(args.recipients),
        ),
        one(
            "--recipient-weights",
            "/population/recipient_weights",
            Value::from(population.weights.render()),
        ),
        one(
            "--session-hashrate-sigma",
            "/population/session_hashrate_sigma",
            serde_json::json!(population.hashrate_sigma),
        ),
        one(
            "--session-difficulty",
            "/population/session_difficulty",
            Value::from(population.difficulty.render()),
        ),
        one("--seed", "/population/seed", serde_json::json!(args.seed)),
        one(
            "--template-bits",
            "/node/template_bits",
            Value::from(format!("{:08x}", args.template_bits()?)),
        ),
        one(
            "--retarget-bits",
            "/node/retarget_bits",
            Value::from(args.retarget_bits),
        ),
        one(
            "--background-shares-per-second",
            "/node/background_shares_per_second",
            serde_json::json!(args.background_shares_per_second),
        ),
        one(
            "--pool-fee-bps",
            "/topology/pool_fee_bps",
            serde_json::json!(args.pool_fee_bps),
        ),
        one(
            "--mid-flight-kill",
            "/mid_flight_kill/ran",
            Value::Bool(args.mid_flight_kill),
        ),
    ];
    expected.push(if args.cadence()?.is_dense() {
        ExpectedSetting {
            flags: vec!["--cadence", "--cadence-gaps"],
            pointer: "/dense_cadence/gap_pattern_seconds",
            value: serde_json::json!(args.cadence_gaps()?),
        }
    } else {
        ExpectedSetting {
            flags: vec!["--cadence"],
            pointer: "/dense_cadence/ran",
            value: Value::Bool(false),
        }
    });
    if churn.is_on() {
        // The seeded plan the churn phase drives: rentals, bursts, storms
        // and tips, as `churn.plan` states it.
        expected.push(ExpectedSetting {
            flags: CHURN_FLAGS.to_vec(),
            pointer: "/churn/plan",
            value: churn.plan().summary(),
        });
    }
    expected.push(if churn.is_on() {
        ExpectedSetting {
            flags: CHURN_FLAGS.to_vec(),
            pointer: "/churn/parameters",
            value: serde_json::json!({
                "seconds": churn.seconds,
                "tips": churn.tips,
                "rental_bursts": churn.bursts.render(),
                "rental_burst_window_seconds": churn.burst_window_seconds,
                "rental_burst_interval_seconds": churn.burst_interval_seconds,
                "rental_lifetime": churn.lifetime.render(),
                "rental_hashrate": churn.rental_hashrate,
                "reconnect_storms": churn.storms,
                "storm_interval_seconds": churn.storm_interval_seconds,
                "storm_reconnect_seconds": churn.storm_reconnect_seconds,
                "seed": churn.seed,
            }),
        }
    } else {
        ExpectedSetting {
            flags: CHURN_FLAGS.to_vec(),
            pointer: "/churn/ran",
            value: Value::Bool(false),
        }
    });
    Ok(expected)
}

/// Whether `run`'s build ran without `flag`: its harness predates the flag,
/// and the driver left it off only because `legacy-flags.json` showed the
/// build ran the pinned value anyway.
fn predates(manifest: &Manifest, run: &LoadedRun, flag: &str) -> bool {
    manifest
        .builds
        .iter()
        .find(|b| b.label == run.run.build)
        .is_some_and(|b| b.dropped_legacy_flags.iter().any(|f| f == flag))
}

/// Why a counted run's churn phase did not carry out the preset's plan, or
/// `None` when every one did: every planned rental spawned and every one
/// planned to leave in the phase departed (a departure is recorded at its
/// planned time whatever a storm did to the rental first), every planned
/// storm dropped its sessions, and every planned churn tip was delivered
/// (`churn.realised` and `churn.tip_delivery`). A plan is only a promise;
/// the latency gates alone would pass on one rental and one tip.
fn churn_unrealised(
    manifest: &Manifest,
    runs: &[LoadedRun],
    pinned: &BTreeMap<String, Value>,
) -> Result<Option<String>> {
    let spec = preset_args(pinned)?.churn_spec()?;
    if !spec.is_on() {
        return Ok(None);
    }
    let plan = spec.plan();
    for run in runs.iter().filter(|r| r.excluded.is_none()) {
        if predates(manifest, run, "--churn-seconds") {
            continue;
        }
        let churn = run.report.as_ref().map(|r| &r["churn"]);
        let count = |pointer: &str| {
            churn
                .and_then(|c| c.pointer(pointer))
                .and_then(|v| v.as_u64().or_else(|| v.as_array().map(|a| a.len() as u64)))
        };
        for (what, pointer, planned) in [
            (
                "rentals spawned",
                "/realised/rentals_spawned",
                plan.rentals.len(),
            ),
            (
                "rentals departed",
                "/realised/rentals_departed",
                plan.rentals
                    .iter()
                    .filter(|r| r.depart_at.is_some())
                    .count(),
            ),
            ("storms", "/realised/storms", plan.storms.len()),
            (
                "churn tips delivered",
                "/tip_delivery/tips",
                plan.tips.len(),
            ),
        ] {
            let realised = count(pointer);
            if realised != Some(planned as u64) {
                return Ok(Some(format!(
                    "**the runs did not drive the pinned workload**: {} reports {} {what}, \
                     not the {planned} the preset's churn plan holds",
                    run.run.id,
                    realised.map_or("no count of".into(), |n| n.to_string())
                )));
            }
        }
        // The stormed sessions came back: every one reconnects after its
        // delay, except a stormed rental whose planned departure came
        // first, so the completed reconnects are at least the drops less
        // the departures.
        let dropped: u64 = churn
            .and_then(|c| c.pointer("/realised/storms"))
            .and_then(Value::as_array)
            .map_or(0, |storms| {
                storms.iter().filter_map(|s| s["dropped"].as_u64()).sum()
            });
        let departed = count("/realised/rentals_departed").unwrap_or(0);
        let reconnected = count("/realised/reconnects_completed");
        if !reconnected.is_some_and(|n| n + departed >= dropped) {
            return Ok(Some(format!(
                "**the runs did not drive the pinned workload**: {} completed {} reconnects \
                 after its storms dropped {dropped} sessions ({departed} rentals departed)",
                run.run.id,
                reconnected.map_or("an unreported number of".into(), |n| n.to_string())
            )));
        }
        // Each storm dropped what its fraction of the sessions then connected
        // asks for (`round(fraction * connected)`, as the driver computes
        // it), and dropped someone: a storm record that dropped no session
        // disrupted nothing.
        let storms = churn
            .and_then(|c| c.pointer("/realised/storms"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for (index, (storm, planned)) in storms.iter().zip(&plan.storms).enumerate() {
            let fraction = storm["fraction"].as_f64();
            let connected = storm["connected"].as_u64();
            let dropped = storm["dropped"].as_u64();
            let expected = connected.map(|c| ((planned.fraction * c as f64).round() as u64).min(c));
            if fraction != Some(planned.fraction)
                || dropped.is_none()
                || dropped != expected
                || dropped == Some(0)
            {
                return Ok(Some(format!(
                    "**the runs did not drive the pinned workload**: {} storm {index} dropped \
                     {} of {} connected sessions at fraction {}, not the plan's {} of them",
                    run.run.id,
                    dropped.map_or("an unreported number".into(), |d| d.to_string()),
                    connected.map_or("an unreported number of".into(), |c| c.to_string()),
                    fraction.map_or("unreported".into(), |f| f.to_string()),
                    planned.fraction
                )));
            }
        }
    }
    Ok(None)
}

/// JSON equality with numbers compared as numbers, at any depth.
pub fn same_json(a: &Value, b: &Value) -> bool {
    match (a, b) {
        // Integers exactly (a seed above 2^53 has neighbours an f64 cannot
        // tell apart); a mixed or fractional pair as numbers (0 == 0.0).
        (Value::Number(x), Value::Number(y)) => {
            if let (Some(p), Some(q)) = (x.as_u64(), y.as_u64()) {
                p == q
            } else if let (Some(p), Some(q)) = (x.as_i64(), y.as_i64()) {
                p == q
            } else {
                x.as_f64() == y.as_f64()
            }
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| same_json(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(key, a)| y.get(key).is_some_and(|b| same_json(a, b)))
        }
        _ => a == b,
    }
}

/// Every counted run's report against [`expected_settings`]. A build whose
/// harness predates a setting's flag is exempt from it: the driver left the
/// flag off that build's command line only because `legacy-flags.json`
/// showed the build ran the pinned value anyway.
fn setting_mismatches(
    manifest: &Manifest,
    runs: &[LoadedRun],
    expected: &[ExpectedSetting],
) -> Vec<String> {
    let mut found = Vec::new();
    for setting in expected {
        for run in runs.iter().filter(|r| r.excluded.is_none()) {
            if setting
                .flags
                .iter()
                .any(|flag| predates(manifest, run, flag))
            {
                continue;
            }
            let reported = run.report.as_ref().and_then(|r| r.pointer(setting.pointer));
            let why = match reported {
                None => format!(
                    "{} does not report `{}`, so its `{}` cannot be checked",
                    run.run.id, setting.pointer, setting.flags[0]
                ),
                Some(value) if same_json(value, &setting.value) => continue,
                Some(value) => format!(
                    "**the runs did not drive the pinned workload**: {} reports `{}` {value}, \
                     not the preset's {}",
                    run.run.id, setting.pointer, setting.value
                ),
            };
            found.push(why);
            break;
        }
    }
    found
}

/// Why some frontend of a counted run was not launched with `flag`'s pinned
/// value as `key`, or `None` when every one was.
fn frontend_env_mismatch(
    runs: &[LoadedRun],
    flag: &str,
    key: &str,
    pinned: Option<&Value>,
) -> Option<String> {
    let pinned = pinned.filter(|v| !v.is_null())?;
    // The number as JSON wrote it: through an f64 an integer above 2^53
    // would already be its neighbour.
    let wanted = match pinned {
        Value::Number(n) => n.to_string(),
        other => other.as_str().unwrap_or_default().to_owned(),
    };
    let same = |value: &str| match (value.parse::<i128>(), wanted.parse::<i128>()) {
        (Ok(a), Ok(b)) => a == b,
        _ => match (value.parse::<f64>(), wanted.parse::<f64>()) {
            (Ok(a), Ok(b)) => a == b,
            _ => value == wanted,
        },
    };
    for run in runs.iter().filter(|r| r.excluded.is_none()) {
        let frontends = run
            .report
            .as_ref()
            .and_then(|r| r["frontend_environment"].as_array())
            .filter(|f| !f.is_empty());
        let Some(frontends) = frontends else {
            return Some(format!(
                "{} reports no frontend environment, so its `{flag}` cannot be checked",
                run.run.id
            ));
        };
        for frontend in frontends {
            match frontend["environment"][key].as_str() {
                Some(value) if same(value) => {}
                Some(value) => {
                    return Some(format!(
                        "**the runs did not drive the pinned workload**: {} launched a frontend \
                         with `{key}={value}`, not the pinned `{flag}` {wanted}",
                        run.run.id
                    ))
                }
                None => {
                    return Some(format!(
                        "{} launched a frontend without `{key}`, so its `{flag}` cannot be checked",
                        run.run.id
                    ))
                }
            }
        }
    }
    None
}

/// Why some frontend of a counted run did not run the pinned pool fee, or
/// `None` when every one did (#535): `topology.pool_fee_bps` only echoes the
/// flag, while each frontend's environment is the fee the server applied,
/// read as the server reads it (`PRISM_POOL_FEE_ENABLED` a boolean, off when
/// unset; `PRISM_POOL_FEE_BPS` 0 when unset). A build whose harness predates the
/// flag ran fee-off, which `legacy-flags.json` accepts for a pinned 0, and so
/// does a fee-off frontend at a pinned 0: from #536 to #568 the harness left
/// the fee off at 0 rather than enabling it at 0 bps.
fn pool_fee_mismatch(
    manifest: &Manifest,
    runs: &[LoadedRun],
    pinned: Option<&Value>,
) -> Option<String> {
    let bps = pinned.and_then(Value::as_u64)?;
    for run in runs.iter().filter(|r| r.excluded.is_none()) {
        if predates(manifest, run, "--pool-fee-bps") {
            continue;
        }
        let frontends = run
            .report
            .as_ref()
            .and_then(|r| r["frontend_environment"].as_array())
            .filter(|f| !f.is_empty());
        let Some(frontends) = frontends else {
            return Some(format!(
                "{} reports no frontend environment, so its `--pool-fee-bps` cannot be checked",
                run.run.id
            ));
        };
        for frontend in frontends {
            // A blank value is unset, as the server's `config::optional` reads it.
            let value = |key: &str| {
                frontend["environment"][key]
                    .as_str()
                    .filter(|value| !value.trim().is_empty())
            };
            let enabled = value("PRISM_POOL_FEE_ENABLED").is_some_and(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            });
            let applied = match value("PRISM_POOL_FEE_BPS") {
                None => Some(0),
                Some(value) => value.parse::<u16>().ok(),
            };
            if (enabled && applied.map(u64::from) != Some(bps)) || (!enabled && bps != 0) {
                return Some(format!(
                    "**the runs did not drive the pinned workload**: {} launched a frontend \
                     with the pool fee {}, not the pinned `--pool-fee-bps` {bps}",
                    run.run.id,
                    match (enabled, applied) {
                        (false, _) => "off".to_owned(),
                        (true, Some(applied)) => format!("at {applied} bps"),
                        (true, None) => "at an unreadable rate".to_owned(),
                    }
                ));
            }
        }
    }
    None
}

/// Why the counted runs did not all report `flag`'s pinned value at
/// `pointer`, or `None` when they did. With no pinned value the runs must
/// agree with one another.
fn pinned_mismatch(
    runs: &[LoadedRun],
    flag: &str,
    pointer: &str,
    pinned: Option<&Value>,
) -> Option<String> {
    let mut expected: Option<Value> = pinned.filter(|v| !v.is_null()).cloned();
    for run in runs.iter().filter(|r| r.excluded.is_none()) {
        let reported = run.report.as_ref().and_then(|r| r.pointer(pointer));
        let Some(reported) = reported.filter(|v| !v.is_null()) else {
            return Some(format!(
                "{} does not report `{pointer}`, so its `{flag}` cannot be checked",
                run.run.id
            ));
        };
        match &expected {
            Some(value) if !same_json(value, reported) => {
                return Some(format!(
                    "**the runs did not drive the pinned workload**: {} ran `{flag}` {reported}, \
                     not {value}",
                    run.run.id
                ))
            }
            Some(_) => {}
            None => expected = Some(reported.clone()),
        }
    }
    None
}

/// Summarize `manifest`'s runs against the preset's `budgets`: #473's D1
/// rule in the D1 phases the preset gates, and every one of the preset's
/// own gates on every run.
///
/// `pinned` is the preset's `args`: every counted run must report the
/// topology and the ACK p99 limit they pin ([`PINNED_REPORT_FIELDS`]) and
/// have launched its frontends with the pinned server settings
/// ([`PINNED_FRONTEND_ENV`]), so no build passes on a smaller workload, a
/// different server configuration or a looser limit of its own.
pub fn compare(
    manifest: &Manifest,
    runs: &[LoadedRun],
    budgets: &Budgets,
    pinned: &BTreeMap<String, Value>,
) -> Result<Comparison> {
    let gated = budgets.phases.as_deref();
    let build = |label: &str| {
        manifest
            .builds
            .iter()
            .find(|b| b.label == label)
            .expect("read_manifest checked both builds")
    };
    let (base, candidate) = (build("base"), build("candidate"));
    let gated_phases: Vec<&str> = D1_PHASES
        .iter()
        .copied()
        .filter(|phase| match gated {
            Some(names) => names.iter().any(|n| n == phase),
            // `null` gates every phase the runs drove, as the gate reads it.
            None => runs.iter().any(|run| run.phase(phase).is_some()),
        })
        .collect();
    ensure!(
        !gated_phases.is_empty(),
        "preset {} gates none of the D1 phases ({}); the comparison has nothing to hold the \
         candidate to",
        manifest.preset.name,
        D1_PHASES.join(", ")
    );
    let mut out = format!(
        "## A/B `{}`: base `{}` (`{}`) against candidate `{}` (`{}`)\n\n",
        manifest.preset.name,
        base.reference,
        &base.commit[..base.commit.len().min(8)],
        candidate.reference,
        &candidate.commit[..candidate.commit.len().min(8)],
    );
    out.push_str(&format!(
        "- Host: {}\n{}- Preset SHA-256: `{}`\n",
        host_line(manifest),
        flush_lines(manifest),
        manifest.preset.sha256
    ));
    for b in [base, candidate] {
        if !b.dropped_legacy_flags.is_empty() {
            out.push_str(&format!(
                "- The {} build's harness predates {}; each was left off as \
                 `crates/qbit-prism-load/legacy-flags.json` allows\n",
                b.label,
                b.dropped_legacy_flags
                    .iter()
                    .map(|f| format!("`{f}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    out.push('\n');
    out.push_str(&run_table(runs));
    let mut passed = true;
    let mut findings = Vec::new();
    for phase in D1_PHASES {
        let rows: Vec<Row> = [base, candidate]
            .iter()
            .map(|b| {
                let mine: Vec<&LoadedRun> =
                    runs.iter().filter(|r| r.run.build == b.label).collect();
                row(b, &mine, phase)
            })
            .collect();
        let present = runs.iter().any(|r| r.phase(phase).is_some());
        let is_gated = gated_phases.contains(phase);
        if !present && !is_gated {
            continue;
        }
        out.push_str(&format!(
            "\n### `{phase}`{}\n\n| {} |\n|{}\n",
            if is_gated {
                " (gated)"
            } else {
                " (reported, not gated)"
            },
            COLUMNS.join(" | "),
            "---|".repeat(COLUMNS.len())
        ));
        for row in &rows {
            let cells: Vec<String> = row.cells.iter().map(|c| c.replace('|', "\\|")).collect();
            out.push_str(&format!("| {} |\n", cells.join(" | ")));
        }
        let (base_row, candidate_row) = (&rows[0], &rows[1]);
        if let (Some(b), Some(c)) = (base_row.achieved_median, candidate_row.achieved_median) {
            out.push_str(&format!(
                "\nCandidate against base: achieved median {:+.1} shares/s",
                c - b
            ));
            if let (Some(bp), Some(cp)) = (base_row.ack_p99_median, candidate_row.ack_p99_median) {
                out.push_str(&format!(", ACK p99 median {:+.1} ms", cp - bp));
            }
            out.push_str(".\n");
        }
        if is_gated {
            if !candidate_row.met {
                passed = false;
                findings.push(if base_row.met {
                    format!("`{phase}`: **regression**, the base meets the D1 rule and the candidate does not")
                } else {
                    format!("`{phase}`: the candidate does not meet the D1 rule (nor does the base)")
                });
            } else if !base_row.met {
                findings.push(format!(
                    "`{phase}`: the candidate meets the D1 rule the base does not"
                ));
            }
        }
    }
    // The same preset must mean the same workload in every phase any run
    // drove, D1 or not: a build that reads a pinned rate differently could
    // pass its own gates on less load.
    // With no base run in the medians there is nothing to compare against:
    // the candidate could only be held to itself.
    if !runs
        .iter()
        .any(|r| r.run.build == "base" && r.excluded.is_none())
    {
        passed = false;
        findings.push(
            "**no base run is in the medians**, so the candidate has no baseline; rerun the series"
                .to_owned(),
        );
    }
    let args = preset_args(pinned)?;
    let planned = crate::cli::phases(&args)?;
    let mut all_phases: Vec<String> = planned.iter().map(|plan| plan.name.clone()).collect();
    for run in runs.iter().filter(|r| r.excluded.is_none()) {
        let names = run.report.as_ref().and_then(|r| r["phases"].as_array());
        for name in names
            .into_iter()
            .flatten()
            .filter_map(|p| p["name"].as_str())
        {
            if !all_phases.iter().any(|known| known == name) {
                all_phases.push(name.to_owned());
            }
        }
    }
    for phase in &all_phases {
        let plan = planned.iter().find(|plan| &plan.name == phase);
        if let Some(why) = phase_workload(runs, phase, plan, args.frontends) {
            passed = false;
            findings.push(format!("`{phase}`: {why}"));
        }
    }
    for (flag, pointer) in PINNED_REPORT_FIELDS {
        if let Some(why) = pinned_mismatch(runs, flag, pointer, pinned.get(*flag)) {
            passed = false;
            findings.push(why);
        }
    }
    // The database the run used: managed unless the preset names one, with
    // the observed replication agreeing with the declared one at entry and
    // after the load, each observation reported (the premise's `agreed`
    // skips one that was never made), and one launched frontend per pinned
    // frontend (every harness since #271 reports these).
    let mode = if pinned.get("--database-url").is_some_and(|v| !v.is_null()) {
        "external"
    } else {
        "managed"
    };
    let frontends = pinned.get("--frontends").and_then(Value::as_u64);
    for run in runs.iter().filter(|r| r.excluded.is_none()) {
        let report = run.report.as_ref();
        let reported_mode = report
            .and_then(|r| r.pointer("/database/mode"))
            .and_then(Value::as_str);
        let agreed = report
            .and_then(|r| r.pointer("/database/replication/agreed_with_declared"))
            .and_then(Value::as_bool);
        let replication = |key: &str| {
            report
                .and_then(|r| r.pointer("/database/replication"))
                .and_then(|block| block[key].as_str())
        };
        let declared = replication("declared");
        let launched = report
            .and_then(|r| r["frontend_environment"].as_array())
            .map(|f| f.len() as u64);
        let why = if reported_mode != Some(mode) {
            Some(format!(
                "ran against a {} database, not the {mode} one the preset asks for",
                reported_mode.unwrap_or("unreported")
            ))
        } else if agreed != Some(true) {
            Some("does not report its observed replication agreeing with the declared one".into())
        } else if declared.is_none()
            || replication("observed") != declared
            || replication("observed_after_load") != declared
        {
            Some(format!(
                "observed replication {} at entry and {} after the load, not the declared {}",
                replication("observed").unwrap_or("unreported"),
                replication("observed_after_load").unwrap_or("unreported"),
                declared.unwrap_or("unreported")
            ))
        } else if frontends.is_some() && launched != frontends {
            Some(format!(
                "launched {} frontends, not the pinned `--frontends` {}",
                launched.map_or("an unreported number of".into(), |n| n.to_string()),
                frontends.unwrap_or_default()
            ))
        } else {
            None
        };
        if let Some(why) = why {
            passed = false;
            findings.push(format!(
                "**the runs did not drive the pinned workload**: {} {why}",
                run.run.id
            ));
            break;
        }
    }
    // Every pinned session connected: the harness starts no phase until each
    // of `--sessions` holds work, and `client.connects` counts each of those
    // connections, which reconnects and rentals only add to (every harness
    // since #271). `topology.sessions` alone only echoes the flag.
    if let Some(sessions) = pinned.get("--sessions").and_then(Value::as_u64) {
        for run in runs.iter().filter(|r| r.excluded.is_none()) {
            let connects = run
                .report
                .as_ref()
                .and_then(|r| r.pointer("/client/connects"))
                .and_then(Value::as_u64);
            if !connects.is_some_and(|n| n >= sessions) {
                passed = false;
                findings.push(format!(
                    "**the runs did not drive the pinned workload**: {} made {} connections, \
                     fewer than the pinned `--sessions` {sessions}",
                    run.run.id,
                    connects.map_or("an unreported number of".into(), |n| n.to_string())
                ));
                break;
            }
        }
    }
    // The samplers ran at the pinned intervals, on every frontend: each
    // phase reports the ORDER-lock sampler's interval and one process record
    // per launched frontend (`frontend_environment`), each at the pinned
    // interval and each having sampled, with no reason it could not (every
    // harness since #271 reports all of it in every phase). A sampler that
    // stopped would spare the run its queries and publish no evidence.
    let lock_ms = pinned
        .get("--lock-sample-interval-ms")
        .and_then(Value::as_f64);
    let process_ms = pinned
        .get("--process-sample-interval-ms")
        .and_then(Value::as_f64);
    let close = |a: f64, b: f64| (a - b).abs() < 1e-6;
    let instance_ids = |list: &[Value]| -> Vec<String> {
        let mut ids: Vec<String> = list
            .iter()
            .map(|entry| entry["instance_id"].as_str().unwrap_or("?").to_owned())
            .collect();
        ids.sort_unstable();
        ids
    };
    let named = |ids: &[String]| {
        if ids.is_empty() {
            "none".to_owned()
        } else {
            ids.join(", ")
        }
    };
    'runs: for run in runs.iter().filter(|r| r.excluded.is_none()) {
        let report = run.report.as_ref();
        let launched = report
            .and_then(|r| r["frontend_environment"].as_array())
            .map_or_else(Vec::new, |list| instance_ids(list));
        let phases = report
            .and_then(|r| r["phases"].as_array())
            .cloned()
            .unwrap_or_default();
        for phase in &phases {
            let name = phase["name"].as_str().unwrap_or("?");
            let lock = phase["order_lock"]["sample_interval_milliseconds"].as_f64();
            let processes = phase["processes"]
                .as_array()
                .filter(|list| !list.is_empty());
            let lock_off = match (lock, lock_ms) {
                (Some(reported), Some(pinned)) if !close(reported, pinned) => {
                    Some((reported, pinned))
                }
                _ => None,
            };
            let process_off = process_ms.filter(|&pinned| {
                processes.into_iter().flatten().any(|process| {
                    !process["sample_interval_seconds"]
                        .as_f64()
                        .is_some_and(|seconds| close(seconds * 1000.0, pinned))
                })
            });
            let sampled = processes.map(|list| instance_ids(list));
            let idle = |summary: &Value| {
                summary["samples"].as_u64().is_none_or(|n| n == 0)
                    || !summary["unavailable_reason"].is_null()
            };
            let why = if lock.is_none() || sampled.is_none() {
                Some(format!("reports no sampler intervals for `{name}`"))
            } else if idle(&phase["order_lock"]) || processes.into_iter().flatten().any(idle) {
                Some(format!(
                    "reports a sampler that took no samples in `{name}`, or says why it could not"
                ))
            } else if sampled.as_ref() != Some(&launched) {
                Some(format!(
                    "sampled {} in `{name}`, not its launched frontends {}",
                    named(sampled.as_deref().unwrap_or_default()),
                    named(&launched)
                ))
            } else if let Some((reported, pinned)) = lock_off {
                Some(format!(
                    "sampled the ORDER lock every {reported} ms in `{name}`, not the pinned \
                     `--lock-sample-interval-ms` {pinned}"
                ))
            } else {
                process_off.map(|pinned| {
                    format!(
                        "sampled its frontends at another interval than the pinned \
                         `--process-sample-interval-ms` {pinned} in `{name}`"
                    )
                })
            };
            if let Some(why) = why {
                passed = false;
                findings.push(format!(
                    "**the runs did not drive the pinned workload**: {} {why}",
                    run.run.id
                ));
                break 'runs;
            }
        }
    }
    // The degraded-database phase's delay is the pinned one.
    if let Some(pinned_delay) = pinned.get("--slow-db-delay-ms").and_then(Value::as_f64) {
        for run in runs.iter().filter(|r| r.excluded.is_none()) {
            let Some(phase) = run.phase("slow_database") else {
                continue;
            };
            let configured = phase["database_delay_milliseconds_configured"].as_f64();
            if configured != Some(pinned_delay) {
                passed = false;
                findings.push(format!(
                    "**the runs did not drive the pinned workload**: {} ran `slow_database` \
                     with a {} ms delay, not the pinned `--slow-db-delay-ms` {}",
                    run.run.id,
                    configured.map_or("unreported".into(), |c| number(c, None)),
                    number(pinned_delay, None)
                ));
                break;
            }
        }
    }
    // Every scheduled own block fires in `steady_state`, so the phases'
    // total is the pinned `--scheduled-blocks`, and each must have landed:
    // the node accepted it (`node.submissions`, which only scheduled blocks
    // reach), or the gated phase went without the payout-revision rebuild
    // it carries (in every harness since #271). Under the dense cadence the
    // flag is instead the dense phase's landing budget: the report states
    // it, and the phase schedules one landing on each slot the gap pattern
    // places while the budget lasts. How those landings fare is that side
    // phase's measurement, after the gated phases, so it is not held here.
    if let Some(blocks) = pinned.get("--scheduled-blocks").and_then(Value::as_u64) {
        let gaps = args.cadence_gaps()?;
        let slots = planned
            .iter()
            .find(|plan| plan.dense_cadence)
            .map(|plan| crate::cadence::landing_offsets(&gaps, plan.seconds as f64).len() as u64);
        let expected = slots.map_or(blocks, |slots| blocks.min(slots));
        for run in runs.iter().filter(|r| r.excluded.is_none()) {
            let report = run.report.as_ref();
            let scheduled: Option<u64> = report
                .and_then(|r| r["phases"].as_array())
                .and_then(|phases| phases.iter().map(|p| p["scheduled_blocks"].as_u64()).sum());
            let budget = report
                .and_then(|r| r.pointer("/dense_cadence/landing_budget"))
                .and_then(Value::as_u64);
            let accepted = report
                .and_then(|r| r.pointer("/node/submissions"))
                .and_then(Value::as_array)
                .map(|list| list.iter().filter(|s| s["accepted"] == true).count() as u64);
            let count =
                |n: Option<u64>| n.map_or("an unreported number of".into(), |n| n.to_string());
            let why = match slots {
                Some(_) if budget != Some(blocks) => Some(format!(
                    "ran a dense-cadence landing budget of {}, not the pinned \
                     `--scheduled-blocks` {blocks}",
                    budget.map_or("unreported".into(), |n| n.to_string())
                )),
                Some(slots) if scheduled != Some(expected) => Some(format!(
                    "scheduled {} dense-cadence landings, not the {expected} that the pinned \
                     `--scheduled-blocks` {blocks} buys of the gap pattern's {slots} slots",
                    count(scheduled)
                )),
                None if scheduled != Some(expected) => Some(format!(
                    "scheduled {} own blocks, not the pinned `--scheduled-blocks` {blocks}",
                    count(scheduled)
                )),
                None if blocks > 0 && accepted != Some(expected) => Some(format!(
                    "landed {} of its {blocks} scheduled own blocks (`node.submissions` \
                     accepted), not the pinned `--scheduled-blocks` {blocks}",
                    count(accepted)
                )),
                _ => None,
            };
            if let Some(why) = why {
                passed = false;
                findings.push(format!(
                    "**the runs did not drive the pinned workload**: {} {why}",
                    run.run.id
                ));
                break;
            }
        }
    }
    // The mid-flight kill caught submits in flight: the harness waits for
    // them before it kills, and reports 0 when none came, which the report
    // itself says means the scenario did not exercise (every harness since
    // #271).
    if args.mid_flight_kill {
        for run in runs.iter().filter(|r| r.excluded.is_none()) {
            let outstanding = run
                .report
                .as_ref()
                .and_then(|r| r.pointer("/mid_flight_kill/submits_outstanding_at_kill"))
                .and_then(Value::as_u64);
            if !outstanding.is_some_and(|n| n > 0) {
                passed = false;
                findings.push(format!(
                    "**the runs did not drive the pinned workload**: {} killed its frontend with \
                     {} submits outstanding, so the pinned `--mid-flight-kill` did not exercise",
                    run.run.id,
                    outstanding.map_or("an unreported number of".into(), |n| n.to_string())
                ));
                break;
            }
        }
    }
    // The memory floor the preset pins stops a run below it, so a counted
    // run's lowest reading must be at or above it; an unread minimum cannot
    // show that (`phases[].min_mem_available_kib`, in every harness since #271).
    if let Some(floor_mib) = pinned
        .get("--min-mem-available-mib")
        .and_then(Value::as_u64)
    {
        for run in runs.iter().filter(|r| r.excluded.is_none()) {
            let lowest: Option<u64> = run
                .report
                .as_ref()
                .and_then(|r| r["phases"].as_array())
                .and_then(|phases| {
                    phases
                        .iter()
                        .map(|p| p["min_mem_available_kib"].as_u64())
                        .collect::<Option<Vec<u64>>>()
                })
                .and_then(|lows| lows.into_iter().min());
            if !lowest.is_some_and(|kib| kib >= floor_mib * 1024) {
                passed = false;
                findings.push(format!(
                    "**the runs did not hold the pinned memory floor**: {} reports a lowest \
                     MemAvailable of {}, under the pinned `--min-mem-available-mib` {floor_mib}",
                    run.run.id,
                    lowest.map_or("nothing it read".into(), |kib| format!(
                        "{} MiB",
                        kib / 1024
                    ))
                ));
                break;
            }
        }
    }
    // The reconnect phase drives the pinned number of completed reconnects
    // (`reconnects.by_phase`, in every harness since #271).
    if let Some(target) = pinned.get("--reconnect-target").and_then(Value::as_u64) {
        for run in runs.iter().filter(|r| r.excluded.is_none()) {
            if run.phase("reconnect").is_none() {
                continue;
            }
            let completed = run
                .report
                .as_ref()
                .and_then(|r| r["reconnects"]["by_phase"].as_array())
                .and_then(|rows| rows.iter().find(|row| row["phase"] == "reconnect"))
                .and_then(|row| row["completed"].as_u64());
            if completed.is_none_or(|n| n < target) {
                passed = false;
                findings.push(format!(
                    "**the runs did not drive the pinned workload**: {} completed {} reconnects \
                     in `reconnect`, fewer than the pinned `--reconnect-target` {target}",
                    run.run.id,
                    completed.map_or("an unreported number of".into(), |n| n.to_string())
                ));
                break;
            }
        }
    }
    for (flag, pointer) in PINNED_REPORT_COUNTS {
        if let Some(why) = pinned_count_mismatch(runs, flag, pointer, pinned.get(*flag)) {
            passed = false;
            findings.push(why);
        }
    }
    // `--node` (#547): the real node's block carries its chain
    // reconciliation and the fake node's does not, so each counted run's
    // block shows which node it drove. A build whose harness predates the
    // flag ran the fake node (legacy-flags.json) and is exempt.
    if let Some(mode) = pinned.get("--node").and_then(Value::as_str) {
        let real = mode == "qbitd";
        for run in runs.iter().filter(|r| r.excluded.is_none()) {
            if predates(manifest, run, "--node") {
                continue;
            }
            let node = run.report.as_ref().and_then(|r| r.get("node"));
            let drove_real = node.map(|n| n.get("chain_reconciliation").is_some());
            if drove_real != Some(real) {
                passed = false;
                findings.push(format!(
                    "**the runs did not drive the pinned workload**: {} drove {}, not the \
                     pinned `--node` {mode}",
                    run.run.id,
                    match drove_real {
                        None => "no reported node",
                        Some(true) => "a real qbitd",
                        Some(false) => "the fake node",
                    }
                ));
                break;
            }
        }
    }
    if let Some(why) = churn_unrealised(manifest, runs, pinned)? {
        passed = false;
        findings.push(why);
    }
    for why in setting_mismatches(manifest, runs, &expected_settings(pinned)?) {
        passed = false;
        findings.push(why);
    }
    for (flag, pointer) in AGREED_REPORT_FIELDS {
        if let Some(why) = pinned_mismatch(runs, flag, pointer, None) {
            passed = false;
            findings.push(why);
        }
    }
    for (flag, key) in PINNED_FRONTEND_ENV {
        if let Some(why) = frontend_env_mismatch(runs, flag, key, pinned.get(*flag)) {
            passed = false;
            findings.push(why);
        }
    }
    if let Some(why) = pool_fee_mismatch(manifest, runs, pinned.get("--pool-fee-bps")) {
        passed = false;
        findings.push(why);
    }
    let (gate_table, gates_ok) = preset_gate_rows([base, candidate], runs, budgets);
    out.push_str(&gate_table);
    if !gates_ok[1] {
        passed = false;
        findings.push(if gates_ok[0] {
            "the preset's own gates: **regression**, every base run passes them and a candidate \
             run does not"
                .to_owned()
        } else {
            "the preset's own gates: a candidate run fails them (as does a base run)".to_owned()
        });
    }
    out.push_str(&format!(
        "\n### Verdict: {}\n\nThe candidate is held to #473's D1 rule in {}: every repeat \
         exits 0, with `shortfall == 0`, no valid share refused, no submit unanswered, and ACK \
         p99 within the limit. Every candidate run must also pass the preset's own gates, and \
         both builds must report the same target rates.\n",
        if passed { "PASS" } else { "FAIL" },
        gated_phases
            .iter()
            .map(|p| format!("`{p}`"))
            .collect::<Vec<_>>()
            .join(" and ")
    ));
    for finding in findings {
        out.push_str(&format!("- {finding}\n"));
    }
    Ok(Comparison {
        markdown: out,
        passed,
    })
}

/// One line per run, in the order they ran, with the host's load around it.
fn run_table(runs: &[LoadedRun]) -> String {
    let mut out = String::from(
        "| run | build | exit | load before / max during | lowest MemAvailable MiB | \
         steady achieved /s | burst achieved /s |\n|---|---|---|---|---|---|---|\n",
    );
    for run in runs {
        let achieved = |phase: &str| {
            run.phase(phase)
                .and_then(|p| {
                    p["achieved_rate_shares_per_second"]
                        .as_f64()
                        .map(|a| number(a, p["target_rate_shares_per_second"].as_f64()))
                })
                .unwrap_or_else(|| "n/a".into())
        };
        let load = |v: Option<f64>| v.map_or("n/a".into(), |l| format!("{l:.2}"));
        out.push_str(&format!(
            "| {} | {} | {} | {} / {} | {} | {} | {} |\n",
            run.run.id,
            run.run.build,
            if run.run.ceiling_hit {
                "ceiling".to_owned()
            } else {
                run.run.exit_code.map_or("n/a".into(), |c| c.to_string())
            },
            load(run.run.load_before),
            load(run.run.load_max),
            run.run
                .mem_available_min_mib
                .map_or("n/a".into(), |m| number(m as f64, None)),
            achieved("steady_state"),
            achieved("burst"),
        ));
    }
    out
}
