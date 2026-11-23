//! L3 collation (#550): one build's runs of a preset suite, rendered as the
//! tables of #473's production-window document.
//!
//! `.github/workflows/prism-load-l3.yml` plans a suite from
//! `crates/qbit-prism-load/presets/suites.toml`, runs each preset repeat in
//! its own job on one pinned commit, and uploads each job's output as the
//! artifact `prism-l3-run-<preset>-r<repeat>`. This module reads the plan and
//! those directories and prints, per D1 phase, one row per preset with the
//! A/B summarizer's own cells ([`crate::compare::row`] and
//! [`crate::compare::COLUMNS`]), beside the configuration columns #473's
//! tables lead with, and every run held to its preset's own gates.
//!
//! A run counts only when its artifact is there, its side report names the
//! plan's commit, it ran the checked-in preset byte for byte (SHA-256) and
//! was built from a clean tree. EP-OBSERVABILITY: a planned run with no
//! artifact, no exit code or no report is listed as missing, never dropped
//! from the table or read as a pass; a figure a report does not carry is
//! "n/a".
//!
//! A suite may run a preset report-only (#695, `report_only` in
//! suites.toml): its plan entries carry `gated: false`. Such a run is
//! collated like any other, in every table and held to its preset's gates,
//! and labelled "report-only, not gated" wherever it appears. A
//! capacity-envelope point is measured, not a verdict (#555 decision 5), so
//! its capacity budgets (shortfall and the latency budgets, see
//! [`capacity_exempt`]) never decide the suite. Everything else still does:
//! it fails the suite unless it reached its gate (an artifact of this
//! build's run of this preset, a gate exit of 0 or 1) and every other check
//! holds: the harness exited 0 (completed, reconciled exactly), no
//! durability finding, every gated phase ran, every tip measured, unanswered
//! submits within the preset's budget, and refused valid shares within it
//! once the commit deadline's refusals in a phase that missed its shortfall
//! budget are set aside (#847, see `refused_beyond_overload`), and how
//! many were is shown and recorded. A preset with no budget for refused
//! valid shares or for unanswered submits is never report-only. A plan
//! entry with no `gated` is gated, so plans and verdicts written before
//! #695 read as they did; a plan whose report-only preset now has no such
//! budget is refused.

use crate::classify::{LEDGER_CONFIRMATION_FAILED, NOT_CONFIRMED_BY_DATABASE};
use crate::compare::{self, Build, LoadedRun, Row, Run, COLUMNS, D1_PHASES};
use crate::gate::{evaluate, number, passed as gate_passed, Budgets};
use crate::preset::Preset;
use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const PLAN_SCHEMA: &str = "qbit.prism.l3-plan.v1";
pub const VERDICT_SCHEMA: &str = "qbit.prism.l3-verdict.v1";

/// Each run job's artifact is named this, then its matrix `id`; downloaded
/// by pattern, each lands in a directory of that name.
pub const RUN_ARTIFACT_PREFIX: &str = "prism-l3-run-";

/// What the plan job wrote: the suite, the one commit (and its tree) every
/// job ran, and the matrix it planned.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub schema: String,
    pub suite: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub commit: String,
    pub tree: String,
    pub event: String,
    pub include: Vec<PlanEntry>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanEntry {
    pub preset: String,
    pub runner: String,
    pub timeout_minutes: u32,
    pub repeat: u32,
    pub id: String,
    /// False for a preset the suite runs report-only (#695). Absent means
    /// gated.
    #[serde(default = "gated_by_default")]
    pub gated: bool,
}

fn gated_by_default() -> bool {
    true
}

fn is_object_id(text: &str) -> bool {
    text.len() == 40 && text.bytes().all(|b| b.is_ascii_hexdigit())
}

pub fn read_plan(path: &Path) -> Result<Plan> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let plan: Plan =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    ensure!(
        plan.schema == PLAN_SCHEMA,
        "{} has schema {:?}, not {PLAN_SCHEMA}",
        path.display(),
        plan.schema
    );
    ensure!(
        is_object_id(&plan.commit) && is_object_id(&plan.tree),
        "the plan's commit and tree must be full object ids"
    );
    ensure!(!plan.include.is_empty(), "the plan names no run");
    let mut ids = BTreeSet::new();
    for entry in &plan.include {
        ensure!(
            entry.repeat >= 1 && entry.id == format!("{}-r{}", entry.preset, entry.repeat),
            "plan entry {:?} is not <preset>-r<repeat> for {} repeat {}",
            entry.id,
            entry.preset,
            entry.repeat
        );
        ensure!(ids.insert(&entry.id), "the plan lists {} twice", entry.id);
    }
    check_gating(&plan.include)?;
    Ok(plan)
}

/// A preset is report-only for every repeat or for none, and at least one
/// run is gated, so a suite always has a verdict to give (#695).
fn check_gating(include: &[PlanEntry]) -> Result<()> {
    let mut gated_by_preset: BTreeMap<&str, bool> = BTreeMap::new();
    for entry in include {
        let first = *gated_by_preset.entry(&entry.preset).or_insert(entry.gated);
        ensure!(
            first == entry.gated,
            "the plan gates some runs of {} and not others",
            entry.preset
        );
    }
    ensure!(
        include.iter().any(|entry| entry.gated),
        "the plan gates no run: every entry is report-only"
    );
    Ok(())
}

const REPORT_ONLY: &str = "report-only, not gated";
const REPORT_ONLY_SUFFIX: &str = " (report-only, not gated)";

/// One planned run as the collation reads it.
#[derive(Clone, Debug)]
pub struct CollatedRun {
    pub entry: PlanEntry,
    pub loaded: LoadedRun,
    /// Why the artifact cannot be counted as this build's run of this
    /// preset at all: no artifact, another revision, another preset file, a
    /// dirty tree. `None` when it can.
    pub identity_problem: Option<String>,
    /// `gate-exit-code`: 0 or 1 is a verdict, anything else (or none) is not.
    pub gate_exit_code: Option<i32>,
    /// Passed every one of its preset's gates, as the collation evaluates
    /// them from the report and the harness's exit code.
    pub passed: bool,
    pub failed_checks: Vec<String>,
    /// A report-only run's failed checks under [`capacity_exempt`] budgets,
    /// with its refused valid shares judged by `refused_beyond_overload`:
    /// every check but its capacity budgets and the commit deadline's
    /// refusals under overload (#847). Empty for a gated run.
    pub failed_beyond_capacity: Vec<String>,
    /// A report-only run's refused valid shares set aside as its commit
    /// deadline's under overload (#847). 0 for a gated run.
    pub refusals_set_aside: u64,
}

impl CollatedRun {
    /// The run reached its gate: the job left a verdict (pass or fail) on an
    /// artifact that is this build's run of this preset.
    pub fn reached_gate(&self) -> bool {
        self.identity_problem.is_none() && matches!(self.gate_exit_code, Some(0 | 1))
    }

    /// Whether the run lets the suite pass. A gated run must pass its gate.
    /// A report-only run (#695) is exempt from its capacity budgets only,
    /// and from the commit deadline's refusals in a phase short of its rate
    /// (#847): it must still reach its gate and hold every other check, the
    /// harness's exit among them.
    pub fn holds_suite(&self) -> bool {
        if self.entry.gated {
            self.passed
        } else {
            self.reached_gate() && self.failed_beyond_capacity.is_empty()
        }
    }

    /// Why a report-only run fails the suite, or `None` when it does not.
    fn report_only_failure(&self) -> Option<String> {
        if self.entry.gated || self.holds_suite() {
            return None;
        }
        Some(if !self.reached_gate() {
            "it never reached its gate".to_owned()
        } else {
            self.failed_beyond_capacity.join("; ")
        })
    }

    /// " (report-only, not gated)" after a report-only run's name.
    fn label(&self) -> &'static str {
        if self.entry.gated {
            ""
        } else {
            REPORT_ONLY_SUFFIX
        }
    }
}

/// A report-only run's budgets: its capacity budgets opened as wide as they
/// go, every other field the preset's. Shortfall is unbounded; a latency
/// budget the preset sets is infinite, so a measured figure passes and an
/// unmeasured one (a tip some session never got work on) still fails;
/// refused valid shares and unanswered submits keep the preset's budgets
/// (the collation judges a report-only run's refused valid shares apart,
/// #847). A budget added to [`Budgets`] later keeps the preset's value
/// here, so it gates report-only runs until it is listed: the default fails
/// closed.
pub fn capacity_exempt(budgets: Budgets) -> Budgets {
    let open = |limit: Option<f64>| limit.map(|_| f64::INFINITY);
    Budgets {
        max_shortfall: u64::MAX,
        tip_last_notify_p99_ms: open(budgets.tip_last_notify_p99_ms),
        tip_last_notify_median_ms: open(budgets.tip_last_notify_median_ms),
        churn_tip_last_notify_p99_ms: open(budgets.churn_tip_last_notify_p99_ms),
        new_session_first_job_p99_ms: open(budgets.new_session_first_job_p99_ms),
        ..budgets
    }
}

/// A report-only run's failed checks (#695, #847), and how many refused
/// valid shares it set aside: every check under [`capacity_exempt`] budgets
/// but refused valid shares, which [`refused_beyond_overload`] judges
/// instead. A run with no report, or one the harness did not complete,
/// already fails on that, and its refusals are not listed beside it.
fn report_only_failures(
    report: &Value,
    exit_code: Option<i32>,
    budgets: &Budgets,
) -> (Vec<String>, u64) {
    let exempt = Budgets {
        max_rejected_valid_shares: None,
        ..capacity_exempt(budgets.clone())
    };
    let mut failed = failed_names(&evaluate(report, exit_code, &exempt));
    let mut set_aside = 0;
    if !report.is_null() && exit_code == Some(0) {
        let (refused, aside) = refused_beyond_overload(report, budgets);
        failed.extend(refused);
        set_aside = aside;
    }
    (failed, set_aside)
}

/// The one refusal a report-only point's overload accounts for (#847):
/// `ledger-confirmation-failed`, "share was not confirmed by the database".
/// The server answers it when a share's commit deadline passes before
/// COMMIT is sent, which is how an append past its capacity sheds load: on
/// the weekly 20x shape (#695) every such refusal in a gated phase was that
/// deadline. But it is also the server's answer to any other failed ledger
/// write, which the client cannot tell apart, so it is set aside only in a
/// report-only run and only in a phase that missed its shortfall budget
/// (see [`refused_beyond_overload`]). Every other refused valid share stays
/// gated: `current chain state is unavailable` (#716), `share outcome is not
/// yet known`, and `share was not committed because its commit gate closed`,
/// which also answers a refusal with no recorded cause.
fn overload_refusal(row: &Value) -> bool {
    row["reason_id"] == LEDGER_CONFIRMATION_FAILED && row["message"] == NOT_CONFIRMED_BY_DATABASE
}

/// A report-only run's refused valid shares (#847), per gated phase, held
/// to the preset's budget, and how many it set aside; a failure is named as
/// [`failed_names`] names one. The [`overload_refusal`]s are set aside only
/// in a phase whose shortfall exceeds the preset's budget. That is evidence
/// the phase ran past its capacity, not proof: a ledger failure that holds
/// sessions until the deadline makes a shortfall too, so a ledger failure
/// answered the same way in such a phase is not gated here (the set-aside
/// count is in the collation for a reader to see). In a phase that kept up
/// they count like any other refusal. The count is the larger of the
/// rejection table's counted refusals and the phase's
/// `rejected_valid_shares` less those set aside, so neither view can hide a
/// refusal the other counts. A gated phase with no name, no shortfall
/// figure, no refusal figure, no rejection table or a row with no count
/// fails, and so does a preset with no budget: collate() refuses one, and
/// this fails closed if it were reached anyway.
fn refused_beyond_overload(report: &Value, budgets: &Budgets) -> (Vec<String>, u64) {
    let Some(budget) = budgets.max_rejected_valid_shares else {
        return (
            vec!["valid shares refused (the preset sets no budget)".to_owned()],
            0,
        );
    };
    let rows = report["rejections"]["by_phase_reason_and_message"].as_array();
    let phases = report["phases"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut failed = Vec::new();
    let mut total_set_aside = 0u64;
    for phase in phases {
        let name = phase["name"].as_str();
        if let (Some(gated), Some(name)) = (&budgets.phases, name) {
            if !gated.iter().any(|gated| gated == name) {
                continue;
            }
        }
        let Some(name) = name else {
            // Only a preset that gates every phase reaches here.
            failed.push("a phase with no name: valid shares refused (not reported)".to_owned());
            continue;
        };
        let check = format!(
            "{name}: valid shares refused, not counting the commit deadline's under overload"
        );
        let (Some(rows), Some(refused), Some(shortfall)) = (
            rows,
            phase["rejected_valid_shares"].as_u64(),
            phase["shortfall"].as_u64(),
        ) else {
            failed.push(format!("{check} (not reported)"));
            continue;
        };
        let overloaded = shortfall > budgets.max_shortfall;
        let (mut set_aside, mut counted) = (0u64, 0u64);
        let mut uncounted_row = false;
        for row in rows
            .iter()
            .filter(|row| row["phase"] == name && row["class"] != "expected")
        {
            let Some(count) = row["count"].as_u64() else {
                uncounted_row = true;
                continue;
            };
            if overloaded && overload_refusal(row) {
                set_aside = set_aside.saturating_add(count);
            } else {
                counted = counted.saturating_add(count);
            }
        }
        if uncounted_row {
            failed.push(format!("{check} (a rejection row not reported)"));
            continue;
        }
        total_set_aside = total_set_aside.saturating_add(set_aside);
        let beyond = counted.max(refused.saturating_sub(set_aside));
        if beyond > budget {
            failed.push(format!(
                "{check} ({beyond}, with {set_aside} answered {NOT_CONFIRMED_BY_DATABASE:?} set \
                 aside at shortfall {shortfall}, budget {budget})"
            ));
        }
    }
    (failed, total_set_aside)
}

fn failed_names(checks: &[crate::gate::Check]) -> Vec<String> {
    checks
        .iter()
        .filter(|check| check.pass == Some(false))
        .map(|check| format!("{} ({})", check.name, check.observed))
        .collect()
}

fn read_code(path: &Path) -> Option<i32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn load_run(plan: &Plan, entry: &PlanEntry, runs_dir: &Path, preset: &Preset) -> CollatedRun {
    let dir = runs_dir.join(format!("{RUN_ARTIFACT_PREFIX}{}", entry.id));
    let exit_code = read_code(&dir.join("harness-exit-code"));
    let gate_exit_code = read_code(&dir.join("gate-exit-code"));
    let mut report_problem = None;
    let report: Option<Value> = if dir.is_dir() {
        match std::fs::read_to_string(dir.join("load-harness-report.json")) {
            Ok(text) => match serde_json::from_str(&text) {
                Ok(report) => Some(report),
                Err(error) => {
                    report_problem = Some(format!("its side report does not parse: {error}"));
                    None
                }
            },
            Err(_) => None,
        }
    } else {
        None
    };
    let identity_problem = if !dir.is_dir() {
        Some("no artifact: the job uploaded none".to_owned())
    } else if let Some(report) = &report {
        let revision = report["versions"]["coordinator_revision"].as_str();
        let sha = report["preset"]["sha256"].as_str();
        let name = report["preset"]["name"].as_str();
        // A failed run's reduced report may carry no versions or preset
        // block; its exit code fails it, and it cannot name another build.
        let failed = exit_code != Some(0);
        if revision.is_some_and(|r| r != plan.commit) || (!failed && revision.is_none()) {
            Some(format!(
                "its report names revision {revision:?}, not the plan's {}; runs of another \
                 build are never pooled",
                plan.commit
            ))
        } else if name.is_some_and(|n| n != preset.name)
            || sha.is_some_and(|s| s != preset.sha256)
            || (!failed && sha.is_none())
        {
            Some(format!(
                "it ran preset {} ({}), not {} ({})",
                name.unwrap_or("unreported"),
                sha.unwrap_or("no sha256"),
                preset.name,
                preset.sha256
            ))
        } else if report["dirty"] == true {
            Some("built from a dirty tree".to_owned())
        } else {
            None
        }
    } else {
        report_problem.clone()
    };
    let excluded = identity_problem.clone().or_else(|| match exit_code {
        Some(0) if report.is_some() => None,
        Some(0) => Some("no side report".to_owned()),
        Some(code) => Some(format!("exit {code}")),
        None => Some("exit not captured".to_owned()),
    });
    let report_value = report.as_ref().unwrap_or(&Value::Null);
    let checks = evaluate(report_value, exit_code, &Budgets::from(&preset.gates));
    // A report-only run is held to everything but its capacity budgets.
    let (failed_beyond_capacity, refusals_set_aside) = if entry.gated {
        (Vec::new(), 0)
    } else {
        report_only_failures(report_value, exit_code, &Budgets::from(&preset.gates))
    };
    // The gate's own verdict counts too: it holds a preset to checks this
    // evaluation may not repeat (a soak preset's), so a run the gate failed
    // never passes here.
    let passed = identity_problem.is_none() && gate_passed(&checks) && gate_exit_code == Some(0);
    // A run that is not this build's run of this preset has no checks worth
    // listing: the reason it is not counted is the whole finding.
    let failed_checks: Vec<String> = match &identity_problem {
        Some(problem) => vec![problem.clone()],
        None => {
            let mut failed = failed_names(&checks);
            if failed.is_empty() && gate_exit_code != Some(0) {
                failed.push(match gate_exit_code {
                    Some(code) => format!("qbit-prism-load-gate exited {code}; see its gate.md"),
                    None => "qbit-prism-load-gate left no exit code".to_owned(),
                });
            }
            failed
        }
    };
    CollatedRun {
        entry: entry.clone(),
        loaded: LoadedRun {
            run: Run {
                id: entry.id.clone(),
                build: entry.preset.clone(),
                repeat: entry.repeat,
                dir: dir.display().to_string(),
                exit_code,
                ceiling_hit: false,
                load_before: None,
                load_max: None,
                mem_available_min_mib: None,
            },
            report,
            excluded,
        },
        identity_problem,
        gate_exit_code,
        passed,
        failed_checks,
        failed_beyond_capacity,
        refusals_set_aside,
    }
}

/// The collation: the Markdown, the verdict document and whether the suite
/// passed (every planned run reached its gate; every gated run passed it,
/// and every report-only run held every check but its capacity budgets and
/// the commit deadline's refusals under overload, #695, #847).
pub struct Collation {
    pub markdown: String,
    pub verdict: Value,
    pub passed: bool,
}

fn int_arg(preset: &Preset, flag: &str) -> Option<u64> {
    preset.args.get(flag).and_then(Value::as_u64)
}

/// The configuration columns #473's tables lead with, plus sessions: the
/// capacity-envelope points (#555) differ from the D1 cells only there.
pub const CONFIG_COLUMNS: &[&str] = &["preset", "sessions", "window", "fe", "repl", "plan"];

fn config_cells(preset: &Preset) -> Vec<String> {
    let text = |flag: &str| {
        preset
            .args
            .get(flag)
            .and_then(Value::as_str)
            .unwrap_or("n/a")
            .to_owned()
    };
    let window = int_arg(preset, "--window-shares").map_or("n/a".into(), |w| {
        if w % 1000 == 0 {
            format!("{}k", w / 1000)
        } else {
            number(w as f64, None)
        }
    });
    let blocks = int_arg(preset, "--scheduled-blocks").unwrap_or(0);
    let mut plan = text("--plan");
    if text("--cadence") == "dense" {
        plan.push_str(&format!(" +dense({blocks})"));
    } else if blocks > 0 {
        plan.push_str(&format!(" +{blocks} blocks"));
    }
    vec![
        format!("`{}`", preset.name),
        int_arg(preset, "--sessions").map_or("n/a".into(), |s| number(s as f64, None)),
        window,
        int_arg(preset, "--frontends").map_or("n/a".into(), |f| f.to_string()),
        text("--replication"),
        plan,
    ]
}

fn short(id: &str) -> &str {
    &id[..id.len().min(8)]
}

fn escape(cell: &str) -> String {
    cell.replace('|', "\\|")
}

/// Collate `plan`'s runs from `runs_dir` against `presets`, the checked-in
/// presets at the plan's commit.
pub fn collate(
    plan: &Plan,
    runs_dir: &Path,
    presets: &BTreeMap<String, Preset>,
) -> Result<Collation> {
    check_gating(&plan.include)?;
    let mut order: Vec<&str> = Vec::new();
    for entry in &plan.include {
        let Some(preset) = presets.get(&entry.preset) else {
            bail!(
                "the plan names preset {}, which is not checked in",
                entry.preset
            );
        };
        // capacity_exempt re-evaluates what evaluate() checks; a soak's own
        // gates and a D1 cell's verdict it cannot exempt or repeat.
        ensure!(
            entry.gated || (preset.soak.is_none() && !preset.gates.d1_verdict_table),
            "the plan runs {} report-only, but a soak or D1 preset is always gated",
            entry.preset
        );
        // #847, EP-VALIDATION: report-only exempts capacity, never
        // correctness, so a preset with no budget for refused valid shares
        // or for unanswered submits would drop that check from its run.
        // Refuse it rather than drop it silently.
        ensure!(
            entry.gated
                || (preset.gates.max_rejected_valid_shares.is_some()
                    && preset.gates.max_unanswered_submits.is_some()),
            "the plan runs {} report-only, but its preset sets no budget for refused valid \
             shares or unanswered submits; report-only exempts capacity, never correctness",
            entry.preset
        );
        if !order.contains(&entry.preset.as_str()) {
            order.push(&entry.preset);
        }
    }
    let runs: Vec<CollatedRun> = plan
        .include
        .iter()
        .map(|entry| load_run(plan, entry, runs_dir, &presets[&entry.preset]))
        .collect();
    let reached = runs.iter().filter(|r| r.reached_gate()).count();
    let passing = runs.iter().filter(|r| r.passed).count();
    let complete = reached == runs.len();
    let passed = complete && runs.iter().all(CollatedRun::holds_suite);
    let (gated, report_only): (Vec<&CollatedRun>, Vec<&CollatedRun>) =
        runs.iter().partition(|r| r.entry.gated);
    let report_only_presets: BTreeSet<&str> = report_only
        .iter()
        .map(|r| r.entry.preset.as_str())
        .collect();
    // check_gating holds a preset's repeats to one setting.
    let preset_label = |name: &str| {
        if report_only_presets.contains(name) {
            REPORT_ONLY_SUFFIX
        } else {
            ""
        }
    };

    let mut out = format!(
        "## L3 suite `{}` on `{}`: commit `{}`, tree `{}`\n\n",
        plan.suite,
        plan.reference,
        short(&plan.commit),
        short(&plan.tree)
    );
    out.push_str(&format!(
        "- Event: `{}`. Planned {} runs of {} presets; {reached} reached their gate and \
         {passing} passed it.\n",
        plan.event,
        runs.len(),
        order.len()
    ));
    if !report_only.is_empty() {
        out.push_str(&format!(
            "- **Report-only, not gated** (#695; capacity-envelope points, #555 decision 5): {}. \
             Its capacity budgets (shortfall, latency) do not decide the suite; its numbers are \
             in the tables below. It still fails the suite unless it reaches its gate and holds \
             every other check: harness exit 0 (completed and reconciled exactly), no durability \
             finding, every gated phase ran, every tip measured, unanswered submits within \
             budget, and refused valid shares within budget once those answered `share was not \
             confirmed by the database` in a phase short of its rate (its commit deadline passing \
             under overload, #847) are set aside. Of {}: {} reached their gate, {} held every \
             check above, {} passed their own gates; {} refused valid share(s) were set aside as \
             the commit deadline's under overload.\n",
            report_only
                .iter()
                .map(|r| format!("`{}`", r.entry.id))
                .collect::<Vec<_>>()
                .join(", "),
            report_only.len(),
            report_only.iter().filter(|r| r.reached_gate()).count(),
            report_only.iter().filter(|r| r.holds_suite()).count(),
            report_only.iter().filter(|r| r.passed).count(),
            report_only
                .iter()
                .map(|r| r.refusals_set_aside)
                .fold(0u64, u64::saturating_add),
        ));
    }
    let unreached: Vec<String> = runs
        .iter()
        .filter(|r| !r.reached_gate())
        .map(|r| {
            format!(
                "`{}`{} ({})",
                r.entry.id,
                r.label(),
                r.identity_problem
                    .clone()
                    .unwrap_or_else(|| match r.gate_exit_code {
                        Some(code) =>
                            format!("gate exit {code}: the gate could not read its inputs"),
                        None => "no gate verdict".to_owned(),
                    })
            )
        })
        .collect();
    out.push_str(&format!(
        "- Runs that never reached a verdict: {}\n",
        if unreached.is_empty() {
            "none".to_owned()
        } else {
            unreached.join(", ")
        }
    ));
    out.push_str(
        "- D1 verdicts are #473's rule over the repeats in the medians. A preset with no D1 \
         verdict table (a capacity-envelope point or a mainnet shape) is measured beside the \
         cells and held only to its own gates, never to D1.\n- Rates compare only on one runner \
         class: each run's runner is below and in its artifact's host.json.\n\n",
    );

    out.push_str(
        "### Runs\n\n| run | runner | harness exit | gate | steady_state achieved /s | burst \
         achieved /s |\n|---|---|---|---|---|---|\n",
    );
    for run in &runs {
        let achieved = |phase: &str| {
            run.loaded
                .phase(phase)
                .and_then(|p| {
                    p["achieved_rate_shares_per_second"]
                        .as_f64()
                        .map(|a| number(a, p["target_rate_shares_per_second"].as_f64()))
                })
                .unwrap_or_else(|| "n/a".into())
        };
        let gate = if let Some(problem) = &run.identity_problem {
            format!("not counted: {problem}")
        } else {
            match run.gate_exit_code {
                Some(0) if run.passed => "pass".to_owned(),
                // The gate passed on its inputs but the collation's own
                // evaluation did not agree: show both, never a pass.
                Some(0) => "gate passed, collation failed".to_owned(),
                Some(1) => "fail".to_owned(),
                Some(code) => format!("no verdict (gate exit {code})"),
                None => "no verdict".to_owned(),
            }
        };
        // EP-OBSERVABILITY: a report-only run's own result is shown, never
        // folded into a pass, and always labelled as outside the verdict.
        let gate = match (run.entry.gated, run.report_only_failure()) {
            (true, _) => gate,
            (false, None) => format!("{REPORT_ONLY}: {gate}"),
            (false, Some(why)) => format!("{REPORT_ONLY}: {gate}; fails the suite: {why}"),
        };
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            run.entry.id,
            run.entry.runner,
            run.loaded
                .run
                .exit_code
                .map_or("not captured".into(), |c| c.to_string()),
            escape(&gate),
            achieved("steady_state"),
            achieved("burst"),
        ));
    }

    let verdict_column = COLUMNS
        .iter()
        .position(|c| *c == "verdict")
        .context("compare::COLUMNS has no verdict column")?;
    // The summarizer's cells after its build and commit columns: the build
    // is the one this collation names in its heading.
    let skip = 2;
    for phase in D1_PHASES {
        let mut rows: Vec<(Vec<String>, Row)> = Vec::new();
        for name in &order {
            let preset = &presets[*name];
            let planned = compare::expected_phases(&preset.args)
                .with_context(|| format!("planning preset {name}'s phases"))?;
            if !planned.iter().any(|p| p.name == *phase) {
                continue;
            }
            let build = Build {
                label: (*name).to_owned(),
                reference: plan.reference.clone(),
                commit: plan.commit.clone(),
                dropped_legacy_flags: Vec::new(),
                predates_report_fields: Vec::new(),
            };
            let mine: Vec<&LoadedRun> = runs
                .iter()
                .filter(|r| r.entry.preset == *name)
                .map(|r| &r.loaded)
                .collect();
            let mut row = compare::row(&build, &mine, phase);
            let mut config = config_cells(preset);
            if !preset.gates.d1_verdict_table {
                row.cells[verdict_column] = "not a D1 cell; see its gates".to_owned();
            }
            if report_only_presets.contains(name) {
                // Its D1 cell is kept, labelled: shown, never a verdict.
                config[0].push_str(REPORT_ONLY_SUFFIX);
                row.cells[verdict_column] = format!("{REPORT_ONLY}: {}", row.cells[verdict_column]);
            }
            rows.push((config, row));
        }
        if rows.is_empty() {
            continue;
        }
        let headers: Vec<&str> = CONFIG_COLUMNS
            .iter()
            .chain(COLUMNS[skip..].iter())
            .copied()
            .collect();
        out.push_str(&format!(
            "\n### `{phase}`\n\n| {} |\n|{}\n",
            headers.join(" | "),
            "---|".repeat(headers.len())
        ));
        for (config, row) in &rows {
            let cells: Vec<String> = config
                .iter()
                .chain(row.cells[skip..].iter())
                .map(|c| escape(c))
                .collect();
            out.push_str(&format!("| {} |\n", cells.join(" | ")));
        }
    }

    out.push_str(
        "\n### The presets' own gates\n\n| preset | runs passing | failed checks |\n|---|---|---|\n",
    );
    for name in &order {
        let mine: Vec<&CollatedRun> = runs.iter().filter(|r| r.entry.preset == *name).collect();
        let failures: Vec<String> = mine
            .iter()
            .flat_map(|r| {
                r.failed_checks
                    .iter()
                    .map(|c| format!("{}: {c}", r.entry.id))
            })
            .collect();
        out.push_str(&format!(
            "| `{name}`{} | {} / {} | {} |\n",
            preset_label(name),
            mine.iter().filter(|r| r.passed).count(),
            mine.len(),
            if failures.is_empty() {
                "–".to_owned()
            } else {
                escape(&failures.join("; "))
            }
        ));
    }
    // Runs that reached their gate and fail the suite on it; the ones that
    // never reached it are counted apart.
    let report_only_failures: Vec<String> = report_only
        .iter()
        .filter(|r| r.reached_gate())
        .filter_map(|r| {
            r.report_only_failure()
                .map(|why| format!("`{}` ({why})", r.entry.id))
        })
        .collect();
    let mut why_failed = Vec::new();
    if gated.iter().any(|r| r.reached_gate() && !r.passed) {
        why_failed.push("not every gated one passed".to_owned());
    }
    if !report_only_failures.is_empty() {
        why_failed.push(format!(
            "a report-only run failed a check beyond its capacity budgets: {}",
            report_only_failures.join(", ")
        ));
    }
    out.push_str(&format!(
        "\n**Suite verdict: {}**\n",
        if passed && report_only.is_empty() {
            "PASS: every planned run reached its gate and passed it".to_owned()
        } else if passed {
            format!(
                "PASS: every planned run reached its gate, every gated run passed it, and every \
                 report-only run held every check but its capacity budgets and the commit \
                 deadline's refusals under overload ({} set aside); {} report-only run(s) are \
                 not gated on those",
                report_only
                    .iter()
                    .map(|r| r.refusals_set_aside)
                    .fold(0u64, u64::saturating_add),
                report_only.len()
            )
        } else if complete {
            format!(
                "FAIL: every planned run reached its gate, and {}",
                why_failed.join("; and ")
            )
        } else {
            let mut text = format!(
                "FAIL: {} of {} planned runs never reached a verdict",
                runs.len() - reached,
                runs.len()
            );
            for why in &why_failed {
                text.push_str(&format!("; and of those that did, {why}"));
            }
            text
        }
    ));

    let verdict = json!({
        "schema": VERDICT_SCHEMA,
        "suite": plan.suite,
        "ref": plan.reference,
        "commit": plan.commit,
        "tree": plan.tree,
        "event": plan.event,
        "planned": runs.len(),
        "gated_runs": gated.len(),
        "reached_gate": reached,
        "passed_gate": passing,
        "complete": complete,
        "passed": passed,
        "runs": runs.iter().map(|r| json!({
            "id": r.entry.id,
            "preset": r.entry.preset,
            "repeat": r.entry.repeat,
            "runner": r.entry.runner,
            "gated": r.entry.gated,
            "holds_suite": r.holds_suite(),
            "refusals_set_aside": r.refusals_set_aside,
            "harness_exit_code": r.loaded.run.exit_code,
            "gate_exit_code": r.gate_exit_code,
            "reached_gate": r.reached_gate(),
            "passed": r.passed,
            "problem": r.identity_problem,
        })).collect::<Vec<_>>(),
    });
    Ok(Collation {
        markdown: out,
        verdict,
        passed,
    })
}
