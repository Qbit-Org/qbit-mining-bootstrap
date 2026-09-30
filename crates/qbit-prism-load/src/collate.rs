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
    Ok(plan)
}

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
}

impl CollatedRun {
    /// The run reached its gate: the job left a verdict (pass or fail) on an
    /// artifact that is this build's run of this preset.
    pub fn reached_gate(&self) -> bool {
        self.identity_problem.is_none() && matches!(self.gate_exit_code, Some(0 | 1))
    }
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
    let checks = evaluate(
        report.as_ref().unwrap_or(&Value::Null),
        exit_code,
        &Budgets::from(&preset.gates),
    );
    // The gate's own verdict counts too: it holds a preset to checks this
    // evaluation may not repeat (a soak preset's), so a run the gate failed
    // never passes here.
    let passed = identity_problem.is_none() && gate_passed(&checks) && gate_exit_code == Some(0);
    // A run that is not this build's run of this preset has no checks worth
    // listing: the reason it is not counted is the whole finding.
    let failed_checks: Vec<String> = match &identity_problem {
        Some(problem) => vec![problem.clone()],
        None => {
            let mut failed: Vec<String> = checks
                .iter()
                .filter(|check| check.pass == Some(false))
                .map(|check| format!("{} ({})", check.name, check.observed))
                .collect();
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
    }
}

/// The collation: the Markdown, the verdict document and whether the suite
/// passed (every planned run reached its gate and passed it).
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
    let mut order: Vec<&str> = Vec::new();
    for entry in &plan.include {
        if !presets.contains_key(&entry.preset) {
            bail!(
                "the plan names preset {}, which is not checked in",
                entry.preset
            );
        }
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
    let passed = complete && passing == runs.len();

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
    let unreached: Vec<String> = runs
        .iter()
        .filter(|r| !r.reached_gate())
        .map(|r| {
            format!(
                "`{}` ({})",
                r.entry.id,
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
            };
            let mine: Vec<&LoadedRun> = runs
                .iter()
                .filter(|r| r.entry.preset == *name)
                .map(|r| &r.loaded)
                .collect();
            let mut row = compare::row(&build, &mine, phase);
            if !preset.gates.d1_verdict_table {
                row.cells[verdict_column] = "not a D1 cell; see its gates".to_owned();
            }
            rows.push((config_cells(preset), row));
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
            "| `{name}` | {} / {} | {} |\n",
            mine.iter().filter(|r| r.passed).count(),
            mine.len(),
            if failures.is_empty() {
                "–".to_owned()
            } else {
                escape(&failures.join("; "))
            }
        ));
    }
    out.push_str(&format!(
        "\n**Suite verdict: {}**\n",
        if passed {
            "PASS: every planned run reached its gate and passed it".to_owned()
        } else if complete {
            "FAIL: every planned run reached its gate, and not every one passed".to_owned()
        } else {
            format!(
                "FAIL: {} of {} planned runs never reached a verdict",
                runs.len() - reached,
                runs.len()
            )
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
        "reached_gate": reached,
        "passed_gate": passing,
        "complete": complete,
        "passed": passed,
        "runs": runs.iter().map(|r| json!({
            "id": r.entry.id,
            "preset": r.entry.preset,
            "repeat": r.entry.repeat,
            "runner": r.entry.runner,
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
