//! The L3 collation (#550): one build's suite in #473's tables.

use anyhow::Result;
use qbit_prism_load::collate::{self, Plan, PlanEntry, PLAN_SCHEMA, RUN_ARTIFACT_PREFIX};
use qbit_prism_load::compare;
use qbit_prism_load::preset::{load_all, presets_dir, Preset};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const COMMIT: &str = "8fc94faf00000000000000000000000000000000";
const TREE: &str = "0123456789abcdef0123456789abcdef01234567";

fn presets() -> BTreeMap<String, Preset> {
    load_all(&presets_dir())
        .unwrap()
        .into_iter()
        .map(|p| (p.name.clone(), p))
        .collect()
}

fn entry(preset: &str, repeat: u32) -> PlanEntry {
    PlanEntry {
        preset: preset.to_owned(),
        runner: "blacksmith-32vcpu-ubuntu-2404".to_owned(),
        timeout_minutes: 60,
        repeat,
        id: format!("{preset}-r{repeat}"),
        gated: true,
    }
}

/// A run of a preset the suite measures report-only (#695).
fn report_only(preset: &str, repeat: u32) -> PlanEntry {
    PlanEntry {
        gated: false,
        ..entry(preset, repeat)
    }
}

/// The 20x growth shape as the weekly reduced L3 saw it (#695): every
/// share reconciled, but steady_state short of its 1,000/s mean.
fn short_of_its_rate(preset: &Preset) -> Value {
    let mut value = report(preset, COMMIT);
    let phases = value["phases"].as_array_mut().unwrap();
    let steady = phases
        .iter_mut()
        .find(|p| p["name"] == "steady_state")
        .unwrap();
    steady["achieved_rate_shares_per_second"] = json!(445.6);
    steady["offered_rate_shares_per_second"] = json!(446.0);
    steady["shortfall"] = json!(4874);
    // And its tip-to-last-notify p99 past the 5,000 ms budget.
    value["time_to_usable_work"]["tips"] = json!([{"all_sessions_milliseconds": 19389.0}]);
    value
}

fn plan(entries: Vec<PlanEntry>) -> Plan {
    Plan {
        schema: PLAN_SCHEMA.to_owned(),
        suite: "l3-full".to_owned(),
        reference: "refs/pull/600/merge".to_owned(),
        commit: COMMIT.to_owned(),
        tree: TREE.to_owned(),
        event: "pull_request".to_owned(),
        include: entries,
    }
}

/// Every phase the preset plans, each meeting #473's rule at its rate. The
/// run's one tip (below) is the one its first phase planned: a median gate
/// (#842) counts the tips a run delivered against the ones its phases
/// planned (#854), and a report that does not say is unmeasured.
fn report(preset: &Preset, commit: &str) -> Value {
    let phases: Vec<Value> = compare::expected_phases(&preset.args)
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(index, plan)| {
            json!({
                "name": plan.name,
                "external_tips_planned": u64::from(index == 0),
                "target_rate_shares_per_second": plan.rate,
                "duration_seconds": plan.seconds as f64,
                "offered_rate_shares_per_second": plan.rate - 0.003,
                "achieved_rate_shares_per_second": plan.rate - 0.003,
                "shortfall": 0,
                "rejected_valid_shares": 0,
                "client_ack_latency": {"p50": 2.5, "p99": 30.0},
                "order_lock": {"max_waiters": 31, "mean_waiters": 0.16},
                "reconciliation": {"missing": 0, "unexpected": 0},
                "scheduled_blocks": 0,
            })
        })
        .collect();
    json!({
        "dirty": false,
        "durability_findings": [],
        "versions": {"coordinator_revision": commit},
        "preset": {"name": preset.name, "sha256": preset.sha256},
        "validator": {"ack_p99_limit_used_milliseconds": 1000.0},
        "topology": {"sessions": preset.args["--sessions"]},
        "time_to_usable_work": {"tips": [{"all_sessions_milliseconds": 600.0}]},
        "rejections": {"no_response_by_phase": {}, "by_phase_reason_and_message": []},
        "phases": phases,
    })
}

fn scratch() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("prism-collate-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// What a run job uploads: its exit codes and the harness's report.
fn write_run(dir: &Path, id: &str, harness: &str, gate: &str, report: Option<&Value>) {
    let run = dir.join(format!("{RUN_ARTIFACT_PREFIX}{id}"));
    std::fs::create_dir_all(&run).unwrap();
    std::fs::write(run.join("harness-exit-code"), format!("{harness}\n")).unwrap();
    std::fs::write(run.join("gate-exit-code"), format!("{gate}\n")).unwrap();
    if let Some(report) = report {
        std::fs::write(
            run.join("load-harness-report.json"),
            serde_json::to_vec(report).unwrap(),
        )
        .unwrap();
    }
}

#[test]
fn a_complete_suite_renders_one_row_per_preset_in_473s_columns_and_passes() -> Result<()> {
    let presets = presets();
    let dir = scratch();
    let names = [
        "throughput-400k-window-1fe-async",
        "throughput-400k-window-2fe-async-3-blocks",
        "dense-cadence-400k-window-2fe-async",
    ];
    let mut entries = Vec::new();
    for name in names {
        for repeat in 1..=2 {
            let entry = entry(name, repeat);
            write_run(
                &dir,
                &entry.id,
                "0",
                "0",
                Some(&report(&presets[name], COMMIT)),
            );
            entries.push(entry);
        }
    }
    let collation = collate::collate(&plan(entries), &dir, &presets)?;
    let md = &collation.markdown;
    assert!(collation.passed, "{md}");
    assert!(md.contains("### `steady_state`"), "{md}");
    assert!(md.contains("### `burst`"), "{md}");
    assert!(
        md.contains("| `throughput-400k-window-2fe-async-3-blocks` | 2,000 | 400k | 2 | async | d1 +3 blocks | 2 / 2 |"),
        "{md}"
    );
    assert!(
        md.contains(
            "| `dense-cadence-400k-window-2fe-async` | 2,000 | 400k | 2 | async | d1 +dense(15) |"
        ),
        "{md}"
    );
    assert!(md.contains("**met** (2 of 2)"), "{md}");
    assert!(md.contains("**Suite verdict: PASS"), "{md}");
    assert_eq!(collation.verdict["complete"], true);
    assert_eq!(collation.verdict["planned"], 6);
    assert_eq!(collation.verdict["tree"], TREE);
    Ok(())
}

#[test]
fn a_planned_run_with_no_artifact_is_listed_and_fails_the_suite_not_dropped() -> Result<()> {
    let presets = presets();
    let dir = scratch();
    let name = "throughput-400k-window-4fe-async";
    write_run(
        &dir,
        &format!("{name}-r1"),
        "0",
        "0",
        Some(&report(&presets[name], COMMIT)),
    );
    let collation = collate::collate(&plan(vec![entry(name, 1), entry(name, 2)]), &dir, &presets)?;
    assert!(!collation.passed);
    assert_eq!(collation.verdict["complete"], false);
    assert_eq!(collation.verdict["reached_gate"], 1);
    assert_eq!(
        collation.verdict["runs"][1]["problem"],
        "no artifact: the job uploaded none"
    );
    let md = &collation.markdown;
    assert!(md.contains(&format!("`{name}-r2` (no artifact")), "{md}");
    assert!(md.contains("1 / 2"), "{md}");
    assert!(md.contains("never reached a verdict"), "{md}");
    Ok(())
}

#[test]
fn another_builds_report_or_another_preset_file_is_never_counted() -> Result<()> {
    let presets = presets();
    let dir = scratch();
    let name = "throughput-400k-window-1fe-async";
    let other = "5d0042f629f3271ef6743ec9d859cb0c35018f31";
    write_run(
        &dir,
        &format!("{name}-r1"),
        "0",
        "0",
        Some(&report(&presets[name], other)),
    );
    let mut edited = report(&presets[name], COMMIT);
    edited["preset"]["sha256"] = json!("00".repeat(32));
    write_run(&dir, &format!("{name}-r2"), "0", "0", Some(&edited));
    let collation = collate::collate(&plan(vec![entry(name, 1), entry(name, 2)]), &dir, &presets)?;
    assert!(!collation.passed);
    assert_eq!(collation.verdict["reached_gate"], 0);
    let md = &collation.markdown;
    assert!(
        md.contains(&format!("names revision Some(\"{other}\")")),
        "{md}"
    );
    assert!(md.contains("it ran preset"), "{md}");
    // Neither run is in the medians, so the row cannot read as met.
    assert!(md.contains("0 / 2"), "{md}");
    Ok(())
}

#[test]
fn a_failed_gate_is_a_verdict_but_not_a_pass() -> Result<()> {
    let presets = presets();
    let dir = scratch();
    let name = "throughput-400k-window-2fe-async";
    let mut refused = report(&presets[name], COMMIT);
    refused["phases"][1]["rejected_valid_shares"] = json!(1);
    write_run(&dir, &format!("{name}-r1"), "0", "1", Some(&refused));
    let collation = collate::collate(&plan(vec![entry(name, 1)]), &dir, &presets)?;
    assert!(!collation.passed);
    assert_eq!(collation.verdict["complete"], true);
    assert_eq!(collation.verdict["passed_gate"], 0);
    assert!(
        collation.markdown.contains("1 valid refused"),
        "{}",
        collation.markdown
    );
    assert!(collation
        .markdown
        .contains("FAIL: every planned run reached its gate"));
    Ok(())
}

#[test]
fn a_run_its_gate_failed_never_passes_though_the_collation_finds_nothing() -> Result<()> {
    // The gate holds a preset to checks the collation may not repeat (a
    // soak's): its failing exit is enough.
    let presets = presets();
    let dir = scratch();
    let name = "throughput-400k-window-1fe-async";
    write_run(
        &dir,
        &format!("{name}-r1"),
        "0",
        "1",
        Some(&report(&presets[name], COMMIT)),
    );
    let collation = collate::collate(&plan(vec![entry(name, 1)]), &dir, &presets)?;
    assert!(!collation.passed);
    assert_eq!(collation.verdict["runs"][0]["passed"], false);
    assert!(
        collation.markdown.contains("qbit-prism-load-gate exited 1"),
        "{}",
        collation.markdown
    );
    Ok(())
}

#[test]
fn a_preset_with_no_d1_table_is_measured_but_given_no_d1_verdict() -> Result<()> {
    let presets = presets();
    let dir = scratch();
    // The short plan runs no burst, so the preset has no burst row.
    let name = "mainnet-shape-650-addresses";
    assert!(!presets[name].gates.d1_verdict_table);
    write_run(
        &dir,
        &format!("{name}-r1"),
        "0",
        "0",
        Some(&report(&presets[name], COMMIT)),
    );
    let collation = collate::collate(&plan(vec![entry(name, 1)]), &dir, &presets)?;
    let md = &collation.markdown;
    assert!(md.contains("not a D1 cell; see its gates"), "{md}");
    assert!(!md.contains("**met**"), "{md}");
    assert!(!md.contains("### `burst`"), "{md}");
    Ok(())
}

#[test]
fn a_report_only_points_failed_budgets_are_shown_but_never_fail_the_suite() -> Result<()> {
    let presets = presets();
    let dir = scratch();
    let gated = "mainnet-shape-650-addresses";
    let point = "mainnet-shape-2600-addresses";
    write_run(
        &dir,
        &format!("{gated}-r1"),
        "0",
        "0",
        Some(&report(&presets[gated], COMMIT)),
    );
    // #695's weekly result: harness exit 0 (completed, reconciled exactly),
    // gate exit 1 on its shortfall budget.
    write_run(
        &dir,
        &format!("{point}-r1"),
        "0",
        "1",
        Some(&short_of_its_rate(&presets[point])),
    );
    let mut the_plan = plan(vec![entry(gated, 1), report_only(point, 1)]);
    the_plan.suite = "l3-reduced".to_owned();
    let collation = collate::collate(&the_plan, &dir, &presets)?;
    let md = &collation.markdown;
    assert!(collation.passed, "{md}");
    assert!(
        md.contains(
            "**Suite verdict: PASS: every planned run reached its gate, every gated run passed \
             it, and every report-only run held every check but its capacity budgets and the \
             commit deadline's refusals under overload (0 set aside); 1 report-only run(s) are \
             not gated on those**"
        ),
        "{md}"
    );
    // Labelled wherever it appears, its own result shown, never a pass.
    assert!(
        md.contains(&format!(
            "**Report-only, not gated** (#695; capacity-envelope points, #555 decision 5): \
             `{point}-r1`"
        )),
        "{md}"
    );
    assert!(
        md.contains(
            "Of 1: 1 reached their gate, 1 held every check above, 0 passed their own gates; 0 \
             refused valid share(s) were set aside as the commit deadline's under overload."
        ),
        "{md}"
    );
    assert!(
        md.contains(&format!(
            "| {point}-r1 | blacksmith-32vcpu-ubuntu-2404 | 0 | report-only, not gated: fail | \
             445.6"
        )),
        "{md}"
    );
    assert!(
        md.contains(&format!("| `{point}` (report-only, not gated) | 8,000 |")),
        "{md}"
    );
    assert!(
        md.contains("report-only, not gated: not a D1 cell; see its gates"),
        "{md}"
    );
    // Its shortfall is in the gates table, against its own budget.
    let gates_row = md
        .lines()
        .find(|l| l.starts_with(&format!("| `{point}` (report-only, not gated) | 0 / 1 |")))
        .unwrap_or_else(|| panic!("{md}"));
    assert!(gates_row.contains("steady_state: shortfall"), "{gates_row}");
    assert!(gates_row.contains("(4874)"), "{gates_row}");
    assert!(gates_row.contains("19389 ms"), "{gates_row}");
    let verdict = &collation.verdict;
    assert_eq!(verdict["passed"], true);
    assert_eq!(verdict["complete"], true);
    assert_eq!(verdict["planned"], 2);
    assert_eq!(verdict["gated_runs"], 1);
    assert_eq!(verdict["passed_gate"], 1);
    assert_eq!(verdict["runs"][1]["gated"], false);
    assert_eq!(verdict["runs"][1]["passed"], false);
    assert_eq!(verdict["runs"][1]["holds_suite"], true);
    assert_eq!(verdict["runs"][1]["reached_gate"], true);
    Ok(())
}

#[test]
fn a_gated_presets_failure_still_fails_a_suite_with_a_report_only_point() -> Result<()> {
    let presets = presets();
    let dir = scratch();
    let gated = "mainnet-shape-650-addresses";
    let point = "mainnet-shape-2600-addresses";
    write_run(
        &dir,
        &format!("{gated}-r1"),
        "0",
        "1",
        Some(&short_of_its_rate(&presets[gated])),
    );
    // The report-only point passing its gates cannot carry the suite.
    write_run(
        &dir,
        &format!("{point}-r1"),
        "0",
        "0",
        Some(&report(&presets[point], COMMIT)),
    );
    let collation = collate::collate(
        &plan(vec![entry(gated, 1), report_only(point, 1)]),
        &dir,
        &presets,
    )?;
    let md = &collation.markdown;
    assert!(!collation.passed, "{md}");
    assert!(
        md.contains(
            "**Suite verdict: FAIL: every planned run reached its gate, and not every gated one \
             passed**"
        ),
        "{md}"
    );
    // A report-only run that passed is still not shown as a plain pass.
    assert!(
        md.contains(&format!(
            "| {point}-r1 | blacksmith-32vcpu-ubuntu-2404 | 0 | report-only, not gated: pass |"
        )),
        "{md}"
    );
    assert_eq!(collation.verdict["passed"], false);
    assert_eq!(collation.verdict["complete"], true);
    Ok(())
}

#[test]
fn a_report_only_run_that_did_not_reconcile_fails_the_suite() -> Result<()> {
    // Report-only exempts the budgets, never the correctness the harness's
    // exit stands for: a lost acknowledged share (exit 4) fails the suite.
    let presets = presets();
    let dir = scratch();
    let gated = "mainnet-shape-650-addresses";
    let point = "mainnet-shape-2600-addresses";
    write_run(
        &dir,
        &format!("{gated}-r1"),
        "0",
        "0",
        Some(&report(&presets[gated], COMMIT)),
    );
    let mut lost = short_of_its_rate(&presets[point]);
    lost["durability_findings"] = json!([{"share": "lost"}]);
    write_run(&dir, &format!("{point}-r1"), "4", "1", Some(&lost));
    let collation = collate::collate(
        &plan(vec![entry(gated, 1), report_only(point, 1)]),
        &dir,
        &presets,
    )?;
    let md = &collation.markdown;
    assert!(!collation.passed, "{md}");
    assert_eq!(collation.verdict["complete"], true);
    assert_eq!(collation.verdict["runs"][1]["holds_suite"], false);
    assert!(
        md.contains(&format!(
            "FAIL: every planned run reached its gate, and a report-only run failed a check \
             beyond its capacity budgets: `{point}-r1` (harness exit code (0: completed and \
             reconciled exactly) (4); acknowledged shares lost or unexplained (durability \
             findings) (1))"
        )),
        "{md}"
    );
    assert!(
        md.contains("report-only, not gated: fail; fails the suite: harness exit code"),
        "{md}"
    );
    // A gated failure beside it is named too.
    write_run(
        &dir,
        &format!("{gated}-r1"),
        "0",
        "1",
        Some(&short_of_its_rate(&presets[gated])),
    );
    let collation = collate::collate(
        &plan(vec![entry(gated, 1), report_only(point, 1)]),
        &dir,
        &presets,
    )?;
    assert!(
        collation.markdown.contains(
            "and not every gated one passed; and a report-only run failed a check beyond its \
             capacity budgets"
        ),
        "{}",
        collation.markdown
    );
    Ok(())
}

#[test]
fn a_report_only_run_is_exempt_from_its_capacity_budgets_only() -> Result<()> {
    // Harness exit 0 and gate exit 1, but on a check that is not a
    // capacity budget: the suite fails, whatever the budgets say.
    let presets = presets();
    let gated = "mainnet-shape-650-addresses";
    let point = "mainnet-shape-2600-addresses";
    let mut no_reconnect = short_of_its_rate(&presets[point]);
    no_reconnect["phases"]
        .as_array_mut()
        .unwrap()
        .retain(|p| p["name"] != "reconnect");
    let mut unmeasured_tip = short_of_its_rate(&presets[point]);
    unmeasured_tip["time_to_usable_work"]["tips"] = json!([
        {"all_sessions_milliseconds": 900.0},
        {"all_sessions_milliseconds": null, "all_sessions_unavailable_reason": "a session got no job"},
    ]);
    for (case, point_report, check) in [
        (
            "a gated phase missing",
            no_reconnect,
            "reconnect: phase ran",
        ),
        ("a tip unmeasured", unmeasured_tip, "a session got no job"),
    ] {
        let dir = scratch();
        write_run(
            &dir,
            &format!("{gated}-r1"),
            "0",
            "0",
            Some(&report(&presets[gated], COMMIT)),
        );
        write_run(&dir, &format!("{point}-r1"), "0", "1", Some(&point_report));
        let collation = collate::collate(
            &plan(vec![entry(gated, 1), report_only(point, 1)]),
            &dir,
            &presets,
        )?;
        let md = &collation.markdown;
        assert!(!collation.passed, "{case}: {md}");
        assert_eq!(collation.verdict["runs"][1]["holds_suite"], false, "{case}");
        let verdict_line = md
            .lines()
            .find(|l| l.starts_with("**Suite verdict"))
            .unwrap();
        assert!(
            verdict_line.contains("a report-only run failed a check beyond its capacity budgets"),
            "{case}: {verdict_line}"
        );
        assert!(verdict_line.contains(check), "{case}: {verdict_line}");
        // The shortfall it also missed is no part of why.
        assert!(
            !verdict_line.contains("shortfall"),
            "{case}: {verdict_line}"
        );
    }
    Ok(())
}

/// A refused-share row of the harness's rejection table.
fn refusal(phase: &str, reason_id: &str, message: &str, count: u64) -> Value {
    json!({
        "phase": phase, "code": 20, "reason_id": reason_id, "message": message,
        "count": count, "class": "backend", "rebuild_pending": false,
    })
}

/// The 20x shape as the weekly reduced L3 saw it on 2026-10-26 (run
/// 40326852601): short of its rate in warm_up and steady_state, and its
/// commit deadline passing under that overload, so both refused valid shares
/// answered `share was not confirmed by the database` (#847).
fn refused_at_its_commit_deadline(preset: &Preset) -> Value {
    let mut value = short_of_its_rate(preset);
    let mut rows = Vec::new();
    for phase in value["phases"].as_array_mut().unwrap() {
        let refused = match phase["name"].as_str().unwrap() {
            "warm_up" => 10003,
            "steady_state" => 1510,
            "slow_database" => 20899,
            _ => continue,
        };
        phase["rejected_valid_shares"] = json!(refused);
        if phase["name"] == "warm_up" {
            phase["shortfall"] = json!(104513);
        }
        rows.push(refusal(
            phase["name"].as_str().unwrap(),
            "ledger-confirmation-failed",
            "share was not confirmed by the database",
            refused,
        ));
    }
    rows.push(json!({
        "phase": "steady_state", "code": 21, "reason_id": "stale-job", "message": "stale job",
        "count": 22736, "class": "expected", "rebuild_pending": false,
    }));
    value["rejections"]["by_phase_reason_and_message"] = json!(rows);
    value
}

#[test]
fn a_report_only_run_sets_aside_only_its_commit_deadlines_refusals() -> Result<()> {
    // #847: report-only exempts capacity, never correctness. The 20x shape
    // gates refused valid shares and unanswered submits at 0; its run sets
    // aside only the refusals its overload explains.
    let presets = presets();
    let gated = "mainnet-shape-650-addresses";
    let point = "mainnet-shape-2600-addresses";
    let gates = &presets[point].gates;
    assert_eq!(
        (
            gates.max_rejected_valid_shares,
            gates.max_unanswered_submits
        ),
        (Some(0), Some(0))
    );
    let collate_with = |point_report: &Value| -> Result<collate::Collation> {
        let dir = scratch();
        write_run(
            &dir,
            &format!("{gated}-r1"),
            "0",
            "0",
            Some(&report(&presets[gated], COMMIT)),
        );
        write_run(&dir, &format!("{point}-r1"), "0", "1", Some(point_report));
        let collation = collate::collate(
            &plan(vec![entry(gated, 1), report_only(point, 1)]),
            &dir,
            &presets,
        );
        std::fs::remove_dir_all(&dir)?;
        collation
    };

    // The commit deadline's refusals: shown against the preset's budget,
    // never folded into a pass, and they do not fail the suite.
    let collation = collate_with(&refused_at_its_commit_deadline(&presets[point]))?;
    let md = &collation.markdown;
    assert!(collation.passed, "{md}");
    assert_eq!(collation.verdict["runs"][1]["holds_suite"], true);
    assert_eq!(collation.verdict["runs"][1]["passed"], false);
    // How many were set aside is said, never left to the gates table alone.
    assert_eq!(collation.verdict["runs"][1]["refusals_set_aside"], 11513);
    assert_eq!(collation.verdict["runs"][0]["refusals_set_aside"], 0);
    assert!(
        md.contains("(11513 set aside); 1 report-only run(s)"),
        "{md}"
    );
    assert!(
        md.contains("11513 refused valid share(s) were set aside as the commit deadline's"),
        "{md}"
    );
    let gates_row = md
        .lines()
        .find(|l| l.starts_with(&format!("| `{point}` (report-only, not gated) | 0 / 1 |")))
        .unwrap_or_else(|| panic!("{md}"));
    assert!(
        gates_row.contains("warm_up: valid shares refused (10003)"),
        "{gates_row}"
    );
    assert!(
        gates_row.contains("steady_state: valid shares refused (1510)"),
        "{gates_row}"
    );
    assert!(
        md.contains("refused valid shares within budget once those answered `share was not confirmed by the database`"),
        "{md}"
    );

    let with = |edit: &dyn Fn(&mut Value)| {
        let mut value = refused_at_its_commit_deadline(&presets[point]);
        edit(&mut value);
        value
    };
    let steady = |value: &mut Value| -> usize {
        value["phases"]
            .as_array()
            .unwrap()
            .iter()
            .position(|p| p["name"] == "steady_state")
            .unwrap()
    };
    // #716/#779's class, beside the deadline's: still gated.
    let authority = with(&|value| {
        let at = steady(value);
        value["phases"][at]["rejected_valid_shares"] = json!(1512);
        value["rejections"]["by_phase_reason_and_message"]
            .as_array_mut()
            .unwrap()
            .push(refusal(
                "steady_state",
                "backend-rpc-unavailable",
                "current chain state is unavailable",
                2,
            ));
    });
    // A commit gate that closed without a proven cause: still gated.
    let gate_closed = with(&|value| {
        let at = steady(value);
        value["phases"][at]["rejected_valid_shares"] = json!(1511);
        value["rejections"]["by_phase_reason_and_message"]
            .as_array_mut()
            .unwrap()
            .push(refusal(
                "steady_state",
                "ledger-confirmation-failed",
                "share was not committed because its commit gate closed",
                1,
            ));
    });
    // A refusal the phase counts but the table does not explain.
    let unexplained = with(&|value| {
        let at = steady(value);
        value["phases"][at]["rejected_valid_shares"] = json!(1513);
    });
    // A refusal the table carries but the phase does not count.
    let uncounted = with(&|value| {
        value["rejections"]["by_phase_reason_and_message"]
            .as_array_mut()
            .unwrap()
            .push(refusal(
                "steady_state",
                "internal-error",
                "internal error",
                4,
            ));
    });
    let no_table = with(&|value| {
        value["rejections"]
            .as_object_mut()
            .unwrap()
            .remove("by_phase_reason_and_message");
    });
    // The same answer in a phase that kept its rate is no overload's: a
    // ledger failure there is gated like any other refusal.
    let kept_up = with(&|value| {
        let at = value["phases"]
            .as_array()
            .unwrap()
            .iter()
            .position(|p| p["name"] == "reconnect")
            .unwrap();
        assert_eq!(value["phases"][at]["shortfall"], 0);
        value["phases"][at]["rejected_valid_shares"] = json!(5);
        value["rejections"]["by_phase_reason_and_message"]
            .as_array_mut()
            .unwrap()
            .push(refusal(
                "reconnect",
                "ledger-confirmation-failed",
                "share was not confirmed by the database",
                5,
            ));
    });
    // A phase whose shortfall is not reported is no evidence of overload.
    let no_shortfall = with(&|value| {
        let at = steady(value);
        value["phases"][at]
            .as_object_mut()
            .unwrap()
            .remove("shortfall");
    });
    let uncounted_row = with(&|value| {
        value["rejections"]["by_phase_reason_and_message"][0]
            .as_object_mut()
            .unwrap()
            .remove("count");
    });
    let unanswered = with(&|value| {
        value["rejections"]["no_response_by_phase"] = json!({"steady_state": 1});
    });
    for (case, point_report, why) in [
        (
            "the #716 class",
            authority,
            "steady_state: valid shares refused, not counting the commit deadline's under \
             overload (2, with 1510 answered \"share was not confirmed by the database\" set aside \
             at shortfall 4874, budget 0)",
        ),
        (
            "a closed commit gate",
            gate_closed,
            "steady_state: valid shares refused, not counting the commit deadline's under overload (1,",
        ),
        (
            "unexplained by the table",
            unexplained,
            "steady_state: valid shares refused, not counting the commit deadline's under overload (3,",
        ),
        (
            "uncounted by the phase",
            uncounted,
            "steady_state: valid shares refused, not counting the commit deadline's under overload (4,",
        ),
        (
            "no rejection table",
            no_table,
            "warm_up: valid shares refused, not counting the commit deadline's under overload (not reported)",
        ),
        (
            "a ledger failure in a phase that kept up",
            kept_up,
            "reconnect: valid shares refused, not counting the commit deadline's under overload \
             (5, with 0 answered \"share was not confirmed by the database\" set aside at \
             shortfall 0, budget 0)",
        ),
        (
            "no shortfall figure",
            no_shortfall,
            "steady_state: valid shares refused, not counting the commit deadline's under \
             overload (not reported)",
        ),
        (
            "a rejection row with no count",
            uncounted_row,
            "warm_up: valid shares refused, not counting the commit deadline's under overload \
             (a rejection row not reported)",
        ),
        (
            "an unanswered submit",
            unanswered,
            "steady_state: submits unanswered (1)",
        ),
    ] {
        let collation = collate_with(&point_report)?;
        let md = &collation.markdown;
        assert!(!collation.passed, "{case}: {md}");
        assert_eq!(collation.verdict["runs"][1]["holds_suite"], false, "{case}");
        let verdict_line = md
            .lines()
            .find(|l| l.starts_with("**Suite verdict"))
            .unwrap();
        assert!(
            verdict_line.contains("a report-only run failed a check beyond its capacity budgets"),
            "{case}: {verdict_line}"
        );
        assert!(verdict_line.contains(why), "{case}: {verdict_line}");
        // Neither the deadline's refusals nor a shortfall it measured are
        // why (an unreported one fails, as it always has).
        assert!(
            !verdict_line.contains("valid shares refused (10003)"),
            "{case}: {verdict_line}"
        );
        assert_eq!(
            verdict_line.contains("shortfall (offers no session could take)"),
            case == "no shortfall figure",
            "{case}: {verdict_line}"
        );
    }
    Ok(())
}

#[test]
fn a_report_only_failure_is_named_even_when_a_gated_run_never_ran() -> Result<()> {
    let presets = presets();
    let dir = scratch();
    let gated = "mainnet-shape-650-addresses";
    let point = "mainnet-shape-2600-addresses";
    let mut lost = short_of_its_rate(&presets[point]);
    lost["durability_findings"] = json!([{"share": "lost"}]);
    write_run(&dir, &format!("{point}-r1"), "4", "1", Some(&lost));
    let collation = collate::collate(
        &plan(vec![entry(gated, 1), report_only(point, 1)]),
        &dir,
        &presets,
    )?;
    let md = &collation.markdown;
    assert!(!collation.passed);
    assert!(
        md.contains(
            "FAIL: 1 of 2 planned runs never reached a verdict; and of those that did, a \
             report-only run failed a check beyond its capacity budgets: `mainnet-shape-2600-addresses-r1`"
        ),
        "{md}"
    );
    Ok(())
}

#[test]
fn a_report_only_run_that_never_reached_its_gate_fails_the_suite() -> Result<()> {
    // A measurement that was not taken is unknown, never folded into a pass.
    let presets = presets();
    let dir = scratch();
    let gated = "mainnet-shape-650-addresses";
    let point = "mainnet-shape-2600-addresses";
    write_run(
        &dir,
        &format!("{gated}-r1"),
        "0",
        "0",
        Some(&report(&presets[gated], COMMIT)),
    );
    let collation = collate::collate(
        &plan(vec![entry(gated, 1), report_only(point, 1)]),
        &dir,
        &presets,
    )?;
    let md = &collation.markdown;
    assert!(!collation.passed, "{md}");
    assert_eq!(collation.verdict["complete"], false);
    assert_eq!(collation.verdict["reached_gate"], 1);
    assert!(
        md.contains(&format!(
            "`{point}-r1` (report-only, not gated) (no artifact"
        )),
        "{md}"
    );
    assert!(
        md.contains("FAIL: 1 of 2 planned runs never reached a verdict"),
        "{md}"
    );
    assert!(
        md.contains("Of 1: 0 reached their gate, 0 held every check above"),
        "{md}"
    );
    // A gate that could not read its inputs (exit 2) is no verdict either.
    write_run(
        &dir,
        &format!("{point}-r1"),
        "0",
        "2",
        Some(&report(&presets[point], COMMIT)),
    );
    let collation = collate::collate(
        &plan(vec![entry(gated, 1), report_only(point, 1)]),
        &dir,
        &presets,
    )?;
    assert!(!collation.passed, "{}", collation.markdown);
    assert_eq!(collation.verdict["complete"], false);
    Ok(())
}

#[test]
fn a_malformed_plan_is_refused() {
    let dir = scratch();
    let write = |value: Value| {
        let path = dir.join(format!("{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        path
    };
    let good = json!({
        "schema": PLAN_SCHEMA, "suite": "l3-full", "ref": "3.x.x", "commit": COMMIT,
        "tree": TREE, "event": "workflow_dispatch",
        "include": [{"preset": "p", "runner": "r", "timeout_minutes": 60, "repeat": 1, "id": "p-r1"}],
    });
    // A plan from before #695 has no `gated`: every run is gated.
    let old = collate::read_plan(&write(good.clone())).unwrap();
    assert!(old.include[0].gated);
    let mut ungated = good.clone();
    ungated["include"][0]["gated"] = json!(false);
    // A preset is report-only in every repeat or in none.
    // A plan that gates no run has no verdict to give.
    assert!(collate::read_plan(&write(ungated.clone())).is_err());
    let mut mixed = ungated.clone();
    mixed["include"].as_array_mut().unwrap().push(
        json!({"preset": "p", "runner": "r", "timeout_minutes": 60, "repeat": 2, "id": "p-r2"}),
    );
    assert!(collate::read_plan(&write(mixed)).is_err());
    let mut one_gated = ungated.clone();
    one_gated["include"].as_array_mut().unwrap().push(
        json!({"preset": "q", "runner": "r", "timeout_minutes": 60, "repeat": 1, "id": "q-r1"}),
    );
    assert!(!collate::read_plan(&write(one_gated)).unwrap().include[0].gated);
    for (pointer, value) in [
        ("/schema", json!("qbit.prism.l3-plan.v0")),
        ("/tree", json!("HEAD^{tree}")),
        ("/include/0/id", json!("p-r2")),
        ("/include", json!([])),
        ("/include/0/gated", json!("no")),
        ("/unexpected", json!(1)),
    ] {
        let mut bad = good.clone();
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        bad.pointer_mut(if parent.is_empty() { "" } else { parent })
            .unwrap()[key] = value;
        assert!(collate::read_plan(&write(bad)).is_err(), "{pointer}");
    }
    let mut twice = good.clone();
    let first = twice["include"][0].clone();
    twice["include"].as_array_mut().unwrap().push(first);
    assert!(collate::read_plan(&write(twice)).is_err());
    // A plan naming a preset that is not checked in cannot be collated.
    let unknown = plan(vec![entry("no-such-preset", 1)]);
    assert!(collate::collate(&unknown, &dir, &presets()).is_err());
    // collate() holds a plan it is handed to the same gating rules.
    let name = "mainnet-shape-650-addresses";
    let mixed = plan(vec![entry(name, 1), report_only(name, 2)]);
    assert!(collate::collate(&mixed, &dir, &presets()).is_err());
    let none_gated = plan(vec![report_only(name, 1)]);
    assert!(collate::collate(&none_gated, &dir, &presets()).is_err());
    // #847: a preset that budgets neither refused valid shares nor
    // unanswered submits is never report-only.
    let unbudgeted = plan(vec![
        entry("mainnet-shape-2600-addresses", 1),
        report_only(name, 1),
    ]);
    let error = collate::collate(&unbudgeted, &dir, &presets())
        .err()
        .expect("a preset with null correctness budgets was collated report-only");
    assert!(
        error
            .to_string()
            .contains("exempts capacity, never correctness"),
        "{error}"
    );
    // A soak's own gates and a D1 cell's verdict are never report-only.
    for always_gated in ["soak-smoke", "throughput-400k-window-2fe-async"] {
        let the_plan = plan(vec![entry(name, 1), report_only(always_gated, 1)]);
        let error = collate::collate(&the_plan, &dir, &presets())
            .err()
            .unwrap_or_else(|| panic!("{always_gated} was collated report-only"));
        assert!(
            error.to_string().contains("always gated"),
            "{always_gated}: {error}"
        );
    }
}
