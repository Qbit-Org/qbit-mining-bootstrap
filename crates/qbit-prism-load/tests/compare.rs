//! The A/B comparison (#511): #473's D1 rule over two builds' repeats.

use anyhow::Result;
use qbit_prism_load::compare::{self, LoadedRun, Manifest};
use qbit_prism_load::gate::Budgets;
use qbit_prism_load::preset::{presets_dir, Preset};
use serde_json::{json, Value};
use std::path::Path;

const BASE: &str = "5d0042f629f3271ef6743ec9d859cb0c35018f31";
const CANDIDATE: &str = "8fc94faf00000000000000000000000000000000";

/// One phase as the side report carries it.
/// One phase as the side report carries it; its length, artifact
/// membership and database delay are the D1 plan's for a phase of that
/// name, as a run of the preset reports them.
fn phase(name: &str, target: f64, achieved: f64, shortfall: u64, rejected: u64, p99: f64) -> Value {
    let mut phase = plain_phase(name, target, achieved, shortfall, rejected, p99);
    if let Some(plan) = compare::expected_phases(&d1_args())
        .unwrap()
        .into_iter()
        .find(|plan| plan.name == name)
    {
        phase["duration_seconds"] = json!(plan.seconds as f64 + 0.004);
        phase["in_artifact"] = json!(plan.in_artifact);
        phase["database_delay_milliseconds_configured"] = json!(plan.database_delay_ms);
    }
    phase
}

fn plain_phase(
    name: &str,
    target: f64,
    achieved: f64,
    shortfall: u64,
    rejected: u64,
    p99: f64,
) -> Value {
    json!({
        "name": name,
        "target_rate_shares_per_second": target,
        "duration_seconds": 300.004,
        "offered_rate_shares_per_second": achieved,
        "achieved_rate_shares_per_second": achieved,
        "shortfall": shortfall,
        "rejected_valid_shares": rejected,
        "client_ack_latency": {"p50": 2.5, "p99": p99},
        "order_lock": {"max_waiters": 15, "mean_waiters": 13.4, "sample_interval_milliseconds": 10.0},
        "processes": [{"instance_id": "load-fe-0", "sample_interval_seconds": 1.0}],
        "reconciliation": {"missing": 0, "unexpected": 0},
        "min_mem_available_kib": 30_000 * 1024,
        "in_artifact": true,
        "database_delay_milliseconds_configured": 0,
        "scheduled_blocks": 0,
        "frontend_restarts": 0,
    })
}

fn report(commit: &str, steady: Value, burst: Value) -> Value {
    let mut report = bare_report(commit, steady, burst);
    // The realism and churn settings as this harness reports the D1 preset.
    for setting in compare::expected_settings(&d1_args()).unwrap() {
        set_pointer(&mut report, setting.pointer, setting.value);
    }
    report
}

/// Set `pointer` in `target`, creating objects on the way; a numeric key
/// indexes an existing array.
fn set_pointer(target: &mut Value, pointer: &str, value: Value) {
    let mut node = target;
    let keys: Vec<&str> = pointer.trim_start_matches('/').split('/').collect();
    for key in &keys[..keys.len() - 1] {
        node = match node {
            Value::Array(items) => &mut items[key.parse::<usize>().unwrap()],
            _ => node
                .as_object_mut()
                .unwrap()
                .entry(key.to_string())
                .or_insert_with(|| json!({})),
        };
    }
    let last = keys[keys.len() - 1];
    match node {
        Value::Array(items) => items[last.parse::<usize>().unwrap()] = value,
        _ => node[last] = value,
    }
}

/// A met phase at 500 shares/s with `delay_ms` of configured database delay.
/// A met phase at the D1 plan's rate for it.
fn planned(name: &str) -> Value {
    let rate = compare::expected_phases(&d1_args())
        .unwrap()
        .into_iter()
        .find(|plan| plan.name == name)
        .map_or(500.0, |plan| plan.rate);
    phase(name, rate, rate - 0.003, 0, 0, 30.0)
}

fn bare_report(commit: &str, steady: Value, burst: Value) -> Value {
    json!({
        "dirty": false,
        "durability_findings": [],
        "versions": {"coordinator_revision": commit},
        "validator": {"ack_p99_limit_used_milliseconds": 1000.0, "forecast_used": 2000.0},
        "topology": {"frontends": 1, "sessions": 2000, "max_outstanding_per_session": 1, "plan": "d1"},
        "frontend_environment": [{"instance_id": "load-fe-0", "environment": {
            "PRISM_RUNTIME_WORKERS": "2",
            "PRISM_DATABASE_MAX_CONNECTIONS": "16",
            "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS": "15",
            "PRISM_BLOCKPOLL_SECONDS": "2",
            "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS": "2016",
        }}],
        "window": {
            "requested_window_shares": 20000,
            "computed_window_shares": 20000,
            "ledger_window_shares_at_start": 20000,
            "seed": {"target_share_bytes": 581},
        },
        "time_to_usable_work": {"tips": [
            {"all_sessions_milliseconds": 600.0},
            {"all_sessions_milliseconds": 700.0},
            {"all_sessions_milliseconds": 800.0},
        ]},
        "database": {"mode": "managed", "replication": {"declared": "async", "agreed_with_declared": true}},
        "rejections": {"no_response_by_phase": {}, "by_phase_reason_and_message": []},
        // The D1 plan's other phases, after the two the tests vary.
        "phases": [steady, burst, planned("warm_up"), planned("reconnect"), planned("slow_database")],
        "reconnects": {"by_phase": [{"phase": "reconnect", "completed": 13}]},
    })
}

fn met_steady() -> Value {
    phase("steady_state", 500.0, 499.997, 0, 0, 30.0)
}

fn burst() -> Value {
    phase("burst", 2000.0, 560.0, 80_000, 0, 6000.0)
}

fn manifest(fdatasync_usecs: f64) -> Manifest {
    let mut runs = Vec::new();
    for (repeat, label) in compare_order() {
        runs.push(json!({
            "id": format!("throughput-20k-window-1fe-{label}-r{repeat}"),
            "build": label,
            "repeat": repeat,
            "dir": format!("runs/throughput-20k-window-1fe-{label}-r{repeat}"),
            "exit_code": 0,
            "ceiling_hit": false,
            "load_before": 0.8,
            "load_max": 6.1,
            "mem_available_min_mib": 30_000,
        }));
    }
    serde_json::from_value(json!({
        "schema": compare::MANIFEST_SCHEMA,
        "preset": {"name": "throughput-20k-window-1fe", "sha256": "ab"},
        "host": {"hostname": "ref", "nproc": 22, "mem_total_mib": 41_000, "kernel": "6.8.0"},
        "pg_test_fsync": {
            "before": {"fdatasync_usecs_per_op": fdatasync_usecs},
            "after": {"fdatasync_usecs_per_op": fdatasync_usecs},
        },
        "builds": [
            {"label": "base", "ref": "5d0042f6", "commit": BASE, "dropped_legacy_flags": ["--seed"]},
            {"label": "candidate", "ref": "3.x.x", "commit": CANDIDATE},
        ],
        "runs": runs,
    }))
    .unwrap()
}

fn compare_order() -> Vec<(u32, &'static str)> {
    vec![
        (1, "base"),
        (1, "candidate"),
        (2, "candidate"),
        (2, "base"),
        (3, "base"),
        (3, "candidate"),
    ]
}

/// Every run met in `steady_state`, except where `steady` says otherwise.
fn loaded(manifest: &Manifest, steady: impl Fn(&str, u32) -> Value) -> Vec<LoadedRun> {
    manifest
        .runs
        .iter()
        .map(|run| {
            let commit = if run.build == "base" { BASE } else { CANDIDATE };
            LoadedRun {
                run: run.clone(),
                report: Some(report(commit, steady(&run.build, run.repeat), burst())),
                excluded: None,
            }
        })
        .collect()
}

/// The D1 preset's pinned flags.
fn d1_args() -> std::collections::BTreeMap<String, Value> {
    Preset::load(&presets_dir().join("throughput-20k-window-1fe.json"))
        .unwrap()
        .args
}

/// The D1 preset's gates: `steady_state` only, #473's rule, tips ungated.
fn d1_budgets() -> Budgets {
    let preset = Preset::load(&presets_dir().join("throughput-20k-window-1fe.json")).unwrap();
    Budgets::from(&preset.gates)
}

#[test]
fn both_builds_meeting_the_rule_pass_and_the_ungated_burst_does_not_fail_it() -> Result<()> {
    let manifest = manifest(288.0);
    let runs = loaded(&manifest, |_, _| met_steady());
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args())?;
    assert!(result.passed, "{}", result.markdown);
    assert!(result.markdown.contains("### Verdict: PASS"));
    assert!(result.markdown.contains("**met** (3 of 3)"));
    assert!(result
        .markdown
        .contains("### `burst` (reported, not gated)"));
    assert!(result.markdown.contains("the reference flush class"));
    assert!(result.markdown.contains("predates `--seed`"));
    Ok(())
}

#[test]
fn a_candidate_shortfall_the_base_does_not_have_is_a_regression() -> Result<()> {
    let manifest = manifest(288.0);
    // #479 STEP 1's shape: the base holds 500/s, the head places 420-475.
    let runs = loaded(&manifest, |build, repeat| match build {
        "candidate" => phase(
            "steady_state",
            500.0,
            [420.787, 470.767, 475.127][repeat as usize - 1],
            [23_763, 8_769, 7_461][repeat as usize - 1],
            0,
            7_394.0,
        ),
        _ => met_steady(),
    });
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args())?;
    assert!(!result.passed);
    assert!(
        result.markdown.contains("**regression**"),
        "{}",
        result.markdown
    );
    assert!(result.markdown.contains("23,763 / 8,769 / 7,461"));
    assert!(result.markdown.contains("470.8 (420.8–475.1)"));
    assert!(result.markdown.contains("ACK p99 7,394 ms over 1,000 ms"));
    Ok(())
}

#[test]
fn an_ack_p99_over_the_limit_or_a_refused_valid_share_fails_the_rule() {
    let manifest = manifest(288.0);
    for bad in [
        phase("steady_state", 500.0, 499.997, 0, 0, 1000.5),
        phase("steady_state", 500.0, 499.997, 0, 2, 30.0),
    ] {
        let runs = loaded(&manifest, |build, repeat| {
            if build == "candidate" && repeat == 2 {
                bad.clone()
            } else {
                met_steady()
            }
        });
        let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
        assert!(!result.passed, "{}", result.markdown);
    }
}

#[test]
fn a_run_that_did_not_exit_0_is_outside_the_medians_and_fails_its_build() -> Result<()> {
    let mut manifest = manifest(288.0);
    manifest.runs[1].exit_code = Some(3);
    let base_dir = tempdir("exit3");
    write_reports(&manifest, &base_dir);
    let runs = compare::load_runs(&manifest, &base_dir)?;
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args())?;
    assert!(!result.passed);
    assert!(result.markdown.contains("2 / 3"));
    assert!(result
        .markdown
        .contains("throughput-20k-window-1fe-candidate-r1 (exit 3) is outside the medians"));
    Ok(())
}

#[test]
fn a_failed_runs_reduced_report_fails_its_build_instead_of_the_comparison() -> Result<()> {
    let mut manifest = manifest(288.0);
    manifest.runs[1].exit_code = Some(2);
    manifest.runs[3].ceiling_hit = true;
    manifest.runs[3].exit_code = None;
    let base_dir = tempdir("failure-report");
    write_reports(&manifest, &base_dir);
    // `report::write_failure`'s reduced report has no `versions` block, and
    // a run killed at the ceiling may leave a truncated one.
    std::fs::write(
        base_dir
            .join(&manifest.runs[1].dir)
            .join("load-harness-report.json"),
        json!({"failed": "the cluster did not start"}).to_string(),
    )?;
    std::fs::write(
        base_dir
            .join(&manifest.runs[3].dir)
            .join("load-harness-report.json"),
        "{\"phases\": [",
    )?;
    let runs = compare::load_runs(&manifest, &base_dir)?;
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args())?;
    assert!(!result.passed);
    assert!(result.markdown.contains("(exit 2) is outside the medians"));
    assert!(result
        .markdown
        .contains("(killed at the run ceiling) is outside the medians"));
    Ok(())
}

#[test]
fn a_candidate_failing_the_presets_own_tip_budget_fails_though_the_d1_rule_holds() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in &mut runs {
        let slowest = if run.run.build == "candidate" {
            9000.0
        } else {
            800.0
        };
        run.report.as_mut().unwrap()["time_to_usable_work"] = json!({"tips": [
            {"all_sessions_milliseconds": slowest},
            {"all_sessions_milliseconds": slowest},
            {"all_sessions_milliseconds": slowest},
        ]});
    }
    let budgets = Budgets {
        tip_last_notify_p99_ms: Some(2500.0),
        ..d1_budgets()
    };
    let result = compare::compare(&manifest, &runs, &budgets, &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result.markdown.contains("| base | 3 / 3 | – |"));
    assert!(result.markdown.contains("| candidate | 0 / 3 |"));
    assert!(result
        .markdown
        .contains("the preset's own gates: **regression**"));
}

#[test]
fn builds_reporting_different_target_rates_fail_the_comparison() {
    let manifest = manifest(288.0);
    let runs = loaded(&manifest, |build, _| match build {
        "candidate" => phase("steady_state", 400.0, 399.997, 0, 0, 30.0),
        _ => met_steady(),
    });
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("ran `steady_state` at 400 shares/s, not the planned 500"));
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[2].report.as_mut().unwrap()["phases"][0]["target_rate_shares_per_second"] = Value::Null;
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("reports no target rate for `steady_state`"));
}

#[test]
fn a_different_target_in_a_phase_outside_d1_fails_the_comparison() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in &mut runs {
        let background = if run.run.build == "candidate" {
            50.0
        } else {
            133.0
        };
        for phase in run.report.as_mut().unwrap()["phases"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .filter(|phase| phase["name"] == "warm_up")
        {
            phase["target_rate_shares_per_second"] = json!(background);
        }
    }
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result
        .markdown
        .contains("ran `warm_up` at 133 shares/s, not the planned 500"));
}

#[test]
fn a_build_running_a_phase_for_less_time_fails_though_every_rate_matches() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in runs.iter_mut().filter(|r| r.run.build == "candidate") {
        run.report.as_mut().unwrap()["phases"][0]["duration_seconds"] = json!(60.002);
    }
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result.markdown.contains("for 60 s, not the planned 300 s"));
    // Clock jitter between equal plans is not a different workload.
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["phases"][0]["duration_seconds"] = json!(301.2);
    assert!(
        compare::compare(&manifest, &runs, &d1_budgets(), &d1_args())
            .unwrap()
            .passed
    );
}

#[test]
fn a_build_held_to_a_limit_other_than_the_pinned_one_fails() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    // A candidate p99 of 5 s passes its own 10 s limit; the preset pins 1 s.
    for run in runs.iter_mut().filter(|r| r.run.build == "candidate") {
        let report = run.report.as_mut().unwrap();
        report["validator"]["ack_p99_limit_used_milliseconds"] = json!(10_000.0);
        report["phases"][0]["client_ack_latency"]["p99"] = json!(5_000.0);
    }
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result
        .markdown
        .contains("ran `--ack-p99-limit-ms` 10000.0, not 1000"));
}

#[test]
fn a_build_running_a_smaller_topology_than_the_preset_pins_fails() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in runs.iter_mut().filter(|r| r.run.build == "candidate") {
        run.report.as_mut().unwrap()["topology"]["sessions"] = json!(200);
    }
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result.markdown.contains("ran `--sessions` 200, not 2000"));
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[0].report.as_mut().unwrap()["database"] = json!({});
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("does not report `/database/replication/declared`"));
}

#[test]
fn a_preset_gating_every_phase_gates_only_the_d1_phases_that_ran() {
    // `pr-smoke`'s shape: `phases: null` and no burst phase.
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in &mut runs {
        run.report.as_mut().unwrap()["phases"]
            .as_array_mut()
            .unwrap()
            .retain(|phase| phase["name"] != "burst");
    }
    let budgets = Budgets {
        phases: None,
        ..d1_budgets()
    };
    // The short plan runs no burst without --burst-seconds.
    let mut short = d1_args();
    short.insert("--plan".into(), json!("short"));
    short.insert("--burst-seconds".into(), Value::Null);
    short.insert("--burst-rate".into(), Value::Null);
    for run in &mut runs {
        run.report.as_mut().unwrap()["topology"]["plan"] = json!("short");
    }
    let result = compare::compare(&manifest, &runs, &budgets, &short).unwrap();
    assert!(result.passed, "{}", result.markdown);
    assert!(!result.markdown.contains("### `burst`"));
    // Under the D1 plan the same runs leave out a planned phase.
    let result = compare::compare(&manifest, &runs, &budgets, &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("does not report the phase the preset plans"));
}

#[test]
fn more_submits_in_flight_or_another_server_setting_than_pinned_fails() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["topology"]["max_outstanding_per_session"] = json!(4);
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("ran `--max-outstanding-per-session` 4, not 1"));
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[3].report.as_mut().unwrap()["frontend_environment"][0]["environment"]
        ["PRISM_DATABASE_MAX_CONNECTIONS"] = json!("64");
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("`PRISM_DATABASE_MAX_CONNECTIONS=64`, not the pinned `--db-max-connections` 16"));
}

#[test]
fn a_build_reporting_another_arrival_or_churn_than_the_preset_fails() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["arrival"] = json!("bursty:cv1=0.5,cv60=0.3,max=8");
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result.markdown.contains("reports `/arrival`"));
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["churn"] = json!({"ran": true});
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result.markdown.contains("reports `/churn/ran` true"));
}

#[test]
fn a_build_that_predates_a_setting_is_exempt_from_it() {
    // The manifest's base predates `--seed` (dropped as legacy): its reports
    // carry no seed or churn block, and that is not a mismatch.
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in runs.iter_mut().filter(|r| r.run.build == "base") {
        let report = run.report.as_mut().unwrap();
        report["population"].as_object_mut().unwrap().remove("seed");
        report.as_object_mut().unwrap().remove("churn");
    }
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(result.passed, "{}", result.markdown);
    // The candidate, which did not drop it, must still report it.
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["population"]
        .as_object_mut()
        .unwrap()
        .remove("seed");
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("does not report `/population/seed`"));
}

#[test]
fn fewer_tips_or_a_smaller_seeded_share_than_pinned_fails() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["time_to_usable_work"]["tips"]
        .as_array_mut()
        .unwrap()
        .truncate(1);
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result.markdown.contains(
        "reports 1 entries at `/time_to_usable_work/tips`, not the pinned `--external-tips` 3"
    ));
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["window"]["seed"]["target_share_bytes"] = json!(100);
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("ran `--seed-share-bytes` 100, not 581"));
}

#[test]
fn a_build_skipping_a_phases_blocks_or_database_delay_fails() {
    let manifest = manifest(288.0);
    // Both builds schedule two blocks in `steady_state`; one candidate run
    // schedules none.
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in &mut runs {
        run.report.as_mut().unwrap()["phases"][0]["scheduled_blocks"] = json!(2);
    }
    runs[3].report.as_mut().unwrap()["phases"][0]["scheduled_blocks"] = json!(0);
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result.markdown.contains("`scheduled_blocks` is 2 in"));
    // A slow_database phase run without the pinned 10 ms delay.
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in &mut runs {
        for phase in run.report.as_mut().unwrap()["phases"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .filter(|phase| phase["name"] == "slow_database")
        {
            phase["database_delay_milliseconds_configured"] = json!(0);
        }
    }
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("with a 0 ms delay, not the pinned `--slow-db-delay-ms` 10"));
}

#[test]
fn fewer_completed_reconnects_than_pinned_fails() {
    let manifest = manifest(288.0);
    let with_reconnects = |completed: u64| {
        let mut runs = loaded(&manifest, |_, _| met_steady());
        runs[1].report.as_mut().unwrap()["reconnects"]["by_phase"][0]["completed"] =
            json!(completed);
        compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap()
    };
    assert!(with_reconnects(13).passed);
    let result = with_reconnects(1);
    assert!(!result.passed);
    assert!(result.markdown.contains(
        "completed 1 reconnects in `reconnect`, fewer than the pinned `--reconnect-target` 12"
    ));
}

#[test]
fn a_build_seeding_a_smaller_window_than_it_reports_requesting_fails() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in runs.iter_mut().filter(|r| r.run.build == "candidate") {
        run.report.as_mut().unwrap()["window"]["ledger_window_shares_at_start"] = json!(2000);
    }
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result
        .markdown
        .contains("ran `--window-shares` 2000, not 20000"));
}

#[test]
fn scheduled_blocks_are_held_to_the_pinned_count_even_when_every_run_agrees() {
    // mainnet-shape-* pins two blocks; every run of both builds reporting
    // none agrees with itself and is still the wrong workload.
    let manifest = manifest(288.0);
    let mut pinned = d1_args();
    pinned.insert("--scheduled-blocks".into(), json!(2));
    let runs = loaded(&manifest, |_, _| met_steady());
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &pinned).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("scheduled 0 own blocks, not the pinned `--scheduled-blocks` 2"));
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in &mut runs {
        run.report.as_mut().unwrap()["phases"][0]["scheduled_blocks"] = json!(2);
    }
    assert!(
        compare::compare(&manifest, &runs, &d1_budgets(), &pinned)
            .unwrap()
            .passed
    );
}

#[test]
fn a_run_that_fell_under_the_pinned_memory_floor_fails() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["phases"][1]["min_mem_available_kib"] = json!(4096 * 1024);
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result.markdown.contains(
        "lowest MemAvailable of 4096 MiB, under the pinned `--min-mem-available-mib` 6144"
    ));
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["phases"][0]["min_mem_available_kib"] = Value::Null;
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("lowest MemAvailable of nothing it read"));
}

#[test]
fn a_preset_the_harness_would_refuse_is_refused_and_dense_gaps_are_pinned() {
    // No sessions: the harness's own entry checks refuse it.
    let mut refused = d1_args();
    refused.insert("--sessions".into(), json!(0));
    assert!(compare::expected_settings(&refused).is_err());
    // Under the dense cadence the gap pattern the report states is pinned.
    let mut dense = d1_args();
    dense.insert("--cadence".into(), json!("dense"));
    dense.insert("--scheduled-blocks".into(), json!(15));
    let settings = compare::expected_settings(&dense).expect("a valid dense preset");
    let gaps = settings
        .iter()
        .find(|s| s.pointer == "/dense_cadence/gap_pattern_seconds")
        .expect("the gap pattern is expected under dense");
    assert!(compare::same_json(&gaps.value, &json!([9, 19, 9, 18, 20])));
    // Otherwise the report must say the dense phase did not run.
    let settings = compare::expected_settings(&d1_args()).unwrap();
    assert!(settings.iter().any(|s| s.pointer == "/dense_cadence/ran"));
}

#[test]
fn a_build_driving_another_node_than_pinned_fails() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["node"]["chain_reconciliation"] = json!({});
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result
        .markdown
        .contains("drove a real qbitd, not the pinned `--node` fake"));
}

/// The D1 preset with pr-smoke's churn phase after it.
fn d1_with_churn() -> std::collections::BTreeMap<String, Value> {
    let mut pinned = d1_args();
    for (flag, value) in [
        ("--churn-seconds", json!(30)),
        ("--churn-rate", json!(20)),
        ("--churn-tips", json!(2)),
        ("--rental-bursts", json!("20,40")),
        ("--rental-burst-window-seconds", json!(4)),
        ("--rental-burst-interval-seconds", json!(10)),
        ("--rental-lifetime", json!("pareto:xm=5,alpha=1.2,max=60")),
        ("--rental-hashrate", json!(5)),
        ("--reconnect-storms", json!("0.3")),
        ("--storm-interval-seconds", json!(20)),
        ("--storm-reconnect-seconds", json!(3)),
    ] {
        pinned.insert(flag.into(), value);
    }
    pinned
}

/// Runs of `pinned` whose reports carry its churn phase, settings and
/// plan, with the plan fully realised.
fn churn_runs(
    manifest: &Manifest,
    pinned: &std::collections::BTreeMap<String, Value>,
) -> Vec<LoadedRun> {
    let mut runs = loaded(manifest, |_, _| met_steady());
    let settings = compare::expected_settings(pinned).unwrap();
    let churn_phase = compare::expected_phases(pinned)
        .unwrap()
        .into_iter()
        .find(|plan| plan.name == "churn")
        .expect("a churn phase is planned");
    let plan = settings
        .iter()
        .find(|s| s.pointer == "/churn/plan")
        .unwrap()
        .value
        .clone();
    for run in &mut runs {
        let report = run.report.as_mut().unwrap();
        for setting in &settings {
            set_pointer(report, setting.pointer, setting.value.clone());
        }
        let mut phase = plain_phase("churn", churn_phase.rate, churn_phase.rate, 0, 0, 30.0);
        phase["duration_seconds"] = json!(churn_phase.seconds as f64);
        phase["in_artifact"] = json!(churn_phase.in_artifact);
        phase["database_delay_milliseconds_configured"] = json!(churn_phase.database_delay_ms);
        report["phases"].as_array_mut().unwrap().push(phase);
        // Each planned storm, run over 600 connected sessions.
        let storms: Vec<Value> = plan["storms"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(index, storm)| {
                let fraction = storm["fraction"].as_f64().unwrap();
                json!({"index": index, "at_seconds": storm["at_seconds"], "fraction": fraction,
                       "connected": 600, "dropped": (fraction * 600.0).round() as u64})
            })
            .collect();
        let dropped: u64 = storms.iter().map(|s| s["dropped"].as_u64().unwrap()).sum();
        report["churn"]["realised"] = json!({
            "reconnects_completed": dropped,
            "rentals_spawned": plan["rental_sessions"],
            "rentals_departed": plan["rentals_departing_in_phase"],
            "storms": storms,
        });
        report["churn"]["tip_delivery"] = json!({"tips": plan["tips_at_seconds"]});
    }
    runs
}

#[test]
fn a_churn_phase_that_spawned_fewer_rentals_than_planned_fails() {
    let manifest = manifest(288.0);
    let pinned = d1_with_churn();
    let runs = churn_runs(&manifest, &pinned);
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &pinned).unwrap();
    assert!(result.passed, "{}", result.markdown);
    for (pointer, short) in [
        ("/churn/realised/rentals_spawned", json!(1)),
        ("/churn/realised/rentals_departed", json!(0)),
        ("/churn/realised/reconnects_completed", json!(0)),
        ("/churn/realised/storms", json!([])),
        ("/churn/tip_delivery/tips", json!([{}])),
        ("/churn/realised/storms/0/dropped", json!(0)),
        ("/churn/realised/storms/0/dropped", json!(5)),
    ] {
        let mut runs = churn_runs(&manifest, &pinned);
        set_pointer(runs[1].report.as_mut().unwrap(), pointer, short);
        let result = compare::compare(&manifest, &runs, &d1_budgets(), &pinned).unwrap();
        assert!(!result.passed, "{pointer}");
        // A missing record fails on the count, a short storm on its drop.
        let why = if pointer.ends_with("/dropped") {
            "not the plan's 0.3 of them"
        } else if pointer.ends_with("/reconnects_completed") {
            "reconnects after its storms dropped"
        } else {
            "the preset's churn plan holds"
        };
        assert!(result.markdown.contains(why), "{pointer}");
    }
}

#[test]
fn the_preflight_refuses_a_preset_that_gates_no_d1_phase() {
    let validate = |name: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_qbit-prism-load-compare"))
            .args(["--validate-preset", "--preset"])
            .arg(presets_dir().join(format!("{name}.json")))
            .output()
            .unwrap()
    };
    assert_eq!(validate("throughput-20k-window-1fe").status.code(), Some(0));
    for name in ["pr-smoke", "rental-churn-bursts-and-storms"] {
        let output = validate(name);
        assert_eq!(output.status.code(), Some(2), "{name}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("gates none of the D1 phases"));
    }
}

#[test]
fn another_database_or_fewer_launched_frontends_than_pinned_fails() {
    let manifest = manifest(288.0);
    for (pointer, value, why) in [
        (
            "/database/mode",
            json!("external"),
            "ran against a external database",
        ),
        (
            "/database/replication/agreed_with_declared",
            json!(false),
            "observed replication agreeing",
        ),
        ("/frontend_environment", json!([]), "launched 0 frontends"),
    ] {
        let mut runs = loaded(&manifest, |_, _| met_steady());
        set_pointer(runs[1].report.as_mut().unwrap(), pointer, value);
        let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
        assert!(!result.passed, "{pointer}");
        assert!(
            result.markdown.contains(why),
            "{pointer}: {}",
            result.markdown
        );
    }
}

#[test]
fn samplers_at_another_interval_than_pinned_fail() {
    let manifest = manifest(288.0);
    for (pointer, value, why) in [
        (
            "/phases/0/order_lock/sample_interval_milliseconds",
            json!(100.0),
            "sampled the ORDER lock every 100 ms",
        ),
        (
            "/phases/0/processes/0/sample_interval_seconds",
            json!(5.0),
            "`--process-sample-interval-ms` 1000",
        ),
        (
            "/phases/0/processes",
            Value::Null,
            "reports no sampler intervals",
        ),
        (
            "/phases/0/processes",
            json!([]),
            "reports no sampler intervals",
        ),
        // Outside `steady_state` too: every phase reports them.
        (
            "/phases/1/order_lock/sample_interval_milliseconds",
            json!(20.0),
            "every 20 ms in `burst`",
        ),
        (
            "/phases/1/order_lock",
            json!({"max_waiters": 15, "mean_waiters": 13.4}),
            "reports no sampler intervals for `burst`",
        ),
        // Every launched frontend is sampled, and only those.
        (
            "/phases/0/processes/0/instance_id",
            json!("load-fe-9"),
            "sampled load-fe-9 in `steady_state`, not its launched frontends load-fe-0",
        ),
        (
            "/phases/2/processes",
            json!([
                {"instance_id": "load-fe-0", "sample_interval_seconds": 1.0},
                {"instance_id": "load-fe-0", "sample_interval_seconds": 1.0},
            ]),
            "sampled load-fe-0, load-fe-0 in `warm_up`",
        ),
    ] {
        let mut runs = loaded(&manifest, |_, _| met_steady());
        set_pointer(runs[1].report.as_mut().unwrap(), pointer, value);
        let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
        assert!(!result.passed, "{pointer}");
        assert!(
            result.markdown.contains(why),
            "{pointer}: {}",
            result.markdown
        );
    }
    // Each pinned interval is held on its own.
    let mut pinned = d1_args();
    pinned.remove("--lock-sample-interval-ms");
    let mut runs = loaded(&manifest, |_, _| met_steady());
    set_pointer(
        runs[1].report.as_mut().unwrap(),
        "/phases/0/processes/0/sample_interval_seconds",
        json!(5.0),
    );
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &pinned).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("`--process-sample-interval-ms` 1000"));
}

/// A met phase as `pinned` plans it.
fn planned_in(pinned: &std::collections::BTreeMap<String, Value>, name: &str) -> Value {
    let plan = compare::expected_phases(pinned)
        .unwrap()
        .into_iter()
        .find(|plan| plan.name == name)
        .unwrap();
    let mut phase = plain_phase(name, plan.rate, plan.rate - 0.003, 0, 0, 30.0);
    phase["duration_seconds"] = json!(plan.seconds as f64 + 0.004);
    phase["in_artifact"] = json!(plan.in_artifact);
    phase["database_delay_milliseconds_configured"] = json!(plan.database_delay_ms);
    phase
}

#[test]
fn a_planned_restart_or_kill_is_held_to_the_preset_even_when_every_run_agrees() {
    let manifest = manifest(288.0);
    // With two frontends the reconnect phase drains and restarts one.
    let mut pinned = d1_args();
    pinned.insert("--frontends".into(), json!(2));
    let two_frontends = |restarts: u64| {
        let mut runs = loaded(&manifest, |_, _| met_steady());
        for run in &mut runs {
            let report = run.report.as_mut().unwrap();
            report["topology"]["frontends"] = json!(2);
            let mut second = report["frontend_environment"][0].clone();
            second["instance_id"] = json!("load-fe-1");
            report["frontend_environment"]
                .as_array_mut()
                .unwrap()
                .push(second);
            for phase in report["phases"].as_array_mut().unwrap() {
                let mut sampler = phase["processes"][0].clone();
                sampler["instance_id"] = json!("load-fe-1");
                phase["processes"].as_array_mut().unwrap().push(sampler);
            }
            report["phases"][3]["frontend_restarts"] = json!(restarts);
        }
        compare::compare(&manifest, &runs, &d1_budgets(), &pinned).unwrap()
    };
    let result = two_frontends(1);
    assert!(result.passed, "{}", result.markdown);
    // One frontend's sampler left out fails, though the other ran.
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in &mut runs {
        let report = run.report.as_mut().unwrap();
        report["topology"]["frontends"] = json!(2);
        let mut second = report["frontend_environment"][0].clone();
        second["instance_id"] = json!("load-fe-1");
        report["frontend_environment"]
            .as_array_mut()
            .unwrap()
            .push(second);
        report["phases"][3]["frontend_restarts"] = json!(1);
    }
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &pinned).unwrap();
    assert!(!result.passed);
    assert!(
        result.markdown.contains(
            "sampled load-fe-0 in `steady_state`, not its launched frontends load-fe-0, load-fe-1"
        ),
        "{}",
        result.markdown
    );
    let result = two_frontends(0);
    assert!(!result.passed);
    assert!(
        result
            .markdown
            .contains("restarted a frontend 0 times in `reconnect`, not the planned 1"),
        "{}",
        result.markdown
    );
    // One frontend alone is never restarted.
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in &mut runs {
        run.report.as_mut().unwrap()["phases"][3]["frontend_restarts"] = json!(1);
    }
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("restarted a frontend 1 times in `reconnect`, not the planned 0"));
    // The mid-flight kill's relaunch is its phase's restart, and the report
    // says the kill ran.
    let mut pinned = d1_args();
    pinned.insert("--mid-flight-kill".into(), json!(true));
    let with_kill = |restarts: u64, ran: bool| {
        let mut runs = loaded(&manifest, |_, _| met_steady());
        for run in &mut runs {
            let report = run.report.as_mut().unwrap();
            let mut kill = planned_in(&pinned, "mid_flight_kill");
            kill["frontend_restarts"] = json!(restarts);
            report["phases"].as_array_mut().unwrap().push(kill);
            report["mid_flight_kill"]["ran"] = json!(ran);
        }
        compare::compare(&manifest, &runs, &d1_budgets(), &pinned).unwrap()
    };
    let result = with_kill(1, true);
    assert!(result.passed, "{}", result.markdown);
    let result = with_kill(0, true);
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("restarted a frontend 0 times in `mid_flight_kill`, not the planned 1"));
    let result = with_kill(1, false);
    assert!(!result.passed);
    assert!(
        result.markdown.contains("`/mid_flight_kill/ran` false"),
        "{}",
        result.markdown
    );
}

#[test]
fn the_dense_landing_budget_is_held_to_the_pinned_scheduled_blocks() {
    let manifest = manifest(288.0);
    let mut pinned = d1_args();
    pinned.insert("--cadence".into(), json!("dense"));
    let slots = qbit_prism_load::cadence::landing_offsets(&[9.0, 19.0, 9.0, 18.0, 20.0], 240.0)
        .len() as u64;
    assert_eq!(slots, 15, "the D1 preset's gaps in its 240 s phase");
    let dense = |pinned_blocks: u64, budget: u64, landed: u64| {
        let mut pinned = pinned.clone();
        pinned.insert("--scheduled-blocks".into(), json!(pinned_blocks));
        let mut runs = loaded(&manifest, |_, _| met_steady());
        for run in &mut runs {
            let report = run.report.as_mut().unwrap();
            let mut phase = planned_in(&pinned, "dense_cadence");
            phase["scheduled_blocks"] = json!(landed);
            report["phases"].as_array_mut().unwrap().push(phase);
            report["dense_cadence"] = json!({
                "ran": true,
                "gap_pattern_seconds": [9.0, 19.0, 9.0, 18.0, 20.0],
                "landing_budget": budget,
            });
        }
        compare::compare(&manifest, &runs, &d1_budgets(), &pinned).unwrap()
    };
    // The budget buys one landing a slot while it lasts.
    for (blocks, landed) in [(12, 12), (15, 15), (20, 15)] {
        let result = dense(blocks, blocks, landed);
        assert!(result.passed, "{blocks}: {}", result.markdown);
    }
    // Every run reading a pinned 15 as 1 agrees with itself and still fails.
    let result = dense(15, 1, 1);
    assert!(!result.passed);
    assert!(
        result.markdown.contains(
            "ran a dense-cadence landing budget of 1, not the pinned `--scheduled-blocks` 15"
        ),
        "{}",
        result.markdown
    );
    let result = dense(15, 15, 1);
    assert!(!result.passed);
    assert!(result.markdown.contains(
        "landed 1 own blocks, not the 15 that the pinned `--scheduled-blocks` 15 buys of the \
         gap pattern's 15 slots"
    ));
}

#[test]
fn integers_beyond_an_f64_compare_exactly() {
    assert!(!compare::same_json(
        &json!(9_007_199_254_740_992_u64),
        &json!(9_007_199_254_740_993_u64)
    ));
    assert!(compare::same_json(&json!(0), &json!(0.0)));
    assert!(compare::same_json(&json!(2016), &json!(2016)));
    assert!(compare::same_json(&json!(-3), &json!(-3.0)));
    assert!(!compare::same_json(&json!(u64::MAX), &json!(-1)));
    assert!(!compare::same_json(&json!(1), &json!(1.5)));
    // So a seed above 2^53 and its neighbour are different populations.
    let manifest = manifest(288.0);
    let mut pinned = d1_args();
    pinned.insert("--seed".into(), json!(9_007_199_254_740_992_u64));
    let with_seed = |seed: u64| {
        let mut runs = loaded(&manifest, |_, _| met_steady());
        for run in &mut runs {
            set_pointer(
                run.report.as_mut().unwrap(),
                "/population/seed",
                json!(seed),
            );
        }
        compare::compare(&manifest, &runs, &d1_budgets(), &pinned).unwrap()
    };
    let result = with_seed(9_007_199_254_740_992);
    assert!(result.passed, "{}", result.markdown);
    let result = with_seed(9_007_199_254_740_993);
    assert!(!result.passed);
    assert!(result
        .markdown
        .contains("`/population/seed` 9007199254740993"));
}

#[test]
fn a_counted_run_that_leaves_out_a_phase_the_others_ran_fails_the_comparison() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    // `burst` is reported, not gated, under the D1 presets.
    runs[1].report.as_mut().unwrap()["phases"]
        .as_array_mut()
        .unwrap()
        .retain(|phase| phase["name"] != "burst");
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result
        .markdown
        .contains("candidate-r1 does not report the phase the preset plans"));
}

#[test]
fn a_series_with_no_base_run_in_the_medians_fails() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    for run in runs.iter_mut().filter(|r| r.run.build == "base") {
        run.excluded = Some("exit 3".into());
    }
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed, "{}", result.markdown);
    assert!(result.markdown.contains("no base run is in the medians"));
}

#[test]
fn an_unreported_figure_is_never_read_as_zero() {
    let manifest = manifest(288.0);
    let mut runs = loaded(&manifest, |_, _| met_steady());
    runs[1].report.as_mut().unwrap()["rejections"] = json!({});
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(!result.passed);
    assert!(result.markdown.contains("unanswered unreported"));
}

#[test]
fn a_report_of_another_build_is_refused_not_pooled() {
    let manifest = manifest(288.0);
    let base_dir = tempdir("cross");
    write_reports(&manifest, &base_dir);
    let crossed = base_dir.join("runs/throughput-20k-window-1fe-base-r2/load-harness-report.json");
    std::fs::write(
        &crossed,
        report(CANDIDATE, met_steady(), burst()).to_string(),
    )
    .unwrap();
    let error = compare::load_runs(&manifest, &base_dir).unwrap_err();
    assert!(format!("{error:#}").contains("never pooled"));
}

#[test]
fn a_host_outside_the_reference_flush_class_is_named() {
    let manifest = manifest(34.0);
    let runs = loaded(&manifest, |_, _| met_steady());
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(result
        .markdown
        .contains("**outside 2× of the reference host's 288 µs**"));
}

#[test]
fn one_missing_flush_reading_places_the_host_in_no_class() {
    let mut manifest = manifest(288.0);
    manifest.pg_test_fsync["after"] = json!({"exit_code": 1});
    let runs = loaded(&manifest, |_, _| met_steady());
    let result = compare::compare(&manifest, &runs, &d1_budgets(), &d1_args()).unwrap();
    assert!(result.markdown.contains("a flush reading is missing"));
    assert!(!result.markdown.contains("the reference flush class"));
}

#[test]
fn a_preset_gating_no_d1_phase_is_refused() {
    let manifest = manifest(288.0);
    let runs = loaded(&manifest, |_, _| met_steady());
    let budgets = Budgets {
        phases: Some(vec!["reconnect".to_owned()]),
        ..d1_budgets()
    };
    assert!(compare::compare(&manifest, &runs, &budgets, &d1_args()).is_err());
}

#[test]
fn a_series_short_of_its_repeats_is_refused() {
    let mut value = raw_manifest("ab");
    value["settings"] = json!({"repeats": 5});
    let dir = tempdir("short");
    let path = dir.join("manifest.json");
    std::fs::write(&path, value.to_string()).unwrap();
    let error = compare::read_manifest(&path).unwrap_err();
    assert!(format!("{error:#}").contains("3 of 5 repeats"));
    value["settings"] = json!({"repeats": 3});
    value["runs"].as_array_mut().unwrap().pop();
    std::fs::write(&path, value.to_string()).unwrap();
    let error = compare::read_manifest(&path).unwrap_err();
    assert!(format!("{error:#}").contains("3 base and 2 candidate"));
}

#[test]
fn the_median_of_an_even_count_is_the_mean_of_the_middle_two() {
    assert_eq!(compare::median(&[3.0, 1.0, 2.0]), Some(2.0));
    assert_eq!(compare::median(&[4.0, 1.0]), Some(2.5));
    assert_eq!(compare::median(&[]), None);
}

#[test]
fn the_binary_exits_1_on_a_regression_and_2_on_another_presets_series() {
    let preset = Preset::load(&presets_dir().join("throughput-20k-window-1fe.json")).unwrap();
    let mut manifest_value = raw_manifest(&preset.sha256);
    let base_dir = tempdir("bin");
    let manifest: Manifest = serde_json::from_value(manifest_value.clone()).unwrap();
    write_reports(&manifest, &base_dir);
    let regressed =
        base_dir.join("runs/throughput-20k-window-1fe-candidate-r3/load-harness-report.json");
    let bad = phase("steady_state", 500.0, 480.0, 5_000, 0, 30.0);
    std::fs::write(&regressed, report(CANDIDATE, bad, burst()).to_string()).unwrap();
    let path = base_dir.join("manifest.json");
    std::fs::write(&path, manifest_value.to_string()).unwrap();
    let run = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_qbit-prism-load-compare"))
            .arg("--manifest")
            .arg(&path)
            .arg("--preset")
            .arg(presets_dir().join("throughput-20k-window-1fe.json"))
            .output()
            .unwrap()
    };
    let output = run();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("### Verdict: FAIL"));
    manifest_value["preset"]["sha256"] = json!("00");
    std::fs::write(&path, manifest_value.to_string()).unwrap();
    assert_eq!(run().status.code(), Some(2));
}

fn raw_manifest(sha256: &str) -> Value {
    let manifest = manifest(288.0);
    json!({
        "schema": compare::MANIFEST_SCHEMA,
        "preset": {"name": "throughput-20k-window-1fe", "sha256": sha256},
        "host": manifest.host,
        "pg_test_fsync": manifest.pg_test_fsync,
        "builds": [
            {"label": "base", "ref": "5d0042f6", "commit": BASE},
            {"label": "candidate", "ref": "3.x.x", "commit": CANDIDATE},
        ],
        "runs": manifest.runs.iter().map(|run| json!({
            "id": run.id, "build": run.build, "repeat": run.repeat, "dir": run.dir,
            "exit_code": run.exit_code, "ceiling_hit": run.ceiling_hit,
            "load_before": run.load_before, "load_max": run.load_max,
            "mem_available_min_mib": run.mem_available_min_mib,
        })).collect::<Vec<_>>(),
    })
}

fn write_reports(manifest: &Manifest, base_dir: &Path) {
    for run in &manifest.runs {
        let dir = base_dir.join(&run.dir);
        std::fs::create_dir_all(&dir).unwrap();
        let commit = if run.build == "base" { BASE } else { CANDIDATE };
        std::fs::write(
            dir.join("load-harness-report.json"),
            report(commit, met_steady(), burst()).to_string(),
        )
        .unwrap();
    }
}

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "qbit-prism-load-compare-{tag}-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
