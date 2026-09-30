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
    }
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

/// Every phase the preset plans, each meeting #473's rule at its rate.
fn report(preset: &Preset, commit: &str) -> Value {
    let phases: Vec<Value> = compare::expected_phases(&preset.args)
        .unwrap()
        .into_iter()
        .map(|plan| {
            json!({
                "name": plan.name,
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
    assert!(collate::read_plan(&write(good.clone())).is_ok());
    for (pointer, value) in [
        ("/schema", json!("qbit.prism.l3-plan.v0")),
        ("/tree", json!("HEAD^{tree}")),
        ("/include/0/id", json!("p-r2")),
        ("/include", json!([])),
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
}
