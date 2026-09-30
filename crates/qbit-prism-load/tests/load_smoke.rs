//! The per-PR load-harness smoke run (#521 item 6): the checked-in `pr-smoke`
//! preset end to end, against a debug `qbit-prism-server` and a managed
//! PostgreSQL 16 cluster, held to the preset's gates.
//!
//! One debug frontend, 100 sessions over 20 payout addresses skewed by a
//! whale and a Zipf tail with a two-order difficulty spread, a 20k window,
//! and 3 retargeting tips in a warm-up-only plan, then 30 s of rental churn:
//! bursts of 20 and 40 rental sessions leaving abruptly, a reconnect storm
//! dropping 30% of the connected sessions, and 2 tips. It asserts what the nightly
//! gate asserts -- every session got usable work on every tip within the
//! preset's generous bound, the harness exited 0 (reconciled exactly), no
//! acknowledged share was lost and no offer fell short -- and that the skew
//! it asked for is the skew it drove.
//!
//! The server binary is built here, into this test's own target directory:
//! a `qbit-prism-load` test cannot ask Cargo for another package's binary,
//! and Cargo has released the build lock by the time tests run.

use anyhow::{ensure, Context, Result};
use clap::Parser;
use qbit_prism_load::{cli::Args, compare, gate, preset, run};
use qbit_prism_test_gate as test_gate;
use std::path::PathBuf;

/// `target/<profile>`, the directory this test binary's `deps` sits in.
fn profile_dir() -> Result<PathBuf> {
    let exe = std::env::current_exe()?;
    let deps = exe.parent().context("test binary has no directory")?;
    Ok(deps
        .parent()
        .context("test binary's deps directory has no parent")?
        .to_owned())
}

/// Build the debug server beside this test binary and return its path.
fn build_server() -> Result<PathBuf> {
    let profile = profile_dir()?;
    let target = profile
        .parent()
        .context("profile directory has no parent")?;
    let status = std::process::Command::new(env!("CARGO"))
        .args([
            "build",
            "--locked",
            "-p",
            "qbit-prism-server",
            "--bin",
            "qbit-prism-server",
        ])
        .env("CARGO_TARGET_DIR", target)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .context("running cargo build for qbit-prism-server")?;
    ensure!(status.success(), "cargo build qbit-prism-server: {status}");
    let server = profile.join("qbit-prism-server");
    ensure!(server.is_file(), "{} was not built", server.display());
    Ok(server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_smoke_preset_serves_every_session_every_tip_and_reconciles() -> Result<()> {
    let Some(pg_bin) = test_gate::pg_bin_dir(test_gate::site!())? else {
        return Ok(());
    };
    let started = std::time::Instant::now();
    let server = build_server()?;
    let built = started.elapsed();
    let out = tempdir()?;
    let preset_path = preset::presets_dir().join("pr-smoke.json");
    let mut argv: Vec<std::ffi::OsString> = vec![
        "qbit-prism-load".into(),
        "--preset".into(),
        preset_path.clone().into(),
        "--server-bin".into(),
        server.into(),
        "--pg-bin-dir".into(),
        pg_bin.into(),
        "--out".into(),
        out.path.clone().into(),
        // A debug server from whatever tree the test runs in: a smoke run,
        // never capacity evidence, and the tips plan writes no artifact.
        "--allow-debug-server".into(),
        "--allow-dirty-tree".into(),
        "--allow-unverified-server-revision".into(),
    ];
    let (expanded, loaded) = preset::expand_command_line(std::mem::take(&mut argv))?;
    let loaded = loaded.context("the pr-smoke preset")?;
    let args = Args::try_parse_from(expanded)?;
    let exit = run::execute_with_preset(args, Some(loaded.clone())).await?;
    let ran = started.elapsed() - built;

    let text = std::fs::read_to_string(out.path.join("load-harness-report.json"))?;
    let report: serde_json::Value = serde_json::from_str(&text)?;
    let checks = gate::evaluate(&report, Some(exit), &gate::Budgets::from(&loaded.gates));
    let table = gate::markdown("smoke", &checks);
    eprintln!("{table}\nserver build {built:?}, harness run {ran:?}");
    assert!(gate::passed(&checks), "{table}");

    // Fake-node mode is unchanged by real-node mode (#547): the report's
    // schema-level key sets are the ones origin/3.x.x wrote.
    let key_sets = report_key_sets(&report);
    let golden_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden/fake_side_report_keys.json");
    let golden: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&golden_path)?)?;
    assert_eq!(
        key_sets, golden,
        "the fake-node side report's keys moved from what origin/3.x.x wrote"
    );

    // The A/B comparator (#511) holds every run to what the preset pins, as
    // a side report states it; a real report must read exactly so, or every
    // comparison would fail on a rendering difference.
    for setting in compare::expected_settings(&loaded.args)? {
        let reported = report.pointer(setting.pointer);
        assert!(
            reported.is_some_and(|value| compare::same_json(value, &setting.value)),
            "{}: reported {reported:?}, expected {}",
            setting.pointer,
            setting.value
        );
    }
    for (flag, pointer) in compare::PINNED_REPORT_FIELDS {
        let pinned = &loaded.args[*flag];
        let reported = report.pointer(pointer);
        assert!(
            reported.is_some_and(|value| compare::same_json(value, pinned)),
            "{pointer}: reported {reported:?}, pinned {flag} {pinned}"
        );
    }
    let reported: Vec<&str> = report["phases"]
        .as_array()
        .context("phases")?
        .iter()
        .filter_map(|phase| phase["name"].as_str())
        .collect();
    for plan in compare::expected_phases(&loaded.args)? {
        let phase = report["phases"]
            .as_array()
            .context("phases")?
            .iter()
            .find(|phase| phase["name"] == plan.name.as_str())
            .with_context(|| format!("{} is not in {reported:?}", plan.name))?;
        assert_eq!(
            phase["target_rate_shares_per_second"].as_f64(),
            Some(plan.rate),
            "{}",
            plan.name
        );
        let duration = phase["duration_seconds"].as_f64().context("duration")?;
        let seconds = plan.seconds as f64;
        assert!(
            (duration - seconds).abs() <= seconds * compare::DURATION_TOLERANCE,
            "{} ran {duration} s against a planned {seconds} s",
            plan.name
        );
        assert_eq!(
            phase["in_artifact"].as_bool(),
            Some(plan.in_artifact),
            "{}",
            plan.name
        );
        // The comparator holds the samplers to the pinned intervals.
        let lock_ms = loaded.args["--lock-sample-interval-ms"]
            .as_f64()
            .context("lock interval")?;
        let process_ms = loaded.args["--process-sample-interval-ms"]
            .as_f64()
            .context("process interval")?;
        assert_eq!(
            phase["order_lock"]["sample_interval_milliseconds"].as_f64(),
            Some(lock_ms),
            "{}",
            plan.name
        );
        for process in phase["processes"].as_array().context("processes")? {
            let seconds = process["sample_interval_seconds"]
                .as_f64()
                .context("interval")?;
            assert!(
                (seconds * 1000.0 - process_ms).abs() < 1e-6,
                "{}",
                plan.name
            );
        }
        // ... on every launched frontend, one record each.
        let ids = |list: &serde_json::Value| -> Result<Vec<String>> {
            let mut ids = list
                .as_array()
                .context("a list")?
                .iter()
                .map(|entry| {
                    entry["instance_id"]
                        .as_str()
                        .map(str::to_owned)
                        .context("instance_id")
                })
                .collect::<Result<Vec<_>>>()?;
            ids.sort_unstable();
            Ok(ids)
        };
        assert_eq!(
            ids(&phase["processes"])?,
            ids(&report["frontend_environment"])?,
            "{}",
            plan.name
        );
        // ... each having sampled, with no reason it could not.
        let summaries = std::iter::once(&phase["order_lock"])
            .chain(phase["processes"].as_array().context("processes")?);
        for summary in summaries {
            assert!(summary["samples"].as_u64() > Some(0), "{}", plan.name);
            assert!(summary["unavailable_reason"].is_null(), "{}", plan.name);
        }
        // A delayed phase's delay was seen to be paid.
        if plan.database_delay_ms > 0 {
            let observed = phase["database_delay_observed_select1_median_milliseconds"]
                .as_f64()
                .context("observed delay")?;
            run::check_delay_observed(plan.database_delay_ms, observed)?;
        }
        // The comparator holds the lowest reading to the preset's memory
        // floor, and treats an unread one as a failure.
        assert!(
            phase["min_mem_available_kib"].as_u64().is_some(),
            "{} read no MemAvailable",
            plan.name
        );
        assert_eq!(
            phase["database_delay_milliseconds_configured"].as_u64(),
            Some(plan.database_delay_ms),
            "{}",
            plan.name
        );
        let frontends = loaded.args["--frontends"]
            .as_u64()
            .context("pinned frontends")?;
        assert_eq!(
            phase["frontend_restarts"].as_u64(),
            Some(plan.frontend_restarts(frontends as usize)),
            "{}",
            plan.name
        );
    }
    // The churn plan the comparator expects is the one the run carried out.
    let churn = &report["churn"];
    assert_eq!(
        churn["realised"]["rentals_spawned"], churn["plan"]["rental_sessions"],
        "{churn}"
    );
    assert_eq!(
        churn["realised"]["storms"].as_array().map(Vec::len),
        churn["plan"]["storms"].as_array().map(Vec::len),
        "{churn}"
    );
    assert_eq!(
        churn["realised"]["rentals_departed"], churn["plan"]["rentals_departing_in_phase"],
        "{churn}"
    );
    let dropped: u64 = churn["realised"]["storms"]
        .as_array()
        .context("storms")?
        .iter()
        .filter_map(|s| s["dropped"].as_u64())
        .sum();
    let departed = churn["realised"]["rentals_departed"]
        .as_u64()
        .context("departed")?;
    let reconnected = churn["realised"]["reconnects_completed"]
        .as_u64()
        .context("reconnects")?;
    assert!(reconnected + departed >= dropped, "{churn}");
    assert_eq!(
        report["database"]["replication"]["agreed_with_declared"], true,
        "the replication premise"
    );
    assert_eq!(report["database"]["mode"], "managed");
    for storm in churn["realised"]["storms"].as_array().context("storms")? {
        let connected = storm["connected"].as_f64().context("connected")?;
        let fraction = storm["fraction"].as_f64().context("fraction")?;
        assert_eq!(
            storm["dropped"].as_f64(),
            Some((fraction * connected).round().min(connected)),
            "{storm}"
        );
        assert!(storm["dropped"].as_u64() > Some(0), "{storm}");
    }
    assert_eq!(
        churn["tip_delivery"]["tips"].as_array().map(Vec::len),
        churn["plan"]["tips_at_seconds"].as_array().map(Vec::len),
        "{churn}"
    );
    for (flag, pointer) in compare::PINNED_REPORT_COUNTS {
        let pinned = loaded.args[*flag].as_u64().context("a pinned count")?;
        let listed = report
            .pointer(pointer)
            .and_then(|v| v.as_array())
            .map(Vec::len);
        assert_eq!(listed, Some(pinned as usize), "{pointer} for {flag}");
    }
    for frontend in report["frontend_environment"]
        .as_array()
        .context("frontend environment")?
    {
        for (flag, key) in compare::PINNED_FRONTEND_ENV {
            let pinned = loaded.args[*flag]
                .as_f64()
                .context("numeric server setting")?;
            let launched: f64 = frontend["environment"][key]
                .as_str()
                .context("launched setting")?
                .parse()?;
            assert_eq!(launched, pinned, "{key} for {flag}");
        }
        // The pool fee every frontend ran is the pinned one.
        assert_eq!(frontend["environment"]["PRISM_POOL_FEE_ENABLED"], "1");
        assert_eq!(
            frontend["environment"]["PRISM_POOL_FEE_BPS"],
            loaded.args["--pool-fee-bps"].to_string().as_str()
        );
    }
    // Every pinned session connected at least once.
    assert!(
        report["client"]["connects"].as_u64()
            >= Some(
                loaded.args["--sessions"]
                    .as_u64()
                    .context("pinned sessions")?
            ),
        "{}",
        report["client"]
    );

    // The run is the preset's, and the skew it asked for is the skew it drove.
    assert_eq!(report["preset"]["name"], "pr-smoke");
    assert_eq!(report["preset"]["sha256"], loaded.sha256.as_str());
    assert_eq!(report["topology"]["sessions"], 100);
    let tips = report["time_to_usable_work"]["tips"]
        .as_array()
        .context("tips")?;
    assert_eq!(tips.len(), 3, "the preset mints three tips");
    for tip in tips {
        assert_eq!(tip["sessions_with_work"], 100, "{tip}");
    }
    let population = &report["population"];
    assert_eq!(population["recipients"], 20);
    assert_eq!(
        population["window_shares_per_recipient_concentration"]["nonzero"], 20,
        "every address holds seeded window shares"
    );
    assert!(
        population["sessions_per_recipient_concentration"]["top1_share"]
            .as_f64()
            .context("sessions top1")?
            > 0.4,
        "the whale holds most sessions: {population}"
    );
    assert!(
        population["session_difficulty_spread_orders_of_magnitude"]
            .as_f64()
            .context("difficulty spread")?
            > 1.0,
        "sessions asked for their own difficulty: {population}"
    );
    assert!(
        population["live_accepted_shares_per_recipient_concentration"]["nonzero"]
            .as_u64()
            .context("live recipients")?
            >= 10,
        "live shares came from many addresses: {population}"
    );
    // Under vardiff every session offers at one rate and the whale's work is
    // in its difficulty: its share of the accepted work is its weight.
    assert_eq!(
        report["phases"][0]["arrival"]["offer_placement"],
        "round-robin"
    );
    let whale = population["live_accepted_work_per_recipient_concentration"]["top1_share"]
        .as_f64()
        .context("live work top1")?;
    assert!(
        (0.45..0.75).contains(&whale),
        "the 60% whale's share of accepted work: {whale}"
    );
    assert_eq!(report["validator"]["artifact_written"], false);

    // The churn phase drove what the preset planned, and every abandoned
    // submit is accounted for as the churn's, never as a divergence.
    let churn = &report["churn"];
    assert_eq!(churn["ran"], true, "{churn}");
    let realised = &churn["realised"];
    assert_eq!(realised["rentals_spawned"], 60, "{realised}");
    assert!(
        realised["rentals_departed"].as_u64().context("departed")? > 0,
        "rentals left abruptly: {realised}"
    );
    let storms = realised["storms"].as_array().context("storms")?;
    assert_eq!(storms.len(), 1, "{realised}");
    assert!(
        storms[0]["dropped"].as_u64().context("dropped")? > 0,
        "{realised}"
    );
    assert!(
        realised["reconnects_completed"]
            .as_u64()
            .context("reconnects")?
            > 0,
        "stormed sessions came back: {realised}"
    );
    assert_eq!(realised["rentals_still_outstanding_at_phase_end"], 0);
    let delivery = churn["tip_delivery"]["tips"]
        .as_array()
        .context("churn tips")?;
    assert_eq!(delivery.len(), 2, "{churn}");
    for tip in delivery {
        assert_eq!(tip["unserved"], 0, "{tip}");
    }
    assert_eq!(
        report["no_response_commits"]["shares"]
            .as_array()
            .context("no-response commits")?
            .iter()
            .filter(|share| share["churn_closed"] != true && share["window_ended"] != true)
            .count(),
        0
    );
    Ok(())
}

/// The side report's top-level keys, and the keys of the blocks whose shape
/// is fixed by the harness rather than by what the run saw.
fn report_key_sets(report: &serde_json::Value) -> serde_json::Value {
    let keys = |value: &serde_json::Value| -> Vec<String> {
        let mut keys: Vec<String> = value
            .as_object()
            .map(|object| object.keys().cloned().collect())
            .unwrap_or_default();
        keys.sort();
        keys
    };
    serde_json::json!({
        "top_level": keys(report),
        "versions": keys(&report["versions"]),
        "topology": keys(&report["topology"]),
        "window": keys(&report["window"]),
        "node": keys(&report["node"]),
    })
}

/// A scratch directory removed on drop.
struct TempDir {
    path: PathBuf,
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn tempdir() -> Result<TempDir> {
    let path = std::env::temp_dir().join(format!("prism-load-smoke-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path)?;
    Ok(TempDir { path })
}
