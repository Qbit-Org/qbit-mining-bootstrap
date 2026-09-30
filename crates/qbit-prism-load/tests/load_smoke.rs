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
use qbit_prism_load::{cli::Args, gate, preset, run};
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
///
/// The nested build has to see what the build of this test binary saw, or
/// each invalidates the other's dependencies and every run pays a ~20 s
/// rebuild: `qbit-prism-load` is selected beside the server so Cargo
/// resolves the shared dependencies' features the same way, and the
/// variables `cargo test` sets for this process but not for its own build
/// (`CARGO_PKG_NAME`, `CARGO_MANIFEST_DIR` and the rest) are removed,
/// because `ring`'s build script tracks them. The server's library is then
/// already built and only its binary links.
fn build_server() -> Result<PathBuf> {
    let profile = profile_dir()?;
    let target = profile
        .parent()
        .context("profile directory has no parent")?;
    let mut command = std::process::Command::new(env!("CARGO"));
    for (name, _) in std::env::vars_os() {
        let name = name.to_string_lossy().into_owned();
        if [
            "CARGO_PKG_",
            "CARGO_MANIFEST_",
            "CARGO_CRATE_",
            "CARGO_BIN_",
            "CARGO_PRIMARY_PACKAGE",
            "CARGO_TARGET_TMPDIR",
        ]
        .iter()
        .any(|prefix| name.starts_with(prefix))
        {
            command.env_remove(name);
        }
    }
    let status = command
        .args([
            "build",
            "--locked",
            "-p",
            "qbit-prism-load",
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
