//! The per-PR fault smoke run (#554): the checked-in `faults-pr-smoke`
//! preset end to end, against a debug `qbit-prism-server` and a managed
//! PostgreSQL 16 cluster.
//!
//! One frontend and 40 sessions on the fake node, then a `faults` phase with
//! two faults, each held to its #554 pass criteria:
//!
//! - `sigterm-drain`: a scheduled block's `submitblock` is held at the fault
//!   relay, and the frontend offering it gets SIGTERM the moment the call
//!   arrives. #585's shutdown has to wait for the offer: the block reaches
//!   the node exactly once, the node's `accepted` is recorded before the
//!   process exits 0, and no committed share goes unanswered. The relaunch
//!   then lands the block, and the next fault waits until every frontend
//!   serves work at the payout revision the landing committed (#686).
//! - `settlement-lock`: an outside transaction holds `SETTLEMENT_LOCK` while
//!   the sessions keep mining, and a tip is minted halfway through. Shares on
//!   retained work keep being acknowledged, the tip's jobs wait for the
//!   release, and every session has work on the tip within 10 s of it.
//!
//! The nightly and weekly presets run the rest of the catalogue under load;
//! this is the part cheap enough for every PR.
//!
//! `profile_dir` and `tempdir` are copied from `tests/load_smoke.rs`, and
//! `build_server` adapted from it: a test binary cannot share another's
//! helpers.

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
async fn faults_under_load_drain_an_offer_in_flight_and_outlast_a_settlement_lock_holder(
) -> Result<()> {
    let Some(pg_bin) = test_gate::pg_bin_dir(test_gate::site!())? else {
        return Ok(());
    };
    let started = std::time::Instant::now();
    let server = build_server()?;
    let built = started.elapsed();
    let out = tempdir()?;
    let preset_path = preset::presets_dir().join("faults-pr-smoke.json");
    let argv: Vec<std::ffi::OsString> = vec![
        "qbit-prism-load".into(),
        "--preset".into(),
        preset_path.into(),
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
    let (expanded, loaded) = preset::expand_command_line(argv)?;
    let loaded = loaded.context("the faults-pr-smoke preset")?;
    let args = Args::try_parse_from(expanded)?;
    let exit = run::execute_with_preset(args, Some(loaded.clone())).await?;
    let ran = started.elapsed() - built;

    let text = std::fs::read_to_string(out.path.join("load-harness-report.json"))?;
    let report: serde_json::Value = serde_json::from_str(&text)?;
    let faults = &report["faults"];
    let mut table = String::new();
    for row in faults["faults"].as_array().context("fault rows")? {
        table.push_str(&format!("{} pass={}\n", row["fault"], row["pass"]));
        for check in row["checks"].as_array().context("fault checks")? {
            table.push_str(&format!(
                "  [{}] {}: {}\n",
                if check["pass"] == true { "ok" } else { "FAIL" },
                check["name"].as_str().unwrap_or_default(),
                check["detail"].as_str().unwrap_or_default()
            ));
        }
    }
    let checks = gate::evaluate(&report, Some(exit), &gate::Budgets::from(&loaded.gates));
    let gate_table = gate::markdown("faults-pr-smoke", &checks);
    eprintln!("{table}\n{gate_table}\nserver build {built:?}, harness run {ran:?}");

    // Each fault ran, in order, and met every one of its criteria.
    let rows = faults["faults"].as_array().context("fault rows")?;
    let kinds: Vec<&str> = rows
        .iter()
        .map(|row| row["fault"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(kinds, ["sigterm-drain", "settlement-lock"], "{table}");
    for row in rows {
        assert_eq!(row["pass"], true, "{} failed:\n{table}", row["fault"]);
    }
    assert_eq!(faults["passed"], true, "{table}");
    assert_eq!(
        faults["blocks_offered_more_than_once"],
        serde_json::json!([]),
        "{table}"
    );
    // The run's own contract on top: exit 0 (reconciled exactly, no fault
    // criterion missed) and the preset's gate.
    assert_eq!(exit, run::EXIT_OK, "{table}\n{gate_table}");
    assert!(gate::passed(&checks), "{gate_table}");
    assert_eq!(
        report["no_response_commits"]["shares"]
            .as_array()
            .context("no-response commits")?
            .iter()
            .filter(|share| share["window_ended"] != true)
            .count(),
        0,
        "a committed share went unanswered mid-run"
    );
    Ok(())
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
    let path = std::env::temp_dir().join(format!("prism-load-faults-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path)?;
    Ok(TempDir { path })
}
