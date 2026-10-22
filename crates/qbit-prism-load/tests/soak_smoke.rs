//! The soak smoke run (#575 item 2): the checked-in `soak-smoke` preset end
//! to end, against a debug `qbit-prism-server` and a managed PostgreSQL 16
//! cluster, held to its preset's gates and its soak gates. It takes about
//! four minutes, so it is an opt-in (`#[ignore]`) test the nightly
//! live-nightly job runs (test/prism-nightly-gated-tests.txt); the per-PR
//! shards run `soak_sampling` instead, which proves the sampling, the
//! rollover and the gating against a real database in seconds.
//!
//! Four minutes of one server lifetime, looping pr-smoke's workload (three
//! retargeting tips, then rental churn with a reconnect storm) four times
//! over a 1,000-share window: the share ledger is rolled into its next
//! partition twice, the operator's share-archive commands seal, archive,
//! verify, detach and drop two partitions under the live load, every 5 s a
//! sample is taken, and the run still reconciles exactly. The memory and
//! descriptor bounds are loose here: four minutes of a debug server is not a
//! trend. soak-short and soak-weekly hold the trends; this proves the
//! machinery that feeds them.
//!
//! The server binary is built here, into this test's own target directory,
//! as the load smoke test does.

use anyhow::{ensure, Context, Result};
use clap::Parser;
use qbit_prism_load::{cli::Args, gate, preset, run, soak};
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

/// Build the debug server beside this test binary and return its path. Its
/// environment and package selection match this test binary's build, so the
/// server's library is not rebuilt; see `load_smoke`'s `build_server`.
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
#[ignore = "nightly #575: a four-minute soak smoke; run with --ignored"]
async fn the_soak_smoke_preset_rolls_over_archives_and_holds_its_gates() -> Result<()> {
    let Some(pg_bin) = test_gate::pg_bin_dir(test_gate::site!())? else {
        return Ok(());
    };
    let started = std::time::Instant::now();
    let server = build_server()?;
    let built = started.elapsed();
    let out = tempdir()?;
    let preset_path = preset::presets_dir().join("soak-smoke.json");
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
        "--allow-debug-server".into(),
        "--allow-dirty-tree".into(),
        "--allow-unverified-server-revision".into(),
    ];
    let (expanded, loaded) = preset::expand_command_line(argv)?;
    let loaded = loaded.context("the soak-smoke preset")?;
    let spec = loaded.soak.clone().context("the preset's soak block")?;
    let args = Args::try_parse_from(expanded)?;
    let exit = run::execute_with_preset(args, Some(loaded.clone())).await?;
    let ran = started.elapsed() - built;

    let text = std::fs::read_to_string(out.path.join("load-harness-report.json"))?;
    let report: serde_json::Value = serde_json::from_str(&text)?;
    let mut checks = gate::evaluate(&report, Some(exit), &gate::Budgets::from(&loaded.gates));
    checks.extend(soak::gate_harness_run(
        &report,
        &out.path,
        &spec,
        "soak smoke",
    ));
    let table = gate::markdown("soak smoke", &checks);
    eprintln!("{table}\nserver build {built:?}, harness run {ran:?}");
    assert!(gate::passed(&checks), "{table}");

    // One lifetime: nothing restarted a frontend, and every phase is a
    // cycle's own.
    assert_eq!(report["validator"]["artifact_written"], false);
    for frontend in report["frontend_environment"]
        .as_array()
        .context("frontends")?
    {
        assert_eq!(frontend["restarts"], 0, "{frontend}");
        assert_eq!(
            frontend["environment"]["PRISM_SHARE_PARTITION_ENSURE_INTERVAL_SECONDS"], "2",
            "the soak's shortened partition maintenance reached the frontend"
        );
    }
    let phases: Vec<&str> = report["phases"]
        .as_array()
        .context("phases")?
        .iter()
        .filter_map(|phase| phase["name"].as_str())
        .collect();
    assert_eq!(
        phases,
        [
            "c01.pr-smoke.warm_up",
            "c01.pr-smoke.churn",
            "c02.pr-smoke.warm_up",
            "c02.pr-smoke.churn",
            "c03.pr-smoke.warm_up",
            "c03.pr-smoke.churn",
            "c04.pr-smoke.warm_up",
            "c04.pr-smoke.churn",
        ]
    );
    // Every cycle minted its own three tips, and every session got work on
    // every tip.
    let tips = report["time_to_usable_work"]["tips"]
        .as_array()
        .context("tips")?;
    assert_eq!(tips.len(), 12, "three tips per cycle");
    for tip in tips {
        assert_eq!(tip["sessions_with_work"], 100, "{tip}");
    }

    // The samples cover the run, and the retention commands really ran.
    let samples = soak::read_samples(&out.path.join(soak::SAMPLES_FILE))?;
    assert!(samples.len() >= 40, "{} samples", samples.len());
    let soak_report = &report["soak"];
    assert_eq!(soak_report["cycles"], 4);
    assert_eq!(
        soak_report["rollovers"].as_array().map(Vec::len),
        Some(2),
        "{soak_report}"
    );
    let dropped = soak_report["dropped"].as_array().context("dropped")?;
    assert!(dropped.len() >= 2, "{soak_report}");
    // Every dropped partition came back from its archive for the
    // reconciliation above, which found every acknowledged share.
    assert_eq!(
        &soak_report["restored_for_reconciliation"], &soak_report["dropped"],
        "{soak_report}"
    );
    let events = std::fs::read_to_string(out.path.join(soak::EVENTS_FILE))?;
    for step in [
        "\"seal\"",
        "\"archive\"",
        "\"verify\"",
        "\"detach\"",
        "\"drop\"",
    ] {
        assert!(events.contains(step), "no {step} event:\n{events}");
    }
    assert!(out.path.join(soak::REPORT_FILE).is_file());
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
    let path = std::env::temp_dir().join(format!("prism-soak-smoke-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path)?;
    Ok(TempDir { path })
}
