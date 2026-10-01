//! CTV fanout settlement under load (#548): a fake-node run with
//! `--ctv-settlement` and more payable recipients than the direct-output cap
//! lands its block through CTV fanout chunks, reconciles exactly, and its
//! side report counts the direct, fanout and carried recipients from the
//! server's own landing rows.
//!
//! One debug frontend, 100 sessions over 400 payout addresses (a Zipf
//! tail), a 20k window, and one scheduled block in a warm-up-only plan
//! (about 40 s). Every one of the 400 addresses holds seeded window shares
//! worth more than the direct-coinbase floor, so with no pool fee the 12
//! largest (the server's default direct-output cap) are paid directly and
//! the rest go to one fanout chunk.
//!
//! The server binary is built as `load_smoke` builds it, into this test's
//! own target directory; see that test's `build_server`.

use anyhow::{ensure, Context, Result};
use clap::Parser;
use qbit_prism_load::{cli::Args, run};
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

/// Build the debug server beside this test binary, exactly as `load_smoke`
/// does, so neither build invalidates the other's dependencies.
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
async fn a_ctv_run_pays_the_direct_overflow_through_fanout_and_counts_the_split() -> Result<()> {
    let Some(pg_bin) = test_gate::pg_bin_dir(test_gate::site!())? else {
        return Ok(());
    };
    let server = build_server()?;
    let out = TempDir::new()?;
    let argv: Vec<std::ffi::OsString> = vec![
        "qbit-prism-load".into(),
        "--server-bin".into(),
        server.into_os_string(),
        "--pg-bin-dir".into(),
        pg_bin.into(),
        "--out".into(),
        out.path.clone().into_os_string(),
        // A debug server from whatever tree the test runs in; the tips plan
        // writes no artifact.
        "--allow-debug-server".into(),
        "--allow-dirty-tree".into(),
        "--allow-unverified-server-revision".into(),
        "--plan".into(),
        "tips".into(),
        "--sessions".into(),
        "100".into(),
        "--recipients".into(),
        "400".into(),
        "--recipient-weights".into(),
        "zipf:1.1".into(),
        "--scheduled-blocks".into(),
        "1".into(),
        "--external-tips".into(),
        "1".into(),
        "--ctv-settlement".into(),
        // No pool fee, so every direct slot is a miner's: a nonzero fee is
        // pinned direct first (pool-fee-first) and would hold one.
        "--pool-fee-bps".into(),
        "0".into(),
    ];
    let args = Args::try_parse_from(argv)?;
    let exit = run::execute(args).await?;

    let text = std::fs::read_to_string(out.path.join("load-harness-report.json"))?;
    let report: serde_json::Value = serde_json::from_str(&text)?;
    assert_eq!(exit, 0, "the run completed and reconciled exactly");
    let environment = &report["frontend_environment"][0]["environment"];
    assert_eq!(environment["PRISM_CTV_SETTLEMENT_ENABLED"], "1");
    assert_eq!(environment["PRISM_CTV_BROADCASTER_ENABLED"], "0");

    let settlement = &report["settlement"];
    assert_eq!(settlement["ctv_settlement"], true, "{settlement}");
    let accepted = settlement["accepted_blocks"].as_u64().context("accepted")?;
    assert!(accepted >= 1, "the scheduled block landed: {settlement}");
    assert_eq!(settlement["measured_blocks"], accepted, "{settlement}");
    assert_eq!(settlement["unmeasured_blocks"], serde_json::json!([]));
    for block in settlement["blocks"].as_array().context("blocks")? {
        assert_eq!(
            block["settlement_mode"], "hybrid_coinbase_ctv_fanout",
            "{block}"
        );
        // The server's default direct-output cap
        // (`PRISM_MAX_DIRECT_COINBASE_OUTPUTS` in `config.rs`), all of it
        // miners'.
        let direct: u64 = 12;
        assert_eq!(block["direct_recipients"], direct, "{block}");
        let fanout = block["fanout_recipients"].as_u64().context("fanout")?;
        let carried = block["carried_recipients"].as_u64().context("carried")?;
        assert!(fanout > 0, "{block}");
        assert_eq!(block["fanout_chunks"], 1, "{block}");
        // Every account the payout window paid or carried is in one count,
        // and the run's 400 addresses are at most that many accounts.
        assert!(direct + fanout + carried <= 400, "{block}");
        assert!(direct + fanout + carried >= 300, "{block}");
    }
    Ok(())
}

/// A scratch directory removed on drop.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!("prism-load-ctv-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
