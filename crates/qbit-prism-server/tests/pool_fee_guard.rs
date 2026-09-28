//! #525: CTV settlement without a pool fee builds no work once a sub-floor
//! balance exists, so every entry point that would run or validate that
//! policy refuses it. Isolated subprocesses; the database and node endpoints
//! are closed ports, so nothing here needs PostgreSQL or qbitd.
use std::{process::Output, time::Duration};
use tokio::{process::Command, time::timeout};

const REFUSAL: &str = "PRISM_CTV_SETTLEMENT_ENABLED=1 requires PRISM_POOL_FEE_ENABLED=1";
const CTV: &[(&str, &str)] = &[("PRISM_CTV_SETTLEMENT_ENABLED", "1")];
const PROGRAM: &str = "fefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe";

fn fee(bps: &'static str) -> [(&'static str, &'static str); 4] {
    [
        ("PRISM_POOL_FEE_ENABLED", "1"),
        ("PRISM_POOL_FEE_BPS", bps),
        ("PRISM_POOL_FEE_RECIPIENT_ID", "pool-fee"),
        ("PRISM_POOL_FEE_P2MR_PROGRAM_HEX", PROGRAM),
    ]
}

async fn command(args: &[&str], settings: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    command.args(args).kill_on_drop(true);
    for (name, _) in std::env::vars().filter(|(name, _)| {
        name.starts_with("PRISM_") || name.starts_with("QBIT_") || name == "RUST_LOG"
    }) {
        command.env_remove(name);
    }
    command
        .env("PRISM_RUNTIME_WORKERS", "2")
        .env(
            "PRISM_DATABASE_URL",
            "postgresql://operator:test-only-password@127.0.0.1:1/offline",
        )
        .env("QBIT_RPC_URL", "http://127.0.0.1:1/")
        .env("QBIT_CHAIN", "regtest")
        .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1");
    for (name, value) in settings {
        command.env(name, value);
    }
    timeout(Duration::from_secs(20), command.output())
        .await
        .expect("the command stalled")
        .unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_refused(output: &Output) {
    assert!(!output.status.success(), "accepted: {}", stderr(output));
    let error = stderr(output);
    assert!(error.contains(REFUSAL), "{error}");
    assert!(
        error.contains("sub-floor dust cannot be settled"),
        "{error}"
    );
}

#[tokio::test]
async fn config_check_refuses_ctv_settlement_without_a_pool_fee() {
    assert_refused(&command(&["check-config"], CTV).await);
    // An explicit disable is the same refusal as an unset flag.
    let disabled = [CTV[0], ("PRISM_POOL_FEE_ENABLED", "0")];
    assert_refused(&command(&["check-config"], &disabled).await);
}

#[tokio::test]
async fn config_check_accepts_ctv_settlement_with_a_pool_fee_including_zero_bps() {
    for bps in ["200", "0"] {
        let settings: Vec<_> = CTV.iter().copied().chain(fee(bps)).collect();
        let output = command(&["check-config"], &settings).await;
        assert!(output.status.success(), "{bps} bps: {}", stderr(&output));
    }
    // Direct settlement is not refused here; see the config unit tests.
    let output = command(&["check-config"], &[]).await;
    assert!(output.status.success(), "{}", stderr(&output));
}

/// The server refuses before it binds a listener or opens the database.
#[tokio::test]
async fn run_refuses_ctv_settlement_without_a_pool_fee_before_startup() {
    for args in [&["run"][..], &[]] {
        let output = command(args, CTV).await;
        assert_refused(&output);
        assert!(!stderr(&output).contains("database"), "{}", stderr(&output));
    }
}

#[tokio::test]
async fn self_check_refuses_ctv_settlement_without_a_pool_fee() {
    let output = command(&["self-check"], CTV).await;
    assert_refused(&output);
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["ok"], false);
}

/// A transition may leave the refused policy, which stays readable as the
/// current one, but cannot enter it.
#[tokio::test]
async fn policy_transition_refuses_only_a_target_without_a_pool_fee() {
    let dir = tempfile::tempdir().unwrap();
    let to = dir.path().join("target.env");
    let with_fee: Vec<_> = CTV.iter().copied().chain(fee("0")).collect();

    std::fs::write(
        &to,
        "PRISM_POOL_FEE_ENABLED=0\nPRISM_POOL_FEE_BPS=\nPRISM_POOL_FEE_P2MR_PROGRAM_HEX=\n",
    )
    .unwrap();
    let output = command(
        &["policy-transition", "--to", to.to_str().unwrap()],
        &with_fee,
    )
    .await;
    assert_refused(&output);
    assert!(
        stderr(&output).contains("invalid target policy configuration"),
        "{}",
        stderr(&output)
    );

    let target = fee("0")
        .iter()
        .map(|(name, value)| format!("{name}={value}\n"))
        .collect::<String>();
    std::fs::write(&to, target).unwrap();
    let output = command(&["policy-transition", "--to", to.to_str().unwrap()], CTV).await;
    // Past configuration, it fails only on the closed database port.
    assert!(!output.status.success());
    assert!(!stderr(&output).contains(REFUSAL), "{}", stderr(&output));
    assert!(
        !stderr(&output).contains("policy configuration"),
        "{}",
        stderr(&output)
    );
}
