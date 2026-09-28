//! #525, #535: settlement without a pool fee builds no work once a sub-floor
//! balance exists, in either settlement mode, so every entry point that
//! would run or validate that policy refuses it. Isolated subprocesses; the
//! database and node endpoints are closed ports, so nothing here needs
//! PostgreSQL or qbitd.
use std::{process::Output, time::Duration};
use tokio::{process::Command, time::timeout};

const REFUSAL: &str = "requires PRISM_POOL_FEE_ENABLED=1 (PRISM_POOL_FEE_BPS=0 is allowed)";
const CTV_REFUSAL: &str = "PRISM_CTV_SETTLEMENT_ENABLED=1 requires PRISM_POOL_FEE_ENABLED=1";
const DIRECT_REFUSAL: &str =
    "direct settlement (PRISM_CTV_SETTLEMENT_ENABLED=0) requires PRISM_POOL_FEE_ENABLED=1";
const CTV: &[(&str, &str)] = &[("PRISM_CTV_SETTLEMENT_ENABLED", "1")];
const DIRECT: &[(&str, &str)] = &[("PRISM_CTV_SETTLEMENT_ENABLED", "0")];
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
async fn config_check_refuses_either_settlement_mode_without_a_pool_fee() {
    for (mode, refusal) in [
        (CTV, CTV_REFUSAL),
        (DIRECT, DIRECT_REFUSAL),
        (&[][..], DIRECT_REFUSAL),
    ] {
        let output = command(&["check-config"], mode).await;
        assert_refused(&output);
        assert!(stderr(&output).contains(refusal), "{}", stderr(&output));
        // An explicit disable is the same refusal as an unset flag.
        let disabled: Vec<_> = mode
            .iter()
            .copied()
            .chain([("PRISM_POOL_FEE_ENABLED", "0")])
            .collect();
        let output = command(&["check-config"], &disabled).await;
        assert_refused(&output);
        assert!(stderr(&output).contains(refusal), "{}", stderr(&output));
    }
}

#[tokio::test]
async fn config_check_accepts_either_settlement_mode_with_a_pool_fee_including_zero_bps() {
    for mode in [CTV, DIRECT] {
        for bps in ["200", "0"] {
            let settings: Vec<_> = mode.iter().copied().chain(fee(bps)).collect();
            let output = command(&["check-config"], &settings).await;
            assert!(
                output.status.success(),
                "{mode:?} {bps} bps: {}",
                stderr(&output)
            );
        }
    }
}

/// The server refuses before it binds a listener or opens the database.
#[tokio::test]
async fn run_refuses_either_settlement_mode_without_a_pool_fee_before_startup() {
    for mode in [CTV, DIRECT] {
        for args in [&["run"][..], &[]] {
            let output = command(args, mode).await;
            assert_refused(&output);
            assert!(!stderr(&output).contains("database"), "{}", stderr(&output));
        }
    }
}

#[tokio::test]
async fn self_check_refuses_either_settlement_mode_without_a_pool_fee() {
    for mode in [CTV, DIRECT] {
        let output = command(&["self-check"], mode).await;
        assert_refused(&output);
        let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["ok"], false);
    }
}

/// A transition may leave the refused policy, which stays readable as the
/// current one, but cannot enter it, in either settlement mode.
#[tokio::test]
async fn policy_transition_refuses_only_a_target_without_a_pool_fee() {
    for mode in [CTV, DIRECT] {
        let dir = tempfile::tempdir().unwrap();
        let to = dir.path().join("target.env");
        let with_fee: Vec<_> = mode.iter().copied().chain(fee("0")).collect();

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
            "{mode:?}: {}",
            stderr(&output)
        );

        let target = fee("0")
            .iter()
            .map(|(name, value)| format!("{name}={value}\n"))
            .collect::<String>();
        std::fs::write(&to, target).unwrap();
        let output = command(&["policy-transition", "--to", to.to_str().unwrap()], mode).await;
        // Past configuration, it fails only on the closed database port.
        assert!(!output.status.success());
        assert!(
            !stderr(&output).contains(REFUSAL),
            "{mode:?}: {}",
            stderr(&output)
        );
        assert!(
            !stderr(&output).contains("policy configuration"),
            "{mode:?}: {}",
            stderr(&output)
        );
    }
}

/// #535: `.env.example`'s lab recipient is refused in production, where
/// nobody could spend the dust swept to it; a pool's own program is not.
#[tokio::test]
async fn production_refuses_the_development_pool_fee_recipient() {
    let dir = tempfile::tempdir().unwrap();
    let seed = |name: &str, byte: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, byte.repeat(32)).unwrap();
        path.to_str().unwrap().to_owned()
    };
    let manifest = seed("manifest", "5a");
    let ledger = seed("ledger", "6b");
    let ledger_public_key = qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"6b".repeat(32))
        .unwrap()
        .public_key_hex();
    let production = [
        ("QBIT_CHAIN", "signet"),
        ("QBIT_PRODUCTION", "1"),
        ("PRISM_ALLOW_TEST_SIGNING_SEEDS", "0"),
        ("QBIT_RPC_USER", "qbitrpc"),
        ("QBIT_RPC_PASSWORD", "not-default"),
        ("PRISM_STRATUM_SHARE_DIFF", "1024"),
        ("PRISM_STRATUM_VARDIFF_MIN_DIFF", "1024"),
        ("PRISM_STRATUM_VARDIFF_START_DIFF", "4096"),
        ("PRISM_STRATUM_VARDIFF_MAX_DIFF", "65536"),
        ("PRISM_MANIFEST_SIGNING_SEED_HEX_FILE", manifest.as_str()),
        (
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX_FILE",
            ledger.as_str(),
        ),
        (
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX",
            ledger_public_key.as_str(),
        ),
        ("PRISM_POOL_FEE_ENABLED", "1"),
        ("PRISM_POOL_FEE_BPS", "0"),
        ("PRISM_POOL_FEE_RECIPIENT_ID", "pool-fee"),
    ];
    let development = qbit_prism_server::config::DEVELOPMENT_POOL_FEE_P2MR_PROGRAM_HEX;
    for (program, refused) in [
        (development.to_owned(), true),
        (development.to_ascii_uppercase(), true),
        (PROGRAM.to_owned(), false),
    ] {
        let settings: Vec<_> = production
            .iter()
            .copied()
            .chain([("PRISM_POOL_FEE_P2MR_PROGRAM_HEX", program.as_str())])
            .collect();
        let output = command(&["check-config"], &settings).await;
        let error = stderr(&output);
        let rejected = error.contains("production rejects the development pool fee recipient");
        assert_eq!(rejected, refused, "{program}: {error}");
        if !refused {
            assert!(output.status.success(), "{program}: {error}");
        }
    }
}

/// #535: a fresh lab setup, `.env.example` copied to `.env`, is a valid
/// configuration in either settlement mode, and the recipient instructions
/// it gives an operator hold: exactly one of an address and a program.
#[tokio::test]
async fn env_example_is_a_valid_configuration_in_either_settlement_mode() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../.env.example");
    let example: Vec<(String, String)> = dotenvy::from_path_iter(path)
        .unwrap()
        .map(Result::unwrap)
        .filter(|(name, _)| name.starts_with("PRISM_") || name.starts_with("QBIT_"))
        .collect();
    assert!(
        example.contains(&(
            "PRISM_POOL_FEE_P2MR_PROGRAM_HEX".into(),
            qbit_prism_server::config::DEVELOPMENT_POOL_FEE_P2MR_PROGRAM_HEX.into(),
        )),
        ".env.example must carry the development recipient"
    );
    let with = |overrides: &[(&'static str, &'static str)]| {
        let mut settings: Vec<(&str, &str)> = example
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .filter(|(name, _)| !overrides.iter().any(|(set, _)| set == name))
            .collect();
        // The Makefile's signing preflight creates the seed files .env.example
        // names; a lab check-config uses the test seeds instead.
        settings.push(("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1"));
        settings.extend(overrides);
        settings
    };
    for ctv in ["0", "1"] {
        let mode = ("PRISM_CTV_SETTLEMENT_ENABLED", ctv);
        let output = command(&["check-config"], &with(&[mode])).await;
        assert!(output.status.success(), "{ctv}: {}", stderr(&output));

        let own_address = [mode, ("PRISM_POOL_FEE_ADDRESS", "qbrt1pool")];
        let output = command(&["check-config"], &with(&own_address)).await;
        assert!(
            stderr(&output).contains("configure exactly one pool fee address or P2MR program"),
            "{ctv}: {}",
            stderr(&output)
        );
        let own_address = [
            mode,
            ("PRISM_POOL_FEE_ADDRESS", "qbrt1pool"),
            ("PRISM_POOL_FEE_P2MR_PROGRAM_HEX", ""),
            ("PRISM_POOL_FEE_RECIPIENT_ID", ""),
        ];
        let output = command(&["check-config"], &with(&own_address)).await;
        assert!(output.status.success(), "{ctv}: {}", stderr(&output));

        let disabled = [mode, ("PRISM_POOL_FEE_ENABLED", "0")];
        let output = command(&["check-config"], &with(&disabled)).await;
        assert!(!output.status.success(), "{ctv}: accepted");
        assert!(
            stderr(&output).contains("which every settlement mode requires"),
            "{ctv}: {}",
            stderr(&output)
        );
    }
}
