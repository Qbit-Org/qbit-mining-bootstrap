//! Validate operator configuration in isolated subprocesses without mutating
//! the test runner's environment or contacting PostgreSQL/qbit.
use qbit_pool_builder::ManifestSigningKey;
use std::{
    process::Output,
    time::{Duration, Instant},
};
use tokio::{process::Command, time::timeout};

async fn check(production: bool, settings: &[(&str, &str)]) -> Output {
    configured_command("check-config", production, settings).await
}

async fn configured_command(
    subcommand: &str,
    production: bool,
    settings: &[(&str, &str)],
) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    command.arg(subcommand).kill_on_drop(true);
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
    if production {
        let seed = "94".repeat(32);
        let public_key = ManifestSigningKey::from_seed_hex(&seed)
            .unwrap()
            .public_key_hex();
        command
            .env("QBIT_PRODUCTION", "1")
            .env("QBIT_CHAIN", "mainnet")
            .env("QBIT_EXPECTED_GENESIS_HASH", "ab".repeat(32))
            .env("PRISM_STRATUM_STALE_GRACE_SECONDS", "0")
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "0")
            .env("PRISM_MANIFEST_SIGNING_SEED_HEX", "93".repeat(32))
            .env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX", seed)
            .env("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX", public_key)
            .env("QBIT_RPC_USER", "operator")
            .env("QBIT_RPC_PASSWORD", "test-only-password")
            .env("PRISM_STRATUM_SHARE_DIFF", "16")
            .env("PRISM_STRATUM_VARDIFF_MIN_DIFF", "1")
            .env("PRISM_STRATUM_VARDIFF_START_DIFF", "16")
            .env("PRISM_STRATUM_VARDIFF_MAX_DIFF", "1024");
    }
    for (name, value) in settings {
        command.env(name, value);
    }
    timeout(Duration::from_secs(3), command.output())
        .await
        .expect("configuration validation attempted network access or stalled")
        .unwrap()
}

async fn rejects(production: bool, settings: &[(&str, &str)], message: &str) {
    let output = check(production, settings).await;
    assert!(
        !output.status.success(),
        "invalid configuration accepted: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains(message), "expected {message:?}, got {error}");
    assert!(
        !error.contains("test-only-password"),
        "configuration error exposed credentials"
    );
}

#[tokio::test]
async fn valid_regtest_and_production_configuration_are_checked_without_services() {
    let started = Instant::now();
    for production in [false, true] {
        let settings = if production {
            vec![
                ("QBIT_CHAIN", "mainnet"),
                ("PRISM_STRATUM_STALE_GRACE_SECONDS", "0"),
            ]
        } else {
            Vec::new()
        };
        let output = check(production, &settings).await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("configuration valid"));
    }
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn large_resource_budgets_fail_instead_of_truncating_or_panicking() {
    for (name, value) in [
        ("PRISM_DATABASE_MAX_CONNECTIONS", "4294967312"), // previously truncated to 16
        ("PRISM_DATABASE_MAX_CONNECTIONS", "1025"),
        ("PRISM_DATABASE_MAX_CONNECTIONS", "3"),
        ("PRISM_JOB_BUILD_EXECUTOR_WORKERS", "18446744073709551615"),
        ("PRISM_JOB_BUILD_EXECUTOR_WORKERS", "11"), // runtime permits 2 + 8 blocking threads
        ("PRISM_RUNTIME_WORKERS", "1025"),
        ("PRISM_SHARE_COMMIT_TIMEOUT_SECONDS", "1e100"),
        ("PRISM_STRATUM_EXTRANONCE2_SIZE", "4294967304"),
        (
            "PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS",
            "18446744073709551615",
        ),
        ("PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION", "1161"),
        ("PRISM_HASHRATE_ROLLUP_BATCH_SHARES", "0"),
        ("PRISM_HASHRATE_ROLLUP_BATCH_SHARES", "100001"),
        ("PRISM_HASHRATE_ROLLUP_INTERVAL_SECONDS", "1e-300"),
    ] {
        rejects(false, &[(name, value)], name).await;
    }
    for name in [
        "PRISM_SUBMIT_TIP_MAX_AGE_SECONDS",
        "PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS",
    ] {
        for value in ["-1", "86401", "NaN", "inf", "1e309"] {
            rejects(false, &[(name, value)], name).await;
        }
    }
    for name in [
        "PRISM_SUBMIT_TIP_MAX_AGE_SECONDS",
        "PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS",
    ] {
        let output = check(false, &[(name, "0")]).await;
        assert!(
            output.status.success(),
            "zero {name} rejected: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    rejects(
        true,
        &[("PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS", "0")],
        "production mode requires a positive PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS",
    )
    .await;
}

#[tokio::test]
async fn reconnect_and_initial_convergence_settings_are_bounded_and_optional() {
    for (name, value, message) in [
        (
            "PRISM_STRATUM_VARDIFF_RESUME_TTL_SECONDS",
            "86401",
            "resume retention",
        ),
        (
            "PRISM_STRATUM_VARDIFF_RESUME_MAX_ENTRIES",
            "1000001",
            "resume retention",
        ),
        (
            "PRISM_STRATUM_VARDIFF_RESUME_MAX_START_FACTOR",
            "NaN",
            "PRISM_STRATUM_VARDIFF_RESUME_MAX_START_FACTOR",
        ),
        (
            "PRISM_STRATUM_VARDIFF_INITIAL_MAX_STEP_UP",
            "3",
            "initial vardiff convergence",
        ),
        (
            "PRISM_STRATUM_VARDIFF_INITIAL_MIN_STEP_UP",
            "0.5",
            "initial vardiff convergence",
        ),
        (
            "PRISM_STRATUM_VARDIFF_INITIAL_MIN_SHARES",
            "0",
            "initial vardiff convergence",
        ),
        (
            "PRISM_STRATUM_VARDIFF_INITIAL_MIN_SECONDS",
            "inf",
            "finite and positive",
        ),
    ] {
        rejects(false, &[(name, value)], message).await;
    }
    let output = check(
        false,
        &[
            ("PRISM_STRATUM_VARDIFF_RESUME_TTL_SECONDS", "0"),
            ("PRISM_STRATUM_VARDIFF_RESUME_MAX_ENTRIES", "0"),
            ("PRISM_STRATUM_VARDIFF_INITIAL_CONVERGENCE", "0"),
            ("PRISM_STRATUM_VARDIFF_INITIAL_MAX_STEP_UP", "ignored"),
            ("PRISM_STRATUM_VARDIFF_INITIAL_MIN_SHARES", "ignored"),
            ("PRISM_STRATUM_VARDIFF_INITIAL_MIN_SECONDS", "ignored"),
            ("PRISM_STRATUM_VARDIFF_INITIAL_MIN_STEP_UP", "ignored"),
        ],
    )
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn production_rejects_test_seeds_and_reused_or_mismatched_trust_keys() {
    rejects(
        true,
        &[("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")],
        "production rejects PRISM_ALLOW_TEST_SIGNING_SEEDS",
    )
    .await;
    rejects(
        true,
        &[("PRISM_MANIFEST_SIGNING_SEED_HEX", &"11".repeat(32))],
        "test signing seeds require",
    )
    .await;
    rejects(
        true,
        &[("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX", &"00".repeat(32))],
        "trusted ledger public key does not match",
    )
    .await;
    rejects(
        true,
        &[(
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX",
            &"93".repeat(32),
        )],
        "manifest and ledger signing keys must differ",
    )
    .await;
}

#[tokio::test]
async fn mainnet_aliases_allow_bounded_stale_grace_and_enforce_production_rules() {
    let output = check(
        true,
        &[
            ("QBIT_CHAIN", "mainnet"),
            ("PRISM_STRATUM_STALE_GRACE_SECONDS", "3"),
        ],
    )
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    rejects(
        false,
        &[
            ("QBIT_CHAIN", " MAINNET "),
            ("PRISM_STRATUM_STALE_GRACE_SECONDS", "0"),
        ],
        "production rejects PRISM_ALLOW_TEST_SIGNING_SEEDS",
    )
    .await;
    rejects(
        false,
        &[("QBIT_CHAIN", "made-up-chain")],
        "QBIT_CHAIN must name",
    )
    .await;
    for chain in ["test", "testnet3", "testnet4", "signet"] {
        let output = check(false, &[("QBIT_CHAIN", chain)]).await;
        assert!(
            output.status.success(),
            "known chain {chain} rejected: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn mainnet_requires_a_valid_genesis_pin_before_startup() {
    for chain in ["main", "mainnet"] {
        for pin in ["", "abc", &"g".repeat(64), &"a".repeat(63), &"a".repeat(65)] {
            rejects(
                true,
                &[("QBIT_CHAIN", chain), ("QBIT_EXPECTED_GENESIS_HASH", pin)],
                "QBIT_EXPECTED_GENESIS_HASH",
            )
            .await;
        }
        let output = check(
            true,
            &[
                ("QBIT_CHAIN", chain),
                ("QBIT_EXPECTED_GENESIS_HASH", &"AB".repeat(32)),
            ],
        )
        .await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    rejects(
        false,
        &[("QBIT_EXPECTED_GENESIS_HASH", "invalid")],
        "QBIT_EXPECTED_GENESIS_HASH",
    )
    .await;
}

#[tokio::test]
async fn production_flags_reject_regtest_and_readiness_budgets_are_validated() {
    for flag in ["QBIT_PRODUCTION", "QBIT_TOOLS_PRODUCTION"] {
        rejects(
            false,
            &[(flag, "1")],
            "production mode rejects regtest QBIT_CHAIN",
        )
        .await;
    }
    for (name, value) in [
        ("PRISM_MIN_PEERS", "0"),
        ("PRISM_MIN_PEERS", "-1"),
        ("PRISM_MIN_PEERS", "18446744073709551616"),
        ("PRISM_TEMPLATE_MAX_AGE_SECONDS", "-1"),
        ("PRISM_TEMPLATE_MAX_AGE_SECONDS", "0.5"),
        ("PRISM_TEMPLATE_MAX_AGE_SECONDS", "86401"),
    ] {
        rejects(false, &[(name, value)], name).await;
    }
    let output = check(
        false,
        &[
            ("PRISM_MIN_PEERS", "2"),
            ("PRISM_TEMPLATE_MAX_AGE_SECONDS", "0"),
        ],
    )
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn retired_writer_sessions_and_invalid_backend_urls_fail_before_startup() {
    rejects(
        false,
        &[("PRISM_LEDGER_WRITER_SESSION_TOKEN", "old-python-session")],
        "fixed ledger writer sessions are retired",
    )
    .await;
    rejects(
        false,
        &[("PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN", "1")],
        "fixed ledger writer sessions are retired",
    )
    .await;
    rejects(
        false,
        &[("PRISM_ALLOW_MEMORY_LEDGER", "1")],
        "PRISM_ALLOW_MEMORY_LEDGER is retired",
    )
    .await;
    rejects(
        false,
        &[("PRISM_DATABASE_URL", "sqlite://local")],
        "PRISM_DATABASE_URL must use postgres",
    )
    .await;
    rejects(
        false,
        &[("QBIT_RPC_URL", "ftp://127.0.0.1/")],
        "qbit RPC URL must use http or https",
    )
    .await;
}

#[tokio::test]
async fn ipv6_rpc_host_configuration_is_valid_without_an_explicit_url() {
    let output = check(
        false,
        &[
            ("QBIT_RPC_URL", ""),
            ("QBIT_RPC_HOST", "::1"),
            ("QBIT_RPC_PORT", "1"),
        ],
    )
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn healthcheck_reaches_ipv6_loopback_and_wildcard_binds() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    for bind in ["::1", "::"] {
        let listener = TcpListener::bind(("::1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
                assert!(request.len() < 4096);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /healthz HTTP/1.1\r\n"));
            assert!(request
                .to_ascii_lowercase()
                .contains(&format!("host: [::1]:{port}\r\n")));
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}").await.unwrap();
        });
        let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
        command
            .arg("healthcheck")
            .kill_on_drop(true)
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("PRISM_AUDIT_BIND", bind)
            .env("PRISM_AUDIT_PORT", port.to_string());
        for name in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            command.env_remove(name);
        }
        let output = timeout(Duration::from_secs(5), command.output())
            .await
            .unwrap()
            .unwrap();
        if !output.status.success() {
            server.abort();
        }
        assert!(
            output.status.success(),
            "bind {bind}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn ctv_fee_premiums_are_validated_before_automatic_or_explicit_fee_work() {
    for market_rate in ["", "1000"] {
        for premium in ["0", "-1", "invalid", "18446744073709551616"] {
            rejects(
                false,
                &[
                    ("PRISM_CTV_SETTLEMENT_ENABLED", "1"),
                    (
                        "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
                        market_rate,
                    ),
                    ("PRISM_CTV_FANOUT_FEE_PREMIUM_BPS", premium),
                ],
                "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS",
            )
            .await;
        }
        let output = check(
            false,
            &[
                ("PRISM_CTV_SETTLEMENT_ENABLED", "1"),
                (
                    "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
                    market_rate,
                ),
                ("PRISM_CTV_FANOUT_FEE_PREMIUM_BPS", "15000"),
            ],
        )
        .await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn run_rejects_invalid_highdiff_settings_before_connecting() {
    for settings in [
        vec![("PRISM_STRATUM_HIGHDIFF_PORT", "0")],
        vec![
            ("PRISM_STRATUM_HIGHDIFF_PORT", "4334"),
            ("PRISM_STRATUM_HIGHDIFF_SHARE_DIFF", "invalid"),
        ],
    ] {
        // The fixture's database and RPC endpoints are intentionally unreachable.
        // Reject these settings before either external dependency is contacted.
        let output = configured_command("run", false, &settings).await;
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("invalid PRISM_STRATUM_HIGHDIFF"), "{error}");
        assert!(!error.contains("test-only-password"));
    }
}
