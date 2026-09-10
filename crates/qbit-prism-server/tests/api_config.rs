//! Each case runs in a clean child process: configuration regressions must not
//! race other tests by changing the shared test runner's environment.
use qbit_prism_server::{api::ApiConfig, config::Config};
use std::process::Command;

#[test]
fn public_and_coordinator_rpc_settings_match_in_isolated_processes() {
    const EXPECTED: &str = "PRISM_TEST_EXPECTED_RPC";
    if let Ok(expected) = std::env::var(EXPECTED) {
        let expected: (String, String, String) = serde_json::from_str(&expected).unwrap();
        let public = ApiConfig::from_env();
        assert_eq!(
            (public.rpc_url, public.rpc_user, public.rpc_password),
            expected
        );
        let coordinator = Config::from_env().unwrap();
        assert_eq!(
            (
                coordinator.rpc_url,
                coordinator.rpc_user,
                coordinator.rpc_password
            ),
            expected
        );
        return;
    }
    type Overrides<'a> = &'a [(&'a str, &'a str)];
    type EndpointAndAuth<'a> = (&'a str, &'a str, &'a str);
    let cases: &[(&str, Overrides<'_>, EndpointAndAuth<'_>)] = &[
        (
            "defaults",
            &[],
            ("http://127.0.0.1:18452/", "qbit", "change-this"),
        ),
        (
            "host",
            &[("QBIT_RPC_HOST", "qbitd")],
            ("http://qbitd:18452/", "qbit", "change-this"),
        ),
        (
            "port",
            &[("QBIT_RPC_PORT", "28452")],
            ("http://127.0.0.1:28452/", "qbit", "change-this"),
        ),
        (
            "host_and_port",
            &[("QBIT_RPC_HOST", "qbitd"), ("QBIT_RPC_PORT", "28452")],
            ("http://qbitd:28452/", "qbit", "change-this"),
        ),
        (
            "url_precedence_and_auth",
            &[
                ("QBIT_RPC_URL", "https://rpc.example:443/wallet/public"),
                ("QBIT_RPC_HOST", "ignored"),
                ("QBIT_RPC_PORT", "1"),
                ("QBIT_RPC_USER", "public-reader"),
                ("QBIT_RPC_PASSWORD", "test-only-secret"),
            ],
            (
                "https://rpc.example:443/wallet/public",
                "public-reader",
                "test-only-secret",
            ),
        ),
        (
            "blank_url_ipv6",
            &[
                ("QBIT_RPC_URL", "  "),
                ("QBIT_RPC_HOST", "::1"),
                ("QBIT_RPC_PORT", "28452"),
            ],
            ("http://[::1]:28452/", "qbit", "change-this"),
        ),
    ];
    for (name, settings, expected) in cases {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "public_and_coordinator_rpc_settings_match_in_isolated_processes",
                "--nocapture",
            ])
            .env_clear()
            .env(
                "PRISM_DATABASE_URL",
                "postgresql://operator@127.0.0.1:1/offline",
            )
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("QBIT_CHAIN", "regtest")
            .env(EXPECTED, serde_json::to_string(expected).unwrap())
            .envs(settings.iter().copied())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{name}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
