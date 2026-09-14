//! Each environment case runs in a clean child process: configuration
//! regressions must not race other tests by changing the shared runner's
//! environment.
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use qbit_prism_server::{
    api::{
        health_refresh_interval_from_env, public_service, router, ApiConfig, ApiState,
        CacheLifetime,
    },
    config::Config,
};
use std::{process::Command, time::Duration};
use tower::ServiceExt;

const CASE: &str = "API_CONFIG_TEST_CASE";

/// Run `test` in a child with only `settings` in its environment.
fn child(test: &str, case: &str, settings: &[(&str, &str)]) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env_clear()
        .env(CASE, case)
        .envs(settings.iter().copied())
        .output()
        .unwrap()
}

#[test]
fn public_and_coordinator_rpc_settings_match_in_isolated_processes() {
    if let Ok(expected) = std::env::var(CASE) {
        let expected: (String, String, String) = serde_json::from_str(&expected).unwrap();
        let public = ApiConfig::from_env().unwrap();
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
        let expected = serde_json::to_string(expected).unwrap();
        let mut settings = settings.to_vec();
        settings.extend([
            (
                "PRISM_DATABASE_URL",
                "postgresql://operator@127.0.0.1:1/offline",
            ),
            ("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1"),
            ("PRISM_RUNTIME_WORKERS", "2"),
            ("QBIT_CHAIN", "regtest"),
        ]);
        let output = child(
            "public_and_coordinator_rpc_settings_match_in_isolated_processes",
            &expected,
            &settings,
        );
        assert!(
            output.status.success(),
            "{name}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// Malformed values fail startup naming the setting, never echoing its value.
#[test]
fn malformed_api_settings_fail_without_silent_fallback() {
    const TEST: &str = "malformed_api_settings_fail_without_silent_fallback";
    if let Ok(case) = std::env::var(CASE) {
        let (name, value): (String, String) = serde_json::from_str(&case).unwrap();
        let error = match name.as_str() {
            "PRISM_PUBLIC_API_PORT"
            | "PRISM_PUBLIC_REPLICA_MAX_LAG_SECONDS"
            | "PRISM_PUBLIC_READINESS_PROBE_INTERVAL_SECONDS" => {
                public_service::ServiceConfig::from_env()
                    .err()
                    .expect("public service configuration must fail")
            }
            _ => ApiConfig::from_env()
                .err()
                .expect("API configuration must fail"),
        };
        let error = format!("{error:#}");
        assert!(error.contains(&name), "{name}: {error}");
        // Short numbers can legitimately appear inside a documented range.
        if value.len() > 5 {
            assert!(!error.contains(&value), "{name} leaked its value: {error}");
        }
        return;
    }
    for (name, value) in [
        ("PRISM_STRATUM_PORT", "70000"),
        ("PRISM_PUBLIC_POOL_FEE_BPS", "10001"),
        ("PRISM_PUBLIC_POOL_FEE_BPS", "fee-bps-value"),
        ("PRISM_PUBLIC_MINIMUM_PAYOUT_BITS", "payout-bits-value"),
        ("PRISM_PAYOUT_MIN_OUTPUT_SATS", "legacy-sats-value"),
        ("PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS", "-100000"),
        ("PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS", "86401"),
        (
            "PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS",
            "timeout-value",
        ),
        ("PRISM_PUBLIC_CACHE_ENABLED", "maybe-cache"),
        ("PRISM_PUBLIC_CACHE_MAX_ENTRIES", "0"),
        ("PRISM_PUBLIC_CACHE_MAX_RESPONSE_BYTES", "-12345"),
        (
            "PRISM_PUBLIC_CACHE_MAX_PAYLOAD_BYTES",
            "payload-bytes-value",
        ),
        ("PRISM_PUBLIC_CACHE_TTL_SECONDS", "ttl-value"),
        (
            "PRISM_PUBLIC_CACHE_STALE_WHILE_REVALIDATE_SECONDS",
            "-54321",
        ),
        ("PRISM_PUBLIC_CONFIG_CACHE_TTL_SECONDS", "config-ttl-value"),
        (
            "PRISM_PUBLIC_ARTIFACT_CACHE_STALE_WHILE_REVALIDATE_SECONDS",
            "artifact-swr-value",
        ),
        ("PRISM_PUBLIC_AGGREGATE_CACHE_TTL_SECONDS", "3.5"),
        ("PRISM_PUBLIC_CACHE_DEBUG_HEADERS", "debug-flag-value"),
        ("PRISM_POSTGRES_READ_CONCURRENCY", "0"),
        ("PRISM_POSTGRES_READ_CONCURRENCY", "concurrency-value"),
        ("PRISM_PUBLIC_HASHRATE_SMOOTHING_SECONDS", "90000"),
        ("PRISM_STRATUM_HIGHDIFF_PORT", "highdiff-port-value"),
        ("PRISM_HEALTH_REFRESH_SECONDS", "0"),
        ("PRISM_HEALTH_REFRESH_SECONDS", "1.5"),
        ("PRISM_HEALTH_REFRESH_SECONDS", "refresh-value"),
        ("PRISM_OPERATOR_BEARER_TOKEN", "short-token"),
        (
            "PRISM_OPERATOR_BEARER_TOKEN",
            "private operator token value",
        ),
        ("PRISM_PUBLIC_API_PORT", "public-port-value"),
        ("PRISM_PUBLIC_REPLICA_MAX_LAG_SECONDS", "0"),
        (
            "PRISM_PUBLIC_READINESS_PROBE_INTERVAL_SECONDS",
            "probe-value",
        ),
    ] {
        let case = serde_json::to_string(&(name, value)).unwrap();
        let output = child(TEST, &case, &[(name, value)]);
        assert!(
            output.status.success(),
            "{name}={value}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn operator_token_file_and_nondefault_settings_are_parsed_at_startup() {
    const TEST: &str = "operator_token_file_and_nondefault_settings_are_parsed_at_startup";
    if let Ok(case) = std::env::var(CASE) {
        if case == "conflict" {
            let error = format!("{:#}", ApiConfig::from_env().err().unwrap());
            assert!(error.contains("PRISM_OPERATOR_BEARER_TOKEN"), "{error}");
            assert!(!error.contains("private-direct-token"), "{error}");
            return;
        }
        let config = ApiConfig::from_env().unwrap();
        assert_eq!(
            config.operator_bearer_token.as_deref(),
            Some("file-operator-token-0123")
        );
        assert_eq!(config.health_refresh_interval, Duration::from_secs(6));
        assert_eq!(config.health_stale_after(), Duration::from_secs(18));
        assert_eq!(
            health_refresh_interval_from_env().unwrap(),
            Duration::from_secs(6)
        );
        assert_eq!(config.stratum_port, 3350);
        assert_eq!(config.pool_fee_bps, 150);
        assert_eq!(config.minimum_payout_bits, 777);
        assert_eq!(config.read_timeout, Duration::from_secs(7));
        assert_eq!(config.read_concurrency, 9);
        assert!(!config.cache_enabled);
        assert!(config.cache_debug_headers);
        assert_eq!(config.cache_max_entries, 12);
        assert_eq!(config.cache_max_bytes, 4096);
        assert_eq!(
            config.cache_lifetimes.default,
            CacheLifetime {
                ttl: 7,
                stale_while_revalidate: 0
            }
        );
        assert_eq!(config.cache_lifetimes.artifact.ttl, 600);
        assert_eq!(config.cache_lifetimes.configuration.ttl, 300);
        assert_eq!(config.stratum_highdiff_port, None);
        assert_eq!(config.hashrate_smoothing_seconds, 0);
        assert_eq!(config.configuration_label, "Operator label");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let token_file = dir.path().join("token");
    std::fs::write(&token_file, "file-operator-token-0123\n").unwrap();
    let token_file = token_file.to_str().unwrap();
    let output = child(
        TEST,
        "custom",
        &[
            ("PRISM_OPERATOR_BEARER_TOKEN_FILE", token_file),
            ("PRISM_HEALTH_REFRESH_SECONDS", "6"),
            ("PRISM_STRATUM_PORT", "3350"),
            ("PRISM_PUBLIC_POOL_FEE_BPS", "150"),
            ("PRISM_PUBLIC_MINIMUM_PAYOUT_BITS", "777"),
            ("PRISM_PAYOUT_MIN_OUTPUT_BITS", "invalid-but-shadowed"),
            ("PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS", "7"),
            ("PRISM_POSTGRES_READ_CONCURRENCY", "9"),
            ("PRISM_PUBLIC_CACHE_ENABLED", "off"),
            ("PRISM_PUBLIC_CACHE_DEBUG_HEADERS", "yes"),
            ("PRISM_PUBLIC_CACHE_MAX_ENTRIES", "12"),
            ("PRISM_PUBLIC_CACHE_MAX_RESPONSE_BYTES", "4096"),
            (
                "PRISM_PUBLIC_CACHE_MAX_PAYLOAD_BYTES",
                "invalid-but-shadowed",
            ),
            ("PRISM_PUBLIC_CACHE_TTL_SECONDS", "7"),
            ("PRISM_PUBLIC_CACHE_STALE_WHILE_REVALIDATE_SECONDS", "0"),
            ("PRISM_PUBLIC_ARTIFACT_CACHE_TTL_SECONDS", "600"),
            ("PRISM_STRATUM_HIGHDIFF_PORT", "0"),
            ("PRISM_PUBLIC_HASHRATE_SMOOTHING_SECONDS", "0"),
            ("PRISM_PUBLIC_CONFIGURATION_LABEL", "Operator label"),
        ],
    );
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let output = child(
        TEST,
        "conflict",
        &[
            ("PRISM_OPERATOR_BEARER_TOKEN_FILE", token_file),
            ("PRISM_OPERATOR_BEARER_TOKEN", "private-direct-token"),
        ],
    );
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Only the operator role opens operator credentials: the independent public
/// service starts even when the operator's mounted token file is absent.
#[test]
fn public_role_never_opens_operator_credentials() {
    const TEST: &str = "public_role_never_opens_operator_credentials";
    const MISSING: &str = "/nonexistent/prism-operator-token-path";
    if let Ok(case) = std::env::var(CASE) {
        match case.as_str() {
            "public" => {
                let config = ApiConfig::from_public_env().unwrap();
                assert_eq!(config.operator_bearer_token, None);
                // The real public entry point, stopped as soon as it is serving.
                let (_stop, shutdown) = tokio::sync::watch::channel(true);
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(public_service::run_from_env(shutdown))
                    .unwrap();
            }
            "operator" => {
                let error = format!("{:#}", ApiConfig::from_env().err().unwrap());
                assert!(
                    error.contains("PRISM_OPERATOR_BEARER_TOKEN_FILE"),
                    "{error}"
                );
                assert!(!error.contains(MISSING), "{error}");
            }
            _ => panic!("unknown case {case}"),
        }
        return;
    }
    let token = [("PRISM_OPERATOR_BEARER_TOKEN_FILE", MISSING)];
    for case in ["public", "operator"] {
        let mut settings = token.to_vec();
        settings.extend([
            ("PRISM_PUBLIC_STRATUM_URL", "stratum+tcp://127.0.0.1:3340"),
            (
                "PRISM_DATABASE_URL",
                "postgresql://operator@127.0.0.1:1/offline",
            ),
            ("PRISM_PUBLIC_API_BIND", "127.0.0.1"),
            ("PRISM_PUBLIC_API_PORT", "0"),
        ]);
        let output = child(TEST, case, &settings);
        assert!(
            output.status.success(),
            "{case}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let check_config = |settings: &[(&str, &str)]| {
        Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"))
            .arg("check-config")
            .env_clear()
            .env(
                "PRISM_DATABASE_URL",
                "postgresql://operator@127.0.0.1:1/offline",
            )
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("QBIT_CHAIN", "regtest")
            .envs(settings.iter().copied())
            .output()
            .unwrap()
    };
    let baseline = check_config(&[]);
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stderr)
    );
    let missing = check_config(&token);
    let error = String::from_utf8_lossy(&missing.stderr);
    assert!(!missing.status.success());
    assert!(
        error.contains("PRISM_OPERATOR_BEARER_TOKEN_FILE"),
        "{error}"
    );
    assert!(!error.contains(MISSING), "{error}");
}

fn state(token: Option<&str>) -> ApiState {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
        .unwrap();
    ApiState::new(
        pool,
        ApiConfig {
            operator_bearer_token: token.map(Into::into),
            ..ApiConfig::default()
        },
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
}

async fn request(
    app: &Router,
    method: &str,
    path: &str,
    authorization: Option<&str>,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(value) = authorization {
        builder = builder.header("authorization", value);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, headers, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn operator_bearer_token_guards_every_operator_route_but_not_public_service() {
    const TOKEN: &str = "operator-route-token-0123456789";
    let operator = router(state(Some(TOKEN)));
    let paths = ["/healthz", "/metrics", "/public/v1/mining-configuration"];
    for path in paths {
        for (method, authorization) in [
            ("GET", None),
            ("HEAD", None),
            ("OPTIONS", None),
            ("GET", Some("Bearer operator-route-token-012345678")),
            ("GET", Some("Basic b3BlcmF0b3I=")),
            ("GET", Some(TOKEN)),
        ] {
            let (status, headers, body) = request(&operator, method, path, authorization).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
            assert_eq!(
                headers["www-authenticate"],
                "Bearer realm=\"prism-operator\""
            );
            assert_eq!(headers["cache-control"], "no-store");
            assert!(!body.contains(TOKEN) && !body.contains("012345678"));
        }
        let (status, _, _) =
            request(&operator, "GET", path, Some(&format!("bearer {TOKEN}"))).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "{path}");
    }
    let (status, _, body) = request(
        &operator,
        "GET",
        "/public/v1/mining-configuration",
        Some(&format!("Bearer {TOKEN}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The same state behind the independent public service needs no credential.
    let (public, _) =
        public_service::router(state(Some(TOKEN)), public_service::ServiceConfig::default());
    for path in paths {
        let (status, headers, _) = request(&public, "GET", path, None).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "{path}");
        assert!(!headers.contains_key("www-authenticate"));
    }
    let (status, _, _) = request(&public, "GET", "/public/v1/mining-configuration", None).await;
    assert_eq!(status, StatusCode::OK);

    // Without a configured token the operator listener stays open.
    let open = router(state(None));
    for path in paths {
        let (status, _, _) = request(&open, "GET", path, None).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "{path}");
    }
}
