//! Real command boundaries, isolated from ambient PostgreSQL credentials.
use std::{process::Output, time::Duration};
use tokio::{process::Command, time::timeout};

fn command(action: &str, production: bool, dsn: Option<&str>, carrier: Option<&str>) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    command
        .arg(action)
        .env_clear()
        .kill_on_drop(true)
        .env("HOME", "/nonexistent-prism-config-test")
        .env("PGPASSFILE", "/nonexistent-prism-config-test")
        .env("PRISM_RUNTIME_WORKERS", "2")
        .env("QBIT_CHAIN", "signet")
        .env("QBIT_PRODUCTION", if production { "1" } else { "0" })
        .env("RUST_LOG", "trace")
        .env(
            "PRISM_PUBLIC_STRATUM_URL",
            "stratum+tcp://pool.example.invalid:3340",
        )
        .env("PRISM_PUBLIC_API_BIND", "127.0.0.1")
        .env("PRISM_PUBLIC_API_PORT", "0")
        // Neither Compose input nor bootstrap credentials belong to this role.
        .env(
            "PRISM_POSTGRES_PASSWORD",
            "fixture-bootstrap-must-not-be-used",
        )
        .env(
            "PRISM_PUBLIC_POSTGRES_PASSWORD",
            "fixture-compose-input-only",
        );
    if let Some(dsn) = dsn {
        command.env("PRISM_DATABASE_URL", dsn);
    }
    if let Some(carrier) = carrier {
        command.env("PGPASSWORD", carrier);
    }
    command
}

async fn output(mut command: Command) -> Output {
    timeout(Duration::from_secs(3), command.output())
        .await
        .expect("configuration validation attempted network access or stalled")
        .unwrap()
}

fn assert_result(output: Output, valid: bool, message: &str) {
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.success(), valid, "unexpected config outcome");
    for secret in [
        "fixture-secret",
        "fixture-bootstrap",
        "fixture-compose",
        "change-this",
        "ch%61nge",
    ] {
        assert!(
            !text.contains(secret),
            "configuration diagnostics exposed a credential"
        );
    }
    assert!(
        text.contains(message),
        "missing expected value-free diagnostic: {message}"
    );
}

#[tokio::test]
async fn malformed_and_missing_dsn_fail_at_validation_and_real_startup() {
    for action in ["check-public-database-config", "public-api"] {
        for (dsn, message) in [
            (None, "PRISM_DATABASE_URL is required"),
            (Some(""), "PRISM_DATABASE_URL is required"),
            (Some("  "), "PRISM_DATABASE_URL is required"),
            (Some("fixture-secret"), "invalid public PRISM_DATABASE_URL"),
            (
                Some("postgres://reader:fixture-secret@localhost:invalid/db"),
                "invalid public PRISM_DATABASE_URL",
            ),
            (
                Some("postgres://reader:fixture-secret@localhost/db?sslmode=fixture-secret"),
                "invalid public PRISM_DATABASE_URL",
            ),
            (
                Some("postgres://reader:fixture-secret@localhost/db?port=fixture-secret"),
                "invalid public PRISM_DATABASE_URL",
            ),
            (
                Some("postgres://reader:%ff@localhost/db"),
                "invalid public PRISM_DATABASE_URL",
            ),
            (
                Some("sqlite://reader:fixture-secret@localhost/db"),
                "must use postgres or postgresql",
            ),
        ] {
            for production in [false, true] {
                assert_result(
                    output(command(action, production, dsn, None)).await,
                    false,
                    message,
                );
            }
        }
    }
}

#[tokio::test]
async fn production_rejects_effective_defaults_at_validation_and_real_startup() {
    for action in ["check-public-database-config", "public-api"] {
        for (dsn, carrier) in [
            ("postgres://qbit:change-this@localhost/db", None),
            ("postgres://qbit:ch%61nge-this@localhost/db", None),
            (
                "postgres://qbit:fixture-secret@localhost/db?password=ch%61nge-this",
                None,
            ),
            ("postgres://qbit@localhost/db", Some("change-this")),
            (
                "postgres://qbit@localhost/db",
                Some("prefix-change-this-suffix"),
            ),
        ] {
            assert_result(
                output(command(action, true, Some(dsn), carrier)).await,
                false,
                "production requires non-default database credentials",
            );
            assert_result(
                output(command(
                    "check-public-database-config",
                    false,
                    Some(dsn),
                    carrier,
                ))
                .await,
                true,
                "public database configuration valid",
            );
        }
    }
}

#[tokio::test]
async fn sqlx_supported_password_sources_and_precedence_remain_valid() {
    for dsn in [
        "postgres://qbit@localhost/db",
        "postgresql://qbit:fixture-secret@localhost/db",
        "postgres://reader%40realm:fixture-secret%24%20%23%3A%40%2F%5C@localhost/db",
        "postgres://qbit@localhost/db?password=fixture-secret%24%20%23",
        "postgres://qbit:fixture-secret@localhost/db?password=",
        "postgres:///db?host=%2Ftmp&user=reader%40realm&password=fixture-secret",
        // SQLx accepts unknown parameters, but their values must not be logged.
        "postgres://qbit@localhost/db?fixture-secret=fixture-secret",
    ] {
        for carrier in [None, Some(""), Some("fixture-secret")] {
            assert_result(
                output(command(
                    "check-public-database-config",
                    true,
                    Some(dsn),
                    carrier,
                ))
                .await,
                true,
                "public database configuration valid",
            );
        }
    }
    // An unused default carrier must not invalidate an explicit reader password.
    assert_result(
        output(command(
            "check-public-database-config",
            true,
            Some("postgres://qbit:fixture-secret@localhost/db"),
            Some("change-this"),
        ))
        .await,
        true,
        "public database configuration valid",
    );
}

#[tokio::test]
async fn native_password_file_and_all_production_selectors_use_the_same_policy() {
    let dir = tempfile::tempdir().unwrap();
    let passfile = dir.path().join("pgpass");
    for (password, valid) in [("fixture-secret", true), ("change-this", false)] {
        std::fs::write(&passfile, format!("localhost:5432:db:qbit:{password}\n")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&passfile, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        for (selector, value) in [
            ("QBIT_PRODUCTION", "1"),
            ("QBIT_TOOLS_PRODUCTION", "true"),
            ("QBIT_CHAIN", " MAINNET "),
            ("QBIT_CHAIN", "main"),
        ] {
            let mut cmd = command(
                "check-public-database-config",
                false,
                Some("postgres://qbit@localhost/db"),
                None,
            );
            cmd.env(selector, value).env("PGPASSFILE", &passfile);
            assert_result(
                output(cmd).await,
                valid,
                if valid {
                    "public database configuration valid"
                } else {
                    "production requires non-default database credentials"
                },
            );
        }
    }
}

#[tokio::test]
async fn direct_public_binary_authenticates_only_with_effective_reader_credentials(
) -> anyhow::Result<()> {
    use anyhow::{ensure, Context};
    use qbit_prism_server::ledger::Ledger;
    use qbit_prism_test_gate as gate;
    use sqlx::PgPool;

    let Some(database) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&database)
        .await
        .context("connect disposable fixture database")?;
    let name = format!("public_auth_{}", uuid::Uuid::new_v4().simple());
    let password = "fixture-secret$ #:@/\\tail";
    sqlx::query(&format!("CREATE SCHEMA {name}"))
        .execute(&admin)
        .await?;
    let result = async {
        let mut writer = url::Url::parse(&database)?;
        writer.query_pairs_mut().append_pair("options", &format!("-csearch_path={name}"));
        let ledger = Ledger::connect(writer.as_str(), "public-auth-fixture".into(), 4, true).await?;
        ledger.pool.close().await;
        sqlx::raw_sql(&format!(
            "CREATE ROLE {name} LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION; \
             GRANT USAGE ON SCHEMA {name} TO {name}; GRANT SELECT ON ALL TABLES IN SCHEMA {name} TO {name};"
        )).execute(&admin).await.map_err(|_| anyhow::anyhow!("provision disposable reader failed"))?;
        let mut reader = writer.clone();
        reader.set_username(&name).map_err(|_| anyhow::anyhow!("set fixture reader role"))?;
        reader.set_password(None).map_err(|_| anyhow::anyhow!("clear fixture password"))?;
        let mut inline = reader.clone();
        let encoded = percent_encoding::utf8_percent_encode(password, percent_encoding::NON_ALPHANUMERIC).to_string();
        inline.set_password(Some(&encoded)).map_err(|_| anyhow::anyhow!("set fixture password"))?;
        let mut query = reader.clone();
        query.query_pairs_mut().append_pair("password", password);
        let mut empty_query = reader.clone();
        empty_query.query_pairs_mut().append_pair("password", "");
        let dir = tempfile::tempdir()?;
        let passfile = dir.path().join("pgpass");
        // SQLx 0.8 takes the last password field verbatim; preserve its native
        // file semantics rather than adding libpq escape handling here.
        std::fs::write(&passfile, format!("*:*:*:{name}:{password}\n"))?;
        #[cfg(unix)] {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&passfile, std::fs::Permissions::from_mode(0o600))?;
        }
        for (label, dsn, carrier, use_file, healthy) in [
            ("absent", &reader, None, false, false),
            ("empty", &reader, Some(""), false, false),
            ("incorrect", &reader, Some("fixture-secret-incorrect"), false, false),
            ("carrier", &reader, Some(password), false, true),
            ("encoded", &inline, Some("fixture-secret-incorrect"), false, true),
            ("query", &query, Some("fixture-secret-incorrect"), false, true),
            ("empty-query", &empty_query, Some(password), false, false),
            ("passfile", &reader, None, true, true),
        ] {
            let mut validate = command("check-public-database-config", true, Some(dsn.as_str()), carrier);
            if use_file { validate.env("PGPASSFILE", &passfile); }
            ensure!(output(validate).await.status.success(), "{label}: credential config rejected");
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            let port = listener.local_addr()?.port();
            drop(listener);
            let mut cmd = command("public-api", true, Some(dsn.as_str()), carrier);
            cmd.env("PRISM_PUBLIC_API_PORT", port.to_string())
                .env("RUST_LOG", "warn")
                .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped());
            if use_file { cmd.env("PGPASSFILE", &passfile); }
            let mut child = cmd.spawn()?;
            let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(1)).build()?;
            let probe = timeout(Duration::from_secs(15), async {
                loop {
                    if let Ok(response) = client.get(format!("http://127.0.0.1:{port}/healthz")).send().await {
                        return Ok::<_, anyhow::Error>((response.status(), response.json::<serde_json::Value>().await?));
                    }
                    ensure!(child.try_wait()?.is_none(), "{label}: public process exited during startup");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }).await;
            child.kill().await?;
            let logs = child.wait_with_output().await?;
            let (status, body) = probe.context("public listener startup timed out")??;
            ensure!(status.as_u16() == if healthy { 200 } else { 503 }, "{label}: wrong readiness status");
            ensure!(body["database_ready"] == healthy, "{label}: wrong database readiness");
            ensure!(!body.to_string().contains("fixture-secret"), "{label}: health exposed credentials");
            ensure!(!String::from_utf8_lossy(&logs.stderr).contains("fixture-secret"), "{label}: logs exposed credentials");
        }
        Ok::<_, anyhow::Error>(())
    }.await;
    let schema_cleanup = sqlx::query(&format!("DROP SCHEMA {name} CASCADE"))
        .execute(&admin)
        .await;
    let role_cleanup = sqlx::query(&format!("DROP ROLE IF EXISTS {name}"))
        .execute(&admin)
        .await;
    admin.close().await;
    result?;
    schema_cleanup?;
    role_cleanup?;
    Ok(())
}
