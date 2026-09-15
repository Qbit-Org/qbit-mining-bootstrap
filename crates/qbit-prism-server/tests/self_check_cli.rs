//! Preserve the operator-facing self-check report in isolated subprocesses.
//! The gated database case uses a private schema and a loopback fake qbit RPC.
use anyhow::{ensure, Result};
use axum::{routing::post, Json, Router};
use qbit_prism_server::ledger::Ledger;
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use std::{process::Output, time::Duration};
use tokio::{process::Command, time::timeout};

async fn self_check(settings: &[(&str, &str)], deadline: Duration) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    command
        .arg("self-check")
        .kill_on_drop(true)
        .env_clear()
        .env("PRISM_RUNTIME_WORKERS", "2")
        .env("PRISM_INSTANCE_ID", "self-check-cli")
        // Port zero cannot address a listening TCP service. Both dependencies
        // remain unavailable even when a developer has local services running.
        .env(
            "PRISM_DATABASE_URL",
            "postgresql://operator:test-only-password@127.0.0.1:0/offline",
        )
        .env("QBIT_RPC_URL", "http://127.0.0.1:0/")
        .env("PRISM_RPC_TIMEOUT_SECONDS", "1")
        .env("QBIT_CHAIN", "regtest")
        .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1");
    for (name, value) in settings {
        command.env(name, value);
    }
    timeout(deadline, command.output())
        .await
        .expect("self-check did not finish within its dependency timeout budget")
        .expect("run self-check subprocess")
}

fn failed_report(output: &Output) -> Value {
    assert!(
        output.status.code().is_some_and(|code| code != 0),
        "self-check must exit with a nonzero status: {:?}",
        output.status
    );
    for stream in [&output.stdout, &output.stderr] {
        assert!(
            !String::from_utf8_lossy(stream).contains("test-only-password"),
            "self-check exposed database credentials"
        );
    }
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "self-check must emit one complete JSON report: {error}; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn unavailable_report(instance_id: Option<&str>, status: &str, warning: &str) -> Value {
    json!({
        "schema": "qbit.prism.self-check.v2",
        "ok": false,
        "instance_id": instance_id,
        "health": null,
        "carry_forward_integrity": null,
        "durability": null,
        "audit_completeness": null,
        "live_instances": {
            "status": status,
            "observed_at": null,
            "clock": "PostgreSQL clock_timestamp() via PRISM_DATABASE_URL",
            "freshness_seconds": 15.0,
            "count": null,
            "instance_ids": null,
            "instances": null,
            "stale_instances": null,
            "inactive_instances": null,
            "unknown_instances": null,
            "single_instance": null,
            "ha_warning": warning
        }
    })
}

#[tokio::test]
async fn invalid_configuration_emits_complete_unknown_report_and_fails() {
    let output = self_check(&[("QBIT_CHAIN", "invalid")], Duration::from_secs(3)).await;
    assert_eq!(
        failed_report(&output),
        unavailable_report(
            None,
            "unknown",
            "Heartbeat not sampled because configuration is unavailable; HA is unknown"
        )
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("QBIT_CHAIN"));
}

#[tokio::test]
async fn unreachable_database_emits_complete_failed_report_without_zero_count() {
    // The heartbeat sample is bounded at five seconds; local checks then
    // attempt RPC with the one-second timeout configured by the helper.
    let output = self_check(&[], Duration::from_secs(8)).await;
    assert_eq!(
        failed_report(&output),
        unavailable_report(
            Some("self-check-cli"),
            "failed",
            "Heartbeat read failed or exceeded 5 seconds; HA is unknown"
        )
    );
    // The RPC diagnostic proves a failed heartbeat sample did not suppress
    // the remaining local checks or get replaced with an empty-cluster report.
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("qbit RPC getblockhash transport failed"),
        "expected the local RPC check to run after heartbeat failure, got: {error}"
    );
}

#[tokio::test]
async fn slow_health_cadence_is_reported_even_when_the_database_is_unavailable() {
    let output = self_check(
        &[("PRISM_HEALTH_REFRESH_SECONDS", "20")],
        Duration::from_secs(8),
    )
    .await;
    let mut expected = unavailable_report(
        Some("self-check-cli"),
        "failed",
        "Heartbeat read failed or exceeded 5 seconds; HA is unknown",
    );
    expected["live_instances"]["freshness_seconds"] = json!(60.0);
    assert_eq!(failed_report(&output), expected);
}

#[tokio::test]
async fn invalid_health_cadence_fails_before_sampling_heartbeats() {
    let output = self_check(
        &[("PRISM_HEALTH_REFRESH_SECONDS", "0")],
        Duration::from_secs(3),
    )
    .await;
    assert_eq!(
        failed_report(&output),
        unavailable_report(
            None,
            "unknown",
            "Heartbeat not sampled because configuration is unavailable; HA is unknown"
        )
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("PRISM_HEALTH_REFRESH_SECONDS"));
}

async fn startup_only_rpc(Json(request): Json<Value>) -> Json<Value> {
    let result = match request["method"].as_str() {
        Some("getblockhash") if request["params"] == json!([0]) => json!("00".repeat(32)),
        Some("getblockchaininfo") => json!({
            "chain": "regtest",
            "initialblockdownload": false,
            "blocks": 100,
            "headers": 100,
            "bestblockhash": "ab".repeat(32),
            "chainwork": "01"
        }),
        _ => {
            return Json(json!({
                "id": request["id"],
                "result": null,
                "error": {"code": -32601, "message": "self-check refresh intentionally unavailable"}
            }));
        }
    };
    Json(json!({"id": request["id"], "result": result, "error": null}))
}

#[tokio::test]
async fn postgres_self_check_samples_before_local_startup_and_reports_missing_table() -> Result<()>
{
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = sqlx::PgPool::connect(&raw).await?;
    let schema = format!("self_check_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut database_url = url::Url::parse(&raw)?;
    database_url
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let database_url = database_url.to_string();
    let result = postgres_reports(&database_url).await;
    // Close the fixture even when a database assertion returns an error.
    let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    result?;
    cleanup?;
    Ok(())
}

async fn postgres_reports(database_url: &str) -> Result<()> {
    // The configured schema is intentionally empty. A successful connection
    // with a missing heartbeat table must still report failure, never zero.
    let output = self_check(
        &[("PRISM_DATABASE_URL", database_url)],
        Duration::from_secs(8),
    )
    .await;
    ensure!(
        failed_report(&output)
            == unavailable_report(
                Some("self-check-cli"),
                "failed",
                "Heartbeat read failed or exceeded 5 seconds; HA is unknown"
            ),
        "missing heartbeat table did not preserve the complete failed report"
    );

    let ledger = Ledger::connect(database_url, "self-check-cli".into(), 4, true).await?;
    let result = async {
        sample_before_startup(database_url, &ledger).await?;
        sample_slow_heartbeats(database_url, &ledger).await
    }
    .await;
    ledger.pool.close().await;
    result
}

async fn sample_slow_heartbeats(database_url: &str, ledger: &Ledger) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_prism_instances(instance_id,heartbeat_at,status) VALUES
         ('frontend-a',clock_timestamp()-interval '20 seconds',$1),
         ('frontend-b',clock_timestamp()-interval '30 seconds',$1),
         ('expired',clock_timestamp()-interval '61 seconds',$1)",
    )
    .bind(json!({"schema":"qbit.prism.audit-health.v1","ready":true}))
    .execute(&ledger.pool)
    .await?;
    let output = self_check(
        &[
            ("PRISM_DATABASE_URL", database_url),
            ("PRISM_HEALTH_REFRESH_SECONDS", "20"),
        ],
        Duration::from_secs(8),
    )
    .await;
    let report = failed_report(&output);
    let live = &report["live_instances"];
    ensure!(
        live["status"] == "observed"
            && live["freshness_seconds"] == 60.0
            && live["count"] == 2
            && live["instance_ids"] == json!(["frontend-a", "frontend-b"])
            && live["single_instance"] == false
            && live["ha_warning"].is_null()
            && live["stale_instances"][0]["instance_id"] == "expired",
        "slow heartbeat cadence did not preserve HA and expire stale rows: {live}"
    );
    Ok(())
}

async fn sample_before_startup(database_url: &str, ledger: &Ledger) -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}/", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(startup_only_rpc))).await
    });
    // Seed the exact pre-existing JSON shape, bypassing the new typed writer.
    let legacy_health = json!({
        "schema": "qbit.prism.audit-health.v1",
        "ready": false,
        "legacy_detail": {"reason": "waiting for template"}
    });
    sqlx::query("UPDATE qbit_prism_instances SET heartbeat_at=clock_timestamp(),status=$1 WHERE instance_id='self-check-cli'")
        .bind(&legacy_health).execute(&ledger.pool).await?;
    let output = self_check(
        &[
            ("PRISM_DATABASE_URL", database_url),
            ("QBIT_RPC_URL", &rpc_url),
        ],
        Duration::from_secs(15),
    )
    .await;
    server.abort();
    let report = failed_report(&output);
    let error = String::from_utf8_lossy(&output.stderr);
    ensure!(
        error.contains("self-check refresh intentionally unavailable"),
        "expected local startup to finish before the deliberate refresh failure: {error}"
    );
    let live = &report["live_instances"];
    let row = &live["instances"][0];
    ensure!(
        live["observed_at"]
            .as_str()
            .is_some_and(|at| !at.is_empty())
            && row["heartbeat_at"].as_str().is_some()
            && row["age_seconds"]
                .as_f64()
                .is_some_and(|age| (0.0..=15.0).contains(&age)),
        "expected a fresh heartbeat sampled with the database clock: {live}"
    );
    let mut expected = unavailable_report(Some("self-check-cli"), "observed", "");
    expected["audit_completeness"] = json!({
        "missing_stored_bodies": 0,
        "missing_canonical_bytes": 0,
    });
    expected["live_instances"] = json!({
        "status": "observed",
        "observed_at": live["observed_at"],
        "clock": "PostgreSQL clock_timestamp() via PRISM_DATABASE_URL",
        "freshness_seconds": 15.0,
        "count": 1,
        "instance_ids": ["self-check-cli"],
        "instances": [{
            "instance_id": "self-check-cli",
            "heartbeat_at": row["heartbeat_at"],
            "age_seconds": row["age_seconds"],
            "status": legacy_health
        }],
        "stale_instances": [],
        "inactive_instances": [],
        "unknown_instances": [],
        "single_instance": true,
        "ha_warning": "Fewer than two live frontends observed; do not present this deployment as HA"
    });
    ensure!(
        report == expected,
        "self-check did not retain the complete sample taken before local startup: {report}"
    );
    let stored: Value = sqlx::query_scalar(
        "SELECT status FROM qbit_prism_instances WHERE instance_id='self-check-cli'",
    )
    .fetch_one(&ledger.pool)
    .await?;
    ensure!(
        stored["state"] == "starting"
            && stored["session_owner_token"].as_str().is_some()
            && stored.get("schema").is_none(),
        "local startup did not replace the stored legacy heartbeat: {stored}"
    );
    Ok(())
}
