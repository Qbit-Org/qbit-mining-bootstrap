use crate::{
    config::{self, Config},
    coordinator::Coordinator,
};
use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::Connection;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Parser)]
#[command(
    version,
    about = "Native multi-instance PRISM mining and operator tools"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Serve Stratum, audit and dashboard APIs, and settlement workers.
    Run,
    /// Serve the public read API using a separate database pool or replica.
    PublicApi,
    /// Validate local configuration and key pairing without starting listeners.
    CheckConfig,
    /// Probe HTTP or Stratum readiness without signing keys or database access.
    Healthcheck {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        public_api: bool,
    },
    /// Check node identity, database integrity, API readiness and cluster settings.
    SelfCheck,
    /// Validate compact target bits and print Prism's exact scaled difficulty.
    HeaderDifficulty {
        #[arg(long)]
        bits: String,
    },
    /// Apply the additive PostgreSQL migration after stopping Python writers.
    Migrate,
    /// Import legacy filesystem audit bodies into shared PostgreSQL storage.
    ImportAudits {
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Reconstruct missing CTV artifacts from verified stored audit bundles.
    BackfillCtv,
    /// Process one batch of durable, mature CTV fanout claims.
    BroadcastCtv,
    /// Validate a complete Stratum-to-PostgreSQL capacity qualification artifact.
    CapacityEvidence(crate::capacity::Args),
    /// Measure the actual native payout/audit builder on a synthetic share window.
    Benchmark {
        #[arg(long, default_value_t = 1000)]
        shares: usize,
        #[arg(long, default_value_t = 10)]
        miners: usize,
        #[arg(long, default_value_t = 10)]
        iterations: usize,
        #[arg(long)]
        output_json: Option<PathBuf>,
    },
}

pub async fn run() -> Result<()> {
    match Cli::parse().command.unwrap_or(Command::Run) {
        Command::Run => crate::server::run(Config::from_env()?).await,
        Command::PublicApi => {
            let (shutdown, receiver) = tokio::sync::watch::channel(false);
            let service = crate::api::public_service::run_from_env(receiver);
            tokio::pin!(service);
            tokio::select! {
                result = &mut service => result,
                result = crate::server::signal() => {
                    result?;
                    shutdown.send_replace(true);
                    tokio::time::timeout(Duration::from_secs(30), service)
                        .await.context("public API shutdown timed out")?
                }
            }
        }
        Command::CheckConfig => {
            let config = Config::from_env()?;
            crate::rollups::settings_from_env()?;
            crate::stratum::StratumConfig::from_env()?.highdiff_config()?;
            println!(
                "PRISM configuration valid; {} runtime workers",
                config.runtime_workers
            );
            Ok(())
        }
        Command::Healthcheck { url, public_api } => healthcheck(url, public_api).await,
        Command::SelfCheck => self_check().await,
        Command::HeaderDifficulty { bits } => {
            let compact = crate::codec::parse_u32_hex(&bits)?;
            let target = crate::codec::target_from_compact(compact)?;
            println!("{}", crate::codec::scaled_target_difficulty(&target)?);
            Ok(())
        }
        Command::Migrate => {
            let config = Config::from_env()?;
            let ledger = crate::ledger::Ledger::connect(
                &config.database_url,
                config.instance_id,
                config.database_connections,
                true,
            )
            .await?;
            let source = ledger
                .migration_source()
                .await?
                .map(|source| {
                    format!(
                        "{} (2.x.x release {})",
                        source.source_state,
                        source.source_release.as_deref().unwrap_or("none")
                    )
                })
                .unwrap_or_else(|| "unrecorded".to_owned());
            println!(
                "PRISM PostgreSQL schema version {} ready; database source: {source}",
                crate::ledger::REQUIRED_SCHEMA_VERSION
            );
            ledger.pool.close().await;
            Ok(())
        }
        Command::ImportAudits { root } => {
            let config = Config::from_env()?;
            let ledger = crate::ledger::Ledger::connect(
                &config.database_url,
                config.instance_id,
                config.database_connections,
                false,
            )
            .await?;
            let count = ledger
                .import_legacy_audits(root.as_deref(), &config.ledger_public_key)
                .await?;
            println!("Imported {count} audit bodies");
            Ok(())
        }
        Command::BackfillCtv => {
            let config = Config::from_env()?;
            let ledger = crate::ledger::Ledger::connect(
                &config.database_url,
                config.instance_id,
                config.database_connections,
                false,
            )
            .await?;
            let count = ledger.backfill_ctv(&config.ledger_public_key).await?;
            println!("Backfilled {count} CTV manifest sets");
            Ok(())
        }
        Command::BroadcastCtv => {
            let coordinator = Coordinator::new(
                Config::from_env()?,
                std::sync::Arc::new(crate::metrics::Metrics::default()),
            )
            .await?;
            coordinator.refresh_once().await?;
            let count = crate::broadcaster::run_once(&coordinator).await?;
            println!("Processed {count} CTV fanouts");
            Ok(())
        }
        Command::CapacityEvidence(args) => crate::capacity::run(args),
        Command::Benchmark {
            shares,
            miners,
            iterations,
            output_json,
        } => {
            let result = tokio::task::spawn_blocking(move || benchmark(shares, miners, iterations))
                .await??;
            let text = serde_json::to_string_pretty(&result)?;
            if let Some(path) = output_json {
                tokio::fs::write(path, &text).await?;
            }
            println!("{text}");
            Ok(())
        }
    }
}

fn diagnostic_host(bind: &str) -> String {
    let host = match bind.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(address)) if address.is_unspecified() => "127.0.0.1",
        Ok(std::net::IpAddr::V6(address)) if address.is_unspecified() => "::1",
        _ => bind,
    };
    config::authority_host(host)
}

async fn healthcheck(url: Option<String>, public_api: bool) -> Result<()> {
    let (bind_name, port_name, default_port) = if public_api {
        ("PRISM_PUBLIC_API_BIND", "PRISM_PUBLIC_API_PORT", 3342u16)
    } else {
        ("PRISM_AUDIT_BIND", "PRISM_AUDIT_PORT", 3341u16)
    };
    let port = config::number(port_name, default_port)?;
    if url.is_none() && port == 0 && !public_api {
        return stratum_healthcheck().await;
    }
    let host = diagnostic_host(&config::value(bind_name, "127.0.0.1"));
    let url = url.unwrap_or_else(|| format!("http://{host}:{port}/healthz"));
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?
        .get(url)
        .send()
        .await?;
    ensure!(
        response.status().is_success(),
        "PRISM is unhealthy (HTTP {})",
        response.status()
    );
    let value: Value = response.json().await?;
    ensure!(value["ok"] == true, "PRISM health is not ready");
    Ok(())
}

async fn stratum_healthcheck() -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let mut port = config::number("PRISM_STRATUM_PORT", 3340u16)?;
    let mut bind = config::value("PRISM_STRATUM_BIND", "127.0.0.1");
    if port == 0 {
        port = config::number("PRISM_STRATUM_HIGHDIFF_PORT", 0u16)?;
        bind = config::optional("PRISM_STRATUM_HIGHDIFF_BIND").unwrap_or(bind);
    }
    ensure!(port > 0, "no configured listener for PRISM health probe");
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut socket =
            tokio::net::TcpStream::connect(format!("{}:{port}", diagnostic_host(&bind)))
                .await
                .context("connect Stratum health probe")?;
        socket
            .write_all(b"{\"id\":1,\"method\":\"mining.get_health\",\"params\":[]}\n")
            .await?;
        let mut reader = BufReader::new(socket).take(4097);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await?;
        ensure!(
            line.len() <= 4096 && line.last() == Some(&b'\n'),
            "invalid Stratum health response frame"
        );
        let response: Value = serde_json::from_slice(&line)?;
        ensure!(
            response["id"] == 1
                && response["error"].is_null()
                && response["result"]["ready"] == true,
            "PRISM Stratum health is not ready"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("Stratum health probe timed out")?
}

#[derive(Serialize)]
struct SelfCheckReport {
    schema: &'static str,
    ok: bool,
    instance_id: Option<String>,
    health: Option<Value>,
    carry_forward_integrity: Option<Value>,
    durability: Option<Vec<(String, String)>>,
    live_instances: LiveInstancesReport,
}

async fn self_check() -> Result<()> {
    let mut report = SelfCheckReport {
        schema: "qbit.prism.self-check.v2",
        ok: false,
        instance_id: None,
        health: None,
        carry_forward_integrity: None,
        durability: None,
        live_instances: unavailable_live_instances(
            "unknown",
            "Heartbeat not sampled because configuration is unavailable; HA is unknown",
        ),
    };
    let result = async {
        let config = Config::from_env()?;
        report.instance_id = Some(config.instance_id.clone());
        // Snapshot before Coordinator::new: Ledger::connect writes a "starting"
        // heartbeat, which must not manufacture an additional live frontend.
        report.live_instances = live_instances(&config.database_url).await;
        // A failed heartbeat sample must not suppress the remaining local checks.
        self_check_local(config, &mut report).await?;
        ensure!(
            report.live_instances.status != "failed",
            "could not read cluster heartbeats"
        );
        Ok(())
    }
    .await;
    report.ok = result.is_ok();
    println!("{}", serde_json::to_string_pretty(&report)?);
    result
}

async fn self_check_local(config: Config, report: &mut SelfCheckReport) -> Result<()> {
    let coordinator = Coordinator::new(
        config,
        std::sync::Arc::new(crate::metrics::Metrics::default()),
    )
    .await?;
    coordinator.refresh_once().await?;
    let integrity: Value = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
        .fetch_one(&coordinator.ledger.pool)
        .await?;
    report.health = Some(coordinator.health().await);
    report.carry_forward_integrity = Some(integrity.clone());
    for field in ["mismatch_count", "current_drift_count"] {
        ensure!(
            integrity[field].as_u64() == Some(0),
            "carry-forward integrity failure in {field}: {integrity}"
        );
    }
    let durability:Vec<(String,String)>=sqlx::query_as("SELECT name,setting FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit') ORDER BY name").fetch_all(&coordinator.ledger.pool).await?;
    report.durability = Some(durability.clone());
    for (name, value) in &durability {
        ensure!(value != "off", "PostgreSQL {name} is disabled");
    }
    healthcheck(None, false).await?;
    let stratum = crate::stratum::StratumConfig::from_env()?;
    if let Some(highdiff) = stratum.highdiff_config()? {
        let recent: Option<String> = sqlx::query_scalar(
            "SELECT miner_id FROM qbit_share_ledger ORDER BY share_seq DESC LIMIT 1",
        )
        .fetch_optional(&coordinator.ledger.pool)
        .await?;
        let username=config::optional("PRISM_SELF_CHECK_ADDRESS").or_else(||coordinator.config.username_fallback.clone()).or_else(||coordinator.config.fee_address.clone()).or(recent).context("set PRISM_SELF_CHECK_ADDRESS to a valid P2MR address to probe highdiff on an empty pool")?;
        let bind = config::optional("PRISM_STRATUM_HIGHDIFF_BIND")
            .unwrap_or_else(|| config::value("PRISM_STRATUM_BIND", "127.0.0.1"));
        let host = diagnostic_host(&bind);
        let port = config::number("PRISM_STRATUM_HIGHDIFF_PORT", 4334u16)?;
        let actual = crate::stratum::probe_first_difficulty(
            &format!("{host}:{port}"),
            &username,
            Duration::from_secs(15),
        )
        .await?;
        ensure!(
            actual >= highdiff.minimum_difficulty,
            "highdiff listener advertised a difficulty below its floor"
        );
    }
    Ok(())
}

// The server publishes every two seconds. Use the configured database's clock
// and a fixed 15-second window, not the caller's clock or an HA election.
const INSTANCE_FRESHNESS_SECONDS: f64 = 15.0;
const LIVE_INSTANCES_QUERY: &str = r#"
WITH sample AS (SELECT clock_timestamp() AS observed_at)
SELECT observed_at::text, COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
        'instance_id', instance_id, 'heartbeat_at', heartbeat_at,
        'age_seconds', extract(epoch FROM (observed_at - heartbeat_at)),
        'status', status
    ) ORDER BY instance_id) FROM qbit_prism_instances
), '[]'::jsonb) FROM sample
"#;

#[derive(Serialize)]
struct LiveInstancesReport {
    status: &'static str,
    observed_at: Option<String>,
    clock: &'static str,
    freshness_seconds: f64,
    count: Option<usize>,
    instance_ids: Option<Vec<Value>>,
    instances: Option<Vec<Value>>,
    stale_instances: Option<Vec<Value>>,
    inactive_instances: Option<Vec<Value>>,
    unknown_instances: Option<Vec<Value>>,
    single_instance: Option<bool>,
    ha_warning: Option<&'static str>,
}

fn unavailable_live_instances(status: &'static str, warning: &'static str) -> LiveInstancesReport {
    LiveInstancesReport {
        status,
        observed_at: None,
        clock: "PostgreSQL clock_timestamp() via PRISM_DATABASE_URL",
        freshness_seconds: INSTANCE_FRESHNESS_SECONDS,
        count: None,
        instance_ids: None,
        instances: None,
        stale_instances: None,
        inactive_instances: None,
        unknown_instances: None,
        single_instance: None,
        ha_warning: Some(warning),
    }
}

async fn live_instances(database_url: &str) -> LiveInstancesReport {
    // A separate read-only connection avoids Ledger::connect's heartbeat write.
    // Config already resolved/validated the DSN; do not read environment again.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut connection = sqlx::PgConnection::connect(database_url).await?;
        sqlx::query("SET default_transaction_read_only = on")
            .execute(&mut connection)
            .await?;
        sqlx::query_as::<_, (String, Value)>(LIVE_INSTANCES_QUERY)
            .fetch_one(&mut connection)
            .await
    })
    .await;
    match result {
        Ok(Ok((observed_at, rows))) => summarize_live_instances(&observed_at, rows),
        // Do not print connection errors: they may contain a credentialed DSN.
        _ => unavailable_live_instances(
            "failed",
            "Heartbeat read failed or exceeded 5 seconds; HA is unknown",
        ),
    }
}

fn summarize_live_instances(observed_at: &str, rows: Value) -> LiveInstancesReport {
    let mut live = Vec::new();
    let mut stale = Vec::new();
    let mut inactive = Vec::new();
    let mut unknown = Vec::new();
    for row in rows.as_array().into_iter().flatten() {
        match row["age_seconds"].as_f64() {
            Some(age) if age > INSTANCE_FRESHNESS_SECONDS => stale.push(row.clone()),
            Some(age) if age >= 0.0 => {
                if row["status"]["schema"] == "qbit.prism.audit-health.v1"
                    && row["status"]["ready"].is_boolean()
                {
                    live.push(row.clone());
                } else if matches!(
                    row["status"]["state"].as_str(),
                    Some("starting" | "stopped")
                ) {
                    inactive.push(row.clone());
                } else {
                    unknown.push(row.clone());
                }
            }
            _ => unknown.push(row.clone()),
        }
    }
    let status = if !unknown.is_empty() {
        "unknown"
    } else if !live.is_empty() {
        "observed"
    } else if !stale.is_empty() {
        "stale"
    } else if !inactive.is_empty() {
        "inactive"
    } else {
        "empty"
    };
    LiveInstancesReport {
        status,
        observed_at: Some(observed_at.to_owned()),
        clock: "PostgreSQL clock_timestamp() via PRISM_DATABASE_URL",
        freshness_seconds: INSTANCE_FRESHNESS_SECONDS,
        count: unknown.is_empty().then_some(live.len()),
        instance_ids: Some(live.iter().map(|row| row["instance_id"].clone()).collect()),
        single_instance: (unknown.is_empty() && !live.is_empty()).then_some(live.len() == 1),
        ha_warning: if !unknown.is_empty() {
            Some("Unrecognized or future-dated heartbeats; HA is unknown")
        } else if live.len() < 2 {
            Some("Fewer than two live frontends observed; do not present this deployment as HA")
        } else {
            None
        },
        instances: Some(live),
        stale_instances: Some(stale),
        inactive_instances: Some(inactive),
        unknown_instances: Some(unknown),
    }
}

fn benchmark(count: usize, miners: usize, iterations: usize) -> Result<Value> {
    ensure!(
        count > 0
            && count <= 10_000_000
            && miners > 0
            && miners <= count
            && iterations > 0
            && iterations <= 10_000,
        "invalid benchmark dimensions"
    );
    let shares: Vec<_> = (0..count)
        .map(|i| qbit_prism::AcceptedShare {
            share_seq: i as u64 + 1,
            share_id: format!("share-{i}"),
            miner_id: format!("miner-{}", i % miners),
            order_key: format!("miner-{}", i % miners),
            p2mr_program_hex: format!("{:064x}", i % miners + 1),
            share_difficulty: 1,
            network_difficulty: count as u128,
            template_height: 1,
            job_id: "benchmark".into(),
            job_issued_at_ms: 0,
            accepted_at_ms: 0,
            ntime: 1,
            credit_policy: None,
        })
        .collect();
    let key = qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"11".repeat(32))?;
    let ledger_key = qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"22".repeat(32))?;
    let found = qbit_prism::FoundBlock {
        block_height: 2,
        coinbase_value_sats: 5_000_000_000,
        network_difficulty: count as u128,
        anchor_job_issued_at_ms: 1,
    };
    let mut milliseconds = Vec::new();
    let mut last_bytes = 0;
    for _ in 0..iterations {
        let started = Instant::now();
        let bundle = qbit_prism::build_audit_bundle(
            shares.clone(),
            found.clone(),
            vec![],
            qbit_prism::PayoutPolicy::day_one_default(),
            &key,
            &ledger_key,
        )?;
        qbit_prism::verify_audit_bundle_with_ledger_public_key(
            &bundle,
            &ledger_key.public_key_hex(),
        )?;
        milliseconds.push(started.elapsed().as_secs_f64() * 1000.0);
        last_bytes = qbit_prism::canonical_audit_bundle_bytes(&bundle)?.len();
    }
    milliseconds.sort_by(f64::total_cmp);
    Ok(
        json!({"schema":"qbit.prism.native-builder-benchmark.v1","shares":count,"miners":miners,"iterations":iterations,"build_and_verify_p50_ms":milliseconds[iterations/2],"build_and_verify_p99_ms":milliseconds[(iterations*99/100).min(iterations-1)],"canonical_audit_bytes":last_bytes,"engine":"in-process-rust"}),
    )
}

#[cfg(test)]
mod live_instance_tests {
    use super::*;

    fn row(id: &str, age: f64, status: Value) -> Value {
        json!({"instance_id":id, "age_seconds":age, "status":status})
    }

    fn health() -> Value {
        json!({"schema":"qbit.prism.audit-health.v1", "ready":false})
    }

    #[test]
    fn empty_table_is_not_live() {
        let report = summarize_live_instances("db-time", json!([]));
        assert_eq!(report.status, "empty");
        assert_eq!(report.count, Some(0));
        assert_eq!(report.single_instance, None);
    }

    #[test]
    fn stale_rows_do_not_count() {
        let report = summarize_live_instances("db-time", json!([row("old", 16.0, health())]));
        assert_eq!(report.status, "stale");
        assert_eq!(report.count, Some(0));
        assert_eq!(report.single_instance, None);
    }

    #[test]
    fn startup_rows_are_inactive_not_live() {
        let report = summarize_live_instances(
            "db-time",
            json!([
                row("starting", 0.0, json!({"state":"starting"})),
                row("stopped", 0.0, json!({"state":"stopped"}))
            ]),
        );
        assert_eq!(report.status, "inactive");
        assert_eq!(report.count, Some(0));
        assert_eq!(report.inactive_instances.unwrap().len(), 2);
        assert_eq!(report.single_instance, None);
    }

    #[test]
    fn inclusive_freshness_boundary_and_unready_servers_are_live() {
        let report = summarize_live_instances(
            "db-time",
            json!([row("a", 15.0, health()), row("b", 0.0, health())]),
        );
        assert_eq!(report.status, "observed");
        assert_eq!(report.count, Some(2));
        assert_eq!(report.instance_ids, Some(vec![json!("a"), json!("b")]));
        assert_eq!(report.single_instance, Some(false));
    }

    #[test]
    fn single_live_instance_warns() {
        let report = summarize_live_instances("db-time", json!([row("a", 1.0, health())]));
        assert_eq!(report.single_instance, Some(true));
        assert!(report.ha_warning.is_some());
    }

    #[test]
    fn future_dated_row_is_unknown() {
        let report = summarize_live_instances("db-time", json!([row("a", -1.0, health())]));
        assert_eq!(report.status, "unknown");
        assert_eq!(report.count, None);
        assert_eq!(report.single_instance, None);
    }

    #[tokio::test]
    async fn failed_heartbeat_connection_is_not_zero_or_healthy() {
        let report = live_instances("postgresql://127.0.0.1:0/unavailable").await;
        assert_eq!(report.status, "failed");
        assert_eq!(report.count, None);
        assert_eq!(report.observed_at, None);
    }

    #[tokio::test]
    async fn heartbeat_sql_observes_empty_stale_and_missing_table() -> Result<()> {
        let Some(url) = qbit_prism_test_gate::database_url(qbit_prism_test_gate::site!())? else {
            return Ok(());
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        let mut connection = pool.acquire().await?;
        // Connection-local table: never modify deployment heartbeat rows.
        sqlx::raw_sql("SET search_path = pg_temp; CREATE TEMP TABLE qbit_prism_instances (instance_id text, heartbeat_at timestamptz, status jsonb)")
            .execute(&mut *connection).await?;
        let (at, rows) = sqlx::query_as::<_, (String, Value)>(LIVE_INSTANCES_QUERY)
            .fetch_one(&mut *connection)
            .await?;
        let report = summarize_live_instances(&at, rows);
        assert_eq!(report.status, "empty");
        assert_eq!(report.count, Some(0));
        sqlx::query("INSERT INTO qbit_prism_instances VALUES ('old', clock_timestamp() - interval '1 minute', $1)")
            .bind(json!({"schema":"qbit.prism.audit-health.v1", "ready":true}))
            .execute(&mut *connection).await?;
        let (at, rows) = sqlx::query_as::<_, (String, Value)>(LIVE_INSTANCES_QUERY)
            .fetch_one(&mut *connection)
            .await?;
        let report = summarize_live_instances(&at, rows);
        assert_eq!(report.status, "stale");
        assert_eq!(report.count, Some(0));
        assert_eq!(report.stale_instances.unwrap()[0]["instance_id"], "old");
        sqlx::query("DROP TABLE qbit_prism_instances")
            .execute(&mut *connection)
            .await?;
        assert!(sqlx::query_as::<_, (String, Value)>(LIVE_INSTANCES_QUERY)
            .fetch_one(&mut *connection)
            .await
            .is_err());
        drop(connection);
        pool.close().await;
        Ok(())
    }
}
