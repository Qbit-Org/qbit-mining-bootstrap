use crate::{
    config::{self, Config},
    coordinator::Coordinator,
    ledger::{live_instances, unavailable_live_instances, LiveInstancesReport},
};
use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use serde::Serialize;
use serde_json::{json, Value};
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
    /// Inspect a cluster halt or reconcile and record an operator recovery.
    FatalState {
        #[command(subcommand)]
        command: FatalStateCommand,
    },
    /// Validate compact target bits and print Prism's exact scaled difficulty.
    HeaderDifficulty {
        #[arg(long)]
        bits: String,
    },
    /// Apply the additive PostgreSQL migration after stopping Python writers.
    Migrate,
    /// Import legacy filesystem audit bodies into shared PostgreSQL storage.
    ImportAudits {
        /// Audit root, defaulting to PRISM_AUDIT_DIR when nonempty.
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

#[derive(Subcommand)]
enum FatalStateCommand {
    /// Print the stored halt; exits nonzero when the cluster is halted.
    Show,
    /// Reconcile a stopped/drained cluster and durably record why it was cleared.
    Clear {
        #[arg(long)]
        reason: String,
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
            config::check_environment()?;
            let config = Config::from_env()?;
            crate::rollups::settings_from_env()?;
            crate::stratum::StratumConfig::from_env()?.highdiff_config()?;
            crate::api::ApiConfig::from_env()?;
            crate::api::public_service::ServiceConfig::from_env()?;
            println!(
                "PRISM configuration valid; {} runtime workers",
                config.runtime_workers
            );
            Ok(())
        }
        Command::Healthcheck { url, public_api } => healthcheck(url, public_api).await,
        Command::SelfCheck => self_check().await,
        Command::FatalState { command } => fatal_state(command).await,
        Command::HeaderDifficulty { bits } => {
            let compact = crate::codec::parse_u32_hex(&bits)?;
            let target = crate::codec::target_from_compact(compact)?;
            println!("{}", crate::codec::scaled_target_difficulty(&target)?);
            Ok(())
        }
        Command::Migrate => {
            let config = config::DatabaseConfig::from_env()?;
            let ledger =
                crate::ledger::Ledger::connect_operator(&config.database_url, true).await?;
            println!("PRISM PostgreSQL schema ready");
            ledger.pool.close().await;
            Ok(())
        }
        Command::ImportAudits { root } => {
            let root = audit_root(root);
            let config = config::DatabaseConfig::from_env()?;
            let ledger_public_key = config::DatabaseConfig::ledger_public_key()?;
            let ledger = crate::ledger::Ledger::connect(
                &config.database_url,
                config.instance_id,
                config.database_connections,
                false,
            )
            .await?;
            let count = ledger
                .import_legacy_audits(root.as_deref(), &ledger_public_key)
                .await?;
            println!("Imported {count} audit bodies");
            Ok(())
        }
        Command::BackfillCtv => {
            let config = config::DatabaseConfig::from_env()?;
            let ledger_public_key = config::DatabaseConfig::ledger_public_key()?;
            let ledger = crate::ledger::Ledger::connect(
                &config.database_url,
                config.instance_id,
                config.database_connections,
                false,
            )
            .await?;
            let count = ledger.backfill_ctv(&ledger_public_key).await?;
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

fn audit_root(root: Option<PathBuf>) -> Option<PathBuf> {
    root.or_else(|| config::optional("PRISM_AUDIT_DIR").map(PathBuf::from))
}

async fn fatal_state(command: FatalStateCommand) -> Result<()> {
    match command {
        FatalStateCommand::Show => {
            let url =
                config::optional("PRISM_DATABASE_URL").context("PRISM_DATABASE_URL is required")?;
            let ledger = crate::ledger::Ledger::connect_operator(&url, false).await?;
            let state = ledger.fatal_state().await?;
            ledger.pool.close().await;
            println!("{}", serde_json::to_string_pretty(&state)?);
            ensure!(
                state["halted"] == false,
                "cluster halted: {}",
                state["fatal_error"].as_str().unwrap_or("unknown")
            );
            Ok(())
        }
        FatalStateCommand::Clear { reason } => {
            ensure!(
                !reason.trim().is_empty() && reason.len() <= 4096,
                "--reason must contain 1 to 4096 bytes of nonblank text"
            );
            let config = Config::from_env()?;
            let ledger =
                crate::ledger::Ledger::connect_operator(&config.database_url, false).await?;
            let result = ledger.clear_fatal_state(&config, &reason).await;
            ledger.pool.close().await;
            println!("{}", serde_json::to_string_pretty(&result?)?);
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
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        // A readiness endpoint must not redirect a bearer credential elsewhere.
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut request = client.get(url);
    if !public_api {
        if let Some(token) = config::secret("PRISM_OPERATOR_BEARER_TOKEN")? {
            request = request.bearer_auth(token);
        }
    }
    let response = request.send().await?;
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
            crate::api::ApiConfig::default().health_stale_after(),
        ),
    };
    let result = async {
        let config = Config::from_env()?;
        let freshness =
            crate::api::health_stale_after(crate::api::health_refresh_interval_from_env()?);
        report.instance_id = Some(config.instance_id.clone());
        // Snapshot before Coordinator::new: Ledger::connect writes a "starting"
        // heartbeat, which must not manufacture an additional live frontend.
        report.live_instances = live_instances(&config.database_url, freshness).await;
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
mod configuration_tests {
    use super::*;

    #[test]
    fn audit_root_env_and_cli_precedence() {
        if let Ok(case) = std::env::var("AUDIT_ROOT_TEST_CASE") {
            let args = if case == "cli" {
                vec!["prism", "import-audits", "--root", "/explicit/audits"]
            } else {
                vec!["prism", "import-audits"]
            };
            let Some(Command::ImportAudits { root }) = Cli::try_parse_from(args).unwrap().command
            else {
                panic!("wrong command");
            };
            let expected = match case.as_str() {
                "unset" | "empty" | "blank" => None,
                "env" => Some(PathBuf::from("/mounted/audits")),
                "cli" => Some(PathBuf::from("/explicit/audits")),
                _ => panic!("unknown case"),
            };
            assert_eq!(audit_root(root), expected);
            return;
        }
        for case in ["unset", "empty", "blank", "env", "cli"] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "tools::configuration_tests::audit_root_env_and_cli_precedence",
                    "--nocapture",
                ])
                .env_clear()
                .env("AUDIT_ROOT_TEST_CASE", case);
            if case != "unset" {
                let value = match case {
                    "empty" => "",
                    "blank" => "  ",
                    _ => "/mounted/audits",
                };
                command.env("PRISM_AUDIT_DIR", value);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{case}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
