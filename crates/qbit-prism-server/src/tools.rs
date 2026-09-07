use crate::{
    config::{self, Config},
    coordinator::Coordinator,
};
use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
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
    /// Validate local configuration and key pairing without starting listeners.
    CheckConfig,
    /// Probe local HTTP health (does not require signing keys or database access).
    Healthcheck {
        #[arg(long)]
        url: Option<String>,
    },
    /// Check node identity, database integrity, API readiness and cluster settings.
    SelfCheck,
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
        Command::CheckConfig => {
            let config = Config::from_env()?;
            crate::stratum::StratumConfig::from_env()?.highdiff_config()?;
            println!(
                "PRISM configuration valid; {} runtime workers",
                config.runtime_workers
            );
            Ok(())
        }
        Command::Healthcheck { url } => healthcheck(url).await,
        Command::SelfCheck => self_check().await,
        Command::Migrate => {
            let config = Config::from_env()?;
            let ledger = crate::ledger::Ledger::connect(
                &config.database_url,
                config.instance_id,
                config.database_connections,
                true,
            )
            .await?;
            println!("PRISM PostgreSQL schema ready");
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
            let coordinator = Coordinator::new(Config::from_env()?).await?;
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

async fn healthcheck(url: Option<String>) -> Result<()> {
    let host = diagnostic_host(&config::value("PRISM_AUDIT_BIND", "127.0.0.1"));
    let url = url.unwrap_or_else(|| {
        format!(
            "http://{host}:{}/healthz",
            config::value("PRISM_AUDIT_PORT", "3341")
        )
    });
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

async fn self_check() -> Result<()> {
    let coordinator = Coordinator::new(Config::from_env()?).await?;
    coordinator.refresh_once().await?;
    let integrity: Value = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
        .fetch_one(&coordinator.ledger.pool)
        .await?;
    for field in ["mismatch_count", "current_drift_count"] {
        ensure!(
            integrity[field].as_u64() == Some(0),
            "carry-forward integrity failure in {field}: {integrity}"
        );
    }
    let durability:Vec<(String,String)>=sqlx::query_as("SELECT name,setting FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit') ORDER BY name").fetch_all(&coordinator.ledger.pool).await?;
    for (name, value) in &durability {
        ensure!(value != "off", "PostgreSQL {name} is disabled");
    }
    healthcheck(None).await?;
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
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"schema":"qbit.prism.self-check.v2","ok":true,"instance_id":coordinator.config.instance_id,"health":coordinator.health().await,"carry_forward_integrity":integrity,"durability":durability})
        )?
    );
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
