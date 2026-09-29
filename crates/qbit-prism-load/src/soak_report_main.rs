//! `qbit-prism-soak-report`: a deployment soak's hourly check (#575 item 2).
//!
//! Given a Prometheus that scrapes the PRISM servers and a read-only URL of
//! their database, it takes one [`Sample`] per interval -- each server's
//! resident memory and open descriptors, share ACK latency and acknowledged
//! share count from Prometheus; connections, WAL, partition and relation
//! sizes, committed shares and payout divergences from the database --
//! appends it to `<out>/soak-samples.jsonl`, holds every sample so far to the
//! soak gates the regtest soak uses ([`soak::evaluate`]), and writes the
//! verdict to `<out>/soak-report.md`. Endpoints come from flags or the
//! environment, never from the repository.
//!
//! `--once` takes one sample and exits with the verdict (0 pass, 1 fail,
//! 2 unreadable inputs), for cron; without it the check repeats every
//! `--interval-seconds` until stopped. Each run resumes from the samples
//! file, so either way the soak's history is the file's.

use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use qbit_prism_load::soak::{self, Gates, LatencyPoint, LedgerPoint, ProcessPoint, Sample};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "qbit-prism-soak-report",
    about = "Sample a PRISM deployment and hold it to the soak gates",
    version
)]
struct Args {
    /// Prometheus base URL (the one serving /api/v1/query).
    #[arg(long, env = "PRISM_SOAK_PROMETHEUS_URL")]
    prometheus_url: String,
    /// PostgreSQL URL of the PRISM database; a read-only role is enough.
    /// `pg_monitor` adds WAL size, `pg_read_all_stats` the idle-in-transaction
    /// count; without them those checks read unknown and fail.
    #[arg(long, env = "PRISM_SOAK_DATABASE_URL", hide_env_values = true)]
    database_url: String,
    /// Directory the samples and the report are kept in.
    #[arg(long)]
    out: PathBuf,
    /// The soak gates, as JSON. Defaults to the checked-in testnet4 gates.
    #[arg(long)]
    gates: Option<PathBuf>,
    /// Label matchers selecting the PRISM servers' series, without braces,
    /// e.g. `job="prism"`. Empty selects every series of the metric.
    #[arg(long, default_value = "")]
    selector: String,
    /// The label that names one server process.
    #[arg(long, default_value = "instance")]
    instance_label: String,
    /// PromQL returning each server's open file descriptors, labelled by
    /// `--instance-label`. Empty reads the servers' own
    /// `qbit_prism_process_open_fds` under `--selector`.
    #[arg(long, default_value = "")]
    fd_query: String,
    /// Seconds between samples.
    #[arg(long, default_value_t = 3600)]
    interval_seconds: u64,
    /// Take one sample, write the report and exit with the verdict.
    #[arg(long)]
    once: bool,
    /// Seconds before the soak's start that committed shares are counted
    /// from, covering the lag of the first sample's Prometheus scrape.
    #[arg(long, default_value_t = 120.0)]
    scrape_slack_seconds: f64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Args::parse();
    match run(&args).await {
        Ok(true) => std::process::exit(0),
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("qbit-prism-soak-report: error: {error:#}");
            std::process::exit(2);
        }
    }
}

async fn run(args: &Args) -> Result<bool> {
    ensure!(
        args.interval_seconds >= 60,
        "--interval-seconds must be at least 60"
    );
    ensure!(
        args.scrape_slack_seconds.is_finite() && args.scrape_slack_seconds >= 0.0,
        "--scrape-slack-seconds must be finite and not negative"
    );
    let gates = match &args.gates {
        Some(path) => Gates::load(path)?,
        None => serde_json::from_str::<Gates>(include_str!("../soak-gates/testnet4.json"))
            .context("the checked-in testnet4 gates")?,
    };
    gates.validate()?;
    std::fs::create_dir_all(&args.out)
        .with_context(|| format!("creating {}", args.out.display()))?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&args.database_url)
        .await
        .context("connecting to the database")?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    loop {
        let passed = tick(args, &gates, &pool, &http).await?;
        if args.once {
            pool.close().await;
            return Ok(passed);
        }
        tokio::time::sleep(Duration::from_secs(args.interval_seconds)).await;
    }
}

/// One sample, appended; every sample so far evaluated; the report written.
async fn tick(
    args: &Args,
    gates: &Gates,
    pool: &sqlx::PgPool,
    http: &reqwest::Client,
) -> Result<bool> {
    let path = args.out.join(soak::SAMPLES_FILE);
    let mut samples = if path.exists() {
        soak::read_samples(&path)?
    } else {
        Vec::new()
    };
    let sample = take_sample(args, pool, http, &samples).await;
    soak::append_line(&path, &sample)?;
    samples.push(sample);
    let checks = soak::evaluate(&samples, gates);
    let text = soak::markdown("PRISM deployment soak", &samples, &checks);
    std::fs::write(args.out.join(soak::REPORT_FILE), &text)?;
    let passed = qbit_prism_load::gate::passed(&checks);
    println!(
        "{} sample {} at {}: {}",
        if passed { "PASS" } else { "FAIL" },
        samples.len(),
        chrono::Utc::now().to_rfc3339(),
        args.out.join(soak::REPORT_FILE).display()
    );
    Ok(passed)
}

/// Braces around the selector plus `extra` matchers.
fn matchers(selector: &str, extra: &str) -> String {
    let parts: Vec<&str> = [extra, selector]
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect();
    format!("{{{}}}", parts.join(","))
}

/// An instant vector as `(labels, value)` pairs.
async fn query(
    http: &reqwest::Client,
    base: &str,
    promql: &str,
) -> Result<Vec<(BTreeMap<String, String>, f64)>> {
    let url = format!("{}/api/v1/query", base.trim_end_matches('/'));
    let body: Value = http
        .get(&url)
        .query(&[("query", promql)])
        .send()
        .await
        .with_context(|| format!("querying Prometheus for {promql}"))?
        .error_for_status()?
        .json()
        .await?;
    if body["status"] != "success" {
        bail!("Prometheus answered {promql} with {}", body["error"]);
    }
    let mut out = Vec::new();
    for row in body["data"]["result"].as_array().into_iter().flatten() {
        let labels: BTreeMap<String, String> = row["metric"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap_or_default().to_owned()))
            .collect();
        let value = row["value"][1]
            .as_str()
            .and_then(|text| text.parse::<f64>().ok())
            .context("a sample value that is not a number")?;
        out.push((labels, value));
    }
    Ok(out)
}

async fn take_sample(
    args: &Args,
    pool: &sqlx::PgPool,
    http: &reqwest::Client,
    earlier: &[Sample],
) -> Sample {
    let at = chrono::Utc::now();
    let started = earlier.first().map(|s| s.at).unwrap_or(at);
    let elapsed = (at - started).num_milliseconds() as f64 / 1000.0;
    let base = &args.prometheus_url;
    let label = &args.instance_label;
    let previous: BTreeMap<String, ProcessPoint> = earlier
        .last()
        .map(|s| {
            s.processes
                .iter()
                .map(|p| (p.instance.clone(), p.clone()))
                .collect()
        })
        .unwrap_or_default();
    let mut processes: BTreeMap<String, ProcessPoint> = BTreeMap::new();
    let note = |instance: &str| -> ProcessPoint {
        ProcessPoint {
            instance: instance.to_owned(),
            ..ProcessPoint::default()
        }
    };
    let mut unknown_everywhere: Vec<String> = Vec::new();
    match query(
        http,
        base,
        &format!(
            "qbit_prism_process_resident_memory_bytes{}",
            matchers(&args.selector, "")
        ),
    )
    .await
    {
        Ok(rows) => {
            for (labels, value) in rows {
                let instance = labels.get(label).cloned().unwrap_or_default();
                let point = processes
                    .entry(instance.clone())
                    .or_insert_with(|| note(&instance));
                // The server reports -1 when procfs could not be read.
                if value >= 0.0 {
                    point.rss_bytes = Some(value as u64);
                } else {
                    point
                        .unknown
                        .push("the server reported its RSS unknown (-1)".into());
                }
            }
        }
        Err(error) => unknown_everywhere.push(format!("rss: {error:#}")),
    }
    let fd_query = if args.fd_query.is_empty() {
        format!(
            "qbit_prism_process_open_fds{}",
            matchers(&args.selector, "")
        )
    } else {
        args.fd_query.clone()
    };
    match query(http, base, &fd_query).await {
        Ok(rows) => {
            for (labels, value) in rows {
                let instance = labels.get(label).cloned().unwrap_or_default();
                if let Some(point) = processes.get_mut(&instance) {
                    // -1 is the server's unknown, never a count.
                    if value >= 0.0 {
                        point.open_fds = Some(value as u64);
                    }
                }
            }
        }
        Err(error) => unknown_everywhere.push(format!("open_fds: {error:#}")),
    }
    // The acknowledged-share counter, raw: its resets are the processes'
    // restarts, which also number their lifetimes (in `pid`, as the PID
    // itself is not exported).
    let mut acked_delta: Option<u64> = Some(0);
    match query(
        http,
        base,
        &format!(
            "qbit_prism_accepted_shares_total{}",
            matchers(&args.selector, "")
        ),
    )
    .await
    {
        Ok(rows) => {
            for (labels, value) in rows {
                let instance = labels.get(label).cloned().unwrap_or_default();
                let point = processes
                    .entry(instance.clone())
                    .or_insert_with(|| note(&instance));
                let now = value.max(0.0) as u64;
                point.accepted_total = Some(now);
                let before = previous.get(&instance);
                let lifetime = before.and_then(|p| p.pid).unwrap_or(1);
                match before.and_then(|p| p.accepted_total) {
                    Some(then) if now >= then => {
                        point.pid = Some(lifetime);
                        acked_delta = acked_delta.map(|sum| sum + (now - then));
                    }
                    Some(_) => {
                        point.pid = Some(lifetime + 1);
                        acked_delta = acked_delta.map(|sum| sum + now);
                    }
                    // First seen: its count so far predates the soak.
                    None => point.pid = Some(lifetime),
                }
            }
        }
        Err(error) => {
            unknown_everywhere.push(format!("accepted_shares_total: {error:#}"));
            acked_delta = None;
        }
    }
    for point in processes.values_mut() {
        if point.open_fds.is_none() {
            point
                .unknown
                .push(format!("no known {fd_query} sample for this instance"));
        }
        point.unknown.extend(unknown_everywhere.iter().cloned());
    }
    let interval = earlier
        .last()
        .map(|s| (at - s.at).num_seconds().max(60))
        .unwrap_or(args.interval_seconds as i64);
    let accepted = matchers(&args.selector, "result=\"accepted\"");
    let quantile = |q: f64| {
        format!(
            "histogram_quantile({q}, sum by (le) (rate(qbit_prism_share_ack_seconds_bucket{accepted}[{interval}s]))) * 1000"
        )
    };
    let scalar = |rows: Result<Vec<(BTreeMap<String, String>, f64)>>| {
        rows.ok()
            .and_then(|rows| rows.first().map(|row| row.1))
            .filter(|value| value.is_finite())
    };
    let latency = LatencyPoint {
        source: "servers' qbit_prism_share_ack_seconds histogram".into(),
        interval_seconds: interval as f64,
        acknowledged: scalar(
            query(
                http,
                base,
                &format!(
                    "sum(increase(qbit_prism_share_ack_seconds_count{accepted}[{interval}s]))"
                ),
            )
            .await,
        )
        .map(|value| value.round() as u64),
        p50_ms: scalar(query(http, base, &quantile(0.5)).await),
        p99_ms: scalar(query(http, base, &quantile(0.99)).await),
    };
    let database = soak::database_point(pool, None).await;
    let from =
        started - chrono::Duration::milliseconds((args.scrape_slack_seconds * 1000.0) as i64);
    let committed = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM qbit_share_ledger \
         WHERE accepted AND accepted_at > $1 AND accepted_at <= $2",
    )
    .bind(from)
    .bind(at)
    .fetch_one(pool)
    .await
    .ok()
    .map(|n| n as u64);
    let slack_rows = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM qbit_share_ledger \
         WHERE accepted AND accepted_at > $1 AND accepted_at <= $2",
    )
    .bind(from)
    .bind(started)
    .fetch_one(pool)
    .await
    .ok()
    .map(|n| n as u64);
    let prior = earlier.last().and_then(|s| s.ledger.as_ref());
    let prior_total = prior.and_then(|l| l.acknowledged_since_start).unwrap_or(0);
    let prior_gaps = prior.map_or(0, |l| l.acknowledged_gaps);
    let (acknowledged, gaps) = match acked_delta {
        Some(delta) => (prior_total + delta, prior_gaps),
        None => (prior_total, prior_gaps + 1),
    };
    Sample {
        schema: soak::SAMPLE_SCHEMA.into(),
        at,
        elapsed_seconds: elapsed,
        phase: None,
        steady: true,
        processes: processes.into_values().collect(),
        database,
        latency: Some(latency),
        ledger: Some(LedgerPoint {
            acknowledged_since_start: Some(acknowledged),
            acknowledged_gaps: gaps,
            committed_since_start: committed,
            tolerance_rows: slack_rows,
            tolerance_seconds: args.scrape_slack_seconds,
        }),
    }
}
