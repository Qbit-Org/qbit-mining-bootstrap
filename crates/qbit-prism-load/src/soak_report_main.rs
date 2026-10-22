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
    // Per process, the lifetime last seen and the last counter reading with
    // the lifetime it belongs to, from whichever earlier sample had them: a
    // sample that could not read the counter neither ends a lifetime nor
    // starts one.
    let mut last_lifetime: BTreeMap<String, u32> = BTreeMap::new();
    let mut last_counter: BTreeMap<String, (u32, u64)> = BTreeMap::new();
    for sample in earlier {
        for process in &sample.processes {
            if let Some(pid) = process.pid {
                last_lifetime.insert(process.instance.clone(), pid);
                if let Some(total) = process.accepted_total {
                    last_counter.insert(process.instance.clone(), (pid, total));
                }
            }
        }
    }
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
    // The acknowledged-share counter, raw, with the time Prometheus scraped
    // it and whether it reset since the last reading: a reset is a restart,
    // which also numbers the process's lifetimes (in `pid`, as the PID itself
    // is not exported).
    let since = earlier
        .iter()
        .rev()
        .find_map(|s| s.ledger.as_ref()?.committed_through)
        .map_or(args.interval_seconds as i64, |through| {
            (at - through).num_seconds().max(60) + 60
        });
    let counter = format!(
        "qbit_prism_accepted_shares_total{}",
        matchers(&args.selector, "")
    );
    let readings = async {
        let values = query(http, base, &counter).await?;
        let stamps = query(http, base, &format!("timestamp({counter})")).await?;
        let resets = query(http, base, &format!("resets({counter}[{since}s])")).await?;
        let by = |rows: Vec<(BTreeMap<String, String>, f64)>| -> BTreeMap<String, f64> {
            rows.into_iter()
                .map(|(labels, value)| (labels.get(label).cloned().unwrap_or_default(), value))
                .collect()
        };
        anyhow::Ok((by(values), by(stamps), by(resets)))
    }
    .await;
    let mut acked_delta: Option<u64> = None;
    let mut restarts_uncounted = 0u64;
    let mut read_at: Option<(f64, f64)> = None;
    match readings {
        Ok((values, stamps, resets)) => {
            acked_delta = Some(0);
            for (instance, value) in values {
                let point = processes
                    .entry(instance.clone())
                    .or_insert_with(|| note(&instance));
                let now = value.max(0.0) as u64;
                point.accepted_total = Some(now);
                if let Some(stamp) = stamps.get(&instance) {
                    read_at = Some(read_at.map_or((*stamp, *stamp), |(low, high)| {
                        (low.min(*stamp), high.max(*stamp))
                    }));
                }
                let reset = resets.get(&instance).is_some_and(|n| *n > 0.0);
                match last_counter.get(&instance) {
                    // The counter is continuous across samples that missed
                    // it, so the whole stretch is counted.
                    Some(&(lifetime, then)) if now >= then && !reset => {
                        point.pid = Some(lifetime);
                        acked_delta = acked_delta.map(|sum| sum + (now - then));
                    }
                    // Restarted: the new process's count is all new, and
                    // what the old one acknowledged after its last reading
                    // is not knowable.
                    Some(&(lifetime, _)) => {
                        point.pid = Some(lifetime + 1);
                        acked_delta = acked_delta.map(|sum| sum + now);
                        restarts_uncounted += 1;
                    }
                    // First seen: its count so far predates the soak.
                    None => {
                        point.pid = Some(*last_lifetime.get(&instance).unwrap_or(&1));
                    }
                }
            }
        }
        Err(error) => unknown_everywhere.push(format!("accepted_shares_total: {error:#}")),
    }
    // A process seen before and missing now is a process gone dark, not one
    // that left the soak: it stays, every figure unknown.
    for (instance, lifetime) in &last_lifetime {
        processes
            .entry(instance.clone())
            .or_insert_with(|| ProcessPoint {
                instance: instance.clone(),
                pid: Some(*lifetime),
                unknown: vec!["no series for this process in this sample".into()],
                ..ProcessPoint::default()
            });
    }
    for point in processes.values_mut() {
        // A process whose counter this sample could not read stays in the
        // lifetime it was last seen in.
        if point.pid.is_none() {
            point.pid = last_lifetime.get(&point.instance).copied();
        }
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
    // Committed rows are counted per interval, over the span the counter
    // readings themselves cover: from the previous reading's scrape time to
    // this one's (the earliest scrape among the processes). Counted while the
    // interval's rows are still in the live ledger, and added up, so a
    // partition the operator's retention later removes takes old rows out of
    // the ledger, not out of the count. The first reading is the baseline.
    let prior = earlier.iter().rev().find_map(|s| {
        let ledger = s.ledger.as_ref()?;
        Some((
            ledger.acknowledged_since_start?,
            ledger.committed_since_start?,
            ledger.committed_through?,
            ledger.acknowledged_gaps,
        ))
    });
    let count = |from: f64, to: f64| {
        let stamp = |seconds: f64| {
            chrono::DateTime::from_timestamp_millis((seconds * 1000.0) as i64).unwrap_or(at)
        };
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)::bigint FROM qbit_share_ledger \
             WHERE accepted AND accepted_at > $1 AND accepted_at <= $2",
        )
        .bind(stamp(from))
        .bind(stamp(to))
        .fetch_one(pool)
    };
    let (mut acknowledged, mut committed, mut through, mut gaps) = match prior {
        Some((acked, committed, through, gaps)) => {
            (Some(acked), Some(committed), Some(through), gaps)
        }
        None => (Some(0), Some(0), None, 0),
    };
    let mut tolerance = (Some(0u64), 0.0f64);
    let mut advanced = false;
    if let (Some((low, high)), Some(delta)) = (read_at, acked_delta) {
        let interval = match through {
            Some(from) => count(from.timestamp_millis() as f64 / 1000.0, low)
                .await
                .ok()
                .map(|n| n as u64),
            None => Some(0),
        };
        let spread = count(low, high).await.ok().map(|n| n as u64);
        if let Some(n) = interval {
            committed = committed.map(|c| c + n);
            acknowledged = acknowledged.map(|a| a + delta);
            through = chrono::DateTime::from_timestamp_millis((low * 1000.0) as i64);
            gaps += restarts_uncounted;
            tolerance = (spread, high - low);
            advanced = true;
        } else {
            // The ledger could not be read: the verdict reads unknown until
            // a sample can count the interval.
            committed = None;
        }
    }
    if !advanced {
        // Neither side advanced, so this sample's readings are not the
        // baseline the next one counts from: both sides then cover the
        // stretch together.
        for point in processes.values_mut() {
            point.accepted_total = None;
        }
    }
    let committed_through = through;
    let (tolerance_rows, tolerance_seconds) = tolerance;
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
            acknowledged_since_start: acknowledged,
            acknowledged_gaps: gaps,
            committed_since_start: committed,
            committed_through,
            tolerance_rows,
            tolerance_seconds,
        }),
    }
}
