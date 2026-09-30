//! The read tier under faults (#554; #189's class, #469): a separate
//! `public-api` process on its own read pool, scraped at a realistic rate,
//! and every frontend's `/metrics`, scraped as Prometheus would, for the
//! whole `faults` phase.
//!
//! The scraper records every request, answered or not; the verdict is
//! computed after the run from those samples, per fault window
//! (EP-OBSERVABILITY: a request that timed out is a failure, never a missing
//! sample).

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::{
    fs::File,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinHandle};

/// The public paths scraped in turn, the ones a pool page polls.
pub const PUBLIC_PATHS: [&str; 4] = [
    "/public/v1/pool-summary",
    "/public/v1/blocks",
    "/public/v1/leaderboard",
    "/public/v1/hashrate-series",
];
/// One public request every this often: 5 requests a second.
pub const PUBLIC_INTERVAL: Duration = Duration::from_millis(200);
/// Every `/metrics` endpoint every this often, as a 5 s Prometheus job.
pub const METRICS_INTERVAL: Duration = Duration::from_secs(5);
/// A request not answered within this is a failed request.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// The public tier's pass line: this share of requests answered 2xx...
pub const PUBLIC_SUCCESS_FLOOR: f64 = 0.99;
/// ...with the p99 of their latency within this.
pub const PUBLIC_P99_LIMIT_MS: f64 = 1_000.0;
/// A frontend's `/metrics` must answer within this.
pub const METRICS_LIMIT_MS: f64 = 1_000.0;

/// One scrape.
#[derive(Clone, Debug)]
pub struct ReadSample {
    /// `public-api`, or the frontend's instance id.
    pub target: String,
    pub path: String,
    pub at: Instant,
    pub millis: f64,
    pub status: Option<u16>,
    pub error: Option<String>,
}

impl ReadSample {
    pub fn ok(&self) -> bool {
        self.status
            .is_some_and(|status| (200..300).contains(&status))
    }
}

pub struct ReadTier {
    child: Option<Child>,
    pub url: String,
    pub log: PathBuf,
    /// Which database the public process reads, for the report.
    pub reads: &'static str,
    samples: Arc<Mutex<Vec<ReadSample>>>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl Drop for ReadTier {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        if let Some(task) = &self.task {
            task.abort();
        }
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl ReadTier {
    /// Start `public-api` on `database_url` (a replica when `replica` is
    /// set, read with `PRISM_PUBLIC_REPLICA_MODE=require`) and the node
    /// settings in `node_env`, as Compose gives it, wait for its `/healthz`,
    /// and begin scraping it and each `(instance, metrics URL)`.
    pub async fn start(
        server_bin: &Path,
        database_url: &str,
        replica: bool,
        node_env: Vec<(String, String)>,
        log_dir: &Path,
        frontends: Vec<(String, String)>,
    ) -> Result<Self> {
        let port = free_port()?;
        let log = log_dir.join("load-public-api.log");
        let output = File::create(&log).context("creating the public API log")?;
        let child = Command::new(server_bin)
            .arg("public-api")
            // As the public role runs in production: no signing keys, no
            // mining configuration, no writer DSN; the node's settings, for
            // the chain facts `pool-summary` reads.
            .env_clear()
            .env("PRISM_DATABASE_URL", database_url)
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("PRISM_PUBLIC_API_BIND", "127.0.0.1")
            .env("PRISM_PUBLIC_API_PORT", port.to_string())
            .env("PRISM_PUBLIC_STRATUM_URL", "stratum+tcp://127.0.0.1:3340")
            .env(
                "PRISM_PUBLIC_REPLICA_MODE",
                if replica { "require" } else { "off" },
            )
            .env("PRISM_PUBLIC_CACHE_ENABLED", "0")
            .env("RUST_LOG", "info")
            .envs(node_env)
            .stdout(Stdio::from(output.try_clone()?))
            .stderr(Stdio::from(output))
            .spawn()
            .context("starting qbit-prism-server public-api")?;
        let url = format!("http://127.0.0.1:{port}");
        let client = client()?;
        let (stop, stop_rx) = watch::channel(false);
        let mut tier = Self {
            child: Some(child),
            url: url.clone(),
            log,
            reads: if replica { "replica" } else { "primary" },
            samples: Arc::new(Mutex::new(Vec::new())),
            stop,
            task: None,
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = tier
                .child
                .as_mut()
                .and_then(|c| c.try_wait().ok().flatten())
            {
                bail!(
                    "public-api exited with {status} before it was ready; see {}",
                    tier.log.display()
                );
            }
            let healthy = client
                .get(format!("{url}/healthz"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            if healthy {
                break;
            }
            if Instant::now() >= deadline {
                bail!(
                    "public-api was not healthy within 60 s; see {}",
                    tier.log.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        tier.task = Some(tokio::spawn(scrape(
            client,
            url,
            frontends,
            tier.samples.clone(),
            stop_rx,
        )));
        Ok(tier)
    }

    /// Stop scraping and the public process; the samples stay.
    pub fn stop(&mut self) {
        self.stop.send_replace(true);
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }

    pub fn samples(&self) -> Vec<ReadSample> {
        self.samples.lock().expect("read tier samples lock").clone()
    }
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("building the read tier's HTTP client")
}

fn free_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

async fn scrape(
    client: reqwest::Client,
    public: String,
    frontends: Vec<(String, String)>,
    samples: Arc<Mutex<Vec<ReadSample>>>,
    mut stop: watch::Receiver<bool>,
) {
    let mut public_tick = tokio::time::interval(PUBLIC_INTERVAL);
    public_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut metrics_tick = tokio::time::interval(METRICS_INTERVAL);
    metrics_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut cursor = 0usize;
    loop {
        tokio::select! {
            _ = stop.changed() => return,
            _ = public_tick.tick() => {
                let path = PUBLIC_PATHS[cursor % PUBLIC_PATHS.len()];
                cursor += 1;
                // Each request on its own task, so one slow answer does not
                // lower the offered rate: the tier is offered 5/s whatever
                // it does with them.
                tokio::spawn(one(client.clone(), "public-api".into(), format!("{public}{path}"),
                    path.into(), samples.clone()));
            }
            _ = metrics_tick.tick() => {
                tokio::spawn(one(client.clone(), "public-api".into(),
                    format!("{public}/metrics"), "/metrics".into(), samples.clone()));
                for (instance, url) in &frontends {
                    tokio::spawn(one(client.clone(), instance.clone(), url.clone(),
                        "/metrics".into(), samples.clone()));
                }
            }
        }
    }
}

async fn one(
    client: reqwest::Client,
    target: String,
    url: String,
    path: String,
    samples: Arc<Mutex<Vec<ReadSample>>>,
) {
    let at = Instant::now();
    let result = client.get(&url).send().await;
    let (status, error) = match result {
        Ok(response) => {
            let status = response.status().as_u16();
            // The body is read, as a real client would: a status line
            // followed by a stalled body is not an answer. A refusal keeps
            // the start of its body, which says why.
            match response.bytes().await {
                Ok(_) if (200..300).contains(&status) => (Some(status), None),
                Ok(body) => (
                    Some(status),
                    Some(String::from_utf8_lossy(&body[..body.len().min(200)]).into_owned()),
                ),
                Err(error) => (None, Some(format!("reading the body: {error}"))),
            }
        }
        Err(error) => (None, Some(error.to_string())),
    };
    samples
        .lock()
        .expect("read tier samples lock")
        .push(ReadSample {
            target,
            path,
            at,
            millis: at.elapsed().as_secs_f64() * 1000.0,
            status,
            error,
        });
}

/// The read tier's figures over `[from, to)`, excluding frontends that were
/// down on purpose (`outages`: instance, start, end) and, for the public
/// process, the windows in which its database was denied to every client
/// (`public_exclusions`).
pub fn summarize(
    samples: &[ReadSample],
    from: Instant,
    to: Instant,
    outages: &[(String, Instant, Instant)],
    public_exclusions: &[(Instant, Instant)],
) -> Value {
    let inside = |sample: &&ReadSample| sample.at >= from && sample.at < to;
    let public: Vec<&ReadSample> = samples
        .iter()
        .filter(inside)
        .filter(|sample| sample.target == "public-api" && sample.path != "/metrics")
        .filter(|sample| {
            !public_exclusions
                .iter()
                .any(|(start, end)| sample.at >= *start && sample.at < *end)
        })
        .collect();
    let ok = public.iter().filter(|sample| sample.ok()).count();
    let latency = crate::measure::summarize(
        public.iter().map(|sample| sample.millis).collect(),
        crate::measure::MILLISECONDS,
        "harness monotonic, request sent to body read",
    );
    let success = (!public.is_empty()).then(|| ok as f64 / public.len() as f64);
    let public_pass = match (success, latency.p99) {
        (Some(success), Some(p99)) => success >= PUBLIC_SUCCESS_FLOOR && p99 <= PUBLIC_P99_LIMIT_MS,
        _ => false,
    };
    let metrics: Vec<&ReadSample> = samples
        .iter()
        .filter(inside)
        .filter(|sample| sample.target != "public-api")
        .filter(|sample| {
            !outages.iter().any(|(instance, start, end)| {
                *instance == sample.target && sample.at >= *start && sample.at < *end
            })
        })
        .collect();
    let slow_or_failed: Vec<Value> = metrics
        .iter()
        .filter(|sample| !sample.ok() || sample.millis > METRICS_LIMIT_MS)
        .map(|sample| {
            json!({
                "target": sample.target,
                "after_seconds": sample.at.saturating_duration_since(from).as_secs_f64(),
                "milliseconds": sample.millis,
                "status": sample.status,
                "error": sample.error,
            })
        })
        .collect();
    let metrics_pass = !metrics.is_empty() && slow_or_failed.is_empty();
    json!({
        "public_api": {
            "requests": public.len(),
            "answered_2xx": ok,
            "success_ratio": success,
            "success_floor": PUBLIC_SUCCESS_FLOOR,
            "latency_milliseconds": latency,
            "p99_limit_milliseconds": PUBLIC_P99_LIMIT_MS,
            "status_counts": status_counts(&public),
            "by_path": PUBLIC_PATHS.iter().map(|path| {
                let on_path: Vec<&ReadSample> =
                    public.iter().copied().filter(|sample| sample.path == *path).collect();
                json!({
                    "path": path,
                    "status_counts": status_counts(&on_path),
                    "first_refusal": on_path.iter().find(|sample| !sample.ok())
                        .and_then(|sample| sample.error.clone()),
                })
            }).collect::<Vec<_>>(),
            "pass": public_pass,
            "unavailable_reason": public.is_empty().then_some("no public request fell in the window"),
        },
        "frontend_metrics": {
            "scrapes": metrics.len(),
            "limit_milliseconds": METRICS_LIMIT_MS,
            "slow_or_failed": slow_or_failed,
            "pass": metrics_pass,
            "unavailable_reason": metrics.is_empty().then_some("no /metrics scrape fell in the window"),
        },
        "pass": public_pass && metrics_pass,
    })
}

fn status_counts(samples: &[&ReadSample]) -> Value {
    let mut counts = std::collections::BTreeMap::<String, u64>::new();
    for sample in samples {
        let key = match (sample.status, &sample.error) {
            (Some(status), _) => status.to_string(),
            (None, Some(_)) => "error".into(),
            (None, None) => "unknown".into(),
        };
        *counts.entry(key).or_default() += 1;
    }
    json!(counts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(
        target: &str,
        path: &str,
        at: Instant,
        millis: f64,
        status: Option<u16>,
    ) -> ReadSample {
        ReadSample {
            target: target.into(),
            path: path.into(),
            at,
            millis,
            status,
            error: status.is_none().then(|| "timed out".into()),
        }
    }

    #[test]
    fn a_timed_out_request_fails_the_tier_and_a_planned_outage_does_not() {
        let origin = Instant::now();
        let at = |seconds: u64| origin + Duration::from_secs(seconds);
        let mut samples: Vec<ReadSample> = (0..200)
            .map(|index| {
                sample(
                    "public-api",
                    PUBLIC_PATHS[0],
                    at(1),
                    10.0 + index as f64,
                    Some(200),
                )
            })
            .collect();
        samples.push(sample("load-fe-0", "/metrics", at(2), 5.0, Some(200)));
        // load-fe-1 was down on purpose at 3 s: its failed scrape is excused.
        samples.push(sample("load-fe-1", "/metrics", at(3), 5_000.0, None));
        let outages = vec![("load-fe-1".to_owned(), at(2), at(4))];
        let verdict = summarize(&samples, origin, at(10), &outages, &[]);
        assert_eq!(verdict["pass"], true, "{verdict}");

        // Three public timeouts out of 203 put the tier under 99 %.
        for _ in 0..3 {
            samples.push(sample("public-api", PUBLIC_PATHS[1], at(5), 5_000.0, None));
        }
        let verdict = summarize(&samples, origin, at(10), &outages, &[]);
        assert_eq!(verdict["public_api"]["pass"], false, "{verdict}");
        // Unless they fell while its database was denied to everyone.
        let verdict = summarize(&samples, origin, at(10), &outages, &[(at(5), at(6))]);
        assert_eq!(verdict["public_api"]["pass"], true, "{verdict}");

        // A frontend's slow /metrics outside any outage fails the tier.
        samples.push(sample("load-fe-0", "/metrics", at(7), 1_500.0, Some(200)));
        let verdict = summarize(&samples, origin, at(10), &outages, &[(at(5), at(6))]);
        assert_eq!(verdict["frontend_metrics"]["pass"], false, "{verdict}");
    }

    #[test]
    fn a_window_with_no_scrape_is_not_a_pass() {
        let origin = Instant::now();
        let verdict = summarize(&[], origin, origin + Duration::from_secs(5), &[], &[]);
        assert_eq!(verdict["pass"], false);
        assert!(verdict["public_api"]["unavailable_reason"].is_string());
    }
}
