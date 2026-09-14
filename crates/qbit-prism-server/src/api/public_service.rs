//! Independent, read-only public dashboard process. No writer lease or schema
//! mutation is available through this role.
#[cfg(test)]
mod metrics_tests;

use super::*;
use anyhow::{ensure, Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::sync::watch;

tokio::task_local! { pub(super) static READ_DEADLINE: Option<tokio::time::Instant>; }

fn statement_millis() -> u64 {
    READ_DEADLINE
        .try_with(|deadline| {
            deadline.map_or(0, |deadline| {
                deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .as_millis()
                    .clamp(1, i32::MAX as u128) as u64
            })
        })
        .unwrap_or(5000)
}
pub(super) fn read_pool(options: PgConnectOptions, concurrency: u32) -> PgPool {
    PgPoolOptions::new().max_connections(concurrency).acquire_timeout(Duration::from_secs(20))
        .after_connect(|connection, _| Box::pin(async move {
            sqlx::query("SELECT set_config('default_transaction_read_only','on',false),set_config('statement_timeout',$1,false)")
                .bind(format!("{}ms", statement_millis())).execute(connection).await?;
            Ok(())
        }))
        .before_acquire(|connection, _| Box::pin(async move {
            // A reused connection inherits the remaining whole-request
            // deadline, including time already spent waiting for admission.
            sqlx::query("SELECT set_config('statement_timeout',$1,false)")
                .bind(format!("{}ms", statement_millis())).execute(connection).await?;
            Ok(true)
        }))
        .connect_lazy_with(options.application_name("prism-public-read"))
}

#[derive(Clone)]
pub struct ServiceConfig {
    pub bind: String,
    pub port: u16,
    pub replica_required: bool,
    pub replica_max_lag: Duration,
    pub probe_interval: Duration,
    pub read_concurrency: u32,
}
impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0".into(),
            port: 3342,
            replica_required: false,
            replica_max_lag: Duration::from_secs(60),
            probe_interval: Duration::from_secs(5),
            read_concurrency: 4,
        }
    }
}
impl ServiceConfig {
    pub fn from_env() -> Result<Self> {
        let positive = |name: &str, default: f64| -> Result<Duration> {
            let value = std::env::var(name)
                .ok()
                .map(|v| v.parse::<f64>())
                .transpose()
                .with_context(|| format!("invalid {name}"))?
                .unwrap_or(default);
            ensure!(
                value.is_finite() && value > 0.,
                "{name} must be positive and finite"
            );
            let duration =
                Duration::try_from_secs_f64(value).with_context(|| format!("invalid {name}"))?;
            ensure!(!duration.is_zero(), "{name} is below the timer resolution");
            Ok(duration)
        };
        let mode = env("PRISM_PUBLIC_REPLICA_MODE", "off")
            .trim()
            .to_ascii_lowercase();
        ensure!(
            matches!(mode.as_str(), "off" | "require"),
            "PRISM_PUBLIC_REPLICA_MODE must be one of off, require"
        );
        let concurrency = std::env::var("PRISM_POSTGRES_READ_CONCURRENCY")
            .ok()
            .map(|v| v.parse::<u32>())
            .transpose()?
            .unwrap_or(4);
        ensure!(
            concurrency > 0 && concurrency <= 1024,
            "PRISM_POSTGRES_READ_CONCURRENCY must be between 1 and 1024"
        );
        Ok(Self {
            bind: env("PRISM_PUBLIC_API_BIND", "0.0.0.0"),
            port: std::env::var("PRISM_PUBLIC_API_PORT")
                .ok()
                .map(|v| v.parse())
                .transpose()?
                .unwrap_or(3342),
            replica_required: mode == "require",
            replica_max_lag: positive("PRISM_PUBLIC_REPLICA_MAX_LAG_SECONDS", 60.)?,
            probe_interval: positive("PRISM_PUBLIC_READINESS_PROBE_INTERVAL_SECONDS", 5.)?,
            read_concurrency: concurrency,
        })
    }
}

#[derive(Default)]
struct ProbeSnapshot {
    ready: bool,
    checked: Option<Instant>,
    checked_at: Option<String>,
    last_error: Option<String>,
    replica: Option<Value>,
    replica_at: Option<Instant>,
}
#[derive(Default)]
struct ServiceMetrics {
    responses: BTreeMap<u16, u64>,
    cache: BTreeMap<String, u64>,
    staleness_refusals: u64,
    replica_refusals: u64,
    degraded_responses: u64,
    outage_refusals: u64,
}
pub struct ServiceState {
    config: ServiceConfig,
    pool: PgPool,
    snapshot: RwLock<ProbeSnapshot>,
    pub(super) requests: AtomicU64,
    metrics: std::sync::Mutex<ServiceMetrics>,
}
pub(super) struct ServiceView {
    pub database_ready: bool,
    pub replica_error: Option<String>,
    replay_lag: Option<f64>,
    payload: Value,
    metrics_freshness: metrics_snapshot::Freshness,
}
impl ServiceState {
    pub async fn probe_once(&self) {
        let probe = async {
            let schema_ready =
                sqlx::query_scalar::<_, bool>(include_str!("queries/read_schema_ready.sql"))
                    .fetch_one(&self.pool)
                    .await?;
            let mut value = if self.config.replica_required {
                sqlx::query_scalar::<_, Value>(include_str!("queries/read_replica_status.sql"))
                    .fetch_one(&self.pool)
                    .await?
            } else {
                json!({})
            };
            value["schema_ready"] = json!(schema_ready);
            Ok::<_, sqlx::Error>(value)
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let result = READ_DEADLINE
            .scope(Some(deadline), tokio::time::timeout_at(deadline, probe))
            .await;
        let mut snapshot = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        snapshot.checked = Some(Instant::now());
        snapshot.checked_at = Some(now());
        match result {
            Ok(Ok(value)) => {
                snapshot.ready = value["schema_ready"] == true
                    && (!self.config.replica_required || value["in_recovery"] == true);
                snapshot.last_error = (value["schema_ready"] != true)
                    .then(|| "native public read schema is incomplete".into());
                if self.config.replica_required {
                    snapshot.replica = Some(value);
                    snapshot.replica_at = Some(Instant::now());
                }
            }
            Ok(Err(error)) => {
                snapshot.ready = false;
                snapshot.last_error = Some(error.to_string());
            }
            Err(_) => {
                snapshot.ready = false;
                snapshot.last_error = Some("database probe timed out".into());
            }
        }
    }
    pub(super) fn view(&self) -> ServiceView {
        let snapshot = self.snapshot.read().unwrap_or_else(|e| e.into_inner());
        let age = snapshot.checked.map(|v| v.elapsed().as_secs_f64());
        let database_ready = snapshot.ready
            && age.is_some_and(|age| {
                age < self
                    .config
                    .probe_interval
                    .as_secs_f64()
                    .mul_add(3., 0.)
                    .max(15.)
            });
        let stale_after = self
            .config
            .probe_interval
            .as_secs_f64()
            .mul_add(3., 0.)
            .max(15.);
        let mut payload = json!({"schema":"qbit.prism.public-read-health.v1","ok":database_ready,"state":if snapshot.checked.is_none(){"starting"}else if database_ready{"ready"}else{"unready"},"database_ready":database_ready,"checked_at":snapshot.checked_at,"probe_age_seconds":age.unwrap_or(-1.),"probe_stale_after_seconds":stale_after});
        if snapshot.checked.is_none() {
            payload["error"] = json!("readiness probe has not completed yet");
        } else if age.is_some_and(|age| age > stale_after) {
            payload["error"] = json!("readiness probe is stale");
        } else if let Some(error) = &snapshot.last_error {
            payload["error"] = json!(error);
        } else if !database_ready {
            payload["error"] = json!("readiness probe returned false");
        }
        let mut replica_error = None;
        let mut replay_lag = None;
        if self.config.replica_required {
            let replica_age = snapshot
                .replica_at
                .map_or(0., |at| at.elapsed().as_secs_f64());
            let mut replica = snapshot.replica.clone().unwrap_or_else(|| json!({}));
            let heartbeat = numeric(&replica["receiver_heartbeat_age_seconds"])
                .map(|v| v.max(0.) + replica_age);
            replay_lag = numeric(&replica["replay_lag_seconds"]).map(|v| v.max(0.) + replica_age);
            replica["probed"] = json!(snapshot.replica.is_some());
            replica["probe_age_seconds"] = json!(snapshot.replica_at.map(|_| replica_age));
            replica["receiver_heartbeat_age_seconds"] = json!(heartbeat);
            replica["replay_lag_seconds"] = json!(replay_lag);
            replica["max_lag_seconds"] = json!(self.config.replica_max_lag.as_secs_f64());
            if snapshot.replica.is_none() {
                replica_error = Some("public read service is warming up".into());
            } else if replica["in_recovery"] != true {
                replica_error =
                    Some("public read service refuses a database that is not in recovery".into());
            } else if heartbeat.is_none() {
                replica_error = Some("read replica replication stream is not connected".into());
            } else if heartbeat.is_some_and(|age| age > self.config.replica_max_lag.as_secs_f64()) {
                replica_error =
                    Some("read replica replication stream exceeded its heartbeat age bound".into());
            }
            payload["replica"] = replica;
            if replica_error.is_some() {
                payload["ok"] = json!(false);
                payload["state"] = json!("unready");
                payload["error"] = json!(replica_error);
            }
        }
        ServiceView {
            database_ready,
            replica_error,
            replay_lag,
            payload,
            metrics_freshness: metrics_snapshot::Freshness::new(age, stale_after),
        }
    }
    pub(super) fn health_response(&self) -> Response {
        let view = self.view();
        json_response(
            if view.payload["ok"] == true {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            view.payload,
        )
    }
    pub(super) fn record_response(&self, status: StatusCode) {
        *self
            .metrics
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .responses
            .entry(status.as_u16())
            .or_default() += 1;
    }
    pub(super) fn record_cache(
        &self,
        path: &str,
        state: &str,
        age: u64,
        result: &ApiResult<Payload>,
        view: &ServiceView,
    ) {
        let mut metrics = self.metrics.lock().unwrap_or_else(|e| e.into_inner());
        *metrics.cache.entry(state.into()).or_default() += 1;
        if reads_database(path) && view.replica_error.is_some() {
            metrics.replica_refusals += 1;
        } else if staleness_budget(path).is_some_and(|budget| age > budget) {
            metrics.staleness_refusals += 1;
        } else if reads_database(path) && !view.database_ready {
            if result.is_ok() && state == "HIT" {
                metrics.degraded_responses += 1;
            } else if result.is_err() {
                metrics.outage_refusals += 1;
            }
        }
    }
    pub(super) fn metrics_response(&self) -> Response {
        let view = self.view();
        let metrics = self.metrics.lock().unwrap_or_else(|e| e.into_inner());
        let mut body=format!("qbit_prism_public_requests_total {}\nqbit_prism_public_ledger_ready {}\nqbit_prism_public_ledger_probe_age_seconds {}\nqbit_prism_public_staleness_refusals_total {}\nqbit_prism_public_replica_refusals_total {}\nqbit_prism_public_degraded_responses_total {}\nqbit_prism_public_database_outage_refusals_total {}\n",self.requests.load(Ordering::Relaxed),u8::from(view.database_ready),view.payload["probe_age_seconds"],metrics.staleness_refusals,metrics.replica_refusals,metrics.degraded_responses,metrics.outage_refusals);
        for (status, count) in &metrics.responses {
            body.push_str(&format!(
                "qbit_prism_public_responses_total{{status=\"{status}\"}} {count}\n"
            ));
        }
        for (state, count) in &metrics.cache {
            body.push_str(&format!(
                "qbit_prism_public_cache_total{{state=\"{state}\"}} {count}\n"
            ));
        }
        if self.config.replica_required {
            let replica = &view.payload["replica"];
            body.push_str(&format!(
                "qbit_prism_public_replica_in_recovery {}\n",
                u8::from(replica["in_recovery"] == true)
            ));
            for (metric, field) in [
                ("heartbeat_age_seconds", "receiver_heartbeat_age_seconds"),
                ("replay_lag_seconds", "replay_lag_seconds"),
                ("apply_backlog_bytes", "apply_backlog_bytes"),
                ("max_lag_seconds", "max_lag_seconds"),
            ] {
                body.push_str(&format!(
                    "qbit_prism_public_replica_{metric} {}\n",
                    numeric(&replica[field]).unwrap_or(-1.)
                ));
            }
        }
        view.metrics_freshness.response(body)
    }
}
fn numeric(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|n| n.is_finite())
}

pub fn router(state: ApiState, config: ServiceConfig) -> (Router, Arc<ServiceState>) {
    let mut state = state.with_read_concurrency(config.read_concurrency);
    let service = Arc::new(ServiceState {
        config,
        pool: state.public_pool.clone(),
        snapshot: RwLock::new(ProbeSnapshot::default()),
        requests: AtomicU64::new(0),
        metrics: std::sync::Mutex::new(ServiceMetrics::default()),
    });
    state.public_service = Some(service.clone());
    (super::router(state), service)
}

pub async fn run_from_env(mut shutdown: watch::Receiver<bool>) -> Result<()> {
    ensure!(
        !env_bool("PRISM_ALLOW_MEMORY_LEDGER", false),
        "public API requires PostgreSQL"
    );
    let stratum = std::env::var("PRISM_PUBLIC_STRATUM_URL")
        .context("PRISM_PUBLIC_STRATUM_URL is required by the independent public service")?;
    let parsed = url::Url::parse(&stratum).context("invalid PRISM_PUBLIC_STRATUM_URL")?;
    ensure!(
        parsed.scheme() == "stratum+tcp" && parsed.host_str().is_some(),
        "PRISM_PUBLIC_STRATUM_URL must be a stratum+tcp URL"
    );
    let config = ServiceConfig::from_env()?;
    let database = std::env::var("PRISM_DATABASE_URL")
        .context("PRISM_DATABASE_URL is required by the public service")?;
    let options = PgConnectOptions::from_str(&database)?.application_name("prism-public-read");
    let pool = read_pool(options, config.read_concurrency);
    let (app, service) = router(
        ApiState::new(
            pool,
            ApiConfig::from_env(),
            std::sync::Arc::new(crate::metrics::Metrics::default()),
        ),
        config.clone(),
    );
    service.probe_once().await;
    let listener = tokio::net::TcpListener::bind((config.bind.as_str(), config.port)).await?;
    let mut probe_shutdown = shutdown.clone();
    let probe = tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.probe_interval);
        loop {
            tokio::select! {_=interval.tick()=>service.probe_once().await,changed=probe_shutdown.changed()=>{if changed.is_err()||*probe_shutdown.borrow(){break;}}}
        }
    });
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
    probe.abort();
    let _ = probe.await;
    result?;
    Ok(())
}

pub(super) fn reads_database(path: &str) -> bool {
    if matches!(
        path,
        "/public/v1/pool-summary"
            | "/public/v1/blocks"
            | "/public/v1/block-markers"
            | "/public/v1/hashrate-series"
            | "/public/v1/leaderboard"
            | "/public/v1/fanouts/pending"
    ) {
        return true;
    }
    if let Some(rest) = path.strip_prefix("/public/v1/miners/") {
        let parts = rest.split('/').collect::<Vec<_>>();
        return !parts[0].is_empty()
            && (parts.len() == 1
                || parts.len() == 2 && matches!(parts[1], "earnings" | "payouts" | "workers"));
    }
    if let Some(rest) = path.strip_prefix("/public/v1/blocks/") {
        return rest
            .strip_suffix("/settlement-artifacts")
            .is_some_and(|hash| !hash.is_empty() && !hash.contains('/'));
    }
    path.strip_prefix("/public/v1/fanouts/")
        .is_some_and(|hash| !hash.is_empty() && !hash.contains('/'))
}
pub(super) fn staleness_budget(path: &str) -> Option<u64> {
    if !reads_database(path) && path != "/public/v1/mining-configuration" {
        return None;
    }
    let ttl = if path == "/public/v1/mining-configuration" {
        300
    } else if matches!(
        path,
        "/public/v1/pool-summary" | "/public/v1/hashrate-series"
    ) || path.ends_with("/workers")
    {
        30
    } else {
        5
    };
    let underlying = if path.starts_with("/public/v1/miners/")
        && path
            .trim_start_matches("/public/v1/miners/")
            .find('/')
            .is_none()
    {
        30
    } else {
        0
    };
    Some(((ttl + underlying) * 3).max(15))
}
pub(super) fn decorate(
    response: &mut Response,
    path: &str,
    _policy: &CachePolicy,
    view: &ServiceView,
    age: u64,
    cache_state: &str,
) {
    let headers = response.headers_mut();
    if let Some(budget) = staleness_budget(path) {
        headers.insert(
            "x-prism-staleness-budget-seconds",
            HeaderValue::from_str(&budget.to_string()).unwrap(),
        );
    } else if path.starts_with("/public/v1/artifacts/") {
        headers.insert(
            "x-prism-staleness-budget-seconds",
            HeaderValue::from_static("unbounded"),
        );
    }
    if let Some(lag) = view.replay_lag {
        headers.insert(
            "x-prism-replica-lag-seconds",
            HeaderValue::from_str(&format!("{lag:.3}")).unwrap(),
        );
    }
    if reads_database(path) && !view.database_ready && view.replica_error.is_none() {
        headers.insert(
            "x-prism-database-state",
            HeaderValue::from_static("unavailable"),
        );
        if cache_state == "HIT" {
            headers.insert(
                "warning",
                HeaderValue::from_static(
                    "110 qbit-prism \"database unavailable; serving cached response\"",
                ),
            );
        }
    }
    if age > 0 {
        headers.insert("age", HeaderValue::from_str(&age.to_string()).unwrap());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn replica_liveness_uses_heartbeat_and_ages_failed_probes() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        let state = ServiceState {
            config: ServiceConfig {
                replica_required: true,
                ..Default::default()
            },
            pool,
            snapshot: RwLock::new(ProbeSnapshot {
                ready: true,
                checked: Some(Instant::now()),
                replica: Some(
                    json!({"in_recovery":true,"receiver_heartbeat_age_seconds":1,"replay_lag_seconds":86400}),
                ),
                replica_at: Some(Instant::now()),
                ..Default::default()
            }),
            requests: AtomicU64::new(0),
            metrics: std::sync::Mutex::new(ServiceMetrics::default()),
        };
        assert!(
            state.view().replica_error.is_none(),
            "idle primary replay lag is advisory"
        );
        state.snapshot.write().unwrap().replica_at = Some(Instant::now() - Duration::from_secs(61));
        assert!(
            state.view().replica_error.is_some(),
            "last good heartbeat must age out"
        );
    }
    #[tokio::test]
    async fn every_sql_statement_uses_the_remaining_request_budget() {
        let Some(url) = qbit_prism_test_gate::database_url(qbit_prism_test_gate::site!())
            .expect("integration gate")
        else {
            return;
        };
        let pool = read_pool(PgConnectOptions::from_str(&url).unwrap(), 1);
        let started = Instant::now();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(600);
        let error = READ_DEADLINE
            .scope(Some(deadline), async {
                sqlx::query("SELECT pg_sleep(0.2)")
                    .execute(&pool)
                    .await
                    .unwrap();
                sqlx::query("SELECT pg_sleep(0.5)")
                    .execute(&pool)
                    .await
                    .unwrap_err()
            })
            .await;
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("57014")
        );
        assert!(
            started.elapsed() < Duration::from_millis(850),
            "second query received a fresh budget"
        );
        let read_only: bool =
            sqlx::query_scalar("SELECT current_setting('default_transaction_read_only')::boolean")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(read_only);
        pool.close().await;
    }
}
