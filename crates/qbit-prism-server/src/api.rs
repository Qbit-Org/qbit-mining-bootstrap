//! Compatibility HTTP API. Every accounting read uses the shared PostgreSQL ledger.
mod charts;
mod metrics_snapshot;
mod operator_auth;
mod public;
pub mod public_service;
mod read_models;
mod response_cache;

use crate::config;
use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use chrono::{SecondsFormat, Utc};
pub(crate) use metrics_snapshot::health_stale_after;
use metrics_snapshot::MetricsSnapshot;
use percent_encoding::percent_decode_str;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

#[derive(Clone)]
pub struct ApiConfig {
    pub rpc_url: String,
    pub rpc_user: String,
    pub rpc_password: String,
    pub stratum_host: String,
    pub stratum_port: u16,
    pub instance_id: String,
    pub pool_name: String,
    pub pool_fee_bps: u16,
    pub minimum_payout_bits: u64,
    pub explorer_block_url: Option<String>,
    pub explorer_tx_url: Option<String>,
    pub cache_enabled: bool,
    pub cache_max_entries: usize,
    pub cache_max_bytes: usize,
    pub cache_lifetimes: CacheLifetimes,
    pub cache_debug_headers: bool,
    pub read_timeout: Duration,
    pub read_concurrency: u32,
    /// Window-sized audit artifact rebuilds and decodes admitted at once,
    /// independently of `read_concurrency`.
    pub audit_rebuild_concurrency: u32,
    /// Audit artifact requests in flight, running or waiting for a rebuild;
    /// the next distinct one is refused without waiting.
    pub audit_artifact_max_in_flight: u32,
    pub public_stratum_url: Option<String>,
    pub public_stratum_highdiff_url: Option<String>,
    pub stratum_highdiff_port: Option<u16>,
    pub configuration_label: String,
    pub configuration_description: String,
    pub block_template_policy: String,
    pub hashrate_smoothing_seconds: i64,
    /// Operator health publication cadence; freshness budgets derive from it.
    pub health_refresh_interval: Duration,
    /// Required on every operator route when set; the public service ignores it.
    pub operator_bearer_token: Option<String>,
}
/// CDN lifetimes, in seconds, for one class of public responses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheLifetime {
    pub ttl: u64,
    pub stale_while_revalidate: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheLifetimes {
    pub default: CacheLifetime,
    pub configuration: CacheLifetime,
    pub artifact: CacheLifetime,
    pub aggregate: CacheLifetime,
}
impl Default for CacheLifetimes {
    fn default() -> Self {
        let lifetime = |ttl, stale_while_revalidate| CacheLifetime {
            ttl,
            stale_while_revalidate,
        };
        Self {
            default: lifetime(5, 30),
            configuration: lifetime(300, 3600),
            artifact: lifetime(86400, 86400),
            aggregate: lifetime(30, 30),
        }
    }
}
impl CacheLifetimes {
    fn from_env() -> anyhow::Result<Self> {
        let defaults = Self::default();
        let lifetime = |kind: &str, default: CacheLifetime| -> anyhow::Result<CacheLifetime> {
            Ok(CacheLifetime {
                ttl: config::number(
                    &format!("PRISM_PUBLIC_{kind}CACHE_TTL_SECONDS"),
                    default.ttl,
                )?,
                stale_while_revalidate: config::number(
                    &format!("PRISM_PUBLIC_{kind}CACHE_STALE_WHILE_REVALIDATE_SECONDS"),
                    default.stale_while_revalidate,
                )?,
            })
        };
        Ok(Self {
            default: lifetime("", defaults.default)?,
            configuration: lifetime("CONFIG_", defaults.configuration)?,
            artifact: lifetime("ARTIFACT_", defaults.artifact)?,
            aggregate: lifetime("AGGREGATE_", defaults.aggregate)?,
        })
    }
}
/// Parse one numeric setting and reject values outside its inclusive range.
fn bounded<T>(name: &str, default: T, minimum: T, maximum: T) -> anyhow::Result<T>
where
    T: std::str::FromStr + PartialOrd + std::fmt::Display,
{
    let value = config::number(name, default)?;
    anyhow::ensure!(
        value >= minimum && value <= maximum,
        "{name} must be {minimum}..{maximum}"
    );
    Ok(value)
}
/// The operator health publisher cadence. Readers derive staleness from the
/// same value, so a slower publisher never reports its own snapshots stale.
pub fn health_refresh_interval_from_env() -> anyhow::Result<Duration> {
    Ok(Duration::from_secs(bounded(
        "PRISM_HEALTH_REFRESH_SECONDS",
        2u64,
        1,
        86400,
    )?))
}
/// The optional operator credential, read only by the operator role.
fn operator_bearer_token_from_env() -> anyhow::Result<Option<String>> {
    let token = config::secret("PRISM_OPERATOR_BEARER_TOKEN")?;
    if let Some(token) = &token {
        anyhow::ensure!(
            token.len() >= 16 && token.bytes().all(|byte| byte.is_ascii_graphic()),
            "PRISM_OPERATOR_BEARER_TOKEN must be at least 16 visible ASCII characters without whitespace"
        );
    }
    Ok(token)
}
impl ApiConfig {
    /// Operator role: the shared API settings plus the operator credential.
    pub fn from_env() -> anyhow::Result<Self> {
        let mut config = Self::from_public_env()?;
        config.operator_bearer_token = operator_bearer_token_from_env()?;
        Ok(config)
    }
    /// Independent public role: never opens or validates operator credentials.
    pub fn from_public_env() -> anyhow::Result<Self> {
        let (rpc_url, rpc_user, rpc_password) = config::rpc_connection_from_env();
        let defaults = Self::default();
        let minimum_payout_bits = match [
            "PRISM_PUBLIC_MINIMUM_PAYOUT_BITS",
            "PRISM_PAYOUT_MIN_OUTPUT_BITS",
            "PRISM_PAYOUT_MIN_OUTPUT_SATS",
        ]
        .into_iter()
        .find(|name| config::optional(name).is_some())
        {
            Some(name) => config::number(name, 0u64)?,
            None => 0,
        };
        let cache_max_entries =
            config::number("PRISM_PUBLIC_CACHE_MAX_ENTRIES", defaults.cache_max_entries)?;
        anyhow::ensure!(
            cache_max_entries > 0,
            "PRISM_PUBLIC_CACHE_MAX_ENTRIES must be positive"
        );
        Ok(Self {
            rpc_url,
            rpc_user,
            rpc_password,
            stratum_host: config::optional("PRISM_PUBLIC_STRATUM_HOST")
                .unwrap_or_else(|| config::value("PRISM_STRATUM_BIND", "127.0.0.1")),
            stratum_port: config::number("PRISM_STRATUM_PORT", defaults.stratum_port)?,
            instance_id: config::value("PRISM_INSTANCE_ID", "prism"),
            pool_name: config::value("PRISM_PUBLIC_POOL_NAME", "PRISM"),
            pool_fee_bps: bounded("PRISM_PUBLIC_POOL_FEE_BPS", 0u16, 0, 10_000)?,
            minimum_payout_bits,
            explorer_block_url: config::optional("PRISM_PUBLIC_EXPLORER_BLOCK_URL_PREFIX"),
            explorer_tx_url: config::optional("PRISM_PUBLIC_EXPLORER_TX_URL_PREFIX"),
            cache_enabled: config::flag("PRISM_PUBLIC_CACHE_ENABLED", defaults.cache_enabled)?,
            cache_max_entries,
            cache_max_bytes: if config::optional("PRISM_PUBLIC_CACHE_MAX_RESPONSE_BYTES").is_some()
            {
                config::number("PRISM_PUBLIC_CACHE_MAX_RESPONSE_BYTES", 0usize)?
            } else {
                config::number(
                    "PRISM_PUBLIC_CACHE_MAX_PAYLOAD_BYTES",
                    defaults.cache_max_bytes,
                )?
            },
            cache_lifetimes: CacheLifetimes::from_env()?,
            cache_debug_headers: config::flag("PRISM_PUBLIC_CACHE_DEBUG_HEADERS", false)?,
            // Zero keeps its established meaning: no whole-request deadline.
            read_timeout: Duration::from_secs(bounded(
                "PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS",
                defaults.read_timeout.as_secs(),
                0,
                86400,
            )?),
            read_concurrency: bounded(
                "PRISM_POSTGRES_READ_CONCURRENCY",
                defaults.read_concurrency,
                1,
                1024,
            )?,
            audit_rebuild_concurrency: bounded(
                "PRISM_PUBLIC_AUDIT_REBUILD_CONCURRENCY",
                defaults.audit_rebuild_concurrency,
                1,
                64,
            )?,
            audit_artifact_max_in_flight: bounded(
                "PRISM_PUBLIC_AUDIT_ARTIFACT_MAX_IN_FLIGHT",
                defaults.audit_artifact_max_in_flight,
                1,
                4096,
            )?,
            public_stratum_url: config::optional("PRISM_PUBLIC_STRATUM_URL"),
            public_stratum_highdiff_url: config::optional("PRISM_PUBLIC_STRATUM_HIGHDIFF_URL"),
            stratum_highdiff_port: Some(config::number("PRISM_STRATUM_HIGHDIFF_PORT", 0u16)?)
                .filter(|port| *port > 0),
            configuration_label: config::value(
                "PRISM_PUBLIC_CONFIGURATION_LABEL",
                &defaults.configuration_label,
            ),
            configuration_description: config::value(
                "PRISM_PUBLIC_CONFIGURATION_DESCRIPTION",
                &defaults.configuration_description,
            ),
            block_template_policy: config::value(
                "PRISM_PUBLIC_BLOCK_TEMPLATE_POLICY",
                &defaults.block_template_policy,
            ),
            hashrate_smoothing_seconds: bounded(
                "PRISM_PUBLIC_HASHRATE_SMOOTHING_SECONDS",
                defaults.hashrate_smoothing_seconds,
                0,
                86400,
            )?,
            health_refresh_interval: health_refresh_interval_from_env()?,
            operator_bearer_token: None,
        })
    }
    /// Snapshots older than this are reported stale by health and metrics.
    pub fn health_stale_after(&self) -> Duration {
        metrics_snapshot::health_stale_after(self.health_refresh_interval)
    }
}
/// Deterministic built-in defaults; never reads the process environment.
impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:18452/".into(),
            rpc_user: "qbit".into(),
            rpc_password: "change-this".into(),
            stratum_host: "127.0.0.1".into(),
            stratum_port: 3340,
            instance_id: "prism".into(),
            pool_name: "PRISM".into(),
            pool_fee_bps: 0,
            minimum_payout_bits: 0,
            explorer_block_url: None,
            explorer_tx_url: None,
            cache_enabled: true,
            cache_max_entries: 1024,
            cache_max_bytes: 1024 * 1024,
            cache_lifetimes: CacheLifetimes::default(),
            cache_debug_headers: false,
            read_timeout: Duration::from_secs(20),
            read_concurrency: 4,
            audit_rebuild_concurrency: 1,
            audit_artifact_max_in_flight: 32,
            public_stratum_url: None,
            public_stratum_highdiff_url: None,
            stratum_highdiff_port: None,
            configuration_label: "PRISM default".into(),
            configuration_description: "Default PRISM Stratum endpoint using the pool's current block template and payout policy.".into(),
            block_template_policy:
                "pool-selected qbit block template with PRISM payout settlement".into(),
            hashrate_smoothing_seconds: 1800,
            health_refresh_interval: Duration::from_secs(2),
            operator_bearer_token: None,
        }
    }
}

type ResponseCache = Arc<Mutex<BTreeMap<String, Arc<response_cache::CacheSlot>>>>;

#[derive(Clone)]
pub struct ApiState {
    pub pool: PgPool,
    pub config: Arc<ApiConfig>,
    /// Published by the runtime; health handlers never wait on database queries.
    pub health: Arc<RwLock<Value>>,
    metrics: Arc<RwLock<MetricsSnapshot>>,
    registry: Arc<crate::metrics::Metrics>,
    pub latest_evidence: Arc<RwLock<Option<Value>>>,
    health_published_at: Arc<RwLock<Instant>>,
    client: reqwest::Client,
    cache: ResponseCache,
    public_pool: PgPool,
    /// Imported audit decodes run after their read connection is released,
    /// so they share the read pool's concurrency through this limit instead.
    audit_decodes: Arc<Semaphore>,
    /// Content-addressed audit artifacts are rebuilt from a share window or
    /// decoded from sealed bytes, both proportional to the window; this limit
    /// is sized on its own so a larger read pool admits no more of them.
    audit_rebuilds: Arc<Semaphore>,
    /// Audit artifact requests admitted to wait for, or run, a rebuild.
    audit_artifacts: Arc<Semaphore>,
    public_service: Option<Arc<public_service::ServiceState>>,
}
#[derive(Clone, Debug)]
struct Payload {
    bytes: Bytes,
    canonical_fallback: Option<String>,
    /// The SHA-256 these bytes are already known to hash to, for a body that
    /// was served by its content address. The ETag is that hash, so carrying
    /// it keeps a second pass over a window-sized body off the runtime.
    content_address: Option<String>,
}
impl Payload {
    fn json(value: Value) -> Self {
        Self {
            bytes: Bytes::from(serde_json::to_vec(&value).expect("JSON value")),
            canonical_fallback: None,
            content_address: None,
        }
    }
    fn raw(bytes: Vec<u8>) -> Self {
        Self {
            bytes: bytes.into(),
            canonical_fallback: None,
            content_address: None,
        }
    }
    /// Bytes whose SHA-256 is `address`, proved before this call.
    fn addressed(bytes: Vec<u8>, address: &str) -> Self {
        Self {
            content_address: Some(address.to_owned()),
            ..Self::raw(bytes)
        }
    }
}
impl ApiState {
    pub fn new(pool: PgPool, config: ApiConfig, registry: Arc<crate::metrics::Metrics>) -> Self {
        let (public_pool, audit_decodes, audit_rebuilds) = read_limits(
            &pool,
            config.read_concurrency,
            config.audit_rebuild_concurrency,
        );
        Self {
            pool,
            public_pool,
            audit_decodes,
            audit_rebuilds,
            audit_artifacts: Arc::new(Semaphore::new(config.audit_artifact_max_in_flight as usize)),
            public_service: None,
            config: Arc::new(config),
            health: Arc::new(RwLock::new(
                json!({"schema":"qbit.prism.audit-health.v1","ok":false,"state":"starting","error":"health snapshot warm-up has not completed yet"}),
            )),
            metrics: Arc::new(RwLock::new(MetricsSnapshot::default())),
            registry,
            latest_evidence: Arc::new(RwLock::new(None)),
            health_published_at: Arc::new(RwLock::new(Instant::now())),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP client"),
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    /// Resize the public read pool and the audit limits together.
    pub fn with_read_concurrency(mut self, concurrency: u32) -> Self {
        (self.public_pool, self.audit_decodes, self.audit_rebuilds) = read_limits(
            &self.pool,
            concurrency,
            self.config.audit_rebuild_concurrency,
        );
        self
    }
    /// The shared imported-audit decode limit, observed by the tests.
    #[doc(hidden)]
    pub fn audit_decode_limit(&self) -> Arc<Semaphore> {
        self.audit_decodes.clone()
    }
    /// The artifact route's rebuild and decode limit, observed by the tests.
    #[doc(hidden)]
    pub fn audit_rebuild_limit(&self) -> Arc<Semaphore> {
        self.audit_rebuilds.clone()
    }
    /// The artifact route's in-flight admission, observed by the tests.
    #[doc(hidden)]
    pub fn audit_artifact_admission(&self) -> Arc<Semaphore> {
        self.audit_artifacts.clone()
    }
    #[cfg(test)]
    pub(crate) fn health_published_at_for_test(&self) -> &RwLock<Instant> {
        &self.health_published_at
    }
    pub fn metrics(&self) -> Arc<crate::metrics::Metrics> {
        self.registry.clone()
    }
    pub fn publish_health(&self, payload: Value) {
        let mut health = self.health.write().unwrap_or_else(|e| e.into_inner());
        *health = payload;
        *self
            .health_published_at
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Instant::now();
    }
    /// Replace the complete metrics body and its publication instant together.
    pub fn publish_metrics(&self, body: String) -> anyhow::Result<()> {
        let mut snapshot = self
            .metrics
            .write()
            .map_err(|_| anyhow::anyhow!("metrics lock poisoned"))?;
        *snapshot = MetricsSnapshot::published(body);
        Ok(())
    }
    async fn rpc(&self, method: &str, params: Value) -> ApiResult<Value> {
        let response = self
            .client
            .post(&self.config.rpc_url)
            .basic_auth(&self.config.rpc_user, Some(&self.config.rpc_password))
            .json(&json!({"jsonrpc":"1.0","id":"prism-public","method":method,"params":params}))
            .send()
            .await
            .map_err(|_| ApiError::upstream(format!("qbit RPC {method} failed")))?;
        let payload: Value = response
            .error_for_status()
            .map_err(|_| ApiError::upstream(format!("qbit RPC {method} failed")))?
            .json()
            .await
            .map_err(|_| {
                ApiError::upstream(format!("qbit RPC {method} returned an invalid payload"))
            })?;
        if !payload["error"].is_null() {
            return Err(ApiError::upstream(format!("qbit RPC {method} failed")));
        }
        Ok(payload["result"].clone())
    }
}
/// One read concurrency bounds both the public read pool and the imported
/// audit decodes that continue after their connection is back in that pool.
/// Artifact rebuilds have their own limit: each holds a window in memory, so
/// raising the read concurrency must not admit more of them.
fn read_limits(
    pool: &PgPool,
    concurrency: u32,
    rebuilds: u32,
) -> (PgPool, Arc<Semaphore>, Arc<Semaphore>) {
    (
        public_service::read_pool(pool.connect_options().as_ref().clone(), concurrency),
        Arc::new(Semaphore::new(concurrency as usize)),
        Arc::new(Semaphore::new(rebuilds as usize)),
    )
}
/// Seconds an over-cap audit artifact request is told to wait before retrying:
/// about one measured rebuild at a 100,000-share window.
const AUDIT_ARTIFACT_RETRY_AFTER_SECONDS: u64 = 5;
/// The error code of that refusal, which the public service counts.
const AUDIT_ARTIFACT_BUSY: &str = "audit_artifact_busy";
pub fn router(state: ApiState) -> Router {
    Router::new().fallback(any(handle)).with_state(state)
}

#[derive(Clone, Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}
type ApiResult<T> = Result<T, ApiError>;
impl ApiError {
    fn bad(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "bad_request",
            message: message.into(),
        }
    }
    fn missing(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: message.into(),
        }
    }
    fn upstream(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "upstream_unavailable",
            message: message.into(),
        }
    }
    fn read_timeout() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "read_timeout",
            message: "the read timed out; try again shortly".into(),
        }
    }
    /// Refused before any audit read: too many audit artifacts are in flight.
    fn audit_artifact_busy() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: AUDIT_ARTIFACT_BUSY,
            message:
                "too many audit artifacts are being rebuilt; retry after the Retry-After delay"
                    .into(),
        }
    }
    /// The `Retry-After` delay an error carries, in seconds.
    fn retry_after(&self) -> Option<u64> {
        (self.code == AUDIT_ARTIFACT_BUSY).then_some(AUDIT_ARTIFACT_RETRY_AFTER_SECONDS)
    }
    fn internal() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: "internal server error".into(),
        }
    }
}
impl From<sqlx::Error> for ApiError {
    fn from(error: sqlx::Error) -> Self {
        tracing::warn!(%error,"public ledger read failed");
        if error
            .as_database_error()
            .is_some_and(|e| e.code().as_deref() == Some("57014"))
            || matches!(error, sqlx::Error::PoolTimedOut)
        {
            Self::read_timeout()
        } else {
            Self::internal()
        }
    }
}

async fn handle(
    State(state): State<ApiState>,
    uri: OriginalUri,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let response = handle_inner(State(state.clone()), uri, method, headers).await;
    if let Some(service) = &state.public_service {
        service.record_response(response.status());
    }
    response
}
async fn handle_inner(
    State(state): State<ApiState>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
) -> Response {
    if state.public_service.is_none() {
        if let Some(token) = &state.config.operator_bearer_token {
            if !operator_auth::authorized(&headers, token) {
                return finish(operator_auth::challenge(), &method);
            }
        }
    }
    let path = uri.path().trim_end_matches('/');
    let is_public = path == "/public/v1" || path.starts_with("/public/v1/");
    if let Some(service) = &state.public_service {
        service
            .requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    if state.public_service.is_some() && !is_public && !matches!(path, "/healthz" | "/metrics") {
        return finish(
            json_response(StatusCode::NOT_FOUND, json!({"error":"unknown endpoint"})),
            &method,
        );
    }
    if method == Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        cors(response.headers_mut());
        response.headers_mut().insert(
            "access-control-allow-methods",
            HeaderValue::from_static("GET, HEAD, OPTIONS"),
        );
        response.headers_mut().insert(
            "access-control-allow-headers",
            HeaderValue::from_static("If-None-Match, Content-Type"),
        );
        return response;
    }
    if method != Method::GET && method != Method::HEAD {
        let mut response = json_response(
            StatusCode::METHOD_NOT_ALLOWED,
            if is_public {
                json!({"schema":"prism.dashboard.error.v1","error":{"code":"bad_request","message":"method not allowed","request_id":null}})
            } else {
                json!({"error":"method not allowed"})
            },
        );
        response
            .headers_mut()
            .insert("allow", HeaderValue::from_static("GET, HEAD, OPTIONS"));
        return response;
    }
    if path == "/healthz" {
        if let Some(service) = &state.public_service {
            return finish(service.health_response(), &method);
        }
        let (mut payload, age) = {
            // Match the publisher's lock order so a later publication cannot
            // lend its freshness to the previous health payload.
            let health = state.health.read().unwrap_or_else(|e| e.into_inner());
            let age = state
                .health_published_at
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .elapsed();
            (health.clone(), age)
        };
        payload["snapshot_age_seconds"] = json!(age.as_secs_f64());
        if age > state.config.health_stale_after() {
            payload["ok"] = json!(false);
            payload["error"] = json!("health snapshot is stale");
        }
        state
            .registry
            .runtime()
            .snapshot()
            .apply_health(&mut payload);
        return finish(
            json_response(
                if payload["ok"] == true {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                },
                payload,
            ),
            &method,
        );
    }
    if path == "/metrics" {
        if let Some(service) = &state.public_service {
            return finish(service.metrics_response(), &method);
        }
        let snapshot = state
            .metrics
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        return finish(
            snapshot.response_with_runtime(
                Instant::now(),
                state.config.health_stale_after(),
                Some(state.registry.runtime().snapshot()),
                Some(&state.registry),
            ),
            &method,
        );
    }
    let query = Query::parse(uri.query().unwrap_or(""));
    let policy = if is_public {
        CachePolicy::for_path(path, &state.config)
    } else {
        CachePolicy::default()
    };
    let service_view = state.public_service.as_ref().map(|service| service.view());
    let (result, cache_status, age) = if is_public {
        response_cache::public_response(&state, path, &query, &policy, service_view.as_ref()).await
    } else {
        (
            audit(&state, path, &query).await.map(Payload::json),
            "BYPASS",
            0,
        )
    };
    if let (Some(service), Some(view)) = (&state.public_service, &service_view) {
        service.record_cache(path, cache_status, age, &result, view);
    }
    let response = match result {
        Ok(payload) => {
            // A content-addressed body carries the hash the ETag is made of;
            // only a body without one is hashed here.
            let etag = match &payload.content_address {
                Some(address) => format!("\"{address}\""),
                None => format!("\"{}\"", hex::encode(Sha256::digest(&payload.bytes))),
            };
            let matched = headers
                .get("if-none-match")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|h| {
                    h.split(',').any(|v| {
                        let v = v.trim().strip_prefix("W/").unwrap_or(v.trim());
                        v == etag || v == "*"
                    })
                });
            let mut response = if matched {
                StatusCode::NOT_MODIFIED.into_response()
            } else {
                (
                    [("content-type", "application/json")],
                    payload.bytes.clone(),
                )
                    .into_response()
            };
            response
                .headers_mut()
                .insert("etag", HeaderValue::from_str(&etag).expect("hash header"));
            if is_public {
                if let Some(reason) = &payload.canonical_fallback {
                    response
                        .headers_mut()
                        .insert("cache-control", HeaderValue::from_static("no-store"));
                    response.headers_mut().insert(
                        "x-prism-artifact-canonical-state",
                        HeaderValue::from_str(reason).unwrap(),
                    );
                } else {
                    policy.headers(response.headers_mut(), cache_status, age);
                }
            }
            response
        }
        Err(error) => {
            let mut response = json_response(
                error.status,
                if is_public {
                    json!({"schema":"prism.dashboard.error.v1","error":{"code":error.code,"message":error.message,"request_id":null}})
                } else {
                    operational_error(path, &error)
                },
            );
            if let Some(seconds) = error.retry_after() {
                response
                    .headers_mut()
                    .insert("retry-after", HeaderValue::from(seconds));
            }
            response
        }
    };
    let mut response = response;
    if is_public {
        if let Some(view) = service_view {
            public_service::decorate(&mut response, path, &policy, &view, age, cache_status);
        }
    }
    finish(response, &method)
}
fn operational_error(path: &str, error: &ApiError) -> Value {
    let mut payload = json!({"error":error.message});
    if error.status != StatusCode::NOT_FOUND {
        return payload;
    }
    let resource = if let Some(v) = path
        .strip_prefix("/audit/blocks/")
        .and_then(|s| s.rsplit_once('/').map(|v| v.0))
        .or_else(|| path.strip_prefix("/audit/block/"))
    {
        Some(("block_hash", v))
    } else if let Some(v) = path
        .strip_prefix("/audit/fanouts/")
        .and_then(|v| v.strip_suffix("/status"))
    {
        Some(("fanout_txid", v))
    } else {
        path.strip_prefix("/audit/commitments/")
            .and_then(|v| v.strip_suffix("/bundle"))
            .map(|v| ("audit_commitment_leaf_hex", v))
    };
    if let Some((key, value)) = resource {
        if let Ok(hash) = clean_hash(value, "hash") {
            payload[key] = json!(hash);
        }
    }
    payload
}
fn finish(mut response: Response, method: &Method) -> Response {
    if method == Method::HEAD {
        *response.body_mut() = Body::empty();
    }
    cors(response.headers_mut());
    response
}
fn cors(headers: &mut HeaderMap) {
    headers.insert("access-control-allow-origin", HeaderValue::from_static("*"));
    headers.insert(
        "access-control-expose-headers",
        HeaderValue::from_static("ETag, Age, X-Prism-Public-Cache, X-Prism-Staleness-Budget-Seconds, X-Prism-Database-State, X-Prism-Replica-Lag-Seconds, X-Prism-Artifact-Canonical-State, Warning, Retry-After"),
    );
}
fn json_response(status: StatusCode, payload: Value) -> Response {
    let mut response = (status, axum::Json(payload)).into_response();
    if !status.is_success() {
        response
            .headers_mut()
            .insert("cache-control", HeaderValue::from_static("no-store"));
    }
    response
}

#[derive(Default, Clone, Debug)]
struct Query(BTreeMap<String, Vec<String>>);
impl Query {
    fn parse(value: &str) -> Self {
        let mut q = BTreeMap::<String, Vec<String>>::new();
        for (k, v) in url::form_urlencoded::parse(value.as_bytes()) {
            if !v.is_empty() {
                q.entry(k.into_owned()).or_default().push(v.into_owned());
            }
        }
        Self(q)
    }
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.first()).map(String::as_str)
    }
    fn page(&self) -> ApiResult<(i64, i64)> {
        let page = self
            .get("page")
            .unwrap_or("1")
            .parse::<i64>()
            .map_err(|_| ApiError::bad("page and limit must be integers"))?;
        let limit = self
            .get("limit")
            .unwrap_or("15")
            .parse::<i64>()
            .map_err(|_| ApiError::bad("page and limit must be integers"))?;
        if page < 1 {
            return Err(ApiError::bad("page must be >= 1"));
        }
        if !(1..=100).contains(&limit) {
            return Err(ApiError::bad("limit must be between 1 and 100"));
        }
        if (page - 1).checked_mul(limit).is_none() {
            return Err(ApiError::bad("page is too large"));
        }
        Ok((page, limit))
    }
    fn search(&self) -> ApiResult<Option<&str>> {
        let s = self.get("search");
        if s.is_some_and(|v| v.chars().count() > 128) {
            return Err(ApiError::bad("search must be 128 characters or fewer"));
        }
        Ok(s)
    }
}
#[derive(Default, Clone)]
struct CachePolicy {
    ttl: u64,
    stale: u64,
    immutable: bool,
    debug_headers: bool,
}
impl CachePolicy {
    fn for_path(path: &str, config: &ApiConfig) -> Self {
        if !config.cache_enabled {
            return Self {
                debug_headers: config.cache_debug_headers,
                ..Self::default()
            };
        }
        let lifetimes = &config.cache_lifetimes;
        let immutable = path.starts_with("/public/v1/artifacts/");
        let lifetime = if path == "/public/v1/mining-configuration" {
            lifetimes.configuration
        } else if immutable {
            lifetimes.artifact
        } else if matches!(
            path,
            "/public/v1/pool-summary" | "/public/v1/hashrate-series"
        ) || path.starts_with("/public/v1/miners/") && path.ends_with("/workers")
        {
            lifetimes.aggregate
        } else {
            lifetimes.default
        };
        Self {
            ttl: lifetime.ttl,
            stale: lifetime.stale_while_revalidate,
            immutable,
            debug_headers: config.cache_debug_headers,
        }
    }
    fn headers(&self, h: &mut HeaderMap, state: &str, age: u64) {
        h.insert(
            "cache-control",
            HeaderValue::from_static("public, max-age=0, must-revalidate"),
        );
        h.insert("age", HeaderValue::from_str(&age.to_string()).unwrap());
        if self.ttl > 0 {
            let mut value = format!("public, max-age={}", self.ttl);
            if self.stale > 0 {
                value.push_str(&format!(", stale-while-revalidate={}", self.stale));
            }
            if self.immutable {
                value.push_str(", immutable");
            }
            let value = HeaderValue::from_str(&value).unwrap();
            h.insert("cdn-cache-control", value.clone());
            h.insert("vercel-cdn-cache-control", value);
        }
        if self.debug_headers {
            h.insert(
                "x-prism-public-cache",
                HeaderValue::from_str(state).unwrap(),
            );
        }
    }
}
fn cache_key(path: &str, q: &Query) -> String {
    let canonical = if path.starts_with("/public/v1/artifacts/")
        || path.starts_with("/public/v1/blocks/")
        || path.starts_with("/public/v1/fanouts/")
    {
        path.split('/')
            .map(|p| {
                if p.len() == 64 && p.bytes().all(|b| b.is_ascii_hexdigit()) {
                    p.to_ascii_lowercase()
                } else {
                    p.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("/")
    } else {
        path.into()
    };
    format!("{canonical}?{}", serde_json::to_string(&q.0).unwrap())
}
fn decode(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}
fn clean_hash(s: &str, name: &str) -> ApiResult<String> {
    let s = decode(s).trim().to_string();
    if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ApiError::bad(format!("{name} must be 64 hex characters")));
    }
    Ok(s.to_ascii_lowercase())
}
fn recipient(s: &str) -> ApiResult<String> {
    let s = decode(s).trim().to_string();
    if s.is_empty() {
        return Err(ApiError::bad("recipient_id is required"));
    }
    if s.chars().count() > 256 {
        return Err(ApiError::bad(
            "recipient_id must be 256 characters or fewer",
        ));
    }
    Ok(s)
}
fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

async fn audit(state: &ApiState, path: &str, q: &Query) -> ApiResult<Value> {
    match path {
        "/owed" | "/owed-balances" => {
            return Ok(
                json!({"schema":"qbit.prism.owed-balances.v1","ledger_backend":"postgres-native","balances":read_models::owed(state).await?}),
            )
        }
        "/audit/carry-forward-integrity" | "/audit/ledger-integrity" => {
            let mut value: Value =
                sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
                    .fetch_one(&state.pool)
                    .await?;
            // #478: debt from divergent landings is exact accounting, so the
            // report's mismatch count never shows it; this line does.
            value["payout_divergence"] =
                sqlx::query_scalar("SELECT qbit_prism_payout_divergence_report()")
                    .fetch_one(&state.pool)
                    .await?;
            value["ledger_backend"] = json!("postgres-native");
            return Ok(value);
        }
        "/audit/latest" => {
            let row = state
                .latest_evidence
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(row) = row {
                return Ok(row);
            }
            return read_models::latest_evidence(state).await;
        }
        "/audit/share-window" => {
            let anchor = q
                .get("anchor_job_issued_at_ms")
                .or(q.get("anchor"))
                .ok_or_else(|| {
                    ApiError::bad("anchor_job_issued_at_ms and network_difficulty are required")
                })?
                .parse::<i64>()
                .map_err(|_| ApiError::bad("anchor_job_issued_at_ms must be an integer"))?;
            let difficulty = q.get("network_difficulty").ok_or_else(|| {
                ApiError::bad("anchor_job_issued_at_ms and network_difficulty are required")
            })?;
            if difficulty.parse::<num_bigint::BigUint>().is_err()
                || difficulty.bytes().all(|v| v == b'0')
            {
                return Err(ApiError::bad("network_difficulty must be positive"));
            }
            let mut rows: Value =
                sqlx::query_scalar(include_str!("api/queries/audit_share_window.sql"))
                    .bind(anchor as f64)
                    .bind(difficulty)
                    .fetch_one(&state.pool)
                    .await?;
            read_models::numeric_fields(
                &mut rows,
                &[
                    "window_multiplier",
                    "requested_window_weight",
                    "share_difficulty",
                    "counted_difficulty",
                ],
            );
            return Ok(
                json!({"schema":"qbit.prism.audit-share-window.v1","ledger_backend":"postgres-native","rows":rows}),
            );
        }
        "/audit/fanouts/pending" => {
            let limit = q
                .get("limit")
                .unwrap_or("100")
                .parse::<i64>()
                .map_err(|_| ApiError::bad("limit must be an integer"))?
                .clamp(1, 1000);
            let payload = read_models::pending_fanouts(state, 1, limit).await?;
            return Ok(
                json!({"schema":"qbit.prism.pending-ctv-fanouts.v1","ledger_backend":"postgres-native","count":payload["rows"].as_array().map_or(0,Vec::len),"rows":payload["rows"]}),
            );
        }
        _ => {}
    }
    if let Some(id) = path
        .strip_prefix("/miners/")
        .or_else(|| path.strip_prefix("/payouts/"))
        .and_then(|p| p.strip_suffix("/status"))
    {
        let id = recipient(id)?;
        let balances = read_models::owed(state).await?;
        let matching: Vec<Value> = balances
            .as_array()
            .into_iter()
            .flatten()
            .filter(|b| b["recipient_id"] == id)
            .cloned()
            .collect();
        let total = matching
            .iter()
            .map(|v| read_models::big(&v["balance_sats"]))
            .sum::<num_bigint::BigUint>();
        let mut history: Value =
            sqlx::query_scalar(include_str!("api/queries/recipient_payout_history.sql"))
                .bind(&id)
                .bind(50i64)
                .fetch_one(&state.pool)
                .await?;
        read_models::numeric_fields(&mut history, &["carry_forward_balance_sats"]);
        return Ok(
            json!({"schema":"qbit.prism.miner-status.v1","ledger_backend":"postgres-native","recipient_id":id,"owed_balance_sats":read_models::big_json(total),"owed_balances":matching,"recent_payouts":history}),
        );
    }
    if let Some(leaf) = path
        .strip_prefix("/audit/commitments/")
        .and_then(|p| p.strip_suffix("/bundle"))
    {
        return read_models::bundle(state, &clean_hash(leaf, "audit commitment leaf")?, true).await;
    }
    if let Some(id) = path
        .strip_prefix("/audit/fanouts/")
        .and_then(|p| p.strip_suffix("/status"))
    {
        return read_models::fanout(state, &clean_hash(id, "fanout txid")?).await;
    }
    if let Some(suffix) = path.strip_prefix("/audit/blocks/") {
        if let Some((hash, kind)) = suffix.rsplit_once('/') {
            let hash = clean_hash(hash, "block hash")?;
            match kind {
                "bundle" => return read_models::bundle(state, &hash, false).await,
                "payouts" => return read_models::audit_payouts(state, &hash).await,
                "ctv-fanout-manifest-set" => return read_models::manifest_set(state, &hash).await,
                "ctv-fanouts" => {
                    let payload = read_models::manifest_set(state, &hash).await?;
                    return Ok(
                        json!({"schema":"qbit.prism.audit-ctv-fanouts.v1","ledger_backend":"postgres-native","block_hash":hash,"rows":payload["artifacts"]}),
                    );
                }
                _ => {}
            }
        }
    }
    if let Some(hash) = path.strip_prefix("/audit/block/") {
        return read_models::audit_payouts(state, &clean_hash(hash, "block hash")?).await;
    }
    Err(ApiError::missing("unknown endpoint"))
}

#[cfg(test)]
mod health_tests {
    use super::*;
    use axum::http::Request;
    use tower::ServiceExt;
    #[tokio::test]
    async fn expired_health_snapshot_returns_503_even_when_last_snapshot_was_ready() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
            .unwrap();
        let state = ApiState::new(
            pool,
            ApiConfig::default(),
            std::sync::Arc::new(crate::metrics::Metrics::default()),
        );
        state.publish_health(json!({"ok":true,"schema":"qbit.prism.audit-health.v1"}));
        *state.health_published_at.write().unwrap() = Instant::now() - Duration::from_secs(3600);
        let response = router(state)
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
