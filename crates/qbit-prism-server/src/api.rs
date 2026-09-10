//! Compatibility HTTP API. Every accounting read uses the shared PostgreSQL ledger.
mod charts;
mod metrics_snapshot;
mod public;
pub mod public_service;
mod read_models;
mod response_cache;

use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use chrono::{SecondsFormat, Utc};
use metrics_snapshot::{health_stale_after, MetricsSnapshot};
use percent_encoding::percent_decode_str;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

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
    pub read_timeout: Duration,
}
impl ApiConfig {
    pub fn from_env() -> Self {
        let (rpc_url, rpc_user, rpc_password) = crate::config::rpc_connection_from_env();
        Self {
            rpc_url,
            rpc_user,
            rpc_password,
            stratum_host: std::env::var("PRISM_PUBLIC_STRATUM_HOST")
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| env("PRISM_STRATUM_BIND", "127.0.0.1")),
            stratum_port: env_num("PRISM_STRATUM_PORT", 3340).min(u16::MAX as u64) as u16,
            instance_id: env("PRISM_INSTANCE_ID", "prism"),
            pool_name: env("PRISM_PUBLIC_POOL_NAME", "PRISM"),
            pool_fee_bps: env_num("PRISM_PUBLIC_POOL_FEE_BPS", 0).min(10000) as u16,
            minimum_payout_bits: [
                "PRISM_PUBLIC_MINIMUM_PAYOUT_BITS",
                "PRISM_PAYOUT_MIN_OUTPUT_BITS",
                "PRISM_PAYOUT_MIN_OUTPUT_SATS",
            ]
            .iter()
            .find_map(|k| std::env::var(k).ok()?.parse::<u64>().ok())
            .unwrap_or(0),
            explorer_block_url: std::env::var("PRISM_PUBLIC_EXPLORER_BLOCK_URL_PREFIX")
                .ok()
                .filter(|v| !v.is_empty()),
            explorer_tx_url: std::env::var("PRISM_PUBLIC_EXPLORER_TX_URL_PREFIX")
                .ok()
                .filter(|v| !v.is_empty()),
            read_timeout: Duration::from_secs(env_num(
                "PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS",
                20,
            )),
            cache_enabled: env_bool("PRISM_PUBLIC_CACHE_ENABLED", true),
            cache_max_entries: env_num("PRISM_PUBLIC_CACHE_MAX_ENTRIES", 1024).max(1) as usize,
            cache_max_bytes: env_num(
                "PRISM_PUBLIC_CACHE_MAX_RESPONSE_BYTES",
                env_num("PRISM_PUBLIC_CACHE_MAX_PAYLOAD_BYTES", 1024 * 1024),
            ) as usize,
        }
    }
}
impl Default for ApiConfig {
    fn default() -> Self {
        Self::from_env()
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
    public_service: Option<Arc<public_service::ServiceState>>,
}
#[derive(Clone, Debug)]
struct Payload {
    bytes: Bytes,
    canonical_fallback: Option<String>,
}
impl Payload {
    fn json(value: Value) -> Self {
        Self {
            bytes: Bytes::from(serde_json::to_vec(&value).expect("JSON value")),
            canonical_fallback: None,
        }
    }
    fn raw(bytes: Vec<u8>) -> Self {
        Self {
            bytes: bytes.into(),
            canonical_fallback: None,
        }
    }
}
impl ApiState {
    pub fn new(pool: PgPool, config: ApiConfig, registry: Arc<crate::metrics::Metrics>) -> Self {
        let public_pool = public_service::read_pool(
            pool.connect_options().as_ref().clone(),
            env_num("PRISM_POSTGRES_READ_CONCURRENCY", 4).clamp(1, 1024) as u32,
        );
        Self {
            pool,
            public_pool,
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
        if age > health_stale_after() {
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
                health_stale_after(),
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
            let etag = format!("\"{}\"", hex::encode(Sha256::digest(&payload.bytes)));
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
        Err(error) => json_response(
            error.status,
            if is_public {
                json!({"schema":"prism.dashboard.error.v1","error":{"code":error.code,"message":error.message,"request_id":null}})
            } else {
                operational_error(path, &error)
            },
        ),
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
        HeaderValue::from_static("ETag, Age, X-Prism-Public-Cache, X-Prism-Staleness-Budget-Seconds, X-Prism-Database-State, X-Prism-Replica-Lag-Seconds, X-Prism-Artifact-Canonical-State, Warning"),
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
}
impl CachePolicy {
    fn for_path(path: &str, config: &ApiConfig) -> Self {
        if !config.cache_enabled {
            return Self::default();
        }
        let (kind, ttl, stale) = if path == "/public/v1/mining-configuration" {
            ("CONFIG_", 300, 3600)
        } else if path.starts_with("/public/v1/artifacts/") {
            ("ARTIFACT_", 86400, 86400)
        } else if matches!(
            path,
            "/public/v1/pool-summary" | "/public/v1/hashrate-series"
        ) || path.starts_with("/public/v1/miners/") && path.ends_with("/workers")
        {
            ("AGGREGATE_", 30, 30)
        } else {
            ("", 5, 30)
        };
        Self {
            ttl: env_num(&format!("PRISM_PUBLIC_{kind}CACHE_TTL_SECONDS"), ttl),
            stale: env_num(
                &format!("PRISM_PUBLIC_{kind}CACHE_STALE_WHILE_REVALIDATE_SECONDS"),
                stale,
            ),
            immutable: kind == "ARTIFACT_",
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
        if env_bool("PRISM_PUBLIC_CACHE_DEBUG_HEADERS", false) {
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
fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.into())
}
fn env_num(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(default)
}
fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .map(|s| matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
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
