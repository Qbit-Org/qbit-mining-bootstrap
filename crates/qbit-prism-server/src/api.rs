//! Compatibility HTTP API. Every accounting read uses the shared PostgreSQL ledger.
mod public;
mod read_models;

use axum::{
    body::Body,
    extract::{OriginalUri, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use chrono::{SecondsFormat, Utc};
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
}
impl ApiConfig {
    pub fn from_env() -> Self {
        let rpc_host = env("QBIT_RPC_HOST", "qbit-node");
        let rpc_port = env("QBIT_RPC_PORT", "18443");
        Self {
            rpc_url: env("QBIT_RPC_URL", &format!("http://{rpc_host}:{rpc_port}")),
            rpc_user: env("QBIT_RPC_USER", "qbit"),
            rpc_password: env("QBIT_RPC_PASSWORD", "qbit"),
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

type ResponseCache = Arc<Mutex<BTreeMap<String, Arc<Mutex<Option<CacheEntry>>>>>>;

#[derive(Clone)]
pub struct ApiState {
    pub pool: PgPool,
    pub config: Arc<ApiConfig>,
    /// Published by the runtime; health handlers never wait on database queries.
    pub health: Arc<RwLock<Value>>,
    pub metrics: Arc<RwLock<String>>,
    pub latest_evidence: Arc<RwLock<Option<Value>>>,
    health_published_at: Arc<RwLock<Instant>>,
    client: reqwest::Client,
    cache: ResponseCache,
}
#[derive(Clone)]
struct CacheEntry {
    payload: Value,
    created: Instant,
}
impl ApiState {
    pub fn new(pool: PgPool, config: ApiConfig) -> Self {
        Self {
            pool,
            config: Arc::new(config),
            health: Arc::new(RwLock::new(
                json!({"schema":"qbit.prism.audit-health.v1","ok":false,"state":"starting","error":"health snapshot warm-up has not completed yet"}),
            )),
            metrics: Arc::new(RwLock::new(String::new())),
            latest_evidence: Arc::new(RwLock::new(None)),
            health_published_at: Arc::new(RwLock::new(Instant::now())),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP client"),
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    pub fn publish_health(&self, payload: Value) {
        let mut health = self.health.write().unwrap_or_else(|e| e.into_inner());
        *health = payload;
        *self
            .health_published_at
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Instant::now();
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

#[derive(Debug)]
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
        Self::internal()
    }
}

async fn handle(
    State(state): State<ApiState>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let path = uri.path().trim_end_matches('/');
    let is_public = path == "/public/v1" || path.starts_with("/public/v1/");
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
        let mut payload = state
            .health
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let age = state
            .health_published_at
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .elapsed();
        payload["snapshot_age_seconds"] = json!(age.as_secs_f64());
        if age
            > Duration::from_secs(
                env_num("PRISM_HEALTH_REFRESH_SECONDS", 2)
                    .saturating_mul(3)
                    .max(15),
            )
        {
            payload["ok"] = json!(false);
            payload["error"] = json!("health snapshot is stale");
        }
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
        let body = state
            .metrics
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        return finish(
            ([("content-type", "text/plain; version=0.0.4")], body).into_response(),
            &method,
        );
    }
    let query = Query::parse(uri.query().unwrap_or(""));
    let (result, cache_status, age, policy) = if is_public {
        let policy = CachePolicy::for_path(path, &state.config);
        if policy.ttl == 0 {
            (
                public::dispatch(&state, path, &query).await,
                "bypass",
                0,
                policy,
            )
        } else {
            let key = cache_key(path, &query);
            let slot = {
                let mut cache = state.cache.lock().await;
                if cache.len() >= state.config.cache_max_entries && !cache.contains_key(&key) {
                    // Keep active computations pinned, evict only idle entries.
                    if let Some(key) = cache
                        .iter()
                        .find(|(_, v)| Arc::strong_count(v) == 1)
                        .map(|(k, _)| k.clone())
                    {
                        cache.remove(&key);
                    }
                }
                if cache.len() >= state.config.cache_max_entries && !cache.contains_key(&key) {
                    None
                } else {
                    Some(
                        cache
                            .entry(key.clone())
                            .or_insert_with(|| Arc::new(Mutex::new(None)))
                            .clone(),
                    )
                }
            };
            if let Some(slot) = slot {
                let mut entry = slot.lock().await;
                if let Some(cached) = entry
                    .as_ref()
                    .filter(|v| v.created.elapsed().as_secs() < policy.ttl)
                {
                    (
                        Ok(cached.payload.clone()),
                        "hit",
                        cached.created.elapsed().as_secs(),
                        policy,
                    )
                } else {
                    let result = public::dispatch(&state, path, &query).await;
                    *entry = None;
                    if let Ok(payload) = &result {
                        *entry = Some(CacheEntry {
                            payload: payload.clone(),
                            created: Instant::now(),
                        });
                        if !serde_json::to_vec(payload)
                            .is_ok_and(|v| v.len() <= state.config.cache_max_bytes)
                        {
                            state.cache.lock().await.remove(&key);
                        }
                    }
                    (result, "miss", 0, policy)
                }
            } else {
                (
                    public::dispatch(&state, path, &query).await,
                    "bypass",
                    0,
                    policy,
                )
            }
        }
    } else {
        (
            audit(&state, path, &query).await,
            "bypass",
            0,
            CachePolicy::default(),
        )
    };
    let response = match result {
        Ok(payload) => {
            let bytes = serde_json::to_vec(&payload).expect("JSON value");
            let etag = format!("\"{}\"", hex::encode(Sha256::digest(&bytes)));
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
                json_response(StatusCode::OK, payload)
            };
            response
                .headers_mut()
                .insert("etag", HeaderValue::from_str(&etag).expect("hash header"));
            if is_public {
                policy.headers(response.headers_mut(), cache_status, age);
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
        HeaderValue::from_static("ETag, Age, X-Prism-Public-Cache"),
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
#[derive(Default)]
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
        let state = ApiState::new(pool, ApiConfig::default());
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
