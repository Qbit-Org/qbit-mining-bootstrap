use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

/// The node answered a call with a JSON-RPC error object: the request reached
/// the node and the node refused it, as distinct from a transport or protocol
/// failure. The message is the one every caller always saw; what this adds is
/// a type a caller can downcast to when "no such block" must be told apart
/// from "the node is unreachable", as the operator recovery command must.
#[derive(Clone, Debug, thiserror::Error)]
#[error("qbit RPC {method}: {error}")]
pub struct RpcReplyError {
    pub method: String,
    pub error: Value,
}

/// The JSON-RPC error code qbitd answers every call with while it is still
/// warming up after a start (`RPC_IN_WARMUP`).
pub const RPC_IN_WARMUP: i64 = -28;

impl RpcReplyError {
    /// The node's JSON-RPC error code, when the error object carries an
    /// integer one.
    pub fn code(&self) -> Option<i64> {
        self.error["code"].as_i64()
    }

    /// The node answered that it is still warming up. qbitd (like the Bitcoin
    /// Core it derives from) checks its warmup flag first in
    /// `CRPCTable::execute`, before it looks the method up, and nothing else
    /// raises this code; the flag is cleared once, at the end of startup. So
    /// this reply proves that the node did not run the call (#526). That
    /// holds for the node the call was sent to: the RPC URL names one node
    /// (a path-routing reverse proxy is fine). An endpoint that retries a
    /// failed POST on another backend is not supported; it could double-submit
    /// a block on its own, with or without this rule.
    pub fn in_warmup(&self) -> bool {
        self.code() == Some(RPC_IN_WARMUP)
    }
}

/// A call that provably never reached the node: the client failed to
/// establish the connection the request would have been written to (the
/// connection was refused, the name did not resolve, no route, the connect
/// timeout, or a TLS or proxy handshake failure), so not one byte of the
/// request left this process for the node (#522).
///
/// The boundary is reqwest's `is_connect()`, which holds exactly when hyper's
/// client reports its `Connect` error kind. hyper raises that kind only while
/// obtaining a connection, from the connector or the pool checkout, and a
/// request is handed to a connection only after one was obtained. hyper
/// retries on another connection only a request its dispatcher returned
/// unwritten, so a connect error after such a retry still proves that no
/// attempt wrote the request. A failure on an established connection, a reset
/// or a closed connection after the write, a lost reply, the request timeout
/// (even one that expires while connecting) and every HTTP or JSON-RPC error
/// are not this type: the request may have reached the node. The message keeps
/// the transport-failure wording every caller already matched on.
#[derive(Clone, Debug, thiserror::Error)]
#[error("qbit RPC {method} transport failed before the request was sent: the connection could not be established ({cause})")]
pub struct RpcNotSentError {
    pub method: String,
    /// The operating system's reason, when the failure carries one (such as
    /// "connection refused"), else "connect failed". Never the URL.
    pub cause: String,
}

impl RpcNotSentError {
    fn from_connect(method: &str, error: &reqwest::Error) -> Self {
        let mut source = std::error::Error::source(error);
        let mut cause = None;
        while let Some(error) = source {
            if let Some(io) = error.downcast_ref::<std::io::Error>() {
                cause = Some(io.kind().to_string());
            }
            source = error.source();
        }
        Self {
            method: method.to_owned(),
            cause: cause.unwrap_or_else(|| "connect failed".to_owned()),
        }
    }
}

/// The calls that hand the node a block or a transaction to relay to the
/// network. PRISM makes the first three; the wallet's own sends are listed
/// so that no later caller can relay through one either (#291).
pub const RELAY_METHODS: &[&str] = &[
    "submitblock",
    "sendrawtransaction",
    "submitpackage",
    "sendtoaddress",
    "sendmany",
    "send",
    "sendall",
    "bumpfee",
];

/// A [`RELAY_METHODS`] call that a client built [`Rpc::without_relay`]
/// refused (#291). As with [`RpcNotSentError`], no byte of the request left
/// this process.
#[derive(Clone, Debug, thiserror::Error)]
#[error("qbit RPC {method} refused before it was sent: block submission is disabled by PRISM_BLOCK_SUBMIT_ENABLED")]
pub struct RpcRelayRefused {
    pub method: String,
}

/// qbitd's default `-rpcservertimeout`: how long the node keeps an idle,
/// kept-alive connection open before it closes it.
pub const QBITD_DEFAULT_RPCSERVERTIMEOUT: Duration = Duration::from_secs(30);

/// How long the client keeps an idle connection to the node for reuse (#759).
/// It stays below the node's own idle close, [`QBITD_DEFAULT_RPCSERVERTIMEOUT`]
/// unless the node sets another. A request written into a connection just as
/// the node closes it is never read, and fails after the connection existed.
/// With the bound below the node's, the client always drops an idle
/// connection first. [`RELAY_METHODS`] calls never reuse a connection at all
/// (see [`Rpc`]).
pub const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(20);
const _: () = assert!(POOL_IDLE_TIMEOUT.as_secs() < QBITD_DEFAULT_RPCSERVERTIMEOUT.as_secs());

/// An HTTP client builder for a node's RPC endpoint, keeping idle connections
/// for at most [`POOL_IDLE_TIMEOUT`]. The public API's node client is built
/// from it.
pub fn node_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().pool_idle_timeout(POOL_IDLE_TIMEOUT)
}

/// Reusable, deadline-bound HTTP connections. Mutating calls are never retried
/// blindly: callers reconcile their durable outbox against chain state first.
#[derive(Clone)]
pub struct Rpc {
    client: reqwest::Client,
    /// The client for [`RELAY_METHODS`], which keeps no idle connection, so
    /// every relay opens a new one (#759). A relay written into a reused
    /// connection that the node, or anything between, was closing fails after
    /// the connection existed, and `submitblock` never offers that block
    /// again. A new connection costs one connect on a rare path. It leaves only
    /// a connect failure, which is provably unsent (#522), or a real unknown.
    fresh: reqwest::Client,
    pool_idle_timeout: Duration,
    url: String,
    user: String,
    password: String,
    next_id: Arc<AtomicU64>,
    /// Whether [`RELAY_METHODS`] calls are sent; inherited by wallet clients.
    relay: bool,
}

impl Rpc {
    pub fn wallet(&self, name: &str) -> Result<Self> {
        let mut rpc = self.clone();
        let mut url = url::Url::parse(&rpc.url)?;
        // A reverse proxy may route this node below a path prefix. Retain
        // that prefix and replace only an existing terminal wallet selection.
        let segments: Vec<_> = url
            .path_segments()
            .context("invalid wallet RPC URL")?
            .collect();
        let trimmed = segments.strip_suffix(&[""]).unwrap_or(&segments);
        let replace_wallet = trimmed.len() >= 2 && trimmed[trimmed.len() - 2] == "wallet";
        let empty_wallet = segments.len() >= 2
            && segments[segments.len() - 2] == "wallet"
            && segments.last() == Some(&"");
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("invalid wallet RPC URL"))?;
            path.pop_if_empty();
            if replace_wallet {
                path.pop().pop();
            } else if empty_wallet {
                path.pop();
            }
            path.push("wallet").push(name);
        }
        rpc.url = url.into();
        Ok(rpc)
    }
    pub fn new(url: String, user: String, password: String, timeout: Duration) -> Result<Self> {
        Self::with_pool_idle_timeout(url, user, password, timeout, POOL_IDLE_TIMEOUT)
    }

    /// [`Rpc::new`] with another bound on how long an idle pooled connection
    /// is kept: for a node whose idle close is shorter than qbitd's default,
    /// and for tests. Keep it below the node's idle close (#759).
    pub fn with_pool_idle_timeout(
        url: String,
        user: String,
        password: String,
        timeout: Duration,
        pool_idle_timeout: Duration,
    ) -> Result<Self> {
        let parsed = url::Url::parse(&url).context("invalid QBIT RPC URL")?;
        anyhow::ensure!(
            matches!(parsed.scheme(), "http" | "https"),
            "QBIT RPC URL must use http(s)"
        );
        let build = |max_idle: usize| {
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(timeout)
                .pool_idle_timeout(pool_idle_timeout)
                .pool_max_idle_per_host(max_idle)
                .redirect(reqwest::redirect::Policy::none())
                .build()
        };
        Ok(Self {
            client: build(usize::MAX)?,
            fresh: build(0)?,
            pool_idle_timeout,
            url,
            user,
            password,
            next_id: Arc::new(AtomicU64::new(1)),
            relay: true,
        })
    }

    /// How long an idle pooled connection is kept for reuse.
    pub fn pool_idle_timeout(&self) -> Duration {
        self.pool_idle_timeout
    }

    /// This client, and every wallet client made from it, refusing each
    /// [`RELAY_METHODS`] call before it is sent: a frontend with
    /// `PRISM_BLOCK_SUBMIT_ENABLED` off talks to its node through one (#291).
    pub fn without_relay(mut self) -> Self {
        self.relay = false;
        self
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.call_timeout(method, params, None).await
    }

    pub async fn call_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Option<Duration>,
    ) -> Result<Value> {
        if !self.relay && RELAY_METHODS.contains(&method) {
            return Err(RpcRelayRefused {
                method: method.to_owned(),
            }
            .into());
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let client = if RELAY_METHODS.contains(&method) {
            &self.fresh
        } else {
            &self.client
        };
        let mut request = client
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.password))
            .json(&json!({"jsonrpc":"1.0","id":id,"method":method,"params":params}));
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
        // Do not include the request URL or credentials in diagnostics.
        let response = request.send().await.map_err(|error| {
            if error.is_connect() {
                RpcNotSentError::from_connect(method, &error).into()
            } else {
                anyhow::anyhow!("qbit RPC {method} transport failed")
            }
        })?;
        let status = response.status();
        let value: Value = response.json().await.map_err(|_| {
            anyhow::anyhow!("qbit RPC {method} returned invalid JSON (HTTP {status})")
        })?;
        if !value["error"].is_null() {
            return Err(RpcReplyError {
                method: method.to_owned(),
                error: value["error"].clone(),
            }
            .into());
        }
        anyhow::ensure!(
            status.is_success(),
            "qbit RPC {method} failed with HTTP {status}"
        );
        anyhow::ensure!(value["id"] == id, "qbit RPC {method} response ID mismatch");
        value
            .get("result")
            .cloned()
            .context("qbit RPC missing result")
    }
}
