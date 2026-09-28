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

/// Reusable, deadline-bound HTTP connections. Mutating calls are never retried
/// blindly: callers reconcile their durable outbox against chain state first.
#[derive(Clone)]
pub struct Rpc {
    client: reqwest::Client,
    url: String,
    user: String,
    password: String,
    next_id: Arc<AtomicU64>,
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
        let parsed = url::Url::parse(&url).context("invalid QBIT RPC URL")?;
        anyhow::ensure!(
            matches!(parsed.scheme(), "http" | "https"),
            "QBIT RPC URL must use http(s)"
        );
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(timeout)
            .pool_idle_timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            url,
            user,
            password,
            next_id: Arc::new(AtomicU64::new(1)),
        })
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
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut request = self
            .client
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
