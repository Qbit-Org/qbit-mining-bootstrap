use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

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
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid wallet RPC URL"))?
            .clear()
            .push("wallet")
            .push(name);
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
        let response = request
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("qbit RPC {method} transport failed"))?;
        let status = response.status();
        let value: Value = response.json().await.map_err(|_| {
            anyhow::anyhow!("qbit RPC {method} returned invalid JSON (HTTP {status})")
        })?;
        if !value["error"].is_null() {
            bail!("qbit RPC {method}: {}", value["error"]);
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
