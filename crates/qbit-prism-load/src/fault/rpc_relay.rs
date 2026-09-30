//! A JSON-RPC relay between each frontend and its node, for the faults that
//! need a found block's offer at a known moment (#554).
//!
//! Every frontend gets its own listening port, so the relay knows which
//! frontend is offering a block. Calls pass through unchanged, headers
//! included, to whatever node the run drives (the fake node or #547's
//! recording relay in front of `qbitd`), and each `submitblock` is recorded.
//! The run launches its frontends on these ports only when `--faults` asks
//! for a fault phase, so every other run's node path is untouched.
//!
//! A port can be armed for its next `submitblock`:
//!
//! - [`Arm::DelayForward`] holds the call before forwarding it, so the offer
//!   stays in flight for as long as a SIGTERM drain needs to see it;
//! - [`Arm::WithholdReply`] forwards the call at once, so the node has the
//!   block, and never returns the node's answer: the offering frontend waits
//!   on a reply that is not coming, the shape #474 C names.
//!
//! Either way the arm reports the call when it arrives.

use anyhow::{Context, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use qbit_prism_server::codec::{double_sha256, hash_display};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::oneshot, task::JoinHandle};

/// What the next `submitblock` on an armed port does.
#[derive(Clone, Copy, Debug)]
pub enum Arm {
    DelayForward(Duration),
    WithholdReply,
}

/// An armed call, as it arrived.
#[derive(Clone, Debug)]
pub struct Seen {
    pub frontend: usize,
    pub block_hash: String,
    pub at: Instant,
}

/// One `submitblock` through the relay.
#[derive(Clone, Debug)]
pub struct RelaySubmit {
    pub frontend: usize,
    pub block_hash: String,
    pub at: Instant,
    /// When the call was sent on to the node; `None` if it never was.
    pub forwarded_at: Option<Instant>,
    /// The node's JSON-RPC `result`, verbatim, once it answered.
    pub result: Option<Value>,
    pub error: Option<String>,
    /// The arm this call met, if any.
    pub armed: Option<&'static str>,
}

struct Armed {
    arm: Arm,
    seen: oneshot::Sender<Seen>,
}

struct Shared {
    upstream: String,
    client: reqwest::Client,
    arms: Vec<Mutex<Option<Armed>>>,
    submits: Mutex<Vec<RelaySubmit>>,
    /// Set when the relay is dropped, so no withheld reply outlives it.
    closed: tokio::sync::watch::Sender<bool>,
}

pub struct RpcFaultRelay {
    shared: Arc<Shared>,
    urls: Vec<String>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for RpcFaultRelay {
    fn drop(&mut self) {
        self.shared.closed.send_replace(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl RpcFaultRelay {
    /// One port per frontend, each forwarding to `upstream` (the node URL
    /// the frontends would otherwise have been given).
    pub async fn open(upstream: &str, frontends: usize) -> Result<Self> {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(8)
            .build()
            .context("building the fault relay's HTTP client")?;
        let (closed, _) = tokio::sync::watch::channel(false);
        let shared = Arc::new(Shared {
            upstream: upstream.to_owned(),
            client,
            arms: (0..frontends).map(|_| Mutex::new(None)).collect(),
            submits: Mutex::new(Vec::new()),
            closed,
        });
        let mut urls = Vec::with_capacity(frontends);
        let mut tasks = Vec::with_capacity(frontends);
        for index in 0..frontends {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .context("binding a fault relay port")?;
            let address = listener.local_addr()?;
            urls.push(rebase_url(upstream, &address.to_string())?);
            let app = Router::new()
                .route("/", post(relay))
                .route("/{*path}", post(relay))
                .with_state((shared.clone(), index));
            tasks.push(tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            }));
        }
        Ok(Self {
            shared,
            urls,
            tasks,
        })
    }

    /// The node URL frontend `index` is launched with.
    pub fn url(&self, index: usize) -> &str {
        &self.urls[index]
    }

    /// Arm frontend `index`'s port for its next `submitblock`. Re-arming
    /// replaces an arm that has not fired.
    pub fn arm(&self, index: usize, arm: Arm) -> oneshot::Receiver<Seen> {
        let (seen, receiver) = oneshot::channel();
        *self.shared.arms[index].lock().expect("relay arm lock") = Some(Armed { arm, seen });
        receiver
    }

    /// Disarm every port, so a fault that ended without its call leaves no
    /// arm for the next one to trip over.
    pub fn disarm_all(&self) {
        for arm in &self.shared.arms {
            *arm.lock().expect("relay arm lock") = None;
        }
    }

    pub fn submits(&self) -> Vec<RelaySubmit> {
        self.shared
            .submits
            .lock()
            .expect("relay submits lock")
            .clone()
    }
}

/// `upstream` with its authority replaced by `authority`, keeping the
/// scheme, any credentials and the path.
fn rebase_url(upstream: &str, authority: &str) -> Result<String> {
    let (scheme, rest) = upstream
        .split_once("://")
        .with_context(|| format!("node URL {upstream:?} has no scheme"))?;
    let (host_part, path) = match rest.find('/') {
        Some(slash) => rest.split_at(slash),
        None => (rest, "/"),
    };
    let credentials = host_part
        .rsplit_once('@')
        .map(|(credentials, _)| format!("{credentials}@"))
        .unwrap_or_default();
    Ok(format!("{scheme}://{credentials}{authority}{path}"))
}

/// The display hash of a serialized block: the double SHA-256 of its
/// 80-byte header, byte-reversed as the node prints it.
pub fn block_hash_of(block_hex: &str) -> Option<String> {
    let header = hex::decode(block_hex.get(..160)?).ok()?;
    Some(hash_display(&double_sha256(&header)))
}

/// Each call is carried by its own task, so a frontend that gives up on a
/// call (its RPC deadline, a SIGKILL) does not cancel it: the node still
/// gets what was sent, as a real node would, and the record is complete.
async fn relay(
    State((shared, index)): State<(Arc<Shared>, usize)>,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (reply, answer) = oneshot::channel();
    tokio::spawn(async move {
        let _ = reply.send(carry(shared, index, uri, headers, body).await);
    });
    answer
        .await
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

async fn carry(
    shared: Arc<Shared>,
    index: usize,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request: Option<Value> = serde_json::from_slice(&body).ok();
    let submitted = request
        .as_ref()
        .filter(|request| request["method"] == "submitblock")
        .and_then(|request| request["params"][0].as_str())
        .map(|block| block_hash_of(block).unwrap_or_else(|| "unreadable".into()));
    let mut armed = None;
    let mut record_index = None;
    if let Some(block_hash) = &submitted {
        armed = shared.arms[index].lock().expect("relay arm lock").take();
        let at = Instant::now();
        let mut submits = shared.submits.lock().expect("relay submits lock");
        record_index = Some(submits.len());
        submits.push(RelaySubmit {
            frontend: index,
            block_hash: block_hash.clone(),
            at,
            forwarded_at: None,
            result: None,
            error: None,
            armed: armed.as_ref().map(|armed| match armed.arm {
                Arm::DelayForward(_) => "delay-forward",
                Arm::WithholdReply => "withhold-reply",
            }),
        });
        drop(submits);
        if let Some(armed) = armed.as_mut() {
            let (placeholder, _) = oneshot::channel();
            let seen = std::mem::replace(&mut armed.seen, placeholder);
            let _ = seen.send(Seen {
                frontend: index,
                block_hash: block_hash.clone(),
                at,
            });
        }
    }
    let arm = armed.map(|armed| armed.arm);
    let mut closed = shared.closed.subscribe();
    if let Some(Arm::DelayForward(delay)) = arm {
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = closed.wait_for(|closed| *closed) => {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        }
    }
    let mut target = shared.upstream.trim_end_matches('/').to_owned();
    target.push_str(uri.path());
    let mut forward = shared.client.post(&target).body(body.clone());
    for (name, value) in &headers {
        if name != axum::http::header::HOST && name != axum::http::header::CONTENT_LENGTH {
            forward = forward.header(name, value);
        }
    }
    if let Some(record) = record_index {
        shared.submits.lock().expect("relay submits lock")[record].forwarded_at =
            Some(Instant::now());
    }
    let answer = forward.send().await;
    let (status, reply) = match answer {
        Ok(response) => {
            let status = response.status();
            match response.bytes().await {
                Ok(bytes) => (status, bytes),
                Err(error) => {
                    note_error(
                        &shared,
                        record_index,
                        format!("reading the node's reply: {error}"),
                    );
                    return StatusCode::BAD_GATEWAY.into_response();
                }
            }
        }
        Err(error) => {
            note_error(
                &shared,
                record_index,
                format!("forwarding to the node: {error}"),
            );
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };
    if let Some(record) = record_index {
        let parsed: Option<Value> = serde_json::from_slice(&reply).ok();
        shared.submits.lock().expect("relay submits lock")[record].result = Some(
            parsed
                .map(|value| value["result"].clone())
                .unwrap_or_else(|| json!({"unparseable": String::from_utf8_lossy(&reply)})),
        );
    }
    if let Some(Arm::WithholdReply) = arm {
        // The node has the block; its answer never reaches the frontend.
        let _ = closed.wait_for(|closed| *closed).await;
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    (
        status,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        reply,
    )
        .into_response()
}

fn note_error(shared: &Shared, record: Option<usize>, error: String) {
    if let Some(record) = record {
        shared.submits.lock().expect("relay submits lock")[record].error = Some(error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_relay_url_keeps_the_scheme_credentials_and_path() {
        assert_eq!(
            rebase_url("http://127.0.0.1:18443/", "127.0.0.1:9").unwrap(),
            "http://127.0.0.1:9/"
        );
        assert_eq!(
            rebase_url("http://user:pw@node:1/wallet/x", "127.0.0.1:9").unwrap(),
            "http://user:pw@127.0.0.1:9/wallet/x"
        );
        assert_eq!(
            rebase_url("http://node:1", "127.0.0.1:9").unwrap(),
            "http://127.0.0.1:9/"
        );
        assert!(rebase_url("node:1", "127.0.0.1:9").is_err());
    }

    #[test]
    fn a_block_hash_is_the_reversed_double_sha_of_its_header() {
        // The genesis header of Bitcoin's main chain, whose hash is known.
        let header = "0100000000000000000000000000000000000000000000000000000000000000\
                      000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa\
                      4b1e5e4a29ab5f49ffff001d1dac2b7c";
        assert_eq!(
            block_hash_of(&format!("{header}01")).unwrap(),
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
        assert_eq!(block_hash_of("00"), None);
    }
}
