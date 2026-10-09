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
//! - [`Arm::Hold`] holds the call until [`RpcFaultRelay::release_held`], so
//!   a fault can fail the database over while a found block is mid-landing
//!   and then let the call reach the node.
//!
//! Either way the arm reports the call when it arrives.
//!
//! The relay can also refuse every `submitblock` with qbitd's warmup error
//! ([`RpcFaultRelay::set_refusing`]), which the server knows was never run
//! (#526): the candidate goes back to `pending` and is retried, so a fault
//! can build a backlog of found blocks.

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
    /// Hold the call until released, then forward it.
    Hold,
}

/// qbitd's JSON-RPC error while it is still starting (`RPC_IN_WARMUP`).
pub const WARMUP_CODE: i64 = -28;

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
    /// The node's JSON-RPC `error`, when it was not null.
    pub rpc_error: Option<Value>,
    pub error: Option<String>,
    /// The arm this call met, if any.
    pub armed: Option<&'static str>,
}

struct Armed {
    arm: Arm,
    seen: oneshot::Sender<Seen>,
}

struct Shared {
    /// The node URL's scheme, credentials and authority, without its path:
    /// a call keeps the path it arrived on, which is already the node's.
    upstream_origin: String,
    client: reqwest::Client,
    arms: Vec<Mutex<Option<Armed>>>,
    submits: Mutex<Vec<RelaySubmit>>,
    /// Set when the relay is dropped, so no withheld reply outlives it.
    closed: tokio::sync::watch::Sender<bool>,
    /// Bumped by `release_held`, which lets every held call go on.
    released: tokio::sync::watch::Sender<u64>,
    /// Every `submitblock` is answered with the warmup error, unforwarded.
    refusing: std::sync::atomic::AtomicBool,
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
        // Idle upstream connections are dropped before qbitd's idle close, or a
        // forwarded call written into it fails as no fault plan said (#759).
        let client = qbit_prism_server::rpc::node_client_builder()
            .pool_max_idle_per_host(8)
            .build()
            .context("building the fault relay's HTTP client")?;
        let (closed, _) = tokio::sync::watch::channel(false);
        let (released, _) = tokio::sync::watch::channel(0);
        let shared = Arc::new(Shared {
            upstream_origin: origin_of(upstream)?,
            client,
            arms: (0..frontends).map(|_| Mutex::new(None)).collect(),
            submits: Mutex::new(Vec::new()),
            closed,
            released,
            refusing: std::sync::atomic::AtomicBool::new(false),
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

    /// Let every call an [`Arm::Hold`] is holding go on to the node.
    pub fn release_held(&self) {
        self.shared
            .released
            .send_modify(|generation| *generation += 1);
    }

    /// Answer every `submitblock` with qbitd's warmup error, or stop.
    pub fn set_refusing(&self, refusing: bool) {
        self.shared
            .refusing
            .store(refusing, std::sync::atomic::Ordering::SeqCst);
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

/// `upstream` without its path: scheme, credentials and authority.
fn origin_of(upstream: &str) -> Result<String> {
    let (scheme, rest) = upstream
        .split_once("://")
        .with_context(|| format!("node URL {upstream:?} has no scheme"))?;
    let authority = rest.split('/').next().unwrap_or(rest);
    Ok(format!("{scheme}://{authority}"))
}

impl RelaySubmit {
    /// The node answered and accepted the block: `submitblock`'s `result` is
    /// null on acceptance and a reason string otherwise, and an RPC error
    /// also carries a null `result`.
    pub fn node_accepted(&self) -> bool {
        self.result.as_ref().is_some_and(Value::is_null) && self.rpc_error.is_none()
    }
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
    // Subscribed before the arm can report the call, so a release that
    // follows the report at once is never missed.
    let mut released = shared.released.subscribe();
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
            rpc_error: None,
            error: None,
            armed: armed.as_ref().map(|armed| match armed.arm {
                Arm::DelayForward(_) => "delay-forward",
                Arm::WithholdReply => "withhold-reply",
                Arm::Hold => "hold",
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
    if let Some(record) = record_index {
        if arm.is_none() && shared.refusing.load(std::sync::atomic::Ordering::SeqCst) {
            // Never forwarded: the node is warming up, as far as the
            // offering frontend can tell.
            let id = request
                .as_ref()
                .map(|request| request["id"].clone())
                .unwrap_or(Value::Null);
            let mut submits = shared.submits.lock().expect("relay submits lock");
            submits[record].armed = Some("refused-warmup");
            submits[record].rpc_error = Some(json!({"code": WARMUP_CODE}));
            drop(submits);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                json!({
                    "result": null,
                    "error": {"code": WARMUP_CODE, "message": "Loading block index…"},
                    "id": id,
                })
                .to_string(),
            )
                .into_response();
        }
    }
    let mut closed = shared.closed.subscribe();
    if let Some(Arm::Hold) = arm {
        tokio::select! {
            _ = released.changed() => {}
            _ = closed.wait_for(|closed| *closed) => {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        }
    }
    if let Some(Arm::DelayForward(delay)) = arm {
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = closed.wait_for(|closed| *closed) => {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        }
    }
    let target = format!(
        "{}{}",
        shared.upstream_origin,
        uri.path_and_query().map_or("/", |path| path.as_str())
    );
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
        let mut submits = shared.submits.lock().expect("relay submits lock");
        submits[record].rpc_error = parsed
            .as_ref()
            .map(|value| value["error"].clone())
            .filter(|error| !error.is_null());
        submits[record].result = Some(
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
        // A call arrives on the node's own path, so it is forwarded to the
        // node's origin, never to the full node URL with the path again.
        assert_eq!(
            origin_of("http://user:pw@node:1/wallet/x").unwrap(),
            "http://user:pw@node:1"
        );
        assert_eq!(origin_of("http://node:1").unwrap(), "http://node:1");
    }

    #[test]
    fn only_a_null_result_without_an_error_is_an_accepted_block() {
        let submit = |result: Option<Value>, rpc_error: Option<Value>| RelaySubmit {
            frontend: 0,
            block_hash: "00".into(),
            at: Instant::now(),
            forwarded_at: Some(Instant::now()),
            result,
            rpc_error,
            error: None,
            armed: None,
        };
        assert!(submit(Some(Value::Null), None).node_accepted());
        assert!(!submit(None, None).node_accepted(), "no answer yet");
        assert!(!submit(Some(json!("duplicate")), None).node_accepted());
        assert!(!submit(Some(Value::Null), Some(json!({"code": -1}))).node_accepted());
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
