//! A JSON-RPC relay between one frontend and its `qbitd`, which S8 uses to
//! find a block at the instant its node dies.
//!
//! It forwards every call byte for byte. A `submitblock` is what it can
//! interfere with:
//!
//! - [`GateMode::Hold`]: the call is held before it reaches the node. Killing
//!   the frontend meanwhile loses the block before the chain ever saw it.
//! - [`GateMode::Withhold`]: the call reaches the node at once, and the
//!   node's answer is held. Killing the frontend meanwhile leaves a block on
//!   the chain that its finder never recorded as landed.
//!
//! Every `submitblock` is recorded with the block hash it carried, and
//! [`RpcGate::release`] lets held calls and answers go. Adapted from the
//! `NodeGate` of `crates/qbit-prism-server/tests/support/live_pg_failover.rs`.

use anyhow::{Context, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use qbit_prism_server::codec::{double_sha256, hash_display};
use serde::Serialize;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GateMode {
    /// Forward everything.
    Pass,
    /// Hold every `submitblock` before the node.
    Hold,
    /// Forward every `submitblock` and hold the node's answer.
    Withhold,
}

struct GateState {
    node: String,
    client: reqwest::Client,
    mode: watch::Sender<GateMode>,
    /// Bumped by `release`: held calls and answers go.
    released: watch::Sender<u64>,
    /// Set by `discard`: held calls end without reaching the node.
    discard: std::sync::atomic::AtomicBool,
    /// The block hash of every `submitblock`, in arrival order.
    arrivals: watch::Sender<Vec<String>>,
    /// The blocks the node answered, in answer order.
    answered: watch::Sender<Vec<String>>,
}

pub struct RpcGate {
    port: u16,
    state: Arc<GateState>,
    task: JoinHandle<()>,
}

impl RpcGate {
    /// A pass-through gate in front of the node's RPC on `node_port`.
    pub async fn open(node_port: u16) -> Result<Self> {
        let state = Arc::new(GateState {
            node: format!("http://127.0.0.1:{node_port}/"),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .build()?,
            mode: watch::channel(GateMode::Pass).0,
            released: watch::channel(0).0,
            discard: std::sync::atomic::AtomicBool::new(false),
            arrivals: watch::channel(Vec::new()).0,
            answered: watch::channel(Vec::new()).0,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding the RPC gate")?;
        let port = listener.local_addr()?.port();
        let app = axum::Router::new()
            .fallback(relay)
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { port, state, task })
    }

    /// The port the frontend's `QBIT_RPC_PORT` names.
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn set_mode(&self, mode: GateMode) {
        if mode == GateMode::Hold {
            self.state
                .discard
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
        self.state.mode.send_replace(mode);
    }

    /// Let every held call and answer go, and pass from now on.
    pub fn release(&self) {
        self.state.mode.send_replace(GateMode::Pass);
        self.state
            .released
            .send_modify(|generation| *generation += 1);
    }

    /// End every held call without forwarding it: what the node would see
    /// of a frontend killed with the call in its buffers. Passes from now on.
    pub fn discard_held(&self) {
        self.state
            .discard
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.release();
    }

    /// Wait for the first `submitblock` after `seen` arrivals; its block hash.
    pub async fn next_submission(&self, seen: usize, limit: Duration) -> Result<String> {
        let mut arrivals = self.state.arrivals.subscribe();
        let found = tokio::time::timeout(limit, arrivals.wait_for(|hashes| hashes.len() > seen))
            .await
            .context("no submitblock reached the gate")??;
        Ok(found[seen].clone())
    }

    /// Wait until the node has answered the `submitblock` of `block`.
    pub async fn node_answered(&self, block: &str, limit: Duration) -> Result<()> {
        let mut answered = self.state.answered.subscribe();
        tokio::time::timeout(
            limit,
            answered.wait_for(|hashes| hashes.iter().any(|hash| hash == block)),
        )
        .await
        .with_context(|| format!("the node never answered the submitblock of {block}"))??;
        Ok(())
    }

    pub fn submissions(&self) -> usize {
        self.state.arrivals.borrow().len()
    }
}

impl Drop for RpcGate {
    fn drop(&mut self) {
        self.release();
        self.task.abort();
    }
}

/// The block hash a `submitblock` call carries, if it is one.
fn submitted_block(request: &Value) -> Option<String> {
    (request["method"] == "submitblock").then(|| {
        request["params"][0]
            .as_str()
            .and_then(|block| hex::decode(block).ok())
            .filter(|block| block.len() >= 80)
            .map_or_else(
                || "<undecodable block>".to_owned(),
                |block| hash_display(&double_sha256(&block[..80])),
            )
    })
}

async fn wait_release(state: &GateState, generation: u64) {
    let mut released = state.released.subscribe();
    let _ = released.wait_for(|now| *now > generation).await;
}

async fn relay(State(gate): State<Arc<GateState>>, headers: HeaderMap, body: Bytes) -> Response {
    let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let submitted = submitted_block(&request);
    let generation = *gate.released.borrow();
    let mode = *gate.mode.borrow();
    if let Some(hash) = &submitted {
        gate.arrivals
            .send_modify(|hashes| hashes.push(hash.clone()));
        if mode == GateMode::Hold {
            wait_release(&gate, generation).await;
            if gate.discard.load(std::sync::atomic::Ordering::SeqCst) {
                return StatusCode::BAD_GATEWAY.into_response();
            }
        }
    }
    let mut upstream = gate
        .client
        .post(&gate.node)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body);
    if let Some(authorization) = headers.get(header::AUTHORIZATION) {
        upstream = upstream.header(header::AUTHORIZATION, authorization.clone());
    }
    let answer = async {
        let response = upstream.send().await?;
        let status = response.status();
        Ok::<_, reqwest::Error>((status, response.bytes().await?))
    }
    .await;
    if let Some(hash) = submitted {
        gate.answered.send_modify(|hashes| hashes.push(hash));
        if mode == GateMode::Withhold {
            wait_release(&gate, generation).await;
        }
    }
    match answer {
        Ok((status, bytes)) => (
            StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
            [(header::CONTENT_TYPE, "application/json")],
            bytes,
        )
            .into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_submitblock_names_its_block_and_other_calls_name_none() {
        let header = vec![0u8; 80];
        let request = json!({"method": "submitblock", "params": [hex::encode(&header)]});
        assert_eq!(
            submitted_block(&request),
            Some(hash_display(&double_sha256(&header)))
        );
        assert_eq!(submitted_block(&json!({"method": "getblockcount"})), None);
        assert_eq!(
            submitted_block(&json!({"method": "submitblock", "params": ["zz"]})),
            Some("<undecodable block>".into())
        );
    }
}
