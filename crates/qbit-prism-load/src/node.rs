//! In-process fake qbit node for the load harness.
//!
//! Started from `crates/qbit-prism-server/tests/support/fake_qbitd.rs` and
//! extended with everything a multi-block, multi-frontend load run needs: a
//! real height to hash map, a hash to parent map, strictly increasing
//! chainwork, `submitblock`, `waitfornewblock`, and an in-process way to mint
//! an external tip.

use anyhow::{Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use chrono::{DateTime, Utc};
use qbit_prism_server::codec;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Notify, task::JoinHandle};

/// The genesis hash the frontends pin at startup (`getblockhash [0]`).
pub const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";
/// Height the synthetic chain starts at, so `getblockhash` has real history.
pub const START_HEIGHT: u64 = 100;
/// Rejection string a real node returns for a block that does not extend the
/// current tip. A string result is a recorded rejection for the coordinator.
pub const PARENT_MISMATCH: &str = "prev-blk-not-found";

/// How a tip came to exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TipOrigin {
    /// Pre-existing synthetic history.
    Bootstrap,
    /// Accepted through `submitblock`, i.e. a pool block.
    Pool,
    /// Minted in process, i.e. somebody else's block.
    External,
}

/// A tip transition, stamped on both clocks.
#[derive(Clone, Debug)]
pub struct TipChange {
    pub hash: String,
    pub height: u64,
    pub origin: TipOrigin,
    pub monotonic: Instant,
    pub wall: DateTime<Utc>,
}

/// One `submitblock` call.
#[derive(Clone, Debug, Serialize)]
pub struct SubmissionRecord {
    pub block_hash: String,
    pub parent: String,
    pub height: u64,
    pub accepted: bool,
    pub rejection: Option<String>,
    pub received_at: DateTime<Utc>,
    pub block_bytes: usize,
}

struct Chain {
    tip: String,
    height: u64,
    heights: Vec<String>,
    parents: HashMap<String, String>,
    chainwork: u128,
    changes: Vec<TipChange>,
}

pub struct NodeState {
    chain: std::sync::Mutex<Chain>,
    submissions: std::sync::Mutex<Vec<SubmissionRecord>>,
    tip_notify: Notify,
    bits: String,
    /// Serve [`retarget_bits`] instead of `bits` for every template.
    retarget: bool,
    address_prefix: String,
    rpc_calls: std::sync::Mutex<HashMap<String, u64>>,
}

/// Steps of the per-block walk, each about 0.8 percent of the mantissa.
const RETARGET_STEPS: u32 = 8;
const RETARGET_STEP_DIVISOR: u32 = 128;

/// The template bits a retargeting node serves for `height`: the base
/// mantissa lowered by a triangle wave of period 16 heights, so consecutive
/// heights always differ, the difficulty walks up for eight blocks and back
/// down for eight, and the whole walk stays within about 6.7 percent above
/// the base difficulty. Per-block retargets in production are small and in
/// both directions; what matters to the refresh path is that the scaled
/// network difficulty differs on every tip, and this guarantees it.
pub fn retarget_bits(base: &str, height: u64) -> Result<String> {
    let bits = codec::parse_u32_hex(base)?;
    let exponent = bits >> 24;
    let mantissa = bits & 0x00ff_ffff;
    let phase = u32::try_from(height % u64::from(2 * RETARGET_STEPS))?;
    let step = if phase <= RETARGET_STEPS {
        phase
    } else {
        2 * RETARGET_STEPS - phase
    };
    let lowered = mantissa - step * (mantissa / RETARGET_STEP_DIVISOR);
    anyhow::ensure!(
        (0x8000..=0x007f_ffff).contains(&lowered),
        "retarget mantissa {lowered:#x} leaves the compact encoding"
    );
    Ok(format!("{:08x}", (exponent << 24) | lowered))
}

fn synthetic_hash(seed: &str) -> String {
    hex::encode(Sha256::digest(seed.as_bytes()))
}

impl NodeState {
    pub fn new(bits: &str, address_prefix: &str) -> Self {
        Self::with_retarget(bits, address_prefix, false)
    }

    pub fn with_retarget(bits: &str, address_prefix: &str, retarget: bool) -> Self {
        let mut heights = vec![GENESIS.to_owned()];
        let mut parents = HashMap::new();
        let mut previous = GENESIS.to_owned();
        for height in 1..=START_HEIGHT {
            let hash = synthetic_hash(&format!("prism-load-bootstrap-{height}"));
            parents.insert(hash.clone(), previous.clone());
            heights.push(hash.clone());
            previous = hash;
        }
        let tip = heights[START_HEIGHT as usize].clone();
        let changes = vec![TipChange {
            hash: tip.clone(),
            height: START_HEIGHT,
            origin: TipOrigin::Bootstrap,
            monotonic: Instant::now(),
            wall: Utc::now(),
        }];
        Self {
            chain: std::sync::Mutex::new(Chain {
                tip,
                height: START_HEIGHT,
                heights,
                parents,
                // Chainwork must be positive and strictly increasing for the
                // whole run: `observe_chain_view` refuses less work and refuses
                // equal work at a different tip.
                chainwork: u128::from(START_HEIGHT) + 1,
                changes,
            }),
            submissions: std::sync::Mutex::new(Vec::new()),
            tip_notify: Notify::new(),
            bits: bits.to_owned(),
            retarget,
            address_prefix: address_prefix.to_owned(),
            rpc_calls: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The bits `getblocktemplate` serves for a template at `height`.
    pub fn template_bits(&self, height: u64) -> String {
        if self.retarget {
            retarget_bits(&self.bits, height).expect("base bits validated at start")
        } else {
            self.bits.clone()
        }
    }

    pub fn retargets(&self) -> bool {
        self.retarget
    }

    pub fn tip(&self) -> (String, u64) {
        let chain = self.chain.lock().expect("node chain lock");
        (chain.tip.clone(), chain.height)
    }

    pub fn chainwork_hex(&self) -> String {
        format!(
            "{:x}",
            self.chain.lock().expect("node chain lock").chainwork
        )
    }

    pub fn tip_changes(&self) -> Vec<TipChange> {
        self.chain.lock().expect("node chain lock").changes.clone()
    }

    pub fn submissions(&self) -> Vec<SubmissionRecord> {
        self.submissions
            .lock()
            .expect("node submission lock")
            .clone()
    }

    pub fn rpc_call_counts(&self) -> HashMap<String, u64> {
        self.rpc_calls.lock().expect("node rpc lock").clone()
    }

    /// The payout script the frontends see for one of this run's addresses.
    /// P2MR is `5220` followed by a 32-byte program (`coordinator.rs`,
    /// `authorize`).
    pub fn payout_script_hex(address: &str) -> String {
        format!("5220{}", hex::encode(Sha256::digest(address.as_bytes())))
    }

    fn advance(&self, hash: String, origin: TipOrigin) -> TipChange {
        let change = {
            let mut chain = self.chain.lock().expect("node chain lock");
            let parent = chain.tip.clone();
            chain.height += 1;
            chain.chainwork += 1;
            chain.parents.insert(hash.clone(), parent);
            chain.heights.push(hash.clone());
            chain.tip = hash.clone();
            let change = TipChange {
                hash,
                height: chain.height,
                origin,
                monotonic: Instant::now(),
                wall: Utc::now(),
            };
            chain.changes.push(change.clone());
            change
        };
        self.tip_notify.notify_waiters();
        change
    }

    /// Mint a tip that is not a pool block, so time-to-usable-work can be
    /// measured without landing anything of the pool's own.
    pub fn mint_external_block(&self) -> TipChange {
        let seed = {
            let chain = self.chain.lock().expect("node chain lock");
            format!("prism-load-external-{}-{}", chain.height + 1, chain.tip)
        };
        self.advance(synthetic_hash(&seed), TipOrigin::External)
    }

    fn submit_block(&self, block_hex: &str) -> Value {
        let Ok(block) = hex::decode(block_hex) else {
            return json!("bad-block-hex");
        };
        if block.len() < 80 {
            return json!("bad-block-length");
        }
        let header: [u8; 32] = codec::double_sha256(&block[..80]);
        let hash = codec::hash_display(&header);
        let mut parent = block[4..36].to_vec();
        parent.reverse();
        let parent = hex::encode(parent);
        let (tip, height) = self.tip();
        let accepted = parent == tip;
        let record = SubmissionRecord {
            block_hash: hash.clone(),
            parent,
            height: height + 1,
            accepted,
            rejection: (!accepted).then(|| PARENT_MISMATCH.to_owned()),
            received_at: Utc::now(),
            block_bytes: block.len(),
        };
        self.submissions
            .lock()
            .expect("node submission lock")
            .push(record);
        if !accepted {
            return json!(PARENT_MISMATCH);
        }
        self.advance(hash, TipOrigin::Pool);
        Value::Null
    }

    /// Long-poll until the tip changes or the timeout expires.
    async fn wait_for_new_block(&self, timeout_ms: u64) -> Value {
        let (before, _) = self.tip();
        // `Notified` only enqueues when first polled, so enable it before the
        // second tip read: otherwise a tip change in between is missed and the
        // caller waits out the whole timeout.
        let waiter = self.tip_notify.notified();
        tokio::pin!(waiter);
        waiter.as_mut().enable();
        let (after, height) = self.tip();
        if after != before {
            return json!({"hash": after, "height": height});
        }
        let _ = tokio::time::timeout(Duration::from_millis(timeout_ms.min(120_000)), waiter).await;
        let (hash, height) = self.tip();
        json!({"hash": hash, "height": height})
    }

    /// Answer one JSON-RPC request. The id is echoed exactly, `result` is
    /// always present, and an unknown method is answered with -32601, as
    /// `crates/qbit-prism-server/src/rpc.rs` requires.
    pub async fn handle(&self, request: &Value) -> Value {
        let method = request["method"].as_str().unwrap_or("").to_owned();
        *self
            .rpc_calls
            .lock()
            .expect("node rpc lock")
            .entry(method.clone())
            .or_insert(0) += 1;
        let params = &request["params"];
        // `curtime` is generated per call so a long phase cannot age the
        // template past `PRISM_TEMPLATE_MAX_AGE_SECONDS`.
        let now = Utc::now().timestamp();
        let (tip, height) = self.tip();
        let result = match method.as_str() {
            "getblockchaininfo" => json!({
                "chain": "test",
                "initialblockdownload": false,
                "blocks": height,
                "headers": height,
                "bestblockhash": tip,
                "chainwork": self.chainwork_hex(),
            }),
            "getnetworkinfo" => json!({ "connections": 2 }),
            "getbestblockhash" => json!(tip),
            "getblocktemplate" => json!({
                "height": height + 1,
                "coinbasevalue": 5_000_000_000u64,
                "previousblockhash": tip,
                "version": 0x2000_0000u32,
                "bits": self.template_bits(height + 1),
                "curtime": now,
                "mintime": now - 1,
                "transactions": [],
            }),
            "getblockhash" => {
                let requested = params[0].as_u64();
                let chain = self.chain.lock().expect("node chain lock");
                match requested.and_then(|h| chain.heights.get(h as usize)) {
                    Some(hash) => json!(hash),
                    None => {
                        return json!({
                            "id": request["id"], "result": Value::Null,
                            "error": {"code": -8, "message": "Block height out of range"}
                        })
                    }
                }
            }
            "getblockheader" => {
                let requested = params[0].as_str().unwrap_or_default();
                let chain = self.chain.lock().expect("node chain lock");
                match chain.parents.get(requested) {
                    Some(parent) => json!({"previousblockhash": parent}),
                    // Genesis has no parent; the coordinator reads the field
                    // with `unwrap_or_default`.
                    None => json!({}),
                }
            }
            "validateaddress" => {
                let address = params[0].as_str().unwrap_or_default();
                if address.starts_with(&self.address_prefix) {
                    json!({"isvalid": true, "scriptPubKey": Self::payout_script_hex(address)})
                } else {
                    json!({ "isvalid": false })
                }
            }
            "submitblock" => self.submit_block(params[0].as_str().unwrap_or_default()),
            "waitfornewblock" => {
                self.wait_for_new_block(params[0].as_u64().unwrap_or(5_000))
                    .await
            }
            // CTV is off for every load run, so these are never consulted; they
            // are served anyway so an accidental enable does not look like a
            // node outage.
            "estimatesmartfee" => json!({"feerate": "0.00001"}),
            "getmempoolinfo" => json!({"minrelaytxfee": "0.00001", "mempoolminfee": "0.00001"}),
            _ => {
                return json!({
                    "id": request["id"], "result": Value::Null,
                    "error": {"code": -32601, "message": "unexpected RPC"}
                })
            }
        };
        json!({"id": request["id"], "result": result, "error": Value::Null})
    }
}

/// A running fake node.
pub struct FakeNode {
    pub url: String,
    pub state: Arc<NodeState>,
    task: JoinHandle<()>,
}

impl Drop for FakeNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeNode {
    pub async fn open(bits: &str, address_prefix: &str) -> Result<Self> {
        Self::open_with_retarget(bits, address_prefix, false).await
    }

    /// [`FakeNode::open`], serving per-height [`retarget_bits`] when asked.
    /// The base bits are checked against the walk before anything listens.
    pub async fn open_with_retarget(
        bits: &str,
        address_prefix: &str,
        retarget: bool,
    ) -> Result<Self> {
        if retarget {
            for height in 0..2 * u64::from(RETARGET_STEPS) {
                retarget_bits(bits, height)?;
            }
        }
        let state = Arc::new(NodeState::with_retarget(bits, address_prefix, retarget));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind fake node listener")?;
        let url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(answer))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { url, state, task })
    }
}

async fn answer(State(state): State<Arc<NodeState>>, Json(request): Json<Value>) -> Json<Value> {
    Json(state.handle(&request).await)
}
