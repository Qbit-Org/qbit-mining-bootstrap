//! In-process fake qbit node for the JSONB ceiling gate.
//!
//! Copied from the file-private node in `tests/readiness_rpc.rs` and reduced to
//! the calls `Coordinator::new` and `Coordinator::refresh_once` actually make.
//! No gate phase submits a block, so there is no `submitblock` arm and the gate
//! never needs `QBITD_BIN`.

use anyhow::{ensure, Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use qbit_prism_server::config::Config;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The stock template bits from `tests/readiness_rpc.rs`. `codec` maps them to
/// a network difficulty of 1,000,000, which is what sizes the payout window.
pub const TEMPLATE_BITS: &str = "207fffff";

pub struct FakeNode {
    pub url: String,
    task: JoinHandle<()>,
    state: Arc<Mutex<NodeState>>,
}

impl Drop for FakeNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct NodeState {
    tip: String,
    tip_parent: String,
    height: u64,
    chainwork: String,
    template: Option<Value>,
    replies: HashMap<String, Value>,
    pauses: HashMap<String, PauseRequest>,
    next_pause: u64,
    accept_blocks: bool,
    accepted: HashMap<u64, String>,
}

struct PauseRequest {
    #[allow(dead_code)]
    id: u64,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

/// Holds exactly one captured HTTP reply. Dropping the guard releases it and
/// removes an unused gate, so a cancelled test cannot poison a later request.
#[allow(dead_code)]
pub struct RpcPause {
    state: Weak<Mutex<NodeState>>,
    method: String,
    id: u64,
    entered: Option<oneshot::Receiver<()>>,
    release: Option<oneshot::Sender<()>>,
}

#[allow(dead_code)]
impl RpcPause {
    pub async fn entered(&mut self) -> Result<()> {
        self.entered
            .take()
            .context("pause already observed")?
            .await
            .context("node stopped before paused request")
    }

    pub fn release(mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for RpcPause {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let mut state = state.lock().expect("fake node state");
            if state
                .pauses
                .get(&self.method)
                .is_some_and(|pause| pause.id == self.id)
            {
                state.pauses.remove(&self.method);
            }
        }
    }
}

#[allow(dead_code)]
impl FakeNode {
    pub async fn open() -> Result<Self> {
        let state = Arc::new(Mutex::new(NodeState {
            tip: "ab".repeat(32),
            tip_parent: "cd".repeat(32),
            height: 100,
            chainwork: "01".into(),
            template: None,
            replies: HashMap::new(),
            pauses: HashMap::new(),
            next_pause: 0,
            accept_blocks: false,
            accepted: HashMap::new(),
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(answer))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { url, task, state })
    }

    pub fn set_tip(&self, tip: &str, parent: &str, height: u64, chainwork: &str) {
        let mut state = self.state.lock().expect("fake node state");
        state.tip = tip.into();
        state.tip_parent = parent.into();
        state.height = height;
        state.chainwork = chainwork.into();
    }

    /// Opt-in real offer-path fixture: null acceptance advances this fake chain.
    pub fn accept_blocks(&self) {
        self.state.lock().expect("fake node state").accept_blocks = true;
    }

    /// Exact response for deterministic template/large-transaction fixtures.
    /// None restores the default template with a fresh time on every request.
    pub fn set_template(&self, template: Option<Value>) {
        self.state.lock().expect("fake node state").template = template;
    }

    /// Override one method for a focused RPC fixture; pauses still apply.
    pub fn set_reply(&self, method: &str, params: Value, value: Value) {
        self.state
            .lock()
            .expect("fake node state")
            .replies
            .insert(format!("{method}:{params}"), value);
    }

    pub fn pause_next(&self, method: &str) -> Result<RpcPause> {
        let mut state = self.state.lock().expect("fake node state");
        ensure!(!state.pauses.contains_key(method), "method already paused");
        state.next_pause = state
            .next_pause
            .checked_add(1)
            .context("pause id overflow")?;
        let id = state.next_pause;
        let (entered, observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        state.pauses.insert(
            method.into(),
            PauseRequest {
                id,
                entered,
                release: released,
            },
        );
        Ok(RpcPause {
            state: Arc::downgrade(&self.state),
            method: method.into(),
            id,
            entered: Some(observed),
            release: Some(release),
        })
    }
}

async fn answer(
    State(state): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    // `curtime` is generated per call so a long fixture load cannot age the
    // template past `template_max_age` before the refresh phase runs.
    let now = chrono::Utc::now().timestamp();
    let method = request["method"].as_str().unwrap_or("");
    let (result, pause) = {
        let mut state = state.lock().expect("fake node state");
        let result = if let Some(reply) = state
            .replies
            .get(&format!("{method}:{}", request["params"]))
        {
            reply.clone()
        } else {
            match method {
        "getblockchaininfo" => json!({
            "chain":"test","initialblockdownload":false,"blocks":state.height,"headers":state.height,
            "bestblockhash":state.tip,"chainwork":state.chainwork
        }),
        "getnetworkinfo" => json!({"connections": 2}),
        "getblocktemplate" => state.template.clone().unwrap_or_else(|| json!({
            "height":state.height+1,"coinbasevalue":5_000_000_000u64,"previousblockhash":state.tip,
            "version":0x20000000u32,"bits":TEMPLATE_BITS,"curtime":now,"mintime":now-1,
            "transactions":[]
        })),
        "estimatesmartfee" => json!({"feerate":"0.00001"}),
        "getmempoolinfo" => json!({"minrelaytxfee":"0.00001","mempoolminfee":"0.00001"}),
        "getbestblockhash" => json!(state.tip),
        "getblockhash" if request["params"][0] == 0 => json!("00".repeat(32)),
        // Above the tip qbitd has no block, and neither does this fake
        // node: the same error `support/scripted_node.rs` answers, never the
        // tip. At or below the tip every height still answers the tip.
        "getblockhash"
            if request["params"][0]
                .as_u64()
                .is_none_or(|height| height > state.height) =>
        {
            return Json(json!({
                "id":request["id"],"result":null,
                "error":{"code":-8,"message":"Block height out of range"}
            }))
        }
        "getblockhash" => json!(state.accepted.get(&request["params"][0].as_u64().unwrap()).unwrap_or(&state.tip)),
        "submitblock" if state.accept_blocks => {
            let block = hex::decode(request["params"][0].as_str().unwrap()).unwrap();
            let hash = qbit_prism_server::codec::hash_display(&qbit_prism_server::codec::double_sha256(&block[..80]));
            state.tip_parent = state.tip.clone();
            state.tip = hash.clone();
            state.height += 1;
            state.chainwork = format!("{:x}", u64::from_str_radix(&state.chainwork, 16).unwrap() + 1);
            let height = state.height;
            state.accepted.insert(height, hash);
            Value::Null
        }
        "getblockheader" => json!({"previousblockhash": state.tip_parent}),
        "validateaddress" => {
            json!({"isvalid":true,"scriptPubKey":format!("5220{}","11".repeat(32))})
        }
        _ => {
            return Json(json!({
                "id":request["id"],"result":null,
                "error":{"code":-32601,"message":"unexpected RPC"}
            }))
        }
    }
        };
        (result, state.pauses.remove(method))
    };
    if let Some(pause) = pause {
        let _ = pause.entered.send(());
        let _ = pause.release.await;
    }
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

/// Copied from `coordinator_config` in `tests/readiness_rpc.rs`.
pub fn coordinator_config(
    database_url: String,
    node: &FakeNode,
    instance_id: &str,
) -> Result<Config> {
    coordinator_config_at(database_url, node.url.clone(), instance_id)
}

/// Same explicit test configuration for an actual node RPC endpoint.
/// This constructor never reads environment variables or starts a fake node.
pub fn coordinator_config_at(
    database_url: String,
    rpc_url: String,
    instance_id: &str,
) -> Result<Config> {
    Ok(Config {
        database_url,
        instance_id: instance_id.to_owned(),
        database_connections: 4,
        initialize_schema: true,
        chain: "testnet".into(),
        expected_genesis_hash: None,
        min_peers: 2,
        template_max_age: Duration::from_secs(120),
        submit_tip_max_age: Duration::from_secs(10),
        template_refresh_failure_exit: Duration::from_secs(120),
        rpc_url,
        rpc_user: "test".into(),
        rpc_password: "test".into(),
        rpc_timeout: Duration::from_secs(30),
        block_submit_timeout: Duration::from_secs(1),
        poll_interval: Duration::from_secs(1),
        blockwait: false,
        build_workers: 2,
        runtime_workers: 2,
        snapshot_interval: Duration::from_secs(60),
        health_timeout: Duration::from_secs(15),
        share_commit_timeout: Duration::from_secs(15),
        share_commit_grace: Duration::from_secs(5),
        block_only_ack_timeout: Duration::from_secs(60),
        candidate_orphan_confirmations: 6,
        capture_overpay_ceiling_bps: 100,
        extranonce2_size: 8,
        coinbase_tag: "/PRISM/".into(),
        manifest_seed: "11".repeat(32),
        ledger_seed: "22".repeat(32),
        ledger_public_key: qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"22".repeat(32))?
            .public_key_hex(),
        username_fallback: None,
        payout_policy: qbit_prism::PayoutPolicy::day_one_default(),
        fee_address: None,
        ctv_enabled: false,
        ctv_config: qbit_prism::SettlementModeConfig::default(),
        ctv_direct_floor: 10_485_760,
        ctv_fee: None,
        ctv_fee_premium_bps: 12000,
        ctv_broadcast: false,
        ctv_broadcast_interval: Duration::from_secs(10),
        version_mask: qbit_prism_server::codec::VERSION_ROLLING_MASK,
        audit_bind: "127.0.0.1".into(),
        audit_port: 0,
    })
}

/// The coinbase script-sig suffix `Coordinator::refresh_once` builds for the
/// shared prepared job, reproduced so the gate's bundle matches the one the
/// refresh phase persists.
pub fn coinbase_suffix(config: &Config) -> String {
    format!(
        "{}{}",
        hex::encode(&config.coinbase_tag),
        "00".repeat(4 + config.extranonce2_size)
    )
}
