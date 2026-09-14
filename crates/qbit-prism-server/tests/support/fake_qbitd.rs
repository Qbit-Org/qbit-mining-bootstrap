//! In-process fake qbit node for the JSONB ceiling gate.
//!
//! Copied from the file-private node in `tests/readiness_rpc.rs` and reduced to
//! the calls `Coordinator::new` and `Coordinator::refresh_once` actually make.
//! No gate phase submits a block, so there is no `submitblock` arm and the gate
//! never needs `QBITD_BIN`.

use anyhow::Result;
use axum::{extract::State, routing::post, Json, Router};
use qbit_prism_server::config::Config;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

/// The stock template bits from `tests/readiness_rpc.rs`. `codec` maps them to
/// a network difficulty of 1,000,000, which is what sizes the payout window.
pub const TEMPLATE_BITS: &str = "207fffff";

pub struct FakeNode {
    pub url: String,
    task: JoinHandle<()>,
}

impl Drop for FakeNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct NodeState {
    tip: String,
    tip_parent: String,
}

impl FakeNode {
    pub async fn open() -> Result<Self> {
        let state = Arc::new(NodeState {
            tip: "ab".repeat(32),
            tip_parent: "cd".repeat(32),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new().route("/", post(answer)).with_state(state);
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { url, task })
    }
}

async fn answer(State(state): State<Arc<NodeState>>, Json(request): Json<Value>) -> Json<Value> {
    // `curtime` is generated per call so a long fixture load cannot age the
    // template past `template_max_age` before the refresh phase runs.
    let now = chrono::Utc::now().timestamp();
    let result = match request["method"].as_str().unwrap_or("") {
        "getblockchaininfo" => json!({
            "chain":"test","initialblockdownload":false,"blocks":100,"headers":100,
            "bestblockhash":state.tip,"chainwork":"01"
        }),
        "getnetworkinfo" => json!({"connections": 2}),
        "getblocktemplate" => json!({
            "height":101,"coinbasevalue":5_000_000_000u64,"previousblockhash":state.tip,
            "version":0x20000000u32,"bits":TEMPLATE_BITS,"curtime":now,"mintime":now-1,
            "transactions":[]
        }),
        "estimatesmartfee" => json!({"feerate":"0.00001"}),
        "getmempoolinfo" => json!({"minrelaytxfee":"0.00001","mempoolminfee":"0.00001"}),
        "getbestblockhash" => json!(state.tip),
        "getblockhash" if request["params"][0] == 0 => json!("00".repeat(32)),
        "getblockhash" => json!(state.tip),
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
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

/// Copied from `coordinator_config` in `tests/readiness_rpc.rs`.
pub fn coordinator_config(
    database_url: String,
    node: &FakeNode,
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
        rpc_url: node.url.clone(),
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
