//! A scripted, mutable fake qbit node for coordinator tests that offer blocks.
//!
//! `tests/offer_latency.rs` and `tests/lost_race_pin.rs` both drive a real
//! `Coordinator` through `submitblock` against a chain the test moves. This is
//! that one node: an active chain by height, each block's parent, every
//! `submitblock` arrival, and the tip history. Unlike `support/fake_qbitd.rs`,
//! whose static tip serves suites that never submit, this node accepts blocks
//! and reorganizes.
//!
//! It answers the way qbitd does where the coordinator can tell the
//! difference: `getblockhash` above the tip, or at a height the node holds no
//! block for, is RPC error -8 (never the tip, never null), and
//! `getblockheader` for a hash it does not know is error -5. A block hash is
//! derived from its parent and its height, so a chain rebuilt after a reorg
//! never re-issues a hash under a different parent.
//!
//! Every tip change adds one unit of chainwork, so a replacement always has
//! strictly more cumulative work than the tip it replaces.
use qbit_prism_server::codec;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, task::JoinHandle};

/// The genesis hash both fixtures answer for height 0.
pub const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

pub struct ChainState {
    pub tip: String,
    pub height: u64,
    pub chainwork: u64,
    /// The active chain by height, for `getblockhash`.
    blocks: HashMap<u64, String>,
    /// Every block's parent the node has seen, for `getblockheader`.
    parents: HashMap<String, String>,
    /// When each `submitblock` arrived, by block hash, taken before the
    /// request is decoded: the node-entry boundary.
    pub submissions: HashMap<String, Vec<Instant>>,
    /// Every best tip in order, with when it became the tip.
    pub tips: Vec<(String, Instant)>,
    /// When set, the next accepted `submitblock` loses a tip race to this
    /// same-height competitor inside the handler, before the reply is sent.
    lose_next_race_to: Option<String>,
}

impl ChainState {
    /// A chain whose tip is `tip` at `height`, on the genesis parent.
    pub fn new(tip: &str, height: u64) -> Self {
        Self {
            tip: tip.to_owned(),
            height,
            chainwork: 1,
            blocks: HashMap::from([(0, GENESIS.to_owned()), (height, tip.to_owned())]),
            parents: HashMap::from([(tip.to_owned(), GENESIS.to_owned())]),
            submissions: HashMap::new(),
            tips: vec![(tip.to_owned(), Instant::now())],
            lose_next_race_to: None,
        }
    }

    /// The active block at `height`, if the node holds one.
    pub fn block_at(&self, height: u64) -> Option<&str> {
        self.blocks.get(&height).map(String::as_str)
    }

    fn set_tip(&mut self, height: u64, hash: &str) {
        let parent = self
            .blocks
            .get(&(height - 1))
            .cloned()
            .expect("a new tip needs an active parent");
        self.blocks.retain(|at, _| *at < height);
        self.blocks.insert(height, hash.to_owned());
        self.parents.insert(hash.to_owned(), parent);
        self.tip = hash.to_owned();
        self.height = height;
        self.chainwork += 1;
        self.tips.push((hash.to_owned(), Instant::now()));
    }

    /// Replace the active block at `height` with `hash`, on the active block
    /// below it, and make it the tip, discarding everything above: the
    /// node's own reorg.
    pub fn reorg_to(&mut self, height: u64, hash: &str) {
        self.set_tip(height, hash);
    }

    /// Extend the active chain by one block of no interest to the pool. Its
    /// hash commits to its parent and its height.
    pub fn advance(&mut self) {
        let height = self.height + 1;
        let hash = codec::hash_display(&codec::double_sha256(
            format!("{}:{height}", self.tip).as_bytes(),
        ));
        self.set_tip(height, &hash);
    }

    /// The next block `submitblock` accepts is the best tip only inside the
    /// handler: before the reply, `competitor` replaces it at the same height.
    pub fn lose_next_race_to(&mut self, competitor: &str) {
        self.lose_next_race_to = Some(competitor.to_owned());
    }

    /// How long `hash` was the best tip before the next tip replaced it, if
    /// it was the tip and has been replaced.
    pub fn tip_lifetime(&self, hash: &str) -> Option<Duration> {
        let at = self.tips.iter().position(|(tip, _)| tip == hash)?;
        let (_, next) = self.tips.get(at + 1)?;
        Some(next.duration_since(self.tips[at].1))
    }
}

pub struct ScriptedNode {
    pub url: String,
    pub state: Arc<Mutex<ChainState>>,
    task: JoinHandle<()>,
}

impl ScriptedNode {
    pub async fn open(state: ChainState) -> anyhow::Result<Self> {
        let state = Arc::new(Mutex::new(state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let app = axum::Router::new()
            .route("/", axum::routing::post(reply))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { url, state, task })
    }

    /// Stop answering: every later RPC fails to connect.
    pub fn stop(&self) {
        self.task.abort();
    }
}

impl Drop for ScriptedNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn rpc_error(code: i64, message: String) -> Result<Value, Value> {
    Err(json!({"code": code, "message": message}))
}

async fn reply(
    axum::extract::State(node): axum::extract::State<Arc<Mutex<ChainState>>>,
    axum::Json(request): axum::Json<Value>,
) -> axum::Json<Value> {
    let arrived = Instant::now();
    let now = chrono::Utc::now().timestamp();
    let mut node = node.lock().await;
    let result = match request["method"].as_str().unwrap_or("") {
        "getblockhash" => match request["params"][0].as_u64() {
            Some(height) if height <= node.height => match node.block_at(height) {
                Some(hash) => Ok(json!(hash)),
                None => rpc_error(-8, format!("no active block held at height {height}")),
            },
            _ => rpc_error(-8, "Block height out of range".into()),
        },
        "getblockheader" => {
            let hash = request["params"][0].as_str().unwrap_or("");
            match node.parents.get(hash) {
                Some(parent) => Ok(json!({"previousblockhash": parent})),
                None => rpc_error(-5, "Block not found".into()),
            }
        }
        "getblockchaininfo" => Ok(json!({"chain":"test","initialblockdownload":false,
            "blocks":node.height,"headers":node.height,"bestblockhash":node.tip,
            "chainwork":format!("{:x}",node.chainwork)})),
        "getbestblockhash" => Ok(json!(node.tip)),
        "getnetworkinfo" => Ok(json!({"connections":2})),
        "getblocktemplate" => Ok(
            json!({"height":node.height+1,"coinbasevalue":500_000_000u64,
            "previousblockhash":node.tip,"version":0x20000000u32,"bits":"207fffff",
            "curtime":now,"mintime":now-1,"transactions":[]}),
        ),
        "estimatesmartfee" => Ok(json!({"feerate":"0.00001"})),
        "getmempoolinfo" => Ok(json!({"minrelaytxfee":"0.00001","mempoolminfee":"0.00001"})),
        "validateaddress" => {
            Ok(json!({"isvalid":true,"scriptPubKey":format!("5220{}","11".repeat(32))}))
        }
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap_or("")).unwrap();
            let hash = codec::hash_display(&codec::double_sha256(&block[..80]));
            node.submissions
                .entry(hash.clone())
                .or_default()
                .push(arrived);
            // Accepted: null reply, and the block is the new best tip.
            let height = node.height + 1;
            node.set_tip(height, &hash);
            // The scripted lost race runs here, under the same lock and
            // before the reply, so no observation can see the block active.
            if let Some(competitor) = node.lose_next_race_to.take() {
                node.reorg_to(height, &competitor);
            }
            Ok(Value::Null)
        }
        method => rpc_error(-32601, format!("unexpected scripted-node RPC {method}")),
    };
    axum::Json(match result {
        Ok(result) => json!({"id":request["id"],"result":result,"error":null}),
        Err(error) => json!({"id":request["id"],"result":null,"error":error}),
    })
}
