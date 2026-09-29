//! The cutover rehearsal's qbit node: the chain a restored 2.x.x ledger was
//! mined on, served from the ledger itself (#575).
//!
//! A mainnet snapshot's blocks are not on regtest, and a started frontend
//! checks its pool blocks against the node: `getblockhash` at a confirmed
//! block's height must name that block, and the tip decides which blocks
//! the reconciler matures. This node answers from the rows of
//! `qbit_pool_blocks`: each confirmed block at its height, a synthetic hash
//! at every other height, and a tip that leaves every immature block
//! immature. Everything else follows `crates/qbit-prism-load/src/node.rs`,
//! reduced to what one frontend's startup, first job and self-check call.
use anyhow::{Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::task::JoinHandle;

/// Every address with this prefix is a valid P2MR address here, paying to
/// `5220 || sha256(address)`, the program the seeds record.
pub const ADDRESS_PREFIX: &str = "qb1";
/// `207fffff`: the stock template bits the load harness serves.
const TEMPLATE_BITS: &str = "207fffff";

struct Chain {
    tag: String,
    tip: u64,
    /// Heights whose block is one of the ledger's, and the reverse map.
    pool: HashMap<u64, String>,
    heights: HashMap<String, u64>,
    parents: HashMap<String, String>,
}

impl Chain {
    fn hash_at(&self, height: u64) -> Option<String> {
        if height > self.tip {
            return None;
        }
        if height == 0 {
            return Some("00".repeat(32));
        }
        Some(self.pool.get(&height).cloned().unwrap_or_else(|| {
            hex::encode(Sha256::digest(format!(
                "prism-rehearsal-chain:{}:{height}",
                self.tag
            )))
        }))
    }

    fn parent_of(&self, hash: &str) -> Option<String> {
        if let Some(parent) = self.parents.get(hash) {
            return Some(parent.clone());
        }
        let height = *self.heights.get(hash)?;
        self.hash_at(height.checked_sub(1)?)
    }
}

/// A running rehearsal node.
pub struct RehearsalNode {
    pub url: String,
    task: JoinHandle<()>,
}

impl Drop for RehearsalNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RehearsalNode {
    /// Serves the chain `pool`'s ledger records. `tag` names the synthetic
    /// hashes of the heights without a pool block, which the seeds use for
    /// the same heights, so a seeded chain is served exactly.
    pub async fn from_ledger(pool: &PgPool, tag: &str) -> Result<Self> {
        let rows = sqlx::query("SELECT block_hash,block_height,parent_hash FROM qbit_pool_blocks WHERE chain_state='confirmed' ORDER BY block_height")
            .fetch_all(pool)
            .await?;
        let mut chain = Chain {
            tag: tag.to_owned(),
            tip: 0,
            pool: HashMap::new(),
            heights: HashMap::new(),
            parents: HashMap::new(),
        };
        let mut highest = 0u64;
        for row in rows {
            let hash: String = row.try_get("block_hash")?;
            let height = u64::try_from(row.try_get::<i64, _>("block_height")?)?;
            chain
                .parents
                .insert(hash.clone(), row.try_get("parent_hash")?);
            chain.heights.insert(hash.clone(), height);
            chain.pool.insert(height, hash);
            highest = highest.max(height);
        }
        // The source's tip was at or above its highest confirmed block, and
        // every block it left immature was less than the maturity depth
        // under that tip, so it is under this one too: serving the highest
        // confirmed block as the tip matures nothing 2.x.x had not.
        chain.tip = highest.max(1);
        Self::serve(chain).await
    }

    async fn serve(chain: Chain) -> Result<Self> {
        let chain = Arc::new(Mutex::new(chain));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind rehearsal node")?;
        let url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new().route("/", post(answer)).with_state(chain);
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { url, task })
    }
}

fn payout_script_hex(address: &str) -> String {
    format!("5220{}", hex::encode(Sha256::digest(address.as_bytes())))
}

fn double_sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(Sha256::digest(bytes)).into()
}

async fn answer(State(chain): State<Arc<Mutex<Chain>>>, Json(request): Json<Value>) -> Json<Value> {
    let method = request["method"].as_str().unwrap_or_default();
    if method == "waitfornewblock" {
        // Nothing but the frontend itself mines here: wait out a bounded
        // share of the long poll, then report the tip.
        let wait = request["params"][0].as_u64().unwrap_or(1_000).min(1_000);
        tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
    }
    let params = &request["params"];
    let now = chrono::Utc::now().timestamp();
    let error = |code: i64, message: &str| {
        Json(
            json!({"id": request["id"], "result": Value::Null, "error": {"code": code, "message": message}}),
        )
    };
    let mut chain = chain.lock().expect("chain lock");
    let tip_hash = chain.hash_at(chain.tip).unwrap_or_default();
    let result = match method {
        "getblockchaininfo" => json!({
            "chain": "test",
            "initialblockdownload": false,
            "blocks": chain.tip,
            "headers": chain.tip,
            "bestblockhash": tip_hash,
            "chainwork": format!("{:x}", u128::from(chain.tip) + 1),
        }),
        "getnetworkinfo" => json!({"connections": 2}),
        "getbestblockhash" => json!(tip_hash),
        "getblocktemplate" => json!({
            "height": chain.tip + 1,
            "coinbasevalue": 5_000_000_000u64,
            "previousblockhash": tip_hash,
            "version": 0x2000_0000u32,
            "bits": TEMPLATE_BITS,
            "curtime": now,
            "mintime": now - 1,
            "transactions": [],
        }),
        "getblockhash" => match params[0].as_u64().and_then(|height| chain.hash_at(height)) {
            Some(hash) => json!(hash),
            None => return error(-8, "Block height out of range"),
        },
        "getblockheader" => match chain.parent_of(params[0].as_str().unwrap_or_default()) {
            Some(parent) => json!({"previousblockhash": parent}),
            None => json!({}),
        },
        "validateaddress" => {
            let address = params[0].as_str().unwrap_or_default();
            if address.starts_with(ADDRESS_PREFIX) {
                json!({"isvalid": true, "scriptPubKey": payout_script_hex(address)})
            } else {
                json!({"isvalid": false})
            }
        }
        "submitblock" => {
            let block = hex::decode(params[0].as_str().unwrap_or_default()).unwrap_or_default();
            if block.len() < 80 {
                json!("bad-block-length")
            } else {
                let mut header = double_sha256(&block[..80]);
                header.reverse();
                let hash = hex::encode(header);
                let mut parent = block[4..36].to_vec();
                parent.reverse();
                if hex::encode(parent) != tip_hash {
                    json!("prev-blk-not-found")
                } else {
                    let height = chain.tip + 1;
                    chain.tip = height;
                    chain.parents.insert(hash.clone(), tip_hash.clone());
                    chain.heights.insert(hash.clone(), height);
                    chain.pool.insert(height, hash);
                    Value::Null
                }
            }
        }
        "waitfornewblock" => json!({"hash": tip_hash, "height": chain.tip}),
        "estimatesmartfee" => json!({"feerate": "0.00001"}),
        "getmempoolinfo" => json!({"minrelaytxfee": "0.00001", "mempoolminfee": "0.00001"}),
        _ => return error(-32601, "unexpected RPC"),
    };
    Json(json!({"id": request["id"], "result": result, "error": Value::Null}))
}
