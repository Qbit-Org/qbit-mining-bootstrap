//! The regtest chain: one `qbitd` per PRISM node (A and B), as each host of
//! the pair runs its own, and a third, C, that stands for the rest of the
//! network. C listens; A and B connect out to it. C mints every external
//! block and the keepalive tips.
//!
//! **The ramp.** Regtest's proof-of-work limit makes every share a block, so
//! the chain is first mined to height 12,960 under a mock clock, where eight
//! 1,440-block retargets (`-legacyretarget`) have lowered the target to
//! `1e7fffc0`: about 131,000 hashes a block, and a share is a block with
//! probability 8 / the window's share count. This is the ramp of
//! `crates/qbit-prism-load/src/qbitd.rs` (#547), on three nodes. It takes
//! 25-40 s, so it is done once and kept: [`ramped_template`] stores the
//! three stopped data directories under a cache root, keyed by the `qbitd`
//! binary, and every scenario starts from a copy.
//!
//! **Keeping the template honest.** A regtest template more than 150 s after
//! its tip is served at the regtest limit, where every share is a block. A
//! copied chain's tip is as old as the cache, so [`Chain::start`] first
//! walks it up to the wall clock in 100 s steps under a mock clock, and a
//! keepalive task then has C mint a tip whenever the tip's own timestamp is
//! [`KEEPALIVE_SECONDS`] old. A cache whose tip is older than
//! [`CACHE_MAX_AGE`] is discarded rather than walked up.
//!
//! **Partitions.** [`Chain::isolate`] turns a node's networking off
//! (`setnetworkactive false`), which drops its peer and refuses new ones;
//! [`Chain::rejoin`] turns it back on and reconnects it to C. A frontend
//! keeps its own node's RPC either way: a dropped host network does not
//! separate a frontend from the `qbitd` on its own host.

use crate::process::Process;
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

/// The height the ramp ends at: eight retargets after the first at 2,880.
pub const RAMP_HEIGHT: u64 = 12_960;
/// The compact bits every block after the ramp carries.
pub const RAMP_BITS: &str = "1e7fffc0";
/// A template this long after its tip is served at the regtest limit.
pub const MIN_DIFFICULTY_GAP_SECONDS: i64 = 150;
/// C mints a keepalive tip once the tip's timestamp is this old.
pub const KEEPALIVE_SECONDS: i64 = 120;
/// A cached ramp whose tip is older than this is rebuilt: walking it up
/// would cost a block per 100 s and eat the retarget headroom.
pub const CACHE_MAX_AGE: Duration = Duration::from_secs(3 * 3600);
/// The RPC credentials of every node. Disposable regtest nodes on loopback.
pub const RPC_USER: &str = "prismtest";
pub const RPC_PASSWORD: &str = "prismtest";

const RAMP_BATCH: u64 = 250;
const RAMP_BLOCKS_PER_CHAIN_SECOND: u64 = 6;
const GENERATE_MAX_TRIES: u64 = 1_000_000_000;
const HEAVY_RPC_TIMEOUT: Duration = Duration::from_secs(600);
/// How long a node may take to load its 13,000-block index and answer RPC:
/// a few seconds on an idle disk, much longer behind a busy one. A setup
/// budget, not a measured behaviour.
const READY_BOUND: Duration = Duration::from_secs(180);
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
/// Peer timeouts compare the wall clock with stamps the ramp took under a
/// mock clock, so they are effectively disabled, as in the load harness.
const PEER_TIMEOUT_SECONDS: u64 = 999_999_999;

/// Which `qbitd`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
pub enum NodeName {
    A,
    B,
    C,
}

impl NodeName {
    pub fn dir(self) -> &'static str {
        match self {
            NodeName::A => "a",
            NodeName::B => "b",
            NodeName::C => "c",
        }
    }
}

/// One regtest `qbitd` and its JSON-RPC.
pub struct QbitNode {
    pub name: NodeName,
    process: Process,
    datadir: PathBuf,
    rpc_port: u16,
    p2p_port: u16,
    client: reqwest::Client,
}

impl QbitNode {
    fn spawn(name: NodeName, bin: &Path, datadir: &Path, log: &Path, listen: bool) -> Result<Self> {
        std::fs::create_dir_all(datadir)?;
        let rpc_port = crate::postgres::free_port()?;
        let p2p_port = crate::postgres::free_port()?;
        let mut command = Command::new(bin);
        command.args([
            "-regtest",
            "-server=1",
            "-dnsseed=0",
            "-discover=0",
            "-fallbackfee=0.00001",
            "-txindex=0",
            "-legacyretarget",
            "-printtoconsole=0",
        ]);
        if listen {
            command.args(["-listen=1", "-bind=127.0.0.1"]);
        } else {
            command.arg("-listen=0");
        }
        command
            .arg(format!("-rpcuser={RPC_USER}"))
            .arg(format!("-rpcpassword={RPC_PASSWORD}"))
            .arg(format!("-datadir={}", datadir.display()))
            .arg(format!("-rpcport={rpc_port}"))
            .arg(format!("-port={p2p_port}"))
            .arg(format!("-peertimeout={PEER_TIMEOUT_SECONDS}"));
        let process = Process::spawn(&format!("qbitd-{}", name.dir()), &mut command, log)?;
        // Idle connections are dropped before qbitd's idle close (#759).
        let client = qbit_prism_server::rpc::node_client_builder()
            .tcp_nodelay(true)
            .build()?;
        Ok(Self {
            name,
            process,
            datadir: datadir.to_owned(),
            rpc_port,
            p2p_port,
            client,
        })
    }

    pub fn rpc_port(&self) -> u16 {
        self.rpc_port
    }

    pub fn datadir(&self) -> &Path {
        &self.datadir
    }

    pub fn log(&self) -> PathBuf {
        self.datadir.join("regtest").join("debug.log")
    }

    async fn wait_ready(&self) -> Result<()> {
        let deadline = Instant::now() + READY_BOUND;
        loop {
            if let Some(status) = self.process.exited() {
                bail!("qbitd {:?} exited during startup: {status}", self.name);
            }
            if let Ok(info) = self.rpc("getblockchaininfo", json!([])).await {
                ensure!(
                    info["chain"] == "regtest",
                    "qbitd {:?} is not on regtest",
                    self.name
                );
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "qbitd {:?} did not answer RPC within {READY_BOUND:?}",
                self.name
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        self.rpc_timeout(method, params, RPC_TIMEOUT).await
    }

    pub async fn rpc_timeout(&self, method: &str, params: Value, limit: Duration) -> Result<Value> {
        let response = self
            .client
            .post(format!("http://127.0.0.1:{}/", self.rpc_port))
            .basic_auth(RPC_USER, Some(RPC_PASSWORD))
            .timeout(limit)
            .json(&json!({"jsonrpc": "1.0", "id": "dual-sim", "method": method, "params": params}))
            .send()
            .await
            .with_context(|| format!("qbitd {:?} {method}", self.name))?;
        let payload: Value = response.json().await?;
        ensure!(
            payload["error"].is_null(),
            "qbitd {:?} {method}: {}",
            self.name,
            payload["error"]
        );
        Ok(payload["result"].clone())
    }

    pub async fn best(&self) -> Result<String> {
        Ok(self
            .rpc("getbestblockhash", json!([]))
            .await?
            .as_str()
            .context("best block hash")?
            .to_owned())
    }

    /// `(height, tip time, tip hash)`.
    pub async fn tip(&self) -> Result<(u64, i64, String)> {
        let info = self.rpc("getblockchaininfo", json!([])).await?;
        Ok((
            info["blocks"].as_u64().context("height missing")?,
            info["time"].as_i64().context("tip time missing")?,
            info["bestblockhash"]
                .as_str()
                .context("best block missing")?
                .to_owned(),
        ))
    }

    pub async fn connections(&self) -> Result<u64> {
        self.rpc("getconnectioncount", json!([]))
            .await?
            .as_u64()
            .context("connection count")
    }

    async fn generate(&self, count: u64, address: &str) -> Result<Vec<String>> {
        let hashes = self
            .rpc_timeout(
                "generatetoaddress",
                json!([count, address, GENERATE_MAX_TRIES]),
                HEAVY_RPC_TIMEOUT,
            )
            .await?;
        let hashes: Vec<String> = hashes
            .as_array()
            .context("generatetoaddress answered no list")?
            .iter()
            .filter_map(|hash| hash.as_str().map(str::to_owned))
            .collect();
        // The default of a million tries silently mines fewer blocks at the
        // ramped difficulty, so the count is checked, never assumed.
        ensure!(
            hashes.len() as u64 == count,
            "generatetoaddress mined {} of {count} blocks",
            hashes.len()
        );
        Ok(hashes)
    }

    /// Ask the node to stop and wait for the process to exit.
    async fn stop(&self) -> Result<()> {
        if self.process.exited().is_some() {
            return Ok(());
        }
        let _ = self.rpc("stop", json!([])).await;
        let deadline = Instant::now() + Duration::from_secs(60);
        while self.process.exited().is_none() {
            ensure!(
                Instant::now() < deadline,
                "qbitd {:?} did not stop within 60 s",
                self.name
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// Connect to `peer` and wait until both count a connection. `onetry`
    /// makes one attempt, so it is repeated until one holds.
    async fn connect_to(&self, peer: &QbitNode) -> Result<()> {
        let address = format!("127.0.0.1:{}", peer.p2p_port);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if self.connections().await? > 0 && peer.connections().await? > 0 {
                let peers = self.rpc("getpeerinfo", json!([])).await?;
                let connected = peers
                    .as_array()
                    .is_some_and(|peers| peers.iter().any(|info| info["addr"] == address.as_str()));
                if connected {
                    return Ok(());
                }
            }
            ensure!(
                Instant::now() < deadline,
                "qbitd {:?} did not connect to {:?} within 30 s",
                self.name,
                peer.name
            );
            let _ = self
                .rpc("addnode", json!([address.as_str(), "onetry"]))
                .await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

fn now_seconds() -> i64 {
    chrono::Utc::now().timestamp()
}

async fn set_mocktime(nodes: &[&QbitNode], time: i64) -> Result<()> {
    for node in nodes {
        node.rpc("setmocktime", json!([time])).await?;
    }
    Ok(())
}

async fn wait_same_tip(nodes: &[&QbitNode], limit: Duration) -> Result<String> {
    let deadline = Instant::now() + limit;
    loop {
        let mut tips = Vec::new();
        for node in nodes {
            tips.push(node.best().await?);
        }
        if tips.windows(2).all(|pair| pair[0] == pair[1]) {
            return Ok(tips.remove(0));
        }
        if Instant::now() >= deadline {
            let mut state = Vec::new();
            for node in nodes {
                state.push(format!(
                    "{:?} on {} with {} peers",
                    node.name,
                    node.best().await?,
                    node.connections().await?
                ));
            }
            bail!(
                "nodes did not reach one tip within {limit:?}: {}",
                state.join("; ")
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The address C mints external blocks to: a derived regtest P2MR address
/// nobody holds a key for.
pub fn external_address() -> String {
    qbit_prism_load::qbitd::derived_address("dual-sim-external").0
}

/// The cached, stopped data directories of a ramped three-node chain, made
/// once per cache root and `qbitd` binary. Concurrent callers race to
/// rename their finished ramp into place; the loser's copy is discarded.
pub async fn ramped_template(bin: &Path, cache_root: &Path) -> Result<PathBuf> {
    let bytes = std::fs::read(bin).with_context(|| format!("reading {}", bin.display()))?;
    let key = hex::encode(&Sha256::digest(&bytes)[..8]);
    let template = cache_root.join(format!("ramp-{key}"));
    if template.join("ramp.json").exists() {
        let record: Value = serde_json::from_slice(&std::fs::read(template.join("ramp.json"))?)?;
        let tip_time = record["tip_time"].as_i64().unwrap_or(0);
        if now_seconds() - tip_time < CACHE_MAX_AGE.as_secs() as i64 {
            return Ok(template);
        }
        let _ = std::fs::remove_dir_all(&template);
    }
    std::fs::create_dir_all(cache_root)?;
    let work = cache_root.join(format!("ramping-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&work)?;
    let result = ramp_into(bin, &work).await;
    match result {
        Ok(record) => {
            std::fs::write(work.join("ramp.json"), serde_json::to_vec_pretty(&record)?)?;
            if std::fs::rename(&work, &template).is_err() {
                // Someone else's ramp landed first; theirs is as good.
                let _ = std::fs::remove_dir_all(&work);
            }
            Ok(template)
        }
        Err(error) => {
            let _ = std::fs::remove_dir_all(&work);
            Err(error)
        }
    }
}

async fn ramp_into(bin: &Path, work: &Path) -> Result<Value> {
    let started = Instant::now();
    let logs = work.join("logs");
    std::fs::create_dir_all(&logs)?;
    let c = QbitNode::spawn(
        NodeName::C,
        bin,
        &work.join("c"),
        &logs.join("qbitd-c.log"),
        true,
    )?;
    let a = QbitNode::spawn(
        NodeName::A,
        bin,
        &work.join("a"),
        &logs.join("qbitd-a.log"),
        false,
    )?;
    let b = QbitNode::spawn(
        NodeName::B,
        bin,
        &work.join("b"),
        &logs.join("qbitd-b.log"),
        false,
    )?;
    for node in [&c, &a, &b] {
        node.wait_ready().await?;
    }
    a.connect_to(&c).await?;
    b.connect_to(&c).await?;
    let address = external_address();
    let nodes = [&a, &b, &c];
    // Only the minter runs on a mock clock. A and B keep the wall clock:
    // every block is in their past and recent enough to end their initial
    // download at once, and a jump of their own clock would expire their
    // block downloads and drop the peer ("Timeout downloading block").
    loop {
        let (height, tip_time, _) = c.tip().await?;
        if height >= RAMP_HEIGHT {
            break;
        }
        // Each block is one second past the median of the last eleven while
        // the clock sits below it, so the clock is held back by the chain
        // time the rest of the ramp will consume.
        let remaining = RAMP_HEIGHT - height;
        let behind = (remaining / RAMP_BLOCKS_PER_CHAIN_SECOND) as i64 + 30;
        set_mocktime(&[&c], (tip_time + 1).max(now_seconds() - behind)).await?;
        c.generate(remaining.min(RAMP_BATCH), &address).await?;
    }
    wait_same_tip(&nodes, Duration::from_secs(120)).await?;
    catch_up(&nodes, &c, &address).await?;
    let template = a
        .rpc("getblocktemplate", json!([{"rules": ["segwit"]}]))
        .await?;
    ensure!(
        template["bits"] == RAMP_BITS,
        "the ramped chain serves template bits {}, not {RAMP_BITS}",
        template["bits"]
    );
    let (height, tip_time, tip) = c.tip().await?;
    for node in [&a, &b, &c] {
        node.stop().await?;
    }
    for name in ["a", "b", "c"] {
        scrub_copied_datadir(&work.join(name));
    }
    Ok(json!({
        "height": height,
        "tip": tip,
        "tip_time": tip_time,
        "bits": RAMP_BITS,
        "seconds": started.elapsed().as_secs_f64(),
    }))
}

/// Walk the tip up to the wall clock in steps inside the minimum-difficulty
/// gap, so every block keeps the ramped bits. Only the minter's clock moves;
/// it ends on the wall clock again.
async fn catch_up(nodes: &[&QbitNode], minter: &QbitNode, address: &str) -> Result<u64> {
    let mut blocks = 0;
    loop {
        let (_, tip_time, _) = minter.tip().await?;
        if tip_time >= now_seconds() - 60 {
            break;
        }
        ensure!(
            blocks < 200,
            "the chain is still behind the wall clock after {blocks} blocks"
        );
        set_mocktime(&[minter], (tip_time + 100).min(now_seconds())).await?;
        minter.generate(1, address).await?;
        blocks += 1;
    }
    set_mocktime(&[minter], 0).await?;
    wait_same_tip(nodes, Duration::from_secs(120)).await?;
    Ok(blocks)
}

/// Files a copied data directory must not keep: the old run's peers, lock
/// and pid.
fn scrub_copied_datadir(datadir: &Path) {
    for name in [
        "peers.dat",
        "anchors.dat",
        "banlist.json",
        ".lock",
        "qbitd.pid",
        "debug.log",
    ] {
        let _ = std::fs::remove_file(datadir.join("regtest").join(name));
    }
}

/// The three running nodes of one scenario.
pub struct Chain {
    pub a: QbitNode,
    pub b: QbitNode,
    pub c: QbitNode,
    address: String,
    isolated: [Arc<AtomicBool>; 2],
    keepalive: Option<JoinHandle<()>>,
    /// Keepalive mints and their failures, for the report.
    events: Arc<std::sync::Mutex<Vec<String>>>,
}

impl Chain {
    /// Start A, B and C from copies of the ramped `template` under `root`,
    /// connect them, walk the tip up to the wall clock and start the
    /// keepalive.
    pub async fn start(bin: &Path, template: &Path, root: &Path, logs: &Path) -> Result<Self> {
        for name in ["a", "b", "c"] {
            crate::postgres::copy_dir(&template.join(name), &root.join(name))?;
            scrub_copied_datadir(&root.join(name));
        }
        let c = QbitNode::spawn(
            NodeName::C,
            bin,
            &root.join("c"),
            &logs.join("qbitd-c.out"),
            true,
        )?;
        let a = QbitNode::spawn(
            NodeName::A,
            bin,
            &root.join("a"),
            &logs.join("qbitd-a.out"),
            false,
        )?;
        let b = QbitNode::spawn(
            NodeName::B,
            bin,
            &root.join("b"),
            &logs.join("qbitd-b.out"),
            false,
        )?;
        for node in [&c, &a, &b] {
            node.wait_ready().await?;
        }
        a.connect_to(&c).await?;
        b.connect_to(&c).await?;
        let address = external_address();
        let nodes = [&a, &b, &c];
        catch_up(&nodes, &c, &address).await?;
        for node in nodes {
            node.rpc("logging", json!([["net"]])).await?;
        }
        let mut chain = Self {
            a,
            b,
            c,
            address,
            isolated: [
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
            ],
            keepalive: None,
            events: Arc::default(),
        };
        chain.keepalive = Some(chain.spawn_keepalive());
        Ok(chain)
    }

    fn spawn_keepalive(&self) -> JoinHandle<()> {
        let port = self.c.rpc_port;
        let client = self.c.client.clone();
        let address = self.address.clone();
        let events = self.events.clone();
        tokio::spawn(async move {
            let rpc = MinterRpc { port, client };
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let Ok(info) = rpc.call("getblockchaininfo", json!([])).await else {
                    continue;
                };
                let Some(tip_time) = info["time"].as_i64() else {
                    continue;
                };
                if now_seconds() < tip_time + KEEPALIVE_SECONDS {
                    continue;
                }
                let minted = rpc.mint_one(tip_time, &address).await;
                if let Ok(mut events) = events.lock() {
                    events.push(match minted {
                        Ok(hash) => format!("keepalive {hash}"),
                        Err(error) => format!("keepalive failed: {error:#}"),
                    });
                }
            }
        })
    }

    pub fn node(&self, name: NodeName) -> &QbitNode {
        match name {
            NodeName::A => &self.a,
            NodeName::B => &self.b,
            NodeName::C => &self.c,
        }
    }

    /// The address external blocks pay.
    pub fn external_address(&self) -> &str {
        &self.address
    }

    /// Mint `count` external blocks on C, once C holds the best tip of every
    /// node still connected, so a mint never forks away a pool block on its
    /// way. Returns their hashes.
    pub async fn mint(&self, count: u64) -> Result<Vec<String>> {
        let mut connected = vec![&self.c];
        if !self.isolated[0].load(Ordering::SeqCst) {
            connected.push(&self.a);
        }
        if !self.isolated[1].load(Ordering::SeqCst) {
            connected.push(&self.b);
        }
        wait_same_tip(&connected, Duration::from_secs(30)).await?;
        let mut hashes = Vec::new();
        for _ in 0..count {
            let (_, tip_time, _) = self.c.tip().await?;
            let rpc = MinterRpc {
                port: self.c.rpc_port,
                client: self.c.client.clone(),
            };
            hashes.push(rpc.mint_one(tip_time, &self.address).await?);
        }
        wait_same_tip(&connected, Duration::from_secs(60)).await?;
        Ok(hashes)
    }

    /// Turn node A's or B's networking off: its peer drops and new ones are
    /// refused.
    pub async fn isolate(&self, name: NodeName) -> Result<()> {
        let index = pool_index(name)?;
        let node = self.node(name);
        node.rpc("setnetworkactive", json!([false])).await?;
        self.isolated[index].store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(30);
        while node.connections().await? > 0 {
            ensure!(
                Instant::now() < deadline,
                "qbitd {name:?} kept a peer for 30 s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// Turn networking back on, reconnect to C and wait for one tip.
    pub async fn rejoin(&self, name: NodeName) -> Result<String> {
        let index = pool_index(name)?;
        let node = self.node(name);
        node.rpc("setnetworkactive", json!([true])).await?;
        node.connect_to(&self.c).await?;
        self.isolated[index].store(false, Ordering::SeqCst);
        wait_same_tip(&[node, &self.c], Duration::from_secs(60)).await
    }

    /// Wait until every connected node holds C's tip.
    pub async fn converged(&self) -> Result<String> {
        let mut nodes = vec![&self.c];
        if !self.isolated[0].load(Ordering::SeqCst) {
            nodes.push(&self.a);
        }
        if !self.isolated[1].load(Ordering::SeqCst) {
            nodes.push(&self.b);
        }
        wait_same_tip(&nodes, Duration::from_secs(60)).await
    }

    pub fn keepalive_events(&self) -> Vec<String> {
        self.events
            .lock()
            .map(|events| events.clone())
            .unwrap_or_default()
    }

    /// Stop the keepalive and every node.
    pub async fn stop(&mut self) {
        if let Some(task) = self.keepalive.take() {
            task.abort();
        }
        for node in [&self.a, &self.b, &self.c] {
            let _ = node.stop().await;
        }
    }
}

impl Drop for Chain {
    fn drop(&mut self) {
        if let Some(task) = self.keepalive.take() {
            task.abort();
        }
    }
}

fn pool_index(name: NodeName) -> Result<usize> {
    match name {
        NodeName::A => Ok(0),
        NodeName::B => Ok(1),
        NodeName::C => bail!("C is the network, not a pool node"),
    }
}

/// C's RPC as the keepalive task holds it.
struct MinterRpc {
    port: u16,
    client: reqwest::Client,
}

impl MinterRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let payload: Value = self
            .client
            .post(format!("http://127.0.0.1:{}/", self.port))
            .basic_auth(RPC_USER, Some(RPC_PASSWORD))
            .timeout(HEAVY_RPC_TIMEOUT)
            .json(&json!({"jsonrpc": "1.0", "id": "dual-sim-minter", "method": method, "params": params}))
            .send()
            .await?
            .json()
            .await?;
        ensure!(payload["error"].is_null(), "{method}: {}", payload["error"]);
        Ok(payload["result"].clone())
    }

    /// One block on C. A block timestamped more than the minimum-difficulty
    /// gap after its parent would be mined at the regtest limit, and the
    /// parent can already be that old (a pool block carries its job's
    /// template time), so C's clock is held at [`KEEPALIVE_SECONDS`] past the
    /// parent for the one block and released after it.
    async fn mint_one(&self, parent_time: i64, address: &str) -> Result<String> {
        let latest = parent_time + KEEPALIVE_SECONDS;
        let held = now_seconds() > latest;
        if held {
            self.call("setmocktime", json!([latest])).await?;
        }
        let mined = self
            .call("generatetoaddress", json!([1, address, GENERATE_MAX_TRIES]))
            .await;
        if held {
            self.call("setmocktime", json!([0])).await?;
        }
        mined?
            .as_array()
            .and_then(|hashes| hashes.first())
            .and_then(|hash| hash.as_str())
            .map(str::to_owned)
            .context("generatetoaddress mined nothing")
    }
}
