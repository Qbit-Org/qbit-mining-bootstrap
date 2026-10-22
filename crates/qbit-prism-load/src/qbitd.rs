//! Real-node mode (#547): a managed regtest `qbitd` pair behind the frontends.
//!
//! Pool node A serves the frontends through a recording JSON-RPC relay; peer
//! B (the shape of #530's `PeerNode`: B listens, A connects out) is the rest
//! of the network and mints every external tip with `generatetoaddress`.
//! Both are started with the arguments of the `live_regtest` fixtures'
//! launcher (`crates/qbit-prism-server/tests/live_regtest.rs`) plus
//! `-legacyretarget` and [`PEER_TIMEOUT_SECONDS`].
//!
//! Regtest's proof-of-work limit makes every share a block, so the chain is
//! ramped first, as #524 does (`tests/support/live_weighted_recipients.rs`):
//! blocks are mined on B under a mock clock until eight 1,440-block epochs,
//! each clamped to 4x, have lowered the target to [`RAMP_BITS`]. That is the
//! fake node's `1e7fffff` to within 0.0008%, so the window, the share
//! difficulty and the hashes per share are the fake run's. Two node rules
//! shape the clock. qbit refuses a block more than about ten minutes ahead
//! of its own clock, so the ramp's mock clock starts in the past and ends at
//! wall time, on both nodes, rather than running ahead. And regtest serves a
//! minimum-difficulty template once a block would be more than
//! [`MIN_DIFFICULTY_GAP_SECONDS`] after the tip, so after the ramp the mock
//! clock is cleared and B mints a keepalive tip whenever
//! [`KEEPALIVE_SECONDS`] pass without a block. Every block and every
//! template the relay sees must keep [`RAMP_BITS`]; the run's premise says
//! so (exit 8).
//!
//! What the node did is taken from the node: a watcher on A stamps every tip
//! change (a `waitfornewblock` long-poll given the tip it last saw, then a
//! walk over the heights, so no block is skipped), the relay records every
//! `submitblock` with the node's verdict verbatim, and at the end every block
//! above the ramp must be exactly one of B's mints or one accepted relay
//! submission.

use crate::node::{MintPurpose, SubmissionRecord, TipChange, TipOrigin};
use anyhow::{bail, ensure, Context, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    serve::ListenerExt,
    Router,
};
use chrono::{DateTime, Utc};
use qbit_prism_server::codec;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinHandle};

/// The height the ramp ends at: eight retargets after the first at 2,880.
pub const RAMP_HEIGHT: u64 = 12_960;
/// The compact bits every block after the ramp carries.
pub const RAMP_BITS: &str = "1e7fffc0";
/// `-legacyretarget`'s epoch on qbit regtest.
pub const RETARGET_INTERVAL: u64 = 1_440;
/// The first height whose bits the next retarget decides.
pub const NEXT_RETARGET_HEIGHT: u64 = RAMP_HEIGHT + RETARGET_INTERVAL;
/// Blocks kept free below the next retarget, for tips nobody planned (a
/// keepalive during a slow teardown, say).
pub const HEADROOM_MARGIN: u64 = 64;
/// A template this long after the tip is served at the regtest limit
/// (measured on qbit 1.0.0: 150 s keeps the ramped bits, 160 s does not).
pub const MIN_DIFFICULTY_GAP_SECONDS: u64 = 150;
/// B mints a keepalive tip once this long passes with no block.
pub const KEEPALIVE_SECONDS: u64 = 120;
/// The own-block interval a phase may imply: 9 s is #224's minimum
/// accepted-candidate interarrival, and 600 s is ten minutes, past which a
/// short plan lands nothing.
pub const CADENCE_BAND_SECONDS: (f64, f64) = (9.0, 600.0);
/// The ledger's window weighs this many network difficulties
/// (`window::WINDOW_MULTIPLIER`), so a share is a block with probability
/// `WINDOW_MULTIPLIER / W` whatever the ramp.
const WINDOW_MULTIPLIER: f64 = crate::window::WINDOW_MULTIPLIER as f64;
/// qbit regtest's bech32m human-readable part.
pub const REGTEST_HRP: &str = "qbrt";
/// The frontends' RPC user, as the fake node's run already names it.
pub const RPC_USER: &str = "qbit";
const RAMP_BATCH: u64 = 250;
const GENERATE_MAX_TRIES: u64 = 1_000_000_000;
/// Chain seconds per ramp block when the mock clock sits below the median
/// time past: each block is one second past the median of the last eleven,
/// measured at about six blocks a second of chain time.
const RAMP_BLOCKS_PER_CHAIN_SECOND: u64 = 6;
/// How far the ramp's tip may end from the wall clock.
const RAMP_TIP_BEHIND_LIMIT: i64 = 60;
const RAMP_TIP_AHEAD_LIMIT: i64 = 300;
/// Deadline of the node-heavy RPCs (a 250-block ramp batch), which are not
/// idempotent and so are given time rather than retried (#533).
const HEAVY_RPC_TIMEOUT: Duration = Duration::from_secs(600);
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const WATCH_POLL_MS: u64 = 1_000;
/// Both nodes' `-peertimeout`. qbit's inactivity and ping checks compare the
/// wall clock with send and receive stamps, and the ramp's stamps are taken
/// under a mock clock that starts about 36 minutes in the past; qbit's own
/// source says tests that use mocktime and see disconnects should raise it.
/// The link to B is the run's whole network, and nothing reconnects it.
const PEER_TIMEOUT_SECONDS: u64 = 999_999_999;
/// How long a mint waits for B to hold A's tip before it gives up rather
/// than fork the chain.
const MINT_SAME_TIP_TIMEOUT: Duration = Duration::from_secs(10);

// --- addresses -----------------------------------------------------------

const BECH32_CHARSET: &[u8; 32] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const BECH32M_CONST: u32 = 0x2bc8_30a3;

fn bech32_polymod(values: &[u8]) -> u32 {
    const GENERATOR: [u32; 5] = [
        0x3b6a_57b2,
        0x2650_8e6d,
        0x1ea1_19fa,
        0x3d42_33dd,
        0x2a14_62b3,
    ];
    let mut checksum: u32 = 1;
    for value in values {
        let top = checksum >> 25;
        checksum = ((checksum & 0x01ff_ffff) << 5) ^ u32::from(*value);
        for (bit, generator) in GENERATOR.iter().enumerate() {
            if (top >> bit) & 1 == 1 {
                checksum ^= generator;
            }
        }
    }
    checksum
}

/// The segwit-style address of a witness program, bech32m-encoded (BIP 350),
/// as qbit encodes a P2MR (witness version 2) output.
pub fn witness_address(hrp: &str, version: u8, program: &[u8]) -> String {
    let mut data = vec![version];
    let (mut accumulator, mut bits) = (0u32, 0u32);
    for byte in program {
        accumulator = (accumulator << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            data.push(((accumulator >> bits) & 31) as u8);
        }
    }
    if bits > 0 {
        data.push(((accumulator << (5 - bits)) & 31) as u8);
    }
    let mut values: Vec<u8> = hrp.bytes().map(|byte| byte >> 5).collect();
    values.push(0);
    values.extend(hrp.bytes().map(|byte| byte & 31));
    values.extend(&data);
    values.extend([0u8; 6]);
    let checksum = bech32_polymod(&values) ^ BECH32M_CONST;
    let mut address = format!("{hrp}1");
    for value in data
        .iter()
        .copied()
        .chain((0..6).map(|index| ((checksum >> (5 * (5 - index))) & 31) as u8))
    {
        address.push(BECH32_CHARSET[value as usize] as char);
    }
    address
}

/// A regtest P2MR address nobody holds a key for, derived from `seed`, and
/// its 32-byte program. Payouts to it are valid outputs; nothing spends
/// them, and phase 1 of #547 checks none.
pub fn derived_address(seed: &str) -> (String, String) {
    let program = Sha256::digest(seed.as_bytes());
    (
        witness_address(REGTEST_HRP, 2, &program),
        hex::encode(program),
    )
}

// --- cadence band ----------------------------------------------------------

/// The own-block interval a share stream implies: a share is a block with
/// probability `8 / window`, so `rate` shares a second find one every
/// `window / (8 x rate)` seconds.
pub fn implied_block_interval_seconds(window_shares: u64, rate: f64) -> f64 {
    window_shares as f64 / (WINDOW_MULTIPLIER * rate)
}

/// Refuse a phase whose offered rate implies an own-block cadence outside
/// [`CADENCE_BAND_SECONDS`] (EP-VALIDATION).
pub fn check_cadence_band(window_shares: u64, phases: &[(String, f64)]) -> Result<()> {
    let (low, high) = CADENCE_BAND_SECONDS;
    for (name, rate) in phases {
        let interval = implied_block_interval_seconds(window_shares, *rate);
        ensure!(
            (low..=high).contains(&interval),
            "--node qbitd: phase {name} offers {rate} shares/s over a {window_shares}-share \
             window, which implies an own block every {interval:.1} s, outside the realistic \
             band of {low}-{high} s; offered rates of {:.1}-{:.1} shares/s keep this window \
             inside it",
            window_shares as f64 / (WINDOW_MULTIPLIER * high),
            window_shares as f64 / (WINDOW_MULTIPLIER * low),
        );
    }
    Ok(())
}

/// The `node.cadence_band` block: the band, and where each phase sits in it.
pub fn cadence_band_block(
    window_shares: u64,
    phases: &[(String, f64, u64)],
    hashes_per_share: f64,
    hashes_per_block: f64,
) -> Value {
    let (low, high) = CADENCE_BAND_SECONDS;
    json!({
        "band_seconds": [low, high],
        "band_rule": "a share is a block with probability 8 / window_shares, so the own-block \
                      interval a phase implies is window_shares / (8 x offered rate); 9 s is \
                      #224's minimum accepted-candidate interarrival, 600 s ten minutes",
        "blocks_per_share": WINDOW_MULTIPLIER / window_shares as f64,
        "window_shares": window_shares,
        "offered_rate_band_shares_per_second": [
            window_shares as f64 / (WINDOW_MULTIPLIER * high),
            window_shares as f64 / (WINDOW_MULTIPLIER * low),
        ],
        "expected_hashes_per_share": hashes_per_share,
        "expected_hashes_per_block": hashes_per_block,
        "network_tip_max_gap_seconds": KEEPALIVE_SECONDS,
        "min_difficulty_gap_seconds": MIN_DIFFICULTY_GAP_SECONDS,
        "phases": phases.iter().map(|(name, rate, seconds)| {
            let interval = implied_block_interval_seconds(window_shares, *rate);
            json!({
                "phase": name,
                "offered_rate": rate,
                "seconds": seconds,
                "implied_own_block_interval_seconds": interval,
                "expected_block_solutions": *seconds as f64 / interval,
                "in_band": (low..=high).contains(&interval),
            })
        }).collect::<Vec<_>>(),
    })
}

/// Blocks a run can add above the ramp, at most: every planned tip and own
/// block, one keepalive per [`KEEPALIVE_SECONDS`] of the run's longest
/// plausible length, and the ramp's own catch-up.
pub fn planned_block_ceiling(planned_mints: u64, planned_landings: u64, run_seconds: u64) -> u64 {
    planned_mints + planned_landings + run_seconds / KEEPALIVE_SECONDS + 2 + RAMP_CATCH_UP_LIMIT
}

/// Refuse a run that could reach the next retarget, where the bits would
/// change under it (EP-VALIDATION).
pub fn check_headroom(ceiling: u64) -> Result<()> {
    let headroom = NEXT_RETARGET_HEIGHT - RAMP_HEIGHT - 1 - HEADROOM_MARGIN;
    ensure!(
        ceiling <= headroom,
        "--node qbitd: the run could add {ceiling} blocks above the ramp, more than the \
         {headroom} the ramped epoch holds before its next retarget at height \
         {NEXT_RETARGET_HEIGHT}; shorten it or mint fewer tips"
    );
    Ok(())
}

const RAMP_CATCH_UP_LIMIT: u64 = 8;
/// `submitblock` results that are not the node refusing the block: a block
/// it already had, and a valid block that did not become the tip.
const SUBMIT_VERDICTS_NOT_REJECTIONS: [&str; 3] =
    ["duplicate", "inconclusive", "duplicate-inconclusive"];

// --- RPC -----------------------------------------------------------------

#[derive(Clone)]
struct Rpc {
    client: reqwest::Client,
    url: String,
    password: String,
}

impl Rpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.call_timeout(method, params, RPC_TIMEOUT).await
    }

    async fn call_timeout(&self, method: &str, params: Value, timeout: Duration) -> Result<Value> {
        let payload: Value = self
            .client
            .post(&self.url)
            .basic_auth(RPC_USER, Some(&self.password))
            .timeout(timeout)
            .json(
                &json!({"jsonrpc": "1.0", "id": "prism-load", "method": method, "params": params}),
            )
            .send()
            .await
            .with_context(|| format!("qbitd RPC {method}"))?
            .json()
            .await
            .with_context(|| format!("qbitd RPC {method}: reading the answer"))?;
        ensure!(
            payload["error"].is_null(),
            "qbitd RPC {method}: {}",
            payload["error"]
        );
        Ok(payload["result"].clone())
    }
}

fn now_seconds() -> i64 {
    Utc::now().timestamp()
}

// --- processes -----------------------------------------------------------

struct NodeProcess {
    name: &'static str,
    child: Mutex<Child>,
    datadir: PathBuf,
    rpc: Rpc,
    rpc_port: u16,
    p2p_port: u16,
}

impl NodeProcess {
    fn spawn(
        name: &'static str,
        bin: &Path,
        root: &Path,
        log_dir: &Path,
        password: &str,
        listen: bool,
        client: &reqwest::Client,
    ) -> Result<Self> {
        let datadir = root.join(name);
        std::fs::create_dir_all(&datadir)?;
        let rpc_port = free_port()?;
        let p2p_port = free_port()?;
        let mut command = Command::new(bin);
        command.args([
            "-regtest",
            "-server=1",
            "-dnsseed=0",
            "-discover=0",
            "-fallbackfee=0.00001",
            "-txindex=0",
            "-legacyretarget",
        ]);
        if listen {
            command.args(["-listen=1", "-bind=127.0.0.1"]);
        } else {
            command.arg("-listen=0");
        }
        command
            .arg(format!("-rpcuser={RPC_USER}"))
            .arg(format!("-rpcpassword={password}"))
            .arg(format!("-datadir={}", datadir.display()))
            .arg(format!("-rpcport={rpc_port}"))
            .arg(format!("-port={p2p_port}"))
            .arg(format!("-peertimeout={PEER_TIMEOUT_SECONDS}"));
        let log = std::fs::File::create(log_dir.join(format!("qbitd-{name}.stdout.log")))?;
        let child = command
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()
            .with_context(|| format!("starting {}", bin.display()))?;
        Ok(Self {
            name,
            child: Mutex::new(child),
            datadir,
            rpc: Rpc {
                client: client.clone(),
                url: format!("http://127.0.0.1:{rpc_port}/"),
                password: password.to_owned(),
            },
            rpc_port,
            p2p_port,
        })
    }

    async fn wait_ready(&self) -> Result<()> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Some(reason) = self.exited() {
                bail!("{reason} during startup");
            }
            if let Ok(info) = self.rpc.call("getblockchaininfo", json!([])).await {
                ensure!(
                    info["chain"] == "regtest",
                    "qbitd {} is not on regtest: {}",
                    self.name,
                    info["chain"]
                );
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "qbitd {} did not answer RPC within {READY_TIMEOUT:?}",
                self.name
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    fn running(&self) -> bool {
        matches!(self.child.lock().expect("qbitd child").try_wait(), Ok(None))
    }

    fn exited(&self) -> Option<String> {
        match self.child.lock().expect("qbitd child").try_wait() {
            Ok(Some(status)) => Some(format!("qbitd {} exited: {status}", self.name)),
            Ok(None) => None,
            Err(error) => Some(format!("qbitd {} could not be polled: {error}", self.name)),
        }
    }

    fn peak_rss_kib(&self) -> Option<u64> {
        let pid = self.child.lock().expect("qbitd child").id();
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        status
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))
            .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
    }

    /// Ask the node to stop, then kill it if it has not within the bound.
    async fn stop(&self, log_dir: &Path) {
        if self.running() {
            let _ = self
                .rpc
                .call_timeout("stop", json!([]), Duration::from_secs(10))
                .await;
            let deadline = Instant::now() + Duration::from_secs(30);
            while self.running() && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            self.kill();
        }
        self.save_log(log_dir);
    }

    fn save_log(&self, log_dir: &Path) {
        let _ = std::fs::copy(
            self.datadir.join("regtest/debug.log"),
            log_dir.join(format!("qbitd-{}.debug.log", self.name)),
        );
    }
}

impl NodeProcess {
    fn kill(&self) {
        let mut child = self.child.lock().expect("qbitd child");
        if matches!(child.try_wait(), Ok(None)) {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

/// The data root of a start that has not finished: removed when the start
/// fails, unless the artifacts are kept.
struct RootGuard {
    root: Option<PathBuf>,
    keep: bool,
}

impl Drop for RootGuard {
    fn drop(&mut self) {
        if let Some(root) = self.root.take().filter(|_| !self.keep) {
            let _ = std::fs::remove_dir_all(root);
        }
    }
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

// --- relay ---------------------------------------------------------------

/// One `submitblock` the relay forwarded, with what the node answered.
#[derive(Clone, Debug, Serialize)]
pub struct RelaySubmission {
    pub block_hash: String,
    pub parent: String,
    pub accepted: bool,
    /// The node's result string (`"duplicate"`, a rejection reason) or its
    /// error, verbatim; `None` for the `null` of an accepted block.
    pub verdict: Option<String>,
    pub received_at: DateTime<Utc>,
    pub response_millis: f64,
    pub block_bytes: usize,
    /// When the node's verdict came back: an accepted block is A's tip from
    /// then, which can be before the watcher wakes.
    #[serde(skip_serializing)]
    answered: (Instant, DateTime<Utc>),
}

#[derive(Default)]
struct RelayLog {
    submissions: Vec<RelaySubmission>,
    latencies: BTreeMap<String, Vec<f64>>,
    transport_errors: Vec<String>,
    /// `submitblock` requests whose block could not be read, so neither its
    /// hash nor the node's verdict on it is known.
    unparsed_submissions: u64,
    /// Bits of every `getblocktemplate` answer, with how often each came.
    template_bits: BTreeMap<String, u64>,
}

struct RelayState {
    upstream: String,
    client: reqwest::Client,
    log: Mutex<RelayLog>,
}

/// Forward one frontend request to A and log it. The forward runs as its
/// own task: a frontend that gives up on the request (a timeout, a kill
/// phase, teardown) drops this handler, and a block A has already accepted
/// must still be recorded, or it would be an unattributed tip.
async fn relay_handler(
    State(state): State<Arc<RelayState>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    tokio::spawn(relay_forward(state, uri, headers, body))
        .await
        .unwrap_or_else(|_| (StatusCode::BAD_GATEWAY, "relay task failed").into_response())
}

async fn relay_forward(
    state: Arc<RelayState>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request: Option<Value> = serde_json::from_slice(&body).ok();
    let method = request
        .as_ref()
        .and_then(|value| value["method"].as_str())
        .unwrap_or("(unparsed)")
        .to_owned();
    let mut forward = state
        .client
        .post(format!(
            "{}{}",
            state.upstream,
            uri.path().trim_start_matches('/')
        ))
        .body(body.clone());
    for name in [
        axum::http::header::AUTHORIZATION,
        axum::http::header::CONTENT_TYPE,
    ] {
        if let Some(value) = headers.get(&name) {
            forward = forward.header(name, value);
        }
    }
    let received_at = Utc::now();
    let started = Instant::now();
    let answer = match forward.send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            response.bytes().await.map(|bytes| (status, bytes))
        }
        Err(error) => Err(error),
    };
    let millis = started.elapsed().as_secs_f64() * 1000.0;
    let answered = (Instant::now(), Utc::now());
    let (status, bytes) = match answer {
        Ok(answer) => answer,
        Err(error) => {
            let mut log = state.log.lock().expect("relay log");
            log.transport_errors.push(format!(
                "{method}: {}",
                crate::frontend::redact_secrets_in_text(&error.to_string())
            ));
            return (StatusCode::BAD_GATEWAY, "qbitd unreachable").into_response();
        }
    };
    {
        let parsed: Option<Value> = serde_json::from_slice(&bytes).ok();
        let mut log = state.log.lock().expect("relay log");
        log.latencies
            .entry(method.clone())
            .or_default()
            .push(millis);
        if method == "getblocktemplate" {
            if let Some(bits) = parsed
                .as_ref()
                .and_then(|value| value["result"]["bits"].as_str())
            {
                *log.template_bits.entry(bits.to_owned()).or_insert(0) += 1;
            }
        }
        if method == "submitblock" {
            let block_hex = request
                .as_ref()
                .and_then(|value| value["params"][0].as_str())
                .unwrap_or_default();
            if let Some((block_hash, parent, block_bytes)) = block_identity(block_hex) {
                let (accepted, verdict) = match &parsed {
                    Some(value) if !value["error"].is_null() => {
                        (false, Some(value["error"].to_string()))
                    }
                    Some(value) if value["result"].is_null() => (true, None),
                    Some(value) => (
                        false,
                        Some(
                            value["result"]
                                .as_str()
                                .map_or_else(|| value["result"].to_string(), str::to_owned),
                        ),
                    ),
                    None => (false, Some(format!("unparsed answer, HTTP {status}"))),
                };
                log.submissions.push(RelaySubmission {
                    block_hash,
                    parent,
                    accepted,
                    verdict,
                    received_at,
                    response_millis: millis,
                    block_bytes,
                    answered,
                });
            } else {
                log.unparsed_submissions += 1;
            }
        }
    }
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    (
        status,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        bytes,
    )
        .into_response()
}

/// The display hash, the parent and the size of a serialized block.
fn block_identity(block_hex: &str) -> Option<(String, String, usize)> {
    let block = hex::decode(block_hex).ok()?;
    if block.len() < 80 {
        return None;
    }
    let hash = codec::hash_display(&codec::double_sha256(&block[..80]));
    let mut parent = block[4..36].to_vec();
    parent.reverse();
    Some((hash, hex::encode(parent), block.len()))
}

// --- the managed pair -----------------------------------------------------

/// One tip change seen on A, before its origin is decided.
#[derive(Clone, Debug)]
struct ObservedTip {
    hash: String,
    height: u64,
    /// The block's own timestamp. A pool block carries its job's template
    /// time, which can be well behind when it landed.
    time: i64,
    monotonic: Instant,
    wall: DateTime<Utc>,
    /// Seen in the same wake-up as a later block, so stamped with its time.
    coalesced: bool,
}

/// One tip B was asked to mint, and what came of it.
#[derive(Clone, Debug)]
struct MintRecord {
    purpose: MintPurpose,
    requested: Instant,
    requested_wall: DateTime<Utc>,
    hash: Option<String>,
    minted: Option<Instant>,
    minted_wall: Option<DateTime<Utc>>,
    error: Option<String>,
}

struct Shared {
    /// The ramp's tip time, until the watcher has seen a block.
    ramp_tip_time: i64,
    tips: Mutex<Vec<ObservedTip>>,
    mints: Mutex<Vec<MintRecord>>,
    failure: Mutex<Option<String>>,
    /// Mints asked for, counted as they are sent, so a request still in the
    /// channel is pending too.
    requested: AtomicU64,
    /// No more keepalives: the run is tearing down. A lock, not a flag, so
    /// no keepalive is asked for after [`Qbitd::quiesce`] closed it.
    stop_minting: Mutex<bool>,
    /// No keepalive for now: a scheduled block is due or outstanding (#638).
    /// Set and read under `stop_minting`, so a keepalive is either already
    /// counted in `requested` when the scheduler reads the settled tip, or
    /// is not asked for until the block's verdict.
    keepalives_held: AtomicBool,
    /// The watcher lost A: the node is gone, which is an abort (exit 6),
    /// not a disagreement.
    lost: AtomicBool,
    /// The watcher stops too: the nodes are going away.
    stop: AtomicBool,
}

/// What the ramp did.
#[derive(Clone, Debug, Serialize)]
pub struct RampRecord {
    pub height: u64,
    pub bits: String,
    pub seconds: f64,
    pub catch_up_blocks: u64,
    pub tip_time_minus_wall_seconds: i64,
    pub peer_lag_seconds: f64,
}

/// The managed pair, its relay and its watchers.
pub struct Qbitd {
    pool: NodeProcess,
    peer: NodeProcess,
    root: PathBuf,
    keep: bool,
    log_dir: PathBuf,
    bin: PathBuf,
    bin_sha256: String,
    subversion: String,
    external_address: String,
    ramp: RampRecord,
    ramp_tip: (String, u64),
    relay_url: String,
    relay: Arc<RelayState>,
    shared: Arc<Shared>,
    mint_tx: mpsc::UnboundedSender<MintRecord>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// When the ramp's tip became the pool node's tip, on both clocks.
    ramp_seen: (Instant, DateTime<Utc>),
    /// Heights of submitted blocks' parents the watcher did not see, as the
    /// node reported them.
    parent_heights: Mutex<HashMap<String, u64>>,
}

impl Qbitd {
    /// Start A and B, ramp the chain, and put the relay in front of A.
    pub async fn start(
        bin: &Path,
        run_tag: &str,
        password: &str,
        log_dir: &Path,
        keep: bool,
    ) -> Result<Self> {
        let bin = std::fs::canonicalize(bin)
            .with_context(|| format!("--qbitd-bin {} does not exist", bin.display()))?;
        let bytes = std::fs::read(&bin).with_context(|| format!("reading {}", bin.display()))?;
        let bin_sha256 = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));
        let root = std::env::temp_dir().join(format!("prism-load-qbitd-{run_tag}"));
        std::fs::create_dir_all(&root)?;
        // Removes the data on a failed start. Declared before the nodes, so
        // it is dropped after them: the directories go once both are dead.
        let mut root_guard = RootGuard {
            root: Some(root.clone()),
            keep,
        };
        // Idle connections are dropped before qbitd's idle close (#759).
        let client = qbit_prism_server::rpc::node_client_builder()
            .tcp_nodelay(true)
            .build()
            .context("building the qbitd RPC client")?;
        let pool = NodeProcess::spawn("a", &bin, &root, log_dir, password, false, &client)?;
        let peer = NodeProcess::spawn("b", &bin, &root, log_dir, password, true, &client)?;
        pool.wait_ready().await?;
        peer.wait_ready().await?;
        let subversion = pool.rpc.call("getnetworkinfo", json!([])).await?["subversion"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let (external_address, _) = derived_address(&format!("prism-load-external-{run_tag}"));
        connect(&pool, &peer).await?;
        let ramp = ramp(&pool.rpc, &peer.rpc, &external_address).await?;
        let ramp_seen = (Instant::now(), Utc::now());
        let tip = pool.rpc.call("getbestblockhash", json!([])).await?;
        let ramp_tip = (
            tip.as_str().context("best block hash")?.to_owned(),
            ramp.height,
        );
        let ramp_tip_time = chain_info(&pool.rpc).await?.1;
        // Every post-ramp peer message goes to the debug logs (the ramp's
        // ~13k blocks would drown them): a lost link is otherwise silent.
        for node in [&pool, &peer] {
            node.rpc.call("logging", json!([["net"]])).await?;
        }

        let relay = Arc::new(RelayState {
            upstream: pool.rpc.url.clone(),
            client: client.clone(),
            log: Mutex::new(RelayLog::default()),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding the qbitd relay")?;
        let relay_url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .fallback(relay_handler)
            .with_state(relay.clone());
        let server = tokio::spawn(async move {
            // Every frontend RPC crosses this hop, so small writes go out at
            // once rather than waiting on Nagle.
            let listener = listener.tap_io(|stream| {
                let _ = stream.set_nodelay(true);
            });
            let _ = axum::serve(listener, app).await;
        });

        let shared = Arc::new(Shared {
            ramp_tip_time,
            tips: Mutex::new(Vec::new()),
            mints: Mutex::new(Vec::new()),
            failure: Mutex::new(None),
            requested: AtomicU64::new(0),
            stop_minting: Mutex::new(false),
            keepalives_held: AtomicBool::new(false),
            lost: AtomicBool::new(false),
            stop: AtomicBool::new(false),
        });
        let (mint_tx, mint_rx) = mpsc::unbounded_channel();
        let watcher = tokio::spawn(watch(pool.rpc.clone(), ramp_tip.clone(), shared.clone()));
        let minter = tokio::spawn(mint(
            pool.rpc.clone(),
            peer.rpc.clone(),
            external_address.clone(),
            mint_rx,
            shared.clone(),
        ));
        let keepalive = tokio::spawn(keepalive(mint_tx.clone(), shared.clone()));
        root_guard.root = None;
        Ok(Self {
            pool,
            peer,
            root,
            keep,
            log_dir: log_dir.to_owned(),
            bin,
            bin_sha256,
            subversion,
            external_address,
            ramp,
            ramp_tip,
            relay_url,
            relay,
            shared,
            mint_tx,
            tasks: Mutex::new(vec![server, watcher, minter, keepalive]),
            ramp_seen,
            parent_heights: Mutex::new(HashMap::new()),
        })
    }

    /// The URL the frontends are given: the relay in front of A.
    pub fn url(&self) -> &str {
        &self.relay_url
    }

    /// Check that A accepts `address` as the P2MR output of `program`.
    pub async fn check_address(&self, address: &str, program: &str) -> Result<()> {
        let answer = self
            .pool
            .rpc
            .call("validateaddress", json!([address]))
            .await?;
        ensure!(
            answer["isvalid"] == true && answer["scriptPubKey"] == format!("5220{program}"),
            "qbitd does not accept {address} as the P2MR output of {program}: {answer}"
        );
        Ok(())
    }

    /// The first reason a node or a watcher failed, if one has.
    pub fn failure(&self) -> Option<String> {
        if let Some(reason) = self.shared.failure.lock().expect("failure").clone() {
            return Some(reason);
        }
        self.pool.exited().or_else(|| self.peer.exited())
    }

    /// Why a node is gone, if one is: a process that exited, or a watcher
    /// that could no longer reach A. A gone node aborts the run (exit 6); its
    /// chain cannot be read back, and that is not a disagreement.
    pub fn lost(&self) -> Option<String> {
        if let Some(reason) = self.pool.exited().or_else(|| self.peer.exited()) {
            return Some(reason);
        }
        self.shared
            .lost
            .load(Ordering::SeqCst)
            .then(|| self.failure())
            .flatten()
    }

    /// Stop minting keepalives and wait, within a bound, for every mint
    /// already asked for to be seen on A, so the tip log is complete.
    pub async fn quiesce(&self) {
        *self.shared.stop_minting.lock().expect("stop minting") = true;
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            let pending = {
                let mints = self.shared.mints.lock().expect("mints");
                let tips = self.shared.tips.lock().expect("tips");
                let seen: HashSet<&str> = tips.iter().map(|tip| tip.hash.as_str()).collect();
                (mints.len() as u64) < self.shared.requested.load(Ordering::SeqCst)
                    || mints.iter().any(|mint| {
                        mint.error.is_none()
                            && mint.hash.as_deref().is_none_or(|hash| !seen.contains(hash))
                    })
            };
            if !pending {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Every tip change on A since the ramp, the ramp's tip first.
    ///
    /// A tip is stamped when its origin says it happened, if that is before
    /// the watcher woke: when B's `generatetoaddress` returned for a mint (as
    /// the fake node stamps its own mints), and when A answered the relay's
    /// `submitblock` for a pool block. A frontend can serve work on a tip
    /// before the watcher's long-poll returns, and a coalesced tip is only
    /// seen with the next one; stamped late, that work would be dropped as
    /// earlier than the tip.
    pub fn tip_changes(&self) -> Vec<TipChange> {
        let tips = self.shared.tips.lock().expect("tips").clone();
        let minted: HashMap<String, (Instant, DateTime<Utc>)> = self
            .shared
            .mints
            .lock()
            .expect("mints")
            .iter()
            .filter_map(|mint| Some((mint.hash.clone()?, (mint.minted?, mint.minted_wall?))))
            .collect();
        let pool: HashMap<String, (Instant, DateTime<Utc>)> = self
            .relay
            .log
            .lock()
            .expect("relay log")
            .submissions
            .iter()
            .filter(|submission| submission.accepted)
            .map(|submission| (submission.block_hash.clone(), submission.answered))
            .collect();
        let mut changes = vec![TipChange {
            hash: self.ramp_tip.0.clone(),
            height: self.ramp_tip.1,
            origin: TipOrigin::Bootstrap,
            monotonic: self.ramp_seen.0,
            wall: self.ramp_seen.1,
        }];
        changes.extend(tips.into_iter().map(|tip| {
            let (origin, at) = if let Some(at) = pool.get(&tip.hash) {
                (TipOrigin::Pool, Some(*at))
            } else if let Some(at) = minted.get(&tip.hash) {
                (TipOrigin::External, Some(*at))
            } else {
                (TipOrigin::Unattributed, None)
            };
            let (monotonic, wall) = at
                .filter(|(monotonic, _)| *monotonic < tip.monotonic)
                .unwrap_or((tip.monotonic, tip.wall));
            TipChange {
                origin,
                hash: tip.hash,
                height: tip.height,
                monotonic,
                wall,
            }
        }));
        changes
    }

    /// The tips minted for `purpose`, as A saw them, in the order asked.
    /// A mint A never saw is left out here and reported by
    /// [`Qbitd::chain_reconciliation`].
    pub fn observed_mints(&self, purpose: MintPurpose) -> Vec<TipChange> {
        let changes = self.tip_changes();
        let mints = self.shared.mints.lock().expect("mints").clone();
        mints
            .iter()
            .filter(|mint| mint.purpose == purpose)
            .filter_map(|mint| {
                let hash = mint.hash.as_ref()?;
                changes.iter().find(|change| &change.hash == hash).cloned()
            })
            .collect()
    }

    /// The relay's `submitblock` log in the fake node's record shape.
    pub fn submissions(&self) -> Vec<SubmissionRecord> {
        let parents = self.parent_heights.lock().expect("parent heights").clone();
        let heights: HashMap<String, u64> = self
            .tip_changes()
            .into_iter()
            .map(|change| (change.hash, change.height))
            .collect();
        self.relay
            .log
            .lock()
            .expect("relay log")
            .submissions
            .iter()
            .map(|submission| SubmissionRecord {
                block_hash: submission.block_hash.clone(),
                parent: submission.parent.clone(),
                height: heights
                    .get(&submission.parent)
                    .or_else(|| parents.get(&submission.parent))
                    .map(|height| height + 1),
                accepted: submission.accepted,
                rejection: submission.verdict.clone(),
                received_at: submission.received_at,
                block_bytes: submission.block_bytes,
            })
            .collect()
    }

    /// What the node's chain says against what the harness recorded, and
    /// every disagreement as a premise contradiction. Call after
    /// [`Qbitd::quiesce`], with the frontends stopped.
    pub async fn chain_reconciliation(&self) -> (Value, Vec<String>) {
        let mut problems = Vec::new();
        let rpc = &self.pool.rpc;
        let best_a = rpc.call("getbestblockhash", json!([])).await;
        let best_b = self.peer.rpc.call("getbestblockhash", json!([])).await;
        let count = rpc.call("getblockcount", json!([])).await;
        let (Ok(best_a), Ok(best_b), Ok(count)) = (best_a, best_b, count) else {
            problems.push("the chain could not be read back from qbitd".to_owned());
            return (json!({"read": false}), problems);
        };
        if best_a != best_b {
            problems.push(format!(
                "pool node and peer ended on different tips ({best_a} and {best_b})"
            ));
        }
        let top = count.as_u64().unwrap_or(0);
        let mints = self.shared.mints.lock().expect("mints").clone();
        let minted: HashSet<String> = mints.iter().filter_map(|mint| mint.hash.clone()).collect();
        let submissions = self
            .relay
            .log
            .lock()
            .expect("relay log")
            .submissions
            .clone();
        let watched: HashSet<String> = self
            .tip_changes()
            .into_iter()
            .map(|change| change.hash)
            .collect();
        for submission in &submissions {
            if watched.contains(&submission.parent) {
                continue;
            }
            // A parent the node does not hold leaves the height unknown.
            if let Ok(header) = rpc.call("getblockheader", json!([submission.parent])).await {
                if let Some(height) = header["height"].as_u64() {
                    self.parent_heights
                        .lock()
                        .expect("parent heights")
                        .insert(submission.parent.clone(), height);
                }
            }
        }
        let accepted: HashSet<String> = submissions
            .iter()
            .filter(|submission| submission.accepted)
            .map(|submission| submission.block_hash.clone())
            .collect();
        let mut chain: Vec<Value> = Vec::new();
        let mut on_chain = HashSet::new();
        let (mut pool_blocks, mut external_blocks) = (0u64, 0u64);
        for height in (self.ramp_tip.1 + 1)..=top {
            let header = match rpc.call("getblockhash", json!([height])).await {
                Ok(hash) => rpc.call("getblockheader", json!([hash])).await,
                Err(error) => Err(error),
            };
            let Ok(header) = header else {
                problems.push(format!("block {height} could not be read back"));
                continue;
            };
            let hash = header["hash"].as_str().unwrap_or_default().to_owned();
            let bits = header["bits"].as_str().unwrap_or_default().to_owned();
            let origin = match (accepted.contains(&hash), minted.contains(&hash)) {
                (true, false) => {
                    pool_blocks += 1;
                    "pool"
                }
                (false, true) => {
                    external_blocks += 1;
                    "external"
                }
                _ => {
                    problems.push(format!(
                        "block {height} ({hash}) is neither one of the peer's mints nor one \
                         accepted relay submission"
                    ));
                    "unattributed"
                }
            };
            if bits != RAMP_BITS {
                problems.push(format!(
                    "block {height} carries bits {bits}, not the ramped {RAMP_BITS}"
                ));
            }
            chain.push(json!({"height": height, "hash": hash, "bits": bits, "origin": origin}));
            on_chain.insert(hash);
        }
        for hash in &accepted {
            if !on_chain.contains(hash) {
                problems.push(format!(
                    "qbitd accepted submitted block {hash} but it is not on the active chain"
                ));
            }
        }
        let mut missing_mints = Vec::new();
        for mint in &mints {
            match (&mint.hash, &mint.error) {
                (Some(hash), _) if !on_chain.contains(hash) => {
                    missing_mints.push(hash.clone());
                    problems.push(format!(
                        "the peer minted {hash} ({:?}) but it is not on the pool node's active chain",
                        mint.purpose
                    ))
                }
                (None, Some(error)) => {
                    problems.push(format!("a {:?} mint failed: {error}", mint.purpose))
                }
                _ => {}
            }
        }
        let requested = self.shared.requested.load(Ordering::SeqCst);
        if (mints.len() as u64) < requested {
            problems.push(format!(
                "{} of {requested} tip(s) asked of the peer were never minted",
                requested - mints.len() as u64
            ));
        }
        // A node's verdict other than acceptance, a duplicate or a valid
        // block off the best chain (`inconclusive`) is a block the pool built
        // wrong: the fake node never says so, and the real one just did.
        for submission in &submissions {
            let verdict = submission.verdict.as_deref().unwrap_or_default();
            if !submission.accepted && !SUBMIT_VERDICTS_NOT_REJECTIONS.contains(&verdict) {
                problems.push(format!(
                    "qbitd rejected submitted block {}: {verdict}",
                    submission.block_hash
                ));
            }
        }
        {
            let log = self.relay.log.lock().expect("relay log");
            if log.unparsed_submissions > 0 {
                problems.push(format!(
                    "{} submitblock request(s) carried no readable block",
                    log.unparsed_submissions
                ));
            }
            for (bits, count) in &log.template_bits {
                if bits != RAMP_BITS {
                    problems.push(format!(
                        "qbitd served {count} template(s) at bits {bits}, not the ramped {RAMP_BITS}"
                    ));
                }
            }
            if !log.transport_errors.is_empty() {
                problems.push(format!(
                    "the relay could not reach qbitd {} time(s)",
                    log.transport_errors.len()
                ));
            }
        }
        let observed = self.shared.tips.lock().expect("tips").len() as u64;
        if observed != chain.len() as u64 {
            problems.push(format!(
                "the watcher saw {observed} tip change(s) for {} block(s) above the ramp",
                chain.len()
            ));
        }
        // What the nodes' links and forks say, when anything disagreed: which
        // of a lost link, a rejected block or a fork it was.
        let diagnosis = if problems.is_empty() {
            Value::Null
        } else {
            self.diagnosis(&missing_mints).await
        };
        (
            json!({
                "read": true,
                "ramp_height": self.ramp_tip.1,
                "tip_height": top,
                "blocks_above_ramp": chain.len(),
                "pool_blocks": pool_blocks,
                "external_blocks": external_blocks,
                "peer_on_same_tip": best_a == best_b,
                "next_retarget_height": NEXT_RETARGET_HEIGHT,
                "blocks": chain,
                "reconciled": problems.is_empty(),
                "problems": problems,
                "diagnosis": diagnosis,
            }),
            problems,
        )
    }

    /// Both nodes' peers and chain tips, and what A knows of each missing
    /// mint, read after a disagreement.
    async fn diagnosis(&self, missing_mints: &[String]) -> Value {
        async fn read(rpc: &Rpc, method: &str, params: Value) -> Value {
            rpc.call(method, params)
                .await
                .unwrap_or_else(|error| json!({"error": format!("{error:#}")}))
        }
        let peers = |info: Value| -> Value {
            info.as_array().map_or(info.clone(), |peers| {
                peers
                    .iter()
                    .map(|peer| {
                        json!({
                            "addr": peer["addr"], "inbound": peer["inbound"],
                            "connection_type": peer["connection_type"],
                            "conntime": peer["conntime"], "lastsend": peer["lastsend"],
                            "lastrecv": peer["lastrecv"], "synced_blocks": peer["synced_blocks"],
                            "synced_headers": peer["synced_headers"],
                        })
                    })
                    .collect()
            })
        };
        let mut known = serde_json::Map::new();
        for hash in missing_mints {
            known.insert(
                hash.clone(),
                read(&self.pool.rpc, "getblockheader", json!([hash])).await,
            );
        }
        json!({
            "read_at": Utc::now().to_rfc3339(),
            "pool_peers": peers(read(&self.pool.rpc, "getpeerinfo", json!([])).await),
            "peer_peers": peers(read(&self.peer.rpc, "getpeerinfo", json!([])).await),
            "pool_chain_tips": read(&self.pool.rpc, "getchaintips", json!([])).await,
            "peer_chain_tips": read(&self.peer.rpc, "getchaintips", json!([])).await,
            "missing_mints_on_pool_node": known,
        })
    }

    /// The real-node keys of the side report's `node` block.
    pub fn report(&self) -> Value {
        let mints = self.shared.mints.lock().expect("mints").clone();
        // When A's watcher saw each tip, not the tip's stamp: the propagation
        // time from B is what this reports.
        let watched: HashMap<String, Instant> = self
            .shared
            .tips
            .lock()
            .expect("tips")
            .iter()
            .map(|tip| (tip.hash.clone(), tip.monotonic))
            .collect();
        let log = self.relay.log.lock().expect("relay log");
        let latency: BTreeMap<&String, Value> = log
            .latencies
            .iter()
            .map(|(method, samples)| (method, latency_summary(samples)))
            .collect();
        let calls: BTreeMap<&String, usize> = log
            .latencies
            .iter()
            .map(|(method, samples)| (method, samples.len()))
            .collect();
        let tips = self.shared.tips.lock().expect("tips");
        json!({
            "mode": "qbitd",
            "qbitd": {
                "binary": self.bin.display().to_string(),
                "binary_sha256": self.bin_sha256,
                "subversion": self.subversion,
                "arguments": "the live_regtest fixtures' launcher plus -legacyretarget and \
                              -peertimeout; pool node -listen=0, peer -listen=1 \
                              -bind=127.0.0.1, peered by addnode",
                "pool_rpc_port": self.pool.rpc_port,
                "peer_p2p_port": self.peer.p2p_port,
                "peak_rss_kib": {
                    "pool": self.pool.peak_rss_kib(),
                    "peer": self.peer.peak_rss_kib(),
                },
            },
            "ramp": self.ramp,
            "external_address": self.external_address,
            "mints": mints.iter().map(|mint| {
                let seen = mint.hash.as_ref().and_then(|hash| watched.get(hash));
                json!({
                    "purpose": mint.purpose,
                    "hash": mint.hash,
                    "requested_at": mint.requested_wall.to_rfc3339(),
                    "mint_milliseconds": mint.minted
                        .map(|at| at.saturating_duration_since(mint.requested).as_secs_f64() * 1000.0),
                    "seen_on_pool_node_milliseconds": seen.zip(mint.minted)
                        .map(|(seen, minted)| seen
                            .saturating_duration_since(minted).as_secs_f64() * 1000.0),
                    "error": mint.error,
                })
            }).collect::<Vec<_>>(),
            "coalesced_tips": tips.iter().filter(|tip| tip.coalesced).count(),
            "relay": {
                "url": self.relay_url,
                "rpc_calls": calls,
                "rpc_latency_milliseconds": latency,
                "template_bits": log.template_bits,
                "transport_errors": log.transport_errors,
                "unparsed_submissions": log.unparsed_submissions,
                "submissions": log.submissions,
            },
        })
    }

    /// Stop the relay, the watchers and both nodes, keep their logs, and
    /// remove the data directories unless asked to keep them.
    pub async fn stop(&self) {
        self.halt();
        self.pool.stop(&self.log_dir).await;
        self.peer.stop(&self.log_dir).await;
        if !self.keep {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

impl Qbitd {
    fn halt(&self) {
        *self.shared.stop_minting.lock().expect("stop minting") = true;
        self.shared.stop.store(true, Ordering::Relaxed);
        for task in self.tasks.lock().expect("tasks").drain(..) {
            task.abort();
        }
    }
}

/// A run that ended before [`Qbitd::stop`] (a cluster that failed to start,
/// say) still stops its tasks, kills both nodes, keeps their logs and
/// removes their data.
impl Drop for Qbitd {
    fn drop(&mut self) {
        self.halt();
        // The nodes die before their directories go, and keep their logs.
        for node in [&self.pool, &self.peer] {
            node.kill();
            node.save_log(&self.log_dir);
        }
        if !self.keep {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

impl crate::node::ExternalMint for Qbitd {
    /// Asks B for a tip and returns at once: B's block reaches A tens of
    /// milliseconds later, and the scheduler that calls this must not stall.
    /// The tip is taken from A's watcher at report time.
    fn mint_external(&self, purpose: MintPurpose) -> Option<TipChange> {
        self.shared.requested.fetch_add(1, Ordering::SeqCst);
        let _ = self.mint_tx.send(MintRecord {
            purpose,
            requested: Instant::now(),
            requested_wall: Utc::now(),
            hash: None,
            minted: None,
            minted_wall: None,
            error: None,
        });
        None
    }

    /// A's tip as its watcher saw it, once every mint asked of B and every
    /// block A accepted from the relay is among the tips the watcher saw:
    /// before then, the tip a frontend serves work on is about to change.
    fn settled_tip(&self) -> Option<String> {
        // A mint that failed is never seen and does not hold the tip; one
        // still in the channel or on B does.
        let minted: Vec<String> = {
            let mints = self.shared.mints.lock().expect("mints");
            if (mints.len() as u64) < self.shared.requested.load(Ordering::SeqCst) {
                return None;
            }
            mints.iter().filter_map(|mint| mint.hash.clone()).collect()
        };
        let accepted: Vec<String> = self
            .relay
            .log
            .lock()
            .expect("relay log")
            .submissions
            .iter()
            .filter(|submission| submission.accepted)
            .map(|submission| submission.block_hash.clone())
            .collect();
        let tips = self.shared.tips.lock().expect("tips");
        let seen: HashSet<&str> = tips.iter().map(|tip| tip.hash.as_str()).collect();
        if minted
            .iter()
            .chain(&accepted)
            .any(|hash| !seen.contains(hash.as_str()))
        {
            return None;
        }
        Some(
            tips.last()
                .map_or_else(|| self.ramp_tip.0.clone(), |tip| tip.hash.clone()),
        )
    }

    fn hold_keepalives(&self, held: bool) {
        let _gate = self.shared.stop_minting.lock().expect("stop minting");
        self.shared.keepalives_held.store(held, Ordering::SeqCst);
    }

    /// The relay records a `submitblock` once A has answered it, and A's
    /// answer to an accepted block comes after A connected it.
    fn block_answered(&self, block_hash: &str) -> bool {
        self.relay
            .log
            .lock()
            .expect("relay log")
            .submissions
            .iter()
            .any(|submission| submission.block_hash == block_hash)
    }
}

fn latency_summary(samples: &[f64]) -> Value {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = |q: f64| -> Value {
        if sorted.is_empty() {
            return Value::Null;
        }
        let index = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
        json!(sorted[index])
    };
    json!({"count": sorted.len(), "p50": rank(0.5), "p99": rank(0.99), "max": sorted.last()})
}

async fn connect(pool: &NodeProcess, peer: &NodeProcess) -> Result<()> {
    let address = format!("127.0.0.1:{}", peer.p2p_port);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let pool_peers = pool.rpc.call("getconnectioncount", json!([])).await?;
        let peer_peers = peer.rpc.call("getconnectioncount", json!([])).await?;
        if pool_peers != 0 && peer_peers != 0 {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "the pool node did not connect to its peer within 30 s"
        );
        if pool_peers == 0 {
            // `onetry` makes one attempt; repeat it until one holds.
            pool.rpc
                .call("addnode", json!([address.as_str(), "onetry"]))
                .await?;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn chain_info(rpc: &Rpc) -> Result<(u64, i64, String)> {
    let info = rpc.call("getblockchaininfo", json!([])).await?;
    Ok((
        info["blocks"].as_u64().context("height missing")?,
        info["time"].as_i64().context("tip time missing")?,
        info["bestblockhash"]
            .as_str()
            .context("best block missing")?
            .to_owned(),
    ))
}

async fn set_mocktime(pool: &Rpc, peer: &Rpc, time: i64) -> Result<()> {
    pool.call("setmocktime", json!([time])).await?;
    peer.call("setmocktime", json!([time])).await?;
    Ok(())
}

async fn generate(rpc: &Rpc, count: u64, address: &str) -> Result<Vec<String>> {
    let hashes = rpc
        .call_timeout(
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

async fn wait_same_tip(pool: &Rpc, peer: &Rpc) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let a = pool.call("getbestblockhash", json!([])).await?;
        let b = peer.call("getbestblockhash", json!([])).await?;
        if a == b {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "the pool node did not reach its peer's tip within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Mine B to [`RAMP_HEIGHT`] under a mock clock shared with A, so the chain
/// ends at wall time, then clear the clocks and check A's template.
async fn ramp(pool: &Rpc, peer: &Rpc, address: &str) -> Result<RampRecord> {
    let started = Instant::now();
    loop {
        let (height, tip_time, _) = chain_info(peer).await?;
        if height >= RAMP_HEIGHT {
            break;
        }
        // Each block is one second past the median of the last eleven while
        // the clock sits below it, so the clock is held back by the chain
        // time the rest of the ramp will consume.
        let remaining = RAMP_HEIGHT - height;
        let behind = (remaining / RAMP_BLOCKS_PER_CHAIN_SECOND) as i64 + 30;
        set_mocktime(pool, peer, (tip_time + 1).max(now_seconds() - behind)).await?;
        generate(peer, remaining.min(RAMP_BATCH), address).await?;
    }
    let mined = Instant::now();
    wait_same_tip(pool, peer).await?;
    let peer_lag_seconds = mined.elapsed().as_secs_f64();
    // A tip left behind the wall clock by more than the minimum-difficulty
    // gap would make the first template trivial, so the chain is walked up
    // to the wall in steps well inside that gap.
    let mut catch_up_blocks = 0;
    loop {
        let (_, tip_time, _) = chain_info(peer).await?;
        if tip_time >= now_seconds() - RAMP_TIP_BEHIND_LIMIT {
            break;
        }
        ensure!(
            catch_up_blocks < RAMP_CATCH_UP_LIMIT,
            "the ramped chain is still behind the wall clock after {catch_up_blocks} blocks"
        );
        set_mocktime(pool, peer, (tip_time + 100).min(now_seconds())).await?;
        generate(peer, 1, address).await?;
        catch_up_blocks += 1;
    }
    wait_same_tip(pool, peer).await?;
    set_mocktime(pool, peer, 0).await?;
    let (height, tip_time, _) = chain_info(pool).await?;
    let ahead = tip_time - now_seconds();
    ensure!(
        ahead <= RAMP_TIP_AHEAD_LIMIT,
        "the ramped chain ended {ahead} s ahead of the wall clock"
    );
    let info = pool.call("getblockchaininfo", json!([])).await?;
    ensure!(
        info["initialblockdownload"] == false,
        "the pool node still reports initial block download after the ramp"
    );
    let template = pool
        .call("getblocktemplate", json!([{"rules": ["segwit"]}]))
        .await?;
    let bits = template["bits"].as_str().unwrap_or_default().to_owned();
    ensure!(
        bits == RAMP_BITS,
        "the ramped chain serves template bits {bits}, not {RAMP_BITS}"
    );
    ensure!(
        height < NEXT_RETARGET_HEIGHT,
        "the ramp overran the epoch: height {height}"
    );
    Ok(RampRecord {
        height,
        bits,
        seconds: started.elapsed().as_secs_f64(),
        catch_up_blocks,
        tip_time_minus_wall_seconds: ahead,
        peer_lag_seconds,
    })
}

/// Stamp every tip change on A. `waitfornewblock` is given the tip last
/// seen, so a change between two calls wakes the next one at once; the
/// heights in between are walked so none is skipped.
async fn watch(rpc: Rpc, start: (String, u64), shared: Arc<Shared>) {
    let (mut tip, mut height) = start;
    let mut failures = 0u32;
    while !shared.stop.load(Ordering::Relaxed) {
        let answer = rpc
            .call_timeout(
                "waitfornewblock",
                json!([WATCH_POLL_MS, tip]),
                Duration::from_millis(WATCH_POLL_MS) + RPC_TIMEOUT,
            )
            .await;
        let monotonic = Instant::now();
        let wall = Utc::now();
        // An answer without a hash and a height is a broken answer, never a
        // tip change: read as one, it would look like a reorganisation.
        let answer =
            answer.and_then(
                |value| match (value["hash"].as_str(), value["height"].as_u64()) {
                    (Some(hash), Some(height)) => Ok((hash.to_owned(), height)),
                    _ => Err(anyhow::anyhow!("waitfornewblock answered {value}")),
                },
            );
        let new_height = match answer {
            Ok((hash, new_height)) if hash != tip => {
                failures = 0;
                new_height
            }
            Ok(_) => {
                failures = 0;
                continue;
            }
            Err(error) => {
                failures += 1;
                if failures >= 5 {
                    shared.lost.store(true, Ordering::SeqCst);
                    shared
                        .failure
                        .lock()
                        .expect("failure")
                        .get_or_insert(format!(
                            "the pool node's tip watcher lost qbitd: {error:#}"
                        ));
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let mut seen = Vec::new();
        let mut parent_matches = true;
        for at in (height + 1)..=new_height {
            let header = match rpc.call("getblockhash", json!([at])).await {
                Ok(hash) => rpc.call("getblockheader", json!([hash])).await,
                Err(error) => Err(error),
            };
            let Ok(header) = header else {
                break;
            };
            if at == height + 1 && header["previousblockhash"].as_str() != Some(tip.as_str()) {
                parent_matches = false;
            }
            seen.push(ObservedTip {
                hash: header["hash"].as_str().unwrap_or_default().to_owned(),
                height: at,
                time: header["time"].as_i64().unwrap_or_default(),
                monotonic,
                wall,
                coalesced: at != new_height,
            });
        }
        if !parent_matches || new_height <= height {
            shared
                .failure
                .lock()
                .expect("failure")
                .get_or_insert(format!(
                    "the pool node reorganised below height {} (phase 1 of #547 plans no reorg)",
                    height + 1
                ));
            return;
        }
        if let Some(last) = seen.last() {
            tip = last.hash.clone();
            height = last.height;
        }
        shared.tips.lock().expect("tips").extend(seen);
    }
}

/// Mint B's tips one at a time, in the order asked.
async fn mint(
    pool: Rpc,
    rpc: Rpc,
    address: String,
    mut requests: mpsc::UnboundedReceiver<MintRecord>,
    shared: Arc<Shared>,
) {
    while let Some(mut record) = requests.recv().await {
        match mint_one(&pool, &rpc, &address).await {
            Ok(hashes) => {
                record.hash = hashes.into_iter().next();
                record.minted = Some(Instant::now());
                record.minted_wall = Some(Utc::now());
            }
            Err(error) => record.error = Some(format!("{error:#}")),
        }
        shared.mints.lock().expect("mints").push(record);
    }
}

/// One block on B. A block timestamped more than the minimum-difficulty gap
/// after its parent would be mined at the regtest limit, and the parent can
/// already be that old (a pool block carries its job's template time), so
/// B's clock is held at [`KEEPALIVE_SECONDS`] past the parent for the one
/// block and released after it.
///
/// B first has to hold A's tip: a mint on B's own tip while a pool block is
/// still on its way from A would fork the chain. A B that does not catch up
/// is a lost link, and the mint fails saying so rather than forking.
async fn mint_one(pool: &Rpc, rpc: &Rpc, address: &str) -> Result<Vec<String>> {
    let deadline = Instant::now() + MINT_SAME_TIP_TIMEOUT;
    loop {
        // Both each time: A can be the one behind (B's last mint in flight).
        let pool_tip = pool.call("getbestblockhash", json!([])).await?;
        let peer_tip = rpc.call("getbestblockhash", json!([])).await?;
        if peer_tip == pool_tip {
            break;
        }
        if Instant::now() >= deadline {
            let pool_peers = pool.call("getconnectioncount", json!([])).await?;
            let peer_peers = rpc.call("getconnectioncount", json!([])).await?;
            bail!(
                "the peer is on {peer_tip}, not the pool node's tip {pool_tip}, after \
                 {MINT_SAME_TIP_TIMEOUT:?} (connections: pool node {pool_peers}, peer {peer_peers})"
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (_, tip_time, _) = chain_info(rpc).await?;
    let latest = tip_time + KEEPALIVE_SECONDS as i64;
    if now_seconds() <= latest {
        return generate(rpc, 1, address).await;
    }
    rpc.call("setmocktime", json!([latest])).await?;
    let mined = generate(rpc, 1, address).await;
    let released = rpc.call("setmocktime", json!([0])).await;
    let mined = mined?;
    released?;
    Ok(mined)
}

/// Ask for a keepalive tip once the tip's own timestamp is
/// [`KEEPALIVE_SECONDS`] old, so no template is served past the
/// minimum-difficulty gap. The tip's timestamp, not when it was seen: a
/// pool block carries its job's template time, which can be tens of
/// seconds behind its landing.
async fn keepalive(requests: mpsc::UnboundedSender<MintRecord>, shared: Arc<Shared>) {
    let mut asked_for: Option<i64> = None;
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        // Held until the request is sent, so `quiesce` cannot close minting
        // between this check and the send and then miss the request.
        let stopped = shared.stop_minting.lock().expect("stop minting");
        if *stopped {
            return;
        }
        if shared.keepalives_held.load(Ordering::SeqCst) {
            continue;
        }
        let (tips, tip_time) = {
            let tips = shared.tips.lock().expect("tips");
            (
                tips.len(),
                tips.last().map_or(shared.ramp_tip_time, |tip| tip.time),
            )
        };
        let key = (tips as i64) << 32 | (tip_time & 0xffff_ffff);
        if now_seconds() >= tip_time + KEEPALIVE_SECONDS as i64 && asked_for != Some(key) {
            asked_for = Some(key);
            shared.requested.fetch_add(1, Ordering::SeqCst);
            let _ = requests.send(MintRecord {
                purpose: MintPurpose::Keepalive,
                requested: Instant::now(),
                requested_wall: Utc::now(),
                hash: None,
                minted: None,
                minted_wall: None,
                error: None,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #638: a keepalive held while a scheduled block is due or outstanding
    /// is neither counted nor asked for, and goes out once released.
    #[tokio::test]
    async fn a_held_keepalive_is_asked_for_only_after_its_release() {
        let shared = Arc::new(Shared {
            ramp_tip_time: now_seconds() - KEEPALIVE_SECONDS as i64 - 1,
            tips: Mutex::new(Vec::new()),
            mints: Mutex::new(Vec::new()),
            failure: Mutex::new(None),
            requested: AtomicU64::new(0),
            stop_minting: Mutex::new(false),
            keepalives_held: AtomicBool::new(true),
            lost: AtomicBool::new(false),
            stop: AtomicBool::new(false),
        });
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(keepalive(tx, shared.clone()));
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert!(rx.try_recv().is_err(), "held: nothing asked for");
        assert_eq!(shared.requested.load(Ordering::SeqCst), 0);
        {
            let _gate = shared.stop_minting.lock().unwrap();
            shared.keepalives_held.store(false, Ordering::SeqCst);
        }
        let record = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("released: asked for")
            .expect("a request");
        assert_eq!(record.purpose, MintPurpose::Keepalive);
        assert_eq!(shared.requested.load(Ordering::SeqCst), 1);
        task.abort();
    }

    #[test]
    fn the_address_encoder_matches_what_qbitd_issued() {
        // `getnewaddress "" p2mr` on qbit 1.0.0 regtest, and the program its
        // `validateaddress` reported.
        let program =
            hex::decode("eaf89558faa83a64005b3babc19b480ea3482ba2f1bd267c23b1c510b83705a9")
                .unwrap();
        assert_eq!(
            witness_address(REGTEST_HRP, 2, &program),
            "qbrt1zatuf2k864qaxgqzm8w4urx6gp635s2az7x7jvlprk8z3pwphqk5s3n9uw6"
        );
    }

    #[test]
    fn the_band_follows_the_window_and_the_rate() {
        assert_eq!(implied_block_interval_seconds(20_000, 50.0), 50.0);
        assert!(check_cadence_band(20_000, &[("steady_state".into(), 50.0)]).is_ok());
        // D1's 500 shares/s over a 20k window lands a block every 5 s.
        assert!(check_cadence_band(20_000, &[("steady_state".into(), 500.0)]).is_err());
        assert!(check_cadence_band(20_000, &[("warm_up".into(), 4.0)]).is_err());
        assert!(check_headroom(1_000).is_ok());
        assert!(check_headroom(1_400).is_err());
    }
}
