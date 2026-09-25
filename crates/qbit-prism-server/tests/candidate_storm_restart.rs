//! Restart mid-drain and a held lease at storm cardinality (#270, workstream A).
//!
//! Incident 3: a frontend that dies between its claim and its terminalization.
//! `crates/qbit-prism-server/src/coordinator/candidate_lease_tests.rs` already
//! covers the in-process form of this — a cancelled task with its lease still
//! live — but an aborted future proves nothing about recovery across a real
//! process death, and #266's one-`submitblock`-per-block-hash guarantee is
//! exactly what an in-process fake cannot test.
//!
//! So this target kills a process. The fake node runs here, in the parent, and
//! counts `submitblock` keyed by block hash, so the counter outlives every
//! child. The child is the real server binary
//! (`env!("CARGO_BIN_EXE_qbit-prism-server")`), configured entirely through
//! `PRISM_*` variables.
//!
//! # The two kill points, both owned by the node
//!
//! A kill point owned by a clock would prove nothing about where the child
//! was, so both of them are places in the child's own call sequence. The node
//! answers a call, records what it learned, and then withholds the reply; the
//! parent is watching the signal it raised and kills the child while it is
//! parked inside that call.
//!
//! - **After the reservation.** The withheld call is the one `submitblock`.
//!   `Ledger::reserve_offer` commits before it, and the child never learns
//!   the outcome, so the row is `offer_reserved` with an unknown offer — the
//!   only durable evidence a crash during that call can leave.
//! - **Before the reservation.** The withheld call is the `getblockhash` of
//!   the pre-offer probe. Only a tip recheck and a readiness proof sit
//!   between it and `Ledger::reserve_offer`, and no database write at all, so
//!   the row is `pending` with a live claim and nothing reserved, and the
//!   recovery has to make the offer itself and record a known outcome.
//!
//! The two are deliberately kept apart: a change that blurred them — a
//! reservation taken earlier, say — moves a row from one scenario's expected
//! state to the other's, and both fail.
//!
//! # Measuring a relaunched process
//!
//! One scenario measures what a relaunched frontend's recovery of a single
//! row costs on the PostgreSQL wire. A relaunched frontend cannot be
//! quiesced: besides the drain it polls for work ten times a second, prunes
//! jobs and blobs every two seconds, publishes health, collects metrics every
//! ten and refreshes the chain, none of which any setting turns off. So that
//! scenario puts `support/ledger_execution_proxy.rs` on the child's wire and
//! attributes executions by statement identity rather than by wall clock, and
//! proves the attribution by requiring its classifier to select nothing at
//! all from an idle window. The parent's own ledger always stays on the
//! direct URL, so nothing the test itself reads is ever observed.
//!
//! The fixture takes its own PostgreSQL database through
//! `support/ledger_database.rs` (#410): the drain holds database-scoped
//! advisory locks, and a killed child leaves its claim behind, so sharing a
//! database would leak this test's contention into every other fixture.
//!
//! Which constructor the child uses matters. #412 added `Ledger::connect_tool`
//! and `Coordinator::new_tool`, a third registration mode that runs every
//! startup gate but writes **no** `starting` heartbeat. A restarted frontend
//! registers a heartbeat; a one-shot tool does not. Be deliberate about which
//! one the test relaunches, and assert what `qbit_prism_instances` is expected
//! to hold afterwards rather than leaving it unstated.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test candidate_storm_restart -- --nocapture
//! ```
//!
//! # What each child is, and what the node is
//!
//! The child relaunches as a **frontend**: `run` reaches
//! `Coordinator::new`, so both lifetimes register the same `PRISM_INSTANCE_ID`
//! in `qbit_prism_instances`, and a `SIGKILL` runs no shutdown path, so the
//! first lifetime's row is still there — never `stopped` — when the second
//! one refreshes it. `Coordinator::new_tool` would have been the wrong choice
//! twice over: a one-shot tool writes no heartbeat at all, so the restart
//! would leave nothing to observe, and `run` is the command an operator
//! actually restarts. The restart test reads `qbit_prism_instances` after the
//! kill and again after the relaunch; the held-lease test reads it while its
//! child is running.
//!
//! The parent's own ledger is the tool connection, `Ledger::connect_tool`, so
//! every row in `qbit_prism_instances` belongs to a child and the assertion
//! above is exact. It also initializes the schema, so the relaunched child
//! (`PRISM_POSTGRES_INIT_SCHEMA` unset, the default) runs no migration and
//! never meets migration 011's quiesce guard, which the killed frontend's
//! surviving row would otherwise refuse.
//!
//! # Why the node serves no block template
//!
//! `Coordinator::refresh_once` reconciles every unfinished pool block against
//! the chain, and that reconciliation bumps the payout revision whenever it
//! confirms a landed block or marks one mature. The drain's own
//! `finish_candidate_at_revision` fences on the revision it observed moments
//! earlier, so a reconciliation that commits in between costs the drain a
//! whole extra claim — the row recovers, and never offers twice, but
//! `attempt_count` is then a race rather than an accounting. The refresh loop
//! is not this target's subject and has its own tests, so this fixture's node
//! answers `getblocktemplate` with an error: `refresh_once` then fails on the
//! template, before it reaches `Coordinator::reconcile`, and the drain is the
//! only writer of the payout revision. Nothing else the drain needs comes
//! from a template — `Coordinator::observe_candidate` reads the chain through
//! `getblockchaininfo`, `getblockhash` and `getbestblockhash` directly — and
//! no miner connects, so no work is ever published.
//!
//! # No timing assertion
//!
//! Every wall clock here is either recorded through `storm_scale::record` and
//! never compared, or a deadline that exists only so a hung child fails the
//! run instead of hanging it. No assertion is on a duration or a rate: the
//! properties are integer counters the test owns — `submitblock` calls per
//! block hash, `attempt_count`, dispatch slots, row states, and statements on
//! the PostgreSQL wire.

use anyhow::{ensure, Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle_with_coinbase_options, verify_audit_bundle_with_ledger_public_key,
    AcceptedShare, FoundBlock, PayoutPolicy,
};
use qbit_prism_server::ledger::{
    Candidate, CandidateState, Ledger, SignerKeys, Snapshot, WindowRef,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    process::{Child, Command},
    sync::{watch, Mutex},
    task::JoinHandle,
};

#[allow(dead_code)]
#[path = "support/storm_scale.rs"]
mod storm_scale;

#[allow(dead_code)]
#[path = "support/ledger_database.rs"]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// The instance ID both lifetimes of the child register under: a restart of
/// one frontend, not the arrival of a second one.
const INSTANCE_ID: &str = "storm-restart-frontend";
/// The parent every candidate header commits to, and the node's tip: the
/// pre-offer staleness screen passes only while the observed tip is this.
const TIP_HASH_BYTE: u8 = 0x22;
/// The tip a decoy node reports before it has served its first block
/// template. Same height and same chainwork as [`TIP_HASH_BYTE`], a different
/// hash: that is what makes a frontend's cached tip hint stale without
/// letting the chain view move. See [`NodePlan::decoy_tip_until_template`].
const DECOY_TIP_BYTE: u8 = 0x33;
/// The node's block height. Every candidate sits at a distinct height at or
/// below it, so `Coordinator::observe_candidate` can prove each one active.
const TIP_HEIGHT: u64 = 900_000;
/// The compact target in the candidate headers and the node's template.
const HEADER_BITS: u32 = 0x207f_ffff;
/// How long the submit loop's own tick is, for the recorded idle-window note.
const SUBMIT_LOOP_TICK: Duration = Duration::from_millis(100);
/// How often the held-lease test renews its claim, well inside the
/// 600-second maximum `Ledger::claim_candidate` and `renew_candidate_claim`
/// accept.
const CLAIM_RENEWAL: Duration = Duration::from_secs(30);
/// How long the held-lease test watches an idle drain. Not an assertion: a
/// shorter window would only observe fewer submit-loop ticks.
const IDLE_OBSERVATION: Duration = Duration::from_secs(2);
/// The coinbase script-sig suffix every sibling is built and rebuilt with:
/// the four-byte extranonce1 and the eight-byte extranonce2 a Stratum job
/// appends, as `candidate_lease_tests.rs` spells it.
const COINBASE_SUFFIX: &str = "000000000000000000000000";
/// The error the fixture's node answers `getblocktemplate` with. See the
/// module docstring: it keeps `refresh_once` short of block reconciliation.
const NO_TEMPLATE: &str = "this fixture's node serves no mining template";

// ---------------------------------------------------------------------------
// The fake node, in the parent, counting `submitblock` per block hash.
// ---------------------------------------------------------------------------

/// What the parent watches the node for. Every field is a count the node
/// owns; `withheld` names the block whose `submitblock` reply the node is
/// holding, and `withheld_probe` the block whose pre-offer `getblockhash`
/// reply it is holding.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct NodeSignal {
    calls: u64,
    submissions: u64,
    templates: u64,
    withheld: Option<String>,
    withheld_probe: Option<String>,
}

/// What the node does beyond answering honestly. Both withholding fields park
/// exactly one reply, so a kill point is a place in the child's own call
/// sequence rather than an instant on a clock.
#[derive(Clone, Copy, Debug, Default)]
struct NodePlan {
    /// The `submitblock` call, counted from the node's first, whose reply is
    /// withheld. The child is then past `Ledger::reserve_offer`, which
    /// commits before that call.
    withhold_submission_at: Option<u64>,
    /// Answer `getblockchaininfo` and `getbestblockhash` with
    /// [`decoy_hash`] until this node has served its first `getblocktemplate`,
    /// and with the real parent on every call after it. The flip is an event
    /// the node owns and signals, never a duration.
    decoy_tip_until_template: bool,
    /// Withhold the reply to the first `getblockhash` at a planned
    /// candidate's height. That call sits inside
    /// `Coordinator::observe_candidate`; only a tip recheck and a readiness
    /// proof follow it before `Ledger::reserve_offer`, and no database write,
    /// so a child parked there has claimed durably and reserved nothing.
    withhold_first_probe: bool,
}

impl NodePlan {
    /// Withhold one `submitblock` reply, as the restart scenario does.
    fn withholding_submission(at: u64) -> Self {
        Self {
            withhold_submission_at: Some(at),
            ..Self::default()
        }
    }

    /// Withhold one pre-offer `getblockhash` reply, and report the decoy tip
    /// that makes a frontend take that probe at all.
    fn withholding_first_probe() -> Self {
        Self {
            decoy_tip_until_template: true,
            withhold_first_probe: true,
            ..Self::default()
        }
    }
}

struct NodeState {
    /// `submitblock` calls per block hash. This is the deliverable counter:
    /// it lives in the parent, so it outlives every child.
    per_hash: BTreeMap<String, u64>,
    /// The height the node reports each planned block at, set before the
    /// first child starts, so a block stays active at its own height for the
    /// rest of the run however many more blocks arrive.
    heights: BTreeMap<String, u64>,
    /// Height to the block this node has accepted at it.
    accepted: BTreeMap<u64, String>,
    signal: NodeSignal,
}

struct NodeShared {
    state: Mutex<NodeState>,
    signal: watch::Sender<NodeSignal>,
    plan: NodePlan,
}

struct FakeNode {
    url: String,
    shared: Arc<NodeShared>,
    signals: watch::Receiver<NodeSignal>,
    task: JoinHandle<()>,
}

impl Drop for FakeNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeNode {
    async fn open(heights: BTreeMap<String, u64>, plan: NodePlan) -> Result<Self> {
        let (signal, signals) = watch::channel(NodeSignal::default());
        let shared = Arc::new(NodeShared {
            state: Mutex::new(NodeState {
                per_hash: BTreeMap::new(),
                heights,
                accepted: BTreeMap::new(),
                signal: NodeSignal::default(),
            }),
            signal,
            plan,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(node_reply))
            .with_state(shared.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            url,
            shared,
            signals,
            task,
        })
    }

    /// The `submitblock` calls this node has answered or withheld, per block
    /// hash. Taken once both children are reaped, so nothing moves under it.
    async fn per_hash(&self) -> BTreeMap<String, u64> {
        self.shared.state.lock().await.per_hash.clone()
    }
}

fn tip_hash() -> String {
    hex::encode([TIP_HASH_BYTE; 32])
}

fn decoy_hash() -> String {
    hex::encode([DECOY_TIP_BYTE; 32])
}

fn genesis_hash() -> String {
    "00".repeat(32)
}

/// The block hash of an assembled block, as the node and the outbox spell it.
fn header_hash(block: &[u8]) -> String {
    let mut hash = Sha256::digest(Sha256::digest(&block[..80])).to_vec();
    hash.reverse();
    hex::encode(hash)
}

async fn node_reply(
    State(node): State<Arc<NodeShared>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let id = request["id"].clone();
    let method = request["method"].as_str().unwrap_or_default().to_owned();
    let mut state = node.state.lock().await;
    state.signal.calls += 1;
    // The tip this node reports right now. Before its first block template a
    // decoy node reports another block of the same height and the same
    // chainwork, so a frontend caches a tip hint its candidates' parent does
    // not match; from that template on it reports the real parent, and the
    // authoritative probe the stale hint provokes then sees a chain that
    // agrees with the cluster's recorded view.
    let reported_tip = match node.plan.decoy_tip_until_template && state.signal.templates == 0 {
        true => decoy_hash(),
        false => tip_hash(),
    };
    let mut withhold = false;
    let reply = match method.as_str() {
        "getblockhash" if request["params"][0] == 0 => ok(&id, json!(genesis_hash())),
        "getblockhash" => {
            let height = request["params"][0].as_u64().unwrap_or_default();
            let hash = state
                .accepted
                .get(&height)
                .cloned()
                .unwrap_or_else(tip_hash);
            // The pre-offer probe's one call about a planned block. Holding
            // its reply parks the child inside
            // `Coordinator::observe_candidate`, which is before the durable
            // reservation; the block whose offer is at stake is the one at
            // this height, and the node learns it by inverting the heights it
            // was given rather than by being told which row the test means.
            if node.plan.withhold_first_probe && state.signal.withheld_probe.is_none() {
                if let Some(planned) = state
                    .heights
                    .iter()
                    .find(|(_, planned_height)| **planned_height == height)
                    .map(|(planned_hash, _)| planned_hash.clone())
                {
                    state.signal.withheld_probe = Some(planned);
                    withhold = true;
                }
            }
            ok(&id, json!(hash))
        }
        "getblockchaininfo" => ok(
            &id,
            json!({"chain":"test","initialblockdownload":false,"blocks":TIP_HEIGHT,
                   "headers":TIP_HEIGHT,"bestblockhash":reported_tip,"chainwork":"01"}),
        ),
        "getbestblockhash" => ok(&id, json!(reported_tip)),
        "getblockheader" => ok(&id, json!({"previousblockhash":"cd".repeat(32)})),
        "getnetworkinfo" => ok(&id, json!({"connections":2})),
        // Deliberate, and the reason is in the module docstring: without a
        // template `refresh_once` never reaches block reconciliation, so the
        // drain is the only writer of the payout revision. Serving it is
        // still an event worth counting: `Coordinator::refresh_once` caches
        // the tip it read strictly before it asks for a template, so this
        // call is the node's proof that a frontend has already cached
        // whatever tip the node reported until now.
        "getblocktemplate" => {
            state.signal.templates += 1;
            rpc_error(&id, -10, NO_TEMPLATE)
        }
        "submitblock" => {
            let Some(block) = request["params"][0]
                .as_str()
                .and_then(|hex| hex::decode(hex).ok())
                .filter(|block| block.len() > 80)
            else {
                return Json(rpc_error(&id, -22, "submitblock parameter is not a block"));
            };
            let hash = header_hash(&block);
            *state.per_hash.entry(hash.clone()).or_default() += 1;
            state.signal.submissions += 1;
            // The node has the block whether or not its caller learns so:
            // recording the acceptance here is what makes the withheld call
            // an unknown outcome rather than a lost block.
            if let Some(height) = state.heights.get(&hash).copied() {
                state.accepted.insert(height, hash.clone());
            }
            if node.plan.withhold_submission_at == Some(state.signal.submissions) {
                state.signal.withheld = Some(hash);
                withhold = true;
            }
            ok(&id, Value::Null)
        }
        other => rpc_error(&id, -32601, &format!("unexpected RPC {other}")),
    };
    let signal = state.signal.clone();
    drop(state);
    node.signal.send_replace(signal);
    if withhold {
        // The reply this call never receives. The parent is watching the
        // signal above and kills the child here, so the child is parked at a
        // place in its own call sequence -- past the durable reservation for
        // a withheld `submitblock`, short of it for a withheld pre-offer
        // `getblockhash` -- and has learned nothing about the outcome.
        std::future::pending::<()>().await;
    }
    Json(reply)
}

fn ok(id: &Value, result: Value) -> Value {
    json!({"id":id,"result":result,"error":null})
}

fn rpc_error(id: &Value, code: i64, message: &str) -> Value {
    json!({"id":id,"result":null,"error":{"code":code,"message":message}})
}

// ---------------------------------------------------------------------------
// The child: the real server binary, configured only through PRISM_*/QBIT_*.
// ---------------------------------------------------------------------------

struct ServerChild {
    child: Child,
    /// The child's stderr, so a deadline that expires names why.
    log: tempfile::NamedTempFile,
}

impl ServerChild {
    /// Spawn `qbit-prism-server run`. Every `PRISM_*` and `QBIT_*` name the
    /// harness might hold is removed first, so the child's configuration is
    /// exactly what is set here and the settings below are traceable from
    /// `src/config/native-settings.txt` to the runtime that reads them.
    fn spawn(database_url: &str, node_url: &str) -> Result<Self> {
        Self::spawn_with(database_url, node_url, &[])
    }

    /// As [`Self::spawn`], with `overrides` applied last so a scenario can
    /// state the settings it depends on beside the reason it depends on them,
    /// instead of every scenario inheriting one of them.
    fn spawn_with(database_url: &str, node_url: &str, overrides: &[(&str, &str)]) -> Result<Self> {
        let log = tempfile::NamedTempFile::new()?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
        for (key, _) in std::env::vars()
            .filter(|(key, _)| key.starts_with("PRISM_") || key.starts_with("QBIT_"))
        {
            command.env_remove(key);
        }
        let ledger_seed = "22".repeat(32);
        command
            .arg("run")
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log.reopen()?))
            .env("RUST_LOG", "warn")
            .env("PRISM_DATABASE_URL", database_url)
            .env("PRISM_INSTANCE_ID", INSTANCE_ID)
            .env("PRISM_DATABASE_MAX_CONNECTIONS", "8")
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("PRISM_JOB_BUILD_EXECUTOR_WORKERS", "2")
            .env("QBIT_CHAIN", "testnet")
            .env("QBIT_RPC_URL", node_url)
            .env("PRISM_MIN_PEERS", "1")
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_MANIFEST_SIGNING_SEED_HEX", "11".repeat(32))
            .env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX", &ledger_seed)
            .env(
                "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX",
                ManifestSigningKey::from_seed_hex(&ledger_seed)?.public_key_hex(),
            )
            // Ephemeral listeners: several fixtures of this suite can run at
            // once, and the defaults are fixed ports.
            .env("PRISM_STRATUM_BIND", "127.0.0.1")
            .env("PRISM_STRATUM_PORT", "0")
            .env("PRISM_AUDIT_PORT", "0")
            // The nondefault settings this target depends on, each observed
            // in the child's behaviour rather than assumed (EP-CONFIG):
            // a `submitblock` the node never answers must not time out
            // before the parent kills the child, or the killed row would be
            // `offered` with an unknown outcome instead of `offer_reserved`.
            .env("PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS", "600")
            // Nothing here long-polls a node that has no new blocks, and no
            // share rollup runs in a fixture with one share.
            .env("PRISM_BLOCKWAIT_ENABLED", "0")
            .env("PRISM_HASHRATE_ROLLUP_ENABLED", "0")
            .env("PRISM_BLOCKPOLL_SECONDS", "5");
        for (key, value) in overrides {
            command.env(key, value);
        }
        Ok(Self {
            child: command.spawn().context("spawning the server binary")?,
            log,
        })
    }

    /// `SIGKILL`, then reap. `tokio::process::Child::start_kill` sends
    /// `SIGKILL`, which no shutdown path can catch: a graceful stop is a
    /// different scenario with its own test. `kill_on_drop` repeats this for
    /// every path that leaves without reaching here, a failing assertion and
    /// a panic included, so no child outlives the test.
    async fn kill(&mut self) -> Result<()> {
        self.child.start_kill().context("signalling the child")?;
        self.child.wait().await.context("reaping the child")?;
        Ok(())
    }

    /// The tail of the child's stderr, for a deadline that expired.
    fn stderr_tail(&self) -> String {
        let bytes = std::fs::read(self.log.path()).unwrap_or_default();
        let from = bytes.len().saturating_sub(4096);
        String::from_utf8_lossy(&bytes[from..]).into_owned()
    }
}

// ---------------------------------------------------------------------------
// The candidates.
// ---------------------------------------------------------------------------

fn keys() -> Result<(ManifestSigningKey, ManifestSigningKey)> {
    Ok((
        ManifestSigningKey::from_seed_hex(&"11".repeat(32))?,
        ManifestSigningKey::from_seed_hex(&"22".repeat(32))?,
    ))
}

fn share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("storm:{id:064x}"),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: TIP_HEIGHT,
        job_id: "storm-job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

/// One sibling: the slim candidate, its block hash and the height the node
/// will report it at.
struct Planned {
    candidate: Candidate,
    block_hash: String,
    height: u64,
}

/// `count` siblings of one window, each at its own height at or below the
/// node's tip and each committing to the node's tip as its parent.
///
/// Two choices here decide what the drain does, and both are the point.
///
/// The siblings are **leased** (#273): `Coordinator::offer_candidate` skips
/// the pre-offer staleness screen for a leased candidate, so every one of
/// them reaches its one `submitblock` call. Without the lease the first
/// confirmation would move the payout revision and the screen would abandon
/// every remaining sibling before it was ever offered — the supersession
/// scenario, which belongs to `candidate_storm.rs`, and which would leave
/// this target's per-hash counter with one sample instead of `count`.
///
/// The heights are **distinct**, so a sibling stays active at its own height
/// for the rest of the run and each row reaches a terminal state on the one
/// claim that offered it. That is what makes `SUM(attempt_count)` an
/// accounting rather than a race.
///
/// The signing keys are the child's: `Coordinator::stored_inputs_mismatch`
/// refuses to rebuild a candidate signed with any other pair.
fn plan(snapshot: &Snapshot, count: usize) -> Result<Vec<Planned>> {
    let (manifest_key, ledger_key) = keys()?;
    let first_height = TIP_HEIGHT
        .checked_sub(count as u64)
        .context("the storm cardinality does not fit below the node's tip")?
        + 1;
    let window = WindowRef::from_snapshot(snapshot)?;
    (0..count)
        .map(|index| {
            let height = first_height + index as u64;
            // The coinbase options are the ones `build_claim_parts` rebuilds
            // with: the stored suffix, and the witness leaves a coinbase-only
            // block yields, which is none. A bundle built without them lands
            // once from its own parts and never survives a rebuild, and every
            // landing after an offer is a rebuild.
            let bundle = build_audit_bundle_with_coinbase_options(
                snapshot.shares.clone(),
                FoundBlock {
                    block_height: height,
                    coinbase_value_sats: 500_000_000,
                    network_difficulty: 100,
                    anchor_job_issued_at_ms: snapshot.anchor_ms,
                },
                snapshot.prior_balances.clone(),
                PayoutPolicy::day_one_default(),
                Some(COINBASE_SUFFIX.to_owned()),
                Vec::new(),
                &manifest_key,
                &ledger_key,
            )?;
            let report =
                verify_audit_bundle_with_ledger_public_key(&bundle, &ledger_key.public_key_hex())?;
            let mut block = vec![0u8; 80];
            block[..4].copy_from_slice(&0x2000_0000u32.to_le_bytes());
            block[4..36].fill(TIP_HASH_BYTE);
            let mut txid = hex::decode(&report.coinbase_txid)?;
            txid.reverse();
            block[36..68].copy_from_slice(&txid);
            block[68..72].copy_from_slice(&1_800_000_000u32.to_le_bytes());
            block[72..76].copy_from_slice(&HEADER_BITS.to_le_bytes());
            block[76..80].copy_from_slice(&(index as u32).to_le_bytes());
            let block_hash = header_hash(&block);
            block.push(1);
            block.extend(hex::decode(&report.coinbase_tx_hex)?);
            Ok(Planned {
                candidate: Candidate {
                    block_hash: block_hash.clone(),
                    block_sha256: Candidate::block_digest_hex(&block),
                    job_id: "storm-job".into(),
                    payout_revision: snapshot.payout_revision,
                    window,
                    bootstrap_share: None,
                    found_block: bundle.found_block.clone(),
                    payout_policy: bundle.payout_policy.clone(),
                    ctv: None,
                    audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
                    signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
                    leased: true,
                    coinbase_suffix_hex: COINBASE_SUFFIX.to_owned(),
                    deferred_share: None,
                    block_bytes: block,
                    as_issued_balances: snapshot.prior_balances.clone(),
                },
                block_hash,
                height,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Reading the outbox and the instance table.
// ---------------------------------------------------------------------------

/// The lifecycle of one outbox row, as the database holds it. `offer_reserved_at`
/// and `claim_instance_id` are read as well as their `_by`/`_token` partners so a
/// scenario that asserts a reservation was never taken can say so about both
/// columns migration 011 added, rather than about one of them.
#[derive(Debug)]
struct Row {
    state: String,
    attempt_count: i32,
    offer_reserved_by: Option<String>,
    offer_reserved_at: Option<f64>,
    offered_at_ms: Option<i64>,
    offer_outcome: Option<String>,
    claim_live: bool,
    claim_token: Option<String>,
    claim_instance_id: Option<String>,
    /// The row still carries the body and the block a recovery needs:
    /// terminalization releases both, so this distinguishes an unfinished row
    /// from one that reached a terminal state.
    payload_present: bool,
}

/// `Row`'s columns, in the order the statement below selects them.
type RowColumns = (
    String,
    i32,
    Option<String>,
    Option<f64>,
    Option<i64>,
    Option<String>,
    bool,
    Option<String>,
    Option<String>,
    bool,
);

async fn row(ledger: &Ledger, block_hash: &str) -> Result<Row> {
    let columns: RowColumns =
        sqlx::query_as("SELECT state,attempt_count,offer_reserved_by,extract(epoch FROM offer_reserved_at)::float8,offered_at_ms,offer_outcome,COALESCE(claim_expires_at>clock_timestamp(),false),claim_token,claim_instance_id,(candidate IS NOT NULL AND block_bytes IS NOT NULL) FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(block_hash)
            .fetch_one(&ledger.pool)
            .await
            .with_context(|| format!("outbox row {block_hash}"))?;
    Ok(Row {
        state: columns.0,
        attempt_count: columns.1,
        offer_reserved_by: columns.2,
        offer_reserved_at: columns.3,
        offered_at_ms: columns.4,
        offer_outcome: columns.5,
        claim_live: columns.6,
        claim_token: columns.7,
        claim_instance_id: columns.8,
        payload_present: columns.9,
    })
}

/// The digest of the block bytes one row still holds, so a scenario can prove
/// a killed row's payload is the one it enqueued rather than only that some
/// payload is present. It is compared with `Candidate::block_sha256`, which is
/// the same SHA-256 over the same bytes.
async fn stored_block_digest(ledger: &Ledger, block_hash: &str) -> Result<String> {
    Ok(sqlx::query_scalar(
        "SELECT encode(sha256(block_bytes),'hex') FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(block_hash)
    .fetch_one(&ledger.pool)
    .await?)
}

/// The largest `attempt_count` any row other than `block_hash` carries.
async fn other_attempts(ledger: &Ledger, block_hash: &str) -> Result<i32> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE(max(attempt_count),0) FROM qbit_block_candidate_outbox WHERE block_hash<>$1",
    )
    .bind(block_hash)
    .fetch_one(&ledger.pool)
    .await?)
}

/// Hold every row but `keep` out of the due set, and release them again.
///
/// Both lanes of `Ledger::claim_candidate` and its due-work probe select on
/// `next_attempt_at<=clock_timestamp()`, so this decides which rows a running
/// drain can see without touching a state, a claim or an attempt count. It is
/// the same kind of fixture statement as the lease expiry below: the test owns
/// when work becomes available, and the server owns everything it then does
/// with it.
async fn hold_all_but(ledger: &Ledger, keep: &str) -> Result<u64> {
    Ok(sqlx::query(
        "UPDATE qbit_block_candidate_outbox SET next_attempt_at='infinity' WHERE block_hash<>$1",
    )
    .bind(keep)
    .execute(&ledger.pool)
    .await?
    .rows_affected())
}

async fn release_all_but(ledger: &Ledger, keep: &str) -> Result<u64> {
    Ok(sqlx::query(
        "UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash<>$1",
    )
    .bind(keep)
    .execute(&ledger.pool)
    .await?
    .rows_affected())
}

/// Expire a dead owner's claim, fenced on the token that owner held, so the
/// statement can only ever hit that claim and never a replacement's.
///
/// The owner is provably dead: it was `SIGKILL`ed and reaped before this runs.
/// `CANDIDATE_LEASE` is a compile-time 120 seconds with no setting to shorten
/// it, and waiting it out would make every scenario here two minutes longer
/// without proving anything the fence does not.
async fn expire_claim(ledger: &Ledger, block_hash: &str, claim_token: Option<&str>) -> Result<()> {
    let expired = sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE block_hash=$1 AND claim_token=$2 AND claim_expires_at>clock_timestamp()")
        .bind(block_hash)
        .bind(claim_token)
        .execute(&ledger.pool)
        .await?
        .rows_affected();
    ensure!(
        expired == 1,
        "the dead owner's lease was not the one expired"
    );
    Ok(())
}

/// Rows by state, as `state=count` pairs, for a message that says what the
/// outbox actually holds.
async fn states(ledger: &Ledger) -> Result<String> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT state,count(*) FROM qbit_block_candidate_outbox GROUP BY state ORDER BY state",
    )
    .fetch_all(&ledger.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(state, count)| format!("{state}={count}"))
        .collect::<Vec<_>>()
        .join(" "))
}

async fn submitted(ledger: &Ledger) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state='submitted'",
    )
    .fetch_one(&ledger.pool)
    .await?)
}

async fn attempts(ledger: &Ledger) -> Result<i64> {
    Ok(
        sqlx::query_scalar(
            "SELECT COALESCE(sum(attempt_count),0) FROM qbit_block_candidate_outbox",
        )
        .fetch_one(&ledger.pool)
        .await?,
    )
}

/// The dispatch slots the drain has consumed. One `nextval` is taken per
/// claim transaction that found due work, and empty polling takes none, so
/// this is the drain's offers, never its wakeups.
async fn dispatch_slots(ledger: &Ledger) -> Result<i64> {
    let (last, called): (i64, bool) =
        sqlx::query_as("SELECT last_value,is_called FROM qbit_prism_candidate_dispatch_sequence")
            .fetch_one(&ledger.pool)
            .await?;
    Ok(if called { last } else { 0 })
}

/// Every `qbit_prism_instances` row as `(instance_id, state, heartbeat_at)`,
/// where `state` is the heartbeat's lifecycle marker and `None` for a health
/// payload, and the heartbeat is UNIX seconds so two samples compare as
/// numbers rather than as rendered timestamps.
async fn instances(ledger: &Ledger) -> Result<Vec<(String, Option<String>, f64)>> {
    Ok(sqlx::query_as(
        "SELECT instance_id,status->>'state',extract(epoch FROM heartbeat_at)::float8 FROM qbit_prism_instances ORDER BY instance_id",
    )
    .fetch_all(&ledger.pool)
    .await?)
}

/// The child registered exactly one frontend row, under the configured
/// instance ID, and it is not a shutdown marker: #412's tool registration
/// would have left no row at all, and a `SIGKILL` retracts nothing. Returns
/// the row's heartbeat time, so a caller can prove a relaunch refreshed it.
async fn one_live_frontend(ledger: &Ledger, when: &str) -> Result<f64> {
    let rows = instances(ledger).await?;
    ensure!(
        rows.len() == 1 && rows[0].0 == INSTANCE_ID,
        "{when}: qbit_prism_instances holds {rows:?}, not one {INSTANCE_ID} frontend row"
    );
    ensure!(
        rows[0].1.as_deref() != Some("stopped"),
        "{when}: the frontend row reports a shutdown it never performed"
    );
    Ok(rows[0].2)
}

// ---------------------------------------------------------------------------
// Bounded waits. None of them asserts a duration: each decides only when a
// counter is read, and a deadline that expires fails the run with the child's
// own stderr instead of hanging it.
// ---------------------------------------------------------------------------

/// The hang guard for one wait. It is deliberately far above any observed
/// drain — the reduced-cardinality job drains a hundred siblings in seconds —
/// because its only job is to fail a wedged run with the child's stderr
/// instead of hanging the suite. Nothing compares an elapsed time to it to
/// decide whether a property holds.
fn deadline(count: usize) -> Duration {
    Duration::from_secs(120) + Duration::from_millis(500) * count as u32
}

async fn wait_for_signal<T: Clone>(
    signals: &mut watch::Receiver<NodeSignal>,
    child: &ServerChild,
    count: usize,
    what: &str,
    ready: impl Fn(&NodeSignal) -> Option<T>,
) -> Result<T> {
    let observed = tokio::time::timeout(deadline(count), signals.wait_for(|s| ready(s).is_some()))
        .await
        .with_context(|| {
            format!(
                "the node did not observe {what} within {:?}; child stderr:\n{}",
                deadline(count),
                child.stderr_tail()
            )
        })?
        .context("the fake node stopped")?;
    ready(&observed).context("the node signal changed under the wait")
}

/// Wait until `expected` outbox rows are `submitted`. The node's own signal
/// says when every block has been offered, but the landing and the
/// confirmation that follow each offer are database work the node never sees,
/// so this one waits on the outbox instead.
async fn wait_for_drain(
    ledger: &Ledger,
    child: &ServerChild,
    count: usize,
    expected: i64,
) -> Result<()> {
    let bound = deadline(count);
    let started = Instant::now();
    loop {
        let terminal = submitted(ledger).await?;
        if terminal >= expected {
            return Ok(());
        }
        ensure!(
            started.elapsed() < bound,
            "only {terminal} of {expected} rows reached a terminal state within {bound:?} ({}); child stderr:\n{}",
            states(ledger).await?,
            child.stderr_tail()
        );
        tokio::time::sleep(SUBMIT_LOOP_TICK).await;
    }
}

// ---------------------------------------------------------------------------
// The fixture: one PostgreSQL database, one tool ledger, one window.
// ---------------------------------------------------------------------------

/// Establish the cluster's chain view and seed the one window every sibling
/// references.
///
/// The chain view is established here, before the snapshot is taken, because
/// `Ledger::observe_chain_view` bumps the payout revision the first time it
/// sees a view: doing it in the parent means the child's first observation
/// finds the view already recorded and the revision the candidates carry is
/// the one the drain starts from.
async fn seed(ledger: &Ledger) -> Result<Snapshot> {
    ledger
        .observe_chain_view(&tip_hash(), TIP_HEIGHT, "01")
        .await?;
    ledger.append(share(1), None).await?;
    ledger.snapshot(100).await
}

fn heights(planned: &[Planned]) -> BTreeMap<String, u64> {
    planned
        .iter()
        .map(|row| (row.block_hash.clone(), row.height))
        .collect()
}

/// Every planned sibling was offered exactly once, and the node saw no block
/// this test did not plan. This is the target's deliverable.
async fn assert_one_offer_per_hash(node: &FakeNode, planned: &[Planned]) -> Result<u64> {
    let per_hash = node.per_hash().await;
    let repeated: Vec<_> = per_hash
        .iter()
        .filter(|(_, calls)| **calls != 1)
        .map(|(hash, calls)| format!("{hash}={calls}"))
        .collect();
    ensure!(
        repeated.is_empty(),
        "the node was offered a block more than once: {}",
        repeated.join(" ")
    );
    let planned_hashes: std::collections::BTreeSet<&str> =
        planned.iter().map(|row| row.block_hash.as_str()).collect();
    let unplanned: Vec<_> = per_hash
        .keys()
        .filter(|hash| !planned_hashes.contains(hash.as_str()))
        .cloned()
        .collect();
    ensure!(
        unplanned.is_empty(),
        "the node was offered blocks this test never enqueued: {unplanned:?}"
    );
    Ok(per_hash.values().sum())
}

// ---------------------------------------------------------------------------
// The restart.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_between_claim_and_terminalization_offers_every_block_exactly_once() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let count = storm_scale::storm_candidates()?;
    let database = FixtureDatabase::open(&raw, "prism_storm_restart_").await?;
    let ledger =
        match Ledger::connect_tool(&database.url, "storm-restart-fixture".into(), 4, true, None)
            .await
        {
            Ok(ledger) => ledger,
            Err(error) => return Err(database.abandon(error).await),
        };
    let outcome = restart(&database, &ledger, count).await;
    ledger.pool.close().await;
    database.close(outcome).await
}

async fn restart(database: &FixtureDatabase, ledger: &Ledger, count: usize) -> Result<()> {
    let snapshot = seed(ledger).await?;
    let plan_started = Instant::now();
    let planned = plan(&snapshot, count)?;
    let planned_in = plan_started.elapsed();
    let enqueue_started = Instant::now();
    for row in &planned {
        ledger.enqueue_candidate(row.candidate.clone()).await?;
    }
    let enqueued = enqueue_started.elapsed();

    // The kill lands mid-drain, once per test whatever the cardinality: the
    // node withholds the reply to this many-th `submitblock` call.
    let kill_at = (count as u64).div_ceil(2);
    let node = FakeNode::open(heights(&planned), NodePlan::withholding_submission(kill_at)).await?;
    let mut signals = node.signals.clone();

    let first_started = Instant::now();
    let mut child = ServerChild::spawn(&database.url, &node.url)?;
    // The node has recorded this block and is holding its reply: the child is
    // past `Ledger::reserve_offer`, which commits before the one
    // `submitblock` call, and has learned nothing about its outcome. Nothing
    // here is a timer, a sleep or a poll of the child.
    let killed_hash = wait_for_signal(&mut signals, &child, count, "the withheld offer", |s| {
        s.withheld.clone()
    })
    .await?;
    let first_lifetime = first_started.elapsed();
    let calls_before_restart = signals.borrow().calls;
    child.kill().await?;

    let heartbeat_before_restart = one_live_frontend(ledger, "after the kill").await?;
    let killed = row(ledger, &killed_hash).await?;
    ensure!(
        killed.state == CandidateState::OfferReserved.as_str(),
        "the killed block is {} and not {}: the reservation is the only durable evidence a crash during the one call can leave",
        killed.state,
        CandidateState::OfferReserved.as_str()
    );
    ensure!(
        killed.offer_reserved_by.as_deref() == Some(INSTANCE_ID),
        "the reservation names {:?}, not the killed frontend",
        killed.offer_reserved_by
    );
    ensure!(
        killed.offered_at_ms.is_none() && killed.offer_outcome.is_none(),
        "the killed frontend recorded an outcome it never learned: {killed:?}"
    );
    ensure!(
        killed.attempt_count == 1,
        "the killed block was claimed {} times before the kill",
        killed.attempt_count
    );
    ensure!(
        killed.claim_live,
        "the killed frontend's claim did not outlive it; a dead owner's lease is what the recovery has to wait out"
    );
    let recovered: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state IN {}",
        CandidateState::UNFINISHED_SQL
    ))
    .fetch_one(&ledger.pool)
    .await?;

    // The owner is provably dead: it was `SIGKILL`ed and reaped above, and
    // the node never answered its call. Expiring that lease is the owner-loss
    // recovery `ledger_postgres::candidate_outbox_is_atomic_and_claims_recover_after_owner_loss`
    // models, not a state this test then asserts: the reservation, the offer
    // record and every `submitblock` count come through the real path.
    expire_claim(ledger, &killed_hash, killed.claim_token.as_deref()).await?;

    let submissions_before_restart = signals.borrow().submissions;
    let restart_started = Instant::now();
    let mut child = ServerChild::spawn(&database.url, &node.url)?;
    wait_for_signal(
        &mut signals,
        &child,
        count,
        "the relaunched frontend's first node call",
        |s| (s.calls > calls_before_restart).then_some(()),
    )
    .await?;
    let first_node_call = restart_started.elapsed();
    wait_for_signal(
        &mut signals,
        &child,
        count,
        "the relaunched frontend's first offer",
        |s| (s.submissions > submissions_before_restart).then_some(()),
    )
    .await?;
    let first_offer = restart_started.elapsed();

    // Every block is offered, across both lifetimes, exactly `count` times in
    // total: the killed one was offered by the first lifetime and must not be
    // offered by the second.
    wait_for_signal(&mut signals, &child, count, "every offer", |s| {
        (s.submissions >= count as u64).then_some(())
    })
    .await?;
    wait_for_drain(ledger, &child, count, count as i64).await?;
    let second_lifetime = restart_started.elapsed();
    // Reap before reading the counters, so nothing moves under the assertions.
    child.kill().await?;

    // The relaunch registered a heartbeat of its own on the row the killed
    // lifetime left behind: one frontend row, never `stopped`, refreshed.
    // `Coordinator::new_tool` writes none, so this is where the difference
    // #412 introduced would show.
    let heartbeat_after_restart = one_live_frontend(ledger, "after the relaunch").await?;
    ensure!(
        heartbeat_after_restart > heartbeat_before_restart,
        "the relaunched frontend did not register a heartbeat: the row still reads {heartbeat_before_restart}"
    );

    let offers = assert_one_offer_per_hash(&node, &planned).await?;
    ensure!(
        offers == count as u64,
        "{offers} submitblock calls for {count} blocks"
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_block_candidate_outbox")
        .fetch_one(&ledger.pool)
        .await?;
    ensure!(
        rows == count as i64,
        "the outbox holds {rows} rows for {count} enqueued blocks; a restart lost one"
    );
    ensure!(
        submitted(ledger).await? == count as i64,
        "not every block reached a terminal state: {}",
        states(ledger).await?
    );
    // One claim per block, plus the one claim that recovered the killed row.
    // Nothing else was ever claimed twice, so nothing was processed twice.
    let attempts = attempts(ledger).await?;
    ensure!(
        attempts == count as i64 + 1,
        "the drain took {attempts} claims for {count} blocks and one recovery; {}",
        states(ledger).await?
    );
    let recovered_row = row(ledger, &killed_hash).await?;
    ensure!(
        recovered_row.attempt_count == 2,
        "the killed block was claimed {} times, not once before the kill and once to recover it",
        recovered_row.attempt_count
    );
    ensure!(
        recovered_row.offered_at_ms.is_none(),
        "the recovery recorded an offer time for a call whose outcome was never known"
    );
    ensure!(
        recovered_row.offer_reserved_by.as_deref() == Some(INSTANCE_ID),
        "the recovery discarded the reservation's evidence"
    );
    // The reservation is what proves the recovery never offered again: every
    // other row records the wall clock of its own one call, and this one
    // cannot, because its outcome was lost with the process that made it.
    let without_outcome: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_block_candidate_outbox WHERE offer_reserved_by IS NOT NULL AND offered_at_ms IS NULL",
    )
    .fetch_one(&ledger.pool)
    .await?;
    ensure!(
        without_outcome == 1,
        "{without_outcome} rows carry a reservation without an offer outcome; exactly one frontend was killed"
    );

    storm_scale::record(
        "candidate_storm_restart",
        &[
            ("candidates", count.to_string()),
            ("kill_at_submission", kill_at.to_string()),
            ("plan_seconds", format!("{:.3}", planned_in.as_secs_f64())),
            ("enqueue_seconds", format!("{:.3}", enqueued.as_secs_f64())),
            (
                "first_lifetime_drain_seconds",
                format!("{:.3}", first_lifetime.as_secs_f64()),
            ),
            (
                "restart_to_first_node_call_seconds",
                format!("{:.3}", first_node_call.as_secs_f64()),
            ),
            (
                "restart_to_first_offer_seconds",
                format!("{:.3}", first_offer.as_secs_f64()),
            ),
            (
                "second_lifetime_drain_seconds",
                format!("{:.3}", second_lifetime.as_secs_f64()),
            ),
            ("rows_recovered_on_restart", recovered.to_string()),
            ("submitblock_calls", offers.to_string()),
            ("claims", attempts.to_string()),
            ("dispatch_slots", dispatch_slots(ledger).await?.to_string()),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The held lease.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_candidate_lease_costs_the_drain_an_offer_and_never_a_wakeup() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let count = storm_scale::storm_candidates()?;
    let database = FixtureDatabase::open(&raw, "prism_storm_lease_").await?;
    let ledger = match Ledger::connect_tool(
        &database.url,
        "storm-lease-fixture".into(),
        4,
        true,
        None,
    )
    .await
    {
        Ok(ledger) => ledger,
        Err(error) => return Err(database.abandon(error).await),
    };
    let outcome = held_lease(&database, &ledger, count).await;
    ledger.pool.close().await;
    database.close(outcome).await
}

async fn held_lease(database: &FixtureDatabase, ledger: &Ledger, count: usize) -> Result<()> {
    let snapshot = seed(ledger).await?;
    let plan_started = Instant::now();
    let planned = plan(&snapshot, count)?;
    let planned_in = plan_started.elapsed();
    let (held_plan, rest) = planned.split_first().context("no siblings planned")?;

    // The held sibling is enqueued alone and claimed while it is the only row
    // in the outbox, so the claim is certainly on it whichever lane serves it.
    ledger
        .enqueue_candidate(held_plan.candidate.clone())
        .await?;
    let held = ledger
        .claim_candidate(600)
        .await?
        .context("the sibling to hold was not claimable")?;
    ensure!(
        held.candidate.block_hash == held_plan.block_hash,
        "the held claim is on another block"
    );
    let enqueue_started = Instant::now();
    for row in rest {
        ledger.enqueue_candidate(row.candidate.clone()).await?;
    }
    let enqueued = enqueue_started.elapsed();
    let siblings = rest.len() as i64;
    // A held lease is a live one: a frontend that holds a claim renews it
    // while it works, and a drain of tens of thousands of siblings outlasts
    // the 600-second maximum a single `claim_candidate` can take. Renewal
    // touches the lease columns only -- `attempt_count` stays this test's one
    // claim -- and a renewal that fails means the drain took the row, which
    // the assertions below then name.
    // Cancel renewal on every return path, including a failed child spawn or
    // assertion, so fixture cleanup never races a detached lease holder.
    let renewal = tokio_util::task::AbortOnDropHandle::new(tokio::spawn({
        let ledger = ledger.clone();
        let held = held.clone();
        async move {
            let mut tick = tokio::time::interval(CLAIM_RENEWAL);
            loop {
                tick.tick().await;
                if ledger.renew_candidate_claim(&held, 600).await.is_err() {
                    break;
                }
            }
        }
    }));

    let node = FakeNode::open(heights(&planned), NodePlan::default()).await?;
    let mut signals = node.signals.clone();
    let drain_started = Instant::now();
    let mut child = ServerChild::spawn(&database.url, &node.url)?;
    wait_for_signal(&mut signals, &child, count, "every offer", |s| {
        (s.submissions >= siblings as u64).then_some(())
    })
    .await?;
    wait_for_drain(ledger, &child, count, siblings).await?;
    let drain = drain_started.elapsed();
    one_live_frontend(ledger, "while the lease is held").await?;

    // One dispatch slot per claim, and the drain took one claim per sibling
    // it could reach: the held row is not among them.
    let after_drain = dispatch_slots(ledger).await?;
    ensure!(
        after_drain == count as i64,
        "the drain consumed {after_drain} dispatch slots for {siblings} offers and this test's one claim"
    );
    // The submit loop keeps ticking with nothing claimable. The due-work
    // probe consumes a slot only while some unfinished row is due and
    // unclaimed, so a held lease costs the drain the offer it cannot make and
    // never a wakeup. A shorter window would only observe fewer ticks; this
    // is an observation, not a deadline.
    tokio::time::sleep(IDLE_OBSERVATION).await;
    let after_idle = dispatch_slots(ledger).await?;
    ensure!(
        after_idle == after_drain,
        "an idle drain budgeted {} wakeups against the dispatch sequence while one lease was held",
        after_idle - after_drain
    );
    child.kill().await?;
    // Stop renewing, so the lease the assertions read is the one the last
    // renewal wrote and nothing changes under them.
    renewal.abort();

    // The held row cost the node nothing and the outbox nothing.
    let per_hash = node.per_hash().await;
    ensure!(
        !per_hash.contains_key(&held_plan.block_hash),
        "the held block was offered while this test's claim was live"
    );
    let offers = assert_one_offer_per_hash(&node, rest).await?;
    ensure!(
        offers == siblings as u64,
        "{offers} submitblock calls for {siblings} claimable blocks"
    );
    let row = row(ledger, &held_plan.block_hash).await?;
    ensure!(
        row.state == CandidateState::Pending.as_str(),
        "the held block is {}: a leased row is neither abandoned nor re-offered",
        row.state
    );
    ensure!(
        row.claim_token.as_deref() == Some(held.claim_token.as_str()) && row.claim_live,
        "the drain took the held claim away: {row:?}"
    );
    ensure!(
        row.offer_reserved_by.is_none() && row.offered_at_ms.is_none(),
        "the held block carries offer evidence: {row:?}"
    );
    ensure!(
        row.attempt_count == 1,
        "the held block was claimed {} times; only this test claimed it",
        row.attempt_count
    );
    ensure!(
        submitted(ledger).await? == siblings,
        "the held lease stalled the rest of the drain: {}",
        states(ledger).await?
    );
    let attempts = attempts(ledger).await?;
    ensure!(
        attempts == count as i64,
        "the drain took {attempts} claims for {siblings} offers and one held row"
    );

    storm_scale::record(
        "candidate_storm_held_lease",
        &[
            ("candidates", count.to_string()),
            ("offered_siblings", siblings.to_string()),
            ("plan_seconds", format!("{:.3}", planned_in.as_secs_f64())),
            ("enqueue_seconds", format!("{:.3}", enqueued.as_secs_f64())),
            ("drain_seconds", format!("{:.3}", drain.as_secs_f64())),
            ("submitblock_calls", offers.to_string()),
            ("claims", attempts.to_string()),
            ("dispatch_slots", after_drain.to_string()),
            (
                "idle_submit_loop_ticks",
                (IDLE_OBSERVATION.as_millis() / SUBMIT_LOOP_TICK.as_millis()).to_string(),
            ),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The kill before the first offer.
// ---------------------------------------------------------------------------

/// A frontend `SIGKILL`ed after it durably claimed a candidate and **before**
/// `Ledger::reserve_offer` committed leaves a row the relaunched frontend
/// offers exactly once, with known offer evidence.
///
/// This is the other side of
/// [`restart_between_claim_and_terminalization_offers_every_block_exactly_once`].
/// That test kills a child whose reservation is already durable, so the
/// killed row can only ever carry an *unknown* offer. This one kills a child
/// that has claimed and reserved nothing, so the recovery must make the one
/// call and record a *known* outcome for it. `#[266]`'s guarantee is that
/// neither row is ever offered twice; the two together are what say the
/// reservation is a boundary rather than a coincidence.
///
/// # Where the kill point is, and why it is not a timer
///
/// `Coordinator::offer_candidate` makes no node call at all between the claim
/// and `Ledger::reserve_offer` unless its cached staleness screen trips, and
/// the screen is skipped outright for a leased candidate (#350). So the row
/// this test kills is the one candidate here that is **not** leased: its
/// screen is authoritative, and a stale cached tip hint sends it through
/// `Coordinator::observe_candidate`, whose `getblockhash` at the candidate's
/// own height is the last node call before the reservation. The node
/// withholds the reply to that one call and signals the parent, which kills
/// the child while it is parked inside it — the same philosophy as the
/// withheld `submitblock` above, and for the same reason: a kill point owned
/// by a clock proves nothing about where the child was.
///
/// Making the hint stale without moving the chain is the whole difficulty.
/// The node reports a decoy tip of the same height and the same chainwork
/// until it has served its first block template, and the real parent from
/// then on. `Coordinator::refresh_once` caches the tip it read *before* it
/// asks for a template, so serving that template is the node's own proof that
/// the child has cached the decoy; after it, the authoritative probe sees a
/// chain that matches the cluster's recorded view exactly, and so falls
/// through to the reservation instead of abandoning the row. A node that
/// merely reported the real parent and relied on the frontend not having
/// observed one yet would be racing `refresh_loop` against `submit_loop`,
/// which start together and both tick immediately.
///
/// # The startup race, closed causally
///
/// Those two loops give no ordering, so a candidate already in the outbox is
/// commonly claimed and probed *before* the first block template — the probe
/// would then see the decoy, `Ledger::observe_chain_view` would refuse an
/// equal-work conflicting tip, and `submit_loop` would reschedule the row
/// with an attempt count that depends on how the two loops interleaved. The
/// child is therefore launched against an **empty** outbox and the victim is
/// published only once the node has served that first template. With nothing
/// due, `Ledger::claim_candidate` evaluates no sequence value, runs neither
/// claim lane, writes nothing and makes no node call, and publishing writes
/// no payout revision; so the ordering here is program order, not timing.
///
/// # The settings this scenario adds, and why
///
/// `PRISM_RPC_TIMEOUT_SECONDS=600` (default 15): the withheld `getblockhash`
/// must not time out before the parent kills the child, or the row would be
/// rescheduled rather than killed mid-observation. `PRISM_BLOCKPOLL_SECONDS`
/// is raised far above the run so the one startup refresh is the only one
/// that can re-cache a tip before the drain's first screen.
///
/// # Why the siblings are held out of the due set for the recovery
///
/// The recovered row is unleased, so its screen runs again on the relaunch —
/// and a sibling confirmation between the relaunch and that claim would move
/// the payout revision and make the screen abandon it. That is the
/// supersession scenario, which `candidate_storm.rs` owns. The siblings are
/// held out of the due set until the recovery has finished, and released
/// after, so the whole population still reaches a terminal state and the
/// per-hash offer counter still covers every planned block.
///
/// # Scale
///
/// The property is scale-independent by nature: it is about one row's
/// position in one child's call sequence. The fixture is still sized through
/// `storm_scale`, as every scenario in this suite is, because the assertions
/// that no *other* row was touched — every sibling at `attempt_count` zero at
/// the kill, one `submitblock` per planned hash afterwards — only say
/// something over a population.
///
/// # The mutation this is aimed at
///
/// Hoist `self.ledger.reserve_offer(claim).await?` above the staleness probe
/// in `Coordinator::offer_candidate`. A pre-offer death would then leave an
/// `offer_reserved` row, the recovery would settle it as an unknown offer and
/// never call the node, and both halves of this test fail: the row read after
/// the kill is not `pending` with a null reservation, and the recovered row
/// carries a reservation without an offer time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kill_before_the_first_offer_recovers_as_one_known_offer() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let count = storm_scale::storm_candidates()?;
    let database = FixtureDatabase::open(&raw, "prism_storm_preoffer_").await?;
    let ledger = match Ledger::connect_tool(
        &database.url,
        "storm-preoffer-fixture".into(),
        4,
        true,
        None,
    )
    .await
    {
        Ok(ledger) => ledger,
        Err(error) => return Err(database.abandon(error).await),
    };
    let outcome = kill_before_first_offer(&database, &ledger, count).await;
    ledger.pool.close().await;
    database.close(outcome).await
}

/// The nondefault settings the pre-offer kill depends on, each observed in
/// the child's behaviour rather than assumed. See the scenario's docstring.
const PRE_OFFER_SETTINGS: &[(&str, &str)] = &[
    ("PRISM_RPC_TIMEOUT_SECONDS", "600"),
    ("PRISM_BLOCKPOLL_SECONDS", "3600"),
];

async fn kill_before_first_offer(
    database: &FixtureDatabase,
    ledger: &Ledger,
    count: usize,
) -> Result<()> {
    let snapshot = seed(ledger).await?;
    let plan_started = Instant::now();
    let mut planned = plan(&snapshot, count)?;
    let planned_in = plan_started.elapsed();
    // The one candidate whose pre-offer screen is authoritative. Every other
    // sibling keeps the lease `plan` gives it, so none of them probes and none
    // of them can be abandoned by a revision this drain itself moved.
    planned[0].candidate.leased = false;
    let victim_hash = planned[0].block_hash.clone();
    let victim_digest = planned[0].candidate.block_sha256.clone();

    let node = FakeNode::open(heights(&planned), NodePlan::withholding_first_probe()).await?;
    let mut signals = node.signals.clone();

    let first_started = Instant::now();
    let mut child = ServerChild::spawn_with(&database.url, &node.url, PRE_OFFER_SETTINGS)?;
    // The child came up against an empty outbox. This is the node's own
    // evidence that the refresh has already cached the decoy tip: the tip is
    // read before the template is asked for, so serving the template
    // happens-after that cache. Nothing here is a sleep or a poll of the child.
    wait_for_signal(
        &mut signals,
        &child,
        count,
        "the node's first block template",
        |s| (s.templates > 0).then_some(()),
    )
    .await?;

    let enqueue_started = Instant::now();
    ledger
        .enqueue_candidate(planned[0].candidate.clone())
        .await?;
    let victim_enqueued = enqueue_started.elapsed();
    // The node is holding the reply to this block's one pre-offer
    // `getblockhash`: the child has claimed durably, has reserved nothing, and
    // has only a tip recheck and a readiness proof left before
    // `Ledger::reserve_offer` -- no database write among them.
    let withheld = wait_for_signal(
        &mut signals,
        &child,
        count,
        "the withheld pre-offer probe",
        |s| s.withheld_probe.clone(),
    )
    .await?;
    ensure!(
        withheld == victim_hash,
        "the node withheld the probe of {withheld}, not of the one candidate published for it"
    );

    // The drain is serial and its one task is parked in that call, so the
    // child claims nothing else: the siblings can be published now and are
    // still untouched at the kill, which is what makes `attempt_count` an
    // accounting over the whole population rather than over one row.
    let siblings_started = Instant::now();
    for row in &planned[1..] {
        ledger.enqueue_candidate(row.candidate.clone()).await?;
    }
    let siblings_enqueued = siblings_started.elapsed();
    let first_lifetime = first_started.elapsed();
    let calls_before_restart = signals.borrow().calls;
    child.kill().await?;

    // ---- What a death before the reservation leaves behind. ----
    let heartbeat_before_restart = one_live_frontend(ledger, "after the kill").await?;
    let killed = row(ledger, &victim_hash).await?;
    ensure!(
        killed.state == CandidateState::Pending.as_str(),
        "the killed block is {} and not {}: a crash before `Ledger::reserve_offer` commits can leave no reservation",
        killed.state,
        CandidateState::Pending.as_str()
    );
    ensure!(
        killed.offer_reserved_at.is_none() && killed.offer_reserved_by.is_none(),
        "the killed frontend reserved an offer it never took: {killed:?}"
    );
    ensure!(
        killed.offered_at_ms.is_none() && killed.offer_outcome.is_none(),
        "the killed frontend recorded an offer it never made: {killed:?}"
    );
    ensure!(
        killed.claim_live
            && killed.claim_token.is_some()
            && killed.claim_instance_id.as_deref() == Some(INSTANCE_ID),
        "the killed frontend's claim did not outlive it: {killed:?}"
    );
    ensure!(
        killed.attempt_count == 1,
        "the killed block was claimed {} times before the kill",
        killed.attempt_count
    );
    ensure!(
        killed.payload_present && stored_block_digest(ledger, &victim_hash).await? == victim_digest,
        "the killed row lost the payload its recovery has to rebuild from: {killed:?}"
    );
    let untouched = other_attempts(ledger, &victim_hash).await?;
    ensure!(
        untouched == 0,
        "a sibling was claimed {untouched} times while the drain was parked in one block's probe"
    );
    ensure!(
        !node.per_hash().await.contains_key(&victim_hash),
        "the node was offered the block whose frontend died before reserving the offer"
    );

    // ---- The recovery. ----
    let held = hold_all_but(ledger, &victim_hash).await?;
    ensure!(
        held == count as u64 - 1,
        "{held} siblings were held out of the due set for {count} planned blocks"
    );
    expire_claim(ledger, &victim_hash, killed.claim_token.as_deref()).await?;

    let restart_started = Instant::now();
    let mut child = ServerChild::spawn_with(&database.url, &node.url, PRE_OFFER_SETTINGS)?;
    wait_for_signal(
        &mut signals,
        &child,
        count,
        "the relaunched frontend's first node call",
        |s| (s.calls > calls_before_restart).then_some(()),
    )
    .await?;
    let first_node_call = restart_started.elapsed();
    wait_for_drain(ledger, &child, count, 1).await?;
    let recovered_in = restart_started.elapsed();

    let recovered = row(ledger, &victim_hash).await?;
    ensure!(
        recovered.state == "submitted",
        "the recovered block is {}, not the terminal state its own offer earned it",
        recovered.state
    );
    ensure!(
        recovered.offered_at_ms.is_some() && recovered.offer_outcome.as_deref() == Some("accepted"),
        "the recovery did not record the outcome of the call it made: {recovered:?}"
    );
    ensure!(
        recovered.offer_reserved_by.as_deref() == Some(INSTANCE_ID),
        "the recovery's reservation names {:?}, not the relaunched frontend",
        recovered.offer_reserved_by
    );
    ensure!(
        recovered.attempt_count == 2,
        "the recovered block was claimed {} times, not once before the kill and once to recover it",
        recovered.attempt_count
    );

    // ---- The rest of the population, once the recovery is out of the way. ----
    let released = release_all_but(ledger, &victim_hash).await?;
    ensure!(
        released == count as u64 - 1,
        "{released} siblings were released for {count} planned blocks"
    );
    wait_for_signal(&mut signals, &child, count, "every offer", |s| {
        (s.submissions >= count as u64).then_some(())
    })
    .await?;
    wait_for_drain(ledger, &child, count, count as i64).await?;
    let second_lifetime = restart_started.elapsed();
    // Reap before reading the counters, so nothing moves under the assertions.
    child.kill().await?;

    let heartbeat_after_restart = one_live_frontend(ledger, "after the relaunch").await?;
    ensure!(
        heartbeat_after_restart > heartbeat_before_restart,
        "the relaunched frontend did not register a heartbeat: the row still reads {heartbeat_before_restart}"
    );

    let offers = assert_one_offer_per_hash(&node, &planned).await?;
    ensure!(
        offers == count as u64,
        "{offers} submitblock calls for {count} blocks"
    );
    ensure!(
        submitted(ledger).await? == count as i64,
        "not every block reached a terminal state: {}",
        states(ledger).await?
    );
    // The inverse of the post-reservation kill's accounting: there, exactly
    // one row ends with a reservation and no outcome, because its one call's
    // answer died with the process that made it. Here the call was never
    // begun, so the recovery made it and every row's evidence is a known one.
    let without_outcome: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_block_candidate_outbox WHERE offer_reserved_by IS NOT NULL AND offered_at_ms IS NULL",
    )
    .fetch_one(&ledger.pool)
    .await?;
    ensure!(
        without_outcome == 0,
        "{without_outcome} rows carry a reservation without an offer outcome; the killed frontend never took one"
    );
    let attempts = attempts(ledger).await?;
    ensure!(
        attempts == count as i64 + 1,
        "the drain took {attempts} claims for {count} blocks and one recovery; {}",
        states(ledger).await?
    );

    storm_scale::record(
        "candidate_storm_pre_offer_kill",
        &[
            ("candidates", count.to_string()),
            ("plan_seconds", format!("{:.3}", planned_in.as_secs_f64())),
            (
                "victim_enqueue_seconds",
                format!("{:.3}", victim_enqueued.as_secs_f64()),
            ),
            (
                "sibling_enqueue_seconds",
                format!("{:.3}", siblings_enqueued.as_secs_f64()),
            ),
            (
                "first_lifetime_seconds",
                format!("{:.3}", first_lifetime.as_secs_f64()),
            ),
            (
                "restart_to_first_node_call_seconds",
                format!("{:.3}", first_node_call.as_secs_f64()),
            ),
            (
                "restart_to_recovered_seconds",
                format!("{:.3}", recovered_in.as_secs_f64()),
            ),
            (
                "second_lifetime_drain_seconds",
                format!("{:.3}", second_lifetime.as_secs_f64()),
            ),
            ("submitblock_calls", offers.to_string()),
            ("claims", attempts.to_string()),
            ("dispatch_slots", dispatch_slots(ledger).await?.to_string()),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// What a relaunched frontend's recovery of one row costs on the wire.
// ---------------------------------------------------------------------------

/// The marker a fixture trigger raises for the one row under measurement. The
/// trigger's `WHEN` clause names that row's `block_hash`, so the marker itself
/// carries no identity and a statement that touches any other row raises
/// nothing.
const RECOVERED_ROW: &str = "recovered_row";

/// How many times every statement text an idle child runs must be observed
/// before the observation window is taken to have covered each periodic path's
/// whole cycle. Two, so a path whose first tick landed mid-window is still
/// seen from one tick to the next.
const IDLE_REPEATS: usize = 2;

/// The shortest an idle observation may be. The slowest background path no
/// setting can slow down or turn off is the metrics collector's hardcoded ten
/// seconds, and the settling rule above cannot wait for a path that has not
/// ticked even once, so a window has to be at least long enough to contain two
/// of its ticks whatever else the child is doing. This is a *wait*, not an
/// assertion: no elapsed time is ever compared with anything.
///
/// One periodic path is longer than any window of this length: the vardiff
/// hint prune, at five minutes. It is not covered by the idle windows, and it
/// is caught the other way -- by the pinned statement count, which one stray
/// selected statement would break.
const IDLE_FLOOR: Duration = Duration::from_secs(25);

/// A bound on how long an idle observation waits for the settling rule on top
/// of that floor. Also a bound on a *wait*: a window that hits this cap is not
/// a failure, because the second window's empty-selection assertion is what
/// proves the first window saw every background statement.
const IDLE_WINDOW_CAP: Duration = Duration::from_secs(60);

/// Statements the recovery of one dead claimant's row costs a relaunched
/// frontend on the PostgreSQL wire, derived from the real binary's own
/// executions through `support/ledger_execution_proxy.rs` and pinned here so
/// a change to it is a change to this line.
///
/// The row is `offer_reserved`: killed after `Ledger::reserve_offer` committed
/// and before the one `submitblock` returned. `Coordinator::process_candidate`
/// routes it straight to `settle_offered_candidate(OfferOutcome::Unknown)`, so
/// this sequence contains **no** offer, no pre-offer probe and no second
/// `submitblock` -- that is #266's guarantee, and it is also what makes the
/// sequence deterministic, which is why this scenario kills after the
/// reservation rather than before it as
/// [`a_kill_before_the_first_offer_recovers_as_one_known_offer`] does. Nine
/// groups, in order:
///
/// - `Ledger::claim_candidate`, 5: `BEGIN`; the writer fence (`fatal_error`
///   and the legacy writer lease); the due-work probe, which allocates one
///   sequence slot; **one** claiming lane statement; `COMMIT`. `claim_candidate`
///   runs the fresh lane too on most slots, so five rather than six is a
///   property of which claim takes this row: the fresh lane selects only a
///   `pending`, never-attempted row, and this row is neither, so on every slot
///   where the fresh lane runs it takes one of the un-attempted siblings
///   instead. The recovered row is therefore claimed by a claim whose fresh
///   lane did not run at all, and that is the same claim at both
///   cardinalities.
/// - `Coordinator::process_candidate`'s opening `Ledger::renew_candidate_claim`,
///   5: `BEGIN`; the writer fence; the row lock (`FOR NO KEY UPDATE`); the
///   renewing `UPDATE` of `claim_expires_at`; `COMMIT`. The 30-second renewal
///   tick does not fire inside a recovery this short.
/// - the landed-audit read, 1, outside any transaction.
/// - the window read, 7: `BEGIN`; the repeatable-read read-only declaration;
///   the payout revision; the prior-balances snapshot; the share-range
///   existence pair; one page of shares; `COMMIT`. The page loop is bounded
///   by the window's **share** count, which this fixture holds at one, never
///   by the sibling count.
/// - `Ledger::observe_chain_view`, before the landing, 5: `BEGIN`; the
///   settlement advisory lock; the writer fence; the cluster read
///   (`FOR UPDATE`); `COMMIT`. The chain is held still, so the equal-work
///   branch runs and no revision `UPDATE` is issued here.
/// - the durable-range verification, 4, outside any transaction: the audit
///   `EXISTS` probe, one share page, and the next and previous share
///   sequence probes.
/// - `Ledger::land_candidate_at_revision`, 16: `BEGIN`; the settlement
///   advisory lock; the writer fence; the claim's row lock (`FOR KEY SHARE`)
///   and its token/expiry/state fence; the revision fence; the existing-audit
///   digest read; the payout-revision read; the prior balances; the
///   `qbit_pool_blocks` insert; the share count; and the four set-based
///   inserts of the audit snapshot, the bundle, the payout entries and the
///   carry-forward, each one statement whatever the recipient count;
///   `COMMIT`.
/// - `Ledger::observe_chain_view` again, after the landing, 5.
/// - `Ledger::finish_candidate_counted_at_revision`, 17: `BEGIN`; the
///   settlement and order advisory locks; the writer fence; the revision
///   fence; the claim fence (2); the first-confirmation read (`FOR UPDATE`);
///   the #478 confirmation record's two reads, the block's not-yet-counting
///   carry rows (with its height and whether it has a record) and the prior
///   balances they meet, both one statement whatever the recipient count and
///   followed by no write because this landing does not diverge; the
///   `qbit_pool_blocks` confirmation `UPDATE`; the confirmed `EXISTS`; the
///   deferred-share read; the revision bump, which takes the cluster row
///   `FOR UPDATE` before its `UPDATE` so that it waits for any job cohort's
///   `KEY SHARE` authority fence (`lock_cluster_authority`, #479), 2; the
///   terminal `UPDATE` of the outbox row; `COMMIT`.
///
/// Asserted **equal** at [`storm_scale::BASELINE_CANDIDATES`] and at the run's
/// own cardinality before it is compared with this constant, so a drift in the
/// sequence and a dependence on N fail with different messages. A failure
/// against this constant prints the whole observed sequence.
const STATEMENTS_PER_RECOVERED_ROW: usize = 65;

#[path = "support/ledger_execution_proxy.rs"]
mod ledger_execution_proxy;
use ledger_execution_proxy::{Execution, ExecutionProxy};

/// Install the marker triggers for one block hash. The trigger lives in the
/// fixture schema, so it fires for a child process's statements exactly as it
/// does for an in-process ledger's, and the proxy on the wire attaches the
/// NOTICE it raises to the execution that raised it. That is how this test
/// learns which executions belong to the recovered row without spelling the
/// server's SQL.
///
/// Both tables are needed: the claim, the renewal and the terminal update
/// write `qbit_block_candidate_outbox`, and the landing writes only
/// `qbit_pool_blocks`.
async fn install_recovery_markers(ledger: &Ledger, block_hash: &str) -> Result<()> {
    ensure!(
        block_hash.len() == 64 && block_hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{block_hash:?} is not a block hash this fixture generated"
    );
    let statements = [
        format!("CREATE FUNCTION prism_storm_recovery_observe() RETURNS trigger LANGUAGE plpgsql AS $marker$ BEGIN RAISE NOTICE 'prism-execution-marker {RECOVERED_ROW} %', TG_OP; RETURN NULL; END; $marker$"),
        format!("CREATE TRIGGER prism_storm_recovery_observe AFTER INSERT OR UPDATE ON qbit_block_candidate_outbox FOR EACH ROW WHEN (NEW.block_hash='{block_hash}') EXECUTE FUNCTION prism_storm_recovery_observe()"),
        format!("CREATE TRIGGER prism_storm_recovery_observe AFTER INSERT OR UPDATE ON qbit_pool_blocks FOR EACH ROW WHEN (NEW.block_hash='{block_hash}') EXECUTE FUNCTION prism_storm_recovery_observe()"),
    ];
    for statement in statements {
        sqlx::query(&statement).execute(&ledger.pool).await?;
    }
    Ok(())
}

/// One maximal run of executions on one connection: a transaction from the
/// `BEGIN` through the `COMMIT` or `ROLLBACK` that ends it, or one statement a
/// pooled connection ran outside any transaction. The boundaries come from the
/// server's own completion tags, never from statement text, so nothing here
/// depends on how the server spells anything. A run opens at an execution
/// whose *first* tag is `BEGIN`: the append's `BEGIN; SET LOCAL …` batch ends
/// on `SET`, and its last tag alone would miss the transaction.
fn runs(executions: &[Execution]) -> Vec<Vec<Execution>> {
    let mut open: BTreeMap<u64, Vec<Execution>> = BTreeMap::new();
    let mut closed: Vec<Vec<Execution>> = Vec::new();
    for execution in executions {
        let tag = execution.completion();
        if execution.tags.first().map(String::as_str) == Some("BEGIN") {
            if let Some(abandoned) = open.remove(&execution.connection) {
                closed.push(abandoned);
            }
            open.insert(execution.connection, vec![execution.clone()]);
            continue;
        }
        match open.get_mut(&execution.connection) {
            Some(run) => {
                run.push(execution.clone());
                if matches!(tag, Some("COMMIT") | Some("ROLLBACK")) {
                    closed.push(open.remove(&execution.connection).expect("an open run"));
                }
            }
            // A statement outside a transaction, or one whose `BEGIN` was
            // before the mark: its own run of one.
            None => closed.push(vec![execution.clone()]),
        }
    }
    closed.extend(open.into_values());
    closed.sort_by_key(|run| run.first().map(|first| first.seq).unwrap_or_default());
    closed
}

/// The statements a set of executions spent on foreground work, in order.
///
/// A run is foreground when it contains at least one statement text the child
/// did not run while it was idle: `BEGIN`, the writer fence, an advisory lock
/// and the due-work probe are all shared with a background loop, but no
/// background loop claims a row, renews a lease, reads a window, lands an
/// audit or takes the cluster row for a chain observation. Taking the whole
/// run once one of its statements is foreground is what recovers the shared
/// statements a marker can never see: triggers exist for `INSERT`, `UPDATE`
/// and `DELETE` only.
///
/// With a `span`, only runs that reach into it are taken. The span is the
/// execution order between the first and last statement marked for the
/// recovered row, and `Coordinator::submit_loop` awaits each row's processing
/// inline before it claims the next, so no other row's work can be in flight
/// inside it. That is the whole of the attribution: background statements are
/// excluded because their texts are the idle ones, and sibling drains are
/// excluded because they are not in the span.
fn foreground(
    executions: &[Execution],
    idle: &std::collections::BTreeSet<String>,
    span: Option<(u64, u64)>,
) -> Vec<String> {
    let mut selected = Vec::new();
    for run in runs(executions) {
        if !run.iter().any(|execution| !idle.contains(&execution.sql)) {
            continue;
        }
        if let Some((first, last)) = span {
            if !run
                .iter()
                .any(|execution| (first..=last).contains(&execution.seq))
            {
                continue;
            }
        }
        selected.extend(run.into_iter().map(|execution| execution.sql));
    }
    selected
}

/// The execution order the recovered row's own markers bracket.
fn marked_span(executions: &[Execution]) -> Result<(u64, u64)> {
    let marked: Vec<u64> = executions
        .iter()
        .filter(|execution| {
            execution
                .markers
                .iter()
                .any(|marker| marker.table == RECOVERED_ROW)
        })
        .map(|execution| execution.seq)
        .collect();
    let first = *marked
        .first()
        .context("no statement in the measured window touched the recovered row")?;
    Ok((first, *marked.last().expect("a last marked statement")))
}

/// Watch a child that has nothing to claim until the window has lasted at
/// least [`IDLE_FLOOR`] **and** every statement text it ran in that window has
/// been observed [`IDLE_REPEATS`] times, and return those texts. Each periodic
/// path has then been seen from one of its ticks to the next, the hardcoded
/// ten-second metrics collector included.
async fn observe_idle(
    proxy: &ExecutionProxy,
    mark: u64,
) -> Result<std::collections::BTreeSet<String>> {
    let started = Instant::now();
    loop {
        let executions = proxy.executions_since(mark)?;
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for execution in &executions {
            *counts.entry(execution.sql.clone()).or_default() += 1;
        }
        let settled = started.elapsed() >= IDLE_FLOOR
            && !counts.is_empty()
            && counts.values().all(|seen| *seen >= IDLE_REPEATS);
        if settled || started.elapsed() >= IDLE_WINDOW_CAP {
            return Ok(counts.into_keys().collect());
        }
        tokio::time::sleep(SUBMIT_LOOP_TICK).await;
    }
}

/// Hold **every** row out of the due set, and release them all again in the
/// order they were enqueued.
///
/// The measured scenario needs a relaunched child with nothing whatever to
/// claim, for as long as its idle windows take. Holding the recovered row out
/// of the due set is what gives it that: the alternative, leaving the row
/// protected only by its dead owner's claim, would make the windows race
/// `CANDIDATE_LEASE`'s compile-time 120 seconds, and a window that outlasted it
/// would find the recovery running inside the very window that is supposed to
/// prove nothing happens there.
///
/// The release sets `next_attempt_at` to each row's own `created_at`, so the
/// whole population becomes claimable in one statement and the oldest-due lane
/// sees the recovered row -- enqueued first -- as the oldest. A release to one
/// wall-clock instant would leave that order to microseconds.
async fn hold_all(ledger: &Ledger) -> Result<u64> {
    Ok(
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at='infinity'")
            .execute(&ledger.pool)
            .await?
            .rows_affected(),
    )
}

async fn release_all_in_enqueue_order(ledger: &Ledger) -> Result<u64> {
    Ok(
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=created_at")
            .execute(&ledger.pool)
            .await?
            .rows_affected(),
    )
}

/// What one measured recovery observed.
struct RecoveryCost {
    statements: usize,
    sequence: Vec<String>,
    /// Statement texts the child ran while it had nothing to claim.
    background: usize,
    /// Rows still unfinished when the recovered row reached its terminal
    /// state: a lower bound on how many were due when it was claimed, and the
    /// reason the two cardinalities are two different measurements rather
    /// than the same one twice.
    unfinished: i64,
    elapsed: Duration,
}

/// The statements a relaunched frontend spends recovering and finishing one
/// row whose claimant died are the same integer at the baseline cardinality
/// and at the run's own, and that integer has not drifted.
///
/// `candidate_storm.rs`'s
/// `drained_rows_cost_the_same_statements_at_the_baseline_and_storm_cardinalities`
/// proves this for a drain driven in-process, where the fixture is the only
/// thing touching the database and a watermark around one claim is an exact
/// measure. Nothing proved it for a **relaunched process**, and a relaunched
/// process cannot be quiesced: besides the drain it runs an empty submit poll
/// every 100 ms, a job and blob pruner at a hardcoded two seconds, a health
/// publisher and cluster heartbeat, a metrics collector at a hardcoded ten
/// seconds, a refresh loop, a candidate lease heartbeat and a pair of session
/// settings for every pooled connection it opens. Counting every execution in
/// a wall-clock bracket would make the number a function of how long the
/// bracket was open, so this test attributes executions by statement identity
/// instead. See [`foreground`] for the classifier and the two properties it
/// rests on; the idle window below is what proves it excludes the background
/// rather than merely coinciding with it.
///
/// # The scenario, and why each step is where it is
///
/// One candidate is published and offered by a first child, whose
/// `submitblock` reply the node withholds; the child is killed there, so the
/// row is `offer_reserved` with a live claim and an outcome nobody knows. The
/// remaining siblings are published next, while no child is running; the dead
/// owner's claim is expired there and then, while it is certainly still live,
/// and **every** row is held out of the due set. A second child is then
/// launched **through the execution proxy** — the parent's own ledger stays on
/// the direct URL, so none of its reads are ever observed — and comes up with
/// nothing claimable at all. That is what makes the two idle windows genuinely
/// idle, and it is why the recovered row is parked rather than left under its
/// dead owner's claim: windows long enough to cover the slowest background
/// path would otherwise race `CANDIDATE_LEASE`'s compile-time 120 seconds, and
/// the recovery would run inside the window that exists to show nothing does.
///
/// The whole population is then made claimable in one statement, in the order
/// it was enqueued. The recovered row is not `pending` and its attempt count
/// is not zero, so the fresh claim lane can never select it; it is claimed by
/// the oldest-due lane, behind a handful of sibling claims, with the rest of
/// the population still unfinished and due underneath it. That is the shape a
/// per-sibling loop in `Ledger::claim_candidate` would be measured against.
///
/// # Honest scope
///
/// No statement on this recovery path has an execution count that depends on
/// the number of unfinished siblings **today**: every one of them is once per
/// transaction, `WHERE block_hash=$1`, a `LIMIT 1` selection over the
/// unfinished set, or a page loop bounded by the window's share count. The
/// equality this test asserts therefore guards against a *future* per-sibling
/// loop. The most realistic site is `Ledger::claim_candidate`, where the
/// `LIMIT 1` due-work probe and the single claiming lane statement would
/// become a `LIMIT k` selection and one `UPDATE` per due row; that claim is
/// inside the measured span, so such a change fails this test at once.
///
/// It proves nothing about plan cost, rows scanned, contention, latency or
/// anything outside the selected transactions — including every startup gate,
/// which runs before the measurement window opens. `Ledger::configure`'s
/// single set-based read of every unfinished row is exactly such a statement,
/// and turning *it* into a per-row loop would be invisible here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relaunched_frontend_recovers_one_row_at_the_same_statement_cost_at_both_cardinalities(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let count = storm_scale::storm_candidates()?;
    let at_baseline = measure_recovery(&raw, storm_scale::BASELINE_CANDIDATES).await?;
    let at_storm = measure_recovery(&raw, count).await?;

    ensure!(
        at_baseline.unfinished != at_storm.unfinished,
        "both cardinalities left {} rows unfinished behind the recovered one, so the equality below compares one measurement with itself",
        at_storm.unfinished
    );
    ensure!(
        at_baseline.statements == at_storm.statements,
        "recovering one row cost a relaunched frontend {} statements with {} rows behind it and {} with {}. \
         Per-row recovery cost is not independent of N; this is a finding about the server, not this test. \
         What the baseline ran: {:#?}\nWhat the storm ran: {:#?}",
        at_baseline.statements,
        at_baseline.unfinished,
        at_storm.statements,
        at_storm.unfinished,
        at_baseline.sequence,
        at_storm.sequence
    );
    ensure!(
        at_storm.statements == STATEMENTS_PER_RECOVERED_ROW,
        "recovering one row cost {} statements, not the pinned {STATEMENTS_PER_RECOVERED_ROW}; the recovery's statement sequence changed and the constant's comment must be re-derived with it. What the recovery ran: {:#?}",
        at_storm.statements,
        at_storm.sequence
    );

    storm_scale::record(
        "restart_recovery_statements",
        &[
            ("candidates", count.to_string()),
            (
                "baseline_candidates",
                storm_scale::BASELINE_CANDIDATES.to_string(),
            ),
            ("statements_at_baseline", at_baseline.statements.to_string()),
            ("statements_at_storm", at_storm.statements.to_string()),
            (
                "unfinished_behind_at_baseline",
                at_baseline.unfinished.to_string(),
            ),
            (
                "unfinished_behind_at_storm",
                at_storm.unfinished.to_string(),
            ),
            (
                "background_texts_at_baseline",
                at_baseline.background.to_string(),
            ),
            ("background_texts_at_storm", at_storm.background.to_string()),
            (
                "baseline_seconds",
                format!("{:.3}", at_baseline.elapsed.as_secs_f64()),
            ),
            (
                "storm_seconds",
                format!("{:.3}", at_storm.elapsed.as_secs_f64()),
            ),
        ],
    );
    Ok(())
}

async fn measure_recovery(raw: &str, count: usize) -> Result<RecoveryCost> {
    let database = FixtureDatabase::open(raw, "prism_storm_recovery_").await?;
    let ledger = match Ledger::connect_tool(
        &database.url,
        "storm-recovery-fixture".into(),
        4,
        true,
        None,
    )
    .await
    {
        Ok(ledger) => ledger,
        Err(error) => return Err(database.abandon(error).await),
    };
    let outcome = recover_one_row(&database, &ledger, count).await;
    ledger.pool.close().await;
    match outcome {
        Ok(cost) => {
            database.close(Ok(())).await?;
            Ok(cost)
        }
        Err(error) => Err(database.abandon(error).await),
    }
}

async fn recover_one_row(
    database: &FixtureDatabase,
    ledger: &Ledger,
    count: usize,
) -> Result<RecoveryCost> {
    let started = Instant::now();
    let snapshot = seed(ledger).await?;
    let planned = plan(&snapshot, count)?;
    let victim_hash = planned[0].block_hash.clone();
    install_recovery_markers(ledger, &victim_hash).await?;

    // ---- One row, offered and killed mid-call by the first child. ----
    let node = FakeNode::open(heights(&planned), NodePlan::withholding_submission(1)).await?;
    let mut signals = node.signals.clone();
    ledger
        .enqueue_candidate(planned[0].candidate.clone())
        .await?;
    let mut child = ServerChild::spawn(&database.url, &node.url)?;
    let withheld = wait_for_signal(&mut signals, &child, count, "the withheld offer", |s| {
        s.withheld.clone()
    })
    .await?;
    ensure!(
        withheld == victim_hash,
        "the node withheld the offer of {withheld}, not of the one candidate published for it"
    );
    let calls_before_restart = signals.borrow().calls;
    child.kill().await?;
    let killed = row(ledger, &victim_hash).await?;
    ensure!(
        killed.state == CandidateState::OfferReserved.as_str() && killed.claim_live,
        "the row to recover is {}, not a live claim on a reserved offer: {killed:?}",
        killed.state
    );

    // ---- The population, published while nothing is draining. Every row,
    // the recovered one included, is then held out of the due set, so the
    // relaunched child comes up with nothing to claim and stays that way for
    // as long as its idle windows take. The dead owner's claim is expired here
    // rather than later, while it is certainly still live, so the fence below
    // can only ever hit that claim. ----
    for row in &planned[1..] {
        ledger.enqueue_candidate(row.candidate.clone()).await?;
    }
    expire_claim(ledger, &victim_hash, killed.claim_token.as_deref()).await?;
    let held = hold_all(ledger).await?;
    ensure!(
        held == count as u64,
        "{held} rows were held out of the due set for {count} planned blocks"
    );

    // ---- The relaunch, on the proxy's wire. ----
    let parsed = url::Url::parse(&database.url)?;
    let upstream = tokio::net::lookup_host((
        parsed.host_str().context("the database URL names a host")?,
        parsed.port().unwrap_or(5432),
    ))
    .await?
    .next()
    .context("the database host resolves")?;
    let proxy = ExecutionProxy::start(upstream).await?;
    let outcome = observe_recovery(
        database,
        ledger,
        &proxy,
        &node,
        &mut signals,
        &planned,
        calls_before_restart,
    )
    .await;
    // The proxy is dropped rather than finished: a `SIGKILL`ed child resets
    // every pooled socket, so its forwarding tasks end in a transport error
    // this test caused on purpose, and reporting that would say nothing about
    // the observation. What the observation rests on is checked while the
    // child is still alive -- `ExecutionProxy::executions_since` and the
    // explicit `check` before the kill both refuse a failed observer -- so a
    // broken proxy still cannot pass as an empty one.
    drop(proxy);
    let mut cost = outcome?;
    cost.elapsed = started.elapsed();
    Ok(cost)
}

#[allow(clippy::too_many_arguments)]
async fn observe_recovery(
    database: &FixtureDatabase,
    ledger: &Ledger,
    proxy: &ExecutionProxy,
    node: &FakeNode,
    signals: &mut watch::Receiver<NodeSignal>,
    planned: &[Planned],
    calls_before_restart: u64,
) -> Result<RecoveryCost> {
    let count = planned.len();
    let victim_hash = planned[0].block_hash.clone();
    // Before the child exists, so the background set below covers its startup
    // as well as its steady state. That matters for one statement group in
    // particular: the session settings a pooled connection runs when it opens.
    // They are the same statements on every connection, so having them in the
    // set is what makes a connection the pool happens to open later -- inside
    // the measured window -- cost the measurement nothing.
    let startup_mark = proxy.mark();
    let mut child = ServerChild::spawn(&proxy.rewrite_url(&database.url)?, &node.url)?;
    wait_for_signal(
        signals,
        &child,
        count,
        "the relaunched frontend's first node call",
        |s| (s.calls > calls_before_restart).then_some(()),
    )
    .await?;

    // ---- Two idle windows. The first settles: it ends once every periodic
    // path has been seen from one of its ticks to the next, and everything the
    // child has run since it was launched then becomes the background set. The
    // second proves that set was complete, by requiring the classifier to
    // select nothing at all from a window of the same shape. Coinciding totals
    // would not say this; an empty selection does, and the window it is taken
    // over contributed nothing to the set it is checked against. ----
    let settled = observe_idle(proxy, proxy.mark()).await?;
    ensure!(
        !settled.is_empty(),
        "the first idle window observed nothing at all; the proxy is not on the child's wire"
    );
    let verify_mark = proxy.mark();
    let background: std::collections::BTreeSet<String> = proxy
        .executions_since(startup_mark)?
        .into_iter()
        .map(|execution| execution.sql)
        .collect();
    observe_idle(proxy, verify_mark).await?;
    let stray = foreground(&proxy.executions_since(verify_mark)?, &background, None);
    ensure!(
        stray.is_empty(),
        "an idle relaunched frontend ran {} statements the classifier attributes to foreground work, so the background set is incomplete and every measurement below would be charged for them: {stray:#?}",
        stray.len()
    );

    // ---- The measured window. ----
    let mark = proxy.mark();
    let released = release_all_in_enqueue_order(ledger).await?;
    ensure!(
        released == count as u64,
        "{released} rows were released for {count} planned blocks"
    );
    wait_for_row_state(ledger, &child, count, &victim_hash, "submitted").await?;
    let executions = proxy.executions_since(mark)?;
    let unfinished: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state IN {}",
        CandidateState::UNFINISHED_SQL
    ))
    .fetch_one(&ledger.pool)
    .await?;
    // Every statement this measurement rests on was recorded by a proxy whose
    // tasks were all healthy at this point; the kill below is what ends them.
    proxy.check()?;
    child.kill().await?;

    // The recovery kept the #266 guarantee it is being measured on: the row
    // reached a terminal state from the evidence it already had, and the node
    // was never offered its block a second time.
    let recovered = row(ledger, &victim_hash).await?;
    ensure!(
        recovered.state == "submitted"
            && recovered.offered_at_ms.is_none()
            && recovered.offer_reserved_by.as_deref() == Some(INSTANCE_ID),
        "the recovered row is {} with offer evidence {:?}/{:?}; a reserved row settles from the evidence it already had, records no offer time it never learned, and never offers again",
        recovered.state,
        recovered.offer_reserved_by,
        recovered.offered_at_ms
    );
    ensure!(
        node.per_hash().await.get(&victim_hash) == Some(&1),
        "the node was offered the recovered block {:?} times, not the one time its first frontend offered it",
        node.per_hash().await.get(&victim_hash)
    );

    let span = marked_span(&executions)?;
    let sequence = foreground(&executions, &background, Some(span));
    ensure!(
        !sequence.is_empty(),
        "the classifier selected no statement for a row the server demonstrably recovered"
    );
    Ok(RecoveryCost {
        statements: sequence.len(),
        sequence,
        background: background.len(),
        unfinished,
        elapsed: Duration::default(),
    })
}

/// Wait until one row reaches `state`. Like [`wait_for_drain`], the deadline
/// is a hang guard that reports the child's stderr, never an assertion.
async fn wait_for_row_state(
    ledger: &Ledger,
    child: &ServerChild,
    count: usize,
    block_hash: &str,
    state: &str,
) -> Result<()> {
    let bound = deadline(count);
    let started = Instant::now();
    loop {
        let observed = row(ledger, block_hash).await?;
        if observed.state == state {
            return Ok(());
        }
        ensure!(
            started.elapsed() < bound,
            "{block_hash} is {} and not {state} within {bound:?} ({}); child stderr:\n{}",
            observed.state,
            states(ledger).await?,
            child.stderr_tail()
        );
        tokio::time::sleep(SUBMIT_LOOP_TICK).await;
    }
}
