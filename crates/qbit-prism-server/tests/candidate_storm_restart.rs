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
//! `PRISM_*` variables. The kill point belongs to the node, not to a timer:
//! the node receives a `submitblock`, records the hash and withholds the
//! reply, and the parent signals the child then — provably past the durable
//! `offer_reserved` reservation that `Ledger::reserve_offer` commits before
//! the one `submitblock` call, and provably short of terminalization, because
//! the child never learned the outcome.
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
//! block hash, `attempt_count`, dispatch slots, and row states.

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
/// owns; `withheld` names the block whose reply the node is holding.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct NodeSignal {
    calls: u64,
    submissions: u64,
    withheld: Option<String>,
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
    /// The `submitblock` call whose reply is withheld, counted from the
    /// node's first. `None` never withholds.
    withhold_at: Option<u64>,
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
    async fn open(heights: BTreeMap<String, u64>, withhold_at: Option<u64>) -> Result<Self> {
        let (signal, signals) = watch::channel(NodeSignal::default());
        let shared = Arc::new(NodeShared {
            state: Mutex::new(NodeState {
                per_hash: BTreeMap::new(),
                heights,
                accepted: BTreeMap::new(),
                signal: NodeSignal::default(),
            }),
            signal,
            withhold_at,
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
            ok(&id, json!(hash))
        }
        "getblockchaininfo" => ok(
            &id,
            json!({"chain":"test","initialblockdownload":false,"blocks":TIP_HEIGHT,
                   "headers":TIP_HEIGHT,"bestblockhash":tip_hash(),"chainwork":"01"}),
        ),
        "getbestblockhash" => ok(&id, json!(tip_hash())),
        "getblockheader" => ok(&id, json!({"previousblockhash":"cd".repeat(32)})),
        "getnetworkinfo" => ok(&id, json!({"connections":2})),
        // Deliberate, and the reason is in the module docstring: without a
        // template `refresh_once` never reaches block reconciliation, so the
        // drain is the only writer of the payout revision.
        "getblocktemplate" => rpc_error(&id, -10, NO_TEMPLATE),
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
            if node.withhold_at == Some(state.signal.submissions) {
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
        // signal above and kills the child here, so the child is past the
        // durable reservation and has learned nothing about the outcome.
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

/// The lifecycle of one outbox row, as the database holds it.
#[derive(Debug)]
struct Row {
    state: String,
    attempt_count: i32,
    offer_reserved_by: Option<String>,
    offered_at_ms: Option<i64>,
    offer_outcome: Option<String>,
    claim_live: bool,
    claim_token: Option<String>,
}

/// `Row`'s columns, in the order the statement below selects them.
type RowColumns = (
    String,
    i32,
    Option<String>,
    Option<i64>,
    Option<String>,
    bool,
    Option<String>,
);

async fn row(ledger: &Ledger, block_hash: &str) -> Result<Row> {
    let columns: RowColumns =
        sqlx::query_as("SELECT state,attempt_count,offer_reserved_by,offered_at_ms,offer_outcome,COALESCE(claim_expires_at>clock_timestamp(),false),claim_token FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(block_hash)
            .fetch_one(&ledger.pool)
            .await
            .with_context(|| format!("outbox row {block_hash}"))?;
    Ok(Row {
        state: columns.0,
        attempt_count: columns.1,
        offer_reserved_by: columns.2,
        offered_at_ms: columns.3,
        offer_outcome: columns.4,
        claim_live: columns.5,
        claim_token: columns.6,
    })
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
    let node = FakeNode::open(heights(&planned), Some(kill_at)).await?;
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
    let expired = sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE block_hash=$1 AND claim_token=$2 AND claim_expires_at>clock_timestamp()")
        .bind(&killed_hash)
        .bind(killed.claim_token.as_deref())
        .execute(&ledger.pool)
        .await?
        .rows_affected();
    ensure!(
        expired == 1,
        "the dead owner's lease was not the one expired"
    );

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

    let node = FakeNode::open(heights(&planned), None).await?;
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
