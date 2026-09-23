//! Drain cost and admission at storm cardinality (#270, workstream A).
//!
//! The 2026-08-20 testnet4 incident left 3,120 durable block candidates behind
//! one decided height. 2.x.x answered with a Python instrument that measured
//! the per-row drain cost of that sibling set; #244 deleted it with no native
//! peer. This target ports the *properties*, not the harness: the native
//! outbox claims one row per query (`Ledger::claim_lane_sql`, `LIMIT 1` with
//! `FOR UPDATE SKIP LOCKED`), so the aggregate-page storm cannot recur in its
//! original form, but N siblings still cost a claim, a lease, a chain probe
//! and a durable write each.
//!
//! Nothing here asserts a duration. Every asserted quantity is an integer from
//! a counter the test owns — node calls per block hash, claims issued, the
//! exact terminal set, statements observed on the wire — plus the plan shape
//! of the statements the server actually issues. Durations are recorded under
//! [`storm_scale::REPORT_PREFIX`] and never asserted. The per-row cost is
//! asserted *equal* at two cardinalities measured in the same process, so the
//! reduced-N CI path proves the same property as a local run at 3,120.
//!
//! The four scenarios, and the property each owns:
//!
//! - [`siblings_behind_a_decided_height_drain_once_each_at_a_constant_node_cost`]
//!   is the clean baseline drain: N rows at one decided height, one of them
//!   the block the node reports active, drained through
//!   `Ledger::claim_candidate` and `Coordinator::process_candidate` with no
//!   faults and no held leases. It owns the exact node-call, claim-count and
//!   terminal-set properties, and it admits brand-new work, offered and
//!   landed for real, while the backlog is still owed.
//! - [`drained_rows_cost_the_same_statements_at_the_baseline_and_storm_cardinalities`]
//!   puts `support/ledger_execution_proxy.rs` on the wire and counts the
//!   statements a drained row costs at two cardinalities in one process
//!   against one schema, asserting them equal.
//! - [`claim_lanes_and_the_dispatch_probe_keep_their_indexes_over_unfinished_due_rows`]
//!   EXPLAINs the real lane statements over N *unfinished due* rows, and one
//!   claim under `ANALYZE, BUFFERS`, so a claim is proven not to walk the
//!   siblings.
//! - [`admission_serves_new_work_first_while_the_due_backlog_is_still_owed`]
//!   scales `ledger_postgres.rs`'s dispatch-fairness shape to a backlog of N,
//!   and proves the fresh lane's priority has not starved the due lane.
//!
//! [`storm_candidates_refuses_a_cardinality_outside_its_own_bounds`] needs no
//! database, so it is not a gated test and is not in
//! `test/prism-gated-tests.txt`.
//!
//! The fixture takes **its own PostgreSQL database** through
//! `support/ledger_database.rs` (#410), and that choice is load-bearing rather
//! than incidental. PostgreSQL scopes advisory locks to a database, not a
//! schema, so a drain of N siblings — N claims, each touching the ordering and
//! settlement keys — would both queue behind other fixtures' locks and inflict
//! its own on them, at the largest scale in the test tree. A per-row cost
//! measured in a shared database is measuring other fixtures' lock waits, not
//! this drain. With the database isolated, claims issued and statements
//! executed per drained row are clean numbers; wall clock still carries the
//! runner's noise, which is the second reason it is recorded and not asserted.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test candidate_storm -- --nocapture
//! ```

use anyhow::{ensure, Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, FoundBlock, PayoutPolicy};
use qbit_prism_server::{
    codec,
    config::Config,
    coordinator::{Coordinator, TipState},
    ledger::{Candidate, CandidateState, Ledger, SignerKeys, Snapshot, WindowRef},
    metrics::Metrics,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex as SyncMutex},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[allow(dead_code)]
#[path = "support/storm_scale.rs"]
mod storm_scale;

#[path = "support/ledger_execution_proxy.rs"]
mod proxy;
use proxy::ExecutionProxy;

#[allow(dead_code)]
#[path = "support/ledger_database.rs"]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// Read-only chain calls one drained sibling costs, derived from the native
/// pre-offer path and pinned here so a change to that path is a change to this
/// line. `Coordinator::offer_candidate` finds the cached tip is not the
/// candidate's parent and runs `observe_candidate` exactly once, which issues,
/// in order:
///
/// 1. `getblockchaininfo` — `readiness::chain_info`'s chain view, which also
///    carries the tip hash, the tip height and the cumulative chainwork;
/// 2. `getnetworkinfo` — the peer count of that same readiness proof. The
///    suite's configured chain is `testnet`, which `readiness::chain_info`
///    treats as public, so the peer floor is enforced and this call is made;
/// 3. `getblockhash` at the candidate's height — is this block the one the
///    chain holds at its height? The single question that decides between
///    adoption and abandonment;
/// 4. `getbestblockhash` — a proof the tip did not move while the height was
///    read;
/// 5. and 6. `getblockchaininfo` and `getnetworkinfo` again, through
///    `ready_tip`, which re-proves readiness against the tip just observed.
///
/// No `submitblock` and no `getblockheader` are on this path: a sibling the
/// chain has already decided against is never offered, and the native
/// pre-offer probe asks for a hash at a height rather than for a header.
/// 2.x.x pinned its own shape against its own selector (`submitblock:
/// siblings`, `getbestblockhash: 3 * siblings`, `getblockheader: 3 *
/// siblings`); this constant is derived from the native path and is
/// deliberately not 3.
const CHAIN_PROBES_PER_SIBLING: usize = 6;

/// The lease every claim in this target takes, in seconds. Well above any
/// per-row drain, so `Coordinator::process_candidate`'s 30-second renewal tick
/// never fires and a drained row costs exactly the one opening renewal.
const CLAIM_LEASE_SECONDS: i64 = 120;

/// The height the storm sits behind: every sibling claims it, and the chain
/// has already decided it in favour of exactly one of them.
const DECIDED_HEIGHT: u64 = 101;

/// The block the decided height extends, and the parent every sibling's header
/// names.
const PARENT: &str = "aa";

/// Terminal rows the outbox retains beside the storm in the plan-shape
/// scenario. `offer_lifecycle.rs`'s index test seeds the same number of
/// retained rows; keeping it identical is what makes the remaining difference
/// — N unfinished *due* rows in the partial index instead of five — the only
/// thing the two plans are read against.
const RETAINED_HISTORY_ROWS: i64 = 50_000;

/// Rounds of "new work arrives, the fresh lane must serve it next" the
/// admission scenario runs against the backlog. More than one dispatch cycle
/// of eight, so the oldest-due slots the cycle reserves are exercised too.
const ADMISSION_ROUNDS: usize = 32;

/// One dispatch cycle: `Ledger::claim_candidate` reserves every eighth slot
/// for the oldest-due lane and offers the other seven to the fresh lane
/// first. Mirrors the weighting in `Ledger::claim_candidate`; a test that
/// needs the fresh lane's answer predicts the lane from the sequence rather
/// than assuming every claim is a fresh one.
const DISPATCH_CYCLE: i64 = 8;

/// The suite's cardinality is one process-wide environment reader (EP-CONFIG's
/// control), and one scenario deliberately sets the variable to prove its
/// refusals. Every reader in this binary takes this lock, so a libtest thread
/// never observes another test's temporary value.
static CARDINALITY: SyncMutex<()> = SyncMutex::new(());

/// The run's cardinality, read through the one shared reader under
/// [`CARDINALITY`].
fn storm_candidates() -> Result<usize> {
    let _guard = CARDINALITY.lock().expect("cardinality lock");
    storm_scale::storm_candidates()
}

// ---------------------------------------------------------------------------
// The fake node
// ---------------------------------------------------------------------------

struct NodeState {
    tip: String,
    height: u64,
    chainwork: u64,
    /// The active chain by height, for `getblockhash`.
    blocks: HashMap<u64, String>,
    /// Every `submitblock`, keyed by the block hash the call carried. The
    /// hash is taken from the block bytes on the wire, so a call is counted
    /// against the block it actually offered.
    submissions: HashMap<String, usize>,
    /// Every read-only chain call, keyed by method. No method on the drain
    /// path names a block hash, so the hash-keyed counter above is the only
    /// per-block one, exactly as the offer property needs.
    probes: HashMap<String, usize>,
}

impl NodeState {
    fn probe_total(&self) -> usize {
        self.probes.values().sum()
    }

    fn submissions_of(&self, block_hash: &str) -> usize {
        self.submissions.get(block_hash).copied().unwrap_or(0)
    }
}

async fn node_reply(
    State(node): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut node = node.lock().await;
    let method = request["method"].as_str().unwrap_or("").to_owned();
    let result = match method.as_str() {
        "getblockhash" if request["params"][0] == 0 => {
            // Startup's genesis probe, before any measurement window opens.
            json!("00".repeat(32))
        }
        "getblockhash" => {
            *node.probes.entry(method.clone()).or_default() += 1;
            let height = request["params"][0].as_u64().unwrap_or(0);
            json!(node
                .blocks
                .get(&height)
                .cloned()
                .unwrap_or_else(|| node.tip.clone()))
        }
        "getblockchaininfo" => {
            *node.probes.entry(method.clone()).or_default() += 1;
            json!({"chain":"test","initialblockdownload":false,
                "blocks":node.height,"headers":node.height,"bestblockhash":node.tip,
                "chainwork":format!("{:x}",node.chainwork)})
        }
        "getnetworkinfo" => {
            *node.probes.entry(method.clone()).or_default() += 1;
            json!({"connections":2})
        }
        "getbestblockhash" => {
            *node.probes.entry(method.clone()).or_default() += 1;
            json!(node.tip)
        }
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap_or("")).unwrap();
            let hash = codec::hash_display(&codec::double_sha256(&block[..80]));
            *node.submissions.entry(hash.clone()).or_default() += 1;
            // Accepted: the block becomes the tip, with more cumulative work.
            let height = node.height + 1;
            node.tip = hash.clone();
            node.height = height;
            node.chainwork += 1;
            node.blocks.insert(height, hash);
            Value::Null
        }
        other => panic!("unexpected candidate-storm RPC {other}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

/// A fixture PostgreSQL database (#410), an in-process node and one
/// coordinator on them. Every scenario opens one and closes it whatever the
/// body did.
struct Storm {
    fixture: FixtureDatabase,
    /// A plain pool on the fixture database, for seeding rows the scenario
    /// does not reach through the ledger and for reading durable state back.
    pool: PgPool,
    coordinator: Arc<Coordinator>,
    node: Arc<Mutex<NodeState>>,
    server: tokio::task::JoinHandle<()>,
    proxy: Option<Arc<ExecutionProxy>>,
}

/// Everything [`Storm::build`] makes inside an already-created fixture
/// database, so a failure there can be handed back to `FixtureDatabase`.
type StormParts = (
    PgPool,
    Arc<Coordinator>,
    Arc<Mutex<NodeState>>,
    tokio::task::JoinHandle<()>,
    Option<Arc<ExecutionProxy>>,
);

impl Storm {
    async fn open(schema_prefix: &str, instance: &str, proxied: bool) -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let fixture = FixtureDatabase::open(&raw, schema_prefix).await?;
        match Self::build(&fixture, instance, proxied).await {
            Ok((pool, coordinator, node, server, proxy)) => Ok(Some(Self {
                fixture,
                pool,
                coordinator,
                node,
                server,
                proxy,
            })),
            Err(error) => Err(fixture.abandon(error).await),
        }
    }

    async fn build(fixture: &FixtureDatabase, instance: &str, proxied: bool) -> Result<StormParts> {
        let pool = PgPool::connect(&fixture.url).await?;
        let proxy = match proxied {
            false => None,
            true => {
                let parsed = url::Url::parse(&fixture.url)?;
                let host = parsed.host_str().context("database URL names a host")?;
                let port = parsed.port().unwrap_or(5432);
                let upstream = tokio::net::lookup_host((host, port))
                    .await?
                    .next()
                    .context("database host resolves")?;
                Some(Arc::new(ExecutionProxy::start(upstream).await?))
            }
        };
        let database_url = match &proxy {
            Some(proxy) => proxy.rewrite_url(&fixture.url)?,
            None => fixture.url.clone(),
        };
        let node = Arc::new(Mutex::new(NodeState {
            tip: PARENT.repeat(32),
            height: DECIDED_HEIGHT - 1,
            chainwork: 1,
            blocks: HashMap::from([(DECIDED_HEIGHT - 1, PARENT.repeat(32))]),
            submissions: HashMap::new(),
            probes: HashMap::new(),
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let rpc_url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(node_reply))
            .with_state(node.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let coordinator = Coordinator::new(
            config(database_url, rpc_url, instance)?,
            Arc::new(Metrics::default()),
        )
        .await?;
        for index in 1..=3u64 {
            coordinator.ledger.append(share(index), None).await?;
        }
        Ok((pool, coordinator, node, server, proxy))
    }

    fn ledger(&self) -> &Ledger {
        &self.coordinator.ledger
    }

    async fn probes(&self) -> usize {
        self.node.lock().await.probe_total()
    }

    async fn submissions_of(&self, block_hash: &str) -> usize {
        self.node.lock().await.submissions_of(block_hash)
    }

    /// Put the chain in the state the incident left: the decided height is
    /// held by `active`, whose siblings are all now stale.
    async fn decide_height(&self, active: &str) -> Result<()> {
        {
            let mut node = self.node.lock().await;
            node.tip = active.to_owned();
            node.height = DECIDED_HEIGHT;
            node.chainwork = 2;
            node.blocks.insert(DECIDED_HEIGHT, active.to_owned());
        }
        // Establish the cluster's view of that chain before any measurement
        // window opens, so no drained row pays for the revision bump of a
        // first observation and every row's cost is the steady-state one.
        self.ledger()
            .observe_chain_view(active, DECIDED_HEIGHT, "2")
            .await?;
        Ok(())
    }

    /// A sibling of the decided height: a durable candidate row whose header
    /// names [`PARENT`], distinguished only by its nonce. It is never rebuilt
    /// and never relayed — the chain decided against it before it was ever
    /// offered — so its body is the minimum the enqueue and the claim decode
    /// authenticate: a well-formed header that hashes to `block_hash`, and a
    /// window reference the ledger can still resolve.
    fn sibling(&self, snapshot: &Snapshot, nonce: u32) -> Result<Candidate> {
        let manifest_key =
            ManifestSigningKey::from_seed_hex(&self.coordinator.config.manifest_seed)?;
        let ledger_key = ManifestSigningKey::from_seed_hex(&self.coordinator.config.ledger_seed)?;
        let mut block = vec![0u8; 80];
        block[..4].copy_from_slice(&0x2000_0000u32.to_le_bytes());
        let mut previous = hex::decode(PARENT.repeat(32))?;
        previous.reverse();
        block[4..36].copy_from_slice(&previous);
        block[36..68].fill(0x33);
        block[68..72].copy_from_slice(&1_800_000_000u32.to_le_bytes());
        block[72..76].copy_from_slice(&0x207f_ffffu32.to_le_bytes());
        block[76..80].copy_from_slice(&nonce.to_le_bytes());
        let mut hash = Sha256::digest(Sha256::digest(&block[..80])).to_vec();
        hash.reverse();
        // One byte past the header: the decode requires a body, and nothing
        // on the abandonment path reads it.
        block.push(0);
        Ok(Candidate {
            block_hash: hex::encode(hash),
            block_sha256: Candidate::block_digest_hex(&block),
            job_id: format!("storm-{DECIDED_HEIGHT}-{nonce}"),
            payout_revision: snapshot.payout_revision,
            window: WindowRef::from_snapshot(snapshot)?,
            bootstrap_share: None,
            found_block: FoundBlock {
                block_height: DECIDED_HEIGHT,
                coinbase_value_sats: 500_000_000,
                network_difficulty: 100,
                anchor_job_issued_at_ms: snapshot.anchor_ms,
            },
            payout_policy: PayoutPolicy::day_one_default(),
            ctv: None,
            audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
            signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
            leased: false,
            coinbase_suffix_hex: "00".repeat(12),
            deferred_share: None,
            block_bytes: block,
            as_issued_balances: snapshot.prior_balances.clone(),
        })
    }

    /// A block found on `snapshot` at `height` on `parent`, with this
    /// frontend's keys, so its post-offer rebuild reproduces its audit and it
    /// can be offered, landed and confirmed. The one candidate shape this
    /// target builds in full: admission during the drain has to reach the node
    /// to be worth asserting. Adapted from `offer_latency.rs`.
    fn found(
        &self,
        snapshot: &Snapshot,
        height: u64,
        parent: &str,
        nonce_start: u32,
    ) -> Result<Candidate> {
        let manifest_key =
            ManifestSigningKey::from_seed_hex(&self.coordinator.config.manifest_seed)?;
        let ledger_key = ManifestSigningKey::from_seed_hex(&self.coordinator.config.ledger_seed)?;
        let bundle = qbit_prism::build_audit_bundle_with_coinbase_options(
            snapshot.shares.clone(),
            FoundBlock {
                block_height: height,
                coinbase_value_sats: 500_000_000,
                network_difficulty: 100,
                anchor_job_issued_at_ms: snapshot.anchor_ms,
            },
            snapshot.prior_balances.clone(),
            PayoutPolicy::day_one_default(),
            Some("00".repeat(12)),
            vec![],
            &manifest_key,
            &ledger_key,
        )?;
        let template = json!({"version":0x20000000u32,"bits":"207fffff","curtime":1_800_000_000u32,
            "previousblockhash":parent,"transactions":[]});
        let job = codec::Job::from_manifest(
            format!("storm-admission-{height}"),
            &template,
            &bundle.signed_coinbase_manifest.manifest,
            "00000000",
            8,
            1e-12,
            0.0,
            true,
        )?;
        let proof = (nonce_start..nonce_start + 100_000)
            .find_map(|nonce| {
                let proof = job
                    .assemble_submission(
                        &"00".repeat(8),
                        &format!("{:08x}", job.ntime),
                        &format!("{nonce:08x}"),
                        None,
                        0,
                    )
                    .ok()?;
                proof.block_pass.then_some(proof)
            })
            .context("constrained block proof missing")?;
        let block_bytes = hex::decode(&proof.block_hex)?;
        Ok(Candidate {
            block_hash: proof.block_hash_hex,
            block_sha256: Candidate::block_digest_hex(&block_bytes),
            job_id: job.job_id,
            payout_revision: snapshot.payout_revision,
            window: WindowRef::from_snapshot(snapshot)?,
            bootstrap_share: None,
            found_block: bundle.found_block.clone(),
            payout_policy: PayoutPolicy::day_one_default(),
            ctv: None,
            audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
            signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
            leased: false,
            coinbase_suffix_hex: "00".repeat(12),
            deferred_share: None,
            block_bytes,
            as_issued_balances: snapshot.prior_balances.clone(),
        })
    }

    /// Enqueue the one row at the decided height the chain decided *for*, and
    /// put the node's chain in that state.
    ///
    /// The row is held out of the due lane: the decided height's own block is
    /// not this drain's work, and leaving it there is what turns the terminal
    /// set into a safety property. The drain must abandon exactly the due
    /// siblings and leave this row pending, unattempted and without a single
    /// node call against its hash.
    async fn decide_against_siblings(&self, snapshot: &Snapshot, nonce: u32) -> Result<String> {
        let candidate = self.sibling(snapshot, nonce)?;
        let active = candidate.block_hash.clone();
        ensure!(
            self.ledger().enqueue_candidate_once(candidate).await?,
            "the decided height's own block was not enqueued"
        );
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE block_hash=$1")
            .bind(&active)
            .execute(&self.pool)
            .await?;
        self.decide_height(&active).await?;
        Ok(active)
    }

    /// Enqueue `count` due siblings of the decided height, every one of them a
    /// never-attempted pending row, as the incident left them.
    async fn enqueue_siblings(
        &self,
        snapshot: &Snapshot,
        count: usize,
        nonce_base: u32,
    ) -> Result<BTreeSet<String>> {
        let mut siblings = BTreeSet::new();
        for index in 0..count {
            let candidate = self.sibling(snapshot, nonce_base + index as u32)?;
            let hash = candidate.block_hash.clone();
            ensure!(
                self.ledger().enqueue_candidate_once(candidate).await?,
                "sibling {index} was not enqueued"
            );
            ensure!(
                siblings.insert(hash),
                "sibling {index} repeated a block hash"
            );
        }
        Ok(siblings)
    }

    /// Dispatch slots consumed so far. `Ledger::claim_candidate` takes one per
    /// poll that found due work, and none when nothing is due, so the delta
    /// over a drain is the number of claims it made.
    async fn dispatch_slots(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM qbit_prism_candidate_dispatch_sequence",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    async fn state_of(&self, block_hash: &str) -> Result<String> {
        Ok(
            sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(block_hash)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn hashes_in_state(&self, state: &str) -> Result<BTreeSet<String>> {
        Ok(
            sqlx::query_scalar("SELECT block_hash FROM qbit_block_candidate_outbox WHERE state=$1")
                .bind(state)
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .collect(),
        )
    }

    /// `SUM(attempt_count)` over the rows the drain finished.
    async fn attempts_over_terminal_rows(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COALESCE(SUM(attempt_count),0) FROM qbit_block_candidate_outbox WHERE state IN ('submitted','abandoned')",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    /// Close every pool and the proxy, then drop the fixture database. The
    /// body's own failure wins; a cleanup failure is attached to it.
    async fn close(self, result: Result<()>) -> Result<()> {
        self.server.abort();
        self.coordinator.ledger.pool.close().await;
        self.pool.close().await;
        let finished = match &self.proxy {
            Some(proxy) => proxy.finish().await,
            None => Ok(()),
        };
        let result = match (result, finished) {
            (Ok(()), finished) => finished,
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(finished)) => {
                Err(error.context(format!("the proxy also failed to finish: {finished:#}")))
            }
        };
        self.fixture.close(result).await
    }
}

fn share(index: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("miner:{index:064x}"),
        miner_id: format!("miner-{}", index % 2),
        order_key: format!("miner-{}", index % 2),
        p2mr_program_hex: format!("{:02x}", 0x10 + index).repeat(32),
        share_difficulty: 100,
        network_difficulty: 100,
        template_height: 100,
        job_id: "seed".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

fn config(database_url: String, rpc_url: String, instance: &str) -> Result<Config> {
    Ok(Config {
        database_url,
        instance_id: format!("storm-{instance}"),
        database_connections: 6,
        initialize_schema: true,
        chain: "testnet".into(),
        expected_genesis_hash: None,
        min_peers: 1,
        template_max_age: Duration::from_secs(120),
        submit_tip_max_age: Duration::from_secs(10),
        template_refresh_failure_exit: Duration::from_secs(120),
        rpc_url,
        rpc_user: "test".into(),
        rpc_password: "test".into(),
        rpc_timeout: Duration::from_secs(10),
        block_submit_timeout: Duration::from_secs(10),
        candidate_orphan_confirmations: 6,
        poll_interval: Duration::from_secs(1),
        blockwait: false,
        build_workers: 2,
        runtime_workers: 2,
        snapshot_interval: Duration::from_secs(60),
        health_timeout: Duration::from_secs(15),
        share_commit_timeout: Duration::from_secs(15),
        share_commit_grace: Duration::from_secs(5),
        block_only_ack_timeout: Duration::from_secs(60),
        extranonce2_size: 8,
        coinbase_tag: "/PRISM/".into(),
        manifest_seed: "11".repeat(32),
        ledger_seed: "22".repeat(32),
        ledger_public_key: ManifestSigningKey::from_seed_hex(&"22".repeat(32))?.public_key_hex(),
        username_fallback: None,
        payout_policy: PayoutPolicy::day_one_default(),
        fee_address: None,
        ctv_enabled: false,
        ctv_config: qbit_prism::SettlementModeConfig::default(),
        ctv_direct_floor: 10_485_760,
        ctv_fee: None,
        ctv_fee_premium_bps: 12000,
        ctv_broadcast: false,
        ctv_broadcast_interval: Duration::from_secs(10),
        version_mask: codec::VERSION_ROLLING_MASK,
        audit_bind: "127.0.0.1".into(),
        audit_port: 0,
    })
}

/// Every node of an `EXPLAIN (FORMAT JSON)` plan tree, depth first. Copied
/// from `offer_lifecycle.rs`, whose index test is the method this one scales.
fn plan_nodes<'a>(plan: &'a Value, nodes: &mut Vec<&'a Value>) {
    nodes.push(plan);
    if let Some(children) = plan["Plans"].as_array() {
        for child in children {
            plan_nodes(child, nodes);
        }
    }
}

fn unix_ms_now() -> Result<i64> {
    let elapsed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    Ok(i64::try_from(elapsed.as_millis())?)
}

/// Claim one due row and drive it through the real path. Returns its block
/// hash and the read-only chain calls the row cost, or `None` once nothing is
/// due.
async fn drain_one(storm: &Storm) -> Result<Option<(String, usize)>> {
    let Some(claim) = storm.ledger().claim_candidate(CLAIM_LEASE_SECONDS).await? else {
        return Ok(None);
    };
    let hash = claim.candidate.block_hash.clone();
    let before = storm.probes().await;
    storm.coordinator.process_candidate(&claim).await?;
    Ok(Some((hash, storm.probes().await - before)))
}

// ---------------------------------------------------------------------------
// The clean baseline drain
// ---------------------------------------------------------------------------

/// N rows at one decided height, one of them the block the node reports
/// active, drained through `Ledger::claim_candidate` and
/// `Coordinator::process_candidate` with no faults and no held leases: the
/// clean baseline every other figure in this target is read against.
///
/// Four integer properties, every one of them from a counter this test owns:
///
/// - **Node calls, exact, per block hash.** Every drained sibling costs
///   [`CHAIN_PROBES_PER_SIBLING`] read-only chain calls and not one more, so
///   the total is exactly `k * N`, and `submitblock` is never called for any
///   of them. The constant is derived from the rows the drain finishes and
///   then held against the pinned value, so a change to the native pre-offer
///   path is a failure here rather than a silently different number.
/// - **Claims issued, exact.** Every sibling is claimed once: the dispatch
///   sequence's delta, the claims the drain made and `SUM(attempt_count)` over
///   the finished rows are the same integer. No row claimed twice, none
///   missed.
/// - **Terminal set, exact.** The abandoned set *is* the sibling set, and the
///   decided height's own block is still pending, unattempted, with no offer
///   evidence and no node call against its hash. Set equality is what makes
///   this a safety property rather than a counting coincidence.
/// - **Admission is not starved.** Three times during the drain, brand-new
///   work is enqueued while the backlog is still owed; each time the next
///   claim is that block, by hash, and it reaches `submitblock` exactly once
///   and finishes submitted. Those are the only offered hashes in this
///   scenario, which is what makes "exactly 0 for every hash that was not
///   offered" a statement about a drain that could have offered.
///
/// The wall clock, the microseconds a row cost and the cardinality are
/// recorded and never asserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn siblings_behind_a_decided_height_drain_once_each_at_a_constant_node_cost() -> Result<()> {
    let candidates = storm_candidates()?;
    let Some(storm) = Storm::open("prism_storm_drain_", "drain", false).await? else {
        return Ok(());
    };
    let outcome = drain_the_storm(&storm, candidates).await;
    storm.close(outcome).await
}

async fn drain_the_storm(storm: &Storm, candidates: usize) -> Result<()> {
    let ledger = storm.ledger();
    let snapshot = ledger.snapshot(100).await?;
    let active = storm.decide_against_siblings(&snapshot, 0).await?;
    let siblings = storm.enqueue_siblings(&snapshot, candidates - 1, 1).await?;
    let slots_before = storm.dispatch_slots().await?;

    // Where new work arrives while the backlog is still owed. The first is as
    // early as the drain can take it; the others sit inside the backlog.
    let admit_at = [1, candidates / 3, (2 * candidates) / 3];
    let mut next_admission = 0usize;
    let mut admitted: Vec<String> = Vec::new();
    let mut drained: BTreeSet<String> = BTreeSet::new();
    let mut probe_costs: BTreeSet<usize> = BTreeSet::new();
    let mut claims = 0usize;
    let started = Instant::now();
    loop {
        if next_admission < admit_at.len() && drained.len() >= admit_at[next_admission] {
            next_admission += 1;
            // The dispatch cycle reserves every eighth slot for the oldest-due
            // lane. Spend a reserved one on the backlog first, so the claim
            // under assertion really is the fresh lane's answer.
            if (storm.dispatch_slots().await? + 1) % DISPATCH_CYCLE == 0 {
                let (hash, cost) = drain_one(storm)
                    .await?
                    .context("the reserved oldest-due slot found no backlog")?;
                claims += 1;
                ensure!(
                    siblings.contains(&hash),
                    "the oldest-due slot claimed {hash}, which is not a sibling of the decided height"
                );
                probe_costs.insert(cost);
                ensure!(drained.insert(hash), "a sibling was claimed twice");
            }
            let (height, parent) = {
                let node = storm.node.lock().await;
                (node.height + 1, node.tip.clone())
            };
            // A block found on the chain as it is now, at the revision it is
            // at now: what a live frontend would have just accepted.
            let snapshot = ledger.snapshot(100).await?;
            let candidate = storm.found(&snapshot, height, &parent, 1_000 * height as u32)?;
            let hash = candidate.block_hash.clone();
            *storm.coordinator.observed_tip.write().await = TipState::baseline(parent);
            ensure!(
                ledger
                    .enqueue_candidate_observed(candidate, Some(unix_ms_now()?))
                    .await?,
                "admitted block {hash} was not enqueued"
            );
            let backlog = siblings.len() - drained.len();
            let claim = ledger
                .claim_candidate(CLAIM_LEASE_SECONDS)
                .await?
                .context("admitted work was not claimable")?;
            claims += 1;
            ensure!(
                claim.candidate.block_hash == hash,
                "a backlog of {backlog} due siblings starved admission: the next claim was {}, not the block just found",
                claim.candidate.block_hash
            );
            storm.coordinator.process_candidate(&claim).await?;
            let offers = storm.submissions_of(&hash).await;
            ensure!(
                offers == 1,
                "admitted block {hash} reached submitblock {offers} times, not once"
            );
            let state = storm.state_of(&hash).await?;
            ensure!(
                state == "submitted",
                "admitted block {hash} finished as {state}, not submitted"
            );
            admitted.push(hash);
            continue;
        }
        let Some((hash, cost)) = drain_one(storm).await? else {
            break;
        };
        claims += 1;
        ensure!(
            siblings.contains(&hash),
            "the drain claimed {hash}, which is neither a due sibling nor admitted work"
        );
        probe_costs.insert(cost);
        ensure!(drained.insert(hash), "a sibling was claimed twice");
    }
    let elapsed = started.elapsed();

    // Node calls, exact, per block hash.
    ensure!(
        admitted.len() == admit_at.len(),
        "{} admissions ran, not {}",
        admitted.len(),
        admit_at.len()
    );
    ensure!(
        probe_costs.len() == 1,
        "drained siblings cost {probe_costs:?} chain calls; the per-row cost is not one constant"
    );
    let derived = *probe_costs.iter().next().expect("one measured cost");
    ensure!(
        derived == CHAIN_PROBES_PER_SIBLING,
        "a drained sibling cost {derived} chain calls, not the pinned {CHAIN_PROBES_PER_SIBLING}; the native pre-offer path changed and the constant's comment must be re-derived with it"
    );
    let probes = derived * drained.len();
    let node = storm.node.lock().await;
    ensure!(
        node.submissions_of(&active) == 0,
        "the decided height's own block was offered"
    );
    for hash in &siblings {
        ensure!(
            node.submissions_of(hash) == 0,
            "sibling {hash} reached submitblock, which the chain had already decided against"
        );
    }
    for hash in &admitted {
        ensure!(
            node.submissions_of(hash) == 1,
            "admitted block {hash} was offered {} times",
            node.submissions_of(hash)
        );
    }
    ensure!(
        node.submissions.len() == admitted.len(),
        "submitblock was called for {} distinct blocks, not the {} admitted",
        node.submissions.len(),
        admitted.len()
    );
    let probe_methods: BTreeSet<String> = node.probes.keys().cloned().collect();
    drop(node);

    // Claims issued, exact.
    let slots = storm.dispatch_slots().await? - slots_before;
    ensure!(
        slots == claims as i64,
        "the dispatch sequence advanced {slots} times for {claims} claims"
    );
    let attempts = storm.attempts_over_terminal_rows().await?;
    ensure!(
        attempts == claims as i64,
        "SUM(attempt_count) over the finished rows is {attempts}, not the {claims} claims the drain made: a row was claimed twice or missed"
    );

    // Terminal set, exact.
    ensure!(
        drained == siblings,
        "the drained set is not the sibling set ({} drained, {} siblings)",
        drained.len(),
        siblings.len()
    );
    let abandoned = storm.hashes_in_state("abandoned").await?;
    ensure!(
        abandoned == siblings,
        "the abandoned set is not exactly the siblings ({} abandoned, {} siblings)",
        abandoned.len(),
        siblings.len()
    );
    let submitted = storm.hashes_in_state("submitted").await?;
    let expected_submitted: BTreeSet<String> = admitted.iter().cloned().collect();
    ensure!(
        submitted == expected_submitted,
        "the submitted set is not exactly the admitted blocks"
    );
    let untouched: (String, i32, Option<i64>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT state,attempt_count,offered_at_ms,offer_outcome,offer_reserved_by FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(&active)
    .fetch_one(&storm.pool)
    .await?;
    ensure!(
        untouched == ("pending".to_owned(), 0, None, None, None),
        "the decided height's own block did not survive the drain untouched and without offer evidence: {untouched:?}"
    );

    storm_scale::record(
        "sibling_drain",
        &[
            ("candidates", candidates.to_string()),
            (
                "baseline_candidates",
                storm_scale::BASELINE_CANDIDATES.to_string(),
            ),
            ("drained_rows", drained.len().to_string()),
            ("admissions", admitted.len().to_string()),
            ("chain_calls_per_row", derived.to_string()),
            ("chain_calls_total", probes.to_string()),
            ("wall_ms", elapsed.as_millis().to_string()),
            (
                "micros_per_row",
                (elapsed.as_micros() / drained.len().max(1) as u128).to_string(),
            ),
            ("probe_methods", format!("{probe_methods:?}")),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Statements per drained row, at two cardinalities in one process
// ---------------------------------------------------------------------------

/// Statements one drained sibling costs on the PostgreSQL wire, derived from
/// the native drain in an isolated fixture database and pinned here so a
/// change to it is a change to this line. Four transactions, in order:
///
/// - `Ledger::claim_candidate`, 5: `BEGIN`; the writer fence
///   (`fatal_error` and the legacy writer lease, read from
///   `qbit_prism_cluster`); the due-work probe, which conditionally allocates
///   one sequence slot via `Ledger::due_work_probe_sql`; the one claiming lane
///   statement, `Ledger::claim_lane_sql` wrapped in the `UPDATE` that takes
///   the token and bumps `attempt_count`; `COMMIT`.
/// - `Coordinator::process_candidate`'s opening `Ledger::renew_candidate_claim`,
///   5: `BEGIN`; the writer fence; the row lock (`FOR NO KEY UPDATE`); the
///   renewing `UPDATE` of `claim_expires_at`; `COMMIT`. The 30-second renewal
///   tick never fires under [`CLAIM_LEASE_SECONDS`], so this is the only
///   renewal a drained row pays for.
/// - `Ledger::observe_chain_view`, inside the pre-offer probe, 5: `BEGIN`;
///   the settlement advisory lock; the writer fence; the cluster read
///   (`FOR UPDATE`); `COMMIT`. The chain is held still under this scenario,
///   so the equal-work branch runs every time and no revision `UPDATE` is
///   issued.
/// - `Ledger::finish_candidate_at_revision`, the abandonment, 11: `BEGIN`;
///   the settlement advisory lock; the order advisory lock; the writer fence;
///   the revision read; the row lock (`FOR KEY SHARE`); the claim fence
///   (token, expiry and state in one read); the maturity check against
///   `qbit_pool_blocks`; the pool-block `UPDATE` that marks an immature block
///   inactive; the terminal `UPDATE` of the outbox row; `COMMIT`.
///
/// Asserted **equal** at [`storm_scale::BASELINE_CANDIDATES`] and at the run's
/// own cardinality before it is compared with this constant, so a drift in the
/// sequence and a dependence on N fail with different messages.
const STATEMENTS_PER_DRAINED_ROW: usize = 26;

/// Rows drained before anything is counted, once [`saturate_ledger_pool`] has
/// opened every pool slot, so whatever the drain path pays only on its first
/// passes (the dispatch sequence's first slot, statement preparation on each
/// pooled connection) is paid before a measured window opens.
const POOL_WARMUP_ROWS: usize = 4;

/// Open every slot of the ledger pool before any measured window, so no
/// measured row can be the one that opens a connection: a connection opened
/// inside a measured window would charge its `after_connect` session settings
/// to whichever row happened to open it.
///
/// The pool returns a dropped connection asynchronously. sqlx spawns a task
/// that pings the server first and only then puts the connection back on the
/// idle queue, so the drain's next acquire can run before that task has,
/// find the idle queue empty and, while the pool is below its maximum, open a
/// new connection rather than wait. On a loaded runner the return task lags
/// long enough for this to happen with two or more connections already open,
/// so draining rows first does not settle the pool: only its maximum does.
/// At the maximum, the same acquire waits for the return instead.
async fn saturate_ledger_pool(pool: &PgPool) -> Result<()> {
    let max = pool.options().get_max_connections();
    let mut held = Vec::with_capacity(max as usize);
    for _ in 0..max {
        held.push(pool.acquire().await?);
    }
    ensure!(
        pool.size() == max,
        "the ledger pool holds {} connections with all {max} of its slots acquired",
        pool.size()
    );
    drop(held);
    Ok(())
}

/// Per-row drain cost in statements is the same at the baseline cardinality
/// and at the run's own, measured in one process against one schema with
/// `support/ledger_execution_proxy.rs` on the wire.
///
/// Every `Execute` and `Query` frame the server was asked to run, on every
/// connection, is counted between one claim and the next; equality is asserted
/// per row, not on an average, and then across the two cardinalities. That
/// equality is what makes the reduced-N CI path prove what a local run at
/// 3,120 proves.
///
/// Each window is enqueued only once the previous one is drained, so both
/// lanes agree on which set is due and the set a measured window finishes is
/// exactly the set it was given. The ledger pool is at its maximum before the
/// first window opens ([`saturate_ledger_pool`]), so no measured row opens a
/// connection. The chain is held still, so the equal-work
/// branch of `Ledger::observe_chain_view` runs for every row: nothing here is
/// admitted, offered or landed, because this is a measurement of what a
/// *storm* row costs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drained_rows_cost_the_same_statements_at_the_baseline_and_storm_cardinalities(
) -> Result<()> {
    let candidates = storm_candidates()?;
    let Some(storm) = Storm::open("prism_storm_statements_", "statements", true).await? else {
        return Ok(());
    };
    let outcome = count_statements(&storm, candidates).await;
    storm.close(outcome).await
}

async fn count_statements(storm: &Storm, candidates: usize) -> Result<()> {
    let proxy = storm
        .proxy
        .clone()
        .context("this scenario needs the execution proxy on the wire")?;
    let ledger = storm.ledger();
    let snapshot = ledger.snapshot(100).await?;
    storm.decide_against_siblings(&snapshot, 0).await?;
    saturate_ledger_pool(&ledger.pool).await?;

    let warmup = storm
        .enqueue_siblings(&snapshot, POOL_WARMUP_ROWS, 1_000_000)
        .await?;
    drain_set(storm, &proxy, &warmup, false).await?;
    let baseline_rows = storm
        .enqueue_siblings(&snapshot, storm_scale::BASELINE_CANDIDATES, 2_000_000)
        .await?;
    let at_baseline = drain_set(storm, &proxy, &baseline_rows, true).await?;
    let storm_rows = storm
        .enqueue_siblings(&snapshot, candidates, 3_000_000)
        .await?;
    let at_storm = drain_set(storm, &proxy, &storm_rows, true).await?;

    ensure!(
        at_baseline.statements == at_storm.statements,
        "a drained row cost {} statements at {} rows and {} at {candidates} rows. \
         Per-row drain cost is not independent of N; this is a finding about the server, not this test",
        at_baseline.statements,
        storm_scale::BASELINE_CANDIDATES,
        at_storm.statements
    );
    ensure!(
        at_storm.statements == STATEMENTS_PER_DRAINED_ROW,
        "a drained row cost {} statements, not the pinned {STATEMENTS_PER_DRAINED_ROW}; the drain's statement sequence changed and the constant's comment must be re-derived with it. What one row ran: {:#?}",
        at_storm.statements,
        at_storm.sequence
    );
    storm_scale::record(
        "drain_statements",
        &[
            ("candidates", candidates.to_string()),
            (
                "baseline_candidates",
                storm_scale::BASELINE_CANDIDATES.to_string(),
            ),
            (
                "statements_per_row_at_baseline",
                at_baseline.statements.to_string(),
            ),
            (
                "statements_per_row_at_storm",
                at_storm.statements.to_string(),
            ),
        ],
    );
    Ok(())
}

/// What one measured drain window observed: the statements every row in it
/// cost, and the statement texts of the first of them, so a failure against
/// [`STATEMENTS_PER_DRAINED_ROW`] reports what the sequence has become rather
/// than only that it moved.
struct DrainCost {
    statements: usize,
    sequence: Vec<String>,
}

/// Drain exactly `expected`, one row at a time, and return the statements each
/// row cost when `measured`. Fails when the rows do not all cost the same, and
/// when a measured window opens on a pool that is no longer at its maximum, so
/// a connection closed since [`saturate_ledger_pool`] is reported as that
/// rather than as the row that had to reopen it.
async fn drain_set(
    storm: &Storm,
    proxy: &ExecutionProxy,
    expected: &BTreeSet<String>,
    measured: bool,
) -> Result<DrainCost> {
    if measured {
        let pool = &storm.ledger().pool;
        let max = pool.options().get_max_connections();
        ensure!(
            pool.size() == max,
            "the ledger pool is at {} of its {max} connections before a measured window; a connection closed since the pool was saturated",
            pool.size()
        );
    }
    let connections = proxy.connections();
    let mut costs: BTreeSet<usize> = BTreeSet::new();
    let mut drained: BTreeSet<String> = BTreeSet::new();
    let mut sequence: Vec<String> = Vec::new();
    while drained.len() < expected.len() {
        let mark = proxy.mark();
        let (hash, _) = drain_one(storm)
            .await?
            .context("the due lane emptied before the set was drained")?;
        let executions = proxy.executions_since(mark)?;
        ensure!(
            expected.contains(&hash),
            "the drain claimed {hash}, which is not in the set under measurement"
        );
        ensure!(drained.insert(hash), "a row was claimed twice");
        if sequence.is_empty() {
            sequence = executions
                .iter()
                .map(|execution| execution.sql.clone())
                .collect();
        }
        costs.insert(executions.len());
    }
    if !measured {
        return Ok(DrainCost {
            statements: 0,
            sequence,
        });
    }
    ensure!(
        proxy.connections() == connections,
        "a pooled connection opened inside the measured window; its session settings would be charged to a drained row"
    );
    ensure!(
        costs.len() == 1,
        "drained rows cost {costs:?} statements; the per-row cost is not one constant. What the first row ran: {sequence:#?}"
    );
    Ok(DrainCost {
        statements: *costs.iter().next().expect("one measured cost"),
        sequence,
    })
}

// ---------------------------------------------------------------------------
// Plan shape and per-claim work over N unfinished due rows
// ---------------------------------------------------------------------------

/// The two claim lanes and the dispatch probe are served by their partial
/// indexes when the outbox holds N *unfinished due* rows, and one claim reads
/// exactly one row.
///
/// `offer_lifecycle.rs`'s
/// `oldest_due_lane_and_dispatch_probe_use_the_unfinished_index_over_retained_history`
/// proves the same plans over 50,000 **retained** rows, where the partial
/// index holds five entries. This is the other side of that question and the
/// storm's actual shape: the same retained history, but with N entries in the
/// unfinished index, all of them due, and N of them never attempted, so the
/// fresh index holds them too. The method is that test's, including refusing
/// to run under a forced planner setting, `ANALYZE`ing first and EXPLAINing
/// the statements `Ledger` itself returns rather than a copy.
///
/// The rows are seeded rather than enqueued because nothing here asserts
/// anything about how a row reached its state: the subject is the plan of the
/// statements the server issues over an outbox of this shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claim_lanes_and_the_dispatch_probe_keep_their_indexes_over_unfinished_due_rows(
) -> Result<()> {
    let candidates = storm_candidates()?;
    let Some(storm) = Storm::open("prism_storm_plans_", "plans", false).await? else {
        return Ok(());
    };
    let outcome = explain_the_lanes(&storm, candidates).await;
    storm.close(outcome).await
}

async fn explain_the_lanes(storm: &Storm, candidates: usize) -> Result<()> {
    let forced: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pg_settings WHERE name IN ('enable_seqscan','enable_sort','enable_indexscan','enable_bitmapscan') AND setting<>'on'",
    )
    .fetch_all(&storm.pool)
    .await?;
    ensure!(forced.is_empty(), "planner settings are forced: {forced:?}");
    seed_retained_history(&storm.pool, RETAINED_HISTORY_ROWS).await?;
    seed_unfinished_due_rows(&storm.pool, candidates as i64).await?;
    sqlx::raw_sql("ANALYZE qbit_block_candidate_outbox")
        .execute(&storm.pool)
        .await?;

    // The two claim lanes must be served in the index's own order at any N:
    // each has to find the *first* due row by its ordering, so a sequential
    // scan or a sort would grow with the backlog. The dispatch probe is a
    // different shape and gets its own check below.
    for (name, statement, index, ordered) in [
        (
            "oldest-due lane",
            Ledger::claim_lane_sql(false),
            "qbit_block_candidate_outbox_unfinished_idx",
            true,
        ),
        (
            "fresh lane",
            Ledger::claim_lane_sql(true),
            "qbit_prism_candidate_fresh_idx",
            true,
        ),
    ] {
        let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {statement}"))
            .fetch_one(&storm.pool)
            .await?;
        let plan = &plan[0]["Plan"];
        let mut nodes = Vec::new();
        plan_nodes(plan, &mut nodes);
        let scans: Vec<(&str, &str)> = nodes
            .iter()
            .filter(|node| {
                node["Relation Name"] == "qbit_block_candidate_outbox"
                    || node["Node Type"] == "Bitmap Index Scan"
            })
            .map(|node| {
                (
                    node["Node Type"].as_str().unwrap_or(""),
                    node["Index Name"].as_str().unwrap_or(""),
                )
            })
            .collect();
        println!("{name} over {candidates} unfinished due rows: {scans:?}");
        ensure!(
            scans.iter().any(|(kind, used)| *used == index
                && matches!(
                    *kind,
                    "Index Scan" | "Index Only Scan" | "Bitmap Index Scan"
                )),
            "{name}: not served by {index}: {plan}"
        );
        ensure!(
            !scans.iter().any(|(kind, _)| *kind == "Seq Scan"),
            "{name}: scans the whole outbox: {plan}"
        );
        if ordered {
            ensure!(
                !nodes.iter().any(|node| node["Node Type"] == "Sort"),
                "{name}: sorts the outbox: {plan}"
            );
            ensure!(
                scans
                    .iter()
                    .any(|(kind, used)| *used == index && *kind == "Index Scan"),
                "{name}: not an ordered index scan: {plan}"
            );
        }
    }

    // The pre-#432 EXISTS probe could choose a sequential scan at storm
    // size: volatile clock predicates caused the planner to expect an early
    // match, but an idle backed-off population scanned retained history too.
    // #432 replaced that probe with an ordered LIMIT 1 derived query using
    // the same advancing clocks and existing unfinished partial index.
    //
    // This suite still records busy/idle plans at the run's cardinality and
    // asserts index use at baseline. The separate candidate_dispatch_probe
    // target checks canonical direct/prepared plans at exact unfinished and
    // retained populations, including churn; no universal cost bound follows.
    let busy_probe = probe_plan_nodes(storm).await?;
    sqlx::query(&format!(
        "UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE state IN {}",
        CandidateState::UNFINISHED_SQL
    ))
    .execute(&storm.pool)
    .await?;
    sqlx::raw_sql("ANALYZE qbit_block_candidate_outbox")
        .execute(&storm.pool)
        .await?;
    let idle_probe = probe_plan_nodes(storm).await?;

    // Trim the unfinished set to the baseline and re-plan, retaining this
    // suite's baseline index-use assertion alongside the recorded storm-size
    // plans.
    sqlx::query(&format!(
        "DELETE FROM qbit_block_candidate_outbox WHERE state IN {} AND block_hash LIKE 'b%' AND block_hash > 'b'||lpad(to_hex($1::bigint),63,'0')",
        CandidateState::UNFINISHED_SQL
    ))
    .bind(storm_scale::BASELINE_CANDIDATES as i64)
    .execute(&storm.pool)
    .await?;
    sqlx::raw_sql("ANALYZE qbit_block_candidate_outbox")
        .execute(&storm.pool)
        .await?;
    let baseline_idle_probe = probe_plan_nodes(storm).await?;
    let unfinished: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state IN {}",
        CandidateState::UNFINISHED_SQL
    ))
    .fetch_one(&storm.pool)
    .await?;
    ensure!(
        baseline_idle_probe.iter().any(|(kind, used)| {
            used == "qbit_block_candidate_outbox_unfinished_idx"
                && matches!(
                    kind.as_str(),
                    "Index Scan" | "Index Only Scan" | "Bitmap Index Scan"
                )
        }) && !baseline_idle_probe
            .iter()
            .any(|(kind, _)| kind == "Seq Scan"),
        "idle dispatch probe over {unfinished} unfinished rows is not served by \
         the unfinished index: {baseline_idle_probe:?}"
    );
    storm_scale::record(
        "dispatch_probe_plans",
        &[
            ("candidates", candidates.to_string()),
            ("retained_rows", RETAINED_HISTORY_ROWS.to_string()),
            ("busy_at_storm", format!("{busy_probe:?}")),
            ("idle_at_storm", format!("{idle_probe:?}")),
            ("baseline_unfinished_rows", unfinished.to_string()),
            ("idle_at_baseline", format!("{baseline_idle_probe:?}")),
        ],
    );
    // Restore the due backlog for the executed claim below.
    sqlx::query(&format!(
        "UPDATE qbit_block_candidate_outbox SET next_attempt_at=created_at WHERE state IN {}",
        CandidateState::UNFINISHED_SQL
    ))
    .execute(&storm.pool)
    .await?;
    sqlx::raw_sql("ANALYZE qbit_block_candidate_outbox")
        .execute(&storm.pool)
        .await?;

    // One claim, executed: the work it does must not grow with the siblings.
    // Rolled back, so the row locks the lane takes are released.
    let mut tx = storm.pool.begin().await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}",
        Ledger::claim_lane_sql(false)
    ))
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await?;
    let plan = &plan[0]["Plan"];
    let mut nodes = Vec::new();
    plan_nodes(plan, &mut nodes);
    let scanned = nodes
        .iter()
        .find(|node| node["Relation Name"] == "qbit_block_candidate_outbox")
        .context("the claim's plan never reads the outbox")?;
    ensure!(
        scanned["Actual Rows"].as_f64() == Some(1.0),
        "one claim read {} rows of the outbox, not one: {plan}",
        scanned["Actual Rows"]
    );
    for node in &nodes {
        let removed = node["Rows Removed by Filter"].as_f64().unwrap_or(0.0);
        ensure!(
            removed == 0.0,
            "one claim discarded {removed} rows by filter, so it walked the siblings: {plan}"
        );
    }
    let hit = plan["Shared Hit Blocks"].as_i64().unwrap_or(-1);
    let read = plan["Shared Read Blocks"].as_i64().unwrap_or(-1);
    storm_scale::record(
        "claim_plan",
        &[
            ("candidates", candidates.to_string()),
            (
                "baseline_candidates",
                storm_scale::BASELINE_CANDIDATES.to_string(),
            ),
            ("retained_rows", RETAINED_HISTORY_ROWS.to_string()),
            ("shared_hit_blocks", hit.to_string()),
            ("shared_read_blocks", read.to_string()),
            ("actual_rows", scanned["Actual Rows"].to_string()),
        ],
    );
    Ok(())
}

/// Terminal history the outbox retains beside the storm. Copied from
/// `offer_lifecycle.rs`, so the two index tests read their plans against the
/// same retained table.
async fn seed_retained_history(pool: &PgPool, terminal_rows: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,attempt_count,created_at,next_attempt_at,completed_at) \
         SELECT lpad(to_hex(i),64,'0'),NULL,lpad(to_hex(i),64,'0'),CASE WHEN i%7=0 THEN 'abandoned' ELSE 'submitted' END,1+(i%3), \
                clock_timestamp()-(i||' seconds')::interval,clock_timestamp()-(i||' seconds')::interval,clock_timestamp()-(i||' seconds')::interval \
         FROM generate_series(1,$1::bigint) AS g(i)",
    )
    .bind(terminal_rows)
    .execute(pool)
    .await?;
    Ok(())
}

/// The storm, as the outbox holds it: `siblings` never-attempted pending rows
/// behind one decided height, every one of them due, plus one row in each
/// other unfinished state so the partial index's whole predicate is
/// populated.
/// The outbox scan nodes of the dispatch probe's plan, as
/// `(node type, index name)` pairs.
async fn probe_plan_nodes(storm: &Storm) -> Result<Vec<(String, String)>> {
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (FORMAT JSON) {}",
        Ledger::due_work_probe_sql()
    ))
    .fetch_one(&storm.pool)
    .await?;
    let plan = plan[0]["Plan"].clone();
    let mut nodes = Vec::new();
    plan_nodes(&plan, &mut nodes);
    Ok(nodes
        .iter()
        .filter(|node| {
            node["Relation Name"] == "qbit_block_candidate_outbox"
                || node["Node Type"] == "Bitmap Index Scan"
        })
        .map(|node| {
            (
                node["Node Type"].as_str().unwrap_or("").to_owned(),
                node["Index Name"].as_str().unwrap_or("").to_owned(),
            )
        })
        .collect())
}

async fn seed_unfinished_due_rows(pool: &PgPool, siblings: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256,state,attempt_count,created_at,next_attempt_at) \
         SELECT 'b'||lpad(to_hex(i),63,'0'),'{\"k\":1}'::jsonb,lpad(to_hex(i),64,'0'),decode('00','hex'),1,repeat('b1',32),'pending',0, \
                clock_timestamp()-(i||' seconds')::interval,clock_timestamp()-(i||' seconds')::interval \
         FROM generate_series(1,$1::bigint) AS g(i)",
    )
    .bind(siblings)
    .execute(pool)
    .await?;
    sqlx::raw_sql(
        r#"INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256,state,attempt_count,created_at,next_attempt_at,offer_reserved_at,offer_reserved_by,offered_at_ms,offer_outcome,last_error) VALUES
           (repeat('c3',32),'{"k":3}',repeat('c3',32),decode('00','hex'),1,repeat('b3',32),'offer_reserved',1,clock_timestamp()-interval '5 minutes',clock_timestamp()-interval '4 minutes',clock_timestamp()-interval '5 minutes','fe-1',NULL,NULL,NULL),
           (repeat('c4',32),'{"k":4}',repeat('c4',32),decode('00','hex'),1,repeat('b4',32),'offered',1,clock_timestamp()-interval '6 minutes',clock_timestamp()-interval '5 minutes',clock_timestamp()-interval '6 minutes','fe-1',1700000000456,'accepted',NULL),
           (repeat('c5',32),'{"k":5}',repeat('c5',32),decode('00','hex'),1,repeat('b5',32),'reconciliation',4,clock_timestamp()-interval '2 hours',clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '2 hours','fe-2',NULL,'unknown','delivery unknown')"#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Admission against a backlog of N
// ---------------------------------------------------------------------------

/// With N due siblings backlogged, the fresh lane still serves brand-new work
/// first, and the oldest-due lane still gets through the work it owes.
///
/// `ledger_postgres.rs`'s
/// `candidate_dispatch_prioritizes_fresh_work_and_services_oldest_due_fairly`
/// is this shape at a handful of rows; here the backlog is the storm's. Each
/// round enqueues a block and asserts, by hash, that the next fresh-lane claim
/// is that block and not one of the N rows already owed. The lane is predicted
/// from the dispatch sequence rather than assumed: every eighth slot belongs
/// to the oldest-due lane, and spending those is how the second half of the
/// property — that the fresh lane's priority has not starved the due lane —
/// is proven, with an old never-attempted row and a due retry row both served.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_serves_new_work_first_while_the_due_backlog_is_still_owed() -> Result<()> {
    let candidates = storm_candidates()?;
    let Some(storm) = Storm::open("prism_storm_admission_", "admission", false).await? else {
        return Ok(());
    };
    let outcome = admit_against_the_backlog(&storm, candidates).await;
    storm.close(outcome).await
}

async fn admit_against_the_backlog(storm: &Storm, candidates: usize) -> Result<()> {
    let ledger = storm.ledger();
    let snapshot = ledger.snapshot(100).await?;
    let backlog = storm.enqueue_siblings(&snapshot, candidates, 0).await?;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET created_at=clock_timestamp()-interval '10 minutes',next_attempt_at=clock_timestamp()-interval '10 minutes'")
        .execute(&storm.pool)
        .await?;

    // Two rows the oldest-due lane owed before the storm arrived: work that
    // was never attempted at all, and a retry that has been attempted.
    let old_unattempted = storm.sibling(&snapshot, 900_001)?.block_hash;
    let retry = storm.sibling(&snapshot, 900_002)?.block_hash;
    for nonce in [900_001u32, 900_002] {
        let candidate = storm.sibling(&snapshot, nonce)?;
        let hash = candidate.block_hash.clone();
        ensure!(
            ledger.enqueue_candidate_once(candidate).await?,
            "the owed row {hash} was not enqueued"
        );
    }
    sqlx::query("UPDATE qbit_block_candidate_outbox SET created_at=clock_timestamp()-interval '2 hours',next_attempt_at=clock_timestamp()-interval '2 hours' WHERE block_hash=$1")
        .bind(&old_unattempted)
        .execute(&storm.pool)
        .await?;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET attempt_count=3,created_at=clock_timestamp()-interval '1 hour',next_attempt_at=clock_timestamp()-interval '1 hour' WHERE block_hash=$1")
        .bind(&retry)
        .execute(&storm.pool)
        .await?;

    let mut served_due: BTreeSet<String> = BTreeSet::new();
    for round in 1..=ADMISSION_ROUNDS {
        let candidate = storm.sibling(&snapshot, 800_000 + round as u32)?;
        let hash = candidate.block_hash.clone();
        ensure!(
            ledger.enqueue_candidate_once(candidate).await?,
            "round {round}: admitted work was not enqueued"
        );
        if (storm.dispatch_slots().await? + 1) % DISPATCH_CYCLE == 0 {
            let due = ledger
                .claim_candidate(CLAIM_LEASE_SECONDS)
                .await?
                .context("the reserved oldest-due slot found no work")?;
            served_due.insert(due.candidate.block_hash.clone());
            ledger.finish_candidate(&due, false, None).await?;
        }
        let claim = ledger
            .claim_candidate(CLAIM_LEASE_SECONDS)
            .await?
            .context("admitted work was not claimable")?;
        ensure!(
            claim.candidate.block_hash == hash,
            "round {round}: a backlog of {} due rows starved admission; the fresh lane returned {}",
            backlog.len(),
            claim.candidate.block_hash
        );
        ledger.finish_candidate(&claim, false, None).await?;
    }
    ensure!(
        served_due.contains(&old_unattempted),
        "the old never-attempted row starved behind {ADMISSION_ROUNDS} rounds of new work"
    );
    ensure!(
        served_due.contains(&retry),
        "the due retry row starved behind {ADMISSION_ROUNDS} rounds of new work"
    );
    storm_scale::record(
        "admission",
        &[
            ("candidates", candidates.to_string()),
            (
                "baseline_candidates",
                storm_scale::BASELINE_CANDIDATES.to_string(),
            ),
            ("backlog_rows", backlog.len().to_string()),
            ("rounds", ADMISSION_ROUNDS.to_string()),
            ("oldest_due_rows_served", served_due.len().to_string()),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The cardinality reader's own boundary
// ---------------------------------------------------------------------------

/// The suite's one cardinality reader refuses a value below its floor, above
/// its ceiling, and one that is not a count, each naming the variable
/// (EP-VALIDATION), and reaches the same value at runtime whether the variable
/// is absent or set (EP-CONFIG). Its first consumer proves its boundary.
///
/// The variable is set and restored in process, under [`CARDINALITY`], because
/// the reader is the thing under test: spawning a process would prove a
/// launcher's behaviour instead. This needs no database, so it is not a gated
/// test and is not in `test/prism-gated-tests.txt`.
#[test]
fn storm_candidates_refuses_a_cardinality_outside_its_own_bounds() -> Result<()> {
    let _guard = CARDINALITY.lock().expect("cardinality lock");
    let original = std::env::var(storm_scale::CANDIDATES_VAR).ok();
    let outcome = check_cardinality_bounds();
    match original {
        Some(value) => std::env::set_var(storm_scale::CANDIDATES_VAR, value),
        None => std::env::remove_var(storm_scale::CANDIDATES_VAR),
    }
    outcome
}

fn check_cardinality_bounds() -> Result<()> {
    for (value, what) in [
        (
            (storm_scale::DEFAULT_CANDIDATES - 1).to_string(),
            "below the floor",
        ),
        (
            (storm_scale::MAX_CANDIDATES + 1).to_string(),
            "above the ceiling",
        ),
        ("one hundred".to_owned(), "malformed"),
    ] {
        std::env::set_var(storm_scale::CANDIDATES_VAR, &value);
        let error = storm_scale::storm_candidates()
            .err()
            .with_context(|| format!("the {what} value {value:?} was accepted"))?;
        let rendered = format!("{error:#}");
        ensure!(
            rendered.contains(storm_scale::CANDIDATES_VAR),
            "the refusal of the {what} value {value:?} does not name the variable: {rendered}"
        );
    }
    std::env::remove_var(storm_scale::CANDIDATES_VAR);
    ensure!(
        storm_scale::storm_candidates()? == storm_scale::DEFAULT_CANDIDATES,
        "an absent {} did not reach the reader as {}",
        storm_scale::CANDIDATES_VAR,
        storm_scale::DEFAULT_CANDIDATES
    );
    let observed = storm_scale::OBSERVED_STORM_CANDIDATES.to_string();
    std::env::set_var(storm_scale::CANDIDATES_VAR, &observed);
    ensure!(
        storm_scale::storm_candidates()? == storm_scale::OBSERVED_STORM_CANDIDATES,
        "the incident's cardinality did not reach the reader through {}",
        storm_scale::CANDIDATES_VAR
    );
    Ok(())
}
