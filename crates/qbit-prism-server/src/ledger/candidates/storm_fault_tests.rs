//! Recovery at each cleanup dependency of a drained candidate (#270).
//!
//! #270 names four dependencies a drain leans on — the chain probe, the
//! landing transaction, the terminal update and the lease renewal — and asks
//! for one recovery case each, leaving the row in a consistent state. Those
//! are native seams: 2.x.x's fault vocabulary was eight cleanup steps of the
//! set-oriented collapse selector (`lab/prism/block_candidates.py`), and #270
//! fences that selector out of scope, so the mapping is derived here rather
//! than ported.
//!
//! The faults in [`super::faults`] are #387's quarantine vocabulary and sit on
//! the decode and park path; they cover none of the four, and they stay where
//! they are. They are reused only where a fault interacts with quarantine.
//!
//! Each case asserts the row is left claimable with its offer record intact,
//! its `attempt_count` incremented exactly once, and no second `submitblock`
//! reachable for that hash. Faults are keyed by block hash, so a case claiming
//! one row never meets another case's fault. Where a fault releases the row,
//! the severed attempt's token is stale from that moment: EP-STATE, so the
//! landing case also drives delayed work under it and proves it cannot write
//! over the replacement owner's claim.
//!
//! **How each dependency is faulted.** The database dependencies use
//! `tests/support/ledger_execution_proxy.rs` on the wire, which withholds one
//! server acknowledgement from the client: either the completion of a *marked*
//! statement, leaving the transaction open to abort, or the completion of the
//! `COMMIT` that followed it, after the write is already durable. What counts
//! as "marked" is decided by a row-level fixture trigger whose `WHEN` clause
//! names one block hash, so a fault can never fire on a neighbour's row. The
//! two outbox `UPDATE`s a drain issues are told apart by what they change:
//! terminalization moves `state` into a terminal value, while renewal moves
//! `claim_expires_at` with `state` unchanged.
//!
//! The chain probe is a node dependency, not a database one: the RPCs carry no
//! candidate identity, so its faults are armed around one drain rather than
//! keyed by hash, and the isolation they are read against is the untouched
//! neighbour row.
//!
//! **What is asserted is the reachable outcome, not a blanket retry rule.** A
//! transaction that committed terminally stays terminal and is not resurrected
//! (EP-ERRORS: reconcile, never repeat the side effect); a rolled-back or
//! unfinished attempt is recoverable, and the next legal attempt is driven to
//! show it without a second `submitblock`. An unknown outcome stays unknown.
//!
//! Each test owns one disposable database, from the [`FixtureDatabase`] below:
//! PostgreSQL scopes advisory locks to a database, and the ledger's order and
//! settlement locks are cluster-wide constants, so a unique schema alone would
//! queue these fixtures on each other. The wire proxy *is* included from
//! `tests/support/`, as `coordinator::window_incident_tests` includes the
//! window fixture; the database helper is adapted instead, because this crate's
//! other storm module needs it too and one path cannot be loaded by two
//! modules of one target (`clippy::duplicate_mod`).
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=... PRISM_TEST_REQUIRE_INTEGRATION=1 \
//!   cargo test -p qbit-prism-server --lib ledger::candidates::storm_fault_tests -- --test-threads=1
//! ```

use super::*;
use crate::codec;
use crate::config::Config;
use crate::coordinator::Coordinator;
use anyhow::anyhow;
use axum::{extract::State, routing::post, Json, Router};
use qbit_prism_test_gate as gate;
use serde_json::json;
use sqlx::Connection;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

// The proxy file carries its own `#![allow(dead_code)]`.
use crate::ledger::execution_proxy::{
    target_executions, target_statement_count, Execution, ExecutionProxy, Fault, FaultPhase,
    Outcome,
};

/// The outbox, as the marker triggers and the proxy faults name it.
const OUTBOX: &str = "qbit_block_candidate_outbox";
/// The landing transaction's first marked write.
const POOL_BLOCKS: &str = "qbit_pool_blocks";
/// The height the fixture's candidates are found at.
const FOUND_HEIGHT: u64 = 101;
/// The network difficulty the fixture's window and blocks are built at.
const NETWORK_DIFFICULTY: u128 = 100;
/// Every wait in this module.
const BUDGET: Duration = Duration::from_secs(30);
/// The small fixed population each case drains: one row that meets the fault
/// and two neighbours that must not.
const POPULATION: u32 = 3;

// ---------------------------------------------------------------------------
// The fake node
// ---------------------------------------------------------------------------

/// A block hash that depends on its parent and its height (EP-VALIDATION).
fn fake_hash(parent: &str, height: u64) -> String {
    hex::encode(Sha256::digest(
        format!("prism-storm-fault:{parent}:{height}").as_bytes(),
    ))
}

struct NodeState {
    /// The active chain, indexed by height.
    chain: Vec<String>,
    /// `submitblock` calls per block hash: "offered once" is read from here.
    submissions: BTreeMap<String, usize>,
    /// Whether an accepted block becomes the tip. Off by default so that
    /// draining one row of a population does not supersede the others.
    extend_on_submit: bool,
    /// Once any block has been offered, `getblockchaininfo` fails with this
    /// reason. Arming it on the offer is what places the fault in the
    /// *post-offer* observation, deterministically and without a seam: the
    /// pre-offer probe has already run by then.
    chain_info_failure_after_offer: Option<String>,
    /// Once any block has been offered, the next `getbestblockhash` answers a
    /// foreign hash: the tip moving between the two reads of one coherent
    /// observation. One-shot, so the retry that follows observes a stable tip.
    shift_tip_after_offer: bool,
    /// Whether any `submitblock` has been seen, which is what arms the two
    /// faults above.
    offered: bool,
}

impl NodeState {
    fn tip(&self) -> &str {
        self.chain.last().expect("the chain has a genesis block")
    }

    fn tip_height(&self) -> u64 {
        self.chain.len() as u64 - 1
    }

    fn chainwork(&self) -> String {
        format!("{:064x}", self.chain.len())
    }

    fn submissions_of(&self, block_hash: &str) -> usize {
        self.submissions.get(block_hash).copied().unwrap_or(0)
    }
}

fn rpc_error(message: &str) -> Json<Value> {
    Json(json!({"id":Value::Null,"result":Value::Null,"error":{"code":-1,"message":message}}))
}

async fn node_reply(
    State(node): State<Arc<AsyncMutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut node = node.lock().await;
    let result = match request["method"].as_str().unwrap() {
        "getblockhash" => {
            let height = request["params"][0].as_u64().unwrap();
            match node.chain.get(usize::try_from(height).unwrap()) {
                Some(hash) => json!(hash),
                // A node has no block above its tip, and neither has this one.
                None => return rpc_error("Block height out of range"),
            }
        }
        "getblockchaininfo" => {
            if let Some(reason) = node.chain_info_failure_after_offer.clone() {
                if node.offered {
                    return rpc_error(&reason);
                }
            }
            json!({"chain":"test","initialblockdownload":false,"blocks":node.tip_height(),
                "headers":node.tip_height(),"bestblockhash":node.tip(),"chainwork":node.chainwork()})
        }
        "getbestblockhash" => {
            if node.offered && std::mem::take(&mut node.shift_tip_after_offer) {
                json!(fake_hash(node.tip(), node.tip_height() + 1))
            } else {
                json!(node.tip())
            }
        }
        "getnetworkinfo" => json!({"connections":2}),
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap()).unwrap();
            let hash = codec::hash_display(&codec::double_sha256(&block[..80]));
            *node.submissions.entry(hash.clone()).or_default() += 1;
            node.offered = true;
            if node.extend_on_submit {
                node.chain.push(hash);
            }
            Value::Null
        }
        method => panic!("unexpected candidate RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

// ---------------------------------------------------------------------------
// The outbox row
// ---------------------------------------------------------------------------

/// Every column a cleanup-dependency verdict is read from.
#[derive(Debug, PartialEq)]
struct Row {
    state: String,
    claimed: bool,
    attempts: i32,
    last_error: Option<String>,
    reserved_by: Option<String>,
    offered_at_ms: Option<i64>,
    outcome: Option<String>,
    completed: bool,
    payload: bool,
}

impl Row {
    /// The offer record this row reached, whatever happened afterwards.
    fn offer_kept(&self, outcome: &str, instance: &str) -> bool {
        self.outcome.as_deref() == Some(outcome)
            && self.offered_at_ms.is_some()
            && self.reserved_by.as_deref() == Some(instance)
    }
}

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

struct Fixture {
    database: Option<FixtureDatabase>,
    /// Reads, marker triggers and the population's setup, never through the
    /// proxy: a severed drain must not take the test's own observations with it.
    direct: PgPool,
    proxy: Arc<ExecutionProxy>,
    coordinator: Arc<Coordinator>,
    node: Arc<AsyncMutex<NodeState>>,
    server: JoinHandle<()>,
    snapshot: Snapshot,
    parent: String,
}

/// The instance id the fixture's coordinator claims and reserves under.
const INSTANCE: &str = "storm-fault";

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let database = FixtureDatabase::open(&raw, "prism_storm_fault_").await?;
        let url = database.url.clone();
        Ok(Some(Self::build(database, url).await?))
    }

    async fn build(database: FixtureDatabase, url: String) -> Result<Self> {
        let mut chain = vec!["00".repeat(32)];
        for height in 1..FOUND_HEIGHT {
            let hash = fake_hash(chain.last().unwrap(), height);
            chain.push(hash);
        }
        let parent = chain.last().unwrap().clone();
        let node = Arc::new(AsyncMutex::new(NodeState {
            chain,
            submissions: BTreeMap::new(),
            extend_on_submit: false,
            chain_info_failure_after_offer: None,
            shift_tip_after_offer: false,
            offered: false,
        }));
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) => return Err(database.abandon(error.into()).await),
        };
        let rpc_url = match listener.local_addr() {
            Ok(addr) => format!("http://{addr}/"),
            Err(error) => return Err(database.abandon(error.into()).await),
        };
        let app = Router::new()
            .route("/", post(node_reply))
            .with_state(node.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let built = async {
            let direct = PgPool::connect(&url).await?;
            let parsed = url::Url::parse(&url)?;
            let host = parsed.host_str().context("the database URL names a host")?;
            let port = parsed.port().unwrap_or(5432);
            let upstream = tokio::net::lookup_host((host, port))
                .await?
                .next()
                .context("the database host resolves")?;
            let proxy = Arc::new(ExecutionProxy::start(upstream).await?);
            let coordinator = Coordinator::new(
                fixture_config(&proxy.rewrite_url(&url)?, &rpc_url),
                Arc::new(crate::metrics::Metrics::default()),
            )
            .await?;
            coordinator
                .ledger
                .observe_chain_view(&parent, FOUND_HEIGHT - 1, &format!("{:064x}", FOUND_HEIGHT))
                .await?;
            coordinator.ledger.append(seed_share(), None).await?;
            let snapshot = coordinator.ledger.snapshot(NETWORK_DIFFICULTY).await?;
            anyhow::Ok((direct, proxy, coordinator, snapshot))
        }
        .await;
        match built {
            Ok((direct, proxy, coordinator, snapshot)) => Ok(Self {
                database: Some(database),
                direct,
                proxy,
                coordinator,
                node,
                server,
                snapshot,
                parent,
            }),
            Err(error) => {
                server.abort();
                Err(database.abandon(error).await)
            }
        }
    }

    /// Enqueue and claim `POPULATION` candidates, so a case can fault one and
    /// read the others as neighbours. Every row is enqueued before any claim
    /// is taken and each claim is kept, so the population leaves every row at
    /// exactly one attempt whichever lane the claim came from; a loop that
    /// discarded a claim it did not want would silently bump a neighbour.
    async fn population(&self) -> Result<Vec<CandidateClaim>> {
        let mut hashes = Vec::new();
        for index in 0..POPULATION {
            let candidate = found_on(&self.snapshot, &self.parent, index * 20_000)?;
            hashes.push(candidate.block_hash.clone());
            self.coordinator
                .ledger
                .enqueue_candidate_observed(candidate, Some(crate::coordinator::unix_ms_now()?))
                .await?;
        }
        let mut claims = Vec::new();
        for _ in 0..POPULATION {
            claims.push(
                self.coordinator
                    .ledger
                    .claim_candidate(120)
                    .await?
                    .context("a candidate of the population was not claimable")?,
            );
        }
        // Order them as they were enqueued; the lanes decide the claim order.
        hashes
            .iter()
            .map(|hash| {
                claims
                    .iter()
                    .find(|claim| &claim.candidate.block_hash == hash)
                    .cloned()
                    .with_context(|| format!("candidate {hash} was never claimed"))
            })
            .collect()
    }

    /// Mark the statements that write `block_hash`, so the proxy can name one
    /// of them without spelling its SQL. `when` restricts the marker to the
    /// one statement of `table`/`op` a case is about.
    async fn mark(&self, name: &str, table: &str, op: &str, when: &str) -> Result<()> {
        sqlx::raw_sql(&format!(
            "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $marker$ \
             BEGIN RAISE NOTICE 'prism-execution-marker % %', TG_TABLE_NAME, TG_OP; RETURN NULL; END $marker$; \
             CREATE TRIGGER {name} AFTER {op} ON {table} FOR EACH ROW WHEN ({when}) \
             EXECUTE FUNCTION {name}();"
        ))
        .execute(&self.direct)
        .await?;
        Ok(())
    }

    async fn row(&self, block_hash: &str) -> Result<Row> {
        let row = sqlx::query_as::<_, (String, bool, i32, Option<String>, Option<String>, Option<i64>, Option<String>, bool, bool)>(
            "SELECT state,claim_token IS NOT NULL,attempt_count,last_error,offer_reserved_by,offered_at_ms,offer_outcome,completed_at IS NOT NULL,candidate IS NOT NULL AND block_bytes IS NOT NULL AND window_anchor_ms IS NOT NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&self.direct)
        .await?;
        Ok(Row {
            state: row.0,
            claimed: row.1,
            attempts: row.2,
            last_error: row.3,
            reserved_by: row.4,
            offered_at_ms: row.5,
            outcome: row.6,
            completed: row.7,
            payload: row.8,
        })
    }

    async fn submissions_of(&self, block_hash: &str) -> usize {
        self.node.lock().await.submissions_of(block_hash)
    }

    async fn landed(&self, block_hash: &str) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_pool_audit_bundles WHERE block_hash=$1)",
        )
        .bind(block_hash)
        .fetch_one(&self.direct)
        .await?)
    }

    async fn pool_block(&self, block_hash: &str) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
                .bind(block_hash)
                .fetch_optional(&self.direct)
                .await?,
        )
    }

    /// End a claim's lease so the next legal attempt can take the row.
    async fn expire(&self, block_hash: &str) -> Result<()> {
        sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second',next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(block_hash)
            .execute(&self.direct)
            .await?;
        Ok(())
    }

    /// Expire the row and take the next legal claim, which must be this row:
    /// every neighbour still holds the live claim the population gave it, so a
    /// claim of anything else would mean the population had already moved.
    async fn reclaim(&self, block_hash: &str) -> Result<CandidateClaim> {
        self.expire(block_hash).await?;
        let claim = self
            .coordinator
            .ledger
            .claim_candidate(120)
            .await?
            .context("the row was not claimable again")?;
        ensure!(
            claim.candidate.block_hash == block_hash,
            "the next legal claim took {} instead of {block_hash}",
            claim.candidate.block_hash
        );
        Ok(claim)
    }

    async fn drain(&self, claim: &CandidateClaim) -> Result<Result<()>> {
        tokio::time::timeout(BUDGET, self.coordinator.process_candidate(claim))
            .await
            .context("the drain did not finish")
    }

    /// Every neighbour is exactly as the population left it: claimed, pending,
    /// one attempt, no offer record and nothing at the node.
    async fn assert_untouched(&self, neighbours: &[&CandidateClaim]) -> Result<()> {
        for claim in neighbours {
            let hash = &claim.candidate.block_hash;
            let row = self.row(hash).await?;
            ensure!(
                row.state == "pending"
                    && row.claimed
                    && row.attempts == 1
                    && row.payload
                    && !row.completed,
                "a neighbour row was changed by another row's fault: {row:?}"
            );
            ensure!(
                row.outcome.is_none() && row.offered_at_ms.is_none() && row.reserved_by.is_none(),
                "a neighbour row gained an offer record: {row:?}"
            );
            ensure!(
                row.last_error.is_none(),
                "a neighbour row recorded another row's failure: {row:?}"
            );
            ensure!(
                self.submissions_of(hash).await == 0,
                "a neighbour block was offered"
            );
        }
        Ok(())
    }

    async fn close(mut self, result: Result<()>) -> Result<()> {
        self.server.abort();
        let proxy = self.proxy.finish().await;
        self.coordinator.ledger.pool.close().await;
        self.direct.close().await;
        let database = self.database.take().expect("the database is closed once");
        database
            .close(match (result, proxy) {
                (Ok(()), proxy) => proxy,
                (error, _) => error,
            })
            .await
    }
}

fn fixture_config(database_url: &str, rpc_url: &str) -> Config {
    Config {
        database_url: database_url.into(),
        instance_id: INSTANCE.into(),
        database_connections: 6,
        initialize_schema: true,
        chain: "testnet".into(),
        expected_genesis_hash: None,
        min_peers: 1,
        template_max_age: Duration::from_secs(120),
        submit_tip_max_age: Duration::from_secs(10),
        template_refresh_failure_exit: Duration::from_secs(120),
        rpc_url: rpc_url.into(),
        rpc_user: "test".into(),
        rpc_password: "test".into(),
        rpc_timeout: Duration::from_secs(5),
        block_submit_timeout: Duration::from_secs(5),
        poll_interval: Duration::from_secs(1),
        blockwait: false,
        build_workers: 1,
        runtime_workers: 2,
        snapshot_interval: Duration::from_secs(60),
        health_timeout: Duration::from_secs(15),
        share_commit_timeout: Duration::from_secs(15),
        share_commit_grace: Duration::from_secs(5),
        block_only_ack_timeout: Duration::from_secs(60),
        candidate_orphan_confirmations: 6,
        extranonce2_size: 8,
        coinbase_tag: "/PRISM/".into(),
        manifest_seed: "11".repeat(32),
        ledger_seed: "22".repeat(32),
        ledger_public_key: ManifestSigningKey::from_seed_hex(&"22".repeat(32))
            .unwrap()
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
        version_mask: codec::VERSION_ROLLING_MASK,
        audit_bind: "127.0.0.1".into(),
        audit_port: 0,
    }
}

fn seed_share() -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("miner:{}", "11".repeat(32)),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 100,
        network_difficulty: 100,
        template_height: FOUND_HEIGHT - 1,
        job_id: "seed".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

/// A block found on the fixture's window with the fixture's keys, extending
/// `parent` at `FOUND_HEIGHT`.
fn found_on(snapshot: &Snapshot, parent: &str, nonce_start: u32) -> Result<Candidate> {
    let manifest_key = ManifestSigningKey::from_seed_hex(&"11".repeat(32))?;
    let ledger_key = ManifestSigningKey::from_seed_hex(&"22".repeat(32))?;
    let bundle = qbit_prism::build_audit_bundle_with_coinbase_options(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: FOUND_HEIGHT,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        qbit_prism::PayoutPolicy::day_one_default(),
        Some("00".repeat(12)),
        vec![],
        &manifest_key,
        &ledger_key,
    )?;
    let template = json!({"version":0x20000000u32,"bits":"207fffff","curtime":1_800_000_000u32,
        "previousblockhash":parent,"transactions":[]});
    let job = codec::Job::from_manifest(
        format!("storm-fault-{nonce_start}"),
        &template,
        &bundle.signed_coinbase_manifest.manifest,
        "00000000",
        8,
        1e-12,
        0.0,
        true,
    )?;
    let proof = (nonce_start..nonce_start + 20_000u32)
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
        payout_policy: qbit_prism::PayoutPolicy::day_one_default(),
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

// ---------------------------------------------------------------------------
// Dependency 1: the chain observation
// ---------------------------------------------------------------------------

/// The post-offer chain observation fails outright for one row and shows a
/// moving tip for another. Neither settles anything: an observation that did
/// not complete proves nothing about the chain, so both rows keep their whole
/// offer record and reach reconciliation with the reason, released and
/// claimable. The third row of the population never sees either fault. The
/// recovery drives the next legal attempt on the first row, with the chain
/// observable again and the block active, and it confirms with no second
/// `submitblock`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_or_shifting_chain_observation_leaves_the_drain_recoverable() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claims = fixture.population().await?;
        let (failed, shifted, neighbour) = (&claims[0], &claims[1], &claims[2]);

        // The observation is unavailable after the call.
        fixture.node.lock().await.chain_info_failure_after_offer =
            Some("injected chain observation failure".into());
        fixture.drain(failed).await??;
        let hash = failed.candidate.block_hash.clone();
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "reconciliation" && !row.claimed && row.attempts == 1 && !row.completed,
            "a failed observation did not leave a reconciliation row: {row:?}"
        );
        ensure!(
            row.offer_kept("accepted", INSTANCE),
            "a failed observation dropped the offer record: {row:?}"
        );
        ensure!(
            row.last_error
                .as_deref()
                .is_some_and(|reason| reason.contains("injected chain observation failure")),
            "the settlement did not record why: {row:?}"
        );
        ensure!(fixture.submissions_of(&hash).await == 1, "offered twice");
        fixture.node.lock().await.chain_info_failure_after_offer = None;

        // The tip moves between the two reads of one observation.
        fixture.node.lock().await.shift_tip_after_offer = true;
        fixture.drain(shifted).await??;
        let shifted_hash = shifted.candidate.block_hash.clone();
        let row = fixture.row(&shifted_hash).await?;
        ensure!(
            row.state == "reconciliation" && !row.claimed && row.attempts == 1,
            "a shifting tip did not leave a reconciliation row: {row:?}"
        );
        ensure!(
            row.offer_kept("accepted", INSTANCE),
            "a shifting tip dropped the offer record: {row:?}"
        );
        ensure!(
            row.last_error
                .as_deref()
                .is_some_and(|reason| reason.contains("tip changed while observing candidate")),
            "the incoherent observation was not named: {row:?}"
        );
        ensure!(
            fixture.submissions_of(&shifted_hash).await == 1,
            "offered twice"
        );

        fixture.assert_untouched(&[neighbour]).await?;

        // Recovery: the chain is observable and now holds the block.
        fixture.node.lock().await.chain.push(hash.clone());
        let recovered = fixture.reclaim(&hash).await?;
        ensure!(recovered.lifecycle.state == CandidateState::Reconciliation);
        fixture.drain(&recovered).await??;
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "submitted" && row.attempts == 2 && row.completed,
            "the recovered row did not confirm: {row:?}"
        );
        ensure!(
            fixture.submissions_of(&hash).await == 1,
            "the recovery offered the block a second time"
        );
        ensure!(fixture.landed(&hash).await?);
        Ok(())
    }
    .await;
    fixture.close(result).await
}

// ---------------------------------------------------------------------------
// Dependency 2: the landing transaction
// ---------------------------------------------------------------------------

/// The landing transaction's acknowledgement is lost after its marked write
/// (the `qbit_pool_blocks` insert) and before any `COMMIT`, so the server
/// aborts it. Nothing landed: no audit bundle, no pool block. The row keeps
/// its offer record, reaches reconciliation with the reason, and is claimable;
/// the next legal attempt lands the audit durably without a second
/// `submitblock`. The marker names one block hash, so the two neighbours
/// cannot meet the fault, and one of them is drained afterwards to show the
/// landing path itself is undamaged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_landing_acknowledgement_rolls_back_and_the_next_attempt_lands() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claims = fixture.population().await?;
        let (target, healthy, neighbour) = (&claims[0], &claims[1], &claims[2]);
        let hash = target.candidate.block_hash.clone();
        fixture
            .mark(
                "mark_landing",
                POOL_BLOCKS,
                "INSERT",
                &format!("NEW.block_hash = '{hash}'"),
            )
            .await?;
        fixture.proxy.plan(Fault {
            table: POOL_BLOCKS.into(),
            op: "INSERT".into(),
            phase: FaultPhase::AfterExecution,
        });

        let mark = fixture.proxy.mark();
        fixture.drain(target).await??;
        let fired = fixture
            .proxy
            .fired()
            .context("the planned landing fault never fired")?;
        let executions = fixture.proxy.executions_since(mark)?;
        let writes = target_executions(&executions, POOL_BLOCKS, "INSERT");
        ensure!(
            target_statement_count(&executions, POOL_BLOCKS, "INSERT") == 1,
            "the landing wrote the pool block more than once: {writes:#?}"
        );
        ensure!(
            writes[0].seq == fired
                && matches!(
                    writes[0].outcome,
                    Outcome::Completed {
                        delivered: false,
                        ..
                    }
                ),
            "the server did not complete the marked write unheard: {:?}",
            writes[0].outcome
        );

        // Nothing the aborted transaction wrote survived it.
        ensure!(
            !fixture.landed(&hash).await? && fixture.pool_block(&hash).await?.is_none(),
            "an aborted landing left durable rows behind"
        );
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "reconciliation" && !row.claimed && row.attempts == 1 && !row.completed,
            "a lost landing acknowledgement did not leave a claimable row: {row:?}"
        );
        ensure!(
            row.offer_kept("accepted", INSTANCE),
            "a lost landing acknowledgement dropped the offer record: {row:?}"
        );
        ensure!(
            row.last_error
                .as_deref()
                .is_some_and(|reason| reason.contains("landing failed after the offer")),
            "the settlement did not record the landing failure: {row:?}"
        );
        ensure!(fixture.submissions_of(&hash).await == 1);
        fixture.assert_untouched(&[healthy, neighbour]).await?;

        // The next legal attempt lands, with no second call.
        let recovered = fixture.reclaim(&hash).await?;

        // EP-STATE: the severed attempt's claim is now stale. Work that
        // arrives late under it must not touch the replacement owner's row,
        // whichever settlement it tries; the fences are on the token, not on
        // the block hash.
        for (what, outcome) in [
            (
                "retry",
                fixture
                    .coordinator
                    .ledger
                    .retry_candidate(target, "delayed work under a stale token")
                    .await,
            ),
            (
                "reconciliation",
                fixture
                    .coordinator
                    .ledger
                    .reconcile_candidate(target, "delayed work under a stale token")
                    .await,
            ),
        ] {
            ensure!(
                outcome.is_err(),
                "a stale token wrote a {what} over the replacement owner's claim"
            );
        }
        let stale = fixture.row(&hash).await?;
        ensure!(
            stale.claimed
                && stale.state == "reconciliation"
                && stale.attempts == 2
                && stale.last_error.as_deref() != Some("delayed work under a stale token"),
            "delayed work changed the replacement owner's row: {stale:?}"
        );
        let held: Option<String> = sqlx::query_scalar(
            "SELECT claim_token FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&hash)
        .fetch_one(&fixture.direct)
        .await?;
        ensure!(
            held.as_deref() == Some(recovered.claim_token.as_str()),
            "the replacement owner lost its claim to delayed work"
        );

        fixture.drain(&recovered).await??;
        ensure!(
            fixture.landed(&hash).await?
                && fixture.pool_block(&hash).await?.as_deref() == Some("prepared"),
            "the recovery did not land the audit it rolled back"
        );
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "reconciliation"
                && row.attempts == 2
                && row.offer_kept("accepted", INSTANCE),
            "the recovered row lost its evidence: {row:?}"
        );
        ensure!(
            fixture.submissions_of(&hash).await == 1,
            "the recovery offered the block a second time"
        );

        // A neighbour drains through the same landing path, unharmed.
        let healthy_hash = healthy.candidate.block_hash.clone();
        fixture.drain(healthy).await??;
        ensure!(
            fixture.landed(&healthy_hash).await?
                && fixture.submissions_of(&healthy_hash).await == 1,
            "the neighbour's landing was damaged by another row's fault"
        );
        fixture.assert_untouched(&[neighbour]).await?;
        Ok(())
    }
    .await;
    fixture.close(result).await
}

// ---------------------------------------------------------------------------
// Dependency 3: the terminal update
// ---------------------------------------------------------------------------

/// The terminal update's `COMMIT` acknowledgement is lost after the commit is
/// durable. The disposition is a fact: the row is `submitted`, completed and
/// released, and it stays that way. The caller is told the outcome is unknown
/// rather than being told it succeeded, and nothing resurrects the row —
/// `retry_candidate` refuses it and no claim lane offers it again, so a second
/// `submitblock` is not reachable. The marker fires only on a *terminal*
/// state change of this one hash, which is what separates it from the renewal
/// update of dependency 4.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_terminal_commit_acknowledgement_stays_terminal() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claims = fixture.population().await?;
        let (target, neighbour_a, neighbour_b) = (&claims[0], &claims[1], &claims[2]);
        let hash = target.candidate.block_hash.clone();
        // The block must be active for the drain to reach terminalization.
        fixture.node.lock().await.extend_on_submit = true;
        fixture
            .mark(
                "mark_terminal",
                OUTBOX,
                "UPDATE",
                &format!(
                    "NEW.block_hash = '{hash}' AND NEW.state IN ('submitted','abandoned','orphaned') \
                     AND OLD.state IS DISTINCT FROM NEW.state"
                ),
            )
            .await?;
        fixture.proxy.plan(Fault {
            table: OUTBOX.into(),
            op: "UPDATE".into(),
            phase: FaultPhase::AfterCommit,
        });

        let mark = fixture.proxy.mark();
        let error = fixture
            .drain(target)
            .await?
            .expect_err("a lost COMMIT acknowledgement was reported as success");
        fixture
            .proxy
            .fired()
            .context("the planned terminal fault never fired")?;
        let executions = fixture.proxy.executions_since(mark)?;
        let writes = target_executions(&executions, OUTBOX, "UPDATE");
        ensure!(
            target_statement_count(&executions, OUTBOX, "UPDATE") == 1,
            "more than one terminal update was marked: {writes:#?}"
        );
        ensure!(
            writes[0].delivered(),
            "the terminal write's own completion was withheld, not the COMMIT's"
        );
        // A whole drain commits several transactions; the one this fault
        // severed is the first COMMIT after the marked terminal write.
        let commit = executions
            .iter()
            .find(|execution| execution.is_commit() && execution.seq > writes[0].seq)
            .context("the terminal COMMIT was never observed")?;
        ensure!(
            commit.completion() == Some("COMMIT") && !commit.delivered(),
            "the terminal COMMIT acknowledgement reached the client: {:?}",
            commit.outcome
        );
        // The drain does go on to attempt a reconciliation it cannot write,
        // but never over the socket the fault closed.
        ensure!(
            fixture
                .proxy
                .executions_since(commit.seq)?
                .iter()
                .all(|execution| execution.connection != commit.connection),
            "the severed connection carried more work after the withheld COMMIT"
        );

        // Durable and terminal, whatever the caller heard.
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "submitted" && row.completed && !row.claimed && !row.payload,
            "the committed terminalization did not survive its lost acknowledgement: {row:?}"
        );
        ensure!(
            row.attempts == 1 && row.offer_kept("accepted", INSTANCE),
            "the terminal row lost its offer record: {row:?}"
        );
        ensure!(
            fixture.submissions_of(&hash).await == 1,
            "the block was offered more than once"
        );
        ensure!(
            format!("{error:#}").contains("could not be settled in reconciliation either"),
            "the caller was not told the outcome is unknown: {error:#}"
        );

        // Nothing resurrects it: not the retry path, not a claim lane.
        let retry = fixture
            .coordinator
            .ledger
            .retry_candidate(target, "a retry after a terminal commit")
            .await;
        ensure!(
            retry.is_err(),
            "the retry path reopened a terminally committed row"
        );
        fixture.expire(&hash).await?;
        let claimed = fixture.coordinator.ledger.claim_candidate(120).await?;
        ensure!(
            claimed
                .as_ref()
                .is_none_or(|claim| claim.candidate.block_hash != hash),
            "a claim lane took a terminally committed row"
        );
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "submitted" && row.attempts == 1,
            "the terminal row was changed after the fact: {row:?}"
        );
        ensure!(fixture.submissions_of(&hash).await == 1);
        // The claim lane above may legitimately have taken a neighbour; both
        // are read against the state that claim left them in.
        let taken = claimed.map(|claim| claim.candidate.block_hash);
        for other in [neighbour_a, neighbour_b] {
            let other_hash = &other.candidate.block_hash;
            let row = fixture.row(other_hash).await?;
            ensure!(
                row.state == "pending"
                    && row.payload
                    && !row.completed
                    && row.outcome.is_none()
                    && row.attempts == if taken.as_deref() == Some(other_hash) { 2 } else { 1 },
                "a neighbour row was changed by the terminal fault: {row:?}"
            );
            ensure!(
                fixture.submissions_of(other_hash).await == 0,
                "a neighbour block was offered"
            );
        }
        Ok(())
    }
    .await;
    fixture.close(result).await
}

// ---------------------------------------------------------------------------
// Dependency 4: the lease renewal
// ---------------------------------------------------------------------------

/// The lease renewal's acknowledgement is lost after its `UPDATE` and before
/// the `COMMIT`, so the renewal aborts. The attempt is cancelled where it
/// stands — before the reservation and before any call — and the row is left
/// exactly as the claim found it: `pending`, one attempt, no offer record,
/// nothing at the node. The retry path releases it with the reason, and the
/// next legal attempt offers the block for the first and only time.
///
/// The marker separates this `UPDATE` from the terminal one of dependency 3
/// by what each changes: a renewal moves `claim_expires_at` and leaves `state`
/// alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_renewal_acknowledgement_cancels_only_that_attempt() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claims = fixture.population().await?;
        let (target, neighbour_a, neighbour_b) = (&claims[0], &claims[1], &claims[2]);
        let hash = target.candidate.block_hash.clone();
        fixture
            .mark(
                "mark_renewal",
                OUTBOX,
                "UPDATE",
                &format!(
                    "NEW.block_hash = '{hash}' AND NEW.state = OLD.state \
                     AND NEW.claim_expires_at IS DISTINCT FROM OLD.claim_expires_at"
                ),
            )
            .await?;
        fixture.proxy.plan(Fault {
            table: OUTBOX.into(),
            op: "UPDATE".into(),
            phase: FaultPhase::AfterExecution,
        });

        let mark = fixture.proxy.mark();
        let error = fixture
            .drain(target)
            .await?
            .expect_err("a lost renewal acknowledgement was reported as success");
        let fired = fixture
            .proxy
            .fired()
            .context("the planned renewal fault never fired")?;
        let executions = fixture.proxy.executions_since(mark)?;
        let writes = target_executions(&executions, OUTBOX, "UPDATE");
        ensure!(
            target_statement_count(&executions, OUTBOX, "UPDATE") == 1,
            "more than one renewal update was marked: {writes:#?}"
        );
        ensure!(
            writes[0].seq == fired
                && matches!(
                    writes[0].outcome,
                    Outcome::Completed {
                        delivered: false,
                        ..
                    }
                ),
            "the renewal was not completed unheard: {:?}",
            writes[0].outcome
        );
        ensure!(
            !executions.iter().any(Execution::is_commit),
            "the renewal transaction reached COMMIT"
        );
        ensure!(
            format!("{error:#}").contains("lease renewal"),
            "the cancellation was not attributed to the renewal: {error:#}"
        );

        // The attempt stopped before the reservation and before the node.
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "pending" && row.attempts == 1 && row.payload && !row.completed,
            "a lost renewal changed the row's lifecycle: {row:?}"
        );
        ensure!(
            row.outcome.is_none() && row.offered_at_ms.is_none() && row.reserved_by.is_none(),
            "a cancelled attempt left an offer record: {row:?}"
        );
        ensure!(
            fixture.submissions_of(&hash).await == 0,
            "an attempt whose renewal was lost still reached the node"
        );
        fixture
            .assert_untouched(&[neighbour_a, neighbour_b])
            .await?;

        // The submit loop's recovery, then the next legal attempt.
        fixture
            .coordinator
            .ledger
            .retry_candidate(target, &error.to_string())
            .await?;
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "pending" && !row.claimed,
            "the retry path did not release the row: {row:?}"
        );
        ensure!(
            row.last_error
                .as_deref()
                .is_some_and(|reason| reason.contains("lease renewal")),
            "the retry path did not record why: {row:?}"
        );

        let recovered = fixture.reclaim(&hash).await?;
        fixture.drain(&recovered).await??;
        let row = fixture.row(&hash).await?;
        ensure!(
            row.attempts == 2 && row.offer_kept("accepted", INSTANCE),
            "the recovery did not offer the block: {row:?}"
        );
        ensure!(
            fixture.submissions_of(&hash).await == 1,
            "the recovery did not offer exactly once"
        );
        Ok(())
    }
    .await;
    fixture.close(result).await
}

// ---------------------------------------------------------------------------
// One disposable database per fixture
// ---------------------------------------------------------------------------

/// A generated database the fixture owns outright, adapted from
/// `tests/support/ledger_database.rs`.
///
/// That file cannot simply be included here: two modules of one lib-test
/// target loading the same path is `clippy::duplicate_mod`, and this crate's
/// other storm module needs the same thing, so the pattern is adapted in each
/// rather than shared. What it exists for is kept exactly: PostgreSQL scopes
/// advisory locks to a *database*, and the ledger's `ORDER_LOCK` and
/// `SETTLEMENT_LOCK` are cluster-wide constants, so a unique schema would
/// leave these fixtures queueing on each other and on every other fixture in
/// this binary. The database is dropped however the test ends — `close`, an
/// early return, a panic or a cancellation — because `Drop` is the fallback
/// for a fixture that never reached `close`.
struct FixtureDatabase {
    /// One connection to the maintenance database the gate's URL names; it
    /// creates and drops the fixture database.
    admin: PgPool,
    /// The gate's URL. It may carry a password, so no message prints it.
    raw: String,
    name: String,
    /// The fixture database's URL, for pools under test.
    url: String,
    /// The database may exist and has not been dropped yet.
    armed: bool,
}

impl FixtureDatabase {
    async fn open(raw: &str, prefix: &str) -> Result<Self> {
        let name = format!("{prefix}{}", uuid::Uuid::new_v4().simple());
        // Generated from a literal prefix and hex, so it needs no quoting
        // beyond the double quotes below; checked rather than assumed.
        ensure!(
            name.len() <= 63
                && name.starts_with(|byte: char| byte.is_ascii_lowercase())
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
            "{name:?} is not a generated PostgreSQL identifier"
        );
        let mut url = url::Url::parse(raw)
            .map_err(|error| anyhow!("the test database URL is not a valid URL: {error}"))?;
        ensure!(
            matches!(url.scheme(), "postgres" | "postgresql") && !url.cannot_be_a_base(),
            "the test database URL must be a postgres:// or postgresql:// URL"
        );
        // sqlx lets dbname= override the path, which would put every fixture
        // back in the maintenance database.
        ensure!(
            !url.query_pairs().any(|(key, _)| key == "dbname"),
            "the test database URL must name its database in the path, not with dbname="
        );
        url.set_path(&format!("/{name}"));
        let admin = PgPool::connect(raw).await?;
        // Armed before CREATE DATABASE is sent: if its reply is lost the
        // database may exist anyway, and cleanup must still look for it.
        let fixture = Self {
            admin,
            raw: raw.to_owned(),
            name,
            url: url.into(),
            armed: true,
        };
        match sqlx::raw_sql(&format!("CREATE DATABASE \"{}\"", fixture.name))
            .execute(&fixture.admin)
            .await
        {
            Ok(_) => Ok(fixture),
            Err(error) => {
                let error = anyhow::Error::from(error).context(format!(
                    "creating fixture database {}; the test role needs CREATEDB, and template1 must have no other session",
                    fixture.name
                ));
                Err(fixture.abandon(error).await)
            }
        }
    }

    /// Drop the database and report the test's outcome. A test's own failure
    /// wins over a cleanup failure, which is attached to it as context.
    async fn close(self, result: Result<()>) -> Result<()> {
        match result {
            Ok(()) => {
                let mut fixture = self;
                fixture.drop_database().await
            }
            Err(error) => Err(self.abandon(error).await),
        }
    }

    /// Drop the database after `error` and return `error`.
    async fn abandon(mut self, error: anyhow::Error) -> anyhow::Error {
        match self.drop_database().await {
            Ok(()) => error,
            Err(cleanup) => error.context(format!(
                "removal of fixture database {} could not be confirmed: {cleanup:#}",
                self.name
            )),
        }
    }

    async fn drop_database(&mut self) -> Result<()> {
        // FORCE ends sessions a test left behind in this database; it touches
        // no other. The statement is idempotent and names only this database,
        // so a lost reply is safe to repeat from a fresh session.
        let statement = drop_statement(&self.name);
        let outcome = match sqlx::raw_sql(&statement).execute(&self.admin).await {
            Ok(_) => Ok(()),
            Err(error) => drop_from_fresh_session(&self.raw, &self.name)
                .await
                .map_err(|retry| {
                    anyhow::Error::from(error).context(format!(
                        "the retry from a fresh session also failed: {retry:#}"
                    ))
                }),
        };
        // Disarm only once it is gone, so a failed cleanup keeps its fallback.
        if outcome.is_ok() {
            self.armed = false;
        }
        self.admin.close().await;
        outcome
    }
}

fn drop_statement(name: &str) -> String {
    format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)")
}

async fn drop_from_fresh_session(raw: &str, name: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut connection = sqlx::PgConnection::connect(raw).await?;
        let dropped = sqlx::raw_sql(&drop_statement(name))
            .execute(&mut connection)
            .await;
        let closed = <sqlx::PgConnection as sqlx::Connection>::close(connection).await;
        dropped?;
        closed?;
        anyhow::Ok(())
    })
    .await
    .context("fixture cleanup did not finish in time; the server may still complete it")?
}

impl Drop for FixtureDatabase {
    /// The fallback for a fixture that never reached `close`. It must not rely
    /// on the test's runtime, which may be shutting down, so it runs on its own
    /// thread and runtime, and it never panics.
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (raw, name) = (self.raw.clone(), self.name.clone());
        let fallback = move || -> Result<()> {
            std::thread::Builder::new()
                .name("storm-fixture-database-drop".into())
                .spawn(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?
                        .block_on(drop_from_fresh_session(&raw, &name))
                })?
                .join()
                .map_err(|_| anyhow!("the drop thread panicked"))?
        };
        // A multi-thread runtime hands this worker's other tasks elsewhere
        // while it waits; a current-thread runtime has nothing else to run.
        let outcome = match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(fallback)
            }
            _ => fallback(),
        };
        if let Err(error) = outcome {
            let _ = std::io::Write::write_fmt(
                &mut std::io::stderr(),
                format_args!(
                    "storm fixture: removal of database {} could not be confirmed: {error:#}\n",
                    self.name
                ),
            );
        }
    }
}
