//! What counts as evidence of a node offer, asserted distinctly (#270).
//!
//! #266's migration 011 made this writable for the first time. The unfinished
//! states are `pending`, `offer_reserved`, `offered` and `reconciliation`;
//! `OfferReserved` is the durable reservation taken *before* the one
//! `submitblock` call, so a claim that finds a row there never offers. Only
//! `offered` — with `offered_at_ms`, `offer_outcome` and `offer_reply` — is
//! evidence that the node was called. A retry, a held lease, a
//! capacity-closed terminalization and a replay adoption are each not
//! evidence on their own, and 2.x.x asserted all four separately because its
//! selector's safety comparison was published against exactly that split.
//!
//! Every case here reaches its state through the real offer path. A test that
//! sets `state='offered'` by hand asserts its own setup and proves nothing,
//! so the offer record is produced by the coordinator, observed at the fake
//! node, and only then read back. [`offer_path_records_a_counted_call_with_a_durable_timestamp_and_reply`]
//! is the positive control the four negatives are read against: one counted
//! `submitblock` for the hash, a durable call time, outcome and reply.
//!
//! #270's four kinds map onto the native lifecycle exactly once each, except
//! the "capacity-closed" terminalization: 2.x.x closed build capacity in the
//! selector, and no native condition does that. The native terminal refusal
//! reachable before any call is the pre-offer supersession, so that is what
//! [`a_pre_offer_supersession_terminalizes_without_any_call`] asserts and what
//! its name says. An explicit capacity-closed terminalization remains
//! unexercised because no reachable native condition produces one.
//!
//! #424 settled what a proven orphan becomes, so the note this module carried
//! while that was open is gone. Two orphan terminalizations are distinguished
//! here, both as provenance questions rather than as threshold questions:
//! a row whose call is known keeps its call evidence through terminalization,
//! and a row interrupted at [`Coordinator::offer_probe`] — after the
//! reservation committed and before any call — terminalizes with the
//! reservation kept, the outcome `unknown`, and no invented call time or
//! reply. #424's own threshold, coherent-tip and revision-fence cases are in
//! `tests/lost_race_pin.rs` and are not repeated.
//!
//! Each test owns one disposable database, from the [`FixtureDatabase`] below:
//! PostgreSQL scopes advisory locks to a database, and the ledger's settlement
//! and order locks are cluster-wide constants, so a unique schema would queue
//! these fixtures on each other and on every other fixture in this binary.
//! That helper is adapted from `tests/support/ledger_database.rs` rather than
//! included, because two modules of one lib-test target loading the same path
//! is `clippy::duplicate_mod`, and `ledger::candidates::storm_fault_tests`
//! needs the same thing.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=... PRISM_TEST_REQUIRE_INTEGRATION=1 \
//!   cargo test -p qbit-prism-server --lib coordinator::storm_evidence_tests -- --test-threads=1
//! ```

use super::*;
use anyhow::anyhow;
use axum::{extract::State, routing::post, Json, Router};
use qbit_prism_test_gate as gate;
use sqlx::{Connection, PgPool};
use std::collections::BTreeMap;
use tokio::task::JoinHandle;

/// The height the fixture's candidates are found at. The node's chain starts
/// one below it, so a fresh candidate extends the tip it names as its parent.
const FOUND_HEIGHT: u64 = 101;
/// The fixture's `candidate_orphan_confirmations`. A competitor proven at
/// `FOUND_HEIGHT` needs the tip this far above it before the orphan verdict.
const ORPHAN_CONFIRMATIONS: u64 = 6;
/// The network difficulty the fixture's window and blocks are built at.
const NETWORK_DIFFICULTY: u128 = 100;
/// Every wait in this module. A fixture that exceeds it has stopped making
/// progress, and the test says which wait it was.
const BUDGET: Duration = Duration::from_secs(20);

// ---------------------------------------------------------------------------
// The fake node
// ---------------------------------------------------------------------------

/// A block hash that depends on its parent and its height (EP-VALIDATION), so
/// no two fixture chains and no two heights share one, and a hash can never be
/// confused with the proof-of-work hash of a real candidate block.
fn fake_hash(parent: &str, height: u64) -> String {
    hex::encode(Sha256::digest(
        format!("prism-storm:{parent}:{height}").as_bytes(),
    ))
}

struct NodeState {
    /// The active chain, indexed by height; `chain[0]` is the genesis hash.
    chain: Vec<String>,
    /// `submitblock` calls, per block hash. Never aggregated: a per-hash count
    /// is what "no second offer for this block" is read from.
    submissions: BTreeMap<String, usize>,
    /// When set, the next `submitblock` extends the chain with a competitor at
    /// `FOUND_HEIGHT` and fillers up to `ORPHAN_CONFIRMATIONS`, instead of the
    /// offered block: the offered block is accepted on a chain that loses. The
    /// transition happens inside the handler, so the observation that follows
    /// the call never races it.
    competitor_on_submit: bool,
    /// Block hashes the node rejects, with the reason it answers. A rejection
    /// is a known outcome, unlike a lost call, and it is durable evidence.
    reject_reasons: BTreeMap<String, String>,
    /// While set, every `getblockchaininfo` fails with this reason: the chain
    /// observation a drain depends on is unavailable.
    chain_info_failure: Option<String>,
    /// Hold the reply to the `n`th `getblockchaininfo` until released, so a
    /// test can inspect the row while an attempt is inside its pre-offer probe.
    hold_chain_info: Option<usize>,
    chain_info_calls: usize,
    gate: Arc<ReplyGate>,
}

#[derive(Default)]
struct ReplyGate {
    entered: Notify,
    release: Notify,
}

impl NodeState {
    fn tip(&self) -> &str {
        self.chain.last().expect("the chain has a genesis block")
    }

    fn tip_height(&self) -> u64 {
        self.chain.len() as u64 - 1
    }

    /// Cumulative work, which must strictly increase with the chain: the
    /// ledger records it against the tip view a settlement is fenced on.
    fn chainwork(&self) -> String {
        format!("{:064x}", self.chain.len())
    }

    /// Extend the chain to `height` with parent-dependent fake hashes.
    fn extend_to(&mut self, height: u64) {
        while self.tip_height() < height {
            let next = self.tip_height() + 1;
            let hash = fake_hash(self.tip(), next);
            self.chain.push(hash);
        }
    }

    fn submissions_of(&self, block_hash: &str) -> usize {
        self.submissions.get(block_hash).copied().unwrap_or(0)
    }
}

fn rpc_error(message: &str) -> Json<Value> {
    Json(json!({"id":Value::Null,"result":Value::Null,"error":{"code":-1,"message":message}}))
}

async fn node_reply(
    State(node): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut node = node.lock().await;
    let mut gate = None;
    let result = match request["method"].as_str().unwrap() {
        "getblockhash" => {
            let height = request["params"][0].as_u64().unwrap();
            // Above the tip the node has no block: a fixture that answered
            // anyway would let a test prove a height the chain never reached.
            match node.chain.get(usize::try_from(height).unwrap()) {
                Some(hash) => json!(hash),
                None => return rpc_error("Block height out of range"),
            }
        }
        "getblockchaininfo" => {
            node.chain_info_calls += 1;
            if let Some(reason) = node.chain_info_failure.clone() {
                return rpc_error(&reason);
            }
            if node.hold_chain_info == Some(node.chain_info_calls) {
                gate = Some(node.gate.clone());
            }
            json!({"chain":"test","initialblockdownload":false,"blocks":node.tip_height(),
                "headers":node.tip_height(),"bestblockhash":node.tip(),"chainwork":node.chainwork()})
        }
        "getbestblockhash" => json!(node.tip()),
        "getnetworkinfo" => json!({"connections":2}),
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap()).unwrap();
            let hash = codec::hash_display(&codec::double_sha256(&block[..80]));
            *node.submissions.entry(hash.clone()).or_default() += 1;
            if let Some(reason) = node.reject_reasons.get(&hash) {
                // A reason string is the node's rejection: a known outcome.
                json!(reason)
            } else if node.competitor_on_submit {
                // The offered block is accepted, on a chain that loses: a
                // different block takes its height and the tip runs away from
                // it to the configured depth.
                node.extend_to(FOUND_HEIGHT + ORPHAN_CONFIRMATIONS - 1);
                Value::Null
            } else {
                node.chain.push(hash);
                Value::Null
            }
        }
        method => panic!("unexpected candidate RPC {method}"),
    };
    drop(node);
    if let Some(gate) = gate {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

// ---------------------------------------------------------------------------
// The outbox row, as evidence
// ---------------------------------------------------------------------------

/// Every column this module reads a verdict from. `offered_at_ms`,
/// `offer_outcome` and `offer_reply` together are the call evidence;
/// `offer_reserved_by` alone is consumed permission.
#[derive(Debug, PartialEq)]
struct Row {
    state: String,
    claimed: bool,
    attempts: i32,
    last_error: Option<String>,
    reserved_by: Option<String>,
    offered_at_ms: Option<i64>,
    outcome: Option<String>,
    reply: Option<String>,
    /// The document, block bytes and window reference are all still present.
    payload: bool,
}

impl Row {
    /// No column records a call: not the time, not the outcome, not the reply.
    fn has_no_call_evidence(&self) -> bool {
        self.offered_at_ms.is_none() && self.outcome.is_none() && self.reply.is_none()
    }
}

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

struct Fixture {
    /// Taken by [`Self::close`]; dropping it drops the database if a test
    /// never gets there.
    database: Option<FixtureDatabase>,
    coordinator: Arc<Coordinator>,
    node: Arc<Mutex<NodeState>>,
    server: JoinHandle<()>,
    snapshot: Snapshot,
    /// The chain tip the fixture's candidates are built on.
    parent: String,
    /// The fixture database and node URLs, so a case can start a second,
    /// independent frontend on them.
    url: String,
    rpc_url: String,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let database = FixtureDatabase::open(&raw, "prism_storm_evidence_").await?;
        let url = database.url.clone();
        match Self::build(database, &url).await {
            Ok(fixture) => Ok(Some(fixture)),
            Err(error) => Err(error),
        }
    }

    async fn build(database: FixtureDatabase, url: &str) -> Result<Self> {
        let mut chain = vec!["00".repeat(32)];
        for height in 1..=FOUND_HEIGHT - 1 {
            let hash = fake_hash(chain.last().unwrap(), height);
            chain.push(hash);
        }
        let parent = chain.last().unwrap().clone();
        let node = Arc::new(Mutex::new(NodeState {
            chain,
            submissions: BTreeMap::new(),
            competitor_on_submit: false,
            reject_reasons: BTreeMap::new(),
            chain_info_failure: None,
            hold_chain_info: None,
            chain_info_calls: 0,
            gate: Arc::new(ReplyGate::default()),
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let rpc_url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(node_reply))
            .with_state(node.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let built = async {
            let coordinator = Coordinator::new(
                fixture_config(url, &rpc_url, "storm-evidence"),
                Arc::new(crate::metrics::Metrics::default()),
            )
            .await?;
            coordinator
                .ledger
                .observe_chain_view(&parent, FOUND_HEIGHT - 1, &format!("{:064x}", FOUND_HEIGHT))
                .await?;
            *coordinator.observed_tip.write().await = TipState::baseline(parent.clone());
            coordinator.ledger.append(seed_share(), None).await?;
            let snapshot = coordinator.ledger.snapshot(NETWORK_DIFFICULTY).await?;
            anyhow::Ok((coordinator, snapshot))
        }
        .await;
        match built {
            Ok((coordinator, snapshot)) => Ok(Self {
                database: Some(database),
                coordinator,
                node,
                server,
                snapshot,
                parent,
                url: url.to_owned(),
                rpc_url,
            }),
            Err(error) => {
                server.abort();
                Err(database.abandon(error).await)
            }
        }
    }

    /// Enqueue a candidate with its proof time, as the share path enqueues it,
    /// and claim it. `nonce_start` distinguishes the rows of a population.
    async fn enqueue_and_claim(&self, nonce_start: u32) -> Result<CandidateClaim> {
        let candidate = found_on(&self.snapshot, &self.parent, nonce_start)?;
        let hash = candidate.block_hash.clone();
        self.coordinator
            .ledger
            .enqueue_candidate_observed(candidate, Some(unix_ms_now()?))
            .await?;
        // The claim must be this row: a loop that discarded an unwanted claim
        // would leave that row owned by a token no test holds.
        let claim = self
            .coordinator
            .ledger
            .claim_candidate(120)
            .await?
            .context("no candidate was claimable")?;
        ensure!(
            claim.candidate.block_hash == hash,
            "the claim took {} instead of the candidate just enqueued",
            claim.candidate.block_hash
        );
        Ok(claim)
    }

    async fn row(&self, block_hash: &str) -> Result<Row> {
        let row = sqlx::query_as::<_, (String, bool, i32, Option<String>, Option<String>, Option<i64>, Option<String>, Option<String>, bool)>(
            "SELECT state,claim_token IS NOT NULL,attempt_count,last_error,offer_reserved_by,offered_at_ms,offer_outcome,offer_reply,candidate IS NOT NULL AND block_bytes IS NOT NULL AND window_anchor_ms IS NOT NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&self.coordinator.ledger.pool)
        .await?;
        Ok(Row {
            state: row.0,
            claimed: row.1,
            attempts: row.2,
            last_error: row.3,
            reserved_by: row.4,
            offered_at_ms: row.5,
            outcome: row.6,
            reply: row.7,
            payload: row.8,
        })
    }

    async fn submissions_of(&self, block_hash: &str) -> usize {
        self.node.lock().await.submissions_of(block_hash)
    }

    /// A second, independent frontend on the same database and node: its own
    /// pool, its own metrics registry, its own instance id and an empty tip
    /// cache, which is what a process that has just started actually has.
    /// Used where a case must recover work across a restart rather than
    /// continue it in the process that enqueued it.
    async fn restarted_frontend(&self, instance_id: &str) -> Result<Arc<Coordinator>> {
        Coordinator::new(
            fixture_config(&self.url, &self.rpc_url, instance_id),
            Arc::new(crate::metrics::Metrics::default()),
        )
        .await
    }

    /// The block's own `qbit_pool_blocks` chain state, `None` when the landing
    /// never wrote one.
    async fn chain_state(&self, block_hash: &str) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
                .bind(block_hash)
                .fetch_optional(&self.coordinator.ledger.pool)
                .await?,
        )
    }

    async fn landed(&self, block_hash: &str) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_pool_audit_bundles WHERE block_hash=$1)",
        )
        .bind(block_hash)
        .fetch_one(&self.coordinator.ledger.pool)
        .await?)
    }

    /// End the claim's lease so the next legal attempt can take the row, as an
    /// expiry would. Deterministic where waiting out a real lease is not.
    async fn expire(&self, block_hash: &str) -> Result<()> {
        sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second',next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(block_hash)
            .execute(&self.coordinator.ledger.pool)
            .await?;
        Ok(())
    }

    async fn close(mut self, result: Result<()>) -> Result<()> {
        self.server.abort();
        self.coordinator.ledger.pool.close().await;
        let database = self.database.take().expect("the database is closed once");
        database.close(result).await
    }
}

/// The fixture's coordinator configuration: one build worker, a short RPC
/// budget, and the orphan depth the two terminalization cases are read at.
/// Every value comes from this literal; no test reads the environment.
fn fixture_config(database_url: &str, rpc_url: &str, instance_id: &str) -> Config {
    Config {
        database_url: database_url.into(),
        instance_id: instance_id.into(),
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
        candidate_orphan_confirmations: ORPHAN_CONFIRMATIONS,
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

/// A block found on the fixture's window with the fixture's keys, so a rebuild
/// reproduces exactly the audit it was found with, extending `parent` at
/// `FOUND_HEIGHT`. `nonce_start` distinguishes the candidates of a population.
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
        format!("storm-evidence-{nonce_start}"),
        &template,
        &bundle.signed_coinbase_manifest.manifest,
        "00000000",
        8,
        1e-12,
        0.0,
        true,
    )?;
    let proof = (nonce_start..nonce_start + 10_000u32)
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
// The positive control
// ---------------------------------------------------------------------------

/// The control the four negatives are read against: the real coordinator offer
/// path, one counted `submitblock` per hash, and a durable call record. An
/// acceptance records the call time and the outcome (the node answered `null`,
/// so there is no reply to record); a rejection records the call time, the
/// outcome and the node's own reason. Neither row is reached by writing
/// `state='offered'`: both are produced by `Coordinator::process_candidate`
/// and observed at the node before the row is read back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offer_path_records_a_counted_call_with_a_durable_timestamp_and_reply() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        // A: the node rejects with a reason, which is the durable reply. It
        // runs first: a rejected block never becomes the tip, so the accepted
        // case below still finds the parent it was built on.
        let rejected = fixture.enqueue_and_claim(0).await?;
        let hash = rejected.candidate.block_hash.clone();
        fixture
            .node
            .lock()
            .await
            .reject_reasons
            .insert(hash.clone(), "bad-txnmrklroot".into());
        tokio::time::timeout(BUDGET, fixture.coordinator.process_candidate(&rejected))
            .await
            .context("the rejected offer did not settle")??;
        let row = fixture.row(&hash).await?;
        ensure!(
            fixture.submissions_of(&hash).await == 1,
            "the rejected offer did not make exactly one call for {hash}"
        );
        ensure!(
            row.state == "reconciliation"
                && row.outcome.as_deref() == Some("rejected")
                && row.reply.as_deref() == Some("bad-txnmrklroot")
                && row.offered_at_ms.is_some(),
            "the node's rejection is not durable evidence of its call: {row:?}"
        );

        // B: the node accepts, and its block becomes the tip.
        let accepted = fixture.enqueue_and_claim(20_000).await?;
        let hash = accepted.candidate.block_hash.clone();
        let before = unix_ms_now()?;
        tokio::time::timeout(BUDGET, fixture.coordinator.process_candidate(&accepted))
            .await
            .context("the accepted offer did not settle")??;
        let after = unix_ms_now()?;
        let row = fixture.row(&hash).await?;
        ensure!(
            fixture.submissions_of(&hash).await == 1,
            "the offer path did not make exactly one call for {hash}"
        );
        ensure!(
            row.state == "submitted" && row.outcome.as_deref() == Some("accepted"),
            "the accepted offer did not settle as submitted: {row:?}"
        );
        let offered_at = row
            .offered_at_ms
            .context("the call time was not recorded")?;
        ensure!(
            (before..=after).contains(&offered_at),
            "the recorded call time {offered_at} is outside the call ({before}..={after})"
        );
        ensure!(
            row.reply.is_none() && row.reserved_by.as_deref() == Some("storm-evidence"),
            "an accepted call recorded a reply it never received: {row:?}"
        );
        ensure!(fixture.landed(&hash).await? && !row.payload);
        Ok(())
    }
    .await;
    fixture.close(result).await
}

// ---------------------------------------------------------------------------
// The four kinds that are not evidence
// ---------------------------------------------------------------------------

/// A pre-offer retry. The attempt fails in the chain observation the staleness
/// screen triggers, before the reservation, and is rescheduled by the retry
/// path the submit loop uses. The row keeps its payload, stays `pending`, and
/// records no call: `last_error` is written where `retry_candidate` writes it
/// and nowhere else, and no offer column is touched. The next legal attempt
/// then offers the block for the first time, so the hash is called exactly
/// once across both attempts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pre_offer_retry_records_no_call_and_leaves_the_row_pending() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.enqueue_and_claim(0).await?;
        let hash = claim.candidate.block_hash.clone();
        // A cached tip that is not the block's parent is what makes the
        // authoritative pre-offer probe run at all.
        *fixture.coordinator.observed_tip.write().await =
            TipState::baseline(fake_hash(&fixture.parent, 999));
        fixture.node.lock().await.chain_info_failure =
            Some("injected chain observation failure".into());
        let error = tokio::time::timeout(BUDGET, fixture.coordinator.process_candidate(&claim))
            .await
            .context("the failing pre-offer probe never returned")?
            .expect_err("an unobservable chain reported a settled candidate");
        ensure!(
            fixture.submissions_of(&hash).await == 0,
            "a candidate was offered before its chain observation succeeded"
        );
        // The submit loop's own recovery, run here as the loop runs it.
        fixture
            .coordinator
            .ledger
            .retry_candidate(&claim, &error.to_string())
            .await?;
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "pending" && !row.claimed && row.attempts == 1 && row.payload,
            "a pre-offer retry did not leave a claimable pending row: {row:?}"
        );
        ensure!(
            row.has_no_call_evidence() && row.reserved_by.is_none(),
            "a pre-offer retry recorded offer evidence: {row:?}"
        );
        ensure!(
            row.last_error
                .as_deref()
                .is_some_and(|reason| reason.contains("injected chain observation failure")),
            "the retry path did not record why: {row:?}"
        );

        // The next legal attempt: the first and only call for this hash.
        fixture.node.lock().await.chain_info_failure = None;
        *fixture.coordinator.observed_tip.write().await =
            TipState::baseline(fixture.parent.clone());
        fixture.expire(&hash).await?;
        let retried = fixture
            .coordinator
            .ledger
            .claim_candidate(120)
            .await?
            .context("the retried row was not claimable")?;
        tokio::time::timeout(BUDGET, fixture.coordinator.process_candidate(&retried))
            .await
            .context("the retried attempt did not settle")??;
        let row = fixture.row(&hash).await?;
        ensure!(
            fixture.submissions_of(&hash).await == 1 && row.state == "submitted",
            "the recovery did not offer exactly once: {row:?}"
        );
        ensure!(row.attempts == 2, "{row:?}");
        Ok(())
    }
    .await;
    fixture.close(result).await
}

/// A held and renewed lease. An attempt inside its pre-offer chain probe owns
/// the row and keeps renewing it, and that ownership is not evidence of a
/// call: while the lease is extended twice the row stays `pending` with every
/// offer column null and the node has seen nothing. Releasing the probe lets
/// the same attempt offer, and only then does the evidence appear.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_and_renewed_lease_before_any_call_is_not_offer_evidence() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.enqueue_and_claim(0).await?;
        let hash = claim.candidate.block_hash.clone();
        let gate = {
            let mut node = fixture.node.lock().await;
            // Hold the next chain read, whichever calls the fixture already made.
            node.hold_chain_info = Some(node.chain_info_calls + 1);
            node.gate.clone()
        };
        *fixture.coordinator.observed_tip.write().await =
            TipState::baseline(fake_hash(&fixture.parent, 999));
        let process = tokio::spawn({
            let coordinator = fixture.coordinator.clone();
            let claim = claim.clone();
            async move { coordinator.process_candidate(&claim).await }
        });
        tokio::time::timeout(BUDGET, gate.entered.notified())
            .await
            .context("the attempt never reached its pre-offer chain probe")?;

        let mut previous = lease_expiry(&fixture, &hash).await?;
        for renewal in 1..=2 {
            fixture
                .coordinator
                .ledger
                .renew_candidate_claim(&claim, 120)
                .await?;
            let expiry = lease_expiry(&fixture, &hash).await?;
            ensure!(
                expiry > previous,
                "renewal {renewal} did not extend the lease ({previous:?} -> {expiry:?})"
            );
            previous = expiry;
            let row = fixture.row(&hash).await?;
            ensure!(
                row.state == "pending" && row.claimed && row.payload,
                "renewal {renewal} moved the row out of pending: {row:?}"
            );
            ensure!(
                row.has_no_call_evidence() && row.reserved_by.is_none(),
                "a renewed lease was recorded as offer evidence: {row:?}"
            );
            ensure!(
                fixture.submissions_of(&hash).await == 0,
                "a renewed lease reached the node"
            );
        }

        gate.release.notify_one();
        tokio::time::timeout(BUDGET, process)
            .await
            .context("the released attempt did not settle")???;
        let row = fixture.row(&hash).await?;
        ensure!(
            fixture.submissions_of(&hash).await == 1
                && row.offered_at_ms.is_some()
                && row.outcome.as_deref() == Some("accepted"),
            "the released attempt did not produce the call evidence the lease never was: {row:?}"
        );
        Ok(())
    }
    .await;
    fixture.close(result).await
}

/// A terminal outcome reached with no call at all.
///
/// #270 words this case as the "capacity-closed" terminalization, which was
/// 2.x.x's selector refusing a candidate whose build capacity had closed.
/// There is no native condition of that shape: what this asserts is the one
/// terminal refusal a native row can reach *before* any call, the pre-offer
/// supersession — the probe proves the block's parent is no longer the tip, so
/// the window it was issued against is closed and
/// `finish_candidate_at_revision(submitted=false)` abandons it. That path is
/// reachable from `pending` only, which is why it is the only one. The name
/// says supersession rather than capacity so that it claims exactly what it
/// proves; see the module docs for the rest of the mapping.
///
/// The row reaches a terminal state and releases its payload with no call at
/// all: no reservation, no call time, no outcome, no reply, nothing at the node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pre_offer_supersession_terminalizes_without_any_call() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.enqueue_and_claim(0).await?;
        let hash = claim.candidate.block_hash.clone();
        // Another block took the candidate's height; the window it was issued
        // against is closed.
        let tip = {
            let mut node = fixture.node.lock().await;
            node.extend_to(FOUND_HEIGHT);
            node.tip().to_owned()
        };
        *fixture.coordinator.observed_tip.write().await = TipState::baseline(tip);
        tokio::time::timeout(BUDGET, fixture.coordinator.process_candidate(&claim))
            .await
            .context("the superseded candidate did not settle")??;
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "abandoned" && !row.claimed && !row.payload,
            "a superseded candidate did not terminalize: {row:?}"
        );
        ensure!(
            row.has_no_call_evidence() && row.reserved_by.is_none(),
            "a terminalization with no call recorded offer evidence: {row:?}"
        );
        ensure!(
            fixture.submissions_of(&hash).await == 0,
            "a superseded candidate was offered"
        );
        ensure!(
            !fixture.landed(&hash).await? && fixture.chain_state(&hash).await?.is_none(),
            "a candidate that was never offered landed an audit or a pool block"
        );
        Ok(())
    }
    .await;
    fixture.close(result).await
}

/// A replay adoption after a restart. The frontend that enqueued the block is
/// gone; a *different* frontend starts on the same database and node, with an
/// empty tip cache, and finds the chain already holding the block — an earlier
/// frontend offered it and lost the outcome, or reconciliation confirmed it.
/// The row is adopted into the no-resubmission lifecycle and reaches the
/// terminal `submitted` state without any call, which is exactly why
/// `submitted` is not evidence of one either. The outcome is `unknown` and the
/// call time stays null; the reply holds the node's *chain evidence*, which is
/// not a `submitblock` answer, and the reservation is attributed to the
/// frontend that actually made the observation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replay_adoption_after_restart_records_an_unknown_offer_with_no_call() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.enqueue_and_claim(0).await?;
        let hash = claim.candidate.block_hash.clone();
        // The chain already holds the block, and no call put it there.
        {
            let mut node = fixture.node.lock().await;
            node.chain.push(hash.clone());
        }
        // The enqueuing frontend is gone: its claim expires and a frontend
        // that has never seen this row starts up and takes the work.
        fixture.expire(&hash).await?;
        let restarted = fixture
            .restarted_frontend("storm-evidence-restarted")
            .await?;
        let recovered = restarted
            .ledger
            .claim_candidate(120)
            .await?
            .context("the restarted frontend found no work")?;
        ensure!(
            recovered.candidate.block_hash == hash
                && recovered.lifecycle.state == CandidateState::Pending,
            "the restarted frontend did not recover the pending row"
        );
        tokio::time::timeout(BUDGET, restarted.process_candidate(&recovered))
            .await
            .context("the adopted candidate did not settle")??;
        restarted.ledger.pool.close().await;

        let row = fixture.row(&hash).await?;
        ensure!(
            fixture.submissions_of(&hash).await == 0,
            "an adopted block that the chain already held was offered again"
        );
        ensure!(
            row.state == "submitted" && row.outcome.as_deref() == Some("unknown"),
            "the adoption did not keep an unknown outcome: {row:?}"
        );
        ensure!(
            row.offered_at_ms.is_none(),
            "the adoption invented a call time: {row:?}"
        );
        ensure!(
            row.reply.as_deref().is_some_and(
                |reply| reply.starts_with("node reports block") && reply.contains(&hash)
            ),
            "the adoption's reply is not the node's chain evidence: {row:?}"
        );
        // EP-OBSERVABILITY: the observation is attributed to the frontend that
        // actually made it, not to the one that enqueued the row.
        ensure!(
            row.reserved_by.as_deref() == Some("storm-evidence-restarted"),
            "the adoption was attributed to the wrong frontend: {row:?}"
        );
        ensure!(
            row.attempts == 2,
            "the restarted frontend's attempt was not counted: {row:?}"
        );
        ensure!(
            fixture.landed(&hash).await?
                && fixture.chain_state(&hash).await?.as_deref() == Some("confirmed"),
            "the adopted block did not land and confirm"
        );
        Ok(())
    }
    .await;
    fixture.close(result).await
}

// ---------------------------------------------------------------------------
// Two orphan terminalizations, told apart by provenance
// ---------------------------------------------------------------------------

/// A known call keeps its call evidence through terminalization. The node
/// accepted the block on a chain that then lost, and the competitor reaches
/// the configured depth. The row is settled `orphaned` and the whole offer
/// record — the reservation, the call time, the outcome — survives it, beside
/// the landed audit and the block's `inactive` pool row that a reorg back
/// would reactivate. Exactly one call was ever made for the hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_known_call_keeps_its_call_evidence_through_orphan_terminalization() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.enqueue_and_claim(0).await?;
        let hash = claim.candidate.block_hash.clone();
        fixture.node.lock().await.competitor_on_submit = true;
        tokio::time::timeout(BUDGET, fixture.coordinator.process_candidate(&claim))
            .await
            .context("the orphaned candidate did not settle")??;
        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "orphaned" && !row.claimed && !row.payload,
            "the known call did not terminalize as an orphan: {row:?}"
        );
        ensure!(
            row.outcome.as_deref() == Some("accepted")
                && row.offered_at_ms.is_some()
                && row.reserved_by.as_deref() == Some("storm-evidence"),
            "terminalization dropped the evidence of a call that is known to have happened: {row:?}"
        );
        // The whole record is preserved, the absent reply included: the node
        // answered `null`, so `offer_reply` was null before terminalization
        // and must still be null after it rather than gaining a reason.
        ensure!(
            row.reply.is_none(),
            "terminalization invented a reply for a call the node accepted: {row:?}"
        );
        ensure!(
            fixture.submissions_of(&hash).await == 1,
            "the orphan settlement offered the block again"
        );
        ensure!(
            fixture.landed(&hash).await?
                && fixture.chain_state(&hash).await?.as_deref() == Some("inactive"),
            "the orphaned block did not keep its landed audit and inactive pool block"
        );
        ensure!(
            row.last_error
                .as_deref()
                .is_some_and(|reason| reason.contains("proven orphan")),
            "the orphan reason is missing: {row:?}"
        );
        Ok(())
    }
    .await;
    fixture.close(result).await
}

/// An interruption at [`Coordinator::offer_probe`] — after the reservation
/// committed, before any `submitblock` — recovered under a competing chain.
/// The reservation is consumed permission, not proof of a call, so the
/// recovery never offers: it terminalizes the row `orphaned` with the
/// reservation kept, the outcome `unknown`, and no invented call time or
/// reply. The node is never called for this hash at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_reservation_orphans_unknown_without_inventing_a_call() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.enqueue_and_claim(0).await?;
        let hash = claim.candidate.block_hash.clone();
        let probe = Arc::new(OfferProbe::default());
        *fixture.coordinator.offer_probe.lock().unwrap() = Some(probe.clone());
        let process = tokio::spawn({
            let coordinator = fixture.coordinator.clone();
            let claim = claim.clone();
            async move { coordinator.process_candidate(&claim).await }
        });
        tokio::time::timeout(BUDGET, probe.entered.notified())
            .await
            .context("the reservation was never taken")?;
        let reserved = fixture.row(&hash).await?;
        ensure!(
            reserved.state == "offer_reserved"
                && reserved.reserved_by.as_deref() == Some("storm-evidence")
                && reserved.has_no_call_evidence(),
            "the reservation is not durable and call-free before the call: {reserved:?}"
        );
        // The interruption: the attempt dies holding the reservation, having
        // called nothing.
        process.abort();
        ensure!(
            fixture.submissions_of(&hash).await == 0,
            "the interrupted attempt reached the node"
        );
        // It recovers under a chain that has moved past the block's height.
        {
            let mut node = fixture.node.lock().await;
            node.extend_to(FOUND_HEIGHT + ORPHAN_CONFIRMATIONS - 1);
        }
        fixture.expire(&hash).await?;
        let recovered = fixture
            .coordinator
            .ledger
            .claim_candidate(120)
            .await?
            .context("the reserved row was not recoverable")?;
        ensure!(
            recovered.lifecycle.state == CandidateState::OfferReserved,
            "the recovery did not find the reservation: {:?}",
            recovered.lifecycle.state
        );
        tokio::time::timeout(BUDGET, fixture.coordinator.process_candidate(&recovered))
            .await
            .context("the recovered reservation did not settle")??;

        let row = fixture.row(&hash).await?;
        ensure!(
            row.state == "orphaned" && !row.claimed && !row.payload,
            "the recovered reservation did not terminalize as an orphan: {row:?}"
        );
        ensure!(
            row.outcome.as_deref() == Some("unknown"),
            "a reservation whose call never happened was given a known outcome: {row:?}"
        );
        ensure!(
            row.offered_at_ms.is_none() && row.reply.is_none(),
            "terminalization invented a call time or a node reply: {row:?}"
        );
        ensure!(
            row.reserved_by.as_deref() == Some("storm-evidence"),
            "terminalization dropped the reservation it was recovered from: {row:?}"
        );
        ensure!(
            fixture.submissions_of(&hash).await == 0,
            "the recovery offered a block whose call was only ever reserved"
        );
        ensure!(
            fixture.landed(&hash).await?
                && fixture.chain_state(&hash).await?.as_deref() == Some("inactive"),
            "the orphaned block did not keep its landed audit and inactive pool block"
        );
        Ok(())
    }
    .await;
    fixture.close(result).await
}

/// The row's lease expiry, as the database holds it.
async fn lease_expiry(fixture: &Fixture, block_hash: &str) -> Result<String> {
    Ok(sqlx::query_scalar(
        "SELECT claim_expires_at::text FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(block_hash)
    .fetch_one(&fixture.coordinator.ledger.pool)
    .await?)
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
