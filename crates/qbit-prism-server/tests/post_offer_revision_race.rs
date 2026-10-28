//! #630: a payout-revision race after the offer is retried in place.
//!
//! Every post-offer write (the landing, the finish as `submitted`, the
//! settlement as `orphaned`) is fenced on the payout revision its own
//! candidate observation returned. On the 400k two-frontend soak a sibling
//! frontend moved that revision between the observation and the write about
//! once in three landings, and the row went to reconciliation with an `ALERT`
//! although nothing had failed. These tests force that interleaving
//! deterministically and pin what happens now:
//!
//! - a race before the landing or after it is observed again and written
//!   again in the same attempt: the row finishes `submitted`, no `ALERT` is
//!   logged, the landing and the confirmation each take effect once, and the
//!   balances, payout entries and credits equal the no-race case's;
//! - a race that outlasts the phase's retry budget, and any error that is not
//!   a revision race, still settle the row in reconciliation with today's
//!   `ALERT`, and the next attempt finishes it with the same accounting.
//!
//! The interleaving is forced at the node, not by timing: the coordinator is
//! pointed at a front for the scripted node (`support/scripted_node.rs`)
//! that runs a scripted step on each `getblockhash` for the candidate's
//! height. Every candidate observation makes exactly that call after it has
//! read the payout revision and before its caller writes, and the caller
//! waits for the reply, so a step's revision move commits strictly between
//! the observation and the write. Nothing here generates load or sleeps.
//!
//! Run through test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test post_offer_revision_race -- --nocapture.
use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, FoundBlock, PayoutPolicy};
use qbit_prism_server::{
    codec,
    config::Config,
    coordinator::{Coordinator, TipState},
    ledger::{
        BlockObservation, Candidate, CandidateClaim, Ledger, SignerKeys, Snapshot, WindowRef,
    },
    metrics::Metrics,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    time::Duration,
};
use tokio::{sync::Mutex, task::JoinHandle};
use tracing::instrument::WithSubscriber;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
#[path = "support/scripted_node.rs"]
#[allow(dead_code)]
mod scripted_node;
use ledger_database::FixtureDatabase;
use scripted_node::{ChainState, ScriptedNode};

/// The height the candidate is found at, on `PARENT` one below it.
const HEIGHT: u64 = 101;
const PARENT: &str = "aa";
/// The same-height block that wins a lost tip race.
const COMPETITOR: &str = "bb";
/// The fixture's `candidate_orphan_confirmations`.
const ORPHAN_CONFIRMATIONS: u64 = 6;
/// Bounds every attempt's processing.
const PROCESS_BOUND: Duration = Duration::from_secs(60);
/// The coordinator's retry line for a lost race (`coordinator.rs`).
const RETRY_LINE: &str =
    "post-offer write lost a payout revision race; observing again and writing at the new revision";
const NOT_PROCESSED: &str =
    "ALERT: offered candidate could not be processed; kept for reconciliation, never offered again";
const NOT_LANDED: &str =
    "ALERT: offered candidate could not land; kept for reconciliation, never offered again";
/// The fence refusal's message (`ledger/connect.rs`), which operators search.
const RACE: &str = "payout revision changed while observing chain state";
/// The retries one post-offer phase allows (`REVISION_RACE_RETRIES`).
const RETRIES: usize = 3;
const INJECTED: &str = "injected node failure at the candidate's height";

// ---------------------------------------------------------------------------
// The node front
// ---------------------------------------------------------------------------

/// What the front does on a `getblockhash` for the candidate's height, in
/// order; once the script is spent, every call is forwarded.
#[derive(Clone)]
enum Step {
    /// Forward to the scripted node.
    Forward,
    /// Move the payout revision first, as any revision writer does: a tip
    /// record, a confirmation or maturity of another block, a policy
    /// transition.
    Bump,
    /// A sibling frontend's reconciler confirms the landed block first: it
    /// commits the confirmation and the deferred-share credit and moves the
    /// revision. This is the race #630 traced on the soak.
    SiblingConfirms(String),
    /// Answer with an RPC error: a node failure, not a revision race.
    Fail,
}

struct FrontState {
    upstream: String,
    client: reqwest::Client,
    pool: PgPool,
    /// A second frontend's ledger on the same database, set once the
    /// coordinator has created the schema.
    sibling: OnceLock<Ledger>,
    script: Mutex<VecDeque<Step>>,
    /// `getblockhash` calls for the candidate's height: one per candidate
    /// observation.
    observations: AtomicUsize,
    /// A step that could not be carried out; the test fails on any.
    failures: std::sync::Mutex<Vec<String>>,
}

impl FrontState {
    async fn take(&self, step: Step) -> Result<Option<Value>> {
        match step {
            Step::Forward => {}
            Step::Bump => {
                sqlx::query(
                    "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
                )
                .execute(&self.pool)
                .await?;
            }
            Step::SiblingConfirms(hash) => {
                let sibling = self
                    .sibling
                    .get()
                    .context("the sibling ledger is not open")?;
                let confirmed = sibling
                    .reconcile_blocks(
                        &[BlockObservation {
                            block_hash: hash,
                            active: true,
                        }],
                        HEIGHT,
                    )
                    .await?;
                ensure!(
                    confirmed == 1,
                    "the sibling did not confirm the block first"
                );
            }
            Step::Fail => return Ok(Some(json!({"code": -1, "message": INJECTED}))),
        }
        Ok(None)
    }
}

async fn relay(
    axum::extract::State(front): axum::extract::State<Arc<FrontState>>,
    axum::Json(request): axum::Json<Value>,
) -> axum::Json<Value> {
    let error = |error: Value| json!({"id": request["id"], "result": null, "error": error});
    if request["method"] == "getblockhash" && request["params"][0].as_u64() == Some(HEIGHT) {
        front.observations.fetch_add(1, Ordering::SeqCst);
        let step = front
            .script
            .lock()
            .await
            .pop_front()
            .unwrap_or(Step::Forward);
        match front.take(step).await {
            Ok(None) => {}
            Ok(Some(failure)) => return axum::Json(error(failure)),
            Err(failure) => {
                let failure = format!("{failure:#}");
                front.failures.lock().unwrap().push(failure.clone());
                return axum::Json(error(json!({"code": -1, "message": failure})));
            }
        }
    }
    let forwarded = async {
        front
            .client
            .post(&front.upstream)
            .json(&request)
            .send()
            .await?
            .json::<Value>()
            .await
    };
    match forwarded.await {
        Ok(reply) => axum::Json(reply),
        Err(failure) => {
            let failure = format!("front could not reach the scripted node: {failure}");
            front.failures.lock().unwrap().push(failure.clone());
            axum::Json(error(json!({"code": -1, "message": failure})))
        }
    }
}

struct Front {
    url: String,
    state: Arc<FrontState>,
    task: JoinHandle<()>,
}

impl Front {
    async fn open(upstream: &str, pool: PgPool) -> Result<Self> {
        let state = Arc::new(FrontState {
            upstream: upstream.to_owned(),
            client: reqwest::Client::builder().no_proxy().build()?,
            pool,
            sibling: OnceLock::new(),
            script: Mutex::default(),
            observations: AtomicUsize::new(0),
            failures: std::sync::Mutex::default(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let app = axum::Router::new()
            .route("/", axum::routing::post(relay))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { url, state, task })
    }
}

impl Drop for Front {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

struct Fixture {
    /// The fixture's own database: advisory locks are per database.
    database: FixtureDatabase,
    pool: PgPool,
    coordinator: Arc<Coordinator>,
    node: ScriptedNode,
    front: Front,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let database = FixtureDatabase::open(&raw, "prism_revision_race_").await?;
        match Self::build(&database).await {
            Ok((pool, coordinator, node, front)) => Ok(Some(Self {
                database,
                pool,
                coordinator,
                node,
                front,
            })),
            Err(error) => Err(database.abandon(error).await),
        }
    }

    async fn build(
        database: &FixtureDatabase,
    ) -> Result<(PgPool, Arc<Coordinator>, ScriptedNode, Front)> {
        let pool = PgPool::connect(&database.url).await?;
        let node = ScriptedNode::open(ChainState::new(&PARENT.repeat(32), HEIGHT - 1)).await?;
        let front = Front::open(&node.url, pool.clone()).await?;
        let config = Config {
            database_url: database.url.clone(),
            instance_id: "revision-race".into(),
            database_connections: 8,
            initialize_schema: true,
            chain: "testnet".into(),
            expected_genesis_hash: None,
            min_peers: 1,
            template_max_age: Duration::from_secs(120),
            submit_tip_max_age: Duration::from_secs(10),
            template_refresh_failure_exit: Duration::from_secs(120),
            rpc_url: front.url.clone(),
            rpc_user: "test".into(),
            rpc_password: "test".into(),
            // A front step (the sibling's confirmation) runs inside the
            // call it answers; leave it room on a loaded host.
            rpc_timeout: Duration::from_secs(20),
            block_submit_timeout: Duration::from_secs(5),
            block_submit_enabled: true,
            poll_interval: Duration::from_secs(1),
            blockwait: false,
            build_workers: 2,
            refresh_build_threads: None,
            runtime_workers: 2,
            snapshot_interval: Duration::from_secs(60),
            health_timeout: Duration::from_secs(15),
            share_commit_timeout: Duration::from_secs(15),
            share_commit_grace: Duration::from_secs(5),
            candidate_orphan_confirmations: ORPHAN_CONFIRMATIONS,
            capture_overpay_ceiling_bps: 100,
            offer_standby: None,
            extranonce2_size: 8,
            coinbase_tag: "/PRISM/".into(),
            manifest_seed: "11".repeat(32),
            ledger_seed: "22".repeat(32),
            ledger_public_key: ManifestSigningKey::from_seed_hex(&"22".repeat(32))?
                .public_key_hex(),
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
            dual_writer: None,
        };
        let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
        coordinator
            .ledger
            .observe_chain_view(&PARENT.repeat(32), HEIGHT - 1, "1")
            .await?;
        for index in 1..=3u64 {
            coordinator.ledger.append(seed_share(index), None).await?;
        }
        let sibling =
            Ledger::connect(&database.url, "revision-race-sibling".into(), 4, false).await?;
        if front.state.sibling.set(sibling).is_err() {
            anyhow::bail!("the sibling ledger was opened twice");
        }
        Ok((pool, coordinator, node, front))
    }

    fn ledger(&self) -> &Ledger {
        &self.coordinator.ledger
    }

    /// A block found on `snapshot` at `HEIGHT` with this frontend's keys, so
    /// the post-offer rebuild reproduces its audit, carrying `deferred` as
    /// the below-target proof whose credit only a confirmation produces.
    fn found(&self, snapshot: &Snapshot, deferred: AcceptedShare) -> Result<Candidate> {
        let manifest_key =
            ManifestSigningKey::from_seed_hex(&self.coordinator.config.manifest_seed)?;
        let ledger_key = ManifestSigningKey::from_seed_hex(&self.coordinator.config.ledger_seed)?;
        let bundle = qbit_prism::build_audit_bundle_with_coinbase_options(
            snapshot.shares.clone(),
            FoundBlock {
                block_height: HEIGHT,
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
            "previousblockhash":PARENT.repeat(32),"transactions":[]});
        let job = codec::Job::from_manifest(
            format!("revision-race-{HEIGHT}"),
            &template,
            &bundle.signed_coinbase_manifest.manifest,
            "00000000",
            8,
            1e-12,
            0.0,
            true,
        )?;
        let proof = (0..20_000u32)
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
            deferred_share: Some(deferred),
            block_bytes,
            as_issued_balances: snapshot.prior_balances.clone(),
        })
    }

    /// Enqueue our block at `HEIGHT` and claim it, with the tip hint and the
    /// revision current so the offer needs no pre-offer probe.
    async fn claim_found(&self) -> Result<CandidateClaim> {
        let ledger = self.ledger();
        let snapshot = ledger.snapshot(u128::from(HEIGHT - 1)).await?;
        let candidate = self.found(&snapshot, deferred_share())?;
        let hash = candidate.block_hash.clone();
        *self.coordinator.observed_tip.write().await = TipState::baseline(PARENT.repeat(32));
        ensure!(
            ledger
                .enqueue_candidate_observed(candidate, Some(unix_ms_now()?))
                .await?,
            "the candidate was not enqueued"
        );
        let claim = ledger
            .claim_candidate(120)
            .await?
            .context("the candidate was not claimable")?;
        ensure!(
            claim.candidate.block_hash == hash,
            "another row was claimed"
        );
        Ok(claim)
    }

    /// Run one attempt on `claim` with `script` loaded into the front,
    /// capturing the attempt's events at INFO and above.
    async fn process(&self, claim: &CandidateClaim, script: Vec<Step>) -> Result<Attempt> {
        *self.front.state.script.lock().await = script.into();
        let before = self.front.state.observations.load(Ordering::SeqCst);
        let capture = LogCapture::default();
        tokio::time::timeout(
            PROCESS_BOUND,
            self.coordinator
                .process_candidate(claim)
                .with_subscriber(capturing_subscriber(&capture)),
        )
        .await
        .context("the attempt did not complete")??;
        let failures = self.front.state.failures.lock().unwrap().clone();
        ensure!(failures.is_empty(), "the front failed a step: {failures:?}");
        ensure!(
            self.front.state.script.lock().await.is_empty(),
            "the attempt made fewer observations than its script"
        );
        Ok(Attempt {
            observations: self.front.state.observations.load(Ordering::SeqCst) - before,
            events: capture.events()?,
        })
    }

    /// Make the reconciliation row due, claim it and run one attempt.
    async fn retry(&self, block_hash: &str, script: Vec<Step>) -> Result<Attempt> {
        let due = sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1 AND state='reconciliation'")
            .bind(block_hash)
            .execute(&self.pool)
            .await?
            .rows_affected();
        ensure!(due == 1, "the row is not in reconciliation");
        let claim = self
            .ledger()
            .claim_candidate(120)
            .await?
            .context("the reconciliation row was not claimable")?;
        ensure!(
            claim.candidate.block_hash == block_hash,
            "another row was claimed"
        );
        self.process(&claim, script).await
    }

    /// The whole outbox row, as JSON, minus the block bytes it cannot render.
    async fn row(&self, block_hash: &str) -> Result<Value> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(o) - 'block_bytes' - 'candidate' FROM qbit_block_candidate_outbox o WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn revision(&self) -> Result<i64> {
        self.ledger().payout_revision().await
    }

    /// Everything a landing and a confirmation write, none of it keyed by the
    /// block hash, which differs between fixtures (the window's anchor time
    /// is part of the audit).
    async fn accounting(&self) -> Result<Accounting> {
        Ok(Accounting {
            balances: sqlx::query_as(
                "SELECT miner_id,payout_order_key,balance_sats::text,active_row_count FROM qbit_payout_carry_forward_current ORDER BY 1,2,3",
            )
            .fetch_all(&self.pool)
            .await?,
            entries: sqlx::query_as(
                "SELECT miner_id,payout_order_key,onchain_amount_sats,carry_forward_balance_sats::text,action FROM qbit_pool_payout_entries ORDER BY 1,2,3",
            )
            .fetch_all(&self.pool)
            .await?,
            carry: sqlx::query_as(
                "SELECT miner_id,gross_amount_sats,onchain_amount_sats,carry_forward_balance_sats::text,maturity_state FROM qbit_payout_carry_forward ORDER BY 1,2,3",
            )
            .fetch_all(&self.pool)
            .await?,
            blocks: sqlx::query_as(
                "SELECT chain_state,maturity_state,audit_publication_sequence IS NOT NULL FROM qbit_pool_blocks ORDER BY 1",
            )
            .fetch_all(&self.pool)
            .await?,
            audits: sqlx::query_scalar("SELECT count(*) FROM qbit_pool_audit_bundles")
                .fetch_one(&self.pool)
                .await?,
            shares: sqlx::query_as(
                "SELECT share_id,miner_id,share_difficulty::text FROM qbit_share_ledger ORDER BY 1",
            )
            .fetch_all(&self.pool)
            .await?,
        })
    }

    async fn close(self, result: Result<()>) -> Result<()> {
        self.node.stop();
        if let Some(sibling) = self.front.state.sibling.get() {
            sibling.pool.close().await;
        }
        self.coordinator.ledger.pool.close().await;
        self.pool.close().await;
        self.database.close(result).await
    }
}

/// What one attempt did, as the front and its events saw it.
struct Attempt {
    observations: usize,
    events: Vec<Value>,
}

impl Attempt {
    fn messages(&self, message: &str) -> Vec<&Value> {
        self.events
            .iter()
            .filter(|event| event["fields"]["message"] == message)
            .collect()
    }

    /// The attempt's `ALERT` lines, of any kind.
    fn alerts(&self) -> Vec<&Value> {
        self.events
            .iter()
            .filter(|event| {
                event["fields"]["message"]
                    .as_str()
                    .is_some_and(|message| message.starts_with("ALERT"))
            })
            .collect()
    }
}

#[derive(Debug, PartialEq)]
struct Accounting {
    balances: Vec<(String, String, String, i64)>,
    entries: Vec<(String, String, i64, String, String)>,
    carry: Vec<(String, i64, i64, String, String)>,
    blocks: Vec<(String, String, bool)>,
    audits: i64,
    shares: Vec<(String, String, String)>,
}

/// The no-race outcome every raced case is compared with: one attempt, two
/// observations (the landing's and the finish's), no retry, no `ALERT`, the
/// block landed, confirmed and counted once, and its deferred share credited.
struct Control {
    accounting: Accounting,
    /// How far the attempt moved the payout revision: the tip record and the
    /// confirmation.
    revision_moves: i64,
    /// The finished row's attempt count: no attempt beyond the first.
    attempt_count: Value,
}

async fn control() -> Result<Option<Control>> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(None);
    };
    let result = async {
        let claim = fixture.claim_found().await?;
        let hash = claim.candidate.block_hash.clone();
        let before = fixture.revision().await?;
        let attempt = fixture.process(&claim, vec![]).await?;
        ensure!(
            attempt.observations == 2,
            "the no-race attempt observed {} times",
            attempt.observations
        );
        ensure!(attempt.messages(RETRY_LINE).is_empty() && attempt.alerts().is_empty());
        let row = fixture.row(&hash).await?;
        ensure!(
            row["state"] == "submitted" && row["last_error"].is_null(),
            "{row}"
        );
        ensure!(fixture.coordinator.blocks.load(Ordering::Relaxed) == 1);
        let accounting = fixture.accounting().await?;
        ensure!(
            accounting.audits == 1
                && accounting.blocks == [("confirmed".to_owned(), "immature".to_owned(), true)]
                && accounting
                    .shares
                    .iter()
                    .any(|(id, ..)| *id == deferred_share().share_id),
            "the no-race case did not land, confirm and credit its block: {accounting:?}"
        );
        Ok(Control {
            accounting,
            revision_moves: fixture.revision().await? - before,
            attempt_count: row["attempt_count"].clone(),
        })
    }
    .await;
    match result {
        Ok(control) => {
            fixture.close(Ok(())).await?;
            Ok(Some(control))
        }
        Err(error) => Err(fixture.close(Err(error)).await.unwrap_err()),
    }
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// The issue's acceptance, on the write the soak lost: the landing committed,
/// and a sibling frontend confirmed the block between the finish's
/// observation and its write. The finish is refused, observed again and
/// written at the new revision in the same attempt, and the row is
/// `submitted` with no reconciliation and no `ALERT`. The partial success is
/// reconciled, not repeated: one audit, one confirmation (the revision moves
/// exactly as in the no-race case, so the finish did not confirm or bump
/// again), one credit of the deferred share, and the balances and payout
/// rows equal the no-race case's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finish_that_loses_the_race_to_a_sibling_confirmation_settles_in_place() -> Result<()> {
    let Some(control) = control().await? else {
        return Ok(());
    };
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.claim_found().await?;
        let hash = claim.candidate.block_hash.clone();
        let before = fixture.revision().await?;
        let script = vec![Step::Forward, Step::SiblingConfirms(hash.clone())];
        let attempt = fixture.process(&claim, script).await?;

        ensure!(
            attempt.observations == 3,
            "expected the landing's, the refused finish's and the retry's observations, saw {}",
            attempt.observations
        );
        let retries = attempt.messages(RETRY_LINE);
        ensure!(
            retries.len() == 1
                && retries[0]["level"] == "INFO"
                && retries[0]["fields"]["write"] == "finish"
                && retries[0]["fields"]["block"] == hash.as_str(),
            "the lost race was not logged once, below ALERT, for the finish: {:?}",
            attempt.events
        );
        ensure!(attempt.alerts().is_empty(), "a recovered race logged an ALERT: {:?}", attempt.alerts());
        let row = fixture.row(&hash).await?;
        ensure!(
            row["state"] == "submitted"
                && row["last_error"].is_null()
                && row["offer_outcome"] == "accepted"
                && row["attempt_count"] == control.attempt_count,
            "the row did not finish in place: {row}"
        );
        ensure!(
            fixture.revision().await? - before == control.revision_moves,
            "the revision moved {} times, the no-race case {}: the confirmation was applied twice or not at all",
            fixture.revision().await? - before,
            control.revision_moves
        );
        // The sibling counted the first confirmation; this frontend's finish
        // found it already counted.
        ensure!(fixture.coordinator.blocks.load(Ordering::Relaxed) == 0);
        let accounting = fixture.accounting().await?;
        ensure!(
            accounting == control.accounting,
            "the raced settlement differs from the no-race case:\n raced: {accounting:?}\nno race: {:?}",
            control.accounting
        );
        ensure!(fixture.node.state.lock().await.submissions.get(&hash).map(Vec::len) == Some(1));
        Ok(())
    }
    .await;
    fixture.close(result).await
}

/// The issue's acceptance, on the landing: the revision moves between the
/// landing's observation and its transaction. The refused landing wrote
/// nothing; it is observed again and landed from the same rebuilt parts, and
/// the row finishes `submitted` in the same attempt with no `ALERT`. The
/// accounting equals the no-race case's, and the revision moved exactly once
/// more than there, by the injected move alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_landing_that_loses_the_race_lands_once_in_place() -> Result<()> {
    let Some(control) = control().await? else {
        return Ok(());
    };
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.claim_found().await?;
        let hash = claim.candidate.block_hash.clone();
        let before = fixture.revision().await?;
        let attempt = fixture.process(&claim, vec![Step::Bump]).await?;

        ensure!(
            attempt.observations == 3,
            "expected the refused landing's, the retry's and the finish's observations, saw {}",
            attempt.observations
        );
        let retries = attempt.messages(RETRY_LINE);
        ensure!(
            retries.len() == 1
                && retries[0]["level"] == "INFO"
                && retries[0]["fields"]["write"] == "landing",
            "the lost race was not logged once, below ALERT, for the landing: {:?}",
            attempt.events
        );
        ensure!(attempt.alerts().is_empty(), "a recovered race logged an ALERT: {:?}", attempt.alerts());
        let row = fixture.row(&hash).await?;
        ensure!(
            row["state"] == "submitted"
                && row["last_error"].is_null()
                && row["attempt_count"] == control.attempt_count,
            "the row did not finish in place: {row}"
        );
        ensure!(fixture.revision().await? - before == control.revision_moves + 1);
        ensure!(fixture.coordinator.blocks.load(Ordering::Relaxed) == 1);
        let accounting = fixture.accounting().await?;
        ensure!(
            accounting == control.accounting,
            "the raced landing differs from the no-race case:\n raced: {accounting:?}\nno race: {:?}",
            control.accounting
        );
        Ok(())
    }
    .await;
    fixture.close(result).await
}

/// The bound: a revision that moves before every write of the phase is
/// retried `RETRIES` times, and then the row is settled in reconciliation
/// with today's `ALERT`, its reason the landing-failed prefix, the fence's
/// message and the spent budget. Nothing landed. The next attempt, with no
/// race, lands and finishes it with the no-race accounting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_race_that_outlasts_the_retry_budget_keeps_the_alert_and_reconciles() -> Result<()> {
    let Some(control) = control().await? else {
        return Ok(());
    };
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.claim_found().await?;
        let hash = claim.candidate.block_hash.clone();
        let attempt = fixture.process(&claim, vec![Step::Bump; RETRIES + 1]).await?;

        ensure!(
            attempt.observations == RETRIES + 1,
            "expected {} landing observations, saw {}",
            RETRIES + 1,
            attempt.observations
        );
        ensure!(
            attempt.messages(RETRY_LINE).len() == RETRIES,
            "expected {RETRIES} retry lines: {:?}",
            attempt.events
        );
        let alerts = attempt.messages(NOT_LANDED);
        ensure!(
            alerts.len() == 1 && alerts[0]["level"] == "ERROR" && attempt.alerts().len() == 1,
            "the spent budget did not keep today's single ALERT: {:?}",
            attempt.events
        );
        let row = fixture.row(&hash).await?;
        let reason = row["last_error"].as_str().unwrap_or_default();
        ensure!(
            row["state"] == "reconciliation"
                && reason.starts_with(qbit_prism_server::ledger::LANDING_FAILED_REASON_PREFIX)
                && reason.ends_with(RACE)
                && reason.contains("retries spent"),
            "the row was not reconciled with the spent race as its reason: {row}"
        );
        ensure!(alerts[0]["fields"]["reason"] == reason);
        ensure!(fixture.accounting().await?.audits == 0, "a refused landing wrote an audit");

        let next = fixture.retry(&hash, vec![]).await?;
        ensure!(next.alerts().is_empty() && next.messages(RETRY_LINE).is_empty());
        ensure!(fixture.row(&hash).await?["state"] == "submitted");
        let accounting = fixture.accounting().await?;
        ensure!(
            accounting == control.accounting,
            "the reconciled block differs from the no-race case:\n reconciled: {accounting:?}\n   no race: {:?}",
            control.accounting
        );
        ensure!(fixture.node.state.lock().await.submissions.get(&hash).map(Vec::len) == Some(1));
        Ok(())
    }
    .await;
    fixture.close(result).await
}

/// A node failure after the landing is not a race either: the node fails the
/// finish's observation after the landing committed, and the row goes to
/// reconciliation with today's `ALERT` and the node's error as its reason, in
/// one attempt with no retry line. The next attempt reuses the landed audit
/// and finishes the row with the no-race accounting. (A refusal of a write
/// itself that is not a race is the orphan test's second refusal.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failure_that_is_not_a_revision_race_keeps_the_alert_and_reconciles() -> Result<()> {
    let Some(control) = control().await? else {
        return Ok(());
    };
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.claim_found().await?;
        let hash = claim.candidate.block_hash.clone();
        let attempt = fixture.process(&claim, vec![Step::Forward, Step::Fail]).await?;

        ensure!(
            attempt.observations == 2,
            "a node failure was retried: {} observations",
            attempt.observations
        );
        ensure!(attempt.messages(RETRY_LINE).is_empty(), "a node failure was treated as a race");
        let alerts = attempt.messages(NOT_PROCESSED);
        ensure!(
            alerts.len() == 1 && alerts[0]["level"] == "ERROR" && attempt.alerts().len() == 1,
            "the node failure did not log today's single ALERT: {:?}",
            attempt.events
        );
        let row = fixture.row(&hash).await?;
        let reason = row["last_error"].as_str().unwrap_or_default();
        ensure!(
            row["state"] == "reconciliation"
                && reason.starts_with("post-offer processing failed (node accepted the offer)")
                && reason.contains(INJECTED)
                && !reason.contains(RACE),
            "the row was not reconciled with the node's error: {row}"
        );
        ensure!(fixture.accounting().await?.audits == 1, "the landing did not commit before the failure");

        let next = fixture.retry(&hash, vec![]).await?;
        ensure!(next.observations == 1, "the retry landed again instead of reusing the audit");
        ensure!(next.alerts().is_empty() && next.messages(RETRY_LINE).is_empty());
        ensure!(fixture.row(&hash).await?["state"] == "submitted");
        let accounting = fixture.accounting().await?;
        ensure!(
            accounting == control.accounting,
            "the reconciled block differs from the no-race case:\n reconciled: {accounting:?}\n   no race: {:?}",
            control.accounting
        );
        Ok(())
    }
    .await;
    fixture.close(result).await
}

/// The orphan settlement retries a race like the other writes, and a
/// refusal of the write itself that is not a race is not retried. Our block
/// loses the tip race, and its first attempt lands the audit and leaves the
/// row in reconciliation (not active, the competitor not yet deep enough).
/// Once the competitor has the configured confirmations, the next attempt's
/// observation proves the orphan, and a sibling frontend's reconciler
/// confirms our block before the settlement is written, as a stale reorg
/// proof would. The settlement loses that race and is written again from a
/// fresh observation, which proves the orphan again; this time it is refused
/// because the ledger holds the block confirmed. That contradiction is a
/// genuine failure: no second retry, today's `ALERT`, and the row stays in
/// reconciliation, never orphaned over a confirmed block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_orphan_settlement_retries_a_race_but_not_a_genuine_refusal() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let claim = fixture.claim_found().await?;
        let hash = claim.candidate.block_hash.clone();
        fixture
            .node
            .state
            .lock()
            .await
            .lose_next_race_to(&COMPETITOR.repeat(32));
        let first = fixture.process(&claim, vec![]).await?;
        ensure!(
            first.observations == 2
                && first.alerts().is_empty()
                && first.messages(RETRY_LINE).is_empty(),
            "the lost tip race was not an ordinary reconciliation: {:?}",
            first.events
        );
        let row = fixture.row(&hash).await?;
        ensure!(
            row["state"] == "reconciliation"
                && row["last_error"]
                    .as_str()
                    .is_some_and(|reason| reason.contains("not on the active chain")),
            "{row}"
        );
        {
            let mut chain = fixture.node.state.lock().await;
            while chain.height < HEIGHT + ORPHAN_CONFIRMATIONS - 1 {
                chain.advance();
            }
        }

        let script = vec![Step::SiblingConfirms(hash.clone()), Step::Forward];
        let attempt = fixture.retry(&hash, script).await?;
        ensure!(
            attempt.observations == 2,
            "expected the refused settlement's and the retry's observations, saw {}",
            attempt.observations
        );
        let retries = attempt.messages(RETRY_LINE);
        ensure!(
            retries.len() == 1
                && retries[0]["level"] == "INFO"
                && retries[0]["fields"]["write"] == "orphan settlement",
            "the orphan settlement's lost race was not retried once: {:?}",
            attempt.events
        );
        let alerts = attempt.messages(NOT_PROCESSED);
        ensure!(
            alerts.len() == 1 && alerts[0]["level"] == "ERROR" && attempt.alerts().len() == 1,
            "the genuine refusal did not log today's single ALERT: {:?}",
            attempt.events
        );
        let row = fixture.row(&hash).await?;
        let reason = row["last_error"].as_str().unwrap_or_default();
        ensure!(
            row["state"] == "reconciliation"
                && reason.starts_with("post-offer processing failed (node accepted the offer)")
                && reason.contains("its block is confirmed")
                && !reason.contains(RACE),
            "the row was not reconciled with the genuine refusal: {row}"
        );
        let blocks = fixture.accounting().await?.blocks;
        ensure!(
            blocks == [("confirmed".to_owned(), "immature".to_owned(), true)],
            "the sibling's confirmation was undone: {blocks:?}"
        );
        ensure!(
            fixture
                .node
                .state
                .lock()
                .await
                .submissions
                .get(&hash)
                .map(Vec::len)
                == Some(1)
        );
        Ok(())
    }
    .await;
    fixture.close(result).await
}

// ---------------------------------------------------------------------------
// Shares and logs
// ---------------------------------------------------------------------------

fn seed_share(index: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("miner:{index:064x}"),
        miner_id: format!("miner-{}", index % 2),
        order_key: format!("miner-{}", index % 2),
        p2mr_program_hex: format!("{:02x}", 0x10 + index).repeat(32),
        share_difficulty: 100,
        network_difficulty: 100,
        template_height: HEIGHT - 1,
        job_id: "seed".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

/// The below-target proof that found the block: credited only by an
/// active-chain confirmation, and only once.
fn deferred_share() -> AcceptedShare {
    AcceptedShare {
        share_id: format!("solver:{:064x}", 9u64),
        miner_id: "solver".into(),
        order_key: "solver".into(),
        p2mr_program_hex: "9a".repeat(32),
        template_height: HEIGHT,
        ..seed_share(9)
    }
}

fn unix_ms_now() -> Result<i64> {
    let elapsed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    Ok(i64::try_from(elapsed.as_millis())?)
}

/// Collects one attempt's events. The subscriber is attached to the
/// attempt's future alone, never installed globally, so each capture belongs
/// to its test.
#[derive(Clone, Default)]
struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl LogCapture {
    fn events(&self) -> Result<Vec<Value>> {
        String::from_utf8(self.0.lock().unwrap().clone())?
            .lines()
            .map(|line| Ok(serde_json::from_str(line)?))
            .collect()
    }
}

fn capturing_subscriber(capture: &LogCapture) -> impl tracing::Subscriber + Send + Sync {
    let writer = capture.clone();
    tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || writer.clone())
        .finish()
}
