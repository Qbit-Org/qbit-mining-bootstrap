//! The lost-race pin for the 2026-09-16 mainnet orphan stall (#413), and the
//! terminal disposition of a proven orphan (#415).
//!
//! On 2026-09-16 the 2.x.x mainnet coordinator held **all** job delivery for
//! 307 s after one of its own blocks lost a tip race: the
//! block was qbitd's best block for 61 ms, a same-height competitor replaced
//! it, and the Python abandon path could not reach a verdict, so the payout
//! transition stayed `landed`, every tip-refresh wave ended `payout_blocked`,
//! and a valid solve found during the hold was discarded.
//!
//! 3.x.x cannot stall that way by construction: a block that is not on the
//! active chain after its offer becomes a `reconciliation` row, and job
//! issuance never reads candidate state (each job carries its own committed
//! payout snapshot; tip observations fence candidates, not delivery). These
//! tests are the **pin** on that property. They are expected to pass against
//! the tree that introduced them; they fail only if issuance is ever coupled
//! to candidate state, if a lost race stops being a reconciliation row, if a
//! block is offered twice, or if the orphan disposition stops being terminal
//! and reversible for credit.
//!
//! The lost race is reproduced exactly: `submitblock` returns null and our
//! hash is the node's best tip, within `RACE_WINDOW` the node reorgs to a
//! same-height competitor, and the tip then advances two further heights.
//!
//! Run through test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test lost_race_pin -- --nocapture.
use anyhow::{ensure, Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, FoundBlock, PayoutPolicy};
use qbit_prism_server::{
    codec,
    config::Config,
    coordinator::{Coordinator, TipState},
    ledger::{BlockObservation, Candidate, Ledger, SignerKeys, Snapshot, WindowRef},
    metrics::{collectors, Metrics},
    stratum::MiningBackend,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

/// The incident's race: our block is the best tip, and the node reorgs to the
/// competitor this soon after. #413 measured 61 ms; the test drives the reorg
/// as soon as the offer is observed at the node and asserts it landed inside
/// this window, so the reproduction stays the incident's and not a slow one.
const RACE_WINDOW: Duration = Duration::from_millis(100);
/// The pin's bound: after the race, each new tip's work must be issued within
/// this much of the tip becoming visible, *while* a candidate claim on the
/// orphaned row is held and blocked on the settlement lock. The 2.x.x
/// incident's equivalent number was 307 s. This is three orders of magnitude
/// below that, and still generous for a loaded CI host; the measured values
/// are printed before the bound is asserted.
const ISSUANCE_BOUND: Duration = Duration::from_secs(2);
/// The height our block and its competitor are both found at.
const HEIGHT: u64 = 101;
const PARENT: &str = "aa";
/// The competitor that wins the race: same height, same parent.
const COMPETITOR: &str = "bb";

struct NodeState {
    tip: String,
    height: u64,
    chainwork: u64,
    /// The active chain by height, for `getblockhash`.
    blocks: HashMap<u64, String>,
    parents: HashMap<String, String>,
    /// Every `submitblock` arrival, by block hash.
    submissions: HashMap<String, Vec<Instant>>,
}

impl NodeState {
    /// Replace the active block at `height` and make the replacement the tip,
    /// discarding everything above: the node's own reorg.
    fn reorg_to(&mut self, height: u64, hash: &str) {
        self.blocks.retain(|at, _| *at < height);
        self.blocks.insert(height, hash.to_owned());
        self.tip = hash.to_owned();
        self.height = height;
        self.chainwork += 1;
    }

    /// Extend the active chain by one block of no interest to the pool.
    fn advance(&mut self) {
        let height = self.height + 1;
        let hash = format!("{height:02x}").repeat(32);
        self.parents.insert(hash.clone(), self.tip.clone());
        self.blocks.insert(height, hash.clone());
        self.tip = hash;
        self.height = height;
        self.chainwork += 1;
    }
}

async fn node_reply(
    State(node): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let arrived = Instant::now();
    let now = chrono::Utc::now().timestamp();
    let mut node = node.lock().await;
    let result = match request["method"].as_str().unwrap_or("") {
        "getblockhash" if request["params"][0] == 0 => json!("00".repeat(32)),
        "getblockhash" => {
            let height = request["params"][0].as_u64().unwrap_or(0);
            // A height above the tip has no block; the node would error, and
            // the coordinator never asks (it compares the tip height first).
            json!(node.blocks.get(&height).cloned())
        }
        "getblockheader" => {
            json!({"previousblockhash": node.parents.get(request["params"][0].as_str().unwrap_or(""))})
        }
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":node.height,"headers":node.height,"bestblockhash":node.tip,
            "chainwork":format!("{:x}",node.chainwork)}),
        "getbestblockhash" => json!(node.tip),
        "getnetworkinfo" => json!({"connections":2}),
        "getblocktemplate" => json!({"height":node.height+1,"coinbasevalue":500_000_000u64,
            "previousblockhash":node.tip,"version":0x20000000u32,"bits":"207fffff",
            "curtime":now,"mintime":now-1,"transactions":[]}),
        "estimatesmartfee" => json!({"feerate":"0.00001"}),
        "getmempoolinfo" => json!({"minrelaytxfee":"0.00001","mempoolminfee":"0.00001"}),
        "validateaddress" => {
            json!({"isvalid":true,"scriptPubKey":format!("5220{}","11".repeat(32))})
        }
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap_or("")).unwrap();
            let hash = codec::hash_display(&codec::double_sha256(&block[..80]));
            node.submissions
                .entry(hash.clone())
                .or_default()
                .push(arrived);
            // Accepted, exactly as qbitd accepted ours: null reply, and our
            // block is the node's best block. The race is lost afterwards.
            let height = node.height + 1;
            let previous = node.tip.clone();
            node.parents.insert(hash.clone(), previous);
            node.tip = hash.clone();
            node.height = height;
            node.chainwork += 1;
            node.blocks.insert(height, hash);
            Value::Null
        }
        method => panic!("unexpected lost-race RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

struct Fixture {
    admin: PgPool,
    /// A plain connection pool on the fixture's schema, for the lock hold.
    pool: PgPool,
    schema: String,
    coordinator: Arc<Coordinator>,
    metrics: Arc<Metrics>,
    node: Arc<Mutex<NodeState>>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_lost_race_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let pool = PgPool::connect(url.as_str()).await?;
        let node = Arc::new(Mutex::new(NodeState {
            tip: PARENT.repeat(32),
            height: HEIGHT - 1,
            chainwork: 1,
            blocks: HashMap::from([(HEIGHT - 1, PARENT.repeat(32))]),
            parents: HashMap::from([(PARENT.repeat(32), "00".repeat(32))]),
            submissions: HashMap::new(),
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let rpc_url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(node_reply))
            .with_state(node.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = Config {
            database_url: url.to_string(),
            instance_id: "lost-race".into(),
            database_connections: 8,
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
            rpc_timeout: Duration::from_secs(5),
            block_submit_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_secs(1),
            blockwait: false,
            build_workers: 2,
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
        };
        let metrics = Arc::new(Metrics::default());
        let coordinator = Coordinator::new(config, metrics.clone()).await?;
        coordinator
            .ledger
            .observe_chain_view(&PARENT.repeat(32), HEIGHT - 1, "1")
            .await?;
        for index in 1..=3u64 {
            coordinator.ledger.append(seed_share(index), None).await?;
        }
        Ok(Some(Self {
            admin,
            pool,
            schema,
            coordinator,
            metrics,
            node,
            server,
        }))
    }

    fn ledger(&self) -> &Ledger {
        &self.coordinator.ledger
    }

    /// A block found on `snapshot` at `HEIGHT` on `parent`, with this
    /// frontend's keys, so the post-offer rebuild reproduces its audit.
    /// `deferred` is the below-target block-only proof the block carries, the
    /// share whose credit the reactivation branch must still produce.
    fn found(
        &self,
        snapshot: &Snapshot,
        parent: &str,
        deferred: Option<AcceptedShare>,
    ) -> Result<Candidate> {
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
            "previousblockhash":parent,"transactions":[]});
        let job = codec::Job::from_manifest(
            format!("lost-race-{HEIGHT}"),
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
            deferred_share: deferred,
            block_bytes,
            as_issued_balances: snapshot.prior_balances.clone(),
        })
    }

    /// The whole outbox row, as JSON, minus the block bytes it cannot render.
    async fn row(&self, block_hash: &str) -> Result<Value> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(o) - 'block_bytes' - 'candidate' || jsonb_build_object('has_block',block_bytes IS NOT NULL,'has_document',candidate IS NOT NULL) FROM qbit_block_candidate_outbox o WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn chain_state(&self, block_hash: &str) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
                .bind(block_hash)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    async fn credited(&self, share_id: &str) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
                .bind(share_id)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// What the block-only acknowledgement path reads for a share on this
    /// block: exactly the columns of `Coordinator`'s disposition poll
    /// (`coordinator/miner_submit.rs`).
    async fn block_only_disposition(
        &self,
        share_id: &str,
        block_hash: &str,
    ) -> Result<(bool, Option<String>, Option<String>)> {
        Ok(sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1), (SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$2), (SELECT offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$2)",
        )
        .bind(share_id)
        .bind(block_hash)
        .fetch_one(&self.pool)
        .await?)
    }

    /// The reconciliation lane retries `min(3600, 10 x attempt_count)` s
    /// apart. Nothing about the disposition depends on waiting that out, so
    /// the tests make the row due instead of sleeping.
    async fn make_due(&self, block_hash: &str) -> Result<()> {
        let due = sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1 AND next_attempt_at<>'infinity'::timestamptz")
            .bind(block_hash)
            .execute(&self.pool)
            .await?
            .rows_affected();
        ensure!(due == 1, "the row was parked or absent, never due again");
        Ok(())
    }

    /// Every `submitblock` arrival for the block.
    async fn arrivals(&self, block_hash: &str) -> Vec<Instant> {
        self.node
            .lock()
            .await
            .submissions
            .get(block_hash)
            .cloned()
            .unwrap_or_default()
    }

    /// The cluster-wide pending-candidate gauges, as the metrics collector
    /// observes them.
    async fn gauges(&self) -> Result<(u64, Duration)> {
        let observed = collectors::database(&self.coordinator.ledger.pool, &self.metrics).await?;
        Ok((observed.candidates, observed.candidate_oldest))
    }

    async fn close(self) -> Result<()> {
        self.server.abort();
        self.coordinator.ledger.pool.close().await;
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

/// The confirmation depth the tests configure, matching the production
/// default so the pin also documents it.
const ORPHAN_CONFIRMATIONS: u64 = 6;

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

/// The below-target proof that found the block: not credited when the block
/// is found, credited only by an active-chain confirmation.
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

/// Offer the candidate, lose the race inside `RACE_WINDOW`, and return the
/// block hash and how long after the offer's arrival the reorg was installed.
async fn lose_the_race(
    fixture: &Fixture,
    deferred: Option<AcceptedShare>,
) -> Result<(String, Duration)> {
    let ledger = fixture.ledger();
    let snapshot = ledger.snapshot(u128::from(HEIGHT - 1)).await?;
    let candidate = fixture.found(&snapshot, &PARENT.repeat(32), deferred)?;
    let hash = candidate.block_hash.clone();
    *fixture.coordinator.observed_tip.write().await = TipState::baseline(PARENT.repeat(32));
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
    let process = {
        let coordinator = fixture.coordinator.clone();
        tokio::spawn(async move { coordinator.process_candidate(&claim).await })
    };

    // The node accepted it and it is the best block. Lose the race now, the
    // way qbitd did 61 ms later: a same-height competitor replaces it.
    let arrival = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(first) = fixture.arrivals(&hash).await.first().copied() {
                return first;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("the block never reached submitblock")?;
    {
        let mut node = fixture.node.lock().await;
        ensure!(
            node.tip == hash && node.height == HEIGHT,
            "the fake node did not make our block the best block at {HEIGHT}"
        );
        node.parents
            .insert(COMPETITOR.repeat(32), PARENT.repeat(32));
        let competitor = COMPETITOR.repeat(32);
        node.reorg_to(HEIGHT, &competitor);
    }
    let raced_after = arrival.elapsed();
    tokio::time::timeout(Duration::from_secs(60), process)
        .await
        .context("the post-offer settlement did not complete")???;
    ensure!(
        raced_after <= RACE_WINDOW,
        "the competitor replaced our block {:.0} ms after it reached the node, outside the {} ms the incident measured",
        raced_after.as_secs_f64() * 1e3,
        RACE_WINDOW.as_millis()
    );
    Ok((hash, raced_after))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_tip_race_reconciles_the_orphan_and_never_delays_the_next_tips_work() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = lost_race_pin(&fixture).await;
    fixture.close().await?;
    result
}

async fn lost_race_pin(fixture: &Fixture) -> Result<()> {
    let ledger = fixture.ledger();
    let (hash, raced_after) = lose_the_race(fixture, Some(deferred_share())).await?;

    // (b) The row is a reconciliation row with the not-active reason, its
    // accepted offer recorded, and the node was offered the block once.
    let row = fixture.row(&hash).await?;
    ensure!(
        row["state"] == "reconciliation",
        "a lost tip race left the row {:?}, not reconciliation",
        row["state"]
    );
    ensure!(
        row["offer_outcome"] == "accepted" && row["completed_at"].is_null(),
        "the accepted offer or the unfinished completion was not recorded: {row}"
    );
    let reason = row["last_error"].as_str().unwrap_or_default().to_owned();
    ensure!(
        reason.contains("not on the active chain after the offer")
            && reason.contains("never offered again"),
        "the reconciliation reason is not the not-active one: {reason:?}"
    );
    ensure!(
        row["has_document"] == true && row["has_block"] == true,
        "the reconciliation row lost its evidence: {row}"
    );
    ensure!(
        fixture.arrivals(&hash).await.len() == 1,
        "submitblock was called {} times for one block",
        fixture.arrivals(&hash).await.len()
    );
    // The landed audit and its pool block survive the lost race: this is the
    // evidence a later reactivation credits from.
    ensure!(
        fixture.chain_state(&hash).await?.as_deref() == Some("prepared"),
        "the landed block is not prepared after the lost race"
    );
    // (c) A share whose only proof is this block is not credited, and the
    // acknowledgement path sees a disposition that is not a proof of loss:
    // a reconciliation row with an `accepted` outcome keeps waiting, because
    // the block can still become active.
    let solver = deferred_share().share_id;
    ensure!(
        fixture.credited(&solver).await? == 0,
        "the orphan's block-only proof was credited before any active-chain confirmation"
    );
    let (credited, state, outcome) = fixture.block_only_disposition(&solver, &hash).await?;
    ensure!(
        !credited
            && state.as_deref() == Some("reconciliation")
            && outcome.as_deref() == Some("accepted"),
        "the block-only acknowledgement path saw ({credited}, {state:?}, {outcome:?})"
    );

    // (a) The pin. The unfinished row is put in every state a stuck candidate
    // can be in at once -- claimed by a live lease, and its outbox row held
    // under a `FOR UPDATE` lock so any settlement write would block -- and
    // the tip then advances two further heights. Every new tip's work must
    // still be issued immediately. This is exactly the shape of the 2.x.x
    // incident, where an unfinished own block held all job delivery for
    // 307 s; here issuance reads no candidate state and waits on nothing the
    // stuck candidate holds, so the bound below is milliseconds.
    fixture.make_due(&hash).await?;
    let stuck = ledger
        .claim_candidate(600)
        .await?
        .context("the reconciliation row was not claimable for its retry")?;
    ensure!(
        stuck.candidate.block_hash == hash,
        "another row was claimed for the retry"
    );
    let mut hold = fixture.pool.begin().await?;
    let locked: Option<String> = sqlx::query_scalar(
        "SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR UPDATE",
    )
    .bind(&hash)
    .fetch_optional(&mut *hold)
    .await?;
    ensure!(
        locked.as_deref() == Some("reconciliation"),
        "the held row is {locked:?}, not the reconciliation row the race left"
    );

    let worker = fixture.coordinator.authorize("solver.worker").await?;
    let mut issuance = Vec::new();
    for advance in 0..2 {
        let tip = {
            let mut node = fixture.node.lock().await;
            node.advance();
            node.tip.clone()
        };
        let visible = Instant::now();
        fixture.coordinator.refresh_once().await?;
        let job = fixture
            .coordinator
            .build_job(&worker, "00000000", 1e-12, 0.0)
            .await?;
        let issued = visible.elapsed();
        ensure!(
            job.wire.previousblockhash == tip,
            "advance {advance}: the issued job is for {}, not the new tip {tip}",
            job.wire.previousblockhash
        );
        issuance.push(issued);
    }
    // Nothing in the refresh or the issuance touched the candidate: the row
    // is still the claimed, unfinished row the race left, and the node was
    // never offered the block again.
    hold.rollback().await?;
    let row = fixture.row(&hash).await?;
    ensure!(
        row["state"] == "reconciliation" && !row["claim_token"].is_null(),
        "issuance disturbed the stuck candidate: {row}"
    );
    ensure!(
        fixture.arrivals(&hash).await.len() == 1,
        "issuance offered the block again"
    );

    // The retry runs its course once the row is free: still reconciliation,
    // still one offer. The competitor has three confirmations here, below
    // ORPHAN_CONFIRMATIONS, so the row is not yet settled terminal.
    fixture.coordinator.process_candidate(&stuck).await?;
    let row = fixture.row(&hash).await?;
    ensure!(
        row["state"] == "reconciliation",
        "the retry left the row {:?} below the orphan confirmation depth",
        row["state"]
    );
    ensure!(
        fixture.arrivals(&hash).await.len() == 1,
        "the reconciliation retry offered the block again"
    );

    let slowest = issuance.iter().copied().max().unwrap_or_default();
    eprintln!(
        "orphan-stall pin (#413): our block was the node's best block at {HEIGHT} and a same-height competitor \
         replaced it {:.0} ms later; the row is in reconciliation with one submitblock. \
         New work for the next two tips was issued {:.0} ms and {:.0} ms after each tip became \
         visible, while that row was claimed by a live lease and its outbox row was held under \
         FOR UPDATE (the 2.x.x incident held all delivery for 307 s).",
        raced_after.as_secs_f64() * 1e3,
        issuance[0].as_secs_f64() * 1e3,
        issuance[1].as_secs_f64() * 1e3,
    );
    ensure!(
        slowest <= ISSUANCE_BOUND,
        "the slowest issuance took {:.0} ms, over the {} ms bound",
        slowest.as_secs_f64() * 1e3,
        ISSUANCE_BOUND.as_millis()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_proven_orphan_is_settled_terminal_and_a_reorg_back_still_credits_it() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = orphan_disposition(&fixture).await;
    fixture.close().await?;
    result
}

async fn orphan_disposition(fixture: &Fixture) -> Result<()> {
    let ledger = fixture.ledger();
    let (hash, _) = lose_the_race(fixture, Some(deferred_share())).await?;
    let solver = deferred_share().share_id;

    // An unfinished row is counted by the pending gauges, which is what kept
    // the migrated alerts firing forever.
    let (pending, oldest) = fixture.gauges().await?;
    ensure!(
        pending == 1 && oldest > Duration::ZERO,
        "the reconciliation row is not counted as pending ({pending}, {oldest:?})"
    );

    // Below the depth, a retry keeps reconciling: unknown is not a verdict.
    for expected in [HEIGHT + 1, HEIGHT + 2] {
        fixture.node.lock().await.advance();
        ensure!(fixture.node.lock().await.height == expected);
        fixture.make_due(&hash).await?;
        let claim = ledger
            .claim_candidate(120)
            .await?
            .context("no retry claim")?;
        fixture.coordinator.process_candidate(&claim).await?;
        ensure!(
            fixture.row(&hash).await?["state"] == "reconciliation",
            "the row was settled terminal at {} confirmations, below {ORPHAN_CONFIRMATIONS}",
            expected - HEIGHT + 1
        );
    }

    // At the depth, the competitor is proven and the row is settled terminal.
    while fixture.node.lock().await.height < HEIGHT + ORPHAN_CONFIRMATIONS - 1 {
        fixture.node.lock().await.advance();
    }
    fixture.make_due(&hash).await?;
    let claim = ledger
        .claim_candidate(120)
        .await?
        .context("the reconciliation row was not claimable at the orphan depth")?;
    fixture.coordinator.process_candidate(&claim).await?;

    let row = fixture.row(&hash).await?;
    ensure!(
        row["state"] == "orphaned" && !row["completed_at"].is_null(),
        "the proven orphan was not settled terminal: {row}"
    );
    let reason = row["last_error"].as_str().unwrap_or_default().to_owned();
    ensure!(
        reason.contains("proven orphan")
            && reason.contains(&COMPETITOR.repeat(32))
            && reason.contains(&format!("{ORPHAN_CONFIRMATIONS} confirmations")),
        "the orphan reason does not carry the chain's evidence: {reason:?}"
    );
    // The disposition preserves everything an operator (#268) needs and
    // everything a reactivation credits from.
    ensure!(
        row["has_document"] == true
            && row["has_block"] == true
            && !row["window_anchor_ms"].is_null()
            && row["offer_outcome"] == "accepted"
            && !row["offer_reserved_by"].is_null()
            && !row["offered_at_ms"].is_null(),
        "the orphaned row lost its document, block bytes, window reference or offer record: {row}"
    );
    ensure!(
        fixture.arrivals(&hash).await.len() == 1,
        "the orphan settlement offered the block again"
    );
    ensure!(
        fixture.chain_state(&hash).await?.as_deref() == Some("inactive"),
        "the orphaned block is not inactive in the ledger"
    );

    // The gauges no longer count it, and the settlement is attributed once.
    let (pending, oldest) = fixture.gauges().await?;
    ensure!(
        pending == 0 && oldest == Duration::ZERO,
        "a terminal orphan is still counted by the pending gauges ({pending}, {oldest:?})"
    );
    let rendered = fixture.metrics.render();
    ensure!(
        rendered
            .lines()
            .any(|line| line == "qbit_prism_block_candidates_orphaned_total 1"),
        "the orphan settlement was not counted once:\n{}",
        rendered
            .lines()
            .filter(|line| line.contains("orphaned"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    // A share whose only proof is the orphan now has its verdict: the
    // acknowledgement path fails it rather than waiting out its bound.
    let (credited, state, _) = fixture.block_only_disposition(&solver, &hash).await?;
    ensure!(
        !credited && state.as_deref() == Some("orphaned"),
        "the block-only acknowledgement path saw ({credited}, {state:?}) for a proven orphan"
    );
    ensure!(
        fixture.credited(&solver).await? == 0,
        "a proven orphan credited its block-only proof"
    );

    // The reactivation: a deep reorg puts our block back on the active chain.
    // The terminal row never reopens; the ordinary reorg reconciler credits
    // the block from the audit the lost race had already landed, its deferred
    // share included.
    {
        let mut node = fixture.node.lock().await;
        node.reorg_to(HEIGHT, &hash);
        node.advance();
    }
    let tip_height = fixture.node.lock().await.height;
    ledger
        .reconcile_blocks(
            &[BlockObservation {
                block_hash: hash.clone(),
                active: true,
            }],
            tip_height,
        )
        .await?;
    ensure!(
        fixture.chain_state(&hash).await?.as_deref() == Some("confirmed"),
        "the reactivated block was not confirmed by the reorg reconciler"
    );
    ensure!(
        fixture.credited(&solver).await? == 1,
        "the reactivated block did not credit its deferred share"
    );
    let row = fixture.row(&hash).await?;
    ensure!(
        row["state"] == "orphaned" && row["has_document"] == true,
        "the reactivation reopened or cleared the terminal row: {row}"
    );
    ensure!(
        fixture.arrivals(&hash).await.len() == 1,
        "the reactivation offered the block again"
    );
    eprintln!(
        "#415: a competitor proven at {ORPHAN_CONFIRMATIONS} confirmations settles the row \
         `orphaned` (terminal, evidence kept, out of the pending gauges, counted once); a later \
         reorg back confirms the block and credits its deferred share from the landed audit, \
         without reopening the row or offering the block again."
    );
    Ok(())
}

/// EP-STATE: the settlement is written from an asynchronous observation, so it
/// revalidates the row's fences before it writes. Two observations completing
/// out of order cannot let an older "not active" verdict overwrite a newer
/// "active" one. Mirrors
/// `ledger_postgres.rs::stale_candidate_active_proof_cannot_overwrite_a_newer_reorg`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_orphan_verdict_cannot_overwrite_a_newer_active_proof() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = stale_orphan_verdict(&fixture).await;
    fixture.close().await?;
    result
}

async fn stale_orphan_verdict(fixture: &Fixture) -> Result<()> {
    let ledger = fixture.ledger();
    let (hash, _) = lose_the_race(fixture, Some(deferred_share())).await?;
    let solver = deferred_share().share_id;
    while fixture.node.lock().await.height < HEIGHT + ORPHAN_CONFIRMATIONS - 1 {
        fixture.node.lock().await.advance();
    }
    // The observation an orphan settlement would be written at.
    let stale_revision = ledger.payout_revision().await?;
    fixture.make_due(&hash).await?;
    let claim = ledger
        .claim_candidate(600)
        .await?
        .context("the reconciliation row was not claimable")?;

    // A newer reorg reconciler proves the block active first, and credits it.
    let tip_height = fixture.node.lock().await.height;
    ledger
        .reconcile_blocks(
            &[BlockObservation {
                block_hash: hash.clone(),
                active: true,
            }],
            tip_height,
        )
        .await?;
    ensure!(
        ledger.payout_revision().await? != stale_revision,
        "the reorg reconciler did not advance the payout revision"
    );
    ensure!(fixture.credited(&solver).await? == 1);

    // The older verdict, arriving late, is refused and writes nothing.
    let refused = ledger
        .orphan_candidate_at_revision(&claim, "stale proven-orphan verdict", stale_revision)
        .await
        .expect_err("a stale orphan verdict must not settle a reactivated block");
    ensure!(
        format!("{refused:#}").contains("payout revision changed"),
        "the stale verdict was refused for the wrong reason: {refused:#}"
    );
    ensure!(
        fixture.row(&hash).await?["state"] == "reconciliation",
        "the stale verdict settled the row anyway"
    );
    ensure!(
        fixture.chain_state(&hash).await?.as_deref() == Some("confirmed"),
        "the stale verdict disturbed the newer active proof"
    );

    // Even at the current revision, a confirmed block is never orphaned.
    let current = ledger.payout_revision().await?;
    let refused = ledger
        .orphan_candidate_at_revision(&claim, "active block", current)
        .await
        .expect_err("a confirmed block must not be settled as an orphan");
    ensure!(
        format!("{refused:#}").contains("its block is confirmed"),
        "a confirmed block was refused for the wrong reason: {refused:#}"
    );
    Ok(())
}

/// EP-OBSERVABILITY: unknown stays distinct from zero. An observation that
/// failed settles nothing, and the row keeps its evidence and its place in
/// the pending gauges until the chain answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_observation_never_settles_a_row_as_orphaned() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = failed_observation(&fixture).await;
    fixture.close().await?;
    result
}

async fn failed_observation(fixture: &Fixture) -> Result<()> {
    let ledger = fixture.ledger();
    let (hash, _) = lose_the_race(fixture, Some(deferred_share())).await?;
    while fixture.node.lock().await.height < HEIGHT + ORPHAN_CONFIRMATIONS - 1 {
        fixture.node.lock().await.advance();
    }
    // The node is gone: every observation this retry makes fails.
    fixture.server.abort();
    fixture.make_due(&hash).await?;
    let claim = ledger
        .claim_candidate(120)
        .await?
        .context("the reconciliation row was not claimable")?;
    // The retry keeps the row: a post-offer failure is settled back into
    // reconciliation with its reason, never into a terminal disposition. The
    // settlement itself must not fail, or the claim would simply expire.
    fixture
        .coordinator
        .process_candidate(&claim)
        .await
        .context("a retry whose observations all fail must still settle its row")?;
    let row = fixture.row(&hash).await?;
    ensure!(
        row["state"] == "reconciliation"
            && row["completed_at"].is_null()
            && row["has_document"] == true
            && row["has_block"] == true,
        "a failed observation changed the row: {row}"
    );
    let reason = row["last_error"].as_str().unwrap_or_default().to_owned();
    ensure!(
        reason.contains("post-offer processing failed") && !reason.contains("proven orphan"),
        "a failed observation produced an orphan verdict: {reason:?}"
    );
    let (pending, _) = fixture.gauges().await?;
    ensure!(
        pending == 1,
        "a failed observation took the row out of the pending gauges"
    );
    Ok(())
}
