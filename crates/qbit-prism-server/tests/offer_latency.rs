//! A266's acceptance measurement for the offer-before-landing order: with
//! every builder permit taken and the landing held for three seconds, a
//! claimed block reaches the node's `submitblock` within 250 ms of its claim,
//! exactly once, and its landing completes only after the hold is released.
//!
//! Several blocks are measured in turn and the distribution of the samples is
//! reported (count, min, p50, p95, p99, max) beside the load and the method,
//! before any bound is asserted, so a failing run still shows what it
//! measured. The absolute numbers are this host's; the property is that the
//! offer never waits behind the builder or the landing.
//!
//! Run through test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test offer_latency -- --nocapture.
use anyhow::{ensure, Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, FoundBlock, PayoutPolicy};
use qbit_prism_server::{
    codec,
    config::Config,
    coordinator::{Coordinator, TipState},
    ledger::{Candidate, Ledger, SignerKeys, Snapshot, WindowRef},
    metrics::Metrics,
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

/// How many blocks are measured, one after another on one chain.
const SAMPLES: usize = 8;
/// The issue's acceptance bound: the offer is at the node within this much
/// of the claim.
const OFFER_BOUND: Duration = Duration::from_millis(250);
/// How long the landing is held after each claim: every builder permit and
/// the settlement lock, which the post-offer observation and the landing
/// transaction both need.
const LANDING_HOLD: Duration = Duration::from_secs(3);
/// The ledger's settlement advisory lock, a cluster-wide constant, taken by
/// `observe_chain_view` and by the landing transaction and never by the offer
/// phase. Held here from a plain connection, as a long `save_job` or another
/// candidate's landing would hold it.
const SETTLEMENT_LOCK: i64 = 0x505249534d000003;
const PARENT: &str = "aa";

struct NodeState {
    tip: String,
    height: u64,
    chainwork: u64,
    /// The active chain by height, for `getblockhash`.
    blocks: HashMap<u64, String>,
    /// When each `submitblock` arrived, by block hash, read before the
    /// request is even decoded: the node-entry boundary of a sample.
    submissions: HashMap<String, Vec<Instant>>,
}

async fn node_reply(
    State(node): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let arrived = Instant::now();
    let mut node = node.lock().await;
    let result = match request["method"].as_str().unwrap_or("") {
        "getblockhash" if request["params"][0] == 0 => json!("00".repeat(32)),
        "getblockhash" => {
            let height = request["params"][0].as_u64().unwrap_or(0);
            json!(node
                .blocks
                .get(&height)
                .cloned()
                .unwrap_or_else(|| node.tip.clone()))
        }
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":node.height,"headers":node.height,"bestblockhash":node.tip,
            "chainwork":format!("{:x}",node.chainwork)}),
        "getbestblockhash" => json!(node.tip),
        "getnetworkinfo" => json!({"connections":2}),
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap_or("")).unwrap();
            let hash = codec::hash_display(&codec::double_sha256(&block[..80]));
            node.submissions
                .entry(hash.clone())
                .or_default()
                .push(arrived);
            // Accepted: the block is the new tip, with more cumulative work.
            let height = node.height + 1;
            node.tip = hash.clone();
            node.height = height;
            node.chainwork += 1;
            node.blocks.insert(height, hash);
            Value::Null
        }
        method => panic!("unexpected offer-latency RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

struct Fixture {
    admin: PgPool,
    /// A plain connection pool on the fixture's schema, for the hold.
    pool: PgPool,
    schema: String,
    coordinator: Arc<Coordinator>,
    node: Arc<Mutex<NodeState>>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_offer_latency_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let pool = PgPool::connect(url.as_str()).await?;
        let node = Arc::new(Mutex::new(NodeState {
            tip: PARENT.repeat(32),
            height: 100,
            chainwork: 1,
            blocks: HashMap::from([(100, PARENT.repeat(32))]),
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
            instance_id: "offer-latency".into(),
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
            rpc_timeout: Duration::from_secs(5),
            block_submit_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_secs(1),
            blockwait: false,
            // One builder permit, which the test holds.
            build_workers: 1,
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
        let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
        coordinator
            .ledger
            .observe_chain_view(&PARENT.repeat(32), 100, "1")
            .await?;
        for index in 1..=3u64 {
            coordinator
                .ledger
                .append(
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
                    },
                    None,
                )
                .await?;
        }
        Ok(Some(Self {
            admin,
            pool,
            schema,
            coordinator,
            node,
            server,
        }))
    }

    fn ledger(&self) -> &Ledger {
        &self.coordinator.ledger
    }

    /// A block found on `snapshot` at `height` on `parent`, with this
    /// frontend's keys, so the post-offer rebuild reproduces its audit. The
    /// candidate carries the snapshot's balances as its as-issued set, as the
    /// share path does.
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
            format!("offer-latency-{height}"),
            &template,
            &bundle.signed_coinbase_manifest.manifest,
            "00000000",
            8,
            1e-12,
            0.0,
            true,
        )?;
        let proof = (nonce_start..nonce_start + 10_000)
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

    /// `(state, audit landed)` of the block.
    async fn outcome(&self, block_hash: &str) -> Result<(String, bool)> {
        Ok(sqlx::query_as(
            "SELECT state,EXISTS(SELECT 1 FROM qbit_pool_audit_bundles WHERE block_hash=$1) FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&self.pool)
        .await?)
    }

    /// The arrival times of every `submitblock` for the block.
    async fn arrivals(&self, block_hash: &str) -> Vec<Instant> {
        self.node
            .lock()
            .await
            .submissions
            .get(block_hash)
            .cloned()
            .unwrap_or_default()
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

fn unix_ms_now() -> Result<i64> {
    let elapsed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    Ok(i64::try_from(elapsed.as_millis())?)
}

/// The nearest-rank percentile of sorted `samples`.
fn percentile(samples: &[Duration], percent: usize) -> Duration {
    let rank = (samples.len() * percent).div_ceil(100).max(1);
    samples[rank - 1]
}

/// One measured block: claim to node entry, with the hold released only
/// after `LANDING_HOLD`, and the landing proven to complete afterwards.
struct Sample {
    claim_to_node: Duration,
    /// How long after the claim the hold was released.
    released_after: Duration,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offer_reaches_the_node_within_the_bound_while_builders_and_landing_are_held() -> Result<()>
{
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = measure(&fixture).await;
    fixture.close().await?;
    result
}

async fn measure(fixture: &Fixture) -> Result<()> {
    let ledger = fixture.ledger();
    let mut samples = Vec::with_capacity(SAMPLES);
    let mut tip = PARENT.repeat(32);
    for index in 0..SAMPLES {
        let height = 101 + index as u64;
        // The block is found on the window and balances as they are now, on
        // the tip as it is now: what a job issued this moment would carry.
        let snapshot = ledger.snapshot(100).await?;
        let candidate = fixture.found(&snapshot, height, &tip, 10_000 * index as u32)?;
        let hash = candidate.block_hash.clone();
        *fixture.coordinator.observed_tip.write().await = TipState::baseline(tip.clone());
        ensure!(
            ledger
                .enqueue_candidate_observed(candidate, Some(unix_ms_now()?))
                .await?,
            "sample {index}: the candidate was not enqueued"
        );

        // The load: every builder permit is taken, and the settlement lock
        // is held by a plain connection, so neither the rebuild nor the
        // post-offer observation nor the landing can run until released.
        let permit = fixture
            .coordinator
            .build_slots
            .clone()
            .acquire_owned()
            .await?;
        ensure!(
            fixture.coordinator.build_slots.available_permits() == 0,
            "the builders are not saturated"
        );
        let mut hold = fixture.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(SETTLEMENT_LOCK)
            .execute(&mut *hold)
            .await?;

        // The claim, and the attempt that offers it.
        let claimed_at = Instant::now();
        let claim = ledger
            .claim_candidate(120)
            .await?
            .context("the candidate was not claimable")?;
        ensure!(
            claim.candidate.block_hash == hash,
            "sample {index}: another row was claimed"
        );
        let process = {
            let coordinator = fixture.coordinator.clone();
            tokio::spawn(async move { coordinator.process_candidate(&claim).await })
        };

        // The node-entry boundary: the first `submitblock` for this hash.
        let arrival = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(first) = fixture.arrivals(&hash).await.first().copied() {
                    return first;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .with_context(|| format!("sample {index}: the block never reached submitblock"))?;
        let claim_to_node = arrival.saturating_duration_since(claimed_at);

        // The hold runs its course from the claim; the landing cannot have
        // happened under it.
        tokio::time::sleep_until((claimed_at + LANDING_HOLD).into()).await;
        let (state, landed) = fixture.outcome(&hash).await?;
        ensure!(
            !landed && !process.is_finished(),
            "sample {index}: the landing ran while the builders and the settlement lock were held ({state})"
        );
        ensure!(
            state == "offered",
            "sample {index}: the block is {state} under the hold, not offered"
        );
        drop(permit);
        hold.rollback().await?;
        let released_after = claimed_at.elapsed();

        // Released: the same attempt lands and confirms, with no second offer.
        tokio::time::timeout(Duration::from_secs(30), process)
            .await
            .with_context(|| format!("sample {index}: the landing did not complete"))???;
        let (state, landed) = fixture.outcome(&hash).await?;
        ensure!(
            state == "submitted" && landed,
            "sample {index}: the block finished as {state} (landed: {landed})"
        );
        let arrivals = fixture.arrivals(&hash).await;
        ensure!(
            arrivals.len() == 1,
            "sample {index}: submitblock was called {} times for one block",
            arrivals.len()
        );
        samples.push(Sample {
            claim_to_node,
            released_after,
        });
        tip = hash;
    }

    // The distribution, reported before any bound is asserted.
    let mut sorted: Vec<Duration> = samples.iter().map(|s| s.claim_to_node).collect();
    sorted.sort();
    let count = sorted.len();
    let (min, p50, p95, p99, max) = (
        sorted[0],
        percentile(&sorted, 50),
        percentile(&sorted, 95),
        percentile(&sorted, 99),
        sorted[count - 1],
    );
    eprintln!(
        "offer latency: claim to submitblock arrival at the node, {count} blocks in sequence, \
         each with the builder permit (1 of 1) taken and SETTLEMENT_LOCK held for {} s from the claim; \
         min {:.1} ms, p50 {:.1} ms, p95 {:.1} ms, p99 {:.1} ms, max {:.1} ms; \
         hold released after {:.0}..{:.0} ms; one submitblock per block; every landing completed after its release",
        LANDING_HOLD.as_secs(),
        min.as_secs_f64() * 1e3,
        p50.as_secs_f64() * 1e3,
        p95.as_secs_f64() * 1e3,
        p99.as_secs_f64() * 1e3,
        max.as_secs_f64() * 1e3,
        samples.iter().map(|s| s.released_after).min().unwrap_or_default().as_secs_f64() * 1e3,
        samples.iter().map(|s| s.released_after).max().unwrap_or_default().as_secs_f64() * 1e3,
    );
    for (index, sample) in samples.iter().enumerate() {
        eprintln!(
            "offer latency sample {index}: {:.1} ms claim to node, hold released after {:.0} ms",
            sample.claim_to_node.as_secs_f64() * 1e3,
            sample.released_after.as_secs_f64() * 1e3
        );
    }
    ensure!(count == SAMPLES, "{count} samples, not {SAMPLES}");
    ensure!(
        max <= OFFER_BOUND,
        "the slowest offer took {:.1} ms, over the {} ms bound",
        max.as_secs_f64() * 1e3,
        OFFER_BOUND.as_millis()
    );
    ensure!(
        samples.iter().all(|s| s.released_after >= LANDING_HOLD),
        "a hold was released early"
    );
    Ok(())
}
