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
//! The lost race, as the scripted node (`support/scripted_node.rs`) plays it:
//! `submitblock` accepts our block and makes it the node's best tip, and in
//! the same handler, before the null reply is sent, a same-height competitor
//! replaces it; the tip then advances further. The reorg is deterministic: no
//! post-offer observation can see our block active first, however loaded the
//! host. Two differences from the incident are deliberate and stated here:
//! our block's activation is visible only in the node's tip history, never to
//! the coordinator (which in the incident could have observed it for 61 ms),
//! and every tip change on the scripted node adds chainwork, so the
//! competitor has strictly MORE cumulative work than our block. The
//! equal-work replacement of the best tip, which `Ledger::observe_chain_view`
//! refuses (#423), is therefore not exercised.
//!
//! Run through test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test lost_race_pin -- --nocapture.
use anyhow::{ensure, Context, Result};
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
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
#[path = "support/scripted_node.rs"]
#[allow(dead_code)]
mod scripted_node;
use ledger_database::FixtureDatabase;
use scripted_node::{ChainState, ScriptedNode};

/// The incident's race: our block is the best tip, and the node reorgs to the
/// competitor this soon after. #413 measured 61 ms; the scripted node replaces
/// it inside the `submitblock` handler, and the tests assert from the node's
/// tip history that it did so inside this window.
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
/// A second competitor, for a reorg that disconnects our block again.
const SECOND_COMPETITOR: &str = "cc";
/// Bounds every wait on the coordinator's own processing.
const PROCESS_BOUND: Duration = Duration::from_secs(60);

struct Fixture {
    /// The fixture's own database: advisory locks are per database, so the
    /// settlement lock this binary's coordinator takes never queues another
    /// test binary's ledger.
    database: FixtureDatabase,
    /// A plain connection pool on the fixture's database, for the lock hold.
    pool: PgPool,
    coordinator: Arc<Coordinator>,
    metrics: Arc<Metrics>,
    node: ScriptedNode,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let database = FixtureDatabase::open(&raw, "prism_lost_race_").await?;
        match Self::build(&database).await {
            Ok((pool, coordinator, metrics, node)) => Ok(Some(Self {
                database,
                pool,
                coordinator,
                metrics,
                node,
            })),
            Err(error) => Err(database.abandon(error).await),
        }
    }

    async fn build(
        database: &FixtureDatabase,
    ) -> Result<(PgPool, Arc<Coordinator>, Arc<Metrics>, ScriptedNode)> {
        let pool = PgPool::connect(&database.url).await?;
        let node = ScriptedNode::open(ChainState::new(&PARENT.repeat(32), HEIGHT - 1)).await?;
        let config = Config {
            database_url: database.url.clone(),
            instance_id: "lost-race".into(),
            database_connections: 8,
            initialize_schema: true,
            chain: "testnet".into(),
            expected_genesis_hash: None,
            min_peers: 1,
            template_max_age: Duration::from_secs(120),
            submit_tip_max_age: Duration::from_secs(10),
            template_refresh_failure_exit: Duration::from_secs(120),
            rpc_url: node.url.clone(),
            rpc_user: "test".into(),
            rpc_password: "test".into(),
            rpc_timeout: Duration::from_secs(5),
            block_submit_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_secs(1),
            blockwait: false,
            build_workers: 2,
            refresh_build_threads: None,
            runtime_workers: 2,
            snapshot_interval: Duration::from_secs(60),
            health_timeout: Duration::from_secs(15),
            share_commit_timeout: Duration::from_secs(15),
            share_commit_grace: Duration::from_secs(5),
            block_only_ack_timeout: Duration::from_secs(60),
            candidate_orphan_confirmations: ORPHAN_CONFIRMATIONS,
            capture_overpay_ceiling_bps: 100,
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
        Ok((pool, coordinator, metrics, node))
    }

    fn ledger(&self) -> &Ledger {
        &self.coordinator.ledger
    }

    /// The scripted node's chain, locked.
    async fn chain(&self) -> tokio::sync::MutexGuard<'_, ChainState> {
        self.node.state.lock().await
    }

    /// Extend the node's chain until the tip is at `height`.
    async fn advance_to(&self, height: u64) {
        let mut chain = self.chain().await;
        while chain.height < height {
            chain.advance();
        }
    }

    /// The node's own reorg at `HEIGHT`: `hash` replaces the active block
    /// there, and its branch is extended one block past the old tip, so it is
    /// the longer chain as well as the one with more work.
    async fn reorg_at_height(&self, hash: &str) {
        let mut chain = self.chain().await;
        let beyond = chain.height + 1;
        chain.reorg_to(HEIGHT, hash);
        while chain.height < beyond {
            chain.advance();
        }
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

    /// `(inactive_since IS NULL, audit_publication_sequence IS NULL)` of the
    /// block's pool row: whether a disconnection was recorded, and whether
    /// the block has never been confirmed.
    async fn block_markers(&self, block_hash: &str) -> Result<(bool, bool)> {
        Ok(sqlx::query_as(
            "SELECT inactive_since IS NULL, audit_publication_sequence IS NULL FROM qbit_pool_blocks WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&self.pool)
        .await?)
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

    /// Claim the due row for `block_hash` and run one attempt on it.
    async fn retry(&self, block_hash: &str) -> Result<()> {
        self.make_due(block_hash).await?;
        let claim = self
            .ledger()
            .claim_candidate(120)
            .await?
            .context("the row was not claimable for its retry")?;
        ensure!(
            claim.candidate.block_hash == block_hash,
            "another row was claimed for the retry"
        );
        tokio::time::timeout(PROCESS_BOUND, self.coordinator.process_candidate(&claim))
            .await
            .context("the retry did not complete")?
    }

    /// Every `submitblock` arrival for the block.
    async fn arrivals(&self, block_hash: &str) -> Vec<Instant> {
        self.chain()
            .await
            .submissions
            .get(block_hash)
            .cloned()
            .unwrap_or_default()
    }

    /// `qbit_prism_blocks_total`'s source: the coordinator's confirmed-block
    /// count, which the server publishes as the counter.
    fn blocks_total(&self) -> u64 {
        self.coordinator.blocks.load(Ordering::Relaxed)
    }

    /// The cluster-wide pending-candidate gauges, as the metrics collector
    /// observes them.
    async fn gauges(&self) -> Result<(u64, Duration)> {
        let observed = collectors::database(&self.coordinator.ledger.pool, &self.metrics).await?;
        Ok((observed.candidates, observed.candidate_oldest))
    }

    /// Stops the node, closes the pools and drops the fixture database; a
    /// test error wins over a cleanup error.
    async fn close(self, result: Result<()>) -> Result<()> {
        self.node.stop();
        self.coordinator.ledger.pool.close().await;
        self.pool.close().await;
        self.database.close(result).await
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

/// Enqueue our block at `HEIGHT` and claim it.
async fn claim_found(
    fixture: &Fixture,
    deferred: Option<AcceptedShare>,
) -> Result<qbit_prism_server::ledger::CandidateClaim> {
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
    Ok(claim)
}

/// Offer the candidate and lose the race: the node accepts it as its best
/// tip, and the competitor replaces it inside the same `submitblock` handler,
/// before the reply. Returns the block hash and how long the node held our
/// block as its best tip.
async fn lose_the_race(
    fixture: &Fixture,
    deferred: Option<AcceptedShare>,
) -> Result<(String, Duration)> {
    let claim = claim_found(fixture, deferred).await?;
    let hash = claim.candidate.block_hash.clone();
    let competitor = COMPETITOR.repeat(32);
    fixture.chain().await.lose_next_race_to(&competitor);
    tokio::time::timeout(PROCESS_BOUND, fixture.coordinator.process_candidate(&claim))
        .await
        .context("the post-offer settlement did not complete")??;
    let chain = fixture.chain().await;
    ensure!(
        chain.submissions.get(&hash).map(Vec::len) == Some(1),
        "the block did not reach submitblock exactly once"
    );
    // The node's own history: our block was its best tip at HEIGHT, and the
    // competitor replaced it there, briefly after.
    let lifetime = chain
        .tip_lifetime(&hash)
        .context("our block was never the node's best tip, or was never replaced")?;
    ensure!(
        chain.block_at(HEIGHT) == Some(competitor.as_str()),
        "the competitor is not the active block at {HEIGHT}"
    );
    ensure!(
        lifetime <= RACE_WINDOW,
        "the competitor replaced our block {:.0} ms after it became the tip, outside the {} ms the incident measured",
        lifetime.as_secs_f64() * 1e3,
        RACE_WINDOW.as_millis()
    );
    Ok((hash, lifetime))
}

/// Settle our lost-race block as a proven orphan: the competitor reaches the
/// configured depth, and one retry observes it.
async fn prove_orphan(fixture: &Fixture, hash: &str) -> Result<()> {
    fixture.advance_to(HEIGHT + ORPHAN_CONFIRMATIONS - 1).await;
    fixture.retry(hash).await?;
    let row = fixture.row(hash).await?;
    ensure!(
        row["state"] == "orphaned" && !row["completed_at"].is_null(),
        "the proven orphan was not settled terminal: {row}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_tip_race_reconciles_the_orphan_and_never_delays_the_next_tips_work() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = lost_race_pin(&fixture).await;
    fixture.close(result).await
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
            let mut chain = fixture.chain().await;
            chain.advance();
            chain.tip.clone()
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
    tokio::time::timeout(PROCESS_BOUND, fixture.coordinator.process_candidate(&stuck))
        .await
        .context("the retry did not complete")??;
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
         with more chainwork replaced it {:.3} ms later, inside submitblock; the row is in reconciliation with \
         one submitblock. New work for the next two tips was issued {:.0} ms and {:.0} ms after each tip became \
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
    fixture.close(result).await
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
        fixture.advance_to(expected).await;
        fixture.retry(&hash).await?;
        ensure!(
            fixture.row(&hash).await?["state"] == "reconciliation",
            "the row was settled terminal at {} confirmations, below {ORPHAN_CONFIRMATIONS}",
            expected - HEIGHT + 1
        );
    }

    // At the depth, the competitor is proven and the row is settled terminal.
    prove_orphan(fixture, &hash).await?;
    let row = fixture.row(&hash).await?;
    let reason = row["last_error"].as_str().unwrap_or_default().to_owned();
    ensure!(
        reason.contains("proven orphan")
            && reason.contains(&COMPETITOR.repeat(32))
            && reason.contains(&format!("{ORPHAN_CONFIRMATIONS} confirmations")),
        "the orphan reason does not carry the chain's evidence: {reason:?}"
    );
    // The disposition releases the payload the way a submitted or abandoned
    // row does, and keeps the offer record and the reason (#268).
    ensure!(
        row["has_document"] == false
            && row["has_block"] == false
            && [
                "window_anchor_ms",
                "window_prior_balances_sha256",
                "window_first_share_seq",
                "window_last_share_seq",
                "window_share_count",
                "window_snapshot_sha256",
            ]
            .iter()
            .all(|column| row[*column].is_null()),
        "the orphaned row kept its document, block bytes or window reference: {row}"
    );
    ensure!(
        row["offer_outcome"] == "accepted"
            && !row["offer_reserved_at"].is_null()
            && !row["offer_reserved_by"].is_null()
            && !row["offered_at_ms"].is_null(),
        "the orphaned row lost its offer record: {row}"
    );
    ensure!(
        fixture.arrivals(&hash).await.len() == 1,
        "the orphan settlement offered the block again"
    );
    // The block's durable evidence stays: its landed audit, and its pool row,
    // inactive, with no disconnection time because it never connected.
    ensure!(
        fixture.chain_state(&hash).await?.as_deref() == Some("inactive"),
        "the orphaned block is not inactive in the ledger"
    );
    ensure!(
        fixture.block_markers(&hash).await? == (true, true),
        "a never-confirmed orphan recorded a disconnection or a confirmation"
    );
    let audit: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM qbit_pool_audit_bundles WHERE block_hash=$1)",
    )
    .bind(&hash)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(audit, "the orphan settlement removed the landed audit");

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
            .any(|line| line == "qbit_prism_accepted_block_revision_work_pending_seconds 0"),
        "a terminal orphan retained an impossible revision-work wait"
    );
    for result in ["published", "degraded", "superseded"] {
        ensure!(
            rendered.lines().any(|line| line
                == format!(
            "qbit_prism_accepted_block_to_revision_work_seconds_count{{result=\"{result}\"}} 0"
        )),
            "orphan retirement fabricated a successful delivery"
        );
    }
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
    ensure!(
        fixture.blocks_total() == 0,
        "an orphan settlement counted a confirmed block"
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
    // share included, and reports it as the orphan's first confirmation.
    fixture.reorg_at_height(&hash).await;
    let tip_height = fixture.chain().await.height;
    let reactivated = ledger
        .reconcile_blocks(
            &[BlockObservation {
                block_hash: hash.clone(),
                active: true,
            }],
            tip_height,
        )
        .await?;
    ensure!(
        reactivated == 1,
        "the reconciler reported {reactivated} first orphan confirmations, not 1"
    );
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
        row["state"] == "orphaned" && row["offer_outcome"] == "accepted",
        "the reactivation reopened or rewrote the terminal row: {row}"
    );
    ensure!(
        fixture.arrivals(&hash).await.len() == 1,
        "the reactivation offered the block again"
    );
    eprintln!(
        "#415: a competitor proven at {ORPHAN_CONFIRMATIONS} confirmations settles the row \
         `orphaned` (terminal, payload released, offer record kept, out of the pending gauges, \
         counted once); a later reorg back confirms the block and credits its deferred share \
         from the landed audit, without reopening the row or offering the block again."
    );
    Ok(())
}

/// A reactivated orphan joins `qbit_prism_blocks_total` exactly once, through
/// the coordinator's own reconciliation: on its first confirmation, and not
/// again when a later reorg disconnects it and another reconnects it. Its
/// deferred share is credited once across the flap, and the block is never
/// offered again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reactivated_orphan_is_counted_and_credited_once_across_a_flap() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = reactivated_orphan_counted_once(&fixture).await;
    fixture.close(result).await
}

async fn reactivated_orphan_counted_once(fixture: &Fixture) -> Result<()> {
    let (hash, _) = lose_the_race(fixture, Some(deferred_share())).await?;
    let solver = deferred_share().share_id;
    prove_orphan(fixture, &hash).await?;
    ensure!(
        fixture.blocks_total() == 0,
        "the orphan was counted before any confirmation"
    );

    // Reactivated: our block is back at HEIGHT, on a longer chain.
    fixture.reorg_at_height(&hash).await;
    fixture.coordinator.refresh_once().await?;
    ensure!(
        fixture.chain_state(&hash).await?.as_deref() == Some("confirmed"),
        "the coordinator's reconciliation did not confirm the reactivated orphan"
    );
    ensure!(
        fixture.blocks_total() == 1,
        "the orphan's first confirmation counted {} blocks, not 1",
        fixture.blocks_total()
    );
    ensure!(fixture.credited(&solver).await? == 1);
    // A refresh on the same chain changes nothing and counts nothing.
    fixture.chain().await.advance();
    fixture.coordinator.refresh_once().await?;
    ensure!(
        fixture.blocks_total() == 1,
        "a steady tip counted the block again"
    );

    // Disconnected again by a second competitor: a real disconnection time.
    fixture.reorg_at_height(&SECOND_COMPETITOR.repeat(32)).await;
    fixture.coordinator.refresh_once().await?;
    ensure!(
        fixture.chain_state(&hash).await?.as_deref() == Some("inactive"),
        "the second reorg did not disconnect the block"
    );
    ensure!(
        fixture.block_markers(&hash).await? == (false, false),
        "a disconnected, once-confirmed block lost its disconnection time or its confirmation record"
    );
    ensure!(
        fixture.blocks_total() == 1,
        "a disconnection changed the count"
    );

    // Reactivated a second time: confirmed and credited, never counted again.
    fixture.reorg_at_height(&hash).await;
    fixture.coordinator.refresh_once().await?;
    ensure!(
        fixture.chain_state(&hash).await?.as_deref() == Some("confirmed"),
        "the second reactivation did not confirm the block"
    );
    ensure!(
        fixture.blocks_total() == 1,
        "the second reactivation counted the orphan again ({})",
        fixture.blocks_total()
    );
    ensure!(
        fixture.credited(&solver).await? == 1,
        "the flap credited the deferred share more than once"
    );
    ensure!(
        fixture.row(&hash).await?["state"] == "orphaned",
        "the flap reopened the terminal row"
    );
    ensure!(
        fixture.arrivals(&hash).await.len() == 1,
        "the flap offered the block again"
    );
    Ok(())
}

/// The counter's ordinary owner is unchanged: a block whose row finishes as
/// `submitted` is counted there, once, and the reorg reconciler never counts
/// it again when a reorg disconnects and reconnects it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinary_confirmation_is_counted_once_across_a_reorg_and_back() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = ordinary_confirmation_counted_once(&fixture).await;
    fixture.close(result).await
}

async fn ordinary_confirmation_counted_once(fixture: &Fixture) -> Result<()> {
    let claim = claim_found(fixture, Some(deferred_share())).await?;
    let hash = claim.candidate.block_hash.clone();
    let solver = deferred_share().share_id;
    tokio::time::timeout(PROCESS_BOUND, fixture.coordinator.process_candidate(&claim))
        .await
        .context("the offer did not complete")??;
    ensure!(
        fixture.row(&hash).await?["state"] == "submitted",
        "the accepted block did not finish as submitted"
    );
    ensure!(
        fixture.blocks_total() == 1,
        "the submitted block was not counted once"
    );

    // Out and back in, through the coordinator's reconciliation.
    fixture.reorg_at_height(&COMPETITOR.repeat(32)).await;
    fixture.coordinator.refresh_once().await?;
    ensure!(fixture.chain_state(&hash).await?.as_deref() == Some("inactive"));
    fixture.reorg_at_height(&hash).await;
    fixture.coordinator.refresh_once().await?;
    ensure!(fixture.chain_state(&hash).await?.as_deref() == Some("confirmed"));
    ensure!(
        fixture.blocks_total() == 1,
        "the reorg reconciler counted an ordinary block again ({})",
        fixture.blocks_total()
    );
    ensure!(fixture.credited(&solver).await? == 1);
    Ok(())
}

/// Reconciliation can confirm a block before its outbox row finishes. Both
/// later dispositions must preserve that one count, including a disconnect
/// followed by orphan settlement and another reactivation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_confirmation_is_counted_before_outbox_terminalization() -> Result<()> {
    for orphaned in [false, true] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result = async {
            let (hash, _) = lose_the_race(&fixture, Some(deferred_share())).await?;
            fixture.reorg_at_height(&hash).await;
            fixture.coordinator.refresh_once().await?;
            ensure!(fixture.row(&hash).await?["state"] == "reconciliation");
            ensure!(
                fixture.blocks_total() == 1,
                "first confirmation was lost while the outbox was unfinished"
            );
            if orphaned {
                fixture.reorg_at_height(&SECOND_COMPETITOR.repeat(32)).await;
                fixture.coordinator.refresh_once().await?;
                prove_orphan(&fixture, &hash).await?;
                fixture.reorg_at_height(&hash).await;
                fixture.coordinator.refresh_once().await?;
                ensure!(fixture.row(&hash).await?["state"] == "orphaned");
            } else {
                fixture.retry(&hash).await?;
                ensure!(fixture.row(&hash).await?["state"] == "submitted");
            }
            ensure!(
                fixture.blocks_total() == 1,
                "terminalization or reactivation counted the same block twice"
            );
            ensure!(fixture.credited(&deferred_share().share_id).await? == 1);
            ensure!(fixture.arrivals(&hash).await.len() == 1);
            Ok(())
        }
        .await;
        fixture.close(result).await?;
    }
    Ok(())
}

/// Reservation-only uncertainty stays unknown: a row whose call may never
/// have happened settles `orphaned` with the `unknown` outcome and no call
/// time and no reply, releases its payload, and no submission is invented.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reservation_only_orphan_stays_unknown_and_releases_its_payload() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = reservation_only_orphan(&fixture).await;
    fixture.close(result).await
}

async fn reservation_only_orphan(fixture: &Fixture) -> Result<()> {
    let ledger = fixture.ledger();
    let claim = claim_found(fixture, None).await?;
    let hash = claim.candidate.block_hash.clone();
    ledger.reserve_offer(&claim).await?;
    let revision = ledger.payout_revision().await?;
    ledger
        .orphan_candidate_at_revision(&claim, "proven orphan: reservation only", revision)
        .await?;
    let row = fixture.row(&hash).await?;
    ensure!(
        row["state"] == "orphaned"
            && row["offer_outcome"] == "unknown"
            && row["offered_at_ms"].is_null()
            && row["offer_reply"].is_null()
            && !row["offer_reserved_at"].is_null()
            && !row["offer_reserved_by"].is_null(),
        "a reservation-only orphan did not keep an unknown offer with no call: {row}"
    );
    ensure!(
        row["has_document"] == false
            && row["has_block"] == false
            && row["window_anchor_ms"].is_null()
            && row["window_snapshot_sha256"].is_null(),
        "a reservation-only orphan kept its payload: {row}"
    );
    ensure!(
        fixture.arrivals(&hash).await.is_empty(),
        "the node was offered a block whose call was only reserved"
    );
    ensure!(
        fixture.chain_state(&hash).await?.is_none(),
        "a block that never landed gained a pool block"
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
    fixture.close(result).await
}

async fn stale_orphan_verdict(fixture: &Fixture) -> Result<()> {
    let ledger = fixture.ledger();
    let (hash, _) = lose_the_race(fixture, Some(deferred_share())).await?;
    let solver = deferred_share().share_id;
    fixture.advance_to(HEIGHT + ORPHAN_CONFIRMATIONS - 1).await;
    // The observation an orphan settlement would be written at.
    let stale_revision = ledger.payout_revision().await?;
    fixture.make_due(&hash).await?;
    let claim = ledger
        .claim_candidate(600)
        .await?
        .context("the reconciliation row was not claimable")?;

    // A newer reorg reconciler proves the block active first, and credits it.
    // Count this first confirmation even while its outbox row is unfinished.
    let tip_height = fixture.chain().await.height;
    let reactivated = ledger
        .reconcile_blocks(
            &[BlockObservation {
                block_hash: hash.clone(),
                active: true,
            }],
            tip_height,
        )
        .await?;
    ensure!(
        reactivated == 1,
        "the unfinished row's first confirmation was not counted"
    );
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
    let row = fixture.row(&hash).await?;
    ensure!(
        row["state"] == "reconciliation" && row["has_block"] == true,
        "the stale verdict settled or released the row anyway: {row}"
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
    fixture.close(result).await
}

async fn failed_observation(fixture: &Fixture) -> Result<()> {
    let ledger = fixture.ledger();
    let (hash, _) = lose_the_race(fixture, Some(deferred_share())).await?;
    fixture.advance_to(HEIGHT + ORPHAN_CONFIRMATIONS - 1).await;
    // The node is gone: every observation this retry makes fails.
    fixture.node.stop();
    fixture.make_due(&hash).await?;
    let claim = ledger
        .claim_candidate(120)
        .await?
        .context("the reconciliation row was not claimable")?;
    // The retry keeps the row: a post-offer failure is settled back into
    // reconciliation with its reason, never into a terminal disposition. The
    // settlement itself must not fail, or the claim would simply expire.
    tokio::time::timeout(PROCESS_BOUND, fixture.coordinator.process_candidate(&claim))
        .await
        .context("the retry did not complete")?
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
    let rendered = fixture.metrics.render();
    ensure!(
        !rendered
            .lines()
            .any(|line| line == "qbit_prism_block_candidates_orphaned_total 1"),
        "a failed observation counted an orphan settlement"
    );
    Ok(())
}
