//! #458's acceptance measurement: from the durable time a pool block was
//! offered to the node that accepted it, to this frontend's first publication
//! of work whose payout revision carries its landing.
//!
//! The fixture is the one `tests/offer_latency.rs` and `tests/lost_race_pin.rs`
//! use: a real `Coordinator` on its own PostgreSQL database
//! (`support/ledger_database.rs`) against the scripted node
//! (`support/scripted_node.rs`), which accepts `submitblock` and moves its
//! chain. Every block here is found, offered, accepted and landed through the
//! ordinary coordinator paths, so the measured sample starts at the
//! `offered_at_ms` the offering frontend committed immediately before its one
//! `submitblock` call, never at the wall clock of the observation.
//!
//! Each test prints the samples it produced before asserting anything, so a
//! failing run still shows what it measured and #291 has the numbers.
//!
//! Run through test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test accepted_publication_latency -- --nocapture.
use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, FoundBlock, PayoutPolicy};
use qbit_prism_server::{
    codec,
    config::Config,
    coordinator::{Coordinator, TipState},
    ledger::{Candidate, CandidateClaim, Ledger, OfferOutcome, SignerKeys, Snapshot, WindowRef},
    metrics::{Metrics, PendingAge},
    stratum::MiningBackend,
};
use qbit_prism_test_gate as gate;
use serde_json::json;
use sqlx::PgPool;
use std::{
    sync::Arc,
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

/// The chain the fixture starts on.
const PARENT: &str = "aa";
const TIP_HEIGHT: u64 = 100;
/// Bounds one candidate attempt and one detached observation.
const PROCESS_BOUND: Duration = Duration::from_secs(30);
const SAMPLE_PREFIX: &str = "qbit_prism_accepted_block_work_publication_seconds";
const PENDING_GAUGE: &str = "qbit_prism_accepted_block_oldest_unpublished_seconds";
const REFUSALS: &str = "qbit_prism_stale_payout_revision_job_refusals_total";

struct Fixture {
    database: FixtureDatabase,
    /// A plain pool on the fixture's database, for row assertions.
    pool: PgPool,
    coordinator: Arc<Coordinator>,
    metrics: Arc<Metrics>,
    node: ScriptedNode,
    /// One paired reading of both clocks, so the node's monotonic arrivals
    /// can be compared with the wall-clock `offered_at_ms` the ledger holds.
    base: (Instant, i64),
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let database = FixtureDatabase::open(&raw, "prism_accepted_publication_").await?;
        match Self::build(&database).await {
            Ok((pool, coordinator, metrics, node)) => Ok(Some(Self {
                database,
                pool,
                coordinator,
                metrics,
                node,
                base: (Instant::now(), unix_ms_now()?),
            })),
            Err(error) => Err(database.abandon(error).await),
        }
    }

    async fn build(
        database: &FixtureDatabase,
    ) -> Result<(PgPool, Arc<Coordinator>, Arc<Metrics>, ScriptedNode)> {
        let pool = PgPool::connect(&database.url).await?;
        let node = ScriptedNode::open(ChainState::new(&PARENT.repeat(32), TIP_HEIGHT)).await?;
        let config = Config {
            database_url: database.url.clone(),
            instance_id: "accepted-publication".into(),
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
            .observe_chain_view(&PARENT.repeat(32), TIP_HEIGHT, "1")
            .await?;
        for index in 1..=3u64 {
            coordinator.ledger.append(seed_share(index), None).await?;
        }
        Ok((pool, coordinator, metrics, node))
    }

    fn ledger(&self) -> &Ledger {
        &self.coordinator.ledger
    }

    /// A block found on `snapshot` at `height` on `parent`, with this
    /// frontend's keys, so the post-offer rebuild reproduces its audit.
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
            format!("accepted-publication-{height}"),
            &template,
            &bundle.signed_coinbase_manifest.manifest,
            "00000000",
            8,
            1e-12,
            0.0,
            true,
        )?;
        let proof = (nonce_start..nonce_start + 20_000)
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

    /// Enqueue a block found on the current window at `height` on `parent`,
    /// and claim it.
    async fn claim_found(&self, height: u64, parent: &str) -> Result<CandidateClaim> {
        let snapshot = self.ledger().snapshot(u128::from(height - 1)).await?;
        let candidate = self.found(&snapshot, height, parent, 10_000 * height as u32)?;
        let hash = candidate.block_hash.clone();
        *self.coordinator.observed_tip.write().await = TipState::baseline(parent.to_owned());
        ensure!(
            self.ledger()
                .enqueue_candidate_observed(candidate, Some(unix_ms_now()?))
                .await?,
            "the candidate was not enqueued"
        );
        let claim = self
            .ledger()
            .claim_candidate(120)
            .await?
            .context("the candidate was not claimable")?;
        ensure!(
            claim.candidate.block_hash == hash,
            "another row was claimed"
        );
        Ok(claim)
    }

    /// Offer and land one block at `height` on `parent`: the ordinary
    /// coordinator path, which ends with the row `submitted` and the block's
    /// payouts in a new cluster payout revision.
    async fn land(&self, height: u64, parent: &str) -> Result<String> {
        let claim = self.claim_found(height, parent).await?;
        let hash = claim.candidate.block_hash.clone();
        tokio::time::timeout(PROCESS_BOUND, self.coordinator.process_candidate(&claim))
            .await
            .context("the offer did not complete")??;
        ensure!(
            self.state(&hash).await? == "submitted",
            "the accepted block did not finish as submitted"
        );
        Ok(hash)
    }

    /// One refresh that must publish, with its detached publication
    /// observation awaited: the wall-clock window the publication happened in
    /// is returned so a sample can be checked against it.
    async fn refresh(&self) -> Result<(i64, i64)> {
        let published = self.coordinator.refresh.subscribe();
        let before = self.coordinator.accepted_publication_observations();
        let started_ms = unix_ms_now()?;
        self.coordinator.refresh_once().await?;
        let finished_ms = unix_ms_now()?;
        ensure!(
            published.has_changed()?,
            "the refresh reused its work and published nothing"
        );
        tokio::time::timeout(PROCESS_BOUND, async {
            while self.coordinator.accepted_publication_observations() <= before {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("the publication observation never finished")?;
        Ok((started_ms, finished_ms))
    }

    /// Extend the node's chain by one block of no interest to the pool, so
    /// the next refresh publishes new work.
    async fn advance(&self) {
        self.node.state.lock().await.advance();
    }

    async fn state(&self, block_hash: &str) -> Result<String> {
        Ok(
            sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(block_hash)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn offered_at_ms(&self, block_hash: &str) -> Result<i64> {
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT offered_at_ms FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&self.pool)
        .await?
        .context("the row records no offer time")
    }

    /// The first `submitblock` arrival for the block, on this process's wall
    /// clock through the fixture's paired clock reading.
    async fn arrival_ms(&self, block_hash: &str) -> Result<i64> {
        let arrival = self
            .node
            .state
            .lock()
            .await
            .submissions
            .get(block_hash)
            .and_then(|arrivals| arrivals.first().copied())
            .context("the block never reached submitblock")?;
        let (instant, wall_ms) = self.base;
        Ok(wall_ms + i64::try_from(arrival.saturating_duration_since(instant).as_millis())?)
    }

    /// `(count, sum seconds)` of one publication result.
    fn samples(&self, result: &str) -> (f64, f64) {
        let body = self.metrics.render();
        (
            sample(
                &body,
                &format!("{SAMPLE_PREFIX}_count{{result=\"{result}\"}}"),
            )
            .unwrap_or(0.),
            sample(
                &body,
                &format!("{SAMPLE_PREFIX}_sum{{result=\"{result}\"}}"),
            )
            .unwrap_or(0.),
        )
    }

    fn refusals(&self) -> f64 {
        sample(&self.metrics.render(), REFUSALS).unwrap_or(0.)
    }

    /// One health tick's gauge derivation, with the rendered gauge proven to
    /// carry the same value the derivation returned.
    async fn pending_age(&self) -> Result<PendingAge> {
        pending_age_of(&self.coordinator, &self.metrics).await
    }

    /// A second frontend process on the same database and node: a restart
    /// of this one, with its own metrics and in-memory state.
    async fn restarted(&self) -> Result<(Arc<Coordinator>, Arc<Metrics>)> {
        let mut config = (*self.coordinator.config).clone();
        config.instance_id = "accepted-publication-restarted".into();
        config.initialize_schema = false;
        let metrics = Arc::new(Metrics::default());
        let coordinator = Coordinator::new(config, metrics.clone()).await?;
        Ok((coordinator, metrics))
    }

    async fn close(self, result: Result<()>) -> Result<()> {
        self.node.stop();
        self.coordinator.ledger.pool.close().await;
        self.pool.close().await;
        self.database.close(result).await
    }
}

fn seed_share(index: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("miner:{index:064x}"),
        miner_id: format!("miner-{}", index % 2),
        order_key: format!("miner-{}", index % 2),
        p2mr_program_hex: format!("{:02x}", 0x10 + index).repeat(32),
        share_difficulty: 100,
        network_difficulty: 100,
        template_height: TIP_HEIGHT,
        job_id: "seed".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

fn unix_ms_now() -> Result<i64> {
    let elapsed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    Ok(i64::try_from(elapsed.as_millis())?)
}

fn sample(body: &str, key: &str) -> Option<f64> {
    let prefix = format!("{key} ");
    body.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.parse().ok())
}

/// One health tick's gauge derivation on `coordinator`, with the rendered
/// gauge proven to carry the same value the derivation returned.
async fn pending_age_of(coordinator: &Coordinator, metrics: &Metrics) -> Result<PendingAge> {
    let age = coordinator.publish_accepted_pending_age().await;
    let rendered = sample(&metrics.render(), PENDING_GAUGE)
        .context("the pending-age gauge is not rendered")?;
    let expected = match age {
        PendingAge::Unknown => -1.,
        PendingAge::None => 0.,
        PendingAge::Oldest(age) => age.as_secs_f64(),
    };
    ensure!(
        rendered == expected,
        "the gauge renders {rendered}, not the derived {expected}"
    );
    Ok(age)
}

/// The statement every settlement write ends with, made from the test's own
/// pool as another frontend's would be: the cluster revision moves past this
/// frontend's published work with nothing else changed.
async fn bump_revision(pool: &PgPool) -> Result<()> {
    let bumped = sqlx::query(
        "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton",
    )
    .execute(pool)
    .await?
    .rows_affected();
    ensure!(bumped == 1, "the cluster row is missing");
    Ok(())
}

fn age_seconds(age: PendingAge) -> Result<f64> {
    match age {
        PendingAge::Oldest(age) => Ok(age.as_secs_f64()),
        other => Err(anyhow::anyhow!("the gauge is {other:?}, not an age")),
    }
}

/// T1. One accepted, landed block produces exactly one `published` sample at
/// the publication that first carries its revision, measured from the durable
/// offer time; a second publication does not sample it again, and the pending
/// gauge returns to a real zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_accepted_landing_is_sampled_once_from_its_durable_offer_time() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = published_sample(&fixture).await;
    fixture.close(result).await
}

async fn published_sample(fixture: &Fixture) -> Result<()> {
    // The first publication has nothing to measure.
    fixture.refresh().await?;
    ensure!(
        fixture.samples("published") == (0., 0.),
        "an empty ledger produced a sample"
    );
    ensure!(fixture.pending_age().await? == PendingAge::None);

    let hash = fixture.land(TIP_HEIGHT + 1, &PARENT.repeat(32)).await?;
    let offered_at_ms = fixture.offered_at_ms(&hash).await?;
    let arrival_ms = fixture.arrival_ms(&hash).await?;
    // The landing is durable and unpublished: the block is pending, not zero.
    let pending = age_seconds(fixture.pending_age().await?)?;

    let (started_ms, finished_ms) = fixture.refresh().await?;
    let (count, sum) = fixture.samples("published");
    eprintln!(
        "accepted publication: one landed block, offered_at_ms {offered_at_ms}, \
         submitblock arrival {arrival_ms} ms, publication window {started_ms}..{finished_ms} ms, \
         pending age before the publication {pending:.3} s; \
         published count {count}, sum {sum:.3} s; superseded {:?}",
        fixture.samples("superseded")
    );
    ensure!(
        count == 1. && fixture.samples("superseded").0 == 0.,
        "the landing produced {count} published and {} superseded samples",
        fixture.samples("superseded").0
    );
    let sample_ms = sum * 1e3;
    ensure!(
        sample_ms >= (started_ms - offered_at_ms) as f64
            && sample_ms <= (finished_ms - offered_at_ms) as f64,
        "the sample is {sample_ms:.1} ms, outside the publication window \
         {}..{} ms measured from the durable offer time",
        started_ms - offered_at_ms,
        finished_ms - offered_at_ms
    );
    ensure!(
        offered_at_ms <= arrival_ms + 5,
        "the durable offer time {offered_at_ms} is after the node's receipt {arrival_ms}"
    );
    ensure!(
        pending > 0. && pending * 1e3 <= sample_ms,
        "the pending age {pending:.3} s did not precede the sample"
    );

    // Idempotent: the next publication finds nothing new, and the gauge is a
    // successful zero.
    fixture.advance().await;
    fixture.refresh().await?;
    ensure!(
        fixture.samples("published") == (count, sum),
        "the block was sampled again by a later publication"
    );
    ensure!(fixture.pending_age().await? == PendingAge::None);
    Ok(())
}

/// M1's landed half: an accepted row whose landing has not happened yet is
/// pending and must produce no sample, whatever is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_accepted_row_that_has_not_landed_yields_no_sample() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = unlanded_row(&fixture).await;
    fixture.close(result).await
}

async fn unlanded_row(fixture: &Fixture) -> Result<()> {
    fixture.refresh().await?;
    ensure!(
        fixture.pending_age().await? == PendingAge::None,
        "the seeding tick is not zero"
    );
    // The durable state a frontend leaves between its accepted `submitblock`
    // call and the landing: `offered`, with the node's acceptance recorded.
    let claim = fixture
        .claim_found(TIP_HEIGHT + 1, &PARENT.repeat(32))
        .await?;
    let hash = claim.candidate.block_hash.clone();
    let offered_at_ms = unix_ms_now()?;
    fixture.ledger().reserve_offer(&claim).await?;
    fixture
        .ledger()
        .record_offer(&claim, offered_at_ms, OfferOutcome::Accepted, None)
        .await?;
    ensure!(fixture.state(&hash).await? == "offered");

    let revision = fixture.ledger().payout_revision().await?;
    fixture
        .coordinator
        .observe_accepted_publication(revision, unix_ms_now()?)
        .await?;
    ensure!(
        fixture.samples("published") == (0., 0.) && fixture.samples("superseded") == (0., 0.),
        "an unlanded row was sampled"
    );
    // Behind the cluster, so an uncovered landing would count: an accepted
    // row that has not landed still does not. The candidate gauges own it.
    bump_revision(&fixture.pool).await?;
    ensure!(
        fixture.pending_age().await? == PendingAge::None,
        "an accepted row that has not landed counted as pending"
    );
    eprintln!("accepted publication: unlanded row not pending, no sample");
    Ok(())
}

/// P3-1: a lost tip race. The node accepted the block (a null reply) and a
/// same-height competitor replaced it, so the row sits in reconciliation with
/// the accepted outcome until an orphan proof: it never lands, is never
/// sampled and is never pending here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_race_is_never_pending_and_never_sampled() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = lost_race(&fixture).await;
    fixture.close(result).await
}

async fn lost_race(fixture: &Fixture) -> Result<()> {
    fixture.refresh().await?;
    ensure!(fixture.pending_age().await? == PendingAge::None);
    let claim = fixture
        .claim_found(TIP_HEIGHT + 1, &PARENT.repeat(32))
        .await?;
    let hash = claim.candidate.block_hash.clone();
    fixture
        .node
        .state
        .lock()
        .await
        .lose_next_race_to(&"bb".repeat(32));
    tokio::time::timeout(PROCESS_BOUND, fixture.coordinator.process_candidate(&claim))
        .await
        .context("the post-offer settlement did not complete")??;
    let (state, outcome): (String, Option<String>) = sqlx::query_as(
        "SELECT state,offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(&hash)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        state == "reconciliation" && outcome.as_deref() == Some("accepted"),
        "the lost race left the row {state} with outcome {outcome:?}"
    );
    // The frontend is behind the cluster, and publishes again: neither makes
    // a block that never landed pending or measurable.
    bump_revision(&fixture.pool).await?;
    ensure!(
        fixture.pending_age().await? == PendingAge::None,
        "a lost race counted as an unpublished landing"
    );
    fixture.refresh().await?;
    ensure!(
        fixture.samples("published") == (0., 0.) && fixture.samples("superseded") == (0., 0.),
        "a lost race was sampled"
    );
    ensure!(fixture.pending_age().await? == PendingAge::None);
    Ok(())
}

/// T2. A row adopted through `adopt_active_candidate` carries no offer time
/// and the `unknown` outcome: it is never sampled and never pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_adopted_row_is_never_sampled_and_never_pending() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = adopted_row(&fixture).await;
    fixture.close(result).await
}

async fn adopted_row(fixture: &Fixture) -> Result<()> {
    fixture.refresh().await?;
    let claim = fixture
        .claim_found(TIP_HEIGHT + 1, &PARENT.repeat(32))
        .await?;
    let hash = claim.candidate.block_hash.clone();
    fixture
        .ledger()
        .adopt_active_candidate(
            &claim,
            "node: block active at height 101",
            "already on the active chain before any offer",
        )
        .await?;
    ensure!(fixture.state(&hash).await? == "reconciliation");
    let outcome: Option<String> = sqlx::query_scalar(
        "SELECT offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(&hash)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(outcome.as_deref() == Some("unknown"), "{outcome:?}");

    let revision = fixture.ledger().payout_revision().await?;
    fixture
        .coordinator
        .observe_accepted_publication(revision, unix_ms_now()?)
        .await?;
    ensure!(
        fixture.samples("published") == (0., 0.) && fixture.samples("superseded") == (0., 0.),
        "an adopted row was sampled"
    );
    ensure!(
        fixture.pending_age().await? == PendingAge::None,
        "an adopted row counted as pending"
    );
    Ok(())
}

/// T3. While a landing stays unpublished, across a failed refresh too, the
/// gauge rises; a second landing never resets it; a failed derivation is
/// unknown, never zero. Its return to zero at the publication is T1's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pending_age_rises_until_the_landing_is_published_and_is_unknown_when_the_read_fails(
) -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = pending_age_rises(&fixture).await;
    fixture.close(result).await
}

async fn pending_age_rises(fixture: &Fixture) -> Result<()> {
    fixture.refresh().await?;
    ensure!(
        fixture.pending_age().await? == PendingAge::None,
        "the seeding tick is not zero"
    );
    let hash = fixture.land(TIP_HEIGHT + 1, &PARENT.repeat(32)).await?;
    let first = age_seconds(fixture.pending_age().await?)?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second = age_seconds(fixture.pending_age().await?)?;
    ensure!(
        second > first,
        "the gauge did not rise across two ticks ({first:.3} s then {second:.3} s)"
    );

    // A second, newer landing must not reset the oldest pending age.
    fixture.land(TIP_HEIGHT + 2, &hash).await?;
    let third = age_seconds(fixture.pending_age().await?)?;
    ensure!(
        third >= second,
        "a second landing reset the gauge to {third:.3} s from {second:.3} s"
    );

    // Refresh fails (the node is gone): nothing is published, so nothing is
    // sampled, and the landings stay pending.
    fixture.node.stop();
    let observations = fixture.coordinator.accepted_publication_observations();
    ensure!(
        fixture.coordinator.refresh_once().await.is_err(),
        "a refresh without a node succeeded"
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    ensure!(
        fixture.coordinator.accepted_publication_observations() == observations
            && fixture.samples("published") == (0., 0.),
        "a failed refresh observed a publication"
    );
    let failed = age_seconds(fixture.pending_age().await?)?;
    ensure!(
        failed > third,
        "the gauge did not keep rising across a failed refresh ({failed:.3} s)"
    );
    eprintln!(
        "accepted publication: pending age {first:.3} s, {second:.3} s, \
         {third:.3} s after a second landing, {failed:.3} s after a failed refresh"
    );

    // The read fails: unknown, never a healthy zero.
    fixture.coordinator.ledger.pool.close().await;
    ensure!(
        fixture.pending_age().await? == PendingAge::Unknown,
        "a failed derivation did not read as unknown"
    );
    Ok(())
}

/// T4. Two landings before one publication: the newest is `published`, the
/// older one `superseded`, and no job build was refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_landings_before_one_publication_are_published_and_superseded() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = superseded_sample(&fixture).await;
    fixture.close(result).await
}

async fn superseded_sample(fixture: &Fixture) -> Result<()> {
    fixture.refresh().await?;
    let first = fixture.land(TIP_HEIGHT + 1, &PARENT.repeat(32)).await?;
    let second = fixture.land(TIP_HEIGHT + 2, &first).await?;
    let offered = (
        fixture.offered_at_ms(&first).await?,
        fixture.offered_at_ms(&second).await?,
    );
    let refusals = fixture.refusals();

    let (started_ms, finished_ms) = fixture.refresh().await?;
    let published = fixture.samples("published");
    let superseded = fixture.samples("superseded");
    eprintln!(
        "accepted publication: two landings, offered_at_ms {} and {}, publication window \
         {started_ms}..{finished_ms} ms; published count {} sum {:.3} s; superseded count {} sum {:.3} s",
        offered.0, offered.1, published.0, published.1, superseded.0, superseded.1
    );
    ensure!(
        published.0 == 1. && superseded.0 == 1.,
        "two landings produced {} published and {} superseded samples",
        published.0,
        superseded.0
    );
    // The older landing is the superseded one: its offer is the earlier, so
    // its sample is the larger.
    ensure!(
        superseded.1 > published.1,
        "the superseded sample {:.3} s is not the older landing's",
        superseded.1
    );
    for (result, sum) in [("published", published.1), ("superseded", superseded.1)] {
        let offered_at_ms = if result == "published" {
            offered.1
        } else {
            offered.0
        };
        let sample_ms = sum * 1e3;
        ensure!(
            sample_ms >= (started_ms - offered_at_ms) as f64
                && sample_ms <= (finished_ms - offered_at_ms) as f64,
            "the {result} sample {sample_ms:.1} ms is outside its publication window"
        );
    }
    ensure!(
        fixture.refusals() == refusals,
        "a job build was refused during the landings"
    );
    ensure!(fixture.pending_age().await? == PendingAge::None);
    Ok(())
}

/// T5. The stale-revision counter follows the `build_job` refusal exactly: a
/// successful build never increments it, and a build refused because the
/// published payout snapshot is behind the cluster's increments it once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_payout_revision_refusal_is_counted_once_and_a_build_is_not() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = stale_revision_refusals(&fixture).await;
    fixture.close(result).await
}

async fn stale_revision_refusals(fixture: &Fixture) -> Result<()> {
    fixture.refresh().await?;
    let worker = fixture.coordinator.authorize("solver.worker").await?;
    let job = fixture
        .coordinator
        .build_job(&worker, "00000000", 1e-12, 0.0)
        .await?;
    ensure!(
        job.wire.previousblockhash == PARENT.repeat(32),
        "the issued job is not for the published tip"
    );
    ensure!(
        fixture.refusals() == 0.,
        "a successful build counted a refusal"
    );

    // The cluster's payout revision moves past the published work's with the
    // tip unchanged: the statement every settlement write ends with, made
    // here as another frontend's would be. (A landing that also moves the tip
    // is issued under the replacement lease at the current revision instead,
    // which refuses nothing and so counts nothing.)
    let bumped = sqlx::query(
        "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton",
    )
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    ensure!(bumped == 1, "the cluster row is missing");
    for expected in [1., 2.] {
        let refused = fixture
            .coordinator
            .build_job(&worker, "00000000", 1e-12, 0.0)
            .await;
        ensure!(refused.is_err(), "the stale build was not refused");
        ensure!(
            fixture.refusals() == expected,
            "the refusal counter reads {}, not {expected}",
            fixture.refusals()
        );
    }

    // The refresh that republishes at the current revision builds again, and
    // counts nothing.
    fixture.refresh().await?;
    fixture
        .coordinator
        .build_job(&worker, "00000000", 1e-12, 0.0)
        .await?;
    ensure!(
        fixture.refusals() == 2.,
        "a successful build after the refresh counted a refusal"
    );
    eprintln!("accepted publication: two stale-revision refusals counted, no build counted");
    Ok(())
}

/// T6 (EP-STATE). An observation whose publication has already been
/// superseded records nothing; the publication that catches up records it,
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_superseded_publication_records_nothing_until_the_next_one() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = superseded_revalidation(&fixture).await;
    fixture.close(result).await
}

async fn superseded_revalidation(fixture: &Fixture) -> Result<()> {
    fixture.refresh().await?;
    ensure!(
        fixture.pending_age().await? == PendingAge::None,
        "the seeding tick is not zero"
    );
    let published_revision = fixture.ledger().payout_revision().await?;
    let hash = fixture.land(TIP_HEIGHT + 1, &PARENT.repeat(32)).await?;
    let landed_revision = fixture.ledger().payout_revision().await?;
    ensure!(
        landed_revision > published_revision,
        "the landing did not move the cluster revision"
    );

    // The publication that authorized this observation is behind the cluster:
    // the landing is not in the work it published, so nothing is recorded.
    fixture
        .coordinator
        .observe_accepted_publication(published_revision, unix_ms_now()?)
        .await?;
    ensure!(
        fixture.samples("published") == (0., 0.),
        "a superseded publication recorded a sample"
    );
    ensure!(age_seconds(fixture.pending_age().await?)? > 0.);

    // The publication that catches up records it once.
    fixture.refresh().await?;
    let (count, sum) = fixture.samples("published");
    eprintln!(
        "accepted publication: revalidation dropped one observation at revision \
         {published_revision} (cluster {landed_revision}); the catching-up publication \
         of {hash} recorded count {count}, sum {sum:.3} s"
    );
    ensure!(count == 1., "the catching-up publication recorded {count}");
    fixture.advance().await;
    fixture.refresh().await?;
    ensure!(
        fixture.samples("published") == (count, sum),
        "the block was sampled twice"
    );
    Ok(())
}

/// F1b: a restarted frontend cannot attribute history it did not see, so
/// until it has published the cluster's revision once its gauge is unknown
/// (-1), which the readiness and coverage alerts own. Once it publishes, the
/// gauge is a real zero; a landing after that which it does not publish is
/// the hold it reports, rising, not reset by a second landing, and back to
/// zero at the publication that carries both. A block offered before it
/// started is never sampled by it; one offered after is, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_frontend_is_unknown_until_it_publishes_then_reports_new_holds() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = restart_semantics(&fixture).await;
    fixture.close(result).await
}

async fn restart_semantics(fixture: &Fixture) -> Result<()> {
    fixture.refresh().await?;
    let before_restart = fixture.land(TIP_HEIGHT + 1, &PARENT.repeat(32)).await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let (restarted, metrics) = fixture.restarted().await?;
    let result = async {
        // Nothing published yet: unknown, not the age of a landing it cannot
        // attribute.
        ensure!(
            pending_age_of(&restarted, &metrics).await? == PendingAge::Unknown,
            "a restarted frontend that has not published reports an age"
        );

        // It publishes the cluster's revision: a real zero, and no sample for
        // a block offered before it started.
        publish(&restarted).await?;
        ensure!(
            pending_age_of(&restarted, &metrics).await? == PendingAge::None,
            "the seeding tick after the restart is not zero"
        );
        ensure!(
            restarted_samples(&metrics) == (0., 0.),
            "the restarted process sampled a block offered before it started"
        );

        // A landing it does not publish is the hold, and it rises.
        let second = fixture.land(TIP_HEIGHT + 2, &before_restart).await?;
        let first = age_seconds(pending_age_of(&restarted, &metrics).await?)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let rising = age_seconds(pending_age_of(&restarted, &metrics).await?)?;
        ensure!(
            rising > first,
            "the hold did not rise ({first:.3} s then {rising:.3} s)"
        );
        // A second landing does not reset it.
        fixture.land(TIP_HEIGHT + 3, &second).await?;
        let later = age_seconds(pending_age_of(&restarted, &metrics).await?)?;
        ensure!(
            later >= rising,
            "a second landing reset the hold to {later:.3} s from {rising:.3} s"
        );

        // The publication that carries both returns it to zero, sampling
        // each once.
        publish(&restarted).await?;
        ensure!(
            pending_age_of(&restarted, &metrics).await? == PendingAge::None,
            "the published landings still read as pending"
        );
        let (published, superseded) = restarted_samples(&metrics);
        eprintln!(
            "accepted publication: restarted process unknown, then 0, then a hold of \
             {first:.3} s, {rising:.3} s, {later:.3} s, then 0; published count {published}, \
             superseded count {superseded}"
        );
        ensure!(
            (published, superseded) == (1., 1.),
            "the two landings after the restart produced {published} published and \
             {superseded} superseded samples"
        );

        // An unrelated later revision bump does not make a landing this
        // frontend already published wait again.
        bump_revision(&fixture.pool).await?;
        ensure!(
            pending_age_of(&restarted, &metrics).await? == PendingAge::None,
            "an unrelated revision bump resurrected a published landing"
        );
        Ok(())
    }
    .await;
    restarted.ledger.pool.close().await;
    result
}

/// One refresh on `coordinator` that must publish, with its queued
/// observation awaited.
async fn publish(coordinator: &Coordinator) -> Result<()> {
    let published = coordinator.refresh.subscribe();
    let observations = coordinator.accepted_publication_observations();
    coordinator.refresh_once().await?;
    ensure!(published.has_changed()?, "the refresh published nothing");
    tokio::time::timeout(PROCESS_BOUND, async {
        while coordinator.accepted_publication_observations() <= observations {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("the publication observation never finished")?;
    Ok(())
}

/// `(published, superseded)` sample counts on `metrics`.
fn restarted_samples(metrics: &Metrics) -> (f64, f64) {
    let body = metrics.render();
    let count = |result: &str| {
        sample(
            &body,
            &format!("{SAMPLE_PREFIX}_count{{result=\"{result}\"}}"),
        )
        .unwrap_or(0.)
    };
    (count("published"), count("superseded"))
}

/// F3: observations run in publication order. Two publications at the same
/// revision are queued first-then-second; the block is claimed by the first,
/// so its sample ends at the first publication's instant, never the second's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_observations_sample_a_block_at_the_first_publication_that_carries_it() -> Result<()>
{
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = publication_order(&fixture).await;
    fixture.close(result).await
}

async fn publication_order(fixture: &Fixture) -> Result<()> {
    fixture.refresh().await?;
    let hash = fixture.land(TIP_HEIGHT + 1, &PARENT.repeat(32)).await?;
    let offered_at_ms = fixture.offered_at_ms(&hash).await?;
    let revision = fixture.ledger().payout_revision().await?;
    let first_ms = unix_ms_now()?;
    let second_ms = first_ms + 10_000;
    let observations = fixture.coordinator.accepted_publication_observations();
    fixture
        .coordinator
        .enqueue_accepted_publication_observation(revision, first_ms);
    fixture
        .coordinator
        .enqueue_accepted_publication_observation(revision, second_ms);
    tokio::time::timeout(PROCESS_BOUND, async {
        while fixture.coordinator.accepted_publication_observations() < observations + 2 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("the queued observations never finished")?;
    let (count, sum) = fixture.samples("published");
    let expected = (first_ms - offered_at_ms) as f64 / 1e3;
    eprintln!(
        "accepted publication: queued publications at {first_ms} and {second_ms} ms; \
         block offered at {offered_at_ms} ms sampled once at {sum:.3} s (expected {expected:.3} s)"
    );
    ensure!(count == 1., "the block was sampled {count} times");
    ensure!(
        (sum - expected).abs() < 1e-6,
        "the sample {sum:.3} s is not the first publication's {expected:.3} s"
    );
    Ok(())
}
