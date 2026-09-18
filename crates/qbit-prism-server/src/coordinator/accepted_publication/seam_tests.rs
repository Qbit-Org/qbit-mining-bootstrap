//! Crate-internal seams for #458's observations.
//!
//! * The stale-revision refusal counter observes only `build_job`'s refusal
//!   for a payout revision behind the cluster's, never a publication race
//!   that refuses the same build with the same error.
//! * A publication that fails after its reservation records no sample and
//!   leaves the pending gauge alone, even when the cluster revision is the
//!   one the failed publication captured. This is the order the observation
//!   must keep with `publish()`: moving it above the publication call makes
//!   this test fail.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=... cargo test -p qbit-prism-server --lib accepted_publication::seam_tests -- --nocapture
//! ```
use super::super::d2_test_support::{
    database_url, settle, test_config, unix_now, TestSchema, TEMPLATE_BITS,
};
use super::super::miner_tests::{Fixture, Gate, SharedLog};
use super::super::test_serial::TEST_LOCK;
use super::super::*;
use crate::{metrics::PendingAge, stratum::MiningBackend};
use anyhow::bail;
use axum::{routing::post, Json, Router};
use tokio_util::task::AbortOnDropHandle;
use tracing::instrument::WithSubscriber;

const REFUSALS: &str = "qbit_prism_stale_payout_revision_job_refusals_total";
const SAMPLE_COUNT: &str = "qbit_prism_accepted_block_work_publication_seconds_count";

fn sample(body: &str, key: &str) -> Option<f64> {
    let prefix = format!("{key} ");
    body.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.parse().ok())
}

fn refusals(coordinator: &Coordinator) -> f64 {
    sample(&coordinator.metrics.render(), REFUSALS).unwrap_or(-1.)
}

/// Build one job with its logs captured: `build_job` answers every refusal
/// with the same protocol error and logs the cause it deferred for.
async fn build(
    coordinator: &Arc<Coordinator>,
    worker: &Worker,
    extranonce1: &'static str,
) -> (bool, String) {
    let log = SharedLog::default();
    let coordinator = coordinator.clone();
    let worker = worker.clone();
    let built = tokio::spawn(
        async move {
            coordinator
                .build_job(&worker, extranonce1, 1e-12, 0.0)
                .await
                .is_ok()
        }
        .with_subscriber(log.dispatch()),
    )
    .await
    .unwrap();
    (built, log.text())
}

/// F2: a stale payout revision refused in `build_job`'s admission counts
/// once; a build refused because another publication landed during its
/// admission ("payout snapshot stale" as well) counts nothing, and neither
/// does the same stale revision met by an admission outside `build_job`.
#[tokio::test]
async fn only_a_stale_revision_refusal_in_the_build_admission_is_counted() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let worker = fixture.job(1, 0, "original.worker").context.worker.clone();
    let coordinator = fixture.coordinator.clone();
    let (built, log) = build(&coordinator, &worker, "00000000").await;
    assert!(built, "{log}");
    assert_eq!(refusals(&coordinator), 0.);

    // The cluster revision moves past the published work's.
    fixture.store.revision.store(1, Ordering::SeqCst);
    let (built, log) = build(&coordinator, &worker, "00000001").await;
    assert!(!built && log.contains("payout snapshot stale"), "{log}");
    assert_eq!(refusals(&coordinator), 1.);
    fixture.store.revision.store(0, Ordering::SeqCst);

    // A publication race: the admission captures the publication stamp, then
    // waits on its revision read while the same tip is republished. The
    // revision still matches; the stamp does not.
    let gate = Arc::new(Gate::default());
    *fixture.store.revision_gate.lock().unwrap() = Some(gate.clone());
    let raced = {
        let coordinator = coordinator.clone();
        let worker = worker.clone();
        tokio::spawn(async move { build(&coordinator, &worker, "00000002").await })
    };
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .expect("the admission never reached its revision read");
    {
        let _prepared = coordinator.prepared.write().await;
        let mut observed = coordinator.observed_tip.write().await;
        let tip = observed.as_deref().expect("a published tip").to_owned();
        observed.publish(&tip).unwrap();
    }
    gate.release.notify_one();
    let (built, log) = tokio::time::timeout(Duration::from_secs(5), raced)
        .await
        .unwrap()
        .unwrap();
    assert!(!built && log.contains("payout snapshot stale"), "{log}");
    assert_eq!(
        refusals(&coordinator),
        1.,
        "a publication race was counted as a stale payout revision"
    );

    // The same stale revision met by an admission outside `build_job` (the
    // resume and repair paths share this check) is not counted.
    fixture.store.revision.store(1, Ordering::SeqCst);
    let identity =
        tip_observation::PreparedIdentity::of(coordinator.prepared.read().await.as_ref().unwrap());
    let epoch = coordinator.readiness.read().await.generation;
    assert!(coordinator
        .begin_issuance_authority(identity, epoch, None)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        refusals(&coordinator),
        1.,
        "an admission outside build_job was counted"
    );
}

// ---------------------------------------------------------------------------
// A publication that fails after its reservation, on real PostgreSQL.
// ---------------------------------------------------------------------------

const TIP_HEIGHT: u64 = 100;

fn tip() -> String {
    "ab".repeat(32)
}

async fn node_reply(Json(request): Json<Value>) -> Json<Value> {
    let result = match request["method"].as_str().unwrap_or_default() {
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":TIP_HEIGHT,"headers":TIP_HEIGHT,"bestblockhash":tip(),
            "chainwork":format!("{:064x}", 1)}),
        "getnetworkinfo" => json!({"connections":2}),
        "getbestblockhash" => json!(tip()),
        "getblockhash" => match request["params"][0].as_u64() {
            Some(0) => json!("00".repeat(32)),
            Some(TIP_HEIGHT) => json!(tip()),
            _ => Value::Null,
        },
        "getblockheader" => json!({"previousblockhash":"cd".repeat(32)}),
        "getblocktemplate" => json!({"version":0x2000_0000u32,"bits":TEMPLATE_BITS,
            "height":TIP_HEIGHT+1,"coinbasevalue":5_000_000_000u64,
            "curtime":unix_now().expect("the host clock precedes the epoch"),
            "previousblockhash":tip(),"transactions":[]}),
        method => panic!("unexpected accepted-publication seam RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

/// F5: the reservation succeeds, the publication is then refused, and the
/// cluster revision is the one the refused publication captured. Nothing may
/// be sampled and the pending gauge must keep counting the landing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publication_refused_after_its_reservation_records_nothing() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let schema = TestSchema::create(&raw, "prism_accepted_publication_seam").await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}/", listener.local_addr()?);
    let _server = AbortOnDropHandle::new(tokio::spawn(async move {
        let app = Router::new().route("/", post(node_reply)).with_state(());
        let _ = axum::serve(listener, app).await;
    }));
    let outcome = async {
        let config = test_config(
            schema.url(),
            rpc_url,
            "accepted-publication-seam",
            Duration::from_secs(15),
        )?;
        let coordinator =
            Coordinator::new(config, Arc::new(crate::metrics::Metrics::default())).await?;
        let result = refused_publication(&coordinator).await;
        coordinator.ledger.pool.close().await;
        result
    }
    .await;
    settle(outcome, schema.remove().await)
}

async fn refused_publication(coordinator: &Arc<Coordinator>) -> Result<()> {
    let pool = &coordinator.ledger.pool;
    coordinator
        .ledger
        .observe_chain_view(&tip(), TIP_HEIGHT, &format!("{:064x}", 1))
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
                    template_height: TIP_HEIGHT,
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
    coordinator.refresh_once().await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while coordinator.accepted_publication_observations() < 1 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("the first publication was never observed")?;
    ensure!(
        coordinator.publish_accepted_pending_age().await == PendingAge::None,
        "the seeding tick did not read zero"
    );

    // A landed, accepted block the published work does not carry yet: the
    // outbox row is terminal `submitted`, offered after this process started,
    // and the cluster revision moves past the published one.
    let offered_at_ms = unix_ms_now()? - 50;
    sqlx::query(
        "INSERT INTO qbit_block_candidate_outbox \
         (block_hash,candidate_sha256,state,completed_at,offer_reserved_at,offer_reserved_by,offered_at_ms,offer_outcome) \
         VALUES ($1,$2,'submitted',clock_timestamp(),clock_timestamp(),'another-frontend',$3,'accepted')",
    )
    .bind("ef".repeat(32))
    .bind("00".repeat(32))
    .bind(offered_at_ms)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton",
    )
    .execute(pool)
    .await?;
    let before = match coordinator.publish_accepted_pending_age().await {
        PendingAge::Oldest(age) => age,
        other => bail!("the unpublished landing reads {other:?}, not an age"),
    };

    // The next refresh captures that revision and reserves its work; at the
    // seam the published tip is superseded, so the publication is refused.
    let probe = Arc::new(OfferProbe::default());
    *coordinator
        .accepted_publication
        .publication_probe
        .lock()
        .unwrap() = Some(probe.clone());
    let observations = coordinator.accepted_publication_observations();
    let refresh = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move { coordinator.refresh_once().await })
    };
    tokio::time::timeout(Duration::from_secs(10), probe.entered.notified())
        .await
        .context("the refresh never reached its publication")?;
    *coordinator
        .accepted_publication
        .publication_probe
        .lock()
        .unwrap() = None;
    let captured = coordinator.ledger.payout_revision().await?;
    *coordinator.observed_tip.write().await = TipState::baseline("cd".repeat(32));
    probe.release.notify_one();
    let refused = tokio::time::timeout(Duration::from_secs(10), refresh)
        .await
        .context("the refused refresh never finished")??;
    ensure!(
        refused.is_err(),
        "the publication was not refused after its reservation"
    );
    ensure!(
        coordinator.ledger.payout_revision().await? == captured,
        "the cluster revision moved; the test no longer isolates the order"
    );

    tokio::time::sleep(Duration::from_millis(200)).await;
    ensure!(
        coordinator.accepted_publication_observations() == observations,
        "a refused publication was observed"
    );
    let body = coordinator.metrics.render();
    ensure!(
        !body.lines().any(|line| line.starts_with(SAMPLE_COUNT)),
        "a refused publication recorded a sample:\n{body}"
    );
    let after = match coordinator.publish_accepted_pending_age().await {
        PendingAge::Oldest(age) => age,
        other => bail!("the refused publication moved the gauge to {other:?}"),
    };
    ensure!(
        after >= before,
        "the gauge fell from {before:?} to {after:?} without a publication"
    );
    println!(
        "accepted publication seam: refused after reservation ({}), no sample, pending {:.3} s then {:.3} s",
        refused.unwrap_err(),
        before.as_secs_f64(),
        after.as_secs_f64()
    );
    Ok(())
}
