//! #622: the readiness clock behind `tip polling stale` follows the node's
//! tip, not the rebuild that replaces the work.
//!
//! Every refresh first polls the node, then rebuilds. At a fast reanchor each
//! poll is a full rebuild, and under load one can outlast the health timeout
//! after its poll. Readiness was renewed only when a refresh completed, so
//! job preparation refused the current, valid work as `tip polling stale`
//! while the node answered on the published tip. Each case holds a real RPC
//! at a gate, never sleeps, and ages the readiness clock the way an
//! outlasting rebuild leaves it.
use super::*;
use tokio::time::timeout;

const BOUND: Duration = Duration::from_secs(5);

fn gate(fixture: &Fixture, method: &str) -> Arc<Gate> {
    let gate = Arc::new(Gate::default());
    fixture.node.lock().unwrap().gate = Some((method.into(), gate.clone()));
    gate
}

async fn entered(gate: &Gate) {
    timeout(BOUND, gate.entered.notified())
        .await
        .expect("the real RPC request must reach its gate");
}

/// Readiness last renewed longer than the health timeout ago, as a rebuild
/// that outlasts it after the previous refresh leaves it. Returns that stamp.
async fn age_readiness(fixture: &Fixture) -> Instant {
    let aged = Instant::now() - fixture.coordinator.config.health_timeout - Duration::from_secs(1);
    fixture.coordinator.readiness.write().await.last_poll = Some(aged);
    aged
}

async fn prepare(
    fixture: &Fixture,
    worker: &Worker,
) -> Result<MiningJob<JobContext>, StratumError> {
    fixture
        .coordinator
        .build_job(worker, "00000000", 1e-12, 0.0)
        .await
}

fn worker(fixture: &Fixture) -> Worker {
    fixture.job(1, 0, "original.worker").context.worker.clone()
}

/// `qbit_prism_job_preparation_deferrals_total{reason}`.
fn deferrals(fixture: &Fixture, reason: &str) -> f64 {
    let wanted = format!("qbit_prism_job_preparation_deferrals_total{{reason=\"{reason}\"}} ");
    let rendered = fixture.coordinator.metrics.render();
    rendered
        .lines()
        .find_map(|line| line.strip_prefix(&wanted))
        .unwrap_or_else(|| panic!("no {reason} deferral sample in:\n{rendered}"))
        .parse()
        .unwrap()
}

/// A refresh-grade poll held at the node, after it reserved its sequence and
/// read the readiness epoch.
async fn held_poll(fixture: &Fixture) -> (Arc<Gate>, tokio::task::JoinHandle<Result<Value>>) {
    let poll = gate(fixture, "getblockchaininfo");
    let coordinator = fixture.coordinator.clone();
    let task = tokio::spawn(async move { coordinator.observe_chain_info(true).await });
    entered(&poll).await;
    (poll, task)
}

#[tokio::test]
async fn a_rebuild_held_after_its_poll_keeps_the_current_work_admitted() {
    let mut fixture = Fixture::new(Duration::from_secs(10)).await;
    // Every refresh is a reanchor rebuild, as at the live fixture's 1 s.
    Arc::get_mut(&mut Arc::get_mut(&mut fixture.coordinator).unwrap().config)
        .unwrap()
        .snapshot_interval = Duration::ZERO;
    fixture.coordinator.refresh_once().await.unwrap();
    let worker = worker(&fixture);
    age_readiness(&fixture).await;
    let published = fixture.coordinator.prepared.read().await.clone().unwrap();
    // The refusal this case is about: current work, unchanged tip, no poll.
    let error = fixture
        .coordinator
        .issued_work_revision(&published)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "tip polling stale");
    assert!(prepare(&fixture, &worker).await.is_err());
    assert_eq!(deferrals(&fixture, "tip_polling_stale"), 1.0);
    // The rebuild has outlasted the critical refresh-stall bound too.
    *fixture.coordinator.refreshed_at.lock().unwrap() = Instant::now() - Duration::from_secs(121);

    // The next refresh polls the node on the published tip, agrees with the
    // cluster's chain view, then stalls in its rebuild at persistence, longer
    // than the health timeout.
    let save = Arc::new(Gate::default());
    *fixture.store.compact.save_gate.lock().unwrap() = Some(save.clone());
    let coordinator = fixture.coordinator.clone();
    let refresh = tokio::spawn(async move { coordinator.refresh_once().await });
    entered(&save).await;

    let job = prepare(&fixture, &worker)
        .await
        .expect("a poll on the published tip renews readiness while the rebuild is held");
    assert_eq!(job.wire.previousblockhash, hash(1));
    assert!(Arc::ptr_eq(&job.context.prepared, &published));
    // Readiness is young, while the stalled rebuild still pages.
    let health_timeout = fixture.coordinator.config.health_timeout;
    assert!(fixture.coordinator.tip_poll_age().await.unwrap() < health_timeout);
    assert!(fixture.coordinator.work_refresh_age() >= Duration::from_secs(120));
    for reason in crate::metrics::JobDeferral::ALL {
        let expected = f64::from(*reason == crate::metrics::JobDeferral::TipPollingStale);
        assert_eq!(deferrals(&fixture, reason.as_str()), expected, "{reason:?}");
    }

    save.release.notify_one();
    timeout(BOUND, refresh).await.unwrap().unwrap().unwrap();
    assert!(fixture.coordinator.work_refresh_age() < health_timeout);
}

#[tokio::test]
async fn a_poll_that_a_newer_observation_replaced_does_not_renew_readiness() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let aged = age_readiness(&fixture).await;
    // The older poll answers the published tip, but only after a newer poll
    // found another tip: completed out of order, it must not win.
    let (poll, older) = held_poll(&fixture).await;
    fixture.detect(2).await;
    poll.release.notify_one();
    timeout(BOUND, older).await.unwrap().unwrap().unwrap();
    assert_eq!(
        fixture.coordinator.observed_tip.read().await.as_deref(),
        Some(hash(2).as_str())
    );
    assert_eq!(
        fixture.coordinator.readiness.read().await.last_poll,
        Some(aged)
    );
    // Nor does the newer poll, which found a tip no work is published for;
    // the replacement lease, not readiness, admits the published work.
    let published = fixture.coordinator.prepared.read().await.clone().unwrap();
    let admitted = fixture.coordinator.issued_work_revision(&published).await;
    assert!(admitted.is_ok_and(|revision| revision.is_some()));
}

#[tokio::test]
async fn a_poll_that_raced_a_revocation_does_not_renew_readiness() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let (poll, raced) = held_poll(&fixture).await;
    // Revoked while the poll is in flight, then restored by a refresh of the
    // new epoch whose proof has since aged: the earlier poll proves neither.
    fixture.coordinator.invalidate_readiness().await;
    let aged = age_readiness(&fixture).await;
    poll.release.notify_one();
    timeout(BOUND, raced).await.unwrap().unwrap().unwrap();
    assert_eq!(
        fixture.coordinator.readiness.read().await.last_poll,
        Some(aged)
    );
}

#[tokio::test]
async fn a_poll_never_restores_revoked_readiness_or_vouches_for_an_old_template() {
    let mut fixture = Fixture::new(Duration::from_secs(10)).await;
    let curtime = chrono::Utc::now().timestamp() - 90;
    fixture.node.lock().unwrap().template = Some(json!({"version":0x20000000u32,
        "bits":"207fffff","curtime":curtime,"previousblockhash":hash(1),
        "transactions":[],"height":101,"coinbasevalue":500_000_000}));
    fixture.coordinator.refresh_once().await.unwrap();

    fixture.coordinator.invalidate_readiness().await;
    fixture.detect(1).await;
    assert_eq!(fixture.coordinator.readiness.read().await.last_poll, None);
    assert_eq!(fixture.coordinator.tip_poll_age().await, None);

    fixture.coordinator.refresh_once().await.unwrap();
    // The published template is now older than the reuse path would accept.
    Arc::get_mut(&mut Arc::get_mut(&mut fixture.coordinator).unwrap().config)
        .unwrap()
        .template_max_age = Duration::from_secs(60);
    let aged = age_readiness(&fixture).await;
    fixture.detect(1).await;
    assert_eq!(
        fixture.coordinator.readiness.read().await.last_poll,
        Some(aged)
    );
    let published = fixture.coordinator.prepared.read().await.clone().unwrap();
    let error = fixture
        .coordinator
        .issued_work_revision(&published)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "tip polling stale");
}

#[tokio::test]
async fn a_poll_does_not_renew_readiness_once_the_cluster_holds_a_heavier_chain() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    // This node still answers the published tip, but the cluster's chain
    // view is heavier: the refresh refuses it, and a poll proves nothing.
    // Neither the refusing refresh's own poll, sent before its chain
    // observation, nor a later poll renews readiness.
    fixture
        .store
        .compact
        .observation_behind
        .store(true, Ordering::SeqCst);
    let aged = age_readiness(&fixture).await;
    let error = fixture.coordinator.refresh_once().await.unwrap_err();
    assert!(error.is::<crate::ledger::ChainObservationBehind>());
    assert_eq!(
        fixture.coordinator.readiness.read().await.last_poll,
        Some(aged)
    );
    fixture.detect(1).await;
    assert_eq!(
        fixture.coordinator.readiness.read().await.last_poll,
        Some(aged)
    );
    // A refresh that agrees with the cluster again restores poll renewal.
    fixture.coordinator.refresh_once().await.unwrap();
    let aged = age_readiness(&fixture).await;
    fixture.detect(1).await;
    assert!(fixture.coordinator.readiness.read().await.last_poll > Some(aged));
}

/// CTV settlement with a configured fanout fee, so a refresh's fee read is
/// exactly one `getmempoolinfo`.
pub(super) async fn ctv_fixture() -> Fixture {
    Fixture::build(
        Duration::from_secs(10),
        |config| {
            config.ctv_enabled = true;
            config.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
        },
        None,
    )
    .await
}

#[tokio::test]
async fn a_refresh_held_before_its_relay_floor_read_does_not_renew_readiness() {
    let fixture = ctv_fixture().await;
    fixture.coordinator.refresh_once().await.unwrap();
    let aged = age_readiness(&fixture).await;
    // The refresh polls the published tip and agrees with the cluster, then
    // stalls reading the relay floor: its poll must not vouch for the floor
    // the previous refresh read.
    let floor = gate(&fixture, "getmempoolinfo");
    let coordinator = fixture.coordinator.clone();
    let refresh = tokio::spawn(async move { coordinator.refresh_once().await });
    entered(&floor).await;
    assert_eq!(
        fixture.coordinator.readiness.read().await.last_poll,
        Some(aged)
    );
    floor.release.notify_one();
    timeout(BOUND, refresh).await.unwrap().unwrap().unwrap();
    assert!(fixture.coordinator.readiness.read().await.last_poll > Some(aged));
}

#[tokio::test]
async fn without_ctv_a_refresh_renews_once_its_chain_view_agrees_whatever_fails_after() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let aged = age_readiness(&fixture).await;
    // The ledger probe after the chain observation fails: no relay floor is
    // involved, so the agreeing poll has already renewed readiness.
    fixture
        .store
        .compact
        .states
        .lock()
        .unwrap()
        .push_back(Err(WindowError::Database(sqlx::Error::PoolClosed)));
    fixture.coordinator.refresh_once().await.unwrap_err();
    assert_eq!(fixture.store.compact.states.lock().unwrap().len(), 0);
    assert!(fixture.coordinator.readiness.read().await.last_poll > Some(aged));
}

/// A relay floor high enough that the fixture's configured CTV fee misses it.
fn raised_floor() -> Value {
    json!({"minrelaytxfee":0.00001,"mempoolminfee":0.01})
}

#[tokio::test]
async fn a_refresh_fee_read_overtaken_by_a_later_higher_floor_cannot_vouch_for_its_fee() {
    let fixture = ctv_fixture().await;
    fixture.coordinator.refresh_once().await.unwrap();
    let published = fixture.coordinator.prepared.read().await.clone().unwrap();
    // The refresh's floor read is sent first and answers the old floor last.
    let floor = gate(&fixture, "getmempoolinfo");
    let coordinator = fixture.coordinator.clone();
    let refresh = tokio::spawn(async move { coordinator.refresh_once().await });
    entered(&floor).await;
    // A block-wait poll sent later reads a raised floor and records it.
    fixture.node.lock().unwrap().mempool = Some(raised_floor());
    fixture.coordinator.observe_chain_info(true).await.unwrap();
    let raised = fixture.coordinator.readiness.read().await.ctv_fee_floor;
    floor.release.notify_one();
    let error = timeout(BOUND, refresh).await.unwrap().unwrap().unwrap_err();
    assert!(error.to_string().contains("relay floor"), "{error:#}");
    let readiness = fixture.coordinator.readiness.read().await;
    assert_eq!(readiness.ctv_fee_floor, raised);
    assert_eq!(readiness.last_poll, None);
    drop(readiness);
    let current = fixture.coordinator.prepared.read().await.clone().unwrap();
    assert!(Arc::ptr_eq(&current, &published));
}

#[tokio::test]
async fn work_built_before_a_raised_floor_is_not_published() {
    let mut fixture = ctv_fixture().await;
    Arc::get_mut(&mut Arc::get_mut(&mut fixture.coordinator).unwrap().config)
        .unwrap()
        .snapshot_interval = Duration::ZERO;
    fixture.coordinator.refresh_once().await.unwrap();
    let published = fixture.coordinator.prepared.read().await.clone().unwrap();
    // The rebuild read the old floor and is held at persistence when a
    // block-wait poll records a raised one.
    let save = Arc::new(Gate::default());
    *fixture.store.compact.save_gate.lock().unwrap() = Some(save.clone());
    let coordinator = fixture.coordinator.clone();
    let refresh = tokio::spawn(async move { coordinator.refresh_once().await });
    entered(&save).await;
    fixture.node.lock().unwrap().mempool = Some(raised_floor());
    fixture.coordinator.observe_chain_info(true).await.unwrap();
    save.release.notify_one();
    // Whichever check refuses it first (the revoked readiness, or the fee at
    // publication), nothing priced under the live floor is published.
    timeout(BOUND, refresh).await.unwrap().unwrap().unwrap_err();
    let current = fixture.coordinator.prepared.read().await.clone().unwrap();
    assert!(Arc::ptr_eq(&current, &published));
}

#[tokio::test]
async fn a_relay_floor_read_is_ordered_by_when_its_own_request_was_sent() {
    // The market rate: a refresh estimates the fee before it reads the floor.
    let fixture = Fixture::build(
        Duration::from_secs(10),
        |config| config.ctv_enabled = true,
        None,
    )
    .await;
    fixture.coordinator.refresh_once().await.unwrap();
    let estimate = gate(&fixture, "estimatesmartfee");
    let coordinator = fixture.coordinator.clone();
    let refresh = tokio::spawn(async move { coordinator.refresh_once().await });
    entered(&estimate).await;
    // During the slow estimate a block-wait poll records a raised floor;
    // then the floor falls back before the refresh reads it.
    fixture.node.lock().unwrap().mempool = Some(raised_floor());
    fixture.coordinator.observe_chain_info(true).await.unwrap();
    let raised = fixture.coordinator.readiness.read().await.ctv_fee_floor;
    fixture.node.lock().unwrap().mempool = None;
    estimate.release.notify_one();
    // The raised floor retired the published fee, so readiness was revoked
    // and this refresh, started before, cannot publish; but its own floor
    // read was sent last, and it is the one kept.
    let _ = timeout(BOUND, refresh).await.unwrap().unwrap();
    assert!(fixture.coordinator.readiness.read().await.ctv_fee_floor < raised);
    // So the next refresh restores readiness instead of refusing the fee.
    fixture.coordinator.refresh_once().await.unwrap();
    assert!(fixture
        .coordinator
        .readiness
        .read()
        .await
        .last_poll
        .is_some());
}

#[tokio::test]
async fn a_poll_that_raises_the_floor_above_the_published_fee_fences_jobs_in_flight() {
    let fixture = ctv_fixture().await;
    fixture.coordinator.refresh_once().await.unwrap();
    let worker = worker(&fixture);
    // A job is built against the old floor and held before its final
    // admission check.
    let probe = Arc::new(OfferProbe::default());
    *fixture.coordinator.build_job_probe.lock().unwrap() = Some(probe.clone());
    let coordinator = fixture.coordinator.clone();
    let job =
        tokio::spawn(async move { coordinator.build_job(&worker, "00000000", 1e-12, 0.0).await });
    timeout(BOUND, probe.entered.notified()).await.unwrap();
    *fixture.coordinator.build_job_probe.lock().unwrap() = None;
    // A block-wait poll then reads a floor above the published fee.
    fixture.node.lock().unwrap().mempool = Some(raised_floor());
    fixture.coordinator.observe_chain_info(true).await.unwrap();
    assert_eq!(fixture.coordinator.readiness.read().await.last_poll, None);
    probe.release.notify_one();
    let refused = timeout(BOUND, job).await.unwrap().unwrap();
    assert!(
        refused.is_err(),
        "a job priced under the live floor was delivered"
    );
}
