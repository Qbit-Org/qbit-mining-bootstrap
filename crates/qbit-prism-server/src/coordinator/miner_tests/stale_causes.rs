//! Each stale-job decision records one closed internal cause at its own branch.
//! Responses, credit and the coarse `stale-job` reason are unchanged.
use super::*;
use crate::metrics::{Metrics, StaleJobCause};

/// `[resume_expired, fee_floor, parent_grace, payout_revision]`.
pub(crate) fn stale_causes(metrics: &Metrics) -> [f64; 4] {
    let body = metrics.render();
    StaleJobCause::ALL
        .iter()
        .map(|cause| {
            let key = format!(
                "qbit_prism_stale_job_rejections_total{{cause=\"{}\"}} ",
                cause.as_str()
            );
            let values: Vec<f64> = body
                .lines()
                .filter_map(|line| line.strip_prefix(&key))
                .map(|value| value.parse().unwrap())
                .collect();
            assert_eq!(values.len(), 1, "expected one sample: {key}");
            values[0]
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
}

/// The complete existing wire response, not only its reason.
pub(crate) fn assert_stale_wire(error: StratumError, message: &str) {
    assert_eq!(
        error.response(json!(41)),
        json!({"id":41,"result":null,"error":[21,message,{"reason_id":"stale-job"}]})
    );
}

async fn ctv_fixture() -> (Fixture, MiningJob<JobContext>) {
    let fixture = Fixture::build(
        Duration::from_secs(10),
        |config| {
            config.ctv_enabled = true;
            config.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
        },
        None,
    )
    .await;
    fixture.coordinator.refresh_once().await.unwrap();
    let worker = fixture.job(1, 0, "original.worker").context.worker.clone();
    let issued = fixture
        .coordinator
        .build_job(&worker, "00000000", 1e-12, 0.0)
        .await
        .unwrap();
    (fixture, issued)
}

#[tokio::test]
async fn fee_floor_cause_precedes_payout_revision_drift_and_reads_no_revision() {
    let (fixture, issued) = ctv_fixture().await;
    let metrics = fixture.coordinator.metrics.clone();
    let original = fixture.coordinator.readiness.read().await.ctv_fee_floor;
    fixture.coordinator.readiness.write().await.ctv_fee_floor = Some(2000);
    // A later predicate is never evaluated to choose a cause: a revision read
    // that would fail is not reached.
    fixture.store.fail_revision.store(true, Ordering::SeqCst);
    assert_stale_wire(
        fixture.submit(&issued, false).await.unwrap_err(),
        "job CTV fee is below the current relay floor",
    );
    assert_eq!(stale_causes(&metrics), [0., 1., 0., 0.]);
    fixture.store.fail_revision.store(false, Ordering::SeqCst);
    fixture.store.revision.store(1, Ordering::SeqCst);
    assert_stale_wire(
        fixture.submit(&issued, false).await.unwrap_err(),
        "job CTV fee is below the current relay floor",
    );
    assert_eq!(stale_causes(&metrics), [0., 2., 0., 0.]);
    // Only removing the floor exposes the revision decision it preceded.
    fixture.coordinator.readiness.write().await.ctv_fee_floor = original;
    assert_stale_wire(
        fixture.submit(&issued, false).await.unwrap_err(),
        "stale job",
    );
    assert_eq!(stale_causes(&metrics), [0., 2., 0., 1.]);
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn parent_without_grace_precedes_payout_revision_drift() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let metrics = fixture.coordinator.metrics.clone();
    let old = fixture.job(1, 0, "original.worker");
    fixture.observe(1, true).await;
    fixture.observe(2, true).await;
    fixture.store.revision.store(1, Ordering::SeqCst);
    assert_stale_wire(fixture.submit(&old, false).await.unwrap_err(), "stale job");
    assert_eq!(stale_causes(&metrics), [0., 0., 1., 0.]);
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn failed_grace_parent_check_is_parent_grace_even_with_revision_drift() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let metrics = fixture.coordinator.metrics.clone();
    fixture.observe(1, true).await;
    fixture.observe(3, true).await;
    fixture.store.revision.store(1, Ordering::SeqCst);
    let old = fixture.job(1, 0, "original.worker");
    assert_stale_wire(fixture.submit(&old, true).await.unwrap_err(), "stale job");
    assert_eq!(stale_causes(&metrics), [0., 0., 1., 0.]);
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn payout_revision_mismatch_on_the_current_parent_counts_each_refusal_once() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let metrics = fixture.coordinator.metrics.clone();
    fixture.observe(1, true).await;
    fixture.store.revision.store(1, Ordering::SeqCst);
    let old = fixture.job(1, 0, "original.worker");
    // A plain SHARE (not a block — #478 Option B captures a block on the current
    // parent) refused for a superseded revision counts the payout_revision cause
    // once per submit.
    let mut proof = fixture.proof(&old, 0);
    proof.block_pass = false;
    for (grace, expected) in [(false, 1.), (true, 2.)] {
        assert_stale_wire(
            fixture
                .coordinator
                .submit(&old.context.worker, &old, proof.clone(), grace.into())
                .await
                .unwrap_err(),
            "stale job",
        );
        assert_eq!(stale_causes(&metrics), [0., 0., 0., expected]);
    }
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn immediate_parent_grace_with_revision_drift_credits_without_a_stale_cause() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let metrics = fixture.coordinator.metrics.clone();
    let job = fixture.job(1, 0, "original.worker");
    fixture.observe(1, true).await;
    fixture.observe(2, true).await;
    fixture.store.revision.store(7, Ordering::SeqCst);
    fixture.submit(&job, true).await.unwrap();
    let records = fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0.credit_policy.as_deref(), Some("stale-grace"));
    assert_eq!(records[0].2, 7, "credit uses the current durable revision");
    assert_eq!(stale_causes(&metrics), [0., 0., 0., 0.]);
    assert!(metrics
        .render()
        .contains("\nqbit_prism_grace_credited_shares_total 1\n"));
}
