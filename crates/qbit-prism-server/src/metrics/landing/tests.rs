use super::*;
use std::time::Duration;

fn sample(body: &str, name: &str) -> f64 {
    body.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap()
        .parse()
        .unwrap()
}
fn count(metrics: &Metrics, result: &str) -> f64 {
    sample(
        &metrics.render(),
        &format!("qbit_prism_accepted_block_to_revision_work_seconds_count{{result=\"{result}\"}}"),
    )
}
fn age(metrics: &Metrics) -> f64 {
    sample(
        &metrics.render(),
        "qbit_prism_accepted_block_revision_work_pending_seconds",
    )
}
async fn tick() {
    tokio::time::advance(Duration::from_secs(1)).await;
}

#[tokio::test(start_paused = true)]
async fn duplicate_proofs_and_deliveries_cannot_recount_or_reanchor() {
    let m = Metrics::default();
    let hash = "ab".repeat(32);
    m.accepted_block(&hash, 1);
    tick().await;
    m.accepted_block(&hash.to_uppercase(), 1);
    m.landed_block(&hash, 7);
    assert_eq!(age(&m), 1.);
    tick().await;
    m.revision_work_delivered(6);
    assert_eq!(age(&m), 2.);
    m.revision_work_delivered(7);
    for _ in 0..3 {
        m.accepted_block(&hash, 1);
        m.landed_block(&hash, 8);
        m.revision_work_delivered(7);
    }
    assert_eq!(age(&m), 0.);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(count(&m, "superseded"), 0.);
}

#[tokio::test(start_paused = true)]
async fn supersession_keeps_oldest_age_and_old_write_cannot_clear_it() {
    let m = Metrics::default();
    m.accepted_block(&"11".repeat(32), 1);
    m.landed_block(&"11".repeat(32), 1);
    tick().await;
    m.accepted_block(&"22".repeat(32), 1);
    m.landed_block(&"22".repeat(32), 2);
    assert_eq!(count(&m, "superseded"), 0.);
    assert_eq!(age(&m), 1.);
    tick().await;
    m.revision_work_delivered(1);
    assert_eq!(age(&m), 2.);
    assert_eq!(count(&m, "superseded"), 0.);
    m.revision_work_delivered(2);
    assert_eq!(age(&m), 0.);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(count(&m, "superseded"), 1.);
    assert_eq!(
        sample(
            &m.render(),
            "qbit_prism_accepted_block_to_revision_work_seconds_sum{result=\"superseded\"}"
        ),
        2.
    );
}

#[tokio::test(start_paused = true)]
async fn late_commit_observer_uses_actual_delivery_clock_and_event_order() {
    for delivered_first in [true, false] {
        let m = Metrics::default();
        let hash = "11".repeat(32);
        m.accepted_block(&hash, 1);
        tick().await;
        if delivered_first {
            m.revision_work_delivered(1);
        }
        tick().await;
        m.revision_work_observed(2);
        tick().await;
        if !delivered_first {
            m.revision_work_delivered(2);
        }
        m.landed_block(&hash, 1);
        assert_eq!(count(&m, "published"), f64::from(delivered_first));
        assert_eq!(count(&m, "superseded"), f64::from(!delivered_first));
        assert_eq!(age(&m), 0.);
    }
}

#[tokio::test(start_paused = true)]
async fn repeated_revision_delivery_uses_first_write_after_each_acceptance() {
    let m = Metrics::default();
    let first = "11".repeat(32);
    let second = "22".repeat(32);
    m.accepted_block(&first, 1);
    tick().await;
    m.revision_work_delivered(1);
    tick().await;
    m.accepted_block(&second, 2);
    m.landed_block(&second, 1);
    assert_eq!(count(&m, "published"), 0.);
    tick().await;
    m.revision_work_delivered(1);
    assert_eq!(count(&m, "published"), 1.);
    m.landed_block(&first, 1);
    assert_eq!(count(&m, "published"), 2.);
    assert_eq!(age(&m), 0.);
    assert_eq!(
        sample(
            &m.render(),
            "qbit_prism_accepted_block_to_revision_work_seconds_sum{result=\"published\"}"
        ),
        2.
    );
}

#[tokio::test(start_paused = true)]
async fn build_identity_excludes_later_acceptances_and_cancel_is_not_timeout() {
    let m = Metrics::default();
    m.accepted_block(&"11".repeat(32), 1);
    let build = m.revision_work_build();
    tick().await;
    m.accepted_block(&"22".repeat(32), 1);
    build.deadline_hit();
    {
        let _cancelled = m.revision_work_build();
    }
    m.landed_block(&"11".repeat(32), 1);
    m.landed_block(&"22".repeat(32), 1);
    tick().await;
    m.revision_work_delivered(1);
    assert_eq!(count(&m, "degraded"), 1.);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(
        sample(&m.render(), "qbit_prism_revision_work_build_timeouts_total"),
        1.
    );
}

#[tokio::test(start_paused = true)]
async fn late_binding_does_not_attribute_later_timeout_to_earlier_success() {
    let m = Metrics::default();
    m.accepted_block(&"11".repeat(32), 1);
    let build = m.revision_work_build();
    tick().await;
    m.revision_work_delivered(1);
    tick().await;
    build.deadline_hit();
    m.landed_block(&"11".repeat(32), 1);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(count(&m, "degraded"), 0.);
}

#[tokio::test(start_paused = true)]
async fn failed_cancelled_and_reordered_observations_preserve_unknown_and_known_age() {
    let m = Metrics::default();
    assert_eq!(age(&m), 0.);
    drop(m.revision_work_refresh());
    assert_eq!(age(&m), -1.);
    let old = m.revision_work_refresh();
    m.revision_work_refresh().succeeded();
    drop(old);
    assert_eq!(age(&m), 0.);
    m.accepted_block(&"11".repeat(32), 1);
    tick().await;
    drop(m.revision_work_refresh());
    assert_eq!(age(&m), 1.);
    let mut cached = m.render();
    tick().await;
    m.overlay_live_observations(&mut cached);
    assert_eq!(
        sample(
            &cached,
            "qbit_prism_accepted_block_revision_work_pending_seconds"
        ),
        2.
    );
}

#[tokio::test(start_paused = true)]
async fn long_stall_saturation_is_bounded_unknown_and_never_recounts_evicted_ids() {
    let m = Metrics::default();
    for id in 0..LIMIT {
        m.accepted_block(&format!("{id:064x}"), 1);
    }
    tick().await;
    assert_eq!(age(&m), 1.);
    m.accepted_block(&format!("{:064x}", LIMIT), 1);
    assert_eq!(age(&m), -1.);
    m.revision_work_refresh().succeeded();
    assert_eq!(age(&m), -1.);
    let state = m.landing.lock().unwrap();
    assert_eq!(state.blocks.len(), LIMIT);
    drop(state);
    m.accepted_block(&format!("{:064x}", 0), 1);
    m.landed_block(&format!("{:064x}", 0), 1);
    m.revision_work_delivered(1);
    m.accepted_block(&format!("{:064x}", 0), 1);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(age(&m), -1.);
    // Freeing completed history cannot restart a previously missed acceptance
    // clock after saturation. Only a process restart opens a new horizon.
    m.revision_work_matured(1);
    let missed = format!("{:064x}", LIMIT);
    m.accepted_block(&missed, 2);
    m.landed_block(&missed, 1);
    m.revision_work_delivered(1);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(m.landing.lock().unwrap().blocks.len(), LIMIT - 1);
}

#[tokio::test(start_paused = true)]
async fn mature_watermark_reclaims_completed_history_but_not_unresolved_age() {
    let m = Metrics::default();
    m.accepted_block(&"11".repeat(32), 1);
    m.landed_block(&"11".repeat(32), 1);
    tick().await;
    m.revision_work_delivered(1);
    m.accepted_block(&"22".repeat(32), 2);
    tick().await;
    m.revision_work_matured(2);
    m.revision_work_matured(1);
    m.accepted_block(&"11".repeat(32), 1);
    m.accepted_block(&"33".repeat(32), 1);
    assert_eq!(m.landing.lock().unwrap().blocks.len(), 1);
    assert_eq!(age(&m), 1.);
    assert_eq!(count(&m, "published"), 1.);
    m.landed_block(&"22".repeat(32), 2);
    m.revision_work_delivered(2);
    m.revision_work_matured(2);
    assert!(m.landing.lock().unwrap().blocks.is_empty());
    assert_eq!(count(&m, "published"), 2.);
}

#[tokio::test(start_paused = true)]
async fn concurrent_reconciliation_cannot_steal_inflight_settlement_identity() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    let settlement = m.revision_work_settlement(&hash);
    tick().await;
    m.revision_work_settlement(&hash).committed(false, 2);
    m.revision_work_delivered(2);
    assert_eq!(count(&m, "published"), 0.);
    assert_eq!(count(&m, "superseded"), 0.);
    settlement.committed(true, 1);
    assert_eq!(count(&m, "superseded"), 1.);
    assert_eq!(age(&m), 0.);
}

#[tokio::test(start_paused = true)]
async fn lost_settlement_revision_is_unknown_and_only_a_proven_first_confirmation_recovers() {
    for first in [true, false] {
        let m = Metrics::default();
        let hash = "11".repeat(32);
        m.accepted_block(&hash, 1);
        drop(m.revision_work_settlement(&hash));
        assert_eq!(age(&m), -1.);
        m.observed_landed_block(&hash, 3);
        m.revision_work_delivered(3);
        assert_eq!(age(&m), -1.);
        assert_eq!(count(&m, "published"), 0.);
        m.revision_work_settlement(&hash).committed(first, 3);
        assert_eq!(age(&m), if first { 0. } else { -1. });
        assert_eq!(count(&m, "published"), f64::from(first));
    }
}

#[tokio::test(start_paused = true)]
async fn first_confirmation_wins_even_when_its_observer_started_second() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    let reconciliation = m.revision_work_settlement(&hash);
    m.revision_work_settlement(&hash).committed(true, 7);
    drop(reconciliation);
    tick().await;
    m.revision_work_delivered(7);
    assert_eq!(age(&m), 0.);
    assert_eq!(count(&m, "published"), 1.);
}
