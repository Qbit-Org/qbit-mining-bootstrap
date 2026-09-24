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
fn unknown(metrics: &Metrics) -> f64 {
    sample(
        &metrics.render(),
        "qbit_prism_accepted_block_revision_work_tracking_unknown",
    )
}
fn timeouts(metrics: &Metrics) -> f64 {
    sample(
        &metrics.render(),
        "qbit_prism_revision_work_build_timeouts_total",
    )
}
fn unlanded(metrics: &Metrics) -> f64 {
    sample(
        &metrics.render(),
        "qbit_prism_accepted_block_unlanded_seconds",
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
    assert_eq!(age(&m), 1.);
    assert!(m.landing.lock().unwrap().unknown());
    m.revision_work_refresh().succeeded();
    assert_eq!(age(&m), 1.);
    assert!(m.landing.lock().unwrap().unknown());
    let state = m.landing.lock().unwrap();
    assert_eq!(state.blocks.len(), LIMIT);
    drop(state);
    m.accepted_block(&format!("{:064x}", 0), 1);
    m.landed_block(&format!("{:064x}", 0), 1);
    m.revision_work_delivered(1);
    m.accepted_block(&format!("{:064x}", 0), 1);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(age(&m), 1.);
    assert!(m.landing.lock().unwrap().unknown());
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

#[tokio::test(start_paused = true)]
async fn exhausted_revision_history_cannot_publish_an_obsolete_write() {
    for overflow in [false, true] {
        let m = Metrics::default();
        let hash = "11".repeat(32);
        m.accepted_block(&hash, 1);
        m.landed_block(&hash, 1);
        for revision in 2..=LIMIT as i64 {
            m.revision_work_observed(revision);
        }
        if overflow {
            m.revision_work_observed(LIMIT as i64 + 1);
        }
        tick().await;
        m.revision_work_delivered(LIMIT as i64);
        assert_eq!(count(&m, "superseded"), f64::from(!overflow));
        assert_eq!(age(&m), if overflow { -1. } else { 0. });
    }
}

#[tokio::test(start_paused = true)]
async fn unknown_revision_cannot_hide_a_separate_known_stall() {
    let m = Metrics::default();
    let unknown = "11".repeat(32);
    m.accepted_block(&unknown, 1);
    drop(m.revision_work_settlement(&unknown));
    let known = "22".repeat(32);
    m.accepted_block(&known, 2);
    m.landed_block(&known, 7);
    tokio::time::advance(Duration::from_secs(308)).await;
    assert_eq!(age(&m), 308.);
    assert_eq!(
        sample(
            &m.render(),
            "qbit_prism_accepted_block_revision_work_tracking_unknown"
        ),
        1.
    );
    m.revision_work_delivered(7);
    assert_eq!(age(&m), -1.);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(
        sample(
            &m.render(),
            "qbit_prism_accepted_block_revision_work_tracking_unknown"
        ),
        1.
    );
}

#[tokio::test(start_paused = true)]
async fn exhausted_delivery_history_blocks_late_binding_without_losing_safe_samples() {
    let m = Metrics::default();
    for id in 0..LIMIT {
        m.accepted_block(&format!("{id:064x}"), 1);
        tick().await;
        m.revision_work_delivered(7);
    }
    m.landed_block(&format!("{:064x}", LIMIT - 1), 7);
    assert_eq!(count(&m, "published"), 1.);
    m.revision_work_delivered(8);
    m.landed_block(&format!("{:064x}", 0), 7);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(age(&m), -1.);
    let state = m.landing.lock().unwrap();
    assert_eq!(state.delivery_count, LIMIT);
    assert!(state.ordering_lost);
}

#[tokio::test(start_paused = true)]
async fn proven_orphan_closes_without_delivery_and_keeps_its_deduplication_horizon() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    let obsolete = m.revision_work_settlement(&hash);
    tick().await;
    m.revision_work_orphaned(&hash);
    drop(obsolete);
    m.revision_work_orphaned(&hash);
    m.accepted_block(&hash, 1);
    m.landed_block(&hash, 1);
    m.revision_work_delivered(1);
    assert_eq!(age(&m), 0.);
    assert_eq!(count(&m, "published"), 0.);
    assert_eq!(m.landing.lock().unwrap().blocks.len(), 1);
    m.revision_work_matured(1);
    m.accepted_block(&hash, 1);
    assert!(m.landing.lock().unwrap().blocks.is_empty());
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
async fn uncertain_orphan_cannot_be_rebound_or_delivered_without_terminal_proof() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    m.landed_block(&hash, 7);
    tick().await;
    {
        let _orphan = m.revision_work_orphan_settlement(&hash);
        assert_eq!(age(&m), -1.);
    } // Lost/cancelled COMMIT acknowledgement.
    m.landed_block(&hash, 8);
    m.revision_work_delivered(8);
    m.revision_work_terminal_probe().succeeded(&[]);
    assert_eq!(age(&m), -1.);
    assert_eq!(count(&m, "published"), 0.);
    assert_eq!(count(&m, "superseded"), 0.);
    m.revision_work_terminal_probe()
        .succeeded(std::slice::from_ref(&hash));
    assert_eq!(age(&m), 0.);
    m.accepted_block(&hash, 1);
    m.landed_block(&hash, 9);
    m.revision_work_delivered(9);
    assert_eq!(age(&m), 0.);
    assert_eq!(count(&m, "published"), 0.);
    assert_eq!(count(&m, "superseded"), 0.);
}

#[tokio::test(start_paused = true)]
async fn terminal_probe_versions_preserve_newer_failure_and_monotonic_orphan_evidence() {
    let m = Metrics::default();
    let a = "11".repeat(32);
    let b = "22".repeat(32);
    m.accepted_block(&a, 1);
    m.accepted_block(&b, 2);
    tick().await;
    let older = m.revision_work_terminal_probe();
    drop(m.revision_work_terminal_probe());
    older.succeeded(std::slice::from_ref(&a));
    {
        let state = m.landing.lock().unwrap();
        assert!(state.terminal_failed, "old success hid a newer failed read");
        assert_eq!(state.pending_count, 1);
        assert!(state.unknown());
    }
    assert_eq!(age(&m), 1., "unknown evidence hid known B's wait");
    let cancelled_old = m.revision_work_terminal_probe();
    m.revision_work_terminal_probe().succeeded(&[]);
    drop(cancelled_old);
    assert!(!m.landing.lock().unwrap().unknown());
    m.revision_work_terminal_probe().succeeded(&[a, b]);
    assert_eq!(age(&m), 0.);
    assert_eq!(count(&m, "published"), 0.);
}

#[tokio::test(start_paused = true)]
async fn terminal_probe_identity_payload_is_bounded_and_skips_closed_tombstones() {
    let m = Metrics::default();
    for n in 1..=LIMIT + 1 {
        m.accepted_block(&format!("{n:064x}"), 1);
    }
    let first = m.revision_work_terminal_probe();
    assert_eq!(first.hashes.len(), LIMIT);
    let closed = first.hashes[0].clone();
    first.succeeded(std::slice::from_ref(&closed));
    let second = m.revision_work_terminal_probe();
    assert_eq!(second.hashes.len(), LIMIT - 1);
    assert!(!second.hashes.contains(&closed));
    assert!(m.landing.lock().unwrap().saturated);
}

#[tokio::test(start_paused = true)]
async fn lost_settlement_reply_does_not_attribute_later_timeouts_to_revision_work() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    drop(m.revision_work_settlement(&hash)); // Lost/cancelled COMMIT reply.
    assert_eq!(age(&m), -1.);
    assert_eq!(unknown(&m), 1.);
    let build = m.revision_work_build();
    tick().await;
    build.deadline_hit();
    assert_eq!(
        timeouts(&m),
        0.,
        "a deadline hit while only unknown tracking remained was counted as a revision-work failure"
    );
    assert_eq!(age(&m), -1., "unknown tracking was reported as zero");
    assert_eq!(unknown(&m), 1.);
    // A proven first confirmation recovers the attempt. The timed-out build
    // never knew this wait, so the recovered delivery is not degraded either.
    m.revision_work_settlement(&hash).committed(true, 3);
    assert_eq!(age(&m), 1.);
    tick().await;
    m.revision_work_delivered(3);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(count(&m, "degraded"), 0.);
    assert_eq!(timeouts(&m), 0.);
    assert_eq!(age(&m), 0.);
    assert_eq!(unknown(&m), 0.);
}

#[tokio::test(start_paused = true)]
async fn known_wait_beside_unknown_tracking_still_attributes_the_timeout() {
    let m = Metrics::default();
    let lost = "11".repeat(32);
    m.accepted_block(&lost, 1);
    drop(m.revision_work_settlement(&lost));
    let known = "22".repeat(32);
    m.accepted_block(&known, 2);
    m.landed_block(&known, 7);
    let build = m.revision_work_build();
    tick().await;
    build.deadline_hit();
    assert_eq!(timeouts(&m), 1.);
    m.revision_work_delivered(7);
    assert_eq!(count(&m, "degraded"), 1.);
    assert_eq!(count(&m, "published"), 0.);
    assert_eq!(age(&m), -1.);
    assert_eq!(unknown(&m), 1.);
}

#[tokio::test(start_paused = true)]
async fn unsettled_orphan_saturation_and_lost_ordering_do_not_attribute_timeouts() {
    let hash = "11".repeat(32);
    let m = Metrics::default();
    m.accepted_block(&hash, 1);
    m.landed_block(&hash, 7);
    {
        let _orphan = m.revision_work_orphan_settlement(&hash);
    } // Lost/cancelled orphan COMMIT reply.
    assert_eq!(age(&m), -1.);
    let build = m.revision_work_build();
    tick().await;
    build.deadline_hit();
    assert_eq!(timeouts(&m), 0.);
    m.revision_work_terminal_probe()
        .succeeded(std::slice::from_ref(&hash));
    assert_eq!(age(&m), 0.);
    assert_eq!(unknown(&m), 0.);
    assert_eq!(count(&m, "degraded"), 0.);
    // An identity that could not be tracked: no known wait remains.
    let m = Metrics::default();
    m.accepted_block("not a block hash", 1);
    assert_eq!(unknown(&m), 1.);
    let build = m.revision_work_build();
    tick().await;
    build.deadline_hit();
    assert_eq!(timeouts(&m), 0.);
    assert_eq!(age(&m), -1.);
    // Lost ordering history: the known wait can no longer be resolved.
    let m = Metrics::default();
    m.accepted_block(&hash, 1);
    m.landed_block(&hash, 1);
    for revision in 2..=LIMIT as i64 + 1 {
        m.revision_work_observed(revision);
    }
    assert_eq!(age(&m), -1.);
    let build = m.revision_work_build();
    tick().await;
    build.deadline_hit();
    assert_eq!(timeouts(&m), 0.);
    assert_eq!(unknown(&m), 1.);
}

#[tokio::test(start_paused = true)]
async fn recovery_from_unknown_before_the_deadline_attributes_the_timeout() {
    // Reviewer case (PR #500, F1): the wait is unknown at the build's start
    // and a proven first confirmation recovers it before the deadline fires.
    // A known wait accepted before the build is open at that instant, and the
    // build's job would have carried the recovered revision.
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    drop(m.revision_work_settlement(&hash)); // Lost reply.
    assert_eq!(age(&m), -1.);
    let build = m.revision_work_build(); // Only unknown tracking at the start.
    tick().await;
    m.revision_work_settlement(&hash).committed(true, 3); // Recovery mid-build.
    assert_eq!(age(&m), 1., "wait is known and open again");
    tick().await;
    build.deadline_hit();
    assert_eq!(
        timeouts(&m),
        1.,
        "deadline hit while a known wait accepted before the build was open"
    );
    tick().await;
    m.revision_work_delivered(3);
    assert_eq!(count(&m, "degraded"), 1.);
    assert_eq!(count(&m, "published"), 0.);
    assert_eq!(age(&m), 0.);
}

#[tokio::test(start_paused = true)]
async fn lost_race_acceptance_is_unlanded_and_never_a_known_pending_wait() {
    // #493 point 1: a definitive submitblock acceptance without an
    // active-chain observation is not a delivery wait. Its age is reported by
    // the unlanded gauge and can never reach the paging floor.
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_unlanded_block(&hash, 1);
    assert_eq!(age(&m), 0.);
    assert_eq!(unlanded(&m), 0.);
    tokio::time::advance(Duration::from_secs(400)).await;
    assert_eq!(age(&m), 0., "a lost race counted as a known pending wait");
    assert_eq!(unlanded(&m), 400.);
    assert_eq!(unknown(&m), 0., "an unlanded block is a known state");
    // The unlanded acceptance is not a known wait for deadline attribution.
    let build = m.revision_work_build();
    tick().await;
    build.deadline_hit();
    assert_eq!(timeouts(&m), 0.);
    // A revision observed while it is unlanded is not its delivery target.
    m.revision_work_observed(5);
    m.revision_work_delivered(5);
    assert_eq!(count(&m, "published"), 0.);
    assert_eq!(count(&m, "superseded"), 0.);
    // The live overlay reports the same unlanded age on a cached body.
    let mut cached = m.render();
    tick().await;
    m.overlay_live_observations(&mut cached);
    assert_eq!(
        sample(&cached, "qbit_prism_accepted_block_unlanded_seconds"),
        402.
    );
    // The proven orphan closes it without a sample; the tombstone holds.
    m.revision_work_orphaned(&hash);
    assert_eq!(unlanded(&m), 0.);
    assert_eq!(age(&m), 0.);
    assert_eq!(unknown(&m), 0.);
    m.accepted_unlanded_block(&hash, 1);
    m.accepted_block(&hash, 1);
    assert_eq!(unlanded(&m), 0.);
    assert_eq!(age(&m), 0.);
    for result in ["published", "degraded", "superseded"] {
        assert_eq!(count(&m, result), 0.);
    }
    assert_eq!(m.landing.lock().unwrap().blocks.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn active_chain_observation_lands_an_accepted_offer_on_its_acceptance_clock() {
    // A winning offer: the acceptance clock starts at the definitive reply,
    // the wait becomes known at the first active-chain observation, and the
    // histogram measures from the acceptance.
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_unlanded_block(&hash, 1);
    tokio::time::advance(Duration::from_secs(2)).await;
    m.accepted_block(&hash, 1);
    assert_eq!(unlanded(&m), 0.);
    assert_eq!(age(&m), 2., "landing restarted the acceptance clock");
    m.landed_block(&hash, 7);
    tick().await;
    m.revision_work_delivered(7);
    assert_eq!(count(&m, "published"), 1.);
    assert_eq!(
        sample(
            &m.render(),
            "qbit_prism_accepted_block_to_revision_work_seconds_sum{result=\"published\"}"
        ),
        3.
    );
    assert_eq!(age(&m), 0.);
    // A landed block is never downgraded by a later duplicate acceptance.
    let m = Metrics::default();
    m.accepted_block(&hash, 1);
    tick().await;
    m.accepted_unlanded_block(&hash, 1);
    assert_eq!(unlanded(&m), 0.);
    assert_eq!(age(&m), 1.);
    // The settlement observer's own active-chain observation lands it too,
    // whatever its COMMIT reply: a committed first confirmation binds the
    // target, a lost reply leaves the landed wait unknown.
    for lost in [false, true] {
        let m = Metrics::default();
        m.accepted_unlanded_block(&hash, 1);
        tick().await;
        assert_eq!(unlanded(&m), 1.);
        let settlement = m.revision_work_settlement(&hash);
        assert_eq!(unlanded(&m), 0.);
        if lost {
            drop(settlement);
            assert_eq!(age(&m), -1.);
            assert_eq!(unknown(&m), 1.);
        } else {
            settlement.committed(true, 3);
            assert_eq!(age(&m), 1.);
            m.revision_work_delivered(3);
            assert_eq!(count(&m, "published"), 1.);
        }
    }
    // A committed orphan verdict closes an unlanded block armed for it.
    let m = Metrics::default();
    m.accepted_unlanded_block(&hash, 1);
    tick().await;
    {
        let _lost = m.revision_work_orphan_settlement(&hash);
        assert_eq!(unlanded(&m), 1.);
        assert_eq!(age(&m), -1.);
    }
    assert_eq!(unknown(&m), 1.);
    m.revision_work_terminal_probe()
        .succeeded(std::slice::from_ref(&hash));
    assert_eq!(unlanded(&m), 0.);
    assert_eq!(age(&m), 0.);
    assert_eq!(unknown(&m), 0.);
}

#[tokio::test(start_paused = true)]
async fn unlanded_age_reports_saturation_as_unknown_rather_than_zero() {
    let m = Metrics::default();
    assert_eq!(unlanded(&m), 0.);
    m.accepted_block("not a block hash", 1);
    assert_eq!(
        unlanded(&m),
        -1.,
        "saturation was reported as no unlanded block"
    );
    assert_eq!(unknown(&m), 1.);
    // A tracked unlanded age stays visible beside saturation; a known wait is
    // still reported by the pending gauge only.
    let m = Metrics::default();
    m.accepted_unlanded_block(&"11".repeat(32), 1);
    tick().await;
    m.accepted_block(&"22".repeat(32), 2);
    m.landed_block(&"22".repeat(32), 7);
    tick().await;
    m.accepted_block("not a block hash", 3);
    assert_eq!(unlanded(&m), 2.);
    assert_eq!(age(&m), 1.);
    assert_eq!(unknown(&m), 1.);
    m.revision_work_delivered(7);
    assert_eq!(age(&m), -1.);
    assert_eq!(unlanded(&m), 2.);
}
