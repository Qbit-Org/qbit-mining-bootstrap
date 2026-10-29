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
        let settlement = m.revision_work_settlement(&hash);
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
        settlement.committed(true, 0, 1);
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
    m.revision_work_settlement(&hash).committed(false, 2, 2);
    m.revision_work_delivered(2);
    assert_eq!(count(&m, "published"), 0.);
    assert_eq!(count(&m, "superseded"), 0.);
    settlement.committed(true, 0, 1);
    assert_eq!(count(&m, "superseded"), 1.);
    assert_eq!(age(&m), 0.);
}

#[tokio::test(start_paused = true)]
async fn first_confirmation_wins_even_when_its_observer_started_second() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    let reconciliation = m.revision_work_settlement(&hash);
    m.revision_work_settlement(&hash).committed(true, 6, 7);
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
        // Only an observer still out keeps the history its revision may
        // need, so only it can exhaust that history (#758).
        m.accepted_block(&hash, 1);
        let settlement = m.revision_work_settlement(&hash);
        for revision in 1..=LIMIT as i64 {
            m.revision_work_observed(revision);
        }
        if overflow {
            m.revision_work_observed(LIMIT as i64 + 1);
        }
        settlement.committed(true, 0, 1);
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
async fn lost_settlement_revision_is_unknown_until_a_first_confirmation_or_a_covering_write() {
    for (first, revision) in [(true, 3), (false, 3), (false, 4)] {
        let m = Metrics::default();
        let hash = "11".repeat(32);
        m.accepted_block(&hash, 1);
        drop(m.revision_work_settlement(&hash));
        assert_eq!(age(&m), -1.);
        m.observed_landed_block(&hash, 3);
        m.revision_work_delivered(3);
        assert_eq!(age(&m), -1.);
        assert_eq!(count(&m, "published"), 0.);
        m.revision_work_settlement(&hash)
            .committed(first, revision - i64::from(first), revision);
        // Only a proven first confirmation restores the original revision
        // and its sample. A non-first one proves `revision` covers the
        // landing, so a write at or above it ends the wait unmeasured (#758).
        assert_eq!(count(&m, "published"), f64::from(first));
        if revision == 4 {
            assert_eq!((age(&m), unknown(&m)), (-1., 1.));
            m.revision_work_delivered(4);
        }
        assert_eq!((age(&m), unknown(&m)), (0., 0.));
        assert_eq!(count(&m, "published"), f64::from(first));
        assert_eq!(count(&m, "superseded"), 0.);
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
    m.revision_work_settlement(&hash).committed(true, 2, 3);
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
    // Lost ordering history: the known wait can no longer be resolved. Only
    // an observer still out keeps the history that can run out (#758).
    let m = Metrics::default();
    m.accepted_block(&hash, 1);
    let settlement = m.revision_work_settlement(&hash);
    for revision in 1..=LIMIT as i64 + 1 {
        m.revision_work_observed(revision);
    }
    settlement.committed(true, 0, 1);
    assert!(m.landing.lock().unwrap().ordering_lost);
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
    m.revision_work_settlement(&hash).committed(true, 2, 3); // Recovery mid-build.
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
            settlement.committed(true, 2, 3);
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

#[tokio::test(start_paused = true)]
async fn deadline_attributed_to_a_known_wait_does_not_degrade_an_unlanded_block() {
    // Review L1 on #503: known wait X attributes the deadline; winning block
    // Y is still unlanded (its observation has not happened yet). Y lands
    // later and publishes: its sample is published, not degraded.
    let m = Metrics::default();
    let x = "11".repeat(32);
    let y = "22".repeat(32);
    m.accepted_block(&x, 1);
    m.landed_block(&x, 7);
    m.accepted_unlanded_block(&y, 2);
    let build = m.revision_work_build();
    tick().await;
    build.deadline_hit();
    assert_eq!(timeouts(&m), 1.);
    m.revision_work_delivered(7);
    assert_eq!(count(&m, "degraded"), 1., "X's own delivery is degraded");
    m.accepted_block(&y, 2);
    m.landed_block(&y, 8);
    tick().await;
    m.revision_work_delivered(8);
    assert_eq!(count(&m, "degraded"), 1.);
    assert_eq!(
        count(&m, "published"),
        1.,
        "the unlanded block was degraded by a deadline that fired before it landed"
    );
    assert_eq!(age(&m), 0.);
}

#[tokio::test(start_paused = true)]
async fn waits_no_write_closes_cannot_exhaust_revision_history_and_hide_a_stall() {
    // An idle backup (#758): a block it lost to a peer, or one it won, stays
    // open with no miner to write to while the payout revision keeps moving.
    for lost in [true, false] {
        let m = Metrics::default();
        let pinned = "11".repeat(32);
        m.accepted_block(&pinned, 1);
        m.revision_work_settlement(&pinned)
            .committed(!lost, 2 - i64::from(!lost), 2);
        assert_eq!(unknown(&m), f64::from(lost));
        for revision in 3..=LIMIT as i64 + 10 {
            m.revision_work_observed(revision);
        }
        let stall = "22".repeat(32);
        m.accepted_block(&stall, 2);
        m.revision_work_settlement(&stall).committed(
            true,
            LIMIT as i64 + 11 - 1,
            LIMIT as i64 + 11,
        );
        tokio::time::advance(Duration::from_secs(308)).await;
        assert_eq!(age(&m), 308., "a real stall must still reach the page");
        let state = m.landing.lock().unwrap();
        assert!(!state.saturated && !state.ordering_lost);
        assert_eq!(state.revisions.len(), 1);
        drop(state);
        // The first write once a miner connects ends every wait.
        m.revision_work_delivered(LIMIT as i64 + 11);
        assert_eq!((age(&m), unknown(&m)), (0., 0.));
        assert_eq!(count(&m, "published"), 1.);
        assert_eq!(count(&m, "superseded"), f64::from(!lost));
    }
}

#[tokio::test(start_paused = true)]
async fn a_frontend_that_loses_every_settlement_race_clears_unknown_at_its_next_write() {
    // Active-active (#758): a peer settles each block first; this frontend's
    // own COMMIT follows, and its miners keep receiving work.
    let m = Metrics::default();
    for block in 0..20_i64 {
        let hash = format!("{block:064x}");
        let covering = 2 * block + 2;
        m.accepted_block(&hash, block as u64 + 1);
        tick().await;
        if block % 2 == 1 {
            // The write can precede this frontend's COMMIT reply.
            m.revision_work_delivered(covering);
            m.revision_work_settlement(&hash)
                .committed(false, covering, covering);
        } else {
            m.revision_work_settlement(&hash)
                .committed(false, covering, covering);
            assert_eq!((age(&m), unknown(&m)), (-1., 1.));
            m.revision_work_observed(covering + 1);
            m.revision_work_delivered(covering + 1);
        }
        assert_eq!((age(&m), unknown(&m)), (0., 0.), "block {block}");
    }
    for result in ["published", "degraded", "superseded"] {
        assert_eq!(
            count(&m, result),
            0.,
            "an unknown first revision was measured"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_covering_revision_waits_for_a_local_first_confirmation_still_in_flight() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    let cancelled = m.revision_work_settlement(&hash);
    let first = m.revision_work_settlement(&hash);
    drop(cancelled);
    m.revision_work_settlement(&hash).committed(false, 5, 5);
    tick().await;
    m.revision_work_delivered(5);
    assert_eq!((age(&m), unknown(&m)), (-1., 1.));
    // The delayed observer of the first COMMIT restores the exact revision.
    first.committed(true, 3, 4);
    assert_eq!((age(&m), unknown(&m)), (0., 0.));
    assert_eq!(count(&m, "superseded"), 1.);
}

#[tokio::test(start_paused = true)]
async fn only_an_eligible_write_at_the_covering_revision_ends_the_unknown_wait() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    m.revision_work_settlement(&hash).committed(false, 5, 5);
    tick().await;
    m.revision_work_delivered(4);
    m.revision_work_observed(6);
    tick().await;
    m.revision_work_delivered(5);
    assert_eq!((age(&m), unknown(&m)), (-1., 1.), "below or obsolete");
    {
        let _orphan = m.revision_work_orphan_settlement(&hash);
    } // Lost/cancelled orphan COMMIT reply.
    m.revision_work_delivered(6);
    assert_eq!(
        (age(&m), unknown(&m)),
        (-1., 1.),
        "orphan verdict unsettled"
    );
    m.revision_work_orphaned(&hash);
    assert_eq!((age(&m), unknown(&m)), (0., 0.));
    for result in ["published", "degraded", "superseded"] {
        assert_eq!(count(&m, result), 0.);
    }
}

#[tokio::test(start_paused = true)]
async fn a_non_owner_settlement_covers_once_the_owner_reply_is_lost() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    let owner = m.revision_work_settlement(&hash);
    let other = m.revision_work_settlement(&hash);
    drop(owner);
    assert_eq!((age(&m), unknown(&m)), (-1., 1.));
    other.committed(false, 5, 5);
    tick().await;
    m.revision_work_delivered(5);
    assert_eq!((age(&m), unknown(&m)), (0., 0.));
    for result in ["published", "degraded", "superseded"] {
        assert_eq!(count(&m, result), 0.);
    }
}

#[tokio::test(start_paused = true)]
async fn an_already_confirmed_block_whose_reply_is_lost_is_covered_too() {
    // A restart first observes the block already confirmed (#493).
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_landed_block(&hash, 1, false);
    drop(m.revision_work_settlement(&hash));
    assert_eq!((age(&m), unknown(&m)), (-1., 1.));
    m.revision_work_settlement(&hash).committed(false, 5, 5);
    tick().await;
    m.revision_work_delivered(5);
    assert_eq!((age(&m), unknown(&m)), (0., 0.));
    assert_eq!(count(&m, "published"), 0.);
}

#[tokio::test(start_paused = true)]
async fn a_first_confirmation_without_a_bump_is_covered_only_by_a_later_revision() {
    // The fatal early return commits the landing at revision 5 unbumped, so
    // work built at 5 may predate it.
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    m.revision_work_observed(5);
    let fatal = m.revision_work_settlement(&hash);
    tick().await;
    m.revision_work_delivered(5);
    fatal.committed(true, 5, 5);
    assert_eq!(
        (age(&m), unknown(&m)),
        (-1., 1.),
        "an unbumped revision was measured"
    );
    // Not even a later proof at that revision can cover it.
    m.revision_work_settlement(&hash).committed(false, 5, 5);
    tick().await;
    m.revision_work_delivered(5);
    assert_eq!((age(&m), unknown(&m)), (-1., 1.));
    m.revision_work_observed(6);
    m.revision_work_delivered(6);
    assert_eq!((age(&m), unknown(&m)), (0., 0.));
    for result in ["published", "degraded", "superseded"] {
        assert_eq!(count(&m, result), 0.);
    }
}

#[tokio::test(start_paused = true)]
async fn an_unlanded_or_unbound_wait_cannot_exhaust_revision_history() {
    // An idle frontend whose wait is unlanded (a lost tip race until its
    // orphan proof) or landed but not yet settled, while tips move (#758).
    for unlanded in [true, false] {
        let m = Metrics::default();
        let pinned = "11".repeat(32);
        if unlanded {
            m.accepted_unlanded_block(&pinned, 1);
        } else {
            m.accepted_block(&pinned, 1);
        }
        for revision in 1..=LIMIT as i64 + 10 {
            m.revision_work_observed(revision);
        }
        {
            let state = m.landing.lock().unwrap();
            assert!(!state.saturated && !state.ordering_lost);
            assert_eq!(state.revisions.len(), 1);
        }
        let stall = "22".repeat(32);
        m.accepted_block(&stall, 2);
        tokio::time::advance(Duration::from_secs(308)).await;
        m.revision_work_settlement(&stall)
            .committed(true, LIMIT as i64 + 10, LIMIT as i64 + 11);
        assert_eq!(age(&m), 308., "a real stall must still reach the page");
        m.revision_work_delivered(LIMIT as i64 + 11);
        assert_eq!(count(&m, "published"), 1.);
        // The landed wait still awaits its own settlement.
        assert_eq!(age(&m), if unlanded { 0. } else { 308. });
    }
}

#[tokio::test(start_paused = true)]
async fn a_binding_below_released_history_is_covered_not_measured() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    tick().await;
    m.revision_work_delivered(1);
    m.revision_work_observed(2);
    // A first revision bound with no observer out, below released history:
    // its first write may be gone, so it must not be measured.
    m.landed_block(&hash, 1);
    assert_eq!((age(&m), unknown(&m)), (-1., 1.));
    tick().await;
    m.revision_work_delivered(2);
    assert_eq!((age(&m), unknown(&m)), (0., 0.));
    for result in ["published", "degraded", "superseded"] {
        assert_eq!(count(&m, result), 0., "a released write was guessed");
    }
}

#[tokio::test(start_paused = true)]
async fn the_last_observer_out_ends_a_covered_wait_whose_write_was_seen() {
    let m = Metrics::default();
    let hash = "11".repeat(32);
    m.accepted_block(&hash, 1);
    let owner = m.revision_work_settlement(&hash);
    let other = m.revision_work_settlement(&hash);
    owner.committed(false, 5, 5);
    tick().await;
    m.revision_work_delivered(5);
    assert_eq!((age(&m), unknown(&m)), (-1., 1.));
    drop(other);
    assert_eq!(
        (age(&m), unknown(&m)),
        (0., 0.),
        "no later event should be needed"
    );
}

#[tokio::test(start_paused = true)]
async fn an_idle_frontend_cannot_exhaust_identities_and_hide_a_stall_after_failover() {
    // An idle backup (#758) reconciles every pool block with no miner to
    // write to; each wait stays open until the block is mature. After a
    // failover the same frontend serves every miner, so its stall must page.
    const MATURITY: u64 = 100;
    for miner in [false, true] {
        let m = Metrics::default();
        m.revision_work_sessions_tracked();
        if miner {
            m.revision_work_session_authorized();
        }
        for height in 1..=LIMIT as u64 + 1 {
            // A peer settled it first; this frontend's settlement binds its
            // own committed revision (#493).
            let hash = format!("{height:064x}");
            let revision = height as i64;
            m.accepted_landed_block(&hash, height, false);
            m.revision_work_settlement(&hash)
                .committed(false, revision, revision);
            m.revision_work_matured(height.saturating_sub(MATURITY));
        }
        {
            let state = m.landing.lock().unwrap();
            assert_eq!(state.saturated, miner);
            let tracked = if miner { LIMIT } else { MATURITY as usize };
            assert_eq!(state.blocks.len(), tracked);
        }
        if !miner {
            m.revision_work_session_authorized();
        }
        m.revision_work_delivered(LIMIT as i64 + 1);
        let stall = format!("{:064x}", LIMIT + 2);
        m.accepted_landed_block(&stall, LIMIT as u64 + 2, false);
        m.revision_work_settlement(&stall)
            .committed(false, LIMIT as i64 + 2, LIMIT as i64 + 2);
        tokio::time::advance(Duration::from_secs(308)).await;
        if miner {
            // A real stall with a miner connected still saturates, unknown
            // until restart, as before.
            assert_eq!((age(&m), unknown(&m)), (-1., 1.));
        } else {
            assert_eq!(
                (age(&m), unknown(&m)),
                (308., 0.),
                "idle waits latched saturation and hid the stall from the page"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn the_mature_checkpoint_retires_only_waits_no_miner_could_have_received() {
    let m = Metrics::default();
    // Accepted before the listener tracked sessions: a miner may have been
    // there.
    let untracked = "11".repeat(32);
    m.accepted_block(&untracked, 1);
    m.landed_block(&untracked, 1);
    m.revision_work_sessions_tracked();
    // A miner connected after it and left without its work.
    let left = "22".repeat(32);
    m.accepted_block(&left, 1);
    m.landed_block(&left, 1);
    m.revision_work_session_authorized();
    m.revision_work_session_ended();
    // No miner since: a known wait, a lost settlement reply and an unlanded
    // acceptance, which no write ends anyway.
    let idle = "33".repeat(32);
    m.accepted_block(&idle, 1);
    m.landed_block(&idle, 1);
    let lost = "44".repeat(32);
    m.accepted_block(&lost, 1);
    drop(m.revision_work_settlement(&lost));
    let not_landed = "55".repeat(32);
    m.accepted_unlanded_block(&not_landed, 1);
    let immature = "66".repeat(32);
    m.accepted_block(&immature, 2);
    m.landed_block(&immature, 1);
    tick().await;
    assert_eq!((age(&m), unknown(&m)), (1., 1.));
    m.revision_work_matured(1);
    assert_eq!((age(&m), unknown(&m)), (1., 0.));
    assert_eq!(unlanded(&m), 1.);
    assert_eq!(m.landing.lock().unwrap().blocks.len(), 4);
    // A retired identity is never recounted, and retirement measured nothing.
    m.accepted_block(&idle, 1);
    m.landed_block(&idle, 1);
    m.revision_work_session_authorized();
    m.revision_work_delivered(1);
    assert_eq!(count(&m, "published"), 3.);
    for result in ["degraded", "superseded"] {
        assert_eq!(count(&m, result), 0.);
    }
    assert_eq!((age(&m), unknown(&m)), (0., 0.));
    assert_eq!(unlanded(&m), 1.);
}
