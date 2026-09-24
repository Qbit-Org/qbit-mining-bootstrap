use super::{
    contract,
    registry::{running_scrape, sample, state},
};
use qbit_prism_server::{
    api::router,
    metrics::{
        AckResult, Collector, ConnectionRefusalReason, DatabaseMetrics, DeliveryMetrics, LockKind,
        Metrics, Outcome, ProcessMetrics, RefreshAcquisition, RefreshTrigger, RejectReason,
        StaleJobCause, TaskKind, WindowAcquisition,
    },
    stratum::StratumStats,
};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn every_http_family_and_closed_label_tuple_stays_bounded_under_varied_inputs() {
    let metrics = Arc::new(Metrics::default());
    let state = state(metrics.clone());
    let startup = running_scrape(router(state.clone()), &[]).await;
    contract::validate(&startup, false).unwrap();
    let startup_census = contract::census(&startup).unwrap();
    assert_eq!(startup_census.families.len(), 56);
    assert_eq!(startup_census.series.len(), 235);
    assert_eq!(sample(&startup, "qbit_prism_runtime_lag_seconds"), -1.);
    assert_eq!(sample(&startup, "qbit_prism_block_candidates_pending"), -1.);
    assert_eq!(
        sample(
            &startup,
            "qbit_prism_collector_success{collector=\"database\"}"
        ),
        -1.
    );
    // Drive ALL from the public enum to expose any newly added variant, but
    // compare its actual HTTP output with the independently pinned contract.
    for iteration in 0..24 {
        let elapsed = Duration::from_millis(iteration * 1703);
        for outcome in AckResult::ALL {
            metrics.observe_share_ack(*outcome, elapsed);
        }
        for reason in RejectReason::ALL {
            metrics.record_rejection(*reason);
        }
        for value in [
            None,
            Some(""),
            Some("unknown"),
            Some(&format!("job-{iteration}-\"\\\n")),
        ] {
            metrics.record_rejection(RejectReason::from_reason_id(value));
        }
        for reason in ConnectionRefusalReason::ALL {
            metrics.record_connection_refusal(*reason);
        }
        for cause in StaleJobCause::ALL {
            metrics.record_stale_job_rejection(*cause);
        }
        for outcome in Outcome::ALL {
            metrics.observe_pool_acquire(*outcome, elapsed);
            for lock in LockKind::ALL {
                metrics.observe_advisory_lock(*lock, *outcome, elapsed);
            }
        }
        metrics.observe_first_offer(elapsed);
        for outcome in WindowAcquisition::ALL {
            metrics.record_window_acquisition(*outcome);
        }
        for trigger in RefreshTrigger::ALL {
            for acquisition in RefreshAcquisition::ALL {
                metrics.observe_refresh(*trigger, *acquisition, elapsed);
            }
        }
        metrics.record_grace_credit();
        metrics.record_late_confirmation();
        metrics.record_candidate_orphaned();
        metrics.set_stratum_connection_limit(iteration as usize);
        let mut snapshot = StratumStats::default().snapshot(iteration);
        snapshot.authorized = iteration as usize;
        snapshot.authorized_with_current_work = iteration as usize / 2;
        metrics.publish_stratum(&snapshot, iteration % 2 == 0, 2, iteration);
        let known = iteration % 3 == 0;
        metrics.publish_delivery(DeliveryMetrics {
            pending_initial_jobs: known.then_some(iteration),
            oldest_initial_job: known.then_some(elapsed),
        });
        metrics.publish_database(known.then_some(DatabaseMetrics {
            candidates: iteration,
            candidate_oldest: elapsed,
            candidate_oldest_unacknowledged: elapsed / 2,
            candidate_oldest_landing_failed: elapsed / 3,
            partition_lead_rows: Some(iteration as i64),
        }));
        metrics.publish_process(known.then_some(ProcessMetrics {
            resident_bytes: iteration,
        }));
        if iteration % 3 == 2 {
            for collector in Collector::ALL {
                drop(metrics.begin_collection(*collector));
            }
        }
        for task in TaskKind::ALL {
            metrics.runtime().track(*task, async {}).await;
            let operation = metrics
                .runtime()
                .start_operation(*task, Duration::from_secs(1));
            operation.progress();
        }
        state.publish_metrics(metrics.render()).unwrap();
        let body = running_scrape(router(state.clone()), &[]).await;
        contract::validate(&body, true).unwrap();
        let populated = contract::census(&body).unwrap();
        assert_eq!(populated.families.len(), 56);
        assert_eq!(populated.series.len(), 669);
        assert_eq!(
            sample(&body, "qbit_prism_block_candidates_pending"),
            if known { iteration as f64 } else { -1. }
        );
        assert_eq!(
            sample(
                &body,
                "qbit_prism_collector_success{collector=\"database\"}"
            ),
            f64::from(known)
        );
    }
}

#[tokio::test]
async fn census_rejects_new_families_labels_types_missing_variants_and_histogram_series() {
    let metrics = Arc::new(Metrics::default());
    let body = running_scrape(router(state(metrics)), &[]).await;
    contract::validate(&body, false).unwrap();
    for changed in [
        format!("{body}# HELP qbit_prism_extra Unexpected.\n# TYPE qbit_prism_extra gauge\nqbit_prism_extra 1\n"),
        body.replace("reason_id=\"unrecognised\"", "reason_id=\"free-form\""),
        body.replace("task=\"refresh\"", "task=\"refresh\",worker=\"miner-secret\""),
        body.replace("qbit_prism_connections gauge", "qbit_prism_connections counter"),
        body.lines().filter(|line| !line.contains("reason_id=\"pool-closed\"")).map(|line| format!("{line}\n")).collect(),
        body.lines().filter(|line| !line.contains("le=\"+Inf\"")).map(|line| format!("{line}\n")).collect(),
        format!("{body}qbit_prism_connections 0\n"),
    ] {
        assert!(contract::validate(&changed, false).is_err());
    }
}

#[test]
fn privacy_controls_distinguish_identifiers_and_height_attribution_from_numeric_coincidence() {
    let identity = "synthetic-miner-identity".to_owned();
    let clean = "qbit_prism_connections 1739\n";
    contract::private_identifiers(clean, std::slice::from_ref(&identity), &[1739]).unwrap();
    contract::height_independent(clean, 1739, "qbit_prism_connections 1739\n", 9281).unwrap();
    for leaked in [
        format!("qbit_prism_connections{{miner=\"{identity}\"}} 1\n"),
        format!("# HELP qbit_prism_connections {identity}\n{clean}"),
        "qbit_prism_connections{height=\"1739\"} 1\n".into(),
        "# HELP qbit_prism_connections tip height 1739\n".into(),
    ] {
        assert!(
            contract::private_identifiers(&leaked, std::slice::from_ref(&identity), &[1739])
                .is_err()
        );
    }
    assert!(
        contract::height_independent(clean, 1739, "qbit_prism_connections 9281\n", 9281).is_err()
    );
}
