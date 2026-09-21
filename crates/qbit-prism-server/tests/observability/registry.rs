use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use qbit_prism_server::{
    api::{router, ApiConfig, ApiState},
    metrics::{self, AckResult, ConnectionRefusalReason, Metrics, RejectReason, StaleJobCause},
};
use sqlx::postgres::PgPoolOptions;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tower::ServiceExt;

pub(super) fn state(metrics: Arc<Metrics>) -> ApiState {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
        .unwrap();
    ApiState::new(pool, ApiConfig::default(), metrics)
}
pub(super) async fn scrape(state: &ApiState) -> String {
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    String::from_utf8(
        to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

/// Exercise the same role routers over a real, ephemeral HTTP listener. Public
/// counters with lazy label sets need a request before their first sample.
pub(super) async fn running_scrape(app: axum::Router, warmup: &[&str]) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    for path in warmup {
        client
            .get(format!("http://{address}{path}"))
            .send()
            .await
            .unwrap();
    }
    let response = client
        .get(format!("http://{address}/metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    server.abort();
    let _ = server.await;
    body
}
pub(super) fn sample(body: &str, key: &str) -> f64 {
    let values: Vec<_> = body
        .lines()
        .filter_map(|line| line.strip_prefix(&format!("{key} ")))
        .collect();
    assert_eq!(values.len(), 1, "expected unique sample: {key}");
    values[0].parse().unwrap()
}

#[tokio::test]
async fn idle_http_scrape_has_unique_help_and_type_for_every_family() {
    let metrics = Arc::new(Metrics::default());
    let state = state(metrics.clone());
    state.publish_metrics(metrics.render()).unwrap();
    let body = scrape(&state).await;
    assert_complete_registry(&body);
}

#[tokio::test]
async fn startup_scrape_renders_registry_without_fabricating_publication() {
    let metrics = Arc::new(Metrics::default());
    let state = state(metrics.clone());
    for published in [false, true] {
        if published {
            state.publish_metrics(metrics.render()).unwrap();
        }
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(
            response.headers()["x-prism-metrics-state"],
            if published { "fresh" } else { "unavailable" }
        );
        assert_eq!(response.headers().contains_key("age"), published);
        let body = String::from_utf8(
            to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert_complete_registry(&body);
        assert_eq!(sample(&body, "qbit_prism_health_state"), 0.);
        assert_eq!(sample(&body, "qbit_prism_accepted_shares_total"), 0.);
        assert_eq!(
            sample(&body, "qbit_prism_stratum_pending_initial_jobs"),
            -1.
        );
        assert_eq!(sample(&body, "qbit_prism_block_candidates_pending"), -1.);
        assert_eq!(
            sample(&body, "qbit_prism_metrics_snapshot_available"),
            f64::from(published)
        );
        assert_eq!(
            sample(&body, "qbit_prism_metrics_snapshot_stale"),
            f64::from(!published)
        );
        let age = sample(&body, "qbit_prism_metrics_snapshot_age_seconds");
        assert!(if published { age >= 0. } else { age == -1. });
        for deferred in [
            "qbit_prism_block_submit_seconds",
            "qbit_prism_database_advisory_lock_wait_seconds",
        ] {
            assert!(!body.lines().any(|line| line.starts_with(deferred)));
        }
    }
}

pub(super) fn assert_complete_registry(body: &str) {
    let expected: BTreeSet<_> = metrics::descriptors().map(|d| d.name).collect();
    let mut helps = BTreeMap::new();
    let mut types = BTreeMap::new();
    let mut samples = BTreeSet::new();
    for line in body.lines() {
        if let Some(line) = line.strip_prefix("# HELP ") {
            *helps
                .entry(line.split_whitespace().next().unwrap())
                .or_insert(0) += 1;
        } else if let Some(line) = line.strip_prefix("# TYPE ") {
            *types
                .entry(line.split_whitespace().next().unwrap())
                .or_insert(0) += 1;
        } else if !line.starts_with('#') {
            assert!(
                samples.insert(line.split_whitespace().next().unwrap()),
                "duplicate sample {line}"
            );
        }
    }
    assert_eq!(expected, helps.keys().copied().collect());
    assert_eq!(expected, types.keys().copied().collect());
    assert!(helps
        .values()
        .chain(types.values())
        .all(|count| *count == 1));
}

#[test]
fn histogram_buckets_are_cumulative_and_rejects_are_closed() {
    let metrics = Metrics::default();
    for millis in [5, 25, 260, 35_000] {
        metrics.observe_share_ack(AckResult::Accepted, Duration::from_millis(millis));
    }
    for reason in RejectReason::ALL {
        let normalized = RejectReason::from_reason_id(Some(reason.as_str()));
        assert_eq!(normalized, *reason);
        metrics.record_rejection(normalized);
    }
    assert_eq!(
        RejectReason::from_reason_id(Some("attacker-controlled")),
        RejectReason::Unrecognised
    );
    let body = metrics.render();
    assert_eq!(sample(&body, "qbit_prism_stale_shares_total"), 2.);
    assert_eq!(sample(&body, "qbit_prism_duplicate_shares_total"), 1.);
    assert_eq!(sample(&body, "qbit_prism_low_difficulty_shares_total"), 1.);
    let prefix = "qbit_prism_share_ack_seconds";
    assert_eq!(
        sample(&body, &format!("{prefix}_count{{result=\"accepted\"}}")),
        4.
    );
    assert_eq!(
        sample(
            &body,
            &format!("{prefix}_bucket{{result=\"accepted\",le=\"0.025\"}}")
        ),
        2.
    );
    assert_eq!(
        sample(
            &body,
            &format!("{prefix}_bucket{{result=\"accepted\",le=\"+Inf\"}}")
        ),
        4.
    );
    let mut previous = 0.;
    for bucket in metrics::BUCKETS {
        let value = sample(
            &body,
            &format!("{prefix}_bucket{{result=\"accepted\",le=\"{bucket}\"}}"),
        );
        assert!(value >= previous && value <= 4.);
        previous = value;
    }
    let reasons: BTreeSet<_> = body
        .lines()
        .filter(|line| line.starts_with("qbit_prism_rejections_total{"))
        .map(|line| line.split('"').nth(1).unwrap())
        .collect();
    assert_eq!(
        reasons,
        RejectReason::ALL.iter().map(|r| r.as_str()).collect()
    );
}

#[tokio::test]
async fn ack_deadline_buckets_resolve_both_edges_without_changing_other_histograms() {
    use metrics::{LockKind, Outcome};

    let metrics = Arc::new(Metrics::default());
    // On either side of each edge, at each edge, and beyond the finite ladder.
    let millis = [14_999, 15_000, 15_001, 19_999, 20_000, 20_001, 35_000];
    for millis in millis {
        let elapsed = Duration::from_millis(millis);
        for result in AckResult::ALL {
            metrics.observe_share_ack(*result, elapsed);
        }
        metrics.observe_first_offer(elapsed);
        for result in Outcome::ALL {
            metrics.observe_pool_acquire(*result, elapsed);
            for lock in LockKind::ALL {
                metrics.observe_advisory_lock(*lock, *result, elapsed);
            }
        }
    }
    let state = state(metrics.clone());
    state.publish_metrics(metrics.render()).unwrap();
    let body = scrape(&state).await;
    assert_complete_registry(&body);
    let mut histograms = 0;
    for line in body.lines().filter(|line| {
        (line.contains("_count{") || line.starts_with("qbit_prism_block_submit_seconds_count "))
            && !line.starts_with("qbit_prism_accepted_block_to_revision_work_seconds_")
    }) {
        let (key, _) = line.rsplit_once(' ').unwrap();
        let (prefix, labels) = key.split_once("_count").unwrap();
        let is_ack = prefix == "qbit_prism_share_ack_seconds";
        let labels = labels.trim_matches(['{', '}']);
        let labels = if labels.is_empty() {
            String::new()
        } else {
            format!("{labels},")
        };
        let mut expected = vec![
            ("0.01", 0.),
            ("0.025", 0.),
            ("0.05", 0.),
            ("0.1", 0.),
            ("0.25", 0.),
            ("0.5", 0.),
            ("1", 0.),
            ("2.5", 0.),
            ("5", 0.),
            ("10", 0.),
            ("30", 6.),
            ("+Inf", 7.),
        ];
        if is_ack {
            expected.extend([("15", 2.), ("20", 5.)]);
        }
        let bucket_prefix = format!("{prefix}_bucket{{{labels}le=");
        assert_eq!(
            body.lines()
                .filter(|line| line.starts_with(&bucket_prefix))
                .count(),
            expected.len()
        );
        for (bound, count) in expected {
            assert_eq!(
                sample(&body, &format!("{bucket_prefix}\"{bound}\"}}")),
                count
            );
        }
        assert_eq!(sample(&body, key), 7.);
        assert!((sample(&body, &key.replace("_count", "_sum")) - 140.).abs() < 1e-9);
        histograms += 1;
    }
    // Two ACK outcomes, first offer, two pool outcomes, six lock/outcome pairs.
    assert_eq!(histograms, 11);
}

/// Label pairs of every sample in `family`, which must be a closed label set.
fn label_pairs(body: &str, family: &str) -> BTreeSet<String> {
    body.lines()
        .filter(|line| line.starts_with(&format!("{family}{{")))
        .map(|line| {
            line.split_once('{')
                .unwrap()
                .1
                .split_once('}')
                .unwrap()
                .0
                .to_owned()
        })
        .collect()
}

#[test]
fn refusal_and_cause_labels_are_closed_and_do_not_touch_share_rejections() {
    let metrics = Metrics::default();
    let body = metrics.render();
    let refusals = "qbit_prism_stratum_connection_refusals_total";
    let causes = "qbit_prism_stale_job_rejections_total";
    let reasons: BTreeSet<_> = ConnectionRefusalReason::ALL
        .iter()
        .map(|reason| format!("reason=\"{}\"", reason.as_str()))
        .collect();
    let stale: BTreeSet<_> = StaleJobCause::ALL
        .iter()
        .map(|cause| format!("cause=\"{}\"", cause.as_str()))
        .collect();
    assert_eq!(label_pairs(&body, refusals), reasons);
    assert_eq!(label_pairs(&body, causes), stale);
    for pair in reasons.iter().map(|pair| format!("{refusals}{{{pair}}}")) {
        assert_eq!(sample(&body, &pair), 0.);
    }
    for pair in stale.iter().map(|pair| format!("{causes}{{{pair}}}")) {
        assert_eq!(sample(&body, &pair), 0.);
    }
    assert_eq!(sample(&body, "qbit_prism_stratum_connection_limit"), -1.);

    metrics.set_stratum_connection_limit(3);
    for reason in ConnectionRefusalReason::ALL {
        metrics.record_connection_refusal(*reason);
    }
    for cause in StaleJobCause::ALL {
        metrics.record_stale_job_rejection(*cause);
    }
    let body = metrics.render();
    assert_eq!(
        label_pairs(&body, refusals),
        reasons,
        "identity-free labels"
    );
    assert_eq!(label_pairs(&body, causes), stale, "identity-free labels");
    for pair in reasons.iter().map(|pair| format!("{refusals}{{{pair}}}")) {
        assert_eq!(sample(&body, &pair), 1.);
    }
    for pair in stale.iter().map(|pair| format!("{causes}{{{pair}}}")) {
        assert_eq!(sample(&body, &pair), 1.);
    }
    assert_eq!(sample(&body, "qbit_prism_stratum_connection_limit"), 3.);
    // Neither refusals nor causes are share rejections of their own.
    for reason in RejectReason::ALL {
        let key = format!(
            "qbit_prism_rejections_total{{reason_id=\"{}\"}}",
            reason.as_str()
        );
        assert_eq!(sample(&body, &key), 0.);
    }
    assert_eq!(sample(&body, "qbit_prism_stale_shares_total"), 0.);
    assert_eq!(sample(&body, "qbit_prism_rejected_shares_total"), 0.);
}

#[tokio::test]
async fn process_collector_reads_real_input_and_distinguishes_failure_from_zero() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("fd")).unwrap();
    std::fs::write(
        directory.path().join("status"),
        "Name:\tprism\nVmRSS:\t0 kB\nThreads:\t2\n",
    )
    .unwrap();
    let metrics = Arc::new(Metrics::default());
    let state = state(metrics.clone());
    let publish = || {
        metrics.publish_process(metrics::collectors::process(directory.path()).ok());
        state.publish_metrics(metrics.render()).unwrap();
    };
    publish();
    let zero = scrape(&state).await;
    assert_eq!(
        sample(&zero, "qbit_prism_process_resident_memory_bytes"),
        0.
    );
    assert_eq!(
        sample(&zero, "qbit_prism_collector_success{collector=\"process\"}"),
        1.
    );
    std::fs::write(
        directory.path().join("status"),
        "VmRSS:\t4096 kB\nThreads:\t3\n",
    )
    .unwrap();
    std::fs::write(directory.path().join("fd/1"), "").unwrap();
    publish();
    let moved = scrape(&state).await;
    assert_eq!(
        sample(&moved, "qbit_prism_process_resident_memory_bytes"),
        4_194_304.
    );
    std::fs::write(
        directory.path().join("status"),
        "VmRSS:\tbad kB\nThreads:\t3\n",
    )
    .unwrap();
    publish();
    let failed = scrape(&state).await;
    assert_eq!(
        sample(&failed, "qbit_prism_process_resident_memory_bytes"),
        -1.
    );
    assert_eq!(
        sample(
            &failed,
            "qbit_prism_collector_available{collector=\"process\"}"
        ),
        0.
    );
    assert_eq!(
        sample(
            &failed,
            "qbit_prism_collector_success{collector=\"process\"}"
        ),
        0.
    );
    assert!(
        sample(
            &failed,
            "qbit_prism_collector_age_seconds{collector=\"process\"}"
        ) >= 0.
    );
}

#[test]
fn pending_candidate_count_and_age_move_together_without_database_prerequisites() {
    let metrics = Metrics::default();
    metrics.publish_database(Some(metrics::DatabaseMetrics {
        candidates: 2,
        candidate_oldest: Duration::from_secs(5),
        partition_lead_rows: Some(67_108_864),
    }));
    let body = metrics.render();
    assert_eq!(sample(&body, "qbit_prism_block_candidates_pending"), 2.);
    assert_eq!(
        sample(&body, "qbit_prism_block_candidate_oldest_pending_seconds"),
        5.
    );
    assert_eq!(
        sample(&body, "qbit_prism_share_ledger_partition_lead_rows"),
        67_108_864.
    );
    metrics.publish_database(Some(metrics::DatabaseMetrics::default()));
    let body = metrics.render();
    assert_eq!(sample(&body, "qbit_prism_block_candidates_pending"), 0.);
    assert_eq!(
        sample(&body, "qbit_prism_block_candidate_oldest_pending_seconds"),
        0.
    );
    // An unpartitioned ledger has no lead to report, and a successful
    // collection must not turn that into exhausted headroom.
    assert_eq!(
        sample(&body, "qbit_prism_share_ledger_partition_lead_rows"),
        -1.
    );
}
