use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use qbit_prism_server::{
    api::{router, ApiConfig, ApiState},
    metrics::{self, AckResult, Metrics, RejectReason},
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
    ApiState::new(pool, ApiConfig::default()).with_metrics(metrics)
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
        metrics.record_rejection(*reason);
    }
    assert_eq!(
        RejectReason::from_reason_id(Some("attacker-controlled")),
        RejectReason::InternalError
    );
    let body = metrics.render();
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
    }));
    let body = metrics.render();
    assert_eq!(sample(&body, "qbit_prism_block_candidates_pending"), 2.);
    assert_eq!(
        sample(&body, "qbit_prism_block_candidate_oldest_pending_seconds"),
        5.
    );
    metrics.publish_database(Some(metrics::DatabaseMetrics::default()));
    let body = metrics.render();
    assert_eq!(sample(&body, "qbit_prism_block_candidates_pending"), 0.);
    assert_eq!(
        sample(&body, "qbit_prism_block_candidate_oldest_pending_seconds"),
        0.
    );
}
