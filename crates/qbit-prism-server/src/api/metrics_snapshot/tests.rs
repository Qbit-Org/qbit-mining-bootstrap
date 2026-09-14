use super::*;
use crate::api::{router, ApiConfig, ApiState};
use crate::metrics::{LockKind, Metrics, Outcome, BUCKETS};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{Arc, Barrier},
};
use tower::ServiceExt;

const HEALTHY: &str = "# TYPE qbit_prism_health_state gauge\nqbit_prism_health_state 1\n# TYPE qbit_prism_accepted_shares_total counter\nqbit_prism_accepted_shares_total 42\nqbit_prism_connections 7\n";

async fn body(response: Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

fn sample(body: &str, name: &str) -> f64 {
    let values: Vec<_> = body
        .lines()
        .filter_map(|line| line.strip_prefix(&format!("{name} ")))
        .collect();
    assert_eq!(values.len(), 1, "expected one sample for {name}: {body}");
    values[0].parse().unwrap()
}

fn assert_unique_exposition(body: &str) {
    let (mut help, mut types, mut samples) = (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    for line in body.lines().filter(|line| !line.is_empty()) {
        let (names, entry) = if let Some(entry) = line.strip_prefix("# HELP ") {
            (&mut help, entry)
        } else if let Some(entry) = line.strip_prefix("# TYPE ") {
            (&mut types, entry)
        } else if line.starts_with('#') {
            continue;
        } else {
            (&mut samples, line)
        };
        let name = entry.split_whitespace().next().unwrap();
        assert!(names.insert(name), "duplicate exposition entry: {line}");
    }
}

fn assert_histogram_metadata(body: &str, family: &str) {
    for prefix in [
        format!("# HELP {family} "),
        format!("# TYPE {family} histogram"),
    ] {
        assert_eq!(
            body.lines()
                .filter(|line| line.starts_with(&prefix))
                .count(),
            1,
            "expected one metadata line for {prefix}"
        );
    }
}

fn assert_pool_histogram(body: &str, result: &str, observations: &[(f64, u64)]) {
    const FAMILY: &str = "qbit_prism_database_pool_acquire_seconds";
    let count = observations.iter().map(|(_, count)| count).sum::<u64>();
    let sum = observations
        .iter()
        .map(|(seconds, count)| seconds * *count as f64)
        .sum::<f64>();
    assert_histogram_metadata(body, FAMILY);
    assert_eq!(
        sample(body, &format!("{FAMILY}_count{{result=\"{result}\"}}")),
        count as f64
    );
    assert_eq!(
        sample(body, &format!("{FAMILY}_sum{{result=\"{result}\"}}")),
        sum
    );
    for limit in BUCKETS.iter().copied().chain([f64::INFINITY]) {
        let le = if limit.is_infinite() {
            "+Inf".into()
        } else {
            limit.to_string()
        };
        let expected = observations
            .iter()
            .filter(|(seconds, _)| *seconds <= limit)
            .map(|(_, count)| count)
            .sum::<u64>();
        assert_eq!(
            sample(
                body,
                &format!("{FAMILY}_bucket{{result=\"{result}\",le=\"{le}\"}}")
            ),
            expected as f64
        );
    }
}

#[tokio::test]
async fn stale_snapshot_overlays_new_pool_waits_without_republishing_other_observations() {
    let metrics = Metrics::default();
    metrics.observe_pool_acquire(Outcome::Success, Duration::from_millis(125));
    metrics.observe_pool_acquire(Outcome::Failure, Duration::from_secs(2));
    metrics.observe_first_offer(Duration::from_millis(250));
    metrics.observe_advisory_lock(
        LockKind::Settlement,
        Outcome::Success,
        Duration::from_secs(1),
    );
    let cached = metrics
        .render()
        .replace("qbit_prism_health_state 0\n", "qbit_prism_health_state 1\n")
        .replace(
            "qbit_prism_accepted_shares_total 0\n",
            "qbit_prism_accepted_shares_total 42\n",
        )
        .replace("qbit_prism_connections 0\n", "qbit_prism_connections 7\n");
    let published_at = Instant::now();
    let snapshot = MetricsSnapshot {
        body: cached.clone(),
        published_at: Some(published_at),
    };

    metrics.observe_pool_acquire(Outcome::Success, Duration::from_millis(500));
    metrics.observe_pool_acquire(Outcome::Failure, Duration::from_secs(4));
    metrics.observe_first_offer(Duration::from_secs(1));
    metrics.observe_advisory_lock(
        LockKind::Settlement,
        Outcome::Success,
        Duration::from_secs(2),
    );
    metrics.record_grace_credit();
    metrics.publish_database(None);

    for elapsed in [31, 32] {
        let response = snapshot.clone().response_with_runtime(
            published_at + Duration::from_secs(elapsed),
            Duration::from_secs(15),
            None,
            Some(&metrics),
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-prism-metrics-state"], "stale");
        assert_eq!(response.headers()["age"], elapsed.to_string());
        assert!(response.headers().contains_key("warning"));
        let text = body(response).await;
        assert_unique_exposition(&text);
        for (name, expected) in [
            ("health_state", 0.),
            ("metrics_snapshot_available", 1.),
            ("metrics_snapshot_stale", 1.),
            ("metrics_snapshot_age_seconds", elapsed as f64),
            ("accepted_shares_total", 42.),
            ("connections", 7.),
            ("grace_credited_shares_total", 0.),
            ("block_candidates_pending", -1.),
            ("collector_available{collector=\"database\"}", 0.),
            ("collector_success{collector=\"database\"}", 0.),
            ("block_submit_seconds_count", 1.),
            ("block_submit_seconds_sum", 0.25),
            (
                "database_advisory_lock_wait_seconds_count{lock=\"settlement\",result=\"success\"}",
                1.,
            ),
            (
                "database_advisory_lock_wait_seconds_sum{lock=\"settlement\",result=\"success\"}",
                1.,
            ),
        ] {
            assert_eq!(sample(&text, &format!("qbit_prism_{name}")), expected);
        }
        let mut successes = vec![(0.125, 1), (0.5, 1)];
        if elapsed == 32 {
            successes.push((1., 1));
        }
        assert_pool_histogram(&text, "success", &successes);
        assert_pool_histogram(&text, "failure", &[(2., 1), (4., 1)]);
        metrics.observe_pool_acquire(Outcome::Success, Duration::from_secs(1));
    }
    assert_eq!(
        snapshot.body, cached,
        "scraping must not mutate the stored publication"
    );
    assert_eq!(snapshot.published_at, Some(published_at));
}

#[tokio::test]
async fn unpublished_snapshot_exposes_live_pool_waits_without_inventing_lazy_samples() {
    let metrics = Metrics::default();
    let snapshot = MetricsSnapshot::default();
    let lazy = [
        "qbit_prism_block_submit_seconds",
        "qbit_prism_database_advisory_lock_wait_seconds",
    ];
    for observed in [false, true] {
        if observed {
            metrics.observe_pool_acquire(Outcome::Success, Duration::from_millis(125));
            metrics.observe_pool_acquire(Outcome::Failure, Duration::from_secs(3));
        }
        let response = snapshot.clone().response_with_runtime(
            Instant::now() + Duration::from_secs(3600),
            Duration::from_secs(15),
            None,
            Some(&metrics),
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-prism-metrics-state"], "unavailable");
        assert!(!response.headers().contains_key("age"));
        assert!(!response.headers().contains_key("warning"));
        let text = body(response).await;
        assert_unique_exposition(&text);
        for (name, expected) in [
            ("health_state", 0.),
            ("metrics_snapshot_available", 0.),
            ("metrics_snapshot_stale", 1.),
            ("metrics_snapshot_age_seconds", -1.),
            ("block_candidates_pending", -1.),
            ("block_candidate_oldest_pending_seconds", -1.),
            ("collector_available{collector=\"database\"}", 0.),
            ("collector_success{collector=\"database\"}", -1.),
        ] {
            assert_eq!(sample(&text, &format!("qbit_prism_{name}")), expected);
        }
        let count = u64::from(observed);
        assert_pool_histogram(&text, "success", &[(0.125, count)]);
        assert_pool_histogram(&text, "failure", &[(3., count)]);
        for family in lazy {
            assert_histogram_metadata(&text, family);
            assert!(
                !text.lines().any(|line| line.starts_with(family)),
                "unobserved histogram {family} must have no samples"
            );
        }
    }
    // These other histograms remain lazy, but real owner events still render
    // through the existing startup fallback before the first publication.
    metrics.observe_first_offer(Duration::from_millis(250));
    metrics.observe_advisory_lock(LockKind::Order, Outcome::Failure, Duration::from_secs(2));
    let text = body(snapshot.clone().response_with_runtime(
        Instant::now(),
        Duration::from_secs(15),
        None,
        Some(&metrics),
    ))
    .await;
    assert_unique_exposition(&text);
    assert_eq!(sample(&text, "qbit_prism_block_submit_seconds_count"), 1.);
    assert_eq!(sample(&text, "qbit_prism_database_advisory_lock_wait_seconds_count{lock=\"order\",result=\"failure\"}"), 1.);
    assert_eq!(snapshot.published_at, None);
    assert!(snapshot.body.is_empty());
}

#[tokio::test]
async fn concurrent_pool_observations_remain_coherent_across_repeated_stale_scrapes() {
    let metrics = Arc::new(Metrics::default());
    let published_at = Instant::now();
    let snapshot = MetricsSnapshot {
        body: metrics.render(),
        published_at: Some(published_at),
    };
    let start = Arc::new(Barrier::new(2));
    let producer = {
        let metrics = metrics.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            start.wait();
            for index in 0..4096 {
                metrics.observe_pool_acquire(Outcome::Success, Duration::from_millis(125));
                metrics.observe_pool_acquire(Outcome::Failure, Duration::from_secs(2));
                if index % 64 == 0 {
                    std::thread::yield_now();
                }
            }
        })
    };
    start.wait();
    let mut scrapes = Vec::new();
    for _ in 0..32 {
        scrapes.push(
            body(snapshot.clone().response_with_runtime(
                published_at + Duration::from_secs(31),
                Duration::from_secs(15),
                None,
                Some(&metrics),
            ))
            .await,
        );
    }
    producer.join().unwrap();
    scrapes.push(
        body(snapshot.response_with_runtime(
            published_at + Duration::from_secs(32),
            Duration::from_secs(15),
            None,
            Some(&metrics),
        ))
        .await,
    );
    let mut previous = [0, 0];
    for text in scrapes {
        assert_unique_exposition(&text);
        assert_eq!(sample(&text, "qbit_prism_metrics_snapshot_stale"), 1.);
        for (index, (result, seconds)) in [("success", 0.125), ("failure", 2.)]
            .into_iter()
            .enumerate()
        {
            let count = sample(
                &text,
                &format!("qbit_prism_database_pool_acquire_seconds_count{{result=\"{result}\"}}"),
            );
            assert_eq!(count.fract(), 0.);
            let count = count as u64;
            assert!(count >= previous[index] && count <= 4096);
            assert_pool_histogram(&text, result, &[(seconds, count)]);
            previous[index] = count;
        }
        // Each producer iteration records success, then failure. One registry
        // clone must preserve that relationship across the two outcome series.
        assert!(previous[0] == previous[1] || previous[0] == previous[1] + 1);
    }
    assert_eq!(previous, [4096, 4096]);
}

#[tokio::test]
async fn age_uses_monotonic_time_and_exceeds_the_budget_strictly() {
    let published_at = Instant::now();
    let snapshot = MetricsSnapshot {
        body: HEALTHY.into(),
        published_at: Some(published_at),
    };
    // Inject monotonic scrape instants: pausing Tokio cannot age std::Instant.
    // A 30-second budget also guards against hardcoding the 15-second floor.
    let budget = Duration::from_secs(30);
    for (elapsed, stale) in [(0., false), (16., false), (30., false), (30.5, true)] {
        let response = snapshot
            .clone()
            .response(published_at + Duration::from_secs_f64(elapsed), budget);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(response.headers()["age"], (elapsed as u64).to_string());
        assert_eq!(
            response.headers()["x-prism-metrics-state"],
            if stale { "stale" } else { "fresh" }
        );
        assert_eq!(response.headers().contains_key("warning"), stale);
        if stale {
            assert_eq!(
                response.headers()["warning"],
                "110 qbit-prism \"metrics snapshot is stale; serving last complete payload\""
            );
        }
        let text = body(response).await;
        assert_eq!(
            sample(&text, "qbit_prism_health_state"),
            if stale { 0. } else { 1. }
        );
        assert_eq!(sample(&text, "qbit_prism_metrics_snapshot_available"), 1.);
        assert_eq!(
            sample(&text, "qbit_prism_metrics_snapshot_stale"),
            f64::from(u8::from(stale))
        );
        assert_eq!(
            sample(&text, "qbit_prism_metrics_snapshot_age_seconds"),
            elapsed
        );
        assert_eq!(sample(&text, "qbit_prism_accepted_shares_total"), 42.);
        assert_eq!(sample(&text, "qbit_prism_connections"), 7.);
        for name in ["available", "stale", "age_seconds"] {
            assert!(text.contains(&format!("# HELP qbit_prism_metrics_snapshot_{name} ")));
            assert!(text.contains(&format!(
                "# TYPE qbit_prism_metrics_snapshot_{name} gauge\n"
            )));
        }
    }
    assert_eq!(
        snapshot.body, HEALTHY,
        "a stale scrape must not change the stored body"
    );
}

#[tokio::test]
async fn missing_snapshot_is_unavailable_even_after_the_budget() {
    let response = MetricsSnapshot::default().response(
        Instant::now() + Duration::from_secs(3600),
        Duration::from_secs(15),
    );
    assert_eq!(response.headers()["x-prism-metrics-state"], "unavailable");
    assert!(!response.headers().contains_key("age"));
    assert!(!response.headers().contains_key("warning"));
    let text = body(response).await;
    assert_eq!(sample(&text, "qbit_prism_metrics_snapshot_available"), 0.);
    assert_eq!(sample(&text, "qbit_prism_metrics_snapshot_stale"), 1.);
    assert_eq!(
        sample(&text, "qbit_prism_metrics_snapshot_age_seconds"),
        -1.
    );
}

#[tokio::test]
async fn expired_publication_fails_closed_through_the_router_and_recovers() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
        .unwrap();
    let state = ApiState::new(
        pool,
        ApiConfig::default(),
        std::sync::Arc::new(crate::metrics::Metrics::default()),
    );
    state.publish_health(json!({"ok":true}));
    state.publish_metrics(HEALTHY.into()).unwrap();
    let expired_at = Instant::now() - state.config.health_stale_after() - Duration::from_secs(1);
    *state.health_published_at.write().unwrap() = expired_at;
    state.metrics.write().unwrap().published_at = Some(expired_at);
    let app = router(state.clone());
    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::SERVICE_UNAVAILABLE);
    for method in ["GET", "HEAD"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-prism-metrics-state"], "stale");
        assert!(response.headers().contains_key("warning"));
        assert!(
            response.headers()["age"]
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                >= state.config.health_stale_after().as_secs()
        );
        let text = body(response).await;
        if method == "HEAD" {
            assert!(text.is_empty());
        } else {
            assert_eq!(sample(&text, "qbit_prism_health_state"), 0.);
            assert_eq!(sample(&text, "qbit_prism_metrics_snapshot_stale"), 1.);
            assert!(
                sample(&text, "qbit_prism_metrics_snapshot_age_seconds")
                    > state.config.health_stale_after().as_secs_f64()
            );
        }
    }
    state
        .publish_metrics(HEALTHY.replace("total 42", "total 43"))
        .unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["x-prism-metrics-state"], "fresh");
    assert!(!response.headers().contains_key("warning"));
    let text = body(response).await;
    assert_eq!(sample(&text, "qbit_prism_health_state"), 1.);
    assert_eq!(sample(&text, "qbit_prism_metrics_snapshot_stale"), 0.);
    assert_eq!(sample(&text, "qbit_prism_accepted_shares_total"), 43.);
}
