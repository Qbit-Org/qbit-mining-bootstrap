use super::*;
use crate::api::{router, ApiConfig, ApiState};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use serde_json::json;
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
    let expired_at = Instant::now() - health_stale_after() - Duration::from_secs(1);
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
                >= health_stale_after().as_secs()
        );
        let text = body(response).await;
        if method == "HEAD" {
            assert!(text.is_empty());
        } else {
            assert_eq!(sample(&text, "qbit_prism_health_state"), 0.);
            assert_eq!(sample(&text, "qbit_prism_metrics_snapshot_stale"), 1.);
            assert!(
                sample(&text, "qbit_prism_metrics_snapshot_age_seconds")
                    > health_stale_after().as_secs_f64()
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
