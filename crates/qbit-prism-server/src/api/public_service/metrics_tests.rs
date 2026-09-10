use super::*;
use axum::{body::to_bytes, http::Request};
use tower::ServiceExt;

#[tokio::test]
async fn metrics_headers_follow_probe_age_not_database_success_or_wall_time() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
        .unwrap();
    let (app, service) = router(
        ApiState::new(pool, ApiConfig::default()),
        ServiceConfig {
            probe_interval: Duration::from_secs(10),
            ..Default::default()
        },
    );
    // Public freshness must retain its own 30-second probe budget. A fresh
    // failure reports ledger_ready=0 without being mislabeled stale/unavailable.
    for (probe_age, ready, expected_state, ledger_ready) in [
        (None, false, "unavailable", 0),
        (Some(0), false, "fresh", 0),
        (Some(16), true, "fresh", 1),
        (Some(31), true, "stale", 0),
    ] {
        let checked = probe_age.map(|age| Instant::now() - Duration::from_secs(age));
        // checked_at is the displayed wall time. Moving it in either direction
        // must not affect the monotonic probe age or freshness classification.
        for wall_time in ["1970-01-01T00:00:00Z", "2100-01-01T00:00:00Z"] {
            *service.snapshot.write().unwrap() = ProbeSnapshot {
                ready,
                checked,
                checked_at: checked.map(|_| wall_time.into()),
                last_error: (!ready).then(|| "database unavailable".into()),
                ..Default::default()
            };
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
                assert_eq!(response.headers()["cache-control"], "no-store");
                assert_eq!(response.headers()["x-prism-metrics-state"], expected_state);
                assert_eq!(
                    response.headers().contains_key("warning"),
                    expected_state == "stale"
                );
                if expected_state == "stale" {
                    assert_eq!(response.headers()["warning"], "110 qbit-prism \"metrics snapshot is stale; serving last complete payload\"");
                }
                let header_age = response
                    .headers()
                    .get("age")
                    .map(|value| value.to_str().unwrap().parse::<u64>().unwrap());
                match (probe_age, header_age) {
                    (Some(age), Some(reported)) => {
                        assert!(reported >= age);
                        assert!(reported <= checked.unwrap().elapsed().as_secs());
                    }
                    (None, None) => {}
                    _ => panic!("Age must be present exactly when the probe age is known"),
                }
                let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
                if method == "HEAD" {
                    assert!(bytes.is_empty());
                } else {
                    let body = std::str::from_utf8(&bytes).unwrap();
                    assert!(
                        body.contains(&format!("qbit_prism_public_ledger_ready {ledger_ready}\n"))
                    );
                    let reported_age: f64 = body
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("qbit_prism_public_ledger_probe_age_seconds ")
                        })
                        .unwrap()
                        .parse()
                        .unwrap();
                    match header_age {
                        Some(age) => assert_eq!(reported_age as u64, age),
                        None => assert_eq!(reported_age, -1.),
                    }
                }
            }
        }
    }
}
