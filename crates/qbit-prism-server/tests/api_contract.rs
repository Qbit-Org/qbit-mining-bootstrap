use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use qbit_prism_server::api::{public_service, router, ApiConfig, ApiState};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

fn app() -> Router {
    router(state())
}
fn state() -> ApiState {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://invalid:invalid@127.0.0.1:1/invalid")
        .unwrap();
    ApiState::new(
        pool,
        ApiConfig::default(),
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
}
async fn get(app: Router, path: &str) -> (StatusCode, axum::http::HeaderMap, Value) {
    let response = app
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, headers, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
async fn validation_preserves_public_and_operator_error_shapes() {
    let cases = [
        (
            "/public/v1/leaderboard?window=24h",
            "window must be one of 3h, reward",
        ),
        (
            "/public/v1/leaderboard?recipient_id=alice",
            "recipient_id requires window=reward",
        ),
        (
            "/public/v1/leaderboard?window=reward&recipient_id=alice&search=alice",
            "search and recipient_id are mutually exclusive",
        ),
        ("/public/v1/blocks?page=0", "page must be >= 1"),
        (
            "/public/v1/blocks?limit=101",
            "limit must be between 1 and 100",
        ),
        (
            "/public/v1/blocks?limit=wat",
            "page and limit must be integers",
        ),
        (
            "/public/v1/hashrate-series?range=3h",
            "range must be one of 1w, 1m, 6m, all",
        ),
        (
            "/public/v1/hashrate-series?bucket=1m",
            "bucket must be one of auto, 5m, 1h, 1d",
        ),
        (
            "/public/v1/hashrate-series?subject=invalid",
            "subject must be pool or miner:{recipient_id}",
        ),
        (
            "/public/v1/artifacts/invalid",
            "artifact sha256 must be 64 hex characters",
        ),
        ("/public/v1/miners/%20", "recipient_id is required"),
        (
            "/public/v1/blocks?chain_state=unknown",
            "chain_state must be one of active, all, reversed",
        ),
        (
            "/public/v1/hashrate-series?view=raw",
            "view must be both or omitted",
        ),
        (
            "/public/v1/hashrate-series?range=1m&bucket=5m",
            "bucket 5m is not allowed for range 1m; allowed: 1h, 1d",
        ),
        (
            "/public/v1/hashrate-series?range=all&bucket=1h",
            "bucket 1h is not allowed for range all; allowed: 1d",
        ),
        (
            "/public/v1/block-markers?range=6m&bucket=5m",
            "bucket 5m is not allowed for range 6m; allowed: 1d",
        ),
        (
            "/public/v1/block-markers?range=3h",
            "range must be one of 1w, 1m, 6m, all",
        ),
    ];
    for (path, message) in cases {
        let (status, headers, body) = get(app(), path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(
            body,
            json!({"schema":"prism.dashboard.error.v1","error":{"code":"bad_request","message":message,"request_id":null}})
        );
    }
    let (status, _, body) = get(app(), "/audit/blocks/nope/bundle").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        json!({"error":"block hash must be 64 hex characters"})
    );
    let (status, _, body) = get(app(), "/public/v1").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
}
#[tokio::test]
async fn cache_etag_and_cors_are_usable_by_dashboards() {
    let app = app();
    let (status, headers, body) = get(app.clone(), "/public/v1/mining-configuration/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["schema"], "prism.dashboard.mining-configuration.v1");
    assert_eq!(
        body["configurations"][0]["stratum_endpoints"][0]["protocol"],
        "stratum_v1"
    );
    assert_eq!(headers["access-control-allow-origin"], "*");
    assert!(headers["cdn-cache-control"]
        .to_str()
        .unwrap()
        .contains("max-age=300"));
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/public/v1/mining-configuration")
                .header("if-none-match", &headers["etag"])
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert!(to_bytes(response.into_body(), 1024)
        .await
        .unwrap()
        .is_empty());
    let response = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/public/v1/blocks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers()["access-control-allow-methods"],
        "GET, HEAD, OPTIONS"
    );
}
#[tokio::test]
async fn health_reads_runtime_snapshot_without_database_access() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://invalid:invalid@127.0.0.1:1/invalid")
        .unwrap();
    let state = ApiState::new(
        pool,
        ApiConfig::default(),
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    );
    let (status, _, body) = get(router(state.clone()), "/healthz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["state"], "starting");
    *state.health.write().unwrap() = json!({"schema":"qbit.prism.audit-health.v1","ok":true});
    assert_eq!(get(router(state), "/healthz").await.0, StatusCode::OK);
}

#[tokio::test]
async fn metrics_freshness_headers_cover_both_roles_before_publication() {
    let (public_app, _) = public_service::router(state(), public_service::ServiceConfig::default());
    for app in [app(), public_app] {
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
            assert_eq!(
                response.headers()["content-type"],
                "text/plain; version=0.0.4"
            );
            assert_eq!(response.headers()["cache-control"], "no-store");
            assert_eq!(response.headers()["x-prism-metrics-state"], "unavailable");
            assert!(!response.headers().contains_key("age"));
            assert!(!response.headers().contains_key("warning"));
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            if method == "HEAD" {
                assert!(bytes.is_empty());
            } else {
                assert!(!bytes.is_empty());
            }
        }
    }
}

#[tokio::test]
async fn metrics_publication_preserves_samples_and_restores_fresh_headers() {
    let state = state();
    state
        .publish_metrics("qbit_prism_health_state 1\nqbit_prism_connections 0\n".into())
        .unwrap();
    let app = router(state);
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
        assert_eq!(response.headers()["x-prism-metrics-state"], "fresh");
        assert!(response.headers()["age"]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .is_ok());
        assert!(!response.headers().contains_key("warning"));
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        if method == "HEAD" {
            assert!(bytes.is_empty());
        } else {
            let body = std::str::from_utf8(&bytes).unwrap();
            assert!(body.starts_with("qbit_prism_health_state 1\nqbit_prism_connections 0\n"));
            assert!(body.contains("qbit_prism_metrics_snapshot_available 1\n"));
            assert!(body.contains("qbit_prism_metrics_snapshot_stale 0\n"));
        }
    }
}

#[test]
fn two_x_fixtures_preserve_versioned_row_shapes_and_chart_counts() {
    let blocks: Value = serde_json::from_str(include_str!(
        "../../../docs/public-dashboard-api/fixtures/blocks-chain-states.json"
    ))
    .unwrap();
    assert_eq!(blocks["schema"], "prism.dashboard.blocks.v2");
    for row in blocks["rows"].as_array().unwrap() {
        assert!(row["chain_state"].is_string());
        assert_eq!(
            row["disconnected_at"].is_null(),
            row["chain_state"] != "reversed"
        );
    }
    let markers: Value = serde_json::from_str(include_str!(
        "../../../docs/public-dashboard-api/fixtures/block-markers.json"
    ))
    .unwrap();
    assert_eq!(markers["schema"], "prism.dashboard.block-markers.v1");
    let mut total = 0;
    for point in markers["points"].as_array().unwrap() {
        let count = point["block_count"].as_u64().unwrap();
        total += count;
        assert_eq!(
            point["blocks"].as_array().unwrap().len(),
            count.min(3) as usize
        );
        assert_eq!(point["truncated"], count > 3);
    }
    assert_eq!(markers["total_blocks"], total);
    let series: Value = serde_json::from_str(include_str!(
        "../../../docs/public-dashboard-api/fixtures/hashrate-series-dual-rate.json"
    ))
    .unwrap();
    assert_eq!(series["schema"], "prism.dashboard.hashrate-series.v2");
    assert_eq!(series["rate_basis"], "accepted_share_difficulty");
    for point in series["points"].as_array().unwrap() {
        assert!(
            point["raw_hashrate_ths"].is_string()
                && point["smoothed_hashrate_ths"].is_string()
                && point["complete"].is_boolean()
        );
        assert!(point.get("hashrate_ths").is_none());
    }
}
