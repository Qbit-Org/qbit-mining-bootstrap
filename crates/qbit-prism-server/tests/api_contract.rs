use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use qbit_prism_server::api::{router, ApiConfig, ApiState};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

fn app() -> Router {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://invalid:invalid@127.0.0.1:1/invalid")
        .unwrap();
    router(ApiState::new(pool, ApiConfig::default()))
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
    let state = ApiState::new(pool, ApiConfig::default());
    let (status, _, body) = get(router(state.clone()), "/healthz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["state"], "starting");
    *state.health.write().unwrap() = json!({"schema":"qbit.prism.audit-health.v1","ok":true});
    assert_eq!(get(router(state), "/healthz").await.0, StatusCode::OK);
}
