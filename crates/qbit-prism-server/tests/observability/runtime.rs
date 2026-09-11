use super::registry::{sample, scrape, state};
use qbit_prism_server::{
    api::router,
    metrics::{Metrics, TaskKind},
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_critical_poll_degrades_real_http_while_another_worker_remains_healthy() {
    let metrics = Arc::new(Metrics::default());
    let state = state(metrics.clone());
    state.publish_health(json!({"ok":true,"ready":true,"schema":"qbit.prism.audit-health.v1"}));
    state
        .publish_metrics("qbit_prism_health_state 1\n".into())
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        axum::serve(listener, router(state.clone()))
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .into_future(),
    );
    let runtime = metrics.runtime();
    let (started, start) = tokio::sync::oneshot::channel();
    struct ReleaseOnDrop(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }
    let released = ReleaseOnDrop(Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let release = released.0.clone();
    let blocking = tokio::spawn(runtime.track(TaskKind::Refresh, async move {
        started.send(()).unwrap();
        std::thread::sleep(Duration::from_secs(3));
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !release.load(std::sync::atomic::Ordering::Acquire)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(5));
        }
    }));
    start.await.unwrap();
    let (stop_monitor, monitor_rx) = tokio::sync::watch::channel(false);
    let sampler = tokio::spawn(runtime.clone().run(monitor_rx));
    tokio::time::sleep(Duration::from_millis(3100)).await;
    let response = reqwest::Client::new()
        .get(format!("http://{address}/healthz"))
        .timeout(Duration::from_millis(500))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let health: serde_json::Value = response.json().await.unwrap();
    assert_eq!(health["status"], "runtime-stalled");
    assert!(health["snapshot_age_seconds"].as_f64().unwrap() < 15.);
    let body = scrape(&state).await;
    assert!(sample(&body, "qbit_prism_runtime_lag_seconds") >= 0.);
    assert_eq!(sample(&body, "qbit_prism_health_state"), 0.);
    assert_eq!(
        sample(&body, "qbit_prism_runtime_task_stalled{task=\"refresh\"}"),
        1.
    );
    assert!(
        sample(
            &body,
            "qbit_prism_runtime_poll_lag_seconds{task=\"refresh\"}"
        ) >= 2.
    );
    drop(released);
    blocking.await.unwrap();
    assert!(!runtime.snapshot().stalled());
    assert!(
        sample(
            &scrape(&state).await,
            "qbit_prism_runtime_poll_lag_seconds{task=\"refresh\"}"
        ) >= 3.
    );
    assert_eq!(
        reqwest::get(format!("http://{address}/healthz"))
            .await
            .unwrap()
            .status(),
        200
    );
    stop.send(()).unwrap();
    stop_monitor.send_replace(true);
    server.await.unwrap().unwrap();
    sampler.await.unwrap().unwrap();
}

#[tokio::test]
async fn idle_slow_async_work_and_cancelled_operations_do_not_leave_false_stalls() {
    let runtime = Arc::new(qbit_prism_server::metrics::runtime::RuntimeMonitor::new(
        Duration::from_millis(10),
    ));
    assert!(!runtime.snapshot().stalled());
    assert_eq!(
        sample(
            &runtime.snapshot().render(),
            "qbit_prism_runtime_lag_seconds"
        ),
        -1.
    );
    let (started, start) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(runtime.track(TaskKind::Submit, async {
        started.send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(30)).await;
    }));
    start.await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !runtime.snapshot().stalled(),
        "Pending is not a blocked poll"
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!runtime.snapshot().stalled());
    let operation = runtime.start_operation(TaskKind::Refresh, Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(runtime.snapshot().stalled());
    drop(operation);
    assert!(!runtime.snapshot().stalled());
}

#[tokio::test]
async fn wall_clock_fields_do_not_change_monotonic_health_or_mask_a_base_failure() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    let metrics = Arc::new(Metrics::default());
    let state = state(metrics.clone());
    for timestamp in [-1_000_000_000_000i64, 1_000_000_000_000] {
        state.publish_health(json!({"ok":true,"checked_at":timestamp}));
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    state.publish_health(json!({"ok":false,"ready":false}));
    let response = router(state)
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
}

use std::future::IntoFuture;
