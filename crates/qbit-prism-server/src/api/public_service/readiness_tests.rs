use super::*;
use axum::{body::to_bytes, http::Request};
use sqlx::postgres::PgSslMode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;
use tracing::instrument::WithSubscriber;

const PRIVATE: &str =
    "postgres://synthetic_role:synthetic_password@synthetic-host:5432/synthetic_database";

#[derive(Clone, Default)]
struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl LogCapture {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

#[test]
fn readiness_event_visibility_uses_existing_module_filter_without_replaying() {
    use super::readiness_events::{ReadinessEvent, ReadinessEvents};

    for (level, warnings, recoveries) in [("error", 0, 0), ("warn", 1, 0), ("info", 1, 1)] {
        let logs = LogCapture::default();
        let writer = logs.clone();
        tracing::subscriber::with_default(
            tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_env_filter(format!("qbit_prism_server::api::public_service={level}"))
                .with_writer(move || writer.clone())
                .finish(),
            || {
                ReadinessEvent::Failed(ProbeFailure::Connection, "schema").emit();
                ReadinessEvent::Recovered.emit();
            },
        );
        let text = logs.text();
        assert_eq!(
            text.matches("public readiness probe failed").count(),
            warnings
        );
        assert_eq!(
            text.matches("public readiness probe recovered").count(),
            recoveries
        );
        assert_private_absent(&text);
    }

    let mut events = ReadinessEvents::default();
    let start = Instant::now();
    let failure = Some((ProbeFailure::Connection, "schema"));
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        events.observe(failure, false, start).unwrap().emit();
    });
    // Subscriber visibility cannot reset the warning budget or invent a replay.
    assert!(events
        .observe(failure, false, start + Duration::from_secs(59))
        .is_none());
    assert_eq!(
        events.observe(failure, false, start + Duration::from_secs(60)),
        Some(ReadinessEvent::Failed(ProbeFailure::Connection, "schema"))
    );
    assert_eq!(
        events.observe(None, true, start + Duration::from_secs(60)),
        Some(ReadinessEvent::Recovered)
    );
}

fn service(pool: PgPool, config: ServiceConfig) -> (Router, Arc<ServiceState>) {
    router(
        ApiState::new(
            pool,
            ApiConfig::default(),
            Arc::new(crate::metrics::Metrics::default()),
        ),
        config,
    )
}

fn assert_private_absent(text: &str) {
    for value in [
        PRIVATE,
        "synthetic_role",
        "synthetic_password",
        "synthetic-host",
        "synthetic_database",
    ] {
        assert!(!text.contains(value), "private value leaked: {value}");
    }
}

async fn health(app: &Router, expected_status: StatusCode) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), expected_status);
    if expected_status.is_success() {
        assert!(!response.headers().contains_key("cache-control"));
    } else {
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
    assert_private_absent(std::str::from_utf8(&bytes).unwrap());
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["schema"], "qbit.prism.public-read-health.v1");
    assert_eq!(value["ok"], expected_status == StatusCode::OK);
    value
}

// Return a real PostgreSQL ErrorResponse to SQLx using entirely synthetic
// fields. This tests the actual probe -> driver -> HTTP/log boundary without
// requiring a database or connecting to the caller's database.
async fn error_pool(code: &str, responses: usize) -> (PgPool, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let fields = format!(
        "SFATAL\0VFATAL\0C{code}\0M{PRIVATE}\0D{PRIVATE}\0H{PRIVATE}\0s{PRIVATE}\0t{PRIVATE}\0\0"
    )
    .into_bytes();
    let server = tokio::spawn(async move {
        for _ in 0..responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let size = socket.read_u32().await.unwrap();
            assert!((8..4096).contains(&size));
            let mut startup = vec![0; size as usize - 4];
            socket.read_exact(&mut startup).await.unwrap();
            socket.write_u8(b'E').await.unwrap();
            socket.write_u32(fields.len() as u32 + 4).await.unwrap();
            socket.write_all(&fields).await.unwrap();
        }
    });
    let options = PgConnectOptions::new_without_pgpass()
        .host("127.0.0.1")
        .port(port)
        .username("synthetic_role")
        .password("synthetic_password")
        .database("synthetic_database")
        .ssl_mode(PgSslMode::Disable);
    (read_pool(options, 1), server)
}

#[tokio::test]
async fn database_failures_are_categorized_without_public_or_operator_secrets() {
    for (code, category, message) in [
        ("28P01", "authentication", "database authentication failed"),
        ("28000", "authentication", "database authentication failed"),
        ("42501", "access", "database access denied"),
        ("08004", "connection", "database connection failed"),
        ("3D000", "connection", "database connection failed"),
        ("42P01", "schema", "native public read schema is incomplete"),
        ("42703", "schema", "native public read schema is incomplete"),
        ("3F000", "schema", "native public read schema is incomplete"),
        (
            "57014",
            "cancellation",
            "database readiness query was canceled",
        ),
        ("XX000", "readiness", "database readiness query failed"),
        // Even an unrecognized SQLSTATE must never be copied into diagnostics.
        (
            "synthetic_password",
            "readiness",
            "database readiness query failed",
        ),
    ] {
        let (pool, server) = error_pool(code, 1).await;
        let (app, service) = service(pool, ServiceConfig::default());
        let logs = LogCapture::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        service.probe_once().with_subscriber(subscriber).await;
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        let body = health(&app, StatusCode::SERVICE_UNAVAILABLE).await;
        assert_eq!(body["error"], message, "SQLSTATE {code}");
        assert_eq!(body["state"], "unready");
        assert_eq!(body["database_ready"], false);
        assert!(body["checked_at"].is_string());
        assert!(body["probe_age_seconds"].is_number());
        let logs = logs.text();
        assert!(logs.contains("public readiness probe failed"), "{logs}");
        assert!(logs.contains(&format!("category=\"{category}\"")), "{logs}");
        assert!(logs.contains("phase=\"schema\""), "{logs}");
        assert!(logs.contains("action=\"check "), "{logs}");
        if code == "57014" {
            assert!(logs.contains("statement deadlines and operator query cancellations"));
        }
        assert_private_absent(&logs);
        service.pool.close().await;
    }
}

#[tokio::test]
async fn driver_sources_are_never_used_in_diagnostics() {
    for (error, expected) in [
        (
            sqlx::Error::Io(std::io::Error::other(PRIVATE)),
            ProbeFailure::Connection,
        ),
        (sqlx::Error::Tls(PRIVATE.into()), ProbeFailure::Connection),
        (
            sqlx::Error::Configuration(PRIVATE.into()),
            ProbeFailure::Configuration,
        ),
        (
            sqlx::Error::Protocol(PRIVATE.into()),
            ProbeFailure::Readiness,
        ),
        (sqlx::Error::Decode(PRIVATE.into()), ProbeFailure::Readiness),
        (
            sqlx::Error::ColumnNotFound(PRIVATE.into()),
            ProbeFailure::Readiness,
        ),
        (sqlx::Error::PoolTimedOut, ProbeFailure::Timeout),
        (sqlx::Error::PoolClosed, ProbeFailure::Connection),
    ] {
        let failure = ProbeFailure::from_sqlx(&error);
        assert_eq!(failure, expected);
        assert_private_absent(failure.public_message());
        let logs = LogCapture::default();
        let writer = logs.clone();
        tracing::subscriber::with_default(
            tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish(),
            || failure.log("replica"),
        );
        assert!(logs.text().contains("phase=\"replica\""));
        assert_private_absent(&logs.text());
    }
}

#[tokio::test]
async fn health_contract_preserves_success_startup_staleness_replica_and_head() {
    let (app, service) = service(
        read_pool(PgConnectOptions::new_without_pgpass(), 1),
        ServiceConfig::default(),
    );
    let body = health(&app, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_eq!(body["state"], "starting");
    assert_eq!(body["error"], "readiness probe has not completed yet");
    {
        let mut snapshot = service.snapshot.write().unwrap();
        snapshot.ready = true;
        snapshot.checked = Some(Instant::now());
        snapshot.checked_at = Some(now());
    }
    let body = health(&app, StatusCode::OK).await;
    assert_eq!(body["state"], "ready");
    assert_eq!(body["database_ready"], true);
    assert_eq!(
        body.as_object().unwrap().len(),
        7,
        "success payload shape changed"
    );
    assert!(body.get("error").is_none());
    service.snapshot.write().unwrap().checked = Some(Instant::now() - Duration::from_secs(16));
    let body = health(&app, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_eq!(body["database_ready"], false);
    assert_eq!(body["error"], "readiness probe is stale");
    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(to_bytes(response.into_body(), 8192)
        .await
        .unwrap()
        .is_empty());
    service.pool.close().await;

    let (app, service) = self::service(
        read_pool(PgConnectOptions::new_without_pgpass(), 1),
        ServiceConfig {
            replica_required: true,
            ..Default::default()
        },
    );
    service.pool.close().await;
    service.probe_once().await;
    let body = health(&app, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_eq!(
        body["error"], "public read service is warming up",
        "replica failure precedence changed"
    );
    assert_eq!(body["database_ready"], false);
}

#[tokio::test]
async fn probe_deadline_still_fails_and_releases_waiting_acquisition() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let options = PgConnectOptions::new_without_pgpass()
        .host("127.0.0.1")
        .port(listener.local_addr().unwrap().port())
        .ssl_mode(PgSslMode::Disable);
    let (app, service) = service(read_pool(options, 1), ServiceConfig::default());
    let probe = tokio::spawn({
        let service = service.clone();
        async move { service.probe_once().await }
    });
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    // Ensure the driver is actually waiting for the handshake before advancing
    // the existing five-second whole-probe deadline.
    assert!(socket.read_u32().await.unwrap() >= 8);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(5)).await;
    probe.await.unwrap();
    tokio::time::resume();
    let body = health(&app, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_eq!(body["error"], "database probe timed out");
    assert_eq!(body["database_ready"], false);
    drop(socket);
    tokio::time::timeout(Duration::from_secs(1), service.pool.close())
        .await
        .unwrap();
}

#[tokio::test]
async fn recovery_waits_for_replica_policy_and_replica_only_refusals_stay_silent() {
    let (app, service) = service(
        read_pool(PgConnectOptions::new_without_pgpass(), 1),
        ServiceConfig {
            replica_required: true,
            ..Default::default()
        },
    );
    service.pool.close().await;
    let healthy = json!({
        "schema_ready": true,
        "in_recovery": true,
        "receiver_heartbeat_age_seconds": 0,
    });
    let refusals = [
        (
            json!({"schema_ready": true, "in_recovery": false}),
            "public read service refuses a database that is not in recovery",
        ),
        (
            json!({"schema_ready": true, "in_recovery": true}),
            "read replica replication stream is not connected",
        ),
        (
            json!({"schema_ready": true, "in_recovery": true, "receiver_heartbeat_age_seconds": 61}),
            "read replica replication stream exceeded its heartbeat age bound",
        ),
    ];
    let logs = LogCapture::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    async {
        service.publish_probe(Ok(healthy.clone()));
        health(&app, StatusCode::OK).await;
        for (value, message) in &refusals {
            service.publish_probe(Ok(value.clone()));
            let body = health(&app, StatusCode::SERVICE_UNAVAILABLE).await;
            assert_eq!(body["error"], *message);
        }
        service.publish_probe(Ok(healthy.clone()));
        assert!(
            logs.text().is_empty(),
            "healthy startup and replica-only refusal"
        );

        for episode in 1..=2 {
            service.publish_probe(Err((ProbeFailure::Timeout, "probe")));
            let body = health(&app, StatusCode::SERVICE_UNAVAILABLE).await;
            assert_eq!(body["database_ready"], false);
            assert_eq!(body["error"], "database probe timed out");
            for (value, message) in &refusals {
                service.publish_probe(Ok(value.clone()));
                let body = health(&app, StatusCode::SERVICE_UNAVAILABLE).await;
                assert_eq!(body["error"], *message);
                assert_eq!(
                    logs.text()
                        .matches("public readiness probe recovered")
                        .count(),
                    episode - 1
                );
            }
            // A query success with a replica refusal must not clear the episode
            // or reset its warning budget when the same database error returns.
            service.publish_probe(Err((ProbeFailure::Timeout, "probe")));
            assert_eq!(
                logs.text().matches("public readiness probe failed").count(),
                episode
            );
            service.publish_probe(Ok(healthy.clone()));
            health(&app, StatusCode::OK).await;
            service.publish_probe(Ok(healthy.clone()));
            assert_eq!(
                logs.text()
                    .matches("public readiness probe recovered")
                    .count(),
                episode
            );
        }
    }
    .with_subscriber(subscriber)
    .await;
    assert_private_absent(&logs.text());
}

#[tokio::test]
async fn blocked_operator_log_does_not_hold_the_health_snapshot_lock() {
    type Release = Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>;
    #[derive(Clone)]
    struct BlockedLog {
        entered: std::sync::mpsc::SyncSender<()>,
        release: Release,
    }
    impl std::io::Write for BlockedLog {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let (lock, wake) = &*self.release;
            let released = lock.lock().unwrap();
            if !*released {
                self.entered.send(()).unwrap();
                drop(wake.wait_while(released, |released| !*released).unwrap());
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct ReleaseOnDrop(Release);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            let (lock, wake) = &*self.0;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
    }

    for recovering in [false, true] {
        let (app, service) = service(
            read_pool(PgConnectOptions::new_without_pgpass(), 1),
            ServiceConfig::default(),
        );
        service.pool.close().await;
        service.publish_probe(if recovering {
            Err((ProbeFailure::Connection, "schema"))
        } else {
            Ok(json!({"schema_ready": true}))
        });
        let old_checked = Instant::now() - Duration::from_secs(60);
        service.snapshot.write().unwrap().checked = Some(old_checked);
        let (entered, observed) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let guard = ReleaseOnDrop(release.clone());
        let writer = BlockedLog { entered, release };
        let probe_service = service.clone();
        // Separate threads let the test release the backpressured sink even
        // when a regression holds a synchronous RwLock while emitting an event.
        let probe = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(move || writer.clone())
                .finish();
            runtime.block_on(
                async move {
                    if recovering {
                        probe_service.publish_probe(Ok(json!({"schema_ready": true})));
                    } else {
                        probe_service.probe_once().await;
                    }
                }
                .with_subscriber(subscriber),
            );
        });
        let entered_log = observed.recv_timeout(Duration::from_secs(3));
        let published = service
            .snapshot
            .try_read()
            .ok()
            .map(|snapshot| (snapshot.ready, snapshot.last_error, snapshot.checked));
        // Do not acquire a blocking read lock on the negative-control path.
        // The normal path exercises HTTP before releasing either event sink.
        if published.is_some() {
            let body = health(
                &app,
                if recovering {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                },
            )
            .await;
            assert!(body["probe_age_seconds"].as_f64().unwrap() < 15.);
            if recovering {
                assert!(body.get("error").is_none());
            } else {
                assert_eq!(body["error"], "database connection failed");
            }
        }
        drop(guard);
        probe.join().unwrap();
        entered_log.unwrap();
        let (ready, error, checked) = published.expect("snapshot lock held by diagnostic sink");
        assert_eq!(ready, recovering);
        assert_eq!(error, (!recovering).then_some(ProbeFailure::Connection));
        assert!(
            checked.unwrap() > old_checked,
            "must read the new probe result"
        );
    }
}

#[tokio::test]
async fn retried_connection_failures_report_timeout_with_outage_guidance() {
    for code in [None, Some("53300"), Some("57P03")] {
        let (pool, server) = if let Some(code) = code {
            // Prove SQLx actually retries these PostgreSQL errors. After three
            // responses, closing the fixture listener leaves refused retries.
            let (pool, server) = error_pool(code, 3).await;
            (pool, Some(server))
        } else {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let options = PgConnectOptions::new_without_pgpass()
                .host("127.0.0.1")
                .port(listener.local_addr().unwrap().port())
                .username("synthetic_role")
                .password("synthetic_password")
                .database("synthetic_database")
                .ssl_mode(PgSslMode::Disable);
            drop(listener);
            (read_pool(options, 1), None)
        };
        let (app, service) = service(pool, ServiceConfig::default());
        let logs = LogCapture::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tokio::time::timeout(
            Duration::from_secs(8),
            service.probe_once().with_subscriber(subscriber),
        )
        .await
        .unwrap();
        if let Some(server) = server {
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }
        let body = health(&app, StatusCode::SERVICE_UNAVAILABLE).await;
        assert_eq!(body["error"], "database probe timed out");
        assert_eq!(body["database_ready"], false);
        let logs = logs.text();
        assert!(logs.contains("category=\"timeout\""), "{logs}");
        assert!(logs.contains("phase=\"probe\""), "{logs}");
        assert!(logs.contains("check database availability, network, connection limits"));
        assert_private_absent(&logs);
        tokio::time::timeout(Duration::from_secs(1), service.pool.close())
            .await
            .unwrap();
    }
}
