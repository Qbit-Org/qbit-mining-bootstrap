//! These use the existing protocol fixture and real TCP response writes.
use super::*;
use qbit_prism_server::metrics::{ConnectionRefusalReason, Metrics, RejectReason, StaleJobCause};
use std::collections::BTreeSet;

#[derive(Default)]
pub(super) struct Gate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl Gate {
    pub(super) async fn wait(&self) {
        self.entered.notify_one();
        self.release.notified().await;
    }
}
fn reset_client(client: Client) {
    // OwnedWriteHalf::drop performs a graceful write shutdown. Reunite first
    // so SO_LINGER=0 closes the full socket with a reset instead of sending FIN.
    let stream = client.reader.into_inner().reunite(client.writer).unwrap();
    #[allow(deprecated)]
    stream.set_linger(Some(Duration::ZERO)).unwrap();
    drop(stream);
}

fn sample(metrics: &qbit_prism_server::metrics::Metrics, key: &str) -> f64 {
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{key} ")))
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn submit_reason_normalization_is_bounded_and_preserves_tcp_errors() {
    let metrics = Arc::new(Metrics::default());
    let (address, backend, _refresh, shutdown, task) =
        start_with_metrics(StratumConfig::default(), metrics.clone()).await;
    let mut client = Client::connect(address).await;
    client.login("miner.reasons").await;
    let cases = [
        (None, "internal-error"),
        (Some("internal-error"), "internal-error"),
        (Some(""), "unrecognised"),
        (Some("future-reason"), "unrecognised"),
        (Some("another-new-reason"), "unrecognised"),
        (Some("untrusted\"\\\nreason"), "unrecognised"),
        (Some("ledger-outcome-unknown"), "ledger-outcome-unknown"),
    ];
    let mut expected = HashMap::<&str, usize>::new();
    for (index, (reason, label)) in cases.into_iter().enumerate() {
        let id = 20 + index as u64;
        *backend.submit_error.lock().unwrap() = Some(StratumError {
            code: 20,
            message: "controlled refusal".into(),
            reason_id: reason.map(str::to_owned),
        });
        client
            .send(client.solved_submit(id, "miner.reasons", 0))
            .await;
        assert_eq!(
            client.response(id).await,
            json!({"id":id,"result":null,"error":[20,"controlled refusal",
                reason.map(|value| json!({"reason_id":value}))]}),
        );
        *expected.entry(label).or_default() += 1;
        let body = http_metrics(metrics.clone()).await;
        let mut observed_labels = BTreeSet::new();
        for line in body
            .lines()
            .filter(|line| line.starts_with("qbit_prism_rejections_total{"))
        {
            let (key, count) = line.rsplit_once(' ').unwrap();
            let label = key
                .strip_prefix("qbit_prism_rejections_total{reason_id=\"")
                .and_then(|value| value.strip_suffix("\"}"))
                .unwrap();
            assert!(observed_labels.insert(label));
            assert_eq!(
                count.parse::<usize>().unwrap(),
                *expected.get(label).unwrap_or(&0)
            );
        }
        assert_eq!(
            observed_labels,
            RejectReason::ALL
                .iter()
                .map(|reason| reason.as_str())
                .collect()
        );
        assert_eq!(
            observed_labels.len(),
            14,
            "free-form reasons must not create series"
        );
        assert_eq!(
            sample(
                &metrics,
                "qbit_prism_share_ack_seconds_count{result=\"rejected\"}"
            ),
            (index + 1) as f64
        );
        assert_eq!(
            sample(
                &metrics,
                "qbit_prism_share_ack_seconds_count{result=\"accepted\"}"
            ),
            0.
        );
    }
    assert!(backend.shares.lock().unwrap().is_empty());
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn complete_submit_frame_to_successful_ack_excludes_partial_frame_and_post_ack_hint() {
    let config = StratumConfig::default();
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let (address, backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut client = Client::connect(address).await;
    client.login("miner.ack").await;
    let gate = Arc::new(Gate::default());
    let hint = Arc::new(Gate::default());
    *backend.submit_gate.lock().unwrap() = Some(gate.clone());
    *backend.hint_gate.lock().unwrap() = Some(hint.clone());
    let request = client.solved_submit(20, "miner.ack", 0);
    client
        .writer
        .write_all(request.to_string().as_bytes())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_share_ack_seconds_count{result=\"accepted\"}"
        ),
        0.
    );
    let complete_sent = tokio::time::Instant::now();
    client.writer.write_all(b"\n").await.unwrap();
    gate.entered.notified().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    gate.release.notify_one();
    assert_eq!(client.response(20).await["result"], true);
    hint.entered.notified().await;
    let elapsed = sample(
        &metrics,
        "qbit_prism_share_ack_seconds_sum{result=\"accepted\"}",
    );
    assert!(
        elapsed >= 0.4 && elapsed <= complete_sent.elapsed().as_secs_f64(),
        "observed {elapsed}"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_share_ack_seconds_sum{result=\"accepted\"}"
        ),
        elapsed
    );
    hint.release.notify_one();
    client.send(request).await;
    assert!(!client.response(20).await["error"].is_null());
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_rejections_total{reason_id=\"duplicate-share\"}"
        ),
        1.
    );
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_share_ack_seconds_count{result=\"rejected\"}"
        ),
        1.
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn accepted_share_with_failed_tcp_response_has_no_ack_or_rejection() {
    let config = StratumConfig::default();
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let stats = config.stats.clone();
    let (address, backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut client = Client::connect(address).await;
    client.login("miner.write-failure").await;
    let gate = Arc::new(Gate::default());
    *backend.submit_gate.lock().unwrap() = Some(gate.clone());
    client
        .send(client.solved_submit(20, "miner.write-failure", 0))
        .await;
    gate.entered.notified().await;
    reset_client(client);
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.release.notify_one();
    timeout(Duration::from_secs(3), async {
        while stats.snapshot(0).connections != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(backend.shares.lock().unwrap().len(), 1);
    assert_eq!(stats.snapshot(0).accepted_submissions, 1);
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_share_ack_seconds_count{result=\"accepted\"}"
        ),
        0.
    );
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_share_ack_seconds_count{result=\"rejected\"}"
        ),
        0.
    );
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_rejections_total{reason_id=\"internal-error\"}"
        ),
        0.
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn pending_initial_work_moves_and_cancellation_does_not_fabricate_share_events() {
    let config = StratumConfig::default();
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let stats = config.stats.clone();
    let (address, backend, _refresh, _shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let gate = Arc::new(Gate::default());
    *backend.build_gate.lock().unwrap() = Some(gate.clone());
    let mut client = Client::connect(address).await;
    client
        .send(json!({"id":1,"method":"mining.subscribe","params":[]}))
        .await;
    client.extranonce1 = client.response(1).await["result"][1]
        .as_str()
        .unwrap()
        .into();
    client
        .send(json!({"id":2,"method":"mining.authorize","params":["miner.wait","x"]}))
        .await;
    assert_eq!(client.response(2).await["result"], true);
    gate.entered.notified().await;
    metrics.publish_delivery(stats.delivery_metrics());
    assert_eq!(
        sample(&metrics, "qbit_prism_stratum_pending_initial_jobs"),
        1.
    );
    assert!(
        sample(
            &metrics,
            "qbit_prism_stratum_oldest_pending_initial_job_seconds"
        ) >= 0.
    );
    gate.release.notify_one();
    client.next_job().await;
    metrics.publish_delivery(stats.delivery_metrics());
    assert_eq!(
        sample(&metrics, "qbit_prism_stratum_pending_initial_jobs"),
        0.
    );
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_stratum_oldest_pending_initial_job_seconds"
        ),
        0.
    );
    let submit = Arc::new(Gate::default());
    *backend.submit_gate.lock().unwrap() = Some(submit.clone());
    client.send(client.solved_submit(20, "miner.wait", 0)).await;
    submit.entered.notified().await;
    assert!(
        !metrics.runtime().snapshot().stalled(),
        "async waiting is not a blocked poll"
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    timeout(Duration::from_secs(3), async {
        while stats.snapshot(0).connections != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(backend.shares.lock().unwrap().is_empty());
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_share_ack_seconds_count{result=\"accepted\"}"
        ),
        0.
    );
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_share_ack_seconds_count{result=\"rejected\"}"
        ),
        0.
    );
    assert!(!metrics.runtime().snapshot().stalled());
}

#[tokio::test]
async fn failed_rejection_response_counts_the_decision_without_an_ack() {
    let config = StratumConfig::default();
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let stats = config.stats.clone();
    let (address, backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut client = Client::connect(address).await;
    client.login("miner.rejected-write").await;
    let request = client.solved_submit(20, "miner.rejected-write", 0);
    client.send(request.clone()).await;
    assert_eq!(client.response(20).await["result"], true);
    let gate = Arc::new(Gate::default());
    *backend.submit_gate.lock().unwrap() = Some(gate.clone());
    client.send(request).await;
    gate.entered.notified().await;
    reset_client(client);
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.release.notify_one();
    timeout(Duration::from_secs(3), async {
        while stats.snapshot(0).connections != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(stats.snapshot(0).rejected_submissions, 1);
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_rejections_total{reason_id=\"duplicate-share\"}"
        ),
        1.
    );
    assert_eq!(sample(&metrics, "qbit_prism_duplicate_shares_total"), 1.);
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_share_ack_seconds_count{result=\"rejected\"}"
        ),
        0.
    );
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_share_ack_seconds_count{result=\"accepted\"}"
        ),
        1.
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

/// `ConnectionRefusalReason::ALL` in declaration order: `[global_limit,
/// username_limit, ip_limit, malformed_frame_budget, unknown_job_budget,
/// authorize_budget]`.
fn refusals(metrics: &Metrics) -> [f64; 6] {
    ConnectionRefusalReason::ALL
        .iter()
        .map(|reason| {
            sample(
                metrics,
                &format!(
                    "qbit_prism_stratum_connection_refusals_total{{reason=\"{}\"}}",
                    reason.as_str()
                ),
            )
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
}

/// Attribution samples carry exactly their closed label, once per value, and
/// never an identity observed during the test.
fn assert_closed_attribution_labels(body: &str, identities: &[&str]) {
    for (family, key, allowed) in [
        (
            "qbit_prism_stratum_connection_refusals_total",
            "reason",
            ConnectionRefusalReason::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<BTreeSet<_>>(),
        ),
        (
            "qbit_prism_stale_job_rejections_total",
            "cause",
            StaleJobCause::ALL
                .iter()
                .map(|value| value.as_str())
                .collect(),
        ),
    ] {
        let mut seen = BTreeSet::new();
        for line in body
            .lines()
            .filter(|line| line.starts_with(&format!("{family}{{")))
        {
            let labels = line.split_once('{').unwrap().1.rsplit_once('}').unwrap().0;
            let value = labels
                .strip_prefix(&format!("{key}=\""))
                .and_then(|rest| rest.strip_suffix('"'))
                .unwrap_or_else(|| panic!("unexpected labels: {line}"));
            assert!(allowed.contains(value), "unexpected label value: {line}");
            assert!(seen.insert(value), "duplicate series: {line}");
        }
        assert_eq!(seen, allowed, "{family}");
    }
    assert!(body
        .lines()
        .any(|line| line.starts_with("qbit_prism_stratum_connection_limit ")));
    for identity in identities {
        assert!(
            !body.contains(identity),
            "identity in exposition: {identity}"
        );
    }
}

/// The existing global-limit behavior: the socket is closed without a response.
async fn assert_refused_without_response(address: std::net::SocketAddr) {
    use tokio::io::AsyncReadExt;
    let mut refused = TcpStream::connect(address).await.unwrap();
    let mut bytes = Vec::new();
    match timeout(Duration::from_secs(5), refused.read_to_end(&mut bytes))
        .await
        .expect("the refused socket stayed open")
    {
        Ok(_) => assert!(bytes.is_empty(), "refused socket received {bytes:?}"),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
    }
}

#[tokio::test]
async fn global_limit_of_one_refuses_a_second_socket_exactly_once_and_keeps_the_configured_gauge() {
    let config = StratumConfig {
        connection_limit: ConnectionLimit::new(1),
        ..Default::default()
    };
    let stats = config.stats.clone();
    let metrics = Arc::new(Metrics::default());
    assert_eq!(sample(&metrics, "qbit_prism_stratum_connection_limit"), -1.);
    let (address, backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut held = Client::connect(address).await;
    held.login("miner.held").await;
    assert_eq!(sample(&metrics, "qbit_prism_stratum_connection_limit"), 1.);
    assert_eq!(refusals(&metrics), [0., 0., 0., 0., 0., 0.]);
    assert_refused_without_response(address).await;
    assert_eq!(refusals(&metrics), [1., 0., 0., 0., 0., 0.]);
    // The only permit is still held: the gauge is capacity, not free permits.
    assert_eq!(sample(&metrics, "qbit_prism_stratum_connection_limit"), 1.);
    assert_eq!(stats.snapshot(0).connections, 1);
    // The admitted session keeps working, and success is not a refusal.
    held.send(held.solved_submit(10, "miner.held", 0)).await;
    assert_eq!(held.response(10).await["result"], true);
    assert_eq!(
        backend.credited_workers.lock().unwrap().as_slice(),
        ["miner.held"]
    );
    assert_eq!(refusals(&metrics), [1., 0., 0., 0., 0., 0.]);
    assert_closed_attribution_labels(
        &metrics.render(),
        &["miner.held", "127.0.0.1", &address.to_string()],
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn default_listener_reports_configured_capacity_while_connections_hold_permits() {
    let config = StratumConfig::default();
    assert_eq!(config.connection_limit.capacity(), 384);
    let metrics = Arc::new(Metrics::default());
    let (address, _backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut clients = Vec::new();
    for index in 0..3 {
        let mut client = Client::connect(address).await;
        client.login(&format!("miner.{index}")).await;
        clients.push(client);
        assert_eq!(
            sample(&metrics, "qbit_prism_stratum_connection_limit"),
            384.
        );
    }
    assert_eq!(refusals(&metrics), [0., 0., 0., 0., 0., 0.]);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn username_limit_refusal_counts_once_while_reauthorization_and_success_do_not() {
    let metrics = Arc::new(Metrics::default());
    let (address, backend, _refresh, shutdown, task) = start_with_metrics(
        StratumConfig {
            max_connections_per_username: 1,
            ..Default::default()
        },
        metrics.clone(),
    )
    .await;
    let mut first = Client::connect(address).await;
    first.login("miner.one").await;
    first.send(first.solved_submit(10, "miner.one", 0)).await;
    assert_eq!(first.response(10).await["result"], true);
    // Same-username reauthorization keeps the session's own permit.
    first
        .send(json!({"id":11,"method":"mining.authorize","params":["miner.one","x"]}))
        .await;
    assert_eq!(
        first.response(11).await,
        json!({"id":11,"result":true,"error":null})
    );
    assert_eq!(refusals(&metrics), [0., 0., 0., 0., 0., 0.]);
    let mut second = Client::connect(address).await;
    second.login("miner.two").await;
    second
        .send(json!({"id":9,"method":"mining.authorize","params":["miner.one","x"]}))
        .await;
    assert_eq!(
        second.response(9).await,
        json!({"id":9,"result":null,"error":[20,"too many connections for username",null]})
    );
    assert_eq!(refusals(&metrics), [0., 1., 0., 0., 0., 0.]);
    second.send(second.solved_submit(12, "miner.two", 0)).await;
    assert_eq!(second.response(12).await["result"], true);
    assert_eq!(refusals(&metrics), [0., 1., 0., 0., 0., 0.]);
    assert_eq!(
        backend.credited_workers.lock().unwrap().as_slice(),
        ["miner.one", "miner.two"]
    );
    // An authorization refusal is not a share rejection.
    for reason in RejectReason::ALL {
        let key = format!(
            "qbit_prism_rejections_total{{reason_id=\"{}\"}}",
            reason.as_str()
        );
        assert_eq!(sample(&metrics, &key), 0.);
    }
    assert_eq!(
        sample(
            &metrics,
            "qbit_prism_share_ack_seconds_count{result=\"rejected\"}"
        ),
        0.
    );
    assert_closed_attribution_labels(
        &metrics.render(),
        &["miner.one", "miner.two", "127.0.0.1", &address.to_string()],
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

/// Manual inspection only, never run by CI: `cargo test -p qbit-prism-server
/// --test stratum_protocol -- --ignored --exact
/// observability::serve_admission_metrics_for_manual_inspection --nocapture`.
/// It produces one refusal of each reason on a real listener, then serves the
/// real API router, republishing every second, at
/// `PRISM_MANUAL_METRICS_BIND` (default `127.0.0.1:0`) for
/// `PRISM_MANUAL_METRICS_HOLD_SECONDS` (default 60).
#[tokio::test]
#[ignore = "serves /metrics for manual inspection"]
async fn serve_admission_metrics_for_manual_inspection() {
    use qbit_prism_server::api::{router, ApiConfig, ApiState};
    let bind = std::env::var("PRISM_MANUAL_METRICS_BIND").unwrap_or_else(|_| "127.0.0.1:0".into());
    let hold: u64 = std::env::var("PRISM_MANUAL_METRICS_HOLD_SECONDS")
        .map_or(60, |value| value.parse().expect("hold seconds"));
    let metrics = Arc::new(Metrics::default());
    let (address, _backend, _refresh, shutdown, task) = start_with_metrics(
        StratumConfig {
            connection_limit: ConnectionLimit::new(2),
            max_connections_per_username: 1,
            ..Default::default()
        },
        metrics.clone(),
    )
    .await;
    let mut first = Client::connect(address).await;
    first.login("miner.one").await;
    let mut second = Client::connect(address).await;
    second.login("miner.two").await;
    second
        .send(json!({"id":9,"method":"mining.authorize","params":["miner.one","x"]}))
        .await;
    assert_eq!(
        second.response(9).await["error"][1],
        "too many connections for username"
    );
    assert_refused_without_response(address).await;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
        .unwrap();
    let state = ApiState::new(pool, ApiConfig::default(), metrics.clone());
    state.publish_metrics(metrics.render()).unwrap();
    let listener = TcpListener::bind(&bind).await.unwrap();
    println!(
        "serving http://{}/metrics for {hold} seconds",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn({
        let state = state.clone();
        async move { axum::serve(listener, router(state)).await.unwrap() }
    });
    for _ in 0..hold {
        tokio::time::sleep(Duration::from_secs(1)).await;
        state.publish_metrics(metrics.render()).unwrap();
    }
    server.abort();
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

/// Serve the coordinator API router on a real listener and scrape it by HTTP.
pub(super) async fn http_metrics(metrics: Arc<Metrics>) -> String {
    use qbit_prism_server::api::{router, ApiConfig, ApiState};
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
        .unwrap();
    let state = ApiState::new(pool, ApiConfig::default(), metrics.clone());
    state.publish_metrics(metrics.render()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    let response = reqwest::get(format!("http://{address}/metrics"))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    server.abort();
    let _ = server.await;
    body
}

#[tokio::test]
async fn real_http_scrape_captures_admission_refusals_and_configured_limit() {
    let metrics = Arc::new(Metrics::default());
    let config = StratumConfig {
        connection_limit: ConnectionLimit::new(2),
        max_connections_per_username: 1,
        ..Default::default()
    };
    let (address, _backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut first = Client::connect(address).await;
    first.login("miner.one").await;
    let mut second = Client::connect(address).await;
    second.login("miner.two").await;
    second
        .send(json!({"id":9,"method":"mining.authorize","params":["miner.one","x"]}))
        .await;
    assert_eq!(
        second.response(9).await["error"][1],
        "too many connections for username"
    );
    assert_refused_without_response(address).await;
    let body = http_metrics(metrics).await;
    let families = [
        "qbit_prism_stratum_connection_refusals_total",
        "qbit_prism_stratum_connection_limit",
        "qbit_prism_stale_job_rejections_total",
    ];
    for descriptor in qbit_prism_server::metrics::descriptors()
        .filter(|descriptor| families.contains(&descriptor.name))
    {
        assert!(body.contains(&format!("# HELP {} {}\n", descriptor.name, descriptor.help)));
    }
    for expected in [
        "# TYPE qbit_prism_stratum_connection_refusals_total counter",
        "qbit_prism_stratum_connection_refusals_total{reason=\"global_limit\"} 1",
        "qbit_prism_stratum_connection_refusals_total{reason=\"username_limit\"} 1",
        "# TYPE qbit_prism_stratum_connection_limit gauge",
        "qbit_prism_stratum_connection_limit 2",
        "# TYPE qbit_prism_stale_job_rejections_total counter",
        "qbit_prism_connections 0",
    ] {
        assert!(
            body.lines().any(|line| line == expected),
            "missing {expected}"
        );
    }
    assert_closed_attribution_labels(
        &body,
        &["miner.one", "miner.two", "127.0.0.1", &address.to_string()],
    );
    let excerpt: Vec<_> = body
        .lines()
        .filter(|line| families.iter().any(|family| line.contains(family)))
        .collect();
    println!(
        "HTTP /metrics excerpt from a real Stratum listener:\n{}",
        excerpt.join("\n")
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
}
