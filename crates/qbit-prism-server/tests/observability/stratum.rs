//! These use the existing protocol fixture and real TCP response writes.
use super::*;

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
