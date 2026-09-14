//! Exercise the actual long-poll loop against gated loopback RPC responses.
use super::*;
use tokio::time::timeout;

struct Running {
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}
impl Running {
    fn start(fixture: &Fixture) -> Self {
        let (shutdown, rx) = watch::channel(false);
        let task = tokio::spawn(fixture.coordinator.clone().blockwait_loop(rx));
        Self { shutdown, task }
    }
    async fn stop(mut self) {
        self.shutdown.send_replace(true);
        timeout(Duration::from_millis(500), &mut self.task)
            .await
            .expect("blockwait must cancel its current wait")
            .unwrap();
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn gate(fixture: &Fixture, method: &str) -> Arc<Gate> {
    let gate = Arc::new(Gate::default());
    fixture.node.lock().unwrap().gate = Some((method.into(), gate.clone()));
    gate
}
async fn entered(gate: &Gate) {
    timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .expect("expected the actual RPC request to enter its gate");
}

#[tokio::test]
async fn changed_notification_after_a_newer_poll_verifies_while_real_refresh_is_blocked() {
    let mut fixture = Fixture::new(Duration::from_secs(10)).await;
    Arc::get_mut(&mut Arc::get_mut(&mut fixture.coordinator).unwrap().config)
        .unwrap()
        .snapshot_interval = Duration::ZERO;
    fixture.coordinator.refresh_once().await.unwrap();
    let old = fixture.job(1, 0, "original.worker");
    fixture.node.lock().unwrap().tip = hash(2);
    let notification = gate(&fixture, "waitfornewblock");
    let running = Running::start(&fixture);
    entered(&notification).await;

    // This real refresh polls A after waitfornewblock has begun and then
    // stalls at persistence. The notification must not inherit its old order.
    fixture.node.lock().unwrap().tip = hash(1);
    let save = Arc::new(Gate::default());
    *fixture.store.save_gate.lock().unwrap() = Some(save.clone());
    let coordinator = fixture.coordinator.clone();
    let refresh = tokio::spawn(async move { coordinator.refresh_once().await });
    entered(&save).await;
    fixture.node.lock().unwrap().tip = hash(2);
    let next_wait = gate(&fixture, "waitfornewblock");
    notification.release.notify_one();
    entered(&next_wait).await;

    assert_eq!(
        fixture
            .coordinator
            .prepared
            .read()
            .await
            .as_ref()
            .unwrap()
            .template["previousblockhash"],
        hash(1)
    );
    fixture.submit(&old, false).await.unwrap();
    {
        let records = fixture.store.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0.credit_policy, None);
        assert!(
            records[0].1.is_none(),
            "detected B must fence A's block candidate"
        );
    }
    assert_eq!(
        fixture.coordinator.observed_tip.read().await.as_deref(),
        Some(hash(2).as_str())
    );
    save.release.notify_one();
    assert!(timeout(Duration::from_secs(2), refresh)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    running.stop().await;
    next_wait.release.notify_one();
}

#[tokio::test]
async fn verification_started_after_notification_still_loses_to_a_later_chain_poll() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    fixture.node.lock().unwrap().tip = hash(2);
    let notification = gate(&fixture, "waitfornewblock");
    let running = Running::start(&fixture);
    entered(&notification).await;
    let verification = gate(&fixture, "getblockchaininfo");
    notification.release.notify_one();
    entered(&verification).await;
    fixture.detect(3).await;
    let first_departure = fixture
        .coordinator
        .observed_tip
        .read()
        .await
        .divergence_for_test();
    assert!(first_departure.is_some());
    let next_wait = gate(&fixture, "waitfornewblock");
    verification.release.notify_one();
    entered(&next_wait).await;
    let state = fixture.coordinator.observed_tip.read().await;
    assert_eq!(state.as_deref(), Some(hash(3).as_str()));
    assert_eq!(state.divergence_for_test(), first_departure);
    drop(state);
    fixture
        .submit(&fixture.job(1, 0, "original.worker"), false)
        .await
        .unwrap();
    assert!(fixture.store.records.lock().unwrap()[0].1.is_none());
    running.stop().await;
    next_wait.release.notify_one();
}

#[tokio::test]
async fn failed_or_malformed_verification_revokes_admission_and_wakes_refresh() {
    for bad_hash in [None, Some(""), Some("not-a-chain-hash")] {
        let mut fixture = Fixture::new(Duration::from_secs(10)).await;
        Arc::get_mut(&mut Arc::get_mut(&mut fixture.coordinator).unwrap().config)
            .unwrap()
            .poll_interval = Duration::from_millis(1);
        fixture.coordinator.refresh_once().await.unwrap();
        let before = fixture.coordinator.readiness.read().await.generation;
        fixture.node.lock().unwrap().tip = hash(2);
        let notification = gate(&fixture, "waitfornewblock");
        let running = Running::start(&fixture);
        entered(&notification).await;
        {
            let mut node = fixture.node.lock().unwrap();
            if let Some(hash) = bad_hash {
                node.tip = hash.into();
            } else {
                node.fail = Some("getblockchaininfo".into());
            }
        }
        let next_wait = gate(&fixture, "waitfornewblock");
        notification.release.notify_one();
        entered(&next_wait).await;
        timeout(
            Duration::from_millis(100),
            fixture.coordinator.wake.notified(),
        )
        .await
        .unwrap();
        let readiness = fixture.coordinator.readiness.read().await;
        assert!(readiness.last_poll.is_none());
        assert!(readiness.generation > before);
        drop(readiness);
        assert_error(
            fixture
                .submit(&fixture.job(1, 0, "original.worker"), false)
                .await
                .unwrap_err(),
            "backend-rpc-unavailable",
            "current chain state is unavailable",
        );
        assert!(fixture.store.records.lock().unwrap().is_empty());
        running.stop().await;
        next_wait.release.notify_one();
    }
}

#[tokio::test]
async fn notification_failure_keeps_last_good_readiness_and_cancels_retry_backoff() {
    let mut fixture = Fixture::new(Duration::from_secs(10)).await;
    Arc::get_mut(&mut Arc::get_mut(&mut fixture.coordinator).unwrap().config)
        .unwrap()
        .poll_interval = Duration::from_secs(60);
    fixture.coordinator.refresh_once().await.unwrap();
    let last_poll = fixture.coordinator.readiness.read().await.last_poll;
    fixture.node.lock().unwrap().calls.clear();
    fixture.node.lock().unwrap().fail = Some("waitfornewblock".into());
    let notification = gate(&fixture, "waitfornewblock");
    let running = Running::start(&fixture);
    entered(&notification).await;
    notification.release.notify_one();
    timeout(
        Duration::from_millis(500),
        fixture.coordinator.wake.notified(),
    )
    .await
    .unwrap();
    assert_eq!(
        fixture.coordinator.readiness.read().await.last_poll,
        last_poll
    );
    assert!(
        !running.task.is_finished(),
        "notification failure is not a critical-task exit"
    );
    assert_eq!(fixture.node.lock().unwrap().calls, ["waitfornewblock"]);
    running.stop().await;
}

#[tokio::test]
async fn same_hash_notification_has_no_verification_rpc_and_wait_is_cancellable() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let last_poll = fixture.coordinator.readiness.read().await.last_poll;
    fixture.node.lock().unwrap().calls.clear();
    let notification = gate(&fixture, "waitfornewblock");
    let running = Running::start(&fixture);
    entered(&notification).await;
    let next_wait = gate(&fixture, "waitfornewblock");
    notification.release.notify_one();
    entered(&next_wait).await;
    assert_eq!(
        fixture.node.lock().unwrap().calls,
        ["waitfornewblock", "waitfornewblock"]
    );
    assert_eq!(
        fixture.coordinator.readiness.read().await.last_poll,
        last_poll
    );
    running.stop().await;
    next_wait.release.notify_one();
}

#[tokio::test]
async fn fresh_verification_rpc_is_cancellable_without_publishing_work() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    fixture.node.lock().unwrap().tip = hash(2);
    let notification = gate(&fixture, "waitfornewblock");
    let running = Running::start(&fixture);
    entered(&notification).await;
    let verification = gate(&fixture, "getblockchaininfo");
    notification.release.notify_one();
    entered(&verification).await;
    running.stop().await;
    verification.release.notify_one();
    assert_eq!(
        fixture
            .coordinator
            .prepared
            .read()
            .await
            .as_ref()
            .unwrap()
            .template["previousblockhash"],
        hash(1)
    );
}
