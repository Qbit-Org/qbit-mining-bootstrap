use super::*;

#[tokio::test]
async fn observed_tip_serves_submit_without_node_rpc() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.node.lock().unwrap().calls.clear();
    fixture
        .submit(&fixture.job(1, 0, "original.worker"), false)
        .await
        .unwrap();
    assert!(fixture.node.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn no_observation_falls_back_to_rpc_without_opening_grace() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let job = fixture.job(1, 0, "original.worker");
    fixture.submit(&job, false).await.unwrap();
    assert_eq!(fixture.node.lock().unwrap().calls, ["getbestblockhash"]);
    assert!(fixture
        .coordinator
        .observed_tip
        .read()
        .await
        .as_deref()
        .is_none());
    fixture.node.lock().unwrap().tip = hash(2);
    assert_error(
        fixture.submit(&job, true).await.unwrap_err(),
        "stale-job",
        "stale job",
    );
    assert!(fixture
        .coordinator
        .observed_tip
        .read()
        .await
        .as_deref()
        .is_none());
}

#[tokio::test]
async fn stale_observation_falls_back_to_rpc_and_does_not_invent_transition() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    // Age the observed monotonic instant directly; no sleeping or live node.
    {
        let mut state = fixture.coordinator.observed_tip.write().await;
        state.age_for_test(Duration::from_secs(11));
    }
    fixture.node.lock().unwrap().calls.clear();
    fixture.node.lock().unwrap().tip = hash(2);
    assert_error(
        fixture
            .submit(&fixture.job(1, 0, "original.worker"), true)
            .await
            .unwrap_err(),
        "stale-job",
        "stale job",
    );
    assert_eq!(fixture.node.lock().unwrap().calls, ["getbestblockhash"]);
}

#[tokio::test]
async fn zero_max_age_forces_per_share_rpc_even_with_fresh_observation() {
    let fixture = Fixture::new(Duration::ZERO).await;
    fixture.observe(1, true).await;
    fixture.node.lock().unwrap().calls.clear();
    let job = fixture.job(1, 0, "original.worker");
    fixture.submit(&job, false).await.unwrap();
    assert_error(
        fixture.submit(&job, false).await.unwrap_err(),
        "duplicate-share",
        "duplicate share",
    );
    assert_eq!(
        fixture.node.lock().unwrap().calls,
        ["getbestblockhash", "getbestblockhash"]
    );
}

#[tokio::test]
async fn parent_rpc_failure_during_grace_is_backend_unavailable() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.observe(2, false).await;
    fixture.node.lock().unwrap().calls.clear();
    fixture.node.lock().unwrap().fail = Some("getblockheader".into());
    assert_error(
        fixture
            .submit(&fixture.job(1, 0, "original.worker"), true)
            .await
            .unwrap_err(),
        "backend-rpc-unavailable",
        "current tip parent is unavailable",
    );
    assert_eq!(fixture.node.lock().unwrap().calls, ["getblockheader"]);
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn failed_fallback_cannot_turn_unknown_chain_state_into_stale() {
    let fixture = Fixture::new(Duration::ZERO).await;
    fixture.node.lock().unwrap().fail = Some("getbestblockhash".into());
    assert_error(
        fixture
            .submit(&fixture.job(1, 0, "original.worker"), true)
            .await
            .unwrap_err(),
        "backend-rpc-unavailable",
        "current chain state is unavailable",
    );
}

#[tokio::test]
async fn refresh_observation_rpc_count_is_independent_of_client_submit_count() {
    for clients in [1, 32, 128] {
        let fixture = Fixture::new(Duration::from_secs(30)).await;
        // Same production observation/predecessor phase called by refresh_once.
        fixture.observe(1, true).await;
        fixture.observe(2, true).await;
        let before = fixture.node.lock().unwrap().calls.clone();
        for client in 0..clients {
            let job = fixture.job(1, 0, &format!("original.worker{client}"));
            fixture.submit(&job, true).await.unwrap();
        }
        assert_eq!(fixture.node.lock().unwrap().calls, before);
        assert_eq!(
            before,
            [
                "getblockchaininfo",
                "getblockheader",
                "getblockchaininfo",
                "getblockheader"
            ]
        );
        assert_eq!(fixture.store.records.lock().unwrap().len(), clients);
    }
}
