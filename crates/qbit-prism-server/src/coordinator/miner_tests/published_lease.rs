use super::*;

#[tokio::test]
async fn detected_replacement_preserves_published_credit_with_zero_grace_and_current_revision() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture
        .coordinator
        .observed_tip
        .write()
        .await
        .age_for_test(Duration::from_secs(11));
    fixture.detect(2).await;
    fixture.store.revision.store(7, Ordering::SeqCst);
    fixture.node.lock().unwrap().calls.clear();
    fixture
        .submit(&fixture.job(1, 0, "original.worker"), false)
        .await
        .unwrap();
    let records = fixture.store.records.lock().unwrap();
    assert_eq!(
        records[0].0.credit_policy, None,
        "published work is ordinary credit, not stale grace"
    );
    assert_eq!(records[0].0.share_difficulty, 1_000_000);
    assert_eq!(records[0].2, 7);
    assert!(
        records[0].1.is_none(),
        "detected tip independently fences old candidates"
    );
    assert!(fixture.node.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn later_detected_tips_and_failed_refresh_do_not_renew_the_first_divergence() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.detect(2).await;
    let first = fixture
        .coordinator
        .observed_tip
        .read()
        .await
        .divergence_for_test();
    fixture.detect(3).await;
    assert_eq!(
        fixture
            .coordinator
            .observed_tip
            .read()
            .await
            .divergence_for_test(),
        first
    );
    fixture
        .coordinator
        .observed_tip
        .write()
        .await
        .expire_lease_for_test(Duration::from_secs(121));
    fixture.node.lock().unwrap().calls.clear();
    assert_error(
        fixture
            .submit(&fixture.job(1, 0, "original.worker"), false)
            .await
            .unwrap_err(),
        "stale-job",
        "stale job",
    );
    assert_eq!(fixture.node.lock().unwrap().calls, ["getbestblockhash"]);
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn divergence_lease_boundary_is_inclusive_and_zero_max_age_disables_it() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.detect(2).await;
    tokio::time::pause();
    fixture
        .coordinator
        .observed_tip
        .write()
        .await
        .expire_lease_for_test(Duration::from_secs(120));
    let selected = fixture.coordinator.submit_tip_view().await.unwrap();
    assert_eq!(selected.hash, hash(1));
    assert!(selected.share_lease);
    fixture
        .submit(&fixture.job(1, 0, "original.worker"), false)
        .await
        .unwrap();
    tokio::time::advance(Duration::from_nanos(1)).await;
    assert!(fixture
        .coordinator
        .observed_tip
        .read()
        .await
        .authority(Duration::from_secs(10), Duration::from_secs(120))
        .is_none());
    tokio::time::resume();
    let forced = Fixture::new(Duration::ZERO).await;
    forced.observe(1, true).await;
    forced.detect(2).await;
    forced.node.lock().unwrap().calls.clear();
    assert_error(
        forced
            .submit(&forced.job(1, 0, "original.worker"), false)
            .await
            .unwrap_err(),
        "stale-job",
        "stale job",
    );
    assert_eq!(forced.node.lock().unwrap().calls, ["getbestblockhash"]);
}

#[tokio::test]
async fn zero_lease_budget_uses_only_ordinary_freshness() {
    let mut fixture = Fixture::new(Duration::from_secs(10)).await;
    Arc::get_mut(&mut Arc::get_mut(&mut fixture.coordinator).unwrap().config)
        .unwrap()
        .template_refresh_failure_exit = Duration::ZERO;
    fixture.observe(1, true).await;
    fixture.detect(2).await;
    fixture
        .coordinator
        .observed_tip
        .write()
        .await
        .age_for_test(Duration::from_secs(11));
    assert_error(
        fixture
            .submit(&fixture.job(1, 0, "original.worker"), false)
            .await
            .unwrap_err(),
        "stale-job",
        "stale job",
    );
}

#[tokio::test]
async fn candidate_only_observation_cannot_publish_authority_or_open_stale_grace() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.node.lock().unwrap().tip = hash(2);
    fixture.coordinator.ready_chain_info().await.unwrap();
    assert_error(
        fixture
            .submit(&fixture.job(1, 0, "original.worker"), true)
            .await
            .unwrap_err(),
        "stale-job",
        "stale job",
    );
}

#[tokio::test]
async fn returning_to_published_tip_closes_divergence_and_next_departure_starts_new_lease() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.detect(2).await;
    fixture
        .coordinator
        .observed_tip
        .write()
        .await
        .expire_lease_for_test(Duration::from_secs(121));
    fixture.detect(1).await;
    assert!(fixture
        .coordinator
        .observed_tip
        .read()
        .await
        .divergence_for_test()
        .is_none());
    assert!(
        !fixture
            .coordinator
            .submit_tip_view()
            .await
            .unwrap()
            .share_lease
    );
    fixture.detect(2).await;
    assert!(
        fixture
            .coordinator
            .submit_tip_view()
            .await
            .unwrap()
            .share_lease
    );
    fixture
        .submit(&fixture.job(1, 0, "original.worker"), false)
        .await
        .unwrap();
}
