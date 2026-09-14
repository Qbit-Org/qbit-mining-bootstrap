use super::*;

#[tokio::test]
async fn slow_parent_lookup_cannot_overwrite_a_newer_tip_or_parent() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, false).await;
    fixture.observe(2, false).await;
    let gate = Arc::new(Gate::default());
    fixture.node.lock().unwrap().gate = Some(("getblockheader".into(), gate.clone()));
    let coordinator = fixture.coordinator.clone();
    let job = fixture.job(1, 0, "original.worker");
    let proof = fixture.proof(&job, 0);
    let submitted = tokio::spawn(async move {
        coordinator
            .submit(&job.context.worker, &job, proof, true.into())
            .await
    });
    gate.entered.notified().await;
    fixture.observe(3, true).await;
    gate.release.notify_one();
    submitted.await.unwrap().unwrap();
    let selected = fixture.coordinator.submit_tip_view().await.unwrap();
    assert_eq!(selected.hash, hash(3));
    assert_eq!(selected.parent, Some(hash(2)));
    assert!(selected.transitioned);
}

#[tokio::test]
async fn submit_selection_cannot_mix_old_hash_with_new_observation_provenance() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.observe(2, true).await;
    let gate = Arc::new(Gate::default());
    *fixture.store.revision_gate.lock().unwrap() = Some(gate.clone());
    let coordinator = fixture.coordinator.clone();
    let job = fixture.job(1, 0, "original.worker");
    let proof = fixture.proof(&job, 0);
    let submitted = tokio::spawn(async move {
        coordinator
            .submit(&job.context.worker, &job, proof, true.into())
            .await
    });
    gate.entered.notified().await;
    // Mutate all coupled fields while submit awaits an independent boundary.
    // Its selected B/A view must not become B/B or borrow C's baseline flag.
    *fixture.coordinator.observed_tip.write().await = TipState::baseline(hash(3));
    fixture
        .coordinator
        .cache_tip_parent(&hash(3))
        .await
        .unwrap();
    gate.release.notify_one();
    submitted.await.unwrap().unwrap();
    assert_eq!(
        fixture.store.records.lock().unwrap()[0]
            .0
            .credit_policy
            .as_deref(),
        Some("stale-grace")
    );
}

#[tokio::test]
async fn late_chain_info_cannot_roll_back_a_newer_observation() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    let gate = Arc::new(Gate::default());
    fixture.node.lock().unwrap().gate = Some(("getblockchaininfo".into(), gate.clone()));
    let coordinator = fixture.coordinator.clone();
    let slow = tokio::spawn(async move { coordinator.ready_chain_info().await });
    gate.entered.notified().await;
    fixture.observe(2, true).await;
    gate.release.notify_one();
    slow.await.unwrap().unwrap();
    let selected = fixture.coordinator.submit_tip_view().await.unwrap();
    assert_eq!(selected.hash, hash(2));
    assert_eq!(selected.parent, Some(hash(1)));
    assert!(selected.transitioned);
}

#[tokio::test]
async fn cross_frontend_revision_change_before_append_cannot_ack_or_credit() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.observe(2, true).await;
    fixture.store.revision.store(7, Ordering::SeqCst);
    let gate = Arc::new(Gate::default());
    *fixture.store.append_gate.lock().unwrap() = Some(gate.clone());
    let coordinator = fixture.coordinator.clone();
    let job = fixture.job(1, 0, "original.worker");
    let proof = fixture.proof(&job, 0);
    let submitted = tokio::spawn(async move {
        coordinator
            .submit(&job.context.worker, &job, proof, true.into())
            .await
    });
    gate.entered.notified().await;
    fixture.store.revision.store(8, Ordering::SeqCst);
    gate.release.notify_one();
    assert_error(
        submitted.await.unwrap().unwrap_err(),
        "ledger-confirmation-failed",
        "share was not confirmed by the database",
    );
    assert!(fixture.store.records.lock().unwrap().is_empty());
    assert_eq!(fixture.coordinator.accepted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn explicit_readiness_revocation_during_revision_lookup_cannot_credit() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    let gate = Arc::new(Gate::default());
    *fixture.store.revision_gate.lock().unwrap() = Some(gate.clone());
    let coordinator = fixture.coordinator.clone();
    let job = fixture.job(1, 0, "original.worker");
    let proof = fixture.proof(&job, 0);
    let submitted = tokio::spawn(async move {
        coordinator
            .submit(&job.context.worker, &job, proof, false.into())
            .await
    });
    gate.entered.notified().await;
    fixture.coordinator.invalidate_readiness().await;
    gate.release.notify_one();
    assert_error(
        submitted.await.unwrap().unwrap_err(),
        "backend-rpc-unavailable",
        "current chain state is unavailable",
    );
    assert!(fixture.store.records.lock().unwrap().is_empty());
}
