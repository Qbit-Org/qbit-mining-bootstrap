//! Real submit/refresh interleavings; gates replace I/O, never credit decisions.
use super::*;
use futures_util::poll;
use tokio::time::timeout;

#[tokio::test]
async fn newer_same_parent_publication_cannot_lend_its_lease_to_an_older_payout() {
    let mut fixture = Fixture::new(Duration::from_secs(10)).await;
    let config = Arc::get_mut(&mut Arc::get_mut(&mut fixture.coordinator).unwrap().config).unwrap();
    config.ctv_enabled = true;
    config.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
    fixture.coordinator.refresh_once().await.unwrap();
    let worker = fixture.job(1, 0, "original.worker").context.worker.clone();
    let old = fixture
        .coordinator
        .build_job(&worker, "00000000", 1e-12, 0.0)
        .await
        .unwrap();
    // Construct both publications through the real refresh path, then replay
    // their handoff below while the submit future is stopped at its locks.
    fixture.store.revision.store(1, Ordering::SeqCst);
    fixture.coordinator.refresh_once().await.unwrap();
    let new = fixture
        .coordinator
        .build_job(&worker, "00000001", 1e-12, 0.0)
        .await
        .unwrap();
    assert_eq!(new.context.prepared.snapshot.payout_revision, 1);
    fixture.store.revision.store(0, Ordering::SeqCst);
    *fixture.coordinator.prepared.write().await = Some(old.context.prepared.clone());
    let mut proof = fixture.proof(&old, 0);
    proof.block_pass = false;

    // Stop at the prepared read after the initial readiness read. Queue a
    // readiness writer before allowing that read to finish: the old split
    // selector then pauses at the fee check with only A/R0 captured.
    let prepared = fixture.coordinator.prepared.write().await;
    let submitted = fixture
        .coordinator
        .submit(&worker, &old, proof, false.into());
    tokio::pin!(submitted);
    assert!(poll!(&mut submitted).is_pending());
    let readiness = fixture.coordinator.readiness.write().await;
    drop(prepared);
    let gate = Arc::new(Gate::default());
    *fixture.store.revision_gate.lock().unwrap() = Some(gate.clone());
    assert!(poll!(&mut submitted).is_pending());
    // The old path waits at the fee check with only its prepared payout. The
    // fixed path checked the fee first and selected A/R0 without a lease,
    // then waits on the revision I/O. Neither holds publication locks now.
    fixture.store.revision_gate.lock().unwrap().take();
    fixture.store.revision.store(1, Ordering::SeqCst);
    {
        let mut prepared = fixture.coordinator.prepared.write().await;
        let mut observed = fixture.coordinator.observed_tip.write().await;
        observed.publish(&hash(1)).unwrap();
        *prepared = Some(new.context.prepared.clone());
    }
    fixture.detect(2).await;
    drop(readiness);
    gate.release.notify_one();
    let result = timeout(Duration::from_secs(5), submitted).await.unwrap();
    assert_error(
        result.expect_err("A/R1's replacement lease must not credit A/R0"),
        "ledger-confirmation-failed",
        "share was not confirmed by the database",
    );
    assert!(fixture.store.records.lock().unwrap().is_empty());
    // The actual selected publication still earns ordinary credit, not a
    // stale-grace policy, with its issued economics and no old-tip candidate.
    fixture.submit(&new, false).await.unwrap();
    let records = fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0.credit_policy, None);
    assert!(records[0].1.is_none());
    assert_eq!(records[0].2, 1);
}

#[tokio::test]
async fn already_invalid_ctv_fee_rejects_before_an_uncached_tip_rpc_outage() {
    let mut fixture = Fixture::new(Duration::ZERO).await;
    let config = Arc::get_mut(&mut Arc::get_mut(&mut fixture.coordinator).unwrap().config).unwrap();
    config.ctv_enabled = true;
    config.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
    fixture.coordinator.refresh_once().await.unwrap();
    let worker = fixture.job(1, 0, "original.worker").context.worker.clone();
    let issued = fixture
        .coordinator
        .build_job(&worker, "00000000", 1e-12, 0.0)
        .await
        .unwrap();
    fixture.coordinator.readiness.write().await.ctv_fee_floor = Some(2000);
    {
        let mut node = fixture.node.lock().unwrap();
        node.calls.clear();
        node.fail = Some("getbestblockhash".into());
    }
    assert_error(
        fixture.submit(&issued, false).await.unwrap_err(),
        "stale-job",
        "job CTV fee is below the current relay floor",
    );
    assert!(fixture.node.lock().unwrap().calls.is_empty());
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn fallback_reselects_real_publication_completed_during_its_rpc() {
    for max_age in [Duration::ZERO, Duration::from_secs(10)] {
        let fixture = Fixture::new(max_age).await;
        fixture.coordinator.refresh_once().await.unwrap();
        let old = fixture.job(1, 0, "original.worker");
        fixture
            .coordinator
            .observed_tip
            .write()
            .await
            .age_for_test(Duration::from_secs(11));
        fixture.node.lock().unwrap().tip = hash(2);
        let gate = Arc::new(Gate::default());
        fixture.node.lock().unwrap().gate = Some(("getbestblockhash".into(), gate.clone()));
        let coordinator = fixture.coordinator.clone();
        let proof = fixture.proof(&old, 0);
        let submitted = tokio::spawn(async move {
            coordinator
                .submit(&old.context.worker, &old, proof, true.into())
                .await
        });
        timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        timeout(Duration::from_secs(5), fixture.coordinator.refresh_once())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fixture
                .coordinator
                .prepared
                .read()
                .await
                .as_ref()
                .unwrap()
                .template["previousblockhash"],
            hash(2)
        );
        gate.release.notify_one();
        timeout(Duration::from_secs(5), submitted)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let records = fixture.store.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0.credit_policy.as_deref(), Some("stale-grace"));
        assert_eq!(records[0].0.share_difficulty, 1_000_000);
        assert_eq!(records[0].0.network_difficulty, 1_000_000);
        assert!(records[0].0.share_id.starts_with("original.worker:"));
        assert!(records[0].1.is_none());
        assert_eq!(records[0].2, 1);
    }
}

#[tokio::test]
async fn fallback_cannot_borrow_a_different_publications_transition() {
    let fixture = Fixture::new(Duration::ZERO).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let old = fixture.job(1, 0, "original.worker");
    fixture.node.lock().unwrap().tip = hash(2);
    let gate = Arc::new(Gate::default());
    fixture.node.lock().unwrap().gate = Some(("getbestblockhash".into(), gate.clone()));
    let coordinator = fixture.coordinator.clone();
    let proof = fixture.proof(&old, 0);
    let submitted = tokio::spawn(async move {
        coordinator
            .submit(&old.context.worker, &old, proof, true.into())
            .await
    });
    timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    // C is another child of A: substituting the latest authority for the RPC
    // answer would incorrectly grant grace, just like borrowing C's flag.
    {
        let mut node = fixture.node.lock().unwrap();
        node.tip = hash(3);
        node.parents.insert(hash(3), hash(1));
    }
    timeout(Duration::from_secs(5), fixture.coordinator.refresh_once())
        .await
        .unwrap()
        .unwrap();
    gate.release.notify_one();
    assert_error(
        timeout(Duration::from_secs(5), submitted)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        "stale-job",
        "stale job",
    );
    assert!(fixture.store.records.lock().unwrap().is_empty());
}
