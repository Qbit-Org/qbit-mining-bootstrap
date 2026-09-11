use super::*;

#[tokio::test]
async fn observed_tip_pending_prepared_and_revision_still_credits_exact_prior_parent() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let job = fixture.job(1, 0, "original.worker");
    fixture.observe(1, true).await;
    fixture.observe(2, true).await;
    fixture.store.revision.store(7, Ordering::SeqCst);
    // Refresh observed the tip and changed durable revision, but publication
    // is slow/failed. This connection has not received replacement work.
    fixture.submit(&job, true).await.unwrap();
    let records = fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    let (share, candidate, revision) = &records[0];
    assert_eq!(*revision, 7, "append must enforce the CURRENT revision");
    assert_eq!(share.credit_policy.as_deref(), Some("stale-grace"));
    assert_eq!(share.miner_id, "miner-original");
    assert_eq!(share.order_key, "miner-original");
    assert_eq!(share.p2mr_program_hex, hash(0xab));
    assert!(share.share_id.starts_with("original.worker:"));
    assert_eq!(share.share_difficulty, 1_000_000);
    assert_eq!(share.network_difficulty, 1_000_000);
    assert_eq!(share.template_height, 100);
    assert_eq!(share.job_issued_at_ms, 100_000);
    assert_eq!(share.job_id, "issued-job");
    assert!(
        candidate.is_none(),
        "a stale block proof earns share credit only"
    );
}

#[tokio::test]
async fn grace_uses_delivery_not_first_seen_and_closes_after_exact_boundary() {
    let fixture = Fixture::new(Duration::from_secs(200)).await;
    let job = fixture.job(1, 0, "original.worker");
    fixture.observe(1, true).await;
    fixture.observe(2, true).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(100)).await;
    let delivered_at = tokio::time::Instant::now().into_std();
    let delivery = Some((hash(2), delivered_at));
    tokio::time::advance(Duration::from_secs(3)).await;
    fixture
        .submit(
            &job,
            StaleGrace::for_connection(Duration::from_secs(3), delivery.clone()),
        )
        .await
        .unwrap();
    tokio::time::advance(Duration::from_nanos(1)).await;
    assert_error(
        fixture
            .submit(
                &job,
                StaleGrace::for_connection(Duration::from_secs(3), delivery),
            )
            .await
            .unwrap_err(),
        "stale-job",
        "stale job",
    );
}

#[tokio::test]
async fn expired_delivery_to_another_tip_cannot_close_undelivered_reorg_grace() {
    let fixture = Fixture::new(Duration::from_secs(200)).await;
    let job = fixture.job(1, 0, "original.worker");
    fixture.observe(1, true).await;
    fixture.observe(2, true).await;
    let delivered = Some((hash(2), tokio::time::Instant::now().into_std()));
    // C is a sibling of B, still building on the original issued parent A.
    fixture
        .node
        .lock()
        .unwrap()
        .parents
        .insert(hash(3), hash(1));
    fixture.observe(3, true).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(20)).await;
    fixture
        .submit(
            &job,
            StaleGrace::for_connection(Duration::from_secs(3), delivered),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn startup_baseline_opens_no_grace_but_a_later_real_transition_does() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let old = fixture.job(1, 0, "original.worker");
    fixture.observe(2, true).await;
    assert_error(
        fixture.submit(&old, true).await.unwrap_err(),
        "stale-job",
        "stale job",
    );
    fixture
        .node
        .lock()
        .unwrap()
        .parents
        .insert(hash(3), hash(1));
    fixture.observe(3, true).await;
    fixture.submit(&old, true).await.unwrap();
}

#[tokio::test]
async fn skipped_tip_uses_chain_parent_and_rejects_two_blocks_back() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.observe(3, true).await;
    let old = fixture.job(1, 0, "original.worker");
    assert_error(
        fixture.submit(&old, true).await.unwrap_err(),
        "stale-job",
        "stale job",
    );
    let intermediate = fixture.job(2, 0, "original.worker");
    fixture.submit(&intermediate, true).await.unwrap();
    assert_eq!(fixture.store.records.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn same_tip_retained_work_credits_normally_and_reauthorization_dedups_original_worker() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    let job = fixture.job(1, 0, "original.worker");
    let mut newly_authorized = job.context.worker.clone();
    newly_authorized.username = "new.worker".into();
    let proof = fixture.proof(&job, 0);
    fixture
        .coordinator
        .submit(&newly_authorized, &job, proof.clone(), false.into())
        .await
        .unwrap();
    assert_error(
        fixture
            .coordinator
            .submit(&newly_authorized, &job, proof, false.into())
            .await
            .unwrap_err(),
        "duplicate-share",
        "duplicate share",
    );
    let records = fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert!(records[0].0.share_id.starts_with("original.worker:"));
    assert_eq!(records[0].0.credit_policy, None);
    assert_eq!(records[0].0.share_difficulty, 1_000_000);
    assert!(
        records[0].1.is_some(),
        "current block proof retains candidate behavior"
    );
}

#[tokio::test]
async fn same_parent_payout_replacement_has_no_grace_exception() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.store.revision.store(1, Ordering::SeqCst);
    let old = fixture.job(1, 0, "original.worker");
    for grace in [false, true] {
        assert_error(
            fixture.submit(&old, grace).await.unwrap_err(),
            "stale-job",
            "stale job",
        );
    }
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn stale_network_block_below_share_target_earns_no_credit_or_candidate() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.observe(2, true).await;
    let job = fixture.job(1, 0, "original.worker");
    let mut proof = fixture.proof(&job, 0);
    proof.share_pass = false;
    assert_error(
        fixture
            .coordinator
            .submit(&job.context.worker, &job, proof, true.into())
            .await
            .unwrap_err(),
        "low-difficulty",
        "low difficulty share",
    );
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unhealthy_node_and_failed_revision_are_backend_unavailable_not_stale() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let job = fixture.job(1, 0, "original.worker");
    fixture.observe(1, true).await;
    fixture.observe(2, true).await;
    fixture.store.fail_revision.store(true, Ordering::SeqCst);
    assert_error(
        fixture.submit(&job, true).await.unwrap_err(),
        "backend-rpc-unavailable",
        "current payout state is unavailable",
    );
    fixture.coordinator.invalidate_readiness().await;
    assert_error(
        fixture.submit(&job, true).await.unwrap_err(),
        "backend-rpc-unavailable",
        "current chain state is unavailable",
    );
    assert!(fixture.store.records.lock().unwrap().is_empty());
}
