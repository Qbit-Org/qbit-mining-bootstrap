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
    // A plain SHARE on the superseded job is refused credit: a same-parent
    // payout replacement grants no stale-grace exception. (#478 Option B now
    // captures a BLOCK on such a job — pinned by
    // `tests/b478_stale_revision_block.rs` and `readiness_rpc` — but its share
    // credit stays fenced, so this asserts the share path with a non-block proof.)
    let mut proof = fixture.proof(&old, 0);
    proof.block_pass = false;
    for grace in [false, true] {
        assert_error(
            fixture
                .coordinator
                .submit(&old.context.worker, &old, proof.clone(), grace.into())
                .await
                .unwrap_err(),
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
    // #581: the payout revision is a database read, so its failure names
    // the database, never the node.
    assert_error(
        fixture.submit(&job, true).await.unwrap_err(),
        "backend-database-unavailable",
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

/// #581: a refresh reads the database, and a guarded tip poll (#622) renews
/// readiness only while the published work is current, so during a database
/// outage or a stall past the health timeout readiness can age out. The
/// stale-readiness refusal then names the database the latest refresh failed
/// on, and the node once a refresh fails on the node instead.
#[tokio::test]
async fn stale_readiness_names_the_dependency_the_latest_refresh_failed_on() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let job = fixture.job(1, 0, "original.worker");
    fixture.observe(1, true).await;
    fixture.store.fail_revision.store(true, Ordering::SeqCst);
    assert!(fixture.coordinator.refresh_once().await.is_err());
    age_readiness_past_the_health_timeout(&fixture).await;
    assert_error(
        fixture.submit(&job, true).await.unwrap_err(),
        "backend-database-unavailable",
        "current chain state is unavailable",
    );

    // The database recovers and the node fails the next refresh instead.
    fixture.store.fail_revision.store(false, Ordering::SeqCst);
    fixture.node.lock().unwrap().fail = Some("getblocktemplate".into());
    assert!(fixture.coordinator.refresh_once().await.is_err());
    age_readiness_past_the_health_timeout(&fixture).await;
    assert_error(
        fixture.submit(&job, true).await.unwrap_err(),
        "backend-rpc-unavailable",
        "current chain state is unavailable",
    );
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

/// #581: a refresh records whether it failed on the database before it
/// releases the refresh lock, so the outcomes land in the order the
/// refreshes ran and an older refresh that finishes late can never overwrite
/// a newer one's (Codex on #646). The older refresh here fails on the
/// database and is held from recording; the newer one cannot start until it
/// has.
#[tokio::test]
async fn a_refresh_records_its_database_failure_before_the_next_refresh_runs() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    fixture.store.fail_revision.store(true, Ordering::SeqCst);
    let gate = Arc::new(Gate::default());
    *fixture.store.revision_gate.lock().unwrap() = Some(gate.clone());
    let older = tokio::spawn({
        let coordinator = fixture.coordinator.clone();
        async move { coordinator.refresh_once().await }
    });
    gate.entered.notified().await;
    // Readiness held: the older refresh finishes its work and waits to
    // record its outcome.
    let readiness = fixture.coordinator.readiness.read().await;
    gate.release.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        fixture.coordinator.refresh_lock.try_lock().is_err(),
        "the refresh released its lock before recording its outcome"
    );
    fixture.store.fail_revision.store(false, Ordering::SeqCst);
    let newer = tokio::spawn({
        let coordinator = fixture.coordinator.clone();
        async move { coordinator.refresh_once().await }
    });
    drop(readiness);
    assert!(older.await.unwrap().is_err());
    newer.await.unwrap().unwrap();
    assert!(
        !fixture
            .coordinator
            .readiness
            .read()
            .await
            .refresh_failed_on_database,
        "the older refresh's database failure overwrote the newer refresh's success"
    );
}

/// A fixture whose readiness, and the deadline of a refresh, last 300 ms, so
/// both pass in real time while a refresh is held.
async fn short_health_timeout() -> Fixture {
    Fixture::build(
        Duration::from_secs(10),
        |config| config.health_timeout = Duration::from_millis(300),
        None,
    )
    .await
}

/// Start a refresh in the background, as the refresh loop runs one.
fn spawn_refresh(fixture: &Fixture) -> tokio::task::JoinHandle<Result<()>> {
    let coordinator = fixture.coordinator.clone();
    tokio::spawn(async move { coordinator.refresh_once().await })
}

/// #655: a refresh that hangs on the database, rather than failing, records
/// no database failure. Once it has outlived its deadline, the health
/// timeout, still waiting on the database, readiness that aged out meanwhile
/// is the database's. Here its first ledger read is held, as a held lock or a
/// full pool holds it. Before #655 the refusal blamed the node, because the
/// latest finished refresh had not failed on the database.
#[tokio::test]
async fn a_refresh_blocked_on_the_database_past_its_deadline_names_the_database() {
    let fixture = short_health_timeout().await;
    let job = fixture.job(1, 0, "original.worker");
    fixture.observe(1, true).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let gate = Arc::new(Gate::default());
    *fixture.store.revision_gate.lock().unwrap() = Some(gate.clone());
    let refresh = spawn_refresh(&fixture);
    gate.entered.notified().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_error(
        fixture.submit(&job, true).await.unwrap_err(),
        "backend-database-unavailable",
        "current chain state is unavailable",
    );
    gate.release.notify_one();
    refresh.await.unwrap().unwrap();
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

/// #655: a refresh is in flight from its start to the moment its outcome is
/// recorded, and a cancelled one is not left behind to name a later refusal.
#[tokio::test]
async fn a_refresh_is_in_flight_only_while_it_runs() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let in_flight = |fixture: &Fixture| {
        fixture
            .coordinator
            .refresh_in_flight
            .lock()
            .unwrap()
            .as_ref()
            .map(|refresh| refresh.waiting.on())
    };
    for cancel in [false, true] {
        let gate = Arc::new(Gate::default());
        *fixture.store.revision_gate.lock().unwrap() = Some(gate.clone());
        let refresh = spawn_refresh(&fixture);
        gate.entered.notified().await;
        assert_eq!(in_flight(&fixture), Some(Dependency::Database));
        if cancel {
            refresh.abort();
            assert!(refresh.await.unwrap_err().is_cancelled());
        } else {
            gate.release.notify_one();
            refresh.await.unwrap().unwrap();
        }
        assert_eq!(in_flight(&fixture), None, "cancelled: {cancel}");
    }
}

/// #655: the node keeps its label. A refresh held in a node call past its
/// deadline is the node's, and a refresh held on the database but still
/// inside its deadline names nothing new: the refusal keeps the dependency
/// the latest finished refresh failed on, here none, so the node's.
#[tokio::test]
async fn a_refresh_blocked_on_the_node_or_within_its_deadline_keeps_the_nodes_label() {
    let fixture = short_health_timeout().await;
    let job = fixture.job(1, 0, "original.worker");
    fixture.observe(1, true).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let gate = Arc::new(Gate::default());
    fixture.node.lock().unwrap().gate = Some(("getblocktemplate".into(), gate.clone()));
    let refresh = spawn_refresh(&fixture);
    gate.entered.notified().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_error(
        fixture.submit(&job, true).await.unwrap_err(),
        "backend-rpc-unavailable",
        "current chain state is unavailable",
    );
    gate.release.notify_one();
    refresh.await.unwrap().unwrap();

    // The default 15 s health timeout: readiness is aged by hand, and the
    // refresh held on the database has just started.
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let gate = Arc::new(Gate::default());
    *fixture.store.revision_gate.lock().unwrap() = Some(gate.clone());
    let refresh = spawn_refresh(&fixture);
    gate.entered.notified().await;
    age_readiness_past_the_health_timeout(&fixture).await;
    assert_error(
        fixture.submit(&job, true).await.unwrap_err(),
        "backend-rpc-unavailable",
        "current chain state is unavailable",
    );
    gate.release.notify_one();
    refresh.await.unwrap().unwrap();
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

/// The readiness proof as old as the health timeout and a second more: the
/// age failing refreshes leave it at.
async fn age_readiness_past_the_health_timeout(fixture: &Fixture) {
    let health_timeout = fixture.coordinator.config.health_timeout;
    let mut readiness = fixture.coordinator.readiness.write().await;
    if let Some(last_poll) = readiness.last_poll.as_mut() {
        *last_poll -= health_timeout + Duration::from_secs(1);
    }
}
