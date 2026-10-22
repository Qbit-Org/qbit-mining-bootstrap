//! Bounded original-worker retention with real Coordinator persistence/resume.
use super::*;

#[tokio::test]
async fn undelivered_grace_survives_zero_or_expired_submit_cache_and_active_eviction() {
    for max_age in [Duration::ZERO, Duration::from_secs(1)] {
        let mut client = Connection::with_max_age(3.0, 1, max_age).await;
        let old = client.deliver().await;
        client.deliver().await;
        client.tip(2).await;
        // Real reconnect recovery refuses prior-parent work. Only provenance
        // retained by THIS connection can preserve the delivery-based grace.
        assert!(client
            .backend
            .fixture
            .coordinator
            .resume_job(&old.context.worker, &old.wire.job_id)
            .await
            .unwrap()
            .is_none());
        *client.backend.fail_build.lock().unwrap() = true;
        client.pause_time();
        tokio::time::advance(Duration::from_secs(20)).await;
        assert_eq!(client.submit(&old, 0).await["result"], true);
        *client.backend.fail_build.lock().unwrap() = false;
        client.deliver().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(client.submit(&old, 100).await["result"], true);
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(
            client.submit(&old, 200).await["error"][2]["reason_id"],
            "unknown-job"
        );
        let records = client.backend.fixture.store.records.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|(share, candidate, _)| {
            share.credit_policy.as_deref() == Some("stale-grace") && candidate.is_none()
        }));
        drop(records);
        tokio::time::resume();
    }
}

#[tokio::test]
async fn local_graveyard_cannot_authorize_another_connections_worker_or_old_parent() {
    let mut client = Connection::new(30.0, 1).await;
    let old = client.deliver().await;
    client.deliver().await;
    let mut other = old.context.worker.clone();
    other.username = "another.worker".into();
    assert!(client
        .backend
        .fixture
        .coordinator
        .resume_job(&other, &old.wire.job_id)
        .await
        .unwrap()
        .is_none());
    let resumed = client
        .backend
        .fixture
        .coordinator
        .resume_job(&old.context.worker, &old.wire.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.context.worker.username, "original.worker");
    assert!(resumed.wire.resume_expires_at.is_some());
    client
        .backend
        .fixture
        .coordinator
        .submit(
            &old.context.worker,
            &resumed,
            client.backend.fixture.proof(&resumed, 0),
            false.into(),
        )
        .await
        .unwrap();
    client.tip(2).await;
    assert!(client
        .backend
        .fixture
        .coordinator
        .resume_job(&old.context.worker, &old.wire.job_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn graveyard_burial_ttl_and_same_tip_capacity_keep_original_identity_bounded() {
    let mut client = Connection::new(30.0, 1).await;
    client.pause_time();
    let oldest = client.deliver().await;
    let retained = client.deliver().await;
    client.deliver().await;
    assert!(client.session.retained.get(&oldest.wire.job_id).is_none());
    assert!(client.session.retained.get(&retained.wire.job_id).is_some());
    client.session.worker.as_mut().unwrap().username = "new.worker".into();
    assert_eq!(
        client.submit(&oldest, 0).await["error"][2]["reason_id"],
        "unknown-job"
    );
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(client.submit(&retained, 0).await["result"], true);
    tokio::time::advance(Duration::from_nanos(1)).await;
    assert_eq!(
        client.submit(&retained, 100).await["error"][2]["reason_id"],
        "unknown-job"
    );
    let records = client.backend.fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert!(records[0].0.share_id.starts_with("original.worker:"));
}

#[tokio::test]
async fn graveyard_username_permit_reuses_and_releases_only_at_actual_retention_end() {
    for expire in [false, true] {
        let mut client = Connection::new(30.0, 1).await;
        let permit_pool = Arc::new(Semaphore::new(1));
        client.session.authorization_permit =
            Some(Arc::new(permit_pool.clone().acquire_owned().await.unwrap()));
        let original = client.deliver().await;
        client.session.authorization_permit = None;
        client.deliver().await;
        assert_eq!(permit_pool.available_permits(), 0);
        assert!(client.session.retained.permit("original.worker").is_some());
        if expire {
            client.pause_time();
            tokio::time::advance(Duration::from_secs(31)).await;
            let hint = client.backend.observed_tip_hint().await;
            client.session.prune_jobs(&client.config, hint.as_ref());
        } else {
            client.deliver().await; // The retained same-tip cap evicts original.
        }
        assert!(client.session.retained.get(&original.wire.job_id).is_none());
        assert_eq!(permit_pool.available_permits(), 1);
        if expire {
            tokio::time::resume();
        }
    }
}

#[tokio::test]
async fn graveyard_removes_known_unrelated_parent_instead_of_extending_grace() {
    let mut client = Connection::new(30.0, 1).await;
    let old = client.deliver().await;
    client.deliver().await;
    client.tip(3).await; // C's parent is B, not issued A.
    let response = client.submit(&old, 0).await;
    assert_eq!(response["error"][2]["reason_id"], "unknown-job");
    assert!(client.session.retained.get(&old.wire.job_id).is_none());
    assert!(client
        .backend
        .fixture
        .store
        .records
        .lock()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn rapid_publications_keep_the_graveyard_bounded_before_its_next_prune() {
    let mut client = Connection::new(30.0, 1).await;
    let mut ids = Vec::new();
    for tip in [1, 2, 3] {
        if tip != 1 {
            client.tip(tip).await;
        }
        for _ in 0..2 {
            ids.push(client.deliver().await.wire.job_id);
            assert!(
                ids.iter()
                    .filter(|id| client.session.retained.get(id).is_some())
                    .count()
                    <= 3
            );
        }
    }
    let hint = client.backend.observed_tip_hint().await;
    client.session.prune_jobs(&client.config, hint.as_ref());
    assert!(ids[..2]
        .iter()
        .all(|id| client.session.retained.get(id).is_none()));
}

#[tokio::test]
async fn published_tip_capacity_preserves_prior_work_when_current_work_resumes_without_delivery() {
    let mut client = Connection::new(30.0, 1).await;
    let permits = Arc::new(Semaphore::new(1));
    client.session.authorization_permit =
        Some(Arc::new(permits.clone().acquire_owned().await.unwrap()));
    let oldest = client.deliver().await;
    client.session.authorization_permit = None;
    let prior_active = client.deliver().await;
    let delivered = client.session.tip_work_delivered.clone();
    client.tip(2).await;
    // This is genuine persisted B work for the same username, recovered by
    // Coordinator::resume_job. This connection has received no B notify.
    let current = client.backend.next.lock().unwrap().clone();
    client
        .backend
        .fixture
        .coordinator
        .persist_issued_job(
            &current.context.worker,
            &current,
            0,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    client.backend.fixture.node.lock().unwrap().calls.clear();
    assert_eq!(client.submit(&current, 0).await["result"], true);
    assert_eq!(client.session.tip_work_delivered, delivered);
    assert_eq!(client.session.jobs.len(), 1);
    assert!(client
        .session
        .retained
        .get(&prior_active.wire.job_id)
        .is_some());
    let response = client.submit(&oldest, 0).await;
    assert_eq!(
        response["result"], true,
        "old prior-parent work must survive: {response}"
    );
    assert!(client.session.retained.get(&oldest.wire.job_id).is_some());
    assert_eq!(permits.available_permits(), 0);
    let records = client.backend.fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 2);
    let (share, candidate, _) = &records[1];
    assert_eq!(share.job_id, oldest.wire.job_id);
    assert!(share.share_id.starts_with("original.worker:"));
    assert_eq!(
        share.job_issued_at_ms,
        oldest.context.prepared.snapshot.anchor_ms
    );
    assert_eq!(
        share.network_difficulty,
        oldest.context.bundle.found_block.network_difficulty
    );
    assert_eq!(share.credit_policy.as_deref(), Some("stale-grace"));
    assert!(candidate.is_none());
    drop(records);
    assert!(client.backend.fixture.node.lock().unwrap().calls.is_empty());
    drop(client);
    assert_eq!(permits.available_permits(), 1);
}

#[tokio::test]
async fn published_tip_capacity_is_reselected_after_awaited_resume() {
    let mut client = Connection::new(30.0, 1).await;
    let oldest = client.deliver().await;
    let prior_active = client.deliver().await;
    let worker = oldest.context.worker.clone();
    let unseen = client
        .backend
        .build_job(&worker, "00000000", 1e-12, 0.0)
        .await
        .unwrap();
    client
        .backend
        .persist_issued_job(&worker, &unseen, 0, Duration::from_secs(30))
        .await
        .unwrap();
    let delivered = client.session.tip_work_delivered.clone();
    let gate = Arc::new(Gate::default());
    *client.backend.slow_resume.lock().unwrap() =
        Some((Instant::now() + Duration::from_secs(30), gate.clone()));
    let coordinator = client.backend.fixture.coordinator.clone();
    let node = client.backend.fixture.node.clone();
    let (response, ()) = tokio::join!(client.submit(&unseen, 0), async move {
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        // The request-entry hint was A. Publish B only after the real resume
        // validated A's stored ownership, payout, parent and absolute expiry.
        node.lock().unwrap().tip = "02".repeat(32);
        coordinator.refresh_once().await.unwrap();
        node.lock().unwrap().calls.clear();
        gate.release.notify_one();
    });
    assert_eq!(response["error"][2]["reason_id"], "stale-job");
    assert!(client
        .backend
        .fixture
        .store
        .records
        .lock()
        .unwrap()
        .is_empty());
    assert_eq!(client.session.tip_work_delivered, delivered);
    assert_eq!(client.session.jobs.len(), 1);
    assert!(client
        .session
        .retained
        .get(&prior_active.wire.job_id)
        .is_some());
    let response = client.submit(&oldest, 0).await;
    assert_eq!(
        response["result"], true,
        "failed resumed work must not evict eligible prior work: {response}"
    );
    let records = client.backend.fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0.job_id, oldest.wire.job_id);
    assert_eq!(records[0].0.credit_policy.as_deref(), Some("stale-grace"));
    assert!(records[0].1.is_none());
    assert!(client.backend.fixture.node.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn published_tip_capacity_is_reselected_after_delayed_prior_tip_delivery() {
    let mut client = Connection::new(30.0, 1).await;
    let oldest = client.deliver().await;
    let prior_active = client.deliver().await;
    let delivered = client.session.tip_work_delivered.clone();
    let gate = Arc::new(Gate::default());
    *client.backend.after_persist.lock().unwrap() = Some(gate.clone());
    let coordinator = client.backend.fixture.coordinator.clone();
    let node = client.backend.fixture.node.clone();
    let (late, ()) = tokio::join!(client.deliver(), async move {
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        // A2 is durably persisted but has not yet been written to the socket.
        node.lock().unwrap().tip = "02".repeat(32);
        coordinator.refresh_once().await.unwrap();
        node.lock().unwrap().calls.clear();
        gate.release.notify_one();
    });
    assert_eq!(late.wire.previousblockhash, oldest.wire.previousblockhash);
    assert_eq!(client.session.tip_work_delivered, delivered);
    assert_eq!(client.session.jobs.len(), 1);
    assert!(client
        .session
        .retained
        .get(&prior_active.wire.job_id)
        .is_some());
    let response = client.submit(&oldest, 0).await;
    assert_eq!(
        response["result"], true,
        "delayed A delivery must use B's capacity class: {response}"
    );
    let records = client.backend.fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0.job_id, oldest.wire.job_id);
    assert_eq!(records[0].0.credit_policy.as_deref(), Some("stale-grace"));
    assert!(records[0].1.is_none());
    assert!(client.backend.fixture.node.lock().unwrap().calls.is_empty());
}

/// #478 STEP 2: a same-parent payout replacement buries the superseded job so a
/// block found on it can still be offered for the retention window, instead of
/// discarding it. A previous-parent job and a same-revision job are unaffected.
/// (`submit_share` fences the share credit; the wire capture is covered by the
/// coordinator pin `tests/b478_stale_revision_block.rs`.)
#[tokio::test]
async fn bury_superseded_same_parent_retains_the_block_but_leaves_other_jobs_live() {
    let fixture = Fixture::new(Duration::from_secs(600)).await;
    let config = StratumConfig {
        max_jobs_per_connection: 8,
        job_retention_seconds: 30.0,
        ..Default::default()
    };
    let mut session = Session::new(&config, SessionObservation::new(config.stats.clone()));
    let issued = |mut job: MiningJob<JobContext>, id: &str| {
        job.wire.job_id = id.into();
        IssuedJob {
            worker: job.context.worker.clone(),
            job,
            authorization_permit: None,
            version_mask: 0,
            retired_at: None,
        }
    };
    // Same parent (tip 1), superseded revision — the #478 case.
    session
        .jobs
        .push_back(issued(fixture.job(1, 0, "w"), "superseded"));
    // A previous-parent job (tip 2): a same-parent replacement must not touch it.
    session
        .jobs
        .push_back(issued(fixture.job(2, 0, "w"), "previous-parent"));
    // Same parent, same revision as the replacement: not superseded.
    session
        .jobs
        .push_back(issued(fixture.job(1, 5, "w"), "same-revision"));

    let replacement = fixture.job(1, 5, "w");
    session.bury_superseded_same_parent(&replacement.wire, &config, None);

    // The superseded same-parent job is buried (block-submittable), not
    // dropped, and it is block-only work from here on.
    assert_eq!(
        session.retained.get("superseded").map(|j| j.job.wire.kind),
        Some(crate::codec::JobKind::BlockOnly),
        "superseded same-parent job must be retained for block submission only"
    );
    assert!(session
        .jobs
        .iter()
        .all(|j| j.job.wire.kind == crate::codec::JobKind::Credit));
    assert!(
        !session
            .jobs
            .iter()
            .any(|j| j.job.wire.job_id == "superseded"),
        "and removed from the live set"
    );
    // A different parent stays live and is not buried.
    assert!(
        session
            .jobs
            .iter()
            .any(|j| j.job.wire.job_id == "previous-parent"),
        "a previous-parent job stays live"
    );
    assert!(session.retained.get("previous-parent").is_none());
    // Same-parent, same-revision work is current and stays live.
    assert!(
        session
            .jobs
            .iter()
            .any(|j| j.job.wire.job_id == "same-revision"),
        "same-revision work is not superseded"
    );
}

/// #478 (coordinator decision): a same-parent payout replacement buries the
/// superseded job BLOCK-ONLY — it releases the username reservation (the permit
/// is dropped, so the semaphore permit returns) yet keeps the job reachable for
/// a block submission.
#[tokio::test]
async fn burying_a_superseded_job_releases_its_reservation_but_keeps_it_block_submittable() {
    let fixture = Fixture::new(Duration::from_secs(600)).await;
    let config = StratumConfig {
        max_jobs_per_connection: 8,
        job_retention_seconds: 30.0,
        ..Default::default()
    };
    let mut session = Session::new(&config, SessionObservation::new(config.stats.clone()));
    let semaphore = Arc::new(Semaphore::new(1));
    let permit = Arc::new(semaphore.clone().try_acquire_owned().unwrap());
    assert_eq!(semaphore.available_permits(), 0, "the reservation is held");
    let mut job = fixture.job(1, 0, "w");
    job.wire.job_id = "superseded".into();
    session.jobs.push_back(IssuedJob {
        worker: job.context.worker.clone(),
        job,
        authorization_permit: Some(permit.clone()),
        version_mask: 0,
        retired_at: None,
    });

    let replacement = fixture.job(1, 1, "w");
    session.bury_superseded_same_parent(&replacement.wire, &config, None);

    // The job is retained and still block-submittable.
    assert!(session.retained.get("superseded").is_some());
    // But its reservation is released: no retained job holds the permit, so a
    // reconnect for the username is no longer refused.
    assert!(
        session.retained.permit("w").is_none(),
        "the username reservation must be released at the replacement"
    );
    // Dropping the caller's last clone returns the semaphore permit.
    drop(permit);
    assert_eq!(
        semaphore.available_permits(),
        1,
        "the semaphore permit is reclaimed once the reservation is released"
    );
}

fn issued_with(
    mut job: MiningJob<JobContext>,
    id: &str,
    permit: Option<Arc<OwnedSemaphorePermit>>,
) -> IssuedJob<JobContext> {
    job.wire.job_id = id.into();
    IssuedJob {
        worker: job.context.worker.clone(),
        job,
        authorization_permit: permit,
        version_mask: 0,
        retired_at: None,
    }
}

/// #478 (general review M1): a retained job of this username that still holds
/// the reservation is found even when block-only work of the same username,
/// which holds none, sits beside it. The lookup follows burial order, never
/// hash-map order, so every run answers the same: each iteration builds a new
/// graveyard, whose map has a new random iteration order.
#[tokio::test]
async fn permit_lookup_finds_the_reservation_past_block_only_work_on_every_run() {
    let fixture = Fixture::new(Duration::from_secs(600)).await;
    let config = StratumConfig {
        max_jobs_per_connection: 8,
        job_retention_seconds: 30.0,
        ..Default::default()
    };
    let one = StratumConfig {
        max_jobs_per_connection: 1,
        ..config.clone()
    };
    for run in 0..64 {
        let mut session = Session::new(&config, SessionObservation::new(config.stats.clone()));
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::new(semaphore.clone().try_acquire_owned().unwrap());
        // Current work evicted into the graveyard keeps its reservation.
        session.jobs.push_back(issued_with(
            fixture.job(1, 1, "w"),
            "held",
            Some(permit.clone()),
        ));
        session.make_job_room(&one, None);
        // Superseded work is retired to block-only beside it.
        for i in 0..4 {
            session.jobs.push_back(issued_with(
                fixture.job(1, 0, "w"),
                &format!("superseded-{i}"),
                Some(permit.clone()),
            ));
        }
        session.bury_superseded_same_parent(&fixture.job(1, 1, "w").wire, &config, None);
        assert!(session
            .retained
            .get("held")
            .is_some_and(|j| j.authorization_permit.is_some()));
        assert!(
            session.retained.permit("w").is_some(),
            "run {run}: the retained reservation was hidden by block-only work"
        );
    }
}

/// #478 (general review m1): the block-only cap is a true N. Superseded work
/// already in the graveyard is retired before the live work is buried, so the
/// cap counts both; credit work is capped separately and is untouched.
#[tokio::test]
async fn block_only_cap_counts_superseded_graveyard_work_and_spares_credit_work() {
    let fixture = Fixture::new(Duration::from_secs(600)).await;
    let n = 2;
    let config = StratumConfig {
        max_jobs_per_connection: n,
        job_retention_seconds: 30.0,
        ..Default::default()
    };
    let mut session = Session::new(&config, SessionObservation::new(config.stats.clone()));
    // N superseded jobs evicted into the graveyard, then N more live.
    for i in 0..n {
        session.jobs.push_back(issued_with(
            fixture.job(1, 0, "w"),
            &format!("evicted-{i}"),
            None,
        ));
    }
    for i in 0..n {
        session.make_job_room(&config, None);
        session.jobs.push_back(issued_with(
            fixture.job(1, 0, "w"),
            &format!("live-{i}"),
            None,
        ));
    }
    let replacement = fixture.job(1, 1, "w");
    session.bury_superseded_same_parent(&replacement.wire, &config, None);
    let ids: Vec<String> = (0..n)
        .map(|i| format!("evicted-{i}"))
        .chain((0..n).map(|i| format!("live-{i}")))
        .collect();
    let block_only: Vec<&String> = ids
        .iter()
        .filter(|id| {
            session
                .retained
                .get(id)
                .is_some_and(|j| j.job.wire.kind == crate::codec::JobKind::BlockOnly)
        })
        .collect();
    assert_eq!(
        block_only.len(),
        n,
        "block-only same-tip work is capped at N"
    );
    assert_eq!(
        block_only,
        ids[n..].iter().collect::<Vec<_>>(),
        "the oldest block-only work is evicted first"
    );
    // Credit work evicted after the replacement is capped on its own: 2N
    // issued through an N-job live set buries the first N.
    for i in 0..2 * n {
        session.make_job_room(&config, None);
        session.jobs.push_back(issued_with(
            fixture.job(1, 1, "w"),
            &format!("current-{i}"),
            None,
        ));
    }
    let credit = (0..n)
        .filter(|i| session.retained.get(&format!("current-{i}")).is_some())
        .count();
    assert_eq!(credit, n, "block-only work never evicts credit work");
    assert_eq!(
        ids[n..]
            .iter()
            .filter(|id| session.retained.get(id).is_some())
            .count(),
        n,
        "and credit work never evicts block-only work"
    );
}

/// #478 (general review B1): block-only work can be captured only on its own
/// parent, so a parent change drops it, while credit work on the previous
/// parent keeps its grace retention.
#[tokio::test]
async fn a_parent_change_drops_block_only_work_but_keeps_previous_parent_credit_work() {
    let mut client = Connection::new(30.0, 1).await;
    let superseded = client.deliver().await;
    client
        .backend
        .fixture
        .store
        .revision
        .store(1, Ordering::SeqCst);
    client.tip(1).await;
    let replacement = client.deliver().await;
    assert_eq!(
        client
            .session
            .retained
            .get(&superseded.wire.job_id)
            .map(|j| j.job.wire.kind),
        Some(crate::codec::JobKind::BlockOnly)
    );
    client.tip(2).await;
    client.deliver().await;
    let hint = client.backend.observed_tip_hint().await;
    client.session.prune_jobs(&client.config, hint.as_ref());
    assert!(client
        .session
        .retained
        .get(&superseded.wire.job_id)
        .is_none());
    assert!(client
        .session
        .retained
        .get(&replacement.wire.job_id)
        .is_some());
}
