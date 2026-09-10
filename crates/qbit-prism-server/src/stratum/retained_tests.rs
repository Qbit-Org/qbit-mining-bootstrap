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
