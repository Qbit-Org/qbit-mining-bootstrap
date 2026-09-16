use super::*;
use crate::coordinator::{
    miner_tests::{Fixture, Gate},
    Prepared,
};
use serde_json::json;
use std::sync::atomic::Ordering;

#[path = "issued_batcher_pg_tests.rs"]
mod postgres;

async fn fixture() -> (Fixture, Arc<Prepared>) {
    let f = Fixture::new(Duration::from_secs(60)).await;
    f.coordinator.refresh_once().await.unwrap();
    let prepared = f.coordinator.prepared.read().await.clone().unwrap();
    (f, prepared)
}

fn enqueue(
    batcher: Arc<IssuedBatcher>,
    prepared: Arc<Prepared>,
    id: &str,
    expiry: i64,
) -> tokio::task::JoinHandle<Result<IssuedJobSave>> {
    let id = id.to_owned();
    tokio::spawn(async move {
        batcher
            .save(
                &id,
                &json!({"prepared_key": prepared.storage_key, "expires_at_ms": expiry}),
                0,
                &prepared.reservation.record.parent_hash,
                expiry,
                prepared.reservation.dependency(&prepared.storage_key),
                Instant::now() + Duration::from_secs(30),
            )
            .await
    })
}

async fn settled(batcher: &IssuedBatcher) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while batcher.slots.available_permits() != ADMITTED {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn real_coordinator_fan_in_and_singleton_keep_durable_children() {
    use crate::stratum::MiningBackend;
    let (f, prepared) = fixture().await;
    let worker = f.job(1, 0, "original.worker").context.worker.clone();
    let mut jobs = Vec::new();
    for i in 0..8 {
        jobs.push(
            f.coordinator
                .build_job(&worker, &format!("{i:08x}"), 1e-12, 0.0)
                .await
                .unwrap(),
        );
    }
    tokio::time::pause();
    let results = futures_util::future::join_all(jobs.iter().map(|job| {
        f.coordinator
            .persist_issued_job(&worker, job, 0, Duration::from_secs(30))
    }))
    .await;
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    let batches = f.store.compact.batch_calls.lock().unwrap().clone();
    assert_eq!(batches, vec![8]);
    {
        let rows = f.store.jobs.lock().unwrap();
        for job in &jobs {
            let child = &rows[&job.wire.job_id];
            assert_eq!(child.payload["prepared_key"], prepared.storage_key);
            assert_eq!(child.payload["extranonce1"], job.wire.extranonce1);
            assert!(rows[&prepared.storage_key].expires_at_ms >= child.expires_at_ms);
        }
    }
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    assert_eq!(
        enqueue(batcher.clone(), prepared, "singleton", 300_000)
            .await
            .unwrap()
            .unwrap(),
        IssuedJobSave::Saved
    );
    assert_eq!(f.store.compact.batch_calls.lock().unwrap().last(), Some(&1));
    settled(&batcher).await;
}

#[tokio::test]
async fn queue_and_batch_bounds_cancel_enrollment_and_release_every_slot() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    let gate = Arc::new(Gate::default());
    *f.store.save_gate.lock().unwrap() = Some(gate.clone());
    let first = enqueue(
        batcher.clone(),
        prepared.clone(),
        "active-canceled",
        300_000,
    );
    gate.entered.notified().await;
    let mut requests = Vec::new();
    for i in 0..ADMITTED {
        requests.push(enqueue(
            batcher.clone(),
            prepared.clone(),
            &format!("pending-{i}"),
            300_000,
        ));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while batcher.slots.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.store.compact.batch_calls.lock().unwrap().as_slice(), &[1]);
    assert_eq!(batcher.sender.capacity(), 1); // 127 queued plus one active.
    requests[0].abort();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    for (i, request) in requests.into_iter().enumerate() {
        if i == 0 {
            assert!(request.await.unwrap_err().is_cancelled());
        } else {
            assert_eq!(request.await.unwrap().unwrap(), IssuedJobSave::Saved);
        }
    }
    settled(&batcher).await;
    let rows = f.store.jobs.lock().unwrap();
    assert!(!rows.contains_key("active-canceled") && !rows.contains_key("pending-0"));
    assert!(f
        .store
        .compact
        .batch_calls
        .lock()
        .unwrap()
        .iter()
        .all(|size| *size <= BATCH));
}

#[tokio::test]
async fn active_member_cancellation_preserves_live_peers_and_exact_expiry() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    let gate = Arc::new(Gate::default());
    *f.store.save_gate.lock().unwrap() = Some(gate.clone());
    tokio::time::pause();
    let first = enqueue(batcher.clone(), prepared.clone(), "cancel", 300_000);
    let peer = enqueue(batcher.clone(), prepared.clone(), "peer", 310_000);
    gate.entered.notified().await;
    assert_eq!(f.store.compact.batch_calls.lock().unwrap().as_slice(), &[2]);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(!peer.is_finished());
    assert!(!f.store.jobs.lock().unwrap().contains_key("peer"));
    gate.release.notify_one();
    assert_eq!(peer.await.unwrap().unwrap(), IssuedJobSave::Saved);
    {
        let rows = f.store.jobs.lock().unwrap();
        // The canceled member owns no notification, but its immutable row can
        // commit with an interested peer. Neither child's expiry slides.
        for (id, expiry) in [("cancel", 300_000), ("peer", 310_000)] {
            assert_eq!(rows[id].expires_at_ms, expiry);
            assert_eq!(
                rows[id].payload,
                json!({
                    "prepared_key": prepared.storage_key, "expires_at_ms": expiry,
                })
            );
        }
        assert_eq!(rows[&prepared.storage_key].expires_at_ms, 370_000);
    }
    assert_eq!(
        enqueue(batcher.clone(), prepared, "later", 320_000)
            .await
            .unwrap()
            .unwrap(),
        IssuedJobSave::Saved
    );
    settled(&batcher).await;
}

#[tokio::test]
async fn all_active_members_canceled_before_commit_leave_no_rows_or_retry() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    let gate = Arc::new(Gate::default());
    *f.store.save_gate.lock().unwrap() = Some(gate.clone());
    tokio::time::pause();
    let first = enqueue(batcher.clone(), prepared.clone(), "cancel-all-1", 300_000);
    let peer = enqueue(batcher.clone(), prepared.clone(), "cancel-all-2", 310_000);
    gate.entered.notified().await;
    assert_eq!(f.store.compact.batch_calls.lock().unwrap().as_slice(), &[2]);
    first.abort();
    peer.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(peer.await.unwrap_err().is_cancelled());
    settled(&batcher).await;
    {
        let rows = f.store.jobs.lock().unwrap();
        assert!(!rows.contains_key("cancel-all-1") && !rows.contains_key("cancel-all-2"));
    }
    assert_eq!(
        enqueue(batcher.clone(), prepared, "after-all-canceled", 320_000)
            .await
            .unwrap()
            .unwrap(),
        IssuedJobSave::Saved,
    );
    settled(&batcher).await;
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[2, 1]
    );
}

#[tokio::test]
async fn canceled_member_original_deadline_still_bounds_live_peers() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    let gate = Arc::new(Gate::default());
    *f.store.save_gate.lock().unwrap() = Some(gate.clone());
    tokio::time::pause();
    let started = Instant::now();
    let first = {
        let batcher = batcher.clone();
        let prepared = prepared.clone();
        tokio::spawn(async move {
            batcher
                .save(
                    "earliest-canceled",
                    &json!({
                        "prepared_key": prepared.storage_key, "expires_at_ms": 300_000,
                    }),
                    0,
                    &prepared.reservation.record.parent_hash,
                    300_000,
                    prepared.reservation.dependency(&prepared.storage_key),
                    started + Duration::from_secs(5),
                )
                .await
        })
    };
    let peer = enqueue(batcher.clone(), prepared, "later-deadline", 310_000);
    gate.entered.notified().await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    tokio::time::advance(Duration::from_secs(5)).await;
    let error = peer.await.unwrap().unwrap_err().to_string();
    assert!(
        error.contains("deadline elapsed") && error.contains("before commit"),
        "{error}"
    );
    assert!(started.elapsed() < Duration::from_secs(30));
    settled(&batcher).await;
    let rows = f.store.jobs.lock().unwrap();
    assert!(!rows.contains_key("earliest-canceled") && !rows.contains_key("later-deadline"));
    assert_eq!(f.store.compact.batch_calls.lock().unwrap().as_slice(), &[2]);
}

#[tokio::test]
async fn canceled_member_during_commit_does_not_hide_live_peers_acknowledgement() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    let gate = Arc::new(Gate::default());
    *f.store.compact.batch_commit_gate.lock().unwrap() = Some(gate.clone());
    tokio::time::pause();
    let first = enqueue(
        batcher.clone(),
        prepared.clone(),
        "committing-canceled",
        300_000,
    );
    let peer = enqueue(batcher.clone(), prepared, "committing-peer", 310_000);
    gate.entered.notified().await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(!peer.is_finished());
    gate.release.notify_one();
    assert_eq!(peer.await.unwrap().unwrap(), IssuedJobSave::Saved);
    settled(&batcher).await;
    let rows = f.store.jobs.lock().unwrap();
    assert_eq!(rows["committing-canceled"].expires_at_ms, 300_000);
    assert_eq!(rows["committing-peer"].expires_at_ms, 310_000);
    assert_eq!(f.store.compact.batch_calls.lock().unwrap().as_slice(), &[2]);
}

#[tokio::test]
async fn all_members_canceled_during_commit_leave_exact_identity_for_reconciliation() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    let gate = Arc::new(Gate::default());
    *f.store.compact.batch_commit_gate.lock().unwrap() = Some(gate.clone());
    tokio::time::pause();
    let first = enqueue(
        batcher.clone(),
        prepared.clone(),
        "all-gone-commit-1",
        300_000,
    );
    let peer = enqueue(
        batcher.clone(),
        prepared.clone(),
        "all-gone-commit-2",
        310_000,
    );
    gate.entered.notified().await;
    first.abort();
    peer.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(peer.await.unwrap_err().is_cancelled());
    settled(&batcher).await;
    assert_eq!(f.store.compact.batch_calls.lock().unwrap().as_slice(), &[2]);
    let count = f.store.jobs.lock().unwrap().len();
    // This fake COMMIT became durable but its acknowledgement was never
    // received. All-gone cancellation must not remove or remint these rows.
    for (id, expiry) in [
        ("all-gone-commit-1", 300_000),
        ("all-gone-commit-2", 310_000),
    ] {
        assert_eq!(f.store.jobs.lock().unwrap()[id].expires_at_ms, expiry);
        assert_eq!(
            enqueue(batcher.clone(), prepared.clone(), id, expiry)
                .await
                .unwrap()
                .unwrap(),
            IssuedJobSave::Saved
        );
    }
    settled(&batcher).await;
    assert_eq!(f.store.jobs.lock().unwrap().len(), count);
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[2, 1, 1]
    );
}

#[tokio::test]
async fn lost_commit_acknowledgement_is_uncertain_and_exact_retry_is_idempotent() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    let gate = Arc::new(Gate::default());
    *f.store.compact.batch_commit_gate.lock().unwrap() = Some(gate.clone());
    tokio::time::pause();
    let first = enqueue(batcher.clone(), prepared.clone(), "committed", 300_000);
    let peer = enqueue(batcher.clone(), prepared.clone(), "committed-peer", 300_000);
    gate.entered.notified().await;
    // Lose the acknowledgement for the whole attempt, independently of any
    // member canceling. Live peers must not mistake shutdown for rollback.
    batcher.shutdown.cancel();
    for result in [first.await.unwrap(), peer.await.unwrap()] {
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("commit outcome uncertain"));
    }
    settled(&batcher).await;
    let count = f.store.jobs.lock().unwrap().len();
    assert!(f.store.jobs.lock().unwrap().contains_key("committed-peer"));
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    assert_eq!(
        enqueue(batcher.clone(), prepared, "committed", 300_000)
            .await
            .unwrap()
            .unwrap(),
        IssuedJobSave::Saved
    );
    assert_eq!(f.store.jobs.lock().unwrap().len(), count);
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[2, 1]
    );
    settled(&batcher).await;
}

#[tokio::test]
async fn shutdown_closes_active_and_queued_waiters_without_a_self_owned_worker() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    let gate = Arc::new(Gate::default());
    *f.store.save_gate.lock().unwrap() = Some(gate.clone());
    let first = enqueue(batcher.clone(), prepared.clone(), "active", 300_000);
    gate.entered.notified().await;
    let pending = enqueue(batcher.clone(), prepared, "pending", 300_000);
    tokio::task::yield_now().await;
    batcher.shutdown.cancel();
    assert!(first.await.unwrap().is_err());
    assert!(pending.await.unwrap().is_err());
    settled(&batcher).await;
    assert!(!f.store.jobs.lock().unwrap().contains_key("active"));
    let weak = Arc::downgrade(&f.store);
    drop(batcher);
    drop(f);
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn original_deadline_includes_queue_and_revocation_after_commit_suppresses_delivery() {
    use crate::stratum::MiningBackend;
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    let gate = Arc::new(Gate::default());
    *f.store.save_gate.lock().unwrap() = Some(gate.clone());
    let first = enqueue(batcher.clone(), prepared.clone(), "blocked", 300_000);
    gate.entered.notified().await;
    let expired = batcher
        .save(
            "expired",
            &json!({"prepared_key": prepared.storage_key, "expires_at_ms": 300_000}),
            0,
            &prepared.reservation.record.parent_hash,
            300_000,
            prepared.reservation.dependency(&prepared.storage_key),
            Instant::now() + Duration::from_millis(5),
        )
        .await
        .unwrap_err();
    assert!(expired.to_string().contains("deadline"));
    first.abort();
    let _ = first.await;
    settled(&batcher).await;
    assert!(!f.store.jobs.lock().unwrap().contains_key("expired"));

    let worker = f.job(1, 0, "original.worker").context.worker.clone();
    let job = f
        .coordinator
        .build_job(&worker, "000000ff", 1e-12, 0.0)
        .await
        .unwrap();
    let id = job.wire.job_id.clone();
    let gate = Arc::new(Gate::default());
    *f.store.compact.batch_commit_gate.lock().unwrap() = Some(gate.clone());
    let coordinator = f.coordinator.clone();
    let saving = tokio::spawn(async move {
        coordinator
            .persist_issued_job(&worker, &job, 0, Duration::from_secs(30))
            .await
    });
    gate.entered.notified().await;
    assert!(f.store.jobs.lock().unwrap().contains_key(&id));
    f.store.revision.store(1, Ordering::SeqCst);
    gate.release.notify_one();
    assert!(saving.await.unwrap().is_err());
    assert!(f.store.jobs.lock().unwrap().contains_key(&id));
}

#[tokio::test]
async fn full_immutable_identity_and_current_revision_partition_groups() {
    let (f, prepared) = fixture().await;
    let batcher = IssuedBatcher::new(f.store.clone());
    tokio::time::pause();
    let base = prepared.reservation.dependency(&prepared.storage_key);
    let mut dependencies = vec![base; 8];
    dependencies[1].key = "other-key";
    dependencies[2].original_revision += 1;
    dependencies[3].parent = "other-parent";
    dependencies[4].original_expires_at_ms += 1;
    dependencies[5].template_sha256 = "other-template";
    dependencies[6].prior_balances_digest = [255; 32];
    let inputs: Vec<_> = dependencies
        .iter()
        .enumerate()
        .map(|(i, d)| {
            (
                format!("group-{i}"),
                json!({"prepared_key":d.key,"expires_at_ms":300_000}),
            )
        })
        .collect();
    let results = futures_util::future::join_all(dependencies.iter().enumerate().map(|(i, d)| {
        batcher.save(
            &inputs[i].0,
            &inputs[i].1,
            i64::from(i == 7),
            d.parent,
            300_000,
            *d,
            Instant::now() + Duration::from_secs(30),
        )
    }))
    .await;
    assert_eq!(results[0].as_ref().unwrap(), &IssuedJobSave::Saved);
    assert_eq!(
        results[1].as_ref().unwrap(),
        &IssuedJobSave::PreparedMissing
    );
    assert!(results[2..].iter().all(Result::is_err));
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[1; 8]
    );
    settled(&batcher).await;
}

#[tokio::test]
async fn failed_group_does_not_poison_later_compatible_children() {
    let (f, prepared) = fixture().await;
    let batcher = IssuedBatcher::new(f.store.clone());
    tokio::time::pause();
    let inputs: Vec<_> = (0..4)
        .map(|i| {
            (
                format!("isolation-{i}"),
                json!({"prepared_key":prepared.storage_key,"expires_at_ms":300_000}),
            )
        })
        .collect();
    let results =
        futures_util::future::join_all(inputs.iter().enumerate().map(|(i, (id, payload))| {
            batcher.save(
                id,
                payload,
                i64::from(i < 2),
                &prepared.reservation.record.parent_hash,
                300_000,
                prepared.reservation.dependency(&prepared.storage_key),
                Instant::now() + Duration::from_secs(30),
            )
        }))
        .await;
    assert!(results[..2].iter().all(Result::is_err));
    assert!(results[2..]
        .iter()
        .all(|result| matches!(result, Ok(IssuedJobSave::Saved))));
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[2, 2]
    );
    let rows = f.store.jobs.lock().unwrap();
    for (i, (id, _)) in inputs.iter().enumerate() {
        assert_eq!(rows.contains_key(id), i >= 2);
    }
}
