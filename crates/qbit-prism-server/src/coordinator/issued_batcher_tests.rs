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

/// Occupy every in-flight slot with a one-child batch held inside the fake's
/// save gate, in order. Later entries wait for a flight in the channel.
async fn hold_flights(
    f: &Fixture,
    batcher: &Arc<IssuedBatcher>,
    prepared: &Arc<Prepared>,
) -> Vec<(tokio::task::JoinHandle<Result<IssuedJobSave>>, Arc<Gate>)> {
    let mut held = Vec::new();
    for i in 0..IN_FLIGHT {
        let gate = Arc::new(Gate::default());
        *f.store.save_gate.lock().unwrap() = Some(gate.clone());
        let request = enqueue(
            batcher.clone(),
            prepared.clone(),
            &format!("held-{i}"),
            300_000,
        );
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .expect("held batch did not reach the save gate");
        held.push((request, gate));
    }
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[1; IN_FLIGHT]
    );
    held
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
    let mut held = hold_flights(&f, &batcher, &prepared).await;
    let mut requests = Vec::new();
    for i in 0..ADMITTED - IN_FLIGHT {
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
    // No batch starts beyond the in-flight bound: the next batch's first
    // entry waits in the collector for the wave to drain, every other
    // admitted entry in the channel, and admission refuses more.
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[1; IN_FLIGHT]
    );
    assert_eq!(batcher.sender.capacity(), IN_FLIGHT + 1);
    requests[0].abort();
    // The first held batch's only caller leaves before COMMIT: its attempt
    // ends and closes the wave; the queue starts once the others finish.
    let (first, _) = held.remove(0);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    for (request, gate) in held {
        gate.release.notify_one();
        assert_eq!(request.await.unwrap().unwrap(), IssuedJobSave::Saved);
    }
    for (i, request) in requests.into_iter().enumerate() {
        if i == 0 {
            assert!(request.await.unwrap_err().is_cancelled());
        } else {
            assert_eq!(request.await.unwrap().unwrap(), IssuedJobSave::Saved);
        }
    }
    settled(&batcher).await;
    let rows = f.store.jobs.lock().unwrap();
    assert!(!rows.contains_key("held-0") && !rows.contains_key("pending-0"));
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
    assert_eq!(
        f.store.jobs.lock().unwrap()[&prepared.storage_key].expires_at_ms,
        370_000
    );
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
    let held = hold_flights(&f, &batcher, &prepared).await;
    let pending = enqueue(batcher.clone(), prepared, "pending", 300_000);
    tokio::task::yield_now().await;
    batcher.shutdown.cancel();
    for (request, _) in held {
        assert!(request.await.unwrap().is_err());
    }
    assert!(pending.await.unwrap().is_err());
    settled(&batcher).await;
    {
        let rows = f.store.jobs.lock().unwrap();
        assert!((0..IN_FLIGHT).all(|i| !rows.contains_key(&format!("held-{i}"))));
        assert!(!rows.contains_key("pending"));
    }
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
    let held = hold_flights(&f, &batcher, &prepared).await;
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
    for (request, _) in held {
        request.abort();
        let _ = request.await;
    }
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

/// #860: a session's batch for the old tip is in flight when a new tip
/// publishes newer work. The newer work's batch overlaps it, commits, and its
/// session is answered first. The new tip moved the payout revision, so:
/// - held before its checks, the old batch fails on the revision as it does
///   today, and answers only its own entry;
/// - held with COMMIT in flight (PostgreSQL's cluster-row fence orders the
///   revision move after such a commit), its immutable row lands, but its
///   session is refused: each caller revalidates its own publication after
///   its own batch, whatever order the batches end in.
#[tokio::test]
async fn overlapping_batch_for_a_superseded_group_cannot_deliver_old_work() {
    use crate::coordinator::miner_tests::hash;
    use crate::stratum::MiningBackend;
    for commit_in_flight in [false, true] {
        let (f, old_prepared) = fixture().await;
        let worker = f.job(1, 0, "original.worker").context.worker.clone();
        let old = f
            .coordinator
            .build_job(&worker, "000000a1", 1e-12, 0.0)
            .await
            .unwrap();
        let old_id = old.wire.job_id.clone();
        let gate = Arc::new(Gate::default());
        if commit_in_flight {
            *f.store.compact.batch_commit_gate.lock().unwrap() = Some(gate.clone());
        } else {
            *f.store.save_gate.lock().unwrap() = Some(gate.clone());
        }
        let saving = {
            let coordinator = f.coordinator.clone();
            let worker = worker.clone();
            tokio::spawn(async move {
                coordinator
                    .save_issued_record(&worker, &old, 0, Duration::from_secs(30))
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        let revision = f.store.revision.load(Ordering::SeqCst);
        f.node.lock().unwrap().tip = hash(2);
        f.coordinator.refresh_once().await.unwrap();
        assert_eq!(f.store.revision.load(Ordering::SeqCst), revision + 1);
        let new_prepared = f.coordinator.prepared.read().await.clone().unwrap();
        assert_ne!(new_prepared.storage_key, old_prepared.storage_key);
        let new = f
            .coordinator
            .build_job(&worker, "000000a2", 1e-12, 0.0)
            .await
            .unwrap();
        // One batch at a time could not start this until the old one ended.
        tokio::time::timeout(
            Duration::from_secs(5),
            f.coordinator
                .save_issued_record(&worker, &new, 0, Duration::from_secs(30)),
        )
        .await
        .expect("the new group's batch waited for the superseded one")
        .unwrap();
        assert!(!saving.is_finished());
        gate.release.notify_one();
        let refused = tokio::time::timeout(Duration::from_secs(5), saving)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        // The batch's own revision check, or the caller's revalidation of
        // its publication after a commit on superseded work.
        let refused = format!("{refused:#}");
        let expected = if commit_in_flight {
            "payout snapshot stale"
        } else {
            "revision changed"
        };
        assert!(refused.contains(expected), "{refused}");
        f.coordinator.issued_batcher.wait_for_idle_for_tests().await;
        let keys: Vec<_> = f
            .store
            .compact
            .issued_calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| (call.id.clone(), call.key.clone()))
            .collect();
        assert_eq!(
            keys,
            vec![
                (old_id.clone(), old_prepared.storage_key.clone()),
                (new.wire.job_id.clone(), new_prepared.storage_key.clone()),
            ]
        );
        let rows = f.store.jobs.lock().unwrap();
        assert!(rows.contains_key(&new.wire.job_id));
        assert_eq!(rows.contains_key(&old_id), commit_in_flight);
    }
}

/// #860: a batch that fails answers only its own entries. Its neighbour in
/// flight, for the same group, commits while it is held and is not failed by
/// it; every entry of both gets exactly its own batch's answer.
#[tokio::test]
async fn failed_batch_answers_only_its_own_entries_beside_a_committing_one() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    tokio::time::pause();
    let gate = Arc::new(Gate::default());
    *f.store.save_gate.lock().unwrap() = Some(gate.clone());
    let failing: Vec<_> = (0..2)
        .map(|i| {
            enqueue(
                batcher.clone(),
                prepared.clone(),
                &format!("fails-{i}"),
                300_000,
            )
        })
        .collect();
    gate.entered.notified().await;
    let committing: Vec<_> = (0..3)
        .map(|i| {
            enqueue(
                batcher.clone(),
                prepared.clone(),
                &format!("commits-{i}"),
                300_000,
            )
        })
        .collect();
    for request in committing {
        assert_eq!(request.await.unwrap().unwrap(), IssuedJobSave::Saved);
    }
    assert!(failing.iter().all(|request| !request.is_finished()));
    f.store.fail_save.store(true, Ordering::SeqCst);
    gate.release.notify_one();
    for request in failing {
        let error = request.await.unwrap().unwrap_err();
        assert!(
            format!("{error:#}").contains("controlled persistence failure"),
            "{error:#}"
        );
    }
    f.store.fail_save.store(false, Ordering::SeqCst);
    settled(&batcher).await;
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[2, 3]
    );
    let rows = f.store.jobs.lock().unwrap();
    assert!((0..2).all(|i| !rows.contains_key(&format!("fails-{i}"))));
    assert!((0..3).all(|i| rows.contains_key(&format!("commits-{i}"))));
}

/// #860: with every flight held, a queued entry's own deadline still ends its
/// wait exactly as before the bound: the caller is answered at its deadline,
/// and the entry never reaches SQL. As before, its admission slot comes back
/// when the collector next takes a flight and discards it.
#[tokio::test]
async fn entry_deadline_passing_while_every_flight_is_held_is_answered_and_never_saved() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    tokio::time::pause();
    let held = hold_flights(&f, &batcher, &prepared).await;
    let started = Instant::now();
    let late = batcher
        .save(
            "late",
            &json!({"prepared_key": prepared.storage_key, "expires_at_ms": 300_000}),
            0,
            &prepared.reservation.record.parent_hash,
            300_000,
            prepared.reservation.dependency(&prepared.storage_key),
            started + Duration::from_millis(5),
        )
        .await
        .unwrap_err();
    assert!(started.elapsed() >= Duration::from_millis(5));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(late.to_string().contains("deadline elapsed"), "{late:#}");
    for (request, gate) in held {
        gate.release.notify_one();
        assert_eq!(request.await.unwrap().unwrap(), IssuedJobSave::Saved);
    }
    settled(&batcher).await;
    assert!(f
        .store
        .compact
        .issued_calls
        .lock()
        .unwrap()
        .iter()
        .all(|call| call.id != "late"));
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[1; IN_FLIGHT]
    );
    assert!(!f.store.jobs.lock().unwrap().contains_key("late"));
}

/// #860: batches start in waves. Once one batch of a wave has finished, no new
/// batch starts until the others have, so the frontend's batches release the
/// cluster row's KEY SHARE together and a queued FOR UPDATE (an authority
/// writer, blob GC) waits for at most one wave. PostgreSQL would otherwise
/// grant each new batch's KEY SHARE past that writer for a whole fan-out.
#[tokio::test]
async fn a_finished_batch_closes_its_wave_until_the_others_finish() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    tokio::time::pause();
    let mut held = hold_flights(&f, &batcher, &prepared).await;
    let (first, gate) = held.remove(0);
    gate.release.notify_one();
    assert_eq!(first.await.unwrap().unwrap(), IssuedJobSave::Saved);
    let next = enqueue(batcher.clone(), prepared.clone(), "next-wave", 300_000);
    // The paused clock runs the collection dwell and anything it starts, and
    // stops well inside every deadline.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[1; IN_FLIGHT]
    );
    assert!(!next.is_finished());
    for (request, gate) in held {
        gate.release.notify_one();
        assert_eq!(request.await.unwrap().unwrap(), IssuedJobSave::Saved);
    }
    assert_eq!(next.await.unwrap().unwrap(), IssuedJobSave::Saved);
    settled(&batcher).await;
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[1; IN_FLIGHT + 1]
    );
}

/// #860: a burst that ended on a lone batch leaves no half-spent wave behind:
/// the next burst's first two batches overlap.
#[tokio::test]
async fn a_burst_after_a_lone_batch_starts_a_fresh_wave() {
    let (f, prepared) = fixture().await;
    let batcher = Arc::new(IssuedBatcher::new(f.store.clone()));
    tokio::time::pause();
    assert_eq!(
        enqueue(batcher.clone(), prepared.clone(), "lone", 300_000)
            .await
            .unwrap()
            .unwrap(),
        IssuedJobSave::Saved
    );
    settled(&batcher).await;
    let gate = Arc::new(Gate::default());
    *f.store.save_gate.lock().unwrap() = Some(gate.clone());
    let first = enqueue(batcher.clone(), prepared.clone(), "burst-0", 300_000);
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let second = enqueue(batcher.clone(), prepared, "burst-1", 300_000);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("the burst's second batch waited for its first")
            .unwrap()
            .unwrap(),
        IssuedJobSave::Saved
    );
    assert!(!first.is_finished());
    gate.release.notify_one();
    assert_eq!(first.await.unwrap().unwrap(), IssuedJobSave::Saved);
    settled(&batcher).await;
    assert_eq!(
        f.store.compact.batch_calls.lock().unwrap().as_slice(),
        &[1, 1, 1]
    );
}
