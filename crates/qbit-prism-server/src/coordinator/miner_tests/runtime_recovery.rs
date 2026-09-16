//! Real runtime handoffs over fake I/O: no legacy hydration/capture adapters.
use super::*;
use prepared_storage::compact::CompactDropProbe;
use tokio::time::timeout;

struct CleanupHold(Arc<prepared_storage::RepairProbe>);
impl Drop for CleanupHold {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn hold_cleanup(
    f: &Fixture,
) -> (
    CleanupHold,
    tokio::sync::oneshot::Receiver<std::thread::ThreadId>,
) {
    let (dropped, receive) = tokio::sync::oneshot::channel();
    let hold = CleanupHold(Arc::new(prepared_storage::RepairProbe::default()));
    *f.store.compact.drop_probe.lock().unwrap() = Some(CompactDropProbe {
        dropped: Some(dropped),
        release: hold.0.clone(),
        runtime_thread: std::thread::current().id(),
    });
    (hold, receive)
}

async fn issued(f: &Fixture, ttl: Duration) -> MiningJob<JobContext> {
    let worker = f.job(1, 0, "original.worker").context.worker.clone();
    let job = f
        .coordinator
        .build_job(&worker, "00000001", 1e-12, 0.0)
        .await
        .unwrap();
    f.coordinator
        .persist_issued_job(&worker, &job, 0, ttl)
        .await
        .unwrap();
    job
}

async fn capacity_returns(f: &Fixture) {
    let permit = timeout(
        Duration::from_secs(5),
        f.coordinator.build_slots.clone().acquire_owned(),
    )
    .await
    .unwrap()
    .unwrap();
    drop(permit);
    assert_eq!(f.coordinator.build_slots.available_permits(), 1);
    assert_eq!(f.coordinator.window_reads.available_permits(), 1);
}

#[tokio::test]
async fn runtime_refresh_retains_admission_through_actual_cleanup_and_cancellation() {
    for cancel in [false, true] {
        let f = Fixture::build(
            Duration::from_secs(10),
            |config| config.snapshot_interval = Duration::ZERO,
            None,
        )
        .await;
        f.coordinator.refresh_once().await.unwrap();
        let original = f.coordinator.prepared.read().await.clone().unwrap();
        let writes = f.store.compact.save_calls.lock().unwrap().len();
        let (hold, dropped) = hold_cleanup(&f);
        let pending = tokio::spawn({
            let coordinator = f.coordinator.clone();
            async move { coordinator.refresh_once().await }
        });
        let cleanup_thread = timeout(Duration::from_secs(5), dropped)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(cleanup_thread, std::thread::current().id());
        assert_eq!(f.coordinator.build_slots.available_permits(), 0);
        assert!(Arc::ptr_eq(
            f.coordinator.prepared.read().await.as_ref().unwrap(),
            &original
        ));
        assert_eq!(f.store.compact.save_calls.lock().unwrap().len(), writes);
        if cancel {
            pending.abort();
        }
        assert_eq!(
            f.coordinator.build_slots.available_permits(),
            0,
            "cancelled async waiter must not release the blocking owner's slot"
        );
        drop(hold);
        if cancel {
            assert!(pending.await.unwrap_err().is_cancelled());
            assert!(Arc::ptr_eq(
                f.coordinator.prepared.read().await.as_ref().unwrap(),
                &original
            ));
            assert_eq!(f.store.compact.save_calls.lock().unwrap().len(), writes);
        } else {
            pending.await.unwrap().unwrap();
            assert_ne!(
                f.coordinator
                    .prepared
                    .read()
                    .await
                    .as_ref()
                    .unwrap()
                    .storage_key,
                original.storage_key
            );
            assert_eq!(f.store.compact.save_calls.lock().unwrap().len(), writes + 1);
        }
        capacity_returns(&f).await;
    }
}

#[tokio::test]
async fn shared_runtime_reconstruction_survives_one_waiter_and_cleans_up_the_last() {
    for cancel_all in [false, true] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let job = issued(&f, Duration::from_secs(30)).await;
        let (hold, dropped) = hold_cleanup(&f);
        let first = tokio::spawn({
            let c = f.coordinator.clone();
            let job = job.clone();
            async move { c.resume_job(&job.context.worker, &job.wire.job_id).await }
        });
        let cleanup_thread = timeout(Duration::from_secs(5), dropped)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(cleanup_thread, std::thread::current().id());
        // Poll the second public resume into the same existing flight before
        // cancelling the first, with the actual blocking cleanup still held.
        let mut second = Box::pin(
            f.coordinator
                .resume_job(&job.context.worker, &job.wire.job_id),
        );
        assert!(timeout(Duration::from_millis(10), &mut second)
            .await
            .is_err());
        assert_eq!(f.store.compact.read_keys.lock().unwrap().len(), 1);
        assert_eq!(f.store.compact.window_calls.lock().unwrap().len(), 1);
        first.abort();
        assert!(matches!(first.await, Err(error) if error.is_cancelled()));
        assert_eq!(f.coordinator.build_slots.available_permits(), 0);
        if cancel_all {
            drop(second);
            assert_eq!(f.coordinator.build_slots.available_permits(), 0);
            drop(hold);
        } else {
            drop(hold);
            let resumed = timeout(Duration::from_secs(5), second)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(resumed.wire.coinb1, job.wire.coinb1);
            assert_eq!(resumed.wire.coinb2, job.wire.coinb2);
            assert_eq!(
                resumed.context.prepared.reservation.record,
                job.context.prepared.reservation.record
            );
        }
        capacity_returns(&f).await;
        // Dead flights cannot retain successful outputs or cancelled futures.
        let resumed = timeout(
            Duration::from_secs(5),
            f.coordinator
                .resume_job(&job.context.worker, &job.wire.job_id),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(resumed.wire.coinb1, job.wire.coinb1);
        assert_eq!(f.store.compact.read_keys.lock().unwrap().len(), 2);
        assert_eq!(f.store.compact.window_calls.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn metadata_failure_is_shared_only_until_the_last_flight_waiter_leaves() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let job = issued(&f, Duration::from_secs(30)).await;
    f.store
        .compact
        .reads
        .lock()
        .unwrap()
        .push_back(Err(anyhow::anyhow!("transient metadata lookup failure")));
    let first = f
        .coordinator
        .resume_flights
        .join(
            &f.coordinator,
            &job.context.prepared.storage_key,
            job.wire.extranonce2_size,
        )
        .await;
    let second = f
        .coordinator
        .resume_flights
        .join(
            &f.coordinator,
            &job.context.prepared.storage_key,
            job.wire.extranonce2_size,
        )
        .await;
    assert!(Arc::ptr_eq(&first, &second));
    for waiter in [&first, &second] {
        let result = waiter.metadata.clone().await;
        assert!(
            matches!(result, Err(error) if error.to_string().contains("transient metadata lookup failure"))
        );
    }
    assert_eq!(f.store.compact.read_keys.lock().unwrap().len(), 1);
    assert!(f.store.compact.window_calls.lock().unwrap().is_empty());
    drop(first);
    let overlapping = f
        .coordinator
        .resume_flights
        .join(
            &f.coordinator,
            &job.context.prepared.storage_key,
            job.wire.extranonce2_size,
        )
        .await;
    assert!(Arc::ptr_eq(&second, &overlapping));
    assert!(overlapping.metadata.clone().await.is_err());
    assert_eq!(f.store.compact.read_keys.lock().unwrap().len(), 1);
    drop(second);
    drop(overlapping);
    let resumed = timeout(
        Duration::from_secs(5),
        f.coordinator
            .resume_job(&job.context.worker, &job.wire.job_id),
    )
    .await
    .unwrap()
    .unwrap()
    .expect("a later request retries the metadata read");
    assert_eq!(f.store.compact.read_keys.lock().unwrap().len(), 2);
    assert_eq!(resumed.wire.coinb1, job.wire.coinb1);
    assert_eq!(resumed.wire.coinb2, job.wire.coinb2);
    assert_eq!(
        resumed.context.prepared.reservation.record,
        job.context.prepared.reservation.record
    );
}

#[tokio::test]
async fn original_resume_expiry_includes_coalescer_build_and_reader_admission() {
    for phase in ["coalescer", "build", "reader"] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let job = issued(&f, Duration::from_secs(1)).await;
        let original = f.store.jobs.lock().unwrap()[&job.wire.job_id]
            .payload
            .clone();
        let flight = if phase == "coalescer" {
            Some(
                f.coordinator
                    .resume_flights
                    .join(
                        &f.coordinator,
                        "prepared:occupied:11111111111111111111111111111111",
                        8,
                    )
                    .await,
            )
        } else {
            None
        };
        let permit = match phase {
            "build" => Some(
                f.coordinator
                    .build_slots
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap(),
            ),
            "reader" => Some(
                f.coordinator
                    .window_reads
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap(),
            ),
            _ => None,
        };
        let mut resume = Box::pin(
            f.coordinator
                .resume_job(&job.context.worker, &job.wire.job_id),
        );
        assert!(
            timeout(Duration::from_millis(20), &mut resume)
                .await
                .is_err(),
            "{phase} did not wait"
        );
        assert_eq!(
            f.store.compact.read_keys.lock().unwrap().len(),
            usize::from(phase != "coalescer")
        );
        assert!(f.store.compact.window_calls.lock().unwrap().is_empty());
        assert!(
            timeout(Duration::from_secs(2), resume)
                .await
                .unwrap()
                .unwrap()
                .is_none(),
            "{phase} replaced the original issued deadline"
        );
        assert_eq!(
            f.store.jobs.lock().unwrap()[&job.wire.job_id].payload,
            original
        );
        drop(permit);
        drop(flight);
        capacity_returns(&f).await;
        let next = issued(&f, Duration::from_secs(30)).await;
        assert!(
            timeout(
                Duration::from_secs(5),
                f.coordinator
                    .resume_job(&next.context.worker, &next.wire.job_id)
            )
            .await
            .unwrap()
            .unwrap()
            .is_some(),
            "{phase} leaked admission after expiry"
        );
    }
}

#[tokio::test]
async fn retained_ctv_context_contains_presence_without_fanout_outputs() {
    let f = Fixture::build(
        Duration::from_secs(10),
        |config| {
            config.ctv_enabled = true;
            config.ctv_direct_floor = u64::MAX;
            config.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
        },
        None,
    )
    .await;
    f.coordinator.refresh_once().await.unwrap();
    let job = issued(&f, Duration::from_secs(30)).await;
    let original = f.original(&job.context.prepared);
    let native = original.bundle.as_ref().unwrap();
    assert!(native.ctv_fanout_manifest_set.is_some());
    assert!(job.context.bundle.ctv_fanout_manifest_set.is_some());
    let retained = serde_json::to_value(&job.context.bundle).unwrap();
    assert_eq!(retained["ctv_fanout_manifest_set"], json!({}));
    assert!(serde_json::to_vec(&retained).unwrap().len() < 1024);
    let resumed = f
        .coordinator
        .resume_job(&job.context.worker, &job.wire.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.wire.coinb1, job.wire.coinb1);
    assert_eq!(resumed.wire.coinb2, job.wire.coinb2);
    assert_eq!(
        serde_json::to_value(&resumed.context.bundle).unwrap(),
        retained
    );
    let hashes = job
        .context
        .prepared
        .reservation
        .record
        .audit_hashes
        .as_ref()
        .unwrap();
    assert_eq!(
        prepared_storage::compact::canonical_json_sha256(native).unwrap(),
        hashes.audit_bundle_sha256
    );
}
