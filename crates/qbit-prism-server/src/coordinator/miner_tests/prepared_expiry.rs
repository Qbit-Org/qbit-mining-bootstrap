use super::work_ledger::WorkLedger;
use super::*;

async fn job(f: &Fixture) -> MiningJob<JobContext> {
    let worker = f.job(1, 0, "original.worker").context.worker.clone();
    f.coordinator
        .build_job(&worker, "00000001", 1e-12, 0.0)
        .await
        .unwrap()
}

async fn persist(f: &Fixture, job: &MiningJob<JobContext>) -> Result<(), StratumError> {
    f.coordinator
        .persist_issued_job(
            &job.context.worker,
            job,
            0x1fffe000,
            Duration::from_secs(30),
        )
        .await
}

#[tokio::test]
async fn fresh_lease_job_resumes_after_original_prepared_deadline() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let original = fixture.coordinator.prepared.read().await.clone().unwrap();
    let original_deadline = fixture.store.jobs.lock().unwrap()[&original.storage_key].expires_at_ms;
    assert_eq!(original_deadline, 265_000);
    // Same-tip reuse at t58 does not rewrite the immutable prepared record.
    fixture
        .store
        .clock_offset_ms
        .store(58_000, Ordering::SeqCst);
    fixture.coordinator.refresh_once().await.unwrap();
    assert_eq!(
        fixture
            .coordinator
            .prepared
            .read()
            .await
            .as_ref()
            .unwrap()
            .storage_key,
        original.storage_key
    );
    fixture
        .store
        .clock_offset_ms
        .store(59_000, Ordering::SeqCst);
    fixture.node.lock().unwrap().tip = hash(2);
    fixture.node.lock().unwrap().fail = Some("getblocktemplate".into());
    assert!(fixture.coordinator.refresh_once().await.is_err());
    fixture.store.revision.store(7, Ordering::SeqCst);
    fixture
        .store
        .clock_offset_ms
        .store(170_000, Ordering::SeqCst);
    // Model the same first divergence's age; 111s is inside the 120s lease.
    fixture
        .coordinator
        .observed_tip
        .write()
        .await
        .expire_lease_for_test(Duration::from_secs(111));
    assert!(fixture
        .store
        .job(&original.storage_key)
        .await
        .unwrap()
        .is_none());
    let issued = job(&fixture).await;
    persist(&fixture, &issued).await.unwrap();
    fixture
        .store
        .clock_offset_ms
        .store(171_000, Ordering::SeqCst);
    let resumed = fixture
        .coordinator
        .resume_job(&issued.context.worker, &issued.wire.job_id)
        .await
        .unwrap()
        .expect("a fresh issued job must retain its prepared dependency through reconnect");
    assert_eq!(resumed.wire.share_target, issued.wire.share_target);
    assert_eq!(resumed.wire.share_difficulty, issued.wire.share_difficulty);
    assert_eq!(resumed.wire.version_mask, 0x1fffe000);
    assert_eq!(resumed.context.worker.username, "original.worker");
    let rows = fixture.store.jobs.lock().unwrap();
    assert_eq!(rows[&original.storage_key].revision, 0);
    assert_eq!(rows[&issued.wire.job_id].revision, 7);
    assert_eq!(rows[&issued.wire.job_id].expires_at_ms, 300_000);
    drop(rows);
    fixture.submit(&resumed, false).await.unwrap();
    assert!(
        fixture.store.records.lock().unwrap()[0].1.is_none(),
        "lease does not admit old-tip candidates"
    );
    fixture
        .store
        .clock_offset_ms
        .store(200_000, Ordering::SeqCst);
    assert!(fixture
        .coordinator
        .resume_job(&issued.context.worker, &issued.wire.job_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn physically_pruned_dependency_repairs_exact_original_after_tip_returns_and_redeparts() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let original = f.coordinator.prepared.read().await.clone().unwrap();
    let payload = f
        .store
        .jobs
        .lock()
        .unwrap()
        .remove(&original.storage_key)
        .unwrap()
        .payload;
    f.node.lock().unwrap().fail = Some("getblocktemplate".into());
    for (seconds, tip) in [(59, 2), (800, 1), (900, 2), (950, 3)] {
        f.store
            .clock_offset_ms
            .store(seconds * 1000, Ordering::SeqCst);
        f.node.lock().unwrap().tip = hash(tip);
        assert!(f.coordinator.refresh_once().await.is_err());
    }
    f.store.revision.store(9, Ordering::SeqCst);
    let issued = job(&f).await;
    persist(&f, &issued).await.unwrap();
    assert_eq!(
        f.store.jobs.lock().unwrap()[&original.storage_key].payload,
        payload
    );
    assert!(f
        .coordinator
        .resume_job(&issued.context.worker, &issued.wire.job_id)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn delayed_publication_repairs_dependency_without_renewing_issued_deadline() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    let save = Arc::new(Gate::default());
    *f.store.save_gate.lock().unwrap() = Some(save.clone());
    let refresh = tokio::spawn({
        let c = f.coordinator.clone();
        async move { c.refresh_once().await }
    });
    save.entered.notified().await;
    let ready = Arc::new(Gate::default());
    f.node.lock().unwrap().gate = Some(("getblockchaininfo".into(), ready.clone()));
    save.release.notify_one();
    ready.entered.notified().await;
    f.store.clock_offset_ms.store(200_000, Ordering::SeqCst);
    ready.release.notify_one();
    refresh.await.unwrap().unwrap();
    let issued = job(&f).await;
    assert!(f
        .store
        .job(&issued.context.prepared.storage_key)
        .await
        .unwrap()
        .is_none());
    persist(&f, &issued).await.unwrap();
    assert!(f
        .coordinator
        .resume_job(&issued.context.worker, &issued.wire.job_id)
        .await
        .unwrap()
        .is_some());
    f.store.clock_offset_ms.store(201_000, Ordering::SeqCst);
    assert!(
        persist(&f, &issued).await.is_err(),
        "a duplicate must not extend its original deadline"
    );
    assert_eq!(
        f.store.jobs.lock().unwrap()[&issued.wire.job_id].expires_at_ms,
        330_000
    );
}

#[tokio::test]
async fn resumed_bootstrap_repair_keeps_original_none_bundle_and_coinbase_suffix() {
    let mut f = Fixture::new(Duration::from_secs(10)).await;
    f.store
        .snapshot
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .shares
        .clear();
    f.coordinator.refresh_once().await.unwrap();
    let issued = job(&f).await;
    let key = issued.context.prepared.storage_key.clone();
    let original = f.store.jobs.lock().unwrap()[&key].payload.clone();
    assert!(original["bundle"].is_null());
    persist(&f, &issued).await.unwrap();
    let config = Arc::get_mut(&mut Arc::get_mut(&mut f.coordinator).unwrap().config).unwrap();
    config.coinbase_tag = "/different frontend/".into();
    let resumed = f
        .coordinator
        .resume_job(&issued.context.worker, &issued.wire.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.wire.coinb1, issued.wire.coinb1);
    assert_eq!(resumed.wire.coinb2, issued.wire.coinb2);
    f.store.jobs.lock().unwrap().remove(&key);
    persist(&f, &resumed).await.unwrap();
    assert_eq!(f.store.jobs.lock().unwrap()[&key].payload, original);
}

struct ReleaseProbe(Arc<prepared_storage::RepairProbe>);
impl Drop for ReleaseProbe {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn probe_fixture() -> (Fixture, ReleaseProbe) {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let prepared = f.coordinator.prepared.read().await.clone().unwrap();
    f.store.jobs.lock().unwrap().remove(&prepared.storage_key);
    let probe = Arc::new(prepared_storage::RepairProbe::default());
    *prepared.repair_probe.lock().unwrap() = Some(probe.clone());
    (f, ReleaseProbe(probe))
}

fn spawn_save(
    f: &Fixture,
    job: MiningJob<JobContext>,
) -> tokio::task::JoinHandle<Result<(), StratumError>> {
    let c = f.coordinator.clone();
    tokio::spawn(async move {
        c.persist_issued_job(&job.context.worker, &job, 0, Duration::from_secs(30))
            .await
    })
}

#[tokio::test]
async fn concurrent_missing_dependency_serializes_once_for_shared_prepared_work() {
    let (f, probe) = probe_fixture().await;
    let first = spawn_save(&f, job(&f).await);
    probe.0.entered.notified().await;
    let mut followers = Vec::new();
    for _ in 0..16 {
        followers.push(spawn_save(&f, job(&f).await));
    }
    probe.0.release();
    first.await.unwrap().unwrap();
    for follower in followers {
        follower.await.unwrap().unwrap();
    }
    assert_eq!(probe.0.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.store.jobs.lock().unwrap().len(), 18);
}

#[tokio::test]
async fn canceled_serializer_keeps_capacity_and_repair_guard_until_actual_completion() {
    let (f, probe) = probe_fixture().await;
    let first_job = job(&f).await;
    let first_id = first_job.wire.job_id.clone();
    let first = spawn_save(&f, first_job);
    probe.0.entered.notified().await;
    let prepared = f.coordinator.prepared.read().await.clone().unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert_eq!(f.coordinator.build_slots.available_permits(), 0);
    assert!(prepared.repair.try_lock().is_err());
    let follower = spawn_save(&f, job(&f).await);
    tokio::task::yield_now().await;
    assert_eq!(probe.0.calls.load(Ordering::SeqCst), 1);
    probe.0.release();
    follower.await.unwrap().unwrap();
    assert_eq!(
        probe.0.calls.load(Ordering::SeqCst),
        2,
        "canceled leader never committed; follower repairs once after it finishes"
    );
    assert_eq!(f.coordinator.build_slots.available_permits(), 1);
    assert!(prepared.repair.try_lock().is_ok());
    assert!(!f.store.jobs.lock().unwrap().contains_key(&first_id));
}

#[tokio::test]
async fn repair_rechecks_elapsed_deadline_and_revision_after_serialization() {
    for expire in [false, true] {
        let (f, probe) = probe_fixture().await;
        let issued = job(&f).await;
        let id = issued.wire.job_id.clone();
        let pending = spawn_save(&f, issued);
        probe.0.entered.notified().await;
        if expire {
            f.store.clock_offset_ms.store(30_000, Ordering::SeqCst);
        } else {
            f.store.revision.store(1, Ordering::SeqCst);
        }
        probe.0.release();
        assert!(pending.await.unwrap().is_err());
        assert!(!f.store.jobs.lock().unwrap().contains_key(&id));
        assert_eq!(
            f.store.jobs.lock().unwrap().len(),
            0,
            "no repaired dependency or child after failed final admission"
        );
        assert_eq!(f.coordinator.build_slots.available_permits(), 1);
    }
}

#[tokio::test]
async fn competing_repair_preserves_immutable_original_or_rejects() {
    for conflict in [false, true] {
        let (f, probe) = probe_fixture().await;
        let issued = job(&f).await;
        let key = issued.context.prepared.storage_key.clone();
        let mut original = serde_json::to_value(&issued.context.prepared.stored).unwrap();
        if conflict {
            original["coinbase_suffix"] = json!("changed");
        }
        let pending = spawn_save(&f, issued);
        probe.0.entered.notified().await;
        f.store
            .save_job(&key, &original, 0, &hash(1), 165)
            .await
            .unwrap();
        probe.0.release();
        assert_eq!(pending.await.unwrap().is_err(), conflict);
        assert_eq!(f.store.jobs.lock().unwrap()[&key].payload, original);
        assert_eq!(
            f.store.jobs.lock().unwrap().len(),
            if conflict { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn issued_duration_boundaries_fail_without_persisting_or_wrapping() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let issued = job(&f).await;
    for ttl in [Duration::ZERO, Duration::MAX] {
        assert!(f
            .coordinator
            .persist_issued_job(&issued.context.worker, &issued, 0, ttl)
            .await
            .is_err());
    }
    assert_eq!(f.store.jobs.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn normal_issuance_shares_backing_without_running_cold_serializer() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let prepared = f.coordinator.prepared.read().await.clone().unwrap();
    assert!(Arc::ptr_eq(&prepared.stored.snapshot, &prepared.snapshot));
    assert!(Arc::ptr_eq(
        prepared.stored.bundle.as_ref().unwrap(),
        prepared.bundle.as_ref().unwrap()
    ));
    let probe = Arc::new(prepared_storage::RepairProbe::default());
    probe.release();
    *prepared.repair_probe.lock().unwrap() = Some(probe.clone());
    let deadline = f.store.jobs.lock().unwrap()[&prepared.storage_key].expires_at_ms;
    for _ in 0..32 {
        persist(&f, &job(&f).await).await.unwrap();
    }
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        f.store.jobs.lock().unwrap()[&prepared.storage_key].expires_at_ms,
        deadline
    );
}
