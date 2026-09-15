//! Real admission consumers with coherent state observations and fixed identity.
use super::*;

async fn issued(f: &Fixture) -> MiningJob<JobContext> {
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

fn change_balances(f: &Fixture) {
    f.store
        .snapshot
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .prior_balances
        .push(qbit_prism::CarryForwardBalance {
            recipient_id: "changed-recipient".into(),
            order_key: "changed-recipient".into(),
            p2mr_program_hex: hash(0xac),
            balance_sats: 1,
        });
}

#[tokio::test]
async fn unready_current_publication_is_an_error_but_retired_work_is_a_miss() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let job = issued(&f).await;
    persist(&f, &job).await.unwrap();
    // Match the live node regression: a fresh baseline does not grant the
    // existing prepared object a published replacement lease.
    *f.coordinator.observed_tip.write().await = TipState::baseline(hash(2));
    assert!(f
        .coordinator
        .resume_job(&job.context.worker, &job.wire.job_id)
        .await
        .is_err());
    assert!(f
        .coordinator
        .build_job(&job.context.worker, "00000002", 1e-12, 0.0)
        .await
        .is_err());
    f.node.lock().unwrap().tip = hash(2);
    f.coordinator.refresh_once().await.unwrap();
    assert!(f
        .coordinator
        .resume_job(&job.context.worker, &job.wire.job_id)
        .await
        .unwrap()
        .is_none());
    // A later valid replacement lease also must not make a retired parent
    // look like a backend outage to retained-job fallback.
    f.detect(3).await;
    assert!(f
        .coordinator
        .resume_job(&job.context.worker, &job.wire.job_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn unchanged_balances_keep_original_identity_and_current_transaction_revision() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let original = f.coordinator.prepared.read().await.clone().unwrap();
    f.detect(2).await;
    f.store.revision.store(7, Ordering::SeqCst);
    let job = issued(&f).await;
    persist(&f, &job).await.unwrap();
    let resumed = f
        .coordinator
        .resume_job(&job.context.worker, &job.wire.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.wire.payout_revision, 0);
    assert_eq!(resumed.context.prepared.window, original.window);
    assert_eq!(resumed.context.prepared.storage_key, original.storage_key);
    assert_eq!(resumed.context.prepared.generation, original.generation);
    {
        let rows = f.store.jobs.lock().unwrap();
        assert_eq!(rows[&original.storage_key].revision, 0);
        assert_eq!(rows[&job.wire.job_id].revision, 7);
        assert_eq!(rows[&job.wire.job_id].expires_at_ms, 130_000);
        assert_eq!(rows[&job.wire.job_id].payload["expires_at_ms"], 130_000);
    }
    f.node.lock().unwrap().calls.clear();
    f.submit(&resumed, false).await.unwrap();
    let records = f.store.records.lock().unwrap();
    assert_eq!(records[0].2, 7);
    assert_eq!(records[0].0.credit_policy, None);
    assert!(records[0].1.is_none());
    assert!(f.node.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn changed_balances_refuse_issue_resume_and_share_only_on_replacement_lease() {
    for leased in [false, true] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let job = issued(&f).await;
        persist(&f, &job).await.unwrap();
        if leased {
            f.detect(2).await;
            f.store.revision.store(7, Ordering::SeqCst);
        }
        change_balances(&f);
        // The non-lease control deliberately holds revision fixed: existing
        // admission uses that transaction fence, not the new lease digest API.
        let build = f
            .coordinator
            .build_job(&job.context.worker, "00000002", 1e-12, 0.0)
            .await;
        let resume = f
            .coordinator
            .resume_job(&job.context.worker, &job.wire.job_id)
            .await
            .unwrap();
        let submit = f.submit(&job, false).await;
        assert_eq!(build.is_ok(), !leased);
        assert_eq!(resume.is_some(), !leased);
        assert_eq!(submit.is_ok(), !leased);
        assert_eq!(f.store.records.lock().unwrap().len(), usize::from(!leased));
        if leased {
            assert!(persist(&f, &job).await.is_err());
        } else {
            assert_eq!(f.store.compact.state_calls.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn stored_same_parent_revision_copy_cannot_borrow_another_publications_lease() {
    for changed in ["key", "generation", "fingerprint", "window"] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let job = issued(&f).await;
        persist(&f, &job).await.unwrap();
        {
            let mut rows = f.store.jobs.lock().unwrap();
            let key = job.context.prepared.storage_key.clone();
            if changed == "key" {
                let original = &rows[&key];
                let copy = work_store::MemoryJob {
                    payload: original.payload.clone(),
                    expires_at_ms: original.expires_at_ms,
                    revision: original.revision,
                    parent: original.parent.clone(),
                };
                rows.insert("prepared:other".into(), copy);
                rows.get_mut(&job.wire.job_id).unwrap().payload["prepared_key"] =
                    json!("prepared:other");
            } else {
                let payload = &mut rows.get_mut(&key).unwrap().payload;
                match changed {
                    "generation" => payload["generation"] = json!(999),
                    "fingerprint" => payload["fingerprint"] = json!("other"),
                    "window" => payload["snapshot"]["anchor_ms"] = json!(99_999),
                    _ => unreachable!(),
                }
            }
        }
        f.detect(2).await;
        f.store.revision.store(7, Ordering::SeqCst);
        assert!(
            f.coordinator
                .resume_job(&job.context.worker, &job.wire.job_id)
                .await
                .unwrap()
                .is_none(),
            "{changed}"
        );
    }
}

#[tokio::test]
async fn lease_state_wait_rechecks_publication_epoch_and_bounds_for_all_consumers() {
    for operation in ["issue", "resume", "submit"] {
        for invalidation in ["publication", "readiness", "expiry"] {
            let f = Fixture::new(Duration::from_secs(10)).await;
            f.coordinator.refresh_once().await.unwrap();
            let job = issued(&f).await;
            persist(&f, &job).await.unwrap();
            f.detect(2).await;
            f.store.revision.store(7, Ordering::SeqCst);
            let gate = Arc::new(Gate::default());
            *f.store.compact.state_gate.lock().unwrap() = Some(gate.clone());
            let proof = f.proof(&job, 0);
            let c = f.coordinator.clone();
            let pending = tokio::spawn(async move {
                match operation {
                    "issue" => c
                        .build_job(&job.context.worker, "00000002", 1e-12, 0.0)
                        .await
                        .map(|_| true),
                    "resume" => c
                        .resume_job(&job.context.worker, &job.wire.job_id)
                        .await
                        .map(|job| job.is_some()),
                    "submit" => c
                        .submit(&job.context.worker, &job, proof, false.into())
                        .await
                        .map(|_| true),
                    _ => unreachable!(),
                }
            });
            gate.entered.notified().await;
            match invalidation {
                "publication" => f.coordinator.refresh_once().await.unwrap(),
                "readiness" => {
                    f.coordinator.invalidate_readiness().await;
                    // A later successful poll cannot revive this operation.
                    f.coordinator.readiness.write().await.last_poll = Some(Instant::now());
                }
                "expiry" => f
                    .coordinator
                    .observed_tip
                    .write()
                    .await
                    .expire_lease_for_test(Duration::from_secs(121)),
                _ => unreachable!(),
            }
            gate.release.notify_one();
            assert!(
                !matches!(pending.await.unwrap(), Ok(true)),
                "{operation}/{invalidation}"
            );
            assert!(f.store.records.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn resume_revalidates_after_reconstruction_and_clock_wait() {
    for changed in ["publication", "readiness", "expiry"] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let job = issued(&f).await;
        persist(&f, &job).await.unwrap();
        f.detect(2).await;
        f.store.revision.store(7, Ordering::SeqCst);
        let state = Arc::new(Gate::default());
        *f.store.compact.state_gate.lock().unwrap() = Some(state.clone());
        let c = f.coordinator.clone();
        let pending =
            tokio::spawn(async move { c.resume_job(&job.context.worker, &job.wire.job_id).await });
        state.entered.notified().await;
        let clock = Arc::new(Gate::default());
        *f.store.compact.clock_gate.lock().unwrap() = Some(clock.clone());
        state.release.notify_one();
        clock.entered.notified().await;
        match changed {
            "publication" => f.coordinator.refresh_once().await.unwrap(),
            "readiness" => {
                f.coordinator.invalidate_readiness().await;
                f.coordinator.readiness.write().await.last_poll = Some(Instant::now());
            }
            "expiry" => f.store.clock_offset_ms.store(30_000, Ordering::SeqCst),
            _ => unreachable!(),
        }
        clock.release.notify_one();
        assert!(!matches!(pending.await.unwrap(), Ok(Some(_))), "{changed}");
    }
}

#[tokio::test]
async fn coherent_lease_revision_is_not_replaced_by_a_newer_scalar_read() {
    for operation in ["persist", "submit"] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let job = issued(&f).await;
        f.detect(2).await;
        f.store.revision.store(7, Ordering::SeqCst);
        let gate = Arc::new(Gate::default());
        *f.store.compact.state_gate.lock().unwrap() = Some(gate.clone());
        let proof = f.proof(&job, 0);
        let id = job.wire.job_id.clone();
        let c = f.coordinator.clone();
        let pending = tokio::spawn(async move {
            if operation == "persist" {
                c.persist_issued_job(&job.context.worker, &job, 0, Duration::from_secs(30))
                    .await
            } else {
                c.submit(&job.context.worker, &job, proof, false.into())
                    .await
            }
        });
        gate.entered.notified().await;
        change_balances(&f);
        f.store.revision.store(8, Ordering::SeqCst);
        gate.release.notify_one();
        assert!(pending.await.unwrap().is_err(), "{operation}");
        assert!(f.store.records.lock().unwrap().is_empty());
        assert!(!f.store.jobs.lock().unwrap().contains_key(&id));
    }
}

#[tokio::test]
async fn lease_failed_observation_is_an_error_and_absolute_expiry_remains_a_miss() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let job = issued(&f).await;
    persist(&f, &job).await.unwrap();
    f.detect(2).await;
    f.store.revision.store(7, Ordering::SeqCst);
    let identity = tip_observation::PreparedIdentity::of(&job.context.prepared);
    f.store
        .compact
        .states
        .lock()
        .unwrap()
        .push_back(Err(WindowError::Database(sqlx::Error::PoolClosed)));
    let failure = f
        .coordinator
        .work_authority_revision(&identity, Some(130_000))
        .await
        .unwrap_err();
    assert!(matches!(
        failure.downcast_ref::<WindowError>(),
        Some(WindowError::Database(_))
    ));
    let gate = Arc::new(Gate::default());
    *f.store.compact.state_gate.lock().unwrap() = Some(gate.clone());
    let c = f.coordinator.clone();
    let pending =
        tokio::spawn(async move { c.work_authority_revision(&identity, Some(130_000)).await });
    gate.entered.notified().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    gate.release.notify_one();
    assert!(pending.await.unwrap().unwrap().is_none());
    tokio::time::resume();
    f.store.clock_offset_ms.store(30_000, Ordering::SeqCst);
    assert!(f
        .coordinator
        .resume_job(&job.context.worker, &job.wire.job_id)
        .await
        .unwrap()
        .is_none());
}
