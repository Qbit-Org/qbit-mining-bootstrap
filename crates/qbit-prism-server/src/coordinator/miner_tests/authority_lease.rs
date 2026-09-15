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

async fn prepare_replacement_lease(f: &Fixture) -> i64 {
    let original = f.coordinator.prepared.read().await.clone().unwrap();
    f.node.lock().unwrap().tip = hash(2);
    f.node.lock().unwrap().fail = Some("getblocktemplate".into());
    assert!(f.coordinator.refresh_once().await.is_err());
    f.node.lock().unwrap().fail = None;
    // Another frontend may observe this chain change before our replacement
    // finishes. The later local publication must not advance this fence again.
    let revision = f
        .coordinator
        .work_ledger
        .observe_chain_view(&hash(2), 100, "01")
        .await
        .unwrap();
    assert!(revision > original.snapshot.payout_revision);
    assert!(Arc::ptr_eq(
        f.coordinator.prepared.read().await.as_ref().unwrap(),
        &original
    ));
    revision
}

#[tokio::test]
async fn late_lease_append_and_issued_save_recheck_publication_and_epoch() {
    for operation in ["append", "persist"] {
        for changed in ["unchanged", "publication", "epoch"] {
            let f = Fixture::new(Duration::from_secs(10)).await;
            f.coordinator.refresh_once().await.unwrap();
            let job = issued(&f).await;
            let id = job.wire.job_id.clone();
            let revision = prepare_replacement_lease(&f).await;
            let epoch = f.coordinator.readiness.read().await.generation;
            let gate = Arc::new(Gate::default());
            if operation == "append" {
                *f.store.append_gate.lock().unwrap() = Some(gate.clone());
            } else {
                *f.store.save_gate.lock().unwrap() = Some(gate.clone());
            }
            let proof = f.proof(&job, 0);
            let coordinator = f.coordinator.clone();
            let pending = tokio::spawn(async move {
                if operation == "append" {
                    coordinator
                        .submit(&job.context.worker, &job, proof, false.into())
                        .await
                } else {
                    coordinator
                        .persist_issued_job(&job.context.worker, &job, 0, Duration::from_secs(30))
                        .await
                }
            });
            tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
                .await
                .unwrap();
            match changed {
                "publication" => {
                    tokio::time::timeout(Duration::from_secs(5), f.coordinator.refresh_once())
                        .await
                        .unwrap()
                        .unwrap();
                }
                "epoch" => {
                    f.coordinator.invalidate_readiness().await;
                    f.detect(2).await;
                    f.coordinator.readiness.write().await.last_poll = Some(Instant::now());
                }
                _ => {}
            }
            assert_eq!(f.store.revision.load(Ordering::SeqCst), revision);
            assert_eq!(
                f.coordinator.readiness.read().await.generation != epoch,
                changed == "epoch"
            );
            gate.release.notify_one();
            let result = tokio::time::timeout(Duration::from_secs(5), pending)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                result.is_ok(),
                changed == "unchanged",
                "{operation}/{changed}"
            );
            let records = f.store.records.lock().unwrap();
            assert_eq!(
                records.len(),
                usize::from(operation == "append" && changed == "unchanged"),
                "{operation}/{changed}"
            );
            if let Some(record) = records.first() {
                assert_eq!(record.2, revision);
                assert_eq!(record.0.credit_policy, None);
            }
            // A refused delivery may leave a committed issued row. The API
            // must not report it deliverable after its authority was revoked.
            if operation == "persist" && changed == "unchanged" {
                assert_eq!(f.store.jobs.lock().unwrap()[&id].revision, revision);
            }
        }
    }
}

#[tokio::test]
async fn pending_lease_cannot_borrow_renewed_tip_bound_but_new_admission_can() {
    for operation in ["append", "persist"] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let job = issued(&f).await;
        let original = job.context.prepared.clone();
        let revision = f.store.revision.load(Ordering::SeqCst);
        let epoch = f.coordinator.readiness.read().await.generation;
        f.detect(2).await;
        let (publication, first_departure) = {
            let mut tip = f.coordinator.observed_tip.write().await;
            tip.expire_lease_for_test(Duration::from_secs(119));
            (tip.publication_stamp(), tip.divergence_for_test().unwrap())
        };
        let gate = Arc::new(Gate::default());
        if operation == "append" {
            *f.store.append_gate.lock().unwrap() = Some(gate.clone());
        } else {
            *f.store.save_gate.lock().unwrap() = Some(gate.clone());
        }
        let proof = f.proof(&job, 0);
        let pending_job = job.clone();
        let coordinator = f.coordinator.clone();
        let pending = tokio::spawn(async move {
            if operation == "append" {
                coordinator
                    .submit(
                        &pending_job.context.worker,
                        &pending_job,
                        proof,
                        false.into(),
                    )
                    .await
            } else {
                coordinator
                    .persist_issued_job(
                        &pending_job.context.worker,
                        &pending_job,
                        0x1fffe000,
                        Duration::from_secs(30),
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        // The operation was admitted with about one second remaining. Let
        // that original bound expire before the node returns and departs again.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(first_departure.elapsed() >= Duration::from_secs(120));
        tokio::time::timeout(Duration::from_secs(5), async {
            f.detect(1).await;
            f.detect(2).await;
        })
        .await
        .unwrap();
        f.coordinator.readiness.write().await.last_poll = Some(Instant::now());
        {
            let tip = f.coordinator.observed_tip.read().await;
            assert_eq!(tip.publication_stamp(), publication);
            assert!(tip.divergence_for_test().unwrap() > first_departure);
            assert!(tip
                .authority(
                    f.coordinator.config.submit_tip_max_age,
                    f.coordinator.config.template_refresh_failure_exit,
                )
                .is_some_and(|authority| authority.share_lease));
        }
        assert_eq!(f.store.revision.load(Ordering::SeqCst), revision);
        assert_eq!(f.coordinator.readiness.read().await.generation, epoch);
        assert!(Arc::ptr_eq(
            f.coordinator.prepared.read().await.as_ref().unwrap(),
            &original
        ));
        gate.release.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap()
            .expect_err("a pending operation cannot borrow the renewed replacement bound");
        assert_ne!(
            error.response(json!(41))["error"][2]["reason_id"],
            "ledger-outcome-unknown",
            "{operation}: the expired pending operation is refused before delivery or COMMIT"
        );
        assert!(f.store.records.lock().unwrap().is_empty());

        // The same published job can start a new operation under the new
        // bounded lease. An issued-row retry keeps identical bytes and expiry.
        if operation == "append" {
            tokio::time::timeout(Duration::from_secs(5), f.submit(&job, false))
                .await
                .unwrap()
                .unwrap();
            let records = f.store.records.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].2, revision);
            assert_eq!(records[0].0.credit_policy, None);
        } else {
            tokio::time::timeout(Duration::from_secs(5), persist(&f, &job))
                .await
                .unwrap()
                .unwrap();
            let rows = f.store.jobs.lock().unwrap();
            assert_eq!(rows[&job.wire.job_id].revision, revision);
            assert_eq!(rows[&job.wire.job_id].expires_at_ms, 130_000);
        }
    }
}

#[tokio::test]
async fn issued_save_wait_preserves_original_deadline_and_lease_before_delivery() {
    for changed in ["unchanged", "absolute-expiry", "lease-expiry"] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let job = issued(&f).await;
        let id = job.wire.job_id.clone();
        let revision = prepare_replacement_lease(&f).await;
        let ttl = Duration::from_secs(if changed == "absolute-expiry" { 1 } else { 30 });
        let expires_at_ms = 100_000 + i64::try_from(ttl.as_millis()).unwrap();
        let gate = Arc::new(Gate::default());
        *f.store.save_gate.lock().unwrap() = Some(gate.clone());
        let coordinator = f.coordinator.clone();
        let pending = tokio::spawn(async move {
            coordinator
                .persist_issued_job(&job.context.worker, &job, 0, ttl)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        if changed == "absolute-expiry" {
            // The save's database clock stays frozen. Only the original
            // operation deadline accounts for this one-second storage wait.
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::time::resume();
        } else if changed == "lease-expiry" {
            f.coordinator
                .observed_tip
                .write()
                .await
                .expire_lease_for_test(Duration::from_secs(121));
        }
        assert_eq!(f.store.database_now(), 100_000);
        gate.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.is_ok(), changed == "unchanged", "{changed}");
        let rows = f.store.jobs.lock().unwrap();
        let row = rows.get(&id).expect("the gated save completed its write");
        assert_eq!(row.revision, revision);
        assert_eq!(row.expires_at_ms, expires_at_ms);
        assert_eq!(row.payload["expires_at_ms"], expires_at_ms);
        assert!(f.store.records.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn dependency_repair_wait_keeps_first_attempt_authority_payload_and_expiry() {
    for changed in ["unchanged", "epoch", "publication"] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let job = issued(&f).await;
        let id = job.wire.job_id.clone();
        let prepared = job.context.prepared.clone();
        let key = prepared.storage_key.clone();
        let revision = prepare_replacement_lease(&f).await;
        let original = f.store.jobs.lock().unwrap().remove(&key).unwrap();
        let expected_payload = serde_json::to_value(StoredJob {
            prepared_key: key.clone(),
            worker: job.context.worker.clone(),
            extranonce1: job.wire.extranonce1.clone(),
            extranonce2_size: job.wire.extranonce2_size,
            share_target_hex: job.wire.share_target.to_str_radix(16),
            share_difficulty: job.wire.share_difficulty,
            version_mask: 0x1fffe000,
            expires_at_ms: 130_000,
        })
        .unwrap();
        let repair = prepared.repair.clone().lock_owned().await;
        let save = Arc::new(Gate::default());
        *f.store.save_gate.lock().unwrap() = Some(save.clone());
        let mut pending = Box::pin(f.coordinator.persist_issued_job(
            &job.context.worker,
            &job,
            0x1fffe000,
            Duration::from_secs(30),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = &mut pending => panic!("persistence returned before the first save gate"),
                () = save.entered.notified() => {}
            }
        })
        .await
        .unwrap();
        assert!(f.store.compact.state_calls.load(Ordering::SeqCst) > 0);
        save.release.notify_one();
        // Once released, the fake synchronously returns PreparedMissing. The
        // next Pending point is the already-held repair mutex, without a sleep
        // or a new fake callback deciding where persistence should stop.
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        if changed == "epoch" {
            f.coordinator.invalidate_readiness().await;
            f.detect(2).await;
            f.coordinator.readiness.write().await.last_poll = Some(Instant::now());
        } else if changed == "publication" {
            f.coordinator.refresh_once().await.unwrap();
        }
        // Retrying after the mutex wait must not recalculate the original
        // absolute deadline from this later database time.
        f.store.clock_offset_ms.store(5_000, Ordering::SeqCst);
        assert_eq!(f.store.revision.load(Ordering::SeqCst), revision);
        drop(repair);
        // No other task can acquire this mutex here: a refused try_lock proves
        // the pinned persistence future had already queued before revocation.
        assert!(prepared.repair.try_lock().is_err());
        let result = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap();
        assert_eq!(result.is_ok(), changed == "unchanged", "{changed}");
        let rows = f.store.jobs.lock().unwrap();
        if changed == "unchanged" {
            assert_eq!(rows[&key].payload, original.payload);
            assert_eq!(rows[&key].revision, original.revision);
            assert_eq!(rows[&id].payload, expected_payload);
            assert_eq!(rows[&id].revision, revision);
            assert_eq!(rows[&id].expires_at_ms, 130_000);
        } else {
            assert!(!rows.contains_key(&key), "{changed}: repair was refused");
            assert!(
                !rows.contains_key(&id),
                "{changed}: issued work was refused"
            );
        }
    }
}

#[tokio::test]
async fn lease_or_resumed_wire_expiry_before_commit_refuses_without_records() {
    for expired in ["lease", "resumed-wire"] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let job = issued(&f).await;
        let ttl = Duration::from_secs(if expired == "resumed-wire" { 1 } else { 30 });
        f.coordinator
            .persist_issued_job(&job.context.worker, &job, 0, ttl)
            .await
            .unwrap();
        let revision = prepare_replacement_lease(&f).await;
        let resumed = f
            .coordinator
            .resume_job(&job.context.worker, &job.wire.job_id)
            .await
            .unwrap()
            .unwrap();
        let wire_expiry = resumed.wire.resume_expires_at.unwrap();
        let gate = Arc::new(Gate::default());
        *f.store.append_gate.lock().unwrap() = Some(gate.clone());
        let proof = f.proof(&resumed, 0);
        let coordinator = f.coordinator.clone();
        let pending = tokio::spawn(async move {
            coordinator
                .submit(&resumed.context.worker, &resumed, proof, false.into())
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        if expired == "lease" {
            f.coordinator
                .observed_tip
                .write()
                .await
                .expire_lease_for_test(Duration::from_secs(121));
        } else {
            // Use the real monotonic deadline produced by resume, with the
            // database clock unchanged. A fresh scalar DB read cannot extend it.
            tokio::time::sleep_until(tokio::time::Instant::from_std(wire_expiry)).await;
            assert!(Instant::now() >= wire_expiry);
            assert_eq!(f.store.database_now(), 100_000);
        }
        assert_eq!(f.store.revision.load(Ordering::SeqCst), revision);
        gate.release.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_ne!(
            error.response(json!(41))["error"][2]["reason_id"],
            "ledger-outcome-unknown",
            "{expired}: an append refused before COMMIT has a definite outcome"
        );
        assert!(f.store.records.lock().unwrap().is_empty(), "{expired}");
    }
}

#[tokio::test]
async fn late_lease_changes_after_commit_keep_confirmed_and_unknown_outcomes() {
    for changed in ["lease-expiry", "publication"] {
        for lost_reply in [false, true] {
            let f = Fixture::new(Duration::from_secs(10)).await;
            f.coordinator.refresh_once().await.unwrap();
            let job = issued(&f).await;
            let revision = prepare_replacement_lease(&f).await;
            let gate = Arc::new(Gate::default());
            *f.store.commit_gate.lock().unwrap() = Some(gate.clone());
            let proof = f.proof(&job, 0);
            let coordinator = f.coordinator.clone();
            let pending = tokio::spawn(async move {
                coordinator
                    .submit(&job.context.worker, &job, proof, false.into())
                    .await
            });
            tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
                .await
                .unwrap();
            // This gate is after begin_commit, unlike append_gate above.
            if changed == "publication" {
                tokio::time::timeout(Duration::from_secs(5), f.coordinator.refresh_once())
                    .await
                    .unwrap()
                    .unwrap();
            } else {
                f.coordinator
                    .observed_tip
                    .write()
                    .await
                    .expire_lease_for_test(Duration::from_secs(121));
            }
            assert_eq!(f.store.revision.load(Ordering::SeqCst), revision);
            if lost_reply {
                *f.store.fail_commit.lock().unwrap() = Some(FailCommit::Recorded);
            }
            gate.release.notify_one();
            let result = tokio::time::timeout(Duration::from_secs(5), pending)
                .await
                .unwrap()
                .unwrap();
            if lost_reply {
                assert_error(
                    result.unwrap_err(),
                    "ledger-outcome-unknown",
                    "share outcome is not yet known",
                );
            } else {
                result.expect("authority changes after COMMIT cannot reject a confirmed share");
            }
            let records = f.store.records.lock().unwrap();
            assert_eq!(records.len(), 1, "{changed}/{lost_reply}");
            assert_eq!(records[0].2, revision);
            assert_eq!(records[0].0.credit_policy, None);
            assert_eq!(f.store.cancelled.load(Ordering::SeqCst), 0);
            assert_eq!(
                f.coordinator.accepted.load(Ordering::SeqCst),
                u64::from(!lost_reply)
            );
        }
    }
}

#[tokio::test]
async fn submit_uses_refreshed_poll_when_lease_state_wait_returns_to_published_tip() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let job = issued(&f).await;
    let stale_poll = Instant::now() - f.coordinator.config.health_timeout - Duration::from_secs(1);
    f.coordinator.readiness.write().await.last_poll = Some(stale_poll);
    f.detect(2).await;
    let gate = Arc::new(Gate::default());
    *f.store.compact.state_gate.lock().unwrap() = Some(gate.clone());
    let proof = f.proof(&job, 0);
    let coordinator = f.coordinator.clone();
    let pending = tokio::spawn(async move {
        coordinator
            .submit(&job.context.worker, &job, proof, false.into())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        f.coordinator.readiness.read().await.last_poll,
        Some(stale_poll)
    );
    f.detect(1).await;
    // detect observes the tip; model the successful polling update separately
    // without replacing the prepared object or its publication stamp.
    f.coordinator.readiness.write().await.last_poll = Some(Instant::now());
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .expect("the returned current tip uses the refreshed poll, not the stale entry value");
    let records = f.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].2, 0);
    assert_eq!(records[0].0.credit_policy, None);
}

#[tokio::test]
async fn tip_return_during_lease_state_wait_uses_ordinary_current_work_checks() {
    for operation in ["issue", "resume", "submit"] {
        for changed_revision in [false, true] {
            let f = Fixture::new(Duration::from_secs(10)).await;
            f.coordinator.refresh_once().await.unwrap();
            let job = issued(&f).await;
            persist(&f, &job).await.unwrap();
            f.detect(2).await;
            if changed_revision {
                f.store.revision.store(1, Ordering::SeqCst);
            }
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
            f.detect(1).await;
            gate.release.notify_one();
            assert_eq!(
                matches!(pending.await.unwrap(), Ok(true)),
                !changed_revision,
                "{operation}/{changed_revision}"
            );
            if operation == "submit" && !changed_revision {
                assert_eq!(f.store.records.lock().unwrap()[0].0.credit_policy, None);
            }
        }
    }
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
