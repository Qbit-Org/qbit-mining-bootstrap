//! The prerequisite APIs exercise real build, capture and authority decisions;
//! scripted I/O never decides publication or manufactures polling readiness.
use super::*;
use prepared_storage::compact::{
    CapturedCompactPrepared, CompactBuildProof, CompactOwner, OriginalPreparedBuild,
};

async fn cold_fixture() -> Fixture {
    let f = Fixture::new(Duration::from_secs(10)).await;
    *f.coordinator.prepared.write().await = None;
    *f.coordinator.readiness.write().await = ReadinessState::default();
    f
}

async fn build_original(
    f: &Fixture,
    proof: CompactBuildProof,
    empty: bool,
    template_time: Option<u64>,
) -> CompactOwner<CapturedCompactPrepared> {
    // The caller captured its epoch before any of this original build work.
    let mut template = f
        .coordinator
        .rpc
        .call("getblocktemplate", json!([]))
        .await
        .unwrap();
    if let Some(time) = template_time {
        template["curtime"] = json!(time);
    }
    let mut snapshot = f.store.snapshot.lock().unwrap().clone().unwrap();
    if empty {
        snapshot.shares.clear();
    }
    let snapshot = Arc::new(snapshot);
    let inputs = BundleInputs::capture(&f.coordinator.config, None).unwrap();
    let suffix = "00".repeat(12);
    let bundle = if empty {
        None
    } else {
        Some(Arc::new(
            f.coordinator
                .build_bundle(
                    snapshot.clone(),
                    template.clone(),
                    None,
                    suffix.clone(),
                    inputs.clone(),
                )
                .await
                .unwrap()
                .0,
        ))
    };
    let window = WindowRef::from_snapshot(&snapshot).unwrap();
    let stored = Arc::new(StoredPrepared {
        template,
        snapshot,
        bundle,
        fee: None,
        fingerprint: "original-template".into(),
        generation: 2,
        parent_of_tip: hash(0),
        coinbase_suffix: suffix,
    });
    let source = OriginalPreparedBuild::from_proven_original_build(
        proof,
        format!("prepared:authority:{}", uuid::Uuid::new_v4().simple()),
        stored,
        window,
        inputs,
    );
    f.coordinator
        .capture_compact_prepared(source, 130_000)
        .await
        .unwrap()
}

async fn captured(f: &Fixture) -> CompactOwner<CapturedCompactPrepared> {
    let proof = f.coordinator.begin_compact_build().await;
    build_original(f, proof, false, None).await
}

async fn assert_unpublished(f: &Fixture) {
    assert!(f.coordinator.prepared.read().await.is_none());
    assert!(f.coordinator.readiness.read().await.last_poll.is_none());
    assert!(f
        .coordinator
        .observed_tip
        .read()
        .await
        .retention_hint()
        .is_none());
    assert_eq!(*f.coordinator.refresh.borrow(), 1);
}

#[tokio::test]
async fn cold_original_reservation_and_atomic_publication_preserve_identity() {
    for empty in [false, true] {
        let f = cold_fixture().await;
        let proof = f.coordinator.begin_compact_build().await;
        let captured = build_original(&f, proof, empty, None).await;
        assert_eq!(captured.record.window.shares.is_none(), empty);
        assert_unpublished(&f).await;
        f.store
            .compact
            .saves
            .lock()
            .unwrap()
            .extend([Ok(true), Ok(false)]);
        {
            let reserved = f
                .coordinator
                .reserve_fresh_compact(&captured)
                .await
                .unwrap();
            assert!(reserved.inserted);
        }
        assert_unpublished(&f).await;
        let retry = f
            .coordinator
            .reserve_fresh_compact(&captured)
            .await
            .unwrap();
        assert!(!retry.inserted);
        assert_unpublished(&f).await;
        {
            let calls = f.store.compact.save_calls.lock().unwrap();
            assert_eq!(calls.len(), 2);
            for call in calls.iter() {
                assert_eq!(call.key, captured.original.storage_key);
                assert_eq!(call.record, captured.record);
                assert_eq!(call.current_revision, captured.record.payout_revision);
                assert_eq!(call.original_expires_at_ms, 130_000);
                assert_eq!(call.template_sha256, captured.template.sha256());
            }
        }
        let guard = f.coordinator.lock_compact_publication(retry).await.unwrap();
        guard.publish().unwrap();
        let published = f.coordinator.prepared.read().await.clone().unwrap();
        assert!(Arc::ptr_eq(&published, &captured.original));
        assert_eq!(published.window, captured.record.window);
        assert_eq!(published.snapshot.payout_revision, 0);
        assert!(f.coordinator.readiness.read().await.last_poll.is_some());
        assert_eq!(*f.coordinator.refresh.borrow(), 2);
        assert_eq!(
            f.coordinator
                .observed_tip
                .read()
                .await
                .retention_hint()
                .unwrap()
                .hash,
            hash(1)
        );
        assert!(
            f.store.jobs.lock().unwrap().is_empty(),
            "no inline persistence"
        );
    }
}

#[tokio::test]
async fn invalidation_before_original_build_cannot_be_recaptured_afterward() {
    let f = cold_fixture().await;
    let proof = f.coordinator.begin_compact_build().await;
    f.coordinator.invalidate_readiness().await;
    let captured = build_original(&f, proof, false, None).await;
    assert!(f
        .coordinator
        .reserve_fresh_compact(&captured)
        .await
        .is_err());
    assert!(f.store.compact.save_calls.lock().unwrap().is_empty());
    assert_unpublished(&f).await;
}

#[tokio::test]
async fn cold_invalidation_during_rpc_state_clock_or_persistence_never_publishes() {
    for stage in ["rpc", "state", "clock", "save"] {
        let f = cold_fixture().await;
        let captured = captured(&f).await;
        let gate = Arc::new(Gate::default());
        match stage {
            "rpc" => f.node.lock().unwrap().gate = Some(("getblockchaininfo".into(), gate.clone())),
            "state" => *f.store.compact.state_gate.lock().unwrap() = Some(gate.clone()),
            "clock" => *f.store.compact.clock_gate.lock().unwrap() = Some(gate.clone()),
            _ => *f.store.compact.save_gate.lock().unwrap() = Some(gate.clone()),
        }
        f.store.compact.saves.lock().unwrap().push_back(Ok(true));
        let c = f.coordinator.clone();
        let task = tokio::spawn(async move {
            c.reserve_fresh_compact(&captured)
                .await
                .map(|reservation| reservation.inserted)
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        // Invalidation must not be blocked behind a reservation's database I/O.
        tokio::time::timeout(Duration::from_secs(1), f.coordinator.invalidate_readiness())
            .await
            .unwrap();
        gate.release.notify_one();
        let error = task.await.unwrap().unwrap_err();
        assert!(
            error.to_string().contains("readiness changed"),
            "{stage}: {error}"
        );
        assert_unpublished(&f).await;
        assert_eq!(
            f.store.compact.save_calls.lock().unwrap().len(),
            usize::from(stage == "save")
        );
    }
}

#[tokio::test]
async fn economics_changed_during_second_node_check_reject_before_compact_save() {
    for changed in ["revision", "balances"] {
        let f = cold_fixture().await;
        let captured = captured(&f).await;
        let clock = Arc::new(Gate::default());
        *f.store.compact.clock_gate.lock().unwrap() = Some(clock.clone());
        // A misplaced economic fence can reach persistence and only reject
        // afterward. Permit that fake write so the no-save assertion detects it.
        f.store.compact.saves.lock().unwrap().push_back(Ok(true));
        let coordinator = f.coordinator.clone();
        let task = tokio::spawn(async move {
            coordinator
                .reserve_fresh_compact(&captured)
                .await
                .map(|reserved| reserved.inserted)
        });
        tokio::time::timeout(Duration::from_secs(5), clock.entered.notified())
            .await
            .unwrap();
        let node = Arc::new(Gate::default());
        f.node.lock().unwrap().gate = Some(("getblockchaininfo".into(), node.clone()));
        clock.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), node.entered.notified())
            .await
            .unwrap();
        if changed == "revision" {
            f.store.revision.store(1, Ordering::SeqCst);
        } else {
            // Mutate real fixture balances: payout_state computes its digest
            // from these rows, without a scripted digest or revision change.
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
        assert_unpublished(&f).await;
        node.release.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            error.to_string().contains("payout snapshot stale"),
            "{changed}: {error}"
        );
        assert!(
            f.store.compact.save_calls.lock().unwrap().is_empty(),
            "{changed}: economic change must reject before persistence"
        );
        assert_unpublished(&f).await;
    }
}

#[tokio::test]
async fn cold_proof_rejects_changed_economics_tip_and_stale_template_without_a_lease() {
    for changed in ["revision", "balances", "tip", "template"] {
        let f = cold_fixture().await;
        let proof = f.coordinator.begin_compact_build().await;
        let captured = build_original(&f, proof, false, (changed == "template").then_some(0)).await;
        match changed {
            "revision" => {
                f.store.revision.store(1, Ordering::SeqCst);
            }
            "balances" => {
                f.store
                    .compact
                    .states
                    .lock()
                    .unwrap()
                    .push_back(Ok(crate::ledger::PayoutState {
                        payout_revision: 0,
                        prior_balances_digest: [7; 32],
                    }))
            }
            "tip" => f.node.lock().unwrap().tip = hash(2),
            _ => {}
        }
        assert!(
            f.coordinator
                .reserve_fresh_compact(&captured)
                .await
                .is_err(),
            "{changed}"
        );
        assert!(f.store.compact.save_calls.lock().unwrap().is_empty());
        assert_unpublished(&f).await;
    }
}

#[tokio::test]
async fn cold_reservation_is_not_authority_after_invalidation_or_new_publication() {
    for changed in ["invalidation", "publication"] {
        let f = cold_fixture().await;
        let captured = captured(&f).await;
        f.store.compact.saves.lock().unwrap().push_back(Ok(true));
        let reserved = f
            .coordinator
            .reserve_fresh_compact(&captured)
            .await
            .unwrap();
        if changed == "invalidation" {
            f.coordinator.invalidate_readiness().await;
        } else {
            f.coordinator.refresh_once().await.unwrap();
        }
        let current = f.coordinator.prepared.read().await.clone();
        assert!(f
            .coordinator
            .lock_compact_publication(reserved)
            .await
            .is_err());
        let after = f.coordinator.prepared.read().await.clone();
        assert_eq!(
            current.as_ref().map(|p| &p.storage_key),
            after.as_ref().map(|p| &p.storage_key)
        );
        assert!(after
            .as_ref()
            .is_none_or(|p| p.storage_key != captured.original.storage_key));
    }
}

#[tokio::test]
async fn cold_reservation_expiring_while_final_publication_lock_waits_stays_unpublished() {
    let f = cold_fixture().await;
    let captured = captured(&f).await;
    f.store.compact.saves.lock().unwrap().push_back(Ok(true));
    let reserved = f
        .coordinator
        .reserve_fresh_compact(&captured)
        .await
        .unwrap();
    // The proof's reads can finish, but its final atomic write must wait.
    let held = f.coordinator.prepared.read().await;
    let (result, ()) = tokio::join!(f.coordinator.lock_compact_publication(reserved), async {
        tokio::time::timeout(Duration::from_secs(5), async {
            // Tokio's fair lock refuses new readers once the final writer
            // queues. This proves all asynchronous authority I/O finished.
            while f.coordinator.prepared.try_read().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(30)).await;
        drop(held);
    });
    let error = result.err().expect("expired final lock must fail");
    assert!(error.to_string().contains("reservation deadline elapsed"));
    assert_unpublished(&f).await;
}

#[tokio::test]
async fn economics_changed_while_final_publication_lock_waits_rejects_guard() {
    let mut unexpected_guards = Vec::new();
    for changed in ["revision", "balances"] {
        let f = cold_fixture().await;
        let captured = captured(&f).await;
        f.store.compact.saves.lock().unwrap().push_back(Ok(true));
        let reserved = f
            .coordinator
            .reserve_fresh_compact(&captured)
            .await
            .unwrap();
        let epoch = f.coordinator.readiness.read().await.generation;
        let held = f.coordinator.prepared.read().await;
        let (result, ()) = tokio::join!(f.coordinator.lock_compact_publication(reserved), async {
            tokio::time::timeout(Duration::from_secs(5), async {
                // The read guard permits the proof's reads but blocks its
                // final writer. Refusing a new reader proves that writer queued.
                while f.coordinator.prepared.try_read().is_ok() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if changed == "revision" {
                f.store.revision.store(1, Ordering::SeqCst);
            } else {
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
            // Model a different frontend's economic change, without any local
            // invalidation or publication that could trip the captured stamp.
            drop(held);
        });
        // Consume any unexpected guard before checking locks it would hold;
        // the test must reject the guard itself, never call publish on it.
        let error = result.err();
        if let Some(error) = error {
            assert!(
                error.to_string().contains("payout snapshot stale"),
                "{changed}: {error}"
            );
        } else {
            unexpected_guards.push(changed);
        }
        assert_eq!(f.coordinator.readiness.read().await.generation, epoch);
        assert_unpublished(&f).await;
    }
    assert!(
        unexpected_guards.is_empty(),
        "economic changes during publication lock wait returned guards: {unexpected_guards:?}"
    );
}

#[tokio::test]
async fn cold_publication_guard_rechecks_expiry_at_installation() {
    let f = cold_fixture().await;
    let captured = captured(&f).await;
    f.store.compact.saves.lock().unwrap().push_back(Ok(true));
    let reserved = f
        .coordinator
        .reserve_fresh_compact(&captured)
        .await
        .unwrap();
    let guard = f
        .coordinator
        .lock_compact_publication(reserved)
        .await
        .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    let error = guard.publish().unwrap_err();
    assert!(error.to_string().contains("reservation deadline elapsed"));
    assert_unpublished(&f).await;
}

#[tokio::test]
async fn cached_same_arc_republication_invalidates_original_build_proof() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let original = f.coordinator.prepared.read().await.clone().unwrap();
    let published_tip = f.coordinator.observed_tip.read().await.publication_stamp();
    let proof = f.coordinator.begin_compact_build().await;
    let captured = build_original(&f, proof, false, None).await;
    f.coordinator.refresh_once().await.unwrap();
    let republished = f.coordinator.prepared.read().await.clone().unwrap();
    assert!(
        Arc::ptr_eq(&original, &republished),
        "exercise cached reuse"
    );
    assert_ne!(
        published_tip,
        f.coordinator.observed_tip.read().await.publication_stamp()
    );
    let error = f
        .coordinator
        .reserve_fresh_compact(&captured)
        .await
        .err()
        .expect("cached publication must invalidate the earlier proof");
    assert!(error.to_string().contains("publication changed"));
    assert!(f.store.compact.save_calls.lock().unwrap().is_empty());
    assert!(Arc::ptr_eq(
        f.coordinator.prepared.read().await.as_ref().unwrap(),
        &republished
    ));
}

#[tokio::test]
async fn cold_reservation_and_publication_preserve_typed_state_failures() {
    for publication in [false, true] {
        for kind in ["database", "decode", "task"] {
            let f = cold_fixture().await;
            let captured = captured(&f).await;
            let reserved = if publication {
                f.store.compact.saves.lock().unwrap().push_back(Ok(true));
                Some(
                    f.coordinator
                        .reserve_fresh_compact(&captured)
                        .await
                        .unwrap(),
                )
            } else {
                None
            };
            let failure = match kind {
                "database" => WindowError::Database(sqlx::Error::PoolClosed),
                "decode" => WindowError::Decode(anyhow::anyhow!("controlled state decode failure")),
                _ => {
                    let task = tokio::spawn(std::future::pending::<()>());
                    task.abort();
                    WindowError::TaskFailed(task.await.unwrap_err())
                }
            };
            f.store
                .compact
                .states
                .lock()
                .unwrap()
                .push_back(Err(failure));
            let error = if let Some(reserved) = reserved {
                f.coordinator.lock_compact_publication(reserved).await.err()
            } else {
                f.coordinator.reserve_fresh_compact(&captured).await.err()
            }
            .expect("state failures must not become a successful reservation or publication");
            assert!(match (kind, error.downcast::<WindowError>().unwrap()) {
                ("database", WindowError::Database(sqlx::Error::PoolClosed)) => true,
                ("decode", WindowError::Decode(error)) =>
                    error.to_string() == "controlled state decode failure",
                ("task", WindowError::TaskFailed(error)) => error.is_cancelled(),
                _ => false,
            });
            assert_eq!(
                f.store.compact.save_calls.lock().unwrap().len(),
                usize::from(publication)
            );
            assert_unpublished(&f).await;
        }
    }
}

#[tokio::test]
async fn one_outer_deadline_or_cancellation_never_bootstraps_cold_readiness() {
    for deadline in [false, true] {
        for stage in ["state", "save"] {
            let f = Arc::new(cold_fixture().await);
            let gate = Arc::new(Gate::default());
            if stage == "state" {
                *f.store.compact.state_gate.lock().unwrap() = Some(gate.clone());
            } else {
                *f.store.compact.save_gate.lock().unwrap() = Some(gate.clone());
            }
            f.store.compact.saves.lock().unwrap().push_back(Ok(true));
            let operation = f.clone();
            let task = tokio::spawn(async move {
                // One caller deadline spans proof, original build/capture,
                // reservation and final publication; no per-stage renewal.
                tokio::time::timeout(Duration::from_secs(30), async {
                    let captured = captured(&operation).await;
                    let reserved = operation
                        .coordinator
                        .reserve_fresh_compact(&captured)
                        .await?;
                    let guard = operation
                        .coordinator
                        .lock_compact_publication(reserved)
                        .await?;
                    guard.publish()
                })
                .await
            });
            tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
                .await
                .unwrap();
            assert_unpublished(&f).await;
            if deadline {
                // Pause only after network/build work reached a controlled
                // wait, so advancing time cannot race real fixture RPC I/O.
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(30)).await;
                assert!(
                    task.await.unwrap().is_err(),
                    "the outer deadline must expire"
                );
                tokio::time::resume();
            } else {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            }
            gate.release.notify_one();
            tokio::task::yield_now().await;
            assert_unpublished(&f).await;
            assert_eq!(
                f.store.compact.save_calls.lock().unwrap().len(),
                usize::from(stage == "save")
            );
            let permit = tokio::time::timeout(
                Duration::from_secs(5),
                f.coordinator.build_slots.clone().acquire_owned(),
            )
            .await
            .unwrap()
            .unwrap();
            drop(permit);
        }
    }
}
