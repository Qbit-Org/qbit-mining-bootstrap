use super::*;

#[tokio::test]
async fn overlapped_refresh_matches_serial_native_and_audit_bytes() {
    for ctv in [false, true] {
        for count in [0, 1, 512] {
            let f = Fixture::build(
                Duration::from_secs(10),
                |config| {
                    config.ctv_enabled = ctv;
                    config.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
                },
                None,
            )
            .await;
            {
                let mut slot = f.store.snapshot.lock().unwrap();
                let snapshot = slot.as_mut().unwrap();
                let seed = snapshot.shares[0].clone();
                snapshot.shares = (1..=count)
                    .map(|seq| {
                        let mut share = seed.clone();
                        share.share_seq = seq;
                        share.share_id = format!("overlap-{seq}");
                        share
                    })
                    .collect();
                snapshot.share_seq = count;
                snapshot.prior_balances = vec![
                    qbit_prism::CarryForwardBalance {
                        recipient_id: "a".into(),
                        order_key: "a".into(),
                        p2mr_program_hex: hash(0x12),
                        balance_sats: 123,
                    },
                    qbit_prism::CarryForwardBalance {
                        recipient_id: "B".into(),
                        order_key: "B".into(),
                        p2mr_program_hex: hash(0x34),
                        balance_sats: 456,
                    },
                ];
            }
            // First iteration uses both workers. Changing only the template
            // uses the cached window and the serial preparation path.
            for cached in [false, true] {
                if cached {
                    let mut template = f
                        .coordinator
                        .prepared
                        .read()
                        .await
                        .as_ref()
                        .unwrap()
                        .template
                        .clone();
                    template["coinbasevalue"] = json!(500_000_001);
                    f.node.lock().unwrap().template = Some(template);
                }
                f.coordinator.refresh_once().await.unwrap();
                let prepared = f.coordinator.prepared.read().await.clone().unwrap();
                let original = f.original(&prepared);
                assert_eq!(
                    prepared.window,
                    WindowRef::from_snapshot(&original.snapshot).unwrap()
                );
                if let Some(range) = prepared.window.shares {
                    let bytes = serde_json::to_vec(&original.snapshot.shares).unwrap();
                    assert_eq!(
                        range.snapshot_sha256,
                        <[u8; 32]>::from(Sha256::digest(bytes))
                    );
                    let bundle = original.bundle.as_ref().unwrap();
                    let bytes = qbit_prism::canonical_audit_bundle_bytes(bundle).unwrap();
                    let hashes = prepared.reservation.record.audit_hashes.as_ref().unwrap();
                    assert_eq!(
                        hashes.audit_bundle_sha256,
                        hex::encode(Sha256::digest(bytes))
                    );
                    assert_eq!(
                        hashes.coinbase_manifest_sha256,
                        hex::encode(Sha256::digest(
                            serde_json::to_vec(&bundle.signed_coinbase_manifest.manifest).unwrap()
                        ))
                    );
                } else {
                    assert!(prepared.reservation.record.audit_hashes.is_none());
                    assert!(prepared.bundle.is_none());
                }
                assert_eq!(f.coordinator.build_slots.available_permits(), 1);
            }
        }
    }
}

#[test]
fn simultaneous_refreshes_finish_with_one_build_slot_and_one_blocking_thread() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let f = Fixture::new(Duration::from_secs(10)).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            let (first, second) =
                tokio::join!(f.coordinator.refresh_once(), f.coordinator.refresh_once());
            first.unwrap();
            second.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(f.store.snapshots.lock().unwrap().len(), 1);
        assert_eq!(f.coordinator.build_slots.available_permits(), 1);
    });
}

#[tokio::test]
async fn economic_drift_during_fee_probe_defers_until_next_refresh() {
    let f = Fixture::build(
        Duration::from_secs(10),
        |config| {
            config.ctv_enabled = true;
            config.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
        },
        None,
    )
    .await;
    f.coordinator.refresh_once().await.unwrap();
    let first = f.coordinator.prepared.read().await.clone().unwrap();
    let saves = f.store.compact.save_calls.lock().unwrap().len();
    let calls = f.store.compact.state_calls.load(Ordering::SeqCst);
    let gate = Arc::new(Gate::default());
    f.node.lock().unwrap().gate = Some(("getmempoolinfo".into(), gate.clone()));
    let c = f.coordinator.clone();
    let pending =
        tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
            async move { c.refresh_once().await },
        ));
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    // A real wait between the old early and late reads. The revision stays
    // fixed; the immutable published work still names the original digest.
    f.store
        .snapshot
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .prior_balances
        .push(qbit_prism::CarryForwardBalance {
            recipient_id: "during-fee".into(),
            order_key: "during-fee".into(),
            p2mr_program_hex: "34".repeat(32),
            balance_sats: 123,
        });
    gate.release.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(format!("{error:#}").contains("payout state changed during work reuse"));
    assert!(Arc::ptr_eq(
        &first,
        f.coordinator.prepared.read().await.as_ref().unwrap()
    ));
    assert_eq!(f.store.compact.save_calls.lock().unwrap().len(), saves);
    assert_eq!(
        f.store.compact.state_calls.load(Ordering::SeqCst) - calls,
        2
    );
    eprintln!(
        "baseline economic drift after early probe: error, 2 state reads, 0 new reservations"
    );
    f.coordinator.refresh_once().await.unwrap();
    let current = f.coordinator.prepared.read().await.clone().unwrap();
    assert_eq!(
        current.snapshot.payout_revision,
        first.snapshot.payout_revision
    );
    assert_ne!(
        current.window.prior_balances_digest,
        first.window.prior_balances_digest
    );
    assert!(current.generation > first.generation);
    assert!(first.reservation.balances.is_empty());
    assert_eq!(current.reservation.balances[0].balance_sats, 123);
}

#[tokio::test]
async fn ledger_probes_run_only_where_they_can_keep_work() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let published = f.coordinator.prepared.read().await.clone().unwrap();
    let probes = || f.store.compact.probe_calls.load(Ordering::SeqCst);
    let snapshots = || f.store.snapshots.lock().unwrap().len();
    // Same template: the early probe decides that the published work stays.
    let (before, taken) = (probes(), snapshots());
    f.coordinator.refresh_once().await.unwrap();
    assert_eq!(probes() - before, 1);
    assert_eq!(snapshots(), taken);
    assert!(Arc::ptr_eq(
        &published,
        f.coordinator.prepared.read().await.as_ref().unwrap()
    ));
    // A new tip replaces the published work whatever the early probe would
    // show, and the transition's revision bump outdates the cached window:
    // no probe at all, one snapshot read.
    let (before, taken) = (probes(), snapshots());
    f.detect(2).await;
    f.coordinator.refresh_once().await.unwrap();
    assert_eq!(probes() - before, 0);
    assert_eq!(snapshots(), taken + 1);
    let rebuilt = f.coordinator.prepared.read().await.clone().unwrap();
    assert_eq!(rebuilt.template["previousblockhash"], hash(2));
    assert_eq!(
        rebuilt.snapshot.payout_revision,
        published.snapshot.payout_revision + 1
    );
    // A tip a peer already recorded leaves the revision alone, so the cached
    // window may still fit: only the admitted probe runs, and the window is
    // reused without a snapshot read.
    *f.store.tip.lock().unwrap() = Some(hash(3));
    let (before, taken) = (probes(), snapshots());
    f.detect(3).await;
    f.coordinator.refresh_once().await.unwrap();
    assert_eq!(probes() - before, 1);
    assert_eq!(snapshots(), taken);
    let reused = f.coordinator.prepared.read().await.clone().unwrap();
    assert_eq!(reused.template["previousblockhash"], hash(3));
    assert_eq!(
        reused.snapshot.payout_revision,
        rebuilt.snapshot.payout_revision
    );
    assert_eq!(reused.window, rebuilt.window);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BuildWaitChange {
    Share,
    AnchorAge,
    Balance,
    Revision,
}

async fn cached_inputs_change_during_build_wait(change: BuildWaitChange) {
    let interval = Duration::from_secs(2);
    let f = Fixture::build(
        Duration::from_secs(10),
        |c| c.snapshot_interval = interval,
        None,
    )
    .await;
    let started = Instant::now();
    f.coordinator.refresh_once().await.unwrap();
    let first = f.coordinator.prepared.read().await.clone().unwrap();
    let reads = f.store.snapshots.lock().unwrap().len();
    let permit = f
        .coordinator
        .build_slots
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let gate = Arc::new(Gate::default());
    *f.store.compact.clock_gate.lock().unwrap() = Some(gate.clone());
    let mut template = first.template.clone();
    template["coinbasevalue"] = json!(500_000_001);
    f.node.lock().unwrap().template = Some(template);
    let c = f.coordinator.clone();
    let mut pending = tokio::spawn(async move { c.refresh_once().await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    assert!(
        started.elapsed() < interval,
        "fixture did not reach admission before reanchor"
    );
    gate.release.notify_one();
    // The last clock probe has completed. The sole build permit is still
    // held here, so this refresh cannot select or build its next window yet.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut pending)
            .await
            .is_err()
    );
    if change == BuildWaitChange::AnchorAge {
        tokio::time::sleep(interval).await;
    }
    {
        let mut slot = f.store.snapshot.lock().unwrap();
        let snapshot = slot.as_mut().unwrap();
        snapshot.anchor_ms += 1;
        if change == BuildWaitChange::Share {
            snapshot.share_seq += 1;
            let mut share = snapshot.shares.last().unwrap().clone();
            share.share_seq = snapshot.share_seq;
            share.share_id = "arrived-during-build-admission".into();
            snapshot.shares.push(share);
        }
        if change == BuildWaitChange::Balance {
            snapshot
                .prior_balances
                .push(qbit_prism::CarryForwardBalance {
                    recipient_id: "during-admission".into(),
                    order_key: "during-admission".into(),
                    p2mr_program_hex: "34".repeat(32),
                    balance_sats: 123,
                });
        }
        if change == BuildWaitChange::Revision {
            snapshot.payout_revision += 1;
            f.store
                .revision
                .store(snapshot.payout_revision, Ordering::SeqCst);
        }
    }
    drop(permit);
    tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let current = f.coordinator.prepared.read().await.clone().unwrap();
    assert_eq!(
        f.store.snapshots.lock().unwrap().len(),
        reads + 1,
        "build admission reused inputs invalidated while waiting"
    );
    assert_eq!(current.snapshot.anchor_ms, first.snapshot.anchor_ms + 1);
    assert_eq!(
        current.snapshot.share_seq,
        first.snapshot.share_seq + u64::from(change == BuildWaitChange::Share)
    );
    if change == BuildWaitChange::Balance {
        assert_eq!(
            current.snapshot.payout_revision,
            first.snapshot.payout_revision
        );
        assert_ne!(
            current.window.prior_balances_digest,
            first.window.prior_balances_digest
        );
        assert!(first.reservation.balances.is_empty());
        assert_eq!(current.reservation.balances[0].balance_sats, 123);
    }
    if change == BuildWaitChange::Revision {
        assert_eq!(
            current.snapshot.payout_revision,
            first.snapshot.payout_revision + 1
        );
    }
}

#[tokio::test]
async fn build_wait_rechecks_new_share_cutoff() {
    cached_inputs_change_during_build_wait(BuildWaitChange::Share).await;
}

#[tokio::test]
async fn build_wait_rechecks_original_reanchor_age() {
    cached_inputs_change_during_build_wait(BuildWaitChange::AnchorAge).await;
}

#[tokio::test]
async fn build_wait_rechecks_same_revision_balance_drift() {
    cached_inputs_change_during_build_wait(BuildWaitChange::Balance).await;
}

#[tokio::test]
async fn build_wait_rechecks_payout_revision_drift() {
    cached_inputs_change_during_build_wait(BuildWaitChange::Revision).await;
}

#[tokio::test]
async fn balance_identity_invalidates_window_even_at_the_same_revision() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let first = f.coordinator.prepared.read().await.clone().unwrap();
    let reads = f.store.snapshots.lock().unwrap().len();
    f.store
        .snapshot
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .prior_balances
        .push(qbit_prism::CarryForwardBalance {
            recipient_id: "carry-only".into(),
            order_key: "carry-only".into(),
            p2mr_program_hex: "34".repeat(32),
            balance_sats: 123,
        });
    f.coordinator.refresh_once().await.unwrap();
    let current = f.coordinator.prepared.read().await.clone().unwrap();
    assert_eq!(
        current.snapshot.payout_revision,
        first.snapshot.payout_revision
    );
    assert_ne!(
        current.window.prior_balances_digest,
        first.window.prior_balances_digest
    );
    assert_eq!(f.store.snapshots.lock().unwrap().len(), reads + 1);
    assert!(
        current.generation > first.generation,
        "changed balances reused the old generation"
    );
    assert!(
        first.reservation.balances.is_empty(),
        "old prepared balances changed"
    );
    assert_eq!(current.reservation.balances[0].balance_sats, 123);
}

#[tokio::test]
async fn retained_prepared_work_does_not_retain_retired_window_rows() {
    let f = Fixture::build(
        Duration::from_secs(10),
        |config| {
            config.snapshot_interval = Duration::ZERO;
        },
        None,
    )
    .await;
    f.coordinator.refresh_once().await.unwrap();
    let first = f.coordinator.prepared.read().await.clone().unwrap();
    let window = {
        let cache = f.coordinator.refresh_lock.lock().await;
        Arc::downgrade(cache.cached_window.as_ref().unwrap())
    };
    assert_eq!(window.strong_count(), 1, "issued work retained the window");
    f.coordinator.refresh_once().await.unwrap();
    assert!(
        window.upgrade().is_none(),
        "reanchor kept old accepted rows alive"
    );
    assert!(
        first.window.shares.is_some(),
        "old issued reference disappeared"
    );
    assert_eq!(f.coordinator.build_slots.available_permits(), 1);
}

#[tokio::test]
async fn empty_window_is_cached_until_the_first_accepted_share() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    let original = {
        let mut slot = f.store.snapshot.lock().unwrap();
        let snapshot = slot.as_mut().unwrap();
        let original = snapshot.clone();
        snapshot.shares.clear();
        snapshot.share_seq = 0;
        original
    };
    f.coordinator.refresh_once().await.unwrap();
    let first = f.coordinator.prepared.read().await.clone().unwrap();
    let reads = f.store.snapshots.lock().unwrap().len();
    f.coordinator.refresh_once().await.unwrap();
    assert_eq!(f.store.snapshots.lock().unwrap().len(), reads);
    assert!(Arc::ptr_eq(
        f.coordinator.prepared.read().await.as_ref().unwrap(),
        &first
    ));
    *f.store.snapshot.lock().unwrap() = Some(original);
    f.coordinator.refresh_once().await.unwrap();
    assert_eq!(f.store.snapshots.lock().unwrap().len(), reads + 1);
    assert!(f
        .coordinator
        .prepared
        .read()
        .await
        .as_ref()
        .unwrap()
        .window
        .shares
        .is_some());
}

#[tokio::test]
async fn failed_reanchor_retries_publication_from_cached_inputs() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let first = f.coordinator.prepared.read().await.clone().unwrap();
    // Force a fresh anchor with otherwise identical economics and template.
    f.coordinator.refresh_lock.lock().await.cached_window.take();
    f.store.snapshot.lock().unwrap().as_mut().unwrap().anchor_ms += 1;
    f.store.fail_save.store(true, Ordering::SeqCst);
    assert!(f.coordinator.refresh_once().await.is_err());
    assert!(Arc::ptr_eq(
        f.coordinator.prepared.read().await.as_ref().unwrap(),
        &first
    ));
    let reads = f.store.snapshots.lock().unwrap().len();
    f.store.fail_save.store(false, Ordering::SeqCst);
    f.coordinator.refresh_once().await.unwrap();
    let current = f.coordinator.prepared.read().await.clone().unwrap();
    assert_ne!(
        current.storage_key, first.storage_key,
        "unpublished cached inputs were mistaken for the published reservation"
    );
    assert_eq!(current.window.anchor_ms, first.window.anchor_ms + 1);
    assert_eq!(
        f.store.snapshots.lock().unwrap().len(),
        reads,
        "retry discarded valid captured inputs"
    );
}
