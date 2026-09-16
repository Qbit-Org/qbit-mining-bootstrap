use super::*;

async fn cached_inputs_change_during_build_wait(expire: bool) {
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
    if expire {
        tokio::time::sleep(interval).await;
    }
    {
        let mut slot = f.store.snapshot.lock().unwrap();
        let snapshot = slot.as_mut().unwrap();
        snapshot.anchor_ms += 1;
        if !expire {
            snapshot.share_seq += 1;
            let mut share = snapshot.shares.last().unwrap().clone();
            share.share_seq = snapshot.share_seq;
            share.share_id = "arrived-during-build-admission".into();
            snapshot.shares.push(share);
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
        first.snapshot.share_seq + u64::from(!expire)
    );
}

#[tokio::test]
async fn build_wait_rechecks_new_share_cutoff() {
    cached_inputs_change_during_build_wait(false).await;
}

#[tokio::test]
async fn build_wait_rechecks_original_reanchor_age() {
    cached_inputs_change_during_build_wait(true).await;
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
