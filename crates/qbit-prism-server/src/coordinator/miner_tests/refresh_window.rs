use super::*;

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
        Arc::downgrade(cache.as_ref().unwrap())
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
