//! #823: authorization costs one node validation per payout address, however
//! many worker suffixes or concurrent sessions name it, and never more than
//! the frontend's validation slots at once; a session waits for them within
//! one bound.
use super::*;

fn validations(f: &Fixture) -> usize {
    f.node
        .lock()
        .unwrap()
        .calls
        .iter()
        .filter(|method| method.as_str() == "validateaddress")
        .count()
}

#[tokio::test]
async fn worker_suffixes_on_one_address_share_one_validation() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    for index in 0..100 {
        let username = format!("qbaddress.rig{index}");
        let worker = f.coordinator.authorize(&username).await.unwrap();
        assert_eq!(worker.username, username);
        assert_eq!(worker.worker_name, Some(format!("rig{index}")));
        assert_eq!(worker.payout_address, "qbaddress");
        assert_eq!(worker.p2mr_program_hex, hash(0xcd));
    }
    let bare = f.coordinator.authorize("qbaddress").await.unwrap();
    assert_eq!(
        (bare.username.as_str(), bare.worker_name),
        ("qbaddress", None)
    );
    assert_eq!(validations(&f), 1);
    f.coordinator.authorize("qbother.rig0").await.unwrap();
    assert_eq!(validations(&f), 2);
}

#[tokio::test]
async fn concurrent_sessions_naming_a_new_address_share_its_validation() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    let gate = Arc::new(Gate::default());
    f.node.lock().unwrap().gate = Some(("validateaddress".into(), gate.clone()));
    let sessions = (0..16)
        .map(|index| {
            let coordinator = f.coordinator.clone();
            tokio::spawn(async move { coordinator.authorize(&format!("qbburst.rig{index}")).await })
        })
        .collect::<Vec<_>>();
    gate.entered.notified().await;
    // Every other session is now queued behind the one validation.
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.release.notify_one();
    for session in sessions {
        assert_eq!(session.await.unwrap().unwrap().p2mr_program_hex, hash(0xcd));
    }
    assert_eq!(validations(&f), 1);
}

#[tokio::test]
async fn validations_wait_for_a_slot_and_a_full_wait_is_a_retryable_refusal() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    let mut slots = Vec::new();
    for _ in 0..payout_identity::MAX_CONCURRENT_VALIDATIONS {
        slots.push(
            f.coordinator
                .identities
                .validation_slot(tokio::time::Instant::now() + payout_identity::VALIDATION_WAIT)
                .await
                .unwrap(),
        );
    }
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let refused = f.coordinator.authorize("qbwaiting.rig").await.unwrap_err();
    let waited = started.elapsed();
    assert!(
        waited >= payout_identity::VALIDATION_WAIT
            && waited < payout_identity::VALIDATION_WAIT + Duration::from_secs(1),
        "waited {waited:?}"
    );
    tokio::time::resume();
    assert_eq!(
        refused.reason_id.as_deref(),
        Some("backend-rpc-unavailable")
    );
    assert_eq!(
        validations(&f),
        0,
        "a refused authorization called the node"
    );

    // A slot freed while a session waits admits it.
    let waiting = {
        let coordinator = f.coordinator.clone();
        tokio::spawn(async move { coordinator.authorize("qbwaiting.rig").await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished());
    slots.pop();
    assert_eq!(waiting.await.unwrap().unwrap().p2mr_program_hex, hash(0xcd));
    assert_eq!(validations(&f), 1);
}

#[tokio::test]
async fn a_session_behind_another_validation_waits_within_the_same_bound() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    // Another session's validation of the address holds its flight and
    // does not finish.
    let flight = f.coordinator.identities.flight("qbstalled");
    let held = flight.clone().lock_owned().await;
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let refused = tokio::time::timeout(
        Duration::from_secs(60),
        f.coordinator.authorize("qbstalled.rig"),
    )
    .await
    .expect("the wait behind another validation is bounded")
    .unwrap_err();
    let waited = started.elapsed();
    assert!(
        waited >= payout_identity::VALIDATION_WAIT
            && waited < payout_identity::VALIDATION_WAIT + Duration::from_secs(1),
        "waited {waited:?}"
    );
    assert_eq!(
        refused.reason_id.as_deref(),
        Some("backend-rpc-unavailable")
    );

    // The wait behind it and the wait for a slot are one bound, not two:
    // the flight frees after three seconds, every slot stays held.
    let mut slots = Vec::new();
    for _ in 0..payout_identity::MAX_CONCURRENT_VALIDATIONS {
        slots.push(
            f.coordinator
                .identities
                .validation_slot(tokio::time::Instant::now() + payout_identity::VALIDATION_WAIT)
                .await
                .unwrap(),
        );
    }
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        drop(held);
    });
    let started = tokio::time::Instant::now();
    f.coordinator.authorize("qbstalled.rig").await.unwrap_err();
    let waited = started.elapsed();
    assert!(
        waited >= payout_identity::VALIDATION_WAIT
            && waited < payout_identity::VALIDATION_WAIT + Duration::from_secs(1),
        "waited {waited:?}"
    );
    release.await.unwrap();
    tokio::time::resume();
    assert_eq!(
        validations(&f),
        0,
        "a refused authorization called the node"
    );
    drop(slots);
    f.coordinator.authorize("qbstalled.rig").await.unwrap();
    assert_eq!(validations(&f), 1);
}

#[tokio::test]
async fn a_refused_address_resolves_to_the_fallback_in_the_same_slot() {
    let f = Fixture::build(
        Duration::from_secs(10),
        |config| config.username_fallback = Some("qbfallback".into()),
        None,
    )
    .await;
    f.node.lock().unwrap().refused = vec!["qbrefused".into()];
    let mut slots = Vec::new();
    for _ in 0..payout_identity::MAX_CONCURRENT_VALIDATIONS {
        slots.push(
            f.coordinator
                .identities
                .validation_slot(tokio::time::Instant::now() + payout_identity::VALIDATION_WAIT)
                .await
                .unwrap(),
        );
    }
    // The session queues for a slot, and another validation queues behind
    // it, so a slot the session frees between its two node calls is taken
    // and its fallback call would wait out the spent bound as busy.
    let session = {
        let coordinator = f.coordinator.clone();
        tokio::spawn(async move { coordinator.authorize("qbrefused.rig").await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (acquired, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let queued = {
        let coordinator = f.coordinator.clone();
        let (acquired, release) = (acquired.clone(), release.clone());
        tokio::spawn(async move {
            let _slot = coordinator
                .identities
                .validation_slot(tokio::time::Instant::now() + Duration::from_secs(60))
                .await
                .unwrap();
            acquired.notify_one();
            release.notified().await;
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    slots.pop();
    let worker = session.await.unwrap().unwrap();
    assert_eq!(
        (worker.payout_address.as_str(), worker.p2mr_program_hex),
        ("qbfallback", hash(0xcd))
    );
    assert_eq!(validations(&f), 2);
    // The session's slot went to the validation queued behind it.
    tokio::time::timeout(Duration::from_secs(5), acquired.notified())
        .await
        .expect("the session released its slot");
    release.notify_one();
    queued.await.unwrap();

    // The node's refusal was cached, so the address costs no further call.
    drop(slots);
    let worker = f.coordinator.authorize("qbrefused.rig2").await.unwrap();
    assert_eq!(worker.payout_address, "qbfallback");
    assert_eq!(validations(&f), 2);
}

#[test]
fn a_full_cache_drops_fallback_entries_before_addresses() {
    let identities = payout_identity::PayoutIdentities::new();
    let identity = |payout_address: &str| payout_identity::PayoutIdentity {
        payout_address: payout_address.into(),
        p2mr_program_hex: hash(0xcd),
    };
    identities.remember("qbkeep", identity("qbkeep"));
    for index in 1..payout_identity::CACHE_CAPACITY {
        identities.remember(&format!("junk{index}"), identity("qbfallback"));
    }
    identities.remember("qbnext", identity("qbnext"));
    assert_eq!(identities.cached("qbkeep"), Some(identity("qbkeep")));
    assert_eq!(identities.cached("qbnext"), Some(identity("qbnext")));
    assert_eq!(identities.cached("junk1"), None);
}

#[test]
fn a_full_cache_drops_expired_entries_before_fallback_entries() {
    let identities = payout_identity::PayoutIdentities::new();
    let identity = |payout_address: &str| payout_identity::PayoutIdentity {
        payout_address: payout_address.into(),
        p2mr_program_hex: hash(0xcd),
    };
    let start = Instant::now();
    for index in 2..payout_identity::CACHE_CAPACITY {
        let address = format!("qbold{index}");
        identities.remember_at(&address, identity(&address), start);
    }
    // Past the TTL every address entry has expired, and two refused
    // addresses then fill the cache with fallback entries.
    let later = start + payout_identity::IDENTITY_TTL + Duration::from_secs(1);
    identities.remember_at("junk0", identity("qbfallback"), later);
    identities.remember_at("junk1", identity("qbfallback"), later);
    identities.remember_at("qbnext", identity("qbnext"), later);
    assert_eq!(identities.cached("junk0"), Some(identity("qbfallback")));
    assert_eq!(identities.cached("junk1"), Some(identity("qbfallback")));
    assert_eq!(identities.cached("qbnext"), Some(identity("qbnext")));
    assert_eq!(identities.cached("qbold2"), None);
}
