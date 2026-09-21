use super::*;
use crate::coordinator::{
    miner_tests::{hash, Fixture, Gate},
    tip_observation::IssuanceAuthority,
};
use futures_util::{future::join_all, poll};
use std::{sync::atomic::Ordering, time::Duration};

const BOUNDARIES: [Boundary; 4] = [
    Boundary::BuildEntry,
    Boundary::PostMaterialization,
    Boundary::PrePersistence,
    Boundary::PostPersistence,
];

async fn fixture() -> (Fixture, Arc<PreparedIdentity>) {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let identity = Arc::new(PreparedIdentity::of(
        f.coordinator.prepared.read().await.as_ref().unwrap(),
    ));
    f.store.revision_calls.store(0, Ordering::SeqCst);
    (f, identity)
}

async fn gate(f: &Fixture) -> Arc<Gate> {
    let gate = Arc::new(Gate::default());
    *f.store.revision_gate.lock().unwrap() = Some(gate.clone());
    gate
}

async fn entered(gate: &Gate) {
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
}

async fn proof(f: &Fixture, identity: &PreparedIdentity) -> IssuanceAuthority {
    let epoch = f.coordinator.readiness.read().await.generation;
    f.coordinator
        .begin_issuance_authority(identity.clone(), epoch, None)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn closed_cohort_rejects_late_join_and_completed_result_reuse() {
    for boundary in BOUNDARIES {
        let (f, identity) = fixture().await;
        let observer = &f.coordinator.revision_observer;
        let gate = gate(&f).await;
        let mut first = Box::pin(observer.observe(boundary, identity.clone(), 0, None));
        let mut peer = Box::pin(observer.observe(boundary, identity.clone(), 0, None));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut peer).is_pending());
        entered(&gate).await;
        assert_eq!(f.store.revision_calls.load(Ordering::SeqCst), 1);
        f.store.revision.store(1, Ordering::SeqCst);
        let mut late = Box::pin(observer.observe(boundary, identity.clone(), 0, None));
        assert!(poll!(&mut late).is_pending());
        gate.release.notify_one();
        assert_eq!(late.await.unwrap(), 1);
        // Leave the first cohort's result unconsumed while another read runs.
        f.store.revision.store(2, Ordering::SeqCst);
        assert_eq!(
            observer.observe(boundary, identity, 0, None).await.unwrap(),
            2
        );
        assert_eq!(first.await.unwrap(), 0);
        assert_eq!(peer.await.unwrap(), 0);
        assert_eq!(f.store.revision_calls.load(Ordering::SeqCst), 3);
        assert_eq!(observer.slots.available_permits(), ADMITTED);
    }
}

#[tokio::test]
async fn all_boundaries_are_distinct_and_reordered_replies_install_no_state() {
    for old_boundary in BOUNDARIES {
        for new_boundary in BOUNDARIES {
            if old_boundary as usize == new_boundary as usize {
                continue;
            }
            let (f, identity) = fixture().await;
            let observer = &f.coordinator.revision_observer;
            let gate = gate(&f).await;
            let mut old = Box::pin(observer.observe(old_boundary, identity.clone(), 0, None));
            assert!(poll!(&mut old).is_pending());
            entered(&gate).await;
            f.store.revision.store(1, Ordering::SeqCst);
            assert_eq!(
                observer
                    .observe(new_boundary, identity.clone(), 0, None)
                    .await
                    .unwrap(),
                1
            );
            gate.release.notify_one();
            assert_eq!(old.await.unwrap(), 0);
            assert_eq!(
                observer
                    .observe(new_boundary, identity, 0, None)
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(f.store.revision_calls.load(Ordering::SeqCst), 3);
        }
    }
}

#[tokio::test]
async fn incompatible_epoch_publication_and_identity_do_not_share() {
    for changed in ["epoch", "publication", "identity"] {
        let (f, identity) = fixture().await;
        let observer = &f.coordinator.revision_observer;
        let other = if changed == "identity" {
            Arc::new(PreparedIdentity::of(&f.job(2, 0, "other").context.prepared))
        } else {
            identity.clone()
        };
        let mut first = Box::pin(observer.observe(Boundary::BuildEntry, identity, 0, None));
        let mut other = Box::pin(observer.observe(
            Boundary::BuildEntry,
            other,
            u64::from(changed == "epoch"),
            (changed == "publication").then(|| (hash(1), 1)),
        ));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut other).is_pending());
        first.await.unwrap();
        other.await.unwrap();
        assert_eq!(
            f.store.revision_calls.load(Ordering::SeqCst),
            2,
            "{changed}"
        );
    }
}

#[tokio::test]
async fn cohort_and_pending_bounds_survive_canceled_first_and_other_waiters() {
    let (f, identity) = fixture().await;
    let observer = &f.coordinator.revision_observer;
    let gate = gate(&f).await;
    let mut requests = Vec::new();
    for _ in 0..ADMITTED {
        // Manual polling establishes all enrollments before the worker runs;
        // the test setup must not stop at Tokio's cooperative task budget.
        let mut request = Box::pin(tokio::task::unconstrained(observer.observe(
            Boundary::BuildEntry,
            identity.clone(),
            0,
            None,
        )));
        assert!(poll!(&mut request).is_pending());
        requests.push(request);
    }
    assert_eq!(observer.slots.available_permits(), 0);
    entered(&gate).await;
    assert_eq!(f.store.revision_calls.load(Ordering::SeqCst), 1);
    // Cancel first enroller and another member after the closed query starts.
    drop(requests.remove(0));
    drop(requests.remove(7));
    let mut extra = Box::pin(observer.observe(Boundary::BuildEntry, identity, 0, None));
    assert!(poll!(&mut extra).is_pending());
    assert_eq!(observer.slots.available_permits(), 0);
    gate.release.notify_one();
    for result in join_all(requests).await {
        assert_eq!(result.unwrap(), 0);
    }
    assert_eq!(extra.await.unwrap(), 0);
    // 128 original enrollees split at 64; extra cannot join the first read.
    assert_eq!(f.store.revision_calls.load(Ordering::SeqCst), 3);
    assert_eq!(observer.slots.available_permits(), ADMITTED);
    assert_eq!(f.store.revision_canceled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn last_cancellation_ends_stuck_read_before_reclaiming_capacity() {
    let (f, identity) = fixture().await;
    let observer = &f.coordinator.revision_observer;
    let gate = gate(&f).await;
    let mut first = Box::pin(observer.observe(Boundary::BuildEntry, identity.clone(), 0, None));
    let mut peer = Box::pin(observer.observe(Boundary::BuildEntry, identity.clone(), 0, None));
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut peer).is_pending());
    entered(&gate).await;
    drop(first);
    drop(peer);
    assert_eq!(observer.slots.available_permits(), ADMITTED - 2);
    let mut pending = Box::pin(observer.observe(Boundary::BuildEntry, identity.clone(), 0, None));
    assert!(poll!(&mut pending).is_pending());
    drop(pending);
    // Never release the fake database gate. The final cancellation must end
    // this read instead of leaving a detached query and its capacity stranded.
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            observer.observe(Boundary::BuildEntry, identity, 0, None),
        )
        .await
        .expect("last canceled cohort stranded the next observation")
        .unwrap(),
        0
    );
    assert_eq!(f.store.revision_calls.load(Ordering::SeqCst), 2);
    assert_eq!(f.store.revision_canceled.load(Ordering::SeqCst), 1);
    assert_eq!(observer.slots.available_permits(), ADMITTED);
}

#[tokio::test]
async fn all_canceled_before_query_start_issue_no_observation() {
    let (f, identity) = fixture().await;
    let observer = &f.coordinator.revision_observer;
    let mut first = Box::pin(observer.observe(Boundary::BuildEntry, identity.clone(), 0, None));
    let mut peer = Box::pin(observer.observe(Boundary::BuildEntry, identity, 0, None));
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut peer).is_pending());
    drop(first);
    drop(peer);
    tokio::time::timeout(Duration::from_secs(5), async {
        while observer.slots.available_permits() != ADMITTED {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.store.revision_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shared_wait_revalidates_each_proof_and_preserves_database_first_errors() {
    for boundary in BOUNDARIES {
        for changed in [
            "unchanged",
            "publication",
            "epoch",
            "database_error",
            "lease",
            "expiry",
        ] {
            let (f, identity) = fixture().await;
            let mut a = proof(&f, &identity).await;
            let mut b = a.clone();
            f.store.revision_calls.store(0, Ordering::SeqCst);
            f.store.clock_calls.store(0, Ordering::SeqCst);
            let gate = gate(&f).await;
            let mut first = Box::pin(f.coordinator.revalidate_issuance_authority_at(
                &mut a,
                Some(101_000),
                Some(boundary),
            ));
            let mut peer = Box::pin(f.coordinator.revalidate_issuance_authority_at(
                &mut b,
                Some(101_000),
                Some(boundary),
            ));
            assert!(poll!(&mut first).is_pending());
            assert!(poll!(&mut peer).is_pending());
            entered(&gate).await;
            assert_eq!(f.store.revision_calls.load(Ordering::SeqCst), 1);
            assert_eq!(f.store.clock_calls.load(Ordering::SeqCst), 2);
            match changed {
                "publication" => f
                    .coordinator
                    .observed_tip
                    .write()
                    .await
                    .publish(&hash(1))
                    .unwrap(),
                "epoch" | "database_error" => {
                    f.coordinator.invalidate_readiness().await;
                    if changed == "database_error" {
                        f.store.fail_revision.store(true, Ordering::SeqCst);
                    }
                }
                "lease" => {
                    f.detect(2).await;
                    f.coordinator
                        .observed_tip
                        .write()
                        .await
                        .expire_lease_for_test(Duration::from_secs(121));
                }
                "expiry" => {
                    tokio::time::pause();
                    tokio::time::advance(Duration::from_secs(1)).await;
                }
                _ => {}
            }
            gate.release.notify_one();
            for result in [first.await, peer.await] {
                assert_eq!(
                    matches!(result, Ok(Some(_))),
                    changed == "unchanged",
                    "{changed}"
                );
                if changed == "database_error" {
                    let error = result.unwrap_err();
                    assert!(format!("{error:#}").contains("unavailable"));
                    assert!(!format!("{error:#}").contains("readiness"));
                }
            }
            if changed == "expiry" {
                tokio::time::resume();
            }
        }
    }
}

#[tokio::test]
async fn shared_failure_is_not_reused_by_recovery() {
    let (f, identity) = fixture().await;
    let observer = &f.coordinator.revision_observer;
    let gate = gate(&f).await;
    let mut first = Box::pin(observer.observe(Boundary::BuildEntry, identity.clone(), 0, None));
    let mut peer = Box::pin(observer.observe(Boundary::BuildEntry, identity.clone(), 0, None));
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut peer).is_pending());
    entered(&gate).await;
    f.store.fail_revision.store(true, Ordering::SeqCst);
    gate.release.notify_one();
    assert!(first
        .await
        .unwrap_err()
        .chain()
        .any(|error| error.to_string() == "unavailable"));
    assert!(peer.await.is_err());
    f.store.fail_revision.store(false, Ordering::SeqCst);
    f.store.revision.store(3, Ordering::SeqCst);
    assert_eq!(
        observer
            .observe(Boundary::BuildEntry, identity, 0, None)
            .await
            .unwrap(),
        3
    );
    assert_eq!(f.store.revision_calls.load(Ordering::SeqCst), 2);
}
