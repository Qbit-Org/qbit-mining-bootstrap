//! A repeated local preference is not a new fork-choice transition.
use super::work_ledger::WorkLedger;
use crate::ledger::{ChainObservationState, ChainTransition};
use anyhow::Result;

#[derive(Default)]
pub(super) struct ChainObservation {
    tip: Option<String>,
    retry_from: Option<ChainTransition>,
}

impl ChainObservation {
    /// Called only after a fresh coherent node proof, while the refresh mutex
    /// serializes this observer. Cold starts use the strict path: a conflicting
    /// equal-work preference alone cannot establish a transition predecessor.
    pub(super) async fn observe(
        &mut self,
        ledger: &dyn WorkLedger,
        tip: &str,
        height: u64,
        work: &str,
        observed: &ChainObservationState,
    ) -> Result<i64> {
        let previous_tip = self.tip.clone();
        if self
            .tip
            .as_deref()
            .is_none_or(|previous| !previous.eq_ignore_ascii_case(tip))
        {
            self.retry_from = self
                .tip
                .replace(tip.to_ascii_lowercase())
                .and_then(|predecessor| {
                    (observed.best_tip_hash.as_deref() == Some(predecessor.as_str())).then_some(
                        ChainTransition {
                            predecessor,
                            origin_chain_epoch: observed.chain_epoch,
                        },
                    )
                });
        }
        // Consume before any possibly committing I/O. Cancellation, lost COMMIT
        // replies and later publication failures leave the new local tip recorded
        // and cannot recreate a witness on the next unchanged poll.
        let predecessor = self.retry_from.take();
        let result = match predecessor.as_ref() {
            Some(from) => {
                ledger
                    .observe_chain_transition(from, tip, height, work, observed)
                    .await
            }
            None => ledger.observe_chain_view(tip, height, work).await,
        };
        // Reconsideration can pass through ancestors below the durable work
        // checkpoint. A definite pre-write refusal must not replace the local
        // predecessor with such an ineligible tip. Restore only the prior tip:
        // this attempt still consumes any pending retry, just as before. A
        // lower-work excursion must not extend that retry across a peer's ABA.
        // Cancellation or an unknown outcome never reaches this branch.
        if result
            .as_ref()
            .is_err_and(|error| error.is::<crate::ledger::ChainObservationBehind>())
        {
            self.tip = previous_tip;
        } else if result
            .as_ref()
            .is_err_and(|error| error.is::<crate::ledger::ChainObservationRetry>())
        {
            // This pre-write revision refusal retains the existing witness.
            // A retry still needs a fresh coherent proof and revision capture.
            self.retry_from = predecessor;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::coordinator::miner_tests::{hash, FailCommit, Fixture, Gate};
    use std::{sync::atomic::Ordering, sync::Arc, time::Duration};
    use tokio::time::timeout;

    const BOUND: Duration = Duration::from_secs(2);

    async fn baseline() -> Fixture {
        let fixture = Fixture::new(Duration::from_secs(10)).await;
        fixture.coordinator.refresh_once().await.unwrap();
        fixture.node.lock().unwrap().tip = hash(2);
        fixture
    }

    async fn unchanged_cannot_replay(fixture: &Fixture) {
        // Another observer genuinely returned the cluster to the original tip.
        // Local node 2 did not transition again, so it has no replacement proof.
        {
            let mut tip = fixture.store.tip.lock().unwrap();
            *tip = Some(hash(1));
            fixture.store.chain_epoch.fetch_add(1, Ordering::SeqCst);
        }
        let revision = fixture.store.revision.fetch_add(1, Ordering::SeqCst) + 1;
        for _ in 0..2 {
            let error = fixture.coordinator.refresh_once().await.unwrap_err();
            assert!(error.to_string().contains("conflicting equal-work"));
        }
        assert_eq!(
            fixture
                .store
                .compact
                .transition_calls
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(fixture.store.revision.load(Ordering::SeqCst), revision);
    }

    async fn lower_work_cannot_recreate_a_consumed_transition(fixture: &Fixture) {
        let calls = fixture
            .store
            .compact
            .transition_calls
            .load(Ordering::SeqCst);
        let revision = fixture.store.revision.load(Ordering::SeqCst);
        fixture.node.lock().unwrap().tip = hash(3);
        fixture
            .store
            .compact
            .observation_behind
            .store(true, Ordering::SeqCst);
        let error = fixture.coordinator.refresh_once().await.unwrap_err();
        assert!(error.is::<crate::ledger::ChainObservationBehind>());
        fixture.node.lock().unwrap().tip = hash(2);
        for _ in 0..2 {
            let error = fixture.coordinator.refresh_once().await.unwrap_err();
            assert!(error.to_string().contains("conflicting equal-work"));
        }
        assert_eq!(
            fixture
                .store
                .compact
                .transition_calls
                .load(Ordering::SeqCst),
            calls,
            "the lower-work refusal recreated a consumed transition"
        );
        assert_eq!(fixture.store.revision.load(Ordering::SeqCst), revision);
    }

    #[tokio::test]
    async fn unknown_observation_commit_never_recreates_a_transition() {
        for outcome in [FailCommit::NotRecorded, FailCommit::Recorded] {
            let fixture = baseline().await;
            *fixture.store.compact.observation_unknown.lock().unwrap() = Some(outcome);
            let error = fixture.coordinator.refresh_once().await.unwrap_err();
            assert!(error
                .to_string()
                .contains("lost chain observation COMMIT reply"));
            assert_eq!(
                fixture.store.tip.lock().unwrap().as_deref(),
                Some(
                    hash(if matches!(outcome, FailCommit::Recorded) {
                        2
                    } else {
                        1
                    })
                    .as_str()
                )
            );
            if matches!(outcome, FailCommit::Recorded) {
                // Reconcile an actually committed, unknown result as a strict
                // same-tip no-op, without creating another transition call.
                fixture.coordinator.refresh_once().await.unwrap();
                assert_eq!(
                    fixture
                        .store
                        .compact
                        .transition_calls
                        .load(Ordering::SeqCst),
                    1
                );
            }
            unchanged_cannot_replay(&fixture).await;
            lower_work_cannot_recreate_a_consumed_transition(&fixture).await;
        }
    }

    #[tokio::test]
    async fn cancelled_observation_before_or_after_commit_never_recreates_a_transition() {
        for committed in [false, true] {
            let fixture = baseline().await;
            let gate = Arc::new(Gate::default());
            let boundary = if committed {
                &fixture.store.compact.observation_after
            } else {
                &fixture.store.compact.observation_before
            };
            *boundary.lock().unwrap() = Some(gate.clone());
            let coordinator = fixture.coordinator.clone();
            let task = tokio::spawn(async move { coordinator.refresh_once().await });
            timeout(BOUND, gate.entered.notified()).await.unwrap();
            assert_eq!(
                fixture.store.tip.lock().unwrap().as_deref(),
                Some(hash(if committed { 2 } else { 1 }).as_str())
            );
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            unchanged_cannot_replay(&fixture).await;
            lower_work_cannot_recreate_a_consumed_transition(&fixture).await;
        }
    }

    #[tokio::test]
    async fn failed_publication_never_recreates_the_committed_transition() {
        let fixture = baseline().await;
        fixture.store.fail_save.store(true, Ordering::SeqCst);
        assert!(fixture.coordinator.refresh_once().await.is_err());
        assert_eq!(
            fixture.store.tip.lock().unwrap().as_deref(),
            Some(hash(2).as_str())
        );
        fixture.store.fail_save.store(false, Ordering::SeqCst);
        unchanged_cannot_replay(&fixture).await;
        lower_work_cannot_recreate_a_consumed_transition(&fixture).await;
    }

    #[tokio::test]
    async fn cancelled_lower_work_observation_does_not_restore_the_previous_tip() {
        let fixture = baseline().await;
        let gate = Arc::new(Gate::default());
        *fixture.store.compact.observation_before.lock().unwrap() = Some(gate.clone());
        fixture
            .store
            .compact
            .observation_behind
            .store(true, Ordering::SeqCst);
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move { coordinator.refresh_once().await });
        timeout(BOUND, gate.entered.notified()).await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        unchanged_cannot_replay(&fixture).await;
        lower_work_cannot_recreate_a_consumed_transition(&fixture).await;
    }

    #[tokio::test]
    async fn lower_work_consumes_an_already_eligible_accounting_retry() {
        let fixture = baseline().await;
        let gate = Arc::new(Gate::default());
        fixture.node.lock().unwrap().gate = Some(("getblockheader".into(), gate.clone()));
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move { coordinator.refresh_once().await });
        timeout(BOUND, gate.entered.notified()).await.unwrap();
        fixture.store.revision.fetch_add(1, Ordering::SeqCst);
        gate.release.notify_one();
        let error = timeout(BOUND, task).await.unwrap().unwrap().unwrap_err();
        assert!(error.is::<crate::ledger::ChainObservationRetry>());
        for _ in 0..2 {
            fixture.node.lock().unwrap().tip = hash(3);
            fixture
                .store
                .compact
                .observation_behind
                .store(true, Ordering::SeqCst);
            let error = fixture.coordinator.refresh_once().await.unwrap_err();
            assert!(error.is::<crate::ledger::ChainObservationBehind>());
            assert_eq!(fixture.store.revision.load(Ordering::SeqCst), 1);
        }
        fixture.node.lock().unwrap().tip = hash(2);
        let error = timeout(BOUND, fixture.coordinator.refresh_once())
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("conflicting equal-work"));
        assert_eq!(fixture.store.revision.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .store
                .compact
                .transition_calls
                .load(Ordering::SeqCst),
            1,
            "a lower-work observation must consume the pending retry"
        );
    }

    #[tokio::test]
    async fn definite_prewrite_refusal_requires_a_new_coherent_proof_to_retry() {
        let fixture = baseline().await;
        let gate = Arc::new(Gate::default());
        fixture.node.lock().unwrap().gate = Some(("getblockheader".into(), gate.clone()));
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move { coordinator.refresh_once().await });
        timeout(BOUND, gate.entered.notified()).await.unwrap();
        fixture.store.revision.fetch_add(1, Ordering::SeqCst);
        gate.release.notify_one();
        let error = timeout(BOUND, task).await.unwrap().unwrap().unwrap_err();
        assert!(error.is::<crate::ledger::ChainObservationRetry>());
        assert_eq!(
            fixture.store.tip.lock().unwrap().as_deref(),
            Some(hash(1).as_str())
        );
        let fresh = Arc::new(Gate::default());
        fixture.node.lock().unwrap().gate = Some(("getblocktemplate".into(), fresh.clone()));
        let coordinator = fixture.coordinator.clone();
        let retry = tokio::spawn(async move { coordinator.refresh_once().await });
        timeout(BOUND, fresh.entered.notified()).await.unwrap();
        assert_eq!(
            fixture
                .store
                .compact
                .transition_calls
                .load(Ordering::SeqCst),
            1
        );
        fresh.release.notify_one();
        timeout(BOUND, retry).await.unwrap().unwrap().unwrap();
        assert_eq!(
            fixture
                .store
                .compact
                .transition_calls
                .load(Ordering::SeqCst),
            2
        );
        assert_eq!(fixture.store.revision.load(Ordering::SeqCst), 2);
    }
}
