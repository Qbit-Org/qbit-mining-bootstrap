//! A repeated local preference is not a new fork-choice transition.
use super::work_ledger::WorkLedger;
use anyhow::Result;

#[derive(Default)]
pub(super) struct ChainObservation {
    tip: Option<String>,
    retry_from: Option<String>,
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
        revision: i64,
    ) -> Result<i64> {
        if self
            .tip
            .as_deref()
            .is_none_or(|previous| !previous.eq_ignore_ascii_case(tip))
        {
            self.retry_from = self.tip.replace(tip.to_ascii_lowercase());
        }
        // Consume before any possibly committing I/O. Cancellation, lost COMMIT
        // replies and later publication failures leave the new local tip recorded
        // and cannot recreate a witness on the next unchanged poll.
        let predecessor = self.retry_from.take();
        let result = match predecessor.as_deref() {
            Some(from) => {
                ledger
                    .observe_chain_transition(from, tip, height, work, revision)
                    .await
            }
            None => ledger.observe_chain_view(tip, height, work).await,
        };
        // Only this typed, pre-write refusal proves there was no tip mutation
        // and that the predecessor was still accepted. A retry must pass through
        // refresh's fresh RPC proof and revision capture; never replay this one.
        if result
            .as_ref()
            .is_err_and(|error| error.is::<crate::ledger::ChainObservationRetry>())
        {
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
        *fixture.store.tip.lock().unwrap() = Some(hash(1));
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
