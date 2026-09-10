use super::*;
use crate::ledger::PoolBlock;

impl work_ledger::WorkLedger for MemoryLedger {
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>> {
        submit_ledger::SubmitLedger::payout_revision(self)
    }
    fn observe_chain_view<'a>(
        &'a self,
        tip: &'a str,
        _height: u64,
        _work: &'a str,
    ) -> BoxFuture<'a, Result<i64>> {
        Box::pin(async move {
            let mut previous = self.tip.lock().unwrap();
            if previous.as_deref().is_some_and(|old| old != tip) {
                self.revision.fetch_add(1, Ordering::SeqCst);
            }
            *previous = Some(tip.into());
            Ok(self.revision.load(Ordering::SeqCst))
        })
    }
    fn snapshot(&self, _network: u128) -> BoxFuture<'_, Result<Snapshot>> {
        Box::pin(async move {
            let mut snapshot = self.snapshot.lock().unwrap().clone().unwrap();
            snapshot.payout_revision = self.revision.load(Ordering::SeqCst);
            Ok(snapshot)
        })
    }
    fn pool_blocks(&self) -> BoxFuture<'_, Result<Vec<PoolBlock>>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn reconcile<'a>(
        &'a self,
        _observations: &'a [BlockObservation],
        _height: u64,
        revision: i64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            ensure!(
                revision == self.revision.load(Ordering::SeqCst),
                "revision changed"
            );
            Ok(())
        })
    }
    fn save_job<'a>(
        &'a self,
        id: &'a str,
        payload: &'a Value,
        revision: i64,
        _parent: &'a str,
        _ttl: i64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let gate = self.save_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            ensure!(
                !self.fail_save.load(Ordering::SeqCst),
                "controlled persistence failure"
            );
            ensure!(
                revision == self.revision.load(Ordering::SeqCst),
                "revision changed"
            );
            self.jobs.lock().unwrap().insert(id.into(), payload.clone());
            Ok(())
        })
    }
    fn job<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<Value>>> {
        Box::pin(async move { Ok(self.jobs.lock().unwrap().get(id).cloned()) })
    }
    fn now_ms(&self) -> BoxFuture<'_, Result<i64>> {
        Box::pin(async { Ok(100_000) })
    }
}
