use super::*;
use crate::ledger::{IssuedJobSave, PoolBlock, PreparedDependency};

pub(crate) struct MemoryJob {
    pub payload: Value,
    pub expires_at_ms: i64,
    pub revision: i64,
    pub parent: String,
}

impl MemoryLedger {
    pub fn database_now(&self) -> i64 {
        100_000 + self.clock_offset_ms.load(Ordering::SeqCst)
    }
}

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
        parent: &'a str,
        ttl: i64,
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
            self.jobs
                .lock()
                .unwrap()
                .entry(id.into())
                .or_insert(MemoryJob {
                    payload: payload.clone(),
                    expires_at_ms: self.database_now() + ttl * 1000,
                    revision,
                    parent: parent.into(),
                });
            Ok(())
        })
    }
    fn save_issued_job<'a>(
        &'a self,
        id: &'a str,
        payload: &'a Value,
        revision: i64,
        parent: &'a str,
        expires_at_ms: i64,
        dependency: PreparedDependency<'a>,
        repair: Option<&'a Value>,
    ) -> BoxFuture<'a, Result<IssuedJobSave>> {
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
            ensure!(
                expires_at_ms > self.database_now(),
                "issued job deadline elapsed"
            );
            ensure!(
                payload["prepared_key"] == dependency.key
                    && payload["expires_at_ms"] == expires_at_ms
                    && parent == dependency.parent,
                "issued job dependency or deadline mismatch"
            );
            let mut jobs = self.jobs.lock().unwrap();
            if let Some(child) = jobs.get(id) {
                ensure!(
                    child.payload == *payload
                        && child.expires_at_ms == expires_at_ms
                        && child.parent == parent
                        && child.revision == revision,
                    "immutable job ID conflict"
                );
            }
            if let Some(original) = jobs.get(dependency.key) {
                ensure!(
                    original.revision == dependency.original_revision
                        && original.parent == dependency.parent
                        && repair.is_none_or(|repair| *repair == original.payload),
                    "immutable prepared dependency conflict"
                );
            } else {
                let Some(repair) = repair else {
                    return Ok(IssuedJobSave::PreparedMissing);
                };
                ensure!(
                    repair["snapshot"]["payout_revision"] == dependency.original_revision
                        && repair["template"]["previousblockhash"] == dependency.parent,
                    "prepared repair identity mismatch"
                );
                jobs.insert(
                    dependency.key.into(),
                    MemoryJob {
                        payload: repair.clone(),
                        expires_at_ms: expires_at_ms + 60_000,
                        revision: dependency.original_revision,
                        parent: dependency.parent.into(),
                    },
                );
            }
            let original = jobs.get_mut(dependency.key).unwrap();
            if original.expires_at_ms < expires_at_ms {
                original.expires_at_ms = expires_at_ms + 60_000;
            }
            jobs.entry(id.into()).or_insert(MemoryJob {
                payload: payload.clone(),
                expires_at_ms,
                revision,
                parent: parent.into(),
            });
            Ok(IssuedJobSave::Saved)
        })
    }
    fn job<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<Value>>> {
        Box::pin(async move {
            Ok(self
                .jobs
                .lock()
                .unwrap()
                .get(id)
                .filter(|job| job.expires_at_ms > self.database_now())
                .map(|job| job.payload.clone()))
        })
    }
    fn now_ms(&self) -> BoxFuture<'_, Result<i64>> {
        Box::pin(async { Ok(self.database_now()) })
    }
}
