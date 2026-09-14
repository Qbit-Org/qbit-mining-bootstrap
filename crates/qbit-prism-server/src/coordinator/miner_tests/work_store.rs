use super::*;
use crate::ledger::{
    CompactPrepared, IssuedJobSave, PayoutState, PoolBlock, PreparedDependency, PreparedTemplate,
    StoredCompactPrepared,
};
use std::collections::VecDeque;

// Script typed observations rather than duplicate the database's blob codec.
// The fake records only public template identity, never private blob bytes.
#[derive(Default)]
pub(crate) struct CompactStore {
    pub reads: StdMutex<VecDeque<Result<Option<StoredCompactPrepared>>>>,
    pub read_keys: StdMutex<Vec<String>>,
    pub saves: StdMutex<VecDeque<Result<bool>>>,
    pub save_calls: StdMutex<Vec<CompactSave>>,
    pub states: StdMutex<VecDeque<Result<PayoutState, WindowError>>>,
    pub windows: StdMutex<VecDeque<Result<Window, WindowError>>>,
    pub window_calls: StdMutex<Vec<(WindowRef, BalanceSource)>>,
    pub window_gate: StdMutex<Option<Arc<Gate>>>,
    pub clock_gate: StdMutex<Option<Arc<Gate>>>,
    pub fail_clock: AtomicBool,
    pub(in crate::coordinator) drop_probe:
        StdMutex<Option<prepared_storage::compact::CompactDropProbe>>,
}

#[derive(Debug)]
pub(crate) struct CompactSave {
    pub key: String,
    pub record: CompactPrepared,
    pub template_sha256: String,
    pub balances: Vec<qbit_prism::CarryForwardBalance>,
    pub current_revision: i64,
    pub original_expires_at_ms: i64,
}

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
    fn compact_drop_probe(&self) -> Option<prepared_storage::compact::CompactDropProbe> {
        self.compact.drop_probe.lock().unwrap().take()
    }
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>> {
        submit_ledger::SubmitLedger::payout_revision(self)
    }
    fn payout_state(&self) -> BoxFuture<'_, Result<PayoutState, WindowError>> {
        Box::pin(async {
            self.compact
                .states
                .lock()
                .unwrap()
                .pop_front()
                .expect("script payout state")
        })
    }
    fn read_window_with_permit<'a>(
        &'a self,
        window: &'a WindowRef,
        balances: BalanceSource,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> BoxFuture<'a, Result<Window, WindowError>> {
        Box::pin(async move {
            let _permit = permit;
            self.compact
                .window_calls
                .lock()
                .unwrap()
                .push((*window, balances));
            let gate = self.compact.window_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            self.compact
                .windows
                .lock()
                .unwrap()
                .pop_front()
                .expect("script window")
        })
    }
    fn save_compact_prepared<'a>(
        &'a self,
        key: &'a str,
        record: &'a CompactPrepared,
        template: &'a PreparedTemplate,
        balances: &'a [qbit_prism::CarryForwardBalance],
        expected_current_revision: i64,
        expires_at_ms: i64,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            self.compact.save_calls.lock().unwrap().push(CompactSave {
                key: key.into(),
                record: record.clone(),
                template_sha256: template.sha256().into(),
                balances: balances.to_vec(),
                current_revision: expected_current_revision,
                original_expires_at_ms: expires_at_ms,
            });
            self.compact
                .saves
                .lock()
                .unwrap()
                .pop_front()
                .expect("script compact save")
        })
    }
    fn compact_prepared<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredCompactPrepared>>> {
        Box::pin(async move {
            self.compact.read_keys.lock().unwrap().push(key.into());
            self.compact
                .reads
                .lock()
                .unwrap()
                .pop_front()
                .expect("script compact read")
        })
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
        Box::pin(async {
            let gate = self.compact.clock_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            if self.compact.fail_clock.load(Ordering::SeqCst) {
                return Err(sqlx::Error::PoolClosed.into());
            }
            Ok(self.database_now())
        })
    }
}
