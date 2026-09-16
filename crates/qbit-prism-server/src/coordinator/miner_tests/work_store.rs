use super::*;
use crate::ledger::{
    CompactDependency, CompactPrepared, CompactRepair, IssuedJobSave, PayoutState, PoolBlock,
    PreparedTemplate, StoredCompactPrepared,
};
use std::collections::VecDeque;

// Script typed observations rather than duplicate the database's blob codec.
// The fake records only public template identity, never private blob bytes.
#[derive(Default)]
pub(crate) struct CompactStore {
    pub reads: StdMutex<VecDeque<Result<Option<StoredCompactPrepared>>>>,
    pub read_keys: StdMutex<Vec<String>>,
    pub metadata: StdMutex<HashMap<String, StoredCompactPrepared>>,
    pub saves: StdMutex<VecDeque<Result<bool>>>,
    pub save_calls: StdMutex<Vec<CompactSave>>,
    pub issued_saves: StdMutex<VecDeque<Result<IssuedJobSave>>>,
    pub issued_calls: StdMutex<Vec<CompactIssuedSave>>,
    pub states: StdMutex<VecDeque<Result<PayoutState, WindowError>>>,
    pub state_gate: StdMutex<Option<Arc<Gate>>>,
    pub state_calls: AtomicUsize,
    pub save_gate: StdMutex<Option<Arc<Gate>>>,
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

#[derive(Debug)]
pub(crate) struct CompactIssuedSave {
    pub id: String,
    pub payload: Value,
    pub current_revision: i64,
    pub parent: String,
    pub expires_at_ms: i64,
    pub key: String,
    pub original_revision: i64,
    pub original_expires_at_ms: i64,
    pub template_sha256: String,
    pub prior_balances_digest: [u8; 32],
    pub repair: bool,
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
            self.compact.state_calls.fetch_add(1, Ordering::SeqCst);
            let scripted = self.compact.states.lock().unwrap().pop_front();
            let result = scripted.unwrap_or_else(|| {
                if self.fail_revision.load(Ordering::SeqCst) {
                    return Err(WindowError::Database(sqlx::Error::PoolClosed));
                }
                let snapshot = self.snapshot.lock().unwrap();
                Ok(PayoutState {
                    payout_revision: self.revision.load(Ordering::SeqCst),
                    prior_balances_digest: qbit_prism::prior_balances_digest(
                        &snapshot.as_ref().expect("fixture snapshot").prior_balances,
                    ),
                })
            });
            let gate = self.compact.state_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            result
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
            if let Some(scripted) = self.compact.windows.lock().unwrap().pop_front() {
                return scripted;
            }
            let snapshot = self
                .snapshots
                .lock()
                .unwrap()
                .iter()
                .chain(
                    self.originals
                        .lock()
                        .unwrap()
                        .values()
                        .map(|stored| &*stored.snapshot),
                )
                .find(|snapshot| WindowRef::from_snapshot(snapshot).unwrap() == *window)
                .cloned()
                .expect("fixture retained the referenced original rows");
            Ok(Window {
                shares: snapshot.shares,
                prior_balances: snapshot.prior_balances,
                payout_revision: self.revision.load(Ordering::SeqCst),
            })
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
            let gate = self
                .compact
                .save_gate
                .lock()
                .unwrap()
                .take()
                .or_else(|| self.save_gate.lock().unwrap().take());
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            ensure!(
                !self.fail_save.load(Ordering::SeqCst),
                "controlled persistence failure"
            );
            ensure!(
                expected_current_revision == self.revision.load(Ordering::SeqCst),
                "revision changed"
            );
            let scripted = self.compact.saves.lock().unwrap().pop_front().transpose()?;
            let observed = StoredCompactPrepared {
                record: record.clone(),
                template: template.value_for_test()?,
                prior_balances: balances.to_vec(),
                original_expires_at_ms: expires_at_ms,
                expires_at_ms,
            };
            let mut payload = serde_json::to_value(record)?;
            payload["original_expires_at_ms"] = json!(expires_at_ms);
            let mut jobs = self.jobs.lock().unwrap();
            let inserted = !jobs.contains_key(key);
            if let Some(original) = jobs.get(key) {
                ensure!(
                    original.payload == payload
                        && original.revision == record.payout_revision
                        && original.parent == record.parent_hash,
                    "immutable compact prepared conflict"
                );
            } else {
                jobs.insert(
                    key.into(),
                    MemoryJob {
                        payload,
                        expires_at_ms,
                        revision: record.payout_revision,
                        parent: record.parent_hash.clone(),
                    },
                );
            }
            self.compact
                .metadata
                .lock()
                .unwrap()
                .entry(key.into())
                .or_insert(observed);
            Ok(scripted.unwrap_or(inserted))
        })
    }
    fn compact_prepared_with_admission<'a>(
        &'a self,
        key: &'a str,
        completion: crate::ledger::ReadAdmission,
    ) -> BoxFuture<'a, Result<Option<crate::ledger::BlockingDrop<StoredCompactPrepared>>>> {
        Box::pin(async move {
            Ok(self
                .compact_prepared(key)
                .await?
                .map(|stored| completion.own(stored)))
        })
    }
    fn compact_prepared<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredCompactPrepared>>> {
        Box::pin(async move {
            self.compact.read_keys.lock().unwrap().push(key.into());
            if let Some(scripted) = self.compact.reads.lock().unwrap().pop_front() {
                return scripted;
            }
            let jobs = self.jobs.lock().unwrap();
            let Some(job) = jobs
                .get(key)
                .filter(|job| job.expires_at_ms > self.database_now())
            else {
                return Ok(None);
            };
            if job.payload.get("format_version").is_none() && job.payload.get("snapshot").is_some()
            {
                return Ok(None);
            }
            let mut payload = job.payload.clone();
            let expiry = payload
                .as_object_mut()
                .context("compact object")?
                .remove("original_expires_at_ms")
                .and_then(|value| value.as_i64())
                .context("original expiry missing")?;
            let record: CompactPrepared = serde_json::from_value(payload.clone())?;
            ensure!(
                serde_json::to_value(&record)? == payload,
                "noncanonical compact prepared"
            );
            ensure!(
                record.payout_revision == job.revision && record.parent_hash == job.parent,
                "compact prepared payload/column mismatch"
            );
            let metadata = self.compact.metadata.lock().unwrap();
            let metadata = metadata.get(key).context("fixture compact blobs missing")?;
            let template = PreparedTemplate::encode(&metadata.template)?;
            // Reuse the actual typed codec for validation, not a fake codec.
            CompactRepair::encode(&record, &template, &metadata.prior_balances, expiry)?;
            Ok(Some(StoredCompactPrepared {
                record,
                template: metadata.template.clone(),
                prior_balances: metadata.prior_balances.clone(),
                original_expires_at_ms: expiry,
                expires_at_ms: job.expires_at_ms,
            }))
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
    fn observe_chain_view_at_revision<'a>(
        &'a self,
        tip: &'a str,
        height: u64,
        work: &'a str,
        expected_revision: i64,
    ) -> BoxFuture<'a, Result<i64>> {
        Box::pin(async move {
            ensure!(
                self.revision.load(Ordering::SeqCst) == expected_revision,
                "chain observation revision changed"
            );
            self.observe_chain_view(tip, height, work).await
        })
    }
    fn snapshot_with_admission(
        &self,
        _network: u128,
        completion: crate::ledger::ReadAdmission,
    ) -> BoxFuture<'_, Result<crate::ledger::BlockingDrop<Snapshot>>> {
        Box::pin(async move {
            let mut snapshot = self.snapshot.lock().unwrap().clone().unwrap();
            snapshot.payout_revision = self.revision.load(Ordering::SeqCst);
            self.snapshots.lock().unwrap().push(snapshot.clone());
            Ok(completion.own(snapshot))
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
    fn save_issued_job_compact<'a>(
        &'a self,
        id: &'a str,
        payload: &'a Value,
        revision: i64,
        parent: &'a str,
        expires_at_ms: i64,
        dependency: CompactDependency<'a>,
        repair: Option<&'a CompactRepair>,
    ) -> BoxFuture<'a, Result<IssuedJobSave>> {
        Box::pin(async move {
            self.compact
                .issued_calls
                .lock()
                .unwrap()
                .push(CompactIssuedSave {
                    id: id.into(),
                    payload: payload.clone(),
                    current_revision: revision,
                    parent: parent.into(),
                    expires_at_ms,
                    key: dependency.key.into(),
                    original_revision: dependency.original_revision,
                    original_expires_at_ms: dependency.original_expires_at_ms,
                    template_sha256: dependency.template_sha256.into(),
                    prior_balances_digest: dependency.prior_balances_digest,
                    repair: repair.is_some(),
                });
            let gate = self.save_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            if let Some(scripted) = self.compact.issued_saves.lock().unwrap().pop_front() {
                match scripted? {
                    IssuedJobSave::PreparedMissing => return Ok(IssuedJobSave::PreparedMissing),
                    IssuedJobSave::Saved => {}
                }
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
            let repaired = repair
                .map(CompactRepair::observation_for_test)
                .transpose()?;
            let mut repair_payload = repaired
                .as_ref()
                .map(|stored| serde_json::to_value(&stored.record))
                .transpose()?;
            if let (Some(payload), Some(stored)) = (&mut repair_payload, &repaired) {
                payload["original_expires_at_ms"] = json!(stored.original_expires_at_ms);
                ensure!(
                    stored.record.payout_revision == dependency.original_revision
                        && stored.record.parent_hash == dependency.parent
                        && stored.original_expires_at_ms == dependency.original_expires_at_ms
                        && stored.record.template_sha256 == dependency.template_sha256
                        && stored.record.window.prior_balances_digest
                            == dependency.prior_balances_digest,
                    "prepared repair identity mismatch"
                );
            }
            if let Some(original) = jobs.get(dependency.key) {
                ensure!(
                    original.revision == dependency.original_revision
                        && original.parent == dependency.parent
                        && original.payload["original_expires_at_ms"]
                            == dependency.original_expires_at_ms
                        && original.payload["template_sha256"] == dependency.template_sha256
                        && original.payload["window"]["prior_balances_digest"]
                            == hex::encode(dependency.prior_balances_digest)
                        && repair_payload
                            .as_ref()
                            .is_none_or(|repair| *repair == original.payload),
                    "immutable prepared dependency conflict"
                );
            } else {
                let Some(repaired) = repaired else {
                    return Ok(IssuedJobSave::PreparedMissing);
                };
                jobs.insert(
                    dependency.key.into(),
                    MemoryJob {
                        payload: repair_payload.unwrap(),
                        expires_at_ms: repaired.original_expires_at_ms,
                        revision: repaired.record.payout_revision,
                        parent: repaired.record.parent_hash.clone(),
                    },
                );
                self.compact
                    .metadata
                    .lock()
                    .unwrap()
                    .insert(dependency.key.into(), repaired);
            }
            let original = jobs.get_mut(dependency.key).unwrap();
            original.expires_at_ms = original.expires_at_ms.max(expires_at_ms + 60_000);
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
