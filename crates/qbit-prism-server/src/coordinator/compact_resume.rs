//! Bounded, waiter-owned reconstruction. No detached task gets a new deadline.
use super::*;
use crate::ledger::{BlockingDrop, PreparedTemplate, ReadAdmission, StoredCompactPrepared};
use futures_util::future::{BoxFuture, Shared};
use futures_util::FutureExt;
use prepared_storage::compact::{
    assemble_captured, audit_parts_sha256, canonical_json_sha256, CompactOwner,
};
use std::sync::{Mutex as StdMutex, Weak};

#[derive(Clone, Debug)]
pub(super) struct SharedFailure(Arc<anyhow::Error>);
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:#}", self.0)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}
type Metadata = Shared<
    BoxFuture<'static, Result<Option<Arc<BlockingDrop<StoredCompactPrepared>>>, SharedFailure>>,
>;
type Rebuild = Shared<BoxFuture<'static, Result<Arc<Prepared>, SharedFailure>>>;

pub(super) struct ResumeFlight {
    pub metadata: Metadata,
    rebuild: StdMutex<Option<Rebuild>>,
    _admission: ReadAdmission,
    changed: Arc<Notify>,
}
impl Drop for ResumeFlight {
    fn drop(&mut self) {
        self.changed.notify_waiters();
    }
}

pub(super) struct ResumeFlights {
    entries: StdMutex<HashMap<(String, usize), Weak<ResumeFlight>>>,
    slots: Arc<Semaphore>,
    changed: Arc<Notify>,
}
impl ResumeFlights {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: StdMutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(capacity)),
            changed: Arc::new(Notify::new()),
        }
    }

    pub async fn join(
        &self,
        coordinator: &Coordinator,
        key: &str,
        extra_size: usize,
    ) -> Arc<ResumeFlight> {
        let key = (key.to_owned(), extra_size);
        loop {
            // Register before inspecting capacity so a last waiter leaving
            // cannot race between the inspection and our wait.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut entries = self.entries.lock().unwrap();
                entries.retain(|_, entry| entry.strong_count() != 0);
                if let Some(flight) = entries.get(&key).and_then(Weak::upgrade) {
                    return flight;
                }
                if let Ok(slot) = self.slots.clone().try_acquire_owned() {
                    let admission = ReadAdmission::notifying(slot, self.changed.clone());
                    let decoder_admission = admission.clone();
                    let ledger = coordinator.work_ledger.clone();
                    let config = coordinator.config.clone();
                    let lookup = key.0.clone();
                    let metadata = async move {
                        let result = async {
                            let Some(stored) = ledger
                                .compact_prepared_with_admission(&lookup, decoder_admission)
                                .await?
                            else {
                                return Ok(None);
                            };
                            if stored.record.audit_builder_version
                                != qbit_prism::AUDIT_BUILDER_VERSION
                                || stored.record.signer_keys != local_signer_keys(&config)?
                            {
                                return Ok(None);
                            }
                            Ok::<_, anyhow::Error>(Some(Arc::new(stored)))
                        }
                        .await;
                        result.map_err(|error| SharedFailure(Arc::new(error)))
                    }
                    .boxed()
                    .shared();
                    let flight = Arc::new(ResumeFlight {
                        metadata,
                        rebuild: StdMutex::new(None),
                        _admission: admission,
                        changed: self.changed.clone(),
                    });
                    entries.insert(key.clone(), Arc::downgrade(&flight));
                    self.changed.notify_waiters();
                    return flight;
                }
            }
            changed.await;
        }
    }
}

impl ResumeFlight {
    pub fn reconstruction(
        &self,
        coordinator: &Coordinator,
        key: String,
        metadata: Arc<BlockingDrop<StoredCompactPrepared>>,
        extra_size: usize,
    ) -> Rebuild {
        let mut shared = self.rebuild.lock().unwrap();
        shared
            .get_or_insert_with(|| {
                let ledger = coordinator.work_ledger.clone();
                let build_slots = coordinator.build_slots.clone();
                let window_reads = coordinator.window_reads.clone();
                let config = coordinator.config.clone();
                async move {
                    reconstruct(
                        ledger,
                        build_slots,
                        window_reads,
                        config,
                        key,
                        metadata,
                        extra_size,
                    )
                    .await
                    .map_err(|error| SharedFailure(Arc::new(error)))
                }
                .boxed()
                .shared()
            })
            .clone()
    }
}

async fn reconstruct(
    ledger: Arc<dyn work_ledger::WorkLedger>,
    build_slots: Arc<Semaphore>,
    window_reads: Arc<Semaphore>,
    config: Arc<Config>,
    key: String,
    metadata: Arc<BlockingDrop<StoredCompactPrepared>>,
    extra_size: usize,
) -> Result<Arc<Prepared>> {
    let permit = build_slots.acquire_owned().await?;
    let owned = CompactOwner::new((metadata, permit));
    let window = if owned.0.record.window.shares.is_some() {
        let reader = window_reads.acquire_owned().await?;
        Some(
            ledger
                .read_window_with_permit(&owned.0.record.window, BalanceSource::AsIssued, reader)
                .await?,
        )
    } else {
        None
    };
    let (metadata, permit) = owned.into_inner();
    let owned = CompactOwner::new((metadata, window, permit));
    #[cfg(test)]
    let drop_probe = ledger.compact_drop_probe();
    let result = owned
        .spawn_blocking(move |(source_metadata, source_window, permit)| {
            let admission = permit;
            #[cfg(test)]
            let _cleanup = drop_probe;
            let metadata = source_metadata;
            let window = source_window;
            let snapshot = Snapshot {
                anchor_ms: metadata.record.window.anchor_ms,
                share_seq: metadata.record.share_seq,
                payout_revision: metadata.record.payout_revision,
                shares: window.map_or_else(Vec::new, |window| window.shares),
                prior_balances: metadata.prior_balances.clone(),
            };
            let inputs = BundleInputs::from(&metadata.record);
            let body = if metadata.record.window.shares.is_some() {
                Some(
                    bundle_build::build_body(
                        &config,
                        &snapshot,
                        &metadata.template,
                        None,
                        metadata.record.coinbase_suffix_hex.clone(),
                        inputs,
                    )?
                    .0,
                )
            } else {
                None
            };
            if let Some(body) = &body {
                let expected = metadata
                    .record
                    .audit_hashes
                    .as_ref()
                    .context("prepared original hashes missing")?;
                ensure!(
                    audit_parts_sha256(body, &snapshot.shares)? == expected.audit_bundle_sha256,
                    "reconstructed prepared audit hash mismatch"
                );
                ensure!(
                    canonical_json_sha256(&body.signed_coinbase_manifest.manifest)?
                        == expected.coinbase_manifest_sha256,
                    "reconstructed prepared coinbase hash mismatch"
                );
            }
            let wire = body
                .as_ref()
                .map(|body| {
                    codec::Job::from_manifest(
                        "shared".into(),
                        &metadata.template,
                        &body.signed_coinbase_manifest.manifest,
                        "00000000",
                        extra_size,
                        1.0,
                        0.0,
                        true,
                    )
                })
                .transpose()?;
            let encoded = PreparedTemplate::encode(&metadata.template)?;
            let captured = assemble_captured(
                key,
                metadata.record.clone(),
                encoded,
                metadata.template.clone(),
                snapshot.prior_balances.clone(),
                body.as_ref().map(PreparedBundle::from),
                wire,
                metadata.original_expires_at_ms,
                Instant::now(),
                None,
            )?;
            drop(body);
            drop(snapshot);
            Ok::<_, anyhow::Error>(CompactOwner::new((captured.original, admission)))
        })
        .await??;
    let (prepared, admission) = result.into_inner();
    drop(admission);
    Ok(prepared)
}
