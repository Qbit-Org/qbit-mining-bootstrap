//! Legacy capture/hydration fixtures retained to exercise the original contracts.
//! Runtime callers use capture_refresh and ResumeFlights instead.
use super::*;

impl CompactPrepared {
    #[cfg(test)]
    pub(in crate::coordinator) fn from_original_build(
        stored: &StoredPrepared,
        window: WindowRef,
        inputs: &BundleInputs,
        template: &PreparedTemplate,
        audit_hashes: Option<PreparedAuditHashes>,
    ) -> Result<Self> {
        Ok(Self {
            format_version: Self::FORMAT_VERSION,
            window,
            share_seq: stored.snapshot.share_seq,
            payout_revision: stored.snapshot.payout_revision,
            template_sha256: template.sha256().into(),
            parent_hash: stored.template["previousblockhash"]
                .as_str()
                .context("prepared parent missing")?
                .into(),
            parent_of_tip: stored.parent_of_tip.clone(),
            fingerprint: stored.fingerprint.clone(),
            generation: stored.generation,
            coinbase_suffix_hex: stored.coinbase_suffix.clone(),
            payout_policy: inputs.payout_policy.clone(),
            ctv: inputs.ctv.clone(),
            fee: stored.fee,
            audit_builder_version: inputs.audit_builder_version,
            signer_keys: inputs.signer_keys.clone(),
            audit_hashes,
        })
    }
}

#[cfg(test)]
#[derive(Debug, thiserror::Error)]
pub(in crate::coordinator) enum IncompatibleCompactBuild {
    #[error("original compact window reference differs from snapshot bounds")]
    WindowSnapshotMismatch,
    #[error("original compact build balances are not in canonical order")]
    NonCanonicalBalances,
    #[error("original bundle balances differ from compact build inputs")]
    BundleBalancesMismatch,
}

/// Explicit handoff from the original builder, before either persistence
/// format reserves the key. Resumed work now carries historical inputs, but its
/// key is already reserved and cannot authorize this unpublished-build handoff.
/// Keep the builder's nonoptional inputs beside its durable representation,
/// just as Prepared does; neither path substitutes current configuration.
#[cfg(test)]
pub(in crate::coordinator) struct OriginalPreparedBuild {
    storage_key: String,
    stored: Arc<StoredPrepared>,
    window: WindowRef,
    inputs: BundleInputs,
    created: Instant,
    build_proof: Option<CompactBuildProof>,
    #[cfg(test)]
    capture_probe: Option<Arc<RepairProbe>>,
    #[cfg(test)]
    drop_probe: Option<CompactDropProbe>,
}

#[cfg(test)]
impl OriginalPreparedBuild {
    /// Call on the coordinator runtime with the exact locals used by the
    /// original build, including its suffix and inputs, never current config.
    /// The owner also protects cancellation while waiting for admission.
    pub fn from_original_build(
        storage_key: String,
        stored: Arc<StoredPrepared>,
        window: WindowRef,
        inputs: BundleInputs,
    ) -> CompactOwner<Self> {
        CompactOwner::new(Self {
            storage_key,
            stored,
            window,
            inputs,
            created: Instant::now(),
            build_proof: None,
            #[cfg(test)]
            capture_probe: None,
            #[cfg(test)]
            drop_probe: None,
        })
    }

    /// The proof was obtained before these original build inputs were used.
    /// Consume it here; never substitute a post-build readiness generation.
    pub fn from_proven_original_build(
        proof: CompactBuildProof,
        storage_key: String,
        stored: Arc<StoredPrepared>,
        window: WindowRef,
        inputs: BundleInputs,
    ) -> CompactOwner<Self> {
        let mut source = Self::from_original_build(storage_key, stored, window, inputs);
        source.value.as_mut().unwrap().build_proof = Some(proof);
        source
    }

    #[cfg(test)]
    pub fn with_capture_probe(
        mut source: CompactOwner<Self>,
        probe: Arc<RepairProbe>,
    ) -> CompactOwner<Self> {
        source.value.as_mut().unwrap().capture_probe = Some(probe);
        source
    }

    #[cfg(test)]
    pub fn with_drop_probe(
        mut source: CompactOwner<Self>,
        probe: CompactDropProbe,
    ) -> CompactOwner<Self> {
        source.value.as_mut().unwrap().drop_probe = Some(probe);
        source
    }
}

/// Original reconstruction inputs and their admission, never a published job.
/// A later builder must move this permit into its blocking owner, not call
/// build_bundle (which acquires another slot). Compare the rebuilt hashes with
/// record.audit_hashes, then revalidate authority and the issued deadline.
#[cfg(test)]
pub(in crate::coordinator) type AdmittedCompactInputs = CompactOwner<CompactInputs>;

#[cfg(test)]
pub(in crate::coordinator) struct CompactInputs {
    pub record: CompactPrepared,
    pub template: Value,
    pub snapshot: Arc<Snapshot>,
    pub inputs: BundleInputs,
    pub original_expires_at_ms: i64,
    pub retained_until_ms: i64,
    pub issued_expires_at_ms: i64,
    /// Coherent with the window read, but not a publication authorization.
    pub observed_payout_revision: i64,
    #[cfg(test)]
    pub _drop_probe: Option<CompactDropProbe>,
    // Last field: cleanup drops the large inputs before releasing admission.
    pub build_permit: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(test)]
pub(in crate::coordinator) struct CompactDropProbe {
    pub dropped: Option<tokio::sync::oneshot::Sender<std::thread::ThreadId>>,
    pub release: Arc<RepairProbe>,
    pub runtime_thread: std::thread::ThreadId,
}

#[cfg(test)]
impl Drop for CompactDropProbe {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(std::thread::current().id());
        }
        // A regression reports the wrong thread instead of deadlocking the
        // single runtime worker that needs to assert and release this probe.
        if std::thread::current().id() != self.runtime_thread {
            self.release.block();
        }
    }
}

impl Coordinator {
    /// Follow a decoded issued row's prepared link. The caller still validates
    /// the issued worker/session; a client-supplied issued key is never looked
    /// up as compact work. Lookup/reader failures stay errors, not cache misses.
    /// This adds no timeout: the caller's outer deadline includes every wait.
    #[cfg(test)]
    pub(in crate::coordinator) async fn hydrate_compact_inputs(
        &self,
        issued: &StoredJob,
    ) -> Result<Option<AdmittedCompactInputs>> {
        prepared_dependency_key(&issued.prepared_key)?;
        if issued.expires_at_ms <= self.work_ledger.now_ms().await? {
            return Ok(None);
        }
        let Some(stored) = self
            .work_ledger
            .compact_prepared(&issued.prepared_key)
            .await?
        else {
            return Ok(None);
        };
        let stored = CompactOwner::new(stored);
        // Compatibility is deliberate and checked before window/build I/O.
        // Use the same exact key equality as build_bundle, not case folding.
        if stored.record.audit_builder_version != qbit_prism::AUDIT_BUILDER_VERSION
            || stored.record.signer_keys != local_signer_keys(&self.config)?
        {
            return Ok(None);
        }
        let build_permit = self.build_slots.clone().acquire_owned().await?;
        // Tuple order releases the stored inputs before the permit on failure.
        let admitted = CompactOwner::new((stored.into_inner(), build_permit));
        let window_permit = self.window_reads.clone().acquire_owned().await?;
        let window = self
            .work_ledger
            .read_window_with_permit(
                &admitted.0.record.window,
                BalanceSource::AsIssued,
                window_permit,
            )
            .await?;
        // No await between the reader's successful handoff and ownership of
        // both the reconstructed window and its admission. Assemble/drop the
        // duplicate balance observation inside this admitted blocking owner.
        let (stored, build_permit) = admitted.into_inner();
        let owned = CompactOwner::new((stored, window, build_permit));
        let issued_expires_at_ms = issued.expires_at_ms;
        #[cfg(test)]
        let drop_probe = self.work_ledger.compact_drop_probe();
        let hydrated = owned
            .spawn_blocking(move |(stored, window, build_permit)| {
                let inputs = BundleInputs::from(&stored.record);
                let snapshot = Arc::new(Snapshot {
                    anchor_ms: stored.record.window.anchor_ms,
                    share_seq: stored.record.share_seq,
                    payout_revision: stored.record.payout_revision,
                    shares: window.shares,
                    prior_balances: stored.prior_balances,
                });
                drop(window.prior_balances);
                CompactOwner::new(CompactInputs {
                    record: stored.record,
                    template: stored.template,
                    snapshot,
                    inputs,
                    original_expires_at_ms: stored.original_expires_at_ms,
                    retained_until_ms: stored.expires_at_ms,
                    issued_expires_at_ms,
                    observed_payout_revision: window.payout_revision,
                    #[cfg(test)]
                    _drop_probe: drop_probe,
                    build_permit,
                })
            })
            .await?;
        // The immutable reservation may already have expired while a live
        // issued job retains its dependency. Neither retention nor hydration
        // renews that reservation or grants the issued job a new deadline.
        let now_ms = self.work_ledger.now_ms().await?;
        if issued.expires_at_ms <= now_ms || hydrated.retained_until_ms <= now_ms {
            return Ok(None);
        }
        Ok(Some(hydrated))
    }

    /// One admitted blocking owner retains its permit through encoding and
    /// hashing, even if the async waiter is cancelled. No nested build_bundle.
    /// Capture the explicit original-build handoff before persistence. Reading
    /// the publication here would select a key already reserved inline.
    #[cfg(test)]
    pub(in crate::coordinator) async fn capture_compact_prepared(
        &self,
        source: CompactOwner<OriginalPreparedBuild>,
        original_expires_at_ms: i64,
    ) -> Result<CompactOwner<CapturedCompactPrepared>> {
        prepared_dependency_key(&source.storage_key)?;
        let permit = self.build_slots.clone().acquire_owned().await?;
        let admitted = CompactOwner::new((source.into_inner(), permit));
        let extranonce2_size = self.config.extranonce2_size;
        let encoded = admitted
            .spawn_blocking(move |(source, permit)| {
                let _permit = permit;
                // A local declared after admission drops first, including
                // rejection before any source fields move into Prepared.
                let original_source = source;
                #[cfg(test)]
                if let Some(probe) = &original_source.capture_probe {
                    probe.block();
                }
                // Check the original handoff's cheap structural identity, not
                // a recomputed replacement hash. A false empty range would
                // otherwise discard the original audit and hydrate no shares.
                let snapshot = &original_source.stored.snapshot;
                let expected_range = match (snapshot.shares.first(), snapshot.shares.last()) {
                    (Some(first), Some(last)) => Some((
                        first.share_seq,
                        last.share_seq,
                        u64::try_from(snapshot.shares.len())?,
                    )),
                    _ => None,
                };
                let supplied_range = original_source.window.shares.map(|range| {
                    (
                        range.first_share_seq,
                        range.last_share_seq,
                        range.share_count,
                    )
                });
                if original_source.window.anchor_ms != snapshot.anchor_ms
                    || supplied_range != expected_range
                {
                    return Err(IncompatibleCompactBuild::WindowSnapshotMismatch.into());
                }
                // The original build must already have used canonical input.
                // Sorting here would silently change a completed audit's hash.
                let balances = &original_source.stored.snapshot.prior_balances;
                if !balances.is_sorted_by(|a, b| !balance_order(a, b).is_gt()) {
                    return Err(IncompatibleCompactBuild::NonCanonicalBalances.into());
                }
                if original_source
                    .stored
                    .bundle
                    .as_ref()
                    .is_some_and(|bundle| bundle.prior_balances != *balances)
                {
                    return Err(IncompatibleCompactBuild::BundleBalancesMismatch.into());
                }
                let stored = original_source.stored;
                let template = PreparedTemplate::encode(&stored.template)?;
                let audit_hashes = if original_source.window.shares.is_some() {
                    let bundle = stored
                        .bundle
                        .as_ref()
                        .context("prepared audit bundle missing")?;
                    Some(PreparedAuditHashes {
                        audit_bundle_sha256: canonical_json_sha256(bundle)?,
                        coinbase_manifest_sha256: canonical_json_sha256(
                            &bundle.signed_coinbase_manifest.manifest,
                        )?,
                    })
                } else {
                    None
                };
                let record = CompactPrepared::from_original_build(
                    &stored,
                    original_source.window,
                    &original_source.inputs,
                    &template,
                    audit_hashes,
                )?;
                let base_wire = stored
                    .bundle
                    .as_ref()
                    .map(|bundle| {
                        bundle_build::shared_base_wire(
                            &stored.template,
                            &bundle.signed_coinbase_manifest.manifest,
                            extranonce2_size,
                        )
                    })
                    .transpose()?;
                let captured = assemble_captured(
                    original_source.storage_key,
                    record,
                    template,
                    stored.template.clone(),
                    stored.snapshot.prior_balances.clone(),
                    stored.bundle.as_deref().map(PreparedBundle::from),
                    base_wire,
                    original_expires_at_ms,
                    original_source.created,
                    original_source.build_proof,
                )?;
                Ok::<_, anyhow::Error>(CompactOwner::new((captured, _permit)))
            })
            .await??;
        // A cancelled waiter leaves both output and admission in one blocking
        // cleanup owner. Release admission only after successful async handoff.
        let (captured, permit) = encoded.into_inner();
        let captured = CompactOwner::new(captured);
        drop(permit);
        Ok(captured)
    }

    /// This reserves a dependency only. Original economic identity and expiry
    /// stay fixed; the transaction receives a separately revalidated fence.
    ///
    /// Requires established polling readiness, even for an unpublished
    /// original build. A healthy tip observation alone does not establish
    /// last_poll; this path cannot bootstrap or restore revoked readiness.
    /// Never set last_poll early or publish work to satisfy this precondition.
    ///
    /// Cold callers instead use begin_compact_build before construction,
    /// reserve_fresh_compact after capture, and lock_compact_publication to
    /// install the checked result under their one outer deadline.
    #[cfg(test)]
    pub(in crate::coordinator) async fn save_captured_compact(
        &self,
        captured: &CapturedCompactPrepared,
    ) -> Result<bool> {
        ensure!(
            captured.original_expires_at_ms > self.work_ledger.now_ms().await?,
            "prepared reservation deadline elapsed"
        );
        let current_revision = self
            .issued_work_revision(&captured.original)
            .await?
            .context("payout snapshot stale")?;
        self.work_ledger
            .save_compact_prepared(
                &captured.original.storage_key,
                &captured.record,
                &captured.template,
                &captured.original.reservation.balances,
                current_revision,
                captured.original_expires_at_ms,
            )
            .await
    }
}
