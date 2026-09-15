//! Original compact inputs, not authority to publish miner work.
//!
//! This additive seam does not switch the inline runtime. Activation still
//! needs atomic typed issued-save/repair and wiring of these authority checks
//! under the existing outer operation deadline. The storage migration requires all
//! frontends stopped and old candidates drained; this is not a rolling writer.
//! A future refresh caller must prepare CanonicalCompactBalances before its
//! original bundle build. Existing SQL-order legacy builds remain unchanged;
//! completed noncanonical bundles cannot be converted by rewriting their hash.
use super::*;
use crate::coordinator::publication_authority::{AbsoluteDeadline, AuthorityViewMut};
use crate::coordinator::tip_observation::PreparedIdentity;
use crate::ledger::{CompactPrepared, PreparedAuditHashes, PreparedTemplate};

// Keep both directions of the inline/compact input mapping together without
// changing the compact record's existing flat fields or serialization order.
impl From<&CompactPrepared> for BundleInputs {
    fn from(record: &CompactPrepared) -> Self {
        Self {
            payout_policy: record.payout_policy.clone(),
            ctv: record.ctv.clone(),
            signer_keys: record.signer_keys.clone(),
            audit_builder_version: record.audit_builder_version,
        }
    }
}

impl CompactPrepared {
    fn from_original_build(
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

/// Successful reads still own potentially large inputs. Keep abandonment off
/// the runtime, including when a caller drops a result outside Tokio context.
pub(in crate::coordinator) struct CompactOwner<T: Send + 'static> {
    value: Option<T>,
    runtime: tokio::runtime::Handle,
}

impl<T: Send + 'static> CompactOwner<T> {
    pub(in crate::coordinator) fn new(value: T) -> Self {
        Self {
            value: Some(value),
            runtime: tokio::runtime::Handle::current(),
        }
    }

    // Used only for synchronous owner-to-owner handoff: never cross an await
    // with the unguarded value or expose it to an async runtime caller.
    pub(in crate::coordinator) fn into_inner(mut self) -> T {
        self.value.take().expect("owned until handed off")
    }

    pub fn spawn_blocking<R: Send + 'static>(
        mut self,
        work: impl FnOnce(T) -> R + Send + 'static,
    ) -> tokio::task::JoinHandle<R> {
        let value = self.value.take().expect("owned until handed off");
        self.runtime.spawn_blocking(move || work(value))
    }
}

impl<T: Send + 'static> std::ops::Deref for CompactOwner<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value.as_ref().expect("owned until handed off")
    }
}

impl<T: Send + 'static> Drop for CompactOwner<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            self.runtime.spawn_blocking(move || drop(value));
        }
    }
}

/// Keep the source identity alive across encoding, waits and save retries.
pub(in crate::coordinator) struct CapturedCompactPrepared {
    pub original: Arc<Prepared>,
    pub record: CompactPrepared,
    pub template: PreparedTemplate,
    pub original_expires_at_ms: i64,
    build_proof: Option<CompactBuildProof>,
}

/// Capture before the original build. This stamp cannot authorize publication
/// by itself: reservation and publication each require fresh external checks.
pub(in crate::coordinator) struct CompactBuildProof {
    readiness_epoch: u64,
    publication: Option<PreparedIdentity>,
    published_tip: Option<(String, u64)>,
}

impl CompactBuildProof {
    pub(in crate::coordinator) fn readiness_epoch(&self) -> u64 {
        self.readiness_epoch
    }
}

/// An idempotently persisted dependency, still unavailable to miners. The
/// original owner keeps all large inputs alive through later validation.
pub(in crate::coordinator) struct ReservedCompact<'a> {
    captured: &'a CapturedCompactPrepared,
    pub inserted: bool,
    deadline: AbsoluteDeadline,
}

/// A short, synchronous installation boundary. All I/O finished before these
/// ordered locks were acquired. Dropping it publishes nothing.
#[must_use]
pub(in crate::coordinator) struct CompactPublicationGuard<'a> {
    view: AuthorityViewMut<'a>,
    refresh: &'a watch::Sender<u64>,
    captured: &'a CapturedCompactPrepared,
    clock: AbsoluteDeadline,
    template_max_age: Duration,
}

impl CompactPublicationGuard<'_> {
    /// No runtime caller is wired in this prerequisite slice. Future callers
    /// install immediately, under their original outer operation deadline.
    pub fn publish(mut self) -> Result<()> {
        ensure!(self.clock.live(), "prepared reservation deadline elapsed");
        crate::readiness::validate_template_age(
            &self.captured.original.template,
            self.template_max_age,
        )?;
        self.view.tip.publish(&self.captured.record.parent_hash)?;
        *self.view.prepared = Some(self.captured.original.clone());
        self.view.readiness.last_poll = Some(Instant::now());
        self.refresh.send_replace(self.captured.original.generation);
        Ok(())
    }
}

/// Original-build input, prepared before any bundle or audit hash exists.
pub(in crate::coordinator) struct CanonicalCompactBalances(Vec<qbit_prism::CarryForwardBalance>);

fn balance_order(
    left: &qbit_prism::CarryForwardBalance,
    right: &qbit_prism::CarryForwardBalance,
) -> std::cmp::Ordering {
    left.order_key
        .cmp(&right.order_key)
        .then_with(|| left.recipient_id.cmp(&right.recipient_id))
        .then_with(|| left.p2mr_program_hex.cmp(&right.p2mr_program_hex))
}

impl CanonicalCompactBalances {
    /// Run inside the original build's blocking owner, before constructing its
    /// snapshot/bundle. Borrow that admission; never acquire a nested slot.
    /// This changes input order only, using the immutable blob's comparator.
    pub fn prepare(
        mut balances: Vec<qbit_prism::CarryForwardBalance>,
        _admission: &tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        balances.sort_by(balance_order);
        Self(balances)
    }

    pub fn into_original_build(self) -> Vec<qbit_prism::CarryForwardBalance> {
        self.0
    }
}

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
pub(in crate::coordinator) type AdmittedCompactInputs = CompactOwner<CompactInputs>;

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
    pub drop_probe: Option<CompactDropProbe>,
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

#[derive(Debug, thiserror::Error)]
#[error("invalid prepared dependency link")]
pub(in crate::coordinator) struct InvalidPreparedDependency;

/// The namespace generated by refresh, including foreign instance IDs. Parse
/// the suffix from the right: configured instance IDs may themselves use ':'.
pub(in crate::coordinator) fn prepared_dependency_key(key: &str) -> Result<()> {
    let (instance, suffix) = key
        .strip_prefix("prepared:")
        .and_then(|tail| tail.rsplit_once(':'))
        .ok_or(InvalidPreparedDependency)?;
    if instance.len() > 128
        || instance.chars().any(char::is_control)
        || instance.starts_with("prepared:")
        || suffix.len() != 32
        || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(InvalidPreparedDependency.into());
    }
    Ok(())
}

/// Hash the producer's canonical Serialize representation without retaining a
/// second whole-bundle byte array or rebuilding the original payout to read it.
pub(in crate::coordinator) fn canonical_json_sha256(
    value: &impl serde::Serialize,
) -> Result<String> {
    struct DigestWriter(Sha256);
    impl std::io::Write for DigestWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = DigestWriter(Sha256::new());
    serde_json::to_writer(&mut writer, value)?;
    Ok(hex::encode(writer.0.finalize()))
}

pub(in crate::coordinator) fn audit_parts_sha256(
    body: &qbit_prism::AuditBundleBody,
    shares: &[AcceptedShare],
) -> Result<String> {
    struct HashWriter(Sha256);
    impl std::io::Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = HashWriter(Sha256::new());
    qbit_prism::write_canonical_audit_bundle_from_parts(&mut writer, body, shares)?;
    Ok(hex::encode(writer.0.finalize()))
}

#[allow(clippy::too_many_arguments)]
pub(in crate::coordinator) fn assemble_captured(
    key: String,
    record: CompactPrepared,
    template: PreparedTemplate,
    template_value: Value,
    balances: Vec<qbit_prism::CarryForwardBalance>,
    bundle: Option<PreparedBundle>,
    base_wire: Option<codec::Job>,
    original_expires_at_ms: i64,
    created: Instant,
    build_proof: Option<CompactBuildProof>,
) -> Result<CapturedCompactPrepared> {
    ensure!(
        record.template_sha256 == template.sha256(),
        "prepared template identity mismatch"
    );
    let reservation = Arc::new(compact_runtime::PreparedReservation {
        record: record.clone(),
        template: template.clone(),
        balances: Arc::new(balances),
        original_expires_at_ms,
    });
    let original = Arc::new(Prepared {
        reservation,
        repair: Arc::new(Mutex::new(())),
        #[cfg(test)]
        repair_probe: Default::default(),
        template: template_value,
        snapshot: Arc::new(PreparedSnapshot {
            anchor_ms: record.window.anchor_ms,
            share_seq: record.share_seq,
            payout_revision: record.payout_revision,
        }),
        window: record.window,
        inputs: BundleInputs::from(&record),
        bundle: bundle.map(Arc::new),
        base_wire,
        storage_key: key,
        fee: record.fee,
        fingerprint: record.fingerprint.clone(),
        generation: record.generation,
        created,
        parent_of_tip: record.parent_of_tip.clone(),
    });
    Ok(CapturedCompactPrepared {
        original,
        record,
        template,
        original_expires_at_ms,
        build_proof,
    })
}

pub(in crate::coordinator) struct RefreshBuild {
    pub proof: CompactBuildProof,
    pub key: String,
    pub template: Value,
    pub snapshot: Snapshot,
    pub inputs: BundleInputs,
    pub fee: Option<FanoutFeeRatePolicy>,
    pub fingerprint: String,
    pub generation: u64,
    pub parent_of_tip: String,
    pub suffix: String,
    pub original_expires_at_ms: i64,
}

impl Coordinator {
    /// The original snapshot and admission move directly into the blocking
    /// owner. Cancellation never drops a large successful build on the runtime.
    pub(in crate::coordinator) async fn capture_refresh(
        &self,
        source: CompactOwner<(RefreshBuild, tokio::sync::OwnedSemaphorePermit)>,
    ) -> Result<CompactOwner<CapturedCompactPrepared>> {
        let config = self.config.clone();
        let result = source
            .spawn_blocking(move |(source, permit)| {
                let admission = permit;
                let mut source = source;
                source.snapshot.prior_balances = CanonicalCompactBalances::prepare(
                    std::mem::take(&mut source.snapshot.prior_balances),
                    &admission,
                )
                .into_original_build();
                let window = WindowRef::from_snapshot(&source.snapshot)?;
                let body = if window.shares.is_some() {
                    Some(
                        bundle_build::build_body(
                            &config,
                            &source.snapshot,
                            &source.template,
                            None,
                            source.suffix.clone(),
                            source.inputs.clone(),
                        )?
                        .0,
                    )
                } else {
                    None
                };
                let hashes = body
                    .as_ref()
                    .map(|body| {
                        Ok::<_, anyhow::Error>(PreparedAuditHashes {
                            audit_bundle_sha256: audit_parts_sha256(body, &source.snapshot.shares)?,
                            coinbase_manifest_sha256: canonical_json_sha256(
                                &body.signed_coinbase_manifest.manifest,
                            )?,
                        })
                    })
                    .transpose()?;
                let base_wire = body
                    .as_ref()
                    .map(|body| {
                        codec::Job::from_manifest(
                            "shared".into(),
                            &source.template,
                            &body.signed_coinbase_manifest.manifest,
                            "00000000",
                            config.extranonce2_size,
                            1.0,
                            0.0,
                            true,
                        )
                    })
                    .transpose()?;
                let template = PreparedTemplate::encode(&source.template)?;
                let record = CompactPrepared {
                    format_version: CompactPrepared::FORMAT_VERSION,
                    window,
                    share_seq: source.snapshot.share_seq,
                    payout_revision: source.snapshot.payout_revision,
                    template_sha256: template.sha256().into(),
                    parent_hash: source.template["previousblockhash"]
                        .as_str()
                        .context("prepared parent missing")?
                        .into(),
                    parent_of_tip: source.parent_of_tip,
                    fingerprint: source.fingerprint,
                    generation: source.generation,
                    coinbase_suffix_hex: source.suffix,
                    payout_policy: source.inputs.payout_policy,
                    ctv: source.inputs.ctv,
                    fee: source.fee,
                    audit_builder_version: source.inputs.audit_builder_version,
                    signer_keys: source.inputs.signer_keys,
                    audit_hashes: hashes,
                };
                let captured = assemble_captured(
                    source.key,
                    record,
                    template,
                    source.template,
                    source.snapshot.prior_balances,
                    body.as_ref().map(PreparedBundle::from),
                    base_wire,
                    source.original_expires_at_ms,
                    Instant::now(),
                    Some(source.proof),
                )?;
                // Body/counts and the original accepted rows drop here, before
                // admission can be released by the synchronous output handoff.
                drop(body);
                drop(source.snapshot.shares);
                Ok::<_, anyhow::Error>(CompactOwner::new((captured, admission)))
            })
            .await??;
        let (captured, permit) = result.into_inner();
        let captured = CompactOwner::new(captured);
        drop(permit);
        Ok(captured)
    }
}

impl Coordinator {
    /// Begin before the original build, including on a cold/default frontend.
    /// This captures revocation/publication identity without creating readiness.
    /// The caller's one outer deadline covers this, build, reserve and install.
    pub(in crate::coordinator) async fn begin_compact_build(&self) -> CompactBuildProof {
        let view = self.authority_view().await;
        CompactBuildProof {
            readiness_epoch: view.readiness.generation,
            publication: view.prepared.as_deref().map(PreparedIdentity::of),
            published_tip: view.tip.publication_stamp(),
        }
    }

    fn check_compact_build_stamp(
        proof: &CompactBuildProof,
        prepared: Option<&Prepared>,
        readiness: &ReadinessState,
        tip: &TipState,
    ) -> Result<()> {
        ensure!(
            readiness.generation == proof.readiness_epoch,
            "node readiness changed during compact build"
        );
        ensure!(
            match (&proof.publication, prepared) {
                (Some(identity), Some(prepared)) => identity.matches(prepared),
                (None, None) => true,
                _ => false,
            } && tip.publication_stamp() == proof.published_tip,
            "work publication changed during compact build"
        );
        Ok(())
    }

    async fn check_compact_build_current(&self, proof: &CompactBuildProof) -> Result<()> {
        let view = self.authority_view().await;
        Self::check_compact_build_stamp(proof, view.prepared.as_deref(), &view.readiness, &view.tip)
    }

    async fn prove_fresh_compact(
        &self,
        captured: &CapturedCompactPrepared,
    ) -> Result<AbsoluteDeadline> {
        let proof = captured
            .build_proof
            .as_ref()
            .context("pre-build authority proof missing")?;
        self.check_compact_build_current(proof).await?;
        let original = &captured.original;
        let parent = &captured.record.parent_hash;
        self.ensure_template_fresh(&original.template).await?;
        let height = original.template["height"]
            .as_u64()
            .and_then(|height| height.checked_sub(1))
            .context("invalid compact template height")?;
        let info = self.ready_tip(parent).await?;
        ensure!(
            info["blocks"].as_u64() == Some(height),
            "compact template height changed"
        );
        let revision = self
            .work_ledger
            .observe_chain_view(
                parent,
                height,
                info["chainwork"]
                    .as_str()
                    .context("node chainwork missing")?,
            )
            .await?;
        ensure!(
            revision == captured.record.payout_revision,
            "payout snapshot stale"
        );
        let requested_at = tokio::time::Instant::now();
        let clock = AbsoluteDeadline::from_database(
            self.work_ledger.now_ms().await?,
            requested_at,
            captured.original_expires_at_ms,
        )?;
        // A slow database lookup cannot leave the earlier node proof in force.
        // These calls may revoke readiness; never hold its lock across them.
        let info = self.ready_tip(parent).await?;
        ensure!(
            info["blocks"].as_u64() == Some(height),
            "compact template height changed"
        );
        // Match refresh's final economic fence after node I/O: balances or
        // revision may have changed while that second node proof waited.
        let state = self.work_ledger.payout_state().await?;
        ensure!(
            state.payout_revision == captured.record.payout_revision
                && state.prior_balances_digest == captured.record.window.prior_balances_digest,
            "payout snapshot stale"
        );
        self.ensure_template_fresh(&original.template).await?;
        let view = self.authority_view().await;
        Self::check_compact_build_stamp(
            proof,
            view.prepared.as_deref(),
            &view.readiness,
            &view.tip,
        )?;
        ensure!(
            view.tip.as_deref() == Some(parent),
            "compact tip observation superseded"
        );
        ensure!(clock.live(), "prepared reservation deadline elapsed");
        Ok(clock)
    }

    /// Reserve without a readiness or replacement-lease shortcut. A revocation
    /// after the transaction may leave an unexposed immutable dependency; it
    /// never publishes work, and retries keep the same absolute expiry.
    pub(in crate::coordinator) async fn reserve_fresh_compact<'a>(
        &self,
        captured: &'a CapturedCompactPrepared,
    ) -> Result<ReservedCompact<'a>> {
        let deadline = self.prove_fresh_compact(captured).await?;
        let inserted = self
            .work_ledger
            .save_compact_prepared(
                &captured.original.storage_key,
                &captured.record,
                &captured.template,
                &captured.original.reservation.balances,
                captured.record.payout_revision,
                captured.original_expires_at_ms,
            )
            .await?;
        self.prove_fresh_compact(captured).await?;
        ensure!(deadline.live(), "prepared reservation deadline elapsed");
        Ok(ReservedCompact {
            captured,
            inserted,
            deadline,
        })
    }

    /// Revalidate after every reservation/caller wait, then take the existing
    /// atomic publication lock order. No RPC or database wait holds these locks.
    pub(in crate::coordinator) async fn lock_compact_publication<'a>(
        &'a self,
        reserved: ReservedCompact<'a>,
    ) -> Result<CompactPublicationGuard<'a>> {
        let captured = reserved.captured;
        let clock = reserved.deadline;
        self.prove_fresh_compact(captured).await?;
        let view = loop {
            // No asynchronous gap between the last external proof and these
            // coupled locks. If acquisition would wait, release partial locks,
            // wait for the boundary, then refresh the external proof.
            let available = self.try_authority_view_mut();
            if let Some(guards) = available {
                break guards;
            }
            {
                let _view = self.authority_view_mut().await;
            }
            // Keep the first clock: retries cannot renew this operation's
            // fixed expiry, including when a clock reply was delayed.
            ensure!(clock.live(), "prepared reservation deadline elapsed");
            self.prove_fresh_compact(captured).await?;
        };
        Self::check_compact_build_stamp(
            captured
                .build_proof
                .as_ref()
                .context("pre-build authority proof missing")?,
            view.prepared.as_deref(),
            &view.readiness,
            &view.tip,
        )?;
        ensure!(
            view.tip.as_deref() == Some(captured.record.parent_hash.as_str()),
            "compact tip observation superseded"
        );
        ensure!(clock.live(), "prepared reservation deadline elapsed");
        crate::readiness::validate_template_age(
            &captured.original.template,
            self.config.template_max_age,
        )?;
        Ok(CompactPublicationGuard {
            view,
            refresh: &self.refresh,
            captured,
            clock,
            template_max_age: self.config.template_max_age,
        })
    }

    /// Follow a decoded issued row's prepared link. The caller still validates
    /// the issued worker/session; a client-supplied issued key is never looked
    /// up as compact work. Lookup/reader failures stay errors, not cache misses.
    /// This adds no timeout: the caller's outer deadline includes every wait.
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
                    drop_probe,
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
                        codec::Job::from_manifest(
                            "shared".into(),
                            &stored.template,
                            &bundle.signed_coinbase_manifest.manifest,
                            "00000000",
                            extranonce2_size,
                            1.0,
                            0.0,
                            true,
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
