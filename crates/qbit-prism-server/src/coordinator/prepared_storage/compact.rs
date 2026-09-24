//! Original compact inputs, not authority to publish miner work.
//!
//! Runtime refresh captures canonical balances before the original borrowed
//! build, reserves the typed record, and publishes under the reviewed authority
//! boundary. Issued save/repair and reconstruction preserve that identity under
//! the original operation deadline. The storage migration requires all
//! frontends stopped and old candidates drained; this is not a rolling writer.
//! Legacy fixture adapters cannot convert completed noncanonical bundles by
//! rewriting their hash.
use super::*;
use crate::coordinator::publication_authority::{AbsoluteDeadline, AuthorityViewMut};
use crate::coordinator::tip_observation::PreparedIdentity;
use crate::ledger::{CompactPrepared, PreparedAuditHashes, PreparedTemplate};

#[cfg(test)]
mod refresh_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
pub(in crate::coordinator) use test_support::{
    CompactDropProbe, IncompatibleCompactBuild, OriginalPreparedBuild,
};

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
    #[cfg(test)]
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
    /// Runtime callers
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

/// Hash the producer's canonical Serialize representation without retaining a
/// second whole-bundle byte array or rebuilding the original payout to read it.
pub(in crate::coordinator) fn canonical_json_sha256(
    value: &impl serde::Serialize,
) -> Result<String> {
    let mut writer = DigestWriter(Sha256::new());
    serde_json::to_writer(&mut writer, value)?;
    Ok(hex::encode(writer.0.finalize()))
}

pub(in crate::coordinator) fn audit_parts_sha256(
    body: &qbit_prism::AuditBundleBody,
    shares: &[AcceptedShare],
) -> Result<String> {
    let mut writer = DigestWriter(Sha256::new());
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

/// Pure template-specific work, independent of the native WindowRef digest.
/// Its counted-share digest and audit framing still use the existing builders.
pub(in crate::coordinator) struct RefreshBody {
    body: Option<qbit_prism::AuditBundleBody>,
    hashes: Option<PreparedAuditHashes>,
    base_wire: Option<codec::Job>,
}

pub(in crate::coordinator) type CapturedRefreshBody =
    CompactOwner<(Result<RefreshBody>, Arc<tokio::sync::OwnedSemaphorePermit>)>;

/// The reused-window rebuild (a new template on the same window): the same
/// chunked fold, leaf, prefix and suffix as the pipelined refresh, on one lane.
pub(in crate::coordinator) fn prepare_refresh_body_with(
    config: &Config,
    snapshot: &Snapshot,
    template: &Value,
    suffix: String,
    inputs: BundleInputs,
    parallelism: qbit_prism::Parallelism,
) -> Result<RefreshBody> {
    let prepared = prepare_refresh_body_unhashed_with(
        config,
        snapshot,
        template,
        suffix,
        inputs,
        parallelism,
    )?;
    let (prefix, _share_digest) =
        qbit_prism::CanonicalAuditHashPrefix::new_with_share_digest(&snapshot.shares, parallelism)?;
    finish_refresh_body_with(prefix, prepared, parallelism)
}

/// The body worker returns before audit encoding, so its continuation can
/// consume the independently computed prefix without blocking this worker.
pub(in crate::coordinator) struct UnhashedRefreshBody {
    body: Option<qbit_prism::AuditBundleBody>,
    base_wire: Option<codec::Job>,
}

/// The body worker's output before audit encoding, with the builder's
/// counted-share fold and leaf digest spread over `parallelism`.
pub(in crate::coordinator) fn prepare_refresh_body_unhashed_with(
    config: &Config,
    snapshot: &Snapshot,
    template: &Value,
    suffix: String,
    inputs: BundleInputs,
    parallelism: qbit_prism::Parallelism,
) -> Result<UnhashedRefreshBody> {
    // WindowRef::from_snapshot uses precisely this condition for shares: Some.
    // The native digest remains mandatory before any compact record is assembled.
    let body = if snapshot.shares.is_empty() {
        None
    } else {
        Some(
            bundle_build::build_body_with(
                config,
                snapshot,
                template,
                None,
                suffix,
                inputs,
                parallelism,
            )?
            .0,
        )
    };
    let base_wire = body
        .as_ref()
        .map(|body| {
            bundle_build::shared_base_wire(
                template,
                &body.signed_coinbase_manifest.manifest,
                config.extranonce2_size,
            )
        })
        .transpose()?;
    Ok(UnhashedRefreshBody { body, base_wire })
}

/// Finish the body worker's output with the independently computed prefix;
/// the audit suffix's counted-share array is serialized over `parallelism`.
pub(in crate::coordinator) fn finish_refresh_body_with(
    prefix: qbit_prism::CanonicalAuditHashPrefix,
    prepared: UnhashedRefreshBody,
    parallelism: qbit_prism::Parallelism,
) -> Result<RefreshBody> {
    let UnhashedRefreshBody { body, base_wire } = prepared;
    let hashes = body
        .as_ref()
        .map(|body| {
            Ok::<_, anyhow::Error>(PreparedAuditHashes {
                audit_bundle_sha256: prefix.finish_with(body, parallelism)?,
                coinbase_manifest_sha256: canonical_json_sha256(
                    &body.signed_coinbase_manifest.manifest,
                )?,
            })
        })
        .transpose()?;
    Ok(RefreshBody {
        body,
        hashes,
        base_wire,
    })
}

pub(in crate::coordinator) struct RefreshBuild {
    pub proof: CompactBuildProof,
    pub key: String,
    pub template: Value,
    pub window: refresh_window::CachedWindow,
    pub body: Option<CapturedRefreshBody>,
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
        source: CompactOwner<(RefreshBuild, Arc<tokio::sync::OwnedSemaphorePermit>)>,
    ) -> Result<CompactOwner<CapturedCompactPrepared>> {
        let config = self.config.clone();
        #[cfg(test)]
        let drop_probe = self.work_ledger.compact_drop_probe();
        let result = source
            .spawn_blocking(move |(inputs, permit)| {
                // Bind admission first so later locals drop before it on error.
                let admission = permit;
                #[cfg(test)]
                let _cleanup = drop_probe;
                let source = inputs;
                let snapshot = &source.window.snapshot;
                let window = source.window.reference;
                let RefreshBody {
                    body,
                    hashes,
                    base_wire,
                } = match source.body {
                    Some(body) => body.into_inner().0?,
                    None => prepare_refresh_body_with(
                        &config,
                        snapshot,
                        &source.template,
                        source.suffix.clone(),
                        source.inputs.clone(),
                        refresh_window::refresh_parallelism(),
                    )?,
                };
                let template = PreparedTemplate::encode(&source.template)?;
                let record = CompactPrepared {
                    format_version: CompactPrepared::FORMAT_VERSION,
                    window,
                    share_seq: snapshot.share_seq,
                    payout_revision: snapshot.payout_revision,
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
                    snapshot.prior_balances.clone(),
                    body.as_ref().map(PreparedBundle::from),
                    base_wire,
                    source.original_expires_at_ms,
                    Instant::now(),
                    Some(source.proof),
                )?;
                // The refresh loop alone retains the original accepted rows
                // until invalidation; the counted shares leave with admission.
                drop(source.window);
                Ok::<_, anyhow::Error>(CompactOwner::new((captured, body, admission)))
            })
            .await??;
        let (captured, body, permit) = result.into_inner();
        let captured = CompactOwner::new(captured);
        // The build slot is released first: a queued blocking task must not
        // hold admission. The counted shares are then destroyed on the
        // blocking pool, off this awaited path.
        drop(permit);
        drop(CompactOwner::new(body));
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
        let _inserted = self
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
            #[cfg(test)]
            inserted: _inserted,
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
}
