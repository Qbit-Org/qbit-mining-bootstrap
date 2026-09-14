//! Original compact inputs, not authority to publish miner work.
//!
//! This additive seam does not switch the inline runtime. Activation still
//! needs atomic typed issued-save/repair and post-build authority checks under
//! the existing outer operation deadline. The storage migration requires all
//! frontends stopped and old candidates drained; this is not a rolling writer.
//! A future refresh caller must prepare CanonicalCompactBalances before its
//! original bundle build. Existing SQL-order legacy builds remain unchanged;
//! completed noncanonical bundles cannot be converted by rewriting their hash.
use super::*;
use crate::ledger::{CompactPrepared, PreparedAuditHashes, PreparedTemplate};

/// Successful reads still own potentially large inputs. Keep abandonment off
/// the runtime, including when a caller drops a result outside Tokio context.
pub(in crate::coordinator) struct CompactOwner<T: Send + 'static> {
    value: Option<T>,
    runtime: tokio::runtime::Handle,
}

impl<T: Send + 'static> CompactOwner<T> {
    fn new(value: T) -> Self {
        Self {
            value: Some(value),
            runtime: tokio::runtime::Handle::current(),
        }
    }

    // Used only for synchronous owner-to-owner handoff: never cross an await
    // with the unguarded value or expose it to an async runtime caller.
    fn into_inner(mut self) -> T {
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
/// format reserves the key. Legacy/resumed Prepared cannot recover historical
/// CTV/version inputs, so there is intentionally no conversion from Prepared.
pub(in crate::coordinator) struct OriginalPreparedBuild {
    storage_key: String,
    stored: Arc<StoredPrepared>,
    window: WindowRef,
    inputs: BundleInputs,
    created: Instant,
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
            #[cfg(test)]
            capture_probe: None,
            #[cfg(test)]
            drop_probe: None,
        })
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
fn prepared_dependency_key(key: &str) -> Result<()> {
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
fn canonical_json_sha256(value: &impl serde::Serialize) -> Result<String> {
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

impl Coordinator {
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
                let inputs = BundleInputs {
                    payout_policy: stored.record.payout_policy.clone(),
                    ctv: stored.record.ctv.clone(),
                    signer_keys: stored.record.signer_keys.clone(),
                    audit_builder_version: stored.record.audit_builder_version,
                };
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
                // Only original parts enter this private authority view. Neither
                // this assembly nor capture publishes it or reads configuration.
                let stored = original_source.stored;
                let original = Arc::new(Prepared {
                    template: stored.template.clone(),
                    snapshot: stored.snapshot.clone(),
                    window: original_source.window,
                    inputs: original_source.inputs,
                    bundle: stored.bundle.clone(),
                    base_wire: None,
                    storage_key: original_source.storage_key,
                    fee: stored.fee,
                    fingerprint: stored.fingerprint.clone(),
                    generation: stored.generation,
                    created: original_source.created,
                    parent_of_tip: stored.parent_of_tip.clone(),
                    stored,
                    repair: Arc::new(Mutex::new(())),
                    #[cfg(test)]
                    repair_probe: Default::default(),
                });
                let stored = &original.stored;
                let template = PreparedTemplate::encode(&stored.template)?;
                // Only the original nonempty window has audit hashes; a later
                // worker-specific bootstrap bundle is not part of this identity.
                let audit_hashes = if original.window.shares.is_some() {
                    let bundle = stored
                        .bundle
                        .as_ref()
                        .context("prepared audit bundle missing")?;
                    // This is the original builder's output, not untrusted
                    // resumed data. Preserve its exact canonical byte identity;
                    // do not reconstruct or rewrite it while capturing hashes.
                    Some(PreparedAuditHashes {
                        audit_bundle_sha256: canonical_json_sha256(bundle)?,
                        coinbase_manifest_sha256: canonical_json_sha256(
                            &bundle.signed_coinbase_manifest.manifest,
                        )?,
                    })
                } else {
                    None
                };
                let record = CompactPrepared {
                    format_version: CompactPrepared::FORMAT_VERSION,
                    window: original.window,
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
                    payout_policy: original.inputs.payout_policy.clone(),
                    ctv: original.inputs.ctv.clone(),
                    fee: stored.fee,
                    audit_builder_version: original.inputs.audit_builder_version,
                    signer_keys: original.inputs.signer_keys.clone(),
                    audit_hashes,
                };
                Ok::<_, anyhow::Error>(CompactOwner::new((
                    CapturedCompactPrepared {
                        original,
                        record,
                        template,
                        original_expires_at_ms,
                    },
                    _permit,
                )))
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
    /// A future cold-reservation path must carry the readiness epoch from
    /// before the original build (not Prepared.generation). After capture it
    /// must freshly check node/tip readiness, template freshness and current
    /// chain payout revision against the original, without a replacement-lease
    /// fallback, and verify the epoch is unchanged. Persistence still uses
    /// transactional revision/configuration fences and the fixed absolute
    /// expiry. The caller must revalidate after persistence before atomic
    /// publication establishes last_poll, under its one outer deadline.
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
                &captured.original.stored.snapshot.prior_balances,
                current_revision,
                captured.original_expires_at_ms,
            )
            .await
    }
}
