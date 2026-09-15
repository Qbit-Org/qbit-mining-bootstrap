//! Runtime views retain original identity and repair inputs, never share arrays.
use super::*;
use crate::ledger::{CompactPrepared, CompactRepair, PreparedTemplate};

/// Metadata of the original snapshot. This is deliberately not a Snapshot:
/// retaining this view never implies ownership of its accepted-share window.
#[derive(Clone, Debug, Serialize)]
pub struct PreparedSnapshot {
    pub anchor_ms: i64,
    pub share_seq: u64,
    pub payout_revision: i64,
}

impl From<&Snapshot> for PreparedSnapshot {
    fn from(snapshot: &Snapshot) -> Self {
        Self {
            anchor_ms: snapshot.anchor_ms,
            share_seq: snapshot.share_seq,
            payout_revision: snapshot.payout_revision,
        }
    }
}

/// Submission needs the original block economics and CTV presence, not either
/// accepted or counted shares. This view is not a serializable audit artifact.
#[derive(Clone, Debug, Serialize)]
pub struct PreparedBundle {
    pub found_block: FoundBlock,
    pub ctv_fanout_manifest_set: Option<PreparedCtv>,
    pub coinbase_script_sig_suffix_hex: Option<String>,
}

/// Presence of the original CTV fanout set, without retaining its outputs.
/// Candidate reconstruction uses the original captured CTV inputs separately.
#[derive(Clone, Debug, Serialize)]
pub struct PreparedCtv {}

impl From<&qbit_prism::AuditBundleBody> for PreparedBundle {
    fn from(body: &qbit_prism::AuditBundleBody) -> Self {
        Self {
            found_block: body.found_block.clone(),
            ctv_fanout_manifest_set: body
                .ctv_fanout_manifest_set
                .as_ref()
                .map(|_| PreparedCtv {}),
            coinbase_script_sig_suffix_hex: body.coinbase_script_sig_suffix_hex.clone(),
        }
    }
}

impl From<&AuditBundle> for PreparedBundle {
    fn from(bundle: &AuditBundle) -> Self {
        Self {
            found_block: bundle.found_block.clone(),
            ctv_fanout_manifest_set: bundle
                .ctv_fanout_manifest_set
                .as_ref()
                .map(|_| PreparedCtv {}),
            coinbase_script_sig_suffix_hex: bundle.coinbase_script_sig_suffix_hex.clone(),
        }
    }
}

/// Exact original immutable inputs needed if the dependency record disappears.
/// Template bytes and balances are shared per reservation; no accepted-share
/// or counted-share collection can enter this type.
pub(super) struct PreparedReservation {
    pub record: CompactPrepared,
    pub template: PreparedTemplate,
    pub balances: Arc<Vec<qbit_prism::CarryForwardBalance>>,
    pub original_expires_at_ms: i64,
}

impl PreparedReservation {
    pub fn encode_repair(&self) -> Result<CompactRepair> {
        CompactRepair::encode(
            &self.record,
            &self.template,
            &self.balances,
            self.original_expires_at_ms,
        )
    }

    pub fn dependency<'a>(&'a self, key: &'a str) -> crate::ledger::CompactDependency<'a> {
        crate::ledger::CompactDependency {
            key,
            original_revision: self.record.payout_revision,
            parent: &self.record.parent_hash,
            original_expires_at_ms: self.original_expires_at_ms,
            template_sha256: &self.record.template_sha256,
            prior_balances_digest: self.record.window.prior_balances_digest,
        }
    }
}

impl Prepared {
    /// Only an actual empty original window can construct bootstrap inputs.
    /// A nonempty reference must go through the admitted window reader.
    fn bootstrap_snapshot(&self) -> Result<Snapshot> {
        ensure!(self.window.shares.is_none(), "nonempty bootstrap window");
        Ok(Snapshot {
            anchor_ms: self.snapshot.anchor_ms,
            share_seq: self.snapshot.share_seq,
            payout_revision: self.snapshot.payout_revision,
            shares: Vec::new(),
            prior_balances: (*self.reservation.balances).clone(),
        })
    }
}

impl Coordinator {
    pub(super) async fn materialize_wire(
        &self,
        prepared: Arc<Prepared>,
        worker: Worker,
        extranonce2_size: usize,
    ) -> Result<(codec::Job, Arc<PreparedBundle>, Option<AcceptedShare>)> {
        if let (Some(wire), Some(bundle)) = (&prepared.base_wire, &prepared.bundle) {
            ensure!(
                wire.extranonce2_size == extranonce2_size,
                "stored extranonce2 size mismatch"
            );
            return Ok((wire.clone(), bundle.clone(), None));
        }
        let permit = self.build_slots.clone().acquire_owned().await?;
        let owned = prepared_storage::compact::CompactOwner::new((prepared, permit));
        let config = self.config.clone();
        let output = owned
            .spawn_blocking(move |(source, permit)| {
                let admission = permit;
                let prepared = source;
                let snapshot = prepared.bootstrap_snapshot()?;
                let (body, bootstrap) = bundle_build::build_body(
                    &config,
                    &snapshot,
                    &prepared.template,
                    Some(worker),
                    prepared.reservation.record.coinbase_suffix_hex.clone(),
                    prepared.inputs.clone(),
                )?;
                let wire = codec::Job::from_manifest(
                    "shared".into(),
                    &prepared.template,
                    &body.signed_coinbase_manifest.manifest,
                    "00000000",
                    extranonce2_size,
                    1.0,
                    0.0,
                    true,
                )?;
                let bundle = Arc::new(PreparedBundle::from(&body));
                drop(body);
                drop(snapshot);
                Ok::<_, anyhow::Error>(prepared_storage::compact::CompactOwner::new((
                    (wire, bundle, bootstrap),
                    admission,
                )))
            })
            .await??;
        let (output, admission) = output.into_inner();
        drop(admission);
        Ok(output)
    }
}
