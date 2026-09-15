//! Borrow the admitted original window for both refresh and reconstruction.
use super::*;

/// The caller owns the blocking executor admission through construction and
/// cleanup. This function never takes another slot or selects current inputs.
pub(super) fn build_body(
    config: &Config,
    snapshot: &Snapshot,
    template: &Value,
    bootstrap: Option<Worker>,
    suffix: String,
    inputs: BundleInputs,
) -> Result<(qbit_prism::AuditBundleBody, Option<AcceptedShare>)> {
    let network = codec::scaled_target_difficulty(&codec::target_from_compact(
        codec::parse_u32_hex(template["bits"].as_str().context("missing bits")?)?,
    )?)?;
    let found = FoundBlock {
        block_height: template["height"].as_u64().context("missing height")?,
        coinbase_value_sats: template["coinbasevalue"]
            .as_u64()
            .context("missing coinbase value")?,
        network_difficulty: network,
        anchor_job_issued_at_ms: snapshot.anchor_ms,
    };
    let bootstrap_share = if let Some(worker) = bootstrap {
        Some(AcceptedShare {
            share_seq: 1,
            share_id: "bootstrap-share".into(),
            miner_id: worker.payout_address.clone(),
            order_key: worker.payout_address,
            p2mr_program_hex: worker.p2mr_program_hex,
            share_difficulty: network,
            network_difficulty: network,
            template_height: template_parent_height(found.block_height)?,
            job_id: "bootstrap-job".into(),
            job_issued_at_ms: snapshot.anchor_ms,
            accepted_at_ms: snapshot.anchor_ms,
            ntime: template["curtime"]
                .as_u64()
                .context("missing time")?
                .try_into()?,
            credit_policy: None,
        })
    } else {
        None
    };
    let shares = match &bootstrap_share {
        Some(share) => std::slice::from_ref(share),
        None => &snapshot.shares,
    };
    let witnesses = codec::witness_merkle_leaves_hex(&codec::transactions_from_template(template)?);
    let manifest_key = ManifestSigningKey::from_seed_hex(&config.manifest_seed)?;
    let ledger_key = ManifestSigningKey::from_seed_hex(&config.ledger_seed)?;
    ensure!(
        inputs.signer_keys == SignerKeys::of(&manifest_key, &ledger_key),
        "job inputs name signing keys other than this frontend's"
    );
    // Prior-only recipients remain in the payout universe, including
    // during bootstrap after an empty reward window.
    let bundle = if let Some(ctv) = inputs.ctv {
        qbit_prism::build_audit_bundle_body_with_ctv_settlement_options(
            shares,
            found,
            snapshot.prior_balances.clone(),
            inputs.payout_policy,
            ctv.direct_floor_sats,
            ctv.settlement_config,
            ctv.fanout_fee_policy,
            Some(suffix),
            witnesses,
            &manifest_key,
            &ledger_key,
        )?
    } else {
        qbit_prism::build_audit_bundle_body_with_coinbase_options(
            shares,
            found,
            snapshot.prior_balances.clone(),
            inputs.payout_policy,
            Some(suffix),
            witnesses,
            &manifest_key,
            &ledger_key,
        )?
    };
    Ok((bundle, bootstrap_share))
}
