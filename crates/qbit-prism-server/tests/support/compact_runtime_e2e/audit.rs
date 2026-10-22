//! Independent native artifact reconstruction from the durable original inputs.
//! PreparedBundle is submission metadata, not a serialized audit artifact.
use super::*;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism_server::{
    codec,
    ledger::{BalanceSource, SignerKeys, StoredCompactPrepared},
};
use sha2::{Digest, Sha256};

pub async fn prove_original_audit(
    f: &Fixture,
    job: &MiningJob<JobContext>,
    stored: StoredCompactPrepared,
) -> Result<()> {
    let window =
        f.a.ledger
            .read_window(&stored.record.window, BalanceSource::AsIssued)
            .await?;
    let wire = job.wire.clone();
    let worker = job.context.worker.clone();
    let bootstrap = job.context.bootstrap_share.clone();
    let metadata = job.context.bundle.clone();
    let manifest_seed = f.a.config.manifest_seed.clone();
    let ledger_seed = f.a.config.ledger_seed.clone();
    // The test owns the real rows and all original inputs until this blocking
    // builder and its cleanup finish. No runtime-private reconstruction helper
    // or current configuration supplies a payout, suffix, policy or CTV option.
    tokio::task::spawn_blocking(move || -> Result<()> {
        let record = &stored.record;
        ensure!(
            serde_json::to_value(&window.prior_balances)?
                == serde_json::to_value(&stored.prior_balances)?,
            "AsIssued balance rows differ from the retained original blob"
        );
        let network =
            codec::scaled_target_difficulty(&codec::target_from_compact(codec::parse_u32_hex(
                stored.template["bits"]
                    .as_str()
                    .context("original bits missing")?,
            )?)?)?;
        let found = qbit_prism::FoundBlock {
            block_height: stored.template["height"]
                .as_u64()
                .context("original height missing")?,
            coinbase_value_sats: stored.template["coinbasevalue"]
                .as_u64()
                .context("original reward missing")?,
            network_difficulty: network,
            anchor_job_issued_at_ms: record.window.anchor_ms,
        };
        ensure!(
            serde_json::to_value(&found)? == serde_json::to_value(&metadata.found_block)?,
            "issued block economics differ from original stored inputs"
        );
        let shares = if let Some(bootstrap) = &bootstrap {
            ensure!(
                record.window.shares.is_none() && window.shares.is_empty(),
                "bootstrap used a nonempty retained window"
            );
            ensure!(
                bootstrap.miner_id == worker.payout_address
                    && bootstrap.p2mr_program_hex == worker.p2mr_program_hex
                    && bootstrap.job_issued_at_ms == record.window.anchor_ms
                    && bootstrap.network_difficulty == network,
                "original bootstrap share does not describe its issued worker/template"
            );
            std::slice::from_ref(bootstrap)
        } else {
            ensure!(
                record.window.shares.is_some(),
                "non-bootstrap job lost its retained window"
            );
            window.shares.as_slice()
        };
        let manifest_key = ManifestSigningKey::from_seed_hex(&manifest_seed)?;
        let ledger_key = ManifestSigningKey::from_seed_hex(&ledger_seed)?;
        ensure!(
            record.signer_keys == SignerKeys::of(&manifest_key, &ledger_key),
            "fixture signing seeds do not match original recorded signers"
        );
        let witnesses =
            codec::witness_merkle_leaves_hex(&codec::transactions_from_template(&stored.template)?);
        let body = match &record.ctv {
            Some(ctv) => qbit_prism::build_audit_bundle_body_with_ctv_settlement_options(
                shares,
                found,
                stored.prior_balances.clone(),
                record.payout_policy.clone(),
                ctv.direct_floor_sats,
                ctv.settlement_config,
                ctv.fanout_fee_policy,
                Some(record.coinbase_suffix_hex.clone()),
                witnesses,
                &manifest_key,
                &ledger_key,
            )?,
            None => qbit_prism::build_audit_bundle_body_with_coinbase_options(
                shares,
                found,
                stored.prior_balances.clone(),
                record.payout_policy.clone(),
                Some(record.coinbase_suffix_hex.clone()),
                witnesses,
                &manifest_key,
                &ledger_key,
            )?,
        };
        if let Some(hashes) = &record.audit_hashes {
            ensure!(
                hashes.audit_bundle_sha256
                    == hex::encode(Sha256::digest(
                        qbit_prism::canonical_audit_bundle_bytes_from_parts(&body, shares)?
                    )),
                "native audit reconstruction differs from the original stored hash"
            );
            ensure!(
                hashes.coinbase_manifest_sha256
                    == hex::encode(Sha256::digest(qbit_pool_builder::canonical_manifest_bytes(
                        &body.signed_coinbase_manifest.manifest
                    )?)),
                "native manifest reconstruction differs from the original stored hash"
            );
        } else {
            ensure!(
                bootstrap.is_some(),
                "nonempty original record omitted audit hashes"
            );
        }
        ensure!(
            body.coinbase_script_sig_suffix_hex == metadata.coinbase_script_sig_suffix_hex,
            "original coinbase suffix changed"
        );
        ensure!(
            body.ctv_fanout_manifest_set.is_some() == metadata.ctv_fanout_manifest_set.is_some(),
            "original CTV presence changed"
        );
        let expected = codec::Job::from_manifest(
            "independent-audit".into(),
            &stored.template,
            &body.signed_coinbase_manifest.manifest,
            "00000000",
            wire.extranonce2_size,
            1.0,
            0.0,
            false,
        )?;
        ensure!(
            expected.coinb1 == wire.coinb1
                && expected.coinb2 == wire.coinb2
                && expected.full_coinbase_prefix == wire.full_coinbase_prefix
                && expected.full_coinbase_suffix == wire.full_coinbase_suffix
                && expected.merkle_branch == wire.merkle_branch,
            "issued coinbase differs from the independent original native audit"
        );
        Ok(())
    })
    .await?
}
