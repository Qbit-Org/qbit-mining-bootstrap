//! A landed CTV block with mature fanouts, shared by `refresh_liveness` and
//! `ctv_fanout_bookkeeping` (#579). Only node chain observations are
//! simulated; the candidate and its CTV artifacts come from the ledger's
//! public writers.
use super::Fixture;
use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AuditBundle, FoundBlock, PayoutPolicy};
use qbit_prism_server::{
    codec,
    ledger::{BlockObservation, Candidate, CandidateCtv, SignerKeys, WindowRef},
};
use serde_json::json;

/// Produce authentic candidate and CTV artifacts through the ledger's public
/// writers. Only node chain observations are simulated.
pub async fn seed_candidate(f: &Fixture, ctv: bool) -> Result<(Candidate, AuditBundle)> {
    let snapshot = f.a.ledger.snapshot(1_000_000).await?;
    let manifest_key = ManifestSigningKey::from_seed_hex(&f.a.config.manifest_seed)?;
    let ledger_key = ManifestSigningKey::from_seed_hex(&f.a.config.ledger_seed)?;
    let found = FoundBlock {
        block_height: 101,
        coinbase_value_sats: 5_000_000_000,
        network_difficulty: 1_000_000,
        anchor_job_issued_at_ms: snapshot.anchor_ms,
    };
    let options = CandidateCtv {
        direct_floor_sats: u64::MAX,
        settlement_config: qbit_prism::SettlementModeConfig {
            max_fanout_recipients_per_transaction: 1,
            ..Default::default()
        },
        fanout_fee_policy: Some(qbit_prism::FanoutFeeRatePolicy::new(1000, 12000)),
    };
    let bundle = if ctv {
        qbit_prism::build_audit_bundle_with_ctv_settlement_options(
            snapshot.shares.clone(),
            found,
            snapshot.prior_balances.clone(),
            PayoutPolicy::day_one_default(),
            options.direct_floor_sats,
            options.settlement_config,
            options.fanout_fee_policy,
            None,
            vec![],
            &manifest_key,
            &ledger_key,
        )?
    } else {
        qbit_prism::build_audit_bundle(
            snapshot.shares.clone(),
            found,
            snapshot.prior_balances.clone(),
            PayoutPolicy::day_one_default(),
            &manifest_key,
            &ledger_key,
        )?
    };
    let report = qbit_prism::verify_audit_bundle_with_ledger_public_key(
        &bundle,
        &ledger_key.public_key_hex(),
    )?;
    let mut block = vec![0u8; 80];
    block[..4].copy_from_slice(&0x20000000u32.to_le_bytes());
    block[4..36].fill(0xab);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&(chrono::Utc::now().timestamp() as u32).to_le_bytes());
    block[72..76].copy_from_slice(&0x207fffffu32.to_le_bytes());
    let hash = codec::hash_display(&codec::double_sha256(&block));
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    let candidate = Candidate {
        block_hash: hash,
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "startup-candidate".into(),
        payout_revision: snapshot.payout_revision,
        window: WindowRef::from_snapshot(&snapshot)?,
        bootstrap_share: None,
        found_block: bundle.found_block.clone(),
        payout_policy: bundle.payout_policy.clone(),
        ctv: ctv.then_some(options),
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
        leased: false,
        coinbase_suffix_hex: bundle
            .coinbase_script_sig_suffix_hex
            .clone()
            .unwrap_or_else(|| "00".repeat(12)),
        deferred_share: None,
        block_bytes: block,
        as_issued_balances: snapshot.prior_balances,
    };
    f.a.ledger.enqueue_candidate(candidate.clone()).await?;
    Ok((candidate, bundle))
}

/// A landed CTV block whose fanouts are mature. `observed` rows are already
/// confirmed, so an attempt is a check-only observation; otherwise every row
/// is still to be broadcast.
pub async fn mature_fanouts(f: &Fixture, observed: bool) -> Result<usize> {
    let (candidate, bundle) = seed_candidate(f, true).await?;
    let count = bundle
        .ctv_fanout_manifest_set
        .as_ref()
        .context("CTV fanouts")?
        .fanout_count as usize;
    ensure!(count > 2, "fixture needs multiple remaining rows");
    let claim =
        f.a.ledger
            .claim_candidate(60)
            .await?
            .context("parent claim")?
            .with_bundle(bundle);
    f.a.ledger
        .land_candidate(&claim, &f.a.config.ledger_public_key)
        .await?;
    f.a.ledger.finish_candidate(&claim, true, None).await?;
    f.a.ledger
        .reconcile_blocks_at_revision(
            &[BlockObservation {
                block_hash: candidate.block_hash.clone(),
                active: true,
            }],
            1101,
            f.a.ledger.payout_revision().await?,
        )
        .await?;
    f.node
        .set_tip(&"ab".repeat(32), &"cd".repeat(32), 1101, "02");
    f.node.set_reply(
        "getblockheader",
        json!([candidate.block_hash]),
        json!({"previousblockhash":"cd".repeat(32),"height":101,"confirmations":1001}),
    );
    f.node
        .set_reply("getblockhash", json!([101]), json!(candidate.block_hash));
    f.node
        .set_reply("getblockhash", json!([1101]), json!("ab".repeat(32)));
    if observed {
        // Recovery of already-confirmed fanouts exercises successful
        // observations and durable recheck scheduling without needing a
        // wallet or real qbitd.
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='confirmed',confirmed_block_hash=$1,confirmed_block_height=1101,confirmed_depth=1")
            .bind("ab".repeat(32)).execute(f.pool()).await?;
    } else {
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='broadcastable'")
            .execute(f.pool())
            .await?;
    }
    f.a.refresh_once().await?;
    Ok(count)
}
