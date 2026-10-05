//! #657: the capture form a fenced share append prepares is, byte for byte,
//! what an enqueue of the same candidate with its share deferred writes:
//! the document, its digest and the deferred share row a capture at the
//! submit check (`capture_stale`) enqueues. So the outbox, the claim's
//! authentication and the confirmation's deferred credit treat a block the
//! fence captured exactly as one the submit check captured. Preparing it
//! leaves the credited form unchanged.
use super::*;
use crate::codec;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{FoundBlock, PayoutPolicy};
use serde_json::json;

fn share(seq: u64, share_id: &str) -> AcceptedShare {
    AcceptedShare {
        share_seq: seq,
        share_id: share_id.into(),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "ab".repeat(32),
        share_difficulty: 1_000,
        network_difficulty: 100_000,
        template_height: 100,
        job_id: "capture-form-job".into(),
        job_issued_at_ms: 900,
        accepted_at_ms: 900,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

/// A block found on a one-share window at revision 7, with its as-issued
/// balances, the way `submission_candidate` builds one.
fn found_candidate() -> Result<Candidate> {
    let snapshot = Snapshot {
        anchor_ms: 1_000,
        share_seq: 1,
        payout_revision: 7,
        shares: vec![share(1, "miner.rig:window")],
        prior_balances: vec![],
    };
    let manifest_key = ManifestSigningKey::from_seed_hex(&"11".repeat(32))?;
    let ledger_key = ManifestSigningKey::from_seed_hex(&"22".repeat(32))?;
    let bundle = qbit_prism::build_audit_bundle_with_coinbase_options(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        Some("00".repeat(12)),
        vec![],
        &manifest_key,
        &ledger_key,
    )?;
    let template = json!({"version":0x20000000u32,"bits":"207fffff","curtime":1_800_000_000u32,
        "previousblockhash":"aa".repeat(32),"transactions":[]});
    let job = codec::Job::from_manifest(
        "capture-form".into(),
        &template,
        &bundle.signed_coinbase_manifest.manifest,
        "00000000",
        8,
        1e-12,
        0.0,
        true,
    )?;
    let proof = (0..20_000u32)
        .find_map(|nonce| {
            let proof = job
                .assemble_submission(
                    &"00".repeat(8),
                    &format!("{:08x}", job.ntime),
                    &format!("{nonce:08x}"),
                    None,
                    0,
                )
                .ok()?;
            proof.block_pass.then_some(proof)
        })
        .context("no block proof in the nonce budget")?;
    let block_bytes = hex::decode(&proof.block_hex)?;
    Ok(Candidate {
        block_hash: proof.block_hash_hex,
        block_sha256: Candidate::block_digest_hex(&block_bytes),
        job_id: job.job_id,
        payout_revision: snapshot.payout_revision,
        window: WindowRef::from_snapshot(&snapshot)?,
        bootstrap_share: None,
        found_block: bundle.found_block.clone(),
        payout_policy: PayoutPolicy::day_one_default(),
        ctv: None,
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
        leased: false,
        coinbase_suffix_hex: "00".repeat(12),
        deferred_share: None,
        block_bytes,
        as_issued_balances: snapshot.prior_balances,
    })
}

#[tokio::test]
async fn a_fenced_candidates_capture_form_is_the_capture_its_submit_check_would_enqueue(
) -> Result<()> {
    let candidate = found_candidate()?;
    // The solver share as the submit path builds it: not yet sequenced.
    let mut solver = share(0, &format!("miner.rig:{}", candidate.block_hash));
    solver.accepted_at_ms = 0;
    let fenced = prepare_fenced_candidate(candidate.clone(), Some(42), solver.clone()).await?;
    let mut deferred = candidate.clone();
    deferred.deferred_share = Some(solver);
    let at_check = prepare_candidate_observed(deferred, Some(42)).await?;
    let capture = fenced
        .capture
        .as_ref()
        .context("a fenced candidate was prepared without its capture form")?;
    assert_eq!(capture.document, at_check.document);
    assert_eq!(capture.sha256, at_check.sha256);
    assert_eq!(Some(&capture.deferred), at_check.deferred.as_ref());
    assert_eq!(fenced.snapshot, at_check.snapshot);
    // The credited form is what an unfenced preparation produces.
    let credited = prepare_candidate_observed(candidate, Some(42)).await?;
    assert_eq!(fenced.document, credited.document);
    assert_eq!(fenced.sha256, credited.sha256);
    assert!(fenced.deferred.is_none() && credited.deferred.is_none());
    assert!(credited.capture.is_none());
    assert_ne!(fenced.sha256, capture.sha256);
    Ok(())
}
