// Independent verification of PRISM audit trust-boundary regressions. These tests
// model a dishonest operator self-signing manipulated data and verify that the
// bundle is rejected against the independently trusted ledger writer key.
//
//   cargo test -p qbit-prism --test audit_trust_boundary -- --nocapture

use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    apply_payout_policy, build_audit_bundle, build_prism_reward_manifest, compute_prism_window,
    verify_audit_bundle, verify_audit_bundle_against_coinbase_tx_hex, AcceptedShare, FoundBlock,
    PayoutPolicy, PrismError,
};

fn p2mr(b: u8) -> String {
    hex::encode([b; 32])
}

#[allow(clippy::too_many_arguments)]
fn share(
    seq: u64,
    miner: &str,
    ok: &str,
    b: u8,
    diff: u128,
    nd: u128,
    job: i64,
    acc: i64,
) -> AcceptedShare {
    AcceptedShare {
        share_seq: seq,
        share_id: format!("share-{seq}"),
        miner_id: miner.to_string(),
        order_key: ok.to_string(),
        p2mr_program_hex: p2mr(b),
        share_difficulty: diff,
        network_difficulty: nd,
        template_height: 100,
        job_id: format!("job-{job}"),
        job_issued_at_ms: job,
        accepted_at_ms: acc,
        ntime: 1_800_000_000,
    }
}

fn operator_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap()
}

fn ledger_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap()
}

fn attacker_ledger_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"44".repeat(32)).unwrap()
}

fn ledger_public_key_hex() -> String {
    ledger_key().public_key_hex()
}

// ===========================================================================
// Audit trust boundary: a dishonest operator signs the manipulated set with
// an untrusted key. Verifiers must reject it unless it is signed by the
// independent ledger writer key they trust.
// ===========================================================================
#[test]
fn share_omission_rejected_by_trusted_ledger_key() {
    let nd = 1u128;
    let found = FoundBlock {
        block_height: 200,
        coinbase_value_sats: 500_000_000,
        network_difficulty: nd,
        anchor_job_issued_at_ms: 1001,
    };
    // Reality: two equal miners. Operator drops the victim entirely.
    let cheating_shares = vec![share(2, "insider", "02", 2, 1, nd, 1001, 1001)];

    let same_key_attempt = build_audit_bundle(
        cheating_shares.clone(),
        found.clone(),
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &operator_key(),
        &operator_key(),
    );
    assert!(matches!(
        same_key_attempt,
        Err(PrismError::LedgerAttestationKeyReuse)
    ));

    // The dishonest operator can still self-sign a manipulated bundle with an
    // attacker-controlled ledger key, but verifiers do not trust that key.
    let cheating = build_audit_bundle(
        cheating_shares,
        found,
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &operator_key(),
        &attacker_ledger_key(),
    )
    .unwrap();

    // The operator mined exactly this truncated split, so the on-chain coinbase
    // matches the bundle — this is the CLI's strongest check.
    let onchain_hex = cheating
        .signed_coinbase_manifest
        .manifest
        .coinbase_tx_hex
        .clone();

    let plain = verify_audit_bundle(&cheating, &ledger_public_key_hex());
    let with_hex = verify_audit_bundle_against_coinbase_tx_hex(
        &cheating,
        &onchain_hex,
        &ledger_public_key_hex(),
    );
    let victim_present = cheating
        .payout_policy_manifest
        .accounts
        .iter()
        .any(|a| a.recipient_id == "victim");

    println!("[audit-trust] victim present in bundle      = {victim_present}");
    println!(
        "[audit-trust] verify_audit_bundle ok        = {}",
        plain.is_ok()
    );
    println!(
        "[audit-trust] verify + on-chain hex ok      = {}",
        with_hex.is_ok()
    );

    assert!(!victim_present, "victim was dropped");
    assert!(plain.is_err(), "trusted-key verifier rejects omission");
    assert!(
        with_hex.is_err(),
        "CLI path with trusted ledger key rejects omission"
    );
}

#[test]
fn fabricated_prior_rejected_by_trusted_ledger_key() {
    let nd = 1u128;
    let found = FoundBlock {
        block_height: 200,
        coinbase_value_sats: 500_000_000,
        network_difficulty: nd,
        anchor_job_issued_at_ms: 1001,
    };
    let shares = vec![
        share(1, "victim", "01", 1, 1, nd, 1000, 1000),
        share(2, "insider", "02", 2, 1, nd, 1001, 1001),
    ];
    // Operator invents a debt for the victim — and signs the attestation over
    // its own fabricated prior_balances (the natural build path).
    let fake_prior = vec![qbit_prism::CarryForwardBalance {
        recipient_id: "victim".to_string(),
        order_key: "01".to_string(),
        p2mr_program_hex: p2mr(1),
        balance_sats: -200_000_000,
    }];
    let bundle = build_audit_bundle(
        shares,
        found,
        fake_prior,
        PayoutPolicy::day_one_default(),
        &operator_key(),
        &attacker_ledger_key(),
    );
    assert!(
        matches!(
            bundle,
            Err(PrismError::PayoutExceedsCandidateBalance {
                coinbase_value_sats: 500_000_000,
                selected_candidate_balance_sats: 300_000_000
            })
        ),
        "fabricated debt that would force overpay is rejected before signing"
    );
}

// ===========================================================================
// Genuinely-fixed items: confirm they really hold under the real path.
// ===========================================================================

// Insolvent dust blocks fail before negative carry can be emitted.
#[test]
fn insolvent_dust_block_is_rejected() {
    let v = 500_000_000u64;
    let nd = 2_000_001u128;
    let policy = PayoutPolicy::day_one_default();
    let shares = vec![
        share(1, "whale", "00", 1, 10_000_000, nd, 1000, 1000),
        share(2, "m1", "01", 2, 1, nd, 1001, 1001),
        share(3, "m2", "02", 3, 1, nd, 1002, 1002),
        share(4, "m3", "03", 4, 1, nd, 1003, 1003),
        share(5, "m4", "04", 5, 1, nd, 1004, 1004),
        share(6, "m5", "05", 6, 1, nd, 1005, 1005),
    ];
    let found = FoundBlock {
        block_height: 200,
        coinbase_value_sats: v,
        network_difficulty: nd,
        anchor_job_issued_at_ms: 1005,
    };
    let reward = build_prism_reward_manifest(&shares, &found).unwrap();
    let err = apply_payout_policy(&reward, &[], &policy).unwrap_err();

    println!("[dust-policy] insolvent dust block rejected = {err:?}");
    assert!(matches!(
        err,
        PrismError::PayoutExceedsCandidateBalance {
            coinbase_value_sats: 500_000_000,
            selected_candidate_balance_sats: 499_999_750
        }
    ));
}

// Rust window selection matches the Python/SQL job+accept contract.
#[test]
fn window_converges_with_python_sql() {
    let anchor = 1001i64;
    let nd = 10u128;
    let shares = vec![
        share(1, "miner-a", "01", 1, 10, nd, 1000, 1000),
        share(2, "miner-b", "02", 2, 20, nd, 1001, 1001),
        share(3, "miner-late", "03", 3, 1000, nd, 1000, 5000), // accepted after anchor
    ];
    let found = FoundBlock {
        block_height: 101,
        coinbase_value_sats: 500_000_000,
        network_difficulty: nd,
        anchor_job_issued_at_ms: anchor,
    };
    let rust: Vec<_> = compute_prism_window(&shares, &found)
        .unwrap()
        .entitlements
        .into_iter()
        .map(|e| (e.recipient_id, e.weight))
        .collect();
    let py_sql: Vec<AcceptedShare> = shares
        .iter()
        .filter(|s| s.job_issued_at_ms <= anchor && s.accepted_at_ms <= anchor)
        .cloned()
        .collect();
    let py: Vec<_> = compute_prism_window(&py_sql, &found)
        .unwrap()
        .entitlements
        .into_iter()
        .map(|e| (e.recipient_id, e.weight))
        .collect();
    println!("[window] rust={rust:?} python/sql={py:?}");
    assert_eq!(
        rust, py,
        "Rust now matches the documented accept-time contract"
    );
    assert!(rust.iter().all(|(m, _)| m != "miner-late"));
}
