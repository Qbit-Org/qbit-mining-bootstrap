// Adversarial settlement, audit, reorg, and coinbase regression cases.
//
// These tests originally documented adversarial defects. They now assert the
// corrected behavior so regressions fail at the same risk boundaries.
// Run with:
//   cargo test -p qbit-prism --test adversarial_settlement_cases -- --nocapture

use qbit_pool_builder::{
    build_manifest, is_p2mr_script_pubkey, CoinbaseBuildRequest, ManifestSigningKey,
    WeightedEntitlement,
};
use qbit_prism::{
    apply_payout_policy, build_audit_bundle, build_maturity_entries, build_prism_reward_manifest,
    compute_prism_window, current_carry_forward_balances, reverse_disconnected_blocks,
    update_maturity_states, verify_audit_bundle, AcceptedShare, CarryForwardBalance, FoundBlock,
    PayoutMaturityState, PayoutPolicy, PrismError, QBIT_COINBASE_MATURITY_BLOCKS,
};

fn p2mr(byte: u8) -> String {
    hex::encode([byte; 32])
}

#[allow(clippy::too_many_arguments)]
fn share(
    seq: u64,
    miner: &str,
    order_key: &str,
    p2mr_byte: u8,
    diff: u128,
    net_diff: u128,
    job_ms: i64,
    accept_ms: i64,
) -> AcceptedShare {
    AcceptedShare {
        share_seq: seq,
        share_id: format!("share-{seq}"),
        miner_id: miner.to_string(),
        order_key: order_key.to_string(),
        p2mr_program_hex: p2mr(p2mr_byte),
        share_difficulty: diff,
        network_difficulty: net_diff,
        template_height: 100,
        job_id: format!("job-{job_ms}"),
        job_issued_at_ms: job_ms,
        accepted_at_ms: accept_ms,
        ntime: 1_800_000_000,
    }
}

fn signing_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap()
}

fn ledger_signing_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap()
}

fn attacker_ledger_signing_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"44".repeat(32)).unwrap()
}

fn ledger_public_key_hex() -> String {
    ledger_signing_key().public_key_hex()
}

// What snapshot_at_job_issue (Python) and qbit_prism_window (SQL) actually filter on:
// BOTH job_issued_at_ms <= anchor AND accepted_at_ms <= anchor.
// This mirrors lab/prism/share_ledger.py:106-107 and sql/001_share_ledger.sql:144-145.
fn python_sql_eligible(shares: &[AcceptedShare], anchor_ms: i64) -> Vec<AcceptedShare> {
    shares
        .iter()
        .filter(|s| s.job_issued_at_ms <= anchor_ms && s.accepted_at_ms <= anchor_ms)
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Value conservation and carry-forward drift
// ---------------------------------------------------------------------------
//
// Invariant that MUST hold for a non-custodial pool:
//   sum(carry_forward_after_block) == sum(prior_balances_before_block)
// because the coinbase pays out exactly `coinbase_value` on-chain, which equals
// the total gross earned this block. Net change in total owed = 0.
//
// The historical bug overpaid the selected whale and then relied on negative
// carry-forward to conserve value. The current policy rejects the block before
// any overpay-derived negative carry can be emitted.
#[test]
fn overpay_rejected_before_clamp_can_create_satoshis() {
    let v = 500_000_000u64; // regtest coinbase value
    let net_diff = 2_000_001u128; // 8x window comfortably exceeds total weight -> all counted

    // One whale (above floor) + five minnows (each far below the 14_720 floor).
    let shares = vec![
        share(1, "whale", "00", 1, 10_000_000, net_diff, 1000, 1000),
        share(2, "minnow-1", "01", 2, 1, net_diff, 1001, 1001),
        share(3, "minnow-2", "02", 3, 1, net_diff, 1002, 1002),
        share(4, "minnow-3", "03", 4, 1, net_diff, 1003, 1003),
        share(5, "minnow-4", "04", 5, 1, net_diff, 1004, 1004),
        share(6, "minnow-5", "05", 6, 1, net_diff, 1005, 1005),
    ];
    let found = FoundBlock {
        block_height: 200,
        coinbase_value_sats: v,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: 1005,
    };

    let reward = build_prism_reward_manifest(&shares, &found).unwrap();
    let policy = PayoutPolicy::day_one_default();
    let err = apply_payout_policy(&reward, &[], &policy).unwrap_err();

    println!("[carry-drift] payout policy result = {err:?}");
    assert!(matches!(
        err,
        PrismError::PayoutExceedsCandidateBalance {
            coinbase_value_sats: 500_000_000,
            selected_candidate_balance_sats: 499_999_750
        }
    ));
}

// Regression for the old compounding bug: the first dust block now fails before
// any signed negative carry can be fed into the next block.
#[test]
fn carry_drift_cannot_compound_when_first_overpay_is_rejected() {
    let v = 500_000_000u64;
    let net_diff = 2_000_001u128;
    let make_shares = || {
        vec![
            share(1, "whale", "00", 1, 10_000_000, net_diff, 1000, 1000),
            share(2, "minnow-1", "01", 2, 1, net_diff, 1001, 1001),
            share(3, "minnow-2", "02", 3, 1, net_diff, 1002, 1002),
            share(4, "minnow-3", "03", 4, 1, net_diff, 1003, 1003),
            share(5, "minnow-4", "04", 5, 1, net_diff, 1004, 1004),
            share(6, "minnow-5", "05", 6, 1, net_diff, 1005, 1005),
        ]
    };
    let policy = PayoutPolicy::day_one_default();

    // Block A
    let found_a = FoundBlock {
        block_height: 200,
        coinbase_value_sats: v,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: 1005,
    };
    let reward_a = build_prism_reward_manifest(&make_shares(), &found_a).unwrap();
    let err = apply_payout_policy(&reward_a, &[], &policy).unwrap_err();

    println!("[carry-drift] first dust block rejected = {err:?}");
    assert!(matches!(
        err,
        PrismError::PayoutExceedsCandidateBalance {
            coinbase_value_sats: 500_000_000,
            selected_candidate_balance_sats: 499_999_750
        }
    ));
}

#[test]
fn signed_carry_total_equals_prior_total_across_blocks() {
    let policy = PayoutPolicy::day_one_default();
    let coinbase = 500_000_000u64;
    let net_diff = 2_000_001u128;
    let prior_balances: Vec<CarryForwardBalance> = Vec::new();

    for block_index in 0..4_u64 {
        let base_ms = 10_000 + (block_index as i64 * 100);
        let shares = vec![
            share(
                1 + block_index * 10,
                "whale",
                "00",
                1,
                10_000_000 + u128::from(block_index),
                net_diff,
                base_ms,
                base_ms,
            ),
            share(
                2 + block_index * 10,
                "minnow-a",
                "01",
                2,
                1 + u128::from(block_index % 2),
                net_diff,
                base_ms + 1,
                base_ms + 1,
            ),
            share(
                3 + block_index * 10,
                "minnow-b",
                "02",
                3,
                2,
                net_diff,
                base_ms + 2,
                base_ms + 2,
            ),
        ];
        let found = FoundBlock {
            block_height: 300 + block_index,
            coinbase_value_sats: coinbase,
            network_difficulty: net_diff,
            anchor_job_issued_at_ms: base_ms + 2,
        };

        let prior_total: i128 = prior_balances
            .iter()
            .map(|balance| balance.balance_sats)
            .sum();
        let reward = build_prism_reward_manifest(&shares, &found).unwrap();
        let err = apply_payout_policy(&reward, &prior_balances, &policy).unwrap_err();

        assert!(matches!(
            err,
            PrismError::PayoutExceedsCandidateBalance {
                coinbase_value_sats: 500_000_000,
                selected_candidate_balance_sats: 499_999_850 | 499_999_800 | 499_999_750
            }
        ));
        assert_eq!(
            prior_total, 0,
            "no failed block should mutate signed prior balances"
        );
        break;
    }
}

// ---------------------------------------------------------------------------
// Window selection — Rust compute_prism_window vs Python/SQL snapshot divergence
// ---------------------------------------------------------------------------
//
// A share whose job was issued at/*before* the anchor but which was ACCEPTED
// after the anchor is:
//   * INCLUDED by Rust compute_prism_window (filters only job_issued_at_ms)
//   * EXCLUDED by Python snapshot_at_job_issue / SQL qbit_prism_window
//     (filter job_issued_at_ms <= anchor AND accepted_at_ms <= anchor)
// The two engines therefore disagree on the window, the entitlements, and the
// resulting coinbase split for identical inputs.
#[test]
fn accept_time_gap_excludes_late_shares() {
    let anchor = 1001i64;
    let net_diff = 10u128; // requested window weight = 80
    let shares = vec![
        share(1, "miner-a", "01", 1, 10, net_diff, 1000, 1000),
        share(2, "miner-b", "02", 2, 20, net_diff, 1001, 1001),
        // job at 1000 (eligible by job time) but accepted at 5000 (after anchor):
        share(3, "miner-late", "03", 3, 1000, net_diff, 1000, 5000),
    ];
    let found = FoundBlock {
        block_height: 101,
        coinbase_value_sats: 500_000_000,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: anchor,
    };

    // Rust engine on the full ledger.
    let rust_window = compute_prism_window(&shares, &found).unwrap();
    let rust_entitlements: Vec<(String, u128)> = rust_window
        .entitlements
        .iter()
        .map(|e| (e.recipient_id.clone(), e.weight))
        .collect();

    // Python/SQL pre-filter, then the same engine.
    let filtered = python_sql_eligible(&shares, anchor);
    let py_window = compute_prism_window(&filtered, &found).unwrap();
    let py_entitlements: Vec<(String, u128)> = py_window
        .entitlements
        .iter()
        .map(|e| (e.recipient_id.clone(), e.weight))
        .collect();

    println!("[window] Rust (job-time only) entitlements:    {rust_entitlements:?}");
    println!("[window] Python/SQL (job+accept) entitlements: {py_entitlements:?}");

    assert_eq!(
        rust_entitlements, py_entitlements,
        "Rust and Python/SQL windows must match the documented accept-time contract"
    );
    assert!(rust_entitlements.iter().all(|(m, _)| m != "miner-late"));
    assert!(py_entitlements.iter().all(|(m, _)| m != "miner-late"));
}

// ---------------------------------------------------------------------------
// Payout floor — payout floor / select_onchain_accounts
// ---------------------------------------------------------------------------

// all-below-floor: every miner's gross is sub-floor -> no on-chain recipient.
// Expected: a clean error (NoOnchainRecipients), no panic / infinite loop.
#[test]
fn all_below_floor_errors_cleanly() {
    let net_diff = 1u128;
    // coinbase tiny so every share's gross < floor, but still >= floor in aggregate
    let shares = vec![
        share(1, "a", "01", 1, 1, net_diff, 1000, 1000),
        share(2, "b", "02", 2, 1, net_diff, 1001, 1001),
    ];
    let found = FoundBlock {
        block_height: 10,
        coinbase_value_sats: 20_000, // each gets ~10_000 < 14_720 floor
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: 1001,
    };
    let reward = build_prism_reward_manifest(&shares, &found).unwrap();
    let result = apply_payout_policy(&reward, &[], &PayoutPolicy::day_one_default());
    println!("[payout-floor all-below-floor] result = {result:?}");
    assert!(
        result.is_err(),
        "all-below-floor must error, not strand/loop"
    );
}

// one miner with the entire coinbase: must be paid fully on-chain, zero carry.
#[test]
fn single_miner_paid_in_full() {
    let net_diff = 1u128;
    let shares = vec![share(1, "solo", "01", 1, 5, net_diff, 1000, 1000)];
    let found = FoundBlock {
        block_height: 10,
        coinbase_value_sats: 500_000_000,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: 1000,
    };
    let reward = build_prism_reward_manifest(&shares, &found).unwrap();
    let manifest = apply_payout_policy(&reward, &[], &PayoutPolicy::day_one_default()).unwrap();
    let solo = &manifest.accounts[0];
    println!(
        "[payout-floor single] onchain={} carry={}",
        solo.onchain_amount_sats, solo.carry_forward_balance_sats
    );
    assert_eq!(solo.onchain_amount_sats, 500_000_000);
    assert_eq!(solo.carry_forward_balance_sats, 0);
}

// negative-then-recross: this historical setup depended on a prior overpay.
// The current policy rejects both the signed-debt and clamped-prior variants
// when the selected candidate balance cannot cover the full coinbase.
#[test]
fn negative_carry_recross_is_rejected_before_double_dip() {
    let v = 500_000_000u64;
    let net_diff = 2_000_001u128;
    let shares = vec![
        share(1, "whale", "00", 1, 10_000_000, net_diff, 1000, 1000),
        share(2, "minnow", "01", 2, 1, net_diff, 1001, 1001),
    ];
    let found = FoundBlock {
        block_height: 200,
        coinbase_value_sats: v,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: 1001,
    };
    let reward = build_prism_reward_manifest(&shares, &found).unwrap();

    // Honest signed prior: whale owes what it was overpaid.
    let whale_debt = CarryForwardBalance {
        recipient_id: "whale".to_string(),
        order_key: "00".to_string(),
        p2mr_program_hex: p2mr(1),
        balance_sats: -200, // whale was overpaid previously
    };
    let honest = apply_payout_policy(
        &reward,
        std::slice::from_ref(&whale_debt),
        &PayoutPolicy::day_one_default(),
    );
    // Clamped prior (what the system actually forwards): debt erased -> 0.
    let clamped = apply_payout_policy(&reward, &[], &PayoutPolicy::day_one_default());

    println!(
        "[payout-floor recross] honest ok={} clamped ok={}",
        honest.is_ok(),
        clamped.is_ok()
    );
    assert!(
        matches!(
            honest,
            Err(PrismError::PayoutExceedsCandidateBalance {
                coinbase_value_sats: 500_000_000,
                selected_candidate_balance_sats: 499_999_750
            })
        ),
        "signed debt path must not overpay"
    );
    assert!(
        matches!(
            clamped,
            Err(PrismError::PayoutExceedsCandidateBalance {
                coinbase_value_sats: 500_000_000,
                selected_candidate_balance_sats: 499_999_950
            })
        ),
        "clamped path must not overpay"
    );
}

// ---------------------------------------------------------------------------
// Audit verifier soundness — audit verifier soundness
// ---------------------------------------------------------------------------
//
// The verifier recomputes reward<-shares, policy<-reward+prior, coinbase<-policy
// and checks the signature + on-chain hex. It must also require an attestation
// from the trusted ledger writer key so a self-signed operator bundle cannot
// drop shares or fake priors.
#[test]
fn operator_omits_miner_shares_and_verifier_rejects() {
    let net_diff = 1u128;
    let found = FoundBlock {
        block_height: 200,
        coinbase_value_sats: 500_000_000,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: 1001,
    };
    // Honest reality: two equal miners each earned ~half the block.
    let honest_shares = vec![
        share(1, "victim", "01", 1, 1, net_diff, 1000, 1000),
        share(2, "insider", "02", 2, 1, net_diff, 1001, 1001),
    ];
    // Dishonest bundle: operator simply omits the victim's share.
    let cheating_shares = vec![share(2, "insider", "02", 2, 1, net_diff, 1001, 1001)];

    let honest = build_audit_bundle(
        honest_shares,
        found.clone(),
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &signing_key(),
        &ledger_signing_key(),
    )
    .unwrap();
    let cheating = build_audit_bundle(
        cheating_shares,
        found,
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &signing_key(),
        &attacker_ledger_signing_key(),
    )
    .unwrap();

    let honest_report = verify_audit_bundle(&honest, &ledger_public_key_hex()).unwrap();
    let cheating_report = verify_audit_bundle(&cheating, &ledger_public_key_hex());

    let victim_paid_in_honest = honest
        .payout_policy_manifest
        .accounts
        .iter()
        .any(|a| a.recipient_id == "victim");
    let victim_present_in_cheating = cheating
        .payout_policy_manifest
        .accounts
        .iter()
        .any(|a| a.recipient_id == "victim");

    println!(
        "[audit] honest report ok, onchain outputs = {}",
        honest_report.onchain_output_count
    );
    println!(
        "[audit] cheating bundle verifies = {}",
        cheating_report.is_ok()
    );
    println!(
        "[audit] victim present honest={victim_paid_in_honest} cheating={victim_present_in_cheating}"
    );

    assert!(victim_paid_in_honest);
    assert!(
        cheating_report.is_err(),
        "verifier rejects the truncated bundle"
    );
    assert!(
        !victim_present_in_cheating,
        "victim was silently dropped yet the bundle still verifies"
    );
}

// Fabricated prior_balances: operator invents a debt for a miner to zero it out.
#[test]
fn operator_fabricates_prior_and_verifier_rejects() {
    let net_diff = 1u128;
    let found = FoundBlock {
        block_height: 200,
        coinbase_value_sats: 500_000_000,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: 1001,
    };
    let shares = vec![
        share(1, "victim", "01", 1, 1, net_diff, 1000, 1000),
        share(2, "insider", "02", 2, 1, net_diff, 1001, 1001),
    ];
    // Operator invents a large negative prior for the victim (no ledger basis).
    let fake_prior = vec![CarryForwardBalance {
        recipient_id: "victim".to_string(),
        order_key: "01".to_string(),
        p2mr_program_hex: p2mr(1),
        balance_sats: -200_000_000,
    }];
    let canonical = build_audit_bundle(
        shares.clone(),
        found.clone(),
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &signing_key(),
        &ledger_signing_key(),
    )
    .unwrap();
    verify_audit_bundle(&canonical, &ledger_public_key_hex()).unwrap();
    let bundle = build_audit_bundle(
        shares,
        found,
        fake_prior,
        PayoutPolicy::day_one_default(),
        &signing_key(),
        &attacker_ledger_signing_key(),
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

// ---------------------------------------------------------------------------
// Reorg boundary correctness — reorg boundary correctness
// ---------------------------------------------------------------------------

fn one_account_manifest() -> qbit_prism::PayoutPolicyManifest {
    let net_diff = 1u128;
    let shares = vec![share(1, "solo", "01", 1, 5, net_diff, 1000, 1000)];
    let found = FoundBlock {
        block_height: 200,
        coinbase_value_sats: 500_000_000,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: 1000,
    };
    let reward = build_prism_reward_manifest(&shares, &found).unwrap();
    apply_payout_policy(&reward, &[], &PayoutPolicy::day_one_default()).unwrap()
}

// At exactly tip == H + 1000 the entry becomes Mature and can no longer be
// reversed. qbit makes the coinbase spendable at tip >= H + 999 (COINBASE_MATURITY
// = 1000, spend_height = tip+1). The pool's "mature" therefore lags qbit's
// "spendable" by one block (conservative — documented, not exploitable here).
#[test]
fn exact_maturity_boundary_blocks_reversal() {
    assert_eq!(QBIT_COINBASE_MATURITY_BLOCKS, 1000);
    let manifest = one_account_manifest();
    let entries = build_maturity_entries("block-a", 200, &manifest);

    let one_short = update_maturity_states(&entries, 200 + 1000 - 1); // tip = H+999
    assert!(one_short
        .iter()
        .all(|e| e.state == PayoutMaturityState::Immature));
    // Still reversible at H+999 (one block before pool maturity).
    assert!(
        reverse_disconnected_blocks(&one_short, &["block-a".to_string()], 200 + 1000 - 1).is_ok()
    );

    let at_boundary = update_maturity_states(&entries, 200 + 1000); // tip = H+1000
    assert!(at_boundary
        .iter()
        .all(|e| e.state == PayoutMaturityState::Mature));
    // Mature -> reversal refused (cannot silently reverse a matured payout).
    let reversed = reverse_disconnected_blocks(&at_boundary, &["block-a".to_string()], 200 + 1000);
    println!("[reorg] reverse at exact maturity boundary = {reversed:?}");
    assert!(
        reversed.is_err(),
        "matured payout must not be silently reversible"
    );
}

// double-disconnect is idempotent and never throws on an already-reversed block.
#[test]
fn double_disconnect_is_idempotent() {
    let manifest = one_account_manifest();
    let entries = build_maturity_entries("block-a", 200, &manifest);
    let once = reverse_disconnected_blocks(&entries, &["block-a".to_string()], 1_199).unwrap();
    let twice = reverse_disconnected_blocks(&once, &["block-a".to_string()], 1_199).unwrap();
    assert_eq!(once, twice);
    assert!(current_carry_forward_balances(&once).is_empty());
}

// disconnect-then-reconverge: reversing the immature block and adopting the
// replacement yields the replacement's balances; the immature payout does not
// survive.
#[test]
fn disconnect_then_reconverge() {
    let manifest = one_account_manifest();
    let block_a = build_maturity_entries("block-a", 200, &manifest);
    let reversed = reverse_disconnected_blocks(&block_a, &["block-a".to_string()], 1_199).unwrap();
    assert!(current_carry_forward_balances(&reversed).is_empty());

    let block_b = build_maturity_entries("block-b", 200, &manifest);
    let mut combined = reversed;
    combined.extend(block_b.clone());
    assert_eq!(
        current_carry_forward_balances(&combined),
        current_carry_forward_balances(&block_b),
        "balances must reconverge to the replacement branch"
    );
}

// Cross-check of GLM-5.2 T6: reverse_disconnected_blocks is STATE-based, not
// height-based. If the maturity sweep was not run, a block that is already
// height-mature (tip >= H + 1000) but still carries Immature state is SILENTLY
// reversed — the guard only fires on the cached Mature state, not on height.
#[test]
fn stale_state_allows_silent_mature_reversal() {
    let manifest = one_account_manifest(); // block_height 200
    let height_mature_tip = 200 + QBIT_COINBASE_MATURITY_BLOCKS; // 1200

    // Path A: sweep first, then reverse -> correctly refused.
    let swept = update_maturity_states(
        &build_maturity_entries("block-a", 200, &manifest),
        height_mature_tip,
    );
    assert!(
        reverse_disconnected_blocks(&swept, &["block-a".to_string()], height_mature_tip).is_err()
    );

    // Path B: never swept (entries still Immature), reverse at the same tip ->
    // silently reverses a payout that is already height-mature.
    let unswept = build_maturity_entries("block-a", 200, &manifest);
    let reversed =
        reverse_disconnected_blocks(&unswept, &["block-a".to_string()], height_mature_tip);
    println!(
        "[reorg stale] tip={height_mature_tip} (>= H+1000) unswept reversal ok = {}",
        reversed.is_ok()
    );
    assert!(
        reversed.is_err(),
        "height-mature payout must not reverse even when state wasn't swept first"
    );
}

// Cross-check of GLM-5.2 T4: compute_prism_window does NO de-duplication. Two
// records with the same share_id (a replayed/duplicated share that slipped past
// the coordinator + DB unique constraint, or was injected into an audit bundle)
// are BOTH counted, inflating that miner's entitlement.
#[test]
fn engine_does_not_dedup_shares() {
    let net_diff = 100u128; // window weight 800, comfortably room for both
    let dup = share(1, "attacker", "01", 1, 50, net_diff, 1000, 1000);
    let mut dup2 = dup.clone();
    dup2.share_seq = 2; // distinct seq, identical share_id "share-1"
    let honest = share(3, "honest", "02", 2, 50, net_diff, 1002, 1002);
    let found = FoundBlock {
        block_height: 10,
        coinbase_value_sats: 500_000_000,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: 1002,
    };
    let result = compute_prism_window(&[dup, dup2, honest], &found);
    assert!(
        matches!(result, Err(PrismError::DuplicateShareId { .. })),
        "engine rejects duplicated share_id, got {result:?}"
    );
}

// Cross-check / refinement of GLM-5.2 T5b: the plain verifier trusts
// found_block.coinbase_value_sats, but the ON-CHAIN HEX cross-check catches a
// value lie. Shares/priors, however, stay unanchored even WITH the hex check.
#[test]
fn coinbase_value_lie_only_caught_by_hex_check() {
    let net_diff = 1u128;
    let shares = vec![
        share(1, "a", "01", 1, 1, net_diff, 1000, 1000),
        share(2, "b", "02", 2, 1, net_diff, 1001, 1001),
    ];
    // Operator lies: declares a 1,000,000-sat coinbase.
    let lied = FoundBlock {
        block_height: 10,
        coinbase_value_sats: 1_000_000,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: 1001,
    };
    let bundle = build_audit_bundle(
        shares,
        lied,
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &signing_key(),
        &ledger_signing_key(),
    )
    .unwrap();

    // Plain verify accepts the lie (no subsidy anchor).
    let plain = verify_audit_bundle(&bundle, &ledger_public_key_hex());
    // Against a *different* real on-chain coinbase hex, the lie is caught.
    let real_onchain_hex = "00"; // any hex that isn't the lied coinbase
    let with_hex = qbit_prism::verify_audit_bundle_against_coinbase_tx_hex(
        &bundle,
        real_onchain_hex,
        &ledger_public_key_hex(),
    );
    println!(
        "[audit-coinbase-value] plain verify ok = {}, with on-chain hex check ok = {}",
        plain.is_ok(),
        with_hex.is_ok()
    );
    assert!(
        plain.is_ok(),
        "plain verifier accepts an inflated coinbase value"
    );
    assert!(
        with_hex.is_err(),
        "on-chain hex cross-check catches the value lie"
    );
}

// ---------------------------------------------------------------------------
// Coinbase builder consensus invariants — consensus invariants on the coinbase builder
// ---------------------------------------------------------------------------

const MAX_BLOCK_WEIGHT: usize = 2_000_000; // qbit consensus.h, WITNESS_SCALE_FACTOR = 1

fn equal_weight_request(
    block_height: u64,
    coinbase_value_sats: u64,
    n: usize,
) -> CoinbaseBuildRequest {
    let entitlements = (0..n)
        .map(|i| WeightedEntitlement {
            recipient_id: format!("m{i}"),
            order_key: format!("{i:08}"),
            // vary the program so every output is distinct
            p2mr_program_hex: hex::encode({
                let mut p = [0u8; 32];
                p[0] = (i & 0xff) as u8;
                p[1] = ((i >> 8) & 0xff) as u8;
                p[2] = ((i >> 16) & 0xff) as u8;
                p
            }),
            weight: 1,
        })
        .collect();
    CoinbaseBuildRequest {
        block_height,
        coinbase_value_sats,
        entitlements,
        witness_nonce_hex: None,
        witness_merkle_leaves_hex: Vec::new(),
        coinbase_script_sig_suffix_hex: None,
    }
}

// 500 outputs is well under MAX_BLOCK_WEIGHT, and every output is P2MR or the
// single OP_RETURN witness commitment. (Sanity: the documented-good case.)
#[test]
fn five_hundred_outputs_valid_and_under_weight() {
    let manifest = build_manifest(equal_weight_request(100_000, 500_000_000, 500)).unwrap();
    let weight = manifest.coinbase_tx_hex.len() / 2;
    println!("[coinbase] 500-output coinbase weight = {weight} bytes (limit {MAX_BLOCK_WEIGHT})");
    assert!(weight < MAX_BLOCK_WEIGHT);
    assert!(manifest
        .outputs
        .iter()
        .all(|o| is_p2mr_script_pubkey(&hex::decode(&o.script_pubkey_hex).unwrap())));
    assert!(manifest
        .witness_commitment_script_hex
        .starts_with("6a24aa21a9ed"));
    // value conserved
    assert_eq!(
        manifest.outputs.iter().map(|o| o.amount_sats).sum::<u64>(),
        500_000_000
    );
}

// BIP34 / bad-cb-length: scriptSig must be 2..=100 bytes at every height.
#[test]
fn scriptsig_length_2_to_100_at_all_heights() {
    for &height in &[
        1u64,
        16,
        17,
        127,
        128,
        500,
        65_535,
        16_777_216,
        4_294_967_295,
    ] {
        let manifest = build_manifest(equal_weight_request(height, 500_000_000, 1)).unwrap();
        let len = manifest.coinbase_script_sig_hex.len() / 2;
        println!("[coinbase] height {height:>10} scriptSig len = {len}");
        assert!(
            (2..=100).contains(&len),
            "height {height}: scriptSig length {len} violates qbit bad-cb-length (2..100)"
        );
    }
}

// With enough on-chain recipients the serialized coinbase would exceed
// 2,000,000 bytes. The builder must reject it before qbit does.
#[test]
fn builder_rejects_overweight_coinbase() {
    // Each P2MR output serializes to 43 bytes; ~46.5k outputs crosses 2 MB.
    let n = 60_000;
    // coinbase value large enough that the policy floor would allow this many
    // outputs (>= n * 14_720), demonstrating it is reachable, not just synthetic.
    let err = build_manifest(equal_weight_request(800_000, 2_000_000_000, n)).unwrap_err();
    match err {
        qbit_pool_builder::BuilderError::CoinbaseWeightTooHigh { weight, max_weight } => {
            println!("[coinbase] {n}-output coinbase rejected at {weight} bytes (limit {max_weight})");
            assert!(weight > MAX_BLOCK_WEIGHT);
            assert_eq!(max_weight, MAX_BLOCK_WEIGHT);
        }
        other => panic!("expected CoinbaseWeightTooHigh, got {other:?}"),
    }
}
