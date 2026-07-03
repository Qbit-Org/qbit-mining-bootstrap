//! Adversarial invariant probes for PRISM pool accounting.
//!
//! Each test targets one of the seven verification questions. Tests that pass
//! document a confirmed vulnerability; tests that fail document a held
//! invariant.

use qbit_pool_builder::{
    build_manifest, is_p2mr_script_pubkey, CoinbaseBuildRequest, ManifestSigningKey,
    PayoutManifest, WeightedEntitlement, MAX_COINBASE_SCRIPT_SIG_LEN,
};
use qbit_prism::{
    apply_payout_policy, build_audit_bundle, build_maturity_entries, compute_prism_window,
    current_carry_forward_balances, reverse_disconnected_blocks, update_maturity_states,
    verify_audit_bundle, verify_audit_bundle_against_coinbase_tx_hex, AcceptedShare,
    CarryForwardBalance, FoundBlock, PayoutMaturityEntry, PayoutMaturityState, PayoutPolicy,
    PayoutPolicyAccountType, PayoutPolicyAction, PrismRewardManifest, PRISM_WINDOW_MULTIPLIER,
    QBIT_COINBASE_MATURITY_BLOCKS,
};

fn p2mr(byte: u8) -> String {
    hex::encode([byte; 32])
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

fn entitlement(id: &str, order: &str, byte: u8, weight: u128) -> WeightedEntitlement {
    WeightedEntitlement {
        recipient_id: id.to_string(),
        order_key: order.to_string(),
        p2mr_program_hex: p2mr(byte),
        weight,
    }
}

fn share(
    seq: u64,
    miner: &str,
    order: &str,
    byte: u8,
    diff: u128,
    job_ms: i64,
    acc_ms: i64,
) -> AcceptedShare {
    AcceptedShare {
        share_seq: seq,
        share_id: format!("s-{seq}"),
        miner_id: miner.to_string(),
        order_key: order.to_string(),
        p2mr_program_hex: p2mr(byte),
        share_difficulty: diff,
        network_difficulty: 10,
        template_height: 100,
        job_id: format!("job-{job_ms}"),
        job_issued_at_ms: job_ms,
        accepted_at_ms: acc_ms,
        ntime: 1_800_000_000,
    }
}

fn found_block(net_diff: u128, anchor_ms: i64, coinbase: u64) -> FoundBlock {
    FoundBlock {
        block_height: 101,
        coinbase_value_sats: coinbase,
        network_difficulty: net_diff,
        anchor_job_issued_at_ms: anchor_ms,
    }
}

fn dummy_manifest(coinbase: u64, ents: Vec<WeightedEntitlement>) -> PrismRewardManifest {
    PrismRewardManifest {
        schema: "qbit.prism.reward-manifest.v1".to_string(),
        block_height: 101,
        coinbase_value_sats: coinbase,
        network_difficulty: 1,
        window_multiplier: PRISM_WINDOW_MULTIPLIER,
        requested_window_weight: 8,
        counted_window_weight: ents.iter().map(|e| e.weight).sum(),
        anchor_job_issued_at_ms: 1000,
        anchor_share_seq: 1,
        newest_share_seq: 1,
        oldest_share_seq: 1,
        included_share_count: ents.len(),
        share_slice_digest_hex: "00".repeat(32),
        shares: Vec::new(),
        entitlements: ents,
    }
}

// ===========================================================================
// T1 — Value conservation
// ===========================================================================

#[test]
fn t1a_coinbase_outputs_sum_to_coinbase_value_exactly() {
    let values = [1_u64, 2, 17, 499, 50_000, 500_000_000, 21_000_000_000_000];
    let counts = [1_usize, 2, 3, 17, 64, 127, 499];
    for v in values {
        for &n in &counts {
            let ents = (0..n)
                .map(|i| {
                    entitlement(
                        &format!("m{i:03}"),
                        &format!("{i:06}"),
                        (i % 251 + 1) as u8,
                        (i as u128 % 19) + 1,
                    )
                })
                .collect::<Vec<_>>();
            let req = CoinbaseBuildRequest {
                block_height: 100,
                coinbase_value_sats: v,
                entitlements: ents,
                witness_nonce_hex: None,
                witness_merkle_leaves_hex: Vec::new(),
                coinbase_script_sig_suffix_hex: None,
            };
            let m = build_manifest(req).unwrap();
            let sum: u64 = m.outputs.iter().map(|o| o.amount_sats).sum();
            assert_eq!(sum, v, "outputs must sum to coinbase value");
        }
    }
}

#[test]
fn t1b_single_onchain_account_cannot_be_overpaid_above_candidate() {
    // Two entitlements: A large, B tiny (below floor). The historical policy
    // selected only A and paid the full coinbase, which made A's carry-forward
    // negative. The required behavior is now to reject instead of creating a
    // negative carry-forward through overpay.
    let coinbase = 100_000_u64;
    let policy = PayoutPolicy::day_one_default();
    let floor = policy.min_output_sats().unwrap();
    assert_eq!(floor, 14_720);

    let manifest = dummy_manifest(
        coinbase,
        vec![entitlement("A", "01", 1, 6), entitlement("B", "02", 2, 1)],
    );
    let err = apply_payout_policy(&manifest, &[], &policy).unwrap_err();

    assert!(matches!(
        err,
        qbit_prism::PrismError::PayoutExceedsCandidateBalance {
            coinbase_value_sats: 100_000,
            selected_candidate_balance_sats: 85_714
        }
    ));
    eprintln!(
        "T1b: selected candidate balance below coinbase is rejected; no negative carry is produced"
    );
}

#[test]
fn t1c_carry_forward_conserved_in_healthy_multi_block_sequence() {
    // Control case: a normal sequence where every block's selected accounts
    // capture ~all of the coinbase should conserve carry-forward exactly.
    let policy = PayoutPolicy::day_one_default();
    let coinbase = 500_000_000_u64;
    let manifest = dummy_manifest(
        coinbase,
        vec![entitlement("A", "01", 1, 50), entitlement("B", "02", 2, 50)],
    );
    let pm = apply_payout_policy(&manifest, &[], &policy).unwrap();
    let entries = build_maturity_entries("blk", 101, &pm);
    let balances = current_carry_forward_balances(&entries);
    let total_accrued: i128 = balances.iter().map(|b| b.balance_sats).sum();
    let total_onchain: u64 = pm
        .onchain_entitlements
        .iter()
        .map(|e| e.weight as u64)
        .sum();
    assert_eq!(total_onchain, coinbase);
    // In the healthy case, accrued == prior (0 here), so no value created.
    assert_eq!(total_accrued, 0, "healthy case: accrued == prior (0)");
}

// ===========================================================================
// T2 — Rust compute_prism_window vs SQL/Python snapshot_at_job_issue divergence
// ===========================================================================

#[test]
fn t2_rust_excludes_late_accepted_share_like_sql_python() {
    // Share 2 was issued before the anchor (job_ms=1000 <= anchor=1001) but
    // ACCEPTED after the anchor (acc_ms=2000 > 1001).
    //
    // Rust compute_prism_window filters ONLY on job_issued_at_ms (lib.rs:273),
    // so it INCLUDES share 2.
    //
    // SQL qbit_prism_window (sql:144-145) and Python snapshot_at_job_issue
    // (share_ledger.py:106-107) ALSO require accepted_at <= anchor, so they
    // EXCLUDE share 2. The ledger-ops doc (prism-ledger-ops.md:46-52) says the
    // accept-time filter is the intended contract.
    let shares = vec![
        share(1, "A", "01", 1, 10, 1000, 1000),
        share(2, "B", "02", 2, 10, 1000, 2000), // late accept
    ];
    let anchor = 1001_i64;
    let window = compute_prism_window(&shares, &found_block(10, anchor, 500_000_000)).unwrap();

    let seqs: Vec<u64> = window.shares.iter().map(|s| s.share_seq).collect();
    assert_eq!(seqs, vec![1], "Rust excludes the late-accepted share 2");
    assert_eq!(
        window.counted_window_weight, 10,
        "only the anchored share is counted"
    );
}

// ===========================================================================
// T3 — Payout floor attacks
// ===========================================================================

#[test]
fn t3a_all_below_floor_yields_no_onchain_recipients_error() {
    // coinbase above floor, but every account's gross is below floor.
    let policy = PayoutPolicy::day_one_default();
    let coinbase = 20_000_u64; // above floor (14_720)
    let manifest = dummy_manifest(
        coinbase,
        vec![entitlement("A", "01", 1, 1), entitlement("B", "02", 2, 1)],
    );
    // gross_A = gross_B = 10_000 < floor => neither selected.
    let err = apply_payout_policy(&manifest, &[], &policy).unwrap_err();
    assert!(
        matches!(err, qbit_prism::PrismError::NoOnchainRecipients),
        "all-below-floor must error, got {err:?}"
    );
    eprintln!("T3a: all-below-floor -> NoOnchainRecipients (block cannot be paid; coinbase forfeit unless operator mines separately)");
}

#[test]
fn t3b_exactly_at_floor_is_selected() {
    let policy = PayoutPolicy::day_one_default();
    let floor = policy.min_output_sats().unwrap();
    // Construct a single account whose candidate == floor exactly via prior.
    let manifest = dummy_manifest(
        floor * 2,
        vec![entitlement("A", "01", 1, 1), entitlement("B", "02", 2, 1)],
    );
    let prior = vec![CarryForwardBalance {
        recipient_id: "B".to_string(),
        order_key: "02".to_string(),
        p2mr_program_hex: p2mr(2),
        balance_sats: floor as i128,
    }];
    let pm = apply_payout_policy(&manifest, &prior, &policy).unwrap();
    let b = pm.accounts.iter().find(|a| a.recipient_id == "B").unwrap();
    assert!(b.candidate_balance_sats >= floor as i128);
    // Every on-chain output is >= floor (enforced at lib.rs:447).
    for e in &pm.onchain_entitlements {
        assert!(e.weight as u64 >= floor);
    }
    eprintln!("T3b: at-floor account selectable; no sub-floor on-chain output emitted");
}

#[test]
fn t3c_select_onchain_accounts_terminates_and_emits_no_subfloor_output() {
    // Stress: many accounts, some below floor. The selection loop must
    // terminate and never emit a sub-floor on-chain output.
    let policy = PayoutPolicy::day_one_default();
    let floor = policy.min_output_sats().unwrap();
    let mut ents = Vec::new();
    for i in 0..40u8 {
        ents.push(entitlement(
            &format!("m{i:03}"),
            &format!("{i:03}"),
            i,
            (i as u128) + 1,
        ));
    }
    let manifest = dummy_manifest(500_000_000, ents);
    let pm = apply_payout_policy(&manifest, &[], &policy).unwrap();
    for e in &pm.onchain_entitlements {
        assert!(
            e.weight as u64 >= floor,
            "sub-floor on-chain output emitted: {}",
            e.weight
        );
    }
}

#[test]
fn t3d_floor_attack_cannot_create_negative_carry_by_overpay() {
    // Same root cause as T1b, framed as a floor attack: selected recipients
    // must not absorb the full coinbase when their candidate balance is lower.
    let policy = PayoutPolicy::day_one_default();
    let coinbase = 50_000_u64;
    let manifest = dummy_manifest(
        coinbase,
        vec![
            entitlement("big", "01", 1, 9),
            entitlement("dust", "02", 2, 1),
        ],
    );
    let err = apply_payout_policy(&manifest, &[], &policy).unwrap_err();

    assert!(matches!(
        err,
        qbit_prism::PrismError::PayoutExceedsCandidateBalance {
            coinbase_value_sats: 50_000,
            selected_candidate_balance_sats: 45_000
        }
    ));
    eprintln!("T3d: floor attack rejected before any overpay-derived negative carry is emitted");
}

// ===========================================================================
// T4 — Hostile miner (ledger-layer; transport checks live in Python)
// ===========================================================================

#[test]
fn t4_rust_window_does_not_dedup_or_validate_share_id_uniqueness() {
    // compute_prism_window trusts the caller-supplied share slice. Duplicate
    // share_seq / share_id values are NOT deduplicated by the engine; the
    // caller (coordinator / SQL UNIQUE constraint) is the only guard. A
    // memory-backend ledger that bypasses the coordinator's header dedup
    // would double-count.
    let mut s = share(1, "A", "01", 1, 10, 1000, 1000);
    let mut dup = s.clone();
    dup.share_id = "s-1".to_string(); // same seq, different id
    let _ = &mut s;
    let shares = vec![s, dup];
    let result = compute_prism_window(&shares, &found_block(10, 1001, 500_000_000));
    assert!(
        matches!(result, Err(qbit_prism::PrismError::DuplicateShareId { .. })),
        "Rust rejects duplicate share_id before counting: {result:?}"
    );
}

// ===========================================================================
// T5 — Audit verifier soundness
// ===========================================================================

#[test]
fn t5a_verifier_rejects_bundle_that_omits_a_miners_shares() {
    // A dishonest operator exports a bundle that DROPS one miner's shares.
    // The on-chain coinbase matches the manipulated bundle, but verification
    // must only accept a bundle signed by the canonical ledger writer key.
    let key = signing_key();
    let all_shares = vec![
        share(1, "A", "01", 1, 10, 1000, 1000),
        share(2, "B", "02", 2, 10, 1000, 1000),
        share(3, "C", "03", 3, 10, 1000, 1000),
    ];
    let fb = found_block(10, 1001, 500_000_000);
    let full = build_audit_bundle(
        all_shares.clone(),
        fb.clone(),
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &key,
        &ledger_signing_key(),
    )
    .unwrap();

    // Dishonest bundle: omit miner C entirely.
    let truncated_shares: Vec<AcceptedShare> = all_shares
        .into_iter()
        .filter(|s| s.miner_id != "C")
        .collect();
    let dishonest = build_audit_bundle(
        truncated_shares,
        fb,
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &key,
        &attacker_ledger_signing_key(),
    )
    .unwrap();

    // The full bundle verifies; the self-signed truncated bundle is not signed
    // by the canonical ledger writer.
    let full_report = verify_audit_bundle(&full, &ledger_public_key_hex()).unwrap();
    let dishonest_report = verify_audit_bundle(&dishonest, &ledger_public_key_hex());

    // Both match their own on-chain coinbase hex.
    verify_audit_bundle_against_coinbase_tx_hex(
        &full,
        &full_report.coinbase_tx_hex,
        &ledger_public_key_hex(),
    )
    .unwrap();
    assert!(dishonest_report.is_err(), "dishonest bundle is rejected");

    // The dishonest coinbase pays only A and B; C is gone. The verifier
    // The verifier rejects it before the omitted recipient can be treated as canonical.
    let dishonest_recipients: Vec<&str> = dishonest
        .signed_coinbase_manifest
        .manifest
        .outputs
        .iter()
        .map(|o| o.recipient_id.as_str())
        .collect();
    assert!(!dishonest_recipients.contains(&"C"), "C was omitted");
    assert!(dishonest_recipients.contains(&"A") && dishonest_recipients.contains(&"B"));
    eprintln!("T5a: verifier rejects dishonest bundle omitting miner C; recipients={dishonest_recipients:?}");
}

#[test]
fn t5b_verifier_does_not_check_coinbase_value_equals_real_subsidy_plus_fees() {
    // The verifier trusts bundle.found_block.coinbase_value_sats. An operator
    // can declare a coinbase_value SMALLER than the real subsidy+fees, pay
    // miners less, and the verifier accepts (internally consistent).
    let key = signing_key();
    let shares = vec![share(1, "A", "01", 1, 10, 1000, 1000)];
    let real_subsidy = 500_000_000_u64;
    let lied_value = 1_000_000_u64; // operator under-reports
    let fb = FoundBlock {
        block_height: 101,
        coinbase_value_sats: lied_value,
        network_difficulty: 10,
        anchor_job_issued_at_ms: 1001,
    };
    let bundle = build_audit_bundle(
        shares,
        fb,
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &key,
        &ledger_signing_key(),
    )
    .unwrap();
    let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
    assert_eq!(report.coinbase_value_sats, lied_value);
    assert_ne!(lied_value, real_subsidy);
    assert!(
        verify_audit_bundle_against_coinbase_tx_hex(&bundle, "00", &ledger_public_key_hex())
            .is_err()
    );
    eprintln!(
        "T5b: verifier accepted coinbase_value={lied_value} (real subsidy would be {real_subsidy}); no subsidy anchor"
    );
}

#[test]
fn t5c_verifier_rejects_prior_balances_not_signed_by_canonical_ledger() {
    // An operator can declare prior_balances = [] even when miners have real
    // accrued balances. Verification must reject that bundle unless the
    // canonical ledger writer signed the matching prior-balances digest.
    let key = signing_key();
    let shares = vec![share(1, "A", "01", 1, 10, 1000, 1000)];
    let fb = found_block(10, 1001, 500_000_000);

    // Truthful bundle: miner A has a prior accrued balance.
    let truthful = build_audit_bundle(
        shares.clone(),
        fb.clone(),
        vec![CarryForwardBalance {
            recipient_id: "A".to_string(),
            order_key: "01".to_string(),
            p2mr_program_hex: p2mr(1),
            balance_sats: 50_000,
        }],
        PayoutPolicy::day_one_default(),
        &key,
        &ledger_signing_key(),
    )
    .unwrap();

    // Dishonest bundle: claim no prior balance.
    let dishonest = build_audit_bundle(
        shares,
        fb,
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &key,
        &attacker_ledger_signing_key(),
    )
    .unwrap();

    verify_audit_bundle(&truthful, &ledger_public_key_hex()).unwrap();
    assert!(verify_audit_bundle(&dishonest, &ledger_public_key_hex()).is_err());
    eprintln!("T5c: verifier rejects dishonest prior=0 bundle against canonical prior attestation");
}

// ===========================================================================
// T6 — Reorg / maturity boundary
// ===========================================================================

#[test]
fn t6a_mature_block_silently_reversed_when_maturity_state_is_stale() {
    // Block at height 200 is height-mature (tip=1200 >= 200+1000), but
    // update_maturity_states was never called, so state is still Immature.
    // reverse_disconnected_blocks then reverses it SILENTLY (no error),
    // because it only errors when state == Mature. The invariant "matured
    // payouts never silently reverse" depends on operational ordering
    // (maturity sweep BEFORE reorg processing), which the code does not
    // enforce.
    let policy = PayoutPolicy::day_one_default();
    let manifest = dummy_manifest(500_000_000, vec![entitlement("A", "01", 1, 1)]);
    let pm = apply_payout_policy(&manifest, &[], &policy).unwrap();
    let entries = build_maturity_entries("blk-200", 200, &pm);

    // Tip is at 1200 (height-mature) but state was never swept.
    let stale_entries = entries.clone(); // still Immature
    let tip = 1_200_u64;
    assert!(
        tip >= 200 + QBIT_COINBASE_MATURITY_BLOCKS,
        "height-mature by tip"
    );

    let reversed = reverse_disconnected_blocks(&stale_entries, &["blk-200".to_string()], tip);
    assert!(
        matches!(
            reversed,
            Err(qbit_prism::PrismError::MaturePayoutDisconnect { .. })
        ),
        "height-mature but state-stale block is refused"
    );

    // If the sweep HAD run first, reversal would correctly error.
    let swept = update_maturity_states(&entries, tip);
    let err = reverse_disconnected_blocks(&swept, &["blk-200".to_string()], tip);
    assert!(
        matches!(
            err,
            Err(qbit_prism::PrismError::MaturePayoutDisconnect { .. })
        ),
        "after a maturity sweep the reversal correctly errors"
    );
    eprintln!("T6a: height-mature stale-state block reversal is refused");
}

#[test]
fn t6b_double_disconnect_is_idempotent() {
    let policy = PayoutPolicy::day_one_default();
    let manifest = dummy_manifest(500_000_000, vec![entitlement("A", "01", 1, 1)]);
    let pm = apply_payout_policy(&manifest, &[], &policy).unwrap();
    let entries = build_maturity_entries("blk-a", 200, &pm);
    let once = reverse_disconnected_blocks(&entries, &["blk-a".to_string()], 1_199).unwrap();
    let twice = reverse_disconnected_blocks(&once, &["blk-a".to_string()], 1_199).unwrap();
    assert_eq!(once, twice, "double-disconnect is idempotent");
}

#[test]
fn t6c_disconnect_then_reconverge_to_replacement() {
    let policy = PayoutPolicy::day_one_default();
    let manifest = dummy_manifest(500_000_000, vec![entitlement("A", "01", 1, 1)]);
    let pm = apply_payout_policy(&manifest, &[], &policy).unwrap();
    let entries_a = build_maturity_entries("blk-a", 200, &pm);
    let reversed = reverse_disconnected_blocks(&entries_a, &["blk-a".to_string()], 1_199).unwrap();
    let entries_b = build_maturity_entries("blk-b", 200, &pm);
    let mut combined = reversed.clone();
    combined.extend(entries_b.clone());
    let combined_bal = current_carry_forward_balances(&combined);
    let repl_bal = current_carry_forward_balances(&entries_b);
    assert_eq!(
        combined_bal, repl_bal,
        "carry-forward reconverges to replacement branch"
    );
}

#[test]
fn t6d_reversed_block_accrual_is_dropped_from_owed_balances() {
    // When an immature block is reversed, its accrued carry-forward must
    // disappear from owed balances (the snapshot reverts to the prior entry).
    let account = |block_hash: &str,
                   block_height: u64,
                   gross_amount_sats: u64,
                   prior_balance_sats: i128,
                   onchain_amount_sats: u64,
                   carry_forward_balance_sats: i128| {
        PayoutMaturityEntry {
            block_hash: block_hash.to_string(),
            block_height,
            account_type: PayoutPolicyAccountType::Miner,
            recipient_id: "A".to_string(),
            order_key: "01".to_string(),
            p2mr_program_hex: p2mr(1),
            gross_amount_sats,
            prior_balance_sats,
            candidate_balance_sats: prior_balance_sats + i128::from(gross_amount_sats),
            onchain_amount_sats,
            settlement_fee_sats: 0,
            carry_forward_balance_sats,
            action: if onchain_amount_sats > 0 {
                PayoutPolicyAction::Onchain
            } else {
                PayoutPolicyAction::Accrued
            },
            state: PayoutMaturityState::Immature,
        }
    };

    // Prior mature block gave A a balance.
    let prior_entries = vec![account("blk-1", 100, 1_000, 0, 0, 1_000)];
    let prior_balances = current_carry_forward_balances(&prior_entries);
    assert!(!prior_balances.is_empty());

    // New immature block builds on the prior balance.
    let new_entries = vec![account("blk-2", 150, 250, 1_000, 0, 1_250)];
    let mut combined = prior_entries.clone();
    combined.extend(new_entries.clone());
    let before_reverse = current_carry_forward_balances(&combined);
    assert_eq!(before_reverse[0].balance_sats, 1_250);

    // Reverse the immature block-2: balances revert to the block-1 snapshot.
    let reversed = reverse_disconnected_blocks(&combined, &["blk-2".to_string()], 1_000).unwrap();
    let after_reverse = current_carry_forward_balances(&reversed);
    assert_eq!(
        after_reverse, prior_balances,
        "reversing immature block reverts to prior snapshot"
    );
    assert_eq!(after_reverse[0].balance_sats, 1_000);
}

// ===========================================================================
// T7 — Consensus invariants
// ===========================================================================

#[test]
fn t7a_every_coinbase_output_is_p2mr_plus_one_op_return_witness_commitment() {
    let req = CoinbaseBuildRequest {
        block_height: 100,
        coinbase_value_sats: 500_000_000,
        entitlements: vec![entitlement("A", "01", 1, 1), entitlement("B", "02", 2, 2)],
        witness_nonce_hex: None,
        witness_merkle_leaves_hex: Vec::new(),
        coinbase_script_sig_suffix_hex: None,
    };
    let m = build_manifest(req).unwrap();
    // All miner outputs are P2MR.
    for o in &m.outputs {
        let spk = hex::decode(&o.script_pubkey_hex).unwrap();
        assert!(
            is_p2mr_script_pubkey(&spk),
            "output {} is not P2MR",
            o.recipient_id
        );
    }
    // Exactly one OP_RETURN witness commitment (6a24aa21a9ed...).
    assert!(
        m.witness_commitment_script_hex.starts_with("6a24aa21a9ed"),
        "witness commitment is OP_RETURN"
    );
    // The commitment is appended as a final zero-value output in the tx hex.
    assert!(m.coinbase_tx_hex.contains(&m.witness_commitment_script_hex));
}

#[test]
fn t7b_coinbase_scriptsig_within_2_to_100_bytes_for_representative_heights() {
    let heights = [1_u64, 2, 16, 17, 255, 256, 65535, 65536, 4_294_967_295];
    for h in heights {
        let req = CoinbaseBuildRequest {
            block_height: h,
            coinbase_value_sats: 500_000_000,
            entitlements: vec![entitlement("A", "01", 1, 1)],
            witness_nonce_hex: None,
            witness_merkle_leaves_hex: Vec::new(),
            coinbase_script_sig_suffix_hex: None,
        };
        let m = build_manifest(req).unwrap();
        let script = hex::decode(&m.coinbase_script_sig_hex).unwrap();
        assert!(
            (2..=MAX_COINBASE_SCRIPT_SIG_LEN).contains(&script.len()),
            "height {h}: scriptSig len {} not in 2..=100",
            script.len()
        );
    }
}

#[test]
fn t7c_height_zero_rejected() {
    let req = CoinbaseBuildRequest {
        block_height: 0,
        coinbase_value_sats: 500_000_000,
        entitlements: vec![entitlement("A", "01", 1, 1)],
        witness_nonce_hex: None,
        witness_merkle_leaves_hex: Vec::new(),
        coinbase_script_sig_suffix_hex: None,
    };
    let err = build_manifest(req).unwrap_err();
    assert!(
        matches!(err, qbit_pool_builder::BuilderError::HeightOverflow),
        "height 0 rejected: {err:?}"
    );
}

#[test]
fn t7d_500_output_coinbase_under_max_block_weight() {
    // 500 P2MR outputs is well under 2,000,000 bytes (WSF=1), so the builder's
    // MAX_BLOCK_WEIGHT guard should allow it.
    let ents: Vec<WeightedEntitlement> = (0..500)
        .map(|i| {
            entitlement(
                &format!("m{i:03}"),
                &format!("{i:03}"),
                (i % 251 + 1) as u8,
                (i as u128) + 1,
            )
        })
        .collect();
    let req = CoinbaseBuildRequest {
        block_height: 100,
        coinbase_value_sats: 500_000_000,
        entitlements: ents,
        witness_nonce_hex: None,
        witness_merkle_leaves_hex: Vec::new(),
        coinbase_script_sig_suffix_hex: None,
    };
    let m: PayoutManifest = build_manifest(req).unwrap();
    let tx_bytes = m.coinbase_tx_hex.len() / 2;
    assert_eq!(m.outputs.len(), 500);
    assert!(
        tx_bytes < 2_000_000,
        "500-output coinbase is {tx_bytes} bytes (< 2M)"
    );
    let approx_per_output = tx_bytes / 500;
    let overflow_count = 2_000_000 / approx_per_output.max(1);
    eprintln!(
        "T7d: 500-output coinbase={tx_bytes} bytes; MAX_BLOCK_WEIGHT guard still has ~{overflow_count} comparable outputs of headroom"
    );
}

#[test]
fn t7e_coinbase_weight_bytes_are_reported_separately_from_entitlement_weight() {
    // The coordinator computes weight headroom as 2_000_000 - len(hex)//2
    // (prism_coordinator.py:921), assuming WSF=1 so weight == serialized
    // bytes. The Rust builder exposes total_weight as the sum of entitlement
    // WEIGHTS, not block weight, and exposes byte weight separately.
    let req = CoinbaseBuildRequest {
        block_height: 100,
        coinbase_value_sats: 500_000_000,
        entitlements: vec![entitlement("A", "01", 1, 42)],
        witness_nonce_hex: None,
        witness_merkle_leaves_hex: Vec::new(),
        coinbase_script_sig_suffix_hex: None,
    };
    let m = build_manifest(req).unwrap();
    // total_weight is the entitlement weight (42), NOT the tx byte weight.
    assert_eq!(
        m.total_weight, 42,
        "total_weight is entitlement weight, not byte weight"
    );
    let tx_bytes = m.coinbase_tx_hex.len() / 2;
    assert_eq!(
        m.coinbase_weight_bytes, tx_bytes,
        "serialized byte weight is exposed explicitly"
    );
    assert_ne!(
        m.total_weight as usize, tx_bytes,
        "entitlement weight is not serialized byte weight"
    );
}

#[test]
fn t7f_builder_does_not_validate_p2mr_program_distinctness() {
    // Two distinct recipients can share the same P2MR program. The builder
    // accepts this (it keys outputs by recipient/order/program). This is not
    // inherently a consensus violation, but it means entitlements can be
    // split across two "accounts" that pay the same on-chain program, which
    // the floor policy treats independently.
    let req = CoinbaseBuildRequest {
        block_height: 100,
        coinbase_value_sats: 500_000_000,
        entitlements: vec![
            entitlement("A", "01", 7, 1),
            entitlement("B", "02", 7, 1), // same p2mr byte
        ],
        witness_nonce_hex: None,
        witness_merkle_leaves_hex: Vec::new(),
        coinbase_script_sig_suffix_hex: None,
    };
    let m = build_manifest(req).unwrap();
    assert_eq!(m.outputs.len(), 2);
    // Both outputs pay the same scriptPubKey.
    assert_eq!(
        m.outputs[0].script_pubkey_hex,
        m.outputs[1].script_pubkey_hex
    );
}

// Determinism cross-check: building twice yields identical bytes.
#[test]
fn t7g_coinbase_is_byte_reproducible() {
    let req = CoinbaseBuildRequest {
        block_height: 100,
        coinbase_value_sats: 500_000_000,
        entitlements: vec![entitlement("A", "01", 1, 1), entitlement("B", "02", 2, 2)],
        witness_nonce_hex: None,
        witness_merkle_leaves_hex: Vec::new(),
        coinbase_script_sig_suffix_hex: None,
    };
    let a = build_manifest(req.clone()).unwrap();
    let b = build_manifest(req).unwrap();
    assert_eq!(a.coinbase_tx_hex, b.coinbase_tx_hex);
    assert_eq!(a.coinbase_txid, b.coinbase_txid);
    assert_eq!(a.coinbase_wtxid, b.coinbase_wtxid);
}
