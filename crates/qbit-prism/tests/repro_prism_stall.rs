//! Reproduction for the live testnet4 PRISM stall.
//!
//! Diagnostic only — confirms the production failure mechanism and shows that
//! the purpose-built settlement-mode selector handles the identical owed-set.

use qbit_pool_builder::WeightedEntitlement;
use qbit_prism::{
    apply_payout_policy, build_maturity_entries, build_policy_coinbase_request,
    current_carry_forward_balances, reverse_disconnected_blocks, select_settlement_mode,
    PayoutPolicy, PoolFeePolicy, PrismError, PrismRewardManifest, SettlementMode,
    SettlementModeConfig, SettlementRecipient, PRISM_WINDOW_MULTIPLIER,
};

const COINBASE: u64 = 21_000_000_000;
const DUST_MINERS: u64 = 7; // sub-floor miners, 1 sat each => 7 sats total

fn p2mr(byte: u8) -> String {
    hex::encode([byte; 32])
}

fn policy_with_pool_fee() -> PayoutPolicy {
    let mut policy = PayoutPolicy::day_one_default();
    policy.pool_fee_policy = Some(PoolFeePolicy {
        fee_bps: 200,
        recipient_id: "pool-fee".to_string(),
        order_key: "zz".to_string(),
        p2mr_program_hex: p2mr(99),
    });
    policy
}

/// Dominant miner weighted so its gross == COINBASE - DUST_MINERS, plus
/// `DUST_MINERS` miners each weighted to exactly 1 sat. total_weight == COINBASE
/// makes the weighted split exact (no remainder), so the numbers match prod.
fn reward_manifest() -> PrismRewardManifest {
    let mut entitlements = vec![WeightedEntitlement {
        recipient_id: "dominant".to_string(),
        order_key: "00".to_string(),
        p2mr_program_hex: p2mr(0),
        weight: u128::from(COINBASE - DUST_MINERS),
    }];
    for i in 0..DUST_MINERS {
        entitlements.push(WeightedEntitlement {
            recipient_id: format!("dust-{i}"),
            order_key: format!("{:02}", i + 1),
            p2mr_program_hex: p2mr((i + 1) as u8),
            weight: 1,
        });
    }
    PrismRewardManifest {
        schema: "qbit.prism.reward-manifest.v1".to_string(),
        block_height: 101,
        coinbase_value_sats: COINBASE,
        network_difficulty: 1,
        window_multiplier: PRISM_WINDOW_MULTIPLIER,
        requested_window_weight: 8,
        counted_window_weight: u128::from(COINBASE),
        anchor_job_issued_at_ms: 1_000,
        anchor_share_seq: 1,
        newest_share_seq: 1,
        oldest_share_seq: 1,
        included_share_count: entitlements.len(),
        share_slice_digest_hex: "00".repeat(32),
        shares: Vec::new(),
        entitlements,
    }
}

#[test]
fn repro_production_stall_exact_error() {
    let policy = PayoutPolicy::day_one_default();
    let err = apply_payout_policy(&reward_manifest(), &[], &policy).unwrap_err();
    // Byte-for-byte the production log line:
    //   coinbase value 21000000000 > selected candidate balance 20999999993
    assert!(
        matches!(
            err,
            PrismError::PayoutExceedsCandidateBalance {
                coinbase_value_sats: 21_000_000_000,
                selected_candidate_balance_sats: 20_999_999_993
            }
        ),
        "got {err:?}"
    );
}

#[test]
fn repro_production_stall_sweeps_dust_into_pool_fee_settlement() {
    let policy = policy_with_pool_fee();
    let reward = reward_manifest();
    let (request, policy_manifest) = build_policy_coinbase_request(&reward, &[], &policy).unwrap();

    let expected_earned_fee = COINBASE * 200 / 10_000;
    let pool_fee = policy_manifest.pool_fee.as_ref().unwrap();
    assert_eq!(pool_fee.earned_pool_fee_sats, expected_earned_fee);
    assert_eq!(pool_fee.swept_dust_liability_sats, DUST_MINERS);
    assert_eq!(pool_fee.amount_sats, expected_earned_fee + DUST_MINERS);

    let dominant = policy_manifest
        .accounts
        .iter()
        .find(|account| account.recipient_id == "dominant")
        .unwrap();
    assert_eq!(dominant.candidate_balance_sats, 20_579_999_993);
    assert_eq!(dominant.onchain_amount_sats, 20_579_999_993);
    assert_eq!(dominant.carry_forward_balance_sats, 0);

    let dust_accounts = policy_manifest
        .accounts
        .iter()
        .filter(|account| account.recipient_id.starts_with("dust-"))
        .collect::<Vec<_>>();
    assert_eq!(dust_accounts.len(), DUST_MINERS as usize);
    assert!(dust_accounts.iter().all(|account| {
        account.onchain_amount_sats == 0 && account.carry_forward_balance_sats == 1
    }));

    let request_sum = request
        .entitlements
        .iter()
        .map(|entitlement| entitlement.weight as u64)
        .sum::<u64>();
    assert_eq!(request_sum, COINBASE);
    assert_eq!(
        request
            .entitlements
            .iter()
            .find(|entitlement| entitlement.recipient_id == "pool-fee")
            .unwrap()
            .weight as u64,
        expected_earned_fee + DUST_MINERS
    );

    let entries = build_maturity_entries("block-with-dust", 101, &policy_manifest);
    let carry = current_carry_forward_balances(&entries);
    assert_eq!(
        carry
            .iter()
            .filter(|balance| balance.recipient_id.starts_with("dust-"))
            .map(|balance| balance.balance_sats)
            .sum::<i128>(),
        i128::from(DUST_MINERS)
    );
    assert!(carry
        .iter()
        .all(|balance| balance.recipient_id != "pool-fee"));

    let reversed =
        reverse_disconnected_blocks(&entries, &["block-with-dust".to_string()], 101).unwrap();
    assert!(current_carry_forward_balances(&reversed).is_empty());
}

#[test]
fn repro_settlement_mode_selector_handles_same_owed_set() {
    // The exact owed amounts apply_payout_policy refuses to settle.
    let floor = PayoutPolicy::day_one_default().min_output_sats().unwrap();
    let mut recipients = vec![SettlementRecipient {
        recipient_id: "dominant".to_string(),
        order_key: "00".to_string(),
        p2mr_program_hex: p2mr(0),
        amount_sats: COINBASE - DUST_MINERS,
    }];
    for i in 0..DUST_MINERS {
        recipients.push(SettlementRecipient {
            recipient_id: format!("dust-{i}"),
            order_key: format!("{:02}", i + 1),
            p2mr_program_hex: p2mr((i + 1) as u8),
            amount_sats: 1,
        });
    }

    let decision =
        select_settlement_mode(&recipients, floor, &SettlementModeConfig::default()).unwrap();

    // Designed path does NOT refuse: dominant settles direct, dust routes to a
    // non-custodial CTV fanout chunk, and value is conserved.
    assert_eq!(decision.mode, SettlementMode::HybridCoinbaseCtvFanout);
    assert_eq!(decision.direct_recipient_count, 1);
    assert_eq!(decision.fanout_recipient_count, DUST_MINERS as usize);
    assert_eq!(decision.total_amount_sats, COINBASE);
}
