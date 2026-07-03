//! Synthetic miner-distribution scale and property tests for PRISM settlement
//!
//!
//! These exercise the reward-window + payout-policy engine on realistic pool
//! shapes at 500 / 5,000 / 50,000 miners and assert the production-grade
//! invariants:
//!
//! - **Conservation:** the sum of every on-chain output (miners + pool fee)
//!   equals the coinbase value exactly — no value is created or destroyed.
//! - **Floor / accrual:** sub-floor miners never produce a dust on-chain output;
//!   they carry forward instead, and a block in which *every* miner is below the
//!   floor cannot be force-settled on-chain (this is precisely the case that
//!   motivates CTV fanout.
//! - **Fee isolation:** the pool fee is a separate on-chain account, computed as
//!   `floor(coinbase * fee_bps / 10_000)`, carved off before miner allocation,
//!   and excluded from miner carry-forward.
//! - **Determinism:** identical shares in any input order produce a
//!   byte-identical settlement.
//! - **Reorg safety:** an immature block can be reversed (its balances vanish),
//!   but a matured payout cannot be silently reversed.
//!
//! No RNG crate is available in the workspace, so all distributions are
//! constructed deterministically.

use qbit_prism::{
    apply_estimated_proportional_fanout_fee, apply_payout_policy, build_maturity_entries,
    build_prism_reward_manifest, current_carry_forward_balances, reverse_disconnected_blocks,
    select_settlement_mode, update_maturity_states, AcceptedShare, CarryForwardBalance,
    FanoutFeeRatePolicy, FoundBlock, PayoutMaturityState, PayoutPolicy, PayoutPolicyAccountType,
    PayoutPolicyAction, PoolFeePolicy, PrismError, SettlementMode, SettlementModeConfig,
    SettlementRecipient, DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS,
    DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS, DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS,
    DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION, DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS,
    QBIT_COINBASE_MATURITY_BLOCKS,
};

const ANCHOR_MS: i64 = 1_000_000;
const DAY_ONE_FLOOR_SATS: u64 = 14_720;

/// 32-byte P2MR program as 64 hex chars, unique per miner index.
fn program_hex(index: u64) -> String {
    format!("{index:064x}")
}

fn miner_id(index: u64) -> String {
    format!("miner-{index}")
}

fn order_key(index: u64) -> String {
    format!("{index:08}")
}

fn share(index: u64, difficulty: u128, network_difficulty: u128) -> AcceptedShare {
    AcceptedShare {
        share_seq: index + 1,
        share_id: format!("s-{index}"),
        miner_id: miner_id(index),
        order_key: order_key(index),
        p2mr_program_hex: program_hex(index),
        share_difficulty: difficulty,
        network_difficulty,
        template_height: 100,
        job_id: format!("job-{index}"),
        job_issued_at_ms: ANCHOR_MS - 1,
        accepted_at_ms: ANCHOR_MS - 1,
        ntime: 1_800_000_000,
    }
}

/// `n` distinct miners, one unit of difficulty each. `network_difficulty` is set
/// to `n` so the window (`8 * n`) always covers the full `n` units of share
/// weight and every share is counted.
fn equal_weight_shares(n: u64) -> Vec<AcceptedShare> {
    (0..n).map(|i| share(i, 1, n as u128)).collect()
}

fn found_block(n: u64, coinbase_value_sats: u64) -> FoundBlock {
    FoundBlock {
        block_height: 100,
        coinbase_value_sats,
        network_difficulty: n as u128,
        anchor_job_issued_at_ms: ANCHOR_MS,
    }
}

fn onchain_sum(manifest: &qbit_prism::PayoutPolicyManifest) -> u64 {
    manifest
        .onchain_entitlements
        .iter()
        .map(|entitlement| entitlement.weight as u64)
        .sum()
}

// ===========================================================================
// Scenario A — large miners that all cross the floor settle directly on-chain.
// Driven at 500 / 5,000 / 50,000.
// ===========================================================================

fn assert_all_above_floor_settles_directly(n: u64) {
    // coinbase / n == 20_000 sats per miner, comfortably above the 14_720 floor.
    let coinbase = n * 20_000;
    let shares = equal_weight_shares(n);
    let reward = build_prism_reward_manifest(&shares, &found_block(n, coinbase)).unwrap();
    let manifest = apply_payout_policy(&reward, &[], &PayoutPolicy::day_one_default()).unwrap();

    // Every miner gets exactly one on-chain output.
    assert_eq!(manifest.onchain_entitlements.len() as u64, n);
    assert_eq!(manifest.accounts.len() as u64, n);
    // Conservation: on-chain outputs sum to the entire coinbase.
    assert_eq!(onchain_sum(&manifest), coinbase);
    // No dust: every output is at or above the floor, and nothing accrues.
    assert!(manifest
        .onchain_entitlements
        .iter()
        .all(|e| e.weight as u64 >= DAY_ONE_FLOOR_SATS));
    assert!(manifest
        .accounts
        .iter()
        .all(|a| a.action == PayoutPolicyAction::Onchain
            && a.account_type == PayoutPolicyAccountType::Miner
            && a.carry_forward_balance_sats == 0));
    // Nobody is owed anything when everyone is paid in full.
    assert!(
        current_carry_forward_balances(&build_maturity_entries("block-a", 100, &manifest))
            .iter()
            .all(|b| b.balance_sats == 0)
    );
}

#[test]
fn scale_500_large_miners_settle_directly() {
    assert_all_above_floor_settles_directly(500);
}

#[test]
fn scale_5k_large_miners_settle_directly() {
    assert_all_above_floor_settles_directly(5_000);
}

#[test]
fn scale_50k_large_miners_settle_directly() {
    assert_all_above_floor_settles_directly(50_000);
}

// ===========================================================================
// Scenario B — thousands of sub-floor miners. With no on-chain recipient able
// to cover the coinbase, direct settlement is correctly refused. This is the
// distribution that must route through CTV fanout via CTV fanout rather than minting a
// custodial holding output.
// ===========================================================================

#[test]
fn scale_5k_all_sub_floor_block_cannot_settle_directly() {
    let n = 5_000;
    // coinbase / n == 1_000 sats per miner, far below the 14_720 floor.
    let coinbase = n * 1_000;
    let shares = equal_weight_shares(n);
    let reward = build_prism_reward_manifest(&shares, &found_block(n, coinbase)).unwrap();

    let err = apply_payout_policy(&reward, &[], &PayoutPolicy::day_one_default()).unwrap_err();
    assert!(
        matches!(
            err,
            PrismError::NoOnchainRecipients | PrismError::PayoutExceedsCandidateBalance { .. }
        ),
        "expected a no-direct-settlement error, got {err:?}"
    );
}

// ===========================================================================
// Scenario B2 — non-DATUM launch settlement. One conservative Stratum template
// must stay small, so economically payable miners below the direct threshold
// route through CTV fanout chunks instead of becoming direct coinbase outputs.
// ===========================================================================

fn settlement_recipient(index: u64, amount_sats: u64) -> SettlementRecipient {
    SettlementRecipient {
        recipient_id: miner_id(index),
        order_key: order_key(index),
        p2mr_program_hex: program_hex(index),
        amount_sats,
    }
}

#[test]
fn scale_non_datum_many_payable_miners_use_ctv_not_coinbase_fanout() {
    let recipients = (0..5_000)
        .map(|index| settlement_recipient(index, DAY_ONE_FLOOR_SATS + 1))
        .collect::<Vec<_>>();

    let decision = select_settlement_mode(
        &recipients,
        DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
        &SettlementModeConfig::default(),
    )
    .unwrap();

    assert_eq!(decision.mode, SettlementMode::CtvFanout);
    assert_eq!(decision.direct_recipient_count, 0);
    assert_eq!(decision.fanout_recipient_count, 5_000);
    assert_eq!(decision.fanout_chunk_count, 5);
    assert_eq!(decision.coinbase_settlement_output_count, 5);
    assert!(decision.coinbase_settlement_output_count <= DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS);
}

#[test]
fn scale_non_datum_whales_direct_mid_sized_miners_ctv() {
    let mut recipients = (0..20)
        .map(|index| settlement_recipient(index, DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS + index))
        .collect::<Vec<_>>();
    recipients.extend((20..3_020).map(|index| settlement_recipient(index, DAY_ONE_FLOOR_SATS + 1)));

    let decision = select_settlement_mode(
        &recipients,
        DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
        &SettlementModeConfig::default(),
    )
    .unwrap();

    assert_eq!(decision.mode, SettlementMode::HybridCoinbaseCtvFanout);
    assert_eq!(
        decision.direct_recipient_count,
        DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS
    );
    assert_eq!(decision.fanout_recipient_count, 3_008);
    assert_eq!(decision.fanout_chunk_count, 4);
    assert_eq!(
        decision.coinbase_settlement_output_count,
        DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS
    );
}

#[test]
fn scale_non_datum_rejects_more_chunks_than_single_template_budget() {
    let recipients = (0..(DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS
        * DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION
        + 1) as u64)
        .map(|index| settlement_recipient(index, DAY_ONE_FLOOR_SATS + 1))
        .collect::<Vec<_>>();

    let err = select_settlement_mode(
        &recipients,
        DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
        &SettlementModeConfig::default(),
    )
    .unwrap_err();

    assert!(err.to_string().contains("more than 16"));
}

#[test]
fn scale_ctv_fanout_fee_premium_is_integer_conserved() {
    let recipients = (0..1_000)
        .map(|index| settlement_recipient(index, 25_000))
        .collect::<Vec<_>>();
    let fee_policy = FanoutFeeRatePolicy::new(1_000, DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS);

    let decision =
        apply_estimated_proportional_fanout_fee(&recipients, DAY_ONE_FLOOR_SATS, &fee_policy)
            .unwrap();

    assert_eq!(decision.total_gross_sats, 25_000_000);
    assert_eq!(decision.requested_fee_sats, decision.applied_fee_sats);
    assert_eq!(decision.requested_fee_sats, 51_712);
    assert_eq!(decision.payable_recipients.len(), 1_000);
    assert!(decision.carry_forward_recipients.is_empty());
    assert!(decision
        .payable_recipients
        .iter()
        .all(|recipient| recipient.net_amount_sats >= DAY_ONE_FLOOR_SATS));

    let net_sum = decision
        .payable_recipients
        .iter()
        .map(|recipient| recipient.net_amount_sats)
        .sum::<u64>();
    assert_eq!(
        net_sum + decision.applied_fee_sats,
        decision.total_gross_sats
    );
}

#[test]
fn scale_ctv_fanout_fee_carries_recipients_that_fee_would_turn_into_dust() {
    let mut recipients = (0..1_000)
        .map(|index| settlement_recipient(index, 25_000))
        .collect::<Vec<_>>();
    recipients.extend((1_000..1_200).map(|index| settlement_recipient(index, 14_750)));
    let fee_policy = FanoutFeeRatePolicy::new(1_000, DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS);

    let decision =
        apply_estimated_proportional_fanout_fee(&recipients, DAY_ONE_FLOOR_SATS, &fee_policy)
            .unwrap();

    assert_eq!(decision.total_gross_sats, 27_950_000);
    assert_eq!(decision.requested_fee_sats, 62_032);
    assert_eq!(decision.applied_fee_sats, decision.requested_fee_sats);
    assert_eq!(decision.payable_recipients.len(), 1_000);
    assert_eq!(decision.carry_forward_recipients.len(), 200);
    assert!(decision
        .payable_recipients
        .iter()
        .all(|recipient| recipient.net_amount_sats >= DAY_ONE_FLOOR_SATS));
    assert!(decision.carry_forward_recipients.iter().all(|recipient| {
        recipient.fee_sats == 0 && recipient.net_amount_sats == recipient.gross_amount_sats
    }));

    let payable_gross_sum = decision
        .payable_recipients
        .iter()
        .map(|recipient| recipient.gross_amount_sats)
        .sum::<u64>();
    let payable_net_sum = decision
        .payable_recipients
        .iter()
        .map(|recipient| recipient.net_amount_sats)
        .sum::<u64>();
    assert_eq!(
        payable_net_sum + decision.applied_fee_sats,
        payable_gross_sum
    );
}

// ===========================================================================
// Scenario C — power-law block: one whale plus thousands of sub-floor miners.
// The whale (with carried prior balance) absorbs the coinbase on-chain while
// every small miner accrues, and carry-forward is conserved.
// ===========================================================================

fn power_law_positive_carry_manifest() -> (qbit_prism::PayoutPolicyManifest, u64, u64) {
    let dust_count: u64 = 4_999;
    let whale_weight: u128 = 5_000;
    let total_weight = whale_weight + dust_count as u128; // 9_999
                                                          // coinbase / total_weight == 10_000 sats/unit: dust (weight 1) lands at
                                                          // 10_000 (< floor, > 0) and accrues; the whale lands at 50_000_000.
    let coinbase: u64 = 10_000 * total_weight as u64; // 99_990_000
    let network_difficulty = total_weight; // window 8*total_weight covers all weight

    let mut shares = vec![share(0, whale_weight, network_difficulty)];
    shares.extend((1..=dust_count).map(|i| share(i, 1, network_difficulty)));

    let reward =
        build_prism_reward_manifest(&shares, &found_block(network_difficulty as u64, coinbase))
            .unwrap();

    // The whale carries enough prior balance to cover the coinbase once the
    // sub-floor dust is redistributed onto it.
    let whale_prior = CarryForwardBalance {
        recipient_id: miner_id(0),
        order_key: order_key(0),
        p2mr_program_hex: program_hex(0),
        balance_sats: coinbase as i128,
    };
    let manifest =
        apply_payout_policy(&reward, &[whale_prior], &PayoutPolicy::day_one_default()).unwrap();

    (manifest, dust_count, coinbase)
}

#[test]
fn scale_power_law_whale_settles_small_miners_accrue() {
    let (manifest, dust_count, coinbase) = power_law_positive_carry_manifest();

    // Exactly one on-chain recipient (the whale), absorbing the whole coinbase.
    assert_eq!(manifest.onchain_entitlements.len(), 1);
    assert_eq!(onchain_sum(&manifest), coinbase);
    let whale = manifest
        .accounts
        .iter()
        .find(|a| a.recipient_id == miner_id(0))
        .unwrap();
    assert_eq!(whale.action, PayoutPolicyAction::Onchain);
    assert_eq!(whale.onchain_amount_sats, coinbase);

    // Every dust miner accrues a positive, sub-floor balance and pays nothing
    // on-chain.
    let accrued: Vec<_> = manifest
        .accounts
        .iter()
        .filter(|a| a.action == PayoutPolicyAction::Accrued)
        .collect();
    assert_eq!(accrued.len() as u64, dust_count);
    assert!(accrued.iter().all(|a| a.onchain_amount_sats == 0
        && a.carry_forward_balance_sats > 0
        && (a.carry_forward_balance_sats as u64) < DAY_ONE_FLOOR_SATS));

    // Carry-forward conservation: with no fee, prior + coinbase must equal the
    // new carry-forward total plus what was paid on-chain.
    let carry_total: i128 = manifest
        .accounts
        .iter()
        .map(|a| a.carry_forward_balance_sats)
        .sum();
    // prior_total + miner_reward == new carry-forward total + on-chain paid.
    let prior_total = coinbase as i128;
    let miner_reward = coinbase as i128; // no pool fee in this scenario
    let onchain_total = coinbase as i128;
    assert_eq!(prior_total + miner_reward, carry_total + onchain_total);
}

// ===========================================================================
// Pool-fee isolation at scale: the fee is a distinct on-chain account, equal to
// floor(coinbase * fee_bps / 10_000), separate from miner liabilities.
// ===========================================================================

#[test]
fn scale_5k_pool_fee_is_isolated_from_miner_payouts() {
    let n = 5_000;
    let coinbase = n * 20_000; // 100_000_000
    let fee_bps: u16 = 250; // 2.5%
    let shares = equal_weight_shares(n);
    let reward = build_prism_reward_manifest(&shares, &found_block(n, coinbase)).unwrap();

    let mut policy = PayoutPolicy::day_one_default();
    policy.pool_fee_policy = Some(PoolFeePolicy {
        fee_bps,
        recipient_id: "pool-fee".to_string(),
        order_key: "zzzzzzzz".to_string(),
        p2mr_program_hex: program_hex(0xFFFF_FFFF),
    });
    let manifest = apply_payout_policy(&reward, &[], &policy).unwrap();

    let expected_fee = coinbase * u64::from(fee_bps) / 10_000;
    let pool_fee = manifest.pool_fee.as_ref().expect("pool fee present");
    assert_eq!(pool_fee.amount_sats, expected_fee);

    // The fee account is typed as a pool fee, not a miner liability.
    let fee_account = manifest
        .accounts
        .iter()
        .find(|a| a.account_type == PayoutPolicyAccountType::PoolFee)
        .expect("pool fee account present");
    assert_eq!(fee_account.recipient_id, "pool-fee");
    assert_eq!(fee_account.onchain_amount_sats, expected_fee);

    // Conservation across the whole coinbase, and miner outputs equal the
    // post-fee reward exactly.
    assert_eq!(onchain_sum(&manifest), coinbase);
    let miner_onchain: u64 = manifest
        .accounts
        .iter()
        .filter(|a| a.account_type == PayoutPolicyAccountType::Miner)
        .map(|a| a.onchain_amount_sats)
        .sum();
    assert_eq!(miner_onchain, coinbase - expected_fee);

    // The pool fee never appears in miner carry-forward state.
    let carry = current_carry_forward_balances(&build_maturity_entries("block-a", 100, &manifest));
    assert!(carry.iter().all(|b| b.recipient_id != "pool-fee"));
}

#[test]
fn scale_pool_fee_can_route_through_hybrid_settlement_without_miner_carry() {
    let coinbase = 1_000_000;
    let fee_bps: u16 = 200;
    let direct_floor = 500_000;
    let shares = vec![share(0, 3, 5), share(1, 2, 5)];
    let reward = build_prism_reward_manifest(&shares, &found_block(5, coinbase)).unwrap();

    let mut policy = PayoutPolicy::day_one_default();
    policy.pool_fee_policy = Some(PoolFeePolicy {
        fee_bps,
        recipient_id: "pool-fee".to_string(),
        order_key: "zzzzzzzz".to_string(),
        p2mr_program_hex: program_hex(0xFFFF_FFFF),
    });
    let manifest = apply_payout_policy(&reward, &[], &policy).unwrap();

    let pool_fee = manifest.pool_fee.as_ref().expect("pool fee present");
    assert_eq!(pool_fee.amount_sats, 20_000);
    let fee_account = manifest
        .accounts
        .iter()
        .find(|account| account.account_type == PayoutPolicyAccountType::PoolFee)
        .expect("pool fee account present");
    assert_eq!(fee_account.onchain_amount_sats, 20_000);
    assert_eq!(fee_account.carry_forward_balance_sats, 0);

    let recipients = manifest
        .onchain_entitlements
        .iter()
        .map(|entitlement| SettlementRecipient {
            recipient_id: entitlement.recipient_id.clone(),
            order_key: entitlement.order_key.clone(),
            p2mr_program_hex: entitlement.p2mr_program_hex.clone(),
            amount_sats: entitlement.weight as u64,
        })
        .collect::<Vec<_>>();
    let decision =
        select_settlement_mode(&recipients, direct_floor, &SettlementModeConfig::default())
            .unwrap();

    assert_eq!(decision.mode, SettlementMode::HybridCoinbaseCtvFanout);
    assert_eq!(
        decision
            .direct_recipients
            .iter()
            .map(|recipient| recipient.recipient_id.as_str())
            .collect::<Vec<_>>(),
        vec!["miner-0"]
    );
    assert_eq!(
        decision
            .fanout_chunks
            .iter()
            .flat_map(|chunk| chunk.recipients.iter())
            .map(|recipient| recipient.recipient_id.as_str())
            .collect::<Vec<_>>(),
        vec!["miner-1", "pool-fee"]
    );
    assert_eq!(
        decision.direct_amount_sats + decision.fanout_amount_sats,
        coinbase
    );

    let carry =
        current_carry_forward_balances(&build_maturity_entries("block-hybrid-fee", 100, &manifest));
    assert!(carry
        .iter()
        .all(|balance| balance.recipient_id != "pool-fee"));
}

// ===========================================================================
// Determinism: shuffled share input yields a byte-identical settlement.
// ===========================================================================

#[test]
fn scale_settlement_is_order_independent() {
    let n = 5_000;
    let coinbase = n * 20_000;
    let ordered = equal_weight_shares(n);
    let mut shuffled = ordered.clone();
    // A deterministic non-trivial permutation (reverse) — the engine sorts
    // internally, so the result must not depend on input order.
    shuffled.reverse();

    let policy = PayoutPolicy::day_one_default();
    let from_ordered = apply_payout_policy(
        &build_prism_reward_manifest(&ordered, &found_block(n, coinbase)).unwrap(),
        &[],
        &policy,
    )
    .unwrap();
    let from_shuffled = apply_payout_policy(
        &build_prism_reward_manifest(&shuffled, &found_block(n, coinbase)).unwrap(),
        &[],
        &policy,
    )
    .unwrap();

    assert_eq!(from_ordered, from_shuffled);
}

// ===========================================================================
// Reorg replay at scale: an immature block reverses cleanly; a matured one is
// refused.
// ===========================================================================

#[test]
fn scale_5k_reorg_replay_reverses_immature_refuses_mature() {
    let height = 100;
    let (manifest, _, _) = power_law_positive_carry_manifest();
    let entries = build_maturity_entries("block-a", height, &manifest);
    let expected_positive_carry: i128 = manifest
        .accounts
        .iter()
        .map(|account| account.carry_forward_balance_sats.max(0))
        .sum();
    let before_reversal = current_carry_forward_balances(&entries);

    let just_immature = height + QBIT_COINBASE_MATURITY_BLOCKS - 1;
    let just_mature = height + QBIT_COINBASE_MATURITY_BLOCKS;

    assert!(expected_positive_carry > 0);
    assert!(before_reversal
        .iter()
        .any(|balance| balance.balance_sats > 0));

    // Immature → still reversible, and reversal drops all of the block's state.
    assert!(update_maturity_states(&entries, just_immature)
        .iter()
        .all(|e| e.state == PayoutMaturityState::Immature));
    let reversed =
        reverse_disconnected_blocks(&entries, &["block-a".to_string()], just_immature).unwrap();
    assert_eq!(reversed.len(), entries.len());
    assert!(reversed
        .iter()
        .all(|e| e.state == PayoutMaturityState::Reversed));
    assert!(current_carry_forward_balances(&reversed).is_empty());

    // Mature → reversal is refused (a settled payout cannot be silently undone).
    let mature_entries = update_maturity_states(&entries, just_mature);
    assert!(mature_entries
        .iter()
        .all(|e| e.state == PayoutMaturityState::Mature));
    assert!(
        reverse_disconnected_blocks(&mature_entries, &["block-a".to_string()], just_mature)
            .is_err(),
        "matured payout must not be reversible"
    );
}
