//! A CTV fanout recipient just above the payout floor no longer refuses the
//! whole audit build.
//!
//! The smallest reproducer of the refusals measured at the production
//! settlement settings: a 200 bps pool-fee-first fee, the default 14,720-sat
//! payout floor and 10,485,760-sat direct floor, 16/12/1,000 output limits and
//! a 1,000 sat per 1,000 weight market rate at the default 1.2x premium. In
//! every refused build one fanout recipient a few sats above the floor drew a
//! proportional fee share larger than that margin. Here a 14,721-sat recipient
//! shares a two-recipient fanout whose 213-sat fee gives it 1.70 sats plus the
//! largest remainder: 2 sats, leaving 14,719. The builder used to refuse with
//! "fanout fee would push a recipient below the payout floor"; now that
//! recipient pays its 1-sat slack and the other recipient pays the rest.

use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    apply_proportional_fanout_fee, build_audit_bundle_with_ctv_settlement_options,
    estimate_ctv_fanout_fee_sats, verify_audit_bundle, AcceptedShare, CoinbaseOutputPolicy,
    FanoutFeeRatePolicy, FoundBlock, PayoutPolicy, PoolFeePolicy, SettlementMode,
    SettlementModeConfig, SettlementRecipient, DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS,
    DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
};

const FLOOR_SATS: u64 = 14_720;
const COINBASE_SATS: u64 = 19_200_000_000;
/// The coinbase less the 200 bps pool fee.
const MINER_REWARD_SATS: u64 = 19_200_000_000 - 384_000_000;
const NEAR_FLOOR_SATS: u64 = 14_721;
const MID_SATS: u64 = 1_830_000;
const WHALE_SATS: u64 = MINER_REWARD_SATS - MID_SATS - NEAR_FLOOR_SATS;

fn program(byte: u8) -> String {
    hex::encode([byte; 32])
}

/// One share per miner. The window, eight times the network difficulty,
/// equals the miner reward, so each miner's gross equals its share weight.
fn share(seq: u64, miner: &str, byte: u8, difficulty: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: seq,
        share_id: format!("share-{seq}"),
        miner_id: miner.to_string(),
        order_key: miner.to_string(),
        p2mr_program_hex: program(byte),
        share_difficulty: u128::from(difficulty),
        network_difficulty: u128::from(MINER_REWARD_SATS / 8),
        template_height: 99,
        job_id: "job-1".to_string(),
        job_issued_at_ms: 1_000,
        accepted_at_ms: 1_000,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

fn production_policy() -> PayoutPolicy {
    let mut policy = PayoutPolicy::day_one_default();
    policy.pool_fee_policy = Some(PoolFeePolicy {
        fee_bps: 200,
        recipient_id: "pool-fee".to_string(),
        order_key: "pool-fee".to_string(),
        p2mr_program_hex: program(0xfe),
    });
    policy.coinbase_output_policy = CoinbaseOutputPolicy::PoolFeeFirst;
    policy
}

#[test]
fn a_fanout_recipient_one_sat_above_the_floor_is_capped_at_its_slack_and_the_bundle_verifies() {
    let fee_policy = FanoutFeeRatePolicy::new(1_000, DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS);
    let fanout_fee_sats = estimate_ctv_fanout_fee_sats(2, &fee_policy).unwrap();
    assert_eq!(fanout_fee_sats, 213);
    assert_eq!(production_policy().min_output_sats().unwrap(), FLOOR_SATS);

    // The proportional split leaves the near-floor miner at 14,719, which is
    // what refused the build before the cap.
    let chunk = [
        ("miner-mid", 2, MID_SATS),
        ("miner-near", 3, NEAR_FLOOR_SATS),
    ]
    .map(|(miner, byte, amount_sats)| SettlementRecipient {
        recipient_id: miner.to_string(),
        order_key: miner.to_string(),
        p2mr_program_hex: program(byte),
        amount_sats,
    });
    let proportional = apply_proportional_fanout_fee(&chunk, fanout_fee_sats, FLOOR_SATS).unwrap();
    assert_eq!(proportional.carry_forward_recipients.len(), 1);
    assert_eq!(
        proportional.carry_forward_recipients[0].recipient_id,
        "miner-near"
    );

    let signing_key = ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap();
    let ledger_key = ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap();
    let bundle = build_audit_bundle_with_ctv_settlement_options(
        vec![
            share(1, "miner-whale", 1, WHALE_SATS),
            share(2, "miner-mid", 2, MID_SATS),
            share(3, "miner-near", 3, NEAR_FLOOR_SATS),
        ],
        FoundBlock {
            block_height: 100,
            coinbase_value_sats: COINBASE_SATS,
            network_difficulty: u128::from(MINER_REWARD_SATS / 8),
            anchor_job_issued_at_ms: 1_000,
        },
        Vec::new(),
        production_policy(),
        DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
        SettlementModeConfig {
            max_coinbase_settlement_outputs: 16,
            max_direct_coinbase_outputs: 12,
            max_fanout_recipients_per_transaction: 1_000,
            reserved_coinbase_outputs: 0,
        },
        Some(fee_policy),
        Some("aaaaaaaa".to_string()),
        Vec::new(),
        &signing_key,
        &ledger_key,
    )
    .unwrap();

    let report = verify_audit_bundle(&bundle, &ledger_key.public_key_hex()).unwrap();
    assert_eq!(report.coinbase_value_sats, COINBASE_SATS);

    let decision = bundle.settlement_mode_decision.as_ref().unwrap();
    assert_eq!(decision.mode, SettlementMode::HybridCoinbaseCtvFanout);
    assert_eq!(
        decision
            .direct_recipients
            .iter()
            .map(|recipient| recipient.recipient_id.as_str())
            .collect::<Vec<_>>(),
        ["miner-whale", "pool-fee"]
    );
    let fanout_set = bundle.ctv_fanout_manifest_set.as_ref().unwrap();
    assert_eq!(fanout_set.manifests.len(), 1);
    assert_eq!(fanout_set.fanout_fee_sats, fanout_fee_sats);
    let outputs = &fanout_set.manifests[0].precommitment.outputs;
    assert!(outputs.iter().all(|output| output.amount_sats >= FLOOR_SATS
        && output.amount_sats + output.fee_sats == output.gross_amount_sats));
    assert_eq!(
        outputs
            .iter()
            .map(|output| (
                output.recipient_id.as_str(),
                output.gross_amount_sats,
                output.fee_sats,
                output.amount_sats
            ))
            .collect::<Vec<_>>(),
        [
            ("miner-mid", MID_SATS, 212, MID_SATS - 212),
            ("miner-near", NEAR_FLOOR_SATS, 1, FLOOR_SATS),
        ]
    );

    // The payout policy manifest records the same capped fees, and nothing is
    // carried: every miner is paid its gross on chain.
    for (miner, fee_sats) in [("miner-mid", 212), ("miner-near", 1), ("miner-whale", 0)] {
        let account = bundle
            .payout_policy_manifest
            .accounts
            .iter()
            .find(|account| account.recipient_id == miner)
            .unwrap();
        assert_eq!(account.settlement_fee_sats, fee_sats, "{miner}");
        assert_eq!(
            i128::from(account.onchain_amount_sats),
            account.candidate_balance_sats
        );
        assert_eq!(account.carry_forward_balance_sats, 0);
    }
    let first_output = &bundle.signed_coinbase_manifest.manifest.outputs[0];
    assert_eq!(first_output.recipient_id, "pool-fee");
    assert_eq!(first_output.amount_sats, 384_000_000);
}
