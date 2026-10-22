//! Export the money-path vectors frozen from 2.x.x.
//!
//! Built as a `qbit-prism` example inside a 2.x.x source tree; see README.md
//! in this directory for the exact regeneration command. Topics 1 to 7 call
//! the 2.x.x engine entry points directly. Topics 8 and 9 read the 2.x.x rule
//! decisions printed by `export_rule_decisions.py` on stdin, apply the 3.x.x
//! rule as implemented by the cited `qbit-prism-server` lines, and compute the
//! payout consequence of both rules with the same engine.

use std::fs;
use std::io::Read;
use std::path::Path;

use qbit_pool_builder::WeightedEntitlement;
use qbit_prism::{
    apply_payout_policy, apply_proportional_fanout_fee, build_prism_reward_manifest,
    compute_prism_window, select_settlement_mode, select_settlement_mode_with_pinned_direct,
    AcceptedShare, CarryForwardBalance, CoinbaseOutputPolicy, FoundBlock, PayoutPolicy,
    PoolFeePolicy, PrismError, PrismRewardManifest, SettlementModeConfig, SettlementRecipient,
    PRISM_WINDOW_MULTIPLIER,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};

const SCHEMA: &str = "qbit.prism.money-path-vectors.v1";
const SOURCE_COMMIT: &str = "504846cc0b72e8f86ed17f896d4ccbbe196a31dc";
const PRODUCED_BY: &str = "PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 crates/qbit-prism/examples/export_rule_decisions.py | cargo run --locked -q -p qbit-prism --example export_money_path_vectors -- vectors-out";

const D2A: &str = "d2a-bootstrap-pooling";
const D2B: &str = "d2b-below-target-credit";
const D2C: &str = "d2c-prior-balances-during-bootstrap";

fn to_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("vector values serialize")
}

fn field<T: DeserializeOwned>(input: &Value, name: &str) -> T {
    serde_json::from_value(input[name].clone()).unwrap_or_else(|err| panic!("{name}: {err}"))
}

fn outcome<T: Serialize>(result: Result<T, PrismError>) -> Value {
    match result {
        Ok(value) => json!({ "ok": to_value(&value) }),
        Err(err) => json!({ "error": err.to_string() }),
    }
}

/// Evaluate one case input through its entry point. The 3.x.x consumer
/// (`crates/qbit-prism/tests/money_path_vectors.rs`) mirrors this dispatcher.
fn run(entry_point: &str, input: &Value) -> Value {
    match entry_point {
        "compute_prism_window" => outcome(compute_prism_window(
            &field::<Vec<AcceptedShare>>(input, "shares"),
            &field(input, "found_block"),
        )),
        "apply_payout_policy" => outcome(apply_payout_policy(
            &field(input, "reward_manifest"),
            &field::<Vec<CarryForwardBalance>>(input, "prior_balances"),
            &field(input, "policy"),
        )),
        "apply_proportional_fanout_fee" => outcome(apply_proportional_fanout_fee(
            &field::<Vec<SettlementRecipient>>(input, "recipients"),
            field(input, "fee_sats"),
            field(input, "min_output_sats"),
        )),
        "select_settlement_mode" => {
            let recipients = field::<Vec<SettlementRecipient>>(input, "recipients");
            let floor = field(input, "direct_floor_sats");
            let config = field::<SettlementModeConfig>(input, "config");
            match field::<Option<SettlementRecipient>>(input, "pinned_direct") {
                Some(pinned) => outcome(select_settlement_mode_with_pinned_direct(
                    &recipients,
                    floor,
                    &config,
                    Some(&pinned),
                )),
                None => outcome(select_settlement_mode(&recipients, floor, &config)),
            }
        }
        "bundle_payout" => outcome(bundle_payout(input)),
        other => panic!("unknown entry point {other}"),
    }
}

/// The payout consequence of one bundle's economic inputs: the reward window
/// and the payout policy manifest the coinbase is built from.
fn bundle_payout(input: &Value) -> Result<Value, PrismError> {
    let reward = build_prism_reward_manifest(
        &field::<Vec<AcceptedShare>>(input, "shares"),
        &field(input, "found_block"),
    )?;
    let payout = apply_payout_policy(
        &reward,
        &field::<Vec<CarryForwardBalance>>(input, "prior_balances"),
        &field(input, "policy"),
    )?;
    Ok(json!({
        "counted_window_weight": to_value(&reward.counted_window_weight),
        "counted_shares": reward
            .shares
            .iter()
            .map(|share| json!({
                "share_seq": share.share_seq,
                "miner_id": share.miner_id,
                "counted_difficulty": to_value(&share.counted_difficulty),
            }))
            .collect::<Vec<_>>(),
        "entitlements": to_value(&reward.entitlements),
        "payout_policy_manifest": to_value(&payout),
    }))
}

fn case(topic: u8, name: &str, why: &str, entry_point: &str, input: Value) -> Value {
    let expected = run(entry_point, &input);
    json!({
        "name": name,
        "topic": topic,
        "why": why,
        "entry_point": entry_point,
        "input": input,
        "expected": expected,
    })
}

/// A rule case computes the payout under both rules. Equal payouts carry one
/// `expected`; a difference must be a listed D2 entry, or export stops.
fn rule_case(
    topic: u8,
    name: &str,
    why: &str,
    rule: Value,
    bundle_2xx: Value,
    bundle_3xx: Value,
    d2_entry: Option<&str>,
) -> Value {
    let expected_2xx = run("bundle_payout", &bundle_2xx);
    let expected_3xx = run("bundle_payout", &bundle_3xx);
    let input = json!({ "rule": rule, "bundle_2xx": bundle_2xx, "bundle_3xx": bundle_3xx });
    let mut case = json!({
        "name": name,
        "topic": topic,
        "why": why,
        "entry_point": "bundle_payout",
        "input": input,
    });
    match (expected_2xx == expected_3xx, d2_entry) {
        (true, None) => case["expected"] = expected_3xx,
        (false, Some(anchor)) => {
            case["expected_2xx"] = expected_2xx;
            case["expected_3xx"] = expected_3xx;
            case["d2_entry"] = json!(anchor);
        }
        (true, Some(anchor)) => panic!("{name}: listed as {anchor} but both rules pay the same"),
        (false, None) => panic!("{name}: undocumented 2.x.x/3.x.x payout difference; stop and ask"),
    }
    case
}

fn program(byte: u8) -> String {
    hex::encode([byte; 32])
}

// ---------------------------------------------------------------------------
// Topics 1 and 2: compute_prism_window

fn share(
    share_seq: u64,
    miner_id: &str,
    order_key: &str,
    byte: u8,
    share_difficulty: u128,
    job_issued_at_ms: i64,
) -> AcceptedShare {
    AcceptedShare {
        share_seq,
        share_id: format!("share-{share_seq}"),
        miner_id: miner_id.to_string(),
        order_key: order_key.to_string(),
        p2mr_program_hex: program(byte),
        share_difficulty,
        network_difficulty: 10,
        template_height: 100,
        job_id: format!("job-{job_issued_at_ms}"),
        job_issued_at_ms,
        accepted_at_ms: job_issued_at_ms,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

fn found(network_difficulty: u128, anchor_job_issued_at_ms: i64) -> FoundBlock {
    FoundBlock {
        block_height: 101,
        coinbase_value_sats: 500_000_000,
        network_difficulty,
        anchor_job_issued_at_ms,
    }
}

fn window(shares: Vec<AcceptedShare>, found_block: FoundBlock) -> Value {
    json!({ "shares": to_value(&shares), "found_block": to_value(&found_block) })
}

fn boundary_shares() -> Vec<AcceptedShare> {
    vec![
        share(1, "miner-old", "01", 1, 999, 1000),
        share(2, "miner-b", "02", 2, 40, 1001),
        share(3, "miner-a", "03", 3, 30, 1002),
        share(4, "miner-c", "04", 4, 50, 1003),
    ]
}

fn window_cases() -> Vec<Value> {
    let w = "compute_prism_window";
    let mut after_anchor = share(3, "miner-late", "03", 3, 1000, 2000);
    after_anchor.accepted_at_ms = 1500;
    let mut accepted_after_anchor = share(4, "miner-slow", "04", 4, 1000, 1000);
    accepted_after_anchor.accepted_at_ms = 1002;
    let mut unsorted = boundary_shares();
    unsorted.swap(0, 2);
    unsorted.swap(1, 3);
    vec![
        case(
            1,
            "exact-fit-boundary",
            "8 x 10 = 80 is filled exactly by the two newest shares (50 + 30); the older shares get nothing.",
            w,
            window(boundary_shares(), found(10, 1003)),
        ),
        case(
            2,
            "fractional-oldest-share",
            "The same shares with network difficulty 9: 72 = 50 + 22, so the oldest counted share counts 22 of its 30.",
            w,
            window(boundary_shares(), found(9, 1003)),
        ),
        case(
            2,
            "oldest-share-counts-one-unit",
            "Weight 8 after a 7-difficulty share leaves exactly one unit for a 1000-difficulty share.",
            w,
            window(
                vec![
                    share(1, "miner-a", "01", 1, 1000, 1000),
                    share(2, "miner-b", "02", 2, 7, 1001),
                ],
                found(1, 1001),
            ),
        ),
        case(
            1,
            "small-log-below-weight",
            "The whole eligible log (60) is below the requested weight (800), so every share counts in full.",
            w,
            window(
                vec![
                    share(1, "miner-a", "01", 1, 10, 1000),
                    share(2, "miner-b", "02", 2, 20, 1001),
                    share(3, "miner-a", "01", 1, 30, 1002),
                ],
                found(100, 1002),
            ),
        ),
        case(
            1,
            "shares-after-anchor-excluded",
            "A share whose job was issued after the anchor, and one issued before it but accepted after it, are both excluded.",
            w,
            window(
                vec![
                    share(1, "miner-a", "01", 1, 10, 1000),
                    share(2, "miner-b", "02", 2, 20, 1001),
                    after_anchor,
                    accepted_after_anchor,
                ],
                found(10, 1001),
            ),
        ),
        case(
            1,
            "unsorted-input",
            "The boundary shares in scrambled sequence order give the same newest-first window as sorted input.",
            w,
            window(unsorted, found(9, 1003)),
        ),
        case(
            1,
            "zero-network-difficulty-is-an-error",
            "A found block with network difficulty 0 cannot define a window.",
            w,
            window(boundary_shares(), found(0, 1003)),
        ),
        case(
            1,
            "no-eligible-share-is-an-error",
            "Every share is after the anchor, so the window is empty.",
            w,
            window(vec![share(1, "miner-a", "01", 1, 10, 2000)], found(10, 1000)),
        ),
    ]
}

// ---------------------------------------------------------------------------
// Topics 3 and 4: apply_payout_policy

fn reward(coinbase_value_sats: u64, entitlements: &[(&str, &str, String, u128)]) -> PrismRewardManifest {
    PrismRewardManifest {
        schema: "qbit.prism.reward-manifest.v1".to_string(),
        block_height: 10,
        coinbase_value_sats,
        network_difficulty: 1,
        window_multiplier: PRISM_WINDOW_MULTIPLIER,
        requested_window_weight: 8,
        counted_window_weight: entitlements.iter().map(|entry| entry.3).sum(),
        anchor_job_issued_at_ms: 1,
        anchor_share_seq: 1,
        newest_share_seq: 1,
        oldest_share_seq: 1,
        included_share_count: entitlements.len(),
        share_slice_digest_hex: "00".repeat(32),
        shares: Vec::new(),
        entitlements: entitlements
            .iter()
            .map(|(recipient_id, order_key, p2mr_program_hex, weight)| WeightedEntitlement {
                recipient_id: recipient_id.to_string(),
                order_key: order_key.to_string(),
                p2mr_program_hex: p2mr_program_hex.clone(),
                weight: *weight,
            })
            .collect(),
    }
}

fn prior(recipient_id: &str, order_key: &str, p2mr_program_hex: String, balance_sats: i128) -> CarryForwardBalance {
    CarryForwardBalance {
        recipient_id: recipient_id.to_string(),
        order_key: order_key.to_string(),
        p2mr_program_hex,
        balance_sats,
    }
}

fn fee_policy(fee_bps: u16, output_policy: CoinbaseOutputPolicy) -> PayoutPolicy {
    let mut policy = PayoutPolicy::day_one_default();
    policy.pool_fee_policy = Some(PoolFeePolicy {
        fee_bps,
        recipient_id: "pool-fee".to_string(),
        order_key: "99".to_string(),
        p2mr_program_hex: program(0x99),
    });
    policy.coinbase_output_policy = output_policy;
    policy
}

fn policy_input(
    reward_manifest: PrismRewardManifest,
    prior_balances: Vec<CarryForwardBalance>,
    policy: PayoutPolicy,
) -> Value {
    json!({
        "reward_manifest": to_value(&reward_manifest),
        "prior_balances": to_value(&prior_balances),
        "policy": to_value(&policy),
    })
}

fn carry_only_cases() -> Vec<Value> {
    let p = "apply_payout_policy";
    let day_one = PayoutPolicy::day_one_default;
    let one_miner = || reward(500_000_000, &[("miner-a", "01", program(1), 1)]);
    let alias_upper = program(0xab).to_uppercase();
    vec![
        case(
            3,
            "prior-only-account-crosses-floor",
            "An account with no share in the window but a 15000-sat carry (floor 14720) is a payout candidate on its own.",
            p,
            policy_input(one_miner(), vec![prior("carry-only", "02", program(2), 15_000)], day_one()),
        ),
        case(
            3,
            "prior-only-account-exactly-at-floor",
            "A carry of exactly 14720 is eligible, but its proportional share of the coinbase can land a sat under the floor.",
            p,
            policy_input(one_miner(), vec![prior("carry-only", "02", program(2), 14_720)], day_one()),
        ),
        case(
            3,
            "prior-only-account-one-sat-under-floor",
            "A carry of 14719 is not eligible and keeps accruing; the miner is paid the whole coinbase.",
            p,
            policy_input(one_miner(), vec![prior("carry-only", "02", program(2), 14_719)], day_one()),
        ),
        case(
            3,
            "prior-plus-gross-crosses-floor",
            "A 5000-sat gross plus a 12000-sat carry crosses the floor and is paid on-chain.",
            p,
            policy_input(
                reward(100_000, &[("large", "01", program(1), 95), ("small", "02", program(2), 5)]),
                vec![prior("small", "02", program(2), 12_000)],
                day_one(),
            ),
        ),
        case(
            3,
            "aliases-of-one-program-aggregate",
            "Two recipient ids, and carry rows in upper- and lower-case hex, for one payout program aggregate into one account before floor selection.",
            p,
            policy_input(
                reward(
                    100_000,
                    &[
                        ("miner-a", "01", program(1), 90),
                        ("alias-x", "05", program(0xab), 5),
                        ("alias-y", "03", alias_upper.clone(), 5),
                    ],
                ),
                vec![
                    prior("alias-y", "03", alias_upper, 4_000),
                    prior("alias-z", "07", program(0xab), 3_000),
                ],
                day_one(),
            ),
        ),
        case(
            3,
            "overpayment-debt-reduces-candidate",
            "A negative carry (overpayment debt after a reorg) reduces that account's candidate balance and its payout.",
            p,
            policy_input(
                reward(100_000, &[("miner-a", "01", program(1), 50), ("miner-b", "02", program(2), 50)]),
                vec![
                    prior("miner-a", "01", program(1), 10_000),
                    prior("miner-b", "02", program(2), -5_000),
                ],
                day_one(),
            ),
        ),
        case(
            3,
            "overpayment-debt-without-fee-is-an-error",
            "With debt and no other carry the eligible candidates sum below the coinbase, and without a pool fee to absorb it the policy fails.",
            p,
            policy_input(
                reward(100_000, &[("miner-a", "01", program(1), 50), ("miner-b", "02", program(2), 50)]),
                vec![prior("miner-b", "02", program(2), -5_000)],
                day_one(),
            ),
        ),
    ]
}

fn pool_fee_cases() -> Vec<Value> {
    let p = "apply_payout_policy";
    let two_miners = |coinbase| {
        reward(coinbase, &[("miner-a", "01", program(1), 1), ("miner-b", "02", program(2), 2)])
    };
    vec![
        case(
            4,
            "fee-125-bps-non-divisible-coinbase",
            "100003 x 125 / 10000 = 1250.0375 rounds down to 1250; the miners split the rest 1:2 with the remainder sat by largest remainder.",
            p,
            policy_input(two_miners(100_003), vec![], fee_policy(125, CoinbaseOutputPolicy::Canonical)),
        ),
        case(
            4,
            "fee-125-bps-three-equal-miners",
            "500000003 x 125 / 10000 rounds down to 6250000; 493750003 split three ways leaves one remainder sat for the first account in canonical order.",
            p,
            policy_input(
                reward(
                    500_000_003,
                    &[
                        ("miner-a", "01", program(1), 1),
                        ("miner-b", "02", program(2), 1),
                        ("miner-c", "03", program(3), 1),
                    ],
                ),
                vec![],
                fee_policy(125, CoinbaseOutputPolicy::Canonical),
            ),
        ),
        case(
            4,
            "fee-0-bps-sweeps-sub-floor-dust",
            "A zero-bps fee earns nothing, but the sub-floor miner's allocation is swept into the pool-fee output.",
            p,
            policy_input(
                reward(100_000, &[("miner-a", "01", program(1), 99_999), ("dust", "02", program(2), 1)]),
                vec![],
                fee_policy(0, CoinbaseOutputPolicy::Canonical),
            ),
        ),
        case(
            4,
            "fee-first-with-sub-floor-fee",
            "Pool-fee-first with a 10-sat fee (1 bps of 100000): the fee stays an on-chain account even though it is below the 14720 floor.",
            p,
            policy_input(two_miners(100_000), vec![], fee_policy(1, CoinbaseOutputPolicy::PoolFeeFirst)),
        ),
        case(
            4,
            "fee-first-without-pool-fee-is-an-error",
            "Pool-fee-first with no pool-fee policy cannot pin a fee output.",
            p,
            policy_input(
                two_miners(100_000),
                vec![],
                PayoutPolicy {
                    coinbase_output_policy: CoinbaseOutputPolicy::PoolFeeFirst,
                    ..PayoutPolicy::day_one_default()
                },
            ),
        ),
        case(
            4,
            "fee-10000-bps-leaves-miners-below-floor",
            "At the 10000-bps cap the fee is the whole coinbase and the miner reward (0) is below the floor.",
            p,
            policy_input(two_miners(100_000), vec![], fee_policy(10_000, CoinbaseOutputPolicy::Canonical)),
        ),
        case(
            4,
            "fee-10001-bps-is-an-error",
            "One basis point over 100% is rejected.",
            p,
            policy_input(two_miners(100_000), vec![], fee_policy(10_001, CoinbaseOutputPolicy::Canonical)),
        ),
        case(
            4,
            "fee-1-bps-floors-to-zero",
            "1 bps of a 9999-sat coinbase is 0.9999 sats and floors to a zero pool fee; with a 1000-sat fixed floor the miners take the whole coinbase, and the fee account is recorded with 0 sats but gets no coinbase output.",
            p,
            policy_input(two_miners(9_999), vec![], fixed_floor(fee_policy(1, CoinbaseOutputPolicy::Canonical))),
        ),
        case(
            4,
            "fee-first-1-bps-floors-to-zero",
            "The same zero pool fee under pool-fee-first: the fee account is recorded with 0 sats, and no zero-amount fee output is pinned.",
            p,
            policy_input(two_miners(9_999), vec![], fixed_floor(fee_policy(1, CoinbaseOutputPolicy::PoolFeeFirst))),
        ),
        case(
            4,
            "fee-1-bps-floors-to-zero-under-day-one-floor",
            "The same zero pool fee with the day-one 14720-sat floor: the 9999-sat coinbase is below the floor, so the policy fails.",
            p,
            policy_input(two_miners(9_999), vec![], fee_policy(1, CoinbaseOutputPolicy::Canonical)),
        ),
    ]
}

/// A 1000-sat fixed floor, so a coinbase small enough for a 1-bps fee to
/// floor to zero still leaves the miners payable.
fn fixed_floor(mut policy: PayoutPolicy) -> PayoutPolicy {
    policy.min_output_sats = Some(1_000);
    policy
}

// ---------------------------------------------------------------------------
// Topics 5 and 6: apply_proportional_fanout_fee and largest-remainder ties

fn recipient(recipient_id: &str, order_key: &str, byte: u8, amount_sats: u64) -> SettlementRecipient {
    SettlementRecipient {
        recipient_id: recipient_id.to_string(),
        order_key: order_key.to_string(),
        p2mr_program_hex: program(byte),
        amount_sats,
    }
}

fn fee_input(recipients: Vec<SettlementRecipient>, fee_sats: u64, min_output_sats: u64) -> Value {
    json!({
        "recipients": to_value(&recipients),
        "fee_sats": fee_sats,
        "min_output_sats": min_output_sats,
    })
}

fn fanout_fee_cases() -> Vec<Value> {
    let f = "apply_proportional_fanout_fee";
    let at_floor = || vec![recipient("large", "01", 1, 100_000), recipient("small", "02", 2, 10_000)];
    vec![
        case(
            5,
            "prune-below-floor-and-recompute",
            "Both small recipients fall under 9950 after their fee share; they carry forward fee-free and the whole fee is recomputed over the large one.",
            f,
            fee_input(
                vec![
                    recipient("small-a", "01", 1, 10_000),
                    recipient("small-b", "02", 2, 10_000),
                    recipient("large", "03", 3, 100_000),
                ],
                1_000,
                9_950,
            ),
        ),
        case(
            5,
            "net-exactly-at-floor-is-payable",
            "The small recipient's fee share is exactly 100, leaving 9900: at the floor, so it is paid.",
            f,
            fee_input(at_floor(), 1_100, 9_900),
        ),
        case(
            5,
            "net-one-sat-under-floor-is-carried",
            "The same split with floor 9901 leaves the small recipient one sat under, so it carries and the large one pays the whole fee.",
            f,
            fee_input(at_floor(), 1_100, 9_901),
        ),
        case(
            5,
            "all-carry-when-no-recipient-survives",
            "The only recipient falls under the floor, so every recipient carries and no fee is applied.",
            f,
            fee_input(vec![recipient("tiny", "01", 1, 10_000)], 1_000, 9_500),
        ),
        case(
            5,
            "zero-floor-is-an-error",
            "min_output_sats must be positive.",
            f,
            fee_input(vec![recipient("tiny", "01", 1, 10_000)], 1_000, 0),
        ),
        case(
            6,
            "equal-remainders-broken-by-canonical-order",
            "Three equal recipients share a 10-sat fee: 3 each plus one remainder sat, which goes to the first account in canonical order regardless of input order.",
            f,
            fee_input(
                vec![
                    recipient("c", "03", 3, 100),
                    recipient("a", "01", 1, 100),
                    recipient("b", "02", 2, 100),
                ],
                10,
                1,
            ),
        ),
        case(
            6,
            "equal-remainders-order-key-before-recipient-id",
            "The canonical tie-break compares order_key before recipient_id, so the remainder sat goes to order key 01 even though its id sorts last.",
            f,
            fee_input(vec![recipient("a-miner", "02", 2, 100), recipient("z-miner", "01", 1, 100)], 1, 1),
        ),
        case(
            6,
            "three-way-split-of-500000000",
            "500000000 over three equal weights is 166666666 each plus two remainder sats for the first two accounts in canonical order.",
            "apply_payout_policy",
            policy_input(
                reward(
                    500_000_000,
                    &[
                        ("miner-c", "03", program(3), 1),
                        ("miner-a", "01", program(1), 1),
                        ("miner-b", "02", program(2), 1),
                    ],
                ),
                vec![],
                PayoutPolicy::day_one_default(),
            ),
        ),
    ]
}

// ---------------------------------------------------------------------------
// Topic 7: select_settlement_mode

fn settlement_config(
    max_coinbase_settlement_outputs: usize,
    max_direct_coinbase_outputs: usize,
    max_fanout_recipients_per_transaction: usize,
) -> SettlementModeConfig {
    SettlementModeConfig {
        max_coinbase_settlement_outputs,
        max_direct_coinbase_outputs,
        max_fanout_recipients_per_transaction,
        reserved_coinbase_outputs: 0,
    }
}

fn settlement_input(
    recipients: Vec<SettlementRecipient>,
    direct_floor_sats: u64,
    config: SettlementModeConfig,
    pinned_direct: Option<SettlementRecipient>,
) -> Value {
    json!({
        "recipients": to_value(&recipients),
        "direct_floor_sats": direct_floor_sats,
        "config": to_value(&config),
        "pinned_direct": to_value(&pinned_direct),
    })
}

fn dust(count: u8) -> Vec<SettlementRecipient> {
    (1..=count)
        .map(|index| recipient(&format!("dust-{index}"), &format!("{index:02}"), index, 1_000))
        .collect()
}

fn settlement_cases() -> Vec<Value> {
    let s = "select_settlement_mode";
    let floor = 20_000;
    let pinned = recipient("pool-fee", "ff", 0xff, 25);
    vec![
        case(
            7,
            "all-sub-floor-into-bounded-chunks",
            "Five sub-floor recipients with at most two per fanout become three chunks, filled in canonical order.",
            s,
            settlement_input(dust(5), floor, settlement_config(16, 12, 2), None),
        ),
        case(
            7,
            "hybrid-largest-direct-and-chunks",
            "Two direct slots go to the largest floor-crossing liabilities; the overflow above the floor and the dust are chunked.",
            s,
            settlement_input(
                vec![
                    recipient("dust-b", "05", 5, 4_000),
                    recipient("direct-b", "02", 2, 40_000),
                    recipient("overflow", "03", 3, 30_000),
                    recipient("direct-a", "01", 1, 50_000),
                    recipient("dust-a", "04", 4, 5_000),
                ],
                floor,
                settlement_config(500, 2, 2),
                None,
            ),
        ),
        case(
            7,
            "direct-floor-boundary",
            "One sat under the direct floor routes to fanout; exactly at and above it are direct.",
            s,
            settlement_input(
                vec![
                    recipient("below", "01", 1, floor - 1),
                    recipient("at", "02", 2, floor),
                    recipient("above", "03", 3, floor + 1),
                ],
                floor,
                settlement_config(500, 10, 10),
                None,
            ),
        ),
        case(
            7,
            "chunks-at-the-output-cap",
            "Six sub-floor recipients at two per chunk need exactly the three-output coinbase budget.",
            s,
            settlement_input(dust(6), floor, settlement_config(3, 0, 2), None),
        ),
        case(
            7,
            "chunks-one-over-the-output-cap-is-an-error",
            "A seventh recipient needs a fourth chunk, one more than the three-output budget.",
            s,
            settlement_input(dust(7), floor, settlement_config(3, 0, 2), None),
        ),
        case(
            7,
            "fanout-size-at-the-truc-cap",
            "1160 recipients per fanout (49973 weight bytes) is the largest chunk size under the 50000-byte TRUC limit.",
            s,
            settlement_input(dust(3), floor, settlement_config(16, 12, 1_160), None),
        ),
        case(
            7,
            "fanout-size-one-over-the-truc-cap-is-an-error",
            "1161 recipients per fanout exceeds the per-transaction maximum.",
            s,
            settlement_input(dust(3), floor, settlement_config(16, 12, 1_161), None),
        ),
        case(
            7,
            "pinned-direct-below-floor",
            "A 25-sat pinned pool fee is settled directly, takes one of the two direct slots, and never enters a fanout chunk.",
            s,
            settlement_input(
                vec![
                    recipient("miner-a", "01", 1, 30_000),
                    recipient("miner-b", "02", 2, 1_000),
                    pinned.clone(),
                ],
                floor,
                settlement_config(500, 2, 1_000),
                Some(pinned.clone()),
            ),
        ),
        case(
            7,
            "pinned-direct-without-a-direct-slot-is-an-error",
            "A pinned recipient needs max_direct_coinbase_outputs of at least one.",
            s,
            settlement_input(
                vec![recipient("miner-a", "01", 1, 30_000), pinned.clone()],
                floor,
                settlement_config(500, 0, 1_000),
                Some(pinned),
            ),
        ),
    ]
}

// ---------------------------------------------------------------------------
// Topic 8: bootstrap transition

fn bootstrap_cases(decisions: &Value) -> Vec<Value> {
    decisions["bootstrap"]
        .as_array()
        .expect("bootstrap decisions")
        .iter()
        .map(|exported| {
            let name = exported["name"].as_str().expect("name");
            let scenario = &exported["scenario"];
            let mut decision_2xx = exported["decision_2xx"].clone();
            let mut bundle_2xx = decision_2xx
                .as_object_mut()
                .expect("decision object")
                .remove("bundle")
                .expect("2.x.x bundle inputs");
            bundle_2xx["policy"] = to_value(&PayoutPolicy::day_one_default());

            // 3.x.x rule: the solver-only bundle is used only when the ledger
            // snapshot has no shares, and prior balances are always kept.
            let ledger = field::<Vec<AcceptedShare>>(scenario, "ledger_shares");
            let template = &scenario["template"];
            let solver = &scenario["solver"];
            let network: u128 = field(scenario, "network_difficulty");
            let anchor: i64 = field(scenario, "anchor_ms");
            let found_block = FoundBlock {
                block_height: field(template, "height"),
                coinbase_value_sats: field(template, "coinbasevalue"),
                network_difficulty: network,
                anchor_job_issued_at_ms: anchor,
            };
            let bootstrap = ledger.is_empty();
            let shares = if bootstrap {
                let payout_address: String = field(solver, "payout_address");
                vec![AcceptedShare {
                    share_seq: 1,
                    share_id: "bootstrap-share".to_string(),
                    miner_id: payout_address.clone(),
                    order_key: payout_address,
                    p2mr_program_hex: field(solver, "p2mr_program_hex"),
                    share_difficulty: network,
                    network_difficulty: network,
                    template_height: found_block.block_height - 1,
                    job_id: "bootstrap-job".to_string(),
                    job_issued_at_ms: anchor,
                    accepted_at_ms: anchor,
                    ntime: field(template, "curtime"),
                    credit_policy: None,
                }]
            } else {
                ledger
            };
            let bundle_3xx = json!({
                "shares": to_value(&shares),
                "found_block": to_value(&found_block),
                "prior_balances": scenario["prior_balances"].clone(),
                "policy": to_value(&PayoutPolicy::day_one_default()),
            });
            let decision_3xx = json!({
                "bootstrap": bootstrap,
                "bundle_built_by": if bootstrap {
                    "synthetic solver share (crates/qbit-prism-server/src/coordinator.rs:727)"
                } else {
                    "ledger snapshot window (crates/qbit-prism-server/src/coordinator.rs:747)"
                },
                "prior_balances_kept": true,
                "implemented_at": [
                    "crates/qbit-prism-server/src/coordinator.rs:556 solver-only bundle only when snapshot.shares is empty",
                    "crates/qbit-prism-server/src/coordinator.rs:759,773 snapshot.prior_balances passed in bootstrap",
                ],
            });
            let d2_entry = match name {
                "below-gate-with-other-miners-shares" => Some(D2A),
                "bootstrap-carry-only-account-at-or-above-floor" => Some(D2C),
                _ => None,
            };
            rule_case(
                8,
                name,
                exported["why"].as_str().expect("why"),
                json!({ "scenario": scenario, "decision_2xx": decision_2xx, "decision_3xx": decision_3xx }),
                bundle_2xx,
                bundle_3xx,
                d2_entry,
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Topic 9: below-target block credit

fn credit_cases(decisions: &Value) -> Vec<Value> {
    decisions["below_target_credit"]
        .as_array()
        .expect("below-target decisions")
        .iter()
        .map(|exported| {
            let name = exported["name"].as_str().expect("name");
            let scenario = &exported["scenario"];
            let decision_2xx = exported["decision_2xx"].clone();
            let ledger = field::<Vec<AcceptedShare>>(scenario, "ledger_shares");
            let next_block = field::<FoundBlock>(scenario, "next_block");
            let solver = &scenario["solver"];
            let credited = &scenario["credited_share"];
            let assigned: u128 = field(&decision_2xx, "assigned_share_difficulty");
            let network: u128 = field(&decision_2xx, "network_difficulty");
            let share_pass: bool = field(scenario, "share_pass");
            let block_pass: bool = field(scenario, "block_pass");
            let node_outcome: String = field(scenario, "node_outcome");
            let credited_share = |share_difficulty: u128| {
                let payout_address: String = field(solver, "payout_address");
                AcceptedShare {
                    share_seq: field(credited, "share_seq"),
                    share_id: field(credited, "share_id"),
                    miner_id: payout_address.clone(),
                    order_key: payout_address,
                    p2mr_program_hex: field(solver, "p2mr_program_hex"),
                    share_difficulty,
                    network_difficulty: network,
                    template_height: next_block.block_height - 2,
                    job_id: "job-below-target".to_string(),
                    job_issued_at_ms: field(credited, "job_issued_at_ms"),
                    accepted_at_ms: field(credited, "accepted_at_ms"),
                    ntime: 1_800_000_000,
                    credit_policy: None,
                }
            };
            let bundle = |credit: Option<u128>| {
                let mut shares = ledger.clone();
                shares.extend(credit.map(credited_share));
                json!({
                    "shares": to_value(&shares),
                    "found_block": to_value(&next_block),
                    "prior_balances": [],
                    "policy": to_value(&PayoutPolicy::day_one_default()),
                })
            };
            let credit_2xx = if field::<bool>(&decision_2xx, "credited") {
                Some(field::<u128>(&decision_2xx, "credited_difficulty"))
            } else {
                None
            };

            // 3.x.x rule: a share-passing proof is credited at its share
            // target; a block-only proof earns network difficulty, and only
            // once its block is confirmed on the active chain.
            let (credit_3xx, credited_at, implemented_at): (Option<u128>, &str, Vec<&str>) =
                if share_pass {
                    (
                        Some(assigned),
                        "share acceptance (share passed its target)",
                        vec!["crates/qbit-prism-server/src/coordinator.rs:1649-1651 share target difficulty when the share passes"],
                    )
                } else if block_pass {
                    let rule = vec![
                        "crates/qbit-prism-server/src/coordinator.rs:1648-1654 network difficulty for a block-only proof",
                        "crates/qbit-prism-server/src/coordinator.rs:1698 deferred_share held with the block candidate",
                        "crates/qbit-prism-server/src/coordinator.rs:1726-1747 submit waits for the credit row or fails",
                    ];
                    match node_outcome.as_str() {
                        "accepted" => (
                            Some(network),
                            "confirmation transaction after node acceptance; the submit acknowledgement waits for it",
                            [rule, vec!["crates/qbit-prism-server/src/ledger/blocks.rs:200 credit_deferred_share on confirmation"]].concat(),
                        ),
                        "confirmed-after-reconciliation" => (
                            Some(network),
                            "reconciliation's confirmation transaction",
                            [rule, vec!["crates/qbit-prism-server/src/ledger/blocks.rs:295 credit_deferred_share on reconciled confirmation"]].concat(),
                        ),
                        "reorged-after-acceptance" => (
                            Some(network),
                            "confirmation transaction; the share row survives the later disconnect",
                            [rule, vec!["crates/qbit-prism-server/src/ledger/blocks.rs:200 credit_deferred_share on confirmation; no server statement updates or deletes qbit_share_ledger"]].concat(),
                        ),
                        "rejected" => (
                            None,
                            "never: the submission fails with block-only proof was not accepted on the active chain",
                            rule,
                        ),
                        other => panic!("unknown node outcome {other}"),
                    }
                } else {
                    (None, "never: low difficulty share", vec![])
                };
            let decision_3xx = json!({
                "credited": credit_3xx.is_some(),
                "credited_difficulty": credit_3xx.map(|value| to_value(&value)),
                "credited_at": credited_at,
                "implemented_at": implemented_at,
            });
            let d2_entry = (credit_2xx != credit_3xx).then_some(D2B);
            rule_case(
                9,
                name,
                exported["why"].as_str().expect("why"),
                json!({ "scenario": scenario, "decision_2xx": decision_2xx, "decision_3xx": decision_3xx }),
                bundle(credit_2xx),
                bundle(credit_3xx),
                d2_entry,
            )
        })
        .collect()
}

fn write(dir: &Path, file: &str, topics: &[(u8, &str)], cases: Vec<Value>) {
    let document = json!({
        "schema": SCHEMA,
        "source_commit": SOURCE_COMMIT,
        "produced_by": PRODUCED_BY,
        "topics": topics
            .iter()
            .map(|(id, name)| json!({ "id": id, "name": name }))
            .collect::<Vec<_>>(),
        "cases": cases,
    });
    let mut text = serde_json::to_string_pretty(&document).expect("vector document");
    text.push('\n');
    fs::write(dir.join(file), text).expect("write vector file");
}

fn main() {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: export_money_path_vectors <out-dir> < rule_decisions.json");
    let dir = Path::new(&dir);
    let mut raw = String::new();
    std::io::stdin()
        .read_to_string(&mut raw)
        .expect("read rule decisions");
    let decisions: Value = serde_json::from_str(&raw).expect("rule decisions JSON");
    fs::create_dir_all(dir).expect("create output directory");
    write(
        dir,
        "window.json",
        &[
            (1, "window clipping and the oldest-share boundary"),
            (2, "fractional oldest share"),
        ],
        window_cases(),
    );
    write(dir, "carry_only.json", &[(3, "carry-only recipients")], carry_only_cases());
    write(
        dir,
        "pool_fee.json",
        &[(4, "pool fee: bps rounding down, remainder split, fee-first pinning")],
        pool_fee_cases(),
    );
    write(
        dir,
        "fanout_fee_and_ties.json",
        &[
            (5, "dust floor exclusion and recompute"),
            (6, "largest-remainder ties"),
        ],
        fanout_fee_cases(),
    );
    write(dir, "settlement_chunks.json", &[(7, "CTV chunk splits")], settlement_cases());
    write(
        dir,
        "bootstrap_transition.json",
        &[(8, "bootstrap transition")],
        bootstrap_cases(&decisions),
    );
    write(
        dir,
        "below_target_credit.json",
        &[(9, "below-target block credit")],
        credit_cases(&decisions),
    );
}
