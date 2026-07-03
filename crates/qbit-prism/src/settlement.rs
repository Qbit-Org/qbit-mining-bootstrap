//! Explicit, reproducible settlement-mode partitioning for PRISM payouts
//! .
//!
//! Given the recipients owed for a block, this decides whether to settle via
//! direct coinbase outputs, a hybrid of direct outputs plus one or more CTV
//! fanout transactions, or fanout alone. The policy honors two hard constraints
//! from the settlement policy for every valid recipient set:
//!
//! - Do not refuse to mine solely because direct payouts are impractical:
//!   direct overflow is routed into bounded fanout chunks.
//! - Do not use a custodial holding address: every recipient is assigned to
//!   either a direct coinbase output or a non-custodial CTV fanout chunk.
//!
//! A recipient is direct-eligible if its owed amount is at or above the direct
//! payout floor ([`crate::PayoutPolicy::min_output_sats`]). Direct slots are
//! assigned to the largest eligible liabilities first, with canonical
//! `(order_key, recipient_id, p2mr_program_hex)` tie-breaks. Returned direct
//! recipients and fanout chunks are canonicalized by the same account key so the
//! partition is independently reproducible.

use crate::{PrismError, SettlementMode};
use qbit_pool_builder::P2MR_PROGRAM_LEN;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeSet;

/// Hard cap on settlement-related coinbase outputs in this policy layer. This
/// includes direct recipient outputs plus one covenant output for each fanout
/// chunk. A 500-output P2MR coinbase is exercised and shown to stay under the
/// block weight limit by the adversarial suite; operators may lower this, but
/// may not raise it until exact coinbase-size partitioning is wired here.
pub const MAX_COINBASE_SETTLEMENT_OUTPUTS: usize = 500;
/// Conservative non-DATUM launch default for one hardware-safe Stratum
/// template. This is an operator policy default, not a qbit consensus limit.
/// OCEAN DATUM uses multiple coinbase byte profiles for different miner
/// firmware; until PRISM supports that, default to a low old-Antminer-friendly
/// settlement output budget.
pub const DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS: usize = 16;
pub const MAX_DIRECT_COINBASE_OUTPUTS: usize = MAX_COINBASE_SETTLEMENT_OUTPUTS;
/// Direct recipient outputs consume coinbase bytes directly. Overflow should
/// route to CTV fanout chunks so one conservative non-DATUM template can serve
/// stock mining firmware.
pub const DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS: usize = 12;
/// OCEAN's public Bitcoin on-chain payout threshold, used only as a policy
/// reference point for qbit launch defaults.
pub const OCEAN_ONCHAIN_PAYOUT_THRESHOLD_SATS: u64 = 1_048_576;
/// Conservative launch default for direct qbit coinbase payouts. Balances below
/// this floor should route through CTV fanout when economically spendable rather
/// than consuming direct coinbase outputs.
pub const DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS: u64 = OCEAN_ONCHAIN_PAYOUT_THRESHOLD_SATS * 10;

/// Hard cap for a single CTV fanout transaction in this policy layer.
///
/// qbit's v3/TRUC standard transaction cap. qbit uses WSF=1, so bytes and
/// transaction weight are equivalent for this policy estimate.
pub const QBIT_TRUC_MAX_TX_WEIGHT_BYTES: usize = 50_000;
pub const CTV_FANOUT_P2MR_OUTPUT_BYTES: usize = 43;
pub const CTV_FANOUT_FIXED_WEIGHT_BYTES: usize = 90;
/// Largest P2MR payout count that keeps a final witnessed built-in-fee v3 fanout
/// under qbit's TRUC standardness cap using the estimate below.
pub const MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION: usize = 1_160;
pub const DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION: usize = 1_000;
/// Launch default for fanout transaction fees: charge 1.2x the market fee
/// estimate to make dust/Sybil fanouts economically self-funded.
pub const DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS: u64 = 12_000;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FanoutFeeRatePolicy {
    /// Market fee rate sampled from qbit RPC at template-construction time,
    /// expressed as sats per 1,000 qbit weight bytes.
    pub market_fee_rate_sats_per_1000_weight: u64,
    /// Multiplier in basis points. 10,000 is 1.0x; 12,000 is 1.2x.
    pub premium_bps: u64,
}

impl FanoutFeeRatePolicy {
    pub fn new(market_fee_rate_sats_per_1000_weight: u64, premium_bps: u64) -> Self {
        Self {
            market_fee_rate_sats_per_1000_weight,
            premium_bps,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SettlementModeConfig {
    /// Maximum settlement-related outputs in the coinbase. Direct recipient
    /// outputs and fanout covenant outputs both consume this budget.
    pub max_coinbase_settlement_outputs: usize,
    /// Maximum number of direct P2MR coinbase payout outputs considered
    /// practical for one block.
    pub max_direct_coinbase_outputs: usize,
    /// Maximum number of payout recipients assigned to one CTV fanout
    /// transaction. This may not exceed
    /// [`MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION`].
    pub max_fanout_recipients_per_transaction: usize,
    /// Coinbase output budget already consumed by settlement-adjacent outputs
    /// outside this partition, such as a pool-fee output.
    pub reserved_coinbase_outputs: usize,
}

impl Default for SettlementModeConfig {
    fn default() -> Self {
        Self {
            max_coinbase_settlement_outputs: DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS,
            max_direct_coinbase_outputs: DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS,
            max_fanout_recipients_per_transaction:
                DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION,
            reserved_coinbase_outputs: 0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SettlementRecipient {
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    pub amount_sats: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SettlementFanoutChunk {
    pub chunk_index: usize,
    pub recipient_count: usize,
    pub amount_sats: u64,
    pub recipients: Vec<SettlementRecipient>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FanoutFeeRecipient {
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    pub gross_amount_sats: u64,
    pub fee_sats: u64,
    pub net_amount_sats: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FanoutFeeDecision {
    pub total_gross_sats: u64,
    pub requested_fee_sats: u64,
    pub applied_fee_sats: u64,
    pub payable_recipients: Vec<FanoutFeeRecipient>,
    pub carry_forward_recipients: Vec<FanoutFeeRecipient>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SettlementModeDecision {
    pub mode: SettlementMode,
    /// Recipients paid by direct coinbase outputs, canonicalized by account key.
    pub direct_recipients: Vec<SettlementRecipient>,
    /// Recipients paid through CTV fanouts, split into bounded chunks.
    pub fanout_chunks: Vec<SettlementFanoutChunk>,
    pub direct_recipient_count: usize,
    pub fanout_recipient_count: usize,
    pub fanout_chunk_count: usize,
    pub direct_amount_sats: u64,
    pub fanout_amount_sats: u64,
    pub total_amount_sats: u64,
    pub coinbase_settlement_output_count: usize,
    pub reserved_coinbase_output_count: usize,
    /// Human-readable rationale for status and audit surfaces.
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SettlementAccountKey {
    order_key: String,
    recipient_id: String,
    p2mr_program_hex: String,
}

/// Choose a deterministic settlement partition from concrete recipients.
///
/// `direct_floor_sats` is the minimum amount for a direct coinbase payout.
/// Recipients below it are always routed to fanout. Recipients at or above it
/// are direct candidates, but only the largest liabilities up to
/// `max_direct_coinbase_outputs` are selected for direct payout; overflow goes
/// to fanout chunks.
pub fn select_settlement_mode(
    recipients: &[SettlementRecipient],
    direct_floor_sats: u64,
    config: &SettlementModeConfig,
) -> Result<SettlementModeDecision, PrismError> {
    validate_config(config)?;
    if recipients.is_empty() {
        return Err(settlement_error("no recipients to settle"));
    }

    let mut canonical = recipients.to_vec();
    normalize_recipient_p2mr_programs(&mut canonical)?;
    canonical.sort_by(canonical_recipient_cmp);
    validate_recipients(&canonical)?;

    let total_amount_sats = sum_amounts(&canonical)?;
    let mut direct_candidates = canonical
        .iter()
        .filter(|recipient| recipient.amount_sats >= direct_floor_sats)
        .cloned()
        .collect::<Vec<_>>();
    direct_candidates.sort_by(direct_priority_cmp);

    let direct_count = select_direct_count(recipients.len(), direct_candidates.len(), config)?;
    let selected_direct_keys = direct_candidates
        .iter()
        .take(direct_count)
        .map(account_key)
        .collect::<BTreeSet<_>>();
    let mut direct_recipients = Vec::new();
    let mut fanout_recipients = Vec::new();
    for recipient in canonical {
        if selected_direct_keys.contains(&account_key(&recipient)) {
            direct_recipients.push(recipient);
        } else {
            fanout_recipients.push(recipient);
        }
    }

    let direct_amount_sats = sum_amounts(&direct_recipients)?;
    let fanout_amount_sats = sum_amounts(&fanout_recipients)?;
    if direct_amount_sats
        .checked_add(fanout_amount_sats)
        .ok_or_else(|| settlement_error("settlement amount overflowed"))?
        != total_amount_sats
    {
        return Err(settlement_error("settlement partition lost value"));
    }

    let fanout_chunks = fanout_recipients
        .chunks(config.max_fanout_recipients_per_transaction)
        .enumerate()
        .map(|(chunk_index, chunk)| {
            let recipients = chunk.to_vec();
            Ok(SettlementFanoutChunk {
                chunk_index,
                recipient_count: recipients.len(),
                amount_sats: sum_amounts(&recipients)?,
                recipients,
            })
        })
        .collect::<Result<Vec<_>, PrismError>>()?;

    let mode = match (direct_recipients.is_empty(), fanout_chunks.is_empty()) {
        (false, true) => SettlementMode::DirectCoinbase,
        (true, false) => SettlementMode::CtvFanout,
        (false, false) => SettlementMode::HybridCoinbaseCtvFanout,
        (true, true) => return Err(settlement_error("settlement partition is empty")),
    };
    let fanout_recipient_count = fanout_chunks
        .iter()
        .map(|chunk| chunk.recipient_count)
        .sum::<usize>();
    let coinbase_settlement_output_count = config
        .reserved_coinbase_outputs
        .checked_add(direct_recipients.len())
        .and_then(|count| count.checked_add(fanout_chunks.len()))
        .ok_or_else(|| settlement_error("coinbase settlement output count overflowed"))?;
    let decision = SettlementModeDecision {
        mode,
        direct_recipient_count: direct_recipients.len(),
        fanout_recipient_count,
        fanout_chunk_count: fanout_chunks.len(),
        direct_amount_sats,
        fanout_amount_sats,
        total_amount_sats,
        coinbase_settlement_output_count,
        reserved_coinbase_output_count: config.reserved_coinbase_outputs,
        reason: decision_reason(
            direct_recipients.len(),
            fanout_recipient_count,
            fanout_chunks.len(),
            direct_floor_sats,
            coinbase_settlement_output_count,
            config,
        ),
        direct_recipients,
        fanout_chunks,
    };
    Ok(decision)
}

/// Deduct a fixed fanout transaction fee from candidate recipients
/// proportionally using integer-sat arithmetic.
///
/// Remainder sats go to the largest fractional remainder, with canonical account
/// key tie-breaks. Recipients whose post-fee amount would fall below
/// `min_output_sats` are removed from this fanout and returned as carry-forward
/// candidates without paying this fanout's fee; the fee is then recomputed over
/// the remaining recipients. If no recipient can pay the fee and remain above
/// the floor, the decision returns all recipients as carry-forward with
/// `applied_fee_sats == 0`.
pub fn apply_proportional_fanout_fee(
    recipients: &[SettlementRecipient],
    fee_sats: u64,
    min_output_sats: u64,
) -> Result<FanoutFeeDecision, PrismError> {
    if min_output_sats == 0 {
        return Err(settlement_error("min_output_sats must be positive"));
    }
    if recipients.is_empty() {
        return Err(settlement_error("no fanout recipients to charge"));
    }

    let mut candidates = recipients.to_vec();
    normalize_recipient_p2mr_programs(&mut candidates)?;
    candidates.sort_by(canonical_recipient_cmp);
    validate_recipients(&candidates)?;
    let total_gross_sats = sum_amounts(&candidates)?;
    let mut carry_forward_recipients = Vec::new();

    loop {
        if candidates.is_empty() {
            carry_forward_recipients.sort_by(fanout_fee_recipient_cmp);
            return Ok(FanoutFeeDecision {
                total_gross_sats,
                requested_fee_sats: fee_sats,
                applied_fee_sats: 0,
                payable_recipients: Vec::new(),
                carry_forward_recipients,
            });
        }

        let candidate_sum = sum_amounts(&candidates)?;
        if fee_sats >= candidate_sum {
            carry_forward_recipients.extend(
                candidates
                    .into_iter()
                    .map(|recipient| carried_fanout_fee_recipient(&recipient)),
            );
            carry_forward_recipients.sort_by(fanout_fee_recipient_cmp);
            return Ok(FanoutFeeDecision {
                total_gross_sats,
                requested_fee_sats: fee_sats,
                applied_fee_sats: 0,
                payable_recipients: Vec::new(),
                carry_forward_recipients,
            });
        }

        let allocated = allocate_proportional_fee(&candidates, fee_sats, candidate_sum)?;
        let mut payable = Vec::new();
        let mut below_floor = Vec::new();
        for recipient in allocated {
            if recipient.net_amount_sats >= min_output_sats {
                payable.push(recipient);
            } else {
                below_floor.push(recipient);
            }
        }
        if below_floor.is_empty() {
            payable.sort_by(fanout_fee_recipient_cmp);
            carry_forward_recipients.sort_by(fanout_fee_recipient_cmp);
            return Ok(FanoutFeeDecision {
                total_gross_sats,
                requested_fee_sats: fee_sats,
                applied_fee_sats: fee_sats,
                payable_recipients: payable,
                carry_forward_recipients,
            });
        }

        let below_floor_keys = below_floor
            .iter()
            .map(|recipient| SettlementAccountKey {
                order_key: recipient.order_key.clone(),
                recipient_id: recipient.recipient_id.clone(),
                p2mr_program_hex: recipient.p2mr_program_hex.clone(),
            })
            .collect::<BTreeSet<_>>();
        carry_forward_recipients.extend(below_floor.into_iter().map(|recipient| {
            FanoutFeeRecipient {
                recipient_id: recipient.recipient_id,
                order_key: recipient.order_key,
                p2mr_program_hex: recipient.p2mr_program_hex,
                gross_amount_sats: recipient.gross_amount_sats,
                fee_sats: 0,
                net_amount_sats: recipient.gross_amount_sats,
            }
        }));
        candidates.retain(|recipient| !below_floor_keys.contains(&account_key(recipient)));
    }
}

/// Estimate the fixed fanout transaction fee from a runtime market fee rate and
/// operator premium. The result is rounded up so settlement never
/// under-reserves by fractional sats.
pub fn estimate_ctv_fanout_fee_sats(
    recipient_count: usize,
    policy: &FanoutFeeRatePolicy,
) -> Result<u64, PrismError> {
    if recipient_count == 0 {
        return Err(settlement_error("fanout recipient count must be positive"));
    }
    if policy.market_fee_rate_sats_per_1000_weight == 0 {
        return Err(settlement_error(
            "market_fee_rate_sats_per_1000_weight must be positive",
        ));
    }
    if policy.premium_bps == 0 {
        return Err(settlement_error("premium_bps must be positive"));
    }

    let fanout_weight = estimated_ctv_fanout_weight_bytes(recipient_count)?;
    let numerator = (fanout_weight as u128)
        .checked_mul(u128::from(policy.market_fee_rate_sats_per_1000_weight))
        .and_then(|value| value.checked_mul(u128::from(policy.premium_bps)))
        .ok_or_else(|| settlement_error("fanout fee estimate overflowed"))?;
    let denominator = 1_000_u128 * 10_000_u128;
    let fee_sats = numerator
        .checked_add(denominator - 1)
        .ok_or_else(|| settlement_error("fanout fee estimate overflowed"))?
        / denominator;
    u64::try_from(fee_sats).map_err(|_| settlement_error("fanout fee estimate exceeds uint64"))
}

/// Convenience wrapper for the launch policy: estimate the premium fanout fee,
/// then allocate it proportionally across the recipients that remain payable.
pub fn apply_estimated_proportional_fanout_fee(
    recipients: &[SettlementRecipient],
    min_output_sats: u64,
    policy: &FanoutFeeRatePolicy,
) -> Result<FanoutFeeDecision, PrismError> {
    let fee_sats = estimate_ctv_fanout_fee_sats(recipients.len(), policy)?;
    apply_proportional_fanout_fee(recipients, fee_sats, min_output_sats)
}

fn validate_config(config: &SettlementModeConfig) -> Result<(), PrismError> {
    if config.max_coinbase_settlement_outputs == 0 {
        return Err(settlement_error(
            "max_coinbase_settlement_outputs must be positive",
        ));
    }
    if config.max_coinbase_settlement_outputs > MAX_COINBASE_SETTLEMENT_OUTPUTS {
        return Err(settlement_error(format!(
            "max_coinbase_settlement_outputs must be <= {MAX_COINBASE_SETTLEMENT_OUTPUTS}"
        )));
    }
    if config.max_direct_coinbase_outputs > MAX_DIRECT_COINBASE_OUTPUTS {
        return Err(settlement_error(format!(
            "max_direct_coinbase_outputs must be <= {MAX_DIRECT_COINBASE_OUTPUTS}"
        )));
    }
    if config.max_fanout_recipients_per_transaction == 0 {
        return Err(settlement_error(
            "max_fanout_recipients_per_transaction must be positive",
        ));
    }
    if config.max_fanout_recipients_per_transaction > MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION {
        return Err(settlement_error(format!(
            "max_fanout_recipients_per_transaction must be <= {MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION}"
        )));
    }
    if estimated_ctv_fanout_weight_bytes(config.max_fanout_recipients_per_transaction)?
        > QBIT_TRUC_MAX_TX_WEIGHT_BYTES
    {
        return Err(settlement_error(format!(
            "max_fanout_recipients_per_transaction exceeds qbit TRUC weight limit {QBIT_TRUC_MAX_TX_WEIGHT_BYTES}"
        )));
    }
    if config.reserved_coinbase_outputs > config.max_coinbase_settlement_outputs {
        return Err(settlement_error(
            "reserved_coinbase_outputs exceeds max_coinbase_settlement_outputs",
        ));
    }
    Ok(())
}

fn allocate_proportional_fee(
    recipients: &[SettlementRecipient],
    fee_sats: u64,
    total_gross_sats: u64,
) -> Result<Vec<FanoutFeeRecipient>, PrismError> {
    let mut allocated = recipients
        .iter()
        .map(|recipient| {
            let fee_product = u128::from(fee_sats)
                .checked_mul(u128::from(recipient.amount_sats))
                .ok_or_else(|| settlement_error("fanout fee allocation overflowed"))?;
            let fee_floor = fee_product / u128::from(total_gross_sats);
            let remainder = fee_product % u128::from(total_gross_sats);
            let fee_sats = u64::try_from(fee_floor)
                .map_err(|_| settlement_error("fanout fee share exceeds uint64"))?;
            Ok((
                FanoutFeeRecipient {
                    recipient_id: recipient.recipient_id.clone(),
                    order_key: recipient.order_key.clone(),
                    p2mr_program_hex: recipient.p2mr_program_hex.clone(),
                    gross_amount_sats: recipient.amount_sats,
                    fee_sats,
                    net_amount_sats: recipient
                        .amount_sats
                        .checked_sub(fee_sats)
                        .ok_or_else(|| settlement_error("fanout fee exceeds recipient amount"))?,
                },
                remainder,
            ))
        })
        .collect::<Result<Vec<_>, PrismError>>()?;

    let floor_fee_sum = allocated
        .iter()
        .map(|(recipient, _)| recipient.fee_sats)
        .sum::<u64>();
    let remainder_fee_sats = fee_sats
        .checked_sub(floor_fee_sum)
        .ok_or_else(|| settlement_error("fanout fee floor allocation exceeded total fee"))?;
    allocated.sort_by(
        |(left_recipient, left_remainder), (right_recipient, right_remainder)| {
            right_remainder
                .cmp(left_remainder)
                .then_with(|| fanout_fee_recipient_cmp(left_recipient, right_recipient))
        },
    );
    for (recipient, _) in allocated.iter_mut().take(remainder_fee_sats as usize) {
        recipient.fee_sats = recipient
            .fee_sats
            .checked_add(1)
            .ok_or_else(|| settlement_error("fanout fee share overflowed"))?;
        recipient.net_amount_sats = recipient
            .gross_amount_sats
            .checked_sub(recipient.fee_sats)
            .ok_or_else(|| settlement_error("fanout fee exceeds recipient amount"))?;
    }

    Ok(allocated
        .into_iter()
        .map(|(recipient, _)| recipient)
        .collect())
}

fn carried_fanout_fee_recipient(recipient: &SettlementRecipient) -> FanoutFeeRecipient {
    FanoutFeeRecipient {
        recipient_id: recipient.recipient_id.clone(),
        order_key: recipient.order_key.clone(),
        p2mr_program_hex: recipient.p2mr_program_hex.clone(),
        gross_amount_sats: recipient.amount_sats,
        fee_sats: 0,
        net_amount_sats: recipient.amount_sats,
    }
}

fn fanout_fee_recipient_cmp(left: &FanoutFeeRecipient, right: &FanoutFeeRecipient) -> Ordering {
    left.order_key
        .cmp(&right.order_key)
        .then_with(|| left.recipient_id.cmp(&right.recipient_id))
        .then_with(|| left.p2mr_program_hex.cmp(&right.p2mr_program_hex))
}

fn select_direct_count(
    total_recipient_count: usize,
    direct_candidate_count: usize,
    config: &SettlementModeConfig,
) -> Result<usize, PrismError> {
    let max_direct = direct_candidate_count.min(config.max_direct_coinbase_outputs);
    for direct_count in (0..=max_direct).rev() {
        let fanout_recipient_count = total_recipient_count
            .checked_sub(direct_count)
            .ok_or_else(|| settlement_error("direct recipient count exceeded total recipients"))?;
        let fanout_chunk_count = fanout_chunk_count(
            fanout_recipient_count,
            config.max_fanout_recipients_per_transaction,
        )?;
        let coinbase_outputs = config
            .reserved_coinbase_outputs
            .checked_add(direct_count)
            .and_then(|count| count.checked_add(fanout_chunk_count))
            .ok_or_else(|| settlement_error("coinbase settlement output count overflowed"))?;
        if coinbase_outputs <= config.max_coinbase_settlement_outputs {
            return Ok(direct_count);
        }
    }
    Err(settlement_error(format!(
        "recipient partition requires more than {} coinbase settlement outputs",
        config.max_coinbase_settlement_outputs
    )))
}

fn fanout_chunk_count(
    recipient_count: usize,
    max_recipients_per_chunk: usize,
) -> Result<usize, PrismError> {
    if recipient_count == 0 {
        return Ok(0);
    }
    recipient_count
        .checked_add(max_recipients_per_chunk - 1)
        .map(|adjusted| adjusted / max_recipients_per_chunk)
        .ok_or_else(|| settlement_error("fanout chunk count overflowed"))
}

pub fn estimated_ctv_fanout_weight_bytes(recipient_count: usize) -> Result<usize, PrismError> {
    CTV_FANOUT_FIXED_WEIGHT_BYTES
        .checked_add(compact_size_len(recipient_count))
        .and_then(|base| {
            recipient_count
                .checked_mul(CTV_FANOUT_P2MR_OUTPUT_BYTES)
                .and_then(|outputs| base.checked_add(outputs))
        })
        .ok_or_else(|| settlement_error("fanout weight estimate overflowed"))
}

fn compact_size_len(value: usize) -> usize {
    if value < 253 {
        1
    } else if value <= u16::MAX as usize {
        3
    } else if value <= u32::MAX as usize {
        5
    } else {
        9
    }
}

fn validate_recipients(recipients: &[SettlementRecipient]) -> Result<(), PrismError> {
    let mut seen = BTreeSet::new();
    for recipient in recipients {
        if recipient.amount_sats == 0 {
            return Err(settlement_error(format!(
                "recipient {} amount must be positive",
                recipient.recipient_id
            )));
        }
        let key = account_key(recipient);
        if !seen.insert(key) {
            return Err(settlement_error(format!(
                "duplicate settlement recipient {}",
                recipient.recipient_id
            )));
        }
    }
    Ok(())
}

fn normalize_recipient_p2mr_programs(
    recipients: &mut [SettlementRecipient],
) -> Result<(), PrismError> {
    for recipient in recipients {
        let bytes = hex::decode(&recipient.p2mr_program_hex).map_err(|err| {
            settlement_error(format!(
                "recipient {} p2mr_program_hex must be hex: {err}",
                recipient.recipient_id
            ))
        })?;
        if bytes.len() != P2MR_PROGRAM_LEN {
            return Err(settlement_error(format!(
                "recipient {} p2mr_program_hex must be {P2MR_PROGRAM_LEN} bytes, got {}",
                recipient.recipient_id,
                bytes.len()
            )));
        }
        recipient.p2mr_program_hex = hex::encode(bytes);
    }
    Ok(())
}

fn sum_amounts(recipients: &[SettlementRecipient]) -> Result<u64, PrismError> {
    recipients.iter().try_fold(0_u64, |sum, recipient| {
        sum.checked_add(recipient.amount_sats)
            .ok_or_else(|| settlement_error("settlement amount overflowed"))
    })
}

fn account_key(recipient: &SettlementRecipient) -> SettlementAccountKey {
    SettlementAccountKey {
        order_key: recipient.order_key.clone(),
        recipient_id: recipient.recipient_id.clone(),
        p2mr_program_hex: recipient.p2mr_program_hex.clone(),
    }
}

fn canonical_recipient_cmp(left: &SettlementRecipient, right: &SettlementRecipient) -> Ordering {
    left.order_key
        .cmp(&right.order_key)
        .then_with(|| left.recipient_id.cmp(&right.recipient_id))
        .then_with(|| left.p2mr_program_hex.cmp(&right.p2mr_program_hex))
}

fn direct_priority_cmp(left: &SettlementRecipient, right: &SettlementRecipient) -> Ordering {
    right
        .amount_sats
        .cmp(&left.amount_sats)
        .then_with(|| canonical_recipient_cmp(left, right))
}

fn decision_reason(
    direct_count: usize,
    fanout_count: usize,
    fanout_chunks: usize,
    direct_floor_sats: u64,
    coinbase_settlement_outputs: usize,
    config: &SettlementModeConfig,
) -> String {
    if fanout_count == 0 {
        return format!(
            "{direct_count} recipients meet the {direct_floor_sats}-sat direct floor and fit within the {}-output coinbase settlement cap",
            config.max_coinbase_settlement_outputs
        );
    }
    if direct_count == 0 {
        return format!(
            "{fanout_count} recipients routed to {fanout_chunks} CTV fanout chunk(s) capped at {} recipients each; {coinbase_settlement_outputs} coinbase settlement outputs used",
            config.max_fanout_recipients_per_transaction
        );
    }
    format!(
        "{direct_count} recipients paid directly by largest-liability priority; {fanout_count} recipients routed to {fanout_chunks} CTV fanout chunk(s); {coinbase_settlement_outputs} of {} coinbase settlement outputs used",
        config.max_coinbase_settlement_outputs
    )
}

fn settlement_error(reason: impl Into<String>) -> PrismError {
    PrismError::SettlementModeSelection {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLOOR: u64 = 10_000;

    fn config(max_direct: usize, max_fanout: usize) -> SettlementModeConfig {
        SettlementModeConfig {
            max_coinbase_settlement_outputs: MAX_COINBASE_SETTLEMENT_OUTPUTS,
            max_direct_coinbase_outputs: max_direct,
            max_fanout_recipients_per_transaction: max_fanout,
            reserved_coinbase_outputs: 0,
        }
    }

    fn capped_config(
        max_coinbase: usize,
        max_direct: usize,
        max_fanout: usize,
        reserved: usize,
    ) -> SettlementModeConfig {
        SettlementModeConfig {
            max_coinbase_settlement_outputs: max_coinbase,
            max_direct_coinbase_outputs: max_direct,
            max_fanout_recipients_per_transaction: max_fanout,
            reserved_coinbase_outputs: reserved,
        }
    }

    fn recipient(id: &str, order: &str, amount_sats: u64) -> SettlementRecipient {
        recipient_with_program(id, order, format!("{order:0>64}"), amount_sats)
    }

    fn recipient_with_program(
        id: &str,
        order: &str,
        p2mr_program_hex: String,
        amount_sats: u64,
    ) -> SettlementRecipient {
        SettlementRecipient {
            recipient_id: id.to_string(),
            order_key: order.to_string(),
            p2mr_program_hex,
            amount_sats,
        }
    }

    fn recipient_names(recipients: &[SettlementRecipient]) -> Vec<String> {
        recipients
            .iter()
            .map(|recipient| recipient.recipient_id.clone())
            .collect()
    }

    fn fanout_chunk_names(decision: &SettlementModeDecision) -> Vec<Vec<String>> {
        decision
            .fanout_chunks
            .iter()
            .map(|chunk| recipient_names(&chunk.recipients))
            .collect()
    }

    #[test]
    fn all_floor_crossing_and_fitting_is_direct_with_canonical_order() {
        let recipients = vec![
            recipient("miner-c", "03", 30_000),
            recipient("miner-a", "01", 10_000),
            recipient("miner-b", "02", 20_000),
        ];

        let decision = select_settlement_mode(&recipients, FLOOR, &config(500, 1_000)).unwrap();

        assert_eq!(decision.mode, SettlementMode::DirectCoinbase);
        assert_eq!(decision.direct_recipient_count, 3);
        assert_eq!(decision.fanout_recipient_count, 0);
        assert_eq!(
            recipient_names(&decision.direct_recipients),
            ["miner-a", "miner-b", "miner-c"]
        );
        assert!(decision.fanout_chunks.is_empty());
        assert_eq!(decision.total_amount_sats, 60_000);
    }

    #[test]
    fn hybrid_selects_largest_floor_crossing_direct_and_chunks_rest() {
        let recipients = vec![
            recipient("dust-b", "05", 4_000),
            recipient("direct-b", "02", 40_000),
            recipient("overflow", "03", 30_000),
            recipient("direct-a", "01", 50_000),
            recipient("dust-a", "04", 5_000),
        ];

        let decision = select_settlement_mode(&recipients, FLOOR, &config(2, 2)).unwrap();

        assert_eq!(decision.mode, SettlementMode::HybridCoinbaseCtvFanout);
        assert_eq!(
            recipient_names(&decision.direct_recipients),
            ["direct-a", "direct-b"]
        );
        assert_eq!(
            fanout_chunk_names(&decision),
            vec![
                vec!["overflow".to_string(), "dust-a".to_string()],
                vec!["dust-b".to_string()]
            ]
        );
        assert_eq!(decision.direct_recipient_count, 2);
        assert_eq!(decision.fanout_recipient_count, 3);
        assert_eq!(decision.fanout_chunk_count, 2);
        assert_eq!(decision.direct_amount_sats, 90_000);
        assert_eq!(decision.fanout_amount_sats, 39_000);
        assert_eq!(decision.total_amount_sats, 129_000);
    }

    #[test]
    fn direct_ties_use_canonical_account_order() {
        let recipients = vec![
            recipient("miner-c", "03", 20_000),
            recipient("miner-b", "02", 20_000),
            recipient("miner-a", "01", 20_000),
        ];

        let decision = select_settlement_mode(&recipients, FLOOR, &config(2, 10)).unwrap();

        assert_eq!(
            recipient_names(&decision.direct_recipients),
            ["miner-a", "miner-b"]
        );
        assert_eq!(
            fanout_chunk_names(&decision),
            vec![vec!["miner-c".to_string()]]
        );
    }

    #[test]
    fn all_sub_floor_is_fanout_only_and_split_into_bounded_chunks() {
        let recipients = (0..50_000)
            .map(|index| recipient(&format!("miner-{index:05}"), &format!("{index:08}"), 1))
            .collect::<Vec<_>>();

        let decision = select_settlement_mode(&recipients, FLOOR, &config(500, 1_000)).unwrap();

        assert_eq!(decision.mode, SettlementMode::CtvFanout);
        assert_eq!(decision.direct_recipient_count, 0);
        assert_eq!(decision.fanout_recipient_count, 50_000);
        assert_eq!(decision.fanout_chunk_count, 50);
        assert_eq!(decision.coinbase_settlement_output_count, 50);
        assert!(decision
            .fanout_chunks
            .iter()
            .all(|chunk| chunk.recipient_count <= 1_000));
        assert!(decision.fanout_chunks.iter().all(|chunk| {
            estimated_ctv_fanout_weight_bytes(chunk.recipient_count).unwrap()
                <= QBIT_TRUC_MAX_TX_WEIGHT_BYTES
        }));
    }

    #[test]
    fn fanout_weight_estimate_pins_truc_boundary() {
        assert!(estimated_ctv_fanout_weight_bytes(1_000).unwrap() <= QBIT_TRUC_MAX_TX_WEIGHT_BYTES);
        assert_eq!(estimated_ctv_fanout_weight_bytes(1_160).unwrap(), 49_973);
        assert!(estimated_ctv_fanout_weight_bytes(1_161).unwrap() > QBIT_TRUC_MAX_TX_WEIGHT_BYTES);
    }

    #[test]
    fn default_policy_boundary_requires_more_fanout_levels_above_coinbase_cap() {
        let config = SettlementModeConfig::default();

        assert_eq!(
            select_direct_count(
                DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS
                    * DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION,
                0,
                &config,
            )
            .unwrap(),
            0
        );
        assert!(select_direct_count(
            DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS
                * DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION
                + 1,
            0,
            &config,
        )
        .unwrap_err()
        .to_string()
        .contains("more than 16"));
    }

    #[test]
    fn default_non_datum_policy_keeps_coinbase_outputs_conservative() {
        let recipients = (0..500)
            .map(|index| recipient(&format!("miner-{index:05}"), &format!("{index:08}"), 20_000))
            .collect::<Vec<_>>();

        let decision =
            select_settlement_mode(&recipients, FLOOR, &SettlementModeConfig::default()).unwrap();

        assert_eq!(
            SettlementModeConfig::default().max_direct_coinbase_outputs,
            12
        );
        assert_eq!(
            SettlementModeConfig::default().max_coinbase_settlement_outputs,
            16
        );
        assert_eq!(decision.mode, SettlementMode::HybridCoinbaseCtvFanout);
        assert_eq!(decision.direct_recipient_count, 12);
        assert_eq!(decision.fanout_recipient_count, 488);
        assert_eq!(decision.fanout_chunk_count, 1);
        assert_eq!(decision.coinbase_settlement_output_count, 13);
        assert!(decision.coinbase_settlement_output_count <= 16);
    }

    #[test]
    fn default_non_datum_policy_fails_when_fanout_chunks_exceed_budget() {
        let recipients = (0..16_001)
            .map(|index| recipient(&format!("miner-{index:05}"), &format!("{index:08}"), 1))
            .collect::<Vec<_>>();

        let err = select_settlement_mode(&recipients, FLOOR, &SettlementModeConfig::default())
            .unwrap_err();

        assert!(err.to_string().contains("more than 16"));
    }

    #[test]
    fn fanout_covenant_outputs_reserve_coinbase_settlement_slots() {
        let recipients = (0..800)
            .map(|index| recipient(&format!("miner-{index:05}"), &format!("{index:08}"), 20_000))
            .collect::<Vec<_>>();

        let decision = select_settlement_mode(&recipients, FLOOR, &config(500, 1_000)).unwrap();

        assert_eq!(decision.mode, SettlementMode::HybridCoinbaseCtvFanout);
        assert_eq!(decision.direct_recipient_count, 499);
        assert_eq!(decision.fanout_recipient_count, 301);
        assert_eq!(decision.fanout_chunk_count, 1);
        assert_eq!(decision.coinbase_settlement_output_count, 500);
    }

    #[test]
    fn reserved_coinbase_outputs_reduce_direct_slots() {
        let recipients = (0..500)
            .map(|index| recipient(&format!("miner-{index:05}"), &format!("{index:08}"), 20_000))
            .collect::<Vec<_>>();

        let decision = select_settlement_mode(
            &recipients,
            FLOOR,
            &capped_config(MAX_COINBASE_SETTLEMENT_OUTPUTS, 500, 1_000, 1),
        )
        .unwrap();

        assert_eq!(decision.direct_recipient_count, 498);
        assert_eq!(decision.fanout_recipient_count, 2);
        assert_eq!(decision.fanout_chunk_count, 1);
        assert_eq!(decision.reserved_coinbase_output_count, 1);
        assert_eq!(decision.coinbase_settlement_output_count, 500);
    }

    #[test]
    fn errors_when_required_fanout_chunks_exceed_coinbase_cap() {
        let recipients = (0..2_001)
            .map(|index| recipient(&format!("miner-{index:05}"), &format!("{index:08}"), 1))
            .collect::<Vec<_>>();

        let err =
            select_settlement_mode(&recipients, FLOOR, &capped_config(2, 0, 1_000, 0)).unwrap_err();

        assert!(err.to_string().contains("more than 2"));
    }

    #[test]
    fn zero_direct_cap_routes_valid_recipients_to_fanout() {
        let recipients = vec![
            recipient("miner-a", "01", 30_000),
            recipient("miner-b", "02", 20_000),
        ];

        let decision = select_settlement_mode(&recipients, FLOOR, &config(0, 10)).unwrap();

        assert_eq!(decision.mode, SettlementMode::CtvFanout);
        assert!(decision.direct_recipients.is_empty());
        assert_eq!(
            fanout_chunk_names(&decision),
            vec![vec!["miner-a".to_string(), "miner-b".to_string()]]
        );
    }

    #[test]
    fn default_direct_threshold_routes_boundary_to_direct_or_fanout() {
        assert_eq!(DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS, 10_485_760);

        let recipients = vec![
            recipient("below", "01", DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS - 1),
            recipient("at", "02", DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS),
            recipient("above", "03", DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS + 1),
        ];

        let decision = select_settlement_mode(
            &recipients,
            DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
            &config(500, 10),
        )
        .unwrap();

        assert_eq!(decision.mode, SettlementMode::HybridCoinbaseCtvFanout);
        assert_eq!(
            recipient_names(&decision.direct_recipients),
            ["at", "above"]
        );
        assert_eq!(
            fanout_chunk_names(&decision),
            vec![vec!["below".to_string()]]
        );
    }

    #[test]
    fn fanout_fee_allocation_uses_integer_remainders_and_canonical_ties() {
        let recipients = vec![
            recipient("a", "01", 100),
            recipient("b", "02", 100),
            recipient("c", "03", 100),
        ];

        let decision = apply_proportional_fanout_fee(&recipients, 10, 1).unwrap();

        assert_eq!(decision.total_gross_sats, 300);
        assert_eq!(decision.requested_fee_sats, 10);
        assert_eq!(decision.applied_fee_sats, 10);
        assert!(decision.carry_forward_recipients.is_empty());
        assert_eq!(
            decision
                .payable_recipients
                .iter()
                .map(|recipient| (
                    recipient.recipient_id.as_str(),
                    recipient.gross_amount_sats,
                    recipient.fee_sats,
                    recipient.net_amount_sats,
                ))
                .collect::<Vec<_>>(),
            vec![("a", 100, 4, 96), ("b", 100, 3, 97), ("c", 100, 3, 97)]
        );
    }

    #[test]
    fn fanout_fee_allocation_prunes_recipients_pushed_below_floor_and_recomputes() {
        let recipients = vec![
            recipient("small-a", "01", 10_000),
            recipient("small-b", "02", 10_000),
            recipient("large", "03", 100_000),
        ];

        let decision = apply_proportional_fanout_fee(&recipients, 1_000, 9_950).unwrap();

        assert_eq!(decision.total_gross_sats, 120_000);
        assert_eq!(decision.applied_fee_sats, 1_000);
        assert_eq!(
            decision
                .payable_recipients
                .iter()
                .map(|recipient| (
                    recipient.recipient_id.as_str(),
                    recipient.gross_amount_sats,
                    recipient.fee_sats,
                    recipient.net_amount_sats,
                ))
                .collect::<Vec<_>>(),
            vec![("large", 100_000, 1_000, 99_000)]
        );
        assert_eq!(
            decision
                .carry_forward_recipients
                .iter()
                .map(|recipient| (
                    recipient.recipient_id.as_str(),
                    recipient.gross_amount_sats,
                    recipient.fee_sats,
                    recipient.net_amount_sats,
                ))
                .collect::<Vec<_>>(),
            vec![
                ("small-a", 10_000, 0, 10_000),
                ("small-b", 10_000, 0, 10_000)
            ]
        );
    }

    #[test]
    fn fanout_fee_allocation_returns_all_carry_when_no_payable_recipient_survives() {
        let recipients = vec![recipient("tiny", "01", 10_000)];

        let decision = apply_proportional_fanout_fee(&recipients, 1_000, 9_500).unwrap();

        assert_eq!(decision.applied_fee_sats, 0);
        assert!(decision.payable_recipients.is_empty());
        assert_eq!(
            decision
                .carry_forward_recipients
                .iter()
                .map(|recipient| (
                    recipient.recipient_id.as_str(),
                    recipient.gross_amount_sats,
                    recipient.fee_sats,
                    recipient.net_amount_sats,
                ))
                .collect::<Vec<_>>(),
            vec![("tiny", 10_000, 0, 10_000)]
        );
    }

    #[test]
    fn fanout_fee_estimate_uses_runtime_rate_premium_and_ceil_rounding() {
        let policy = FanoutFeeRatePolicy::new(1_000, DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS);

        let fee_sats = estimate_ctv_fanout_fee_sats(3, &policy).unwrap();

        assert_eq!(estimated_ctv_fanout_weight_bytes(3).unwrap(), 220);
        assert_eq!(fee_sats, 264);
    }

    #[test]
    fn fanout_fee_estimate_rejects_invalid_policy_inputs() {
        assert!(estimate_ctv_fanout_fee_sats(
            0,
            &FanoutFeeRatePolicy::new(1_000, DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS)
        )
        .unwrap_err()
        .to_string()
        .contains("recipient count"));
        assert!(estimate_ctv_fanout_fee_sats(
            1,
            &FanoutFeeRatePolicy::new(0, DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS)
        )
        .unwrap_err()
        .to_string()
        .contains("fee_rate"));
        assert!(
            estimate_ctv_fanout_fee_sats(1, &FanoutFeeRatePolicy::new(1_000, 0))
                .unwrap_err()
                .to_string()
                .contains("premium")
        );
    }

    #[test]
    fn estimated_fanout_fee_allocation_prunes_below_floor_recipients() {
        let recipients = vec![
            recipient("small-a", "01", 10_000),
            recipient("small-b", "02", 10_000),
            recipient("large", "03", 100_000),
        ];
        let policy = FanoutFeeRatePolicy::new(20_000, DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS);

        let decision =
            apply_estimated_proportional_fanout_fee(&recipients, 9_950, &policy).unwrap();

        assert_eq!(decision.requested_fee_sats, 5_280);
        assert_eq!(decision.applied_fee_sats, 5_280);
        assert_eq!(
            decision
                .payable_recipients
                .iter()
                .map(|recipient| (
                    recipient.recipient_id.as_str(),
                    recipient.gross_amount_sats,
                    recipient.fee_sats,
                    recipient.net_amount_sats,
                ))
                .collect::<Vec<_>>(),
            vec![("large", 100_000, 5_280, 94_720)]
        );
        assert_eq!(
            decision
                .carry_forward_recipients
                .iter()
                .map(|recipient| (
                    recipient.recipient_id.as_str(),
                    recipient.gross_amount_sats,
                    recipient.fee_sats,
                    recipient.net_amount_sats,
                ))
                .collect::<Vec<_>>(),
            vec![
                ("small-a", 10_000, 0, 10_000),
                ("small-b", 10_000, 0, 10_000)
            ]
        );
    }

    #[test]
    fn partition_is_input_order_independent() {
        let mut recipients = vec![
            recipient("miner-a", "01", 10_000),
            recipient("miner-b", "02", 50_000),
            recipient("miner-c", "03", 5_000),
            recipient("miner-d", "04", 20_000),
            recipient("miner-e", "05", 3_000),
        ];
        let forward = select_settlement_mode(&recipients, FLOOR, &config(2, 2)).unwrap();
        recipients.reverse();
        let reversed = select_settlement_mode(&recipients, FLOOR, &config(2, 2)).unwrap();

        assert_eq!(forward, reversed);
    }

    #[test]
    fn typed_account_keys_do_not_collapse_delimiter_values() {
        let p2mr_program_hex = "11".repeat(32);
        let recipients = vec![
            recipient_with_program("c", "a\0b", p2mr_program_hex.clone(), 20_000),
            recipient_with_program("b\0c", "a", p2mr_program_hex, 20_000),
        ];

        let decision = select_settlement_mode(&recipients, FLOOR, &config(2, 10)).unwrap();

        assert_eq!(decision.direct_recipient_count, 2);
        assert_eq!(decision.total_amount_sats, 40_000);
    }

    #[test]
    fn validates_and_normalizes_p2mr_program_hex() {
        let uppercase = recipient_with_program("miner-a", "01", "AB".repeat(32), 20_000);
        let decision = select_settlement_mode(&[uppercase], FLOOR, &config(1, 10)).unwrap();
        assert_eq!(
            decision.direct_recipients[0].p2mr_program_hex,
            "ab".repeat(32)
        );

        let non_hex = recipient_with_program("miner-b", "02", "zz".repeat(32), 20_000);
        assert!(select_settlement_mode(&[non_hex], FLOOR, &config(1, 10))
            .unwrap_err()
            .to_string()
            .contains("p2mr_program_hex must be hex"));

        let short = recipient_with_program("miner-c", "03", "11".repeat(31), 20_000);
        assert!(select_settlement_mode(&[short], FLOOR, &config(1, 10))
            .unwrap_err()
            .to_string()
            .contains("p2mr_program_hex must be 32 bytes"));

        let duplicate_after_normalization = [
            recipient_with_program("miner-d", "04", "CD".repeat(32), 20_000),
            recipient_with_program("miner-d", "04", "cd".repeat(32), 30_000),
        ];
        assert!(
            select_settlement_mode(&duplicate_after_normalization, FLOOR, &config(2, 10))
                .unwrap_err()
                .to_string()
                .contains("duplicate settlement recipient")
        );
    }

    #[test]
    fn every_recipient_is_assigned_once_and_chunks_are_bounded() {
        for total in [1, 2, 499, 500, 501, 5_000] {
            let recipients = (0..total)
                .map(|index| {
                    let amount = if index % 3 == 0 { 25_000 } else { 2_500 };
                    recipient(&format!("miner-{index:05}"), &format!("{index:08}"), amount)
                })
                .collect::<Vec<_>>();
            let decision = select_settlement_mode(&recipients, FLOOR, &config(500, 777)).unwrap();
            let assigned = decision.direct_recipient_count + decision.fanout_recipient_count;

            assert_eq!(assigned, total);
            assert_eq!(decision.fanout_chunk_count, decision.fanout_chunks.len());
            assert!(decision.direct_recipient_count <= 500);
            assert!(decision
                .fanout_chunks
                .iter()
                .all(|chunk| chunk.recipient_count <= 777));
            assert_eq!(
                decision.total_amount_sats,
                sum_amounts(&recipients).unwrap()
            );
        }
    }

    #[test]
    fn rejects_invalid_config_empty_batch_zero_amount_and_duplicate_accounts() {
        assert!(
            select_settlement_mode(&[recipient("a", "01", 1)], FLOOR, &config(1, 0))
                .unwrap_err()
                .to_string()
                .contains("must be positive")
        );
        assert!(select_settlement_mode(
            &[recipient("a", "01", 1)],
            FLOOR,
            &config(MAX_DIRECT_COINBASE_OUTPUTS + 1, 1),
        )
        .unwrap_err()
        .to_string()
        .contains("max_direct_coinbase_outputs"));
        assert!(select_settlement_mode(
            &[recipient("a", "01", 1)],
            FLOOR,
            &config(1, MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION + 1),
        )
        .unwrap_err()
        .to_string()
        .contains("must be <="));
        assert!(select_settlement_mode(&[], FLOOR, &config(1, 1))
            .unwrap_err()
            .to_string()
            .contains("no recipients"));
        assert!(
            select_settlement_mode(&[recipient("a", "01", 0)], FLOOR, &config(1, 1))
                .unwrap_err()
                .to_string()
                .contains("amount must be positive")
        );
        assert!(select_settlement_mode(
            &[recipient("a", "01", 1), recipient("a", "01", 2)],
            FLOOR,
            &config(1, 1),
        )
        .unwrap_err()
        .to_string()
        .contains("duplicate settlement recipient"));
    }
}
