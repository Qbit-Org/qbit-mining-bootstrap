use qbit_pool_builder::{
    build_manifest, build_signed_manifest, verify_ed25519_message, verify_signed_manifest,
    BuilderError, CoinbaseBuildRequest, ManifestSignature, ManifestSigningKey, PayoutManifest,
    SignedPayoutManifest, WeightedEntitlement,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

mod ctv;
pub use ctv::*;

mod broadcast;
pub use broadcast::*;

mod audit_body_ref;
pub use audit_body_ref::*;

mod settlement;
pub use settlement::*;

pub const PRISM_WINDOW_MULTIPLIER: u128 = 8;
pub const DEFAULT_P2MR_SPEND_INPUT_BYTES: u64 = 3_680;
pub const DEFAULT_MIN_OUTPUT_FEERATE_SATS_PER_BYTE: u64 = 1;
pub const DEFAULT_MIN_OUTPUT_SAFETY_MULTIPLIER: u64 = 4;
pub const QBIT_COINBASE_MATURITY_BLOCKS: u64 = 1_000;
pub const PRISM_AUDIT_COMMITMENT_LEAF_TAG: &str = "qbit.prism.audit.commitment.v1";
pub const AUDIT_BUNDLE_SCHEMA_V1: &str = "qbit.prism.audit-bundle.v1";
pub const AUDIT_BUNDLE_SCHEMA_V1_1: &str = "qbit.prism.audit-bundle.v1.1";

#[derive(Debug, thiserror::Error)]
pub enum PrismError {
    #[error("network difficulty must be positive")]
    ZeroNetworkDifficulty,
    #[error("share difficulty must be positive for share sequence {share_seq}")]
    ZeroShareDifficulty { share_seq: u64 },
    #[error("duplicate share_id in PRISM window input: {share_id}")]
    DuplicateShareId { share_id: String },
    #[error("window arithmetic overflowed")]
    WindowOverflow,
    #[error("payout policy arithmetic overflowed")]
    PayoutPolicyOverflow,
    #[error("minimum output floor must be positive")]
    InvalidMinOutputFloor,
    #[error("pool fee bps {fee_bps} exceeds 10000")]
    PoolFeeBpsTooHigh { fee_bps: u16 },
    #[error("pool fee account duplicates a miner payout account")]
    DuplicatePoolFeeAccount,
    #[error("no eligible shares at or before job issue time {anchor_job_issued_at_ms}")]
    EmptyWindow { anchor_job_issued_at_ms: i64 },
    #[error("duplicate carry-forward balance for recipient {recipient_id}")]
    DuplicateCarryForward { recipient_id: String },
    #[error(
        "coinbase value {coinbase_value_sats} is below minimum output floor {min_output_sats}"
    )]
    CoinbaseBelowFloor {
        coinbase_value_sats: u64,
        min_output_sats: u64,
    },
    #[error("no on-chain payout recipient remains after applying minimum output floor")]
    NoOnchainRecipients,
    #[error(
        "on-chain payouts would exceed selected candidate balances: coinbase value {coinbase_value_sats} > selected candidate balance {selected_candidate_balance_sats}"
    )]
    PayoutExceedsCandidateBalance {
        coinbase_value_sats: u64,
        selected_candidate_balance_sats: u128,
    },
    #[error("policy output for recipient {recipient_id} is below floor: {amount_sats} < {min_output_sats}")]
    PayoutBelowFloor {
        recipient_id: String,
        amount_sats: u64,
        min_output_sats: u64,
    },
    #[error("disconnect would reverse matured payout entry for block {block_hash}")]
    MaturePayoutDisconnect { block_hash: String },
    #[error("builder failed: {0}")]
    Builder(#[from] BuilderError),
    #[error("json serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("audit bundle mismatch in {artifact}")]
    AuditMismatch { artifact: &'static str },
    #[error("on-chain coinbase tx hex does not match the audit bundle")]
    AuditCoinbaseTxMismatch,
    #[error(
        "expected coinbase value {expected_coinbase_value_sats} does not match audit bundle coinbase value {actual_coinbase_value_sats}"
    )]
    ExpectedCoinbaseValueMismatch {
        expected_coinbase_value_sats: u64,
        actual_coinbase_value_sats: u64,
    },
    #[error("ledger attestation signing key must be distinct from coinbase manifest signing key")]
    LedgerAttestationKeyReuse,
    #[error("invalid CTV fanout manifest: {reason}")]
    InvalidCtvFanoutManifest { reason: String },
    #[error("invalid audit commitment: {reason}")]
    InvalidAuditCommitment { reason: String },
    #[error("cannot select a settlement mode: {reason}")]
    SettlementModeSelection { reason: String },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AcceptedShare {
    pub share_seq: u64,
    pub share_id: String,
    pub miner_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    pub share_difficulty: u128,
    pub network_difficulty: u128,
    pub template_height: u64,
    pub job_id: String,
    pub job_issued_at_ms: i64,
    pub accepted_at_ms: i64,
    pub ntime: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credit_policy: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FoundBlock {
    pub block_height: u64,
    pub coinbase_value_sats: u64,
    pub network_difficulty: u128,
    pub anchor_job_issued_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PrismWindow {
    pub anchor_job_issued_at_ms: i64,
    pub anchor_share_seq: u64,
    pub requested_window_weight: u128,
    pub counted_window_weight: u128,
    pub shares: Vec<CountedShare>,
    pub entitlements: Vec<WeightedEntitlement>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PrismRewardManifest {
    pub schema: String,
    pub block_height: u64,
    pub coinbase_value_sats: u64,
    pub network_difficulty: u128,
    pub window_multiplier: u128,
    pub requested_window_weight: u128,
    pub counted_window_weight: u128,
    pub anchor_job_issued_at_ms: i64,
    pub anchor_share_seq: u64,
    pub newest_share_seq: u64,
    pub oldest_share_seq: u64,
    pub included_share_count: usize,
    pub share_slice_digest_hex: String,
    pub shares: Vec<CountedShare>,
    pub entitlements: Vec<WeightedEntitlement>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PayoutPolicy {
    pub p2mr_spend_input_bytes: u64,
    pub target_feerate_sats_per_byte: u64,
    pub safety_multiplier: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_output_sats: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_fee_policy: Option<PoolFeePolicy>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PoolFeePolicy {
    pub fee_bps: u16,
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CarryForwardBalance {
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    pub balance_sats: i128,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PayoutPolicyManifest {
    pub schema: String,
    pub block_height: u64,
    pub coinbase_value_sats: u64,
    pub min_output_sats: u64,
    pub floor_formula: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_fee: Option<PoolFeeManifest>,
    pub accounts: Vec<PayoutPolicyAccount>,
    pub onchain_entitlements: Vec<WeightedEntitlement>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PayoutPolicyAccount {
    #[serde(default, skip_serializing_if = "is_default_account_type")]
    pub account_type: PayoutPolicyAccountType,
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    pub gross_amount_sats: u64,
    pub prior_balance_sats: i128,
    pub candidate_balance_sats: i128,
    pub onchain_amount_sats: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub settlement_fee_sats: u64,
    pub carry_forward_balance_sats: i128,
    pub action: PayoutPolicyAction,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PoolFeeManifest {
    pub fee_bps: u16,
    #[serde(default)]
    pub earned_pool_fee_sats: u64,
    #[serde(default)]
    pub swept_dust_liability_sats: u64,
    pub amount_sats: u64,
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PayoutPolicyAccountType {
    Miner,
    PoolFee,
}

impl Default for PayoutPolicyAccountType {
    fn default() -> Self {
        Self::Miner
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PayoutPolicyAction {
    Onchain,
    Accrued,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PayoutMaturityEntry {
    pub block_hash: String,
    pub block_height: u64,
    #[serde(default, skip_serializing_if = "is_default_account_type")]
    pub account_type: PayoutPolicyAccountType,
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    pub gross_amount_sats: u64,
    pub prior_balance_sats: i128,
    pub candidate_balance_sats: i128,
    pub onchain_amount_sats: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub settlement_fee_sats: u64,
    pub carry_forward_balance_sats: i128,
    pub action: PayoutPolicyAction,
    pub state: PayoutMaturityState,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PayoutMaturityState {
    Immature,
    Mature,
    Reversed,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AuditBundle {
    pub schema: String,
    pub shares: Vec<AcceptedShare>,
    pub found_block: FoundBlock,
    pub prior_balances: Vec<CarryForwardBalance>,
    pub payout_policy: PayoutPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coinbase_script_sig_suffix_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub witness_merkle_leaves_hex: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audit_commitment_leaves_hex: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_commitment_root_hex: Option<String>,
    pub ledger_window_attestation: LedgerWindowAttestation,
    pub reward_manifest: PrismRewardManifest,
    pub payout_policy_manifest: PayoutPolicyManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settlement_mode_decision: Option<SettlementModeDecision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ctv_fanout_fee_policy: Option<FanoutFeeRatePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ctv_fanout_manifest_set: Option<CtvFanoutManifestSet>,
    pub signed_coinbase_manifest: SignedPayoutManifest,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LedgerWindowAttestation {
    pub schema: String,
    pub block_height: u64,
    pub coinbase_value_sats: u64,
    pub anchor_job_issued_at_ms: i64,
    pub network_difficulty: u128,
    pub share_slice_digest_hex: String,
    pub prior_balances_digest_hex: String,
    pub signature: ManifestSignature,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AuditVerificationReport {
    pub schema: String,
    pub block_height: u64,
    pub coinbase_value_sats: u64,
    pub reward_manifest_sha256_hex: String,
    pub payout_policy_manifest_sha256_hex: String,
    pub prism_audit_commitment_leaf_hex: String,
    pub audit_commitment_root_hex: String,
    pub coinbase_manifest_sha256_hex: String,
    pub audit_bundle_sha256_hex: String,
    pub coinbase_txid: String,
    pub coinbase_wtxid: String,
    pub coinbase_tx_hex: String,
    pub min_output_sats: u64,
    pub onchain_output_count: usize,
    pub accrued_account_count: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CountedShare {
    pub share_seq: u64,
    pub share_id: String,
    pub miner_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    pub share_difficulty: u128,
    pub counted_difficulty: u128,
    pub job_issued_at_ms: i64,
    pub accepted_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credit_policy: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MinerKey {
    miner_id: String,
    order_key: String,
    p2mr_program_hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PolicyAccountSeed {
    recipient_id: String,
    order_key: String,
    p2mr_program_hex: String,
    gross_amount_sats: u64,
    prior_balance_sats: i128,
}

impl PayoutPolicy {
    pub fn day_one_default() -> Self {
        Self {
            p2mr_spend_input_bytes: DEFAULT_P2MR_SPEND_INPUT_BYTES,
            target_feerate_sats_per_byte: DEFAULT_MIN_OUTPUT_FEERATE_SATS_PER_BYTE,
            safety_multiplier: DEFAULT_MIN_OUTPUT_SAFETY_MULTIPLIER,
            min_output_sats: None,
            pool_fee_policy: None,
        }
    }

    pub fn min_output_sats(&self) -> Result<u64, PrismError> {
        if let Some(min_output_sats) = self.min_output_sats {
            if min_output_sats == 0 {
                return Err(PrismError::InvalidMinOutputFloor);
            }
            return Ok(min_output_sats);
        }
        self.p2mr_spend_input_bytes
            .checked_mul(self.target_feerate_sats_per_byte)
            .and_then(|value| value.checked_mul(self.safety_multiplier))
            .ok_or(PrismError::PayoutPolicyOverflow)
    }

    pub fn floor_formula(&self) -> String {
        if let Some(min_output_sats) = self.min_output_sats {
            return format!("configured fixed floor: {min_output_sats} sats");
        }
        format!(
            "{} bytes/input * {} sat/byte * {}x safety",
            self.p2mr_spend_input_bytes, self.target_feerate_sats_per_byte, self.safety_multiplier
        )
    }
}

pub fn compute_prism_window(
    shares: &[AcceptedShare],
    found_block: &FoundBlock,
) -> Result<PrismWindow, PrismError> {
    if found_block.network_difficulty == 0 {
        return Err(PrismError::ZeroNetworkDifficulty);
    }
    for share in shares {
        if share.share_difficulty == 0 {
            return Err(PrismError::ZeroShareDifficulty {
                share_seq: share.share_seq,
            });
        }
    }
    let mut seen_share_ids = BTreeSet::new();
    for share in shares {
        if !seen_share_ids.insert(share.share_id.as_str()) {
            return Err(PrismError::DuplicateShareId {
                share_id: share.share_id.clone(),
            });
        }
    }

    let requested_window_weight = found_block
        .network_difficulty
        .checked_mul(PRISM_WINDOW_MULTIPLIER)
        .ok_or(PrismError::WindowOverflow)?;
    let mut eligible = shares
        .iter()
        .filter(|share| {
            share.job_issued_at_ms <= found_block.anchor_job_issued_at_ms
                && share.accepted_at_ms <= found_block.anchor_job_issued_at_ms
        })
        .collect::<Vec<_>>();
    eligible.sort_by(|left, right| right.share_seq.cmp(&left.share_seq));

    let mut remaining = requested_window_weight;
    let mut counted_shares = Vec::new();
    for share in eligible {
        if remaining == 0 {
            break;
        }
        let counted = share.share_difficulty.min(remaining);
        counted_shares.push(CountedShare {
            share_seq: share.share_seq,
            share_id: share.share_id.clone(),
            miner_id: share.miner_id.clone(),
            order_key: share.order_key.clone(),
            p2mr_program_hex: share.p2mr_program_hex.clone(),
            share_difficulty: share.share_difficulty,
            counted_difficulty: counted,
            job_issued_at_ms: share.job_issued_at_ms,
            accepted_at_ms: share.accepted_at_ms,
            credit_policy: share.credit_policy.clone(),
        });
        remaining -= counted;
    }

    if counted_shares.is_empty() {
        return Err(PrismError::EmptyWindow {
            anchor_job_issued_at_ms: found_block.anchor_job_issued_at_ms,
        });
    }
    let anchor_share_seq = counted_shares[0].share_seq;

    let counted_window_weight = counted_shares
        .iter()
        .try_fold(0_u128, |sum, share| {
            sum.checked_add(share.counted_difficulty)
        })
        .ok_or(PrismError::WindowOverflow)?;
    let entitlements = aggregate_entitlements(&counted_shares)?;

    Ok(PrismWindow {
        anchor_job_issued_at_ms: found_block.anchor_job_issued_at_ms,
        anchor_share_seq,
        requested_window_weight,
        counted_window_weight,
        shares: counted_shares,
        entitlements,
    })
}

pub fn build_prism_reward_manifest(
    shares: &[AcceptedShare],
    found_block: &FoundBlock,
) -> Result<PrismRewardManifest, PrismError> {
    let window = compute_prism_window(shares, found_block)?;
    let newest_share_seq = window.shares[0].share_seq;
    let oldest_share_seq = window
        .shares
        .last()
        .expect("non-empty PRISM windows have a last share")
        .share_seq;
    Ok(PrismRewardManifest {
        schema: "qbit.prism.reward-manifest.v1".to_string(),
        block_height: found_block.block_height,
        coinbase_value_sats: found_block.coinbase_value_sats,
        network_difficulty: found_block.network_difficulty,
        window_multiplier: PRISM_WINDOW_MULTIPLIER,
        requested_window_weight: window.requested_window_weight,
        counted_window_weight: window.counted_window_weight,
        anchor_job_issued_at_ms: window.anchor_job_issued_at_ms,
        anchor_share_seq: window.anchor_share_seq,
        newest_share_seq,
        oldest_share_seq,
        included_share_count: window.shares.len(),
        share_slice_digest_hex: share_slice_digest_hex(&window.shares),
        shares: window.shares,
        entitlements: window.entitlements,
    })
}

pub fn canonical_reward_manifest_bytes(
    manifest: &PrismRewardManifest,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(manifest)
}

pub fn canonical_payout_policy_manifest_bytes(
    manifest: &PayoutPolicyManifest,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(manifest)
}

pub fn canonical_audit_bundle_bytes(bundle: &AuditBundle) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(bundle)
}

pub fn prism_audit_commitment_leaf_hex(
    reward_manifest: &PrismRewardManifest,
    payout_policy_manifest: &PayoutPolicyManifest,
) -> Result<String, PrismError> {
    let reward_manifest_hash = Sha256::digest(canonical_reward_manifest_bytes(reward_manifest)?);
    let payout_policy_manifest_hash = Sha256::digest(canonical_payout_policy_manifest_bytes(
        payout_policy_manifest,
    )?);
    let mut payload = Vec::with_capacity(PRISM_AUDIT_COMMITMENT_LEAF_TAG.len() + 64);
    payload.extend_from_slice(PRISM_AUDIT_COMMITMENT_LEAF_TAG.as_bytes());
    payload.extend_from_slice(&reward_manifest_hash);
    payload.extend_from_slice(&payout_policy_manifest_hash);
    Ok(sha256_hex(&payload))
}

pub fn audit_commitment_root_hex(commitment_leaves_hex: &[String]) -> Result<String, PrismError> {
    let leaves = parse_audit_commitment_leaves(commitment_leaves_hex)?;
    Ok(hex::encode(merkle_root(leaves)))
}

fn parse_audit_commitment_leaves(
    commitment_leaves_hex: &[String],
) -> Result<Vec<[u8; 32]>, PrismError> {
    if commitment_leaves_hex.is_empty() {
        return Err(PrismError::InvalidAuditCommitment {
            reason: "at least one commitment leaf is required".to_string(),
        });
    }
    commitment_leaves_hex
        .iter()
        .enumerate()
        .map(|(index, raw)| parse_audit_commitment_leaf(raw, index))
        .collect()
}

fn parse_audit_commitment_leaf(raw: &str, index: usize) -> Result<[u8; 32], PrismError> {
    let decoded = hex::decode(raw).map_err(|error| PrismError::InvalidAuditCommitment {
        reason: format!("commitment leaf {index} is not hex: {error}"),
    })?;
    if decoded.len() != 32 {
        return Err(PrismError::InvalidAuditCommitment {
            reason: format!(
                "commitment leaf {index} must be 32 bytes, got {}",
                decoded.len()
            ),
        });
    }
    let mut leaf = [0_u8; 32];
    leaf.copy_from_slice(&decoded);
    Ok(leaf)
}

fn merkle_root(mut hashes: Vec<[u8; 32]>) -> [u8; 32] {
    debug_assert!(!hashes.is_empty());
    while hashes.len() > 1 {
        let mut next = Vec::with_capacity((hashes.len() + 1) / 2);
        for pair in hashes.chunks(2) {
            let left = pair[0];
            let right = if pair.len() == 2 { pair[1] } else { pair[0] };
            next.push(hash256_pair(&left, &right));
        }
        hashes = next;
    }
    hashes[0]
}

fn hash256_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut payload = [0_u8; 64];
    payload[..32].copy_from_slice(left);
    payload[32..].copy_from_slice(right);
    hash256(&payload)
}

fn hash256(bytes: &[u8]) -> [u8; 32] {
    let first = Sha256::digest(bytes);
    let second = Sha256::digest(first);
    let mut out = [0_u8; 32];
    out.copy_from_slice(&second);
    out
}

pub fn build_prism_coinbase_request(
    shares: &[AcceptedShare],
    found_block: &FoundBlock,
) -> Result<(CoinbaseBuildRequest, PrismWindow), PrismError> {
    let window = compute_prism_window(shares, found_block)?;
    let request = CoinbaseBuildRequest {
        block_height: found_block.block_height,
        coinbase_value_sats: found_block.coinbase_value_sats,
        entitlements: window.entitlements.clone(),
        witness_nonce_hex: None,
        witness_merkle_leaves_hex: Vec::new(),
        coinbase_script_sig_suffix_hex: None,
    };
    Ok((request, window))
}

pub fn apply_payout_policy(
    reward_manifest: &PrismRewardManifest,
    prior_balances: &[CarryForwardBalance],
    policy: &PayoutPolicy,
) -> Result<PayoutPolicyManifest, PrismError> {
    let min_output_sats = policy.min_output_sats()?;
    let mut pool_fee = pool_fee_manifest(reward_manifest.coinbase_value_sats, policy)?;
    let fee_amount_sats = pool_fee
        .as_ref()
        .map(|manifest| manifest.amount_sats)
        .unwrap_or(0);
    let miner_reward_value_sats = reward_manifest
        .coinbase_value_sats
        .checked_sub(fee_amount_sats)
        .ok_or(PrismError::PayoutPolicyOverflow)?;
    if miner_reward_value_sats < min_output_sats {
        return Err(PrismError::CoinbaseBelowFloor {
            coinbase_value_sats: miner_reward_value_sats,
            min_output_sats,
        });
    }

    let settlement_entitlements =
        aggregate_entitlements_by_payout_program(&reward_manifest.entitlements)?;
    let gross_amounts =
        allocate_weighted_amounts(miner_reward_value_sats, &settlement_entitlements)?;
    let prior_by_program = carry_forward_by_payout_program(prior_balances)?;
    let mut seen_accounts = BTreeSet::new();
    let mut account_seeds = gross_amounts
        .into_iter()
        .map(|(entitlement, gross_amount_sats)| {
            let key = payout_program_key(&entitlement.p2mr_program_hex);
            seen_accounts.insert(key.clone());
            let prior_balance_sats = prior_by_program
                .get(&key)
                .map(|balance| balance.balance_sats)
                .unwrap_or(0);
            Ok(PolicyAccountSeed {
                recipient_id: entitlement.recipient_id,
                order_key: entitlement.order_key,
                p2mr_program_hex: entitlement.p2mr_program_hex,
                gross_amount_sats,
                prior_balance_sats,
            })
        })
        .collect::<Result<Vec<_>, PrismError>>()?;

    for balance in prior_by_program.values() {
        let key = payout_program_key(&balance.p2mr_program_hex);
        if seen_accounts.contains(&key) {
            continue;
        }
        account_seeds.push(PolicyAccountSeed {
            recipient_id: balance.recipient_id.clone(),
            order_key: balance.order_key.clone(),
            p2mr_program_hex: balance.p2mr_program_hex.clone(),
            gross_amount_sats: 0,
            prior_balance_sats: balance.balance_sats,
        });
        seen_accounts.insert(key);
    }

    let mut accounts = account_seeds
        .into_iter()
        .map(|seed| {
            let candidate_balance_sats = seed
                .prior_balance_sats
                .checked_add(i128::from(seed.gross_amount_sats))
                .ok_or(PrismError::PayoutPolicyOverflow)?;
            Ok(PayoutPolicyAccount {
                account_type: PayoutPolicyAccountType::Miner,
                recipient_id: seed.recipient_id,
                order_key: seed.order_key,
                p2mr_program_hex: seed.p2mr_program_hex,
                gross_amount_sats: seed.gross_amount_sats,
                prior_balance_sats: seed.prior_balance_sats,
                candidate_balance_sats,
                onchain_amount_sats: 0,
                settlement_fee_sats: 0,
                carry_forward_balance_sats: candidate_balance_sats,
                action: PayoutPolicyAction::Accrued,
            })
        })
        .collect::<Result<Vec<_>, PrismError>>()?;

    let mut onchain_amounts = Vec::new();
    let eligible_indices = eligible_onchain_accounts(&accounts, min_output_sats);
    if eligible_indices.is_empty() {
        if pool_fee.is_none() {
            return Err(PrismError::NoOnchainRecipients);
        }
        add_swept_dust_to_pool_fee(&mut pool_fee, miner_reward_value_sats)?;
    } else {
        let eligible_candidate_sum = selected_candidate_balance_sum(&accounts, &eligible_indices)?;
        if eligible_candidate_sum < u128::from(miner_reward_value_sats) {
            if pool_fee.is_none() {
                return Err(PrismError::PayoutExceedsCandidateBalance {
                    coinbase_value_sats: miner_reward_value_sats,
                    selected_candidate_balance_sats: eligible_candidate_sum,
                });
            }
            let swept_dust_sats = miner_reward_value_sats
                .checked_sub(
                    u64::try_from(eligible_candidate_sum)
                        .map_err(|_| PrismError::PayoutPolicyOverflow)?,
                )
                .ok_or(PrismError::PayoutPolicyOverflow)?;
            add_swept_dust_to_pool_fee(&mut pool_fee, swept_dust_sats)?;
            for index in eligible_indices {
                let account = &accounts[index];
                let amount_sats = u64::try_from(account.candidate_balance_sats)
                    .map_err(|_| PrismError::PayoutPolicyOverflow)?;
                onchain_amounts.push((
                    WeightedEntitlement {
                        recipient_id: account.recipient_id.clone(),
                        order_key: account.order_key.clone(),
                        p2mr_program_hex: account.p2mr_program_hex.clone(),
                        weight: u128::from(amount_sats),
                    },
                    amount_sats,
                ));
            }
        } else {
            let emitted_indices =
                select_onchain_accounts(&accounts, miner_reward_value_sats, min_output_sats)?;
            let policy_weights = emitted_indices
                .iter()
                .map(|index| WeightedEntitlement {
                    recipient_id: accounts[*index].recipient_id.clone(),
                    order_key: accounts[*index].order_key.clone(),
                    p2mr_program_hex: accounts[*index].p2mr_program_hex.clone(),
                    weight: accounts[*index].candidate_balance_sats as u128,
                })
                .collect::<Vec<_>>();
            onchain_amounts = allocate_weighted_amounts(miner_reward_value_sats, &policy_weights)?;
        }
    }

    for (entitlement, amount_sats) in onchain_amounts {
        if amount_sats < min_output_sats {
            return Err(PrismError::PayoutBelowFloor {
                recipient_id: entitlement.recipient_id,
                amount_sats,
                min_output_sats,
            });
        }
        let account = accounts
            .iter_mut()
            .find(|account| {
                account.recipient_id == entitlement.recipient_id
                    && account.order_key == entitlement.order_key
                    && account.p2mr_program_hex == entitlement.p2mr_program_hex
            })
            .expect("selected on-chain account must exist");
        if i128::from(amount_sats) > account.candidate_balance_sats {
            return Err(PrismError::PayoutExceedsCandidateBalance {
                coinbase_value_sats: reward_manifest.coinbase_value_sats,
                selected_candidate_balance_sats: u128::try_from(account.candidate_balance_sats)
                    .map_err(|_| PrismError::PayoutPolicyOverflow)?,
            });
        }
        account.onchain_amount_sats = amount_sats;
        account.carry_forward_balance_sats = account
            .candidate_balance_sats
            .checked_sub(i128::from(amount_sats))
            .ok_or(PrismError::PayoutPolicyOverflow)?;
        account.action = PayoutPolicyAction::Onchain;
    }

    if let Some(pool_fee) = &pool_fee {
        let fee_key = payout_program_key(&pool_fee.p2mr_program_hex);
        if seen_accounts.contains(&fee_key) {
            return Err(PrismError::DuplicatePoolFeeAccount);
        }
        accounts.push(PayoutPolicyAccount {
            account_type: PayoutPolicyAccountType::PoolFee,
            recipient_id: pool_fee.recipient_id.clone(),
            order_key: pool_fee.order_key.clone(),
            p2mr_program_hex: pool_fee.p2mr_program_hex.clone(),
            gross_amount_sats: pool_fee.amount_sats,
            prior_balance_sats: 0,
            candidate_balance_sats: i128::from(pool_fee.amount_sats),
            onchain_amount_sats: pool_fee.amount_sats,
            settlement_fee_sats: 0,
            carry_forward_balance_sats: 0,
            action: PayoutPolicyAction::Onchain,
        });
    }

    let onchain_entitlements = accounts
        .iter()
        .filter(|account| account.onchain_amount_sats > 0)
        .map(|account| WeightedEntitlement {
            recipient_id: account.recipient_id.clone(),
            order_key: account.order_key.clone(),
            p2mr_program_hex: account.p2mr_program_hex.clone(),
            weight: u128::from(account.onchain_amount_sats),
        })
        .collect::<Vec<_>>();
    let onchain_sum = onchain_entitlements
        .iter()
        .try_fold(0_u64, |sum, entitlement| {
            sum.checked_add(
                u64::try_from(entitlement.weight).map_err(|_| PrismError::PayoutPolicyOverflow)?,
            )
            .ok_or(PrismError::PayoutPolicyOverflow)
        })?;
    if onchain_sum != reward_manifest.coinbase_value_sats {
        return Err(PrismError::PayoutPolicyOverflow);
    }

    accounts.sort_by(|left, right| {
        left.order_key
            .cmp(&right.order_key)
            .then_with(|| left.recipient_id.cmp(&right.recipient_id))
            .then_with(|| left.p2mr_program_hex.cmp(&right.p2mr_program_hex))
    });

    Ok(PayoutPolicyManifest {
        schema: "qbit.prism.payout-policy.v1".to_string(),
        block_height: reward_manifest.block_height,
        coinbase_value_sats: reward_manifest.coinbase_value_sats,
        min_output_sats,
        floor_formula: policy.floor_formula(),
        pool_fee,
        accounts,
        onchain_entitlements,
    })
}

pub fn build_policy_coinbase_request(
    reward_manifest: &PrismRewardManifest,
    prior_balances: &[CarryForwardBalance],
    policy: &PayoutPolicy,
) -> Result<(CoinbaseBuildRequest, PayoutPolicyManifest), PrismError> {
    build_policy_coinbase_request_with_coinbase_script_sig_suffix(
        reward_manifest,
        prior_balances,
        policy,
        None,
    )
}

pub fn build_policy_coinbase_request_with_coinbase_script_sig_suffix(
    reward_manifest: &PrismRewardManifest,
    prior_balances: &[CarryForwardBalance],
    policy: &PayoutPolicy,
    coinbase_script_sig_suffix_hex: Option<String>,
) -> Result<(CoinbaseBuildRequest, PayoutPolicyManifest), PrismError> {
    build_policy_coinbase_request_with_coinbase_options(
        reward_manifest,
        prior_balances,
        policy,
        coinbase_script_sig_suffix_hex,
        Vec::new(),
    )
}

pub fn build_policy_coinbase_request_with_coinbase_options(
    reward_manifest: &PrismRewardManifest,
    prior_balances: &[CarryForwardBalance],
    policy: &PayoutPolicy,
    coinbase_script_sig_suffix_hex: Option<String>,
    witness_merkle_leaves_hex: Vec<String>,
) -> Result<(CoinbaseBuildRequest, PayoutPolicyManifest), PrismError> {
    let manifest = apply_payout_policy(reward_manifest, prior_balances, policy)?;
    let request = CoinbaseBuildRequest {
        block_height: reward_manifest.block_height,
        coinbase_value_sats: reward_manifest.coinbase_value_sats,
        entitlements: manifest.onchain_entitlements.clone(),
        witness_nonce_hex: None,
        witness_merkle_leaves_hex,
        coinbase_script_sig_suffix_hex,
    };
    Ok((request, manifest))
}

pub fn build_maturity_entries(
    block_hash: &str,
    block_height: u64,
    policy_manifest: &PayoutPolicyManifest,
) -> Vec<PayoutMaturityEntry> {
    policy_manifest
        .accounts
        .iter()
        .map(|account| PayoutMaturityEntry {
            block_hash: block_hash.to_string(),
            block_height,
            account_type: account.account_type.clone(),
            recipient_id: account.recipient_id.clone(),
            order_key: account.order_key.clone(),
            p2mr_program_hex: account.p2mr_program_hex.clone(),
            gross_amount_sats: account.gross_amount_sats,
            prior_balance_sats: account.prior_balance_sats,
            candidate_balance_sats: account.candidate_balance_sats,
            onchain_amount_sats: account.onchain_amount_sats,
            settlement_fee_sats: account.settlement_fee_sats,
            carry_forward_balance_sats: account.carry_forward_balance_sats,
            action: account.action.clone(),
            state: PayoutMaturityState::Immature,
        })
        .collect()
}

pub fn update_maturity_states(
    entries: &[PayoutMaturityEntry],
    active_tip_height: u64,
) -> Vec<PayoutMaturityEntry> {
    entries
        .iter()
        .map(|entry| {
            let mut updated = entry.clone();
            if entry.state == PayoutMaturityState::Immature
                && active_tip_height
                    >= entry
                        .block_height
                        .saturating_add(QBIT_COINBASE_MATURITY_BLOCKS)
            {
                updated.state = PayoutMaturityState::Mature;
            }
            updated
        })
        .collect()
}

pub fn reverse_disconnected_blocks(
    entries: &[PayoutMaturityEntry],
    disconnected_block_hashes: &[String],
    active_tip_height: u64,
) -> Result<Vec<PayoutMaturityEntry>, PrismError> {
    entries
        .iter()
        .map(|entry| {
            if disconnected_block_hashes.contains(&entry.block_hash) {
                if entry
                    .block_height
                    .saturating_add(QBIT_COINBASE_MATURITY_BLOCKS)
                    <= active_tip_height
                {
                    return Err(PrismError::MaturePayoutDisconnect {
                        block_hash: entry.block_hash.clone(),
                    });
                }
                match entry.state {
                    PayoutMaturityState::Immature => {
                        let mut reversed = entry.clone();
                        reversed.state = PayoutMaturityState::Reversed;
                        Ok(reversed)
                    }
                    PayoutMaturityState::Mature => Err(PrismError::MaturePayoutDisconnect {
                        block_hash: entry.block_hash.clone(),
                    }),
                    PayoutMaturityState::Reversed => Ok(entry.clone()),
                }
            } else {
                Ok(entry.clone())
            }
        })
        .collect()
}

pub fn current_carry_forward_balances(entries: &[PayoutMaturityEntry]) -> Vec<CarryForwardBalance> {
    let mut balances: BTreeMap<String, CarryForwardBalance> = BTreeMap::new();
    for entry in entries {
        if entry.state == PayoutMaturityState::Reversed
            || entry.account_type == PayoutPolicyAccountType::PoolFee
        {
            continue;
        }
        let key = payout_program_key(&entry.p2mr_program_hex);
        // Current balances replay only active per-block deltas. The
        // carry_forward_balance_sats snapshot is audit evidence for that
        // block, but using it as current state can preserve reversed history.
        let delta_sats = i128::from(entry.gross_amount_sats)
            .checked_sub(i128::from(entry.onchain_amount_sats))
            .expect("u64 on-chain amount always fits i128");
        let balance = balances.entry(key).or_insert_with(|| CarryForwardBalance {
            recipient_id: entry.recipient_id.clone(),
            order_key: entry.order_key.clone(),
            p2mr_program_hex: entry.p2mr_program_hex.clone(),
            balance_sats: 0,
        });
        if (entry.order_key.as_str(), entry.recipient_id.as_str())
            < (balance.order_key.as_str(), balance.recipient_id.as_str())
        {
            balance.recipient_id = entry.recipient_id.clone();
            balance.order_key = entry.order_key.clone();
        }
        balance.balance_sats = balance
            .balance_sats
            .checked_add(delta_sats)
            .expect("carry-forward delta replay should not overflow i128");
    }
    balances
        .into_values()
        .filter(|balance| balance.balance_sats != 0)
        .collect()
}

pub fn owed_balances_for_display(entries: &[PayoutMaturityEntry]) -> Vec<CarryForwardBalance> {
    current_carry_forward_balances(entries)
        .into_iter()
        .map(|mut balance| {
            balance.balance_sats = balance.balance_sats.max(0);
            balance
        })
        .collect()
}

pub fn build_ledger_window_attestation(
    reward_manifest: &PrismRewardManifest,
    prior_balances: &[CarryForwardBalance],
    signing_key: &ManifestSigningKey,
) -> Result<LedgerWindowAttestation, PrismError> {
    let mut attestation = LedgerWindowAttestation {
        schema: "qbit.prism.ledger-window-attestation.v1".to_string(),
        block_height: reward_manifest.block_height,
        coinbase_value_sats: reward_manifest.coinbase_value_sats,
        anchor_job_issued_at_ms: reward_manifest.anchor_job_issued_at_ms,
        network_difficulty: reward_manifest.network_difficulty,
        share_slice_digest_hex: reward_manifest.share_slice_digest_hex.clone(),
        prior_balances_digest_hex: prior_balances_digest_hex(prior_balances),
        signature: ManifestSignature {
            algorithm: "ed25519".to_string(),
            public_key_hex: signing_key.public_key_hex(),
            signature_hex: String::new(),
        },
    };
    let payload = canonical_ledger_window_attestation_bytes(&attestation)?;
    attestation.signature.signature_hex = signing_key.sign_message_hex(&payload);
    Ok(attestation)
}

pub fn canonical_ledger_window_attestation_bytes(
    attestation: &LedgerWindowAttestation,
) -> Result<Vec<u8>, serde_json::Error> {
    #[derive(Serialize)]
    struct Payload<'a> {
        schema: &'a str,
        block_height: u64,
        coinbase_value_sats: u64,
        anchor_job_issued_at_ms: i64,
        network_difficulty: u128,
        share_slice_digest_hex: &'a str,
        prior_balances_digest_hex: &'a str,
    }

    serde_json::to_vec(&Payload {
        schema: &attestation.schema,
        block_height: attestation.block_height,
        coinbase_value_sats: attestation.coinbase_value_sats,
        anchor_job_issued_at_ms: attestation.anchor_job_issued_at_ms,
        network_difficulty: attestation.network_difficulty,
        share_slice_digest_hex: &attestation.share_slice_digest_hex,
        prior_balances_digest_hex: &attestation.prior_balances_digest_hex,
    })
}

fn audit_bundle_schema_for_shares(shares: &[AcceptedShare]) -> &'static str {
    if shares.iter().any(|share| share.credit_policy.is_some()) {
        AUDIT_BUNDLE_SCHEMA_V1_1
    } else {
        AUDIT_BUNDLE_SCHEMA_V1
    }
}

pub fn build_audit_bundle(
    shares: Vec<AcceptedShare>,
    found_block: FoundBlock,
    prior_balances: Vec<CarryForwardBalance>,
    payout_policy: PayoutPolicy,
    coinbase_signing_key: &ManifestSigningKey,
    ledger_signing_key: &ManifestSigningKey,
) -> Result<AuditBundle, PrismError> {
    build_audit_bundle_with_coinbase_script_sig_suffix(
        shares,
        found_block,
        prior_balances,
        payout_policy,
        None,
        coinbase_signing_key,
        ledger_signing_key,
    )
}

pub fn build_audit_bundle_with_coinbase_script_sig_suffix(
    shares: Vec<AcceptedShare>,
    found_block: FoundBlock,
    prior_balances: Vec<CarryForwardBalance>,
    payout_policy: PayoutPolicy,
    coinbase_script_sig_suffix_hex: Option<String>,
    coinbase_signing_key: &ManifestSigningKey,
    ledger_signing_key: &ManifestSigningKey,
) -> Result<AuditBundle, PrismError> {
    build_audit_bundle_with_coinbase_options(
        shares,
        found_block,
        prior_balances,
        payout_policy,
        coinbase_script_sig_suffix_hex,
        Vec::new(),
        coinbase_signing_key,
        ledger_signing_key,
    )
}

pub fn build_audit_bundle_with_coinbase_options(
    shares: Vec<AcceptedShare>,
    found_block: FoundBlock,
    prior_balances: Vec<CarryForwardBalance>,
    payout_policy: PayoutPolicy,
    coinbase_script_sig_suffix_hex: Option<String>,
    witness_merkle_leaves_hex: Vec<String>,
    coinbase_signing_key: &ManifestSigningKey,
    ledger_signing_key: &ManifestSigningKey,
) -> Result<AuditBundle, PrismError> {
    if coinbase_signing_key
        .public_key_hex()
        .eq_ignore_ascii_case(&ledger_signing_key.public_key_hex())
    {
        return Err(PrismError::LedgerAttestationKeyReuse);
    }
    let reward_manifest = build_prism_reward_manifest(&shares, &found_block)?;
    let ledger_window_attestation =
        build_ledger_window_attestation(&reward_manifest, &prior_balances, ledger_signing_key)?;
    let payout_policy_manifest =
        apply_payout_policy(&reward_manifest, &prior_balances, &payout_policy)?;
    let audit_commitment_leaves_hex = vec![prism_audit_commitment_leaf_hex(
        &reward_manifest,
        &payout_policy_manifest,
    )?];
    let audit_commitment_root_hex = audit_commitment_root_hex(&audit_commitment_leaves_hex)?;
    let coinbase_request = CoinbaseBuildRequest {
        block_height: reward_manifest.block_height,
        coinbase_value_sats: reward_manifest.coinbase_value_sats,
        entitlements: payout_policy_manifest.onchain_entitlements.clone(),
        witness_nonce_hex: Some(audit_commitment_root_hex.clone()),
        witness_merkle_leaves_hex: witness_merkle_leaves_hex.clone(),
        coinbase_script_sig_suffix_hex: coinbase_script_sig_suffix_hex.clone(),
    };
    let signed_coinbase_manifest = build_signed_manifest(coinbase_request, coinbase_signing_key)?;

    Ok(AuditBundle {
        schema: audit_bundle_schema_for_shares(&shares).to_string(),
        shares,
        found_block,
        prior_balances,
        payout_policy,
        coinbase_script_sig_suffix_hex,
        witness_merkle_leaves_hex,
        audit_commitment_leaves_hex,
        audit_commitment_root_hex: Some(audit_commitment_root_hex),
        ledger_window_attestation,
        reward_manifest,
        payout_policy_manifest,
        settlement_mode_decision: None,
        ctv_fanout_fee_policy: None,
        ctv_fanout_manifest_set: None,
        signed_coinbase_manifest,
    })
}

pub fn build_audit_bundle_with_ctv_settlement_options(
    shares: Vec<AcceptedShare>,
    found_block: FoundBlock,
    prior_balances: Vec<CarryForwardBalance>,
    payout_policy: PayoutPolicy,
    direct_floor_sats: u64,
    settlement_config: SettlementModeConfig,
    ctv_fanout_fee_policy: Option<FanoutFeeRatePolicy>,
    coinbase_script_sig_suffix_hex: Option<String>,
    witness_merkle_leaves_hex: Vec<String>,
    coinbase_signing_key: &ManifestSigningKey,
    ledger_signing_key: &ManifestSigningKey,
) -> Result<AuditBundle, PrismError> {
    if coinbase_signing_key
        .public_key_hex()
        .eq_ignore_ascii_case(&ledger_signing_key.public_key_hex())
    {
        return Err(PrismError::LedgerAttestationKeyReuse);
    }
    let reward_manifest = build_prism_reward_manifest(&shares, &found_block)?;
    let ledger_window_attestation =
        build_ledger_window_attestation(&reward_manifest, &prior_balances, ledger_signing_key)?;
    let mut payout_policy_manifest =
        apply_payout_policy(&reward_manifest, &prior_balances, &payout_policy)?;
    let settlement_recipients =
        settlement_recipients_from_entitlements(&payout_policy_manifest.onchain_entitlements)?;
    let settlement_mode_decision = select_settlement_mode(
        &settlement_recipients,
        direct_floor_sats,
        &settlement_config,
    )?;
    let fanout_fee_recipients = if let Some(fee_policy) = ctv_fanout_fee_policy.as_ref() {
        apply_ctv_fanout_fee_accounting(
            &mut payout_policy_manifest,
            &settlement_mode_decision,
            fee_policy,
        )?
    } else {
        BTreeMap::new()
    };
    let reward_manifest_sha256_hex =
        sha256_hex(&canonical_reward_manifest_bytes(&reward_manifest)?);
    let payout_policy_manifest_sha256_hex = sha256_hex(&canonical_payout_policy_manifest_bytes(
        &payout_policy_manifest,
    )?);

    let mut prepared_fanouts = Vec::new();
    for chunk in &settlement_mode_decision.fanout_chunks {
        let chunk_index =
            u32::try_from(chunk.chunk_index).map_err(|_| PrismError::PayoutPolicyOverflow)?;
        let chunk_count = u32::try_from(settlement_mode_decision.fanout_chunk_count)
            .map_err(|_| PrismError::PayoutPolicyOverflow)?;
        prepared_fanouts.push(prepare_ctv_fanout_precommitment(
            CtvFanoutPrecommitmentInput {
                block_height: reward_manifest.block_height,
                chunk_index,
                chunk_count,
                coinbase_value_sats: reward_manifest.coinbase_value_sats,
                settlement_mode: settlement_mode_decision.mode.clone(),
                reward_manifest_sha256_hex: reward_manifest_sha256_hex.clone(),
                payout_policy_manifest_sha256_hex: payout_policy_manifest_sha256_hex.clone(),
                payouts: chunk
                    .recipients
                    .iter()
                    .map(|recipient| {
                        let fee_recipient = fanout_fee_recipients.get(&account_key(
                            &recipient.recipient_id,
                            &recipient.order_key,
                            &recipient.p2mr_program_hex,
                        ));
                        CtvFanoutPayout {
                            recipient_id: recipient.recipient_id.clone(),
                            order_key: recipient.order_key.clone(),
                            p2mr_program_hex: recipient.p2mr_program_hex.clone(),
                            gross_amount_sats: fee_recipient
                                .map(|recipient| recipient.gross_amount_sats)
                                .unwrap_or(0),
                            fee_sats: fee_recipient
                                .map(|recipient| recipient.fee_sats)
                                .unwrap_or(0),
                            amount_sats: fee_recipient
                                .map(|recipient| recipient.net_amount_sats)
                                .unwrap_or(recipient.amount_sats),
                        }
                    })
                    .collect(),
            },
        )?);
    }

    let mut audit_commitment_leaves_hex = vec![prism_audit_commitment_leaf_hex(
        &reward_manifest,
        &payout_policy_manifest,
    )?];
    audit_commitment_leaves_hex.extend(
        prepared_fanouts
            .iter()
            .map(|fanout| fanout.commitment_witness_leaf_hex.clone()),
    );
    let audit_commitment_root_hex = audit_commitment_root_hex(&audit_commitment_leaves_hex)?;

    let coinbase_request = CoinbaseBuildRequest {
        block_height: reward_manifest.block_height,
        coinbase_value_sats: reward_manifest.coinbase_value_sats,
        entitlements: ctv_settlement_coinbase_entitlements(
            &settlement_mode_decision,
            &prepared_fanouts,
        )?,
        witness_nonce_hex: Some(audit_commitment_root_hex.clone()),
        witness_merkle_leaves_hex: witness_merkle_leaves_hex.clone(),
        coinbase_script_sig_suffix_hex: coinbase_script_sig_suffix_hex.clone(),
    };
    let signed_coinbase_manifest = build_signed_manifest(coinbase_request, coinbase_signing_key)?;
    let ctv_fanout_manifest_set = if prepared_fanouts.is_empty() {
        None
    } else {
        let manifests = prepared_fanouts
            .into_iter()
            .map(|prepared| {
                let parent_vout = find_coinbase_output_vout(
                    &signed_coinbase_manifest.manifest,
                    &prepared.covenant_recipient_id,
                    &prepared.covenant_order_key,
                    &prepared.covenant_p2mr_program_hex,
                    prepared.covenant_output_value_sats,
                )?;
                build_ctv_fanout_manifest_from_precommitment(
                    prepared.precommitment,
                    signed_coinbase_manifest.manifest.coinbase_tx_hex.clone(),
                    parent_vout,
                )
            })
            .collect::<Result<Vec<_>, PrismError>>()?;
        Some(build_ctv_fanout_manifest_set(manifests)?)
    };

    Ok(AuditBundle {
        schema: audit_bundle_schema_for_shares(&shares).to_string(),
        shares,
        found_block,
        prior_balances,
        payout_policy,
        coinbase_script_sig_suffix_hex,
        witness_merkle_leaves_hex,
        audit_commitment_leaves_hex,
        audit_commitment_root_hex: Some(audit_commitment_root_hex),
        ledger_window_attestation,
        reward_manifest,
        payout_policy_manifest,
        settlement_mode_decision: Some(settlement_mode_decision),
        ctv_fanout_fee_policy,
        ctv_fanout_manifest_set,
        signed_coinbase_manifest,
    })
}

fn apply_ctv_fanout_fee_accounting(
    policy_manifest: &mut PayoutPolicyManifest,
    settlement_mode_decision: &SettlementModeDecision,
    fee_policy: &FanoutFeeRatePolicy,
) -> Result<BTreeMap<String, FanoutFeeRecipient>, PrismError> {
    let mut recipients_by_key = BTreeMap::new();
    for chunk in &settlement_mode_decision.fanout_chunks {
        let fanout_fee_sats = estimate_ctv_fanout_fee_sats(chunk.recipients.len(), fee_policy)?;
        let fee_decision = apply_proportional_fanout_fee(
            &chunk.recipients,
            fanout_fee_sats,
            policy_manifest.min_output_sats,
        )?;
        if !fee_decision.carry_forward_recipients.is_empty() {
            return Err(PrismError::SettlementModeSelection {
                reason: "fanout fee would push a recipient below the payout floor".to_string(),
            });
        }
        for recipient in fee_decision.payable_recipients {
            let key = account_key(
                &recipient.recipient_id,
                &recipient.order_key,
                &recipient.p2mr_program_hex,
            );
            if recipients_by_key.insert(key, recipient).is_some() {
                return Err(PrismError::SettlementModeSelection {
                    reason: "duplicate fanout fee recipient".to_string(),
                });
            }
        }
    }

    for account in &mut policy_manifest.accounts {
        let key = account_key(
            &account.recipient_id,
            &account.order_key,
            &account.p2mr_program_hex,
        );
        if let Some(recipient) = recipients_by_key.get(&key) {
            if account.onchain_amount_sats != recipient.gross_amount_sats {
                return Err(PrismError::AuditMismatch {
                    artifact: "ctv_fanout_fee_accounting",
                });
            }
            account.settlement_fee_sats = recipient.fee_sats;
        }
    }
    Ok(recipients_by_key)
}

fn settlement_recipients_from_entitlements(
    entitlements: &[WeightedEntitlement],
) -> Result<Vec<SettlementRecipient>, PrismError> {
    entitlements
        .iter()
        .map(|entitlement| {
            Ok(SettlementRecipient {
                recipient_id: entitlement.recipient_id.clone(),
                order_key: entitlement.order_key.clone(),
                p2mr_program_hex: entitlement.p2mr_program_hex.clone(),
                amount_sats: u64::try_from(entitlement.weight)
                    .map_err(|_| PrismError::PayoutPolicyOverflow)?,
            })
        })
        .collect()
}

fn ctv_settlement_coinbase_entitlements(
    decision: &SettlementModeDecision,
    prepared_fanouts: &[PreparedCtvFanout],
) -> Result<Vec<WeightedEntitlement>, PrismError> {
    if prepared_fanouts.len() != decision.fanout_chunks.len() {
        return Err(PrismError::AuditMismatch {
            artifact: "ctv_fanout_chunk_count",
        });
    }
    let mut entitlements = decision
        .direct_recipients
        .iter()
        .map(|recipient| WeightedEntitlement {
            recipient_id: recipient.recipient_id.clone(),
            order_key: recipient.order_key.clone(),
            p2mr_program_hex: recipient.p2mr_program_hex.clone(),
            weight: u128::from(recipient.amount_sats),
        })
        .collect::<Vec<_>>();
    entitlements.extend(prepared_fanouts.iter().map(|fanout| WeightedEntitlement {
        recipient_id: fanout.covenant_recipient_id.clone(),
        order_key: fanout.covenant_order_key.clone(),
        p2mr_program_hex: fanout.covenant_p2mr_program_hex.clone(),
        weight: u128::from(fanout.covenant_output_value_sats),
    }));
    Ok(entitlements)
}

fn find_coinbase_output_vout(
    manifest: &PayoutManifest,
    recipient_id: &str,
    order_key: &str,
    p2mr_program_hex: &str,
    amount_sats: u64,
) -> Result<u32, PrismError> {
    let output = manifest
        .outputs
        .iter()
        .find(|output| {
            output.recipient_id == recipient_id
                && output.order_key == order_key
                && output.p2mr_program_hex == p2mr_program_hex
                && output.amount_sats == amount_sats
        })
        .ok_or(PrismError::AuditMismatch {
            artifact: "ctv_coinbase_output",
        })?;
    u32::try_from(output.vout).map_err(|_| PrismError::AuditMismatch {
        artifact: "ctv_coinbase_output_vout",
    })
}

fn verify_ctv_settlement_bundle(
    decision: &SettlementModeDecision,
    fanout_set: Option<&CtvFanoutManifestSet>,
    policy_manifest: &PayoutPolicyManifest,
    coinbase_manifest: &PayoutManifest,
    audit_commitment_leaves_hex: &[String],
) -> Result<Vec<WeightedEntitlement>, PrismError> {
    let mut covered_recipients = decision.direct_recipients.clone();
    let mut entitlements = decision
        .direct_recipients
        .iter()
        .map(|recipient| WeightedEntitlement {
            recipient_id: recipient.recipient_id.clone(),
            order_key: recipient.order_key.clone(),
            p2mr_program_hex: recipient.p2mr_program_hex.clone(),
            weight: u128::from(recipient.amount_sats),
        })
        .collect::<Vec<_>>();

    if decision.fanout_chunk_count == 0 {
        if fanout_set.is_some() {
            return Err(PrismError::AuditMismatch {
                artifact: "unexpected_ctv_fanout_manifest_set",
            });
        }
    } else {
        let fanout_set = fanout_set.ok_or(PrismError::AuditMismatch {
            artifact: "ctv_fanout_manifest_set",
        })?;
        verify_ctv_fanout_manifest_set(fanout_set)?;
        if fanout_set.parent_coinbase_txid != coinbase_manifest.coinbase_txid {
            return Err(PrismError::AuditMismatch {
                artifact: "ctv_fanout_parent_coinbase",
            });
        }
        if fanout_set.settlement_mode != decision.mode {
            return Err(PrismError::AuditMismatch {
                artifact: "ctv_fanout_settlement_mode",
            });
        }
        if fanout_set.fanout_count as usize != decision.fanout_chunk_count {
            return Err(PrismError::AuditMismatch {
                artifact: "ctv_fanout_chunk_count",
            });
        }
        for manifest in &fanout_set.manifests {
            if manifest.parent_coinbase_tx_hex != coinbase_manifest.coinbase_tx_hex {
                return Err(PrismError::AuditMismatch {
                    artifact: "ctv_fanout_parent_coinbase_tx",
                });
            }
            verify_ctv_fanout_manifest_commitment_leaf(manifest, audit_commitment_leaves_hex)?;
            let chunk_index = manifest.precommitment.chunk_index;
            entitlements.push(WeightedEntitlement {
                recipient_id: format!("ctv-fanout-{chunk_index}"),
                order_key: format!("ctv-fanout-{chunk_index:08}"),
                p2mr_program_hex: manifest.precommitment.p2mr_program_hex.clone(),
                weight: u128::from(manifest.covenant_output_value_sats),
            });
            covered_recipients.extend(manifest.precommitment.outputs.iter().map(|output| {
                SettlementRecipient {
                    recipient_id: output.recipient_id.clone(),
                    order_key: output.order_key.clone(),
                    p2mr_program_hex: output.p2mr_program_hex.clone(),
                    amount_sats: if output.gross_amount_sats == 0 {
                        output.amount_sats
                    } else {
                        output.gross_amount_sats
                    },
                }
            }));
        }
    }

    let mut expected =
        settlement_recipients_from_entitlements(&policy_manifest.onchain_entitlements)?;
    expected.sort_by(settlement_recipient_cmp);
    covered_recipients.sort_by(settlement_recipient_cmp);
    if covered_recipients != expected {
        return Err(PrismError::AuditMismatch {
            artifact: "ctv_settlement_coverage",
        });
    }
    Ok(entitlements)
}

fn settlement_recipient_cmp(
    left: &SettlementRecipient,
    right: &SettlementRecipient,
) -> std::cmp::Ordering {
    left.order_key
        .cmp(&right.order_key)
        .then_with(|| left.recipient_id.cmp(&right.recipient_id))
        .then_with(|| left.p2mr_program_hex.cmp(&right.p2mr_program_hex))
        .then_with(|| left.amount_sats.cmp(&right.amount_sats))
}

fn hex_vec_eq_ignore_ascii_case(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

pub fn verify_audit_bundle(
    bundle: &AuditBundle,
    ledger_writer_public_key_hex: &str,
) -> Result<AuditVerificationReport, PrismError> {
    if bundle.schema != audit_bundle_schema_for_shares(&bundle.shares) {
        return Err(PrismError::AuditMismatch { artifact: "schema" });
    }
    verify_ledger_window_attestation(bundle, ledger_writer_public_key_hex)?;
    verify_signed_manifest(&bundle.signed_coinbase_manifest)?;

    let expected_reward_manifest =
        build_prism_reward_manifest(&bundle.shares, &bundle.found_block)?;
    if expected_reward_manifest != bundle.reward_manifest {
        return Err(PrismError::AuditMismatch {
            artifact: "reward_manifest",
        });
    }

    let mut expected_policy_manifest = apply_payout_policy(
        &expected_reward_manifest,
        &bundle.prior_balances,
        &bundle.payout_policy,
    )?;
    if let (Some(decision), Some(fee_policy)) = (
        bundle.settlement_mode_decision.as_ref(),
        bundle.ctv_fanout_fee_policy.as_ref(),
    ) {
        apply_ctv_fanout_fee_accounting(&mut expected_policy_manifest, decision, fee_policy)?;
    }
    if expected_policy_manifest != bundle.payout_policy_manifest {
        return Err(PrismError::AuditMismatch {
            artifact: "payout_policy_manifest",
        });
    }
    let expected_audit_commitment_leaf =
        prism_audit_commitment_leaf_hex(&expected_reward_manifest, &expected_policy_manifest)?;
    let mut expected_audit_commitment_leaves = vec![expected_audit_commitment_leaf.clone()];

    let expected_coinbase_entitlements = if let Some(decision) = &bundle.settlement_mode_decision {
        let entitlements = verify_ctv_settlement_bundle(
            decision,
            bundle.ctv_fanout_manifest_set.as_ref(),
            &expected_policy_manifest,
            &bundle.signed_coinbase_manifest.manifest,
            &bundle.audit_commitment_leaves_hex,
        )?;
        if let Some(fanout_set) = &bundle.ctv_fanout_manifest_set {
            expected_audit_commitment_leaves.extend(
                fanout_set
                    .manifests
                    .iter()
                    .map(|manifest| manifest.commitment_witness_leaf_hex.clone()),
            );
        }
        entitlements
    } else {
        expected_policy_manifest.onchain_entitlements.clone()
    };
    if !hex_vec_eq_ignore_ascii_case(
        &bundle.audit_commitment_leaves_hex,
        &expected_audit_commitment_leaves,
    ) {
        return Err(PrismError::AuditMismatch {
            artifact: "audit_commitment_leaves",
        });
    }
    let expected_audit_commitment_root_hex =
        audit_commitment_root_hex(&expected_audit_commitment_leaves)?;
    if !bundle
        .audit_commitment_root_hex
        .as_deref()
        .is_some_and(|root| root.eq_ignore_ascii_case(&expected_audit_commitment_root_hex))
    {
        return Err(PrismError::AuditMismatch {
            artifact: "audit_commitment_root",
        });
    }
    let expected_coinbase_request = CoinbaseBuildRequest {
        block_height: expected_reward_manifest.block_height,
        coinbase_value_sats: expected_reward_manifest.coinbase_value_sats,
        entitlements: expected_coinbase_entitlements,
        witness_nonce_hex: Some(expected_audit_commitment_root_hex),
        witness_merkle_leaves_hex: bundle.witness_merkle_leaves_hex.clone(),
        coinbase_script_sig_suffix_hex: bundle.coinbase_script_sig_suffix_hex.clone(),
    };

    let expected_coinbase_manifest = build_manifest(expected_coinbase_request)?;
    if expected_coinbase_manifest != bundle.signed_coinbase_manifest.manifest {
        return Err(PrismError::AuditMismatch {
            artifact: "coinbase_manifest",
        });
    }

    audit_verification_report(bundle, &expected_coinbase_manifest)
}

pub fn verify_audit_bundle_with_ledger_public_key(
    bundle: &AuditBundle,
    writer_public_key_hex: &str,
) -> Result<AuditVerificationReport, PrismError> {
    verify_audit_bundle(bundle, writer_public_key_hex)
}

pub fn verify_audit_bundle_against_coinbase_tx_hex(
    bundle: &AuditBundle,
    onchain_coinbase_tx_hex: &str,
    ledger_writer_public_key_hex: &str,
) -> Result<AuditVerificationReport, PrismError> {
    let report = verify_audit_bundle(bundle, ledger_writer_public_key_hex)?;
    if !report
        .coinbase_tx_hex
        .eq_ignore_ascii_case(onchain_coinbase_tx_hex.trim())
    {
        return Err(PrismError::AuditCoinbaseTxMismatch);
    }
    Ok(report)
}

pub fn verify_audit_bundle_against_coinbase_tx_hex_and_expected_coinbase_value(
    bundle: &AuditBundle,
    onchain_coinbase_tx_hex: &str,
    ledger_writer_public_key_hex: &str,
    expected_coinbase_value_sats: u64,
) -> Result<AuditVerificationReport, PrismError> {
    let report = verify_audit_bundle_against_coinbase_tx_hex(
        bundle,
        onchain_coinbase_tx_hex,
        ledger_writer_public_key_hex,
    )?;
    if report.coinbase_value_sats != expected_coinbase_value_sats {
        return Err(PrismError::ExpectedCoinbaseValueMismatch {
            expected_coinbase_value_sats,
            actual_coinbase_value_sats: report.coinbase_value_sats,
        });
    }
    Ok(report)
}

fn audit_verification_report(
    bundle: &AuditBundle,
    coinbase_manifest: &PayoutManifest,
) -> Result<AuditVerificationReport, PrismError> {
    let reward_manifest_sha256_hex =
        sha256_hex(&canonical_reward_manifest_bytes(&bundle.reward_manifest)?);
    let payout_policy_manifest_sha256_hex = sha256_hex(&canonical_payout_policy_manifest_bytes(
        &bundle.payout_policy_manifest,
    )?);
    let prism_audit_commitment_leaf_hex =
        prism_audit_commitment_leaf_hex(&bundle.reward_manifest, &bundle.payout_policy_manifest)?;
    let audit_commitment_root_hex = audit_commitment_root_hex(&bundle.audit_commitment_leaves_hex)?;
    Ok(AuditVerificationReport {
        schema: "qbit.prism.audit-verification-report.v1".to_string(),
        block_height: bundle.found_block.block_height,
        coinbase_value_sats: bundle.found_block.coinbase_value_sats,
        reward_manifest_sha256_hex,
        payout_policy_manifest_sha256_hex,
        prism_audit_commitment_leaf_hex,
        audit_commitment_root_hex,
        coinbase_manifest_sha256_hex: sha256_hex(&serde_json::to_vec(coinbase_manifest)?),
        audit_bundle_sha256_hex: sha256_hex(&canonical_audit_bundle_bytes(bundle)?),
        coinbase_txid: coinbase_manifest.coinbase_txid.clone(),
        coinbase_wtxid: coinbase_manifest.coinbase_wtxid.clone(),
        coinbase_tx_hex: coinbase_manifest.coinbase_tx_hex.clone(),
        min_output_sats: bundle.payout_policy_manifest.min_output_sats,
        onchain_output_count: coinbase_manifest.outputs.len(),
        accrued_account_count: bundle
            .payout_policy_manifest
            .accounts
            .iter()
            .filter(|account| account.action == PayoutPolicyAction::Accrued)
            .count(),
    })
}

fn pool_fee_manifest(
    coinbase_value_sats: u64,
    policy: &PayoutPolicy,
) -> Result<Option<PoolFeeManifest>, PrismError> {
    let Some(pool_fee_policy) = &policy.pool_fee_policy else {
        return Ok(None);
    };
    if pool_fee_policy.fee_bps > 10_000 {
        return Err(PrismError::PoolFeeBpsTooHigh {
            fee_bps: pool_fee_policy.fee_bps,
        });
    }
    let fee_sats = u64::try_from(
        u128::from(coinbase_value_sats)
            .checked_mul(u128::from(pool_fee_policy.fee_bps))
            .ok_or(PrismError::PayoutPolicyOverflow)?
            / 10_000,
    )
    .map_err(|_| PrismError::PayoutPolicyOverflow)?;
    Ok(Some(PoolFeeManifest {
        fee_bps: pool_fee_policy.fee_bps,
        earned_pool_fee_sats: fee_sats,
        swept_dust_liability_sats: 0,
        amount_sats: fee_sats,
        recipient_id: pool_fee_policy.recipient_id.clone(),
        order_key: pool_fee_policy.order_key.clone(),
        p2mr_program_hex: pool_fee_policy.p2mr_program_hex.clone(),
    }))
}

fn is_default_account_type(account_type: &PayoutPolicyAccountType) -> bool {
    *account_type == PayoutPolicyAccountType::Miner
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn verify_ledger_window_attestation(
    bundle: &AuditBundle,
    writer_public_key_hex: &str,
) -> Result<(), PrismError> {
    let attestation = &bundle.ledger_window_attestation;
    if attestation.schema != "qbit.prism.ledger-window-attestation.v1" {
        return Err(PrismError::AuditMismatch {
            artifact: "ledger_attestation_schema",
        });
    }
    if attestation.anchor_job_issued_at_ms != bundle.found_block.anchor_job_issued_at_ms
        || attestation.network_difficulty != bundle.found_block.network_difficulty
        || attestation.block_height != bundle.found_block.block_height
        || attestation.coinbase_value_sats != bundle.found_block.coinbase_value_sats
    {
        return Err(PrismError::AuditMismatch {
            artifact: "ledger_attestation_scope",
        });
    }
    if attestation.share_slice_digest_hex != bundle.reward_manifest.share_slice_digest_hex {
        return Err(PrismError::AuditMismatch {
            artifact: "ledger_attestation_share_digest",
        });
    }
    if attestation.prior_balances_digest_hex != prior_balances_digest_hex(&bundle.prior_balances) {
        return Err(PrismError::AuditMismatch {
            artifact: "ledger_attestation_prior_digest",
        });
    }
    if !attestation
        .signature
        .public_key_hex
        .eq_ignore_ascii_case(writer_public_key_hex)
    {
        return Err(PrismError::AuditMismatch {
            artifact: "ledger_attestation_public_key",
        });
    }
    if attestation.signature.algorithm != "ed25519" {
        return Err(PrismError::AuditMismatch {
            artifact: "ledger_attestation_signature",
        });
    }
    verify_ed25519_message(
        &attestation.signature.public_key_hex,
        &canonical_ledger_window_attestation_bytes(attestation)?,
        &attestation.signature.signature_hex,
    )?;
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn share_slice_digest_hex(shares: &[CountedShare]) -> String {
    let mut hasher = Sha256::new();
    for share in shares {
        update_u64(&mut hasher, share.share_seq);
        update_string(&mut hasher, &share.share_id);
        update_string(&mut hasher, &share.miner_id);
        update_string(&mut hasher, &share.order_key);
        update_string(&mut hasher, &share.p2mr_program_hex);
        update_u128(&mut hasher, share.share_difficulty);
        update_u128(&mut hasher, share.counted_difficulty);
        update_i64(&mut hasher, share.job_issued_at_ms);
        update_i64(&mut hasher, share.accepted_at_ms);
        if let Some(credit_policy) = &share.credit_policy {
            update_string(&mut hasher, "credit_policy");
            update_string(&mut hasher, credit_policy);
        }
    }
    hex::encode(hasher.finalize())
}

fn prior_balances_digest_hex(balances: &[CarryForwardBalance]) -> String {
    let mut ordered = balances.to_vec();
    ordered.sort_by(|left, right| {
        left.order_key
            .cmp(&right.order_key)
            .then_with(|| left.recipient_id.cmp(&right.recipient_id))
            .then_with(|| left.p2mr_program_hex.cmp(&right.p2mr_program_hex))
    });

    let mut hasher = Sha256::new();
    for balance in ordered {
        update_string(&mut hasher, &balance.recipient_id);
        update_string(&mut hasher, &balance.order_key);
        update_string(&mut hasher, &balance.p2mr_program_hex);
        update_i128(&mut hasher, balance.balance_sats);
    }
    hex::encode(hasher.finalize())
}

fn update_string(hasher: &mut Sha256, value: &str) {
    update_u64(hasher, value.len() as u64);
    hasher.update(value.as_bytes());
}

fn update_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

fn update_i64(hasher: &mut Sha256, value: i64) {
    hasher.update(value.to_be_bytes());
}

fn update_i128(hasher: &mut Sha256, value: i128) {
    hasher.update(value.to_be_bytes());
}

fn update_u128(hasher: &mut Sha256, value: u128) {
    hasher.update(value.to_be_bytes());
}

fn aggregate_entitlements(
    counted_shares: &[CountedShare],
) -> Result<Vec<WeightedEntitlement>, PrismError> {
    let mut weights: BTreeMap<MinerKey, u128> = BTreeMap::new();
    for share in counted_shares {
        let key = MinerKey {
            miner_id: share.miner_id.clone(),
            order_key: share.order_key.clone(),
            p2mr_program_hex: share.p2mr_program_hex.clone(),
        };
        let existing = weights.entry(key).or_insert(0);
        *existing = existing
            .checked_add(share.counted_difficulty)
            .ok_or(PrismError::WindowOverflow)?;
    }

    Ok(weights
        .into_iter()
        .map(|(key, weight)| WeightedEntitlement {
            recipient_id: key.miner_id,
            order_key: key.order_key,
            p2mr_program_hex: key.p2mr_program_hex,
            weight,
        })
        .collect())
}

fn carry_forward_by_payout_program(
    balances: &[CarryForwardBalance],
) -> Result<BTreeMap<String, CarryForwardBalance>, PrismError> {
    let mut by_key = BTreeMap::new();
    for balance in balances {
        let key = payout_program_key(&balance.p2mr_program_hex);
        let entry = by_key.entry(key).or_insert_with(|| CarryForwardBalance {
            recipient_id: balance.recipient_id.clone(),
            order_key: balance.order_key.clone(),
            p2mr_program_hex: balance.p2mr_program_hex.clone(),
            balance_sats: 0,
        });
        if (balance.order_key.as_str(), balance.recipient_id.as_str())
            < (entry.order_key.as_str(), entry.recipient_id.as_str())
        {
            entry.recipient_id = balance.recipient_id.clone();
            entry.order_key = balance.order_key.clone();
        }
        entry.balance_sats = entry
            .balance_sats
            .checked_add(balance.balance_sats)
            .ok_or(PrismError::PayoutPolicyOverflow)?;
    }
    Ok(by_key)
}

fn aggregate_entitlements_by_payout_program(
    entitlements: &[WeightedEntitlement],
) -> Result<Vec<WeightedEntitlement>, PrismError> {
    let mut by_program: BTreeMap<String, WeightedEntitlement> = BTreeMap::new();
    for entitlement in entitlements {
        let key = payout_program_key(&entitlement.p2mr_program_hex);
        let entry = by_program
            .entry(key)
            .or_insert_with(|| WeightedEntitlement {
                recipient_id: entitlement.recipient_id.clone(),
                order_key: entitlement.order_key.clone(),
                p2mr_program_hex: entitlement.p2mr_program_hex.clone(),
                weight: 0,
            });
        if (
            entitlement.order_key.as_str(),
            entitlement.recipient_id.as_str(),
        ) < (entry.order_key.as_str(), entry.recipient_id.as_str())
        {
            entry.recipient_id = entitlement.recipient_id.clone();
            entry.order_key = entitlement.order_key.clone();
        }
        entry.weight = entry
            .weight
            .checked_add(entitlement.weight)
            .ok_or(PrismError::PayoutPolicyOverflow)?;
    }
    Ok(by_program.into_values().collect())
}

fn add_swept_dust_to_pool_fee(
    pool_fee: &mut Option<PoolFeeManifest>,
    swept_dust_sats: u64,
) -> Result<(), PrismError> {
    if swept_dust_sats == 0 {
        return Ok(());
    }
    let pool_fee = pool_fee.as_mut().ok_or(PrismError::PayoutPolicyOverflow)?;
    pool_fee.swept_dust_liability_sats = pool_fee
        .swept_dust_liability_sats
        .checked_add(swept_dust_sats)
        .ok_or(PrismError::PayoutPolicyOverflow)?;
    pool_fee.amount_sats = pool_fee
        .earned_pool_fee_sats
        .checked_add(pool_fee.swept_dust_liability_sats)
        .ok_or(PrismError::PayoutPolicyOverflow)?;
    Ok(())
}

fn eligible_onchain_accounts(accounts: &[PayoutPolicyAccount], min_output_sats: u64) -> Vec<usize> {
    accounts
        .iter()
        .enumerate()
        .filter_map(|(index, account)| {
            (account.candidate_balance_sats >= i128::from(min_output_sats)).then_some(index)
        })
        .collect()
}

fn selected_candidate_balance_sum(
    accounts: &[PayoutPolicyAccount],
    selected: &[usize],
) -> Result<u128, PrismError> {
    selected.iter().try_fold(0_u128, |sum, index| {
        let candidate_balance = u128::try_from(accounts[*index].candidate_balance_sats)
            .map_err(|_| PrismError::PayoutPolicyOverflow)?;
        sum.checked_add(candidate_balance)
            .ok_or(PrismError::PayoutPolicyOverflow)
    })
}

fn select_onchain_accounts(
    accounts: &[PayoutPolicyAccount],
    coinbase_value_sats: u64,
    min_output_sats: u64,
) -> Result<Vec<usize>, PrismError> {
    let mut selected = eligible_onchain_accounts(accounts, min_output_sats);
    loop {
        if selected.is_empty() {
            return Err(PrismError::NoOnchainRecipients);
        }
        let selected_candidate_balance_sats = selected_candidate_balance_sum(accounts, &selected)?;
        if selected_candidate_balance_sats < u128::from(coinbase_value_sats) {
            return Err(PrismError::PayoutExceedsCandidateBalance {
                coinbase_value_sats,
                selected_candidate_balance_sats,
            });
        }
        let weights = selected
            .iter()
            .map(|index| WeightedEntitlement {
                recipient_id: accounts[*index].recipient_id.clone(),
                order_key: accounts[*index].order_key.clone(),
                p2mr_program_hex: accounts[*index].p2mr_program_hex.clone(),
                weight: accounts[*index].candidate_balance_sats as u128,
            })
            .collect::<Vec<_>>();
        let allocations = allocate_weighted_amounts(coinbase_value_sats, &weights)?;
        let under_floor = allocations
            .iter()
            .filter(|(_entitlement, amount_sats)| *amount_sats < min_output_sats)
            .map(|(entitlement, _amount_sats)| {
                account_key(
                    &entitlement.recipient_id,
                    &entitlement.order_key,
                    &entitlement.p2mr_program_hex,
                )
            })
            .collect::<Vec<_>>();
        if under_floor.is_empty() {
            return Ok(selected);
        }
        selected.retain(|index| {
            let key = account_key(
                &accounts[*index].recipient_id,
                &accounts[*index].order_key,
                &accounts[*index].p2mr_program_hex,
            );
            !under_floor.contains(&key)
        });
    }
}

fn allocate_weighted_amounts(
    total_sats: u64,
    entitlements: &[WeightedEntitlement],
) -> Result<Vec<(WeightedEntitlement, u64)>, PrismError> {
    let mut ordered = entitlements.to_vec();
    ordered.sort_by(|left, right| {
        left.order_key
            .cmp(&right.order_key)
            .then_with(|| left.recipient_id.cmp(&right.recipient_id))
            .then_with(|| left.p2mr_program_hex.cmp(&right.p2mr_program_hex))
    });
    let total_weight = ordered.iter().try_fold(0_u128, |sum, entitlement| {
        sum.checked_add(entitlement.weight)
            .ok_or(PrismError::PayoutPolicyOverflow)
    })?;
    if total_weight == 0 {
        return Err(PrismError::PayoutPolicyOverflow);
    }

    let mut provisional = ordered
        .into_iter()
        .map(|entitlement| {
            let product = u128::from(total_sats)
                .checked_mul(entitlement.weight)
                .ok_or(PrismError::PayoutPolicyOverflow)?;
            let base = product / total_weight;
            let remainder = product % total_weight;
            let amount = u64::try_from(base).map_err(|_| PrismError::PayoutPolicyOverflow)?;
            Ok((entitlement, amount, remainder))
        })
        .collect::<Result<Vec<_>, PrismError>>()?;

    let allocated =
        provisional
            .iter()
            .try_fold(0_u64, |sum, (_entitlement, amount, _remainder)| {
                sum.checked_add(*amount)
                    .ok_or(PrismError::PayoutPolicyOverflow)
            })?;
    let remainder_sats = total_sats
        .checked_sub(allocated)
        .ok_or(PrismError::PayoutPolicyOverflow)?;
    let mut remainder_order = (0..provisional.len()).collect::<Vec<_>>();
    remainder_order.sort_by(|left, right| {
        provisional[*right]
            .2
            .cmp(&provisional[*left].2)
            .then_with(|| {
                provisional[*left]
                    .0
                    .order_key
                    .cmp(&provisional[*right].0.order_key)
            })
            .then_with(|| {
                provisional[*left]
                    .0
                    .recipient_id
                    .cmp(&provisional[*right].0.recipient_id)
            })
            .then_with(|| {
                provisional[*left]
                    .0
                    .p2mr_program_hex
                    .cmp(&provisional[*right].0.p2mr_program_hex)
            })
    });
    for index in remainder_order.into_iter().take(remainder_sats as usize) {
        provisional[index].1 = provisional[index]
            .1
            .checked_add(1)
            .ok_or(PrismError::PayoutPolicyOverflow)?;
    }

    Ok(provisional
        .into_iter()
        .map(|(entitlement, amount, _remainder)| (entitlement, amount))
        .collect())
}

fn account_key(recipient_id: &str, order_key: &str, p2mr_program_hex: &str) -> String {
    format!("{recipient_id}\0{order_key}\0{p2mr_program_hex}")
}

fn payout_program_key(p2mr_program_hex: &str) -> String {
    p2mr_program_hex.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use qbit_pool_builder::{build_manifest, BuilderError, ManifestSigningKey};
    use serde::Deserialize;

    fn p2mr_program(byte: u8) -> String {
        hex::encode([byte; 32])
    }

    fn array32_from_hex(raw: &str) -> [u8; 32] {
        let decoded = hex::decode(raw).unwrap();
        decoded.try_into().unwrap()
    }

    fn consensus_witness_commitment_script_hex(
        witness_merkle_leaves_hex: &[String],
        witness_nonce_hex: &str,
    ) -> String {
        let mut leaves = Vec::with_capacity(witness_merkle_leaves_hex.len() + 1);
        leaves.push([0_u8; 32]);
        leaves.extend(
            witness_merkle_leaves_hex
                .iter()
                .map(|leaf| array32_from_hex(leaf)),
        );
        let witness_root = merkle_root(leaves);
        let witness_nonce = array32_from_hex(witness_nonce_hex);
        let commitment = hash256_pair(&witness_root, &witness_nonce);
        format!("6a24aa21a9ed{}", hex::encode(commitment))
    }

    fn share(
        share_seq: u64,
        miner_id: &str,
        order_key: &str,
        p2mr_byte: u8,
        share_difficulty: u128,
        job_issued_at_ms: i64,
    ) -> AcceptedShare {
        AcceptedShare {
            share_seq,
            share_id: format!("share-{share_seq}"),
            miner_id: miner_id.to_string(),
            order_key: order_key.to_string(),
            p2mr_program_hex: p2mr_program(p2mr_byte),
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

    fn found_block(network_difficulty: u128, anchor_job_issued_at_ms: i64) -> FoundBlock {
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty,
            anchor_job_issued_at_ms,
        }
    }

    #[derive(Debug, Deserialize)]
    struct Fixture {
        found_block: FoundBlock,
        shares: Vec<AcceptedShare>,
        expected_counted_window_weight: u128,
        expected_shares: Vec<ExpectedShare>,
        expected_entitlements: Vec<ExpectedEntitlement>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct ExpectedShare {
        share_seq: u64,
        counted_difficulty: u128,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct ExpectedEntitlement {
        recipient_id: String,
        weight: u128,
    }

    fn load_fixture(raw: &str) -> Fixture {
        serde_json::from_str(raw).unwrap()
    }

    fn assert_fixture(raw: &str) {
        let fixture = load_fixture(raw);
        let window = compute_prism_window(&fixture.shares, &fixture.found_block).unwrap();
        assert_eq!(
            window.counted_window_weight,
            fixture.expected_counted_window_weight
        );
        assert_eq!(
            window
                .shares
                .iter()
                .map(|share| ExpectedShare {
                    share_seq: share.share_seq,
                    counted_difficulty: share.counted_difficulty,
                })
                .collect::<Vec<_>>(),
            fixture.expected_shares
        );
        assert_eq!(
            window
                .entitlements
                .iter()
                .map(|entitlement| ExpectedEntitlement {
                    recipient_id: entitlement.recipient_id.clone(),
                    weight: entitlement.weight,
                })
                .collect::<Vec<_>>(),
            fixture.expected_entitlements
        );
    }

    #[test]
    fn credit_policy_is_preserved_on_counted_shares() {
        let mut shares = vec![share(1, "miner-a", "01", 0x11, 10, 1000)];
        shares[0].credit_policy = Some("stale-grace".to_string());

        let window = compute_prism_window(&shares, &found_block(10, 1000)).unwrap();

        assert_eq!(
            window.shares[0].credit_policy.as_deref(),
            Some("stale-grace")
        );
    }

    #[test]
    fn stale_grace_shares_use_upgraded_audit_bundle_schema() {
        let mut shares = vec![share(1, "miner-a", "01", 0x11, 10, 1000)];
        shares[0].credit_policy = Some("stale-grace".to_string());
        let bundle = build_audit_bundle(
            shares,
            found_block(10, 1000),
            vec![],
            PayoutPolicy::day_one_default(),
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap();

        assert_eq!(bundle.schema, AUDIT_BUNDLE_SCHEMA_V1_1);
        verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();

        let mut mislabeled = bundle.clone();
        mislabeled.schema = AUDIT_BUNDLE_SCHEMA_V1.to_string();
        assert!(matches!(
            verify_audit_bundle(&mislabeled, &ledger_public_key_hex()),
            Err(PrismError::AuditMismatch { artifact: "schema" })
        ));
    }

    #[derive(Debug, Deserialize)]
    struct PayoutPolicyFixture {
        found_block: FoundBlock,
        shares: Vec<AcceptedShare>,
        expected_min_output_sats: u64,
        expected_onchain_count: usize,
        expected_accrued_recipients: Vec<String>,
    }

    fn load_policy_fixture(raw: &str) -> PayoutPolicyFixture {
        serde_json::from_str(raw).unwrap()
    }

    fn manifest_signing_key() -> ManifestSigningKey {
        ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap()
    }

    fn ledger_signing_key() -> ManifestSigningKey {
        ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap()
    }

    fn ledger_public_key_hex() -> String {
        ledger_signing_key().public_key_hex()
    }

    fn power_law_prior_balances() -> Vec<CarryForwardBalance> {
        vec![CarryForwardBalance {
            recipient_id: "miner-whale".to_string(),
            order_key: "01".to_string(),
            p2mr_program_hex: "11".repeat(32),
            balance_sats: 4_800,
        }]
    }

    fn power_law_audit_bundle() -> AuditBundle {
        let fixture = load_policy_fixture(include_str!(
            "../fixtures/power-law-accrual.prism-fixture.json"
        ));
        build_audit_bundle(
            fixture.shares,
            fixture.found_block,
            power_law_prior_balances(),
            PayoutPolicy::day_one_default(),
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap()
    }

    #[test]
    fn small_log_bootstrap_uses_full_eligible_log() {
        let shares = vec![
            share(1, "miner-a", "01", 1, 10, 1000),
            share(2, "miner-b", "02", 2, 20, 1001),
            share(3, "miner-a", "01", 1, 30, 1002),
        ];
        let window = compute_prism_window(&shares, &found_block(100, 1002)).unwrap();

        assert_eq!(window.requested_window_weight, 800);
        assert_eq!(window.counted_window_weight, 60);
        assert_eq!(
            window
                .shares
                .iter()
                .map(|share| (share.share_seq, share.counted_difficulty))
                .collect::<Vec<_>>(),
            vec![(3, 30), (2, 20), (1, 10)]
        );
        assert_eq!(
            window
                .entitlements
                .iter()
                .map(|entitlement| (entitlement.recipient_id.as_str(), entitlement.weight))
                .collect::<Vec<_>>(),
            vec![("miner-a", 40), ("miner-b", 20)]
        );
    }

    #[test]
    fn window_is_newest_backward_and_partially_counts_oldest_boundary_share() {
        let shares = vec![
            share(1, "miner-old", "01", 1, 999, 1000),
            share(2, "miner-b", "02", 2, 40, 1001),
            share(3, "miner-a", "03", 3, 30, 1002),
            share(4, "miner-c", "04", 4, 50, 1003),
        ];
        let window = compute_prism_window(&shares, &found_block(10, 1003)).unwrap();

        assert_eq!(window.requested_window_weight, 80);
        assert_eq!(window.counted_window_weight, 80);
        assert_eq!(
            window
                .shares
                .iter()
                .map(|share| (share.share_seq, share.counted_difficulty))
                .collect::<Vec<_>>(),
            vec![(4, 50), (3, 30)]
        );
        assert_eq!(
            window
                .entitlements
                .iter()
                .map(|entitlement| (entitlement.recipient_id.as_str(), entitlement.weight))
                .collect::<Vec<_>>(),
            vec![("miner-a", 30), ("miner-c", 50)]
        );
    }

    #[test]
    fn difficulty_change_uses_found_block_network_difficulty_for_window_width() {
        let shares = vec![
            share(1, "miner-a", "01", 1, 10, 1000),
            share(2, "miner-b", "02", 2, 20, 1001),
            share(3, "miner-a", "01", 1, 30, 1002),
            share(4, "miner-c", "03", 3, 50, 1003),
        ];

        let low_diff = compute_prism_window(&shares, &found_block(10, 1003)).unwrap();
        let high_diff = compute_prism_window(&shares, &found_block(20, 1003)).unwrap();

        assert_eq!(low_diff.counted_window_weight, 80);
        assert_eq!(
            low_diff
                .entitlements
                .iter()
                .map(|entitlement| (entitlement.recipient_id.as_str(), entitlement.weight))
                .collect::<Vec<_>>(),
            vec![("miner-a", 30), ("miner-c", 50)]
        );
        assert_eq!(high_diff.counted_window_weight, 110);
        assert_eq!(
            high_diff
                .entitlements
                .iter()
                .map(|entitlement| (entitlement.recipient_id.as_str(), entitlement.weight))
                .collect::<Vec<_>>(),
            vec![("miner-a", 40), ("miner-b", 20), ("miner-c", 50)]
        );
    }

    #[test]
    fn anchor_requires_job_issue_and_accept_time() {
        let mut after_anchor = share(3, "miner-late", "03", 3, 1000, 2000);
        after_anchor.accepted_at_ms = 1500;
        let shares = vec![
            share(1, "miner-a", "01", 1, 10, 1000),
            share(2, "miner-b", "02", 2, 20, 1001),
            after_anchor,
        ];
        let window = compute_prism_window(&shares, &found_block(10, 1001)).unwrap();

        assert_eq!(
            window
                .shares
                .iter()
                .map(|share| share.share_seq)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
    }

    #[test]
    fn prism_entitlements_feed_builder_exactly() {
        let shares = vec![
            share(1, "miner-a", "01", 1, 1, 1000),
            share(2, "miner-b", "02", 2, 1, 1001),
            share(3, "miner-c", "03", 3, 1, 1002),
        ];
        let (request, window) =
            build_prism_coinbase_request(&shares, &found_block(10, 1002)).unwrap();
        let manifest = build_manifest(request).unwrap();

        assert_eq!(window.counted_window_weight, 3);
        assert_eq!(
            manifest
                .outputs
                .iter()
                .map(|output| (output.recipient_id.as_str(), output.amount_sats))
                .collect::<Vec<_>>(),
            vec![
                ("miner-a", 166_666_667),
                ("miner-b", 166_666_667),
                ("miner-c", 166_666_666),
            ]
        );
        assert_eq!(
            manifest
                .outputs
                .iter()
                .map(|output| output.amount_sats)
                .sum::<u64>(),
            500_000_000
        );
    }

    #[test]
    fn checked_in_hand_worked_fixtures_match_engine() {
        assert_fixture(include_str!(
            "../fixtures/bootstrap-small-log.prism-fixture.json"
        ));
        assert_fixture(include_str!(
            "../fixtures/difficulty-change.prism-fixture.json"
        ));
    }

    #[test]
    fn reward_manifest_is_reproducible_and_contains_window_metadata() {
        let shares = vec![
            share(1, "miner-a", "01", 1, 10, 1000),
            share(2, "miner-b", "02", 2, 20, 1001),
            share(3, "miner-a", "01", 1, 30, 1002),
            share(4, "miner-c", "03", 3, 50, 1003),
        ];
        let found_block = found_block(10, 1003);

        let left = build_prism_reward_manifest(&shares, &found_block).unwrap();
        let right = build_prism_reward_manifest(&shares, &found_block).unwrap();

        assert_eq!(left, right);
        assert_eq!(
            canonical_reward_manifest_bytes(&left).unwrap(),
            canonical_reward_manifest_bytes(&right).unwrap()
        );
        assert_eq!(left.schema, "qbit.prism.reward-manifest.v1");
        assert_eq!(left.anchor_share_seq, 4);
        assert_eq!(left.newest_share_seq, 4);
        assert_eq!(left.oldest_share_seq, 3);
        assert_eq!(left.included_share_count, 2);
        assert_eq!(left.counted_window_weight, 80);
        assert_eq!(left.share_slice_digest_hex.len(), 64);
    }

    #[test]
    fn day_one_floor_formula_matches_pq_spend_cost() {
        let policy = PayoutPolicy::day_one_default();
        assert_eq!(policy.min_output_sats().unwrap(), 14_720);
        assert_eq!(
            policy.floor_formula(),
            "3680 bytes/input * 1 sat/byte * 4x safety"
        );
    }

    #[test]
    fn fixed_floor_override_replaces_formula_floor() {
        let mut policy = PayoutPolicy::day_one_default();
        policy.min_output_sats = Some(10_000);

        assert_eq!(policy.min_output_sats().unwrap(), 10_000);
        assert_eq!(policy.floor_formula(), "configured fixed floor: 10000 sats");
    }

    #[test]
    fn fixed_floor_override_must_be_positive() {
        let mut policy = PayoutPolicy::day_one_default();
        policy.min_output_sats = Some(0);

        assert!(matches!(
            policy.min_output_sats().unwrap_err(),
            PrismError::InvalidMinOutputFloor
        ));
    }

    #[test]
    fn skewed_distribution_exercises_floor_accrual_and_exact_builder_handoff() {
        let fixture = load_policy_fixture(include_str!(
            "../fixtures/power-law-accrual.prism-fixture.json"
        ));
        let reward_manifest =
            build_prism_reward_manifest(&fixture.shares, &fixture.found_block).unwrap();
        let err =
            build_policy_coinbase_request(&reward_manifest, &[], &PayoutPolicy::day_one_default())
                .unwrap_err();
        assert!(matches!(
            err,
            PrismError::PayoutExceedsCandidateBalance {
                coinbase_value_sats: 500_000_000,
                selected_candidate_balance_sats: 499_995_200
            }
        ));

        let (request, policy_manifest) = build_policy_coinbase_request(
            &reward_manifest,
            &power_law_prior_balances(),
            &PayoutPolicy::day_one_default(),
        )
        .unwrap();
        let coinbase_manifest = build_manifest(request).unwrap();

        assert_eq!(
            policy_manifest.min_output_sats,
            fixture.expected_min_output_sats
        );
        assert_eq!(
            policy_manifest.onchain_entitlements.len(),
            fixture.expected_onchain_count
        );
        assert_eq!(
            policy_manifest
                .accounts
                .iter()
                .filter(|account| account.action == PayoutPolicyAction::Accrued)
                .map(|account| account.recipient_id.clone())
                .collect::<Vec<_>>(),
            fixture.expected_accrued_recipients
        );
        assert!(policy_manifest
            .accounts
            .iter()
            .filter(|account| account.action == PayoutPolicyAction::Accrued)
            .all(|account| account.carry_forward_balance_sats > 0));
        assert!(coinbase_manifest
            .outputs
            .iter()
            .all(|output| output.amount_sats >= fixture.expected_min_output_sats));
        assert_eq!(
            coinbase_manifest
                .outputs
                .iter()
                .map(|output| output.amount_sats)
                .sum::<u64>(),
            fixture.found_block.coinbase_value_sats
        );
    }

    #[test]
    fn prior_accrual_can_cross_floor_and_be_paid_onchain() {
        let reward_manifest = PrismRewardManifest {
            schema: "qbit.prism.reward-manifest.v1".to_string(),
            block_height: 10,
            coinbase_value_sats: 100_000,
            network_difficulty: 1,
            window_multiplier: PRISM_WINDOW_MULTIPLIER,
            requested_window_weight: 8,
            counted_window_weight: 2,
            anchor_job_issued_at_ms: 1,
            anchor_share_seq: 2,
            newest_share_seq: 2,
            oldest_share_seq: 1,
            included_share_count: 2,
            share_slice_digest_hex: "00".repeat(32),
            shares: Vec::new(),
            entitlements: vec![
                WeightedEntitlement {
                    recipient_id: "large".to_string(),
                    order_key: "01".to_string(),
                    p2mr_program_hex: p2mr_program(1),
                    weight: 95,
                },
                WeightedEntitlement {
                    recipient_id: "small".to_string(),
                    order_key: "02".to_string(),
                    p2mr_program_hex: p2mr_program(2),
                    weight: 5,
                },
            ],
        };
        let prior = vec![CarryForwardBalance {
            recipient_id: "small".to_string(),
            order_key: "02".to_string(),
            p2mr_program_hex: p2mr_program(2),
            balance_sats: 12_000,
        }];
        let manifest =
            apply_payout_policy(&reward_manifest, &prior, &PayoutPolicy::day_one_default())
                .unwrap();
        let small = manifest
            .accounts
            .iter()
            .find(|account| account.recipient_id == "small")
            .unwrap();
        assert_eq!(small.action, PayoutPolicyAction::Onchain);
        assert!(small.onchain_amount_sats >= manifest.min_output_sats);
    }

    #[test]
    fn prior_only_account_with_zero_gross_can_be_paid_onchain() {
        let policy = PayoutPolicy::day_one_default();
        let reward_manifest = PrismRewardManifest {
            schema: "qbit.prism.reward-manifest.v1".to_string(),
            block_height: 10,
            coinbase_value_sats: 20_000,
            network_difficulty: 1,
            window_multiplier: PRISM_WINDOW_MULTIPLIER,
            requested_window_weight: 8,
            counted_window_weight: 2,
            anchor_job_issued_at_ms: 1,
            anchor_share_seq: 2,
            newest_share_seq: 2,
            oldest_share_seq: 1,
            included_share_count: 2,
            share_slice_digest_hex: "00".repeat(32),
            shares: Vec::new(),
            entitlements: vec![
                WeightedEntitlement {
                    recipient_id: "current-a".to_string(),
                    order_key: "01".to_string(),
                    p2mr_program_hex: p2mr_program(1),
                    weight: 1,
                },
                WeightedEntitlement {
                    recipient_id: "current-b".to_string(),
                    order_key: "02".to_string(),
                    p2mr_program_hex: p2mr_program(2),
                    weight: 1,
                },
            ],
        };
        let prior = vec![CarryForwardBalance {
            recipient_id: "prior-only".to_string(),
            order_key: "03".to_string(),
            p2mr_program_hex: p2mr_program(3),
            balance_sats: 20_000,
        }];

        let manifest = apply_payout_policy(&reward_manifest, &prior, &policy).unwrap();
        let prior_only = manifest
            .accounts
            .iter()
            .find(|account| account.recipient_id == "prior-only")
            .unwrap();

        assert_eq!(prior_only.gross_amount_sats, 0);
        assert_eq!(prior_only.prior_balance_sats, 20_000);
        assert_eq!(prior_only.candidate_balance_sats, 20_000);
        assert_eq!(prior_only.action, PayoutPolicyAction::Onchain);
        assert_eq!(prior_only.onchain_amount_sats, 20_000);
        assert_eq!(prior_only.carry_forward_balance_sats, 0);
        assert_eq!(
            manifest
                .onchain_entitlements
                .iter()
                .map(|entitlement| entitlement.weight as u64)
                .sum::<u64>(),
            reward_manifest.coinbase_value_sats
        );
    }

    #[test]
    fn payout_policy_aggregates_same_payout_program_before_floor_selection() {
        let program = p2mr_program(7);
        let mut policy = PayoutPolicy::day_one_default();
        policy.min_output_sats = Some(14_000);
        let reward_manifest = PrismRewardManifest {
            schema: "qbit.prism.reward-manifest.v1".to_string(),
            block_height: 10,
            coinbase_value_sats: 20_000,
            network_difficulty: 1,
            window_multiplier: PRISM_WINDOW_MULTIPLIER,
            requested_window_weight: 8,
            counted_window_weight: 2,
            anchor_job_issued_at_ms: 1,
            anchor_share_seq: 2,
            newest_share_seq: 2,
            oldest_share_seq: 1,
            included_share_count: 2,
            share_slice_digest_hex: "00".repeat(32),
            shares: Vec::new(),
            entitlements: vec![
                WeightedEntitlement {
                    recipient_id: "tq1same.bob".to_string(),
                    order_key: "02".to_string(),
                    p2mr_program_hex: program.clone(),
                    weight: 1,
                },
                WeightedEntitlement {
                    recipient_id: "tq1same.alice".to_string(),
                    order_key: "01".to_string(),
                    p2mr_program_hex: program.clone(),
                    weight: 1,
                },
            ],
        };

        let manifest = apply_payout_policy(&reward_manifest, &[], &policy).unwrap();

        assert_eq!(manifest.accounts.len(), 1);
        let account = &manifest.accounts[0];
        assert_eq!(account.recipient_id, "tq1same.alice");
        assert_eq!(account.order_key, "01");
        assert_eq!(account.p2mr_program_hex, program);
        assert_eq!(account.gross_amount_sats, 20_000);
        assert_eq!(account.action, PayoutPolicyAction::Onchain);
        assert_eq!(account.onchain_amount_sats, 20_000);
        assert_eq!(manifest.onchain_entitlements.len(), 1);
    }

    #[test]
    fn carry_forward_replay_aggregates_same_payout_program_aliases() {
        let program = p2mr_program(7);
        let entries = vec![
            PayoutMaturityEntry {
                block_hash: "block-a".to_string(),
                block_height: 10,
                account_type: PayoutPolicyAccountType::Miner,
                recipient_id: "tq1same.bob".to_string(),
                order_key: "02".to_string(),
                p2mr_program_hex: program.clone(),
                gross_amount_sats: 8_000,
                prior_balance_sats: 0,
                candidate_balance_sats: 8_000,
                onchain_amount_sats: 0,
                settlement_fee_sats: 0,
                carry_forward_balance_sats: 8_000,
                action: PayoutPolicyAction::Accrued,
                state: PayoutMaturityState::Immature,
            },
            PayoutMaturityEntry {
                block_hash: "block-b".to_string(),
                block_height: 11,
                account_type: PayoutPolicyAccountType::Miner,
                recipient_id: "tq1same.alice".to_string(),
                order_key: "01".to_string(),
                p2mr_program_hex: program.clone(),
                gross_amount_sats: 7_000,
                prior_balance_sats: 0,
                candidate_balance_sats: 7_000,
                onchain_amount_sats: 0,
                settlement_fee_sats: 0,
                carry_forward_balance_sats: 7_000,
                action: PayoutPolicyAction::Accrued,
                state: PayoutMaturityState::Immature,
            },
        ];

        let balances = current_carry_forward_balances(&entries);

        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].recipient_id, "tq1same.alice");
        assert_eq!(balances[0].order_key, "01");
        assert_eq!(balances[0].p2mr_program_hex, program);
        assert_eq!(balances[0].balance_sats, 15_000);
    }

    #[test]
    fn carry_forward_replay_preserves_gross_settlement_when_fee_is_disclosed() {
        let entries = vec![PayoutMaturityEntry {
            block_hash: "block-fee".to_string(),
            block_height: 10,
            account_type: PayoutPolicyAccountType::Miner,
            recipient_id: "miner-a".to_string(),
            order_key: "01".to_string(),
            p2mr_program_hex: p2mr_program(1),
            gross_amount_sats: 20_000,
            prior_balance_sats: 0,
            candidate_balance_sats: 20_000,
            onchain_amount_sats: 20_000,
            settlement_fee_sats: 500,
            carry_forward_balance_sats: 0,
            action: PayoutPolicyAction::Onchain,
            state: PayoutMaturityState::Immature,
        }];

        assert!(current_carry_forward_balances(&entries).is_empty());
    }

    #[test]
    fn no_pool_fee_policy_preserves_zero_fee_manifest_and_outputs() {
        let reward_manifest = PrismRewardManifest {
            schema: "qbit.prism.reward-manifest.v1".to_string(),
            block_height: 10,
            coinbase_value_sats: 100_003,
            network_difficulty: 1,
            window_multiplier: PRISM_WINDOW_MULTIPLIER,
            requested_window_weight: 8,
            counted_window_weight: 3,
            anchor_job_issued_at_ms: 1,
            anchor_share_seq: 2,
            newest_share_seq: 2,
            oldest_share_seq: 1,
            included_share_count: 2,
            share_slice_digest_hex: "00".repeat(32),
            shares: Vec::new(),
            entitlements: vec![
                WeightedEntitlement {
                    recipient_id: "miner-a".to_string(),
                    order_key: "01".to_string(),
                    p2mr_program_hex: p2mr_program(1),
                    weight: 1,
                },
                WeightedEntitlement {
                    recipient_id: "miner-b".to_string(),
                    order_key: "02".to_string(),
                    p2mr_program_hex: p2mr_program(2),
                    weight: 2,
                },
            ],
        };

        let (request, policy_manifest) =
            build_policy_coinbase_request(&reward_manifest, &[], &PayoutPolicy::day_one_default())
                .unwrap();
        let coinbase_manifest = build_manifest(request).unwrap();

        assert!(policy_manifest.pool_fee.is_none());
        assert!(policy_manifest
            .accounts
            .iter()
            .all(|account| account.account_type == PayoutPolicyAccountType::Miner));
        assert_eq!(
            coinbase_manifest
                .outputs
                .iter()
                .map(|output| output.amount_sats)
                .sum::<u64>(),
            100_003
        );
        assert!(coinbase_manifest
            .outputs
            .iter()
            .all(|output| output.recipient_id != "pool-fee"));
    }

    #[test]
    fn zero_bps_pool_fee_is_audited_without_onchain_fee_output() {
        let reward_manifest = PrismRewardManifest {
            schema: "qbit.prism.reward-manifest.v1".to_string(),
            block_height: 10,
            coinbase_value_sats: 100_003,
            network_difficulty: 1,
            window_multiplier: PRISM_WINDOW_MULTIPLIER,
            requested_window_weight: 8,
            counted_window_weight: 3,
            anchor_job_issued_at_ms: 1,
            anchor_share_seq: 2,
            newest_share_seq: 2,
            oldest_share_seq: 1,
            included_share_count: 2,
            share_slice_digest_hex: "00".repeat(32),
            shares: Vec::new(),
            entitlements: vec![
                WeightedEntitlement {
                    recipient_id: "miner-a".to_string(),
                    order_key: "01".to_string(),
                    p2mr_program_hex: p2mr_program(1),
                    weight: 1,
                },
                WeightedEntitlement {
                    recipient_id: "miner-b".to_string(),
                    order_key: "02".to_string(),
                    p2mr_program_hex: p2mr_program(2),
                    weight: 2,
                },
            ],
        };
        let mut policy = PayoutPolicy::day_one_default();
        policy.pool_fee_policy = Some(PoolFeePolicy {
            fee_bps: 0,
            recipient_id: "pool-fee".to_string(),
            order_key: "99".to_string(),
            p2mr_program_hex: p2mr_program(99),
        });

        let (request, policy_manifest) =
            build_policy_coinbase_request(&reward_manifest, &[], &policy).unwrap();
        let coinbase_manifest = build_manifest(request).unwrap();
        let pool_fee = policy_manifest.pool_fee.as_ref().unwrap();
        let pool_fee_account = policy_manifest
            .accounts
            .iter()
            .find(|account| account.account_type == PayoutPolicyAccountType::PoolFee)
            .unwrap();

        assert_eq!(pool_fee.fee_bps, 0);
        assert_eq!(pool_fee.amount_sats, 0);
        assert_eq!(pool_fee_account.onchain_amount_sats, 0);
        assert_eq!(pool_fee_account.carry_forward_balance_sats, 0);
        assert!(policy_manifest
            .onchain_entitlements
            .iter()
            .all(|entitlement| entitlement.recipient_id != "pool-fee"));
        assert!(coinbase_manifest
            .outputs
            .iter()
            .all(|output| output.recipient_id != "pool-fee"));
        assert_eq!(
            coinbase_manifest
                .outputs
                .iter()
                .map(|output| output.amount_sats)
                .sum::<u64>(),
            reward_manifest.coinbase_value_sats
        );
        assert!(current_carry_forward_balances(&build_maturity_entries(
            "block-zero-fee",
            10,
            &policy_manifest,
        ))
        .iter()
        .all(|balance| balance.recipient_id != "pool-fee"));
    }

    #[test]
    fn pool_fee_bps_rounds_down_and_miners_split_remainder() {
        let reward_manifest = PrismRewardManifest {
            schema: "qbit.prism.reward-manifest.v1".to_string(),
            block_height: 10,
            coinbase_value_sats: 100_003,
            network_difficulty: 1,
            window_multiplier: PRISM_WINDOW_MULTIPLIER,
            requested_window_weight: 8,
            counted_window_weight: 3,
            anchor_job_issued_at_ms: 1,
            anchor_share_seq: 2,
            newest_share_seq: 2,
            oldest_share_seq: 1,
            included_share_count: 2,
            share_slice_digest_hex: "00".repeat(32),
            shares: Vec::new(),
            entitlements: vec![
                WeightedEntitlement {
                    recipient_id: "miner-a".to_string(),
                    order_key: "01".to_string(),
                    p2mr_program_hex: p2mr_program(1),
                    weight: 1,
                },
                WeightedEntitlement {
                    recipient_id: "miner-b".to_string(),
                    order_key: "02".to_string(),
                    p2mr_program_hex: p2mr_program(2),
                    weight: 2,
                },
            ],
        };
        let mut policy = PayoutPolicy::day_one_default();
        policy.pool_fee_policy = Some(PoolFeePolicy {
            fee_bps: 125,
            recipient_id: "pool-fee".to_string(),
            order_key: "99".to_string(),
            p2mr_program_hex: p2mr_program(99),
        });

        let (request, policy_manifest) =
            build_policy_coinbase_request(&reward_manifest, &[], &policy).unwrap();
        let coinbase_manifest = build_manifest(request).unwrap();
        let pool_fee = policy_manifest.pool_fee.as_ref().unwrap();
        let accounts = policy_manifest
            .accounts
            .iter()
            .map(|account| {
                (
                    account.recipient_id.as_str(),
                    account.account_type.clone(),
                    account.gross_amount_sats,
                    account.onchain_amount_sats,
                    account.carry_forward_balance_sats,
                )
            })
            .collect::<Vec<_>>();
        let outputs = coinbase_manifest
            .outputs
            .iter()
            .map(|output| (output.recipient_id.as_str(), output.amount_sats))
            .collect::<Vec<_>>();

        assert_eq!(pool_fee.amount_sats, 1_250);
        assert_eq!(
            accounts,
            vec![
                ("miner-a", PayoutPolicyAccountType::Miner, 32_918, 32_918, 0),
                ("miner-b", PayoutPolicyAccountType::Miner, 65_835, 65_835, 0),
                (
                    "pool-fee",
                    PayoutPolicyAccountType::PoolFee,
                    1_250,
                    1_250,
                    0
                ),
            ]
        );
        assert_eq!(
            outputs,
            vec![
                ("miner-a", 32_918),
                ("miner-b", 65_835),
                ("pool-fee", 1_250),
            ]
        );
        assert_eq!(
            coinbase_manifest
                .outputs
                .iter()
                .map(|output| output.amount_sats)
                .sum::<u64>(),
            reward_manifest.coinbase_value_sats
        );
        let maturity_entries = build_maturity_entries("block-with-fee", 10, &policy_manifest);
        assert!(maturity_entries
            .iter()
            .any(|entry| entry.account_type == PayoutPolicyAccountType::PoolFee));
        assert!(current_carry_forward_balances(&maturity_entries)
            .iter()
            .all(|balance| balance.recipient_id != "pool-fee"));
    }

    #[test]
    fn pool_fee_account_cannot_duplicate_miner_account() {
        let reward_manifest = PrismRewardManifest {
            schema: "qbit.prism.reward-manifest.v1".to_string(),
            block_height: 10,
            coinbase_value_sats: 100_000,
            network_difficulty: 1,
            window_multiplier: PRISM_WINDOW_MULTIPLIER,
            requested_window_weight: 8,
            counted_window_weight: 1,
            anchor_job_issued_at_ms: 1,
            anchor_share_seq: 1,
            newest_share_seq: 1,
            oldest_share_seq: 1,
            included_share_count: 1,
            share_slice_digest_hex: "00".repeat(32),
            shares: Vec::new(),
            entitlements: vec![WeightedEntitlement {
                recipient_id: "miner-a".to_string(),
                order_key: "01".to_string(),
                p2mr_program_hex: p2mr_program(1),
                weight: 1,
            }],
        };
        let mut policy = PayoutPolicy::day_one_default();
        policy.pool_fee_policy = Some(PoolFeePolicy {
            fee_bps: 125,
            recipient_id: "miner-a".to_string(),
            order_key: "01".to_string(),
            p2mr_program_hex: p2mr_program(1),
        });

        assert!(matches!(
            apply_payout_policy(&reward_manifest, &[], &policy),
            Err(PrismError::DuplicatePoolFeeAccount)
        ));
    }

    #[test]
    fn pool_fee_bps_above_one_hundred_percent_is_rejected() {
        let reward_manifest = PrismRewardManifest {
            schema: "qbit.prism.reward-manifest.v1".to_string(),
            block_height: 10,
            coinbase_value_sats: 100_000,
            network_difficulty: 1,
            window_multiplier: PRISM_WINDOW_MULTIPLIER,
            requested_window_weight: 8,
            counted_window_weight: 1,
            anchor_job_issued_at_ms: 1,
            anchor_share_seq: 1,
            newest_share_seq: 1,
            oldest_share_seq: 1,
            included_share_count: 1,
            share_slice_digest_hex: "00".repeat(32),
            shares: Vec::new(),
            entitlements: vec![WeightedEntitlement {
                recipient_id: "miner-a".to_string(),
                order_key: "01".to_string(),
                p2mr_program_hex: p2mr_program(1),
                weight: 1,
            }],
        };
        let mut policy = PayoutPolicy::day_one_default();
        policy.pool_fee_policy = Some(PoolFeePolicy {
            fee_bps: 10_001,
            recipient_id: "pool-fee".to_string(),
            order_key: "99".to_string(),
            p2mr_program_hex: p2mr_program(99),
        });

        assert!(matches!(
            apply_payout_policy(&reward_manifest, &[], &policy),
            Err(PrismError::PoolFeeBpsTooHigh { fee_bps: 10_001 })
        ));
    }

    #[test]
    fn qbit_coinbase_maturity_is_one_thousand_blocks() {
        let fixture = load_policy_fixture(include_str!(
            "../fixtures/power-law-accrual.prism-fixture.json"
        ));
        let reward_manifest =
            build_prism_reward_manifest(&fixture.shares, &fixture.found_block).unwrap();
        let (_request, policy_manifest) = build_policy_coinbase_request(
            &reward_manifest,
            &power_law_prior_balances(),
            &PayoutPolicy::day_one_default(),
        )
        .unwrap();
        let entries = build_maturity_entries("block-a", 200, &policy_manifest);

        let almost_mature = update_maturity_states(&entries, 1_199);
        assert!(almost_mature
            .iter()
            .all(|entry| entry.state == PayoutMaturityState::Immature));

        let mature = update_maturity_states(&entries, 1_200);
        assert!(mature
            .iter()
            .all(|entry| entry.state == PayoutMaturityState::Mature));
    }

    #[test]
    fn current_carry_forward_balances_replays_active_deltas() {
        let account = |block_hash: &str,
                       block_height: u64,
                       gross_amount_sats: u64,
                       prior_balance_sats: i128,
                       onchain_amount_sats: u64,
                       balance: i128| {
            PayoutMaturityEntry {
                block_hash: block_hash.to_string(),
                block_height,
                account_type: PayoutPolicyAccountType::Miner,
                recipient_id: "miner-a".to_string(),
                order_key: "01".to_string(),
                p2mr_program_hex: p2mr_program(1),
                gross_amount_sats,
                prior_balance_sats,
                candidate_balance_sats: prior_balance_sats + i128::from(gross_amount_sats),
                onchain_amount_sats,
                settlement_fee_sats: 0,
                carry_forward_balance_sats: balance,
                action: if onchain_amount_sats > 0 {
                    PayoutPolicyAction::Onchain
                } else {
                    PayoutPolicyAction::Accrued
                },
                state: PayoutMaturityState::Immature,
            }
        };
        let block_a = account("block-a", 101, 100, 0, 0, 100);
        let block_b = account("block-b", 102, 50, 100, 0, 150);

        let balances = current_carry_forward_balances(&[block_a.clone(), block_b.clone()]);

        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].balance_sats, 150);

        let mut reversed_a = block_a;
        reversed_a.state = PayoutMaturityState::Reversed;
        let balances_after_reorg = current_carry_forward_balances(&[reversed_a, block_b.clone()]);

        assert_eq!(balances_after_reorg.len(), 1);
        assert_eq!(balances_after_reorg[0].balance_sats, 50);

        let mut reversed_b = block_b;
        reversed_b.state = PayoutMaturityState::Reversed;
        assert!(current_carry_forward_balances(&[reversed_b]).is_empty());

        let accrued = account("block-c", 103, 100, 0, 0, 100);
        let paid_down = account("block-d", 104, 0, 100, 100, 0);
        assert!(current_carry_forward_balances(&[accrued, paid_down]).is_empty());
    }

    #[test]
    fn current_carry_forward_balances_preserves_overpayment_debt_after_reorg() {
        let account = |block_hash: &str,
                       block_height: u64,
                       gross_amount_sats: u64,
                       prior_balance_sats: i128,
                       onchain_amount_sats: u64,
                       balance: i128| {
            PayoutMaturityEntry {
                block_hash: block_hash.to_string(),
                block_height,
                account_type: PayoutPolicyAccountType::Miner,
                recipient_id: "miner-a".to_string(),
                order_key: "01".to_string(),
                p2mr_program_hex: p2mr_program(1),
                gross_amount_sats,
                prior_balance_sats,
                candidate_balance_sats: prior_balance_sats + i128::from(gross_amount_sats),
                onchain_amount_sats,
                settlement_fee_sats: 0,
                carry_forward_balance_sats: balance,
                action: if onchain_amount_sats > 0 {
                    PayoutPolicyAction::Onchain
                } else {
                    PayoutPolicyAction::Accrued
                },
                state: PayoutMaturityState::Immature,
            }
        };
        let mut block_a = account("block-a", 101, 100, 0, 0, 100);
        let block_b = account("block-b", 102, 50, 100, 0, 150);
        let block_c = account("block-c", 103, 25, 150, 120, 55);

        assert_eq!(
            current_carry_forward_balances(&[block_a.clone(), block_b.clone(), block_c.clone()])[0]
                .balance_sats,
            55
        );

        block_a.state = PayoutMaturityState::Reversed;
        let active_entries = vec![block_a, block_b, block_c];
        let balances = current_carry_forward_balances(&active_entries);

        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].balance_sats, -45);

        let display_balances = owed_balances_for_display(&active_entries);
        assert_eq!(display_balances[0].balance_sats, 0);
    }

    #[test]
    fn maturity_entry_deserialization_requires_replay_amounts() {
        let legacy_without_replay_amounts = r#"{
            "block_hash": "block-a",
            "block_height": 101,
            "recipient_id": "miner-a",
            "order_key": "01",
            "p2mr_program_hex": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "onchain_amount_sats": 0,
            "carry_forward_balance_sats": 100,
            "action": "accrued",
            "state": "immature"
        }"#;

        let err =
            serde_json::from_str::<PayoutMaturityEntry>(legacy_without_replay_amounts).unwrap_err();

        assert!(
            err.to_string().contains("gross_amount_sats"),
            "legacy maturity entries must not silently replay with zero gross amount: {err}"
        );
    }

    #[test]
    fn reorg_reverses_immature_entries_and_reconverges_on_new_chain() {
        let fixture = load_policy_fixture(include_str!(
            "../fixtures/power-law-accrual.prism-fixture.json"
        ));
        let reward_manifest =
            build_prism_reward_manifest(&fixture.shares, &fixture.found_block).unwrap();
        let (_request, policy_manifest) = build_policy_coinbase_request(
            &reward_manifest,
            &power_law_prior_balances(),
            &PayoutPolicy::day_one_default(),
        )
        .unwrap();
        let block_a_entries = build_maturity_entries("block-a", 200, &policy_manifest);
        assert!(!current_carry_forward_balances(&block_a_entries).is_empty());

        let reversed =
            reverse_disconnected_blocks(&block_a_entries, &[String::from("block-a")], 1_199)
                .unwrap();
        assert!(reversed
            .iter()
            .all(|entry| entry.state == PayoutMaturityState::Reversed));
        assert!(current_carry_forward_balances(&reversed).is_empty());

        let block_b_entries = build_maturity_entries("block-b", 200, &policy_manifest);
        let mut combined = reversed;
        combined.extend(block_b_entries.clone());
        assert_eq!(
            current_carry_forward_balances(&combined),
            current_carry_forward_balances(&block_b_entries)
        );
    }

    #[test]
    fn multi_block_disconnect_reverses_only_disconnected_immature_blocks() {
        let fixture = load_policy_fixture(include_str!(
            "../fixtures/power-law-accrual.prism-fixture.json"
        ));
        let reward_manifest =
            build_prism_reward_manifest(&fixture.shares, &fixture.found_block).unwrap();
        let (_request, policy_manifest) = build_policy_coinbase_request(
            &reward_manifest,
            &power_law_prior_balances(),
            &PayoutPolicy::day_one_default(),
        )
        .unwrap();
        let mut entries = build_maturity_entries("block-a", 200, &policy_manifest);
        entries.extend(build_maturity_entries("block-b", 201, &policy_manifest));
        entries.extend(build_maturity_entries("block-c", 202, &policy_manifest));

        let reversed = reverse_disconnected_blocks(
            &entries,
            &[String::from("block-a"), String::from("block-b")],
            1_199,
        )
        .unwrap();

        assert!(reversed
            .iter()
            .filter(|entry| entry.block_hash == "block-a" || entry.block_hash == "block-b")
            .all(|entry| entry.state == PayoutMaturityState::Reversed));
        assert!(reversed
            .iter()
            .filter(|entry| entry.block_hash == "block-c")
            .all(|entry| entry.state == PayoutMaturityState::Immature));
    }

    #[test]
    fn disconnect_processing_is_idempotent_for_already_reversed_entries() {
        let fixture = load_policy_fixture(include_str!(
            "../fixtures/power-law-accrual.prism-fixture.json"
        ));
        let reward_manifest =
            build_prism_reward_manifest(&fixture.shares, &fixture.found_block).unwrap();
        let (_request, policy_manifest) = build_policy_coinbase_request(
            &reward_manifest,
            &power_law_prior_balances(),
            &PayoutPolicy::day_one_default(),
        )
        .unwrap();
        let entries = build_maturity_entries("block-a", 200, &policy_manifest);
        let once =
            reverse_disconnected_blocks(&entries, &[String::from("block-a")], 1_199).unwrap();
        let twice = reverse_disconnected_blocks(&once, &[String::from("block-a")], 1_199).unwrap();

        assert_eq!(once, twice);
    }

    #[test]
    fn matured_entries_cannot_be_reversed_silently() {
        let fixture = load_policy_fixture(include_str!(
            "../fixtures/power-law-accrual.prism-fixture.json"
        ));
        let reward_manifest =
            build_prism_reward_manifest(&fixture.shares, &fixture.found_block).unwrap();
        let (_request, policy_manifest) = build_policy_coinbase_request(
            &reward_manifest,
            &power_law_prior_balances(),
            &PayoutPolicy::day_one_default(),
        )
        .unwrap();
        let entries = update_maturity_states(
            &build_maturity_entries("block-a", 200, &policy_manifest),
            1_200,
        );

        assert!(matches!(
            reverse_disconnected_blocks(&entries, &[String::from("block-a")], 1_200),
            Err(PrismError::MaturePayoutDisconnect { .. })
        ));
    }

    #[test]
    fn audit_bundle_verifier_recomputes_manifests_and_matches_coinbase_hex() {
        let bundle = power_law_audit_bundle();
        let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();

        assert_eq!(report.schema, "qbit.prism.audit-verification-report.v1");
        assert_eq!(report.block_height, 200);
        assert_eq!(report.coinbase_value_sats, 500_000_000);
        assert_eq!(report.min_output_sats, 14_720);
        assert_eq!(report.onchain_output_count, 3);
        assert_eq!(report.accrued_account_count, 3);
        assert_eq!(
            report.coinbase_tx_hex,
            bundle.signed_coinbase_manifest.manifest.coinbase_tx_hex
        );
        assert_eq!(report.reward_manifest_sha256_hex.len(), 64);
        assert_eq!(report.payout_policy_manifest_sha256_hex.len(), 64);
        assert_eq!(report.prism_audit_commitment_leaf_hex.len(), 64);
        assert!(bundle.witness_merkle_leaves_hex.is_empty());
        assert_eq!(
            bundle.audit_commitment_leaves_hex,
            vec![report.prism_audit_commitment_leaf_hex.clone()]
        );
        assert_eq!(
            report.audit_commitment_root_hex,
            bundle.signed_coinbase_manifest.manifest.witness_nonce_hex
        );
        assert_eq!(report.coinbase_manifest_sha256_hex.len(), 64);
        assert_eq!(report.audit_bundle_sha256_hex.len(), 64);

        let uppercase_coinbase = report.coinbase_tx_hex.to_ascii_uppercase();
        assert!(verify_audit_bundle_against_coinbase_tx_hex(
            &bundle,
            &uppercase_coinbase,
            &ledger_public_key_hex()
        )
        .is_ok());
    }

    #[test]
    fn audit_bundle_verifier_recomputes_coinbase_with_witness_leaves() {
        let witness_leaves = vec!["11".repeat(32)];
        let bundle = build_audit_bundle_with_coinbase_options(
            vec![share(1, "miner-a", "01", 1, 10, 1000)],
            found_block(10, 1000),
            Vec::new(),
            PayoutPolicy::day_one_default(),
            Some("aaaaaaaa".to_string()),
            witness_leaves.clone(),
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap();

        let expected_audit_leaf = prism_audit_commitment_leaf_hex(
            &bundle.reward_manifest,
            &bundle.payout_policy_manifest,
        )
        .unwrap();
        assert_eq!(bundle.witness_merkle_leaves_hex, witness_leaves);
        assert_eq!(
            bundle.audit_commitment_leaves_hex,
            vec![expected_audit_leaf.clone()]
        );
        assert_eq!(
            bundle.audit_commitment_root_hex.as_deref(),
            Some(expected_audit_leaf.as_str())
        );
        assert_eq!(
            bundle.signed_coinbase_manifest.manifest.witness_nonce_hex,
            expected_audit_leaf
        );
        let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
        assert_eq!(
            report.audit_commitment_root_hex,
            bundle.signed_coinbase_manifest.manifest.witness_nonce_hex
        );

        let mut tampered_root = bundle.clone();
        tampered_root.audit_commitment_root_hex = Some("00".repeat(32));
        assert!(matches!(
            verify_audit_bundle(&tampered_root, &ledger_public_key_hex()),
            Err(PrismError::AuditMismatch {
                artifact: "audit_commitment_root"
            })
        ));

        let mut tampered = bundle;
        tampered.audit_commitment_leaves_hex = vec!["22".repeat(32)];
        assert!(matches!(
            verify_audit_bundle(&tampered, &ledger_public_key_hex()),
            Err(PrismError::AuditMismatch {
                artifact: "audit_commitment_leaves"
            })
        ));
    }

    #[test]
    fn audit_bundle_coinbase_witness_commitment_uses_consensus_leaves_and_audit_nonce() {
        let actual_template_wtxid = "11".repeat(32);
        let bundle = build_audit_bundle_with_coinbase_options(
            vec![share(1, "miner-a", "01", 1, 10, 1000)],
            found_block(10, 1000),
            Vec::new(),
            PayoutPolicy::day_one_default(),
            Some("aaaaaaaa".to_string()),
            vec![actual_template_wtxid.clone()],
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap();
        let manifest = &bundle.signed_coinbase_manifest.manifest;
        let expected_audit_leaf = prism_audit_commitment_leaf_hex(
            &bundle.reward_manifest,
            &bundle.payout_policy_manifest,
        )
        .unwrap();

        assert_eq!(
            bundle.witness_merkle_leaves_hex,
            vec![actual_template_wtxid]
        );
        assert!(!bundle
            .witness_merkle_leaves_hex
            .iter()
            .any(|leaf| leaf == &expected_audit_leaf));
        assert_eq!(
            bundle.audit_commitment_leaves_hex,
            vec![expected_audit_leaf.clone()]
        );
        assert_eq!(manifest.witness_nonce_hex, expected_audit_leaf);
        assert_ne!(manifest.witness_nonce_hex, "00".repeat(32));
        assert_eq!(
            manifest.witness_commitment_script_hex,
            consensus_witness_commitment_script_hex(
                &bundle.witness_merkle_leaves_hex,
                &manifest.witness_nonce_hex,
            )
        );
    }

    #[test]
    fn audit_bundle_can_build_and_verify_hybrid_ctv_settlement() {
        let found_block = FoundBlock {
            block_height: 101,
            coinbase_value_sats: 100_000,
            network_difficulty: 5,
            anchor_job_issued_at_ms: 1000,
        };
        let config = SettlementModeConfig {
            max_coinbase_settlement_outputs: 16,
            max_direct_coinbase_outputs: 1,
            max_fanout_recipients_per_transaction: 10,
            reserved_coinbase_outputs: 0,
        };

        let bundle = build_audit_bundle_with_ctv_settlement_options(
            vec![
                share(1, "miner-a", "01", 1, 3, 1000),
                share(2, "miner-b", "02", 2, 2, 1000),
            ],
            found_block,
            Vec::new(),
            PayoutPolicy::day_one_default(),
            50_000,
            config,
            None,
            Some("aaaaaaaa".to_string()),
            Vec::new(),
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap();

        let decision = bundle.settlement_mode_decision.as_ref().unwrap();
        assert_eq!(decision.mode, SettlementMode::HybridCoinbaseCtvFanout);
        assert_eq!(decision.direct_recipient_count, 1);
        assert_eq!(decision.fanout_recipient_count, 1);
        let fanout_set = bundle.ctv_fanout_manifest_set.as_ref().unwrap();
        assert_eq!(fanout_set.fanout_count, 1);
        assert_eq!(fanout_set.covenant_output_value_sats, 40_000);
        assert_eq!(
            fanout_set.manifests[0].parent_coinbase_tx_hex,
            bundle.signed_coinbase_manifest.manifest.coinbase_tx_hex
        );
        assert!(bundle.witness_merkle_leaves_hex.is_empty());
        assert!(bundle
            .audit_commitment_leaves_hex
            .iter()
            .any(|leaf| { leaf == &fanout_set.manifests[0].commitment_witness_leaf_hex }));
        let expected_audit_root = audit_commitment_root_hex(&bundle.audit_commitment_leaves_hex)
            .expect("audit commitment leaves are valid");
        assert_eq!(
            bundle.audit_commitment_root_hex.as_deref(),
            Some(expected_audit_root.as_str())
        );
        assert_eq!(
            bundle.signed_coinbase_manifest.manifest.witness_nonce_hex,
            expected_audit_root
        );
        assert_eq!(
            bundle
                .signed_coinbase_manifest
                .manifest
                .outputs
                .iter()
                .map(|output| (output.recipient_id.as_str(), output.amount_sats))
                .collect::<Vec<_>>(),
            vec![("miner-a", 60_000), ("ctv-fanout-0", 40_000)]
        );

        let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
        assert_eq!(report.coinbase_value_sats, 100_000);
        assert_eq!(report.onchain_output_count, 2);
    }

    #[test]
    fn audit_bundle_can_build_and_verify_hybrid_ctv_settlement_with_fanout_fee() {
        let found_block = FoundBlock {
            block_height: 101,
            coinbase_value_sats: 100_000,
            network_difficulty: 5,
            anchor_job_issued_at_ms: 1000,
        };
        let config = SettlementModeConfig {
            max_coinbase_settlement_outputs: 16,
            max_direct_coinbase_outputs: 1,
            max_fanout_recipients_per_transaction: 10,
            reserved_coinbase_outputs: 0,
        };
        let fee_policy = FanoutFeeRatePolicy::new(1_000, 10_000);
        let expected_fee = estimate_ctv_fanout_fee_sats(1, &fee_policy).unwrap();

        let bundle = build_audit_bundle_with_ctv_settlement_options(
            vec![
                share(1, "miner-a", "01", 1, 3, 1000),
                share(2, "miner-b", "02", 2, 2, 1000),
            ],
            found_block,
            Vec::new(),
            PayoutPolicy::day_one_default(),
            50_000,
            config,
            Some(fee_policy),
            Some("aaaaaaaa".to_string()),
            Vec::new(),
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap();

        let miner_b = bundle
            .payout_policy_manifest
            .accounts
            .iter()
            .find(|account| account.recipient_id == "miner-b")
            .unwrap();
        assert_eq!(miner_b.onchain_amount_sats, 40_000);
        assert_eq!(miner_b.settlement_fee_sats, expected_fee);
        assert_eq!(miner_b.carry_forward_balance_sats, 0);
        assert_eq!(
            bundle
                .payout_policy_manifest
                .onchain_entitlements
                .iter()
                .map(|entitlement| entitlement.weight)
                .sum::<u128>(),
            100_000
        );

        let fanout_set = bundle.ctv_fanout_manifest_set.as_ref().unwrap();
        let fanout = &fanout_set.manifests[0];
        let output = &fanout.precommitment.outputs[0];
        assert_eq!(fanout_set.covenant_output_value_sats, 40_000);
        assert_eq!(fanout_set.fanout_fee_sats, expected_fee);
        assert_eq!(output.gross_amount_sats, 40_000);
        assert_eq!(output.fee_sats, expected_fee);
        assert_eq!(output.amount_sats, 40_000 - expected_fee);
        assert_eq!(
            fanout.precommitment.fanout_output_sum_sats + fanout.precommitment.fanout_fee_sats,
            fanout.covenant_output_value_sats
        );

        let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
        assert_eq!(report.coinbase_value_sats, 100_000);
        assert_eq!(report.onchain_output_count, 2);
    }

    #[test]
    fn ctv_fanout_fee_charges_pool_fee_output() {
        let found_block = FoundBlock {
            block_height: 101,
            coinbase_value_sats: 1_000_000,
            network_difficulty: 5,
            anchor_job_issued_at_ms: 1000,
        };
        let config = SettlementModeConfig {
            max_coinbase_settlement_outputs: 16,
            max_direct_coinbase_outputs: 1,
            max_fanout_recipients_per_transaction: 10,
            reserved_coinbase_outputs: 0,
        };
        let fee_policy = FanoutFeeRatePolicy::new(1_000, 12_000);
        let expected_fanout_fee = estimate_ctv_fanout_fee_sats(2, &fee_policy).unwrap();
        let mut payout_policy = PayoutPolicy::day_one_default();
        payout_policy.pool_fee_policy = Some(PoolFeePolicy {
            fee_bps: 200,
            recipient_id: "pool-fee".to_string(),
            order_key: "zzzzzzzz".to_string(),
            p2mr_program_hex: "ff".repeat(32),
        });

        let bundle = build_audit_bundle_with_ctv_settlement_options(
            vec![
                share(1, "miner-a", "01", 1, 3, 1000),
                share(2, "miner-b", "02", 2, 2, 1000),
            ],
            found_block,
            Vec::new(),
            payout_policy,
            50_000,
            config,
            Some(fee_policy),
            Some("aaaaaaaa".to_string()),
            Vec::new(),
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap();

        let pool_fee = bundle.payout_policy_manifest.pool_fee.as_ref().unwrap();
        assert_eq!(pool_fee.amount_sats, 20_000);
        let pool_fee_account = bundle
            .payout_policy_manifest
            .accounts
            .iter()
            .find(|account| account.account_type == PayoutPolicyAccountType::PoolFee)
            .unwrap();
        assert_eq!(pool_fee_account.onchain_amount_sats, 20_000);
        assert!(pool_fee_account.settlement_fee_sats > 0);

        let miner_b = bundle
            .payout_policy_manifest
            .accounts
            .iter()
            .find(|account| account.recipient_id == "miner-b")
            .unwrap();
        assert_eq!(miner_b.onchain_amount_sats, 392_000);
        assert!(miner_b.settlement_fee_sats > 0);

        let fanout_set = bundle.ctv_fanout_manifest_set.as_ref().unwrap();
        let fanout = &fanout_set.manifests[0];
        assert_eq!(fanout_set.fanout_count, 1);
        assert_eq!(fanout.precommitment.outputs.len(), 2);
        assert_eq!(fanout.precommitment.anchor_vout, None);
        assert_eq!(fanout_set.fanout_fee_sats, expected_fanout_fee);
        assert_eq!(
            fanout.precommitment.fanout_output_sum_sats + fanout.precommitment.fanout_fee_sats,
            fanout.covenant_output_value_sats
        );

        let miner_output = fanout
            .precommitment
            .outputs
            .iter()
            .find(|output| output.recipient_id == "miner-b")
            .unwrap();
        assert_eq!(miner_output.gross_amount_sats, 392_000);
        assert_eq!(miner_output.fee_sats, miner_b.settlement_fee_sats);
        assert_eq!(miner_output.amount_sats, 392_000 - miner_output.fee_sats);

        let pool_fee_output = fanout
            .precommitment
            .outputs
            .iter()
            .find(|output| output.recipient_id == "pool-fee")
            .unwrap();
        assert_eq!(pool_fee_output.gross_amount_sats, 20_000);
        assert_eq!(
            pool_fee_output.fee_sats,
            pool_fee_account.settlement_fee_sats
        );
        assert_eq!(
            pool_fee_output.amount_sats,
            20_000 - pool_fee_output.fee_sats
        );
        assert_eq!(
            miner_output.fee_sats + pool_fee_output.fee_sats,
            expected_fanout_fee
        );
    }

    #[test]
    fn ctv_fanout_fee_charges_pool_only_fanout_chunk() {
        let found_block = FoundBlock {
            block_height: 101,
            coinbase_value_sats: 1_000_000,
            network_difficulty: 5,
            anchor_job_issued_at_ms: 1000,
        };
        let config = SettlementModeConfig {
            max_coinbase_settlement_outputs: 16,
            max_direct_coinbase_outputs: 2,
            max_fanout_recipients_per_transaction: 10,
            reserved_coinbase_outputs: 0,
        };
        let fee_policy = FanoutFeeRatePolicy::new(1_000, 12_000);
        let expected_fanout_fee = estimate_ctv_fanout_fee_sats(1, &fee_policy).unwrap();
        let mut payout_policy = PayoutPolicy::day_one_default();
        payout_policy.pool_fee_policy = Some(PoolFeePolicy {
            fee_bps: 200,
            recipient_id: "pool-fee".to_string(),
            order_key: "zzzzzzzz".to_string(),
            p2mr_program_hex: "ff".repeat(32),
        });

        let bundle = build_audit_bundle_with_ctv_settlement_options(
            vec![
                share(1, "miner-a", "01", 1, 3, 1000),
                share(2, "miner-b", "02", 2, 2, 1000),
            ],
            found_block,
            Vec::new(),
            payout_policy,
            300_000,
            config,
            Some(fee_policy),
            Some("aaaaaaaa".to_string()),
            Vec::new(),
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap();

        let decision = bundle.settlement_mode_decision.as_ref().unwrap();
        assert_eq!(decision.direct_recipient_count, 2);
        assert_eq!(decision.fanout_recipient_count, 1);

        let fanout_set = bundle.ctv_fanout_manifest_set.as_ref().unwrap();
        let fanout = &fanout_set.manifests[0];
        assert_eq!(fanout_set.fanout_fee_sats, expected_fanout_fee);
        assert_eq!(fanout.precommitment.fanout_fee_sats, expected_fanout_fee);
        assert_eq!(fanout.precommitment.anchor_vout, None);
        assert_eq!(fanout.precommitment.outputs.len(), 1);
        assert_eq!(fanout.covenant_output_value_sats, 20_000);
        assert_eq!(
            fanout.precommitment.fanout_output_sum_sats + fanout.precommitment.fanout_fee_sats,
            fanout.covenant_output_value_sats
        );

        let pool_fee_output = &fanout.precommitment.outputs[0];
        assert_eq!(pool_fee_output.recipient_id, "pool-fee");
        assert_eq!(pool_fee_output.gross_amount_sats, 20_000);
        assert_eq!(pool_fee_output.fee_sats, expected_fanout_fee);
        assert_eq!(pool_fee_output.amount_sats, 20_000 - expected_fanout_fee);

        let pool_fee_account = bundle
            .payout_policy_manifest
            .accounts
            .iter()
            .find(|account| account.account_type == PayoutPolicyAccountType::PoolFee)
            .unwrap();
        assert_eq!(pool_fee_account.settlement_fee_sats, expected_fanout_fee);

        for miner in ["miner-a", "miner-b"] {
            let account = bundle
                .payout_policy_manifest
                .accounts
                .iter()
                .find(|account| account.recipient_id == miner)
                .unwrap();
            assert_eq!(account.settlement_fee_sats, 0);
        }

        let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
        assert_eq!(report.coinbase_value_sats, 1_000_000);
        assert_eq!(report.onchain_output_count, 3);
    }

    #[test]
    fn ctv_fanout_fee_rejects_pool_only_chunk_when_fee_would_make_it_dust() {
        let found_block = FoundBlock {
            block_height: 101,
            coinbase_value_sats: 1_000_000,
            network_difficulty: 5,
            anchor_job_issued_at_ms: 1000,
        };
        let config = SettlementModeConfig {
            max_coinbase_settlement_outputs: 16,
            max_direct_coinbase_outputs: 2,
            max_fanout_recipients_per_transaction: 10,
            reserved_coinbase_outputs: 0,
        };
        let fee_policy = FanoutFeeRatePolicy::new(1_000_000, 10_000);
        let mut payout_policy = PayoutPolicy::day_one_default();
        payout_policy.pool_fee_policy = Some(PoolFeePolicy {
            fee_bps: 200,
            recipient_id: "pool-fee".to_string(),
            order_key: "zzzzzzzz".to_string(),
            p2mr_program_hex: "ff".repeat(32),
        });

        let err = build_audit_bundle_with_ctv_settlement_options(
            vec![
                share(1, "miner-a", "01", 1, 3, 1000),
                share(2, "miner-b", "02", 2, 2, 1000),
            ],
            found_block,
            Vec::new(),
            payout_policy,
            300_000,
            config,
            Some(fee_policy),
            Some("aaaaaaaa".to_string()),
            Vec::new(),
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            PrismError::SettlementModeSelection { reason }
                if reason.contains("fanout fee would push a recipient below")
        ));
    }

    #[test]
    fn audit_bundle_rejects_fanout_fee_that_pushes_recipient_below_floor() {
        let found_block = FoundBlock {
            block_height: 101,
            coinbase_value_sats: 100_000,
            network_difficulty: 5,
            anchor_job_issued_at_ms: 1000,
        };
        let config = SettlementModeConfig {
            max_coinbase_settlement_outputs: 16,
            max_direct_coinbase_outputs: 1,
            max_fanout_recipients_per_transaction: 10,
            reserved_coinbase_outputs: 0,
        };
        let fee_policy = FanoutFeeRatePolicy::new(1_000_000, 10_000);

        let err = build_audit_bundle_with_ctv_settlement_options(
            vec![
                share(1, "miner-a", "01", 1, 3, 1000),
                share(2, "miner-b", "02", 2, 2, 1000),
            ],
            found_block,
            Vec::new(),
            PayoutPolicy::day_one_default(),
            50_000,
            config,
            Some(fee_policy),
            Some("aaaaaaaa".to_string()),
            Vec::new(),
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            PrismError::SettlementModeSelection { reason }
                if reason.contains("fanout fee would push a recipient below")
        ));
    }

    #[test]
    fn audit_bundle_verifier_rejects_missing_prism_audit_commitment_leaf() {
        let mut bundle = power_law_audit_bundle();
        let audit_leaf = prism_audit_commitment_leaf_hex(
            &bundle.reward_manifest,
            &bundle.payout_policy_manifest,
        )
        .unwrap();
        assert!(bundle.audit_commitment_leaves_hex.contains(&audit_leaf));
        bundle
            .audit_commitment_leaves_hex
            .retain(|leaf| leaf != &audit_leaf);

        assert!(matches!(
            verify_audit_bundle(&bundle, &ledger_public_key_hex()),
            Err(PrismError::AuditMismatch {
                artifact: "audit_commitment_leaves"
            })
        ));
    }

    #[test]
    fn ledger_attestation_binds_block_height_and_coinbase_value() {
        let bundle = power_law_audit_bundle();

        let mut replayed_height = bundle.clone();
        let mut wrong_height_manifest = bundle.reward_manifest.clone();
        wrong_height_manifest.block_height += 1;
        replayed_height.ledger_window_attestation = build_ledger_window_attestation(
            &wrong_height_manifest,
            &replayed_height.prior_balances,
            &ledger_signing_key(),
        )
        .unwrap();
        assert!(matches!(
            verify_audit_bundle(&replayed_height, &ledger_public_key_hex()),
            Err(PrismError::AuditMismatch {
                artifact: "ledger_attestation_scope"
            })
        ));

        let mut replayed_value = bundle.clone();
        let mut wrong_value_manifest = bundle.reward_manifest.clone();
        wrong_value_manifest.coinbase_value_sats += 1;
        replayed_value.ledger_window_attestation = build_ledger_window_attestation(
            &wrong_value_manifest,
            &replayed_value.prior_balances,
            &ledger_signing_key(),
        )
        .unwrap();
        assert!(matches!(
            verify_audit_bundle(&replayed_value, &ledger_public_key_hex()),
            Err(PrismError::AuditMismatch {
                artifact: "ledger_attestation_scope"
            })
        ));
    }

    #[test]
    fn verifier_rejects_unexpected_coinbase_value() {
        let bundle = power_law_audit_bundle();
        let report = verify_audit_bundle_against_coinbase_tx_hex_and_expected_coinbase_value(
            &bundle,
            &bundle.signed_coinbase_manifest.manifest.coinbase_tx_hex,
            &ledger_public_key_hex(),
            500_000_000,
        )
        .unwrap();
        assert_eq!(report.coinbase_value_sats, 500_000_000);

        assert!(matches!(
            verify_audit_bundle_against_coinbase_tx_hex_and_expected_coinbase_value(
                &bundle,
                &bundle.signed_coinbase_manifest.manifest.coinbase_tx_hex,
                &ledger_public_key_hex(),
                500_000_001,
            ),
            Err(PrismError::ExpectedCoinbaseValueMismatch {
                expected_coinbase_value_sats: 500_000_001,
                actual_coinbase_value_sats: 500_000_000,
            })
        ));
    }

    #[test]
    fn audit_bundle_verifier_accepts_coinbase_script_sig_suffix() {
        let fixture = load_policy_fixture(include_str!(
            "../fixtures/power-law-accrual.prism-fixture.json"
        ));
        let suffix = "111111112222222222222222".to_string();
        let bundle = build_audit_bundle_with_coinbase_script_sig_suffix(
            fixture.shares,
            fixture.found_block,
            power_law_prior_balances(),
            PayoutPolicy::day_one_default(),
            Some(suffix.clone()),
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
        .unwrap();
        let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();

        assert_eq!(
            bundle.coinbase_script_sig_suffix_hex.as_deref(),
            Some(suffix.as_str())
        );
        assert_eq!(
            bundle
                .signed_coinbase_manifest
                .manifest
                .coinbase_script_sig_suffix_hex,
            suffix
        );
        assert!(bundle
            .signed_coinbase_manifest
            .manifest
            .coinbase_script_sig_hex
            .ends_with(&suffix));
        assert!(report.coinbase_tx_hex.contains(&suffix));
    }

    #[test]
    fn audit_bundle_verifier_rejects_tampered_share_window() {
        let mut bundle = power_law_audit_bundle();
        bundle.shares[0].share_difficulty += 1;

        assert!(matches!(
            verify_audit_bundle(&bundle, &ledger_public_key_hex()),
            Err(PrismError::AuditMismatch {
                artifact: "reward_manifest"
            })
        ));
    }

    #[test]
    fn audit_bundle_verifier_rejects_tampered_payout_policy_manifest() {
        let mut bundle = power_law_audit_bundle();
        bundle.payout_policy_manifest.accounts[0].gross_amount_sats += 1;

        assert!(matches!(
            verify_audit_bundle(&bundle, &ledger_public_key_hex()),
            Err(PrismError::AuditMismatch {
                artifact: "payout_policy_manifest"
            })
        ));
    }

    #[test]
    fn audit_bundle_verifier_rejects_tampered_signed_coinbase_manifest() {
        let mut bundle = power_law_audit_bundle();
        bundle.signed_coinbase_manifest.manifest.outputs[0].amount_sats += 1;

        assert!(matches!(
            verify_audit_bundle(&bundle, &ledger_public_key_hex()),
            Err(PrismError::Builder(
                BuilderError::SignatureVerificationFailed
            ))
        ));
    }

    #[test]
    fn audit_bundle_verifier_rejects_wrong_onchain_coinbase_hex() {
        let bundle = power_law_audit_bundle();

        assert!(matches!(
            verify_audit_bundle_against_coinbase_tx_hex(&bundle, "00", &ledger_public_key_hex()),
            Err(PrismError::AuditCoinbaseTxMismatch)
        ));
    }
}
