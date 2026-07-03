//! Deterministic OP_CHECKTEMPLATEVERIFY (CTV) fanout settlement manifests for
//! qbit PRISM payouts.
//!
//! # Settlement model
//!
//! When direct coinbase outputs are impractical, the pool mines a coinbase
//! whose payout output is a native P2MR (qbit's taproot-style) script-path
//! output committing to `<ctv_hash> OP_CHECKTEMPLATEVERIFY`. After coinbase
//! maturity, *anyone* can broadcast the precommitted "fanout" transaction that
//! pays miners. There is **no pool spend key**: CTV plus proof-of-work enforce
//! the exact payout outputs at consensus level, so the operator cannot redirect
//! or withhold funds after the block is mined.
//!
//! The manifest itself is an off-chain audit artifact. It exists so a miner can
//! *independently* reconstruct and verify the on-chain commitment, trusting only
//! the chain.
//!
//! # Fees and the CPFP anchor
//!
//! A CTV-committed transaction's fee is fixed at commit time and cannot be
//! raised — changing any output breaks the template hash, so RBF is impossible.
//! PRISM therefore supports two deliberately separate fee shapes:
//!
//! - built-in-fee fanouts pay a positive parent fee directly and include no
//!   P2A anchor output;
//! - legacy CPFP fanouts pay zero parent fee and end with a keyless P2A
//!   ephemeral anchor of value 0, so a broadcaster can attach a fee-paying
//!   child and submit the parent + child package.
//!
//! The broken mixed shape — a nonzero parent fee plus the zero-sat anchor — is
//! nonstandard under qbit policy and is rejected here.
//!
//! # Trust boundary (read before integrating)
//!
//! [`verify_ctv_fanout_manifest_structure`] proves only that a manifest is
//! **internally consistent**: that the supplied fanout transaction matches the
//! committed CTV template, that the supplied parent coinbase output's
//! scriptPubKey is the P2MR covenant for that template, that outputs + fee equal
//! the covenant value, and that no field has been mutated. Every byte it checks
//! is supplied by whoever assembled the manifest.
//!
//! It does **NOT**, and structurally **cannot**, prove any of the following —
//! all of which a consumer that intends to *trust* a manifest MUST verify
//! independently against the chain (this is the job of the broadcaster / status
//! layer, recovery artifacts):
//!
//! 1. that `parent_coinbase_tx_hex` is the coinbase of a real block with valid
//!    proof-of-work (the coinbase-shape check here is necessary but not
//!    sufficient — it does not prove the tx was mined);
//! 2. that the block is at `precommitment.block_height`;
//! 3. that the coinbase has reached [`QBIT_COINBASE_MATURITY_BLOCKS`] depth
//!    before the fanout may be spent;
//! 4. that the manifest's `commitment_witness_leaf_hex` was committed in *that*
//!    block (the pre-mining commitment), which
//!    [`verify_ctv_fanout_manifest_commitment_leaf`] checks only against
//!    caller-supplied audit commitment leaves — the caller is responsible for
//!    proving those leaves are bound by the real mined coinbase witness nonce.
//!
//! A returning-`Ok` structural check is therefore **not** proof that a miner
//! will be paid. The functions are named accordingly so this is not mistaken.
//!
//! [`QBIT_COINBASE_MATURITY_BLOCKS`]: crate::QBIT_COINBASE_MATURITY_BLOCKS

use crate::PrismError;
use qbit_pool_builder::{p2mr_script_pubkey, P2MR_PROGRAM_LEN};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub const CTV_FANOUT_MANIFEST_SCHEMA: &str = "qbit.prism.ctv-fanout-manifest.v1";
pub const CTV_FANOUT_MANIFEST_SET_SCHEMA: &str = "qbit.prism.ctv-fanout-manifest-set.v1";
pub const CTV_FANOUT_PRECOMMITMENT_SCHEMA: &str = "qbit.prism.ctv-fanout-precommitment.v1";
pub const CTV_FANOUT_COMMITMENT_LEAF_TAG: &str = "qbit.prism.ctv-fanout.commitment.v1";
pub const QBIT_P2MR_LEAF_VERSION: u8 = 0xc0;
pub const QBIT_P2MR_CONTROL_BLOCK: u8 = QBIT_P2MR_LEAF_VERSION | 1;
pub const QBIT_OP_CHECKTEMPLATEVERIFY: u8 = 0xbb;
pub const QBIT_CTV_INPUT_INDEX: u32 = 0;
pub const COINBASE_PREVOUT_VOUT: u32 = u32::MAX;
/// Consensus sanity ceiling on any single satoshi amount, mirroring qbit's
/// `MAX_MONEY` (`210_000_000 * COIN`). Amounts above this cannot appear in a
/// valid transaction, so the manifest rejects them defensively.
pub const QBIT_MAX_MONEY_SATS: u64 = 210_000_000 * 100_000_000;
/// Canonical keyless pay-to-anchor (P2A) scriptPubKey: `OP_1 <0x4e73>`. qbit
/// recognizes this as a standard, anyone-can-spend output used for CPFP
/// fee-bumping (`src/script/script.cpp` `IsPayToAnchor` in qbit core).
pub const P2A_ANCHOR_SCRIPT_PUBKEY_HEX: &str = "51024e73";
/// The fee anchor carries zero value: it exists only as a CPFP attach point, so
/// no miner value is diverted into it. As ephemeral dust it requires the fanout
/// to be a 0-fee transaction swept by exactly one child.
pub const CTV_FANOUT_ANCHOR_VALUE_SATS: u64 = 0;
/// The fanout must be a version-3 (TRUC) transaction so a 0-fee parent relays as
/// a CPFP package on qbit (`TX_MAX_STANDARD_VERSION == 3`).
pub const CTV_FANOUT_TX_VERSION: u32 = 3;
/// BIP68 disable flag. It lets the sequence field discriminate CTV chunks
/// without adding relative-lock semantics to nonzero chunks.
pub const SEQUENCE_LOCKTIME_DISABLE_FLAG: u32 = 1 << 31;
/// The fanout input sequence is part of the CTV hash. Using `chunk_index` as the
/// low bits of a disabled sequence makes same-block chunks distinct even if
/// their payout sets match.
pub const CTV_FANOUT_SEQUENCE_BASE: u32 = SEQUENCE_LOCKTIME_DISABLE_FLAG;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SettlementMode {
    DirectCoinbase,
    HybridCoinbaseCtvFanout,
    CtvFanout,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CtvFanoutPayout {
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub gross_amount_sats: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub fee_sats: u64,
    pub amount_sats: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CtvFanoutManifestInput {
    pub block_height: u64,
    pub chunk_index: u32,
    pub chunk_count: u32,
    /// Informational record of the full block reward for `block_height`.
    ///
    /// This is **not** the authoritative value for the fanout: the covenant
    /// output value (derived from the parsed parent coinbase) is. In
    /// `hybrid_coinbase_ctv_fanout` mode the coinbase legitimately has outputs
    /// beyond the covenant (direct payouts), so `coinbase_value_sats` does not
    /// equal `covenant_output_value_sats` in general. Confirming that the
    /// covenant captures the correct share of the block reward — and that no
    /// value is skimmed into undisclosed coinbase outputs — is bound via
    /// `reward_manifest_sha256_hex` / `payout_policy_manifest_sha256_hex` and
    /// verified at the policy + chain layer, not here.
    pub coinbase_value_sats: u64,
    pub settlement_mode: SettlementMode,
    pub fanout_tx_template_hex: String,
    pub parent_coinbase_tx_hex: String,
    pub parent_coinbase_vout: u32,
    pub fanout_tx_hex: String,
    pub reward_manifest_sha256_hex: String,
    pub payout_policy_manifest_sha256_hex: String,
    pub payouts: Vec<CtvFanoutPayout>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CtvFanoutPrecommitmentInput {
    pub block_height: u64,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub coinbase_value_sats: u64,
    pub settlement_mode: SettlementMode,
    pub reward_manifest_sha256_hex: String,
    pub payout_policy_manifest_sha256_hex: String,
    pub payouts: Vec<CtvFanoutPayout>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PreparedCtvFanout {
    pub precommitment: CtvFanoutPrecommitment,
    pub precommitment_sha256_hex: String,
    pub commitment_witness_leaf_hex: String,
    pub covenant_recipient_id: String,
    pub covenant_order_key: String,
    pub covenant_p2mr_program_hex: String,
    pub covenant_output_value_sats: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CtvFanoutOutput {
    pub vout: u32,
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    pub script_pubkey_hex: String,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub gross_amount_sats: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub fee_sats: u64,
    pub amount_sats: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CtvFanoutPrecommitment {
    pub schema: String,
    pub block_height: u64,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub coinbase_value_sats: u64,
    pub settlement_mode: SettlementMode,
    pub covenant_output_value_sats: u64,
    pub covenant_script_pubkey_hex: String,
    pub fanout_tx_template_hex: String,
    pub fanout_input_index: u32,
    pub fanout_sequence: u32,
    pub fanout_lock_time: u32,
    pub fanout_output_sum_sats: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub fanout_fee_sats: u64,
    /// Index of the keyless P2A fee anchor in legacy zero-fee CPFP mode. It is
    /// absent for built-in-fee fanouts, because qbit policy rejects a nonzero
    /// parent fee combined with a zero-sat dust anchor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_vout: Option<u32>,
    pub ctv_hash_hex: String,
    pub ctv_leaf_script_hex: String,
    pub p2mr_program_hex: String,
    pub p2mr_control_block_hex: String,
    pub reward_manifest_sha256_hex: String,
    pub payout_policy_manifest_sha256_hex: String,
    pub outputs: Vec<CtvFanoutOutput>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CtvFanoutManifest {
    pub schema: String,
    pub precommitment: CtvFanoutPrecommitment,
    pub precommitment_sha256_hex: String,
    pub commitment_witness_leaf_hex: String,
    pub parent_coinbase_txid: String,
    pub parent_coinbase_tx_hex: String,
    pub parent_coinbase_vout: u32,
    pub covenant_output_value_sats: u64,
    pub covenant_script_pubkey_hex: String,
    pub fanout_tx_hex: String,
    pub fanout_txid: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CtvFanoutManifestSet {
    pub schema: String,
    pub block_height: u64,
    pub coinbase_value_sats: u64,
    pub settlement_mode: SettlementMode,
    pub reward_manifest_sha256_hex: String,
    pub payout_policy_manifest_sha256_hex: String,
    pub parent_coinbase_txid: String,
    pub fanout_count: u32,
    pub fanout_output_sum_sats: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub fanout_fee_sats: u64,
    pub covenant_output_value_sats: u64,
    pub manifests: Vec<CtvFanoutManifest>,
}

pub fn build_ctv_fanout_manifest(
    input: CtvFanoutManifestInput,
) -> Result<CtvFanoutManifest, PrismError> {
    if input.block_height > u64::from(u32::MAX) {
        return Err(invalid_ctv_manifest(
            "block height must fit fanout lock_time",
        ));
    }
    if input.settlement_mode == SettlementMode::DirectCoinbase {
        return Err(invalid_ctv_manifest(
            "direct_coinbase mode cannot include a CTV fanout manifest",
        ));
    }
    validate_chunk_identity(input.chunk_index, input.chunk_count)?;
    validate_sha256_hex(
        &input.reward_manifest_sha256_hex,
        "reward_manifest_sha256_hex",
    )?;
    validate_sha256_hex(
        &input.payout_policy_manifest_sha256_hex,
        "payout_policy_manifest_sha256_hex",
    )?;

    let parent = parse_transaction_hex(&input.parent_coinbase_tx_hex, "parent_coinbase_tx_hex")?;
    ensure_coinbase_shape(&parent)?;
    let fanout_template =
        parse_transaction_hex(&input.fanout_tx_template_hex, "fanout_tx_template_hex")?;
    let fanout = parse_transaction_hex(&input.fanout_tx_hex, "fanout_tx_hex")?;
    validate_fanout_template(&fanout_template)?;
    let covenant_output = parent
        .outputs
        .get(input.parent_coinbase_vout as usize)
        .ok_or_else(|| invalid_ctv_manifest("parent coinbase vout is out of range"))?;
    let ctv_hash = default_ctv_hash_for_parsed_tx(&fanout_template, QBIT_CTV_INPUT_INDEX as usize)?;
    let final_ctv_hash = default_ctv_hash_for_parsed_tx(&fanout, QBIT_CTV_INPUT_INDEX as usize)?;
    if final_ctv_hash != ctv_hash {
        return Err(invalid_ctv_manifest(
            "fanout transaction does not match committed CTV template",
        ));
    }
    let ctv_leaf_script = ctv_leaf_script(&ctv_hash);
    let p2mr_program = p2mr_tapleaf_hash(&ctv_leaf_script);
    let covenant_script_pubkey = p2mr_script_pubkey(p2mr_program);
    let fanout_input = fanout
        .inputs
        .get(QBIT_CTV_INPUT_INDEX as usize)
        .ok_or_else(|| invalid_ctv_manifest("fanout transaction must have input 0"))?;
    let expected_sequence = expected_fanout_sequence(input.chunk_index)?;
    if fanout_input.sequence != expected_sequence {
        return Err(invalid_ctv_manifest(
            "fanout sequence must equal the chunk index",
        ));
    }
    let outputs = canonical_fanout_outputs(input.payouts)?;
    let fanout_output_sum_sats = outputs.iter().try_fold(0_u64, |sum, output| {
        sum.checked_add(output.amount_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout output sum overflowed"))
    })?;
    let fanout_fee_sats = outputs.iter().try_fold(0_u64, |sum, output| {
        sum.checked_add(output.fee_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout fee sum overflowed"))
    })?;
    let anchor_vout = anchor_vout_for_fee(outputs.len(), fanout_fee_sats)?;
    let precommitment = CtvFanoutPrecommitment {
        schema: CTV_FANOUT_PRECOMMITMENT_SCHEMA.to_string(),
        block_height: input.block_height,
        chunk_index: input.chunk_index,
        chunk_count: input.chunk_count,
        coinbase_value_sats: input.coinbase_value_sats,
        settlement_mode: input.settlement_mode,
        covenant_output_value_sats: covenant_output.amount_sats,
        covenant_script_pubkey_hex: hex::encode(covenant_script_pubkey),
        fanout_tx_template_hex: normalize_hex(&input.fanout_tx_template_hex)?,
        fanout_input_index: QBIT_CTV_INPUT_INDEX,
        fanout_sequence: fanout_input.sequence,
        fanout_lock_time: fanout.lock_time,
        fanout_output_sum_sats,
        fanout_fee_sats,
        anchor_vout,
        ctv_hash_hex: hex::encode(ctv_hash),
        ctv_leaf_script_hex: hex::encode(&ctv_leaf_script),
        p2mr_program_hex: hex::encode(p2mr_program),
        p2mr_control_block_hex: hex::encode([QBIT_P2MR_CONTROL_BLOCK]),
        reward_manifest_sha256_hex: input.reward_manifest_sha256_hex.to_ascii_lowercase(),
        payout_policy_manifest_sha256_hex: input
            .payout_policy_manifest_sha256_hex
            .to_ascii_lowercase(),
        outputs,
    };
    let precommitment_sha256_hex = sha256_hex(&canonical_precommitment_bytes(&precommitment)?);
    let commitment_witness_leaf_hex = commitment_witness_leaf_hex(&precommitment_sha256_hex)?;

    let manifest = CtvFanoutManifest {
        schema: CTV_FANOUT_MANIFEST_SCHEMA.to_string(),
        precommitment,
        precommitment_sha256_hex,
        commitment_witness_leaf_hex,
        parent_coinbase_txid: parent.txid_hex(),
        parent_coinbase_tx_hex: normalize_hex(&input.parent_coinbase_tx_hex)?,
        parent_coinbase_vout: input.parent_coinbase_vout,
        covenant_output_value_sats: covenant_output.amount_sats,
        covenant_script_pubkey_hex: hex::encode(covenant_script_pubkey),
        fanout_tx_hex: normalize_hex(&input.fanout_tx_hex)?,
        fanout_txid: fanout.txid_hex(),
    };
    verify_ctv_fanout_manifest_structure(&manifest)?;
    Ok(manifest)
}

/// Build the pre-mining commitment material for one CTV fanout chunk.
///
/// This does not need the future coinbase txid. qbit's CTV hash commits to the
/// fanout template with a null placeholder prevout, so live job construction can
/// mine the returned `covenant_p2mr_program_hex` as a coinbase output and place
/// `commitment_witness_leaf_hex` in the witness commitment before any miner
/// hashes the job.
pub fn prepare_ctv_fanout_precommitment(
    input: CtvFanoutPrecommitmentInput,
) -> Result<PreparedCtvFanout, PrismError> {
    if input.block_height > u64::from(u32::MAX) {
        return Err(invalid_ctv_manifest(
            "block height must fit fanout lock_time",
        ));
    }
    if input.settlement_mode == SettlementMode::DirectCoinbase {
        return Err(invalid_ctv_manifest(
            "direct_coinbase mode cannot include a CTV fanout precommitment",
        ));
    }
    validate_chunk_identity(input.chunk_index, input.chunk_count)?;
    validate_sha256_hex(
        &input.reward_manifest_sha256_hex,
        "reward_manifest_sha256_hex",
    )?;
    validate_sha256_hex(
        &input.payout_policy_manifest_sha256_hex,
        "payout_policy_manifest_sha256_hex",
    )?;

    let outputs = canonical_fanout_outputs(input.payouts)?;
    let fanout_output_sum_sats = outputs.iter().try_fold(0_u64, |sum, output| {
        sum.checked_add(output.amount_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout output sum overflowed"))
    })?;
    let fanout_fee_sats = outputs.iter().try_fold(0_u64, |sum, output| {
        sum.checked_add(output.fee_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout fee sum overflowed"))
    })?;
    let covenant_output_value_sats = fanout_output_sum_sats
        .checked_add(fanout_fee_sats)
        .ok_or_else(|| invalid_ctv_manifest("fanout covenant value overflowed"))?;
    let sequence = expected_fanout_sequence(input.chunk_index)?;
    let fanout_tx_template_hex = unsigned_fanout_tx_hex(
        &"00".repeat(32),
        0,
        &outputs,
        fanout_has_cpfp_anchor(fanout_fee_sats),
        input.block_height as u32,
        sequence,
    )?;
    let ctv_hash_hex =
        default_ctv_hash_hex(&fanout_tx_template_hex, QBIT_CTV_INPUT_INDEX as usize)?;
    let ctv_leaf_script_hex = ctv_leaf_script_hex(&ctv_hash_hex)?;
    let p2mr_program_hex = p2mr_program_for_ctv_leaf_hex(&ctv_leaf_script_hex)?;
    let covenant_script_pubkey_hex = hex::encode(p2mr_script_pubkey(decode_program_hex(
        &p2mr_program_hex,
        "p2mr_program_hex",
    )?));
    let precommitment = CtvFanoutPrecommitment {
        schema: CTV_FANOUT_PRECOMMITMENT_SCHEMA.to_string(),
        block_height: input.block_height,
        chunk_index: input.chunk_index,
        chunk_count: input.chunk_count,
        coinbase_value_sats: input.coinbase_value_sats,
        settlement_mode: input.settlement_mode,
        covenant_output_value_sats,
        covenant_script_pubkey_hex,
        fanout_tx_template_hex,
        fanout_input_index: QBIT_CTV_INPUT_INDEX,
        fanout_sequence: sequence,
        fanout_lock_time: input.block_height as u32,
        fanout_output_sum_sats,
        fanout_fee_sats,
        anchor_vout: anchor_vout_for_fee(outputs.len(), fanout_fee_sats)?,
        ctv_hash_hex,
        ctv_leaf_script_hex,
        p2mr_program_hex: p2mr_program_hex.clone(),
        p2mr_control_block_hex: hex::encode([QBIT_P2MR_CONTROL_BLOCK]),
        reward_manifest_sha256_hex: input.reward_manifest_sha256_hex.to_ascii_lowercase(),
        payout_policy_manifest_sha256_hex: input
            .payout_policy_manifest_sha256_hex
            .to_ascii_lowercase(),
        outputs,
    };
    verify_precommitment(&precommitment)?;
    let precommitment_sha256_hex = sha256_hex(&canonical_precommitment_bytes(&precommitment)?);
    let commitment_witness_leaf_hex = commitment_witness_leaf_hex(&precommitment_sha256_hex)?;
    Ok(PreparedCtvFanout {
        covenant_recipient_id: format!("ctv-fanout-{}", precommitment.chunk_index),
        covenant_order_key: format!("ctv-fanout-{:08}", precommitment.chunk_index),
        covenant_p2mr_program_hex: p2mr_program_hex,
        covenant_output_value_sats: precommitment.covenant_output_value_sats,
        precommitment,
        precommitment_sha256_hex,
        commitment_witness_leaf_hex,
    })
}

/// Finalize a prepared fanout after the mined coinbase outpoint is known.
pub fn build_ctv_fanout_manifest_from_precommitment(
    precommitment: CtvFanoutPrecommitment,
    parent_coinbase_tx_hex: String,
    parent_coinbase_vout: u32,
) -> Result<CtvFanoutManifest, PrismError> {
    verify_precommitment(&precommitment)?;
    let parent = parse_transaction_hex(&parent_coinbase_tx_hex, "parent_coinbase_tx_hex")?;
    ensure_coinbase_shape(&parent)?;
    let parent_coinbase_txid = parent.txid_hex();
    let fanout_tx_hex = witnessed_fanout_tx_hex(
        &parent_coinbase_txid,
        parent_coinbase_vout,
        &precommitment.outputs,
        precommitment.anchor_vout.is_some(),
        precommitment.fanout_lock_time,
        precommitment.fanout_sequence,
        &hex::decode(&precommitment.ctv_leaf_script_hex).map_err(|err| {
            invalid_ctv_manifest(format!("ctv_leaf_script_hex must be hex: {err}"))
        })?,
    )?;
    build_ctv_fanout_manifest(CtvFanoutManifestInput {
        block_height: precommitment.block_height,
        chunk_index: precommitment.chunk_index,
        chunk_count: precommitment.chunk_count,
        coinbase_value_sats: precommitment.coinbase_value_sats,
        settlement_mode: precommitment.settlement_mode.clone(),
        fanout_tx_template_hex: precommitment.fanout_tx_template_hex.clone(),
        parent_coinbase_tx_hex,
        parent_coinbase_vout,
        fanout_tx_hex,
        reward_manifest_sha256_hex: precommitment.reward_manifest_sha256_hex.clone(),
        payout_policy_manifest_sha256_hex: precommitment.payout_policy_manifest_sha256_hex.clone(),
        payouts: precommitment
            .outputs
            .iter()
            .map(|output| CtvFanoutPayout {
                recipient_id: output.recipient_id.clone(),
                order_key: output.order_key.clone(),
                p2mr_program_hex: output.p2mr_program_hex.clone(),
                gross_amount_sats: output.gross_amount_sats,
                fee_sats: output.fee_sats,
                amount_sats: output.amount_sats,
            })
            .collect(),
    })
}

pub fn build_ctv_fanout_manifest_set(
    mut manifests: Vec<CtvFanoutManifest>,
) -> Result<CtvFanoutManifestSet, PrismError> {
    if manifests.is_empty() {
        return Err(invalid_ctv_manifest("fanout manifest set is empty"));
    }
    manifests.sort_by_key(|manifest| manifest.precommitment.chunk_index);
    let first = &manifests[0].precommitment;
    let fanout_output_sum_sats = manifests.iter().try_fold(0_u64, |sum, manifest| {
        sum.checked_add(manifest.precommitment.fanout_output_sum_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout set output sum overflowed"))
    })?;
    let fanout_fee_sats = manifests.iter().try_fold(0_u64, |sum, manifest| {
        sum.checked_add(manifest.precommitment.fanout_fee_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout set fee sum overflowed"))
    })?;
    let covenant_output_value_sats = manifests.iter().try_fold(0_u64, |sum, manifest| {
        sum.checked_add(manifest.covenant_output_value_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout set covenant value overflowed"))
    })?;
    let set = CtvFanoutManifestSet {
        schema: CTV_FANOUT_MANIFEST_SET_SCHEMA.to_string(),
        block_height: first.block_height,
        coinbase_value_sats: first.coinbase_value_sats,
        settlement_mode: first.settlement_mode.clone(),
        reward_manifest_sha256_hex: first.reward_manifest_sha256_hex.clone(),
        payout_policy_manifest_sha256_hex: first.payout_policy_manifest_sha256_hex.clone(),
        parent_coinbase_txid: manifests[0].parent_coinbase_txid.clone(),
        fanout_count: u32::try_from(manifests.len())
            .map_err(|_| invalid_ctv_manifest("fanout manifest count exceeds uint32"))?,
        fanout_output_sum_sats,
        fanout_fee_sats,
        covenant_output_value_sats,
        manifests,
    };
    verify_ctv_fanout_manifest_set(&set)?;
    Ok(set)
}

/// Verify a manifest is **internally consistent**.
///
/// This recomputes the CTV hash, P2MR covenant program, witness leaf, and
/// precommitment hash from the manifest's own bytes and checks every field
/// against them — fanout matches the committed template, the parent coinbase
/// output is the covenant for that template, the payouts plus any built-in fee
/// equal the covenant value, CPFP anchors appear only on zero-fee fanouts,
/// output ordering is canonical, and so on.
///
/// **It does not prove the parent coinbase was mined.** Every input here is
/// supplied by whoever assembled the manifest, so a fully self-consistent
/// manifest can be fabricated around a coinbase that exists on no chain.
/// Returning `Ok` is **not** proof that a miner will be paid. A consumer that
/// intends to trust the manifest must additionally verify on-chain inclusion,
/// proof-of-work, height, and maturity, and check the commitment leaf against
/// the real mined coinbase via [`verify_ctv_fanout_manifest_commitment_leaf`].
/// See the module-level documentation for the full trust boundary.
pub fn verify_ctv_fanout_manifest_structure(
    manifest: &CtvFanoutManifest,
) -> Result<(), PrismError> {
    if manifest.schema != CTV_FANOUT_MANIFEST_SCHEMA {
        return Err(invalid_ctv_manifest("unexpected fanout manifest schema"));
    }
    let precommitment = &manifest.precommitment;
    verify_precommitment(precommitment)?;
    validate_canonical_sha256_hex(
        &manifest.precommitment_sha256_hex,
        "manifest precommitment_sha256_hex",
    )?;
    validate_canonical_sha256_hex(
        &manifest.commitment_witness_leaf_hex,
        "manifest commitment_witness_leaf_hex",
    )?;
    validate_canonical_sha256_hex(
        &manifest.parent_coinbase_txid,
        "manifest parent_coinbase_txid",
    )?;
    validate_canonical_hex(
        &manifest.parent_coinbase_tx_hex,
        "manifest parent_coinbase_tx_hex",
    )?;
    validate_canonical_hex(
        &manifest.covenant_script_pubkey_hex,
        "manifest covenant_script_pubkey_hex",
    )?;
    validate_canonical_hex(&manifest.fanout_tx_hex, "manifest fanout_tx_hex")?;
    validate_canonical_sha256_hex(&manifest.fanout_txid, "manifest fanout_txid")?;
    if precommitment.settlement_mode == SettlementMode::DirectCoinbase {
        return Err(invalid_ctv_manifest(
            "direct_coinbase mode cannot include a CTV fanout manifest",
        ));
    }
    validate_chunk_identity(precommitment.chunk_index, precommitment.chunk_count)?;
    let recomputed_precommitment_sha256 =
        sha256_hex(&canonical_precommitment_bytes(precommitment)?);
    if manifest.precommitment_sha256_hex != recomputed_precommitment_sha256 {
        return Err(invalid_ctv_manifest("fanout precommitment hash mismatch"));
    }
    let expected_commitment_witness_leaf =
        commitment_witness_leaf_hex(&manifest.precommitment_sha256_hex)?;
    if manifest.commitment_witness_leaf_hex != expected_commitment_witness_leaf {
        return Err(invalid_ctv_manifest(
            "fanout commitment witness leaf mismatch",
        ));
    }
    if precommitment.fanout_input_index != QBIT_CTV_INPUT_INDEX {
        return Err(invalid_ctv_manifest(
            "only fanout input index 0 is supported",
        ));
    }
    if precommitment.block_height > u64::from(u32::MAX) {
        return Err(invalid_ctv_manifest(
            "block height must fit fanout lock_time",
        ));
    }
    validate_sha256_hex(
        &precommitment.reward_manifest_sha256_hex,
        "reward_manifest_sha256_hex",
    )?;
    validate_sha256_hex(
        &precommitment.payout_policy_manifest_sha256_hex,
        "payout_policy_manifest_sha256_hex",
    )?;
    let expected_control = hex::encode([QBIT_P2MR_CONTROL_BLOCK]);
    if precommitment.p2mr_control_block_hex != expected_control {
        return Err(invalid_ctv_manifest("P2MR control block mismatch"));
    }

    let parent = parse_transaction_hex(
        &manifest.parent_coinbase_tx_hex,
        "manifest parent_coinbase_tx_hex",
    )?;
    ensure_coinbase_shape(&parent)?;
    if parent.txid_hex() != manifest.parent_coinbase_txid {
        return Err(invalid_ctv_manifest("parent coinbase txid mismatch"));
    }
    let covenant_output = parent
        .outputs
        .get(manifest.parent_coinbase_vout as usize)
        .ok_or_else(|| invalid_ctv_manifest("parent coinbase vout is out of range"))?;
    if covenant_output.amount_sats != manifest.covenant_output_value_sats {
        return Err(invalid_ctv_manifest("covenant output value mismatch"));
    }
    if covenant_output.amount_sats != precommitment.covenant_output_value_sats {
        return Err(invalid_ctv_manifest(
            "parent covenant value does not match precommitment",
        ));
    }
    if hex::encode(&covenant_output.script_pubkey) != manifest.covenant_script_pubkey_hex {
        return Err(invalid_ctv_manifest("covenant scriptPubKey mismatch"));
    }
    if manifest.covenant_script_pubkey_hex != precommitment.covenant_script_pubkey_hex {
        return Err(invalid_ctv_manifest(
            "parent covenant scriptPubKey does not match precommitment",
        ));
    }

    let fanout_template = parse_transaction_hex(
        &precommitment.fanout_tx_template_hex,
        "manifest fanout_tx_template_hex",
    )?;
    validate_fanout_template(&fanout_template)?;
    let fanout = parse_transaction_hex(&manifest.fanout_tx_hex, "manifest fanout_tx_hex")?;
    if fanout.txid_hex() != manifest.fanout_txid {
        return Err(invalid_ctv_manifest("fanout txid mismatch"));
    }
    if fanout.inputs.len() != 1 {
        return Err(invalid_ctv_manifest(
            "CTV fanout transaction must spend exactly one input",
        ));
    }
    let fanout_input = &fanout.inputs[QBIT_CTV_INPUT_INDEX as usize];
    if !fanout_input.script_sig.is_empty() {
        return Err(invalid_ctv_manifest("fanout input scriptSig must be empty"));
    }
    if fanout_input.prev_txid_hex != manifest.parent_coinbase_txid {
        return Err(invalid_ctv_manifest("fanout prevout txid mismatch"));
    }
    if fanout_input.prev_vout != manifest.parent_coinbase_vout {
        return Err(invalid_ctv_manifest("fanout prevout vout mismatch"));
    }
    if fanout_input.sequence != precommitment.fanout_sequence {
        return Err(invalid_ctv_manifest("fanout sequence mismatch"));
    }
    if fanout_input.sequence != expected_fanout_sequence(precommitment.chunk_index)? {
        return Err(invalid_ctv_manifest(
            "fanout sequence must equal the chunk index",
        ));
    }
    if fanout.lock_time != precommitment.fanout_lock_time {
        return Err(invalid_ctv_manifest("fanout lock_time mismatch"));
    }
    if precommitment.fanout_lock_time != precommitment.block_height as u32 {
        return Err(invalid_ctv_manifest(
            "fanout lock_time must equal block height for unique CTV context",
        ));
    }

    let template_ctv_hash = default_ctv_hash_for_parsed_tx(
        &fanout_template,
        precommitment.fanout_input_index as usize,
    )?;
    if hex::encode(template_ctv_hash) != precommitment.ctv_hash_hex {
        return Err(invalid_ctv_manifest("precommitment CTV hash mismatch"));
    }
    let ctv_hash =
        default_ctv_hash_for_parsed_tx(&fanout, precommitment.fanout_input_index as usize)?;
    if ctv_hash != template_ctv_hash {
        return Err(invalid_ctv_manifest("CTV hash mismatch"));
    }
    let ctv_leaf_script = ctv_leaf_script(&ctv_hash);
    if hex::encode(&ctv_leaf_script) != precommitment.ctv_leaf_script_hex {
        return Err(invalid_ctv_manifest("CTV leaf script mismatch"));
    }
    let p2mr_program = p2mr_tapleaf_hash(&ctv_leaf_script);
    if hex::encode(p2mr_program) != precommitment.p2mr_program_hex {
        return Err(invalid_ctv_manifest("P2MR program mismatch"));
    }
    let covenant_script_pubkey = p2mr_script_pubkey(p2mr_program);
    if hex::encode(covenant_script_pubkey) != manifest.covenant_script_pubkey_hex {
        return Err(invalid_ctv_manifest(
            "covenant scriptPubKey does not match CTV leaf",
        ));
    }
    verify_fanout_witness(&fanout, &ctv_leaf_script)?;

    let has_cpfp_anchor = precommitment.anchor_vout.is_some();
    let expected_output_count = if has_cpfp_anchor {
        precommitment.outputs.len() + 1
    } else {
        precommitment.outputs.len()
    };
    if fanout.outputs.len() != expected_output_count {
        return Err(invalid_ctv_manifest(if has_cpfp_anchor {
            "fanout must have exactly the payouts plus one fee anchor output"
        } else {
            "built-in-fee fanout must have exactly the payout outputs"
        }));
    }
    let mut fanout_output_sum_sats = 0_u64;
    let mut fanout_fee_sats = 0_u64;
    for (index, (output, payout)) in fanout
        .outputs
        .iter()
        .zip(&precommitment.outputs)
        .enumerate()
    {
        fanout_output_sum_sats = fanout_output_sum_sats
            .checked_add(output.amount_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout output sum overflowed"))?;
        fanout_fee_sats = fanout_fee_sats
            .checked_add(payout.fee_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout fee sum overflowed"))?;
        if payout.vout != index as u32 {
            return Err(invalid_ctv_manifest(format!(
                "fanout output {index} vout mismatch"
            )));
        }
        if output.amount_sats != payout.amount_sats {
            return Err(invalid_ctv_manifest(format!(
                "fanout output {index} amount mismatch"
            )));
        }
        let expected_program =
            decode_program_hex(&payout.p2mr_program_hex, "payout p2mr_program_hex")?;
        let expected_script = p2mr_script_pubkey(expected_program);
        if output.script_pubkey != expected_script {
            return Err(invalid_ctv_manifest(format!(
                "fanout output {index} scriptPubKey mismatch"
            )));
        }
        if hex::encode(expected_script) != payout.script_pubkey_hex {
            return Err(invalid_ctv_manifest(format!(
                "fanout output {index} manifest scriptPubKey mismatch"
            )));
        }
    }
    if fanout_output_sum_sats != precommitment.fanout_output_sum_sats {
        return Err(invalid_ctv_manifest("fanout output sum mismatch"));
    }
    if fanout_fee_sats != precommitment.fanout_fee_sats {
        return Err(invalid_ctv_manifest("fanout fee sum mismatch"));
    }
    if let Some(anchor_vout) = precommitment.anchor_vout {
        // After the payouts the legacy zero-fee fanout carries one keyless P2A
        // ephemeral anchor (value 0). It lets the broadcaster CPFP-bump the fee
        // without holding a key or diverting any miner value.
        let anchor = &fanout.outputs[precommitment.outputs.len()];
        ensure_fee_anchor_output(anchor, anchor_vout, precommitment.outputs.len())?;
    }
    let funded_value_sats = fanout_output_sum_sats
        .checked_add(fanout_fee_sats)
        .ok_or_else(|| invalid_ctv_manifest("fanout funded value overflowed"))?;
    if funded_value_sats != manifest.covenant_output_value_sats {
        return Err(invalid_ctv_manifest(
            "fanout payouts plus fee do not equal covenant output value",
        ));
    }
    Ok(())
}

pub fn verify_ctv_fanout_manifest_set(set: &CtvFanoutManifestSet) -> Result<(), PrismError> {
    if set.schema != CTV_FANOUT_MANIFEST_SET_SCHEMA {
        return Err(invalid_ctv_manifest(
            "unexpected fanout manifest set schema",
        ));
    }
    if set.manifests.is_empty() {
        return Err(invalid_ctv_manifest("fanout manifest set is empty"));
    }
    if set.fanout_count as usize != set.manifests.len() {
        return Err(invalid_ctv_manifest("fanout manifest set count mismatch"));
    }
    if set.settlement_mode == SettlementMode::DirectCoinbase {
        return Err(invalid_ctv_manifest(
            "direct_coinbase mode cannot include a CTV fanout manifest set",
        ));
    }
    validate_canonical_sha256_hex(
        &set.reward_manifest_sha256_hex,
        "fanout set reward_manifest_sha256_hex",
    )?;
    validate_canonical_sha256_hex(
        &set.payout_policy_manifest_sha256_hex,
        "fanout set payout_policy_manifest_sha256_hex",
    )?;
    validate_canonical_sha256_hex(&set.parent_coinbase_txid, "fanout set parent_coinbase_txid")?;

    let mut parent_vouts = BTreeSet::new();
    let mut ctv_hashes = BTreeSet::new();
    let mut fanout_txids = BTreeSet::new();
    let mut commitment_leaves = BTreeSet::new();
    let mut fanout_output_sum_sats = 0_u64;
    let mut fanout_fee_sats = 0_u64;
    let mut covenant_output_value_sats = 0_u64;
    for (expected_index, manifest) in set.manifests.iter().enumerate() {
        verify_ctv_fanout_manifest_structure(manifest)?;
        let precommitment = &manifest.precommitment;
        if precommitment.chunk_index != expected_index as u32 {
            return Err(invalid_ctv_manifest(
                "fanout manifest set chunks must be contiguous and canonical",
            ));
        }
        if precommitment.chunk_count != set.fanout_count {
            return Err(invalid_ctv_manifest("fanout chunk count mismatch"));
        }
        if precommitment.block_height != set.block_height {
            return Err(invalid_ctv_manifest("fanout set block height mismatch"));
        }
        if precommitment.coinbase_value_sats != set.coinbase_value_sats {
            return Err(invalid_ctv_manifest("fanout set coinbase value mismatch"));
        }
        if precommitment.settlement_mode != set.settlement_mode {
            return Err(invalid_ctv_manifest("fanout set settlement mode mismatch"));
        }
        if precommitment.reward_manifest_sha256_hex != set.reward_manifest_sha256_hex {
            return Err(invalid_ctv_manifest("fanout set reward manifest mismatch"));
        }
        if precommitment.payout_policy_manifest_sha256_hex != set.payout_policy_manifest_sha256_hex
        {
            return Err(invalid_ctv_manifest(
                "fanout set payout policy manifest mismatch",
            ));
        }
        if manifest.parent_coinbase_txid != set.parent_coinbase_txid {
            return Err(invalid_ctv_manifest("fanout set parent coinbase mismatch"));
        }
        if !parent_vouts.insert(manifest.parent_coinbase_vout) {
            return Err(invalid_ctv_manifest(
                "duplicate fanout parent coinbase vout",
            ));
        }
        if !ctv_hashes.insert(precommitment.ctv_hash_hex.clone()) {
            return Err(invalid_ctv_manifest("duplicate fanout CTV hash"));
        }
        if !fanout_txids.insert(manifest.fanout_txid.clone()) {
            return Err(invalid_ctv_manifest("duplicate fanout txid"));
        }
        if !commitment_leaves.insert(manifest.commitment_witness_leaf_hex.clone()) {
            return Err(invalid_ctv_manifest(
                "duplicate fanout commitment witness leaf",
            ));
        }
        fanout_output_sum_sats = fanout_output_sum_sats
            .checked_add(precommitment.fanout_output_sum_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout set output sum overflowed"))?;
        fanout_fee_sats = fanout_fee_sats
            .checked_add(precommitment.fanout_fee_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout set fee sum overflowed"))?;
        covenant_output_value_sats = covenant_output_value_sats
            .checked_add(manifest.covenant_output_value_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout set covenant value overflowed"))?;
    }
    if fanout_output_sum_sats != set.fanout_output_sum_sats {
        return Err(invalid_ctv_manifest("fanout set output sum mismatch"));
    }
    if fanout_fee_sats != set.fanout_fee_sats {
        return Err(invalid_ctv_manifest("fanout set fee sum mismatch"));
    }
    if covenant_output_value_sats != set.covenant_output_value_sats {
        return Err(invalid_ctv_manifest("fanout set covenant value mismatch"));
    }
    let funded_value_sats = fanout_output_sum_sats
        .checked_add(fanout_fee_sats)
        .ok_or_else(|| invalid_ctv_manifest("fanout set funded value overflowed"))?;
    if funded_value_sats != covenant_output_value_sats {
        return Err(invalid_ctv_manifest(
            "fanout set payouts plus fee do not equal covenant value",
        ));
    }
    Ok(())
}

/// Verify structural consistency ([`verify_ctv_fanout_manifest_structure`])
/// **and** that the manifest's commitment leaf appears in the audit commitment
/// leaves bound by the coinbase witness nonce.
///
/// This is the pre-mining commitment check, but it only anchors trust as far as
/// the supplied leaves do. The caller **must** pass the audit commitment leaves
/// proven from the *actual mined coinbase* of a proof-of-work-valid, mature
/// block at `manifest.precommitment.block_height`, having independently
/// verified that block's inclusion, PoW, height, maturity, and witness nonce
/// commitment root. Passing attacker-chosen leaves provides no security. This
/// function neither sees nor validates the chain.
pub fn verify_ctv_fanout_manifest_commitment_leaf(
    manifest: &CtvFanoutManifest,
    audit_commitment_leaves_hex: &[String],
) -> Result<(), PrismError> {
    verify_ctv_fanout_manifest_structure(manifest)?;
    let expected = manifest.commitment_witness_leaf_hex.to_ascii_lowercase();
    if !audit_commitment_leaves_hex
        .iter()
        .any(|leaf| leaf.to_ascii_lowercase() == expected)
    {
        return Err(invalid_ctv_manifest(
            "fanout precommitment witness leaf is missing",
        ));
    }
    Ok(())
}

pub fn default_ctv_hash_hex(tx_hex: &str, input_index: usize) -> Result<String, PrismError> {
    let tx = parse_transaction_hex(tx_hex, "tx_hex")?;
    Ok(hex::encode(default_ctv_hash_for_parsed_tx(
        &tx,
        input_index,
    )?))
}

pub fn ctv_leaf_script_hex(ctv_hash_hex: &str) -> Result<String, PrismError> {
    let ctv_hash = decode_hash_hex(ctv_hash_hex, "ctv_hash_hex")?;
    Ok(hex::encode(ctv_leaf_script(&ctv_hash)))
}

pub fn p2mr_program_for_ctv_leaf_hex(ctv_leaf_script_hex: &str) -> Result<String, PrismError> {
    let leaf_script = hex::decode(ctv_leaf_script_hex)
        .map_err(|err| invalid_ctv_manifest(format!("invalid ctv_leaf_script_hex: {err}")))?;
    Ok(hex::encode(p2mr_tapleaf_hash(&leaf_script)))
}

/// Canonical byte encoding of a precommitment, hashed to produce
/// `precommitment_sha256_hex` and, in turn, the on-chain commitment leaf.
///
/// The encoding is `serde_json` over [`CtvFanoutPrecommitment`]: fields appear
/// in struct-declaration order, there are no maps (so no key-ordering
/// ambiguity), and all amounts are JSON integers. An independent verifier in
/// another language MUST reproduce this exact byte layout to re-derive the
/// commitment leaf; the `default_ctv_hash_matches_qbit_reference_vector` and
/// `ctv_fanout_precommitment_hash_matches_golden_vector` tests pin the wire
/// format. A future revision may replace this with an explicit length-prefixed
/// encoding (like the CTV preimage) to remove the JSON dependency entirely.
pub fn canonical_ctv_fanout_precommitment_bytes(
    precommitment: &CtvFanoutPrecommitment,
) -> Result<Vec<u8>, PrismError> {
    canonical_precommitment_bytes(precommitment)
}

/// Canonical byte encoding of a complete fanout manifest set. This is the
/// content-addressed artifact should publish and mirror for recovery.
pub fn canonical_ctv_fanout_manifest_set_bytes(
    set: &CtvFanoutManifestSet,
) -> Result<Vec<u8>, PrismError> {
    verify_ctv_fanout_manifest_set(set)?;
    serde_json::to_vec(set).map_err(PrismError::from)
}

pub fn ctv_fanout_manifest_set_sha256_hex(
    set: &CtvFanoutManifestSet,
) -> Result<String, PrismError> {
    Ok(sha256_hex(&canonical_ctv_fanout_manifest_set_bytes(set)?))
}

fn verify_precommitment(precommitment: &CtvFanoutPrecommitment) -> Result<(), PrismError> {
    if precommitment.schema != CTV_FANOUT_PRECOMMITMENT_SCHEMA {
        return Err(invalid_ctv_manifest(
            "unexpected fanout precommitment schema",
        ));
    }
    validate_canonical_hex(
        &precommitment.covenant_script_pubkey_hex,
        "precommitment covenant_script_pubkey_hex",
    )?;
    validate_canonical_hex(
        &precommitment.fanout_tx_template_hex,
        "precommitment fanout_tx_template_hex",
    )?;
    validate_canonical_sha256_hex(&precommitment.ctv_hash_hex, "precommitment ctv_hash_hex")?;
    validate_canonical_hex(
        &precommitment.ctv_leaf_script_hex,
        "precommitment ctv_leaf_script_hex",
    )?;
    let precommitment_p2mr_program = canonical_p2mr_program_hex(
        &precommitment.p2mr_program_hex,
        "precommitment p2mr_program_hex",
    )?;
    if precommitment.p2mr_program_hex != precommitment_p2mr_program {
        return Err(invalid_ctv_manifest(
            "precommitment p2mr_program_hex must be lowercase canonical hex",
        ));
    }
    validate_canonical_hex(
        &precommitment.p2mr_control_block_hex,
        "precommitment p2mr_control_block_hex",
    )?;
    validate_canonical_sha256_hex(
        &precommitment.reward_manifest_sha256_hex,
        "precommitment reward_manifest_sha256_hex",
    )?;
    validate_canonical_sha256_hex(
        &precommitment.payout_policy_manifest_sha256_hex,
        "precommitment payout_policy_manifest_sha256_hex",
    )?;
    validate_chunk_identity(precommitment.chunk_index, precommitment.chunk_count)?;
    if precommitment.fanout_sequence != expected_fanout_sequence(precommitment.chunk_index)? {
        return Err(invalid_ctv_manifest(
            "fanout sequence must equal the chunk index",
        ));
    }
    if precommitment.fanout_lock_time != precommitment.block_height as u32 {
        return Err(invalid_ctv_manifest(
            "fanout lock_time must equal block height for unique CTV context",
        ));
    }
    if precommitment.outputs.is_empty() {
        return Err(invalid_ctv_manifest("fanout precommitment has no outputs"));
    }
    let mut expected_sum = 0_u64;
    let mut expected_fee_sats = 0_u64;
    let mut seen = BTreeSet::new();
    for (index, output) in precommitment.outputs.iter().enumerate() {
        if output.vout != index as u32 {
            return Err(invalid_ctv_manifest(format!(
                "fanout output {index} vout mismatch"
            )));
        }
        if output.amount_sats == 0 {
            return Err(invalid_ctv_manifest(format!(
                "fanout output {index} amount must be positive"
            )));
        }
        let key = (
            output.order_key.clone(),
            output.recipient_id.clone(),
            canonical_p2mr_program_hex(&output.p2mr_program_hex, "precommitment p2mr_program_hex")?,
        );
        if !seen.insert(key) {
            return Err(invalid_ctv_manifest("duplicate fanout payout recipient"));
        }
        let expected_program =
            decode_program_hex(&output.p2mr_program_hex, "precommitment p2mr_program_hex")?;
        if output.p2mr_program_hex != hex::encode(expected_program) {
            return Err(invalid_ctv_manifest(
                "precommitment p2mr_program_hex must be lowercase canonical hex",
            ));
        }
        if hex::encode(p2mr_script_pubkey(expected_program)) != output.script_pubkey_hex {
            return Err(invalid_ctv_manifest(format!(
                "fanout output {index} scriptPubKey mismatch"
            )));
        }
        expected_sum = expected_sum
            .checked_add(output.amount_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout output sum overflowed"))?;
        expected_fee_sats = expected_fee_sats
            .checked_add(output.fee_sats)
            .ok_or_else(|| invalid_ctv_manifest("fanout fee sum overflowed"))?;
    }
    if expected_sum != precommitment.fanout_output_sum_sats {
        return Err(invalid_ctv_manifest("fanout output sum mismatch"));
    }
    if expected_fee_sats != precommitment.fanout_fee_sats {
        return Err(invalid_ctv_manifest("fanout fee sum mismatch"));
    }
    let funded_value_sats = expected_sum
        .checked_add(expected_fee_sats)
        .ok_or_else(|| invalid_ctv_manifest("fanout funded value overflowed"))?;
    if funded_value_sats != precommitment.covenant_output_value_sats {
        return Err(invalid_ctv_manifest(
            "fanout payouts plus fee do not equal covenant output value",
        ));
    }
    match (precommitment.fanout_fee_sats, precommitment.anchor_vout) {
        (0, Some(anchor_vout)) if anchor_vout as usize == precommitment.outputs.len() => {}
        (0, Some(_)) => {
            return Err(invalid_ctv_manifest(
                "fee anchor vout must follow the payouts",
            ));
        }
        (0, None) => {
            return Err(invalid_ctv_manifest(
                "zero-fee fanout must include a CPFP anchor",
            ));
        }
        (_, Some(_)) => {
            return Err(invalid_ctv_manifest(
                "built-in-fee fanout must not include a CPFP anchor",
            ));
        }
        (_, None) => {}
    }
    Ok(())
}

fn canonical_fanout_outputs(
    mut payouts: Vec<CtvFanoutPayout>,
) -> Result<Vec<CtvFanoutOutput>, PrismError> {
    if payouts.is_empty() {
        return Err(invalid_ctv_manifest("fanout must have at least one payout"));
    }
    for payout in &mut payouts {
        payout.p2mr_program_hex =
            canonical_p2mr_program_hex(&payout.p2mr_program_hex, "payout p2mr_program_hex")?;
    }
    payouts.sort_by(|left, right| {
        (
            &left.order_key,
            &left.recipient_id,
            &left.p2mr_program_hex,
            left.amount_sats,
        )
            .cmp(&(
                &right.order_key,
                &right.recipient_id,
                &right.p2mr_program_hex,
                right.amount_sats,
            ))
    });
    let mut seen = BTreeSet::new();
    payouts
        .into_iter()
        .enumerate()
        .map(|(index, payout)| {
            if payout.amount_sats == 0 {
                return Err(invalid_ctv_manifest(
                    "fanout payout amount must be positive",
                ));
            }
            let gross_amount_sats = if payout.gross_amount_sats == 0 {
                payout.amount_sats
            } else {
                payout.gross_amount_sats
            };
            if payout
                .amount_sats
                .checked_add(payout.fee_sats)
                .ok_or_else(|| invalid_ctv_manifest("fanout payout gross amount overflowed"))?
                != gross_amount_sats
            {
                return Err(invalid_ctv_manifest(
                    "fanout payout gross amount must equal amount plus fee",
                ));
            }
            let p2mr_program =
                decode_program_hex(&payout.p2mr_program_hex, "payout p2mr_program_hex")?;
            let key = (
                payout.order_key.clone(),
                payout.recipient_id.clone(),
                payout.p2mr_program_hex.clone(),
            );
            if !seen.insert(key) {
                return Err(invalid_ctv_manifest("duplicate fanout payout recipient"));
            }
            Ok(CtvFanoutOutput {
                vout: index as u32,
                recipient_id: payout.recipient_id,
                order_key: payout.order_key,
                p2mr_program_hex: payout.p2mr_program_hex,
                script_pubkey_hex: hex::encode(p2mr_script_pubkey(p2mr_program)),
                gross_amount_sats,
                fee_sats: payout.fee_sats,
                amount_sats: payout.amount_sats,
            })
        })
        .collect()
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Require `tx` to have coinbase shape: exactly one input spending the null
/// outpoint (`00..00:0xffffffff`).
///
/// This rejects manifests whose covenant is funded by an ordinary mid-block
/// transaction rather than a coinbase. Only coinbase outputs carry the
/// maturity / no-cheap-reorg guarantee that makes "anyone may broadcast the
/// fanout" safe. It is necessary but **not** sufficient — it does not prove the
/// transaction was actually mined (see the module trust boundary).
fn ensure_coinbase_shape(tx: &ParsedTransaction) -> Result<(), PrismError> {
    if tx.inputs.len() != 1 {
        return Err(invalid_ctv_manifest(
            "parent coinbase must have exactly one input",
        ));
    }
    let input = &tx.inputs[0];
    if input.prev_txid_hex != "00".repeat(32) || input.prev_vout != COINBASE_PREVOUT_VOUT {
        return Err(invalid_ctv_manifest(
            "parent coinbase input must spend the null outpoint",
        ));
    }
    Ok(())
}

fn validate_fanout_template(tx: &ParsedTransaction) -> Result<(), PrismError> {
    if tx.version != CTV_FANOUT_TX_VERSION {
        return Err(invalid_ctv_manifest(
            "fanout template must be a version 3 (TRUC) transaction",
        ));
    }
    if tx.inputs.len() != 1 {
        return Err(invalid_ctv_manifest(
            "fanout template transaction must have exactly one input",
        ));
    }
    if tx.witness_stacks.is_some() {
        return Err(invalid_ctv_manifest(
            "fanout template transaction must not include witness data",
        ));
    }
    if !tx.inputs[QBIT_CTV_INPUT_INDEX as usize]
        .script_sig
        .is_empty()
    {
        return Err(invalid_ctv_manifest(
            "fanout template input scriptSig must be empty",
        ));
    }
    Ok(())
}

fn canonical_precommitment_bytes(
    precommitment: &CtvFanoutPrecommitment,
) -> Result<Vec<u8>, PrismError> {
    serde_json::to_vec(precommitment).map_err(PrismError::from)
}

fn validate_chunk_identity(chunk_index: u32, chunk_count: u32) -> Result<(), PrismError> {
    if chunk_count == 0 {
        return Err(invalid_ctv_manifest("fanout chunk_count must be positive"));
    }
    if chunk_index >= chunk_count {
        return Err(invalid_ctv_manifest(
            "fanout chunk_index must be less than chunk_count",
        ));
    }
    Ok(())
}

fn expected_fanout_sequence(chunk_index: u32) -> Result<u32, PrismError> {
    CTV_FANOUT_SEQUENCE_BASE
        .checked_add(chunk_index)
        .ok_or_else(|| invalid_ctv_manifest("fanout sequence overflowed"))
}

fn canonical_p2mr_program_hex(value: &str, field_name: &str) -> Result<String, PrismError> {
    Ok(hex::encode(decode_program_hex(value, field_name)?))
}

fn validate_canonical_hex(value: &str, field_name: &str) -> Result<(), PrismError> {
    let bytes = hex::decode(value)
        .map_err(|err| invalid_ctv_manifest(format!("{field_name} must be hex: {err}")))?;
    if value != hex::encode(bytes) {
        return Err(invalid_ctv_manifest(format!(
            "{field_name} must be lowercase canonical hex"
        )));
    }
    Ok(())
}

fn p2a_anchor_script_pubkey() -> Vec<u8> {
    hex::decode(P2A_ANCHOR_SCRIPT_PUBKEY_HEX).expect("static P2A script hex")
}

fn fanout_has_cpfp_anchor(fanout_fee_sats: u64) -> bool {
    fanout_fee_sats == 0
}

fn anchor_vout_for_fee(
    output_count: usize,
    fanout_fee_sats: u64,
) -> Result<Option<u32>, PrismError> {
    if !fanout_has_cpfp_anchor(fanout_fee_sats) {
        return Ok(None);
    }
    Ok(Some(u32::try_from(output_count).map_err(|_| {
        invalid_ctv_manifest("fanout anchor vout exceeds uint32")
    })?))
}

/// Validate the trailing keyless P2A ephemeral anchor that funds CPFP
/// fee-bumping. It must be the last output, carry zero value (so no miner value
/// is diverted), and use the canonical pay-to-anchor scriptPubKey.
fn ensure_fee_anchor_output(
    output: &ParsedTxOut,
    declared_vout: u32,
    expected_index: usize,
) -> Result<(), PrismError> {
    if declared_vout as usize != expected_index {
        return Err(invalid_ctv_manifest("fee anchor vout mismatch"));
    }
    if output.amount_sats != CTV_FANOUT_ANCHOR_VALUE_SATS {
        return Err(invalid_ctv_manifest("fee anchor value must be zero"));
    }
    if output.script_pubkey != p2a_anchor_script_pubkey() {
        return Err(invalid_ctv_manifest(
            "fee anchor must use the P2A scriptPubKey",
        ));
    }
    Ok(())
}

fn commitment_witness_leaf_hex(precommitment_sha256_hex: &str) -> Result<String, PrismError> {
    let precommitment_hash = decode_hash_hex(precommitment_sha256_hex, "precommitment_sha256_hex")?;
    let mut payload = Vec::new();
    payload.extend_from_slice(CTV_FANOUT_COMMITMENT_LEAF_TAG.as_bytes());
    payload.extend_from_slice(&precommitment_hash);
    Ok(hex::encode(sha256_array(&payload)))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(sha256_array(bytes))
}

fn default_ctv_hash_for_parsed_tx(
    tx: &ParsedTransaction,
    input_index: usize,
) -> Result<[u8; 32], PrismError> {
    if input_index >= tx.inputs.len() {
        return Err(invalid_ctv_manifest("CTV input index out of range"));
    }
    let mut preimage = Vec::new();
    preimage.extend_from_slice(&tx.version.to_le_bytes());
    preimage.extend_from_slice(&tx.lock_time.to_le_bytes());
    if tx.inputs.iter().any(|input| !input.script_sig.is_empty()) {
        let scripts = tx.inputs.iter().fold(Vec::new(), |mut bytes, input| {
            bytes.extend_from_slice(&compact_size(input.script_sig.len() as u64));
            bytes.extend_from_slice(&input.script_sig);
            bytes
        });
        preimage.extend_from_slice(&sha256_array(&scripts));
    }
    let input_count = u32::try_from(tx.inputs.len())
        .map_err(|_| invalid_ctv_manifest("CTV input count exceeds uint32"))?;
    preimage.extend_from_slice(&input_count.to_le_bytes());
    let sequences = tx.inputs.iter().fold(Vec::new(), |mut bytes, input| {
        bytes.extend_from_slice(&input.sequence.to_le_bytes());
        bytes
    });
    preimage.extend_from_slice(&sha256_array(&sequences));
    let output_count = u32::try_from(tx.outputs.len())
        .map_err(|_| invalid_ctv_manifest("CTV output count exceeds uint32"))?;
    preimage.extend_from_slice(&output_count.to_le_bytes());
    let outputs = tx.outputs.iter().fold(Vec::new(), |mut bytes, output| {
        output.serialize(&mut bytes);
        bytes
    });
    preimage.extend_from_slice(&sha256_array(&outputs));
    let input_index = u32::try_from(input_index)
        .map_err(|_| invalid_ctv_manifest("CTV input index exceeds uint32"))?;
    preimage.extend_from_slice(&input_index.to_le_bytes());
    Ok(sha256_array(&preimage))
}

fn verify_fanout_witness(tx: &ParsedTransaction, leaf_script: &[u8]) -> Result<(), PrismError> {
    let witness = tx
        .witness_stacks
        .as_ref()
        .ok_or_else(|| invalid_ctv_manifest("fanout transaction must include P2MR witness"))?;
    let input_witness = witness
        .get(QBIT_CTV_INPUT_INDEX as usize)
        .ok_or_else(|| invalid_ctv_manifest("fanout witness input 0 is missing"))?;
    if input_witness.len() != 2 {
        return Err(invalid_ctv_manifest(
            "fanout P2MR witness must contain leaf script and control block",
        ));
    }
    if input_witness[0] != leaf_script {
        return Err(invalid_ctv_manifest("fanout witness leaf script mismatch"));
    }
    if input_witness[1] != [QBIT_P2MR_CONTROL_BLOCK] {
        return Err(invalid_ctv_manifest(
            "fanout witness control block mismatch",
        ));
    }
    Ok(())
}

fn ctv_leaf_script(ctv_hash: &[u8; 32]) -> Vec<u8> {
    let mut script = Vec::with_capacity(34);
    script.push(32);
    script.extend_from_slice(ctv_hash);
    script.push(QBIT_OP_CHECKTEMPLATEVERIFY);
    script
}

fn p2mr_tapleaf_hash(leaf_script: &[u8]) -> [u8; 32] {
    let mut payload = Vec::new();
    payload.push(QBIT_P2MR_LEAF_VERSION);
    payload.extend_from_slice(&compact_size(leaf_script.len() as u64));
    payload.extend_from_slice(leaf_script);
    tagged_hash("P2MRLeaf", &payload)
}

fn tagged_hash(tag: &str, payload: &[u8]) -> [u8; 32] {
    let tag_hash = sha256_array(tag.as_bytes());
    let mut hasher = Sha256::new();
    hasher.update(tag_hash);
    hasher.update(tag_hash);
    hasher.update(payload);
    hasher.finalize().into()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedTransaction {
    version: u32,
    inputs: Vec<ParsedTxIn>,
    outputs: Vec<ParsedTxOut>,
    lock_time: u32,
    witness_stacks: Option<Vec<Vec<Vec<u8>>>>,
    tx_without_witness: Vec<u8>,
}

impl ParsedTransaction {
    fn txid_hex(&self) -> String {
        let mut txid = hash256_array(&self.tx_without_witness);
        txid.reverse();
        hex::encode(txid)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedTxIn {
    prev_txid_hex: String,
    prev_vout: u32,
    script_sig: Vec<u8>,
    sequence: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedTxOut {
    amount_sats: u64,
    script_pubkey: Vec<u8>,
}

impl ParsedTxOut {
    fn serialize(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.amount_sats.to_le_bytes());
        out.extend_from_slice(&compact_size(self.script_pubkey.len() as u64));
        out.extend_from_slice(&self.script_pubkey);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TxInputSpec {
    prev_txid_hex: String,
    prev_vout: u32,
    script_sig: Vec<u8>,
    sequence: u32,
    witness_stack: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TxOutputSpec {
    amount_sats: u64,
    script_pubkey: Vec<u8>,
}

fn output_spec_from_fanout_output(output: &CtvFanoutOutput) -> Result<TxOutputSpec, PrismError> {
    Ok(TxOutputSpec {
        amount_sats: output.amount_sats,
        script_pubkey: hex::decode(&output.script_pubkey_hex).map_err(|err| {
            invalid_ctv_manifest(format!(
                "fanout output scriptPubKey must be canonical hex: {err}"
            ))
        })?,
    })
}

fn p2a_anchor_output_spec() -> TxOutputSpec {
    TxOutputSpec {
        amount_sats: CTV_FANOUT_ANCHOR_VALUE_SATS,
        script_pubkey: p2a_anchor_script_pubkey(),
    }
}

fn output_specs_with_anchor(outputs: &[CtvFanoutOutput]) -> Result<Vec<TxOutputSpec>, PrismError> {
    let mut all = outputs
        .iter()
        .map(output_spec_from_fanout_output)
        .collect::<Result<Vec<_>, PrismError>>()?;
    all.push(p2a_anchor_output_spec());
    Ok(all)
}

fn fanout_output_specs(
    outputs: &[CtvFanoutOutput],
    include_anchor: bool,
) -> Result<Vec<TxOutputSpec>, PrismError> {
    if include_anchor {
        return output_specs_with_anchor(outputs);
    }
    outputs
        .iter()
        .map(output_spec_from_fanout_output)
        .collect::<Result<Vec<_>, PrismError>>()
}

fn unsigned_fanout_tx_hex(
    parent_txid_hex: &str,
    parent_vout: u32,
    outputs: &[CtvFanoutOutput],
    include_anchor: bool,
    lock_time: u32,
    sequence: u32,
) -> Result<String, PrismError> {
    serialize_tx_hex(
        CTV_FANOUT_TX_VERSION,
        &[TxInputSpec {
            prev_txid_hex: normalize_txid_hex(parent_txid_hex, "fanout parent txid")?,
            prev_vout: parent_vout,
            script_sig: Vec::new(),
            sequence,
            witness_stack: Vec::new(),
        }],
        &fanout_output_specs(outputs, include_anchor)?,
        lock_time,
        false,
    )
}

fn witnessed_fanout_tx_hex(
    parent_txid_hex: &str,
    parent_vout: u32,
    outputs: &[CtvFanoutOutput],
    include_anchor: bool,
    lock_time: u32,
    sequence: u32,
    leaf_script: &[u8],
) -> Result<String, PrismError> {
    serialize_tx_hex(
        CTV_FANOUT_TX_VERSION,
        &[TxInputSpec {
            prev_txid_hex: normalize_txid_hex(parent_txid_hex, "fanout parent txid")?,
            prev_vout: parent_vout,
            script_sig: Vec::new(),
            sequence,
            witness_stack: vec![leaf_script.to_vec(), vec![QBIT_P2MR_CONTROL_BLOCK]],
        }],
        &fanout_output_specs(outputs, include_anchor)?,
        lock_time,
        true,
    )
}

fn serialize_tx_hex(
    version: u32,
    inputs: &[TxInputSpec],
    outputs: &[TxOutputSpec],
    lock_time: u32,
    with_witness: bool,
) -> Result<String, PrismError> {
    let mut tx = Vec::new();
    tx.extend_from_slice(&version.to_le_bytes());
    if with_witness {
        tx.extend_from_slice(&[0, 1]);
    }
    tx.extend_from_slice(&compact_size(inputs.len() as u64));
    for input in inputs {
        let mut prev_txid = hex::decode(&input.prev_txid_hex)
            .map_err(|err| invalid_ctv_manifest(format!("prev txid must be hex: {err}")))?;
        if prev_txid.len() != 32 {
            return Err(invalid_ctv_manifest("prev txid must be 32 bytes"));
        }
        prev_txid.reverse();
        tx.extend_from_slice(&prev_txid);
        tx.extend_from_slice(&input.prev_vout.to_le_bytes());
        tx.extend_from_slice(&compact_size(input.script_sig.len() as u64));
        tx.extend_from_slice(&input.script_sig);
        tx.extend_from_slice(&input.sequence.to_le_bytes());
    }
    tx.extend_from_slice(&compact_size(outputs.len() as u64));
    for output in outputs {
        tx.extend_from_slice(&output.amount_sats.to_le_bytes());
        tx.extend_from_slice(&compact_size(output.script_pubkey.len() as u64));
        tx.extend_from_slice(&output.script_pubkey);
    }
    if with_witness {
        for input in inputs {
            tx.extend_from_slice(&compact_size(input.witness_stack.len() as u64));
            for item in &input.witness_stack {
                tx.extend_from_slice(&compact_size(item.len() as u64));
                tx.extend_from_slice(item);
            }
        }
    }
    tx.extend_from_slice(&lock_time.to_le_bytes());
    Ok(hex::encode(tx))
}

fn parse_transaction_hex(tx_hex: &str, field_name: &str) -> Result<ParsedTransaction, PrismError> {
    let tx_bytes = hex::decode(tx_hex)
        .map_err(|err| invalid_ctv_manifest(format!("{field_name} is not valid hex: {err}")))?;
    parse_transaction(&tx_bytes, field_name)
}

fn parse_transaction(bytes: &[u8], field_name: &str) -> Result<ParsedTransaction, PrismError> {
    let mut reader = TxReader::new(bytes, field_name);
    let version = reader.read_u32()?;
    let has_witness = if reader.peek_u8()? == 0 {
        reader.read_u8()?;
        let flag = reader.read_u8()?;
        if flag == 0 {
            return Err(invalid_ctv_manifest(format!(
                "{field_name} has invalid witness flag 0"
            )));
        }
        true
    } else {
        false
    };
    let input_count = reader.read_compact_size_as_usize()?;
    if input_count == 0 {
        return Err(invalid_ctv_manifest(format!("{field_name} has no inputs")));
    }
    let mut inputs = Vec::with_capacity(input_count);
    for _ in 0..input_count {
        let prev_txid_raw = reader.read_bytes(32)?;
        let mut prev_txid_display = prev_txid_raw.to_vec();
        prev_txid_display.reverse();
        let prev_vout = reader.read_u32()?;
        let script_sig = reader.read_var_bytes()?;
        let sequence = reader.read_u32()?;
        inputs.push(ParsedTxIn {
            prev_txid_hex: hex::encode(prev_txid_display),
            prev_vout,
            script_sig,
            sequence,
        });
    }
    let output_count = reader.read_compact_size_as_usize()?;
    let mut outputs = Vec::with_capacity(output_count);
    for _ in 0..output_count {
        let amount_sats = reader.read_i64_nonnegative()? as u64;
        let script_pubkey = reader.read_var_bytes()?;
        outputs.push(ParsedTxOut {
            amount_sats,
            script_pubkey,
        });
    }
    let witness_stacks = if has_witness {
        let mut stacks = Vec::with_capacity(input_count);
        for _ in 0..input_count {
            let item_count = reader.read_compact_size_as_usize()?;
            let mut stack = Vec::with_capacity(item_count);
            for _ in 0..item_count {
                stack.push(reader.read_var_bytes()?);
            }
            stacks.push(stack);
        }
        Some(stacks)
    } else {
        None
    };
    let lock_time = reader.read_u32()?;
    reader.expect_finished()?;

    let mut tx_without_witness = Vec::new();
    tx_without_witness.extend_from_slice(&version.to_le_bytes());
    tx_without_witness.extend_from_slice(&compact_size(inputs.len() as u64));
    for input in &inputs {
        let mut prev_txid = hex::decode(&input.prev_txid_hex).expect("display txid hex");
        prev_txid.reverse();
        tx_without_witness.extend_from_slice(&prev_txid);
        tx_without_witness.extend_from_slice(&input.prev_vout.to_le_bytes());
        tx_without_witness.extend_from_slice(&compact_size(input.script_sig.len() as u64));
        tx_without_witness.extend_from_slice(&input.script_sig);
        tx_without_witness.extend_from_slice(&input.sequence.to_le_bytes());
    }
    tx_without_witness.extend_from_slice(&compact_size(outputs.len() as u64));
    for output in &outputs {
        output.serialize(&mut tx_without_witness);
    }
    tx_without_witness.extend_from_slice(&lock_time.to_le_bytes());

    Ok(ParsedTransaction {
        version,
        inputs,
        outputs,
        lock_time,
        witness_stacks,
        tx_without_witness,
    })
}

struct TxReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    field_name: &'a str,
}

impl<'a> TxReader<'a> {
    fn new(bytes: &'a [u8], field_name: &'a str) -> Self {
        Self {
            bytes,
            offset: 0,
            field_name,
        }
    }

    fn peek_u8(&self) -> Result<u8, PrismError> {
        self.bytes
            .get(self.offset)
            .copied()
            .ok_or_else(|| self.truncated())
    }

    fn read_u8(&mut self) -> Result<u8, PrismError> {
        let byte = self.peek_u8()?;
        self.offset += 1;
        Ok(byte)
    }

    fn read_u32(&mut self) -> Result<u32, PrismError> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes(
            bytes.try_into().expect("length checked"),
        ))
    }

    fn read_i64_nonnegative(&mut self) -> Result<i64, PrismError> {
        let bytes = self.read_bytes(8)?;
        let value = i64::from_le_bytes(bytes.try_into().expect("length checked"));
        if value < 0 {
            return Err(invalid_ctv_manifest(format!(
                "{} has negative transaction output value",
                self.field_name
            )));
        }
        if value as u64 > QBIT_MAX_MONEY_SATS {
            return Err(invalid_ctv_manifest(format!(
                "{} has transaction output value above MAX_MONEY",
                self.field_name
            )));
        }
        Ok(value)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], PrismError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| self.truncated())?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| self.truncated())?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_var_bytes(&mut self) -> Result<Vec<u8>, PrismError> {
        let len = self.read_compact_size_as_usize()?;
        Ok(self.read_bytes(len)?.to_vec())
    }

    fn read_compact_size_as_usize(&mut self) -> Result<usize, PrismError> {
        let value = usize::try_from(self.read_compact_size()?).map_err(|_| {
            invalid_ctv_manifest(format!("{} compact size exceeds usize", self.field_name))
        })?;
        // A compact size is always followed by either that many bytes (a
        // var-length field) or that many sub-elements that each consume at
        // least one byte. Either way it cannot exceed the bytes remaining, so
        // bounding it here keeps `Vec::with_capacity` from allocating gigabytes
        // for a crafted count before the per-element read can fail.
        if value > self.remaining() {
            return Err(self.truncated());
        }
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_compact_size(&mut self) -> Result<u64, PrismError> {
        let first = self.read_u8()?;
        match first {
            0x00..=0xfc => Ok(u64::from(first)),
            0xfd => {
                let raw = self.read_bytes(2)?;
                let value = u16::from_le_bytes(raw.try_into().expect("length checked"));
                if value < 0xfd {
                    return Err(invalid_ctv_manifest(format!(
                        "{} has non-canonical compact size",
                        self.field_name
                    )));
                }
                Ok(u64::from(value))
            }
            0xfe => {
                let raw = self.read_bytes(4)?;
                let value = u32::from_le_bytes(raw.try_into().expect("length checked"));
                if value <= u32::from(u16::MAX) {
                    return Err(invalid_ctv_manifest(format!(
                        "{} has non-canonical compact size",
                        self.field_name
                    )));
                }
                Ok(u64::from(value))
            }
            0xff => {
                let raw = self.read_bytes(8)?;
                let value = u64::from_le_bytes(raw.try_into().expect("length checked"));
                if value <= u64::from(u32::MAX) {
                    return Err(invalid_ctv_manifest(format!(
                        "{} has non-canonical compact size",
                        self.field_name
                    )));
                }
                Ok(value)
            }
        }
    }

    fn expect_finished(&self) -> Result<(), PrismError> {
        if self.offset != self.bytes.len() {
            return Err(invalid_ctv_manifest(format!(
                "{} has trailing transaction bytes",
                self.field_name
            )));
        }
        Ok(())
    }

    fn truncated(&self) -> PrismError {
        invalid_ctv_manifest(format!("{} transaction is truncated", self.field_name))
    }
}

fn normalize_hex(value: &str) -> Result<String, PrismError> {
    let bytes = hex::decode(value)
        .map_err(|err| invalid_ctv_manifest(format!("invalid transaction hex: {err}")))?;
    Ok(hex::encode(bytes))
}

fn normalize_txid_hex(value: &str, field_name: &str) -> Result<String, PrismError> {
    let bytes = hex::decode(value)
        .map_err(|err| invalid_ctv_manifest(format!("{field_name} must be hex: {err}")))?;
    if bytes.len() != 32 {
        return Err(invalid_ctv_manifest(format!(
            "{field_name} must be 32 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(hex::encode(bytes))
}

fn validate_sha256_hex(value: &str, field_name: &str) -> Result<(), PrismError> {
    let _ = decode_hash_hex(value, field_name)?;
    Ok(())
}

fn validate_canonical_sha256_hex(value: &str, field_name: &str) -> Result<(), PrismError> {
    let hash = decode_hash_hex(value, field_name)?;
    if value != hex::encode(hash) {
        return Err(invalid_ctv_manifest(format!(
            "{field_name} must be lowercase canonical hex"
        )));
    }
    Ok(())
}

fn decode_hash_hex(value: &str, field_name: &str) -> Result<[u8; 32], PrismError> {
    let bytes = hex::decode(value)
        .map_err(|err| invalid_ctv_manifest(format!("{field_name} must be hex: {err}")))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        invalid_ctv_manifest(format!(
            "{field_name} must be 32 bytes, got {}",
            bytes.len()
        ))
    })
}

fn decode_program_hex(value: &str, field_name: &str) -> Result<[u8; P2MR_PROGRAM_LEN], PrismError> {
    let bytes = hex::decode(value)
        .map_err(|err| invalid_ctv_manifest(format!("{field_name} must be hex: {err}")))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        invalid_ctv_manifest(format!(
            "{field_name} must be {P2MR_PROGRAM_LEN} bytes, got {}",
            bytes.len()
        ))
    })
}

fn sha256_array(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

fn hash256_array(data: &[u8]) -> [u8; 32] {
    sha256_array(&sha256_array(data))
}

fn compact_size(size: u64) -> Vec<u8> {
    if size < 253 {
        vec![size as u8]
    } else if size <= u64::from(u16::MAX) {
        let mut bytes = vec![0xfd];
        bytes.extend_from_slice(&(size as u16).to_le_bytes());
        bytes
    } else if size <= u64::from(u32::MAX) {
        let mut bytes = vec![0xfe];
        bytes.extend_from_slice(&(size as u32).to_le_bytes());
        bytes
    } else {
        let mut bytes = vec![0xff];
        bytes.extend_from_slice(&size.to_le_bytes());
        bytes
    }
}

fn invalid_ctv_manifest(reason: impl Into<String>) -> PrismError {
    PrismError::InvalidCtvFanoutManifest {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK_HEIGHT: u64 = 101;
    const PARENT_PREVOUT_TXID: &str =
        "0101010101010101010101010101010101010101010101010101010101010101";

    #[derive(Clone)]
    struct TxInputSpec {
        prev_txid_hex: String,
        prev_vout: u32,
        script_sig: Vec<u8>,
        sequence: u32,
        witness_stack: Vec<Vec<u8>>,
    }

    #[derive(Clone)]
    struct TxOutputSpec {
        amount_sats: u64,
        script_pubkey: Vec<u8>,
    }

    fn payout_program(byte: u8) -> String {
        hex::encode([byte; 32])
    }

    fn serialize_tx(
        version: u32,
        inputs: &[TxInputSpec],
        outputs: &[TxOutputSpec],
        lock_time: u32,
        with_witness: bool,
    ) -> String {
        let mut tx = Vec::new();
        tx.extend_from_slice(&version.to_le_bytes());
        if with_witness {
            tx.extend_from_slice(&[0, 1]);
        }
        tx.extend_from_slice(&compact_size(inputs.len() as u64));
        for input in inputs {
            let mut prev_txid = hex::decode(&input.prev_txid_hex).unwrap();
            prev_txid.reverse();
            tx.extend_from_slice(&prev_txid);
            tx.extend_from_slice(&input.prev_vout.to_le_bytes());
            tx.extend_from_slice(&compact_size(input.script_sig.len() as u64));
            tx.extend_from_slice(&input.script_sig);
            tx.extend_from_slice(&input.sequence.to_le_bytes());
        }
        tx.extend_from_slice(&compact_size(outputs.len() as u64));
        for output in outputs {
            tx.extend_from_slice(&output.amount_sats.to_le_bytes());
            tx.extend_from_slice(&compact_size(output.script_pubkey.len() as u64));
            tx.extend_from_slice(&output.script_pubkey);
        }
        if with_witness {
            for input in inputs {
                tx.extend_from_slice(&compact_size(input.witness_stack.len() as u64));
                for item in &input.witness_stack {
                    tx.extend_from_slice(&compact_size(item.len() as u64));
                    tx.extend_from_slice(item);
                }
            }
        }
        tx.extend_from_slice(&lock_time.to_le_bytes());
        hex::encode(tx)
    }

    fn anchor_output_spec() -> TxOutputSpec {
        TxOutputSpec {
            amount_sats: CTV_FANOUT_ANCHOR_VALUE_SATS,
            script_pubkey: hex::decode(P2A_ANCHOR_SCRIPT_PUBKEY_HEX).unwrap(),
        }
    }

    /// Payout outputs followed by the keyless P2A fee anchor for legacy
    /// zero-fee test fanouts.
    fn outputs_with_anchor(outputs: &[TxOutputSpec]) -> Vec<TxOutputSpec> {
        let mut all = outputs.to_vec();
        all.push(anchor_output_spec());
        all
    }

    fn outputs_with_optional_anchor(
        outputs: &[TxOutputSpec],
        include_anchor: bool,
    ) -> Vec<TxOutputSpec> {
        if include_anchor {
            outputs_with_anchor(outputs)
        } else {
            outputs.to_vec()
        }
    }

    fn unsigned_fanout_tx_for_vout(
        parent_txid: &str,
        parent_vout: u32,
        outputs: &[TxOutputSpec],
        lock_time: u32,
        sequence: u32,
    ) -> String {
        unsigned_fanout_tx_for_vout_with_anchor(
            parent_txid,
            parent_vout,
            outputs,
            true,
            lock_time,
            sequence,
        )
    }

    fn unsigned_fanout_tx_for_vout_with_anchor(
        parent_txid: &str,
        parent_vout: u32,
        outputs: &[TxOutputSpec],
        include_anchor: bool,
        lock_time: u32,
        sequence: u32,
    ) -> String {
        serialize_tx(
            CTV_FANOUT_TX_VERSION,
            &[TxInputSpec {
                prev_txid_hex: parent_txid.to_string(),
                prev_vout: parent_vout,
                script_sig: Vec::new(),
                sequence,
                witness_stack: Vec::new(),
            }],
            &outputs_with_optional_anchor(outputs, include_anchor),
            lock_time,
            false,
        )
    }

    fn witnessed_fanout_tx_for_vout(
        parent_txid: &str,
        parent_vout: u32,
        outputs: &[TxOutputSpec],
        lock_time: u32,
        sequence: u32,
        leaf_script: &[u8],
    ) -> String {
        witnessed_fanout_tx_for_vout_with_anchor(
            parent_txid,
            parent_vout,
            outputs,
            true,
            lock_time,
            sequence,
            leaf_script,
        )
    }

    fn witnessed_fanout_tx_for_vout_with_anchor(
        parent_txid: &str,
        parent_vout: u32,
        outputs: &[TxOutputSpec],
        include_anchor: bool,
        lock_time: u32,
        sequence: u32,
        leaf_script: &[u8],
    ) -> String {
        serialize_tx(
            CTV_FANOUT_TX_VERSION,
            &[TxInputSpec {
                prev_txid_hex: parent_txid.to_string(),
                prev_vout: parent_vout,
                script_sig: Vec::new(),
                sequence,
                witness_stack: vec![leaf_script.to_vec(), vec![QBIT_P2MR_CONTROL_BLOCK]],
            }],
            &outputs_with_optional_anchor(outputs, include_anchor),
            lock_time,
            true,
        )
    }

    fn coinbase_tx(covenant_value: u64, covenant_script_pubkey: &[u8]) -> String {
        coinbase_tx_with_outputs(&[TxOutputSpec {
            amount_sats: covenant_value,
            script_pubkey: covenant_script_pubkey.to_vec(),
        }])
    }

    fn coinbase_tx_with_outputs(outputs: &[TxOutputSpec]) -> String {
        serialize_tx(
            2,
            &[TxInputSpec {
                prev_txid_hex: "00".repeat(32),
                prev_vout: u32::MAX,
                script_sig: vec![1, 1],
                sequence: u32::MAX,
                witness_stack: Vec::new(),
            }],
            outputs,
            0,
            false,
        )
    }

    fn sample_payouts() -> Vec<CtvFanoutPayout> {
        vec![
            CtvFanoutPayout {
                recipient_id: "miner-a".to_string(),
                order_key: "01".to_string(),
                p2mr_program_hex: payout_program(1),
                gross_amount_sats: 0,
                fee_sats: 0,
                amount_sats: 60_000,
            },
            CtvFanoutPayout {
                recipient_id: "miner-b".to_string(),
                order_key: "02".to_string(),
                p2mr_program_hex: payout_program(2),
                gross_amount_sats: 0,
                fee_sats: 0,
                amount_sats: 40_000,
            },
        ]
    }

    fn sample_positive_fee_payouts() -> Vec<CtvFanoutPayout> {
        vec![
            CtvFanoutPayout {
                recipient_id: "miner-a".to_string(),
                order_key: "01".to_string(),
                p2mr_program_hex: payout_program(1),
                gross_amount_sats: 60_000,
                fee_sats: 120,
                amount_sats: 59_880,
            },
            CtvFanoutPayout {
                recipient_id: "miner-b".to_string(),
                order_key: "02".to_string(),
                p2mr_program_hex: payout_program(2),
                gross_amount_sats: 40_000,
                fee_sats: 0,
                amount_sats: 40_000,
            },
        ]
    }

    fn sample_manifest_from_payouts(payouts: Vec<CtvFanoutPayout>) -> CtvFanoutManifest {
        sample_manifest_from_payouts_and_chunk(payouts, 0, 1)
    }

    fn sample_manifest_from_payouts_and_chunk(
        payouts: Vec<CtvFanoutPayout>,
        chunk_index: u32,
        chunk_count: u32,
    ) -> CtvFanoutManifest {
        let canonical_outputs = canonical_fanout_outputs(payouts.clone()).unwrap();
        let fanout_output_sum_sats = canonical_outputs
            .iter()
            .map(|output| output.amount_sats)
            .sum::<u64>();
        let fanout_fee_sats = canonical_outputs
            .iter()
            .map(|output| output.fee_sats)
            .sum::<u64>();
        let covenant_output_value_sats = fanout_output_sum_sats + fanout_fee_sats;
        let include_anchor = fanout_has_cpfp_anchor(fanout_fee_sats);
        let outputs = canonical_outputs
            .iter()
            .map(|output| TxOutputSpec {
                amount_sats: output.amount_sats,
                script_pubkey: hex::decode(output.script_pubkey_hex.clone()).unwrap(),
            })
            .collect::<Vec<_>>();
        let sequence = expected_fanout_sequence(chunk_index).unwrap();
        let unsigned_tx = unsigned_fanout_tx_for_vout_with_anchor(
            PARENT_PREVOUT_TXID,
            chunk_index,
            &outputs,
            include_anchor,
            BLOCK_HEIGHT as u32,
            sequence,
        );
        let ctv_hash = default_ctv_hash_hex(&unsigned_tx, 0).unwrap();
        let leaf_script = hex::decode(ctv_leaf_script_hex(&ctv_hash).unwrap()).unwrap();
        let p2mr_program =
            hex::decode(p2mr_program_for_ctv_leaf_hex(&hex::encode(&leaf_script)).unwrap())
                .unwrap();
        let covenant_script = p2mr_script_pubkey(p2mr_program.try_into().unwrap());
        let parent_coinbase_tx_hex = coinbase_tx(covenant_output_value_sats, &covenant_script);
        let parent_coinbase_txid = parse_transaction_hex(&parent_coinbase_tx_hex, "coinbase")
            .unwrap()
            .txid_hex();
        let fanout_tx_hex = witnessed_fanout_tx_for_vout_with_anchor(
            &parent_coinbase_txid,
            0,
            &outputs,
            include_anchor,
            BLOCK_HEIGHT as u32,
            sequence,
            &leaf_script,
        );
        build_ctv_fanout_manifest(CtvFanoutManifestInput {
            block_height: BLOCK_HEIGHT,
            chunk_index,
            chunk_count,
            coinbase_value_sats: covenant_output_value_sats,
            settlement_mode: SettlementMode::CtvFanout,
            fanout_tx_template_hex: unsigned_tx,
            parent_coinbase_tx_hex,
            parent_coinbase_vout: 0,
            fanout_tx_hex,
            reward_manifest_sha256_hex: "aa".repeat(32),
            payout_policy_manifest_sha256_hex: "bb".repeat(32),
            payouts,
        })
        .unwrap()
    }

    fn sample_manifest() -> CtvFanoutManifest {
        sample_manifest_from_payouts(sample_payouts())
    }

    #[test]
    fn prepared_ctv_fanout_finalizes_after_coinbase_outpoint_is_known() {
        let prepared = prepare_ctv_fanout_precommitment(CtvFanoutPrecommitmentInput {
            block_height: BLOCK_HEIGHT,
            chunk_index: 0,
            chunk_count: 1,
            coinbase_value_sats: 100_000,
            settlement_mode: SettlementMode::CtvFanout,
            reward_manifest_sha256_hex: "aa".repeat(32),
            payout_policy_manifest_sha256_hex: "bb".repeat(32),
            payouts: sample_payouts(),
        })
        .unwrap();

        assert_eq!(prepared.covenant_output_value_sats, 100_000);
        assert_eq!(prepared.covenant_p2mr_program_hex.len(), 64);
        assert_eq!(prepared.commitment_witness_leaf_hex.len(), 64);
        let covenant_script = p2mr_script_pubkey(
            hex::decode(&prepared.covenant_p2mr_program_hex)
                .unwrap()
                .try_into()
                .unwrap(),
        );
        let parent_coinbase_tx_hex = coinbase_tx(100_000, &covenant_script);

        let manifest = build_ctv_fanout_manifest_from_precommitment(
            prepared.precommitment.clone(),
            parent_coinbase_tx_hex,
            0,
        )
        .unwrap();

        assert_eq!(
            manifest.precommitment_sha256_hex,
            prepared.precommitment_sha256_hex
        );
        assert_eq!(
            manifest.commitment_witness_leaf_hex,
            prepared.commitment_witness_leaf_hex
        );
        assert_eq!(
            manifest.precommitment.p2mr_program_hex,
            prepared.covenant_p2mr_program_hex
        );
        verify_ctv_fanout_manifest_commitment_leaf(
            &manifest,
            &[prepared.commitment_witness_leaf_hex],
        )
        .unwrap();
    }

    fn sample_manifest_set() -> CtvFanoutManifestSet {
        let chunks = [
            vec![CtvFanoutPayout {
                recipient_id: "miner-a".to_string(),
                order_key: "01".to_string(),
                p2mr_program_hex: payout_program(1),
                gross_amount_sats: 0,
                fee_sats: 0,
                amount_sats: 60_000,
            }],
            vec![CtvFanoutPayout {
                recipient_id: "miner-b".to_string(),
                order_key: "02".to_string(),
                p2mr_program_hex: payout_program(2),
                gross_amount_sats: 0,
                fee_sats: 0,
                amount_sats: 40_000,
            }],
        ];
        let prepared = chunks
            .iter()
            .enumerate()
            .map(|(chunk_index, payouts)| {
                let outputs = canonical_fanout_outputs(payouts.clone()).unwrap();
                let output_specs = outputs
                    .iter()
                    .map(|output| TxOutputSpec {
                        amount_sats: output.amount_sats,
                        script_pubkey: hex::decode(&output.script_pubkey_hex).unwrap(),
                    })
                    .collect::<Vec<_>>();
                let sequence = expected_fanout_sequence(chunk_index as u32).unwrap();
                let unsigned_tx = unsigned_fanout_tx_for_vout(
                    PARENT_PREVOUT_TXID,
                    chunk_index as u32,
                    &output_specs,
                    BLOCK_HEIGHT as u32,
                    sequence,
                );
                let ctv_hash = default_ctv_hash_hex(&unsigned_tx, 0).unwrap();
                let leaf_script = hex::decode(ctv_leaf_script_hex(&ctv_hash).unwrap()).unwrap();
                let p2mr_program =
                    hex::decode(p2mr_program_for_ctv_leaf_hex(&hex::encode(&leaf_script)).unwrap())
                        .unwrap();
                let covenant_script = p2mr_script_pubkey(p2mr_program.try_into().unwrap());
                (
                    payouts.clone(),
                    output_specs,
                    unsigned_tx,
                    leaf_script,
                    TxOutputSpec {
                        amount_sats: outputs.iter().map(|output| output.amount_sats).sum(),
                        script_pubkey: covenant_script.to_vec(),
                    },
                )
            })
            .collect::<Vec<_>>();
        let parent_coinbase_tx_hex = coinbase_tx_with_outputs(
            &prepared
                .iter()
                .map(|(_, _, _, _, covenant)| covenant.clone())
                .collect::<Vec<_>>(),
        );
        let parent_coinbase_txid = parse_transaction_hex(&parent_coinbase_tx_hex, "coinbase")
            .unwrap()
            .txid_hex();
        let manifests = prepared
            .into_iter()
            .enumerate()
            .map(
                |(chunk_index, (payouts, output_specs, unsigned_tx, leaf_script, _))| {
                    let sequence = expected_fanout_sequence(chunk_index as u32).unwrap();
                    let fanout_tx_hex = witnessed_fanout_tx_for_vout(
                        &parent_coinbase_txid,
                        chunk_index as u32,
                        &output_specs,
                        BLOCK_HEIGHT as u32,
                        sequence,
                        &leaf_script,
                    );
                    build_ctv_fanout_manifest(CtvFanoutManifestInput {
                        block_height: BLOCK_HEIGHT,
                        chunk_index: chunk_index as u32,
                        chunk_count: 2,
                        coinbase_value_sats: 100_000,
                        settlement_mode: SettlementMode::CtvFanout,
                        fanout_tx_template_hex: unsigned_tx,
                        parent_coinbase_tx_hex: parent_coinbase_tx_hex.clone(),
                        parent_coinbase_vout: chunk_index as u32,
                        fanout_tx_hex,
                        reward_manifest_sha256_hex: "aa".repeat(32),
                        payout_policy_manifest_sha256_hex: "bb".repeat(32),
                        payouts,
                    })
                    .unwrap()
                },
            )
            .collect::<Vec<_>>();
        build_ctv_fanout_manifest_set(manifests).unwrap()
    }

    fn refresh_precommitment_commitments(manifest: &mut CtvFanoutManifest) {
        manifest.precommitment_sha256_hex =
            sha256_hex(&canonical_precommitment_bytes(&manifest.precommitment).unwrap());
        manifest.commitment_witness_leaf_hex =
            commitment_witness_leaf_hex(&manifest.precommitment_sha256_hex).unwrap();
    }

    fn replace_manifest_fanout_tx(
        manifest: &mut CtvFanoutManifest,
        outputs: Vec<TxOutputSpec>,
        sequence: u32,
    ) {
        let leaf_script = hex::decode(&manifest.precommitment.ctv_leaf_script_hex).unwrap();
        manifest.fanout_tx_hex = serialize_tx(
            CTV_FANOUT_TX_VERSION,
            &[TxInputSpec {
                prev_txid_hex: manifest.parent_coinbase_txid.clone(),
                prev_vout: manifest.parent_coinbase_vout,
                script_sig: Vec::new(),
                sequence,
                witness_stack: vec![leaf_script, vec![QBIT_P2MR_CONTROL_BLOCK]],
            }],
            &outputs,
            manifest.precommitment.fanout_lock_time,
            true,
        );
        manifest.fanout_txid = parse_transaction_hex(&manifest.fanout_tx_hex, "fanout")
            .unwrap()
            .txid_hex();
    }

    fn manifest_fanout_output_specs(manifest: &CtvFanoutManifest) -> Vec<TxOutputSpec> {
        parse_transaction_hex(&manifest.fanout_tx_hex, "fanout")
            .unwrap()
            .outputs
            .into_iter()
            .map(|output| TxOutputSpec {
                amount_sats: output.amount_sats,
                script_pubkey: output.script_pubkey,
            })
            .collect()
    }

    #[test]
    fn ctv_fanout_manifest_binds_coinbase_value_outpoint_and_template() {
        let manifest = sample_manifest();

        assert_eq!(manifest.schema, CTV_FANOUT_MANIFEST_SCHEMA);
        assert_eq!(
            manifest.precommitment.schema,
            CTV_FANOUT_PRECOMMITMENT_SCHEMA
        );
        assert_eq!(manifest.precommitment.chunk_index, 0);
        assert_eq!(manifest.precommitment.chunk_count, 1);
        assert_eq!(
            manifest.precommitment.fanout_sequence,
            SEQUENCE_LOCKTIME_DISABLE_FLAG
        );
        // Payouts equal the full covenant value: no fee is baked in, and the
        // fee anchor carries zero value.
        assert_eq!(manifest.precommitment.fanout_output_sum_sats, 100_000);
        assert_eq!(manifest.covenant_output_value_sats, 100_000);
        // The legacy zero-fee fee anchor follows the two payouts.
        assert_eq!(manifest.precommitment.anchor_vout, Some(2));
        assert_eq!(manifest.precommitment.fanout_lock_time, BLOCK_HEIGHT as u32);
        assert_eq!(manifest.precommitment.ctv_leaf_script_hex.len(), 68);
        assert_eq!(
            manifest.covenant_script_pubkey_hex,
            format!("5220{}", manifest.precommitment.p2mr_program_hex)
        );
        assert_eq!(manifest.commitment_witness_leaf_hex.len(), 64);
        verify_ctv_fanout_manifest_structure(&manifest).unwrap();
        verify_ctv_fanout_manifest_commitment_leaf(
            &manifest,
            &[manifest.commitment_witness_leaf_hex.clone()],
        )
        .unwrap();
    }

    #[test]
    fn ctv_fanout_manifest_canonicalizes_payout_order() {
        let canonical = sample_manifest_from_payouts(sample_payouts());
        let mut shuffled = sample_payouts();
        shuffled.reverse();
        let from_shuffled = sample_manifest_from_payouts(shuffled);

        assert_eq!(from_shuffled.precommitment, canonical.precommitment);
        assert_eq!(
            from_shuffled.precommitment_sha256_hex,
            canonical.precommitment_sha256_hex
        );
        assert_eq!(
            from_shuffled.commitment_witness_leaf_hex,
            canonical.commitment_witness_leaf_hex
        );
        assert_eq!(
            from_shuffled.precommitment.outputs[0].recipient_id,
            "miner-a"
        );
    }

    #[test]
    fn ctv_fanout_manifest_canonicalizes_payout_program_case() {
        let mut payouts = sample_payouts();
        payouts[0].p2mr_program_hex = payouts[0].p2mr_program_hex.to_ascii_uppercase();

        let manifest = sample_manifest_from_payouts(payouts);

        assert_eq!(
            manifest.precommitment.outputs[0].p2mr_program_hex,
            payout_program(1)
        );
        verify_ctv_fanout_manifest_structure(&manifest).unwrap();
    }

    #[test]
    fn ctv_fanout_manifest_rejects_case_variant_duplicate_programs() {
        let program_hex = payout_program(0xab);
        let payouts = vec![
            CtvFanoutPayout {
                recipient_id: "miner-a".to_string(),
                order_key: "01".to_string(),
                p2mr_program_hex: program_hex.clone(),
                gross_amount_sats: 0,
                fee_sats: 0,
                amount_sats: 60_000,
            },
            CtvFanoutPayout {
                recipient_id: "miner-a".to_string(),
                order_key: "01".to_string(),
                p2mr_program_hex: program_hex.to_ascii_uppercase(),
                gross_amount_sats: 0,
                fee_sats: 0,
                amount_sats: 40_000,
            },
        ];

        let err = canonical_fanout_outputs(payouts).unwrap_err().to_string();

        assert!(err.contains("duplicate fanout payout recipient"), "{err}");
    }

    #[test]
    fn ctv_fanout_manifest_rejects_noncanonical_precommitment_program_case() {
        let mut manifest = sample_manifest();
        let program_hex = "ab".repeat(32);
        let program = decode_program_hex(&program_hex, "test p2mr_program_hex").unwrap();
        manifest.precommitment.outputs[0].p2mr_program_hex = program_hex.to_ascii_uppercase();
        manifest.precommitment.outputs[0].script_pubkey_hex =
            hex::encode(p2mr_script_pubkey(program));
        refresh_precommitment_commitments(&mut manifest);

        let err = verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string();

        assert!(err.contains("lowercase canonical hex"), "{err}");
    }

    #[test]
    fn ctv_fanout_manifest_set_verifies_two_chunks_and_exposes_recovery_hash() {
        let set = sample_manifest_set();

        verify_ctv_fanout_manifest_set(&set).unwrap();
        assert_eq!(set.schema, CTV_FANOUT_MANIFEST_SET_SCHEMA);
        assert_eq!(set.fanout_count, 2);
        assert_eq!(set.fanout_output_sum_sats, 100_000);
        assert_eq!(set.covenant_output_value_sats, 100_000);
        assert_eq!(set.manifests[0].precommitment.chunk_index, 0);
        assert_eq!(set.manifests[1].precommitment.chunk_index, 1);
        assert_eq!(set.manifests[0].precommitment.chunk_count, 2);
        assert_eq!(set.manifests[1].precommitment.chunk_count, 2);
        assert_eq!(
            set.manifests[0].precommitment.fanout_sequence,
            SEQUENCE_LOCKTIME_DISABLE_FLAG
        );
        assert_eq!(
            set.manifests[1].precommitment.fanout_sequence,
            SEQUENCE_LOCKTIME_DISABLE_FLAG + 1
        );
        assert_ne!(
            set.manifests[1].precommitment.fanout_sequence & SEQUENCE_LOCKTIME_DISABLE_FLAG,
            0
        );
        assert_eq!(
            set.manifests[1].precommitment.fanout_sequence & !SEQUENCE_LOCKTIME_DISABLE_FLAG,
            1
        );
        assert_ne!(
            set.manifests[0].precommitment.ctv_hash_hex,
            set.manifests[1].precommitment.ctv_hash_hex
        );
        assert_eq!(ctv_fanout_manifest_set_sha256_hex(&set).unwrap().len(), 64);
        assert_eq!(
            ctv_fanout_manifest_set_sha256_hex(&set).unwrap(),
            "aac0bf8c672142fb6bc3f2f8f84970bbc14609d516867c4c4d13443cefd6e31f"
        );
    }

    #[test]
    fn ctv_fanout_manifest_set_rejects_missing_or_noncanonical_chunk() {
        let mut missing = sample_manifest_set();
        missing.manifests.pop();

        let err = verify_ctv_fanout_manifest_set(&missing)
            .unwrap_err()
            .to_string();
        assert!(err.contains("count mismatch"), "{err}");

        let mut noncanonical = sample_manifest_set();
        noncanonical.manifests.reverse();
        let err = verify_ctv_fanout_manifest_set(&noncanonical)
            .unwrap_err()
            .to_string();
        assert!(err.contains("contiguous and canonical"), "{err}");
    }

    #[test]
    fn ctv_fanout_manifest_set_hash_rejects_noncanonical_hex() {
        let mut set = sample_manifest_set();
        set.manifests[0].fanout_tx_hex = set.manifests[0].fanout_tx_hex.to_ascii_uppercase();

        let err = ctv_fanout_manifest_set_sha256_hex(&set)
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("fanout_tx_hex must be lowercase canonical hex"),
            "{err}"
        );
    }

    #[test]
    fn ctv_fanout_manifest_rejects_chunk_sequence_mismatch() {
        let mut set = sample_manifest_set();
        let mut manifest = set.manifests.pop().unwrap();
        manifest.precommitment.fanout_sequence = 0;
        refresh_precommitment_commitments(&mut manifest);

        let err = verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string();

        assert!(err.contains("sequence must equal the chunk index"), "{err}");
    }

    #[test]
    fn ctv_fanout_manifest_rejects_mutated_final_sequence() {
        let mut manifest = sample_manifest();
        let outputs = manifest_fanout_output_specs(&manifest);
        let sequence = manifest.precommitment.fanout_sequence + 1;
        replace_manifest_fanout_tx(&mut manifest, outputs, sequence);

        let err = verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string();

        assert!(err.contains("fanout sequence mismatch"), "{err}");
    }

    #[test]
    fn ctv_fanout_manifest_rejects_mutated_anchor_script() {
        let mut manifest = sample_manifest();
        let mut outputs = manifest_fanout_output_specs(&manifest);
        outputs.last_mut().unwrap().script_pubkey = vec![0x51];
        let sequence = manifest.precommitment.fanout_sequence;
        replace_manifest_fanout_tx(&mut manifest, outputs, sequence);

        let err = verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string();

        assert!(err.contains("CTV hash mismatch"), "{err}");
    }

    #[test]
    fn ctv_fanout_manifest_rejects_missing_commitment_leaf() {
        let manifest = sample_manifest();

        assert!(verify_ctv_fanout_manifest_commitment_leaf(&manifest, &[])
            .unwrap_err()
            .to_string()
            .contains("witness leaf is missing"));
    }

    #[test]
    fn ctv_fanout_manifest_rejects_misplaced_fee_anchor() {
        let mut manifest = sample_manifest();
        manifest.precommitment.anchor_vout = Some(99);
        refresh_precommitment_commitments(&mut manifest);

        assert!(verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string()
            .contains("fee anchor vout"));
    }

    #[test]
    fn ctv_fanout_manifest_appends_zero_value_p2a_anchor() {
        let manifest = sample_manifest();
        let fanout = parse_transaction_hex(&manifest.fanout_tx_hex, "fanout").unwrap();
        let anchor = fanout.outputs.last().unwrap();
        // The committed fanout ends in a keyless, zero-value P2A anchor.
        assert_eq!(anchor.amount_sats, 0);
        assert_eq!(
            hex::encode(&anchor.script_pubkey),
            P2A_ANCHOR_SCRIPT_PUBKEY_HEX
        );
        assert_eq!(
            manifest.precommitment.anchor_vout.unwrap() as usize,
            fanout.outputs.len() - 1
        );
        // The fanout is a version-3 (TRUC) transaction for 0-fee package relay.
        assert_eq!(fanout.version, CTV_FANOUT_TX_VERSION);
    }

    #[test]
    fn ctv_fanout_manifest_omits_anchor_when_parent_pays_fee() {
        let manifest = sample_manifest_from_payouts(sample_positive_fee_payouts());
        let fanout = parse_transaction_hex(&manifest.fanout_tx_hex, "fanout").unwrap();

        assert_eq!(manifest.precommitment.fanout_fee_sats, 120);
        assert_eq!(manifest.precommitment.anchor_vout, None);
        assert_eq!(fanout.outputs.len(), manifest.precommitment.outputs.len());
        assert!(fanout.outputs.iter().all(|output| {
            output.amount_sats > 0 && output.script_pubkey != p2a_anchor_script_pubkey()
        }));
        assert_eq!(
            manifest.precommitment.fanout_output_sum_sats + manifest.precommitment.fanout_fee_sats,
            manifest.covenant_output_value_sats
        );
        verify_ctv_fanout_manifest_structure(&manifest).unwrap();
    }

    #[test]
    fn ctv_fanout_manifest_rejects_positive_fee_with_anchor_output() {
        let payouts = sample_positive_fee_payouts();
        let canonical_outputs = canonical_fanout_outputs(payouts.clone()).unwrap();
        let outputs = canonical_outputs
            .iter()
            .map(|output| TxOutputSpec {
                amount_sats: output.amount_sats,
                script_pubkey: hex::decode(output.script_pubkey_hex.clone()).unwrap(),
            })
            .collect::<Vec<_>>();
        let sequence = expected_fanout_sequence(0).unwrap();
        let unsigned_tx = unsigned_fanout_tx_for_vout_with_anchor(
            PARENT_PREVOUT_TXID,
            0,
            &outputs,
            true,
            BLOCK_HEIGHT as u32,
            sequence,
        );
        let ctv_hash = default_ctv_hash_hex(&unsigned_tx, 0).unwrap();
        let leaf_script = hex::decode(ctv_leaf_script_hex(&ctv_hash).unwrap()).unwrap();
        let p2mr_program =
            hex::decode(p2mr_program_for_ctv_leaf_hex(&hex::encode(&leaf_script)).unwrap())
                .unwrap();
        let covenant_script = p2mr_script_pubkey(p2mr_program.try_into().unwrap());
        let parent_coinbase_tx_hex = coinbase_tx(100_000, &covenant_script);
        let parent_coinbase_txid = parse_transaction_hex(&parent_coinbase_tx_hex, "coinbase")
            .unwrap()
            .txid_hex();
        let fanout_tx_hex = witnessed_fanout_tx_for_vout_with_anchor(
            &parent_coinbase_txid,
            0,
            &outputs,
            true,
            BLOCK_HEIGHT as u32,
            sequence,
            &leaf_script,
        );

        let err = build_ctv_fanout_manifest(CtvFanoutManifestInput {
            block_height: BLOCK_HEIGHT,
            chunk_index: 0,
            chunk_count: 1,
            coinbase_value_sats: 100_000,
            settlement_mode: SettlementMode::CtvFanout,
            fanout_tx_template_hex: unsigned_tx,
            parent_coinbase_tx_hex,
            parent_coinbase_vout: 0,
            fanout_tx_hex,
            reward_manifest_sha256_hex: "aa".repeat(32),
            payout_policy_manifest_sha256_hex: "bb".repeat(32),
            payouts,
        })
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("built-in-fee fanout must have exactly the payout outputs"),
            "{err}"
        );
    }

    #[test]
    fn default_ctv_hash_matches_qbit_reference_vector() {
        let tx_hex = concat!(
            "0300000002",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "0100000003026161feffffff",
            "2222222222222222222222222222222222222222222222222222222222222222",
            "020000000403aabbcc2a000000",
            "02",
            "90d003000000000021200101010101010101010101010101010101010101010101010101010101010101",
            "a8cc0300000000000403020304",
            "11000000",
        );

        assert_eq!(
            default_ctv_hash_hex(tx_hex, 0).unwrap(),
            "348db5ddb9c75ef67bc1e493fcfc7d5c1ab3f7536f66340adb52d0ea9a62c83e"
        );
        assert_eq!(
            default_ctv_hash_hex(tx_hex, 1).unwrap(),
            "d5aa8c19f7104876196cc0f3589031a816728600f41d051ef26e95b6ba75ff8e"
        );
    }

    #[test]
    fn ctv_fanout_manifest_rejects_mutated_output_amount() {
        let mut manifest = sample_manifest();
        let mut tx = hex::decode(&manifest.fanout_tx_hex).unwrap();
        let output_value_offset = 4 + 2 + 1 + 32 + 4 + 1 + 4 + 1;
        tx[output_value_offset] -= 1;
        manifest.fanout_tx_hex = hex::encode(tx);
        manifest.fanout_txid = parse_transaction_hex(&manifest.fanout_tx_hex, "fanout")
            .unwrap()
            .txid_hex();

        assert!(verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string()
            .contains("CTV hash mismatch"));
    }

    #[test]
    fn ctv_fanout_manifest_rejects_parent_outpoint_mismatch() {
        let mut manifest = sample_manifest();
        manifest.parent_coinbase_vout = 1;

        assert!(verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string()
            .contains("parent coinbase vout is out of range"));
    }

    #[test]
    fn ctv_fanout_manifest_rejects_value_drift() {
        let mut manifest = sample_manifest();
        manifest.precommitment.fanout_output_sum_sats = 99_999;
        refresh_precommitment_commitments(&mut manifest);

        assert!(verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string()
            .contains("fanout output sum mismatch"));
    }

    #[test]
    fn ctv_fanout_manifest_rejects_missing_witness_leaf() {
        let mut manifest = sample_manifest();
        let fanout = parse_transaction_hex(&manifest.fanout_tx_hex, "fanout").unwrap();
        manifest.fanout_tx_hex = hex::encode(fanout.tx_without_witness);

        assert!(verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string()
            .contains("must include P2MR witness"));
    }

    #[test]
    fn ctv_fanout_manifest_rejects_non_unique_lock_time() {
        let mut manifest = sample_manifest();
        manifest.precommitment.fanout_lock_time = 0;
        refresh_precommitment_commitments(&mut manifest);

        assert!(verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string()
            .contains("lock_time must equal block height"));
    }

    #[test]
    fn structural_verification_is_not_proof_of_payment() {
        // The "coinbase" in this sample exists on no chain — the test fabricates
        // it. Structural verification still passes, which is precisely why a
        // caller must not treat `Ok` as proof a miner will be paid: on-chain
        // inclusion/PoW/maturity and the commitment leaf of the *real* mined
        // coinbase must be checked separately. With no real witness leaves the
        // commitment check fails closed.
        let manifest = sample_manifest();
        verify_ctv_fanout_manifest_structure(&manifest).unwrap();
        let err = verify_ctv_fanout_manifest_commitment_leaf(&manifest, &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("witness leaf is missing"), "{err}");
    }

    #[test]
    fn ctv_fanout_manifest_rejects_non_coinbase_parent() {
        let manifest = sample_manifest();
        let parent = parse_transaction_hex(&manifest.parent_coinbase_tx_hex, "parent").unwrap();
        let covenant = parent.outputs[0].clone();
        // Same covenant output, but funded by an ordinary tx (real prevout)
        // instead of a coinbase. The covenant would otherwise verify identically.
        let non_coinbase = serialize_tx(
            2,
            &[TxInputSpec {
                prev_txid_hex: "11".repeat(32),
                prev_vout: 0,
                script_sig: Vec::new(),
                sequence: u32::MAX,
                witness_stack: Vec::new(),
            }],
            &[TxOutputSpec {
                amount_sats: covenant.amount_sats,
                script_pubkey: covenant.script_pubkey.clone(),
            }],
            0,
            false,
        );
        let mut manifest = manifest;
        manifest.parent_coinbase_txid = parse_transaction_hex(&non_coinbase, "p")
            .unwrap()
            .txid_hex();
        manifest.parent_coinbase_tx_hex = non_coinbase;

        let err = verify_ctv_fanout_manifest_structure(&manifest)
            .unwrap_err()
            .to_string();
        assert!(err.contains("null outpoint"), "{err}");
    }

    #[test]
    fn parser_rejects_oversized_declared_count_without_allocating() {
        // version(4) + compact-size 0xff with value 0x7fffffffffffffff and no
        // following bytes. The pre-fix code would `Vec::with_capacity` ~9e18;
        // now the declared count is bounded against remaining bytes.
        let tx_hex = "02000000ffffffffffffffff7f";
        let err = default_ctv_hash_hex(tx_hex, 0).unwrap_err().to_string();
        assert!(err.contains("truncated"), "{err}");
    }

    #[test]
    fn parser_rejects_output_above_max_money() {
        let tx_hex = serialize_tx(
            2,
            &[TxInputSpec {
                prev_txid_hex: "00".repeat(32),
                prev_vout: u32::MAX,
                script_sig: vec![1, 1],
                sequence: u32::MAX,
                witness_stack: Vec::new(),
            }],
            &[TxOutputSpec {
                amount_sats: QBIT_MAX_MONEY_SATS + 1,
                script_pubkey: vec![0x51],
            }],
            0,
            false,
        );
        let err = default_ctv_hash_hex(&tx_hex, 0).unwrap_err().to_string();
        assert!(err.contains("above MAX_MONEY"), "{err}");
    }

    #[test]
    fn ctv_fanout_precommitment_hash_matches_golden_vector() {
        // Pins the canonical precommitment wire format so an independent
        // verifier in another language can reproduce the commitment leaf, and
        // so accidental field reordering is caught. Update deliberately only
        // when the schema is intentionally revised.
        let manifest = sample_manifest();
        assert_eq!(
            manifest.precommitment_sha256_hex,
            "1d731134cd1edd87260cc2b43d091ac40d669b34465e6cde4466c45a6eb226ad"
        );
        assert_eq!(
            manifest.commitment_witness_leaf_hex,
            commitment_witness_leaf_hex(&manifest.precommitment_sha256_hex).unwrap()
        );
    }
}
