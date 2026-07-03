use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::fmt::Write;

pub const P2MR_SCRIPT_LEN: usize = 34;
pub const P2MR_PROGRAM_LEN: usize = 32;
pub const P2MR_WITNESS_VERSION_OPCODE: u8 = 0x52;
pub const OP_RETURN: u8 = 0x6a;
pub const WITNESS_COMMITMENT_HEADER: [u8; 4] = [0xaa, 0x21, 0xa9, 0xed];
pub const WITNESS_COMMITMENT_SCRIPT_PREFIX: [u8; 2] = [OP_RETURN, 36];
pub const DEFAULT_WITNESS_NONCE: [u8; 32] = [0; 32];
pub const MAX_SEQUENCE_NONFINAL: u32 = 0xffff_fffe;
pub const MAX_COINBASE_SCRIPT_SIG_LEN: usize = 100;
/// qbit consensus.h: MAX_BLOCK_WEIGHT with WITNESS_SCALE_FACTOR = 1.
pub const MAX_BLOCK_WEIGHT: usize = 2_000_000;

#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error("coinbase value must be positive")]
    EmptyCoinbaseValue,
    #[error("at least one entitlement is required")]
    EmptyEntitlements,
    #[error("entitlement weight must be positive for recipient {recipient_id}")]
    ZeroWeight { recipient_id: String },
    #[error("total entitlement weight overflowed u128")]
    WeightOverflow,
    #[error("allocation arithmetic overflowed")]
    AllocationOverflow,
    #[error("allocated output total {allocated} does not equal coinbase value {coinbase_value}")]
    AllocationTotalMismatch { allocated: u64, coinbase_value: u64 },
    #[error("invalid P2MR program hex for recipient {recipient_id}: {reason}")]
    InvalidP2mrProgram {
        recipient_id: String,
        reason: String,
    },
    #[error("coinbase height must fit in u32")]
    HeightOverflow,
    #[error("coinbase scriptSig length {0} exceeds {MAX_COINBASE_SCRIPT_SIG_LEN} bytes")]
    CoinbaseScriptSigTooLong(usize),
    #[error("coinbase transaction weight {weight} exceeds max block weight {max_weight}")]
    CoinbaseWeightTooHigh { weight: usize, max_weight: usize },
    #[error("hex decode failed: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("json serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid manifest signing key seed length: expected 32 bytes, got {0}")]
    InvalidSigningSeedLength(usize),
    #[error("invalid manifest public key length: expected 32 bytes, got {0}")]
    InvalidPublicKeyLength(usize),
    #[error("invalid manifest signature length: expected 64 bytes, got {0}")]
    InvalidSignatureLength(usize),
    #[error("manifest signature verification failed")]
    SignatureVerificationFailed,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CoinbaseBuildRequest {
    pub block_height: u64,
    pub coinbase_value_sats: u64,
    pub entitlements: Vec<WeightedEntitlement>,
    #[serde(default)]
    pub witness_nonce_hex: Option<String>,
    #[serde(default)]
    pub witness_merkle_leaves_hex: Vec<String>,
    #[serde(default)]
    pub coinbase_script_sig_suffix_hex: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WeightedEntitlement {
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    pub weight: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SignedPayoutManifest {
    pub manifest: PayoutManifest,
    pub signature: ManifestSignature,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PayoutManifest {
    pub schema: String,
    pub block_height: u64,
    pub coinbase_value_sats: u64,
    pub total_weight: u128,
    #[serde(default)]
    pub coinbase_weight_bytes: usize,
    pub payout_count: usize,
    pub coinbase_tx_hex: String,
    pub coinbase_txid: String,
    pub coinbase_wtxid: String,
    pub coinbase_script_sig_hex: String,
    pub coinbase_script_sig_suffix_hex: String,
    pub witness_nonce_hex: String,
    pub witness_commitment_script_hex: String,
    pub outputs: Vec<PayoutOutput>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PayoutOutput {
    pub vout: usize,
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program_hex: String,
    pub script_pubkey_hex: String,
    pub weight: u128,
    pub amount_sats: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ManifestSignature {
    pub algorithm: String,
    pub public_key_hex: String,
    pub signature_hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltCoinbase {
    pub tx_with_witness: Vec<u8>,
    pub tx_without_witness: Vec<u8>,
    pub txid: [u8; 32],
    pub wtxid: [u8; 32],
    pub witness_commitment_script: Vec<u8>,
    pub outputs: Vec<AllocatedOutput>,
    pub total_weight: u128,
    pub coinbase_weight_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocatedOutput {
    pub recipient_id: String,
    pub order_key: String,
    pub p2mr_program: [u8; P2MR_PROGRAM_LEN],
    pub script_pubkey: [u8; P2MR_SCRIPT_LEN],
    pub weight: u128,
    pub amount_sats: u64,
}

pub struct ManifestSigningKey(SigningKey);

impl ManifestSigningKey {
    pub fn from_seed_hex(seed_hex: &str) -> Result<Self, BuilderError> {
        let seed = hex::decode(seed_hex)?;
        if seed.len() != 32 {
            return Err(BuilderError::InvalidSigningSeedLength(seed.len()));
        }
        let mut seed_array = [0_u8; 32];
        seed_array.copy_from_slice(&seed);
        Ok(Self(SigningKey::from_bytes(&seed_array)))
    }

    pub fn public_key_hex(&self) -> String {
        hex::encode(self.0.verifying_key().to_bytes())
    }

    pub fn sign_message_hex(&self, message: &[u8]) -> String {
        hex::encode(self.0.sign(message).to_bytes())
    }
}

pub fn build_signed_manifest(
    request: CoinbaseBuildRequest,
    signing_key: &ManifestSigningKey,
) -> Result<SignedPayoutManifest, BuilderError> {
    let manifest = build_manifest(request)?;
    let canonical_manifest = canonical_manifest_bytes(&manifest)?;
    let signature = signing_key.0.sign(&canonical_manifest);
    Ok(SignedPayoutManifest {
        manifest,
        signature: ManifestSignature {
            algorithm: "ed25519".to_string(),
            public_key_hex: signing_key.public_key_hex(),
            signature_hex: hex::encode(signature.to_bytes()),
        },
    })
}

pub fn verify_signed_manifest(signed: &SignedPayoutManifest) -> Result<(), BuilderError> {
    if signed.signature.algorithm != "ed25519" {
        return Err(BuilderError::SignatureVerificationFailed);
    }
    verify_ed25519_message(
        &signed.signature.public_key_hex,
        &canonical_manifest_bytes(&signed.manifest)?,
        &signed.signature.signature_hex,
    )
}

pub fn verify_ed25519_message(
    public_key_hex: &str,
    message: &[u8],
    signature_hex: &str,
) -> Result<(), BuilderError> {
    let public_key_bytes = hex::decode(public_key_hex)?;
    if public_key_bytes.len() != 32 {
        return Err(BuilderError::InvalidPublicKeyLength(public_key_bytes.len()));
    }
    let mut public_key_array = [0_u8; 32];
    public_key_array.copy_from_slice(&public_key_bytes);
    let public_key = VerifyingKey::from_bytes(&public_key_array)
        .map_err(|_| BuilderError::SignatureVerificationFailed)?;

    let signature_bytes = hex::decode(signature_hex)?;
    if signature_bytes.len() != 64 {
        return Err(BuilderError::InvalidSignatureLength(signature_bytes.len()));
    }
    let mut signature_array = [0_u8; 64];
    signature_array.copy_from_slice(&signature_bytes);
    let signature = Signature::from_bytes(&signature_array);
    public_key
        .verify(message, &signature)
        .map_err(|_| BuilderError::SignatureVerificationFailed)
}

pub fn build_manifest(request: CoinbaseBuildRequest) -> Result<PayoutManifest, BuilderError> {
    let built = build_coinbase(&request)?;
    let coinbase_script_sig = coinbase_script_sig(&request)?;
    let coinbase_script_sig_suffix = coinbase_script_sig_suffix(&request)?;
    let outputs = built
        .outputs
        .iter()
        .enumerate()
        .map(|(index, output)| PayoutOutput {
            vout: index,
            recipient_id: output.recipient_id.clone(),
            order_key: output.order_key.clone(),
            p2mr_program_hex: hex::encode(output.p2mr_program),
            script_pubkey_hex: hex::encode(output.script_pubkey),
            weight: output.weight,
            amount_sats: output.amount_sats,
        })
        .collect::<Vec<_>>();

    Ok(PayoutManifest {
        schema: "qbit.prism.coinbase-manifest.v1".to_string(),
        block_height: request.block_height,
        coinbase_value_sats: request.coinbase_value_sats,
        total_weight: built.total_weight,
        coinbase_weight_bytes: built.coinbase_weight_bytes,
        payout_count: outputs.len(),
        coinbase_tx_hex: hex::encode(&built.tx_with_witness),
        coinbase_txid: display_hash_hex(&built.txid),
        coinbase_wtxid: display_hash_hex(&built.wtxid),
        coinbase_script_sig_hex: hex::encode(coinbase_script_sig),
        coinbase_script_sig_suffix_hex: hex::encode(coinbase_script_sig_suffix),
        witness_nonce_hex: witness_nonce(&request).map(hex::encode)?,
        witness_commitment_script_hex: hex::encode(&built.witness_commitment_script),
        outputs,
    })
}

pub fn build_coinbase(request: &CoinbaseBuildRequest) -> Result<BuiltCoinbase, BuilderError> {
    let outputs = allocate_outputs(request)?;
    let witness_nonce = witness_nonce(request)?;
    let witness_leaves = witness_leaves(request)?;
    let witness_commitment_script = witness_commitment_script(&witness_leaves, &witness_nonce);

    let mut tx_without_witness = Vec::new();
    serialize_coinbase_prefix(&mut tx_without_witness, request)?;
    serialize_outputs(
        &mut tx_without_witness,
        &outputs,
        &witness_commitment_script,
    );
    tx_without_witness.extend_from_slice(&coinbase_lock_time(request.block_height)?.to_le_bytes());

    let mut tx_with_witness = Vec::new();
    tx_with_witness.extend_from_slice(&2_i32.to_le_bytes());
    tx_with_witness.push(0);
    tx_with_witness.push(1);
    serialize_coinbase_input(&mut tx_with_witness, request)?;
    serialize_outputs(&mut tx_with_witness, &outputs, &witness_commitment_script);
    serialize_witness(&mut tx_with_witness, &witness_nonce);
    tx_with_witness.extend_from_slice(&coinbase_lock_time(request.block_height)?.to_le_bytes());
    let coinbase_weight_bytes = tx_with_witness.len();
    if coinbase_weight_bytes > MAX_BLOCK_WEIGHT {
        return Err(BuilderError::CoinbaseWeightTooHigh {
            weight: coinbase_weight_bytes,
            max_weight: MAX_BLOCK_WEIGHT,
        });
    }

    let allocated = outputs
        .iter()
        .try_fold(0_u64, |sum, output| sum.checked_add(output.amount_sats))
        .ok_or(BuilderError::AllocationOverflow)?;
    if allocated != request.coinbase_value_sats {
        return Err(BuilderError::AllocationTotalMismatch {
            allocated,
            coinbase_value: request.coinbase_value_sats,
        });
    }

    Ok(BuiltCoinbase {
        txid: hash256(&tx_without_witness),
        wtxid: hash256(&tx_with_witness),
        tx_with_witness,
        tx_without_witness,
        witness_commitment_script,
        total_weight: outputs
            .iter()
            .try_fold(0_u128, |sum, output| sum.checked_add(output.weight))
            .ok_or(BuilderError::WeightOverflow)?,
        coinbase_weight_bytes,
        outputs,
    })
}

pub fn p2mr_script_pubkey(program: [u8; P2MR_PROGRAM_LEN]) -> [u8; P2MR_SCRIPT_LEN] {
    let mut script = [0_u8; P2MR_SCRIPT_LEN];
    script[0] = P2MR_WITNESS_VERSION_OPCODE;
    script[1] = P2MR_PROGRAM_LEN as u8;
    script[2..].copy_from_slice(&program);
    script
}

pub fn is_p2mr_script_pubkey(script: &[u8]) -> bool {
    script.len() == P2MR_SCRIPT_LEN
        && script[0] == P2MR_WITNESS_VERSION_OPCODE
        && script[1] == P2MR_PROGRAM_LEN as u8
}

pub fn canonical_manifest_bytes(manifest: &PayoutManifest) -> Result<Vec<u8>, BuilderError> {
    serde_json::to_vec(manifest).map_err(BuilderError::from)
}

fn allocate_outputs(request: &CoinbaseBuildRequest) -> Result<Vec<AllocatedOutput>, BuilderError> {
    if request.coinbase_value_sats == 0 {
        return Err(BuilderError::EmptyCoinbaseValue);
    }
    if request.entitlements.is_empty() {
        return Err(BuilderError::EmptyEntitlements);
    }

    let mut entitlements = request.entitlements.clone();
    entitlements.sort_by(compare_entitlements);

    let total_weight = entitlements.iter().try_fold(0_u128, |sum, entitlement| {
        if entitlement.weight == 0 {
            return Err(BuilderError::ZeroWeight {
                recipient_id: entitlement.recipient_id.clone(),
            });
        }
        sum.checked_add(entitlement.weight)
            .ok_or(BuilderError::WeightOverflow)
    })?;

    let mut provisional = entitlements
        .into_iter()
        .map(|entitlement| {
            let program = parse_p2mr_program(&entitlement)?;
            let product = u128::from(request.coinbase_value_sats)
                .checked_mul(entitlement.weight)
                .ok_or(BuilderError::AllocationOverflow)?;
            let base = product / total_weight;
            let remainder = product % total_weight;
            let amount_sats = u64::try_from(base).map_err(|_| BuilderError::AllocationOverflow)?;
            Ok((
                AllocatedOutput {
                    recipient_id: entitlement.recipient_id,
                    order_key: entitlement.order_key,
                    p2mr_program: program,
                    script_pubkey: p2mr_script_pubkey(program),
                    weight: entitlement.weight,
                    amount_sats,
                },
                remainder,
            ))
        })
        .collect::<Result<Vec<_>, BuilderError>>()?;

    let allocated = provisional
        .iter()
        .try_fold(0_u64, |sum, (output, _)| {
            sum.checked_add(output.amount_sats)
        })
        .ok_or(BuilderError::AllocationOverflow)?;
    let remainder_sats = request
        .coinbase_value_sats
        .checked_sub(allocated)
        .ok_or(BuilderError::AllocationOverflow)?;

    let mut remainder_order = (0..provisional.len()).collect::<Vec<_>>();
    remainder_order.sort_by(|left, right| {
        let left_remainder = provisional[*left].1;
        let right_remainder = provisional[*right].1;
        right_remainder
            .cmp(&left_remainder)
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
                    .p2mr_program
                    .cmp(&provisional[*right].0.p2mr_program)
            })
    });

    for index in remainder_order.into_iter().take(remainder_sats as usize) {
        provisional[index].0.amount_sats = provisional[index]
            .0
            .amount_sats
            .checked_add(1)
            .ok_or(BuilderError::AllocationOverflow)?;
    }

    let outputs = provisional
        .into_iter()
        .map(|(output, _)| output)
        .collect::<Vec<_>>();
    let final_total = outputs
        .iter()
        .try_fold(0_u64, |sum, output| sum.checked_add(output.amount_sats))
        .ok_or(BuilderError::AllocationOverflow)?;
    if final_total != request.coinbase_value_sats {
        return Err(BuilderError::AllocationTotalMismatch {
            allocated: final_total,
            coinbase_value: request.coinbase_value_sats,
        });
    }

    Ok(outputs)
}

fn compare_entitlements(left: &WeightedEntitlement, right: &WeightedEntitlement) -> Ordering {
    left.order_key
        .cmp(&right.order_key)
        .then_with(|| left.recipient_id.cmp(&right.recipient_id))
        .then_with(|| left.p2mr_program_hex.cmp(&right.p2mr_program_hex))
}

fn parse_p2mr_program(
    entitlement: &WeightedEntitlement,
) -> Result<[u8; P2MR_PROGRAM_LEN], BuilderError> {
    let program = hex::decode(&entitlement.p2mr_program_hex).map_err(|error| {
        BuilderError::InvalidP2mrProgram {
            recipient_id: entitlement.recipient_id.clone(),
            reason: error.to_string(),
        }
    })?;
    if program.len() != P2MR_PROGRAM_LEN {
        return Err(BuilderError::InvalidP2mrProgram {
            recipient_id: entitlement.recipient_id.clone(),
            reason: format!("expected 32 bytes, got {}", program.len()),
        });
    }
    let mut program_array = [0_u8; P2MR_PROGRAM_LEN];
    program_array.copy_from_slice(&program);
    Ok(program_array)
}

fn witness_nonce(request: &CoinbaseBuildRequest) -> Result<[u8; 32], BuilderError> {
    match request.witness_nonce_hex.as_deref() {
        Some(raw) => {
            let decoded = hex::decode(raw)?;
            if decoded.len() != 32 {
                return Err(BuilderError::InvalidP2mrProgram {
                    recipient_id: "witness_nonce".to_string(),
                    reason: format!("expected 32 bytes, got {}", decoded.len()),
                });
            }
            let mut nonce = [0_u8; 32];
            nonce.copy_from_slice(&decoded);
            Ok(nonce)
        }
        None => Ok(DEFAULT_WITNESS_NONCE),
    }
}

fn witness_leaves(request: &CoinbaseBuildRequest) -> Result<Vec<[u8; 32]>, BuilderError> {
    request
        .witness_merkle_leaves_hex
        .iter()
        .enumerate()
        .map(|(index, raw)| {
            let decoded = hex::decode(raw)?;
            if decoded.len() != 32 {
                return Err(BuilderError::InvalidP2mrProgram {
                    recipient_id: format!("witness_merkle_leaf[{index}]"),
                    reason: format!("expected 32 bytes, got {}", decoded.len()),
                });
            }
            let mut leaf = [0_u8; 32];
            leaf.copy_from_slice(&decoded);
            Ok(leaf)
        })
        .collect()
}

fn witness_commitment_script(
    extra_witness_leaves: &[[u8; 32]],
    witness_nonce: &[u8; 32],
) -> Vec<u8> {
    let witness_root = witness_merkle_root(extra_witness_leaves);
    let commitment = hash256_pair(&witness_root, witness_nonce);
    let mut script = Vec::with_capacity(38);
    script.extend_from_slice(&WITNESS_COMMITMENT_SCRIPT_PREFIX);
    script.extend_from_slice(&WITNESS_COMMITMENT_HEADER);
    script.extend_from_slice(&commitment);
    script
}

fn witness_merkle_root(extra_witness_leaves: &[[u8; 32]]) -> [u8; 32] {
    let mut hashes = Vec::with_capacity(extra_witness_leaves.len() + 1);
    hashes.push([0_u8; 32]);
    hashes.extend_from_slice(extra_witness_leaves);
    merkle_root(hashes)
}

fn merkle_root(mut hashes: Vec<[u8; 32]>) -> [u8; 32] {
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
    let mut data = [0_u8; 64];
    data[..32].copy_from_slice(left);
    data[32..].copy_from_slice(right);
    hash256(&data)
}

fn hash256(data: &[u8]) -> [u8; 32] {
    let first = Sha256::digest(data);
    let second = Sha256::digest(first);
    let mut out = [0_u8; 32];
    out.copy_from_slice(&second);
    out
}

fn display_hash_hex(internal_le_hash: &[u8; 32]) -> String {
    let mut hex = String::with_capacity(64);
    for byte in internal_le_hash.iter().rev() {
        write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
    }
    hex
}

fn serialize_coinbase_prefix(
    out: &mut Vec<u8>,
    request: &CoinbaseBuildRequest,
) -> Result<(), BuilderError> {
    out.extend_from_slice(&2_i32.to_le_bytes());
    serialize_coinbase_input(out, request)
}

fn serialize_coinbase_input(
    out: &mut Vec<u8>,
    request: &CoinbaseBuildRequest,
) -> Result<(), BuilderError> {
    out.extend_from_slice(&compact_size(1));
    out.extend_from_slice(&[0_u8; 32]);
    out.extend_from_slice(&0xffff_ffff_u32.to_le_bytes());
    let script_sig = coinbase_script_sig(request)?;
    out.extend_from_slice(&compact_size(script_sig.len() as u64));
    out.extend_from_slice(&script_sig);
    out.extend_from_slice(&MAX_SEQUENCE_NONFINAL.to_le_bytes());
    Ok(())
}

fn serialize_outputs(
    out: &mut Vec<u8>,
    outputs: &[AllocatedOutput],
    witness_commitment_script: &[u8],
) {
    out.extend_from_slice(&compact_size((outputs.len() + 1) as u64));
    for output in outputs {
        out.extend_from_slice(&(output.amount_sats as i64).to_le_bytes());
        out.extend_from_slice(&compact_size(output.script_pubkey.len() as u64));
        out.extend_from_slice(&output.script_pubkey);
    }
    out.extend_from_slice(&0_i64.to_le_bytes());
    out.extend_from_slice(&compact_size(witness_commitment_script.len() as u64));
    out.extend_from_slice(witness_commitment_script);
}

fn serialize_witness(out: &mut Vec<u8>, witness_nonce: &[u8; 32]) {
    out.extend_from_slice(&compact_size(1));
    out.extend_from_slice(&compact_size(witness_nonce.len() as u64));
    out.extend_from_slice(witness_nonce);
}

fn compact_size(value: u64) -> Vec<u8> {
    if value < 253 {
        vec![value as u8]
    } else if value <= 0xffff {
        let mut out = vec![253];
        out.extend_from_slice(&(value as u16).to_le_bytes());
        out
    } else if value <= 0xffff_ffff {
        let mut out = vec![254];
        out.extend_from_slice(&(value as u32).to_le_bytes());
        out
    } else {
        let mut out = vec![255];
        out.extend_from_slice(&value.to_le_bytes());
        out
    }
}

fn coinbase_height_script(height: u64) -> Result<Vec<u8>, BuilderError> {
    if height > u64::from(u32::MAX) {
        return Err(BuilderError::HeightOverflow);
    }
    if height <= 16 {
        // qbit/Bitcoin Core validate the BIP34 height as a script prefix. For
        // small heights that prefix is OP_N; the following OP_0 is extra
        // coinbase data so the scriptSig still satisfies the 2-byte minimum.
        return Ok(vec![0x50 + height as u8, 0x00]);
    }
    let mut encoded = minimal_script_num(height as i64);
    let mut script = compact_push(encoded.len());
    script.append(&mut encoded);
    Ok(script)
}

fn coinbase_script_sig(request: &CoinbaseBuildRequest) -> Result<Vec<u8>, BuilderError> {
    let mut script = coinbase_height_script(request.block_height)?;
    script.extend_from_slice(&coinbase_script_sig_suffix(request)?);
    if script.len() > MAX_COINBASE_SCRIPT_SIG_LEN {
        return Err(BuilderError::CoinbaseScriptSigTooLong(script.len()));
    }
    Ok(script)
}

fn coinbase_script_sig_suffix(request: &CoinbaseBuildRequest) -> Result<Vec<u8>, BuilderError> {
    match request.coinbase_script_sig_suffix_hex.as_deref() {
        Some(raw) => Ok(hex::decode(raw)?),
        None => Ok(Vec::new()),
    }
}

fn minimal_script_num(value: i64) -> Vec<u8> {
    if value == 0 {
        return Vec::new();
    }
    let negative = value < 0;
    let mut abs_value = if negative { -value } else { value } as u64;
    let mut result = Vec::new();
    while abs_value > 0 {
        result.push((abs_value & 0xff) as u8);
        abs_value >>= 8;
    }
    if result.last().is_some_and(|last| last & 0x80 != 0) {
        result.push(if negative { 0x80 } else { 0x00 });
    } else if negative {
        let last = result.last_mut().expect("non-zero values have a byte");
        *last |= 0x80;
    }
    result
}

fn compact_push(len: usize) -> Vec<u8> {
    match len {
        0..=75 => vec![len as u8],
        76..=0xff => vec![76, len as u8],
        0x100..=0xffff => {
            let mut out = vec![77];
            out.extend_from_slice(&(len as u16).to_le_bytes());
            out
        }
        _ => {
            let mut out = vec![78];
            out.extend_from_slice(&(len as u32).to_le_bytes());
            out
        }
    }
}

fn coinbase_lock_time(height: u64) -> Result<u32, BuilderError> {
    if height == 0 || height > u64::from(u32::MAX) {
        return Err(BuilderError::HeightOverflow);
    }
    Ok((height - 1) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIGNING_SEED: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    fn program(byte: u8) -> String {
        hex::encode([byte; 32])
    }

    fn entitlement(id: &str, order_key: &str, p2mr_byte: u8, weight: u128) -> WeightedEntitlement {
        WeightedEntitlement {
            recipient_id: id.to_string(),
            order_key: order_key.to_string(),
            p2mr_program_hex: program(p2mr_byte),
            weight,
        }
    }

    fn request(entitlements: Vec<WeightedEntitlement>) -> CoinbaseBuildRequest {
        CoinbaseBuildRequest {
            block_height: 42,
            coinbase_value_sats: 500_000_000,
            entitlements,
            witness_nonce_hex: None,
            witness_merkle_leaves_hex: Vec::new(),
            coinbase_script_sig_suffix_hex: None,
        }
    }

    #[test]
    fn deterministic_coinbase_and_manifest_signature_verify() {
        let request = request(vec![
            entitlement("miner-c", "03", 3, 1),
            entitlement("miner-a", "01", 1, 1),
            entitlement("miner-b", "02", 2, 1),
        ]);
        let key = ManifestSigningKey::from_seed_hex(SIGNING_SEED).unwrap();
        let left = build_signed_manifest(request.clone(), &key).unwrap();
        let right = build_signed_manifest(request, &key).unwrap();

        assert_eq!(left, right);
        verify_signed_manifest(&left).unwrap();
        assert_eq!(
            left.manifest
                .outputs
                .iter()
                .map(|out| out.recipient_id.as_str())
                .collect::<Vec<_>>(),
            vec!["miner-a", "miner-b", "miner-c"]
        );
        assert_eq!(
            left.manifest
                .outputs
                .iter()
                .map(|out| out.amount_sats)
                .sum::<u64>(),
            left.manifest.coinbase_value_sats
        );
    }

    #[test]
    fn weighted_split_uses_largest_remainder_with_stable_tie_break() {
        let mut request = request(vec![
            entitlement("miner-b", "02", 2, 1),
            entitlement("miner-a", "01", 1, 1),
            entitlement("miner-c", "03", 3, 1),
        ]);
        request.coinbase_value_sats = 10;

        let manifest = build_manifest(request).unwrap();
        assert_eq!(
            manifest
                .outputs
                .iter()
                .map(|out| (out.recipient_id.as_str(), out.amount_sats))
                .collect::<Vec<_>>(),
            vec![("miner-a", 4), ("miner-b", 3), ("miner-c", 3)]
        );
    }

    #[test]
    fn p2mr_outputs_and_witness_commitment_are_encoded() {
        let manifest = build_manifest(request(vec![entitlement("miner-a", "01", 7, 1)])).unwrap();
        assert_eq!(manifest.outputs.len(), 1);
        assert!(manifest.outputs[0].script_pubkey_hex.starts_with("5220"));
        assert_eq!(
            hex::decode(&manifest.outputs[0].script_pubkey_hex)
                .unwrap()
                .len(),
            P2MR_SCRIPT_LEN
        );
        assert!(manifest
            .witness_commitment_script_hex
            .starts_with("6a24aa21a9ed"));
        assert!(manifest
            .coinbase_tx_hex
            .contains(&manifest.witness_commitment_script_hex));
    }

    #[test]
    fn coinbase_height_script_uses_bip34_prefix_with_min_length_padding() {
        assert_eq!(coinbase_height_script(1).unwrap(), vec![0x51, 0x00]);
        assert_eq!(coinbase_height_script(16).unwrap(), vec![0x60, 0x00]);
        assert_eq!(coinbase_height_script(17).unwrap(), vec![0x01, 0x11]);
        assert_eq!(coinbase_height_script(128).unwrap(), vec![0x02, 0x80, 0x00]);
    }

    #[test]
    fn witness_commitment_uses_ordered_internal_wtxid_leaves() {
        let mut request = request(vec![entitlement("miner-a", "01", 7, 1)]);
        let leaf_a = [0x11_u8; 32];
        let leaf_b = [0x22_u8; 32];
        request.witness_merkle_leaves_hex = vec![hex::encode(leaf_a), hex::encode(leaf_b)];

        let key = ManifestSigningKey::from_seed_hex(SIGNING_SEED).unwrap();
        let signed = build_signed_manifest(request, &key).unwrap();
        verify_signed_manifest(&signed).unwrap();

        let expected_root = merkle_root(vec![[0_u8; 32], leaf_a, leaf_b]);
        let expected_commitment = hash256_pair(&expected_root, &DEFAULT_WITNESS_NONCE);
        assert_eq!(
            signed.manifest.witness_commitment_script_hex,
            format!("6a24aa21a9ed{}", hex::encode(expected_commitment))
        );
    }

    #[test]
    fn rejects_non_32_byte_p2mr_program() {
        let mut bad = entitlement("miner-a", "01", 1, 1);
        bad.p2mr_program_hex = "abcd".to_string();
        assert!(matches!(
            build_manifest(request(vec![bad])),
            Err(BuilderError::InvalidP2mrProgram { .. })
        ));
    }

    #[test]
    fn signature_fails_after_manifest_tamper() {
        let key = ManifestSigningKey::from_seed_hex(SIGNING_SEED).unwrap();
        let mut manifest =
            build_signed_manifest(request(vec![entitlement("miner-a", "01", 1, 1)]), &key).unwrap();
        manifest.manifest.outputs[0].amount_sats -= 1;
        assert!(matches!(
            verify_signed_manifest(&manifest),
            Err(BuilderError::SignatureVerificationFailed)
        ));
    }

    #[test]
    fn allocation_sums_exactly_across_generated_cases() {
        let coinbase_values = [1_u64, 2, 17, 499, 50_000, 500_000_000, 21_000_000_000_000];
        let miner_counts = [1_usize, 2, 3, 17, 64, 127];

        for coinbase_value in coinbase_values {
            for miner_count in miner_counts {
                let entitlements = (0..miner_count)
                    .map(|index| {
                        entitlement(
                            &format!("miner-{index:03}"),
                            &format!("{index:06}"),
                            (index % 251 + 1) as u8,
                            (index as u128 % 19) + 1,
                        )
                    })
                    .collect::<Vec<_>>();
                let request = CoinbaseBuildRequest {
                    block_height: 100,
                    coinbase_value_sats: coinbase_value,
                    entitlements,
                    witness_nonce_hex: None,
                    witness_merkle_leaves_hex: Vec::new(),
                    coinbase_script_sig_suffix_hex: None,
                };

                let manifest = build_manifest(request).unwrap();
                assert_eq!(
                    manifest
                        .outputs
                        .iter()
                        .map(|output| output.amount_sats)
                        .sum::<u64>(),
                    coinbase_value
                );
                assert!(manifest.outputs.iter().all(|output| {
                    is_p2mr_script_pubkey(&hex::decode(&output.script_pubkey_hex).unwrap())
                }));
            }
        }
    }

    #[test]
    fn script_sig_suffix_supports_stratum_extranonce_templates() {
        let mut left_request = request(vec![entitlement("miner-a", "01", 1, 1)]);
        left_request.coinbase_script_sig_suffix_hex = Some("111111112222222222222222".to_string());
        let mut right_request = left_request.clone();
        right_request.coinbase_script_sig_suffix_hex = Some("111111113333333333333333".to_string());

        let left = build_manifest(left_request).unwrap();
        let right = build_manifest(right_request).unwrap();

        assert_ne!(left.coinbase_tx_hex, right.coinbase_tx_hex);
        assert_ne!(left.coinbase_txid, right.coinbase_txid);
        assert_ne!(left.coinbase_wtxid, right.coinbase_wtxid);
        assert_eq!(
            left.witness_commitment_script_hex,
            right.witness_commitment_script_hex
        );
        assert_eq!(left.outputs, right.outputs);
        assert!(left
            .coinbase_script_sig_hex
            .ends_with("111111112222222222222222"));
        assert_eq!(
            left.coinbase_script_sig_suffix_hex,
            "111111112222222222222222"
        );
    }

    #[test]
    fn rejects_oversized_coinbase_script_sig_suffix() {
        let mut request = request(vec![entitlement("miner-a", "01", 1, 1)]);
        request.coinbase_script_sig_suffix_hex = Some("ff".repeat(MAX_COINBASE_SCRIPT_SIG_LEN));

        assert!(matches!(
            build_manifest(request),
            Err(BuilderError::CoinbaseScriptSigTooLong(_))
        ));
    }
}
