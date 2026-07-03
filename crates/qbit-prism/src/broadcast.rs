//! Non-custodial CPFP broadcaster primitives for CTV fanout settlement
//! for recovery artifacts.
//!
//! The fanout committed by [`crate::build_ctv_fanout_manifest`] is a 0-fee
//! version-3 (TRUC) transaction whose last output is a keyless P2A ephemeral
//! anchor. After the coinbase matures, the fanout must be broadcast, but its fee
//! is fixed and cannot be raised. The broadcaster therefore attaches a
//! fee-paying **child** that spends the anchor and submits both as a package
//! (child-pays-for-parent), choosing the effective fee at broadcast time.
//!
//! # The broadcaster is not a custodian
//!
//! It holds no key over miner funds and cannot change a payout: the fanout's
//! outputs are fixed by the covenant. It only pays for confirmation. Because the
//! anchor is keyless ([`crate::P2A_ANCHOR_SCRIPT_PUBKEY_HEX`]), **any owed miner
//! can build and broadcast the rescue child themselves** if the pool's
//! broadcaster is down — [`build_cpfp_child`] is exactly that key-free builder.
//!
//! # What this module covers (and what it does not)
//!
//! [`build_cpfp_child`] produces the *unsigned* child: it spends the anchor
//! (which needs no signature) plus a caller-supplied funding input (which the
//! caller's wallet/node signs — no key is handled here), and sends the change
//! back to the caller. [`fanout_cpfp_package`] assembles the `[parent, child]`
//! package for `submitpackage`. [`fanout_settlement_status`] is the pure status
//! state machine behind a read-only miner-facing status surface.
//!
//! The live orchestration — polling the node for maturity, funding/signing the
//! child via wallet RPC, calling `submitpackage`, rebroadcasting/fee-bumping the
//! child, and serving the status HTTP endpoint — requires a running qbit node
//! and is intentionally out of this library. It must be validated end-to-end on
//! a live (signet) node before any production claim.

use crate::{PrismError, CTV_FANOUT_TX_VERSION, QBIT_COINBASE_MATURITY_BLOCKS};
use serde::{Deserialize, Serialize};

/// The fee-bump child is itself a version-3 (TRUC) transaction so the
/// 0-fee parent + child relay together as a package.
pub const CPFP_CHILD_TX_VERSION: u32 = CTV_FANOUT_TX_VERSION;
/// RBF-signalling, non-final sequence so the broadcaster can re-issue the child
/// at a higher fee while chasing the fee market.
pub const CPFP_CHILD_SEQUENCE: u32 = 0xffff_fffd;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CpfpChildRequest {
    /// Display (big-endian) txid of the parent fanout transaction.
    pub fanout_txid: String,
    /// Index of the parent fanout's P2A anchor output (its `anchor_vout`).
    pub anchor_vout: u32,
    /// Display txid of the broadcaster's own funding UTXO.
    pub funding_txid: String,
    pub funding_vout: u32,
    /// Value of the funding UTXO. The fee is taken from this; the remainder is
    /// returned as change. The anchor contributes nothing (it is value 0).
    pub funding_value_sats: u64,
    /// Total package fee to pay (must cover the 0-fee parent and the child at
    /// the desired feerate). The caller computes this from the parent + child
    /// vsize and the target feerate.
    pub fee_sats: u64,
    /// scriptPubKey the change is returned to (the broadcaster's own address).
    pub change_script_pubkey_hex: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CpfpChild {
    /// Unsigned legacy-serialized child. Input 0 (the anchor) needs no
    /// signature; input 1 (funding) must be signed by the caller's wallet
    /// before broadcast.
    pub unsigned_child_tx_hex: String,
    pub change_value_sats: u64,
    pub fee_sats: u64,
}

/// Build the unsigned CPFP child that fee-bumps a fanout by spending its keyless
/// P2A anchor plus a funding input. No private key is handled: the anchor is
/// anyone-can-spend and the funding input is left unsigned for the caller's
/// wallet to sign.
pub fn build_cpfp_child(request: &CpfpChildRequest) -> Result<CpfpChild, PrismError> {
    let fanout_txid = decode_txid(&request.fanout_txid, "fanout_txid")?;
    let funding_txid = decode_txid(&request.funding_txid, "funding_txid")?;
    if fanout_txid == funding_txid && request.anchor_vout == request.funding_vout {
        return Err(invalid(
            "funding input must differ from the anchor outpoint",
        ));
    }
    let change_script_pubkey = hex::decode(&request.change_script_pubkey_hex)
        .map_err(|err| invalid(format!("change_script_pubkey_hex must be hex: {err}")))?;
    if change_script_pubkey.is_empty() {
        return Err(invalid("change scriptPubKey must not be empty"));
    }
    if request.fee_sats == 0 {
        return Err(invalid("CPFP fee must be positive"));
    }
    if request.fee_sats >= request.funding_value_sats {
        return Err(invalid(
            "funding value must exceed the fee so the change output is positive",
        ));
    }
    // The anchor is value 0, so the child's input value is the funding value
    // alone; change = funding - fee.
    let change_value_sats = request.funding_value_sats - request.fee_sats;

    let mut tx = Vec::new();
    tx.extend_from_slice(&CPFP_CHILD_TX_VERSION.to_le_bytes());
    tx.extend_from_slice(&compact_size(2));
    serialize_input(&mut tx, &fanout_txid, request.anchor_vout);
    serialize_input(&mut tx, &funding_txid, request.funding_vout);
    tx.extend_from_slice(&compact_size(1));
    serialize_output(&mut tx, change_value_sats, &change_script_pubkey);
    tx.extend_from_slice(&0u32.to_le_bytes()); // lock_time

    Ok(CpfpChild {
        unsigned_child_tx_hex: hex::encode(tx),
        change_value_sats,
        fee_sats: request.fee_sats,
    })
}

/// Assemble the `submitpackage` argument: the parent fanout followed by its
/// signed CPFP child, topologically ordered (parent first, child last).
pub fn fanout_cpfp_package(parent_fanout_tx_hex: &str, signed_child_tx_hex: &str) -> Vec<String> {
    vec![
        parent_fanout_tx_hex.to_string(),
        signed_child_tx_hex.to_string(),
    ]
}

/// Settlement status of a CTV fanout, derived purely from chain facts. This is
/// the logic behind the read-only, miner-facing status surface.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FanoutSettlementStatus {
    /// The funding coinbase has been disconnected from the active chain and the
    /// fanout has not confirmed: the payout must be recomputed against the new
    /// tip.
    Reorged,
    /// The coinbase is on the active chain but has not reached spend maturity.
    AwaitingMaturity,
    /// The coinbase is mature; the fanout package can be broadcast.
    Broadcastable,
    /// The fanout transaction has confirmed; the payout is final.
    Confirmed,
}

/// Decide the settlement status from chain facts. A confirmed fanout is always
/// `Confirmed` (it is in the chain regardless of the coinbase's reorg state).
pub fn fanout_settlement_status(
    coinbase_height: u64,
    active_tip_height: u64,
    coinbase_in_active_chain: bool,
    fanout_confirmed: bool,
) -> FanoutSettlementStatus {
    if fanout_confirmed {
        return FanoutSettlementStatus::Confirmed;
    }
    if !coinbase_in_active_chain {
        return FanoutSettlementStatus::Reorged;
    }
    if active_tip_height < coinbase_height.saturating_add(QBIT_COINBASE_MATURITY_BLOCKS) {
        return FanoutSettlementStatus::AwaitingMaturity;
    }
    FanoutSettlementStatus::Broadcastable
}

fn decode_txid(value: &str, field_name: &str) -> Result<[u8; 32], PrismError> {
    let bytes =
        hex::decode(value).map_err(|err| invalid(format!("{field_name} must be hex: {err}")))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        invalid(format!(
            "{field_name} must be 32 bytes, got {}",
            bytes.len()
        ))
    })
}

fn serialize_input(out: &mut Vec<u8>, display_txid: &[u8; 32], vout: u32) {
    // Transactions serialize the prevout txid in internal (reversed) byte order.
    let mut internal = *display_txid;
    internal.reverse();
    out.extend_from_slice(&internal);
    out.extend_from_slice(&vout.to_le_bytes());
    out.push(0x00); // empty scriptSig (compact size 0); witness added when signed
    out.extend_from_slice(&CPFP_CHILD_SEQUENCE.to_le_bytes());
}

fn serialize_output(out: &mut Vec<u8>, value_sats: u64, script_pubkey: &[u8]) {
    out.extend_from_slice(&value_sats.to_le_bytes());
    out.extend_from_slice(&compact_size(script_pubkey.len() as u64));
    out.extend_from_slice(script_pubkey);
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

fn invalid(reason: impl Into<String>) -> PrismError {
    PrismError::InvalidCtvFanoutManifest {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> CpfpChildRequest {
        CpfpChildRequest {
            fanout_txid: "ab".repeat(32),
            anchor_vout: 2,
            funding_txid: "cd".repeat(32),
            funding_vout: 1,
            funding_value_sats: 50_000,
            fee_sats: 12_000,
            change_script_pubkey_hex: format!("5220{}", "11".repeat(32)),
        }
    }

    #[test]
    fn build_cpfp_child_spends_anchor_and_funding() {
        let child = build_cpfp_child(&sample_request()).unwrap();

        assert_eq!(child.change_value_sats, 38_000);
        assert_eq!(child.fee_sats, 12_000);
        let hex = &child.unsigned_child_tx_hex;
        // Version 3 (TRUC), then two inputs.
        assert!(hex.starts_with("03000000"), "{hex}");
        // Both prevout txids appear (the bytes are palindromic so reversal is
        // a no-op for this fixture), and the change scriptPubKey is present.
        assert!(hex.contains(&"ab".repeat(32)));
        assert!(hex.contains(&"cd".repeat(32)));
        assert!(hex.contains(&format!("5220{}", "11".repeat(32))));
        // RBF-signalling sequence so the child can be re-bumped.
        assert!(hex.contains("fdffffff"));
    }

    #[test]
    fn build_cpfp_child_reverses_non_palindromic_prevout_txids() {
        let mut request = sample_request();
        request.fanout_txid =
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f".to_string();
        request.funding_txid =
            "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f".to_string();

        let child = build_cpfp_child(&request).unwrap();

        assert!(child
            .unsigned_child_tx_hex
            .contains("1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"));
        assert!(child
            .unsigned_child_tx_hex
            .contains("3f3e3d3c3b3a393837363534333231302f2e2d2c2b2a29282726252423222120"));
        assert!(!child.unsigned_child_tx_hex.contains(&request.fanout_txid));
        assert!(!child.unsigned_child_tx_hex.contains(&request.funding_txid));
    }

    #[test]
    fn build_cpfp_child_is_deterministic() {
        assert_eq!(
            build_cpfp_child(&sample_request()).unwrap(),
            build_cpfp_child(&sample_request()).unwrap()
        );
    }

    #[test]
    fn build_cpfp_child_rejects_fee_at_or_above_funding() {
        let mut request = sample_request();
        request.fee_sats = request.funding_value_sats;
        assert!(build_cpfp_child(&request)
            .unwrap_err()
            .to_string()
            .contains("funding value must exceed the fee"));
    }

    #[test]
    fn build_cpfp_child_rejects_zero_fee() {
        let mut request = sample_request();
        request.fee_sats = 0;
        assert!(build_cpfp_child(&request)
            .unwrap_err()
            .to_string()
            .contains("fee must be positive"));
    }

    #[test]
    fn build_cpfp_child_rejects_funding_equal_to_anchor() {
        let mut request = sample_request();
        request.funding_txid = request.fanout_txid.clone();
        request.funding_vout = request.anchor_vout;
        assert!(build_cpfp_child(&request)
            .unwrap_err()
            .to_string()
            .contains("must differ from the anchor outpoint"));
    }

    #[test]
    fn fanout_cpfp_package_orders_parent_then_child() {
        assert_eq!(
            fanout_cpfp_package("aa", "bb"),
            vec!["aa".to_string(), "bb".to_string()]
        );
    }

    #[test]
    fn fanout_settlement_status_tracks_chain_state() {
        // Immature: tip below coinbase_height + maturity.
        assert_eq!(
            fanout_settlement_status(100, 100 + QBIT_COINBASE_MATURITY_BLOCKS - 1, true, false),
            FanoutSettlementStatus::AwaitingMaturity
        );
        // Mature and unbroadcast: broadcastable.
        assert_eq!(
            fanout_settlement_status(100, 100 + QBIT_COINBASE_MATURITY_BLOCKS, true, false),
            FanoutSettlementStatus::Broadcastable
        );
        // Coinbase reorged out before the fanout confirmed.
        assert_eq!(
            fanout_settlement_status(100, 100 + QBIT_COINBASE_MATURITY_BLOCKS, false, false),
            FanoutSettlementStatus::Reorged
        );
        // A confirmed fanout is final regardless of the coinbase reorg flag.
        assert_eq!(
            fanout_settlement_status(100, 100 + QBIT_COINBASE_MATURITY_BLOCKS, false, true),
            FanoutSettlementStatus::Confirmed
        );
    }
}
