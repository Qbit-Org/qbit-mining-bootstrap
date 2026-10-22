//! The normalized native audit row keeps only a `PrismRewardManifestHeader`
//! and rebuilds the counted window on read (#267). These tests prove the
//! rebuild is byte-identical, not merely equivalent, and that a header which
//! disagrees with its window is refused by name before any digest runs.

use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, canonical_audit_bundle_bytes, canonical_reward_manifest_bytes,
    restore_reward_manifest, AcceptedShare, AuditBundle, CarryForwardBalance, FoundBlock,
    PayoutPolicy, PrismError, PrismRewardManifest, PrismRewardManifestHeader,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

const WINDOW_SHARES: u64 = 400;
const MINERS: u64 = 7;

/// A few hundred shares from several miners, as in `audit_parts.rs`. The
/// oldest share's difficulty exceeds `u64::MAX`, so it is the partially
/// counted window boundary and its `u128` values exercise arbitrary-precision
/// JSON. `credit_policy_every` marks every n-th share `stale-grace`, which
/// selects the v1.1 schema and exercises the `credit_policy` field that
/// `CountedShare` omits when absent.
fn window_shares(credit_policy_every: Option<u64>) -> Vec<AcceptedShare> {
    (1..=WINDOW_SHARES)
        .map(|share_seq| {
            let miner = share_seq % MINERS;
            let share_difficulty = if share_seq == 1 {
                u128::from(u64::MAX) + 12_345
            } else {
                u128::from((miner + 1) * (miner + 1) * (share_seq % 5 + 1))
            };
            let job_issued_at_ms = 1_000 + i64::try_from(share_seq).unwrap();
            AcceptedShare {
                share_seq,
                share_id: format!("share-{share_seq}"),
                miner_id: format!("miner-{miner}"),
                order_key: format!("{miner:02}"),
                p2mr_program_hex: hex::encode([u8::try_from(miner + 1).unwrap(); 32]),
                share_difficulty,
                network_difficulty: 5_000,
                template_height: 100,
                job_id: format!("job-{job_issued_at_ms}"),
                job_issued_at_ms,
                accepted_at_ms: job_issued_at_ms,
                ntime: 1_800_000_000,
                credit_policy: credit_policy_every
                    .filter(|every| share_seq % every == 0)
                    .map(|_| "stale-grace".to_string()),
            }
        })
        .collect()
}

fn found_block() -> FoundBlock {
    FoundBlock {
        block_height: 101,
        coinbase_value_sats: 500_000_000,
        network_difficulty: 5_000,
        anchor_job_issued_at_ms: 1_000 + i64::try_from(WINDOW_SHARES).unwrap(),
    }
}

fn fixture_bundle(credit_policy_every: Option<u64>) -> AuditBundle {
    build_audit_bundle(
        window_shares(credit_policy_every),
        found_block(),
        vec![CarryForwardBalance {
            recipient_id: "miner-2".to_string(),
            order_key: "02".to_string(),
            p2mr_program_hex: hex::encode([3_u8; 32]),
            balance_sats: 4_800,
        }],
        PayoutPolicy::day_one_default(),
        &ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap(),
        &ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap(),
    )
    .unwrap()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The value a normalized native row stores: the logical bundle minus the
/// top-level `shares` and minus `reward_manifest.shares`.
fn stored_shape(bundle: &AuditBundle) -> Value {
    let mut stored = serde_json::to_value(bundle).unwrap();
    let body = stored.as_object_mut().unwrap();
    body.remove("shares").unwrap();
    body["reward_manifest"]
        .as_object_mut()
        .unwrap()
        .remove("shares")
        .unwrap();
    stored
}

/// Criterion 1: the rebuilt manifest equals the original and the reassembled
/// bundle serializes to the original canonical bytes, byte for byte; a single
/// changed field in one rebuilt share changes the canonical sha256.
#[test]
fn restored_manifest_is_byte_identical_to_the_original() {
    for credit_policy_every in [None, Some(13)] {
        let original = fixture_bundle(credit_policy_every);
        let original_bytes = canonical_audit_bundle_bytes(&original).unwrap();
        let (header, window) = original.reward_manifest.clone().into_parts();
        assert!(
            !window.is_empty() && window.len() == header.included_share_count,
            "the split window must be the counted window"
        );

        let restored =
            restore_reward_manifest(header.clone(), &original.shares, &original.found_block)
                .unwrap();
        assert_eq!(restored, original.reward_manifest);
        assert_eq!(
            restored.shares, window,
            "rebuilt window differs from the split one"
        );
        assert_eq!(
            canonical_reward_manifest_bytes(&restored).unwrap(),
            canonical_reward_manifest_bytes(&original.reward_manifest).unwrap()
        );

        let mut reassembled = original.clone();
        reassembled.reward_manifest = restored;
        let reassembled_bytes = canonical_audit_bundle_bytes(&reassembled).unwrap();
        assert_eq!(reassembled, original);
        assert!(
            reassembled_bytes == original_bytes,
            "canonical bytes differ after reconstruction"
        );
        assert_eq!(sha256_hex(&reassembled_bytes), sha256_hex(&original_bytes));

        // The split-and-join pair is lossless on its own as well.
        let rejoined = header.clone().into_manifest(window.clone());
        assert_eq!(rejoined, original.reward_manifest);

        // The header's serde form round-trips and is what the stored row holds.
        let stored = stored_shape(&original);
        let stored_header: PrismRewardManifestHeader =
            serde_json::from_value(stored["reward_manifest"].clone()).unwrap();
        assert_eq!(stored_header, header);
        assert_eq!(
            serde_json::to_value(&header).unwrap(),
            stored["reward_manifest"],
            "header serialization must equal the stored reward_manifest"
        );

        // Negative: one field of one rebuilt share moves the canonical sha256.
        // The mutation lands on `reassembled`, whose window is the restored
        // one, so this is about the window a read serves: the canonical
        // digest a read checks last is sensitive to each of these fields of
        // a rebuilt share. It is not a test of the rebuild itself; that is
        // the equality with the original above.
        for (field, mutate) in [
            (
                "counted_difficulty",
                (|share: &mut qbit_prism::CountedShare| {
                    share.counted_difficulty += 1;
                }) as fn(&mut qbit_prism::CountedShare),
            ),
            ("miner_id", |share| share.miner_id.push('x')),
            ("credit_policy", |share| {
                share.credit_policy = Some("stale-grace".to_string());
            }),
        ] {
            let mut tampered = reassembled.clone();
            let index = tampered.reward_manifest.shares.len() / 2;
            mutate(&mut tampered.reward_manifest.shares[index]);
            let tampered_bytes = canonical_audit_bundle_bytes(&tampered).unwrap();
            assert_ne!(
                sha256_hex(&tampered_bytes),
                sha256_hex(&original_bytes),
                "changing {field} of one rebuilt share left the canonical sha256 unchanged"
            );
        }
    }
}

/// Criterion 2: a stored header that disagrees with the rebuild fails as the
/// `reward_manifest` mismatch, not later at the canonical digest.
#[test]
fn header_mismatch_is_refused_by_name() {
    let original = fixture_bundle(Some(11));
    let (header, _) = original.reward_manifest.clone().into_parts();
    let tampered: Vec<(&str, PrismRewardManifestHeader)> = vec![
        ("share_slice_digest_hex", {
            let mut h = header.clone();
            h.share_slice_digest_hex = "00".repeat(32);
            h
        }),
        ("counted_window_weight", {
            let mut h = header.clone();
            h.counted_window_weight -= 1;
            h
        }),
        ("included_share_count", {
            let mut h = header.clone();
            h.included_share_count -= 1;
            h
        }),
        ("entitlements", {
            let mut h = header.clone();
            h.entitlements[0].weight += 1;
            h
        }),
        ("newest_share_seq", {
            let mut h = header.clone();
            h.newest_share_seq += 1;
            h
        }),
    ];
    for (field, header) in tampered {
        let error = restore_reward_manifest(header, &original.shares, &original.found_block)
            .expect_err(&format!("a tampered {field} was accepted"));
        assert!(
            matches!(
                error,
                PrismError::AuditMismatch {
                    artifact: "reward_manifest"
                }
            ),
            "tampered {field}: expected the reward_manifest mismatch, got {error}"
        );
    }
    // A window that is not the one the header was folded from is refused too,
    // even when the header itself is untouched.
    let short_window = &original.shares[1..];
    let error = restore_reward_manifest(header.clone(), short_window, &original.found_block)
        .expect_err("a different window was accepted");
    assert!(matches!(
        error,
        PrismError::AuditMismatch {
            artifact: "reward_manifest"
        }
    ));
    // The untouched header against its own window still restores.
    restore_reward_manifest(header, &original.shares, &original.found_block).unwrap();
}

/// EP-COMPAT, both directions. A full manifest cannot be read as a header
/// (it has `shares`, which the header denies), so a legacy row can never be
/// mistaken for a normalized one. And a binary that predates this change
/// cannot decode a normalized row as a bundle: `PrismRewardManifest.shares`
/// has no default, so the decode fails explicitly instead of serving a body
/// with an empty window.
#[test]
fn legacy_and_normalized_shapes_cannot_be_confused() {
    let original = fixture_bundle(None);
    let full_manifest = serde_json::to_value(&original.reward_manifest).unwrap();
    let error = PrismRewardManifestHeader::deserialize(&full_manifest)
        .expect_err("a manifest with shares was decoded as a header");
    assert!(
        error.to_string().contains("unknown field `shares`"),
        "unexpected error: {error}"
    );

    let mut stored = stored_shape(&original);
    stored["shares"] = serde_json::to_value(&original.shares).unwrap();
    let error = AuditBundle::deserialize(&stored)
        .expect_err("an older reader decoded a normalized row as a full bundle");
    assert!(
        error.to_string().contains("missing field `shares`"),
        "unexpected error: {error}"
    );

    // The legacy stored shape still decodes as a full manifest.
    let legacy: PrismRewardManifest = serde_json::from_value(full_manifest).unwrap();
    assert_eq!(legacy, original.reward_manifest);
}
