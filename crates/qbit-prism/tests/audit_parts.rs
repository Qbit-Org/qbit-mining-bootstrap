//! Equivalence between the owned `build_audit_bundle*` entry points and the
//! borrowed parts API (`build_audit_body*`, `verify_audit_parts*`,
//! `canonical_audit_bundle_bytes_from_parts`).

use std::sync::Arc;

use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_body, build_audit_body_with_coinbase_options,
    build_audit_body_with_coinbase_script_sig_suffix, build_audit_body_with_ctv_settlement_options,
    build_audit_bundle, build_audit_bundle_with_coinbase_options,
    build_audit_bundle_with_coinbase_script_sig_suffix,
    build_audit_bundle_with_ctv_settlement_options, canonical_audit_bundle_bytes,
    canonical_audit_bundle_bytes_from_parts, verify_audit_bundle,
    verify_audit_bundle_against_coinbase_tx_hex,
    verify_audit_bundle_against_coinbase_tx_hex_and_expected_coinbase_value, verify_audit_parts,
    verify_audit_parts_against_coinbase_tx_hex,
    verify_audit_parts_against_coinbase_tx_hex_and_expected_coinbase_value,
    write_canonical_audit_bundle_from_parts, AcceptedShare, AuditBody, AuditBundle,
    CarryForwardBalance, FanoutFeeRatePolicy, FoundBlock, PayoutPolicy, PoolFeePolicy, PrismError,
    SettlementModeConfig, AUDIT_BUNDLE_SCHEMA_V1, AUDIT_BUNDLE_SCHEMA_V1_1,
};
use sha2::{Digest, Sha256};

const WINDOW_SHARES: u64 = 400;
const MINERS: u64 = 7;

/// sha256 and length of each case's canonical bytes, recorded from the owned
/// builders at 3.x.x 1398bbc1 (before the parts API existed). Both paths must
/// still produce exactly these bytes.
const BASE_CANONICAL: [(&str, &str, usize); 6] = [
    (
        "build_audit_bundle/v1",
        "937cb8ac3c00c94540e10e17fb3697ed6efb08d25bae3fa73a414c07c46aeed1",
        240_707,
    ),
    (
        "build_audit_bundle/v1.1+pool-fee",
        "bb4cedab4b4923472ca3ab348a47d3b80a80f0f373537ffc47b4331319b3873f",
        243_744,
    ),
    (
        "script_sig_suffix/v1",
        "974a3743e418f4085a323403b4e738b45370ab856b9f54a4ae00810db5e467b2",
        240_775,
    ),
    (
        "coinbase_options/v1.1+suffix+witness",
        "3b65dee47f30a3795660fc1d1e08f70709fae3cb6598af78c36996e1f66fa560",
        243_101,
    ),
    (
        "ctv_settlement/v1+fee+suffix+witness",
        "2bbec9d9dea5fdb98509417a8a57228ea71b682c257e7589cdbb4960bed2e554",
        249_975,
    ),
    (
        "ctv_settlement/v1.1",
        "d4398423dce8429e58508a05773e361e62bb41c08bbaf00ad51e106addac2613",
        250_890,
    ),
];

fn manifest_signing_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap()
}

fn ledger_signing_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap()
}

fn ledger_public_key_hex() -> String {
    ledger_signing_key().public_key_hex()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// A few hundred shares from several miners. The oldest share's difficulty
/// exceeds `u64::MAX`, so it is the partially counted window boundary and its
/// `u128` values exercise arbitrary-precision JSON. `credit_policy_every`
/// marks every n-th share `stale-grace`, which selects the v1.1 schema.
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

fn prior_balances() -> Vec<CarryForwardBalance> {
    vec![
        CarryForwardBalance {
            recipient_id: "miner-2".to_string(),
            order_key: "02".to_string(),
            p2mr_program_hex: hex::encode([3_u8; 32]),
            balance_sats: 4_800,
        },
        CarryForwardBalance {
            recipient_id: "miner-departed".to_string(),
            order_key: "99".to_string(),
            p2mr_program_hex: hex::encode([0x99_u8; 32]),
            balance_sats: 12_000,
        },
    ]
}

fn pool_fee_payout_policy() -> PayoutPolicy {
    PayoutPolicy {
        pool_fee_policy: Some(PoolFeePolicy {
            fee_bps: 200,
            recipient_id: "pool-fee".to_string(),
            order_key: "ff".to_string(),
            p2mr_program_hex: hex::encode([0xee_u8; 32]),
        }),
        ..PayoutPolicy::day_one_default()
    }
}

fn witness_leaves() -> Vec<String> {
    vec!["11".repeat(32), "22".repeat(32)]
}

fn ctv_config() -> SettlementModeConfig {
    SettlementModeConfig {
        max_coinbase_settlement_outputs: 4,
        max_direct_coinbase_outputs: 2,
        max_fanout_recipients_per_transaction: 3,
        reserved_coinbase_outputs: 0,
    }
}

#[derive(Clone)]
enum Shape {
    Plain,
    ScriptSigSuffix(Option<String>),
    CoinbaseOptions {
        suffix: Option<String>,
        witness: Vec<String>,
    },
    CtvSettlement {
        direct_floor_sats: u64,
        config: SettlementModeConfig,
        fee_policy: Option<FanoutFeeRatePolicy>,
        suffix: Option<String>,
        witness: Vec<String>,
    },
}

#[derive(Clone)]
struct Case {
    name: &'static str,
    shares: Vec<AcceptedShare>,
    prior_balances: Vec<CarryForwardBalance>,
    payout_policy: PayoutPolicy,
    shape: Shape,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "build_audit_bundle/v1",
            shares: window_shares(None),
            prior_balances: prior_balances(),
            payout_policy: PayoutPolicy::day_one_default(),
            shape: Shape::Plain,
        },
        Case {
            name: "build_audit_bundle/v1.1+pool-fee",
            shares: window_shares(Some(13)),
            prior_balances: prior_balances(),
            payout_policy: pool_fee_payout_policy(),
            shape: Shape::Plain,
        },
        Case {
            name: "script_sig_suffix/v1",
            shares: window_shares(None),
            prior_balances: prior_balances(),
            payout_policy: PayoutPolicy::day_one_default(),
            shape: Shape::ScriptSigSuffix(Some("aaaaaaaa".to_string())),
        },
        Case {
            name: "coinbase_options/v1.1+suffix+witness",
            shares: window_shares(Some(11)),
            prior_balances: prior_balances(),
            payout_policy: PayoutPolicy::day_one_default(),
            shape: Shape::CoinbaseOptions {
                suffix: Some("0badc0de".to_string()),
                witness: witness_leaves(),
            },
        },
        Case {
            name: "ctv_settlement/v1+fee+suffix+witness",
            shares: window_shares(None),
            prior_balances: prior_balances(),
            payout_policy: PayoutPolicy::day_one_default(),
            shape: Shape::CtvSettlement {
                direct_floor_sats: 60_000_000,
                config: ctv_config(),
                fee_policy: Some(FanoutFeeRatePolicy::new(1_000, 12_000)),
                suffix: Some("aaaaaaaa".to_string()),
                witness: witness_leaves(),
            },
        },
        Case {
            name: "ctv_settlement/v1.1",
            shares: window_shares(Some(17)),
            prior_balances: prior_balances(),
            payout_policy: PayoutPolicy::day_one_default(),
            shape: Shape::CtvSettlement {
                direct_floor_sats: 60_000_000,
                config: ctv_config(),
                fee_policy: None,
                suffix: None,
                witness: Vec::new(),
            },
        },
    ]
}

/// The pre-existing owned entry point for the case's builder shape.
fn build_owned(case: Case) -> AuditBundle {
    let Case {
        shares,
        prior_balances,
        payout_policy,
        shape,
        ..
    } = case;
    let coinbase_key = manifest_signing_key();
    let ledger_key = ledger_signing_key();
    match shape {
        Shape::Plain => build_audit_bundle(
            shares,
            found_block(),
            prior_balances,
            payout_policy,
            &coinbase_key,
            &ledger_key,
        ),
        Shape::ScriptSigSuffix(suffix) => build_audit_bundle_with_coinbase_script_sig_suffix(
            shares,
            found_block(),
            prior_balances,
            payout_policy,
            suffix,
            &coinbase_key,
            &ledger_key,
        ),
        Shape::CoinbaseOptions { suffix, witness } => build_audit_bundle_with_coinbase_options(
            shares,
            found_block(),
            prior_balances,
            payout_policy,
            suffix,
            witness,
            &coinbase_key,
            &ledger_key,
        ),
        Shape::CtvSettlement {
            direct_floor_sats,
            config,
            fee_policy,
            suffix,
            witness,
        } => build_audit_bundle_with_ctv_settlement_options(
            shares,
            found_block(),
            prior_balances,
            payout_policy,
            direct_floor_sats,
            config,
            fee_policy,
            suffix,
            witness,
            &coinbase_key,
            &ledger_key,
        ),
    }
    .unwrap()
}

/// The borrowing entry point for the same shape, lending `shares`.
fn build_borrowed(case: &Case, shares: &[AcceptedShare]) -> AuditBody {
    let coinbase_key = manifest_signing_key();
    let ledger_key = ledger_signing_key();
    let prior_balances = case.prior_balances.clone();
    let payout_policy = case.payout_policy.clone();
    match case.shape.clone() {
        Shape::Plain => build_audit_body(
            shares,
            found_block(),
            prior_balances,
            payout_policy,
            &coinbase_key,
            &ledger_key,
        ),
        Shape::ScriptSigSuffix(suffix) => build_audit_body_with_coinbase_script_sig_suffix(
            shares,
            found_block(),
            prior_balances,
            payout_policy,
            suffix,
            &coinbase_key,
            &ledger_key,
        ),
        Shape::CoinbaseOptions { suffix, witness } => build_audit_body_with_coinbase_options(
            shares,
            found_block(),
            prior_balances,
            payout_policy,
            suffix,
            witness,
            &coinbase_key,
            &ledger_key,
        ),
        Shape::CtvSettlement {
            direct_floor_sats,
            config,
            fee_policy,
            suffix,
            witness,
        } => build_audit_body_with_ctv_settlement_options(
            shares,
            found_block(),
            prior_balances,
            payout_policy,
            direct_floor_sats,
            config,
            fee_policy,
            suffix,
            witness,
            &coinbase_key,
            &ledger_key,
        ),
    }
    .unwrap()
}

#[test]
fn fixture_covers_the_branches_the_builders_take() {
    let cases = cases();
    assert_eq!(cases.len(), BASE_CANONICAL.len());
    let bundles = cases.into_iter().map(build_owned).collect::<Vec<_>>();
    let schemas = bundles
        .iter()
        .map(|bundle| bundle.schema.as_str())
        .collect::<Vec<_>>();
    assert!(schemas.contains(&AUDIT_BUNDLE_SCHEMA_V1));
    assert!(schemas.contains(&AUDIT_BUNDLE_SCHEMA_V1_1));
    for bundle in &bundles {
        let miners = bundle
            .reward_manifest
            .shares
            .iter()
            .map(|share| share.miner_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(miners.len(), usize::try_from(MINERS).unwrap());
        assert!(bundle.shares.len() >= 300);
        assert!(bundle
            .shares
            .iter()
            .any(|share| share.share_difficulty > u128::from(u64::MAX)));
    }
    assert!(bundles
        .iter()
        .any(|bundle| bundle.payout_policy.pool_fee_policy.is_some()));
    assert!(bundles
        .iter()
        .any(|bundle| bundle.coinbase_script_sig_suffix_hex.is_some()));
    assert!(bundles
        .iter()
        .any(|bundle| !bundle.witness_merkle_leaves_hex.is_empty()));
    assert!(bundles.iter().any(|bundle| bundle
        .ctv_fanout_manifest_set
        .as_ref()
        .is_some_and(|set| set.manifests.len() > 1)
        && bundle.ctv_fanout_fee_policy.is_some()));
    assert!(bundles
        .iter()
        .any(|bundle| bundle.ctv_fanout_manifest_set.is_some()
            && bundle.ctv_fanout_fee_policy.is_none()));
}

/// For every builder shape: owned-path bytes, borrowed-parts bytes and
/// `into_parts` -> `into_bundle` bytes are identical and equal the bytes the
/// owned builders produced before the parts API existed; the sha256 values
/// and verification reports agree across both verifiers.
#[test]
fn parts_path_matches_owned_path_byte_for_byte() {
    let ledger_key = ledger_public_key_hex();
    for (case, (name, base_sha256, base_len)) in cases().into_iter().zip(BASE_CANONICAL) {
        assert_eq!(case.name, name);

        let owned = build_owned(case.clone());
        let owned_bytes = canonical_audit_bundle_bytes(&owned).unwrap();
        assert_eq!(owned_bytes.len(), base_len, "{name}: length drifted");
        assert_eq!(
            sha256_hex(&owned_bytes),
            base_sha256,
            "{name}: bytes drifted"
        );
        assert_eq!(serde_json::to_vec(&owned).unwrap(), owned_bytes, "{name}");

        let body = build_borrowed(&case, &case.shares);
        let parts_bytes = canonical_audit_bundle_bytes_from_parts(&body, &case.shares).unwrap();
        assert_eq!(parts_bytes, owned_bytes, "{name}: parts bytes differ");
        let mut streamed = Vec::new();
        write_canonical_audit_bundle_from_parts(&mut streamed, &body, &case.shares).unwrap();
        assert_eq!(streamed, owned_bytes, "{name}: streamed bytes differ");

        let (split_body, split_shares) = owned.clone().into_parts();
        assert_eq!(split_body, body, "{name}");
        assert_eq!(split_shares, case.shares, "{name}");
        let reassembled = split_body.into_bundle(split_shares);
        assert_eq!(reassembled, owned, "{name}");
        assert_eq!(
            canonical_audit_bundle_bytes(&reassembled).unwrap(),
            owned_bytes,
            "{name}: reassembled bytes differ"
        );
        assert_eq!(
            body.clone().into_bundle(case.shares.clone()),
            owned,
            "{name}"
        );

        let decoded: AuditBundle = serde_json::from_slice(&parts_bytes).unwrap();
        assert_eq!(decoded, owned, "{name}: canonical bytes do not decode");

        let bundle_report = verify_audit_bundle(&owned, &ledger_key).unwrap();
        let parts_report = verify_audit_parts(&body, &case.shares, &ledger_key).unwrap();
        assert_eq!(parts_report, bundle_report, "{name}: reports differ");
        assert_eq!(bundle_report.audit_bundle_sha256_hex, base_sha256, "{name}");

        let coinbase_tx_hex = bundle_report.coinbase_tx_hex.to_ascii_uppercase();
        assert_eq!(
            verify_audit_parts_against_coinbase_tx_hex(
                &body,
                &case.shares,
                &coinbase_tx_hex,
                &ledger_key
            )
            .unwrap(),
            verify_audit_bundle_against_coinbase_tx_hex(&owned, &coinbase_tx_hex, &ledger_key)
                .unwrap(),
            "{name}"
        );
        let coinbase_value_sats = found_block().coinbase_value_sats;
        assert_eq!(
            verify_audit_parts_against_coinbase_tx_hex_and_expected_coinbase_value(
                &body,
                &case.shares,
                &coinbase_tx_hex,
                &ledger_key,
                coinbase_value_sats,
            )
            .unwrap(),
            verify_audit_bundle_against_coinbase_tx_hex_and_expected_coinbase_value(
                &owned,
                &coinbase_tx_hex,
                &ledger_key,
                coinbase_value_sats,
            )
            .unwrap(),
            "{name}"
        );
        assert!(matches!(
            verify_audit_parts_against_coinbase_tx_hex(&body, &case.shares, "00", &ledger_key),
            Err(PrismError::AuditCoinbaseTxMismatch)
        ));
        assert!(matches!(
            verify_audit_parts_against_coinbase_tx_hex_and_expected_coinbase_value(
                &body,
                &case.shares,
                &coinbase_tx_hex,
                &ledger_key,
                coinbase_value_sats + 1,
            ),
            Err(PrismError::ExpectedCoinbaseValueMismatch { .. })
        ));
    }
}

#[test]
fn parts_verifier_reads_the_lent_window() {
    let ledger_key = ledger_public_key_hex();
    let cases = cases();

    let v1 = &cases[0];
    let body = build_borrowed(v1, &v1.shares);
    let mut reassigned = v1.shares.clone();
    reassigned[200].miner_id = "miner-interloper".to_string();
    assert!(matches!(
        verify_audit_parts(&body, &reassigned, &ledger_key),
        Err(PrismError::AuditMismatch {
            artifact: "reward_manifest"
        })
    ));

    let v1_1 = &cases[1];
    let body = build_borrowed(v1_1, &v1_1.shares);
    assert_eq!(body.schema, AUDIT_BUNDLE_SCHEMA_V1_1);
    let mut stripped = v1_1.shares.clone();
    for share in &mut stripped {
        share.credit_policy = None;
    }
    assert!(matches!(
        verify_audit_parts(&body, &stripped, &ledger_key),
        Err(PrismError::AuditMismatch { artifact: "schema" })
    ));
}

/// A clone of the window would need a second allocation while the first is
/// still alive, so an unchanged buffer address proves the window was moved
/// or lent, never copied.
#[test]
fn builders_never_copy_the_window() {
    let ledger_key = ledger_public_key_hex();
    for case in cases() {
        let name = case.name;

        // Owned wrappers move the caller's Vec into the bundle.
        let owned_case = case.clone();
        let buffer = owned_case.shares.as_ptr();
        let capacity = owned_case.shares.capacity();
        let bundle = build_owned(owned_case);
        assert_eq!(bundle.shares.as_ptr(), buffer, "{name}: owned path copied");
        assert_eq!(bundle.shares.capacity(), capacity, "{name}");

        // into_parts / into_bundle move the window through unchanged.
        let (body, shares) = bundle.into_parts();
        assert_eq!(shares.as_ptr(), buffer, "{name}: into_parts copied");
        let bundle = body.into_bundle(shares);
        assert_eq!(bundle.shares.as_ptr(), buffer, "{name}: into_bundle copied");

        // A shared window (as a server snapshot would hold it) is only lent:
        // no extra owner, no new buffer.
        let window: Arc<[AcceptedShare]> = Arc::from(case.shares.clone());
        let window_buffer = window.as_ptr();
        let body = build_borrowed(&case, &window);
        let bytes = canonical_audit_bundle_bytes_from_parts(&body, &window).unwrap();
        verify_audit_parts(&body, &window, &ledger_key).unwrap();
        assert_eq!(Arc::strong_count(&window), 1, "{name}");
        assert_eq!(window.as_ptr(), window_buffer, "{name}");
        assert_eq!(
            bytes,
            canonical_audit_bundle_bytes(&bundle).unwrap(),
            "{name}"
        );
    }
}
