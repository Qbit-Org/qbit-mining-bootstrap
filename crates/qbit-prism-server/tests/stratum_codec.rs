use num_traits::One;
use qbit_pool_builder::{build_manifest, CoinbaseBuildRequest, WeightedEntitlement};
use qbit_prism_server::codec::*;
use serde_json::json;

fn fixture(
    extranonce1: &str,
    program: &str,
) -> (serde_json::Value, qbit_pool_builder::PayoutManifest) {
    let template = json!({"height":101,"coinbasevalue":5_000_000_000u64,"previousblockhash":"0123456789abcdef".repeat(4),
        "version":0x20000000u32,"bits":"207fffff","curtime":1_700_000_000u32,"mintime":1_699_999_999u32,"transactions":[]});
    let manifest = build_manifest(CoinbaseBuildRequest {
        block_height: 101,
        coinbase_value_sats: 5_000_000_000,
        entitlements: vec![WeightedEntitlement {
            recipient_id: "miner".into(),
            order_key: "miner".into(),
            p2mr_program_hex: program.into(),
            weight: 1,
        }],
        witness_nonce_hex: Some("00".repeat(32)),
        witness_merkle_leaves_hex: vec![],
        coinbase_script_sig_suffix_hex: Some(format!("505249534d{extranonce1}{}", "00".repeat(8))),
        pinned_first_output: None,
    })
    .unwrap();
    (template, manifest)
}

#[test]
fn witness_coinbase_round_trip_and_header_endianness() {
    let (template, manifest) = fixture("12345678", &"ab".repeat(32));
    let job = Job::from_manifest(
        "job".into(),
        &template,
        &manifest,
        "12345678",
        8,
        1e-9,
        0.0,
        true,
    )
    .unwrap();
    let submission = job
        .assemble_submission(
            "0000000000000000",
            "6553f100",
            "01020304",
            Some("00002000"),
            VERSION_ROLLING_MASK,
        )
        .unwrap();
    assert_eq!(submission.coinbase_tx_hex, manifest.coinbase_tx_hex);
    let header = hex::decode(&submission.header_hex).unwrap();
    assert_eq!(&header[..4], &0x20002000u32.to_le_bytes());
    let mut previous = hex::decode(template["previousblockhash"].as_str().unwrap()).unwrap();
    previous.reverse();
    assert_eq!(&header[4..36], previous);
    assert_eq!(
        &header[36..68],
        &double_sha256(
            &strip_witness_transaction(&hex::decode(&manifest.coinbase_tx_hex).unwrap()).unwrap()
        )
    );
    assert_eq!(&header[76..], &0x01020304u32.to_le_bytes());
    assert_eq!(
        submission.block_hash_hex,
        hash_display(&double_sha256(&header))
    );
    if submission.block_pass {
        assert_eq!(&submission.block_hex[160..162], "01");
        assert_eq!(&submission.block_hex[162..], manifest.coinbase_tx_hex);
    } else {
        assert!(submission.block_hex.is_empty());
    }
    assert_eq!(job.notify()["params"][4], json!([]));
}

#[test]
fn extranonce_split_ignores_identical_output_and_witness_bytes() {
    let (template, manifest) = fixture("00000000", &"00".repeat(32));
    let job = Job::from_manifest(
        "job".into(),
        &template,
        &manifest,
        "00000000",
        8,
        1e-9,
        0.0,
        true,
    )
    .unwrap();
    let submission = job
        .assemble_submission("1122334455667788", "6553f100", "00000000", None, 0)
        .unwrap();
    assert!(submission
        .coinbase_tx_hex
        .contains("52200000000000000000000000000000000000000000000000000000000000000000"));
    assert!(submission
        .coinbase_tx_hex
        .contains("505249534d000000001122334455667788"));
    assert!(job
        .assemble_submission("1122", "6553f100", "00000000", None, 0)
        .is_err());
    assert!(job
        .assemble_submission(
            "0000000000000000",
            "6553f100",
            "00000000",
            Some("80000000"),
            VERSION_ROLLING_MASK
        )
        .is_err());
    assert!(job
        .assemble_submission("0000000000000000", "00000001", "00000000", None, 0)
        .is_err());
}

#[test]
fn transaction_parser_rejects_truncation_and_noncanonical_sizes() {
    let (_, manifest) = fixture("12345678", &"ab".repeat(32));
    let tx = hex::decode(manifest.coinbase_tx_hex).unwrap();
    for length in 0..tx.len() {
        assert!(
            strip_witness_transaction(&tx[..length]).is_err(),
            "length {length}"
        );
    }
    let mut trailing = tx.clone();
    trailing.push(0);
    assert!(strip_witness_transaction(&trailing).is_err());
    let mut malformed = tx;
    malformed.splice(6..7, [253, 1, 0]);
    assert!(strip_witness_transaction(&malformed).is_err());
}

#[test]
fn targets_preserve_network_block_solutions_above_listener_floor() {
    assert_eq!(
        difficulty_target(1.0).unwrap(),
        target_from_compact(0x1d00ffff).unwrap()
    );
    assert_eq!(
        difficulty_target(2.0).unwrap(),
        target_from_compact(0x1d00ffff).unwrap() / 2u8
    );
    assert_eq!(
        difficulty_target(f64::MAX).unwrap(),
        num_bigint::BigUint::one()
    );
    assert!(difficulty_target(f64::NAN).is_err());
    assert!(target_from_compact(0x20800001).is_err());
    assert!(target_from_compact(0x2300ffff).is_err());
    assert_eq!(
        scaled_target_difficulty(&target_from_compact(0x207fffff).unwrap()).unwrap(),
        1_000_000
    );
    let (template, manifest) = fixture("12345678", &"ab".repeat(32));
    let job = Job::from_manifest(
        "job".into(),
        &template,
        &manifest,
        "12345678",
        8,
        500_000.0,
        500_000.0,
        true,
    )
    .unwrap();
    let solution = (0..1000)
        .find_map(|nonce| {
            let submission = job
                .assemble_submission(
                    "0000000000000000",
                    "6553f100",
                    &format!("{nonce:08x}"),
                    None,
                    0,
                )
                .unwrap();
            submission.block_pass.then_some(submission)
        })
        .expect("regtest block solution");
    assert!(!solution.share_pass);
    assert_eq!(&solution.block_hex[160..162], "01");
    assert_eq!(&solution.block_hex[162..], manifest.coinbase_tx_hex);
    assert!((job.share_difficulty - 500_000.0).abs() < 1e-9);
}

#[test]
fn merkle_branches_use_txids_instead_of_wtxids() {
    let (_, manifest) = fixture("12345678", &"ab".repeat(32));
    let tx = hex::decode(manifest.coinbase_tx_hex).unwrap();
    let txid = double_sha256(&strip_witness_transaction(&tx).unwrap());
    let branches = merkle_branch_for_coinbase(&[tx.clone(), tx.clone(), tx.clone()]).unwrap();
    assert_eq!(branches.len(), 2);
    assert_eq!(branches[0], txid);
    assert_eq!(
        branches[1],
        double_sha256(&[txid.as_slice(), txid.as_slice()].concat())
    );
    assert_ne!(witness_merkle_leaves_hex(&[tx])[0], hex::encode(txid));
}

#[test]
fn version_mask_advertisement_is_authoritative() {
    assert_eq!(
        version_mask_from_template(&json!({}), VERSION_ROLLING_MASK).unwrap(),
        VERSION_ROLLING_MASK
    );
    assert_eq!(
        version_mask_from_template(&json!({"versionrollingmask":0}), VERSION_ROLLING_MASK).unwrap(),
        0
    );
    assert_eq!(
        version_mask_from_template(&json!({"versionrollingmask":"0x0000e000"}), 0).unwrap(),
        0xe000
    );
    assert!(version_mask_from_template(
        &json!({"versionrollingmask":"bogus"}),
        VERSION_ROLLING_MASK
    )
    .is_err());
}

#[test]
fn vardiff_uses_actual_accepted_work_and_limits_idle_step() {
    use qbit_prism_server::vardiff::*;
    let config = VardiffConfig {
        minimum: 1.0,
        maximum: 1000.0,
        ..Default::default()
    };
    assert_eq!(
        config.next_difficulty(100.0, 0.0, 90.0, Some(100.0)).0,
        25.0
    );
    assert_eq!(config.next_difficulty(100.0, 2400.0, 90.0, None).0, 400.0);
    // A window contains six shares from jobs that were assigned difficulty 10,
    // even though the connection's latest target is 100.
    assert_eq!(config.next_difficulty(100.0, 60.0, 90.0, None).0, 25.0);
    assert_eq!(
        password_difficulties("x,d=12,md=4,d=NaN,ignored=5"),
        (Some(12.0), Some(4.0))
    );
}
