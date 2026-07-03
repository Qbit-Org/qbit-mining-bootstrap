use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, canonical_audit_bundle_bytes, verify_audit_bundle, AcceptedShare,
    AuditBundle, CarryForwardBalance, FoundBlock, PayoutPolicy,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{fs, process::Command};

#[derive(Debug, Deserialize)]
struct Fixture {
    found_block: FoundBlock,
    shares: Vec<AcceptedShare>,
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

#[test]
fn verifier_cli_accepts_exported_power_law_bundle() {
    let fixture: Fixture = serde_json::from_str(include_str!(
        "../fixtures/power-law-accrual.prism-fixture.json"
    ))
    .unwrap();
    let bundle = build_audit_bundle(
        fixture.shares,
        fixture.found_block,
        power_law_prior_balances(),
        PayoutPolicy::day_one_default(),
        &manifest_signing_key(),
        &ledger_signing_key(),
    )
    .unwrap();
    let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
    let bundle_path = std::env::temp_dir().join(format!(
        "qbit-prism-audit-bundle-{}.json",
        std::process::id()
    ));
    fs::write(&bundle_path, serde_json::to_vec(&bundle).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-verify"))
        .arg(&bundle_path)
        .arg("--coinbase-tx-hex")
        .arg(&report.coinbase_tx_hex)
        .arg("--ledger-writer-public-key-hex")
        .arg(ledger_public_key_hex())
        .arg("--expected-coinbase-value-sats")
        .arg(report.coinbase_value_sats.to_string())
        .output()
        .unwrap();
    let _ = fs::remove_file(&bundle_path);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("qbit.prism.audit-verification-report.v1")
    );
}

#[test]
fn verifier_cli_rejects_unexpected_coinbase_value() {
    let fixture: Fixture = serde_json::from_str(include_str!(
        "../fixtures/power-law-accrual.prism-fixture.json"
    ))
    .unwrap();
    let bundle = build_audit_bundle(
        fixture.shares,
        fixture.found_block,
        power_law_prior_balances(),
        PayoutPolicy::day_one_default(),
        &manifest_signing_key(),
        &ledger_signing_key(),
    )
    .unwrap();
    let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
    let bundle_path = std::env::temp_dir().join(format!(
        "qbit-prism-audit-bundle-mismatch-{}.json",
        std::process::id()
    ));
    fs::write(&bundle_path, serde_json::to_vec(&bundle).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-verify"))
        .arg(&bundle_path)
        .arg("--coinbase-tx-hex")
        .arg(&report.coinbase_tx_hex)
        .arg("--ledger-writer-public-key-hex")
        .arg(ledger_public_key_hex())
        .arg("--expected-coinbase-value-sats")
        .arg((report.coinbase_value_sats + 1).to_string())
        .output()
        .unwrap();
    let _ = fs::remove_file(&bundle_path);

    assert!(
        !output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("expected coinbase value"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn canonicalize_cli_emits_verifier_hash_bytes() {
    let fixture: Fixture = serde_json::from_str(include_str!(
        "../fixtures/power-law-accrual.prism-fixture.json"
    ))
    .unwrap();
    let bundle = build_audit_bundle(
        fixture.shares,
        fixture.found_block,
        power_law_prior_balances(),
        PayoutPolicy::day_one_default(),
        &manifest_signing_key(),
        &ledger_signing_key(),
    )
    .unwrap();
    let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
    let bundle_path = std::env::temp_dir().join(format!(
        "qbit-prism-audit-canonicalize-input-{}.json",
        std::process::id()
    ));
    fs::write(&bundle_path, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-canonicalize"))
        .arg("--input")
        .arg(&bundle_path)
        .output()
        .unwrap();
    let _ = fs::remove_file(&bundle_path);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, canonical_audit_bundle_bytes(&bundle).unwrap());
    assert_eq!(
        hex::encode(Sha256::digest(&output.stdout)),
        report.audit_bundle_sha256_hex
    );
    let reparsed: AuditBundle = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(reparsed, bundle);
}

#[test]
fn build_audit_bundle_cli_emits_suffix_aware_bundle() {
    let suffix = "111111112222222222222222".to_string();
    let witness_leaf = "11".repeat(32);
    let mut input: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/power-law-accrual.prism-fixture.json"
    ))
    .unwrap();
    input["coinbase_script_sig_suffix_hex"] = serde_json::Value::String(suffix.clone());
    input["witness_merkle_leaves_hex"] = serde_json::json!([witness_leaf.clone()]);
    input["prior_balances"] = serde_json::to_value(power_law_prior_balances()).unwrap();
    let input_path = std::env::temp_dir().join(format!(
        "qbit-prism-build-audit-bundle-input-{}.json",
        std::process::id()
    ));
    fs::write(&input_path, serde_json::to_vec(&input).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-build-audit-bundle"))
        .arg("--input")
        .arg(&input_path)
        .arg("--signing-key-seed-hex")
        .arg("42".repeat(32))
        .arg("--ledger-signing-key-seed-hex")
        .arg("43".repeat(32))
        .output()
        .unwrap();
    let _ = fs::remove_file(&input_path);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let bundle: AuditBundle = serde_json::from_slice(&output.stdout).unwrap();
    let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();

    assert_eq!(bundle.coinbase_script_sig_suffix_hex, Some(suffix.clone()));
    assert_eq!(
        bundle
            .signed_coinbase_manifest
            .manifest
            .coinbase_script_sig_suffix_hex,
        suffix
    );
    assert_eq!(bundle.witness_merkle_leaves_hex, vec![witness_leaf]);
    assert_eq!(
        bundle.audit_commitment_leaves_hex,
        vec![report.prism_audit_commitment_leaf_hex.clone()]
    );
    assert!(!bundle
        .witness_merkle_leaves_hex
        .contains(&report.prism_audit_commitment_leaf_hex));
    assert_eq!(
        bundle.signed_coinbase_manifest.manifest.witness_nonce_hex,
        report.audit_commitment_root_hex
    );
    assert_eq!(report.onchain_output_count, 3);
    assert_eq!(report.accrued_account_count, 3);
}

#[test]
fn build_audit_bundle_cli_emits_ctv_settlement_bundle() {
    let input = serde_json::json!({
        "shares": [
            {
                "share_seq": 1,
                "share_id": "share-1",
                "miner_id": "miner-a",
                "order_key": "01",
                "p2mr_program_hex": "01".repeat(32),
                "share_difficulty": 3,
                "network_difficulty": 5,
                "template_height": 100,
                "job_id": "job-1",
                "job_issued_at_ms": 1000,
                "accepted_at_ms": 1000,
                "ntime": 1800000000
            },
            {
                "share_seq": 2,
                "share_id": "share-2",
                "miner_id": "miner-b",
                "order_key": "02",
                "p2mr_program_hex": "02".repeat(32),
                "share_difficulty": 2,
                "network_difficulty": 5,
                "template_height": 100,
                "job_id": "job-1",
                "job_issued_at_ms": 1000,
                "accepted_at_ms": 1000,
                "ntime": 1800000000
            }
        ],
        "found_block": {
            "block_height": 101,
            "coinbase_value_sats": 100000,
            "network_difficulty": 5,
            "anchor_job_issued_at_ms": 1000
        },
        "coinbase_script_sig_suffix_hex": "aaaaaaaa",
        "witness_merkle_leaves_hex": ["22".repeat(32)],
        "ctv_settlement": {
            "direct_floor_sats": 50000,
            "config": {
                "max_coinbase_settlement_outputs": 16,
                "max_direct_coinbase_outputs": 1,
                "max_fanout_recipients_per_transaction": 10,
                "reserved_coinbase_outputs": 0
            }
        }
    });
    let input_path = std::env::temp_dir().join(format!(
        "qbit-prism-build-audit-bundle-ctv-input-{}.json",
        std::process::id()
    ));
    fs::write(&input_path, serde_json::to_vec(&input).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-build-audit-bundle"))
        .arg("--input")
        .arg(&input_path)
        .arg("--signing-key-seed-hex")
        .arg("42".repeat(32))
        .arg("--ledger-signing-key-seed-hex")
        .arg("43".repeat(32))
        .output()
        .unwrap();
    let _ = fs::remove_file(&input_path);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let bundle: AuditBundle = serde_json::from_slice(&output.stdout).unwrap();
    let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();

    assert_eq!(report.coinbase_value_sats, 100_000);
    assert_eq!(report.onchain_output_count, 2);
    assert_eq!(bundle.witness_merkle_leaves_hex, vec!["22".repeat(32)]);
    assert_eq!(
        bundle
            .settlement_mode_decision
            .as_ref()
            .unwrap()
            .fanout_chunk_count,
        1
    );
    assert_eq!(
        bundle
            .ctv_fanout_manifest_set
            .as_ref()
            .unwrap()
            .fanout_count,
        1
    );
    let fanout_leaf =
        &bundle.ctv_fanout_manifest_set.as_ref().unwrap().manifests[0].commitment_witness_leaf_hex;
    assert!(bundle.audit_commitment_leaves_hex.contains(fanout_leaf));
    assert!(!bundle.witness_merkle_leaves_hex.contains(fanout_leaf));
}

#[test]
fn reorg_verify_cli_reverses_disconnected_immature_entries() {
    let fixture: Fixture = serde_json::from_str(include_str!(
        "../fixtures/power-law-accrual.prism-fixture.json"
    ))
    .unwrap();
    let disconnected_bundle = build_audit_bundle(
        fixture.shares.clone(),
        fixture.found_block.clone(),
        power_law_prior_balances(),
        PayoutPolicy::day_one_default(),
        &manifest_signing_key(),
        &ledger_signing_key(),
    )
    .unwrap();
    let replacement_bundle = build_audit_bundle(
        fixture.shares,
        fixture.found_block,
        power_law_prior_balances(),
        PayoutPolicy::day_one_default(),
        &manifest_signing_key(),
        &ledger_signing_key(),
    )
    .unwrap();
    let input = serde_json::json!({
        "disconnected_block_hash": "block-a",
        "disconnected_block_height": 200,
        "disconnected_payout_policy_manifest": disconnected_bundle.payout_policy_manifest,
        "replacement_block_hash": "block-b",
        "replacement_block_height": 200,
        "replacement_payout_policy_manifest": replacement_bundle.payout_policy_manifest,
    });
    let input_path = std::env::temp_dir().join(format!(
        "qbit-prism-reorg-verify-input-{}.json",
        std::process::id()
    ));
    fs::write(&input_path, serde_json::to_vec(&input).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-reorg-verify"))
        .arg("--input")
        .arg(&input_path)
        .output()
        .unwrap();
    let _ = fs::remove_file(&input_path);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(report["schema"], "qbit.prism.reorg-verification-report.v1");
    assert_eq!(
        report["disconnected_entry_count"],
        report["reversed_entry_count"]
    );
    assert!(report["replacement_entry_count"].as_u64().unwrap() > 0);
}
