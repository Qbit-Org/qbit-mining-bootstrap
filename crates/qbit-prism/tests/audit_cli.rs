use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, canonical_audit_bundle_bytes, verify_audit_bundle, AcceptedShare,
    AuditBundle, CarryForwardBalance, CoinbaseOutputPolicy, FoundBlock, PayoutPolicy,
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

fn p2mr_program(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn live_testnet_scale_bundle() -> AuditBundle {
    let network_difficulty = 6_570_101_980_226_794_000_000_u128;
    let share_difficulty = network_difficulty * 8;
    let share = AcceptedShare {
        share_seq: 1,
        share_id: "share-1".to_string(),
        miner_id: "miner-a".to_string(),
        order_key: "01".to_string(),
        p2mr_program_hex: p2mr_program(0x11),
        share_difficulty,
        network_difficulty,
        template_height: 100,
        job_id: "job-1".to_string(),
        job_issued_at_ms: 1_800_000_000_000,
        accepted_at_ms: 1_800_000_000_000,
        ntime: 1_800_000_000,
        credit_policy: None,
    };
    build_audit_bundle(
        vec![share],
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty,
            anchor_job_issued_at_ms: 1_800_000_000_000,
        },
        Vec::new(),
        PayoutPolicy::day_one_default(),
        &manifest_signing_key(),
        &ledger_signing_key(),
    )
    .unwrap()
}

fn canonical_share_segment_bytes(
    first_share_seq: u64,
    last_share_seq: u64,
    shares: &[AcceptedShare],
) -> Vec<u8> {
    let shares_json = String::from_utf8(serde_json::to_vec(shares).unwrap()).unwrap();
    format!(
        "{{\"schema\":\"qbit.prism.audit-share-segment.v1\",\"first_share_seq\":{first_share_seq},\"last_share_seq\":{last_share_seq},\"share_count\":{},\"shares\":{shares_json}}}",
        shares.len()
    )
    .into_bytes()
}

fn canonicalize_cli_value(value: &serde_json::Value, label: &str) -> Vec<u8> {
    let bundle_path = std::env::temp_dir().join(format!(
        "qbit-prism-audit-canonicalize-{label}-{}.json",
        std::process::id()
    ));
    fs::write(&bundle_path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
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
    output.stdout
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

    let bundle_path = std::env::temp_dir().join(format!(
        "qbit-prism-audit-canonicalize-large-u128-{}.json",
        std::process::id()
    ));
    fs::write(&bundle_path, serde_json::to_vec(&bundle).unwrap()).unwrap();
    let canonical_output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-canonicalize"))
        .arg("--input")
        .arg(&bundle_path)
        .output()
        .unwrap();
    let _ = fs::remove_file(&bundle_path);

    assert!(
        canonical_output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&canonical_output.stdout),
        String::from_utf8_lossy(&canonical_output.stderr)
    );
    assert_eq!(
        canonical_output.stdout,
        canonical_audit_bundle_bytes(&bundle).unwrap()
    );
}

#[test]
fn canonicalizer_restores_typed_bytes_for_reordered_unicode_and_optional_input() {
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
    let canonical = canonical_audit_bundle_bytes(&bundle).unwrap();

    // Value serialization uses map order rather than AuditBundle's typed field
    // order. The adapter must restore the typed canonical representation.
    let mut reordered = serde_json::to_value(&bundle).unwrap();
    assert_eq!(canonicalize_cli_value(&reordered, "reordered"), canonical);

    reordered["shares"][0]["credit_policy"] = serde_json::Value::Null;
    assert_eq!(
        canonicalize_cli_value(&reordered, "explicit-optional-null"),
        canonical
    );

    reordered["shares"][0]["miner_id"] = serde_json::Value::String("miner-é".into());
    let unicode_bundle: AuditBundle = serde_json::from_value(reordered.clone()).unwrap();
    let unicode_canonical = canonical_audit_bundle_bytes(&unicode_bundle).unwrap();
    let unicode_output = canonicalize_cli_value(&reordered, "unicode");
    assert_eq!(unicode_output, unicode_canonical);
    assert!(unicode_output.windows("miner-é".len()).any(|window| window == "miner-é".as_bytes()));
}

#[test]
fn verifier_cli_accepts_live_testnet_scale_legacy_bundle() {
    let bundle = live_testnet_scale_bundle();
    let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
    let bundle_path = std::env::temp_dir().join(format!(
        "qbit-prism-audit-bundle-large-u128-{}.json",
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
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("qbit.prism.audit-verification-report.v1")
    );
    let canonical_output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-canonicalize"))
        .arg("--input")
        .arg(&bundle_path)
        .output()
        .unwrap();
    let _ = fs::remove_file(&bundle_path);
    assert!(canonical_output.status.success());
    assert_eq!(
        canonical_output.stdout,
        canonical_audit_bundle_bytes(&bundle).unwrap()
    );
    assert!(bundle.found_block.network_difficulty > u64::MAX as u128);
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
    assert_eq!(
        output.stdout,
        canonical_audit_bundle_bytes(&bundle).unwrap()
    );
    assert_eq!(
        hex::encode(Sha256::digest(&output.stdout)),
        report.audit_bundle_sha256_hex
    );
    let reparsed: AuditBundle = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(reparsed, bundle);
}

#[test]
fn build_audit_bundle_cli_canonical_output_matches_cc5_golden_bytes() {
    let fixture: Fixture = serde_json::from_str(include_str!(
        "../fixtures/power-law-accrual.prism-fixture.json"
    ))
    .unwrap();
    let expected_bundle = build_audit_bundle(
        fixture.shares,
        fixture.found_block,
        power_law_prior_balances(),
        PayoutPolicy::day_one_default(),
        &manifest_signing_key(),
        &ledger_signing_key(),
    )
    .unwrap();
    let mut input: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/power-law-accrual.prism-fixture.json"
    ))
    .unwrap();
    input["prior_balances"] = serde_json::to_value(power_law_prior_balances()).unwrap();
    let input_path = std::env::temp_dir().join(format!(
        "qbit-prism-build-audit-bundle-canonical-input-{}.json",
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
        .arg("--canonical-output")
        .output()
        .unwrap();
    let summary_output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-build-audit-bundle"))
        .arg("--input")
        .arg(&input_path)
        .arg("--signing-key-seed-hex")
        .arg("42".repeat(32))
        .arg("--ledger-signing-key-seed-hex")
        .arg("43".repeat(32))
        .arg("--job-summary-output")
        .output()
        .unwrap();
    let mut compact_input = input.clone();
    let mut compact_identities = Vec::<serde_json::Value>::new();
    let mut compact_shares = Vec::<serde_json::Value>::new();
    for share in input["shares"].as_array().unwrap() {
        let identity = serde_json::json!([
            share["miner_id"],
            share["order_key"],
            share["p2mr_program_hex"],
        ]);
        let identity_index = compact_identities
            .iter()
            .position(|candidate| candidate == &identity)
            .unwrap_or_else(|| {
                compact_identities.push(identity);
                compact_identities.len() - 1
            });
        compact_shares.push(serde_json::json!([
            share["share_seq"],
            share["share_id"],
            identity_index,
            share["share_difficulty"],
            share["job_issued_at_ms"],
            share["accepted_at_ms"],
            share
                .get("credit_policy")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        ]));
    }
    compact_input["shares"] = serde_json::json!([]);
    compact_input["compact_share_identities"] = serde_json::Value::Array(compact_identities);
    compact_input["compact_shares"] = serde_json::Value::Array(compact_shares);
    fs::write(&input_path, serde_json::to_vec(&compact_input).unwrap()).unwrap();
    let compact_summary_output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-build-audit-bundle"))
        .arg("--input")
        .arg(&input_path)
        .arg("--signing-key-seed-hex")
        .arg("42".repeat(32))
        .arg("--ledger-signing-key-seed-hex")
        .arg("43".repeat(32))
        .arg("--job-summary-output")
        .arg("--phase-metrics")
        .output()
        .unwrap();
    let _ = fs::remove_file(&input_path);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        output.stdout,
        canonical_audit_bundle_bytes(&expected_bundle).unwrap()
    );
    let emitted_bundle: AuditBundle = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(emitted_bundle, expected_bundle);

    assert!(
        summary_output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&summary_output.stdout),
        String::from_utf8_lossy(&summary_output.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&summary_output.stdout).unwrap();
    let summary_fields = summary.as_object().unwrap();
    assert_eq!(
        summary_fields
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec![
            "found_block",
            "payout_policy_manifest",
            "signed_coinbase_manifest",
        ]
    );
    assert_eq!(
        summary,
        serde_json::json!({
            "found_block": &expected_bundle.found_block,
            "signed_coinbase_manifest": &expected_bundle.signed_coinbase_manifest,
            "payout_policy_manifest": &expected_bundle.payout_policy_manifest,
        })
    );
    assert!(summary.get("shares").is_none());
    assert!(summary.get("reward_manifest").is_none());
    assert!(
        compact_summary_output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&compact_summary_output.stdout),
        String::from_utf8_lossy(&compact_summary_output.stderr)
    );
    assert_eq!(compact_summary_output.stdout, summary_output.stdout);
    assert!(String::from_utf8_lossy(&compact_summary_output.stderr)
        .contains("qbit-prism-build-phase-metrics"));

    let report = verify_audit_bundle(&emitted_bundle, &ledger_public_key_hex()).unwrap();
    assert_eq!(output.stdout.len(), 10_999);
    assert_eq!(
        hex::encode(Sha256::digest(&output.stdout)),
        "65b11e1b7e2025472fad2e4cd6b555eaba5eab2a4903e17179ba792d58780a4b"
    );
    assert_eq!(
        report.reward_manifest_sha256_hex,
        "14feb3360ba2d97faadf178151ca7c09bbb6a6e59e6c39a079e7d97986357ae1"
    );
    assert_eq!(
        report.payout_policy_manifest_sha256_hex,
        "2db25eb6db270fb0e5ef9100158b2bfcca95ae6228717fb9779de47fbe11a668"
    );
    assert_eq!(
        report.coinbase_manifest_sha256_hex,
        "635e6133c760cbed7965b0475a273a82495141b4e3939a9908441baa532a0c39"
    );
    assert_eq!(
        emitted_bundle.reward_manifest.share_slice_digest_hex,
        "fc39e87eaedeb6cb6442afbdf060ddc81a93846acc3ffdb47cf202c408c6a9d3"
    );
    assert_eq!(
        report.audit_commitment_root_hex,
        "492c8e5f83049d3f6b04a175a531f130a8c52c2fa944b06741f442587f365d3a"
    );
    assert_eq!(report.onchain_output_count, 3);
    assert_eq!(report.accrued_account_count, 3);
}

#[test]
fn audit_clis_accept_compact_body_ref_bundle() {
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
    let tmp_dir = std::env::temp_dir().join(format!(
        "qbit-prism-compact-body-ref-{}",
        std::process::id()
    ));
    fs::create_dir_all(&tmp_dir).unwrap();

    let first_share_seq = bundle.shares.first().unwrap().share_seq;
    let last_share_seq = bundle.shares.last().unwrap().share_seq;
    let segment = serde_json::json!({
        "schema": "qbit.prism.audit-share-segment.v1",
        "first_share_seq": first_share_seq,
        "last_share_seq": last_share_seq,
        "share_count": bundle.shares.len(),
        "shares": &bundle.shares,
    });
    let segment_bytes = serde_json::to_vec(&segment).unwrap();
    let segment_sha256 = hex::encode(Sha256::digest(&segment_bytes));
    let segment_path = tmp_dir.join("segment.json");
    fs::write(&segment_path, &segment_bytes).unwrap();

    let mut bundle_without_shares = serde_json::to_value(&bundle).unwrap();
    bundle_without_shares
        .as_object_mut()
        .unwrap()
        .remove("shares");
    let body_ref = serde_json::json!({
        "schema": "qbit.prism.audit-body-ref.v1",
        "audit_bundle_sha256": report.audit_bundle_sha256_hex,
        "share_count": bundle.shares.len(),
        "bundle_without_shares": bundle_without_shares,
        "share_parts": [
            {
                "kind": "segment",
                "first_share_seq": first_share_seq,
                "last_share_seq": last_share_seq,
                "share_count": bundle.shares.len(),
                "sha256": segment_sha256,
                "body_uri": "segment.json",
            }
        ],
    });
    let body_ref_path = tmp_dir.join("body-ref.json");
    fs::write(&body_ref_path, serde_json::to_vec(&body_ref).unwrap()).unwrap();

    let verify_output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-verify"))
        .arg(&body_ref_path)
        .arg("--coinbase-tx-hex")
        .arg(&report.coinbase_tx_hex)
        .arg("--ledger-writer-public-key-hex")
        .arg(ledger_public_key_hex())
        .arg("--expected-coinbase-value-sats")
        .arg(report.coinbase_value_sats.to_string())
        .output()
        .unwrap();
    assert!(
        verify_output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&verify_output.stdout),
        String::from_utf8_lossy(&verify_output.stderr)
    );

    let canonical_output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-canonicalize"))
        .arg("--input")
        .arg(&body_ref_path)
        .output()
        .unwrap();
    let _ = fs::remove_dir_all(&tmp_dir);

    assert!(
        canonical_output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&canonical_output.stdout),
        String::from_utf8_lossy(&canonical_output.stderr)
    );
    assert_eq!(
        canonical_output.stdout,
        canonical_audit_bundle_bytes(&bundle).unwrap()
    );
}

#[test]
fn audit_clis_accept_compact_body_ref_bundle_with_live_testnet_scale_u128s() {
    let bundle = live_testnet_scale_bundle();
    let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
    let tmp_dir = std::env::temp_dir().join(format!(
        "qbit-prism-compact-body-ref-large-u128-{}",
        std::process::id()
    ));
    fs::create_dir_all(&tmp_dir).unwrap();

    let first_share_seq = bundle.shares.first().unwrap().share_seq;
    let last_share_seq = bundle.shares.last().unwrap().share_seq;
    let segment = serde_json::json!({
        "schema": "qbit.prism.audit-share-segment.v1",
        "first_share_seq": first_share_seq,
        "last_share_seq": last_share_seq,
        "share_count": bundle.shares.len(),
        "shares": &bundle.shares,
    });
    let segment_bytes = serde_json::to_vec(&segment).unwrap();
    let segment_sha256 = hex::encode(Sha256::digest(&segment_bytes));
    let segment_path = tmp_dir.join("segment.json");
    fs::write(&segment_path, &segment_bytes).unwrap();

    let mut bundle_without_shares = serde_json::to_value(&bundle).unwrap();
    bundle_without_shares
        .as_object_mut()
        .unwrap()
        .remove("shares");
    let body_ref = serde_json::json!({
        "schema": "qbit.prism.audit-body-ref.v1",
        "audit_bundle_sha256": report.audit_bundle_sha256_hex,
        "share_count": bundle.shares.len(),
        "bundle_without_shares": bundle_without_shares,
        "share_parts": [
            {
                "kind": "segment",
                "first_share_seq": first_share_seq,
                "last_share_seq": last_share_seq,
                "share_count": bundle.shares.len(),
                "sha256": segment_sha256,
                "body_uri": "segment.json",
            }
        ],
    });
    let body_ref_path = tmp_dir.join("body-ref.json");
    fs::write(&body_ref_path, serde_json::to_vec(&body_ref).unwrap()).unwrap();

    let verify_output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-verify"))
        .arg(&body_ref_path)
        .arg("--coinbase-tx-hex")
        .arg(&report.coinbase_tx_hex)
        .arg("--ledger-writer-public-key-hex")
        .arg(ledger_public_key_hex())
        .arg("--expected-coinbase-value-sats")
        .arg(report.coinbase_value_sats.to_string())
        .output()
        .unwrap();
    assert!(
        verify_output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&verify_output.stdout),
        String::from_utf8_lossy(&verify_output.stderr)
    );

    let canonical_output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-canonicalize"))
        .arg("--input")
        .arg(&body_ref_path)
        .output()
        .unwrap();
    let _ = fs::remove_dir_all(&tmp_dir);

    assert!(
        canonical_output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&canonical_output.stdout),
        String::from_utf8_lossy(&canonical_output.stderr)
    );
    assert_eq!(
        canonical_output.stdout,
        canonical_audit_bundle_bytes(&bundle).unwrap()
    );
}

#[test]
fn audit_clis_accept_v2_range_proof_bundle() {
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
    let tmp_dir =
        std::env::temp_dir().join(format!("qbit-prism-audit-bundle-v2-{}", std::process::id()));
    fs::create_dir_all(&tmp_dir).unwrap();

    let first_share_seq = bundle.shares.first().unwrap().share_seq;
    let last_share_seq = bundle.shares.last().unwrap().share_seq;
    let segment_bytes =
        canonical_share_segment_bytes(first_share_seq, last_share_seq, &bundle.shares);
    let range_sha256 = hex::encode(Sha256::digest(&segment_bytes));
    let segment_path = tmp_dir.join("segment-slot.json");
    fs::write(&segment_path, &segment_bytes).unwrap();

    let mut bundle_without_shares = serde_json::to_value(&bundle).unwrap();
    bundle_without_shares
        .as_object_mut()
        .unwrap()
        .remove("shares");
    let body_v2 = serde_json::json!({
        "schema": "qbit.prism.audit-bundle.v2",
        "audit_bundle_sha256": report.audit_bundle_sha256_hex,
        "logical_audit_bundle_schema": "qbit.prism.audit-bundle.v1",
        "share_count": bundle.shares.len(),
        "shares_key_index": 1,
        "bundle_without_shares": bundle_without_shares,
        "share_window_proof": {
            "schema": "qbit.prism.window-completeness-proof.v1",
            "first_share_seq": first_share_seq,
            "last_share_seq": last_share_seq,
            "share_count": bundle.shares.len(),
            "share_slice_digest_hex": bundle.reward_manifest.share_slice_digest_hex,
            "share_parts": [
                {
                    "kind": "segment_range",
                    "first_share_seq": first_share_seq,
                    "last_share_seq": last_share_seq,
                    "share_count": bundle.shares.len(),
                    "range_sha256": range_sha256,
                    "body_uri": "segment-slot.json",
                }
            ],
        },
    });
    let body_v2_path = tmp_dir.join("audit-bundle-v2.json");
    fs::write(&body_v2_path, serde_json::to_vec(&body_v2).unwrap()).unwrap();

    let verify_output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-verify"))
        .arg(&body_v2_path)
        .arg("--coinbase-tx-hex")
        .arg(&report.coinbase_tx_hex)
        .arg("--ledger-writer-public-key-hex")
        .arg(ledger_public_key_hex())
        .arg("--expected-coinbase-value-sats")
        .arg(report.coinbase_value_sats.to_string())
        .output()
        .unwrap();
    assert!(
        verify_output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&verify_output.stdout),
        String::from_utf8_lossy(&verify_output.stderr)
    );

    let canonical_output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-audit-canonicalize"))
        .arg("--input")
        .arg(&body_v2_path)
        .output()
        .unwrap();
    let _ = fs::remove_dir_all(&tmp_dir);

    assert!(
        canonical_output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&canonical_output.stdout),
        String::from_utf8_lossy(&canonical_output.stderr)
    );
    assert_eq!(
        canonical_output.stdout,
        canonical_audit_bundle_bytes(&bundle).unwrap()
    );
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

fn pool_fee_first_cli_input(coinbase_output_policy: &str) -> serde_json::Value {
    serde_json::json!({
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
            "coinbase_value_sats": 1000000,
            "network_difficulty": 5,
            "anchor_job_issued_at_ms": 1000
        },
        "payout_policy": {
            "p2mr_spend_input_bytes": 3680,
            "target_feerate_sats_per_byte": 1,
            "safety_multiplier": 4,
            "pool_fee_policy": {
                "fee_bps": 200,
                "recipient_id": "pool-fee",
                "order_key": "zzzzzzzz",
                "p2mr_program_hex": "ff".repeat(32)
            },
            "coinbase_output_policy": coinbase_output_policy
        },
        "coinbase_script_sig_suffix_hex": "aaaaaaaa",
        "ctv_settlement": {
            "direct_floor_sats": 50000,
            "config": {
                "max_coinbase_settlement_outputs": 16,
                "max_direct_coinbase_outputs": 2,
                "max_fanout_recipients_per_transaction": 10,
                "reserved_coinbase_outputs": 0
            }
        }
    })
}

fn run_build_audit_bundle_cli(input: &serde_json::Value, tag: &str) -> std::process::Output {
    let input_path = std::env::temp_dir().join(format!(
        "qbit-prism-build-audit-bundle-{tag}-input-{}.json",
        std::process::id()
    ));
    fs::write(&input_path, serde_json::to_vec(input).unwrap()).unwrap();
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
    output
}

#[test]
fn build_audit_bundle_cli_honors_pool_fee_first_output_policy() {
    let output = run_build_audit_bundle_cli(
        &pool_fee_first_cli_input("pool-fee-first"),
        "pool-fee-first",
    );
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let bundle: AuditBundle = serde_json::from_slice(&output.stdout).unwrap();
    let report = verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();

    assert_eq!(
        bundle.payout_policy_manifest.coinbase_output_policy,
        CoinbaseOutputPolicy::PoolFeeFirst
    );
    assert_eq!(
        report.coinbase_output_policy,
        CoinbaseOutputPolicy::PoolFeeFirst
    );
    let outputs = &bundle.signed_coinbase_manifest.manifest.outputs;
    assert_eq!(outputs[0].recipient_id, "pool-fee");
    assert_eq!(outputs[0].vout, 0);
    assert_eq!(outputs[0].amount_sats, 20_000);
    let serialized_bundle = String::from_utf8(output.stdout).unwrap();
    assert!(serialized_bundle.contains("\"coinbase_output_policy\": \"pool-fee-first\""));
}

#[test]
fn build_audit_bundle_cli_rejects_unknown_coinbase_output_policy() {
    let output = run_build_audit_bundle_cli(&pool_fee_first_cli_input("fee-first"), "bad-policy");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(
        stderr.contains("unknown variant") && stderr.contains("pool-fee-first"),
        "stderr: {stderr}"
    );
}

#[test]
fn build_audit_bundle_cli_rejects_pool_fee_first_without_pool_fee_policy() {
    let mut input = pool_fee_first_cli_input("pool-fee-first");
    input["payout_policy"]
        .as_object_mut()
        .unwrap()
        .remove("pool_fee_policy");

    let output = run_build_audit_bundle_cli(&input, "pool-fee-first-no-fee");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(
        stderr.contains("pool-fee-first coinbase output policy requires a configured pool fee"),
        "stderr: {stderr}"
    );
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

#[test]
fn build_audit_bundle_serve_mode_caches_parsed_windows() {
    use std::io::{BufRead, BufReader as StdBufReader, Write as IoWrite};
    use std::process::Stdio;

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/power-law-accrual.prism-fixture.json"
    ))
    .unwrap();
    let mut compact_identities = Vec::<serde_json::Value>::new();
    let mut compact_shares = Vec::<serde_json::Value>::new();
    for share in fixture["shares"].as_array().unwrap() {
        let identity = serde_json::json!([
            share["miner_id"],
            share["order_key"],
            share["p2mr_program_hex"],
        ]);
        let identity_index = compact_identities
            .iter()
            .position(|candidate| candidate == &identity)
            .unwrap_or_else(|| {
                compact_identities.push(identity);
                compact_identities.len() - 1
            });
        compact_shares.push(serde_json::json!([
            share["share_seq"],
            share["share_id"],
            identity_index,
            share["share_difficulty"],
            share["job_issued_at_ms"],
            share["accepted_at_ms"],
            share
                .get("credit_policy")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        ]));
    }
    let build_fields = serde_json::json!({
        "found_block": fixture["found_block"],
        "prior_balances": serde_json::to_value(power_law_prior_balances()).unwrap(),
    });

    let one_shot_input = {
        let mut input = build_fields.clone();
        input["shares"] = serde_json::json!([]);
        input["compact_share_identities"] =
            serde_json::Value::Array(compact_identities.clone());
        input["compact_shares"] = serde_json::Value::Array(compact_shares.clone());
        input
    };
    let one_shot_path = std::env::temp_dir().join(format!(
        "qbit-prism-serve-one-shot-input-{}.json",
        std::process::id()
    ));
    fs::write(&one_shot_path, serde_json::to_vec(&one_shot_input).unwrap()).unwrap();
    let one_shot = Command::new(env!("CARGO_BIN_EXE_qbit-prism-build-audit-bundle"))
        .arg("--input")
        .arg(&one_shot_path)
        .arg("--signing-key-seed-hex")
        .arg("42".repeat(32))
        .arg("--ledger-signing-key-seed-hex")
        .arg("43".repeat(32))
        .arg("--job-summary-output")
        .output()
        .unwrap();
    let _ = fs::remove_file(&one_shot_path);
    assert!(
        one_shot.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&one_shot.stderr)
    );
    let expected_summary: serde_json::Value =
        serde_json::from_slice(&one_shot.stdout).unwrap();

    let mut daemon = Command::new(env!("CARGO_BIN_EXE_qbit-prism-build-audit-bundle"))
        .arg("--serve")
        .arg("--signing-key-seed-hex")
        .arg("42".repeat(32))
        .arg("--ledger-signing-key-seed-hex")
        .arg("43".repeat(32))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = daemon.stdin.take().unwrap();
    let mut stdout = StdBufReader::new(daemon.stdout.take().unwrap());

    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    let handshake: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(handshake["event"], "handshake");
    // Protocol 2: prepare_window plus the append-invalidation epoch tag.
    assert_eq!(handshake["protocol"], 2);
    assert_eq!(handshake["tool"], "qbit-prism-build-audit-bundle");

    let mut request_with_window = build_fields.clone();
    request_with_window["window_key"] =
        serde_json::json!({"share_snapshot_sha256": "window-a"});
    request_with_window["compact_share_identities"] =
        serde_json::Value::Array(compact_identities.clone());
    request_with_window["compact_shares"] = serde_json::Value::Array(compact_shares.clone());
    writeln!(
        stdin,
        "{}",
        serde_json::to_string(&request_with_window).unwrap()
    )
    .unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    let uploaded: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(uploaded["ok"], true, "upload response: {line}");
    assert_eq!(uploaded["summary"], expected_summary);
    assert_eq!(uploaded["window_cache"]["hit"], false);
    assert_eq!(uploaded["window_cache"]["misses"], 1);
    assert_eq!(uploaded["window_cache"]["entries"], 1);
    assert!(uploaded["metrics"]["input_deserialization_seconds"]
        .as_f64()
        .is_some());
    assert!(uploaded["metrics"]["output_serialization_seconds"]
        .as_f64()
        .is_some());
    assert!(uploaded["metrics"]["phases_seconds"].is_object());

    let mut request_cached = build_fields.clone();
    request_cached["window_key"] = serde_json::json!({"share_snapshot_sha256": "window-a"});
    writeln!(stdin, "{}", serde_json::to_string(&request_cached).unwrap()).unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    let hit: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(hit["ok"], true, "hit response: {line}");
    assert_eq!(hit["summary"], expected_summary);
    assert_eq!(hit["window_cache"]["hit"], true);
    assert_eq!(hit["window_cache"]["hits"], 1);

    let mut request_unknown = build_fields.clone();
    request_unknown["window_key"] =
        serde_json::json!({"share_snapshot_sha256": "window-unknown"});
    writeln!(stdin, "{}", serde_json::to_string(&request_unknown).unwrap()).unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    let missing: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(missing["ok"], false);
    assert_eq!(missing["needs_window"], true);

    // Identities without shares are rejected like one-shot mode, and never
    // classified as a cache hit or a window that needs uploading.
    let mut request_identities_only = build_fields.clone();
    request_identities_only["window_key"] =
        serde_json::json!({"share_snapshot_sha256": "window-a"});
    request_identities_only["compact_share_identities"] =
        serde_json::Value::Array(compact_identities.clone());
    writeln!(
        stdin,
        "{}",
        serde_json::to_string(&request_identities_only).unwrap()
    )
    .unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    let identities_only: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(identities_only["ok"], false);
    assert_eq!(identities_only["needs_window"], false);

    // Two fresh uploads bound the cache to the most recent generations and
    // evict window-a.
    for name in ["window-b", "window-c"] {
        let mut request = request_with_window.clone();
        request["window_key"] = serde_json::json!({"share_snapshot_sha256": name});
        writeln!(stdin, "{}", serde_json::to_string(&request).unwrap()).unwrap();
        line.clear();
        stdout.read_line(&mut line).unwrap();
        let response: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(response["ok"], true, "upload response: {line}");
        assert!(response["window_cache"]["entries"].as_u64().unwrap() <= 2);
    }
    writeln!(stdin, "{}", serde_json::to_string(&request_cached).unwrap()).unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    let evicted: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(evicted["ok"], false);
    assert_eq!(evicted["needs_window"], true);

    drop(stdin);
    let status = daemon.wait().unwrap();
    assert!(status.success());
}

#[test]
fn prepare_window_declares_out_of_range_values_and_survives_them() {
    // serde refuses an integer above its declared width with the same parse
    // error a corrupt line gets. Answered as "malformed serve request", the
    // coordinator read it as an anomaly and SIGKILLed a healthy daemon once
    // per materialization for as long as the window held the value. The
    // daemon now classifies such a request as out_of_range -- naming the
    // field, its literal and the width -- keeps its state, and goes on
    // serving; a genuinely malformed line is still malformed, and the two
    // rejection shapes carry their stable category beside the message.
    use std::io::{BufRead, BufReader as StdBufReader, Read as IoRead, Write as IoWrite};
    use std::process::Stdio;

    let mut daemon = Command::new(env!("CARGO_BIN_EXE_qbit-prism-build-audit-bundle"))
        .arg("--serve")
        .arg("--signing-key-seed-hex")
        .arg("42".repeat(32))
        .arg("--ledger-signing-key-seed-hex")
        .arg("43".repeat(32))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = daemon.stdin.take().unwrap();
    let mut stdout = StdBufReader::new(daemon.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap()["event"],
        "handshake"
    );

    // Literals above u128/u32 cannot be built with json!, so the request is
    // written as text -- exactly as the coordinator's json.dumps sends it.
    fn share_text(seq: u64, difficulty: &str, ntime: &str, ms: i64) -> String {
        format!(
            concat!(
                "{{\"share_seq\":{seq},\"share_id\":\"share-{seq}\",\"miner_id\":\"miner-{m}\",",
                "\"order_key\":\"{m:02}:miner\",\"p2mr_program_hex\":\"{hex}\",",
                "\"share_difficulty\":{difficulty},\"network_difficulty\":1000,",
                "\"template_height\":100,\"job_id\":\"job-{seq}\",",
                "\"job_issued_at_ms\":{ms},\"accepted_at_ms\":{ms},\"ntime\":{ntime}}}"
            ),
            seq = seq,
            m = seq % 3,
            hex = format!("{:02x}", seq % 256).repeat(32),
            difficulty = difficulty,
            ms = ms,
            ntime = ntime,
        )
    }
    let prepare = |records: &[String], window_weight: &str| {
        format!(
            "{{\"request\":\"prepare_window\",\"mode\":\"full\",\"append_invalidation_epoch\":1,\
             \"anchor_job_issued_at_ms\":1000003,\"window_weight\":{window_weight},\"records\":[{}]}}",
            records.join(",")
        )
    };
    fn answer(
        stdin: &mut std::process::ChildStdin,
        stdout: &mut StdBufReader<std::process::ChildStdout>,
        request: &str,
    ) -> serde_json::Value {
        writeln!(stdin, "{request}").unwrap();
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap_or_else(|error| panic!("{error}: {line}"))
    }

    // share_difficulty one above u128 on the second record.
    let beyond_u128 = answer(
        &mut stdin,
        &mut stdout,
        &prepare(
            &[
                share_text(1, "1", "1700000000", 1_000_001),
                share_text(
                    2,
                    "340282366920938463463374607431768211456",
                    "1700000000",
                    1_000_001,
                ),
            ],
            "1000",
        ),
    );
    assert_eq!(beyond_u128["ok"], false, "{beyond_u128}");
    assert_eq!(beyond_u128["request"], "prepare_window");
    assert_eq!(beyond_u128["out_of_range"], true);
    assert_eq!(beyond_u128["field"], "records[1].share_difficulty");
    assert_eq!(beyond_u128["width"], "u128");
    assert!(beyond_u128.get("needs_window").is_none());
    assert!(beyond_u128["error"]
        .as_str()
        .unwrap()
        .contains("340282366920938463463374607431768211456"));

    // ntime one above u32, and a window_weight one above u128.
    let beyond_u32 = answer(
        &mut stdin,
        &mut stdout,
        &prepare(&[share_text(1, "1", "4294967296", 1_000_001)], "1000"),
    );
    assert_eq!(beyond_u32["out_of_range"], true, "{beyond_u32}");
    assert_eq!(beyond_u32["field"], "records[0].ntime");
    assert_eq!(beyond_u32["width"], "u32");
    let beyond_weight = answer(
        &mut stdin,
        &mut stdout,
        &prepare(
            &[share_text(1, "1", "1700000000", 1_000_001)],
            "340282366920938463463374607431768211456",
        ),
    );
    assert_eq!(beyond_weight["out_of_range"], true, "{beyond_weight}");
    assert_eq!(beyond_weight["field"], "window_weight");

    // Every input at its width, but the retained total leaves u128: the
    // accumulator is declined, never saturated.
    let overflow = answer(
        &mut stdin,
        &mut stdout,
        &prepare(
            &[
                share_text(
                    1,
                    "170141183460469231731687303715884105728",
                    "1700000000",
                    1_000_001,
                ),
                share_text(
                    2,
                    "170141183460469231731687303715884105728",
                    "1700000000",
                    1_000_001,
                ),
            ],
            "340282366920938463463374607431768211455",
        ),
    );
    assert_eq!(overflow["out_of_range"], true, "{overflow}");
    assert_eq!(overflow["field"], "total_difficulty");
    assert_eq!(overflow["width"], "u128");

    // A genuinely malformed line is still a malformed request.
    let malformed = answer(&mut stdin, &mut stdout, "{\"request\":\"prepare_window\",\"mode\":\"full\",\"records\":[{\"share_difficulty\":1.5}]");
    assert_eq!(malformed["ok"], false);
    assert!(malformed["error"]
        .as_str()
        .unwrap()
        .starts_with("malformed serve request"));
    assert!(malformed.get("out_of_range").is_none());

    // Rejections name their condition in a stable category.
    let duplicate = answer(
        &mut stdin,
        &mut stdout,
        &prepare(
            &[
                share_text(1, "1", "1700000000", 1_000_001),
                share_text(1, "2", "1700000000", 1_000_001),
            ],
            "1000",
        ),
    );
    assert_eq!(duplicate["fold_invalid"], true, "{duplicate}");
    assert_eq!(duplicate["rejection"], "duplicate_share_seq");
    assert_eq!(
        duplicate["error"],
        "full payout window contains duplicate share_seq"
    );

    // The same process, afterwards, prepares and advances a window at the
    // declared edges exactly: u64::MAX share_seq, u128::MAX window_weight,
    // u32::MAX ntime, and difficulties whose retained total is exactly
    // u128::MAX.
    let edge = answer(
        &mut stdin,
        &mut stdout,
        &prepare(
            &[
                share_text(
                    1,
                    "340282366920938463463374607431768211454",
                    "4294967295",
                    1_000_001,
                ),
                share_text(18446744073709551615, "1", "4294967295", 1_000_001),
            ],
            "340282366920938463463374607431768211455",
        ),
    );
    assert_eq!(edge["ok"], true, "{edge}");
    assert_eq!(edge["record_count"], 2);
    let base_digest = edge["share_snapshot_sha256"].as_str().unwrap().to_string();
    let items_len = edge["window_items_len"].as_u64().unwrap() as usize;
    let mut raw = vec![0u8; items_len + 1];
    stdout.read_exact(&mut raw).unwrap();
    assert_eq!(raw[items_len], b'\n');
    let items = String::from_utf8(raw[..items_len].to_vec()).unwrap();
    assert!(items.contains("\"share_difficulty\":340282366920938463463374607431768211454"));
    assert!(items.contains("\"share_seq\":18446744073709551615"));
    assert!(items.contains("\"ntime\":4294967295"));

    let advance = answer(
        &mut stdin,
        &mut stdout,
        &format!(
        "{{\"request\":\"prepare_window\",\"mode\":\"advance\",\"append_invalidation_epoch\":1,\
         \"anchor_job_issued_at_ms\":1000004,\"base_digest\":\"{base_digest}\",\"records\":[]}}"
    ),
    );
    assert_eq!(advance["ok"], true, "{advance}");
    assert_eq!(advance["record_count"], 2);
    let appended_len = advance["appended_items_len"].as_u64().unwrap() as usize;
    let mut raw = vec![0u8; appended_len + 1];
    stdout.read_exact(&mut raw).unwrap();

    let not_append = answer(
        &mut stdin,
        &mut stdout,
        &format!(
        "{{\"request\":\"prepare_window\",\"mode\":\"advance\",\"append_invalidation_epoch\":1,\
         \"anchor_job_issued_at_ms\":1000005,\"base_digest\":\"{base_digest}\",\"records\":[{}]}}",
        share_text(3, "1", "1700000000", 1_000_005)
    ),
    );
    assert_eq!(not_append["fallback"], true, "{not_append}");
    assert_eq!(not_append["rejection"], "delta_not_append");

    drop(stdin);
    let status = daemon.wait().unwrap();
    assert!(status.success());
}

#[test]
fn prepare_window_advance_ignores_the_append_invalidation_epoch_tag() {
    // The coordinator's late-append policy is finer than any tag this daemon
    // could apply: it clears the incremental cache only when the late row
    // predates the cached anchor, and otherwise RETAGS the window so the next
    // delta build advances it in place. A daemon that demanded an exact tag
    // match turned every such retag into a needs_full -- a full DB walk, fold
    // and upload -- while catching nothing the coordinator had not already
    // decided. The tag rides along for diagnostics and nothing else.
    use std::io::{BufRead, BufReader as StdBufReader, Read as IoRead, Write as IoWrite};
    use std::process::Stdio;

    fn share(seq: u64) -> serde_json::Value {
        serde_json::json!({
            "share_seq": seq,
            "share_id": format!("share-{seq}"),
            "miner_id": format!("miner-{}", seq % 3),
            "order_key": format!("{:02}:miner", seq % 3),
            "p2mr_program_hex": format!("{:02x}", seq).repeat(32),
            "share_difficulty": 1,
            "network_difficulty": 1000,
            "template_height": 100,
            "job_id": format!("job-{seq}"),
            "job_issued_at_ms": 1_000_000 + seq as i64,
            "accepted_at_ms": 1_000_000 + seq as i64,
            "ntime": 1_700_000_000u32,
        })
    }

    let mut daemon = Command::new(env!("CARGO_BIN_EXE_qbit-prism-build-audit-bundle"))
        .arg("--serve")
        .arg("--signing-key-seed-hex")
        .arg("42".repeat(32))
        .arg("--ledger-signing-key-seed-hex")
        .arg("43".repeat(32))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = daemon.stdin.take().unwrap();
    let mut stdout = StdBufReader::new(daemon.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap()["event"],
        "handshake"
    );

    // A section reader that consumes the raw canonical-items payload plus its
    // terminating newline, so the next envelope line starts clean.
    let drain_section = |stdout: &mut StdBufReader<std::process::ChildStdout>, len: usize| {
        let mut raw = vec![0u8; len + 1];
        stdout.read_exact(&mut raw).unwrap();
        assert_eq!(raw[len], b'\n');
        raw.truncate(len);
        raw
    };

    let prepared = serde_json::json!({
        "request": "prepare_window",
        "mode": "full",
        "append_invalidation_epoch": 7,
        "anchor_job_issued_at_ms": 1_000_003i64,
        "window_weight": 1_000,
        "records": [share(1), share(2), share(3)],
    });
    writeln!(stdin, "{}", serde_json::to_string(&prepared).unwrap()).unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    let full: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(full["ok"], true, "full response: {line}");
    assert_eq!(full["record_count"], 3);
    let base_digest = full["share_snapshot_sha256"].as_str().unwrap().to_string();
    drain_section(&mut stdout, full["window_items_len"].as_u64().unwrap() as usize);

    // A build at a newer epoch must not evict the prepared window either.
    let build = serde_json::json!({
        "window_key": {"share_snapshot_sha256": base_digest},
        "append_invalidation_epoch": 8,
        "found_block": {
            "block_height": 101,
            "coinbase_value_sats": 5_000_000_000u64,
            "network_difficulty": 1000,
            "anchor_job_issued_at_ms": 1_000_003i64,
        },
        "prior_balances": [],
    });
    writeln!(stdin, "{}", serde_json::to_string(&build).unwrap()).unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    let served: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(served["ok"], true, "build response: {line}");
    assert_eq!(served["window_cache"]["hit"], true);

    // The retag case: the base was prepared at epoch 7, the advance arrives
    // at epoch 9, and it advances in place instead of answering needs_full.
    let advance = serde_json::json!({
        "request": "prepare_window",
        "mode": "advance",
        "append_invalidation_epoch": 9,
        "anchor_job_issued_at_ms": 1_000_004i64,
        "base_digest": base_digest,
        "records": [share(4)],
    });
    writeln!(stdin, "{}", serde_json::to_string(&advance).unwrap()).unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    let advanced: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(advanced["ok"], true, "advance response: {line}");
    assert!(advanced.get("needs_full").is_none());
    assert_eq!(advanced["record_count"], 4);
    assert_eq!(advanced["added_rows"], 1);
    // The tags are reported, never acted on.
    assert_eq!(advanced["base_append_invalidation_epoch"], 7);
    assert_eq!(advanced["append_invalidation_epoch"], 9);
    let appended = drain_section(
        &mut stdout,
        advanced["appended_items_len"].as_u64().unwrap() as usize,
    );
    assert!(!appended.is_empty());

    // An unheld digest is still the one thing that answers needs_full.
    let unknown = serde_json::json!({
        "request": "prepare_window",
        "mode": "advance",
        "append_invalidation_epoch": 9,
        "anchor_job_issued_at_ms": 1_000_005i64,
        "base_digest": "0".repeat(64),
        "records": [],
    });
    writeln!(stdin, "{}", serde_json::to_string(&unknown).unwrap()).unwrap();
    line.clear();
    stdout.read_line(&mut line).unwrap();
    let missing: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(missing["ok"], false);
    assert_eq!(missing["needs_full"], true);

    drop(stdin);
    let status = daemon.wait().unwrap();
    assert!(status.success());
}
