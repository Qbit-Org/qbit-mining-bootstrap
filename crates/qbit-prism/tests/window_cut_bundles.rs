//! Dual-writer windows (PRISM 3.1): the per-node cut in audit bundles.
//!
//! A window with a cut records it in its reward manifest, which the coinbase
//! commits through the audit commitment leaf. These tests pin that:
//! - a window without a cut builds exactly the 3.0 bytes (the golden vectors
//!   elsewhere in this crate pin those bytes; here the two entry points are
//!   compared directly);
//! - a window with one builds, verifies and canonicalizes through the library
//!   and the shipped binaries, identically at every builder parallelism;
//! - dropping, adding or altering the cut, or a share above it, fails;
//! - the fixture's bytes are pinned (`DUAL_WRITER_GOLDEN`).

use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle_body_with_coinbase_options,
    build_audit_bundle_body_with_coinbase_options_parallel,
    build_audit_bundle_body_with_ctv_settlement_options_parallel, build_prism_reward_manifest,
    build_prism_reward_manifest_with_cut, canonical_audit_bundle_bytes,
    canonical_audit_bundle_bytes_from_parts, canonical_reward_manifest_bytes,
    restore_reward_manifest, verify_audit_bundle, verify_audit_parts, AcceptedShare, AuditBundle,
    AuditBundleBody, CarryForwardBalance, FanoutFeeRatePolicy, FoundBlock, Parallelism,
    PayoutPolicy, PrismError, PrismRewardManifestHeader, SettlementModeConfig, WindowCut,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{fs, process::Command};

#[derive(Debug, Deserialize)]
struct Fixture {
    found_block: FoundBlock,
    window_cut: WindowCut,
    prior_balances: Vec<CarryForwardBalance>,
    shares: Vec<AcceptedShare>,
}

const FIXTURE_JSON: &str = include_str!("../fixtures/dual-writer-window.prism-fixture.json");

fn fixture() -> Fixture {
    serde_json::from_str(FIXTURE_JSON).unwrap()
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

fn ctv_config() -> SettlementModeConfig {
    SettlementModeConfig {
        max_coinbase_settlement_outputs: 4,
        max_direct_coinbase_outputs: 2,
        max_fanout_recipients_per_transaction: 3,
        reserved_coinbase_outputs: 0,
    }
}

fn geometries() -> Vec<Parallelism> {
    vec![
        Parallelism::serial(),
        Parallelism::new(2, 1),
        Parallelism::new(3, 2),
        Parallelism::new(4, 7),
        Parallelism::new(8, 64),
    ]
}

fn build(
    ctv: bool,
    shares: &[AcceptedShare],
    found_block: &FoundBlock,
    cut: Option<WindowCut>,
    balances: &[CarryForwardBalance],
    parallelism: Parallelism,
) -> Result<AuditBundleBody, PrismError> {
    if ctv {
        build_audit_bundle_body_with_ctv_settlement_options_parallel(
            shares,
            found_block.clone(),
            cut,
            balances.to_vec(),
            PayoutPolicy::day_one_default(),
            2_000,
            ctv_config(),
            Some(FanoutFeeRatePolicy::new(1_000, 12_000)),
            Some("aaaaaaaa".into()),
            vec!["11".repeat(32)],
            &manifest_signing_key(),
            &ledger_signing_key(),
            parallelism,
        )
    } else {
        build_audit_bundle_body_with_coinbase_options_parallel(
            shares,
            found_block.clone(),
            cut,
            balances.to_vec(),
            PayoutPolicy::day_one_default(),
            Some("bbbbbbbb".into()),
            vec!["22".repeat(32)],
            &manifest_signing_key(),
            &ledger_signing_key(),
            parallelism,
        )
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[test]
fn the_fixture_is_a_dual_writer_window_with_late_rows_outside_its_cut() {
    let fixture = fixture();
    let cut = fixture.window_cut;
    assert_eq!(cut, WindowCut::new(Some(40), Some(39)).unwrap());
    let seqs: Vec<u64> = fixture.shares.iter().map(|share| share.share_seq).collect();
    assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]));
    // Both nodes' rows, a gap, and nothing above the cut: the late node-1
    // rows 41 and 43 are not in the window.
    assert!(seqs.contains(&39) && seqs.contains(&40) && !seqs.contains(&7));
    assert!(seqs.iter().all(|seq| *seq <= cut.top().unwrap()));
    // Node 1's clock ran ahead, but the cut only reaches rows stamped at or
    // before the anchor, so the fold's anchor rule keeps every share: a 3.0
    // verifier and this one agree on eligibility.
    let anchor = fixture.found_block.anchor_job_issued_at_ms;
    assert!(fixture
        .shares
        .iter()
        .all(|share| share.accepted_at_ms <= anchor && share.job_issued_at_ms <= anchor));
}

#[test]
fn a_cut_window_builds_verifies_and_restores_at_every_parallelism() {
    let fixture = fixture();
    let cut = Some(fixture.window_cut);
    for ctv in [false, true] {
        let serial = build(
            ctv,
            &fixture.shares,
            &fixture.found_block,
            cut,
            &fixture.prior_balances,
            Parallelism::serial(),
        )
        .unwrap();
        assert_eq!(serial.reward_manifest.cut, cut);
        let serial_bytes =
            canonical_audit_bundle_bytes_from_parts(&serial, &fixture.shares).unwrap();
        for parallelism in geometries() {
            let body = build(
                ctv,
                &fixture.shares,
                &fixture.found_block,
                cut,
                &fixture.prior_balances,
                parallelism,
            )
            .unwrap();
            assert_eq!(body, serial, "ctv {ctv} {parallelism:?}");
            assert_eq!(
                canonical_audit_bundle_bytes_from_parts(&body, &fixture.shares).unwrap(),
                serial_bytes
            );
        }
        verify_audit_parts(&serial, &fixture.shares, &ledger_public_key_hex())
            .unwrap_or_else(|error| panic!("ctv {ctv}: {error:?}"));
        let (header, window) = serial.reward_manifest.clone().into_parts();
        assert_eq!(header.cut, cut);
        let restored =
            restore_reward_manifest(header, &fixture.shares, &fixture.found_block).unwrap();
        assert_eq!(restored.shares, window);
        assert_eq!(restored, serial.reward_manifest);
    }
}

#[test]
fn the_manifest_records_the_cut_after_the_window_bounds_and_only_when_present() {
    let fixture = fixture();
    let with = build_prism_reward_manifest_with_cut(
        &fixture.shares,
        &fixture.found_block,
        Some(fixture.window_cut),
    )
    .unwrap();
    let text = String::from_utf8(canonical_reward_manifest_bytes(&with).unwrap()).unwrap();
    assert!(
        text.contains(r#""oldest_share_seq":4,"cut":{"0":40,"1":39},"included_share_count":28"#),
        "{text}"
    );
    let without = build_prism_reward_manifest(&fixture.shares, &fixture.found_block).unwrap();
    let text = String::from_utf8(canonical_reward_manifest_bytes(&without).unwrap()).unwrap();
    assert!(!text.contains("\"cut\""), "{text}");
    assert!(text.contains(r#""oldest_share_seq":4,"included_share_count":28"#));
    // Same fold: the cut is metadata the manifest commits, not a filter.
    let mut stripped = with.clone();
    stripped.cut = None;
    assert_eq!(stripped, without);
}

#[test]
fn without_a_cut_the_parallel_entry_point_builds_the_3_0_body() {
    let fixture = fixture();
    let legacy = build_audit_bundle_body_with_coinbase_options(
        &fixture.shares,
        fixture.found_block.clone(),
        fixture.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        Some("bbbbbbbb".into()),
        vec!["22".repeat(32)],
        &manifest_signing_key(),
        &ledger_signing_key(),
    )
    .unwrap();
    let no_cut = build(
        false,
        &fixture.shares,
        &fixture.found_block,
        None,
        &fixture.prior_balances,
        Parallelism::new(3, 2),
    )
    .unwrap();
    assert_eq!(no_cut, legacy);
    assert_eq!(
        canonical_audit_bundle_bytes_from_parts(&no_cut, &fixture.shares).unwrap(),
        canonical_audit_bundle_bytes_from_parts(&legacy, &fixture.shares).unwrap()
    );
}

#[test]
fn the_cut_changes_the_manifest_commitment_and_coinbase_but_not_the_window() {
    let fixture = fixture();
    let with = build(
        false,
        &fixture.shares,
        &fixture.found_block,
        Some(fixture.window_cut),
        &fixture.prior_balances,
        Parallelism::serial(),
    )
    .unwrap();
    let without = build(
        false,
        &fixture.shares,
        &fixture.found_block,
        None,
        &fixture.prior_balances,
        Parallelism::serial(),
    )
    .unwrap();
    // The window, its attestation and the payouts are identical...
    assert_eq!(
        with.reward_manifest.share_slice_digest_hex,
        without.reward_manifest.share_slice_digest_hex
    );
    assert_eq!(
        with.reward_manifest.entitlements,
        without.reward_manifest.entitlements
    );
    assert_eq!(
        with.ledger_window_attestation,
        without.ledger_window_attestation
    );
    assert_eq!(with.payout_policy_manifest, without.payout_policy_manifest);
    // ...and the commitment, which binds the manifest and so the cut, is not.
    assert_ne!(
        with.audit_commitment_leaves_hex,
        without.audit_commitment_leaves_hex
    );
    assert_ne!(
        with.audit_commitment_root_hex,
        without.audit_commitment_root_hex
    );
    assert_ne!(
        with.signed_coinbase_manifest.manifest,
        without.signed_coinbase_manifest.manifest
    );
}

fn verify_error(bundle: &AuditBundle) -> String {
    format!(
        "{:?}",
        verify_audit_bundle(bundle, &ledger_public_key_hex()).unwrap_err()
    )
}

#[test]
fn a_dropped_added_or_altered_cut_fails_verification() {
    let fixture = fixture();
    let cut = fixture.window_cut;
    let bundle = build(
        false,
        &fixture.shares,
        &fixture.found_block,
        Some(cut),
        &fixture.prior_balances,
        Parallelism::serial(),
    )
    .unwrap()
    .into_bundle(fixture.shares.clone());
    verify_audit_bundle(&bundle, &ledger_public_key_hex()).unwrap();
    let commitment = "AuditMismatch { artifact: \"audit_commitment_leaves\" }";

    let mut dropped = bundle.clone();
    dropped.reward_manifest.cut = None;
    assert_eq!(verify_error(&dropped), commitment);

    // Still above every share, so only the commitment can catch it.
    for altered in [
        WindowCut::new(Some(41), Some(39)).unwrap(),
        WindowCut::new(Some(40), Some(37)).unwrap(),
        WindowCut::new(Some(40), None).unwrap(),
    ] {
        let mut tampered = bundle.clone();
        tampered.reward_manifest.cut = Some(altered);
        assert_eq!(verify_error(&tampered), commitment, "{altered:?}");
    }

    let plain = build(
        false,
        &fixture.shares,
        &fixture.found_block,
        None,
        &fixture.prior_balances,
        Parallelism::serial(),
    )
    .unwrap()
    .into_bundle(fixture.shares.clone());
    verify_audit_bundle(&plain, &ledger_public_key_hex()).unwrap();
    let mut added = plain.clone();
    added.reward_manifest.cut = Some(cut);
    assert_eq!(verify_error(&added), commitment);

    // A cut below a share fails before any commitment is compared.
    let mut below = bundle.clone();
    below.reward_manifest.cut = Some(WindowCut::new(Some(38), Some(39)).unwrap());
    assert_eq!(
        verify_error(&below),
        "ShareOutsideWindowCut { share_seq: 40 }"
    );
}

#[test]
fn a_share_above_the_cut_is_refused_by_the_builder_and_the_fold() {
    let fixture = fixture();
    for (cut, share_seq) in [
        (WindowCut::new(Some(39), Some(39)).unwrap(), 40),
        (WindowCut::new(Some(38), Some(37)).unwrap(), 39),
        (WindowCut::new(None, None).unwrap(), 4),
    ] {
        for parallelism in geometries() {
            let error = build(
                false,
                &fixture.shares,
                &fixture.found_block,
                Some(cut),
                &fixture.prior_balances,
                parallelism,
            )
            .unwrap_err();
            assert_eq!(
                format!("{error:?}"),
                format!("ShareOutsideWindowCut {{ share_seq: {share_seq} }}"),
                "{cut:?} {parallelism:?}"
            );
        }
        let error =
            build_prism_reward_manifest_with_cut(&fixture.shares, &fixture.found_block, Some(cut))
                .unwrap_err();
        assert!(matches!(error, PrismError::ShareOutsideWindowCut { .. }));
    }
    // A share above the cut is refused even when the anchor makes it
    // ineligible: every input share must fit the cut.
    let mut shares = fixture.shares.clone();
    let mut late = shares.last().unwrap().clone();
    late.share_seq = 43;
    late.share_id = "miner-dave:late".into();
    late.accepted_at_ms = fixture.found_block.anchor_job_issued_at_ms + 1;
    shares.push(late);
    assert!(matches!(
        build_prism_reward_manifest_with_cut(
            &shares,
            &fixture.found_block,
            Some(fixture.window_cut)
        ),
        Err(PrismError::ShareOutsideWindowCut { share_seq: 43 })
    ));
}

#[test]
fn stored_manifest_headers_decode_with_and_without_a_cut() {
    let fixture = fixture();
    let manifest = build_prism_reward_manifest_with_cut(
        &fixture.shares,
        &fixture.found_block,
        Some(fixture.window_cut),
    )
    .unwrap();
    let (header, _) = manifest.into_parts();
    let json = serde_json::to_value(&header).unwrap();
    assert_eq!(json["cut"], serde_json::json!({"0": 40, "1": 39}));
    let decoded: PrismRewardManifestHeader = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(decoded, header);

    // A 3.0 header has no cut key and still decodes, as `None`.
    let mut legacy = json.clone();
    legacy.as_object_mut().unwrap().remove("cut");
    let decoded: PrismRewardManifestHeader = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(decoded.cut, None);
    assert!(!serde_json::to_string(&decoded).unwrap().contains("\"cut\""));

    // Unknown keys and malformed cuts are still refused.
    let mut unknown = legacy.clone();
    unknown["window_cut"] = serde_json::json!({"0": 40, "1": 39});
    assert!(serde_json::from_value::<PrismRewardManifestHeader>(unknown).is_err());
    for bad in [
        serde_json::json!({"0": 40}),
        serde_json::json!({"0": 0, "1": 39}),
        serde_json::json!([40, 39]),
    ] {
        let mut malformed = json.clone();
        malformed["cut"] = bad;
        assert!(serde_json::from_value::<PrismRewardManifestHeader>(malformed).is_err());
    }
}

/// The library's and the binaries' digests for the fixture, recorded when
/// the cut field was introduced (PRISM 3.1, builder version 1). A window with
/// a cut is a new input, so the builder version did not change; these pin its
/// bytes from here on. Never re-record an entry: a value that moves is a
/// canonical-bytes regression.
struct DualWriterGolden {
    canonical_sha256: &'static str,
    reward_manifest_sha256: &'static str,
    audit_commitment_root: &'static str,
}

const DUAL_WRITER_GOLDEN: DualWriterGolden = DualWriterGolden {
    canonical_sha256: "4723ab662bde0fea5b787b6853cb2860d706645734f5e6f025126400784fa0d9",
    reward_manifest_sha256: "55f8f89c61ef7ecf29c5e4b779f230006dd5c722b673ad7887f8eeadb4a3ab26",
    audit_commitment_root: "0b8974621e7381efa37fed7b57a0cf71c288ee7988dadc2cfa89ffd09787a665",
};

fn fixture_cli_input() -> serde_json::Value {
    let mut input: serde_json::Value = serde_json::from_str(FIXTURE_JSON).unwrap();
    input.as_object_mut().unwrap().remove("description");
    input["coinbase_script_sig_suffix_hex"] = serde_json::json!("bbbbbbbb");
    input["witness_merkle_leaves_hex"] = serde_json::json!(["22".repeat(32)]);
    input
}

fn temp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "qbit-prism-window-cut-{tag}-{}.json",
        std::process::id()
    ))
}

fn run(binary: &str, args: &[&std::ffi::OsStr]) -> std::process::Output {
    let output = Command::new(binary).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{binary}\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[test]
fn the_shipped_binaries_build_verify_and_canonicalize_a_cut_bundle() {
    let fixture = fixture();
    let library = build(
        false,
        &fixture.shares,
        &fixture.found_block,
        Some(fixture.window_cut),
        &fixture.prior_balances,
        Parallelism::serial(),
    )
    .unwrap()
    .into_bundle(fixture.shares.clone());
    let library_bytes = canonical_audit_bundle_bytes(&library).unwrap();

    let input_path = temp_path("input");
    fs::write(
        &input_path,
        serde_json::to_vec(&fixture_cli_input()).unwrap(),
    )
    .unwrap();
    let built = run(
        env!("CARGO_BIN_EXE_qbit-prism-build-audit-bundle"),
        &[
            "--signing-key-seed-hex".as_ref(),
            "42".repeat(32).as_ref(),
            "--ledger-signing-key-seed-hex".as_ref(),
            "43".repeat(32).as_ref(),
            "--input".as_ref(),
            input_path.as_os_str(),
            "--canonical-output".as_ref(),
        ],
    );
    let _ = fs::remove_file(&input_path);
    assert_eq!(built.stdout, library_bytes);

    let report = verify_audit_bundle(&library, &ledger_public_key_hex()).unwrap();
    let bundle_path = temp_path("bundle");
    fs::write(&bundle_path, &built.stdout).unwrap();
    let verified = run(
        env!("CARGO_BIN_EXE_qbit-prism-audit-verify"),
        &[
            bundle_path.as_os_str(),
            "--coinbase-tx-hex".as_ref(),
            report.coinbase_tx_hex.as_ref(),
            "--ledger-writer-public-key-hex".as_ref(),
            ledger_public_key_hex().as_ref(),
            "--expected-coinbase-value-sats".as_ref(),
            report.coinbase_value_sats.to_string().as_ref(),
        ],
    );
    assert!(String::from_utf8_lossy(&verified.stdout)
        .contains("qbit.prism.audit-verification-report.v1"));

    // The canonicalizer reproduces the bytes from a pretty, reordered copy.
    let mut reordered = serde_json::to_value(&library).unwrap();
    reordered["reward_manifest"]["cut"] = serde_json::json!({"1": 39, "0": 40});
    fs::write(&bundle_path, serde_json::to_vec_pretty(&reordered).unwrap()).unwrap();
    let canonical = run(
        env!("CARGO_BIN_EXE_qbit-prism-audit-canonicalize"),
        &["--input".as_ref(), bundle_path.as_os_str()],
    );
    let _ = fs::remove_file(&bundle_path);
    assert_eq!(canonical.stdout, library_bytes);

    let golden = DUAL_WRITER_GOLDEN;
    assert_eq!(sha256_hex(&library_bytes), golden.canonical_sha256);
    assert_eq!(
        sha256_hex(&canonical_reward_manifest_bytes(&library.reward_manifest).unwrap()),
        golden.reward_manifest_sha256
    );
    assert_eq!(
        library.audit_commitment_root_hex.as_deref(),
        Some(golden.audit_commitment_root)
    );
    assert_eq!(report.audit_bundle_sha256_hex, golden.canonical_sha256);
}

#[test]
fn a_3_0_verifier_view_of_a_cut_bundle_is_refused_loudly() {
    // A verifier that predates the field ignores it on decode and so rebuilds
    // the manifest without a cut. Model that by decoding the canonical bytes
    // into a value, dropping the key and verifying the result: the commitment
    // leaf no longer matches. 3.0 binaries refuse dual-writer bundles; they
    // never mis-verify one.
    let fixture = fixture();
    let bundle = build(
        false,
        &fixture.shares,
        &fixture.found_block,
        Some(fixture.window_cut),
        &fixture.prior_balances,
        Parallelism::serial(),
    )
    .unwrap()
    .into_bundle(fixture.shares.clone());
    let mut value = serde_json::to_value(&bundle).unwrap();
    value["reward_manifest"]
        .as_object_mut()
        .unwrap()
        .remove("cut");
    let old_view: AuditBundle = serde_json::from_value(value).unwrap();
    assert_eq!(
        verify_error(&old_view),
        "AuditMismatch { artifact: \"audit_commitment_leaves\" }"
    );
}

/// xorshift64*, as the builder's parallel tests use: deterministic and
/// dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

/// One ledger row of the model: its node, its fields as a share.
#[derive(Clone)]
struct Row {
    node: u8,
    share: AcceptedShare,
}

/// The server's selection, restated over a row list: eligible rows are
/// accepted (all rows here are), stamped at or before the anchor, and inside
/// the cut for their node; the window is the newest-first run of them up to
/// the crossing row.
fn select(rows: &[Row], anchor: i64, cut: WindowCut, weight: u128) -> Vec<AcceptedShare> {
    let mut eligible: Vec<&Row> = rows
        .iter()
        .filter(|row| {
            row.share.accepted_at_ms <= anchor
                && row.share.job_issued_at_ms <= anchor
                && cut.admits(row.node, row.share.share_seq)
        })
        .collect();
    eligible.sort_by_key(|row| std::cmp::Reverse(row.share.share_seq));
    let mut remaining = weight;
    let mut window = Vec::new();
    for row in eligible {
        if remaining == 0 {
            break;
        }
        remaining = remaining.saturating_sub(row.share.share_difficulty);
        window.push(row.share.clone());
    }
    window.reverse();
    window
}

/// The builder's cut for `node` at `anchor`: its newest row at or below
/// `high_water` stamped at or before the anchor (for the building node that
/// is its newest row; for the peer, its newest synced one).
fn cut_entry(rows: &[Row], node: u8, high_water: u64, anchor: i64) -> Option<u64> {
    rows.iter()
        .filter(|row| {
            row.node == node
                && row.share.share_seq <= high_water
                && row.share.accepted_at_ms <= anchor
                && row.share.job_issued_at_ms <= anchor
        })
        .map(|row| row.share.share_seq)
        .max()
}

#[test]
fn randomized_cut_windows_fold_the_same_on_both_nodes_and_verify() {
    let mut rng = Rng(0x5EED_D2C7_0000_0031);
    for round in 0..64 {
        // Two writers: node 0 even and node 1 odd share_seq values, each
        // with its own monotone clock; node 1's runs up to 2 s ahead or
        // behind. A row's accepted_at is its node's clock at append.
        let skew = rng.below(4_001) as i64 - 2_000;
        let mut clock = [1_900_000_000_000i64, 1_900_000_000_000 + skew];
        let mut next = [2u64, 1];
        let mut rows = Vec::new();
        for index in 0..(40 + rng.below(160)) {
            let node = (rng.below(3) == 0) as u8;
            clock[node as usize] += 1 + rng.below(50) as i64;
            let seq = next[node as usize];
            next[node as usize] += 2 * (1 + rng.below(2));
            rows.push(Row {
                node,
                share: AcceptedShare {
                    share_seq: seq,
                    share_id: format!("n{node}-{seq}-{index}"),
                    miner_id: format!("miner-{}", rng.below(6)),
                    order_key: format!("{:02}", rng.below(6)),
                    p2mr_program_hex: hex::encode([rng.below(6) as u8 + 1; 32]),
                    share_difficulty: 1 + u128::from(rng.below(5_000)),
                    network_difficulty: 1,
                    template_height: 100,
                    job_id: format!("job-{node}"),
                    job_issued_at_ms: clock[node as usize] - rng.below(20) as i64,
                    accepted_at_ms: clock[node as usize],
                    ntime: 1,
                    credit_policy: None,
                },
            });
        }
        // Node 0 builds at its clock; node 1's rows have been synced up to a
        // high-water mark, and later ones arrive after the cut.
        let anchor = clock[0];
        let synced = rows
            .iter()
            .filter(|row| row.node == 1)
            .map(|row| row.share.share_seq)
            .filter(|_| rng.below(4) != 0)
            .max()
            .unwrap_or(0);
        let cut = WindowCut::new(
            cut_entry(&rows, 0, u64::MAX, anchor),
            cut_entry(&rows, 1, synced, anchor),
        )
        .unwrap();
        let network_difficulty = 1 + u128::from(rng.below(30_000));
        let weight = network_difficulty * 8;
        let on_a = select(&rows, anchor, cut, weight);
        if on_a.is_empty() {
            continue;
        }
        // On node 1 the same rows below the cut exist, plus node 1's later
        // rows and minus node 0's that have not reached it: the window is a
        // function of the rows inside the cut only.
        let on_b_rows: Vec<Row> = rows
            .iter()
            .filter(|row| row.node == 1 || cut.admits(0, row.share.share_seq))
            .cloned()
            .collect();
        assert_eq!(
            select(&on_b_rows, anchor, cut, weight),
            on_a,
            "round {round}: node 1 reproduces node 0's window"
        );
        // Every share of the cut passes the anchor rule, so eligibility
        // agrees with the fold's (and a 3.0 verifier's).
        assert!(on_a
            .iter()
            .all(|share| share.accepted_at_ms <= anchor && share.job_issued_at_ms <= anchor));
        let found_block = FoundBlock {
            block_height: 101,
            coinbase_value_sats: 100_000_000 + rng.below(1_000_000),
            network_difficulty,
            anchor_job_issued_at_ms: anchor,
        };
        let body = build(
            round % 3 == 0,
            &on_a,
            &found_block,
            Some(cut),
            &[],
            Parallelism::new(2 + round % 3, 1 + round % 7),
        )
        .unwrap_or_else(|error| panic!("round {round}: {error:?}"));
        let serial = build(
            round % 3 == 0,
            &on_a,
            &found_block,
            Some(cut),
            &[],
            Parallelism::serial(),
        )
        .unwrap();
        assert_eq!(body, serial, "round {round}");
        verify_audit_parts(&body, &on_a, &ledger_public_key_hex())
            .unwrap_or_else(|error| panic!("round {round}: {error:?}"));
    }
}
