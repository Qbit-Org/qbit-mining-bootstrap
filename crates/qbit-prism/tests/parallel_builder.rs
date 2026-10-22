//! The builder's parallel fast path (#275) produces the serial path's bytes.
//!
//! Every randomized window is built twice, once through the unchanged serial
//! entry points and once through the `_parallel` variants at several worker
//! and chunk geometries, and the canonical audit bytes, the pipelined audit
//! hash, the share-array digest and the commitment leaf must be identical.
//! The verifier, which is serial, must accept the parallel body. Golden
//! vectors pin one fixed window's digests so a change to either path is
//! caught even when both drift together.

use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle_body_with_coinbase_options,
    build_audit_bundle_body_with_coinbase_options_parallel,
    build_audit_bundle_body_with_ctv_settlement_options,
    build_audit_bundle_body_with_ctv_settlement_options_parallel,
    canonical_audit_bundle_bytes_from_parts, canonical_reward_manifest_bytes,
    prism_audit_commitment_leaf_hex, restore_reward_manifest, verify_audit_parts, AcceptedShare,
    AuditBundleBody, CanonicalAuditHashPrefix, CarryForwardBalance, FanoutFeeRatePolicy,
    FoundBlock, Parallelism, PayoutPolicy, PrismError, SettlementModeConfig,
};
use sha2::{Digest, Sha256};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*: deterministic, dependency-free.
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

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

const STRING_EDGES: &[&str] = &[
    "plain",
    "quote\"inside",
    "back\\slash",
    "new\nline\ttab\r",
    "nul\u{0}byte",
    "unicode-é-日本-🙂",
    "line\u{2028}sep\u{2029}",
    "ctl\u{1f}\u{7f}",
    "</script>&'",
    "",
];

const DIFFICULTY_EDGES: &[u128] = &[
    1,
    2,
    7,
    u32::MAX as u128,
    u64::MAX as u128,
    u64::MAX as u128 + 1,
    u128::MAX / 64,
    u128::MAX / 16,
];

fn manifest_signing_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap()
}

fn ledger_signing_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap()
}

fn ctv_config() -> SettlementModeConfig {
    SettlementModeConfig {
        max_coinbase_settlement_outputs: 4,
        max_direct_coinbase_outputs: 2,
        max_fanout_recipients_per_transaction: 3,
        reserved_coinbase_outputs: 0,
    }
}

fn prior_balances(rng: &mut Rng) -> Vec<CarryForwardBalance> {
    (0..rng.below(4))
        .map(|index| CarryForwardBalance {
            recipient_id: format!("carry-{}-{index}", rng.pick(STRING_EDGES)),
            order_key: format!("{:02}", rng.below(100)),
            p2mr_program_hex: hex::encode([rng.below(256) as u8; 32]),
            balance_sats: rng.below(20_000) as i128,
        })
        .collect()
}

/// A random window: sizes from empty to a few thousand, every string edge,
/// u128 difficulties at both ends, ineligible shares after the anchor, a
/// credit policy on some shares, and ascending, descending or shuffled order.
fn random_window(rng: &mut Rng, max_len: u64) -> (Vec<AcceptedShare>, FoundBlock) {
    let count = match rng.below(6) {
        0 => 0,
        1 => 1,
        2 => rng.below(12),
        3 => rng.below(200),
        _ => rng.below(max_len),
    };
    let anchor = 1_000_000 + rng.below(1_000) as i64;
    let miners: Vec<(String, String, String)> = (0..1 + rng.below(9))
        .map(|index| {
            (
                format!("miner-{}-{index}", rng.pick(STRING_EDGES)),
                format!("{:02}", rng.below(100)),
                hex::encode([rng.below(256) as u8; 32]),
            )
        })
        .collect();
    let with_credit = rng.below(3) == 0;
    let mut shares: Vec<AcceptedShare> = (1..=count)
        .map(|share_seq| {
            let (miner_id, order_key, program) = rng.pick(&miners).clone();
            let job_issued_at_ms = anchor - rng.below(5_000) as i64 + rng.below(20) as i64 * 300;
            AcceptedShare {
                share_seq,
                share_id: format!("share-{share_seq}-{}", rng.pick(STRING_EDGES)),
                miner_id,
                order_key,
                p2mr_program_hex: program,
                share_difficulty: if rng.below(4) == 0 {
                    *rng.pick(DIFFICULTY_EDGES)
                } else {
                    1 + rng.below(50) as u128
                },
                network_difficulty: *rng.pick(DIFFICULTY_EDGES),
                template_height: rng.below(1_000_000),
                job_id: format!("job-{}", rng.pick(STRING_EDGES)),
                job_issued_at_ms,
                accepted_at_ms: job_issued_at_ms + rng.below(1_500) as i64,
                ntime: rng.below(u32::MAX as u64) as u32,
                credit_policy: (with_credit && rng.below(2) == 0).then(|| "stale-grace".into()),
            }
        })
        .collect();
    match rng.below(3) {
        0 => {}
        1 => shares.reverse(),
        _ => {
            for index in (1..shares.len()).rev() {
                let other = rng.below(index as u64 + 1) as usize;
                shares.swap(index, other);
            }
        }
    }
    let network_difficulty = match rng.below(3) {
        0 => 1 + rng.below(3) as u128,
        1 => 1 + rng.below(200) as u128,
        _ => *rng.pick(DIFFICULTY_EDGES),
    };
    let found_block = FoundBlock {
        block_height: 100 + rng.below(1_000),
        coinbase_value_sats: 1 + rng.below(5_000_000_000),
        network_difficulty,
        anchor_job_issued_at_ms: anchor,
    };
    (shares, found_block)
}

fn geometries() -> Vec<Parallelism> {
    vec![
        Parallelism::serial(),
        Parallelism::new(2, 1),
        Parallelism::new(3, 2),
        Parallelism::new(4, 7),
        Parallelism::new(8, 64),
        Parallelism::new(2, 4096),
    ]
}

fn error_text(error: &PrismError) -> String {
    format!("{error:?}")
}

fn build_serial(
    ctv: bool,
    shares: &[AcceptedShare],
    found_block: &FoundBlock,
    balances: &[CarryForwardBalance],
) -> Result<AuditBundleBody, PrismError> {
    if ctv {
        build_audit_bundle_body_with_ctv_settlement_options(
            shares,
            found_block.clone(),
            balances.to_vec(),
            PayoutPolicy::day_one_default(),
            2_000,
            ctv_config(),
            Some(FanoutFeeRatePolicy::new(1_000, 12_000)),
            Some("aaaaaaaa".into()),
            vec!["11".repeat(32)],
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
    } else {
        build_audit_bundle_body_with_coinbase_options(
            shares,
            found_block.clone(),
            balances.to_vec(),
            PayoutPolicy::day_one_default(),
            Some("bbbbbbbb".into()),
            vec!["22".repeat(32), "33".repeat(32)],
            &manifest_signing_key(),
            &ledger_signing_key(),
        )
    }
}

fn build_parallel(
    ctv: bool,
    shares: &[AcceptedShare],
    found_block: &FoundBlock,
    balances: &[CarryForwardBalance],
    parallelism: Parallelism,
) -> Result<AuditBundleBody, PrismError> {
    if ctv {
        build_audit_bundle_body_with_ctv_settlement_options_parallel(
            shares,
            found_block.clone(),
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
            balances.to_vec(),
            PayoutPolicy::day_one_default(),
            Some("bbbbbbbb".into()),
            vec!["22".repeat(32), "33".repeat(32)],
            &manifest_signing_key(),
            &ledger_signing_key(),
            parallelism,
        )
    }
}

/// Every digest the refresh pipeline publishes for `(body, shares)`.
struct Digests {
    canonical_bytes_sha256: String,
    pipelined_audit_sha256: String,
    share_array_sha256: String,
    reward_manifest_sha256: String,
    leaf_hex: String,
}

fn digests(body: &AuditBundleBody, shares: &[AcceptedShare], parallelism: Parallelism) -> Digests {
    let (prefix, share_array) =
        CanonicalAuditHashPrefix::new_with_share_digest(shares, parallelism).unwrap();
    let canonical = canonical_audit_bundle_bytes_from_parts(body, shares).unwrap();
    Digests {
        canonical_bytes_sha256: hex::encode(Sha256::digest(&canonical)),
        pipelined_audit_sha256: prefix.finish_with(body, parallelism).unwrap(),
        share_array_sha256: hex::encode(share_array),
        reward_manifest_sha256: hex::encode(Sha256::digest(
            canonical_reward_manifest_bytes(&body.reward_manifest).unwrap(),
        )),
        leaf_hex: body.audit_commitment_leaves_hex[0].clone(),
    }
}

fn check_window(
    ctv: bool,
    shares: &[AcceptedShare],
    found_block: &FoundBlock,
    balances: &[CarryForwardBalance],
    label: &str,
) {
    let serial = build_serial(ctv, shares, found_block, balances);
    for parallelism in geometries() {
        let parallel = build_parallel(ctv, shares, found_block, balances, parallelism);
        match (&serial, &parallel) {
            (Err(expected), Err(actual)) => {
                assert_eq!(
                    error_text(expected),
                    error_text(actual),
                    "{label} {parallelism:?}"
                );
                continue;
            }
            (Err(expected), Ok(_)) => panic!("{label} {parallelism:?}: serial failed {expected:?}"),
            (Ok(_), Err(actual)) => panic!("{label} {parallelism:?}: parallel failed {actual:?}"),
            (Ok(_), Ok(_)) => {}
        }
        let (serial, parallel) = (serial.as_ref().unwrap(), parallel.as_ref().unwrap());
        assert_eq!(serial, parallel, "{label} {parallelism:?}: body differs");
        let serial_bytes = canonical_audit_bundle_bytes_from_parts(serial, shares).unwrap();
        let parallel_bytes = canonical_audit_bundle_bytes_from_parts(parallel, shares).unwrap();
        assert_eq!(
            serial_bytes, parallel_bytes,
            "{label} {parallelism:?}: bytes differ"
        );
        let expected = Digests {
            canonical_bytes_sha256: hex::encode(Sha256::digest(&serial_bytes)),
            pipelined_audit_sha256: CanonicalAuditHashPrefix::new(shares)
                .unwrap()
                .finish(serial)
                .unwrap(),
            share_array_sha256: hex::encode(Sha256::digest(serde_json::to_vec(shares).unwrap())),
            reward_manifest_sha256: hex::encode(Sha256::digest(
                canonical_reward_manifest_bytes(&serial.reward_manifest).unwrap(),
            )),
            leaf_hex: prism_audit_commitment_leaf_hex(
                &serial.reward_manifest,
                &serial.payout_policy_manifest,
            )
            .unwrap(),
        };
        let actual = digests(parallel, shares, parallelism);
        assert_eq!(
            actual.canonical_bytes_sha256, expected.canonical_bytes_sha256,
            "{label}"
        );
        assert_eq!(
            actual.pipelined_audit_sha256, expected.canonical_bytes_sha256,
            "{label}"
        );
        assert_eq!(
            actual.pipelined_audit_sha256, expected.pipelined_audit_sha256,
            "{label}"
        );
        assert_eq!(
            actual.share_array_sha256, expected.share_array_sha256,
            "{label}"
        );
        assert_eq!(
            actual.reward_manifest_sha256, expected.reward_manifest_sha256,
            "{label}"
        );
        assert_eq!(actual.leaf_hex, expected.leaf_hex, "{label}");
        // The serial verifier accepts the parallel body and rebuilds its window.
        verify_audit_parts(parallel, shares, &ledger_signing_key().public_key_hex())
            .unwrap_or_else(|error| panic!("{label} {parallelism:?}: verify {error:?}"));
        let (header, window) = parallel.reward_manifest.clone().into_parts();
        let restored = restore_reward_manifest(header, shares, found_block).unwrap();
        assert_eq!(
            restored.shares, window,
            "{label} {parallelism:?}: restored window"
        );
    }
}

#[test]
fn randomized_windows_build_identical_bodies_bytes_and_digests() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for round in 0..96 {
        let (shares, found_block) = random_window(&mut rng, 2_000);
        let balances = prior_balances(&mut rng);
        let ctv = round % 4 == 3;
        check_window(
            ctv,
            &shares,
            &found_block,
            &balances,
            &format!("round {round} ({} shares, ctv {ctv})", shares.len()),
        );
    }
}

#[test]
fn boundary_windows_build_identical_bodies() {
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    // Exact chunk boundaries for every geometry, and one share past each.
    for count in [
        1usize, 2, 3, 4, 7, 8, 14, 15, 63, 64, 65, 127, 128, 129, 4096, 4097,
    ] {
        let (_, mut found_block) = random_window(&mut rng, 1);
        let shares: Vec<AcceptedShare> = (1..=count as u64)
            .map(|share_seq| AcceptedShare {
                share_seq,
                share_id: format!("share-{share_seq}"),
                miner_id: format!("miner-{}", share_seq % 5),
                order_key: format!("{:02}", share_seq % 5),
                p2mr_program_hex: hex::encode([(share_seq % 5) as u8; 32]),
                share_difficulty: if share_seq == 1 {
                    u128::from(u64::MAX) + 12_345
                } else {
                    20
                },
                network_difficulty: u128::MAX,
                template_height: 100,
                job_id: "job".into(),
                job_issued_at_ms: found_block.anchor_job_issued_at_ms - 10,
                accepted_at_ms: found_block.anchor_job_issued_at_ms - 5,
                ntime: 1,
                credit_policy: (share_seq % 3 == 0).then(|| "stale-grace".into()),
            })
            .collect();
        found_block.network_difficulty = (count.max(1) as u128 * 20) / 8;
        for ctv in [false, true] {
            check_window(
                ctv,
                &shares,
                &found_block,
                &[],
                &format!("count {count} ctv {ctv}"),
            );
        }
    }
}

#[test]
fn empty_window_prefix_and_digest_match_the_serial_path() {
    for parallelism in geometries() {
        let (prefix, digest) =
            CanonicalAuditHashPrefix::new_with_share_digest(&[], parallelism).unwrap();
        assert_eq!(hex::encode(digest), hex::encode(Sha256::digest(b"[]")));
        let mut rng = Rng(7);
        let (shares, found_block) = loop {
            let (shares, found_block) = random_window(&mut rng, 50);
            if !shares.is_empty()
                && build_serial(false, &shares, &found_block, &[]).is_ok()
                && shares.iter().all(|share| share.credit_policy.is_none())
            {
                break (shares, found_block);
            }
        };
        let body = build_serial(false, &shares, &found_block, &[]).unwrap();
        // The empty window's prefix is the v1 schema; its suffix is the body's.
        assert_eq!(
            prefix.finish_with(&body, parallelism).unwrap(),
            CanonicalAuditHashPrefix::new(&[])
                .unwrap()
                .finish(&body)
                .unwrap()
        );
    }
}

/// (name, canonical audit bytes sha256, share-array sha256, reward-manifest
/// sha256) recorded from the serial builders at 3.x.x 7b78ea6f, before the
/// parallel fast path existed. Both paths must still produce exactly these
/// digests; never re-record them from a build that contains the fast path.
const GOLDEN: [(&str, &str, &str, &str); 2] = [
    (
        "coinbase_options/2048",
        "644daf335593c96fbb368310d726ed906df9cfbd59223013e7bee025a8aa88e9",
        "f6090b4bf622adf7e06b7698c3e8e38be0803ad9e0f1737719314782d76b0942",
        "5911f26eddf010cec907b9d3fafda7b2438247a07ebe7bdb784e60dbd8444f51",
    ),
    (
        "ctv_settlement/2048",
        "71269b7579306219dd49bb1e3003e21563a50a5d234db3a8b2ab1c8bd43413b7",
        "f6090b4bf622adf7e06b7698c3e8e38be0803ad9e0f1737719314782d76b0942",
        "5911f26eddf010cec907b9d3fafda7b2438247a07ebe7bdb784e60dbd8444f51",
    ),
];

fn golden_window() -> (Vec<AcceptedShare>, FoundBlock) {
    let mut rng = Rng(0x0BAD_5EED_0BAD_5EED);
    let shares: Vec<AcceptedShare> = (1..=2_048u64)
        .map(|share_seq| {
            let miner = share_seq % 7;
            AcceptedShare {
                share_seq,
                share_id: format!(
                    "share-{share_seq}-{}",
                    STRING_EDGES[share_seq as usize % 10]
                ),
                miner_id: format!("miner-{miner}-{}", STRING_EDGES[miner as usize]),
                order_key: format!("{miner:02}"),
                p2mr_program_hex: hex::encode([miner as u8 + 1; 32]),
                // The oldest share exceeds u64::MAX and is the partially counted
                // window boundary; the rest keep the window economically sane.
                share_difficulty: if share_seq == 1 {
                    u64::MAX as u128 + 12_345
                } else {
                    ((miner + 1) * (miner + 1) * (share_seq % 5 + 1)) as u128
                },
                network_difficulty: DIFFICULTY_EDGES[share_seq as usize % 8],
                template_height: 100,
                job_id: format!("job-{}", rng.below(1_000)),
                job_issued_at_ms: 1_000 + share_seq as i64,
                accepted_at_ms: 1_000 + share_seq as i64,
                ntime: 1_800_000_000,
                credit_policy: (share_seq % 5 == 0).then(|| "stale-grace".into()),
            }
        })
        .collect();
    let found_block = FoundBlock {
        block_height: 101,
        coinbase_value_sats: 500_000_000,
        network_difficulty: 5_000,
        anchor_job_issued_at_ms: 1_000 + 2_048,
    };
    (shares, found_block)
}

#[test]
fn golden_vectors_pin_both_paths() {
    let (shares, found_block) = golden_window();
    let balances = [];
    let mut mismatches = Vec::new();
    for (index, ctv) in [false, true].into_iter().enumerate() {
        let (name, canonical, share_array, reward) = GOLDEN[index];
        let serial = build_serial(ctv, &shares, &found_block, &balances).unwrap();
        let serial_digests = digests(&serial, &shares, Parallelism::serial());
        for parallelism in geometries() {
            let parallel =
                build_parallel(ctv, &shares, &found_block, &balances, parallelism).unwrap();
            let actual = digests(&parallel, &shares, parallelism);
            for (label, expected, value, serial_value) in [
                (
                    "canonical_bytes_sha256",
                    canonical,
                    &actual.canonical_bytes_sha256,
                    &serial_digests.canonical_bytes_sha256,
                ),
                (
                    "pipelined_audit_sha256",
                    canonical,
                    &actual.pipelined_audit_sha256,
                    &serial_digests.pipelined_audit_sha256,
                ),
                (
                    "share_array_sha256",
                    share_array,
                    &actual.share_array_sha256,
                    &serial_digests.share_array_sha256,
                ),
                (
                    "reward_manifest_sha256",
                    reward,
                    &actual.reward_manifest_sha256,
                    &serial_digests.reward_manifest_sha256,
                ),
            ] {
                if value != expected {
                    mismatches.push(format!(
                        "{name} {label} {parallelism:?}: got {value}, pinned {expected}, serial path gives {serial_value}"
                    ));
                }
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "golden mismatches:\n{}",
        mismatches.join("\n")
    );
}

fn vm_rss_mib() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|rest| rest.trim().split(' ').next())
        .and_then(|kib| kib.parse::<f64>().ok())
        .map_or(0.0, |kib| kib / 1024.0)
}

/// Memory probe for the builder at the refresh's shape (400k shares): builds
/// the tee, the body and the suffix `B275_RSS_CYCLES` times at
/// `B275_RSS_WORKERS` workers and prints the process RSS after each cycle.
/// Run under the heavy lock; compare against `MALLOC_ARENA_MAX` settings.
#[test]
#[ignore = "400k memory probe; run under the heavy test lock"]
fn rss_probe_400k() {
    let workers: usize = std::env::var("B275_RSS_WORKERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8);
    let cycles: usize = std::env::var("B275_RSS_CYCLES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(6);
    let parallelism = if workers <= 1 {
        Parallelism::serial()
    } else {
        Parallelism::new(workers, 4096)
    };
    let (seed, found_block) = golden_window();
    let seed = seed[1].clone();
    let shares: Vec<AcceptedShare> = (0..400_000u64)
        .map(|index| {
            let mut share = seed.clone();
            share.share_seq = index + 1;
            share.share_id = format!("share-{index:07}-{}", "x".repeat(24));
            share.miner_id = format!("miner-{:04}", index % 2_000);
            share.order_key = format!("{:04}", index % 2_000);
            share.job_id = format!("job-{:06}", index % 500);
            share.share_difficulty = 20;
            share.job_issued_at_ms = found_block.anchor_job_issued_at_ms - 10;
            share.accepted_at_ms = found_block.anchor_job_issued_at_ms - 5;
            share
        })
        .collect();
    let found_block = FoundBlock {
        network_difficulty: 400_000 * 20 / 8,
        ..found_block
    };
    println!(
        "RSS_PROBE workers={workers} after_window={:.0}",
        vm_rss_mib()
    );
    for cycle in 0..cycles {
        let started = std::time::Instant::now();
        let (prefix, digest) =
            CanonicalAuditHashPrefix::new_with_share_digest(&shares, parallelism).unwrap();
        let body = build_parallel(false, &shares, &found_block, &[], parallelism).unwrap();
        let audit = prefix.finish_with(&body, parallelism).unwrap();
        let peak = vm_rss_mib();
        drop(body);
        println!(
            "RSS_PROBE cycle={cycle} ms={} peak_mib={peak:.0} after_drop_mib={:.0} digest={} audit={}",
            started.elapsed().as_millis(),
            vm_rss_mib(),
            &hex::encode(digest)[..8],
            &audit[..8]
        );
    }
}
