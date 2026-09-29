//! Property tests for the Stratum line codec and the codec's target
//! conversions (#575). Small case counts keep them in the per-PR suite; the
//! cargo-fuzz targets in `fuzz/` search the same ground for hours nightly.
//! They read no environment input, so they are not gated.
use num_bigint::BigUint;
use num_traits::{One, ToPrimitive, Zero};
use proptest::prelude::*;
use qbit_pool_builder::{build_manifest, CoinbaseBuildRequest, WeightedEntitlement};
use qbit_prism_server::{
    codec::{self, Job},
    ledger::SessionId,
    stratum::{
        serve_connection, MiningBackend, MiningJob, StaleGrace, StratumConfig, StratumError, Worker,
    },
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Allocates sessions and refuses every username, so a connection answers
/// requests without ever building work: its output is a pure function of
/// its input.
#[derive(Default)]
struct NoWork(AtomicU32);

impl MiningBackend for NoWork {
    type Context = ();
    async fn new_session_id(&self) -> Result<SessionId, StratumError> {
        Ok((self.0.fetch_add(1, Ordering::Relaxed) + 1).into())
    }
    async fn authorize(&self, _username: &str) -> Result<Worker, StratumError> {
        Err(StratumError::new(
            20,
            "invalid payout",
            "unauthorized-worker",
        ))
    }
    async fn build_job(
        &self,
        _worker: &Worker,
        _extranonce1: &str,
        _difficulty: f64,
        _minimum_difficulty: f64,
    ) -> Result<MiningJob<()>, StratumError> {
        unreachable!("no worker is ever authorized")
    }
    async fn submit(
        &self,
        _worker: &Worker,
        _job: &MiningJob<()>,
        _submission: codec::Submission,
        _grace: StaleGrace,
    ) -> Result<(), StratumError> {
        unreachable!("no worker is ever authorized")
    }
}

/// Feed `writes` to one connection over an in-memory pipe, end the stream,
/// and return every line the server wrote.
fn converse(config: StratumConfig, writes: Vec<Vec<u8>>) -> Vec<Value> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let (client, server) = tokio::io::duplex(1 << 20);
        let (server_reader, server_writer) = tokio::io::split(server);
        let (refresh, refresh_rx) = tokio::sync::watch::channel(0);
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(serve_connection(
            server_reader,
            server_writer,
            Arc::new(NoWork::default()),
            config,
            refresh_rx,
            shutdown_rx,
            Arc::new(qbit_prism_server::metrics::Metrics::default()),
        ));
        let (mut reader, mut writer) = tokio::io::split(client);
        for write in writes {
            // The server may close first (an oversize frame); what it wrote
            // before closing is the result.
            if writer.write_all(&write).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let _ = writer.shutdown().await;
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.unwrap();
        task.await.unwrap().unwrap();
        drop((refresh, shutdown));
        output
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect()
    })
}

/// Cut `stream` at the given fractions of its length.
fn fragment(stream: &[u8], cuts: &[f64]) -> Vec<Vec<u8>> {
    let mut at: Vec<usize> = cuts
        .iter()
        .map(|c| (c * stream.len() as f64) as usize)
        .collect();
    at.push(0);
    at.push(stream.len());
    at.sort_unstable();
    at.dedup();
    at.windows(2).map(|w| stream[w[0]..w[1]].to_vec()).collect()
}

fn json_id() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<u64>().prop_map(|n| json!(n)),
        any::<i64>().prop_map(|n| json!(n)),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(|f| json!(f)),
        any::<String>().prop_map(Value::String),
        (any::<u8>(), any::<String>()).prop_map(|(n, s)| json!({"n": n, "s": [s]})),
    ]
}

fn request() -> impl Strategy<Value = Value> {
    let method = prop_oneof![
        Just("mining.subscribe"),
        Just("mining.extranonce.subscribe"),
        Just("mining.suggest_difficulty"),
        Just("mining.configure"),
        Just("mining.get_health"),
        Just("mining.authorize"),
        Just("mining.submit"),
        Just("mining.bogus"),
    ];
    let params = prop_oneof![
        Just(json!([])),
        any::<String>().prop_map(|s| json!([s, "x"])),
        any::<f64>().prop_map(|f| json!([f.to_string()])),
        Just(json!([["version-rolling"], {"version-rolling.mask": "1fffe000"}])),
        Just(json!({"not": "an array"})),
    ];
    (json_id(), method, params)
        .prop_map(|(id, method, params)| json!({"id": id, "method": method, "params": params}))
}

/// Answers, not notifications: every frame gets exactly one.
fn answers(lines: &[Value]) -> Vec<&Value> {
    lines.iter().filter(|l| l.get("method").is_none()).collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, failure_persistence: None, ..ProptestConfig::default() })]

    /// Every frame is answered once, in order, with its own id, whatever the
    /// id's JSON type; and cutting the stream into fragments at any points
    /// changes nothing the server writes.
    #[test]
    fn frames_round_trip_their_ids_under_any_fragmentation(
        requests in prop::collection::vec(request(), 1..8),
        cuts in prop::collection::vec(0.0f64..1.0, 0..12),
    ) {
        let stream: Vec<u8> = requests
            .iter()
            .flat_map(|r| {
                let mut line = serde_json::to_vec(r).unwrap();
                line.push(b'\n');
                line
            })
            .collect();
        let whole = converse(StratumConfig::default(), vec![stream.clone()]);
        let pieces = converse(StratumConfig::default(), fragment(&stream, &cuts));
        prop_assert_eq!(&whole, &pieces);
        let answers = answers(&whole);
        prop_assert_eq!(answers.len(), requests.len());
        for (answer, request) in answers.iter().zip(&requests) {
            prop_assert_eq!(&answer["id"], &request["id"]);
            prop_assert!(answer["result"].is_null() != answer["error"].is_null());
        }
    }

    /// A frame of exactly the bound (newline included) is served; one byte
    /// more is refused with one null-id answer and the connection closes,
    /// leaving later frames unread.
    #[test]
    fn the_message_bound_is_exact_and_an_oversize_frame_closes(
        max in 256usize..4096,
        over in 0usize..2,
        cuts in prop::collection::vec(0.0f64..1.0, 0..6),
    ) {
        let request = br#"{"id":7,"method":"mining.get_health"}"#;
        let mut frame = request.to_vec();
        frame.resize(max - 1 + over, b' ');
        frame.push(b'\n');
        let stream = [frame.as_slice(), br#"{"id":8,"method":"mining.extranonce.subscribe"}"#, b"\n"].concat();
        let config = StratumConfig { max_message_bytes: max, ..StratumConfig::default() };
        let lines = converse(config, fragment(&stream, &cuts));
        if over == 0 {
            prop_assert_eq!(lines.len(), 2);
            prop_assert_eq!(&lines[0]["id"], &json!(7));
            prop_assert_eq!(&lines[1]["id"], &json!(8));
        } else {
            prop_assert_eq!(lines.len(), 1);
            prop_assert_eq!(&lines[0]["id"], &Value::Null);
            prop_assert_eq!(&lines[0]["error"][1], &json!("Stratum message exceeds size limit"));
        }
    }
}

/// The largest target a job can use, 2^256 - 1.
fn max_target() -> BigUint {
    (BigUint::one() << 256usize) - 1u8
}

fn difficulty_one() -> BigUint {
    codec::target_from_compact(0x1d00ffff).unwrap()
}

/// Bitcoin's `GetCompact`, written independently of the codec's decoder.
fn compact(target: &BigUint) -> u32 {
    let bytes = target.to_bytes_be();
    let mut size = bytes.len() as u32;
    let mut mantissa = if size <= 3 {
        (target << (8 * (3 - size) as usize)).to_u32().unwrap()
    } else {
        u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]])
    };
    if mantissa & 0x0080_0000 != 0 {
        mantissa >>= 8;
        size += 1;
    }
    (size << 24) | mantissa
}

/// A finite positive f64 as its exact binary rational `mantissa * 2^exponent`.
fn rational(value: f64) -> (u64, i32) {
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1 << 52) - 1);
    if exponent == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1 << 52), exponent - 1075)
    }
}

/// `left * 2^exponent`, in exact integers, compared against `right`.
fn scaled_le(left: &BigUint, mantissa: u64, exponent: i32, right: &BigUint) -> bool {
    let product = left * BigUint::from(mantissa);
    if exponent >= 0 {
        (product << exponent as usize) <= *right
    } else {
        product <= (right << (-exponent) as usize)
    }
}

fn positive_difficulty() -> impl Strategy<Value = f64> {
    prop_oneof![
        (-1074i32..1024, 1.0f64..2.0).prop_map(|(e, m)| m * 2f64.powi(e)),
        any::<f64>().prop_filter("positive finite", |d| d.is_finite() && *d > 0.0),
        (1u64..(1 << 53)).prop_map(|n| n as f64),
    ]
    .prop_filter("positive finite", |d| d.is_finite() && *d > 0.0)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

    /// `difficulty_target` is exact floor division of the difficulty-1 target
    /// by the difficulty's exact binary value, clamped to [1, 2^256 - 1].
    #[test]
    fn difficulty_target_is_the_exact_clamped_floor_quotient(d in positive_difficulty()) {
        let target = codec::difficulty_target(d).unwrap();
        prop_assert!(target >= BigUint::one() && target <= max_target());
        let (mantissa, exponent) = rational(d);
        let one = difficulty_one();
        if target > BigUint::one() && target < max_target() {
            // target * d <= T1 < (target + 1) * d
            prop_assert!(scaled_le(&target, mantissa, exponent, &one));
            prop_assert!(!scaled_le(&(&target + 1u8), mantissa, exponent, &one));
        } else if target == max_target() {
            prop_assert!(scaled_le(&target, mantissa, exponent, &one));
        } else {
            prop_assert!(!scaled_le(&BigUint::from(2u8), mantissa, exponent, &one));
        }
    }

    /// A higher difficulty never yields a larger target, and an integer
    /// difficulty divides the difficulty-1 target exactly.
    #[test]
    fn difficulty_target_is_monotone_and_exact_on_integers(
        a in positive_difficulty(),
        b in positive_difficulty(),
        n in 1u64..(1 << 53),
    ) {
        let (low, high) = if a <= b { (a, b) } else { (b, a) };
        prop_assert!(codec::difficulty_target(low).unwrap() >= codec::difficulty_target(high).unwrap());
        prop_assert_eq!(
            codec::difficulty_target(n as f64).unwrap(),
            (difficulty_one() / BigUint::from(n)).max(BigUint::one())
        );
    }

    /// Where no clamp applies, difficulty -> target -> difficulty returns the
    /// difficulty to within f64 rounding.
    #[test]
    fn difficulty_survives_a_target_round_trip(exponent in -30i32..60, fraction in 1.0f64..2.0) {
        let d = fraction * 2f64.powi(exponent);
        let back = codec::target_difficulty(&codec::difficulty_target(d).unwrap()).unwrap();
        prop_assert!(((back - d) / d).abs() < 1e-12, "{} came back as {}", d, back);
    }

    /// A decoded compact target re-encodes canonically and decodes to itself;
    /// a canonical encoding decodes and re-encodes to itself.
    #[test]
    fn compact_targets_round_trip(bits in any::<u32>(), size in 3u32..=32, mantissa in 0x01_0000u32..0x80_0000) {
        let (exponent, fraction) = (bits >> 24, bits & 0x007f_ffff);
        // Negative, zero, or too wide to be a 256-bit target.
        let refusable = bits & 0x0080_0000 != 0 || fraction == 0 || exponent > 34 || {
            let value = if exponent <= 3 {
                BigUint::from(fraction >> (8 * (3 - exponent)))
            } else {
                BigUint::from(fraction) << (8 * (exponent - 3)) as usize
            };
            value.is_zero() || value.bits() > 256
        };
        match codec::target_from_compact(bits) {
            Ok(target) => {
                prop_assert!(!refusable, "decoded {:08x}", bits);
                prop_assert!(!target.is_zero() && target <= max_target());
                prop_assert_eq!(codec::target_from_compact(compact(&target)).unwrap(), target);
            }
            Err(_) => prop_assert!(refusable, "refused {:08x}", bits),
        }
        let canonical = (size << 24) | mantissa;
        prop_assert_eq!(compact(&codec::target_from_compact(canonical).unwrap()), canonical);
    }

    /// The scaled difficulty is the floored regtest ratio, never below one.
    #[test]
    fn scaled_difficulty_is_the_floored_regtest_ratio(d in positive_difficulty()) {
        let target = codec::difficulty_target(d).unwrap();
        let regtest = codec::target_from_compact(0x207fffff).unwrap();
        let expected = ((regtest * BigUint::from(1_000_000u64)) / &target).max(BigUint::one());
        match codec::scaled_target_difficulty(&target) {
            Ok(scaled) => prop_assert_eq!(BigUint::from(scaled), expected),
            Err(_) => prop_assert!(expected.bits() > 128),
        }
    }
}

/// A template with `count` pre-segwit transactions, and its manifest.
fn work(
    extranonce1: &str,
    extranonce2_size: usize,
    count: u8,
    bits: u32,
) -> (Value, qbit_pool_builder::PayoutManifest, Vec<Vec<u8>>) {
    let transactions: Vec<Vec<u8>> = (0..count)
        .map(|tag| {
            let mut tx = vec![1, 0, 0, 0, 1];
            tx.extend([tag; 32]);
            tx.extend(0u32.to_le_bytes());
            tx.extend([1, 0x51]);
            tx.extend(u32::MAX.to_le_bytes());
            tx.push(1);
            tx.extend(1_000u64.to_le_bytes());
            tx.extend([1, 0x51]);
            tx.extend(0u32.to_le_bytes());
            tx
        })
        .collect();
    let template = json!({"height":101,"coinbasevalue":5_000_000_000u64,"previousblockhash":"0123456789abcdef".repeat(4),
        "version":0x20000000u32,"bits":format!("{bits:08x}"),"curtime":1_700_000_000u32,"mintime":1_699_999_999u32,
        "transactions": transactions.iter().map(|tx| json!({"data": hex::encode(tx)})).collect::<Vec<_>>()});
    let manifest = build_manifest(CoinbaseBuildRequest {
        block_height: 101,
        coinbase_value_sats: 5_000_000_000,
        entitlements: vec![WeightedEntitlement {
            recipient_id: "miner".into(),
            order_key: "miner".into(),
            p2mr_program_hex: "ab".repeat(32),
            weight: 1,
        }],
        witness_nonce_hex: Some("00".repeat(32)),
        witness_merkle_leaves_hex: codec::witness_merkle_leaves_hex(&transactions),
        coinbase_script_sig_suffix_hex: Some(format!(
            "{extranonce1}{}",
            "00".repeat(extranonce2_size)
        )),
        pinned_first_output: None,
    })
    .unwrap();
    (template, manifest, transactions)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, failure_persistence: None, ..ProptestConfig::default() })]

    /// A submission decodes back to exactly the fields the miner sent: the
    /// header carries the version, previous hash, merkle root of the rebuilt
    /// coinbase, time, bits and nonce; the coinbase is the job's halves around
    /// the two extranonces; the pass flags are the hash against the targets;
    /// and a block carries the template's transactions.
    #[test]
    fn a_submission_round_trips_its_fields(
        extranonce1 in "[0-9a-f]{8}",
        extranonce2 in prop::collection::vec(any::<u8>(), 1..=16),
        count in 0u8..4,
        ntime in 1_699_999_999u32..,
        nonce in any::<u32>(),
        rolled in any::<u32>(),
        difficulty in 1e-9f64..4.0,
        minimum in prop_oneof![Just(0.0), 1e-9f64..8.0],
        mainnet in any::<bool>(),
    ) {
        let bits = if mainnet { 0x1d00ffff } else { 0x207fffff };
        let (template, manifest, transactions) = work(&extranonce1, extranonce2.len(), count, bits);
        let job = Job::from_manifest("job".into(), &template, &manifest, &extranonce1,
            extranonce2.len(), difficulty, minimum, true).unwrap();
        // The share target: the difficulty's, no harder than the network's,
        // then no easier than the floor's.
        let mut target = codec::difficulty_target(difficulty).unwrap().max(job.network_target.clone());
        if minimum > 0.0 {
            target = target.min(codec::difficulty_target(minimum).unwrap());
        }
        prop_assert_eq!(&job.share_target, &target);
        prop_assert_eq!(job.share_difficulty, codec::target_difficulty(&target).unwrap());

        let mask = codec::VERSION_ROLLING_MASK;
        let bits_rolled = rolled & mask;
        let en2 = hex::encode(&extranonce2);
        let submission = job.assemble_submission(&en2.to_ascii_uppercase(), &format!("{ntime:08x}"),
            &format!("{nonce:08X}"), Some(&format!("{bits_rolled:08x}")), mask).unwrap();
        let header = hex::decode(&submission.header_hex).unwrap();
        prop_assert_eq!(header.len(), 80);
        let version = (0x20000000u32 & !mask) | bits_rolled;
        prop_assert_eq!(&header[..4], &version.to_le_bytes());
        prop_assert_eq!(submission.applied_version, version);
        let mut previous = hex::decode(template["previousblockhash"].as_str().unwrap()).unwrap();
        previous.reverse();
        prop_assert_eq!(&header[4..36], previous.as_slice());
        let preimage = hex::decode(format!("{}{extranonce1}{en2}{}", job.coinb1, job.coinb2)).unwrap();
        let mut root = codec::double_sha256(&preimage);
        for sibling in &codec::merkle_branch_for_coinbase(&transactions).unwrap() {
            root = codec::double_sha256(&[root.as_slice(), sibling.as_slice()].concat());
        }
        prop_assert_eq!(&header[36..68], root.as_slice());
        prop_assert_eq!(&header[68..72], &ntime.to_le_bytes());
        prop_assert_eq!(&header[72..76], &bits.to_le_bytes());
        prop_assert_eq!(&header[76..], &nonce.to_le_bytes());
        prop_assert_eq!((submission.ntime, submission.nonce), (ntime, nonce));
        prop_assert_eq!(&submission.extranonce2_hex, &en2);

        let coinbase = hex::decode(&submission.coinbase_tx_hex).unwrap();
        prop_assert_eq!(&submission.coinbase_tx_hex,
            &format!("{}{extranonce1}{en2}{}", job.full_coinbase_prefix, job.full_coinbase_suffix));
        prop_assert_eq!(codec::strip_witness_transaction(&coinbase).unwrap(), preimage);

        let hash = codec::double_sha256(&header);
        prop_assert_eq!(&submission.block_hash_hex, &codec::hash_display(&hash));
        let value = BigUint::from_bytes_le(&hash);
        prop_assert_eq!(submission.share_pass, value <= job.share_target);
        prop_assert_eq!(submission.block_pass, value <= job.network_target);
        if submission.block_pass {
            let block = hex::decode(&submission.block_hex).unwrap();
            prop_assert_eq!(&block[..80], header.as_slice());
            prop_assert_eq!(codec::witness_merkle_leaves_from_block(&block).unwrap(),
                codec::witness_merkle_leaves_hex(&transactions));
        } else {
            prop_assert!(submission.block_hex.is_empty());
        }

        // The notification a miner builds this header from says the same.
        let notify = job.notify();
        let params = notify["params"].as_array().unwrap();
        let mut prevhash = hex::decode(params[1].as_str().unwrap()).unwrap();
        for word in prevhash.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        prop_assert_eq!(prevhash, previous);
        prop_assert_eq!(codec::parse_u32_hex(params[5].as_str().unwrap()).unwrap(), 0x20000000);
        prop_assert_eq!(codec::parse_u32_hex(params[6].as_str().unwrap()).unwrap(), bits);
        prop_assert_eq!(params[4].as_array().unwrap().len(), codec::merkle_branch_for_coinbase(&transactions).unwrap().len());
    }
}
