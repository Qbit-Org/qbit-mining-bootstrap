//! External-target mode (`qbit-prism-load external`), database-free.
//!
//! The target here is a small Stratum server of the test's own that checks
//! every submit's proof of work against the difficulty it advertised for that
//! submit's job, as the server does, so a client that mined a job at any
//! other difficulty would be refused `low-difficulty`. The gated run against
//! the in-repo frontend is `tests/external_frontend.rs`.

use anyhow::Result;
use clap::Parser;
use qbit_prism_load::{
    client,
    external::{self, histogram::LogHistogram, stats, ExternalArgs, Shutdown},
    run,
};
use qbit_prism_server::codec;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::watch;

const PREVHASH: &str = "00000000000000000001aabbccddeeff00112233445566778899aabbccddeeff";
const VERSION: u32 = 0x2000_0000;
/// Network difficulty 1: one hash in 2^32 solves a block, so no share at the
/// difficulties below is ever stepped over as a block solution.
const NBITS: u32 = 0x1d00_ffff;
const NTIME: u32 = 0x6b49_d200;
/// About 256 hashes a share: microseconds even in a debug build.
const EASY: f64 = 1.0 / 16_777_216.0;
/// About 4,096 hashes a share.
const HARDER: f64 = 1.0 / 1_048_576.0;

/// What the target saw.
#[derive(Default)]
struct Seen {
    /// While set, every new connection is closed at once.
    refusing: std::sync::atomic::AtomicBool,
    connections: AtomicUsize,
    submits: AtomicUsize,
    accepted: Mutex<HashSet<[u8; 32]>>,
    refused: Mutex<Vec<String>>,
}

struct FakeTarget {
    address: String,
    seen: Arc<Seen>,
    /// Bumping it closes every connection accepted before the bump.
    drops: watch::Sender<u64>,
    /// Changing it sends every authorized connection the new difficulty and
    /// then a new job with `clean_jobs`.
    difficulty: watch::Sender<f64>,
    /// The accept loop: aborting it closes the listener.
    listener: tokio::task::JoinHandle<()>,
}

async fn fake_target(difficulty: f64) -> FakeTarget {
    fake_target_admitting(difficulty, usize::MAX).await
}

/// A target that serves its first `admitted` connections and closes every
/// later one at once, as a frontend at its connection cap does.
async fn fake_target_admitting(difficulty: f64, admitted: usize) -> FakeTarget {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let seen = Arc::new(Seen::default());
    let (drops, _) = watch::channel(0u64);
    let (difficulty, _) = watch::channel(difficulty);
    let task = {
        let seen = seen.clone();
        let drops = drops.clone();
        let difficulty = difficulty.clone();
        tokio::spawn(async move {
            let extranonce = AtomicUsize::new(1);
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                if seen.connections.fetch_add(1, Ordering::SeqCst) >= admitted
                    || seen.refusing.load(Ordering::SeqCst)
                {
                    drop(socket);
                    continue;
                }
                let extranonce1 = (extranonce.fetch_add(1, Ordering::SeqCst) as u32).to_be_bytes();
                let generation = *drops.borrow();
                tokio::spawn(serve(
                    socket,
                    extranonce1,
                    seen.clone(),
                    generation,
                    drops.subscribe(),
                    difficulty.subscribe(),
                ));
            }
        })
    };
    FakeTarget {
        address,
        seen,
        drops,
        difficulty,
        listener: task,
    }
}

fn coinbase() -> (Vec<u8>, Vec<u8>) {
    (vec![0x01; 42], vec![0x02; 60])
}

fn line(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    bytes
}

async fn serve(
    socket: tokio::net::TcpStream,
    extranonce1: [u8; 4],
    seen: Arc<Seen>,
    generation: u64,
    mut drops: watch::Receiver<u64>,
    mut difficulty: watch::Receiver<f64>,
) {
    let (read, mut write) = socket.into_split();
    let mut lines = BufReader::new(read).lines();
    let (coinb1, coinb2) = coinbase();
    let header_prev = client::header_prev_from_wire(PREVHASH).unwrap();
    let network = codec::target_from_compact(NBITS).unwrap();
    // Each job keeps the difficulty it was announced with.
    let mut jobs: HashMap<String, f64> = HashMap::new();
    let mut authorized = false;
    let mut next_job = 0u64;
    let mut announce = |jobs: &mut HashMap<String, f64>, difficulty: f64| -> Vec<u8> {
        next_job += 1;
        let job_id = format!("job-{next_job}");
        jobs.insert(job_id.clone(), difficulty);
        let mut bytes = line(&json!({"id": null, "method": "mining.set_difficulty",
                                     "params": [difficulty]}));
        bytes.extend(line(
            &json!({"id": null, "method": "mining.notify", "params": [
                job_id, PREVHASH, hex::encode(&coinb1), hex::encode(&coinb2), [],
                format!("{VERSION:08x}"), format!("{NBITS:08x}"), format!("{NTIME:08x}"), true
            ]}),
        ));
        bytes
    };
    loop {
        let request: Value = tokio::select! {
            next = lines.next_line() => match next {
                Ok(Some(text)) => serde_json::from_str(&text).unwrap(),
                _ => return,
            },
            _ = drops.changed() => {
                if *drops.borrow() != generation {
                    return;
                }
                continue;
            }
            changed = difficulty.changed(), if authorized => {
                if changed.is_err() {
                    return;
                }
                let current = *difficulty.borrow_and_update();
                let bytes = announce(&mut jobs, current);
                if write.write_all(&bytes).await.is_err() {
                    return;
                }
                continue;
            }
        };
        let id = request["id"].clone();
        let mut reply = match request["method"].as_str() {
            Some("mining.subscribe") => line(&json!({"id": id, "error": null, "result": [
                [["mining.notify", "1"]], hex::encode(extranonce1), 8
            ]})),
            Some("mining.configure") => line(&json!({"id": id, "error": null,
                                                     "result": {"version-rolling": false}})),
            Some("mining.authorize") => {
                authorized = true;
                let current = *difficulty.borrow_and_update();
                let mut bytes = line(&json!({"id": id, "error": null, "result": true}));
                bytes.extend(announce(&mut jobs, current));
                bytes
            }
            Some("mining.submit") => {
                seen.submits.fetch_add(1, Ordering::SeqCst);
                let fields: Vec<&str> = request["params"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap())
                    .collect();
                let verdict = match jobs.get(fields[1]) {
                    None => Err((21, "stale job", "unknown-job")),
                    Some(job_difficulty) => {
                        let extranonce2 = hex::decode(fields[2]).unwrap();
                        let merkle =
                            client::merkle_root(&coinb1, &extranonce1, &extranonce2, &coinb2, &[]);
                        let header = client::assemble_header(
                            VERSION,
                            &header_prev,
                            &merkle,
                            codec::parse_u32_hex(fields[3]).unwrap(),
                            NBITS,
                            codec::parse_u32_hex(fields[4]).unwrap(),
                        );
                        let hash = codec::double_sha256(&header);
                        let target = client::target_bytes_le(
                            &codec::difficulty_target(*job_difficulty)
                                .unwrap()
                                .max(network.clone()),
                        );
                        if !client::le_at_most(&hash, &target) {
                            Err((23, "low difficulty share", "low-difficulty"))
                        } else if !seen.accepted.lock().unwrap().insert(hash) {
                            Err((22, "duplicate share", "duplicate-share"))
                        } else {
                            Ok(())
                        }
                    }
                };
                match verdict {
                    Ok(()) => line(&json!({"id": id, "error": null, "result": true})),
                    Err((code, message, reason_id)) => {
                        seen.refused.lock().unwrap().push(reason_id.to_owned());
                        line(&json!({"id": id, "result": null,
                                     "error": [code, message, {"reason_id": reason_id}]}))
                    }
                }
            }
            _ => continue,
        };
        if write.write_all(&reply).await.is_err() {
            return;
        }
        reply.clear();
    }
}

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "prism-external-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn args(target: &str, out: &Path, extra: &[&str]) -> ExternalArgs {
    let out = out.display().to_string();
    let mut argv = vec![
        "qbit-prism-load external",
        "--target",
        target,
        "--address",
        "pload1external",
        "--out",
        &out,
        "--progress-seconds",
        "0",
        "--work-timeout-seconds",
        "20",
        "--drain-seconds",
        "5",
    ];
    argv.extend_from_slice(extra);
    ExternalArgs::try_parse_from(argv).unwrap()
}

fn read(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

/// Without the confirmation flag nothing is sent: the mode refuses before a
/// session exists, and says which flag confirms and why.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_guard_flag_nothing_is_sent() -> Result<()> {
    let target = fake_target(EASY).await;
    let scratch = Scratch::new("guard");
    let args = args(&target.address, &scratch.path("stats.json"), &[]);
    assert!(!args.i_understand_external_target);
    let error = match external::run(&args, &Shutdown::never()).await {
        Ok(_) => panic!("the run went ahead without {}", external::GUARD_FLAG),
        Err(error) => format!("{error:#}"),
    };
    assert!(error.contains(external::GUARD_FLAG), "{error}");
    assert!(error.contains("never at a pool serving miners"), "{error}");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(target.seen.connections.load(Ordering::SeqCst), 0);
    assert!(!scratch.path("stats.json").exists());
    Ok(())
}

/// EP-VALIDATION: each refusal names its flag.
#[test]
fn the_command_line_is_checked_at_entry() {
    let refused = |extra: &[&str], needle: &str| {
        let mut argv = vec![
            "qbit-prism-load external",
            "--target",
            "127.0.0.1:3333",
            "--address",
            "pload1external",
            external::GUARD_FLAG,
        ];
        argv.extend_from_slice(extra);
        let args = ExternalArgs::try_parse_from(argv).unwrap();
        let error = format!("{:#}", args.validate().unwrap_err());
        assert!(error.contains(needle), "{extra:?}: {error}");
    };
    refused(&["--max-difficulty", "0.001"], "--max-difficulty");
    refused(&["--difficulty", "0.0001"], "--difficulty");
    refused(&["--sessions", "0"], "--sessions");
    refused(&["--rate", "0"], "--rate");
    refused(&["--rate", "NaN"], "--rate");
    refused(&["--rate", "200000"], "--rate");
    refused(&["--duration-seconds", "0"], "--duration-seconds");
    refused(&["--worker-prefix", "a.b"], "--worker-prefix");
    refused(&["--work-timeout-seconds", "0"], "--work-timeout-seconds");
    let mut argv = vec![
        "qbit-prism-load external",
        "--address",
        "pload1.external",
        "--target",
        "127.0.0.1:3333",
        external::GUARD_FLAG,
    ];
    let error = format!(
        "{:#}",
        ExternalArgs::try_parse_from(&argv)
            .unwrap()
            .validate()
            .unwrap_err()
    );
    assert!(error.contains("--address"), "{error}");
    argv[2] = "pload1external";
    argv[4] = "no-port";
    let error = format!(
        "{:#}",
        ExternalArgs::try_parse_from(&argv)
            .unwrap()
            .validate()
            .unwrap_err()
    );
    assert!(error.contains("host:port"), "{error}");
    argv[4] = "127.0.0.1:3333";
    ExternalArgs::try_parse_from(&argv)
        .unwrap()
        .validate()
        .unwrap();
}

/// The subcommand is recognised only as the first word, so a harness command
/// line is never read as one.
#[test]
fn only_the_first_word_selects_the_mode() {
    let line = |words: &[&str]| -> Vec<std::ffi::OsString> {
        words.iter().map(std::ffi::OsString::from).collect()
    };
    assert_eq!(
        external::Command::of(&line(&["qbit-prism-load", "external", "--target", "x:1"])),
        Some(external::Command::Run)
    );
    assert_eq!(
        external::Command::of(&line(&["qbit-prism-load", "external-merge", "a.json"])),
        Some(external::Command::Merge)
    );
    assert_eq!(
        external::Command::of(&line(&["qbit-prism-load", "--plan", "external"])),
        None
    );
    assert_eq!(external::Command::of(&line(&["qbit-prism-load"])), None);
}

/// Two histograms merged are the histogram of both samples, bucket for
/// bucket, and a percentile is never below the sample it stands for nor more
/// than one bucket width above it.
#[test]
fn histograms_merge_exactly_and_bound_their_percentiles() {
    let mut rng = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % 5_000_000
    };
    let (mut first, mut second, mut both) = (
        LogHistogram::default(),
        LogHistogram::default(),
        LogHistogram::default(),
    );
    let mut values = Vec::new();
    for index in 0..20_000 {
        let value = next();
        values.push(value);
        both.record_micros(value);
        if index % 3 == 0 {
            first.record_micros(value);
        } else {
            second.record_micros(value);
        }
    }
    first.merge(&second).unwrap();
    assert_eq!(first, both);
    values.sort_unstable();
    for q in [0.5, 0.9, 0.99, 0.999] {
        let rank = ((q * values.len() as f64).ceil() as usize).max(1);
        let exact = values[rank - 1];
        let reported = both.quantile_micros(q).unwrap();
        assert!(reported >= exact, "q {q}: {reported} < {exact}");
        assert!(
            reported as f64 <= exact as f64 * (1.0 + external::histogram::RELATIVE_PRECISION) + 1.0,
            "q {q}: {reported} is more than a bucket above {exact}"
        );
    }
    assert_eq!(both.quantile_micros(1.0), values.last().copied());
    // Round trip, and a histogram from another precision is refused.
    let wire = serde_json::to_value(&both).unwrap();
    assert_eq!(
        serde_json::from_value::<LogHistogram>(wire.clone()).unwrap(),
        both
    );
    let mut other = wire.clone();
    other["significant_bits"] = json!(7);
    assert!(serde_json::from_value::<LogHistogram>(other).is_err());
    // Extremes that cannot be its samples are refused, not merged into a
    // summary that would panic on them.
    for (min, max) in [(json!(4_000_000), json!(1)), (json!(0), json!(9_000_000))] {
        let mut broken = wire.clone();
        broken["min"] = min;
        broken["max"] = max;
        assert!(serde_json::from_value::<LogHistogram>(broken).is_err());
    }
    // So is a sum no set of its samples could have.
    let mut broken = wire.clone();
    broken["sum"] = json!(0);
    assert!(serde_json::from_value::<LogHistogram>(broken).is_err());
    // And buckets whose counts overflow as they are added up.
    let mut broken = wire.clone();
    broken["buckets"] = json!([[100, u64::MAX], [100, 1]]);
    broken["count"] = json!(0);
    assert!(serde_json::from_value::<LogHistogram>(broken).is_err());
    // A sum that leaves no room for both extremes, and an empty histogram
    // with a sum.
    let both = json!({"unit": "microseconds", "significant_bits": 10, "count": 2,
                      "sum": 200, "min": 100, "max": 101, "buckets": [[100, 1], [101, 1]]});
    assert!(serde_json::from_value::<LogHistogram>(both).is_err());
    let fits = json!({"unit": "microseconds", "significant_bits": 10, "count": 2,
                      "sum": 201, "min": 100, "max": 101, "buckets": [[100, 1], [101, 1]]});
    assert!(serde_json::from_value::<LogHistogram>(fits).is_ok());
    let none = json!({"unit": "microseconds", "significant_bits": 10, "count": 0,
                      "sum": 5, "min": null, "max": null, "buckets": []});
    assert!(serde_json::from_value::<LogHistogram>(none).is_err());
    // A sum no samples in their buckets could have, though the extremes
    // allow it.
    let middle = json!({"unit": "microseconds", "significant_bits": 10, "count": 102,
                        "sum": 200, "min": 0, "max": 200,
                        "buckets": [[0, 1], [100, 100], [200, 1]]});
    assert!(serde_json::from_value::<LogHistogram>(middle).is_err());
    let middle = json!({"unit": "microseconds", "significant_bits": 10, "count": 102,
                        "sum": 10200, "min": 0, "max": 200,
                        "buckets": [[0, 1], [100, 100], [200, 1]]});
    assert!(serde_json::from_value::<LogHistogram>(middle).is_ok());
    // And an empty bucket, which would let an extreme no sample has pass.
    let empty = json!({"unit": "microseconds", "significant_bits": 10, "count": 1,
                       "sum": 100, "min": 0, "max": 100, "buckets": [[0, 0], [100, 1]]});
    assert!(serde_json::from_value::<LogHistogram>(empty).is_err());
    let empty = LogHistogram::default().summary("test");
    assert_eq!(empty["samples"], json!(0));
    assert!(empty["p99"].is_null());
    assert!(empty["unavailable_reason"].is_string());
}

fn sum_timeline(document: &Value, field: &str) -> u64 {
    document["totals"]["timeline"]
        .as_array()
        .unwrap()
        .iter()
        .map(|second| second[field].as_u64().unwrap())
        .sum()
}

async fn wait_until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The whole mode against a target that verifies every share: the shares are
/// mined at the difficulty the target advertised for their job, including
/// after it raises the difficulty mid-run, every acknowledgement the client
/// counts is one the target gave, a dropped connection is counted and
/// reconnected, and the counts reconcile with each other, the timeline and
/// the share log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_mines_the_advertised_difficulty_and_counts_what_the_target_did() -> Result<()> {
    let target = fake_target(EASY).await;
    let scratch = Scratch::new("run");
    let out = scratch.path("stats.json");
    let log = scratch.path("shares.jsonl");
    let log_arg = log.display().to_string();
    let args = args(
        &target.address,
        &out,
        &[
            external::GUARD_FLAG,
            "--sessions",
            "4",
            "--rate",
            "40",
            "--duration-seconds",
            "6",
            "--label",
            "vm-a",
            "--worker-prefix",
            "vma",
            "--share-log",
            &log_arg,
        ],
    );
    let run = tokio::spawn(async move { external::run(&args, &Shutdown::never()).await });
    let seen = target.seen.clone();
    wait_until("load reaches the target", Duration::from_secs(30), || {
        seen.submits.load(Ordering::SeqCst) >= 20
    })
    .await;
    // Every connection goes at once, as a frontend that dies takes them.
    target.drops.send_modify(|generation| *generation += 1);
    wait_until("the sessions reconnect", Duration::from_secs(30), || {
        seen.connections.load(Ordering::SeqCst) >= 8
    })
    .await;
    let before = seen.submits.load(Ordering::SeqCst);
    wait_until("load resumes", Duration::from_secs(30), || {
        seen.submits.load(Ordering::SeqCst) >= before + 20
    })
    .await;
    // Sixteen times harder: a session that kept mining at the old
    // difficulty would be refused low-difficulty.
    target.difficulty.send_replace(HARDER);
    let outcome = run.await??;
    assert_eq!(outcome.exit_code, run::EXIT_OK);
    let document = read(&out);
    assert_eq!(document, outcome.document);
    assert_eq!(document["schema"], json!(stats::SCHEMA));
    let summary = &document["summary"];
    let accepted = summary["shares"]["accepted"].as_u64().unwrap();
    assert!(accepted >= 40, "{summary:#}");
    assert_eq!(summary["shares"]["rejected"], json!(0), "{summary:#}");
    assert!(target.seen.refused.lock().unwrap().is_empty());
    // Every acknowledgement counted is one the target gave.
    assert_eq!(
        target.seen.accepted.lock().unwrap().len() as u64,
        accepted,
        "{summary:#}"
    );
    assert_eq!(
        summary["reconnects"]["disconnects"],
        json!(4),
        "{summary:#}"
    );
    assert_eq!(summary["reconnects"]["completed"], json!(4), "{summary:#}");
    assert_eq!(summary["reconnects"]["outage"]["samples"], json!(4));
    assert_eq!(summary["connections"]["initial"], json!(4));
    assert_eq!(
        summary["connections"]["time_to_first_job"]["samples"],
        json!(4)
    );
    assert_eq!(summary["difficulty"]["advertised_min"], json!(EASY));
    assert_eq!(summary["difficulty"]["advertised_max"], json!(HARDER));
    assert_eq!(summary["difficulty"]["offers_above_ceiling"], json!(0));
    assert_eq!(summary["offers"]["unaccounted"], json!(0), "{summary:#}");
    assert_eq!(summary["ack_latency"]["samples"], json!(accepted));
    assert!(summary["ack_latency"]["p99"].as_f64().unwrap() > 0.0);
    assert!(summary["rates"]["accepted_per_second"].as_f64().unwrap() > 0.0);
    assert_eq!(summary["jobs"]["notifies"], json!(12), "{summary:#}");
    assert_eq!(summary["difficulty"]["advertisements"], json!(12));
    // The timeline holds the same counts by second.
    assert_eq!(sum_timeline(&document, "accepted"), accepted);
    assert_eq!(
        sum_timeline(&document, "offered"),
        summary["offers"]["minted"].as_u64().unwrap()
    );
    assert_eq!(sum_timeline(&document, "disconnects"), 4);
    let process = &document["processes"][0];
    assert_eq!(process["label"], json!("vm-a"));
    assert_eq!(process["ended"], json!("completed"));
    assert_eq!(process["sessions_holding_work_at_start"], json!(4));
    assert!(process["window"]["seconds"].as_f64().unwrap() >= 6.0);
    // One line per submit sent, each acknowledged id once.
    let lines: Vec<Value> = std::fs::read_to_string(&log)?
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        lines.len() as u64,
        summary["shares"]["sent"].as_u64().unwrap()
    );
    assert_eq!(process["share_log"]["lines"], json!(lines.len()));
    assert_eq!(process["share_log"]["dropped"], json!(0));
    let ids: HashSet<&str> = lines
        .iter()
        .filter(|line| line["outcome"] == json!("accepted"))
        .map(|line| line["share_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len() as u64, accepted);
    assert!(ids
        .iter()
        .all(|id| id.starts_with("pload1external.vma-s0000")));
    Ok(())
}

/// A job above the ceiling is never mined: no submit reaches the target,
/// each offer on it is counted, and the counts still reconcile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_job_above_the_ceiling_is_counted_and_not_mined() -> Result<()> {
    let target = fake_target(1.0 / 1024.0).await;
    let scratch = Scratch::new("ceiling");
    let out = scratch.path("stats.json");
    let args = args(
        &target.address,
        &out,
        &[
            external::GUARD_FLAG,
            "--sessions",
            "2",
            "--rate",
            "20",
            "--duration-seconds",
            "2",
        ],
    );
    let outcome = external::run(&args, &Shutdown::never()).await?;
    assert_eq!(outcome.exit_code, run::EXIT_OK);
    let summary = &outcome.document["summary"];
    assert_eq!(target.seen.submits.load(Ordering::SeqCst), 0);
    assert_eq!(summary["shares"]["sent"], json!(0));
    let above = summary["difficulty"]["offers_above_ceiling"]
        .as_u64()
        .unwrap();
    assert!(above > 0, "{summary:#}");
    assert_eq!(summary["offers"]["above_difficulty_ceiling"], json!(above));
    assert_eq!(summary["client_failures"]["offer"]["count"], json!(above));
    assert_eq!(summary["offers"]["unaccounted"], json!(0), "{summary:#}");
    let sample = summary["client_failures"]["offer"]["samples"][0]
        .as_str()
        .unwrap();
    assert!(
        sample.starts_with(client::DIFFICULTY_ABOVE_CEILING),
        "{sample}"
    );
    // A document full of offer failures is whole, and merges.
    let merged = external::merge_files(&[out])?;
    assert_eq!(merged["summary"]["offers"]["unaccounted"], json!(0));
    Ok(())
}

/// One process per machine: two processes' documents add into one whose
/// counts, histograms and timeline are the sums, and a document given twice
/// is refused rather than counted twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn documents_from_two_processes_merge_into_their_sum() -> Result<()> {
    let target = fake_target(EASY).await;
    let scratch = Scratch::new("merge");
    let (a, b) = (scratch.path("a.json"), scratch.path("b.json"));
    let run = |out: &Path, label: &'static str| {
        let args = args(
            &target.address,
            out,
            &[
                external::GUARD_FLAG,
                "--sessions",
                "3",
                "--rate",
                "30",
                "--duration-seconds",
                "3",
                "--label",
                label,
                "--worker-prefix",
                label,
            ],
        );
        async move { external::run(&args, &Shutdown::never()).await }
    };
    let (first, second) = tokio::join!(run(&a, "vm-a"), run(&b, "vm-b"));
    let (first, second) = (first?, second?);
    let merged = external::merge_files(&[a.clone(), b.clone()])?;
    assert_eq!(merged["kind"], json!("merged"));
    assert_eq!(merged["processes"].as_array().unwrap().len(), 2);
    let total = |document: &Value, key: &str| document["totals"][key].as_u64().unwrap();
    for key in [
        "accepted",
        "offers_minted",
        "offers_dispatched",
        "connections_opened",
    ] {
        assert_eq!(
            total(&merged, key),
            total(&first.document, key) + total(&second.document, key),
            "{key}"
        );
    }
    let mut ack: LogHistogram =
        serde_json::from_value(first.document["totals"]["ack_latency"].clone())?;
    ack.merge(&serde_json::from_value(
        second.document["totals"]["ack_latency"].clone(),
    )?)?;
    assert_eq!(
        serde_json::from_value::<LogHistogram>(merged["totals"]["ack_latency"].clone())?,
        ack
    );
    assert_eq!(
        sum_timeline(&merged, "accepted"),
        total(&merged, "accepted")
    );
    assert_eq!(merged["summary"]["sessions"], json!(6));
    assert_eq!(merged["summary"]["offered_rate"], json!(60.0));
    let rate = merged["summary"]["rates"]["accepted_per_second"]
        .as_f64()
        .unwrap();
    let own = |document: &Value| {
        document["summary"]["rates"]["accepted_per_second"]
            .as_f64()
            .unwrap()
    };
    assert!((rate - own(&first.document) - own(&second.document)).abs() < 1e-9);
    // A merged document merges again, and nothing is counted twice.
    let path = scratch.path("merged.json");
    external::write_document(&path, &merged)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path.clone(), a.clone()]).unwrap_err()
    );
    assert!(error.contains("count it twice"), "{error}");
    let error = format!(
        "{:#}",
        external::merge_files(&[a.clone(), a.clone()]).unwrap_err()
    );
    assert!(error.contains("count it twice"), "{error}");
    // A damaged document is refused with the reason, not merged into
    // figures it would corrupt.
    // Two documents each whole in itself, whose counters cannot be added.
    let huge: Vec<PathBuf> = [&a, &b]
        .iter()
        .enumerate()
        .map(|(index, path)| {
            let mut huge = read(path);
            huge["totals"]["notifies"] = json!(u64::MAX);
            let out = scratch.path(&format!("huge-{index}.json"));
            external::write_document(&out, &huge).unwrap();
            out
        })
        .collect();
    let error = format!("{:#}", external::merge_files(&huge).unwrap_err());
    assert!(error.contains("overflows"), "{error}");
    // And one whose counters fit apart but not in the sums it reports, or
    // with the timeline and histograms that count the same events.
    let mut sums = read(&a);
    let rejected = sums["totals"]["rejected"].as_u64().unwrap() + 1;
    sums["totals"]["accepted"] = json!(u64::MAX);
    sums["totals"]["rejected"] = json!(rejected);
    sums["processes"][0]["accepted"] = json!(u64::MAX);
    sums["processes"][0]["rejected"] = json!(rejected);
    let path = scratch.path("sums.json");
    external::write_document(&path, &sums)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("inconsistent totals"), "{error}");
    // A count the timeline does not hold, and a rejection tally rebuilt past
    // u64::MAX from repeated triples.
    let mut timeline = read(&a);
    let accepted = timeline["totals"]["accepted"].as_u64().unwrap();
    timeline["totals"]["accepted"] = json!(accepted + 1);
    timeline["processes"][0]["accepted"] = json!(accepted + 1);
    let path = scratch.path("timeline.json");
    external::write_document(&path, &timeline)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("timeline accepted"), "{error}");
    let mut reconnects = read(&a);
    let completed = reconnects["totals"]["reconnects_completed"]
        .as_u64()
        .unwrap();
    reconnects["totals"]["timeline"][0]["reconnects"] = json!(completed + 7);
    let path = scratch.path("reconnects.json");
    external::write_document(&path, &reconnects)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("timeline reconnects"), "{error}");
    let mut triples = read(&a);
    let triple = json!({"code": 21, "reason_id": "stale-job", "message": "stale job",
                        "count": u64::MAX});
    triples["totals"]["rejections"]["by_reason"] = json!([triple.clone(), triple]);
    let path = scratch.path("triples.json");
    external::write_document(&path, &triples)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("overflows"), "{error}");
    // A document already at the tip bound with u64::MAX tips dropped: the
    // next tip's eviction cannot be counted.
    let mut full = read(&a);
    let tips: serde_json::Map<String, Value> = (0..stats::TIP_KEYS)
        .map(|index| {
            (
                format!("{index:064x}"),
                json!({"first_seen_unix_ms": 1, "last_seen_unix_ms": 2, "sessions": 1}),
            )
        })
        .collect();
    full["totals"]["tips"] = json!({"tips": tips, "dropped": u64::MAX});
    let mut newer = read(&b);
    newer["totals"]["tips"] = json!({"tips": {"f".repeat(64): {
        "first_seen_unix_ms": 3, "last_seen_unix_ms": 4, "sessions": 1}}, "dropped": 0});
    let (full_path, newer_path) = (scratch.path("full.json"), scratch.path("newer.json"));
    external::write_document(&full_path, &full)?;
    external::write_document(&newer_path, &newer)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[full_path, newer_path]).unwrap_err()
    );
    assert!(error.contains("overflows"), "{error}");
    // More offers' ends than offers dispatched.
    let mut ends = read(&a);
    ends["totals"]["offers_discarded"] =
        json!(ends["totals"]["offers_dispatched"].as_u64().unwrap() + 1);
    let path = scratch.path("ends.json");
    external::write_document(&path, &ends)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("offers were dispatched"), "{error}");
    // A window no run makes, which the summary would otherwise walk second
    // by second.
    let mut window = read(&a);
    window["processes"][0]["window"]["started_unix_ms"] = json!(0);
    window["processes"][0]["window"]["ended_unix_ms"] = json!(i64::MAX);
    let path = scratch.path("window.json");
    external::write_document(&path, &window)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("no run of"), "{error}");
    // Extremes a window's span overflows in, and a session count past what
    // a process holds.
    let mut extremes = read(&a);
    extremes["processes"][0]["window"]["started_unix_ms"] = json!(i64::MIN);
    extremes["processes"][0]["window"]["ended_unix_ms"] = json!(i64::MAX);
    extremes["processes"][0]["window"]["seconds"] = json!(1.0);
    let path = scratch.path("extremes.json");
    external::write_document(&path, &extremes)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("no run of"), "{error}");
    let mut sessions = read(&a);
    sessions["processes"][0]["sessions"] = json!(usize::MAX);
    let path = scratch.path("sessions.json");
    external::write_document(&path, &sessions)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("sessions, past"), "{error}");
    let mut minted = read(&a);
    minted["processes"][0]["offers_minted"] = json!(u64::MAX);
    let path = scratch.path("minted.json");
    external::write_document(&path, &minted)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("offers minted, more than"), "{error}");
    let mut windowless = read(&a);
    windowless["processes"][0]["window"] = Value::Null;
    let path = scratch.path("windowless.json");
    external::write_document(&path, &windowless)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("no window"), "{error}");
    let mut damaged = read(&a);
    damaged["processes"][0]["accepted"] = json!(123_456_789u64);
    let path = scratch.path("damaged-process.json");
    external::write_document(&path, &damaged)?;
    let error = format!(
        "{:#}",
        external::merge_files(&[path, b.clone()]).unwrap_err()
    );
    assert!(error.contains("processes' accepted"), "{error}");
    for (field, value, needle) in [
        ("offers_shortfall", json!(1_000_000_000u64), "do not add up"),
        (
            "client_failures",
            json!({"offer": {"count": 1, "recorded": 2, "samples": []}}),
            "recorded",
        ),
    ] {
        let mut damaged = read(&a);
        damaged["totals"][field] = value;
        let path = scratch.path("damaged.json");
        external::write_document(&path, &damaged)?;
        let error = format!(
            "{:#}",
            external::merge_files(&[path.clone(), b.clone()]).unwrap_err()
        );
        assert!(error.contains("inconsistent totals"), "{field}: {error}");
        assert!(error.contains(needle), "{field}: {error}");
    }
    Ok(())
}

/// A shutdown ends the load early and still writes the document, marked as
/// interrupted, and the run exits as an aborted one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shutdown_ends_the_load_early_and_keeps_the_stats() -> Result<()> {
    let target = fake_target(EASY).await;
    let scratch = Scratch::new("shutdown");
    let out = scratch.path("stats.json");
    let args = args(
        &target.address,
        &out,
        &[
            external::GUARD_FLAG,
            "--sessions",
            "2",
            "--rate",
            "20",
            "--duration-seconds",
            "600",
        ],
    );
    let shutdown = Shutdown::never();
    let asked = shutdown.clone();
    let seen = target.seen.clone();
    tokio::spawn(async move {
        while seen.submits.load(Ordering::SeqCst) < 5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        asked.request("interrupted");
    });
    let started = std::time::Instant::now();
    let outcome = external::run(&args, &shutdown).await?;
    assert!(started.elapsed() < Duration::from_secs(60));
    assert_eq!(outcome.exit_code, run::EXIT_ABORTED);
    let document = read(&out);
    let process = &document["processes"][0];
    assert_eq!(process["ended"], json!("interrupted: interrupted"));
    assert!(process["window"]["seconds"].as_f64().unwrap() < 600.0);
    assert!(document["summary"]["shares"]["accepted"].as_u64().unwrap() >= 1);
    Ok(())
}

/// A target that goes down for good mid-run: every session loses its
/// connection and keeps failing to reconnect. From then on no offer goes to
/// a session without a connection, so the outage reads as shortfall rather
/// than as offers held for sessions that cannot send them, and every offer
/// is still accounted for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_outage_is_shortfall_and_every_offer_is_accounted_for() -> Result<()> {
    let target = fake_target(EASY).await;
    let scratch = Scratch::new("down");
    let out = scratch.path("stats.json");
    let args = args(
        &target.address,
        &out,
        &[
            external::GUARD_FLAG,
            "--sessions",
            "3",
            "--rate",
            "30",
            "--duration-seconds",
            // Long enough that whole seconds after the outage remain inside
            // the window however late a slow runner gets to it.
            "8",
        ],
    );
    let run = tokio::spawn(async move { external::run(&args, &Shutdown::never()).await });
    let seen = target.seen.clone();
    wait_until("load reaches the target", Duration::from_secs(30), || {
        seen.submits.load(Ordering::SeqCst) >= 10
    })
    .await;
    target.listener.abort();
    target.drops.send_modify(|generation| *generation += 1);
    let down_at = chrono::Utc::now().timestamp();
    let outcome = run.await??;
    assert_eq!(outcome.exit_code, run::EXIT_OK);
    let document = &outcome.document;
    let summary = &document["summary"];
    assert_eq!(
        summary["reconnects"]["disconnects"],
        json!(3),
        "{summary:#}"
    );
    assert_eq!(summary["reconnects"]["completed"], json!(0), "{summary:#}");
    assert!(summary["reconnects"]["failed_attempts"].as_u64().unwrap() >= 3);
    // A whole second after the outage, nothing is dispatched: every offer is
    // shortfall. An offer can only reach a session in the instant between
    // its socket closing and its disconnect being counted.
    let window_end = document["processes"][0]["window"]["ended_unix_ms"]
        .as_i64()
        .unwrap()
        / 1000;
    let after: Vec<&Value> = document["totals"]["timeline"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| {
            let second = s["unix_second"].as_i64().unwrap();
            second > down_at + 1 && second < window_end
        })
        .collect();
    assert!(!after.is_empty(), "{document:#}");
    for second in after {
        assert_eq!(second["dispatched"], json!(0), "{second}");
        assert_eq!(second["shortfall"], second["offered"], "{second}");
    }
    assert!(
        summary["offers"]["discarded"].as_u64().unwrap() <= 3,
        "{summary:#}"
    );
    assert_eq!(summary["offers"]["unaccounted"], json!(0), "{summary:#}");
    let process = &document["processes"][0];
    assert_eq!(process["sessions_aborted_at_stop"], json!(0));
    assert_eq!(process["events_cut_off"], Value::Null);
    Ok(())
}

/// A session that has no connection is never offered anything: here two of
/// four sessions are never admitted, and the load runs on the other two
/// without a single offer parked on the two that cannot send it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sessions_without_a_connection_are_never_offered_work() -> Result<()> {
    let target = fake_target_admitting(EASY, 2).await;
    let scratch = Scratch::new("admitted");
    let out = scratch.path("stats.json");
    let mut args = args(
        &target.address,
        &out,
        &[
            external::GUARD_FLAG,
            "--sessions",
            "4",
            "--rate",
            "20",
            "--duration-seconds",
            "2",
        ],
    );
    args.work_timeout_seconds = 1;
    let outcome = external::run(&args, &Shutdown::never()).await?;
    assert_eq!(outcome.exit_code, run::EXIT_OK);
    let summary = &outcome.document["summary"];
    let process = &outcome.document["processes"][0];
    assert_eq!(process["sessions_holding_work_at_start"], json!(2));
    assert!(
        summary["shares"]["accepted"].as_u64().unwrap() >= 10,
        "{summary:#}"
    );
    assert_eq!(process["held_without_a_connection_at_drain_end"], json!(0));
    assert_eq!(summary["offers"]["discarded"], json!(0), "{summary:#}");
    assert_eq!(summary["offers"]["unaccounted"], json!(0), "{summary:#}");
    assert!(
        summary["connections"]["initial_connect_failures"]
            .as_u64()
            .unwrap()
            >= 2
    );
    Ok(())
}

/// A window mints every offer its rate and length promise, the one due at
/// its very end included: at one offer a second for one second, one offer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_makes_every_offer_its_rate_and_length_promise() -> Result<()> {
    let target = fake_target(EASY).await;
    let scratch = Scratch::new("boundary");
    for (rate, seconds, offers) in [("1", "1", 1u64), ("3", "2", 6)] {
        let out = scratch.path(&format!("stats-{rate}-{seconds}.json"));
        let args = args(
            &target.address,
            &out,
            &[
                external::GUARD_FLAG,
                "--sessions",
                "2",
                "--rate",
                rate,
                "--duration-seconds",
                seconds,
            ],
        );
        let outcome = external::run(&args, &Shutdown::never()).await?;
        let summary = &outcome.document["summary"];
        assert_eq!(summary["offers"]["minted"], json!(offers), "{summary:#}");
        assert_eq!(summary["shares"]["accepted"], json!(offers), "{summary:#}");
        // At least its length; past it only by the last tick's lateness.
        let window = outcome.document["processes"][0]["window"]["seconds"]
            .as_f64()
            .unwrap();
        let length = seconds.parse::<f64>()?;
        assert!(window >= length && window < length + 1.0, "{window}");
    }
    Ok(())
}

async fn next_event(
    inbox: &mut tokio::sync::mpsc::UnboundedReceiver<client::Event>,
    what: &str,
    mut wanted: impl FnMut(&client::Event) -> bool,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
            .expect("the session is still running");
        if wanted(&event) {
            return;
        }
    }
}

/// An offer that reaches a session in the instant its connection is gone is
/// dropped, as a discarded offer, when the session reconnects, rather than
/// sent with the reconnect as a burst the outage never offered. A harness
/// session, which keeps its offers, still sends it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_offer_taken_while_disconnected_is_dropped_on_reconnect() -> Result<()> {
    for drop in [true, false] {
        let target = fake_target(EASY).await;
        let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let shared = Arc::new(client::SessionShared {
            phase: std::sync::RwLock::new(external::PHASE.to_owned()),
            events,
            record_notifies: std::sync::atomic::AtomicBool::new(false),
            kill_fence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            stopping: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        let config = client::SessionConfig {
            index: 0,
            username: "pload1external.t-s00000".into(),
            password: "x".into(),
            difficulty: client::DifficultySource::Advertised {
                ceiling: external::DEFAULT_MAX_DIFFICULTY,
            },
            version_rolling_mask: codec::VERSION_ROLLING_MASK,
            connect_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(5),
            quiesce_limit: Duration::from_secs(5),
            drop_offers_held_while_disconnected: drop,
        };
        let handle = client::spawn_session(config, 0, target.address.clone(), shared, 1);
        next_event(&mut inbox, "the first connection", |event| {
            matches!(event, client::Event::Connected { .. })
        })
        .await;
        target.seen.refusing.store(true, Ordering::SeqCst);
        target.drops.send_modify(|generation| *generation += 1);
        next_event(&mut inbox, "the disconnect", |event| {
            matches!(event, client::Event::Disconnected { .. })
        })
        .await;
        let phase: Arc<str> = Arc::from(external::PHASE);
        assert!(
            handle.try_offer(1, &phase),
            "a session's queue takes one offer"
        );
        let sent_before = target.seen.submits.load(Ordering::SeqCst);
        target.seen.refusing.store(false, Ordering::SeqCst);
        if drop {
            next_event(&mut inbox, "the dropped offer", |event| {
                matches!(event, client::Event::DiscardedOffer { .. })
            })
            .await;
            next_event(&mut inbox, "the reconnect", |event| {
                matches!(event, client::Event::Connected { .. })
            })
            .await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(handle.outstanding.load(Ordering::SeqCst), 0);
            assert_eq!(target.seen.submits.load(Ordering::SeqCst), sent_before);
        } else {
            next_event(&mut inbox, "the held offer's answer", |event| {
                matches!(event, client::Event::Submit(record) if record.outcome.label() == "accepted")
            })
            .await;
            assert_eq!(target.seen.submits.load(Ordering::SeqCst), sent_before + 1);
        }
        let _ = handle.control.send(client::Control::Stop);
        tokio::time::timeout(Duration::from_secs(5), handle.task).await??;
    }
    Ok(())
}

/// A search does not hold the session's reading back: here the first job is
/// one no search of the nonce span is likely to solve, and a new, easy job
/// arrives while it runs. The search stops for the new job, the session
/// reads it, and the same offer is mined on it and sent, rather than ground
/// out on the retired job for millions of hashes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_search_stops_for_a_new_job_and_mines_the_offer_on_it() -> Result<()> {
    let target = fake_target(1.0).await;
    let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
    let shared = Arc::new(client::SessionShared {
        phase: std::sync::RwLock::new(external::PHASE.to_owned()),
        events,
        record_notifies: std::sync::atomic::AtomicBool::new(false),
        kill_fence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        stopping: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    });
    let config = client::SessionConfig {
        index: 0,
        username: "pload1external.t-s00000".into(),
        password: "x".into(),
        // A ceiling of 1 lets the session take on the hopeless job at all.
        difficulty: client::DifficultySource::Advertised { ceiling: 1.0 },
        version_rolling_mask: codec::VERSION_ROLLING_MASK,
        connect_timeout: Duration::from_secs(5),
        handshake_timeout: Duration::from_secs(5),
        quiesce_limit: Duration::from_secs(5),
        drop_offers_held_while_disconnected: true,
    };
    let handle = client::spawn_session(config, 0, target.address.clone(), shared, 1);
    next_event(&mut inbox, "the first connection", |event| {
        matches!(event, client::Event::Connected { .. })
    })
    .await;
    let phase: Arc<str> = Arc::from(external::PHASE);
    assert!(handle.try_offer(1, &phase));
    tokio::time::sleep(Duration::from_millis(200)).await;
    target.difficulty.send_replace(EASY);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let record = loop {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .expect("the offer is sent on the new job within seconds")
            .expect("the session is still running");
        match event {
            client::Event::Submit(record) => break record,
            client::Event::Failure(failure) => panic!("the offer failed: {failure:?}"),
            _ => {}
        }
    };
    assert_eq!(record.job_id, "job-2", "{record:?}");
    assert_eq!(record.outcome.label(), "accepted");
    // The answer's record goes out just before its slot is released.
    let outstanding = handle.outstanding.clone();
    wait_until(
        "the offer's slot is released",
        Duration::from_secs(5),
        || outstanding.load(Ordering::SeqCst) == 0,
    )
    .await;
    let _ = handle.control.send(client::Control::Stop);
    tokio::time::timeout(Duration::from_secs(5), handle.task).await??;
    Ok(())
}

/// A run's stop is not held behind a search: here the session is grinding
/// a job no search of the span is likely to solve when the run stops, and it
/// stops at once rather than after millions of hashes, its offer discarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_ends_a_search_at_once() -> Result<()> {
    let target = fake_target(1.0).await;
    let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
    let stopping = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shared = Arc::new(client::SessionShared {
        phase: std::sync::RwLock::new(external::PHASE.to_owned()),
        events,
        record_notifies: std::sync::atomic::AtomicBool::new(false),
        kill_fence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        stopping: stopping.clone(),
    });
    let config = client::SessionConfig {
        index: 0,
        username: "pload1external.t-s00000".into(),
        password: "x".into(),
        difficulty: client::DifficultySource::Advertised { ceiling: 1.0 },
        version_rolling_mask: codec::VERSION_ROLLING_MASK,
        connect_timeout: Duration::from_secs(5),
        handshake_timeout: Duration::from_secs(5),
        quiesce_limit: Duration::from_secs(5),
        drop_offers_held_while_disconnected: true,
    };
    let handle = client::spawn_session(config, 0, target.address.clone(), shared, 1);
    next_event(&mut inbox, "the first connection", |event| {
        matches!(event, client::Event::Connected { .. })
    })
    .await;
    let phase: Arc<str> = Arc::from(external::PHASE);
    assert!(handle.try_offer(1, &phase));
    tokio::time::sleep(Duration::from_millis(200)).await;
    stopping.store(true, Ordering::SeqCst);
    let _ = handle.control.send(client::Control::Stop);
    tokio::time::timeout(Duration::from_secs(3), handle.task)
        .await
        .expect("the session stops within seconds, not after the search")?;
    assert_eq!(handle.outstanding.load(Ordering::SeqCst), 0);
    assert_eq!(target.seen.submits.load(Ordering::SeqCst), 0);
    Ok(())
}

/// Nothing already beside --out can stop the document being written after
/// the load: here a directory sits where a fixed partial name would be.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_document_is_written_whatever_lies_beside_it() -> Result<()> {
    let target = fake_target(EASY).await;
    let scratch = Scratch::new("beside");
    let out = scratch.path("stats.json");
    std::fs::create_dir(scratch.path(".stats.json.partial"))?;
    let args = args(
        &target.address,
        &out,
        &[
            external::GUARD_FLAG,
            "--sessions",
            "1",
            "--rate",
            "5",
            "--duration-seconds",
            "1",
        ],
    );
    let outcome = external::run(&args, &Shutdown::never()).await?;
    assert_eq!(outcome.exit_code, run::EXIT_OK);
    assert_eq!(read(&out), outcome.document);
    Ok(())
}

/// A session waiting for a worker to run its search does not time what
/// happens on its connection late: here the only blocking worker is busy for
/// 3 s, the session's search waits behind it, and its connection closes
/// meanwhile. The close is timed when the reader saw it, not when the
/// session got round to it.
#[test]
fn a_close_is_timed_when_it_happened_even_while_the_search_waits_for_a_worker() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let target = fake_target(EASY).await;
        let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let shared = Arc::new(client::SessionShared {
            phase: std::sync::RwLock::new(external::PHASE.to_owned()),
            events,
            record_notifies: std::sync::atomic::AtomicBool::new(false),
            kill_fence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            stopping: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        let config = client::SessionConfig {
            index: 0,
            username: "pload1external.t-s00000".into(),
            password: "x".into(),
            difficulty: client::DifficultySource::Advertised {
                ceiling: external::DEFAULT_MAX_DIFFICULTY,
            },
            version_rolling_mask: codec::VERSION_ROLLING_MASK,
            connect_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(5),
            quiesce_limit: Duration::from_secs(5),
            drop_offers_held_while_disconnected: true,
        };
        let handle = client::spawn_session(config, 0, target.address.clone(), shared, 1);
        next_event(&mut inbox, "the first connection", |event| {
            matches!(event, client::Event::Connected { .. })
        })
        .await;
        // The only blocking worker, busy.
        let busy = tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_secs(3)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let phase: Arc<str> = Arc::from(external::PHASE);
        assert!(handle.try_offer(1, &phase));
        tokio::time::sleep(Duration::from_millis(50)).await;
        target.seen.refusing.store(true, Ordering::SeqCst);
        let closed_at = std::time::Instant::now();
        target.drops.send_modify(|generation| *generation += 1);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let closed = loop {
            let event = tokio::time::timeout_at(deadline, inbox.recv())
                .await
                .expect("the close is reported")
                .expect("the session is still running");
            if let client::Event::Closed(closed) = event {
                break closed;
            }
        };
        busy.await?;
        let late = closed.at.saturating_duration_since(closed_at);
        assert!(
            late < Duration::from_secs(1),
            "the close was timed {late:?} late"
        );
        let _ = handle.control.send(client::Control::Stop);
        tokio::time::timeout(Duration::from_secs(10), handle.task).await??;
        Ok::<_, anyhow::Error>(())
    })
}

/// A session that comes back to a tip, as one can while the frontends
/// behind a balancer disagree, is counted on it once, at its first
/// sighting.
#[test]
fn a_session_is_counted_once_per_tip_however_often_it_comes_back() {
    let anchor = stats::Anchor::now();
    let mut collector = stats::Collector::new(anchor, None, 2);
    let start = std::time::Instant::now();
    let sight = |session: usize, tip: &str, millis: u64| {
        client::Event::Tip(client::TipSighting {
            session,
            frontend: 0,
            tip: tip.into(),
            at: start + Duration::from_millis(millis),
        })
    };
    for event in [
        sight(0, "a", 0),
        sight(1, "a", 10),
        sight(0, "b", 20),
        sight(0, "a", 900),
        sight(1, "a", 950),
    ] {
        collector.apply(event);
    }
    let tips = &collector.totals.tips;
    assert_eq!(tips.tips["a"].sessions, 2);
    assert_eq!(
        tips.tips["a"].last_seen_unix_ms - tips.tips["a"].first_seen_unix_ms,
        10
    );
    assert_eq!(tips.tips["b"].sessions, 1);
}

/// The output checks never touch an earlier run's stats, and refuse a share
/// log that would be overwritten by the document.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_outputs_are_checked_without_touching_an_earlier_run() -> Result<()> {
    let target = fake_target(EASY).await;
    let scratch = Scratch::new("outputs");
    let out = scratch.path("stats.json");
    std::fs::write(&out, "an earlier run's stats")?;
    let out_arg = out.display().to_string();
    let missing = scratch.path("no-such-directory/shares.jsonl");
    let missing = missing.display().to_string();
    std::fs::create_dir(scratch.path("sub"))?;
    let detour = scratch.path("sub/../stats.json").display().to_string();
    for (extra, needle) in [
        (vec!["--share-log", &out_arg], "same file"),
        (vec!["--share-log", &detour], "same file"),
        (vec!["--share-log", &missing], "no-such-directory"),
    ] {
        let mut flags = vec![external::GUARD_FLAG];
        flags.extend(extra.iter().copied());
        let args = args(&target.address, &out, &flags);
        let error = match external::run(&args, &Shutdown::never()).await {
            Ok(_) => panic!("{extra:?} ran"),
            Err(error) => format!("{error:#}"),
        };
        assert!(error.contains(needle), "{extra:?}: {error}");
        assert_eq!(
            std::fs::read_to_string(&out)?,
            "an earlier run's stats",
            "{extra:?}"
        );
    }
    // Neither file exists yet, and the share log reaches the document's path
    // through a detour: still the same file, and still refused.
    let fresh = scratch.path("fresh.json");
    let fresh_detour = scratch.path("sub/../fresh.json").display().to_string();
    let detoured = args(
        &target.address,
        &fresh,
        &[external::GUARD_FLAG, "--share-log", &fresh_detour],
    );
    let error = match external::run(&detoured, &Shutdown::never()).await {
        Ok(_) => panic!("a share log aliasing a fresh --out ran"),
        Err(error) => format!("{error:#}"),
    };
    assert!(error.contains("same file"), "{error}");
    assert!(!fresh.exists());
    // A share log that is the document under another name: a hard link to
    // an --out that exists.
    let shares = scratch.path("shares.jsonl");
    std::fs::hard_link(&out, &shares)?;
    let hard = shares.display().to_string();
    let linked_hard = args(
        &target.address,
        &out,
        &[external::GUARD_FLAG, "--share-log", &hard],
    );
    let error = match external::run(&linked_hard, &Shutdown::never()).await {
        Ok(_) => panic!("a share log hard-linked to --out ran"),
        Err(error) => format!("{error:#}"),
    };
    assert!(error.contains("same file"), "{error}");
    assert_eq!(std::fs::read_to_string(&out)?, "an earlier run's stats");
    std::fs::remove_file(&shares)?;
    // A dangling symlink to the document's path: the log would be created
    // through it, at the document's path.
    std::os::unix::fs::symlink(&fresh, scratch.path("link.jsonl"))?;
    let link = scratch.path("link.jsonl").display().to_string();
    let linked = args(
        &target.address,
        &fresh,
        &[external::GUARD_FLAG, "--share-log", &link],
    );
    let error = match external::run(&linked, &Shutdown::never()).await {
        Ok(_) => panic!("a share log symlinked to a fresh --out ran"),
        Err(error) => format!("{error:#}"),
    };
    assert!(error.contains("same file"), "{error}");
    assert!(!fresh.exists());
    std::fs::remove_file(scratch.path("link.jsonl"))?;
    assert_eq!(target.seen.connections.load(Ordering::SeqCst), 0);
    // Nothing is left behind by the check either.
    let leftovers: Vec<_> = std::fs::read_dir(&scratch.0)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name())
        .collect();
    let mut leftovers = leftovers;
    leftovers.sort();
    assert_eq!(
        leftovers,
        vec![
            std::ffi::OsString::from("stats.json"),
            std::ffi::OsString::from("sub")
        ]
    );
    Ok(())
}

/// The binary itself: `external` and `external-merge` are read before the
/// harness's own command line, the guard's refusal is exit 2 with its reason
/// on stderr, and two runs' documents merge through the subcommand.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_binary_runs_both_subcommands() -> Result<()> {
    let binary = env!("CARGO_BIN_EXE_qbit-prism-load");
    let target = fake_target(EASY).await;
    let scratch = Scratch::new("binary");
    let refused = tokio::process::Command::new(binary)
        .args([
            "external",
            "--target",
            &target.address,
            "--address",
            "pload1external",
        ])
        .output()
        .await?;
    assert_eq!(refused.status.code(), Some(run::EXIT_ERROR));
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains(external::GUARD_FLAG), "{stderr}");
    assert_eq!(target.seen.connections.load(Ordering::SeqCst), 0);
    let mut documents = Vec::new();
    for label in ["vm-a", "vm-b"] {
        let out = scratch.path(&format!("{label}.json"));
        let status = tokio::process::Command::new(binary)
            .args([
                "external",
                "--target",
                &target.address,
                external::GUARD_FLAG,
                "--address",
                "pload1external",
                "--label",
                label,
                "--sessions",
                "2",
                "--rate",
                "10",
                "--duration-seconds",
                "1",
                "--progress-seconds",
                "0",
                "--out",
            ])
            .arg(&out)
            .status()
            .await?;
        assert_eq!(status.code(), Some(run::EXIT_OK), "{label}");
        documents.push(out);
    }
    let merged = scratch.path("merged.json");
    let status = tokio::process::Command::new(binary)
        .arg("external-merge")
        .args(&documents)
        .arg("--out")
        .arg(&merged)
        .status()
        .await?;
    assert_eq!(status.code(), Some(run::EXIT_OK));
    let merged = read(&merged);
    assert_eq!(merged["kind"], json!("merged"));
    assert_eq!(merged["summary"]["labels"], json!(["vm-a", "vm-b"]));
    assert!(merged["summary"]["shares"]["accepted"].as_u64().unwrap() >= 2);
    Ok(())
}

/// A target nobody answers on is refused as blocked once the work timeout
/// passes, and the document says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_target_that_never_serves_work_is_blocked() -> Result<()> {
    // Bound and closed: nothing listens on it.
    let address = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.local_addr()?.to_string()
    };
    let scratch = Scratch::new("blocked");
    let out = scratch.path("stats.json");
    let mut args = args(
        &address,
        &out,
        &[
            external::GUARD_FLAG,
            "--sessions",
            "2",
            "--duration-seconds",
            "5",
        ],
    );
    args.work_timeout_seconds = 1;
    let outcome = external::run(&args, &Shutdown::never()).await?;
    assert_eq!(outcome.exit_code, run::EXIT_BLOCKED);
    let process = &outcome.document["processes"][0];
    assert!(process["ended"]
        .as_str()
        .unwrap()
        .starts_with("blocked: no session held work"));
    assert!(process["window"].is_null());
    assert!(
        outcome.document["summary"]["connections"]["initial_connect_failures"]
            .as_u64()
            .unwrap()
            >= 2
    );
    assert!(read(&out)["summary"]["rates"]["accepted_per_second"].is_null());
    Ok(())
}
