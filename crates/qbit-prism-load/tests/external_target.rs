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
                seen.connections.fetch_add(1, Ordering::SeqCst);
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
    first.merge(&second);
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
    let mut other = wire;
    other["significant_bits"] = json!(7);
    assert!(serde_json::from_value::<LogHistogram>(other).is_err());
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
    )?);
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
    let error = format!("{:#}", external::merge_files(&[a.clone(), a]).unwrap_err());
    assert!(error.contains("count it twice"), "{error}");
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
/// connection and keeps failing to reconnect until the run stops it, and the
/// offers they had taken are still accounted for, not lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_that_ends_with_the_target_down_still_accounts_for_every_offer() -> Result<()> {
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
            "3",
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
    let outcome = run.await??;
    assert_eq!(outcome.exit_code, run::EXIT_OK);
    let summary = &outcome.document["summary"];
    assert_eq!(
        summary["reconnects"]["disconnects"],
        json!(3),
        "{summary:#}"
    );
    assert_eq!(summary["reconnects"]["completed"], json!(0), "{summary:#}");
    assert!(summary["reconnects"]["failed_attempts"].as_u64().unwrap() >= 3);
    assert!(
        summary["offers"]["discarded"].as_u64().unwrap() >= 1,
        "{summary:#}"
    );
    assert_eq!(summary["offers"]["unaccounted"], json!(0), "{summary:#}");
    assert_eq!(
        outcome.document["processes"][0]["sessions_aborted_at_stop"],
        json!(0)
    );
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
