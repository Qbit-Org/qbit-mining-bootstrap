//! #558 (#487 S14 follow-up): vardiff converges per session over a real
//! Stratum socket.
//!
//! Two miners of different hashrates connect to the native listener at the
//! same startup difficulty: one well below its target difficulty (16x) and
//! one well above it (16x). Each solves real share proofs at its current
//! job's difficulty and paces its submits from a fixed hashrate: a share
//! every `difficulty / hashrate` seconds, on a schedule anchored to the
//! previous share's due time so jitter does not accumulate, and restarted
//! when a job at a new difficulty arrives, as a miner starts hashing the new
//! job then. Vardiff should
//! bring each session to `hashrate * target_seconds`, the difficulty at
//! which it submits one share per `target_seconds`.
//!
//! What each session is held to, from the difficulties the listener sent it
//! and when:
//! - it settles within `MAX_RETARGET_PERIODS_TO_SETTLE` retarget periods of
//!   its first job, and within `MAX_ADJUSTMENTS_TO_SETTLE` adjustments: from
//!   then on every difficulty keeps the share interval inside `BAND` of the
//!   target interval. The listener sends nothing for an evaluation that
//!   leaves the difficulty alone, so the period bound is on elapsed time;
//! - every adjustment, before and after settling, moves toward the target:
//!   the difficulty never reverses direction;
//! - it stays settled for at least `SETTLED_RETARGET_PERIODS` retarget
//!   periods, with at most one further adjustment.
//!
//! The listener's clock is the real one (vardiff reads `std::time::Instant`),
//! so the premise that the client kept its pace is checked, and reported as
//! such, instead of being assumed. The fake backend is a pared-down copy of
//! `tests/stratum_protocol.rs`'s: no persistence, no resume, one tip.

use qbit_pool_builder::{build_manifest, CoinbaseBuildRequest, WeightedEntitlement};
use qbit_prism_server::{
    codec::{difficulty_target, double_sha256, Job, Submission},
    ledger::SessionId,
    stratum::*,
};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    time::{sleep_until, timeout, Instant},
};

/// The share interval vardiff aims for, and how often it may retarget.
const TARGET_SECONDS: f64 = 0.1;
const RETARGET_SECONDS: f64 = 0.5;
/// Both sessions start here; each target is 16x away from it.
const START_DIFFICULTY: f64 = 1.6e-8;
const START_ERROR: f64 = 16.0;
/// Settled: the share interval is within this factor of TARGET_SECONDS.
const BAND: f64 = 1.5;
/// A 16x error takes two adjustments at vardiff's 4x step limit; two more
/// allow for a window cut short by the listener's once-a-second timer.
const MAX_ADJUSTMENTS_TO_SETTLE: usize = 4;
/// Vardiff evaluates a session on each accepted share and on the listener's
/// once-a-second timer, at least RETARGET_SECONDS apart. The fast session
/// settles after two share-driven periods (1.0 s). The slow one sends no
/// share before the listener's first timer tick past RETARGET_SECONDS (0.5
/// to 1.5 s in, about 1.0 s in practice: an idle step down), then needs one
/// more window of two shares (0.8 s): about 3.6 periods, 4.6 at worst. Six
/// periods (3.0 s) cover the worst case.
const MAX_RETARGET_PERIODS_TO_SETTLE: f64 = 6.0;
/// How long a session must then be seen staying settled.
const SETTLED_RETARGET_PERIODS: f64 = 8.0;
const RUN: Duration = Duration::from_secs(8);
/// Regtest bits would make every share a block; mainnet-scale bits keep the
/// shares proving exactly their job's share difficulty.
const NETWORK_BITS: &str = "1d00ffff";

#[derive(Default)]
struct Backend {
    sessions: AtomicU32,
    jobs: AtomicU64,
    shares: Mutex<HashSet<String>>,
}

impl MiningBackend for Backend {
    type Context = ();
    async fn new_session_id(&self) -> Result<SessionId, StratumError> {
        Ok((self.sessions.fetch_add(1, Ordering::Relaxed) + 1).into())
    }
    async fn authorize(&self, username: &str) -> Result<Worker, StratumError> {
        Ok(Worker {
            username: username.into(),
            payout_address: username.split('.').next().unwrap().into(),
            worker_name: username.split_once('.').map(|(_, w)| w.into()),
            p2mr_program_hex: "ab".repeat(32),
        })
    }
    async fn build_job(
        &self,
        worker: &Worker,
        extranonce1: &str,
        difficulty: f64,
        minimum: f64,
    ) -> Result<MiningJob<()>, StratumError> {
        let template = json!({"version":0x20000000u32,"bits":NETWORK_BITS,"curtime":1_700_000_000u32,
            "previousblockhash":format!("{:064x}", 0),"transactions":[]});
        let manifest = build_manifest(CoinbaseBuildRequest {
            block_height: 1,
            coinbase_value_sats: 5_000_000_000,
            entitlements: vec![WeightedEntitlement {
                recipient_id: worker.payout_address.clone(),
                order_key: worker.payout_address.clone(),
                p2mr_program_hex: worker.p2mr_program_hex.clone(),
                weight: 1,
            }],
            witness_nonce_hex: None,
            witness_merkle_leaves_hex: vec![],
            coinbase_script_sig_suffix_hex: Some(format!("{extranonce1}{}", "00".repeat(8))),
            pinned_first_output: None,
        })
        .unwrap();
        let job = Job::from_manifest(
            format!("job-{}", self.jobs.fetch_add(1, Ordering::Relaxed)),
            &template,
            &manifest,
            extranonce1,
            8,
            difficulty,
            minimum,
            true,
        )
        .unwrap();
        Ok(MiningJob {
            wire: job,
            context: Arc::new(()),
        })
    }
    async fn submit(
        &self,
        _worker: &Worker,
        _job: &MiningJob<()>,
        submission: Submission,
        _grace: StaleGrace,
    ) -> Result<(), StratumError> {
        if !submission.share_pass {
            return Err(StratumError::new(
                23,
                "low difficulty share",
                "low-difficulty",
            ));
        }
        if !self
            .shares
            .lock()
            .unwrap()
            .insert(submission.block_hash_hex)
        {
            return Err(StratumError::new(22, "duplicate share", "duplicate-share"));
        }
        Ok(())
    }
}

/// A share as submitted: seconds since the first job when it was due and
/// when it was actually sent, and its job's difficulty.
#[derive(Debug)]
struct Share {
    due: f64,
    sent: f64,
    difficulty: f64,
}

/// What one session saw: every difficulty it was sent, at seconds since its
/// first job, and every share it submitted.
#[derive(Debug, Default)]
struct Trace {
    difficulties: Vec<(f64, f64)>,
    shares: Vec<Share>,
    refusals: Vec<Value>,
    ran_seconds: f64,
}

/// A share proof for the current job at its share difficulty.
fn solve(notify: &Value, extranonce1: &str, difficulty: f64, nonce: &mut u32) -> Value {
    let p = notify["params"].as_array().unwrap();
    let text = |index: usize| p[index].as_str().unwrap();
    let coinbase = hex::decode(format!(
        "{}{extranonce1}0000000000000000{}",
        text(2),
        text(3)
    ))
    .unwrap();
    let merkle = double_sha256(&coinbase);
    let mut previous = hex::decode(text(1)).unwrap();
    for word in previous.as_chunks_mut::<4>().0 {
        word.reverse();
    }
    let word = |index: usize| u32::from_str_radix(text(index), 16).unwrap().to_le_bytes();
    let target = difficulty_target(difficulty).unwrap();
    loop {
        *nonce = nonce.checked_add(1).expect("nonce space exhausted");
        let header = [
            word(5).as_slice(),
            previous.as_slice(),
            merkle.as_slice(),
            word(7).as_slice(),
            word(6).as_slice(),
            nonce.to_le_bytes().as_slice(),
        ]
        .concat();
        if num_bigint::BigUint::from_bytes_le(&double_sha256(&header)) <= target {
            return json!({"id":null,"method":"mining.submit",
                "params":[p[0].clone(),p[0],"0000000000000000",p[7],format!("{nonce:08x}")]});
        }
    }
}

/// Mine as `username` at `hashrate` (difficulty per second) for `RUN`.
async fn mine(address: std::net::SocketAddr, username: &str, hashrate: f64) -> Trace {
    let (read, mut writer) = TcpStream::connect(address).await.unwrap().into_split();
    // One reader task, so waiting on a line never races the pacing timer.
    let (lines, mut incoming) = mpsc::unbounded_channel::<Value>();
    tokio::spawn(async move {
        let mut reader = BufReader::new(read).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            if lines.send(serde_json::from_str(&line).unwrap()).is_err() {
                break;
            }
        }
    });
    let mut send = async |value: Value| {
        writer
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap()
    };
    let next = async |incoming: &mut mpsc::UnboundedReceiver<Value>| {
        timeout(Duration::from_secs(5), incoming.recv())
            .await
            .expect("the listener sent nothing for 5 s")
            .expect("the listener closed the connection")
    };

    send(json!({"id":1,"method":"mining.subscribe","params":[]})).await;
    send(json!({"id":2,"method":"mining.authorize","params":[username,"x"]})).await;
    let mut extranonce1 = String::new();
    let mut announced = None;
    let mut job: Option<(Value, f64)> = None;
    while job.is_none() {
        let message = next(&mut incoming).await;
        match (message["id"].as_u64(), message["method"].as_str()) {
            (Some(1), _) => extranonce1 = message["result"][1].as_str().unwrap().into(),
            (Some(2), _) => assert_eq!(message["result"], true, "{username} was not authorized"),
            (_, Some("mining.set_difficulty")) => announced = message["params"][0].as_f64(),
            (_, Some("mining.notify")) => job = Some((message, announced.unwrap())),
            _ => {}
        }
    }
    let (mut notify, mut difficulty) = job.unwrap();
    let started = Instant::now();
    let seconds = |at: Instant| at.duration_since(started).as_secs_f64();
    let deadline = started + RUN;
    let mut trace = Trace {
        difficulties: vec![(0.0, difficulty)],
        ..Default::default()
    };
    let (mut last_due, mut outstanding, mut id, mut nonce) = (started, None, 10u64, 0u32);
    loop {
        let interval = Duration::from_secs_f64(difficulty / hashrate);
        let mut due = last_due + interval;
        // A client that fell more than a share behind (a stalled host)
        // resumes its pace instead of submitting the backlog in a burst.
        if Instant::now().saturating_duration_since(due) > interval {
            due = Instant::now();
        }
        tokio::select! {
            _ = sleep_until(deadline) => break,
            message = incoming.recv() => {
                let message = message.expect("the listener closed the connection");
                match message["method"].as_str() {
                    Some("mining.set_difficulty") => announced = message["params"][0].as_f64(),
                    Some("mining.notify") => {
                        notify = message;
                        let sent = announced.expect("a job before any difficulty");
                        if sent != difficulty {
                            let now = Instant::now();
                            trace.difficulties.push((seconds(now), sent));
                            last_due = now;
                        }
                        difficulty = sent;
                    }
                    _ if message["id"].as_u64() == outstanding => {
                        if message["result"] != true {
                            trace.refusals.push(message);
                        }
                        outstanding = None;
                    }
                    _ => {}
                }
            }
            _ = sleep_until(due), if outstanding.is_none() => {
                let mut submit = solve(&notify, &extranonce1, difficulty, &mut nonce);
                submit["id"] = json!(id);
                submit["params"][0] = json!(username);
                send(submit).await;
                trace.shares.push(Share { due: seconds(due), sent: seconds(Instant::now()), difficulty });
                outstanding = Some(id);
                id += 1;
                last_due = due;
            }
        }
    }
    trace.ran_seconds = seconds(Instant::now());
    trace
}

/// The pace the client kept at each difficulty it was sent: for each span
/// from one advertised difficulty to the next (or the end of the run), the
/// shares it sent there against the whole intervals the span holds at its
/// hashrate. The schedule restarts at each new difficulty, so a client that
/// kept its pace sent exactly that many, give or take the one in flight.
/// Returns `(seconds at the span's start, sent, expected)` per span.
fn pace_by_difficulty(trace: &Trace, hashrate: f64) -> Vec<(f64, usize, usize)> {
    trace
        .difficulties
        .iter()
        .enumerate()
        .map(|(index, &(start, difficulty))| {
            let end = trace
                .difficulties
                .get(index + 1)
                .map_or(trace.ran_seconds, |(at, _)| *at);
            let sent = trace
                .shares
                .iter()
                .filter(|share| {
                    share.difficulty == difficulty && share.sent >= start && share.sent < end
                })
                .count();
            // The epsilon keeps a whole number of intervals whole in floating point.
            let expected = ((end - start) / (difficulty / hashrate) + 1e-9).floor() as usize;
            (start, sent, expected)
        })
        .collect()
}

/// Whether a span's shares fell short of its pace: up to a fifth of the
/// expected shares may be missing, and never fewer than one (the share in
/// flight at the span's end).
fn short_of_pace(sent: usize, expected: usize) -> bool {
    sent + (expected / 5).max(1) < expected
}

/// The index of the first adjustment that moves away from where the first
/// one went (a reversal), or None when every adjustment keeps its direction.
fn reversal(difficulties: &[(f64, f64)]) -> Option<usize> {
    let mut direction = None;
    for (index, pair) in difficulties.windows(2).enumerate() {
        let up = pair[1].1 > pair[0].1;
        if *direction.get_or_insert(up) != up {
            return Some(index + 1);
        }
    }
    None
}

/// The index of the first difficulty from which every later one is within
/// `BAND` of `target`, or None when the session never settled.
fn settled_from(difficulties: &[(f64, f64)], target: f64) -> Option<usize> {
    let within = |difficulty: f64| (1.0 / BAND..=BAND).contains(&(difficulty / target));
    let outside = difficulties.iter().rposition(|(_, d)| !within(*d));
    match outside {
        None => Some(0),
        Some(last) if last + 1 < difficulties.len() => Some(last + 1),
        Some(_) => None,
    }
}

fn check(name: &str, trace: &Trace, hashrate: f64) {
    let target = hashrate * TARGET_SECONDS;
    assert!(
        trace.refusals.is_empty(),
        "{name}: refused shares {:?}",
        trace.refusals
    );
    // The premise: the listener measured a client that kept its pace. It is
    // judged at every difficulty the session was sent, as the shares were
    // actually sent: a span in which the client fell short of its hashrate
    // (a stalled host) fails here, as the host's fault and not vardiff's,
    // before its effect on the trajectory can be read as a vardiff finding.
    // The final difficulty's mean interval is checked too. Shares sent over
    // half an interval late (bunched by a busy host) are only counted:
    // convergence despite them is still convergence, and every failure below
    // names them.
    let (final_at, final_difficulty) = *trace.difficulties.last().unwrap();
    let expected = final_difficulty / hashrate;
    let paced: Vec<&Share> = trace
        .shares
        .iter()
        .filter(|share| share.difficulty == final_difficulty && share.sent >= final_at)
        .collect();
    let steps: Vec<String> = trace
        .difficulties
        .iter()
        .map(|(at, d)| format!("{at:.2}s:{:.3}x", d / target))
        .collect();
    assert!(
        paced.len() >= 10,
        "{name}: only {} shares at the final difficulty; difficulty / target over time {steps:?}",
        paced.len()
    );
    let measured = (paced.last().unwrap().sent - paced[0].sent) / (paced.len() - 1) as f64;
    let late = paced
        .iter()
        .filter(|share| share.sent - share.due > expected / 2.0)
        .count();
    let context = format!(
        "{name}: difficulty / target over time {steps:?}; the client sent every {measured:.3}s \
         against the {expected:.3}s its hashrate implies, {late} of {} shares over half an \
         interval late",
        paced.len()
    );
    assert!(
        (0.8..=1.25).contains(&(measured / expected)),
        "{context}: premise failed, the client did not keep its pace (host overloaded?)"
    );
    let spans = pace_by_difficulty(trace, hashrate);
    let short: Vec<String> = spans
        .iter()
        .filter(|(_, sent, expected)| short_of_pace(*sent, *expected))
        .map(|(at, sent, expected)| format!("from {at:.2}s: {sent} of {expected}"))
        .collect();
    assert!(
        short.is_empty(),
        "{context}: premise failed, the client sent too few shares at some difficulty \
         ({short:?}): a stalled host, or the listener acknowledging slowly, since the client \
         sends a share only once the last is answered"
    );
    let start = trace.difficulties[0].1 / target;
    assert!(
        (start - START_ERROR).abs() < 1e-9 || (start - 1.0 / START_ERROR).abs() < 1e-9,
        "{context}: the session did not start {START_ERROR}x from its target"
    );
    if let Some(index) = reversal(&trace.difficulties) {
        panic!("{context}: adjustment {index} reversed the direction of the ones before it");
    }
    let settled = settled_from(&trace.difficulties, target)
        .unwrap_or_else(|| panic!("{context}: never settled within {BAND}x of its target"));
    assert!(
        settled <= MAX_ADJUSTMENTS_TO_SETTLE,
        "{context}: took {settled} adjustments to settle, more than {MAX_ADJUSTMENTS_TO_SETTLE}"
    );
    let settled_at = trace.difficulties[settled].0;
    assert!(
        settled_at <= MAX_RETARGET_PERIODS_TO_SETTLE * RETARGET_SECONDS,
        "{context}: settled at {settled_at:.2}s, later than {MAX_RETARGET_PERIODS_TO_SETTLE} \
         retarget periods ({:.2}s)",
        MAX_RETARGET_PERIODS_TO_SETTLE * RETARGET_SECONDS
    );
    let held = trace.ran_seconds - settled_at;
    assert!(
        held >= SETTLED_RETARGET_PERIODS * RETARGET_SECONDS,
        "{context}: settled at {settled_at:.2}s, leaving {held:.2}s, less than \
         {SETTLED_RETARGET_PERIODS} retarget periods to show it stays settled"
    );
    let changes = trace.difficulties.len() - 1 - settled;
    assert!(
        changes <= 1,
        "{context}: changed {changes} times after settling"
    );
    let spans: Vec<String> = spans
        .iter()
        .map(|(at, sent, expected)| format!("{at:.2}s:{sent}/{expected}"))
        .collect();
    eprintln!(
        "{context}; shares sent / expected per difficulty {spans:?}; settled after {settled} \
         adjustments in {:.1} retarget periods",
        settled_at / RETARGET_SECONDS
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_below_and_above_their_target_settle_into_the_share_interval_band() {
    let mut config = StratumConfig {
        startup_difficulty: START_DIFFICULTY,
        ..Default::default()
    };
    config.vardiff.target_seconds = TARGET_SECONDS;
    config.vardiff.retarget_seconds = RETARGET_SECONDS;
    config.vardiff.minimum = 1e-9;
    config.vardiff.maximum = 1e-3;
    config.vardiff.validate().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (_refresh, refresh_rx) = watch::channel(0);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_listener(
        listener,
        config,
        Arc::new(Backend::default()),
        refresh_rx,
        shutdown_rx,
        Arc::new(qbit_prism_server::metrics::Metrics::default()),
    ));
    // Difficulty per second: the fast miner's target is 16x its start, the
    // slow one's 1/16 of it.
    let fast = START_DIFFICULTY * START_ERROR / TARGET_SECONDS;
    let slow = START_DIFFICULTY / START_ERROR / TARGET_SECONDS;
    let (fast_trace, slow_trace) = tokio::join!(
        mine(address, "miner.fast", fast),
        mine(address, "miner.slow", slow)
    );
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
    check("below its target (fast)", &fast_trace, fast);
    check("above its target (slow)", &slow_trace, slow);
}

#[test]
fn settling_is_the_first_difficulty_after_the_last_one_outside_the_band() {
    let target = 1.0;
    assert_eq!(
        settled_from(&[(0.0, 16.0), (1.0, 4.0), (2.0, 1.1)], target),
        Some(2)
    );
    assert_eq!(settled_from(&[(0.0, 1.2)], target), Some(0));
    assert_eq!(
        settled_from(&[(0.0, 16.0), (1.0, 1.0), (2.0, 0.5)], target),
        None
    );
    assert_eq!(
        settled_from(&[(0.0, 16.0), (1.0, 1.0), (2.0, 0.5), (3.0, 0.9)], target),
        Some(3)
    );
}

#[test]
fn a_reversal_is_any_adjustment_against_the_first_ones_direction() {
    assert_eq!(reversal(&[(0.0, 16.0), (1.0, 4.0), (2.0, 1.0)]), None);
    assert_eq!(reversal(&[(0.0, 0.0625), (1.0, 0.25), (2.0, 0.99)]), None);
    assert_eq!(reversal(&[(0.0, 16.0)]), None);
    // Settled at 1x, then back up to 1.4x: inside the band, but a reversal.
    assert_eq!(
        reversal(&[(0.0, 16.0), (1.0, 4.0), (2.0, 1.0), (3.0, 1.4)]),
        Some(3)
    );
    assert_eq!(reversal(&[(0.0, 0.0625), (1.0, 1.2), (2.0, 0.9)]), Some(2));
}

#[test]
fn a_span_short_of_its_pace_is_found_even_when_it_expects_two_shares() {
    let share = |sent: f64, difficulty: f64| Share {
        due: sent,
        sent,
        difficulty,
    };
    // Hashrate 1: a difficulty-0.4 span of 0.8 s expects 2 shares.
    let trace = |shares| Trace {
        difficulties: vec![(0.0, 1.6), (1.0, 0.4), (1.8, 0.1)],
        shares,
        ran_seconds: 2.8,
        ..Default::default()
    };
    let paced = trace(
        (0..10)
            .map(|i| share(1.8 + 0.1 * f64::from(i), 0.1))
            .collect(),
    );
    assert_eq!(pace_by_difficulty(&paced, 1.0)[1], (1.0, 0, 2));
    let full = trace(
        [share(1.4, 0.4), share(1.79, 0.4)]
            .into_iter()
            .chain((1..10).map(|i| share(1.8 + 0.1 * f64::from(i), 0.1)))
            .collect(),
    );
    assert_eq!(pace_by_difficulty(&full, 1.0)[1], (1.0, 2, 2));
    assert_eq!(pace_by_difficulty(&full, 1.0)[2], (1.8, 9, 10));
    assert!(short_of_pace(0, 2), "a stalled two-share window is found");
    assert!(!short_of_pace(1, 2) && !short_of_pace(0, 0) && !short_of_pace(9, 10));
    assert!(
        !short_of_pace(77, 80),
        "the worst span seen under load passes"
    );
    assert!(short_of_pace(63, 80));
}
