//! The session target: a script of Stratum requests and backend events, one
//! per line, driven through one or more connections that share a backend.
//!
//! An optional first line `#cfg key=value ...` sets the listener and backend
//! (`max`, `en2`, `grace`, `retain`, `jobs`, `user_limit`, `mb`, `ab`, `ub`,
//! `vardiff`, `retarget`, `initial_min`, `initial_shares`, `start`, `min`,
//! `bits`, `txs`, `mask`); every value is clamped into its valid range. Other lines are either an event:
//!
//! - `#refresh` a new tip, `#revision` a same-parent payout replacement
//! - `#reconnect` close this connection and open another
//! - `#fail-build`, `#fail-submit` the backend's next call fails
//! - `#pipeline` toggle waiting for each answer before the next request
//! - `#sleep` two milliseconds of real time, so short retention can lapse
//! - `#again` resend the previous frame byte for byte (a duplicate share)
//!
//! or a frame, sent after replacing placeholders with live values, so a
//! mutated script still reaches deep states: `$user` (the authorized
//! username), `$job` / `$job1`..`$job9` (the newest / an older job notified
//! on this connection), `$oldjob` (the newest job of the previous
//! connection), `$en2` (a fresh extranonce2), `$en2prev` (the previous one),
//! `$ntime` (the job's time), `$vbits` (version bits inside the negotiated
//! mask), `$mask` (the listener mask) and `$nonce` (a nonce that meets the
//! job's share target for those fields, when one is near).
//!
//! In the default lockstep mode each frame is followed by a `get_health`
//! barrier and every answer is read before the next line, so placeholders see
//! the state the server has reached.
use crate::{
    alloc,
    backend::FuzzBackend,
    checks::{self, Frame},
    driver::{Conn, Harness},
    pow::{self, RawProof},
    runtime,
};
use qbit_prism_server::{codec, stratum::StratumConfig, vardiff::VardiffConfig};

/// Scripts past this many frames add run time, not coverage.
const MAX_STEPS: usize = 256;
const MAX_SLEEPS: usize = 8;

pub struct Setup {
    pub config: StratumConfig,
    pub bits: u32,
    pub transactions: usize,
}

fn value<T: std::str::FromStr>(v: &str) -> Option<T> {
    v.parse().ok()
}

fn hex_u32(v: &str) -> Option<u32> {
    u32::from_str_radix(v.trim_start_matches("0x"), 16).ok()
}

pub fn setup(line: Option<&str>) -> Setup {
    let mut config = StratumConfig {
        vardiff: VardiffConfig {
            minimum: 1e-9,
            ..Default::default()
        },
        ..Default::default()
    };
    let (mut bits, mut transactions) = (0x207fffffu32, 1usize);
    for token in line.unwrap_or("").split_whitespace() {
        let Some((key, v)) = token.split_once('=') else {
            continue;
        };
        let finite = |d: f64| d.is_finite().then_some(d);
        match key {
            "max" => {
                if let Some(n) = value::<usize>(v) {
                    config.max_message_bytes = n.clamp(256, 65536);
                }
            }
            "en2" => {
                if let Some(n) = value::<usize>(v) {
                    config.extranonce2_size = n.clamp(1, 16);
                }
            }
            "grace" => {
                if let Some(s) = value::<f64>(v).and_then(finite) {
                    config.stale_grace_seconds = s.clamp(0.0, 30.0);
                }
            }
            "retain" => {
                if let Some(s) = value::<f64>(v).and_then(finite) {
                    config.job_retention_seconds = s.clamp(0.001, 60.0);
                }
            }
            "jobs" => {
                if let Some(n) = value::<usize>(v) {
                    config.max_jobs_per_connection = n.clamp(1, 64);
                }
            }
            "user_limit" => {
                if let Some(n) = value::<usize>(v) {
                    config.max_connections_per_username = n.min(4);
                }
            }
            "mb" => config.max_malformed_frames_per_interval = value(v).unwrap_or(0u32).min(16),
            "ab" => config.max_authorize_attempts_per_interval = value(v).unwrap_or(0u32).min(16),
            "ub" => config.max_unknown_jobs_per_interval = value(v).unwrap_or(0u32).min(16),
            "vardiff" => config.vardiff.enabled = v != "0",
            "retarget" => {
                if let Some(s) = value::<f64>(v).and_then(finite) {
                    config.vardiff.retarget_seconds = s.clamp(0.001, 90.0);
                }
            }
            "initial_min" => {
                if let Some(s) = value::<f64>(v).and_then(finite) {
                    config.vardiff.initial_min_seconds = s.clamp(0.001, 10.0);
                }
            }
            "initial_shares" => {
                if let Some(n) = value::<u64>(v) {
                    config.vardiff.initial_min_shares = n.clamp(1, 16);
                }
            }
            "start" => {
                if let Some(d) = value::<f64>(v).and_then(finite).filter(|d| *d > 0.0) {
                    config.startup_difficulty = d.clamp(1e-9, 1024.0);
                }
            }
            "min" => {
                if let Some(d) = value::<f64>(v).and_then(finite).filter(|d| *d >= 0.0) {
                    config.minimum_difficulty = d.min(1024.0);
                }
            }
            "bits" => {
                if let Some(b) = hex_u32(v).filter(|b| codec::target_from_compact(*b).is_ok()) {
                    bits = b;
                }
            }
            "txs" => transactions = value(v).unwrap_or(1usize).min(4),
            "mask" => {
                if let Some(m) = hex_u32(v) {
                    config.version_rolling_mask = m;
                }
            }
            _ => {}
        }
    }
    Setup {
        config,
        bits,
        transactions,
    }
}

/// Values the placeholders resolve to, carried across connections.
#[derive(Default)]
struct Scratch {
    extranonce2: u64,
    previous_jobs: Vec<String>,
    nonce_start: u32,
}

fn substitute(line: &str, conn: &Conn<'_>, backend: &FuzzBackend, scratch: &mut Scratch) -> String {
    if !line.contains('$') {
        return line.to_owned();
    }
    let state = &conn.checker.state;
    let delivered = &state.delivered;
    let nth = |n: usize, jobs: &[String]| {
        jobs.len()
            .checked_sub(n + 1)
            .map_or_else(|| "no-such-job".to_owned(), |i| jobs[i].clone())
    };
    // The job a line names decides the other fields' values.
    let mut job_id = nth(0, delivered);
    let mut out = line.to_owned();
    for n in (1..=9).rev() {
        let key = format!("$job{n}");
        if out.contains(&key) {
            job_id = nth(n, delivered);
            out = out.replace(&key, &job_id);
        }
    }
    if out.contains("$oldjob") {
        job_id = nth(0, &scratch.previous_jobs);
        out = out.replace("$oldjob", &job_id);
    }
    out = out.replace("$job", &nth(0, delivered));
    let width = conn.checker.config.extranonce2_size * 2;
    let mut extranonce2 = format!("{:0width$x}", scratch.extranonce2, width = width);
    extranonce2.truncate(width);
    if out.contains("$en2prev") {
        out = out.replace("$en2prev", &extranonce2);
    }
    if out.contains("$en2") {
        scratch.extranonce2 += 1;
        extranonce2 = format!("{:0width$x}", scratch.extranonce2, width = width);
        extranonce2.truncate(width);
        out = out.replace("$en2", &extranonce2);
    }
    let backend = backend.state();
    let issued = backend.issued.get(&job_id);
    let mask = issued.and_then(|i| i.mask).unwrap_or(state.advertised_mask);
    let vbits = format!("{:08x}", mask.isolate_lowest_one());
    out = out.replace("$vbits", &vbits);
    out = out.replace(
        "$mask",
        &format!("{:08x}", conn.checker.config.version_rolling_mask),
    );
    out = out.replace("$user", state.authorized.as_deref().unwrap_or("miner.fuzz"));
    let ntime = issued.map_or(1_700_000_000, |i| i.job.ntime);
    out = out.replace("$ntime", &format!("{ntime:08x}"));
    if out.contains("$nonce") {
        let nonce = issued
            .and_then(|issued| {
                let proof = RawProof {
                    extranonce2: extranonce2.clone(),
                    ntime: format!("{ntime:08x}"),
                    nonce: String::new(),
                    version_bits: line.contains("$vbits").then(|| vbits.clone()),
                };
                pow::solve(&issued.job, mask, &proof, scratch.nonce_start)
            })
            .unwrap_or_else(|| "00000000".into());
        scratch.nonce_start = scratch.nonce_start.wrapping_add(257);
        out = out.replace("$nonce", &nonce);
    }
    out
}

/// What a script reached: every non-barrier answer as `<id>:<outcome>`, in
/// order across connections, and the shares credited.
#[derive(Debug, Default)]
pub struct Reached {
    pub outcomes: Vec<String>,
    pub credits: usize,
}

pub fn run(data: &[u8]) -> Reached {
    let text = String::from_utf8_lossy(data);
    let mut lines = text.split('\n').peekable();
    let cfg = lines.next_if(|l| l.starts_with("#cfg"));
    let Setup {
        config,
        bits,
        transactions,
    } = setup(cfg);
    let script: Vec<&str> = lines.take(MAX_STEPS).collect();
    let limit = (16 << 20) + 256 * data.len();
    let mut reached = Reached::default();
    alloc::bounded(limit, "a session iteration", || {
        runtime().block_on(async {
            let backend = FuzzBackend::new(config.extranonce2_size, bits, transactions);
            let mut harness = Harness::new(config, backend.clone());
            let mut scratch = Scratch::default();
            let (mut lockstep, mut sleeps, mut barrier) = (true, 0, 0u32);
            let mut previous = Vec::new();
            let mut script = script.into_iter();
            'connections: loop {
                let mut conn = harness.connect();
                let mut reconnect = false;
                for line in script.by_ref() {
                    match line.trim_end_matches('\r') {
                        "#refresh" | "#revision" => {
                            {
                                let mut state = backend.state();
                                if line.starts_with("#refresh") {
                                    state.generation += 1;
                                } else {
                                    state.payout_revision += 1;
                                }
                            }
                            conn.refresh();
                            // Let the loop take the refresh and deliver, then
                            // read the new work before the next placeholder.
                            for _ in 0..16 {
                                tokio::task::yield_now().await;
                            }
                            // A barrier can overtake the refresh, whose wake the
                            // loop's select may take second; retry a few.
                            let state = &conn.checker.state;
                            let expect = state.authorized.is_some() && state.extranonce1.is_some();
                            let before = state.delivered.len();
                            for _ in 0..8 {
                                if !lockstep || !expect || conn.closed() {
                                    break;
                                }
                                barrier += 1;
                                conn.barrier(barrier).await;
                                if conn.checker.state.delivered.len() > before {
                                    break;
                                }
                            }
                        }
                        "#reconnect" => {
                            reconnect = true;
                            break;
                        }
                        "#fail-build" => {
                            let mut state = backend.state();
                            state.fail_builds += 1;
                            state.injected += 1;
                        }
                        "#fail-submit" => {
                            let mut state = backend.state();
                            state.fail_submits += 1;
                            state.injected += 1;
                        }
                        "#pipeline" => lockstep = !lockstep,
                        "#sleep" if sleeps < MAX_SLEEPS => {
                            sleeps += 1;
                            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        }
                        _ => {
                            let bytes = if line.trim_end_matches('\r') == "#again" {
                                std::mem::take(&mut previous)
                            } else {
                                let mut bytes =
                                    substitute(line, &conn, &backend, &mut scratch).into_bytes();
                                bytes.push(b'\n');
                                bytes
                            };
                            previous.clone_from(&bytes);
                            let (frames, oversize) =
                                checks::frames(&bytes, conn.checker.config.max_message_bytes);
                            conn.send(&bytes, frames.iter().map(|f| Frame::classify(f)))
                                .await;
                            if oversize {
                                // Refused and closed; the script goes on
                                // only after a `#reconnect`.
                                conn.read_to_close().await;
                            } else if lockstep {
                                barrier += 1;
                                conn.barrier(barrier).await;
                            } else {
                                conn.drain(64).await;
                            }
                        }
                    }
                }
                scratch.previous_jobs = conn.checker.state.delivered.clone();
                let state = conn.close().await;
                reached.outcomes.extend(
                    state
                        .outcomes
                        .into_iter()
                        .filter(|o| !o.starts_with("fz-barrier-")),
                );
                if !reconnect {
                    break 'connections;
                }
            }
            checks::credits(&backend, &harness.accepted);
            reached.credits = harness.accepted.len();
        })
    });
    reached
}
