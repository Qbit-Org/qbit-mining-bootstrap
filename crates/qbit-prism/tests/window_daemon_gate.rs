//! The frozen payout-window corpus (#269) replayed through the real
//! `qbit-prism-build-audit-bundle --serve` daemon, one fresh process per
//! case: a `full` prepare_window, then one `advance` per delta, with the
//! coordinator's opaque items mirror rebuilt from the raw sections exactly as
//! the coordinator keeps it and checked against every step's digest.
//! Requests are written as JSON text lines from the input documents, so
//! literals wider than any Rust integer reach the daemon verbatim and must be
//! declined as `out_of_range`.
//!
//! Every non-empty window's frozen compact spool tail is then uploaded to the
//! same daemon as a build request, so the daemon's own decoder consumes the
//! frozen bytes; its build summary must equal the one built from the window
//! the daemon prepared from the full records.

#[path = "support/window_corpus.rs"]
mod window_corpus;

use qbit_prism::AcceptedShare;
use serde_json::value::RawValue;
use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use window_corpus::{
    python_json, sha256_hex, spool_tail, Case, Expectation, Mismatches, Outcome, Outputs, Phase,
    Tally,
};

/// Per response; a hung daemon fails the case instead of hanging CI.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);
/// After stdin closes; a daemon with nothing left to read exits at once.
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);
/// Window key of the build that uploads the frozen spool tail. It is no
/// content digest, so the daemon decodes the upload rather than serving the
/// prepared window it already holds.
const SPOOL_UPLOAD_KEY: &str = "frozen-spool-tail";

enum ReadRequest {
    Line,
    Exact(usize),
}

struct Daemon {
    child: Child,
    stdin: Option<Sender<Vec<u8>>>,
    reads: Sender<ReadRequest>,
    responses: Receiver<std::io::Result<Vec<u8>>>,
    stderr: Option<JoinHandle<String>>,
}

impl Daemon {
    fn spawn() -> Daemon {
        let mut child = Command::new(env!("CARGO_BIN_EXE_qbit-prism-build-audit-bundle"))
            .arg("--serve")
            .arg("--signing-key-seed-hex")
            .arg("42".repeat(32))
            .arg("--ledger-signing-key-seed-hex")
            .arg("43".repeat(32))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the serve daemon");

        // Writes and reads each run on a helper thread so the main thread
        // only ever waits with a deadline.
        let mut stdin = child.stdin.take().unwrap();
        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>();
        thread::spawn(move || {
            for bytes in stdin_rx {
                if stdin
                    .write_all(&bytes)
                    .and_then(|()| stdin.flush())
                    .is_err()
                {
                    break;
                }
            }
        });
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let (reads_tx, reads_rx) = mpsc::channel::<ReadRequest>();
        let (responses_tx, responses_rx) = mpsc::channel();
        thread::spawn(move || {
            for request in reads_rx {
                let result = match request {
                    ReadRequest::Line => {
                        let mut line = Vec::new();
                        stdout.read_until(b'\n', &mut line).map(|_| line)
                    }
                    ReadRequest::Exact(len) => {
                        let mut bytes = vec![0u8; len];
                        stdout.read_exact(&mut bytes).map(|()| bytes)
                    }
                };
                if responses_tx.send(result).is_err() {
                    break;
                }
            }
        });
        let mut stderr = child.stderr.take().unwrap();
        let stderr = thread::spawn(move || {
            let mut text = String::new();
            let _ = stderr.read_to_string(&mut text);
            text
        });
        Daemon {
            child,
            stdin: Some(stdin_tx),
            reads: reads_tx,
            responses: responses_rx,
            stderr: Some(stderr),
        }
    }

    fn read(&self, request: ReadRequest) -> Result<Vec<u8>, String> {
        self.reads
            .send(request)
            .map_err(|_| "daemon stdout reader is gone".to_string())?;
        match self.responses.recv_timeout(RESPONSE_TIMEOUT) {
            Ok(Ok(bytes)) => Ok(bytes),
            Ok(Err(error)) => Err(format!("reading daemon stdout: {error}")),
            Err(_) => Err(format!("no daemon response within {RESPONSE_TIMEOUT:?}")),
        }
    }

    fn read_json_line(&self) -> Result<Value, String> {
        let line = self.read(ReadRequest::Line)?;
        if line.is_empty() {
            return Err("daemon closed stdout".to_string());
        }
        serde_json::from_slice(&line)
            .map_err(|error| format!("{error}: {}", String::from_utf8_lossy(&line)))
    }

    /// Send one request line exactly as given and read its response line.
    fn send_line(&self, mut line: Vec<u8>) -> Result<Value, String> {
        line.push(b'\n');
        self.stdin
            .as_ref()
            .expect("stdin is open")
            .send(line)
            .map_err(|_| "daemon stdin writer is gone".to_string())?;
        self.read_json_line()
    }

    fn request(&self, request: &Value) -> Result<Value, String> {
        self.send_line(python_json(request).into_bytes())
    }

    /// Read one raw canonical-items section and its terminating newline.
    fn read_section(&self, len: usize) -> Result<Vec<u8>, String> {
        let mut bytes = self.read(ReadRequest::Exact(len + 1))?;
        if bytes.pop() != Some(b'\n') {
            return Err(format!(
                "raw section of {len} bytes is not newline-terminated"
            ));
        }
        Ok(bytes)
    }

    /// Close stdin and require a clean exit within the timeout.
    fn finish(mut self) -> Result<(), String> {
        drop(self.stdin.take());
        let deadline = Instant::now() + EXIT_TIMEOUT;
        let status = loop {
            match self.child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                Ok(None) => return Err("daemon did not exit after stdin closed".to_string()),
                Err(error) => return Err(format!("waiting for the daemon: {error}")),
            }
        };
        let stderr = self
            .stderr
            .take()
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default();
        if status.success() {
            Ok(())
        } else {
            Err(format!("daemon exited with {status}; stderr: {stderr}"))
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn usize_field(response: &Value, field: &str) -> Result<usize, String> {
    response[field]
        .as_u64()
        .map(|value| value as usize)
        .ok_or_else(|| format!("response has no {field}: {response}"))
}

fn str_field(response: &Value, field: &str) -> Result<String, String> {
    response[field]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| format!("response has no {field}: {response}"))
}

/// A response that is not `ok`: the three distinguishable prepare_window
/// outcomes, or a failure.
fn refusal(response: &Value, phase: Phase) -> Result<Outcome, String> {
    let message = response["error"].as_str().unwrap_or_default().to_string();
    let category = response["rejection"].as_str().map(str::to_string);
    if response["out_of_range"] == true {
        return Ok(Outcome::Declined {
            field: str_field(response, "field")?,
            width: str_field(response, "width")?,
            detail: message,
        });
    }
    let expected_flag = match phase {
        Phase::Full => "fold_invalid",
        Phase::Advance(_) => "fallback",
    };
    if response[expected_flag] == true {
        return Ok(Outcome::Rejected {
            phase,
            category,
            message,
        });
    }
    Err(format!("unexpected response at {phase}: {response}"))
}

/// The coordinator trusts its mirror only while it hashes to the digest the
/// daemon reports; hold every step to that.
fn check_mirror_digest(
    mismatches: &mut Mismatches,
    case: &Case,
    step: Phase,
    items: &[u8],
    digest: &str,
) {
    let mut framed = Vec::with_capacity(items.len() + 2);
    framed.push(b'[');
    framed.extend_from_slice(items);
    framed.push(b']');
    let mirrored = sha256_hex(&framed);
    if mirrored != digest {
        mismatches.push(
            &case.name,
            &format!("{step} mirror digest"),
            format!("share_snapshot_sha256 {digest}, sha256 of the mirrored items {mirrored}"),
        );
    }
}

fn exchange(daemon: &Daemon, case: &Case, mismatches: &mut Mismatches) -> Result<Outcome, String> {
    let handshake = daemon.read_json_line()?;
    if handshake["event"] != "handshake" || handshake["protocol"] != 2 {
        return Err(format!("unexpected handshake: {handshake}"));
    }

    let full = daemon.request(&case.full_request())?;
    if full["ok"] != true {
        return refusal(&full, Phase::Full);
    }
    let mut items = daemon.read_section(usize_field(&full, "window_items_len")?)?;
    let mut digest = str_field(&full, "share_snapshot_sha256")?;
    let mut record_count = usize_field(&full, "record_count")?;
    check_mirror_digest(mismatches, case, Phase::Full, &items, &digest);

    let mut advance_stats = Vec::new();
    for index in 0..case.advances().len() {
        let response = daemon.request(&case.advance_request(index, &digest))?;
        if response["ok"] != true {
            return refusal(&response, Phase::Advance(index));
        }
        let appended = daemon.read_section(usize_field(&response, "appended_items_len")?)?;
        let drop_bytes = usize_field(&response, "retained_drop_bytes")?;
        if drop_bytes > items.len() {
            return Err(format!(
                "advances[{index}] retained_drop_bytes {drop_bytes} exceeds the {}-byte mirror",
                items.len()
            ));
        }
        items.drain(..drop_bytes);
        items.extend_from_slice(&appended);
        digest = str_field(&response, "share_snapshot_sha256")?;
        record_count = usize_field(&response, "record_count")?;
        check_mirror_digest(mismatches, case, Phase::Advance(index), &items, &digest);
        advance_stats.push([
            usize_field(&response, "added_rows")?,
            usize_field(&response, "expired_rows")?,
            usize_field(&response, "touched_pages")?,
        ]);
    }

    // Split the mirrored stream into its records straight from the daemon's
    // bytes: RawValue keeps each element's exact span.
    let mut canonical = Vec::with_capacity(items.len() + 2);
    canonical.push(b'[');
    canonical.extend_from_slice(&items);
    canonical.push(b']');
    let elements: Vec<Box<RawValue>> = serde_json::from_slice(&canonical)
        .map_err(|error| format!("mirrored items stream does not parse: {error}"))?;
    let shares: Vec<AcceptedShare> = elements
        .iter()
        .map(|element| serde_json::from_str(element.get()))
        .collect::<Result<_, _>>()
        .map_err(|error| format!("mirrored record does not parse: {error}"))?;
    Ok(Outcome::Outputs(Box::new(Outputs {
        record_count,
        canonical_items: items,
        canonical_digest: digest,
        fragments: elements
            .iter()
            .map(|element| element.get().as_bytes().to_vec())
            .collect(),
        spool_tail: spool_tail(&shares),
        advance_stats,
    })))
}

fn excerpt(value: &Value) -> String {
    let text = value.to_string();
    match text.char_indices().nth(160) {
        Some((cut, _)) => format!("{}...", &text[..cut]),
        None => text,
    }
}

/// The first JSON path at which two build summaries differ.
fn first_difference(prepared: &Value, decoded: &Value, path: &str) -> Option<String> {
    match (prepared, decoded) {
        (Value::Object(left), Value::Object(right)) => {
            left.keys().chain(right.keys()).find_map(|key| {
                first_difference(
                    left.get(key).unwrap_or(&Value::Null),
                    right.get(key).unwrap_or(&Value::Null),
                    &format!("{path}.{key}"),
                )
            })
        }
        (Value::Array(left), Value::Array(right)) if left.len() == right.len() => left
            .iter()
            .zip(right)
            .enumerate()
            .find_map(|(index, (left, right))| {
                first_difference(left, right, &format!("{path}[{index}]"))
            }),
        _ if prepared == decoded => None,
        _ => Some(format!(
            "{path}: prepared window {}, frozen spool tail {}",
            excerpt(prepared),
            excerpt(decoded)
        )),
    }
}

/// Build the case's window twice in the same daemon and require identical
/// summaries: once from the window the daemon prepared from the full records,
/// once from the frozen compact spool tail through the daemon's own upload
/// decoder. The summary's reward manifest lists every counted share with
/// each field the compact format carries (`share_seq`, `share_id`,
/// `miner_id`, `order_key`, `p2mr_program_hex`, `share_difficulty`,
/// `job_issued_at_ms`, `accepted_at_ms`, `credit_policy`) and none it does
/// not, so a decoder that drops, swaps or misindexes a field changes it.
/// Returns false for an empty window, which has no compact rows to upload.
fn check_spool_decoder(daemon: &Daemon, case: &Case, outputs: &Outputs) -> Result<bool, String> {
    if outputs.record_count == 0 {
        return Ok(false);
    }
    let tail = case.frozen_spool_tail(&outputs.spool_tail).ok_or(
        "the entry pins no spool_tail literal and the computed tail does not hash to the \
         frozen spool_tail_sha256",
    )?;
    let shares: Vec<AcceptedShare> = outputs
        .fragments
        .iter()
        .map(|fragment| serde_json::from_slice(fragment))
        .collect::<Result<_, _>>()
        .map_err(|error| format!("mirrored record does not parse: {error}"))?;
    let oldest = shares
        .iter()
        .min_by_key(|share| share.share_seq)
        .expect("a non-empty window has an oldest share");
    let newer_difficulty = shares
        .iter()
        .filter(|share| share.share_seq != oldest.share_seq)
        .try_fold(0u128, |sum, share| sum.checked_add(share.share_difficulty))
        .ok_or("window difficulty total leaves u128")?;
    // The final anchor admits every retained share. A requested weight
    // (eight times the network difficulty) just above the newer shares'
    // total counts each of them in full and the oldest one at least in
    // part, so every share is listed, while the payout arithmetic stays far
    // inside u128 even when the oldest share is 2^127 wide.
    let found_block = format!(
        "{{\"anchor_job_issued_at_ms\":{},\"block_height\":1,\
         \"coinbase_value_sats\":5000000000,\"network_difficulty\":{}}}",
        python_json(case.final_anchor()),
        newer_difficulty / 8 + 1
    );

    let prepared = daemon.send_line(
        format!(
            "{{\"found_block\":{found_block},\"window_key\":{{\"share_snapshot_sha256\":\"{}\"}}}}",
            outputs.canonical_digest
        )
        .into_bytes(),
    )?;
    if prepared["ok"] != true || prepared["window_cache"]["hit"] != true {
        return Err(format!(
            "build from the prepared window: {}",
            excerpt(&prepared)
        ));
    }
    // The frozen tail is the request line's own suffix, closing brace
    // included, exactly as the 2.x.x coordinator spooled it.
    let mut upload = format!(
        "{{\"found_block\":{found_block},\"window_key\":{{\"share_snapshot_sha256\":\
         \"{SPOOL_UPLOAD_KEY}\"}}"
    )
    .into_bytes();
    upload.extend_from_slice(&tail);
    let decoded = daemon.send_line(upload)?;
    if decoded["ok"] != true || decoded["window_cache"]["hit"] != false {
        return Err(format!(
            "build from the frozen spool tail: {}",
            excerpt(&decoded)
        ));
    }
    if prepared["summary"] == decoded["summary"] {
        Ok(true)
    } else {
        Err(
            first_difference(&prepared["summary"], &decoded["summary"], "summary")
                .unwrap_or_else(|| "build summaries differ".to_string()),
        )
    }
}

fn replay_all(what: &str, cases: &[Case], expected: Tally) {
    let mut mismatches = Mismatches::default();
    let mut tally = Tally::default();
    let mut spool_decoded = 0;
    for case in cases {
        let daemon = Daemon::spawn();
        let outcome = exchange(&daemon, case, &mut mismatches);
        if let Ok(Outcome::Outputs(outputs)) = &outcome {
            match check_spool_decoder(&daemon, case, outputs) {
                Ok(true) => spool_decoded += 1,
                Ok(false) => {}
                Err(error) => mismatches.push(&case.name, "frozen spool tail decode", error),
            }
        }
        // Every outcome, rejections and declines included, leaves a daemon
        // that exits cleanly once stdin closes.
        if let Err(error) = daemon.finish() {
            mismatches.push(&case.name, "daemon exit", error);
        }
        match outcome {
            Ok(outcome) => mismatches.settle(case, outcome, &mut tally),
            Err(error) => {
                tally.cases += 1;
                mismatches.push(&case.name, "daemon exchange", error);
            }
        }
    }
    // Every frozen non-empty window's spool tail must reach the decoder.
    let decodable = cases
        .iter()
        .filter(|case| {
            matches!(case.expectation(), Expectation::Outputs)
                && case.entry["record_count"]
                    .as_u64()
                    .is_some_and(|count| count > 0)
        })
        .count();
    println!(
        "{what}: {spool_decoded} of {decodable} frozen spool tails decoded by the daemon and \
         built identically to its prepared window"
    );
    if spool_decoded != decodable {
        mismatches.push(
            "corpus",
            "spool tails decoded",
            format!("expected {decodable}, got {spool_decoded}"),
        );
    }
    mismatches.finish(what, tally, expected);
}

#[test]
fn frozen_corpus_replays_through_the_serve_daemon() {
    replay_all(
        "window_daemon_gate",
        &window_corpus::load(),
        window_corpus::EXPECTED_TALLY,
    );
}

#[test]
fn supplementary_cases_replay_through_the_serve_daemon() {
    replay_all(
        "window_daemon_gate supplementary",
        &window_corpus::load_supplementary(),
        window_corpus::EXPECTED_SUPPLEMENTARY_TALLY,
    );
}
