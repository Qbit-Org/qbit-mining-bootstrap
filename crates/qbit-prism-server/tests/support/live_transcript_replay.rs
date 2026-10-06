//! #575 item 3: replay recorded Stratum v1 client transcripts against a live
//! regtest server and check that every answer is one the client accepts.
//!
//! A transcript is a JSON-lines file in `tests/fixtures/stratum_transcripts`
//! (see the README there): a header line, then one line per message with its
//! `direction` (`c2s` from the miner, `s2c` from the pool it was recorded
//! against), a `timestamp` in seconds from the start, and the `message`.
//! The client's lines are replayed in order on one connection; the pool's
//! lines are the expectations.
//!
//! Replay rewrites only what the recorded pool chose and the live one must
//! choose again: the payout address in worker names becomes the fixture's
//! regtest address (the worker suffix is kept), and a submit the recorded
//! pool accepted is solved again on the live server's latest job, with its
//! extranonce2 size, share difficulty and negotiated version-rolling mask,
//! keeping the recorded shape (five fields, or six with version bits). It is
//! a share that is not a block unless the session's share difficulty is at
//! the network's, as a large `mining.suggest_difficulty` makes it on regtest.
//! Each transcript starts once every block found earlier has settled and the
//! server serves the node's tip at the cluster's payout revision; a re-solved
//! submit refused as `stale-job` or `unknown-job` (its job superseded in
//! flight) is solved again on newer work, up to three times. A
//! submit the recorded pool refused is sent as recorded. Timestamps order the
//! lines; replay does not wait out recorded gaps.
//!
//! Compatibility, per request method: `mining.subscribe` answers
//! `[subscriptions, extranonce1, extranonce2_size]`; `mining.configure`
//! answers every requested extension with a boolean, and grants version
//! rolling, within the requested mask, when the recorded pool did;
//! `mining.authorize`, `mining.extranonce.subscribe`,
//! `mining.suggest_difficulty` and `mining.submit` succeed when the recorded
//! pool's did and fail when it failed; any other method fails or succeeds as
//! it did. Every response carries `id`, `result` and `error`. Every
//! `mining.notify`, `mining.set_difficulty` and `mining.set_version_mask`
//! the live server sends has the shape miners parse, the first difficulty
//! precedes the first job, and each kind of work notification the recording
//! holds is sent live. Every accepted replayed share is in the ledger.
use super::share_client::{is_hex, reason_id, start_share_only_servers, StratumSession, ANSWER};
use super::*;
use qbit_prism_server::codec::parse_u32_hex;
use std::{collections::BTreeSet, path::Path};

/// The corpus, relative to this package.
const CORPUS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/stratum_transcripts"
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    ClientToServer,
    ServerToClient,
}

struct Line {
    direction: Direction,
    timestamp: f64,
    message: Value,
}

struct Transcript {
    name: String,
    source: String,
    lines: Vec<Line>,
}

impl Transcript {
    /// Parse and check one transcript file.
    fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let mut rows = text
            .lines()
            .enumerate()
            .filter(|(_, row)| !row.trim().is_empty());
        let (_, header) = rows.next().context("empty transcript")?;
        let header: Value = serde_json::from_str(header)?;
        let header = &header["transcript"];
        let name = header["name"]
            .as_str()
            .context("header needs transcript.name")?
            .to_owned();
        let stem = path.file_stem().and_then(|stem| stem.to_str());
        ensure!(
            stem == Some(name.as_str()),
            "transcript.name must match the file name"
        );
        let source = header["source"]
            .as_str()
            .context("header needs transcript.source")?
            .to_owned();
        ensure!(
            source == "synthetic" || source == "captured",
            "transcript.source must be \"synthetic\" or \"captured\", not {source:?}"
        );
        for field in ["dialect", "notes"] {
            ensure!(
                header[field]
                    .as_str()
                    .is_some_and(|value| !value.is_empty()),
                "header needs transcript.{field}"
            );
        }
        let mut lines = Vec::new();
        let mut previous = 0.0f64;
        for (number, row) in rows {
            let parsed: Value = serde_json::from_str(row)
                .with_context(|| format!("line {}: not JSON", number + 1))?;
            let direction = match parsed["direction"].as_str() {
                Some("c2s") => Direction::ClientToServer,
                Some("s2c") => Direction::ServerToClient,
                other => bail!("line {}: direction {other:?} is not c2s or s2c", number + 1),
            };
            let timestamp = parsed["timestamp"]
                .as_f64()
                .with_context(|| format!("line {}: timestamp must be seconds", number + 1))?;
            ensure!(
                timestamp >= previous,
                "line {}: timestamp {timestamp} precedes {previous}",
                number + 1
            );
            previous = timestamp;
            let message = parsed["message"].clone();
            ensure!(
                message.is_object(),
                "line {}: message must be an object",
                number + 1
            );
            lines.push(Line {
                direction,
                timestamp,
                message,
            });
        }
        let transcript = Self {
            name,
            source,
            lines,
        };
        // Every client request with an id has the recorded answer that is
        // its expectation.
        for line in transcript.client_lines() {
            if !line.message["id"].is_null() && line.message.get("method").is_some() {
                transcript
                    .recorded_answer(&line.message["id"])
                    .with_context(|| format!("request {} has no recorded answer", line.message))?;
            }
        }
        Ok(transcript)
    }

    fn client_lines(&self) -> impl Iterator<Item = &Line> {
        self.lines
            .iter()
            .filter(|line| line.direction == Direction::ClientToServer)
    }

    fn recorded_answer(&self, id: &Value) -> Option<&Value> {
        self.lines
            .iter()
            .filter(|line| line.direction == Direction::ServerToClient)
            .map(|line| &line.message)
            .find(|message| message.get("method").is_none() && &message["id"] == id)
    }

    /// The notification methods the recorded pool sent.
    fn recorded_notifications(&self) -> BTreeSet<String> {
        self.lines
            .iter()
            .filter(|line| line.direction == Direction::ServerToClient)
            .filter_map(|line| line.message["method"].as_str().map(str::to_owned))
            .collect()
    }
}

fn corpus() -> Result<Vec<Transcript>> {
    let mut paths: Vec<_> = std::fs::read_dir(CORPUS)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<_>>()?;
    paths.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "jsonl")
    });
    paths.sort();
    ensure!(!paths.is_empty(), "no transcripts in {CORPUS}");
    paths
        .iter()
        .map(|path| Transcript::load(path).with_context(|| format!("{}", path.display())))
        .collect()
}

/// The corpus parses and every file is labelled; runs without any input.
#[test]
fn stratum_transcript_corpus_is_well_formed_and_labelled() -> Result<()> {
    let transcripts = corpus()?;
    for transcript in &transcripts {
        ensure!(
            transcript.client_lines().count() > 0,
            "{}: no client lines",
            transcript.name
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recorded_stratum_transcripts_replay_compatibly_against_a_live_server() -> Result<()> {
    let transcripts = corpus()?;
    let Some(mut fixture) = Fixture::open_with_servers(false, false).await? else {
        return Ok(());
    };
    let result = async {
        start_share_only_servers(&mut fixture, &[(0, Vec::new())]).await?;
        let mut accepted = Vec::new();
        let mut report = Vec::new();
        for transcript in &transcripts {
            let replayed = replay(&fixture, transcript).await.with_context(|| {
                format!("transcript {} ({})", transcript.name, transcript.source)
            })?;
            report.push(format!(
                "{} ({}, {:.1}s recorded): {} requests, {} shares accepted{}",
                transcript.name,
                transcript.source,
                transcript.lines.last().map_or(0.0, |line| line.timestamp),
                replayed.requests,
                replayed.accepted.len(),
                if replayed.not_sent.is_empty() {
                    String::new()
                } else {
                    format!("; recorded but not sent live: {:?}", replayed.not_sent)
                }
            ));
            accepted.extend(replayed.accepted);
        }
        let ledger: BTreeSet<String> =
            sqlx::query_scalar("SELECT share_id FROM qbit_share_ledger WHERE accepted")
                .fetch_all(&fixture.pool)
                .await?
                .into_iter()
                .collect();
        let missing: Vec<_> = accepted.iter().filter(|id| !ledger.contains(*id)).collect();
        ensure!(
            missing.is_empty(),
            "accepted replayed shares missing: {missing:?}"
        );
        eprintln!("transcript replay:\n  {}", report.join("\n  "));
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

/// What one transcript's replay did.
struct Replayed {
    requests: usize,
    /// Share ids of accepted replayed submits.
    accepted: Vec<String>,
    /// Notification methods the recorded pool sent that the live one did not.
    not_sent: BTreeSet<String>,
}

/// The fixture's regtest address with the recorded worker suffix, which
/// keeps only characters a worker name needs.
fn live_username(fixture: &Fixture, recorded: &Value) -> String {
    let suffix = recorded
        .as_str()
        .and_then(|name| name.rsplit_once('.'))
        .map(|(_, worker)| worker)
        .filter(|worker| {
            !worker.is_empty()
                && worker
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        })
        .unwrap_or("replay");
    format!("{}.{suffix}", fixture.address)
}

/// How often a re-solved submit refused as superseded is solved again.
const STALE_RETRIES: usize = 3;

/// Whether a refusal says the job was superseded, not that the share or the
/// dialect was wrong.
fn superseded(answer: &Value) -> bool {
    matches!(reason_id(answer), Some("stale-job" | "unknown-job"))
}

/// Until the server is ready on the node's current tip, so a transcript
/// starts on current work: a block a previous transcript found is built on,
/// and has landed, so a recorded accepted submit cannot meet the revision
/// fence ([`Fixture::settled`]).
async fn current(fixture: &Fixture) -> Result<()> {
    fixture.settled(0, 30).await
}

async fn replay(fixture: &Fixture, transcript: &Transcript) -> Result<Replayed> {
    current(fixture).await?;
    let mut live = StratumSession::open(fixture.stratum[0]).await?;
    let mut replayed = Replayed {
        requests: 0,
        accepted: Vec::new(),
        not_sent: BTreeSet::new(),
    };
    let mut username = None;
    for line in transcript.client_lines() {
        let recorded = &line.message;
        let method = recorded["method"].as_str().unwrap_or_default();
        let id = recorded["id"].clone();
        let expected = transcript.recorded_answer(&id).cloned();
        let mut message = recorded.clone();
        // Set for a submit the recorded pool accepted: the worker name and
        // whether it rolls versions. It is solved again on live work.
        let mut resolve = None;
        match method {
            "mining.authorize" => {
                let name = live_username(fixture, &recorded["params"][0]);
                message["params"][0] = json!(name);
                username = Some(name);
            }
            "mining.submit" => {
                let name = username
                    .clone()
                    .unwrap_or_else(|| live_username(fixture, &recorded["params"][0]));
                message["params"][0] = json!(name);
                if expected
                    .as_ref()
                    .is_some_and(|answer| answer["result"] == true)
                {
                    let rolls = recorded["params"].as_array().map_or(0, Vec::len) >= 6;
                    resolve = Some((name, rolls));
                }
            }
            _ => {}
        }
        let mut share_id = None;
        if let Some((name, rolls)) = &resolve {
            let (params, submitted) = live.solve_latest(name, None, *rolls).await?;
            message["params"] = params;
            share_id = Some(submitted.share_id);
        }
        live.send(&message).await?;
        if id.is_null() {
            continue;
        }
        replayed.requests += 1;
        let mut answer = live.answer(&id, ANSWER).await?;
        // A re-solved submit whose job a new tip superseded in flight is
        // solved again on the newer work, as a miner's next share would be.
        if let Some((name, rolls)) = &resolve {
            for _ in 0..STALE_RETRIES {
                if !superseded(&answer) {
                    break;
                }
                live.newer_work().await?;
                let (params, submitted) = live.solve_latest(name, None, *rolls).await?;
                message["params"] = params;
                share_id = Some(submitted.share_id);
                live.send(&message).await?;
                answer = live.answer(&id, ANSWER).await?;
            }
        }
        let expected = expected.context("no recorded answer")?;
        compatible(method, recorded, &expected, &answer)
            .with_context(|| format!("{method} answered {answer}, recorded {expected}"))?;
        live.learn(method, &answer)?;
        if let Some(share_id) = share_id {
            replayed.accepted.push(share_id);
        }
    }
    // Work the recording received must be sent live too, difficulty first.
    let recorded = transcript.recorded_notifications();
    if recorded.contains("mining.notify") {
        live.work().await?;
    }
    let position = |method: &str| live.notifications.iter().position(|sent| sent == method);
    if let (Some(difficulty), Some(job)) =
        (position("mining.set_difficulty"), position("mining.notify"))
    {
        ensure!(
            difficulty < job,
            "the first mining.notify preceded the first mining.set_difficulty"
        );
    }
    for method in &recorded {
        if !live.notifications.contains(method) {
            ensure!(
                !matches!(method.as_str(), "mining.notify" | "mining.set_difficulty"),
                "the live server never sent {method}"
            );
            replayed.not_sent.insert(method.clone());
        }
    }
    Ok(replayed)
}

/// Whether the live `answer` to `request` is one the recorded client, which
/// got `expected`, accepts. See the module documentation.
fn compatible(method: &str, request: &Value, expected: &Value, answer: &Value) -> Result<()> {
    let object = answer.as_object().context("answer is not an object")?;
    ensure!(
        object.contains_key("result") && object.contains_key("error"),
        "answer lacks result or error"
    );
    let succeeded = |message: &Value| message["error"].is_null() && message["result"] != false;
    let result = &answer["result"];
    match method {
        "mining.subscribe" => {
            let parts = result
                .as_array()
                .context("subscribe result is not an array")?;
            ensure!(parts.len() >= 3, "subscribe result has fewer than 3 parts");
            ensure!(parts[0].is_array(), "subscriptions are not an array");
            ensure!(is_hex(&parts[1], None), "extranonce1 is not hex");
            ensure!(
                parts[2]
                    .as_u64()
                    .is_some_and(|size| (1..=16).contains(&size)),
                "extranonce2_size is not 1..=16"
            );
        }
        "mining.configure" => {
            ensure!(answer["error"].is_null(), "configure failed");
            let granted = result
                .as_object()
                .context("configure result is not an object")?;
            for extension in request["params"][0].as_array().into_iter().flatten() {
                let extension = extension.as_str().context("extension is not a string")?;
                ensure!(
                    granted.get(extension).is_some_and(Value::is_boolean),
                    "{extension} not answered with a boolean"
                );
            }
            if expected["result"]["version-rolling"] == true {
                ensure!(
                    result["version-rolling"] == true,
                    "version rolling refused where the recorded pool granted it"
                );
                ensure!(
                    is_hex(&result["version-rolling.mask"], Some(8)),
                    "mask is not 8 hex digits"
                );
                let mask =
                    parse_u32_hex(result["version-rolling.mask"].as_str().unwrap_or_default())?;
                let requested = request["params"][1]["version-rolling.mask"]
                    .as_str()
                    .map_or(Ok(u32::MAX), parse_u32_hex)?;
                ensure!(mask != 0, "an empty version-rolling mask");
                ensure!(
                    mask & !requested == 0,
                    "mask {mask:08x} exceeds the requested {requested:08x}"
                );
            }
        }
        _ => {
            ensure!(
                succeeded(answer) == succeeded(expected),
                "{} where the recorded pool {}",
                if succeeded(answer) {
                    "succeeded"
                } else {
                    "failed"
                },
                if succeeded(expected) {
                    "succeeded"
                } else {
                    "failed"
                }
            );
            if matches!(
                method,
                "mining.authorize" | "mining.submit" | "mining.extranonce.subscribe"
            ) && expected["result"] == true
            {
                ensure!(*result == true, "result is not true");
            }
        }
    }
    Ok(())
}
