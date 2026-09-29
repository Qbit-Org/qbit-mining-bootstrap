//! What a correct connection may say. The frame model predicts, from the
//! bytes sent alone, which frames the server must answer; the checks then hold
//! every answer and notification to the documented protocol.
use crate::{backend::FuzzBackend, pow::RawProof, violation};
use qbit_prism_server::stratum::StratumConfig;
use serde_json::Value;

/// Reason IDs the native server may put on the wire (docs/prism-rejections.md).
pub const DOCUMENTED_REASONS: &[&str] = &[
    "stale-job",
    "duplicate-share",
    "low-difficulty",
    "malformed-submit",
    "unauthorized-worker",
    "unknown-job",
    "invalid-extranonce",
    "invalid-ntime-or-nonce",
    "backend-rpc-unavailable",
    "internal-error",
    "pool-closed",
    "ledger-confirmation-failed",
    "ledger-outcome-unknown",
];

/// The documented code for a reason ID (the coordinator's `protocol_error`).
pub fn code_for(reason: &str) -> i64 {
    match reason {
        "stale-job" | "unknown-job" | "pool-closed" => 21,
        "duplicate-share" => 22,
        "low-difficulty" => 23,
        _ => 20,
    }
}

/// Refusals that carry no reason ID: admission and per-session budgets.
const REASONLESS: &[&str] = &[
    "too many authorization attempts",
    "too many unknown job submissions",
    "too many connections for username",
];

/// One newline-terminated frame as the server must see it.
#[derive(Clone, Debug)]
pub enum Frame {
    /// Not a JSON object: answered with a null-id `malformed-submit` error.
    Invalid,
    /// A JSON object: answered exactly once with its own id.
    Request(Value),
}

impl Frame {
    /// Classify with the server's own parser; this models framing, not JSON.
    pub fn classify(bytes: &[u8]) -> Self {
        match serde_json::from_slice::<Value>(bytes) {
            Ok(value) if value.is_object() => Frame::Request(value),
            _ => Frame::Invalid,
        }
    }
}

/// Split a byte stream the way the server frames it under `max` bytes per
/// message: complete frames, then whether the stream ends in an oversize
/// frame (which closes the connection). A final partial frame within the
/// bound is dropped at end of stream, as the server drops it.
pub fn frames(stream: &[u8], max: usize) -> (Vec<Vec<u8>>, bool) {
    let mut frames = Vec::new();
    let mut start = 0;
    while start < stream.len() {
        match stream[start..].iter().position(|&b| b == b'\n') {
            Some(end) if end < max => {
                frames.push(stream[start..start + end + 1].to_vec());
                start += end + 1;
            }
            Some(_) => return (frames, true),
            None => return (frames, stream.len() - start > max),
        }
    }
    (frames, false)
}

/// The per-connection state the answers establish, in the order the server
/// processes frames.
#[derive(Default)]
pub struct ConnState {
    pub extranonce1: Option<String>,
    pub authorized: Option<String>,
    /// Jobs notified on this connection, oldest first.
    pub delivered: Vec<String>,
    pub difficulty: Option<f64>,
    pub advertised_mask: u32,
    /// The mask the miner last asked for with `mining.configure`.
    pub miner_mask: Option<u32>,
    pub malformed: u32,
    pub authorizes: u32,
    /// Why the server may close: the oversize answer, or a spent budget.
    pub closing: Option<&'static str>,
    pub answered: usize,
    /// `<id>:<outcome>` per answer: `ok`, the reason ID or the reason-less
    /// message. What a seed reached, for the seed tests.
    pub outcomes: Vec<String>,
}

/// A submit the server answered `true`, with what the miner sent.
#[derive(Clone, Debug)]
pub struct Accepted {
    pub job_id: String,
    pub username: String,
    pub proof: RawProof,
    /// Jobs notified on the connection before this submit was processed.
    pub delivered_before: Vec<String>,
}

pub struct Checker<'a> {
    pub config: &'a StratumConfig,
    pub backend: &'a FuzzBackend,
    pub state: ConnState,
    pub accepted: &'a mut Vec<Accepted>,
}

fn is_hex(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|b| b.is_ascii_hexdigit())
}

impl Checker<'_> {
    /// Check one line the server wrote. `pending` is the oldest frame still
    /// owed an answer.
    pub fn line(&mut self, line: &[u8], pending: Option<&Frame>) -> bool {
        let limit = 4 * self.config.max_message_bytes + 16 * 1024;
        if line.len() > limit {
            violation(format!(
                "a {}-byte line exceeds the {limit}-byte bound",
                line.len()
            ));
        }
        if self.state.closing.is_some() {
            violation(format!(
                "the server wrote after its closing answer: {}",
                String::from_utf8_lossy(line)
            ));
        }
        let value: Value = serde_json::from_slice(line)
            .unwrap_or_else(|e| violation(format!("the server wrote invalid JSON ({e})")));
        if let Some(method) = value.get("method") {
            self.notification(method, &value);
            return false;
        }
        let Some(frame) = pending else {
            // Only the oversize refusal answers no complete frame.
            self.oversize(&value);
            return false;
        };
        self.answer(frame, &value);
        self.state.answered += 1;
        true
    }

    /// The single answer to an oversize frame, after which the server closes.
    pub fn oversize(&mut self, value: &Value) {
        let error = self.error(value, &Value::Null);
        if error != Some(("malformed-submit", "Stratum message exceeds size limit")) {
            violation(format!("unexpected unsolicited line {value}"));
        }
        self.state.closing = Some("oversize");
    }

    fn notification(&mut self, method: &Value, value: &Value) {
        if value.get("id") != Some(&Value::Null) {
            violation(format!("notification with an id: {value}"));
        }
        let params = value["params"]
            .as_array()
            .unwrap_or_else(|| violation(format!("notification params: {value}")));
        match method.as_str() {
            Some("mining.set_difficulty") => {
                let difficulty = params
                    .first()
                    .and_then(Value::as_f64)
                    .filter(|d| params.len() == 1 && d.is_finite() && *d > 0.0)
                    .unwrap_or_else(|| violation(format!("set_difficulty: {value}")));
                self.state.difficulty = Some(difficulty);
            }
            Some("mining.set_version_mask") => {
                let mask = params
                    .first()
                    .and_then(Value::as_str)
                    .filter(|m| params.len() == 1 && is_hex(m, 8))
                    .and_then(|m| u32::from_str_radix(m, 16).ok())
                    .unwrap_or_else(|| violation(format!("set_version_mask: {value}")));
                if mask & !self.config.version_rolling_mask != 0
                    || self.state.miner_mask.is_none_or(|miner| mask & !miner != 0)
                {
                    violation(format!("advertised mask {mask:08x} was never negotiated"));
                }
                self.state.advertised_mask = mask;
            }
            Some("mining.notify") => self.notify(params, value),
            _ => violation(format!("unknown notification {value}")),
        }
    }

    fn notify(&mut self, params: &[Value], value: &Value) {
        if self.state.authorized.is_none() || self.state.extranonce1.is_none() {
            violation(format!("work before subscribe and authorize: {value}"));
        }
        if params.len() != 9 {
            violation(format!("notify arity: {value}"));
        }
        let job_id = params[0]
            .as_str()
            .unwrap_or_else(|| violation(format!("notify job id: {value}")));
        let backend = self.backend.state();
        let issued = backend
            .issued
            .get(job_id)
            .unwrap_or_else(|| violation(format!("notified job {job_id} was never built")));
        if Some(&issued.job.extranonce1) != self.state.extranonce1.as_ref() {
            violation(format!("job {job_id} was built for another connection"));
        }
        let expected = issued.job.notify();
        if params[..8] != expected["params"].as_array().unwrap()[..8] || !params[8].is_boolean() {
            violation(format!(
                "notify {value} differs from the built job {expected}"
            ));
        }
        // Difficulty precedes the work it applies to.
        if self.state.difficulty != Some(issued.job.share_difficulty) {
            violation(format!(
                "job {job_id} at difficulty {} followed set_difficulty {:?}",
                issued.job.share_difficulty, self.state.difficulty
            ));
        }
        self.state.delivered.push(job_id.to_owned());
    }

    /// The `(reason, message)` of an error answer with the given id, or
    /// `None` for a success; any other shape is a violation.
    fn error<'v>(&self, value: &'v Value, id: &Value) -> Option<(&'v str, &'v str)> {
        let object = value
            .as_object()
            .filter(|o| o.len() == 3 && o.contains_key("result") && o.contains_key("error"))
            .unwrap_or_else(|| violation(format!("answer shape: {value}")));
        if object.get("id") != Some(id) {
            violation(format!("answer {value} does not carry the request id {id}"));
        }
        let error = &object["error"];
        if error.is_null() {
            if object["result"].is_null() {
                violation(format!("answer with neither result nor error: {value}"));
            }
            return None;
        }
        if !object["result"].is_null() {
            violation(format!("answer with both result and error: {value}"));
        }
        let (code, message, data) = match error.as_array().map(Vec::as_slice) {
            Some([code, message, data]) => (code, message, data),
            _ => violation(format!("error shape: {value}")),
        };
        let code = code
            .as_i64()
            .unwrap_or_else(|| violation(format!("error code: {value}")));
        let message = message
            .as_str()
            .unwrap_or_else(|| violation(format!("error message: {value}")));
        if data.is_null() {
            if code != 20 || !REASONLESS.contains(&message) {
                violation(format!(
                    "reason-less error outside the documented set: {value}"
                ));
            }
            return Some(("", message));
        }
        let reason = data
            .as_object()
            .filter(|d| d.len() == 1)
            .and_then(|d| d.get("reason_id"))
            .and_then(Value::as_str)
            .unwrap_or_else(|| violation(format!("error data: {value}")));
        if !DOCUMENTED_REASONS.contains(&reason) || code != code_for(reason) {
            violation(format!("undocumented reason or code: {value}"));
        }
        if reason == "internal-error"
            || (reason == "backend-rpc-unavailable" && self.backend.state().injected == 0)
        {
            violation(format!("a fault nothing injected: {value}"));
        }
        Some((reason, message))
    }

    fn answer(&mut self, frame: &Frame, value: &Value) {
        let request = match frame {
            Frame::Invalid => {
                if self.error(value, &Value::Null)
                    != Some(("malformed-submit", "invalid JSON request"))
                {
                    violation(format!("invalid frame answered with {value}"));
                }
                self.charge_malformed();
                return;
            }
            Frame::Request(request) => request,
        };
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let error = self.error(value, &id);
        let outcome = match error {
            None => "ok",
            Some(("", message)) => message,
            Some((reason, _)) => reason,
        };
        self.state.outcomes.push(format!(
            "{}:{outcome}",
            id.as_str().map_or(id.to_string(), str::to_owned)
        ));
        let method = request.get("method").and_then(Value::as_str);
        let params = match request.get("params") {
            None => Some(&[][..]),
            Some(p) => p.as_array().map(Vec::as_slice),
        };
        let expect = |want: &str| {
            if error.map(|(reason, _)| reason) != Some(want) {
                violation(format!("{request} should fail with {want}, got {value}"));
            }
        };
        match (method, params) {
            (None, _) | (Some(_), None) => expect("malformed-submit"),
            (Some("mining.subscribe"), Some(_)) => self.subscribed(value, error),
            (Some("mining.authorize"), Some(params)) => self.authorized(params, value, error),
            (Some("mining.configure"), Some(params)) => self.configured(params, value, error),
            (Some("mining.extranonce.subscribe" | "mining.suggest_difficulty"), Some(_)) => {
                if error.is_some() || value["result"] != Value::Bool(true) {
                    violation(format!("{request} must succeed, got {value}"));
                }
            }
            (Some("mining.get_health"), Some(params)) => {
                if !params.is_empty() {
                    expect("malformed-submit");
                } else if value["result"] != serde_json::json!({"ready": true}) {
                    violation(format!("health answer {value}"));
                }
            }
            (Some("mining.submit"), Some(params)) => self.submitted(params, value, error),
            (Some(_), Some(_)) => expect("malformed-submit"),
        }
        if error.is_some_and(|(reason, _)| reason == "malformed-submit") {
            self.charge_malformed();
        }
    }

    fn charge_malformed(&mut self) {
        self.state.malformed += 1;
        let budget = self.config.max_malformed_frames_per_interval;
        if budget > 0 && self.state.malformed > budget {
            self.state.closing = Some("malformed-frame budget");
        }
    }

    fn subscribed(&mut self, value: &Value, error: Option<(&str, &str)>) {
        if error.is_some() {
            violation(format!("subscribe failed: {value}"));
        }
        let result = value["result"].as_array().map(Vec::as_slice);
        let Some([subscriptions, extranonce1, size]) = result else {
            violation(format!("subscribe result: {value}"));
        };
        let extranonce1 = extranonce1.as_str().filter(|e| is_hex(e, 8));
        if subscriptions != &serde_json::json!([])
            || extranonce1.is_none()
            || size.as_u64() != Some(self.config.extranonce2_size as u64)
        {
            violation(format!("subscribe result: {value}"));
        }
        let extranonce1 = extranonce1.unwrap().to_owned();
        if self
            .state
            .extranonce1
            .as_ref()
            .is_some_and(|old| *old != extranonce1)
        {
            violation("a resubscribe changed the connection's extranonce1");
        }
        self.state.extranonce1 = Some(extranonce1);
    }

    fn authorized(&mut self, params: &[Value], value: &Value, error: Option<(&str, &str)>) {
        self.state.authorizes += 1;
        let budget = self.config.max_authorize_attempts_per_interval;
        let over = budget > 0 && self.state.authorizes > budget;
        match error {
            None if over => violation(format!("authorize over its budget succeeded: {value}")),
            None => {
                let username = params.first().and_then(Value::as_str).unwrap_or("");
                self.state.authorized = Some(username.to_owned());
            }
            Some(("", "too many authorization attempts")) if over => {
                self.state.closing = Some("authorize budget");
            }
            Some(("", "too many connections for username"))
                if self.config.max_connections_per_username > 0 => {}
            Some(("unauthorized-worker", _)) => {}
            Some(_) => violation(format!("authorize answer {value}")),
        }
    }

    fn configured(&mut self, params: &[Value], value: &Value, error: Option<(&str, &str)>) {
        let rolling = params
            .first()
            .and_then(Value::as_array)
            .is_some_and(|e| e.iter().any(|e| e == "version-rolling"));
        if let Some((reason, _)) = error {
            if !rolling || reason != "malformed-submit" {
                violation(format!("configure answer {value}"));
            }
            return;
        }
        let result = value["result"]
            .as_object()
            .unwrap_or_else(|| violation(format!("configure result {value}")));
        if !rolling {
            if result.values().any(|v| v != &Value::Bool(false)) {
                violation(format!("configure accepted an unknown extension: {value}"));
            }
            return;
        }
        let requested = params
            .get(1)
            .and_then(|o| o.get("version-rolling.mask"))
            .map_or(Some(u32::MAX), |m| {
                m.as_str()
                    .filter(|m| is_hex(m, 8))
                    .and_then(|m| u32::from_str_radix(m, 16).ok())
            })
            .unwrap_or_else(|| violation(format!("configure accepted a malformed mask: {value}")));
        let mask = result
            .get("version-rolling.mask")
            .and_then(Value::as_str)
            .filter(|m| is_hex(m, 8))
            .and_then(|m| u32::from_str_radix(m, 16).ok())
            .unwrap_or_else(|| violation(format!("configure mask {value}")));
        if mask & !(requested & self.config.version_rolling_mask) != 0
            || result.get("version-rolling") != Some(&Value::Bool(mask != 0))
        {
            violation(format!(
                "configure granted mask {mask:08x} over {requested:08x}"
            ));
        }
        self.state.miner_mask = Some(requested);
        self.state.advertised_mask = mask;
    }

    /// The submit checks the server makes before any lookup are fully
    /// determined by the connection state and the fields, so their answers are
    /// predicted exactly; a job the backend never built can only be unknown.
    fn submitted(&mut self, params: &[Value], value: &Value, error: Option<(&str, &str)>) {
        let reason = error.map(|(reason, _)| reason);
        let predicted = if self.state.authorized.is_none() || self.state.extranonce1.is_none() {
            Some("unauthorized-worker")
        } else if params.len() < 5 || params.iter().any(|p| !p.is_string()) {
            Some("malformed-submit")
        } else if params[0].as_str() != self.state.authorized.as_deref() {
            Some("unauthorized-worker")
        } else if params[2].as_str().unwrap().len() != self.config.extranonce2_size * 2 {
            Some("invalid-extranonce")
        } else if params[3].as_str().unwrap().len() != 8 || params[4].as_str().unwrap().len() != 8 {
            Some("invalid-ntime-or-nonce")
        } else {
            None
        };
        if let Some(want) = predicted {
            if reason != Some(want) {
                violation(format!(
                    "submit {params:?} should fail with {want}, got {value}"
                ));
            }
            return;
        }
        let field = |i: usize| params[i].as_str().unwrap().to_owned();
        let job_id = field(1);
        let built = self.backend.state().issued.contains_key(&job_id);
        match error {
            Some(("", "too many unknown job submissions"))
                if self.config.max_unknown_jobs_per_interval > 0 =>
            {
                self.state.closing = Some("unknown-job budget");
            }
            _ if !built && reason != Some("unknown-job") => {
                violation(format!("a job never built was answered {value}"));
            }
            None => self.accepted.push(Accepted {
                job_id,
                username: field(0),
                proof: RawProof {
                    extranonce2: field(2),
                    ntime: field(3),
                    nonce: field(4),
                    version_bits: params.get(5).and_then(Value::as_str).map(str::to_owned),
                },
                delivered_before: self.state.delivered.clone(),
            }),
            Some(("", _)) => violation(format!("submit answer {value}")),
            Some(_) => {}
        }
    }
}

/// Every share the backend credited must be one the server acknowledged, in
/// the same order, and must be a valid proof on unretired work of the worker
/// it was issued to, whatever the session decided on the way.
pub fn credits(backend: &FuzzBackend, accepted: &[Accepted]) {
    let state = backend.state();
    if state.credits.len() != accepted.len() {
        violation(format!(
            "{} credits but {} acknowledged submits",
            state.credits.len(),
            accepted.len()
        ));
    }
    for (credit, ack) in state.credits.iter().zip(accepted) {
        let submission = &credit.submission;
        let issued = state
            .issued
            .get(&credit.job_id)
            .unwrap_or_else(|| violation(format!("credit for unbuilt job {}", credit.job_id)));
        if credit.job_id != ack.job_id
            || !submission
                .extranonce2_hex
                .eq_ignore_ascii_case(&ack.proof.extranonce2)
            || format!("{:08x}", submission.ntime) != ack.proof.ntime.to_ascii_lowercase()
            || format!("{:08x}", submission.nonce) != ack.proof.nonce.to_ascii_lowercase()
        {
            violation(format!(
                "credit {credit:?} is not the acknowledged submit {ack:?}"
            ));
        }
        if credit.worker != issued.worker.username {
            violation(format!(
                "{credit:?} credited to a worker the job was not issued to"
            ));
        }
        let mask = issued
            .mask
            .unwrap_or_else(|| violation(format!("credit on unpersisted job {}", credit.job_id)));
        match crate::pow::header_hash(&issued.job, mask, &ack.proof) {
            Ok(hash) if hash <= issued.job.share_target => {}
            Ok(hash) => violation(format!(
                "credited share {ack:?} hashes to {hash:x}, above its target {:x}",
                issued.job.share_target
            )),
            Err(why) => violation(format!("credited share {ack:?} is malformed: {why}")),
        }
        // #478: a same-parent payout replacement delivered on this connection
        // retires the work it supersedes to block-only; it never earns again.
        if let Some(position) = ack.delivered_before.iter().position(|j| *j == ack.job_id) {
            for later in &ack.delivered_before[position + 1..] {
                let later = &state.issued[later].job;
                if later.previousblockhash == issued.job.previousblockhash
                    && later.payout_revision != issued.job.payout_revision
                {
                    violation(format!(
                        "credited {} after {} superseded it",
                        ack.job_id, later.job_id
                    ));
                }
            }
        }
    }
}
