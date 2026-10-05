//! What an external-target process measured, kept so that several
//! processes' documents add into exactly the document one process driving
//! all of their sessions would have written.
//!
//! A document has four parts. `processes` says who measured what: one entry
//! per client process, with its configuration and its own counts. `totals`
//! is the raw, additive record: counts, latency histograms and a per-second
//! timeline keyed by wall-clock second. `summary` and `definitions` are
//! derived from those two and are recomputed, never read back, when
//! documents are merged ([`merge`]).

use crate::classify::{self, Rejection};
use crate::client::{self, Event, Outcome, SubmitRecord};
use crate::external::histogram::LogHistogram;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub const SCHEMA: &str = "qbit.prism.external-load.v1";

/// The clock every latency in the document is on.
pub const LATENCY_CLOCK: &str = "the client process's monotonic clock";

/// Distinct keys a [`Tally`] keeps before it counts the rest under `other`.
pub const TALLY_KEYS: usize = 64;

/// Distinct rejection triples kept before the rest are counted under `other`.
pub const REJECTION_KEYS: usize = 256;

/// Tips kept; past this the oldest is dropped and counted.
pub const TIP_KEYS: usize = 1024;

/// Sample messages kept per client-failure kind.
pub const FAILURE_SAMPLES: usize = 8;

/// Share-log lines that may wait for the writer: tens of megabytes at most.
/// Past it a line is dropped and counted, rather than held without bound or
/// waited for on the async runtime.
pub const SHARE_LOG_BACKLOG: usize = 65_536;

/// `total + more`, refused rather than wrapped: two documents' counters can
/// only pass `u64::MAX` together if one of them was damaged.
fn sum(total: u64, more: u64, what: &str) -> Result<u64> {
    total
        .checked_add(more)
        .with_context(|| format!("{what} overflows when the documents are added"))
}

/// One instant on both clocks, so an event's monotonic `Instant` can be
/// placed on the wall clock without reading the wall clock again: a wall
/// clock stepped mid-run moves nothing.
#[derive(Clone, Copy, Debug)]
pub struct Anchor {
    instant: Instant,
    unix_ms: i64,
}

impl Anchor {
    pub fn now() -> Self {
        Self {
            instant: Instant::now(),
            unix_ms: chrono::Utc::now().timestamp_millis(),
        }
    }

    pub fn unix_ms(&self, at: Instant) -> i64 {
        match at.checked_duration_since(self.instant) {
            Some(after) => self.unix_ms + after.as_millis() as i64,
            None => self.unix_ms - self.instant.duration_since(at).as_millis() as i64,
        }
    }

    pub fn unix_second(&self, at: Instant) -> i64 {
        self.unix_ms(at).div_euclid(1000)
    }
}

/// Counts by a free-text key, bounded: the first [`TALLY_KEYS`] distinct keys
/// are kept and every later one is counted under `other`, so a stream of
/// distinct messages cannot grow the document without bound.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tally {
    pub counts: BTreeMap<String, u64>,
    pub other: u64,
}

impl Tally {
    pub fn add(&mut self, key: &str, count: u64) {
        if let Some(held) = self.counts.get_mut(key) {
            *held += count;
        } else if self.counts.len() < TALLY_KEYS {
            self.counts.insert(key.to_owned(), count);
        } else {
            self.other += count;
        }
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        for (key, count) in &other.counts {
            if let Some(held) = self.counts.get_mut(key) {
                *held = sum(*held, *count, "a tally")?;
            } else if self.counts.len() < TALLY_KEYS {
                self.counts.insert(key.clone(), *count);
            } else {
                self.other = sum(self.other, *count, "a tally")?;
            }
        }
        self.other = sum(self.other, other.other, "a tally")?;
        Ok(())
    }

    pub fn total(&self) -> u64 {
        self.counts.values().sum::<u64>() + self.other
    }
}

/// Rejections by the `(code, reason_id, message)` triple the server sent.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RejectionsWire", into = "RejectionsWire")]
pub struct Rejections {
    counts: BTreeMap<(i64, Option<String>, String), u64>,
    other: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectionsWire {
    by_reason: Vec<RejectionEntry>,
    other: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectionEntry {
    code: i64,
    reason_id: Option<String>,
    message: String,
    /// [`classify::classify`]'s class, written for the reader and
    /// recomputed rather than trusted when read back.
    #[serde(default)]
    class: Option<String>,
    count: u64,
}

impl Rejections {
    pub fn add(&mut self, rejection: &Rejection, count: u64) {
        let key = (
            rejection.code,
            rejection.reason_id.clone(),
            rejection.message.clone(),
        );
        if let Some(held) = self.counts.get_mut(&key) {
            *held += count;
        } else if self.counts.len() < REJECTION_KEYS {
            self.counts.insert(key, count);
        } else {
            self.other += count;
        }
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        for (key, count) in &other.counts {
            if let Some(held) = self.counts.get_mut(key) {
                *held = sum(*held, *count, "a rejection count")?;
            } else if self.counts.len() < REJECTION_KEYS {
                self.counts.insert(key.clone(), *count);
            } else {
                self.other = sum(self.other, *count, "a rejection count")?;
            }
        }
        self.other = sum(self.other, other.other, "a rejection count")?;
        Ok(())
    }

    /// Every triple with its class and count, most frequent first.
    pub fn by_reason(&self) -> Vec<Value> {
        let mut rows: Vec<_> = self.counts.iter().collect();
        rows.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        rows.into_iter()
            .map(|((code, reason_id, message), count)| {
                json!({
                    "code": code,
                    "reason_id": reason_id,
                    "message": message,
                    "class": class_of(*code, reason_id, message),
                    "count": count,
                })
            })
            .collect()
    }

    /// Counts by [`classify::RejectionClass`]; the triples past
    /// [`REJECTION_KEYS`] have no class and are `unclassified`.
    pub fn by_class(&self) -> BTreeMap<String, u64> {
        let mut classes = BTreeMap::new();
        for ((code, reason_id, message), count) in &self.counts {
            *classes
                .entry(class_of(*code, reason_id, message).to_owned())
                .or_insert(0) += count;
        }
        if self.other > 0 {
            classes.insert("unclassified".into(), self.other);
        }
        classes
    }
}

fn class_of(code: i64, reason_id: &Option<String>, message: &str) -> &'static str {
    classify::classify(&Rejection {
        code,
        reason_id: reason_id.clone(),
        message: message.to_owned(),
    })
    .as_str()
}

impl From<Rejections> for RejectionsWire {
    fn from(rejections: Rejections) -> Self {
        let by_reason = rejections
            .counts
            .into_iter()
            .map(|((code, reason_id, message), count)| RejectionEntry {
                class: Some(class_of(code, &reason_id, &message).to_owned()),
                code,
                reason_id,
                message,
                count,
            })
            .collect();
        Self {
            by_reason,
            other: rejections.other,
        }
    }
}

impl TryFrom<RejectionsWire> for Rejections {
    type Error = anyhow::Error;

    fn try_from(wire: RejectionsWire) -> Result<Self> {
        let mut rejections = Self {
            other: wire.other,
            ..Self::default()
        };
        for entry in wire.by_reason {
            rejections.add(
                &Rejection {
                    code: entry.code,
                    reason_id: entry.reason_id,
                    message: entry.message,
                },
                entry.count,
            );
        }
        Ok(rejections)
    }
}

/// One whole wall-clock second.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Second {
    pub unix_second: i64,
    /// Offers the token bucket minted this second.
    pub offered: u64,
    /// Of those, placed with a session.
    pub dispatched: u64,
    /// Of those, no session could take.
    pub shortfall: u64,
    /// Answers read this second.
    pub accepted: u64,
    pub rejected: u64,
    /// Submits given up on this second (socket closed, run ended).
    pub no_response: u64,
    /// Connections that ended this second, other than the run's own stop.
    pub disconnects: u64,
    /// Reconnects that reached work this second.
    pub reconnects: u64,
    /// Sessions holding work when the second was sampled, summed over the
    /// processes that sampled it; `null` when none did.
    pub sessions_holding_work: Option<u64>,
    /// How many processes sampled `sessions_holding_work` this second.
    pub sampled_by: u64,
}

impl Second {
    fn merge(&mut self, other: &Self) -> Result<()> {
        let what = "a timeline second's count";
        self.offered = sum(self.offered, other.offered, what)?;
        self.dispatched = sum(self.dispatched, other.dispatched, what)?;
        self.shortfall = sum(self.shortfall, other.shortfall, what)?;
        self.accepted = sum(self.accepted, other.accepted, what)?;
        self.rejected = sum(self.rejected, other.rejected, what)?;
        self.no_response = sum(self.no_response, other.no_response, what)?;
        self.disconnects = sum(self.disconnects, other.disconnects, what)?;
        self.reconnects = sum(self.reconnects, other.reconnects, what)?;
        self.sessions_holding_work = match (self.sessions_holding_work, other.sessions_holding_work)
        {
            (Some(a), Some(b)) => Some(sum(a, b, what)?),
            (a, b) => a.or(b),
        };
        self.sampled_by = sum(self.sampled_by, other.sampled_by, what)?;
        Ok(())
    }
}

/// Per-second counts by wall-clock second, written in ascending order.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<Second>", into = "Vec<Second>")]
pub struct Timeline(BTreeMap<i64, Second>);

impl Timeline {
    pub fn second(&mut self, unix_second: i64) -> &mut Second {
        self.0.entry(unix_second).or_insert_with(|| Second {
            unix_second,
            ..Second::default()
        })
    }

    pub fn seconds(&self) -> impl Iterator<Item = &Second> {
        self.0.values()
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        for second in other.0.values() {
            self.second(second.unix_second).merge(second)?;
        }
        Ok(())
    }
}

impl From<Timeline> for Vec<Second> {
    fn from(timeline: Timeline) -> Self {
        timeline.0.into_values().collect()
    }
}

impl TryFrom<Vec<Second>> for Timeline {
    type Error = anyhow::Error;

    fn try_from(seconds: Vec<Second>) -> Result<Self> {
        let mut timeline = Self::default();
        for second in seconds {
            timeline.second(second.unix_second).merge(&second)?;
        }
        Ok(timeline)
    }
}

/// When a tip's first work reached the sessions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TipStat {
    /// The first session to hold work on the tip.
    pub first_seen_unix_ms: i64,
    /// The last session to first hold work on the tip.
    pub last_seen_unix_ms: i64,
    /// Sessions that held work on the tip.
    pub sessions: u64,
}

/// Tips by hash, bounded to [`TIP_KEYS`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tips {
    pub tips: BTreeMap<String, TipStat>,
    /// Tips dropped, oldest first, to stay within the bound.
    pub dropped: u64,
}

impl Tips {
    fn add(&mut self, tip: &str, stat: &TipStat) -> Result<()> {
        match self.tips.get_mut(tip) {
            Some(held) => {
                held.first_seen_unix_ms = held.first_seen_unix_ms.min(stat.first_seen_unix_ms);
                held.last_seen_unix_ms = held.last_seen_unix_ms.max(stat.last_seen_unix_ms);
                held.sessions = sum(held.sessions, stat.sessions, "a tip's sessions")?;
            }
            None => {
                self.tips.insert(tip.to_owned(), stat.clone());
                while self.tips.len() > TIP_KEYS {
                    let oldest = self
                        .tips
                        .iter()
                        .min_by_key(|(hash, stat)| (stat.first_seen_unix_ms, *hash))
                        .map(|(hash, _)| hash.clone())
                        .expect("a tip to drop");
                    self.tips.remove(&oldest);
                    self.dropped += 1;
                }
            }
        }
        Ok(())
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        for (tip, stat) in &other.tips {
            self.add(tip, stat)?;
        }
        self.dropped = sum(self.dropped, other.dropped, "the dropped tips")?;
        Ok(())
    }
}

/// Client failures of one kind: how many, how many of them a no-response
/// record already carries, and the first few messages.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureStat {
    pub count: u64,
    /// Already a submit record (a write that failed is recorded as a
    /// no-response first), so the offer accounting does not count it twice.
    pub recorded: u64,
    pub samples: Vec<String>,
}

impl FailureStat {
    fn merge(&mut self, other: &Self) -> Result<()> {
        self.count = sum(self.count, other.count, "a client-failure count")?;
        self.recorded = sum(self.recorded, other.recorded, "a client-failure count")?;
        for sample in &other.samples {
            if self.samples.len() < FAILURE_SAMPLES && !self.samples.contains(sample) {
                self.samples.push(sample.clone());
            }
        }
        Ok(())
    }
}

/// The additive record. Every field adds across processes.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Totals {
    pub offers_minted: u64,
    pub offers_dispatched: u64,
    pub offers_shortfall: u64,
    /// Offers a session took and never sent: the run stopped it first (one
    /// without a connection at the time included), or it did not stop in
    /// time and was aborted (`Process::sessions_aborted_at_stop`).
    pub offers_discarded: u64,
    /// Offers held by a session that did not stop within 10 s and was
    /// aborted. Whether each was sent is unknown, and none has a share-log
    /// line: a share among them may be in the target's ledger unexplained.
    pub offers_unknown_at_abort: u64,
    /// Offers whose job the target advertised above the client's ceiling:
    /// not mined, and also counted under `client_failures.offer`.
    pub offers_above_difficulty_ceiling: u64,
    pub accepted: u64,
    pub rejected: u64,
    /// Submits still unanswered when the drain ended.
    pub no_response_run_ended: u64,
    /// Submits whose connection went before their answer came.
    pub no_response_mid_run: u64,
    pub no_response_reasons: Tally,
    pub rejections: Rejections,
    /// By `client::FailureKind`, kebab-case.
    pub client_failures: BTreeMap<String, FailureStat>,
    /// Accepted submits: the submit's write to the read of its answer.
    pub ack_latency: LogHistogram,
    /// Rejected submits, the same way.
    pub rejection_latency: LogHistogram,
    /// Connections that reached work, the first and every reconnect.
    pub connections_opened: u64,
    pub initial_connections: u64,
    /// A session's first connection: from the start of the attempt that
    /// succeeded to its first job. Failed attempts before it are
    /// `initial_connect_failures`.
    pub time_to_first_job: LogHistogram,
    /// Failed first-connection attempts (each is retried).
    pub initial_connect_failures: u64,
    pub initial_connect_errors: Tally,
    /// Connections that ended during the run, other than at its stop.
    pub disconnects: u64,
    pub disconnect_causes: Tally,
    pub reconnects_completed: u64,
    pub reconnect_failed_attempts: u64,
    pub reconnect_errors: Tally,
    /// A completed reconnect: from losing the connection to holding work
    /// again, every failed attempt and backoff included.
    pub reconnect_outage: LogHistogram,
    pub notifies: u64,
    pub clean_jobs: u64,
    pub difficulty_advertisements: u64,
    pub advertised_difficulty_min: Option<f64>,
    pub advertised_difficulty_max: Option<f64>,
    /// Share solutions that also solved a block: stepped over, never sent.
    pub discarded_block_solutions: u64,
    pub tips: Tips,
    pub timeline: Timeline,
}

impl Totals {
    /// Add another process's totals. A counter that would pass `u64::MAX` is
    /// refused rather than wrapped.
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        macro_rules! add {
            ($($field:ident),* $(,)?) => {
                $(self.$field = sum(self.$field, other.$field, stringify!($field))?;)*
            };
        }
        add!(
            offers_minted,
            offers_dispatched,
            offers_shortfall,
            offers_discarded,
            offers_unknown_at_abort,
            offers_above_difficulty_ceiling,
            accepted,
            rejected,
            no_response_run_ended,
            no_response_mid_run,
            connections_opened,
            initial_connections,
            initial_connect_failures,
            disconnects,
            reconnects_completed,
            reconnect_failed_attempts,
            notifies,
            clean_jobs,
            difficulty_advertisements,
            discarded_block_solutions,
        );
        self.no_response_reasons.merge(&other.no_response_reasons)?;
        self.rejections.merge(&other.rejections)?;
        for (kind, stat) in &other.client_failures {
            self.client_failures
                .entry(kind.clone())
                .or_default()
                .merge(stat)?;
        }
        self.ack_latency.merge(&other.ack_latency)?;
        self.rejection_latency.merge(&other.rejection_latency)?;
        self.time_to_first_job.merge(&other.time_to_first_job)?;
        self.initial_connect_errors
            .merge(&other.initial_connect_errors)?;
        self.disconnect_causes.merge(&other.disconnect_causes)?;
        self.reconnect_errors.merge(&other.reconnect_errors)?;
        self.reconnect_outage.merge(&other.reconnect_outage)?;
        self.advertised_difficulty_min = min_f64(
            self.advertised_difficulty_min,
            other.advertised_difficulty_min,
        );
        self.advertised_difficulty_max = max_f64(
            self.advertised_difficulty_max,
            other.advertised_difficulty_max,
        );
        self.tips.merge(&other.tips)?;
        self.timeline.merge(&other.timeline)?;
        Ok(())
    }

    pub fn no_response(&self) -> u64 {
        self.no_response_run_ended + self.no_response_mid_run
    }

    /// What every process's totals hold by construction, checked on a
    /// document read back for a merge: a hand-edited or damaged one is
    /// refused with the reason rather than added into figures it would
    /// corrupt, or into arithmetic it would overflow.
    pub fn check(&self) -> Result<()> {
        ensure!(
            self.offers_dispatched.checked_add(self.offers_shortfall) == Some(self.offers_minted),
            "offers dispatched ({}) and shortfall ({}) do not add up to the offers minted ({})",
            self.offers_dispatched,
            self.offers_shortfall,
            self.offers_minted
        );
        for (kind, stat) in &self.client_failures {
            ensure!(
                stat.recorded <= stat.count,
                "client failures of kind {kind}: {} recorded of {}",
                stat.recorded,
                stat.count
            );
        }
        for (tip, stat) in &self.tips.tips {
            ensure!(
                stat.first_seen_unix_ms <= stat.last_seen_unix_ms && stat.sessions > 0,
                "tip {tip}: first seen after last seen, or by no session"
            );
        }
        for second in self.timeline.seconds() {
            ensure!(
                second.dispatched.checked_add(second.shortfall) == Some(second.offered),
                "second {}: offers dispatched and shortfall do not add up to the offers made",
                second.unix_second
            );
        }
        Ok(())
    }

    /// Every submit that went out and was settled one way or the other.
    pub fn sent(&self) -> u64 {
        self.accepted + self.rejected + self.no_response()
    }
}

fn min_f64(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn max_f64(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

/// The optional per-submit log (`--share-log`): one JSON line per submit,
/// so the offered, acknowledged, rejected and unanswered share ids can be
/// reconciled against the target's ledger, which this process cannot read
/// (#291's failover drill).
pub struct ShareLog {
    path: PathBuf,
    /// Lines for the writer thread. The collector runs on the async runtime
    /// and must never wait on the disk: a slow filesystem would stall that
    /// worker's sessions and read as acknowledgement latency.
    lines: Option<std::sync::mpsc::SyncSender<Vec<u8>>>,
    /// Lines handed to the writer.
    sent: u64,
    /// Lines dropped because the writer was `SHARE_LOG_BACKLOG` behind.
    dropped: u64,
    /// Writes every line it is sent, then flushes, and returns how many it
    /// wrote and the first write that failed.
    writer: Option<std::thread::JoinHandle<(u64, Option<String>)>>,
}

/// What the share log holds, recorded in the process entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareLogInfo {
    pub path: String,
    pub lines: u64,
    /// Lines dropped because the disk fell too far behind; the log is then
    /// missing that many submits.
    pub dropped: u64,
    /// The first write that failed; the log stops there and says so.
    pub error: Option<String>,
}

impl ShareLog {
    /// Create (or truncate) the log before any load, so a path that cannot
    /// be written refuses the run instead of losing the ids.
    pub fn create(path: &Path) -> Result<Self> {
        let file = std::fs::File::create(path)
            .with_context(|| format!("creating the share log {}", path.display()))?;
        let (lines, received) = std::sync::mpsc::sync_channel::<Vec<u8>>(SHARE_LOG_BACKLOG);
        let writer = std::thread::Builder::new()
            .name("share-log".into())
            .spawn(move || {
                let mut writer = std::io::BufWriter::new(file);
                let (mut written, mut error) = (0u64, None);
                // Every line is taken off the channel even after a failure,
                // so nothing waits on a writer that has stopped writing.
                for line in received {
                    if error.is_none() {
                        match writer.write_all(&line) {
                            Ok(()) => written += 1,
                            Err(failure) => error = Some(format!("{failure}")),
                        }
                    }
                }
                if error.is_none() {
                    if let Err(failure) = writer.flush() {
                        error = Some(format!("{failure}"));
                    }
                }
                (written, error)
            })
            .context("starting the share-log writer")?;
        Ok(Self {
            path: path.to_owned(),
            lines: Some(lines),
            sent: 0,
            dropped: 0,
            writer: Some(writer),
        })
    }

    fn write(&mut self, line: &Value) {
        let mut bytes = serde_json::to_vec(line).expect("a JSON value serializes");
        bytes.push(b'\n');
        if let Some(lines) = &self.lines {
            match lines.try_send(bytes) {
                Ok(()) => self.sent += 1,
                Err(std::sync::mpsc::TrySendError::Full(_)) => self.dropped += 1,
                // The writer is gone; finish() reports why.
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {}
            }
        }
    }

    /// What to report if the writer cannot be waited for: the lines it was
    /// sent, which it may not all have written, and why.
    pub fn unfinished(&self) -> impl Fn(&str) -> ShareLogInfo {
        let (path, sent, dropped) = (self.path.display().to_string(), self.sent, self.dropped);
        move |why: &str| ShareLogInfo {
            path: path.clone(),
            lines: sent,
            dropped,
            error: Some(format!(
                "{why}: lines counts the {sent} sent to the writer, which may not all be in the \
                 file"
            )),
        }
    }

    /// Close the log once the writer has written every line. This waits on
    /// the disk, so it is not called on the async runtime.
    pub fn finish(mut self) -> ShareLogInfo {
        drop(self.lines.take());
        let (lines, error) = match self.writer.take().map(std::thread::JoinHandle::join) {
            Some(Ok(result)) => result,
            Some(Err(_)) => (0, Some("the share-log writer panicked".to_owned())),
            None => (0, None),
        };
        ShareLogInfo {
            path: self.path.display().to_string(),
            lines,
            dropped: self.dropped,
            error,
        }
    }
}

/// Folds the sessions' events into [`Totals`] as they arrive, so a run of
/// hours holds counts and histograms rather than every submit record.
pub struct Collector {
    pub totals: Totals,
    anchor: Anchor,
    /// Whether each session holds work: set on its `Connected`, cleared on
    /// its `Disconnected`. Shared with the scheduler, which offers only to a
    /// session that holds work.
    holding: Arc<[AtomicBool]>,
    holding_count: usize,
    /// Per tip, the sessions that have held work on it, one bit each: a
    /// session sights a tip again on every connection, and may come back to
    /// one after holding another, but only its first sighting says when the
    /// tip's work reached it. Kept for the tips `totals.tips` keeps.
    tip_sessions: HashMap<String, Vec<u64>>,
    share_log: Option<ShareLog>,
    /// The most session events seen waiting behind the one being counted.
    pub event_backlog_max: usize,
}

impl Collector {
    pub fn new(anchor: Anchor, share_log: Option<ShareLog>, sessions: usize) -> Self {
        Self {
            totals: Totals::default(),
            anchor,
            holding: (0..sessions).map(|_| AtomicBool::new(false)).collect(),
            holding_count: 0,
            tip_sessions: HashMap::new(),
            share_log,
            event_backlog_max: 0,
        }
    }

    /// The per-session flags the scheduler reads.
    pub fn holding(&self) -> Arc<[AtomicBool]> {
        self.holding.clone()
    }

    pub fn sessions_holding_work(&self) -> usize {
        self.holding_count
    }

    /// Record how many sessions hold work now, as the given second's sample.
    pub fn sample_holding_work(&mut self, unix_second: i64) {
        let holding = self.holding_count as u64;
        let second = self.totals.timeline.second(unix_second);
        if second.sampled_by == 0 {
            second.sampled_by = 1;
        }
        second.sessions_holding_work = Some(holding);
    }

    /// The share log, to be finished off the async runtime.
    pub fn take_share_log(&mut self) -> Option<ShareLog> {
        self.share_log.take()
    }

    pub fn apply(&mut self, event: Event) {
        let now = Instant::now();
        match event {
            Event::CensusBarrier(ack) => {
                let _ = ack.send(());
            }
            Event::Submit(record) => self.submit(&record, now),
            Event::Reconnect(record) => {
                if record.completed {
                    // Its second in the timeline comes from its `Opened`,
                    // which carries when the session held work again.
                    self.totals.reconnects_completed += 1;
                    self.totals
                        .reconnect_outage
                        .record_millis(record.seconds * 1000.0);
                } else if record.reason == "initial" {
                    self.totals.initial_connect_failures += 1;
                    self.totals
                        .initial_connect_errors
                        .add(record.error.as_deref().unwrap_or("unknown"), 1);
                } else {
                    self.totals.reconnect_failed_attempts += 1;
                    self.totals
                        .reconnect_errors
                        .add(record.error.as_deref().unwrap_or("unknown"), 1);
                }
            }
            Event::Opened(opened) => {
                self.totals.connections_opened += 1;
                if opened.cause != "initial" {
                    self.second(opened.ready).reconnects += 1;
                }
                if opened.cause == "initial" {
                    self.totals.initial_connections += 1;
                    self.totals.time_to_first_job.record_millis(
                        opened
                            .ready
                            .saturating_duration_since(opened.started)
                            .as_secs_f64()
                            * 1000.0,
                    );
                }
            }
            Event::Closed(closed) => {
                // The run's own stop is the end of the measurement, not a
                // disconnect.
                if closed.cause != "stopped" {
                    self.totals.disconnects += 1;
                    self.totals.disconnect_causes.add(&closed.cause, 1);
                    self.second(closed.at).disconnects += 1;
                }
            }
            Event::Connected { session, .. } => {
                if let Some(flag) = self.holding.get(session) {
                    if !flag.swap(true, Ordering::Relaxed) {
                        self.holding_count += 1;
                    }
                }
            }
            Event::Disconnected { session, .. } => {
                if let Some(flag) = self.holding.get(session) {
                    if flag.swap(false, Ordering::Relaxed) {
                        self.holding_count -= 1;
                    }
                }
            }
            Event::Tip(sighting) => self.tip(&sighting),
            Event::Notify(sighting) => {
                self.totals.notifies += 1;
                if sighting.clean_jobs {
                    self.totals.clean_jobs += 1;
                }
            }
            Event::DifficultyAdvertised { difficulty, .. } => {
                self.totals.difficulty_advertisements += 1;
                self.totals.advertised_difficulty_min =
                    min_f64(self.totals.advertised_difficulty_min, Some(difficulty));
                self.totals.advertised_difficulty_max =
                    max_f64(self.totals.advertised_difficulty_max, Some(difficulty));
            }
            Event::DiscardedBlockSolution { .. } => self.totals.discarded_block_solutions += 1,
            Event::DiscardedOffer { .. } => self.totals.offers_discarded += 1,
            // An external session mines what it is advertised and never
            // compares; were one to arrive, it is reported, not dropped.
            Event::DifficultyMismatch {
                advertised,
                configured,
                ..
            } => self.failure(
                "difficulty-mismatch",
                false,
                &format!("advertised {advertised}, configured {configured}"),
            ),
            Event::Failure(failure) => {
                if failure.error.starts_with(client::DIFFICULTY_ABOVE_CEILING) {
                    self.totals.offers_above_difficulty_ceiling += 1;
                }
                let kind = serde_json::to_value(failure.kind)
                    .ok()
                    .and_then(|kind| kind.as_str().map(str::to_owned))
                    .unwrap_or_else(|| format!("{:?}", failure.kind));
                self.failure(&kind, failure.recorded, &failure.error);
            }
        }
    }

    fn second(&mut self, at: Instant) -> &mut Second {
        let second = self.anchor.unix_second(at);
        self.totals.timeline.second(second)
    }

    fn failure(&mut self, kind: &str, recorded: bool, error: &str) {
        let stat = self
            .totals
            .client_failures
            .entry(kind.to_owned())
            .or_default();
        stat.count += 1;
        stat.recorded += u64::from(recorded);
        if stat.samples.len() < FAILURE_SAMPLES && !stat.samples.iter().any(|s| s == error) {
            stat.samples.push(error.to_owned());
        }
    }

    fn submit(&mut self, record: &SubmitRecord, now: Instant) {
        let answered = record.responded.unwrap_or(now);
        match &record.outcome {
            Outcome::Accepted => {
                self.totals.accepted += 1;
                if let Some(latency) = record.latency_millis {
                    self.totals.ack_latency.record_millis(latency);
                }
                self.second(answered).accepted += 1;
            }
            Outcome::Rejected(rejection) => {
                self.totals.rejected += 1;
                self.totals.rejections.add(rejection, 1);
                if let Some(latency) = record.latency_millis {
                    self.totals.rejection_latency.record_millis(latency);
                }
                self.second(answered).rejected += 1;
            }
            Outcome::NoResponse { reason } => {
                if reason == client::RUN_ENDED {
                    self.totals.no_response_run_ended += 1;
                } else {
                    self.totals.no_response_mid_run += 1;
                }
                self.totals.no_response_reasons.add(reason, 1);
                self.second(now).no_response += 1;
            }
        }
        if let Some(log) = self.share_log.as_mut() {
            let (code, reason_id, message, no_response_reason) = match &record.outcome {
                Outcome::Rejected(rejection) => (
                    Some(rejection.code),
                    rejection.reason_id.clone(),
                    Some(rejection.message.clone()),
                    None,
                ),
                Outcome::NoResponse { reason } => (None, None, None, Some(reason.clone())),
                Outcome::Accepted => (None, None, None, None),
            };
            let line = json!({
                "share_id": record.share_id,
                "outcome": record.outcome.label(),
                "session": record.session,
                "job_id": record.job_id,
                "sent_unix_ms": self.anchor.unix_ms(record.sent),
                "answered_unix_ms": record.responded.map(|at| self.anchor.unix_ms(at)),
                "latency_ms": record.latency_millis,
                "code": code,
                "reason_id": reason_id,
                "message": message,
                "no_response_reason": no_response_reason,
            });
            log.write(&line);
        }
    }

    fn tip(&mut self, sighting: &client::TipSighting) {
        let seen = self.tip_sessions.entry(sighting.tip.clone()).or_default();
        let (word, bit) = (sighting.session / 64, 1u64 << (sighting.session % 64));
        if seen.len() <= word {
            seen.resize(word + 1, 0);
        }
        if seen[word] & bit != 0 {
            return;
        }
        seen[word] |= bit;
        let at = self.anchor.unix_ms(sighting.at);
        // One session more: past u64::MAX only in a run that cannot happen.
        let _ = self.totals.tips.add(
            &sighting.tip,
            &TipStat {
                first_seen_unix_ms: at,
                last_seen_unix_ms: at,
                sessions: 1,
            },
        );
        // A tip the bound dropped takes its sessions with it.
        if self.tip_sessions.len() > self.totals.tips.tips.len() {
            let kept = &self.totals.tips.tips;
            self.tip_sessions.retain(|tip, _| kept.contains_key(tip));
        }
    }
}

/// The load window one process drove.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    pub started_unix_ms: i64,
    pub ended_unix_ms: i64,
    pub seconds: f64,
}

/// One client process: what it was asked to do, what it did, and its own
/// counts, so per-process rates survive a merge.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    pub run_id: String,
    pub label: String,
    pub host: Option<String>,
    pub harness_version: String,
    pub target: String,
    /// What `--target` resolved to at entry. Every connection resolves it
    /// again, so a name the operator moves is followed.
    pub target_resolved: Vec<String>,
    pub address: String,
    pub worker_prefix: String,
    pub sessions: usize,
    pub rate: f64,
    pub duration_seconds: u64,
    /// Asked for with `d=` in the Stratum password; `null` when not asked.
    pub requested_difficulty: Option<f64>,
    pub max_difficulty: f64,
    pub work_timeout_seconds: u64,
    pub drain_seconds: u64,
    pub started_at: String,
    pub ended_at: String,
    /// `null` when the run ended before its window opened.
    pub window: Option<Window>,
    pub sessions_holding_work_at_start: usize,
    /// `completed`, `interrupted: <signal>` or `blocked: <reason>`.
    pub ended: String,
    pub exit_code: i32,
    pub offers_minted: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub no_response: u64,
    /// What sessions holding work still had outstanding when the drain
    /// ended: a submit not yet answered (then recorded as no-response
    /// `run ended`) or an offer still being mined (then discarded).
    pub outstanding_at_drain_end: usize,
    /// Offers held by sessions without a connection when the drain ended,
    /// which the drain does not wait for: they were never sent, and are
    /// counted under `totals.offers_discarded`.
    pub held_without_a_connection_at_drain_end: usize,
    /// Sessions that had not stopped within 10 s of being told to (one in
    /// the middle of a handshake), aborted. What they had taken is counted
    /// under `totals.offers_discarded`.
    pub sessions_aborted_at_stop: usize,
    /// Set when the stats stopped taking events before the last one arrived,
    /// with why; `null` when every event was counted.
    pub events_cut_off: Option<String>,
    /// The most session events that waited for the stats at once. Near
    /// `EVENT_BACKLOG_LIMIT` the run stops itself rather than let it grow.
    pub event_backlog_max: usize,
    /// This process's CPU over its whole life, and the cores it had, so a
    /// shortfall can be told from a busy client.
    pub client_cpu_seconds: Option<f64>,
    pub available_parallelism: Option<usize>,
    pub file_descriptor_limit: Option<u64>,
    pub share_log: Option<ShareLogInfo>,
}

/// The document one process writes, or a merge of several.
pub fn document(kind: &str, processes: &[Process], totals: &Totals) -> Result<Value> {
    Ok(json!({
        "schema": SCHEMA,
        "kind": kind,
        "processes": processes,
        "summary": summary(processes, totals),
        "definitions": definitions(),
        "totals": serde_json::to_value(totals).context("serializing the totals")?,
    }))
}

/// The documents at `inputs`, added into one. A process that appears twice
/// is refused: its counts would be added twice.
pub fn merge(inputs: &[(PathBuf, Value)]) -> Result<Value> {
    ensure!(!inputs.is_empty(), "nothing to merge");
    let mut processes: Vec<Process> = Vec::new();
    let mut totals = Totals::default();
    let mut seen: BTreeMap<String, PathBuf> = BTreeMap::new();
    for (path, value) in inputs {
        let schema = value.get("schema").and_then(Value::as_str);
        ensure!(
            schema == Some(SCHEMA),
            "{} is not a {SCHEMA} document (schema {schema:?})",
            path.display()
        );
        let these: Vec<Process> = serde_json::from_value(
            value
                .get("processes")
                .cloned()
                .with_context(|| format!("{} has no processes", path.display()))?,
        )
        .with_context(|| format!("reading the processes of {}", path.display()))?;
        for process in &these {
            if let Some(first) = seen.insert(process.run_id.clone(), path.clone()) {
                anyhow::bail!(
                    "run {} ({}) is in both {} and {}; merging it twice would count it twice",
                    process.run_id,
                    process.label,
                    first.display(),
                    path.display()
                );
            }
        }
        let these_totals: Totals = serde_json::from_value(
            value
                .get("totals")
                .cloned()
                .with_context(|| format!("{} has no totals", path.display()))?,
        )
        .with_context(|| format!("reading the totals of {}", path.display()))?;
        these_totals
            .check()
            .with_context(|| format!("{} holds inconsistent totals", path.display()))?;
        // Each process's own counts are the totals' share of it: the
        // summary reads rates from the one and counts from the other.
        for (name, own, total) in [
            (
                "accepted",
                these.iter().map(|p| p.accepted).sum::<u64>(),
                these_totals.accepted,
            ),
            (
                "rejected",
                these.iter().map(|p| p.rejected).sum(),
                these_totals.rejected,
            ),
            (
                "no_response",
                these.iter().map(|p| p.no_response).sum(),
                these_totals.no_response(),
            ),
            (
                "offers_minted",
                these.iter().map(|p| p.offers_minted).sum(),
                these_totals.offers_minted,
            ),
        ] {
            ensure!(
                own == total,
                "{} holds inconsistent totals: its processes' {name} add up to {own}, its totals \
                 say {total}",
                path.display()
            );
        }
        processes.extend(these);
        totals
            .merge(&these_totals)
            .with_context(|| format!("adding {}", path.display()))?;
    }
    document("merged", &processes, &totals)
}

/// The figures people quote, derived from the processes and the totals.
pub fn summary(processes: &[Process], totals: &Totals) -> Value {
    let windows: Vec<&Window> = processes.iter().filter_map(|p| p.window.as_ref()).collect();
    let earliest = windows.iter().map(|w| w.started_unix_ms).min();
    let latest = windows.iter().map(|w| w.ended_unix_ms).max();
    let overlap_start = windows.iter().map(|w| w.started_unix_ms).max();
    let overlap_end = windows.iter().map(|w| w.ended_unix_ms).min();
    let overlap = match (overlap_start, overlap_end) {
        (Some(start), Some(end)) if end > start => Some((start, end)),
        _ => None,
    };
    let per_process_rate = |count: fn(&Process) -> u64| -> Option<f64> {
        let rated: Vec<f64> = processes
            .iter()
            .filter_map(|p| {
                p.window
                    .as_ref()
                    .filter(|w| w.seconds > 0.0)
                    .map(|w| count(p) as f64 / w.seconds)
            })
            .collect();
        (!rated.is_empty()).then(|| rated.iter().sum())
    };
    // Whole seconds every window covers: a second at either edge holds only
    // part of some process's load and would read as a dip.
    let steady: Vec<&Second> = match overlap {
        Some((start, end)) => {
            let first = start.div_euclid(1000) + 1;
            let last = end.div_euclid(1000) - 1;
            totals
                .timeline
                .seconds()
                .filter(|s| s.unix_second >= first && s.unix_second <= last)
                .collect()
        }
        None => Vec::new(),
    };
    let steady_seconds = match overlap {
        Some((start, end)) => (end.div_euclid(1000) - start.div_euclid(1000) - 1).max(0),
        None => 0,
    };
    let steady_rate = if steady_seconds > 0 {
        // A covered second with nothing accepted has no timeline entry of its
        // own when nothing else happened in it; it still counts as 0.
        let mut accepted: Vec<u64> = steady.iter().map(|s| s.accepted).collect();
        accepted.resize(steady_seconds as usize, 0);
        let sum: u64 = accepted.iter().sum();
        json!({
            "seconds": steady_seconds,
            "min": accepted.iter().min(),
            "mean": sum as f64 / steady_seconds as f64,
            "max": accepted.iter().max(),
            "unavailable_reason": null,
        })
    } else {
        json!({
            "seconds": 0,
            "min": null, "mean": null, "max": null,
            "unavailable_reason": "no whole second is inside every process's load window",
        })
    };
    let failures_unrecorded: u64 = totals
        .client_failures
        .iter()
        .filter(|(kind, _)| kind.as_str() == "offer")
        .map(|(_, stat)| stat.count.saturating_sub(stat.recorded))
        .sum();
    let accounted = totals.sent()
        + failures_unrecorded
        + totals.offers_discarded
        + totals.offers_unknown_at_abort;
    let spreads: Vec<f64> = totals
        .tips
        .tips
        .values()
        .map(|tip| tip.last_seen_unix_ms.saturating_sub(tip.first_seen_unix_ms) as f64)
        .collect();
    // A process that dropped its oldest tips leaves the tips it kept short of
    // its sightings of the ones it dropped, and a merge cannot tell which
    // they were, so the distinct count is unknown rather than estimated.
    let tips_partial = totals.tips.dropped > 0;
    json!({
        "processes": processes.len(),
        "labels": processes.iter().map(|p| p.label.clone()).collect::<Vec<_>>(),
        "targets": processes.iter().map(|p| p.target.clone()).collect::<std::collections::BTreeSet<_>>(),
        "sessions": processes.iter().map(|p| p.sessions).sum::<usize>(),
        "offered_rate": processes.iter().map(|p| p.rate).sum::<f64>(),
        "ended": processes
            .iter()
            .map(|p| json!({"label": p.label, "ended": p.ended}))
            .collect::<Vec<_>>(),
        "window": {
            "earliest_start": earliest.map(rfc3339),
            "latest_end": latest.map(rfc3339),
            "overlap_seconds": overlap.map(|(start, end)| (end - start) as f64 / 1000.0),
            "processes_without_a_window": processes.iter().filter(|p| p.window.is_none()).count(),
        },
        "offers": {
            "minted": totals.offers_minted,
            "dispatched": totals.offers_dispatched,
            "shortfall": totals.offers_shortfall,
            "discarded": totals.offers_discarded,
            "unknown_at_abort": totals.offers_unknown_at_abort,
            "above_difficulty_ceiling": totals.offers_above_difficulty_ceiling,
            "failed_before_sending": failures_unrecorded,
            "unaccounted": totals.offers_dispatched as i64 - accounted as i64,
        },
        "shares": {
            "sent": totals.sent(),
            "accepted": totals.accepted,
            "rejected": totals.rejected,
            "no_response": totals.no_response(),
            "no_response_run_ended": totals.no_response_run_ended,
            "no_response_mid_run": totals.no_response_mid_run,
            "no_response_reasons": totals.no_response_reasons,
        },
        "rates": {
            "offered_per_second": per_process_rate(|p| p.offers_minted),
            "accepted_per_second": per_process_rate(|p| p.accepted),
            "accepted_per_second_by_wall_second": steady_rate,
        },
        "ack_latency": totals.ack_latency.summary(LATENCY_CLOCK),
        "rejection_latency": totals.rejection_latency.summary(LATENCY_CLOCK),
        "rejections": {
            "by_class": totals.rejections.by_class(),
            "by_reason": totals.rejections.by_reason(),
        },
        "connections": {
            "initial": totals.initial_connections,
            "opened": totals.connections_opened,
            "time_to_first_job": totals.time_to_first_job.summary(LATENCY_CLOCK),
            "initial_connect_failures": totals.initial_connect_failures,
            "initial_connect_errors": totals.initial_connect_errors,
        },
        "reconnects": {
            "disconnects": totals.disconnects,
            "disconnect_causes": totals.disconnect_causes,
            "completed": totals.reconnects_completed,
            "failed_attempts": totals.reconnect_failed_attempts,
            "errors": totals.reconnect_errors,
            "outage": totals.reconnect_outage.summary(LATENCY_CLOCK),
        },
        "difficulty": {
            "requested": processes.iter().map(|p| p.requested_difficulty).collect::<Vec<_>>(),
            "ceiling": processes.iter().map(|p| p.max_difficulty).collect::<Vec<_>>(),
            "advertisements": totals.difficulty_advertisements,
            "advertised_min": totals.advertised_difficulty_min,
            "advertised_max": totals.advertised_difficulty_max,
            "offers_above_ceiling": totals.offers_above_difficulty_ceiling,
        },
        "jobs": {
            "notifies": totals.notifies,
            "clean_jobs": totals.clean_jobs,
            "discarded_block_solutions": totals.discarded_block_solutions,
        },
        "tips": {
            "kept": totals.tips.tips.len(),
            "dropped_by_processes": totals.tips.dropped,
            "distinct": (!tips_partial).then_some(totals.tips.tips.len()),
            "distinct_unavailable_reason": tips_partial.then_some(
                "a process saw more tips than it keeps and dropped its oldest"
            ),
            "fan_out_max_milliseconds": spreads.iter().copied().reduce(f64::max),
            "fan_out_p50_milliseconds": crate::gate::nearest_rank(&spreads, 0.5),
        },
        "client_failures": totals.client_failures,
    })
}

fn rfc3339(unix_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(unix_ms)
        .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| unix_ms.to_string())
}

/// What each figure means, carried in every document (EP-OBSERVABILITY).
pub fn definitions() -> Value {
    json!({
        "scope": "Client-side only. The process sees the target's answers, not its ledger: an \
                  accepted share is one the target acknowledged, and nothing here shows it is \
                  durable. Reconcile the --share-log ids against the target's database for that.",
        "offers": "An open-loop token bucket mints --rate offers a second over the load window and \
                   gives each to the next session that holds work and has nothing outstanding; \
                   one no such session can take is shortfall, never queued, so an outage shows \
                   as shortfall rather than as offers held for sessions without a connection. \
                   A dispatched offer is mined and sent, fails before sending \
                   (failed_before_sending, including above_difficulty_ceiling), is discarded \
                   unsent when the run stops its session, or is unknown_at_abort: held by a \
                   session that did not stop in time, sent or not. unaccounted is what none of \
                   those covers, and is 0 for a complete run.",
        "shares": "Every submit sent is accepted, rejected or no_response. no_response_mid_run \
                   lost its connection before the answer came (the target or the path went \
                   away); no_response_run_ended was still unanswered when the drain ended.",
        "rates": "offered_per_second and accepted_per_second add each process's count over its \
                  own load window. Every offer is made inside the window, and accepted counts \
                  the answers to them, including the few read in the drain after it, as the \
                  harness counts a phase's submits. accepted_per_second_by_wall_second takes \
                  the accepted answers read in each whole wall-clock second inside every \
                  process's window, summed over the processes, so it depends on the machines' \
                  clocks agreeing.",
        "ack_latency": "From the submit's write to the session reading its answer, on the client's \
                        monotonic clock: the network, any load balancer and the target's \
                        acknowledgement path. Percentiles come from a histogram that adds \
                        exactly across processes.",
        "reconnects": "A disconnect is a connection the run did not close. A completed \
                       reconnect's outage runs from losing the connection to holding work \
                       again, every failed attempt (retried every 250 ms) included.",
        "time_to_first_job": "A session's first connection, from the start of the attempt that \
                              succeeded to its first job; the attempts that failed before it \
                              are initial_connect_failures, retried every 250 ms.",
        "difficulty": "Each job is mined at the difficulty the target advertised before it. An \
                       offer whose job is above the process's ceiling is not mined.",
        "tips": "Per tip, from the first session to the last to hold work on it, each session \
                 counted once however often it comes back to the tip; across processes this \
                 depends on their clocks agreeing. A process keeps its newest 1,024 tips: past \
                 that the distinct count is unknown and the fan-out covers the tips kept.",
        "timeline": "totals.timeline: one entry per wall-clock second in which something \
                     happened, placed on the wall clock through one anchor taken at process start, \
                     so a clock step mid-run moves nothing; sessions_holding_work is sampled once \
                     a second during the load window and summed over the processes that sampled.",
    })
}
