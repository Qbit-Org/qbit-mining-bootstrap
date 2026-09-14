//! The dense-cadence scenario: own blocks landing about 9 s apart and in
//! 18–20 s pairs, and what each payout-revision bump costs every frontend.
//!
//! #271's acceptance criterion 6 asks for the shape #224 recorded — an
//! accepted-candidate minimum interarrival of about 9.14 s, 35 accepted blocks
//! in the trailing hour, and repeated pairs about 18–20 s apart — and for a
//! per-frontend answer to two questions: how long does a frontend reject
//! shares with `new payout work is pending`, and how many shares does it
//! reject before it serves work at the new revision. The answer sets the
//! budget for #291's soak.
//!
//! Four clocks appear here, and every reported time names the one it came
//! from (EP-OBSERVABILITY):
//!
//! - **harness monotonic** (`std::time::Instant`), reported as milliseconds
//!   since the phase's own start instant, and used for every duration;
//! - **harness wall clock** (UTC), stamped when the harness asked a session
//!   for a landing;
//! - **fake-node wall clock** (UTC), stamped inside `submitblock` and at each
//!   tip transition;
//! - **PostgreSQL's `clock_timestamp()`**, read in the same row as
//!   `payout_revision`, so a bump carries the server's own notion of when it
//!   was visible.
//!
//! Nothing in this module reads production code's behaviour differently from
//! the rest of the harness: the revision sampler runs one read-only
//! `SELECT` on the harness's side pool, outside the frontends' path and
//! outside the delay proxy.

use crate::{
    classify::{self, Rejection},
    client::{ClientFailure, FailureKind, NotifySighting, Outcome, SubmitRecord, TipSighting},
    measure,
    node::{SubmissionRecord, TipChange, TipOrigin},
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

/// The side phase's name, in the side report and in every `SubmitRecord`.
pub const PHASE: &str = "dense_cadence";

/// #224's shape: 9 s single gaps, and 18–20 s pairs.
pub const DEFAULT_GAPS: &str = "9,19,9,18,20";
/// A gap shorter than this cannot be measured: a frontend's rebuild alone
/// takes `PRISM_BLOCKPOLL_SECONDS` plus build time, so two landings closer
/// than 5 s would share one rebuild and no window could be attributed to
/// either (EP-VALIDATION).
pub const MIN_GAP_SECONDS: f64 = 5.0;
/// #271 asks for at least ten landings in the phase.
pub const MIN_LANDINGS: usize = 10;
/// Seconds of load before the first landing, so the phase is already at its
/// offered rate when the first block lands.
pub const LEAD_IN_SECONDS: f64 = 5.0;
/// Seconds reserved after the last landing, so its windows are observed
/// inside the phase rather than truncated by its end.
pub const TAIL_SECONDS: f64 = 15.0;
/// How often `payout_revision` is sampled during the phase.
pub const REVISION_SAMPLE_INTERVAL_MS: u64 = 25;
/// A defensive ceiling on the generated schedule. With gaps of at least
/// `MIN_GAP_SECONDS` a phase would have to run for hours to reach it.
const MAX_LANDINGS: usize = 4096;

/// Which cadence scenario a run asked for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cadence {
    /// No dense-cadence phase. This is what every run before #271's criterion
    /// 6 did, and what the default still does.
    None,
    /// The `dense_cadence` side phase.
    Dense,
}

impl Cadence {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "none" => Ok(Self::None),
            "dense" => Ok(Self::Dense),
            other => anyhow::bail!("unknown cadence {other:?}; use none or dense"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Dense => "dense",
        }
    }

    pub fn is_dense(self) -> bool {
        self == Self::Dense
    }
}

/// Parse `--cadence-gaps` into seconds, refusing anything unmeasurable at the
/// entry boundary (EP-VALIDATION).
pub fn parse_gaps(text: &str) -> Result<Vec<f64>> {
    let mut gaps = Vec::new();
    for (position, field) in text.split(',').enumerate() {
        let trimmed = field.trim();
        ensure!(
            !trimmed.is_empty(),
            "--cadence-gaps entry {} is empty; give a comma-separated list of seconds",
            position + 1
        );
        let value: f64 = trimmed.parse().with_context(|| {
            format!(
                "--cadence-gaps entry {} ({trimmed:?}) is not a number of seconds",
                position + 1
            )
        })?;
        ensure!(
            value.is_finite(),
            "--cadence-gaps entry {} ({value}) must be finite",
            position + 1
        );
        ensure!(
            value >= MIN_GAP_SECONDS,
            "--cadence-gaps entry {} ({value}) is below the {MIN_GAP_SECONDS} s floor; a shorter \
             gap shares one frontend rebuild between two landings, so neither landing's window \
             could be attributed",
            position + 1
        );
        gaps.push(value);
    }
    ensure!(
        !gaps.is_empty(),
        "--cadence-gaps must name at least one gap"
    );
    Ok(gaps)
}

/// The landing offsets the gap pattern places inside a phase of
/// `phase_seconds`, in seconds from the phase's start.
///
/// The first landing is at `LEAD_IN_SECONDS`; each later one is the previous
/// plus the next gap, cycling through the pattern. A landing is only placed if
/// `TAIL_SECONDS` still fit after it, so the last landing's windows are
/// measured inside the phase.
pub fn landing_offsets(gaps: &[f64], phase_seconds: f64) -> Vec<f64> {
    let latest = phase_seconds - TAIL_SECONDS;
    let mut offsets = Vec::new();
    if gaps.is_empty() {
        return offsets;
    }
    let mut at = LEAD_IN_SECONDS;
    let mut index = 0usize;
    while at <= latest && offsets.len() < MAX_LANDINGS {
        offsets.push(at);
        let gap = gaps[index % gaps.len()];
        if gap <= 0.0 || !gap.is_finite() {
            break;
        }
        at += gap;
        index += 1;
    }
    offsets
}

/// Validate `--cadence-gaps` against `--cadence-seconds` and return the gaps.
///
/// Refuses at entry, and says why, when the pattern cannot place
/// `MIN_LANDINGS` landings inside the phase.
pub fn validate(gaps_text: &str, phase_seconds: u64) -> Result<Vec<f64>> {
    let gaps = parse_gaps(gaps_text)?;
    let offsets = landing_offsets(&gaps, phase_seconds as f64);
    ensure!(
        offsets.len() >= MIN_LANDINGS,
        "--cadence-gaps {gaps_text:?} places only {} landing(s) in a {phase_seconds} s phase, and \
         #271 asks for at least {MIN_LANDINGS}. The schedule starts at {LEAD_IN_SECONDS} s and \
         reserves the last {TAIL_SECONDS} s for the final landing's windows, so raise \
         --cadence-seconds to at least {} or shorten the gaps",
        offsets.len(),
        required_seconds(&gaps).ceil() as u64
    );
    Ok(gaps)
}

/// The shortest phase that holds `MIN_LANDINGS` landings at this pattern.
pub fn required_seconds(gaps: &[f64]) -> f64 {
    if gaps.is_empty() {
        return f64::INFINITY;
    }
    let mut at = LEAD_IN_SECONDS;
    for index in 0..MIN_LANDINGS.saturating_sub(1) {
        at += gaps[index % gaps.len()];
    }
    at + TAIL_SECONDS
}

// --- payout-revision sampling --------------------------------------------

/// One reading of `qbit_prism_cluster.payout_revision`, on both clocks.
#[derive(Clone, Debug)]
pub struct RevisionSample {
    pub revision: i64,
    /// The server's `clock_timestamp()`, read in the same row.
    pub server_timestamp: DateTime<Utc>,
    /// The harness's monotonic clock, stamped when the row came back.
    pub monotonic: Instant,
    /// The revision this reading replaced. `None` for the baseline.
    pub previous_revision: Option<i64>,
}

/// What the sampler saw over one phase.
#[derive(Clone, Debug, Default)]
pub struct RevisionSeries {
    pub interval_ms: u64,
    pub samples: u64,
    pub errors: u64,
    /// The first sampler error, verbatim. A sampler that failed is reported as
    /// having failed, never as having seen nothing (EP-ERRORS).
    pub first_error: Option<String>,
    /// The revision the phase started at.
    pub baseline: Option<RevisionSample>,
    /// Every observed change, in order.
    pub changes: Vec<RevisionSample>,
}

impl RevisionSeries {
    /// True when the bump list is unknown rather than empty.
    ///
    /// A sampler that never read anything is blind, and so is one whose first
    /// read succeeded and whose every later read failed: it observed no
    /// change, but it also saw almost nothing, and `changes_observed: 0` with
    /// `blind: false` is the misreading this flag exists to prevent
    /// (EP-ERRORS). Errors beside observed changes are not blindness: the
    /// sampler did see the revision move, and `coverage` says how much of the
    /// phase it watched.
    pub fn blind(&self) -> bool {
        self.baseline.is_none() || (self.changes.is_empty() && self.errors > 0)
    }

    /// Why the sampler is blind, when it is.
    pub fn blind_reason(&self) -> Option<&'static str> {
        match (self.baseline.is_none(), self.changes.is_empty() && self.errors > 0) {
            (true, _) => Some(
                "the sampler produced no reading, so the bump list is unknown rather than empty",
            ),
            (false, true) => Some(
                "the sampler read a baseline and then failed; it observed no change, but it                  also observed almost nothing, so an empty bump list is unknown rather than                  empty. coverage says what fraction of its ticks returned a reading",
            ),
            (false, false) => None,
        }
    }

    /// Fraction of the sampler's ticks that returned a reading: `samples` over
    /// `samples + errors`. `None` when it never ticked, which is not 0.
    pub fn coverage(&self) -> Option<f64> {
        let ticks = self.samples + self.errors;
        (ticks > 0).then(|| self.samples as f64 / ticks as f64)
    }
}

/// Samples `payout_revision` on the harness's side pool for the life of a
/// phase.
///
/// `qbit_prism_cluster` is the singleton row `Ledger::payout_revision()` reads
/// (`crates/qbit-prism-server/src/ledger/connect.rs`), bumped by
/// `observe_chain_view` (`ledger/window.rs`) and by candidate confirmation or
/// abandonment (`ledger/blocks.rs`). The read takes no advisory lock and no
/// row lock, so it cannot perturb what it measures.
pub struct RevisionSampler {
    series: Arc<Mutex<RevisionSeries>>,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RevisionSampler {
    pub fn start(pool: PgPool, interval: Duration) -> Self {
        let series = Arc::new(Mutex::new(RevisionSeries {
            interval_ms: interval.as_millis() as u64,
            ..Default::default()
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let task = {
            let series = series.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut last: Option<i64> = None;
                while !stop.load(Ordering::Relaxed) {
                    ticker.tick().await;
                    let read = sqlx::query_as::<_, (i64, DateTime<Utc>)>(
                        "SELECT payout_revision, clock_timestamp() FROM qbit_prism_cluster \
                         WHERE singleton",
                    )
                    .fetch_one(&pool)
                    .await;
                    let at = Instant::now();
                    let mut state = series.lock().expect("revision sampler lock");
                    match read {
                        Ok((revision, server_timestamp)) => {
                            state.samples += 1;
                            let sample = RevisionSample {
                                revision,
                                server_timestamp,
                                monotonic: at,
                                previous_revision: last,
                            };
                            match last {
                                None => state.baseline = Some(sample),
                                Some(previous) if previous != revision => {
                                    state.changes.push(sample)
                                }
                                Some(_) => {}
                            }
                            last = Some(revision);
                        }
                        Err(error) => {
                            state.errors += 1;
                            if state.first_error.is_none() {
                                state.first_error = Some(format!("{error}"));
                            }
                        }
                    }
                }
            })
        };
        Self { series, stop, task }
    }

    /// Stop sampling and take what was seen.
    pub async fn finish(self) -> RevisionSeries {
        self.stop.store(true, Ordering::Relaxed);
        let _ = tokio::time::timeout(Duration::from_secs(5), self.task).await;
        std::mem::take(&mut *self.series.lock().expect("revision sampler lock"))
    }
}

// --- landings -------------------------------------------------------------

/// One landing the harness asked for.
#[derive(Clone, Debug)]
pub struct Landing {
    pub index: usize,
    /// Where the gap pattern put it, in seconds from the phase's start.
    pub scheduled_offset_seconds: f64,
    /// When the harness sent `Control::ScheduledBlock`, on both clocks.
    pub requested_monotonic: Instant,
    pub requested_wall: DateTime<Utc>,
    pub session: usize,
    pub frontend: usize,
}

/// A frontend's health across the phase, so a window measured while it was
/// restarting is marked incomplete rather than reported as a clean number.
#[derive(Clone, Debug)]
pub struct FrontendHealth {
    pub index: usize,
    pub instance_id: String,
    pub restarts_before: usize,
    pub restarts_after: usize,
    /// Set when the frontend exited during the phase.
    pub exited: Option<String>,
}

impl FrontendHealth {
    fn restarted(&self) -> bool {
        self.restarts_after > self.restarts_before
    }

    fn trouble(&self) -> Option<String> {
        match (self.restarted(), &self.exited) {
            (_, Some(status)) => Some(format!(
                "{} exited during the phase ({status})",
                self.instance_id
            )),
            (true, None) => Some(format!(
                "{} restarted {} time(s) during the phase",
                self.instance_id,
                self.restarts_after - self.restarts_before
            )),
            (false, None) => None,
        }
    }
}

/// Everything the report builder needs. It is deliberately a plain data
/// struct over already-collected records: the builder is pure, so attribution
/// is unit-testable without a database (EP-STATE).
pub struct ReportInputs<'a> {
    pub cadence: Cadence,
    pub gaps: &'a [f64],
    pub offsets: &'a [f64],
    pub phase_seconds: u64,
    pub phase_rate: f64,
    pub phase_started: Instant,
    pub phase_started_wall: DateTime<Utc>,
    pub phase_ended: Instant,
    pub phase_duration_millis: u64,
    /// `--scheduled-blocks`, which is the dense phase's landing budget.
    pub landing_budget: usize,
    /// Schedule slots the budget could not pay for.
    pub slots_over_budget: usize,
    pub landings: &'a [Landing],
    pub revisions: Option<&'a RevisionSeries>,
    pub submits: &'a [SubmitRecord],
    pub notifies: &'a [NotifySighting],
    pub tips: &'a [TipSighting],
    pub node_submissions: &'a [SubmissionRecord],
    pub tip_changes: &'a [TipChange],
    /// Each session's frontend, snapshotted at the end of the phase.
    pub session_frontend: &'a [usize],
    pub frontends: &'a [FrontendHealth],
    /// Every client failure of the run.
    pub failures: &'a [ClientFailure],
    /// This run's committed share identifiers, so "lost valid work" can be
    /// shown to be lost rather than asserted to be.
    pub committed: &'a BTreeSet<String>,
    pub aborted: Option<&'a str>,
}

/// How a landing ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LandingOutcome {
    /// The node accepted the block and the tip moved to it.
    Landed,
    /// The frontend or the node refused it.
    Rejected,
    /// The session never produced a submit for it.
    NeverProduced,
    /// The frontend accepted the share and candidate, but no `submitblock`
    /// was recorded before the phase ended.
    AcceptedWithoutNodeSubmission,
    /// The submit's answer never arrived.
    NoResponse,
}

impl LandingOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Landed => "landed",
            Self::Rejected => "rejected",
            Self::NeverProduced => "never_produced",
            Self::AcceptedWithoutNodeSubmission => "accepted_without_node_submission",
            Self::NoResponse => "no_response",
        }
    }
}

/// One landing, resolved against the submits, the node and the tip changes.
struct Resolved<'a> {
    landing: &'a Landing,
    submit: Option<&'a SubmitRecord>,
    block_hash: Option<String>,
    node: Option<&'a SubmissionRecord>,
    tip: Option<&'a TipChange>,
    outcome: LandingOutcome,
    failure: Option<&'a str>,
    /// The attribution span, for landings that really landed.
    span: Option<(Instant, Instant, bool)>,
}

fn rejection_of(record: &SubmitRecord) -> Option<&Rejection> {
    match &record.outcome {
        Outcome::Rejected(rejection) => Some(rejection),
        _ => None,
    }
}

/// The block hash a submit carried: the part of the share identifier after the
/// colon, which `coordinator.rs` builds from the header's display hash, and
/// which is therefore exactly the hash the fake node records for
/// `submitblock`.
fn block_hash_of(record: &SubmitRecord) -> Option<String> {
    record
        .share_id
        .rsplit_once(':')
        .map(|(_, hash)| hash.to_owned())
}

fn millis_since(origin: Instant, at: Instant) -> f64 {
    at.saturating_duration_since(origin).as_secs_f64() * 1000.0
}

/// Resolve every landing, then give the ones that landed a contiguous
/// attribution span so each later rejection and bump belongs to exactly one
/// landing or to nothing (EP-STATE).
fn resolve<'a>(inputs: &ReportInputs<'a>) -> Vec<Resolved<'a>> {
    // Scheduled-block submits of this phase, oldest first, each consumed by at
    // most one landing.
    let mut candidates: Vec<&SubmitRecord> = inputs
        .submits
        .iter()
        .filter(|record| record.phase == PHASE && record.scheduled_block && !record.reoffer)
        .collect();
    candidates.sort_by_key(|record| record.sent);
    let mut taken = vec![false; candidates.len()];

    let mut resolved: Vec<Resolved<'a>> = Vec::with_capacity(inputs.landings.len());
    for (position, landing) in inputs.landings.iter().enumerate() {
        // A landing's own attempt window runs to the next landing's request,
        // so a session that answered late cannot be credited to the wrong one.
        let until = inputs
            .landings
            .get(position + 1)
            .map(|next| next.requested_monotonic)
            .unwrap_or(inputs.phase_ended);
        let mut submit = None;
        for (slot, record) in candidates.iter().enumerate() {
            if taken[slot]
                || record.session != landing.session
                || record.sent < landing.requested_monotonic
                || record.sent >= until
            {
                continue;
            }
            taken[slot] = true;
            submit = Some(*record);
            break;
        }
        let failure = inputs
            .failures
            .iter()
            .find(|failure| {
                failure.session == landing.session
                    && failure.kind == FailureKind::ScheduledBlock
                    && failure.at >= landing.requested_monotonic
                    && failure.at < until
            })
            .map(|failure| failure.error.as_str());
        let block_hash = submit.and_then(block_hash_of);
        let node = block_hash.as_ref().and_then(|hash| {
            inputs
                .node_submissions
                .iter()
                .find(|record| record.block_hash == *hash)
        });
        let tip = block_hash.as_ref().and_then(|hash| {
            inputs
                .tip_changes
                .iter()
                .find(|change| change.hash == *hash && change.origin == TipOrigin::Pool)
        });
        let outcome = match (submit, node, tip) {
            (None, _, _) => LandingOutcome::NeverProduced,
            (Some(record), _, _) if matches!(record.outcome, Outcome::NoResponse { .. }) => {
                LandingOutcome::NoResponse
            }
            (Some(record), _, _) if rejection_of(record).is_some() => LandingOutcome::Rejected,
            (Some(_), Some(submission), _) if !submission.accepted => LandingOutcome::Rejected,
            (Some(_), Some(_), Some(_)) => LandingOutcome::Landed,
            (Some(_), _, _) => LandingOutcome::AcceptedWithoutNodeSubmission,
        };
        resolved.push(Resolved {
            landing,
            submit,
            block_hash,
            node,
            tip,
            outcome,
            failure,
            span: None,
        });
    }

    // Spans run from one landing's tip change to the next one's, so the
    // intervals tile the phase without overlapping.
    let landed: Vec<usize> = {
        let mut order: Vec<usize> = resolved
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.tip.is_some())
            .map(|(index, _)| index)
            .collect();
        order.sort_by_key(|index| resolved[*index].tip.expect("tip present").monotonic);
        order
    };
    for (position, index) in landed.iter().enumerate() {
        let start = resolved[*index].tip.expect("tip present").monotonic;
        let (end, truncated) = match landed.get(position + 1) {
            Some(next) => (resolved[*next].tip.expect("tip present").monotonic, false),
            None => (inputs.phase_ended, true),
        };
        resolved[*index].span = Some((start, end, truncated));
    }
    resolved
}

/// One rebuild-pending rejection, already attributed.
struct PendingRejection<'a> {
    record: &'a SubmitRecord,
    at: Instant,
    payout: bool,
}

fn pending_rejections(submits: &[SubmitRecord]) -> Vec<PendingRejection<'_>> {
    submits
        .iter()
        .filter(|record| record.phase == PHASE && !record.reoffer)
        .filter_map(|record| {
            let rejection = rejection_of(record)?;
            if !classify::is_rebuild_pending(rejection) {
                return None;
            }
            // A rejection is stamped when the client read it, which is the
            // only instant the harness observed directly.
            let at = record.responded?;
            Some(PendingRejection {
                record,
                at,
                payout: rejection.message == classify::NEW_PAYOUT_WORK_PENDING,
            })
        })
        .collect()
}

/// First, last, count and duration of one window.
fn window(origin: Instant, times: &[Instant]) -> Value {
    let first = times.iter().min();
    let last = times.iter().max();
    match (first, last) {
        (Some(first), Some(last)) => json!({
            "count": times.len(),
            "first_millis_after_landing": millis_since(origin, *first),
            "last_millis_after_landing": millis_since(origin, *last),
            "duration_millis": millis_since(*first, *last),
        }),
        _ => json!({
            "count": 0,
            "first_millis_after_landing": Value::Null,
            "last_millis_after_landing": Value::Null,
            "duration_millis": Value::Null,
            "unavailable_reason": "this frontend returned no such rejection inside the landing's span",
        }),
    }
}

/// Accumulated per-frontend distributions, for the run summaries.
#[derive(Default)]
struct Distribution {
    tip_duration: Vec<f64>,
    payout_duration: Vec<f64>,
    combined_duration: Vec<f64>,
    tip_count: Vec<f64>,
    payout_count: Vec<f64>,
    combined_count: Vec<f64>,
    before_revision: Vec<f64>,
    lost: Vec<f64>,
    tip_work_max: Vec<f64>,
    revision_work_max: Vec<f64>,
}

impl Distribution {
    fn summarize(&self) -> Value {
        json!({
            "tip_pending_window_duration_millis": millis_summary(&self.tip_duration),
            "payout_pending_window_duration_millis": millis_summary(&self.payout_duration),
            "combined_rebuild_pending_window_duration_millis": millis_summary(&self.combined_duration),
            "tip_pending_rejections_per_landing": count_summary(&self.tip_count),
            "payout_pending_rejections_per_landing": count_summary(&self.payout_count),
            "combined_rebuild_pending_rejections_per_landing": count_summary(&self.combined_count),
            "rejected_before_new_revision_work_per_landing": count_summary(&self.before_revision),
            "lost_valid_shares_per_landing": count_summary(&self.lost),
            "time_to_new_tip_work_max_millis": millis_summary(&self.tip_work_max),
            "time_to_new_revision_work_max_millis": millis_summary(&self.revision_work_max),
            // The label travels with the summarised numbers, because these are
            // the ones a reader quotes. The per-landing tables carry the same
            // sibling; the summaries used to carry nothing, and the
            // definitions block is keyed on the unsuffixed field names, so a
            // reader looking either summary key up found nothing.
            "new_revision_work_approximation": NEW_REVISION_APPROXIMATION,
        })
    }
}

const CLOCK: &str = "harness monotonic";
/// A count distribution has one sample per landing and frontend. It is not on
/// a clock of its own: the rejections it counts are stamped on the harness's
/// monotonic clock, and the string says that rather than apologising for a
/// wrong unit -- the unit is now `count`.
const COUNT_CLOCK: &str = "one sample per landing and frontend; the rejections counted are \
                           stamped on the harness monotonic clock";

/// A distribution of milliseconds on the harness's monotonic clock.
fn millis_summary(values: &[f64]) -> measure::LatencySummary {
    measure::summarize(values.to_vec(), measure::MILLISECONDS, CLOCK)
}

/// A distribution of counts. These are shares, not milliseconds, and the unit
/// field has to say so: a consumer generic over the summary shape reads `unit`
/// and would render 85 discarded shares as "85 ms".
fn count_summary(values: &[f64]) -> measure::LatencySummary {
    measure::summarize(values.to_vec(), measure::COUNT, COUNT_CLOCK)
}

/// Build the `dense_cadence` section of the side report.
///
/// The section is additive: it carries no field any earlier consumer reads,
/// and the capacity-evidence artifact is untouched (EP-COMPAT).
pub fn build(inputs: &ReportInputs<'_>) -> Value {
    if !inputs.cadence.is_dense() {
        return json!({
            "ran": false,
            "cadence": inputs.cadence.as_str(),
            "reason": "the run did not ask for --cadence dense",
        });
    }
    let resolved = resolve(inputs);
    let rejections = pending_rejections(inputs.submits);
    let empty = RevisionSeries::default();
    let revisions = inputs.revisions.unwrap_or(&empty);

    // --- bumps ------------------------------------------------------------
    // Every change the sampler saw belongs to exactly one landing's span or
    // to nothing; nothing is dropped (EP-STATE).
    let mut bump_records = Vec::new();
    let mut attributed_bumps = 0usize;
    let mut unattributed_bumps = 0usize;
    let mut bumps_by_landing: BTreeMap<usize, Vec<&RevisionSample>> = BTreeMap::new();
    for change in &revisions.changes {
        let owner = resolved.iter().find(|entry| {
            entry
                .span
                .is_some_and(|(start, end, _)| change.monotonic >= start && change.monotonic < end)
        });
        match owner {
            Some(entry) => {
                attributed_bumps += 1;
                bumps_by_landing
                    .entry(entry.landing.index)
                    .or_default()
                    .push(change);
            }
            None => unattributed_bumps += 1,
        }
        bump_records.push(json!({
            "revision": change.revision,
            "previous_revision": change.previous_revision,
            "revision_delta": change
                .previous_revision
                .map(|previous| change.revision - previous),
            "server_timestamp": change.server_timestamp.to_rfc3339(),
            "harness_monotonic_millis_since_phase_start":
                millis_since(inputs.phase_started, change.monotonic),
            "attributed_to_landing": owner.map(|entry| entry.landing.index),
            "cause": match owner {
                Some(entry) => json!(format!(
                    "followed the pool tip {} at height {}",
                    entry
                        .tip
                        .map(|tip| tip.hash.clone())
                        .unwrap_or_default(),
                    entry.tip.map(|tip| tip.height).unwrap_or_default(),
                )),
                None => json!("unknown: the bump fell inside no landing's span"),
            },
        }));
    }

    // --- landings ---------------------------------------------------------
    let mut landing_records = Vec::new();
    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut per_frontend: BTreeMap<usize, Distribution> = BTreeMap::new();
    let mut overall = Distribution::default();
    let mut attributed_rejections = 0usize;

    for entry in &resolved {
        *counts.entry(entry.outcome.as_str()).or_insert(0) += 1;
        let mut frontend_tables = Vec::new();
        if let Some((start, end, truncated)) = entry.span {
            let reference = bumps_by_landing
                .get(&entry.landing.index)
                .and_then(|bumps| bumps.last().copied());
            for health in inputs.frontends {
                let sessions: Vec<usize> = inputs
                    .session_frontend
                    .iter()
                    .enumerate()
                    .filter(|(_, frontend)| **frontend == health.index)
                    .map(|(session, _)| session)
                    .collect();
                let mine: Vec<&PendingRejection<'_>> = rejections
                    .iter()
                    .filter(|rejection| {
                        rejection.record.frontend == health.index
                            && rejection.at >= start
                            && rejection.at < end
                    })
                    .collect();
                attributed_rejections += mine.len();
                let tip_times: Vec<Instant> = mine
                    .iter()
                    .filter(|rejection| !rejection.payout)
                    .map(|rejection| rejection.at)
                    .collect();
                let payout_times: Vec<Instant> = mine
                    .iter()
                    .filter(|rejection| rejection.payout)
                    .map(|rejection| rejection.at)
                    .collect();
                let combined_times: Vec<Instant> =
                    mine.iter().map(|rejection| rejection.at).collect();

                // Time to new-tip work, by the same definition
                // `run::time_to_usable_work` uses: the first mining.notify
                // whose prevhash resolves to the new tip, measured from the
                // node's tip stamp. The search ends at the span's end, as
                // the new-revision search below does: a notify for this tip
                // that arrives after the next landing's tip change is a late
                // job for a tip that is no longer the tip, and crediting it
                // here reported new-tip work inside a span that had none.
                // A frontend with no such job inside the span is its own
                // outcome, with its own reason (EP-STATE).
                let tip_hash = entry.tip.map(|tip| tip.hash.as_str()).unwrap_or_default();
                let mut tip_work: Vec<f64> = Vec::new();
                for session in &sessions {
                    if let Some(at) = inputs
                        .tips
                        .iter()
                        .filter(|sighting| {
                            sighting.session == *session
                                && sighting.tip == tip_hash
                                && sighting.at >= start
                                && sighting.at < end
                        })
                        .map(|sighting| sighting.at)
                        .min()
                    {
                        tip_work.push(millis_since(start, at));
                    }
                }
                let new_tip_work_unavailable_reason =
                    tip_work.is_empty().then_some(NO_NEW_TIP_WORK_IN_SPAN);

                // Time to new-revision work. The wire carries no payout
                // revision, so the first clean_jobs=true notify at or after
                // the bump stands in for it, and is labelled as an
                // approximation. The search ends at the span's end: the next
                // landing's own clean_jobs notify is that landing's work,
                // and a frontend that had not served the new revision by
                // then is reported as such rather than credited with the
                // later job -- which understated the time and stopped the
                // rejected-before count at the wrong event (EP-STATE).
                let mut revision_work: Vec<f64> = Vec::new();
                let mut first_revision_work: Option<Instant> = None;
                if let Some(bump) = reference {
                    for session in &sessions {
                        if let Some(at) = inputs
                            .notifies
                            .iter()
                            .filter(|notify| {
                                notify.session == *session
                                    && notify.clean_jobs
                                    && notify.at >= bump.monotonic
                                    && notify.at < end
                            })
                            .map(|notify| notify.at)
                            .min()
                        {
                            revision_work.push(millis_since(bump.monotonic, at));
                            first_revision_work = Some(
                                first_revision_work.map_or(at, |current: Instant| current.min(at)),
                            );
                        }
                    }
                }
                let rejected_before_new_revision_work = first_revision_work
                    .map(|at| mine.iter().filter(|rejection| rejection.at < at).count());
                // Unknown is not zero: a frontend with no new-revision work
                // inside the span is its own outcome, with its own reason.
                let new_revision_work_unavailable_reason =
                    match (reference, revision_work.is_empty()) {
                        (None, _) => Some(NO_REFERENCE_BUMP),
                        (Some(_), true) => Some(NO_NEW_REVISION_WORK_IN_SPAN),
                        (Some(_), false) => None,
                    };

                // Lost valid work: the client only ever offers a nonce that
                // meets the share target, so every rebuild-pending rejection
                // is a valid share the pool threw away. None of them is
                // persisted, which is checked here rather than asserted.
                let lost: Vec<&str> = mine
                    .iter()
                    .map(|rejection| rejection.record.share_id.as_str())
                    .collect();

                let incomplete = health.trouble().or_else(|| {
                    inputs
                        .aborted
                        .map(|reason| format!("the phase was aborted: {reason}"))
                });
                let distribution = per_frontend.entry(health.index).or_default();
                for target in [distribution, &mut overall] {
                    if let (Some(first), Some(last)) =
                        (tip_times.iter().min(), tip_times.iter().max())
                    {
                        target.tip_duration.push(millis_since(*first, *last));
                    }
                    if let (Some(first), Some(last)) =
                        (payout_times.iter().min(), payout_times.iter().max())
                    {
                        target.payout_duration.push(millis_since(*first, *last));
                    }
                    if let (Some(first), Some(last)) =
                        (combined_times.iter().min(), combined_times.iter().max())
                    {
                        target.combined_duration.push(millis_since(*first, *last));
                    }
                    target.tip_count.push(tip_times.len() as f64);
                    target.payout_count.push(payout_times.len() as f64);
                    target.combined_count.push(combined_times.len() as f64);
                    if let Some(count) = rejected_before_new_revision_work {
                        target.before_revision.push(count as f64);
                    }
                    target.lost.push(lost.len() as f64);
                    if let Some(worst) = tip_work.iter().copied().max_by(f64::total_cmp) {
                        target.tip_work_max.push(worst);
                    }
                    if let Some(worst) = revision_work.iter().copied().max_by(f64::total_cmp) {
                        target.revision_work_max.push(worst);
                    }
                }

                frontend_tables.push(json!({
                    "frontend": health.index,
                    "instance_id": health.instance_id,
                    "sessions": sessions.len(),
                    "tip_pending_window": window(start, &tip_times),
                    "payout_pending_window": window(start, &payout_times),
                    "combined_rebuild_pending_window":
                        window(start, &combined_times),
                    "reference_bump": reference.map(|bump| json!({
                        "revision": bump.revision,
                        "server_timestamp": bump.server_timestamp.to_rfc3339(),
                        "millis_after_landing": millis_since(start, bump.monotonic),
                    })),
                    "time_to_new_tip_work_millis": millis_summary(&tip_work),
                    "sessions_with_new_tip_work": tip_work.len(),
                    "new_tip_work_unavailable_reason": new_tip_work_unavailable_reason,
                    "time_to_new_revision_work_millis": millis_summary(&revision_work),
                    "sessions_with_new_revision_work": revision_work.len(),
                    "new_revision_work_unavailable_reason": new_revision_work_unavailable_reason,
                    "new_revision_work_approximation": NEW_REVISION_APPROXIMATION,
                    "rejected_before_new_revision_work": rejected_before_new_revision_work,
                    "rejected_before_new_revision_work_unavailable_reason":
                        new_revision_work_unavailable_reason,
                    "lost_valid_shares": lost.len(),
                    "lost_valid_shares_found_in_postgres": lost
                        .iter()
                        .filter(|share| inputs.committed.contains(**share))
                        .count(),
                    "incomplete": incomplete.is_some(),
                    "incomplete_reason": incomplete,
                    "span_truncated_at_phase_end": truncated,
                }));
            }
        }
        landing_records.push(json!({
            "index": entry.landing.index,
            "scheduled_offset_seconds": entry.landing.scheduled_offset_seconds,
            "requested_at": entry.landing.requested_wall.to_rfc3339(),
            "requested_millis_since_phase_start":
                millis_since(inputs.phase_started, entry.landing.requested_monotonic),
            "session": entry.landing.session,
            "frontend": entry.landing.frontend,
            "outcome": entry.outcome.as_str(),
            "block_hash": entry.block_hash,
            "submit": entry.submit.map(|record| json!({
                "share_id": record.share_id,
                "job_id": record.job_id,
                "sent_millis_since_phase_start": millis_since(inputs.phase_started, record.sent),
                "response_millis_since_phase_start":
                    record.responded.map(|at| millis_since(inputs.phase_started, at)),
                "send_to_response_millis": record.latency_millis,
                "answer": match &record.outcome {
                    Outcome::Accepted => json!({"outcome": "accepted"}),
                    Outcome::Rejected(rejection) => json!({
                        "outcome": "rejected",
                        "code": rejection.code,
                        "reason_id": rejection.reason_id,
                        "message": rejection.message,
                    }),
                    Outcome::NoResponse { reason } =>
                        json!({"outcome": "no-response", "reason": reason}),
                },
            })),
            "never_produced_error": entry.failure,
            "node": entry.node.map(|record| json!({
                "accepted": record.accepted,
                "rejection": record.rejection,
                "height": record.height,
                "parent": record.parent,
                "block_bytes": record.block_bytes,
                "received_at": record.received_at.to_rfc3339(),
            })),
            "tip_change": entry.tip.map(|tip| json!({
                "hash": tip.hash,
                "height": tip.height,
                "origin": tip.origin,
                "wall": tip.wall.to_rfc3339(),
                "millis_since_phase_start": millis_since(inputs.phase_started, tip.monotonic),
            })),
            "bumps": bumps_by_landing
                .get(&entry.landing.index)
                .map(|bumps| bumps.len())
                .unwrap_or(0),
            "bump_revisions": bumps_by_landing
                .get(&entry.landing.index)
                .map(|bumps| bumps.iter().map(|bump| bump.revision).collect::<Vec<_>>())
                .unwrap_or_default(),
            "frontends": frontend_tables,
        }));
    }

    // A landing is granted an attribution span on its pool tip change alone
    // (resolve), whatever its outcome, and a span is what a window is measured
    // over. Deriving these two from the `landed` tally instead let them
    // disagree with the windows the section actually carries: an
    // accepted_without_node_submission landing whose tip is present gets a full
    // per-frontend table and a contribution to summaries, and if it were the
    // only one the section would have said landings 0, windows_available false
    // and "every attempt failed" beside real windows and a real p99.
    // landing_outcomes.landed is still reported, one key away.
    let with_windows = resolved.iter().filter(|entry| entry.span.is_some()).count();
    let landed = *counts.get("landed").unwrap_or(&0);
    let unattributed_rejections = rejections.len().saturating_sub(attributed_rejections);

    // Lost valid work is counted over every rebuild-pending rejection in the
    // phase, not only the ones a landing's span owns. An unattributed
    // rejection is just as much a proven share the pool discarded, and it is
    // precisely the case the PostgreSQL cross-check exists for: a landing
    // whose pool tip change is missing leaves its rejections unattributed, and
    // a census that skipped them would read as a clean pass in the one
    // situation that would make it fail (EP-STATE).
    let lost_shares: Vec<&str> = rejections
        .iter()
        .map(|rejection| rejection.record.share_id.as_str())
        .collect();
    let lost_in_postgres: Vec<String> = lost_shares
        .iter()
        .filter(|share| inputs.committed.contains(**share))
        .map(|share| (*share).to_owned())
        .collect();
    let no_landing_reason = no_landing_reason(inputs, &resolved, with_windows);
    let combined_p99 = optional_summary(&overall.combined_duration, millis_summary)
        .and_then(|summary| summary.p99);
    let count_p99 =
        optional_summary(&overall.combined_count, count_summary).and_then(|summary| summary.p99);

    json!({
        "ran": true,
        "cadence": inputs.cadence.as_str(),
        "phase": PHASE,
        "in_artifact": false,
        "note": "a side phase: it is not part of the capacity-evidence artifact, and it runs with \
                 no proxy delay",
        "phase_seconds": inputs.phase_seconds,
        "phase_duration_millis": inputs.phase_duration_millis,
        "phase_started_at": inputs.phase_started_wall.to_rfc3339(),
        "offered_rate_shares_per_second": inputs.phase_rate,
        "gap_pattern_seconds": inputs.gaps,
        "gap_pattern_note": "repeated cyclically; #224's shape is 9 s single gaps and 18-20 s pairs",
        "lead_in_seconds": LEAD_IN_SECONDS,
        "tail_seconds": TAIL_SECONDS,
        "scheduled_landing_offsets_seconds": inputs.offsets,
        "landing_budget": inputs.landing_budget,
        "schedule_slots_over_budget": inputs.slots_over_budget,
        "landings": with_windows,
        // Every observed change of payout_revision, which is what
        // definitions.bump calls a bump. The split between the ones a
        // landing's span owns and the rest is in bump_attribution, two keys
        // below; publishing the attributed subset under the bare word made the
        // headline number mean something other than its own definition.
        "bumps": revisions.changes.len(),
        "landing_attempts": resolved.len(),
        "windows_available": with_windows > 0,
        "reason": no_landing_reason,
        "revision_sampler": {
            "source": "SELECT payout_revision, clock_timestamp() FROM qbit_prism_cluster WHERE singleton",
            "interval_millis": if revisions.interval_ms == 0 {
                REVISION_SAMPLE_INTERVAL_MS
            } else {
                revisions.interval_ms
            },
            "samples": revisions.samples,
            "errors": revisions.errors,
            "coverage": revisions.coverage(),
            "coverage_definition": "samples / (samples + errors): the fraction of the sampler's \
                                    ticks that returned a reading. Null, not 0, when it never \
                                    ticked. A sampler that read 3% of its ticks is visible as \
                                    such whatever blind says.",
            "first_error": revisions.first_error,
            "blind": revisions.blind(),
            "blind_reason": revisions.blind_reason(),
            "baseline_revision": revisions.baseline.as_ref().map(|sample| sample.revision),
            "baseline_server_timestamp": revisions
                .baseline
                .as_ref()
                .map(|sample| sample.server_timestamp.to_rfc3339()),
            "changes_observed": revisions.changes.len(),
        },
        "landing_records": landing_records,
        "landing_outcomes": {
            "landed": landed,
            "rejected": counts.get("rejected").copied().unwrap_or(0),
            "never_produced": counts.get("never_produced").copied().unwrap_or(0),
            "accepted_without_node_submission":
                counts.get("accepted_without_node_submission").copied().unwrap_or(0),
            "no_response": counts.get("no_response").copied().unwrap_or(0),
        },
        "bump_records": bump_records,
        "bump_attribution": {
            "observed": revisions.changes.len(),
            "attributed": attributed_bumps,
            "unattributed": unattributed_bumps,
            "note": "a bump is attributed to the landing whose pool tip change it follows and \
                     which the next landing has not yet replaced; anything else is unattributed \
                     with its cause unknown. observed is the top-level bumps count, and \
                     attributed + unattributed equals it.",
        },
        "rejection_attribution": {
            "rebuild_pending_rejections_in_phase": rejections.len(),
            "attributed": attributed_rejections,
            "unattributed": unattributed_rejections,
            "note": "a rebuild-pending rejection outside every landing's span is counted as \
                     unattributed, never dropped",
        },
        "lost_valid_work": {
            "definition": "a share the client had already proven against the share target, \
                           rejected with new tip work is pending or new payout work is pending. \
                           It was never persisted, so it is not a durability finding, but it is \
                           miner work the pool discarded.",
            "shares": lost_shares.len(),
            "shares_attributed": attributed_rejections,
            "shares_unattributed": unattributed_rejections,
            "split_note": "shares counts every rebuild-pending rejection in the phase and is what \
                           shares_found_in_postgres is checked over. shares_attributed is the \
                           subset a landing's span owns, which is what the per-landing, \
                           per-frontend lost_valid_shares tables sum to; the rest fell inside no \
                           span and is counted here rather than leaving the census.",
            "shares_found_in_postgres": lost_in_postgres.len(),
            "shares_found_in_postgres_sample": lost_in_postgres.iter().take(10).collect::<Vec<_>>(),
        },
        "frontend_health": inputs.frontends.iter().map(|health| json!({
            "frontend": health.index,
            "instance_id": health.instance_id,
            "restarts_before_phase": health.restarts_before,
            "restarts_after_phase": health.restarts_after,
            "restarted_during_phase": health.restarted(),
            "exited_during_phase": health.exited,
            "windows_incomplete": health.trouble().is_some() || inputs.aborted.is_some(),
        })).collect::<Vec<_>>(),
        "summaries": {
            "per_frontend": per_frontend.iter().map(|(frontend, distribution)| {
                let mut value = distribution.summarize();
                value["frontend"] = json!(frontend);
                value
            }).collect::<Vec<_>>(),
            "overall": overall.summarize(),
        },
        "proposed_budget_for_issue_291": {
            "metric": "p99 of the combined rebuild-pending window per landing, per frontend",
            "window_p99_millis": combined_p99,
            "rejections_per_landing_per_frontend_p99": count_p99,
            "recommended_soak_budget_millis": combined_p99
                .map(|p99| (p99 / 100.0).ceil() * 100.0),
            "note": "the soak in #291 should fail if either number is exceeded at the same \
                     topology. Both are unknown, not zero, when this run produced no landing.",
        },
        "definitions": definitions(),
    })
}

/// Why a run produced no landing, when it produced none.
fn no_landing_reason(
    inputs: &ReportInputs<'_>,
    resolved: &[Resolved<'_>],
    with_windows: usize,
) -> Option<String> {
    if with_windows > 0 {
        return None;
    }
    if inputs.landing_budget == 0 {
        return Some(format!(
            "the landing budget --scheduled-blocks was 0, so the {} scheduled slot(s) in the \
             pattern were never used; no window is reported and none is invented",
            inputs.offsets.len()
        ));
    }
    if inputs.offsets.is_empty() {
        return Some(
            "the gap pattern placed no landing inside the phase; no window is reported".to_owned(),
        );
    }
    if resolved.is_empty() {
        return Some(
            "the phase ended before any scheduled slot came due; no window is reported".to_owned(),
        );
    }
    Some(format!(
        "all {} landing attempt(s) failed: {}",
        resolved.len(),
        {
            let mut tally: BTreeMap<&str, usize> = BTreeMap::new();
            for entry in resolved {
                *tally.entry(entry.outcome.as_str()).or_insert(0) += 1;
            }
            tally
                .into_iter()
                .map(|(kind, count)| format!("{kind}={count}"))
                .collect::<Vec<_>>()
                .join(" ")
        }
    ))
}

/// How the first `clean_jobs` notify stands in for a revision the wire never
/// carries.
pub const NEW_REVISION_APPROXIMATION: &str =
    "approximate: mining.notify carries no payout revision, so the first notify with \
     clean_jobs=true at or after the bump and before the end of the landing's span stands in \
     for the first job built at the new revision. clean_jobs is set when the parent or the \
     payout revision differs from the session's last job (stratum.rs, deliver_job), so inside \
     a landing's span, after the tip has already been served, it is the rebuild at the new \
     revision. A clean_jobs notify after the span is the next landing's work and is never \
     counted for this one.";

/// Why a frontend's new-tip figures are absent: no session on the frontend
/// saw a job built on the landing's tip before the span ended. A notify for
/// that tip after the span is a late job for a tip that has already been
/// replaced, and is not borrowed.
pub const NO_NEW_TIP_WORK_IN_SPAN: &str =
    "no session on this frontend received a job built on the landing's tip between the tip \
     change and the end of the landing's span: the frontend had not served the new tip before \
     the next landing's tip change (or the phase's end), and a notify for this tip that arrived \
     after the span is a late job for a replaced tip, not counted for it";

/// Why a frontend's new-revision figures are absent: no bump to measure from.
pub const NO_REFERENCE_BUMP: &str = "no bump was attributed to this landing";
/// Why a frontend's new-revision figures are absent: the bump happened, but
/// no session on the frontend saw a job at the new revision before the span
/// ended. The next landing's job is its own and is not borrowed.
pub const NO_NEW_REVISION_WORK_IN_SPAN: &str =
    "no session on this frontend received a clean_jobs job between the bump and the end of \
     the landing's span: the frontend had not served work at the new revision before the next \
     landing's tip change (or the phase's end), and the next landing's job is not borrowed \
     for it";

/// Exactly how every window and time in this section is measured, and on which
/// clock.
pub fn definitions() -> Value {
    json!({
        "clocks": {
            "harness_monotonic": "std::time::Instant in the harness process. Every duration and \
                                  every *_millis_* field is on this clock, and offsets are \
                                  measured from the phase's start instant or from the landing's \
                                  tip change, as the field name says.",
            "harness_wall": "chrono::Utc::now() in the harness process, stamped when the harness \
                             asked a session for a landing (requested_at).",
            "node_wall": "chrono::Utc::now() inside the fake node, stamped when submitblock was \
                          answered (node.received_at) and at each tip transition (tip_change.wall).",
            "postgres_clock_timestamp": "clock_timestamp() read in the same row as \
                                         payout_revision, so a bump carries the server's own \
                                         notion of when the new value was visible."
        },
        "landing": "one own block: the harness sends Control::ScheduledBlock, the session searches \
                    its current job for a network-target solution and submits it, the frontend \
                    appends the share with a candidate, and the candidate worker calls submitblock. \
                    A landing counts as landed only when the fake node accepted the block and the \
                    tip moved to it. The block hash is the part of the share identifier after the \
                    colon, which is the same display hash the node records, so the match is exact \
                    rather than by time.",
        "landings": "the number of landings that were granted an attribution span, which is the \
                     number that have a window: resolve grants a span on the landing's own pool \
                     tip change, whatever the outcome, so this is exactly what \
                     windows_available, the per-frontend tables and the summaries are built \
                     over. It is usually the same as landing_outcomes.landed, and differs when a \
                     landing's tip moved but the node's submission record is missing \
                     (accepted_without_node_submission); the outcome tally is reported apart so \
                     the two can be read against each other. landing_attempts counts every slot \
                     the budget paid for, whatever happened to it.",
        "span": "a landing's attribution span runs from its own pool tip change to the next \
                 landing's pool tip change, and for the last landing to the end of the phase \
                 (span_truncated_at_phase_end is then true). The spans tile the phase, so every \
                 bump and every rebuild-pending rejection belongs to exactly one landing or to \
                 unattributed.",
        "tip_pending_window": "the first and last `new tip work is pending` rejection that \
                               frontend returned inside the span, the count, and last minus first \
                               in milliseconds. The instant is when the client read the rejection \
                               line, which is the only instant the harness observed directly.",
        "payout_pending_window": "the same for `new payout work is pending`. The two are reported \
                                  apart because coordinator.rs checks the tip before the payout \
                                  revision, so a landing produces the tip message first and the \
                                  payout message only if the revision moved again after the \
                                  rebuild.",
        "combined_rebuild_pending_window": "first to last of either message, with the sum of both \
                                            counts. This is the window #291 should budget against.",
        "time_to_new_tip_work": "t1 - t0 per session, where t0 is the landing's tip change on the \
                                 node and t1 is the first mining.notify whose prevhash resolves to \
                                 that tip and arrives before the end of the landing's span. Same \
                                 definition as the run's time_to_usable_work section, restricted \
                                 to one frontend's sessions. A notify for the tip after the span \
                                 is a late job for a tip the next landing has already replaced; \
                                 it is never counted, and a frontend with no such job inside the \
                                 span reports sessions_with_new_tip_work 0 with \
                                 new_tip_work_unavailable_reason rather than a borrowed time.",
        "time_to_new_revision_work": NEW_REVISION_APPROXIMATION,
        "reference_bump": "the last bump attributed to the landing: the revision a frontend has to \
                           reach before it stops answering `new payout work is pending`. \
                           time_to_new_revision_work and rejected_before_new_revision_work are \
                           both measured from it.",
        "rejected_before_new_revision_work": "rebuild-pending rejections that frontend returned \
                                              between the landing's tip change and the earliest \
                                              new-revision work on any of its sessions inside the \
                                              landing's span. Null, with a reason, when there was \
                                              no bump or no such job inside the span; a job seen \
                                              only after the span is the next landing's and stops \
                                              nothing here.",
        "lost_valid_shares": "every rebuild-pending rejection in the span. The client only submits \
                              a nonce it has already checked against the share target, so each one \
                              is a valid share the pool discarded. None is persisted, which is \
                              verified against this run's committed share identifiers rather than \
                              asserted. The per-landing tables count a span's rejections; the \
                              phase-wide lost_valid_work block counts every rebuild-pending \
                              rejection, attributed or not, and its shares_found_in_postgres is \
                              checked over all of them.",
        "bump": "an observed change of qbit_prism_cluster.payout_revision. Two bumps inside one \
                 sampling interval appear as one change with revision_delta above 1, so the delta \
                 is reported rather than assumed to be 1. The top-level bumps key is this count, \
                 whatever caused the change; bump_attribution splits it into the ones a landing's \
                 span owns and the rest, and the per-landing bumps field counts only that \
                 landing's.",
        "advisory_locks_sampled": {
            "note": "database-side queueing for this phase is reported in the phase's own entry \
                     under phases[] in this report, not here. Two PRISM advisory locks are \
                     sampled, from the same polls, and each is reported in its own block with \
                     the same shape and the same own/foreign split.",
            "phases_order_lock": measure::ORDER_LOCK.taken_by,
            "phases_settlement_lock": measure::SETTLEMENT_LOCK.taken_by,
            "why_both": "the dense-cadence scenario measures the rebuild after a landing, and the \
                         rebuild takes SETTLEMENT_LOCK first and ORDER_LOCK second. A run that \
                         sampled ORDER_LOCK alone reported part of the queueing the rebuild pays \
                         for and gave a reader no way to tell which part.",
            "not_sampled": "MIGRATION_LOCK (0x505249534d000001) and CPFP_FUNDING_LOCK \
                            (0x505249534d000006). Neither is on the share-append or rebuild path \
                            this phase measures.",
            "shared_sampler": "one poll carries both locks, so the two blocks cover exactly the \
                               same instants and share a samples count and a sampler cost."
        },
        "unknown_is_not_zero": "a measurement that could not be taken is null with a reason. A \
                                run with no landing reports landings 0 and no windows, and bumps \
                                only as many payout-revision changes as were actually observed -- \
                                all of them unattributed, since there is no span to own them. A \
                                sampler that saw nothing says so through blind and coverage \
                                rather than through a zero."
    })
}

/// `measure::summarize` returns a summary even for an empty sample; this says
/// "there was nothing to summarize" instead, so a budget proposal from no data
/// is `null` rather than a number.
fn optional_summary(
    values: &[f64],
    summarize: fn(&[f64]) -> measure::LatencySummary,
) -> Option<measure::LatencySummary> {
    if values.is_empty() {
        return None;
    }
    Some(summarize(values))
}
