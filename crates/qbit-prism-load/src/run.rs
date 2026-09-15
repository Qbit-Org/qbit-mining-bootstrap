//! Orchestration: provenance, clusters, node, seeding, frontends, phases,
//! reconciliation and outputs.

use crate::{
    artifact::{self, ArtifactInputs, PhaseEvidence, Withhold},
    cadence::{self, FrontendHealth, Landing, RevisionSampler, RevisionSeries},
    classify::{self, BlockedLog, Rejection, RejectionClass},
    cli::{phases, Args, PhasePlan},
    client::{
        self, ClientFailure, Event, FailureKind, NotifySighting, Outcome, SessionConfig,
        SessionHandle, SessionShared, SubmitRecord, TipSighting,
    },
    cluster::{self, ManagedPostgres, ObservedReplication, Replication},
    digest,
    frontend::{self, Frontend, FrontendSpec, SharedEnvironment},
    kill::KillDriver,
    measure::{self, LockSampler, ProcessSampler},
    node::FakeNode,
    profile, provenance, proxy, report,
    restart::{RestartDriver, RestartRecord},
    window,
};
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

/// Exit codes. 0 is a completed, reconciled run; every other value is a
/// distinguishable outcome, never a silent success.
pub const EXIT_OK: i32 = 0;
pub const EXIT_ERROR: i32 = 2;
pub const EXIT_BLOCKED: i32 = 3;
/// An acknowledged share that PostgreSQL does not hold: a loss.
pub const EXIT_DURABILITY: i32 = 4;
/// A share PostgreSQL holds whose acknowledgement never reached the client:
/// the server refused it (`ledger-confirmation-failed`), said it did not
/// know (`ledger-outcome-unknown`), or the socket closed before its answer
/// was read. Nothing was lost, but an acknowledgement and a commit diverged.
pub const EXIT_ACK_COMMIT_DIVERGENCE: i32 = 5;
pub const EXIT_ABORTED: i32 = 6;
/// Rejections that can only happen if the harness offered bad work.
pub const EXIT_HARNESS_BUG_REJECTIONS: i32 = 7;
/// A premise of the measurement was contradicted by what a frontend
/// advertised: the shares measured do not weigh what the configuration says.
/// The artifact is withheld, because it would name a configuration the run
/// did not measure.
pub const EXIT_PREMISE_CONTRADICTED: i32 = 8;

/// The server's `share_commit_grace`: how long past the share-commit timeout
/// it goes on waiting for a COMMIT reply before answering
/// `ledger-outcome-unknown`. It is set in
/// `crates/qbit-prism-server/src/config.rs` (`Config::from_env`), is not an
/// environment variable and is not exported, so the value is restated here
/// and a test reads that file to keep the two the same. A submit can
/// therefore be answered, legitimately, up to the commit timeout plus this
/// grace after it was sent.
pub const SERVER_SHARE_COMMIT_GRACE: Duration = Duration::from_secs(5);

/// Added to the configured share-commit timeout to bound every drain the
/// harness derives: the drained restart, the phase boundaries and the
/// teardown. It must stay strictly greater than `SERVER_SHARE_COMMIT_GRACE`:
/// the server may still legitimately answer until the timeout plus the
/// grace, and its answer then has to cross the socket and be read. It used
/// to equal the grace, so every drain gave up at exactly the moment the
/// server was still allowed to answer, with nothing left for transit or
/// scheduling (EP-ERRORS). The extra 5 s over the grace is the transit and
/// scheduling allowance the margin used to consist of entirely.
pub const DRAIN_MARGIN: Duration = Duration::from_secs(10);

/// How long a drained restart waits for a frontend's sessions to have no
/// submit outstanding. At least the commit timeout the frontends were
/// configured with (EP-ERRORS: one deadline through dependent work), so a
/// submit the server is still allowed to be working on is not declared
/// stuck.
pub fn drain_limit(share_commit_timeout_seconds: f64) -> Duration {
    Duration::from_secs_f64(share_commit_timeout_seconds.max(0.0)) + DRAIN_MARGIN
}

/// Which `artifact_kind` a run earns, from what was established at entry.
///
/// Qualification evidence needs every premise the artifact rests on: a
/// clean tree, so the revision names the code that ran; a server binary
/// shown to be what that tree builds; and a server shown to be a release
/// build, because a debug build does not measure capacity. Each override
/// that admits a run short of one of those forces `example`. The profile
/// used to be left out: `--allow-debug-server` admitted a `target/debug`
/// binary, the dep-info still established its revision, and the artifact
/// came out as `qualification` while the harness's own refusal text said
/// the build measured nothing (EP-OBSERVABILITY).
pub fn artifact_kind(
    dirty: bool,
    example_requested: bool,
    revision_established: bool,
    server_profile: frontend::BuildProfile,
) -> &'static str {
    if dirty
        || example_requested
        || !revision_established
        || server_profile != frontend::BuildProfile::Release
    {
        artifact::ARTIFACT_EXAMPLE
    } else {
        artifact::ARTIFACT_QUALIFICATION
    }
}

/// Which backends the lock sampler attributes to this run's frontends, and
/// the sentence the report says about it: every frontend's
/// `application_name` was seen in `pg_stat_activity`, so rows are
/// attributed by it; or not, so every waiter in the database is counted
/// and the sentence says why. `live_names` is what `pg_stat_activity`
/// showed, or the reason it could not be read.
///
/// An unreadable view used to arrive as an empty list, which is exactly
/// what "no frontend carried its name" looks like, so the report blamed
/// the driver for the sampler's own blindness (EP-OBSERVABILITY).
pub fn lock_attribution(
    frontend_names: &[String],
    live_names: std::result::Result<Vec<String>, String>,
) -> (Vec<String>, String) {
    let live_names = match live_names {
        Ok(names) => names,
        Err(error) => {
            return (
                Vec::new(),
                format!(
                    "every PRISM advisory-lock waiter in this database is counted: \
                     pg_stat_activity could not be read at startup ({error}), so whether the \
                     driver carried application_name for the {} frontends is unknown. A \
                     foreign holder of the same advisory lock would distort these numbers.",
                    frontend_names.len()
                ),
            );
        }
    };
    let attributed = frontend_names
        .iter()
        .filter(|name| live_names.contains(name))
        .count();
    if attributed == frontend_names.len() {
        (
            frontend_names.to_vec(),
            "application_name carried in PRISM_DATABASE_URL and seen in pg_stat_activity"
                .to_owned(),
        )
    } else {
        (
            Vec::new(),
            format!(
                "every PRISM advisory-lock waiter in this database is counted: the driver \
                 carried application_name for {attributed} of {} frontends ({live_names:?} \
                 seen). A foreign holder of the same advisory lock would distort these numbers.",
                frontend_names.len()
            ),
        )
    }
}

/// The first hard refusal among the classified log lines, verbatim.
pub fn hard_block_line(blocked: &[BlockedLog]) -> Option<String> {
    blocked
        .iter()
        .find(|log| classify::is_hard_block(log))
        .map(|log| log.line.clone())
}

/// Whether the artifact is withheld, and why, from what the run observed.
///
/// A hard refusal in a frontend log outranks the rest: the refusal is a
/// result about the size. A contradicted premise outranks an abort: it
/// says the numbers mean something else however complete they are, where an
/// abort says they are incomplete. All three stay in the side report
/// whichever one names the withholding.
pub fn withhold_decision(
    hard_block: Option<&str>,
    premise_contradicted: Option<&str>,
    aborted: Option<&str>,
) -> Option<Withhold> {
    if let Some(line) = hard_block {
        return Some(Withhold::Blocked(line.to_owned()));
    }
    if let Some(reason) = premise_contradicted {
        return Some(Withhold::PremiseContradicted(reason.to_owned()));
    }
    aborted.map(|reason| Withhold::Aborted(reason.to_owned()))
}

/// Why the share difficulty the frontends advertised contradicts the run's
/// premise, when it does. `mismatches` is every `(session, advertised,
/// configured)` the sessions reported; an empty list is agreement.
///
/// The share difficulty is a premise of the whole measurement: the window
/// arithmetic, each share's weight and the artifact's rate all assume the
/// frontends serve the configured target. A frontend that advertised a
/// lower value is served by a client that goes on mining the harder
/// configured target, whose shares are still accepted, so the artifact would
/// validate while measuring less work than the frontend configuration
/// claims. Recording the mismatch and going on used to be exactly that
/// (EP-OBSERVABILITY).
pub fn difficulty_premise_contradiction(mismatches: &[(usize, f64, f64)]) -> Option<String> {
    if mismatches.is_empty() {
        return None;
    }
    let sessions: BTreeSet<usize> = mismatches.iter().map(|(session, _, _)| *session).collect();
    let examples: Vec<String> = mismatches
        .iter()
        .take(5)
        .map(|(session, advertised, configured)| {
            format!(
                "session {session} was advertised {advertised} against the configured {configured}"
            )
        })
        .collect();
    Some(format!(
        "{} session(s) were advertised a share difficulty other than the configured one \
         ({} mismatch(es) in all): {}",
        sessions.len(),
        mismatches.len(),
        examples.join("; ")
    ))
}

/// The replication mode the run declared and the one it observed.
///
/// The declared mode is a premise of the measurement the same way the share
/// difficulty is: a run that declares an asynchronous standby and observes
/// none is not measuring what it says, and an artifact that validated
/// anyway would name conditions the run did not establish. The run used to
/// record both and do nothing when they disagreed (EP-OBSERVABILITY).
#[derive(Clone, Debug)]
pub struct ReplicationPremise {
    pub declared: Replication,
    /// What `detect_replication` saw at entry, before any frontend ran.
    pub at_entry: ObservedReplication,
    /// The same detection after the load stopped, once the run reached it.
    pub after_load: Option<ObservedReplication>,
}

impl ReplicationPremise {
    /// Why the observed replication contradicts the declared one, when it
    /// does. An observation that could not be made contradicts it too: the
    /// run cannot tell whether its standby exists, and does not guess.
    pub fn contradiction(&self) -> Option<String> {
        let checks = [
            ("at entry", Some(&self.at_entry)),
            ("after the load", self.after_load.as_ref()),
        ];
        let reasons: Vec<String> = checks
            .into_iter()
            .filter_map(|(when, observed)| match observed? {
                ObservedReplication::Observed { mode } if *mode == self.declared => None,
                ObservedReplication::Observed { mode } => Some(format!(
                    "the run declared replication {} and observed {} {when}",
                    self.declared.as_str(),
                    mode.as_str()
                )),
                ObservedReplication::Unknown { reason } => Some(format!(
                    "the run declared replication {} and could not observe the replication \
                     mode {when}, so it cannot tell whether that is what it measured: {reason}",
                    self.declared.as_str()
                )),
            })
            .collect();
        if reasons.is_empty() {
            None
        } else {
            Some(reasons.join("; "))
        }
    }

    /// Whether every observation made agrees with the declaration.
    pub fn agreed(&self) -> bool {
        self.contradiction().is_none()
    }

    /// The `replication` entry of the side report's `premise` block.
    pub fn block(&self) -> Value {
        json!({
            "declared": self.declared.as_str(),
            "observed_at_entry": self.at_entry.as_str(),
            "observed_at_entry_reason": self.at_entry.reason(),
            "observed_after_load": self.after_load.as_ref().map(ObservedReplication::as_str),
            "observed_after_load_reason": self.after_load.as_ref()
                .and_then(ObservedReplication::reason),
            "checked_after_load": self.after_load.is_some(),
            "agreed": self.agreed(),
        })
    }
}

/// The one contradiction the run reports, from the premises it checks. Each
/// check produces its own reason, and a run can fail more than one.
pub fn premise_contradiction(
    difficulty: Option<String>,
    replication: Option<String>,
) -> Option<String> {
    let reasons: Vec<String> = [difficulty, replication].into_iter().flatten().collect();
    if reasons.is_empty() {
        None
    } else {
        Some(reasons.join("; "))
    }
}

/// What the side report says beside the premise checks.
pub const PREMISE_NOTE: &str =
    "the share difficulty is a premise of the whole measurement: the window arithmetic, each \
     share's weight and the artifact's rate assume the frontends serve the configured target. \
     A frontend that advertised another value was measured at a different amount of work per \
     share, so its numbers are not evidence for the configuration the artifact would name: the \
     artifact is withheld and the run exits 8. Checked once every session holds work, before \
     any phase, and again after the load stops. The replication mode is a premise the same \
     way: the run declares one and observes one at entry and again after the load, and a \
     disagreement withholds the artifact and exits 8 too. So does an observation that could \
     not be made -- a pg_stat_replication this role cannot read -- because a run that cannot \
     tell whether its standby exists has not established the conditions it claims, and the \
     harness does not guess in either direction.";

/// The side report's `premise` block.
pub fn premise_block(
    contradiction: Option<&str>,
    mismatches: &[(usize, f64, f64)],
    replication: &ReplicationPremise,
) -> Value {
    json!({
        "share_difficulty_agreed": difficulty_premise_contradiction(mismatches).is_none(),
        "replication_agreed": replication.agreed(),
        "contradicted": contradiction.is_some(),
        "error": contradiction,
        "difficulty_mismatches": mismatches.iter()
            .map(|(session, advertised, configured)| json!({
                "session": session, "advertised": advertised, "configured": configured}))
            .collect::<Vec<_>>(),
        "replication": replication.block(),
        "note": PREMISE_NOTE,
    })
}

/// What the run's exit code is decided from, once the side report is
/// written. Kept apart from the report so the order of precedence is one
/// function that a test can pin.
#[derive(Clone, Copy, Debug)]
pub struct RunOutcome<'a> {
    pub withhold: Option<&'a Withhold>,
    pub durability_findings: usize,
    pub harness_bug_rejections: usize,
    pub divergences: usize,
    pub unknown_outcome_commits: usize,
    /// Committed shares whose submit got no response: the share is there,
    /// only its acknowledgement is missing.
    pub no_response_commits: usize,
    /// Of those, the ones whose answer was lost mid-run rather than because the
    /// measurement window closed underneath the submit. Only these mean the
    /// miner and the database disagree for a reason the run should answer for;
    /// a tail left outstanding when the drain expires is an artefact of where
    /// the window ended, and the server is still allowed to answer it.
    pub no_response_commits_mid_run: usize,
}

impl RunOutcome<'_> {
    /// The exit code, in order of precedence: a withheld artifact first
    /// (blocked, then a contradicted premise, then aborted), then the
    /// reconciliation findings, then the harness-bug rejections, then the
    /// three divergence buckets.
    pub fn exit_code(&self) -> i32 {
        match self.withhold {
            Some(Withhold::Blocked(_)) => return EXIT_BLOCKED,
            Some(Withhold::PremiseContradicted(_)) => return EXIT_PREMISE_CONTRADICTED,
            Some(Withhold::Aborted(_)) => return EXIT_ABORTED,
            None => {}
        }
        if self.durability_findings > 0 {
            return EXIT_DURABILITY;
        }
        if self.harness_bug_rejections > 0 {
            return EXIT_HARNESS_BUG_REJECTIONS;
        }
        if self.divergences > 0
            || self.unknown_outcome_commits > 0
            || self.no_response_commits_mid_run > 0
        {
            return EXIT_ACK_COMMIT_DIVERGENCE;
        }
        EXIT_OK
    }

    /// One line for stderr saying why the code is what it is, or nothing
    /// for a clean run.
    pub fn explanation(&self, report_path: &std::path::Path) -> Option<String> {
        let report = report_path.display();
        Some(match self.withhold {
            Some(Withhold::Blocked(line)) => format!("run blocked: {line}; see {report}"),
            Some(Withhold::PremiseContradicted(reason)) => {
                format!("premise contradicted, artifact withheld: {reason}; see {report}")
            }
            Some(Withhold::Aborted(reason)) => format!("run aborted: {reason}"),
            None if self.durability_findings > 0 => {
                format!("durability findings recorded; see {report}")
            }
            None if self.harness_bug_rejections > 0 => format!(
                "{} rejections classified as harness bugs; see {report}",
                self.harness_bug_rejections
            ),
            None if self.divergences > 0 => format!(
                "{} shares committed after the server refused them with \
                 ledger-confirmation-failed; see {report}",
                self.divergences
            ),
            None if self.unknown_outcome_commits > 0 => format!(
                "{} shares committed after the server answered ledger-outcome-unknown; see \
                 {report}",
                self.unknown_outcome_commits
            ),
            None if self.no_response_commits_mid_run > 0 => format!(
                "{} shares committed whose submit got no response mid-run: present in \
                 PostgreSQL, acknowledgement lost in transit, nothing lost; see {report}",
                self.no_response_commits_mid_run
            ),
            // Said even though the run exits 0, because it is the difference
            // between a clean run and a clean run with a tail the window cut off.
            None if self.no_response_commits > 0 => format!(
                "{} shares committed after the measurement window closed on their submit: \
                 present in PostgreSQL, nothing lost, and the server was still allowed to \
                 answer; see {report}",
                self.no_response_commits
            ),
            None => return None,
        })
    }
}

/// What the side report says beside a blocked run's log lines.
pub const BLOCKED_NOTE: &str =
    "Window sizes of 200k and above are refused until #273 (the PostgreSQL JSONB container \
     ceiling), and found-block candidates are refused at 400k until #265. A refusal is a \
     result, never something to work around, and it is a result whenever it is logged: at \
     startup, or later in the run once ordinary shares were already flowing.";

/// What applying a phase's delay to the proxy did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DelayChange {
    /// The proxy already held this delay; nothing had to settle.
    Unchanged,
    /// Every outstanding submit settled and the new delay is on.
    Applied,
    /// Submits were still outstanding at the limit. The delay was left as it
    /// was, so those submits finish under the delay they were offered under.
    Refused { outstanding: usize },
}

/// Wait until no session has a submit outstanding, or `limit` passes. Returns
/// what is still outstanding: 0 means everything settled.
pub async fn settle_outstanding(sessions: &[SessionHandle], limit: Duration) -> usize {
    let deadline = Instant::now() + limit;
    let outstanding = || -> usize {
        sessions
            .iter()
            .map(|session| session.outstanding.load(Ordering::Relaxed))
            .sum()
    };
    let mut left = outstanding();
    while left > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
        left = outstanding();
    }
    left
}

/// Put `delay_ms` on the proxy, but only once nothing offered under the
/// previous delay is still in flight.
///
/// The proxy reads its delay per chunk, so a submit outstanding when the
/// delay changes would finish under the new one while keeping the phase
/// stamp it was offered with; that phase's rate and p99 would then describe
/// a delay it does not report. If the outstanding submits do not settle
/// within `limit` the delay is left untouched and the caller is told how
/// many remain, so a number is never produced under a delay other than the
/// one it names (EP-STATE).
pub async fn apply_phase_delay(
    proxy: &proxy::DelayProxy,
    sessions: &[SessionHandle],
    delay_ms: u64,
    limit: Duration,
) -> DelayChange {
    if proxy.delay_millis() == delay_ms {
        return DelayChange::Unchanged;
    }
    let outstanding = settle_outstanding(sessions, limit).await;
    if outstanding > 0 {
        return DelayChange::Refused { outstanding };
    }
    proxy.set_delay_millis(delay_ms);
    DelayChange::Applied
}

/// Everything the sessions reported, folded by the collector task.
#[derive(Default)]
pub struct Collected {
    pub submits: Vec<SubmitRecord>,
    pub reconnects: Vec<client::ReconnectRecord>,
    pub tips: Vec<TipSighting>,
    /// Only recorded while a phase asks for them; see
    /// `SessionShared::record_notifies`.
    pub notifies: Vec<NotifySighting>,
    pub discarded_block_solutions: u64,
    pub discarded_offers: u64,
    /// The discarded offers by the phase whose scheduler placed them.
    pub discarded_offers_by_phase: BTreeMap<String, u64>,
    pub difficulty_mismatches: Vec<(usize, f64, f64)>,
    pub failures: Vec<ClientFailure>,
    /// Every successful connection, including reconnects: a count of events,
    /// not of sessions.
    pub connects: u64,
    pub disconnects: Vec<(usize, usize, String)>,
    /// The sessions currently holding work: added on `Connected`, removed on
    /// `Disconnected`. A session that connected, dropped and reconnected is
    /// in here once, so this is what the startup gate reads (EP-STATE).
    pub holding_work: BTreeSet<usize>,
}

impl Collected {
    pub fn apply(&mut self, event: Event) {
        match event {
            Event::CensusBarrier(ack) => {
                let _ = ack.send(());
            }
            Event::Submit(record) => self.submits.push(*record),
            Event::Reconnect(record) => self.reconnects.push(record),
            Event::Tip(sighting) => self.tips.push(sighting),
            Event::Notify(sighting) => self.notifies.push(sighting),
            Event::DiscardedBlockSolution { .. } => self.discarded_block_solutions += 1,
            Event::DiscardedOffer { phase, .. } => {
                self.discarded_offers += 1;
                *self.discarded_offers_by_phase.entry(phase).or_insert(0) += 1;
            }
            Event::DifficultyMismatch {
                session,
                advertised,
                configured,
            } => self
                .difficulty_mismatches
                .push((session, advertised, configured)),
            Event::Connected { session, .. } => {
                self.connects += 1;
                self.holding_work.insert(session);
            }
            Event::Disconnected {
                session,
                frontend,
                reason,
            } => {
                self.holding_work.remove(&session);
                self.disconnects.push((session, frontend, reason));
            }
            Event::Failure(failure) => self.failures.push(failure),
        }
    }

    /// How many distinct sessions hold work right now.
    pub fn sessions_holding_work(&self) -> usize {
        self.holding_work.len()
    }
}

struct PhaseRun {
    plan: PhasePlan,
    /// False for the phase an abort cut short: its numbers cover only the
    /// part that ran.
    completed: bool,
    started_wall: chrono::DateTime<chrono::Utc>,
    ended_wall: chrono::DateTime<chrono::Utc>,
    duration_millis: u64,
    tokens: u64,
    dispatched: u64,
    shortfall: u64,
    locks: measure::PhaseLocks,
    processes: Vec<measure::ProcessSummary>,
    ack_deltas: Vec<measure::ServerAckDelta>,
    replication_start: cluster::ReplicationObservation,
    replication_end: cluster::ReplicationObservation,
    proxy_delay_configured_ms: u64,
    /// How long the boundary waited for the previous phase's submits to
    /// settle before this phase's delay was applied; `None` when the delay
    /// did not change and nothing had to settle.
    delay_settled_seconds: Option<f64>,
    /// Median `SELECT 1` round trip through the frontends' URL, timed just
    /// before the phase was driven with its delay already applied; the
    /// reason when it could not be timed. Unknown is not zero.
    proxy_delay_observed_ms: std::result::Result<f64, String>,
    min_mem_available_kib: Option<u64>,
    scheduled_blocks: usize,
    frontend_restarts: usize,
    /// Every drained restart this phase completed, with its timings and the
    /// scrapes bracketing the counter reset.
    restart_records: Vec<RestartRecord>,
    mid_flight_indeterminate: Vec<SubmitRecord>,
    /// Submits outstanding on the killed frontend at the instant of the kill.
    outstanding_at_kill: Option<usize>,
    /// Monotonic bounds of the phase, for the dense-cadence section's offsets.
    started: Instant,
    ended: Instant,
    /// Set only for the `dense_cadence` phase.
    dense: Option<DensePhase>,
}

/// What the dense-cadence phase collected beyond the usual per-phase numbers.
struct DensePhase {
    gaps: Vec<f64>,
    offsets: Vec<f64>,
    landings: Vec<Landing>,
    slots_over_budget: usize,
    revisions: RevisionSeries,
    /// Each session's frontend, snapshotted at the end of the phase.
    session_frontend: Vec<usize>,
    frontend_health: Vec<FrontendHealth>,
}

pub async fn execute(args: Args) -> Result<i32> {
    args.validate()?;
    // The managed cluster's binaries are resolved and checked before anything
    // is created, so a wrong `--pg-bin-dir` names the flag and the missing
    // binary rather than surfacing later as a failed `initdb`. An external
    // database needs none of them, and is not asked for them.
    let pg_bin_dir = match &args.database_url {
        Some(_) => None,
        None => {
            let dir = cluster::resolve_bin_dir(args.pg_bin_dir.as_deref())?;
            cluster::verify_bin_dir(&dir)?;
            Some(dir)
        }
    };
    let started_wall = chrono::Utc::now();
    let run_id = uuid::Uuid::new_v4();
    let run_tag = run_id.simple().to_string()[..8].to_owned();
    let address_prefix = "pload1".to_owned();
    let payout_address = format!("{address_prefix}{run_tag}");
    let share_prefix = format!("{payout_address}.");

    // Whatever this invocation ends as -- blocked, aborted, failed before it
    // measured, or complete -- nothing an earlier run wrote may outlive it in
    // the directory, so the earlier outputs go now rather than on each exit
    // path separately.
    let stale_outputs_removed = report::claim_out_dir(&args.out)?;
    let removed_at_entry = stale_outputs_removed.clone();
    let log_dir = args.out.join("logs");
    std::fs::create_dir_all(&log_dir)?;

    // --- provenance -------------------------------------------------------
    let revision = git(&["rev-parse", "HEAD"])?.trim().to_owned();
    ensure!(
        revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit()),
        "git rev-parse HEAD did not return a commit: {revision:?}"
    );
    let dirty = !git(&["status", "--porcelain", "--untracked-files=no"])?
        .trim()
        .is_empty();
    if dirty && !args.allow_dirty_tree {
        bail!(
            "tracked files are modified, so the artifact could not name the code that ran; \
             commit them or pass --allow-dirty-tree"
        );
    }
    let server_bin = resolve_server_bin(args.server_bin.clone())?;
    // The revision above is the checkout's; the binary has to be shown to be
    // what that checkout builds, or the artifact would name a commit that did
    // not produce its measurements (EP-OBSERVABILITY).
    let repo_root = PathBuf::from(git(&["rev-parse", "--show-toplevel"])?.trim());
    let revision_evidence = provenance::server_revision_evidence(&server_bin, &repo_root);
    if let provenance::RevisionEvidence::Unestablished { reason } = &revision_evidence {
        ensure!(
            args.allow_unverified_server_revision,
            "{} cannot be tied to {revision}: {reason}. Rebuild it from this checkout \
             (cargo build --locked --release -p qbit-prism-server) or pass \
             --allow-unverified-server-revision, which forces artifact_kind example",
            server_bin.display()
        );
    }
    let server_profile = frontend::build_profile(&server_bin);
    let harness_profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    frontend::check_server_profile(server_profile, args.allow_debug_server, &server_bin)?;
    let server_bytes =
        std::fs::read(&server_bin).with_context(|| format!("reading {}", server_bin.display()))?;
    let server_digest = format!("sha256:{}", hex::encode(Sha256::digest(&server_bytes)));
    let artifact_kind = artifact_kind(
        dirty,
        args.example_artifact,
        revision_evidence.is_established(),
        server_profile,
    )
    .to_owned();

    // --- file descriptors -------------------------------------------------
    let needed = (args.sessions as u64) * 4 + 1024;
    let (fd_before, fd_after) = measure::raise_file_descriptor_limit(needed)?;

    // --- fake node --------------------------------------------------------
    let node = FakeNode::open(window::TEMPLATE_BITS, &address_prefix).await?;
    let node_state = node.state.clone();

    // --- PostgreSQL -------------------------------------------------------
    let replication = Replication::parse(&args.replication)?;
    let max_connections = args.frontends as u32 * args.db_max_connections + 32;
    let mut managed: Option<ManagedPostgres> = None;
    let direct_url = match &args.database_url {
        Some(url) => url.clone(),
        None => {
            let bin_dir = pg_bin_dir.context("the PostgreSQL bin directory was not resolved")?;
            let cluster =
                ManagedPostgres::start(bin_dir, replication, max_connections, args.keep_artifacts)
                    .await?;
            let url = cluster.primary_url.clone();
            managed = Some(cluster);
            url
        }
    };
    let result = run_inner(
        &args,
        RunContext {
            run_id,
            run_tag: run_tag.clone(),
            started_wall,
            payout_address,
            share_prefix,
            revision,
            revision_evidence,
            dirty,
            server_bin,
            server_digest,
            server_profile,
            harness_profile,
            artifact_kind,
            fd_before,
            fd_after,
            node_url: node.url.clone(),
            node_state,
            direct_url: direct_url.clone(),
            log_dir,
            stale_outputs_removed,
            declared_replication: replication,
            managed_standby: managed.as_ref().and_then(|m| m.standby_url.clone()),
            pg_stat_statements: managed
                .as_ref()
                .and_then(|m| m.pg_stat_statements.clone())
                .unwrap_or_else(|| "unknown (external database)".into()),
        },
    )
    .await;
    // Cleanup runs on every exit path.
    if let Some(mut cluster) = managed {
        cluster.stop();
    }
    drop(node);
    // An error out of the run is the one exit that wrote no report of its
    // own. The directory was taken at entry, so it would otherwise be left
    // empty, as if nothing had run: the reason goes into it instead. The
    // exit code is unchanged, and so is the error the caller sees.
    if let Err(error) = &result {
        match report::write_failure(&args.out, run_id, &format!("{error:#}"), &removed_at_entry) {
            Ok(Some(path)) => eprintln!("run failed; the reason is recorded in {}", path.display()),
            Ok(None) => {}
            Err(write_error) => eprintln!(
                "run failed, and the failure could not be recorded in {}: {write_error:#}",
                args.out.display()
            ),
        }
    }
    result
}

struct RunContext {
    run_id: uuid::Uuid,
    run_tag: String,
    started_wall: chrono::DateTime<chrono::Utc>,
    payout_address: String,
    share_prefix: String,
    revision: String,
    /// Whether the server binary was shown to be what `revision` builds.
    revision_evidence: provenance::RevisionEvidence,
    dirty: bool,
    server_bin: PathBuf,
    server_digest: String,
    server_profile: frontend::BuildProfile,
    harness_profile: &'static str,
    artifact_kind: String,
    fd_before: u64,
    fd_after: u64,
    node_url: String,
    node_state: Arc<crate::node::NodeState>,
    direct_url: String,
    log_dir: PathBuf,
    /// The earlier run's outputs removed from `--out` at entry, by name.
    stale_outputs_removed: Vec<String>,
    declared_replication: Replication,
    managed_standby: Option<String>,
    pg_stat_statements: String,
}

async fn run_inner(args: &Args, ctx: RunContext) -> Result<i32> {
    // --- schema, durability, seeding -------------------------------------
    let seed_ledger =
        qbit_prism_server::ledger::Ledger::connect(&ctx.direct_url, "load-seed".into(), 4, true)
            .await
            .context("initialising the schema")?;
    let (fsync, full_page_writes, synchronous_commit) =
        cluster::durability(&seed_ledger.pool).await?;
    let durability: BTreeMap<String, String> = [
        ("fsync", fsync.clone()),
        ("full_page_writes", full_page_writes.clone()),
        ("synchronous_commit", synchronous_commit.clone()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect();
    if durability.values().any(|value| value != "on") {
        seed_ledger.pool.close().await;
        bail!(
            "PostgreSQL durability is not on (fsync={fsync} full_page_writes={full_page_writes} \
             synchronous_commit={synchronous_commit}); refusing to run the load"
        );
    }
    let postgres_version = cluster::server_version(&seed_ledger.pool).await?;

    let bits = qbit_prism_server::codec::parse_u32_hex(window::TEMPLATE_BITS)?;
    let solution = window::solve_window(bits, args.window_shares)?;
    let seed = window::SeedPlan::new(
        args.window_shares,
        solution.scaled_share_difficulty,
        solution.scaled_network_difficulty,
        args.seed_share_bytes,
    )?;
    let seed_stats = seed.load(&seed_ledger.pool, "load-seed").await?;
    let window_at_start =
        window::observed_window_length(&seed_ledger.pool, solution.scaled_network_difficulty)
            .await?;
    seed_ledger.pool.close().await;

    // A side pool, outside the frontends' path and outside the delay proxy.
    let side = PgPoolOptions::new()
        .max_connections(6)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&ctx.direct_url)
        .await
        .context("opening the harness side pool")?;
    if ctx.pg_stat_statements.starts_with("loaded") {
        let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .execute(&side)
            .await;
    }
    // The declared replication mode is a premise, checked here before a
    // frontend is launched: a run that declares a standby and observes
    // none, or cannot observe at all, is refused now rather than after the
    // load, and checked again once the load stops.
    let entry_replication = ReplicationPremise {
        declared: ctx.declared_replication,
        at_entry: cluster::detect_replication(&side).await,
        after_load: None,
    };
    if let Some(reason) = entry_replication.contradiction() {
        return finish_early(
            args,
            &ctx,
            Vec::new(),
            Vec::new(),
            &entry_replication,
            EarlyExit::PremiseContradicted {
                reason,
                difficulty_mismatches: Vec::new(),
            },
        )
        .await;
    }

    // --- delay proxy ------------------------------------------------------
    // The upstream is `host:port` as the URL wrote it; a hostname is resolved
    // by the proxy, the same way the SQLx connections above accepted it.
    let delay_proxy = proxy::DelayProxy::open(&host_port(&ctx.direct_url)?).await?;
    let proxied_url = rewrite_host(&ctx.direct_url, &delay_proxy.url_host())?;
    let direct_rtt = proxy::measure_select1_millis(&ctx.direct_url, 21)
        .await
        .ok();
    let proxied_rtt_idle = proxy::measure_select1_millis(&proxied_url, 21).await.ok();
    // The proxied URL is what the frontends are given, so the proxy is shown
    // to be on it rather than assumed to be: with the slow phase's delay set,
    // a round trip through the URL exactly as written has to cost at least
    // twice the delay. A URL the rewrite had left an endpoint in comes back
    // in microseconds here, and would otherwise carry the run to a
    // `slow_database` phase reporting a delay nothing applied.
    delay_proxy.set_delay_millis(args.slow_db_delay_ms);
    let proxied_rtt_delayed = proxy::measure_select1_millis(&proxied_url, 21)
        .await
        .context("timing a round trip through the delay proxy")?;
    delay_proxy.set_delay_millis(0);
    check_delay_observed(args.slow_db_delay_ms, proxied_rtt_delayed)?;

    // --- frontends --------------------------------------------------------
    let per_frontend = args.sessions.div_ceil(args.frontends);
    let shared_env = SharedEnvironment {
        rpc_url: ctx.node_url.clone(),
        rpc_user: "qbit".into(),
        rpc_password: format!("load-{}", ctx.run_tag),
        share_difficulty: format!("{}", solution.share_difficulty),
        max_difficulty: "1024".into(),
        database_max_connections: args.db_max_connections,
        runtime_workers: args.runtime_workers,
        stratum_max_connections: (per_frontend * 2 + 64).max(384),
        stratum_max_pending_initial_jobs: (per_frontend + 16).max(128),
        share_commit_timeout_seconds: format!("{}", args.share_commit_timeout_seconds),
        blockpoll_seconds: format!("{}", args.blockpoll_seconds),
        rust_log: "info".into(),
    };
    let mut frontends: Vec<Frontend> = Vec::new();
    let mut blocked: Vec<BlockedLog> = Vec::new();
    for index in 0..args.frontends {
        let instance_id = format!("load-fe-{index}");
        let spec = FrontendSpec {
            index,
            database_url: with_application_name(&proxied_url, &instance_id),
            instance_id,
            stratum_port: free_port()?,
            audit_port: free_port()?,
        };
        let environment = frontend::frontend_environment(&shared_env, &spec);
        let mut child = Frontend::launch(ctx.server_bin.clone(), spec, environment, &ctx.log_dir)?;
        // Frontend 1 first: concurrent first-boot migrations wait inside a
        // transaction bounded by the 5 s lock_timeout.
        let ready = child
            .wait_ready(Duration::from_secs(args.work_timeout))
            .await;
        if let Err(error) = ready {
            blocked.extend(scan_logs(&child.read_stderr()));
            frontends.push(child);
            return finish_early(
                args,
                &ctx,
                frontends,
                blocked,
                &entry_replication,
                EarlyExit::Blocked(error.to_string()),
            )
            .await;
        }
        frontends.push(child);
    }
    for child in &frontends {
        blocked.extend(scan_logs(&child.read_stderr()));
    }
    if let Some(text) = hard_block_line(&blocked) {
        return finish_early(
            args,
            &ctx,
            frontends,
            blocked,
            &entry_replication,
            EarlyExit::Blocked(text),
        )
        .await;
    }

    // --- samplers ---------------------------------------------------------
    let process_samplers: Vec<ProcessSampler> = frontends
        .iter()
        .map(|child| {
            ProcessSampler::start(
                child.spec.instance_id.clone(),
                child.pid(),
                Duration::from_millis(args.process_sample_interval_ms),
            )
        })
        .collect();
    // ORDER_LOCK is database-wide, so the sampler has to know which backends
    // are this run's. `application_name` only works if the driver carries it,
    // which is checked here rather than assumed.
    let frontend_names: Vec<String> = frontends
        .iter()
        .map(|child| child.spec.instance_id.clone())
        .collect();
    let live_names = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT COALESCE(application_name,'') FROM pg_stat_activity \
         WHERE datname = current_database()",
    )
    .fetch_all(&side)
    .await
    .map_err(|error| format!("{error:#}"));
    let (sampler_names, attribution) = lock_attribution(&frontend_names, live_names);
    let lock_sampler = LockSampler::start(
        side.clone(),
        Duration::from_millis(args.lock_sample_interval_ms),
        sampler_names,
        attribution,
    );

    // --- sessions ---------------------------------------------------------
    let (events_tx, mut events_rx) = tokio::sync::mpsc::unbounded_channel();
    let collected = Arc::new(Mutex::new(Collected::default()));
    let collector = {
        let collected = collected.clone();
        tokio::spawn(async move {
            while let Some(event) = events_rx.recv().await {
                collected.lock().expect("collector lock").apply(event);
            }
        })
    };
    let shared_session = Arc::new(SessionShared {
        phase: std::sync::RwLock::new("setup".to_owned()),
        events: events_tx,
        record_notifies: std::sync::atomic::AtomicBool::new(false),
    });
    let mut sessions: Vec<SessionHandle> = Vec::with_capacity(args.sessions);
    // One deadline for everything that waits on the server to answer a
    // submit: the phase boundaries, the drained restart, and each session's
    // own quiesce before a deliberate close.
    let quiesce_limit = drain_limit(args.share_commit_timeout_seconds);
    for index in 0..args.sessions {
        let frontend_index = index % args.frontends;
        let config = SessionConfig {
            index,
            username: format!("{}.s{index:05}", ctx.payout_address),
            password: "x".into(),
            share_difficulty: solution.share_difficulty,
            version_rolling_mask: qbit_prism_server::codec::VERSION_ROLLING_MASK,
            connect_timeout: Duration::from_secs(20),
            handshake_timeout: Duration::from_secs(args.work_timeout.min(120)),
            quiesce_limit,
        };
        sessions.push(client::spawn_session(
            config,
            frontend_index,
            frontends[frontend_index].stratum_address(),
            shared_session.clone(),
            args.max_outstanding_per_session,
        ));
    }
    // Every session must hold work before the first phase starts. This is
    // read per session, not as a count of connection events: a session that
    // dropped and reconnected while another was still in its handshake would
    // otherwise satisfy the total on the other's behalf.
    let work_deadline = Instant::now() + Duration::from_secs(args.work_timeout);
    loop {
        let connected = collected
            .lock()
            .expect("collector lock")
            .sessions_holding_work();
        if connected >= args.sessions {
            break;
        }
        if Instant::now() >= work_deadline {
            for child in &frontends {
                blocked.extend(scan_logs(&child.read_stderr()));
            }
            for session in &sessions {
                let _ = session.control.send(client::Control::Stop);
            }
            let text = blocked
                .first()
                .map(|log| log.line.clone())
                .unwrap_or_else(|| {
                    format!(
                        "only {connected} of {} sessions received work",
                        args.sessions
                    )
                });
            lock_sampler.stop();
            for sampler in &process_samplers {
                sampler.stop();
            }
            return finish_early(
                args,
                &ctx,
                frontends,
                blocked,
                &entry_replication,
                EarlyExit::Blocked(text),
            )
            .await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Every session has its first job, and with it the difficulty its
    // frontend advertised. A frontend that advertised something other than
    // the configured value has already contradicted the premise every later
    // number would rest on, so the run is refused here rather than ten
    // minutes later; the same check runs again after the load, for a value
    // that changes mid-run.
    let early_mismatches = collected
        .lock()
        .expect("collector lock")
        .difficulty_mismatches
        .clone();
    if let Some(reason) = difficulty_premise_contradiction(&early_mismatches) {
        for session in &sessions {
            let _ = session.control.send(client::Control::Stop);
        }
        lock_sampler.stop();
        for sampler in &process_samplers {
            sampler.stop();
        }
        for child in &frontends {
            blocked.extend(scan_logs(&child.read_stderr()));
        }
        return finish_early(
            args,
            &ctx,
            frontends,
            blocked,
            &entry_replication,
            EarlyExit::PremiseContradicted {
                reason,
                difficulty_mismatches: early_mismatches,
            },
        )
        .await;
    }

    // --- phases -----------------------------------------------------------
    let plans = phases(args)?;
    let mut runs: Vec<PhaseRun> = Vec::new();
    let mut aborted: Option<String> = None;
    let mut external_tips: Vec<crate::node::TipChange> = Vec::new();
    let mut remaining_blocks = args.scheduled_blocks;
    let mut remaining_tips = args.external_tips;
    let settle_limit = drain_limit(args.share_commit_timeout_seconds);
    for plan in &plans {
        // The proxy's delay is changed only once nothing offered under the
        // previous delay is still in flight. The proxy reads its delay per
        // chunk, so a submit outstanding across the boundary would otherwise
        // finish under the next phase's delay while keeping its own phase's
        // stamp, and that phase's rate and p99 would stop describing the
        // delay it reports (EP-STATE). Nothing is offered while it settles:
        // the previous phase's scheduler has returned. A drain that does not
        // complete within the commit timeout and its margin aborts the run,
        // as the reconnect phase's restart does, rather than leaking.
        let settling = Instant::now();
        let delay_settled_seconds = match apply_phase_delay(
            &delay_proxy,
            &sessions,
            plan.database_delay_ms,
            settle_limit,
        )
        .await
        {
            DelayChange::Unchanged => None,
            DelayChange::Applied => Some(settling.elapsed().as_secs_f64()),
            DelayChange::Refused { outstanding } => {
                aborted = Some(format!(
                    "{outstanding} submit(s) offered before the {} phase were still outstanding \
                     {:.1} s later, so its {} ms delay could not be applied without them \
                     finishing under it",
                    plan.name,
                    settle_limit.as_secs_f64(),
                    plan.database_delay_ms
                ));
                break;
            }
        };
        *shared_session.phase.write().expect("phase lock") = plan.name.clone();
        // The delay this phase will report is observed through the frontends'
        // URL before the phase is driven. A delayed phase whose trip did not
        // pay it aborts the run: its numbers would describe a delay that was
        // not there, and the artifact is withheld (EP-OBSERVABILITY).
        let proxy_delay_observed_ms = proxy::measure_select1_millis(&proxied_url, 21)
            .await
            .map_err(|error| format!("{error:#}"));
        if plan.database_delay_ms > 0 {
            let checked = proxy_delay_observed_ms
                .as_ref()
                .map_err(|error| anyhow::anyhow!("{error}"))
                .and_then(|median| check_delay_observed(plan.database_delay_ms, *median));
            if let Err(error) = checked {
                aborted = Some(format!(
                    "the {} phase's delay could not be shown to be applied: {error:#}",
                    plan.name
                ));
                break;
            }
        }
        let replication_start = cluster::observe_replication(&side, &plan.name).await?;
        measure::reset_statement_stats(&side).await;
        let mut before_scrapes = Vec::new();
        for child in &frontends {
            before_scrapes
                .push(measure::scrape_metrics(&child.spec.instance_id, &child.metrics_url()).await);
        }
        let restarts_before: Vec<usize> = frontends.iter().map(|child| child.restarts).collect();
        let revision_sampler = plan.dense_cadence.then(|| {
            RevisionSampler::start(
                side.clone(),
                Duration::from_millis(cadence::REVISION_SAMPLE_INTERVAL_MS),
            )
        });
        shared_session
            .record_notifies
            .store(plan.dense_cadence, std::sync::atomic::Ordering::Relaxed);
        let started = Instant::now();
        let started_wall = chrono::Utc::now();
        let outcome = drive_phase(
            args,
            plan,
            &sessions,
            &mut frontends,
            &process_samplers,
            &ctx.node_state,
            &mut external_tips,
            &mut remaining_blocks,
            &mut remaining_tips,
            &collected,
        )
        .await?;
        // The phase's bounds are when it started and stopped scheduling; a
        // restart the phase's deadline cut across was completed inside
        // `drive_phase` after that, as boundary time.
        let ended = outcome.ended;
        let ended_wall = outcome.ended_wall;
        shared_session
            .record_notifies
            .store(false, std::sync::atomic::Ordering::Relaxed);
        let dense = match revision_sampler {
            Some(sampler) => {
                let revisions = sampler.finish().await;
                let aborted_text = outcome.aborted.clone().unwrap_or_default();
                Some(DensePhase {
                    gaps: args.cadence_gaps()?,
                    offsets: outcome.dense_offsets.clone(),
                    landings: outcome.dense_landings.clone(),
                    slots_over_budget: outcome.slots_over_budget,
                    revisions,
                    session_frontend: sessions
                        .iter()
                        .map(|session| session.frontend.load(Ordering::Relaxed))
                        .collect(),
                    frontend_health: frontends
                        .iter()
                        .zip(restarts_before.iter())
                        .map(|(child, before)| FrontendHealth {
                            index: child.spec.index,
                            instance_id: child.spec.instance_id.clone(),
                            restarts_before: *before,
                            restarts_after: child.restarts,
                            exited: aborted_text
                                .contains(&child.spec.instance_id)
                                .then(|| aborted_text.clone()),
                        })
                        .collect(),
                })
            }
            None => None,
        };
        let mut after_scrapes = Vec::new();
        for child in &frontends {
            after_scrapes
                .push(measure::scrape_metrics(&child.spec.instance_id, &child.metrics_url()).await);
        }
        let replication_end = cluster::observe_replication(&side, &plan.name).await?;
        let mut locks = lock_sampler.summarize(started, ended);
        // All three PRISM locks share one normalized query text, so the same
        // aggregate belongs to both blocks and its own note says so.
        let statement = measure::advisory_lock_statement(&side).await;
        locks.order.advisory_lock_statement = statement.clone();
        locks.settlement.advisory_lock_statement = statement;
        let processes = process_samplers
            .iter()
            .map(|sampler| {
                sampler.summarize(sampler.elapsed_of(started), sampler.elapsed_of(ended))
            })
            .collect();
        runs.push(PhaseRun {
            plan: plan.clone(),
            completed: outcome.aborted.is_none(),
            started_wall,
            ended_wall,
            duration_millis: ended.saturating_duration_since(started).as_millis() as u64,
            tokens: outcome.tokens,
            dispatched: outcome.dispatched,
            shortfall: outcome.shortfall,
            locks,
            processes,
            // A frontend restarted during the phase reset its counters; the
            // delta is assembled from the scrapes on each side of every
            // restart, and a restart no scrape bracketed (a mid-flight kill)
            // leaves it unknown rather than negative or zero.
            ack_deltas: frontends
                .iter()
                .enumerate()
                .zip(before_scrapes.iter().zip(after_scrapes.iter()))
                .map(|((index, child), (before, after))| {
                    let splits: Vec<&measure::AckSplit> = outcome
                        .restart_records
                        .iter()
                        .filter(|record| record.index == index)
                        .map(|record| &record.split)
                        .collect();
                    let restarts = child.restarts.saturating_sub(restarts_before[index]);
                    measure::ack_delta_across_restarts(before, &splits, after, restarts)
                })
                .collect(),
            replication_start,
            replication_end,
            proxy_delay_configured_ms: plan.database_delay_ms,
            delay_settled_seconds,
            proxy_delay_observed_ms,
            min_mem_available_kib: outcome.min_mem_available_kib,
            scheduled_blocks: outcome.scheduled_blocks,
            frontend_restarts: outcome.frontend_restarts,
            restart_records: outcome.restart_records,
            mid_flight_indeterminate: outcome.indeterminate,
            outstanding_at_kill: outcome.outstanding_at_kill,
            started,
            ended,
            dense,
        });
        if let Some(reason) = outcome.aborted {
            aborted = Some(reason);
            break;
        }
    }
    *shared_session.phase.write().expect("phase lock") = "teardown".to_owned();

    // --- stop the load ----------------------------------------------------
    // Quiesce first: a socket closed with a submit outstanding manufactures an
    // indeterminate share that no phase asked for, and the run would then
    // report a durability finding it created itself. The last phase's delay
    // stays on until its submits have settled, for the same reason the
    // phase boundaries wait: the answers still in flight are that phase's.
    // The wait is the same limit the boundaries and the drained restart use,
    // the configured share-commit timeout plus its margin: it was a fixed
    // 90 s, which a commit timeout above 85 s outran, and a submit the server
    // was still allowed to be working on then became a "run ended"
    // no-response in the last artifact phase (EP-ERRORS).
    for session in &sessions {
        session.paused.store(true, Ordering::Relaxed);
        let _ = session.control.send(client::Control::Pause);
    }
    let draining = Instant::now();
    let undrained = settle_outstanding(&sessions, settle_limit).await;
    let drained_seconds = draining.elapsed().as_secs_f64();
    delay_proxy.set_delay_millis(0);
    for session in &sessions {
        let _ = session.control.send(client::Control::Stop);
    }
    for session in sessions.drain(..) {
        let _ = tokio::time::timeout(Duration::from_secs(15), session.task).await;
    }
    lock_sampler.stop();
    for sampler in &process_samplers {
        sampler.stop();
    }
    drop(shared_session);
    let _ = tokio::time::timeout(Duration::from_secs(10), collector).await;
    let collected = std::mem::take(&mut *collected.lock().expect("collector lock"));

    // The startup check ran before any phase. A refusal logged after it -- a
    // scheduled-block rebuild hitting the JSONB ceiling, say -- is the same
    // hard block seen late, and ordinary shares can keep flowing past a
    // candidate refusal, so nothing downstream would necessarily catch it.
    // It is re-checked here: the artifact is withheld and the run exits
    // blocked, as it would have at startup, with the whole side report
    // (EP-OBSERVABILITY).
    //
    // The frontends are stopped before the check, not after the outputs are
    // written. Nothing past this point needs a live frontend -- the sessions
    // have stopped, the samplers have stopped, the metrics were scraped at
    // each phase's end, and the evidence, profile and report are built from
    // the frontends' specs, environments and log paths -- while a rebuild
    // still running would keep writing to a log the check had already read
    // (`stop_and_scan_logs`).
    blocked.extend(stop_and_scan_logs(&mut frontends));
    let late_hard_block = hard_block_line(&blocked);
    // The premise checks again, over the whole run: a set_difficulty that
    // arrived after the startup check is the same contradiction seen late,
    // and so is a standby that vanished, appeared or became unreadable
    // while the load ran.
    let replication_premise = ReplicationPremise {
        after_load: Some(cluster::detect_replication(&side).await),
        ..entry_replication
    };
    let premise_contradiction = premise_contradiction(
        difficulty_premise_contradiction(&collected.difficulty_mismatches),
        replication_premise.contradiction(),
    );
    let mut withhold = withhold_decision(
        late_hard_block.as_deref(),
        premise_contradiction.as_deref(),
        aborted.as_deref(),
    );
    let window_at_end =
        window::observed_window_length(&side, solution.scaled_network_difficulty).await?;

    // --- reconciliation ---------------------------------------------------
    let writer_ids: Vec<String> = frontends
        .iter()
        .map(|child| child.spec.instance_id.clone())
        .collect();
    let committed: BTreeSet<String> = sqlx::query_scalar::<_, String>(
        "SELECT share_id FROM qbit_share_ledger \
         WHERE accepted AND writer_id = ANY($1) AND share_id LIKE $2",
    )
    .bind(&writer_ids)
    .bind(format!("{}%", ctx.share_prefix.replace('_', "\\_")))
    .fetch_all(&side)
    .await
    .context("reading this run's committed shares")?
    .into_iter()
    .collect();

    let mut phase_reconciliations: Vec<(String, digest::Reconciliation)> = Vec::new();
    for phase in &runs {
        let (offered, acknowledged) =
            offered_and_acknowledged(&collected.submits, &phase.plan.name);
        phase_reconciliations.push((
            phase.plan.name.clone(),
            digest::reconcile(offered, acknowledged, &committed),
        ));
    }
    let attribution = digest::attribute_unexpected(
        &committed,
        &phase_reconciliations
            .iter()
            .map(|(name, rec)| (name.clone(), rec))
            .collect::<Vec<_>>(),
    );
    let mut unexpected_by_phase: HashMap<String, usize> = HashMap::new();
    for (name, rows) in &attribution.by_phase {
        unexpected_by_phase.insert(name.clone(), rows.len());
    }

    // --- artifact ---------------------------------------------------------
    let configuration = frontend::configuration_block(&frontends[0].environment)?;
    let host = measure::host_facts();
    let replication_rows = json!({
        "declared": ctx.declared_replication.as_str(),
        "observed": replication_premise.at_entry.as_str(),
        "observed_reason": replication_premise.at_entry.reason(),
        "observed_after_load": replication_premise.after_load.as_ref()
            .map(ObservedReplication::as_str),
        "observed_after_load_reason": replication_premise.after_load.as_ref()
            .and_then(ObservedReplication::reason),
        "agreed_with_declared": replication_premise.agreed(),
        "standby_name": cluster::STANDBY_NAME,
        "slot": cluster::STANDBY_SLOT,
        "standby_url_present": ctx.managed_standby.is_some(),
        "observations": runs.iter().flat_map(|phase| {
            vec![
                json!({"phase": phase.plan.name, "at": "start", "observation": phase.replication_start}),
                json!({"phase": phase.plan.name, "at": "end", "observation": phase.replication_end}),
            ]
        }).collect::<Vec<_>>(),
    });
    let proxy_block = json!({
        "kind": "in-harness tokio TCP proxy",
        "delay_semantics": proxy::DELAY_SEMANTICS,
        "upstream": delay_proxy.upstream,
        "upstream_resolved": delay_proxy.upstream_resolved.iter()
            .map(ToString::to_string).collect::<Vec<_>>(),
        "configured_slow_database_delay_milliseconds": args.slow_db_delay_ms,
        "direct_select1_median_milliseconds": direct_rtt,
        "proxied_select1_median_milliseconds_at_zero_delay": proxied_rtt_idle,
        "proxied_select1_median_milliseconds_at_slow_database_delay": proxied_rtt_delayed,
        "delayed_round_trip_floor_milliseconds": delay_floor_millis(args.slow_db_delay_ms),
        "delay_verification": "before the run and again before every phase, a SELECT 1 round \
                               trip is timed through the proxied URL exactly as the frontends \
                               received it; a trip under a delay that costs less than twice the \
                               delay means the connections bypass the proxy, and the run refuses \
                               to report that phase",
        "measured_proxy_overhead_milliseconds": match (direct_rtt, proxied_rtt_idle) {
            (Some(direct), Some(proxied)) => Some(proxied - direct),
            _ => None,
        },
    });
    let settings = profile::show_all(&side).await?;
    let profile_document = profile::build(
        settings,
        &postgres_version,
        replication_rows.clone(),
        proxy_block.clone(),
        host.clone(),
        json!(frontends
            .iter()
            .map(|child| json!({
                "instance_id": child.spec.instance_id,
                "stratum_port": child.spec.stratum_port,
                "audit_port": child.spec.audit_port,
                "environment": frontend::redacted(&child.environment),
            }))
            .collect::<Vec<_>>()),
    );
    let profile_canonical = profile::canonical_json(&profile_document);
    let profile_digest = profile::digest(&profile_canonical);
    let profile_path = args.out.join("database-profile.json");
    profile::write_document(&profile_path, &profile_canonical)?;

    let subject: BTreeMap<String, String> = [
        ("coordinator_revision", ctx.revision.clone()),
        ("coordinator_image_digest", ctx.server_digest.clone()),
        ("postgres_server_version", postgres_version.clone()),
        ("database_profile_sha256", profile_digest.clone()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect();

    let slow_delay_observed = runs
        .iter()
        .find(|phase| phase.plan.name == "slow_database")
        .map(|phase| phase.proxy_delay_configured_ms as f64)
        .unwrap_or(args.slow_db_delay_ms as f64);
    let mut phase_evidence = Vec::new();
    let mut artifact_phase_names = Vec::new();
    let mut unmeasured_phases: Vec<String> = Vec::new();
    for phase in &runs {
        if !phase.plan.in_artifact {
            continue;
        }
        let reconciliation = phase_reconciliations
            .iter()
            .find(|(name, _)| *name == phase.plan.name)
            .map(|(_, rec)| rec)
            .context("missing reconciliation")?;
        let latency = phase_latency(&collected.submits, &phase.plan.name);
        // The artifact's schema requires a p50 and a p99. A phase that
        // acknowledged nothing has no latency to state, and writing 0.000 would
        // put a measurement in the evidence that was never taken -- the same
        // unknown-as-zero this crate refuses everywhere else. The validator
        // would refuse such an artifact anyway, on the share counts, but the
        // harness must not author the false number and leave catching it to
        // someone else.
        if has_no_ack_latency(&latency) {
            unmeasured_phases.push(phase.plan.name.clone());
        }
        let reconnects = collected
            .reconnects
            .iter()
            .filter(|record| record.phase == phase.plan.name && record.completed)
            .count() as u64;
        phase_evidence.push(PhaseEvidence {
            name: phase.plan.name.clone(),
            duration_millis: phase.duration_millis,
            offered: reconciliation.offered.len() as u64,
            acknowledged: reconciliation.acknowledged.len() as u64,
            committed: reconciliation.committed.len() as u64,
            rejected_valid: rejected_valid_count(&collected.submits, &phase.plan.name),
            missing: reconciliation.missing.len() as u64,
            unexpected: *unexpected_by_phase.get(&phase.plan.name).unwrap_or(&0) as u64,
            acknowledged_digest: reconciliation.acknowledged_digest(),
            committed_digest: reconciliation.committed_digest(),
            ack_p50_millis: latency.p50.unwrap_or(0.0),
            ack_p99_millis: latency.p99.unwrap_or(0.0),
            reconnect_events: (phase.plan.name == "reconnect").then_some(reconnects),
            database_delay_millis: (phase.plan.name == "slow_database")
                .then_some(slow_delay_observed),
        });
        artifact_phase_names.push(phase.plan.name.clone());
    }
    if !unmeasured_phases.is_empty() {
        // Treated as a contradicted premise rather than given a code of its
        // own: a capacity artifact rests on every phase it names having
        // measured acknowledgements, and one that did not makes the artifact
        // evidence of nothing, which is exactly what exit 8 already means.
        withhold.get_or_insert_with(|| {
            Withhold::PremiseContradicted(format!(
                "{} acknowledged no shares, so the artifact has no ACK latency to state for it",
                unmeasured_phases.join(", ")
            ))
        });
    }
    let overall_latency = ack_latency(
        &collected.submits,
        &artifact_phase_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );
    let union_ack: BTreeSet<String> = phase_evidence
        .iter()
        .flat_map(|phase| {
            phase_reconciliations
                .iter()
                .find(|(name, _)| *name == phase.name)
                .map(|(_, rec)| rec.acknowledged.clone())
                .unwrap_or_default()
        })
        .collect();
    let union_committed: BTreeSet<String> = phase_evidence
        .iter()
        .flat_map(|phase| {
            phase_reconciliations
                .iter()
                .find(|(name, _)| *name == phase.name)
                .map(|(_, rec)| rec.committed.clone())
                .unwrap_or_default()
        })
        .collect();
    let inputs = ArtifactInputs {
        artifact_kind: ctx.artifact_kind.clone(),
        run_id: ctx.run_id,
        generated_at: chrono::Utc::now(),
        subject: subject.clone(),
        durability: durability.clone(),
        configuration: configuration.clone(),
        forecast_peak_shares_per_second: format!("{}", args.forecast_peak_shares_per_second),
        ack_p99_limit_milliseconds: format!("{}", args.ack_p99_limit_ms),
        overall_ack_p50_millis: overall_latency.p50.unwrap_or(0.0),
        overall_ack_p99_millis: overall_latency.p99.unwrap_or(0.0),
        overall_acknowledged_digest: digest::share_id_digest(union_ack.iter().map(String::as_str)),
        overall_committed_digest: digest::share_id_digest(
            union_committed.iter().map(String::as_str),
        ),
        phases: phase_evidence.clone(),
    };
    // An aborted run gets no artifact: its evidence is incomplete however
    // complete a partial phase looks, and `artifact::build` would refuse a
    // run that never reached all three required phases anyway. Neither does
    // a run whose frontend logged a hard refusal of the size at any point,
    // nor one whose frontend advertised a share difficulty other than the
    // configured one: their numbers may be complete and still describe a
    // size the server did not serve in full, or work the configuration does
    // not name (EP-ERRORS). The side report below still carries every
    // number, with the reason.
    let mut evidence = artifact::write_or_withhold(
        &inputs,
        withhold.as_ref(),
        &args.out,
        &ctx.server_bin.display().to_string(),
    )?;
    // A rejection whose reason the classifier does not recognise already
    // invalidates the artifact, through rejected_valid_shares, and used to
    // do so quietly: the reader found it by reading the rejections JSON.
    // It means the harness's model of the server is out of date, which is
    // the reader's problem to solve, so the refusal reason names it.
    let unrecognised = unrecognised_rejection_reasons(&collected.submits);
    name_unrecognised_reasons(&mut evidence, &unrecognised);

    // --- side report ------------------------------------------------------
    let slowest_rate = phase_evidence
        .iter()
        .map(|phase| {
            phase.acknowledged as f64
                / (phase.duration_millis as f64 / 1000.0).max(f64::MIN_POSITIVE)
        })
        .fold(f64::INFINITY, f64::min);
    let worst_p99 = phase_evidence
        .iter()
        .map(|phase| phase.ack_p99_millis)
        .max_by(f64::total_cmp);
    let harness_bugs: Vec<&SubmitRecord> = collected
        .submits
        .iter()
        .filter(|record| !record.reoffer && bug_rejection(record))
        .collect();
    let driven: Vec<DrivenPhase> = runs
        .iter()
        .map(|phase| DrivenPhase {
            name: phase.plan.name.clone(),
            kill_indeterminate: phase
                .mid_flight_indeterminate
                .iter()
                .map(|record| record.share_id.clone())
                .collect(),
        })
        .collect();
    let gaps = classify_gaps(
        &driven,
        &phase_reconciliations,
        &attribution,
        &collected.submits,
        args.share_commit_timeout_seconds,
    );
    let GapReport {
        findings: durability_findings,
        divergences,
        unknown_outcome_commits,
        no_response_commits,
    } = gaps;
    let node_submissions = ctx.node_state.submissions();
    let tip_changes = ctx.node_state.tip_changes();
    let dense_cadence = dense_cadence_report(
        args,
        &runs,
        &collected,
        &node_submissions,
        &tip_changes,
        &committed,
        aborted.as_deref(),
    );
    let side_report = json!({
        "schema": report::SCHEMA,
        "run_id": ctx.run_id.to_string(),
        "run_tag": ctx.run_tag,
        "started_at": ctx.started_wall.to_rfc3339(),
        "finished_at": chrono::Utc::now().to_rfc3339(),
        "aborted": aborted,
        "dirty": ctx.dirty,
        "artifact_kind": ctx.artifact_kind,
        "host": host,
        "versions": {
            "harness_version": env!("CARGO_PKG_VERSION"),
            "harness_build_profile": ctx.harness_profile,
            "server_binary": ctx.server_bin.display().to_string(),
            "server_build_profile": ctx.server_profile.as_str(),
            "server_binary_sha256": ctx.server_digest,
            "coordinator_revision": ctx.revision,
            "server_revision_evidence": ctx.revision_evidence,
            "postgres_server_version": postgres_version,
            "rustc_target": std::env::consts::ARCH,
        },
        "file_descriptor_limit": {"before": ctx.fd_before, "after": ctx.fd_after},
        "topology": {
            "frontends": args.frontends,
            "sessions": args.sessions,
            "sessions_per_frontend": per_frontend,
            "max_outstanding_per_session": args.max_outstanding_per_session,
            "plan": args.plan,
            "payout_address": ctx.payout_address,
            "share_id_prefix": ctx.share_prefix,
            "writer_ids": writer_ids,
        },
        "frontend_environment": frontends.iter().map(|child| json!({
            "instance_id": child.spec.instance_id,
            "stratum_port": child.spec.stratum_port,
            "audit_port": child.spec.audit_port,
            "restarts": child.restarts,
            "stdout_log": child.stdout_path.display().to_string(),
            "stderr_log": child.stderr_path.display().to_string(),
            "environment": frontend::redacted(&child.environment),
        })).collect::<Vec<_>>(),
        "retired_configuration_keys": frontend::RETIRED_CONFIGURATION_KEYS,
        "window": {
            "template_bits": window::TEMPLATE_BITS,
            "scaled_network_difficulty": solution.scaled_network_difficulty.to_string(),
            "window_weight": solution.window_weight.to_string(),
            "share_difficulty_diff1": format!("{}", solution.share_difficulty),
            "scaled_share_difficulty": solution.scaled_share_difficulty.to_string(),
            "requested_window_shares": solution.requested_window,
            "computed_window_shares": solution.computed_window,
            "ledger_window_shares_at_start": window_at_start,
            "ledger_window_shares_at_end": window_at_end,
            "expected_hashes_per_share": solution.hashes_per_share,
            "expected_hashes_per_block": solution.hashes_per_block,
            "seed": {
                "rows": seed_stats.rows,
                "seconds": seed_stats.seconds,
                "rows_per_second": seed_stats.rows_per_second,
                "serialized_bytes": seed_stats.serialized_bytes,
                "target_share_bytes": seed.target_share_bytes(),
            },
        },
        "database": {
            "mode": if args.database_url.is_some() { "external" } else { "managed" },
            "durability": durability,
            "replication": replication_rows,
            "pg_stat_statements": ctx.pg_stat_statements,
            "delay_proxy": proxy_block,
            "database_profile_sha256": profile_digest,
            "database_profile_path": profile_path.display().to_string(),
        },
        "phases": runs.iter().map(|phase| phase_report(
            phase, &collected, &phase_reconciliations, &unexpected_by_phase,
        )).collect::<Vec<_>>(),
        "reconciliation": {
            "digest_definition": digest::DIGEST_DEFINITION,
            "committed_rows_for_this_run": committed.len(),
            "unexpected_attributed_by_phase": attribution.by_phase.iter()
                .map(|(name, rows)| json!({"phase": name, "count": rows.len(),
                    "sample": rows.iter().take(10).collect::<Vec<_>>()}))
                .collect::<Vec<_>>(),
            "unexpected_outside_phases": {
                "count": attribution.outside_phases.len(),
                "sample": attribution.outside_phases.iter().take(10).collect::<Vec<_>>(),
                "note": "also a durability finding, under durability_findings with kind \
                         committed share that no phase offered; the count here is the \
                         same rows",
            },
        },
        "time_to_usable_work": time_to_usable_work(
            &external_tips, &tip_changes, &collected, args.sessions,
        ),
        "dense_cadence": dense_cadence,
        "node": {
            "url": ctx.node_url,
            "template_bits": window::TEMPLATE_BITS,
            "submissions": node_submissions,
            "tip_changes": tip_changes.iter().map(|change| json!({
                "hash": change.hash, "height": change.height,
                "origin": change.origin, "wall": change.wall.to_rfc3339(),
            })).collect::<Vec<_>>(),
            "rpc_call_counts": ctx.node_state.rpc_call_counts(),
        },
        "premise": premise_block(
            premise_contradiction.as_deref(),
            &collected.difficulty_mismatches,
            &replication_premise,
        ),
        "client": {
            "discarded_block_solutions": collected.discarded_block_solutions,
            "discarded_offers": collected.discarded_offers,
            "difficulty_mismatches": collected.difficulty_mismatches.iter()
                .map(|(session, advertised, configured)| json!({
                    "session": session, "advertised": advertised, "configured": configured}))
                .collect::<Vec<_>>(),
            "difficulty_mismatches_note": "repeated under premise, which says what they \
                                           mean for the run",
            "failures": collected.failures.iter().take(200)
                .map(|failure| json!({
                    "session": failure.session, "phase": failure.phase, "kind": failure.kind,
                    "recorded_as_submit": failure.recorded, "error": failure.error,
                }))
                .collect::<Vec<_>>(),
            "failure_count": collected.failures.len(),
            "failures_by_kind": failures_by_kind(&collected.failures),
            "failures_note": "every failure is also in its phase's offer_accounting, which \
                              says what it does to that phase's dispatched count",
            "connects": collected.connects,
            "disconnects": collected.disconnects.len(),
        },
        "reconnects": reconnect_report(&collected),
        "mid_flight_kill": mid_flight_report(&runs, &collected, &committed),
        "rejections": rejection_report(&collected.submits),
        "harness_bug_rejections": harness_bugs.iter().take(50).map(|record| json!({
            "share_id": record.share_id, "phase": record.phase, "job_id": record.job_id,
            "outcome": describe_outcome(&record.outcome),
        })).collect::<Vec<_>>(),
        "harness_bug_rejection_count": harness_bugs.len(),
        "blocked": {
            "blocked": late_hard_block.is_some(),
            "error": late_hard_block.as_deref(),
            "log_matches": blocked,
            "note": BLOCKED_NOTE,
        },
        "durability_findings": durability_findings,
        "ack_commit_divergence": {
            "definition": "a share PostgreSQL holds that the server refused with \
                           ledger-confirmation-failed. Nothing was lost: the share is credited \
                           in the payout window, but the miner was told it was not confirmed.",
            "mechanism": "coordinator.rs wraps the append in \
                          tokio::time::timeout(share_commit_timeout, save); when it fires the \
                          sqlx future is dropped mid-COMMIT and PostgreSQL can still commit.",
            "server_issue": "Qbit-Org/qbit-mining-bootstrap#324",
            "share_commit_timeout_seconds": args.share_commit_timeout_seconds,
            "count": divergences.len(),
            "shares": divergences,
        },
        "unknown_outcome_commits": {
            "definition": "a share PostgreSQL holds that the server answered \
                           ledger-outcome-unknown. Nothing was lost and the answer was honest \
                           -- the server had waited past the commit deadline and its grace \
                           window without a reply -- but the miner was still refused a share \
                           the database holds.",
            "server_issue": "Qbit-Org/qbit-mining-bootstrap#324",
            "count": unknown_outcome_commits.len(),
            "shares": unknown_outcome_commits,
        },
        "no_response_commits": {
            "definition": NO_RESPONSE_COMMIT_DEFINITION,
            "count": no_response_commits.len(),
            "shares": no_response_commits,
        },
        "honest_value_notes": report::honest_value_notes(),
        "drain": {
            "note": "sessions quiesce before the run closes their sockets, for up to the \
                     configured share-commit timeout plus the drain margin -- the limit the \
                     phase boundaries and the drained restart wait. A submit still outstanding \
                     after that is one the server's own deadline had already passed, and it is \
                     recorded as no-response (run ended) in its phase.",
            "limit_seconds": settle_limit.as_secs_f64(),
            "waited_seconds": drained_seconds,
            "submits_outstanding_at_stop": undrained,
        },
        "validator": validator_block(&evidence, args, slowest_rate, worst_p99, &unrecognised),
        "stale_outputs_removed": ctx.stale_outputs_removed,
    });
    let report_path = args.out.join("load-harness-report.json");
    report::write_json(&report_path, &side_report)?;

    // --- exit code --------------------------------------------------------
    println!("{}", summary_text(&side_report, &evidence, &unrecognised));
    // The frontends were stopped before the final hard-block check above.
    side.close().await;
    let outcome = RunOutcome {
        withhold: withhold.as_ref(),
        durability_findings: durability_findings.as_array().map(Vec::len).unwrap_or(0),
        harness_bug_rejections: harness_bugs.len(),
        divergences: divergences.len(),
        unknown_outcome_commits: unknown_outcome_commits.len(),
        no_response_commits: no_response_commits.len(),
        no_response_commits_mid_run: no_response_commits
            .iter()
            .filter(|share| share["window_ended"] != serde_json::Value::Bool(true))
            .count(),
    };
    if let Some(line) = outcome.explanation(&report_path) {
        eprintln!("{line}");
    }
    Ok(outcome.exit_code())
}

// --- phase driving -------------------------------------------------------

/// What driving one phase produced.
pub struct PhaseOutcome {
    pub tokens: u64,
    pub dispatched: u64,
    pub shortfall: u64,
    pub min_mem_available_kib: Option<u64>,
    pub aborted: Option<String>,
    pub scheduled_blocks: usize,
    pub frontend_restarts: usize,
    /// Completed drained restarts, with the scrapes bracketing each reset.
    pub restart_records: Vec<RestartRecord>,
    pub indeterminate: Vec<SubmitRecord>,
    /// Submits outstanding on the killed frontend at the instant of the kill.
    pub outstanding_at_kill: Option<usize>,
    /// The dense-cadence schedule this phase drove, and what it drove.
    pub dense_offsets: Vec<f64>,
    pub dense_landings: Vec<Landing>,
    /// Schedule slots the landing budget could not pay for.
    pub slots_over_budget: usize,
    /// When the phase stopped scheduling: its deadline, or the abort. This
    /// is the end of the measured window -- `duration_millis`, the lock and
    /// process windows and the rates' denominator -- and it is stamped
    /// before a restart or kill still in flight is seen through, so that
    /// wait is boundary time, not the phase's.
    pub ended: Instant,
    pub ended_wall: chrono::DateTime<chrono::Utc>,
}

/// Drive one phase: the open-loop schedule from `plan`, with the phase's
/// events (reconnects, the drained restart, the mid-flight kill, scheduled
/// blocks, tips and dense landings) placed on their offsets, until the
/// plan's deadline or an abort. The outcome's `ended` is that instant. A
/// restart or kill still in flight then is completed before this returns,
/// with nothing scheduled meanwhile, so the next phase never offers to a
/// session that is still paused or pointed at a dead process.
#[allow(clippy::too_many_arguments)]
pub async fn drive_phase(
    args: &Args,
    plan: &PhasePlan,
    sessions: &[SessionHandle],
    frontends: &mut [Frontend],
    samplers: &[ProcessSampler],
    node_state: &crate::node::NodeState,
    external_tips: &mut Vec<crate::node::TipChange>,
    remaining_blocks: &mut usize,
    remaining_tips: &mut usize,
    collected: &Arc<Mutex<Collected>>,
) -> Result<PhaseOutcome> {
    let started = Instant::now();
    let duration = Duration::from_secs(plan.seconds);
    let cursor = AtomicUsize::new(0);
    // Every offer this loop places is stamped with this phase, however late
    // the session gets to send it.
    let phase: Arc<str> = Arc::from(plan.name.as_str());
    let mut outcome = PhaseOutcome {
        tokens: 0,
        dispatched: 0,
        shortfall: 0,
        min_mem_available_kib: measure::mem_available_kib(),
        aborted: None,
        scheduled_blocks: 0,
        frontend_restarts: 0,
        restart_records: Vec::new(),
        indeterminate: Vec::new(),
        outstanding_at_kill: None,
        dense_offsets: Vec::new(),
        dense_landings: Vec::new(),
        slots_over_budget: 0,
        ended: started,
        ended_wall: chrono::Utc::now(),
    };
    // Event schedule inside the phase.
    let reconnect_interval = if plan.reconnects {
        Some(duration.as_secs_f64() / (args.reconnect_target as f64 + 2.0))
    } else {
        None
    };
    let restart_at = (plan.reconnects && args.frontends >= 2).then(|| duration.as_secs_f64() / 3.0);
    let mut next_reconnect = reconnect_interval.unwrap_or(f64::INFINITY);
    let mut reconnect_cursor = 0usize;
    let mut restart_done = restart_at.is_none();
    // The drained restart in flight, if any. It is polled from this loop and
    // never awaited, so the other frontends keep receiving scheduled load
    // while one is away: that outage is what the phase measures.
    let mut restart: Option<RestartDriver> = None;
    let restart_drain_limit = drain_limit(args.share_commit_timeout_seconds);
    let restart_ready_limit = Duration::from_secs(args.work_timeout);
    // With `--cadence dense`, `--scheduled-blocks` is the dense phase's
    // landing budget and `steady_state` schedules none, so the budget is not
    // spent before the phase that measures it. Without it, nothing changes.
    let dense_run = args.cadence()?.is_dense();
    let block_times: Vec<f64> =
        if plan.name == "steady_state" && !dense_run && *remaining_blocks > 0 {
            let count = *remaining_blocks;
            (0..count)
                .map(|index| duration.as_secs_f64() * (index as f64 + 1.0) / (count as f64 + 1.0))
                .collect()
        } else {
            Vec::new()
        };
    let mut block_cursor = 0usize;
    let tip_times: Vec<f64> = if plan.name == "warm_up" && *remaining_tips > 0 {
        let count = *remaining_tips;
        (0..count)
            .map(|index| duration.as_secs_f64() * (index as f64 + 1.0) / (count as f64 + 2.0))
            .collect()
    } else {
        Vec::new()
    };
    let mut tip_cursor = 0usize;
    let mut kill_done = !plan.mid_flight_kill;
    // The mid-flight kill in flight, if any: polled from this loop and
    // never awaited, for the same reason the drained restart is. Awaiting
    // it stalled the scheduler across the kill, the relaunch, the readiness
    // wait and two settles, so the healthy frontends got no traffic and the
    // next tick turned the gap into a burst plus shortfall (EP-STATE).
    let mut kill: Option<KillDriver> = None;
    // The dense-cadence schedule: one own-block landing per offset, each paid
    // for out of the landing budget. A slot the budget cannot pay for is
    // counted, never silently dropped (EP-ERRORS).
    let landing_offsets: Vec<f64> = if plan.dense_cadence {
        cadence::landing_offsets(&args.cadence_gaps()?, duration.as_secs_f64())
    } else {
        Vec::new()
    };
    outcome.dense_offsets = landing_offsets.clone();
    let mut landing_cursor = 0usize;
    let mut mem_check = Instant::now();

    let mut ticker = tokio::time::interval(Duration::from_millis(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    loop {
        ticker.tick().await;
        let elapsed = started.elapsed();
        if elapsed >= duration {
            break;
        }
        let seconds = elapsed.as_secs_f64();
        // Open-loop token bucket: the clock decides how many shares were
        // offered, and any that cannot be placed are counted as a shortfall
        // rather than deferred into a backlog.
        let want = (seconds * plan.rate).floor() as u64;
        while outcome.tokens < want {
            outcome.tokens += 1;
            if offer_round_robin(sessions, &cursor, args.max_outstanding_per_session, &phase) {
                outcome.dispatched += 1;
            } else {
                outcome.shortfall += 1;
            }
        }
        if seconds >= next_reconnect {
            next_reconnect += reconnect_interval.unwrap_or(f64::INFINITY);
            if let Some(session) = sessions.get(reconnect_cursor % sessions.len()) {
                let _ = session.control.send(client::Control::Reconnect {
                    reason: "client-initiated".into(),
                    phase: phase.clone(),
                });
            }
            reconnect_cursor += 1;
        }
        if let Some(at) = restart_at {
            if !restart_done && seconds >= at {
                restart_done = true;
                let index = 1.min(frontends.len() - 1);
                restart = Some(RestartDriver::start(
                    index,
                    sessions,
                    restart_drain_limit,
                    restart_ready_limit,
                ));
            }
        }
        if let Some(driver) = restart.as_mut() {
            match driver.poll(sessions, frontends, samplers) {
                Ok(None) => {}
                Ok(Some(record)) => {
                    outcome.frontend_restarts += 1;
                    outcome.restart_records.push(record);
                    restart = None;
                }
                Err(error) => {
                    outcome.aborted = Some(format!(
                        "the drained restart of load-fe-{} could not be performed: {error:#}",
                        driver.index()
                    ));
                    break;
                }
            }
        }
        if block_cursor < block_times.len() && seconds >= block_times[block_cursor] {
            block_cursor += 1;
            *remaining_blocks = remaining_blocks.saturating_sub(1);
            outcome.scheduled_blocks += 1;
            if let Some(session) = sessions.first() {
                let _ = session.control.send(client::Control::ScheduledBlock);
            }
        }
        if landing_cursor < landing_offsets.len() && seconds >= landing_offsets[landing_cursor] {
            let offset = landing_offsets[landing_cursor];
            landing_cursor += 1;
            if *remaining_blocks == 0 || sessions.is_empty() {
                outcome.slots_over_budget += 1;
            } else {
                *remaining_blocks -= 1;
                outcome.scheduled_blocks += 1;
                // Landings rotate across sessions so no single frontend is
                // privileged with an early view of its own block.
                let index = outcome.dense_landings.len() % sessions.len();
                let session = &sessions[index];
                let _ = session.control.send(client::Control::ScheduledBlock);
                outcome.dense_landings.push(Landing {
                    index: outcome.dense_landings.len(),
                    scheduled_offset_seconds: offset,
                    requested_monotonic: Instant::now(),
                    requested_wall: chrono::Utc::now(),
                    session: session.index,
                    frontend: session.frontend.load(Ordering::Relaxed),
                });
            }
        }
        if tip_cursor < tip_times.len() && seconds >= tip_times[tip_cursor] {
            tip_cursor += 1;
            *remaining_tips = remaining_tips.saturating_sub(1);
            external_tips.push(node_state.mint_external_block());
        }
        if !kill_done && seconds >= duration.as_secs_f64() / 3.0 {
            kill_done = true;
            let index = if frontends.len() >= 2 { 1 } else { 0 };
            kill = Some(KillDriver::start(
                index,
                restart_ready_limit,
                restart_drain_limit,
                collected,
            ));
        }
        if let Some(driver) = kill.as_mut() {
            match driver.poll(sessions, frontends, samplers, collected) {
                Ok(None) => {}
                Ok(Some(record)) => {
                    outcome.indeterminate = record.indeterminate;
                    outcome.outstanding_at_kill = Some(record.outstanding_at_kill);
                    outcome.frontend_restarts += 1;
                    kill = None;
                }
                Err(error) => {
                    outcome.aborted = Some(format!(
                        "the mid-flight kill of load-fe-{} could not be completed: {error:#}",
                        driver.index()
                    ));
                    break;
                }
            }
        }
        if mem_check.elapsed() >= Duration::from_secs(1) {
            mem_check = Instant::now();
            let available = measure::mem_available_kib();
            if let Some(available) = available {
                outcome.min_mem_available_kib = Some(
                    outcome
                        .min_mem_available_kib
                        .map_or(available, |current| current.min(available)),
                );
                if available < args.min_mem_available_mib * 1024 {
                    outcome.aborted = Some(format!(
                        "MemAvailable fell to {} MiB, below the {} MiB floor",
                        available / 1024,
                        args.min_mem_available_mib
                    ));
                    break;
                }
            }
            for child in frontends.iter_mut() {
                if let Some(status) = child.exited() {
                    outcome.aborted = Some(format!(
                        "{} exited unexpectedly with {status}",
                        child.spec.instance_id
                    ));
                    break;
                }
            }
            if outcome.aborted.is_some() {
                break;
            }
        }
    }
    // The measured window closes here, with the schedule: every number that
    // is per unit of phase time divides by this span. It used to be stamped
    // by the caller after the wait below, so a restart that outran the
    // deadline -- a drain that took until the deadline, then a relaunch and
    // its readiness wait -- was inside `duration_millis`, the lock and
    // process windows and the achieved-rate denominator while nothing was
    // being offered, and a 60 s phase came out longer and slower than it
    // was (EP-OBSERVABILITY).
    outcome.ended = Instant::now();
    outcome.ended_wall = chrono::Utc::now();
    // A restart or a kill still in flight at the phase boundary is seen
    // through, so its sessions are retargeted -- and, for the kill, its
    // re-offers sent -- before the next phase offers to them. Their own
    // deadlines bound the wait, and a failure aborts as it would inside the
    // phase. Nothing is offered meanwhile, and the wait is boundary time,
    // outside the window recorded above, like the settle before the next
    // phase's delay is applied.
    while outcome.aborted.is_none() && (restart.is_some() || kill.is_some()) {
        if let Some(driver) = restart.as_mut() {
            match driver.poll(sessions, frontends, samplers) {
                Ok(None) => {}
                Ok(Some(record)) => {
                    outcome.frontend_restarts += 1;
                    outcome.restart_records.push(record);
                    restart = None;
                }
                Err(error) => {
                    outcome.aborted = Some(format!(
                        "the drained restart of load-fe-{} could not be performed: {error:#}",
                        driver.index()
                    ));
                    break;
                }
            }
        }
        if let Some(driver) = kill.as_mut() {
            match driver.poll(sessions, frontends, samplers, collected) {
                Ok(None) => {}
                Ok(Some(record)) => {
                    outcome.indeterminate = record.indeterminate;
                    outcome.outstanding_at_kill = Some(record.outstanding_at_kill);
                    outcome.frontend_restarts += 1;
                    kill = None;
                }
                Err(error) => {
                    outcome.aborted = Some(format!(
                        "the mid-flight kill of load-fe-{} could not be completed: {error:#}",
                        driver.index()
                    ));
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(outcome)
}

fn offer_round_robin(
    sessions: &[SessionHandle],
    cursor: &AtomicUsize,
    limit: usize,
    phase: &Arc<str>,
) -> bool {
    let count = sessions.len();
    for _ in 0..count {
        let index = cursor.fetch_add(1, Ordering::Relaxed) % count;
        if sessions[index].try_offer(limit, phase) {
            return true;
        }
    }
    false
}

/// The submits whose answer the kill destroyed: every no-response recorded
/// since the kill began -- `since` is the length of the submit log at that
/// instant -- on the killed frontend's own sessions. A no-response from a
/// session on another frontend in the same window (a socket closed for its
/// own reasons) is not the kill's and is neither counted in its census nor
/// re-offered as one of its shares (EP-STATE).
pub fn indeterminate_after_kill(
    records: &[SubmitRecord],
    since: usize,
    frontend: usize,
) -> Vec<SubmitRecord> {
    records[since.min(records.len())..]
        .iter()
        .filter(|record| {
            record.frontend == frontend
                && !record.reoffer
                && matches!(record.outcome, Outcome::NoResponse { .. })
        })
        .cloned()
        .collect()
}

// --- helpers -------------------------------------------------------------

fn git(args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .output()
        .with_context(|| format!("running git {}", args.join(" ")))?;
    ensure!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

fn resolve_server_bin(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        ensure!(path.exists(), "{} does not exist", path.display());
        return Ok(path);
    }
    let own = std::env::current_exe().context("locating this executable")?;
    let sibling = own
        .parent()
        .context("this executable has no directory")?
        .join("qbit-prism-server");
    ensure!(
        sibling.exists(),
        "no qbit-prism-server beside {}; pass --server-bin",
        own.display()
    );
    Ok(sibling)
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

/// `host:port` of a `postgresql://` URL, for the delay proxy to dial.
///
/// The URL is read with the parser SQLx already accepted it with, so the host
/// and port the proxy fronts are the ones SQLx connected to: a hostname, an
/// IPv4 or a bracketed IPv6 literal, with or without an explicit port, or a
/// `host`/`port` query parameter, and the default port when none is given.
/// Testing the authority for a colon, as this once did, took the colons
/// inside `[::1]` for a port and handed the proxy an address without one
/// (EP-VALIDATION). An IPv6 literal is bracketed on the way out, which is
/// the form a `host:port` lookup needs.
pub fn host_port(url: &str) -> Result<String> {
    let options: sqlx::postgres::PgConnectOptions = url.parse().with_context(|| {
        format!(
            "parsing the database URL {}",
            frontend::redact_url_secrets(url)
        )
    })?;
    let host = options.get_host();
    // Two ways a socket reaches here. An explicit `?host=/var/run/postgresql`
    // sets `socket`, and SQLx's own default host is a socket directory on some
    // platforms -- on macOS a URL with no host at all yields /tmp -- which
    // arrives as a *host* that happens to be a path, leaving `socket` empty. Both
    // are refused with the same reason, because `lookup_host` would otherwise
    // report a path as a DNS failure and hide what is really wrong.
    ensure!(
        options.get_socket().is_none() && !host.starts_with('/'),
        "the database URL names a Unix socket ({host}), which the delay proxy cannot front; \
         give it a TCP host and port"
    );
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    Ok(format!("{host}:{}", options.get_port()))
}

/// Add an `application_name` parameter, so `pg_stat_activity` can say which
/// frontend a backend belongs to. sqlx reads it out of the URL; whether it
/// really carried it is verified against `pg_stat_activity`, never assumed.
pub fn with_application_name(url: &str, name: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}application_name={name}")
}

/// The query parameters that name the endpoint SQLx dials. SQLx reads them
/// after the authority and lets them win, so a URL carrying one connects
/// wherever it says however the authority is rewritten.
pub const ENDPOINT_PARAMETERS: [&str; 3] = ["host", "hostaddr", "port"];

/// Replace the endpoint of a `postgresql://` URL with `host_port`, keeping the
/// user info, the path and every option that does not name an endpoint.
///
/// The authority is rewritten, and the libpq-style `host`, `hostaddr` and
/// `port` parameters are dropped from the query string, because SQLx applies
/// those over the authority: with `postgresql:///db?host=db.internal` left
/// intact every frontend would dial `db.internal` directly, the proxy would
/// see no connection, and the `slow_database` phase would report a delay that
/// nothing applied (EP-OBSERVABILITY). Everything else in the query --
/// `sslmode`, `application_name`, `options` -- is kept as written.
pub fn rewrite_host(url: &str, host_port: &str) -> Result<String> {
    let (scheme, rest) = url
        .split_once("://")
        .context("database URL has no scheme")?;
    // The authority ends at the first `/` or `?`. User info is looked for
    // inside it only, so an `@` in the query cannot be taken for one.
    let (authority, tail) = rest
        .find(['/', '?'])
        .map_or((rest, ""), |at| rest.split_at(at));
    let userinfo = authority.rsplit_once('@').map(|(user, _)| user);
    let (path, query) = match tail.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (tail, None),
    };
    let kept: Vec<&str> = query
        .map(|query| {
            query
                .split('&')
                .filter(|pair| !pair.is_empty() && !names_endpoint(pair))
                .collect()
        })
        .unwrap_or_default();
    let mut rewritten = match userinfo {
        Some(user) => format!("{scheme}://{user}@{host_port}{path}"),
        None => format!("{scheme}://{host_port}{path}"),
    };
    if !kept.is_empty() {
        rewritten.push('?');
        rewritten.push_str(&kept.join("&"));
    }
    Ok(rewritten)
}

/// Whether one `key=value` pair of a query string sets an endpoint parameter.
/// The key is compared as SQLx reads it, percent-decoded, so `h%6Fst` is
/// `host` here as it is there.
fn names_endpoint(pair: &str) -> bool {
    let key = pair.split_once('=').map_or(pair, |(key, _)| key);
    let decoded = frontend::percent_decode(key);
    ENDPOINT_PARAMETERS.contains(&decoded.as_str())
}

/// The least a round trip can cost under a one-way per-chunk delay: the
/// request is held once on the way in and the reply once on the way out, and
/// the proxy's sleep never returns early, so a trip that comes back sooner
/// did not go through the proxy.
pub fn delay_floor_millis(delay_ms: u64) -> f64 {
    2.0 * delay_ms as f64
}

/// Refuse a delayed round-trip measurement that did not pay the delay. A
/// phase's delay is believed only once a trip through the URL the frontends
/// were given has been seen to cost it, so a bypass -- a rewrite that left
/// the real endpoint in the URL, a proxy that lost its delay -- is refused
/// with the numbers rather than reported as a measurement (EP-OBSERVABILITY).
pub fn check_delay_observed(delay_ms: u64, observed_median_ms: f64) -> Result<()> {
    let floor = delay_floor_millis(delay_ms);
    ensure!(
        observed_median_ms >= floor,
        "a round trip through the proxied database URL took {observed_median_ms:.3} ms with a \
         {delay_ms} ms one-way delay configured, below the {floor:.0} ms floor a proxied trip \
         must pay; the frontends' connections are not going through the delay proxy"
    );
    Ok(())
}

/// Stop every frontend, then read what each one logged.
///
/// The final hard-block check used to read the frontends' stderr while
/// every one of them was still running, and they were not stopped until
/// after the artifact, the profile and the side report had been written. A
/// scheduled block's rebuild can still be running after the sessions have
/// drained, and one that logged the JSONB-ceiling refusal after the check
/// -- during the replication, window, reconciliation and profile queries
/// that followed it -- was missed: the check found nothing and the harness
/// wrote a self-validating qualification artifact for a window size that
/// had in fact been refused. Stopping first closes that gap: `Frontend::kill`
/// sends the process group SIGKILL and reaps it, stderr is a file the
/// process wrote to directly, and a reaped process has nothing left to
/// write, so what this returns is the whole log of the run and nothing can
/// be added to it afterwards (EP-OBSERVABILITY).
pub fn stop_and_scan_logs(frontends: &mut [Frontend]) -> Vec<BlockedLog> {
    for child in frontends.iter_mut() {
        child.kill();
    }
    frontends
        .iter()
        .flat_map(|child| scan_logs(&child.read_stderr()))
        .collect()
}

fn scan_logs(text: &str) -> Vec<BlockedLog> {
    text.lines()
        .filter_map(classify::classify_log_line)
        .collect()
}

fn rejection_of(record: &SubmitRecord) -> Option<&Rejection> {
    match &record.outcome {
        Outcome::Rejected(rejection) => Some(rejection),
        _ => None,
    }
}

/// A rejection that means the harness offered work the server was right to
/// refuse. A re-offer's `duplicate-share` is expected and never counted here.
fn bug_rejection(record: &SubmitRecord) -> bool {
    !record.reoffer
        && rejection_of(record)
            .map(|rejection| classify::classify(rejection) == RejectionClass::HarnessBug)
            .unwrap_or(false)
}

fn harness_bug_count(records: &[SubmitRecord], phase: &str) -> u64 {
    records
        .iter()
        .filter(|record| record.phase == phase && bug_rejection(record))
        .count() as u64
}

/// A rejection of a share the harness believed valid: everything except the
/// races the server is entitled to lose. A backend refusal is a capacity
/// result, not a harness defect, but it is still a share that did not get its
/// acknowledgement, so the artifact has to carry it.
fn rejected_valid(record: &SubmitRecord) -> bool {
    !record.reoffer
        && rejection_of(record)
            .map(|rejection| classify::classify(rejection) != RejectionClass::Expected)
            .unwrap_or(false)
}

fn rejected_valid_count(records: &[SubmitRecord], phase: &str) -> u64 {
    records
        .iter()
        .filter(|record| record.phase == phase && rejected_valid(record))
        .count() as u64
}

/// O and A for one phase. A share the server refused in a race it was entitled
/// to lose is not an offered valid share: it never reaches PostgreSQL, and the
/// full census is in the side report. Everything else the harness offered and
/// did not get acknowledged stays in O, so the artifact cannot hide it.
pub fn offered_and_acknowledged(
    records: &[SubmitRecord],
    phase: &str,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut offered = BTreeSet::new();
    let mut acknowledged = BTreeSet::new();
    for record in records.iter().filter(|record| record.phase == phase) {
        if record.reoffer {
            continue;
        }
        match &record.outcome {
            Outcome::Accepted => {
                offered.insert(record.share_id.clone());
                acknowledged.insert(record.share_id.clone());
            }
            Outcome::NoResponse { .. } => {
                offered.insert(record.share_id.clone());
            }
            Outcome::Rejected(rejection) => {
                if classify::classify(rejection) != RejectionClass::Expected {
                    offered.insert(record.share_id.clone());
                }
            }
        }
    }
    (offered, acknowledged)
}

/// Client ACK latency over the named phases: the time from writing a submit
/// line to reading the response that *acknowledged* it. Only an accepted
/// share is an acknowledgement. A refusal answers in microseconds -- the
/// server never reached PostgreSQL for it -- and summarising it with the
/// acknowledgements pulled a phase's p50 to 0.7 ms while accepted shares were
/// taking seconds, far enough for an artifact to pass the ACK p99 limit that
/// its accepted shares were over (EP-OBSERVABILITY). Re-offers are the
/// mid-flight kill's, not the phase's. Refusals are summarised apart, in
/// `rejection_latency`.
pub fn ack_latency(records: &[SubmitRecord], phases: &[&str]) -> measure::LatencySummary {
    measure::summarize(
        records
            .iter()
            .filter(|record| {
                phases.contains(&record.phase.as_str())
                    && !record.reoffer
                    && matches!(record.outcome, Outcome::Accepted)
            })
            .filter_map(|record| record.latency_millis)
            .collect(),
        measure::MILLISECONDS,
        "client monotonic",
    )
}

/// The same span for the shares the server refused, kept apart from the
/// acknowledgements so a fast refusal is visible as a refusal and never as a
/// quick acknowledgement.
pub fn rejection_latency(records: &[SubmitRecord], phase: &str) -> measure::LatencySummary {
    measure::summarize(
        records
            .iter()
            .filter(|record| {
                record.phase == phase
                    && !record.reoffer
                    && matches!(record.outcome, Outcome::Rejected(_))
            })
            .filter_map(|record| record.latency_millis)
            .collect(),
        measure::MILLISECONDS,
        "client monotonic",
    )
}

/// Whether a phase measured no acknowledgement latency at all.
///
/// The artifact's schema requires a p50 and a p99 for every phase it names. A
/// phase that acknowledged nothing has neither, and writing `0.000` would state
/// a measurement that was never taken. The validator would refuse the artifact
/// anyway, on the share counts, but the harness must not author the false
/// number and leave catching it to the consumer.
pub fn has_no_ack_latency(latency: &measure::LatencySummary) -> bool {
    latency.p50.is_none() || latency.p99.is_none()
}

fn phase_latency(records: &[SubmitRecord], phase: &str) -> measure::LatencySummary {
    ack_latency(records, &[phase])
}

fn describe_outcome(outcome: &Outcome) -> Value {
    match outcome {
        Outcome::Accepted => json!({"outcome": "accepted"}),
        Outcome::Rejected(rejection) => json!({
            "outcome": "rejected",
            "code": rejection.code,
            "reason_id": rejection.reason_id,
            "message": rejection.message,
            "class": classify::classify(rejection).as_str(),
        }),
        Outcome::NoResponse { reason } => json!({"outcome": "no-response", "reason": reason}),
    }
}

/// `(phase, reason_id, code, message)`.
type RejectionKey = (String, String, i64, String);
/// Total count, then a count per frontend index.
type RejectionTally = (u64, BTreeMap<usize, u64>);

fn rejection_report(records: &[SubmitRecord]) -> Value {
    let mut by_key: BTreeMap<RejectionKey, RejectionTally> = BTreeMap::new();
    let mut no_response: BTreeMap<String, u64> = BTreeMap::new();
    for record in records {
        match &record.outcome {
            Outcome::Rejected(rejection) => {
                let key = (
                    record.phase.clone(),
                    rejection.reason_id.clone().unwrap_or_default(),
                    rejection.code,
                    rejection.message.clone(),
                );
                let entry = by_key.entry(key).or_default();
                entry.0 += 1;
                *entry.1.entry(record.frontend).or_insert(0) += 1;
            }
            Outcome::NoResponse { .. } => {
                *no_response.entry(record.phase.clone()).or_insert(0) += 1;
            }
            Outcome::Accepted => {}
        }
    }
    json!({
        "by_phase_reason_and_message": by_key.into_iter().map(|((phase, reason, code, message), (count, by_frontend))| {
            let rejection = Rejection { code, reason_id: (!reason.is_empty()).then(|| reason.clone()), message: message.clone() };
            json!({
                "phase": phase, "code": code, "reason_id": reason, "message": message,
                "count": count,
                "class": classify::classify(&rejection).as_str(),
                "rebuild_pending": classify::is_rebuild_pending(&rejection),
                "by_frontend": by_frontend.into_iter().map(|(frontend, count)| json!({"frontend": frontend, "count": count})).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
        "no_response_by_phase": no_response,
    })
}

fn reconnect_report(collected: &Collected) -> Value {
    let mut by_phase: BTreeMap<String, (u64, u64, Vec<f64>)> = BTreeMap::new();
    for record in &collected.reconnects {
        let entry = by_phase.entry(record.phase.clone()).or_default();
        if record.completed {
            entry.0 += 1;
            entry.2.push(record.seconds);
        } else {
            entry.1 += 1;
        }
    }
    json!({
        "definition": "a completed reconnect is a close followed by a re-authorize and a job; \
                       time_to_reconnect_milliseconds runs from the close to the job, every \
                       failed attempt and backoff in between included, so it is the outage \
                       and not the handshake that ended it",
        "by_phase": by_phase.into_iter().map(|(phase, (completed, failed, seconds))| {
            let summary = measure::summarize(
                seconds.iter().map(|s| s * 1000.0).collect(),
                measure::MILLISECONDS,
                "client monotonic",
            );
            json!({"phase": phase, "completed": completed, "failed_attempts": failed,
                   "time_to_reconnect_milliseconds": summary})
        }).collect::<Vec<_>>(),
        "total_completed": collected.reconnects.iter().filter(|r| r.completed).count(),
        "total_failed_attempts": collected.reconnects.iter().filter(|r| !r.completed).count(),
    })
}

/// What became of one of the mid-flight kill's indeterminate shares, from
/// the server's answer to its re-offer and whether PostgreSQL holds it.
///
/// The two answers and the database's two states make the outcomes below;
/// each is a different claim, and the two that say the server and the
/// database disagree are possible losses. A re-offer that came back
/// `duplicate-share` while the database does not hold the share used to be
/// ignored: the server believes it has a share PostgreSQL does not
/// (EP-OBSERVABILITY).
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReofferOutcome {
    /// The re-offer was accepted and PostgreSQL holds the share: the kill
    /// destroyed the original before the server committed it, and the
    /// re-offer got it in.
    ReofferAcceptedAndCommitted,
    /// The re-offer was accepted and PostgreSQL does not hold the share: an
    /// acknowledged share with no row, a possible loss.
    ReofferAcceptedNotInPostgres,
    /// The server called the re-offer a duplicate and PostgreSQL holds the
    /// share: the original had committed before the kill, and only its
    /// acknowledgement was lost.
    CommittedBeforeKill,
    /// The server called the re-offer a duplicate and PostgreSQL does not
    /// hold the share: the server believes it has a share the database does
    /// not, a possible loss.
    DuplicateNotInPostgres,
    /// The re-offer was refused for some other reason; `in_postgres` says
    /// whether the original landed anyway.
    ReofferRejected,
    /// The re-offer got no answer, or was never sent.
    ReofferUnanswered,
}

impl ReofferOutcome {
    /// Whether the outcome is one where the server and the database
    /// disagree about the share.
    pub fn is_possible_loss(self) -> bool {
        matches!(
            self,
            Self::ReofferAcceptedNotInPostgres | Self::DuplicateNotInPostgres
        )
    }
}

/// Classify one indeterminate share from its re-offer's answer, if any, and
/// whether PostgreSQL holds it.
pub fn reoffer_outcome(reoffer: Option<&Outcome>, in_postgres: bool) -> ReofferOutcome {
    match (reoffer, in_postgres) {
        (Some(Outcome::Accepted), true) => ReofferOutcome::ReofferAcceptedAndCommitted,
        (Some(Outcome::Accepted), false) => ReofferOutcome::ReofferAcceptedNotInPostgres,
        (Some(Outcome::Rejected(rejection)), true) if classify::is_duplicate_share(rejection) => {
            ReofferOutcome::CommittedBeforeKill
        }
        (Some(Outcome::Rejected(rejection)), false) if classify::is_duplicate_share(rejection) => {
            ReofferOutcome::DuplicateNotInPostgres
        }
        (Some(Outcome::Rejected(_)), _) => ReofferOutcome::ReofferRejected,
        (Some(Outcome::NoResponse { .. }), _) | (None, _) => ReofferOutcome::ReofferUnanswered,
    }
}

/// The mid-flight kill's census: every indeterminate share with its
/// re-offer's answer, whether PostgreSQL holds it and what that comes to,
/// then the outcomes counted, with the possible losses and the duplicate
/// case named on their own with their share ids.
pub fn mid_flight_census(
    indeterminate: &[SubmitRecord],
    submits: &[SubmitRecord],
    committed: &BTreeSet<String>,
) -> Value {
    let mut outcomes: BTreeMap<ReofferOutcome, u64> = BTreeMap::new();
    let mut possible_losses: Vec<String> = Vec::new();
    let mut duplicates_not_in_postgres: Vec<String> = Vec::new();
    let shares: Vec<Value> = indeterminate
        .iter()
        .map(|record| {
            let reoffer = submits
                .iter()
                .find(|other| other.reoffer && other.share_id == record.share_id);
            let in_postgres = committed.contains(&record.share_id);
            let outcome = reoffer_outcome(reoffer.map(|other| &other.outcome), in_postgres);
            *outcomes.entry(outcome).or_insert(0) += 1;
            if outcome.is_possible_loss() {
                possible_losses.push(record.share_id.clone());
            }
            if outcome == ReofferOutcome::DuplicateNotInPostgres {
                duplicates_not_in_postgres.push(record.share_id.clone());
            }
            json!({
                "share_id": record.share_id,
                "session": record.session,
                "frontend": record.frontend,
                "job_id": record.job_id,
                "classification": "indeterminate",
                "reoffer_answer": reoffer.map(|other| describe_outcome(&other.outcome)),
                "in_postgres": in_postgres,
                "outcome": outcome,
            })
        })
        .collect();
    json!({
        "indeterminate_shares": shares.len(),
        "shares": shares,
        "outcomes": outcomes.iter()
            .map(|(outcome, count)| json!({"outcome": outcome, "count": count}))
            .collect::<Vec<_>>(),
        "possible_losses": {
            "count": possible_losses.len(),
            "share_ids": possible_losses,
            "definition": "an indeterminate share whose re-offer the server accepted while \
                           PostgreSQL does not hold it, or called a duplicate while PostgreSQL \
                           does not hold it: the server and the database disagree about the \
                           share either way",
        },
        "duplicate_not_in_postgres": {
            "count": duplicates_not_in_postgres.len(),
            "share_ids": duplicates_not_in_postgres,
            "note": "the server answered duplicate-share to a re-offer of a share PostgreSQL \
                     does not hold, so it believes it has a share the database does not. This \
                     is a possible loss and is reported as its own outcome; it does not change \
                     the exit code, because the mid-flight kill is a deliberate side scenario in \
                     which indeterminate shares are legitimate, but it is never dropped.",
        },
    })
}

fn mid_flight_report(
    runs: &[PhaseRun],
    collected: &Collected,
    committed: &BTreeSet<String>,
) -> Value {
    let Some(phase) = runs.iter().find(|phase| phase.plan.mid_flight_kill) else {
        return json!({"ran": false});
    };
    let mut report = mid_flight_census(
        &phase.mid_flight_indeterminate,
        &collected.submits,
        committed,
    );
    if let Some(object) = report.as_object_mut() {
        object.insert("ran".into(), json!(true));
        object.insert("phase".into(), json!(phase.plan.name));
        object.insert(
            "submits_outstanding_at_kill".into(),
            json!(phase.outstanding_at_kill),
        );
        object.insert(
            "note".into(),
            json!(
                "an indeterminate share is one whose acknowledgement the kill destroyed; it is \
                 re-offered with exactly the header it carried, and its final PostgreSQL outcome \
                 is reported under outcome. Zero outstanding at the kill means the scenario did \
                 not exercise."
            ),
        );
    }
    report
}

/// When each session first received work on each of `tips`, measured from
/// the node's tip stamp, while the tip was still the tip.
///
/// `all_changes` is every tip change the node recorded, of any origin; a
/// tip's reign ends at the first change after it. A notify for the tip that
/// arrives after that is a late job for a replaced tip: real, but not usable
/// work on this tip, and counting it credited the tip with a session whose
/// work really arrived under the next one, at a time that belonged to the
/// next one's reign -- the same borrowing the dense section's per-frontend
/// search had (EP-STATE). A session with no work inside the reign is not
/// counted, and the entry says how many sessions that was.
pub fn time_to_usable_work(
    tips: &[crate::node::TipChange],
    all_changes: &[crate::node::TipChange],
    collected: &Collected,
    sessions: usize,
) -> Value {
    let entries: Vec<Value> = tips
        .iter()
        .map(|tip| {
            let replaced_at = all_changes
                .iter()
                .filter(|change| change.monotonic > tip.monotonic)
                .map(|change| change.monotonic)
                .min();
            let mut first: HashMap<usize, Instant> = HashMap::new();
            for sighting in &collected.tips {
                if sighting.tip != tip.hash || sighting.at < tip.monotonic {
                    continue;
                }
                if replaced_at.is_some_and(|end| sighting.at >= end) {
                    continue;
                }
                let slot = first.entry(sighting.session).or_insert(sighting.at);
                *slot = (*slot).min(sighting.at);
            }
            let deltas: Vec<f64> = first
                .values()
                .map(|at| at.saturating_duration_since(tip.monotonic).as_secs_f64() * 1000.0)
                .collect();
            let all_seen = first
                .values()
                .max()
                .map(|at| at.saturating_duration_since(tip.monotonic).as_secs_f64() * 1000.0);
            json!({
                "tip": tip.hash,
                "height": tip.height,
                "origin": tip.origin,
                "minted_at": tip.wall.to_rfc3339(),
                "replaced_after_milliseconds": replaced_at.map(|end| {
                    end.saturating_duration_since(tip.monotonic).as_secs_f64() * 1000.0
                }),
                "sessions_with_work": first.len(),
                "sessions_without_work_before_replacement": sessions.saturating_sub(first.len()),
                "sessions_total": sessions,
                "latency_milliseconds": measure::summarize(
                    deltas,
                    measure::MILLISECONDS,
                    "client monotonic against the node's tip stamp",
                ),
                "all_sessions_milliseconds": all_seen,
            })
        })
        .collect();
    json!({
        "definition": "t1 - t0, where t0 is the fake node's tip stamp and t1 is the first \
                       mining.notify whose prevhash resolves to that tip and arrives before the \
                       node's next tip change (replaced_after_milliseconds, null while the tip \
                       was never replaced). A notify for the tip after it was replaced is a \
                       late job for a replaced tip and is not counted, so sessions_with_work \
                       is the number of sessions that got usable work while the tip was the \
                       tip, and all_sessions_milliseconds is null unless every session did.",
        "tips": entries,
    })
}

/// The `dense_cadence` section of the side report.
///
/// Additive: nothing else in the report or the artifact changes shape, and a
/// run without `--cadence dense` says so rather than emitting an empty table
/// that could be read as a measurement (EP-COMPAT, EP-OBSERVABILITY).
#[allow(clippy::too_many_arguments)]
fn dense_cadence_report(
    args: &Args,
    runs: &[PhaseRun],
    collected: &Collected,
    node_submissions: &[crate::node::SubmissionRecord],
    tip_changes: &[crate::node::TipChange],
    committed: &BTreeSet<String>,
    aborted: Option<&str>,
) -> Value {
    let cadence = args.cadence().unwrap_or(cadence::Cadence::None);
    if !cadence.is_dense() {
        return json!({
            "ran": false,
            "cadence": cadence.as_str(),
            "reason": "the run did not ask for --cadence dense",
        });
    }
    let Some((phase, dense)) = runs
        .iter()
        .find(|phase| phase.plan.dense_cadence)
        .and_then(|phase| phase.dense.as_ref().map(|dense| (phase, dense)))
    else {
        return json!({
            "ran": false,
            "cadence": cadence.as_str(),
            "landings": 0,
            "bumps": 0,
            "reason": "the run ended before the dense_cadence phase could run, so there is no \
                       schedule, no landing and no window to report",
            "aborted": aborted,
        });
    };
    let mut document = cadence::build(&cadence::ReportInputs {
        cadence,
        gaps: &dense.gaps,
        offsets: &dense.offsets,
        phase_seconds: phase.plan.seconds,
        phase_rate: phase.plan.rate,
        phase_started: phase.started,
        phase_started_wall: phase.started_wall,
        phase_ended: phase.ended,
        phase_duration_millis: phase.duration_millis,
        landing_budget: args.scheduled_blocks,
        slots_over_budget: dense.slots_over_budget,
        landings: &dense.landings,
        revisions: Some(&dense.revisions),
        submits: &collected.submits,
        notifies: &collected.notifies,
        tips: &collected.tips,
        node_submissions,
        tip_changes,
        session_frontend: &dense.session_frontend,
        frontends: &dense.frontend_health,
        failures: &collected.failures,
        committed,
        aborted,
    });
    // The run-level view of the same measurement, over every session at once,
    // through the section the whole run already uses for tips of any origin.
    let pool_tips: Vec<crate::node::TipChange> = tip_changes
        .iter()
        .filter(|change| {
            change.origin == crate::node::TipOrigin::Pool
                && change.monotonic >= phase.started
                && change.monotonic <= phase.ended
        })
        .cloned()
        .collect();
    document["time_to_new_tip_work_all_sessions"] = time_to_usable_work(
        &pool_tips,
        tip_changes,
        collected,
        dense.session_frontend.len(),
    );
    document
}

/// Failure counts by kind, for the side report.
pub fn failures_by_kind(failures: &[ClientFailure]) -> Value {
    let mut by_kind: BTreeMap<FailureKind, u64> = BTreeMap::new();
    for failure in failures {
        *by_kind.entry(failure.kind).or_insert(0) += 1;
    }
    json!(by_kind
        .into_iter()
        .map(|(kind, count)| json!({"kind": kind, "count": count}))
        .collect::<Vec<_>>())
}

/// What a phase's `dispatched` count came to, and whether the failures
/// explain the difference.
///
/// `dispatched` is what the scheduler placed. Every placed offer ends as a
/// submit record, a discarded offer (the session was paused or stopped
/// before it sent), or an offer that failed before a submit line was
/// written; a write failure is a submit record already. The failures were
/// collected and never read, so `dispatched` and the submits recorded could
/// disagree with no account of why; now the account is beside the numbers,
/// and what it does not explain is `unaccounted` rather than silent
/// (EP-OBSERVABILITY).
pub fn offer_accounting(phase: &str, dispatched: u64, collected: &Collected) -> Value {
    let submits_recorded = collected
        .submits
        .iter()
        .filter(|record| record.phase == phase && !record.reoffer && !record.scheduled_block)
        .count() as u64;
    let discarded = collected
        .discarded_offers_by_phase
        .get(phase)
        .copied()
        .unwrap_or(0);
    let in_phase = || collected.failures.iter().filter(|f| f.phase == phase);
    let failed_before_a_submit = in_phase()
        .filter(|f| f.kind == FailureKind::Offer && !f.recorded)
        .count() as u64;
    let failed_at_the_write = in_phase()
        .filter(|f| f.kind == FailureKind::Offer && f.recorded)
        .count() as u64;
    let failures: Vec<&ClientFailure> = in_phase().collect();
    let unaccounted = dispatched as i64
        - submits_recorded as i64
        - discarded as i64
        - failed_before_a_submit as i64;
    json!({
        "dispatched": dispatched,
        "submits_recorded": submits_recorded,
        "offers_discarded": discarded,
        "offers_failed_before_a_submit": failed_before_a_submit,
        "offers_failed_at_the_write": failed_at_the_write,
        "unaccounted": unaccounted,
        "client_failures": failures.len(),
        "client_failures_by_kind": failures_by_kind(&failures.iter().map(|f| (*f).clone()).collect::<Vec<_>>()),
        "client_failures_sample": failures.iter().take(20).map(|f| json!({
            "session": f.session, "kind": f.kind, "recorded_as_submit": f.recorded, "error": f.error,
        })).collect::<Vec<_>>(),
        "note": "dispatched is what the scheduler placed. Every placed offer ends as a submit \
                 record (accepted, rejected or no-response; re-offers and scheduled blocks are \
                 not placed by the scheduler and are not counted here), a discarded offer (the \
                 session was paused or stopped before it sent), or an offer that failed before \
                 a submit line was written (no job to mine, no solution found). An offer whose \
                 write failed is already a no-response submit record and is listed apart so it \
                 is not counted twice. unaccounted is what none of those explains; it is 0 in a \
                 run whose sessions all stopped cleanly, and anything else is reported rather \
                 than assumed away.",
    })
}

fn phase_report(
    phase: &PhaseRun,
    collected: &Collected,
    reconciliations: &[(String, digest::Reconciliation)],
    unexpected_by_phase: &HashMap<String, usize>,
) -> Value {
    let reconciliation = reconciliations
        .iter()
        .find(|(name, _)| *name == phase.plan.name)
        .map(|(_, rec)| rec);
    let latency = phase_latency(&collected.submits, &phase.plan.name);
    let seconds = phase.duration_millis as f64 / 1000.0;
    let acknowledged = reconciliation
        .map(|rec| rec.acknowledged.len())
        .unwrap_or(0);
    json!({
        "name": phase.plan.name,
        "in_artifact": phase.plan.in_artifact,
        "completed": phase.completed,
        "started_at": phase.started_wall.to_rfc3339(),
        "ended_at": phase.ended_wall.to_rfc3339(),
        "duration_seconds": seconds,
        "target_rate_shares_per_second": phase.plan.rate,
        "offered_tokens": phase.tokens,
        "dispatched": phase.dispatched,
        "shortfall": phase.shortfall,
        "offer_accounting": offer_accounting(&phase.plan.name, phase.dispatched, collected),
        "achieved_rate_shares_per_second": acknowledged as f64 / seconds.max(f64::MIN_POSITIVE),
        "offered_rate_shares_per_second": phase.dispatched as f64 / seconds.max(f64::MIN_POSITIVE),
        "client_ack_latency": latency,
        "client_ack_latency_definition": "accepted shares only, from writing the submit line to \
                                          reading the acknowledgement; a refusal is not an \
                                          acknowledgement and is summarised under \
                                          client_rejection_latency",
        "client_rejection_latency": rejection_latency(&collected.submits, &phase.plan.name),
        "server_share_ack_seconds": phase.ack_deltas,
        "order_lock": phase.locks.order,
        "settlement_lock": phase.locks.settlement,
        "processes": phase.processes,
        "database_delay_milliseconds_configured": phase.proxy_delay_configured_ms,
        "previous_phase_settled_before_delay_change_seconds": phase.delay_settled_seconds,
        "database_delay_observed_select1_median_milliseconds": phase.proxy_delay_observed_ms.as_ref().ok(),
        "database_delay_observation_error": phase.proxy_delay_observed_ms.as_ref().err(),
        "database_delay_round_trip_floor_milliseconds": delay_floor_millis(phase.proxy_delay_configured_ms),
        "min_mem_available_kib": phase.min_mem_available_kib,
        "scheduled_blocks": phase.scheduled_blocks,
        "frontend_restarts": phase.frontend_restarts,
        "drained_restarts": phase.restart_records.iter().map(|record| json!({
            "frontend": record.index,
            "drain_seconds": record.drain_seconds,
            "outage_seconds": record.outage_seconds,
            "scraped_before_kill": record.split.end_of_previous.ok,
            "scraped_after_restart": record.split.start_of_next.ok,
        })).collect::<Vec<_>>(),
        "rejected_valid_shares": rejected_valid_count(&collected.submits, &phase.plan.name),
        "harness_bug_rejections": harness_bug_count(&collected.submits, &phase.plan.name),
        "reconciliation": reconciliation.map(|rec| json!({
            "offered": rec.offered.len(),
            "acknowledged": rec.acknowledged.len(),
            "committed": rec.committed.len(),
            "missing": rec.missing.len(),
            "missing_sample": rec.missing.iter().take(10).collect::<Vec<_>>(),
            "unexpected": unexpected_by_phase.get(&phase.plan.name).copied().unwrap_or(0),
            "acknowledged_share_ids_sha256": rec.acknowledged_digest(),
            "postgres_share_ids_sha256": rec.committed_digest(),
        })),
    })
}

/// Split the gaps between what was acknowledged and what PostgreSQL holds into
/// the failures they really are.
///
/// An acknowledged share the database does not hold is a loss. A share the
/// database holds that the server refused with `ledger-confirmation-failed`
/// is not a loss: the append committed after the commit deadline had already
/// answered the miner. Neither is one whose submit got no response: the
/// socket closed before an answer was read, and whether the server ever
/// sent one cannot be known from this side. They are reported separately
/// because only the first means a miner's credited work disappeared.
///
/// Which of the failures a committed-but-unacknowledged share is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GapKind {
    /// PostgreSQL holds it, and the server had already refused it with
    /// `ledger-confirmation-failed`. Nothing was lost.
    AckCommitDivergence,
    /// PostgreSQL holds it, and the server had already answered
    /// `ledger-outcome-unknown`: it said it did not know whether the append
    /// landed. Nothing was lost, and the answer was honest, but the miner was
    /// still refused a share the database holds.
    UnknownOutcomeCommitted,
    /// PostgreSQL holds it, and the submit got no response: the socket
    /// closed, or the run ended, before an answer was read. The share is
    /// present; only its acknowledgement is missing, and the harness cannot
    /// tell whether the server sent one. Nothing was lost. It used to be
    /// read as a durability loss, the most expensive wrong answer the
    /// harness can give, because a loss is a stop-and-ask (EP-ERRORS).
    NoResponseCommitted,
    /// PostgreSQL holds it and nothing explains why no acknowledgement
    /// covers it.
    DurabilityLoss,
}

/// What the side report says a no-response commit is.
pub const NO_RESPONSE_COMMIT_DEFINITION: &str =
    "a share PostgreSQL holds whose submit got no response: the socket closed, or the run \
     ended, before the client read an answer. The share is present and only its \
     acknowledgement is missing; whether the server ever sent one cannot be known from this \
     side, so this is transport-indeterminate, neither a loss nor a refusal. It is kept apart \
     from durability_findings, whose shares are missing or unexplained, and from the two \
     refusal buckets, whose shares the server answered.";

/// Decide from the submit record the harness has, if any.
pub fn classify_committed_gap(record: Option<&SubmitRecord>) -> GapKind {
    match record.map(|record| &record.outcome) {
        Some(Outcome::Rejected(rejection)) if classify::is_confirmation_failure(rejection) => {
            GapKind::AckCommitDivergence
        }
        Some(Outcome::Rejected(rejection)) if classify::is_outcome_unknown(rejection) => {
            GapKind::UnknownOutcomeCommitted
        }
        Some(Outcome::NoResponse { .. }) => GapKind::NoResponseCommitted,
        _ => GapKind::DurabilityLoss,
    }
}

/// The reconciliation gaps of a run, each kind in its own bucket.
#[derive(Clone, Debug, Default)]
pub struct GapReport {
    /// The `durability_findings` list: the losses.
    pub findings: Value,
    /// Committed after `ledger-confirmation-failed`.
    pub divergences: Vec<Value>,
    /// Committed after `ledger-outcome-unknown`.
    pub unknown_outcome_commits: Vec<Value>,
    /// Committed with no response read.
    pub no_response_commits: Vec<Value>,
}

/// The kind a committed row that no phase offered is reported under.
pub const OUTSIDE_PHASES_KIND: &str = "committed share that no phase offered";

/// The durability finding for the run-prefixed rows PostgreSQL holds that no
/// phase claims, or nothing when there are none.
///
/// Every share the harness offered has a submit record stamped with the
/// phase that offered it, so a row outside every phase is one of two
/// things: a commit the harness never saw offered, or a persisted rejection
/// the harness classified as a race the server was entitled to lose and so
/// kept out of the offered set. Both are durability questions, and the
/// count used to be recorded under `reconciliation.unexpected_outside_phases`
/// where nothing read it. It carries the same weight as an acknowledged
/// share that went missing (EP-OBSERVABILITY).
pub fn outside_phases_finding(rows: &[String]) -> Option<Value> {
    if rows.is_empty() {
        return None;
    }
    Some(json!({
        "phase": Value::Null,
        "kind": OUTSIDE_PHASES_KIND,
        "count": rows.len(),
        "sample": rows.iter().take(20).collect::<Vec<_>>(),
        "note": "a run-prefixed row PostgreSQL holds that is in no phase's offered set: \
                 either a commit the harness never saw offered, or a persisted rejection \
                 the harness kept out of the offered set as an entitled race. Neither is \
                 explained, so it is a durability finding and the run exits 4.",
    }))
}

/// One phase the run drove, as `classify_gaps` needs it: its name, and the
/// shares its mid-flight kill made indeterminate. That set is the kill's
/// census, empty for every other phase and for a kill that found nothing
/// outstanding, and it is the only thing the kill exempts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DrivenPhase {
    pub name: String,
    pub kill_indeterminate: BTreeSet<String>,
}

/// `phases` is every phase the run drove.
pub fn classify_gaps(
    phases: &[DrivenPhase],
    reconciliations: &[(String, digest::Reconciliation)],
    attribution: &digest::UnexpectedAttribution,
    submits: &[SubmitRecord],
    share_commit_timeout_seconds: f64,
) -> GapReport {
    let mut findings = Vec::new();
    let mut divergences = Vec::new();
    let mut unknown_outcomes = Vec::new();
    let mut no_responses = Vec::new();
    let by_share: HashMap<&str, &SubmitRecord> = submits
        .iter()
        .filter(|record| !record.reoffer)
        .map(|record| (record.share_id.as_str(), record))
        .collect();
    for DrivenPhase {
        name: phase_name,
        kill_indeterminate,
    } in phases
    {
        // The mid-flight kill destroys the answers to the submits outstanding
        // on the frontend it kills, and those shares are its census:
        // re-offered, and reported under `mid_flight_kill` with the server's
        // answer and whether PostgreSQL holds each. They are all the kill
        // explains. The whole phase used to be skipped here, so a share it
        // acknowledged in the ordinary way and PostgreSQL then lost stayed in
        // `reconciliation.missing`, reached no finding, and the run could
        // exit 0: a false negative on the one thing the harness exists to
        // catch (EP-ERRORS). Now only a committed row whose submit is in the
        // census is left to the census; every other gap in the phase is what
        // it would be anywhere else.
        let Some((_, reconciliation)) = reconciliations.iter().find(|(name, _)| name == phase_name)
        else {
            continue;
        };
        if !reconciliation.missing.is_empty() {
            findings.push(json!({
                "phase": phase_name,
                "kind": "acknowledged share missing from PostgreSQL",
                "count": reconciliation.missing.len(),
                "sample": reconciliation.missing.iter().take(20).collect::<Vec<_>>(),
            }));
        }
        let Some((_, rows)) = attribution
            .by_phase
            .iter()
            .find(|(name, _)| name == phase_name)
        else {
            continue;
        };
        let mut unexplained = Vec::new();
        for share in rows {
            let record = by_share.get(share.as_str()).copied();
            let rejection = record.and_then(|record| match &record.outcome {
                Outcome::Rejected(rejection) => Some(rejection),
                _ => None,
            });
            match classify_committed_gap(record) {
                kind @ (GapKind::AckCommitDivergence | GapKind::UnknownOutcomeCommitted) => {
                    let detail = json!({
                        "share_id": share,
                        "phase": phase_name,
                        "frontend": record.map(|record| record.frontend),
                        "session": record.map(|record| record.session),
                        "job_id": record.map(|record| record.job_id.clone()),
                        "code": rejection.map(|r| r.code),
                        "reason_id": rejection.and_then(|r| r.reason_id.clone()),
                        "message": rejection.map(|r| r.message.clone()),
                        "send_to_response_milliseconds": record.and_then(|r| r.latency_millis),
                        "share_commit_timeout_milliseconds": share_commit_timeout_seconds * 1000.0,
                        "response_after_commit_deadline": record
                            .and_then(|r| r.latency_millis)
                            .map(|latency| latency >= share_commit_timeout_seconds * 1000.0),
                    });
                    if kind == GapKind::AckCommitDivergence {
                        divergences.push(detail);
                    } else {
                        unknown_outcomes.push(detail);
                    }
                }
                // The kill's own: its re-offer and its PostgreSQL outcome are
                // in the `mid_flight_kill` census, and the lost answer is the
                // scenario, not a transport accident the run answers for.
                GapKind::NoResponseCommitted if kill_indeterminate.contains(share) => {}
                GapKind::NoResponseCommitted => {
                    let reason = record.and_then(|record| match &record.outcome {
                        Outcome::NoResponse { reason } => Some(reason.clone()),
                        _ => None,
                    });
                    no_responses.push(json!({
                        "share_id": share,
                        "phase": phase_name,
                        "frontend": record.map(|record| record.frontend),
                        "session": record.map(|record| record.session),
                        "job_id": record.map(|record| record.job_id.clone()),
                        "no_response_reason": reason.clone(),
                        "classification": "transport-indeterminate",
                        "window_ended": reason.as_deref() == Some(client::RUN_ENDED),
                    }));
                }
                GapKind::DurabilityLoss => unexplained.push(share.clone()),
            }
        }
        if !unexplained.is_empty() {
            findings.push(json!({
                "phase": phase_name,
                "kind": "committed share that was never acknowledged",
                "count": unexplained.len(),
                "sample": unexplained.iter().take(20).collect::<Vec<_>>(),
            }));
        }
    }
    // The rows no phase claims are not any phase's, so no kill's census can
    // cover them: they are findings in every run.
    findings.extend(outside_phases_finding(&attribution.outside_phases));
    GapReport {
        findings: json!(findings),
        divergences,
        unknown_outcome_commits: unknown_outcomes,
        no_response_commits: no_responses,
    }
}

/// How a run ends before its first phase, with only the reduced side report
/// to show for it.
enum EarlyExit {
    /// No frontend served work, or a log showed a hard refusal.
    Blocked(String),
    /// A frontend advertised a share difficulty other than the configured
    /// one, or the observed replication mode is not the declared one, so
    /// nothing that would have been measured could be evidence.
    PremiseContradicted {
        reason: String,
        difficulty_mismatches: Vec<(usize, f64, f64)>,
    },
}

async fn finish_early(
    args: &Args,
    ctx: &RunContext,
    mut frontends: Vec<Frontend>,
    blocked: Vec<BlockedLog>,
    replication: &ReplicationPremise,
    exit: EarlyExit,
) -> Result<i32> {
    let report_path = args.out.join("load-harness-report.json");
    let (blocked_error, premise, line, code) = match &exit {
        EarlyExit::Blocked(error) => (
            Some(error.as_str()),
            premise_block(None, &[], replication),
            format!("run blocked: {error}"),
            EXIT_BLOCKED,
        ),
        EarlyExit::PremiseContradicted {
            reason,
            difficulty_mismatches,
        } => (
            None,
            premise_block(Some(reason), difficulty_mismatches, replication),
            format!("premise contradicted, artifact withheld: {reason}"),
            EXIT_PREMISE_CONTRADICTED,
        ),
    };
    let document = json!({
        "schema": report::SCHEMA,
        "run_id": ctx.run_id.to_string(),
        "blocked": {
            "blocked": blocked_error.is_some(),
            "error": blocked_error,
            "log_matches": blocked,
            "note": BLOCKED_NOTE,
        },
        "premise": premise,
        "validator": {
            "artifact_written": false,
            "withheld_reason": line,
        },
        "topology": {
            "frontends": args.frontends,
            "sessions": args.sessions,
            "window_shares": args.window_shares,
        },
        "frontend_environment": frontends.iter().map(|child| json!({
            "instance_id": child.spec.instance_id,
            "stderr_log": child.stderr_path.display().to_string(),
            "environment": frontend::redacted(&child.environment),
        })).collect::<Vec<_>>(),
        "host": measure::host_facts(),
        "stale_outputs_removed": ctx.stale_outputs_removed,
    });
    report::write_json(&report_path, &document)?;
    eprintln!("{line}");
    eprintln!("side report: {}", report_path.display());
    for child in frontends.iter_mut() {
        child.kill();
    }
    Ok(code)
}

/// The side report's `validator` block: the verdict and the reproducing
/// command when an artifact was written, and the reason when it was not.
/// One rejection reason the classifier does not recognise, as the run saw
/// it, with how many rejections carried it.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct UnrecognisedReason {
    pub reason_id: String,
    pub code: i64,
    pub message: String,
    pub count: u64,
}

/// Every `(reason_id, code, message)` the classifier answered `Unknown` for,
/// over every submit of the run including the mid-flight re-offers: an
/// unrecognised reason on a re-offer says the same thing about the harness's
/// model of the server as one on a first offer.
pub fn unrecognised_rejection_reasons(records: &[SubmitRecord]) -> Vec<UnrecognisedReason> {
    let mut by_key: BTreeMap<(String, i64, String), u64> = BTreeMap::new();
    for rejection in records.iter().filter_map(rejection_of) {
        if classify::classify(rejection) == RejectionClass::Unknown {
            *by_key
                .entry((
                    rejection.reason_id.clone().unwrap_or_default(),
                    rejection.code,
                    rejection.message.clone(),
                ))
                .or_insert(0) += 1;
        }
    }
    by_key
        .into_iter()
        .map(|((reason_id, code, message), count)| UnrecognisedReason {
            reason_id,
            code,
            message,
            count,
        })
        .collect()
}

/// The one line that says what the unrecognised reasons mean, or nothing
/// when there are none.
pub fn unrecognised_reasons_line(reasons: &[UnrecognisedReason]) -> Option<String> {
    if reasons.is_empty() {
        return None;
    }
    let total: u64 = reasons.iter().map(|reason| reason.count).sum();
    let named: Vec<String> = reasons
        .iter()
        .map(|reason| {
            format!(
                "reason_id {:?} code {} message {:?} x{}",
                reason.reason_id, reason.code, reason.message, reason.count
            )
        })
        .collect();
    Some(format!(
        "{total} rejection(s) carried a reason the harness does not recognise ({}): the \
         harness's model of the server's rejections is out of date, which is the reader's \
         problem to solve; they are counted in rejected_valid_shares and the exit code is \
         unchanged",
        named.join("; ")
    ))
}

/// Put the unrecognised reasons into the artifact's refusal reason, when the
/// artifact was written and refused. A valid artifact is left valid: its
/// unrecognised reasons were in a side phase, and the summary names them on
/// their own line either way.
pub fn name_unrecognised_reasons(
    evidence: &mut artifact::Evidence,
    reasons: &[UnrecognisedReason],
) {
    let Some(line) = unrecognised_reasons_line(reasons) else {
        return;
    };
    if let artifact::Evidence::Written { verdict, .. } = evidence {
        if !verdict.valid {
            verdict.error_chain.push(line);
        }
    }
}

fn validator_block(
    evidence: &artifact::Evidence,
    args: &Args,
    slowest_rate: f64,
    worst_p99: Option<f64>,
    unrecognised: &[UnrecognisedReason],
) -> Value {
    let mut block = match evidence {
        artifact::Evidence::Written {
            path,
            verdict,
            command,
            ..
        } => json!({
            "artifact_written": true,
            "verdict": verdict,
            "command": command,
            "artifact_path": path.display().to_string(),
        }),
        artifact::Evidence::Withheld {
            reason,
            stale_artifact_removed,
        } => json!({
            "artifact_written": false,
            "withheld_reason": reason,
            "stale_artifact_removed": stale_artifact_removed,
            "verdict": Value::Null,
            "command": Value::Null,
            "artifact_path": Value::Null,
        }),
    };
    let suggestions = json!({
        "unrecognised_rejection_reasons": {
            "count": unrecognised.iter().map(|reason| reason.count).sum::<u64>(),
            "reasons": unrecognised,
            "note": unrecognised_reasons_line(unrecognised),
        },
        "forecast_used": args.forecast_peak_shares_per_second,
        "slowest_artifact_phase_rate_shares_per_second": slowest_rate,
        "suggested_forecast_for_a_valid_artifact": (slowest_rate / 2.0).max(0.0),
        "ack_p99_limit_used_milliseconds": args.ack_p99_limit_ms,
        "worst_artifact_phase_ack_p99_milliseconds": worst_p99,
        "suggested_ack_p99_limit_milliseconds": worst_p99.map(|p99| {
            // Round up to the next 100 ms, and never above the commit
            // timeout the consumer checks against.
            ((p99 / 100.0).ceil() * 100.0).min(args.share_commit_timeout_seconds * 1000.0)
        }),
    });
    if let (Some(block), Some(suggestions)) = (block.as_object_mut(), suggestions.as_object()) {
        for (key, value) in suggestions {
            block.insert(key.clone(), value.clone());
        }
    }
    block
}

/// The text the run prints once the side report is written.
pub fn summary_text(
    report: &Value,
    evidence: &artifact::Evidence,
    unrecognised: &[UnrecognisedReason],
) -> String {
    let mut text = String::new();
    text.push_str("=== qbit-prism-load ===\n");
    for phase in report["phases"].as_array().into_iter().flatten() {
        text.push_str(&format!(
            "phase {:<16} {:>8.1}s target={:<8} offered={:<8} acked={:<8} rate={:.1}/s \
             ack p50={:?} p99={:?} order_waiters_max={} settlement_waiters_max={} \
             shortfall={}\n",
            phase["name"].as_str().unwrap_or_default(),
            phase["duration_seconds"].as_f64().unwrap_or_default(),
            phase["target_rate_shares_per_second"]
                .as_f64()
                .unwrap_or_default(),
            phase["dispatched"].as_u64().unwrap_or_default(),
            phase["reconciliation"]["acknowledged"]
                .as_u64()
                .unwrap_or_default(),
            phase["achieved_rate_shares_per_second"]
                .as_f64()
                .unwrap_or_default(),
            phase["client_ack_latency"]["p50"].as_f64(),
            phase["client_ack_latency"]["p99"].as_f64(),
            phase["order_lock"]["max_waiters"]
                .as_u64()
                .unwrap_or_default(),
            phase["settlement_lock"]["max_waiters"]
                .as_u64()
                .unwrap_or_default(),
            phase["shortfall"].as_u64().unwrap_or_default(),
        ));
        // The offer accounting is presented on this line as offered=; when
        // something it placed did not become a submit, the line says so.
        let accounting = &phase["offer_accounting"];
        let failures = accounting["client_failures"].as_u64().unwrap_or_default();
        let unaccounted = accounting["unaccounted"].as_i64().unwrap_or_default();
        if failures > 0 || unaccounted != 0 {
            text.push_str(&format!(
                "  offer accounting for {}: submits_recorded={} offers_discarded={} \
                 offers_failed_before_a_submit={} client_failures={} unaccounted={}\n",
                phase["name"].as_str().unwrap_or_default(),
                accounting["submits_recorded"].as_u64().unwrap_or_default(),
                accounting["offers_discarded"].as_u64().unwrap_or_default(),
                accounting["offers_failed_before_a_submit"]
                    .as_u64()
                    .unwrap_or_default(),
                failures,
                unaccounted,
            ));
        }
    }
    match evidence {
        artifact::Evidence::Written {
            verdict, command, ..
        } => {
            text.push_str(&format!(
                "artifact verdict: {}\n",
                if verdict.valid {
                    verdict.summary.clone().unwrap_or_default()
                } else {
                    format!("INVALID: {}", verdict.error_chain.join(": "))
                }
            ));
            text.push_str("validate with:\n");
            text.push_str(command);
            text.push('\n');
        }
        artifact::Evidence::Withheld { reason, .. } => {
            text.push_str(&format!("artifact withheld: {reason}\n"));
        }
    }
    if let Some(line) = unrecognised_reasons_line(unrecognised) {
        text.push_str(&format!("unrecognised rejection reasons: {line}\n"));
    }
    // The mid-flight kill's possible losses are visible here as well as in
    // the JSON: they do not change the exit code, so nothing else says so.
    let possible = &report["mid_flight_kill"]["possible_losses"];
    if possible["count"].as_u64().unwrap_or_default() > 0 {
        let duplicates = &report["mid_flight_kill"]["duplicate_not_in_postgres"];
        text.push_str(&format!(
            "mid-flight kill: {} re-offered share(s) the server and PostgreSQL disagree about \
             ({} answered duplicate-share while not in PostgreSQL): {}\n",
            possible["count"],
            duplicates["count"],
            possible["share_ids"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    text
}
