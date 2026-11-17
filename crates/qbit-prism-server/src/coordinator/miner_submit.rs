//! Miner credit decisions, independently fenced from current-work candidates.
//!
//! A share's acknowledgement never outruns its ledger outcome. The append runs
//! in its own task behind a [`CommitGate`]: at `share_commit_timeout` the
//! acknowledgement either closes the gate, so COMMIT is never sent, or finds
//! COMMIT already in flight and waits `share_commit_grace` for its reply. An
//! outcome still unknown at the deadline is answered `ledger-outcome-unknown`,
//! never as a failure. An append carrying a found block follows the same
//! deadline and grace but is never refused, so it runs on past an unknown
//! answer. Block-only proofs wait for their candidate's disposition up to
//! `share_commit_timeout` itself (#574).
//! A block-bearing submission still pending at its bound is answered
//! `ledger-outcome-unknown` and counted in `block_proof_ack_capped_total`; the
//! bound only ends the wait, never the probe, enqueue, append or landing.
//!
//! A found block's append is the only path to `submitblock`, so one that
//! fails before COMMIT for a passing database condition (a lock wait past
//! `lock_timeout`, a pool timeout, a connection lost before COMMIT) is run
//! again inside
//! its own task, a bounded number of times, while the job's parent is still
//! the observed tip ([`FoundBlockRetry`]). Only a failure proven to precede
//! COMMIT is run again, so the earlier attempt wrote nothing, and every
//! attempt finds what is already recorded: a share-pass append probes the
//! share's ID under `ORDER_LOCK` and inserts its outbox row only if the block
//! hash has none, and a block-only enqueue checks for the row under
//! `ORDER_LOCK`. None credits the share twice or writes a second outbox row.
//! A block-only enqueue that may have sent COMMIT (its connection failed
//! during it) keeps its unknown answer and is followed, never run again.
use super::submit_ledger::{Appended, CommitGate, GateClosure, GateState};
use super::*;
use crate::ledger::{CommitGateClosed, OwnLogLost, PayoutRevisionChanged, WindowNotHeld};
use crate::metrics::{AppendStage, AppendStageOutcome, BlockAckPath, StaleJobCause};
use sqlx::postgres::{PgDatabaseError, PgSeverity};
use tokio::task::{JoinError, JoinHandle};
use tracing::instrument::WithSubscriber;

#[cfg(test)]
#[path = "miner_submit_acquire_tests.rs"]
mod acquire_tests;

/// How a persistence attempt ended, as far as its acknowledgement can tell.
#[derive(Debug)]
pub(super) enum SaveOutcome {
    Accepted,
    Duplicate,
    /// The original lease expired or was replaced before COMMIT: `stale-job`.
    Stale,
    /// The lease's commit gate could not prove its chain authority before
    /// COMMIT (#716): readiness or the tip authority lapsed, or contention for
    /// the authority's locks outlasted the acknowledgement deadline (shorter
    /// contention is waited out and the append runs again). Nothing was
    /// written, and neither a stale race nor a ledger failure is proven.
    /// Answered as admission answers
    /// readiness it cannot prove: `backend-rpc-unavailable`, or
    /// `backend-database-unavailable` when the refresh failed on the ledger
    /// database.
    AuthorityUnavailable,
    /// The ledger did not record the share: `ledger-confirmation-failed`.
    Failed(anyhow::Error),
    /// A block-only proof whose block is stale by the pool's own decision,
    /// not by a ledger failure: its candidate was abandoned before its offer
    /// (a parent change, the #478 overpay ceiling, or capture turned off), or
    /// it is block-only work, whose share is never credited. Also a
    /// share-pass append the payout-revision fence refused before any write
    /// (#675). `stale-job`, with the cause recorded once.
    Superseded(StaleJobCause),
    /// Not known by the acknowledgement deadline; the share may still be
    /// credited.
    Unknown {
        phase: &'static str,
        detail: String,
    },
}

/// How a block-only proof's spawned duplicate probe and candidate enqueue
/// ended. The task runs to completion whatever happens to the submission
/// (#574), so an acknowledgement bound or a cancelled session never stops a
/// found block short of the outbox.
enum EnqueueStep {
    /// The share is already in the ledger; nothing was enqueued.
    Recorded,
    /// The probe failed before anything was written: a definite failure.
    ProbeFailed(anyhow::Error),
    /// The enqueue's own result: `true` enqueued, `false` already enqueued.
    Enqueue(Result<bool>),
}

/// Leave a probe and candidate enqueue that outlived the acknowledgement
/// bound running, and log its outcome once it resolves. Cancelling it could
/// strand a COMMIT that was already sent, or leave the block unenqueued.
fn follow_enqueue(handle: JoinHandle<EnqueueStep>, share_id: String, block_hash: String) {
    tokio::spawn(async move {
        let (outcome, error) = match handle.await {
            Ok(EnqueueStep::Enqueue(Ok(true))) => ("enqueued", None),
            Ok(EnqueueStep::Enqueue(Ok(false))) => ("already-enqueued", None),
            Ok(EnqueueStep::Recorded) => ("already-recorded", None),
            Ok(EnqueueStep::ProbeFailed(error)) => ("probe-error", Some(format!("{error:#}"))),
            Ok(EnqueueStep::Enqueue(Err(error))) => ("error", Some(format!("{error:#}"))),
            Err(error) => ("task-ended", Some(error.to_string())),
        };
        tracing::warn!(
            share_id,
            block_hash,
            path = "block-only",
            phase = "enqueue-pending",
            outcome,
            error,
            "unknown share outcome resolved"
        );
    });
}

/// Before `Committing`, COMMIT was never sent, so every error is definite.
/// After it, only a severity-ERROR reply to COMMIT proves a rollback; any
/// other failure may follow a durable commit. A success whose COMMIT took the
/// whole `statement_timeout` may be a cancelled synchronous-replication wait
/// that committed only locally.
pub(super) fn classify_share_append(
    joined: std::result::Result<Result<bool>, JoinError>,
    state: GateState,
    commit_elapsed: Option<Duration>,
    statement_timeout: Option<Duration>,
) -> SaveOutcome {
    if state != GateState::Committing {
        return match joined {
            Ok(Ok(true)) => SaveOutcome::Accepted,
            Ok(Ok(false)) => SaveOutcome::Duplicate,
            Ok(Err(error)) => SaveOutcome::Failed(error),
            Err(error) => SaveOutcome::Failed(error.into()),
        };
    }
    match joined {
        Ok(Ok(true)) => {
            sync_rep_unknown(commit_elapsed, statement_timeout).unwrap_or(SaveOutcome::Accepted)
        }
        Ok(Ok(false)) => SaveOutcome::Duplicate,
        Ok(Err(error)) if database_error_severity(&error) == Some(PgSeverity::Error) => {
            SaveOutcome::Failed(error)
        }
        Ok(Err(error)) => SaveOutcome::Unknown {
            phase: "commit-error",
            detail: format!("{error:#}"),
        },
        Err(error) => SaveOutcome::Unknown {
            phase: "commit-error",
            detail: error.to_string(),
        },
    }
}

/// A successful COMMIT that took the whole `statement_timeout` may be a
/// cancelled synchronous-replication wait that committed only locally, so it
/// is answered unknown rather than as a confirmation.
fn sync_rep_unknown(
    commit_elapsed: Option<Duration>,
    statement_timeout: Option<Duration>,
) -> Option<SaveOutcome> {
    match statement_timeout {
        Some(limit) if commit_elapsed.is_none_or(|elapsed| elapsed >= limit) => {
            Some(SaveOutcome::Unknown {
                phase: "sync-rep-guard",
                detail: format!(
                    "possible sync-rep cancellation: COMMIT took {commit_elapsed:?}, at least statement_timeout {limit:?}"
                ),
            })
        }
        _ => None,
    }
}

/// The outcome of a share whose commit gate closed before COMMIT, decided by
/// the closure that won the gate; `error` is the refusal the append reported.
/// - A proven stale lease is a stale race.
/// - #716: a lease fence that could not prove its chain authority (readiness
///   or the tip authority lapsed, at the hook or after it waited out a tip
///   poll, a fee or readiness write, or the publication holding or awaiting
///   one of the authority's locks) proves neither stale work nor a ledger
///   failure. Nor does a deadline that closed the gate while the append was
///   still waiting out that contention. Both are answered as admission
///   answers readiness it cannot prove, not as `ledger-confirmation-failed`,
///   which feeds the share-append failure warning.
/// - A deadline closure, or a refusal that recorded none, stays strict.
fn closed_gate_outcome(closure: Option<GateClosure>, error: anyhow::Error) -> SaveOutcome {
    if closure != Some(GateClosure::StaleAuthority) {
        tracing::info!(
            %error,
            closure = closure.map_or("unspecified", GateClosure::label),
            "share commit gate refused before COMMIT"
        );
    }
    match closure {
        Some(GateClosure::StaleAuthority) => SaveOutcome::Stale,
        Some(GateClosure::AuthorityUnavailable | GateClosure::AuthorityContended) => {
            SaveOutcome::AuthorityUnavailable
        }
        Some(GateClosure::DeadlineOrCancelled) | None => SaveOutcome::Failed(error),
    }
}

/// A refusal at the append's pre-COMMIT hook: COMMIT was never sent and the
/// attempt rolled back, so nothing was written.
fn refused_at_hook(error: &anyhow::Error) -> bool {
    error.downcast_ref::<CommitGateClosed>().is_some()
}

fn sqlx_error(error: &anyhow::Error) -> Option<&sqlx::Error> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
}

fn database_error_severity(error: &anyhow::Error) -> Option<PgSeverity> {
    match sqlx_error(error)? {
        sqlx::Error::Database(error) => error
            .try_downcast_ref::<PgDatabaseError>()
            .map(|error| error.severity()),
        _ => None,
    }
}

/// Whether a failed candidate enqueue provably committed nothing: the pool
/// never handed out a connection, the server answered with a severity-ERROR
/// response, or no database I/O failed at all.
pub(super) fn enqueue_failed_before_commit(error: &anyhow::Error) -> bool {
    match sqlx_error(error) {
        None | Some(sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed) => true,
        Some(_) => database_error_severity(error) == Some(PgSeverity::Error),
    }
}

/// Whether a found block's append that failed before COMMIT met a passing
/// condition of the database or of its connection, which the same append may
/// clear by running again: the pool timed out (`PoolTimedOut`), the
/// connection or its TLS session failed (`Io`, `Tls`), or the server answered
/// with a connection exception (class 08), insufficient resources (class
/// 53), a serialization failure or deadlock (40001, 40P01), a lock wait past
/// `lock_timeout` (55P03, `ORDER_LOCK` included), a server shutting down,
/// crashing or starting (57P01, 57P02, 57P03; `pg_terminate_backend` sends
/// 57P01 too, and cannot be told from a shutdown), or an idle session's or
/// idle transaction's timeout (57P05, 25P03). The caller proves that COMMIT
/// was never sent; this only says whether trying again can help.
///
/// Nothing else is run again. The deliberate refusals, all raised before any
/// write, are named here even though none is a database error, so that no
/// wrapping can make one look transient:
/// - [`CommitGateClosed`]: the gate refused COMMIT; a found block's gate
///   carries no lease and is never closed, so it is not met here.
/// - [`PayoutRevisionChanged`]: the revision moved with capture off; the
///   block is dropped by that setting, as the submit check drops one.
/// - [`WindowNotHeld`]: this primary does not hold the window the coinbase
///   pays (#619); the block could never land here.
/// - [`OwnLogLost`]: this node's database was restored under it (D-8, D-9),
///   and the frontend is stopping.
///
/// The rest are refusals or errors the same append would meet again, so they
/// fall outside the list above without naming them: a configuration
/// fingerprint that is not this frontend's, a halted cluster, a live legacy
/// writer, a duplicate share or a payload that differs from the recorded one,
/// a candidate identity conflict, a candidate that fails its own checks, the
/// partition refusal (23514) that the ledger already ran once more, a
/// read-only database (25006), a cancelled statement (57014: an operator's
/// `pg_cancel_backend`, or a statement that ran out `statement_timeout`,
/// which the error alone cannot tell apart), a closed pool (`PoolClosed`, a
/// shutdown), a protocol error (the connection setup's own refusal of a
/// server without `fsync` is one) and every other server error.
pub(super) fn transient_before_commit(error: &anyhow::Error) -> bool {
    let refused = error.chain().any(|cause| {
        cause.is::<CommitGateClosed>()
            || cause.is::<PayoutRevisionChanged>()
            || cause.is::<WindowNotHeld>()
            || cause.is::<OwnLogLost>()
    });
    if refused {
        return false;
    }
    match sqlx_error(error) {
        Some(sqlx::Error::PoolTimedOut | sqlx::Error::Io(_) | sqlx::Error::Tls(_)) => true,
        Some(sqlx::Error::Database(database)) => database.code().is_some_and(|code| {
            code.starts_with("08")
                || code.starts_with("53")
                || matches!(
                    code.as_ref(),
                    "40001" | "40P01" | "55P03" | "57P01" | "57P02" | "57P03" | "57P05" | "25P03"
                )
        }),
        _ => false,
    }
}

/// How many times in all a found block's append runs, the first attempt
/// included, when each fails before COMMIT for a passing reason. An attempt
/// that waits out `lock_timeout` (5 s) or the pool's 15 s acquire timeout
/// meets [`FOUND_BLOCK_APPEND_WINDOW`] first (after about 9 or 4 attempts);
/// this bound stops a failure that returns at once, a dead pooled connection
/// after a failover, from spinning, and ends it only after about 48 s of
/// pauses, so such a failover still has most of the window.
pub(super) const FOUND_BLOCK_APPEND_ATTEMPTS: u32 = 16;

/// No attempt of a found block's append starts later than this after its
/// submit checks admitted it: one 60 s block interval. A blip (a lock stall,
/// a checkpoint storm, a saturated pool, a failover) fits in it; a database
/// still failing past it is in an outage, and the block has most likely been
/// superseded, which the parent check usually sees first. An attempt already
/// running at the end keeps its own bounds (pool, lock and statement
/// timeouts), as every append's does.
pub(super) const FOUND_BLOCK_APPEND_WINDOW: Duration = Duration::from_secs(60);

/// The pause before the second attempt. Each later pause doubles, up to
/// [`FOUND_BLOCK_APPEND_BACKOFF_CAP`]: 0.25, 0.5, 1, 2, then 4 s.
pub(super) const FOUND_BLOCK_APPEND_BACKOFF: Duration = Duration::from_millis(250);
pub(super) const FOUND_BLOCK_APPEND_BACKOFF_CAP: Duration = Duration::from_secs(4);

/// What a found block's candidate is built from: the job's immutable issued
/// inputs and the submission. A retried append builds its candidate again
/// from here, on a blocking worker, since the attempt before it consumed its
/// own; the first attempt pays nothing for a copy it may never need.
#[derive(Clone)]
pub(super) struct CandidateSource {
    context: Arc<JobContext>,
    job_id: String,
    /// The job's parent: the block matters only while it is the observed tip.
    parent: String,
    extranonce1: String,
    extranonce2_size: usize,
    submission: Arc<codec::Submission>,
    deferred_share: Option<AcceptedShare>,
}

/// Why a found block's append is not run again.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetryEnd {
    /// A deliberate refusal, or an error the same append would meet again.
    NotTransient,
    /// The frontend is shutting down: its shutdown was signalled, or its
    /// ledger pool closed, the last step of every shutdown.
    Shutdown,
    /// This node's own log was lost (D-8, D-9: a `FrontendStop` sets it
    /// before the stop) or is behind (a detected local rollback): no share of
    /// this node's may be appended.
    OwnLog,
    /// The job's parent is no longer the observed tip: the block can no
    /// longer extend the chain.
    ParentChanged,
    /// [`FOUND_BLOCK_APPEND_ATTEMPTS`] attempts have run.
    Attempts,
    /// The next attempt would start past [`FOUND_BLOCK_APPEND_WINDOW`].
    Window,
    /// The candidate could not be built again: its blocking worker failed.
    Rebuild,
}

impl RetryEnd {
    fn label(self) -> &'static str {
        match self {
            Self::NotTransient => "not-transient",
            Self::Shutdown => "shutdown",
            Self::OwnLog => "own-log",
            Self::ParentChanged => "parent-changed",
            Self::Attempts => "attempts",
            Self::Window => "window",
            Self::Rebuild => "rebuild-failed",
        }
    }
}

/// The retry of one found block's append, share-pass or block-only, after a
/// failure proven to precede COMMIT. It lives in the append's own spawned
/// task, so neither the acknowledgement deadline nor a cancelled submission
/// ends it, and it never outlives the frontend: it starts no new attempt once
/// the shutdown is signalled (`Coordinator::shutdown_signal`), the own log is
/// lost or behind, or the ledger pool has closed, and the shutdown signal and
/// the pool's close both wake its backoff. An attempt already running
/// finishes under its own database timeouts and its result stands. Without a
/// shutdown signal (tools, test fixtures) only the other endings apply.
pub(super) struct FoundBlockRetry {
    source: CandidateSource,
    ledger: Arc<Ledger>,
    observed_tip: Arc<RwLock<TipState>>,
    peer_sync: Option<watch::Receiver<crate::peer_sync::PeerSyncStatus>>,
    shutdown: Option<watch::Receiver<bool>>,
    share_id: String,
    path: &'static str,
    window_end: tokio::time::Instant,
    /// Attempts run so far, the one that just failed included.
    attempts: u32,
    backoff: Duration,
}

impl FoundBlockRetry {
    fn block_hash(&self) -> &str {
        &self.source.submission.block_hash_hex
    }

    /// After attempt `attempts` failed before COMMIT with `error`: the
    /// candidate for the next attempt, once its backoff has passed, or
    /// `None` when the append is not run again. `unused` is a candidate the
    /// failed attempt did not consume (a block-only probe that failed before
    /// its enqueue); otherwise one is built again. A retry that ends with the
    /// block unrecorded after a passing failure is logged at ERROR with the
    /// block hash: nothing will offer that block.
    async fn next_attempt(
        &mut self,
        error: &anyhow::Error,
        unused: Option<Candidate>,
    ) -> Option<Candidate> {
        // A first attempt that met a deliberate refusal or a lasting error
        // keeps its existing answer and logs; only a retry that was entered
        // logs its end.
        let entered = self.attempts > 1 || transient_before_commit(error);
        let end = match self.wait(error).await {
            Ok(()) => match unused {
                Some(candidate) => {
                    self.attempts += 1;
                    return Some(candidate);
                }
                None => match submission_candidate(&self.source).await {
                    Ok(candidate) => {
                        self.attempts += 1;
                        return Some(candidate);
                    }
                    Err(rebuild) => {
                        tracing::warn!(block_hash = self.block_hash(), error = %format_args!("{rebuild:#}"), "found block's candidate could not be built again for its next append");
                        RetryEnd::Rebuild
                    }
                },
            },
            Err(end) => end,
        };
        if entered {
            tracing::error!(
                block_hash = self.block_hash(),
                share_id = self.share_id,
                path = self.path,
                attempts = self.attempts,
                stopped = end.label(),
                error = %format_args!("{error:#}"),
                "found block not recorded: its append failed before COMMIT and is not run again, so the block is never offered"
            );
        }
        None
    }

    /// Decide whether the append runs again and wait out its backoff. A
    /// failure that is not run again is decided before the observed tip's
    /// lock is read, so its answer never waits for a publication.
    async fn wait(&mut self, error: &anyhow::Error) -> Result<(), RetryEnd> {
        self.running()?;
        if !transient_before_commit(error) {
            return Err(RetryEnd::NotTransient);
        }
        if self.attempts >= FOUND_BLOCK_APPEND_ATTEMPTS {
            return Err(RetryEnd::Attempts);
        }
        self.parent_is_tip().await?;
        let resume = tokio::time::Instant::now() + self.backoff;
        if resume > self.window_end {
            return Err(RetryEnd::Window);
        }
        tracing::warn!(
            block_hash = self.block_hash(),
            share_id = self.share_id,
            path = self.path,
            attempt = self.attempts,
            backoff_ms = self.backoff.as_millis() as u64,
            error = %format_args!("{error:#}"),
            "found block's append failed before COMMIT; appending it again"
        );
        // The shutdown signal, and the pool's close that ends every shutdown,
        // wake this wait at once. A signal whose sender is gone is a frontend
        // that has stopped.
        let signalled = async {
            match self.shutdown.clone() {
                Some(mut shutdown) => drop(shutdown.wait_for(|stop| *stop).await),
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = tokio::time::sleep_until(resume) => {}
            () = signalled => return Err(RetryEnd::Shutdown),
            () = self.ledger.pool.close_event() => return Err(RetryEnd::Shutdown),
        }
        self.backoff = (self.backoff * 2).min(FOUND_BLOCK_APPEND_BACKOFF_CAP);
        self.running()?;
        self.parent_is_tip().await
    }

    /// The frontend is not stopping: no shutdown is signalled, its ledger
    /// pool is open, and its own log is neither lost nor behind.
    fn running(&self) -> Result<(), RetryEnd> {
        let signalled = self
            .shutdown
            .as_ref()
            .is_some_and(|shutdown| *shutdown.borrow() || shutdown.has_changed().is_err());
        if signalled || self.ledger.pool.is_closed() {
            return Err(RetryEnd::Shutdown);
        }
        let own_log_behind = self
            .peer_sync
            .as_ref()
            .is_some_and(|status| !status.borrow().own_log_caught_up);
        if self.ledger.own_log_lost() || own_log_behind {
            return Err(RetryEnd::OwnLog);
        }
        Ok(())
    }

    /// The block can still extend the chain: its job's parent is the
    /// observed tip, as the submit check required when it admitted the share.
    async fn parent_is_tip(&self) -> Result<(), RetryEnd> {
        if self.observed_tip.read().await.as_deref() != Some(self.source.parent.as_str()) {
            return Err(RetryEnd::ParentChanged);
        }
        Ok(())
    }

    /// The append recorded the block, or found it recorded, after `attempts`.
    fn recorded(&self) {
        if self.attempts > 1 {
            tracing::warn!(
                block_hash = self.block_hash(),
                share_id = self.share_id,
                path = self.path,
                attempts = self.attempts,
                "found block recorded after its append was run again"
            );
        }
    }
}

/// A spawned share append. If the submission itself is cancelled first, an
/// append that may still be refused is closed and aborted, as dropping it
/// did before; one already committing, or carrying a block, runs on.
/// An append's result, how long COMMIT took, and when the append finished.
/// Both times are taken inside the task, so a late poll of this handle can
/// neither count scheduler delay as database time nor make an on-time
/// confirmation look late.
type AppendJoin = (Result<Appended>, Option<Duration>, tokio::time::Instant);

struct AppendTask {
    handle: Option<JoinHandle<AppendJoin>>,
    gate: Arc<CommitGate>,
    refusable: bool,
}

impl AppendTask {
    /// The task's result, if it finishes by `deadline`.
    async fn finish_by(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Option<std::result::Result<AppendJoin, JoinError>> {
        let handle = self.handle.as_mut()?;
        let joined = tokio::time::timeout_at(deadline, handle).await.ok()?;
        self.handle = None;
        Some(joined)
    }

    /// Cancel an append whose gate is closed. Every await before COMMIT rolls
    /// back when dropped, so the doomed append stops queueing for the pool and
    /// `ORDER_LOCK` now instead of running to the hook's refusal. The one
    /// await that could not roll back, the reply to `BEGIN`, retires its
    /// connection instead (`ledger::append_admission`, #482).
    fn abort(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }

    /// Leave an append whose outcome was answered unknown running, and log
    /// that outcome once it resolves.
    fn follow(mut self, share_id: String, path: &'static str, phase: &'static str) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        tokio::spawn(async move {
            match handle.await {
                Ok((Ok(Appended::Inserted), _, _)) => {
                    tracing::warn!(
                        share_id,
                        path,
                        phase,
                        outcome = "committed",
                        "unknown share outcome resolved"
                    )
                }
                Ok((Ok(Appended::Recorded), _, _)) => {
                    tracing::warn!(
                        share_id,
                        path,
                        phase,
                        outcome = "already-recorded",
                        "unknown share outcome resolved"
                    )
                }
                // #657: the block was captured, its share deferred; the
                // candidate loop offers it and confirmation credits the share.
                Ok((Ok(Appended::Captured), _, _)) => {
                    tracing::warn!(
                        share_id,
                        path,
                        phase,
                        outcome = "captured",
                        "unknown share outcome resolved"
                    )
                }
                Ok((Err(error), _, _)) => {
                    tracing::warn!(share_id, path, phase, outcome = "error", %error, "unknown share outcome resolved")
                }
                Err(error) => {
                    tracing::warn!(share_id, path, phase, outcome = "task-ended", %error, "unknown share outcome resolved")
                }
            }
        });
    }
}

impl Drop for AppendTask {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            if self.refusable && !handle.is_finished() && self.gate.close() {
                handle.abort();
            }
        }
    }
}

impl CandidateSource {
    pub(super) fn new(
        job: &MiningJob<JobContext>,
        submission: codec::Submission,
        deferred_share: Option<AcceptedShare>,
    ) -> Self {
        Self {
            context: Arc::clone(&job.context),
            job_id: job.wire.job_id.clone(),
            parent: job.wire.previousblockhash.clone(),
            extranonce1: job.wire.extranonce1.clone(),
            extranonce2_size: job.wire.extranonce2_size,
            submission: Arc::new(submission),
            deferred_share,
        }
    }
}

/// Construct the durable candidate from the job's immutable issued inputs.
/// Every call with the same source builds the same candidate.
pub(super) async fn submission_candidate(source: &CandidateSource) -> Result<Candidate> {
    let CandidateSource {
        context,
        job_id,
        parent: _,
        extranonce1,
        extranonce2_size,
        submission,
        deferred_share,
    } = source.clone();
    // The job context is shared; the recipient-sized copy and block
    // encoding belong on a blocking worker, before the ledger
    // transaction begins.
    tokio::task::spawn_blocking(move || {
        let original = context
            .bundle
            .coinbase_script_sig_suffix_hex
            .as_ref()
            .context("job coinbase suffix missing")?;
        let placeholder_length = (4 + extranonce2_size) * 2;
        let prefix = original
            .get(
                ..original
                    .len()
                    .checked_sub(placeholder_length)
                    .context("job coinbase suffix too short")?,
            )
            .context("invalid job suffix")?;
        let suffix = format!("{prefix}{}{}", extranonce1, submission.extranonce2_hex);
        // The slim candidate: the window reference `refresh_once`
        // already computed, the stored inputs the job was built with,
        // and the block as bytes. Nothing here walks the window, clones
        // the bundle or reads configuration. The as-issued balances,
        // O(recipients), travel beside the document so the enqueue can
        // write the snapshot the post-offer landing rebuilds from.
        let inputs = &context.prepared.inputs;
        let block_bytes = hex::decode(&submission.block_hex)?;
        anyhow::Ok(Candidate {
            block_hash: submission.block_hash_hex.clone(),
            block_sha256: Candidate::block_digest_hex(&block_bytes),
            job_id,
            payout_revision: context.prepared.snapshot.payout_revision,
            window: context.prepared.window,
            bootstrap_share: context.bootstrap_share.clone(),
            found_block: context.bundle.found_block.clone(),
            payout_policy: inputs.payout_policy.clone(),
            ctv: inputs.ctv.clone(),
            audit_builder_version: inputs.audit_builder_version,
            signer_keys: inputs.signer_keys.clone(),
            leased: false,
            coinbase_suffix_hex: suffix,
            deferred_share,
            block_bytes,
            as_issued_balances: (*context.prepared.reservation.balances).clone(),
        })
    })
    .await
    .context("candidate construction worker failed")?
}

impl Coordinator {
    /// Record the internal cause at its refusal branch. The response stays the
    /// generic `stale-job` answer that miners and the share observation see.
    fn stale_job(&self, cause: StaleJobCause) -> StratumError {
        self.metrics.record_stale_job_rejection(cause);
        protocol_error("stale-job", "stale job")
    }

    /// The retry of the found block built from `source`, whose share
    /// `share_id` was admitted at `start`; `path` labels its logs.
    fn found_block_retry(
        &self,
        source: CandidateSource,
        share_id: &str,
        path: &'static str,
        start: tokio::time::Instant,
    ) -> FoundBlockRetry {
        FoundBlockRetry {
            source,
            ledger: self.ledger.clone(),
            observed_tip: self.observed_tip.clone(),
            peer_sync: self.peer_sync.get().cloned(),
            shutdown: self.shutdown_signal.get().cloned(),
            share_id: share_id.to_owned(),
            path,
            window_end: start + FOUND_BLOCK_APPEND_WINDOW,
            attempts: 1,
            backoff: FOUND_BLOCK_APPEND_BACKOFF,
        }
    }

    pub(super) async fn submit_share(
        &self,
        _worker: &Worker,
        job: &MiningJob<JobContext>,
        submission: codec::Submission,
        stale_grace: StaleGrace,
    ) -> Result<(), StratumError> {
        // #829: the submit checks below run from here to `start`.
        let entered = tokio::time::Instant::now();
        // The proof-observation boundary of the first-offer latency: this
        // frontend's wall clock as the locally validated block proof enters
        // the coordinator, before any await. Recorded on the candidate row
        // at enqueue; a clock before the epoch leaves it unknown.
        let proof_observed_at_ms = submission.block_pass.then(|| unix_ms_now().ok()).flatten();
        let (last_poll, readiness_generation, refresh_failed_on_database) = {
            let readiness = self.readiness.read().await;
            let last_poll = readiness.last_poll.ok_or_else(|| {
                protocol_error(
                    "backend-rpc-unavailable",
                    "current chain state is unavailable",
                )
            })?;
            (
                last_poll,
                readiness.generation,
                readiness.refresh_failed_on_database,
            )
        };
        let context = &job.context;
        self.ensure_job_fee_current(context.prepared.fee)
            .await
            .map_err(|_| {
                self.metrics
                    .record_stale_job_rejection(StaleJobCause::FeeFloor);
                protocol_error("stale-job", "job CTV fee is below the current relay floor")
            })?;
        let tip_observation::SubmitAdmission {
            current,
            tip: selected,
            lease,
        } = self.submit_admission().await?;
        if lease.is_none()
            && last_poll.elapsed() >= self.config.health_timeout
            && !selected.share_lease
        {
            // #581: readiness ages out when neither a refresh nor a guarded
            // tip poll (#622) renews it; the reason names the dependency the
            // latest refresh failed on, or (#655) the one the refresh in
            // flight is still blocked on past its deadline.
            return Err(protocol_error(
                self.stale_readiness_reason(refresh_failed_on_database),
                "current chain state is unavailable",
            ));
        }
        let revision = if let Some(lease) = &lease {
            // Re-reading only a revision here would pair a newer transaction
            // fence with the older balance digest checked by lease admission.
            lease
                .revision_for(&context.prepared)
                .ok_or_else(|| protocol_error("stale-job", "stale job"))?
        } else {
            self.submit_ledger.payout_revision().await.map_err(|_| {
                protocol_error(DATABASE_UNAVAILABLE, "current payout state is unavailable")
            })?
        };
        let parent_stale = selected.hash != job.wire.previousblockhash;
        // One observation of the published tip, shared by the block-capture
        // admission and the ordinary candidate-currency check below, so a tip
        // that moves between them cannot make one see a current parent and the
        // other a stale one.
        let observed_tip_is_parent =
            self.observed_tip.read().await.as_deref() == Some(job.wire.previousblockhash.as_str());
        // #478 block capture: a block-bearing proof whose parent is still the
        // active tip is offered to the node even when its payout revision was
        // superseded by a same-parent settlement bump. The block is valid on
        // this parent; only the pool's payout snapshot moved. Share credit
        // stays fenced on the current revision (the deferred/block-only path),
        // and the block lands as-issued at the observed revision, carrying any
        // divergence as debt in the additive balance (see `land_offered`).
        // `PRISM_CAPTURE_OVERPAY_CEILING_BPS=0` turns capture off: such a
        // proof is refused `stale-job` exactly as before #478.
        let block_capture = self.config.capture_overpay_ceiling_bps > 0
            && submission.block_pass
            && !parent_stale
            && !selected.share_lease
            && observed_tip_is_parent;
        // #478: work a same-parent payout replacement retired is block-only.
        // It is fenced here, before any grace branch, so it reaches no credit
        // path at all: not stale grace or parent grace after a flip, not the
        // ordinary current-revision credit, and (below) not the deferred share
        // of its captured block. Only a block on the active parent passes.
        let block_only = job.wire.kind == codec::JobKind::BlockOnly;
        if block_only && !block_capture {
            return Err(self.stale_job(if parent_stale {
                StaleJobCause::ParentGrace
            } else {
                StaleJobCause::PayoutRevision
            }));
        }
        let grace =
            parent_stale && stale_grace.eligible_for(&selected.hash) && selected.transitioned;
        if grace {
            let parent = self.tip_parent(&selected).await.map_err(|_| {
                protocol_error(
                    "backend-rpc-unavailable",
                    "current tip parent is unavailable",
                )
            })?;
            if parent != job.wire.previousblockhash {
                return Err(self.stale_job(StaleJobCause::ParentGrace));
            }
        } else if parent_stale {
            // A stale parent is attributed before any coincident revision change.
            return Err(self.stale_job(StaleJobCause::ParentGrace));
        } else if !block_capture
            && ((context.prepared.snapshot.payout_revision != revision
                && !(selected.share_lease
                    && context.prepared.snapshot.payout_revision
                        == current.snapshot.payout_revision
                    && current.template["previousblockhash"].as_str()
                        == Some(job.wire.previousblockhash.as_str())))
                || (current.snapshot.payout_revision != revision && !selected.share_lease))
        {
            // A plain share on a superseded revision is still refused for
            // credit; only a block-bearing proof on the current tip is spared,
            // and it is spared for capture, not credit.
            return Err(self.stale_job(StaleJobCause::PayoutRevision));
        }
        // Prior-parent share credit deliberately uses the current durable
        // revision. That exception never admits an obsolete block candidate.
        let stale = parent_stale;
        let candidate_current = !stale
            && !selected.share_lease
            && context.prepared.snapshot.payout_revision == revision
            && current.snapshot.payout_revision == revision
            && observed_tip_is_parent;
        // #478: a block on the current tip whose revision has moved, or any
        // block from block-only work. It is captured (offered and landed
        // as-issued) but its share is never credited at the current revision.
        // `block_capture` already requires `submission.block_pass &&
        // !parent_stale && observed tip`, so this is exactly a current-tip
        // block that must not take the credited path.
        let capture_stale = block_capture && (block_only || !candidate_current);
        if !submission.share_pass
            && !(submission.block_pass && (candidate_current || capture_stale))
        {
            return Err(protocol_error("low-difficulty", "low difficulty share"));
        }
        let network = context.bundle.found_block.network_difficulty;
        let difficulty = if submission.share_pass {
            codec::scaled_target_difficulty(&job.wire.share_target)
                .map_err(|_| protocol_error("internal-error", "difficulty overflow"))?
        } else {
            network
        };
        let share = AcceptedShare {
            share_seq: 0,
            share_id: format!("{}:{}", context.worker.username, submission.block_hash_hex),
            miner_id: context.worker.payout_address.clone(),
            order_key: context.worker.payout_address.clone(),
            p2mr_program_hex: context.worker.p2mr_program_hex.clone(),
            share_difficulty: difficulty,
            network_difficulty: network,
            template_height: template_parent_height(context.bundle.found_block.block_height)
                .map_err(|_| protocol_error("internal-error", "invalid candidate block height"))?,
            job_id: job.wire.job_id.clone(),
            job_issued_at_ms: context.prepared.snapshot.anchor_ms,
            accepted_at_ms: 0,
            ntime: submission.ntime,
            credit_policy: stale.then(|| "stale-grace".into()),
        };
        {
            // An explicit unsafe/unknown observation during revision or parent
            // I/O revokes this admission even if a later poll recovers trust.
            let readiness = self.readiness.read().await;
            if readiness.generation != readiness_generation || readiness.last_poll.is_none() {
                return Err(protocol_error(
                    "backend-rpc-unavailable",
                    "current chain state is unavailable",
                ));
            }
        }
        if let Some(lease) = &lease {
            if !self.revalidate_published_lease(lease).await.map_err(|_| {
                protocol_error(
                    "backend-rpc-unavailable",
                    "current chain state is unavailable",
                )
            })? {
                return Err(protocol_error("stale-job", "stale job"));
            }
        }
        // Both acknowledgement bounds are measured from here.
        let start = tokio::time::Instant::now();
        // #829: only checks that admit the share to its append are recorded;
        // a refused or abandoned submission's are not.
        self.metrics.observe_append_stage(
            AppendStage::SubmitChecks,
            AppendStageOutcome::Passed,
            start.duration_since(entered),
        );
        let share_id = share.share_id.clone();
        let block_hash = submission.block_hash_hex.clone();
        // Credit the share now, defer it to its block's confirmation (a
        // below-target block proof, or a block captured on a superseded
        // revision, #478), or never (block-only work, #478).
        let credit_now = submission.share_pass && !capture_stale;
        let deferred_share = (!credit_now && !block_only).then(|| share.clone());
        // A found block carries what its append needs to run again after a
        // passing failure before COMMIT: the inputs its candidate is built
        // from, and the parent its retry checks against the observed tip.
        let found = match (submission.block_pass && (candidate_current || capture_stale))
            .then(|| CandidateSource::new(job, submission, deferred_share))
        {
            Some(source) => submission_candidate(&source).await.map(|candidate| {
                let path = if credit_now { "share" } else { "block-only" };
                Some((
                    candidate,
                    self.found_block_retry(source, &share.share_id, path, start),
                ))
            }),
            None => Ok(None),
        };
        let outcome = match found {
            Err(error) => SaveOutcome::Failed(error),
            // A captured stale-revision block goes through the block-only path:
            // its share is deferred (credited only if the block confirms), so
            // no share is credited against the superseded revision.
            Ok(found) if credit_now => {
                // Ordinary current-tip/candidate admission keeps its existing
                // credit contract, including a proof that returned from lease
                // selection to ordinary authority before admission finished.
                let fence = lease
                    .filter(|_| selected.share_lease)
                    .map(|lease| self.lease_commit_fence(lease, job.wire.resume_expires_at));
                self.persist_share_pass(
                    share,
                    found,
                    proof_observed_at_ms,
                    revision,
                    &block_hash,
                    start,
                    fence,
                )
                .await
            }
            Ok(found) => {
                let (candidate, retry) = found.unzip();
                self.persist_block_only(
                    &share,
                    candidate,
                    retry,
                    proof_observed_at_ms,
                    &block_hash,
                    start,
                )
                .await
            }
        };
        match outcome {
            SaveOutcome::Accepted => {
                self.accepted.fetch_add(1, Ordering::Relaxed);
                if grace {
                    self.metrics.record_grace_credit();
                }
                if current.bundle.is_none() {
                    self.wake.notify_one();
                }
                Ok(())
            }
            SaveOutcome::Duplicate => Err(protocol_error("duplicate-share", "duplicate share")),
            SaveOutcome::Stale => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                Err(protocol_error("stale-job", "stale job"))
            }
            SaveOutcome::AuthorityUnavailable => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                // As admission answers readiness it cannot prove, from the
                // same readiness read: the ledger database when the latest
                // refresh failed on it or the refresh in flight is still
                // blocked on it (#581, #655), the node otherwise. The
                // readiness lock is not read again, so a refusal never waits
                // behind a writer for its answer.
                Err(protocol_error(
                    self.stale_readiness_reason(refresh_failed_on_database),
                    "current chain state is unavailable",
                ))
            }
            SaveOutcome::Superseded(cause) => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                Err(self.stale_job(cause))
            }
            // The enqueue refused the block before COMMIT: this primary does
            // not hold the window its coinbase pays (#619). The job's
            // authority is stale, not the ledger failing, so the answer is
            // `stale-job`; nothing was written or offered. The ledger counted
            // `window_not_held` at the refusal itself, so a refusal that
            // finishes after this answer's deadline is counted too; it is not
            // counted again here.
            SaveOutcome::Failed(error)
                if error
                    .downcast_ref::<crate::ledger::WindowNotHeld>()
                    .is_some() =>
            {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                Err(protocol_error("stale-job", "stale job"))
            }
            SaveOutcome::Failed(error) => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                if refused_at_hook(&error) {
                    // A refused local gate proves COMMIT was never sent, and
                    // `persist_share_pass` logged its closure. A deadline
                    // closure or an unspecified refusal has no proven cause:
                    // it remains strict.
                    return Err(protocol_error(
                        "ledger-confirmation-failed",
                        "share was not committed because its commit gate closed",
                    ));
                }
                if error.to_string().contains("duplicate-share")
                    || error.to_string().contains("duplicate share_id")
                {
                    return Err(protocol_error("duplicate-share", "duplicate share"));
                }
                tracing::warn!(%error,"share persistence failed");
                Err(protocol_error(
                    "ledger-confirmation-failed",
                    "share was not confirmed by the database",
                ))
            }
            SaveOutcome::Unknown { phase, detail } => {
                // An error response like any other rejection; the share may
                // still be credited, so its ID is logged for reconciliation.
                self.rejected.fetch_add(1, Ordering::Relaxed);
                let path = if credit_now { "share" } else { "block-only" };
                tracing::warn!(
                    share_id,
                    block_hash,
                    path,
                    phase,
                    detail,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "share outcome unknown at the acknowledgement deadline"
                );
                Err(protocol_error(
                    "ledger-outcome-unknown",
                    "share outcome is not yet known",
                ))
            }
        }
    }

    /// Append a share-pass submission under a commit gate. The only
    /// acknowledgement deadline is `share_commit_timeout`, plus
    /// `share_commit_grace` once COMMIT is in flight. An append that carries a
    /// block candidate gets the same deadline and grace (#574: never less than
    /// a plain share) but is never refused, so it runs on past the answer.
    /// If the payout revision moved before its commit, such an append
    /// captures its block instead (#657), and the answer follows the block's
    /// disposition, as a block-only proof's does.
    #[allow(clippy::too_many_arguments)]
    async fn persist_share_pass(
        &self,
        share: AcceptedShare,
        found: Option<(Candidate, FoundBlockRetry)>,
        proof_observed_at_ms: Option<i64>,
        revision: i64,
        block_hash: &str,
        start: tokio::time::Instant,
        lease: Option<publication_authority::LeaseCommitFence>,
    ) -> SaveOutcome {
        let share_id = share.share_id.clone();
        let (candidate, mut retry) = found.unzip();
        // #657: with capture on, a block whose revision moved before its
        // commit is captured, as the submit check captures one whose revision
        // already moved; with it off, the fence refuses it as before.
        let moved = if self.config.capture_overpay_ceiling_bps > 0 {
            crate::ledger::MovedRevision::Capture
        } else {
            crate::ledger::MovedRevision::Refuse
        };
        let gate = Arc::new(CommitGate::with_lease(lease));
        // A found block commits with its share, and the outbox is the only
        // path to submitblock, so a candidate-bearing append is never refused,
        // and one that fails before COMMIT for a passing reason runs again.
        let refusable = candidate.is_none();
        let ledger = self.submit_ledger.clone();
        let task_gate = gate.clone();
        let mut task = AppendTask {
            handle: Some(tokio::spawn(async move {
                let mut attempt = (share, candidate);
                let result = loop {
                    // Only a lease fence sends a refused append round again
                    // (#716), and a fenced append carries no candidate, so the
                    // copy is a share's few strings.
                    let again = task_gate.fenced().then(|| attempt.clone());
                    // A found block's append keeps its share's few strings
                    // for another attempt; its candidate is built again.
                    let share_again = retry.as_ref().map(|_| attempt.0.clone());
                    let (share, candidate) = attempt;
                    let result = ledger
                        .append_at_revision_observed(
                            share,
                            candidate,
                            proof_observed_at_ms,
                            revision,
                            moved,
                            task_gate.clone(),
                        )
                        .await;
                    let refused = matches!(&result, Err(error) if refused_at_hook(error));
                    if let Some(again) = again.filter(|_| refused) {
                        // Nothing was written: the hook refused before COMMIT
                        // and the attempt rolled back, releasing ORDER_LOCK.
                        let waited = tokio::time::Instant::now();
                        if !task_gate.retry_after_contention().await {
                            break result;
                        }
                        tracing::info!(
                            share_id = %again.0.share_id,
                            waited_ms = waited.elapsed().as_millis() as u64,
                            "share commit gate waited out contention for its authority; appending again"
                        );
                        attempt = again;
                        continue;
                    }
                    let (Some(retry), Some(share)) = (retry.as_mut(), share_again) else {
                        break result;
                    };
                    let error = match &result {
                        Ok(_) => {
                            retry.recorded();
                            break result;
                        }
                        Err(error) => error,
                    };
                    // A found block's gate carries no lease and nothing closes
                    // it, so it is still open exactly when the attempt failed
                    // before its pre-COMMIT hook let COMMIT be sent: nothing
                    // was written and the transaction rolled back. A gate at
                    // `Committing` may have committed, and keeps its answer.
                    if task_gate.state() != GateState::Open {
                        break result;
                    }
                    let Some(candidate) = retry.next_attempt(error, None).await else {
                        break result;
                    };
                    attempt = (share, Some(candidate));
                };
                // Measured here, not at the join: a coordinator task that is
                // scheduled late must not turn a durable commit into unknown.
                let commit_elapsed = task_gate.committing_since().map(|since| since.elapsed());
                (result, commit_elapsed, tokio::time::Instant::now())
            }
            // Its retries and refusals log where the submission logs.
            .with_current_subscriber())),
            gate: gate.clone(),
            refusable,
        };
        let deadline = start + self.config.share_commit_timeout;
        let (joined, phase) = match task.finish_by(deadline).await {
            Some(joined) => (Some(joined), "commit-in-flight"),
            None if refusable && gate.close() => {
                task.abort();
                // `close` also reports a gate that a lease fence closed first,
                // at the pre-COMMIT hook while the append's ROLLBACK was still
                // in flight, or once it had waited out contention for its
                // authority. The deadline did not win that gate, so the
                // fence's closure decides the answer, as it does once the
                // append returns (#716). A gate whose append was still
                // waiting out that contention closes for the contention.
                return match gate.closure() {
                    closure @ Some(
                        GateClosure::StaleAuthority
                        | GateClosure::AuthorityUnavailable
                        | GateClosure::AuthorityContended,
                    ) => closed_gate_outcome(closure, CommitGateClosed.into()),
                    Some(GateClosure::DeadlineOrCancelled) | None => SaveOutcome::Failed(
                        anyhow::anyhow!("share commit deadline passed before COMMIT was sent"),
                    ),
                };
            }
            // A refusable gate that would not close is already committing.
            None if refusable || gate.state() == GateState::Committing => (
                task.finish_by(deadline + self.config.share_commit_grace)
                    .await,
                "commit-in-flight",
            ),
            None => (None, "candidate-pending"),
        };
        let Some(joined) = joined else {
            task.follow(share_id, "share", phase);
            if !refusable {
                self.metrics.record_block_ack_capped(BlockAckPath::Share);
            }
            return SaveOutcome::Unknown {
                phase,
                detail: "the append had not finished by the acknowledgement deadline".into(),
            };
        };
        let (joined, commit_elapsed, finished_at) = match joined {
            Ok((result, commit_elapsed, finished_at)) => {
                (Ok(result), commit_elapsed, Some(finished_at))
            }
            Err(error) => (Err(error), None, None),
        };
        // #657: the payout revision moved after the submit check, so the
        // append committed the block as a capture and deferred the share. As
        // for a block-only proof, the answer is the block's disposition: the
        // share is credited if the block confirms within the bound.
        // A capture commits only after `begin_commit`, so its COMMIT time is
        // known and the sync-rep guard applies to it as to any commit.
        if matches!(joined, Ok(Ok(Appended::Captured))) {
            if let Some(unknown) = sync_rep_unknown(commit_elapsed, self.statement_timeout) {
                return unknown;
            }
            return self
                .deferred_share_disposition(
                    &share_id,
                    block_hash,
                    start + self.config.share_commit_timeout,
                    "candidate-pending",
                    (BlockAckPath::Share, "share"),
                )
                .await;
        }
        let joined = joined.map(|appended| appended.map(|appended| appended == Appended::Inserted));
        let outcome =
            classify_share_append(joined, gate.state(), commit_elapsed, self.statement_timeout);
        let outcome = match outcome {
            // A refused local gate proves COMMIT was never sent; the closure
            // that won it decides the answer.
            SaveOutcome::Failed(error) if refused_at_hook(&error) => {
                closed_gate_outcome(gate.closure(), error)
            }
            // The append's payout-revision fence refused the share before any
            // write: a settlement moved the revision between the submit check
            // and the append's read under `ORDER_LOCK`. Its work was
            // superseded, which the submit check itself answers `stale-job`
            // with cause `payout_revision` a moment later, so the answer is
            // the same (#675). The ledger is healthy: this is not
            // `ledger-confirmation-failed`, which feeds the share-append
            // failure warning. A block-bearing append is refused here only
            // with capture off, and its found block is then never offered, as
            // the submit check drops one; that loss is logged.
            SaveOutcome::Failed(error)
                if error
                    .downcast_ref::<crate::ledger::PayoutRevisionChanged>()
                    .is_some() =>
            {
                if !refusable {
                    tracing::warn!(
                        %error,
                        block_hash,
                        "found block refused at the payout-revision fence with capture off; it is not offered"
                    );
                }
                SaveOutcome::Superseded(StaleJobCause::PayoutRevision)
            }
            outcome => outcome,
        };
        // Count the confirmation from when the append finished. Classifying
        // here can happen arbitrarily later, and an on-time confirmation must
        // not be reported as a late one.
        if matches!(outcome, SaveOutcome::Accepted)
            && finished_at.is_some_and(|at| at >= start + self.config.share_commit_timeout)
        {
            self.metrics.record_late_confirmation();
        }
        outcome
    }

    /// Credit a block-only proof once its candidate is confirmed on the active
    /// chain, waiting at most `share_commit_timeout` from `start` (#574): a
    /// captured proof's answer used to wait up to `max(share_commit_timeout,
    /// 60 s)` for its own frontend's landing, and the miner's session can
    /// submit nothing else meanwhile. At the bound the proof is answered
    /// `ledger-outcome-unknown`, as any share still pending at its deadline
    /// is; the block and any deferred share are still landed and credited
    /// once, because only the wait ends there.
    async fn persist_block_only(
        &self,
        share: &AcceptedShare,
        candidate: Option<Candidate>,
        mut retry: Option<FoundBlockRetry>,
        proof_observed_at_ms: Option<i64>,
        block_hash: &str,
        start: tokio::time::Instant,
    ) -> SaveOutcome {
        let bound = start + self.config.share_commit_timeout;
        let candidate = match candidate.context("missing candidate") {
            Ok(candidate) => candidate,
            Err(error) => return SaveOutcome::Failed(error),
        };
        // Block-only work (#478) defers no share: no credit will ever appear,
        // so once its block is captured the share is refused at once rather
        // than acknowledged at confirmation.
        let defers_share = candidate.deferred_share.is_some();
        // The probe and the enqueue run in one spawned task that the bound
        // never cancels: only the wait for it is bounded (#574). A probe cut
        // off at the bound would answer before anything was written and
        // never enqueue the block. Past the bound the task is followed, so a
        // slow probe or a degraded database delays the block's offer, never
        // drops it.
        //
        // The ledger is partitioned by `share_seq` (migration 016) and holds
        // no global `share_id` index, so an unbounded probe descends one
        // per-leaf index per attached partition. This share was submitted by
        // the connection that is still waiting for its acknowledgement, so it
        // can only be in the newest leaves: `qbit_prism_share_probe_floor()`
        // is the lower bound of the partition two below the one the next
        // `share_seq` lands in, which prunes the probe to at most three
        // leaves holding rows at executor start, whatever width each
        // partition was created with.
        //
        // The enqueue commits the pending outbox row and the deferred share
        // together, so it is never cut off either: dropping it could discard
        // the reply to a COMMIT that was already sent.
        //
        // A probe that failed wrote nothing, and so did an enqueue that
        // provably failed before COMMIT; when the failure was a passing one,
        // both run again in this task while the block can still matter
        // ([`FoundBlockRetry`]). Each attempt probes the share again, and
        // the enqueue finds an existing outbox row and writes no second one.
        let mut phase = "candidate-pending";
        let ledger = self.ledger.clone();
        let share_id = share.share_id.clone();
        let mut enqueue = tokio::spawn(async move {
            let mut candidate = candidate;
            loop {
                let probed = async {
                    sqlx::query_scalar::<_, bool>(
                        "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1 AND share_seq>=qbit_prism_share_probe_floor())",
                    )
                    .bind(&share_id)
                    .fetch_one(&mut *ledger.acquire().await?)
                    .await
                }
                .await;
                // A probe that failed leaves the candidate for the next
                // attempt; an enqueue consumes it.
                let (step, unused) = match probed {
                    Ok(true) => (EnqueueStep::Recorded, None),
                    Ok(false) => (
                        EnqueueStep::Enqueue(
                            ledger
                                .enqueue_candidate_observed(candidate, proof_observed_at_ms)
                                .await,
                        ),
                        None,
                    ),
                    Err(error) => (EnqueueStep::ProbeFailed(error.into()), Some(candidate)),
                };
                let error = match &step {
                    EnqueueStep::ProbeFailed(error) => error,
                    EnqueueStep::Enqueue(Err(error)) if enqueue_failed_before_commit(error) => {
                        error
                    }
                    // An enqueue whose COMMIT may have been sent keeps its
                    // answer, followed through the outbox.
                    EnqueueStep::Enqueue(Err(_)) => break step,
                    EnqueueStep::Recorded | EnqueueStep::Enqueue(Ok(_)) => {
                        if let Some(retry) = &retry {
                            retry.recorded();
                        }
                        break step;
                    }
                };
                let Some(retry) = retry.as_mut() else {
                    break step;
                };
                match retry.next_attempt(error, unused).await {
                    Some(next) => candidate = next,
                    None => break step,
                }
            }
        }
        // Its retries and refusals log where the submission logs.
        .with_current_subscriber());
        match tokio::time::timeout_at(bound, &mut enqueue).await {
            Ok(Ok(EnqueueStep::Recorded)) => return SaveOutcome::Duplicate,
            // Nothing is written before the enqueue, so every failure up to
            // it is definite.
            Ok(Ok(EnqueueStep::ProbeFailed(error))) => return SaveOutcome::Failed(error),
            Ok(Ok(EnqueueStep::Enqueue(Ok(true)))) => {}
            Ok(Ok(EnqueueStep::Enqueue(Ok(false)))) => return SaveOutcome::Duplicate,
            Ok(Ok(EnqueueStep::Enqueue(Err(error)))) if enqueue_failed_before_commit(&error) => {
                return SaveOutcome::Failed(error)
            }
            Ok(Ok(EnqueueStep::Enqueue(Err(error)))) => {
                tracing::warn!(
                    share_id = %share.share_id,
                    block_hash,
                    path = "block-only",
                    %error,
                    "block-only enqueue outcome unknown; following the outbox"
                );
                phase = "enqueue-unknown";
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    share_id = %share.share_id,
                    block_hash,
                    path = "block-only",
                    %error,
                    "block-only enqueue task ended without a result; following the outbox"
                );
                phase = "enqueue-unknown";
            }
            Err(_) => {
                follow_enqueue(enqueue, share.share_id.clone(), block_hash.to_string());
                self.metrics
                    .record_block_ack_capped(BlockAckPath::BlockOnly);
                return SaveOutcome::Unknown {
                    phase: "enqueue-pending",
                    detail: "the duplicate probe or candidate enqueue had not finished by the acknowledgement bound"
                        .into(),
                };
            }
        }
        if !defers_share {
            // The block is captured (enqueued, or being followed); the share
            // of block-only work is never credited, so it is refused now.
            if phase == "enqueue-unknown" {
                tracing::warn!(
                    share_id = %share.share_id,
                    block_hash,
                    "block-only capture enqueue outcome unknown; the candidate loop offers it if it committed, and the share is refused"
                );
            }
            return SaveOutcome::Superseded(StaleJobCause::PayoutRevision);
        }
        // A block below the advertised share target earns only proven network
        // work, and only after active-chain confirmation.
        self.deferred_share_disposition(
            &share.share_id,
            block_hash,
            bound,
            phase,
            (BlockAckPath::BlockOnly, "block-only"),
        )
        .await
    }

    /// Follow a captured block until its confirmation credits the proof's
    /// deferred solver share, or its candidate settles otherwise, waiting at
    /// most until `bound` (#574). Used by a block-only proof after its
    /// enqueue, and by a share-pass proof whose append captured its block when
    /// the payout revision moved before its commit (#657). Past the bound the
    /// answer is `ledger-outcome-unknown`; the landing runs on and credits the
    /// share once, whenever it commits. `path` labels the logs and
    /// `block_proof_ack_capped_total`.
    async fn deferred_share_disposition(
        &self,
        share_id: &str,
        block_hash: &str,
        bound: tokio::time::Instant,
        mut phase: &'static str,
        (capped, log_path): (BlockAckPath, &'static str),
    ) -> SaveOutcome {
        loop {
            // A bound already past, as for a capture whose append committed
            // in the share grace (#657), starts no poll: its answer would
            // arrive too late to use.
            if tokio::time::Instant::now() >= bound {
                break;
            }
            // Observe credit and disposition in one MVCC snapshot so
            // finalization cannot fall between two separate reads. The credit
            // this polls for is written by the reconciliation of the block
            // this call is still acknowledging, so it lands in the newest
            // leaves and carries the same probe floor as the probe above:
            // without it, every poll of this loop would descend one per-leaf
            // `share_id` index per attached partition.
            //
            // Each poll runs in its own task, as the probe and enqueue do, and
            // only the wait for it is bounded. A bound that fell inside the
            // poll's checkout, during the pool's probe of an idle connection
            // or a new connection's setup, would drop that connection with
            // its reply unread: the server sees a reset and the pool loses a
            // healthy connection. Past the bound the poll finishes on its own
            // and returns its connection.
            let poll = tokio::spawn({
                let ledger = self.ledger.clone();
                let (share_id, block_hash) = (share_id.to_owned(), block_hash.to_owned());
                async move {
                    sqlx::query_as::<_, (bool, Option<String>, Option<String>, Option<String>)>(
                        "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1 AND share_seq>=qbit_prism_share_probe_floor()), (SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$2), (SELECT offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$2), (SELECT last_error FROM qbit_block_candidate_outbox WHERE block_hash=$2)",
                    )
                    .bind(share_id)
                    .bind(block_hash)
                    .fetch_one(&mut *ledger.acquire().await?)
                    .await
                }
            });
            let poll = tokio::time::timeout_at(bound, poll)
                .await
                .map(|joined| anyhow::Ok(joined??));
            match poll {
                Err(_) => break,
                Ok(Ok((true, _, _, _))) => return SaveOutcome::Accepted,
                // A candidate abandoned before its offer never reached the
                // node: the pool itself decided the block is stale (its
                // parent changed, the #478 overpay ceiling refused it, or
                // capture is off). That is `stale-job`, as base answered such
                // a proof at submit, never a ledger failure: the ledger wrote
                // the candidate, and `ledger-confirmation-failed` feeds the
                // share-append failure warning.
                Ok(Ok((false, Some(state), _, reason))) if state == "abandoned" => {
                    return SaveOutcome::Superseded(
                        if reason.as_deref() == Some("parent superseded") {
                            StaleJobCause::ParentGrace
                        } else {
                            StaleJobCause::PayoutRevision
                        },
                    )
                }
                // D2b's answer for a candidate the node refused after the
                // offer (kept in reconciliation with the rejected outcome),
                // or one the chain proved an orphan after the offer (#415,
                // terminal with its evidence). It is not a proof:
                // reconciliation still credits the deferred share if the
                // block later becomes active, and so does the reorg
                // reconciler for an orphaned block, from its landed audit.
                Ok(Ok((false, Some(state), outcome, _)))
                    if state == ORPHANED_STATE
                        || (state == CandidateState::Reconciliation.as_str()
                            && outcome.as_deref() == Some(OfferOutcome::Rejected.as_str())) =>
                {
                    return SaveOutcome::Failed(anyhow::anyhow!(
                        "block-only proof was not accepted on the active chain"
                    ))
                }
                Ok(Ok((false, Some(_), _, _))) => phase = "candidate-pending",
                Ok(Ok((false, None, _, _))) => {}
                // The candidate is already durable, so a failed read proves
                // nothing about its credit.
                Ok(Err(error)) => {
                    tracing::warn!(
                        share_id,
                        block_hash,
                        path = log_path,
                        %error,
                        "{log_path} disposition poll failed; retrying"
                    );
                    phase = "poll-error";
                }
            }
            if tokio::time::timeout_at(bound, tokio::time::sleep(Duration::from_millis(50)))
                .await
                .is_err()
            {
                break;
            }
        }
        // The landing runs on without this wait: its confirmation credits the
        // deferred share once, whenever it commits.
        self.metrics.record_block_ack_capped(capped);
        SaveOutcome::Unknown {
            phase,
            detail: "the block candidate had no disposition by share_commit_timeout".into(),
        }
    }
}
