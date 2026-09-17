//! Miner credit decisions, independently fenced from current-work candidates.
//!
//! A share's acknowledgement never outruns its ledger outcome. The append runs
//! in its own task behind a [`CommitGate`]: at `share_commit_timeout` the
//! acknowledgement either closes the gate, so COMMIT is never sent, or finds
//! COMMIT already in flight and waits `share_commit_grace` for its reply. An
//! outcome still unknown at the deadline is answered `ledger-outcome-unknown`,
//! never as a failure. Block-only proofs wait for their candidate's
//! disposition up to `block_only_ack_timeout` instead.
use super::submit_ledger::{CommitGate, GateState};
use super::*;
use crate::ledger::CommitGateClosed;
use crate::metrics::StaleJobCause;
use sqlx::postgres::{PgDatabaseError, PgSeverity};
use tokio::task::{JoinError, JoinHandle};

#[cfg(test)]
#[path = "miner_submit_acquire_tests.rs"]
mod acquire_tests;

/// How a persistence attempt ended, as far as its acknowledgement can tell.
#[derive(Debug)]
pub(super) enum SaveOutcome {
    Accepted,
    Duplicate,
    /// The ledger did not record the share: `ledger-confirmation-failed`.
    Failed(anyhow::Error),
    /// Not known by the acknowledgement deadline; the share may still be
    /// credited.
    Unknown {
        phase: &'static str,
        detail: String,
    },
}

/// Classify a finished share-pass append by how far its gate got.
///
/// Leave a candidate enqueue that outlived the acknowledgement bound running,
/// and log its outcome once it resolves. Cancelling it could strand a COMMIT
/// that was already sent.
fn follow_enqueue(handle: JoinHandle<Result<bool>>, share_id: String, block_hash: String) {
    tokio::spawn(async move {
        let (outcome, error) = match handle.await {
            Ok(Ok(true)) => ("enqueued", None),
            Ok(Ok(false)) => ("already-enqueued", None),
            Ok(Err(error)) => ("error", Some(format!("{error:#}"))),
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
        Ok(Ok(true)) => match statement_timeout {
            Some(limit) if commit_elapsed.is_none_or(|elapsed| elapsed >= limit) => {
                SaveOutcome::Unknown {
                    phase: "sync-rep-guard",
                    detail: format!(
                        "possible sync-rep cancellation: COMMIT took {commit_elapsed:?}, at least statement_timeout {limit:?}"
                    ),
                }
            }
            _ => SaveOutcome::Accepted,
        },
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

/// A spawned share append. If the submission itself is cancelled first, an
/// append that may still be refused is closed and aborted, as dropping it
/// did before; one already committing, or carrying a block, runs on.
/// An append's result, how long COMMIT took, and when the append finished.
/// Both times are taken inside the task, so a late poll of this handle can
/// neither count scheduler delay as database time nor make an on-time
/// confirmation look late.
type AppendJoin = (Result<bool>, Option<Duration>, tokio::time::Instant);

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
    /// `ORDER_LOCK` now instead of running to the hook's refusal.
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
                Ok((Ok(true), _, _)) => {
                    tracing::warn!(
                        share_id,
                        path,
                        phase,
                        outcome = "committed",
                        "unknown share outcome resolved"
                    )
                }
                Ok((Ok(false), _, _)) => {
                    tracing::warn!(
                        share_id,
                        path,
                        phase,
                        outcome = "already-recorded",
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

/// Construct the durable candidate from the job's immutable issued inputs.
pub(super) async fn submission_candidate(
    job: &MiningJob<JobContext>,
    submission: codec::Submission,
    share: AcceptedShare,
) -> Result<Candidate> {
    let context = Arc::clone(&job.context);
    let job_id = job.wire.job_id.clone();
    let extranonce1 = job.wire.extranonce1.clone();
    let extranonce2_size = job.wire.extranonce2_size;
    // The job context is shared; the recipient-sized copy and block encoding
    // belong on a blocking worker, before the ledger transaction begins.
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
            block_hash: submission.block_hash_hex,
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
            deferred_share: (!submission.share_pass).then_some(share),
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

    pub(super) async fn submit_share(
        &self,
        _worker: &Worker,
        job: &MiningJob<JobContext>,
        submission: codec::Submission,
        stale_grace: StaleGrace,
    ) -> Result<(), StratumError> {
        // The proof-observation boundary of the first-offer latency: this
        // frontend's wall clock as the locally validated block proof enters
        // the coordinator, before any await. Recorded on the candidate row
        // at enqueue; a clock before the epoch leaves it unknown.
        let proof_observed_at_ms = submission.block_pass.then(|| unix_ms_now().ok()).flatten();
        let (last_poll, readiness_generation) = {
            let readiness = self.readiness.read().await;
            let last_poll = readiness.last_poll.ok_or_else(|| {
                protocol_error(
                    "backend-rpc-unavailable",
                    "current chain state is unavailable",
                )
            })?;
            (last_poll, readiness.generation)
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
            return Err(protocol_error(
                "backend-rpc-unavailable",
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
                protocol_error(
                    "backend-rpc-unavailable",
                    "current payout state is unavailable",
                )
            })?
        };
        let parent_stale = selected.hash != job.wire.previousblockhash;
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
        } else if (context.prepared.snapshot.payout_revision != revision
            && !(selected.share_lease
                && context.prepared.snapshot.payout_revision == current.snapshot.payout_revision
                && current.template["previousblockhash"].as_str()
                    == Some(job.wire.previousblockhash.as_str())))
            || (current.snapshot.payout_revision != revision && !selected.share_lease)
        {
            return Err(self.stale_job(StaleJobCause::PayoutRevision));
        }
        // Prior-parent share credit deliberately uses the current durable
        // revision. That exception never admits an obsolete block candidate.
        let stale = parent_stale;
        let candidate_current = !stale
            && !selected.share_lease
            && context.prepared.snapshot.payout_revision == revision
            && current.snapshot.payout_revision == revision
            && self.observed_tip.read().await.as_deref()
                == Some(job.wire.previousblockhash.as_str());
        if !submission.share_pass && !(submission.block_pass && candidate_current) {
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
        let share_id = share.share_id.clone();
        let block_hash = submission.block_hash_hex.clone();
        let share_pass = submission.share_pass;
        let candidate = if submission.block_pass && candidate_current {
            submission_candidate(job, submission, share.clone())
                .await
                .map(Some)
        } else {
            Ok(None)
        };
        let outcome = match candidate {
            Err(error) => SaveOutcome::Failed(error),
            Ok(candidate) if share_pass => {
                // Ordinary current-tip/candidate admission keeps its existing
                // credit contract, including a proof that returned from lease
                // selection to ordinary authority before admission finished.
                let fence = lease
                    .filter(|_| selected.share_lease)
                    .map(|lease| self.lease_commit_fence(lease, job.wire.resume_expires_at));
                self.persist_share_pass(
                    share,
                    candidate,
                    proof_observed_at_ms,
                    revision,
                    start,
                    fence,
                )
                .await
            }
            Ok(candidate) => {
                self.persist_block_only(&share, candidate, proof_observed_at_ms, &block_hash, start)
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
            SaveOutcome::Failed(error) => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                if error.downcast_ref::<CommitGateClosed>().is_some() {
                    // A refused local gate proves COMMIT was never sent. It
                    // can mean revoked authority or lock contention, so do not
                    // label it a stale job or a database failure.
                    tracing::info!(%error, "share commit gate refused before COMMIT");
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
                let path = if share_pass { "share" } else { "block-only" };
                tracing::warn!(
                    share_id,
                    block_hash,
                    path,
                    phase,
                    detail,
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
    /// acknowledgement deadline is `share_commit_timeout` plus
    /// `share_commit_grace`, or `block_only_ack_timeout` for an append that
    /// carries a block candidate.
    async fn persist_share_pass(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        proof_observed_at_ms: Option<i64>,
        revision: i64,
        start: tokio::time::Instant,
        lease: Option<publication_authority::LeaseCommitFence>,
    ) -> SaveOutcome {
        let share_id = share.share_id.clone();
        let gate = Arc::new(CommitGate::with_lease(lease));
        // A found block commits with its share, and the outbox is the only
        // path to submitblock, so a candidate-bearing append is never refused.
        let refusable = candidate.is_none();
        let ledger = self.submit_ledger.clone();
        let task_gate = gate.clone();
        let mut task = AppendTask {
            handle: Some(tokio::spawn(async move {
                let result = ledger
                    .append_at_revision_observed(
                        share,
                        candidate,
                        proof_observed_at_ms,
                        revision,
                        task_gate.clone(),
                    )
                    .await;
                // Measured here, not at the join: a coordinator task that is
                // scheduled late must not turn a durable commit into unknown.
                let commit_elapsed = task_gate.committing_since().map(|since| since.elapsed());
                (result, commit_elapsed, tokio::time::Instant::now())
            })),
            gate: gate.clone(),
            refusable,
        };
        let (joined, phase) = if refusable {
            let deadline = start + self.config.share_commit_timeout;
            match task.finish_by(deadline).await {
                Some(joined) => (Some(joined), "commit-in-flight"),
                None if gate.close() => {
                    task.abort();
                    return SaveOutcome::Failed(anyhow::anyhow!(
                        "share commit deadline passed before COMMIT was sent"
                    ));
                }
                None => (
                    task.finish_by(deadline + self.config.share_commit_grace)
                        .await,
                    "commit-in-flight",
                ),
            }
        } else {
            (
                task.finish_by(start + self.config.block_only_ack_timeout)
                    .await,
                "candidate-pending",
            )
        };
        let Some(joined) = joined else {
            task.follow(share_id, "share", phase);
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
        let outcome =
            classify_share_append(joined, gate.state(), commit_elapsed, self.statement_timeout);
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
    /// chain, waiting at most `block_only_ack_timeout` from `start`.
    async fn persist_block_only(
        &self,
        share: &AcceptedShare,
        candidate: Option<Candidate>,
        proof_observed_at_ms: Option<i64>,
        block_hash: &str,
        start: tokio::time::Instant,
    ) -> SaveOutcome {
        let bound = start + self.config.block_only_ack_timeout;
        let candidate = match candidate.context("missing candidate") {
            Ok(candidate) => candidate,
            Err(error) => return SaveOutcome::Failed(error),
        };
        // Nothing is written before the enqueue, so every failure up to it is
        // definite.
        let exists = tokio::time::timeout_at(bound, async {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1)",
            )
            .bind(&share.share_id)
            .fetch_one(&mut *self.ledger.acquire().await?)
            .await
        })
        .await;
        match exists {
            Err(_) => {
                return SaveOutcome::Failed(anyhow::anyhow!(
                    "block-only acknowledgement bound passed before the candidate was enqueued"
                ))
            }
            Ok(Err(error)) => return SaveOutcome::Failed(error.into()),
            Ok(Ok(true)) => return SaveOutcome::Duplicate,
            Ok(Ok(false)) => {}
        }
        // The enqueue commits the pending outbox row and the deferred share
        // together, so it is never cut off: dropping it could discard the reply
        // to a COMMIT that was already sent. Only the wait for it is bounded,
        // so a degraded database cannot hold the acknowledgement past `bound`.
        let mut phase = "candidate-pending";
        let ledger = self.ledger.clone();
        let mut enqueue = tokio::spawn(async move {
            ledger
                .enqueue_candidate_observed(candidate, proof_observed_at_ms)
                .await
        });
        match tokio::time::timeout_at(bound, &mut enqueue).await {
            Ok(Ok(Ok(true))) => {}
            Ok(Ok(Ok(false))) => return SaveOutcome::Duplicate,
            Ok(Ok(Err(error))) if enqueue_failed_before_commit(&error) => {
                return SaveOutcome::Failed(error)
            }
            Ok(Ok(Err(error))) => {
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
                return SaveOutcome::Unknown {
                    phase: "enqueue-pending",
                    detail: "the candidate enqueue had not finished by the acknowledgement bound"
                        .into(),
                };
            }
        }
        // A block below the advertised share target earns only proven network
        // work, and only after active-chain confirmation.
        loop {
            // Observe credit and disposition in one MVCC snapshot so
            // finalization cannot fall between two separate reads.
            let poll = tokio::time::timeout_at(bound, async {
                sqlx::query_as::<_, (bool, Option<String>, Option<String>)>(
                    "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1), (SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$2), (SELECT offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$2)",
                )
                .bind(&share.share_id)
                .bind(block_hash)
                .fetch_one(&mut *self.ledger.acquire().await?)
                .await
            })
            .await;
            match poll {
                Err(_) => break,
                Ok(Ok((true, _, _))) => return SaveOutcome::Accepted,
                // D2b's answer for a candidate the pre-offer probe abandoned
                // as superseded, one the node refused after the offer (kept
                // in reconciliation with the rejected outcome), or one the
                // chain proved an orphan after the offer (#415, terminal with
                // its evidence). It is not a proof: reconciliation still
                // credits the deferred share if the block later becomes
                // active, and so does the reorg reconciler for an orphaned
                // block, from its landed audit.
                Ok(Ok((false, Some(state), outcome)))
                    if state == "abandoned"
                        || state == ORPHANED_STATE
                        || (state == CandidateState::Reconciliation.as_str()
                            && outcome.as_deref() == Some(OfferOutcome::Rejected.as_str())) =>
                {
                    return SaveOutcome::Failed(anyhow::anyhow!(
                        "block-only proof was not accepted on the active chain"
                    ))
                }
                Ok(Ok((false, Some(_), _))) => phase = "candidate-pending",
                Ok(Ok((false, None, _))) => {}
                // The candidate is already durable, so a failed read proves
                // nothing about its credit.
                Ok(Err(error)) => {
                    tracing::warn!(
                        share_id = %share.share_id,
                        block_hash,
                        path = "block-only",
                        %error,
                        "block-only disposition poll failed; retrying"
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
        SaveOutcome::Unknown {
            phase,
            detail: "the block candidate had no disposition by block_only_ack_timeout".into(),
        }
    }
}
