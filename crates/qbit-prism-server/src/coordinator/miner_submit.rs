//! Miner credit decisions, independently fenced from current-work candidates.
use super::*;

impl Coordinator {
    pub(super) async fn submit_share(
        &self,
        _worker: &Worker,
        job: &MiningJob<JobContext>,
        submission: codec::Submission,
        stale_grace: StaleGrace,
    ) -> Result<(), StratumError> {
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
                protocol_error("stale-job", "job CTV fee is below the current relay floor")
            })?;
        let tip_observation::SubmitAdmission {
            current,
            tip: selected,
        } = self.submit_admission().await?;
        if last_poll.elapsed() >= self.config.health_timeout && !selected.share_lease {
            return Err(protocol_error(
                "backend-rpc-unavailable",
                "current chain state is unavailable",
            ));
        }
        let revision = self.submit_ledger.payout_revision().await.map_err(|_| {
            protocol_error(
                "backend-rpc-unavailable",
                "current payout state is unavailable",
            )
        })?;
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
                return Err(protocol_error("stale-job", "stale job"));
            }
        } else if parent_stale
            || (context.prepared.snapshot.payout_revision != revision
                && !(selected.share_lease
                    && context.prepared.snapshot.payout_revision
                        == current.snapshot.payout_revision
                    && current.template["previousblockhash"].as_str()
                        == Some(job.wire.previousblockhash.as_str())))
            || (current.snapshot.payout_revision != revision && !selected.share_lease)
        {
            return Err(protocol_error("stale-job", "stale job"));
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
        let save = async {
            let candidate = if submission.block_pass && candidate_current {
                let original = context
                    .bundle
                    .coinbase_script_sig_suffix_hex
                    .as_ref()
                    .context("job coinbase suffix missing")?;
                let placeholder_length = (4 + job.wire.extranonce2_size) * 2;
                let prefix = original
                    .get(
                        ..original
                            .len()
                            .checked_sub(placeholder_length)
                            .context("job coinbase suffix too short")?,
                    )
                    .context("invalid job suffix")?;
                let suffix = format!(
                    "{prefix}{}{}",
                    job.wire.extranonce1, submission.extranonce2_hex
                );
                Some(Candidate {
                    block_hash: submission.block_hash_hex.clone(),
                    block_hex: submission.block_hex,
                    job_id: job.wire.job_id.clone(),
                    payout_revision: context.prepared.snapshot.payout_revision,
                    bundle: (*context.bundle).clone(),
                    coinbase_suffix_hex: Some(suffix),
                    deferred_share: (!submission.share_pass).then(|| share.clone()),
                })
            } else {
                None
            };
            if submission.share_pass {
                self.submit_ledger
                    .append_at_revision(share, candidate, revision)
                    .await
            } else {
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1)",
                )
                .bind(&share.share_id)
                .fetch_one(&self.ledger.pool)
                .await?;
                if exists {
                    return Ok(false);
                }
                if !self
                    .ledger
                    .enqueue_candidate_once(candidate.context("missing candidate")?)
                    .await?
                {
                    return Ok(false);
                }
                // A block below the advertised share target earns only proven
                // network work, and only after active-chain confirmation.
                loop {
                    // Observe credit and disposition in one MVCC snapshot so
                    // finalization cannot fall between two separate reads.
                    let (credited,state): (bool,Option<String>) = sqlx::query_as(
                        "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1), (SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$2)",
                    )
                    .bind(&share.share_id)
                    .bind(&submission.block_hash_hex)
                    .fetch_one(&self.ledger.pool)
                    .await?;
                    if credited {
                        break Ok(true);
                    }
                    ensure!(
                        state.as_deref() == Some("pending"),
                        "block-only proof was not accepted on the active chain"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        let save = tokio::time::timeout(self.config.share_commit_timeout, save)
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("share confirmation deadline exceeded")));
        match save {
            Ok(true) => {
                self.accepted.fetch_add(1, Ordering::Relaxed);
                if current.bundle.is_none() {
                    self.wake.notify_one();
                }
                Ok(())
            }
            Ok(false) => Err(protocol_error("duplicate-share", "duplicate share")),
            Err(error) => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
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
        }
    }
}
