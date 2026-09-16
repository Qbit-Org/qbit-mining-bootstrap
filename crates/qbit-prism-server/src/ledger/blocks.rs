use super::audit::{persist_audit_snapshot, verify_durable_range, AuditSnapshotWrite};
use super::candidates::{header_bits_hex, CandidateState, ClaimParts};
use super::*;
use qbit_prism::{verify_audit_parts, AuditVerificationReport};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockObservation {
    pub block_hash: String,
    pub active: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolBlock {
    pub block_hash: String,
    pub height: u64,
    pub chain_state: String,
    pub maturity_state: String,
}

#[derive(Clone, Debug)]
pub struct FanoutClaim {
    pub fanout_txid: String,
    pub block_hash: String,
    pub manifest: Value,
    pub claim_token: String,
    pub attempt_count: i64,
    pub progress: Value,
}

impl Ledger {
    /// Verify the pinned audit signature and the actual first transaction of
    /// the solved block before recording any obligations. Prepared rows do not
    /// contribute to carry balances until an active-chain observation confirms
    /// them. The transaction also publishes all recovery fanout artifacts.
    ///
    /// The evidence is as issued (migration 011): the payout and carry rows
    /// are the accounts of the immutable payout manifest the block's coinbase
    /// commits to, whose prior balances are the candidate's window reference,
    /// whatever the canonical balances are when the block lands. The block
    /// row is marked with that audit's digest in the same transaction, and
    /// `qbit_carry_forward_integrity_mismatches()` validates marked rows
    /// against that manifest instead of the chain-ordered running sum. The
    /// current balances are unaffected: they sum the active per-block deltas.
    pub async fn land_candidate(
        &self,
        claim: &CandidateClaim,
        ledger_public_key: &str,
    ) -> Result<AuditVerificationReport> {
        self.land_candidate_checked(claim, ledger_public_key, None)
            .await
    }

    /// Recovery for an already-active block independently proved at this
    /// revision. Its signed audit must still match current canonical carry.
    pub async fn land_candidate_at_revision(
        &self,
        claim: &CandidateClaim,
        ledger_public_key: &str,
        expected_revision: i64,
    ) -> Result<AuditVerificationReport> {
        self.land_candidate_checked(claim, ledger_public_key, Some(expected_revision))
            .await
    }

    async fn land_candidate_checked(
        &self,
        claim: &CandidateClaim,
        ledger_public_key: &str,
        expected_revision: Option<i64>,
    ) -> Result<AuditVerificationReport> {
        let parts = claim.parts.clone().context(
            "candidate claim carries no rebuilt audit parts; rebuild its window before landing",
        )?;
        let candidate = &claim.candidate;
        let block = &candidate.block_bytes;
        ensure!(block.len() > 80, "candidate block is truncated");
        // The durable row already authenticates its header and block bytes.
        // Compact bits belong to block metadata; adding them to FoundBlock
        // would change the signed canonical audit format and historical hashes.
        let bits = header_bits_hex(block)?;
        let (tx_count, count_bytes) = compact_size(&block[80..])?;
        ensure!(tx_count > 0, "candidate has no coinbase");
        let mut parent = block[4..36].to_vec();
        parent.reverse();
        let parent_hash = hex::encode(parent);
        // Every step that walks the window runs in this one blocking task,
        // before the transaction opens: verification, the stored body, the
        // canonical byte count, the serialized leaves and accounts, and the
        // share snapshot's ordering check and digest. The transaction below
        // binds only what it produced.
        let landing = tokio_util::task::AbortOnDropHandle::new(tokio::task::spawn_blocking({
            let parts = parts.clone();
            let public_key = ledger_public_key.to_owned();
            let window = candidate.window;
            let bootstrap = candidate.bootstrap_share.clone();
            move || landing_from_parts(&parts, window, bootstrap.as_ref(), &public_key)
        }))
        .await??;
        let report = &landing.report;
        let coinbase = hex::decode(&report.coinbase_tx_hex)?;
        ensure!(
            block.get(80 + count_bytes..80 + count_bytes + coinbase.len())
                == Some(coinbase.as_slice()),
            "candidate coinbase differs from verified audit"
        );
        // The proof that the window is the ledger's own history reads the
        // whole range, so it runs before the settlement lock; under the lock
        // the range is only counted. It is needed exactly where the snapshot
        // is persisted: audit rows are never deleted, so a block that already
        // has one takes the existing-row digest path below instead. A row
        // that appears between this probe and the transaction lands on that
        // same path. The read is paged and each page is compared on the
        // blocking thread that mapped it, so it is off the runtime as well as
        // outside the lock.
        let landed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_pool_audit_bundles WHERE block_hash=$1)",
        )
        .bind(&claim.candidate.block_hash)
        .fetch_one(&mut *self.acquire().await?)
        .await?;
        if !landed {
            verify_durable_range(&self.pool, &landing.snapshot, self.metrics.as_deref()).await?;
        }
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        let state = require_claim(&mut tx, claim).await?;
        if let Some(expected) = expected_revision {
            require_revision(&mut tx, expected).await?;
        }
        let existing: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT audit_bundle_sha256,found_block_bits FROM qbit_pool_audit_bundles WHERE block_hash=$1",
        )
        .bind(&candidate.block_hash)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((digest, stored_bits)) = existing {
            ensure!(
                digest == report.audit_bundle_sha256_hex,
                "existing block audit differs from candidate"
            );
            if let Some(stored_bits) = stored_bits {
                ensure!(
                    stored_bits.eq_ignore_ascii_case(&bits),
                    "existing block bits differ from candidate header"
                );
            } else {
                // Older prepared rows can be recovered with their original
                // durable block even though no extra bits field existed.
                sqlx::query("UPDATE qbit_pool_audit_bundles SET found_block_bits=$2 WHERE block_hash=$1 AND found_block_bits IS NULL")
                    .bind(&candidate.block_hash).bind(&bits).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            return Ok(landing.report);
        }
        let revision: i64 =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            revision == expected_revision.unwrap_or(candidate.payout_revision),
            "candidate payout revision was superseded"
        );
        // The parts were built on the as-issued set the reference names
        // (`landing_from_parts` proved the digest tie), which is the set the
        // block's coinbase commits to. Whether the current canonical balances
        // must still be that set depends on the row's state as the database
        // holds it under the claim lock, never on the claim's memory of it.
        // A `pending` row has not been offered: its landing keeps the fence,
        // because a divergent pending block is superseded work the caller
        // may still abandon. A row in the offer lifecycle (reserved, offered,
        // adopted or reconciling) is a block the node has or may have, and
        // it lands with its issued accounts however the balances have moved:
        // a miner paid on chain twice against one balance carries the
        // difference as debt in the additive current balance. That divergence
        // is reported, and the marker written below records the provenance
        // the integrity validator checks marked rows by.
        let prior = read_prior_balances(&mut tx).await?;
        let current =
            tokio::task::spawn_blocking(move || qbit_prism::prior_balances_digest(&prior)).await?;
        if current != landing.prior_balances_digest {
            ensure!(
                state != CandidateState::Pending,
                "candidate prior balances differ from current canonical balances"
            );
            tracing::warn!(
                block = %candidate.block_hash,
                state = state.as_str(),
                as_issued_balances = %hex::encode(landing.prior_balances_digest),
                current_balances = %hex::encode(current),
                "ALERT: landing an as-issued audit whose prior balances differ from the current canonical balances; the block's issued accounts are recorded as evidence and the additive balances carry the difference"
            );
        }
        // Solver attribution is recorded on the block row, once, here (#144).
        // Four dashboard queries used to suffix-match every block's hash
        // against the ledger on every request; a partition detach would blank
        // a historical block's solver, and migration 015 backfilled the same
        // lookup into these columns. The subselect is the expression 015 used,
        // served by `qbit_share_ledger_accepted_block_suffix_idx`, and it
        // carries no probe floor on purpose: a landing delayed by
        // reconciliation may be older than the floor, and landings are rare
        // enough that the unbounded descent costs nothing. A block with no
        // matching accepted share (the bootstrap and below-target paths, where
        // the proof is not a credited share) lands with the four columns NULL,
        // which is what the readers' LATERAL fallback covers.
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,as_issued_audit_sha256,solver_miner_id,solver_share_id,solver_share_difficulty,solver_network_difficulty) SELECT $1,$2,$3,$4,$5,$6,solver.miner_id,solver.share_id,solver.share_difficulty,solver.network_difficulty FROM (VALUES (1)) AS block_row(one) LEFT JOIN LATERAL (SELECT share.miner_id,share.share_id,share.share_difficulty,share.network_difficulty FROM qbit_share_ledger share WHERE share.accepted AND length(share.share_id)>=65 AND lower(right(share.share_id,64))=$1 ORDER BY share.accepted_at DESC,share.share_seq DESC LIMIT 1) solver ON true")
            .bind(&candidate.block_hash).bind(i64::try_from(report.block_height)?).bind(parent_hash).bind(&report.coinbase_txid).bind(&report.coinbase_manifest_sha256_hex).bind(&report.audit_bundle_sha256_hex).execute(&mut *tx).await?;
        let snapshot_digest = persist_audit_snapshot(&mut tx, &landing.snapshot).await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,audit_body_byte_len,schema_version,found_block_network_difficulty,found_block_coinbase_value_sats,audit_commitment_leaves_hex,witness_merkle_leaves_hex,share_snapshot_sha256,found_block_bits) VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric,$8,$9,$10,$11,$12)")
            .bind(&candidate.block_hash).bind(sqlx::types::Json(&*landing.body)).bind(&report.audit_bundle_sha256_hex).bind(&report.coinbase_tx_hex)
            .bind(landing.audit_body_byte_len).bind(&parts.body.schema)
            .bind(parts.body.found_block.network_difficulty.to_string()).bind(i64::try_from(report.coinbase_value_sats)?)
            .bind(&landing.audit_commitment_leaves).bind(&landing.witness_merkle_leaves).bind(snapshot_digest).bind(&bits).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) SELECT $1,$2,a->>'recipient_id',a->>'order_key',decode(a->>'p2mr_program_hex','hex'),(a->>'onchain_amount_sats')::bigint,(a->>'carry_forward_balance_sats')::numeric,a->>'action' FROM jsonb_array_elements($3::jsonb) a")
            .bind(&candidate.block_hash).bind(i64::try_from(report.block_height)?).bind(&landing.accounts).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO qbit_payout_carry_forward(block_hash,block_height,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,settlement_fee_sats,carry_forward_balance_sats,action) SELECT $1,$2,a->>'recipient_id',a->>'order_key',decode(a->>'p2mr_program_hex','hex'),(a->>'gross_amount_sats')::bigint,(a->>'prior_balance_sats')::numeric,(a->>'candidate_balance_sats')::numeric,(a->>'onchain_amount_sats')::bigint,COALESCE((a->>'settlement_fee_sats')::bigint,0),(a->>'carry_forward_balance_sats')::numeric,a->>'action' FROM jsonb_array_elements($3::jsonb) a WHERE COALESCE(a->>'account_type','miner')='miner'")
            .bind(&candidate.block_hash).bind(i64::try_from(report.block_height)?).bind(&landing.accounts).execute(&mut *tx).await?;
        if let Some(set) = &parts.body.ctv_fanout_manifest_set {
            persist_fanouts(&mut tx, &candidate.block_hash, set).await?;
        }
        tx.commit().await?;
        Ok(landing.report)
    }

    /// `submitted` means the caller proved this block is on the active chain.
    /// A successful submitblock RPC alone is insufficient. Ambiguous network
    /// errors use retry_candidate and retain every recovery artifact.
    /// `abandoned` is reachable from `pending` only: a row the node was
    /// offered is never abandoned, it stays in reconciliation with its
    /// evidence (`Ledger::reconcile_candidate`).
    pub async fn finish_candidate(
        &self,
        claim: &CandidateClaim,
        submitted: bool,
        error: Option<&str>,
    ) -> Result<()> {
        let revision = self.payout_revision().await?;
        self.finish_candidate_at_revision(claim, submitted, error, revision)
            .await
    }

    pub async fn finish_candidate_at_revision(
        &self,
        claim: &CandidateClaim,
        submitted: bool,
        error: Option<&str>,
        expected_revision: i64,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        self.lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let revision: i64 =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            revision == expected_revision,
            "payout revision changed while observing candidate disposition"
        );
        let state = require_claim(&mut tx, claim).await?;
        if submitted {
            let changed = sqlx::query("UPDATE qbit_pool_blocks SET chain_state='confirmed',inactive_since=NULL WHERE block_hash=$1 AND chain_state IN ('prepared','inactive') AND maturity_state='immature'").bind(&claim.candidate.block_hash).execute(&mut *tx).await?.rows_affected();
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_pool_blocks WHERE block_hash=$1 AND chain_state='confirmed')").bind(&claim.candidate.block_hash).fetch_one(&mut *tx).await?;
            ensure!(
                exists,
                "cannot confirm candidate without prepared audit and payout rows"
            );
            self.credit_deferred_share(&mut tx, &claim.candidate.block_hash)
                .await?;
            if changed > 0 {
                bump_revision(&mut tx).await?;
            }
        } else {
            ensure!(
                state == CandidateState::Pending,
                "cannot abandon a candidate in state {}: an offered block keeps its evidence in reconciliation",
                state.as_str()
            );
            let mature:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_pool_blocks WHERE block_hash=$1 AND maturity_state='mature')").bind(&claim.candidate.block_hash).fetch_one(&mut *tx).await?;
            ensure!(!mature, "cannot abandon a mature candidate");
            let changed = sqlx::query("UPDATE qbit_pool_blocks SET chain_state='inactive',inactive_since=CASE WHEN chain_state='confirmed' THEN clock_timestamp() ELSE inactive_since END WHERE block_hash=$1 AND chain_state IN ('prepared','confirmed') AND maturity_state='immature'").bind(&claim.candidate.block_hash).execute(&mut *tx).await?.rows_affected();
            if changed > 0 {
                sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='reorged',claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,updated_at=clock_timestamp() WHERE block_hash=$1").bind(&claim.candidate.block_hash).execute(&mut *tx).await?;
                bump_revision(&mut tx).await?;
            }
        }
        // The terminal row is "no window": the six window columns, the block
        // and the document go NULL in one statement, so retention's
        // `window_anchor_ms IS NOT NULL` predicate is exactly the live set and
        // the outbox does not keep every submitted or abandoned block forever.
        // The offer record (reservation, call time, outcome) is small and
        // stays on a submitted row as the evidence of its one offer.
        sqlx::query("UPDATE qbit_block_candidate_outbox SET state=$3,candidate=NULL,block_bytes=NULL,window_anchor_ms=NULL,window_prior_balances_sha256=NULL,window_first_share_seq=NULL,window_last_share_seq=NULL,window_share_count=NULL,window_snapshot_sha256=NULL,completed_at=clock_timestamp(),updated_at=clock_timestamp(),last_error=$4,claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL WHERE block_hash=$1 AND claim_token=$2")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(if submitted {"submitted"} else {"abandoned"}).bind(error).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn pool_blocks_for_reconcile(&self) -> Result<Vec<PoolBlock>> {
        sqlx::query("SELECT block_hash,block_height,chain_state,maturity_state FROM qbit_pool_blocks WHERE (maturity_state='immature' AND chain_state IN ('prepared','confirmed','inactive')) OR block_hash=(SELECT block_hash FROM qbit_pool_blocks WHERE chain_state='confirmed' AND maturity_state='mature' ORDER BY block_height DESC,block_hash DESC LIMIT 1) ORDER BY block_height,block_hash")
            .fetch_all(&mut *self.acquire().await?).await?.into_iter().map(|row| Ok(PoolBlock {
                block_hash:row.try_get("block_hash")?,height:u64::try_from(row.try_get::<i64,_>("block_height")?)?,
                chain_state:row.try_get("chain_state")?,maturity_state:row.try_get("maturity_state")?,
            })).collect()
    }

    /// Observations must all come from one stable RPC tip. Missing observations
    /// leave blocks untouched. A temporary fork is reversible; mature history
    /// never silently becomes a debit or a new payout.
    pub async fn reconcile_blocks(
        &self,
        observations: &[BlockObservation],
        tip_height: u64,
    ) -> Result<()> {
        let revision = self.payout_revision().await?;
        self.reconcile_blocks_at_revision(observations, tip_height, revision)
            .await
    }

    pub async fn reconcile_blocks_at_revision(
        &self,
        observations: &[BlockObservation],
        tip_height: u64,
        expected_revision: i64,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        self.lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let revision: i64 =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            revision == expected_revision,
            "payout revision changed while collecting chain observations"
        );
        let fatal = self
            .reconcile_blocks_in(&mut tx, observations, tip_height)
            .await?;
        tx.commit().await?;
        if let Some(message) = fatal {
            bail!(message);
        }
        Ok(())
    }

    // The caller owns settlement/order locks and decides whether a fatal result
    // is committed (normal observer) or rolled back (operator recovery).
    pub(super) async fn reconcile_blocks_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        observations: &[BlockObservation],
        tip_height: u64,
    ) -> Result<Option<String>> {
        let mut changed = false;
        let observed: std::collections::HashMap<&str, bool> = observations
            .iter()
            .map(|o| (o.block_hash.as_str(), o.active))
            .collect();
        ensure!(
            observed.len() == observations.len(),
            "duplicate block observations"
        );
        let hashes: Vec<&str> = observed.keys().copied().collect();
        let rows = sqlx::query("SELECT block_hash,block_height,chain_state,maturity_state FROM qbit_pool_blocks WHERE chain_state IN ('prepared','confirmed','inactive') AND block_hash=ANY($1::text[]) ORDER BY block_height,block_hash FOR UPDATE").bind(&hashes).fetch_all(&mut **tx).await?;
        for row in rows {
            let hash: String = row.try_get("block_hash")?;
            let Some(&active) = observed.get(hash.as_str()) else {
                continue;
            };
            let state: String = row.try_get("chain_state")?;
            let maturity: String = row.try_get("maturity_state")?;
            if !active && maturity == "mature" {
                let message = format!(
                    "mature pool block disconnected: {hash}; manual reconciliation required; after investigation run qbit-prism-server fatal-state clear --reason <text>"
                );
                sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=$1,updated_at=clock_timestamp() WHERE singleton").bind(&message).execute(&mut **tx).await?;
                return Ok(Some(message));
            }
            if active && state != "confirmed" {
                sqlx::query(
                    "UPDATE qbit_pool_blocks SET chain_state='confirmed',inactive_since=NULL WHERE block_hash=$1",
                )
                .bind(&hash)
                .execute(&mut **tx)
                .await?;
                // A crash may happen after submitblock but before the durable
                // ACK. Reconciliation must credit its deferred share as part
                // of the same confirmation transaction.
                self.credit_deferred_share(tx, &hash).await?;
                sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='awaiting_maturity',claim_token=NULL,claim_expires_at=NULL,updated_at=clock_timestamp() WHERE block_hash=$1 AND settlement_status='reorged'").bind(&hash).execute(&mut **tx).await?;
                changed = true;
            } else if !active && state == "confirmed" {
                sqlx::query(
                    "UPDATE qbit_pool_blocks SET chain_state='inactive',inactive_since=clock_timestamp() WHERE block_hash=$1",
                )
                .bind(&hash)
                .execute(&mut **tx)
                .await?;
                sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='reorged',claim_token=NULL,claim_expires_at=NULL,updated_at=clock_timestamp() WHERE block_hash=$1").bind(&hash).execute(&mut **tx).await?;
                changed = true;
            }
        }
        if changed {
            bump_revision(tx).await?;
        }
        let matured: i32 = sqlx::query_scalar("SELECT qbit_mark_mature_pool_payouts($1)")
            .bind(i64::try_from(tip_height)?)
            .fetch_one(&mut **tx)
            .await?;
        if matured > 0 {
            bump_revision(tx).await?;
        }
        Ok(None)
    }

    pub async fn claim_fanout(&self, lease_seconds: i64) -> Result<Option<FanoutClaim>> {
        ensure!(lease_seconds > 0, "claim duration must be positive");
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let token = Uuid::new_v4().to_string();
        let row = sqlx::query("WITH next AS (SELECT a.fanout_txid FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE b.chain_state='confirmed' AND b.maturity_state='mature' AND (a.settlement_status IN ('broadcastable','broadcast_submitted','failed') OR (a.settlement_status='confirmed' AND (a.confirmed_depth<1000 OR a.fanout_txid=(SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE settlement_status='confirmed' AND confirmed_depth>=1000 ORDER BY confirmed_block_height DESC,fanout_txid DESC LIMIT 1)))) AND (a.next_broadcast_attempt_at IS NULL OR a.next_broadcast_attempt_at<=clock_timestamp()) AND (a.claim_expires_at IS NULL OR a.claim_expires_at<=clock_timestamp()) ORDER BY (a.settlement_status='confirmed'),a.next_broadcast_attempt_at NULLS FIRST,b.block_height,a.chunk_index FOR UPDATE OF a SKIP LOCKED LIMIT 1) UPDATE qbit_ctv_fanout_artifacts a SET claim_token=$1,claim_instance_id=$2,claim_expires_at=clock_timestamp()+$3*interval '1 second' FROM next WHERE a.fanout_txid=next.fanout_txid RETURNING a.fanout_txid,a.block_hash,a.manifest,a.broadcast_attempt_count,jsonb_build_object('status',a.settlement_status,'confirmed_block_hash',a.confirmed_block_hash,'confirmed_block_height',a.confirmed_block_height,'confirmed_depth',a.confirmed_depth,'scan_next_height',a.spend_scan_next_height,'scan_anchor_height',a.spend_scan_anchor_height,'scan_anchor_hash',a.spend_scan_anchor_hash) AS progress")
            .bind(&token).bind(&self.instance_id).bind(lease_seconds).fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        row.map(|row| {
            Ok(FanoutClaim {
                fanout_txid: row.try_get("fanout_txid")?,
                block_hash: row.try_get("block_hash")?,
                manifest: row.try_get("manifest")?,
                claim_token: token,
                attempt_count: row.try_get("broadcast_attempt_count")?,
                progress: row.try_get("progress")?,
            })
        })
        .transpose()
    }

    async fn credit_deferred_share(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        block_hash: &str,
    ) -> Result<()> {
        let row = sqlx::query(
            "SELECT share,share_sha256 FROM qbit_prism_deferred_shares WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(row) = row {
            let share: AcceptedShare = serde_json::from_value(row.try_get("share")?)?;
            let digest: String = row.try_get("share_sha256")?;
            ensure!(
                hex::encode(Sha256::digest(serde_json::to_vec(&share)?)) == digest,
                "deferred share digest mismatch"
            );
            self.append_in(tx, share).await?;
        }
        Ok(())
    }

    pub async fn finish_fanout(
        &self,
        claim: &FanoutClaim,
        status: &str,
        submit_result: Option<Value>,
        error: Option<&str>,
    ) -> Result<()> {
        if submit_result
            .as_ref()
            .is_some_and(|r| r["check_only"] == true)
        {
            return self
                .observe_fanout(claim, status, submit_result.unwrap())
                .await;
        }
        ensure!(
            [
                "broadcast_submitted",
                "confirmed",
                "failed",
                "broadcastable"
            ]
            .contains(&status),
            "invalid fanout result status"
        );
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        super::fanout::require_fanout(&mut tx, claim).await?;
        super::fanout::apply_progress(&mut tx, claim, status, submit_result.as_ref()).await?;
        let attempt_status = match status {
            "confirmed" => "accepted",
            "broadcast_submitted" => "submitted",
            "failed" => "failed",
            _ => "planned",
        };
        sqlx::query("INSERT INTO qbit_ctv_fanout_broadcast_attempts(fanout_txid,attempt_status,submit_result,error) VALUES($1,$2,$3,$4)")
            .bind(&claim.fanout_txid).bind(attempt_status).bind(&submit_result).bind(error).execute(&mut *tx).await?;
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status=CASE WHEN $3='failed' AND settlement_status='confirmed' THEN settlement_status ELSE $3 END,broadcast_attempt_count=broadcast_attempt_count+1,broadcast_attempt_detail_count=LEAST(32,broadcast_attempt_detail_count+1),first_broadcast_attempt_at=COALESCE(first_broadcast_attempt_at,clock_timestamp()),last_broadcast_attempt_at=clock_timestamp(),last_broadcast_attempt_status=$4,last_broadcast_submit_result=$5,last_broadcast_error=$6,broadcast_attempt_status_counts=jsonb_set(broadcast_attempt_status_counts,ARRAY[$4],to_jsonb(COALESCE((broadcast_attempt_status_counts->>$4)::bigint,0)+1)),next_broadcast_attempt_at=clock_timestamp()+LEAST(3600,10*(broadcast_attempt_count+1))*interval '1 second',broadcast_retry_backoff_seconds=LEAST(3600,10*(broadcast_attempt_count+1)),claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,updated_at=clock_timestamp() WHERE fanout_txid=$1 AND claim_token=$2")
            .bind(&claim.fanout_txid).bind(&claim.claim_token).bind(status).bind(attempt_status).bind(&submit_result).bind(error).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM qbit_ctv_fanout_broadcast_attempts WHERE fanout_txid=$1 AND attempt_seq NOT IN (SELECT attempt_seq FROM qbit_ctv_fanout_broadcast_attempts WHERE fanout_txid=$1 ORDER BY attempt_seq DESC LIMIT 32)").bind(&claim.fanout_txid).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

/// What one landing binds, produced off the runtime from the claim's parts.
struct Landing {
    report: AuditVerificationReport,
    /// The stored body: the parts' body, minus the counted window inside
    /// `reward_manifest.shares`, serialized once and copied into the bind as
    /// bytes rather than serialized again on the runtime.
    body: Box<serde_json::value::RawValue>,
    audit_body_byte_len: i64,
    audit_commitment_leaves: Value,
    witness_merkle_leaves: Value,
    accounts: Value,
    prior_balances_digest: [u8; 32],
    snapshot: AuditSnapshotWrite,
}

/// A writer that only counts: the canonical byte length without the bytes.
struct CountingWriter(u64);

impl std::io::Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len() as u64;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// All of landing's whole-window work, in one blocking call over the parts.
///
/// For a non-empty window the share snapshot digest **must equal the
/// reference's `snapshot_sha256`**, and the parts' prior balances **must
/// hash to the reference's `prior_balances_digest`**: the parts landing
/// stores are the window and the as-issued balances the candidate names,
/// which are what its coinbase commits to, or nothing lands.
fn landing_from_parts(
    parts: &ClaimParts,
    window: WindowRef,
    bootstrap: Option<&AcceptedShare>,
    ledger_public_key: &str,
) -> Result<Landing> {
    let body = &parts.body;
    let shares = &parts.shares;
    let report = verify_audit_parts(body, shares, ledger_public_key)?;
    let prior_balances_digest = qbit_prism::prior_balances_digest(&body.prior_balances);
    ensure!(
        prior_balances_digest == window.prior_balances_digest,
        "rebuilt audit prior balances {} differ from the candidate's window reference {}",
        hex::encode(prior_balances_digest),
        hex::encode(window.prior_balances_digest)
    );
    // The stored body drops the one copy of the window it still carries:
    // `reward_manifest.shares`, a pure fold over the snapshot. #267 removes it
    // so stored bodies stop growing with the window, and reads rebuild the
    // fold from the snapshot and prove the result against
    // `audit_bundle_sha256`. The parts' body has no top-level `shares` array
    // to begin with, so this is the only copy left to drop. The canonical
    // bytes below are unaffected: they are written from `body` and `shares`
    // directly, not from this value.
    let mut stored = serde_json::to_value(&**body)?;
    stored
        .as_object_mut()
        .context("audit bundle body is not an object")?
        .get_mut("reward_manifest")
        .and_then(Value::as_object_mut)
        .context("audit reward manifest is not an object")?
        .remove("shares");
    let raw = serde_json::value::to_raw_value(&stored)?;
    let mut counter = CountingWriter(0);
    qbit_prism::write_canonical_audit_bundle_from_parts(&mut counter, body, shares)?;
    ensure!(!shares.is_empty(), "audit share snapshot cannot be empty");
    ensure!(
        shares.windows(2).all(|s| s[0].share_seq < s[1].share_seq),
        "audit share snapshot must be ordered canonically"
    );
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(&**shares)?));
    let (first, last, inline) = match (window.shares, bootstrap) {
        (Some(range), _) => {
            ensure!(
                digest == hex::encode(range.snapshot_sha256),
                "rebuilt share snapshot {digest} differs from the candidate's window reference {}",
                hex::encode(range.snapshot_sha256)
            );
            ensure!(
                shares[0].share_seq == range.first_share_seq
                    && shares[shares.len() - 1].share_seq == range.last_share_seq
                    && u64::try_from(shares.len())? == range.share_count,
                "rebuilt share snapshot bounds differ from the candidate's window reference"
            );
            (
                i64::try_from(range.first_share_seq)?,
                i64::try_from(range.last_share_seq)?,
                None,
            )
        }
        (None, Some(bootstrap)) => {
            ensure!(
                shares.len() == 1 && &shares[0] == bootstrap,
                "empty-window audit parts do not hold the candidate's bootstrap share"
            );
            (
                i64::try_from(bootstrap.share_seq)?,
                i64::try_from(bootstrap.share_seq)?,
                Some(serde_json::to_value(&**shares)?),
            )
        }
        (None, None) => bail!("candidate bootstrap share disagrees with its window reference"),
    };
    ensure!(
        body.found_block.anchor_job_issued_at_ms == window.anchor_ms,
        "audit parts anchor differs from the candidate's window reference"
    );
    Ok(Landing {
        report,
        body: raw,
        audit_body_byte_len: i64::try_from(counter.0)?,
        audit_commitment_leaves: serde_json::to_value(&body.audit_commitment_leaves_hex)?,
        witness_merkle_leaves: serde_json::to_value(&body.witness_merkle_leaves_hex)?,
        accounts: serde_json::to_value(&body.payout_policy_manifest.accounts)?,
        prior_balances_digest,
        snapshot: AuditSnapshotWrite {
            digest,
            first_share_seq: first,
            last_share_seq: last,
            anchor_ms: window.anchor_ms,
            network_difficulty: body.found_block.network_difficulty,
            share_count: i64::try_from(shares.len())?,
            inline,
            shares: Arc::clone(shares),
        },
    })
}

/// Prove the claim live under the row lock and return the row's lifecycle
/// state as the database holds it now. Callers decide by this state, never
/// by the claim's in-memory `lifecycle`, which describes the row as it was
/// claimed: the same attempt may have reserved, offered or adopted it since.
async fn require_claim(
    tx: &mut Transaction<'_, Postgres>,
    claim: &CandidateClaim,
) -> Result<CandidateState> {
    // Block takeover (FOR UPDATE), while allowing the owner to renew the
    // non-key lease columns throughout a long audit-persistence transaction.
    sqlx::query(
        "SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR KEY SHARE",
    )
    .bind(&claim.candidate.block_hash)
    .fetch_optional(&mut **tx)
    .await?;
    let row: Option<(bool, String)> = sqlx::query_as(&format!("SELECT claim_token=$2 AND claim_expires_at>clock_timestamp() AND state IN {},state FROM qbit_block_candidate_outbox WHERE block_hash=$1", CandidateState::UNFINISHED_SQL))
        .bind(&claim.candidate.block_hash).bind(&claim.claim_token).fetch_optional(&mut **tx).await?;
    match row {
        Some((true, state)) => CandidateState::parse(&state),
        _ => bail!("candidate claim was lost or expired"),
    }
}

async fn bump_revision(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton").execute(&mut **tx).await?;
    Ok(())
}

pub(super) async fn persist_fanouts(
    tx: &mut Transaction<'_, Postgres>,
    block_hash: &str,
    set: &qbit_prism::CtvFanoutManifestSet,
) -> Result<()> {
    let raw = qbit_prism::canonical_ctv_fanout_manifest_set_bytes(set)?;
    let digest = hex::encode(Sha256::digest(&raw));
    let mode = serde_json::to_value(&set.settlement_mode)?
        .as_str()
        .context("invalid settlement mode")?
        .to_owned();
    sqlx::query("INSERT INTO qbit_ctv_fanout_sets(block_hash,manifest_set_json,manifest_set,manifest_set_sha256,settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,fanout_count,fanout_output_sum_sats,covenant_output_value_sats) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING")
        .bind(block_hash).bind(String::from_utf8(raw)?).bind(serde_json::to_value(set)?).bind(&digest).bind(mode).bind(&set.parent_coinbase_txid).bind(&set.manifests[0].parent_coinbase_tx_hex)
        .bind(i32::try_from(set.fanout_count)?).bind(i64::try_from(set.fanout_output_sum_sats)?).bind(i64::try_from(set.covenant_output_value_sats)?).execute(&mut **tx).await?;
    let matches: bool = sqlx::query_scalar("SELECT manifest_set=$2 AND manifest_set_sha256=$3 FROM qbit_ctv_fanout_sets WHERE block_hash=$1")
        .bind(block_hash).bind(serde_json::to_value(set)?).bind(&digest).fetch_one(&mut **tx).await?;
    ensure!(
        matches,
        "existing CTV manifest set conflicts with verified audit"
    );
    for manifest in &set.manifests {
        let pre = &manifest.precommitment;
        let value = serde_json::to_value(manifest)?;
        let raw = serde_json::to_string(&value)?;
        sqlx::query("INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,anchor_vout,covenant_output_value_sats,fanout_output_sum_sats) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) ON CONFLICT DO NOTHING")
            .bind(&manifest.fanout_txid).bind(block_hash).bind(&digest).bind(&raw).bind(value).bind(hex::encode(Sha256::digest(raw.as_bytes())))
            .bind(&manifest.precommitment_sha256_hex).bind(&pre.ctv_hash_hex).bind(&manifest.commitment_witness_leaf_hex).bind(i32::try_from(pre.chunk_index)?).bind(i32::try_from(pre.chunk_count)?)
            .bind(&manifest.parent_coinbase_txid).bind(i32::try_from(manifest.parent_coinbase_vout)?).bind(&pre.fanout_tx_template_hex).bind(&manifest.fanout_tx_hex)
            .bind(pre.anchor_vout.map(i32::try_from).transpose()?).bind(i64::try_from(manifest.covenant_output_value_sats)?).bind(i64::try_from(pre.fanout_output_sum_sats)?).execute(&mut **tx).await?;
        let matches: bool = sqlx::query_scalar("SELECT manifest=$2 AND block_hash=$3 AND manifest_set_sha256=$4 FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1")
            .bind(&manifest.fanout_txid).bind(serde_json::to_value(manifest)?).bind(block_hash).bind(&digest).fetch_one(&mut **tx).await?;
        ensure!(
            matches,
            "existing CTV artifact conflicts with verified audit"
        );
    }
    Ok(())
}

pub(super) fn compact_size(bytes: &[u8]) -> Result<(u64, usize)> {
    let Some(&tag) = bytes.first() else {
        bail!("truncated compact size")
    };
    let (size, min) = match tag {
        0..=252 => return Ok((u64::from(tag), 1)),
        253 => (2, 253),
        254 => (4, 65536),
        255 => (8, 4294967296),
    };
    let mut value = [0u8; 8];
    value[..size].copy_from_slice(bytes.get(1..1 + size).context("truncated compact size")?);
    let value = u64::from_le_bytes(value);
    ensure!(value >= min, "non-canonical compact size");
    Ok((value, size + 1))
}
