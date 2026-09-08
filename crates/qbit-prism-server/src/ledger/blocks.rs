use super::audit::persist_audit_snapshot;
use super::*;
use qbit_prism::{verify_audit_bundle_with_ledger_public_key, AuditVerificationReport};

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
        let bundle = claim.candidate.bundle.clone();
        let public_key = ledger_public_key.to_owned();
        let report = tokio::task::spawn_blocking(move || {
            verify_audit_bundle_with_ledger_public_key(&bundle, &public_key)
        })
        .await??;
        let block = hex::decode(&claim.candidate.block_hex)?;
        ensure!(block.len() > 80, "candidate block is truncated");
        // The durable serialized candidate already authenticates its header.
        // Compact bits belong to block metadata; adding them to FoundBlock
        // would change the signed canonical audit format and historical hashes.
        let bits = format!(
            "{:08x}",
            u32::from_le_bytes(block[72..76].try_into().expect("validated header length"))
        );
        let (tx_count, count_bytes) = compact_size(&block[80..])?;
        ensure!(tx_count > 0, "candidate has no coinbase");
        let coinbase = hex::decode(&report.coinbase_tx_hex)?;
        ensure!(
            block.get(80 + count_bytes..80 + count_bytes + coinbase.len())
                == Some(coinbase.as_slice()),
            "candidate coinbase differs from verified audit"
        );
        let mut parent = block[4..36].to_vec();
        parent.reverse();
        let parent_hash = hex::encode(parent);
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        require_claim(&mut tx, claim).await?;
        if let Some(expected) = expected_revision {
            require_revision(&mut tx, expected).await?;
        }
        let existing: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT audit_bundle_sha256,found_block_bits FROM qbit_pool_audit_bundles WHERE block_hash=$1",
        )
        .bind(&claim.candidate.block_hash)
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
                // serialized candidate even though no extra bits field existed.
                sqlx::query("UPDATE qbit_pool_audit_bundles SET found_block_bits=$2 WHERE block_hash=$1 AND found_block_bits IS NULL")
                    .bind(&claim.candidate.block_hash).bind(&bits).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            return Ok(report);
        }
        let revision: i64 =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            revision == expected_revision.unwrap_or(claim.candidate.payout_revision),
            "candidate payout revision was superseded"
        );
        let prior = read_prior_balances(&mut tx).await?;
        ensure!(
            prior == claim.candidate.bundle.prior_balances,
            "candidate prior balances differ from current canonical balances"
        );
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256) VALUES($1,$2,$3,$4,$5)")
            .bind(&claim.candidate.block_hash).bind(i64::try_from(report.block_height)?).bind(parent_hash).bind(&report.coinbase_txid).bind(&report.coinbase_manifest_sha256_hex).execute(&mut *tx).await?;
        let snapshot_digest = persist_audit_snapshot(&mut tx, &claim.candidate.bundle).await?;
        let mut bundle_value = serde_json::to_value(&claim.candidate.bundle)?;
        bundle_value
            .as_object_mut()
            .context("audit bundle is not an object")?
            .remove("shares");
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,audit_body_byte_len,schema_version,found_block_network_difficulty,found_block_coinbase_value_sats,audit_commitment_leaves_hex,witness_merkle_leaves_hex,share_snapshot_sha256,found_block_bits) VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric,$8,$9,$10,$11,$12)")
            .bind(&claim.candidate.block_hash).bind(&bundle_value).bind(&report.audit_bundle_sha256_hex).bind(&report.coinbase_tx_hex)
            .bind(i64::try_from(qbit_prism::canonical_audit_bundle_bytes(&claim.candidate.bundle)?.len())?).bind(&claim.candidate.bundle.schema)
            .bind(claim.candidate.bundle.found_block.network_difficulty.to_string()).bind(i64::try_from(report.coinbase_value_sats)?)
            .bind(serde_json::to_value(&claim.candidate.bundle.audit_commitment_leaves_hex)?).bind(serde_json::to_value(&claim.candidate.bundle.witness_merkle_leaves_hex)?).bind(snapshot_digest).bind(&bits).execute(&mut *tx).await?;
        let accounts =
            serde_json::to_value(&claim.candidate.bundle.payout_policy_manifest.accounts)?;
        sqlx::query("INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) SELECT $1,$2,a->>'recipient_id',a->>'order_key',decode(a->>'p2mr_program_hex','hex'),(a->>'onchain_amount_sats')::bigint,(a->>'carry_forward_balance_sats')::numeric,a->>'action' FROM jsonb_array_elements($3::jsonb) a")
            .bind(&claim.candidate.block_hash).bind(i64::try_from(report.block_height)?).bind(&accounts).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO qbit_payout_carry_forward(block_hash,block_height,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,settlement_fee_sats,carry_forward_balance_sats,action) SELECT $1,$2,a->>'recipient_id',a->>'order_key',decode(a->>'p2mr_program_hex','hex'),(a->>'gross_amount_sats')::bigint,(a->>'prior_balance_sats')::numeric,(a->>'candidate_balance_sats')::numeric,(a->>'onchain_amount_sats')::bigint,COALESCE((a->>'settlement_fee_sats')::bigint,0),(a->>'carry_forward_balance_sats')::numeric,a->>'action' FROM jsonb_array_elements($3::jsonb) a WHERE COALESCE(a->>'account_type','miner')='miner'")
            .bind(&claim.candidate.block_hash).bind(i64::try_from(report.block_height)?).bind(&accounts).execute(&mut *tx).await?;
        if let Some(set) = &claim.candidate.bundle.ctv_fanout_manifest_set {
            persist_fanouts(&mut tx, &claim.candidate.block_hash, set).await?;
        }
        tx.commit().await?;
        Ok(report)
    }

    /// `submitted` means the caller proved this block is on the active chain.
    /// A successful submitblock RPC alone is insufficient. Ambiguous network
    /// errors use retry_candidate and retain every recovery artifact.
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
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let revision: i64 =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            revision == expected_revision,
            "payout revision changed while observing candidate disposition"
        );
        require_claim(&mut tx, claim).await?;
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
            let mature:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_pool_blocks WHERE block_hash=$1 AND maturity_state='mature')").bind(&claim.candidate.block_hash).fetch_one(&mut *tx).await?;
            ensure!(!mature, "cannot abandon a mature candidate");
            let changed = sqlx::query("UPDATE qbit_pool_blocks SET chain_state='inactive',inactive_since=CASE WHEN chain_state='confirmed' THEN clock_timestamp() ELSE inactive_since END WHERE block_hash=$1 AND chain_state IN ('prepared','confirmed') AND maturity_state='immature'").bind(&claim.candidate.block_hash).execute(&mut *tx).await?.rows_affected();
            if changed > 0 {
                sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='reorged',claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,updated_at=clock_timestamp() WHERE block_hash=$1").bind(&claim.candidate.block_hash).execute(&mut *tx).await?;
                bump_revision(&mut tx).await?;
            }
        }
        sqlx::query("UPDATE qbit_block_candidate_outbox SET state=$3,candidate=NULL,completed_at=clock_timestamp(),updated_at=clock_timestamp(),last_error=$4,claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL WHERE block_hash=$1 AND claim_token=$2")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(if submitted {"submitted"} else {"abandoned"}).bind(error).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn pool_blocks_for_reconcile(&self) -> Result<Vec<PoolBlock>> {
        sqlx::query("SELECT block_hash,block_height,chain_state,maturity_state FROM qbit_pool_blocks WHERE (maturity_state='immature' AND chain_state IN ('prepared','confirmed','inactive')) OR block_hash=(SELECT block_hash FROM qbit_pool_blocks WHERE chain_state='confirmed' AND maturity_state='mature' ORDER BY block_height DESC,block_hash DESC LIMIT 1) ORDER BY block_height,block_hash")
            .fetch_all(&self.pool).await?.into_iter().map(|row| Ok(PoolBlock {
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
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let revision: i64 =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            revision == expected_revision,
            "payout revision changed while collecting chain observations"
        );
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
        let rows = sqlx::query("SELECT block_hash,block_height,chain_state,maturity_state FROM qbit_pool_blocks WHERE chain_state IN ('prepared','confirmed','inactive') AND block_hash=ANY($1::text[]) ORDER BY block_height,block_hash FOR UPDATE").bind(&hashes).fetch_all(&mut *tx).await?;
        for row in rows {
            let hash: String = row.try_get("block_hash")?;
            let Some(&active) = observed.get(hash.as_str()) else {
                continue;
            };
            let state: String = row.try_get("chain_state")?;
            let maturity: String = row.try_get("maturity_state")?;
            if !active && maturity == "mature" {
                let message = format!(
                    "mature pool block disconnected: {hash}; manual reconciliation required"
                );
                sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=$1,updated_at=clock_timestamp() WHERE singleton").bind(&message).execute(&mut *tx).await?;
                tx.commit().await?;
                bail!(message);
            }
            if active && state != "confirmed" {
                sqlx::query(
                    "UPDATE qbit_pool_blocks SET chain_state='confirmed',inactive_since=NULL WHERE block_hash=$1",
                )
                .bind(&hash)
                .execute(&mut *tx)
                .await?;
                // A crash may happen after submitblock but before the durable
                // ACK. Reconciliation must credit its deferred share as part
                // of the same confirmation transaction.
                self.credit_deferred_share(&mut tx, &hash).await?;
                sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='awaiting_maturity',claim_token=NULL,claim_expires_at=NULL,updated_at=clock_timestamp() WHERE block_hash=$1 AND settlement_status='reorged'").bind(&hash).execute(&mut *tx).await?;
                changed = true;
            } else if !active && state == "confirmed" {
                sqlx::query(
                    "UPDATE qbit_pool_blocks SET chain_state='inactive',inactive_since=clock_timestamp() WHERE block_hash=$1",
                )
                .bind(&hash)
                .execute(&mut *tx)
                .await?;
                sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='reorged',claim_token=NULL,claim_expires_at=NULL,updated_at=clock_timestamp() WHERE block_hash=$1").bind(&hash).execute(&mut *tx).await?;
                changed = true;
            }
        }
        if changed {
            bump_revision(&mut tx).await?;
        }
        let matured: i32 = sqlx::query_scalar("SELECT qbit_mark_mature_pool_payouts($1)")
            .bind(i64::try_from(tip_height)?)
            .fetch_one(&mut *tx)
            .await?;
        if matured > 0 {
            bump_revision(&mut tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn claim_fanout(&self, lease_seconds: i64) -> Result<Option<FanoutClaim>> {
        ensure!(lease_seconds > 0, "claim duration must be positive");
        let mut tx = self.pool.begin().await?;
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
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
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

async fn require_claim(tx: &mut Transaction<'_, Postgres>, claim: &CandidateClaim) -> Result<()> {
    let valid: Option<bool> = sqlx::query_scalar("SELECT claim_token=$2 AND claim_expires_at>clock_timestamp() AND state='pending' FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR UPDATE")
        .bind(&claim.candidate.block_hash).bind(&claim.claim_token).fetch_optional(&mut **tx).await?;
    ensure!(valid == Some(true), "candidate claim was lost or expired");
    Ok(())
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

fn compact_size(bytes: &[u8]) -> Result<(u64, usize)> {
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
