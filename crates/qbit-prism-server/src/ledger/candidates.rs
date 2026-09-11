use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Candidate {
    pub block_hash: String,
    pub block_hex: String,
    pub job_id: String,
    pub payout_revision: i64,
    pub bundle: AuditBundle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coinbase_suffix_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferred_share: Option<AcceptedShare>,
}

#[derive(Clone, Debug)]
pub struct CandidateClaim {
    pub candidate: Candidate,
    pub claim_token: String,
}

impl Ledger {
    pub async fn enqueue_candidate(&self, candidate: Candidate) -> Result<()> {
        self.enqueue_candidate_once(candidate).await.map(|_| ())
    }

    pub async fn enqueue_candidate_once(&self, candidate: Candidate) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE block_hash=$1)",
        )
        .bind(&candidate.block_hash)
        .fetch_one(&mut *tx)
        .await?;
        if exists {
            tx.commit().await?;
            return Ok(false);
        }
        let inserted = persist_candidate(&mut tx, &candidate, None).await?;
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn claim_candidate(&self, lease_seconds: i64) -> Result<Option<CandidateClaim>> {
        ensure!(
            (1..=600).contains(&lease_seconds),
            "invalid candidate lease duration"
        );
        let token = Uuid::new_v4().to_string();
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        // Empty polling does not use a scheduling slot. A racing SKIP LOCKED
        // selection can still leave a gap; this weighting is deliberately an
        // approximate service ratio rather than a global serialization point.
        let slot: Option<i64> = sqlx::query_scalar("SELECT nextval('qbit_prism_candidate_dispatch_sequence') WHERE EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE state='pending' AND next_attempt_at<=clock_timestamp() AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()))")
            .fetch_optional(&mut *tx).await?;
        let mut row = None;
        if let Some(slot) = slot {
            if slot % 8 != 0 {
                row = claim_candidate_lane(&mut tx, true, &token, &self.instance_id, lease_seconds)
                    .await?;
            }
            if row.is_none() {
                row =
                    claim_candidate_lane(&mut tx, false, &token, &self.instance_id, lease_seconds)
                        .await?;
            }
        }
        // Decode by storage version. A v1 row carries its JSONB candidate; a
        // #258 v2 row carries NULL and a chunk body this server does not
        // import, and a later version is unknown. Those are parked inside the
        // claim transaction so the due lane never offers them again.
        let mut claimed = None;
        if let Some(row) = row {
            let block_hash: String = row.try_get("block_hash")?;
            let storage_version: i32 = row.try_get("storage_version")?;
            let candidate: Option<Value> = row.try_get("candidate")?;
            match (storage_version, candidate) {
                (1, Some(candidate)) => {
                    claimed = Some((candidate, row.try_get::<String, _>("candidate_sha256")?));
                }
                (version, candidate) => {
                    let reason = if version == 1 {
                        "pending storage_version 1 candidate has no JSONB body".to_owned()
                    } else {
                        format!("candidate storage_version {version} is not supported by this server; only version 1 JSONB candidates are (a #258 chunked body must be drained by the 2.x.x release)")
                    };
                    park_candidate(&mut tx, &block_hash, &token, &reason).await?;
                    tracing::warn!(block=%block_hash, storage_version=version, has_body=candidate.is_some(), "parked a candidate this server cannot decode; operator action required");
                }
            }
        }
        tx.commit().await?;
        claimed
            .map(|(candidate, digest)| {
                let candidate: Candidate =
                    serde_json::from_value(candidate).context("invalid persisted candidate")?;
                ensure!(
                    hex::encode(Sha256::digest(serde_json::to_vec(&candidate)?)) == digest,
                    "persisted candidate digest mismatch"
                );
                Ok(CandidateClaim {
                    candidate,
                    claim_token: token,
                })
            })
            .transpose()
    }

    /// Keep a live processing attempt owned while it waits for build capacity
    /// or performs expensive verification. Expired tokens never revive.
    pub async fn renew_candidate_claim(
        &self,
        claim: &CandidateClaim,
        lease_seconds: i64,
    ) -> Result<()> {
        ensure!(
            (1..=600).contains(&lease_seconds),
            "invalid candidate lease duration"
        );
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        // Evaluate expiry after obtaining the row lock: a blocked UPDATE can
        // otherwise have matched a live token before waiting past its expiry.
        // NO KEY UPDATE is compatible with the processing transaction's KEY
        // SHARE lock, so audit persistence cannot block its own heartbeat.
        sqlx::query("SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR NO KEY UPDATE")
            .bind(&claim.candidate.block_hash).fetch_optional(&mut *tx).await?;
        let updated = sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()+$3*interval '1 second',updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state='pending' AND claim_expires_at>clock_timestamp()")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(lease_seconds).execute(&mut *tx).await?.rows_affected();
        ensure!(updated == 1, "candidate claim was lost or expired");
        tx.commit().await?;
        Ok(())
    }

    pub async fn retry_candidate(&self, claim: &CandidateClaim, error: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        lock_candidate_row(&mut tx, claim).await?;
        let result = sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,last_error=$3,next_attempt_at=clock_timestamp()+LEAST(60,attempt_count)*interval '1 second',updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state='pending' AND claim_expires_at>clock_timestamp()")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(error).execute(&mut *tx).await?;
        ensure!(
            result.rows_affected() == 1,
            "candidate claim was lost or expired"
        );
        tx.commit().await?;
        Ok(())
    }

    pub async fn candidate_revision_valid(&self, candidate: &Candidate) -> Result<bool> {
        Ok(candidate.payout_revision == self.payout_revision().await?)
    }
}

async fn lock_candidate_row(
    tx: &mut Transaction<'_, Postgres>,
    claim: &CandidateClaim,
) -> Result<()> {
    sqlx::query(
        "SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR UPDATE",
    )
    .bind(&claim.candidate.block_hash)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(())
}

async fn claim_candidate_lane(
    tx: &mut Transaction<'_, Postgres>,
    fresh: bool,
    token: &str,
    instance_id: &str,
    lease_seconds: i64,
) -> Result<Option<PgRow>> {
    let ordering = if fresh {
        "AND attempt_count=0 ORDER BY created_at DESC,block_hash"
    } else {
        // Includes never-attempted rows: continuous new work must not strand
        // an older candidate that has not yet received its first attempt.
        "ORDER BY next_attempt_at,created_at,block_hash"
    };
    let query = format!("WITH next AS (SELECT block_hash FROM qbit_block_candidate_outbox WHERE state='pending' AND next_attempt_at<=clock_timestamp() AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()) {ordering} FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE qbit_block_candidate_outbox o SET claim_token=$1,claim_instance_id=$2,claim_expires_at=clock_timestamp()+$3*interval '1 second',attempt_count=attempt_count+1,updated_at=clock_timestamp() FROM next WHERE o.block_hash=next.block_hash RETURNING o.block_hash,o.storage_version,o.candidate,o.candidate_sha256");
    Ok(sqlx::query(&query)
        .bind(token)
        .bind(instance_id)
        .bind(lease_seconds)
        .fetch_optional(&mut **tx)
        .await?)
}

/// Park a claimed row this server cannot decode: release the claim, record
/// why in `last_error`, and move `next_attempt_at` past every lease expiry.
/// The row stays `pending` with its body untouched, so a release that reads
/// it can pick it up by resetting `next_attempt_at`; until then it is
/// operator work, not a retry loop.
async fn park_candidate(
    tx: &mut Transaction<'_, Postgres>,
    block_hash: &str,
    token: &str,
    reason: &str,
) -> Result<()> {
    let parked = sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,last_error=$3,next_attempt_at='infinity',updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state='pending'")
        .bind(block_hash).bind(token).bind(reason).execute(&mut **tx).await?.rows_affected();
    ensure!(parked == 1, "candidate to park was not held by this claim");
    Ok(())
}

pub(super) async fn persist_candidate(
    tx: &mut Transaction<'_, Postgres>,
    candidate: &Candidate,
    share_id: Option<&str>,
) -> Result<bool> {
    let header = hex::decode(&candidate.block_hex)?;
    ensure!(header.len() > 80, "candidate block is truncated");
    let mut hash = Sha256::digest(Sha256::digest(&header[..80])).to_vec();
    hash.reverse();
    ensure!(
        hex::encode(hash) == candidate.block_hash,
        "candidate header hash mismatch"
    );
    let payload = serde_json::to_value(candidate)?;
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(candidate)?));
    let inserted = sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,share_id,candidate,candidate_sha256) VALUES($1,$2,$3,$4) ON CONFLICT(block_hash) DO NOTHING")
        .bind(&candidate.block_hash).bind(share_id).bind(payload).bind(&digest).execute(&mut **tx).await?.rows_affected();
    if inserted == 0 {
        let same: bool = sqlx::query_scalar("SELECT candidate_sha256=$2 AND share_id IS NOT DISTINCT FROM $3 FROM qbit_block_candidate_outbox WHERE block_hash=$1").bind(&candidate.block_hash).bind(&digest).bind(share_id).fetch_one(&mut **tx).await?;
        ensure!(same, "candidate identity conflict");
    }
    if let Some(share) = &candidate.deferred_share {
        let payload = serde_json::to_value(share)?;
        let digest = hex::encode(Sha256::digest(serde_json::to_vec(share)?));
        sqlx::query("INSERT INTO qbit_prism_deferred_shares(block_hash,share,share_sha256) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
            .bind(&candidate.block_hash).bind(&payload).bind(&digest).execute(&mut **tx).await?;
        let same:bool = sqlx::query_scalar("SELECT share=$2 AND share_sha256=$3 FROM qbit_prism_deferred_shares WHERE block_hash=$1")
            .bind(&candidate.block_hash).bind(payload).bind(digest).fetch_one(&mut **tx).await?;
        ensure!(same, "deferred share identity conflict");
    }
    Ok(inserted == 1)
}
