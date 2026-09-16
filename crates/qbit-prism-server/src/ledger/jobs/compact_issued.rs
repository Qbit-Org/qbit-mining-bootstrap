//! Atomic storage for issued work referencing compact prepared records.
//!
//! This sibling does not activate a runtime caller or change the generic inline
//! APIs. Before activation, drain with compatible pre-007 frontends, stop ALL
//! frontends, verify zero pending candidates, apply the coordinated migrations,
//! then start compatible binaries. This does not support rolling old/new writers.
//! Original reservation expiry is immutable identity, retention is durability,
//! and the issued child's absolute deadline is its separate authority boundary.
use super::*;

/// Original identity carried by the caller from its prepared reservation.
/// Current revision and publication/lease eligibility are separate caller
/// decisions and must be revalidated after waits before issuing miner work.
#[derive(Clone, Copy, Debug)]
pub struct CompactDependency<'a> {
    pub key: &'a str,
    pub original_revision: i64,
    pub parent: &'a str,
    pub original_expires_at_ms: i64,
    pub template_sha256: &'a str,
    pub prior_balances_digest: [u8; 32],
}

/// Exact original inputs encoded before acquiring transaction locks.
///
/// Encoding/cloning is whole-template and whole-balance work. The future caller
/// must create this on its admitted blocking build, keeping owned admission and
/// repair guards alive through that work even when the async waiter is cancelled.
/// No runtime caller is enabled here. Once the prepared row is gone, its absent
/// metadata cannot be recovered from blob digests: the caller must retain the
/// exact original record and reservation expiry, not reconstruct current inputs.
#[derive(Clone, Debug)]
pub struct CompactRepair {
    record: CompactPrepared,
    template: PreparedTemplate,
    payload: Value,
    balance_bytes: Vec<u8>,
    original_expires_at_ms: i64,
}

impl CompactRepair {
    #[cfg(test)]
    pub(crate) fn observation_for_test(&self) -> Result<StoredCompactPrepared> {
        Ok(StoredCompactPrepared {
            record: self.record.clone(),
            template: self.template.value_for_test()?,
            prior_balances: serde_json::from_slice(&self.balance_bytes)?,
            original_expires_at_ms: self.original_expires_at_ms,
            expires_at_ms: self.original_expires_at_ms,
        })
    }

    pub fn encode(
        record: &CompactPrepared,
        template: &PreparedTemplate,
        balances: &[CarryForwardBalance],
        original_expires_at_ms: i64,
    ) -> Result<Self> {
        record.validate()?;
        DateTime::<Utc>::from_timestamp_millis(original_expires_at_ms)
            .context("prepared original expiry out of range")?;
        ensure!(
            record.template_sha256 == template.digest && record.parent_hash == template.parent,
            "prepared template identity mismatch"
        );
        // Migration 008 and put_balance_snapshot use precisely this bytewise
        // comparator and native JSON encoding. Cross-API fixtures pin parity;
        // this cold encoder keeps all whole-set CPU work outside the transaction.
        let mut balances = balances.to_vec();
        balances.sort_by(|a, b| {
            a.order_key
                .cmp(&b.order_key)
                .then_with(|| a.recipient_id.cmp(&b.recipient_id))
                .then_with(|| a.p2mr_program_hex.cmp(&b.p2mr_program_hex))
        });
        ensure!(
            qbit_prism::prior_balances_digest(&balances) == record.window.prior_balances_digest,
            "prepared balance digest mismatch"
        );
        Ok(Self {
            record: record.clone(),
            template: template.clone(),
            payload: prepared::encode_record(record, original_expires_at_ms)?,
            balance_bytes: serde_json::to_vec(&balances)?,
            original_expires_at_ms,
        })
    }

    pub fn dependency<'a>(&'a self, key: &'a str) -> CompactDependency<'a> {
        CompactDependency {
            key,
            original_revision: self.record.payout_revision,
            parent: &self.record.parent_hash,
            original_expires_at_ms: self.original_expires_at_ms,
            template_sha256: &self.record.template_sha256,
            prior_balances_digest: self.record.window.prior_balances_digest,
        }
    }

    fn check_identity(&self, dependency: CompactDependency<'_>) -> Result<()> {
        let original = self.dependency(dependency.key);
        ensure!(
            original.original_revision == dependency.original_revision
                && original.parent == dependency.parent
                && original.original_expires_at_ms == dependency.original_expires_at_ms
                && original.template_sha256 == dependency.template_sha256
                && original.prior_balances_digest == dependency.prior_balances_digest,
            "prepared repair identity mismatch"
        );
        Ok(())
    }
}

impl Ledger {
    /// Save one issued job and retain or atomically restore its compact parent.
    ///
    /// Only a physically missing prepared RECORD without supplied repair inputs
    /// is PreparedMissing. A surviving record with a missing blob is an error,
    /// even with repair inputs; this API never silently repairs that corruption.
    /// The hot path checks metadata consistency and blob existence, not unseen
    /// blob bytes. Supplied repair inputs additionally compare every payload and
    /// blob byte against survivors; hydration remains responsible for decoding
    /// and authenticating bytes on its read path.
    ///
    /// The original reservation may have elapsed. It stays immutable identity;
    /// only retention may grow to support the SAME absolute child deadline.
    /// A caller retains one outer deadline and revalidates its lease/balance
    /// authority after waits. This transaction fences the current revision and
    /// configuration, introduces no timeout/retry loop, and grants no new lease.
    #[allow(clippy::too_many_arguments)]
    pub async fn save_issued_job_compact(
        &self,
        job_id: &str,
        payload: &Value,
        expected_current_revision: i64,
        parent_hash: &str,
        expires_at_ms: i64,
        dependency: CompactDependency<'_>,
        repair: Option<&CompactRepair>,
    ) -> Result<IssuedJobSave> {
        let original = dependency.validate()?;
        ensure!(
            !job_id.is_empty() && job_id != dependency.key,
            "invalid issued job dependency"
        );
        ensure!(
            payload["prepared_key"].as_str() == Some(dependency.key)
                && payload["expires_at_ms"].as_i64() == Some(expires_at_ms)
                && dependency.parent == parent_hash,
            "issued job dependency or deadline mismatch"
        );
        if let Some(repair) = repair {
            repair.check_identity(dependency)?;
        }
        let expires = DateTime::<Utc>::from_timestamp_millis(expires_at_ms)
            .context("issued job expiry out of range")?;
        let renewed = expires_at_ms
            .checked_add(DEPENDENCY_HEADROOM_MS)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .context("prepared dependency expiry overflow")?;

        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        // Acquire the row fence before reading mutable cluster state: if this
        // waits behind configure/reset, all checks below see its committed state.
        let fingerprint: Option<String> = sqlx::query_scalar(
            "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton FOR SHARE",
        )
        .fetch_one(&mut *tx)
        .await?;
        writable(&mut tx).await?;
        require_revision(&mut tx, expected_current_revision).await?;
        if let Some(pinned) = self.config_fingerprint() {
            ensure!(
                fingerprint.as_deref() == Some(pinned),
                "cluster configuration fingerprint differs from this frontend's pin"
            );
        }
        require_live(&mut tx, expires).await?;

        let mut row = dependency_row(&mut tx, dependency.key).await?;
        let missing_record = row.is_none();
        require_live(&mut tx, expires).await?;
        if let Some(row) = &row {
            dependency.check_row(row)?;
            // These row locks also prevent a delete between the existence check
            // and the optional exact-byte verification below. Future GC still
            // takes SETTLEMENT then ORDER before scanning/deleting references.
            lock_blob_metadata(&mut tx, dependency).await?;
        } else {
            let Some(repair) = repair else {
                tx.rollback().await?;
                return Ok(IssuedJobSave::PreparedMissing);
            };
            if let Some(range) = repair.record.window.shares {
                ensure!(
                    probe_share_rows(
                        &mut tx,
                        range.first_share_seq as i64,
                        range.last_share_seq as i64
                    )
                    .await?,
                    "prepared share endpoint missing"
                );
            }
            prepared::put_template(&mut tx, &repair.template).await?;
            put_balances(&mut tx, repair).await?;
            let record = &repair.record;
            let range = record.window.shares;
            sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at,window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,window_last_share_seq,window_share_count,window_snapshot_sha256,template_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT DO NOTHING")
                .bind(dependency.key).bind(&self.instance_id).bind(&record.parent_hash)
                .bind(record.payout_revision).bind(&repair.payload).bind(original.max(renewed))
                .bind(record.window.anchor_ms).bind(hex::encode(record.window.prior_balances_digest))
                .bind(range.map(|r| r.first_share_seq as i64)).bind(range.map(|r| r.last_share_seq as i64))
                .bind(range.map(|r| r.share_count as i64)).bind(range.map(|r| hex::encode(r.snapshot_sha256)))
                .bind(&record.template_sha256).execute(&mut *tx).await?;
            row = dependency_row(&mut tx, dependency.key).await?;
        }
        let row = row.context("prepared dependency disappeared")?;
        dependency.check_row(&row)?;
        if let Some(repair) = repair {
            let same: bool =
                sqlx::query_scalar("SELECT payload=$2 FROM qbit_prism_jobs WHERE job_id=$1")
                    .bind(dependency.key)
                    .bind(&repair.payload)
                    .fetch_one(&mut *tx)
                    .await?;
            ensure!(same, "immutable compact prepared conflict");
            prepared::check_columns(&row, &repair.record)?;
            if !missing_record {
                let same: bool = sqlx::query_scalar(
                    "SELECT template_bytes=$2 FROM qbit_prism_templates WHERE template_sha256=$1",
                )
                .bind(&repair.template.digest)
                .bind(&repair.template.bytes)
                .fetch_one(&mut *tx)
                .await?;
                ensure!(same, "immutable prepared template conflict");
                check_balances(&mut tx, repair).await?;
            }
        }
        require_live(&mut tx, expires).await?;
        if row.try_get::<DateTime<Utc>, _>("expires_at")? < expires {
            sqlx::query("UPDATE qbit_prism_jobs SET expires_at=GREATEST(expires_at,$2) WHERE job_id=$1 AND expires_at<$3")
                .bind(dependency.key).bind(renewed).bind(expires).execute(&mut *tx).await?;
        }
        let inserted = sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING")
            .bind(job_id).bind(&self.instance_id).bind(parent_hash).bind(expected_current_revision)
            .bind(payload).bind(expires).execute(&mut *tx).await?.rows_affected();
        if inserted == 0 {
            let same: bool = sqlx::query_scalar("SELECT payload=$2 AND parent_hash=$3 AND payout_revision=$4 AND expires_at=$5 AND num_nulls(window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,window_last_share_seq,window_share_count,window_snapshot_sha256,template_sha256)=7 FROM qbit_prism_jobs WHERE job_id=$1")
                .bind(job_id).bind(payload).bind(parent_hash).bind(expected_current_revision)
                .bind(expires).fetch_one(&mut *tx).await?;
            ensure!(same, "immutable job ID conflict");
        }
        require_live(&mut tx, expires).await?;
        tx.commit().await?;
        Ok(IssuedJobSave::Saved)
    }
}

async fn put_balances(tx: &mut Transaction<'_, Postgres>, repair: &CompactRepair) -> Result<()> {
    let digest = hex::encode(repair.record.window.prior_balances_digest);
    let inserted = sqlx::query("INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(&digest).bind(&repair.balance_bytes).execute(&mut **tx).await?.rows_affected();
    if inserted == 0 {
        check_balances(tx, repair).await?;
    }
    Ok(())
}

async fn check_balances(tx: &mut Transaction<'_, Postgres>, repair: &CompactRepair) -> Result<()> {
    let same: bool = sqlx::query_scalar(
        "SELECT balances=$2 FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
    )
    .bind(hex::encode(repair.record.window.prior_balances_digest))
    .bind(&repair.balance_bytes)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(same, "immutable balance snapshot conflict");
    Ok(())
}

impl CompactDependency<'_> {
    fn validate(&self) -> Result<DateTime<Utc>> {
        ensure!(
            !self.key.is_empty() && !self.parent.is_empty() && self.original_revision >= 0,
            "invalid compact prepared dependency"
        );
        prepared::digest_text(self.template_sha256)?;
        DateTime::<Utc>::from_timestamp_millis(self.original_expires_at_ms)
            .context("prepared original expiry out of range")
    }

    fn check_row(&self, row: &PgRow) -> Result<()> {
        let original: Option<Value> = row.try_get("original_expires_at_ms")?;
        ensure!(
            row.try_get::<String, _>("parent_hash")? == self.parent
                && row.try_get::<i64, _>("payout_revision")? == self.original_revision
                && row
                    .try_get::<Option<String>, _>("template_sha256")?
                    .as_deref()
                    == Some(self.template_sha256)
                && row.try_get::<Option<String>, _>("window_prior_balances_sha256")?
                    == Some(hex::encode(self.prior_balances_digest))
                && original.as_ref().and_then(Value::as_i64) == Some(self.original_expires_at_ms),
            "immutable compact prepared dependency conflict"
        );
        ensure!(
            row.try_get::<bool, _>("payload_matches_columns")?,
            "compact prepared payload/column mismatch"
        );
        ensure!(
            row.try_get::<DateTime<Utc>, _>("expires_at")? >= self.validate()?,
            "prepared retention precedes original expiry"
        );
        Ok(())
    }
}

// Return only bounded identity/column metadata and server-side consistency
// results: never transfer the prepared payload, template or balance bytes on
// the hot path. The blob keys' existence is not proof of unseen byte integrity.
async fn dependency_row(tx: &mut Transaction<'_, Postgres>, key: &str) -> Result<Option<PgRow>> {
    Ok(sqlx::query(
        r#"SELECT parent_hash,payout_revision,expires_at,
        window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,
        window_last_share_seq,window_share_count,window_snapshot_sha256,template_sha256,
        payload->'original_expires_at_ms' AS original_expires_at_ms,
        COALESCE(payload->'format_version'=to_jsonb($2::integer)
            AND payload->>'format_version'=($2::integer)::text
            AND payload->'parent_hash'=to_jsonb(parent_hash)
            AND payload->'payout_revision'=to_jsonb(payout_revision)
            AND payload->'template_sha256'=to_jsonb(template_sha256)
            AND payload->'window'=jsonb_build_object(
                'anchor_ms',window_anchor_ms,
                'prior_balances_digest',window_prior_balances_sha256,
                'shares',CASE WHEN window_first_share_seq IS NULL THEN 'null'::jsonb
                    ELSE jsonb_build_object('first_share_seq',window_first_share_seq,
                        'last_share_seq',window_last_share_seq,'share_count',window_share_count,
                        'snapshot_sha256',window_snapshot_sha256) END),false)
            AS payload_matches_columns
        FROM qbit_prism_jobs WHERE job_id=$1 FOR KEY SHARE"#,
    )
    .bind(key)
    .bind(i32::from(CompactPrepared::FORMAT_VERSION))
    .fetch_optional(&mut **tx)
    .await?)
}

async fn lock_blob_metadata(
    tx: &mut Transaction<'_, Postgres>,
    dependency: CompactDependency<'_>,
) -> Result<()> {
    let template: Option<bool> = sqlx::query_scalar(
        "SELECT true FROM qbit_prism_templates WHERE template_sha256=$1 FOR KEY SHARE",
    )
    .bind(dependency.template_sha256)
    .fetch_optional(&mut **tx)
    .await?;
    ensure!(template == Some(true), "prepared template blob missing");
    let balances: Option<bool> = sqlx::query_scalar(
        "SELECT true FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1 FOR KEY SHARE",
    ).bind(hex::encode(dependency.prior_balances_digest)).fetch_optional(&mut **tx).await?;
    ensure!(balances == Some(true), "prepared balance snapshot missing");
    Ok(())
}

async fn require_live(tx: &mut Transaction<'_, Postgres>, expires: DateTime<Utc>) -> Result<()> {
    let live: bool = sqlx::query_scalar("SELECT $1::timestamptz > clock_timestamp()")
        .bind(expires)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(live, "issued job deadline elapsed");
    Ok(())
}
