use super::*;

/// Immutable identity of the shared record referenced by an issued job.
#[derive(Clone, Copy, Debug)]
pub struct PreparedDependency<'a> {
    pub key: &'a str,
    pub original_revision: i64,
    pub parent: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IssuedJobSave {
    Saved,
    PreparedMissing,
}

// Renewal is infrequent even when many clients receive the same prepared work.
const DEPENDENCY_HEADROOM_MS: i64 = 60_000;

impl Ledger {
    pub async fn save_job(
        &self,
        job_id: &str,
        payload: &Value,
        payout_revision: i64,
        parent_hash: &str,
        ttl_seconds: i64,
    ) -> Result<()> {
        ensure!(ttl_seconds > 0, "job TTL must be positive");
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        let revision: i64 =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            revision == payout_revision,
            "payout revision changed during job construction"
        );
        let inserted = sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at) VALUES($1,$2,$3,$4,$5,clock_timestamp()+$6*interval '1 second') ON CONFLICT DO NOTHING")
            .bind(job_id).bind(&self.instance_id).bind(parent_hash).bind(payout_revision).bind(payload).bind(ttl_seconds).execute(&mut *tx).await?.rows_affected();
        if inserted == 0 {
            let same: bool = sqlx::query_scalar("SELECT payload=$2 AND parent_hash=$3 AND payout_revision=$4 FROM qbit_prism_jobs WHERE job_id=$1").bind(job_id).bind(payload).bind(parent_hash).bind(payout_revision).fetch_one(&mut *tx).await?;
            ensure!(same, "immutable job ID conflict");
        }
        tx.commit().await?;
        Ok(())
    }

    /// Save compact miner work only while its immutable prepared dependency is
    /// durable through the same absolute deadline. Repair is a cold-path retry;
    /// the expected current revision never replaces the dependency's revision.
    pub async fn save_issued_job(
        &self,
        job_id: &str,
        payload: &Value,
        expected_current_revision: i64,
        parent_hash: &str,
        expires_at_ms: i64,
        dependency: PreparedDependency<'_>,
        repair_payload: Option<&Value>,
    ) -> Result<IssuedJobSave> {
        ensure!(
            !job_id.is_empty() && !dependency.key.is_empty() && job_id != dependency.key,
            "invalid issued job dependency"
        );
        ensure!(
            payload["prepared_key"].as_str() == Some(dependency.key)
                && payload["expires_at_ms"].as_i64() == Some(expires_at_ms)
                && dependency.parent == parent_hash,
            "issued job dependency or deadline mismatch"
        );
        if let Some(repair) = repair_payload {
            ensure!(
                repair["snapshot"]["payout_revision"].as_i64()
                    == Some(dependency.original_revision)
                    && repair["template"]["previousblockhash"].as_str() == Some(dependency.parent),
                "prepared repair identity mismatch"
            );
        }
        let expires = DateTime::<Utc>::from_timestamp_millis(expires_at_ms)
            .context("issued job expiry out of range")?;
        let renewed = expires_at_ms
            .checked_add(DEPENDENCY_HEADROOM_MS)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .context("prepared dependency expiry overflow")?;
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        require_revision(&mut tx, expected_current_revision).await?;
        let live: bool = sqlx::query_scalar("SELECT $1::timestamptz > clock_timestamp()")
            .bind(expires)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(live, "issued job deadline elapsed");

        // KEY SHARE prevents concurrent GC from deleting the dependency. The
        // normal path reads metadata only, not the potentially large payload.
        let mut row = dependency_row(&mut tx, dependency.key).await?;
        if row.is_none() {
            let Some(repair) = repair_payload else {
                // The caller may now wait for CPU capacity or serialize a
                // large record. Release transaction locks before that work.
                tx.rollback().await?;
                return Ok(IssuedJobSave::PreparedMissing);
            };
            sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING")
                .bind(dependency.key).bind(&self.instance_id).bind(dependency.parent)
                .bind(dependency.original_revision).bind(repair).bind(renewed)
                .execute(&mut *tx).await?;
            row = dependency_row(&mut tx, dependency.key).await?;
        }
        let row = row.context("prepared dependency disappeared")?;
        ensure!(
            row.try_get::<String, _>("parent_hash")? == dependency.parent
                && row.try_get::<i64, _>("payout_revision")? == dependency.original_revision,
            "immutable prepared dependency conflict"
        );
        if let Some(repair) = repair_payload {
            let same: bool =
                sqlx::query_scalar("SELECT payload=$2 FROM qbit_prism_jobs WHERE job_id=$1")
                    .bind(dependency.key)
                    .bind(repair)
                    .fetch_one(&mut *tx)
                    .await?;
            ensure!(same, "immutable prepared dependency conflict");
        }
        if row.try_get::<DateTime<Utc>, _>("expires_at")? < expires {
            sqlx::query("UPDATE qbit_prism_jobs SET expires_at=GREATEST(expires_at,$2) WHERE job_id=$1 AND expires_at<$3")
                .bind(dependency.key).bind(renewed).bind(expires).execute(&mut *tx).await?;
        }
        let inserted = sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING")
            .bind(job_id).bind(&self.instance_id).bind(parent_hash)
            .bind(expected_current_revision).bind(payload).bind(expires)
            .execute(&mut *tx).await?.rows_affected();
        if inserted == 0 {
            let same: bool = sqlx::query_scalar("SELECT payload=$2 AND parent_hash=$3 AND payout_revision=$4 AND expires_at=$5 FROM qbit_prism_jobs WHERE job_id=$1")
                .bind(job_id).bind(payload).bind(parent_hash).bind(expected_current_revision)
                .bind(expires).fetch_one(&mut *tx).await?;
            ensure!(same, "immutable job ID conflict");
        }
        // Reject a deadline exhausted while waiting for row locks; retries cannot reset it.
        let live: bool = sqlx::query_scalar("SELECT $1::timestamptz > clock_timestamp()")
            .bind(expires)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(live, "issued job deadline elapsed");
        tx.commit().await?;
        Ok(IssuedJobSave::Saved)
    }

    pub async fn job(&self, job_id: &str) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT payload FROM qbit_prism_jobs WHERE job_id=$1 AND expires_at>clock_timestamp()",
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn prune_expired_jobs(&self) -> Result<u64> {
        // A candidate ID may have been selected before renewal committed. The
        // outer predicate is rechecked after DELETE waits for its row lock.
        Ok(sqlx::query("DELETE FROM qbit_prism_jobs WHERE job_id IN (SELECT job_id FROM qbit_prism_jobs WHERE expires_at < clock_timestamp() ORDER BY expires_at LIMIT 4096) AND expires_at < clock_timestamp()").execute(&self.pool).await?.rows_affected())
    }
}

async fn dependency_row(tx: &mut Transaction<'_, Postgres>, key: &str) -> Result<Option<PgRow>> {
    Ok(sqlx::query("SELECT parent_hash,payout_revision,expires_at FROM qbit_prism_jobs WHERE job_id=$1 FOR KEY SHARE")
        .bind(key).fetch_optional(&mut **tx).await?)
}
