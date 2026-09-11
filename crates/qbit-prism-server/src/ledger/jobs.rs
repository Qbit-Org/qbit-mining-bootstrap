use super::*;

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

    pub async fn job(&self, job_id: &str) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT payload FROM qbit_prism_jobs WHERE job_id=$1 AND expires_at>clock_timestamp()",
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn prune_expired_jobs(&self) -> Result<u64> {
        Ok(sqlx::query("DELETE FROM qbit_prism_jobs WHERE job_id IN (SELECT job_id FROM qbit_prism_jobs WHERE expires_at < clock_timestamp() ORDER BY expires_at LIMIT 4096)").execute(&self.pool).await?.rows_affected())
    }
}
