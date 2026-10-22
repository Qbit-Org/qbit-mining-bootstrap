//! Shared operational vardiff hints. These rows never determine share credit.
use super::*;

#[derive(Clone, Debug)]
pub struct WorkerDifficulty {
    pub difficulty: f64,
    pub evidence_at_ms: i64,
    pub age_ms: u64,
}

fn valid_difficulty(value: f64) -> Result<()> {
    ensure!(
        value.is_finite() && value > 0.0,
        "worker difficulty must be positive and finite"
    );
    Ok(())
}

impl Ledger {
    pub async fn worker_difficulty(
        &self,
        listener: &str,
        username: &str,
        ttl_seconds: u64,
    ) -> Result<Option<WorkerDifficulty>> {
        let row = sqlx::query("SELECT difficulty::text AS difficulty,floor(extract(epoch FROM evidence_at)*1000)::bigint AS evidence_at_ms,greatest(0,floor(extract(epoch FROM clock_timestamp()-evidence_at)*1000))::bigint AS age_ms FROM qbit_worker_difficulty WHERE listener=$1 AND worker_username=$2 AND evidence_at>=clock_timestamp()-$3::double precision*interval '1 second'")
            .bind(listener).bind(username).bind(ttl_seconds as f64).fetch_optional(&mut *self.acquire().await?).await?;
        let Some(row) = row else { return Ok(None) };
        let difficulty: f64 = row.try_get::<String, _>("difficulty")?.parse()?;
        // A legacy numeric outside the wire format is unusable as a hint.
        if valid_difficulty(difficulty).is_err() {
            return Ok(None);
        }
        Ok(Some(WorkerDifficulty {
            difficulty,
            evidence_at_ms: row.try_get("evidence_at_ms")?,
            age_ms: u64::try_from(row.try_get::<i64, _>("age_ms")?)?,
        }))
    }

    pub async fn record_worker_difficulty(
        &self,
        listener: &str,
        username: &str,
        difficulty: f64,
        evidence_at_ms: i64,
    ) -> Result<bool> {
        valid_difficulty(difficulty)?;
        ensure!(
            !listener.is_empty() && !username.is_empty(),
            "worker difficulty identity must not be empty"
        );
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let changed = sqlx::query("INSERT INTO qbit_worker_difficulty(listener,worker_username,difficulty,evidence_at) VALUES($1,$2,$3::text::numeric,to_timestamp($4::bigint::double precision/1000)) ON CONFLICT(listener,worker_username) DO UPDATE SET difficulty=EXCLUDED.difficulty,evidence_at=EXCLUDED.evidence_at,updated_at=clock_timestamp() WHERE qbit_worker_difficulty.evidence_at<=EXCLUDED.evidence_at")
            .bind(listener).bind(username).bind(difficulty.to_string()).bind(evidence_at_ms).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(changed != 0)
    }

    pub async fn lower_worker_difficulty(
        &self,
        listener: &str,
        username: &str,
        difficulty: f64,
    ) -> Result<bool> {
        valid_difficulty(difficulty)?;
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let changed = sqlx::query("UPDATE qbit_worker_difficulty SET difficulty=$3::text::numeric,updated_at=clock_timestamp() WHERE listener=$1 AND worker_username=$2 AND difficulty>$3::text::numeric")
            .bind(listener).bind(username).bind(difficulty.to_string()).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(changed != 0)
    }

    pub async fn prune_worker_difficulties(&self, ttl_seconds: u64, limit: u32) -> Result<u64> {
        ensure!(
            (1..=10000).contains(&limit),
            "worker difficulty prune limit must be 1..10000"
        );
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let changed = sqlx::query("WITH expired AS (SELECT listener,worker_username FROM qbit_worker_difficulty WHERE evidence_at<clock_timestamp()-$1::double precision*interval '1 second' ORDER BY evidence_at,listener,worker_username FOR UPDATE SKIP LOCKED LIMIT $2) DELETE FROM qbit_worker_difficulty d USING expired e WHERE d.listener=e.listener AND d.worker_username=e.worker_username")
            .bind(ttl_seconds as f64).bind(i64::from(limit)).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(changed)
    }

    /// When the ledger accepted `share_id`, or `None` if it holds no such
    /// accepted row.
    ///
    /// The ledger is partitioned by `share_seq` (migration 017) and has no
    /// global `share_id` index, so an unbounded probe descends one per-leaf
    /// index per attached partition. Vardiff evidence is a share the session
    /// submitted moments ago, so the bounded probe
    /// (`qbit_prism_share_probe_floor()`, the lower bound of the partition
    /// two below the one the next `share_seq` lands in) answers it from at
    /// most three leaves holding rows. A miss is not an
    /// answer: the caller uses this timestamp to decide whether a retained
    /// difficulty may be resumed, so an older but still online row must be
    /// found rather than silently reported as absent. The second probe runs
    /// unbounded, and is reached only when the cheap one found nothing.
    pub async fn share_accepted_at_ms(&self, share_id: &str) -> Result<Option<i64>> {
        let mut connection = self.acquire().await?;
        let accepted_at: Option<i64> = sqlx::query_scalar("SELECT floor(extract(epoch FROM accepted_at)*1000)::bigint FROM qbit_share_ledger WHERE share_id=$1 AND accepted AND share_seq>=qbit_prism_share_probe_floor()")
            .bind(share_id).fetch_optional(&mut *connection).await?;
        if accepted_at.is_some() {
            return Ok(accepted_at);
        }
        Ok(sqlx::query_scalar("SELECT floor(extract(epoch FROM accepted_at)*1000)::bigint FROM qbit_share_ledger WHERE share_id=$1 AND accepted")
            .bind(share_id).fetch_optional(&mut *connection).await?)
    }
}
