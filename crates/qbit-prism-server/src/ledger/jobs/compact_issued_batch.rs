//! One bounded hot transaction. Cold repair and prepared reservation stay single.
use super::compact_issued::{dependency_row, lock_blob_metadata, require_live};
use super::*;
use sqlx::{pool::PoolConnection, Acquire, QueryBuilder};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::time::Instant;

/// Small immutable child metadata; never contains a template or payout window.
#[derive(Clone, Debug)]
pub struct CompactIssuedJob {
    pub job_id: String,
    pub payload: Value,
    pub expires_at_ms: i64,
}

/// One attempt's original monotonic deadline and conservative COMMIT marker.
/// A lost acknowledgement must be reconciled using the original child identity.
pub struct CompactBatchAttempt {
    pub deadline: Instant,
    commit_started: AtomicBool,
    cleanup: Arc<BatchCleanup>,
}

impl CompactBatchAttempt {
    pub fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            commit_started: AtomicBool::new(false),
            cleanup: Arc::default(),
        }
    }

    pub fn commit_started(&self) -> bool {
        self.commit_started.load(Ordering::Acquire)
    }

    /// Keep the collector's active slot until cancellation drains the transaction.
    /// This cleanup interval does not extend a caller's original deadline.
    pub async fn wait_for_cleanup(&self) {
        loop {
            let notified = self.cleanup.finished.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.cleanup.pending.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    pub(crate) fn start_commit(&self) -> Result<()> {
        ensure!(
            Instant::now() < self.deadline,
            "issued job deadline elapsed"
        );
        self.commit_started.store(true, Ordering::Release);
        Ok(())
    }
}

#[derive(Default)]
struct BatchCleanup {
    pending: AtomicBool,
    finished: tokio::sync::Notify,
}

impl BatchCleanup {
    fn finish(&self) {
        self.pending.store(false, Ordering::Release);
        self.finished.notify_waiters();
    }
}

// A dropped transaction queues ROLLBACK, but cancellation during BEGIN can
// precede SQLx's tracked transaction depth. Explicitly roll back before allowing
// the next batch. A broken network gets bounded cleanup and discards the connection.
struct BatchConnection {
    connection: Option<PoolConnection<Postgres>>,
    clean: bool,
    cleanup: Arc<BatchCleanup>,
}

impl Drop for BatchConnection {
    fn drop(&mut self) {
        if self.clean {
            self.cleanup.finish();
            return;
        }
        let mut connection = self.connection.take().unwrap();
        let cleanup = self.cleanup.clone();
        tokio::spawn(async move {
            if !matches!(
                tokio::time::timeout(
                    std::time::Duration::from_secs(16),
                    sqlx::Executor::execute(&mut *connection, "ROLLBACK"),
                )
                .await,
                Ok(Ok(_))
            ) {
                connection.close_on_drop();
            }
            drop(connection);
            cleanup.finish();
        });
    }
}

impl Ledger {
    /// Atomically save 1..=64 children sharing the full immutable dependency.
    /// Any conflict fails the whole batch, including retention renewal. No SQL
    /// retry is performed. Cancellation after COMMIT starts has an unknown result.
    pub async fn save_issued_jobs_compact(
        &self,
        jobs: &[CompactIssuedJob],
        expected_current_revision: i64,
        parent_hash: &str,
        dependency: CompactDependency<'_>,
        attempt: &CompactBatchAttempt,
    ) -> Result<IssuedJobSave> {
        tokio::time::timeout_at(attempt.deadline, self.save_compact_batch(
            jobs, expected_current_revision, parent_hash, dependency, attempt,
        )).await.map_err(|_| anyhow::anyhow!(if attempt.commit_started() {
            "compact issued batch deadline elapsed; commit outcome uncertain; reconcile original job identities"
        } else {
            "issued job deadline elapsed"
        }))?
    }

    async fn save_compact_batch(
        &self,
        jobs: &[CompactIssuedJob],
        revision: i64,
        parent: &str,
        dependency: CompactDependency<'_>,
        attempt: &CompactBatchAttempt,
    ) -> Result<IssuedJobSave> {
        ensure!(
            !jobs.is_empty() && jobs.len() <= 64,
            "invalid compact issued batch size"
        );
        dependency.validate()?;
        let mut unique = std::collections::BTreeMap::new();
        let mut min_expiry = i64::MAX;
        let mut max_expiry = i64::MIN;
        for job in jobs {
            ensure!(
                !job.job_id.is_empty() && job.job_id != dependency.key,
                "invalid issued job dependency"
            );
            ensure!(
                job.payload["prepared_key"].as_str() == Some(dependency.key)
                    && job.payload["expires_at_ms"].as_i64() == Some(job.expires_at_ms)
                    && parent == dependency.parent,
                "issued job dependency or deadline mismatch"
            );
            DateTime::<Utc>::from_timestamp_millis(job.expires_at_ms)
                .context("issued job expiry out of range")?;
            if let Some(previous) = unique.insert(&job.job_id, job) {
                ensure!(
                    previous.payload == job.payload && previous.expires_at_ms == job.expires_at_ms,
                    "immutable job ID conflict"
                );
            }
            min_expiry = min_expiry.min(job.expires_at_ms);
            max_expiry = max_expiry.max(job.expires_at_ms);
        }
        let earliest = DateTime::<Utc>::from_timestamp_millis(min_expiry).unwrap();
        let latest = DateTime::<Utc>::from_timestamp_millis(max_expiry).unwrap();
        let renewed = max_expiry
            .checked_add(DEPENDENCY_HEADROOM_MS)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .context("prepared dependency expiry overflow")?;
        let acquired = self.acquire().await?;
        attempt.cleanup.pending.store(true, Ordering::Release);
        let mut connection = BatchConnection {
            connection: Some(acquired),
            clean: false,
            cleanup: attempt.cleanup.clone(),
        };
        let mut tx = connection.connection.as_mut().unwrap().begin().await?;
        // Preserve a stricter configured limit. 15 seconds is the batch cleanup
        // ceiling, not a new operation deadline; queue/pool/BEGIN consumed time
        // from the original deadline before this point.
        let remaining = attempt
            .deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .clamp(1, 15_000) as i64;
        sqlx::query("SELECT set_config('statement_timeout',LEAST(COALESCE(NULLIF((SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'),0),15000),$1)::text,true)")
            .bind(remaining).execute(&mut *tx).await?;
        // KEY SHARE fences every authority writer (each takes the row FOR
        // UPDATE first; see `lock_cluster_authority`) and the collector's
        // exclusive cluster fence until commit, without blocking the share
        // append's `ledger_clock_ms` UPDATE, which holds ORDER_LOCK while it
        // waits and would otherwise queue every share behind this cohort.
        // Check authority only after acquiring it; this path must never
        // acquire either advisory lock while holding it.
        let fingerprint: Option<String> = sqlx::query_scalar(
            "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton FOR KEY SHARE",
        )
        .fetch_one(&mut *tx)
        .await?;
        writable(&mut tx).await?;
        require_revision(&mut tx, revision).await?;
        if let Some(pinned) = self.config_fingerprint() {
            ensure!(
                fingerprint.as_deref() == Some(pinned),
                "cluster configuration fingerprint differs from this frontend's pin"
            );
        }
        require_live(&mut tx, earliest).await?;
        let row = dependency_row(&mut tx, dependency.key).await?;
        require_live(&mut tx, earliest).await?;
        let Some(row) = row else {
            tx.rollback().await?;
            connection.clean = true;
            return Ok(IssuedJobSave::PreparedMissing);
        };
        dependency.check_row(&row)?;
        lock_blob_metadata(&mut tx, dependency).await?;
        require_live(&mut tx, earliest).await?;
        if row.try_get::<DateTime<Utc>, _>("expires_at")? < latest {
            sqlx::query("UPDATE qbit_prism_jobs SET expires_at=GREATEST(expires_at,$2) WHERE job_id=$1 AND expires_at<$3")
                .bind(dependency.key).bind(renewed).bind(latest).execute(&mut *tx).await?;
        }
        let mut insert = QueryBuilder::<Postgres>::new("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at) ");
        insert.push_values(unique.values(), |mut row, job| {
            row.push_bind(&job.job_id)
                .push_bind(&self.instance_id)
                .push_bind(parent)
                .push_bind(revision)
                .push_bind(&job.payload)
                .push_bind(DateTime::<Utc>::from_timestamp_millis(job.expires_at_ms).unwrap());
        });
        insert.push(" ON CONFLICT DO NOTHING");
        let inserted = insert.build().execute(&mut *tx).await?.rows_affected();
        if inserted != unique.len() as u64 {
            // Compare every input, including null compact columns, in one query.
            let mut check = QueryBuilder::<Postgres>::new("SELECT COALESCE(bool_and(j.job_id IS NOT NULL AND j.payload=v.payload AND j.expires_at=v.expires AND j.parent_hash=");
            check.push_bind(parent).push(" AND j.payout_revision=").push_bind(revision)
                .push(" AND num_nulls(j.window_anchor_ms,j.window_prior_balances_sha256,j.window_first_share_seq,j.window_last_share_seq,j.window_share_count,j.window_snapshot_sha256,j.template_sha256)=7),false) FROM (");
            check.push_values(unique.values(), |mut row, job| {
                row.push_bind(&job.job_id)
                    .push_bind(&job.payload)
                    .push_bind(DateTime::<Utc>::from_timestamp_millis(job.expires_at_ms).unwrap());
            });
            check.push(") AS v(id,payload,expires) LEFT JOIN qbit_prism_jobs j ON j.job_id=v.id");
            let same: bool = check.build_query_scalar().fetch_one(&mut *tx).await?;
            ensure!(same, "immutable job ID conflict");
        }
        require_live(&mut tx, earliest).await?;
        attempt.start_commit()?;
        tx.commit().await.context(
            "compact issued batch commit outcome uncertain; reconcile original job identities",
        )?;
        connection.clean = true;
        Ok(IssuedJobSave::Saved)
    }
}

#[cfg(test)]
#[path = "compact_issued_batch_tests.rs"]
mod tests;
