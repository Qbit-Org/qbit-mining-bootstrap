//! Bounded collection of immutable prepared templates and shared balances.
use super::*;
use tokio::time::{timeout_at, Instant};

const BLOB_PAGE: i64 = 256;

/// One pruner's in-memory progress, safe to reset on restart. Advance past
/// retained keys too, so a live prefix cannot starve later orphaned blobs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BlobPruneCursor {
    template: String,
    balance: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct BlobPruneResult {
    pub templates: u64,
    pub balances: u64,
}

impl Ledger {
    /// Inspect at most 256 keys in EACH blob table, including when no job
    /// expired. Never reads blob payloads. Run job expiry separately before
    /// starting this deadline: its larger DELETE must not hold advisory locks.
    ///
    /// Settlement serializes prepared save/renewal/repair; ordering serializes
    /// candidate insertion and its balance blob. Acquire them in that order
    /// before inspecting references. Keep every remaining job reference (even
    /// an expired row awaiting the next batch) and every candidate reference.
    /// Terminal candidate writes detach references atomically; a claim's lease
    /// expiry is irrelevant, and unexpected surviving references fail closed.
    ///
    /// One caller-supplied deadline covers acquisition, locks, scans and commit.
    /// Each statement also gets the remaining budget: dropping an SQLx future
    /// queues rollback but does not cancel the in-flight PostgreSQL statement.
    /// Cancellation rolls back blob deletes, not already committed job expiry.
    /// A failed/unknown commit leaves cursor progress unchanged; replaying the
    /// same page is safe.
    pub async fn prune_unreferenced_blobs(
        &self,
        cursor: &mut BlobPruneCursor,
        deadline: Instant,
    ) -> Result<BlobPruneResult> {
        let (result, next) = timeout_at(deadline, self.sweep_blobs(cursor, deadline))
            .await
            .context("blob cleanup deadline elapsed")??;
        *cursor = next;
        Ok(result)
    }

    async fn sweep_blobs(
        &self,
        cursor: &BlobPruneCursor,
        deadline: Instant,
    ) -> Result<(BlobPruneResult, BlobPruneCursor)> {
        let mut tx = BlobTransaction {
            tx: self.begin().await?,
            deadline,
        };
        self.lock(tx.statement().await?, SETTLEMENT_LOCK).await?;
        self.lock(tx.statement().await?, ORDER_LOCK).await?;
        writable(tx.statement().await?).await?;

        // Bound the inspected keys BEFORE reference filtering. OFFSET 0
        // is a planner fence that preserves correlated indexed probes;
        // PostgreSQL may choose the same bounded plan without this insurance.
        let templates: Vec<String> = sqlx::query_scalar(
                "SELECT template_sha256 FROM qbit_prism_templates WHERE template_sha256 > $1 ORDER BY template_sha256 LIMIT $2",
            ).bind(&cursor.template).bind(BLOB_PAGE).fetch_all(&mut **tx.statement().await?).await?;
        let balances: Vec<String> = sqlx::query_scalar(
                "SELECT prior_balances_digest FROM qbit_prism_balance_snapshots WHERE prior_balances_digest > $1 ORDER BY prior_balances_digest LIMIT $2",
            ).bind(&cursor.balance).bind(BLOB_PAGE).fetch_all(&mut **tx.statement().await?).await?;

        let removed_templates = sqlx::query(
                "DELETE FROM qbit_prism_templates t WHERE t.template_sha256 = ANY($1) AND NOT EXISTS (SELECT 1 FROM qbit_prism_jobs j WHERE j.template_sha256=t.template_sha256 OFFSET 0)",
            ).bind(&templates).execute(&mut **tx.statement().await?).await?.rows_affected();
        let removed_balances = sqlx::query(
                "DELETE FROM qbit_prism_balance_snapshots b WHERE b.prior_balances_digest = ANY($1) AND NOT EXISTS (SELECT 1 FROM qbit_prism_jobs j WHERE j.window_prior_balances_sha256=b.prior_balances_digest OFFSET 0) AND NOT EXISTS (SELECT 1 FROM qbit_block_candidate_outbox c WHERE c.window_prior_balances_sha256=b.prior_balances_digest OFFSET 0)",
            ).bind(&balances).execute(&mut **tx.statement().await?).await?.rows_affected();
        let next = BlobPruneCursor {
            template: templates.last().cloned().unwrap_or_default(),
            balance: balances.last().cloned().unwrap_or_default(),
        };
        tx.commit().await?;
        Ok((
            BlobPruneResult {
                templates: removed_templates,
                balances: removed_balances,
            },
            next,
        ))
    }
}

/// Apply the remaining deadline whenever a statement obtains the transaction,
/// including advisory locks, the write fence and commit. Keep Ledger::lock so
/// lock-wait observations continue through the shared timing API.
struct BlobTransaction<'a> {
    tx: Transaction<'a, Postgres>,
    deadline: Instant,
}

impl<'a> BlobTransaction<'a> {
    async fn statement(&mut self) -> Result<&mut Transaction<'a, Postgres>> {
        let remaining = self
            .deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        ensure!(remaining > 0, "blob cleanup deadline elapsed");
        let millis = remaining.min(i32::MAX as u128) as i64;
        // SET LOCAL is reverted with this transaction; preserve a stricter
        // configured statement timeout and the existing lock_timeout unchanged.
        // PostgreSQL LEAST ignores NULL: timeout 0 (disabled) uses our budget.
        sqlx::query("SELECT set_config('statement_timeout', LEAST(NULLIF(EXTRACT(EPOCH FROM current_setting('statement_timeout')::interval)*1000,0),$1::bigint)::bigint::text,true)")
        .bind(millis).execute(&mut *self.tx).await?;
        Ok(&mut self.tx)
    }

    async fn commit(mut self) -> Result<()> {
        self.statement().await?;
        self.tx.commit().await?;
        Ok(())
    }
}

// The stable cutoff permits an expiry-index range scan even with zero expired
// jobs. A bounded array initplan keeps DELETE on selected primary keys instead
// of a hash semi-join scanning every live job. Preserve the outer expiry recheck
// after a concurrent renewal's row-lock wait; selection grants no delete right.
pub(super) const EXPIRED_JOBS: &str = "DELETE FROM qbit_prism_jobs WHERE job_id = ANY(ARRAY(SELECT job_id FROM qbit_prism_jobs WHERE expires_at < statement_timestamp() ORDER BY expires_at LIMIT 4096)) AND expires_at < clock_timestamp()";
