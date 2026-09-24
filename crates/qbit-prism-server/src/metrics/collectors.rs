//! Background observations. HTTP rendering performs no external I/O.
use super::{time_pool_acquire, DatabaseMetrics, Metrics, ProcessMetrics};
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::watch;

/// Read from one procfs process directory; the explicit path also permits
/// deterministic tests of the production parser and failure behavior.
pub fn process(proc_path: &Path) -> Result<ProcessMetrics> {
    let status = std::fs::read_to_string(proc_path.join("status"))?;
    let field = |name: &str, unit: Option<&str>| -> Result<u64> {
        let line = status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .context("missing procfs field")?;
        let mut words = line.split_whitespace();
        let value: u64 = words.next().context("missing procfs value")?.parse()?;
        anyhow::ensure!(
            words.next() == unit && words.next().is_none(),
            "unexpected procfs field units"
        );
        Ok(value)
    };
    let resident_bytes = field("VmRSS:", Some("kB"))?
        .checked_mul(1024)
        .context("procfs RSS overflow")?;
    Ok(ProcessMetrics { resident_bytes })
}

/// One bounded read-only MVCC snapshot over unfinished candidate metadata:
/// every row the offer lifecycle (migration 011) has not finished, pending
/// and offered-but-not-landed alike, using `CandidateState::UNFINISHED_SQL`
/// so terminal submitted, abandoned and orphaned rows are excluded. The same
/// snapshot reads attached share ledger partition headroom (#144) and terminal
/// orphan evidence for at most 4,096 locally unresolved block identities. There
/// is no share-table scan, candidate JSON decode, or accounting lock. A failed
/// read leaves the database collection unavailable and landing evidence unknown.
pub async fn database(pool: &PgPool, metrics: &Metrics) -> Result<DatabaseMetrics> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let terminal = metrics.revision_work_terminal_probe();
    tokio::time::timeout_at(deadline, async {
        let connection = time_pool_acquire(Some(metrics), pool.acquire()).await?;
        let mut tx = crate::ledger::shielded_begin(connection).await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx).await?;
        sqlx::query("SELECT set_config('statement_timeout','2000',true),set_config('lock_timeout','500',true)")
            .execute(&mut *tx).await?;
        // One scan of the unfinished rows yields the count and three ages in
        // the same snapshot (#493). The paging age counts rows the node has
        // not accepted: pre-offer rows, offered rows whose one submitblock
        // outcome is unknown (but not a row adopted on the node's evidence
        // that its block is active), and rows the node definitively rejected
        // unless the reply names a side-chain block, which is a lost tip
        // race. A node-accepted lost race awaiting its orphan proof is
        // unfinished but acknowledged. The landing-failed age is the oldest
        // reconciliation row whose audit landing has not committed (the
        // landing transaction is the only writer of the pool-block row, so
        // its absence is the durable fact) or whose last error names a
        // landing refusal: a retry that fails for a transient reason
        // overwrites the error but not the fact. It is measured from the
        // offer reservation, the start of the post-offer lifecycle, so
        // pre-offer backoff never counts toward it.
        let age = |since: &str, rows: &str| format!(
            "COALESCE(GREATEST(0,extract(epoch FROM transaction_timestamp()-min({since}){rows})),0)::double precision"
        );
        const UNACKNOWLEDGED: &str = " FILTER (WHERE state IN ('pending','offer_reserved') \
            OR (offer_outcome='unknown' AND COALESCE(offer_reply,'') NOT LIKE $1) \
            OR (offer_outcome='rejected' AND COALESCE(offer_reply,'') <> ALL($3)))";
        const LANDING_FAILED: &str = " FILTER (WHERE state='reconciliation' \
            AND (COALESCE(last_error,'') LIKE $2 \
                 OR NOT EXISTS (SELECT 1 FROM qbit_pool_blocks landed WHERE landed.block_hash=outbox.block_hash)))";
        let census = format!(
            "SELECT count(*), {all}, {unacknowledged}, {landing_failed} \
             FROM qbit_block_candidate_outbox outbox WHERE state IN {unfinished}",
            all = age("created_at", ""),
            unacknowledged = age("created_at", UNACKNOWLEDGED),
            landing_failed = age("COALESCE(offer_reserved_at,created_at)", LANDING_FAILED),
            unfinished = crate::ledger::CandidateState::UNFINISHED_SQL,
        );
        let (candidates, candidate_age, unacknowledged_age, landing_failed_age): (i64, f64, f64, f64) =
            sqlx::query_as(&census)
                .bind(format!("{}%", crate::ledger::ADOPTED_OFFER_REPLY_PREFIX))
                .bind(format!("{}%", crate::ledger::LANDING_FAILED_REASON_PREFIX))
                .bind(crate::ledger::SIDE_CHAIN_REPLIES)
                .fetch_one(&mut *tx)
                .await?;
        let partition_lead_rows: Option<i64> = sqlx::query_scalar(
            "SELECT max(upper_seq)-qbit_prism_share_next_seq() FROM qbit_prism_share_partitions WHERE state='attached'"
        ).fetch_one(&mut *tx).await?;
        let snapshot = DatabaseMetrics { candidates: candidates.try_into()?, candidate_oldest: seconds(candidate_age)?, candidate_oldest_unacknowledged: seconds(unacknowledged_age)?, candidate_oldest_landing_failed: seconds(landing_failed_age)?, partition_lead_rows };
        // One bounded identity lookup in this same read-only snapshot. Terminal
        // processing state stays orphaned even if a later reorg credits it.
        // Never load retained history or add I/O to work publication/scrapes.
        let orphaned: Vec<String> = if terminal.hashes.is_empty() {
            Vec::new()
        } else {
            sqlx::query_scalar("SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=ANY($1::text[]) AND state='orphaned'")
                .bind(&terminal.hashes).fetch_all(&mut *tx).await?
        };
        tx.commit().await?;
        terminal.succeeded(&orphaned);
        Ok::<_, anyhow::Error>(snapshot)
    }).await.context("metrics database collection deadline exceeded")?
}

fn seconds(value: f64) -> Result<Duration> {
    Duration::try_from_secs_f64(value).context("invalid measured database age")
}

pub async fn run(
    metrics: Arc<Metrics>,
    pool: PgPool,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! { _ = shutdown.changed() => break, _ = interval.tick() => {} }
        let process_attempt = metrics.begin_collection(super::Collector::Process);
        let process = tokio::task::spawn_blocking(|| process(Path::new("/proc/self"))).await;
        process_attempt.publish_process(process.ok().and_then(Result::ok));
        let database_attempt = metrics.begin_collection(super::Collector::Database);
        tokio::select! {
            _ = shutdown.changed() => break,
            result = database(&pool, &metrics) => {
                if let Err(error) = &result { tracing::warn!(%error, "metrics database collection unavailable"); }
                database_attempt.publish_database(result.ok());
            }
        }
    }
    Ok(())
}
