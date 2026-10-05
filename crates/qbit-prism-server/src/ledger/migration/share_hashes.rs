//! Migration 002's share-hash backfill, applied after the migration
//! transaction on a populated 2.x.x source (#582).
//!
//! 002 creates `qbit_prism_share_hashes`, the global header authority the
//! native append consults before it credits a share, and maps the header of
//! every accepted legacy share whose ID ends in 64 hex digits, the earliest
//! `share_seq` winning where legacy worker-scoped IDs repeat a header. As
//! one `INSERT ... SELECT DISTINCT ON` inside the migration transaction that
//! ran under the statement timeout, at a cost per share that rose with the
//! ledger (37 µs at 1.03M shares, 78 µs at 4.13M), so a production ledger
//! outgrew even the 600 s cap. On a ledger with rows the mapping is built
//! after the commit instead, in batches of consecutive `share_seq`, each one
//! statement in its own transaction under the pool's statement timeout, and
//! 2 is recorded last. A fresh or empty source has nothing to map and
//! records 2 in the migration transaction.
//!
//! Each batch is 002's statement restricted to its range, and the batches
//! run in ascending order, so a repeated header is mapped to its earliest
//! share exactly as the single statement mapped it: a later batch's copy of
//! a header already mapped meets ON CONFLICT DO NOTHING. The cursor,
//! `qbit_prism_share_hash_backfill.next_seq`, advances in the transaction
//! that inserts the batch, so an interrupted run resumes at the first range
//! that did not commit, and a range that committed is never read again.
//!
//! Until 2 is recorded, every start refuses the database: this release's,
//! and every earlier native release's, since each requires 2. The 2.x.x
//! writer is fenced by 002's lease trigger, which committed with the
//! transaction. So no share is appended while the mapping is incomplete,
//! and none can be credited against a legacy header not yet mapped. The
//! progress table exists from the transaction that applied 002 to the one
//! that records 2, which drops it. A native record with 3 and not 2 whose
//! database has the table is therefore a backfill that has not finished,
//! which `migrate_schema` resumes; without the table it is an edited
//! record, refused as before.
use super::online::{acquire_runner_lock, recorded};
use super::*;
use sqlx::{Connection, PgConnection};
use std::time::{Duration, Instant};

/// The migration the backfill completes, recorded once every legacy share
/// is mapped.
const VERSION: i32 = 2;

/// One batch: 002's backfill over the `share_seq` range `[$1, $2)`.
/// `cutover_rehearsal.rs` attributes this statement to the backfill by its
/// first words.
const BATCH: &str = "INSERT INTO qbit_prism_share_hashes(header_hash,share_id) SELECT DISTINCT ON (lower(right(share_id,64))) lower(right(share_id,64)),share_id FROM qbit_share_ledger WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$' AND share_seq>=$1 AND share_seq<$2 ORDER BY lower(right(share_id,64)),share_seq ON CONFLICT DO NOTHING";

/// The `share_seq` values the first batch of a run covers. Later batches
/// double or halve toward `BATCH_TARGET`, within `BATCH_MIN..=BATCH_MAX`.
const BATCH_START: i64 = 10_000;
const BATCH_MIN: i64 = 1_000;
const BATCH_MAX: i64 = 200_000;
/// What one batch statement should take: far inside the statement timeout
/// (15 s by default), and long enough that each transaction's commit is a
/// small part of it.
const BATCH_TARGET: Duration = Duration::from_millis(500);
/// How often a run logs its progress.
const REPORT_EVERY: Duration = Duration::from_secs(10);

/// Where a pending backfill stands: every legacy share below `next_seq` is
/// mapped, and the ledger held no row at or above `end_seq` when the
/// migration transaction committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Progress {
    pub(super) next_seq: i64,
    pub(super) end_seq: i64,
}

impl Progress {
    /// Why a start refuses the database while the backfill is pending.
    pub(super) fn refusal(&self) -> String {
        format!(
            "database is not ready: migration 2's share-hash backfill has not finished (#582). qbit_prism_share_hashes maps the legacy shares below share_seq {} of the {} the ledger held at migration, and until every one is mapped a share could be credited twice. Run `qbit-prism-server migrate` to resume it from there; every start refuses the database until it has finished and recorded migration 2",
            self.next_seq, self.end_seq
        )
    }
}

/// Refuse, before any DDL, a source that already has a relation under the
/// progress table's name. Only a migration transaction of this release
/// creates it, and only with 002 and 003, so on a source without 3 it is
/// someone else's.
pub(super) async fn refuse_held_name(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let held: Option<String> = sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname='qbit_prism_share_hash_backfill'")
        .fetch_optional(&mut **tx)
        .await?;
    if let Some(kind) = held {
        bail!(
            "refusing to migrate before any DDL: a {} named qbit_prism_share_hash_backfill already exists, and migration 2 creates its share-hash backfill's progress table under that name. Nothing was changed. Check what it holds, then rename or move it aside and migrate again",
            super::online::relation_kind(&kind)
        );
    }
    Ok(())
}

/// Create the cursor in the transaction that applies 002 to a ledger with
/// rows, covering every `share_seq` the ledger holds. The cutover locks
/// exclude writers here, and after the commit nothing appends until 2 is
/// recorded.
pub(super) async fn create_cursor(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::raw_sql("CREATE TABLE qbit_prism_share_hash_backfill (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), next_seq bigint NOT NULL, end_seq bigint NOT NULL, started_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp())")
        .execute(&mut **tx)
        .await?;
    sqlx::query("INSERT INTO qbit_prism_share_hash_backfill(next_seq,end_seq) SELECT COALESCE(min(share_seq),0),COALESCE(max(share_seq),-1)+1 FROM qbit_share_ledger")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// The cursor of a pending backfill, or `None` when none is pending: the
/// progress table exists only from the transaction that applied 002 to a
/// populated ledger to the one that records 2.
pub(super) async fn progress(connection: &mut PgConnection) -> Result<Option<Progress>> {
    let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname='qbit_prism_share_hash_backfill')")
        .fetch_one(&mut *connection)
        .await?;
    if !pending {
        return Ok(None);
    }
    let (next_seq, end_seq): (i64, i64) = sqlx::query_as(
        "SELECT next_seq,end_seq FROM qbit_prism_share_hash_backfill WHERE singleton",
    )
    .fetch_optional(&mut *connection)
    .await?
    .context("qbit_prism_share_hash_backfill has no row: migration 2 created it with its cursor and nothing deletes the row, so it was edited or restored selectively. Restore the full backup")?;
    Ok(Some(Progress { next_seq, end_seq }))
}

/// The size of the next batch, from how long the last one took.
fn next_batch(rows: i64, took: Duration) -> i64 {
    if took < BATCH_TARGET / 2 {
        (rows * 2).min(BATCH_MAX)
    } else if took > BATCH_TARGET * 2 {
        (rows / 2).max(BATCH_MIN)
    } else {
        rows
    }
}

/// Map the legacy shares the cursor has not passed, then drop the cursor
/// and record 2. Idempotent: a run finding no progress table finds 2
/// recorded by the run that dropped it.
pub(super) async fn apply(
    connection: &mut PgConnection,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    // The pool's statement and lock timeouts stay in force: every statement
    // here is one bounded batch or a catalog step.
    acquire_runner_lock(connection).await?;
    let Some(mut progress) = progress(connection).await? else {
        ensure!(
            recorded(connection, VERSION).await?,
            "refusing to continue migration 2: its share-hash backfill's progress table is gone, but 2 is not recorded. Only the transaction that records 2 drops it, so it was dropped by hand. Restore the full backup"
        );
        tracing::info!(version = VERSION, "share-hash backfill already complete");
        return Ok(());
    };
    let started = Instant::now();
    let first_seq = progress.next_seq;
    tracing::info!(
        version = VERSION,
        next_seq = progress.next_seq,
        end_seq = progress.end_seq,
        "backfilling qbit_prism_share_hashes for the legacy shares in batches; every start refuses the database until migration 2 is recorded"
    );
    let mut rows = BATCH_START;
    let mut mapped: u64 = 0;
    let mut reported = Instant::now();
    loop {
        while progress.next_seq < progress.end_seq {
            let upper = progress.next_seq.saturating_add(rows).min(progress.end_seq);
            let batch = Instant::now();
            let mut tx = connection.begin().await?;
            // The range starts where the last committed batch ended. Only
            // the runner lock's holder moves the cursor, so it is where
            // this run left it.
            let next: i64 = sqlx::query_scalar(
                "SELECT next_seq FROM qbit_prism_share_hash_backfill WHERE singleton FOR UPDATE",
            )
            .fetch_one(&mut *tx)
            .await?;
            ensure!(
                next == progress.next_seq,
                "refusing to continue migration 2: its share-hash backfill's cursor moved from {} to {next} under this run, which holds the runner lock; migrate again",
                progress.next_seq
            );
            mapped += sqlx::query(BATCH)
                .bind(next)
                .bind(upper)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("migration 2: backfilling qbit_prism_share_hashes for share_seq {next} to {upper}; every earlier batch committed, so migrate again to resume from share_seq {next}"))?
                .rows_affected();
            sqlx::query("UPDATE qbit_prism_share_hash_backfill SET next_seq=$1,updated_at=clock_timestamp() WHERE singleton")
                .bind(upper)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            progress.next_seq = upper;
            rows = next_batch(rows, batch.elapsed());
            if reported.elapsed() >= REPORT_EVERY {
                reported = Instant::now();
                let done = progress.next_seq - first_seq;
                let left = progress.end_seq - progress.next_seq;
                let rate = done as f64 / started.elapsed().as_secs_f64().max(0.001);
                tracing::info!(
                    version = VERSION,
                    next_seq = progress.next_seq,
                    end_seq = progress.end_seq,
                    mapped,
                    seqs_per_second = rate.round() as u64,
                    remaining_s = (left as f64 / rate.max(1.0)).round() as u64,
                    "share-hash backfill progress"
                );
            }
        }
        // Record 2 once the cursor has passed every row the ledger holds,
        // read again under the migration lock. Nothing can append before 2
        // is recorded, so the end found at migration is still the end; a
        // row past it would be mapped by another pass, not left unmapped.
        let mut tx = connection.begin().await?;
        lock(&mut tx, MIGRATION_LOCK, metrics).await?;
        let (next_seq, end_seq): (i64, i64) = sqlx::query_as("SELECT next_seq,(SELECT COALESCE(max(share_seq),-1)+1 FROM qbit_share_ledger) FROM qbit_prism_share_hash_backfill WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await?;
        if next_seq < end_seq {
            sqlx::query("UPDATE qbit_prism_share_hash_backfill SET end_seq=$1,updated_at=clock_timestamp() WHERE singleton")
                .bind(end_seq)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            tracing::warn!(
                version = VERSION,
                previous_end_seq = progress.end_seq,
                end_seq,
                "the share ledger holds rows past the end the backfill was planned to; mapping them too"
            );
            progress = Progress { next_seq, end_seq };
            continue;
        }
        sqlx::raw_sql("DROP TABLE qbit_prism_share_hash_backfill")
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO qbit_prism_schema_migrations(version) VALUES($1) ON CONFLICT (version) DO NOTHING",
        )
        .bind(VERSION)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        break;
    }
    tracing::info!(
        version = VERSION,
        mapped,
        seqs = progress.next_seq - first_seq,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "share-hash backfill complete; migration 2 recorded"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_grow_and_shrink_toward_the_target_within_their_bounds() {
        assert_eq!(next_batch(10_000, Duration::from_millis(100)), 20_000);
        assert_eq!(next_batch(10_000, Duration::from_millis(500)), 10_000);
        assert_eq!(next_batch(10_000, Duration::from_millis(1_500)), 5_000);
        assert_eq!(next_batch(BATCH_MAX, Duration::from_millis(1)), BATCH_MAX);
        assert_eq!(next_batch(BATCH_MIN, Duration::from_secs(10)), BATCH_MIN);
    }

    #[test]
    fn a_batch_is_the_single_statement_restricted_to_its_range() {
        // The single statement 002 ran before #582, less its range. The
        // batches' result equals its result only while the two agree.
        let single = "INSERT INTO qbit_prism_share_hashes(header_hash,share_id) SELECT DISTINCT ON (lower(right(share_id,64))) lower(right(share_id,64)),share_id FROM qbit_share_ledger WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$' ORDER BY lower(right(share_id,64)),share_seq ON CONFLICT DO NOTHING";
        assert_eq!(
            BATCH.replace(" AND share_seq>=$1 AND share_seq<$2", ""),
            single
        );
    }

    #[test]
    fn the_refusal_names_the_cursor_and_the_remedy() {
        let refusal = Progress {
            next_seq: 40,
            end_seq: 100,
        }
        .refusal();
        assert!(
            refusal.contains("below share_seq 40 of the 100"),
            "{refusal}"
        );
        assert!(refusal.contains("`qbit-prism-server migrate`"), "{refusal}");
    }
}
