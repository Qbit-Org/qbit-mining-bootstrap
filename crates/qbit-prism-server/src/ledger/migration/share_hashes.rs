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
/// A batch's cost follows the rows it holds, not the values it covers: a
/// gap or a run of rejected shares makes batches cheap, and the next dense
/// range starts at whatever size they grew to. `BATCH_MAX` keeps that
/// first dense batch inside the statement timeout: 50,000 `share_seq` took
/// under 4 s at the slowest rate measured on a mainnet-shaped ledger (about
/// 13,000 a second, PostgreSQL's default memory settings, the mapping's
/// indexes past the cache). A batch that outlasts the timeout all the same
/// is retried at half its size.
const BATCH_START: i64 = 10_000;
const BATCH_MIN: i64 = 1_000;
const BATCH_MAX: i64 = 50_000;
/// What one batch statement should take: far inside the statement timeout
/// (15 s by default), and long enough that each transaction's commit is a
/// small part of it.
const BATCH_TARGET: Duration = Duration::from_millis(500);
/// How often a run logs its progress.
const REPORT_EVERY: Duration = Duration::from_secs(10);

/// Where a pending backfill stands. The legacy shares from `start_seq` up
/// to `next_seq` are mapped and those from `next_seq` up to `end_seq` are
/// not; the ledger held no row at or above `end_seq` when the migration
/// transaction committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Progress {
    pub(super) start_seq: i64,
    pub(super) next_seq: i64,
    pub(super) end_seq: i64,
}

impl Progress {
    /// Why a start refuses the database while the backfill is pending.
    pub(super) fn refusal(&self) -> String {
        format!(
            "database is not ready: migration 2's share-hash backfill has not finished (#582). The legacy shares from share_seq {} up to {} are mapped in qbit_prism_share_hashes and those from {} up to {} are not, and until every one is mapped a share could be credited twice. Run `qbit-prism-server migrate` to resume it from there; every start refuses the database until it has finished and recorded migration 2",
            self.start_seq, self.next_seq, self.next_seq, self.end_seq
        )
    }
}

/// The kind of the relation under the progress table's name in the
/// current schema, if any.
async fn cursor_relation(connection: &mut PgConnection) -> Result<Option<String>> {
    Ok(sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname='qbit_prism_share_hash_backfill'")
        .fetch_optional(&mut *connection)
        .await?)
}

/// Refuse, before any DDL, a source that already has a relation under the
/// progress table's name. Only a migration transaction of this release
/// creates it, and only with 002 and 003, so on a source without 3 it is
/// someone else's.
pub(super) async fn refuse_held_name(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    if let Some(kind) = cursor_relation(tx).await? {
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
    sqlx::raw_sql("CREATE TABLE qbit_prism_share_hash_backfill (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), start_seq bigint NOT NULL, next_seq bigint NOT NULL, end_seq bigint NOT NULL, started_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp())")
        .execute(&mut **tx)
        .await?;
    sqlx::query("INSERT INTO qbit_prism_share_hash_backfill(start_seq,next_seq,end_seq) SELECT first_seq,first_seq,end_seq FROM (SELECT COALESCE(min(share_seq),0) AS first_seq,COALESCE(max(share_seq),-1)+1 AS end_seq FROM qbit_share_ledger) bounds")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// The cursor of a pending backfill, or `None` when none is pending: the
/// progress table exists only from the transaction that applied 002 to a
/// populated ledger to the one that records 2.
pub(super) async fn progress(connection: &mut PgConnection) -> Result<Option<Progress>> {
    match cursor_relation(connection).await?.as_deref() {
        None => return Ok(None),
        Some("r") => {}
        Some(kind) => bail!(
            "a {} named qbit_prism_share_hash_backfill holds the name of migration 2's share-hash backfill progress table; check what it holds, then rename or move it aside",
            super::online::relation_kind(kind)
        ),
    }
    let row = match sqlx::query_as(
        "SELECT start_seq,next_seq,end_seq FROM qbit_prism_share_hash_backfill WHERE singleton",
    )
    .fetch_optional(&mut *connection)
    .await
    {
        Ok(row) => row,
        // Dropped since the look-up by the transaction that records 2, as a
        // start that takes no migration lock can see. Every other caller
        // holds a lock that transaction needs, so this is outside any
        // transaction it could leave aborted.
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01") => {
            return Ok(None)
        }
        Err(error) => return Err(error.into()),
    };
    let (start_seq, next_seq, end_seq): (i64, i64, i64) = row.context("qbit_prism_share_hash_backfill has no row: migration 2 created it with its cursor and nothing deletes the row, so it was edited or restored selectively. Restore the full backup")?;
    Ok(Some(Progress {
        start_seq,
        next_seq,
        end_seq,
    }))
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

/// Whether a statement was cancelled by the statement timeout. An
/// operator's cancel request carries the same SQLSTATE and another message,
/// and still stops the run.
fn statement_timed_out(error: &dyn sqlx::error::DatabaseError) -> bool {
    error.code().as_deref() == Some("57014") && error.message().contains("statement timeout")
}

/// Map the legacy shares the cursor has not passed, then drop the cursor
/// and record 2. Idempotent: a run finding no progress table finds 2
/// recorded by the run that dropped it.
pub(super) async fn apply(
    connection: &mut PgConnection,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    // The pool's statement and lock timeouts stay in force for the batches:
    // each is one bounded statement.
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
        start_seq = progress.start_seq,
        next_seq = progress.next_seq,
        end_seq = progress.end_seq,
        "backfilling qbit_prism_share_hashes for the legacy shares in batches; every start refuses the database until migration 2 is recorded"
    );
    let mut rows = BATCH_START;
    let mut mapped: u64 = 0;
    let mut reported = Instant::now();
    loop {
        while progress.next_seq < progress.end_seq {
            let next = progress.next_seq;
            let upper = next.saturating_add(rows).min(progress.end_seq);
            let batch = Instant::now();
            let mut tx = connection.begin().await?;
            let inserted = match sqlx::query(BATCH)
                .bind(next)
                .bind(upper)
                .execute(&mut *tx)
                .await
            {
                Ok(done) => done.rows_affected(),
                // Only this batch is lost; the same range is tried again at
                // half the size.
                Err(sqlx::Error::Database(error))
                    if rows > BATCH_MIN && statement_timed_out(&*error) =>
                {
                    tx.rollback().await?;
                    rows = (rows / 2).max(BATCH_MIN);
                    tracing::warn!(
                        version = VERSION,
                        next_seq = next,
                        upper,
                        retry_seqs = rows,
                        "a share-hash backfill batch outlasted the statement timeout; retrying it at half the size"
                    );
                    continue;
                }
                Err(error) => {
                    return Err(anyhow::Error::from(error).context(format!("migration 2: backfilling qbit_prism_share_hashes for share_seq {next} to {upper}; every earlier batch committed, so migrate again to resume from share_seq {next}")));
                }
            };
            // Only the runner lock's holder moves the cursor, so it is where
            // this run left it; the condition checks that it still is.
            let advanced = sqlx::query("UPDATE qbit_prism_share_hash_backfill SET next_seq=$2,updated_at=clock_timestamp() WHERE singleton AND next_seq=$1")
                .bind(next)
                .bind(upper)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            ensure!(
                advanced == 1,
                "refusing to continue migration 2: its share-hash backfill's cursor is no longer at share_seq {next}, where this run, which holds the runner lock, left it; migrate again"
            );
            tx.commit().await?;
            mapped += inserted;
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
        // Like 013's and 017's records, this waits for the migration lock
        // and for any reader of the cursor table rather than failing on the
        // pool's timeouts once all the mapping is done.
        let mut tx = connection.begin().await?;
        sqlx::query(
            "SELECT set_config('statement_timeout','0',true),set_config('lock_timeout','0',true)",
        )
        .execute(&mut *tx)
        .await?;
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
            progress.next_seq = next_seq;
            progress.end_seq = end_seq;
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
            start_seq: 1,
            next_seq: 40,
            end_seq: 100,
        }
        .refusal();
        assert!(
            refusal.contains(
                "from share_seq 1 up to 40 are mapped in qbit_prism_share_hashes and those from 40 up to 100 are not"
            ),
            "{refusal}"
        );
        assert!(refusal.contains("`qbit-prism-server migrate`"), "{refusal}");
    }
}
