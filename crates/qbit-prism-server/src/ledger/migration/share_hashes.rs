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
//! Until 2 is recorded, every start refuses the database, unless the
//! recent range below has permitted serving: this release's, and every
//! earlier native release's, since each requires 2. The 2.x.x writer is
//! fenced by 002's lease trigger, which committed with the transaction. So
//! no share is appended while the mapping is incomplete, and none can be
//! credited against a legacy header not yet mapped. The progress table
//! exists from the transaction that applied 002 to the one that records 2,
//! which drops it. A native record with 3 and not 2 whose database has the
//! table is therefore a backfill that has not finished, which
//! `migrate_schema` resumes; without the table it is an edited record,
//! refused as before.
//!
//! The migration record alone is a weak fence: an earlier build that meets
//! 3 without 2 tells the operator to record 2 by hand, and after that it
//! would serve with legacy headers unmapped. So while the backfill is
//! pending the database also declares the capability
//! `share_hash_backfill_pending = 1` (#669). The transaction that creates
//! the cursor declares it, and the transaction that records 2 removes it
//! with the cursor. Every earlier build that checks capabilities refuses
//! the declaration at connect and at migrate, whatever the record says, and
//! this release refuses the cursor itself.
//!
//! The fence is declared there and nowhere else, never again on a resume.
//! An earlier build cannot remove it, so it must never coexist with an
//! earlier build's runner. Migrations serialize on the migration lock, and
//! no earlier build's migrate passes its capability check once the fence
//! has committed. So the only runners an earlier build can have started are
//! for a cursor an earlier build created, which carries no fence. Declared
//! later, by a resume, a fence could meet such a runner already past its
//! check: still mapping, or queued for the runners' lock. That runner would
//! record 2 without removing the fence, and leave it behind. The price is
//! that a backfill a #582 build started, before #669, stays unfenced to its
//! end. A declaration whose cursor is gone, which only a hand-dropped
//! cursor leaves, is refused by every start and migrate of this release.
//! Before it records 2, the runner reads the declaration again under the
//! migration lock and stops at any value but 1 or 2: a newer release
//! declared it meanwhile, and the cursor, the declaration and the record
//! are that release's to finish.
//!
//! Serving with the backfill pending. On a production ledger the backfill
//! is most of the cutover's migration, hours at 65.9M shares, and `migrate
//! --defer-share-hashes` takes it out of the outage (`ShareHashBackfill`).
//! Before anything serves, that run maps only the recent range
//! (`map_recent`): the accepted legacy shares whose template height is
//! within `RECENT_HEIGHTS` of the highest an accepted legacy share has. A
//! header commits to its parent, so every copy of a legacy header has the
//! same template height, and the range holds every copy of each header it
//! maps: its batches are 002's statement restricted to the range, in
//! ascending `share_seq`, so each header maps to its earliest share as the
//! whole backfill maps it. A native share is credited only on a job this
//! cluster issued, whose parent is the current tip or, within stale grace,
//! the tip's parent (`coordinator/miner_submit.rs`). It can repeat a legacy
//! header only on that header's parent, so only a header in the range,
//! unless the chain reorganized deeper than the coinbase maturity the range
//! spans, which block maturity does not survive either. So the append's
//! header check is exact without the rest of the mapping. The transaction
//! that ends the range raises the fence to 2, which permits serving: every
//! start of this release accepts the database with 2 unrecorded, and every
//! earlier build refuses the value, at connect and at migrate, as it
//! refuses any value it does not know. The cursor does not move.
//!
//! The deferred run claims the backfill before it maps anything: the
//! migration transaction that creates the cursor, or that finds it at 1,
//! sets the cursor's `deferred_at`. A run that stops before its range has
//! permitted serving leaves the claim at fence 1, and from there only the
//! operator's `migrate` maps anything: `--defer-share-hashes` again maps
//! the range, and plain `migrate`, chosen explicitly, the whole backfill
//! before anything serves. Every other connect that migrates, a frontend's
//! that compose starts with `PRISM_POSTGRES_INIT_SCHEMA=1` among them,
//! refuses the database as every start does, and before 013 drops the
//! index the range reads, instead of mapping for hours while the
//! operator's retry waits for the runners' lock. Without the claim, every
//! such connect still runs a backfill at fence 1 to its end.
//!
//! A backfill that permits serving is the operator's to finish: only plain
//! `migrate` maps the rest, in the same batches, while frontends serve. No
//! other connect touches it. Its end is the one the cursor was planned to,
//! never extended: every row at or above it is a native share, which
//! mapped its own header in the transaction that appended it. So the
//! transaction that raises the fence also moves the ledger's share_seq
//! sequence up to that end if it lags behind (rows inserted with explicit
//! values, or a restore that left it uncalled), and writes nothing to it
//! otherwise. Before it records 2, plain `migrate` refuses the database if
//! a native share repeats a legacy header after all, naming it, and leaves
//! the cursor and the fence as they are: a header credited twice is
//! reported, not recorded over.
use super::online::{acquire_runner_lock, recorded};
use super::*;
use sqlx::{Connection, PgConnection};
use std::time::{Duration, Instant};

/// The migration the backfill completes, recorded once every legacy share
/// is mapped.
pub(super) const VERSION: i32 = 2;

/// One batch: 002's backfill over the `share_seq` range `[$1, $2)`.
/// `cutover_rehearsal.rs` attributes this statement to the backfill by its
/// first words.
const BATCH: &str = "INSERT INTO qbit_prism_share_hashes(header_hash,share_id) SELECT DISTINCT ON (lower(right(share_id,64))) lower(right(share_id,64)),share_id FROM qbit_share_ledger WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$' AND share_seq>=$1 AND share_seq<$2 ORDER BY lower(right(share_id,64)),share_seq ON CONFLICT DO NOTHING";

/// One batch of the recent range: `BATCH` restricted to the legacy shares
/// at template height `$3` and above. Every copy of a header has the same
/// template height, so the restriction keeps all of a header's copies or
/// none of them, and the earliest still wins.
const RECENT_BATCH: &str = "INSERT INTO qbit_prism_share_hashes(header_hash,share_id) SELECT DISTINCT ON (lower(right(share_id,64))) lower(right(share_id,64)),share_id FROM qbit_share_ledger WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$' AND share_seq>=$1 AND share_seq<$2 AND template_height>=$3 ORDER BY lower(right(share_id,64)),share_seq ON CONFLICT DO NOTHING";

/// How far below the highest template height of an accepted legacy share
/// the recent range reaches: the coinbase maturity, 1,000 blocks. A native
/// share is credited only on the tip or, within stale grace, the tip's
/// parent, so it can repeat a legacy header below the range only after the
/// chain reorganized deeper than that, which block maturity does not
/// survive either.
const RECENT_HEIGHTS: i64 = 1_000;

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
/// not, but for the recent range once it is mapped; the ledger held no row
/// at or above `end_seq` when the migration transaction committed.
/// `deferred` is whether `migrate --defer-share-hashes` has claimed it
/// (`declare_deferred`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Progress {
    pub(super) start_seq: i64,
    pub(super) next_seq: i64,
    pub(super) end_seq: i64,
    pub(super) deferred: bool,
}

impl Progress {
    /// Why a start refuses the database while the backfill is pending and
    /// does not permit serving. `fence` is what the database declares: 1,
    /// whose recent range `migrate --defer-share-hashes` can map, or none,
    /// on a backfill a build before #669 started, which only plain
    /// `migrate` finishes. At 1 with the deferred run's claim, that run
    /// stopped before its range permitted serving, and only the operator's
    /// `migrate` takes it on.
    pub(super) fn refusal(&self, fence: Option<i32>) -> String {
        let remedy = match fence {
            Some(FENCE_PENDING) if self.deferred => format!("`qbit-prism-server migrate --defer-share-hashes` claimed it and stopped before its recent range was mapped, so no start and no other connect maps it. Run `qbit-prism-server migrate --defer-share-hashes` again to map the legacy shares within {RECENT_HEIGHTS} template heights of the highest one and permit serving, or plain `qbit-prism-server migrate` to map them all and record migration 2 before anything serves; every start refuses the database until one of them has finished"),
            Some(FENCE_PENDING) => format!("Run `qbit-prism-server migrate` to resume it from there, or `qbit-prism-server migrate --defer-share-hashes` to map only the legacy shares within {RECENT_HEIGHTS} template heights of the highest one and serve while a later `migrate` maps the rest; every start refuses the database until one of them has finished"),
            _ => "Run `qbit-prism-server migrate` to resume it from there; every start refuses the database until it has finished and recorded migration 2".to_owned(),
        };
        format!(
            "database is not ready: migration 2's share-hash backfill has not finished (#582). The legacy shares from share_seq {} up to {} are mapped in qbit_prism_share_hashes and those from {} up to {} are not, and until every one is mapped a share could be credited twice. {remedy}",
            self.start_seq, self.next_seq, self.next_seq, self.end_seq
        )
    }
}

/// The capability a pending backfill declares, fencing every build before
/// #669 off the database (see the module doc).
pub(super) const PENDING_CAPABILITY: &str = "share_hash_backfill_pending";
/// The fence's value from the transaction that creates the cursor: every
/// start refuses the database.
pub(super) const FENCE_PENDING: i32 = 1;
/// The fence's value once the recent range is mapped: every start of this
/// release serves the database with 2 unrecorded, every earlier build
/// refuses the value, and only plain `migrate` maps the rest.
pub(super) const FENCE_SERVING: i32 = 2;

/// What a connect that migrates does with a pending share-hash backfill
/// (#582). Every connect with `initialize` used to run it to its end, and
/// at the fence's first value every one still does, unless a deferred
/// `migrate` has claimed it. At 2 the database serves while the rest is
/// mapped, and only `migrate` maps it, so a frontend that compose starts
/// with `PRISM_POSTGRES_INIT_SCHEMA=1` never takes hours of mapping on at
/// its start (see the module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShareHashBackfill {
    /// Every connect with `initialize` but `migrate`'s: a frontend's, a
    /// tool's or an operator's. A backfill that does not permit serving is
    /// run to its end and 2 recorded, as before, unless a deferred
    /// `migrate` claimed it, which is refused as every start refuses it;
    /// one that permits serving is left as it is.
    Initialize,
    /// `qbit-prism-server migrate`: every pending backfill is run to its
    /// end and 2 recorded, one a deferred `migrate` claimed and one that
    /// permits serving included, the latter up to the end its cursor was
    /// planned to.
    Finish,
    /// `qbit-prism-server migrate --defer-share-hashes`: a backfill that
    /// does not permit serving yet is claimed for the deferral, only its
    /// recent range is mapped, and serving is permitted with the rest
    /// pending. One that permits serving already is left as it is, and one
    /// a build before #669 started, which declares no fence, is refused.
    Defer,
}

/// Declare the fence of a pending backfill, in the migration transaction
/// that creates its cursor and nowhere else (see the module doc).
pub(super) async fn declare_pending(connection: &mut PgConnection) -> Result<()> {
    sqlx::query("INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES($1,$2) ON CONFLICT (capability) DO NOTHING")
        .bind(PENDING_CAPABILITY)
        .bind(FENCE_PENDING)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

/// Claim a pending backfill for `migrate --defer-share-hashes` before
/// anything is mapped: in the migration transaction that creates its
/// cursor, or that finds it at fence 1. A deferred run that stops before
/// its recent range permits serving leaves the claim, and from then on no
/// connect but the operator's `migrate` maps anything (see the module
/// doc). The first claim's time is kept. The column is the code's, like
/// the recent range's, and goes with the cursor when 2 is recorded.
pub(super) async fn declare_deferred(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::raw_sql(
        "ALTER TABLE qbit_prism_share_hash_backfill ADD COLUMN IF NOT EXISTS deferred_at timestamptz",
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query("UPDATE qbit_prism_share_hash_backfill SET deferred_at=COALESCE(deferred_at,clock_timestamp()) WHERE singleton")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// The fence the database declares: `None` once 2 is recorded, and on a
/// backfill a build before #669 started. Reads the capability table, which
/// every database with a cursor has: 006 ran in the transaction that
/// created it.
pub(super) async fn fence(connection: &mut PgConnection) -> Result<Option<i32>> {
    Ok(sqlx::query_scalar(
        "SELECT capability_value FROM qbit_prism_schema_capabilities WHERE capability=$1",
    )
    .bind(PENDING_CAPABILITY)
    .fetch_optional(&mut *connection)
    .await?)
}

/// Why a database that declares the fence, at `value`, without its cursor
/// is refused.
pub(super) fn orphaned_fence_refusal(value: i32) -> String {
    // At 2 frontends may have served since the recent range was mapped, and
    // the pre-migration backup holds none of their shares.
    let remedy = if value == FENCE_SERVING {
        "Frontends may have served since the fence reached 2, so the pre-migration backup is no longer a copy of this ledger; once"
    } else {
        "Restore the full pre-migration backup and migrate again, or, once"
    };
    format!(
        "database declares {PENDING_CAPABILITY} = {value}, but migration 2's share-hash backfill cursor qbit_prism_share_hash_backfill is gone. Only the transaction that records 2 removes them, and it removes both, so the cursor was dropped by hand and legacy shares may be unmapped (#669). {remedy} qbit_prism_share_hashes is verified to map every accepted share whose ID ends in 64 hex digits (docs/prism-rust-migration.md has the query), remove the declaration with DELETE FROM qbit_prism_schema_capabilities WHERE capability='{PENDING_CAPABILITY}' and migrate again"
    )
}

/// Why `migrate --defer-share-hashes` refuses a backfill that declares no
/// fence. A build before #669 started it, and this release never declares
/// the fence on a resume (see the module doc), so serving cannot be
/// permitted: plain `migrate` finishes it before anything serves.
pub(super) fn unfenced_defer_refusal(progress: &Progress) -> String {
    format!(
        "refusing to defer migration 2's share-hash backfill: a build before #669 started it, so the database declares no {PENDING_CAPABILITY} fence, and only a fenced backfill can be served with legacy shares unmapped. A resume never declares the fence: a runner of that earlier build could still be mapping, and would record 2 and leave the fence behind. Nothing was changed. Run `qbit-prism-server migrate` without --defer-share-hashes, which maps the legacy shares from share_seq {} up to {} and records 2 before anything serves",
        progress.next_seq, progress.end_seq
    )
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
    // `deferred_at` exists once a deferred run has added it, so it is read
    // through the row's JSON, which lacks the key without the column.
    let row = match sqlx::query_as(
        "SELECT start_seq,next_seq,end_seq,(to_jsonb(b)->>'deferred_at') IS NOT NULL FROM qbit_prism_share_hash_backfill b WHERE singleton",
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
    let (start_seq, next_seq, end_seq, deferred): (i64, i64, i64, bool) = row.context("qbit_prism_share_hash_backfill has no row: migration 2 created it with its cursor and nothing deletes the row, so it was edited or restored selectively. Restore the full backup")?;
    Ok(Some(Progress {
        start_seq,
        next_seq,
        end_seq,
        deferred,
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

/// What a run of batches maps, and whether it moves the cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pass {
    /// The backfill proper, `BATCH`: each batch's transaction moves the
    /// cursor past it, so an interrupted run resumes at the first range
    /// that did not commit.
    Backfill,
    /// The recent range, `RECENT_BATCH` at `min_height` and above. The
    /// cursor stays where it is: an interrupted run maps the whole range
    /// again, and what it had mapped meets ON CONFLICT DO NOTHING.
    Recent { min_height: i64 },
}

/// Map the legacy shares of `[from, to)` in ascending batches of
/// consecutive `share_seq`, each one statement in its own transaction under
/// the pool's statement timeout, and return the rows mapped. A batch that
/// outlasts the timeout is rolled back and tried again at half its size.
async fn map_batches(connection: &mut PgConnection, pass: Pass, from: i64, to: i64) -> Result<u64> {
    let started = Instant::now();
    let mut reported = Instant::now();
    let mut rows = BATCH_START;
    let mut next = from;
    let mut mapped: u64 = 0;
    while next < to {
        let upper = next.saturating_add(rows).min(to);
        let batch = Instant::now();
        let mut tx = connection.begin().await?;
        let statement = match pass {
            Pass::Backfill => sqlx::query(BATCH).bind(next).bind(upper),
            Pass::Recent { min_height } => sqlx::query(RECENT_BATCH)
                .bind(next)
                .bind(upper)
                .bind(min_height),
        };
        let inserted = match statement.execute(&mut *tx).await {
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
                    ?pass,
                    next_seq = next,
                    upper,
                    retry_seqs = rows,
                    "a share-hash backfill batch outlasted the statement timeout; retrying it at half the size"
                );
                continue;
            }
            Err(error) => {
                let context = match pass {
                    Pass::Backfill => format!("migration 2: backfilling qbit_prism_share_hashes for share_seq {next} to {upper}; every earlier batch committed, so migrate again to resume from share_seq {next}"),
                    Pass::Recent { min_height } => format!("migration 2: mapping the recent range of qbit_prism_share_hashes, template height {min_height} and above, for share_seq {next} to {upper}; every earlier batch committed, and `qbit-prism-server migrate --defer-share-hashes` maps the range again from its start, keeping them"),
                };
                return Err(anyhow::Error::from(error).context(context));
            }
        };
        if pass == Pass::Backfill {
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
        }
        tx.commit().await?;
        mapped += inserted;
        next = upper;
        rows = next_batch(rows, batch.elapsed());
        if reported.elapsed() >= REPORT_EVERY {
            reported = Instant::now();
            let rate = (next - from) as f64 / started.elapsed().as_secs_f64().max(0.001);
            tracing::info!(
                version = VERSION,
                ?pass,
                next_seq = next,
                end_seq = to,
                mapped,
                seqs_per_second = rate.round() as u64,
                remaining_s = ((to - next) as f64 / rate.max(1.0)).round() as u64,
                "share-hash backfill progress"
            );
        }
    }
    Ok(mapped)
}

/// Map the legacy shares the cursor has not passed, then drop the cursor
/// and record 2. Idempotent: a run finding no progress table finds 2
/// recorded by the run that dropped it. A backfill that permits serving
/// (the fence at 2) is mapped only by `ShareHashBackfill::Finish`, the
/// operator's `migrate`, and only up to the end its cursor was planned to;
/// every other connect leaves it as it is, and refuses one a deferred
/// `migrate` claimed at fence 1.
pub(super) async fn apply(
    connection: &mut PgConnection,
    backfill: ShareHashBackfill,
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
    // Read under the runners' lock, which the recent range's run holds
    // while it raises the fence to 2, so this run's answer holds until it
    // records 2: only a newer release's declaration can change it, and the
    // record step refuses that. `migrate_schema` planned this run from the
    // fence and the claim it read before the run waited for the lock, and a
    // deferred `migrate` can have changed either meanwhile, so the plan is
    // checked again here, before anything is mapped.
    let declared = fence(connection).await?;
    let serving = declared == Some(FENCE_SERVING);
    if backfill != ShareHashBackfill::Finish {
        if serving {
            tracing::info!(
                version = VERSION,
                next_seq = progress.next_seq,
                end_seq = progress.end_seq,
                "migration 2's share-hash backfill permits serving; `qbit-prism-server migrate` maps the rest"
            );
            return Ok(());
        }
        // Claimed meanwhile: the deferred run's recent range comes first,
        // before 013 drops the index that serves it, so this connect stops
        // ahead of 013, as `migrate_schema` stops one that finds the claim.
        if progress.deferred && declared == Some(FENCE_PENDING) {
            bail!("{}", progress.refusal(declared));
        }
    } else if serving {
        // Recording 2 under fence 2 needs the bound 017 records, so
        // `migrate_schema` schedules this run after 017. Planned at fence 1,
        // ahead of 017, it finds 2 here only if a deferred `migrate` raised
        // the fence while this run waited for the lock. If that run stopped
        // before 017, the record would refuse after every batch had run,
        // hours on a production ledger, so this run refuses first.
        ensure!(
            conversion_bound_covers(connection, progress.end_seq).await?,
            "refusing to finish migration 2's share-hash backfill before mapping anything: it permits serving now, with {PENDING_CAPABILITY} = {FENCE_SERVING}, but qbit_prism_share_partitioning records no conversion bound, which recording 2 needs, so migration 017 has not partitioned the share ledger yet. The share-hash fence changed while this run waited for the runner lock. Nothing was mapped; rerun `qbit-prism-server migrate`, which applies 017 before it finishes the backfill"
        );
    }

    let started = Instant::now();
    let first_seq = progress.next_seq;
    tracing::info!(
        version = VERSION,
        start_seq = progress.start_seq,
        next_seq = progress.next_seq,
        end_seq = progress.end_seq,
        "backfilling qbit_prism_share_hashes for the legacy shares in batches{}",
        if serving {
            " while frontends serve, up to the end the cursor was planned to"
        } else {
            "; every start refuses the database until migration 2 is recorded"
        }
    );
    let mut mapped: u64 = 0;
    loop {
        mapped += map_batches(
            connection,
            Pass::Backfill,
            progress.next_seq,
            progress.end_seq,
        )
        .await?;
        progress.next_seq = progress.end_seq;
        if serving {
            // Every legacy header is mapped by now, so the check covers
            // every native share that can repeat one. It reads them while
            // frontends serve, in short chunks, and before the migration
            // lock, which a starting frontend's migration waits for under
            // its lock timeout.
            refuse_double_credit(connection, progress.end_seq).await?;
        }
        let mut tx = connection.begin().await?;
        // Record 2 once the cursor has passed every legacy row, under the
        // migration lock. Like 013's and 017's records, this waits for the
        // migration lock and for any reader of the cursor table rather than
        // failing on the pool's timeouts once all the mapping is done.
        sqlx::query(
            "SELECT set_config('statement_timeout','0',true),set_config('lock_timeout','0',true)",
        )
        .execute(&mut *tx)
        .await?;
        lock(&mut tx, MIGRATION_LOCK, metrics).await?;
        // The fence is this release's at 1 or 2, or absent on a cursor an
        // earlier build created. Any other value was declared by a newer
        // release while this runner mapped without the migration lock. That
        // declaration is the newer release's to remove, with the cursor and
        // the record, so this runner leaves all three to it (#669).
        let fence = fence(&mut tx).await?;
        if let Some(value) = fence.filter(|value| !(FENCE_PENDING..=FENCE_SERVING).contains(value))
        {
            bail!("refusing to record migration 2: while its share-hash backfill ran, a newer PRISM release declared {PENDING_CAPABILITY} = {value}, but this server understands {PENDING_CAPABILITY} {FENCE_PENDING} to {FENCE_SERVING} only. That release finishes the backfill; upgrade the server before starting or migrating here");
        }
        // Only the recent range raises the fence, under the runners' lock,
        // which this run holds.
        ensure!(
            serving || fence != Some(FENCE_SERVING),
            "refusing to record migration 2: {PENDING_CAPABILITY} reached {FENCE_SERVING} while this run, which holds the runner lock, mapped the legacy shares with serving refused; migrate again"
        );
        if serving {
            // Frontends serve: every row at or above the planned end is a
            // native share, which mapped its own header as it was appended,
            // so the end stays where it was planned.
            check_frozen_end(&mut tx, &progress).await?;
        } else {
            // Nothing can append before 2 is recorded, so the end found at
            // migration is still the end; a row past it would be mapped by
            // another pass, not left unmapped.
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
        }
        sqlx::raw_sql("DROP TABLE qbit_prism_share_hash_backfill")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM qbit_prism_schema_capabilities WHERE capability=$1")
            .bind(PENDING_CAPABILITY)
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

/// What a backfill that permits serving checks under the migration lock
/// before it records 2: its cursor stands at the end it was planned to,
/// where this run left it, and that end, never extended, is the legacy
/// ledger's, so it lies within the release table that 017 attached as the
/// first partition, at or below the conversion bound. Native shares are
/// appended from it upwards, below that bound too until the sequence
/// passes it.
async fn check_frozen_end(tx: &mut Transaction<'_, Postgres>, progress: &Progress) -> Result<()> {
    let cursor: (i64, i64) = sqlx::query_as(
        "SELECT next_seq,end_seq FROM qbit_prism_share_hash_backfill WHERE singleton FOR UPDATE",
    )
    .fetch_one(&mut **tx)
    .await?;
    let end_seq = progress.end_seq;
    ensure!(
        cursor == (end_seq, end_seq),
        "refusing to record migration 2: its share-hash backfill's cursor reads next_seq {} and end_seq {}, not {end_seq} and {end_seq}, where this run, which holds the runner lock, left it; migrate again",
        cursor.0,
        cursor.1
    );
    ensure!(
        conversion_bound_covers(tx, end_seq).await?,
        "refusing to record migration 2: its share-hash backfill permits serving, but qbit_prism_share_partitioning records no conversion bound, so migration 017 has not partitioned the share ledger and nothing should have served it. Migrate again, which applies 017 before it finishes the backfill"
    );
    Ok(())
}

/// Whether 017 has recorded the conversion bound that recording 2 under
/// fence 2 needs: the release table holds the legacy shares below it. An
/// end above it is refused: the end the cursor was planned to would not be
/// the legacy ledger's.
async fn conversion_bound_covers(connection: &mut PgConnection, end_seq: i64) -> Result<bool> {
    let bound = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT conversion_bound FROM qbit_prism_share_partitioning WHERE singleton",
    )
    .fetch_optional(&mut *connection)
    .await?
    .flatten();
    let Some(bound) = bound else {
        return Ok(false);
    };
    ensure!(
        end_seq <= bound,
        "refusing to record migration 2: its share-hash backfill's end, share_seq {end_seq}, lies above the conversion bound {bound} under which the release table holds the legacy shares, so the end the cursor was planned to is not the legacy ledger's. The cursor or qbit_prism_share_partitioning was edited; restore the full backup"
    );
    Ok(true)
}

/// One chunk of the double-credit check: the first native share in
/// `[$2, $3)`, at or above the legacy end `$1`, that repeats the header of
/// an accepted legacy share below that end. Each native row costs two
/// probes, both on indexes 2.x built: its header in
/// `qbit_share_ledger_accepted_block_suffix_idx` (`lower(right(share_id,64))`,
/// partial on IDs of 65 characters or more, so the predicate repeats
/// `length(share_id)>=65`), and, for a legacy ID that is the bare 64-digit
/// header, the share ID unique index in lower and upper case. A bare ID in
/// mixed case is the one form neither probe finds; 2.x wrote header hashes
/// in one case.
const DOUBLE_CREDIT: &str = "SELECT n.share_id,n.share_seq FROM qbit_share_ledger n WHERE n.share_seq>=$2 AND n.share_seq<$3 AND n.accepted AND (EXISTS (SELECT 1 FROM qbit_share_ledger l WHERE l.accepted AND length(l.share_id)>=65 AND lower(right(l.share_id,64))=lower(right(n.share_id,64)) AND l.share_seq<$1 AND l.share_id<>n.share_id) OR EXISTS (SELECT 1 FROM qbit_share_ledger l WHERE l.share_id IN (lower(right(n.share_id,64)),upper(right(n.share_id,64))) AND l.accepted AND l.share_seq<$1 AND l.share_id<>n.share_id)) ORDER BY n.share_seq LIMIT 1";

/// Refuse to record 2 when a native share repeats the header of an accepted
/// legacy share below `end_seq`. The native share mapped the header first,
/// so the backfill's batch met ON CONFLICT DO NOTHING and said nothing, and
/// the header was credited twice. The recent range should have made that
/// impossible (see the module doc), so it is reported loudly, and the
/// cursor, the fence and the record stay as they are: every legacy header
/// is mapped by now, so no later share can repeat one, and frontends keep
/// serving.
///
/// The native shares are read from `end_seq` up to the last one appended
/// when the check starts, in chunks of consecutive `share_seq` sized as the
/// backfill's batches are, each one statement in a transaction of its own
/// under the pool's statement timeout; a chunk that outlasts it is tried
/// again at half its size. Days after a cutover a single statement over
/// them all would outlast that timeout, or, given none, hold a snapshot on
/// the serving primary for minutes, while every append updates the cluster
/// row (#738). Shares appended once the check has started need no reading:
/// every legacy header is mapped by then, committed by the batches before
/// it, so an append that repeats one finds it credited at its probe, or,
/// if its probe ran before the batch that mapped the header committed,
/// fails on the header's key when it maps its own header, an insert with
/// no ON CONFLICT clause. A share that did repeat one mapped the header
/// before the batch that reached the legacy copy, and that batch's ON
/// CONFLICT waited for the append's transaction to end, so the share was
/// committed before the check started, at or below the last `share_seq`
/// the check reads first.
async fn refuse_double_credit(connection: &mut PgConnection, end_seq: i64) -> Result<()> {
    let last: Option<i64> =
        sqlx::query_scalar("SELECT max(share_seq) FROM qbit_share_ledger WHERE share_seq>=$1")
            .bind(end_seq)
            .fetch_one(&mut *connection)
            .await?;
    let Some(last) = last else {
        return Ok(());
    };
    let to = last.saturating_add(1);
    let started = Instant::now();
    let mut reported = Instant::now();
    let mut rows = BATCH_START;
    let mut next = end_seq;
    while next < to {
        let upper = next.saturating_add(rows).min(to);
        let chunk = Instant::now();
        let repeat: Option<(String, i64)> = match sqlx::query_as(DOUBLE_CREDIT)
            .bind(end_seq)
            .bind(next)
            .bind(upper)
            .fetch_optional(&mut *connection)
            .await
        {
            Ok(repeat) => repeat,
            // Only this chunk is lost; the same range is read again at half
            // the size.
            Err(sqlx::Error::Database(error))
                if rows > BATCH_MIN && statement_timed_out(&*error) =>
            {
                rows = (rows / 2).max(BATCH_MIN);
                tracing::warn!(
                    version = VERSION,
                    next_seq = next,
                    upper,
                    retry_seqs = rows,
                    "a chunk of the share-hash backfill's double-credit check outlasted the statement timeout; retrying it at half the size"
                );
                continue;
            }
            Err(error) => {
                return Err(anyhow::Error::from(error).context(format!("migration 2: checking the native shares from share_seq {next} to {upper} for a header an accepted legacy share holds, before recording 2. Every batch of the backfill committed, and 2 is not recorded; migrate again")));
            }
        };
        if let Some((share_id, share_seq)) = repeat {
            let header = share_id
                .get(share_id.len().saturating_sub(64)..)
                .unwrap_or(&share_id)
                .to_ascii_lowercase();
            bail!("refusing to record migration 2: native share {share_id}, at share_seq {share_seq}, repeats header {header} of an accepted legacy share below share_seq {end_seq}, so that header was credited twice. The recent range mapped before serving should have refused it: the chain reorganized more than {RECENT_HEIGHTS} blocks below the legacy tip, or a native job reproduced a legacy header. The mapping is otherwise complete, so no later share can repeat a legacy header, and frontends may keep serving; the cursor, {PENDING_CAPABILITY} = {FENCE_SERVING} and the missing record of 2 stay as they are. Report the double credit and reconcile it before anything records 2");
        }
        next = upper;
        rows = next_batch(rows, chunk.elapsed());
        if reported.elapsed() >= REPORT_EVERY {
            reported = Instant::now();
            tracing::info!(
                version = VERSION,
                next_seq = next,
                last_seq = last,
                "share-hash backfill double-credit check progress"
            );
        }
    }
    tracing::info!(
        version = VERSION,
        end_seq,
        last_seq = last,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "no native share repeats the header of an accepted legacy share"
    );
    Ok(())
}

/// The recent range, run by `migrate --defer-share-hashes` in the slot the
/// backfill takes otherwise, before 013 drops 2.x's
/// `qbit_share_ledger_template_height_idx`, which serves its bounds: map
/// every accepted legacy share whose template height is within
/// `RECENT_HEIGHTS` of the highest, then raise the fence to 2, which
/// permits serving with the rest pending (see the module doc). The cursor
/// does not move: plain `migrate` maps the whole backfill later, and meets
/// the range's headers again with ON CONFLICT DO NOTHING. The transaction
/// that raises the fence holds the ledger's share_seq sequence at or above
/// the cursor's end, so that end stays below every native share. Idempotent:
/// an interrupted run maps the range again, and a backfill that permits
/// serving already, or that has finished, is left as it is.
pub(super) async fn map_recent(
    connection: &mut PgConnection,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    acquire_runner_lock(connection).await?;
    let Some(progress) = progress(connection).await? else {
        ensure!(
            recorded(connection, VERSION).await?,
            "refusing to continue migration 2: its share-hash backfill's progress table is gone, but 2 is not recorded. Only the transaction that records 2 drops it, so it was dropped by hand. Restore the full backup"
        );
        tracing::info!(
            version = VERSION,
            "share-hash backfill already complete; nothing to defer"
        );
        return Ok(());
    };
    match fence(connection).await? {
        Some(FENCE_PENDING) => {}
        Some(FENCE_SERVING) => {
            tracing::info!(
                version = VERSION,
                next_seq = progress.next_seq,
                end_seq = progress.end_seq,
                "the recent range of migration 2's share-hash backfill is mapped and serving permitted already; `qbit-prism-server migrate` maps the rest"
            );
            return Ok(());
        }
        None => bail!("{}", unfenced_defer_refusal(&progress)),
        Some(value) => bail!("refusing to defer migration 2's share-hash backfill: a newer PRISM release declared {PENDING_CAPABILITY} = {value}, but this server understands {PENDING_CAPABILITY} {FENCE_PENDING} to {FENCE_SERVING} only. That release finishes the backfill; upgrade the server before starting or migrating here"),
    }
    let started = Instant::now();
    // The range's bounds, each read from 2.x's (template_height, share_seq)
    // index: the lowest template height it maps, and the first share at or
    // above that height. Nothing has appended since the cursor was planned,
    // so every row below its end is a legacy share. A bare min(share_seq)
    // is planned as a walk of the primary key from the ledger's first row,
    // which on a ledger in height order reads every row below the range
    // before it meets the range; the materialized CTE reads only the
    // range's index entries.
    let min_height: i64 = sqlx::query_scalar(
        "SELECT GREATEST(0,max(template_height)-$2) FROM qbit_share_ledger WHERE accepted AND share_seq<$1",
    )
    .bind(progress.end_seq)
    .bind(RECENT_HEIGHTS)
    .fetch_one(&mut *connection)
    .await?;
    let start_seq: Option<i64> = sqlx::query_scalar(
        "WITH recent AS MATERIALIZED (SELECT share_seq FROM qbit_share_ledger WHERE accepted AND template_height>=$2 AND share_seq<$1) SELECT min(share_seq) FROM recent",
    )
    .bind(progress.end_seq)
    .bind(min_height)
    .fetch_one(&mut *connection)
    .await?;
    tracing::info!(
        version = VERSION,
        recent_min_height = min_height,
        recent_start_seq = ?start_seq,
        end_seq = progress.end_seq,
        "mapping the recent range of migration 2's share-hash backfill; every start refuses the database until serving is permitted"
    );
    let mapped = match start_seq {
        Some(start_seq) => {
            map_batches(
                connection,
                Pass::Recent { min_height },
                start_seq,
                progress.end_seq,
            )
            .await?
        }
        // No accepted legacy share at all: nothing to map, and no header a
        // native share could repeat.
        None => 0,
    };
    // Serving is permitted in one transaction with the range's record, and
    // only while nothing has appended since the cursor was planned: the
    // range's end is the ledger's.
    let mut tx = connection.begin().await?;
    sqlx::query(
        "SELECT set_config('statement_timeout','0',true),set_config('lock_timeout','0',true)",
    )
    .execute(&mut *tx)
    .await?;
    lock(&mut tx, MIGRATION_LOCK, metrics).await?;
    let fence = fence(&mut tx).await?;
    ensure!(
        fence == Some(FENCE_PENDING),
        "refusing to permit serving with migration 2's share-hash backfill pending: {PENDING_CAPABILITY} changed to {} while this run, which holds the runner lock, mapped the recent range; migrate again",
        fence.map_or("nothing".to_owned(), |value| value.to_string())
    );
    let end_seq: i64 =
        sqlx::query_scalar("SELECT COALESCE(max(share_seq),-1)+1 FROM qbit_share_ledger")
            .fetch_one(&mut *tx)
            .await?;
    ensure!(
        end_seq == progress.end_seq,
        "refusing to permit serving with migration 2's share-hash backfill pending: the share ledger's end is share_seq {end_seq}, not the end {} the backfill was planned to, and nothing appends before serving is permitted, so a writer went around every gate. Nothing serves meanwhile. Run `qbit-prism-server migrate` without --defer-share-hashes, which maps every row and records 2",
        progress.end_seq
    );
    hold_share_seq_at_or_above(&mut tx, progress.end_seq).await?;
    sqlx::raw_sql("ALTER TABLE qbit_prism_share_hash_backfill ADD COLUMN IF NOT EXISTS recent_min_height bigint, ADD COLUMN IF NOT EXISTS recent_start_seq bigint")
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE qbit_prism_share_hash_backfill SET recent_min_height=$1,recent_start_seq=$2 WHERE singleton")
        .bind(min_height)
        .bind(start_seq)
        .execute(&mut *tx)
        .await?;
    let raised = sqlx::query("UPDATE qbit_prism_schema_capabilities SET capability_value=$2 WHERE capability=$1 AND capability_value=$3")
        .bind(PENDING_CAPABILITY)
        .bind(FENCE_SERVING)
        .bind(FENCE_PENDING)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    ensure!(
        raised == 1,
        "refusing to permit serving with migration 2's share-hash backfill pending: {PENDING_CAPABILITY} is no longer {FENCE_PENDING}; migrate again"
    );
    tx.commit().await?;
    tracing::info!(
        version = VERSION,
        recent_min_height = min_height,
        recent_start_seq = ?start_seq,
        next_seq = progress.next_seq,
        end_seq = progress.end_seq,
        mapped,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "mapped the recent range of migration 2's share-hash backfill; serving is permitted, and `qbit-prism-server migrate` maps the rest and records 2"
    );
    Ok(())
}

/// Hold the share ledger's share_seq sequence at or above `end_seq`, the
/// frozen end, in the transaction that permits serving. Native shares draw
/// their share_seq from it, and plain `migrate` takes every row below the
/// end for a legacy share: a native one there would escape the
/// double-credit check, which reads from the end up. The ledger's rows end
/// below `end_seq`, but the sequence can lag behind them, after rows were
/// inserted with explicit values or a restore left it uncalled. Only then
/// is it moved, to hand out `end_seq` next, and logged; otherwise nothing
/// is written to it, so its `last_value` and `is_called` stay as the source
/// had them, which the cutover's evidence compares. A sequence moves
/// outside transactions: should this one roll back, the sequence keeps the
/// move, which only skips numbers no row holds. It is found by the column
/// that owns it.
async fn hold_share_seq_at_or_above(
    tx: &mut Transaction<'_, Postgres>,
    end_seq: i64,
) -> Result<()> {
    let sequence: String = sqlx::query_scalar::<_, Option<String>>(
        "SELECT pg_get_serial_sequence('qbit_share_ledger','share_seq')",
    )
    .fetch_one(&mut **tx)
    .await?
    .context("refusing to permit serving with migration 2's share-hash backfill pending: no sequence is owned by qbit_share_ledger.share_seq, so nothing holds the native shares above the backfill's end. Nothing serves meanwhile. Run `qbit-prism-server migrate` without --defer-share-hashes, which maps every row and records 2")?;
    let mut state = sequence_state(tx, &sequence).await?;
    if next_value(state) < end_seq {
        sqlx::query("SELECT setval($1::regclass,$2,true)")
            .bind(&sequence)
            .bind(end_seq - 1)
            .execute(&mut **tx)
            .await?;
        let moved = sequence_state(tx, &sequence).await?;
        tracing::info!(
            version = VERSION,
            sequence = %sequence,
            previous_last_value = state.0,
            previous_is_called = state.1,
            last_value = moved.0,
            is_called = moved.1,
            end_seq,
            "moved the share ledger's share_seq sequence up to migration 2's share-hash backfill end: it lagged behind the ledger's rows, and a native share appended below the end would be taken for a legacy one"
        );
        state = moved;
    }
    ensure!(
        next_value(state) >= end_seq,
        "refusing to permit serving with migration 2's share-hash backfill pending: the share_seq sequence {sequence} hands out {} next, below the backfill's end {end_seq}, so a native share would be taken for a legacy one. Nothing serves meanwhile. Run `qbit-prism-server migrate --defer-share-hashes` again",
        next_value(state)
    );
    Ok(())
}

/// What `sequence` holds, as `(last_value, is_called)`. A sequence reads
/// as a one-row relation, under the name `pg_get_serial_sequence` renders,
/// quoted where it must be.
async fn sequence_state(connection: &mut PgConnection, sequence: &str) -> Result<(i64, bool)> {
    let read = format!("SELECT last_value,is_called FROM {sequence}");
    Ok(sqlx::query_as(&read).fetch_one(&mut *connection).await?)
}

/// The value a sequence in `state` hands out next: one past `last_value`
/// once it has been called, `last_value` itself before.
fn next_value((last_value, is_called): (i64, bool)) -> i64 {
    if is_called {
        last_value.saturating_add(1)
    } else {
        last_value
    }
}

/// Refuse a share-archive restore while a backfill is pending, at any
/// fence. An attached restore maps its rows' headers itself, which the
/// earliest-share rule needs the backfill to have done first, so it waits
/// for 2. Checked under the lifecycle lock, before the restore reads
/// anything.
pub(crate) async fn refuse_restore_while_pending(connection: &mut PgConnection) -> Result<()> {
    if let Some(progress) = progress(connection).await? {
        bail!(
            "refusing to restore a share archive while migration 2's share-hash backfill is pending: the legacy shares from share_seq {} up to {} are not all mapped in qbit_prism_share_hashes, and a restore maps the headers of the rows it restores. Run `qbit-prism-server migrate` to finish the backfill and record 2, then restore. Nothing was changed",
            progress.next_seq,
            progress.end_seq
        );
    }
    Ok(())
}

/// Refuse to detach or drop any share-ledger partition while a backfill is
/// pending, at any fence. The backfill maps the legacy shares below the
/// cursor's end from the attached ledger, and before it records 2 the
/// double-credit check reads every native share from that end up through
/// the attached parent too (`refuse_double_credit`). A partition gone from
/// the parent would hide legacy shares from the one, which would never be
/// mapped, or native shares from the other, so 2 could be recorded over a
/// header credited twice. So every partition stays attached until 2 is
/// recorded, the release table and the native ones alike, whatever its
/// bounds. Checked under the lifecycle lock, before the step reads
/// anything else.
pub(crate) async fn refuse_departure_while_pending(
    connection: &mut PgConnection,
    step: &str,
    partition_name: &str,
) -> Result<()> {
    if let Some(progress) = progress(connection).await? {
        bail!(
            "refusing to {step} {partition_name} while migration 2's share-hash backfill is pending: the backfill maps the legacy shares below share_seq {end} from the attached share ledger, and before it records 2 it checks every native share from there up, through the attached ledger too, for a header a legacy share holds, so every partition stays attached until 2 is recorded. Run `qbit-prism-server migrate` to finish the backfill and record 2, then {step} it. Nothing was changed",
            end = progress.end_seq
        );
    }
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
    fn a_recent_batch_is_a_batch_restricted_to_its_heights() {
        // The recent range's earliest-share rule is the backfill's only while
        // the two statements differ by the height alone.
        assert_eq!(RECENT_BATCH.replace(" AND template_height>=$3", ""), BATCH);
    }

    #[test]
    fn a_double_credit_chunk_is_the_whole_check_restricted_to_its_range() {
        // The check over every native share at or above the end `$1`, as
        // one statement. The chunks, consecutive from the end up, read the
        // same rows with the same two probes only while the two agree.
        let whole = "SELECT n.share_id,n.share_seq FROM qbit_share_ledger n WHERE n.share_seq>=$1 AND n.accepted AND (EXISTS (SELECT 1 FROM qbit_share_ledger l WHERE l.accepted AND length(l.share_id)>=65 AND lower(right(l.share_id,64))=lower(right(n.share_id,64)) AND l.share_seq<$1 AND l.share_id<>n.share_id) OR EXISTS (SELECT 1 FROM qbit_share_ledger l WHERE l.share_id IN (lower(right(n.share_id,64)),upper(right(n.share_id,64))) AND l.accepted AND l.share_seq<$1 AND l.share_id<>n.share_id)) ORDER BY n.share_seq LIMIT 1";
        assert_eq!(
            DOUBLE_CREDIT.replace("n.share_seq>=$2 AND n.share_seq<$3", "n.share_seq>=$1"),
            whole
        );
    }

    #[test]
    fn a_sequence_hands_out_last_value_next_until_it_is_called() {
        assert_eq!(next_value((41, true)), 42);
        assert_eq!(next_value((42, false)), 42);
        assert_eq!(next_value((i64::MAX, true)), i64::MAX);
    }

    #[test]
    fn the_recent_range_spans_the_coinbase_maturity() {
        assert_eq!(
            RECENT_HEIGHTS,
            i64::try_from(qbit_prism::QBIT_COINBASE_MATURITY_BLOCKS).unwrap()
        );
    }

    #[test]
    fn the_refusal_names_the_cursor_and_the_remedy() {
        let progress = Progress {
            start_seq: 1,
            next_seq: 40,
            end_seq: 100,
            deferred: false,
        };
        let claimed = Progress {
            deferred: true,
            ..progress
        };
        for (progress, fence) in [
            (progress, Some(FENCE_PENDING)),
            (progress, None),
            (claimed, Some(FENCE_PENDING)),
        ] {
            let refusal = progress.refusal(fence);
            assert!(
                refusal.contains(
                    "from share_seq 1 up to 40 are mapped in qbit_prism_share_hashes and those from 40 up to 100 are not"
                ),
                "{refusal}"
            );
            assert!(refusal.contains("`qbit-prism-server migrate`"), "{refusal}");
        }
        // Only a fenced backfill can be deferred: one a build before #669
        // started is finished by plain `migrate` alone.
        assert!(progress
            .refusal(Some(FENCE_PENDING))
            .contains("`qbit-prism-server migrate --defer-share-hashes`"));
        assert!(!progress.refusal(None).contains("--defer-share-hashes"));
        assert!(unfenced_defer_refusal(&progress)
            .contains("without --defer-share-hashes, which maps the legacy shares from share_seq 40 up to 100"));
        // A deferred run that stopped before its recent range is named, and
        // so is its retry, ahead of plain `migrate`.
        let refusal = claimed.refusal(Some(FENCE_PENDING));
        assert!(
            refusal.contains("`qbit-prism-server migrate --defer-share-hashes` claimed it and stopped before its recent range was mapped")
                && refusal.contains("Run `qbit-prism-server migrate --defer-share-hashes` again")
                && refusal.contains("or plain `qbit-prism-server migrate`"),
            "{refusal}"
        );
        assert!(!progress
            .refusal(Some(FENCE_PENDING))
            .contains("stopped before its recent range"));
    }

    #[test]
    fn an_orphaned_fence_is_named_at_its_value() {
        for value in [FENCE_PENDING, FENCE_SERVING] {
            let refusal = orphaned_fence_refusal(value);
            assert!(
                refusal.contains(&format!("declares share_hash_backfill_pending = {value}, but migration 2's share-hash backfill cursor qbit_prism_share_hash_backfill is gone")),
                "{refusal}"
            );
        }
        // Once frontends may have served, the pre-migration backup is no
        // remedy.
        assert!(
            orphaned_fence_refusal(FENCE_PENDING).contains("Restore the full pre-migration backup")
        );
        assert!(!orphaned_fence_refusal(FENCE_SERVING)
            .contains("Restore the full pre-migration backup"));
    }
}
