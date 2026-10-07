//! Migration 002's share-hash backfill on a populated 2.x.x source (#582).
//!
//! 002 maps the header of every accepted legacy share whose ID ends in 64 hex
//! digits, the earliest `share_seq` winning. That used to be one statement
//! inside the migration transaction, which outlasted the statement timeout
//! on a production-sized ledger. It now runs after the commit, in batches of
//! consecutive `share_seq` with a durable cursor, and records 2 last. These
//! tests hold the batched result to the single statement's on a ledger that
//! repeats headers across batch boundaries, interrupt a run mid-batch and
//! resume it, and check that nothing serves the database in between. With
//! `migrate --defer-share-hashes` only the recent range is mapped before
//! the database serves, and plain `migrate` maps the rest while frontends
//! append; the last tests here hold that to the same rule.
use super::*;
use qbit_prism_server::ledger::{archive, ShareHashBackfill, REQUIRED_SCHEMA_VERSIONS};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// The class of the online runners' session lock (`ONLINE_DDL_LOCK_CLASS`),
/// keyed by the ledger's schema.
const RUNNER_LOCK_CLASS: i32 = 0x5052_4953;
/// Ledger rows the seed writes, before its gaps: several batches at the
/// runner's first batch of 10,000 `share_seq`.
const SEED_ROWS: i64 = 60_000;
/// A share whose header first appears in a late batch; `share(LATE)` repeats
/// that header under another identity.
const LATE: u64 = 50_001;

/// A 2.x.x ledger that exercises 002's rule: one header per share, with
/// gaps in `share_seq`; every 97th row repeats, in upper case, the header of
/// the share 9,001 below or above it, so the earlier copy wins whichever
/// side it is on and the pairs straddle batch boundaries; non-hex legacy
/// IDs, bare 64-digit IDs and rejected rows among them.
async fn seed(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,reject_reason,writer_id,writer_epoch) \
         SELECT g,i.id,'miner-'||g%5,'miner-'||g%5,decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(1),1,to_timestamp(2),g%1000<>13,CASE WHEN g%1000=13 THEN 'duplicate-share' END,'backfill-test',0 \
         FROM generate_series(1,$1::bigint) g, LATERAL (SELECT CASE \
             WHEN g%97=0 THEN 'repeat-'||g||':'||upper(lpad(to_hex(g+CASE WHEN g%2=0 THEN -9001 ELSE 9001 END),64,'0')) \
             WHEN g%1000=7 THEN 'legacy:'||g \
             WHEN g%1000=11 THEN lpad(to_hex(g),64,'0') \
             ELSE 'worker-'||g%5||':'||lpad(to_hex(g),64,'0') END AS id) i \
         WHERE g%89<>0",
    )
    .bind(SEED_ROWS)
    .execute(pool)
    .await?;
    sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1)")
        .bind(SEED_ROWS)
        .execute(pool)
        .await?;
    Ok(())
}

/// What 002's single statement mapped, for the legacy shares below `below`
/// (all of them with `i64::MAX`): the header of each accepted share whose
/// ID ends in 64 hex digits, to its earliest share.
async fn expected_mapping(pool: &PgPool, below: i64) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT DISTINCT ON (lower(right(share_id,64))) lower(right(share_id,64)),share_id FROM qbit_share_ledger WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$' AND share_seq<$1 ORDER BY lower(right(share_id,64)),share_seq",
    )
    .bind(below)
    .fetch_all(pool)
    .await?)
}

async fn mapping(pool: &PgPool) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT header_hash,share_id FROM qbit_prism_share_hashes ORDER BY header_hash",
    )
    .fetch_all(pool)
    .await?)
}

async fn schema_versions(pool: &PgPool) -> Result<Vec<i32>> {
    Ok(
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(pool)
            .await?,
    )
}

/// The cursor of a pending backfill, `None` once its table is gone.
async fn cursor(pool: &PgPool) -> Result<Option<(i64, i64)>> {
    if !sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('qbit_prism_share_hash_backfill') IS NOT NULL",
    )
    .fetch_one(pool)
    .await?
    {
        return Ok(None);
    }
    Ok(Some(
        sqlx::query_as("SELECT next_seq,end_seq FROM qbit_prism_share_hash_backfill")
            .fetch_one(pool)
            .await?,
    ))
}

/// 2 is recorded after every migration of the transaction and before the
/// online 013, 017 and 024, which each check that they come after every
/// lower version.
async fn assert_recorded_in_order(pool: &PgPool) -> Result<()> {
    let (after_transaction, before_online): (bool, bool) = sqlx::query_as(
        "SELECT (SELECT applied_at FROM qbit_prism_schema_migrations WHERE version=2) >= (SELECT max(applied_at) FROM qbit_prism_schema_migrations WHERE version NOT IN (2,13,17,24)), \
                (SELECT applied_at FROM qbit_prism_schema_migrations WHERE version=2) <= (SELECT min(applied_at) FROM qbit_prism_schema_migrations WHERE version IN (13,17,24))",
    )
    .fetch_one(pool)
    .await?;
    ensure!(
        after_transaction && before_online,
        "2 is not recorded between the transaction's migrations and 013, 017 and 024"
    );
    Ok(())
}

/// Every start refuses a database whose backfill has not finished, naming
/// its cursor, whether or not 2 is recorded.
async fn assert_refused_at(url: &str, next_seq: i64, end_seq: i64) -> Result<()> {
    let error = Ledger::connect(url, "cold".into(), 8, false)
        .await
        .err()
        .context("a start accepted a database whose share-hash backfill has not finished")?
        .to_string();
    ensure!(
        error.contains("migration 2's share-hash backfill has not finished")
            && error.contains(&format!("those from {next_seq} up to {end_seq} are not")),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn migration_002_maps_a_populated_2x_ledger_in_batches_as_its_single_statement_did(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let expected = expected_mapping(&pool, i64::MAX).await?;
    // The seed exercises every branch of the rule.
    ensure!(expected.iter().any(|(_, id)| id.starts_with("repeat-")));
    ensure!(expected.iter().any(|(_, id)| id.len() == 64));
    ensure!(!expected.iter().any(|(_, id)| id.starts_with("legacy:")));
    let ledger = db.ledger("cutover").await?;
    assert_eq!(mapping(&pool).await?, expected);
    assert_eq!(cursor(&pool).await?, None, "the progress table outlived 2");
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_recorded_in_order(&pool).await?;
    // A legacy header stays credited to its legacy share.
    let error = ledger
        .append(share(LATE), None)
        .await
        .err()
        .context("a native share repeating a legacy header was credited")?;
    assert!(
        format!("{error:#}").contains("header already credited globally"),
        "{error:#}"
    );
    // A second start has nothing left to map.
    let again = db.ledger("again").await?;
    assert_eq!(mapping(&pool).await?, expected);
    db.close(vec![ledger, again]).await
}

#[tokio::test]
async fn migration_002_backfill_resumes_from_its_last_committed_batch_after_an_interrupt(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let expected = expected_mapping(&pool, i64::MAX).await?;
    let (first, end): (i64, i64) =
        sqlx::query_as("SELECT min(share_seq),max(share_seq)+1 FROM qbit_share_ledger")
            .fetch_one(&pool)
            .await?;
    let late = format!("{LATE:064x}");
    let late_seq: i64 = sqlx::query_scalar(
        "SELECT min(share_seq) FROM qbit_share_ledger WHERE accepted AND lower(right(share_id,64))=$1",
    )
    .bind(&late)
    .fetch_one(&pool)
    .await?;
    assert_eq!(late_seq, LATE as i64, "the seed moved the late header");

    // Hold the runners' lock so the migration transaction commits and the
    // backfill waits before its first batch.
    let mut runners = PgConnection::connect(&db.url).await?;
    sqlx::query("SELECT pg_advisory_lock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let mut migrate = Box::pin(Ledger::connect(&db.url, "interrupted".into(), 8, true));
    let committed = timeout(Duration::from_secs(60), async {
        while cursor(&pool).await?.is_none() {
            sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    });
    tokio::select! {
        result = &mut migrate => bail!("migrate ended before the backfill ran: {:?}", result.err()),
        result = committed => result.context("the migration transaction never committed")??,
    }
    // Committed: every migration but 2 and the online 013, 017 and 024 recorded,
    // nothing mapped yet, and no start serves the database.
    let pending: Vec<i32> = REQUIRED_SCHEMA_VERSIONS
        .iter()
        .copied()
        .filter(|version| ![2, 13, 17, 24].contains(version))
        .collect();
    assert_eq!(schema_versions(&pool).await?, pending);
    assert_eq!(cursor(&pool).await?, Some((first, end)));
    assert!(mapping(&pool).await?.is_empty());
    assert_refused_at(&db.url, first, end).await?;

    // An open insert of the late header stops the batch that reaches it.
    let mut blocker = pool.begin().await?;
    sqlx::query(
        "INSERT INTO qbit_prism_share_hashes(header_hash,share_id) VALUES($1,'blocker:'||$1)",
    )
    .bind(&late)
    .execute(&mut *blocker)
    .await?;
    sqlx::query("SELECT pg_advisory_unlock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let waiting = timeout(Duration::from_secs(60), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'INSERT INTO qbit_prism_share_hashes%' AND wait_event_type='Lock'")
                .fetch_optional(&pool)
                .await?;
            if let Some(pid) = pid {
                return Ok::<_, anyhow::Error>(pid);
            }
            sleep(Duration::from_millis(10)).await;
        }
    });
    let pid: i32 = tokio::select! {
        result = &mut migrate => bail!("migrate ended before its batch reached the late header: {:?}", result.err()),
        result = waiting => result.context("no batch waited on the late header")??,
    };
    // Interrupt the run mid-batch, as a crash or a killed migrate would, at
    // once: the waiting batch also runs under the pool's lock timeout.
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .execute(&pool)
        .await?;
    let failed = timeout(Duration::from_secs(30), migrate).await?;
    ensure!(
        failed.is_err(),
        "migrate succeeded although its batch was killed"
    );
    blocker.rollback().await?;
    // Earlier batches committed and the cursor says where they ended. The
    // killed batch left nothing: the mapping is the single statement's over
    // exactly that prefix, 2 is not recorded, and starts still refuse.
    let (next, planned_end) = cursor(&pool).await?.context("the progress table is gone")?;
    assert_eq!(planned_end, end);
    assert!(
        next > first && next <= late_seq,
        "cursor {next} is not past the first batch and before {late_seq}"
    );
    assert_eq!(mapping(&pool).await?, expected_mapping(&pool, next).await?);
    assert_eq!(schema_versions(&pool).await?, pending);
    assert_refused_at(&db.url, next, end).await?;
    // A hand-recorded 2 does not let a start serve while the table says
    // legacy shares are unmapped.
    sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
        .execute(&pool)
        .await?;
    assert_refused_at(&db.url, next, end).await?;
    sqlx::query("DELETE FROM qbit_prism_schema_migrations WHERE version=2")
        .execute(&pool)
        .await?;

    // migrate resumes at the cursor and finishes: the single statement's
    // mapping, 2 recorded before 013, 017 and 024, the progress table gone.
    let resumed = timeout(Duration::from_secs(120), db.ledger("resumed")).await??;
    assert_eq!(mapping(&pool).await?, expected);
    assert_eq!(cursor(&pool).await?, None);
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_recorded_in_order(&pool).await?;
    let error = resumed
        .append(share(LATE), None)
        .await
        .err()
        .context("a native share repeating a legacy header was credited")?;
    assert!(
        format!("{error:#}").contains("header already credited globally"),
        "{error:#}"
    );
    runners.close().await?;
    db.close(vec![resumed]).await
}

/// Nothing appends before 2 is recorded, so the end the cursor was planned
/// to is the ledger's end. A row past it, from a writer that went around
/// every gate, is still mapped before 2 is recorded, not left unmapped.
#[tokio::test]
async fn migration_002_maps_a_row_past_its_planned_end_before_recording_2() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let mut runners = PgConnection::connect(&db.url).await?;
    sqlx::query("SELECT pg_advisory_lock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let mut migrate = Box::pin(Ledger::connect(&db.url, "cutover".into(), 8, true));
    let committed = timeout(Duration::from_secs(60), async {
        while cursor(&pool).await?.is_none() {
            sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    });
    tokio::select! {
        result = &mut migrate => bail!("migrate ended before the backfill ran: {:?}", result.err()),
        result = committed => result.context("the migration transaction never committed")??,
    }
    let (_, end) = cursor(&pool).await?.context("the progress table is gone")?;
    let straggler = "ee".repeat(32);
    sqlx::query(
        "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) \
         VALUES($1,'straggler:'||$2,'miner-0','miner-0',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(1),1,to_timestamp(2),true,'backfill-test',0)",
    )
    .bind(end + 5)
    .bind(&straggler)
    .execute(&pool)
    .await?;
    sqlx::query("SELECT pg_advisory_unlock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let ledger = timeout(Duration::from_secs(120), migrate).await??;
    assert_eq!(
        mapping(&pool).await?,
        expected_mapping(&pool, i64::MAX).await?
    );
    let mapped: Option<String> =
        sqlx::query_scalar("SELECT share_id FROM qbit_prism_share_hashes WHERE header_hash=$1")
            .bind(&straggler)
            .fetch_optional(&pool)
            .await?;
    assert_eq!(mapped, Some(format!("straggler:{straggler}")));
    assert_eq!(cursor(&pool).await?, None);
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    runners.close().await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn migration_002_refuses_a_2x_source_holding_the_backfill_progress_name_before_any_ddl(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    sqlx::raw_sql("CREATE TABLE qbit_prism_share_hash_backfill(operator_notes text)")
        .execute(&pool)
        .await?;
    let error = db
        .ledger("cutover")
        .await
        .err()
        .context("migrate adopted a foreign qbit_prism_share_hash_backfill")?
        .to_string();
    assert!(
        error.contains("a table named qbit_prism_share_hash_backfill already exists")
            && error.contains("Nothing was changed"),
        "{error}"
    );
    let untouched: bool = sqlx::query_scalar(
        "SELECT to_regclass('qbit_prism_schema_migrations') IS NULL AND to_regclass('qbit_prism_share_hashes') IS NULL",
    )
    .fetch_one(&pool)
    .await?;
    assert!(untouched, "the refused migrate changed the source");
    db.close(Vec::new()).await
}

/// The value the database declares the pending backfill's fence at, if
/// any (#669).
async fn fence_value(pool: &PgPool) -> Result<Option<i32>> {
    Ok(sqlx::query_scalar(
        "SELECT capability_value FROM qbit_prism_schema_capabilities WHERE capability='share_hash_backfill_pending'",
    )
    .fetch_optional(pool)
    .await?)
}

/// Whether the database declares the pending backfill's fence (#669).
async fn fence_declared(pool: &PgPool) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM qbit_prism_schema_capabilities WHERE capability='share_hash_backfill_pending' AND capability_value=1)",
    )
    .fetch_one(pool)
    .await?)
}

/// Commit the migration transaction of a populated 2.x.x source and leave
/// its backfill pending: the runners' lock is held while it commits, and the
/// waiting `migrate` is then dropped. Returns the connection holding the
/// lock.
async fn pending_backfill(db: &Database, pool: &PgPool) -> Result<PgConnection> {
    stop_after_the_transaction(
        db,
        pool,
        Ledger::connect(&db.url, "pending".into(), 8, true),
    )
    .await
}

/// Run `migrate` on a populated 2.x.x source until its migration
/// transaction has committed the cursor, then drop it before its online
/// part runs, as a crash or a killed `migrate` would: the runners' lock is
/// held meanwhile. Returns the connection holding the lock.
async fn stop_after_the_transaction(
    db: &Database,
    pool: &PgPool,
    migrate: impl std::future::Future<Output = Result<Ledger>>,
) -> Result<PgConnection> {
    let mut runners = PgConnection::connect(&db.url).await?;
    sqlx::query("SELECT pg_advisory_lock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let mut migrate = Box::pin(migrate);
    let committed = timeout(Duration::from_secs(60), async {
        while cursor(pool).await?.is_none() {
            sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    });
    tokio::select! {
        result = &mut migrate => bail!("migrate ended before the backfill ran: {:?}", result.err()),
        result = committed => result.context("the migration transaction never committed")??,
    }
    drop(migrate);
    Ok(runners)
}

/// The fence keeps every build before #669 off the database until 2 is
/// recorded, whatever the record says: declared with the cursor, still
/// declared when 2 is recorded by hand and while a resume maps, and removed
/// only with the cursor. That an earlier build refuses the declaration is
/// `a_pending_share_hash_backfill_fences_every_earlier_build`.
#[tokio::test]
async fn migration_002_fences_earlier_builds_until_its_backfill_is_recorded() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let expected = expected_mapping(&pool, i64::MAX).await?;
    let mut runners = pending_backfill(&db, &pool).await?;
    assert!(
        fence_declared(&pool).await?,
        "a pending backfill is not fenced"
    );
    // Recorded by hand, 2 neither lifts the fence nor lets this build start.
    sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
        .execute(&pool)
        .await?;
    assert!(fence_declared(&pool).await?);
    let (next, end) = cursor(&pool).await?.context("the progress table is gone")?;
    assert_refused_at(&db.url, next, end).await?;
    sqlx::query("DELETE FROM qbit_prism_schema_migrations WHERE version=2")
        .execute(&pool)
        .await?;
    // A resume maps under the fence: seen while a batch waits on an open
    // insert.
    let blocker = block_late_header(&pool).await?;
    sqlx::query("SELECT pg_advisory_unlock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let mut resume = Box::pin(Ledger::connect(&db.url, "resumed".into(), 8, true));
    tokio::select! {
        result = &mut resume => bail!("the resume ended before its batch reached the late header: {:?}", result.err()),
        result = batch_waiting(&pool) => { result?; }
    }
    assert!(fence_declared(&pool).await?, "a resume mapped unfenced");
    blocker.rollback().await?;
    let resumed = timeout(Duration::from_secs(120), resume).await??;
    // Recorded: the fence went with the cursor.
    assert!(
        !fence_declared(&pool).await?,
        "the fence outlived the backfill"
    );
    assert_eq!(cursor(&pool).await?, None);
    assert_eq!(mapping(&pool).await?, expected);
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    runners.close().await?;
    db.close(vec![resumed]).await
}

/// An open insert of the late header, which stops the batch that reaches
/// it until the transaction ends.
async fn block_late_header(pool: &PgPool) -> Result<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut blocker = pool.begin().await?;
    sqlx::query(
        "INSERT INTO qbit_prism_share_hashes(header_hash,share_id) VALUES($1,'blocker:'||$1)",
    )
    .bind(format!("{LATE:064x}"))
    .execute(&mut *blocker)
    .await?;
    Ok(blocker)
}

/// The backend of a batch waiting on `block_late_header`.
async fn batch_waiting(pool: &PgPool) -> Result<i32> {
    timeout(Duration::from_secs(60), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'INSERT INTO qbit_prism_share_hashes%' AND wait_event_type='Lock'")
                .fetch_optional(pool)
                .await?;
            if let Some(pid) = pid {
                return Ok::<_, anyhow::Error>(pid);
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("no batch waited on the late header")?
}

/// A backfill an earlier build started carries no fence, and this release
/// does not declare one when it resumes it: an earlier build's runner that
/// passed its capability check before then could finish the backfill after
/// this release's runner stopped, and would leave the fence behind (#669).
/// The test crashes this release's runner mid-batch, then plays the
/// earlier runner, which finishes the backfill knowing nothing of the
/// fence. Nothing is left that refuses the database.
#[tokio::test]
async fn a_crashed_resume_leaves_no_fence_for_an_earlier_runner_to_strand() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let expected = expected_mapping(&pool, i64::MAX).await?;
    let mut runners = pending_backfill(&db, &pool).await?;
    // A backfill an earlier build started declares no fence.
    sqlx::query(
        "DELETE FROM qbit_prism_schema_capabilities WHERE capability='share_hash_backfill_pending'",
    )
    .execute(&pool)
    .await?;
    let blocker = block_late_header(&pool).await?;
    sqlx::query("SELECT pg_advisory_unlock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let mut resume = Box::pin(Ledger::connect(&db.url, "this-build".into(), 8, true));
    let pid = tokio::select! {
        result = &mut resume => bail!("the resume ended before its batch reached the late header: {:?}", result.err()),
        result = batch_waiting(&pool) => result?,
    };
    assert!(
        !fence_declared(&pool).await?,
        "a resume declared a fence an earlier runner could leave behind"
    );
    // This release's runner crashes mid-batch.
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .execute(&pool)
        .await?;
    ensure!(
        timeout(Duration::from_secs(30), resume).await?.is_err(),
        "the resume succeeded although its batch was killed"
    );
    blocker.rollback().await?;
    // The earlier runner, queued for the lock, finishes the backfill the way
    // #663's runner does: maps the rest, drops the cursor and records 2.
    sqlx::query("SELECT pg_advisory_lock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let (next, end) = cursor(&pool).await?.context("the progress table is gone")?;
    let mut tx = runners.begin().await?;
    sqlx::query("INSERT INTO qbit_prism_share_hashes(header_hash,share_id) SELECT DISTINCT ON (lower(right(share_id,64))) lower(right(share_id,64)),share_id FROM qbit_share_ledger WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$' AND share_seq>=$1 AND share_seq<$2 ORDER BY lower(right(share_id,64)),share_seq ON CONFLICT DO NOTHING")
        .bind(next)
        .bind(end)
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("DROP TABLE qbit_prism_share_hash_backfill; INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    sqlx::query("SELECT pg_advisory_unlock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    // No fence was left: migrate finishes 013, 017 and 024, and the database
    // starts.
    assert!(
        !fence_declared(&pool).await?,
        "a fence outlived the backfill"
    );
    let migrated = db.ledger("after").await?;
    assert_eq!(mapping(&pool).await?, expected);
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    let started = Ledger::connect(&db.url, "cold".into(), 8, false).await?;
    runners.close().await?;
    db.close(vec![migrated, started]).await
}

/// A newer release may declare the fence at its own value while this
/// release's runner maps between holds of the migration lock (#669). That
/// declaration is the newer release's to remove, with the cursor and the
/// record: the runner reads the fence again under the migration lock
/// before it records 2, and leaves all three. This release's next migrate
/// refuses the newer value, and every start still refuses the pending
/// backfill.
#[tokio::test]
async fn a_runner_leaves_a_newer_releases_fence_with_its_cursor_and_record() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let mut runners = pending_backfill(&db, &pool).await?;
    let blocker = block_late_header(&pool).await?;
    sqlx::query("SELECT pg_advisory_unlock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let mut resume = Box::pin(Ledger::connect(&db.url, "this-build".into(), 8, true));
    tokio::select! {
        result = &mut resume => bail!("the resume ended before its batch reached the late header: {:?}", result.err()),
        result = batch_waiting(&pool) => { result?; }
    }
    // A newer release declares the fence at its own value meanwhile: past
    // 2, which is this release's.
    set_fence(&pool, 3).await?;
    blocker.rollback().await?;
    let error = timeout(Duration::from_secs(120), resume)
        .await?
        .err()
        .context("the runner recorded 2 over a newer release's fence")?;
    let text = format!("{error:#}");
    ensure!(
        text.contains("refusing to record migration 2")
            && text.contains("share_hash_backfill_pending = 3")
            && text.contains("share_hash_backfill_pending 1 to 2 only")
            && text.contains("upgrade the server"),
        "{text}"
    );
    assert_eq!(
        fence_value(&pool).await?,
        Some(3),
        "the runner removed a newer release's fence"
    );
    assert!(
        cursor(&pool).await?.is_some(),
        "the runner dropped the cursor"
    );
    assert!(!schema_versions(&pool).await?.contains(&2));
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a start accepted a pending backfill")?
        .to_string();
    ensure!(
        error.contains("share-hash backfill has not finished"),
        "{error}"
    );
    let error = db
        .ledger("migrate")
        .await
        .err()
        .context("migrate accepted share_hash_backfill_pending = 3")?
        .to_string();
    ensure!(
        error.contains("share_hash_backfill_pending = 3, but this server understands share_hash_backfill_pending 1 to 2 only")
            && error.contains("upgrade the server"),
        "{error}"
    );
    runners.close().await?;
    db.close(Vec::new()).await
}

/// A runner of an earlier build that is still mapping when this release
/// resumes the backfill holds the runners' lock, finishes, drops the cursor
/// and records 2 without knowing the fence. This release must not have
/// declared one meanwhile, which nothing would remove (#669), and a resume
/// declares none. The test plays the earlier runner, holding the lock while
/// the resume's transaction commits, then finishing the backfill the way
/// #663's runner does.
#[tokio::test]
async fn a_resume_leaves_no_fence_when_an_earlier_runner_finishes_the_backfill() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let expected = expected_mapping(&pool, i64::MAX).await?;
    let mut runners = pending_backfill(&db, &pool).await?;
    // A backfill an earlier build started declares no fence.
    sqlx::query(
        "DELETE FROM qbit_prism_schema_capabilities WHERE capability='share_hash_backfill_pending'",
    )
    .execute(&pool)
    .await?;
    let mut resume = Box::pin(Ledger::connect(&db.url, "this-build".into(), 8, true));
    // The resume's transaction has committed once its runner polls for the
    // lock the earlier runner holds.
    let queued = timeout(Duration::from_secs(60), async {
        loop {
            let polling: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'SELECT pg_try_advisory_lock%')")
                .fetch_one(&pool)
                .await?;
            if polling {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    });
    tokio::select! {
        result = &mut resume => bail!("the resume ended while the runners' lock was held: {:?}", result.err()),
        result = queued => result.context("the resume's runner never queued for the runners' lock")??,
    }
    assert!(
        !fence_declared(&pool).await?,
        "the resume's transaction declared a fence an earlier runner would leave behind"
    );
    // The earlier runner finishes: maps the rest, drops the cursor and
    // records 2, in its last transaction, knowing nothing of the fence.
    let (next, end) = cursor(&pool).await?.context("the progress table is gone")?;
    let mut tx = runners.begin().await?;
    sqlx::query("INSERT INTO qbit_prism_share_hashes(header_hash,share_id) SELECT DISTINCT ON (lower(right(share_id,64))) lower(right(share_id,64)),share_id FROM qbit_share_ledger WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$' AND share_seq>=$1 AND share_seq<$2 ORDER BY lower(right(share_id,64)),share_seq ON CONFLICT DO NOTHING")
        .bind(next)
        .bind(end)
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("DROP TABLE qbit_prism_share_hash_backfill; INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    sqlx::query("SELECT pg_advisory_unlock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    // This release's runner finds the backfill complete, and the database
    // starts: no fence was left behind.
    let resumed = timeout(Duration::from_secs(120), resume).await??;
    assert!(
        !fence_declared(&pool).await?,
        "a fence outlived the backfill"
    );
    assert_eq!(mapping(&pool).await?, expected);
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    let started = Ledger::connect(&db.url, "cold".into(), 8, false).await?;
    runners.close().await?;
    db.close(vec![resumed, started]).await
}

/// A fence whose cursor is gone, which only a hand-dropped cursor leaves,
/// is refused by migrate before any DDL and, once 2 is recorded by hand
/// too, by every start: recording 2 by hand unlocks nothing (#669). That
/// holds at 2, the value that permits serving, as at 1. Past 2 the name is
/// a newer release's declaration, refused as any newer capability is and
/// never offered for deletion as an orphan.
#[tokio::test]
async fn migration_002_refuses_a_backfill_fence_left_without_its_cursor() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let runners = pending_backfill(&db, &pool).await?;
    runners.close().await?;
    sqlx::raw_sql("DROP TABLE qbit_prism_share_hash_backfill")
        .execute(&pool)
        .await?;
    let pending: Vec<i32> = schema_versions(&pool).await?;
    let refused = |error: String| -> Result<()> {
        ensure!(
            error.contains("declares share_hash_backfill_pending = 1, but migration 2's share-hash backfill cursor qbit_prism_share_hash_backfill is gone"),
            "{error}"
        );
        Ok(())
    };
    refused(
        db.ledger("migrate")
            .await
            .err()
            .context("migrate accepted a fence without its cursor")?
            .to_string(),
    )?;
    assert_eq!(schema_versions(&pool).await?, pending);
    assert!(fence_declared(&pool).await?);
    // At 2, serving's value, the fence is this release's too, and an orphan.
    set_fence(&pool, 2).await?;
    let error = db
        .ledger("migrate-serving")
        .await
        .err()
        .context("migrate accepted a serving fence without its cursor")?
        .to_string();
    ensure!(
        error.contains("declares share_hash_backfill_pending = 2, but migration 2's share-hash backfill cursor qbit_prism_share_hash_backfill is gone")
            && !error.contains("Restore the full pre-migration backup"),
        "{error}"
    );
    assert_eq!(schema_versions(&pool).await?, pending);
    // Past 2 the name is a newer release's: never an orphan to delete, and
    // refused in the order any newer capability is, after the record.
    set_fence(&pool, 3).await?;
    let error = db
        .ledger("migrate-newer")
        .await
        .err()
        .context("migrate accepted a newer release's share_hash_backfill_pending = 3")?
        .to_string();
    ensure!(
        error.contains("migration 3 is recorded and 2 is not")
            && !error.contains("is gone")
            && !error.contains("DELETE FROM qbit_prism_schema_capabilities"),
        "{error}"
    );
    assert_eq!(schema_versions(&pool).await?, pending);
    set_fence(&pool, 1).await?;
    // Recording 2 by hand as well lets nothing start: 013, 017 and 024
    // never ran behind the pending backfill, and migrate still names the
    // fence.
    sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
        .execute(&pool)
        .await?;
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a start accepted a fence without its cursor")?
        .to_string();
    ensure!(error.contains("missing migration(s) 13, 17, 24"), "{error}");
    refused(
        db.ledger("migrate-again")
            .await
            .err()
            .context("migrate accepted a fence without its cursor and a hand-recorded 2")?
            .to_string(),
    )?;
    assert!(fence_declared(&pool).await?);
    // With the record past 3 without 2, serving's value is no different: a
    // start still refuses the missing 13, 17 and 24, and migrate names the
    // orphan. A newer release's value is refused as newer.
    set_fence(&pool, 2).await?;
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a start accepted a serving fence without its cursor")?
        .to_string();
    ensure!(error.contains("missing migration(s) 13, 17, 24"), "{error}");
    let error = db
        .ledger("migrate-serving-again")
        .await
        .err()
        .context("migrate accepted a serving fence without its cursor and a hand-recorded 2")?
        .to_string();
    ensure!(
        error.contains("declares share_hash_backfill_pending = 2, but migration 2's share-hash backfill cursor qbit_prism_share_hash_backfill is gone"),
        "{error}"
    );
    set_fence(&pool, 3).await?;
    let error = db
        .ledger("migrate-newer-again")
        .await
        .err()
        .context("migrate accepted a newer release's share_hash_backfill_pending = 3 and a hand-recorded 2")?
        .to_string();
    ensure!(
        error.contains("share_hash_backfill_pending = 3, but this server understands share_hash_backfill_pending 1 to 2 only")
            && error.contains("upgrade the server")
            && !error.contains("is gone"),
        "{error}"
    );
    db.close(Vec::new()).await
}

/// Declare the fence at `value`, as a newer release might.
async fn set_fence(pool: &PgPool, value: i32) -> Result<()> {
    sqlx::query("UPDATE qbit_prism_schema_capabilities SET capability_value=$1 WHERE capability='share_hash_backfill_pending'")
        .bind(value)
        .execute(pool)
        .await?;
    Ok(())
}

/// A fence declared on a database with every migration recorded and no
/// cursor is refused at every start and every migrate: only the
/// transaction that records 2 removes it, with the cursor, so it can only
/// be left by hand, and legacy shares may be unmapped (#669). So it is at
/// 2, which permits serving only beside its cursor. Past 2 the name is a
/// newer release's declaration: refused as that release's, without the
/// remedy that would delete it.
#[tokio::test]
async fn every_start_refuses_a_backfill_fence_on_a_migrated_database() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    sqlx::query("INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('share_hash_backfill_pending',1)")
        .execute(&pool)
        .await?;
    for (what, error) in [
        (
            "a start",
            Ledger::connect(&db.url, "cold".into(), 8, false)
                .await
                .err(),
        ),
        ("migrate", db.ledger("migrate").await.err()),
    ] {
        let error = error
            .with_context(|| format!("{what} accepted a fence without its cursor"))?
            .to_string();
        ensure!(
            error.contains("declares share_hash_backfill_pending = 1, but migration 2's share-hash backfill cursor qbit_prism_share_hash_backfill is gone"),
            "{what}: {error}"
        );
    }
    set_fence(&pool, 2).await?;
    for (what, error) in [
        (
            "a start",
            Ledger::connect(&db.url, "cold".into(), 8, false)
                .await
                .err(),
        ),
        ("migrate", db.ledger("migrate").await.err()),
    ] {
        let error = error
            .with_context(|| format!("{what} accepted a serving fence without its cursor"))?
            .to_string();
        ensure!(
            error.contains("declares share_hash_backfill_pending = 2, but migration 2's share-hash backfill cursor qbit_prism_share_hash_backfill is gone")
                && !error.contains("Restore the full pre-migration backup"),
            "{what}: {error}"
        );
    }
    set_fence(&pool, 3).await?;
    for (what, error) in [
        (
            "a start",
            Ledger::connect(&db.url, "cold".into(), 8, false)
                .await
                .err(),
        ),
        ("migrate", db.ledger("migrate").await.err()),
    ] {
        let error = error
            .with_context(|| format!("{what} accepted share_hash_backfill_pending = 3"))?
            .to_string();
        ensure!(
            error.contains("share_hash_backfill_pending = 3, but this server understands share_hash_backfill_pending 1 to 2 only")
                && error.contains("upgrade the server")
                && !error.contains("is gone"),
            "{what}: {error}"
        );
    }
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    db.close(vec![first]).await
}

/// Rows the recent range's seed writes: twenty headers to a template
/// height, so heights 0 to 3,000.
const HEIGHT_ROWS: i64 = 60_000;
/// The recent range's lowest template height on that seed: the highest an
/// accepted share has, 3,000, less the 1,000 the range reaches.
const RECENT_MIN_HEIGHT: i64 = 2_000;
/// The first share whose own header is at `RECENT_MIN_HEIGHT`: the range's
/// first share would be here without the copies far below it.
const RECENT_FIRST_OWN: i64 = 20 * RECENT_MIN_HEIGHT;

/// A 2.x.x ledger for the recent range: `seed`'s rule, with template
/// heights. A header commits to its parent, so every copy of a header has
/// its height: twenty headers to a height, whichever row carries a copy.
/// Every 97th row repeats, in upper case, the header of the row 9,001
/// below it or, where it can, 9,001 above it; so headers just above the
/// range's lowest height have their earliest copies far below the first
/// share whose own header is there.
async fn seed_heights(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,reject_reason,writer_id,writer_epoch) \
         SELECT g,i.id,'miner-'||g%5,'miner-'||g%5,decode(repeat('11',32),'hex'),1,100,h.header/20,'job',to_timestamp(1),1,to_timestamp(2),g%1000<>13,CASE WHEN g%1000=13 THEN 'duplicate-share' END,'backfill-test',0 \
         FROM generate_series(1,$1::bigint) g, \
         LATERAL (SELECT CASE WHEN g%97<>0 THEN g WHEN g%2=0 AND g>9001 THEN g-9001 WHEN g+9001<=$1 THEN g+9001 ELSE g-9001 END AS header) h, \
         LATERAL (SELECT CASE \
             WHEN g%97=0 THEN 'repeat-'||g||':'||upper(lpad(to_hex(h.header),64,'0')) \
             WHEN g%1000=7 THEN 'legacy:'||g \
             WHEN g%1000=11 THEN lpad(to_hex(g),64,'0') \
             ELSE 'worker-'||g%5||':'||lpad(to_hex(g),64,'0') END AS id) i \
         WHERE g%89<>0",
    )
    .bind(HEIGHT_ROWS)
    .execute(pool)
    .await?;
    sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1)")
        .bind(HEIGHT_ROWS)
        .execute(pool)
        .await?;
    Ok(())
}

/// What the recent range maps: 002's rule over the accepted legacy shares
/// at `min_height` and above.
async fn expected_recent_mapping(pool: &PgPool, min_height: i64) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT DISTINCT ON (lower(right(share_id,64))) lower(right(share_id,64)),share_id FROM qbit_share_ledger WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$' AND template_height>=$1 ORDER BY lower(right(share_id,64)),share_seq",
    )
    .bind(min_height)
    .fetch_all(pool)
    .await?)
}

/// The mapping of the legacy shares alone, those below `end`.
async fn legacy_mapping(pool: &PgPool, end: i64) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT h.header_hash,h.share_id FROM qbit_prism_share_hashes h WHERE h.share_id IN (SELECT share_id FROM qbit_share_ledger WHERE share_seq<$1) ORDER BY h.header_hash",
    )
    .bind(end)
    .fetch_all(pool)
    .await?)
}

/// The share ledger's end: one past its highest `share_seq`.
async fn ledger_end(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT COALESCE(max(share_seq),-1)+1 FROM qbit_share_ledger")
            .fetch_one(pool)
            .await?,
    )
}

/// What the recent range recorded beside the cursor.
async fn recent_range(pool: &PgPool) -> Result<(Option<i64>, Option<i64>)> {
    Ok(sqlx::query_as(
        "SELECT recent_min_height,recent_start_seq FROM qbit_prism_share_hash_backfill WHERE singleton",
    )
    .fetch_one(pool)
    .await?)
}

/// Every required migration but 2: what a deferred backfill leaves.
fn all_but_2() -> Vec<i32> {
    REQUIRED_SCHEMA_VERSIONS
        .iter()
        .copied()
        .filter(|version| *version != 2)
        .collect()
}

/// A 2.x.x source seeded for the recent range and migrated with
/// `--defer-share-hashes`. Returns the source's planned end.
async fn deferred(db: &Database, pool: &PgPool) -> Result<(Ledger, i64)> {
    two_x::apply_frozen_2x_schema(pool, SourceState::Pre258).await?;
    seed_heights(pool).await?;
    let end = ledger_end(pool).await?;
    let ledger = Ledger::connect_migrate(&db.url, ShareHashBackfill::Defer).await?;
    Ok((ledger, end))
}

/// `migrate --defer-share-hashes` maps the recent range and permits
/// serving. Every accepted legacy share within 1,000 template heights of
/// the highest maps its header to the earliest copy, copies far below the
/// first share whose own header is in the range included, and nothing
/// below the range is mapped. The fence is 2, the cursor stays where it was
/// planned, 2 is not recorded, and every start serves the database: a
/// native share repeating a header in the range is refused, and a new one
/// is credited.
#[tokio::test]
async fn migrate_defer_share_hashes_maps_the_recent_range_and_permits_serving() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed_heights(&pool).await?;
    let (first, end): (i64, i64) =
        sqlx::query_as("SELECT min(share_seq),max(share_seq)+1 FROM qbit_share_ledger")
            .fetch_one(&pool)
            .await?;
    let top: i64 =
        sqlx::query_scalar("SELECT max(template_height) FROM qbit_share_ledger WHERE accepted")
            .fetch_one(&pool)
            .await?;
    assert_eq!(top - 1_000, RECENT_MIN_HEIGHT, "the seed moved its heights");
    let expected = expected_recent_mapping(&pool, RECENT_MIN_HEIGHT).await?;
    let start_seq: i64 = sqlx::query_scalar(
        "SELECT min(share_seq) FROM qbit_share_ledger WHERE accepted AND template_height>=$1",
    )
    .bind(RECENT_MIN_HEIGHT)
    .fetch_one(&pool)
    .await?;
    // The range's first share is an early copy of a header in it, far below
    // the first share whose own header is there, and wins its header.
    let (first_id,): (String,) =
        sqlx::query_as("SELECT share_id FROM qbit_share_ledger WHERE share_seq=$1")
            .bind(start_seq)
            .fetch_one(&pool)
            .await?;
    ensure!(
        start_seq < RECENT_FIRST_OWN && first_id.starts_with("repeat-"),
        "the seed's copies no longer straddle the range's first share: {start_seq} {first_id}"
    );
    assert!(expected.iter().any(|(_, id)| *id == first_id));
    ensure!(expected.iter().any(|(_, id)| id.len() == 64));

    let migrated = Ledger::connect_migrate(&db.url, ShareHashBackfill::Defer).await?;
    // The range, row for row, and each header at its earliest share, as the
    // whole backfill maps it.
    let mapped = mapping(&pool).await?;
    assert_eq!(mapped, expected);
    let whole: HashSet<(String, String)> = expected_mapping(&pool, i64::MAX)
        .await?
        .into_iter()
        .collect();
    assert!(mapped.iter().all(|entry| whole.contains(entry)));
    let below: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_share_hashes h JOIN qbit_share_ledger l ON l.share_id=h.share_id WHERE l.template_height<$1")
        .bind(RECENT_MIN_HEIGHT)
        .fetch_one(&pool)
        .await?;
    assert_eq!(below, 0, "a header below the recent range was mapped");
    // Serving is permitted; the rest is pending where it was planned.
    assert_eq!(fence_value(&pool).await?, Some(2));
    assert_eq!(cursor(&pool).await?, Some((first, end)));
    assert_eq!(
        recent_range(&pool).await?,
        (Some(RECENT_MIN_HEIGHT), Some(start_seq))
    );
    assert_eq!(schema_versions(&pool).await?, all_but_2());
    assert_eq!(
        migrated.pending_share_hash_backfill().await?,
        Some((first, end))
    );
    let cold = Ledger::connect(&db.url, "cold".into(), 8, false).await?;
    let error = cold
        .append(share(55_000), None)
        .await
        .err()
        .context("a native share repeating a header in the recent range was credited")?;
    assert!(
        format!("{error:#}").contains("header already credited globally"),
        "{error:#}"
    );
    assert!(cold.append(share(1_000_000), None).await?.inserted);
    db.close(vec![migrated, cold]).await
}

/// A frontend that compose starts with PRISM_POSTGRES_INIT_SCHEMA=1
/// migrates at its start, as tools and operator connects may. None of them
/// maps a backfill that permits serving: the cursor, the fence and the
/// mapping stay as `--defer-share-hashes` left them, and so does a second
/// deferred `migrate`.
#[tokio::test]
async fn a_frontend_with_init_schema_leaves_a_deferred_backfill_to_migrate() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let (migrated, _) = deferred(&db, &pool).await?;
    let pending = cursor(&pool)
        .await?
        .context("the deferred backfill has no cursor")?;
    let mapped = mapping(&pool).await?;
    let frontend = db.ledger("frontend").await?;
    let tool = Ledger::connect_tool(&db.url, "tool".into(), 2, true, None).await?;
    let operator = Ledger::connect_operator(&db.url, true).await?;
    let again = Ledger::connect_migrate(&db.url, ShareHashBackfill::Defer).await?;
    assert_eq!(
        cursor(&pool).await?,
        Some(pending),
        "a connect moved the cursor"
    );
    assert_eq!(fence_value(&pool).await?, Some(2));
    assert_eq!(
        mapping(&pool).await?,
        mapped,
        "a connect mapped legacy shares"
    );
    assert_eq!(schema_versions(&pool).await?, all_but_2());
    db.close(vec![migrated, frontend, tool, operator, again])
        .await
}

/// Plain `migrate` finishes a deferred backfill while frontends append,
/// up to the end its cursor was planned to: the legacy mapping becomes the
/// whole backfill's, 2 is recorded, and the cursor and the fence go. The
/// native shares, appended before and during the run, keep the headers
/// they mapped themselves, and a row past the planned end that no append
/// wrote stays unmapped: the end is never extended over native rows.
#[tokio::test]
async fn migrate_finishes_a_deferred_backfill_up_to_its_planned_end_while_frontends_append(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let (migrated, end) = deferred(&db, &pool).await?;
    let (first, _) = cursor(&pool)
        .await?
        .context("the deferred backfill has no cursor")?;
    let frontend = db.ledger("frontend").await?;
    let mut native = Vec::new();
    for id in 1_000_000..1_000_003 {
        native.push(frontend.append(share(id), None).await?.share);
    }
    // A row past the planned end that no append wrote, so nothing mapped it.
    let straggler = "ee".repeat(32);
    sqlx::query(
        "INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) \
         VALUES('straggler:'||$1,'miner-0','miner-0',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(1),1,to_timestamp(2),true,'backfill-test',0)",
    )
    .bind(&straggler)
    .execute(&pool)
    .await?;
    // A legacy header below the recent range, held open so the run stops in
    // the batch that reaches it.
    let held = 30_001_u64;
    let mut blocker = pool.begin().await?;
    sqlx::query(
        "INSERT INTO qbit_prism_share_hashes(header_hash,share_id) VALUES($1,'blocker:'||$1)",
    )
    .bind(format!("{held:064x}"))
    .execute(&mut *blocker)
    .await?;
    let mut finish = Box::pin(Ledger::connect_migrate(&db.url, ShareHashBackfill::Finish));
    tokio::select! {
        result = &mut finish => bail!("migrate ended before its batch reached the held header: {:?}", result.err()),
        result = batch_waiting(&pool) => { result?; }
    }
    // Frontends keep appending while it maps.
    for id in 2_000_000..2_000_003 {
        native.push(frontend.append(share(id), None).await?.share);
    }
    let (next, planned) = cursor(&pool).await?.context("the cursor is gone mid-run")?;
    assert_eq!(planned, end, "the run moved its planned end");
    assert!(next > first && next <= held as i64, "cursor {next}");
    blocker.rollback().await?;
    let finished = timeout(Duration::from_secs(120), finish).await??;

    assert_eq!(cursor(&pool).await?, None);
    assert_eq!(fence_value(&pool).await?, None);
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    let legacy = expected_mapping(&pool, end).await?;
    assert_eq!(legacy_mapping(&pool, end).await?, legacy);
    for share in &native {
        assert!(share.share_seq as i64 >= end);
        let header = share.share_id[share.share_id.len() - 64..].to_owned();
        let holder: Option<String> =
            sqlx::query_scalar("SELECT share_id FROM qbit_prism_share_hashes WHERE header_hash=$1")
                .bind(&header)
                .fetch_optional(&pool)
                .await?;
        assert_eq!(holder.as_deref(), Some(share.share_id.as_str()));
    }
    let unmapped: Option<String> =
        sqlx::query_scalar("SELECT share_id FROM qbit_prism_share_hashes WHERE header_hash=$1")
            .bind(&straggler)
            .fetch_optional(&pool)
            .await?;
    assert_eq!(unmapped, None, "the backfill read past its planned end");
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_share_hashes")
        .fetch_one(&pool)
        .await?;
    assert_eq!(total, (legacy.len() + native.len()) as i64);
    db.close(vec![migrated, frontend, finished]).await
}

/// A native share that repeats a legacy header below the recent range was
/// credited twice: nothing mapped the header when it was appended, and the
/// batch that reached the legacy copy met the native share's mapping and
/// said nothing. Plain `migrate` maps every legacy share, then refuses to
/// record 2, naming the native share, and leaves the cursor at its end and
/// the fence at 2: frontends keep serving, and the double credit is
/// reported, not recorded over. Both forms of a legacy ID are found: a
/// worker-scoped one through 2.x's header-suffix index, and the bare
/// 64-digit header through the share ID index.
#[tokio::test]
async fn migrate_refuses_to_record_2_over_a_native_share_repeating_a_legacy_header() -> Result<()> {
    for (legacy_seq, bare) in [(1_005_u64, false), (1_011_u64, true)] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        let (migrated, end) = deferred(&db, &pool).await?;
        let (legacy_id, height): (String, i64) = sqlx::query_as(
            "SELECT share_id,template_height FROM qbit_share_ledger WHERE share_seq=$1 AND accepted",
        )
        .bind(legacy_seq as i64)
        .fetch_one(&pool)
        .await?;
        assert_eq!(legacy_id.len() == 64, bare, "{legacy_id}");
        assert!(height < RECENT_MIN_HEIGHT);
        let frontend = db.ledger("frontend").await?;
        let repeated = frontend.append(share(legacy_seq), None).await?;
        assert!(
            repeated.inserted,
            "the header below the recent range was refused before the backfill reached it"
        );
        frontend.append(share(3_000_000), None).await?;

        let error = Ledger::connect_migrate(&db.url, ShareHashBackfill::Finish)
            .await
            .err()
            .context("migrate recorded 2 over a header credited twice")?;
        let text = format!("{error:#}");
        ensure!(
            text.contains(&format!(
                "refusing to record migration 2: native share {}, at share_seq {}, repeats header {legacy_seq:064x}",
                repeated.share.share_id, repeated.share.share_seq
            )) && text.contains("so that header was credited twice"),
            "{legacy_id}: {text}"
        );
        // Every legacy share is mapped; the record, the cursor and the fence
        // wait for the reconciliation.
        assert_eq!(cursor(&pool).await?, Some((end, end)));
        assert_eq!(fence_value(&pool).await?, Some(2));
        assert_eq!(schema_versions(&pool).await?, all_but_2());
        let holder: Option<String> =
            sqlx::query_scalar("SELECT share_id FROM qbit_prism_share_hashes WHERE header_hash=$1")
                .bind(format!("{legacy_seq:064x}"))
                .fetch_optional(&pool)
                .await?;
        assert_eq!(holder.as_deref(), Some(repeated.share.share_id.as_str()));
        let cold = Ledger::connect(&db.url, "cold".into(), 8, false).await?;
        let again = Ledger::connect_migrate(&db.url, ShareHashBackfill::Finish)
            .await
            .err()
            .context("a second migrate recorded 2 over a header credited twice")?;
        ensure!(
            format!("{again:#}").contains("so that header was credited twice"),
            "{again:#}"
        );
        db.close(vec![migrated, frontend, cold]).await?;
    }
    Ok(())
}

/// Share-archive restore, detach and drop refuse while a backfill is
/// pending, here with serving permitted: a restore maps its rows' headers,
/// and the release table holds legacy shares the backfill has still to
/// map. A lead partition, above the backfill's end, is not refused for it,
/// and once 2 is recorded nothing is.
#[tokio::test]
async fn share_archive_restore_detach_and_drop_refuse_while_a_backfill_is_pending() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let (migrated, end) = deferred(&db, &pool).await?;
    let root = tempfile::tempdir()?;
    let manifest = root.path().join("absent").join("manifest.json");
    let options = || archive::PlanOptions {
        network_difficulty: "100".into(),
        retention_days: 0,
        window_multiple: 4,
        check_duplicates: false,
    };
    let lead: String = sqlx::query_scalar("SELECT partition_name FROM qbit_prism_share_partitions WHERE lower_seq IS NOT NULL ORDER BY lower_seq LIMIT 1")
        .fetch_one(&pool)
        .await?;
    let lead_lower: i64 = sqlx::query_scalar(
        "SELECT lower_seq FROM qbit_prism_share_partitions WHERE partition_name=$1",
    )
    .bind(&lead)
    .fetch_one(&pool)
    .await?;
    assert!(lead_lower >= end);
    let operator = Ledger::connect_operator(&db.url, false).await?;
    const PENDING: &str = "while migration 2's share-hash backfill is pending";
    for attach in [false, true] {
        let error = archive::restore(&operator, &manifest, root.path(), attach)
            .await
            .err()
            .context("a restore ran while the backfill was pending")?
            .to_string();
        ensure!(
            error.contains(&format!("refusing to restore a share archive {PENDING}")),
            "{error}"
        );
    }
    let error = archive::detach(&operator, "qbit_share_ledger_p0", &options())
        .await
        .err()
        .context("the release table was detached while the backfill was pending")?
        .to_string();
    ensure!(
        error.contains(&format!(
            "refusing to detach qbit_share_ledger_p0 {PENDING}"
        )) && error.contains("starts at share_seq MINVALUE"),
        "{error}"
    );
    let error = archive::drop_partition(&operator, "qbit_share_ledger_p0", root.path())
        .await
        .err()
        .context("the release table was dropped while the backfill was pending")?
        .to_string();
    ensure!(
        error.contains(&format!("refusing to drop qbit_share_ledger_p0 {PENDING}")),
        "{error}"
    );
    // A lead partition holds no legacy share: detach refuses it for its own
    // reasons, never for the backfill.
    if let Err(error) = archive::detach(&operator, &lead, &options()).await {
        ensure!(!error.to_string().contains(PENDING), "{lead}: {error}");
    }
    // Recorded, nothing refuses for the backfill.
    let finished = Ledger::connect_migrate(&db.url, ShareHashBackfill::Finish).await?;
    assert_eq!(cursor(&pool).await?, None);
    let error = archive::restore(&operator, &manifest, root.path(), true)
        .await
        .err()
        .context("a restore read an absent manifest")?;
    ensure!(!format!("{error:#}").contains(PENDING), "{error:#}");
    let error = archive::detach(&operator, "qbit_share_ledger_p0", &options())
        .await
        .err()
        .context("an unsealed release table was detached")?;
    ensure!(!format!("{error:#}").contains(PENDING), "{error:#}");
    db.close(vec![migrated, operator, finished]).await
}

/// Deferred, 013, 017 and 024 run with the backfill pending, which they
/// never did before: the recent range runs first, while 2.x's
/// template-height index it reads is there, and the cursor table is no
/// object 013's, 017's or 024's checks look at. A later `migrate` of either
/// kind applies nothing over them; plain `migrate` maps the rest after
/// them and records 2 last.
#[tokio::test]
async fn migrations_13_17_and_24_run_with_a_deferred_backfill_pending() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let (migrated, end) = deferred(&db, &pool).await?;
    assert_eq!(schema_versions(&pool).await?, all_but_2());
    let pending = cursor(&pool)
        .await?
        .context("the deferred backfill has no cursor")?;
    assert_eq!(pending.1, end);
    let (partitioned, trimmed, lane): (bool, bool, bool) = sqlx::query_as(
        "SELECT (SELECT relkind='p' FROM pg_class WHERE oid='qbit_share_ledger'::regclass), \
                to_regclass('qbit_share_ledger_template_height_idx') IS NULL \
                    AND to_regclass('qbit_share_ledger_accepted_seq_walk_idx') IS NOT NULL \
                    AND to_regclass('qbit_share_ledger_accepted_miner_history_idx') IS NOT NULL, \
                to_regclass('qbit_ctv_fanout_artifacts_lane_idx') IS NOT NULL",
    )
    .fetch_one(&pool)
    .await?;
    assert!(partitioned, "017 did not convert the share ledger");
    assert!(trimmed, "013 did not trim the share ledger's indexes");
    assert!(lane, "024 did not build the fanout lane index");
    // The recent range ran: it records its bounds beside the cursor.
    assert_eq!(recent_range(&pool).await?.0, Some(RECENT_MIN_HEIGHT));
    // Nothing is applied twice, and a start that migrates applies nothing.
    let again = Ledger::connect_migrate(&db.url, ShareHashBackfill::Defer).await?;
    let frontend = db.ledger("frontend").await?;
    assert_eq!(schema_versions(&pool).await?, all_but_2());
    assert_eq!(cursor(&pool).await?, Some(pending));
    let finished = Ledger::connect_migrate(&db.url, ShareHashBackfill::Finish).await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(cursor(&pool).await?, None);
    assert_eq!(fence_value(&pool).await?, None);
    assert_eq!(
        mapping(&pool).await?,
        expected_mapping(&pool, i64::MAX).await?
    );
    let last: bool = sqlx::query_scalar("SELECT (SELECT applied_at FROM qbit_prism_schema_migrations WHERE version=2) >= (SELECT max(applied_at) FROM qbit_prism_schema_migrations)")
        .fetch_one(&pool)
        .await?;
    assert!(last, "2 is not recorded last");
    let recorded = Ledger::connect_migrate(&db.url, ShareHashBackfill::Finish).await?;
    db.close(vec![migrated, again, frontend, finished, recorded])
        .await
}

/// A backfill a build before #669 started declares no fence, and this
/// release never declares one on a resume (#669), so serving cannot be
/// permitted on it: `migrate --defer-share-hashes` refuses it before any
/// DDL and changes nothing, and plain `migrate` finishes it as before.
#[tokio::test]
async fn migrate_defer_share_hashes_refuses_a_backfill_an_earlier_build_started() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let expected = expected_mapping(&pool, i64::MAX).await?;
    let runners = pending_backfill(&db, &pool).await?;
    runners.close().await?;
    sqlx::query(
        "DELETE FROM qbit_prism_schema_capabilities WHERE capability='share_hash_backfill_pending'",
    )
    .execute(&pool)
    .await?;
    let pending = cursor(&pool).await?.context("the progress table is gone")?;
    let versions = schema_versions(&pool).await?;
    let error = Ledger::connect_migrate(&db.url, ShareHashBackfill::Defer)
        .await
        .err()
        .context("migrate --defer-share-hashes deferred an unfenced backfill")?
        .to_string();
    ensure!(
        error.contains(
            "refusing to defer migration 2's share-hash backfill: a build before #669 started it"
        ) && error.contains("Nothing was changed")
            && error.contains("without --defer-share-hashes"),
        "{error}"
    );
    assert_eq!(cursor(&pool).await?, Some(pending));
    assert_eq!(fence_value(&pool).await?, None, "a resume declared a fence");
    assert!(mapping(&pool).await?.is_empty());
    assert_eq!(schema_versions(&pool).await?, versions);
    assert_refused_at(&db.url, pending.0, pending.1).await?;
    let finished = Ledger::connect_migrate(&db.url, ShareHashBackfill::Finish).await?;
    assert_eq!(mapping(&pool).await?, expected);
    assert_eq!(cursor(&pool).await?, None);
    assert_eq!(fence_value(&pool).await?, None);
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    db.close(vec![finished]).await
}

/// A backfill that plain `migrate` started and left part-way, fenced at 1,
/// is deferred on its resume as on a fresh source: the native path runs
/// the recent range in the backfill's slot. The headers the interrupted
/// run mapped stay as they were, including early copies of headers in the
/// range, which the range meets again and keeps; serving is permitted with
/// the cursor where that run left it.
#[tokio::test]
async fn migrate_defer_share_hashes_resumes_a_backfill_plain_migrate_left_part_way() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed_heights(&pool).await?;
    let mut runners = pending_backfill(&db, &pool).await?;
    let (first, end) = cursor(&pool).await?.context("the progress table is gone")?;
    assert_eq!(fence_value(&pool).await?, Some(1));
    // The interrupted run's batches: below `left`, which lies past the
    // recent range's first share, so the two overlap.
    let left = 35_000_i64;
    let mut tx = runners.begin().await?;
    sqlx::query("INSERT INTO qbit_prism_share_hashes(header_hash,share_id) SELECT DISTINCT ON (lower(right(share_id,64))) lower(right(share_id,64)),share_id FROM qbit_share_ledger WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$' AND share_seq>=$1 AND share_seq<$2 ORDER BY lower(right(share_id,64)),share_seq ON CONFLICT DO NOTHING")
        .bind(first)
        .bind(left)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE qbit_prism_share_hash_backfill SET next_seq=$1 WHERE singleton")
        .bind(left)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    runners.close().await?;
    assert_refused_at(&db.url, left, end).await?;
    let prefix: HashSet<(String, String)> =
        expected_mapping(&pool, left).await?.into_iter().collect();
    let recent: HashSet<(String, String)> = expected_recent_mapping(&pool, RECENT_MIN_HEIGHT)
        .await?
        .into_iter()
        .collect();
    ensure!(
        prefix.intersection(&recent).next().is_some(),
        "the interrupted run mapped no header of the recent range"
    );

    let resumed = Ledger::connect_migrate(&db.url, ShareHashBackfill::Defer).await?;
    let mapped = mapping(&pool).await?;
    let expected: HashSet<(String, String)> = prefix.union(&recent).cloned().collect();
    assert_eq!(mapped.len(), expected.len(), "a header is mapped twice");
    assert_eq!(mapped.into_iter().collect::<HashSet<_>>(), expected);
    assert_eq!(fence_value(&pool).await?, Some(2));
    assert_eq!(cursor(&pool).await?, Some((left, end)));
    assert_eq!(schema_versions(&pool).await?, all_but_2());
    let cold = Ledger::connect(&db.url, "cold".into(), 8, false).await?;
    db.close(vec![resumed, cold]).await
}

/// The ledger's share_seq sequence, as `(last_value, is_called)`.
async fn share_seq_state(pool: &PgPool) -> Result<(i64, bool)> {
    Ok(
        sqlx::query_as("SELECT last_value,is_called FROM qbit_share_ledger_share_seq_seq")
            .fetch_one(pool)
            .await?,
    )
}

/// The share_seq sequence's `last_value` as `pg_sequences` lists it, the
/// value the cutover's evidence compares: NULL while it is uncalled.
async fn listed_last_value(pool: &PgPool) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar("SELECT last_value FROM pg_sequences WHERE schemaname=current_schema() AND sequencename='qbit_share_ledger_share_seq_seq'")
        .fetch_one(pool)
        .await?)
}

/// Wait until a runner polls for the runners' lock the test holds: a
/// `migrate` whose transaction has committed, its online part queued.
async fn runner_queued(pool: &PgPool) -> Result<()> {
    timeout(Duration::from_secs(60), async {
        loop {
            let polling: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'SELECT pg_try_advisory_lock%')")
                .fetch_one(pool)
                .await?;
            if polling {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("no runner queued for the runners' lock")?
}

/// Ensure a connect was refused for a deferred backfill whose recent range
/// is not mapped, naming the deferred run's retry.
fn refused_for_the_deferred_range(what: &str, error: Option<anyhow::Error>) -> Result<()> {
    let text = format!(
        "{:#}",
        error.with_context(|| format!(
            "{what} accepted a deferred backfill whose recent range is not mapped"
        ))?
    );
    ensure!(
        text.contains("`qbit-prism-server migrate --defer-share-hashes` claimed it and stopped before its recent range was mapped")
            && text.contains("Run `qbit-prism-server migrate --defer-share-hashes` again"),
        "{what}: {text}"
    );
    Ok(())
}

/// A `migrate --defer-share-hashes` that stopped after its migration
/// transaction, before its recent range permitted serving, leaves its claim
/// on the cursor at fence 1. A frontend that compose starts with
/// PRISM_POSTGRES_INIT_SCHEMA=1 then runs neither the whole backfill nor
/// 013, which drops the index the range reads: it refuses the database,
/// naming the deferred run's retry, as every start does, and so do a
/// tool's and an operator's connect that migrate. The retry then maps the
/// range and permits serving as a first run does.
#[tokio::test]
async fn a_deferred_migrate_stopped_before_its_recent_range_is_left_to_the_operator() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed_heights(&pool).await?;
    let expected = expected_recent_mapping(&pool, RECENT_MIN_HEIGHT).await?;
    let runners = stop_after_the_transaction(
        &db,
        &pool,
        Ledger::connect_migrate(&db.url, ShareHashBackfill::Defer),
    )
    .await?;
    runners.close().await?;
    let pending = cursor(&pool).await?.context("the progress table is gone")?;
    assert_eq!(fence_value(&pool).await?, Some(1));
    let claimed: bool =
        sqlx::query_scalar("SELECT deferred_at IS NOT NULL FROM qbit_prism_share_hash_backfill")
            .fetch_one(&pool)
            .await?;
    assert!(
        claimed,
        "the deferred run committed no claim with its cursor"
    );
    let versions = schema_versions(&pool).await?;

    refused_for_the_deferred_range(
        "a frontend that migrates",
        timeout(Duration::from_secs(120), db.ledger("frontend"))
            .await?
            .err(),
    )?;
    refused_for_the_deferred_range(
        "a tool that migrates",
        Ledger::connect_tool(&db.url, "tool".into(), 2, true, None)
            .await
            .err(),
    )?;
    refused_for_the_deferred_range(
        "an operator connect that migrates",
        Ledger::connect_operator(&db.url, true).await.err(),
    )?;
    refused_for_the_deferred_range(
        "a start",
        Ledger::connect(&db.url, "cold".into(), 8, false)
            .await
            .err(),
    )?;
    // None of them mapped a share or applied a migration, so 2.x's
    // template-height index, which serves the range, is still there.
    assert_eq!(
        cursor(&pool).await?,
        Some(pending),
        "a connect moved the cursor"
    );
    assert!(
        mapping(&pool).await?.is_empty(),
        "a connect mapped legacy shares"
    );
    assert_eq!(
        schema_versions(&pool).await?,
        versions,
        "a connect applied a migration"
    );
    let indexed: bool = sqlx::query_scalar(
        "SELECT to_regclass('qbit_share_ledger_template_height_idx') IS NOT NULL",
    )
    .fetch_one(&pool)
    .await?;
    assert!(indexed, "013 ran ahead of the recent range");

    // The retry completes as a first run does.
    let retried = Ledger::connect_migrate(&db.url, ShareHashBackfill::Defer).await?;
    assert_eq!(mapping(&pool).await?, expected);
    assert_eq!(fence_value(&pool).await?, Some(2));
    assert_eq!(cursor(&pool).await?, Some(pending));
    assert_eq!(schema_versions(&pool).await?, all_but_2());
    let frontend = db.ledger("frontend").await?;
    db.close(vec![retried, frontend]).await
}

/// A connect that planned the whole backfill at fence 1 before a deferred
/// `migrate` claimed it, and then waited for the runners' lock, reads the
/// claim again once it holds the lock: it refuses before it maps anything,
/// and before 013, as one that found the claim at its migration does.
#[tokio::test]
async fn a_connect_queued_behind_a_deferred_claim_maps_nothing() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed_heights(&pool).await?;
    let mut runners = PgConnection::connect(&db.url).await?;
    sqlx::query("SELECT pg_advisory_lock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    // A frontend migrates the source, without a claim: its backfill is
    // planned, and waits for the lock.
    let mut frontend = Box::pin(db.ledger("frontend"));
    tokio::select! {
        result = &mut frontend => bail!("the frontend's connect ended while the runners' lock was held: {:?}", result.err()),
        result = runner_queued(&pool) => result?,
    }
    let pending = cursor(&pool).await?.context("the progress table is gone")?;
    assert_eq!(fence_value(&pool).await?, Some(1));
    let versions = schema_versions(&pool).await?;
    // A deferred `migrate` claims it meanwhile, as its migration
    // transaction does.
    sqlx::raw_sql("ALTER TABLE qbit_prism_share_hash_backfill ADD COLUMN deferred_at timestamptz; UPDATE qbit_prism_share_hash_backfill SET deferred_at=clock_timestamp()")
        .execute(&pool)
        .await?;
    sqlx::query("SELECT pg_advisory_unlock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    refused_for_the_deferred_range(
        "a frontend queued behind the claim",
        timeout(Duration::from_secs(120), frontend).await?.err(),
    )?;
    assert_eq!(cursor(&pool).await?, Some(pending), "the frontend mapped");
    assert!(mapping(&pool).await?.is_empty(), "the frontend mapped");
    assert_eq!(
        schema_versions(&pool).await?,
        versions,
        "the frontend applied an online migration"
    );
    runners.close().await?;
    db.close(Vec::new()).await
}

/// Plain `migrate` planned at fence 1 runs the backfill ahead of 017. A
/// deferred `migrate` can raise the fence to 2 while that run waits for the
/// runners' lock, and stop before 017. Recording 2 under fence 2 needs
/// 017's conversion bound, so the run would refuse at its record after
/// every batch, hours on a production ledger: it refuses before it maps
/// anything instead. A rerun plans from fence 2, applies 013, 017 and 024
/// first, then maps the rest and records 2.
#[tokio::test]
async fn plain_migrate_refuses_before_mapping_when_the_fence_reached_2_while_it_waited(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let expected = expected_mapping(&pool, i64::MAX).await?;
    let mut runners = PgConnection::connect(&db.url).await?;
    sqlx::query("SELECT pg_advisory_lock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let mut finish = Box::pin(Ledger::connect_migrate(&db.url, ShareHashBackfill::Finish));
    tokio::select! {
        result = &mut finish => bail!("migrate ended while the runners' lock was held: {:?}", result.err()),
        result = runner_queued(&pool) => result?,
    }
    let pending = cursor(&pool).await?.context("the progress table is gone")?;
    assert_eq!(fence_value(&pool).await?, Some(1));
    // A deferred `migrate` raised the fence meanwhile and stopped before
    // 017.
    set_fence(&pool, 2).await?;
    sqlx::query("SELECT pg_advisory_unlock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let error = timeout(Duration::from_secs(120), finish)
        .await?
        .err()
        .context("plain migrate recorded 2 without 017's conversion bound")?;
    let text = format!("{error:#}");
    ensure!(
        text.contains(
            "refusing to finish migration 2's share-hash backfill before mapping anything"
        ) && text
            .contains("The share-hash fence changed while this run waited for the runner lock")
            && text.contains("rerun `qbit-prism-server migrate`"),
        "{text}"
    );
    assert_eq!(
        cursor(&pool).await?,
        Some(pending),
        "the run moved the cursor"
    );
    assert!(
        mapping(&pool).await?.is_empty(),
        "the run mapped legacy shares"
    );
    let finished = Ledger::connect_migrate(&db.url, ShareHashBackfill::Finish).await?;
    assert_eq!(mapping(&pool).await?, expected);
    assert_eq!(cursor(&pool).await?, None);
    assert_eq!(fence_value(&pool).await?, None);
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    runners.close().await?;
    db.close(vec![finished]).await
}

/// The double-credit check reads the native shares in chunks of
/// `share_seq`, from the backfill's end up to the last share appended
/// before it starts, each chunk a short statement of its own. Here the
/// native shares span a million `share_seq`, twenty times the check's
/// largest chunk, and only the last one repeats a legacy header: plain
/// `migrate` still refuses to record 2, naming it, and leaves the cursor
/// and the fence as they are.
#[tokio::test]
async fn migrate_refuses_a_double_credit_in_the_last_chunk_of_its_check() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let (migrated, end) = deferred(&db, &pool).await?;
    let frontend = db.ledger("frontend").await?;
    // A legacy header below the recent range, so nothing refuses its
    // repeat at the append.
    let legacy_seq = 1_005_u64;
    // A clean share at the end, one half-way up, and the double credit
    // last; the sequence skips the share_seq between them.
    let mut appended = Vec::new();
    for (id, after) in [
        (1_000_000, None),
        (1_000_001, Some(end + 500_000)),
        (legacy_seq, Some(end + 1_000_000)),
    ] {
        if let Some(after) = after {
            sqlx::query(
                "SELECT setval(pg_get_serial_sequence('qbit_share_ledger','share_seq'),$1)",
            )
            .bind(after)
            .execute(&pool)
            .await?;
        }
        let result = frontend.append(share(id), None).await?;
        assert!(result.inserted, "share {id} was refused");
        appended.push(result.share);
    }
    assert!(appended[0].share_seq as i64 >= end);
    let repeated = appended.pop().context("nothing was appended")?;
    assert!(repeated.share_seq as i64 > end + 1_000_000);

    let error = Ledger::connect_migrate(&db.url, ShareHashBackfill::Finish)
        .await
        .err()
        .context("migrate recorded 2 over a header credited twice in its check's last chunk")?;
    let text = format!("{error:#}");
    ensure!(
        text.contains(&format!(
            "refusing to record migration 2: native share {}, at share_seq {}, repeats header {legacy_seq:064x}",
            repeated.share_id, repeated.share_seq
        )),
        "{text}"
    );
    assert_eq!(cursor(&pool).await?, Some((end, end)));
    assert_eq!(fence_value(&pool).await?, Some(2));
    assert_eq!(schema_versions(&pool).await?, all_but_2());
    db.close(vec![migrated, frontend]).await
}

/// Native shares draw their share_seq from the ledger's sequence, and plain
/// `migrate` takes every row below the frozen end for a legacy share. A
/// source whose sequence lags behind its rows, called or not, as explicit
/// inserts or a restore leave it, has the sequence moved up to the end by
/// the transaction that permits serving: the next native share lands at
/// the end, not in a gap below it.
#[tokio::test]
async fn migrate_defer_share_hashes_moves_a_lagging_share_seq_sequence_up_to_the_frozen_end(
) -> Result<()> {
    // A share_seq the seed leaves free, every 89th, far below its end.
    const GAP: i64 = 89 * 300;
    for is_called in [true, false] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
        seed_heights(&pool).await?;
        let end = ledger_end(&pool).await?;
        // Behind the ledger's rows: GAP is the next value either way.
        let last_value = if is_called { GAP - 1 } else { GAP };
        sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1,$2)")
            .bind(last_value)
            .bind(is_called)
            .execute(&pool)
            .await?;
        assert_eq!(share_seq_state(&pool).await?, (last_value, is_called));

        let migrated = Ledger::connect_migrate(&db.url, ShareHashBackfill::Defer).await?;
        assert_eq!(fence_value(&pool).await?, Some(2));
        assert_eq!(
            share_seq_state(&pool).await?,
            (end - 1, true),
            "is_called {is_called}: the sequence was not moved up to the end"
        );
        let frontend = db.ledger("frontend").await?;
        let native = frontend.append(share(1_000_000), None).await?;
        assert!(native.inserted);
        assert_eq!(
            native.share.share_seq as i64, end,
            "is_called {is_called}: the native share did not land at the frozen end"
        );
        db.close(vec![migrated, frontend]).await?;
    }
    Ok(())
}

/// The converse: a sequence that hands out the end next, or a value past
/// it, as a promoted physical copy's does, is left exactly as the source
/// had it, `last_value` and `is_called` alike. The cutover's evidence
/// compares `pg_sequences.last_value` before and after `migrate`.
#[tokio::test]
async fn migrate_defer_share_hashes_leaves_a_share_seq_sequence_at_or_past_the_end_untouched(
) -> Result<()> {
    for ahead in [None, Some(32)] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
        seed_heights(&pool).await?;
        let end = ledger_end(&pool).await?;
        if let Some(ahead) = ahead {
            // Past the end, and not called since.
            sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1,false)")
                .bind(end + ahead)
                .execute(&pool)
                .await?;
        }
        let before = share_seq_state(&pool).await?;
        // The seed leaves it called at the ledger's last row, which hands
        // out the end next.
        assert_eq!(
            before,
            ahead.map_or((end - 1, true), |ahead| (end + ahead, false))
        );
        let listed = listed_last_value(&pool).await?;

        let migrated = Ledger::connect_migrate(&db.url, ShareHashBackfill::Defer).await?;
        assert_eq!(fence_value(&pool).await?, Some(2));
        assert_eq!(
            share_seq_state(&pool).await?,
            before,
            "the deferred run wrote to a sequence that did not lag"
        );
        assert_eq!(listed_last_value(&pool).await?, listed);
        db.close(vec![migrated]).await?;
    }
    Ok(())
}
