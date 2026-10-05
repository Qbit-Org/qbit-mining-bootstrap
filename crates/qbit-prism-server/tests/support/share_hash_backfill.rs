//! Migration 002's share-hash backfill on a populated 2.x.x source (#582).
//!
//! 002 maps the header of every accepted legacy share whose ID ends in 64 hex
//! digits, the earliest `share_seq` winning. That used to be one statement
//! inside the migration transaction, which outlasted the statement timeout
//! on a production-sized ledger. It now runs after the commit, in batches of
//! consecutive `share_seq` with a durable cursor, and records 2 last. These
//! tests hold the batched result to the single statement's on a ledger that
//! repeats headers across batch boundaries, interrupt a run mid-batch and
//! resume it, and check that nothing serves the database in between.
use super::*;
use qbit_prism_server::ledger::REQUIRED_SCHEMA_VERSIONS;
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
/// online 013 and 017, which each check that they come after every lower
/// version.
async fn assert_recorded_in_order(pool: &PgPool) -> Result<()> {
    let (after_transaction, before_online): (bool, bool) = sqlx::query_as(
        "SELECT (SELECT applied_at FROM qbit_prism_schema_migrations WHERE version=2) >= (SELECT max(applied_at) FROM qbit_prism_schema_migrations WHERE version NOT IN (2,13,17)), \
                (SELECT applied_at FROM qbit_prism_schema_migrations WHERE version=2) <= (SELECT min(applied_at) FROM qbit_prism_schema_migrations WHERE version IN (13,17))",
    )
    .fetch_one(pool)
    .await?;
    ensure!(
        after_transaction && before_online,
        "2 is not recorded between the transaction's migrations and 013 and 017"
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
    // Committed: every migration but 2 and the online 013 and 017 recorded,
    // nothing mapped yet, and no start serves the database.
    let pending: Vec<i32> = REQUIRED_SCHEMA_VERSIONS
        .iter()
        .copied()
        .filter(|version| ![2, 13, 17].contains(version))
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
    // mapping, 2 recorded before 013 and 017, the progress table gone.
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
    let mut runners = PgConnection::connect(&db.url).await?;
    sqlx::query("SELECT pg_advisory_lock($1,hashtext(current_schema()))")
        .bind(RUNNER_LOCK_CLASS)
        .execute(&mut runners)
        .await?;
    let mut migrate = Box::pin(Ledger::connect(&db.url, "pending".into(), 8, true));
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
    // No fence was left: migrate finishes 013 and 017, and the database
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
/// too, by every start: recording 2 by hand unlocks nothing (#669). At
/// another value the name is a newer release's declaration, refused as any
/// newer capability is and never offered for deletion as an orphan.
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
    // At another value the name is a newer release's: never an orphan to
    // delete, and refused in the order any newer capability is, after the
    // record.
    set_fence(&pool, 2).await?;
    let error = db
        .ledger("migrate-newer")
        .await
        .err()
        .context("migrate accepted a newer release's share_hash_backfill_pending = 2")?
        .to_string();
    ensure!(
        error.contains("migration 3 is recorded and 2 is not")
            && !error.contains("is gone")
            && !error.contains("DELETE FROM qbit_prism_schema_capabilities"),
        "{error}"
    );
    assert_eq!(schema_versions(&pool).await?, pending);
    set_fence(&pool, 1).await?;
    // Recording 2 by hand as well lets nothing start: 013 and 017 never ran
    // behind the pending backfill, and migrate still names the fence.
    sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
        .execute(&pool)
        .await?;
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a start accepted a fence without its cursor")?
        .to_string();
    ensure!(error.contains("missing migration(s) 13, 17"), "{error}");
    refused(
        db.ledger("migrate-again")
            .await
            .err()
            .context("migrate accepted a fence without its cursor and a hand-recorded 2")?
            .to_string(),
    )?;
    assert!(fence_declared(&pool).await?);
    // With the record past 3 without 2, a newer release's value is refused
    // as newer.
    set_fence(&pool, 2).await?;
    let error = db
        .ledger("migrate-newer-again")
        .await
        .err()
        .context("migrate accepted a newer release's share_hash_backfill_pending = 2 and a hand-recorded 2")?
        .to_string();
    ensure!(
        error.contains("share_hash_backfill_pending = 2, but this server understands share_hash_backfill_pending 1 to 1 only")
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
/// be left by hand, and legacy shares may be unmapped (#669). At another
/// value the name is a newer release's declaration: refused as that
/// release's, without the remedy that would delete it.
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
            .with_context(|| format!("{what} accepted share_hash_backfill_pending = 2"))?
            .to_string();
        ensure!(
            error.contains("share_hash_backfill_pending = 2, but this server understands share_hash_backfill_pending 1 to 1 only")
                && error.contains("upgrade the server")
                && !error.contains("is gone"),
            "{what}: {error}"
        );
    }
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    db.close(vec![first]).await
}
