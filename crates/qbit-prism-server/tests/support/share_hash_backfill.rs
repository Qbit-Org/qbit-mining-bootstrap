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
/// online 013, 017 and 023, which each check that they come after every
/// lower version.
async fn assert_recorded_in_order(pool: &PgPool) -> Result<()> {
    let (after_transaction, before_online): (bool, bool) = sqlx::query_as(
        "SELECT (SELECT applied_at FROM qbit_prism_schema_migrations WHERE version=2) >= (SELECT max(applied_at) FROM qbit_prism_schema_migrations WHERE version NOT IN (2,13,17,23)), \
                (SELECT applied_at FROM qbit_prism_schema_migrations WHERE version=2) <= (SELECT min(applied_at) FROM qbit_prism_schema_migrations WHERE version IN (13,17,23))",
    )
    .fetch_one(pool)
    .await?;
    ensure!(
        after_transaction && before_online,
        "2 is not recorded between the transaction's migrations and 013, 017 and 023"
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
    // Committed: every migration but 2 and the online 013, 017 and 023 recorded,
    // nothing mapped yet, and no start serves the database.
    let pending: Vec<i32> = REQUIRED_SCHEMA_VERSIONS
        .iter()
        .copied()
        .filter(|version| ![2, 13, 17, 23].contains(version))
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
    // mapping, 2 recorded before 013, 017 and 023, the progress table gone.
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
