//! Migration 013 (#153): the share ledger index trim, applied online.
//!
//! The migration replaces two covering indexes with narrower ones and drops
//! two without a consumer. It is the first migration the migrator applies
//! outside its transaction, with `CREATE INDEX CONCURRENTLY` and `DROP
//! INDEX CONCURRENTLY`, so these tests cover what that changes: the index
//! set a fresh and a 2.x.x source end up with, that appends keep landing
//! while a build waits, that an interrupted, pre-built or foreign index
//! under a reserved name is rebuilt, kept or refused, that an index
//! swapped under a drop target while the builds run is refused too, and
//! that a kept index swapped while the other replacement builds is refused
//! before the version is recorded.
//!
//! Since 017 the ledger is a partitioned table whose release table is the
//! first partition: `undo_013` undoes 017 first (the concurrent builds need
//! the plain table), the trimmed set is asserted on the parent and on that
//! partition, and every successful migrate here ends with 017 applied
//! again through its online runner.
use super::*;
use qbit_prism_server::{ledger::REQUIRED_SCHEMA_VERSIONS, metrics::Metrics};
use sqlx::Row;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const SEQ_WALK: &str = "qbit_share_ledger_accepted_seq_walk_idx";
const MINER_HISTORY: &str = "qbit_share_ledger_accepted_miner_history_idx";
const SEQ_WALK_DEFINITION: &str = "CREATE INDEX qbit_share_ledger_accepted_seq_walk_idx ON qbit_share_ledger USING btree (share_seq DESC) INCLUDE (job_issued_at, accepted_at, share_difficulty) WHERE accepted";
const MINER_HISTORY_DEFINITION: &str = "CREATE INDEX qbit_share_ledger_accepted_miner_history_idx ON qbit_share_ledger USING btree (miner_id, accepted_at DESC) INCLUDE (share_difficulty, share_seq, share_id) WHERE accepted";
/// The release indexes 013 drops, and the 001 DDL that restores them.
const REPLACED: [(&str, &str); 4] = [
    (
        "qbit_share_ledger_accepted_seq_window_idx",
        "CREATE INDEX qbit_share_ledger_accepted_seq_window_idx ON qbit_share_ledger (share_seq DESC) INCLUDE (job_issued_at, accepted_at, miner_id, payout_order_key, p2mr_program, share_difficulty, share_id) WHERE accepted",
    ),
    (
        "qbit_share_ledger_accepted_miner_recent_idx",
        "CREATE INDEX qbit_share_ledger_accepted_miner_recent_idx ON qbit_share_ledger (miner_id, accepted_at DESC) INCLUDE (share_difficulty, share_seq, share_id, payout_order_key) WHERE accepted",
    ),
    (
        "qbit_share_ledger_accepted_window_idx",
        "CREATE INDEX qbit_share_ledger_accepted_window_idx ON qbit_share_ledger (job_issued_at, share_seq DESC) WHERE accepted",
    ),
    (
        "qbit_share_ledger_template_height_idx",
        "CREATE INDEX qbit_share_ledger_template_height_idx ON qbit_share_ledger (template_height, share_seq) WHERE accepted",
    ),
];
const KEPT: [&str; 4] = [
    "qbit_share_ledger_accepted_block_suffix_idx",
    "qbit_share_ledger_accepted_recent_idx",
    "qbit_share_ledger_pkey",
    "qbit_share_ledger_share_id_key",
];

/// Every index on the share ledger (the plain table before 017, the
/// partitioned parent after it): name, definition as the server renders it
/// (without the schema it always puts on the table), validity and the index
/// relation's OID, ordered by name.
async fn ledger_indexes(pool: &PgPool) -> Result<Vec<(String, String, bool, String)>> {
    table_indexes(pool, "qbit_share_ledger").await
}

async fn table_indexes(pool: &PgPool, table: &str) -> Result<Vec<(String, String, bool, String)>> {
    let rows = sqlx::query("SELECT i.relname::text AS name,replace(pg_get_indexdef(x.indexrelid),current_schema()||'.','') AS definition,x.indisvalid AS valid,x.indexrelid::text AS oid FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid WHERE x.indrelid=to_regclass($1) ORDER BY 1")
        .bind(table)
        .fetch_all(pool)
        .await?;
    rows.iter()
        .map(|row| {
            Ok((
                row.try_get("name")?,
                row.try_get("definition")?,
                row.try_get("valid")?,
                row.try_get("oid")?,
            ))
        })
        .collect()
}

/// The name an index of the release table carries once 017 has attached
/// that table as `qbit_share_ledger_p0`: the index keeps its OID under it,
/// while the parent gets a new partitioned index under the release name.
fn leaf_name(name: &str) -> String {
    name.replacen("qbit_share_ledger_", "qbit_share_ledger_p0_", 1)
}

async fn schema_versions(pool: &PgPool) -> Result<Vec<i32>> {
    Ok(
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(pool)
            .await?,
    )
}

/// The index set 013 leaves, as 017 then partitions it: on the parent the
/// kept release indexes and the two replacements as partitioned indexes,
/// without the global share_id key a partitioned table cannot carry; on
/// the release table, now the first partition, the same set under its
/// `_p0` names plus its own share_id key. Every index valid, with 13 and
/// 16 recorded. Returns the parent's indexes.
async fn assert_trimmed(pool: &PgPool) -> Result<Vec<(String, String, bool, String)>> {
    let indexes = ledger_indexes(pool).await?;
    let mut expected: Vec<&str> = KEPT
        .iter()
        .copied()
        .filter(|name| *name != "qbit_share_ledger_share_id_key")
        .collect();
    expected.extend([MINER_HISTORY, SEQ_WALK]);
    expected.sort_unstable();
    let names: Vec<&str> = indexes.iter().map(|(name, ..)| name.as_str()).collect();
    assert_eq!(names, expected);
    for (name, definition, valid, _) in &indexes {
        assert!(valid, "{name} is not valid");
        if name == SEQ_WALK {
            assert_eq!(
                definition,
                &SEQ_WALK_DEFINITION
                    .replace(" ON qbit_share_ledger ", " ON ONLY qbit_share_ledger ")
            );
        }
        if name == MINER_HISTORY {
            assert_eq!(
                definition,
                &MINER_HISTORY_DEFINITION
                    .replace(" ON qbit_share_ledger ", " ON ONLY qbit_share_ledger ")
            );
        }
    }
    let leaf = table_indexes(pool, "qbit_share_ledger_p0").await?;
    let mut expected: Vec<String> = KEPT
        .iter()
        .chain([MINER_HISTORY, SEQ_WALK].iter())
        .map(|name| name.replacen("qbit_share_ledger_", "qbit_share_ledger_p0_", 1))
        .collect();
    expected.sort_unstable();
    let names: Vec<&str> = leaf.iter().map(|(name, ..)| name.as_str()).collect();
    assert_eq!(names, expected);
    for (name, definition, valid, _) in &leaf {
        assert!(valid, "{name} is not valid");
        if name == "qbit_share_ledger_p0_accepted_seq_walk_idx" {
            assert_eq!(
                definition,
                &SEQ_WALK_DEFINITION
                    .replace(SEQ_WALK, name)
                    .replace(" ON qbit_share_ledger ", " ON qbit_share_ledger_p0 ")
            );
        }
    }
    assert_eq!(schema_versions(pool).await?, REQUIRED_SCHEMA_VERSIONS);
    Ok(indexes)
}

/// Undo 013: restore the release indexes as 001 creates them and remove
/// its record, preserving the other migrations present in the fixture.
/// 017 is undone first, so the ledger is the plain table the concurrent
/// builds need; 016 stays, and the next migrate applies 013 and then 017
/// again, both online.
pub(super) async fn undo_013(pool: &PgPool) -> Result<()> {
    super::share_partitions::undo_017(pool).await?;
    let versions = schema_versions(pool).await?;
    assert!(versions.contains(&13));
    let mut sql = format!(
        "DELETE FROM qbit_prism_schema_migrations WHERE version=13; DROP INDEX {SEQ_WALK}; DROP INDEX {MINER_HISTORY};"
    );
    for (_, restore) in REPLACED {
        sql.push_str(restore);
        sql.push(';');
    }
    sqlx::raw_sql(&sql).execute(pool).await?;
    assert_eq!(
        schema_versions(pool).await?,
        versions
            .into_iter()
            .filter(|version| *version != 13)
            .collect::<Vec<_>>()
    );
    Ok(())
}

/// A share row as a 2.x.x or native writer leaves it, without the native
/// ordering lock: enough for index builds and for qbit_prism_window.
async fn insert_share<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    id: u64,
    miner: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) VALUES($1,$2,$2,decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(1),1,clock_timestamp(),true,'index-trim',0)")
        .bind(format!("{miner}:{id:064x}"))
        .bind(miner)
        .execute(executor)
        .await?;
    Ok(())
}

async fn share_count(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger")
        .fetch_one(pool)
        .await?)
}

/// A fresh ledger is created while the cutover locks exclude writers, so
/// 013 is applied inside the migration transaction. Existing native
/// ledgers use the online runner even when they have no visible shares.
#[tokio::test]
async fn migration_013_trims_the_indexes_of_an_empty_ledger_in_the_transaction_and_a_restart_keeps_them(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let a = db.ledger("a").await?;
    let trimmed = assert_trimmed(&pool).await?;
    // Recorded after every other migration of the same run.
    let recorded_last: bool = sqlx::query_scalar("SELECT (SELECT applied_at FROM qbit_prism_schema_migrations WHERE version=13) >= (SELECT max(applied_at) FROM qbit_prism_schema_migrations WHERE version<13)")
        .fetch_one(&pool)
        .await?;
    assert!(recorded_last);
    a.append(share(1), None).await?;
    a.append(share(2), None).await?;
    // The frozen 001 window function still walks the ledger on the new
    // index set, and the native snapshot reads the same rows.
    let counted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_window(clock_timestamp(), 1600)")
            .fetch_one(&pool)
            .await?;
    assert_eq!(counted, 2);
    assert_eq!(a.snapshot(100).await?.shares.len(), 2);
    // A second start applies nothing and keeps the same indexes.
    let b = db.ledger("b").await?;
    assert_eq!(ledger_indexes(&pool).await?, trimmed);
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn migration_013_replaces_the_release_indexes_of_a_frozen_2x_source_and_keeps_its_rows(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    for id in 1..=3 {
        insert_share(&pool, id, "alice").await?;
    }
    let release = ledger_indexes(&pool).await?;
    for (name, _) in REPLACED {
        assert!(
            release.iter().any(|(found, ..)| found == name),
            "{name} missing from the frozen release schema"
        );
    }
    let ledger = db.ledger("cutover").await?;
    assert_trimmed(&pool).await?;
    assert_eq!(share_count(&pool).await?, 3);
    let counted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_window(clock_timestamp(), 1600)")
            .fetch_one(&pool)
            .await?;
    assert_eq!(counted, 3);
    db.close(vec![ledger]).await
}

fn assert_acquire_counts(metrics: &Metrics, success: u64, failure: u64) {
    let body = metrics.render();
    for (outcome, expected) in [("success", success), ("failure", failure)] {
        let key =
            format!("qbit_prism_database_pool_acquire_seconds_count{{result=\"{outcome}\"}} ");
        let samples: Vec<_> = body
            .lines()
            .filter_map(|line| line.strip_prefix(&key))
            .collect();
        assert_eq!(samples.len(), 1, "missing or duplicate {outcome} series");
        assert_eq!(samples[0].parse::<u64>().unwrap(), expected, "{outcome}");
    }
}

async fn blocked_build(
    pool: &PgPool,
    migrate: Pin<&mut impl Future<Output = Result<Ledger>>>,
) -> Result<i32> {
    let waiting = timeout(Duration::from_secs(60), async {
        loop {
            // The relation OID scopes this to this test's private schema.
            let pid: Option<i32> = sqlx::query_scalar("SELECT a.pid FROM pg_stat_activity a WHERE a.query LIKE 'CREATE INDEX CONCURRENTLY%' AND a.wait_event_type='Lock' AND EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='relation' AND l.relation='qbit_share_ledger'::regclass AND l.granted)")
                .fetch_optional(pool).await?;
            if let Some(pid) = pid {
                return Ok(pid);
            }
            sleep(Duration::from_millis(10)).await;
        }
    });
    tokio::select! {
        result = migrate => match result {
            Ok(_) => anyhow::bail!("the online migration succeeded while a writer transaction was open"),
            Err(error) => Err(error).context("the online migration failed before its build waited for the open writer"),
        },
        result = waiting => result.context("the concurrent build never waited for the open writer")?,
    }
}

#[tokio::test]
async fn migration_013_builds_its_indexes_without_blocking_appends() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    undo_013(&pool).await?;
    // The first share is still uncommitted, so the native ledger looks
    // empty to the migrator. A plain CREATE INDEX would queue behind this
    // writer's table lock, and every later append behind that.
    let mut writer = pool.begin().await?;
    insert_share(&mut *writer, 1, "alice").await?;
    assert_eq!(share_count(&pool).await?, 0);
    let metrics = Arc::new(Metrics::default());
    let mut migrate = Box::pin(Ledger::connect_with_metrics(
        &db.url,
        "online".into(),
        8,
        true,
        Some(metrics.clone()),
    ));
    blocked_build(&pool, migrate.as_mut()).await?;
    // Exactly two checkouts so far: migrate_schema's transaction and the
    // online runner's detached connection. DDL has not completed yet.
    assert_acquire_counts(&metrics, 2, 0);
    // Appends keep landing while the build waits.
    timeout(Duration::from_secs(5), async {
        let mut tx = pool.begin().await?;
        insert_share(&mut *tx, 2, "bob").await?;
        tx.commit().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("an append blocked behind the online index build")??;
    assert!(
        futures_util::poll!(&mut migrate).is_pending(),
        "the online migration finished before the writer committed"
    );
    writer.commit().await?;
    let online = timeout(Duration::from_secs(60), migrate).await??;
    // Migration 017's detached connection, registration's transaction and
    // heartbeat add three observed checkouts;
    // the intervening startup validation checkouts remain untimed. The
    // online version-recording transaction reuses its detached connection.
    assert_acquire_counts(&metrics, 5, 0);
    assert_trimmed(&pool).await?;
    assert_eq!(share_count(&pool).await?, 2);

    // Cancel the runner's real SQL after checkout, then resume without metrics.
    // This is distinct from cancelling a checkout future (shared helper tests).
    undo_013(&pool).await?;
    let mut writer = pool.begin().await?;
    insert_share(&mut *writer, 3, "alice").await?;
    let mut migrate = Box::pin(Ledger::connect_with_metrics(
        &db.url,
        "cancelled-online".into(),
        8,
        true,
        Some(metrics.clone()),
    ));
    let pid = blocked_build(&pool, migrate.as_mut()).await?;
    assert_acquire_counts(&metrics, 7, 0);
    let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
        .bind(pid)
        .fetch_one(&pool)
        .await?;
    assert!(cancelled);
    let error = timeout(Duration::from_secs(10), migrate)
        .await?
        .err()
        .context("cancelled online build succeeded")?;
    assert_eq!(
        error
            .downcast_ref::<sqlx::Error>()
            .and_then(sqlx::Error::as_database_error)
            .and_then(|error| error.code())
            .as_deref(),
        Some("57014"),
        "{error:#}"
    );
    // SQL cancellation neither relabels the checkout nor observes it again.
    assert_acquire_counts(&metrics, 7, 0);
    writer.rollback().await?;
    // The runner awaits close on SQL error; its session lock must be released
    // so the metrics-None restart can rebuild the interrupted index.
    let resumed = timeout(Duration::from_secs(60), db.ledger("resumed-no-metrics")).await??;
    assert_acquire_counts(&metrics, 7, 0);
    assert_trimmed(&pool).await?;
    assert_eq!(share_count(&pool).await?, 2);
    db.close(vec![first, online, resumed]).await
}

#[tokio::test]
async fn migration_013_refuses_redefined_drop_targets_before_any_ddl() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    insert_share(&pool, 1, "alice").await?;
    insert_share(&pool, 2, "alice").await?;
    undo_013(&pool).await?;
    sqlx::raw_sql(
        "CREATE TABLE operator_shares (miner_id text); INSERT INTO operator_shares VALUES ('alice'), ('alice')",
    )
    .execute(&pool)
    .await?;
    let versions = schema_versions(&pool).await?;
    for (name, restore) in REPLACED {
        sqlx::raw_sql(&format!("DROP INDEX {name}"))
            .execute(&pool)
            .await?;
        for table in ["qbit_share_ledger", "operator_shares"] {
            for unique in [false, true] {
                // Duplicate miners leave the unique build invalid. Neither
                // validity nor a matching table proves this is our old index.
                let qualifier = if unique { "UNIQUE " } else { "" };
                let built = sqlx::raw_sql(&format!(
                    "CREATE {qualifier}INDEX CONCURRENTLY {name} ON {table} (miner_id)"
                ))
                .execute(&pool)
                .await;
                assert_eq!(built.is_ok(), !unique);
                let index_state = "SELECT pg_get_indexdef(indexrelid),indisvalid,indexrelid::text FROM pg_index WHERE indexrelid=to_regclass($1)";
                let foreign: (String, bool, String) = sqlx::query_as(index_state)
                    .bind(name)
                    .fetch_one(&pool)
                    .await?;
                assert_eq!(foreign.1, !unique);
                let before = ledger_indexes(&pool).await?;
                let error = db
                    .ledger("foreign-drop-target")
                    .await
                    .err()
                    .context("the online migration dropped an operator's redefined index")?
                    .to_string();
                assert!(error.contains(&format!("index {name}")), "{error}");
                assert!(error.contains("will not drop it"), "{error}");
                assert!(error.contains("nothing was changed"), "{error}");
                let after: (String, bool, String) = sqlx::query_as(index_state)
                    .bind(name)
                    .fetch_one(&pool)
                    .await?;
                assert_eq!(after, foreign, "the operator's index was changed");
                assert_eq!(ledger_indexes(&pool).await?, before);
                assert_eq!(schema_versions(&pool).await?, versions);
                assert_eq!(share_count(&pool).await?, 2);
                sqlx::raw_sql(&format!("DROP INDEX {name}"))
                    .execute(&pool)
                    .await?;
            }
        }
        sqlx::raw_sql(restore).execute(&pool).await?;
    }
    let migrated = db.ledger("restored").await?;
    assert_trimmed(&pool).await?;
    db.close(vec![first, migrated]).await
}

#[tokio::test]
async fn migration_013_resumes_an_interrupted_build_keeps_its_own_index_and_refuses_another(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    insert_share(&pool, 1, "alice").await?;
    insert_share(&pool, 2, "alice").await?;
    undo_013(&pool).await?;
    // Without initialize, a start refuses the database, naming the gap.
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a non-initializing start accepted a database missing 013")?
        .to_string();
    assert!(error.contains("missing migration(s) 13"), "{error}");
    // Invalid indexes with another definition or on another table belong
    // to the operator too. Refuse them before building or dropping anything.
    sqlx::raw_sql(
        "CREATE TABLE operator_shares (miner_id text); INSERT INTO operator_shares VALUES ('alice'), ('alice')",
    )
    .execute(&pool)
    .await?;
    for table in ["qbit_share_ledger", "operator_shares"] {
        let failed = sqlx::raw_sql(&format!(
            "CREATE UNIQUE INDEX CONCURRENTLY {SEQ_WALK} ON {table} (miner_id)"
        ))
        .execute(&pool)
        .await;
        assert!(failed.is_err(), "duplicate miners must fail a unique build");
        let index_state = "SELECT pg_get_indexdef(indexrelid),indisvalid,indexrelid::text FROM pg_index WHERE indexrelid=to_regclass($1)";
        let foreign: (String, bool, String) = sqlx::query_as(index_state)
            .bind(SEQ_WALK)
            .fetch_one(&pool)
            .await?;
        assert!(!foreign.1, "the failed build's index must be invalid");
        let before = ledger_indexes(&pool).await?;
        let error = db
            .ledger("foreign-invalid")
            .await
            .err()
            .context("the online migration rebuilt a foreign invalid index")?
            .to_string();
        assert!(
            error.contains(&format!(
                "index {SEQ_WALK} on {table} already exists with a different definition"
            )),
            "{error}"
        );
        let after: (String, bool, String) = sqlx::query_as(index_state)
            .bind(SEQ_WALK)
            .fetch_one(&pool)
            .await?;
        assert_eq!(after, foreign, "the operator's index was changed");
        assert_eq!(ledger_indexes(&pool).await?, before);
        assert_eq!(
            schema_versions(&pool).await?,
            [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 18, 19]
        );
        sqlx::raw_sql(&format!("DROP INDEX {SEQ_WALK}"))
            .execute(&pool)
            .await?;
    }
    // Interrupt the declared build while it waits for an open writer.
    // Only this matching invalid index is safe for the migrator to rebuild.
    let mut writer = pool.begin().await?;
    insert_share(&mut *writer, 3, "alice").await?;
    let mut builder = pool.acquire().await?;
    sqlx::query("SET statement_timeout='500ms'")
        .execute(&mut *builder)
        .await?;
    let failed = sqlx::raw_sql(&SEQ_WALK_DEFINITION.replacen(
        "CREATE INDEX ",
        "CREATE INDEX CONCURRENTLY ",
        1,
    ))
    .execute(&mut *builder)
    .await
    .expect_err("the concurrent build must time out behind the writer");
    assert_eq!(
        failed
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("57014"),
        "{failed}"
    );
    sqlx::query("SET statement_timeout=0")
        .execute(&mut *builder)
        .await?;
    drop(builder);
    writer.rollback().await?;
    let leftover = ledger_indexes(&pool)
        .await?
        .into_iter()
        .find(|(name, ..)| name == SEQ_WALK)
        .context("the failed build left no index")?;
    assert!(!leftover.2, "the failed build's index must be invalid");
    assert_eq!(leftover.1, SEQ_WALK_DEFINITION);
    let resumed = db.ledger("resumed").await?;
    let rebuilt = assert_trimmed(&pool).await?;
    assert_ne!(
        rebuilt
            .iter()
            .find(|(name, ..)| name == SEQ_WALK)
            .unwrap()
            .3,
        leftover.3,
        "the interrupted build's invalid index was not replaced"
    );
    // A valid index under a reserved name with another definition is
    // refused, naming it; nothing is built, dropped or recorded.
    undo_013(&pool).await?;
    sqlx::raw_sql(&format!(
        "CREATE INDEX {SEQ_WALK} ON qbit_share_ledger (share_seq)"
    ))
    .execute(&pool)
    .await?;
    let before = ledger_indexes(&pool).await?;
    let error = db
        .ledger("refused")
        .await
        .err()
        .context("the online migration adopted an index it did not declare")?
        .to_string();
    assert!(
        error.contains(&format!(
            "index {SEQ_WALK} on qbit_share_ledger already exists with a different definition"
        )),
        "{error}"
    );
    assert!(error.contains("migrate again"), "{error}");
    assert_eq!(ledger_indexes(&pool).await?, before);
    assert_eq!(
        schema_versions(&pool).await?,
        [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 18, 19]
    );
    // The declared definition under the reserved name is an earlier build
    // of the migration's own: kept as it is, not rebuilt.
    sqlx::raw_sql(&format!("DROP INDEX {SEQ_WALK}; {SEQ_WALK_DEFINITION}"))
        .execute(&pool)
        .await?;
    let prebuilt = ledger_indexes(&pool)
        .await?
        .into_iter()
        .find(|(name, ..)| name == SEQ_WALK)
        .context("the pre-built index is missing")?;
    let adopted = db.ledger("adopted").await?;
    assert_trimmed(&pool).await?;
    // 017 then attached the table as the first partition, renaming the
    // adopted index; it is the same relation.
    let leaf = table_indexes(&pool, "qbit_share_ledger_p0").await?;
    let kept = leaf
        .iter()
        .find(|(name, ..)| *name == leaf_name(SEQ_WALK))
        .context("the adopted index is missing")?;
    assert_eq!(kept.3, prebuilt.3, "the pre-built index was rebuilt");
    assert_eq!(share_count(&pool).await?, 2);
    db.close(vec![first, resumed, adopted]).await
}

/// Start the migration behind an open writer and, while its first build
/// waits, rename the index under `name` to `operator_kept` and create the
/// operator's own index under `name` on operator_shares. The rename locks
/// the index alone, not the table the build holds, so it lands at once.
/// Returns the run's refusal, which must open with `refusal`, and the
/// operator's index as created.
async fn swap_during_build(
    db: &Database,
    pool: &PgPool,
    name: &str,
    share_id: u64,
    refusal: &str,
) -> Result<(String, (String, bool, String))> {
    let index_state = "SELECT pg_get_indexdef(indexrelid),indisvalid,indexrelid::text FROM pg_index WHERE indexrelid=to_regclass($1)";
    let mut writer = pool.begin().await?;
    insert_share(&mut *writer, share_id, "alice").await?;
    let mut migrate = Box::pin(db.ledger("swapped"));
    blocked_build(pool, migrate.as_mut()).await?;
    let swap = format!(
        "ALTER INDEX {name} RENAME TO operator_kept; CREATE INDEX {name} ON operator_shares (miner_id)"
    );
    timeout(Duration::from_secs(5), sqlx::raw_sql(&swap).execute(pool))
        .await
        .context("the operator's swap blocked behind the build")??;
    let foreign: (String, bool, String) = sqlx::query_as(index_state)
        .bind(name)
        .fetch_one(pool)
        .await?;
    assert!(
        futures_util::poll!(&mut migrate).is_pending(),
        "the online migration finished before the writer committed"
    );
    writer.commit().await?;
    let online = timeout(Duration::from_secs(60), migrate).await?;
    let error = online
        .err()
        .with_context(|| {
            format!("the online migration finished although {name} was swapped while it built")
        })?
        .to_string();
    let after: (String, bool, String) = sqlx::query_as(index_state)
        .bind(name)
        .fetch_one(pool)
        .await?;
    assert_eq!(after, foreign, "the operator's index was changed");
    assert!(error.contains(refusal), "{error}");
    assert!(
        error.contains(&format!(
            "index {name} on operator_shares now reads as CREATE INDEX {name} ON operator_shares USING btree (miner_id) (valid: true)"
        )),
        "{error}"
    );
    assert!(error.contains("The migration is not recorded."), "{error}");
    assert!(
        !error.to_lowercase().contains("nothing was changed"),
        "{error}"
    );
    Ok((error, foreign))
}

/// The plan is decided before any DDL, and the builds after it take hours
/// on a large ledger, in which nothing stops an operator from moving a
/// release index aside and putting their own under its name. Each drop
/// looks its target up again when it is reached: the swapped one is
/// refused, the refusal says what the run had already built and dropped,
/// and the next start keeps that. The invalid index a rebuild replaces is
/// looked up again the same way.
#[tokio::test]
async fn migration_013_refuses_a_drop_target_swapped_while_it_built() -> Result<()> {
    const SEQ_WINDOW: &str = "qbit_share_ledger_accepted_seq_window_idx";
    const MINER_RECENT: &str = "qbit_share_ledger_accepted_miner_recent_idx";
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let oid = |indexes: &[(String, String, bool, String)], name: &str| {
        indexes
            .iter()
            .find(|(found, ..)| found == name)
            .map(|(_, _, _, oid)| oid.clone())
    };
    let first = db.ledger("first").await?;
    insert_share(&pool, 1, "alice").await?;
    undo_013(&pool).await?;
    sqlx::raw_sql("CREATE TABLE operator_shares (miner_id text)")
        .execute(&pool)
        .await?;
    // A release index the migration drops, swapped while the first build
    // waits. Both builds and the drop before it in plan order have run by
    // the time the swapped name is reached; the refusal says so.
    let (error, _) = swap_during_build(
        &db,
        &pool,
        SEQ_WINDOW,
        2,
        "refusing to continue migration 13",
    )
    .await?;
    assert!(
        error.contains(&format!(
            "the name held the release's CREATE INDEX {SEQ_WINDOW} ON qbit_share_ledger "
        )),
        "{error}"
    );
    assert!(
        error.contains(&format!(
            "It had already built {MINER_HISTORY}, {SEQ_WALK} and dropped {MINER_RECENT}; both stay as they are"
        )),
        "{error}"
    );
    let after_refusal = ledger_indexes(&pool).await?;
    let mut expected: Vec<&str> = KEPT.to_vec();
    expected.extend([
        MINER_HISTORY,
        SEQ_WALK,
        "operator_kept",
        "qbit_share_ledger_accepted_window_idx",
        "qbit_share_ledger_template_height_idx",
    ]);
    expected.sort_unstable();
    let names: Vec<&str> = after_refusal
        .iter()
        .map(|(name, ..)| name.as_str())
        .collect();
    assert_eq!(names, expected);
    for (name, definition, valid, _) in &after_refusal {
        assert!(valid, "{name} is not valid");
        if name == "operator_kept" {
            assert_eq!(
                definition.replace("operator_kept", SEQ_WINDOW),
                REPLACED[0]
                    .1
                    .replace(" (share_seq DESC)", " USING btree (share_seq DESC)"),
                "the release index was not merely renamed"
            );
        }
    }
    assert_eq!(
        schema_versions(&pool).await?,
        [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 18, 19]
    );
    assert_eq!(share_count(&pool).await?, 2);
    // The operator puts the name back; the next start keeps both builds
    // and finishes the drops.
    sqlx::raw_sql(&format!(
        "DROP INDEX {SEQ_WINDOW}; ALTER INDEX operator_kept RENAME TO {SEQ_WINDOW}"
    ))
    .execute(&pool)
    .await?;
    let resumed = db.ledger("resumed").await?;
    assert_trimmed(&pool).await?;
    let leaf = table_indexes(&pool, "qbit_share_ledger_p0").await?;
    for name in [MINER_HISTORY, SEQ_WALK] {
        assert_eq!(
            oid(&leaf, &leaf_name(name)),
            oid(&after_refusal, name),
            "{name} was rebuilt"
        );
    }
    // The invalid index an interrupted build left, swapped the same way
    // while the build before it in plan order waits: the rebuild refuses
    // to drop by that name too.
    undo_013(&pool).await?;
    let mut writer = pool.begin().await?;
    insert_share(&mut *writer, 3, "alice").await?;
    let mut builder = pool.acquire().await?;
    sqlx::query("SET statement_timeout='500ms'")
        .execute(&mut *builder)
        .await?;
    sqlx::raw_sql(&SEQ_WALK_DEFINITION.replacen("CREATE INDEX ", "CREATE INDEX CONCURRENTLY ", 1))
        .execute(&mut *builder)
        .await
        .expect_err("the concurrent build must time out behind the writer");
    sqlx::query("SET statement_timeout=0")
        .execute(&mut *builder)
        .await?;
    drop(builder);
    writer.rollback().await?;
    let leftover = ledger_indexes(&pool)
        .await?
        .into_iter()
        .find(|(name, ..)| name == SEQ_WALK)
        .context("the failed build left no index")?;
    assert!(!leftover.2, "the failed build's index must be invalid");
    let (error, _) =
        swap_during_build(&db, &pool, SEQ_WALK, 4, "refusing to continue migration 13").await?;
    assert!(
        error.contains(&format!(
            "the name held the invalid {SEQ_WALK_DEFINITION} on qbit_share_ledger an interrupted build left"
        )),
        "{error}"
    );
    assert!(
        error.contains(&format!(
            "It had already built {MINER_HISTORY}; what it built stays"
        )),
        "{error}"
    );
    let after_refusal = ledger_indexes(&pool).await?;
    let mut expected: Vec<&str> = KEPT.to_vec();
    expected.extend([MINER_HISTORY, "operator_kept"]);
    expected.extend(REPLACED.iter().map(|(name, _)| *name));
    expected.sort_unstable();
    let names: Vec<&str> = after_refusal
        .iter()
        .map(|(name, ..)| name.as_str())
        .collect();
    assert_eq!(names, expected);
    let moved = after_refusal
        .iter()
        .find(|(name, ..)| name == "operator_kept")
        .context("the invalid index is missing")?;
    assert!(!moved.2, "the moved invalid index was made valid");
    assert_eq!(
        moved.3, leftover.3,
        "the invalid index was not merely renamed"
    );
    assert_eq!(
        schema_versions(&pool).await?,
        [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 18, 19]
    );
    sqlx::raw_sql(&format!(
        "DROP INDEX {SEQ_WALK}; ALTER INDEX operator_kept RENAME TO {SEQ_WALK}"
    ))
    .execute(&pool)
    .await?;
    let rebuilt = db.ledger("rebuilt").await?;
    assert_trimmed(&pool).await?;
    let leaf = table_indexes(&pool, "qbit_share_ledger_p0").await?;
    assert_eq!(
        oid(&leaf, &leaf_name(MINER_HISTORY)),
        oid(&after_refusal, MINER_HISTORY),
        "the kept build was rebuilt"
    );
    assert_ne!(
        oid(&leaf, &leaf_name(SEQ_WALK)).unwrap(),
        leftover.3,
        "the interrupted build's invalid index was not replaced"
    );
    // The writer that interrupted the build rolled back; the swaps' writers
    // committed.
    assert_eq!(share_count(&pool).await?, 3);
    db.close(vec![first, resumed, rebuilt]).await
}

/// A replacement already built and valid at preflight is kept without a
/// build of its own, so the run's step for it is over before the other
/// replacement's build starts, and that build takes hours on a large
/// ledger, in which the kept index can be moved aside like a drop target.
/// The whole declared set is looked up once more inside the transaction
/// that records the version: the moved index is refused there, 13 is not
/// recorded, and once the operator puts the name back the next start
/// records it keeping both indexes.
#[tokio::test]
async fn migration_013_refuses_to_record_when_a_kept_index_moved_while_it_built() -> Result<()> {
    const SEQ_WINDOW: &str = "qbit_share_ledger_accepted_seq_window_idx";
    const MINER_RECENT: &str = "qbit_share_ledger_accepted_miner_recent_idx";
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let oid = |indexes: &[(String, String, bool, String)], name: &str| {
        indexes
            .iter()
            .find(|(found, ..)| found == name)
            .map(|(_, _, _, oid)| oid.clone())
    };
    let first = db.ledger("first").await?;
    insert_share(&pool, 1, "alice").await?;
    undo_013(&pool).await?;
    // An earlier build of the migration's own under one reserved name, so
    // the run plans Keep for it and builds only the other replacement.
    sqlx::raw_sql(&format!(
        "CREATE TABLE operator_shares (miner_id text); {SEQ_WALK_DEFINITION}"
    ))
    .execute(&pool)
    .await?;
    let prebuilt = ledger_indexes(&pool).await?;
    // The kept index is swapped while that build waits. The build, its
    // check and every drop then run as planned, and the record refuses.
    let (error, _) =
        swap_during_build(&db, &pool, SEQ_WALK, 2, "refusing to record migration 13").await?;
    assert!(
        error.contains(&format!(
            "but the migration declares a valid {SEQ_WALK_DEFINITION} on qbit_share_ledger under that name"
        )),
        "{error}"
    );
    assert!(
        error.contains(&format!(
            "It had already built {MINER_HISTORY} and dropped {MINER_RECENT}, {SEQ_WINDOW}, qbit_share_ledger_accepted_window_idx, qbit_share_ledger_template_height_idx; both stay as they are"
        )),
        "{error}"
    );
    assert!(error.contains("migrate again"), "{error}");
    assert_eq!(
        schema_versions(&pool).await?,
        [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 18, 19]
    );
    let after_refusal = ledger_indexes(&pool).await?;
    let mut expected: Vec<&str> = KEPT.to_vec();
    expected.extend([MINER_HISTORY, "operator_kept"]);
    expected.sort_unstable();
    let names: Vec<&str> = after_refusal
        .iter()
        .map(|(name, ..)| name.as_str())
        .collect();
    assert_eq!(names, expected);
    let moved = after_refusal
        .iter()
        .find(|(name, ..)| name == "operator_kept")
        .context("the kept index is missing")?;
    assert!(moved.2, "the moved kept index was made invalid");
    assert_eq!(
        Some(moved.3.clone()),
        oid(&prebuilt, SEQ_WALK),
        "the kept index was not merely renamed"
    );
    assert_eq!(share_count(&pool).await?, 2);
    // The operator puts the name back; the next start keeps both indexes,
    // skips the drops, and records 13.
    sqlx::raw_sql(&format!(
        "DROP INDEX {SEQ_WALK}; ALTER INDEX operator_kept RENAME TO {SEQ_WALK}"
    ))
    .execute(&pool)
    .await?;
    let resumed = db.ledger("resumed").await?;
    assert_trimmed(&pool).await?;
    let leaf = table_indexes(&pool, "qbit_share_ledger_p0").await?;
    assert_eq!(
        oid(&leaf, &leaf_name(SEQ_WALK)),
        oid(&prebuilt, SEQ_WALK),
        "the kept index was rebuilt"
    );
    assert_eq!(
        oid(&leaf, &leaf_name(MINER_HISTORY)),
        oid(&after_refusal, MINER_HISTORY),
        "the built index was rebuilt"
    );
    db.close(vec![first, resumed]).await
}
