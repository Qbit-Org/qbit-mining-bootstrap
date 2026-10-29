//! 027's and 028's up-front table locks against a real PostgreSQL, on the
//! schema rc.6 serves (26, without 27, 28, 31 and 32): what the migration
//! transaction holds while it waits for them, that the files wait for no
//! other lock once it has them, and what a migrate that cannot take them by
//! its deadline leaves and says, and what it does about an autovacuum pass
//! holding one of the tables, on a cluster of the test's own. The cycle
//! with a serving frontend itself is `ledger_postgres`'s
//! `premigrate_lock_order`.
use super::*;
use crate::coordinator::miner_tests::SharedLog;
use qbit_prism_test_gate as gate;
use sqlx::{Connection, PgConnection};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tracing::instrument::WithSubscriber;

struct Database {
    admin: PgPool,
    schema: String,
    url: String,
    ledger: crate::ledger::Ledger,
}

impl Database {
    /// A native ledger in a schema of its own, every migration applied, and
    /// then 032, 031, 028 and 027 undone, as rc.6 left it.
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        Ok(Some(Self::open_at(&raw).await?))
    }

    /// The same at `raw`, whose role creates the schema and owns the ledger.
    async fn open_at(raw: &str) -> Result<Self> {
        let admin = PgPool::connect(raw).await?;
        let schema = format!("prism_table_locks_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let url = in_schema(raw, &schema)?;
        let ledger = crate::ledger::Ledger::connect(&url, "table-locks".into(), 2, true).await?;
        let mut undo = String::from(
            "DELETE FROM qbit_prism_schema_migrations WHERE version IN (27,28,31,32); \
             DROP FUNCTION qbit_prism_append_share, qbit_prism_ledger_clock; \
             DROP INDEX qbit_share_ledger_origin_seq_idx; \
             ALTER TABLE qbit_prism_audit_snapshots DROP CONSTRAINT qbit_prism_audit_snapshots_cut_check, DROP COLUMN cut_seq_0, DROP COLUMN cut_seq_1; \
             DROP TABLE qbit_prism_peer_sync_conflicts, qbit_prism_peer_sync_cursors, \
                 qbit_prism_node_lineage, qbit_prism_node_identity, qbit_prism_node_roles; \
             DROP FUNCTION qbit_prism_peer_share_mark(), qbit_prism_preserve_node_roles(); \
             DROP INDEX qbit_pool_blocks_origin_sync_idx, qbit_prism_jobs_prepared_origin_sync_idx, qbit_prism_jobs_origin_expiry_idx; \
             ALTER TABLE qbit_pool_blocks DROP COLUMN sync_seq; ALTER TABLE qbit_prism_jobs DROP COLUMN sync_seq; \
             DROP FUNCTION qbit_prism_next_sync_seq(), qbit_prism_sync_barrier(); DROP SEQUENCE qbit_prism_sync_seq;",
        );
        for table in crate::peer_sync::COPIED_TABLES
            .iter()
            .filter(|table| **table != "qbit_prism_node_roles")
        {
            undo.push_str(&format!(" ALTER TABLE {table} DROP COLUMN origin_node;"));
        }
        sqlx::raw_sql(&undo).execute(&ledger.pool).await?;
        Ok(Self {
            admin,
            schema,
            url,
            ledger,
        })
    }

    async fn versions(&self) -> Result<Vec<i32>> {
        Ok(
            sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
                .fetch_all(&self.ledger.pool)
                .await?,
        )
    }

    async fn close(self) -> Result<()> {
        self.ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

/// `raw` with `schema` as its search path.
fn in_schema(raw: &str, schema: &str) -> Result<String> {
    let mut url = url::Url::parse(raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    Ok(url.into())
}

/// Drop the schema after a test's `result`, keeping both errors.
async fn finish(db: Database, result: Result<()>) -> Result<()> {
    let closed = db.close().await;
    match (result, closed) {
        (Ok(()), closed) => closed,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(closed)) => {
            Err(error.context(format!("schema cleanup also failed: {closed:#}")))
        }
    }
}

/// The relations in this schema now, by OID.
async fn relations(connection: &mut PgConnection) -> Result<Vec<sqlx::postgres::types::Oid>> {
    Ok(sqlx::query_scalar(
        "SELECT oid FROM pg_class WHERE relnamespace=current_schema()::regnamespace",
    )
    .fetch_all(connection)
    .await?)
}

/// Once the transaction holds the tables 027 and 028 alter, applying the
/// two files takes a lock on no other relation that existed before them,
/// beyond those tables' partitions and indexes, which no other transaction
/// can lock without first locking the table. So the files wait for nothing
/// a serving frontend holds, and the only wait of the migration on such a
/// lock is the up-front one, bounded by `ATTEMPT`.
#[tokio::test]
async fn once_it_holds_their_tables_027_and_028_wait_for_no_other_lock() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = applied_after_the_lock(&db).await;
    finish(db, result).await
}

async fn applied_after_the_lock(db: &Database) -> Result<()> {
    ensure!(
        !db.versions().await?.contains(&27),
        "the undo left 27 recorded"
    );
    let mut connection = PgConnection::connect(&db.url).await?;
    let existing = relations(&mut connection).await?;
    let mut tx = connection.begin().await?;
    lock_altered_tables(&mut tx, &[26]).await?;
    let (_, tables) = pending(&[26]);
    let covered: Vec<sqlx::postgres::types::Oid> = sqlx::query_scalar(
            "WITH tables AS (SELECT t.name::regclass::oid AS oid FROM unnest($1::text[]) t(name) \
                             UNION SELECT p.relid::oid FROM unnest($1::text[]) t(name), LATERAL pg_partition_tree(t.name::regclass) p) \
             SELECT oid FROM tables UNION SELECT indexrelid FROM pg_index WHERE indrelid IN (SELECT oid FROM tables)",
        )
        .bind(&tables)
        .fetch_all(&mut *tx)
        .await?;
    sqlx::raw_sql(native_migration(27))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(native_migration(28))
        .execute(&mut *tx)
        .await?;
    let locked: Vec<(sqlx::postgres::types::Oid, String, String)> = sqlx::query_as(
        "SELECT relation,relation::regclass::text,mode FROM pg_locks \
             WHERE locktype='relation' AND pid=pg_backend_pid() AND granted",
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await?;
    connection.close().await?;
    let others: Vec<String> = locked
        .iter()
        .filter(|(oid, ..)| existing.contains(oid) && !covered.contains(oid))
        .map(|(_, name, mode)| format!("{name} {mode}"))
        .collect();
    ensure!(
        others.is_empty(),
        "027 and 028 locked relations beyond the tables locked up front: {others:?}"
    );
    // And the up-front lock did cover them: all twelve tables, and the
    // share ledger's partitions with it.
    let held = locked
        .iter()
        .filter(|(oid, _, mode)| covered.contains(oid) && mode == "AccessExclusiveLock")
        .count();
    ensure!(
        held > tables.len(),
        "{held} relations held exclusively: {locked:?}"
    );
    Ok(())
}

/// While its up-front request waits, the migration transaction holds only
/// the tables it has locked so far of that request, the migration history
/// (which only a starting frontend reads) and shared locks from its checks,
/// which no serving frontend's statement waits for: nothing else that a
/// frontend may need while the migrate waits.
#[tokio::test]
async fn while_it_waits_for_their_tables_the_migrate_holds_nothing_else_a_frontend_needs(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = held_while_waiting(&db).await;
    finish(db, result).await
}

async fn held_while_waiting(db: &Database) -> Result<()> {
    let mut frontend = PgConnection::connect(&db.url).await?;
    let existing = relations(&mut frontend).await?;
    let (_, tables) = pending(&[26]);
    let allowed: Vec<sqlx::postgres::types::Oid> = sqlx::query_scalar(
        "SELECT t.name::regclass::oid FROM unnest($1::text[]) t(name) \
         UNION SELECT p.relid::oid FROM unnest($1::text[]) t(name), LATERAL pg_partition_tree(t.name::regclass) p \
         UNION SELECT 'qbit_prism_schema_migrations'::regclass::oid",
    )
    .bind(&tables)
    .fetch_all(&db.ledger.pool)
    .await?;
    // A read of the last table locked, open: every attempt waits for it.
    let mut reader = frontend.begin().await?;
    sqlx::query("SELECT count(*) FROM qbit_share_ledger")
        .execute(&mut *reader)
        .await?;
    let migrate = committed_within(
        &db.ledger.pool,
        "table-locks-wait",
        ShareHashBackfill::Initialize,
        None,
        Duration::from_secs(30),
    );
    // Every relation lock of a backend whose request for one of them
    // waits, read at once while it waits; then the read ends.
    let observe = async {
        let seen = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let held: Vec<(sqlx::postgres::types::Oid, String, String)> = sqlx::query_as(
                    "SELECT l.relation,l.relation::regclass::text,l.mode FROM pg_locks l \
                     WHERE l.locktype='relation' AND l.granted AND l.pid=(SELECT w.pid FROM pg_locks w \
                         WHERE w.locktype='relation' AND NOT w.granted AND w.mode='AccessExclusiveLock' \
                           AND w.relation=ANY($1) LIMIT 1)",
                )
                .bind(&allowed)
                .fetch_all(&db.ledger.pool)
                .await?;
                if !held.is_empty() {
                    break Ok::<_, anyhow::Error>(held);
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("no attempt of the migrate was seen waiting for 027's tables");
        reader.commit().await?;
        seen?
    };
    let (online, seen) = tokio::join!(migrate, observe);
    frontend.close().await?;
    let (online, seen) = (online?, seen?);
    ensure!(
        online.iter().any(|pending| pending.version() == 31),
        "{online:?}"
    );
    let shared = ["AccessShareLock", "RowShareLock"];
    let beyond: Vec<String> = seen
        .iter()
        .filter(|(oid, _, mode)| {
            existing.contains(oid) && !allowed.contains(oid) && !shared.contains(&mode.as_str())
        })
        .map(|(_, name, mode)| format!("{name} {mode}"))
        .collect();
    ensure!(
        beyond.is_empty(),
        "while waiting the migrate held {beyond:?} (all: {seen:?})"
    );
    ensure!(
        seen.iter()
            .any(|(_, name, mode)| name == "qbit_pool_blocks" && mode == "AccessExclusiveLock"),
        "the waiting attempt did not hold the first table: {seen:?}"
    );
    let versions = db.versions().await?;
    ensure!(
        versions.contains(&27) && versions.contains(&28),
        "{versions:?}"
    );
    Ok(())
}

/// Two reads of tables 027 alters that stay open past the migrate's
/// deadline: every attempt gives up, and the refusal names each reader's
/// transaction once, oldest first, with the tables it holds (the share
/// ledger once, not its partitions) and its query, so a transaction that
/// holds several tables cannot hide another. Nothing of 027 or 028 is left
/// behind, and once the reads end the next migrate applies both.
#[tokio::test]
async fn a_migrate_that_cannot_lock_027s_tables_by_its_deadline_names_the_holder_and_changes_nothing(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = refused_at_the_deadline(&db).await;
    finish(db, result).await
}

async fn refused_at_the_deadline(db: &Database) -> Result<()> {
    let mut frontend = PgConnection::connect(&db.url).await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut frontend)
        .await?;
    let mut reader = frontend.begin().await?;
    sqlx::query("SELECT count(*) /* an rc.6 broadcaster */ FROM qbit_prism_templates")
        .execute(&mut *reader)
        .await?;
    // A read of the share ledger holds every partition too; the refusal
    // names its table once.
    sqlx::query("SELECT count(*) /* an rc.6 broadcaster */ FROM qbit_share_ledger")
        .execute(&mut *reader)
        .await?;
    // A second, younger transaction on another table.
    let mut collector = PgConnection::connect(&db.url).await?;
    let collector_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut collector)
        .await?;
    let mut second = collector.begin().await?;
    sqlx::query("SELECT count(*) /* an rc.6 collector */ FROM qbit_pool_blocks")
        .execute(&mut *second)
        .await?;
    let started = std::time::Instant::now();
    let error = committed_within(
        &db.ledger.pool,
        "table-locks-deadline",
        ShareHashBackfill::Initialize,
        None,
        Duration::from_secs(1),
    )
    .await
    .err()
    .context("the migrate took its tables while a read held one")?;
    let elapsed = started.elapsed();
    let message = format!("{error:#}");
    ensure!(
        message.contains("refusing to wait any longer for the table locks of migration 27, 28")
            && message.contains("Nothing was changed: they are not recorded"),
        "{message}"
    );
    let holding = message
        .split_once("Holding them now: ")
        .map(|(_, holding)| holding)
        .context(message.clone())?;
    let broadcaster = holding
        .find(&format!(
            "pid {pid} (idle in transaction, its transaction open "
        ))
        .context(message.clone())?;
    let collector_row = holding
        .find(&format!(
            "pid {collector_pid} (idle in transaction, its transaction open "
        ))
        .context(message.clone())?;
    let rows = |needle: &str| holding.matches(needle).count();
    ensure!(
        broadcaster < collector_row
            && rows(" holding ") == 2
            && rows(&format!("pid {pid} (")) == 1
            && rows(&format!("pid {collector_pid} (")) == 1
            && holding.contains(
                "s) holding qbit_prism_templates, qbit_share_ledger: SELECT count(*) /* an rc.6 broadcaster */ FROM qbit_share_ledger"
            )
            && holding.contains(
                "s) holding qbit_pool_blocks: SELECT count(*) /* an rc.6 collector */ FROM qbit_pool_blocks"
            )
            && rows("qbit_share_ledger_p") == 0,
        "one row per transaction, oldest first, its tables listed once: {message}"
    );
    ensure!(
        elapsed < Duration::from_secs(5),
        "the deadline of 1 s took {elapsed:?}"
    );
    ensure!(
        !db.versions()
            .await?
            .iter()
            .any(|version| [27, 28].contains(version)),
        "a refused migrate recorded 27 or 28"
    );
    let left: (bool, bool, bool) = sqlx::query_as(
            "SELECT to_regclass('qbit_prism_node_roles') IS NOT NULL, \
                    to_regclass('qbit_prism_sync_seq') IS NOT NULL, \
                    EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='qbit_pool_blocks'::regclass AND attname='origin_node' AND NOT attisdropped)",
        )
        .fetch_one(&db.ledger.pool)
        .await?;
    ensure!(left == (false, false, false), "027 left {left:?}");
    reader.commit().await?;
    frontend.close().await?;
    second.commit().await?;
    collector.close().await?;
    committed_within(
        &db.ledger.pool,
        "table-locks-deadline",
        ShareHashBackfill::Initialize,
        None,
        Duration::from_secs(30),
    )
    .await?;
    let versions = db.versions().await?;
    ensure!(
        versions.contains(&27) && versions.contains(&28),
        "{versions:?}"
    );
    Ok(())
}

/// A read of the migration history held open past the migrate's deadline:
/// every attempt gives up its request for the history within 100 ms, as for
/// 027's tables, and the refusal names the history, the pid holding it and
/// its query, before anything is read or changed. Once the read ends the
/// next migrate applies 27 and 28.
#[tokio::test]
async fn a_history_read_held_past_the_deadline_is_refused_like_the_tables() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = history_refused_at_the_deadline(&db).await;
    finish(db, result).await
}

async fn history_refused_at_the_deadline(db: &Database) -> Result<()> {
    let mut frontend = PgConnection::connect(&db.url).await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut frontend)
        .await?;
    let mut reader = frontend.begin().await?;
    sqlx::query("SELECT count(*) /* a starting frontend */ FROM qbit_prism_schema_migrations")
        .execute(&mut *reader)
        .await?;
    let started = std::time::Instant::now();
    let error = committed_within(
        &db.ledger.pool,
        "table-locks-history",
        ShareHashBackfill::Initialize,
        None,
        Duration::from_secs(1),
    )
    .await
    .err()
    .context("the migrate locked the history while a read held it")?;
    let elapsed = started.elapsed();
    let message = format!("{error:#}");
    ensure!(
        message
            .contains("refusing to wait any longer for the table locks of the migration history")
            && message.contains(&format!(
                "pid {pid} (idle in transaction"
            ))
            && message.contains(
                "s) holding qbit_prism_schema_migrations: SELECT count(*) /* a starting frontend */"
            )
            && message.contains("Nothing was changed. Holding"),
        "{message}"
    );
    ensure!(
        elapsed < Duration::from_secs(5),
        "the deadline of 1 s took {elapsed:?}"
    );
    ensure!(
        !db.versions().await?.contains(&27),
        "a refused migrate recorded 27"
    );
    reader.commit().await?;
    frontend.close().await?;
    committed_within(
        &db.ledger.pool,
        "table-locks-history",
        ShareHashBackfill::Initialize,
        None,
        Duration::from_secs(30),
    )
    .await?;
    let versions = db.versions().await?;
    ensure!(
        versions.contains(&27) && versions.contains(&28),
        "{versions:?}"
    );
    Ok(())
}

/// Where `deadlock_timeout` is below `ATTEMPT`, PostgreSQL can break a cycle
/// through the up-front request by aborting the request itself. That is
/// contention, tried again like a timeout, not a failure of the migrate.
/// The request here locks two tables of its own, the first free and the
/// second held by a reader that then asks for the first, with a deadlock
/// check due after 80 ms of waiting: before the request's own 100 ms
/// timeouts, and long after the reader joins the cycle.
#[tokio::test]
async fn a_deadlock_abort_of_the_up_front_request_is_contention() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = deadlock_is_contention(&db, false).await;
    finish(db, result).await
}

/// Where the run lowers `deadlock_timeout` but this role may not, after all
/// (its grant revoked mid-run, a `SET ROLE`), the refused `SET` is
/// `CannotLower`, which `committed_within` runs again without lowering, not
/// a failure of the migrate; without lowering the same role sets its other
/// timeouts as before, and only the table is refused it.
#[tokio::test]
async fn a_lowered_deadlock_timeout_refused_to_the_role_is_cannot_lower() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = refused_lowering(&db).await;
    finish(db, result).await
}

async fn refused_lowering(db: &Database) -> Result<()> {
    let role = format!("prism_cannot_lower_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE ROLE {role} NOLOGIN"))
        .execute(&db.admin)
        .await?;
    let tried = async {
        let mut connection = PgConnection::connect(&db.url).await?;
        let mut outcomes = Vec::new();
        for lower in [true, false] {
            let mut tx = connection.begin().await?;
            sqlx::query(&format!("SET LOCAL ROLE {role}"))
                .execute(&mut *tx)
                .await?;
            let error = LOWER_DEADLOCK_TIMEOUT
                .scope(
                    lower,
                    lock_within_attempt(
                        &mut tx,
                        "LOCK TABLE qbit_prism_templates IN ACCESS EXCLUSIVE MODE",
                        Locking::Migrations(vec![27]),
                        vec!["qbit_prism_templates"],
                        "the test's request".to_owned(),
                    ),
                )
                .await
                .err()
                .context("the role locked a table it has no privilege on")?;
            tx.rollback().await?;
            outcomes.push(error);
        }
        connection.close().await?;
        Ok::<_, anyhow::Error>(outcomes)
    }
    .await;
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&db.admin)
        .await?;
    let outcomes = tried?;
    ensure!(
        outcomes[0].downcast_ref::<CannotLower>().is_some(),
        "a refused lowered SET was not CannotLower: {:#}",
        outcomes[0]
    );
    ensure!(
        outcomes[1].downcast_ref::<CannotLower>().is_none()
            && outcomes[1].downcast_ref::<LockContention>().is_none()
            && format!("{:#}", outcomes[1]).starts_with("the test's request: "),
        "{:#}",
        outcomes[1]
    );
    Ok(())
}

/// Where the run lowers `deadlock_timeout` (`LOWER_DEADLOCK_TIMEOUT`), the
/// request itself is checked for a deadlock after `DEADLOCK_CHECK`, with no
/// lowering of its own, so the same cycle is broken within the attempt by
/// aborting it, which is contention. A request that takes its tables
/// leaves the transaction's `deadlock_timeout` as it found it, so nothing
/// after the up-front locks runs under the lowered one.
#[tokio::test]
async fn a_lowered_deadlock_timeout_holds_for_the_lock_request_only() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = lowered_for_the_request(&db).await;
    finish(db, result).await
}

async fn lowered_for_the_request(db: &Database) -> Result<()> {
    deadlock_is_contention(db, true).await?;
    let mut migrate = PgConnection::connect(&db.url).await?;
    let mut tx = migrate.begin().await?;
    let before: String = sqlx::query_scalar("SELECT current_setting('deadlock_timeout')")
        .fetch_one(&mut *tx)
        .await?;
    LOWER_DEADLOCK_TIMEOUT
        .scope(
            true,
            lock_within_attempt(
                &mut tx,
                "LOCK TABLE deadlock_first, deadlock_second IN ACCESS EXCLUSIVE MODE",
                Locking::Migrations(vec![27]),
                vec!["deadlock_first", "deadlock_second"],
                "the test's request".to_owned(),
            ),
        )
        .await?;
    let after: String = sqlx::query_scalar("SELECT current_setting('deadlock_timeout')")
        .fetch_one(&mut *tx)
        .await?;
    tx.rollback().await?;
    migrate.close().await?;
    ensure!(
        before == after && after != format!("{}ms", DEADLOCK_CHECK.as_millis()),
        "deadlock_timeout {before} before the request, {after} after it"
    );
    Ok(())
}

/// The cycle above, the request's deadlock check due after 80 ms of its
/// own, or after `DEADLOCK_CHECK` where the run lowers it (`lowered`).
async fn deadlock_is_contention(db: &Database, lowered: bool) -> Result<()> {
    sqlx::raw_sql("CREATE TABLE deadlock_first(x int); CREATE TABLE deadlock_second(x int)")
        .execute(&db.ledger.pool)
        .await?;
    let mut reader = PgConnection::connect(&db.url).await?;
    // Only the request checks for the deadlock.
    sqlx::query("SET deadlock_timeout = '10s'")
        .execute(&mut reader)
        .await?;
    let mut read = reader.begin().await?;
    sqlx::query("SELECT count(*) FROM deadlock_second")
        .execute(&mut *read)
        .await?;
    let mut migrate = PgConnection::connect(&db.url).await?;
    let mut tx = migrate.begin().await?;
    if !lowered {
        sqlx::query("SET LOCAL deadlock_timeout = '80ms'")
            .execute(&mut *tx)
            .await?;
    }
    let request = LOWER_DEADLOCK_TIMEOUT.scope(
        lowered,
        lock_within_attempt(
            &mut tx,
            "LOCK TABLE deadlock_first, deadlock_second IN ACCESS EXCLUSIVE MODE",
            Locking::Migrations(vec![27]),
            vec!["deadlock_first", "deadlock_second"],
            "the test's request".to_owned(),
        ),
    );
    let reader_side = async {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='relation' \
                 AND relation='deadlock_second'::regclass AND NOT granted)",
            )
            .fetch_one(&db.ledger.pool)
            .await?
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("the request never waited for the reader's table")??;
        // Closes the cycle; answered once the request is aborted.
        sqlx::query("SELECT count(*) FROM deadlock_first")
            .execute(&mut *read)
            .await?;
        read.commit().await?;
        Ok::<_, anyhow::Error>(())
    };
    let (requested, read) = tokio::join!(request, reader_side);
    read?;
    let error = requested
        .err()
        .context("the request took both tables through the cycle")?;
    let contention = error
        .downcast_ref::<LockContention>()
        .with_context(|| format!("a deadlock abort was not contention: {error:#}"))?;
    ensure!(
        contention.reason.contains("deadlock detected"),
        "the request gave up for another reason: {error:#}"
    );
    tx.rollback().await?;
    migrate.close().await?;
    reader.close().await?;
    Ok(())
}

/// A PostgreSQL of the test's own, from `PRISM_TEST_PG_BIN_DIR`, whose
/// autovacuum visits the database every second but, by its thresholds,
/// processes no table that does not lower them itself: so the one pass a
/// test starts is the only one. (`autovacuum = off` would not do: a table's
/// own `autovacuum_enabled` can only turn it off.) Like `cutover_rehearsal`'s cluster it listens on
/// loopback TCP only, with no Unix socket, and every role logs in with a
/// random password, so a shared host's other users cannot reach it;
/// stopped when dropped, even after a start that failed half way.
struct Cluster {
    bin: PathBuf,
    dir: tempfile::TempDir,
    port: u16,
    password: String,
    running: bool,
}

impl Cluster {
    /// The bootstrap superuser.
    const SUPERUSER: &'static str = "prism";

    fn start(bin: String) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let password = uuid::Uuid::new_v4().simple().to_string();
        let password_file = dir.path().join("password");
        std::fs::write(&password_file, &password)?;
        // Held until `pg_ctl start`, so initdb's run cannot lose it (#639).
        let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        let mut cluster = Self {
            bin: bin.into(),
            port: reservation.local_addr()?.port(),
            dir,
            password,
            running: false,
        };
        let status = Command::new(cluster.bin.join("initdb"))
            .args(["-U", Self::SUPERUSER, "--auth=scram-sha-256", "-E", "UTF8"])
            .arg(format!("--pwfile={}", password_file.display()))
            .arg("-D")
            .arg(cluster.dir.path().join("data"))
            .stdout(Stdio::null())
            .status()?;
        std::fs::remove_file(&password_file)?;
        ensure!(status.success(), "initdb failed");
        // Marked first, so a start that fails half way is still stopped.
        cluster.running = true;
        drop(reservation);
        let status = Command::new(cluster.bin.join("pg_ctl"))
            .arg("-D")
            .arg(cluster.dir.path().join("data"))
            .arg("-l")
            .arg(cluster.dir.path().join("postgres.log"))
            .arg("-o")
            .arg(format!(
                "-p {} -c listen_addresses=127.0.0.1 -c unix_socket_directories= -c autovacuum_naptime=1s \
                 -c autovacuum_vacuum_threshold=2000000000 -c autovacuum_vacuum_insert_threshold=2000000000 \
                 -c autovacuum_analyze_threshold=2000000000",
                cluster.port
            ))
            .args(["-w", "start"])
            .stdout(Stdio::null())
            .status()?;
        ensure!(
            status.success(),
            "pg_ctl start on 127.0.0.1:{} failed: {}",
            cluster.port,
            cluster.server_log().unwrap_or_default()
        );
        Ok(cluster)
    }

    /// `role`'s URL; every role this test creates has the cluster's password.
    fn url(&self, role: &str) -> String {
        format!(
            "postgresql://{role}:{}@127.0.0.1:{}/postgres",
            self.password, self.port
        )
    }

    fn server_log(&self) -> Result<String> {
        Ok(std::fs::read_to_string(
            self.dir.path().join("postgres.log"),
        )?)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if self.running {
            let _ = Command::new(self.bin.join("pg_ctl"))
                .arg("-D")
                .arg(self.dir.path().join("data"))
                .args(["-m", "immediate", "-w", "stop"])
                .stdout(Stdio::null())
                .status();
        }
    }
}

/// An autovacuum pass holding a share ledger partition, on a ledger in
/// rc.6's schema that `migrator` owns: no superuser, no reader of other
/// roles' activity, and no `SET` on `deadlock_timeout`. The worker's pid and
/// the partition.
struct Pass {
    db: Database,
    /// The cluster's superuser, in the ledger's schema.
    superuser: PgPool,
    pid: i32,
    partition: String,
    oid: sqlx::postgres::types::Oid,
}

impl Pass {
    /// Rows go into a share ledger partition, which is then, alone in the
    /// cluster, vacuumed after any insert, one page per 100 ms, so the pass
    /// lasts minutes, idle nearly all of it. Once its worker holds the
    /// partition, autovacuum is turned off: that pass runs on, and no other
    /// starts.
    async fn start(cluster: &Cluster) -> Result<Self> {
        let root = PgPool::connect(&cluster.url(Cluster::SUPERUSER)).await?;
        sqlx::raw_sql(&format!(
            "CREATE ROLE migrator LOGIN PASSWORD '{}'; GRANT CREATE ON DATABASE postgres TO migrator",
            cluster.password
        ))
        .execute(&root)
        .await?;
        let db = Database::open_at(&cluster.url("migrator")).await?;
        sqlx::query(
            "INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) \
             SELECT 'autovacuum:'||g,'miner','miner',decode(repeat('11',32),'hex'),1,1,1,'job',to_timestamp(1),1,to_timestamp(2),true,'fixture',0 \
             FROM generate_series(1,20000) g",
        )
        .execute(&db.ledger.pool)
        .await?;
        let partitions: Vec<(sqlx::postgres::types::Oid, String)> = sqlx::query_as(
            "SELECT DISTINCT tableoid,tableoid::regclass::text FROM qbit_share_ledger",
        )
        .fetch_all(&db.ledger.pool)
        .await?;
        let [(oid, partition)] = &partitions[..] else {
            bail!("the rows went into {partitions:?}, not one partition");
        };
        let (oid, partition) = (*oid, partition.clone());
        sqlx::query(&format!(
            "ALTER TABLE {partition} SET (autovacuum_vacuum_insert_threshold = 1, autovacuum_vacuum_insert_scale_factor = 0, \
                 autovacuum_vacuum_cost_delay = 100, autovacuum_vacuum_cost_limit = 1)"
        ))
        .execute(&db.ledger.pool)
        .await?;
        let pid = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let found: Option<i32> = sqlx::query_scalar(
                    "SELECT a.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid \
                     WHERE a.backend_type='autovacuum worker' AND l.locktype='relation' AND l.granted AND l.relation=$1",
                )
                .bind(oid)
                .fetch_optional(&root)
                .await?;
                if let Some(found) = found {
                    break Ok::<_, anyhow::Error>(found);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .with_context(|| format!("no autovacuum worker took {partition} within 60 s"))??;
        sqlx::query("ALTER SYSTEM SET autovacuum = off")
            .execute(&root)
            .await?;
        sqlx::query("SELECT pg_reload_conf()")
            .execute(&root)
            .await?;
        root.close().await;
        let superuser =
            PgPool::connect(&in_schema(&cluster.url(Cluster::SUPERUSER), &db.schema)?).await?;
        Ok(Self {
            db,
            superuser,
            pid,
            partition,
            oid,
        })
    }

    /// Whether the worker still holds the partition: a cancelled pass
    /// leaves it, though the worker goes on with the rest of its list.
    async fn holding(&self) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='relation' AND relation=$2 AND granted)",
        )
        .bind(self.pid)
        .bind(self.oid)
        .fetch_one(&self.superuser)
        .await?)
    }

    /// The migrate on `pool`, its deadline `budget`, logged to `log`.
    async fn migrate(pool: &PgPool, budget: Duration, log: &SharedLog) -> Result<()> {
        committed_within(
            pool,
            "table-locks-autovacuum",
            ShareHashBackfill::Initialize,
            None,
            budget,
        )
        .with_subscriber(log.dispatch())
        .await
        .map(drop)
    }

    /// After a migrate that took the tables: the worker no longer holds the
    /// partition, PostgreSQL logged the cancel it sent from the migrate's
    /// deadlock check, and 27 and 28 are recorded.
    async fn cancelled(&self, cluster: &Cluster) -> Result<()> {
        ensure!(!self.holding().await?, "the pass outlived the migrate");
        let server = cluster.server_log()?;
        ensure!(
            server.contains("canceling autovacuum task"),
            "PostgreSQL logged no cancelled autovacuum task: {server}"
        );
        let versions = self.db.versions().await?;
        ensure!(
            versions.contains(&27) && versions.contains(&28),
            "{versions:?}"
        );
        Ok(())
    }

    /// The cluster goes with the schema, so nothing is dropped.
    async fn close(self) {
        self.superuser.close().await;
        self.db.ledger.pool.close().await;
        self.db.admin.close().await;
    }
}

/// An autovacuum pass holding a share ledger partition, which no 100 ms
/// attempt waits long enough for PostgreSQL's 1 s deadlock check to cancel,
/// so every attempt used to give up on it until the deadline: a superuser's
/// migrate lowers its own `deadlock_timeout`, PostgreSQL cancels the pass
/// from the second attempt's deadlock check, and 27 and 28 are applied.
#[tokio::test]
async fn the_migrate_cancels_an_autovacuum_pass_holding_a_share_ledger_partition() -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let cluster = Cluster::start(bin)?;
    let pass = Pass::start(&cluster).await?;
    let result = cancelled_by_the_migrate(&cluster, &pass).await;
    pass.close().await;
    result
}

async fn cancelled_by_the_migrate(cluster: &Cluster, pass: &Pass) -> Result<()> {
    ensure!(pass.holding().await?, "the pass ended before the migrate");
    let log = SharedLog::default();
    Pass::migrate(&pass.superuser, Duration::from_secs(20), &log)
        .await
        .with_context(|| format!("the migrate as a superuser: {}", log.text()))?;
    ensure!(
        !log.text().contains("may not lower its deadlock_timeout"),
        "{}",
        log.text()
    );
    pass.cancelled(cluster).await
}

/// A role that may not lower `deadlock_timeout` waits the pass out: it is
/// warned once, naming the worker, keeps trying until its deadline, and is
/// then refused, with the worker named first, on its partition, though its
/// query is hidden from the role, and the remedies, the grant first. Nothing is recorded and the
/// pass runs on. Once the role is granted `SET` on the parameter, the
/// migrate takes the tables as a superuser's does.
#[tokio::test]
async fn an_autovacuum_pass_the_role_may_not_cancel_is_named_in_the_refusal() -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let cluster = Cluster::start(bin)?;
    let pass = Pass::start(&cluster).await?;
    let result = named_then_granted(&cluster, &pass).await;
    pass.close().await;
    result
}

async fn named_then_granted(cluster: &Cluster, pass: &Pass) -> Result<()> {
    let log = SharedLog::default();
    let error = Pass::migrate(&pass.db.ledger.pool, Duration::from_secs(1), &log)
        .await
        .err()
        .context("the migrate took the tables while the pass held a partition")?;
    let message = format!("{error:#}");
    let worker = format!(
        "autovacuum worker pid {} on {}, its query hidden from this role, so whether it is an anti-wraparound vacuum is unknown",
        pass.pid, pass.partition
    );
    ensure!(
        message.contains("refusing to wait any longer for the table locks of migration 27, 28")
            && message.contains(
                "Nothing was changed: they are not recorded. Holding them now: autovacuum worker pid "
            )
            && message.contains(&worker)
            && message.ends_with(&format!(
                ". {LOWER_REMEDY}. {AUTOVACUUM_REMEDY}, and migrate again"
            )),
        "{message}"
    );
    let logs = log.text();
    ensure!(
        logs.matches("an autovacuum worker holds a table the migration must lock")
            .count()
            == 1
            && logs.contains(&format!("{LOWER_REMEDY}. {AUTOVACUUM_REMEDY}"))
            && logs.contains(&worker),
        "{message}\n{logs}"
    );
    ensure!(pass.holding().await?, "the pass ended: {message}");
    ensure!(
        !pass.db.versions().await?.contains(&27),
        "a refused migrate recorded 27"
    );
    let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
        .fetch_one(&pass.superuser)
        .await?;
    if version < 150_000 {
        // No `SET` privilege on a parameter before PostgreSQL 15.
        return Ok(());
    }
    sqlx::query("GRANT SET ON PARAMETER deadlock_timeout TO migrator")
        .execute(&pass.superuser)
        .await?;
    let log = SharedLog::default();
    Pass::migrate(&pass.db.ledger.pool, Duration::from_secs(20), &log)
        .await
        .with_context(|| {
            format!(
                "the migrate granted SET on deadlock_timeout: {}",
                log.text()
            )
        })?;
    pass.cancelled(cluster).await
}
