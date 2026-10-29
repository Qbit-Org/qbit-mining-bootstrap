//! 027's and 028's up-front table locks against a real PostgreSQL, on the
//! schema rc.6 serves (26, without 27, 28, 31 and 32): what the migration
//! transaction holds while it waits for them, that the files wait for no
//! other lock once it has them, and what a migrate that cannot take them by
//! its deadline leaves and says, and what it does about an autovacuum pass
//! holding one of the tables, on a cluster of the test's own. The cycle
//! with a serving frontend itself is `ledger_postgres`'s
//! `premigrate_lock_order`.
use super::*;
use qbit_prism_test_gate as gate;
use sqlx::{Connection, PgConnection};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
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
    let result = deadlock_is_contention(&db).await;
    finish(db, result).await
}

async fn deadlock_is_contention(db: &Database) -> Result<()> {
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
    sqlx::query("SET LOCAL deadlock_timeout = '80ms'")
        .execute(&mut *tx)
        .await?;
    let request = lock_within_attempt(
        &mut tx,
        "LOCK TABLE deadlock_first, deadlock_second IN ACCESS EXCLUSIVE MODE",
        Locking::Migrations(vec![27]),
        vec!["deadlock_first", "deadlock_second"],
        "the test's request".to_owned(),
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
/// autovacuum visits the database every second, which the shared test
/// database's does not; stopped when dropped.
struct Cluster {
    bin: PathBuf,
    dir: tempfile::TempDir,
    port: u16,
    superuser: String,
}

impl Cluster {
    fn start(bin: String) -> Result<Self> {
        let superuser = String::from_utf8(Command::new("id").arg("-un").output()?.stdout)?
            .trim()
            .to_owned();
        let cluster = Self {
            bin: bin.into(),
            dir: tempfile::tempdir()?,
            port: std::net::TcpListener::bind("127.0.0.1:0")?
                .local_addr()?
                .port(),
            superuser,
        };
        let data = cluster.path("data")?;
        cluster.command(
            "initdb",
            &["-D", &data, "-A", "trust", "--no-locale", "-E", "UTF8"],
        )?;
        cluster.command(
            "pg_ctl",
            &[
                "-D",
                &data,
                "-l",
                &cluster.path("postgres.log")?,
                "-o",
                &format!(
                    "-h 127.0.0.1 -p {} -k {} -c autovacuum_naptime=1s",
                    cluster.port,
                    cluster.path("")?
                ),
                "-w",
                "start",
            ],
        )?;
        Ok(cluster)
    }

    fn path(&self, name: &str) -> Result<String> {
        Ok(self
            .dir
            .path()
            .join(name)
            .to_str()
            .context("a test path that is not UTF-8")?
            .to_owned())
    }

    fn command(&self, binary: &str, args: &[&str]) -> Result<()> {
        let output = Command::new(self.bin.join(binary)).args(args).output()?;
        ensure!(
            output.status.success(),
            "{binary} failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    fn url(&self, role: &str) -> String {
        format!("postgresql://{role}@127.0.0.1:{}/postgres", self.port)
    }

    fn server_log(&self) -> Result<String> {
        Ok(std::fs::read_to_string(self.path("postgres.log")?)?)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if let Ok(data) = self.path("data") {
            let _ = self.command("pg_ctl", &["-D", &data, "-m", "immediate", "-w", "stop"]);
        }
    }
}

/// What a test logs, read back.
#[derive(Clone, Default)]
struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl LogCapture {
    fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync {
        let writer = self.clone();
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish()
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

/// An autovacuum pass holding a share ledger partition, on a ledger in
/// rc.6's schema that `migrator`, no superuser and no reader of other
/// roles' activity, owns: the worker's pid and the partition.
struct Pass {
    db: Database,
    /// The cluster's superuser, in the ledger's schema.
    superuser: PgPool,
    pid: i32,
    partition: String,
}

impl Pass {
    /// Every leaf partition of the share ledger is vacuumed after any
    /// insert, one page per 100 ms, so the pass on the rows inserted lasts
    /// minutes, idle nearly all of it. Once a worker holds a partition,
    /// autovacuum is turned off: that pass runs on, and no other starts.
    async fn start(cluster: &Cluster) -> Result<Self> {
        let root = PgPool::connect(&cluster.url(&cluster.superuser)).await?;
        sqlx::raw_sql("CREATE ROLE migrator LOGIN; GRANT CREATE ON DATABASE postgres TO migrator")
            .execute(&root)
            .await?;
        let db = Database::open_at(&cluster.url("migrator")).await?;
        let partitions: Vec<(sqlx::postgres::types::Oid, String)> = sqlx::query_as(
            "SELECT relid::oid,relid::regclass::text FROM pg_partition_tree('qbit_share_ledger') WHERE isleaf",
        )
        .fetch_all(&db.ledger.pool)
        .await?;
        ensure!(!partitions.is_empty(), "the share ledger has no partition");
        for (_, partition) in &partitions {
            sqlx::query(&format!(
                "ALTER TABLE {partition} SET (autovacuum_vacuum_insert_threshold = 1, autovacuum_vacuum_insert_scale_factor = 0, \
                     autovacuum_vacuum_cost_delay = 100, autovacuum_vacuum_cost_limit = 1)"
            ))
            .execute(&db.ledger.pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) \
             SELECT 'autovacuum:'||g,'miner','miner',decode(repeat('11',32),'hex'),1,1,1,'job',to_timestamp(1),1,to_timestamp(2),true,'fixture',0 \
             FROM generate_series(1,20000) g",
        )
        .execute(&db.ledger.pool)
        .await?;
        let oids: Vec<sqlx::postgres::types::Oid> =
            partitions.iter().map(|(oid, _)| *oid).collect();
        let (pid, oid) = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let found: Option<(i32, sqlx::postgres::types::Oid)> = sqlx::query_as(
                    "SELECT a.pid,l.relation FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid \
                     WHERE a.backend_type='autovacuum worker' AND l.locktype='relation' AND l.granted AND l.relation=ANY($1) LIMIT 1",
                )
                .bind(&oids)
                .fetch_optional(&root)
                .await?;
                if let Some(found) = found {
                    break Ok::<_, anyhow::Error>(found);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("no autovacuum worker took a share ledger partition within 60 s")??;
        sqlx::query("ALTER SYSTEM SET autovacuum = off")
            .execute(&root)
            .await?;
        sqlx::query("SELECT pg_reload_conf()")
            .execute(&root)
            .await?;
        root.close().await;
        let partition = partitions
            .into_iter()
            .find_map(|(partition_oid, name)| (partition_oid == oid).then_some(name))
            .context("the worker's partition")?;
        let superuser =
            PgPool::connect(&in_schema(&cluster.url(&cluster.superuser), &db.schema)?).await?;
        Ok(Self {
            db,
            superuser,
            pid,
            partition,
        })
    }

    async fn running(&self) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND backend_type='autovacuum worker')",
        )
        .bind(self.pid)
        .fetch_one(&self.superuser)
        .await?)
    }

    /// The cluster goes with the schema, so nothing is dropped.
    async fn close(self) {
        self.superuser.close().await;
        self.db.ledger.pool.close().await;
        self.db.admin.close().await;
    }
}

/// An autovacuum pass holding a share ledger partition, which no 100 ms
/// attempt waits long enough for PostgreSQL to cancel, so every attempt
/// gave up on it until the deadline: the migrate, as a superuser, cancels
/// the pass once an attempt has given up, logs the worker and its
/// partition, and then applies 27 and 28.
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
    ensure!(pass.running().await?, "the pass ended before the migrate");
    let logs = LogCapture::default();
    committed_within(
        &pass.superuser,
        "table-locks-autovacuum",
        ShareHashBackfill::Initialize,
        None,
        Duration::from_secs(20),
    )
    .with_subscriber(logs.subscriber())
    .await
    .with_context(|| format!("the migrate as a superuser: {}", logs.text()))?;
    let logs = logs.text();
    ensure!(
        logs.contains(&format!(
            "cancelled autovacuum worker pid {} on {}: it held a table the migration must lock",
            pass.pid, pass.partition
        )),
        "{logs}"
    );
    let versions = pass.db.versions().await?;
    ensure!(
        versions.contains(&27) && versions.contains(&28),
        "{versions:?}"
    );
    let server = cluster.server_log()?;
    ensure!(
        server.contains("canceling autovacuum task"),
        "the server logged no cancelled autovacuum task: {server}"
    );
    Ok(())
}

/// Where the role may not cancel the autovacuum pass, or cannot even see
/// its query, the migrate leaves it alone and keeps trying until its
/// deadline, then names it as an autovacuum worker on its partition, with
/// what an administrator can do and how many passes it cancelled: none.
/// The pass runs on, and nothing is recorded.
#[tokio::test]
async fn an_autovacuum_pass_the_role_may_not_cancel_is_named_in_the_refusal() -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let cluster = Cluster::start(bin)?;
    let pass = Pass::start(&cluster).await?;
    let result = named_not_cancelled(&pass).await;
    pass.close().await;
    result
}

async fn named_not_cancelled(pass: &Pass) -> Result<()> {
    let refused = |pool: PgPool, logs: LogCapture| async move {
        let error = committed_within(
            &pool,
            "table-locks-autovacuum",
            ShareHashBackfill::Initialize,
            None,
            Duration::from_secs(1),
        )
        .with_subscriber(logs.subscriber())
        .await
        .err()
        .context("the migrate took the tables while the pass held a partition")?;
        pool.close().await;
        Ok::<_, anyhow::Error>(format!("{error:#}"))
    };
    // `migrator` sees neither the worker's backend type nor its query.
    let hidden = refused(PgPool::connect(&pass.db.url).await?, LogCapture::default()).await?;
    ensure!(
        hidden.contains("refusing to wait any longer for the table locks of migration 27, 28")
            && hidden.contains(&format!(
                "Nothing was changed: they are not recorded. The migrate cancelled 0 autovacuum passes on them meanwhile. Holding them now: autovacuum worker pid {} on {}, its query hidden from this role, so whether it is an anti-wraparound vacuum is unknown. {AUTOVACUUM_REMEDY}, and migrate again",
                pass.pid, pass.partition
            )),
        "{hidden}"
    );
    ensure!(pass.running().await?, "the pass ended: {hidden}");
    // With pg_read_all_stats it sees the query, but before PostgreSQL 18
    // only a superuser may cancel an autovacuum worker, and from 18 a
    // member of pg_signal_autovacuum_worker.
    sqlx::query("GRANT pg_read_all_stats TO migrator")
        .execute(&pass.superuser)
        .await?;
    let logs = LogCapture::default();
    let seen = refused(PgPool::connect(&pass.db.url).await?, logs.clone()).await?;
    let logs = logs.text();
    ensure!(
        seen.contains(
            "The migrate cancelled 0 autovacuum passes on them meanwhile: this role may not cancel an autovacuum worker (permission denied to cancel query"
        ) && seen.contains(&format!(
            "Holding them now: autovacuum worker pid {} on {}: autovacuum: ",
            pass.pid, pass.partition
        )) && seen.ends_with(&format!(". {AUTOVACUUM_REMEDY}, and migrate again"))
            && !seen.contains("End those transactions"),
        "{seen}"
    );
    ensure!(
        logs.matches("PostgreSQL does not let this role cancel the autovacuum worker")
            .count()
            == 1
            && !logs.contains("cancelled autovacuum worker"),
        "{logs}"
    );
    ensure!(pass.running().await?, "the pass ended: {seen}");
    ensure!(
        !pass.db.versions().await?.contains(&27),
        "a refused migrate recorded 27"
    );
    Ok(())
}
