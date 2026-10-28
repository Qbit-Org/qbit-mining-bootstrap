//! The pre-migrate on a populated ledger: 3.1's `migrate` against a database
//! rc.6 migrated and filled, with appends running beside it as a frontend's
//! would, on one PostgreSQL 16 cluster that logs every statement.
//!
//! It answers two questions the cutover's pre-migrate depends on, with no
//! frontend or chain involved:
//!
//! * Does the migrate leave the share ledger's data partition (p0, which
//!   holds every row of a converted 3.0 ledger) as it found it: never
//!   rewritten, frozen, vacuumed, clustered or analyzed (qbit-tools #1345)?
//!   Its relfilenode, relfrozenxid and relminmxid must be unchanged, its
//!   vacuum and analyze counters too, no statement the cluster ran may be a
//!   VACUUM, ANALYZE, CLUSTER or REINDEX, and every index build that names
//!   p0 must be CREATE INDEX CONCURRENTLY. The cluster runs with autovacuum
//!   off, so a change can only be the migrate's.
//! * What it costs: the 027 and 028 transaction's lock hold as appends see
//!   it, and the 031 build's steps, read from the statement log.
//!
//! [`run`] is sized by [`Plan`]: a small ledger in CI, millions of rows for
//! the measurement (`tests/scenarios.rs`).

use crate::postgres::PgNode;
use crate::sim::{create_owner, DATABASE, OWNER_ROLE};
use anyhow::{ensure, Context, Result};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A spawned task that is aborted when dropped, so a step that fails
/// part-way never leaves it running: a child it waits on with
/// `kill_on_drop` is killed with it.
pub struct AbortOnDrop<T>(pub tokio::task::JoinHandle<T>);

impl<T> AbortOnDrop<T> {
    pub fn spawn<F>(future: F) -> Self
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Self(tokio::spawn(future))
    }

    /// Wait for the task's result.
    pub async fn join(mut self) -> Result<T> {
        Ok((&mut self.0).await?)
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// What one run builds and measures.
#[derive(Clone, Debug, Serialize)]
pub struct Plan {
    /// Synthetic share rows in p0 before the migrate; at most p0's bound,
    /// 2^24, so that every row is in p0, as on a converted 3.0 ledger.
    pub rows: u64,
    /// Rows per seeding transaction.
    pub batch: u64,
    /// Synthetic rows of `qbit_prism_audit_snapshots`, which 028 scans under
    /// its lock: a production-like few hundred.
    pub audit_snapshots: u64,
    /// PostgreSQL's `maintenance_work_mem` and
    /// `max_parallel_maintenance_workers` for the run, as the migrate's
    /// CREATE INDEX CONCURRENTLY uses them.
    pub maintenance_work_mem: String,
    pub maintenance_workers: u32,
    /// PostgreSQL's `shared_buffers` for the run.
    pub shared_buffers: String,
    /// The appender's pace: one append every this many milliseconds.
    pub append_every_ms: u64,
}

/// The savepoint the migrator applies its scratch copy of the schema under
/// (`ledger/migration.rs`), and rolls back.
const SCRATCH_SAVEPOINT: &str = "qbit_prism_release_schema";

/// One append as a frontend makes it: the header's mapping read first,
/// then the ledger row and the mapping, under the frontends' 5 s lock
/// timeout.
const APPEND: &str = "INSERT INTO qbit_share_ledger \
    (share_id, miner_id, payout_order_key, p2mr_program, share_difficulty, network_difficulty, \
     template_height, job_id, job_issued_at, ntime, writer_id, writer_epoch) \
    VALUES ($1, 'appender', 'appender', decode(repeat('ab', 32), 'hex'), 1, 1000, 1, 'appender-job', \
            clock_timestamp(), 1, 'appender', 1)";

/// The appender's tally.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Appends {
    pub count: u64,
    pub errors: Vec<String>,
    pub longest_ms: u64,
    /// Appends that took over 100 ms, each `(started_ms, took_ms)` on the
    /// run's clock: the waits the migrate caused.
    pub slow: Vec<(u64, u64)>,
}

/// A relation's identity on disk and its freeze horizon.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Storage {
    pub relation: String,
    pub relfilenode: i64,
    pub relfrozenxid: String,
    pub relminmxid: String,
}

/// A table's maintenance counters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Maintenance {
    pub relation: String,
    pub vacuum_count: i64,
    pub autovacuum_count: i64,
    pub analyze_count: i64,
    pub autoanalyze_count: i64,
}

/// One statement the cluster logged during the migrate.
#[derive(Clone, Debug, Serialize)]
pub struct Logged {
    pub at: String,
    pub pid: String,
    pub duration_ms: f64,
    pub sql: String,
}

/// What [`run`] found.
#[derive(Clone, Debug, Serialize)]
pub struct Outcome {
    pub plan: Plan,
    pub p0_heap_bytes: i64,
    pub p0_rows: i64,
    pub seed_ms: u64,
    pub migrate_ms: u64,
    pub migrate_stdout: String,
    pub migrate_success: bool,
    pub versions: Vec<i32>,
    pub index_valid: bool,
    pub p0_leaf_bytes: i64,
    pub storage_before: Vec<Storage>,
    pub storage_after: Vec<Storage>,
    pub maintenance_before: Vec<Maintenance>,
    pub maintenance_after: Vec<Maintenance>,
    /// Statements whose command is VACUUM, ANALYZE, CLUSTER or REINDEX.
    pub maintenance_statements: Vec<String>,
    /// Every index build naming p0, and whether each was CONCURRENTLY.
    pub p0_index_builds: Vec<(bool, String)>,
    /// From 027's statement ending (its last step takes the ledger's lock)
    /// to the commit: how long appends could wait on the migrate.
    pub ledger_lock_hold_ms: Option<f64>,
    /// From 027's start (its first step takes the first copied table's
    /// lock) to the commit: how long landings, job preparation and template
    /// writes could wait.
    pub transaction_ms: Option<f64>,
    /// The 027 and 028 files' own statement times.
    pub migration_027_ms: Option<f64>,
    pub migration_028_ms: Option<f64>,
    /// 031's build on p0, and its catalog steps.
    pub p0_build_ms: Option<f64>,
    pub parent_index_ms: Option<f64>,
    pub attach_ms: Vec<f64>,
    /// Temporary files the cluster logged (a spilled sort), in bytes.
    pub temp_file_bytes: u64,
    pub appends: Appends,
}

/// The statement log entries from `offset` on: `(timestamp, pid, text)`,
/// each entry with its continuation lines.
fn entries(log: &Path, offset: u64) -> Result<Vec<(String, String, String)>> {
    let bytes = std::fs::read(log).with_context(|| format!("reading {}", log.display()))?;
    let text =
        String::from_utf8_lossy(&bytes[offset.min(bytes.len() as u64) as usize..]).into_owned();
    let mut out: Vec<(String, String, String)> = Vec::new();
    for line in text.lines() {
        // `%m [%p] %a `: "2026-10-19 18:40:01.123 UTC [1234] app LOG:  ..."
        let starts = line.len() > 24
            && line.as_bytes()[4] == b'-'
            && line.as_bytes()[10] == b' '
            && line.contains(" [");
        if starts {
            let (stamp, rest) = line.split_at(line.find(" [").unwrap_or(0));
            let pid = rest
                .trim_start_matches(" [")
                .split(']')
                .next()
                .unwrap_or_default()
                .to_owned();
            out.push((stamp.to_owned(), pid, line.to_owned()));
        } else if let Some(last) = out.last_mut() {
            last.2.push('\n');
            last.2.push_str(line);
        }
    }
    Ok(out)
}

/// `duration: 12.345 ms  statement: ...` (simple protocol) or
/// `duration: 1.2 ms  execute <unnamed>: ...` (extended): the duration and
/// the SQL.
fn statement(entry: &str) -> Option<(f64, String)> {
    let rest = &entry[entry.find("duration: ")? + "duration: ".len()..];
    let (ms, rest) = rest.split_once(" ms")?;
    let ms: f64 = ms.trim().parse().ok()?;
    let sql = if let Some((_, sql)) = rest.split_once("statement: ") {
        sql
    } else if let Some((_, sql)) = rest.split_once("execute ") {
        sql.split_once(": ")?.1
    } else {
        return None;
    };
    Some((ms, sql.to_owned()))
}

/// The commands of `sql`: split on `;`, `--` comments removed, trimmed.
fn commands(sql: &str) -> Vec<String> {
    let stripped: String = sql
        .lines()
        .map(|line| line.split("--").next().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    stripped
        .split(';')
        .map(|command| command.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|command| !command.is_empty())
        .collect()
}

/// The commands of `sql` that are maintenance: VACUUM, ANALYZE, CLUSTER,
/// REINDEX.
fn maintenance_commands(sql: &str) -> Vec<String> {
    commands(sql)
        .into_iter()
        .filter(|command| {
            let word = command
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_ascii_uppercase();
            matches!(
                word.as_str(),
                "VACUUM" | "ANALYZE" | "ANALYSE" | "CLUSTER" | "REINDEX"
            )
        })
        .collect()
}

/// The commands of `sql` that build an index on `table`, each with whether
/// it is CONCURRENTLY: `CREATE [UNIQUE] INDEX [CONCURRENTLY] [[IF NOT
/// EXISTS] <name>] ON [ONLY] <target>`, schema-qualified or not, where the
/// target is `table`, or `parent` without ONLY (a build of a partitioned
/// table recurses into every partition, under a SHARE lock on each).
fn index_builds_on(sql: &str, table: &str, parent: &str) -> Vec<(bool, String)> {
    let named = |target: &str, name: &str| {
        target == name
            || target
                .rsplit_once('.')
                .is_some_and(|(_, last)| last == name)
    };
    commands(sql)
        .into_iter()
        .filter_map(|command| {
            let words: Vec<String> = command
                .split_whitespace()
                .map(|word| word.trim_matches('"').to_ascii_lowercase())
                .collect();
            let mut words = words.iter().map(String::as_str).peekable();
            let concurrent;
            {
                let mut take = |expected: &str| words.next_if_eq(&expected).is_some();
                if !take("create") {
                    return None;
                }
                take("unique");
                if !take("index") {
                    return None;
                }
                concurrent = take("concurrently");
                if take("if") && !(take("not") && take("exists")) {
                    return None;
                }
            }
            // The index's name is optional: then ON [ONLY] <target>.
            if words.next_if_eq(&"on").is_none() {
                words.next();
                words.next_if_eq(&"on")?;
            }
            let only = words.next_if_eq(&"only").is_some();
            let target = words.next()?;
            (named(target, table) || (!only && named(target, parent)))
                .then(|| (concurrent, command.clone()))
        })
        .collect()
}

/// Milliseconds between two `%m` timestamps of one log ("YYYY-MM-DD
/// HH:MM:SS.mmm TZ", one zone).
fn between(from: &str, to: &str) -> Option<f64> {
    fn at(stamp: &str) -> Option<chrono::NaiveDateTime> {
        let mut parts = stamp.split_whitespace();
        let text = format!("{} {}", parts.next()?, parts.next()?);
        chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f").ok()
    }
    Some((at(to)? - at(from)?).num_microseconds()? as f64 / 1000.0)
}

async fn storage(pool: &PgPool) -> Result<Vec<Storage>> {
    let rows: Vec<(String, i64, String, String)> = sqlx::query_as(
        "SELECT c.relname::text, c.relfilenode::bigint, c.relfrozenxid::text, c.relminmxid::text \
         FROM pg_class c \
         WHERE c.oid IN (SELECT inhrelid FROM pg_inherits \
                         WHERE inhparent = 'qbit_share_ledger'::regclass) \
            OR c.oid = (SELECT reltoastrelid FROM pg_class \
                        WHERE oid = 'qbit_share_ledger_p0'::regclass) \
         ORDER BY 1",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(relation, relfilenode, relfrozenxid, relminmxid)| Storage {
                relation,
                relfilenode,
                relfrozenxid,
                relminmxid,
            },
        )
        .collect())
}

async fn maintenance(pool: &PgPool) -> Result<Vec<Maintenance>> {
    // The counters are flushed by each backend at most once a second.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    sqlx::query("SELECT pg_stat_clear_snapshot()")
        .execute(pool)
        .await?;
    // pg_stat_all_tables, not _user_tables: p0's TOAST table is in pg_toast.
    let rows: Vec<(String, i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT relname::text, vacuum_count, autovacuum_count, analyze_count, autoanalyze_count \
         FROM pg_stat_all_tables \
         WHERE relid IN (SELECT inhrelid FROM pg_inherits \
                         WHERE inhparent = 'qbit_share_ledger'::regclass) \
            OR relid = (SELECT reltoastrelid FROM pg_class \
                        WHERE oid = 'qbit_share_ledger_p0'::regclass) \
         ORDER BY 1",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(relation, vacuum_count, autovacuum_count, analyze_count, autoanalyze_count)| {
                Maintenance {
                    relation,
                    vacuum_count,
                    autovacuum_count,
                    analyze_count,
                    autoanalyze_count,
                }
            },
        )
        .collect())
}

/// A command for `server` with exactly `url` as its database and nothing
/// inherited from this process's PRISM or qbit settings.
fn server_command(server: &Path, url: &str, args: &[&str]) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(server);
    for (name, _) in std::env::vars_os() {
        let name = name.to_string_lossy();
        if name.starts_with("PRISM_") || name.starts_with("QBIT_") {
            command.env_remove(name.as_ref());
        }
    }
    command
        .args(args)
        .env("PRISM_DATABASE_URL", url)
        .env("RUST_LOG", "info")
        .kill_on_drop(true);
    command
}

async fn seed(pool: &PgPool, plan: &Plan) -> Result<()> {
    let mut done = 0;
    while done < plan.rows {
        let count = plan.batch.min(plan.rows - done);
        // The share id leads with the row's number, so the unique index on it
        // grows at its right edge as the seed appends: random ids make every
        // batch beyond the cache a random write per row (hours at 15M rows),
        // and 031 never reads that index.
        // About 450 bytes a row, a production ledger's average: a share id
        // of `<job>:<extranonce>:<ntime>:<nonce>:<header hash>`, a miner,
        // a payout key and a job id of realistic lengths.
        sqlx::query(
            "INSERT INTO qbit_share_ledger \
               (share_id, miner_id, payout_order_key, p2mr_program, share_difficulty, \
                network_difficulty, template_height, job_id, job_issued_at, ntime, \
                writer_id, writer_epoch, accepted_at) \
             SELECT 'prism-a-' || lpad(to_hex(g), 16, '0') || md5(g::text) || ':6a1f2c3d:' \
                        || lpad(to_hex(g * 7), 8, '0') || ':' || md5(g::text || 'h') || md5(g::text || 'i'), \
                    'bc1p' || md5((g % 997)::text) || md5((g % 991)::text), \
                    'p2mr:' || md5((g % 997)::text) || md5((g % 991)::text) || ':0001', \
                    decode(md5((g % 997)::text) || md5((g % 991)::text), 'hex'), \
                    65536, 123456789012345, 100000 + g / 4000, \
                    'prism-a-' || md5((g / 30)::text), \
                    timestamptz '2026-01-01' + g * interval '880 ms', 1767225600 + g, \
                    'prism-coordinator-' || md5('w'), 7, \
                    timestamptz '2026-01-01' + g * interval '886 ms' \
             FROM generate_series($1::bigint, $2::bigint) g",
        )
        .bind(done as i64 + 1)
        .bind((done + count) as i64)
        .execute(pool)
        .await?;
        done += count;
    }
    for g in 0..plan.audit_snapshots {
        sqlx::query(
            "INSERT INTO qbit_prism_audit_snapshots \
               (snapshot_sha256, first_share_seq, last_share_seq, anchor_ms, share_count) \
             VALUES (encode(sha256(('snapshot' || $1)::bytea), 'hex'), $1 * 2000 + 1, \
                     $1 * 2000 + 2000, 1767225600000 + $1 * 600000, 2000)",
        )
        .bind(g as i64)
        .execute(pool)
        .await
        .context("seeding qbit_prism_audit_snapshots")?;
    }
    // A long-lived ledger has been vacuumed and its hint bits set before any
    // migrate reaches it; this is the run's setup, before what is measured.
    sqlx::query("VACUUM (ANALYZE) qbit_share_ledger_p0")
        .execute(pool)
        .await?;
    Ok(())
}

/// Append every `every` until `stop`, as a frontend's appends do.
async fn appender(url: String, every: Duration, stop: Arc<AtomicBool>, clock: Instant) -> Appends {
    let mut appends = Appends::default();
    let pool = match PgPoolOptions::new().max_connections(1).connect(&url).await {
        Ok(pool) => pool,
        Err(error) => {
            appends.errors.push(format!("connect: {error}"));
            return appends;
        }
    };
    let mut n = 0u64;
    while !stop.load(Ordering::SeqCst) {
        n += 1;
        let started = Instant::now();
        let at_ms = clock.elapsed().as_millis() as u64;
        let header = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let share_id = format!("appender:{n}:{header}");
        let outcome: Result<()> = async {
            let mut tx = pool.begin().await?;
            sqlx::query("SET LOCAL lock_timeout = '5s'")
                .execute(&mut *tx)
                .await?;
            sqlx::query("SELECT 1 FROM qbit_prism_share_hashes WHERE header_hash = $1")
                .bind(&header)
                .fetch_optional(&mut *tx)
                .await?;
            sqlx::query(APPEND)
                .bind(&share_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "INSERT INTO qbit_prism_share_hashes(header_hash, share_id) VALUES($1, $2)",
            )
            .bind(&header)
            .bind(&share_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        }
        .await;
        let took = started.elapsed().as_millis() as u64;
        appends.count += 1;
        appends.longest_ms = appends.longest_ms.max(took);
        if took > 100 {
            appends.slow.push((at_ms, took));
        }
        if let Err(error) = outcome {
            appends.errors.push(format!("{error:#}"));
        }
        tokio::time::sleep(every.saturating_sub(started.elapsed())).await;
    }
    pool.close().await;
    appends
}

/// Build a cluster under `work`, migrate it with rc.6's `rc6`, seed it per
/// `plan`, then run this workspace's `server` `migrate` with the appender
/// running, and report.
pub async fn run(
    pg_bin: &Path,
    rc6: &Path,
    server: &Path,
    work: &Path,
    plan: Plan,
) -> Result<Outcome> {
    if work.exists() {
        std::fs::remove_dir_all(work)?;
    }
    std::fs::create_dir_all(work)?;
    let log: PathBuf = work.join("postgres.log");
    let settings = vec![
        "log_min_duration_statement = 0".to_owned(),
        "log_temp_files = 0".to_owned(),
        "log_autovacuum_min_duration = 0".to_owned(),
        "autovacuum = off".to_owned(),
        format!("maintenance_work_mem = '{}'", plan.maintenance_work_mem),
        format!(
            "max_parallel_maintenance_workers = {}",
            plan.maintenance_workers
        ),
        "max_wal_size = '8GB'".to_owned(),
        format!("shared_buffers = '{}'", plan.shared_buffers),
    ];
    let node = PgNode::init("premigrate", pg_bin, &work.join("pg"), &log, &settings)?;
    create_owner(&node).await?;
    let url = node.url(OWNER_ROLE, DATABASE);

    let output = server_command(rc6, &url, &["migrate"]).output().await?;
    ensure!(
        output.status.success(),
        "rc.6's migrate failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .context("connecting as the owner")?;
    // Every row in p0, as on a converted 3.0 ledger: the seed and the
    // appends stay below p0's upper bound.
    let p0_upper: i64 = sqlx::query_scalar(
        "SELECT upper_seq FROM qbit_prism_share_partitions \
         WHERE partition_name = 'qbit_share_ledger_p0'",
    )
    .fetch_one(&pool)
    .await?;
    ensure!(
        (plan.rows as i64) + 100_000 < p0_upper,
        "{} rows would not all fit in p0, whose upper bound is {p0_upper}",
        plan.rows
    );
    let seeded = Instant::now();
    seed(&pool, &plan).await?;
    let seed_ms = seeded.elapsed().as_millis() as u64;
    // The seed's writes are flushed before anything is measured: a
    // checkpoint still syncing them stalls every commit, the migrate's and
    // the appender's, on fsync, which is the host's disk, not the migrate.
    let admin = node.admin_pool(DATABASE).await?;
    sqlx::query("CHECKPOINT").execute(&admin).await?;
    admin.close().await;
    let (p0_heap_bytes, p0_rows): (i64, i64) = sqlx::query_as(
        "SELECT pg_relation_size('qbit_share_ledger_p0'), \
                (SELECT count(*) FROM qbit_share_ledger_p0)",
    )
    .fetch_one(&pool)
    .await?;

    let storage_before = storage(&pool).await?;
    let maintenance_before = maintenance(&pool).await?;
    let offset = std::fs::metadata(&log)?.len();

    let clock = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let appending = AbortOnDrop::spawn(appender(
        url.clone(),
        Duration::from_millis(plan.append_every_ms),
        stop.clone(),
        clock,
    ));
    // Some appends before the migrate, as a baseline.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let migrating = Instant::now();
    let output = server_command(server, &url, &["migrate"]).output().await?;
    let migrate_ms = migrating.elapsed().as_millis() as u64;
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::SeqCst);
    let appends = appending.join().await.context("the appender")?;
    let migrate_stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    std::fs::write(
        work.join("migrate-3.1.log"),
        format!(
            "{}\n--- stdout\n{migrate_stdout}\n--- stderr\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ),
    )?;

    let versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(&pool)
            .await?;
    let (index_valid, p0_leaf_bytes): (Option<bool>, Option<i64>) = sqlx::query_as(
        "SELECT (SELECT indisvalid FROM pg_index \
                 WHERE indexrelid = to_regclass('qbit_share_ledger_origin_seq_idx')), \
                pg_relation_size(to_regclass('qbit_share_ledger_p0_origin_seq_idx'))",
    )
    .fetch_one(&pool)
    .await?;
    let storage_after = storage(&pool).await?;
    let maintenance_after = maintenance(&pool).await?;
    pool.close().await;

    let entries = entries(&log, offset)?;
    let mut maintenance_statements = Vec::new();
    let mut p0_index_builds = Vec::new();
    let mut statements: Vec<Logged> = Vec::new();
    let mut temp_file_bytes = 0u64;
    // The migrator applies every file to a scratch schema first, under a
    // savepoint it rolls back, before it applies them to the database:
    // what a backend runs between the two touches no live table.
    let mut scratch: std::collections::BTreeSet<String> = Default::default();
    for (at, pid, entry) in &entries {
        if let Some(size) = entry
            .split_once("temporary file: ")
            .and_then(|(_, rest)| rest.split_once("size "))
            .and_then(|(_, size)| size.split_whitespace().next()?.parse::<u64>().ok())
        {
            temp_file_bytes += size;
        }
        let Some((duration_ms, sql)) = statement(entry) else {
            continue;
        };
        // VACUUM and its kind cannot run in a transaction block at all,
        // so a scratch apply's are counted too.
        maintenance_statements.extend(maintenance_commands(&sql));
        let trimmed = sql.trim();
        if trimmed.starts_with(&format!("SAVEPOINT {SCRATCH_SAVEPOINT}")) {
            scratch.insert(pid.clone());
            continue;
        }
        if trimmed.starts_with(&format!("ROLLBACK TO SAVEPOINT {SCRATCH_SAVEPOINT}")) {
            scratch.remove(pid);
            continue;
        }
        if scratch.contains(pid) {
            continue;
        }
        p0_index_builds.extend(index_builds_on(
            &sql,
            "qbit_share_ledger_p0",
            "qbit_share_ledger",
        ));
        statements.push(Logged {
            at: at.clone(),
            pid: pid.clone(),
            duration_ms,
            sql,
        });
    }
    let find = |needle: &str| statements.iter().find(|s| s.sql.contains(needle));
    let migration_027 = find("-- 3.1 dual writer, node identity (D1)");
    let migration_028 = find("-- 3.1 dual writer, window cuts (D2)");
    // The commit of the transaction that applied them: the migrate
    // backend's first COMMIT after 028.
    let commit = migration_028.and_then(|m028| {
        statements
            .iter()
            .skip_while(|s| !std::ptr::eq(*s, m028))
            .find(|s| s.pid == m028.pid && s.sql.trim().eq_ignore_ascii_case("COMMIT"))
    });
    // A statement is logged as it ends. 027's first step takes the first
    // table's lock, its last the ledger's.
    let ledger_lock_hold_ms = migration_027
        .zip(commit)
        .and_then(|(m027, commit)| between(&m027.at, &commit.at));
    let transaction_ms = migration_027
        .zip(commit)
        .and_then(|(m027, commit)| Some(between(&m027.at, &commit.at)? + m027.duration_ms));
    let p0_build_ms = statements
        .iter()
        .filter(|s| {
            !index_builds_on(&s.sql, "qbit_share_ledger_p0", "qbit_share_ledger").is_empty()
        })
        .map(|s| s.duration_ms)
        .reduce(f64::max);
    let parent_index_ms = statements
        .iter()
        .filter(|s| {
            let upper = s.sql.to_ascii_uppercase();
            upper.contains("CREATE INDEX")
                && upper.contains(" ON ONLY ")
                && s.sql.contains("qbit_share_ledger_origin_seq_idx")
        })
        .map(|s| s.duration_ms)
        .next_back();
    let attach_ms = statements
        .iter()
        .filter(|s| {
            s.sql.to_ascii_uppercase().contains("ATTACH PARTITION")
                && s.sql.contains("_origin_seq_idx")
        })
        .map(|s| s.duration_ms)
        .collect();
    Ok(Outcome {
        plan,
        p0_heap_bytes,
        p0_rows,
        seed_ms,
        migrate_ms,
        migrate_stdout,
        migrate_success: output.status.success(),
        versions,
        index_valid: index_valid == Some(true),
        p0_leaf_bytes: p0_leaf_bytes.unwrap_or(0),
        storage_before,
        storage_after,
        maintenance_before,
        maintenance_after,
        maintenance_statements,
        p0_index_builds,
        ledger_lock_hold_ms,
        transaction_ms,
        migration_027_ms: migration_027.map(|s| s.duration_ms),
        migration_028_ms: migration_028.map(|s| s.duration_ms),
        p0_build_ms,
        parent_index_ms,
        attach_ms,
        temp_file_bytes,
        appends,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_commands_are_found_past_comments_and_in_any_case() {
        assert_eq!(
            maintenance_commands("-- EXPLAIN ANALYZE in a comment\nSELECT 1; vacuum (freeze) t;"),
            vec!["vacuum (freeze) t".to_owned()]
        );
        assert!(maintenance_commands("CREATE INDEX CONCURRENTLY i ON t (a)").is_empty());
        assert_eq!(maintenance_commands("ANALYZE t").len(), 1);
        assert_eq!(
            maintenance_commands("CLUSTER t USING i;REINDEX TABLE t").len(),
            2
        );
    }

    #[test]
    fn index_builds_are_those_on_the_table_and_its_parent_without_only() {
        let builds = |sql: &str| index_builds_on(sql, "qbit_share_ledger_p0", "qbit_share_ledger");
        assert_eq!(
            builds(
                "CREATE INDEX CONCURRENTLY qbit_share_ledger_p0_origin_seq_idx ON \
                 public.qbit_share_ledger_p0 USING btree (origin_node, share_seq)"
            ),
            vec![(
                true,
                "CREATE INDEX CONCURRENTLY qbit_share_ledger_p0_origin_seq_idx ON \
                 public.qbit_share_ledger_p0 USING btree (origin_node, share_seq)"
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            )]
        );
        assert!(!builds("create unique index if not exists i on qbit_share_ledger_p0 (a)")[0].0);
        // Unnamed, and a recursive build of the parent: both build on p0.
        assert_eq!(builds("CREATE INDEX ON qbit_share_ledger_p0 (a)").len(), 1);
        assert_eq!(builds("CREATE INDEX i ON qbit_share_ledger (a)").len(), 1);
        // ON ONLY the parent builds nothing on a partition.
        assert!(builds("CREATE INDEX i ON ONLY qbit_share_ledger (a)").is_empty());
        // A file naming p0 elsewhere builds nothing on it.
        assert!(builds(
            "-- CREATE INDEX on qbit_share_ledger_p0 later\n\
             CREATE INDEX i ON qbit_prism_jobs (a); ALTER TABLE qbit_share_ledger_p0 ADD x int"
        )
        .is_empty());
        assert!(builds("CREATE INDEX i ON qbit_share_ledger_p01 (a)").is_empty());
    }

    #[test]
    fn statements_are_read_from_both_protocols() {
        let simple =
            "2026-10-19 18:40:01.123 UTC [12] app LOG:  duration: 12.5 ms  statement: COMMIT";
        assert_eq!(statement(simple), Some((12.5, "COMMIT".to_owned())));
        let extended =
            "2026-10-19 18:40:01.123 UTC [12] app LOG:  duration: 0.2 ms  execute sqlx_s_1: SELECT 1";
        assert_eq!(statement(extended), Some((0.2, "SELECT 1".to_owned())));
        assert_eq!(
            statement("2026-10-19 18:40:01.123 UTC [12] app LOG:  checkpoint"),
            None
        );
        assert_eq!(
            between("2026-10-19 18:40:01.123 UTC", "2026-10-19 18:40:02.500 UTC"),
            Some(1377.0)
        );
        assert_eq!(
            between("2026-10-19 23:59:59.900 UTC", "2026-10-20 00:00:00.100 UTC"),
            Some(200.0)
        );
    }
}
