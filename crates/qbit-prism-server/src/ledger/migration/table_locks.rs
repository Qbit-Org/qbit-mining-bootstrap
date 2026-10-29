//! The table locks of 027 and 028, taken all at once before either file
//! runs, the migration history's lock taken the same way, and the migration
//! transaction tried again when either cannot be (#789).
//!
//! Both files are applied in the migration transaction while earlier
//! frontends may still serve the database: the pre-migrate runs 3.1's
//! `migrate` beside rc.6 frontends (`r01-rc6-premigrate`). Each of their
//! `ALTER TABLE`s takes its table's ACCESS EXCLUSIVE lock and keeps it until
//! the commit, so run as written they hold every table already altered
//! while they wait for the next. A frontend transaction that has read the
//! table being waited for and then reads one already altered closes a
//! cycle, which PostgreSQL breaks only after `deadlock_timeout` (1 s) by
//! aborting one side: the migrate in #789's first run, an rc.6 CTV
//! broadcaster's statement in its second, where rc.6's metrics collector
//! also gave up on its 500 ms lock timeout behind a table the migrate held.
//!
//! So `migrate_schema` first takes the lock of every table the pending
//! migrations of `ALTERED` alter, in one `LOCK TABLE` whose lock timeout and
//! statement timeout are both `ATTEMPT`: the lock timeout bounds each
//! table's wait, the statement timeout their sum. The statement takes the
//! tables in order and keeps those it has while it waits for the next, but
//! for `ATTEMPT` at most in all. The migration history's
//! ACCESS EXCLUSIVE lock, the first table lock of every migration
//! transaction, which a starting frontend's read queues behind, is taken
//! with the same bound. If either cannot be taken, or PostgreSQL picks the
//! request as a deadlock victim (possible where `deadlock_timeout` is below
//! `ATTEMPT`, as the migrate's own is where it may lower it, below), the
//! attempt is given up (`LockContention`), and
//! `migrate_schema_committed` rolls the whole migration transaction back,
//! which releases every lock it took (each is transaction-scoped,
//! MIGRATION_LOCK included), and runs it again from the start after a
//! backoff, until `BUDGET` has passed. Nothing is recorded before the
//! commit, and 027 and 028 are `IF NOT EXISTS` throughout, so each attempt
//! plans afresh from what it reads and converges on the same schema, also
//! when another migrator committed them meanwhile. Once it holds the
//! tables, the files wait for no other lock a frontend can hold
//! (`postgres_tests`): catalog changes, 027's three small index builds and
//! 028's check over the audit snapshots, then the commit.
//!
//! `ATTEMPT` is 100 ms, well under PostgreSQL's 1 s `deadlock_timeout` and
//! the 500 ms lock timeout of rc.6's metrics collector. A frontend waits for
//! the migrate's request at most that long, and the migrate leaves any cycle
//! it closes within it. PostgreSQL checks a waiter for a deadlock only once
//! it has waited a second, so no frontend is checked, let alone aborted, for
//! a wait on the migrate, and none reaches the collector's timeout behind
//! it: the migrate always gives up first. All of this holds only while
//! `ATTEMPT` stays well under `deadlock_timeout` and every frontend lock
//! timeout, and only on the primary: a hot standby replays the ACCESS
//! EXCLUSIVE locks an attempt took before it gave up, and a reader there
//! waits for that replay for as long as `max_standby_streaming_delay`
//! lets it (`docs/prism-rust-migration.md`).
//!
//! Autovacuum holds SHARE UPDATE EXCLUSIVE on the table it processes, a
//! share ledger partition included, which every attempt's request conflicts
//! with. PostgreSQL cancels an autovacuum worker that blocks a lock request,
//! unless the worker prevents wraparound, from the waiter's own deadlock
//! check, which runs once the waiter has waited its `deadlock_timeout`:
//! before #791, 027's `ALTER TABLE` waited that second and PostgreSQL
//! cancelled the pass, but no attempt waits that long. So each attempt's
//! transaction lowers its own `deadlock_timeout` to `DEADLOCK_CHECK`, half of
//! `ATTEMPT`: the check then runs within the attempt and cancels the worker
//! blocking the request it waits on, whose lock is then usually granted
//! within the attempt, or else in the next. PostgreSQL itself picks that
//! worker, the one directly blocking the request, and tells an
//! anti-wraparound one by its flags, with no permission check and whatever
//! this role can see. The setting is the migrate's own: every frontend keeps
//! its `deadlock_timeout`, so the frontend waits above stay as they are, and
//! no frontend is checked, let alone aborted, for a wait on the migrate.
//! Where the early check finds a real deadlock, PostgreSQL aborts the
//! migrate's request, which is contention as a timeout is (`gave_up`). With
//! `log_lock_waits` on, each attempt that waits past `DEADLOCK_CHECK` logs
//! the wait.
//!
//! Only a superuser may lower `deadlock_timeout`, or from PostgreSQL 15 a
//! role granted `SET` on it, which `committed_within` reads once per run,
//! so no attempt fails on the `SET`. A role without either waits a blocking
//! pass out as before, warned once, and its refusal names the worker and
//! the remedies: the grant, or an administrator's `pg_cancel_backend`
//! (a superuser's, since CVE-2023-5870's fix in 16.1, 15.5 and 14.10, and
//! from PostgreSQL 18 a member of `pg_signal_autovacuum_worker`'s too), or
//! autovacuum turned off on the table until the migrate is done.
//!
//! Every connect that migrates goes through `migrate_schema_committed`, a
//! frontend's start with `PRISM_POSTGRES_INIT_SCHEMA=1` included, also on a
//! fully migrated database, since each takes the history's lock: a start
//! that cannot take it retries for up to `BUDGET`, where it used to fail
//! after the session's lock timeout.
//!
//! The price: the share ledger and `qbit_prism_share_hashes`, which 027
//! alters last, are now held from the start of 027 to the commit, through
//! its non-concurrent index builds on `qbit_pool_blocks` and
//! `qbit_prism_jobs` and 028's check. `qbit_pool_blocks` was held through
//! those builds in either design; it has a row per landed block, and the
//! expiry prune keeps `qbit_prism_jobs` to its unexpired jobs, so both
//! builds are small. In two runs of the R1 scenario 027 and 028 committed
//! 372 and 558 ms after the migrate started, and no share sent meanwhile
//! waited over 39 ms for its answer.
//!
//! A later migration applied in the transaction that alters an existing
//! table must be listed in `ALTERED`, or exempted in `EXEMPT` with the
//! reason; `every_plain_migration_after_026_that_alters_a_table_locks_it_up_front`
//! fails otherwise.
use super::*;
use crate::ledger::connect::begin;
use crate::metrics::Metrics;

/// How long one attempt waits for all of its tables together (see the
/// module docs).
pub(super) const ATTEMPT: Duration = Duration::from_millis(100);
/// How long `migrate` keeps trying, as 017's swap and 031's catalog steps
/// keep trying for their own locks (`partition.rs`).
const BUDGET: Duration = Duration::from_secs(600);
/// The first backoff, doubling after each attempt to at most `BACKOFF_MAX`.
const BACKOFF: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(2);
/// A retry is logged as a warning on the first attempt and every
/// `WARN_EVERY` attempts, and at debug otherwise: each attempt runs the
/// whole migration transaction again.
const WARN_EVERY: u32 = 10;
/// How many holders a refusal names.
const HOLDERS_NAMED: i64 = 5;
/// The migration history, locked before anything else is read.
const HISTORY: &str = "qbit_prism_schema_migrations";
/// How long an attempt's requests wait before PostgreSQL checks them for a
/// deadlock, and cancels an autovacuum worker blocking one, where the role
/// may lower its `deadlock_timeout`: well within `ATTEMPT` (see the module
/// docs).
const DEADLOCK_CHECK: Duration = Duration::from_millis(50);
/// How an anti-wraparound autovacuum's query ends: PostgreSQL does not
/// cancel one for a lock request.
const ANTI_WRAPAROUND: &str = " (to prevent wraparound)";
/// What lets PostgreSQL cancel an autovacuum worker within an attempt, in a
/// refusal that names one where the role could not lower `deadlock_timeout`.
const LOWER_REMEDY: &str = "This role may not lower its deadlock_timeout, so PostgreSQL could not cancel an autovacuum pass within an attempt: grant it SET ON PARAMETER deadlock_timeout (PostgreSQL 15 and later), or migrate as a superuser";
/// What ends an autovacuum worker's hold, in a refusal that names one.
const AUTOVACUUM_REMEDY: &str = "Stopping the frontends does not stop an autovacuum worker: an administrator can end its pass with SELECT pg_cancel_backend(<pid>) as a superuser (from PostgreSQL 18 also as a member of pg_signal_autovacuum_worker), or keep autovacuum off its table with ALTER TABLE <table> SET (autovacuum_enabled = off) until the migrate is done and then RESET (autovacuum_enabled), but should let an anti-wraparound vacuum finish";

/// The tables each migration applied in the transaction while frontends
/// may serve alters, in its file's order. Exactly the targets of the file's
/// `ALTER TABLE`, `CREATE INDEX` and trigger statements that it does not
/// create itself (`the_up_front_locks_are_exactly_the_tables_each_file_alters`).
/// `LOCK TABLE` takes a partitioned table's partitions too, as `ALTER TABLE`
/// does.
const ALTERED: &[(i32, &[&str])] = &[
    (
        27,
        &[
            "qbit_pool_blocks",
            "qbit_prism_audit_snapshots",
            "qbit_pool_audit_bundles",
            "qbit_pool_payout_entries",
            "qbit_payout_carry_forward",
            "qbit_ctv_fanout_sets",
            "qbit_ctv_fanout_artifacts",
            "qbit_prism_templates",
            "qbit_prism_balance_snapshots",
            "qbit_prism_jobs",
            "qbit_prism_share_hashes",
            "qbit_share_ledger",
        ],
    ),
    (28, &["qbit_prism_audit_snapshots"]),
];

/// Migrations after 026 applied in the transaction that alter an existing
/// table without an up-front lock, each with the reason no frontend can
/// hold that table while it runs, read by
/// `every_plain_migration_after_026_that_alters_a_table_locks_it_up_front`.
/// None.
#[cfg(test)]
const EXEMPT: &[(i32, &str)] = &[];

/// The versions of `ALTERED` missing from `versions`, and every table they
/// alter, once each, in the order the files alter them.
fn pending(versions: &[i32]) -> (Vec<i32>, Vec<&'static str>) {
    let mut pending = Vec::new();
    let mut tables: Vec<&'static str> = Vec::new();
    for (version, altered) in ALTERED {
        if versions.contains(version) {
            continue;
        }
        pending.push(*version);
        for table in *altered {
            if !tables.contains(table) {
                tables.push(table);
            }
        }
    }
    (pending, tables)
}

/// What an attempt was locking when it gave up.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Locking {
    /// The migration history, before anything is read.
    History,
    /// The tables of these migrations of `ALTERED`, before they run.
    Migrations(Vec<i32>),
}

impl Locking {
    fn subject(&self) -> String {
        match self {
            Self::History => "the migration history".to_owned(),
            Self::Migrations(versions) => format!("migration {}", schema_version_list(versions)),
        }
    }

    /// What a refused run left unrecorded, after "Nothing was changed".
    fn unrecorded(&self) -> &'static str {
        match self {
            Self::History => "",
            Self::Migrations(versions) if versions.len() == 1 => ": it is not recorded",
            Self::Migrations(_) => ": they are not recorded",
        }
    }
}

/// An attempt that could not take a lock within `ATTEMPT`, or whose request
/// PostgreSQL aborted as a deadlock. Its transaction is aborted and still
/// holds every lock it took, until `migrate_schema_committed` rolls it back.
#[derive(Debug)]
pub(super) struct LockContention {
    locking: Locking,
    tables: Vec<&'static str>,
    /// PostgreSQL's message: a lock or statement timeout, or the deadlock.
    reason: String,
}

impl std::fmt::Display for LockContention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} could not lock {} within {} ms ({})",
            self.locking.subject(),
            self.tables.join(", "),
            ATTEMPT.as_millis(),
            self.reason
        )
    }
}

impl std::error::Error for LockContention {}

/// PostgreSQL's message when `error` gave the request up: its lock timeout,
/// its statement timeout (an operator's cancel carries the same SQLSTATE
/// and another message, and stops the run), or a deadlock PostgreSQL broke
/// by aborting it.
fn gave_up(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .filter(|error| {
            matches!(error.code().as_deref(), Some("55P03") | Some("40P01"))
                || statement_timed_out(*error)
        })
        .map(|error| error.message().to_owned())
}

/// Take `statement`'s locks on `tables` within `ATTEMPT` in all, under the
/// transaction's own timeouts again afterwards. Giving up is
/// `LockContention`; any other failure is returned with `context`.
async fn lock_within_attempt(
    tx: &mut Transaction<'_, Postgres>,
    statement: &str,
    locking: Locking,
    tables: Vec<&'static str>,
    context: String,
) -> Result<()> {
    let (lock_timeout, statement_timeout): (String, String) = sqlx::query_as(
        "SELECT current_setting('lock_timeout'),current_setting('statement_timeout')",
    )
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "SELECT set_config('lock_timeout',$1,true),set_config('statement_timeout',$1,true)",
    )
    .bind(ATTEMPT.as_millis().to_string())
    .execute(&mut **tx)
    .await?;
    if let Err(error) = sqlx::query(statement).execute(&mut **tx).await {
        return Err(match gave_up(&error) {
            Some(reason) => LockContention {
                locking,
                tables,
                reason,
            }
            .into(),
            None => anyhow::Error::from(error).context(context),
        });
    }
    sqlx::query(
        "SELECT set_config('lock_timeout',$1,true),set_config('statement_timeout',$2,true)",
    )
    .bind(lock_timeout)
    .bind(statement_timeout)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Lock the migration history ACCESS EXCLUSIVE, the first table lock of the
/// migration transaction, within `ATTEMPT`: a starting frontend's read of
/// the history waits behind the request at most that long.
pub(super) async fn lock_history(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    lock_within_attempt(
        tx,
        &format!("LOCK TABLE ONLY {HISTORY} IN ACCESS EXCLUSIVE MODE"),
        Locking::History,
        vec![HISTORY],
        format!("refusing {HISTORY} before any DDL: cannot lock the migration-history table for validation"),
    )
    .await
}

/// Lock every table the pending migrations of `ALTERED` alter, before the
/// first of them runs; nothing when all are recorded.
pub(super) async fn lock_altered_tables(
    tx: &mut Transaction<'_, Postgres>,
    versions: &[i32],
) -> Result<()> {
    let (pending, tables) = pending(versions);
    if tables.is_empty() {
        return Ok(());
    }
    let statement = format!("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE", tables.join(", "));
    let context = format!(
        "locking the tables of migration {} before it runs",
        schema_version_list(&pending)
    );
    lock_within_attempt(
        tx,
        &statement,
        Locking::Migrations(pending),
        tables,
        context,
    )
    .await
}

/// `migrate_schema` in a transaction of its own, committed. An attempt
/// that could not take the migration history's lock or the tables of
/// `ALTERED` is rolled back, holding nothing through the backoff, and the
/// whole transaction runs again from the start, until `BUDGET` has passed;
/// then the refusal names who holds the tables. Each attempt lowers its own
/// `deadlock_timeout` where the role may (see the module docs). Any other
/// failure is returned as it was, its transaction rolled back.
pub(in crate::ledger) async fn migrate_schema_committed(
    pool: &PgPool,
    instance_id: &str,
    backfill_mode: ShareHashBackfill,
    metrics: Option<&Metrics>,
) -> Result<Vec<OnlineMigration>> {
    committed_within(pool, instance_id, backfill_mode, metrics, BUDGET).await
}

async fn committed_within(
    pool: &PgPool,
    instance_id: &str,
    backfill_mode: ShareHashBackfill,
    metrics: Option<&Metrics>,
    budget: Duration,
) -> Result<Vec<OnlineMigration>> {
    let started = tokio::time::Instant::now();
    let deadline = started + budget;
    let mut backoff = BACKOFF;
    let mut attempt: u32 = 1;
    // Whether this run's role may lower its `deadlock_timeout`, read once,
    // in the first attempt, so that no attempt fails on the `SET`.
    let mut lowers = None;
    loop {
        let mut tx = begin(pool, metrics).await?;
        let lower = match lowers {
            Some(lower) => lower,
            None => *lowers.insert(may_lower_deadlock_timeout(&mut tx).await?),
        };
        if lower {
            // Its own only: every frontend keeps its `deadlock_timeout`.
            sqlx::query("SELECT set_config('deadlock_timeout',$1,true)")
                .bind(DEADLOCK_CHECK.as_millis().to_string())
                .execute(&mut *tx)
                .await?;
        }
        let error = match migrate_schema(&mut tx, instance_id, backfill_mode, metrics).await {
            Ok(online) => {
                tx.commit().await?;
                return Ok(online);
            }
            Err(error) => error,
        };
        let contention = match error.downcast::<LockContention>() {
            Ok(contention) => contention,
            // Dropped, the transaction queues its ROLLBACK, which the
            // connection's return to the pool sends.
            Err(error) => return Err(error),
        };
        // An aborted transaction keeps its locks until it ends, so it ends
        // here, before the backoff, not when the connection is next used.
        tx.rollback().await.with_context(|| {
            format!("rolling back the attempt that could not lock its tables: {contention}")
        })?;
        let LockContention {
            locking,
            tables,
            reason,
        } = contention;
        // Once per run, at its first contention, refused or not.
        if attempt == 1 && !lower {
            tracing::warn!(
                "this role may not lower its deadlock_timeout, so PostgreSQL cannot cancel an autovacuum pass that blocks the migration's locks within an attempt, and such a pass holds every attempt off until it ends. Grant the role SET ON PARAMETER deadlock_timeout (PostgreSQL 15 and later), or migrate as a superuser"
            );
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            let held = refusal_tail(&holders(pool, &tables).await, lower);
            bail!(
                "refusing to wait any longer for the table locks of {}: {attempt} attempts over {} s each waited {} ms for {} and gave up, so that no serving frontend waits on the migrate longer than that. Nothing was changed{}.{held}",
                locking.subject(),
                started.elapsed().as_secs(),
                ATTEMPT.as_millis(),
                tables.join(", "),
                locking.unrecorded()
            );
        }
        let backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX);
        let waited_s = started.elapsed().as_secs();
        if attempt == 1 || attempt.is_multiple_of(WARN_EVERY) {
            tracing::warn!(
                locking = %locking.subject(),
                attempt,
                waited_s,
                backoff_ms,
                %reason,
                "the migration could not lock the tables it alters within {} ms; rolled back, holding nothing, and trying again after a backoff",
                ATTEMPT.as_millis()
            );
        } else {
            tracing::debug!(
                locking = %locking.subject(),
                attempt,
                waited_s,
                backoff_ms,
                %reason,
                "the migration could not lock its tables; trying again"
            );
        }
        tokio::time::sleep(backoff.min(deadline - now)).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
        attempt += 1;
    }
}

/// Whether this transaction's role may lower its `deadlock_timeout`, which
/// only a superuser may set, or from PostgreSQL 15 a role granted `SET` on
/// it.
async fn may_lower_deadlock_timeout(
    connection: &mut sqlx::PgConnection,
) -> Result<bool, sqlx::Error> {
    let (version, superuser): (i32, bool) = sqlx::query_as(
        "SELECT current_setting('server_version_num')::int,current_setting('is_superuser')='on'",
    )
    .fetch_one(&mut *connection)
    .await?;
    if superuser || version < 150_000 {
        return Ok(superuser);
    }
    sqlx::query_scalar("SELECT has_parameter_privilege(current_user,'deadlock_timeout','SET')")
        .fetch_one(&mut *connection)
        .await
}

/// A backend holding a lock on the tables an attempt could not take, as a
/// refusal names it.
#[derive(Clone, Debug, PartialEq)]
struct Holder {
    pid: i32,
    /// An autovacuum worker: by `backend_type`, or, where this role sees no
    /// more, a vacuum or analyze in progress under no role.
    autovacuum: bool,
    /// Its state and how long its transaction has been open, `None` where
    /// `pg_stat_activity` hides them from this role.
    activity: Option<(String, f64)>,
    /// The tables it holds, a partition's lock counted as its table's, in
    /// the attempt's order.
    tables: String,
    /// The relations it holds themselves, a partition by its own name.
    relations: String,
    /// Its query, `None` where hidden from this role.
    query: Option<String>,
}

impl std::fmt::Display for Holder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pid = self.pid;
        if self.autovacuum {
            let relations = &self.relations;
            return match &self.query {
                None => write!(
                    f,
                    "autovacuum worker pid {pid} on {relations}, its query hidden from this role, so whether it is an anti-wraparound vacuum is unknown"
                ),
                Some(query) if query.contains(ANTI_WRAPAROUND) => write!(
                    f,
                    "autovacuum worker pid {pid} on {relations}, an anti-wraparound vacuum, which PostgreSQL does not cancel for a lock request: {query}"
                ),
                Some(query) => write!(f, "autovacuum worker pid {pid} on {relations}: {query}"),
            };
        }
        match &self.activity {
            Some((state, age)) => {
                write!(f, "pid {pid} ({state}, its transaction open {age:.0} s)")?
            }
            None => write!(f, "pid {pid} (its activity hidden from this role)")?,
        }
        write!(f, " holding {}", self.tables)?;
        match &self.query {
            Some(query) => write!(f, ": {query}"),
            None => Ok(()),
        }
    }
}

/// A `Holder` as `holders` reads it: the pid, whether it is an autovacuum
/// worker, whether its activity is visible to this role, its state and
/// age, its tables and relations, and its query.
type HolderRow = (i32, bool, bool, String, f64, String, String, String);

/// The backends that hold a lock on `tables` or their partitions now, one
/// each, at most `HOLDERS_NAMED`: autovacuum workers first, since stopping
/// the frontends does not stop one, then the longest open. The partitions
/// come from `pg_inherits`, which locks none of them, as
/// `pg_partition_tree` would.
async fn holders(pool: &PgPool, tables: &[&str]) -> Result<Vec<Holder>> {
    let rows: Vec<HolderRow> = sqlx::query_as(
        "WITH RECURSIVE tree(name,at,relid) AS (SELECT t.name,t.at,t.name::regclass::oid FROM unnest($1::text[]) WITH ORDINALITY t(name,at) \
                         UNION SELECT tree.name,tree.at,i.inhrelid FROM tree JOIN pg_inherits i ON i.inhparent=tree.relid), \
              held AS (SELECT tree.name,tree.at,l.pid,l.relation FROM pg_locks l JOIN tree ON tree.relid=l.relation \
                       WHERE l.locktype='relation' AND l.granted AND l.pid<>pg_backend_pid() \
                         AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())) \
         SELECT pid,autovacuum,visible,state,age,tables,relations,query FROM \
             (SELECT a.pid,a.xact_start, \
                     COALESCE(a.backend_type='autovacuum worker',a.usesysid IS NULL \
                         AND (a.pid IN (SELECT pid FROM pg_stat_progress_vacuum) OR a.pid IN (SELECT pid FROM pg_stat_progress_analyze))) AS autovacuum, \
                     a.backend_type IS NOT NULL AS visible,COALESCE(a.state,'') AS state, \
                     COALESCE(extract(epoch FROM clock_timestamp()-a.xact_start),0)::float8 AS age, \
                     COALESCE((SELECT string_agg(t.name,', ' ORDER BY t.at) FROM (SELECT DISTINCT name,at FROM held WHERE held.pid=a.pid) t),'') AS tables, \
                     COALESCE((SELECT string_agg(DISTINCT held.relation::regclass::text,', ') FROM held WHERE held.pid=a.pid),'') AS relations, \
                     left(COALESCE(a.query,''),200) AS query \
              FROM pg_stat_activity a WHERE a.pid IN (SELECT pid FROM held)) h \
         ORDER BY autovacuum DESC,xact_start NULLS LAST,pid LIMIT $2",
    )
    .bind(tables)
    .bind(HOLDERS_NAMED)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(pid, autovacuum, visible, state, age, tables, relations, query)| Holder {
                pid,
                autovacuum,
                activity: visible.then_some((state, age)),
                tables,
                relations,
                query: visible.then_some(query),
            },
        )
        .collect())
}

/// The end of a refusal, after "Nothing was changed": who holds the tables
/// now and what ends each hold, with, for an autovacuum worker where this
/// run's role could not lower `deadlock_timeout` (`lowered`), what lets
/// PostgreSQL cancel it.
fn refusal_tail(held: &Result<Vec<Holder>>, lowered: bool) -> String {
    match held {
        Ok(held) if held.is_empty() => " No transaction holds them now; migrate again".to_owned(),
        Ok(held) => {
            let mut ends = Vec::new();
            if held.iter().any(|holder| !holder.autovacuum) {
                ends.push("End those transactions, or stop the frontends");
            }
            if held.iter().any(|holder| holder.autovacuum) {
                if !lowered {
                    ends.push(LOWER_REMEDY);
                }
                ends.push(AUTOVACUUM_REMEDY);
            }
            let held: Vec<String> = held.iter().map(Holder::to_string).collect();
            format!(
                " Holding them now: {}. {}, and migrate again",
                held.join("; "),
                ends.join(". ")
            )
        }
        Err(read) => format!(
            " Who holds them could not be read ({read:#}). Look in pg_locks and pg_stat_activity for transactions or autovacuum workers on those tables, end them, or stop the frontends, and migrate again"
        ),
    }
}

#[cfg(test)]
mod postgres_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// What each statement of `sql` that locks a table it names locks, and
    /// what its `CREATE TABLE`s create, from the embedded file: `ALTER
    /// TABLE`, `CREATE [UNIQUE] INDEX ... ON` and `CREATE`/`DROP TRIGGER ...
    /// ON`, outside comments, keywords in any case.
    fn named_tables(sql: &str) -> (Vec<String>, Vec<String>) {
        let words: Vec<String> = sql
            .lines()
            .map(|line| line.split("--").next().unwrap_or_default())
            .flat_map(|line| {
                line.split(|c: char| c.is_whitespace() || "(),;".contains(c))
                    .filter(|word| !word.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .collect();
        let is =
            |at: usize, word: &str| words.get(at).is_some_and(|w| w.eq_ignore_ascii_case(word));
        let skip = |mut at: usize, optional: &[&str]| {
            while optional.iter().any(|word| is(at, word)) {
                at += 1;
            }
            at
        };
        let (mut locked, mut created) = (Vec::new(), Vec::new());
        for at in 0..words.len() {
            let target = if is(at, "ALTER") && is(at + 1, "TABLE") {
                Some(skip(at + 2, &["IF", "EXISTS", "ONLY"]))
            } else if (is(at, "CREATE") && (is(at + 1, "INDEX") || is(at + 2, "INDEX")))
                || ((is(at, "CREATE") || is(at, "DROP")) && is(at + 1, "TRIGGER"))
            {
                (at..words.len())
                    .find(|on| is(*on, "ON"))
                    .map(|on| skip(on + 1, &["ONLY"]))
            } else {
                if is(at, "CREATE") && is(at + 1, "TABLE") {
                    created.push(words[skip(at + 2, &["IF", "NOT", "EXISTS"])].clone());
                }
                None
            };
            if let Some(name) = target.and_then(|target| words.get(target)) {
                if !locked.contains(name) {
                    locked.push(name.clone());
                }
            }
        }
        (locked, created)
    }

    /// The up-front lock of each migration is exactly the existing tables
    /// its file alters, indexes or triggers, in the file's order, so the
    /// list cannot drift from the file, and nothing else a frontend uses is
    /// locked for it.
    #[test]
    fn the_up_front_locks_are_exactly_the_tables_each_file_alters() {
        for (version, altered) in ALTERED {
            let (locked, created) = named_tables(native_migration(*version));
            let existing: Vec<&str> = locked
                .iter()
                .map(String::as_str)
                .filter(|table| !created.iter().any(|new| new == table))
                .collect();
            assert_eq!(existing, *altered, "migration {version}");
        }
    }

    /// Every migration after 026 applied in the transaction, where an
    /// earlier frontend may still serve, that alters, indexes or triggers a
    /// table it does not create is in `ALTERED` or `EXEMPT`, so a new one
    /// cannot run without the up-front lock unnoticed.
    #[test]
    fn every_plain_migration_after_026_that_alters_a_table_locks_it_up_front() {
        for (version, sql) in NATIVE_MIGRATIONS {
            if *version <= 26 || ONLINE_MIGRATIONS.contains(version) {
                continue;
            }
            let (locked, created) = named_tables(sql);
            let existing: Vec<&String> = locked
                .iter()
                .filter(|table| !created.contains(table))
                .collect();
            let listed = ALTERED.iter().any(|(altered, _)| altered == version);
            let exempt = EXEMPT.iter().any(|(exempted, _)| exempted == version);
            assert!(
                existing.is_empty() || listed || exempt,
                "migration {version} alters {existing:?}: list it in ALTERED, or in EXEMPT with the reason"
            );
            assert!(
                !(listed && exempt),
                "migration {version} is both listed and exempt"
            );
        }
        // The ones listed are plain migrations after 026.
        for (version, _) in ALTERED {
            assert!(*version > 26 && !ONLINE_MIGRATIONS.contains(version));
            assert!(NATIVE_MIGRATIONS
                .iter()
                .any(|(native, _)| native == version));
        }
    }

    #[test]
    fn only_the_pending_migrations_tables_are_locked_once_each_in_file_order() {
        let (versions, tables) = pending(&[26]);
        assert_eq!(versions, [27, 28]);
        assert_eq!(tables.len(), 12, "{tables:?}");
        assert_eq!(tables.first(), Some(&"qbit_pool_blocks"));
        assert_eq!(tables.last(), Some(&"qbit_share_ledger"));
        assert_eq!(
            pending(&[27]),
            (vec![28], vec!["qbit_prism_audit_snapshots"])
        );
        assert_eq!(pending(&[27, 28]), (Vec::new(), Vec::new()));
    }

    #[test]
    fn the_attempt_gives_up_well_before_a_deadlock_check_or_the_collector() {
        // PostgreSQL's default deadlock_timeout, and the lock timeout of
        // rc.6's metrics collector (`metrics/collectors.rs`).
        assert!(ATTEMPT * 5 <= Duration::from_millis(500));
        assert!(BACKOFF_MAX < BUDGET);
        // The migrate's own deadlock check, and its cancel of a blocking
        // autovacuum worker, run well within an attempt.
        assert!(DEADLOCK_CHECK * 2 <= ATTEMPT);
    }

    fn worker(query: Option<&str>) -> Holder {
        Holder {
            pid: 4242,
            autovacuum: true,
            activity: query.map(|_| ("active".to_owned(), 75.4)),
            tables: "qbit_share_ledger".to_owned(),
            relations: "qbit_share_ledger_p0".to_owned(),
            query: query.map(str::to_owned),
        }
    }

    fn client(visible: bool) -> Holder {
        Holder {
            pid: 77,
            autovacuum: false,
            activity: visible.then(|| ("idle in transaction".to_owned(), 12.6)),
            tables: "qbit_prism_templates, qbit_share_ledger".to_owned(),
            relations: "qbit_prism_templates, qbit_share_ledger, qbit_share_ledger_p0".to_owned(),
            query: visible.then(|| "SELECT 1".to_owned()),
        }
    }

    /// An autovacuum worker is named as one, on the partition it holds,
    /// with what decides whether PostgreSQL cancels it for the migrate: its
    /// query, an anti-wraparound vacuum's, or hidden from the role, when
    /// that is unknown. A client keeps its row, and says when its activity is
    /// hidden.
    #[test]
    fn an_autovacuum_holder_is_named_as_a_worker_on_its_partition() {
        assert_eq!(
            worker(Some("autovacuum: VACUUM ANALYZE prism.qbit_share_ledger_p0")).to_string(),
            "autovacuum worker pid 4242 on qbit_share_ledger_p0: autovacuum: VACUUM ANALYZE prism.qbit_share_ledger_p0"
        );
        assert_eq!(
            worker(Some("autovacuum: VACUUM prism.qbit_share_ledger_p0 (to prevent wraparound)"))
                .to_string(),
            "autovacuum worker pid 4242 on qbit_share_ledger_p0, an anti-wraparound vacuum, which PostgreSQL does not cancel for a lock request: autovacuum: VACUUM prism.qbit_share_ledger_p0 (to prevent wraparound)"
        );
        assert_eq!(
            worker(None).to_string(),
            "autovacuum worker pid 4242 on qbit_share_ledger_p0, its query hidden from this role, so whether it is an anti-wraparound vacuum is unknown"
        );
        assert_eq!(
            client(true).to_string(),
            "pid 77 (idle in transaction, its transaction open 13 s) holding qbit_prism_templates, qbit_share_ledger: SELECT 1"
        );
        assert_eq!(
            client(false).to_string(),
            "pid 77 (its activity hidden from this role) holding qbit_prism_templates, qbit_share_ledger"
        );
    }

    /// A refusal says what ends each kind of hold: for an autovacuum
    /// worker, an administrator's cancel or autovacuum turned off on its
    /// table, and where the role could not lower `deadlock_timeout`, the
    /// grant that lets PostgreSQL cancel it. A refusal without autovacuum
    /// reads as before.
    #[test]
    fn a_refusal_names_the_remedy_for_each_kind_of_holder() {
        for lowered in [true, false] {
            assert_eq!(
                refusal_tail(&Ok(vec![client(true)]), lowered),
                format!(
                    " Holding them now: {}. End those transactions, or stop the frontends, and migrate again",
                    client(true)
                )
            );
            assert_eq!(
                refusal_tail(&Ok(Vec::new()), lowered),
                " No transaction holds them now; migrate again"
            );
        }
        assert_eq!(
            refusal_tail(&Ok(vec![worker(Some("autovacuum: VACUUM t"))]), false),
            format!(
                " Holding them now: autovacuum worker pid 4242 on qbit_share_ledger_p0: autovacuum: VACUUM t. {LOWER_REMEDY}. {AUTOVACUUM_REMEDY}, and migrate again"
            )
        );
        assert_eq!(
            refusal_tail(&Ok(vec![worker(None), client(true)]), true),
            format!(
                " Holding them now: {}; {}. End those transactions, or stop the frontends. {AUTOVACUUM_REMEDY}, and migrate again",
                worker(None),
                client(true)
            )
        );
        assert!(
            LOWER_REMEDY.contains("SET ON PARAMETER deadlock_timeout")
                && AUTOVACUUM_REMEDY.contains("SELECT pg_cancel_backend(<pid>)")
                && AUTOVACUUM_REMEDY.contains("SET (autovacuum_enabled = off)")
        );
        assert!(refusal_tail(&Err(anyhow::anyhow!("gone")), false)
            .starts_with(" Who holds them could not be read (gone)"));
    }
}
