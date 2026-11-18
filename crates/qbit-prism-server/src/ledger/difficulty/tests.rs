//! A difficulty hint's statements end on the server by the write's deadline
//! (#872): the rules that take each statement's timeout from what is left of
//! the bound, and, on a real PostgreSQL, the remaining-time rule and the
//! custom `BEGIN` that sets the first timeout.
use super::*;
use futures_util::FutureExt;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use std::panic::{resume_unwind, AssertUnwindSafe};
use tokio::time::Instant;

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

#[test]
fn a_statement_gets_what_is_left_of_the_bound_less_a_tenth_in_whole_milliseconds() {
    let start = Instant::now();
    let deadline = HintDeadline::starting(start, ms(500));
    assert_eq!(deadline.at(), start + ms(500));
    assert_eq!(deadline.statement_timeout(start), Some(ms(450)));
    // Rounded down, never up past what is left.
    assert_eq!(
        deadline.statement_timeout(start + Duration::from_micros(100_400)),
        Some(ms(349))
    );
    assert_eq!(deadline.statement_timeout(start + ms(400)), Some(ms(50)));
    // A non-default bound is the one the server is given.
    let longer = HintDeadline::starting(start, ms(1_500));
    assert_eq!(longer.statement_timeout(start), Some(ms(1_350)));
}

// `statement_timeout = 0` disables the timeout, so no bound, margin or
// rounding ever produces it: with less than a millisecond left the
// statement is not sent at all.
#[test]
fn a_statement_with_less_than_a_millisecond_left_is_not_sent_rather_than_given_zero() {
    let start = Instant::now();
    let deadline = HintDeadline::starting(start, ms(500));
    // 449.9 ms less the 50 ms margin is 0.9 ms: under a millisecond.
    assert_eq!(
        deadline.statement_timeout(start + Duration::from_micros(449_100)),
        None
    );
    assert_eq!(deadline.statement_timeout(start + ms(449)), Some(ms(1)));
    assert_eq!(deadline.statement_timeout(start + ms(450)), None);
    assert_eq!(deadline.statement_timeout(start + ms(600)), None);
    // The smallest bound the configuration takes, 1 ms, leaves 0.9 ms.
    let smallest = HintDeadline::starting(start, ms(1));
    assert_eq!(smallest.statement_timeout(start), None);
    // A bound whose tenth rounds the rest under a millisecond.
    let tiny = HintDeadline::starting(start, Duration::from_micros(1_100));
    assert_eq!(tiny.statement_timeout(start), None);
    let small = HintDeadline::starting(start, Duration::from_micros(1_200));
    assert_eq!(small.statement_timeout(start), Some(ms(1)));
    // The re-arm follows the same rule.
    assert_eq!(
        deadline.arm(ms(450), Duration::ZERO, start + ms(450)),
        Arm::Spent
    );
    assert_eq!(
        deadline.arm(
            ms(450),
            Duration::ZERO,
            start + Duration::from_micros(449_100)
        ),
        Arm::Spent
    );
    // Whatever is set reads at least 1.
    for left in [ms(1), Duration::from_micros(1_999), ms(450)] {
        let statement = begin_statement(left);
        assert!(!statement.ends_with("= 0"), "{statement}");
    }
    assert_eq!(
        begin_statement(ms(1)),
        "BEGIN; SET LOCAL statement_timeout = 1; SET LOCAL synchronous_commit = local"
    );
}

// Each statement's timeout comes from what is left when it is sent. The
// timeout in force is used while a statement sent under it would end by the
// deadline less half the margin (475 ms into a 500 ms bound); otherwise it is
// set again, to what will be left once the setting's own round trip (the last
// one measured) is over, so a statement sent late gets only the rest, and one
// sent after a slow reply is not let run past the bound.
#[test]
fn a_later_statement_of_one_write_gets_only_what_is_left_of_its_bound() {
    let start = Instant::now();
    let deadline = HintDeadline::starting(start, ms(500));
    let first = deadline.statement_timeout(start).unwrap();
    assert_eq!(first, ms(450));
    assert_eq!(deadline.arm(first, Duration::ZERO, start), Arm::Armed);
    assert_eq!(
        deadline.arm(first, Duration::ZERO, start + ms(25)),
        Arm::Armed
    );
    assert_eq!(
        deadline.arm(first, Duration::ZERO, start + ms(26)),
        Arm::Set(ms(424))
    );
    assert_eq!(
        deadline.arm(first, Duration::ZERO, start + ms(300)),
        Arm::Set(ms(150))
    );
    // Set again, it allows for the setting's own round trip, as measured.
    assert_eq!(
        deadline.arm(first, ms(20), start + ms(300)),
        Arm::Set(ms(130))
    );
    // Its reply on time: the statement goes out under it.
    assert_eq!(deadline.arm(ms(130), ms(20), start + ms(320)), Arm::Armed);
    // Its reply 45 ms late: if used, the statement would end at 495 ms, so it
    // is set again from the slower round trip.
    assert_eq!(
        deadline.arm(ms(130), ms(65), start + ms(365)),
        Arm::Set(ms(20))
    );
    // Whatever the timeout in force and the round trip: a statement sent
    // under an armed timeout ends by the deadline less half the margin, and
    // one sent once a new setting's estimated round trip is over ends under
    // it by the deadline less the margin.
    for now in (0..=500).step_by(5) {
        for armed in [1, 50, 130, 450] {
            for round_trip in [0, 5, 30] {
                let now = start + ms(now);
                match deadline.arm(ms(armed), ms(round_trip), now) {
                    Arm::Armed => assert!(now + ms(armed) <= deadline.at() - ms(25)),
                    Arm::Set(timeout) => {
                        assert!(now + ms(round_trip) + timeout <= deadline.at() - ms(50))
                    }
                    Arm::Spent => {
                        assert!(deadline.statement_timeout(now + ms(round_trip)).is_none())
                    }
                }
            }
        }
    }
}

/// A timeout setting on the paused clock: each takes the next of `trips`
/// (the last one once they run out), and is counted.
struct ClockedSetting {
    trips: Vec<Duration>,
    settings: usize,
}

impl SetStatementTimeout for ClockedSetting {
    async fn set_statement_timeout(
        &mut self,
        _timeout: Duration,
        _in_force: Duration,
    ) -> Result<()> {
        let trip = self
            .trips
            .get(self.settings)
            .or(self.trips.last())
            .copied()
            .unwrap_or_default();
        self.settings += 1;
        tokio::time::advance(trip).await;
        Ok(())
    }
}

/// One hint write as `write_worker_difficulty_hint` sends it, at a steady
/// round trip `trip`, through the real [`arm_with`] on the paused clock: the
/// `BEGIN`, then `prepares` prepares SQLx has cached (they send nothing), then
/// `statements` statements, each armed first, then `COMMIT` if more than the
/// margin is left. Its round trips, and whether it committed inside the bound.
async fn hint_write(trip: Duration, prepares: usize, statements: usize) -> (usize, bool) {
    let deadline = HintDeadline::after(ms(500));
    let mut state = ArmState {
        timeout: deadline.statement_timeout(Instant::now()).unwrap(),
        round_trip: Duration::ZERO,
    };
    tokio::time::advance(trip).await;
    state.round_trip = trip;
    let mut setting = ClockedSetting {
        trips: vec![trip],
        settings: 0,
    };
    let mut trips = 1;
    for step in 0..prepares + statements {
        let before = setting.settings;
        arm_with(&deadline, &mut state, &mut setting)
            .await
            .expect("the write was spent");
        assert!(
            setting.settings - before <= 1,
            "{} settings for one statement",
            setting.settings - before
        );
        if step >= prepares {
            tokio::time::advance(trip).await;
            trips += 1;
        }
    }
    trips += setting.settings;
    let committed = deadline.statement_timeout(Instant::now()).is_some();
    tokio::time::advance(trip).await;
    trips += 1;
    (trips, committed && Instant::now() < deadline.at())
}

// The real arming loop, at round trips past half the 50 ms margin: one re-arm
// per statement at most, none for prepares that send nothing beyond the one
// the late `BEGIN` timeout needs, and the write commits inside its 500 ms
// bound. On a local database it never re-arms.
#[tokio::test(start_paused = true)]
async fn slow_round_trips_cost_at_most_one_re_arm_per_statement() {
    // A decrease: four prepares (the re-arm, the guard, the update, the
    // read-back), then the guard, the update and the read-back.
    assert_eq!(hint_write(ms(1), 4, 3).await, (5, true));
    assert_eq!(hint_write(ms(30), 4, 3).await, (8, true));
    // Evidence whose share is in a recent partition: six prepares (the
    // re-arm, both probes, the guard, the record, the read-back), then the
    // probe, the guard, the record and the read-back.
    assert_eq!(hint_write(ms(1), 6, 4).await, (6, true));
    assert_eq!(hint_write(ms(30), 6, 4).await, (10, true));
}

// A re-arm whose reply comes back late is checked again when it arrives: set
// once more from the slower round trip, and used only if the statement would
// still end in time, else the write is spent. No statement goes out under a
// timeout that would end past the deadline less half the margin.
#[tokio::test(start_paused = true)]
async fn a_late_re_arm_reply_is_set_again_once_then_the_write_is_spent() {
    for (trips, expected) in [
        (vec![ms(70), ms(1)], Some(ms(10))),
        (vec![ms(70), ms(120)], None),
    ] {
        let deadline = HintDeadline::after(ms(500));
        let mut state = ArmState {
            timeout: ms(450),
            round_trip: ms(1),
        };
        // A statement waited 300 ms under the first timeout.
        tokio::time::advance(ms(300)).await;
        let mut setting = ClockedSetting { trips, settings: 0 };
        let armed = arm_with(&deadline, &mut state, &mut setting).await;
        assert_eq!(setting.settings, 2);
        match expected {
            Some(timeout) => {
                let armed = armed.unwrap();
                assert_eq!(armed, timeout);
                assert!(Instant::now() + armed <= deadline.at() - ms(25));
            }
            None => {
                let error = armed.unwrap_err();
                assert!(
                    error.downcast_ref::<HintBoundSpent>().is_some(),
                    "{error:#}"
                );
            }
        }
    }
}

#[test]
fn a_bound_too_long_for_postgresql_or_for_the_clock_is_clamped_not_wrapped() {
    let start = Instant::now();
    let long = HintDeadline::starting(start, Duration::from_secs(40 * 86_400));
    assert_eq!(
        long.statement_timeout(start),
        Some(ms(MAX_STATEMENT_TIMEOUT_MS))
    );
    let endless = HintDeadline::starting(start, Duration::MAX);
    assert!(endless.at() > start + Duration::from_secs(86_400 * 365));
    assert_eq!(
        endless.statement_timeout(start),
        Some(ms(MAX_STATEMENT_TIMEOUT_MS))
    );
}

/// A disposable schema, migrated, with a ledger whose connections carry
/// `application` so the test can find them in `pg_stat_activity`.
struct Database {
    admin: PgPool,
    schema: String,
    ledger: Ledger,
    application: String,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_hint_bound_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let application = format!("hint-bound-{}", &schema[17..29]);
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            // `remote_apply`, which the pool keeps, as an HA deployment's
            // writer role may set it: a hint must leave it as it found it.
            .append_pair(
                "options",
                &format!("-csearch_path={schema} -csynchronous_commit=remote_apply"),
            )
            .append_pair("application_name", &application);
        let url = url.to_string();
        match Ledger::connect(&url, "hint-bound".into(), 2, true).await {
            Ok(ledger) => Ok(Some(Self {
                admin,
                schema,
                ledger,
                application,
            })),
            Err(error) => {
                let _ = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
                    .execute(&admin)
                    .await;
                admin.close().await;
                Err(error)
            }
        }
    }

    async fn close(self) -> Result<()> {
        self.ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }

    /// The ledger's backends still running a statement after up to a second:
    /// none, unless one runs on. (The pool's return flushes a dropped
    /// transaction's `ROLLBACK` in a task of its own, briefly `active`.)
    async fn active(&self) -> Result<Vec<(i32, String)>> {
        let mut active = Vec::new();
        for _ in 0..100 {
            active = sqlx::query_as(
                "SELECT pid,query FROM pg_stat_activity WHERE application_name=$1 AND state='active'",
            )
            .bind(&self.application)
            .fetch_all(&self.admin)
            .await?;
            if active.is_empty() {
                break;
            }
            tokio::time::sleep(ms(10)).await;
        }
        Ok(active)
    }

    /// Whether every one of the ledger's backends is idle outside a
    /// transaction, waiting up to a second for the pool's return to flush a
    /// queued `ROLLBACK`.
    async fn settled(&self) -> Result<bool> {
        for _ in 0..100 {
            let busy: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND state<>'idle'",
            )
            .bind(&self.application)
            .fetch_one(&self.admin)
            .await?;
            if busy == 0 {
                return Ok(true);
            }
            tokio::time::sleep(ms(10)).await;
        }
        Ok(false)
    }

    /// The ledger's backend waiting for a lock, once one is.
    async fn waiting(&self) -> Result<i32> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'")
                    .bind(&self.application)
                    .fetch_optional(&self.admin)
                    .await?;
                if let Some(pid) = waiting {
                    return Ok::<_, anyhow::Error>(pid);
                }
                tokio::time::sleep(ms(5)).await;
            }
        })
        .await
        .context("no hint statement waited for the lock")?
    }

    /// Whether backend `pid` is gone, waiting up to five seconds for it.
    async fn gone(&self, pid: i32) -> Result<bool> {
        for _ in 0..500 {
            let present: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
                    .bind(pid)
                    .fetch_one(&self.admin)
                    .await?;
            if !present {
                return Ok(true);
            }
            tokio::time::sleep(ms(10)).await;
        }
        Ok(false)
    }
}

async fn run(
    body: impl for<'a> FnOnce(&'a Database) -> futures_util::future::LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    // The schema and pools are cleaned up whether the body fails or panics.
    let result = AssertUnwindSafe(body(&db)).catch_unwind().await;
    let cleanup = db.close().await;
    match result {
        Err(panic) => resume_unwind(panic),
        Ok(Ok(())) => cleanup,
        Ok(Err(error)) => match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => Err(error.context(format!("schema cleanup also failed: {cleanup}"))),
        },
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// The SQLSTATE of the server's error in `error`'s chain.
fn sqlstate(error: &anyhow::Error) -> Option<String> {
    super::super::migration::database_error(error)
        .and_then(|cause| cause.code())
        .map(|code| code.into_owned())
}

// The remaining-time rule on a real server: the write guard is held for half
// the bound by a lock from another connection, then the write itself for
// good. The write is set its timeout again from what is left, so PostgreSQL
// ends it before the bound, where the first timeout, run again from the
// write's start, would have let it run half a bound past.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_a_hint_statement_sent_late_gets_only_what_is_left_of_the_write_bound(
) -> Result<()> {
    run(|db| {
        Box::pin(async move {
            db.ledger
                .record_worker_difficulty("primary", "rig", 64.0, now_ms())
                .await?;
            let mut guard = db.admin.begin().await?;
            sqlx::query(&format!(
                "LOCK TABLE {}.qbit_prism_cluster IN ACCESS EXCLUSIVE MODE",
                db.schema
            ))
            .execute(&mut *guard)
            .await?;
            let mut row = db.admin.begin().await?;
            sqlx::query(&format!("SELECT 1 FROM {}.qbit_worker_difficulty WHERE listener='primary' AND worker_username='rig' FOR UPDATE", db.schema))
                .fetch_one(&mut *row)
                .await?;
            let bound = Duration::from_secs(2);
            let deadline = HintDeadline::after(bound);
            let ledger = &db.ledger;
            let write = ledger.write_worker_difficulty_hint("primary", "rig", 32.0, None, 900, deadline);
            let release = async {
                db.waiting().await?;
                tokio::time::sleep_until(deadline.at() - bound / 2).await;
                guard.commit().await?;
                Ok::<_, anyhow::Error>(())
            };
            let (written, released) = tokio::join!(write, release);
            released?;
            let ended = Instant::now();
            let error = written.err().context("the held write returned")?;
            ensure!(hint_timed_out(&error), "{error:#}");
            ensure!(
                error.downcast_ref::<HintBoundSpent>().is_none()
                    && sqlstate(&error).as_deref() == Some("57014"),
                "not the server's timeout: {error:#}"
            );
            // Set again at half the bound to what was left, 1 s less the
            // 200 ms margin: ended about 1.8 s in, where the first timeout,
            // 1.8 s from the write's own start at 1 s, would end it at 2.8 s.
            let start = deadline.at() - bound;
            ensure!(
                ended < deadline.at() && ended >= start + ms(1_700),
                "ended {:?} into the 2 s bound",
                ended - start
            );
            // Nothing is left running, and the connection is back in the
            // pool outside any transaction.
            let active = db.active().await?;
            ensure!(active.is_empty(), "still running: {active:?}");
            ensure!(db.settled().await?, "a hint transaction stayed open");
            row.rollback().await?;
            let kept = ledger.worker_difficulty("primary", "rig", 900).await?;
            ensure!(kept.as_ref().map(|hint| hint.difficulty) == Some(64.0));
            Ok(())
        })
    })
    .await
}

// A statement the connection has not prepared yet takes its `Parse` round
// trip first, and `Sync` restarts the server's timer for its run. Here the
// update waits while it is prepared (its `ROW EXCLUSIVE` lock against a
// `SHARE` lock held for half the bound) and then while it runs (a row lock).
// Prepared under the timeout in force and armed again from what is left, it
// still ends before the bound, where a second full timeout would have let it
// run 1.8 s from the middle of a 2 s bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_a_statement_that_waits_while_it_is_prepared_gets_only_what_is_left_to_run(
) -> Result<()> {
    run(|db| {
        Box::pin(async move {
            db.ledger
                .record_worker_difficulty("primary", "rig", 64.0, now_ms())
                .await?;
            let mut row = db.admin.begin().await?;
            sqlx::query(&format!("SELECT 1 FROM {}.qbit_worker_difficulty WHERE listener='primary' AND worker_username='rig' FOR UPDATE", db.schema))
                .fetch_one(&mut *row)
                .await?;
            let mut table = db.admin.begin().await?;
            sqlx::query(&format!(
                "LOCK TABLE {}.qbit_worker_difficulty IN SHARE MODE",
                db.schema
            ))
            .execute(&mut *table)
            .await?;
            let bound = Duration::from_secs(2);
            let deadline = HintDeadline::after(bound);
            let ledger = &db.ledger;
            // No connection has prepared the decrease's update yet.
            let write =
                ledger.write_worker_difficulty_hint("primary", "rig", 32.0, None, 900, deadline);
            let release = async {
                let pid = db.waiting().await?;
                let query: String = sqlx::query_scalar("SELECT query FROM pg_stat_activity WHERE pid=$1")
                    .bind(pid)
                    .fetch_one(&db.admin)
                    .await?;
                ensure!(query.starts_with("UPDATE qbit_worker_difficulty"), "{query}");
                tokio::time::sleep_until(deadline.at() - bound / 2).await;
                table.commit().await?;
                Ok::<_, anyhow::Error>(())
            };
            let (written, released) = tokio::join!(write, release);
            released?;
            let ended = Instant::now();
            let error = written.err().context("the held write returned")?;
            ensure!(
                hint_timed_out(&error) && sqlstate(&error).as_deref() == Some("57014"),
                "not the server's timeout: {error:#}"
            );
            let start = deadline.at() - bound;
            ensure!(
                ended < deadline.at() && ended >= start + ms(1_700),
                "ended {:?} into the 2 s bound",
                ended - start
            );
            let active = db.active().await?;
            ensure!(active.is_empty(), "still running: {active:?}");
            row.rollback().await?;
            Ok(())
        })
    })
    .await
}

// A refused custom `BEGIN` (its `BEGIN` ran, its `SET LOCAL` did not) leaves
// the server in an aborted transaction SQLx never counted. The connection is
// retired, so the next checkout is a fresh one at the pool's own timeout that
// can begin, never the aborted one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_a_refused_custom_begin_retires_its_connection() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let pool = &db.ledger.pool;
            let mut connection = pool.acquire().await?;
            let (pid, default): (i32, String) =
                sqlx::query_as("SELECT pg_backend_pid(),current_setting('statement_timeout')||' '||current_setting('synchronous_commit')")
                    .fetch_one(&mut *connection)
                    .await?;
            let mut custom = CustomBegin::checked_out(connection);
            let refused = custom
                .custom_begin("BEGIN; SET LOCAL statement_timeout = 'never'".into())
                .await
                .is_err();
            ensure!(refused, "the server took the refused setting");
            drop(custom);
            ensure!(db.gone(pid).await?, "the aborted connection is still open");
            next_checkout_is_fresh(pool, pid, &default).await
        })
    })
    .await
}

// A custom `BEGIN` whose caller is cancelled between the write and the reply
// leaves a server transaction, with its `SET LOCAL`, that SQLx never
// counted. The connection is retired the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_a_custom_begin_cancelled_mid_round_trip_retires_its_connection() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let pool = &db.ledger.pool;
            let mut connection = pool.acquire().await?;
            let (pid, default): (i32, String) =
                sqlx::query_as("SELECT pg_backend_pid(),current_setting('statement_timeout')||' '||current_setting('synchronous_commit')")
                    .fetch_one(&mut *connection)
                    .await?;
            let mut custom = CustomBegin::checked_out(connection);
            let cancelled = tokio::time::timeout(
                ms(100),
                custom.custom_begin(
                    "BEGIN; SET LOCAL statement_timeout = 1234; SELECT pg_sleep(0.5)".into(),
                ),
            )
            .await
            .is_err();
            ensure!(cancelled, "the round trip was not cut short");
            drop(custom);
            ensure!(
                db.gone(pid).await?,
                "the cancelled connection is still open"
            );
            next_checkout_is_fresh(pool, pid, &default).await
        })
    })
    .await
}

// The successful path changes nothing: one connection, back in the pool at
// the pool's own timeout once the transaction ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_a_custom_begin_that_completes_returns_its_connection_at_the_pool_timeout(
) -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let pool = &db.ledger.pool;
            let mut connection = pool.acquire().await?;
            let (pid, default): (i32, String) =
                sqlx::query_as("SELECT pg_backend_pid(),current_setting('statement_timeout')||' '||current_setting('synchronous_commit')")
                    .fetch_one(&mut *connection)
                    .await?;
            let mut custom = CustomBegin::checked_out(connection);
            {
                let mut tx = custom.custom_begin(begin_statement(ms(1234))).await?;
                let inside: String =
                    sqlx::query_scalar("SELECT current_setting('statement_timeout')||' '||current_setting('synchronous_commit')")
                        .fetch_one(&mut *tx)
                        .await?;
                ensure!(inside == "1234ms local", "{inside}");
            }
            drop(custom);
            ensure!(
                returned(pool).await,
                "the connection never returned to the pool"
            );
            ensure!(
                db.settled().await?,
                "a transaction survived on the connection"
            );
            // The pool may hold another idle connection: hold each it hands
            // out until it hands out this one.
            let mut held = Vec::new();
            let setting = loop {
                ensure!(
                    held.len() < 2,
                    "the connection was not returned to the pool"
                );
                let mut next = pool.acquire().await?;
                let (again, setting): (i32, String) =
                    sqlx::query_as("SELECT pg_backend_pid(),current_setting('statement_timeout')||' '||current_setting('synchronous_commit')")
                        .fetch_one(&mut *next)
                        .await?;
                if again == pid {
                    break setting;
                }
                held.push(next);
            };
            ensure!(setting == default, "{setting}, not the pool's {default}");
            Ok(())
        })
    })
    .await
}

/// Whether the pool holds an idle connection, waiting up to a second for
/// one being returned (SQLx returns a connection in a task of its own).
async fn returned(pool: &PgPool) -> bool {
    for _ in 0..100 {
        if pool.num_idle() > 0 {
            return true;
        }
        tokio::time::sleep(ms(10)).await;
    }
    false
}

/// The next checkout is not backend `old`, runs at the pool's own
/// `statement_timeout` and `synchronous_commit`, `default`, outside any
/// transaction, and can begin.
async fn next_checkout_is_fresh(pool: &PgPool, old: i32, default: &str) -> Result<()> {
    let mut next = pool.acquire().await?;
    let (pid, setting): (i32, String) =
        sqlx::query_as("SELECT pg_backend_pid(),current_setting('statement_timeout')||' '||current_setting('synchronous_commit')")
            .fetch_one(&mut *next)
            .await?;
    ensure!(pid != old, "the retired connection was offered again");
    ensure!(setting == default, "{setting}, not the pool's {default}");
    let mut tx = shielded_begin(next).await?;
    sqlx::query("SELECT 1").execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

// A write whose bound is spent by the time its checkout arrives sends
// nothing, so its connection, untouched, goes back to the pool: it is not
// closed, and the pool opens no new one for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_a_write_whose_bound_is_spent_at_checkout_keeps_its_connection() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let pool = &db.ledger.pool;
            // Both of the pool's connections, so the write waits for one.
            let mut first = pool.acquire().await?;
            let mut second = pool.acquire().await?;
            let mut pids = Vec::new();
            for connection in [&mut first, &mut second] {
                let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut **connection)
                    .await?;
                pids.push(pid);
            }
            let bound = ms(400);
            let deadline = HintDeadline::after(bound);
            let write = db
                .ledger
                .write_worker_difficulty_hint("primary", "rig", 32.0, None, 900, deadline);
            let release = async {
                // 20 ms left, under the 40 ms margin.
                tokio::time::sleep_until(deadline.at() - ms(20)).await;
                drop(first);
            };
            let (written, ()) = tokio::join!(write, release);
            let error = written.err().context("a spent write ran")?;
            ensure!(
                error.downcast_ref::<HintBoundSpent>().is_some() && hint_timed_out(&error),
                "{error:#}"
            );
            drop(second);
            ensure!(db.settled().await?, "a connection is busy");
            let mut held = Vec::new();
            for _ in 0..2 {
                let mut connection = pool.acquire().await?;
                let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *connection)
                    .await?;
                ensure!(
                    pids.contains(&pid),
                    "the pool opened {pid} in place of {pids:?}"
                );
                held.push(connection);
            }
            Ok(())
        })
    })
    .await
}

/// The pool's own `statement_timeout` and `synchronous_commit`, as the
/// fixture's tests read them: `<timeout> <level>`.
async fn pool_settings(pool: &PgPool) -> Result<String> {
    let settings: String = sqlx::query_scalar(
        "SELECT current_setting('statement_timeout')||' '||current_setting('synchronous_commit')",
    )
    .fetch_one(pool)
    .await?;
    ensure!(settings.ends_with(" remote_apply"), "{settings}");
    Ok(settings)
}

// A committed hint leaves its connection as the pool keeps it: its
// `statement_timeout` and its `synchronous_commit` (`remote_apply` here) are
// the pool's again, not the hint's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_a_committed_hint_leaves_its_connection_at_the_pools_settings() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let pool = &db.ledger.pool;
            let default = pool_settings(pool).await?;
            db.ledger
                .record_worker_difficulty("primary", "rig", 64.0, now_ms())
                .await?;
            // Hold one of the pool's two connections, so the hint takes the
            // other.
            let mut held = pool.acquire().await?;
            let other: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *held)
                .await?;
            let hint = db
                .ledger
                .write_worker_difficulty_hint(
                    "primary",
                    "rig",
                    32.0,
                    None,
                    900,
                    HintDeadline::after(Duration::from_secs(5)),
                )
                .await?;
            ensure!(hint.as_ref().map(|hint| hint.difficulty) == Some(32.0), "{hint:?}");
            ensure!(returned(pool).await, "the hint's connection never returned");
            let mut connection = pool.acquire().await?;
            let (pid, settings): (i32, String) = sqlx::query_as("SELECT pg_backend_pid(),current_setting('statement_timeout')||' '||current_setting('synchronous_commit')")
                .fetch_one(&mut *connection)
                .await?;
            ensure!(pid != other, "the hint ran on the held connection");
            ensure!(settings == default, "{settings}, not the pool's {default}");
            Ok(())
        })
    })
    .await
}

// `synchronous_commit = local` is safe only because a hint transaction
// writes the hint row and nothing else. Its statements are pinned on the
// wire (`tests/difficulty_hint_bound.rs`); this pins that no trigger, rule
// or referencing key carries a hint row's write to another table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_a_hint_write_reaches_no_table_but_the_hint_rows() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let (triggers, rules, referencing): (i64, i64, i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM pg_trigger WHERE tgrelid='qbit_worker_difficulty'::regclass AND NOT tgisinternal),\
                 (SELECT count(*) FROM pg_rewrite WHERE ev_class='qbit_worker_difficulty'::regclass),\
                 (SELECT count(*) FROM pg_constraint WHERE confrelid='qbit_worker_difficulty'::regclass)",
            )
            .fetch_one(&db.ledger.pool)
            .await?;
            ensure!(
                (triggers, rules, referencing) == (0, 0, 0),
                "qbit_worker_difficulty has {triggers} triggers, {rules} rules and {referencing} referencing keys"
            );
            Ok(())
        })
    })
    .await
}
