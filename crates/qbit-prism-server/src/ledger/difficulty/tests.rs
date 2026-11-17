//! A difficulty hint's statements end on the server by the write's deadline
//! (#872): the rules that take each statement's timeout from what is left of
//! the bound, and, on a real PostgreSQL, the remaining-time rule and the
//! custom `BEGIN` that sets the first timeout.
use super::*;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
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
    assert_eq!(deadline.arm(start, start + ms(450)), Arm::Spent);
    assert_eq!(
        deadline.arm(start, start + Duration::from_micros(449_100)),
        Arm::Spent
    );
    // Whatever is set reads at least 1.
    for left in [ms(1), Duration::from_micros(1_999), ms(450)] {
        let statement = begin_statement(left);
        assert!(!statement.ends_with("= 0"), "{statement}");
    }
    assert_eq!(
        begin_statement(ms(1)),
        "BEGIN; SET LOCAL statement_timeout = 1"
    );
}

// Each statement's timeout comes from what is left when it is sent: a
// timeout set up to half the margin ago still ends the statement inside the
// bound; an older one is set again from what is left, so a statement sent
// late gets only the rest.
#[test]
fn a_later_statement_of_one_write_gets_only_what_is_left_of_its_bound() {
    let start = Instant::now();
    let deadline = HintDeadline::starting(start, ms(500));
    assert_eq!(deadline.arm(start, start), Arm::Armed);
    assert_eq!(deadline.arm(start, start + ms(25)), Arm::Armed);
    assert_eq!(deadline.arm(start, start + ms(26)), Arm::Set(ms(424)));
    assert_eq!(deadline.arm(start, start + ms(300)), Arm::Set(ms(150)));
    // Measured from when it was last set, not from the start.
    assert_eq!(deadline.arm(start + ms(300), start + ms(320)), Arm::Armed);
    assert_eq!(
        deadline.arm(start + ms(300), start + ms(330)),
        Arm::Set(ms(120))
    );
    // A statement sent under an armed timeout ends by the deadline less
    // half the margin at the latest.
    for sent in 0..=25 {
        let armed = deadline.statement_timeout(start).unwrap();
        assert!(start + ms(sent) + armed <= deadline.at() - ms(25));
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
            .append_pair("options", &format!("-csearch_path={schema}"))
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

    /// The ledger's backends running a statement now.
    async fn active(&self) -> Result<Vec<(i32, String)>> {
        Ok(sqlx::query_as(
            "SELECT pid,query FROM pg_stat_activity WHERE application_name=$1 AND state='active'",
        )
        .bind(&self.application)
        .fetch_all(&self.admin)
        .await?)
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
    let result = body(&db).await;
    let cleanup = db.close().await;
    match (result, cleanup) {
        (Ok(()), cleanup) => cleanup,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("schema cleanup also failed: {cleanup}")))
        }
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
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
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
            next_checkout_is_fresh(pool, pid).await
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
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
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
            next_checkout_is_fresh(pool, pid).await
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
                sqlx::query_as("SELECT pg_backend_pid(),current_setting('statement_timeout')")
                    .fetch_one(&mut *connection)
                    .await?;
            let mut custom = CustomBegin::checked_out(connection);
            {
                let mut tx = custom.custom_begin(begin_statement(ms(1234))).await?;
                let inside: String =
                    sqlx::query_scalar("SELECT current_setting('statement_timeout')")
                        .fetch_one(&mut *tx)
                        .await?;
                ensure!(inside == "1234ms", "{inside}");
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
                    sqlx::query_as("SELECT pg_backend_pid(),current_setting('statement_timeout')")
                        .fetch_one(&mut *next)
                        .await?;
                if again == pid {
                    break setting;
                }
                held.push(next);
            };
            ensure!(setting == default && setting == "15s", "{setting}");
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
/// `statement_timeout` outside any transaction, and can begin.
async fn next_checkout_is_fresh(pool: &PgPool, old: i32) -> Result<()> {
    let mut next = pool.acquire().await?;
    let (pid, setting): (i32, String) =
        sqlx::query_as("SELECT pg_backend_pid(),current_setting('statement_timeout')")
            .fetch_one(&mut *next)
            .await?;
    ensure!(pid != old, "the retired connection was offered again");
    ensure!(setting == "15s", "{setting}");
    let mut tx = shielded_begin(next).await?;
    sqlx::query("SELECT 1").execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
