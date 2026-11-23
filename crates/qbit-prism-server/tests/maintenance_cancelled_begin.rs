//! Partition maintenance and the rollup sweep cancelled between `BEGIN` and
//! its reply (#482).
//!
//! `partitions::ensure` runs at startup, on the maintenance tick (which
//! shutdown cuts), in the share append's partition repair (which the
//! share-commit deadline cuts) and in peer sync. `rollups::advance` runs on
//! the rollup tick, which shutdown cuts. Each opened its transaction with a
//! plain `Transaction::begin`. SQLx 0.8 counts a transaction only once
//! `BEGIN`'s reply is read, so a future dropped with that reply outstanding
//! handed the pool a connection whose server side was in a transaction SQLx
//! never counted. The next autocommit write on that connection, such as the
//! instance's stopped marker at shutdown, then ran inside it and was lost when
//! the pool closed. The next `BEGIN` drew PostgreSQL's warning
//! `there is already a transaction in progress`.
//!
//! Each case routes a one-connection pool through
//! `support/ledger_execution_proxy.rs` and holds the reply of the operation's
//! `BEGIN` at the proxy. It drops the operation there, as a `select!` on
//! shutdown or a timeout drops it, then releases the reply. It then checks
//! three things:
//! - the backend settles idle, outside any transaction;
//! - an autocommit write on the same connection survives the pool's close;
//! - the next `BEGIN` on it draws no warning.
use anyhow::{bail, ensure, Context, Result};
use futures_util::FutureExt;
use qbit_prism_server::{ledger::Ledger, partitions, rollups};
use qbit_prism_test_gate as gate;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::future::Future;
use std::panic::{resume_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;
use tracing::instrument::WithSubscriber;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

#[path = "support/ledger_execution_proxy.rs"]
mod execution_proxy;
use execution_proxy::ExecutionProxy;

/// Ceiling for every wait in this binary.
const WAIT: Duration = Duration::from_secs(10);
const NOTICE_TARGET: &str = "sqlx::postgres::notice";

/// Warnings SQLx relayed while a future ran, as JSON lines.
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
    /// The messages of the server notices SQLx relayed.
    fn notices(&self) -> Result<Vec<String>> {
        let text = String::from_utf8(self.0.lock().unwrap().clone())?;
        let mut notices = Vec::new();
        for line in text.lines() {
            let event: Value = serde_json::from_str(line)?;
            if event["target"] == NOTICE_TARGET {
                notices.push(
                    event["fields"]["message"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                );
            }
        }
        Ok(notices)
    }
}

fn capturing_subscriber(capture: &LogCapture) -> impl tracing::Subscriber + Send + Sync {
    let writer = capture.clone();
    tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(move || writer.clone())
        .finish()
}

struct Harness {
    db: FixtureDatabase,
    /// Direct sessions, not through the proxy: the observer's view.
    control: PgPool,
    proxy: ExecutionProxy,
}

impl Harness {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let db = FixtureDatabase::open(&raw, "prism_maintenance_begin_").await?;
        let opened = async {
            // Migrated (the partitioned share ledger and the rollup tables)
            // by a ledger of its own, closed again: the cases use bare pools.
            let ledger = Ledger::connect(&db.url, "maintenance-begin".into(), 2, true).await?;
            ledger.pool.close().await;
            let control = PgPoolOptions::new()
                .max_connections(2)
                .connect(&db.url)
                .await?;
            sqlx::query("CREATE TABLE prism_maintenance_probe(id int PRIMARY KEY)")
                .execute(&control)
                .await?;
            let url = url::Url::parse(&db.url)?;
            let upstream = tokio::net::lookup_host((
                url.host_str().context("database host")?,
                url.port().unwrap_or(5432),
            ))
            .await?
            .next()
            .context("database address")?;
            let proxy = ExecutionProxy::start(upstream).await?;
            Ok::<_, anyhow::Error>((control, proxy))
        }
        .await;
        match opened {
            Ok((control, proxy)) => Ok(Some(Self { db, control, proxy })),
            Err(error) => Err(db.abandon(error).await),
        }
    }

    async fn close(self, result: Result<()>) -> Result<()> {
        self.control.close().await;
        let finished = self.proxy.finish().await;
        self.db.close(result.and(finished)).await
    }

    /// The backend's state once nothing more is in flight on it: `idle` once
    /// the pool has put the connection back, waiting up to [`WAIT`].
    async fn settled(&self, pid: i32) -> Result<String> {
        let mut state = String::new();
        let deadline = tokio::time::Instant::now() + WAIT;
        while tokio::time::Instant::now() < deadline {
            state = sqlx::query_scalar("SELECT state FROM pg_stat_activity WHERE pid=$1")
                .bind(pid)
                .fetch_optional(&self.control)
                .await?
                .unwrap_or_else(|| "gone (the connection was closed, not returned)".into());
            if state == "idle" {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        Ok(state)
    }
}

/// What a case does on the pool once the cancelled checkout has settled.
#[derive(Clone, Copy, Debug)]
enum Then {
    /// An autocommit write, then the pool closed, as shutdown's stopped
    /// marker is written and the pool closed after it.
    WriteAndClose,
    /// The next transaction, watched for PostgreSQL's warning.
    Begin,
}

/// Drop `operation` while the reply to its `BEGIN` is held at the proxy, as a
/// shutdown `select!` or a deadline drops it, and check what it left on the
/// pool's one connection.
async fn cancelled_inside_begin<F, Fut, T>(
    h: &Harness,
    id: i32,
    then: Then,
    operation: F,
) -> Result<()>
where
    F: FnOnce(PgPool) -> Fut,
    Fut: Future<Output = Result<T>>,
    T: std::fmt::Debug,
{
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&h.proxy.rewrite_url(&h.db.url)?)
        .await?;
    let result = async {
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&pool)
            .await?;
        let mark = h.proxy.mark();
        let pause = h.proxy.pause_statement("BEGIN")?;
        {
            let operation = std::pin::pin!(operation(pool.clone()));
            tokio::select! {
                _ = pause.entered() => {}
                result = operation => bail!("the operation finished before its BEGIN reply was held: {result:?}"),
            }
            // Dropped here, with `BEGIN`'s reply outstanding.
        }
        pause.release();
        drop(pause);
        let state = h.settled(pid).await?;
        ensure!(
            state == "idle",
            "the cancelled checkout's backend is {state}, not idle outside a transaction"
        );
        let connection = h
            .proxy
            .executions_since(mark)?
            .first()
            .context("the cancelled BEGIN")?
            .connection;
        match then {
            Then::WriteAndClose => {
                sqlx::query("INSERT INTO prism_maintenance_probe VALUES ($1)")
                    .bind(id)
                    .execute(&pool)
                    .await?;
                pool.close().await;
                let durable: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM prism_maintenance_probe WHERE id=$1)",
                )
                .bind(id)
                .fetch_one(&h.control)
                .await?;
                ensure!(
                    durable,
                    "the autocommit write after the cancelled BEGIN was lost with the pool"
                );
                // On the wire: the cancelled BEGIN, its rollback, the write.
                let wire: Vec<String> = h
                    .proxy
                    .executions_since(mark)?
                    .into_iter()
                    .filter(|execution| execution.connection == connection)
                    .map(|execution| execution.sql)
                    .collect();
                let rollback = wire.iter().position(|sql| sql == "ROLLBACK");
                let write = wire
                    .iter()
                    .position(|sql| sql.starts_with("INSERT INTO prism_maintenance_probe"));
                ensure!(
                    wire.first().map(String::as_str) == Some("BEGIN")
                        && rollback.zip(write).is_some_and(|(rollback, write)| rollback < write),
                    "the write did not follow a rollback of the cancelled BEGIN: {wire:?}"
                );
            }
            Then::Begin => {
                let capture = LogCapture::default();
                async {
                    let mut tx = pool.begin().await?;
                    sqlx::query("SELECT 1").execute(&mut *tx).await?;
                    tx.commit().await?;
                    Ok::<_, anyhow::Error>(())
                }
                .with_subscriber(capturing_subscriber(&capture))
                .await?;
                // No server notice at all, so the check holds whatever
                // language the server's `lc_messages` answers in (in English,
                // the warning is `there is already a transaction in progress`).
                let notices = capture.notices()?;
                ensure!(notices.is_empty(), "the next BEGIN drew a warning: {notices:?}");
            }
        }
        Ok(())
    }
    .await;
    pool.close().await;
    result
}

async fn run(
    body: impl for<'a> FnOnce(&'a Harness) -> futures_util::future::LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = AssertUnwindSafe(body(&h)).catch_unwind().await;
    match result {
        Err(panic) => {
            let _ = h.close(Ok(())).await;
            resume_unwind(panic)
        }
        Ok(result) => h.close(result).await,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_maintenance_cancelled_inside_begin_leaves_its_connection_clean() -> Result<()> {
    run(|h| {
        Box::pin(async move {
            for (id, then) in [(1, Then::WriteAndClose), (2, Then::Begin)] {
                cancelled_inside_begin(h, id, then, |pool| async move {
                    partitions::ensure(&pool).await
                })
                .await
                .with_context(|| format!("partition maintenance, then {then:?}"))?;
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollup_sweep_cancelled_inside_begin_leaves_its_connection_clean() -> Result<()> {
    run(|h| {
        Box::pin(async move {
            for (id, then) in [(3, Then::WriteAndClose), (4, Then::Begin)] {
                cancelled_inside_begin(h, id, then, |pool| async move {
                    rollups::advance(&pool, 1000).await
                })
                .await
                .with_context(|| format!("rollup sweep, then {then:?}"))?;
            }
            Ok(())
        })
    })
    .await
}
