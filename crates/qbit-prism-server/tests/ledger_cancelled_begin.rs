//! A ledger future cancelled between `BEGIN` and its reply (#482).
//!
//! The share-commit deadline aborts the persist task (#324); a miner
//! disconnect drops it; the metrics collector and session allocation run
//! under timeouts of their own. Any of them can drop a ledger future while a
//! `BEGIN` is on the wire and its reply is not yet read. SQLx 0.8 counts a
//! transaction only once that reply is in, so the server's transaction
//! outlived the future and the pooled connection: the next checkout's
//! `BEGIN` landed inside it and PostgreSQL warned `there is already a
//! transaction in progress` in the frontend log.
//!
//! Every test here routes one two-connection ledger pool through
//! `support/ledger_execution_proxy.rs`, holds the reply of exactly one
//! `BEGIN` at the proxy (the share append's `BEGIN; SET LOCAL …` batch is
//! also held after its completions, at the closing `ReadyForQuery`), aborts
//! the future waiting for it, releases the reply and then reads the connection back from three sides: the server
//! (`pg_stat_activity`, `pg_locks`), the wire (the proxy's execution log for
//! that connection) and the next checkout, which runs with a subscriber that
//! records every warning SQLx relays from a server notice. The proof is that
//! the next transaction on that same backend starts clean.
use anyhow::{bail, ensure, Context, Result};
use qbit_prism::AcceptedShare;
use qbit_prism_server::ledger::Ledger;
use qbit_prism_test_gate as gate;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::instrument::WithSubscriber;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

#[path = "support/ledger_execution_proxy.rs"]
mod execution_proxy;
use execution_proxy::{Execution, ExecutionProxy};

const ORDER_LOCK: i64 = 0x505249534d000002;
const SETTLEMENT_LOCK: i64 = 0x505249534d000003;
/// Ceiling for every wait in this binary.
const WAIT: Duration = Duration::from_secs(10);
const NOTICE_TARGET: &str = "sqlx::postgres::notice";
const ALREADY_IN_PROGRESS: &str = "there is already a transaction in progress";
/// The share append's opening simple query (`Ledger::APPEND_TRANSACTION_BEGIN`):
/// `BEGIN` and the transaction's generic-plan setting in one round trip.
const APPEND_BEGIN: &str = "BEGIN; SET LOCAL plan_cache_mode = force_generic_plan";

fn share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("worker:{id:064x}"),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

/// One ledger backend as `pg_stat_activity` reports it from a session that
/// is not one of the ledger's own.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Backend {
    pid: i32,
    state: String,
    query: String,
}

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
    fn events(&self) -> Result<Vec<Value>> {
        String::from_utf8(self.0.lock().unwrap().clone())?
            .lines()
            .map(|line| Ok(serde_json::from_str(line)?))
            .collect()
    }

    /// Server notices SQLx relayed at WARN, `(target, message)`.
    fn notices(&self) -> Result<Vec<(String, String)>> {
        Ok(self
            .events()?
            .iter()
            .filter(|event| event["target"] == NOTICE_TARGET)
            .map(|event| {
                (
                    event["target"].as_str().unwrap_or_default().to_owned(),
                    event["fields"]["message"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                )
            })
            .collect())
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
    ledger: Ledger,
    /// Direct sessions, not through the proxy: the observer's view.
    control: PgPool,
    proxy: ExecutionProxy,
}

impl Harness {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let db = FixtureDatabase::open(&raw, "prism_cancelled_begin_").await?;
        let url = url::Url::parse(&db.url)?;
        let upstream = tokio::net::lookup_host((
            url.host_str().context("database host")?,
            url.port().unwrap_or(5432),
        ))
        .await?
        .next()
        .context("database address")?;
        let proxy = ExecutionProxy::start(upstream).await?;
        let ledger = Ledger::connect(
            &proxy.rewrite_url(&db.url)?,
            "cancelled-begin".into(),
            2,
            true,
        )
        .await?;
        let control = PgPoolOptions::new()
            .max_connections(2)
            .connect(&db.url)
            .await?;
        Ok(Some(Self {
            db,
            ledger,
            control,
            proxy,
        }))
    }

    /// Every backend of the ledger's pool, as registered by
    /// [`register_backend`], in the server's own words.
    async fn ledger_backends(&self) -> Result<Vec<Backend>> {
        let rows: Vec<(i32, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT pid, state, query FROM pg_stat_activity \
             WHERE pid IN (SELECT pid FROM prism_cancelled_begin_backends) ORDER BY pid",
        )
        .fetch_all(&self.control)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(pid, state, query)| Backend {
                pid,
                state: state.unwrap_or_default(),
                query: query.unwrap_or_default(),
            })
            .collect())
    }

    /// Advisory locks any ledger backend holds on the ledger's keys.
    async fn ledger_advisory_locks(&self) -> Result<Vec<(i32, i64)>> {
        let rows: Vec<(i32, i64)> = sqlx::query_as(
            "SELECT l.pid, ((l.classid::bigint << 32) | l.objid::bigint) FROM pg_locks l \
             WHERE l.locktype='advisory' AND l.granted \
             AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()) \
             AND ((l.classid::bigint << 32) | l.objid::bigint) IN ($1,$2) ORDER BY 1,2",
        )
        .bind(ORDER_LOCK)
        .bind(SETTLEMENT_LOCK)
        .fetch_all(&self.control)
        .await?;
        Ok(rows)
    }

    /// Wait until the cancelled checkout has settled, whichever way: its
    /// backend is gone (retired and closed) or idle again (returned), and no
    /// ledger backend is still running anything. Reports the ledger backends
    /// as the server sees them at that moment.
    async fn settled_backends(&self, cancelled_pid: i32) -> Result<Vec<Backend>> {
        timeout(WAIT, async {
            loop {
                let backends = self.ledger_backends().await?;
                let cancelled = backends.iter().find(|backend| backend.pid == cancelled_pid);
                let settled = cancelled.is_none_or(|backend| backend.state != "active")
                    && backends.iter().all(|backend| backend.state != "active")
                    && (cancelled.is_none() || self.ledger.pool.num_idle() >= 1);
                if settled {
                    return Ok::<_, anyhow::Error>(backends);
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("the cancelled checkout never settled")?
    }

    async fn ids(&self) -> Result<Vec<String>> {
        Ok(
            sqlx::query_scalar("SELECT share_id FROM qbit_share_ledger ORDER BY share_id")
                .fetch_all(&self.control)
                .await?,
        )
    }

    async fn close(self, result: Result<()>) -> Result<()> {
        self.ledger.pool.close().await;
        self.control.close().await;
        let observed = self.proxy.finish().await;
        self.db.close(result.and(observed)).await
    }
}

/// Aborts the spawned future if the test leaves before joining it, exactly
/// as `AppendTask::abort` cancels a persist task at the share deadline.
struct Running<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Running<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) -> Running<T> {
    Running(tokio::spawn(future))
}

/// Statement texts the proxy saw on `connection` after `mark`, in order.
fn wire(executions: &[Execution], connection: u64) -> Vec<String> {
    executions
        .iter()
        .filter(|execution| execution.connection == connection)
        .map(|execution| execution.sql.clone())
        .collect()
}

/// A fixture table naming the ledger pool's backends, so the observer can
/// tell them apart from every other session in the database.
async fn create_backend_registry(h: &Harness) -> Result<()> {
    sqlx::query("CREATE TABLE prism_cancelled_begin_backends(pid int PRIMARY KEY)")
        .execute(&h.control)
        .await?;
    Ok(())
}

/// Register the backend behind one pooled connection and return its pid.
async fn register_backend(h: &Harness, connection: &mut sqlx::PgConnection) -> Result<i32> {
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(connection)
        .await?;
    sqlx::query(
        "INSERT INTO prism_cancelled_begin_backends(pid) VALUES($1) ON CONFLICT DO NOTHING",
    )
    .bind(pid)
    .execute(&h.control)
    .await?;
    Ok(pid)
}

/// How the cancelled future is dropped: a persist task is aborted
/// (`AppendTask::abort`), a `tokio::time::timeout` drops the future in place.
#[derive(Clone, Copy)]
enum Cancel {
    Abort,
    Drop,
}

/// Which part of the opening statement's reply the proxy holds.
#[derive(Clone, Copy)]
enum Hold {
    /// Everything from its first `CommandComplete`: the client has none of
    /// the reply.
    Reply,
    /// Only the closing `ReadyForQuery`: every completion of the batch has
    /// been handed to the client's socket, the round trip has not ended.
    ReadyForQuery,
}

/// What the observer saw once a future cancelled inside `BEGIN` had settled.
struct Observed {
    /// Backend pid of the cancelled checkout.
    cancelled_pid: i32,
    /// Proxy connection the cancelled checkout used.
    cancelled_connection: u64,
    /// Proxy connections open before the next transaction.
    connections_before_next: u64,
    /// Ledger backends once the cancelled checkout had settled.
    settled: Vec<Backend>,
    /// The next transaction's backend pid.
    next_pid: i32,
    /// Proxy connection the next transaction used.
    next_connection: u64,
    /// Statement texts on the cancelled connection since the cancelled
    /// `BEGIN`, in order.
    wire: Vec<String>,
    /// For each statement in `wire`, whether it opens a transaction.
    wire_begins: Vec<bool>,
    /// Server notices SQLx relayed while the next transaction ran.
    notices: Vec<(String, String)>,
}

/// Cancel `operation` while the reply to its opening statement `begin` is
/// held at the proxy, release the reply, wait for the checkout to settle and
/// run the next transaction on the pool, which can only get the connection
/// the cancelled checkout used or a replacement for it.
async fn cancel_inside_begin<T: Send + 'static>(
    h: &Harness,
    cancel: Cancel,
    begin: &str,
    hold: Hold,
    operation: impl Future<Output = T> + Send + 'static,
) -> Result<Observed> {
    // Keep one connection checked out so the cancelled one is the only
    // connection the next checkout can get.
    let mut spare = h.ledger.pool.acquire().await?;
    register_backend(h, &mut spare).await?;
    // Open the second connection on a plain read and register it too, so the
    // operation's checkout is a known backend rather than a fresh socket.
    let cancelled_pid = {
        let mut second = h.ledger.pool.acquire().await?;
        register_backend(h, &mut second).await?
    };
    let mark = h.proxy.mark();
    let pause = match hold {
        Hold::Reply => h.proxy.pause_statement(begin)?,
        Hold::ReadyForQuery => h.proxy.pause_statement_ready(begin)?,
    };
    let held = match cancel {
        Cancel::Abort => {
            let mut running = spawn(operation);
            let held = tokio::select! {
                seq = pause.entered() => seq,
                _ = &mut running.0 => bail!("the operation finished before its BEGIN reply was held"),
            };
            // The future is blocked reading the BEGIN reply. Abort it there.
            running.0.abort();
            match timeout(WAIT, &mut running.0).await? {
                Err(joined) if joined.is_cancelled() => {}
                Err(joined) => bail!("the operation panicked instead of cancelling: {joined}"),
                Ok(_) => bail!("the operation finished instead of cancelling"),
            }
            held
        }
        Cancel::Drop => {
            let operation = std::pin::pin!(operation);
            let held = tokio::select! {
                seq = pause.entered() => seq,
                _ = operation => bail!("the operation finished before its BEGIN reply was held"),
            };
            // `select!` dropped the future when the other arm won, exactly
            // as a timeout drops it.
            held
        }
    };
    let held = h
        .proxy
        .executions_since(mark)?
        .into_iter()
        .find(|execution| execution.seq == held)
        .context("held execution")?;
    ensure!(held.sql == begin, "held {:?}", held.sql);
    if let Hold::ReadyForQuery = hold {
        // Every completion of the batch, `BEGIN`'s and `SET`'s, went out
        // before the held `ReadyForQuery`; only the end of the round trip
        // was outstanding when the future was cancelled.
        ensure!(
            held.delivered() && held.completion() == Some("SET"),
            "the batch's completions were not delivered before the hold: {held:?}"
        );
    }
    let cancelled_connection = held.connection;
    // Before the reply is delivered the server has already run BEGIN: the
    // backend is in a transaction the client will never learn about.
    pause.release();
    drop(pause);
    let settled = h.settled_backends(cancelled_pid).await?;
    eprintln!("ledger backends after the cancelled BEGIN settled: {settled:?}");
    let locks = h.ledger_advisory_locks().await?;
    ensure!(
        locks.is_empty(),
        "advisory locks held after cancellation: {locks:?}"
    );
    let connections_before_next = h.proxy.connections();
    let next_mark = h.proxy.mark();
    // The next transaction on the pool.
    let capture = LogCapture::default();
    let next_pid: i32 = async {
        let mut tx = h.ledger.pool.begin().await?;
        let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok::<_, anyhow::Error>(pid)
    }
    .with_subscriber(capturing_subscriber(&capture))
    .await?;
    sqlx::query(
        "INSERT INTO prism_cancelled_begin_backends(pid) VALUES($1) ON CONFLICT DO NOTHING",
    )
    .bind(next_pid)
    .execute(&h.control)
    .await?;
    let notices = capture.notices()?;
    let next = h.proxy.executions_since(next_mark)?;
    let next_connection = next
        .iter()
        .find(|execution| execution.begins_transaction())
        .context("the next transaction's BEGIN")?
        .connection;
    let since = h.proxy.executions_since(mark)?;
    let wire_begins = since
        .iter()
        .filter(|execution| execution.connection == cancelled_connection)
        .map(Execution::begins_transaction)
        .collect();
    let wire = wire(&since, cancelled_connection);
    eprintln!("wire on proxy connection {cancelled_connection}: {wire:?}");
    eprintln!(
        "next transaction: backend {next_pid} on proxy connection {next_connection}, notices {notices:?}"
    );
    drop(spare);
    Ok(Observed {
        cancelled_pid,
        cancelled_connection,
        connections_before_next,
        settled,
        next_pid,
        next_connection,
        wire,
        wire_begins,
        notices,
    })
}

/// What every cancellation must leave behind, whichever way the guard
/// disposed of the connection. `begin` is the cancelled opening statement.
fn assert_clean(h: &Harness, observed: &Observed, begin: &str) -> Result<()> {
    ensure!(
        observed
            .settled
            .iter()
            .all(|backend| backend.state == "idle"),
        "a ledger backend is not idle after the cancelled BEGIN: {:?}",
        observed.settled
    );
    ensure!(
        observed
            .notices
            .iter()
            .all(|(_, message)| !message.contains(ALREADY_IN_PROGRESS)),
        "PostgreSQL warned on the next BEGIN: {:?}",
        observed.notices
    );
    ensure!(
        observed.wire.first().map(String::as_str) == Some(begin),
        "the cancelled BEGIN was not the first statement observed: {:?}",
        observed.wire
    );
    // Any later statement that opens a transaction, whether a plain `BEGIN`
    // or an append's `BEGIN; SET LOCAL …` batch, must follow a ROLLBACK.
    let second_begin = observed.wire_begins[1..].iter().position(|begins| *begins);
    ensure!(
        second_begin.is_none_or(|begin| {
            observed.wire[1..]
                .iter()
                .position(|sql| sql == "ROLLBACK")
                .is_some_and(|rollback| rollback < begin)
        }),
        "a second BEGIN reached the cancelled connection before a ROLLBACK: {:?}",
        observed.wire
    );
    let _ = h;
    Ok(())
}

/// A share append aborted while its `BEGIN` reply is outstanding, as the
/// share-commit deadline (`AppendTask::abort`) or a miner disconnect aborts
/// the persist task. The borrowed `BEGIN` on the admission-guarded
/// connection cannot be rolled back by the guard, so the connection is
/// retired: it never re-enters the pool, and the next transaction runs on a
/// replacement.
#[tokio::test]
async fn share_append_cancelled_inside_begin_retires_the_connection() -> Result<()> {
    share_append_cancelled_inside_begin(Hold::Reply).await
}

/// The append's `BEGIN` carries its `SET LOCAL plan_cache_mode` in the same
/// simple query. Cancelled after both completions went out but before the
/// `ReadyForQuery` that ends the round trip, the guard must still count the
/// `BEGIN` as unfinished and retire the connection: completion is recorded
/// only once the whole batch has been read, not at `BEGIN`'s own reply.
#[tokio::test]
async fn share_append_cancelled_before_its_begin_batch_ends_retires_the_connection() -> Result<()> {
    share_append_cancelled_inside_begin(Hold::ReadyForQuery).await
}

async fn share_append_cancelled_inside_begin(hold: Hold) -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        create_backend_registry(&h).await?;
        let ledger = h.ledger.clone();
        let observed = cancel_inside_begin(&h, Cancel::Abort, APPEND_BEGIN, hold, async move {
            ledger.append(share(1), None).await
        })
        .await?;
        ensure!(
            h.ids().await?.is_empty(),
            "a cancelled append credited a share"
        );
        assert_clean(&h, &observed, APPEND_BEGIN)?;
        ensure!(
            observed.wire == [APPEND_BEGIN],
            "the retired connection carried more than the cancelled BEGIN: {:?}",
            observed.wire
        );
        ensure!(
            !observed
                .settled
                .iter()
                .any(|backend| backend.pid == observed.cancelled_pid),
            "the cancelled checkout's backend outlived the retirement: {:?}",
            observed.settled
        );
        ensure!(
            observed.next_pid != observed.cancelled_pid
                && observed.next_connection != observed.cancelled_connection
                && observed.connections_before_next < h.proxy.connections(),
            "the next transaction did not run on a replacement connection"
        );
        // The money path continues on the pool, one connection per append,
        // at the same round trips as before.
        let mark = h.proxy.mark();
        let connections = h.proxy.connections();
        ensure!(h.ledger.append(share(2), None).await?.inserted);
        ensure!(h.ids().await? == vec![share(2).share_id]);
        let normal: Vec<String> = h
            .proxy
            .executions_since(mark)?
            .iter()
            .map(|execution| execution.sql.clone())
            .collect();
        eprintln!(
            "normal append after the retirement: {} executions, {} new connections: {normal:?}",
            normal.len(),
            h.proxy.connections() - connections
        );
        ensure!(
            h.ledger_advisory_locks().await?.is_empty(),
            "advisory locks held after the next append"
        );
        Ok(())
    }
    .await;
    h.close(result).await
}

/// Session allocation (`Ledger::begin`, the owned form) dropped at the same
/// point, as the subscribe timeout drops it. The `BEGIN` round trip runs in
/// its own task, so the drop cannot interrupt it: the transaction it opened
/// is rolled back on the same connection, which returns to the pool clean
/// and serves the next transaction.
#[tokio::test]
async fn session_allocation_dropped_inside_begin_returns_the_connection_clean() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        create_backend_registry(&h).await?;
        let ledger = h.ledger.clone();
        let observed = cancel_inside_begin(&h, Cancel::Drop, "BEGIN", Hold::Reply, async move {
            ledger.new_session_id().await.map(|id| id.value())
        })
        .await?;
        assert_clean(&h, &observed, "BEGIN")?;
        ensure!(
            observed.wire.starts_with(&[
                "BEGIN".to_owned(),
                "ROLLBACK".to_owned(),
                "BEGIN".to_owned()
            ]),
            "the cancelled BEGIN was not rolled back on its connection before the next: {:?}",
            observed.wire
        );
        ensure!(
            observed.next_pid == observed.cancelled_pid
                && observed.next_connection == observed.cancelled_connection
                && observed.connections_before_next == h.proxy.connections(),
            "the next transaction did not reuse the cancelled checkout's connection"
        );
        let reservations: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_prism_session_reservations")
                .fetch_one(&h.control)
                .await?;
        ensure!(
            reservations == 0,
            "a cancelled allocation left a reservation"
        );
        let session = h.ledger.new_session_id().await?;
        session.release().await?;
        Ok(())
    }
    .await;
    h.close(result).await
}
