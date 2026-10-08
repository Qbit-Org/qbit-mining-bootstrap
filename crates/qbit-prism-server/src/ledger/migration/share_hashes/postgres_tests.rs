//! The share-hash backfill's seams that only an injected fault reaches,
//! against a real PostgreSQL (Codex on #746). An attempt to record 2 whose
//! COMMIT committed, but whose reply was then lost or cut short, has
//! finished the backfill: the run reports it done, and never retries into
//! the cursor that commit dropped, on a fresh connection when the
//! attempt's own one dropped (#748). A cursor dropped between its look-up
//! and its read reads as nothing pending. Faults are keyed by schema, so
//! the other tests in this binary never meet them; the proxy's serve one
//! schema's ledger.
use super::faults::{self, Fault};
use super::*;
use crate::ledger::execution_proxy as proxy;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use std::sync::Arc;

struct Database {
    admin: PgPool,
    schema: String,
    url: String,
    ledger: crate::ledger::Ledger,
}

impl Database {
    /// A native ledger in a schema of its own, every migration applied.
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_record_two_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger =
            crate::ledger::Ledger::connect(url.as_str(), "record-two".into(), 2, true).await?;
        Ok(Some(Self {
            admin,
            schema,
            url: url.into(),
            ledger,
        }))
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

/// The migrated ledger as a backfill that permits serving leaves it once
/// every batch has run: 2 unrecorded, the cursor at the end of the legacy
/// ledger, here empty, and the fence at 2, beside 017's conversion bound.
async fn pending_at_its_end(connection: &mut PgConnection) -> Result<()> {
    let mut tx = connection.begin().await?;
    sqlx::query("DELETE FROM qbit_prism_schema_migrations WHERE version=$1")
        .bind(VERSION)
        .execute(&mut *tx)
        .await?;
    create_cursor(&mut tx).await?;
    sqlx::query(
        "INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES($1,$2)",
    )
    .bind(PENDING_CAPABILITY)
    .bind(FENCE_SERVING)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE qbit_prism_share_partitioning SET conversion_bound=COALESCE(conversion_bound,1) WHERE singleton",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// `backfill-share-hashes` whose first record attempt commits and then
/// fails with a statement timeout, as an attempt whose COMMIT reply was cut
/// short would. The run reads what the attempt left, finds 2 recorded and
/// the cursor gone, and reports the backfill done. Without that read it
/// would try again, and fail on the dropped cursor.
#[tokio::test]
async fn a_record_attempt_that_committed_before_it_failed_finishes_the_backfill() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let mut connection = PgConnection::connect(&db.url).await?;
        pending_at_its_end(&mut connection).await?;
        faults::inject(&db.schema, Fault::LoseRecordCommitReply);
        let throttle = Throttle::default().with_record_attempts(3, Duration::from_millis(100))?;
        let reconnect: PgConnectOptions = db.url.parse()?;
        let finished = finish(&mut connection, &reconnect, &throttle, None).await;
        let fired = !faults::armed(&db.schema);
        connection.close().await?;
        let (finished, run_connection) = finished?;
        ensure!(fired, "the injected failure never fired");
        // Its own connection answered the read, and keeps the runners' lock.
        ensure!(run_connection == RunConnection::Kept, "{run_connection:?}");
        ensure!(
            finished.mapped == 0 && finished.range == Some((0, 0)) && !finished.already_complete(),
            "{finished:?}"
        );
        // What the attempt committed: 2 recorded, the cursor and the fence
        // gone.
        let mut check = PgConnection::connect(&db.url).await?;
        let recorded_2 = recorded(&mut check, VERSION).await?;
        let cursor = cursor_relation(&mut check).await?;
        let declared = fence(&mut check).await?;
        check.close().await?;
        ensure!(
            recorded_2 && cursor.is_none() && declared.is_none(),
            "2 recorded: {recorded_2}, cursor: {cursor:?}, fence: {declared:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = db.close().await;
    match (result, closed) {
        (Ok(()), closed) => closed,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(closed)) => {
            Err(error.context(format!("schema cleanup also failed: {closed:#}")))
        }
    }
}

/// An attempt that fails as any other might, here with a lock timeout,
/// then loses its connection to the read after it (#748 review): the read
/// moves to a fresh connection, which finds 2 unrecorded, and the run stops,
/// naming both failures, and that the connection was lost while checking,
/// not by the attempt. The runners' lock went with it all the same.
#[tokio::test]
async fn a_connection_lost_while_checking_a_failed_attempt_keeps_both_errors() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let mut connection = PgConnection::connect(&db.url).await?;
        pending_at_its_end(&mut connection).await?;
        faults::inject(&db.schema, Fault::FailRecordAttempt);
        faults::inject(&db.schema, Fault::LoseConnectionBeforeCheck);
        let throttle = Throttle::default().with_record_attempts(3, Duration::from_millis(100))?;
        let reconnect: PgConnectOptions = db.url.parse()?;
        let stopped = finish(&mut connection, &reconnect, &throttle, None).await;
        let fired = !faults::armed(&db.schema);
        drop(connection);
        ensure!(fired, "an injected failure never fired");
        let text = format!(
            "{:#}",
            stopped
                .err()
                .context("recorded 2 over a connection lost while checking")?
        );
        ensure!(
            text.contains("refusing to try recording migration 2 again: an attempt failed, and the connection was lost while checking whether it had recorded 2 (")
                && text.contains("on a fresh one 2 was not recorded when checked")
                && text.contains("canceling statement due to lock timeout")
                && !text.contains("an attempt lost its connection"),
            "{text}"
        );
        let (recorded_2, cursor, declared) = recorded_state(&db).await?;
        ensure!(
            !recorded_2 && cursor.as_deref() == Some("r") && declared == Some(FENCE_SERVING),
            "2 recorded: {recorded_2}, cursor: {cursor:?}, fence: {declared:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = db.close().await;
    match (result, closed) {
        (Ok(()), closed) => closed,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(closed)) => {
            Err(error.context(format!("schema cleanup also failed: {closed:#}")))
        }
    }
}

/// A cursor dropped after the look-up that found it, as the transaction
/// that records 2 drops it under a reader that takes no migration lock,
/// reads as nothing pending, through either reader of the cursor: the
/// read's 42P01 is the cursor's own, and it is gone when looked up again.
/// The read's 42P01 for any other relation is an error (`self-check`'s
/// test in `ledger_postgres` holds that).
#[tokio::test]
async fn a_cursor_dropped_after_its_look_up_reads_as_nothing_pending() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let drop_cursor =
        || Fault::BeforeCursorRead("DROP TABLE qbit_prism_share_hash_backfill".into());
    let result = async {
        let mut connection = PgConnection::connect(&db.url).await?;
        pending_at_its_end(&mut connection).await?;
        faults::inject(&db.schema, drop_cursor());
        let read = pending(&mut connection).await;
        ensure!(!faults::armed(&db.schema), "the injected drop never ran");
        ensure!(read?.is_none(), "a dropped cursor read as pending");
        let mut tx = connection.begin().await?;
        create_cursor(&mut tx).await?;
        tx.commit().await?;
        faults::inject(&db.schema, drop_cursor());
        let read = progress(&mut connection).await;
        ensure!(!faults::armed(&db.schema), "the injected drop never ran");
        ensure!(read?.is_none(), "a dropped cursor read as pending");
        ensure!(cursor_relation(&mut connection).await?.is_none());
        connection.close().await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = db.close().await;
    match (result, closed) {
        (Ok(()), closed) => closed,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(closed)) => {
            Err(error.context(format!("schema cleanup also failed: {closed:#}")))
        }
    }
}

/// 2's record, its cursor and its fence, read on a connection of its own.
async fn recorded_state(db: &Database) -> Result<(bool, Option<String>, Option<i32>)> {
    let mut check = PgConnection::connect(&db.url).await?;
    let state = (
        recorded(&mut check, VERSION).await?,
        cursor_relation(&mut check).await?,
        fence(&mut check).await?,
    );
    check.close().await?;
    Ok(state)
}

/// What the proxy does to the record's connection once the record's
/// DELETE of the fence has run, which a statement trigger of the test's
/// marks (the start gate refuses any trigger on the migration history).
enum Wire {
    /// Close both sockets at the phase: `AfterCommit` withholds the reply
    /// to the COMMIT that follows the DELETE, which the server has
    /// committed; `AfterExecution` the DELETE's own, before the COMMIT, so
    /// the server rolls the transaction back.
    Drop(proxy::FaultPhase),
    /// Hold the reply to that COMMIT, which the server has committed, with
    /// both sockets open: a connection gone half-open.
    Hold,
}

/// A run of `backfill-share-hashes` through the proxy, done: what it came
/// to, how long it took, and what the test still holds.
struct Run {
    operator: crate::ledger::Ledger,
    proxy: Arc<proxy::ExecutionProxy>,
    refusal: Option<ConnectRefusal>,
    hold: Option<proxy::CommitPause>,
    finished: Result<Finished>,
    took: Duration,
}

impl Run {
    /// Close the operator's pool, stop the relay and finish the proxy, and
    /// only then release a held reply: delivered to a client that is gone,
    /// it would fail the proxy's forwarding task.
    async fn close(self) -> Result<Result<Finished>> {
        self.operator.pool.close().await;
        drop(self.refusal);
        let finished = self.proxy.finish().await;
        drop(self.hold);
        finished?;
        Ok(self.finished)
    }
}

/// `backfill-share-hashes` at `throttle` on the ledger stood at the end of
/// its backfill (`pending_at_its_end`), through the execution proxy with
/// `wire` applied to its record, and, given `refusals`, a relay in front
/// that turns away that many connections once the proxy has acted
/// (`ConnectRefusal`). The run is bounded by `bound`.
async fn record_over(
    db: &Database,
    wire: Wire,
    refusals: Option<u32>,
    throttle: &Throttle,
    bound: Duration,
) -> Result<Run> {
    let mut connection = PgConnection::connect(&db.url).await?;
    pending_at_its_end(&mut connection).await?;
    sqlx::raw_sql(
        "CREATE FUNCTION test_mark_record() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_prism_schema_capabilities DELETE'; RETURN NULL; END $$; \
         CREATE TRIGGER test_mark_record AFTER DELETE ON qbit_prism_schema_capabilities FOR EACH STATEMENT EXECUTE FUNCTION test_mark_record();",
    )
    .execute(&mut connection)
    .await?;
    connection.close().await?;
    let url = url::Url::parse(&db.url)?;
    let upstream = tokio::net::lookup_host((
        url.host_str().context("database host")?,
        url.port().unwrap_or(5432),
    ))
    .await?
    .next()
    .context("database address")?;
    let proxy = Arc::new(proxy::ExecutionProxy::start(upstream).await?);
    let mut operator_url = proxy.rewrite_url(&db.url)?;
    let refusal = match refusals {
        None => None,
        Some(refusals) => {
            let acted = proxy.clone();
            let relay =
                ConnectRefusal::start(proxy_address(&operator_url).await?, refusals, move || {
                    acted.fired().is_some()
                })
                .await?;
            operator_url = relay.rewrite_url(&operator_url)?;
            Some(relay)
        }
    };
    let operator = crate::ledger::Ledger::connect_operator(&operator_url, false).await?;
    let hold = match wire {
        Wire::Drop(phase) => {
            proxy.plan(proxy::Fault {
                table: "qbit_prism_schema_capabilities".into(),
                op: "DELETE".into(),
                phase,
            });
            None
        }
        Wire::Hold => Some(proxy.pause_after_commit("qbit_prism_schema_capabilities", "DELETE")?),
    };
    let started = std::time::Instant::now();
    let finished = tokio::time::timeout(bound, operator.backfill_share_hashes(throttle))
        .await
        .with_context(|| format!("the run outlived its {} s bound", bound.as_secs()))
        .and_then(|finished| finished);
    Ok(Run {
        operator,
        proxy,
        refusal,
        hold,
        finished,
        took: started.elapsed(),
    })
}

/// The address `database_url` names.
async fn proxy_address(database_url: &str) -> Result<std::net::SocketAddr> {
    let url = url::Url::parse(database_url)?;
    let address = tokio::net::lookup_host((
        url.host_str().context("database host")?,
        url.port().unwrap_or(5432),
    ))
    .await?
    .next()
    .context("database address")?;
    Ok(address)
}

/// A TCP relay to `upstream` that, once `armed` holds, turns away the next
/// `refusals` connections: the first by closing it at once, as a refused
/// connect reads, the others with PostgreSQL's FATAL 57P03, "the database
/// system is starting up", as a restarting server answers. It relays every
/// other connection.
struct ConnectRefusal {
    addr: std::net::SocketAddr,
    refused: Arc<std::sync::atomic::AtomicU32>,
    accept: tokio::task::JoinHandle<()>,
}

impl ConnectRefusal {
    async fn start(
        upstream: std::net::SocketAddr,
        refusals: u32,
        armed: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Result<Self> {
        use std::sync::atomic::Ordering;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let refused = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counted = refused.clone();
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                if armed() && counted.load(Ordering::SeqCst) < refusals {
                    let nth = counted.fetch_add(1, Ordering::SeqCst);
                    tokio::spawn(refuse(client, nth));
                } else {
                    tokio::spawn(relay(client, upstream));
                }
            }
        });
        Ok(Self {
            addr,
            refused,
            accept,
        })
    }

    /// `database_url` with its host and port replaced by this relay.
    fn rewrite_url(&self, database_url: &str) -> Result<String> {
        let mut url = url::Url::parse(database_url)?;
        url.set_host(Some(&self.addr.ip().to_string()))?;
        url.set_port(Some(self.addr.port()))
            .map_err(|()| anyhow::anyhow!("database URL cannot carry a port"))?;
        Ok(url.to_string())
    }

    /// The connections turned away so far.
    fn refused(&self) -> u32 {
        self.refused.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for ConnectRefusal {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// Turn one connection away: the first closed at once, the others told
/// 57P03 once they have sent their startup message.
async fn refuse(mut client: tokio::net::TcpStream, nth: u32) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    if nth == 0 {
        return;
    }
    let _ = async {
        // An SSLRequest is answered "no", then the startup message read.
        loop {
            let mut length = [0u8; 4];
            client.read_exact(&mut length).await?;
            let mut body =
                vec![0u8; usize::try_from(u32::from_be_bytes(length))?.saturating_sub(4)];
            client.read_exact(&mut body).await?;
            if body.get(..4) == Some(&80_877_103u32.to_be_bytes()[..]) {
                client.write_all(b"N").await?;
                continue;
            }
            break;
        }
        let mut fields = Vec::new();
        for (kind, value) in [
            (b'S', "FATAL"),
            (b'V', "FATAL"),
            (b'C', "57P03"),
            (b'M', "the database system is starting up"),
        ] {
            fields.push(kind);
            fields.extend_from_slice(value.as_bytes());
            fields.push(0);
        }
        fields.push(0);
        let mut frame = vec![b'E'];
        frame.extend_from_slice(&u32::try_from(fields.len() + 4)?.to_be_bytes());
        frame.extend_from_slice(&fields);
        client.write_all(&frame).await?;
        client.shutdown().await?;
        anyhow::Ok(())
    }
    .await;
}

async fn relay(mut client: tokio::net::TcpStream, upstream: std::net::SocketAddr) {
    if let Ok(mut server) = tokio::net::TcpStream::connect(upstream).await {
        let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
    }
}

/// What a run to be bounded by: every record attempt and backoff, one
/// attempt's deadline and a fresh read more, and half a minute.
fn bound(throttle: &Throttle) -> Duration {
    throttle.record_wait()
        + throttle.record_attempt_deadline()
        + throttle.fresh_read_deadline()
        + Duration::from_secs(30)
}

/// A throttle of `statement_timeout`, whose record is tried patiently: the
/// migration lock is the database's, and the other tests in this binary
/// take it to migrate their own schemas, which a record attempt can meet.
fn patient(statement_timeout: Duration) -> Result<Throttle> {
    Throttle::new(
        Throttle::DEFAULT_MAX_BATCH,
        statement_timeout,
        Throttle::DEFAULT_DUTY_CYCLE,
    )?
    .with_record_attempts(20, Duration::from_millis(100))
}

/// The record's connection drops after the server committed its COMMIT,
/// before the reply (#748): the attempt's own connection is gone, so the
/// run reads on a fresh one, through the proxy as the operator's pool
/// connects, that 2 is recorded and the cursor gone, and reports the
/// backfill done. The lost connection is dropped, not closed.
#[tokio::test]
async fn a_record_whose_connection_drops_after_its_commit_is_read_on_a_fresh_connection(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let throttle = patient(Duration::from_secs(2))?;
        let run = record_over(
            &db,
            Wire::Drop(proxy::FaultPhase::AfterCommit),
            None,
            &throttle,
            bound(&throttle),
        )
        .await?;
        let withheld = run.proxy.fired();
        let commits = run
            .proxy
            .executions_since(0)?
            .into_iter()
            .filter(|execution| execution.is_commit())
            .map(|execution| execution.delivered())
            .collect::<Vec<_>>();
        let finished = run.close().await??;
        ensure!(withheld.is_some(), "the proxy withheld no COMMIT reply");
        ensure!(
            commits.contains(&false),
            "every COMMIT reply was delivered: {commits:?}"
        );
        ensure!(
            finished.mapped == 0 && finished.range == Some((0, 0)) && !finished.already_complete(),
            "{finished:?}"
        );
        let (recorded_2, cursor, declared) = recorded_state(&db).await?;
        ensure!(
            recorded_2 && cursor.is_none() && declared.is_none(),
            "2 recorded: {recorded_2}, cursor: {cursor:?}, fence: {declared:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = db.close().await;
    match (result, closed) {
        (Ok(()), closed) => closed,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(closed)) => {
            Err(error.context(format!("schema cleanup also failed: {closed:#}")))
        }
    }
}

/// As above, but the server restarts meanwhile, as the codes that send the
/// read afresh often mean: the first fresh connect is refused, the second
/// told 57P03, "the database system is starting up". The read connects
/// again after a backoff each time, and the third connect reads 2
/// recorded.
#[tokio::test]
async fn a_fresh_read_connects_again_while_the_server_turns_it_away() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let throttle = patient(Duration::from_secs(2))?;
        let run = record_over(
            &db,
            Wire::Drop(proxy::FaultPhase::AfterCommit),
            Some(2),
            &throttle,
            bound(&throttle),
        )
        .await?;
        let refused = run.refusal.as_ref().map(ConnectRefusal::refused);
        let finished = run.close().await??;
        ensure!(refused == Some(2), "{refused:?} connections refused, not 2");
        ensure!(
            finished.range == Some((0, 0)) && !finished.already_complete(),
            "{finished:?}"
        );
        let (recorded_2, cursor, declared) = recorded_state(&db).await?;
        ensure!(
            recorded_2 && cursor.is_none() && declared.is_none(),
            "2 recorded: {recorded_2}, cursor: {cursor:?}, fence: {declared:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = db.close().await;
    match (result, closed) {
        (Ok(()), closed) => closed,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(closed)) => {
            Err(error.context(format!("schema cleanup also failed: {closed:#}")))
        }
    }
}

/// The record's COMMIT is committed, but its reply never comes, the
/// connection left half-open (`Wire::Hold`): the attempt is abandoned at
/// its client-side deadline, its connection taken for lost, and the run
/// reads afresh that 2 is recorded. Without the deadline it would wait out
/// TCP; here the run's bound would end it.
#[tokio::test]
async fn a_record_whose_commit_reply_never_comes_is_read_afresh_at_its_deadline() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        // A short statement timeout keeps the attempt's deadline short.
        let throttle = patient(Duration::from_millis(300))?;
        let deadline = throttle.record_attempt_deadline();
        // Its attempt's deadline and a fresh read, and a minute for attempts
        // that meet the migration lock first: without the deadline the
        // held reply keeps the run past it.
        let held = deadline + throttle.fresh_read_deadline() + Duration::from_secs(60);
        let run = record_over(&db, Wire::Hold, None, &throttle, held).await?;
        let took = run.took;
        let finished = run.close().await??;
        // The server committed at once: only the held reply kept it so long.
        ensure!(
            took >= deadline,
            "the run took {took:?}, less than its attempt's deadline {deadline:?}"
        );
        ensure!(
            finished.range == Some((0, 0)) && !finished.already_complete(),
            "{finished:?}"
        );
        let (recorded_2, cursor, declared) = recorded_state(&db).await?;
        ensure!(
            recorded_2 && cursor.is_none() && declared.is_none(),
            "2 recorded: {recorded_2}, cursor: {cursor:?}, fence: {declared:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = db.close().await;
    match (result, closed) {
        (Ok(()), closed) => closed,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(closed)) => {
            Err(error.context(format!("schema cleanup also failed: {closed:#}")))
        }
    }
}

/// The record's connection drops before its COMMIT: the server rolls the
/// attempt back, and the fresh connection reads 2 unrecorded and the
/// cursor in place. The runners' lock went with the lost connection, so
/// the run stops rather than retry, naming the rerun, and the rerun, which
/// takes the lock again, records 2.
#[tokio::test]
async fn a_record_whose_connection_drops_before_its_commit_stops_for_a_rerun() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let throttle = patient(Duration::from_secs(2))?;
        let run = record_over(
            &db,
            Wire::Drop(proxy::FaultPhase::AfterExecution),
            None,
            &throttle,
            bound(&throttle),
        )
        .await?;
        let withheld = run.proxy.fired();
        let state = recorded_state(&db).await;
        let rerun = run
            .operator
            .backfill_share_hashes(&Throttle::default())
            .await;
        let stopped = run.close().await?;
        ensure!(withheld.is_some(), "the proxy dropped no connection");
        let text = format!(
            "{:#}",
            stopped
                .err()
                .context("recorded 2 over a connection that dropped before its COMMIT")?
        );
        ensure!(
            text.contains("refusing to try recording migration 2 again: an attempt lost its connection, and on a fresh one 2 was not recorded when checked")
                && text.contains("A COMMIT still in flight there")
                && text.contains("Run `qbit-prism-server backfill-share-hashes` again: it takes the lock"),
            "{text}"
        );
        let (recorded_2, cursor, declared) = state?;
        ensure!(
            !recorded_2 && cursor.as_deref() == Some("r") && declared == Some(FENCE_SERVING),
            "2 recorded: {recorded_2}, cursor: {cursor:?}, fence: {declared:?}"
        );
        let rerun = rerun?;
        ensure!(
            rerun.range == Some((0, 0)) && !rerun.already_complete(),
            "{rerun:?}"
        );
        let (recorded_2, cursor, declared) = recorded_state(&db).await?;
        ensure!(
            recorded_2 && cursor.is_none() && declared.is_none(),
            "2 recorded: {recorded_2}, cursor: {cursor:?}, fence: {declared:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = db.close().await;
    match (result, closed) {
        (Ok(()), closed) => closed,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(closed)) => {
            Err(error.context(format!("schema cleanup also failed: {closed:#}")))
        }
    }
}
