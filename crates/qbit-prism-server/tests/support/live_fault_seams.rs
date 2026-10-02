//! Fault seams for the #474 live cases, shared by its lost-reply cases (C)
//! and its async-promotion case (B).
//!
//! - [`RpcReplyHold`]: a JSON-RPC proxy between the servers and the node that
//!   records every call and its answer and, when armed for a method, lets
//!   the next such call run on the node and then withholds the node's reply
//!   until the test releases it. The node has performed the side effect;
//!   the calling server has not heard so.
//! - [`JournalGate`]: a database trigger that holds a chosen row update on a
//!   lock the test takes, so a frontend cannot journal an outcome while the
//!   test kills it. Crash ordering comes from this barrier, not from sleeps.
//! - [`kill_frontend`], [`end_frontend_backends`] and [`start_frontend`]:
//!   `SIGKILL` one server, terminate its database backends, and restart it
//!   pointed at a proxy.
//! - [`StratumRelay`]: a Stratum proxy between a miner and a server that,
//!   when armed for a request id, withholds the server's answer and closes
//!   both sides, so the miner never receives an acknowledgement the server
//!   sent only after its ledger outcome was durable.
use super::*;
use std::sync::Arc;
use tokio::{
    io::AsyncReadExt,
    net::{tcp::OwnedWriteHalf, TcpStream},
    sync::{oneshot, watch},
};

/// One JSON-RPC call a server made through [`RpcReplyHold`] and the node's
/// answer, `None` until it arrived (or when it never did).
#[derive(Clone, Debug)]
pub(crate) struct RpcCall {
    pub method: String,
    pub params: Value,
    pub reply: Option<Value>,
}

/// A reply the node sent and the proxy withholds: the call's parameters,
/// the node's full answer, and the release.
pub(crate) struct HeldReply {
    pub params: Value,
    pub reply: Value,
    pub release: oneshot::Sender<()>,
}

struct RpcState {
    upstream: u16,
    calls: Mutex<Vec<RpcCall>>,
    trap: Mutex<Option<(String, oneshot::Sender<HeldReply>)>>,
    /// Flipped at drop to release every held reply.
    released: watch::Sender<bool>,
}

/// A transparent HTTP/1.1 JSON-RPC proxy in front of the node that can
/// withhold the reply to one call after the node answered it. Only the held
/// connection waits; every other call passes.
pub(crate) struct RpcReplyHold {
    pub port: u16,
    state: Arc<RpcState>,
    task: tokio::task::JoinHandle<()>,
}

impl RpcReplyHold {
    pub(crate) async fn start(upstream: u16) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let state = Arc::new(RpcState {
            upstream,
            calls: Mutex::new(Vec::new()),
            trap: Mutex::new(None),
            released: watch::Sender::new(false),
        });
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((downstream, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    // A failed connection is what the server sees as a
                    // transport failure; nothing else to report.
                    let _ = relay_rpc(state, downstream).await;
                });
            }
        });
        Ok(Self { port, state, task })
    }

    /// Withhold the reply to the next call of `method`, once the node has
    /// answered it.
    pub(crate) fn arm(&self, method: &str) -> oneshot::Receiver<HeldReply> {
        let (sender, receiver) = oneshot::channel();
        *self.state.trap.lock().unwrap() = Some((method.into(), sender));
        receiver
    }

    /// Every call of `method` so far, in the order the proxy read them.
    pub(crate) fn calls(&self, method: &str) -> Vec<RpcCall> {
        self.state
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.method == method)
            .cloned()
            .collect()
    }
}

impl Drop for RpcReplyHold {
    fn drop(&mut self) {
        self.state.released.send_replace(true);
        self.task.abort();
    }
}

/// Relays one server connection, one request and reply at a time. Each
/// request goes to the node on a new connection: a held reply can outlast
/// qbitd's idle timeout, and the next request must not be written into a
/// connection the node closed meanwhile.
async fn relay_rpc(state: Arc<RpcState>, downstream: TcpStream) -> Result<()> {
    let (down_read, mut down_write) = downstream.into_split();
    let mut down_read = BufReader::new(down_read);
    loop {
        let Some((request, body)) = read_http(&mut down_read).await? else {
            return Ok(());
        };
        let call: Value = serde_json::from_slice(&body).unwrap_or_default();
        let method = call["method"].as_str().unwrap_or_default().to_owned();
        let index = {
            let mut calls = state.calls.lock().unwrap();
            calls.push(RpcCall {
                method: method.clone(),
                params: call["params"].clone(),
                reply: None,
            });
            calls.len() - 1
        };
        let upstream = TcpStream::connect(("127.0.0.1", state.upstream)).await?;
        let (up_read, mut up_write) = upstream.into_split();
        up_write.write_all(&request).await?;
        let Some((reply, reply_body)) = read_http(&mut BufReader::new(up_read)).await? else {
            return Ok(());
        };
        let answer: Value = serde_json::from_slice(&reply_body).unwrap_or_default();
        state.calls.lock().unwrap()[index].reply = Some(answer.clone());
        let trap = {
            let mut trap = state.trap.lock().unwrap();
            match trap.take() {
                Some((armed, sender)) if armed == method => Some(sender),
                other => {
                    *trap = other;
                    None
                }
            }
        };
        if let Some(sender) = trap {
            let (release, released) = oneshot::channel();
            let mut cleanup = state.released.subscribe();
            let held = HeldReply {
                params: call["params"].clone(),
                reply: answer,
                release,
            };
            if sender.send(held).is_ok() {
                tokio::select! {
                    _ = released => {}
                    _ = cleanup.wait_for(|released| *released) => {}
                }
            }
        }
        down_write.write_all(&reply).await?;
    }
}

/// One HTTP/1.1 message framed by `Content-Length`, as qbitd and the
/// servers' client frame every message: the raw bytes and the body. `None`
/// at a clean end of stream.
async fn read_http<R>(reader: &mut BufReader<R>) -> Result<Option<(Vec<u8>, Vec<u8>)>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut raw = Vec::new();
    let mut length = None;
    loop {
        let start = raw.len();
        if reader.read_until(b'\n', &mut raw).await? == 0 {
            ensure!(raw.is_empty(), "connection closed inside an HTTP header");
            return Ok(None);
        }
        let line = std::str::from_utf8(&raw[start..])?.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = Some(value.trim().parse::<usize>()?);
            }
        }
    }
    let mut body = vec![0; length.context("HTTP message without Content-Length")?];
    reader.read_exact(&mut body).await?;
    raw.extend_from_slice(&body);
    Ok(Some((raw, body)))
}

/// A `BEFORE UPDATE` trigger on one table of the fixture's schema that,
/// for rows matching its condition, waits on an advisory lock. The trigger
/// takes the lock shared, so an open gate never serializes the servers; the
/// test takes it exclusively on its own connection to close the gate.
pub(crate) struct JournalGate {
    name: String,
    table: String,
    key: String,
    holder: Option<(sqlx::pool::PoolConnection<sqlx::Postgres>, i32)>,
}

impl JournalGate {
    /// Install an open gate named `name` on `table` for updates where `when`
    /// (a trigger `WHEN` condition over `OLD` and `NEW`) holds.
    pub(crate) async fn install(
        fixture: &Fixture,
        name: &str,
        table: &str,
        when: &str,
    ) -> Result<Self> {
        let key = format!("{}.{name}", fixture.schema);
        sqlx::raw_sql(&format!(
            "CREATE FUNCTION {schema}.{name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock_shared(hashtext('{key}')::bigint); RETURN NEW; END $$; CREATE TRIGGER {name} BEFORE UPDATE ON {schema}.{table} FOR EACH ROW WHEN ({when}) EXECUTE FUNCTION {schema}.{name}();",
            schema = fixture.schema
        ))
        .execute(&fixture.pool)
        .await?;
        Ok(Self {
            name: name.into(),
            table: table.into(),
            key,
            holder: None,
        })
    }

    /// Close the gate: every matching update from now on waits.
    pub(crate) async fn close(&mut self, fixture: &Fixture) -> Result<()> {
        ensure!(
            self.holder.is_none(),
            "gate {} is already closed",
            self.name
        );
        let mut connection = fixture.pool.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock(hashtext($1)::bigint)")
            .bind(&self.key)
            .execute(&mut *connection)
            .await?;
        let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await?;
        self.holder = Some((connection, pid));
        Ok(())
    }

    /// Open the gate, keeping its trigger: every waiting update proceeds.
    pub(crate) async fn open(&mut self) -> Result<()> {
        let (connection, _) = self.holder.take().context("the gate is open")?;
        self.unlock(connection).await
    }

    /// Release the gate's lock on its holder. If the unlock fails, the
    /// connection is closed rather than returned to the pool still holding
    /// the session lock; closing it releases the lock.
    async fn unlock(
        &self,
        mut connection: sqlx::pool::PoolConnection<sqlx::Postgres>,
    ) -> Result<()> {
        let unlocked = sqlx::query("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
            .bind(&self.key)
            .execute(&mut *connection)
            .await;
        if unlocked.is_err() {
            drop(connection.detach());
        }
        unlocked?;
        Ok(())
    }

    /// The backends whose update waits on the closed gate.
    pub(crate) async fn waiting(&self, fixture: &Fixture) -> Result<Vec<i32>> {
        let (_, pid) = self.holder.as_ref().context("the gate is open")?;
        Ok(sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) ORDER BY pid",
        )
        .bind(pid)
        .fetch_all(&fixture.admin)
        .await?)
    }

    /// Open the gate and remove its trigger.
    pub(crate) async fn remove(mut self, fixture: &Fixture) -> Result<()> {
        if let Some((connection, _)) = self.holder.take() {
            self.unlock(connection).await?;
        }
        sqlx::raw_sql(&format!(
            "DROP TRIGGER {name} ON {schema}.{table}; DROP FUNCTION {schema}.{name}();",
            name = self.name,
            table = self.table,
            schema = fixture.schema
        ))
        .execute(&fixture.pool)
        .await?;
        Ok(())
    }
}

/// A gate dropped while closed (a failed case) closes its holder
/// connection instead of returning it to the pool with the session lock
/// held, which would hold every gated update, and the teardown, forever.
impl Drop for JournalGate {
    fn drop(&mut self) {
        if let Some((connection, _)) = self.holder.take() {
            drop(connection.detach());
        }
    }
}

/// Start server `index` with its node calls going to `rpc_port` (a proxy),
/// sponsoring CPFP packages at `fee` when given, and wait until it is ready.
pub(crate) async fn start_frontend(
    fixture: &Fixture,
    index: usize,
    rpc_port: u16,
    fee: Option<u64>,
) -> Result<Process> {
    let mut database = url::Url::parse(&fixture.database_url)?;
    database
        .query_pairs_mut()
        .append_pair("application_name", &backend_name(fixture, index));
    let process = fixture.start_server_with(
        index,
        fee,
        &[
            ("QBIT_RPC_PORT", rpc_port.to_string()),
            ("PRISM_DATABASE_URL", database.to_string()),
        ],
    )?;
    until(&format!("server {index} readiness"), 60, || async {
        Ok(fixture
            .client
            .get(format!("http://127.0.0.1:{}/healthz", fixture.api[index]))
            .send()
            .await?
            .status()
            .is_success())
    })
    .await?;
    // The name is how its backends are found after a kill.
    let named: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name=$1")
            .bind(backend_name(fixture, index))
            .fetch_one(&fixture.admin)
            .await?;
    ensure!(
        named > 0,
        "server {index}'s database connections do not carry their application_name"
    );
    Ok(process)
}

/// `SIGKILL` server `index` and reap it: no graceful shutdown, no claim
/// handed back, no journal written after this returns.
pub(crate) fn kill_frontend(fixture: &mut Fixture, index: usize) -> Result<()> {
    let server = fixture
        .servers
        .get_mut(index)
        .with_context(|| format!("server {index} was never started"))?;
    server.stop();
    ensure!(
        server.child.try_wait()?.is_some(),
        "server {index} is still running after SIGKILL"
    );
    Ok(())
}

/// The `application_name` of server `index`'s database connections when
/// [`start_frontend`] started it: unique to the fixture, so no other test's
/// backends on a shared cluster match it.
fn backend_name(fixture: &Fixture, index: usize) -> String {
    format!("{}-live-{index}", fixture.schema)
}

/// Terminate every database backend of server `index`, which the test has
/// already killed, and wait until they are gone. A statement the dead
/// process wrote before it died can still be sitting unread in a backend's
/// socket; run later, an implicit single-statement transaction would commit
/// for a client that no longer exists. Terminated, it rolls back. Returns
/// how many backends were terminated.
pub(crate) async fn end_frontend_backends(fixture: &Fixture, index: usize) -> Result<usize> {
    ensure!(
        fixture.servers[index].child.try_wait()?.is_some(),
        "server {index} is still running"
    );
    let name = backend_name(fixture, index);
    let backends: Vec<i32> =
        sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE application_name=$1")
            .bind(&name)
            .fetch_all(&fixture.admin)
            .await?;
    for pid in &backends {
        sqlx::query("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .execute(&fixture.admin)
            .await?;
    }
    until(&format!("server {index}'s backends gone"), 10, || async {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1",
        )
        .bind(&name)
        .fetch_one(&fixture.admin)
        .await?
            == 0)
    })
    .await?;
    Ok(backends.len())
}

/// The server index of instance id `live-<index>`.
pub(crate) fn frontend_index(instance: &str) -> Result<usize> {
    let index = instance
        .strip_prefix("live-")
        .with_context(|| format!("unexpected instance id {instance}"))?
        .parse()?;
    ensure!(index < 2, "unexpected instance id {instance}");
    Ok(index)
}

/// Mark the dead owner's lease on `table`'s row `key_column = key` as
/// expired, which is what the lease would be after its 120 s: for a fanout,
/// the claim query's own `claim_expires_at<=clock_timestamp()` predicate
/// then hands the row to a survivor; a candidate claim is revoked as well.
/// Refused unless `owner` still holds it, so a live claim is never cut short.
pub(crate) async fn expire_dead_lease(
    fixture: &Fixture,
    table: &str,
    key_column: &str,
    key: &str,
    owner: &str,
) -> Result<()> {
    let expired = sqlx::query(&format!(
        "UPDATE {table} SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE {key_column}=$1 AND claim_instance_id=$2 AND claim_token IS NOT NULL"
    ))
    .bind(key)
    .bind(owner)
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    ensure!(
        expired == 1,
        "the dead owner {owner} does not hold the claim on {key}"
    );
    // #581: a candidate claim is taken over once a survivor has watched it go
    // unrenewed for its lease, never by the database clock; revoking it is
    // what lets the survivor take it at once.
    if table == "qbit_block_candidate_outbox" {
        qbit_prism_server::ledger::revoke_candidate_claims(&fixture.pool, Some(key), false).await?;
    }
    Ok(())
}

/// The server's withheld answer to an armed request, and the switch that
/// closes both sides of the relayed connection without delivering it.
pub(crate) struct HeldAnswer {
    pub answer: Value,
    pub sever: oneshot::Sender<()>,
}

struct StratumState {
    upstream: u16,
    trap: Mutex<Option<(Value, oneshot::Sender<HeldAnswer>)>>,
    released: watch::Sender<bool>,
}

/// A line-oriented Stratum proxy in front of one server's listener.
pub(crate) struct StratumRelay {
    pub port: u16,
    state: Arc<StratumState>,
    task: tokio::task::JoinHandle<()>,
}

impl StratumRelay {
    pub(crate) async fn start(upstream: u16) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let state = Arc::new(StratumState {
            upstream,
            trap: Mutex::new(None),
            released: watch::Sender::new(false),
        });
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((downstream, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    let _ = relay_stratum(state, downstream).await;
                });
            }
        });
        Ok(Self { port, state, task })
    }

    /// Withhold the server's answer to request `id` on any relayed
    /// connection, then close that connection when the test severs it.
    pub(crate) fn arm(&self, id: Value) -> oneshot::Receiver<HeldAnswer> {
        let (sender, receiver) = oneshot::channel();
        *self.state.trap.lock().unwrap() = Some((id, sender));
        receiver
    }
}

impl Drop for StratumRelay {
    fn drop(&mut self) {
        self.state.released.send_replace(true);
        self.task.abort();
    }
}

/// Copies the miner's bytes to the server unchanged, and the server's lines
/// to the miner one at a time, until the armed answer arrives: that line is
/// never written, and once severed both sides are closed.
async fn relay_stratum(state: Arc<StratumState>, downstream: TcpStream) -> Result<()> {
    let upstream = TcpStream::connect(("127.0.0.1", state.upstream)).await?;
    let (mut down_read, down_write) = downstream.into_split();
    let (up_read, mut up_write) = upstream.into_split();
    let requests = tokio::spawn(async move {
        let _ = tokio::io::copy(&mut down_read, &mut up_write).await;
        let _ = up_write.shutdown().await;
    });
    let result = relay_answers(&state, BufReader::new(up_read), down_write).await;
    // Dropping the copy closes the miner's read half and the server's
    // write half; returning drops the rest.
    requests.abort();
    result
}

async fn relay_answers(
    state: &StratumState,
    mut lines: BufReader<tokio::net::tcp::OwnedReadHalf>,
    mut down_write: OwnedWriteHalf,
) -> Result<()> {
    loop {
        let mut line = String::new();
        if lines.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let message: Value = serde_json::from_str(&line).unwrap_or_default();
        let trap = {
            let mut trap = state.trap.lock().unwrap();
            match trap.take() {
                Some((id, sender)) if message.get("method").is_none() && message["id"] == id => {
                    Some(sender)
                }
                other => {
                    *trap = other;
                    None
                }
            }
        };
        if let Some(sender) = trap {
            let (sever, severed) = oneshot::channel();
            let mut cleanup = state.released.subscribe();
            if sender
                .send(HeldAnswer {
                    answer: message,
                    sever,
                })
                .is_ok()
            {
                tokio::select! {
                    _ = severed => {}
                    _ = cleanup.wait_for(|released| *released) => {}
                }
            }
            return Ok(());
        }
        down_write.write_all(line.as_bytes()).await?;
    }
}
