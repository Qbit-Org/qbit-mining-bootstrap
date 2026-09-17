//! A test-only PostgreSQL wire observer for single-execution proofs.
//!
//! One [`ExecutionProxy`] sits between one ledger pool and a disposable
//! database on a stable TCP endpoint, the same shape as the HA endpoint in
//! `postgres_failover.rs`. Where that proxy only copies bytes, this one frames
//! both directions of the protocol so a test can count what the server was
//! actually asked to run:
//!
//! - a client `Parse` frame only names a statement and is never counted;
//!   `Bind` attaches a portal to a parsed statement; `Execute` (extended
//!   protocol) and `Query` (simple protocol) are the frames that make the
//!   server run something, and each becomes one [`Execution`];
//! - a server `CommandComplete`, `EmptyQueryResponse` or `ErrorResponse`
//!   settles the oldest in-flight execution of that connection, and
//!   `ReadyForQuery` closes a simple query that may have carried several
//!   statements;
//! - every `ErrorResponse` is also recorded as a [`Rejection`], in the same
//!   order as executions. Rejections are not executions: a statement sent
//!   into an aborted transaction under a text the connection has not
//!   prepared is rejected at its `Parse`, and no `Execute` frame for it
//!   exists to count;
//! - an `Execute` whose statement text the proxy did not learn is reported
//!   as an error rather than as an unidentifiable execution;
//! - a `NoticeResponse` whose message starts with [`MARKER_PREFIX`] is a
//!   marker raised by a statement-level fixture trigger in the test schema.
//!   It is attached to the in-flight execution, which is how a test learns
//!   the identity of "the statement that writes table X" while it runs,
//!   instead of spelling that statement's SQL or its position.
//!
//! `SSLRequest` and `GSSENCRequest` are answered with `N` here so the
//! observed stream stays plaintext; the startup message is forwarded as-is;
//! every new client socket, including a pool reconnect after a severed
//! connection, gets its own upstream socket and its own statement and portal
//! maps.
//!
//! A [`Fault`] withholds exactly one server acknowledgement: either the
//! completion of a marked execution, or the completion of the `COMMIT` that
//! follows it on the same connection. Withholding closes both sockets, so the
//! server aborts the transaction (before commit) or has already committed it
//! (after commit) while the client sees a transport error rather than a
//! result. The proxy records the withheld completion, so a test can tell
//! command execution and durable commit apart.
#![allow(dead_code)]

use anyhow::{bail, ensure, Context, Result};
use futures_util::FutureExt;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};

/// Start of the NOTICE message a fixture trigger raises:
/// `prism-execution-marker <table> <INSERT|UPDATE|DELETE>`.
pub const MARKER_PREFIX: &str = "prism-execution-marker";
/// AFTER ROW notices installed by the compact-runtime measurement fixture.
pub const JSONB_WRITE_PREFIX: &str = "prism-jsonb-write ";

/// Observed row mutations, including unchanged JSONB values carried by UPDATE.
/// These are server executions, not proof of a subsequent transaction commit.
#[derive(Clone, Debug, Default)]
pub struct JsonbWrites {
    pub rows: u64,
    pub values: u64,
    pub max_uncompressed_bytes: u64,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonbWriteNotice {
    table: String,
    operation: String,
    values: BTreeMap<String, u64>,
}

const SSL_REQUEST: u32 = 80_877_103;
const GSSENC_REQUEST: u32 = 80_877_104;
const CANCEL_REQUEST: u32 = 80_877_102;

/// Which client frame asked the server to run the statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    /// `Execute` of a bound portal.
    Extended,
    /// `Query` text, possibly several statements.
    Simple,
}

/// What the server answered, as seen on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// No answer observed yet, or the connection closed before one.
    Pending,
    /// `CommandComplete` (or `EmptyQueryResponse`); `delivered` becomes true
    /// only after forwarding the entire frame to the client succeeds.
    Completed { tag: String, delivered: bool },
    /// `ErrorResponse` with its SQLSTATE and message.
    Failed { code: String, message: String },
}

/// A marker NOTICE attached to an execution by a fixture trigger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Marker {
    pub table: String,
    pub op: String,
}

/// One `Execute` or `Query` frame and the server's answer to it.
#[derive(Clone, Debug)]
pub struct Execution {
    /// Global order across every connection the proxy served.
    pub seq: u64,
    pub connection: u64,
    pub protocol: Protocol,
    /// Statement text learned from `Parse` (through the portal) or `Query`.
    pub sql: String,
    pub markers: Vec<Marker>,
    pub outcome: Outcome,
    /// Actual DataRow frames, counted without retaining their payloads.
    pub rows_received: u64,
    pub jsonb_writes: BTreeMap<(String, String), JsonbWrites>,
    select_rows: Option<u64>,
    ready: bool,
}

impl Execution {
    pub fn marked(&self, table: &str, op: &str) -> bool {
        self.markers
            .iter()
            .any(|marker| marker.table == table && marker.op == op)
    }

    /// The completion tag, whether or not it reached the client.
    pub fn completion(&self) -> Option<&str> {
        match &self.outcome {
            Outcome::Completed { tag, .. } => Some(tag),
            _ => None,
        }
    }

    pub fn delivered(&self) -> bool {
        matches!(
            self.outcome,
            Outcome::Completed {
                delivered: true,
                ..
            }
        )
    }

    /// The SQLSTATE of a failed execution.
    pub fn sqlstate(&self) -> Option<&str> {
        match &self.outcome {
            Outcome::Failed { code, .. } => Some(code),
            _ => None,
        }
    }

    pub fn is_commit(&self) -> bool {
        self.completion() == Some("COMMIT")
    }

    /// Simple-query batches can produce several completions; only their final
    /// ReadyForQuery proves the remainder of the batch is no longer pending.
    pub fn complete_response(&self) -> bool {
        self.delivered() && (self.protocol == Protocol::Extended || self.ready)
    }

    /// A successfully completed SELECT's actual rows, including measured zero.
    /// Partial, failed, lost-response and mixed-result observations are errors.
    pub fn returned_rows(&self) -> Result<u64> {
        ensure!(self.complete_response(), "SELECT response is incomplete");
        let reported = self
            .select_rows
            .context("execution has no SELECT completion")?;
        ensure!(
            reported == self.rows_received,
            "SELECT completion differs from observed DataRow count"
        );
        Ok(self.rows_received)
    }
}

/// One `ErrorResponse` the server sent, kept apart from executions: a
/// statement can be rejected without any frame having asked the server to
/// run it. A statement the client prepares first is rejected at `Parse`, for
/// example, when it is sent into an aborted transaction, and no `Execute`
/// frame for it ever exists.
#[derive(Clone, Debug)]
pub struct Rejection {
    /// Same order as [`Execution::seq`], so [`ExecutionProxy::mark`] covers
    /// both.
    pub seq: u64,
    pub connection: u64,
    pub code: String,
    pub message: String,
    pub frame: RejectedFrame,
}

/// What the rejected client frame was.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectedFrame {
    /// A `Parse` of this statement text; the server never ran it.
    Parse(String),
    /// The recorded execution with this sequence number, which failed.
    Execution(u64),
    /// Any other frame, such as a `Bind`.
    Other,
}

/// When a planned fault closes the connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPhase {
    /// After the server completed the marked statement, before the client
    /// hears about it; the transaction is still open and will abort.
    AfterExecution,
    /// After the server completed the `COMMIT` that follows the marked
    /// statement on the same connection, before the client hears about it.
    AfterCommit,
}

/// Withhold one acknowledgement related to the next execution that carries
/// the marker for `table`/`op`.
#[derive(Clone, Debug)]
pub struct Fault {
    pub table: String,
    pub op: String,
    pub phase: FaultPhase,
}

/// Holds a completed COMMIT reply after PostgreSQL has released transaction
/// locks. Dropping the handle releases delivery, including on test cancellation.
pub struct CommitPause(Arc<CommitPauseState>);

#[derive(Default)]
struct CommitPauseState {
    entered: Notify,
    release: Notify,
    seq: AtomicU64,
    released: AtomicBool,
}

impl CommitPause {
    pub async fn entered(&self) -> u64 {
        loop {
            let entered = self.0.entered.notified();
            tokio::pin!(entered);
            entered.as_mut().enable();
            let seq = self.0.seq.load(Ordering::SeqCst);
            if seq != 0 {
                return seq;
            }
            entered.await;
        }
    }
    pub fn release(&self) {
        self.0.released.store(true, Ordering::SeqCst);
        self.0.release.notify_waiters();
    }
}

impl Drop for CommitPause {
    fn drop(&mut self) {
        self.release();
    }
}

struct PausePlan {
    table: String,
    op: String,
    control: Arc<CommitPauseState>,
}

#[derive(Default)]
struct State {
    executions: Vec<Execution>,
    rejections: Vec<Rejection>,
    plan: Option<Fault>,
    /// Connection whose next `COMMIT` completion is withheld.
    armed: Option<u64>,
    /// Sequence number of the execution whose acknowledgement was withheld.
    fired: Option<u64>,
    pause_plan: Option<PausePlan>,
    pause_armed: Option<u64>,
}

struct Shared {
    seq: AtomicU64,
    connections: AtomicU64,
    state: Mutex<State>,
}

#[derive(Default)]
struct Connection {
    statements: HashMap<Vec<u8>, String>,
    portals: HashMap<Vec<u8>, String>,
    /// Statement text of every `Parse` the server has not yet answered with
    /// `ParseComplete`, oldest first.
    parses: VecDeque<String>,
    /// Indices into `State::executions`, oldest first.
    inflight: VecDeque<usize>,
}

/// Statement text of an `Execute` whose `Bind` or `Parse` the proxy did not
/// see. A client whose whole session passes through the proxy never produces
/// one, so [`ExecutionProxy::executions_since`] refuses to report it.
const UNPARSED: &str = "<unparsed statement>";
const UNBOUND: &str = "<unbound portal>";

pub struct ExecutionProxy {
    addr: SocketAddr,
    shared: Arc<Shared>,
    /// Taken by [`Self::finish`], so a shared proxy is finished once.
    accept: Mutex<Option<JoinHandle<Result<()>>>>,
    tasks: Arc<Mutex<JoinSet<Result<()>>>>,
    failure: Mutex<Option<String>>,
}

impl ExecutionProxy {
    /// Listen on a loopback port and forward every client to `upstream`.
    pub async fn start(upstream: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(Shared {
            seq: AtomicU64::new(0),
            connections: AtomicU64::new(0),
            state: Mutex::default(),
        });
        let tasks: Arc<Mutex<JoinSet<Result<()>>>> = Arc::default();
        let accept = {
            let shared = shared.clone();
            let tasks = tasks.clone();
            tokio::spawn(async move {
                loop {
                    let (client, _) = listener.accept().await.context("proxy accept")?;
                    let id = shared.connections.fetch_add(1, Ordering::SeqCst) + 1;
                    tasks.lock().expect("proxy task registry").spawn(serve(
                        client,
                        upstream,
                        id,
                        shared.clone(),
                    ));
                }
            })
        };
        Ok(Self {
            addr,
            shared,
            accept: Mutex::new(Some(accept)),
            tasks,
            failure: Mutex::default(),
        })
    }

    /// `database_url` with its host and port replaced by this proxy.
    pub fn rewrite_url(&self, database_url: &str) -> Result<String> {
        let mut url = url::Url::parse(database_url)?;
        url.set_host(Some(&self.addr.ip().to_string()))?;
        url.set_port(Some(self.addr.port()))
            .map_err(|()| anyhow::anyhow!("database URL cannot carry a port"))?;
        Ok(url.to_string())
    }

    /// Client sockets accepted so far, including ones closed since.
    pub fn connections(&self) -> u64 {
        self.shared.connections.load(Ordering::SeqCst)
    }

    /// A point in the execution order; see [`Self::executions_since`].
    pub fn mark(&self) -> u64 {
        self.shared.seq.load(Ordering::SeqCst)
    }

    /// Every execution recorded after `mark`, on every connection, oldest
    /// first. Fails if a proxy task has failed, so a broken observer cannot
    /// pass as an empty observation, and fails if an execution's statement
    /// is unknown, so it cannot escape identification by text.
    pub fn executions_since(&self, mark: u64) -> Result<Vec<Execution>> {
        self.check()?;
        let state = self.shared.state.lock().expect("proxy state");
        let executions: Vec<Execution> = state
            .executions
            .iter()
            .filter(|execution| execution.seq > mark)
            .cloned()
            .collect();
        if let Some(unknown) = executions
            .iter()
            .find(|execution| [UNPARSED, UNBOUND].contains(&execution.sql.as_str()))
        {
            bail!("the proxy could not identify an execution's statement: {unknown:?}");
        }
        Ok(executions)
    }

    /// Every `ErrorResponse` recorded after `mark`, on every connection,
    /// oldest first, including rejections no recorded execution asked for.
    pub fn rejections_since(&self, mark: u64) -> Result<Vec<Rejection>> {
        self.check()?;
        let state = self.shared.state.lock().expect("proxy state");
        Ok(state
            .rejections
            .iter()
            .filter(|rejection| rejection.seq > mark)
            .cloned()
            .collect())
    }

    /// Arm one fault; it disarms itself when it fires.
    pub fn plan(&self, fault: Fault) {
        let mut state = self.shared.state.lock().expect("proxy state");
        state.plan = Some(fault);
        state.armed = None;
        state.fired = None;
    }

    /// Pause only the completed COMMIT following the next marked mutation.
    /// The observer reports completed-but-undelivered until forwarding succeeds.
    pub fn pause_after_commit(&self, table: &str, op: &str) -> Result<CommitPause> {
        let mut state = self.shared.state.lock().expect("proxy state");
        ensure!(
            state.plan.is_none() && state.pause_plan.is_none(),
            "proxy already has a delivery plan"
        );
        let control = Arc::new(CommitPauseState::default());
        state.pause_plan = Some(PausePlan {
            table: table.into(),
            op: op.into(),
            control: control.clone(),
        });
        state.pause_armed = None;
        Ok(CommitPause(control))
    }

    /// Sequence number of the execution whose acknowledgement the last
    /// planned fault withheld, once it has fired.
    pub fn fired(&self) -> Option<u64> {
        self.shared.state.lock().expect("proxy state").fired
    }

    /// Surface any connection task that ended with an error or a panic.
    pub fn check(&self) -> Result<()> {
        let mut failure = self.failure.lock().expect("proxy failure");
        if let Some(error) = failure.as_ref() {
            bail!("proxy observation previously failed: {error}");
        }
        let mut tasks = self.tasks.lock().expect("proxy task registry");
        while let Some(joined) = tasks.try_join_next() {
            if let Err(error) = settle(joined) {
                *failure = Some(format!("{error:#}"));
                return Err(error);
            }
        }
        Ok(())
    }

    /// Stop accepting, close every connection and join every task. Every
    /// task is joined even when one of them failed; the first failure is
    /// returned. A second call finds nothing left to join.
    pub async fn finish(&self) -> Result<()> {
        let accept = self.accept.lock().expect("proxy accept handle").take();
        let mut outcome = Ok(());
        if let Some(accept) = accept {
            accept.abort();
            match accept.await {
                Ok(result) => outcome = result,
                Err(error) if error.is_cancelled() => {}
                Err(error) => outcome = Err(anyhow::anyhow!("proxy accept task failed: {error}")),
            }
        }
        let mut tasks = std::mem::take(&mut *self.tasks.lock().expect("proxy task registry"));
        tasks.abort_all();
        while let Some(joined) = tasks.join_next().await {
            let settled = settle(joined);
            if outcome.is_ok() {
                outcome = settled;
            }
        }
        outcome
    }
}

impl Drop for ExecutionProxy {
    fn drop(&mut self) {
        // A proxy dropped without `finish` must not leave forwarding tasks
        // holding sockets to the disposable database.
        if let Ok(mut accept) = self.accept.lock() {
            if let Some(accept) = accept.take() {
                accept.abort();
            }
        }
        if let Ok(mut tasks) = self.tasks.lock() {
            tasks.abort_all();
        }
    }
}

fn settle(joined: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    match joined {
        Ok(result) => result.context("proxy connection task failed"),
        Err(error) if error.is_cancelled() => Ok(()),
        Err(error) => bail!("proxy connection task panicked: {error}"),
    }
}

/// Executions since a mark that are the operation marked `table`/`op`:
/// the marked executions themselves plus every other frame that ran the
/// same statement text, marked or not. A statement replayed inside an
/// aborted transaction fails before its triggers run and so carries no
/// marker, but it is the same statement and is counted.
pub fn target_executions<'a>(
    executions: &'a [Execution],
    table: &str,
    op: &str,
) -> Vec<&'a Execution> {
    let identities: HashSet<&str> = executions
        .iter()
        .filter(|execution| execution.marked(table, op))
        .map(|execution| execution.sql.as_str())
        .collect();
    executions
        .iter()
        .filter(|execution| {
            execution.marked(table, op) || identities.contains(execution.sql.as_str())
        })
        .collect()
}

/// How many statements of the operation marked `table`/`op` the executions
/// carried: one per marker, since the fixture trigger is statement level, so
/// a simple `Query` frame that runs the statement twice counts twice, plus
/// one for each unmarked frame of the same text (a replay the server failed
/// before its triggers ran). This counts statements sent in execution frames;
/// a statement rejected at `Parse` is a [`Rejection`], not a count. A frame
/// whose connection a fault closed stops being observed there, so later
/// statements of that same frame are not counted.
pub fn target_statement_count(executions: &[Execution], table: &str, op: &str) -> usize {
    target_executions(executions, table, op)
        .iter()
        .map(|execution| {
            execution
                .markers
                .iter()
                .filter(|marker| marker.table == table && marker.op == op)
                .count()
                .max(1)
        })
        .sum()
}

/// Executions since a mark that carry a marker for any table other than
/// `table`, that is, writes the fixture observes elsewhere.
pub fn other_marked_executions<'a>(executions: &'a [Execution], table: &str) -> Vec<&'a Execution> {
    executions
        .iter()
        .filter(|execution| execution.markers.iter().any(|marker| marker.table != table))
        .collect()
}

async fn serve(
    mut client: TcpStream,
    upstream: SocketAddr,
    id: u64,
    shared: Arc<Shared>,
) -> Result<()> {
    let mut server = TcpStream::connect(upstream)
        .await
        .context("proxy upstream connect")?;
    client.set_nodelay(true)?;
    server.set_nodelay(true)?;
    // Startup messages carry no type byte: a length, then a version code.
    loop {
        let mut length = [0u8; 4];
        match client.read_exact(&mut length).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        let length = usize::try_from(u32::from_be_bytes(length))?;
        ensure!((8..=10_000).contains(&length), "invalid startup length");
        let mut body = vec![0u8; length - 4];
        client.read_exact(&mut body).await?;
        let code = u32::from_be_bytes(body[..4].try_into().expect("four bytes"));
        let mut frame = Vec::with_capacity(length);
        frame.extend_from_slice(&u32::try_from(length)?.to_be_bytes());
        frame.extend_from_slice(&body);
        match code {
            SSL_REQUEST | GSSENC_REQUEST => client.write_all(b"N").await?,
            CANCEL_REQUEST => {
                server.write_all(&frame).await?;
                return Ok(());
            }
            _ => {
                server.write_all(&frame).await?;
                break;
            }
        }
    }
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    // A frame's kind, length and body are decoded separately. Buffer socket
    // reads so a large result does not pay three reads per DataRow; keep the
    // existing frame-by-frame observation, forwarding and fault boundaries.
    let client_read = BufReader::new(client_read);
    let server_read = BufReader::new(server_read);
    let connection = Arc::new(Mutex::new(Connection::default()));
    // Whichever direction ends first (client EOF, server EOF, or a fault)
    // drops both sockets; the server then aborts any open transaction.
    relay(
        pump_client(
            client_read,
            server_write,
            connection.clone(),
            shared.clone(),
            id,
        ),
        pump_server(server_read, client_write, connection, shared, id),
    )
    .await
}

async fn relay(
    client: impl Future<Output = Result<()>>,
    server: impl Future<Output = Result<ServerEnd>>,
) -> Result<()> {
    tokio::pin!(client);
    tokio::select! {
        result = &mut client => result,
        result = server => match result? {
            ServerEnd::Closed => Ok(()),
            ServerEnd::ClientWrite(error) => {
                // A client can close while a reply is being forwarded, before
                // select polls its readable EOF. Only a frame-boundary EOF
                // observed NOW justifies the same outcome as the client arm.
                // Do not wait for it, accept resets, or hide malformed/partial
                // frames. Polling the original future preserves any buffered
                // COMMIT/ROLLBACK requests and their still-unknown outcomes.
                if error.kind() == std::io::ErrorKind::BrokenPipe {
                    if let Some(result) = client.as_mut().now_or_never() {
                        return result.context("client boundary after failed reply delivery");
                    }
                }
                Err(error).context("forwarding server frame to client")
            }
        },
    }
}

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<(u8, Vec<u8>)>> {
    let mut kind = [0u8; 1];
    match reader.read_exact(&mut kind).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let mut length = [0u8; 4];
    reader.read_exact(&mut length).await?;
    let length = usize::try_from(u32::from_be_bytes(length))?;
    ensure!(length >= 4, "invalid frame length");
    let mut body = vec![0u8; length - 4];
    reader.read_exact(&mut body).await?;
    Ok(Some((kind[0], body)))
}

fn frame(kind: u8, body: &[u8]) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(body.len() + 5);
    bytes.push(kind);
    bytes.extend_from_slice(&u32::try_from(body.len() + 4)?.to_be_bytes());
    bytes.extend_from_slice(body);
    Ok(bytes)
}

fn cstring(body: &[u8], at: &mut usize) -> Result<Vec<u8>> {
    let end = body[*at..]
        .iter()
        .position(|&byte| byte == 0)
        .map(|offset| *at + offset)
        .context("unterminated string in frame")?;
    let value = body[*at..end].to_vec();
    *at = end + 1;
    Ok(value)
}

fn text(bytes: Vec<u8>) -> String {
    String::from_utf8_lossy(&bytes).into_owned()
}

fn record(
    shared: &Shared,
    connection: &Mutex<Connection>,
    id: u64,
    protocol: Protocol,
    sql: String,
) {
    let seq = shared.seq.fetch_add(1, Ordering::SeqCst) + 1;
    let index = {
        let mut state = shared.state.lock().expect("proxy state");
        state.executions.push(Execution {
            seq,
            connection: id,
            protocol,
            sql,
            markers: Vec::new(),
            outcome: Outcome::Pending,
            rows_received: 0,
            jsonb_writes: BTreeMap::new(),
            select_rows: None,
            ready: false,
        });
        state.executions.len() - 1
    };
    connection
        .lock()
        .expect("proxy connection")
        .inflight
        .push_back(index);
}

async fn pump_client(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    connection: Arc<Mutex<Connection>>,
    shared: Arc<Shared>,
    id: u64,
) -> Result<()> {
    while let Some((kind, body)) = read_frame(&mut from).await? {
        let mut at = 0;
        match kind {
            b'P' => {
                let name = cstring(&body, &mut at)?;
                let sql = text(cstring(&body, &mut at)?);
                let mut state = connection.lock().expect("proxy connection");
                state.parses.push_back(sql.clone());
                state.statements.insert(name, sql);
            }
            b'B' => {
                let portal = cstring(&body, &mut at)?;
                let statement = cstring(&body, &mut at)?;
                let mut state = connection.lock().expect("proxy connection");
                let sql = state
                    .statements
                    .get(&statement)
                    .cloned()
                    .unwrap_or_else(|| UNPARSED.to_owned());
                state.portals.insert(portal, sql);
            }
            b'E' => {
                let portal = cstring(&body, &mut at)?;
                let sql = connection
                    .lock()
                    .expect("proxy connection")
                    .portals
                    .get(&portal)
                    .cloned()
                    .unwrap_or_else(|| UNBOUND.to_owned());
                record(&shared, &connection, id, Protocol::Extended, sql);
            }
            b'Q' => {
                let sql = text(cstring(&body, &mut at)?);
                record(&shared, &connection, id, Protocol::Simple, sql);
            }
            b'C' => {
                ensure!(!body.is_empty(), "empty Close frame");
                at = 1;
                let name = cstring(&body, &mut at)?;
                let mut state = connection.lock().expect("proxy connection");
                match body[0] {
                    b'S' => {
                        state.statements.remove(&name);
                    }
                    b'P' => {
                        state.portals.remove(&name);
                    }
                    _ => {}
                }
            }
            b'X' => ensure!(body.is_empty(), "invalid Terminate frame"),
            _ => {}
        }
        to.write_all(&frame(kind, &body)?).await?;
    }
    Ok(())
}

enum Action {
    Forward,
    Complete {
        index: usize,
    },
    Sever,
    Pause {
        index: usize,
        control: Arc<CommitPauseState>,
    },
}

enum ServerEnd {
    Closed,
    ClientWrite(std::io::Error),
}

async fn pump_server(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    connection: Arc<Mutex<Connection>>,
    shared: Arc<Shared>,
    id: u64,
) -> Result<ServerEnd> {
    while let Some((kind, body)) = read_frame(&mut from).await? {
        let action = match kind {
            b'C' => {
                let mut at = 0;
                let tag = text(cstring(&body, &mut at)?);
                complete(&shared, &connection, id, tag)?
            }
            b'I' => complete(&shared, &connection, id, String::new())?,
            b'D' => {
                let index = front(&connection).context("DataRow without an observed execution")?;
                let mut state = shared.state.lock().expect("proxy state");
                let count = &mut state.executions[index].rows_received;
                *count = count.checked_add(1).context("DataRow counter overflow")?;
                Action::Forward
            }
            b'1' => {
                connection
                    .lock()
                    .expect("proxy connection")
                    .parses
                    .pop_front();
                Action::Forward
            }
            b'E' => {
                let (code, message) = fields(&body)?;
                fail(&shared, &connection, id, code, message);
                Action::Forward
            }
            b'N' => {
                let (_, message) = fields(&body)?;
                notice(&shared, &connection, &message)?;
                Action::Forward
            }
            _ => Action::Forward,
        };
        let completed = match action {
            Action::Forward => None,
            Action::Complete { index } => Some(index),
            Action::Sever => return Ok(ServerEnd::Closed),
            Action::Pause { index, control } => {
                // No observer lock or PostgreSQL transaction lock is held here.
                loop {
                    let release = control.release.notified();
                    tokio::pin!(release);
                    release.as_mut().enable();
                    if control.released.load(Ordering::SeqCst) {
                        break;
                    }
                    release.await;
                }
                Some(index)
            }
        };
        if let Err(error) = to.write_all(&frame(kind, &body)?).await {
            return Ok(ServerEnd::ClientWrite(error));
        }
        if let Some(index) = completed {
            let mut state = shared.state.lock().expect("proxy state");
            if let Outcome::Completed { delivered, .. } = &mut state.executions[index].outcome {
                *delivered = true;
            }
        }
        if kind == b'Z' {
            ready(&shared, &connection, id, body.first().copied());
        }
    }
    Ok(ServerEnd::Closed)
}

/// SQLSTATE and message of an ErrorResponse or NoticeResponse.
fn fields(body: &[u8]) -> Result<(String, String)> {
    let mut code = String::new();
    let mut message = String::new();
    let mut at = 0;
    while at < body.len() && body[at] != 0 {
        let kind = body[at];
        at += 1;
        let value = text(cstring(body, &mut at)?);
        match kind {
            b'C' => code = value,
            b'M' => message = value,
            _ => {}
        }
    }
    Ok((code, message))
}

fn front(connection: &Mutex<Connection>) -> Option<usize> {
    connection
        .lock()
        .expect("proxy connection")
        .inflight
        .front()
        .copied()
}

fn pop(connection: &Mutex<Connection>) {
    connection
        .lock()
        .expect("proxy connection")
        .inflight
        .pop_front();
}

fn complete(
    shared: &Shared,
    connection: &Mutex<Connection>,
    id: u64,
    tag: String,
) -> Result<Action> {
    let Some(index) = front(connection) else {
        return Ok(Action::Forward);
    };
    let mut state = shared.state.lock().expect("proxy state");
    let State {
        executions,
        rejections: _,
        plan,
        armed,
        fired,
        pause_plan,
        pause_armed,
    } = &mut *state;
    let execution = &mut executions[index];
    if let Some(count) = tag.strip_prefix("SELECT ") {
        let count: u64 = count.parse().context("invalid SELECT completion count")?;
        execution.select_rows = Some(
            execution
                .select_rows
                .unwrap_or(0)
                .checked_add(count)
                .context("SELECT completion counter overflow")?,
        );
    }
    let protocol = execution.protocol;
    let mut sever = false;
    if let Some(fault) = plan.as_ref() {
        let marked = execution.marked(&fault.table, &fault.op);
        match fault.phase {
            FaultPhase::AfterExecution if marked => sever = true,
            FaultPhase::AfterCommit if marked => *armed = Some(id),
            FaultPhase::AfterCommit if *armed == Some(id) && tag == "COMMIT" => sever = true,
            _ => {}
        }
    }
    let mut pause = None;
    if !sever {
        if let Some(plan) = pause_plan.as_ref() {
            if execution.marked(&plan.table, &plan.op) {
                *pause_armed = Some(id);
            } else if *pause_armed == Some(id) && tag == "COMMIT" {
                pause = Some(plan.control.clone());
                *pause_plan = None;
                *pause_armed = None;
            }
        }
    }
    execution.outcome = Outcome::Completed {
        tag,
        delivered: false,
    };
    if let Some(control) = &pause {
        control.seq.store(execution.seq, Ordering::SeqCst);
        control.entered.notify_waiters();
    }
    if sever {
        *fired = Some(execution.seq);
        *plan = None;
        *armed = None;
    }
    drop(state);
    if protocol == Protocol::Extended {
        pop(connection);
    }
    if sever {
        Ok(Action::Sever)
    } else if let Some(control) = pause {
        Ok(Action::Pause { index, control })
    } else {
        Ok(Action::Complete { index })
    }
}

/// An `ErrorResponse` answers the oldest unanswered client frame. A client
/// sends `Parse` ahead of the `Bind` and `Execute` that use it, so an
/// unanswered `Parse` is the rejected frame; otherwise it is the oldest
/// in-flight execution, if any.
fn fail(shared: &Shared, connection: &Mutex<Connection>, id: u64, code: String, message: String) {
    let (parse, index) = {
        let mut state = connection.lock().expect("proxy connection");
        match state.parses.pop_front() {
            Some(sql) => (Some(sql), None),
            None => (None, state.inflight.pop_front()),
        }
    };
    let seq = shared.seq.fetch_add(1, Ordering::SeqCst) + 1;
    let mut state = shared.state.lock().expect("proxy state");
    let frame = match (parse, index) {
        (Some(sql), _) => RejectedFrame::Parse(sql),
        (None, Some(index)) => {
            let execution = &mut state.executions[index];
            execution.outcome = Outcome::Failed {
                code: code.clone(),
                message: message.clone(),
            };
            RejectedFrame::Execution(execution.seq)
        }
        (None, None) => RejectedFrame::Other,
    };
    state.rejections.push(Rejection {
        seq,
        connection: id,
        code,
        message,
        frame,
    });
}

fn notice(shared: &Shared, connection: &Mutex<Connection>, message: &str) -> Result<()> {
    if let Some(encoded) = message.strip_prefix(JSONB_WRITE_PREFIX) {
        let notice: JsonbWriteNotice =
            serde_json::from_str(encoded).context("invalid JSONB write observation")?;
        ensure!(
            !notice.table.is_empty()
                && matches!(notice.operation.as_str(), "INSERT" | "UPDATE")
                && notice.values.values().all(|bytes| *bytes > 0),
            "invalid JSONB write measurement"
        );
        let index = front(connection).context("JSONB write without an observed execution")?;
        let mut state = shared.state.lock().expect("proxy state");
        let summary = state.executions[index]
            .jsonb_writes
            .entry((notice.table, notice.operation))
            .or_default();
        summary.rows = summary
            .rows
            .checked_add(1)
            .context("JSONB row counter overflow")?;
        summary.values = summary
            .values
            .checked_add(notice.values.len().try_into()?)
            .context("JSONB value counter overflow")?;
        summary.max_uncompressed_bytes = summary
            .max_uncompressed_bytes
            .max(notice.values.values().copied().max().unwrap_or(0));
        return Ok(());
    }
    let Some(rest) = message.strip_prefix(MARKER_PREFIX) else {
        return Ok(());
    };
    let mut words = rest.split_whitespace();
    let (Some(table), Some(op)) = (words.next(), words.next()) else {
        return Ok(());
    };
    let Some(index) = front(connection) else {
        return Ok(());
    };
    shared.state.lock().expect("proxy state").executions[index]
        .markers
        .push(Marker {
            table: table.to_owned(),
            op: op.to_owned(),
        });
    Ok(())
}

fn ready(shared: &Shared, connection: &Mutex<Connection>, id: u64, status: Option<u8>) {
    {
        // Every frame before this `ReadyForQuery` has been answered, or was
        // skipped by the server after an earlier error in the same batch; a
        // skipped execution stays `Pending` and nothing later settles it.
        let mut state = connection.lock().expect("proxy connection");
        let mut observations = shared.state.lock().expect("proxy state");
        for index in &state.inflight {
            observations.executions[*index].ready = true;
        }
        state.parses.clear();
        state.inflight.clear();
    }
    if status == Some(b'I') {
        // The transaction ended without the COMMIT the fault was waiting for
        // (a rollback): the arming no longer applies to this connection.
        let mut state = shared.state.lock().expect("proxy state");
        if state.armed == Some(id) && state.fired.is_none() {
            state.armed = None;
        }
        if state.pause_armed == Some(id) {
            state.pause_armed = None;
        }
    }
}

#[cfg(test)]
#[path = "ledger_execution_proxy_close.rs"]
mod close_tests;
