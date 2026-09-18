//! Stratum bind order on restart (#460).
//!
//! `run` binds the primary Stratum listener, and the high difficulty one when
//! `PRISM_STRATUM_HIGHDIFF_PORT` is set, before `Coordinator::new`. The order
//! is the property: `Coordinator::new` asks the node for its chain and then
//! registers the instance with a `starting` row in `qbit_prism_instances`,
//! and nothing retracts that row when `run` returns an error. A restart that
//! loses the bind race to its predecessor must therefore fail before it has
//! written anything, or it leaves a row behind that `fatal-state clear`
//! refuses on until an operator deletes it by hand.
//!
//! The child is the real server binary (`env!("CARGO_BIN_EXE_qbit-prism-server")`
//! `run`), configured only through `PRISM_*` and `QBIT_*` variables, against
//! a fake node served here, in the parent, and a PostgreSQL database of its
//! own (`support/ledger_database.rs`, #410). The harness is the one
//! `candidate_storm_restart.rs` uses; see that file for why the node answers
//! `getblocktemplate` with an error and why the parent reads the database
//! through `Ledger::connect_tool`, which registers no instance of its own, so
//! every `qbit_prism_instances` row here belongs to a child.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test stratum_restart_bind -- --nocapture
//! ```
//!
//! # Ports
//!
//! The primary listener takes `PRISM_STRATUM_PORT=0` and announces the port it
//! bound in its "PRISM listening" line, and a port this test must hold is one
//! it bound itself at port 0 and keeps open, so neither ever races another
//! process for a free port. The high difficulty listener refuses port 0, so
//! the one test that needs it bound picks a free port, releases it and passes
//! it on; that test alone retries when something else took the port first.
//!
//! # Not covered here
//!
//! The 2.x.x bind retry (retry until the predecessor releases the port, abort
//! the retry on shutdown, fail fast with zero retries) has no native subject:
//! native `run` binds once. Nor does a shutdown during the node readiness
//! wait: no signal handler is installed until every task is spawned, so a
//! `SIGTERM` inside `Coordinator::new` is the default disposition.

use anyhow::{bail, ensure, Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism_server::ledger::Ledger;
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use std::{
    net::{SocketAddr, TcpListener},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, sync::watch, task::JoinHandle};

#[allow(dead_code)]
#[path = "support/ledger_database.rs"]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// The instance ID every child registers under: each child is a restart of
/// one frontend, not the arrival of a second one.
const INSTANCE_ID: &str = "stratum-restart-frontend";
/// The error context `run` gives each Stratum bind.
const PRIMARY_BIND: &str = "bind primary Stratum listener";
const HIGHDIFF_BIND: &str = "bind high difficulty Stratum listener";
/// The operating system's text for `EADDRINUSE`.
const ADDRESS_IN_USE: &str = "Address already in use";
/// How often a bounded wait looks again.
const POLL: Duration = Duration::from_millis(50);
/// The hang guard for one wait. Nothing compares an elapsed time to it to
/// decide whether a property holds; it only fails a wedged run with the
/// child's stderr instead of hanging the suite.
const DEADLINE: Duration = Duration::from_secs(120);
/// The port `run` gives the high difficulty listener when none is set.
const HIGHDIFF_DEFAULT_PORT: u16 = 4334;
/// How many ports the high difficulty test may pick before it gives up.
const HIGHDIFF_ATTEMPTS: usize = 5;
/// The error the fixture's node answers `getblocktemplate` with, which keeps
/// the refresh loop out of every test here.
const NO_TEMPLATE: &str = "this fixture's node serves no mining template";

// ---------------------------------------------------------------------------
// The fake node, in the parent, able to hold one `getblockchaininfo` reply.
// ---------------------------------------------------------------------------

struct NodeShared {
    /// Whether the next `getblockchaininfo` call is held until `release`.
    hold_next: AtomicBool,
    /// Set once a held call has arrived.
    held: watch::Sender<bool>,
    /// Set by the test to answer the held call.
    released: watch::Sender<bool>,
}

struct FakeNode {
    url: String,
    shared: Arc<NodeShared>,
    task: JoinHandle<()>,
}

impl Drop for FakeNode {
    fn drop(&mut self) {
        // A held call must not outlive the test either.
        self.shared.released.send_replace(true);
        self.task.abort();
    }
}

impl FakeNode {
    /// `hold` holds the node's first `getblockchaininfo` reply, which
    /// `Coordinator::new` waits on before it registers the instance.
    async fn open(hold: bool) -> Result<Self> {
        let shared = Arc::new(NodeShared {
            hold_next: AtomicBool::new(hold),
            held: watch::channel(false).0,
            released: watch::channel(false).0,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(node_reply))
            .with_state(shared.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { url, shared, task })
    }

    /// Wait until the child's `getblockchaininfo` call is being held.
    async fn wait_held(&self, child: &mut ServerChild) -> Result<()> {
        let mut held = self.shared.held.subscribe();
        let started = Instant::now();
        loop {
            if *held.borrow_and_update() {
                return Ok(());
            }
            child.ensure_running("before the node held its getblockchaininfo call")?;
            ensure!(
                started.elapsed() < DEADLINE,
                "the child sent no getblockchaininfo call within {DEADLINE:?}; child stderr:\n{}",
                child.stderr_tail()
            );
            let _ = tokio::time::timeout(POLL, held.changed()).await;
        }
    }

    fn release(&self) {
        self.shared.released.send_replace(true);
    }
}

async fn node_reply(
    State(node): State<Arc<NodeShared>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let id = request["id"].clone();
    let tip = "22".repeat(32);
    Json(match request["method"].as_str().unwrap_or_default() {
        "getblockhash" if request["params"][0] == 0 => ok(&id, json!("00".repeat(32))),
        "getblockhash" | "getbestblockhash" => ok(&id, json!(tip)),
        "getblockchaininfo" => {
            if node.hold_next.swap(false, Ordering::SeqCst) {
                node.held.send_replace(true);
                let _ = node
                    .released
                    .subscribe()
                    .wait_for(|released| *released)
                    .await;
            }
            ok(
                &id,
                json!({"chain":"test","initialblockdownload":false,"blocks":900_000,
                       "headers":900_000,"bestblockhash":tip,"chainwork":"01"}),
            )
        }
        "getblockheader" => ok(&id, json!({"previousblockhash":"cd".repeat(32)})),
        "getnetworkinfo" => ok(&id, json!({"connections":2})),
        "getblocktemplate" => rpc_error(&id, -10, NO_TEMPLATE),
        other => rpc_error(&id, -32601, &format!("unexpected RPC {other}")),
    })
}

fn ok(id: &Value, result: Value) -> Value {
    json!({"id":id,"result":result,"error":null})
}

fn rpc_error(id: &Value, code: i64, message: &str) -> Value {
    json!({"id":id,"result":null,"error":{"code":code,"message":message}})
}

// ---------------------------------------------------------------------------
// The child: the real server binary, configured only through PRISM_*/QBIT_*.
// ---------------------------------------------------------------------------

/// How a child's startup ended, as far as the parent can see it.
enum Startup {
    /// The child published its health, so both Stratum listeners are bound
    /// and every task is running.
    Serving,
    /// The child exited without registering.
    Exited(ExitStatus),
}

struct ServerChild {
    child: Child,
    /// The child's stderr: the "PRISM listening" line, the error `run`
    /// returned, and the reason a deadline expired.
    log: tempfile::NamedTempFile,
}

impl Drop for ServerChild {
    /// Every path that leaves a test, a failed assertion and a panic
    /// included, kills and reaps the child. After a graceful stop both calls
    /// find the child already reaped and do nothing.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl ServerChild {
    /// Spawn `qbit-prism-server run` with the primary listener on
    /// `primary_port` and the high difficulty listener on `highdiff_port`,
    /// or without one. Every `PRISM_*` and `QBIT_*` name the harness might
    /// hold is removed first, so the child's configuration is exactly what is
    /// set here.
    fn spawn(
        database_url: &str,
        node_url: &str,
        primary_port: u16,
        highdiff_port: Option<u16>,
    ) -> Result<Self> {
        let log = tempfile::NamedTempFile::new()?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
        for (key, _) in std::env::vars()
            .filter(|(key, _)| key.starts_with("PRISM_") || key.starts_with("QBIT_"))
        {
            command.env_remove(key);
        }
        let ledger_seed = "22".repeat(32);
        command
            .arg("run")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log.reopen()?))
            // The listening line is the only place the bound primary port is
            // announced; plain text keeps it parseable.
            .env("RUST_LOG", "warn,qbit_prism_server::server=info")
            .env("NO_COLOR", "1")
            .env("PRISM_DATABASE_URL", database_url)
            .env("PRISM_INSTANCE_ID", INSTANCE_ID)
            .env("PRISM_DATABASE_MAX_CONNECTIONS", "8")
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("PRISM_JOB_BUILD_EXECUTOR_WORKERS", "2")
            .env("QBIT_CHAIN", "testnet")
            .env("QBIT_RPC_URL", node_url)
            .env("PRISM_MIN_PEERS", "1")
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_MANIFEST_SIGNING_SEED_HEX", "11".repeat(32))
            .env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX", &ledger_seed)
            .env(
                "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX",
                ManifestSigningKey::from_seed_hex(&ledger_seed)?.public_key_hex(),
            )
            .env("PRISM_STRATUM_BIND", "127.0.0.1")
            .env("PRISM_STRATUM_PORT", primary_port.to_string())
            // Port 0 disables the audit listener, so every listening socket
            // the child holds is a Stratum one.
            .env("PRISM_AUDIT_PORT", "0")
            // A held `getblockchaininfo` reply must not time out while the
            // test inspects the child.
            .env("PRISM_RPC_TIMEOUT_SECONDS", "600")
            .env("PRISM_BLOCKWAIT_ENABLED", "0")
            .env("PRISM_HASHRATE_ROLLUP_ENABLED", "0")
            .env("PRISM_BLOCKPOLL_SECONDS", "5");
        if let Some(port) = highdiff_port {
            command.env("PRISM_STRATUM_HIGHDIFF_PORT", port.to_string());
        }
        Ok(Self {
            child: command.spawn().context("spawning the server binary")?,
            log,
        })
    }

    fn stderr(&self) -> String {
        String::from_utf8_lossy(&std::fs::read(self.log.path()).unwrap_or_default()).into_owned()
    }

    /// The tail of the child's stderr, for a failure message.
    fn stderr_tail(&self) -> String {
        let stderr = self.stderr();
        let mut from = stderr.len().saturating_sub(4096);
        while !stderr.is_char_boundary(from) {
            from += 1;
        }
        stderr[from..].to_owned()
    }

    fn ensure_running(&mut self, when: &str) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            bail!(
                "the child exited {when} with {status}; child stderr:\n{}",
                self.stderr_tail()
            );
        }
        Ok(())
    }

    async fn wait_exit(&mut self) -> Result<ExitStatus> {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            ensure!(
                started.elapsed() < DEADLINE,
                "the child did not exit within {DEADLINE:?}; child stderr:\n{}",
                self.stderr_tail()
            );
            tokio::time::sleep(POLL).await;
        }
    }

    /// Wait until the child either publishes its health or exits.
    ///
    /// Registration alone is not enough to stop a child by: `run` installs
    /// its `SIGTERM` handler only after every task is spawned, so a signal
    /// sent while the row is still `starting` kills the child outright. The
    /// health publisher is spawned in that same run of statements and needs
    /// a database round trip before its row appears.
    async fn startup(&mut self, ledger: &Ledger) -> Result<Startup> {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(Startup::Exited(status));
            }
            if instances(ledger)
                .await?
                .iter()
                .any(|(id, state)| id == INSTANCE_ID && state.is_none())
            {
                return Ok(Startup::Serving);
            }
            ensure!(
                started.elapsed() < DEADLINE,
                "the child neither published its health nor exited within {DEADLINE:?}; child stderr:\n{}",
                self.stderr_tail()
            );
            tokio::time::sleep(POLL).await;
        }
    }

    async fn serving(&mut self, ledger: &Ledger) -> Result<()> {
        match self.startup(ledger).await? {
            Startup::Serving => Ok(()),
            Startup::Exited(status) => bail!(
                "the child exited with {status} before it served; child stderr:\n{}",
                self.stderr_tail()
            ),
        }
    }

    /// Wait for a failed startup: the child exits with status 1, the code
    /// `main` returns an error with, and not on a signal.
    async fn failed(&mut self) -> Result<()> {
        let status = self.wait_exit().await?;
        ensure!(
            status.code() == Some(1),
            "the child exited with {status}, not with the error exit code 1; child stderr:\n{}",
            self.stderr_tail()
        );
        Ok(())
    }

    /// `SIGTERM`, then a clean exit and the `stopped` marker: the child
    /// served until it was told to stop.
    async fn stop(&mut self, ledger: &Ledger) -> Result<()> {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()?;
        ensure!(status.success(), "could not signal the child");
        let status = self.wait_exit().await?;
        ensure!(
            status.success(),
            "the child exited with {status} after SIGTERM; child stderr:\n{}",
            self.stderr_tail()
        );
        let rows = instances(ledger).await?;
        ensure!(
            rows == [(INSTANCE_ID.to_owned(), Some("stopped".to_owned()))],
            "after a graceful stop qbit_prism_instances holds {rows:?}, not one stopped {INSTANCE_ID} row"
        );
        Ok(())
    }

    /// The primary listener's address, from the child's "PRISM listening"
    /// line.
    fn listening_address(&self) -> Result<SocketAddr> {
        let stderr = strip_ansi(&self.stderr());
        let line = stderr
            .lines()
            .find(|line| line.contains("PRISM listening"))
            .with_context(|| {
                format!(
                    "the child logged no PRISM listening line; child stderr:\n{}",
                    self.stderr_tail()
                )
            })?;
        line.split_whitespace()
            .find_map(|field| field.strip_prefix("address="))
            .with_context(|| format!("no address in {line:?}"))?
            .parse()
            .with_context(|| format!("no socket address in {line:?}"))
    }
}

/// Drops terminal escape sequences, should the child's formatter emit any.
fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            plain.push(c);
        }
    }
    plain
}

/// Every `qbit_prism_instances` row as `(instance_id, state)`, where `state`
/// is the heartbeat's lifecycle marker and `None` for a health payload.
async fn instances(ledger: &Ledger) -> Result<Vec<(String, Option<String>)>> {
    Ok(sqlx::query_as(
        "SELECT instance_id,status->>'state' FROM qbit_prism_instances ORDER BY instance_id",
    )
    .fetch_all(&ledger.pool)
    .await?)
}

async fn ensure_no_instances(ledger: &Ledger, when: &str) -> Result<()> {
    let rows = instances(ledger).await?;
    ensure!(
        rows.is_empty(),
        "{when}: qbit_prism_instances holds {rows:?}; a startup that failed on its Stratum bind must write no row, and `fatal-state clear` refuses any row that is not drained or stopped"
    );
    Ok(())
}

async fn connect(address: SocketAddr, child: &ServerChild) -> Result<()> {
    tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .context("connect timed out")?
        .with_context(|| {
            format!(
                "connecting to {address} failed; child stderr:\n{}",
                child.stderr_tail()
            )
        })?;
    Ok(())
}

fn ensure_bind_failure(child: &ServerChild, context: &str) -> Result<()> {
    let stderr = child.stderr();
    ensure!(
        stderr.contains(context) && stderr.contains(ADDRESS_IN_USE),
        "the child's error is not {context:?} with {ADDRESS_IN_USE:?}; child stderr:\n{}",
        child.stderr_tail()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The fixture: one PostgreSQL database and one tool ledger.
// ---------------------------------------------------------------------------

struct Fixture {
    database: FixtureDatabase,
    /// The parent's tool connection. It initializes the schema, so no child
    /// runs a migration, and it registers no instance.
    ledger: Ledger,
}

impl Fixture {
    async fn open(raw: &str) -> Result<Self> {
        let database = FixtureDatabase::open(raw, "prism_stratum_bind_").await?;
        match Ledger::connect_tool(&database.url, "stratum-bind-fixture".into(), 4, true, None)
            .await
        {
            Ok(ledger) => Ok(Self { database, ledger }),
            Err(error) => Err(database.abandon(error).await),
        }
    }

    async fn close(self, outcome: Result<()>) -> Result<()> {
        self.ledger.pool.close().await;
        self.database.close(outcome).await
    }
}

// ---------------------------------------------------------------------------
// A predecessor still holds a port.
// ---------------------------------------------------------------------------

/// The 2.x.x "bound before recovery" case: a restart whose primary port is
/// still held exits with the bind error and leaves no instance row, so a
/// later start on a free port finds nothing in its way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_primary_port_fails_startup_before_registration() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let fixture = Fixture::open(&raw).await?;
    let outcome = held_primary_port(&fixture).await;
    fixture.close(outcome).await
}

async fn held_primary_port(fixture: &Fixture) -> Result<()> {
    let node = FakeNode::open(false).await?;
    // The predecessor: held for the whole test.
    let predecessor = TcpListener::bind("127.0.0.1:0")?;
    let held = predecessor.local_addr()?.port();
    let mut child = ServerChild::spawn(&fixture.database.url, &node.url, held, None)?;
    child.failed().await?;
    ensure_bind_failure(&child, PRIMARY_BIND)?;
    ensure_no_instances(&fixture.ledger, "after the lost primary bind").await?;
    drop(child);

    // What the empty table buys: the next start, on a free port, registers
    // and serves as if the failed one had never run.
    let mut child = ServerChild::spawn(&fixture.database.url, &node.url, 0, None)?;
    child.serving(&fixture.ledger).await?;
    connect(child.listening_address()?, &child).await?;
    child.stop(&fixture.ledger).await?;
    drop(predecessor);
    Ok(())
}

/// The same for the high difficulty listener: its bind also precedes
/// registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_highdiff_port_fails_startup_before_registration() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let fixture = Fixture::open(&raw).await?;
    let outcome = held_highdiff_port(&fixture).await;
    fixture.close(outcome).await
}

async fn held_highdiff_port(fixture: &Fixture) -> Result<()> {
    let node = FakeNode::open(false).await?;
    let predecessor = TcpListener::bind("127.0.0.1:0")?;
    let held = predecessor.local_addr()?.port();
    let mut child = ServerChild::spawn(&fixture.database.url, &node.url, 0, Some(held))?;
    child.failed().await?;
    ensure_bind_failure(&child, HIGHDIFF_BIND)?;
    ensure_no_instances(&fixture.ledger, "after the lost high difficulty bind").await?;
    drop(child);
    drop(predecessor);
    Ok(())
}

// ---------------------------------------------------------------------------
// The node is slow.
// ---------------------------------------------------------------------------

/// The 2.x.x "listeners accept during recovery" case: while the node holds
/// `Coordinator::new` in its chain check, the primary listener is already
/// bound and the instance is not yet registered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listeners_are_bound_while_the_node_is_slow() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let fixture = Fixture::open(&raw).await?;
    let outcome = slow_node(&fixture).await;
    fixture.close(outcome).await
}

async fn slow_node(fixture: &Fixture) -> Result<()> {
    let node = FakeNode::open(true).await?;
    let mut child = ServerChild::spawn(&fixture.database.url, &node.url, 0, None)?;
    node.wait_held(&mut child).await?;
    // Read once, not polled: the line is written before `Coordinator::new`
    // sends its first RPC, so it is in the file by the time the node holds
    // the second.
    let address = child.listening_address()?;
    // Only the connection is asserted. No accept loop runs until the
    // coordinator is up, so the kernel's backlog completes the handshake and
    // no Stratum byte could come back yet.
    connect(address, &child).await?;
    ensure_no_instances(&fixture.ledger, "while the node held getblockchaininfo").await?;
    child.ensure_running("while the node held getblockchaininfo")?;

    node.release();
    child.serving(&fixture.ledger).await?;
    child.stop(&fixture.ledger).await
}

// ---------------------------------------------------------------------------
// The high difficulty listener, absent and present.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_highdiff_port_only_the_primary_listener_is_bound() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let fixture = Fixture::open(&raw).await?;
    let outcome = highdiff_absent(&fixture).await;
    fixture.close(outcome).await
}

async fn highdiff_absent(fixture: &Fixture) -> Result<()> {
    let node = FakeNode::open(false).await?;
    // A high difficulty bind the child was not asked for would take the
    // default port on the primary's address, and it cannot: this test holds
    // that port, or another process already does. The child sets itself
    // undumpable, so its sockets cannot be listed from /proc instead.
    let _default = match TcpListener::bind(("127.0.0.1", HIGHDIFF_DEFAULT_PORT)) {
        Ok(listener) => Some(listener),
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => None,
        Err(error) => return Err(error.into()),
    };
    let mut child = ServerChild::spawn(&fixture.database.url, &node.url, 0, None)?;
    child.serving(&fixture.ledger).await?;
    connect(child.listening_address()?, &child).await?;
    child.stop(&fixture.ledger).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_configured_highdiff_port_is_bound_and_accepts() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let fixture = Fixture::open(&raw).await?;
    let outcome = highdiff_present(&fixture).await;
    fixture.close(outcome).await
}

async fn highdiff_present(fixture: &Fixture) -> Result<()> {
    let node = FakeNode::open(false).await?;
    for attempt in 1..=HIGHDIFF_ATTEMPTS {
        // The one pick-then-bind in this file: the high difficulty listener
        // refuses port 0. Another process can take the port in between, so
        // that loss, and only that loss, is retried.
        let port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let mut child = ServerChild::spawn(&fixture.database.url, &node.url, 0, Some(port))?;
        match child.startup(&fixture.ledger).await? {
            Startup::Exited(status) => {
                ensure!(
                    status.code() == Some(1),
                    "the child exited with {status}; child stderr:\n{}",
                    child.stderr_tail()
                );
                ensure_bind_failure(&child, HIGHDIFF_BIND)?;
                ensure_no_instances(&fixture.ledger, "after a lost high difficulty bind").await?;
                eprintln!("attempt {attempt}: port {port} was taken before the child bound it");
            }
            Startup::Serving => {
                connect(child.listening_address()?, &child).await?;
                connect(SocketAddr::from(([127, 0, 0, 1], port)), &child).await?;
                return child.stop(&fixture.ledger).await;
            }
        }
    }
    bail!(
        "another process took the picked high difficulty port on all {HIGHDIFF_ATTEMPTS} attempts"
    )
}
