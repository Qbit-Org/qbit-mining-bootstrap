//! Opt-in native-process end-to-end tests against a real qbit regtest node.
//! QBITD_BIN=/path/to/qbitd PRISM_TEST_DATABASE_URL=postgres://... cargo test -p qbit-prism-server --test live_regtest -- --nocapture
use anyhow::{bail, ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{verify_audit_bundle_against_coinbase_tx_hex, AuditBundle};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::{
    fs::File,
    future::Future,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

#[path = "support/live_highdiff.rs"]
mod highdiff_tests;

#[path = "support/live_ctv_cpfp.rs"]
mod cpfp_tests;

/// Each fixture starts a regtest `qbitd` and two servers, and a server binds
/// its listeners only after coordinator startup: the schema migrations, which
/// the second server of a fixture waits for under the migrations table lock,
/// and the node handshake. libtest ran two fixtures at once on the two-vCPU CI
/// runner, and the readiness wait below, a 30-second setup budget rather than
/// behaviour under test, timed out four times on one branch while the sibling
/// fixture mined and restarted its node; the failover test's 20-second
/// reconnection wait timed out twice more under the same load, and every rerun
/// passed. The tests of this binary therefore run one at a time, as the ledger
/// suites do: the guard lives in the fixture, so the next one opens only after
/// cleanup has stopped every process.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Process {
    child: ChildHandle,
    log: PathBuf,
    /// Credential and signing values from the command's own arguments and
    /// explicit environment, removed from its diagnostics.
    secrets: Vec<String>,
}

/// A child whose exit status can be observed through a shared reference, so
/// diagnostics can report it from `&Fixture`.
struct ChildHandle(Mutex<Child>);
impl ChildHandle {
    fn lock(&self) -> MutexGuard<'_, Child> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
    fn try_wait(&self) -> std::io::Result<Option<ExitStatus>> {
        self.lock().try_wait()
    }
}

impl Process {
    fn spawn(command: &mut Command, path: PathBuf) -> Result<Self> {
        let secrets = diagnostics::command_secrets(command);
        let log = File::create(&path)?;
        let child = command
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        Ok(Self {
            child: ChildHandle(Mutex::new(child)),
            log: path,
            secrets,
        })
    }
    fn stop(&mut self) {
        let mut child = self.child.lock();
        let _ = child.kill();
        let _ = child.wait();
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    admin: PgPool,
    pool: PgPool,
    schema: String,
    database_url: String,
    rpc_port: u16,
    stratum: [u16; 2],
    highdiff: [u16; 2],
    api: [u16; 2],
    servers: Vec<Process>,
    miners: Vec<Process>,
    node: Process,
    /// The `qbitd` executable the node was started from, for restarts.
    qbitd: String,
    /// The PRISM server executable each server process is started from.
    server: PathBuf,
    client: reqwest::Client,
    address: String,
    ctv: bool,
    /// Declared last, so it is released after the processes and pools above.
    _serial: tokio::sync::MutexGuard<'static, ()>,
}

/// The child executables a fixture starts.
struct Launch {
    qbitd: String,
    server: PathBuf,
    ctv: bool,
}

/// The fixture's database handles, created before any child starts.
struct Database {
    admin: PgPool,
    pool: PgPool,
    schema: String,
    database_url: String,
}

/// What a startup attempt owns before its fixture is complete. On failure,
/// child evidence is captured from here before the children are stopped and
/// the temporary directory is removed.
#[derive(Default)]
struct Startup {
    directory: Option<tempfile::TempDir>,
    database_url: Option<String>,
    node: Option<Process>,
    fixture: Option<Fixture>,
    /// The `SERIAL` guard until the fixture takes it. Declared last, so a
    /// failed startup releases it only after its children are stopped.
    serial: Option<tokio::sync::MutexGuard<'static, ()>>,
}

impl Startup {
    fn finish(mut self, result: Result<()>) -> Result<Fixture> {
        match result {
            Ok(()) => self
                .fixture
                .take()
                .context("startup completed without a fixture"),
            Err(error) => Err(self.fail(error)),
        }
    }

    /// Keeps `error` as the root cause and attaches the child report, which
    /// is collected before `self` drops so a log read, status query or
    /// collector panic cannot replace the original failure.
    fn fail(self, error: anyhow::Error) -> anyhow::Error {
        let report = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.report()))
            .unwrap_or_else(|_| "child diagnostics unavailable: the collector panicked".into());
        // Dropping stops every started child and removes the temporary directory.
        drop(self);
        error.context(format!(
            "live fixture startup failed; child diagnostics captured before cleanup:\n{report}"
        ))
    }

    fn report(&self) -> String {
        if let Some(fixture) = &self.fixture {
            return fixture.diagnostics();
        }
        let children = [
            ("qbit node".to_owned(), self.node.as_ref()),
            ("server-0".to_owned(), None),
            ("server-1".to_owned(), None),
        ];
        diagnostics::report(&children, self.database_url.as_deref())
    }
}

fn free_port() -> Result<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}
async fn until<F, Fut>(label: &str, seconds: u64, mut condition: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let started = Instant::now();
    let mut last = None;
    loop {
        match condition().await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => last = Some(error),
        }
        if started.elapsed() > Duration::from_secs(seconds) {
            bail!(
                "timed out waiting for {label}: {}",
                last.map_or_else(|| "condition not met".into(), |e| e.to_string())
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

impl Fixture {
    async fn open(ctv: bool) -> Result<Option<Self>> {
        let Some((qbitd, database)) = gate::qbitd_and_database_url(gate::site!())? else {
            return Ok(None);
        };
        let launch = Launch {
            qbitd,
            server: env!("CARGO_BIN_EXE_qbit-prism-server").into(),
            ctv,
        };
        let mut startup = Startup {
            serial: Some(SERIAL.lock().await),
            ..Startup::default()
        };
        let result = Self::start(&database, launch, &mut startup).await;
        startup.finish(result).map(Some)
    }

    async fn start(database: &str, launch: Launch, startup: &mut Startup) -> Result<()> {
        startup.directory = Some(tempfile::tempdir()?);
        let admin = PgPool::connect(database).await?;
        let schema = format!("prism_live_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(database)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let database_url = url.to_string();
        let pool = PgPool::connect(&database_url).await?;
        let database = Database {
            admin,
            pool,
            schema,
            database_url,
        };
        Self::start_children(database, launch, startup).await
    }

    /// Starts the node and both servers, keeping each started child in
    /// `startup` so a failure at any later step can still report it.
    async fn start_children(
        database: Database,
        launch: Launch,
        startup: &mut Startup,
    ) -> Result<()> {
        let directory = startup
            .directory
            .as_ref()
            .context("startup directory missing")?
            .path()
            .to_path_buf();
        startup.database_url = Some(database.database_url.clone());
        let rpc_port = free_port()?;
        let mut node_command = Command::new(&launch.qbitd);
        node_command
            .args([
                "-regtest",
                "-server=1",
                "-listen=0",
                "-dnsseed=0",
                "-discover=0",
                "-fallbackfee=0.00001",
                "-rpcuser=prismtest",
                "-rpcpassword=prismtest",
                "-txindex=0",
            ])
            .arg(format!("-datadir={}", directory.display()))
            .arg(format!("-rpcport={rpc_port}"))
            .arg(format!("-port={}", free_port()?));
        startup.node = Some(Process::spawn(
            &mut node_command,
            directory.join("qbit.log"),
        )?);
        let stratum = [free_port()?, free_port()?];
        let highdiff = [free_port()?, free_port()?];
        let api = [free_port()?, free_port()?];
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(45))
            .build()?;
        // Moved, never released: the fixture now holds it through cleanup.
        let serial = startup.serial.take().context("serial guard missing")?;
        let fixture = startup.fixture.insert(Self {
            directory: startup.directory.take().expect("directory checked above"),
            admin: database.admin,
            pool: database.pool,
            schema: database.schema,
            database_url: database.database_url,
            rpc_port,
            stratum,
            highdiff,
            api,
            servers: Vec::new(),
            miners: Vec::new(),
            node: startup.node.take().expect("node spawned above"),
            qbitd: launch.qbitd,
            server: launch.server,
            client,
            address: String::new(),
            ctv: launch.ctv,
            _serial: serial,
        });
        until("qbit RPC", 30, || async {
            Ok(fixture
                .rpc("getblockchaininfo", json!([]))
                .await?
                .get("chain")
                == Some(&json!("regtest")))
        })
        .await?;
        fixture.rpc("createwallet", json!(["prism"])).await?;
        fixture.address = fixture
            .rpc("getnewaddress", json!(["", "p2mr"]))
            .await?
            .as_str()
            .context("wallet address missing")?
            .into();
        fixture
            .rpc("generatetoaddress", json!([1, fixture.address]))
            .await?;
        for index in 0..2 {
            let process = fixture.start_server(index)?;
            fixture.servers.push(process);
        }
        for index in 0..2 {
            until(
                &format!("PRISM HTTP readiness of server {index}"),
                30,
                || async {
                    Ok(fixture
                        .client
                        .get(format!("http://127.0.0.1:{}/healthz", fixture.api[index]))
                        .send()
                        .await?
                        .status()
                        .is_success())
                },
            )
            .await?;
        }
        Ok(())
    }

    fn start_server(&self, index: usize) -> Result<Process> {
        self.start_server_with_sponsorship(index, None)
    }

    fn start_server_with_sponsorship(&self, index: usize, fee: Option<u64>) -> Result<Process> {
        let mut command = Command::new(&self.server);
        // Inherited operator PRISM settings must not alter a disposable test.
        for (name, _) in std::env::vars()
            .filter(|(name, _)| name.starts_with("PRISM_") || name.starts_with("QBIT_"))
        {
            command.env_remove(name);
        }
        command
            .env("PRISM_DATABASE_URL", &self.database_url)
            .env("PRISM_POSTGRES_INIT_SCHEMA", "1")
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_INSTANCE_ID", format!("live-{index}"))
            .env("QBIT_CHAIN", "regtest")
            .env("QBIT_RPC_HOST", "127.0.0.1")
            .env("QBIT_RPC_PORT", self.rpc_port.to_string())
            .env("QBIT_RPC_USER", "prismtest")
            .env("QBIT_RPC_PASSWORD", "prismtest")
            .env("PRISM_STRATUM_BIND", "127.0.0.1")
            .env("PRISM_STRATUM_PORT", self.stratum[index].to_string())
            .env(
                "PRISM_STRATUM_HIGHDIFF_PORT",
                self.highdiff[index].to_string(),
            )
            .env("PRISM_AUDIT_PORT", self.api[index].to_string())
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("PRISM_BLOCKPOLL_SECONDS", "0.2")
            .env("PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS", "1")
            .env("PRISM_PUBLIC_CACHE_ENABLED", "0")
            .env("RUST_LOG", "warn");
        if self.ctv {
            command
                .env("PRISM_CTV_SETTLEMENT_ENABLED", "1")
                .env("PRISM_CTV_BROADCASTER_ENABLED", "1")
                .env("PRISM_CTV_BROADCASTER_POLL_SECONDS", "0.2")
                .env("PRISM_CTV_SPEND_SCAN_BLOCKS", "1")
                .env("PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS", "1000000000000")
                .env(
                    "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
                    "1000",
                );
        }
        if let Some(fee) = fee {
            command
                .env("PRISM_CTV_BROADCASTER_WALLET", "prism")
                .env("PRISM_CTV_BROADCASTER_FEE_BITS", fee.to_string());
        }
        Process::spawn(
            &mut command,
            self.directory.path().join(format!("server-{index}.log")),
        )
    }

    fn start_miner(&mut self, index: usize) -> Result<()> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-miner"));
        command.args([
            "--address",
            &format!("127.0.0.1:{}", self.stratum[index]),
            "--username",
            &format!("{}.live-{index}", self.address),
            "--threads",
            "1",
            "--hashes-per-second",
            "8",
            "--duration-seconds",
            "90",
            "--pause-after-share-ms",
            "150",
        ]);
        self.miners.push(Process::spawn(
            &mut command,
            self.directory
                .path()
                .join(format!("miner-{index}-{}.json", self.miners.len())),
        )?);
        Ok(())
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let payload: Value = self
            .client
            .post(format!("http://127.0.0.1:{}/", self.rpc_port))
            .basic_auth("prismtest", Some("prismtest"))
            .json(&json!({"jsonrpc":"1.0","id":"live-test","method":method,"params":params}))
            .send()
            .await?
            .json()
            .await?;
        ensure!(
            payload["error"].is_null(),
            "RPC {method}: {}",
            payload["error"]
        );
        Ok(payload["result"].clone())
    }

    async fn count(&self, writer: usize) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM qbit_share_ledger WHERE accepted AND writer_id=$1",
        )
        .bind(format!("live-{writer}"))
        .fetch_one(&self.pool)
        .await?)
    }

    async fn subscription(&self, index: usize) -> Result<String> {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", self.stratum[index])).await?;
        let (read, mut write) = stream.into_split();
        write
            .write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n")
            .await?;
        let mut lines = BufReader::new(read).lines();
        loop {
            let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
                .await??
                .context("subscription closed")?;
            let value: Value = serde_json::from_str(&line)?;
            if value["id"] == 1 {
                return Ok(value["result"][1]
                    .as_str()
                    .context("extranonce missing")?
                    .into());
            }
        }
    }

    /// Stop the miners and wait until no block candidate is still being
    /// worked on. Since 011 a claim passes through `offer_reserved` and
    /// `offered` before its landing, so the SIGKILL in the failover test can
    /// leave such a row, or a `reconciliation` row it was recovering, under a
    /// real 120-second lease; the surviving process reclaims it through the
    /// production expiry path, lands it, and finishes it or releases it into
    /// `reconciliation` with a backoff. Drained therefore means: no row is
    /// `pending`, `offer_reserved` or `offered`, no row holds a live claim,
    /// and every remaining `reconciliation` row was released by its attempt
    /// (its `next_attempt_at` is in the future, or it is parked). Such rows
    /// may stay: a rejected block, or one the chain never activates, is
    /// retried on read-only observations and never terminalizes, so waiting
    /// for it would wait forever. A row that is due and unclaimed is between
    /// attempts and still counts as work.
    async fn quiesce(&mut self) -> Result<()> {
        for miner in &mut self.miners {
            miner.stop();
        }
        until("candidate outbox drain", 140, || async {
            Ok(sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state IN ('pending','offer_reserved','offered') OR (state='reconciliation' AND (claim_expires_at>clock_timestamp() OR next_attempt_at<=clock_timestamp()))",
            )
            .fetch_one(&self.pool)
            .await?
                == 0)
        })
        .await
    }

    async fn integrity(&self) -> Result<()> {
        let report: Value = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
            .fetch_one(&self.pool)
            .await?;
        ensure!(
            report["mismatch_count"] == 0 && report["current_drift_count"] == 0,
            "carry integrity failed: {report}"
        );
        Ok(())
    }

    async fn assert_public_block_bits(&self, hash: &str, expected: &Value) -> Result<()> {
        ensure!(
            expected
                .as_str()
                .is_some_and(|bits| bits.len() == 8 && bits != "00000000"),
            "node did not provide valid compact bits"
        );
        let stored: Option<String> = sqlx::query_scalar(
            "SELECT found_block_bits FROM qbit_pool_audit_bundles WHERE block_hash=$1",
        )
        .bind(hash)
        .fetch_one(&self.pool)
        .await?;
        ensure!(
            stored.as_deref() == expected.as_str(),
            "durable block bits differ from node"
        );
        for port in self.api {
            let response: Value = self
                .client
                .get(format!(
                    "http://127.0.0.1:{port}/public/v1/blocks?limit=100"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let row = response["rows"]
                .as_array()
                .context("public block rows missing")?
                .iter()
                .find(|row| row["hash"] == hash)
                .context("confirmed block missing from public response")?;
            ensure!(
                &row["bits"] == expected,
                "public block bits differ from node: {row}"
            );
        }
        Ok(())
    }

    async fn cleanup(mut self) -> Result<()> {
        for miner in &mut self.miners {
            miner.stop();
        }
        for server in &mut self.servers {
            server.stop();
        }
        self.node.stop();
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }

    /// Bounded, sanitized log tails and observed status for the node, both
    /// server roles (labelled when not started) and every miner.
    fn diagnostics(&self) -> String {
        let mut children = vec![("qbit node".to_owned(), Some(&self.node))];
        for index in 0..self.servers.len().max(2) {
            children.push((format!("server-{index}"), self.servers.get(index)));
        }
        for (index, miner) in self.miners.iter().enumerate() {
            children.push((format!("miner-{index}"), Some(miner)));
        }
        diagnostics::report(&children, Some(&self.database_url))
    }
}

/// Child evidence for live-fixture failures.
///
/// Logs are untrusted bytes. Only the last `TAIL_BYTES` of each are read, and
/// a line cut by that bound is dropped. Complete lines are sanitized first;
/// the line and byte budget then keeps only whole sanitized lines, so neither
/// bound can expose part of a secret the sanitizer would have matched whole.
mod diagnostics {
    use super::Process;
    use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
    use std::{
        io::{ErrorKind, Read, Seek, SeekFrom},
        path::Path,
        process::{Command, ExitStatus},
    };

    pub const TAIL_BYTES: u64 = 16 * 1024;
    pub const MAX_LINES: usize = 40;
    pub const MAX_TEXT_BYTES: usize = 8 * 1024;
    const REDACTED: &str = "[redacted]";
    /// Stands in for a control character a child wrote raw.
    const REPLACEMENT: char = '\u{fffd}';
    /// The shortest hex, base64 or base64url run withheld as possible key
    /// material (a 128-bit value in hex, or 24 bytes in base64).
    pub const ENCODED_RUN: usize = 32;
    /// The seed bytes the server accepts as test signing seeds when
    /// `PRISM_ALLOW_TEST_SIGNING_SEEDS=1` is in its environment.
    const TEST_SEED_BYTES: [u8; 4] = [0x11, 0x22, 0x42, 0x43];

    pub fn report(children: &[(String, Option<&Process>)], database_url: Option<&str>) -> String {
        let mut secrets: Vec<String> = children
            .iter()
            .filter_map(|(_, process)| *process)
            .flat_map(|process| process.secrets.iter().cloned())
            .collect();
        if let Some(url) = database_url {
            add_url(&mut secrets, url);
        }
        // Longest first, so a whole URL is replaced before its password.
        secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        secrets.dedup();
        children
            .iter()
            .map(|(role, process)| match process {
                None => format!("--- {role}: not started"),
                Some(process) => section(role, process, &secrets),
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn section(role: &str, process: &Process, secrets: &[String]) -> String {
        let observed = process.child.try_wait();
        // An exited child's final unterminated line is complete.
        let exited = matches!(observed, Ok(Some(_)));
        let status = describe_status(observed);
        let name = process
            .log
            .file_name()
            .map_or_else(|| "log".into(), |name| name.to_string_lossy());
        let tail = match read_tail(&process.log, exited) {
            Tail::Missing => return format!("--- {role} [{status}]: {name} missing"),
            Tail::Unreadable(kind) => {
                return format!("--- {role} [{status}]: {name} unreadable ({kind:?})")
            }
            Tail::Read(tail) => tail,
        };
        let sanitized = sanitize(&tail.lines, secrets);
        let shown = budget(&sanitized);
        let mut text = format!(
            "--- {role} [{status}]: {name}, last {} of {} bytes inspected, {} partial line(s) dropped, {} of {} sanitized line(s) shown",
            tail.inspected,
            tail.size,
            tail.dropped,
            shown.len(),
            sanitized.len()
        );
        for line in shown {
            text.push('\n');
            text.push_str(line);
        }
        text
    }

    /// `running`, `exited (...)` or `unknown (...)`; a failed status query is
    /// never reported as an exit.
    pub fn describe_status(observed: std::io::Result<Option<ExitStatus>>) -> String {
        match observed {
            Ok(None) => "running".into(),
            Ok(Some(status)) => {
                if let Some(code) = status.code() {
                    return format!("exited (code {code})");
                }
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    if let Some(signal) = status.signal() {
                        return format!("exited (signal {signal})");
                    }
                }
                "exited (no code or signal)".into()
            }
            Err(error) => format!("unknown (status query failed: {:?})", error.kind()),
        }
    }

    pub enum Tail {
        Missing,
        Unreadable(ErrorKind),
        Read(Lines),
    }

    pub struct Lines {
        pub size: u64,
        pub inspected: usize,
        pub dropped: usize,
        pub lines: Vec<String>,
    }

    /// Complete lines from the last `TAIL_BYTES` of `path`. A leading line
    /// cut by the window is dropped, as is an unterminated final line unless
    /// `final_line_complete` (its writer has exited).
    pub fn read_tail(path: &Path, final_line_complete: bool) -> Tail {
        let read = || -> std::io::Result<(u64, u64, Vec<u8>)> {
            let mut file = std::fs::File::open(path)?;
            let size = file.metadata()?.len();
            let start = size.saturating_sub(TAIL_BYTES);
            file.seek(SeekFrom::Start(start))?;
            let mut bytes = Vec::new();
            file.take(TAIL_BYTES).read_to_end(&mut bytes)?;
            Ok((size, start, bytes))
        };
        let (size, start, bytes) = match read() {
            Ok(read) => read,
            Err(error) if error.kind() == ErrorKind::NotFound => return Tail::Missing,
            Err(error) => return Tail::Unreadable(error.kind()),
        };
        let inspected = bytes.len();
        let mut body = bytes.as_slice();
        let mut dropped = 0;
        if start > 0 && !body.is_empty() {
            dropped += 1;
            body = match body.iter().position(|byte| *byte == b'\n') {
                Some(end) => &body[end + 1..],
                None => &[],
            };
        }
        if !body.is_empty() && !body.ends_with(b"\n") && !final_line_complete {
            dropped += 1;
            body = match body.iter().rposition(|byte| *byte == b'\n') {
                Some(end) => &body[..=end],
                None => &[],
            };
        }
        let body = body.strip_suffix(b"\n").unwrap_or(body);
        let lines = if body.is_empty() {
            Vec::new()
        } else {
            body.split(|byte| *byte == b'\n')
                .map(|line| {
                    let line = line.strip_suffix(b"\r").unwrap_or(line);
                    String::from_utf8_lossy(line).into_owned()
                })
                .collect()
        };
        Tail::Read(Lines {
            size,
            inspected,
            dropped,
            lines,
        })
    }

    /// The newest whole lines that fit both `MAX_LINES` and `MAX_TEXT_BYTES`.
    pub fn budget(lines: &[String]) -> Vec<&str> {
        let mut bytes = 0;
        let mut shown: Vec<&str> = Vec::new();
        for line in lines.iter().rev() {
            if shown.len() == MAX_LINES || bytes + line.len() + 1 > MAX_TEXT_BYTES {
                break;
            }
            bytes += line.len() + 1;
            shown.push(line);
        }
        shown.reverse();
        shown
    }

    /// Credential and signing values in a command's explicit environment and
    /// arguments: values of credential-named settings, URL credentials, and
    /// the test signing seeds when the command enables them.
    pub fn command_secrets(command: &Command) -> Vec<String> {
        let mut secrets = Vec::new();
        let mut settings: Vec<(String, String)> = command
            .get_envs()
            .filter_map(|(name, value)| Some((name.to_str()?.into(), value?.to_str()?.into())))
            .collect();
        settings.extend(command.get_args().filter_map(|argument| {
            let (name, value) = argument.to_str()?.trim_start_matches('-').split_once('=')?;
            Some((name.into(), value.into()))
        }));
        for (name, value) in &settings {
            if credential_name(name) && !is_flag(value) {
                add(&mut secrets, value);
            }
            add_url(&mut secrets, value);
            if name == "PRISM_ALLOW_TEST_SIGNING_SEEDS" && value != "0" {
                for byte in TEST_SEED_BYTES {
                    let seed = [byte; 32];
                    add(&mut secrets, &format!("{byte:02x}").repeat(32));
                    add(&mut secrets, &format!("{seed:?}"));
                    add(&mut secrets, &format!("{seed:x?}"));
                }
            }
        }
        secrets
    }

    fn is_flag(value: &str) -> bool {
        ["0", "1", "true", "false", "yes", "no", "on", "off"]
            .iter()
            .any(|flag| value.trim().eq_ignore_ascii_case(flag))
    }

    /// A value as written, as the inside of the Rust `{:?}` and JSON string
    /// literals a child might print it in, and each of those as `sanitize_line`
    /// leaves it. A password may carry a control character once it is decoded,
    /// and a child that writes the decoded value raw would otherwise survive
    /// normalization unmatched.
    fn add(secrets: &mut Vec<String>, value: &str) {
        let value = value.trim();
        if value.is_empty() {
            return;
        }
        let debug = format!("{value:?}");
        let json = serde_json::Value::from(value).to_string();
        // Both literals are quoted, so the inner slice is at ASCII boundaries.
        for form in [value, &debug[1..debug.len() - 1], &json[1..json.len() - 1]] {
            let mut forms = vec![form.to_owned()];
            // A form that normalizes to replacement characters alone would
            // match every normalized control byte, so it is not stored.
            let normalized = normalize_controls(form);
            if normalized != form && normalized.chars().any(|c| c != REPLACEMENT) {
                forms.push(normalized);
            }
            for form in forms {
                if !secrets.contains(&form) {
                    secrets.push(form);
                }
            }
        }
    }

    /// A URL with a password, and each password as written, decoded, and
    /// re-encoded in the percent and form encodings a child might print.
    /// Passwords are the userinfo password and, for the PostgreSQL URLs PRISM
    /// accepts, every query parameter SQLx reads as `password`: its
    /// form-decoded name matches exactly, and its value is form-decoded.
    fn add_url(secrets: &mut Vec<String>, value: &str) {
        let Ok(url) = url::Url::parse(value.trim()) else {
            return;
        };
        let mut passwords = Vec::new();
        if let Some(password) = url.password() {
            let decoded = percent_decode_str(password).decode_utf8_lossy();
            passwords.push((password.to_owned(), decoded.into_owned()));
        }
        if matches!(url.scheme(), "postgres" | "postgresql") {
            for pair in url.query().unwrap_or_default().split('&') {
                let Some((name, decoded)) = url::form_urlencoded::parse(pair.as_bytes()).next()
                else {
                    continue;
                };
                if name == "password" {
                    let written = pair.split_once('=').map_or("", |(_, written)| written);
                    passwords.push((written.to_owned(), decoded.into_owned()));
                }
            }
        }
        passwords.retain(|(written, decoded)| !written.is_empty() && !decoded.is_empty());
        if passwords.is_empty() {
            return;
        }
        add(secrets, value);
        for (written, decoded) in &passwords {
            add(secrets, written);
            add(secrets, decoded);
            add(
                secrets,
                &utf8_percent_encode(decoded, NON_ALPHANUMERIC).to_string(),
            );
            add(
                secrets,
                &url::form_urlencoded::byte_serialize(decoded.as_bytes()).collect::<String>(),
            );
        }
    }

    fn credential_name(name: &str) -> bool {
        const WORDS: [&str; 16] = [
            "password",
            "passwd",
            "passphrase",
            "secret",
            "token",
            "seed",
            "private",
            "privkey",
            "credential",
            "rpcauth",
            "authorization",
            "apikey",
            "signing",
            "mnemonic",
            "xprv",
            "cookie",
        ];
        let name = name.to_ascii_lowercase();
        WORDS.iter().any(|word| name.contains(word))
            || name
                .split(|c: char| !c.is_ascii_alphanumeric())
                .any(|part| part.ends_with("key") || matches!(part, "pwd" | "dsn" | "auth"))
    }

    /// Removes armored blocks, then sanitizes each remaining line. A tail
    /// that shows an END marker before any BEGIN started inside a block, so
    /// everything up to that marker is withheld; a block with no visible END
    /// withholds the rest of the tail.
    pub fn sanitize(lines: &[String], secrets: &[String]) -> Vec<String> {
        const BEGIN: &str = "-----BEGIN";
        const END: &str = "-----END";
        let mut inside = lines
            .iter()
            .find_map(|line| match (line.find(BEGIN), line.find(END)) {
                (None, None) => None,
                (Some(begin), Some(end)) => Some(end < begin),
                (begin, _) => Some(begin.is_none()),
            })
            .unwrap_or(false);
        let mut withheld = 0;
        let mut output = Vec::new();
        for line in lines {
            // True when the line's last marker is an END, closing any block.
            let closes = line.rfind(END) > line.rfind(BEGIN);
            if inside || line.contains(BEGIN) {
                withheld += 1;
                inside = !closes;
                continue;
            }
            if withheld > 0 {
                output.push(format!(
                    "[withheld {withheld} line(s) of armored key material]"
                ));
                withheld = 0;
            }
            output.push(sanitize_line(line, secrets));
        }
        if withheld > 0 {
            output.push(format!(
                "[withheld {withheld} line(s) of armored key material]"
            ));
        }
        output
    }

    pub fn sanitize_line(line: &str, secrets: &[String]) -> String {
        let mut text = normalize_controls(line);
        for secret in secrets {
            text = replace_ignoring_ascii_case(&text, secret);
        }
        redact_encoded(&redact_assignment(&redact_urls(&text)))
    }

    /// Every control character but tab becomes the replacement character, so
    /// a child's raw bytes never reach the output. `command_secrets` stores
    /// the result of this for each secret as well, because it runs before the
    /// secrets are replaced and would otherwise stop them matching.
    fn normalize_controls(text: &str) -> String {
        text.chars()
            .map(|c| {
                if c.is_control() && c != '\t' {
                    REPLACEMENT
                } else {
                    c
                }
            })
            .collect()
    }

    fn replace_ignoring_ascii_case(text: &str, secret: &str) -> String {
        let haystack = text.to_ascii_lowercase();
        let needle = secret.to_ascii_lowercase();
        let mut output = String::with_capacity(text.len());
        let mut from = 0;
        while let Some(found) = haystack[from..].find(&needle) {
            output.push_str(&text[from..from + found]);
            output.push_str(REDACTED);
            from += found + needle.len();
        }
        output.push_str(&text[from..]);
        output
    }

    /// Replaces database URLs and URLs carrying userinfo.
    fn redact_urls(text: &str) -> String {
        let mut output = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(separator) = rest.find("://") {
            // The scheme starts after the last non-scheme character, which
            // may be several bytes long.
            let scheme_start = rest[..separator]
                .char_indices()
                .rev()
                .find(|(_, c)| !(c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-')))
                .map_or(0, |(index, c)| index + c.len_utf8());
            let after = &rest[separator + 3..];
            let end = after
                .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>' | '`'))
                .unwrap_or(after.len());
            let authority = after[..end]
                .split(['/', '?', '#'])
                .next()
                .unwrap_or_default();
            let scheme = rest[scheme_start..separator].to_ascii_lowercase();
            output.push_str(&rest[..scheme_start]);
            if scheme.starts_with("postgres") || authority.contains('@') {
                output.push_str("[redacted-url]");
            } else {
                output.push_str(&rest[scheme_start..separator + 3 + end]);
            }
            rest = &after[end..];
        }
        output.push_str(rest);
        output
    }

    /// Withholds the rest of the line after a credential-named setting's
    /// `=` or `:` (not `::`), or after its space when written as a flag.
    fn redact_assignment(text: &str) -> String {
        let bytes = text.as_bytes();
        let identifier = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-');
        let mut index = 0;
        while index < bytes.len() {
            if !identifier(bytes[index]) {
                index += 1;
                continue;
            }
            let start = index;
            while index < bytes.len() && identifier(bytes[index]) {
                index += 1;
            }
            let name = &text[start..index];
            let mut next = index;
            if next < bytes.len() && matches!(bytes[next], b'"' | b'\'') {
                next += 1;
            }
            while next < bytes.len() && matches!(bytes[next], b' ' | b'\t') {
                next += 1;
            }
            let assigns = match bytes.get(next) {
                Some(b'=') => true,
                Some(b':') => bytes.get(next + 1) != Some(&b':'),
                _ => name.starts_with('-') && next > index && next < bytes.len(),
            };
            if assigns && credential_name(name) {
                let cut = if matches!(bytes.get(next), Some(b'=' | b':')) {
                    next + 1
                } else {
                    index
                };
                return format!("{} {REDACTED}", &text[..cut]);
            }
        }
        text.into()
    }

    /// Replaces every run of `ENCODED_RUN` or more characters from the hex,
    /// base64 and base64url alphabets, whatever its content: encoded key
    /// material cannot be told apart from long names or paths, so those are
    /// withheld too.
    fn redact_encoded(text: &str) -> String {
        let member =
            |c: char| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '=' | '_' | '-');
        let mut output = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(start) = rest.find(member) {
            let length = rest[start..]
                .find(|c| !member(c))
                .unwrap_or(rest.len() - start);
            output.push_str(&rest[..start]);
            if length >= ENCODED_RUN {
                output.push_str("[redacted-encoded]");
            } else {
                output.push_str(&rest[start..start + length]);
            }
            rest = &rest[start + length..];
        }
        output.push_str(rest);
        output
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_two_server_mining_failover_audit_and_reorg() -> Result<()> {
    let Some(mut fixture) = Fixture::open(false).await? else {
        return Ok(());
    };
    let result=async {
        let a=fixture.subscription(0).await?;let b=fixture.subscription(1).await?;ensure!(a!=b,"servers reused extranonce1");
        fixture.start_miner(0)?;fixture.start_miner(1)?;
        until("accepted shares on both servers",30,||async {Ok(fixture.count(0).await?>2&&fixture.count(1).await?>2)}).await?;
        fixture.servers[0].stop();fixture.miners[0].stop();
        let before=fixture.count(1).await?;
        until("surviving server mining",20,||async {Ok(fixture.count(1).await?>before+2)}).await?;
        fixture.servers[0]=fixture.start_server(0)?;
        until("restarted Stratum listener",20,||async {Ok(tokio::net::TcpStream::connect(("127.0.0.1",fixture.stratum[0])).await.is_ok())}).await?;
        let restarted=fixture.subscription(0).await?;ensure!(restarted!=a&&restarted!=b,"restart reused session extranonce");
        let before=fixture.count(0).await?;fixture.start_miner(0)?;
        until("restarted server mining",20,||async {Ok(fixture.count(0).await?>before+2)}).await?;
        fixture.quiesce().await?;
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE accepted").fetch_one(&fixture.pool).await?;
        let unique:i64=sqlx::query_scalar("SELECT count(*) FROM qbit_prism_share_hashes").fetch_one(&fixture.pool).await?;ensure!(count==unique,"duplicate headers credited");
        for index in 0..2 {
            let latest:Value=fixture.client.get(format!("http://127.0.0.1:{}/audit/latest",fixture.api[index])).send().await?.error_for_status()?.json().await?;
            ensure!(latest["accepted_share_count"].as_i64()==Some(count),"API does not expose cluster share count: {latest}");
        }
        let hash:String=sqlx::query_scalar("SELECT block_hash FROM qbit_pool_blocks WHERE chain_state='confirmed' ORDER BY block_height DESC LIMIT 1").fetch_one(&fixture.pool).await?;
        let body:Value=fixture.client.get(format!("http://127.0.0.1:{}/audit/blocks/{hash}/bundle",fixture.api[1])).send().await?.error_for_status()?.json().await?;
        let bundle:AuditBundle=serde_json::from_value(body["audit_bundle"].clone())?;
        let block=fixture.rpc("getblock",json!([hash,2])).await?;
        fixture.assert_public_block_bits(&hash,&block["bits"]).await?;
        let coinbase=fixture.rpc("getrawtransaction",json!([block["tx"][0]["txid"],false,hash])).await?;
        let key=ManifestSigningKey::from_seed_hex(&"22".repeat(32))?.public_key_hex();
        verify_audit_bundle_against_coinbase_tx_hex(&bundle,coinbase.as_str().context("node coinbase missing")?,&key)?;
        fixture.rpc("invalidateblock",json!([hash])).await?;
        let replacement_address=fixture.rpc("getnewaddress",json!(["","p2mr"])).await?;
        let replacement=fixture.rpc("generatetoaddress",json!([2,replacement_address])).await?;
        until("immature pool block disconnect",20,||async {Ok(sqlx::query_scalar::<_,String>("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1").bind(&hash).fetch_one(&fixture.pool).await?=="inactive")}).await?;
        fixture.integrity().await?;
        fixture.rpc("invalidateblock",json!([replacement[0]])).await?;
        fixture.rpc("reconsiderblock",json!([hash])).await?;
        let restoration_address=fixture.rpc("getnewaddress",json!(["","p2mr"])).await?;
        fixture.rpc("generatetoaddress",json!([2,restoration_address])).await?;
        until("pool block reconnection",20,||async {Ok(sqlx::query_scalar::<_,String>("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1").bind(&hash).fetch_one(&fixture.pool).await?=="confirmed")}).await?;
        fixture.assert_public_block_bits(&hash,&block["bits"]).await?;
        fixture.integrity().await?;
        eprintln!("live regtest: {count} committed shares across two processes; failover/restart, unique extranonces, actual-coinbase audit and disconnect/reconnect verified");
        Ok::<_,anyhow::Error>(())
    }.await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_ctv_maturity_broadcast_and_confirmation() -> Result<()> {
    let Some(mut fixture) = Fixture::open(true).await? else {
        return Ok(());
    };
    let result=async {
        fixture.start_miner(0)?;
        until("mined CTV pool block",30,||async {Ok(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE b.chain_state='confirmed'").fetch_one(&fixture.pool).await?>0)}).await?;
        fixture.quiesce().await?;
        let row=sqlx::query("SELECT a.fanout_txid,b.block_height FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE b.chain_state='confirmed' ORDER BY b.block_height LIMIT 1").fetch_one(&fixture.pool).await?;
        let txid:String=row.try_get("fanout_txid")?;
        let height:i64=row.try_get("block_height")?;
        let tip=fixture.rpc("getblockcount",json!([])).await?.as_i64().context("tip missing")?;
        fixture.rpc("generatetoaddress",json!([height+1000-tip,fixture.address])).await?;
        until("CTV fanout in node mempool",40,||async {Ok(fixture.rpc("getrawmempool",json!([])).await?.as_array().is_some_and(|rows|rows.contains(&json!(txid))))}).await?;
        fixture.rpc("generatetoaddress",json!([1,fixture.address])).await?;
        until("CTV confirmation read model",45,||async {Ok(sqlx::query_scalar::<_,String>("SELECT settlement_status FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).fetch_one(&fixture.pool).await?=="confirmed")}).await?;
        let attempts:i64=sqlx::query_scalar("SELECT broadcast_attempt_count FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).fetch_one(&fixture.pool).await?;
        ensure!(attempts>=1,"broadcast attempts were not persisted");
        let confirmed_hash:String=sqlx::query_scalar("SELECT confirmed_block_hash FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).fetch_one(&fixture.pool).await?;
        let scan_cursor:Option<i64>=sqlx::query_scalar("SELECT spend_scan_next_height FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).fetch_one(&fixture.pool).await?;
        ensure!(scan_cursor.is_some(),"no-txindex confirmation failed to persist its bounded scan cursor");
        fixture.rpc("invalidateblock",json!([confirmed_hash])).await?;
        let reorg_address=fixture.rpc("getnewaddress",json!(["","p2mr"])).await?;
        // Mine a stronger branch without the still-pending fanout so its
        // disconnection is observable before a later block reconfirms it.
        for _ in 0..2 {fixture.rpc("generateblock",json!([reorg_address,[]])).await?;}
        until("CTV confirmation disconnect detected",20,||async {Ok(sqlx::query_scalar::<_,String>("SELECT settlement_status FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).fetch_one(&fixture.pool).await?!="confirmed")}).await?;
        until("disconnected CTV fanout recovered in mempool",20,||async {Ok(fixture.rpc("getrawmempool",json!([])).await?.as_array().is_some_and(|rows|rows.contains(&json!(txid))))}).await?;
        fixture.rpc("generatetoaddress",json!([1,reorg_address])).await.context("mining replacement fanout confirmation")?;
        until("CTV confirmation recovered after reorg",30,||async {Ok(sqlx::query_scalar::<_,bool>("SELECT settlement_status='confirmed' AND confirmed_block_hash<>$2 FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).bind(&confirmed_hash).fetch_one(&fixture.pool).await?)}).await?;
        fixture.integrity().await?;
        eprintln!("live CTV regtest: mature covenant {txid} broadcast, confirmed without txindex, and recovered after confirmation reorg; {attempts} initial durable attempts across two broadcaster processes");
        Ok::<_,anyhow::Error>(())
    }.await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

#[cfg(unix)]
mod startup_diagnostics_tests {
    use super::diagnostics::{
        budget, command_secrets, describe_status, read_tail, report, sanitize, Tail, MAX_LINES,
        MAX_TEXT_BYTES, TAIL_BYTES,
    };
    use super::*;
    use axum::{
        routing::{get, post},
        Json, Router,
    };
    use std::{
        os::unix::{fs::PermissionsExt, process::ExitStatusExt},
        path::Path,
    };

    const SEED_11: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn script(directory: &Path, name: &str, body: &str) -> Result<String> {
        let path = directory.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}"))?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
        Ok(path.to_str().context("script path is not UTF-8")?.into())
    }

    /// Records the fake child's arguments where the test can read them.
    fn record_arguments(witness: &Path) -> String {
        let witness = witness.display();
        format!("printf '%s\\n' \"$@\" > '{witness}.tmp' && mv '{witness}.tmp' '{witness}'\n")
    }

    /// Pools that never connect unless used; startup reaches no SQL.
    fn lazy_database() -> Result<Database> {
        let database_url =
            "postgres://diagnostics:lazy-pool-Passw0rd@127.0.0.1:1/unused".to_owned();
        let options =
            || sqlx::postgres::PgPoolOptions::new().acquire_timeout(Duration::from_millis(200));
        Ok(Database {
            admin: options().connect_lazy(&database_url)?,
            pool: options().connect_lazy(&database_url)?,
            schema: "prism_live_unused".into(),
            database_url,
        })
    }

    /// A startup holding the guard of its own mutex rather than `SERIAL`, so
    /// these tests neither wait for nor block a live fixture; the returned
    /// mutex shows when the guard is released.
    fn startup() -> Result<(Startup, PathBuf, &'static tokio::sync::Mutex<()>)> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().to_path_buf();
        let serial = Box::leak(Box::new(tokio::sync::Mutex::new(())));
        let startup = Startup {
            directory: Some(directory),
            serial: Some(serial.try_lock()?),
            ..Startup::default()
        };
        Ok((startup, path, serial))
    }

    async fn read_witness(path: &Path) -> Result<String> {
        until("fake child witness", 20, || async { Ok(path.exists()) }).await?;
        Ok(std::fs::read_to_string(path)?)
    }

    /// Answers the fixture's startup RPCs on the port the fake qbitd was
    /// given, returning an RPC error for `fail`.
    async fn serve_fake_node(arguments: PathBuf, fail: Option<&'static str>) -> Result<()> {
        let recorded = read_witness(&arguments).await?;
        let port: u16 = recorded
            .lines()
            .find_map(|argument| argument.strip_prefix("-rpcport="))
            .context("fake qbitd was not given -rpcport")?
            .parse()?;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
        let answer = move |Json(request): Json<Value>| async move {
            let method = request["method"].as_str().unwrap_or_default();
            if Some(method) == fail {
                return Json(
                    json!({"result":null,"error":{"code":-4,"message":"injected failure"}}),
                );
            }
            let result = match method {
                "getblockchaininfo" => json!({"chain":"regtest"}),
                "getnewaddress" => json!("fake-address"),
                _ => Value::Null,
            };
            Json(json!({"result":result,"error":null}))
        };
        axum::serve(listener, Router::new().route("/", post(answer))).await?;
        Ok(())
    }

    async fn serve_fake_health(witness: PathBuf) -> Result<()> {
        let port: u16 = read_witness(&witness).await?.trim().parse()?;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
        let app = Router::new().route("/healthz", get(|| async { "ok" }));
        axum::serve(listener, app).await?;
        Ok(())
    }

    fn assert_absent(text: &str, fragments: &[&str]) {
        let lower = text.to_ascii_lowercase();
        for fragment in fragments {
            assert!(
                !lower.contains(&fragment.to_ascii_lowercase()),
                "diagnostics exposed {fragment:?}:\n{text}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_phase_failure_keeps_original_error_line_and_status() -> Result<()> {
        let scratch = tempfile::tempdir()?;
        let arguments = scratch.path().join("node.args");
        let qbitd = script(
            scratch.path(),
            "qbitd",
            &format!(
                "echo SCOUT385-DISTINCTIVE-NODE-LINE\nfor argument in \"$@\"; do echo \"arg $argument\"; done\n{}exec sleep 60\n",
                record_arguments(&arguments)
            ),
        )?;
        let (mut startup, directory, serial) = startup()?;
        let node = tokio::spawn(serve_fake_node(arguments, Some("createwallet")));
        let launch = Launch {
            qbitd,
            server: "/nonexistent/qbit-prism-server".into(),
            ctv: false,
        };
        let result = Fixture::start_children(lazy_database()?, launch, &mut startup).await;
        assert!(serial.try_lock().is_err(), "guard released before cleanup");
        let error = startup
            .finish(result)
            .err()
            .context("startup unexpectedly succeeded")?;
        node.abort();

        assert!(serial.try_lock().is_ok(), "failed startup kept the guard");
        assert!(
            error
                .root_cause()
                .to_string()
                .starts_with("RPC createwallet:"),
            "original error replaced: {error:?}"
        );
        let text = format!("{error:?}");
        assert!(text.contains("--- qbit node [running]: qbit.log"), "{text}");
        assert!(text.contains("SCOUT385-DISTINCTIVE-NODE-LINE"), "{text}");
        assert!(text.contains("--- server-0: not started"), "{text}");
        assert!(text.contains("--- server-1: not started"), "{text}");
        assert_absent(&text, &["prismtest", "lazy-pool-Passw0rd"]);
        assert!(!directory.exists(), "startup directory survived failure");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partial_server_start_reports_node_and_started_server() -> Result<()> {
        let scratch = tempfile::tempdir()?;
        let arguments = scratch.path().join("node.args");
        // A directory where server-1's log belongs makes its spawn fail after
        // server-0 has started.
        let qbitd = script(
            scratch.path(),
            "qbitd",
            &format!(
                "echo SCOUT385-PARTIAL-NODE-LINE\nfor argument in \"$@\"; do case \"$argument\" in -datadir=*) mkdir \"${{argument#-datadir=}}/server-1.log\";; esac; done\n{}exec sleep 60\n",
                record_arguments(&arguments)
            ),
        )?;
        let server = script(
            scratch.path(),
            "server",
            "echo \"server $PRISM_INSTANCE_ID database $PRISM_DATABASE_URL\"\nexec sleep 60\n",
        )?;
        let (mut startup, directory, serial) = startup()?;
        let node = tokio::spawn(serve_fake_node(arguments, None));
        let launch = Launch {
            qbitd,
            server: server.into(),
            ctv: false,
        };
        let result = Fixture::start_children(lazy_database()?, launch, &mut startup).await;
        assert!(serial.try_lock().is_err(), "guard released before cleanup");
        let error = startup
            .finish(result)
            .err()
            .context("startup unexpectedly succeeded")?;
        node.abort();

        assert!(serial.try_lock().is_ok(), "failed startup kept the guard");
        let io = error
            .root_cause()
            .downcast_ref::<std::io::Error>()
            .context("original spawn error replaced")?;
        assert_eq!(io.kind(), std::io::ErrorKind::IsADirectory, "{error:?}");
        let text = format!("{error:?}");
        assert!(text.contains("--- qbit node [running]: qbit.log"), "{text}");
        assert!(text.contains("SCOUT385-PARTIAL-NODE-LINE"), "{text}");
        assert!(
            text.contains("--- server-0 [running]: server-0.log"),
            "{text}"
        );
        assert!(text.contains("--- server-1: not started"), "{text}");
        assert_absent(&text, &["lazy-pool-Passw0rd", "postgres://"]);
        assert!(!directory.exists(), "startup directory survived failure");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn started_fixture_reports_every_role_and_cleanup_removes_directory() -> Result<()> {
        let scratch = tempfile::tempdir()?;
        let arguments = scratch.path().join("node.args");
        let qbitd = script(
            scratch.path(),
            "qbitd",
            &format!(
                "echo SCOUT385-STARTED-NODE\n{}exec sleep 60\n",
                record_arguments(&arguments)
            ),
        )?;
        let api = scratch.path().display();
        let server = script(
            scratch.path(),
            "server",
            &format!(
                "echo \"SCOUT385-STARTED-SERVER $PRISM_INSTANCE_ID\"\necho \"PRISM_DATABASE_URL=$PRISM_DATABASE_URL\"\necho \"connecting to $PRISM_DATABASE_URL with $QBIT_RPC_PASSWORD\"\necho \"manifest seed {SEED_11}\"\necho \"$PRISM_AUDIT_PORT\" > \"{api}/api-$PRISM_INSTANCE_ID.tmp\" && mv \"{api}/api-$PRISM_INSTANCE_ID.tmp\" \"{api}/api-$PRISM_INSTANCE_ID\"\nexec sleep 60\n"
            ),
        )?;
        let (mut startup, directory, serial) = startup()?;
        let helpers = [
            tokio::spawn(serve_fake_node(arguments, None)),
            tokio::spawn(serve_fake_health(scratch.path().join("api-live-0"))),
            tokio::spawn(serve_fake_health(scratch.path().join("api-live-1"))),
        ];
        let launch = Launch {
            qbitd,
            server: server.into(),
            ctv: false,
        };
        let result = Fixture::start_children(lazy_database()?, launch, &mut startup).await;
        let mut fixture = startup.finish(result)?;
        assert!(serial.try_lock().is_err(), "fixture did not take the guard");
        // Nothing listens on the Stratum port, so the real miner exits.
        fixture.start_miner(0)?;
        until("miner exit", 20, || {
            std::future::ready(
                fixture.miners[0]
                    .child
                    .try_wait()
                    .map(|status| status.is_some())
                    .map_err(Into::into),
            )
        })
        .await?;

        let text = fixture.diagnostics();
        assert!(text.contains("--- qbit node [running]: qbit.log"), "{text}");
        assert!(text.contains("SCOUT385-STARTED-NODE"), "{text}");
        for index in 0..2 {
            assert!(
                text.contains(&format!("--- server-{index} [running]: server-{index}.log")),
                "{text}"
            );
            assert!(
                text.contains(&format!("SCOUT385-STARTED-SERVER live-{index}")),
                "{text}"
            );
        }
        assert!(text.contains("--- miner-0 [exited (code "), "{text}");
        assert_absent(
            &text,
            &[
                "lazy-pool-Passw0rd",
                "postgres://",
                "prismtest",
                "1111111111",
            ],
        );
        assert!(!text.contains(&directory.display().to_string()), "{text}");

        // The lazy database makes the schema drop fail; the directory must go regardless.
        let _ = fixture.cleanup().await;
        for helper in helpers {
            helper.abort();
        }
        assert!(!directory.exists(), "cleanup left the fixture directory");
        assert!(serial.try_lock().is_ok(), "cleanup kept the guard");
        Ok(())
    }

    #[tokio::test]
    async fn unreadable_node_log_before_fixture_keeps_original_error_and_cleans_up() -> Result<()> {
        let (mut startup, directory, serial) = startup()?;
        let log = directory.join("qbit.log");
        let node = Process::spawn(
            Command::new("/bin/sh").args(["-c", "echo gone; exec sleep 60"]),
            log.clone(),
        )?;
        let pid = node.child.lock().id();
        std::fs::remove_file(&log)?;
        std::fs::create_dir(&log)?;
        startup.node = Some(node);
        startup.database_url = Some("postgres://u:pw-secret@db/prism".into());

        let error = startup.fail(anyhow::anyhow!("original startup failure"));
        assert!(serial.try_lock().is_ok(), "failed startup kept the guard");
        assert_eq!(error.root_cause().to_string(), "original startup failure");
        let text = format!("{error:?}");
        assert!(
            text.contains("--- qbit node [running]: qbit.log unreadable (IsADirectory)"),
            "{text}"
        );
        assert!(text.contains("--- server-0: not started"), "{text}");
        assert!(!directory.exists(), "startup directory survived failure");
        let alive = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stderr(Stdio::null())
            .status()?;
        assert!(!alive.success(), "node child survived startup failure");
        Ok(())
    }

    #[test]
    fn report_labels_status_final_lines_and_missing_logs() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let spawn = |script: &str, name: &str| {
            Process::spawn(
                Command::new("/bin/sh").args(["-c", script]),
                directory.path().join(name),
            )
        };
        let running = spawn("echo RUNNING-LINE; exec sleep 60", "running.log")?;
        let exited = spawn(
            "echo first; printf EXITED-UNTERMINATED; exit 7",
            "exited.log",
        )?;
        let missing = spawn("exec sleep 60", "missing.log")?;
        exited.child.lock().wait()?;
        std::fs::remove_file(directory.path().join("missing.log"))?;
        let started = Instant::now();
        while !std::fs::read_to_string(directory.path().join("running.log"))?
            .contains("RUNNING-LINE")
        {
            ensure!(
                started.elapsed() < Duration::from_secs(10),
                "running child wrote nothing"
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        let text = report(
            &[
                ("server-0".into(), Some(&running)),
                ("server-1".into(), Some(&exited)),
                ("miner-0".into(), Some(&missing)),
                ("qbit node".into(), None),
            ],
            None,
        );
        assert!(
            text.contains("--- server-0 [running]: running.log, last 13 of 13 bytes"),
            "{text}"
        );
        assert!(text.contains("RUNNING-LINE"), "{text}");
        assert!(
            text.contains("--- server-1 [exited (code 7)]: exited.log"),
            "{text}"
        );
        assert!(text.contains("EXITED-UNTERMINATED"), "{text}");
        assert!(
            text.contains("--- miner-0 [running]: missing.log missing"),
            "{text}"
        );
        assert!(text.contains("--- qbit node: not started"), "{text}");
        assert!(
            !text.contains(&directory.path().display().to_string()),
            "{text}"
        );
        Ok(())
    }

    #[test]
    fn status_distinguishes_running_exit_signal_and_query_failure() {
        assert_eq!(describe_status(Ok(None)), "running");
        assert_eq!(
            describe_status(Ok(Some(ExitStatus::from_raw(7 << 8)))),
            "exited (code 7)"
        );
        assert_eq!(
            describe_status(Ok(Some(ExitStatus::from_raw(9)))),
            "exited (signal 9)"
        );
        let unknown = describe_status(Err(std::io::Error::other("wait failed")));
        assert!(
            unknown.starts_with("unknown (status query failed"),
            "{unknown}"
        );
        assert!(!unknown.contains("exited"), "{unknown}");
    }

    #[test]
    fn sanitizer_removes_planted_credentials_and_keeps_unrelated_lines() {
        let url = "postgres://prism_user:p%40ss%2Fw0rd@db.internal:5432/prism?options=-csearch_path%3Dprism_live_x";
        let mut command = Command::new("server");
        command
            .env("PRISM_DATABASE_URL", url)
            .env("QBIT_RPC_PASSWORD", "rpc-S3cret")
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_RUNTIME_WORKERS", "2")
            .args(["-rpcpassword=node-Pa55", "-rpcuser=operator"]);
        let secrets = command_secrets(&command);
        assert!(
            !secrets.iter().any(|secret| secret == "1" || secret == "2"),
            "{secrets:?}"
        );
        let lines: Vec<String> = [
            "SCOUT385-DISTINCTIVE-CHILD-LINE",
            "config error: invalid PRISM_STRATUM_PORT",
            "long name PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT is withheld",
            "alphabetic base64 of the 0x22 seed IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI=",
            "QBIT_RPC_PASSWORD\t=\ttab-hunter3",
            "--api-token\ttab-flag-9",
            "panicked at src/config.rs:292:21",
            "source crates/qbit-prism-server/src/config.rs is a long path",
            "GET http://127.0.0.1:18443/ failed",
            &format!("connecting to {url}"),
            &format!("PRISM_DATABASE_URL={url}"),
            "retry postgresql://other:hidden-Pw@replica/db and https://user:tok3n@example.test/x",
            "saw p@ss/w0rd, P%40SS%2FW0RD and p%40ss%2fw0rd",
            "rpc said rpc-s3cret",
            "-rpcpassword=node-Pa55 then NODE-PA55",
            &format!("manifest seed {SEED_11}"),
            &format!("LEDGER {}", "22".repeat(32)),
            &format!(
                "{{\"ledger_attestation_signing_seed_hex\":\"{}\"}}",
                "4a".repeat(32)
            ),
            "Authorization: Basic cHJpc210ZXN0OnByaXNtdGVzdA==",
            "--secret-token hunter2-flag",
            "unrelated Zm9vYmFyYmF6cXV4MTIzNDU2Nzg5MGFiY2RlZg looks like a key",
            "-----BEGIN PRIVATE KEY-----",
            "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7",
            "c2hvcnQ=",
            "-----END PRIVATE KEY-----",
            "AFTER-PEM-LINE",
            "-----BEGIN OPENSSH PRIVATE KEY-----b3BlbnNzaC1rZXk=-----END OPENSSH PRIVATE KEY-----",
            "AFTER-ONE-LINE",
            "escape \u{1b}[31m red",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let text = sanitize(&lines, &secrets).join("\n");
        for visible in [
            "SCOUT385-DISTINCTIVE-CHILD-LINE",
            "config error: invalid PRISM_STRATUM_PORT",
            "long name [redacted-encoded] is withheld",
            "QBIT_RPC_PASSWORD\t= [redacted]",
            "--api-token [redacted]",
            "panicked at src/config.rs:292:21",
            "source [redacted-encoded].rs is a long path",
            "http://127.0.0.1:18443/",
            "AFTER-PEM-LINE",
            "AFTER-ONE-LINE",
            "[withheld 4 line(s) of armored key material]",
            "[withheld 1 line(s) of armored key material]",
        ] {
            assert!(text.contains(visible), "lost {visible:?}:\n{text}");
        }
        assert_absent(
            &text,
            &[
                "p%40ss",
                "p@ss",
                "w0rd",
                "prism_user",
                "db.internal",
                "hidden-Pw",
                "tok3n",
                "rpc-S3cret",
                "node-Pa55",
                "1111111111",
                "2222222222",
                "4a4a4a4a",
                "cHJpc210",
                "hunter2",
                "Zm9vYmFy",
                "MIIEvQ",
                "c2hvcnQ",
                "b3BlbnNzaC",
                "postgres",
                "\u{1b}",
                "IiIiIiIi",
                "tab-hunter3",
                "tab-flag-9",
            ],
        );
    }

    /// The sanitized lines a child's collector report shows after the child
    /// wrote `lines`, with `database_url` and test signing seeds enabled in
    /// the child's command.
    fn planted_report_lines(database_url: &str, lines: &[String]) -> Result<Vec<String>> {
        let directory = tempfile::tempdir()?;
        let planted = directory.path().join("planted.txt");
        std::fs::write(&planted, format!("{}\n", lines.join("\n")))?;
        let child = Process::spawn(
            Command::new("/bin/cat")
                .arg(&planted)
                .env("PRISM_DATABASE_URL", database_url)
                .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1"),
            directory.path().join("server-0.log"),
        )?;
        child.child.lock().wait()?;
        let text = report(&[("server-0".into(), Some(&child))], Some(database_url));
        Ok(text.lines().skip(1).map(String::from).collect())
    }

    #[test]
    fn sanitizer_removes_query_passwords_escaped_values_and_seed_arrays() -> Result<()> {
        let strings = |items: &[&str]| items.iter().map(|item| item.to_string()).collect();
        let plain = "postgres://prism:plain-Pw0rd@db/prism";
        let control = "tab\tpw7\u{1}z";
        // (label, database URL, planted lines, expected sanitized lines)
        let mut cases: Vec<(String, &str, Vec<String>, Vec<String>)> = vec![
            (
                "bare query password".into(),
                "postgres://prism@db/prism?password=QpS3cretValue",
                strings(&["auth failed using QpS3cretValue"]),
                strings(&["auth failed using [redacted]"]),
            ),
            (
                "percent-encoded query name and value".into(),
                "postgres://prism@db/prism?pass%77ord=enc%2Bpw9",
                strings(&["decoded enc+pw9", "written enc%2Bpw9"]),
                strings(&["decoded [redacted]", "written [redacted]"]),
            ),
            (
                "form-decoded plus in a query password".into(),
                "postgres://prism@db/prism?password=two+words9",
                strings(&["saw two words9 and two+words9"]),
                strings(&["saw [redacted] and [redacted]"]),
            ),
            (
                "repeated query password".into(),
                "postgres://prism@db/prism?password=first-pw1&password=second-pw2",
                strings(&["first-pw1 then second-pw2"]),
                strings(&["[redacted] then [redacted]"]),
            ),
            (
                "userinfo and query passwords as string literals".into(),
                "postgres://prism:Pa%22ss%5Cw0rd@db/prism?password=qp%22x%5Cy",
                strings(&[r#"user "Pa\"ss\\w0rd""#, r#"query "qp\"x\\y""#]),
                strings(&[r#"user "[redacted]""#, r#"query "[redacted]""#]),
            ),
            (
                "control characters as Debug and JSON escapes".into(),
                "postgres://prism:tab%09pw7%01z@db/prism",
                vec![
                    format!("debug {control:?}"),
                    format!("json {}", serde_json::to_string(control)?),
                ],
                strings(&[r#"debug "[redacted]""#, r#"json "[redacted]""#]),
            ),
            (
                // Normalization rewrites the raw control byte before secrets
                // are replaced, so the stored forms must include the rewrite.
                "control characters written raw".into(),
                "postgres://prism:tab%09pw7%01z@db/prism",
                vec![format!("raw {control} end")],
                strings(&["raw [redacted] end"]),
            ),
            (
                "malformed percent escape in a query password".into(),
                "postgres://prism@db/prism?password=%zz9q",
                strings(&["bad escape %zz9q"]),
                strings(&["bad escape [redacted]"]),
            ),
            (
                "non-ASCII password as a Debug literal".into(),
                "postgres://prism:p%C3%A9%22wd9x@db/prism",
                vec![format!("debug {:?}", "pé\"wd9x")],
                strings(&[r#"debug "[redacted]""#]),
            ),
            (
                "empty and valueless password parameters".into(),
                "postgres://prism@db/prism?password=&password",
                strings(&["password rejected for user prism at port 5432"]),
                strings(&["password rejected for user prism at port 5432"]),
            ),
            (
                "parameters SQLx does not read as a password".into(),
                "postgres://prism@db/prism?passwordx=keepme1&PASSWORD=keepme2&pass=keepme3",
                strings(&["keepme1 keepme2 keepme3"]),
                strings(&["keepme1 keepme2 keepme3"]),
            ),
            (
                "query password in a URL PRISM does not accept as a database".into(),
                "https://example.test/status?password=keepme5",
                strings(&["keepme5 stays visible"]),
                strings(&["keepme5 stays visible"]),
            ),
            (
                "database URL without a password".into(),
                "postgres://prism@db/prism",
                strings(&["prism connected without a password"]),
                strings(&["prism connected without a password"]),
            ),
            (
                "malformed database URL".into(),
                "not a url ?password=nope-pw",
                strings(&["nope-pw stays visible"]),
                strings(&["nope-pw stays visible"]),
            ),
            (
                "short arrays and identifiers".into(),
                plain,
                strings(&["ids [1, 2, 3] [17, 17, 3] abc123 0x7f port 5432"]),
                strings(&["ids [1, 2, 3] [17, 17, 3] abc123 0x7f port 5432"]),
            ),
        ];
        for byte in [0x11_u8, 0x22, 0x42, 0x43] {
            let seed = [byte; 32];
            for (format, rendering) in [
                ("{:?}", format!("{seed:?}")),
                ("{:x?}", format!("{seed:x?}")),
            ] {
                cases.push((
                    format!("test seed 0x{byte:02x} as {format}"),
                    plain,
                    vec![format!("seed {rendering} end")],
                    strings(&["seed [redacted] end"]),
                ));
            }
        }
        let mut failures = Vec::new();
        for (label, database_url, lines, expected) in &cases {
            let shown = planted_report_lines(database_url, lines)?;
            if &shown != expected {
                failures.push(format!("{label}: {shown:?}"));
            }
        }
        ensure!(
            failures.is_empty(),
            "{} of {} case(s) failed:\n{}",
            failures.len(),
            cases.len(),
            failures.join("\n")
        );
        Ok(())
    }

    #[test]
    fn sanitizer_withholds_ambiguous_armored_boundaries() {
        let lines = |items: &[&str]| {
            items
                .iter()
                .map(|item| item.to_string())
                .collect::<Vec<_>>()
        };
        let started_inside = sanitize(
            &lines(&[
                "dGFpbA==",
                "tail-of-key",
                "-----END RSA PRIVATE KEY-----",
                "VISIBLE-AFTER",
            ]),
            &[],
        );
        assert_eq!(
            started_inside,
            [
                "[withheld 3 line(s) of armored key material]",
                "VISIBLE-AFTER"
            ]
        );
        let never_ended = sanitize(
            &lines(&[
                "VISIBLE-BEFORE",
                "-----BEGIN EC PRIVATE KEY-----",
                "c2hvcnQ=",
                "more",
            ]),
            &[],
        );
        assert_eq!(
            never_ended,
            [
                "VISIBLE-BEFORE",
                "[withheld 3 line(s) of armored key material]"
            ]
        );
    }

    #[test]
    fn tail_window_that_cuts_a_key_block_or_secret_line_withholds_it() -> Result<()> {
        let directory = tempfile::tempdir()?;
        // The BEGIN marker and the start of the key body fall before the window.
        let key = directory.path().join("key.log");
        let mut text = String::from("-----BEGIN PRIVATE KEY-----\n");
        while text.len() < TAIL_BYTES as usize + 512 {
            text.push_str("c2hvcnQ=\n");
        }
        text.push_str("-----END PRIVATE KEY-----\nVISIBLE-AFTER-KEY\n");
        std::fs::write(&key, &text)?;
        let Tail::Read(tail) = read_tail(&key, false) else {
            panic!("key log unread");
        };
        assert_eq!(tail.inspected, TAIL_BYTES as usize);
        let shown = sanitize(&tail.lines, &[]);
        assert_eq!(shown.last().map(String::as_str), Some("VISIBLE-AFTER-KEY"));
        assert_absent(&shown.join("\n"), &["c2hvcnQ"]);

        // A running child is still writing a key whose BEGIN marker has left
        // the window and whose END is not yet written: no marker is visible,
        // and the body is alphabetic base64 (the 0x22 test seed).
        let unmarked = directory.path().join("unmarked.log");
        let mut text = String::from("-----BEGIN PRIVATE KEY-----\n");
        while text.len() < TAIL_BYTES as usize + 512 {
            text.push_str("IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIi\n");
        }
        text.push_str("IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI=\nIiI");
        std::fs::write(&unmarked, &text)?;
        let Tail::Read(tail) = read_tail(&unmarked, false) else {
            panic!("unmarked log unread");
        };
        assert!(!tail.lines.iter().any(|line| line.contains("BEGIN")));
        let shown = sanitize(&tail.lines, &[]);
        assert!(!shown.is_empty());
        assert_absent(&shown.join("\n"), &["IiIi"]);

        // The window starts inside a credential assignment, after its name.
        let cut = directory.path().join("cut.log");
        let head = "old\ncredential password=hunter2-boundary-secret\n";
        let bytes = format!("{head}{}", "filler-line-0000\n".repeat(963)).into_bytes();
        let window_start = bytes.len() - TAIL_BYTES as usize;
        let value = head.find("hunter2").expect("value in head")..head.len() - 1;
        assert!(
            value.contains(&window_start),
            "window starts at {window_start}"
        );
        std::fs::write(&cut, &bytes)?;
        let Tail::Read(tail) = read_tail(&cut, false) else {
            panic!("cut log unread");
        };
        assert_eq!((tail.dropped, tail.size), (1, bytes.len() as u64));
        assert!(tail.lines.iter().all(|line| line == "filler-line-0000"));
        Ok(())
    }

    #[test]
    fn tail_handles_partial_malformed_missing_and_unreadable_logs() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let partial = directory.path().join("partial.log");
        std::fs::write(&partial, b"a\r\nb\npassword=hunter2-par")?;
        let Tail::Read(running) = read_tail(&partial, false) else {
            panic!("partial log unread");
        };
        assert_eq!(
            (running.lines, running.dropped),
            (vec!["a".to_owned(), "b".to_owned()], 1)
        );
        let Tail::Read(exited) = read_tail(&partial, true) else {
            panic!("partial log unread");
        };
        assert_eq!(exited.lines.len(), 3);
        assert_eq!(sanitize(&exited.lines, &[])[2], "password= [redacted]");

        let malformed = directory.path().join("malformed.log");
        std::fs::write(&malformed, b"ok\xff\xfe\x1b]0;title\x07done\n")?;
        let Tail::Read(tail) = read_tail(&malformed, false) else {
            panic!("malformed log unread");
        };
        let shown = sanitize(&tail.lines, &[]);
        assert!(
            shown[0].starts_with("ok\u{fffd}\u{fffd}\u{fffd}]0;title\u{fffd}done"),
            "{shown:?}"
        );

        let empty = directory.path().join("empty.log");
        std::fs::write(&empty, b"")?;
        assert!(
            matches!(read_tail(&empty, false), Tail::Read(tail) if tail.size == 0 && tail.lines.is_empty())
        );
        assert!(matches!(
            read_tail(&directory.path().join("absent.log"), false),
            Tail::Missing
        ));
        assert!(matches!(
            read_tail(directory.path(), false),
            Tail::Unreadable(_)
        ));
        Ok(())
    }

    #[test]
    fn budget_keeps_newest_whole_lines_after_sanitizing() {
        let many: Vec<String> = (0..100).map(|index| format!("line-{index}")).collect();
        let shown = budget(&many);
        assert_eq!(shown.len(), MAX_LINES);
        assert_eq!((shown[0], shown[MAX_LINES - 1]), ("line-60", "line-99"));

        let wide: Vec<String> = (0..20)
            .map(|index| format!("{index:04}{}", "w".repeat(996)))
            .collect();
        let shown = budget(&wide);
        assert_eq!(shown.len(), MAX_TEXT_BYTES / 1001);
        assert!(shown.iter().map(|line| line.len() + 1).sum::<usize>() <= MAX_TEXT_BYTES);
        assert!(shown.last().is_some_and(|line| line.starts_with("0019")));

        let oversized = vec!["older".to_owned(), "x".repeat(MAX_TEXT_BYTES)];
        assert!(budget(&oversized).is_empty());

        // A secret longer than the budget is removed before truncation, not cut by it.
        let secret = vec![format!("signing_seed={}", "q".repeat(MAX_TEXT_BYTES * 2))];
        assert_eq!(
            budget(&sanitize(&secret, &[])),
            ["signing_seed= [redacted]"]
        );
    }

    #[test]
    fn url_redaction_handles_a_multibyte_character_before_the_scheme() {
        let lines: Vec<String> = [
            "•https://example.test/safe",
            "🚀postgres://u:emoji-pw@db/prism",
            "éhttps://user:accent-tok3n@example.test/x",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let text = sanitize(&lines, &[]);
        assert_eq!(
            text,
            [
                "•https://example.test/safe",
                "🚀[redacted-url]",
                "é[redacted-url]"
            ]
        );
        assert_absent(&text.join("\n"), &["emoji-pw", "accent-tok3n"]);
    }

    #[test]
    fn multibyte_url_separator_in_a_child_log_keeps_original_error_and_witness() -> Result<()> {
        let (mut startup, directory, serial) = startup()?;
        let node = Process::spawn(
            Command::new("/bin/sh").args([
                "-c",
                "printf '%s\\n' WITNESS-UNICODE-LINE '•postgres://u:unicode-pw@db/prism'",
            ]),
            directory.join("qbit.log"),
        )?;
        node.child.lock().wait()?;
        startup.node = Some(node);

        let error = startup.fail(anyhow::anyhow!("original unicode failure"));
        assert!(serial.try_lock().is_ok(), "failed startup kept the guard");
        assert_eq!(error.root_cause().to_string(), "original unicode failure");
        let text = format!("{error:?}");
        assert!(!text.contains("collector panicked"), "{text}");
        assert!(
            text.contains("--- qbit node [exited (code 0)]: qbit.log"),
            "{text}"
        );
        assert!(text.contains("WITNESS-UNICODE-LINE"), "{text}");
        assert!(text.contains("•[redacted-url]"), "{text}");
        assert_absent(&text, &["unicode-pw"]);
        assert!(!directory.exists(), "startup directory survived failure");
        Ok(())
    }
}
