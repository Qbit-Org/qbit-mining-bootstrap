//! #575 item 1: the cutover rehearsal.
//!
//! Takes a drained 2.x.x ledger through `docs/prism-rust-migration.md`'s
//! cutover exactly as an operator runs it (`check-config`, `migrate`,
//! `import-audits`, then one native frontend and `self-check`), and holds the
//! result to what the source held:
//!
//! - the recovery evidence (`scripts/prism-recovery-evidence.{sql,py}`) is
//!   identical before and after, as the recovery procedure requires;
//! - every address's owed, lifetime and pending balance, read before with the
//!   public API's own SQL over the 2.x.x tables and after from the running
//!   frontend's `/public/v1/miners/<address>`;
//! - the payout window: 2.x.x's `qbit_prism_window` before, the native
//!   snapshot after, share for share, and the prior balances beside it;
//! - the row count and the money and work sums of every 2.x.x table;
//! - a frontend on the migrated ledger passes `self-check` and issues a
//!   first Stratum job.
//!
//! It records each step's wall time and the longest lock each held, from a
//! side connection sampling `pg_locks` every 10 ms. `rehearse_dump` runs the
//! same on a supplied `pg_dump`, restored into a private PostgreSQL cluster it
//! creates and removes; it connects to nothing else.
use super::recovery;
use anyhow::{bail, ensure, Context, Result};
use qbit_prism_server::ledger::Ledger;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection, PgPool, Row};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

#[path = "ledger_2x_seed.rs"]
pub mod seed;

#[path = "rehearsal_node.rs"]
pub mod node;

/// The signing keys the rehearsal's frontend runs with
/// (`PRISM_ALLOW_TEST_SIGNING_SEEDS`). A migrated ledger has no cluster
/// fingerprint yet, so the first frontend pins its own.
pub const COINBASE_SEED: &str = "11";
pub const LEDGER_SEED: &str = "22";
/// How often the side connection samples `pg_locks`.
const LOCK_SAMPLE: Duration = Duration::from_millis(10);
/// The `application_name` every rehearsal tool connection carries, so the
/// sampler sees only them.
const TOOL_APPLICATION: &str = "prism-cutover-rehearsal";

/// A database the rehearsal migrates.
pub struct Target {
    /// `search_path` pinned to `schema`.
    pub url: String,
    pub schema: String,
    pub admin: PgPool,
    pub pool: PgPool,
}

impl Target {
    fn recovery(&self) -> recovery::Database {
        recovery::Database {
            admin: self.admin.clone(),
            pool: self.pool.clone(),
            schema: self.schema.clone(),
            url: self.url.clone(),
        }
    }

    fn tool_url(&self) -> Result<String> {
        let mut url = url::Url::parse(&self.url)?;
        url.query_pairs_mut()
            .append_pair("application_name", TOOL_APPLICATION);
        Ok(url.into())
    }
}

/// The node a rehearsal frontend starts against.
#[derive(Clone, Debug)]
pub enum NodeChoice {
    /// [`node::RehearsalNode`], serving the ledger's own chain, with a lab
    /// configuration and test signing keys.
    Ledger { tag: String },
    /// The operator's reviewed production environment (`ENV_FILE`): its
    /// synced node, chain, genesis pin, signing keys and pool fee, as the
    /// real cutover runs with them. The rehearsal replaces only the database
    /// URL, the listen addresses and ports, and the instance ID.
    Operator { env: Vec<(String, String)> },
    /// A lab node the caller runs, such as the weekly scenario's regtest.
    External {
        url: String,
        user: String,
        password: String,
        chain: String,
    },
}

pub struct Options {
    pub pg_bin: PathBuf,
    /// `import-audits --root`; an empty directory when there are no bodies.
    pub audit_root: PathBuf,
    /// The trusted ledger key the history was signed with.
    pub ledger_public_key: String,
    pub node: NodeChoice,
    pub start_frontend: bool,
    /// Settings added to every command, such as
    /// `PRISM_DATABASE_STATEMENT_TIMEOUT_MS`.
    pub extra_env: Vec<(String, String)>,
    /// The evidence of the database the dump was taken from, when the caller
    /// has it: the restore must reproduce it.
    pub source_evidence: Option<Value>,
}

/// One lock a rehearsal step held, as the sampler saw it.
#[derive(Clone, Debug, Serialize)]
pub struct LockHold {
    pub object: String,
    pub mode: String,
    /// From the first sample that saw it to the last; a lock seen once was
    /// held for less than one sampling interval.
    pub held_ms: u64,
    /// Whether it was ever seen waiting rather than granted.
    pub waited: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct StepReport {
    pub name: String,
    pub wall_ms: u64,
    pub longest_lock: Option<LockHold>,
    pub longest_access_exclusive: Option<LockHold>,
    pub locks: Vec<LockHold>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Check {
    pub name: String,
    pub pass: bool,
    pub detail: String,
}

#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub schema: &'static str,
    pub source: Value,
    pub rows: Value,
    pub steps: Vec<StepReport>,
    pub checks: Vec<Check>,
    /// Per operator command, the statements it spent the most sampled time
    /// in, with that time in milliseconds.
    pub statements: BTreeMap<String, Vec<(String, u64)>>,
    pub lock_sample_ms: u64,
    pub pass: bool,
}

impl Report {
    pub fn new(source: Value) -> Self {
        Self {
            schema: "qbit.prism.cutover-rehearsal.v1",
            source,
            lock_sample_ms: LOCK_SAMPLE.as_millis() as u64,
            ..Self::default()
        }
    }

    fn check(&mut self, name: &str, pass: bool, detail: impl Into<String>) {
        self.checks.push(Check {
            name: name.to_owned(),
            pass,
            detail: detail.into(),
        });
    }

    fn step(&mut self, name: &str, wall: Duration, holds: Vec<LockHold>) {
        let longest_lock = holds.first().cloned();
        let longest_access_exclusive = holds
            .iter()
            .find(|hold| hold.mode == "AccessExclusiveLock")
            .cloned();
        self.steps.push(StepReport {
            name: name.to_owned(),
            wall_ms: wall.as_millis() as u64,
            longest_lock,
            longest_access_exclusive,
            locks: holds.into_iter().take(5).collect(),
        });
    }

    /// Settles the verdict: every check passed.
    pub fn finish(&mut self) {
        self.pass = !self.checks.is_empty() && self.checks.iter().all(|check| check.pass);
    }

    /// The pass/fail report an operator reads.
    pub fn render(&self) -> String {
        let mut text = format!(
            "PRISM cutover rehearsal: {}\n\nSteps (wall time; longest lock held, sampled every {} ms):\n",
            if self.pass { "PASS" } else { "FAIL" },
            self.lock_sample_ms
        );
        for step in &self.steps {
            let describe =
                |hold: &LockHold| format!("{} {} {} ms", hold.mode, hold.object, hold.held_ms);
            let mut lock = step
                .longest_lock
                .as_ref()
                .map_or_else(|| "no lock sampled".to_owned(), describe);
            if let Some(exclusive) = &step.longest_access_exclusive {
                if Some(exclusive.object.as_str())
                    != step.longest_lock.as_ref().map(|hold| hold.object.as_str())
                    || step.longest_lock.as_ref().map(|hold| hold.mode.as_str())
                        != Some("AccessExclusiveLock")
                {
                    lock += &format!("; longest ACCESS EXCLUSIVE {}", describe(exclusive));
                }
            }
            text += &format!("  {:<44} {:>10} ms   {lock}\n", step.name, step.wall_ms);
        }
        if let Some(statements) = self.statements.get("migrate") {
            text += "\nmigrate's longest statements (sampled):\n";
            for (statement, took) in statements.iter().take(5) {
                text += &format!("  {took:>10} ms  {statement}\n");
            }
        }
        text += "\nChecks:\n";
        for check in &self.checks {
            text += &format!(
                "  [{}] {:<34} {}\n",
                if check.pass { "PASS" } else { "FAIL" },
                check.name,
                check.detail
            );
        }
        text
    }
}

// ---------------------------------------------------------------------------
// Lock sampling.
// ---------------------------------------------------------------------------

/// One lock of one session: its object and mode.
type LockKey = (i32, String, String);

#[derive(Default)]
struct Samples {
    /// Sampled time each tool statement was seen running, by its text.
    statements: HashMap<String, Duration>,
    /// Locks seen in the latest sample: when the continuous hold began, when
    /// it was last seen, and whether it was ever waited for.
    open: HashMap<LockKey, (Duration, Duration, bool)>,
    /// Holds that ended: a lock no longer seen closes its interval, so a lock
    /// released and taken again is two holds, not one.
    closed: Vec<(LockKey, Duration, Duration, bool)>,
    /// (elapsed, step) at each change of step.
    timeline: Vec<(Duration, String)>,
}

impl Samples {
    fn close_unseen(&mut self, seen: &std::collections::HashSet<LockKey>) {
        let ended: Vec<LockKey> = self
            .open
            .keys()
            .filter(|key| !seen.contains(*key))
            .cloned()
            .collect();
        for key in ended {
            let (first, last, waited) = self.open.remove(&key).expect("listed above");
            self.closed.push((key, first, last, waited));
        }
    }
}

/// A side connection sampling the tool's locks and statements.
struct LockSampler {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<Result<Samples>>,
    started: Instant,
}

/// The step an online migration statement belongs to. The migration
/// transaction sends each migration file whole, and those files name these
/// functions in their definitions, so only a statement that *is* the call
/// counts: the online runners send each on its own.
fn classify(query: &str, previous: &str) -> String {
    let query = query.trim_start().to_ascii_lowercase();
    let online = [
        (
            "select qbit_prism_share_ledger_convert_swap(",
            "migrate: 017 swap",
        ),
        (
            "select qbit_prism_share_ledger_convert_validate(",
            "migrate: 017 validate",
        ),
        (
            "select qbit_prism_share_ledger_convert_prepare(",
            "migrate: 017 prepare",
        ),
        (
            "alter table qbit_share_ledger validate constraint",
            "migrate: 017 validate",
        ),
        (
            "select qbit_prism_share_partition_ensure(",
            "migrate: 017 lead partitions",
        ),
        (
            "create index concurrently",
            "migrate: 013, 024 and 031 concurrent indexes",
        ),
        // 031's catalog steps after its leaves: the parent ON ONLY, under a
        // SHARE lock on the ledger, and each leaf's attachment.
        (
            "create index qbit_share_ledger_origin_seq_idx on only",
            "migrate: 031 parent index and leaf attachments",
        ),
        (
            "alter index \"qbit_share_ledger_origin_seq_idx\" attach partition",
            "migrate: 031 parent index and leaf attachments",
        ),
        ("drop index concurrently", "migrate: 013 concurrent indexes"),
        // One batch of 002's backfill (#582); the migration transaction no
        // longer sends this statement.
        (
            "insert into qbit_prism_share_hashes",
            "migrate: 002 share-hash backfill",
        ),
    ]
    .into_iter()
    .find(|(prefix, _)| query.starts_with(prefix))
    .map(|(_, step)| step);
    match online {
        Some(step) => step.to_owned(),
        None if previous.is_empty() => "migrate: transaction (001, 002-020)".to_owned(),
        None => previous.to_owned(),
    }
}

impl LockSampler {
    async fn start(admin_url: &str, schema: &str) -> Result<Self> {
        let mut connection = PgConnection::connect(admin_url).await?;
        let stop = Arc::new(AtomicBool::new(false));
        let started = Instant::now();
        let schema = schema.to_owned();
        let flag = stop.clone();
        let task = tokio::spawn(async move {
            let mut samples = Samples::default();
            let mut step = String::new();
            let mut previous = Duration::ZERO;
            while !flag.load(Ordering::Relaxed) {
                let at = started.elapsed();
                let since = at - previous;
                previous = at;
                let mut running = std::collections::HashSet::new();
                let mut seen = std::collections::HashSet::new();
                // A relation is named with its OID, so the release table and
                // the partitioned parent that takes its name stay apart; an
                // advisory lock by its full key.
                let rows = sqlx::query("SELECT a.pid,COALESCE(a.query,'') AS query,a.state,l.mode,l.granted,CASE WHEN l.locktype='relation' THEN c.relname||'#'||l.relation::text ELSE 'advisory:'||l.classid::text||':'||l.objid::text||':'||l.objsubid::text END AS object FROM pg_stat_activity a LEFT JOIN pg_locks l ON l.pid=a.pid AND l.locktype IN ('relation','advisory') LEFT JOIN pg_class c ON c.oid=l.relation LEFT JOIN pg_namespace n ON n.oid=c.relnamespace WHERE a.application_name=$1 AND a.pid<>pg_backend_pid() AND (l.locktype IS NULL OR l.locktype='advisory' OR n.nspname=$2)")
                    .bind(TOOL_APPLICATION)
                    .bind(&schema)
                    .fetch_all(&mut connection)
                    .await?;
                for row in &rows {
                    let pid: i32 = row.try_get("pid")?;
                    let active =
                        row.try_get::<Option<String>, _>("state")?.as_deref() == Some("active");
                    if active {
                        let query: String = row.try_get("query")?;
                        if running.insert((pid, query.clone())) {
                            let text = query.split_whitespace().collect::<Vec<_>>().join(" ");
                            *samples
                                .statements
                                .entry(text.chars().take(160).collect())
                                .or_default() += since;
                        }
                        let next = classify(&query, &step);
                        if next != step {
                            step = next;
                            samples.timeline.push((at, step.clone()));
                        }
                    }
                    let (Some(object), Some(mode)) = (
                        row.try_get::<Option<String>, _>("object")?,
                        row.try_get::<Option<String>, _>("mode")?,
                    ) else {
                        continue;
                    };
                    let granted = row.try_get::<Option<bool>, _>("granted")?.unwrap_or(true);
                    let key = (pid, object, mode);
                    seen.insert(key.clone());
                    let entry = samples.open.entry(key).or_insert((at, at, false));
                    entry.1 = at;
                    entry.2 |= !granted;
                }
                samples.close_unseen(&seen);
                tokio::time::sleep(LOCK_SAMPLE).await;
            }
            samples.close_unseen(&std::collections::HashSet::new());
            connection.close().await?;
            Ok(samples)
        });
        Ok(Self {
            stop,
            task,
            started,
        })
    }

    /// Stops sampling and returns, per step, the continuous lock holds that
    /// began in it, longest first, with each step's wall time; and the
    /// statements the tool spent the most sampled time in.
    async fn finish(
        self,
        fallback: &str,
    ) -> Result<(Vec<(String, Duration, Vec<LockHold>)>, Vec<(String, u64)>)> {
        let total = self.started.elapsed();
        self.stop.store(true, Ordering::Relaxed);
        let samples = self.task.await??;
        let mut timeline = samples.timeline;
        if timeline.is_empty() {
            timeline.push((Duration::ZERO, fallback.to_owned()));
        } else {
            timeline[0].0 = Duration::ZERO;
        }
        let mut steps: Vec<(String, Duration, Vec<LockHold>)> = Vec::new();
        for (index, (from, name)) in timeline.iter().enumerate() {
            let to = timeline.get(index + 1).map_or(total, |(at, _)| *at);
            match steps.iter_mut().find(|(step, ..)| step == name) {
                Some(step) => step.1 += to - *from,
                None => steps.push((name.clone(), to - *from, Vec::new())),
            }
        }
        for ((_, object, mode), first, last, waited) in samples.closed {
            let step = timeline
                .iter()
                .rev()
                .find(|(at, _)| *at <= first)
                .map_or(&timeline[0].1, |(_, name)| name);
            // The OID only keeps relations apart while sampling.
            let object = object
                .split_once('#')
                .map_or(object.as_str(), |(name, _)| name)
                .to_owned();
            let hold = LockHold {
                object,
                mode,
                held_ms: (last - first).as_millis() as u64,
                waited,
            };
            if let Some(entry) = steps.iter_mut().find(|(name, ..)| name == step) {
                entry.2.push(hold);
            }
        }
        for (_, _, holds) in &mut steps {
            holds.sort_by(|a, b| {
                b.held_ms
                    .cmp(&a.held_ms)
                    .then_with(|| strength(&b.mode).cmp(&strength(&a.mode)))
                    .then_with(|| a.object.cmp(&b.object))
            });
            // One line per lock, its longest hold across the sessions.
            let mut seen = std::collections::HashSet::new();
            holds.retain(|hold| seen.insert((hold.object.clone(), hold.mode.clone())));
        }
        let mut statements: Vec<(String, u64)> = samples
            .statements
            .into_iter()
            .map(|(text, took)| (text, took.as_millis() as u64))
            .collect();
        statements.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        statements.truncate(8);
        Ok((steps, statements))
    }
}

fn strength(mode: &str) -> u8 {
    match mode {
        "AccessExclusiveLock" => 8,
        "ExclusiveLock" => 7,
        "ShareRowExclusiveLock" => 6,
        "ShareLock" => 5,
        "ShareUpdateExclusiveLock" => 4,
        "RowExclusiveLock" => 3,
        "RowShareLock" => 2,
        _ => 1,
    }
}

// ---------------------------------------------------------------------------
// Operator commands.
// ---------------------------------------------------------------------------

/// Ports for one frontend's listeners, each held by a bound listener from
/// its pick until just before the frontend spawns (#639). A dropped
/// ephemeral bind can be picked again for another of the three, or handed
/// to another socket while `check-config`, `migrate` and `import-audits` run;
/// the frontend then fails to bind it.
pub(crate) struct Ports {
    pub(crate) stratum: u16,
    pub(crate) highdiff: u16,
    pub(crate) api: u16,
    held: Vec<std::net::TcpListener>,
}

impl Ports {
    pub(crate) fn reserve() -> Result<Self> {
        let mut held = Vec::with_capacity(3);
        let mut port = || -> Result<u16> {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            let port = listener.local_addr()?.port();
            held.push(listener);
            Ok(port)
        };
        let (stratum, highdiff, api) = (port()?, port()?, port()?);
        Ok(Self {
            stratum,
            highdiff,
            api,
            held,
        })
    }

    /// Closes the reservations so the frontend can bind them; called
    /// immediately before it spawns.
    pub(crate) fn release(&mut self) {
        self.held.clear();
    }

    pub(crate) fn held(&self) -> usize {
        self.held.len()
    }
}

/// Names each port, so a bind failure shows which listener collided.
impl std::fmt::Display for Ports {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stratum 127.0.0.1:{}, high-diff 127.0.0.1:{}, audit 127.0.0.1:{}",
            self.stratum, self.highdiff, self.api
        )
    }
}

/// The environment of the rehearsal's frontend and of every operator
/// command, cleared of anything inherited (EP-CONFIG).
/// [`lab_env`], or for [`NodeChoice::Operator`] the operator's own
/// environment with only what must be the rehearsal's replaced.
fn command_env(
    database_url: &str,
    node: &NodeChoice,
    lab_node: &(String, String, String, String),
    ports: &Ports,
) -> Vec<(String, String)> {
    let NodeChoice::Operator { env } = node else {
        return lab_env(database_url, lab_node, ports);
    };
    let replaced = [
        ("PRISM_DATABASE_URL", database_url.to_owned()),
        ("PRISM_PUBLIC_DATABASE_URL", database_url.to_owned()),
        ("PRISM_POSTGRES_INIT_SCHEMA", "0".to_owned()),
        ("PRISM_INSTANCE_ID", "cutover-rehearsal".to_owned()),
        ("PRISM_STRATUM_BIND", "127.0.0.1".to_owned()),
        ("PRISM_STRATUM_PORT", ports.stratum.to_string()),
        ("PRISM_STRATUM_HIGHDIFF_PORT", ports.highdiff.to_string()),
        ("PRISM_AUDIT_BIND", "127.0.0.1".to_owned()),
        ("PRISM_AUDIT_PORT", ports.api.to_string()),
        ("PRISM_PUBLIC_CACHE_ENABLED", "0".to_owned()),
        ("PRISM_CTV_BROADCASTER_ENABLED", "0".to_owned()),
    ];
    let mut merged: Vec<(String, String)> = env
        .iter()
        .filter(|(name, _)| !replaced.iter().any(|(replace, _)| replace == name))
        .cloned()
        .collect();
    merged.extend(
        replaced
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value)),
    );
    merged
}

/// A lab frontend's environment: test signing keys, a 200 bps pool fee.
fn lab_env(
    database_url: &str,
    node: &(String, String, String, String),
    ports: &Ports,
) -> Vec<(String, String)> {
    let (rpc_url, user, password, chain) = node;
    [
        ("PRISM_DATABASE_URL", database_url),
        ("PRISM_POSTGRES_INIT_SCHEMA", "0"),
        ("PRISM_DATABASE_MAX_CONNECTIONS", "8"),
        ("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1"),
        ("PRISM_MANIFEST_SIGNING_SEED_HEX", &COINBASE_SEED.repeat(32)),
        (
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX",
            &LEDGER_SEED.repeat(32),
        ),
        ("PRISM_INSTANCE_ID", "cutover-rehearsal"),
        ("QBIT_CHAIN", chain),
        ("QBIT_RPC_URL", rpc_url),
        ("QBIT_RPC_USER", user),
        ("QBIT_RPC_PASSWORD", password),
        ("QBIT_PRODUCTION", "0"),
        ("PRISM_MIN_PEERS", "1"),
        ("PRISM_STRATUM_BIND", "127.0.0.1"),
        ("PRISM_STRATUM_PORT", &ports.stratum.to_string()),
        ("PRISM_STRATUM_HIGHDIFF_PORT", &ports.highdiff.to_string()),
        ("PRISM_AUDIT_BIND", "127.0.0.1"),
        ("PRISM_AUDIT_PORT", &ports.api.to_string()),
        ("PRISM_RUNTIME_WORKERS", "2"),
        ("PRISM_BLOCKPOLL_SECONDS", "0.5"),
        ("PRISM_PUBLIC_CACHE_ENABLED", "0"),
        ("PRISM_CTV_SETTLEMENT_ENABLED", "0"),
        ("PRISM_CTV_BROADCASTER_ENABLED", "0"),
        ("PRISM_POOL_FEE_ENABLED", "1"),
        ("PRISM_POOL_FEE_BPS", "200"),
        (
            "PRISM_POOL_FEE_ADDRESS",
            &seed::synthetic_address("prism-rehearsal-pool-fee"),
        ),
        ("RUST_LOG", "warn"),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value.to_owned()))
    .collect()
}

fn server_command(env: &[(String, String)]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    command.env_clear();
    command.env("PATH", std::env::var_os("PATH").unwrap_or_default());
    command.envs(env.iter().map(|(name, value)| (name, value)));
    command.kill_on_drop(true);
    command
}

/// Runs one operator command: whether it succeeded, its standard output,
/// and its standard error.
async fn run_tool(env: &[(String, String)], args: &[&str]) -> Result<(bool, String, String)> {
    let output = server_command(env)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await?;
    Ok((
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    ))
}

/// A command's outcome line: its first output line, or its error.
fn outcome((ok, stdout, stderr): &(bool, String, String)) -> String {
    if *ok {
        return first_line(stdout);
    }
    let plain = strip_ansi(&format!("{stderr}\n{stdout}"));
    plain
        .lines()
        .rev()
        .find(|line| line.trim_start().starts_with("Error"))
        .map_or_else(|| first_line(&plain), first_line)
}

/// Log lines without their terminal colour codes.
fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut characters = text.chars();
    while let Some(character) = characters.next() {
        if character == '\u{1b}' {
            for next in characters.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            plain.push(character);
        }
    }
    plain
}

/// Runs one operator command under the lock sampler and records its steps.
async fn measured(
    report: &mut Report,
    target: &Target,
    admin_url: &str,
    env: &[(String, String)],
    name: &str,
    args: &[&str],
) -> Result<(bool, String, String)> {
    let sampler = LockSampler::start(admin_url, &target.schema).await?;
    let started = Instant::now();
    let result = run_tool(env, args).await;
    let wall = started.elapsed();
    let (steps, statements) = sampler.finish(name).await?;
    report.statements.insert(name.to_owned(), statements);
    if name == "migrate" {
        for (step, took, holds) in steps {
            report.step(&step, took, holds);
        }
        report.step("migrate (total)", wall, Vec::new());
    } else {
        let holds = steps.into_iter().flat_map(|(_, _, holds)| holds).collect();
        report.step(name, wall, holds);
    }
    result
}

// ---------------------------------------------------------------------------
// Invariants.
// ---------------------------------------------------------------------------

/// The 2.x.x tables and what the rehearsal sums in each. Every column named
/// is a 2.x.x column, so the same query reads the source and the migrated
/// ledger.
const TABLE_FACTS: &[(&str, &str)] = &[
    ("qbit_share_ledger", "count(*)::text||'/'||count(*) FILTER (WHERE accepted)::text||'/'||COALESCE(sum(share_difficulty) FILTER (WHERE accepted),0)::text||'/'||COALESCE(max(share_seq),0)::text||'/'||count(*) FILTER (WHERE credit_policy IS NOT NULL)::text"),
    ("qbit_pool_blocks", "count(*)::text||'/'||COALESCE(string_agg(chain_state||':'||maturity_state,',' ORDER BY block_hash),'')"),
    ("qbit_pool_audit_bundles", "count(*)::text||'/'||count(body_uri)::text||'/'||COALESCE(md5(string_agg(block_hash||audit_bundle_sha256,',' ORDER BY block_hash)),'')"),
    ("qbit_payout_carry_forward", "count(*)::text||'/'||COALESCE(sum(gross_amount_sats),0)::text||'/'||COALESCE(sum(onchain_amount_sats),0)::text||'/'||COALESCE(sum(carry_forward_balance_sats),0)::text"),
    ("qbit_payout_carry_forward_current", "count(*)::text||'/'||COALESCE(sum(balance_sats),0)::text||'/'||COALESCE(sum(active_row_count),0)::text"),
    ("qbit_pool_payout_entries", "count(*)::text||'/'||COALESCE(sum(onchain_amount_sats),0)::text||'/'||count(*) FILTER (WHERE maturity_state='immature')::text"),
    ("qbit_block_candidate_outbox", "count(*)::text||'/'||COALESCE(string_agg(state,',' ORDER BY block_hash),'')"),
    ("qbit_ctv_fanout_sets", "count(*)::text||'/'||COALESCE(sum(fanout_output_sum_sats),0)::text"),
    ("qbit_ctv_fanout_artifacts", "count(*)::text||'/'||COALESCE(string_agg(settlement_status,',' ORDER BY fanout_txid),'')"),
    ("qbit_ctv_fanout_broadcast_attempts", "count(*)::text"),
    ("qbit_hashrate_rollup_pool", "count(*)::text||'/'||COALESCE(sum(accepted_share_count),0)::text||'/'||COALESCE(sum(accepted_share_difficulty),0)::text||'/'||COALESCE(md5(string_agg(grain_seconds||':'||bucket_epoch||':'||accepted_share_count||':'||accepted_share_difficulty,',' ORDER BY grain_seconds,bucket_epoch)),'')"),
    ("qbit_hashrate_rollup_miner", "count(*)::text||'/'||COALESCE(sum(accepted_share_count),0)::text||'/'||COALESCE(sum(accepted_share_difficulty),0)::text||'/'||COALESCE(md5(string_agg(grain_seconds||':'||bucket_epoch||':'||miner_id||':'||accepted_share_count||':'||accepted_share_difficulty,',' ORDER BY grain_seconds,bucket_epoch,miner_id)),'')"),
    ("qbit_hashrate_rollup_progress", "COALESCE(max(last_share_seq),-1)::text"),
    ("qbit_worker_difficulty", "count(*)::text||'/'||COALESCE(md5(string_agg(listener||':'||worker_username||':'||difficulty||':'||evidence_at||':'||updated_at,',' ORDER BY listener,worker_username)),'')"),
    ("qbit_ledger_writer_lease", "COALESCE(string_agg(writer_id||':'||writer_epoch||':'||writer_session_token||':'||lease_expires_at||':'||updated_at,','),'')"),
    // An independent recomputation of every balance from the carry rows,
    // not from the summary table the owed balances read.
    ("qbit_recomputed_carry_forward_balances()", "count(*)::text||'/'||COALESCE(md5(string_agg(miner_id||':'||payout_order_key||':'||encode(p2mr_program,'hex')||':'||balance_sats,',' ORDER BY p2mr_program)),'')"),
];
/// The facts a running frontend may change without touching history: it
/// prunes retained vardiff past its evidence TTL.
const FRONTEND_MAY_CHANGE: &[&str] = &["qbit_worker_difficulty"];

/// Every address's balances as the public API computes them
/// (`src/api/public.rs`, `miner`): owed, lifetime and pending maturity.
const BALANCES_SQL: &str = "WITH ids AS (SELECT DISTINCT miner_id FROM qbit_payout_carry_forward),\
 owed AS (SELECT miner_id,sum(owed_balance_sats) AS v FROM qbit_current_owed_balances() GROUP BY miner_id),\
 life AS (SELECT c.miner_id,sum(c.gross_amount_sats) AS v FROM qbit_payout_carry_forward c JOIN qbit_pool_blocks b USING(block_hash) WHERE c.maturity_state<>'reversed' AND b.chain_state='confirmed' AND b.maturity_state<>'reversed' GROUP BY c.miner_id),\
 pend AS (SELECT c.miner_id,sum(GREATEST(c.onchain_amount_sats-c.settlement_fee_sats,0)) AS v FROM qbit_payout_carry_forward c JOIN qbit_pool_blocks b USING(block_hash) WHERE c.action='onchain' AND c.maturity_state='immature' AND b.chain_state='confirmed' AND b.maturity_state='immature' GROUP BY c.miner_id)\
 SELECT ids.miner_id,COALESCE(owed.v,0)::text AS owed,COALESCE(life.v,0)::text AS lifetime,COALESCE(pend.v,0)::text AS pending FROM ids LEFT JOIN owed USING(miner_id) LEFT JOIN life USING(miner_id) LEFT JOIN pend USING(miner_id) ORDER BY ids.miner_id";

/// Balances per address: (owed, lifetime, pending).
pub type Balances = BTreeMap<String, (String, String, String)>;

pub async fn balances(pool: &PgPool) -> Result<Balances> {
    sqlx::query(BALANCES_SQL)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get("miner_id")?,
                (
                    row.try_get("owed")?,
                    row.try_get("lifetime")?,
                    row.try_get("pending")?,
                ),
            ))
        })
        .collect()
}

pub async fn table_facts(pool: &PgPool) -> Result<BTreeMap<String, String>> {
    let mut facts = BTreeMap::new();
    for (table, expression) in TABLE_FACTS {
        let fact: String = sqlx::query_scalar(&format!("SELECT {expression} FROM {table}"))
            .fetch_one(pool)
            .await
            .with_context(|| format!("reading {table}"))?;
        facts.insert((*table).to_owned(), fact);
    }
    Ok(facts)
}

/// The payout window at the latest network difficulty the ledger records,
/// and the prior balances beside it.
#[derive(Debug, PartialEq, Eq)]
pub struct Window {
    pub network_difficulty: u128,
    /// (share_seq, share_id, miner_id, share_difficulty, credit_policy),
    /// newest first.
    pub shares: Vec<(i64, String, String, String, Option<String>)>,
    /// (recipient, order key, program hex, balance).
    pub prior_balances: Vec<(String, String, String, String)>,
}

async fn latest_network_difficulty(pool: &PgPool) -> Result<u128> {
    let text: Option<String> = sqlx::query_scalar("SELECT network_difficulty::text FROM qbit_share_ledger WHERE accepted ORDER BY share_seq DESC LIMIT 1")
        .fetch_optional(pool)
        .await?;
    Ok(text.map(|text| text.parse()).transpose()?.unwrap_or(1))
}

/// 2.x.x's own window function over the source, anchored now.
pub async fn legacy_window(pool: &PgPool) -> Result<Window> {
    let network_difficulty = latest_network_difficulty(pool).await?;
    let weight = network_difficulty * qbit_prism::PRISM_WINDOW_MULTIPLIER;
    let shares = sqlx::query_as("SELECT share_seq,share_id,miner_id,share_difficulty::text,credit_policy FROM qbit_prism_window(clock_timestamp(),$1::numeric) ORDER BY share_seq DESC")
        .bind(weight.to_string())
        .fetch_all(pool)
        .await?;
    let mut prior_balances: Vec<(String, String, String, String)> = sqlx::query_as("SELECT miner_id,payout_order_key,encode(p2mr_program,'hex'),balance_sats::text FROM qbit_current_carry_forward_balances()")
        .fetch_all(pool)
        .await?;
    prior_balances.sort();
    Ok(Window {
        network_difficulty,
        shares,
        prior_balances,
    })
}

/// The native snapshot a frontend builds its next job from, read as an
/// operator tool: a tool registers no frontend heartbeat, so the ledger a
/// frontend later starts on is the one the real cutover leaves.
pub async fn native_window(url: &str, network_difficulty: u128) -> Result<Window> {
    let ledger =
        Ledger::connect_tool(url, "cutover-rehearsal-window".into(), 2, false, None).await?;
    let snapshot = ledger.snapshot(network_difficulty).await;
    ledger.pool.close().await;
    let snapshot = snapshot?;
    let mut shares: Vec<_> = snapshot
        .shares
        .into_iter()
        .map(|share| {
            (
                share.share_seq as i64,
                share.share_id,
                share.miner_id,
                share.share_difficulty.to_string(),
                share.credit_policy,
            )
        })
        .collect();
    // The snapshot's own order is its contract with the payout engine;
    // compare it as returned, newest first.
    shares.reverse();
    let mut prior_balances: Vec<_> = snapshot
        .prior_balances
        .into_iter()
        .filter(|balance| balance.balance_sats != 0)
        .map(|balance| {
            (
                balance.recipient_id,
                balance.order_key,
                balance.p2mr_program_hex,
                balance.balance_sats.to_string(),
            )
        })
        .collect();
    prior_balances.sort();
    Ok(Window {
        network_difficulty,
        shares,
        prior_balances,
    })
}

fn digest<T: Serialize>(value: &T) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(value).unwrap_or_default(),
    ))
}

/// The first key whose values differ, for a failure's detail.
fn first_difference<K: std::fmt::Debug + Ord, V: std::fmt::Debug + PartialEq>(
    before: &BTreeMap<K, V>,
    after: &BTreeMap<K, V>,
) -> String {
    for (key, value) in before {
        match after.get(key) {
            Some(other) if other == value => {}
            other => return format!("{key:?}: before {value:?}, after {other:?}"),
        }
    }
    after
        .keys()
        .find(|key| !before.contains_key(*key))
        .map_or_else(String::new, |key| format!("{key:?} appeared"))
}

// ---------------------------------------------------------------------------
// The rehearsal.
// ---------------------------------------------------------------------------

/// What `rehearse` read from the source before migrating it.
struct Before {
    evidence: Value,
    facts: BTreeMap<String, String>,
    balances: Balances,
    window: Window,
}

/// Rehearses the cutover on `target`, a drained 2.x.x ledger, filling
/// `report`. Returns an error only when the rehearsal itself cannot go on;
/// a failed expectation is a failed check.
pub async fn rehearse(
    target: &Target,
    admin_url: &str,
    options: &Options,
    report: &mut Report,
) -> Result<()> {
    let started = Instant::now();
    let evidence = recovery::evidence(&target.recovery(), &options.pg_bin).await?;
    let before = Before {
        facts: table_facts(&target.pool).await?,
        balances: balances(&target.pool).await?,
        window: legacy_window(&target.pool).await?,
        evidence,
    };
    report.step(
        "source evidence and invariants",
        started.elapsed(),
        Vec::new(),
    );
    report.rows = json!({
        "tables": before.facts,
        "accepted_shares": before.evidence["accepted_shares"],
        "last_share_seq": before.evidence["last_share_seq"],
        "addresses": before.balances.len(),
        "window_shares": before.window.shares.len(),
        "pending_payout_addresses": before.balances.values().filter(|b| b.2 != "0").count(),
    });
    report.rows["bytes_before"] = sizes(&target.pool).await?;
    if let Some(source) = &options.source_evidence {
        report.check(
            "restore reproduces the source",
            *source == before.evidence,
            if *source == before.evidence {
                "evidence equal".to_owned()
            } else {
                first_difference(
                    &json_map(&source["records"]),
                    &json_map(&before.evidence["records"]),
                )
            },
        );
    }
    report.check(
        "source drained",
        before.evidence["unfinished_candidates"] == 0,
        format!(
            "{} unfinished candidates",
            before.evidence["unfinished_candidates"]
        ),
    );

    // The node first: check-config and the frontend both name it.
    let rehearsal_node;
    let node = match &options.node {
        NodeChoice::Ledger { tag } => {
            rehearsal_node = Some(node::RehearsalNode::from_ledger(&target.pool, tag).await?);
            let url = rehearsal_node.as_ref().expect("just set").url.clone();
            (
                url,
                "rehearsal".to_owned(),
                "rehearsal".to_owned(),
                "testnet".to_owned(),
            )
        }
        NodeChoice::External {
            url,
            user,
            password,
            chain,
        } => {
            rehearsal_node = None;
            (url.clone(), user.clone(), password.clone(), chain.clone())
        }
        NodeChoice::Operator { .. } => {
            rehearsal_node = None;
            Default::default()
        }
    };
    let mut ports = Ports::reserve()?;
    let mut env = command_env(&target.tool_url()?, &options.node, &node, &ports);
    env.extend(options.extra_env.iter().cloned());
    // The import verifies history against the trusted key. A lab run's test
    // seeds are not that key's, and the import needs no seed; the operator's
    // own seeds are.
    let mut import_env = env.clone();
    if !matches!(options.node, NodeChoice::Operator { .. }) {
        import_env.retain(|(name, _)| !name.contains("SIGNING_SEED"));
    }
    import_env.retain(|(name, _)| name != "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX");
    import_env.push((
        "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX".into(),
        options.ledger_public_key.clone(),
    ));

    let checked = measured(
        report,
        target,
        admin_url,
        &env,
        "check-config",
        &["check-config"],
    )
    .await?;
    report.check("check-config", checked.0, outcome(&checked));
    let migrated = measured(report, target, admin_url, &env, "migrate", &["migrate"]).await?;
    report.check("migrate", migrated.0, outcome(&migrated));
    if !migrated.0 {
        report.finish();
        return Ok(());
    }
    let root = options.audit_root.to_string_lossy().into_owned();
    let imported = measured(
        report,
        target,
        admin_url,
        &import_env,
        "import-audits",
        &["import-audits", "--root", &root],
    )
    .await?;
    let complete = imported.1.contains("\"missing_stored_bodies\":0")
        && imported.1.contains("\"missing_canonical_bytes\":0");
    report.check(
        "import-audits complete",
        imported.0 && complete,
        outcome(&imported),
    );

    let started = Instant::now();
    let evidence = recovery::evidence(&target.recovery(), &options.pg_bin).await?;
    report.check(
        "recovery evidence unchanged",
        evidence == before.evidence,
        if evidence == before.evidence {
            format!(
                "audit head {}",
                evidence["audit_head_sha256"].as_str().unwrap_or("?")
            )
        } else {
            first_difference(
                &json_map(&before.evidence["records"]),
                &json_map(&evidence["records"]),
            )
        },
    );
    let facts = table_facts(&target.pool).await?;
    report.check(
        "row counts and sums unchanged",
        facts == before.facts,
        if facts == before.facts {
            format!("{} tables", facts.len())
        } else {
            first_difference(&before.facts, &facts)
        },
    );
    let after_balances = balances(&target.pool).await?;
    report.check(
        "balances unchanged (SQL)",
        after_balances == before.balances,
        if after_balances == before.balances {
            format!("{} addresses", after_balances.len())
        } else {
            first_difference(&before.balances, &after_balances)
        },
    );
    let window = native_window(&target.url, before.window.network_difficulty).await?;
    report.check(
        "payout window unchanged",
        !before.window.shares.is_empty() && window.shares == before.window.shares,
        format!(
            "native {} shares, 2.x.x {} shares, digest {}",
            window.shares.len(),
            before.window.shares.len(),
            &digest(&window.shares)[..16]
        ),
    );
    report.check(
        "window prior balances unchanged",
        window.prior_balances == before.window.prior_balances,
        format!(
            "native {} balances, 2.x.x {}",
            window.prior_balances.len(),
            before.window.prior_balances.len()
        ),
    );
    report.step(
        "migrated evidence and invariants",
        started.elapsed(),
        Vec::new(),
    );
    report.rows["bytes_after"] = sizes(&target.pool).await?;

    if options.start_frontend {
        // A synced node's tip is past the snapshot's, so the frontend may
        // mature blocks the source left immature: their pending balance then
        // moves, legitimately.
        let exact_pending = !matches!(options.node, NodeChoice::Operator { .. });
        frontend(report, &env, &mut ports, &before.balances, exact_pending).await?;
        // What the frontend's startup and reconciliation left of history.
        let after_run = table_facts(&target.pool).await?;
        let unchanged = |facts: &BTreeMap<String, String>| {
            facts
                .iter()
                .filter(|(table, _)| !FRONTEND_MAY_CHANGE.contains(&table.as_str()))
                .map(|(table, fact)| (table.clone(), fact.clone()))
                .collect::<BTreeMap<_, _>>()
        };
        let (expected, found) = (unchanged(&before.facts), unchanged(&after_run));
        report.check(
            "history unchanged after the frontend ran",
            !exact_pending || expected == found,
            if expected == found {
                format!("{} tables", found.len())
            } else {
                first_difference(&expected, &found)
            },
        );
    }
    drop(rehearsal_node);
    report.finish();
    Ok(())
}

/// The database and the share ledger with its indexes, in bytes. The ledger
/// is partitioned after 017, so its size is summed over the partition tree.
async fn sizes(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object('database',pg_database_size(current_database()),'share_ledger',CASE WHEN (SELECT relkind FROM pg_class WHERE oid='qbit_share_ledger'::regclass)='p' THEN (SELECT COALESCE(sum(pg_total_relation_size(relid)),0) FROM pg_partition_tree('qbit_share_ledger'::regclass)) ELSE pg_total_relation_size('qbit_share_ledger'::regclass) END)")
        .fetch_one(pool)
        .await?)
}

fn json_map(value: &Value) -> BTreeMap<String, Value> {
    value
        .as_object()
        .map(|object| object.clone().into_iter().collect())
        .unwrap_or_default()
}

fn first_line(output: &str) -> String {
    let line = output
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("")
        .trim();
    line.chars().take(400).collect()
}

/// A child that is killed when dropped.
struct Frontend(tokio::process::Child);

impl Drop for Frontend {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

/// One native frontend on the migrated ledger: readiness, balances through
/// the public API, a first Stratum job, and `self-check`.
async fn frontend(
    report: &mut Report,
    env: &[(String, String)],
    ports: &mut Ports,
    before: &Balances,
    exact_pending: bool,
) -> Result<()> {
    let started = Instant::now();
    let log = tempfile::NamedTempFile::new()?;
    let mut command = server_command(env);
    command
        .arg("run")
        .stdin(Stdio::null())
        .stdout(log.reopen()?)
        .stderr(log.reopen()?);
    ports.release();
    let child = command.spawn()?;
    let mut frontend = Frontend(child);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let health = format!("http://127.0.0.1:{}/healthz", ports.api);
    let deadline = Instant::now() + Duration::from_secs(90);
    let ready = loop {
        if let Ok(response) = client.get(&health).send().await {
            if response.status().is_success() {
                break true;
            }
        }
        if let Some(status) = frontend.0.try_wait()? {
            bail!(
                "the frontend ({ports}) exited with {status} before readiness: {}",
                tail(log.path())
            );
        }
        if Instant::now() > deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    report.step("frontend readiness", started.elapsed(), Vec::new());
    report.check(
        "frontend ready",
        ready,
        if ready {
            "healthz 200".to_owned()
        } else {
            format!("{ports}: {}", tail(log.path()))
        },
    );
    if !ready {
        return Ok(());
    }

    let started = Instant::now();
    let mut api = api_balances(&client, ports.api, before).await?;
    report.step("API balances", started.elapsed(), Vec::new());
    let mut expected = before.clone();
    if !exact_pending {
        for balances in expected.values_mut().chain(api.values_mut()) {
            balances.2.clear();
        }
    }
    report.check(
        "API balances equal the source",
        api == expected,
        if api == expected {
            format!(
                "{} addresses{}",
                api.len(),
                if exact_pending {
                    ""
                } else {
                    ", owed and lifetime"
                }
            )
        } else {
            first_difference(&expected, &api)
        },
    );

    let started = Instant::now();
    let username = before
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| seed::synthetic_address("prism-rehearsal-miner"));
    let job = first_job(ports.stratum, &username).await;
    report.step("first Stratum job", started.elapsed(), Vec::new());
    report.check(
        "first job issued",
        job.is_ok(),
        match &job {
            Ok(job) => format!("mining.notify job {job}"),
            Err(error) => format!("{error:#}"),
        },
    );

    let started = Instant::now();
    // The highdiff probe authorizes as a ledger address, as an operator
    // names one with PRISM_SELF_CHECK_ADDRESS.
    let mut env = env.to_vec();
    if !env
        .iter()
        .any(|(name, _)| name == "PRISM_SELF_CHECK_ADDRESS")
    {
        env.push(("PRISM_SELF_CHECK_ADDRESS".into(), username.clone()));
    }
    let (ok, output, errors) = run_tool(&env, &["self-check"]).await?;
    let parsed: Value = serde_json::from_str(&output).unwrap_or(Value::Null);
    report.step("self-check", started.elapsed(), Vec::new());
    let passed = ok && parsed["ok"] == true;
    report.check(
        "self-check",
        passed,
        if passed {
            format!("health {}", parsed["health"]["status"])
        } else {
            first_line(&format!("{errors}\n{output}"))
        },
    );
    report.check(
        "audit completeness",
        parsed["audit_completeness"]
            == json!({"missing_stored_bodies": 0, "missing_canonical_bytes": 0}),
        parsed["audit_completeness"].to_string(),
    );
    frontend.0.kill().await?;
    Ok(())
}

/// Every address in `before`, read from a frontend's public API.
pub async fn api_balances(
    client: &reqwest::Client,
    port: u16,
    before: &Balances,
) -> Result<Balances> {
    use futures_util::StreamExt;
    futures_util::stream::iter(before.keys().cloned())
        .map(|miner| {
            let client = client.clone();
            let url = format!(
                "http://127.0.0.1:{port}/public/v1/miners/{}",
                percent_encoding::utf8_percent_encode(&miner, percent_encoding::NON_ALPHANUMERIC)
            );
            async move {
                let body: Value = client
                    .get(url)
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                let text = |field: &str| match &body[field] {
                    Value::String(text) => text.clone(),
                    other => other.to_string(),
                };
                anyhow::Ok((
                    miner,
                    (
                        text("owed_balance_bits"),
                        text("lifetime_earnings_bits"),
                        text("pending_maturity_bits"),
                    ),
                ))
            }
        })
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
}

fn tail(path: &Path) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(8)..].join(" | ")
}

/// Subscribes and authorizes as `username` and waits for the first
/// `mining.notify`, returning its job id.
async fn first_job(port: u16, username: &str) -> Result<String> {
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    let (read, mut write) = stream.into_split();
    write
        .write_all(
            b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[\"cutover-rehearsal\"]}\n",
        )
        .await?;
    write
        .write_all(
            format!(
                "{{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"{username}.rehearsal\",\"x\"]}}\n"
            )
            .as_bytes(),
        )
        .await?;
    let mut lines = BufReader::new(read).lines();
    loop {
        let line = tokio::time::timeout(Duration::from_secs(40), lines.next_line())
            .await
            .context("no mining.notify within 40 s")??
            .context("the frontend closed the Stratum session")?;
        let message: Value = serde_json::from_str(&line)?;
        if message["id"] == 2 && message["result"] != true {
            bail!("authorize refused: {line}");
        }
        if message["method"] == "mining.notify" {
            return Ok(message["params"][0].as_str().unwrap_or("?").to_owned());
        }
    }
}

// ---------------------------------------------------------------------------
// A supplied pg_dump, on a private cluster.
// ---------------------------------------------------------------------------

/// A private PostgreSQL cluster: created with `initdb`, `fsync` on, listening
/// on loopback only with a random password (a shared host's other users
/// cannot reach the snapshot), stopped and removed when dropped, even after
/// a start that failed half way.
pub struct PrivateCluster {
    pg_bin: PathBuf,
    data: tempfile::TempDir,
    password: String,
    running: bool,
    pub port: u16,
}

impl PrivateCluster {
    pub async fn start(pg_bin: &Path, parent: &Path) -> Result<Self> {
        let (pg_bin, parent) = (pg_bin.to_owned(), parent.to_owned());
        tokio::task::spawn_blocking(move || Self::start_blocking(pg_bin, &parent)).await?
    }

    fn start_blocking(pg_bin: PathBuf, parent: &Path) -> Result<Self> {
        let data = tempfile::Builder::new()
            .prefix("prism-rehearsal-pg-")
            .tempdir_in(parent)?;
        let password = uuid::Uuid::new_v4().simple().to_string();
        let password_file = data.path().join("password");
        std::fs::write(&password_file, &password)?;
        // Held until `pg_ctl start`, so initdb's run cannot lose it (#639).
        let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        let mut cluster = Self {
            pg_bin,
            data,
            password,
            running: false,
            port: reservation.local_addr()?.port(),
        };
        let status = std::process::Command::new(cluster.pg_bin.join("initdb"))
            .args([
                "-U",
                "rehearsal",
                "--auth=scram-sha-256",
                "-E",
                "UTF8",
                "--no-sync",
            ])
            .arg(format!("--pwfile={}", password_file.display()))
            .arg("-D")
            .arg(cluster.data.path().join("data"))
            .stdout(Stdio::null())
            .status()?;
        std::fs::remove_file(&password_file)?;
        ensure!(status.success(), "initdb failed");
        // Marked first, so a start that fails half way is still stopped.
        cluster.running = true;
        drop(reservation);
        let status = std::process::Command::new(cluster.pg_bin.join("pg_ctl"))
            .arg("-D")
            .arg(cluster.data.path().join("data"))
            .arg("-l")
            .arg(cluster.data.path().join("postgres.log"))
            .arg("-o")
            .arg(format!(
                "-p {} -c listen_addresses=127.0.0.1 -c unix_socket_directories= -c fsync=on -c full_page_writes=on -c synchronous_commit=on -c max_connections=100",
                cluster.port
            ))
            .args(["-w", "start"])
            .stdout(Stdio::null())
            .status()?;
        ensure!(
            status.success(),
            "pg_ctl start on 127.0.0.1:{} failed: {}",
            cluster.port,
            std::fs::read_to_string(cluster.data.path().join("postgres.log")).unwrap_or_default()
        );
        Ok(cluster)
    }

    /// A URL for `database`, with the cluster's password.
    pub fn url(&self, database: &str) -> String {
        format!(
            "postgresql://rehearsal:{}@127.0.0.1:{}/{database}",
            self.password, self.port
        )
    }
}

impl Drop for PrivateCluster {
    fn drop(&mut self) {
        if self.running {
            let _ = std::process::Command::new(self.pg_bin.join("pg_ctl"))
                .arg("-D")
                .arg(self.data.path().join("data"))
                .args(["-m", "immediate", "-w", "stop"])
                .stdout(Stdio::null())
                .status();
        }
    }
}

/// How a dump was written: `pg_restore` reads the archive formats, `psql` a
/// plain SQL script.
fn is_archive(dump: &Path) -> Result<bool> {
    if dump.is_dir() {
        return Ok(true);
    }
    use std::io::Read;
    let mut header = Vec::with_capacity(512);
    std::fs::File::open(dump)?
        .take(512)
        .read_to_end(&mut header)?;
    // Custom format starts with PGDMP; a tar archive has `ustar` at 257.
    Ok(header.starts_with(b"PGDMP") || header.get(257..262) == Some(b"ustar".as_slice()))
}

/// A rehearsal of a supplied dump.
pub struct DumpRehearsal {
    pub dump: PathBuf,
    /// The schema holding `qbit_share_ledger`; found in the dump if `None`.
    pub schema: Option<String>,
    pub options: Options,
    /// Where the private cluster lives while it runs.
    pub workdir: PathBuf,
}

/// Restores `dump` into a fresh private cluster and rehearses the cutover
/// on it. Nothing outside that cluster is connected to.
pub async fn rehearse_dump(rehearsal: &DumpRehearsal, report: &mut Report) -> Result<()> {
    let metadata = std::fs::metadata(&rehearsal.dump)
        .with_context(|| format!("reading the dump {}", rehearsal.dump.display()))?;
    ensure!(metadata.len() > 0, "the dump is empty");
    if let Some(schema) = &rehearsal.schema {
        ensure!(
            !schema.is_empty()
                && schema
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                && !schema.starts_with(|c: char| c.is_ascii_digit()),
            "the schema must be a lower-case SQL identifier, not {schema:?}"
        );
    }
    let started = Instant::now();
    let cluster = PrivateCluster::start(&rehearsal.options.pg_bin, &rehearsal.workdir).await?;
    let admin_url = cluster.url("postgres");
    let admin = PgPool::connect(&admin_url).await?;
    sqlx::query("CREATE DATABASE rehearsal")
        .execute(&admin)
        .await?;
    let database_url = cluster.url("rehearsal");
    // The password reaches the clients through the environment, never
    // their arguments.
    let client_url = format!(
        "postgresql://rehearsal@127.0.0.1:{}/rehearsal",
        cluster.port
    );
    let restore = if is_archive(&rehearsal.dump)? {
        Command::new(rehearsal.options.pg_bin.join("pg_restore"))
            .args([
                "--no-owner",
                "--no-privileges",
                "--exit-on-error",
                "--dbname",
            ])
            .arg(&client_url)
            .arg(&rehearsal.dump)
            .env("PGPASSWORD", &cluster.password)
            .output()
            .await?
    } else {
        // A plain dump replays its own OWNER and GRANT statements: take it
        // with --no-owner --no-privileges, or use an archive format.
        Command::new(rehearsal.options.pg_bin.join("psql"))
            .args(["-X", "-q", "-v", "ON_ERROR_STOP=1", "--dbname"])
            .arg(&client_url)
            .arg("-f")
            .arg(&rehearsal.dump)
            .env("PGPASSWORD", &cluster.password)
            .output()
            .await?
    };
    ensure!(
        restore.status.success(),
        "restoring the dump failed: {}",
        String::from_utf8_lossy(&restore.stderr)
    );
    report.step(
        "restore into a private cluster",
        started.elapsed(),
        Vec::new(),
    );
    let probe = PgPool::connect(&database_url).await?;
    let schema = match &rehearsal.schema {
        Some(schema) => schema.clone(),
        None => {
            let schemas: Vec<String> = sqlx::query_scalar("SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='qbit_share_ledger' AND c.relkind IN ('r','p') ORDER BY 1")
                .fetch_all(&probe)
                .await?;
            match schemas.as_slice() {
                [schema] => schema.clone(),
                [] => bail!("the dump has no qbit_share_ledger"),
                many => bail!(
                    "the dump has a ledger in several schemas ({}); name one",
                    many.join(", ")
                ),
            }
        }
    };
    probe.close().await;
    let mut url = url::Url::parse(&database_url)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let url = url.to_string();
    let target = Target {
        url: url.clone(),
        schema,
        admin: PgPool::connect(&database_url).await?,
        pool: PgPool::connect(&url).await?,
    };
    let result = rehearse(&target, &database_url, &rehearsal.options, report).await;
    target.pool.close().await;
    target.admin.close().await;
    admin.close().await;
    drop(cluster);
    result
}
