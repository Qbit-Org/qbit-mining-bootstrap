//! Managed PostgreSQL primary and optional streaming standby.
//!
//! The `Cluster` helper follows
//! `crates/qbit-prism-server/tests/postgres_failover.rs`, extended with the
//! replication slot, the explicit `application_name`, the synchronous flip and
//! a teardown that runs on every exit path.

use anyhow::{bail, ensure, Context, Result};
use serde::Serialize;
use sqlx::{PgPool, Row};
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

pub const STANDBY_NAME: &str = "prism_standby_1";
pub const STANDBY_SLOT: &str = "prism_standby_1_slot";
/// The primary-side synchronous setting D3 flips on: the managed cluster's
/// default, priority-based, under which the standby reports `sync`.
pub const SYNCHRONOUS_NAMES: &str = "FIRST 1 (prism_standby_1)";
/// The quorum-based form of the same setting, under which the standby
/// reports `quorum`. Only a test that needs the second topology asks for it.
pub const QUORUM_SYNCHRONOUS_NAMES: &str = "ANY 1 (prism_standby_1)";

/// How the primary names its synchronous standby.
///
/// PostgreSQL reports a standby differently under the two forms of
/// `synchronous_standby_names`: a member of a `FIRST n` set is `sync`, and a
/// candidate of an `ANY n` set is `quorum`. Both are synchronous, and
/// [`classify_replication`] reads them the same way. The managed cluster
/// uses `FIRST` unless told otherwise.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SynchronousMethod {
    /// `FIRST 1 (prism_standby_1)`: priority-based, the standby reports `sync`.
    #[default]
    First,
    /// `ANY 1 (prism_standby_1)`: quorum-based, the standby reports `quorum`.
    Any,
}

impl SynchronousMethod {
    /// The `synchronous_standby_names` value the primary is given.
    pub const fn standby_names(self) -> &'static str {
        match self {
            Self::First => SYNCHRONOUS_NAMES,
            Self::Any => QUORUM_SYNCHRONOUS_NAMES,
        }
    }

    /// The `sync_state` PostgreSQL reports for the standby once the setting
    /// has applied, which is what the managed cluster waits for.
    pub const fn standby_sync_state(self) -> &'static str {
        match self {
            Self::First => "sync",
            Self::Any => "quorum",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Replication {
    /// One asynchronous standby (decision D3's default).
    Async,
    /// The same standby with `synchronous_standby_names` set on the primary.
    Sync,
    /// No standby at all.
    None,
}

impl Replication {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "async" => Ok(Self::Async),
            "sync" => Ok(Self::Sync),
            "none" => Ok(Self::None),
            other => bail!("unknown replication mode {other:?}; use async, sync or none"),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Async => "async",
            Self::Sync => "sync",
            Self::None => "none",
        }
    }
}

/// One `pg_stat_replication` row, recorded at every phase boundary.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ReplicationRow {
    pub application_name: String,
    pub state: String,
    pub sync_state: String,
    pub sent_lsn: Option<String>,
    pub write_lsn: Option<String>,
    pub flush_lsn: Option<String>,
    pub replay_lsn: Option<String>,
}

/// The replication view at one instant.
///
/// A view that could not be read is recorded as unread, with the reason:
/// `synchronous_standby_names` is `null` and `rows` is empty with `error`
/// set. It used to come back as an empty name and no rows, which is exactly
/// what a primary with no standby looks like, so a phase boundary at which
/// `pg_stat_replication` was unreadable read as "no standby" and the
/// premise check's own `unknown` state had no counterpart in the per-phase
/// observations (EP-OBSERVABILITY).
#[derive(Clone, Debug, Serialize)]
pub struct ReplicationObservation {
    pub at: chrono::DateTime<chrono::Utc>,
    pub label: String,
    /// `None` when `SHOW synchronous_standby_names` failed; see `error`.
    pub synchronous_standby_names: Option<String>,
    pub rows: Vec<ReplicationRow>,
    /// Why the view, or the setting, could not be read, when it could not.
    /// `rows` is then not an observation of anything.
    pub error: Option<String>,
}

pub async fn observe_replication(pool: &PgPool, label: &str) -> Result<ReplicationObservation> {
    let names: std::result::Result<String, sqlx::Error> =
        sqlx::query_scalar("SHOW synchronous_standby_names")
            .fetch_one(pool)
            .await;
    let rows = sqlx::query(
        "SELECT application_name,state,sync_state,sent_lsn::text,write_lsn::text,\
         flush_lsn::text,replay_lsn::text FROM pg_stat_replication",
    )
    .fetch_all(pool)
    .await;
    let mut errors = Vec::new();
    let names = match names {
        Ok(names) => Some(names),
        Err(error) => {
            errors.push(format!(
                "synchronous_standby_names could not be read: {error}"
            ));
            None
        }
    };
    let rows = match rows {
        Ok(rows) => rows,
        Err(error) => {
            errors.push(format!("pg_stat_replication could not be read: {error}"));
            Vec::new()
        }
    };
    Ok(ReplicationObservation {
        at: chrono::Utc::now(),
        label: label.to_owned(),
        synchronous_standby_names: names,
        error: (!errors.is_empty()).then(|| errors.join("; ")),
        rows: rows
            .into_iter()
            .map(|row| ReplicationRow {
                application_name: row.try_get("application_name").unwrap_or_default(),
                state: row.try_get("state").unwrap_or_default(),
                sync_state: row.try_get("sync_state").unwrap_or_default(),
                sent_lsn: row.try_get("sent_lsn").ok(),
                write_lsn: row.try_get("write_lsn").ok(),
                flush_lsn: row.try_get("flush_lsn").ok(),
                replay_lsn: row.try_get("replay_lsn").ok(),
            })
            .collect(),
    })
}

/// What `detect_replication` saw: a mode, or the reason it could not tell.
///
/// The unreadable case used to come back as `none`, a definite value for
/// something the harness had not read. It is its own state, with the
/// reason, and the run treats it as a contradicted premise: a run that
/// cannot tell whether its standby exists has not established the
/// conditions it claims (EP-OBSERVABILITY).
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ObservedReplication {
    Observed { mode: Replication },
    Unknown { reason: String },
}

impl ObservedReplication {
    /// The mode's name, or `unknown`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Observed { mode } => mode.as_str(),
            Self::Unknown { .. } => "unknown",
        }
    }

    /// Why the mode could not be observed, when it could not.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Observed { .. } => None,
            Self::Unknown { reason } => Some(reason),
        }
    }
}

/// Detect what replication is actually configured. Never assumed from a
/// flag, and never guessed: a `pg_stat_replication` that cannot be read, or
/// whose rows hide `sync_state` from this role, is `Unknown` with the
/// reason rather than `none` in one direction or `async` in the other.
///
/// The read is here; what the rows mean is [`classify_replication`].
pub async fn detect_replication(pool: &PgPool) -> ObservedReplication {
    let states: std::result::Result<Vec<Option<String>>, sqlx::Error> =
        sqlx::query_scalar("SELECT sync_state FROM pg_stat_replication")
            .fetch_all(pool)
            .await;
    match states {
        Err(error) => ObservedReplication::Unknown {
            reason: format!("pg_stat_replication could not be read: {error}"),
        },
        Ok(states) => classify_replication(&states),
    }
}

/// The `sync_state` values PostgreSQL 16 reports for a synchronous standby:
/// `sync` for a member of a `FIRST n` set and `quorum` for a candidate of an
/// `ANY n` set. A commit waits on either.
pub const SYNCHRONOUS_SYNC_STATES: [&str; 2] = ["sync", "quorum"];

/// What one `sync_state` column per `pg_stat_replication` row says about the
/// cluster, as [`detect_replication`] reads it. Pure, so every shape of the
/// view is testable without a database.
///
/// PostgreSQL 16's `sync_state` takes four values: `async`, `potential`,
/// `quorum` and `sync`. The arms, in order:
///
/// 1. No rows: no standby, `Observed(None)`.
/// 2. Any row whose state is null: `Unknown`. PostgreSQL shows a role
///    without `pg_read_all_stats` the rows but not their state columns, so a
///    standby exists and whether it is synchronous cannot be told. This arm
///    has to stay above the next one: a hidden state beside a visible one
///    could be anything, and letting a visible `sync` decide would report a
///    mode the harness has not observed.
/// 3. Any row `sync` or `quorum` ([`SYNCHRONOUS_SYNC_STATES`]):
///    `Observed(Sync)`. A commit waits on the synchronous set, and an
///    additional asynchronous standby beside it does not make the cluster
///    asynchronous, so one synchronous row is enough (`any` semantics, as
///    PostgreSQL itself applies them). `quorum` is what an external cluster
///    with `synchronous_standby_names = 'ANY n (...)'` reports; before it was
///    recognized, a correct `--replication sync` run against such a cluster
///    was refused as a contradicted premise, and a `--replication async` run
///    was accepted for a cluster that is synchronous.
/// 4. Otherwise `Observed(Async)`.
///
/// `potential` is deliberately not synchronous. Under `FIRST n`, a
/// `potential` standby is one that would be promoted into the synchronous
/// set if a current member left, but is not in it now: no commit is waiting
/// on it. Accepting it would refuse a correctly configured
/// `--replication async` run at the premise check (exit 8), and would let a
/// `--replication sync` claim pass on a cluster where nothing is actually
/// synchronous. A value outside the four-value domain falls through to
/// `Async` as it always has (EP-VALIDATION).
pub fn classify_replication(states: &[Option<String>]) -> ObservedReplication {
    if states.is_empty() {
        return ObservedReplication::Observed {
            mode: Replication::None,
        };
    }
    if states.iter().any(Option::is_none) {
        return ObservedReplication::Unknown {
            reason: format!(
                "{} pg_stat_replication row(s) are visible but their sync_state is null, \
                 which PostgreSQL shows a role without pg_read_all_stats: a standby exists \
                 and whether it is synchronous cannot be told",
                states.len()
            ),
        };
    }
    if states.iter().any(|state| {
        state
            .as_deref()
            .is_some_and(|state| SYNCHRONOUS_SYNC_STATES.contains(&state))
    }) {
        return ObservedReplication::Observed {
            mode: Replication::Sync,
        };
    }
    ObservedReplication::Observed {
        mode: Replication::Async,
    }
}

struct Cluster {
    bin: PathBuf,
    data: PathBuf,
    log: PathBuf,
    running: bool,
}

impl Cluster {
    fn run(&self, binary: &str, args: &[&str]) -> Result<String> {
        let output = Command::new(self.bin.join(binary))
            .args(args)
            .output()
            .with_context(|| format!("running {binary}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        ensure!(
            output.status.success(),
            "{binary} failed: {stdout} {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(stdout)
    }

    fn control(&self, args: &[&str]) -> Result<()> {
        let mut all = vec!["-D", self.data.to_str().context("non-UTF-8 data path")?];
        all.extend_from_slice(args);
        self.run("pg_ctl", &all).map(|_| ())
    }

    /// Start the server, or fail with the end of its own log.
    ///
    /// `pg_ctl` reports only that the server did not start and asks for the
    /// log to be examined; the reason -- a socket path over the platform's
    /// limit, a setting out of range -- is in the log, and the log is in the
    /// cluster root, which goes with the failure unless `--keep-artifacts`
    /// was given. The error carries its last lines instead (#485).
    fn start(&mut self, options: &str) -> Result<()> {
        let started = self.control(&[
            "-l",
            self.log.to_str().context("non-UTF-8 log path")?,
            "-o",
            options,
            "-w",
            "-t",
            "120",
            "start",
        ]);
        if let Err(error) = started {
            let pg_ctl = format!("{error:#}");
            let pg_ctl = pg_ctl.trim_end();
            let log = self.log.display();
            bail!(match log_tail(&self.log, START_FAILURE_LOG_LINES) {
                Some(tail) => format!(
                    "{pg_ctl}\nPostgreSQL did not start; the last lines of its log {log} (removed \
                     with the cluster root unless --keep-artifacts):\n{tail}"
                ),
                None => format!(
                    "{pg_ctl}\nPostgreSQL did not start, and its log {log} could not be read or \
                     is empty"
                ),
            });
        }
        self.running = true;
        Ok(())
    }

    fn stop(&mut self) {
        if self.running {
            let _ = self.control(&["-m", "immediate", "-w", "-t", "60", "stop"]);
            self.running = false;
        }
    }
}

/// How many trailing lines of a server log a failed start carries.
pub const START_FAILURE_LOG_LINES: usize = 20;

/// The last `lines` non-empty lines of a log, or `None` when it cannot be
/// read or holds nothing.
pub fn log_tail(path: &Path, lines: usize) -> Option<String> {
    let contents = std::fs::read(path).ok()?;
    let contents = String::from_utf8_lossy(&contents);
    let kept: Vec<&str> = contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    (!kept.is_empty()).then(|| kept[kept.len().saturating_sub(lines)..].join("\n"))
}

/// The longest Unix-domain socket path PostgreSQL accepts: one less than
/// `sizeof(sun_path)`, which is 108 on Linux and 104 on macOS and the BSDs.
#[cfg(target_os = "linux")]
pub const MAX_SOCKET_PATH_BYTES: usize = 107;
#[cfg(not(target_os = "linux"))]
pub const MAX_SOCKET_PATH_BYTES: usize = 103;

/// The socket file PostgreSQL creates in `-k <dir>`, at the widest port, so
/// a root that fits it fits whatever port is allocated later.
const WIDEST_SOCKET_FILE: &str = ".s.PGSQL.65535";

/// Hex digits of the random part of a root's name. The root used to carry a
/// whole simple UUID (32), which put the socket 107 bytes deep under macOS's
/// default `TMPDIR` of `/var/folders/<2>/<28>/T/`; 12 bring it to 87 there.
/// The name is claimed with `create_dir`, so a collision fails rather than
/// sharing a directory.
const ROOT_NAME_HEX: usize = 12;

/// The temporary cluster root, removed on drop unless the run asked to keep
/// it.
///
/// It is a guard rather than a plain path because the directory has to exist
/// before the `ManagedPostgres` that owns the cleanup can be built: every `?`
/// in between -- reading the current user, `initdb`, allocating a port,
/// starting the primary -- used to return without removing it, leaving an
/// orphan `/tmp/prism-load-<uuid>` behind, and a failure after `initdb` left a
/// whole data directory there.
pub struct TempRoot {
    path: PathBuf,
    keep: bool,
}

impl TempRoot {
    /// A root under the process temp directory (`TMPDIR`).
    pub fn create(keep: bool) -> Result<Self> {
        Self::create_in(&std::env::temp_dir(), keep)
    }

    /// A root under `base`, refused before anything is created when
    /// PostgreSQL's socket in it would exceed [`MAX_SOCKET_PATH_BYTES`].
    /// The server would refuse to start there, and a refusal at entry names
    /// the cause where the server's own line would otherwise have to be
    /// dug out of its log (EP-CONFIG).
    pub fn create_in(base: &Path, keep: bool) -> Result<Self> {
        let name = format!(
            "prism-load-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..ROOT_NAME_HEX]
        );
        let path = base.join(name);
        let socket = path.join(WIDEST_SOCKET_FILE);
        let socket_bytes = socket.as_os_str().len();
        ensure!(
            socket_bytes <= MAX_SOCKET_PATH_BYTES,
            "the managed cluster's Unix socket would be {} ({socket_bytes} bytes), over the \
             {MAX_SOCKET_PATH_BYTES}-byte limit PostgreSQL accepts on this platform, because the \
             temporary directory {} is too deep; set TMPDIR to a shorter directory (for \
             example TMPDIR=/tmp) or use --database-url",
            socket.display(),
            base.display()
        );
        std::fs::create_dir(&path)
            .with_context(|| format!("create cluster root {}", path.display()))?;
        Ok(Self { path, keep })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// A primary plus, optionally, one streaming standby, both owned by the run.
pub struct ManagedPostgres {
    pub primary_url: String,
    pub primary_port: u16,
    pub standby_url: Option<String>,
    pub standby_port: Option<u16>,
    pub replication: Replication,
    /// How the primary names the standby when the mode is synchronous.
    pub synchronous_method: SynchronousMethod,
    pub pg_stat_statements: Option<String>,
    pub bin_dir: PathBuf,
    /// Dropped after `stop`, which is what removes the directory.
    root: TempRoot,
    primary: Cluster,
    standby: Option<Cluster>,
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

fn current_user() -> Result<String> {
    Ok(
        String::from_utf8(Command::new("id").arg("-un").output()?.stdout)?
            .trim()
            .to_owned(),
    )
}

/// The harness's own PostgreSQL binary-directory variable. It is deliberately
/// not one of the shared test-gate variables: those belong to the gate crate
/// (#322), and a second reader of one would make the gate's manifest wrong.
pub const PG_BIN_DIR_VAR: &str = "QBIT_PRISM_LOAD_PG_BIN_DIR";

/// Resolve the server binary directory: `--pg-bin-dir`, then
/// [`PG_BIN_DIR_VAR`], then `pg_config --bindir`.
pub fn resolve_bin_dir(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }
    if let Ok(value) = std::env::var(PG_BIN_DIR_VAR) {
        if !value.trim().is_empty() {
            return Ok(PathBuf::from(value.trim()));
        }
    }
    let output = Command::new("pg_config")
        .arg("--bindir")
        .output()
        .with_context(|| format!("pg_config --bindir (set --pg-bin-dir or {PG_BIN_DIR_VAR})"))?;
    ensure!(output.status.success(), "pg_config --bindir failed");
    Ok(PathBuf::from(
        String::from_utf8(output.stdout)?.trim().to_owned(),
    ))
}

/// The server binaries the managed mode runs.
pub const REQUIRED_BINARIES: [&str; 3] = ["initdb", "pg_ctl", "pg_basebackup"];

/// Check that a resolved bin directory really holds the server binaries.
///
/// Called before anything is created, so a wrong `--pg-bin-dir` names the flag
/// and the missing binary instead of surfacing much later as a failed `initdb`
/// with a temporary cluster root already on disk (EP-VALIDATION).
pub fn verify_bin_dir(dir: &Path) -> Result<()> {
    ensure!(
        dir.is_dir(),
        "--pg-bin-dir {} is not a directory; point it, or {PG_BIN_DIR_VAR}, at the PostgreSQL 16 \
         server binaries (on Debian and Ubuntu, /usr/lib/postgresql/16/bin)",
        dir.display()
    );
    for name in REQUIRED_BINARIES {
        ensure!(
            dir.join(name).is_file(),
            "--pg-bin-dir {} does not contain {name}; the managed cluster needs {}",
            dir.display(),
            REQUIRED_BINARIES.join(", ")
        );
    }
    Ok(())
}

/// The file names `pg_stat_statements` ships under: `.so` on Linux, and
/// `.dylib` from Homebrew's PostgreSQL on macOS, which the managed cluster
/// used never to find, so it never preloaded the extension there (#485).
pub const PG_STAT_STATEMENTS_LIBRARIES: [&str; 2] =
    ["pg_stat_statements.so", "pg_stat_statements.dylib"];

/// The `pg_stat_statements` library in `pkglibdir`, if it is there under
/// either name.
pub fn pg_stat_statements_library(pkglibdir: &Path) -> Option<PathBuf> {
    PG_STAT_STATEMENTS_LIBRARIES
        .iter()
        .map(|name| pkglibdir.join(name))
        .find(|path| path.is_file())
}

fn pkglibdir(bin: &Path) -> Option<PathBuf> {
    let output = Command::new(bin.join("pg_config"))
        .arg("--pkglibdir")
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| PathBuf::from(String::from_utf8_lossy(&output.stdout).trim().to_owned()))
}

impl ManagedPostgres {
    /// Start the primary, then the standby the mode asks for. A synchronous
    /// standby is named with `FIRST 1`, the managed cluster's default.
    pub async fn start(
        bin_dir: PathBuf,
        replication: Replication,
        max_connections: u32,
        keep_artifacts: bool,
    ) -> Result<Self> {
        Self::start_with_method(
            bin_dir,
            replication,
            max_connections,
            keep_artifacts,
            SynchronousMethod::First,
        )
        .await
    }

    /// [`start`](Self::start), naming a synchronous standby with the given
    /// method. `ANY 1` exists so a test can verify the quorum topology an
    /// external cluster may use; the harness itself runs `FIRST 1`.
    pub async fn start_with_method(
        bin_dir: PathBuf,
        replication: Replication,
        max_connections: u32,
        keep_artifacts: bool,
        synchronous_method: SynchronousMethod,
    ) -> Result<Self> {
        let root = TempRoot::create(keep_artifacts)?;
        let user = current_user()?;
        let mut primary = Cluster {
            bin: bin_dir.clone(),
            data: root.path().join("primary"),
            log: root.path().join("primary.log"),
            running: false,
        };
        primary.run(
            "initdb",
            &[
                "-D",
                primary.data.to_str().context("non-UTF-8 data path")?,
                "-A",
                "trust",
                "--no-locale",
                "-E",
                "UTF8",
            ],
        )?;
        let primary_port = free_port()?;
        let preload = pkglibdir(&bin_dir)
            .and_then(|dir| pg_stat_statements_library(&dir))
            .map(|_| "pg_stat_statements".to_owned());
        // `synchronous_standby_names` is deliberately absent: a command-line
        // value shadows the `ALTER SYSTEM` flip the synchronous mode needs.
        let mut options = format!(
            "-h 127.0.0.1 -p {primary_port} -k {root} -c fsync=on -c full_page_writes=on \
             -c wal_level=replica -c max_wal_senders=10 -c max_replication_slots=10 \
             -c max_connections={max_connections}",
            root = root.path().display()
        );
        if preload.is_some() {
            options.push_str(" -c shared_preload_libraries=pg_stat_statements");
        }
        primary.start(&options)?;
        let primary_url = format!("postgresql://{user}@127.0.0.1:{primary_port}/postgres");
        let mut managed = Self {
            primary_url,
            primary_port,
            standby_url: None,
            standby_port: None,
            replication,
            synchronous_method,
            pg_stat_statements: preload.clone().map(|_| "loaded".to_owned()),
            bin_dir: bin_dir.clone(),
            root,
            primary,
            standby: None,
        };
        if managed.pg_stat_statements.is_none() {
            managed.pg_stat_statements = Some("unavailable".to_owned());
        }
        if replication != Replication::None {
            if let Err(error) = managed.attach_standby(&user).await {
                managed.stop();
                return Err(error);
            }
        }
        Ok(managed)
    }

    async fn attach_standby(&mut self, user: &str) -> Result<()> {
        let admin = PgPool::connect(&self.primary_url)
            .await
            .context("connect to the managed primary")?;
        sqlx::query("SELECT pg_create_physical_replication_slot($1)")
            .bind(STANDBY_SLOT)
            .execute(&admin)
            .await
            .context("create the standby replication slot")?;
        let standby_port = free_port()?;
        let mut standby = Cluster {
            bin: self.bin_dir.clone(),
            data: self.root.path().join("standby"),
            log: self.root.path().join("standby.log"),
            running: false,
        };
        let conninfo = format!(
            "host=127.0.0.1 port={} user={user} application_name={STANDBY_NAME}",
            self.primary_port
        );
        standby.run(
            "pg_basebackup",
            &[
                "-D",
                standby.data.to_str().context("non-UTF-8 data path")?,
                "-d",
                &conninfo,
                "-X",
                "stream",
                "-R",
                "-S",
                STANDBY_SLOT,
                "-c",
                "fast",
            ],
        )?;
        // `-R` writes a `primary_conninfo` of its own. Later entries in
        // postgresql.auto.conf win, so the explicit one below is what applies,
        // and it carries the application_name `pg_stat_replication` is keyed on.
        let auto = standby.data.join("postgresql.auto.conf");
        let mut contents = std::fs::read_to_string(&auto).unwrap_or_default();
        contents.push_str(&format!(
            "\nprimary_conninfo = '{conninfo}'\nprimary_slot_name = '{STANDBY_SLOT}'\n\
             hot_standby = on\n"
        ));
        std::fs::write(&auto, contents).context("write standby recovery configuration")?;
        let max_connections: i32 =
            sqlx::query_scalar("SELECT current_setting('max_connections')::int")
                .fetch_one(&admin)
                .await?;
        standby.start(&format!(
            "-h 127.0.0.1 -p {standby_port} -k {root} -c hot_standby=on -c fsync=on \
             -c full_page_writes=on -c max_connections={max_connections} -c max_wal_senders=10",
            root = self.root.path().display()
        ))?;
        self.standby = Some(standby);
        self.standby_port = Some(standby_port);
        self.standby_url = Some(format!(
            "postgresql://{user}@127.0.0.1:{standby_port}/postgres"
        ));
        wait_for(Duration::from_secs(60), || async {
            let streaming: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_replication \
                 WHERE application_name=$1 AND state='streaming')",
            )
            .bind(STANDBY_NAME)
            .fetch_one(&admin)
            .await
            .unwrap_or(false);
            streaming
        })
        .await
        .context("standby never reached state='streaming'")?;
        if self.replication == Replication::Sync {
            let method = self.synchronous_method;
            sqlx::query(&format!(
                "ALTER SYSTEM SET synchronous_standby_names = '{}'",
                method.standby_names()
            ))
            .execute(&admin)
            .await?;
            sqlx::query("SELECT pg_reload_conf()")
                .execute(&admin)
                .await?;
            // The wait is for the one state the method produces, `sync`
            // under `FIRST` and `quorum` under `ANY`, not for either: a
            // cluster that reached the other would be misconfigured.
            wait_for(Duration::from_secs(60), || async {
                sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_replication \
                     WHERE application_name=$1 AND sync_state=$2)",
                )
                .bind(STANDBY_NAME)
                .bind(method.standby_sync_state())
                .fetch_one(&admin)
                .await
                .unwrap_or(false)
            })
            .await
            .with_context(|| {
                format!(
                    "standby never reached sync_state='{}'",
                    method.standby_sync_state()
                )
            })?;
        }
        admin.close().await;
        Ok(())
    }

    /// Stop both clusters and, unless artifacts are kept, remove the data
    /// directories. Safe to call more than once.
    pub fn stop(&mut self) {
        if let Some(standby) = self.standby.as_mut() {
            standby.stop();
        }
        self.primary.stop();
        // The root itself goes when `TempRoot` drops, which is after this
        // returns, so the data directory is never removed under a live server.
    }
}

impl Drop for ManagedPostgres {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn wait_for<F, Fut>(limit: Duration, mut probe: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + limit;
    loop {
        if probe().await {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            bail!("condition not reached within {limit:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The three durability settings the artifact reads back from PostgreSQL.
pub async fn durability(pool: &PgPool) -> Result<(String, String, String)> {
    let row = sqlx::query(
        "SELECT current_setting('fsync') AS fsync, \
         current_setting('full_page_writes') AS full_page_writes, \
         current_setting('synchronous_commit') AS synchronous_commit",
    )
    .fetch_one(pool)
    .await?;
    Ok((
        row.try_get("fsync")?,
        row.try_get("full_page_writes")?,
        row.try_get("synchronous_commit")?,
    ))
}

pub async fn server_version(pool: &PgPool) -> Result<String> {
    Ok(sqlx::query_scalar("SHOW server_version")
        .fetch_one(pool)
        .await?)
}
