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
/// The primary-side synchronous setting D3 flips on.
pub const SYNCHRONOUS_NAMES: &str = "FIRST 1 (prism_standby_1)";

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
#[derive(Clone, Debug, Serialize)]
pub struct ReplicationObservation {
    pub at: chrono::DateTime<chrono::Utc>,
    pub label: String,
    pub synchronous_standby_names: String,
    pub rows: Vec<ReplicationRow>,
}

pub async fn observe_replication(pool: &PgPool, label: &str) -> Result<ReplicationObservation> {
    let names: String = sqlx::query_scalar("SHOW synchronous_standby_names")
        .fetch_one(pool)
        .await
        .unwrap_or_default();
    let rows = sqlx::query(
        "SELECT application_name,state,sync_state,sent_lsn::text,write_lsn::text,\
         flush_lsn::text,replay_lsn::text FROM pg_stat_replication",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    Ok(ReplicationObservation {
        at: chrono::Utc::now(),
        label: label.to_owned(),
        synchronous_standby_names: names,
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

/// Detect what replication is actually configured, for external databases.
/// Never assumed from a flag.
pub async fn detect_replication(pool: &PgPool) -> Result<Replication> {
    let observation = observe_replication(pool, "detect").await?;
    if observation.rows.is_empty() {
        return Ok(Replication::None);
    }
    Ok(if observation.rows.iter().any(|r| r.sync_state == "sync") {
        Replication::Sync
    } else {
        Replication::Async
    })
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

    fn start(&mut self, options: &str) -> Result<()> {
        self.control(&[
            "-l",
            self.log.to_str().context("non-UTF-8 log path")?,
            "-o",
            options,
            "-w",
            "-t",
            "120",
            "start",
        ])?;
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

/// A primary plus, optionally, one streaming standby, both owned by the run.
pub struct ManagedPostgres {
    pub primary_url: String,
    pub primary_port: u16,
    pub standby_url: Option<String>,
    pub standby_port: Option<u16>,
    pub replication: Replication,
    pub pg_stat_statements: Option<String>,
    pub bin_dir: PathBuf,
    pub root: PathBuf,
    keep_artifacts: bool,
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
    /// Start the primary, then the standby the mode asks for.
    pub async fn start(
        bin_dir: PathBuf,
        replication: Replication,
        max_connections: u32,
        keep_artifacts: bool,
    ) -> Result<Self> {
        let root =
            std::env::temp_dir().join(format!("prism-load-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&root).context("create cluster root")?;
        let user = current_user()?;
        let mut primary = Cluster {
            bin: bin_dir.clone(),
            data: root.join("primary"),
            log: root.join("primary.log"),
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
            .map(|dir| dir.join("pg_stat_statements.so"))
            .filter(|path| path.exists())
            .map(|_| "pg_stat_statements".to_owned());
        // `synchronous_standby_names` is deliberately absent: a command-line
        // value shadows the `ALTER SYSTEM` flip the synchronous mode needs.
        let mut options = format!(
            "-h 127.0.0.1 -p {primary_port} -k {root} -c fsync=on -c full_page_writes=on \
             -c wal_level=replica -c max_wal_senders=10 -c max_replication_slots=10 \
             -c max_connections={max_connections}",
            root = root.display()
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
            pg_stat_statements: preload.clone().map(|_| "loaded".to_owned()),
            bin_dir: bin_dir.clone(),
            root: root.clone(),
            keep_artifacts,
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
            data: self.root.join("standby"),
            log: self.root.join("standby.log"),
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
            root = self.root.display()
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
            sqlx::query(&format!(
                "ALTER SYSTEM SET synchronous_standby_names = '{SYNCHRONOUS_NAMES}'"
            ))
            .execute(&admin)
            .await?;
            sqlx::query("SELECT pg_reload_conf()")
                .execute(&admin)
                .await?;
            wait_for(Duration::from_secs(60), || async {
                sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_replication \
                     WHERE application_name=$1 AND sync_state='sync')",
                )
                .bind(STANDBY_NAME)
                .fetch_one(&admin)
                .await
                .unwrap_or(false)
            })
            .await
            .context("standby never reached sync_state='sync'")?;
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
        if !self.keep_artifacts {
            let _ = std::fs::remove_dir_all(&self.root);
        }
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
