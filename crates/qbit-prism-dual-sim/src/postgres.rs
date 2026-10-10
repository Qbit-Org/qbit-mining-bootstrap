//! One node's PostgreSQL 16 cluster, which the fault injector can crash,
//! freeze, restore from an older base backup or replace with an empty disk.
//!
//! Each cluster listens on loopback TCP only (`unix_socket_directories` is
//! empty, so no socket path can outgrow the 107-byte limit) and keeps the
//! durability settings the ledger refuses to run without: `fsync`,
//! `full_page_writes` and `synchronous_commit` on. `wal_level=replica` lets
//! it serve base backups and, in the 3.0 topology, a streaming standby.
//! Authentication is `trust` on loopback for the harness's own superuser
//! (the OS user, as `initdb` names it); the roles the nodes use are created
//! by the topology.
//!
//! The `Cluster` of `crates/qbit-prism-server/tests/support/live_pg_failover.rs`
//! is the model for the `pg_ctl` handling.

use crate::process::{alive, children, signal};
use anyhow::{bail, ensure, Context, Result};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

/// Settings every cluster runs with, written to `postgresql.conf` after
/// `initdb` (never through `pg_ctl -o`, which goes through a shell), so a
/// base backup or a standby clone carries them too.
const DURABLE_SETTINGS: &[(&str, &str)] = &[
    ("listen_addresses", "'127.0.0.1'"),
    ("unix_socket_directories", "''"),
    ("fsync", "on"),
    ("full_page_writes", "on"),
    ("synchronous_commit", "on"),
    ("max_connections", "200"),
    ("wal_level", "replica"),
    ("max_wal_senders", "10"),
    ("max_replication_slots", "10"),
    ("log_line_prefix", "'%m [%p] %a '"),
    ("log_min_duration_statement", "'1s'"),
    ("log_lock_waits", "on"),
];

/// How long a `pg_ctl` start or stop may take: crash recovery after a kill
/// replays the WAL since the last checkpoint, a few seconds here.
const PG_CTL_SECONDS: &str = "60";

/// What the cluster is doing, for the report and for faults that need it
/// running or stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PgState {
    Running,
    Stopped,
    Crashed,
    Frozen,
}

pub struct PgNode {
    name: String,
    bin: PathBuf,
    data: PathBuf,
    log: PathBuf,
    port: u16,
    user: String,
    extra: Vec<String>,
    state: PgState,
    /// The postmaster and backends a freeze stopped, to continue them.
    frozen: Vec<i32>,
}

impl PgNode {
    /// `initdb` a new cluster in `data` (absent or empty) and start it.
    pub fn init(name: &str, bin: &Path, data: &Path, log: &Path, extra: &[String]) -> Result<Self> {
        if !data.exists() {
            std::fs::create_dir_all(data)?;
        }
        let user = current_user()?;
        run(
            &bin.join("initdb"),
            &[
                "-D",
                path_str(data)?,
                "-A",
                "trust",
                "-U",
                &user,
                "--no-locale",
                "-E",
                "UTF8",
            ],
        )
        .with_context(|| format!("initdb for node {name}"))?;
        write_settings(data, extra)?;
        let mut node = Self::adopt(name, bin, data, log, extra)?;
        node.start()?;
        Ok(node)
    }

    /// A handle on an existing data directory (a base backup made into a
    /// standby, say), not yet started.
    pub fn adopt(
        name: &str,
        bin: &Path,
        data: &Path,
        log: &Path,
        extra: &[String],
    ) -> Result<Self> {
        Ok(Self {
            name: name.to_owned(),
            bin: bin.to_owned(),
            data: data.to_owned(),
            log: log.to_owned(),
            port: free_port()?,
            user: current_user()?,
            extra: extra.to_vec(),
            state: PgState::Stopped,
            frozen: Vec::new(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn data(&self) -> &Path {
        &self.data
    }

    pub fn log(&self) -> &Path {
        &self.log
    }

    pub fn state(&self) -> PgState {
        self.state
    }

    /// The harness's superuser URL for `database`.
    pub fn admin_url(&self, database: &str) -> String {
        self.url(&self.user, database)
    }

    pub fn url(&self, user: &str, database: &str) -> String {
        format!("postgresql://{user}@127.0.0.1:{}/{database}", self.port)
    }

    pub fn superuser(&self) -> &str {
        &self.user
    }

    fn pg_ctl(&self, args: &[&str]) -> Result<std::process::Output> {
        let mut all = vec!["-D", path_str(&self.data)?, "-t", PG_CTL_SECONDS];
        all.extend_from_slice(args);
        Command::new(self.bin.join("pg_ctl"))
            .args(&all)
            .output()
            .context("running pg_ctl")
    }

    /// Start the postmaster and wait until it accepts connections. After a
    /// crash this is the crash recovery.
    pub fn start(&mut self) -> Result<()> {
        let options = format!("-p {}", self.port);
        let output = self.pg_ctl(&["-l", path_str(&self.log)?, "-o", &options, "-w", "start"])?;
        ensure!(
            output.status.success(),
            "pg_ctl start of node {} failed: {} {}\n{}",
            self.name,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
            self.tail(30)
        );
        self.state = PgState::Running;
        Ok(())
    }

    /// `pg_ctl stop -m immediate`: an abrupt stop that still lets the
    /// postmaster take its backends down. Nothing if already down.
    pub fn stop_immediate(&mut self) -> Result<()> {
        if self.state == PgState::Frozen {
            self.thaw()?;
        }
        if self.postmaster_pid().is_some_and(alive) {
            let output = self.pg_ctl(&["-m", "immediate", "-w", "stop"])?;
            ensure!(
                output.status.success(),
                "pg_ctl stop of node {} failed: {}",
                self.name,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        self.state = PgState::Stopped;
        Ok(())
    }

    /// `pg_ctl stop -m fast`: a clean shutdown with a checkpoint.
    pub fn stop_fast(&mut self) -> Result<()> {
        if self.state == PgState::Frozen {
            self.thaw()?;
        }
        if self.postmaster_pid().is_some_and(alive) {
            let output = self.pg_ctl(&["-m", "fast", "-w", "stop"])?;
            ensure!(
                output.status.success(),
                "pg_ctl stop of node {} failed: {}",
                self.name,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        self.state = PgState::Stopped;
        Ok(())
    }

    /// The postmaster's pid, from the first line of `postmaster.pid`.
    pub fn postmaster_pid(&self) -> Option<i32> {
        std::fs::read_to_string(self.data.join("postmaster.pid"))
            .ok()?
            .lines()
            .next()?
            .trim()
            .parse()
            .ok()
    }

    /// SIGKILL the postmaster and every backend at once, as the OOM killer
    /// or a host crash takes PostgreSQL down: no checkpoint, no goodbye to
    /// any client. The next [`PgNode::start`] runs crash recovery.
    pub fn kill9(&mut self) -> Result<()> {
        let Some(postmaster) = self.postmaster_pid() else {
            bail!("node {} has no postmaster to kill", self.name);
        };
        let mut victims = children(postmaster);
        victims.push(postmaster);
        for pid in &victims {
            let _ = signal(*pid, libc::SIGKILL);
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while victims.iter().any(|pid| alive(*pid) && !zombie(*pid)) {
            ensure!(
                Instant::now() < deadline,
                "node {}'s processes outlived SIGKILL for 30 s",
                self.name
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        self.frozen.clear();
        self.state = PgState::Crashed;
        Ok(())
    }

    /// SIGSTOP the postmaster and every backend: connections stay open and
    /// every lock stays held, and nothing answers.
    pub fn freeze(&mut self) -> Result<()> {
        let Some(postmaster) = self.postmaster_pid() else {
            bail!("node {} has no postmaster to freeze", self.name);
        };
        let mut stopped = vec![postmaster];
        stopped.extend(children(postmaster));
        for pid in &stopped {
            signal(*pid, libc::SIGSTOP)?;
        }
        self.frozen = stopped;
        self.state = PgState::Frozen;
        Ok(())
    }

    pub fn thaw(&mut self) -> Result<()> {
        for pid in self.frozen.drain(..) {
            let _ = signal(pid, libc::SIGCONT);
        }
        self.state = PgState::Running;
        Ok(())
    }

    /// A consistent, self-contained base backup of the running cluster in
    /// `dest` (`pg_basebackup -X stream`), startable as is.
    pub fn base_backup(&self, dest: &Path) -> Result<()> {
        let port = self.port.to_string();
        run(
            &self.bin.join("pg_basebackup"),
            &[
                "-h",
                "127.0.0.1",
                "-p",
                &port,
                "-U",
                &self.user,
                "-D",
                path_str(dest)?,
                "-X",
                "stream",
                "-c",
                "fast",
            ],
        )
        .with_context(|| format!("base backup of node {}", self.name))
    }

    /// Replace the data directory with a copy of `backup` and start it: the
    /// node comes back as it was when the backup was taken.
    pub fn restore_from(&mut self, backup: &Path) -> Result<()> {
        self.stop_immediate()?;
        std::fs::remove_dir_all(&self.data)
            .with_context(|| format!("removing node {}'s data", self.name))?;
        copy_dir(backup, &self.data)?;
        set_private(&self.data)?;
        self.start()
    }

    /// Stop and delete the data directory: the disk is gone.
    pub fn wipe(&mut self) -> Result<()> {
        self.stop_immediate()?;
        if self.data.exists() {
            std::fs::remove_dir_all(&self.data)
                .with_context(|| format!("removing node {}'s data", self.name))?;
        }
        Ok(())
    }

    /// `initdb` an empty cluster in the wiped data directory and start it,
    /// on the same port: the replacement disk.
    pub fn reinit(&mut self) -> Result<()> {
        ensure!(
            !self.data.exists(),
            "node {}'s data directory still exists; wipe it first",
            self.name
        );
        std::fs::create_dir_all(&self.data)?;
        run(
            &self.bin.join("initdb"),
            &[
                "-D",
                path_str(&self.data)?,
                "-A",
                "trust",
                "-U",
                &self.user,
                "--no-locale",
                "-E",
                "UTF8",
            ],
        )?;
        write_settings(&self.data, &self.extra)?;
        self.start()
    }

    /// Make this (stopped, empty) node a streaming standby of `primary`
    /// through `link_port` (the replication relay), with a physical slot.
    pub fn clone_as_standby(&mut self, primary: &PgNode, link_port: u16, slot: &str) -> Result<()> {
        ensure!(
            !self.data.exists() || std::fs::read_dir(&self.data)?.next().is_none(),
            "node {}'s data directory is not empty",
            self.name
        );
        let conninfo = format!(
            "host=127.0.0.1 port={link_port} user={} application_name={slot}",
            primary.user
        );
        run(
            &self.bin.join("pg_basebackup"),
            &[
                "-D",
                path_str(&self.data)?,
                "-d",
                &conninfo,
                "-X",
                "stream",
                "-R",
                "-C",
                "-S",
                slot,
                "-c",
                "fast",
            ],
        )
        .with_context(|| format!("cloning node {} as a standby", self.name))?;
        set_private(&self.data)?;
        self.start()
    }

    /// Promote a standby into an independent primary and wait until it has
    /// left recovery.
    pub async fn promote(&self) -> Result<()> {
        let pool = self.admin_pool("postgres").await?;
        let promoted: bool = sqlx::query_scalar("SELECT pg_promote(true, 60)")
            .fetch_one(&pool)
            .await?;
        ensure!(promoted, "node {} did not promote within 60 s", self.name);
        let in_recovery: bool = sqlx::query_scalar("SELECT pg_is_in_recovery()")
            .fetch_one(&pool)
            .await?;
        ensure!(
            !in_recovery,
            "node {} is still in recovery after promotion",
            self.name
        );
        pool.close().await;
        Ok(())
    }

    /// A small superuser pool on `database`.
    pub async fn admin_pool(&self, database: &str) -> Result<PgPool> {
        PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(20))
            .connect(&self.admin_url(database))
            .await
            .with_context(|| format!("connecting to node {}'s {database}", self.name))
    }

    /// The last `lines` lines of the server log.
    pub fn tail(&self, lines: usize) -> String {
        let log = std::fs::read_to_string(&self.log).unwrap_or_default();
        let tail: Vec<&str> = log.lines().rev().take(lines).collect();
        let mut report = format!(
            "--- node {} PostgreSQL log, last {lines} lines\n",
            self.name
        );
        for line in tail.into_iter().rev() {
            report.push_str(line);
            report.push('\n');
        }
        report
    }
}

impl Drop for PgNode {
    fn drop(&mut self) {
        if self.state == PgState::Frozen {
            let _ = self.thaw();
        }
        if let Some(pid) = self.postmaster_pid() {
            if alive(pid) {
                let _ = self.pg_ctl(&["-m", "immediate", "-w", "stop"]);
            }
        }
    }
}

/// A zombie has exited and waits for its parent; it counts as gone.
fn zombie(pid: i32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|stat| {
            stat.rsplit_once(')')
                .and_then(|(_, rest)| rest.trim().chars().next())
        })
        == Some('Z')
}

/// Append the durable settings and `extra` (`name = value` lines) to the
/// data directory's `postgresql.conf`.
fn write_settings(data: &Path, extra: &[String]) -> Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(data.join("postgresql.conf"))
        .context("opening postgresql.conf")?;
    writeln!(file, "\n# dual-sim")?;
    for (name, value) in DURABLE_SETTINGS {
        writeln!(file, "{name} = {value}")?;
    }
    for line in extra {
        writeln!(file, "{line}")?;
    }
    Ok(())
}

fn current_user() -> Result<String> {
    let output = Command::new("id").arg("-un").output()?;
    ensure!(output.status.success(), "id -un failed");
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str()
        .with_context(|| format!("{} is not UTF-8", path.display()))
}

fn run(program: &Path, args: &[&str]) -> Result<()> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("running {}", program.display()))?;
    ensure!(
        output.status.success(),
        "{} failed: {}{}",
        program.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// A port nobody listens on now. The listener is dropped before the
/// postmaster binds it; a collision fails the start loudly, never silently.
pub fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

fn set_private(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

/// Copy a directory tree, files and symlinks as they are.
pub fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let target = to.join(entry.file_name());
        if kind.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else if kind.is_symlink() {
            std::os::unix::fs::symlink(std::fs::read_link(entry.path())?, &target)?;
        } else {
            std::fs::copy(entry.path(), &target).with_context(|| {
                format!("copying {} to {}", entry.path().display(), target.display())
            })?;
        }
    }
    Ok(())
}
