//! A disposable PostgreSQL 16 cluster a live scenario owns and can break:
//! its data directory on a filesystem the scenario controls (#575 disk
//! exhaustion) or its clock under an injected offset (#575 clock jumps). Its
//! log and Unix socket live in `home`, outside the data directory.
//!
//! Any test binary can include it with `#[path]`, next to
//! `live_host_tools.rs` as module `host_tools`; it needs `anyhow`.
use super::host_tools::run;
use anyhow::{ensure, Context, Result};
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt, path::PathBuf, process::Command};

pub(crate) struct ClusterOptions<'a> {
    /// `PRISM_TEST_PG_BIN_DIR`, from the gate.
    pub bin: PathBuf,
    /// An empty or absent directory for the cluster's data.
    pub data: PathBuf,
    /// A short directory for the log and the Unix socket.
    pub home: PathBuf,
    /// Extra `initdb` arguments.
    pub initdb: &'a [&'a str],
    /// Extra `postgres` options, after the durable defaults.
    pub settings: &'a str,
    /// Environment for the postmaster and every backend it forks.
    pub env: Vec<(String, String)>,
}

pub(crate) struct PrivateCluster {
    bin: PathBuf,
    data: PathBuf,
    log: PathBuf,
    socket: PathBuf,
    settings: String,
    env: Vec<(String, String)>,
    port: u16,
    /// Holds `port` until the first start binds it, so no concurrent socket
    /// takes it meanwhile (#533). A restart reuses the port unreserved.
    reservation: Option<std::net::TcpListener>,
    user: String,
}

impl PrivateCluster {
    /// `initdb` and start, with trust authentication on loopback only.
    pub(crate) fn start(options: ClusterOptions<'_>) -> Result<Self> {
        if !options.data.exists() {
            std::fs::create_dir(&options.data)?;
        }
        std::fs::set_permissions(&options.data, std::fs::Permissions::from_mode(0o700))?;
        let user = String::from_utf8(Command::new("id").arg("-un").output()?.stdout)?
            .trim()
            .to_owned();
        let mut initdb = vec![
            "-D",
            options.data.to_str().context("data path")?,
            "-A",
            "trust",
            "--no-locale",
            "-E",
            "UTF8",
        ];
        initdb.extend_from_slice(options.initdb);
        run(&options.bin.join("initdb"), &initdb)?;
        let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        let mut cluster = Self {
            log: options.home.join("postgresql.log"),
            socket: options.home,
            settings: options.settings.to_owned(),
            env: options.env,
            bin: options.bin,
            data: options.data,
            port: reservation.local_addr()?.port(),
            reservation: Some(reservation),
            user,
        };
        cluster.pg_start()?;
        Ok(cluster)
    }

    fn pg_ctl(&self, args: &[&str]) -> Result<std::process::Output> {
        let mut all = vec!["-D", self.data.to_str().context("data path")?, "-t", "60"];
        all.extend_from_slice(args);
        Ok(Command::new(self.bin.join("pg_ctl"))
            .args(&all)
            .envs(self.env.iter().map(|(name, value)| (name, value)))
            .output()?)
    }

    fn pg_start(&mut self) -> Result<()> {
        self.reservation = None;
        let options = format!(
            "-h 127.0.0.1 -p {} -k {} -c fsync=on -c full_page_writes=on -c synchronous_commit=on -c max_connections=200 {}",
            self.port,
            self.socket.display(),
            self.settings
        );
        let output = self.pg_ctl(&[
            "-l",
            self.log.to_str().context("log path")?,
            "-o",
            &options,
            "-w",
            "start",
        ])?;
        ensure!(
            output.status.success(),
            "pg_ctl start failed: {} {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
            self.diagnostics()
        );
        Ok(())
    }

    pub(crate) fn url(&self) -> String {
        format!(
            "postgresql://{}@127.0.0.1:{}/postgres",
            self.user, self.port
        )
    }

    pub(crate) fn running(&self) -> Result<bool> {
        Ok(self.pg_ctl(&["status"])?.status.success())
    }

    /// Start the cluster if its postmaster is gone; whether it had to be.
    pub(crate) fn ensure_running(&mut self) -> Result<bool> {
        if self.running()? {
            return Ok(false);
        }
        self.pg_start()?;
        Ok(true)
    }

    /// The log's PANIC, FATAL and ERROR lines, grouped by message with
    /// quoted names and numbers removed.
    pub(crate) fn errors(&self) -> String {
        let log = std::fs::read_to_string(&self.log).unwrap_or_default();
        let mut kinds: BTreeMap<String, usize> = BTreeMap::new();
        for line in log.lines() {
            let Some(level) = ["PANIC", "FATAL", "ERROR"]
                .into_iter()
                .find(|level| line.contains(&format!("{level}:")))
            else {
                continue;
            };
            let message = line
                .split_once(&format!("{level}:"))
                .map_or(line, |(_, message)| message)
                .trim();
            let shape: String = message
                .split('"')
                .enumerate()
                .map(|(index, part)| if index % 2 == 1 { "\"…\"" } else { part })
                .collect();
            let shape: String = shape.chars().filter(|c| !c.is_ascii_digit()).collect();
            *kinds.entry(format!("{level}: {shape}")).or_default() += 1;
        }
        kinds
            .iter()
            .map(|(kind, count)| format!("{count:>6} {kind}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub(crate) fn diagnostics(&self) -> String {
        let log = std::fs::read_to_string(&self.log)
            .unwrap_or_else(|error| format!("unreadable: {error}"));
        let tail: Vec<_> = log.lines().rev().take(30).collect();
        let mut report = String::from("--- private PostgreSQL log (last 30 lines, newest first)\n");
        for line in tail {
            report.push_str(line);
            report.push('\n');
        }
        report
    }
}

impl Drop for PrivateCluster {
    fn drop(&mut self) {
        let _ = self.pg_ctl(&["-m", "immediate", "-w", "stop"]);
    }
}
