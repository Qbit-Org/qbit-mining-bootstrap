//! A private durable primary for server-wide insert-LSN measurements.
//! Separate schemas do not isolate WAL from other tests or their maintenance.
use anyhow::{ensure, Context, Result};
use std::{path::PathBuf, process::Command};

pub struct Primary {
    pub url: String,
    bin: PathBuf,
    data: PathBuf,
    running: bool,
    _directory: tempfile::TempDir,
}

impl Primary {
    pub async fn start(bin: String) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::start_blocking(bin)).await?
    }

    fn start_blocking(bin: String) -> Result<Self> {
        // Keep the Unix socket path short and pg_ctl's option path space-free.
        let directory = tempfile::Builder::new()
            .prefix("compact-wal-")
            .tempdir_in("/tmp")?;
        let data = directory.path().join("data");
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        let mut primary = Self {
            url: String::new(),
            bin: bin.into(),
            data,
            running: false,
            _directory: directory,
        };
        primary.command(
            "initdb",
            &[
                "-D",
                primary.data.to_str().context("database path")?,
                "-A",
                "trust",
                "--no-locale",
                "-E",
                "UTF8",
            ],
        )?;
        // Mark before starting so any partially successful startup is stopped.
        primary.running = true;
        primary.control(&[
            "-l",
            primary.data.with_extension("log").to_str().context("log path")?,
            "-o",
            &format!(
                "-h 127.0.0.1 -p {port} -k {} -c fsync=on -c full_page_writes=on -c synchronous_commit=on",
                primary._directory.path().display()
            ),
            "-w",
            "start",
        ])?;
        let username = Command::new("id").arg("-un").output()?;
        ensure!(
            username.status.success(),
            "could not identify database owner"
        );
        let username = String::from_utf8(username.stdout)?;
        let mut url = url::Url::parse(&format!("postgresql://127.0.0.1:{port}/postgres"))?;
        url.set_username(username.trim())
            .map_err(|_| anyhow::anyhow!("invalid database owner"))?;
        primary.url = url.to_string();
        Ok(primary)
    }

    fn command(&self, binary: &str, args: &[&str]) -> Result<()> {
        let output = Command::new(self.bin.join(binary)).args(args).output()?;
        ensure!(
            output.status.success(),
            "{binary} failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    fn control(&self, args: &[&str]) -> Result<()> {
        let mut all = vec![
            "-D",
            self.data.to_str().context("database path")?,
            "-t",
            "10",
        ];
        all.extend_from_slice(args);
        self.command("pg_ctl", &all)
    }

    fn stop(&mut self) -> Result<()> {
        if self.running {
            self.control(&["-m", "immediate", "-w", "stop"])?;
            self.running = false;
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<()> {
        tokio::task::spawn_blocking(move || self.stop()).await?
    }
}

impl Drop for Primary {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
