//! Frontend child processes: the exact environment they get, and their
//! lifecycle.
//!
//! Every variable is set explicitly and the child's environment is cleared
//! first, so nothing the harness happens to inherit can change what the
//! runtime reads (EP-CONFIG). No `value()`-read key is ever exported empty:
//! `config::value` takes a set-but-empty variable literally.

use anyhow::{bail, ensure, Context, Result};
use qbit_prism_server::capacity::CONFIGURATION_KEYS;
use std::{
    collections::BTreeMap,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Variables whose values never appear in a report.
pub const SECRET_KEYS: &[&str] = &[
    "QBIT_RPC_PASSWORD",
    "PRISM_MANIFEST_SIGNING_SEED_HEX",
    "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX",
];

/// The keys the native server has formally retired (#361). Evidence naming any
/// of them is refused by the `v3` validator as not having measured the native
/// binary, so the harness neither sets them on a frontend nor records them.
///
/// Under `v2` these were carried with the values the frontends really used and
/// annotated as unread (#288). The server has since answered that question, so
/// the honest thing is now to leave them out rather than to explain them.
pub use qbit_prism_server::capacity::RETIRED_CONFIGURATION_KEYS;

/// Everything identical across frontends.
#[derive(Clone, Debug)]
pub struct SharedEnvironment {
    pub rpc_url: String,
    pub rpc_user: String,
    pub rpc_password: String,
    pub share_difficulty: String,
    pub max_difficulty: String,
    pub database_max_connections: u32,
    pub runtime_workers: usize,
    pub stratum_max_connections: usize,
    pub stratum_max_pending_initial_jobs: usize,
    pub share_commit_timeout_seconds: String,
    pub blockpoll_seconds: String,
    pub rust_log: String,
}

/// Everything distinct per frontend.
#[derive(Clone, Debug)]
pub struct FrontendSpec {
    pub index: usize,
    pub instance_id: String,
    pub stratum_port: u16,
    pub audit_port: u16,
    pub database_url: String,
}

/// The exact environment one frontend is launched with.
///
/// The vardiff knobs are pinned to the native defaults rather than left unset,
/// so the artifact's `configuration` block reports the value the runtime read
/// and not a default the reader might change later.
pub fn frontend_environment(
    shared: &SharedEnvironment,
    spec: &FrontendSpec,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    let mut set = |key: &str, value: String| {
        env.insert(key.to_owned(), value);
    };
    // Node and chain.
    set("QBIT_CHAIN", "testnet".into());
    set("QBIT_RPC_URL", shared.rpc_url.clone());
    set("QBIT_RPC_USER", shared.rpc_user.clone());
    set("QBIT_RPC_PASSWORD", shared.rpc_password.clone());
    set("QBIT_PRODUCTION", "0".into());
    set("PRISM_MIN_PEERS", "1".into());
    set("PRISM_TEMPLATE_MAX_AGE_SECONDS", "120".into());
    // Test signing seeds, never production mode.
    set("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1".into());
    set("PRISM_ALLOW_MEMORY_LEDGER", "0".into());
    // Storage.
    set("PRISM_DATABASE_URL", spec.database_url.clone());
    set("PRISM_POSTGRES_INIT_SCHEMA", "1".into());
    set(
        "PRISM_DATABASE_MAX_CONNECTIONS",
        shared.database_max_connections.to_string(),
    );
    set("PRISM_DATABASE_STATEMENT_TIMEOUT_MS", "15000".into());
    set("PRISM_DATABASE_LOCK_TIMEOUT_MS", "5000".into());
    // Identity and listeners.
    set("PRISM_INSTANCE_ID", spec.instance_id.clone());
    set("PRISM_STRATUM_BIND", "127.0.0.1".into());
    set("PRISM_STRATUM_PORT", spec.stratum_port.to_string());
    set("PRISM_AUDIT_BIND", "127.0.0.1".into());
    set("PRISM_AUDIT_PORT", spec.audit_port.to_string());
    // Difficulty. Vardiff off, and the floor equal to the share difficulty so
    // no clamp can move the target the client mines against.
    set("PRISM_STRATUM_VARDIFF", "0".into());
    set("PRISM_STRATUM_SHARE_DIFF", shared.share_difficulty.clone());
    set(
        "PRISM_STRATUM_VARDIFF_MIN_DIFF",
        shared.share_difficulty.clone(),
    );
    set(
        "PRISM_STRATUM_VARDIFF_START_DIFF",
        shared.share_difficulty.clone(),
    );
    set(
        "PRISM_STRATUM_VARDIFF_MAX_DIFF",
        shared.max_difficulty.clone(),
    );
    set("PRISM_STRATUM_VARDIFF_TARGET_SECONDS", "15".into());
    set("PRISM_STRATUM_VARDIFF_RETARGET_SECONDS", "90".into());
    set("PRISM_STRATUM_VARDIFF_MAX_STEP_UP", "4".into());
    set("PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN", "4".into());
    set("PRISM_STRATUM_VARDIFF_EWMA_ALPHA", "0.4".into());
    set("PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE", "0.25".into());
    // Read only by `capacity-evidence` (#288); recorded, never claimed as a
    // native tuning control.
    // Share commit and Stratum transport.
    set(
        "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS",
        shared.share_commit_timeout_seconds.clone(),
    );
    set("PRISM_STRATUM_SEND_TIMEOUT_SECONDS", "20".into());
    set("PRISM_STRATUM_EXTRANONCE2_SIZE", "8".into());
    set(
        "PRISM_STRATUM_MAX_CONNECTIONS",
        shared.stratum_max_connections.to_string(),
    );
    set(
        "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS",
        shared.stratum_max_pending_initial_jobs.to_string(),
    );
    set("PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME", "0".into());
    set("PRISM_STRATUM_MAX_MESSAGE_BYTES", "16384".into());
    set(
        "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION",
        "64".into(),
    );
    set("PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS", "30".into());
    set("PRISM_STRATUM_STALE_GRACE_SECONDS", "3".into());
    set("PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS", "30".into());
    set("PRISM_STRATUM_VARDIFF_RESUME", "1".into());
    set("PRISM_STRATUM_VARDIFF_RESUME_TTL_SECONDS", "900".into());
    // Runtime sizing.
    set("PRISM_RUNTIME_WORKERS", shared.runtime_workers.to_string());
    set("PRISM_JOB_BUILD_EXECUTOR_WORKERS", "2".into());
    set("PRISM_POSTGRES_READ_CONCURRENCY", "4".into());
    // Refresh cadence and settlement policy.
    set("PRISM_BLOCKPOLL_SECONDS", shared.blockpoll_seconds.clone());
    set("PRISM_BLOCKWAIT_ENABLED", "1".into());
    set("PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS", "60".into());
    set("PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS", "15".into());
    set("PRISM_CTV_SETTLEMENT_ENABLED", "0".into());
    set("PRISM_CTV_BROADCASTER_ENABLED", "0".into());
    set("PRISM_POOL_FEE_ENABLED", "0".into());
    set("PRISM_COINBASE_OUTPUT_POLICY", "canonical".into());
    set("PRISM_COINBASE_TAG", "/PRISM/".into());
    set("PRISM_HASHRATE_ROLLUP_ENABLED", "1".into());
    set("RUST_LOG", shared.rust_log.clone());
    env
}

/// The 16 keys `capacity-evidence` validates, taken from the exact environment
/// the frontends were launched with.
pub fn configuration_block(env: &BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
    let mut block = BTreeMap::new();
    for key in CONFIGURATION_KEYS {
        let value = env
            .get(*key)
            .with_context(|| format!("frontend environment is missing {key}"))?;
        ensure!(
            !value.trim().is_empty(),
            "frontend environment sets {key} to an empty string"
        );
        block.insert((*key).to_owned(), value.clone());
    }
    Ok(block)
}

/// The environment as a report may print it.
pub fn redacted(env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    env.iter()
        .map(|(key, value)| (key.clone(), redact_value(key, value)))
        .collect()
}

/// One variable as a report may print it. A secret key loses its whole value;
/// every other value loses any password a URL inside it carries, so a
/// `postgresql://user:password@host/db` in `PRISM_DATABASE_URL` -- or in any
/// URL-valued variable added later -- never reaches a file meant to be
/// attached to an issue.
pub fn redact_value(key: &str, value: &str) -> String {
    let secret = SECRET_KEYS.contains(&key) || key.contains("SEED") || key.contains("PASSWORD");
    if secret {
        REDACTED.to_owned()
    } else {
        redact_url_secrets(value)
    }
}

pub const REDACTED: &str = "<redacted>";

/// Strip the password from a URL-shaped value: the password half of the
/// authority's userinfo, and the value of any `password` query parameter
/// (the form libpq and sqlx also accept). Anything that is not a URL is
/// returned unchanged.
///
/// The query key is compared as SQLx reads it, percent-decoded: SQLx takes
/// `pass%77ord=secret` as the password, so the redaction has to as well.
/// Comparing the encoded spelling let that value through into both reports
/// (EP-OBSERVABILITY). The key is written back as it came, so the URL stays
/// the one the frontend was given, minus the secret.
pub fn redact_url_secrets(value: &str) -> String {
    let Some((scheme, rest)) = value.split_once("://") else {
        return value.to_owned();
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    let authority = match authority.rsplit_once('@') {
        Some((userinfo, host)) => match userinfo.split_once(':') {
            Some((user, _password)) => format!("{user}:{REDACTED}@{host}"),
            None => format!("{userinfo}@{host}"),
        },
        None => authority.to_owned(),
    };
    let tail = match tail.split_once('?') {
        Some((path, query)) => {
            let (query, fragment) = match query.split_once('#') {
                Some((query, fragment)) => (query, Some(fragment)),
                None => (query, None),
            };
            let query = query
                .split('&')
                .map(|pair| match pair.split_once('=') {
                    Some((name, _)) if percent_decode(name).eq_ignore_ascii_case("password") => {
                        format!("{name}={REDACTED}")
                    }
                    _ => pair.to_owned(),
                })
                .collect::<Vec<_>>()
                .join("&");
            match fragment {
                Some(fragment) => format!("{path}?{query}#{fragment}"),
                None => format!("{path}?{query}"),
            }
        }
        None => tail.to_owned(),
    };
    format!("{scheme}://{authority}{tail}")
}

/// Decode `%XX` escapes and `+` in one query-string component, as a
/// form-encoded reader does; an escape that is not two hex digits is kept
/// as written, the way SQLx's decoder keeps it. This is how SQLx reads a
/// URL's query keys, so it is how every key comparison in the harness --
/// the endpoint parameters the proxy rewrite drops, the `password`
/// parameter the redaction strips -- has to read them.
pub fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len()
                && bytes[index + 1].is_ascii_hexdigit()
                && bytes[index + 2].is_ascii_hexdigit() =>
            {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).expect("ascii");
                out.push(u8::from_str_radix(hex, 16).expect("two hex digits"));
                index += 3;
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Build profile of a binary, inferred from its Cargo output directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuildProfile {
    Debug,
    Release,
    Unknown,
}

impl BuildProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Release => "release",
            Self::Unknown => "unknown",
        }
    }
}

/// Inferred from the directory Cargo put the binary in, after following
/// symlinks: a link into `target/release` is a release build, while a copied
/// or installed binary has no Cargo directory to read and stays `Unknown`.
pub fn build_profile(path: &Path) -> BuildProfile {
    let resolved = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    match resolved
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
    {
        Some("debug") => BuildProfile::Debug,
        Some("release") => BuildProfile::Release,
        _ => BuildProfile::Unknown,
    }
}

/// Whether a server of this profile may be driven. Only a build shown to be a
/// release build measures capacity; a debug build and a build whose profile
/// cannot be determined both need `--allow-debug-server`, because an unknown
/// profile is not evidence of a release build. The override admits the run
/// and nothing more: the artifact it produces is `example` evidence
/// (`run::artifact_kind`), the same way a dirty tree's or an unverified
/// binary's is.
pub fn check_server_profile(profile: BuildProfile, allow_debug: bool, path: &Path) -> Result<()> {
    match profile {
        BuildProfile::Release => Ok(()),
        BuildProfile::Debug => {
            ensure!(
                allow_debug,
                "{} is a debug build, which does not measure capacity; pass \
                 --allow-debug-server to run it anyway, which forces artifact_kind example",
                path.display()
            );
            Ok(())
        }
        BuildProfile::Unknown => {
            ensure!(
                allow_debug,
                "the build profile of {} cannot be determined: it is not in a Cargo debug or \
                 release directory, so it cannot be shown to be a release build; build it with \
                 --release and point --server-bin at target/release, or pass \
                 --allow-debug-server to run it anyway, which forces artifact_kind example",
                path.display()
            );
            Ok(())
        }
    }
}

/// How a frontend's log files are opened.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogMode {
    /// The first launch of an invocation: whatever an earlier invocation
    /// left in the same `--out` is discarded.
    Truncate,
    /// A restart within the invocation: the previous process's lines stay.
    Append,
}

impl LogMode {
    fn open(self, path: &Path) -> Result<std::fs::File> {
        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        match self {
            Self::Truncate => options.write(true).truncate(true),
            Self::Append => options.append(true),
        };
        options
            .open(path)
            .with_context(|| format!("opening {}", path.display()))
    }
}

/// A running frontend.
pub struct Frontend {
    pub spec: FrontendSpec,
    pub environment: BTreeMap<String, String>,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
    pub restarts: usize,
    server_bin: PathBuf,
    child: Option<Child>,
}

impl Frontend {
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    pub fn stratum_address(&self) -> String {
        format!("127.0.0.1:{}", self.spec.stratum_port)
    }

    pub fn metrics_url(&self) -> String {
        format!("http://127.0.0.1:{}/metrics", self.spec.audit_port)
    }

    pub fn health_url(&self) -> String {
        format!("http://127.0.0.1:{}/healthz", self.spec.audit_port)
    }

    pub fn launch(
        server_bin: PathBuf,
        spec: FrontendSpec,
        environment: BTreeMap<String, String>,
        log_dir: &Path,
    ) -> Result<Self> {
        let stdout_path = log_dir.join(format!("{}.stdout.log", spec.instance_id));
        let stderr_path = log_dir.join(format!("{}.stderr.log", spec.instance_id));
        let mut frontend = Self {
            spec,
            environment,
            stdout_path,
            stderr_path,
            restarts: 0,
            server_bin,
            child: None,
        };
        // A fresh invocation starts its logs empty. The blocked-run classifier
        // reads the whole stderr file, so a refusal an earlier run in the same
        // `--out` logged would otherwise make this healthy run exit 3.
        frontend.spawn(LogMode::Truncate)?;
        Ok(frontend)
    }

    fn spawn(&mut self, logs: LogMode) -> Result<()> {
        let stdout = logs.open(&self.stdout_path)?;
        let stderr = logs.open(&self.stderr_path)?;
        let mut command = Command::new(&self.server_bin);
        command.arg("run");
        command.env_clear();
        // A child with no PATH or HOME still runs, but keeping them makes the
        // process legible in `ps` and lets it resolve a CA bundle if one is
        // ever needed.
        for passthrough in ["PATH", "HOME", "LANG", "LC_ALL", "TZ"] {
            if let Ok(value) = std::env::var(passthrough) {
                command.env(passthrough, value);
            }
        }
        for (key, value) in &self.environment {
            command.env(key, value);
        }
        command.stdin(Stdio::null());
        command.stdout(Stdio::from(stdout));
        command.stderr(Stdio::from(stderr));
        // Its own session, so the whole frontend can be killed as a group even
        // if it ever spawns helpers.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command
            .spawn()
            .with_context(|| format!("spawn {}", self.server_bin.display()))?;
        self.child = Some(child);
        Ok(())
    }

    /// Restart after a kill, keeping the same ports, instance id and logs:
    /// what the previous process logged in this invocation is evidence.
    pub fn restart(&mut self) -> Result<()> {
        self.kill();
        self.restarts += 1;
        self.spawn(LogMode::Append)
    }

    /// SIGKILL the whole process group, then reap.
    pub fn kill(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let pid = child.id() as libc::pid_t;
            unsafe {
                libc::killpg(pid, libc::SIGKILL);
                libc::kill(pid, libc::SIGKILL);
            }
            let _ = child.wait();
        }
        self.child = None;
    }

    pub fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.child
            .as_mut()
            .and_then(|c| c.try_wait().ok().flatten())
    }

    /// Wait for the `PRISM listening` line and a 200 from `/healthz`.
    pub async fn wait_ready(&mut self, limit: Duration) -> Result<()> {
        let deadline = Instant::now() + limit;
        let mut listening = false;
        loop {
            if let Some(status) = self.exited() {
                bail!(
                    "{} exited during startup with {status}; see {}",
                    self.spec.instance_id,
                    self.stderr_path.display()
                );
            }
            if !listening {
                let log = std::fs::read_to_string(&self.stderr_path).unwrap_or_default();
                listening = log.contains("PRISM listening");
            }
            if listening {
                let response = reqwest::Client::new()
                    .get(self.health_url())
                    .timeout(Duration::from_secs(5))
                    .send()
                    .await;
                if response.is_ok_and(|r| r.status().is_success()) {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                bail!(
                    "{} did not become ready within {limit:?} (listening={listening}); see {}",
                    self.spec.instance_id,
                    self.stderr_path.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// The frontend's captured stderr, for the blocked-run classifier.
    pub fn read_stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }
}

impl Drop for Frontend {
    fn drop(&mut self) {
        self.kill();
    }
}
