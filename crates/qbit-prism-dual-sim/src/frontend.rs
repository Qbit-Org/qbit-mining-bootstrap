//! One node's `qbit-prism-server run` frontend: its environment, its process
//! and its health.
//!
//! The environment is the load harness's (`frontend_environment` in
//! `crates/qbit-prism-load/src/frontend.rs`) on regtest: vardiff off and one
//! fixed share difficulty that makes the payout window the scenario's share
//! count on the ramped chain, test signing seeds, the 0-bps pool fee every
//! server requires (#535), and, when the scenario settles through CTV, the
//! live fixtures' CTV settings. Every inherited `PRISM_*` and `QBIT_*`
//! variable is removed first, so an operator's shell cannot change a run.
//!
//! In dual-writer mode the frontend also gets CONTRACT.md §3's settings:
//! `PRISM_DUAL_WRITER`, `PRISM_NODE_INDEX`, `PRISM_CARRY_OWNER`, the peer's
//! DSN (through the peer link's relay) and the sync cadence.

use crate::process::Process;
use anyhow::{ensure, Context, Result};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

/// `PRISM_PAYOUT_MIN_OUTPUT_SATS` on every frontend. A block pays about
/// 4.6e8 sats at the ramped height; the smallest account (1 of 29 sessions)
/// earns about 1.5e7 of it, below this floor, so it accrues carry on one
/// block and is paid down on a later one.
pub const PAYOUT_FLOOR_SATS: u64 = 20_000_000;
/// `PRISM_POOL_FEE_BPS`. The pool-fee output also takes the swept sub-floor
/// dust, and through a CTV fanout it must stay above the payout floor once
/// its share of the fanout fee is carved out (`qbit-prism`'s
/// `apply_ctv_fanout_fee_accounting` refuses the settlement otherwise): 5%
/// of a block is about 2.3e7 sats, above the floor.
pub const POOL_FEE_BPS: u64 = 500;

/// The two PRISM nodes. A is node 0 and the carry owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub enum Node {
    A,
    B,
}

impl Node {
    pub const BOTH: [Node; 2] = [Node::A, Node::B];

    pub fn index(self) -> usize {
        match self {
            Node::A => 0,
            Node::B => 1,
        }
    }

    pub fn peer(self) -> Node {
        match self {
            Node::A => Node::B,
            Node::B => Node::A,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Node::A => "a",
            Node::B => "b",
        }
    }

    pub fn instance_id(self) -> String {
        format!("dual-sim-{}", self.label())
    }
}

/// CONTRACT.md §3's dual-writer settings for one frontend.
#[derive(Clone, Debug, Serialize)]
pub struct DualSettings {
    pub node_index: u8,
    pub carry_owner: bool,
    /// Never serialized: it carries the sync role's DSN.
    #[serde(skip)]
    pub peer_database_url: String,
    #[serde(skip)]
    pub peer_database_url_fallback: Option<String>,
    pub sync_interval_ms: u64,
    pub sync_batch_rows: u64,
}

/// How the payouts settle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Settlement {
    /// Every payout is a direct coinbase output.
    Direct,
    /// Payouts below the direct floor go through CTV fanouts, which the
    /// broadcaster sends at maturity (`PRISM_CTV_SETTLEMENT_ENABLED`).
    Ctv,
}

/// Everything one frontend start needs.
#[derive(Clone, Debug)]
pub struct FrontendSpec {
    pub node: Node,
    pub server_bin: PathBuf,
    pub database_url: String,
    pub qbitd_rpc_port: u16,
    pub stratum_port: u16,
    pub api_port: u16,
    /// Diff-1 share difficulty, as `qbit_prism_load::window::solve_window`
    /// derives it for the scenario's window.
    pub share_difficulty: f64,
    pub settlement: Settlement,
    pub dual: Option<DualSettings>,
    pub stratum_max_connections: usize,
    /// D-7's readiness listener: its port and token, when the scenario's
    /// balancer checks `/readyz`.
    pub readiness: Option<(u16, String)>,
    /// Applied last, over everything above.
    pub overrides: Vec<(String, String)>,
}

impl FrontendSpec {
    /// The exact environment the frontend starts with.
    pub fn environment(&self) -> BTreeMap<String, String> {
        let mut env = BTreeMap::new();
        let mut set = |key: &str, value: String| {
            env.insert(key.to_owned(), value);
        };
        set("QBIT_CHAIN", "regtest".into());
        set("QBIT_RPC_HOST", "127.0.0.1".into());
        set("QBIT_RPC_PORT", self.qbitd_rpc_port.to_string());
        set("QBIT_RPC_USER", crate::chain::RPC_USER.into());
        set("QBIT_RPC_PASSWORD", crate::chain::RPC_PASSWORD.into());
        set("QBIT_PRODUCTION", "0".into());
        set("PRISM_MIN_PEERS", "1".into());
        set("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1".into());
        set("PRISM_DATABASE_URL", self.database_url.clone());
        set("PRISM_POSTGRES_INIT_SCHEMA", "1".into());
        set("PRISM_DATABASE_MAX_CONNECTIONS", "16".into());
        set("PRISM_INSTANCE_ID", self.node.instance_id());
        set("PRISM_STRATUM_BIND", "127.0.0.1".into());
        set("PRISM_STRATUM_PORT", self.stratum_port.to_string());
        set("PRISM_AUDIT_BIND", "127.0.0.1".into());
        set("PRISM_AUDIT_PORT", self.api_port.to_string());
        let difficulty = format!("{:e}", self.share_difficulty);
        set("PRISM_STRATUM_VARDIFF", "0".into());
        set("PRISM_STRATUM_SHARE_DIFF", difficulty.clone());
        set("PRISM_STRATUM_VARDIFF_MIN_DIFF", difficulty.clone());
        set("PRISM_STRATUM_VARDIFF_START_DIFF", difficulty);
        set(
            "PRISM_STRATUM_MAX_CONNECTIONS",
            self.stratum_max_connections.to_string(),
        );
        set("PRISM_RUNTIME_WORKERS", "2".into());
        set("PRISM_BLOCKPOLL_SECONDS", "0.2".into());
        set("PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS", "1".into());
        set("PRISM_PUBLIC_CACHE_ENABLED", "0".into());
        set("PRISM_HEALTH_REFRESH_SECONDS", "1".into());
        set("RUST_LOG", "warn".into());
        // #525, #535: every server refuses to start without a pool fee.
        set("PRISM_POOL_FEE_ENABLED", "1".into());
        set("PRISM_POOL_FEE_BPS", POOL_FEE_BPS.to_string());
        set("PRISM_POOL_FEE_RECIPIENT_ID", "pool-fee".into());
        set(
            "PRISM_POOL_FEE_P2MR_PROGRAM_HEX",
            "fefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe".into(),
        );
        set(
            "PRISM_PAYOUT_MIN_OUTPUT_SATS",
            PAYOUT_FLOOR_SATS.to_string(),
        );
        if self.settlement == Settlement::Ctv {
            // As the live fixtures run CTV: every miner payout goes through a
            // fanout, so fanouts are exercised on every block.
            set(
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS",
                "1000000000000".into(),
            );
            set("PRISM_CTV_SETTLEMENT_ENABLED", "1".into());
            set("PRISM_CTV_BROADCASTER_ENABLED", "1".into());
            set("PRISM_CTV_BROADCASTER_POLL_SECONDS", "0.2".into());
            set("PRISM_CTV_SPEND_SCAN_BLOCKS", "1".into());
            set(
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
                "1000".into(),
            );
        }
        if let Some((port, token)) = &self.readiness {
            set("PRISM_READINESS_BIND", "127.0.0.1".into());
            set("PRISM_READINESS_PORT", port.to_string());
            set("PRISM_READINESS_TOKEN", token.clone());
        }
        if let Some(dual) = &self.dual {
            set("PRISM_DUAL_WRITER", "1".into());
            set("PRISM_NODE_INDEX", dual.node_index.to_string());
            set(
                "PRISM_CARRY_OWNER",
                if dual.carry_owner { "1" } else { "0" }.into(),
            );
            set("PRISM_PEER_DATABASE_URL", dual.peer_database_url.clone());
            if let Some(fallback) = &dual.peer_database_url_fallback {
                set("PRISM_PEER_DATABASE_URL_FALLBACK", fallback.clone());
            }
            set(
                "PRISM_PEER_SYNC_INTERVAL_MS",
                dual.sync_interval_ms.to_string(),
            );
            set(
                "PRISM_PEER_SYNC_BATCH_ROWS",
                dual.sync_batch_rows.to_string(),
            );
        }
        for (key, value) in &self.overrides {
            env.insert(key.clone(), value.clone());
        }
        env
    }

    /// The command for `args` (`run`, or an operator subcommand) with this
    /// frontend's environment, after removing every inherited setting.
    pub fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(&self.server_bin);
        for (name, _) in std::env::vars_os() {
            let name = name.to_string_lossy();
            if name.starts_with("PRISM_") || name.starts_with("QBIT_") {
                command.env_remove(name.as_ref());
            }
        }
        command.args(args).envs(self.environment());
        command
    }
}

/// One operator subcommand's run.
#[derive(Clone, Debug, Serialize)]
pub struct ToolRun {
    pub args: Vec<String>,
    pub success: bool,
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl ToolRun {
    /// stdout as JSON, for the commands that print one report.
    pub fn json(&self) -> Result<Value> {
        serde_json::from_str(self.stdout.trim()).with_context(|| {
            format!(
                "qbit-prism-server {} printed no JSON report: {} {}",
                self.args.join(" "),
                self.stdout.trim(),
                self.stderr.trim()
            )
        })
    }
}

/// A frontend across its starts, kills and freezes.
pub struct Frontend {
    pub spec: FrontendSpec,
    log: PathBuf,
    process: Option<Process>,
    starts: u32,
    client: reqwest::Client,
}

impl Frontend {
    pub fn new(spec: FrontendSpec, logs: &Path) -> Result<Self> {
        Ok(Self {
            log: logs.join(format!("frontend-{}.log", spec.node.label())),
            spec,
            process: None,
            starts: 0,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?,
        })
    }

    pub fn node(&self) -> Node {
        self.spec.node
    }

    pub fn log(&self) -> &Path {
        &self.log
    }

    pub fn starts(&self) -> u32 {
        self.starts
    }

    pub fn running(&self) -> bool {
        self.process
            .as_ref()
            .is_some_and(|process| process.exited().is_none())
    }

    pub fn frozen(&self) -> bool {
        self.process.as_ref().is_some_and(Process::frozen)
    }

    pub fn pid(&self) -> Option<u32> {
        self.process.as_ref().map(Process::pid)
    }

    /// Start `qbit-prism-server run`.
    pub fn start(&mut self) -> Result<()> {
        ensure!(
            !self.running(),
            "frontend {:?} is already running",
            self.node()
        );
        let mut command = self.spec.command(&["run"]);
        self.starts += 1;
        let name = format!("frontend-{} (start {})", self.node().label(), self.starts);
        self.process = Some(Process::spawn(&name, &mut command, &self.log)?);
        Ok(())
    }

    /// SIGKILL: the process dies with whatever it was doing.
    pub fn kill9(&mut self) -> Result<()> {
        if let Some(process) = &self.process {
            process.kill9()?;
        }
        Ok(())
    }

    /// SIGTERM, a graceful stop, then SIGKILL after `grace`.
    pub fn stop(&mut self, grace: Duration) -> Result<bool> {
        match &self.process {
            Some(process) => process.terminate(grace),
            None => Ok(true),
        }
    }

    pub fn freeze(&mut self) -> Result<()> {
        self.process
            .as_ref()
            .context("no frontend process to freeze")?
            .freeze()
    }

    pub fn thaw(&mut self) -> Result<()> {
        self.process
            .as_ref()
            .context("no frontend process to thaw")?
            .thaw()
    }

    /// `/healthz` read straight from the frontend, not through any link:
    /// the status and the body, whatever they are.
    pub async fn health(&self) -> Result<(u16, Value)> {
        let response = self
            .client
            .get(format!("http://127.0.0.1:{}/healthz", self.spec.api_port))
            .send()
            .await?;
        let status = response.status().as_u16();
        let body = response.json().await.unwrap_or(Value::Null);
        Ok((status, body))
    }

    /// Wait until `/healthz` answers 200 with `ok: true`, failing at once if
    /// the process exits.
    pub async fn wait_ready(&self, limit: Duration) -> Result<Duration> {
        let started = Instant::now();
        let mut last;
        loop {
            if let Some(process) = &self.process {
                if let Some(status) = process.exited() {
                    anyhow::bail!(
                        "frontend {:?} exited before it was ready ({status}); see {}",
                        self.node(),
                        self.log.display()
                    );
                }
            }
            match self.health().await {
                Ok((200, body)) if body["ok"] == true => return Ok(started.elapsed()),
                Ok((status, body)) => {
                    last = format!(
                        "HTTP {status}, status {}, error {}",
                        body["status"], body["error"]
                    )
                }
                Err(error) => last = format!("{error:#}"),
            }
            ensure!(
                started.elapsed() < limit,
                "frontend {:?} was not ready within {limit:?}: {last}",
                self.node()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Run an operator subcommand (`node-identity set`, `carry-owner
    /// transfer`, ...) with exactly this frontend's environment, as an
    /// operator runs it on the node; its exit status, stdout and stderr.
    /// Bounded: a hung command fails the scenario rather than the job.
    pub async fn tool(&self, args: &[&str], limit: Duration) -> Result<ToolRun> {
        let mut command = tokio::process::Command::from(self.spec.command(args));
        command.kill_on_drop(true);
        let output = tokio::time::timeout(limit, command.output())
            .await
            .with_context(|| format!("qbit-prism-server {} ran over {limit:?}", args.join(" ")))?
            .with_context(|| format!("running qbit-prism-server {}", args.join(" ")))?;
        Ok(ToolRun {
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            success: output.status.success(),
            code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }

    /// `GET /readyz` on the readiness listener: the status code, with the
    /// token when `with_token`. `None` when the frontend has no listener.
    pub async fn readyz(&self, with_token: bool) -> Result<Option<u16>> {
        let Some((port, token)) = &self.spec.readiness else {
            return Ok(None);
        };
        let mut request = self.client.get(format!("http://127.0.0.1:{port}/readyz"));
        if with_token {
            request = request.header(crate::balancer::TOKEN_HEADER, token);
        }
        Ok(Some(request.send().await?.status().as_u16()))
    }

    /// The `qbit_prism_*` samples of `/metrics` whose names start with
    /// `prefix`, as `name{labels} value` lines.
    pub async fn metrics(&self, prefix: &str) -> Result<Vec<String>> {
        let body = self
            .client
            .get(format!("http://127.0.0.1:{}/metrics", self.spec.api_port))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Ok(body
            .lines()
            .filter(|line| line.starts_with(prefix))
            .map(str::to_owned)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(dual: Option<DualSettings>) -> FrontendSpec {
        FrontendSpec {
            node: Node::B,
            server_bin: "qbit-prism-server".into(),
            database_url: "postgresql://prism@127.0.0.1:1/prism".into(),
            qbitd_rpc_port: 2,
            stratum_port: 3,
            api_port: 4,
            share_difficulty: 1.2e-7,
            settlement: Settlement::Ctv,
            dual,
            stratum_max_connections: 512,
            readiness: None,
            overrides: vec![("RUST_LOG".into(), "info".into())],
        }
    }

    #[test]
    fn single_writer_mode_sets_no_dual_writer_setting() {
        let env = spec(None).environment();
        assert!(env.keys().all(|key| !key.starts_with("PRISM_DUAL")
            && !key.starts_with("PRISM_NODE_INDEX")
            && !key.starts_with("PRISM_CARRY_OWNER")
            && !key.starts_with("PRISM_PEER_")));
        assert_eq!(env["PRISM_INSTANCE_ID"], "dual-sim-b");
        assert_eq!(env["RUST_LOG"], "info", "overrides apply last");
    }

    #[test]
    fn dual_writer_mode_sets_the_contract_settings() {
        let env = spec(Some(DualSettings {
            node_index: 1,
            carry_owner: false,
            peer_database_url: "postgresql://prism_peer_sync@127.0.0.1:5/prism".into(),
            peer_database_url_fallback: None,
            sync_interval_ms: 250,
            sync_batch_rows: 5000,
        }))
        .environment();
        assert_eq!(env["PRISM_DUAL_WRITER"], "1");
        assert_eq!(env["PRISM_NODE_INDEX"], "1");
        assert_eq!(env["PRISM_CARRY_OWNER"], "0");
        assert_eq!(env["PRISM_PEER_SYNC_INTERVAL_MS"], "250");
        assert!(!env.contains_key("PRISM_PEER_DATABASE_URL_FALLBACK"));
    }
}
