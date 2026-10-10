//! The whole simulated pair, assembled for one scenario.
//!
//! **Topologies.**
//!
//! - [`Topology::SingleWriter`], the 3.0 pair: A's PostgreSQL is the one
//!   writer, B's is its asynchronous streaming standby, and both frontends
//!   write to A's (B's across the writer link). Dual-writer mode is off.
//! - [`Topology::DualWriter`], the 3.1 pair: each node's PostgreSQL is an
//!   independent primary, each frontend writes only to its own, and each
//!   pulls the rows its peer originated over the peer link. A is node 0 and
//!   the carry owner.
//! - [`Topology::Unsynced`]: two independent single-writer nodes with no
//!   link between their databases. Nothing in production runs this; it is
//!   the checker's negative control, which must fail invariant 3.
//!
//! **Links.** Every path a fault can cut is a [`Relay`]: the balancer's
//! Stratum and health routes to each node (`public-stratum-*`,
//! `public-health-*`), each node's pull from its peer (`peer-a-to-b` is A's
//! frontend reading B's database), and in the 3.0 pair B's writer link to
//! A's database and the standby's replication link. A frontend reaches its
//! own database and its own `qbitd` directly, as on one host.
//!
//! **Roles.** Each database is owned by a plain login role, `prism`, which
//! the frontends use; the dual-writer peer pull uses `prism_peer_sync`,
//! read-only. Both log in without a password over loopback (`trust`).

use crate::{
    balancer::{BackendTarget, Balancer, BalancerConfig},
    chain::{self, Chain},
    frontend::{DualSettings, Frontend, FrontendSpec, Node, Settlement},
    load::{Load, LoadPlan, RunClock},
    postgres::PgNode,
    relay::{LinkState, Relay, RelayStats},
    rpc_gate::RpcGate,
};
use anyhow::{bail, ensure, Context, Result};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant},
};

/// The database every node's PRISM ledger lives in.
pub const DATABASE: &str = "prism";
/// The frontends' login role, owner of [`DATABASE`].
pub const OWNER_ROLE: &str = "prism";
/// The read-only role a node's peer pull logs in as.
pub const PEER_ROLE: &str = "prism_peer_sync";
/// The 3.0 standby's physical replication slot.
pub const STANDBY_SLOT: &str = "prism_standby_b";

/// What the balancer stand-in checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    /// `/healthz` with `ok: true`: 3.0's only readiness signal.
    Healthz,
    /// CONTRACT.md D-7: the token-protected `GET /readyz` on
    /// `PRISM_READINESS_PORT`, as the Hashbalancer checks the pair.
    Readyz,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Topology {
    SingleWriter,
    DualWriter,
    Unsynced,
}

/// Where the harness's inputs are: the gate's two plus the binaries it
/// built, and the directories it writes.
#[derive(Clone, Debug)]
pub struct Inputs {
    pub pg_bin: PathBuf,
    pub qbitd: PathBuf,
    pub server: PathBuf,
    /// Holds `qbit-prism-audit-verify`.
    pub verifier_dir: PathBuf,
    /// Short and on a real disk: data directories, the ramp cache.
    pub work_root: PathBuf,
    /// Where each scenario's report and logs go.
    pub report_root: PathBuf,
    /// Keep the scenario's data directories after a pass.
    pub keep: bool,
}

/// What one scenario runs.
#[derive(Clone, Debug, Serialize)]
pub struct SimConfig {
    pub scenario: String,
    pub topology: Topology,
    pub settlement: Settlement,
    /// The payout window's length in shares at the ramped difficulty.
    pub window_shares: u64,
    pub load: LoadPlan,
    pub balancer: BalancerConfig,
    pub readiness: Readiness,
    /// Put an [`RpcGate`] between each frontend and its `qbitd` (S8).
    pub rpc_gates: bool,
    pub sync_interval_ms: u64,
    pub sync_batch_rows: u64,
    /// Extra frontend settings, per node, applied last.
    pub overrides: BTreeMap<Node, Vec<(String, String)>>,
}

impl SimConfig {
    /// The defaults every scenario starts from: four accounts of different
    /// sizes (16, 8, 4 and 1 sessions), so the smallest accrues carry and is
    /// paid down while the others are paid at once; 20 shares/s; a
    /// 2,000-share window; CTV settlement; the balancer's default checks.
    pub fn new(scenario: &str, topology: Topology) -> Result<Self> {
        let window_shares = 2_000;
        let bits = u32::from_str_radix(chain::RAMP_BITS, 16)?;
        let solution = qbit_prism_load::window::solve_window(bits, window_shares)?;
        Ok(Self {
            scenario: scenario.to_owned(),
            topology,
            settlement: Settlement::Ctv,
            window_shares,
            load: LoadPlan {
                accounts: vec![
                    crate::load::Account::derived("big", 16),
                    crate::load::Account::derived("mid", 8),
                    crate::load::Account::derived("small", 4),
                    crate::load::Account::derived("tiny", 1),
                ],
                rate: 20.0,
                share_difficulty: solution.share_difficulty,
            },
            balancer: BalancerConfig::default(),
            // The dual-writer pair is checked as the Hashbalancer will check
            // it; the 3.0 pair keeps 3.0's signal.
            readiness: match topology {
                Topology::DualWriter => Readiness::Readyz,
                Topology::SingleWriter | Topology::Unsynced => Readiness::Healthz,
            },
            rpc_gates: false,
            sync_interval_ms: 250,
            sync_batch_rows: 5_000,
            overrides: BTreeMap::new(),
        })
    }
}

/// One thing that happened, on the run's clock.
#[derive(Clone, Debug, Serialize)]
pub struct TimelineEvent {
    pub at_ms: u64,
    pub event: String,
}

/// The relays of one run, by name.
pub struct Links {
    relays: BTreeMap<String, Relay>,
}

impl Links {
    pub fn get(&self, name: &str) -> Result<&Relay> {
        self.relays
            .get(name)
            .with_context(|| format!("no link named {name}"))
    }

    pub fn set(&self, name: &str, state: LinkState) -> Result<()> {
        self.get(name)?.set(state);
        Ok(())
    }

    pub fn names(&self) -> Vec<String> {
        self.relays.keys().cloned().collect()
    }

    pub fn stats(&self) -> Vec<RelayStats> {
        self.relays.values().map(Relay::stats).collect()
    }

    /// The links a cut of `node`'s whole network breaks: its public routes,
    /// both directions of the peer pull, and in the 3.0 pair the writer and
    /// replication links that cross to or from it.
    pub fn of_node(&self, node: Node) -> Vec<String> {
        let label = node.label();
        let peer = node.peer().label();
        self.relays
            .keys()
            .filter(|name| {
                name.as_str() == format!("public-stratum-{label}")
                    || name.as_str() == format!("public-health-{label}")
                    || name.as_str() == format!("peer-{label}-to-{peer}")
                    || name.as_str() == format!("peer-{peer}-to-{label}")
                    || name.as_str() == "writer-b-to-a"
                    || name.as_str() == "replication-b-from-a"
            })
            .cloned()
            .collect()
    }

    /// The links between the two databases: the peer pulls, or in the 3.0
    /// pair the writer and replication links.
    pub fn between_nodes(&self) -> Vec<String> {
        self.relays
            .keys()
            .filter(|name| {
                name.starts_with("peer-")
                    || name.as_str() == "writer-b-to-a"
                    || name.as_str() == "replication-b-from-a"
            })
            .cloned()
            .collect()
    }
}

pub struct Sim {
    pub config: SimConfig,
    pub inputs: Inputs,
    pub clock: RunClock,
    /// The chain's height when the scenario began: the census starts above
    /// it.
    pub start_height: u64,
    /// The scenario's data root (short) and its report directory.
    pub root: PathBuf,
    pub report_dir: PathBuf,
    pub logs: PathBuf,
    pub chain: Chain,
    pub pg: BTreeMap<Node, PgNode>,
    pub frontends: BTreeMap<Node, Frontend>,
    pub links: Links,
    pub balancer: Balancer,
    pub load: Option<Load>,
    /// Each frontend's gate to its `qbitd`, with `rpc_gates`.
    pub gates: BTreeMap<Node, RpcGate>,
    timeline: Mutex<Vec<TimelineEvent>>,
    /// Base backups taken during the run, by name.
    pub backups: BTreeMap<String, PathBuf>,
    /// When the pair began writing as two primaries: just before the dual
    /// bootstrap, or the cutover's. Rows written before it are history both
    /// databases share (the 3.0 pair wrote one database).
    pub dual_since: Option<chrono::DateTime<chrono::Utc>>,
}

impl Sim {
    /// Build and start everything, and wait until both frontends are ready
    /// and the balancer has marked them up. The load is started but not yet
    /// offered: call `load().resume()`.
    pub async fn start(config: SimConfig, inputs: Inputs) -> Result<Self> {
        let clock = RunClock {
            started: Instant::now(),
        };
        let run = format!(
            "{}-{}",
            config.scenario,
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        );
        let root = inputs.work_root.join(&run);
        let report_dir = inputs.report_root.join(&config.scenario);
        let logs = report_dir.join("logs");
        if report_dir.exists() {
            std::fs::remove_dir_all(&report_dir)?;
        }
        std::fs::create_dir_all(&logs)?;
        std::fs::create_dir_all(&root)?;
        let template =
            chain::ramped_template(&inputs.qbitd, &inputs.work_root.join("ramp-cache")).await?;
        let chain = Chain::start(&inputs.qbitd, &template, &root.join("chain"), &logs).await?;
        let (start_height, _, _) = chain.c.tip().await?;

        // --- databases ---------------------------------------------------
        let mut pg = BTreeMap::new();
        let settings: Vec<String> = Vec::new();
        let node_a = PgNode::init(
            "a",
            &inputs.pg_bin,
            &root.join("pg-a"),
            &logs.join("postgres-a.log"),
            &settings,
        )?;
        create_owner(&node_a).await?;
        let mut relays = BTreeMap::new();
        let node_b = match config.topology {
            Topology::SingleWriter => {
                let replication = Relay::open("replication-b-from-a", node_a.port()).await?;
                let mut node_b = PgNode::adopt(
                    "b",
                    &inputs.pg_bin,
                    &root.join("pg-b"),
                    &logs.join("postgres-b.log"),
                    &settings,
                )?;
                node_b.clone_as_standby(&node_a, replication.port(), STANDBY_SLOT)?;
                relays.insert(replication.name().to_owned(), replication);
                node_b
            }
            Topology::DualWriter | Topology::Unsynced => {
                let node_b = PgNode::init(
                    "b",
                    &inputs.pg_bin,
                    &root.join("pg-b"),
                    &logs.join("postgres-b.log"),
                    &settings,
                )?;
                create_owner(&node_b).await?;
                node_b
            }
        };
        pg.insert(Node::A, node_a);
        pg.insert(Node::B, node_b);

        // --- links -------------------------------------------------------
        if config.topology == Topology::SingleWriter {
            let writer = Relay::open("writer-b-to-a", pg[&Node::A].port()).await?;
            relays.insert(writer.name().to_owned(), writer);
        }
        if config.topology == Topology::DualWriter {
            for node in Node::BOTH {
                let name = format!("peer-{}-to-{}", node.label(), node.peer().label());
                let relay = Relay::open(&name, pg[&node.peer()].port()).await?;
                relays.insert(name, relay);
            }
        }

        // --- frontends ---------------------------------------------------
        let mut config = config;
        // One token for both nodes, as the pair shares the Hashbalancer's.
        let token = format!("dual-sim-{}", uuid::Uuid::new_v4().simple());
        if config.readiness == Readiness::Readyz {
            config.balancer.check_path = "/readyz".into();
            config.balancer.require_ok = false;
            config.balancer.check_token = Some(token.clone());
        }
        let mut gates = BTreeMap::new();
        if config.rpc_gates {
            for node in Node::BOTH {
                gates.insert(
                    node,
                    RpcGate::open(chain.node(qbitd_of(node)).rpc_port()).await?,
                );
            }
        }
        let mut frontends = BTreeMap::new();
        for node in Node::BOTH {
            let stratum_port = crate::postgres::free_port()?;
            let api_port = crate::postgres::free_port()?;
            let readiness = (config.readiness == Readiness::Readyz)
                .then(|| Ok::<_, anyhow::Error>((crate::postgres::free_port()?, token.clone())))
                .transpose()?;
            let database_url = match (config.topology, node) {
                (Topology::SingleWriter, Node::B) => format!(
                    "postgresql://{OWNER_ROLE}@127.0.0.1:{}/{DATABASE}",
                    relays["writer-b-to-a"].port()
                ),
                _ => pg[&node].url(OWNER_ROLE, DATABASE),
            };
            let dual = (config.topology == Topology::DualWriter).then(|| DualSettings {
                node_index: node.index() as u8,
                carry_owner: node == Node::A,
                peer_database_url: format!(
                    "postgresql://{PEER_ROLE}@127.0.0.1:{}/{DATABASE}",
                    relays[&format!("peer-{}-to-{}", node.label(), node.peer().label())].port()
                ),
                peer_database_url_fallback: None,
                sync_interval_ms: config.sync_interval_ms,
                sync_batch_rows: config.sync_batch_rows,
            });
            let spec = FrontendSpec {
                node,
                server_bin: inputs.server.clone(),
                database_url,
                qbitd_rpc_port: gates
                    .get(&node)
                    .map_or_else(|| chain.node(qbitd_of(node)).rpc_port(), RpcGate::port),
                stratum_port,
                api_port,
                share_difficulty: config.load.share_difficulty,
                settlement: config.settlement,
                dual,
                stratum_max_connections: 512,
                readiness: readiness.clone(),
                overrides: config.overrides.get(&node).cloned().unwrap_or_default(),
            };
            let public_stratum =
                Relay::open(&format!("public-stratum-{}", node.label()), stratum_port).await?;
            // The balancer's checks reach the readiness listener when there
            // is one, else `/healthz` on the operator listener.
            let health_port = readiness.as_ref().map_or(api_port, |(port, _)| *port);
            let public_health =
                Relay::open(&format!("public-health-{}", node.label()), health_port).await?;
            relays.insert(public_stratum.name().to_owned(), public_stratum);
            relays.insert(public_health.name().to_owned(), public_health);
            frontends.insert(node, Frontend::new(spec, &logs)?);
        }
        let links = Links { relays };

        let ready_limit = Duration::from_secs(180);
        let dual_since = (config.topology == Topology::DualWriter).then(chrono::Utc::now);
        if config.topology == Topology::DualWriter {
            // The bootstrap a fresh pair runs (CONTRACT.md D-9): migrate
            // each database, personalise it as its node, then let the peer
            // role read it. B starts before A, as the cutover does (D-4), so
            // A's guard reads B's seeded journal on its first look.
            for node in [Node::B, Node::A] {
                bootstrap_node(&frontends[&node], &pg[&node]).await?;
            }
            for node in [Node::B, Node::A] {
                frontends.get_mut(&node).context("frontend")?.start()?;
            }
        } else {
            // A migrates its database first; in the 3.0 pair B then finds
            // the schema in place.
            frontends.get_mut(&Node::A).context("frontend a")?.start()?;
            frontends[&Node::A].wait_ready(ready_limit).await?;
            frontends.get_mut(&Node::B).context("frontend b")?.start()?;
        }
        for node in Node::BOTH {
            frontends[&node].wait_ready(ready_limit).await?;
        }

        // --- balancer and load ------------------------------------------
        let backends = Node::BOTH
            .iter()
            .map(|node| {
                Ok(BackendTarget {
                    name: node.label().to_owned(),
                    stratum_port: links
                        .get(&format!("public-stratum-{}", node.label()))?
                        .port(),
                    health_port: links
                        .get(&format!("public-health-{}", node.label()))?
                        .port(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let balancer = Balancer::start(config.balancer.clone(), backends, clock.started).await?;
        for node in Node::BOTH {
            balancer
                .wait_state(node.label(), true, Duration::from_secs(30))
                .await?;
        }
        let finders: Vec<(Node, u16)> = Node::BOTH
            .iter()
            .map(|node| {
                Ok((
                    *node,
                    links
                        .get(&format!("public-stratum-{}", node.label()))?
                        .port(),
                ))
            })
            .collect::<Result<_>>()?;
        let load = Load::start(&config.load, balancer.port(), &finders, clock)?;
        let sessions: usize = config.load.accounts.iter().map(|a| a.sessions).sum();
        load.wait_holding(sessions, Duration::from_secs(60)).await?;

        let sim = Self {
            config,
            inputs,
            clock,
            start_height,
            root,
            report_dir,
            logs,
            chain,
            pg,
            frontends,
            links,
            balancer,
            load: Some(load),
            gates,
            timeline: Mutex::new(Vec::new()),
            backups: BTreeMap::new(),
            dual_since,
        };
        sim.mark(&format!(
            "started: {:?} pair, {} sessions through the balancer",
            sim.config.topology, sessions
        ));
        Ok(sim)
    }

    /// Record an event on the run's timeline.
    pub fn mark(&self, event: &str) {
        let at_ms = self.clock.now_ms();
        if let Ok(mut timeline) = self.timeline.lock() {
            timeline.push(TimelineEvent {
                at_ms,
                event: event.to_owned(),
            });
        }
    }

    pub fn timeline(&self) -> Vec<TimelineEvent> {
        self.timeline.lock().map(|t| t.clone()).unwrap_or_default()
    }

    pub fn load(&self) -> Result<&Load> {
        self.load.as_ref().context("the load has stopped")
    }

    pub fn frontend(&self, node: Node) -> &Frontend {
        &self.frontends[&node]
    }

    pub fn frontend_mut(&mut self, node: Node) -> &mut Frontend {
        self.frontends.get_mut(&node).expect("both frontends exist")
    }

    /// The database a node's own frontend writes: its own, or in the 3.0
    /// pair A's for both.
    pub fn ledger_node(&self, node: Node) -> Node {
        match self.config.topology {
            Topology::SingleWriter => Node::A,
            _ => node,
        }
    }

    pub fn pg_mut(&mut self, node: Node) -> &mut PgNode {
        self.pg.get_mut(&node).expect("both databases exist")
    }

    /// The databases the invariants are checked in: the 3.0 pair's one
    /// writer, otherwise both.
    pub fn ledgers(&self) -> Vec<Node> {
        crate::invariants::ledger_nodes(self)
    }

    /// Wait until `node` serves work on its own `qbitd`'s tip at its
    /// database's payout revision, with no block candidate of its ledger on
    /// its way to the chain: the state in which a block it finds is built on
    /// current work (`Fixture::settled` in `live_regtest.rs`).
    pub async fn wait_node_settled(&self, node: Node, limit: Duration) -> Result<()> {
        let pool = self.pool(self.ledger_node(node)).await?;
        let started = Instant::now();
        let mut last = String::new();
        let result = loop {
            // In flight, or a reconciliation attempt claimed or due (the
            // live fixtures' quiesce rule): a released attempt scheduled for
            // later is bookkeeping, and waiting for it can take minutes.
            let unfinished: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM qbit_block_candidate_outbox \
                 WHERE state IN ('pending', 'offer_reserved', 'offered') \
                    OR (state = 'reconciliation' AND (claim_expires_at > clock_timestamp() \
                        OR next_attempt_at <= clock_timestamp()))",
            )
            .fetch_one(&pool)
            .await?;
            let revision: i64 = sqlx::query_scalar(
                "SELECT payout_revision FROM qbit_prism_cluster WHERE singleton",
            )
            .fetch_one(&pool)
            .await?;
            let tip = self.chain.node(qbitd_of(node)).best().await?;
            if let Ok((_, health)) = self.frontend(node).health().await {
                if unfinished == 0
                    && health["ok"] == true
                    && health["observed_tip"] == tip.as_str()
                    && health["payout_state_generation"] == revision
                {
                    break Ok(());
                }
                last = format!(
                    "{unfinished} unfinished candidates; health ok {}, observed tip {}, generation {} (database revision {revision}, node tip {tip})",
                    health["ok"], health["observed_tip"], health["payout_state_generation"]
                );
            }
            if started.elapsed() > limit {
                break Err(anyhow::anyhow!(
                    "node {node:?} did not settle within {limit:?}: {last}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        pool.close().await;
        result
    }

    /// Whether `hash` became a block candidate (or a landed block) in
    /// `node`'s ledger within `limit`.
    pub async fn wait_candidate(&self, node: Node, hash: &str, limit: Duration) -> Result<bool> {
        let pool = self.pool(self.ledger_node(node)).await?;
        let started = Instant::now();
        let found = loop {
            let known: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM qbit_block_candidate_outbox WHERE block_hash = $1) \
                     OR EXISTS (SELECT 1 FROM qbit_pool_blocks WHERE block_hash = $1)",
            )
            .bind(hash)
            .fetch_one(&pool)
            .await?;
            if known || started.elapsed() > limit {
                break known;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        pool.close().await;
        Ok(found)
    }

    /// A block candidate's row, for a failure message.
    pub async fn candidate_state(&self, node: Node, hash: &str) -> String {
        let Ok(pool) = self.pool(node).await else {
            return "database unreachable".into();
        };
        let row: Option<(String, Option<String>, Option<String>, i32)> = sqlx::query_as(
            "SELECT state, offer_outcome, last_error, attempt_count \
             FROM qbit_block_candidate_outbox WHERE block_hash = $1",
        )
        .bind(hash)
        .fetch_optional(&pool)
        .await
        .unwrap_or(None);
        pool.close().await;
        match row {
            Some((state, outcome, error, attempts)) => format!(
                "candidate {state}, outcome {outcome:?}, {attempts} attempts, last error {error:?}"
            ),
            None => "no candidate row".into(),
        }
    }

    /// Wait until `hash` is landed and confirmed in each of `nodes`'
    /// databases. Returns how long it took.
    pub async fn wait_confirmed(
        &self,
        hash: &str,
        nodes: &[Node],
        limit: Duration,
    ) -> Result<Duration> {
        let started = Instant::now();
        for node in nodes {
            let pool = self.pool(*node).await?;
            loop {
                let state: Option<String> = sqlx::query_scalar(
                    "SELECT chain_state FROM qbit_pool_blocks WHERE block_hash = $1",
                )
                .bind(hash)
                .fetch_optional(&pool)
                .await?;
                if state.as_deref() == Some("confirmed") {
                    break;
                }
                if started.elapsed() > limit {
                    pool.close().await;
                    let candidate = self.candidate_state(*node, hash).await;
                    bail!(
                        "block {hash} is {state:?} on node {node:?}, not confirmed, after {limit:?} ({candidate})"
                    );
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            pool.close().await;
        }
        Ok(started.elapsed())
    }

    /// Bring the run to rest for the checker: stop offering shares and wait
    /// for every answer, let every node's chain converge, wait until no
    /// database holds an unfinished block candidate, and until the
    /// databases agree (the 3.0 standby has replayed the primary; in the 3.1
    /// pair both hold the same shares and blocks).
    pub async fn settle(&self, limit: Duration) -> Result<()> {
        let started = Instant::now();
        let load = self.load()?;
        load.pause();
        load.wait_answered(Duration::from_secs(40)).await?;
        self.chain.converged().await?;
        for node in self.ledgers() {
            let pool = self.pool(node).await?;
            loop {
                let unfinished: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM qbit_block_candidate_outbox \
                     WHERE state IN ('pending', 'offer_reserved', 'offered') \
                        OR (state = 'reconciliation' AND (claim_expires_at > clock_timestamp() \
                            OR next_attempt_at <= clock_timestamp()))",
                )
                .fetch_one(&pool)
                .await?;
                if unfinished == 0 {
                    break;
                }
                ensure!(
                    started.elapsed() < limit,
                    "node {node:?} still holds {unfinished} unfinished block candidates after {limit:?}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            pool.close().await;
        }
        match self.config.topology {
            Topology::SingleWriter => {
                self.wait_standby_replayed(limit - started.elapsed().min(limit))
                    .await?
            }
            Topology::DualWriter => {
                self.wait_converged(limit - started.elapsed().min(limit))
                    .await?
            }
            Topology::Unsynced => {}
        }
        self.mark("settled");
        Ok(())
    }

    /// Wait until the 3.0 standby has replayed everything the primary had
    /// written when the wait began.
    pub async fn wait_standby_replayed(&self, limit: Duration) -> Result<()> {
        let primary = self.pg[&Node::A].admin_pool("postgres").await?;
        let standby = self.pg[&Node::B].admin_pool("postgres").await?;
        let target: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
            .fetch_one(&primary)
            .await?;
        let started = Instant::now();
        loop {
            let caught_up: bool =
                sqlx::query_scalar("SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn")
                    .bind(&target)
                    .fetch_one(&standby)
                    .await?;
            if caught_up {
                break;
            }
            ensure!(
                started.elapsed() < limit,
                "the standby did not replay to {target} within {limit:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        primary.close().await;
        standby.close().await;
        Ok(())
    }

    /// Wait until the two databases hold the same shares and blocks, and
    /// still do a second later: with the load paused (`settle`), the pair's
    /// end state.
    pub async fn wait_converged(&self, limit: Duration) -> Result<()> {
        let started = Instant::now();
        let mut last = None;
        loop {
            let mut reading = Vec::new();
            for node in Node::BOTH {
                let pool = self.pool(node).await?;
                let counts: (i64, i64) = sqlx::query_as(
                    "SELECT (SELECT count(*) FROM qbit_prism_share_hashes), \
                            (SELECT count(*) FROM qbit_pool_blocks)",
                )
                .fetch_one(&pool)
                .await?;
                pool.close().await;
                reading.push(counts);
            }
            if reading[0] == reading[1] && last.as_ref() == Some(&reading) {
                return Ok(());
            }
            ensure!(
                started.elapsed() < limit,
                "the databases did not converge within {limit:?}: (shares, blocks) A {:?}, B {:?}",
                reading[0],
                reading[1]
            );
            last = Some(reading);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Wait until each node holds every share and block its peer had
    /// originated when this was called: the sync has caught up to now. Under
    /// load the two databases are never equal at one instant, so this waits
    /// for the first reading's rows to arrive, not for equal counts. It says
    /// nothing about a node's own rows (a restored node pulling its own rows
    /// back is S6's and S7's check). Connecting and every read are capped by
    /// the time left, and reads are retried until the bound; a timeout names
    /// each node's sync conflicts, since a row the peer refuses never
    /// arrives.
    pub async fn wait_synced(&self, limit: Duration) -> Result<()> {
        let started = Instant::now();
        let deadline = tokio::time::Instant::now() + limit;
        // A pool's close waits for every connection it lent, and a read
        // cancelled mid-query leaves one pinging a stalled database: cap it
        // too, and let a pool that will not close go.
        async fn close_all(pools: BTreeMap<Node, PgPool>) {
            for pool in pools.into_values() {
                let _ = tokio::time::timeout(Duration::from_secs(5), pool.close()).await;
            }
        }
        let mut pools = BTreeMap::new();
        for node in Node::BOTH {
            match tokio::time::timeout_at(deadline, self.pool(node)).await {
                Ok(Ok(pool)) => {
                    pools.insert(node, pool);
                }
                Ok(Err(error)) => {
                    close_all(pools).await;
                    return Err(error);
                }
                Err(_) => {
                    close_all(pools).await;
                    anyhow::bail!(
                        "connecting to the two databases outlived the {limit:?} bound (still \
                         connecting to node {node:?})"
                    );
                }
            }
        }
        // One read, capped by what is left of the bound.
        async fn capped<T>(
            deadline: tokio::time::Instant,
            read: impl std::future::Future<Output = Result<T>>,
        ) -> Result<T> {
            tokio::time::timeout_at(deadline, read)
                .await
                .map_err(|_| anyhow::anyhow!("the read outlived the bound"))?
        }
        let mut since = BTreeMap::new();
        let mut target: BTreeMap<Node, (i64, i64)> = BTreeMap::new();
        let mut last = String::new();
        let result = loop {
            // What each node had originated, as its own database holds it,
            // and the database's clock, read once each.
            for node in Node::BOTH {
                if let std::collections::btree_map::Entry::Vacant(slot) = since.entry(node) {
                    match capped(deadline, db_clock(&pools[&node])).await {
                        Ok(at) => {
                            slot.insert((at, started.elapsed()));
                        }
                        Err(error) => last = format!("reading node {node:?}'s clock: {error:#}"),
                    }
                }
                if let std::collections::btree_map::Entry::Vacant(slot) = target.entry(node) {
                    match capped(deadline, origin_counts(&pools[&node], node)).await {
                        Ok(counts) => {
                            slot.insert(counts);
                        }
                        Err(error) => last = format!("reading node {node:?}'s own rows: {error:#}"),
                    }
                }
            }
            if target.len() == 2 {
                let mut behind = Vec::new();
                for node in Node::BOTH {
                    let peer = node.peer();
                    match capped(deadline, origin_counts(&pools[&peer], node)).await {
                        Ok(held) if held.0 >= target[&node].0 && held.1 >= target[&node].1 => {}
                        Ok(held) => behind.push(format!(
                            "node {peer:?} holds {held:?} of node {node:?}'s (shares, blocks), \
                             which had {:?}",
                            target[&node]
                        )),
                        Err(error) => behind.push(format!("reading node {peer:?}: {error:#}")),
                    }
                }
                if behind.is_empty() {
                    break Ok(());
                }
                last = behind.join("; ");
            }
            if started.elapsed() >= limit {
                let mut conflicts = Vec::new();
                for (node, pool) in &pools {
                    // Every conflict, and those seen during the wait: a row
                    // refused before it stays missing just the same. Without
                    // the database's clock at the start, the second is unread.
                    let (since, late) = match since.get(node) {
                        Some((at, read_after)) => (Some(*at), *read_after),
                        None => (None, Duration::ZERO),
                    };
                    let count = tokio::time::timeout(
                        Duration::from_secs(5),
                        sqlx::query_as::<_, (i64, Option<i64>)>(
                            "SELECT count(*), count(*) FILTER (WHERE last_seen_at >= $1) \
                                                  + CASE WHEN $1 IS NULL THEN NULL ELSE 0 END \
                             FROM qbit_prism_peer_sync_conflicts",
                        )
                        .bind(since)
                        .fetch_one(pool),
                    )
                    .await;
                    conflicts.push(match count {
                        Ok(Ok((all, Some(recent)))) if late < Duration::from_secs(2) => {
                            format!("node {node:?}: {all} ({recent} seen during the wait)")
                        }
                        Ok(Ok((all, Some(recent)))) => format!(
                            "node {node:?}: {all} ({recent} seen since its clock was first read, \
                             {:.0} s into the wait)",
                            late.as_secs_f64()
                        ),
                        Ok(Ok((all, None))) => {
                            format!("node {node:?}: {all} (during the wait: unread)")
                        }
                        Ok(Err(error)) => format!("node {node:?}: unread ({error})"),
                        Err(_) => format!("node {node:?}: unread (timed out)"),
                    });
                }
                break Err(anyhow::anyhow!(
                    "the sync did not catch up within {limit:?}: {last}; sync conflicts {}",
                    conflicts.join(", ")
                ));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
        close_all(pools).await;
        result
    }

    /// The 3.0 to 3.1 cutover (S9), as the cutover playbook runs it: drain
    /// both frontends, let B's standby replay everything, promote it into an
    /// independent primary, link the two databases both ways, point B's
    /// frontend at its own database, bootstrap both (migrate, node identity,
    /// peer grants; D-9), and start B, then A, in dual mode (D-4), checked
    /// by `/readyz` from then on (D-7). Returns the chain height at the
    /// cutover: every pool block above it is the dual-writer pair's.
    pub async fn cutover_to_dual(&mut self) -> Result<u64> {
        ensure!(
            self.config.topology == Topology::SingleWriter,
            "a cutover starts from the 3.0 pair"
        );
        let load = self.load()?;
        load.pause();
        load.wait_answered(Duration::from_secs(40)).await?;
        for node in [Node::B, Node::A] {
            self.frontend_mut(node).stop(Duration::from_secs(30))?;
        }
        self.mark("drained: both frontends stopped");
        // The drain's precondition: no candidate that can still be offered.
        // After the promotion both databases hold every candidate, so one
        // left unfinished could be offered by both nodes (D-2).
        let pool = self.pool(Node::A).await?;
        let (offerable, bookkeeping): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE state IN ('pending', 'offer_reserved', 'offered')), \
                    count(*) FILTER (WHERE state = 'reconciliation') \
             FROM qbit_block_candidate_outbox",
        )
        .fetch_one(&pool)
        .await?;
        pool.close().await;
        ensure!(
            offerable == 0,
            "the drain left {offerable} candidates that could still be offered; the cutover must not start"
        );
        self.mark(&format!(
            "no candidate can still be offered; {bookkeeping} landed blocks await their reconciliation bookkeeping"
        ));
        self.wait_standby_replayed(Duration::from_secs(60)).await?;
        self.pg[&Node::B].promote().await?;
        self.mark("B's standby promoted into an independent primary");
        for name in ["writer-b-to-a", "replication-b-from-a"] {
            self.links.relays.remove(name);
        }
        for node in Node::BOTH {
            let name = format!("peer-{}-to-{}", node.label(), node.peer().label());
            let relay = Relay::open(&name, self.pg[&node.peer()].port()).await?;
            self.links.relays.insert(name, relay);
        }
        let token = format!("dual-sim-{}", uuid::Uuid::new_v4().simple());
        for node in Node::BOTH {
            let readiness_port = crate::postgres::free_port()?;
            let peer_link = self
                .links
                .get(&format!("peer-{}-to-{}", node.label(), node.peer().label()))?
                .port();
            let database_url = self.pg[&node].url(OWNER_ROLE, DATABASE);
            let (interval, batch) = (self.config.sync_interval_ms, self.config.sync_batch_rows);
            let spec = &mut self.frontend_mut(node).spec;
            spec.database_url = database_url;
            spec.dual = Some(DualSettings {
                node_index: node.index() as u8,
                carry_owner: node == Node::A,
                peer_database_url: format!(
                    "postgresql://{PEER_ROLE}@127.0.0.1:{peer_link}/{DATABASE}"
                ),
                peer_database_url_fallback: None,
                sync_interval_ms: interval,
                sync_batch_rows: batch,
            });
            spec.readiness = Some((readiness_port, token.clone()));
            self.links
                .get(&format!("public-health-{}", node.label()))?
                .retarget(readiness_port);
        }
        self.balancer.set_probe(crate::balancer::Probe {
            path: "/readyz".into(),
            require_ok: false,
            token: Some(token),
        });
        self.config.topology = Topology::DualWriter;
        self.config.readiness = Readiness::Readyz;
        self.dual_since = Some(chrono::Utc::now());
        for node in [Node::B, Node::A] {
            bootstrap_node(&self.frontends[&node], &self.pg[&node]).await?;
        }
        for node in [Node::B, Node::A] {
            self.frontend_mut(node).start()?;
        }
        for node in Node::BOTH {
            self.frontend(node)
                .wait_ready(Duration::from_secs(180))
                .await?;
        }
        let (height, _, _) = self.chain.c.tip().await?;
        self.mark(&format!("cut over to dual mode at height {height}"));
        Ok(height)
    }

    /// A superuser pool on `node`'s PRISM database.
    pub async fn pool(&self, node: Node) -> Result<PgPool> {
        PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(20))
            .connect(&self.pg[&node].admin_url(DATABASE))
            .await
            .with_context(|| format!("connecting to node {node:?}'s ledger"))
    }

    /// Take a base backup of `node`'s database now, kept as `name`.
    pub fn base_backup(&mut self, node: Node, name: &str) -> Result<()> {
        let dest = self.root.join("backups").join(name);
        std::fs::create_dir_all(dest.parent().context("backup dir")?)?;
        self.pg[&node].base_backup(&dest)?;
        self.backups.insert(name.to_owned(), dest);
        self.mark(&format!("base backup {name} of node {node:?}"));
        Ok(())
    }

    /// Stop the load, every frontend, the balancer's sessions and the chain,
    /// and remove the data directories unless asked to keep them.
    pub async fn shutdown(mut self, keep: bool) -> Result<()> {
        if let Some(mut load) = self.load.take() {
            let _ = load.stop().await;
        }
        for frontend in self.frontends.values_mut() {
            let _ = frontend.stop(Duration::from_secs(10));
        }
        for node in self.pg.values_mut() {
            let _ = node.stop_fast();
        }
        self.chain.stop().await;
        if !keep && !self.inputs.keep {
            let _ = std::fs::remove_dir_all(&self.root);
        }
        Ok(())
    }

    /// The ports and paths a person reproducing a failure needs.
    pub fn describe(&self) -> serde_json::Value {
        serde_json::json!({
            "root": self.root,
            "report_dir": self.report_dir,
            "postgres": self.pg.iter().map(|(node, pg)| (node.label(), pg.port())).collect::<BTreeMap<_, _>>(),
            "links": self.links.names(),
            "balancer_port": self.balancer.port(),
        })
    }
}

/// A database's clock now.
async fn db_clock(pool: &PgPool) -> Result<chrono::DateTime<chrono::Utc>> {
    Ok(sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await?)
}

/// How many shares and blocks `origin` originated, as `pool`'s database
/// holds them.
async fn origin_counts(pool: &PgPool, origin: Node) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT (SELECT count(*) FROM qbit_prism_share_hashes WHERE origin_node = $1), \
                (SELECT count(*) FROM qbit_pool_blocks WHERE origin_node = $1)",
    )
    .bind(origin.index() as i16)
    .fetch_one(pool)
    .await?)
}

/// The `qbitd` a node's frontend uses: its own host's.
pub fn qbitd_of(node: Node) -> chain::NodeName {
    match node {
        Node::A => chain::NodeName::A,
        Node::B => chain::NodeName::B,
    }
}

async fn create_owner(node: &PgNode) -> Result<()> {
    let pool = node.admin_pool("postgres").await?;
    sqlx::query(&format!("CREATE ROLE {OWNER_ROLE} LOGIN"))
        .execute(&pool)
        .await?;
    sqlx::query(&format!("CREATE DATABASE {DATABASE} OWNER {OWNER_ROLE}"))
        .execute(&pool)
        .await?;
    // The peer role as D1 specifies it for the pair (status/D1.md, "the
    // read-only peer role"), less the password: the harness's loopback
    // logins are `trust`.
    for statement in [
        format!(
            "CREATE ROLE {PEER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION \
             NOBYPASSRLS NOINHERIT CONNECTION LIMIT 4"
        ),
        format!("ALTER ROLE {PEER_ROLE} SET default_transaction_read_only = on"),
        format!("ALTER ROLE {PEER_ROLE} SET statement_timeout = '30s'"),
        format!("ALTER ROLE {PEER_ROLE} SET lock_timeout = '5s'"),
        format!("ALTER ROLE {PEER_ROLE} SET idle_in_transaction_session_timeout = '60s'"),
    ] {
        sqlx::query(&statement).execute(&pool).await?;
    }
    pool.close().await;
    Ok(())
}

/// The peer role's grants on a migrated database, exactly D1's list, so a
/// pull that needs more than D1 documents for D5 fails here first.
pub const PEER_ROLE_GRANTS: &[&str] = &[
    "GRANT CONNECT ON DATABASE prism TO prism_peer_sync",
    "GRANT USAGE ON SCHEMA public TO prism_peer_sync",
    "GRANT SELECT ON qbit_share_ledger, qbit_prism_share_hashes, qbit_pool_blocks, \
     qbit_prism_audit_snapshots, qbit_pool_audit_bundles, qbit_pool_payout_entries, \
     qbit_payout_carry_forward, qbit_ctv_fanout_sets, qbit_ctv_fanout_artifacts, \
     qbit_prism_templates, qbit_prism_balance_snapshots, qbit_prism_jobs, \
     qbit_prism_node_roles TO prism_peer_sync",
    "GRANT SELECT ON qbit_prism_node_identity, qbit_prism_node_lineage, \
     qbit_prism_peer_sync_cursors TO prism_peer_sync",
    "GRANT SELECT (config_fingerprint) ON qbit_prism_cluster TO prism_peer_sync",
    "GRANT SELECT ON SEQUENCE qbit_prism_sync_seq, qbit_share_ledger_share_seq_seq \
     TO prism_peer_sync",
];

/// Bring a fresh database to the dual-writer pair's starting state, as the
/// bootstrap does: `qbit-prism-server migrate`, then `node-identity set`
/// (D-9), then the peer role's grants. Each command runs with the node's
/// own frontend environment.
pub async fn bootstrap_node(frontend: &Frontend, pg: &PgNode) -> Result<()> {
    let node = frontend.node();
    let index = if node == Node::A { "0" } else { "1" };
    for args in [
        vec!["migrate"],
        vec!["node-identity", "set", "--index", index],
    ] {
        let run = frontend.tool(&args, Duration::from_secs(300)).await?;
        ensure!(
            run.success,
            "qbit-prism-server {} on node {node:?} failed ({:?}): {} {}",
            args.join(" "),
            run.code,
            run.stdout.trim(),
            run.stderr.trim()
        );
    }
    let pool = pg.admin_pool(DATABASE).await?;
    for statement in PEER_ROLE_GRANTS {
        sqlx::query(statement)
            .execute(&pool)
            .await
            .with_context(|| format!("node {node:?}: {statement}"))?;
    }
    pool.close().await;
    Ok(())
}

/// The binaries the harness runs, built into this workspace's target
/// directory by a nested `cargo build` exactly as `qbit-prism-load`'s tests
/// build the server (`crates/qbit-prism-load/tests/faults.rs`): the server
/// and the audit verifier.
pub fn build_binaries(manifest_dir: &Path, profile_dir: &Path) -> Result<(PathBuf, PathBuf)> {
    let target = profile_dir
        .parent()
        .context("profile directory has no parent")?;
    let mut command =
        std::process::Command::new(std::env::var_os("CARGO").unwrap_or("cargo".into()));
    for (name, _) in std::env::vars_os() {
        let name = name.to_string_lossy().into_owned();
        if [
            "CARGO_PKG_",
            "CARGO_MANIFEST_",
            "CARGO_CRATE_",
            "CARGO_BIN_",
            "CARGO_PRIMARY_PACKAGE",
            "CARGO_TARGET_TMPDIR",
        ]
        .iter()
        .any(|prefix| name.starts_with(prefix))
        {
            command.env_remove(name);
        }
    }
    let release = profile_dir
        .file_name()
        .is_some_and(|name| name == "release");
    command.args([
        "build",
        "--locked",
        "-p",
        "qbit-prism-server",
        "--bin",
        "qbit-prism-server",
        "-p",
        "qbit-prism",
        "--bin",
        "qbit-prism-audit-verify",
    ]);
    if release {
        command.arg("--release");
    }
    let status = command
        .env("CARGO_TARGET_DIR", target)
        .current_dir(manifest_dir)
        .status()
        .context("running cargo build for the server and the verifier")?;
    ensure!(
        status.success(),
        "cargo build of the server and the verifier: {status}"
    );
    let server = profile_dir.join("qbit-prism-server");
    let verifier = profile_dir.join("qbit-prism-audit-verify");
    for binary in [&server, &verifier] {
        if !binary.exists() {
            bail!("{} was not built", binary.display());
        }
    }
    Ok((server, profile_dir.to_owned()))
}
