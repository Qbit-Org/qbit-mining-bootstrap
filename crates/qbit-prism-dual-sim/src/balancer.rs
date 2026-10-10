//! The balancer stand-in: a scripted TCP router in front of both nodes'
//! Stratum listeners, shaped like the Hashbalancer's HAProxy configuration
//! for the pair (CONTRACT.md §1 "Routing", D4's qbit-tools change).
//!
//! - **Health:** an HTTP check against each node every `check_interval`,
//!   through the same public link as its Stratum traffic, so a cut host
//!   fails its checks. A node is marked down after `fall` failed checks in a
//!   row and up after `rise` good ones (HAProxy's `inter`, `fall`, `rise`).
//!   A check passes on HTTP 200, and with `require_ok` also on a JSON body
//!   whose `ok` is true.
//! - **Routing:** a new session goes to the first node that is up, in
//!   preference order: A, then B as the backup. With neither up it is
//!   refused (reset), and the miner retries, as against a real balancer.
//! - **Mark-down:** every session on a node marked down is closed at once
//!   (`on-marked-down shutdown-sessions`), so its miners reconnect to the
//!   other node instead of waiting on a dead socket.
//! - **Failback:** when A comes back up, new sessions go to A. Sessions on B
//!   stay there ([`Failback::Sticky`]), are all closed at once
//!   (`on-marked-up shutdown-backup-sessions`), or are moved at a bounded
//!   rate ([`Failback::Paced`]), so A's startup is not hit by every miner at
//!   once.
//!
//! Every routing decision and state change is stamped on the run's clock
//! for the scenario report.

use anyhow::{Context, Result};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
};

/// What happens to the backup's sessions once the preferred node is back.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum Failback {
    Sticky,
    Immediate,
    Paced { sessions_per_second: f64 },
}

/// How a new session picks among the nodes that are up.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Routing {
    /// The first node up, in preference order: the pair's configuration.
    Preferred,
    /// Alternate between every node that is up: a misrouting balancer, or
    /// one whose backup flag was lost, which makes both nodes write at once
    /// (scenario S5).
    RoundRobin,
}

#[derive(Clone, Debug, Serialize)]
pub struct BalancerConfig {
    pub routing: Routing,
    pub check_interval: Duration,
    pub check_timeout: Duration,
    pub fall: u32,
    pub rise: u32,
    /// The check's path: `/readyz` (CONTRACT.md D-7) or `/healthz`.
    pub check_path: String,
    /// Also require a JSON body with `"ok": true` (`/healthz` only).
    pub require_ok: bool,
    /// The readiness token, sent as [`TOKEN_HEADER`]. Never serialized.
    #[serde(skip)]
    pub check_token: Option<String>,
    pub failback: Failback,
}

/// The Hashbalancer's health-token header (`HEALTH_HEADER`), which PRISM's
/// readiness endpoint requires (CONTRACT.md D-7).
pub const TOKEN_HEADER: &str = "X-Qbit-Healthcheck-Token";

impl Default for BalancerConfig {
    /// The checks the scenarios run with unless they say otherwise: the
    /// Hashbalancer's target shape for the pair (a check every 2 s with a 1 s
    /// timeout, down after three failures, up after two passes), so a dead
    /// node is out of rotation within 4 to 7 s. A frontend's `/healthz`
    /// answers 503 for about a second while it rebuilds work after every
    /// payout-revision bump (each landed block); faster checks eject healthy
    /// nodes on every block, both at once.
    fn default() -> Self {
        Self {
            routing: Routing::Preferred,
            check_interval: Duration::from_secs(2),
            check_timeout: Duration::from_millis(1_000),
            fall: 3,
            rise: 2,
            check_path: "/healthz".into(),
            require_ok: true,
            check_token: None,
            failback: Failback::Paced {
                sessions_per_second: 20.0,
            },
        }
    }
}

/// One node behind the balancer: where its sessions and checks go.
#[derive(Clone, Debug)]
pub struct BackendTarget {
    pub name: String,
    pub stratum_port: u16,
    pub health_port: u16,
}

#[derive(Clone, Debug, Serialize)]
pub struct BalancerEvent {
    pub at_ms: u64,
    pub event: String,
}

/// A node marked up or down by its checks.
#[derive(Clone, Debug, Serialize)]
pub struct Transition {
    pub at_ms: u64,
    pub backend: String,
    pub up: bool,
}

/// A check that failed, and whether its node was up (serving) then.
#[derive(Clone, Debug, Serialize)]
pub struct FailedCheck {
    pub at_ms: u64,
    pub backend: String,
    pub while_up: bool,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct BalancerReport {
    pub config: BalancerConfig,
    /// What happened, as text, for the scenario's timeline.
    pub events: Vec<BalancerEvent>,
    /// Every mark-up and mark-down, in order.
    pub transitions: Vec<Transition>,
    /// Every failed check, in order.
    pub failed_checks: Vec<FailedCheck>,
    /// Sessions routed to each node over the run.
    pub routed: BTreeMap<String, u64>,
    /// Sessions refused because no node was up, or reset because their node
    /// was marked down as they connected.
    pub refused: u64,
}

impl BalancerReport {
    /// Whether `backend` was up at `at_ms`: its last mark at or before then
    /// was up. Every node starts down.
    pub fn up_at(&self, backend: &str, at_ms: u64) -> bool {
        self.transitions
            .iter()
            .rev()
            .find(|t| t.backend == backend && t.at_ms <= at_ms)
            .is_some_and(|t| t.up)
    }

    /// Whether `backend` was up for the whole of `from_ms..=to_ms`: up at
    /// `from_ms`, and not marked down again until `to_ms`.
    pub fn up_throughout(&self, backend: &str, from_ms: u64, to_ms: u64) -> bool {
        self.up_at(backend, from_ms)
            && !self
                .transitions
                .iter()
                .any(|t| t.backend == backend && t.at_ms > from_ms && t.at_ms <= to_ms && !t.up)
    }

    /// How long after a fault's effect a mark-down can still land: `fall`
    /// failing checks, each taking up to the longer of the interval and the
    /// timeout, plus one interval for the streak to start.
    pub fn mark_down_horizon(config: &BalancerConfig) -> Duration {
        config.check_interval.max(config.check_timeout) * config.fall + config.check_interval
    }
}

struct Backend {
    target: BackendTarget,
    up: watch::Sender<bool>,
    /// Open sessions, by id, each with its closer.
    sessions: Mutex<BTreeMap<u64, watch::Sender<bool>>>,
    routed: AtomicU64,
}

struct Shared {
    config: BalancerConfig,
    /// What each check requests, which a cutover changes mid-run (the 3.0
    /// pair's `/healthz` to the 3.1 pair's `/readyz`).
    probe: std::sync::RwLock<Probe>,
    backends: Vec<Backend>,
    started: Instant,
    events: Mutex<Vec<BalancerEvent>>,
    transitions: Mutex<Vec<Transition>>,
    failed_checks: Mutex<Vec<FailedCheck>>,
    next_session: AtomicU64,
    refused: AtomicU64,
}

impl Shared {
    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn event(&self, event: String) {
        let at_ms = self.now_ms();
        if let Ok(mut events) = self.events.lock() {
            events.push(BalancerEvent { at_ms, event });
        }
    }

    fn transition(&self, backend: &str, up: bool) {
        let at_ms = self.now_ms();
        if let Ok(mut transitions) = self.transitions.lock() {
            transitions.push(Transition {
                at_ms,
                backend: backend.to_owned(),
                up,
            });
        }
    }

    fn failed_check(&self, backend: &str, while_up: bool, reason: &str) {
        let at_ms = self.now_ms();
        if let Ok(mut failed) = self.failed_checks.lock() {
            failed.push(FailedCheck {
                at_ms,
                backend: backend.to_owned(),
                while_up,
                reason: reason.to_owned(),
            });
        }
    }
}

/// The request a health check makes.
#[derive(Clone, Debug)]
pub struct Probe {
    pub path: String,
    pub require_ok: bool,
    pub token: Option<String>,
}

pub struct Balancer {
    port: u16,
    shared: Arc<Shared>,
    tasks: Vec<JoinHandle<()>>,
}

impl Balancer {
    /// Start routing to `backends`, in preference order. Each starts down
    /// and is marked up by its checks, so nothing is routed to a node that
    /// never passed one. `started` is the run's clock.
    pub async fn start(
        config: BalancerConfig,
        backends: Vec<BackendTarget>,
        started: Instant,
    ) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding the balancer")?;
        let port = listener.local_addr()?.port();
        let probe = std::sync::RwLock::new(Probe {
            path: config.check_path.clone(),
            require_ok: config.require_ok,
            token: config.check_token.clone(),
        });
        let shared = Arc::new(Shared {
            probe,
            config,
            backends: backends
                .into_iter()
                .map(|target| Backend {
                    target,
                    up: watch::channel(false).0,
                    sessions: Mutex::new(BTreeMap::new()),
                    routed: AtomicU64::new(0),
                })
                .collect(),
            started,
            events: Mutex::new(Vec::new()),
            transitions: Mutex::new(Vec::new()),
            failed_checks: Mutex::new(Vec::new()),
            next_session: AtomicU64::new(0),
            refused: AtomicU64::new(0),
        });
        let client = reqwest::Client::builder()
            .timeout(shared.config.check_timeout)
            .pool_max_idle_per_host(0)
            .build()?;
        let mut tasks = Vec::new();
        for index in 0..shared.backends.len() {
            tasks.push(tokio::spawn(check_loop(
                shared.clone(),
                index,
                client.clone(),
            )));
        }
        tasks.push(tokio::spawn(accept_loop(listener, shared.clone())));
        tasks.push(tokio::spawn(failback_loop(shared.clone())));
        Ok(Self {
            port,
            shared,
            tasks,
        })
    }

    /// The port miners connect to.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// A handle that checks `name` on demand exactly as the check loop does
    /// (path, token, timeout), for a scenario that must time a node's
    /// withdrawal finer than the check interval.
    pub fn node_check(&self, name: &str) -> Result<NodeCheck> {
        let backend = self
            .shared
            .backends
            .iter()
            .find(|backend| backend.target.name == name)
            .with_context(|| format!("the balancer has no node {name}"))?;
        let client = reqwest::Client::builder()
            .timeout(self.shared.config.check_timeout)
            .pool_max_idle_per_host(0)
            .build()?;
        Ok(NodeCheck {
            shared: self.shared.clone(),
            port: backend.target.health_port,
            client,
        })
    }

    pub fn is_up(&self, name: &str) -> bool {
        self.shared
            .backends
            .iter()
            .find(|backend| backend.target.name == name)
            .is_some_and(|backend| *backend.up.borrow())
    }

    /// Open sessions per node.
    pub fn sessions(&self) -> BTreeMap<String, usize> {
        self.shared
            .backends
            .iter()
            .map(|backend| {
                let open = backend.sessions.lock().map(|s| s.len()).unwrap_or(0);
                (backend.target.name.clone(), open)
            })
            .collect()
    }

    /// Wait until `name` is up (or down) as the checks see it.
    pub async fn wait_state(&self, name: &str, up: bool, limit: Duration) -> Result<()> {
        let backend = self
            .shared
            .backends
            .iter()
            .find(|backend| backend.target.name == name)
            .with_context(|| format!("no backend {name}"))?;
        let mut state = backend.up.subscribe();
        tokio::time::timeout(limit, state.wait_for(|state| *state == up))
            .await
            .with_context(|| {
                format!(
                    "the balancer did not mark {name} {} within {limit:?}",
                    if up { "up" } else { "down" }
                )
            })??;
        Ok(())
    }

    /// Change what every later check requests, and through which port of
    /// each node (`health_ports`, in backend order).
    pub fn set_probe(&self, probe: Probe) {
        if let Ok(mut current) = self.shared.probe.write() {
            *current = probe;
        }
        self.shared.event("checks reconfigured".into());
    }

    pub fn report(&self) -> BalancerReport {
        BalancerReport {
            config: self.shared.config.clone(),
            events: self
                .shared
                .events
                .lock()
                .map(|e| e.clone())
                .unwrap_or_default(),
            transitions: self
                .shared
                .transitions
                .lock()
                .map(|t| t.clone())
                .unwrap_or_default(),
            failed_checks: self
                .shared
                .failed_checks
                .lock()
                .map(|f| f.clone())
                .unwrap_or_default(),
            routed: self
                .shared
                .backends
                .iter()
                .map(|backend| {
                    (
                        backend.target.name.clone(),
                        backend.routed.load(Ordering::Relaxed),
                    )
                })
                .collect(),
            refused: self.shared.refused.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Balancer {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        for backend in &self.shared.backends {
            if let Ok(sessions) = backend.sessions.lock() {
                for closer in sessions.values() {
                    closer.send_replace(true);
                }
            }
        }
    }
}

/// One node's check, made on demand; see [`Balancer::node_check`].
#[derive(Clone)]
pub struct NodeCheck {
    shared: Arc<Shared>,
    port: u16,
    client: reqwest::Client,
}

impl NodeCheck {
    /// Whether the node answers ready now, with the reason when it does not.
    pub async fn check(&self) -> Result<(), String> {
        let probe = self
            .shared
            .probe
            .read()
            .map(|probe| probe.clone())
            .map_err(|_| "the probe's lock is poisoned".to_owned())?;
        check(&self.client, &probe, self.port).await
    }

    /// The run clock's time, as the balancer's records use it.
    pub fn now_ms(&self) -> u64 {
        self.shared.now_ms()
    }
}

/// One check: whether the node answered 200 (and `ok: true` when required),
/// with the reason when it did not.
async fn check(client: &reqwest::Client, probe: &Probe, port: u16) -> Result<(), String> {
    let url = format!("http://127.0.0.1:{port}{}", probe.path);
    let mut request = client.get(&url);
    if let Some(token) = &probe.token {
        request = request.header(TOKEN_HEADER, token);
    }
    let response = request.send().await.map_err(|error| {
        if error.is_timeout() {
            "timed out".to_owned()
        } else {
            "connection failed".to_owned()
        }
    })?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    if probe.require_ok {
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|_| "unreadable body".to_owned())?;
        if body["ok"] != true {
            return Err("ok is not true".to_owned());
        }
    }
    Ok(())
}

async fn check_loop(shared: Arc<Shared>, index: usize, client: reqwest::Client) {
    let backend = &shared.backends[index];
    let (mut passes, mut failures) = (0u32, 0u32);
    let mut last_reason = String::new();
    let mut ticker = tokio::time::interval(shared.config.check_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let probe = shared
            .probe
            .read()
            .map(|probe| probe.clone())
            .map_err(|_| ())
            .ok();
        let Some(probe) = probe else { return };
        let result = check(&client, &probe, backend.target.health_port).await;
        let up = *backend.up.borrow();
        match result {
            Ok(()) => {
                failures = 0;
                passes += 1;
                if !up && passes >= shared.config.rise {
                    // Recorded before the state is published, so whoever sees
                    // the node up finds the mark that made it so.
                    shared.transition(&backend.target.name, true);
                    backend.up.send_replace(true);
                    shared.event(format!(
                        "{} marked up after {passes} passing checks",
                        backend.target.name
                    ));
                }
            }
            Err(reason) => {
                passes = 0;
                failures += 1;
                shared.failed_check(&backend.target.name, up, &reason);
                if up && failures >= shared.config.fall {
                    shared.transition(&backend.target.name, false);
                    backend.up.send_replace(false);
                    let closed = close_all(backend);
                    shared.event(format!(
                        "{} marked down after {failures} failed checks ({reason}); closed {closed} sessions",
                        backend.target.name
                    ));
                } else if !up && reason != last_reason {
                    shared.event(format!("{} check failing: {reason}", backend.target.name));
                }
                last_reason = reason;
            }
        }
    }
}

fn close_all(backend: &Backend) -> usize {
    let Ok(mut sessions) = backend.sessions.lock() else {
        return 0;
    };
    let count = sessions.len();
    for (_, closer) in std::mem::take(&mut *sessions) {
        closer.send_replace(true);
    }
    count
}

async fn accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    let mut turn = 0usize;
    while let Ok((client, _)) = listener.accept().await {
        let _ = client.set_nodelay(true);
        let up: Vec<usize> = shared
            .backends
            .iter()
            .enumerate()
            .filter(|(_, backend)| *backend.up.borrow())
            .map(|(index, _)| index)
            .collect();
        let chosen = match shared.config.routing {
            Routing::Preferred => up.first().copied(),
            Routing::RoundRobin if up.is_empty() => None,
            Routing::RoundRobin => {
                turn = turn.wrapping_add(1);
                Some(up[turn % up.len()])
            }
        };
        let Some(index) = chosen else {
            shared.refused.fetch_add(1, Ordering::Relaxed);
            crate::relay::reset(client);
            continue;
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            session(client, shared, index).await;
        });
    }
}

async fn session(client: TcpStream, shared: Arc<Shared>, index: usize) {
    let backend = &shared.backends[index];
    let Ok(upstream) = TcpStream::connect(("127.0.0.1", backend.target.stratum_port)).await else {
        crate::relay::reset(client);
        return;
    };
    let _ = upstream.set_nodelay(true);
    let id = shared.next_session.fetch_add(1, Ordering::Relaxed);
    let (closer, mut closed) = watch::channel(false);
    if let Ok(mut sessions) = backend.sessions.lock() {
        sessions.insert(id, closer);
    }
    // A session chosen just before its node's mark-down can register after
    // the mark-down closed every session: it ends as the mark-down would have
    // ended it, before a byte reaches the node, and is not counted as routed.
    if !*backend.up.borrow() {
        if let Ok(mut sessions) = backend.sessions.lock() {
            sessions.remove(&id);
        }
        crate::relay::reset(client);
        crate::relay::reset(upstream);
        shared.refused.fetch_add(1, Ordering::Relaxed);
        shared.event(format!(
            "a session to {} reset: its node was marked down as it connected",
            backend.target.name
        ));
        return;
    }
    backend.routed.fetch_add(1, Ordering::Relaxed);
    let (mut client, mut upstream) = (client, upstream);
    tokio::select! {
        _ = tokio::io::copy_bidirectional(&mut client, &mut upstream) => {}
        _ = closed.wait_for(|closed| *closed) => {
            // Closed by the balancer: a reset, as HAProxy's shutdown-sessions
            // aborts the connection.
            crate::relay::abort_on_close(&client);
            crate::relay::abort_on_close(&upstream);
        }
    }
    if let Ok(mut sessions) = backend.sessions.lock() {
        sessions.remove(&id);
    }
}

/// Move sessions off the backups once the preferred node is up again.
async fn failback_loop(shared: Arc<Shared>) {
    let Some(preferred) = shared.backends.first() else {
        return;
    };
    if shared.config.routing != Routing::Preferred {
        return;
    }
    let mut up = preferred.up.subscribe();
    loop {
        if up.wait_for(|up| *up).await.is_err() {
            return;
        }
        match shared.config.failback {
            Failback::Sticky => {}
            Failback::Immediate => {
                let mut moved = 0;
                for backend in &shared.backends[1..] {
                    moved += close_all(backend);
                }
                if moved > 0 {
                    shared.event(format!("failback: closed {moved} backup sessions at once"));
                }
            }
            Failback::Paced {
                sessions_per_second,
            } => {
                let gap = Duration::from_secs_f64(1.0 / sessions_per_second.max(0.001));
                let mut moved = 0;
                loop {
                    if !*preferred.up.borrow() {
                        break;
                    }
                    let next = shared.backends[1..].iter().find_map(|backend| {
                        let mut sessions = backend.sessions.lock().ok()?;
                        let id = *sessions.keys().next()?;
                        sessions.remove(&id)
                    });
                    let Some(closer) = next else {
                        break;
                    };
                    closer.send_replace(true);
                    moved += 1;
                    tokio::time::sleep(gap).await;
                }
                if moved > 0 {
                    shared.event(format!(
                        "failback: moved {moved} backup sessions at {sessions_per_second}/s"
                    ));
                }
            }
        }
        if up.wait_for(|up| !*up).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use std::sync::atomic::AtomicBool;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A node stand-in: an echo listener tagged with its name, and a health
    /// endpoint that answers as `healthy` says.
    async fn node(name: &'static str, healthy: Arc<AtomicBool>) -> Result<BackendTarget> {
        let stratum = TcpListener::bind("127.0.0.1:0").await?;
        let stratum_port = stratum.local_addr()?.port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = stratum.accept().await {
                tokio::spawn(async move {
                    let _ = stream.write_all(name.as_bytes()).await;
                    let mut buffer = [0u8; 64];
                    while let Ok(count) = stream.read(&mut buffer).await {
                        if count == 0 {
                            break;
                        }
                    }
                });
            }
        });
        let health = TcpListener::bind("127.0.0.1:0").await?;
        let health_port = health.local_addr()?.port();
        let app = Router::new().route(
            "/healthz",
            get(move || {
                let healthy = healthy.clone();
                async move {
                    if healthy.load(Ordering::SeqCst) {
                        (axum::http::StatusCode::OK, r#"{"ok":true}"#)
                    } else {
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            r#"{"ok":false}"#,
                        )
                    }
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(health, app).await;
        });
        Ok(BackendTarget {
            name: name.into(),
            stratum_port,
            health_port,
        })
    }

    async fn routed_to(port: u16) -> Result<(String, TcpStream)> {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
        let mut name = [0u8; 1];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut name)).await??;
        Ok((String::from_utf8_lossy(&name).into_owned(), stream))
    }

    #[test]
    fn up_throughout_needs_an_up_mark_before_the_span_and_no_down_mark_inside_it() {
        let marks = |marks: &[(u64, &str, bool)]| BalancerReport {
            config: BalancerConfig::default(),
            events: Vec::new(),
            transitions: marks
                .iter()
                .map(|&(at_ms, backend, up)| Transition {
                    at_ms,
                    backend: backend.into(),
                    up,
                })
                .collect(),
            failed_checks: Vec::new(),
            routed: BTreeMap::new(),
            refused: 0,
        };
        let report = marks(&[
            (100, "a", true),
            (500, "a", false),
            (900, "a", true),
            (50, "b", true),
        ]);
        assert!(report.up_throughout("a", 100, 499));
        assert!(
            !report.up_throughout("a", 100, 500),
            "marked down inside the span"
        );
        assert!(
            !report.up_throughout("a", 600, 800),
            "down when the span starts"
        );
        assert!(report.up_throughout("a", 900, 5_000));
        assert!(
            !report.up_throughout("a", 50, 200),
            "not yet up when the span starts"
        );
        assert!(report.up_throughout("b", 50, 5_000));
        assert!(!report.up_throughout("c", 0, 1), "never marked up");
        assert!(report.up_at("a", 499) && !report.up_at("a", 500) && report.up_at("a", 900));
        let slow = BalancerConfig {
            check_interval: Duration::from_secs(1),
            check_timeout: Duration::from_secs(3),
            ..BalancerConfig::default()
        };
        assert_eq!(
            BalancerReport::mark_down_horizon(&slow),
            Duration::from_secs(10)
        );
    }

    #[tokio::test]
    async fn sessions_prefer_a_move_to_b_when_a_fails_and_fail_back_at_the_paced_rate() -> Result<()>
    {
        let (a_up, b_up) = (
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
        );
        let config = BalancerConfig {
            check_interval: Duration::from_millis(50),
            failback: Failback::Paced {
                sessions_per_second: 50.0,
            },
            ..BalancerConfig::default()
        };
        let balancer = Balancer::start(
            config,
            vec![
                node("a", a_up.clone()).await?,
                node("b", b_up.clone()).await?,
            ],
            Instant::now(),
        )
        .await?;
        balancer
            .wait_state("a", true, Duration::from_secs(5))
            .await?;
        balancer
            .wait_state("b", true, Duration::from_secs(5))
            .await?;
        let (first, mut on_a) = routed_to(balancer.port()).await?;
        assert_eq!(first, "a", "A is preferred");

        a_up.store(false, Ordering::SeqCst);
        balancer
            .wait_state("a", false, Duration::from_secs(5))
            .await?;
        let mut buffer = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), on_a.read(&mut buffer)).await?;
        assert!(matches!(read, Ok(0) | Err(_)), "A's session was shut down");
        let (moved, _on_b) = routed_to(balancer.port()).await?;
        assert_eq!(moved, "b", "a new session lands on the backup");

        a_up.store(true, Ordering::SeqCst);
        balancer
            .wait_state("a", true, Duration::from_secs(5))
            .await?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while balancer.sessions()["b"] > 0 {
            assert!(
                Instant::now() < deadline,
                "the backup's session was never moved"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let (back, _) = routed_to(balancer.port()).await?;
        assert_eq!(back, "a");
        // The failback logs its batch once it finds no session left to move.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let report = balancer.report();
            assert!(report
                .events
                .iter()
                .any(|e| e.event.starts_with("a marked down")));
            if report
                .events
                .iter()
                .any(|e| e.event.starts_with("failback: moved 1"))
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "no failback event: {:?}",
                report.events
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // The same run, as structured records: A went up, down and up again,
        // and the checks that took it down failed while it was serving.
        let report = balancer.report();
        let a: Vec<bool> = report
            .transitions
            .iter()
            .filter(|t| t.backend == "a")
            .map(|t| t.up)
            .collect();
        assert_eq!(a, vec![true, false, true]);
        let serving_failures = report
            .failed_checks
            .iter()
            .filter(|f| f.backend == "a" && f.while_up)
            .count();
        // At least fall (3); a check timing out on a busy host adds more.
        assert!(serving_failures >= 3, "{:?}", report.failed_checks);
        Ok(())
    }

    #[tokio::test]
    async fn with_no_node_up_a_session_is_refused() -> Result<()> {
        let down = Arc::new(AtomicBool::new(false));
        let balancer = Balancer::start(
            BalancerConfig {
                check_interval: Duration::from_millis(50),
                ..BalancerConfig::default()
            },
            vec![
                node("a", down.clone()).await?,
                node("b", down.clone()).await?,
            ],
            Instant::now(),
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mut stream = TcpStream::connect(("127.0.0.1", balancer.port())).await?;
        let mut buffer = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buffer)).await?;
        assert!(matches!(read, Ok(0) | Err(_)));
        assert_eq!(balancer.report().refused, 1);
        Ok(())
    }
}
