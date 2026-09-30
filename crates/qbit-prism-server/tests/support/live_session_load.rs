//! Session load for the live suite, at #487 L4's session counts (#553).
//!
//! [`SessionLoad`] keeps `sessions` Stratum connections open against a
//! fixture's frontends, as a pool's miners would. Each session has a home
//! frontend and fails over to the next ready one when its connection closes,
//! and together they offer a fixed share rate. Every proof is a share that
//! misses its job's block target, so the load never finds a block and never
//! changes which own blocks a scenario has. The servers run with
//! [`load_server_env`]: #575's share-only settings, which make such a share
//! possible on regtest, room for every session, and the production reanchor
//! (#598). A load share
//! weighs about 2^-32 of a difficulty-1 share, so the payout window and every
//! scenario's economics are unchanged. What the load does exercise is the
//! share append under `ORDER_LOCK`, the database pool, and the notify fan-out
//! to every session on every tip.
//!
//! The load records what each session was told and what it was answered. It
//! stamps every tip of every watched node with a `waitfornewblock` long-poll
//! given the last tip, and it samples each frontend's `/healthz`.
//! [`LoadRecord::check`] then holds the run to three things:
//! - Every acknowledged share is a durable ledger row, no refused share is one,
//!   and every durable row was acknowledged or has an unknown answer.
//! - No session earns a refusal a correct share-only miner never earns: a
//!   duplicate, low-difficulty or malformed submit.
//! - A session on a frontend has work on each tip of the frontend's node
//!   within [`DELIVERY_BOUND`], provided the tip lasted that long, the node
//!   answered throughout, and the frontend reported itself ready when the
//!   bound expired. That is #413's condition, and the two-node scenarios'
//!   notify bound.
//!
//! It reports #481's time to usable work per frontend and per tip: from the
//! node's stamp to each session's first notify on that parent. A tip replaced
//! before a session was served is counted as replaced, never as zero.
use super::share_client::{solve_share, Answer, SHARE_ONLY_SETTINGS};
use super::*;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::Lines,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::watch,
    task::JoinHandle,
    time::MissedTickBehavior,
};

/// How long a session on a ready frontend may wait for work on a new tip:
/// the native current-work-gap warning's 15 s, as the two-node scenarios'
/// `NOTIFY_BOUND` (#413 held delivery for 307 s).
pub(super) const DELIVERY_BOUND: Duration = Duration::from_secs(15);

/// Connections a frontend keeps free beyond the load for a scenario's own
/// clients (its highdiff watcher and miners, a block client).
const SCENARIO_CONNECTIONS: usize = 100;

/// The server's default `PRISM_STRATUM_MAX_CONNECTIONS`, which a smaller
/// load keeps: the pending initial-job limit (128) may not exceed it.
const DEFAULT_MAX_CONNECTIONS: usize = 384;

/// A request's answer bound: the server's share commit timeout plus grace
/// (#577), as the share client's.
const ANSWER_BOUND: Duration = super::share_client::ANSWER;

/// Subscribe, authorize and first work: the server bounds session allocation
/// by its 30 s initial-job timeout (#562).
const HANDSHAKE_BOUND: Duration = Duration::from_secs(35);

/// How long [`SessionLoad::stop`] waits for answers still outstanding.
const DRAIN_BOUND: Duration = Duration::from_secs(30);

/// Refusals a session that submits only valid, distinct shares on work it
/// was given can never earn.
const IMPOSSIBLE_REASONS: [&str; 3] = ["duplicate-share", "low-difficulty", "malformed-submit"];

/// The payout-artifact reanchor a load run's frontends use: the server's,
/// the harness's and Compose's default, where the live fixture's is 1 s.
/// Under a 1 s reanchor, 2,000 sessions on one frontend get no work on a new
/// tip at all: each job build is superseded by the next publication before
/// it completes (#598).
const REANCHOR_SECONDS: u64 = 60;

/// The frontend settings a load run needs: #575's share-only settings, a
/// connection limit that holds every session on one frontend, since a
/// scenario that stops one frontend moves every session to the other, and
/// the production reanchor ([`REANCHOR_SECONDS`]).
pub(super) fn load_server_env(sessions: usize) -> Vec<(String, String)> {
    SHARE_ONLY_SETTINGS
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .chain([
            (
                "PRISM_STRATUM_MAX_CONNECTIONS".to_owned(),
                (sessions + SCENARIO_CONNECTIONS)
                    .max(DEFAULT_MAX_CONNECTIONS)
                    .to_string(),
            ),
            (
                "PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS".to_owned(),
                REANCHOR_SECONDS.to_string(),
            ),
        ])
        .collect()
}

/// What a load offers and where.
#[derive(Clone, Debug)]
pub(super) struct LoadPlan {
    pub name: String,
    pub sessions: usize,
    /// Shares a second, over every session together.
    pub rate: f64,
    /// The payout address of every load session; each session's worker is
    /// `load<index>`.
    pub address: String,
    /// Frontend `i`'s Stratum and HTTP ports.
    pub stratum: Vec<u16>,
    pub api: Vec<u16>,
    /// RPC ports of the watched nodes (`prismtest` credentials).
    pub nodes: Vec<u16>,
    /// Which watched node frontend `i` follows; `None` for none, whose tips
    /// are then not measured.
    pub node_of: Vec<Option<usize>>,
    pub seed: u64,
}

impl LoadPlan {
    /// `sessions` sessions offering `rate` shares a second through both of
    /// the fixture's frontends, paid to `address`, both frontends on the
    /// fixture's node, which is the only node watched.
    pub(super) fn on_fixture(
        fixture: &Fixture,
        name: &str,
        sessions: usize,
        rate: f64,
        address: String,
    ) -> Self {
        Self {
            name: name.to_owned(),
            sessions,
            rate,
            address,
            stratum: fixture.stratum.to_vec(),
            api: fixture.api.to_vec(),
            nodes: vec![fixture.rpc_port],
            node_of: vec![Some(0), Some(0)],
            seed: 0x0553_0000 ^ sessions as u64,
        }
    }

    fn username(&self, index: usize) -> String {
        format!("{}.load{index}", self.address)
    }
}

/// One tip a watched node took, when it took it.
#[derive(Clone, Debug)]
struct TipStamp {
    node: usize,
    hash: String,
    at: Instant,
}

/// One connection of one session.
#[derive(Clone, Debug)]
struct Connection {
    frontend: usize,
    /// Subscribed, authorized and holding work.
    ready_at: Instant,
    closed_at: Option<Instant>,
    /// Every parent the connection was given work on, when it first was;
    /// consecutive notifies on one parent are one entry.
    parents: Vec<(Instant, String)>,
}

#[derive(Clone, Debug)]
struct SubmitRecord {
    share_id: String,
    frontend: usize,
    answer: Answer,
}

#[derive(Debug, Default)]
struct SessionLog {
    connections: Vec<Connection>,
    submits: Vec<SubmitRecord>,
    connect_failures: usize,
    last_failure: Option<String>,
}

/// State every session and poller shares.
struct Shared {
    plan: LoadPlan,
    /// Each frontend's latest `/healthz` answer.
    ready: Vec<AtomicBool>,
    /// `(when, frontend, ready)` at each change, the first sample included.
    health: Mutex<Vec<(Instant, usize, bool)>>,
    /// Per frontend: the slowest `/healthz` answer and the timed-out samples.
    health_stats: Mutex<Vec<(Duration, usize)>>,
    tips: Mutex<Vec<TipStamp>>,
    /// `(node, from, to)`: spans in which a watched node did not answer.
    unreachable: Mutex<Vec<(usize, Instant, Instant)>>,
    /// Sessions holding work right now.
    connected: AtomicUsize,
}

impl Shared {
    /// The first ready frontend from `home` on, in failover order.
    fn pick(&self, home: usize) -> Option<usize> {
        let count = self.ready.len();
        (0..count)
            .map(|offset| (home + offset) % count)
            .find(|frontend| self.ready[*frontend].load(Ordering::SeqCst))
    }
}

/// A running load. [`SessionLoad::stop`] ends it and returns the record.
pub(super) struct SessionLoad {
    shared: Arc<Shared>,
    stop: watch::Sender<bool>,
    sessions: Vec<JoinHandle<SessionLog>>,
    pollers: Vec<JoinHandle<()>>,
    started: Instant,
}

impl SessionLoad {
    pub(super) fn start(plan: LoadPlan) -> Result<Self> {
        ensure!(
            plan.sessions > 0 && plan.rate.is_finite() && plan.rate > 0.0,
            "a load needs sessions and a positive rate: {plan:?}"
        );
        ensure!(
            plan.stratum.len() == plan.api.len() && plan.stratum.len() == plan.node_of.len(),
            "every frontend needs a Stratum port, an HTTP port and a node: {plan:?}"
        );
        let started = Instant::now();
        let shared = Arc::new(Shared {
            ready: plan.api.iter().map(|_| AtomicBool::new(false)).collect(),
            health: Mutex::new(Vec::new()),
            health_stats: Mutex::new(vec![(Duration::ZERO, 0); plan.api.len()]),
            tips: Mutex::new(Vec::new()),
            unreachable: Mutex::new(Vec::new()),
            connected: AtomicUsize::new(0),
            plan,
        });
        let (stop, stopped) = watch::channel(false);
        let client = reqwest::Client::builder().build()?;
        let mut pollers: Vec<JoinHandle<()>> = (0..shared.plan.api.len())
            .map(|frontend| {
                tokio::spawn(poll_health(
                    shared.clone(),
                    frontend,
                    client.clone(),
                    stopped.clone(),
                ))
            })
            .collect();
        for node in 0..shared.plan.nodes.len() {
            pollers.push(tokio::spawn(watch_node(
                shared.clone(),
                node,
                stopped.clone(),
            )));
        }
        let sessions = (0..shared.plan.sessions)
            .map(|index| tokio::spawn(run_session(shared.clone(), index, stopped.clone())))
            .collect();
        Ok(Self {
            shared,
            stop,
            sessions,
            pollers,
            started,
        })
    }

    /// Sessions holding work right now.
    pub(super) fn connected(&self) -> usize {
        self.shared.connected.load(Ordering::SeqCst)
    }

    /// Wait until at least `fraction` of the sessions hold work.
    pub(super) async fn until_connected(&self, fraction: f64, seconds: u64) -> Result<()> {
        let wanted = (self.shared.plan.sessions as f64 * fraction).ceil() as usize;
        until(
            &format!("{wanted} load sessions holding work"),
            seconds,
            || async { Ok(self.connected() >= wanted) },
        )
        .await
        .with_context(|| format!("{} of them hold work", self.connected()))
    }

    /// Stop offering shares, wait for outstanding answers, close every
    /// session and return what the load saw.
    pub(super) async fn stop(self) -> LoadRecord {
        let offered_seconds = self.started.elapsed().as_secs_f64();
        let _ = self.stop.send(true);
        let mut sessions = Vec::with_capacity(self.sessions.len());
        for handle in self.sessions {
            // A session task that panicked leaves an empty log, which the
            // check then reports as a session that never held work.
            sessions.push(handle.await.unwrap_or_default());
        }
        for poller in self.pollers {
            poller.abort();
            let _ = poller.await;
        }
        let ended = Instant::now();
        let shared = &self.shared;
        let health = shared.health.lock().expect("health").clone();
        let health_stats = shared.health_stats.lock().expect("health stats").clone();
        let tips = shared.tips.lock().expect("tips").clone();
        let unreachable = shared.unreachable.lock().expect("unreachable").clone();
        LoadRecord {
            plan: shared.plan.clone(),
            started: self.started,
            ended,
            offered_seconds,
            health,
            health_stats,
            tips,
            unreachable,
            sessions,
        }
    }
}

/// How long a `/healthz` answer may take before the sample counts as not
/// ready. A slow answer is reported (`healthz_max_ms`, `healthz_timeouts`).
const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);

/// Sample frontend `frontend`'s `/healthz` every 250 ms, on its own task so
/// one slow frontend does not delay the other's samples.
async fn poll_health(
    shared: Arc<Shared>,
    frontend: usize,
    client: reqwest::Client,
    mut stop: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let url = format!("http://127.0.0.1:{}/healthz", shared.plan.api[frontend]);
    let mut last: Option<bool> = None;
    loop {
        tokio::select! {
            _ = stop.changed() => return,
            _ = ticker.tick() => {}
        }
        let asked = Instant::now();
        let answer = client.get(&url).timeout(HEALTH_TIMEOUT).send().await;
        let elapsed = asked.elapsed();
        let ready = match &answer {
            Ok(response) => {
                let mut stats = shared.health_stats.lock().expect("health stats");
                stats[frontend].0 = stats[frontend].0.max(elapsed);
                response.status().is_success()
            }
            Err(error) => {
                if error.is_timeout() {
                    shared.health_stats.lock().expect("health stats")[frontend].1 += 1;
                }
                false
            }
        };
        shared.ready[frontend].store(ready, Ordering::SeqCst);
        if last != Some(ready) {
            last = Some(ready);
            shared
                .health
                .lock()
                .expect("health")
                .push((asked, frontend, ready));
        }
    }
}

/// A watcher call slower than this, or failed, marks its node unreachable
/// for the call's span.
const NODE_ANSWER_BOUND: Duration = Duration::from_secs(3);

/// Stamp every tip change on watched node `node`. `waitfornewblock` is
/// given the tip last seen, so a change between two calls wakes the next
/// one at once. A stopped or restarting node is retried, not an error: the
/// scenario owns the node's life.
async fn watch_node(shared: Arc<Shared>, node: usize, mut stop: watch::Receiver<bool>) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(6))
        .build()
        .expect("watcher client");
    let url = format!("http://127.0.0.1:{}/", shared.plan.nodes[node]);
    let mut last: Option<String> = None;
    loop {
        let (method, params) = match &last {
            Some(tip) => ("waitfornewblock", json!([1000, tip])),
            None => ("getbestblockhash", json!([])),
        };
        let asked = Instant::now();
        let call = client
            .post(&url)
            .basic_auth("prismtest", Some("prismtest"))
            .json(&json!({"jsonrpc":"1.0","id":"load-watch","method":method,"params":params}))
            .send();
        let answer = tokio::select! {
            _ = stop.changed() => return,
            answer = call => answer,
        };
        let at = Instant::now();
        let payload: Option<Value> = match answer {
            Ok(response) => response.json().await.ok(),
            Err(_) => None,
        };
        let hash = payload.and_then(|payload| {
            let result = &payload["result"];
            result["hash"]
                .as_str()
                .or_else(|| result.as_str())
                .map(str::to_owned)
        });
        // A long-poll answers within its one second unless the node is
        // stopped, killed or restarting: then the node could not have given
        // the frontends work either.
        if hash.is_none() || at - asked > NODE_ANSWER_BOUND {
            shared
                .unreachable
                .lock()
                .expect("unreachable")
                .push((node, asked, at));
        }
        match hash {
            Some(hash) if last.as_deref() != Some(hash.as_str()) => {
                shared.tips.lock().expect("tips").push(TipStamp {
                    node,
                    hash: hash.clone(),
                    at,
                });
                last = Some(hash);
            }
            Some(_) => {}
            None => {
                tokio::select! {
                    _ = stop.changed() => return,
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                }
            }
        }
    }
}

/// A subscribed, authorized connection holding work.
struct Live {
    lines: Lines<BufReader<OwnedReadHalf>>,
    writer: OwnedWriteHalf,
    frontend: usize,
    extranonce1: String,
    extranonce2_size: usize,
    difficulty: f64,
    notify: Value,
    counter: u64,
    next_id: u64,
    /// The request id and submit index of the share awaiting its answer.
    outstanding: Option<(u64, usize, Instant)>,
}

/// The display-order hash of a notify's previous-block field, which Stratum
/// sends with each 32-bit word byte-swapped.
fn notify_parent(notify: &Value) -> Option<String> {
    let mut wire = hex::decode(notify["params"][1].as_str()?).ok()?;
    if wire.len() != 32 {
        return None;
    }
    for word in wire.as_chunks_mut::<4>().0 {
        word.reverse();
    }
    wire.reverse();
    Some(hex::encode(wire))
}

async fn handshake(port: u16, frontend: usize, username: &str) -> Result<(Live, Connection)> {
    let stream = tokio::time::timeout(
        Duration::from_secs(10),
        TcpStream::connect(("127.0.0.1", port)),
    )
    .await
    .context("Stratum connect timed out")??;
    stream.set_nodelay(true)?;
    let (read, mut writer) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    writer
        .write_all(
            format!(
                "{}\n{}\n",
                json!({"id":1,"method":"mining.subscribe","params":["prism-live-load"]}),
                json!({"id":2,"method":"mining.authorize","params":[username,"x"]})
            )
            .as_bytes(),
        )
        .await?;
    let deadline = Instant::now() + HANDSHAKE_BOUND;
    let (mut extranonce1, mut extranonce2_size) = (None, None);
    let (mut authorized, mut difficulty, mut notify) = (false, None, None);
    while !(authorized && difficulty.is_some() && notify.is_some() && extranonce1.is_some()) {
        let left = deadline.saturating_duration_since(Instant::now());
        ensure!(!left.is_zero(), "no work within {HANDSHAKE_BOUND:?}");
        let line = tokio::time::timeout(left, lines.next_line())
            .await
            .context("handshake timed out")??
            .context("closed during the handshake")?;
        let message: Value = serde_json::from_str(&line)?;
        match (message["id"].as_u64(), message["method"].as_str()) {
            (Some(1), None) => {
                ensure!(message["error"].is_null(), "subscribe: {message}");
                extranonce1 = message["result"][1].as_str().map(str::to_owned);
                extranonce2_size = message["result"][2].as_u64();
            }
            (Some(2), None) => {
                ensure!(message["result"] == true, "authorize: {message}");
                authorized = true;
            }
            (_, Some("mining.set_difficulty")) => difficulty = message["params"][0].as_f64(),
            (_, Some("mining.notify")) => notify = Some(message),
            _ => {}
        }
    }
    let ready_at = Instant::now();
    let notify = notify.expect("checked above");
    let parent = notify_parent(&notify).context("notify parent malformed")?;
    Ok((
        Live {
            lines,
            writer,
            frontend,
            extranonce1: extranonce1.expect("checked above"),
            extranonce2_size: usize::try_from(extranonce2_size.context("extranonce2 size")?)?,
            difficulty: difficulty.expect("checked above"),
            notify,
            counter: 0,
            next_id: 10,
            outstanding: None,
        },
        Connection {
            frontend,
            ready_at,
            closed_at: None,
            parents: vec![(ready_at, parent)],
        },
    ))
}

/// One session: connect to the first ready frontend from its home, offer a
/// share each interval while it holds work, answer nothing twice, and
/// reconnect when the server closes it.
async fn run_session(
    shared: Arc<Shared>,
    index: usize,
    mut stop: watch::Receiver<bool>,
) -> SessionLog {
    let plan = &shared.plan;
    let mut log = SessionLog::default();
    let username = plan.username(index);
    let home = index % plan.stratum.len();
    let interval = Duration::from_secs_f64(plan.sessions as f64 / plan.rate);
    let mut rng = StdRng::seed_from_u64(plan.seed ^ index as u64);
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + interval.mul_f64(rng.gen::<f64>()),
        interval,
    );
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Spread the first connections, so the frontends' initial-job admission
    // (128 pending by default) is not the thing measured.
    let spread = Duration::from_millis(u64::try_from(index).unwrap_or(0) * 5 % 10_000);
    tokio::select! {
        _ = stop.changed() => return log,
        _ = tokio::time::sleep(spread) => {}
    }
    let mut live: Option<Live> = None;
    let mut stopping: Option<Instant> = None;
    loop {
        // A dropped sender (the test failed before stopping the load) stops
        // the session too.
        if stopping.is_none() && (*stop.borrow() || stop.has_changed().is_err()) {
            stopping = Some(Instant::now());
        }
        if let Some(since) = stopping {
            let drained = live.as_ref().is_none_or(|live| live.outstanding.is_none());
            if drained || since.elapsed() > DRAIN_BOUND {
                break;
            }
        }
        let Some(current) = live.as_mut() else {
            if stopping.is_some() {
                break;
            }
            match shared.pick(home) {
                None => {
                    tokio::select! {
                        _ = stop.changed() => {}
                        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                    }
                }
                Some(frontend) => {
                    let port = plan.stratum[frontend];
                    let attempt = tokio::select! {
                        _ = stop.changed() => None,
                        attempt = handshake(port, frontend, &username) => Some(attempt),
                    };
                    match attempt {
                        None => {}
                        Some(Ok((connection, record))) => {
                            log.connections.push(record);
                            shared.connected.fetch_add(1, Ordering::SeqCst);
                            live = Some(connection);
                        }
                        Some(Err(error)) => {
                            log.connect_failures += 1;
                            log.last_failure = Some(format!("frontend {frontend}: {error:#}"));
                            tokio::select! {
                                _ = stop.changed() => {}
                                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                            }
                        }
                    }
                }
            }
            continue;
        };
        let wait = if stopping.is_some() {
            DRAIN_BOUND
        } else {
            Duration::from_secs(3600)
        };
        let event = tokio::select! {
            _ = stop.changed(), if stopping.is_none() => Event::Stop,
            _ = ticker.tick(), if stopping.is_none() => Event::Tick,
            line = tokio::time::timeout(wait, current.lines.next_line()) => match line {
                Ok(Ok(Some(line))) => Event::Line(line),
                Ok(Ok(None)) => Event::Closed("closed by the server".into()),
                Ok(Err(error)) => Event::Closed(format!("read: {error}")),
                Err(_) => Event::Stop,
            },
        };
        let failure = match event {
            Event::Stop => None,
            Event::Tick => {
                if let Some((_, submit, sent)) = current.outstanding {
                    if sent.elapsed() > ANSWER_BOUND {
                        log.submits[submit].answer = Answer::TimedOut;
                        current.outstanding = None;
                    }
                }
                if current.outstanding.is_none() {
                    submit(current, &username, &mut log)
                        .await
                        .err()
                        .map(|error| format!("submit: {error:#}"))
                } else {
                    None
                }
            }
            Event::Line(line) => receive(current, &line, &mut log)
                .err()
                .map(|error| format!("{error:#}")),
            Event::Closed(why) => Some(why),
        };
        if let Some(why) = failure {
            close(&shared, &mut live, &mut log, &why);
        }
    }
    if live.is_some() {
        close(&shared, &mut live, &mut log, "load stopped");
    }
    log
}

enum Event {
    Stop,
    Tick,
    Line(String),
    Closed(String),
}

/// Solve the latest job for a share that is not a block and send it.
async fn submit(live: &mut Live, username: &str, log: &mut SessionLog) -> Result<()> {
    live.counter += 1;
    let solution = solve_share(
        &live.notify,
        &live.extranonce1,
        live.extranonce2_size,
        live.difficulty,
        live.counter,
    )?;
    live.next_id += 1;
    let id = live.next_id;
    let request = json!({"id":id,"method":"mining.submit","params":[
        username, live.notify["params"][0], solution.extranonce2, solution.ntime, solution.nonce,
    ]});
    let sent = Instant::now();
    log.submits.push(SubmitRecord {
        share_id: format!("{username}:{}", solution.hash),
        frontend: live.frontend,
        answer: Answer::Lost("never written".into()),
    });
    let submit = log.submits.len() - 1;
    live.writer
        .write_all(format!("{request}\n").as_bytes())
        .await?;
    log.submits[submit].answer = Answer::TimedOut;
    live.outstanding = Some((id, submit, sent));
    Ok(())
}

fn receive(live: &mut Live, line: &str, log: &mut SessionLog) -> Result<()> {
    let message: Value = serde_json::from_str(line)?;
    match message["method"].as_str() {
        Some("mining.notify") => {
            let parent = notify_parent(&message).context("notify parent malformed")?;
            let connection = log.connections.last_mut().context("no connection")?;
            if connection.parents.last().map(|(_, known)| known) != Some(&parent) {
                connection.parents.push((Instant::now(), parent));
            }
            live.notify = message;
        }
        Some("mining.set_difficulty") => {
            live.difficulty = message["params"][0]
                .as_f64()
                .context("difficulty missing")?;
        }
        Some(_) => {}
        None => {
            if let Some((id, submit, _)) = live.outstanding {
                if message["id"].as_u64() == Some(id) {
                    log.submits[submit].answer =
                        if message["result"] == true && message["error"].is_null() {
                            Answer::Accepted
                        } else {
                            Answer::Rejected(message)
                        };
                    live.outstanding = None;
                }
            }
        }
    }
    Ok(())
}

fn close(shared: &Shared, live: &mut Option<Live>, log: &mut SessionLog, why: &str) {
    let Some(connection) = live.take() else {
        return;
    };
    if let Some((_, submit, _)) = connection.outstanding {
        log.submits[submit].answer = Answer::Lost(why.to_owned());
    }
    if let Some(record) = log.connections.last_mut() {
        record.closed_at = Some(Instant::now());
    }
    shared.connected.fetch_sub(1, Ordering::SeqCst);
}

/// Everything a stopped load saw.
pub(super) struct LoadRecord {
    plan: LoadPlan,
    started: Instant,
    ended: Instant,
    offered_seconds: f64,
    health: Vec<(Instant, usize, bool)>,
    health_stats: Vec<(Duration, usize)>,
    tips: Vec<TipStamp>,
    unreachable: Vec<(usize, Instant, Instant)>,
    sessions: Vec<SessionLog>,
}

/// One tip as one frontend's sessions received it.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct Delivery {
    pub frontend: usize,
    pub node: usize,
    pub hash: String,
    /// Seconds from the load's start to the node's stamp.
    pub stamped: f64,
    /// Seconds until the node's next tip; `None` if it lasted to the end.
    pub lifetime: Option<f64>,
    /// The frontend's node answered from the stamp through the bound after
    /// it, and the frontend reported itself ready when the bound expired.
    pub gated: bool,
    /// Sessions holding work on this frontend at the stamp.
    pub eligible: usize,
    pub served: usize,
    /// Served before the watcher's stamp, counted as zero seconds.
    pub ahead_of_stamp: usize,
    /// Not served before the node's next tip replaced this one.
    pub replaced: usize,
    /// Closed before being served, or the load ended first.
    pub gone: usize,
    /// Not served within the bound although the tip lasted: a violation
    /// when `gated`.
    pub late: usize,
    /// Time to usable work of each served session, seconds, sorted.
    pub seconds: Vec<f64>,
}

impl Delivery {
    fn quantile(&self, quantile: f64) -> Option<f64> {
        let last = self.seconds.len().checked_sub(1)?;
        Some(self.seconds[((last as f64) * quantile).round() as usize])
    }

    /// When every eligible session had work, the time the last one got it.
    fn all_sessions(&self) -> Option<f64> {
        (self.eligible > 0 && self.served == self.eligible)
            .then(|| self.seconds.last().copied())
            .flatten()
    }

    fn json(&self) -> Value {
        json!({
            "frontend": self.frontend,
            "node": self.node,
            "tip": self.hash,
            "stamped_s": round(self.stamped),
            "lifetime_s": self.lifetime.map(round),
            "gated": self.gated,
            "eligible": self.eligible,
            "served": self.served,
            "ahead_of_stamp": self.ahead_of_stamp,
            "replaced": self.replaced,
            "gone": self.gone,
            "late": self.late,
            "p50_s": self.quantile(0.5).map(round),
            "p99_s": self.quantile(0.99).map(round),
            "max_s": self.seconds.last().copied().map(round),
            "all_sessions_s": self.all_sessions().map(round),
        })
    }
}

fn round(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

/// How the load's submissions ended and what the ledger holds.
#[derive(Debug, Default, PartialEq)]
pub(super) struct ShareCounts {
    pub submitted: usize,
    pub accepted: usize,
    pub rejected: BTreeMap<String, usize>,
    /// Timed out or lost with the connection: the server may hold either.
    pub unknown: usize,
    pub unknown_durable: usize,
}

/// Hold `submits` to the durable ledger rows `durable`: every accepted
/// share is durable, no refused share is, and a durable share was accepted
/// or has an unknown answer.
fn reconcile(submits: &[&SubmitRecord], durable: &HashSet<String>) -> Result<ShareCounts> {
    let mut counts = ShareCounts {
        submitted: submits.len(),
        ..ShareCounts::default()
    };
    let mut answered: HashMap<&str, &Answer> = HashMap::new();
    for record in submits {
        ensure!(
            answered
                .insert(record.share_id.as_str(), &record.answer)
                .is_none(),
            "share {} was submitted twice",
            record.share_id
        );
        match &record.answer {
            Answer::Accepted => {
                counts.accepted += 1;
                ensure!(
                    durable.contains(&record.share_id),
                    "acknowledged share {} is not in the ledger",
                    record.share_id
                );
            }
            Answer::Rejected(_) => {
                let reason = record.answer.reason_id().unwrap_or("(no reason_id)");
                *counts.rejected.entry(reason.to_owned()).or_default() += 1;
                ensure!(
                    !durable.contains(&record.share_id),
                    "refused share {} ({reason}) is in the ledger",
                    record.share_id
                );
            }
            Answer::TimedOut | Answer::Lost(_) => {
                counts.unknown += 1;
                counts.unknown_durable += usize::from(durable.contains(&record.share_id));
            }
        }
    }
    for share in durable {
        ensure!(
            answered.contains_key(share.as_str()),
            "ledger share {share} was never submitted by the load"
        );
    }
    Ok(counts)
}

impl LoadRecord {
    fn seconds(&self, at: Instant) -> f64 {
        at.saturating_duration_since(self.started).as_secs_f64()
    }

    fn node_of(&self, frontend: usize) -> Option<usize> {
        self.plan.node_of.get(frontend).copied().flatten()
    }

    /// Frontend `frontend`'s last `/healthz` sample at or before `at` was
    /// ready. A frontend fences its health for the moments between a new tip
    /// and work on it (`observed_tip_advance_fences_old_prepared_jobs_and_health`),
    /// so readiness is read when the bound expires, not throughout it.
    fn ready_at(&self, frontend: usize, at: Instant) -> bool {
        self.health
            .iter()
            .rev()
            .find(|(sampled, index, _)| *index == frontend && *sampled <= at)
            .is_some_and(|(_, _, ready)| *ready)
    }

    /// The intervals, in seconds from the start, in which `frontend` did not
    /// answer `/healthz` with success; an open one ends with the load.
    fn unready(&self, frontend: usize) -> Vec<[f64; 2]> {
        let mut intervals = Vec::new();
        let mut since: Option<Instant> = None;
        for (at, index, ready) in &self.health {
            if *index != frontend {
                continue;
            }
            match (ready, since) {
                (false, None) => since = Some(*at),
                (true, Some(from)) => {
                    intervals.push([round(self.seconds(from)), round(self.seconds(*at))]);
                    since = None;
                }
                _ => {}
            }
        }
        if let Some(from) = since {
            intervals.push([round(self.seconds(from)), round(self.seconds(self.ended))]);
        }
        intervals
    }

    /// Watched node `node` answered its watcher throughout `from`..`to`.
    fn reachable_through(&self, node: usize, from: Instant, to: Instant) -> bool {
        !self
            .unreachable
            .iter()
            .any(|(index, start, end)| *index == node && *start < to && *end > from)
    }

    /// Every tip each frontend's node took, as the frontend's sessions got it.
    pub(super) fn deliveries(&self) -> Vec<Delivery> {
        let mut deliveries = Vec::new();
        for frontend in 0..self.plan.stratum.len() {
            let connections: Vec<&Connection> = self
                .sessions
                .iter()
                .flat_map(|session| &session.connections)
                .filter(|connection| connection.frontend == frontend)
                .collect();
            for (position, stamp) in self.tips.iter().enumerate() {
                if self.node_of(frontend) != Some(stamp.node) {
                    continue;
                }
                let previous = self.tips[..position]
                    .iter()
                    .rev()
                    .find(|earlier| earlier.node == stamp.node)
                    .map(|earlier| earlier.at);
                let next = self.tips[position + 1..]
                    .iter()
                    .find(|later| later.node == stamp.node)
                    .map(|later| later.at);
                let bound_end = stamp.at + DELIVERY_BOUND;
                let mut delivery = Delivery {
                    frontend,
                    node: stamp.node,
                    hash: stamp.hash.clone(),
                    stamped: self.seconds(stamp.at),
                    lifetime: next.map(|next| (next - stamp.at).as_secs_f64()),
                    gated: bound_end <= self.ended
                        && self.ready_at(frontend, bound_end)
                        && self.reachable_through(stamp.node, stamp.at, bound_end),
                    ..Delivery::default()
                };
                for connection in &connections {
                    if connection.ready_at > stamp.at
                        || connection
                            .closed_at
                            .is_some_and(|closed| closed <= stamp.at)
                    {
                        continue;
                    }
                    delivery.eligible += 1;
                    // Work on this parent given after the node's previous tip:
                    // an earlier notify on the same parent was for an earlier
                    // time this block was the tip.
                    let served = connection.parents.iter().find(|(at, parent)| {
                        parent == &stamp.hash && previous.is_none_or(|previous| *at > previous)
                    });
                    match served {
                        Some((at, _)) if next.is_none_or(|next| *at <= next) => {
                            if *at < stamp.at {
                                delivery.ahead_of_stamp += 1;
                            }
                            let seconds = at.saturating_duration_since(stamp.at).as_secs_f64();
                            delivery.served += 1;
                            delivery.seconds.push(seconds);
                            if seconds > DELIVERY_BOUND.as_secs_f64() {
                                delivery.late += 1;
                            }
                        }
                        _ => {
                            let deadline = next.map_or(bound_end, |next| next.min(bound_end));
                            if next.is_some_and(|next| next < bound_end) {
                                delivery.replaced += 1;
                            } else if connection.closed_at.is_some_and(|closed| closed < deadline)
                                || self.ended < deadline
                            {
                                delivery.gone += 1;
                            } else {
                                delivery.late += 1;
                            }
                        }
                    }
                }
                delivery.seconds.sort_by(f64::total_cmp);
                deliveries.push(delivery);
            }
        }
        deliveries
    }

    /// Hold the load to its checks and return its report, which is also
    /// printed as one `live-load <name>: {json}` line.
    pub(super) async fn check(&self, fixture: &Fixture) -> Result<Value> {
        let prefix = format!("{}.load", self.plan.address);
        let durable: HashSet<String> = sqlx::query_scalar(
            "SELECT share_id FROM qbit_share_ledger WHERE accepted AND starts_with(share_id,$1)",
        )
        .bind(&prefix)
        .fetch_all(&fixture.pool)
        .await?
        .into_iter()
        .collect();
        let report = self.report(&durable);
        eprintln!("live-load {}: {report}", self.plan.name);
        self.verdict(&durable)?;
        Ok(report)
    }

    fn submits(&self) -> Vec<&SubmitRecord> {
        self.sessions
            .iter()
            .flat_map(|session| &session.submits)
            .collect()
    }

    /// The checks, on what the ledger holds.
    fn verdict(&self, durable: &HashSet<String>) -> Result<()> {
        let submits = self.submits();
        let counts = reconcile(&submits, durable)?;
        let impossible: Vec<String> = IMPOSSIBLE_REASONS
            .iter()
            .filter_map(|reason| {
                counts
                    .rejected
                    .get(*reason)
                    .map(|count| format!("{count} {reason}"))
            })
            .collect();
        ensure!(
            impossible.is_empty(),
            "the load earned refusals a correct miner never earns: {}",
            impossible.join(", ")
        );
        ensure!(
            counts.accepted > 0,
            "no load share was acknowledged: {counts:?}"
        );
        let never = self
            .sessions
            .iter()
            .filter(|session| session.connections.is_empty())
            .count();
        ensure!(
            never == 0,
            "{never} of {} load sessions never held work; last failure: {:?}",
            self.sessions.len(),
            self.sessions
                .iter()
                .find_map(|session| session.last_failure.clone())
        );
        let late: Vec<String> = self
            .deliveries()
            .iter()
            .filter(|delivery| delivery.gated && delivery.late > 0)
            .map(|delivery| {
                format!(
                    "frontend {} tip {} at {:.1}s: {} of {} sessions without work within {DELIVERY_BOUND:?} (max served {:?}s)",
                    delivery.frontend,
                    delivery.hash,
                    delivery.stamped,
                    delivery.late,
                    delivery.eligible,
                    delivery.seconds.last()
                )
            })
            .collect();
        ensure!(
            late.is_empty(),
            "new-tip work did not reach every session on a ready frontend: {}",
            late.join("; ")
        );
        Ok(())
    }

    fn report(&self, durable: &HashSet<String>) -> Value {
        let submits = self.submits();
        let counts = reconcile(&submits, durable);
        let deliveries = self.deliveries();
        let mut frontends = Vec::new();
        for frontend in 0..self.plan.stratum.len() {
            let own: Vec<&Delivery> = deliveries
                .iter()
                .filter(|delivery| delivery.frontend == frontend && delivery.eligible > 0)
                .collect();
            let mut all: Vec<f64> = own
                .iter()
                .flat_map(|delivery| delivery.seconds.iter().copied())
                .collect();
            all.sort_by(f64::total_cmp);
            let pick = |quantile: f64| {
                all.len()
                    .checked_sub(1)
                    .map(|last| round(all[((last as f64) * quantile).round() as usize]))
            };
            frontends.push(json!({
                "frontend": frontend,
                "unready_s": self.unready(frontend),
                "healthz_max_ms": (self.health_stats[frontend].0.as_secs_f64() * 1000.0).round(),
                "healthz_timeouts": self.health_stats[frontend].1,
                "tips": own.len(),
                "gated_tips": own.iter().filter(|delivery| delivery.gated).count(),
                "session_deliveries": all.len(),
                "p50_s": pick(0.5),
                "p99_s": pick(0.99),
                "max_s": all.last().copied().map(round),
                "worst_all_sessions_s": own
                    .iter()
                    .filter_map(|delivery| delivery.all_sessions())
                    .max_by(f64::total_cmp)
                    .map(round),
                "replaced": own.iter().map(|delivery| delivery.replaced).sum::<usize>(),
                "late": own.iter().map(|delivery| delivery.late).sum::<usize>(),
                "submitted": submits.iter().filter(|record| record.frontend == frontend).count(),
            }));
        }
        let connections: usize = self
            .sessions
            .iter()
            .map(|session| session.connections.len())
            .sum();
        json!({
            "sessions": self.plan.sessions,
            "offered_rate": self.plan.rate,
            "seconds": round(self.offered_seconds),
            "offered": (self.plan.rate * self.offered_seconds).floor(),
            "shares": match &counts {
                Ok(counts) => json!({
                    "submitted": counts.submitted,
                    "accepted": counts.accepted,
                    "rejected": counts.rejected,
                    "unknown": counts.unknown,
                    "unknown_durable": counts.unknown_durable,
                    "durable": durable.len(),
                }),
                Err(error) => json!({"error": format!("{error:#}")}),
            },
            "node_unreachable_s": (0..self.plan.nodes.len())
                .map(|node| {
                    self.unreachable
                        .iter()
                        .filter(|(index, _, _)| *index == node)
                        .map(|(_, from, to)| [round(self.seconds(*from)), round(self.seconds(*to))])
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
            "connections": connections,
            "reconnects": connections.saturating_sub(self.plan.sessions),
            "connect_failures": self.sessions.iter().map(|session| session.connect_failures).sum::<usize>(),
            "delivery_bound_s": DELIVERY_BOUND.as_secs_f64(),
            "time_to_usable_work": frontends,
            "tips": deliveries
                .iter()
                .filter(|delivery| delivery.eligible > 0)
                .map(Delivery::json)
                .collect::<Vec<_>>(),
        })
    }
}

/// The scenario-level load settings: `PRISM_L4_SESSIONS` and
/// `PRISM_L4_SHARE_RATE`, or each case's defaults.
pub(super) fn load_settings(sessions: usize, rate: f64) -> Result<(usize, f64)> {
    fn read<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
    where
        T::Err: std::fmt::Display,
    {
        match std::env::var(name) {
            Ok(value) => value
                .trim()
                .parse()
                .map_err(|error| anyhow::anyhow!("{name}={value:?}: {error}")),
            Err(std::env::VarError::NotPresent) => Ok(default),
            Err(error) => bail!("{name}: {error}"),
        }
    }
    let sessions: usize = read("PRISM_L4_SESSIONS", sessions)?;
    let rate: f64 = read("PRISM_L4_SHARE_RATE", rate)?;
    ensure!(
        sessions > 0 && rate.is_finite() && rate > 0.0,
        "PRISM_L4_SESSIONS and PRISM_L4_SHARE_RATE must be positive"
    );
    Ok((sessions, rate))
}

/// Run `scenario` on `fixture` under `plan`'s load, then stop the load and
/// check it. The scenario's own error comes first; the load's check runs
/// only when the scenario passed, since a failed scenario can leave its
/// frontends in any state.
pub(super) async fn under_load<F>(
    fixture: &mut Fixture,
    plan: LoadPlan,
    scenario: F,
) -> Result<Value>
where
    F: AsyncFnOnce(&mut Fixture, &SessionLoad) -> Result<()>,
{
    let load = SessionLoad::start(plan)?;
    let result = scenario(fixture, &load).await;
    let record = load.stop().await;
    result?;
    record.check(fixture).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(start: Instant, seconds: f64) -> Instant {
        start + Duration::from_secs_f64(seconds)
    }

    fn record(
        start: Instant,
        connections: Vec<Connection>,
        tips: &[(usize, &str, f64)],
    ) -> LoadRecord {
        LoadRecord {
            plan: LoadPlan {
                name: "unit".into(),
                sessions: connections.len(),
                rate: 1.0,
                address: "qbrt1load".into(),
                stratum: vec![1, 2],
                api: vec![3, 4],
                nodes: vec![5, 6],
                node_of: vec![Some(0), Some(1)],
                seed: 0,
            },
            started: start,
            ended: at(start, 100.0),
            offered_seconds: 100.0,
            health: vec![(start, 0, true), (start, 1, true)],
            health_stats: vec![(Duration::ZERO, 0); 2],
            unreachable: Vec::new(),
            tips: tips
                .iter()
                .map(|(node, hash, seconds)| TipStamp {
                    node: *node,
                    hash: (*hash).into(),
                    at: at(start, *seconds),
                })
                .collect(),
            sessions: connections
                .into_iter()
                .map(|connection| SessionLog {
                    connections: vec![connection],
                    ..SessionLog::default()
                })
                .collect(),
        }
    }

    fn connection(start: Instant, frontend: usize, parents: &[(f64, &str)]) -> Connection {
        Connection {
            frontend,
            ready_at: at(start, 1.0),
            closed_at: None,
            parents: parents
                .iter()
                .map(|(seconds, parent)| (at(start, *seconds), (*parent).into()))
                .collect(),
        }
    }

    #[test]
    fn delivery_follows_each_frontends_node_and_gates_only_lasting_tips() {
        let start = Instant::now();
        let record = record(
            start,
            vec![
                connection(start, 0, &[(1.0, "g"), (10.5, "a"), (20.2, "c")]),
                connection(start, 0, &[(1.0, "g"), (12.0, "a")]),
                connection(start, 1, &[(1.0, "g"), (10.1, "a")]),
            ],
            &[
                (0, "g", 0.0),
                (1, "g", 0.0),
                (0, "a", 10.0),
                (1, "a", 9.9),
                (0, "b", 20.0),
                (0, "c", 20.1),
            ],
        );
        let deliveries = record.deliveries();
        let find = |frontend: usize, hash: &str| {
            deliveries
                .iter()
                .find(|delivery| delivery.frontend == frontend && delivery.hash == hash)
                .unwrap_or_else(|| panic!("no delivery of {hash} on {frontend}"))
        };
        let a0 = find(0, "a");
        assert_eq!((a0.eligible, a0.served, a0.late), (2, 2, 0));
        assert!((a0.seconds[1] - 2.0).abs() < 1e-6, "{a0:?}");
        assert!(a0.gated && a0.lifetime.is_some_and(|life| (life - 10.0).abs() < 1e-6));
        let a1 = find(1, "a");
        assert_eq!((a1.eligible, a1.served), (1, 1));
        assert!((a1.seconds[0] - 0.2).abs() < 1e-6, "{a1:?}");
        // Replaced 0.1 s later: never served, never late.
        let b0 = find(0, "b");
        assert_eq!((b0.served, b0.replaced, b0.late), (0, 2, 0));
        // The last tip lasts: the second session never got it.
        let c0 = find(0, "c");
        assert_eq!((c0.served, c0.late), (1, 1));
        assert!(c0.gated);
        assert!(!deliveries
            .iter()
            .any(|delivery| delivery.frontend == 1 && delivery.hash == "b"));
        let error = record.verdict(&HashSet::new()).unwrap_err().to_string();
        assert!(error.contains("no load share was acknowledged"), "{error}");
    }

    #[test]
    fn a_frontend_still_unready_when_the_bound_expires_is_not_gated() {
        let start = Instant::now();
        let mut record = record(
            start,
            vec![connection(start, 0, &[(1.0, "g")])],
            &[(0, "g", 0.0), (0, "a", 10.0)],
        );
        let gated = |record: &LoadRecord| {
            record
                .deliveries()
                .into_iter()
                .find(|delivery| delivery.hash == "a")
                .map(|delivery| (delivery.late, delivery.gated))
        };
        // The post-tip health fence: unready briefly, ready again.
        record.health.push((at(start, 10.2), 0, false));
        record.health.push((at(start, 12.0), 0, true));
        assert_eq!(gated(&record), Some((1, true)));
        record.health.push((at(start, 20.0), 0, false));
        assert_eq!(gated(&record), Some((1, false)));
    }

    #[test]
    fn a_tip_whose_node_stops_answering_within_the_bound_is_not_gated() {
        let start = Instant::now();
        let mut record = record(
            start,
            vec![connection(start, 0, &[(1.0, "g")])],
            &[(0, "g", 0.0), (0, "a", 10.0)],
        );
        record
            .unreachable
            .push((0, at(start, 11.0), at(start, 17.0)));
        let a0 = record
            .deliveries()
            .into_iter()
            .find(|delivery| delivery.hash == "a")
            .expect("delivery");
        assert_eq!((a0.late, a0.gated), (1, false));
        record.unreachable = vec![(1, at(start, 11.0), at(start, 17.0))];
        let a0 = record
            .deliveries()
            .into_iter()
            .find(|delivery| delivery.hash == "a")
            .expect("delivery");
        assert!(a0.gated, "another node's outage exempted it");
    }

    fn submit(share: &str, answer: Answer) -> SubmitRecord {
        SubmitRecord {
            share_id: share.into(),
            frontend: 0,
            answer,
        }
    }

    #[test]
    fn reconciliation_needs_every_ack_durable_and_no_refusal_durable() {
        let refused = Answer::Rejected(
            json!({"id":3,"result":null,"error":[21,"stale job",{"reason_id":"stale-job"}]}),
        );
        let submits = [
            submit("s1", Answer::Accepted),
            submit("s2", refused.clone()),
            submit("s3", Answer::TimedOut),
            submit("s4", Answer::Lost("closed".into())),
        ];
        let records: Vec<&SubmitRecord> = submits.iter().collect();
        let durable = |shares: &[&str]| shares.iter().map(|share| (*share).to_owned()).collect();
        let counts = reconcile(&records, &durable(&["s1", "s3"])).expect("reconciles");
        assert_eq!(
            (counts.accepted, counts.unknown, counts.unknown_durable),
            (1, 2, 1)
        );
        assert_eq!(counts.rejected.get("stale-job"), Some(&1));
        assert!(reconcile(&records, &durable(&[]))
            .unwrap_err()
            .to_string()
            .contains("acknowledged share s1"));
        assert!(reconcile(&records, &durable(&["s1", "s2"]))
            .unwrap_err()
            .to_string()
            .contains("refused share s2"));
        assert!(reconcile(&records, &durable(&["s1", "s9"]))
            .unwrap_err()
            .to_string()
            .contains("s9 was never submitted"));
    }

    #[test]
    fn notify_parent_is_the_display_hash() {
        let display = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let mut wire = hex::decode(display).expect("hex");
        wire.reverse();
        for word in wire.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        let notify = json!({"params": ["job", hex::encode(wire)]});
        assert_eq!(notify_parent(&notify).as_deref(), Some(display));
    }
}
