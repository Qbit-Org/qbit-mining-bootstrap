//! 3.1 dual-writer Stratum gating (D4): a listener accepts connections only
//! while the frontend admits miners. A frontend that is not ready refuses
//! every connection at the socket, so no TCP check, balancer or miner can see
//! it as up; a withdrawal refuses again while established sessions carry on,
//! unless it rests on a definite fault, which closes them too, each after
//! the request in hand; and a decision the health publisher stops renewing
//! closes the listener by itself. These tests read no environment input, so
//! they are not gated.
use qbit_prism_server::{
    codec,
    ledger::SessionId,
    listen::reserve_address,
    metrics::Metrics,
    readiness::admission::{AdmissionSignal, AdmissionState, Withdrawal},
    stratum::{
        run_gated_listener, MiningBackend, MiningJob, StaleGrace, StratumConfig, StratumError,
        Worker,
    },
};
use serde_json::{json, Value};
use std::{
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::watch,
    task::JoinHandle,
    time::{timeout, Instant},
};

/// Allocates sessions and authorizes nobody: enough for a subscribe to prove
/// a session is being served.
#[derive(Default)]
struct NoWork(AtomicU32);

impl MiningBackend for NoWork {
    type Context = ();
    async fn new_session_id(&self) -> Result<SessionId, StratumError> {
        Ok((self.0.fetch_add(1, Ordering::Relaxed) + 1).into())
    }
    async fn authorize(&self, _username: &str) -> Result<Worker, StratumError> {
        Err(StratumError::new(
            20,
            "invalid payout",
            "unauthorized-worker",
        ))
    }
    async fn build_job(
        &self,
        _worker: &Worker,
        _extranonce1: &str,
        _difficulty: f64,
        _minimum_difficulty: f64,
    ) -> Result<MiningJob<()>, StratumError> {
        unreachable!("no worker is ever authorized")
    }
    async fn submit(
        &self,
        _worker: &Worker,
        _job: &MiningJob<()>,
        _submission: codec::Submission,
        _grace: StaleGrace,
    ) -> Result<(), StratumError> {
        unreachable!("no worker is ever authorized")
    }
}

/// Holds every authorize until the test releases it, so a session can have
/// a request in flight.
struct HeldAuthorize {
    sessions: AtomicU32,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

impl HeldAuthorize {
    fn new() -> Self {
        Self {
            sessions: AtomicU32::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}

impl MiningBackend for HeldAuthorize {
    type Context = ();
    async fn new_session_id(&self) -> Result<SessionId, StratumError> {
        Ok((self.sessions.fetch_add(1, Ordering::Relaxed) + 1).into())
    }
    async fn authorize(&self, _username: &str) -> Result<Worker, StratumError> {
        self.entered.notify_one();
        let _permit = self.release.acquire().await;
        Err(StratumError::new(
            20,
            "invalid payout",
            "unauthorized-worker",
        ))
    }
    async fn build_job(
        &self,
        _worker: &Worker,
        _extranonce1: &str,
        _difficulty: f64,
        _minimum_difficulty: f64,
    ) -> Result<MiningJob<()>, StratumError> {
        unreachable!("no worker is ever authorized")
    }
    async fn submit(
        &self,
        _worker: &Worker,
        _job: &MiningJob<()>,
        _submission: codec::Submission,
        _grace: StaleGrace,
    ) -> Result<(), StratumError> {
        unreachable!("no worker is ever authorized")
    }
}

struct Gate {
    addr: SocketAddr,
    decide: watch::Sender<AdmissionSignal>,
    stop: watch::Sender<bool>,
    /// Held so sessions see a live work channel; a closed one ends them.
    _refresh: watch::Sender<u64>,
    metrics: Arc<Metrics>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Gate {
    async fn start(stale_after: Duration) -> Self {
        Self::start_with(stale_after, Arc::new(NoWork::default())).await
    }

    async fn start_with<B: MiningBackend>(stale_after: Duration, backend: Arc<B>) -> Self {
        let address = reserve_address(("127.0.0.1", 0)).await.unwrap();
        let addr = address.local_addr();
        let (decide, admission) = watch::channel(AdmissionSignal::UNDECIDED);
        let (stop, shutdown) = watch::channel(false);
        let (refresh_sender, refresh) = watch::channel(0u64);
        let metrics = Arc::new(Metrics::default());
        let task = tokio::spawn(run_gated_listener(
            address,
            StratumConfig::default(),
            backend,
            refresh,
            shutdown,
            metrics.clone(),
            admission,
            stale_after,
        ));
        Self {
            addr,
            decide,
            stop,
            _refresh: refresh_sender,
            metrics,
            task,
        }
    }

    fn admit(&self, admits: bool) {
        self.decide
            .send_replace(AdmissionSignal::decided(admits, Instant::now()));
    }

    /// Withdraw for `reason`; `definite` as for a fault that forbids the
    /// node's shares, which closes the established sessions too.
    fn withdraw(&self, reason: Withdrawal, definite: bool) {
        self.decide.send_replace(AdmissionSignal::of(
            AdmissionState::Withdrawn { reason },
            definite,
            Instant::now(),
        ));
    }

    fn accepting_gauge(&self) -> String {
        self.metrics
            .render()
            .lines()
            .find(|line| line.starts_with("qbit_prism_stratum_listener_accepting{"))
            .unwrap_or_default()
            .to_owned()
    }

    async fn stop(self) {
        self.stop.send_replace(true);
        timeout(Duration::from_secs(15), self.task)
            .await
            .expect("the gated listener stops on shutdown")
            .unwrap()
            .unwrap();
    }
}

/// One connect: `Ok` when the handshake completed, `Err` with the error
/// otherwise. A connect that neither completes nor fails within two seconds
/// fails the test: a gated address must answer, by accepting or refusing.
async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
    timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .expect("a connect to the gated address neither completed nor was refused")
}

fn refused(outcome: &io::Result<TcpStream>) -> bool {
    matches!(outcome, Err(error) if error.kind() == io::ErrorKind::ConnectionRefused)
}

/// Connect until the gate opens, within five seconds.
async fn connected(addr: SocketAddr) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match connect(addr).await {
            Ok(stream) => return stream,
            Err(error) if Instant::now() < deadline => {
                assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused, "{error}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("the gate never opened: {error}"),
        }
    }
}

/// Connect until connections are refused, within five seconds.
async fn refusing(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let outcome = connect(addr).await;
        if refused(&outcome) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the gate never closed: {outcome:?}"
        );
        drop(outcome);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Send one request on an established session and read its response.
async fn exchange(stream: &mut BufReader<TcpStream>, request: Value) -> Value {
    stream
        .get_mut()
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    timeout(Duration::from_secs(5), stream.read_line(&mut line))
        .await
        .expect("the session answers")
        .unwrap();
    serde_json::from_str(&line).unwrap_or_else(|error| panic!("{request}: {error}: {line:?}"))
}

#[tokio::test]
async fn a_frontend_that_does_not_admit_refuses_connections_at_the_socket() {
    let gate = Gate::start(Duration::from_secs(15)).await;
    // Never admitted: every attempt is refused, none completes a handshake.
    for _ in 0..5 {
        let outcome = connect(gate.addr).await;
        assert!(
            refused(&outcome),
            "a not-ready frontend accepted: {outcome:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        gate.accepting_gauge(),
        "qbit_prism_stratum_listener_accepting{listener=\"default\"} 0"
    );
    // A decision that admits nothing keeps it refusing.
    gate.admit(false);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(refused(&connect(gate.addr).await));

    gate.admit(true);
    let mut session = BufReader::new(connected(gate.addr).await);
    let subscribed = exchange(
        &mut session,
        json!({"id":1,"method":"mining.subscribe","params":["gate-test"]}),
    )
    .await;
    assert_eq!(subscribed["id"], 1);
    assert!(subscribed["error"].is_null(), "{subscribed}");
    assert_eq!(
        gate.accepting_gauge(),
        "qbit_prism_stratum_listener_accepting{listener=\"default\"} 1"
    );

    // Withdrawn: new connections are refused again, on the same port, while
    // the established session is still served.
    gate.admit(false);
    refusing(gate.addr).await;
    let health = exchange(
        &mut session,
        json!({"id":2,"method":"mining.get_health","params":[]}),
    )
    .await;
    assert_eq!(health["id"], 2);
    assert!(health["error"].is_null(), "{health}");
    assert_eq!(
        gate.accepting_gauge(),
        "qbit_prism_stratum_listener_accepting{listener=\"default\"} 0"
    );

    // Readmitted on the same address.
    gate.admit(true);
    drop(connected(gate.addr).await);
    gate.stop().await;
}

/// Subscribe a new session on an admitting gate.
async fn subscribed(gate: &Gate) -> BufReader<TcpStream> {
    let mut session = BufReader::new(connected(gate.addr).await);
    let subscribed = exchange(
        &mut session,
        json!({"id":1,"method":"mining.subscribe","params":["gate-test"]}),
    )
    .await;
    assert!(subscribed["error"].is_null(), "{subscribed}");
    session
}

/// The session's next read finds it closed.
async fn closed(session: &mut BufReader<TcpStream>, what: &str) {
    let mut line = String::new();
    let read = timeout(Duration::from_secs(5), session.read_line(&mut line))
        .await
        .unwrap_or_else(|_| panic!("a session outlived {what}"));
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "{what}: the session was still served: {read:?} {line:?}"
    );
}

/// A withdrawal on a definite fault, here a database that answered that it
/// is not this node's, closes the sessions already accepted as well as the
/// listener, so none of them writes another share there; also one a
/// not-ready withdrawal had left in place. A database that only stopped
/// answering withdraws the node without closing them.
#[tokio::test]
async fn a_definite_fault_closes_established_sessions_and_a_silent_database_does_not() {
    let gate = Gate::start(Duration::from_secs(15)).await;
    gate.admit(true);
    let mut session = subscribed(&gate).await;
    gate.withdraw(Withdrawal::WriterNotLocal, true);
    refusing(gate.addr).await;
    closed(&mut session, "a definite fault").await;

    gate.admit(true);
    let mut session = subscribed(&gate).await;
    gate.withdraw(Withdrawal::NotReady, false);
    refusing(gate.addr).await;
    let health = exchange(
        &mut session,
        json!({"id":2,"method":"mining.get_health","params":[]}),
    )
    .await;
    assert!(health["error"].is_null(), "{health}");
    gate.withdraw(Withdrawal::OwnLogBehind, true);
    closed(&mut session, "a later definite fault").await;

    // Unanswered for the writer probe's streak: withdrawn, sessions served.
    gate.admit(true);
    let mut session = subscribed(&gate).await;
    gate.withdraw(Withdrawal::WriterNotLocal, false);
    refusing(gate.addr).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let health = exchange(
        &mut session,
        json!({"id":3,"method":"mining.get_health","params":[]}),
    )
    .await;
    assert!(health["error"].is_null(), "{health}");
    gate.stop().await;
}

/// A session closed by a definite fault stops at a safe point: the request
/// in hand, here an authorize the backend holds, is answered first, so a
/// share being made durable is never cut off mid-flight.
#[tokio::test]
async fn a_closed_session_answers_the_request_in_hand_first() {
    let backend = Arc::new(HeldAuthorize::new());
    let gate = Gate::start_with(Duration::from_secs(15), backend.clone()).await;
    gate.admit(true);
    let mut session = subscribed(&gate).await;
    session
        .get_mut()
        .write_all(
            format!(
                "{}\n",
                json!({"id":2,"method":"mining.authorize","params":["gate.worker","x"]})
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(5), backend.entered.notified())
        .await
        .expect("the authorize reached the backend");
    gate.withdraw(Withdrawal::WriterNotLocal, true);
    refusing(gate.addr).await;
    backend.release.add_permits(1);
    let mut line = String::new();
    timeout(Duration::from_secs(5), session.read_line(&mut line))
        .await
        .expect("the request in hand was answered")
        .unwrap();
    let answer: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(answer["id"], 2, "{answer}");
    closed(&mut session, "a definite fault, after its answer").await;
    gate.stop().await;
}

#[tokio::test]
async fn a_decision_the_publisher_stops_renewing_closes_the_listener() {
    let stale_after = Duration::from_millis(400);
    let gate = Gate::start(stale_after).await;
    let decided = Instant::now();
    gate.decide
        .send_replace(AdmissionSignal::decided(true, decided));
    drop(connected(gate.addr).await);
    // No further decision: the listener closes once the last one is stale.
    refusing(gate.addr).await;
    assert!(
        decided.elapsed() >= stale_after,
        "closed before the decision went stale"
    );
    // A decision already stale when it arrives admits nothing.
    gate.decide.send_replace(AdmissionSignal::decided(
        true,
        Instant::now() - stale_after - Duration::from_millis(1),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(refused(&connect(gate.addr).await));
    // A fresh one opens it again.
    gate.admit(true);
    drop(connected(gate.addr).await);
    gate.stop().await;
}

#[tokio::test]
async fn shutdown_stops_a_listener_that_was_never_admitted() {
    let gate = Gate::start(Duration::from_secs(15)).await;
    assert!(refused(&connect(gate.addr).await));
    gate.stop().await;
}
