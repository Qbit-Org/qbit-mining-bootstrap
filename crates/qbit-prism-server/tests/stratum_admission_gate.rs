//! 3.1 dual-writer Stratum gating (D4): a listener accepts connections only
//! while the frontend admits miners. A frontend that is not ready refuses
//! every connection at the socket, so no TCP check, balancer or miner can see
//! it as up; withdrawing refuses again while established sessions carry on;
//! and a decision the health publisher stops renewing closes the listener by
//! itself. These tests read no environment input, so they are not gated.
use qbit_prism_server::{
    codec,
    ledger::SessionId,
    listen::reserve_address,
    metrics::Metrics,
    readiness::admission::AdmissionSignal,
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
        let address = reserve_address(("127.0.0.1", 0)).await.unwrap();
        let addr = address.local_addr();
        let (decide, admission) = watch::channel(AdmissionSignal::UNDECIDED);
        let (stop, shutdown) = watch::channel(false);
        let (refresh_sender, refresh) = watch::channel(0u64);
        let metrics = Arc::new(Metrics::default());
        let task = tokio::spawn(run_gated_listener(
            address,
            StratumConfig::default(),
            Arc::new(NoWork::default()),
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
