//! Actual listener/socket fixture; only backend I/O is controlled.
use qbit_prism_server::{
    codec::Submission,
    ledger::Ledger,
    stratum::{
        run_listener, MiningBackend, MiningJob, StaleGrace, StratumConfig, StratumError,
        StratumStats, Worker,
    },
};
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{watch, Notify},
    task::JoinHandle,
    time::{timeout, Duration},
};

#[derive(Default)]
pub struct Backend {
    pub ledger: Option<Ledger>,
    pub allocation_calls: AtomicU32,
    pub ids: AtomicU32,
    pub authorize_calls: AtomicU32,
    pub build_calls: AtomicU32,
    pub fail_once: AtomicBool,
    pub stall_once: AtomicBool,
    pub allocation_started: Notify,
    pub allocation_release: Notify,
}
impl MiningBackend for Backend {
    type Context = ();
    async fn new_session_id(&self) -> Result<u32, StratumError> {
        self.allocation_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(StratumError::backend("controlled allocator unavailable"));
        }
        let id = match &self.ledger {
            Some(ledger) => ledger
                .new_session_id()
                .await
                .map_err(|_| StratumError::backend("database unavailable"))?,
            None => self.ids.fetch_add(1, Ordering::SeqCst) + 1,
        };
        // Models a consumed sequence value whose RPC reply misses the deadline.
        if self.stall_once.swap(false, Ordering::SeqCst) {
            self.allocation_started.notify_one();
            self.allocation_release.notified().await;
        }
        Ok(id)
    }
    async fn authorize(&self, username: &str) -> Result<Worker, StratumError> {
        self.authorize_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Worker {
            username: username.into(),
            payout_address: "controlled-payout".into(),
            worker_name: None,
            p2mr_program_hex: "ab".repeat(32),
        })
    }
    async fn build_job(
        &self,
        _: &Worker,
        _: &str,
        _: f64,
        _: f64,
    ) -> Result<MiningJob<()>, StratumError> {
        self.build_calls.fetch_add(1, Ordering::SeqCst);
        Err(StratumError::backend("controlled no prepared work"))
    }
    async fn submit(
        &self,
        _: &Worker,
        _: &MiningJob<()>,
        _: Submission,
        _: StaleGrace,
    ) -> Result<(), StratumError> {
        unreachable!("this fixture issues no work")
    }
}

pub struct Server {
    pub address: std::net::SocketAddr,
    pub backend: Arc<Backend>,
    pub stats: Arc<StratumStats>,
    shutdown: watch::Sender<bool>,
    _refresh: watch::Sender<u64>,
    task: JoinHandle<anyhow::Result<()>>,
}
impl Server {
    pub async fn start(mut config: StratumConfig, backend: Arc<Backend>) -> Self {
        config.vardiff.enabled = false;
        let stats = config.stats.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (refresh, refresh_rx) = watch::channel(0);
        let task = tokio::spawn(run_listener(
            listener,
            config,
            backend.clone(),
            refresh_rx,
            shutdown_rx,
            Arc::new(qbit_prism_server::metrics::Metrics::default()),
        ));
        Self {
            address,
            backend,
            stats,
            shutdown,
            _refresh: refresh,
            task,
        }
    }
    pub async fn connections(&self, count: usize) {
        timeout(Duration::from_secs(5), async {
            while self.stats.snapshot(0).connections != count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("connection did not enter/leave the actual session loop");
    }
    pub async fn stop(self) {
        self.shutdown.send(true).unwrap();
        timeout(Duration::from_secs(12), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

pub struct Client(pub BufReader<TcpStream>);
impl Client {
    pub async fn connect(server: &Server) -> Self {
        Self(BufReader::new(
            TcpStream::connect(server.address).await.unwrap(),
        ))
    }
    pub async fn send(&mut self, request: Value) {
        let mut bytes = serde_json::to_vec(&request).unwrap();
        bytes.push(b'\n');
        self.0.get_mut().write_all(&bytes).await.unwrap();
    }
    pub async fn read(&mut self) -> Value {
        let mut line = String::new();
        let count = timeout(Duration::from_secs(5), self.0.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(count > 0, "session closed before returning a response");
        serde_json::from_str(&line).unwrap()
    }
    pub async fn request(&mut self, request: Value) -> Value {
        self.send(request).await;
        self.read().await
    }
}
