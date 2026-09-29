//! One in-memory connection to the production session loop, with every line
//! the server writes checked as it is read.
use crate::{
    backend::FuzzBackend,
    checks::{Accepted, Checker, ConnState, Frame},
    violation,
};
use qbit_prism_server::{metrics::Metrics, stratum::StratumConfig};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, ReadHalf, WriteHalf},
    sync::watch,
    task::JoinHandle,
};

/// `PRISM_FUZZ_TRACE=1` prints every frame sent and line read, for
/// reproducing a crash artifact by hand.
fn trace() -> bool {
    static TRACE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TRACE.get_or_init(|| std::env::var_os("PRISM_FUZZ_TRACE").is_some_and(|v| v == "1"))
}

/// Long enough that only a hung server trips it.
const ANSWER_DEADLINE: Duration = Duration::from_secs(10);

/// What every connection of one iteration shares.
pub struct Harness {
    pub config: StratumConfig,
    pub backend: Arc<FuzzBackend>,
    pub metrics: Arc<Metrics>,
    pub refresh: Arc<watch::Sender<u64>>,
    pub shutdown: watch::Sender<bool>,
    pub accepted: Vec<Accepted>,
}

impl Harness {
    pub fn new(config: StratumConfig, backend: Arc<FuzzBackend>) -> Self {
        if let Err(error) = config.validate() {
            panic!("fuzz config must be valid: {error:#}");
        }
        Self {
            config,
            backend,
            metrics: Arc::new(Metrics::default()),
            refresh: Arc::new(watch::channel(0).0),
            shutdown: watch::channel(false).0,
            accepted: Vec::new(),
        }
    }

    /// Open a connection. Must run inside the runtime.
    pub fn connect(&mut self) -> Conn<'_> {
        let (client, server) = tokio::io::duplex(1 << 24);
        let (server_reader, server_writer) = tokio::io::split(server);
        let (reader, writer) = tokio::io::split(client);
        let task = tokio::spawn(qbit_prism_server::stratum::serve_connection(
            server_reader,
            server_writer,
            self.backend.clone(),
            self.config.clone(),
            self.refresh.subscribe(),
            self.shutdown.subscribe(),
            self.metrics.clone(),
        ));
        Conn {
            checker: Checker {
                config: &self.config,
                backend: &self.backend,
                state: ConnState::default(),
                accepted: &mut self.accepted,
            },
            refresh: self.refresh.clone(),
            reader: BufReader::new(reader),
            writer,
            task: Some(task),
            pending: VecDeque::new(),
            eof: false,
        }
    }
}

pub struct Conn<'a> {
    pub checker: Checker<'a>,
    refresh: Arc<watch::Sender<u64>>,
    reader: BufReader<ReadHalf<DuplexStream>>,
    writer: WriteHalf<DuplexStream>,
    task: Option<JoinHandle<anyhow::Result<()>>>,
    /// Frames sent and not yet answered, oldest first.
    pending: VecDeque<Frame>,
    eof: bool,
}

impl Conn<'_> {
    /// No further frame can be answered: the server closed or announced it.
    pub fn closed(&self) -> bool {
        self.eof || self.checker.state.closing.is_some()
    }

    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Send raw bytes; `frames` are the complete frames they finish, in order.
    pub async fn send(&mut self, bytes: &[u8], frames: impl IntoIterator<Item = Frame>) {
        if self.closed() {
            return;
        }
        self.pending.extend(frames);
        if trace() {
            eprintln!(">> {}", String::from_utf8_lossy(bytes).trim_end());
        }
        // A write can race the server's close; the answers decide validity.
        let _ = self.writer.write_all(bytes).await;
    }

    /// Tell every connection's loop that published work changed.
    pub fn refresh(&self) {
        self.refresh.send_modify(|generation| *generation += 1);
    }

    /// Read and check everything until the server closes, as it must after
    /// refusing an oversize frame.
    pub async fn read_to_close(&mut self) {
        while !self.eof {
            self.read_line().await;
        }
    }

    /// A `get_health` probe answered after everything sent before it, so
    /// every line those frames caused has been read and checked.
    pub async fn barrier(&mut self, n: u32) {
        let probe = format!(
            "{{\"id\":\"fz-barrier-{n}\",\"method\":\"mining.get_health\",\"params\":[]}}\n"
        );
        self.send(probe.as_bytes(), [Frame::classify(probe.as_bytes())])
            .await;
        self.drain(0).await;
    }

    /// Read and check lines until at most `keep` frames are unanswered or the
    /// server closes.
    pub async fn drain(&mut self, keep: usize) {
        while self.pending.len() > keep && !self.eof {
            self.read_line().await;
        }
    }

    async fn read_line(&mut self) {
        let mut line = Vec::new();
        let read = tokio::time::timeout(ANSWER_DEADLINE, self.reader.read_until(b'\n', &mut line))
            .await
            .unwrap_or_else(|_| {
                violation(format!(
                    "the server stopped answering with {} frames pending",
                    self.pending.len()
                ))
            })
            .unwrap_or_else(|e| violation(format!("pipe read failed: {e}")));
        if read == 0 {
            self.eof = true;
            if !self.pending.is_empty() && self.checker.state.closing.is_none() {
                violation(format!(
                    "the server closed with {} frames unanswered and no cause",
                    self.pending.len()
                ));
            }
            return;
        }
        if trace() {
            eprintln!("<< {}", String::from_utf8_lossy(&line).trim_end());
        }
        if line.last() != Some(&b'\n') {
            violation("the server wrote a line without its newline");
        }
        if self.checker.line(&line, self.pending.front()) {
            self.pending.pop_front();
        }
    }

    /// End the stream, check everything the server still writes, and require
    /// a clean return from the session loop.
    pub async fn close(mut self) -> ConnState {
        let _ = self.writer.shutdown().await;
        self.read_to_close().await;
        let task = self.task.take().expect("closed once");
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => violation(format!("the session loop failed: {error:#}")),
            Err(join) if join.is_panic() => std::panic::resume_unwind(join.into_panic()),
            Err(join) => violation(format!("the session task ended abnormally: {join}")),
        }
        std::mem::take(&mut self.checker.state)
    }
}

impl Drop for Conn<'_> {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
