use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    coordinator::Coordinator,
    stratum::{run_listener, ConnectionLimit, StratumConfig},
};
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::{timeout, timeout_at, Instant},
};

pub struct Listener {
    pub address: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl Listener {
    pub async fn start(frontend: &Arc<Coordinator>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, receiver) = watch::channel(false);
        let config = StratumConfig {
            connection_limit: ConnectionLimit::new(2_000),
            job_retention_seconds: 300.0,
            vardiff: qbit_prism_server::vardiff::VardiffConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let task = tokio::spawn(run_listener(
            listener,
            config,
            frontend.clone(),
            frontend.refresh.subscribe(),
            receiver,
            frontend.metrics.clone(),
        ));
        Ok(Self {
            address,
            shutdown,
            task,
        })
    }

    pub async fn close(&mut self) -> Result<()> {
        self.shutdown.send_replace(true);
        timeout(Duration::from_secs(15), &mut self.task)
            .await
            .context("listener shutdown deadline")???;
        Ok(())
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        self.task.abort();
    }
}

pub struct Client {
    reader: BufReader<OwnedReadHalf>,
    // Keep the write half alive throughout the delivery bracket.
    writer: OwnedWriteHalf,
    pub initial_job: String,
}

impl Client {
    pub async fn login(address: SocketAddr, index: usize, parent: &str) -> Result<Self> {
        // One deadline covers connect, writes, replies, and initial work.
        timeout(Duration::from_secs(30), async {
            let stream = TcpStream::connect(address).await?;
            stream.set_nodelay(true)?;
            let (reader, writer) = stream.into_split();
            let mut client = Self { reader: BufReader::new(reader), writer, initial_job: String::new() };
            client.send(json!({"id":1,"method":"mining.subscribe","params":[]})).await?;
            let response = client.response(1).await?;
            ensure!(response["error"].is_null() && response["result"].is_array(), "subscribe failed: {response}");
            client.send(json!({"id":2,"method":"mining.authorize","params":[format!("b275.worker{index}"),"x"]})).await?;
            let response = client.response(2).await?;
            ensure!(response["error"].is_null() && response["result"] == true, "authorize failed: {response}");
            client.initial_job = client.notify(parent).await?;
            Ok(client)
        }).await.context("login deadline elapsed")?
    }

    async fn send(&mut self, value: Value) -> Result<()> {
        self.writer
            .write_all(format!("{value}\n").as_bytes())
            .await?;
        Ok(())
    }

    async fn read(&mut self) -> Result<Value> {
        let mut line = String::new();
        ensure!(
            self.reader.read_line(&mut line).await? > 0,
            "Stratum closed before notify"
        );
        Ok(serde_json::from_str(&line)?)
    }

    async fn response(&mut self, id: u64) -> Result<Value> {
        loop {
            let value = self.read().await?;
            if value["id"] == id {
                return Ok(value);
            }
            ensure!(
                value["method"] != "mining.notify",
                "notify preceded authorization reply"
            );
        }
    }

    async fn notify(&mut self, parent: &str) -> Result<String> {
        loop {
            let value = self.read().await?;
            if value["method"] == "mining.notify" {
                // Fixture parents repeat a single byte, so Stratum's word
                // swapping does not change this expected wire representation.
                ensure!(
                    value["params"][1] == parent,
                    "unexpected notify parent: {value}"
                );
                ensure!(value["params"][8] == true, "new-tip work must clean jobs");
                return Ok(value["params"][0]
                    .as_str()
                    .context("notify lacks job ID")?
                    .into());
            }
        }
    }

    pub async fn receive(
        &mut self,
        parent: &str,
        start: Instant,
        deadline: Instant,
    ) -> Result<(String, f64)> {
        timeout_at(deadline, async {
            let job = self.notify(parent).await?;
            let elapsed = start.elapsed().as_secs_f64();
            ensure!(job != self.initial_job, "new tip reused initial job ID");
            Ok((job, elapsed))
        })
        .await
        .context("new-tip delivery deadline elapsed")?
    }
}
