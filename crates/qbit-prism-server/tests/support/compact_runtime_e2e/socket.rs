use super::*;
use qbit_prism_server::{
    codec::Submission,
    stratum::{run_listener, StratumConfig},
};
use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::watch,
    task::JoinHandle,
};

pub struct Listener {
    pub address: SocketAddr,
    #[allow(dead_code)]
    pub stats: Arc<qbit_prism_server::stratum::StratumStats>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}
impl Listener {
    pub async fn start(coordinator: &Arc<Coordinator>, difficulty: f64) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, receiver) = watch::channel(false);
        let config = StratumConfig {
            startup_difficulty: difficulty,
            vardiff: qbit_prism_server::vardiff::VardiffConfig {
                enabled: false,
                minimum: difficulty,
                ..Default::default()
            },
            job_retention_seconds: 15.0,
            stale_grace_seconds: 0.0,
            ..Default::default()
        };
        let stats = config.stats.clone();
        let task = tokio::spawn(run_listener(
            listener,
            config,
            coordinator.clone(),
            coordinator.refresh.subscribe(),
            receiver,
            coordinator.metrics.clone(),
        ));
        Ok(Self {
            address,
            stats,
            shutdown,
            task,
        })
    }
    pub async fn close(&mut self) -> Result<()> {
        self.shutdown.send_replace(true);
        timeout(Duration::from_secs(12), &mut self.task)
            .await
            .context("Stratum shutdown timed out")???;
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
    writer: OwnedWriteHalf,
    pub extranonce1: String,
    pub notify: Value,
    pub difficulty: f64,
}
impl Client {
    pub async fn connect(address: SocketAddr) -> Result<Self> {
        let (reader, writer) = TcpStream::connect(address).await?.into_split();
        Ok(Self {
            reader: BufReader::new(reader),
            writer,
            extranonce1: String::new(),
            notify: Value::Null,
            difficulty: 0.0,
        })
    }
    pub async fn send(&mut self, value: Value) -> Result<()> {
        self.writer
            .write_all(format!("{value}\n").as_bytes())
            .await?;
        Ok(())
    }
    pub async fn read(&mut self) -> Result<Value> {
        let mut line = String::new();
        ensure!(
            timeout(Duration::from_secs(5), self.reader.read_line(&mut line)).await?? > 0,
            "Stratum closed before a response"
        );
        let value: Value = serde_json::from_str(&line)?;
        if value["method"] == "mining.set_difficulty" {
            self.difficulty = value["params"][0].as_f64().context("difficulty missing")?;
        }
        Ok(value)
    }
    pub async fn response(&mut self, id: u64) -> Result<Value> {
        timeout(Duration::from_secs(5), async {
            loop {
                let value = self.read().await?;
                if value["id"] == id {
                    return Ok(value);
                }
            }
        })
        .await
        .context("Stratum request deadline elapsed")?
    }
    pub async fn configure(&mut self) -> Result<()> {
        self.send(json!({"id":0,"method":"mining.configure","params":[["version-rolling"],{"version-rolling.mask":format!("{MASK:08x}")}]})).await?;
        let reply = self.response(0).await?;
        ensure!(
            reply["result"]["version-rolling"] == true
                && reply["result"]["version-rolling.mask"] == format!("{MASK:08x}"),
            "mask negotiation failed: {reply}"
        );
        Ok(())
    }
    pub async fn login(&mut self, name: &str) -> Result<()> {
        self.send(json!({"id":1,"method":"mining.subscribe","params":[]}))
            .await?;
        self.extranonce1 = self.response(1).await?["result"][1]
            .as_str()
            .context("subscription entropy missing")?
            .into();
        self.send(json!({"id":2,"method":"mining.authorize","params":[name,"x"]}))
            .await?;
        let reply = self.response(2).await?;
        ensure!(reply["result"] == true, "authorization failed: {reply}");
        timeout(Duration::from_secs(5), async {
            loop {
                let value = self.read().await?;
                if value["method"] == "mining.notify" {
                    self.notify = value;
                    return Ok::<_, anyhow::Error>(());
                }
            }
        })
        .await
        .context("initial notify deadline elapsed")?
    }
}

// Use the production codec. This loop finds a low-difficulty ordinary share,
// avoiding candidate submission and any need for fake submitblock semantics.
pub fn ordinary_submit(
    job: &MiningJob<JobContext>,
    worker: &Worker,
    request: u64,
) -> Result<(Value, Submission)> {
    for nonce in 0..10_000u32 {
        let nonce = format!("{nonce:08x}");
        let ntime = format!("{:08x}", job.wire.ntime);
        let extra = "00".repeat(job.wire.extranonce2_size);
        let proof = job
            .wire
            .assemble_submission(&extra, &ntime, &nonce, Some("00002000"), MASK)?;
        if proof.share_pass && !proof.block_pass {
            return Ok((
                json!({"id":request,"method":"mining.submit","params":[worker.username,job.wire.job_id,extra,ntime,nonce,"00002000"]}),
                proof,
            ));
        }
    }
    anyhow::bail!("no ordinary share found within bounded fixture search")
}
