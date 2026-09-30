//! #604: a session's first job must not queue behind a rebuild storm.
//!
//! Every publication makes every connected session rebuild its job. Those
//! rebuilds and a new session's first job used to share one first come,
//! first served admission, so a session opening during a fan-out waited for
//! the whole fan-out, and opening sessions one at a time grew with the
//! square of the session count. Rebuilds now go through a smaller rebuild
//! lane first and keep it through persistence, so a first job waits for at
//! most the lane's rebuilds.
//!
//! The case runs a real `Coordinator` against a private PostgreSQL schema and
//! a fake node, serves every session through `stratum::serve_connection` over
//! an in-memory pipe, and keeps `STORM_SESSIONS` sessions rebuilding back to
//! back by signalling the refresh channel continuously, so each rebuild is a
//! real build, revalidation and persisted job. It then opens sessions one at
//! a time and counts the storm's deliveries between each one's authorize and
//! its first job: a count, not a wall-clock bound.
//!
//! PRISM_TEST_DATABASE_URL=postgres://postgres:prism@127.0.0.1:5432/postgres \
//!     cargo test -p qbit-prism-server --lib first_job_lane_tests

use super::d2_test_support::{
    database_url, settle, test_config, unix_now, TestSchema, TEMPLATE_BITS,
};
use super::test_serial::TEST_LOCK;
use super::*;
use crate::stratum::{serve_connection, StratumConfig};
use anyhow::{anyhow, bail};
use axum::{routing::post, Json, Router};
use std::sync::atomic::AtomicU64;
use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf,
};
use tokio::task::JoinSet;

/// Sessions that already hold work and rebuild on every refresh signal.
const STORM_SESSIONS: usize = 1024;
/// Sessions opened one at a time while the storm runs.
const NEW_SESSIONS: usize = 8;
/// A first job may be overtaken by at most this many storm deliveries. The
/// rebuild lane at the default 128 initial-job permits is 32, so a first job
/// waits for about 32 rebuilds admitted ahead of it and their persistence,
/// however many sessions rebuild; first come, first served puts it behind
/// nearly every storm session.
const OVERTAKEN_BOUND: u64 = STORM_SESSIONS as u64 / 8;
const HEIGHT: u64 = 100;
const TIP: &str = "a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0";
const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// One fixed tip; `validateaddress` answers a P2MR script for any address.
async fn node_reply(Json(request): Json<Value>) -> Json<Value> {
    let result = match request["method"].as_str().unwrap_or_default() {
        "getblockhash" => match request["params"][0].as_u64() {
            Some(HEIGHT) => json!(TIP),
            Some(0) => json!(GENESIS),
            _ => Value::Null,
        },
        "getbestblockhash" => json!(TIP),
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":HEIGHT,"headers":HEIGHT,"bestblockhash":TIP,
            "chainwork":format!("{:064x}",1)}),
        "getnetworkinfo" => json!({"connections":2}),
        "getblockheader" => json!({"previousblockhash":GENESIS}),
        "getblocktemplate" => json!({"version":0x2000_0000u32,"bits":TEMPLATE_BITS,
            "height":HEIGHT+1,"coinbasevalue":500_000_000u64,
            "curtime":unix_now().unwrap_or_default(),
            "previousblockhash":TIP,"transactions":[]}),
        "getmempoolinfo" => json!({"minrelaytxfee":0.00001,"mempoolminfee":0.00001}),
        "validateaddress" => json!({"isvalid":true,
            "scriptPubKey":format!("5220{}", "ab".repeat(32))}),
        method => {
            return Json(json!({"id":request["id"],"result":null,
                "error":{"code":-32601,"message":format!("unexpected RPC {method}")}}))
        }
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

/// The miner side of one in-memory Stratum connection.
struct Client {
    writer: WriteHalf<DuplexStream>,
    lines: Lines<BufReader<ReadHalf<DuplexStream>>>,
}

impl Client {
    async fn send(&mut self, id: u64, method: &str, params: Value) -> Result<()> {
        let mut frame = serde_json::to_vec(&json!({"id":id,"method":method,"params":params}))?;
        frame.push(b'\n');
        self.writer.write_all(&frame).await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Value> {
        let line = self
            .lines
            .next_line()
            .await?
            .context("the session closed")?;
        Ok(serde_json::from_str(&line)?)
    }

    async fn response(&mut self, id: u64) -> Result<Value> {
        loop {
            let value = self.next().await?;
            if value["id"] == id {
                ensure!(value["error"].is_null(), "request {id} refused: {value}");
                return Ok(value);
            }
        }
    }

    async fn first_job(&mut self) -> Result<()> {
        while self.next().await?["method"] != "mining.notify" {}
        Ok(())
    }

    async fn subscribe(&mut self) -> Result<()> {
        self.send(1, "mining.subscribe", json!([])).await?;
        self.response(1).await?;
        Ok(())
    }

    /// Authorize after `subscribe`; the first job follows.
    async fn authorize(&mut self, username: &str) -> Result<()> {
        self.send(2, "mining.authorize", json!([username, "x"]))
            .await?;
        ensure!(
            self.response(2).await?["result"] == true,
            "{username} was not authorized"
        );
        Ok(())
    }
}

struct Fixture {
    schema: TestSchema,
    coordinator: Arc<Coordinator>,
    node: tokio::task::JoinHandle<()>,
    stratum: StratumConfig,
    metrics: Arc<crate::metrics::Metrics>,
    shutdown: watch::Sender<bool>,
    sessions: JoinSet<Result<()>>,
}

impl Fixture {
    async fn open(raw: &str) -> Result<Self> {
        let schema = TestSchema::create(raw, "prism_first_job_lane").await?;
        let opened = async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let rpc_url = format!("http://{}/", listener.local_addr()?);
            let config = test_config(
                schema.url(),
                rpc_url,
                "first-job-lane",
                Duration::from_secs(15),
            )?;
            let node = tokio::spawn(async move {
                let _ = axum::serve(listener, Router::new().route("/", post(node_reply))).await;
            });
            match Coordinator::new(config, Arc::new(crate::metrics::Metrics::default())).await {
                Ok(coordinator) => Ok((coordinator, node)),
                Err(error) => {
                    node.abort();
                    Err(error)
                }
            }
        }
        .await;
        let (coordinator, node) = match opened {
            Ok(opened) => opened,
            Err(error) => return Err(schema.abandon(error).await),
        };
        let mut stratum = StratumConfig {
            initial_job_timeout_seconds: 60.0,
            ..Default::default()
        };
        stratum.vardiff.enabled = false;
        Ok(Self {
            schema,
            coordinator,
            node,
            stratum,
            metrics: Arc::new(crate::metrics::Metrics::default()),
            shutdown: watch::channel(false).0,
            sessions: JoinSet::new(),
        })
    }

    /// One share so the publication carries shared work, as production's
    /// windows do, then the first publication.
    async fn publish(&self) -> Result<()> {
        self.coordinator
            .ledger
            .append(
                AcceptedShare {
                    share_seq: 0,
                    share_id: format!("first-job-lane:{:064x}", 1),
                    miner_id: "miner-seed.rig".into(),
                    order_key: "miner-seed.rig".into(),
                    p2mr_program_hex: "cd".repeat(32),
                    share_difficulty: 1,
                    network_difficulty: 100,
                    template_height: HEIGHT + 1,
                    job_id: "first-job-lane-seed".into(),
                    job_issued_at_ms: 1,
                    accepted_at_ms: 0,
                    ntime: 1_800_000_000,
                    credit_policy: None,
                },
                None,
            )
            .await?;
        self.coordinator.refresh_once().await?;
        let prepared = self.coordinator.prepared.read().await.clone();
        ensure!(
            prepared.is_some_and(|prepared| prepared.bundle.is_some()),
            "the seeded window did not publish shared work"
        );
        Ok(())
    }

    fn connect(&mut self) -> Client {
        let (client, server) = tokio::io::duplex(1 << 16);
        let (reader, writer) = tokio::io::split(server);
        self.sessions.spawn(serve_connection(
            reader,
            writer,
            self.coordinator.clone(),
            self.stratum.clone(),
            self.coordinator.refresh.subscribe(),
            self.shutdown.subscribe(),
            self.metrics.clone(),
        ));
        let (reader, writer) = tokio::io::split(client);
        Client {
            writer,
            lines: BufReader::new(reader).lines(),
        }
    }

    async fn close(mut self) -> Result<()> {
        let _ = self.shutdown.send(true);
        self.sessions.abort_all();
        while self.sessions.join_next().await.is_some() {}
        self.node.abort();
        self.coordinator.ledger.pool.close().await;
        self.schema.remove().await
    }
}

/// With `STORM_SESSIONS` sessions rebuilding back to back, each session
/// opened one at a time gets its first job after at most `OVERTAKEN_BOUND`
/// storm deliveries, while the storm keeps delivering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_job_is_not_queued_behind_a_rebuild_storm() -> Result<()> {
    let Some(url) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::open(&url).await?;
    let result = async {
        fixture.publish().await?;
        let mut storm = Vec::with_capacity(STORM_SESSIONS);
        for index in 0..STORM_SESSIONS {
            let mut client = fixture.connect();
            client.subscribe().await?;
            client.authorize(&format!("qbrt1storm.w{index}")).await?;
            storm.push(client);
        }
        let mut readers = JoinSet::new();
        let delivered = Arc::new(AtomicU64::new(0));
        for mut client in storm {
            let delivered = delivered.clone();
            readers.spawn(async move {
                client.first_job().await?;
                loop {
                    if client.next().await?["method"] == "mining.notify" {
                        delivered.fetch_add(1, Ordering::Relaxed);
                    }
                }
                #[allow(unreachable_code)]
                Ok::<_, anyhow::Error>(())
            });
        }
        // Every storm session holds its first job before the storm starts.
        tokio::time::timeout(Duration::from_secs(120), async {
            while fixture.stratum.stats.snapshot(0).job_delivery_successes < STORM_SESSIONS as u64 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("the storm sessions never all received work")?;
        let started = delivered.load(Ordering::Relaxed);
        let refresh = fixture.coordinator.refresh.clone();
        let signal = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                refresh.send_modify(|_| {});
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
        // Let the storm fill the admission queues.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut overtaken = Vec::with_capacity(NEW_SESSIONS);
        let mut pending = 0;
        for index in 0..NEW_SESSIONS {
            pending = pending.max(fixture.stratum.stats.snapshot(0).pending_builds);
            let mut client = fixture.connect();
            client.subscribe().await?;
            // Counted from before the authorize that starts the first job.
            let before = delivered.load(Ordering::Relaxed);
            let waited = Instant::now();
            client.authorize(&format!("qbrt1new.w{index}")).await?;
            tokio::time::timeout(Duration::from_secs(60), client.first_job())
                .await
                .with_context(|| format!("new session {index} never received work"))??;
            overtaken.push((delivered.load(Ordering::Relaxed) - before, waited.elapsed()));
        }
        drop(signal);
        let storm_deliveries = delivered.load(Ordering::Relaxed) - started;
        while let Some(reader) = readers.try_join_next() {
            match reader {
                Ok(Err(error)) => bail!("a storm session failed: {error:#}"),
                Err(error) => bail!("a storm session's reader ended: {error}"),
                Ok(Ok(())) => {}
            }
        }
        readers.abort_all();
        println!(
            "#604: {STORM_SESSIONS} storm sessions delivered {storm_deliveries} rebuilds, with \
             up to {pending} deliveries pending, while {NEW_SESSIONS} sessions opened; storm \
             deliveries ahead of each first job and its wait: {overtaken:?}"
        );
        ensure!(
            storm_deliveries >= STORM_SESSIONS as u64 && pending >= STORM_SESSIONS / 2,
            "the storm delivered {storm_deliveries} rebuilds with at most {pending} pending, so it \
             proves nothing"
        );
        let slowest = overtaken.iter().map(|(count, _)| *count).max().unwrap_or(0);
        if slowest >= OVERTAKEN_BOUND {
            return Err(anyhow!(
                "a first job waited for {slowest} storm deliveries (bound {OVERTAKEN_BOUND}): \
                 first jobs queue behind the rebuild storm; per session: {overtaken:?}"
            ));
        }
        Ok(())
    }
    .await;
    settle(result, fixture.close().await)
}
