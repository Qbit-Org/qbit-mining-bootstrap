//! Opt-in native-process end-to-end tests against a real qbit regtest node.
//! QBITD_BIN=/path/to/qbitd PRISM_TEST_DATABASE_URL=postgres://... cargo test -p qbit-prism-server --test live_regtest -- --nocapture
use anyhow::{bail, ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{verify_audit_bundle_against_coinbase_tx_hex, AuditBundle};
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::{
    fs::File,
    future::Future,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

#[path = "support/live_highdiff.rs"]
mod highdiff_tests;

#[path = "support/live_ctv_cpfp.rs"]
mod cpfp_tests;

struct Process {
    child: Child,
    log: PathBuf,
}
impl Process {
    fn spawn(command: &mut Command, path: PathBuf) -> Result<Self> {
        let log = File::create(&path)?;
        let child = command
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        Ok(Self { child, log: path })
    }
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    admin: PgPool,
    pool: PgPool,
    schema: String,
    database_url: String,
    rpc_port: u16,
    stratum: [u16; 2],
    highdiff: [u16; 2],
    api: [u16; 2],
    servers: Vec<Process>,
    miners: Vec<Process>,
    node: Process,
    client: reqwest::Client,
    address: String,
    ctv: bool,
}

fn free_port() -> Result<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}
async fn until<F, Fut>(label: &str, seconds: u64, mut condition: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let started = Instant::now();
    let mut last = None;
    loop {
        match condition().await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => last = Some(error),
        }
        if started.elapsed() > Duration::from_secs(seconds) {
            bail!(
                "timed out waiting for {label}: {}",
                last.map_or_else(|| "condition not met".into(), |e| e.to_string())
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

impl Fixture {
    async fn open(ctv: bool) -> Result<Option<Self>> {
        let (Ok(binary), Ok(database)) = (
            std::env::var("QBITD_BIN"),
            std::env::var("PRISM_TEST_DATABASE_URL"),
        ) else {
            eprintln!("skipping live regtest; set QBITD_BIN and PRISM_TEST_DATABASE_URL");
            return Ok(None);
        };
        let directory = tempfile::tempdir()?;
        let admin = PgPool::connect(&database).await?;
        let schema = format!("prism_live_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&database)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let database_url = url.to_string();
        let pool = PgPool::connect(&database_url).await?;
        let rpc_port = free_port()?;
        let mut node_command = Command::new(binary);
        node_command
            .args([
                "-regtest",
                "-server=1",
                "-listen=0",
                "-dnsseed=0",
                "-discover=0",
                "-fallbackfee=0.00001",
                "-rpcuser=prismtest",
                "-rpcpassword=prismtest",
                "-txindex=0",
            ])
            .arg(format!("-datadir={}", directory.path().display()))
            .arg(format!("-rpcport={rpc_port}"))
            .arg(format!("-port={}", free_port()?));
        let node = Process::spawn(&mut node_command, directory.path().join("qbit.log"))?;
        let mut fixture = Self {
            directory,
            admin,
            pool,
            schema,
            database_url,
            rpc_port,
            stratum: [free_port()?, free_port()?],
            highdiff: [free_port()?, free_port()?],
            api: [free_port()?, free_port()?],
            servers: Vec::new(),
            miners: Vec::new(),
            node,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(45))
                .build()?,
            address: String::new(),
            ctv,
        };
        until("qbit RPC", 30, || async {
            Ok(fixture
                .rpc("getblockchaininfo", json!([]))
                .await?
                .get("chain")
                == Some(&json!("regtest")))
        })
        .await?;
        fixture.rpc("createwallet", json!(["prism"])).await?;
        fixture.address = fixture
            .rpc("getnewaddress", json!(["", "p2mr"]))
            .await?
            .as_str()
            .context("wallet address missing")?
            .into();
        fixture
            .rpc("generatetoaddress", json!([1, fixture.address]))
            .await?;
        for index in 0..2 {
            let process = fixture.start_server(index)?;
            fixture.servers.push(process);
        }
        for index in 0..2 {
            until("PRISM HTTP readiness", 30, || async {
                Ok(fixture
                    .client
                    .get(format!("http://127.0.0.1:{}/healthz", fixture.api[index]))
                    .send()
                    .await?
                    .status()
                    .is_success())
            })
            .await?;
        }
        Ok(Some(fixture))
    }

    fn start_server(&self, index: usize) -> Result<Process> {
        self.start_server_with_sponsorship(index, None)
    }

    fn start_server_with_sponsorship(&self, index: usize, fee: Option<u64>) -> Result<Process> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
        // Inherited operator PRISM settings must not alter a disposable test.
        for (name, _) in std::env::vars()
            .filter(|(name, _)| name.starts_with("PRISM_") || name.starts_with("QBIT_"))
        {
            command.env_remove(name);
        }
        command
            .env("PRISM_DATABASE_URL", &self.database_url)
            .env("PRISM_POSTGRES_INIT_SCHEMA", "1")
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_INSTANCE_ID", format!("live-{index}"))
            .env("QBIT_CHAIN", "regtest")
            .env("QBIT_RPC_HOST", "127.0.0.1")
            .env("QBIT_RPC_PORT", self.rpc_port.to_string())
            .env("QBIT_RPC_USER", "prismtest")
            .env("QBIT_RPC_PASSWORD", "prismtest")
            .env("PRISM_STRATUM_BIND", "127.0.0.1")
            .env("PRISM_STRATUM_PORT", self.stratum[index].to_string())
            .env(
                "PRISM_STRATUM_HIGHDIFF_PORT",
                self.highdiff[index].to_string(),
            )
            .env("PRISM_AUDIT_PORT", self.api[index].to_string())
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("PRISM_BLOCKPOLL_SECONDS", "0.2")
            .env("PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS", "1")
            .env("PRISM_PUBLIC_CACHE_ENABLED", "0")
            .env("RUST_LOG", "warn");
        if self.ctv {
            command
                .env("PRISM_CTV_SETTLEMENT_ENABLED", "1")
                .env("PRISM_CTV_BROADCASTER_ENABLED", "1")
                .env("PRISM_CTV_BROADCASTER_POLL_SECONDS", "0.2")
                .env("PRISM_CTV_SPEND_SCAN_BLOCKS", "1")
                .env("PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS", "1000000000000")
                .env(
                    "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
                    "1000",
                );
        }
        if let Some(fee) = fee {
            command
                .env("PRISM_CTV_BROADCASTER_WALLET", "prism")
                .env("PRISM_CTV_BROADCASTER_FEE_BITS", fee.to_string());
        }
        Process::spawn(
            &mut command,
            self.directory.path().join(format!("server-{index}.log")),
        )
    }

    fn start_miner(&mut self, index: usize) -> Result<()> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-miner"));
        command.args([
            "--address",
            &format!("127.0.0.1:{}", self.stratum[index]),
            "--username",
            &format!("{}.live-{index}", self.address),
            "--threads",
            "1",
            "--hashes-per-second",
            "8",
            "--duration-seconds",
            "90",
            "--pause-after-share-ms",
            "150",
        ]);
        self.miners.push(Process::spawn(
            &mut command,
            self.directory
                .path()
                .join(format!("miner-{index}-{}.json", self.miners.len())),
        )?);
        Ok(())
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let payload: Value = self
            .client
            .post(format!("http://127.0.0.1:{}/", self.rpc_port))
            .basic_auth("prismtest", Some("prismtest"))
            .json(&json!({"jsonrpc":"1.0","id":"live-test","method":method,"params":params}))
            .send()
            .await?
            .json()
            .await?;
        ensure!(
            payload["error"].is_null(),
            "RPC {method}: {}",
            payload["error"]
        );
        Ok(payload["result"].clone())
    }

    async fn count(&self, writer: usize) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM qbit_share_ledger WHERE accepted AND writer_id=$1",
        )
        .bind(format!("live-{writer}"))
        .fetch_one(&self.pool)
        .await?)
    }

    async fn subscription(&self, index: usize) -> Result<String> {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", self.stratum[index])).await?;
        let (read, mut write) = stream.into_split();
        write
            .write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n")
            .await?;
        let mut lines = BufReader::new(read).lines();
        loop {
            let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
                .await??
                .context("subscription closed")?;
            let value: Value = serde_json::from_str(&line)?;
            if value["id"] == 1 {
                return Ok(value["result"][1]
                    .as_str()
                    .context("extranonce missing")?
                    .into());
            }
        }
    }

    async fn quiesce(&mut self) -> Result<()> {
        for miner in &mut self.miners {
            miner.stop();
        }
        // SIGKILL leaves a real 120-second candidate lease behind. Let the
        // surviving process reclaim it through the production expiry path.
        until("candidate outbox drain", 140, || async {
            Ok(sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state='pending'",
            )
            .fetch_one(&self.pool)
            .await?
                == 0)
        })
        .await
    }

    async fn integrity(&self) -> Result<()> {
        let report: Value = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
            .fetch_one(&self.pool)
            .await?;
        ensure!(
            report["mismatch_count"] == 0 && report["current_drift_count"] == 0,
            "carry integrity failed: {report}"
        );
        Ok(())
    }

    async fn assert_public_block_bits(&self, hash: &str, expected: &Value) -> Result<()> {
        ensure!(
            expected
                .as_str()
                .is_some_and(|bits| bits.len() == 8 && bits != "00000000"),
            "node did not provide valid compact bits"
        );
        let stored: Option<String> = sqlx::query_scalar(
            "SELECT found_block_bits FROM qbit_pool_audit_bundles WHERE block_hash=$1",
        )
        .bind(hash)
        .fetch_one(&self.pool)
        .await?;
        ensure!(
            stored.as_deref() == expected.as_str(),
            "durable block bits differ from node"
        );
        for port in self.api {
            let response: Value = self
                .client
                .get(format!(
                    "http://127.0.0.1:{port}/public/v1/blocks?limit=100"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let row = response["rows"]
                .as_array()
                .context("public block rows missing")?
                .iter()
                .find(|row| row["hash"] == hash)
                .context("confirmed block missing from public response")?;
            ensure!(
                &row["bits"] == expected,
                "public block bits differ from node: {row}"
            );
        }
        Ok(())
    }

    async fn cleanup(mut self) -> Result<()> {
        for miner in &mut self.miners {
            miner.stop();
        }
        for server in &mut self.servers {
            server.stop();
        }
        self.node.stop();
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }

    fn diagnostics(&self) -> String {
        self.servers
            .iter()
            .chain(self.miners.iter())
            .map(|process| {
                let text = std::fs::read_to_string(&process.log).unwrap_or_default();
                let lines: Vec<_> = text.lines().rev().take(15).collect();
                format!(
                    "{}:\n{}",
                    process.log.display(),
                    lines.into_iter().rev().collect::<Vec<_>>().join("\n")
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_two_server_mining_failover_audit_and_reorg() -> Result<()> {
    let Some(mut fixture) = Fixture::open(false).await? else {
        return Ok(());
    };
    let result=async {
        let a=fixture.subscription(0).await?;let b=fixture.subscription(1).await?;ensure!(a!=b,"servers reused extranonce1");
        fixture.start_miner(0)?;fixture.start_miner(1)?;
        until("accepted shares on both servers",30,||async {Ok(fixture.count(0).await?>2&&fixture.count(1).await?>2)}).await?;
        fixture.servers[0].stop();fixture.miners[0].stop();
        let before=fixture.count(1).await?;
        until("surviving server mining",20,||async {Ok(fixture.count(1).await?>before+2)}).await?;
        fixture.servers[0]=fixture.start_server(0)?;
        until("restarted Stratum listener",20,||async {Ok(tokio::net::TcpStream::connect(("127.0.0.1",fixture.stratum[0])).await.is_ok())}).await?;
        let restarted=fixture.subscription(0).await?;ensure!(restarted!=a&&restarted!=b,"restart reused session extranonce");
        let before=fixture.count(0).await?;fixture.start_miner(0)?;
        until("restarted server mining",20,||async {Ok(fixture.count(0).await?>before+2)}).await?;
        fixture.quiesce().await?;
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE accepted").fetch_one(&fixture.pool).await?;
        let unique:i64=sqlx::query_scalar("SELECT count(*) FROM qbit_prism_share_hashes").fetch_one(&fixture.pool).await?;ensure!(count==unique,"duplicate headers credited");
        for index in 0..2 {
            let latest:Value=fixture.client.get(format!("http://127.0.0.1:{}/audit/latest",fixture.api[index])).send().await?.error_for_status()?.json().await?;
            ensure!(latest["accepted_share_count"].as_i64()==Some(count),"API does not expose cluster share count: {latest}");
        }
        let hash:String=sqlx::query_scalar("SELECT block_hash FROM qbit_pool_blocks WHERE chain_state='confirmed' ORDER BY block_height DESC LIMIT 1").fetch_one(&fixture.pool).await?;
        let body:Value=fixture.client.get(format!("http://127.0.0.1:{}/audit/blocks/{hash}/bundle",fixture.api[1])).send().await?.error_for_status()?.json().await?;
        let bundle:AuditBundle=serde_json::from_value(body["audit_bundle"].clone())?;
        let block=fixture.rpc("getblock",json!([hash,2])).await?;
        fixture.assert_public_block_bits(&hash,&block["bits"]).await?;
        let coinbase=fixture.rpc("getrawtransaction",json!([block["tx"][0]["txid"],false,hash])).await?;
        let key=ManifestSigningKey::from_seed_hex(&"22".repeat(32))?.public_key_hex();
        verify_audit_bundle_against_coinbase_tx_hex(&bundle,coinbase.as_str().context("node coinbase missing")?,&key)?;
        fixture.rpc("invalidateblock",json!([hash])).await?;
        let replacement_address=fixture.rpc("getnewaddress",json!(["","p2mr"])).await?;
        let replacement=fixture.rpc("generatetoaddress",json!([2,replacement_address])).await?;
        until("immature pool block disconnect",20,||async {Ok(sqlx::query_scalar::<_,String>("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1").bind(&hash).fetch_one(&fixture.pool).await?=="inactive")}).await?;
        fixture.integrity().await?;
        fixture.rpc("invalidateblock",json!([replacement[0]])).await?;
        fixture.rpc("reconsiderblock",json!([hash])).await?;
        let restoration_address=fixture.rpc("getnewaddress",json!(["","p2mr"])).await?;
        fixture.rpc("generatetoaddress",json!([2,restoration_address])).await?;
        until("pool block reconnection",20,||async {Ok(sqlx::query_scalar::<_,String>("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1").bind(&hash).fetch_one(&fixture.pool).await?=="confirmed")}).await?;
        fixture.assert_public_block_bits(&hash,&block["bits"]).await?;
        fixture.integrity().await?;
        eprintln!("live regtest: {count} committed shares across two processes; failover/restart, unique extranonces, actual-coinbase audit and disconnect/reconnect verified");
        Ok::<_,anyhow::Error>(())
    }.await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_ctv_maturity_broadcast_and_confirmation() -> Result<()> {
    let Some(mut fixture) = Fixture::open(true).await? else {
        return Ok(());
    };
    let result=async {
        fixture.start_miner(0)?;
        until("mined CTV pool block",30,||async {Ok(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE b.chain_state='confirmed'").fetch_one(&fixture.pool).await?>0)}).await?;
        fixture.quiesce().await?;
        let row=sqlx::query("SELECT a.fanout_txid,b.block_height FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE b.chain_state='confirmed' ORDER BY b.block_height LIMIT 1").fetch_one(&fixture.pool).await?;
        let txid:String=row.try_get("fanout_txid")?;
        let height:i64=row.try_get("block_height")?;
        let tip=fixture.rpc("getblockcount",json!([])).await?.as_i64().context("tip missing")?;
        fixture.rpc("generatetoaddress",json!([height+1000-tip,fixture.address])).await?;
        until("CTV fanout in node mempool",40,||async {Ok(fixture.rpc("getrawmempool",json!([])).await?.as_array().is_some_and(|rows|rows.contains(&json!(txid))))}).await?;
        fixture.rpc("generatetoaddress",json!([1,fixture.address])).await?;
        until("CTV confirmation read model",45,||async {Ok(sqlx::query_scalar::<_,String>("SELECT settlement_status FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).fetch_one(&fixture.pool).await?=="confirmed")}).await?;
        let attempts:i64=sqlx::query_scalar("SELECT broadcast_attempt_count FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).fetch_one(&fixture.pool).await?;
        ensure!(attempts>=1,"broadcast attempts were not persisted");
        let confirmed_hash:String=sqlx::query_scalar("SELECT confirmed_block_hash FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).fetch_one(&fixture.pool).await?;
        let scan_cursor:Option<i64>=sqlx::query_scalar("SELECT spend_scan_next_height FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).fetch_one(&fixture.pool).await?;
        ensure!(scan_cursor.is_some(),"no-txindex confirmation failed to persist its bounded scan cursor");
        fixture.rpc("invalidateblock",json!([confirmed_hash])).await?;
        let reorg_address=fixture.rpc("getnewaddress",json!(["","p2mr"])).await?;
        // Mine a stronger branch without the still-pending fanout so its
        // disconnection is observable before a later block reconfirms it.
        for _ in 0..2 {fixture.rpc("generateblock",json!([reorg_address,[]])).await?;}
        until("CTV confirmation disconnect detected",20,||async {Ok(sqlx::query_scalar::<_,String>("SELECT settlement_status FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).fetch_one(&fixture.pool).await?!="confirmed")}).await?;
        until("disconnected CTV fanout recovered in mempool",20,||async {Ok(fixture.rpc("getrawmempool",json!([])).await?.as_array().is_some_and(|rows|rows.contains(&json!(txid))))}).await?;
        fixture.rpc("generatetoaddress",json!([1,reorg_address])).await.context("mining replacement fanout confirmation")?;
        until("CTV confirmation recovered after reorg",30,||async {Ok(sqlx::query_scalar::<_,bool>("SELECT settlement_status='confirmed' AND confirmed_block_hash<>$2 FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&txid).bind(&confirmed_hash).fetch_one(&fixture.pool).await?)}).await?;
        fixture.integrity().await?;
        eprintln!("live CTV regtest: mature covenant {txid} broadcast, confirmed without txindex, and recovered after confirmation reorg; {attempts} initial durable attempts across two broadcaster processes");
        Ok::<_,anyhow::Error>(())
    }.await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}
