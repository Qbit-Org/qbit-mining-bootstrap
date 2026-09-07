use super::*;
use num_bigint::BigUint;
use qbit_prism_server::codec::{
    difficulty_target, double_sha256, hash_display, parse_u32_hex, scaled_target_difficulty,
    target_from_compact,
};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

struct HighdiffClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    buffer: Vec<u8>,
    username: String,
    extranonce1: String,
    extranonce2_size: usize,
    difficulty: f64,
    notify: Value,
}

impl HighdiffClient {
    async fn open(fixture: &Fixture, index: usize) -> Result<Self> {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", fixture.highdiff[index])).await?;
        let (read, write) = stream.into_split();
        let mut client = Self {
            reader: BufReader::new(read),
            writer: write,
            buffer: Vec::new(),
            username: format!("{}.highdiff", fixture.address),
            extranonce1: String::new(),
            extranonce2_size: 0,
            difficulty: 0.0,
            notify: Value::Null,
        };
        client
            .send(json!({"id":1,"method":"mining.subscribe","params":["highdiff-regtest"]}))
            .await?;
        client
            .send(json!({"id":2,"method":"mining.authorize","params":[client.username,"x"]}))
            .await?;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let message = client.read().await?;
                ensure!(
                    message.get("error").is_none_or(Value::is_null),
                    "highdiff handshake rejected: {message}"
                );
                if message["id"] == 1 {
                    client.extranonce1 = message["result"][1]
                        .as_str()
                        .context("extranonce1 missing")?
                        .into();
                    client.extranonce2_size = message["result"][2]
                        .as_u64()
                        .context("extranonce2 size missing")?
                        .try_into()?;
                }
                if message["method"] == "mining.notify" {
                    break;
                }
            }
            ensure!(
                client.difficulty >= 500_000.0,
                "first highdiff advertisement below floor: {}",
                client.difficulty
            );
            ensure!(
                !client.extranonce1.is_empty() && client.extranonce2_size > 0,
                "subscription entropy missing"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(client)
    }

    async fn send(&mut self, payload: Value) -> Result<()> {
        self.writer
            .write_all(format!("{payload}\n").as_bytes())
            .await?;
        Ok(())
    }

    async fn read(&mut self) -> Result<Value> {
        ensure!(
            self.reader.read_until(b'\n', &mut self.buffer).await? > 0,
            "highdiff connection closed"
        );
        ensure!(
            self.buffer.len() < 2 * 1024 * 1024,
            "oversized highdiff message"
        );
        let message: Value = serde_json::from_slice(&self.buffer)?;
        self.buffer.clear();
        if message["method"] == "mining.set_difficulty" {
            self.difficulty = message["params"][0]
                .as_f64()
                .context("invalid difficulty")?;
        }
        if message["method"] == "mining.notify" {
            self.notify = message.clone();
        }
        Ok(message)
    }

    async fn response(&mut self, id: u64) -> Result<Value> {
        loop {
            let value = self.read().await?;
            if value["id"] == id {
                return Ok(value);
            }
        }
    }

    fn solve(&self, id: u64, future_time: bool) -> Result<(Value, String, u128)> {
        let params = self.notify["params"].as_array().context("notify missing")?;
        let field = |index: usize| params[index].as_str().context("invalid notify field");
        let extranonce2 = "00".repeat(self.extranonce2_size);
        let coinbase = hex::decode(format!(
            "{}{}{extranonce2}{}",
            field(2)?,
            self.extranonce1,
            field(3)?
        ))?;
        let mut merkle = double_sha256(&coinbase);
        for sibling in params[4].as_array().context("merkle branch missing")? {
            let sibling = hex::decode(sibling.as_str().context("invalid sibling")?)?;
            ensure!(sibling.len() == 32, "invalid sibling length");
            merkle = double_sha256(&[merkle.as_slice(), sibling.as_slice()].concat());
        }
        let mut previous = hex::decode(field(1)?)?;
        for word in previous.chunks_exact_mut(4) {
            word.reverse();
        }
        let bits = parse_u32_hex(field(6)?)?;
        let network_target = target_from_compact(bits)?;
        let share_target = difficulty_target(self.difficulty)?;
        ensure!(
            share_target < network_target,
            "test requires highdiff floor harder than network"
        );
        let ntime = if future_time {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs() as u32
                + 4 * 60 * 60
        } else {
            parse_u32_hex(field(7)?)?
        };
        let version = parse_u32_hex(field(5)?)?;
        for nonce in 0..10_000u32 {
            let header = [
                version.to_le_bytes().as_slice(),
                previous.as_slice(),
                merkle.as_slice(),
                ntime.to_le_bytes().as_slice(),
                bits.to_le_bytes().as_slice(),
                nonce.to_le_bytes().as_slice(),
            ]
            .concat();
            let hash = double_sha256(&header);
            let value = BigUint::from_bytes_le(&hash);
            if value <= network_target && value > share_target {
                let request = json!({"id":id,"method":"mining.submit","params":[self.username,field(0)?,extranonce2,format!("{ntime:08x}"),format!("{nonce:08x}")]});
                return Ok((
                    request,
                    hash_display(&hash),
                    scaled_target_difficulty(&network_target)?,
                ));
            }
        }
        bail!("no regtest block-only proof in constrained nonce budget")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_highdiff_block_only_proof_waits_for_active_chain_credit() -> Result<()> {
    let Some(fixture) = Fixture::open(false).await? else {
        return Ok(());
    };
    let result=async {
        let mut miner=HighdiffClient::open(&fixture,0).await?;
        let (request,block_hash,network_work)=miner.solve(10,false)?;
        let share_id=format!("{}:{block_hash}",miner.username);
        // Pause candidate claims independently of job/share persistence.
        // This proves an outbox write alone cannot produce a block-only ACK
        // without blocking a concurrent replacement job on this connection.
        let pause_key=Uuid::new_v4().as_u128() as i64;
        sqlx::raw_sql(&format!("CREATE FUNCTION test_pause_candidate_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.claim_token IS NOT NULL AND NEW.claim_token IS DISTINCT FROM OLD.claim_token THEN PERFORM pg_advisory_xact_lock({pause_key}); END IF; RETURN NEW; END $$; CREATE TRIGGER test_pause_candidate_claim BEFORE UPDATE OF claim_token ON qbit_block_candidate_outbox FOR EACH ROW EXECUTE FUNCTION test_pause_candidate_claim();")).execute(&fixture.pool).await?;
        let mut settlement=fixture.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(pause_key).execute(&mut *settlement).await?;
        miner.send(request).await?;
        until("durable highdiff candidate intent",5,||async {
            Ok(sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE block_hash=$1 AND state='pending')")
                .bind(&block_hash).fetch_one(&fixture.pool).await?)
        }).await?;
        let before:i64=sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1").bind(&share_id).fetch_one(&fixture.pool).await?;
        ensure!(before==0,"block-only proof was credited before active-chain finalization");
        ensure!(tokio::time::timeout(Duration::from_millis(250),miner.response(10)).await.is_err(),"block-only proof ACKed before finalization");
        settlement.commit().await?;
        let response=tokio::time::timeout(Duration::from_secs(15),miner.response(10)).await??;
        ensure!(response["result"]==true && response["error"].is_null(),"valid highdiff block rejected: {response}");
        let row=sqlx::query("SELECT s.share_difficulty::text AS work,s.network_difficulty::text AS network,b.chain_state,s.accepted FROM qbit_share_ledger s JOIN qbit_pool_blocks b ON b.block_hash=$2 WHERE s.share_id=$1")
            .bind(&share_id).bind(&block_hash).fetch_one(&fixture.pool).await?;
        ensure!(row.try_get::<bool,_>("accepted")? && row.try_get::<String,_>("chain_state")?=="confirmed","ACK preceded durable active-chain credit");
        ensure!(row.try_get::<String,_>("work")?==network_work.to_string() && row.try_get::<String,_>("network")?==network_work.to_string(),"block-only proof credited assigned highdiff instead of proven network work");
        let block=fixture.rpc("getblockheader",json!([block_hash])).await?;
        ensure!(block["confirmations"].as_i64().unwrap_or(0)>0,"ACKed block is not on the active chain");
        drop(miner);

        // A valid PoW with an invalid future timestamp reaches submitblock,
        // whose terminal rejection must produce no accepted share or ACK.
        let mut miner=HighdiffClient::open(&fixture,1).await?;
        let (request,rejected_hash,_)=miner.solve(11,true)?;
        miner.send(request).await?;
        let response=tokio::time::timeout(Duration::from_secs(15),miner.response(11)).await??;
        ensure!(response["result"]!=true && !response["error"].is_null(),"node-rejected block-only proof received an accepted ACK");
        let rejected_share_id=format!("{}:{rejected_hash}",miner.username);
        let credits:i64=sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1").bind(&rejected_share_id).fetch_one(&fixture.pool).await?;
        ensure!(credits==0,"rejected block-only proof inflated share accounting");
        let disposition=sqlx::query("SELECT state,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1").bind(&rejected_hash).fetch_one(&fixture.pool).await?;
        ensure!(disposition.try_get::<String,_>("state")?=="abandoned","rejected candidate not terminal");
        ensure!(disposition.try_get::<Option<String>,_>("last_error")?.is_some_and(|e|e.contains("time-too-new")),"test did not reach node timestamp rejection");
        fixture.integrity().await?;
        eprintln!("live highdiff: withheld ACK until durable active-chain credit; credited exactly {network_work} network work; rejected future block received no ACK/credit");
        Ok::<_,anyhow::Error>(())
    }.await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

fn direct_coordinator_config(fixture: &Fixture) -> Result<qbit_prism_server::config::Config> {
    use qbit_prism_server::config::Config;
    Ok(Config {
        database_url: fixture.database_url.clone(),
        instance_id: "live-observed-tip".into(),
        database_connections: 4,
        initialize_schema: false,
        chain: "regtest".into(),
        rpc_url: format!("http://127.0.0.1:{}/", fixture.rpc_port),
        rpc_user: "prismtest".into(),
        rpc_password: "prismtest".into(),
        rpc_timeout: Duration::from_secs(10),
        poll_interval: Duration::from_secs(1),
        blockwait: false,
        build_workers: 2,
        runtime_workers: 2,
        snapshot_interval: Duration::from_secs(30),
        health_timeout: Duration::from_secs(15),
        share_commit_timeout: Duration::from_secs(15),
        extranonce2_size: 8,
        coinbase_tag: "/PRISM/".into(),
        manifest_seed: "11".repeat(32),
        ledger_seed: "22".repeat(32),
        ledger_public_key: ManifestSigningKey::from_seed_hex(&"22".repeat(32))?.public_key_hex(),
        username_fallback: None,
        payout_policy: qbit_prism::PayoutPolicy::day_one_default(),
        fee_address: None,
        ctv_enabled: false,
        ctv_config: qbit_prism::SettlementModeConfig::default(),
        ctv_direct_floor: 10_485_760,
        ctv_fee: None,
        ctv_broadcast: false,
        ctv_broadcast_interval: Duration::from_secs(10),
        version_mask: qbit_prism_server::codec::VERSION_ROLLING_MASK,
        audit_bind: "127.0.0.1".into(),
        audit_port: free_port()?,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observed_tip_advance_fences_old_prepared_jobs_and_health() -> Result<()> {
    use qbit_prism_server::{coordinator::Coordinator, stratum::MiningBackend};
    let Some(fixture) = Fixture::open(false).await? else {
        return Ok(());
    };
    let result=async {
        let coordinator=Coordinator::new(direct_coordinator_config(&fixture)?).await?;
        coordinator.refresh_once().await?;
        ensure!(coordinator.health().await["ready"]==true,"initial published template is not healthy");
        let worker=coordinator.authorize(&format!("{}.fence",fixture.address)).await?;
        let session=coordinator.new_session_id().await?;
        let job=coordinator.build_job(&worker,&format!("{session:08x}"),1e-9,0.0).await?;
        coordinator.persist_issued_job(&worker,&job,0,Duration::from_secs(30)).await?;
        let submission=(0..10_000u32).find_map(|nonce| {
            let proof=job.wire.assemble_submission(&"00".repeat(8),&format!("{:08x}",job.wire.ntime),&format!("{nonce:08x}"),None,0).ok()?;
            proof.share_pass.then_some(proof)
        }).context("no constrained regtest proof")?;
        let original=job.wire.previousblockhash.clone();
        // This is the state between observing a validated new tip and
        // successfully building/publishing its payout work. A failed builder
        // must not leave the older immutable prepared object authoritative.
        *coordinator.observed_tip.write().await=Some("fe".repeat(32));
        ensure!(coordinator.health().await["ready"]==false,"stale prepared work remained healthy after tip observation");
        ensure!(coordinator.build_job(&worker,&format!("{session:08x}"),1e-9,0.0).await.is_err(),"new miner received work for superseded observed tip");
        ensure!(coordinator.resume_job(&worker,&job.wire.job_id).await.is_err(),"reconnect restored work while new observed tip was pending");
        let rejected=coordinator.submit(&worker,&job,submission,false).await.err().context("old share was accepted while new tip publication was pending")?;
        ensure!(rejected.reason_id.as_deref()==Some("stale-job"),"wrong stale tip rejection: {rejected}");
        let credits:i64=sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE accepted").fetch_one(&fixture.pool).await?;
        ensure!(credits==0,"untrusted cached job inflated share accounting");
        *coordinator.observed_tip.write().await=Some(original);
        ensure!(coordinator.health().await["ready"]==true,"restoring tip authority did not restore health");
        coordinator.ledger.pool.close().await;
        eprintln!("observed-tip regression: health, new jobs, resume and submitted shares all fenced during unpublished tip transition");
        Ok::<_,anyhow::Error>(())
    }.await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}
