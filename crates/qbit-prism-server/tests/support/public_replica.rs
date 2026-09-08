use super::*;
use serde_json::Value;
use std::{
    fs::File,
    io::Write,
    process::{Child, Stdio},
};

struct PublicProcess {
    child: Child,
    log: PathBuf,
    url: String,
}
impl PublicProcess {
    fn start(database: &str, port: u16, log: PathBuf) -> Result<Self> {
        let output = File::create(&log)?;
        let child = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"))
            .arg("public-api")
            // The public role must start independently of all signing keys,
            // mining configuration, node credentials and inherited writer DSNs.
            .env_clear()
            .env("PRISM_DATABASE_URL", database)
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("PRISM_PUBLIC_API_BIND", "127.0.0.1")
            .env("PRISM_PUBLIC_API_PORT", port.to_string())
            .env("PRISM_PUBLIC_STRATUM_URL", "stratum+tcp://127.0.0.1:3340")
            .env("PRISM_PUBLIC_REPLICA_MODE", "require")
            .env("PRISM_PUBLIC_REPLICA_MAX_LAG_SECONDS", "2")
            .env("PRISM_PUBLIC_READINESS_PROBE_INTERVAL_SECONDS", "0.1")
            .env("PRISM_POSTGRES_READ_CONCURRENCY", "2")
            .env("PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS", "2")
            .env("PRISM_PUBLIC_CACHE_ENABLED", "0")
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(output.try_clone()?))
            .stderr(Stdio::from(output))
            .spawn()?;
        Ok(Self {
            child,
            log,
            url: format!("http://127.0.0.1:{port}"),
        })
    }

    async fn health_until(
        &mut self,
        client: &reqwest::Client,
        expected: u16,
        predicate: impl Fn(&Value) -> bool,
    ) -> Result<Value> {
        let mut last = String::new();
        let result = timeout(Duration::from_secs(20), async {
            loop {
                if let Some(status) = self.child.try_wait()? {
                    anyhow::bail!("public process exited: {status}")
                }
                if let Ok(response) = client.get(format!("{}/healthz", self.url)).send().await {
                    let status = response.status().as_u16();
                    let body: Value = response.json().await?;
                    if status == expected && predicate(&body) {
                        return Ok::<_, anyhow::Error>(body);
                    }
                    last = format!("HTTP {status}: {body}");
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        result.with_context(|| {
            format!(
                "public readiness timeout; last={last}; log={}",
                std::fs::read_to_string(&self.log).unwrap_or_default()
            )
        })?
    }
}
impl Drop for PublicProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct PausedWalSender(i32);
impl PausedWalSender {
    fn pause(pid: i32) -> Result<Self> {
        let status = Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status()?;
        ensure!(status.success(), "could not suspend disposable WAL sender");
        Ok(Self(pid))
    }
}
impl Drop for PausedWalSender {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-CONT", &self.0.to_string()])
            .status();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_public_service_tracks_real_standby_stream_and_refuses_promotion() -> Result<()> {
    let Ok(bin) = std::env::var("PRISM_TEST_PG_BIN_DIR") else {
        eprintln!("skipping physical public-read replica test; set PRISM_TEST_PG_BIN_DIR");
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let primary = Cluster {
        bin: bin.clone().into(),
        data: dir.path().join("primary"),
    };
    let standby = Cluster {
        bin: bin.into(),
        data: dir.path().join("standby"),
    };
    primary.command(
        "initdb",
        &[
            "-D",
            primary.data.to_str().unwrap(),
            "-A",
            "trust",
            "--no-locale",
            "-E",
            "UTF8",
        ],
    )?;
    // Short real WAL keepalives make a two-second heartbeat budget meaningful
    // even while the pool is idle. These settings are copied by basebackup.
    writeln!(std::fs::OpenOptions::new().append(true).open(primary.data.join("postgresql.conf"))?,"\nwal_sender_timeout = '1s'\nwal_receiver_status_interval = '1s'\nwal_retrieve_retry_interval = '100ms'")?;
    let primary_port = port()?;
    let standby_port = port()?;
    primary.start(primary_port, dir.path())?;
    let username = String::from_utf8(Command::new("id").arg("-un").output()?.stdout)?
        .trim()
        .to_owned();
    let primary_url = format!("postgresql://{username}@127.0.0.1:{primary_port}/postgres");
    let writer = Ledger::connect(&primary_url, "only-writer".into(), 4, true).await?;
    writer.append(proof(1), None).await?;
    sqlx::raw_sql("CREATE ROLE public_reader LOGIN; GRANT CONNECT ON DATABASE postgres TO public_reader; GRANT USAGE ON SCHEMA public TO public_reader; GRANT SELECT ON ALL TABLES IN SCHEMA public TO public_reader; GRANT pg_monitor TO public_reader; ALTER ROLE public_reader SET default_transaction_read_only=on;").execute(&writer.pool).await?;
    standby.command("pg_basebackup",&["-D",standby.data.to_str().unwrap(),"-d",&format!("host=127.0.0.1 port={primary_port} user={username} application_name=public_test_standby"),"-R","-X","stream","-c","fast"])?;
    standby.start(standby_port, dir.path())?;
    let standby_url = format!("postgresql://{username}@127.0.0.1:{standby_port}/postgres");
    let standby_admin = PgPool::connect(&standby_url).await?;
    timeout(Duration::from_secs(15), async {
        loop {
            if sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_wal_receiver WHERE status='streaming')",
            )
            .fetch_one(&standby_admin)
            .await?
            {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let reader_url = format!("postgresql://public_reader@127.0.0.1:{standby_port}/postgres");
    let mut public = PublicProcess::start(&reader_url, port()?, dir.path().join("public.log"))?;
    public
        .health_until(&client, 200, |v| {
            v["ok"] == true && v["replica"]["in_recovery"] == true
        })
        .await?;
    // Committed after basebackup: this row must reach the independent public
    // process through actual streaming replication, never a primary fallback.
    let hash = "ad".repeat(32);
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES($1,101,repeat('00',32),repeat('01',32),repeat('02',32),'confirmed')").bind(&hash).execute(&writer.pool).await?;
    timeout(Duration::from_secs(10), async {
        loop {
            let response = client
                .get(format!("{}/public/v1/blocks", public.url))
                .send()
                .await?;
            if response.status().is_success() {
                let body: Value = response.json().await?;
                if body["rows"]
                    .as_array()
                    .is_some_and(|rows| rows.iter().any(|row| row["hash"] == hash))
                {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;
    assert_eq!(
        client
            .get(format!("{}/status", public.url))
            .send()
            .await?
            .status()
            .as_u16(),
        404,
        "public role exposed the operator API"
    );
    assert_eq!(
        client
            .post(format!("{}/public/v1/blocks", public.url))
            .send()
            .await?
            .status()
            .as_u16(),
        405
    );
    let reader = PgPool::connect(&reader_url).await?;
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT has_table_privilege(current_user,'qbit_share_ledger','INSERT')"
        )
        .fetch_one(&reader)
        .await?
    );
    assert!(sqlx::query(
        "INSERT INTO qbit_prism_instances(instance_id) VALUES('public-must-not-write')"
    )
    .execute(&reader)
    .await
    .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_prism_instances")
            .fetch_one(&writer.pool)
            .await?,
        1
    );

    // Even valid readable schema/data on a primary cannot make require mode
    // ready. The public role needs no writer credentials to prove this.
    let mut on_primary = PublicProcess::start(
        &format!("postgresql://public_reader@127.0.0.1:{primary_port}/postgres"),
        port()?,
        dir.path().join("primary-public.log"),
    )?;
    on_primary
        .health_until(&client, 503, |v| v["replica"]["in_recovery"] == false)
        .await?;
    assert_eq!(
        client
            .get(format!("{}/public/v1/blocks", on_primary.url))
            .send()
            .await?
            .status()
            .as_u16(),
        503
    );
    drop(on_primary);

    // A connected receiver alone is insufficient: freeze its disposable
    // sender without closing the socket and let the real heartbeat age out.
    let sender: i32 = sqlx::query_scalar(
        "SELECT pid FROM pg_stat_replication WHERE application_name='public_test_standby'",
    )
    .fetch_one(&writer.pool)
    .await?;
    let paused = PausedWalSender::pause(sender)?;
    let stale = public
        .health_until(&client, 503, |v| {
            v["replica"]["receiver_heartbeat_age_seconds"]
                .as_f64()
                .is_some_and(|age| age > 2.0)
        })
        .await?;
    assert_eq!(stale["replica"]["in_recovery"], true);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pg_stat_wal_receiver")
            .fetch_one(&standby_admin)
            .await?,
        1,
        "test must exercise a connected but stale stream"
    );
    assert_eq!(
        client
            .get(format!("{}/public/v1/blocks", public.url))
            .send()
            .await?
            .status()
            .as_u16(),
        503
    );
    drop(paused);
    public
        .health_until(&client, 200, |v| v["ok"] == true)
        .await?;

    // Stop the WAL source while leaving standby SQL queries available. The
    // service must fence stale reads, then recover after streaming reconnects.
    writer.pool.close().await;
    primary.control(&["-m", "immediate", "-w", "stop"])?;
    let unready = public
        .health_until(&client, 503, |v| {
            v["replica"]["in_recovery"] == true && v["ok"] == false
        })
        .await?;
    assert!(
        unready["replica"]["receiver_heartbeat_age_seconds"].is_null()
            || unready["replica"]["receiver_heartbeat_age_seconds"]
                .as_f64()
                .unwrap_or_default()
                > 2.0
    );
    assert_eq!(
        client
            .get(format!("{}/public/v1/blocks", public.url))
            .send()
            .await?
            .status()
            .as_u16(),
        503
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_pool_blocks")
            .fetch_one(&standby_admin)
            .await?,
        1,
        "standby itself must still be queryable during stream failure"
    );
    primary.start(primary_port, dir.path())?;
    public
        .health_until(&client, 200, |v| {
            v["ok"] == true && v["replica"]["in_recovery"] == true
        })
        .await?;
    assert_eq!(
        client
            .get(format!("{}/public/v1/blocks", public.url))
            .send()
            .await?
            .status()
            .as_u16(),
        200
    );

    primary.control(&["-m", "immediate", "-w", "stop"])?;
    standby.control(&["-w", "promote"])?;
    public
        .health_until(&client, 503, |v| v["replica"]["in_recovery"] == false)
        .await?;
    assert_eq!(
        client
            .get(format!("{}/public/v1/blocks", public.url))
            .send()
            .await?
            .status()
            .as_u16(),
        503,
        "promotion must fence existing pooled public sessions"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_prism_instances")
            .fetch_one(&standby_admin)
            .await?,
        1
    );
    reader.close().await;
    standby_admin.close().await;
    drop(public);
    Ok(())
}
