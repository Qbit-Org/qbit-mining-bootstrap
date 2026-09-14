//! Destructive operations here target only two disposable local PostgreSQLs.
//! Set PRISM_TEST_PG_BIN_DIR to the directory containing initdb/pg_ctl/pg_basebackup.
use anyhow::{ensure, Context, Result};
use qbit_prism::AcceptedShare;
use qbit_prism_server::ledger::Ledger;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};

#[path = "support/public_replica.rs"]
mod public_replica;

struct Cluster {
    bin: PathBuf,
    data: PathBuf,
}
impl Cluster {
    fn command(&self, binary: &str, args: &[&str]) -> Result<()> {
        let output = Command::new(self.bin.join(binary)).args(args).output()?;
        ensure!(
            output.status.success(),
            "{binary} failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }
    fn control(&self, args: &[&str]) -> Result<()> {
        let mut all = vec!["-D", self.data.to_str().context("invalid test path")?];
        all.extend_from_slice(args);
        self.command("pg_ctl", &all)
    }
    fn start(&self, port: u16, socket: &Path) -> Result<()> {
        let log = self.data.with_extension("log");
        self.control(&[
            "-l",
            log.to_str().unwrap(),
            "-o",
            &format!(
                "-h 127.0.0.1 -p {port} -k {} -c wal_level=replica -c max_wal_senders=4",
                socket.display()
            ),
            "-w",
            "start",
        ])
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = self.control(&["-m", "immediate", "-w", "stop"]);
    }
}
fn port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}
fn proof(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("ha-worker:{id:064x}"),
        miner_id: "ha-miner".into(),
        order_key: "ha-miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "ha-job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_shares_survive_synchronous_primary_loss_and_pool_reconnect() -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
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
    let primary_port = port()?;
    let standby_port = port()?;
    primary.start(primary_port, dir.path())?;
    let username = String::from_utf8(Command::new("id").arg("-un").output()?.stdout)?
        .trim()
        .to_owned();
    let primary_url = format!("postgresql://{username}@127.0.0.1:{primary_port}/postgres");
    let bootstrap = Ledger::connect(&primary_url, "bootstrap".into(), 4, true).await?;
    bootstrap.configure("failover-test").await?;
    bootstrap.pool.close().await;
    standby.command(
        "pg_basebackup",
        &[
            "-D",
            standby.data.to_str().unwrap(),
            "-d",
            &format!(
                "host=127.0.0.1 port={primary_port} user={username} application_name=prism_standby"
            ),
            "-R",
            "-X",
            "stream",
            "-c",
            "fast",
        ],
    )?;
    standby.start(standby_port, dir.path())?;
    let admin = PgPool::connect(&primary_url).await?;
    sqlx::query("ALTER SYSTEM SET synchronous_standby_names='prism_standby'")
        .execute(&admin)
        .await?;
    sqlx::query("SELECT pg_reload_conf()")
        .execute(&admin)
        .await?;
    timeout(Duration::from_secs(15),async {
        loop {
            let synchronized:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_replication WHERE application_name='prism_standby' AND state='streaming' AND sync_state='sync')").fetch_one(&admin).await?;
            if synchronized {return Ok::<_,anyhow::Error>(());}sleep(Duration::from_millis(50)).await;
        }
    }).await??;

    // This stable endpoint models an HA proxy. Existing SQLx sockets close with
    // the failed primary; newly acquired sockets reach the promoted server.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = listener.local_addr()?;
    let target = Arc::new(AtomicU16::new(primary_port));
    let routing = target.clone();
    let proxy = tokio::spawn(async move {
        while let Ok((mut client, _)) = listener.accept().await {
            let target = routing.load(Ordering::SeqCst);
            tokio::spawn(async move {
                if let Ok(mut upstream) = TcpStream::connect(("127.0.0.1", target)).await {
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                }
            });
        }
    });
    let url = format!("postgresql://{username}@{endpoint}/postgres");
    let a = Arc::new(Ledger::connect(&url, "frontend-a".into(), 4, false).await?);
    let b = Arc::new(Ledger::connect(&url, "frontend-b".into(), 4, false).await?);
    let mut writes = tokio::task::JoinSet::new();
    for id in 1..=40 {
        let ledger = if id % 2 == 0 { a.clone() } else { b.clone() };
        writes.spawn(async move { ledger.append(proof(id), None).await });
    }
    let mut acknowledged = Vec::new();
    while let Some(result) = writes.join_next().await {
        let result = result??;
        assert!(result.inserted);
        acknowledged.push(result.share.share_id);
    }
    assert_eq!(acknowledged.len(), 40);
    admin.close().await;
    primary.control(&["-m", "immediate", "-w", "stop"])?;
    target.store(standby_port, Ordering::SeqCst);
    timeout(Duration::from_secs(15), async {
        loop {
            if sqlx::query_scalar::<_, bool>("SELECT pg_is_in_recovery()")
                .fetch_one(&a.pool)
                .await
                .unwrap_or(false)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    assert!(
        a.payout_revision().await.is_err(),
        "an unpromoted standby cannot authorize mining readiness"
    );
    standby.control(&["-w", "promote"])?;
    timeout(Duration::from_secs(30), async {
        loop {
            if a.payout_revision().await.is_ok() && b.payout_revision().await.is_ok() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    let durable: Vec<String> =
        sqlx::query_scalar("SELECT share_id FROM qbit_share_ledger ORDER BY share_id")
            .fetch_all(&a.pool)
            .await?;
    acknowledged.sort();
    assert_eq!(
        durable, acknowledged,
        "every acknowledged share must survive primary storage loss"
    );
    assert!(
        !b.append(proof(1), None).await?.inserted,
        "replayed proof must not gain second credit after promotion"
    );
    assert!(a.append(proof(41), None).await?.inserted);
    assert!(b.append(proof(42), None).await?.inserted);
    let (count,unique,min,max):(i64,i64,i64,i64)=sqlx::query_as("SELECT count(*),count(DISTINCT share_seq),min(share_seq),max(share_seq) FROM qbit_share_ledger").fetch_one(&a.pool).await?;
    assert_eq!((count, unique, min), (42, 42, 1));
    assert!(max >= 42);
    a.pool.close().await;
    b.pool.close().await;
    proxy.abort();
    Ok(())
}
