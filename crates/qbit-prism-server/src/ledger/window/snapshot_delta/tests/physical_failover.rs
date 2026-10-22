//! Physical history replacement, using only disposable local PostgreSQLs.
use super::*;
use std::{
    path::PathBuf,
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

struct Cluster {
    bin: PathBuf,
    data: PathBuf,
}

impl Cluster {
    fn command(&self, binary: &str, args: &[&str]) -> Result<()> {
        let output = Command::new(self.bin.join(binary)).args(args).output()?;
        anyhow::ensure!(
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

    fn start(&self, port: u16) -> Result<()> {
        self.control(&[
            "-l",
            self.data.with_extension("log").to_str().unwrap(),
            "-o",
            &format!(
                "-h 127.0.0.1 -p {port} -k {} -c wal_level=replica -c max_wal_senders=4",
                self.data.parent().unwrap().display()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_async_failover_replaced_history_matches_full_read() -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    physical_failover(bin, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_async_failover_with_retroactive_fixture_matches_full_read() -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    physical_failover(bin, true).await
}

async fn insert_fixture(ledger: &Ledger, id: &str) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch)
        VALUES(75,$1,'fixture-miner','fixture-miner',decode(repeat('11',32),'hex'),1,1,1,'fixture-job',to_timestamp(1),1,to_timestamp(2),true,'fixture',0)")
        .bind(id).execute(&ledger.pool).await?;
    Ok(())
}

async fn physical_failover(bin: String, retroactive: bool) -> Result<()> {
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
            "--data-checksums",
            "-E",
            "UTF8",
        ],
    )?;
    // pg_rewind must read back to the divergence checkpoint even after its
    // single-user crash recovery checkpoints the target. Persist the setting
    // so that recovery, not only pg_ctl's normal start, retains those segments.
    let config = primary.data.join("postgresql.conf");
    let mut settings = std::fs::read_to_string(&config)?;
    settings.push_str("\nwal_keep_size = '128MB'\n");
    std::fs::write(config, settings)?;
    let primary_port = port()?;
    let standby_port = port()?;
    primary.start(primary_port)?;
    let username = String::from_utf8(Command::new("id").arg("-un").output()?.stdout)?
        .trim()
        .to_owned();
    let primary_url = format!("postgresql://{username}@127.0.0.1:{primary_port}/postgres");
    // Preserve the frontend and its pool through both promotions. Old sockets
    // die with the fenced writer; new sockets follow the stable endpoint.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = listener.local_addr()?;
    let target = Arc::new(AtomicU16::new(primary_port));
    let routing = target.clone();
    let proxy = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((mut client, _)) = accepted else { break };
                    let port = routing.load(Ordering::SeqCst);
                    connections.spawn(async move {
                        if let Ok(mut upstream) = TcpStream::connect(("127.0.0.1", port)).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                        }
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    }));
    let writer_url = format!("postgresql://{username}@{endpoint}/postgres");
    let ledger = Ledger::connect(&writer_url, "surviving-frontend".into(), 4, true).await?;
    let durable_cutoff = if retroactive { 80 } else { 8 };
    for index in 1..=durable_cutoff {
        if retroactive && index == 75 {
            let revision = ledger.payout_revision().await?;
            let refused = ledger
                .append_at_revision_gated(share(index, 1), None, revision, &|| false)
                .await;
            assert!(refused
                .unwrap_err()
                .downcast_ref::<CommitGateClosed>()
                .is_some());
            continue;
        }
        ledger.append(share(index, 1), None).await?;
    }
    // Capture a real physical copy, then leave it disconnected while the
    // primary keeps accepting shares. No trigger bypass or sequence reset.
    standby.command(
        "pg_basebackup",
        &[
            "-D",
            standby.data.to_str().unwrap(),
            "-d",
            &format!("host=127.0.0.1 port={primary_port} user={username}"),
            "-R",
            "-X",
            "stream",
            "-c",
            "fast",
        ],
    )?;
    if retroactive {
        insert_fixture(&ledger, "fixture-original").await?;
    } else {
        for index in 9..=80 {
            ledger.append(share(index, 1), None).await?;
        }
    }
    let prior = capture(&ledger, 1).await?;
    assert_eq!(prior.share_seq, 80);
    assert_eq!(prior.shares.first().unwrap().share_seq, 73);
    primary.control(&["-m", "immediate", "-w", "stop"])?;
    standby.start(standby_port)?;
    standby.control(&["-w", "promote"])?;
    let promoted_url = format!("postgresql://{username}@127.0.0.1:{standby_port}/postgres");
    target.store(standby_port, Ordering::SeqCst);
    wait_for_writer(&ledger).await?;
    let promoted = &ledger;
    let recovered_cutoff: i64 = sqlx::query_scalar("SELECT max(share_seq) FROM qbit_share_ledger")
        .fetch_one(&promoted.pool)
        .await?;
    assert_eq!(recovered_cutoff, i64::try_from(durable_cutoff)?);
    let mut first_reissued = None;
    if retroactive {
        insert_fixture(promoted, "fixture-replacement").await?;
    } else {
        for index in 1..=80 {
            let appended = promoted.append(share(1000 + index, 1), None).await?;
            first_reissued.get_or_insert(appended.share.share_seq);
            if appended.share.share_seq >= prior.share_seq {
                assert_eq!(appended.share.share_seq, prior.share_seq);
                break;
            }
        }
    }
    let full = capture(promoted, 1).await?;
    eprintln!(
        "physical failover: recovered cutoff={recovered_cutoff}, first reissued={first_reissued:?}, prior/fresh cutoff={}/{}, prior/fresh leaf={:?}/{:?}",
        prior.share_seq, full.share_seq, prior.leaf, full.leaf
    );
    assert_eq!(full.share_seq, prior.share_seq);
    let before = prior.leaf.as_ref().unwrap();
    let after = full.leaf.as_ref().unwrap();
    assert_eq!(before.tableoid, after.tableoid);
    assert_eq!(before.inherits_xmin, after.inherits_xmin);
    assert_ne!(before.timeline, after.timeline);
    assert_ne!(full.shares, prior.shares, "same-count payload was replaced");
    if retroactive {
        assert_eq!(
            full.shares.last(),
            prior.shares.last(),
            "even the entire newest share survives unchanged"
        );
    }
    assert!(full.anchor_ms >= prior.anchor_ms);
    let phantom = Snapshot {
        shares: prior.shares.clone(),
        ..full.snapshot.clone()
    };
    assert!(matches!(
        promoted
            .read_window(&WindowRef::from_snapshot(&phantom)?, BalanceSource::Current)
            .await,
        Err(WindowError::SnapshotDigestMismatch { .. })
    ));
    differential(promoted, retained(prior.clone(), 1), 1, false).await?;
    // Exercise the real fallback assembly too, then authenticate its complete
    // payload through the existing full reader at the issued reference's anchor.
    let admission = ReadAdmission::default();
    let fresh = promoted
        .snapshot_with_admission(
            1,
            admission.clone(),
            Some(admission.own(retained(prior, 1))),
        )
        .await?
        .into_inner();
    let reference = WindowRef::from_snapshot(&fresh)?;
    let checked = promoted
        .read_window(&reference, BalanceSource::Current)
        .await?;
    assert_eq!(fresh.shares, full.shares);
    let oracle = Snapshot {
        shares: checked.shares,
        prior_balances: checked.prior_balances,
        ..fresh.snapshot.clone()
    };
    assert_eq!(
        serde_json::to_vec(&fresh.snapshot)?,
        serde_json::to_vec(&oracle)?
    );
    assert_eq!(reference, WindowRef::from_snapshot(&oracle)?);
    // Rejoin exactly as D3 step 6 permits, then exercise another promotion.
    // Both fresh physical copy and verified rewind must follow the new history.
    sqlx::query("CHECKPOINT").execute(&promoted.pool).await?;
    if retroactive {
        std::fs::remove_dir_all(&primary.data)?;
        primary.command(
            "pg_basebackup",
            &[
                "-D",
                primary.data.to_str().unwrap(),
                "-d",
                &promoted_url,
                "-R",
                "-X",
                "stream",
                "-c",
                "fast",
            ],
        )?;
    } else {
        primary.command(
            "pg_rewind",
            &[
                "--target-pgdata",
                primary.data.to_str().unwrap(),
                "--source-server",
                &promoted_url,
                "--write-recovery-conf",
            ],
        )?;
    }
    primary.start(primary_port)?;
    let rejoined = sqlx::PgPool::connect(&primary_url).await?;
    let replay_through: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&promoted.pool)
        .await?;
    timeout(Duration::from_secs(15), async {
        loop {
            let caught_up: bool = sqlx::query_scalar(
                "SELECT pg_is_in_recovery() AND pg_last_wal_replay_lsn() >= $1::text::pg_lsn",
            )
            .bind(&replay_through)
            .fetch_one(&rejoined)
            .await?;
            if caught_up {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    rejoined.close().await;
    let second_prior = capture(promoted, 1).await?;
    standby.control(&["-m", "immediate", "-w", "stop"])?;
    primary.control(&["-w", "promote"])?;
    target.store(primary_port, Ordering::SeqCst);
    wait_for_writer(&ledger).await?;
    let second_fresh = differential(&ledger, retained(second_prior.clone(), 1), 1, false).await?;
    assert_eq!(second_fresh.shares, second_prior.shares);
    assert_eq!(
        second_fresh.leaf.as_ref().unwrap().tableoid,
        second_prior.leaf.as_ref().unwrap().tableoid
    );
    assert_eq!(
        second_fresh.leaf.as_ref().unwrap().inherits_xmin,
        second_prior.leaf.as_ref().unwrap().inherits_xmin
    );
    assert_ne!(
        second_fresh.leaf.as_ref().unwrap().timeline,
        second_prior.leaf.as_ref().unwrap().timeline
    );
    eprintln!(
        "rejoin via {}: timeline {:?} -> {:?}",
        if retroactive { "basebackup" } else { "rewind" },
        second_prior.leaf,
        second_fresh.leaf
    );
    differential(&ledger, retained(second_fresh, 1), 1, true).await?;
    ledger.pool.close().await;
    drop(proxy);
    Ok(())
}

async fn wait_for_writer(ledger: &Ledger) -> Result<()> {
    timeout(Duration::from_secs(15), async {
        loop {
            if sqlx::query_scalar::<_, bool>("SELECT NOT pg_is_in_recovery()")
                .fetch_one(&ledger.pool)
                .await
                .unwrap_or(false)
            {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    Ok(())
}
