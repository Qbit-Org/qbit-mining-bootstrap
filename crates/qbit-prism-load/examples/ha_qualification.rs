//! Opt-in, bounded #281 functional exercise. Never accepts an existing database.
//! This is ledger/fixture evidence, not a Compose, operator LB or #291 rehearsal.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use qbit_prism::AcceptedShare;
use qbit_prism_load::{cluster, digest, frontend::redact_secrets_in_text};
use qbit_prism_server::ledger::{Ledger, SignerKeys};
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool};
use std::{
    collections::BTreeSet,
    net::SocketAddr,
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
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout, Instant},
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    pg_bin_dir: PathBuf,
    /// New directory for evidence; existing directories are refused.
    #[arg(long)]
    out: PathBuf,
    /// Existing compact_runtime_e2e test executable, built with cargo --no-run.
    #[arg(long)]
    runtime_tests: PathBuf,
    /// Existing qbit-prism-server library test executable for share ACK barriers.
    #[arg(long)]
    ack_tests: PathBuf,
}

/// The endpoint only changes AFTER positive old-primary fencing and promotion.
/// JoinSet owns every accepted stream; dropping it closes existing connections.
struct WriterEndpoint {
    address: SocketAddr,
    target: Arc<AtomicU16>,
    task: JoinHandle<()>,
}
impl WriterEndpoint {
    async fn start(port: u16) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let target = Arc::new(AtomicU16::new(port));
        let routing = target.clone();
        let task = tokio::spawn(async move {
            let mut streams = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((mut client, _)) = accepted else { break };
                        let port = routing.load(Ordering::SeqCst);
                        streams.spawn(async move {
                            if let Ok(mut upstream) = TcpStream::connect(("127.0.0.1", port)).await {
                                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                            }
                        });
                    }
                    _ = streams.join_next(), if !streams.is_empty() => {}
                }
            }
        });
        Ok(Self {
            address,
            target,
            task,
        })
    }
}
impl Drop for WriterEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn control(bin: &Path, data: &Path, args: &[&str]) -> Result<()> {
    let output = Command::new(bin.join("pg_ctl"))
        .arg("-D")
        .arg(data)
        .args(["-w", "-t", "15"])
        .args(args)
        .output()?;
    ensure!(
        output.status.success(),
        "owned pg_ctl operation failed for {}",
        data.display()
    );
    Ok(())
}

fn stopped(bin: &Path, data: &Path) -> Result<bool> {
    let result = Command::new(bin.join("pg_ctl"))
        .arg("-D")
        .arg(data)
        .arg("status")
        .output()?;
    // 3 means this exact directory has no running server. Other errors are unknown.
    Ok(result.status.code() == Some(3) && !data.join("postmaster.pid").exists())
}

fn share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("ha-functional:{id:064x}"),
        miner_id: "ha-functional".into(),
        order_key: "ha-functional".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "ha-functional".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

async fn data_directory(url: &str) -> Result<PathBuf> {
    let mut connection = PgConnection::connect(url).await?;
    let data: String = sqlx::query_scalar("SHOW data_directory")
        .fetch_one(&mut connection)
        .await?;
    connection.close().await?;
    Ok(PathBuf::from(data).canonicalize()?)
}

async fn exercise(
    args: &Args,
    managed: &cluster::ManagedPostgres,
    primary: &Path,
    standby: &Path,
    evidence: &mut Value,
) -> Result<()> {
    let admin = PgPool::connect(&managed.primary_url).await?;
    let replica = PgPool::connect(managed.standby_url.as_ref().context("no standby")?).await?;
    let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
        .fetch_one(&admin)
        .await?;
    ensure!(
        (160000..170000).contains(&version),
        "requires PostgreSQL 16"
    );
    ensure!(
        cluster::durability(&admin).await? == ("on".into(), "on".into(), "on".into()),
        "durability disabled"
    );
    let observation = cluster::observe_replication(&admin, "owned-primary-before").await?;
    ensure!(
        observation.error.is_none()
            && observation.synchronous_standby_names.as_deref() == Some("")
            && observation.rows.len() == 1
            && observation.rows[0].application_name == cluster::STANDBY_NAME
            && observation.rows[0].state == "streaming"
            && observation.rows[0].sync_state == "async",
        "did not observe exactly one dedicated async standby"
    );
    evidence["primary_before"] = json!(observation);
    let active_slot: bool = sqlx::query_scalar(
        "SELECT active AND slot_type='physical' FROM pg_replication_slots WHERE slot_name=$1",
    )
    .bind(cluster::STANDBY_SLOT)
    .fetch_one(&admin)
    .await?;
    ensure!(
        active_slot,
        "dedicated physical replication slot is not active"
    );

    // Reuse the existing Coordinator/Stratum fixture and its original deadlines.
    // Its node is controlled local RPC, not qbitd or the Compose overlay.
    let suites = [
        (&args.runtime_tests, vec![
            "real_socket_reconnect_resumes_original_entropy_mask_and_submits_once",
            "delayed_old_refresh_cannot_replace_new_tip_publication_or_resume_retired_work",
            "resume_expiry_includes_blocked_share_read_and_releases_resources",
            "unknown_issued_commit_is_observed_and_reconciled_without_reissuing",
        ]),
        (&args.ack_tests, vec![
            "coordinator::commit_reconcile_tests::commit_reconcile_append_queued_on_the_order_lock_is_aborted_at_the_deadline",
            "coordinator::commit_reconcile_tests::commit_reconcile_commit_held_past_the_deadline_is_accepted_late",
            "coordinator::commit_reconcile_tests::commit_reconcile_commit_held_past_the_grace_is_unknown_and_lands_later",
        ]),
    ];
    for (binary, tests) in suites {
        for test in tests {
            let output = tokio::process::Command::new(binary)
                .env_clear()
                .env("PATH", std::env::var_os("PATH").unwrap_or_default())
                .env(
                    qbit_prism_test_gate::Input::DatabaseUrl.name(),
                    &managed.primary_url,
                )
                .env(qbit_prism_test_gate::SWITCH_VAR, "1")
                .arg(test)
                .args(["--exact", "--nocapture", "--test-threads=1"])
                .kill_on_drop(true)
                .output()
                .await?;
            let log = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            std::fs::write(
                args.out.join(format!("{test}.log")),
                redact_secrets_in_text(&log),
            )?;
            ensure!(
                output.status.success() && log.contains("1 passed; 0 failed"),
                "runtime fixture failed or selected no test: {test}"
            );
            evidence["runtime_tests"]
                .as_array_mut()
                .unwrap()
                .push(json!(test));
        }
    }
    let schemas: Vec<String> = sqlx::query_scalar(
            "SELECT nspname::text FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname NOT IN ('public', 'information_schema')",
        )
        .fetch_all(&admin)
        .await?;
    evidence["runtime_schemas_remaining"] = json!(schemas);
    ensure!(schemas.is_empty(), "fixture left owned schemas behind");

    let endpoint = WriterEndpoint::start(managed.primary_port).await?;
    evidence["owned_writer_endpoint"] = json!(endpoint.address.to_string());
    // This DSN remains identical for both Ledger pools throughout the exercise.
    let writer_url = managed.primary_url.replace(
        &format!("127.0.0.1:{}", managed.primary_port),
        &endpoint.address.to_string(),
    );
    let a = Ledger::connect(&writer_url, "ha-functional-east".into(), 4, true).await?;
    a.configure(
        "ha-functional",
        &SignerKeys {
            manifest_key_hex: "aa".repeat(32),
            ledger_key_hex: "bb".repeat(32),
        },
    )
    .await?;
    let b = Ledger::connect(&writer_url, "ha-functional-west".into(), 4, false).await?;
    ensure!(
        cluster::durability(&a.pool).await?.2 == "on"
            && cluster::durability(&b.pool).await?.2 == "on",
        "writer durability changed"
    );
    let mut acknowledged = BTreeSet::new();
    for id in 1..=8 {
        let result = if id % 2 == 0 { &a } else { &b }
            .append(share(id), None)
            .await?;
        ensure!(result.inserted, "baseline append was not new");
        acknowledged.insert(result.share.share_id);
    }
    let baseline_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_flush_lsn()::text")
        .fetch_one(&admin)
        .await?;
    timeout(Duration::from_secs(15), async {
        loop {
            let caught_up: bool =
                sqlx::query_scalar("SELECT pg_last_wal_replay_lsn() >= $1::text::pg_lsn")
                    .bind(&baseline_lsn)
                    .fetch_one(&replica)
                    .await?;
            if caught_up {
                break Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("standby never replayed the baseline")??;
    let standby_lsn: String = sqlx::query_scalar("SELECT pg_last_wal_replay_lsn()::text")
        .fetch_one(&replica)
        .await?;
    replica.close().await;
    control(&args.pg_bin_dir, standby, &["-m", "immediate", "stop"])?;
    ensure!(
        stopped(&args.pg_bin_dir, standby)?,
        "standby stop unverified"
    );
    let standby_down = Instant::now();
    for id in 9..=11 {
        let result = a.append(share(id), None).await?;
        ensure!(result.inserted, "gap append was not new");
        acknowledged.insert(result.share.share_id);
    }
    let last_pre_fence_append = Instant::now();
    let gap_bytes: i64 = sqlx::query_scalar(
        "SELECT pg_wal_lsn_diff(pg_current_wal_flush_lsn(), $1::text::pg_lsn)::bigint",
    )
    .bind(&standby_lsn)
    .fetch_one(&admin)
    .await?;
    ensure!(gap_bytes > 0, "did not create an unreplicated WAL gap");
    evidence["gap_before_fence"] = json!({
        "acknowledgement_source": "Ledger::append return, not miner wire ACK",
        "acknowledged": acknowledged, "standby_replay_lsn_before_stop": standby_lsn,
        "primary_flush_minus_standby_replay_bytes": gap_bytes,
        "standby_down_elapsed_ms": standby_down.elapsed().as_millis(),
        "replay_lag_seconds": null,
        "replay_lag_reason": "standby disconnected; no fresh time-lag observation",
        "primary_observation": cluster::observe_replication(&admin, "owned-primary-standby-down").await?,
    });
    // Keep an existing writer socket to prove fencing covers it as well as admission.
    let mut old_socket = PgConnection::connect(&managed.primary_url).await?;
    let failover_started = Instant::now();
    control(&args.pg_bin_dir, primary, &["-m", "immediate", "stop"])?;
    ensure!(
        stopped(&args.pg_bin_dir, primary)?,
        "primary fencing unverified; refusing promotion"
    );
    ensure!(
        TcpStream::connect(("127.0.0.1", managed.primary_port))
            .await
            .is_err(),
        "old primary still admits sockets"
    );
    let old_result = timeout(
        Duration::from_secs(2),
        sqlx::query("SELECT 1").execute(&mut old_socket),
    )
    .await;
    ensure!(
        matches!(old_result, Ok(Err(_))),
        "old writer socket not positively closed"
    );
    evidence["fence"] = json!({"method": "owned pg_ctl immediate stop; no restart automation", "old_socket_failed": true, "new_connection_refused": true, "verified_before_promotion": true});
    admin.close().await;
    // `pg_ctl start` does not restore the previous command-line options. Restate
    // the owned driver's loopback port and durability settings on this restart.
    // Redirect the restarted postmaster too: otherwise it inherits pg_ctl's
    // captured stdout pipe and Command::output waits for the server to exit.
    let standby_log = standby.with_extension("log");
    let standby_options = format!(
        "-h 127.0.0.1 -p {} -k {} -c hot_standby=on -c fsync=on -c full_page_writes=on -c max_connections=32 -c max_wal_senders=10",
        managed.standby_port.unwrap(), standby.parent().context("standby parent")?.display(),
    );
    control(
        &args.pg_bin_dir,
        standby,
        &[
            "-l",
            standby_log.to_str().context("standby log path")?,
            "-o",
            &standby_options,
            "start",
        ],
    )?;
    let promoted = PgPool::connect(managed.standby_url.as_ref().unwrap()).await?;
    let in_recovery: bool = sqlx::query_scalar("SELECT pg_is_in_recovery()")
        .fetch_one(&promoted)
        .await?;
    ensure!(
        in_recovery,
        "standby was already writable before authorized promotion"
    );
    let did_promote: bool = sqlx::query_scalar("SELECT pg_promote(true, 15)")
        .fetch_one(&promoted)
        .await?;
    ensure!(
        did_promote,
        "promotion timed out; inspect owned cluster, never retry blindly"
    );
    let in_recovery: bool = sqlx::query_scalar("SELECT pg_is_in_recovery()")
        .fetch_one(&promoted)
        .await?;
    ensure!(!in_recovery, "promotion did not establish writer role");
    let promotion_ms = failover_started.elapsed().as_millis();
    endpoint
        .target
        .store(managed.standby_port.unwrap(), Ordering::SeqCst);
    timeout(Duration::from_secs(15), async {
        loop {
            if a.payout_revision().await.is_ok() && b.payout_revision().await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("both original pools did not recover through stable endpoint")?;
    let committed: BTreeSet<String> =
        sqlx::query_scalar::<_, String>("SELECT share_id FROM qbit_share_ledger")
            .fetch_all(&a.pool)
            .await?
            .into_iter()
            .collect();
    let reconciliation = digest::reconcile(acknowledged.clone(), acknowledged, &committed);
    let expected_missing: BTreeSet<String> = (9..=11).map(|id| share(id).share_id).collect();
    ensure!(
        reconciliation.missing == expected_missing && committed.len() == 8,
        "unexpected async loss set"
    );
    ensure!(
        !b.append(share(1), None).await?.inserted,
        "surviving proof received duplicate credit"
    );
    ensure!(
        a.append(share(12), None).await?.inserted,
        "new primary cannot commit fresh work"
    );
    evidence["reconciliation"] = json!({
        "acknowledged_count": reconciliation.acknowledged.len(), "survived_count": committed.len(),
        "acknowledged_missing": reconciliation.missing, "acknowledged_digest": reconciliation.acknowledged_digest(),
        "committed_digest": reconciliation.committed_digest(), "missing_shares_retried": false,
        "duplicate_surviving_share_inserted": false, "post_promotion_new_share_inserted": true,
        "unknown_commit_injection": "not exercised by the async ledger phase",
    });
    evidence["timing"] = json!({"fence_start_to_promotion_ms": promotion_ms,
        "last_pre_fence_append_to_first_post_promotion_append_ms": last_pre_fence_append.elapsed().as_millis(),
        "mining_endpoint_downtime_ms": null, "mining_endpoint_downtime_reason": "no overlay or operator TCP load balancer in this exercise"});
    evidence["primary_after"] =
        json!(cluster::observe_replication(&promoted, "owned-promoted-primary").await?);
    evidence["writer_endpoint"] =
        json!({"same_dsn_both_pools": true, "retargeted_only_after_fencing_and_promotion": true});
    a.pool.close().await;
    b.pool.close().await;
    promoted.close().await;
    Ok(())
}

#[cfg(test)]
mod cli_tests {
    use super::Args;
    use clap::{error::ErrorKind, Parser};

    #[test]
    fn missing_either_or_both_suites_fails_before_resource_setup() {
        for extra in [
            vec![],
            vec!["--runtime-tests", "/unused/runtime"],
            vec!["--ack-tests", "/unused/ack"],
        ] {
            let mut arguments = vec![
                "ha_qualification",
                "--pg-bin-dir",
                "/unused/pg",
                "--out",
                "/unused/out",
            ];
            arguments.extend(extra);
            let error = Args::try_parse_from(arguments)
                .err()
                .expect("missing suite parsed");
            assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
        }
    }

    #[test]
    fn both_suite_paths_reach_the_consumer() {
        let args = Args::try_parse_from([
            "ha_qualification",
            "--pg-bin-dir",
            "/unused/pg",
            "--out",
            "/unused/out",
            "--runtime-tests",
            "/unused/runtime",
            "--ack-tests",
            "/unused/ack",
        ])
        .unwrap();
        assert_eq!(args.runtime_tests.to_str(), Some("/unused/runtime"));
        assert_eq!(args.ack_tests.to_str(), Some("/unused/ack"));
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args = Args::parse();
    cluster::verify_bin_dir(&args.pg_bin_dir)?;
    std::fs::create_dir(&args.out).context("evidence directory must be new")?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut evidence = json!({"schema": "qbit.prism.ha-functional.v1", "result": "incomplete",
        "scope": "disposable local functional evidence; no D1/#291/live-#281 certification",
        "topology": "one primary plus one dedicated async standby; no public reader",
        "runtime_tests": [], "runtime_schemas_remaining": null,
        "operator_lb": "not executed", "real_overlay_resume": "not executed",
        "primary_exporter_alert_delivery": "not executed"});
    // The existing managed driver provisions new clusters; there is no URL input.
    let mut managed = cluster::ManagedPostgres::start(
        args.pg_bin_dir.clone(),
        cluster::Replication::Async,
        32,
        true,
    )
    .await?;
    let primary = data_directory(&managed.primary_url).await?;
    let standby = data_directory(managed.standby_url.as_ref().context("standby URL")?).await?;
    let root = primary.parent().context("primary root")?.to_path_buf();
    ensure!(
        root.file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("prism-load-")
            && primary.file_name().unwrap() == "primary"
            && standby == root.join("standby"),
        "unexpected managed paths"
    );
    evidence["owned_resources"] = json!({"root": root, "primary": primary, "standby": standby,
        "primary_port": managed.primary_port, "standby_port": managed.standby_port,
        "containers": [], "external_databases": []});
    let result = tokio::select! {
        result = timeout(Duration::from_secs(180), exercise(&args, &managed, &primary, &standby, &mut evidence)) => result.context("functional exercise deadline expired").and_then(|r| r),
        _ = interrupt.recv() => Err(anyhow::anyhow!("interrupted")),
        _ = terminate.recv() => Err(anyhow::anyhow!("terminated")),
    };
    // Retain directories until positive process-exit evidence, even on failure.
    managed.stop();
    for name in ["primary.log", "standby.log"] {
        if let Err(error) = std::fs::copy(root.join(name), args.out.join(name)) {
            evidence["diagnostic_copy_error"] = json!(format!("{name}: {error}"));
        }
    }
    // Let cancellation drop the endpoint's JoinSet and close its accepted streams.
    tokio::task::yield_now().await;
    let endpoint_closed = match evidence["owned_writer_endpoint"].as_str() {
        Some(address) => TcpStream::connect(address).await.is_err(),
        None => true,
    };
    let primary_stopped = stopped(&args.pg_bin_dir, &primary).unwrap_or(false);
    let standby_stopped = stopped(&args.pg_bin_dir, &standby).unwrap_or(false);
    let ports_closed = TcpStream::connect(("127.0.0.1", managed.primary_port))
        .await
        .is_err()
        && TcpStream::connect(("127.0.0.1", managed.standby_port.unwrap()))
            .await
            .is_err();
    drop(managed);
    let removed = if primary_stopped && standby_stopped && ports_closed {
        std::fs::remove_dir_all(&root).is_ok()
    } else {
        false
    };
    evidence["cleanup"] = json!({"primary_stopped": primary_stopped, "standby_stopped": standby_stopped,
        "postgres_ports_closed": ports_closed, "writer_endpoint_closed": endpoint_closed,
        "owned_directory_removed": removed,
        "complete": primary_stopped && standby_stopped && ports_closed && endpoint_closed && removed});
    if let Err(error) = &result {
        evidence["error"] = json!(redact_secrets_in_text(&format!("{error:#}")));
    }
    let clean = evidence["cleanup"]["complete"] == true;
    evidence["result"] = json!(if result.is_ok() && clean {
        "passed"
    } else {
        "failed"
    });
    std::fs::write(
        args.out.join("ha-functional.json"),
        serde_json::to_vec_pretty(&evidence)?,
    )?;
    println!(
        "{}: {}",
        evidence["result"],
        args.out.join("ha-functional.json").display()
    );
    result?;
    ensure!(
        clean,
        "cleanup unverified: retain evidence and inspect only owned resources"
    );
    Ok(())
}
