//! Orchestration: provenance, clusters, node, seeding, frontends, phases,
//! reconciliation and outputs.

use crate::{
    artifact::{self, ArtifactInputs, PhaseEvidence},
    classify::{self, BlockedLog, Rejection, RejectionClass},
    cli::{phases, Args, PhasePlan},
    client::{self, Event, Outcome, SessionConfig, SessionHandle, SessionShared, SubmitRecord},
    cluster::{self, ManagedPostgres, Replication},
    digest,
    frontend::{self, Frontend, FrontendSpec, SharedEnvironment},
    measure::{self, LockSampler, ProcessSampler},
    node::FakeNode,
    profile, proxy, report, window,
};
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

/// Exit codes. 0 is a completed, reconciled run; every other value is a
/// distinguishable outcome, never a silent success.
pub const EXIT_OK: i32 = 0;
pub const EXIT_ERROR: i32 = 2;
pub const EXIT_BLOCKED: i32 = 3;
/// An acknowledged share that PostgreSQL does not hold: a loss.
pub const EXIT_DURABILITY: i32 = 4;
/// A share PostgreSQL holds that the server told the client it had not
/// confirmed. Nothing was lost, but an acknowledgement and a commit diverged.
pub const EXIT_ACK_COMMIT_DIVERGENCE: i32 = 5;
pub const EXIT_ABORTED: i32 = 6;
/// Rejections that can only happen if the harness offered bad work.
pub const EXIT_HARNESS_BUG_REJECTIONS: i32 = 7;

#[derive(Default)]
struct Collected {
    submits: Vec<SubmitRecord>,
    reconnects: Vec<client::ReconnectRecord>,
    tips: Vec<(usize, String, Instant)>,
    discarded_block_solutions: u64,
    discarded_offers: u64,
    difficulty_mismatches: Vec<(usize, f64, f64)>,
    failures: Vec<(usize, String)>,
    connects: u64,
    disconnects: Vec<(usize, usize, String)>,
}

struct PhaseRun {
    plan: PhasePlan,
    started_wall: chrono::DateTime<chrono::Utc>,
    ended_wall: chrono::DateTime<chrono::Utc>,
    duration_millis: u64,
    tokens: u64,
    dispatched: u64,
    shortfall: u64,
    lock: measure::LockSummary,
    processes: Vec<measure::ProcessSummary>,
    ack_deltas: Vec<measure::ServerAckDelta>,
    replication_start: cluster::ReplicationObservation,
    replication_end: cluster::ReplicationObservation,
    proxy_delay_configured_ms: u64,
    min_mem_available_kib: Option<u64>,
    scheduled_blocks: usize,
    frontend_restarts: usize,
    mid_flight_indeterminate: Vec<SubmitRecord>,
}

pub async fn execute(args: Args) -> Result<i32> {
    args.validate()?;
    let started_wall = chrono::Utc::now();
    let run_id = uuid::Uuid::new_v4();
    let run_tag = run_id.simple().to_string()[..8].to_owned();
    let address_prefix = "pload1".to_owned();
    let payout_address = format!("{address_prefix}{run_tag}");
    let share_prefix = format!("{payout_address}.");

    std::fs::create_dir_all(&args.out)
        .with_context(|| format!("creating {}", args.out.display()))?;
    let log_dir = args.out.join("logs");
    std::fs::create_dir_all(&log_dir)?;

    // --- provenance -------------------------------------------------------
    let revision = git(&["rev-parse", "HEAD"])?.trim().to_owned();
    ensure!(
        revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit()),
        "git rev-parse HEAD did not return a commit: {revision:?}"
    );
    let dirty = !git(&["status", "--porcelain", "--untracked-files=no"])?
        .trim()
        .is_empty();
    if dirty && !args.allow_dirty_tree {
        bail!(
            "tracked files are modified, so the artifact could not name the code that ran; \
             commit them or pass --allow-dirty-tree"
        );
    }
    let server_bin = resolve_server_bin(args.server_bin.clone())?;
    let server_profile = frontend::build_profile(&server_bin);
    let harness_profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    ensure!(
        server_profile != frontend::BuildProfile::Debug || args.allow_debug_server,
        "{} is a debug build, which does not measure capacity; pass --allow-debug-server to \
         run it anyway",
        server_bin.display()
    );
    let server_bytes =
        std::fs::read(&server_bin).with_context(|| format!("reading {}", server_bin.display()))?;
    let server_digest = format!("sha256:{}", hex::encode(Sha256::digest(&server_bytes)));
    let artifact_kind = if dirty || args.example_artifact {
        artifact::ARTIFACT_EXAMPLE
    } else {
        artifact::ARTIFACT_QUALIFICATION
    }
    .to_owned();

    // --- file descriptors -------------------------------------------------
    let needed = (args.sessions as u64) * 4 + 1024;
    let (fd_before, fd_after) = measure::raise_file_descriptor_limit(needed)?;

    // --- fake node --------------------------------------------------------
    let node = FakeNode::open(window::TEMPLATE_BITS, &address_prefix).await?;
    let node_state = node.state.clone();

    // --- PostgreSQL -------------------------------------------------------
    let replication = Replication::parse(&args.replication)?;
    let max_connections = args.frontends as u32 * args.db_max_connections + 32;
    let mut managed: Option<ManagedPostgres> = None;
    let direct_url = match &args.database_url {
        Some(url) => url.clone(),
        None => {
            let bin_dir = cluster::resolve_bin_dir(args.pg_bin_dir.as_deref())?;
            let cluster =
                ManagedPostgres::start(bin_dir, replication, max_connections, args.keep_artifacts)
                    .await?;
            let url = cluster.primary_url.clone();
            managed = Some(cluster);
            url
        }
    };
    let result = run_inner(
        &args,
        RunContext {
            run_id,
            run_tag: run_tag.clone(),
            started_wall,
            payout_address,
            share_prefix,
            revision,
            dirty,
            server_bin,
            server_digest,
            server_profile,
            harness_profile,
            artifact_kind,
            fd_before,
            fd_after,
            node_url: node.url.clone(),
            node_state,
            direct_url: direct_url.clone(),
            log_dir,
            declared_replication: replication,
            managed_standby: managed.as_ref().and_then(|m| m.standby_url.clone()),
            pg_stat_statements: managed
                .as_ref()
                .and_then(|m| m.pg_stat_statements.clone())
                .unwrap_or_else(|| "unknown (external database)".into()),
        },
    )
    .await;
    // Cleanup runs on every exit path.
    if let Some(mut cluster) = managed {
        cluster.stop();
    }
    drop(node);
    result
}

struct RunContext {
    run_id: uuid::Uuid,
    run_tag: String,
    started_wall: chrono::DateTime<chrono::Utc>,
    payout_address: String,
    share_prefix: String,
    revision: String,
    dirty: bool,
    server_bin: PathBuf,
    server_digest: String,
    server_profile: frontend::BuildProfile,
    harness_profile: &'static str,
    artifact_kind: String,
    fd_before: u64,
    fd_after: u64,
    node_url: String,
    node_state: Arc<crate::node::NodeState>,
    direct_url: String,
    log_dir: PathBuf,
    declared_replication: Replication,
    managed_standby: Option<String>,
    pg_stat_statements: String,
}

async fn run_inner(args: &Args, ctx: RunContext) -> Result<i32> {
    // --- schema, durability, seeding -------------------------------------
    let seed_ledger =
        qbit_prism_server::ledger::Ledger::connect(&ctx.direct_url, "load-seed".into(), 4, true)
            .await
            .context("initialising the schema")?;
    let (fsync, full_page_writes, synchronous_commit) =
        cluster::durability(&seed_ledger.pool).await?;
    let durability: BTreeMap<String, String> = [
        ("fsync", fsync.clone()),
        ("full_page_writes", full_page_writes.clone()),
        ("synchronous_commit", synchronous_commit.clone()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect();
    if durability.values().any(|value| value != "on") {
        seed_ledger.pool.close().await;
        bail!(
            "PostgreSQL durability is not on (fsync={fsync} full_page_writes={full_page_writes} \
             synchronous_commit={synchronous_commit}); refusing to run the load"
        );
    }
    let postgres_version = cluster::server_version(&seed_ledger.pool).await?;

    let bits = qbit_prism_server::codec::parse_u32_hex(window::TEMPLATE_BITS)?;
    let solution = window::solve_window(bits, args.window_shares)?;
    let seed = window::SeedPlan::new(
        args.window_shares,
        solution.scaled_share_difficulty,
        solution.scaled_network_difficulty,
        args.seed_share_bytes,
    )?;
    let seed_stats = seed.load(&seed_ledger.pool, "load-seed").await?;
    let window_at_start =
        window::observed_window_length(&seed_ledger.pool, solution.scaled_network_difficulty)
            .await?;
    seed_ledger.pool.close().await;

    // A side pool, outside the frontends' path and outside the delay proxy.
    let side = PgPoolOptions::new()
        .max_connections(6)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&ctx.direct_url)
        .await
        .context("opening the harness side pool")?;
    if ctx.pg_stat_statements.starts_with("loaded") {
        let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .execute(&side)
            .await;
    }
    let observed_replication = cluster::detect_replication(&side).await?;

    // --- delay proxy ------------------------------------------------------
    let upstream: std::net::SocketAddr = host_port(&ctx.direct_url)?
        .parse()
        .context("the database host:port is not a socket address")?;
    let delay_proxy = proxy::DelayProxy::open(upstream).await?;
    let proxied_url = rewrite_host(&ctx.direct_url, &delay_proxy.url_host())?;
    let direct_rtt = proxy::measure_select1_millis(&ctx.direct_url, 21)
        .await
        .ok();
    let proxied_rtt_idle = proxy::measure_select1_millis(&proxied_url, 21).await.ok();

    // --- frontends --------------------------------------------------------
    let per_frontend = args.sessions.div_ceil(args.frontends);
    let shared_env = SharedEnvironment {
        rpc_url: ctx.node_url.clone(),
        rpc_user: "qbit".into(),
        rpc_password: format!("load-{}", ctx.run_tag),
        share_difficulty: format!("{}", solution.share_difficulty),
        max_difficulty: "1024".into(),
        database_max_connections: args.db_max_connections,
        runtime_workers: args.runtime_workers,
        stratum_max_connections: (per_frontend * 2 + 64).max(384),
        stratum_max_pending_initial_jobs: (per_frontend + 16).max(128),
        share_commit_timeout_seconds: format!("{}", args.share_commit_timeout_seconds),
        blockpoll_seconds: format!("{}", args.blockpoll_seconds),
        rust_log: "info".into(),
    };
    let mut frontends: Vec<Frontend> = Vec::new();
    let mut blocked: Vec<BlockedLog> = Vec::new();
    for index in 0..args.frontends {
        let instance_id = format!("load-fe-{index}");
        let spec = FrontendSpec {
            index,
            database_url: with_application_name(&proxied_url, &instance_id),
            instance_id,
            stratum_port: free_port()?,
            audit_port: free_port()?,
        };
        let environment = frontend::frontend_environment(&shared_env, &spec);
        let mut child = Frontend::launch(ctx.server_bin.clone(), spec, environment, &ctx.log_dir)?;
        // Frontend 1 first: concurrent first-boot migrations wait inside a
        // transaction bounded by the 5 s lock_timeout.
        let ready = child
            .wait_ready(Duration::from_secs(args.work_timeout))
            .await;
        if let Err(error) = ready {
            blocked.extend(scan_logs(&child.read_stderr()));
            frontends.push(child);
            return finish_blocked(args, &ctx, frontends, blocked, error.to_string()).await;
        }
        frontends.push(child);
    }
    for child in &frontends {
        blocked.extend(scan_logs(&child.read_stderr()));
    }
    if blocked.iter().any(classify::is_hard_block) {
        let text = blocked
            .iter()
            .find(|log| classify::is_hard_block(log))
            .map(|log| log.line.clone())
            .unwrap_or_default();
        return finish_blocked(args, &ctx, frontends, blocked, text).await;
    }

    // --- samplers ---------------------------------------------------------
    let process_samplers: Vec<ProcessSampler> = frontends
        .iter()
        .map(|child| {
            ProcessSampler::start(
                child.spec.instance_id.clone(),
                child.pid(),
                Duration::from_millis(args.process_sample_interval_ms),
            )
        })
        .collect();
    // ORDER_LOCK is database-wide, so the sampler has to know which backends
    // are this run's. `application_name` only works if the driver carries it,
    // which is checked here rather than assumed.
    let frontend_names: Vec<String> = frontends
        .iter()
        .map(|child| child.spec.instance_id.clone())
        .collect();
    let live_names: Vec<String> = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT COALESCE(application_name,'') FROM pg_stat_activity \
         WHERE datname = current_database()",
    )
    .fetch_all(&side)
    .await
    .unwrap_or_default();
    let attributed: Vec<String> = frontend_names
        .iter()
        .filter(|name| live_names.contains(name))
        .cloned()
        .collect();
    let (sampler_names, attribution) = if attributed.len() == frontend_names.len() {
        (
            frontend_names.clone(),
            "application_name carried in PRISM_DATABASE_URL and seen in pg_stat_activity"
                .to_owned(),
        )
    } else {
        (
            Vec::new(),
            format!(
                "every ORDER_LOCK waiter in this database is counted: the driver carried                  application_name for {} of {} frontends ({:?} seen). A foreign holder of the                  same advisory lock would distort these numbers.",
                attributed.len(),
                frontend_names.len(),
                live_names
            ),
        )
    };
    let lock_sampler = LockSampler::start(
        side.clone(),
        Duration::from_millis(args.lock_sample_interval_ms),
        sampler_names,
        attribution,
    );

    // --- sessions ---------------------------------------------------------
    let (events_tx, mut events_rx) = tokio::sync::mpsc::unbounded_channel();
    let collected = Arc::new(Mutex::new(Collected::default()));
    let collector = {
        let collected = collected.clone();
        tokio::spawn(async move {
            while let Some(event) = events_rx.recv().await {
                let mut state = collected.lock().expect("collector lock");
                match event {
                    Event::Submit(record) => state.submits.push(*record),
                    Event::Reconnect(record) => state.reconnects.push(record),
                    Event::Tip { session, tip, at } => state.tips.push((session, tip, at)),
                    Event::DiscardedBlockSolution { .. } => state.discarded_block_solutions += 1,
                    Event::DiscardedOffer { .. } => state.discarded_offers += 1,
                    Event::DifficultyMismatch {
                        session,
                        advertised,
                        configured,
                    } => state
                        .difficulty_mismatches
                        .push((session, advertised, configured)),
                    Event::Connected { .. } => state.connects += 1,
                    Event::Disconnected {
                        session,
                        frontend,
                        reason,
                    } => state.disconnects.push((session, frontend, reason)),
                    Event::Failure { session, error } => state.failures.push((session, error)),
                }
            }
        })
    };
    let shared_session = Arc::new(SessionShared {
        phase: std::sync::RwLock::new("setup".to_owned()),
        events: events_tx,
    });
    let mut sessions: Vec<SessionHandle> = Vec::with_capacity(args.sessions);
    for index in 0..args.sessions {
        let frontend_index = index % args.frontends;
        let config = SessionConfig {
            index,
            username: format!("{}.s{index:05}", ctx.payout_address),
            password: "x".into(),
            share_difficulty: solution.share_difficulty,
            version_rolling_mask: qbit_prism_server::codec::VERSION_ROLLING_MASK,
            connect_timeout: Duration::from_secs(20),
            handshake_timeout: Duration::from_secs(args.work_timeout.min(120)),
        };
        sessions.push(client::spawn_session(
            config,
            frontend_index,
            frontends[frontend_index].stratum_address(),
            shared_session.clone(),
            args.max_outstanding_per_session,
        ));
    }
    // Every session must hold work before the first phase starts.
    let work_deadline = Instant::now() + Duration::from_secs(args.work_timeout);
    loop {
        let connected = collected.lock().expect("collector lock").connects;
        if connected as usize >= args.sessions {
            break;
        }
        if Instant::now() >= work_deadline {
            for child in &frontends {
                blocked.extend(scan_logs(&child.read_stderr()));
            }
            for session in &sessions {
                let _ = session.control.send(client::Control::Stop);
            }
            let text = blocked
                .first()
                .map(|log| log.line.clone())
                .unwrap_or_else(|| {
                    format!(
                        "only {connected} of {} sessions received work",
                        args.sessions
                    )
                });
            lock_sampler.stop();
            for sampler in &process_samplers {
                sampler.stop();
            }
            return finish_blocked(args, &ctx, frontends, blocked, text).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // --- phases -----------------------------------------------------------
    let plans = phases(args)?;
    let mut runs: Vec<PhaseRun> = Vec::new();
    let mut aborted: Option<String> = None;
    let mut external_tips: Vec<crate::node::TipChange> = Vec::new();
    let mut remaining_blocks = args.scheduled_blocks;
    let mut remaining_tips = args.external_tips;
    for plan in &plans {
        *shared_session.phase.write().expect("phase lock") = plan.name.clone();
        delay_proxy.set_delay_millis(plan.database_delay_ms);
        let replication_start = cluster::observe_replication(&side, &plan.name).await?;
        measure::reset_statement_stats(&side).await;
        let mut before_scrapes = Vec::new();
        for child in &frontends {
            before_scrapes
                .push(measure::scrape_metrics(&child.spec.instance_id, &child.metrics_url()).await);
        }
        let started = Instant::now();
        let started_wall = chrono::Utc::now();
        let outcome = drive_phase(
            args,
            plan,
            &sessions,
            &mut frontends,
            &process_samplers,
            &ctx,
            &mut external_tips,
            &mut remaining_blocks,
            &mut remaining_tips,
            &collected,
        )
        .await?;
        let ended = Instant::now();
        let ended_wall = chrono::Utc::now();
        let mut after_scrapes = Vec::new();
        for child in &frontends {
            after_scrapes
                .push(measure::scrape_metrics(&child.spec.instance_id, &child.metrics_url()).await);
        }
        let replication_end = cluster::observe_replication(&side, &plan.name).await?;
        let mut lock = lock_sampler.summarize(started, ended);
        lock.advisory_lock_statement = measure::advisory_lock_statement(&side).await;
        let processes = process_samplers
            .iter()
            .map(|sampler| {
                sampler.summarize(sampler.elapsed_of(started), sampler.elapsed_of(ended))
            })
            .collect();
        runs.push(PhaseRun {
            plan: plan.clone(),
            started_wall,
            ended_wall,
            duration_millis: ended.saturating_duration_since(started).as_millis() as u64,
            tokens: outcome.tokens,
            dispatched: outcome.dispatched,
            shortfall: outcome.shortfall,
            lock,
            processes,
            ack_deltas: before_scrapes
                .iter()
                .zip(after_scrapes.iter())
                .map(|(before, after)| measure::ack_delta(before, after))
                .collect(),
            replication_start,
            replication_end,
            proxy_delay_configured_ms: plan.database_delay_ms,
            min_mem_available_kib: outcome.min_mem_available_kib,
            scheduled_blocks: outcome.scheduled_blocks,
            frontend_restarts: outcome.frontend_restarts,
            mid_flight_indeterminate: outcome.indeterminate,
        });
        if let Some(reason) = outcome.aborted {
            aborted = Some(reason);
            break;
        }
    }
    delay_proxy.set_delay_millis(0);
    *shared_session.phase.write().expect("phase lock") = "teardown".to_owned();

    // --- stop the load ----------------------------------------------------
    // Quiesce first: a socket closed with a submit outstanding manufactures an
    // indeterminate share that no phase asked for, and the run would then
    // report a durability finding it created itself.
    for session in &sessions {
        let _ = session.control.send(client::Control::Pause);
    }
    let drain_deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < drain_deadline
        && sessions
            .iter()
            .any(|session| session.outstanding.load(Ordering::Relaxed) > 0)
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let undrained: usize = sessions
        .iter()
        .map(|session| session.outstanding.load(Ordering::Relaxed))
        .sum();
    for session in &sessions {
        let _ = session.control.send(client::Control::Stop);
    }
    for session in sessions.drain(..) {
        let _ = tokio::time::timeout(Duration::from_secs(15), session.task).await;
    }
    lock_sampler.stop();
    for sampler in &process_samplers {
        sampler.stop();
    }
    drop(shared_session);
    let _ = tokio::time::timeout(Duration::from_secs(10), collector).await;
    let collected = std::mem::take(&mut *collected.lock().expect("collector lock"));

    for child in &frontends {
        blocked.extend(scan_logs(&child.read_stderr()));
    }
    let window_at_end =
        window::observed_window_length(&side, solution.scaled_network_difficulty).await?;

    // --- reconciliation ---------------------------------------------------
    let writer_ids: Vec<String> = frontends
        .iter()
        .map(|child| child.spec.instance_id.clone())
        .collect();
    let committed: BTreeSet<String> = sqlx::query_scalar::<_, String>(
        "SELECT share_id FROM qbit_share_ledger \
         WHERE accepted AND writer_id = ANY($1) AND share_id LIKE $2",
    )
    .bind(&writer_ids)
    .bind(format!("{}%", ctx.share_prefix.replace('_', "\\_")))
    .fetch_all(&side)
    .await
    .context("reading this run's committed shares")?
    .into_iter()
    .collect();

    let mut phase_reconciliations: Vec<(String, digest::Reconciliation)> = Vec::new();
    for phase in &runs {
        let (offered, acknowledged) =
            offered_and_acknowledged(&collected.submits, &phase.plan.name);
        phase_reconciliations.push((
            phase.plan.name.clone(),
            digest::reconcile(offered, acknowledged, &committed),
        ));
    }
    let attribution = digest::attribute_unexpected(
        &committed,
        &phase_reconciliations
            .iter()
            .map(|(name, rec)| (name.clone(), rec))
            .collect::<Vec<_>>(),
    );
    let mut unexpected_by_phase: HashMap<String, usize> = HashMap::new();
    for (name, rows) in &attribution.by_phase {
        unexpected_by_phase.insert(name.clone(), rows.len());
    }

    // --- artifact ---------------------------------------------------------
    let configuration = frontend::configuration_block(&frontends[0].environment)?;
    let host = measure::host_facts();
    let replication_rows = json!({
        "declared": ctx.declared_replication.as_str(),
        "observed": observed_replication.as_str(),
        "standby_name": cluster::STANDBY_NAME,
        "slot": cluster::STANDBY_SLOT,
        "standby_url_present": ctx.managed_standby.is_some(),
        "observations": runs.iter().flat_map(|phase| {
            vec![
                json!({"phase": phase.plan.name, "at": "start", "observation": phase.replication_start}),
                json!({"phase": phase.plan.name, "at": "end", "observation": phase.replication_end}),
            ]
        }).collect::<Vec<_>>(),
    });
    let proxy_block = json!({
        "kind": "in-harness tokio TCP proxy",
        "delay_semantics": proxy::DELAY_SEMANTICS,
        "configured_slow_database_delay_milliseconds": args.slow_db_delay_ms,
        "direct_select1_median_milliseconds": direct_rtt,
        "proxied_select1_median_milliseconds_at_zero_delay": proxied_rtt_idle,
        "measured_proxy_overhead_milliseconds": match (direct_rtt, proxied_rtt_idle) {
            (Some(direct), Some(proxied)) => Some(proxied - direct),
            _ => None,
        },
    });
    let settings = profile::show_all(&side).await?;
    let profile_document = profile::build(
        settings,
        &postgres_version,
        replication_rows.clone(),
        proxy_block.clone(),
        host.clone(),
        json!(frontends
            .iter()
            .map(|child| json!({
                "instance_id": child.spec.instance_id,
                "stratum_port": child.spec.stratum_port,
                "audit_port": child.spec.audit_port,
                "environment": frontend::redacted(&child.environment),
            }))
            .collect::<Vec<_>>()),
    );
    let profile_canonical = profile::canonical_json(&profile_document);
    let profile_digest = profile::digest(&profile_canonical);
    let profile_path = args.out.join("database-profile.json");
    std::fs::write(&profile_path, format!("{profile_canonical}\n"))?;

    let subject: BTreeMap<String, String> = [
        ("coordinator_revision", ctx.revision.clone()),
        ("coordinator_image_digest", ctx.server_digest.clone()),
        ("postgres_server_version", postgres_version.clone()),
        ("database_profile_sha256", profile_digest.clone()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect();

    let slow_delay_observed = runs
        .iter()
        .find(|phase| phase.plan.name == "slow_database")
        .map(|phase| phase.proxy_delay_configured_ms as f64)
        .unwrap_or(args.slow_db_delay_ms as f64);
    let mut phase_evidence = Vec::new();
    let mut artifact_phase_names = Vec::new();
    for phase in &runs {
        if !phase.plan.in_artifact {
            continue;
        }
        let reconciliation = phase_reconciliations
            .iter()
            .find(|(name, _)| *name == phase.plan.name)
            .map(|(_, rec)| rec)
            .context("missing reconciliation")?;
        let latency = phase_latency(&collected.submits, &phase.plan.name);
        let reconnects = collected
            .reconnects
            .iter()
            .filter(|record| record.phase == phase.plan.name && record.completed)
            .count() as u64;
        phase_evidence.push(PhaseEvidence {
            name: phase.plan.name.clone(),
            duration_millis: phase.duration_millis,
            offered: reconciliation.offered.len() as u64,
            acknowledged: reconciliation.acknowledged.len() as u64,
            committed: reconciliation.committed.len() as u64,
            rejected_valid: rejected_valid_count(&collected.submits, &phase.plan.name),
            missing: reconciliation.missing.len() as u64,
            unexpected: *unexpected_by_phase.get(&phase.plan.name).unwrap_or(&0) as u64,
            acknowledged_digest: reconciliation.acknowledged_digest(),
            committed_digest: reconciliation.committed_digest(),
            ack_p50_millis: latency.p50.unwrap_or(0.0),
            ack_p99_millis: latency.p99.unwrap_or(0.0),
            reconnect_events: (phase.plan.name == "reconnect").then_some(reconnects),
            database_delay_millis: (phase.plan.name == "slow_database")
                .then_some(slow_delay_observed),
        });
        artifact_phase_names.push(phase.plan.name.clone());
    }
    let artifact_submits: Vec<&SubmitRecord> = collected
        .submits
        .iter()
        .filter(|record| artifact_phase_names.contains(&record.phase))
        .collect();
    let overall_latency = measure::summarize(
        artifact_submits
            .iter()
            .filter_map(|record| record.latency_millis)
            .collect(),
        "client monotonic",
    );
    let union_ack: BTreeSet<String> = phase_evidence
        .iter()
        .flat_map(|phase| {
            phase_reconciliations
                .iter()
                .find(|(name, _)| *name == phase.name)
                .map(|(_, rec)| rec.acknowledged.clone())
                .unwrap_or_default()
        })
        .collect();
    let union_committed: BTreeSet<String> = phase_evidence
        .iter()
        .flat_map(|phase| {
            phase_reconciliations
                .iter()
                .find(|(name, _)| *name == phase.name)
                .map(|(_, rec)| rec.committed.clone())
                .unwrap_or_default()
        })
        .collect();
    let inputs = ArtifactInputs {
        artifact_kind: ctx.artifact_kind.clone(),
        run_id: ctx.run_id,
        generated_at: chrono::Utc::now(),
        subject: subject.clone(),
        durability: durability.clone(),
        configuration: configuration.clone(),
        forecast_peak_shares_per_second: format!("{}", args.forecast_peak_shares_per_second),
        ack_p99_limit_milliseconds: format!("{}", args.ack_p99_limit_ms),
        overall_ack_p50_millis: overall_latency.p50.unwrap_or(0.0),
        overall_ack_p99_millis: overall_latency.p99.unwrap_or(0.0),
        overall_acknowledged_digest: digest::share_id_digest(union_ack.iter().map(String::as_str)),
        overall_committed_digest: digest::share_id_digest(
            union_committed.iter().map(String::as_str),
        ),
        phases: phase_evidence.clone(),
    };
    let document = artifact::build(&inputs)?;
    let evidence_path = args.out.join("capacity-evidence.json");
    report::write_json(&evidence_path, &document)?;
    let options = artifact::validation_options(&inputs);
    let verdict = artifact::verdict(&document, &options);
    let command = artifact::cli_command(
        &inputs,
        &evidence_path.display().to_string(),
        &ctx.server_bin.display().to_string(),
    );

    // --- side report ------------------------------------------------------
    let slowest_rate = phase_evidence
        .iter()
        .map(|phase| {
            phase.acknowledged as f64
                / (phase.duration_millis as f64 / 1000.0).max(f64::MIN_POSITIVE)
        })
        .fold(f64::INFINITY, f64::min);
    let worst_p99 = phase_evidence
        .iter()
        .map(|phase| phase.ack_p99_millis)
        .max_by(f64::total_cmp);
    let harness_bugs: Vec<&SubmitRecord> = collected
        .submits
        .iter()
        .filter(|record| !record.reoffer && bug_rejection(record))
        .collect();
    let (durability_findings, divergences) = classify_gaps(
        &runs,
        &phase_reconciliations,
        &attribution,
        &collected.submits,
        args.share_commit_timeout_seconds,
    );
    let node_submissions = ctx.node_state.submissions();
    let tip_changes = ctx.node_state.tip_changes();
    let side_report = json!({
        "schema": report::SCHEMA,
        "run_id": ctx.run_id.to_string(),
        "run_tag": ctx.run_tag,
        "started_at": ctx.started_wall.to_rfc3339(),
        "finished_at": chrono::Utc::now().to_rfc3339(),
        "aborted": aborted,
        "dirty": ctx.dirty,
        "artifact_kind": ctx.artifact_kind,
        "host": host,
        "versions": {
            "harness_version": env!("CARGO_PKG_VERSION"),
            "harness_build_profile": ctx.harness_profile,
            "server_binary": ctx.server_bin.display().to_string(),
            "server_build_profile": ctx.server_profile.as_str(),
            "server_binary_sha256": ctx.server_digest,
            "coordinator_revision": ctx.revision,
            "postgres_server_version": postgres_version,
            "rustc_target": std::env::consts::ARCH,
        },
        "file_descriptor_limit": {"before": ctx.fd_before, "after": ctx.fd_after},
        "topology": {
            "frontends": args.frontends,
            "sessions": args.sessions,
            "sessions_per_frontend": per_frontend,
            "max_outstanding_per_session": args.max_outstanding_per_session,
            "plan": args.plan,
            "payout_address": ctx.payout_address,
            "share_id_prefix": ctx.share_prefix,
            "writer_ids": writer_ids,
        },
        "frontend_environment": frontends.iter().map(|child| json!({
            "instance_id": child.spec.instance_id,
            "stratum_port": child.spec.stratum_port,
            "audit_port": child.spec.audit_port,
            "restarts": child.restarts,
            "stdout_log": child.stdout_path.display().to_string(),
            "stderr_log": child.stderr_path.display().to_string(),
            "environment": frontend::redacted(&child.environment),
        })).collect::<Vec<_>>(),
        "unread_configuration_keys": frontend::UNREAD_CONFIGURATION_KEYS,
        "window": {
            "template_bits": window::TEMPLATE_BITS,
            "scaled_network_difficulty": solution.scaled_network_difficulty.to_string(),
            "window_weight": solution.window_weight.to_string(),
            "share_difficulty_diff1": format!("{}", solution.share_difficulty),
            "scaled_share_difficulty": solution.scaled_share_difficulty.to_string(),
            "requested_window_shares": solution.requested_window,
            "computed_window_shares": solution.computed_window,
            "ledger_window_shares_at_start": window_at_start,
            "ledger_window_shares_at_end": window_at_end,
            "expected_hashes_per_share": solution.hashes_per_share,
            "expected_hashes_per_block": solution.hashes_per_block,
            "seed": {
                "rows": seed_stats.rows,
                "seconds": seed_stats.seconds,
                "rows_per_second": seed_stats.rows_per_second,
                "serialized_bytes": seed_stats.serialized_bytes,
                "target_share_bytes": seed.target_share_bytes(),
            },
        },
        "database": {
            "mode": if args.database_url.is_some() { "external" } else { "managed" },
            "durability": durability,
            "replication": replication_rows,
            "pg_stat_statements": ctx.pg_stat_statements,
            "delay_proxy": proxy_block,
            "database_profile_sha256": profile_digest,
            "database_profile_path": profile_path.display().to_string(),
        },
        "phases": runs.iter().map(|phase| phase_report(
            phase, &collected, &phase_reconciliations, &unexpected_by_phase,
        )).collect::<Vec<_>>(),
        "reconciliation": {
            "digest_definition": digest::DIGEST_DEFINITION,
            "committed_rows_for_this_run": committed.len(),
            "unexpected_attributed_by_phase": attribution.by_phase.iter()
                .map(|(name, rows)| json!({"phase": name, "count": rows.len(),
                    "sample": rows.iter().take(10).collect::<Vec<_>>()}))
                .collect::<Vec<_>>(),
            "unexpected_outside_phases": {
                "count": attribution.outside_phases.len(),
                "sample": attribution.outside_phases.iter().take(10).collect::<Vec<_>>(),
            },
        },
        "time_to_usable_work": time_to_usable_work(&external_tips, &collected, args.sessions),
        "node": {
            "url": ctx.node_url,
            "template_bits": window::TEMPLATE_BITS,
            "submissions": node_submissions,
            "tip_changes": tip_changes.iter().map(|change| json!({
                "hash": change.hash, "height": change.height,
                "origin": change.origin, "wall": change.wall.to_rfc3339(),
            })).collect::<Vec<_>>(),
            "rpc_call_counts": ctx.node_state.rpc_call_counts(),
        },
        "client": {
            "discarded_block_solutions": collected.discarded_block_solutions,
            "discarded_offers": collected.discarded_offers,
            "difficulty_mismatches": collected.difficulty_mismatches.iter()
                .map(|(session, advertised, configured)| json!({
                    "session": session, "advertised": advertised, "configured": configured}))
                .collect::<Vec<_>>(),
            "failures": collected.failures.iter().take(200)
                .map(|(session, error)| json!({"session": session, "error": error}))
                .collect::<Vec<_>>(),
            "failure_count": collected.failures.len(),
            "connects": collected.connects,
            "disconnects": collected.disconnects.len(),
        },
        "reconnects": reconnect_report(&collected),
        "mid_flight_kill": mid_flight_report(&runs, &collected, &committed),
        "rejections": rejection_report(&collected.submits),
        "harness_bug_rejections": harness_bugs.iter().take(50).map(|record| json!({
            "share_id": record.share_id, "phase": record.phase, "job_id": record.job_id,
            "outcome": describe_outcome(&record.outcome),
        })).collect::<Vec<_>>(),
        "harness_bug_rejection_count": harness_bugs.len(),
        "blocked": {
            "blocked": false,
            "log_matches": blocked,
        },
        "durability_findings": durability_findings,
        "ack_commit_divergence": {
            "definition": "a share PostgreSQL holds that the server refused with \
                           ledger-confirmation-failed. Nothing was lost: the share is credited \
                           in the payout window, but the miner was told it was not confirmed.",
            "mechanism": "coordinator.rs wraps the append in \
                          tokio::time::timeout(share_commit_timeout, save); when it fires the \
                          sqlx future is dropped mid-COMMIT and PostgreSQL can still commit.",
            "server_issue": "Qbit-Org/qbit-mining-bootstrap#324",
            "share_commit_timeout_seconds": args.share_commit_timeout_seconds,
            "count": divergences.len(),
            "shares": divergences,
        },
        "honest_value_notes": report::honest_value_notes(),
        "drain": {
            "note": "sessions quiesce before the run closes their sockets; anything still \
                     outstanding here is a genuine lost acknowledgement",
            "submits_outstanding_at_stop": undrained,
        },
        "validator": {
            "verdict": verdict,
            "command": command,
            "artifact_path": evidence_path.display().to_string(),
            "forecast_used": args.forecast_peak_shares_per_second,
            "slowest_artifact_phase_rate_shares_per_second": slowest_rate,
            "suggested_forecast_for_a_valid_artifact": (slowest_rate / 2.0).max(0.0),
            "ack_p99_limit_used_milliseconds": args.ack_p99_limit_ms,
            "worst_artifact_phase_ack_p99_milliseconds": worst_p99,
            "suggested_ack_p99_limit_milliseconds": worst_p99.map(|p99| {
                // Round up to the next 100 ms, and never above the commit
                // timeout the consumer checks against.
                ((p99 / 100.0).ceil() * 100.0).min(args.share_commit_timeout_seconds * 1000.0)
            }),
        },
    });
    let report_path = args.out.join("load-harness-report.json");
    report::write_json(&report_path, &side_report)?;

    // --- exit code --------------------------------------------------------
    println!("{}", summary_text(&side_report, &verdict, &command));
    for mut child in frontends {
        child.kill();
    }
    side.close().await;
    if let Some(reason) = aborted {
        eprintln!("run aborted: {reason}");
        return Ok(EXIT_ABORTED);
    }
    if !durability_findings
        .as_array()
        .map(Vec::is_empty)
        .unwrap_or(true)
    {
        eprintln!(
            "durability findings recorded; see {}",
            report_path.display()
        );
        return Ok(EXIT_DURABILITY);
    }
    if !harness_bugs.is_empty() {
        eprintln!(
            "{} rejections classified as harness bugs; see {}",
            harness_bugs.len(),
            report_path.display()
        );
        return Ok(EXIT_HARNESS_BUG_REJECTIONS);
    }
    if !divergences.is_empty() {
        eprintln!(
            "{} shares committed after the server refused them with \
             ledger-confirmation-failed; see {}",
            divergences.len(),
            report_path.display()
        );
        return Ok(EXIT_ACK_COMMIT_DIVERGENCE);
    }
    Ok(EXIT_OK)
}

// --- phase driving -------------------------------------------------------

struct PhaseOutcome {
    tokens: u64,
    dispatched: u64,
    shortfall: u64,
    min_mem_available_kib: Option<u64>,
    aborted: Option<String>,
    scheduled_blocks: usize,
    frontend_restarts: usize,
    indeterminate: Vec<SubmitRecord>,
}

#[allow(clippy::too_many_arguments)]
async fn drive_phase(
    args: &Args,
    plan: &PhasePlan,
    sessions: &[SessionHandle],
    frontends: &mut [Frontend],
    samplers: &[ProcessSampler],
    ctx: &RunContext,
    external_tips: &mut Vec<crate::node::TipChange>,
    remaining_blocks: &mut usize,
    remaining_tips: &mut usize,
    collected: &Arc<Mutex<Collected>>,
) -> Result<PhaseOutcome> {
    let started = Instant::now();
    let duration = Duration::from_secs(plan.seconds);
    let cursor = AtomicUsize::new(0);
    let mut outcome = PhaseOutcome {
        tokens: 0,
        dispatched: 0,
        shortfall: 0,
        min_mem_available_kib: measure::mem_available_kib(),
        aborted: None,
        scheduled_blocks: 0,
        frontend_restarts: 0,
        indeterminate: Vec::new(),
    };
    // Event schedule inside the phase.
    let reconnect_interval = if plan.reconnects {
        Some(duration.as_secs_f64() / (args.reconnect_target as f64 + 2.0))
    } else {
        None
    };
    let restart_at = (plan.reconnects && args.frontends >= 2).then(|| duration.as_secs_f64() / 3.0);
    let mut next_reconnect = reconnect_interval.unwrap_or(f64::INFINITY);
    let mut reconnect_cursor = 0usize;
    let mut restart_done = restart_at.is_none();
    let block_times: Vec<f64> = if plan.name == "steady_state" && *remaining_blocks > 0 {
        let count = *remaining_blocks;
        (0..count)
            .map(|index| duration.as_secs_f64() * (index as f64 + 1.0) / (count as f64 + 1.0))
            .collect()
    } else {
        Vec::new()
    };
    let mut block_cursor = 0usize;
    let tip_times: Vec<f64> = if plan.name == "warm_up" && *remaining_tips > 0 {
        let count = *remaining_tips;
        (0..count)
            .map(|index| duration.as_secs_f64() * (index as f64 + 1.0) / (count as f64 + 2.0))
            .collect()
    } else {
        Vec::new()
    };
    let mut tip_cursor = 0usize;
    let mut kill_done = !plan.mid_flight_kill;
    let mut mem_check = Instant::now();

    let mut ticker = tokio::time::interval(Duration::from_millis(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    loop {
        ticker.tick().await;
        let elapsed = started.elapsed();
        if elapsed >= duration {
            break;
        }
        let seconds = elapsed.as_secs_f64();
        // Open-loop token bucket: the clock decides how many shares were
        // offered, and any that cannot be placed are counted as a shortfall
        // rather than deferred into a backlog.
        let want = (seconds * plan.rate).floor() as u64;
        while outcome.tokens < want {
            outcome.tokens += 1;
            if offer_round_robin(sessions, &cursor, args.max_outstanding_per_session) {
                outcome.dispatched += 1;
            } else {
                outcome.shortfall += 1;
            }
        }
        if seconds >= next_reconnect {
            next_reconnect += reconnect_interval.unwrap_or(f64::INFINITY);
            if let Some(session) = sessions.get(reconnect_cursor % sessions.len()) {
                let _ = session.control.send(client::Control::Reconnect {
                    reason: "client-initiated".into(),
                });
            }
            reconnect_cursor += 1;
        }
        if let Some(at) = restart_at {
            if !restart_done && seconds >= at {
                restart_done = true;
                outcome.frontend_restarts += 1;
                drained_restart(args, sessions, frontends, samplers, 1).await?;
            }
        }
        if block_cursor < block_times.len() && seconds >= block_times[block_cursor] {
            block_cursor += 1;
            *remaining_blocks = remaining_blocks.saturating_sub(1);
            outcome.scheduled_blocks += 1;
            if let Some(session) = sessions.first() {
                let _ = session.control.send(client::Control::ScheduledBlock);
            }
        }
        if tip_cursor < tip_times.len() && seconds >= tip_times[tip_cursor] {
            tip_cursor += 1;
            *remaining_tips = remaining_tips.saturating_sub(1);
            external_tips.push(ctx.node_state.mint_external_block());
        }
        if !kill_done && seconds >= duration.as_secs_f64() / 3.0 {
            kill_done = true;
            outcome.indeterminate =
                mid_flight_kill(args, sessions, frontends, samplers, collected).await?;
            outcome.frontend_restarts += 1;
        }
        if mem_check.elapsed() >= Duration::from_secs(1) {
            mem_check = Instant::now();
            let available = measure::mem_available_kib();
            if let Some(available) = available {
                outcome.min_mem_available_kib = Some(
                    outcome
                        .min_mem_available_kib
                        .map_or(available, |current| current.min(available)),
                );
                if available < args.min_mem_available_mib * 1024 {
                    outcome.aborted = Some(format!(
                        "MemAvailable fell to {} MiB, below the {} MiB floor",
                        available / 1024,
                        args.min_mem_available_mib
                    ));
                    break;
                }
            }
            for child in frontends.iter_mut() {
                if let Some(status) = child.exited() {
                    outcome.aborted = Some(format!(
                        "{} exited unexpectedly with {status}",
                        child.spec.instance_id
                    ));
                    break;
                }
            }
            if outcome.aborted.is_some() {
                break;
            }
        }
    }
    Ok(outcome)
}

fn offer_round_robin(sessions: &[SessionHandle], cursor: &AtomicUsize, limit: usize) -> bool {
    let count = sessions.len();
    for _ in 0..count {
        let index = cursor.fetch_add(1, Ordering::Relaxed) % count;
        if sessions[index].try_offer(limit) {
            return true;
        }
    }
    false
}

/// Quiesce one frontend's sessions, kill it, restart it and let them reconnect.
async fn drained_restart(
    args: &Args,
    sessions: &[SessionHandle],
    frontends: &mut [Frontend],
    samplers: &[ProcessSampler],
    index: usize,
) -> Result<()> {
    let index = index.min(frontends.len() - 1);
    for session in sessions {
        if session.frontend.load(Ordering::Relaxed) == index {
            let _ = session.control.send(client::Control::Pause);
        }
    }
    // Let outstanding submits settle before the process goes away, so the
    // drained restart produces no indeterminate shares.
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline
        && sessions.iter().any(|session| {
            session.frontend.load(Ordering::Relaxed) == index
                && session.outstanding.load(Ordering::Relaxed) > 0
        })
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    frontends[index].restart()?;
    frontends[index]
        .wait_ready(Duration::from_secs(args.work_timeout))
        .await?;
    if let Some(sampler) = samplers.get(index) {
        sampler.set_pid(frontends[index].pid());
    }
    let address = frontends[index].stratum_address();
    for session in sessions {
        if session.frontend.load(Ordering::Relaxed) == index {
            let _ = session.control.send(client::Control::Retarget {
                frontend: index,
                address: address.clone(),
                reconnect: false,
            });
        }
    }
    Ok(())
}

/// SIGKILL a frontend with submits outstanding, then re-offer every share whose
/// answer was lost, with exactly the header it carried.
async fn mid_flight_kill(
    args: &Args,
    sessions: &[SessionHandle],
    frontends: &mut [Frontend],
    samplers: &[ProcessSampler],
    collected: &Arc<Mutex<Collected>>,
) -> Result<Vec<SubmitRecord>> {
    let index = if frontends.len() >= 2 { 1 } else { 0 };
    let before = collected.lock().expect("collector lock").submits.len();
    frontends[index].kill();
    tokio::time::sleep(Duration::from_millis(500)).await;
    frontends[index].restart()?;
    frontends[index]
        .wait_ready(Duration::from_secs(args.work_timeout))
        .await?;
    if let Some(sampler) = samplers.get(index) {
        sampler.set_pid(frontends[index].pid());
    }
    let address = frontends[index].stratum_address();
    for session in sessions {
        if session.frontend.load(Ordering::Relaxed) == index {
            let _ = session.control.send(client::Control::Retarget {
                frontend: index,
                address: address.clone(),
                reconnect: false,
            });
        }
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let indeterminate: Vec<SubmitRecord> = {
        let state = collected.lock().expect("collector lock");
        state.submits[before.min(state.submits.len())..]
            .iter()
            .filter(|record| matches!(record.outcome, Outcome::NoResponse { .. }))
            .cloned()
            .collect()
    };
    for record in &indeterminate {
        if let Some(session) = sessions.get(record.session) {
            let _ = session.control.send(client::Control::Reoffer {
                share_id: record.share_id.clone(),
                job_id: record.job_id.clone(),
                extranonce2_hex: record.extranonce2_hex.clone(),
                ntime_hex: record.ntime_hex.clone(),
                nonce_hex: record.nonce_hex.clone(),
                header_hex: record.header_hex.clone(),
            });
        }
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(indeterminate)
}

// --- helpers -------------------------------------------------------------

fn git(args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .output()
        .with_context(|| format!("running git {}", args.join(" ")))?;
    ensure!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

fn resolve_server_bin(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        ensure!(path.exists(), "{} does not exist", path.display());
        return Ok(path);
    }
    let own = std::env::current_exe().context("locating this executable")?;
    let sibling = own
        .parent()
        .context("this executable has no directory")?
        .join("qbit-prism-server");
    ensure!(
        sibling.exists(),
        "no qbit-prism-server beside {}; pass --server-bin",
        own.display()
    );
    Ok(sibling)
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

/// `host:port` of a `postgresql://` URL.
pub fn host_port(url: &str) -> Result<String> {
    let rest = url
        .split_once("://")
        .map(|(_, rest)| rest)
        .context("database URL has no scheme")?;
    let rest = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    let authority = rest
        .split(['/', '?'])
        .next()
        .context("database URL has no host")?;
    ensure!(!authority.is_empty(), "database URL has no host");
    Ok(if authority.contains(':') {
        authority.to_owned()
    } else {
        format!("{authority}:5432")
    })
}

/// Add an `application_name` parameter, so `pg_stat_activity` can say which
/// frontend a backend belongs to. sqlx reads it out of the URL; whether it
/// really carried it is verified against `pg_stat_activity`, never assumed.
pub fn with_application_name(url: &str, name: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}application_name={name}")
}

/// Replace the authority of a `postgresql://` URL, keeping user info and path.
pub fn rewrite_host(url: &str, host_port: &str) -> Result<String> {
    let (scheme, rest) = url
        .split_once("://")
        .context("database URL has no scheme")?;
    let (userinfo, hostrest) = match rest.rsplit_once('@') {
        Some((user, host)) => (Some(user), host),
        None => (None, rest),
    };
    let split = hostrest
        .find(['/', '?'])
        .map_or((hostrest, ""), |at| hostrest.split_at(at));
    Ok(match userinfo {
        Some(user) => format!("{scheme}://{user}@{host_port}{}", split.1),
        None => format!("{scheme}://{host_port}{}", split.1),
    })
}

fn scan_logs(text: &str) -> Vec<BlockedLog> {
    text.lines()
        .filter_map(classify::classify_log_line)
        .collect()
}

fn rejection_of(record: &SubmitRecord) -> Option<&Rejection> {
    match &record.outcome {
        Outcome::Rejected(rejection) => Some(rejection),
        _ => None,
    }
}

/// A rejection that means the harness offered work the server was right to
/// refuse. A re-offer's `duplicate-share` is expected and never counted here.
fn bug_rejection(record: &SubmitRecord) -> bool {
    !record.reoffer
        && rejection_of(record)
            .map(|rejection| classify::classify(rejection) == RejectionClass::HarnessBug)
            .unwrap_or(false)
}

fn harness_bug_count(records: &[SubmitRecord], phase: &str) -> u64 {
    records
        .iter()
        .filter(|record| record.phase == phase && bug_rejection(record))
        .count() as u64
}

/// A rejection of a share the harness believed valid: everything except the
/// races the server is entitled to lose. A backend refusal is a capacity
/// result, not a harness defect, but it is still a share that did not get its
/// acknowledgement, so the artifact has to carry it.
fn rejected_valid(record: &SubmitRecord) -> bool {
    !record.reoffer
        && rejection_of(record)
            .map(|rejection| classify::classify(rejection) != RejectionClass::Expected)
            .unwrap_or(false)
}

fn rejected_valid_count(records: &[SubmitRecord], phase: &str) -> u64 {
    records
        .iter()
        .filter(|record| record.phase == phase && rejected_valid(record))
        .count() as u64
}

/// O and A for one phase. A share the server refused in a race it was entitled
/// to lose is not an offered valid share: it never reaches PostgreSQL, and the
/// full census is in the side report. Everything else the harness offered and
/// did not get acknowledged stays in O, so the artifact cannot hide it.
pub fn offered_and_acknowledged(
    records: &[SubmitRecord],
    phase: &str,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut offered = BTreeSet::new();
    let mut acknowledged = BTreeSet::new();
    for record in records.iter().filter(|record| record.phase == phase) {
        if record.reoffer {
            continue;
        }
        match &record.outcome {
            Outcome::Accepted => {
                offered.insert(record.share_id.clone());
                acknowledged.insert(record.share_id.clone());
            }
            Outcome::NoResponse { .. } => {
                offered.insert(record.share_id.clone());
            }
            Outcome::Rejected(rejection) => {
                if classify::classify(rejection) != RejectionClass::Expected {
                    offered.insert(record.share_id.clone());
                }
            }
        }
    }
    (offered, acknowledged)
}

fn phase_latency(records: &[SubmitRecord], phase: &str) -> measure::LatencySummary {
    measure::summarize(
        records
            .iter()
            .filter(|record| record.phase == phase && !record.reoffer)
            .filter_map(|record| record.latency_millis)
            .collect(),
        "client monotonic",
    )
}

fn describe_outcome(outcome: &Outcome) -> Value {
    match outcome {
        Outcome::Accepted => json!({"outcome": "accepted"}),
        Outcome::Rejected(rejection) => json!({
            "outcome": "rejected",
            "code": rejection.code,
            "reason_id": rejection.reason_id,
            "message": rejection.message,
            "class": classify::classify(rejection).as_str(),
        }),
        Outcome::NoResponse { reason } => json!({"outcome": "no-response", "reason": reason}),
    }
}

/// `(phase, reason_id, code, message)`.
type RejectionKey = (String, String, i64, String);
/// Total count, then a count per frontend index.
type RejectionTally = (u64, BTreeMap<usize, u64>);

fn rejection_report(records: &[SubmitRecord]) -> Value {
    let mut by_key: BTreeMap<RejectionKey, RejectionTally> = BTreeMap::new();
    let mut no_response: BTreeMap<String, u64> = BTreeMap::new();
    for record in records {
        match &record.outcome {
            Outcome::Rejected(rejection) => {
                let key = (
                    record.phase.clone(),
                    rejection.reason_id.clone().unwrap_or_default(),
                    rejection.code,
                    rejection.message.clone(),
                );
                let entry = by_key.entry(key).or_default();
                entry.0 += 1;
                *entry.1.entry(record.frontend).or_insert(0) += 1;
            }
            Outcome::NoResponse { .. } => {
                *no_response.entry(record.phase.clone()).or_insert(0) += 1;
            }
            Outcome::Accepted => {}
        }
    }
    json!({
        "by_phase_reason_and_message": by_key.into_iter().map(|((phase, reason, code, message), (count, by_frontend))| {
            let rejection = Rejection { code, reason_id: (!reason.is_empty()).then(|| reason.clone()), message: message.clone() };
            json!({
                "phase": phase, "code": code, "reason_id": reason, "message": message,
                "count": count,
                "class": classify::classify(&rejection).as_str(),
                "rebuild_pending": classify::is_rebuild_pending(&rejection),
                "by_frontend": by_frontend.into_iter().map(|(frontend, count)| json!({"frontend": frontend, "count": count})).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
        "no_response_by_phase": no_response,
    })
}

fn reconnect_report(collected: &Collected) -> Value {
    let mut by_phase: BTreeMap<String, (u64, u64, Vec<f64>)> = BTreeMap::new();
    for record in &collected.reconnects {
        let entry = by_phase.entry(record.phase.clone()).or_default();
        if record.completed {
            entry.0 += 1;
            entry.2.push(record.seconds);
        } else {
            entry.1 += 1;
        }
    }
    json!({
        "definition": "a completed reconnect is a close followed by a re-authorize and a job",
        "by_phase": by_phase.into_iter().map(|(phase, (completed, failed, seconds))| {
            let summary = measure::summarize(seconds.iter().map(|s| s * 1000.0).collect(), "client monotonic");
            json!({"phase": phase, "completed": completed, "failed_attempts": failed,
                   "time_to_reconnect_milliseconds": summary})
        }).collect::<Vec<_>>(),
        "total_completed": collected.reconnects.iter().filter(|r| r.completed).count(),
        "total_failed_attempts": collected.reconnects.iter().filter(|r| !r.completed).count(),
    })
}

fn mid_flight_report(
    runs: &[PhaseRun],
    collected: &Collected,
    committed: &BTreeSet<String>,
) -> Value {
    let Some(phase) = runs.iter().find(|phase| phase.plan.mid_flight_kill) else {
        return json!({"ran": false});
    };
    let shares: Vec<Value> = phase
        .mid_flight_indeterminate
        .iter()
        .map(|record| {
            let reoffer = collected
                .submits
                .iter()
                .find(|other| other.reoffer && other.share_id == record.share_id);
            json!({
                "share_id": record.share_id,
                "session": record.session,
                "frontend": record.frontend,
                "job_id": record.job_id,
                "classification": "indeterminate",
                "reoffer_answer": reoffer.map(|other| describe_outcome(&other.outcome)),
                "in_postgres": committed.contains(&record.share_id),
            })
        })
        .collect();
    json!({
        "ran": true,
        "phase": phase.plan.name,
        "indeterminate_shares": shares.len(),
        "shares": shares,
    })
}

fn time_to_usable_work(
    tips: &[crate::node::TipChange],
    collected: &Collected,
    sessions: usize,
) -> Value {
    let entries: Vec<Value> = tips
        .iter()
        .map(|tip| {
            let mut first: HashMap<usize, Instant> = HashMap::new();
            for (session, seen, at) in &collected.tips {
                if *seen == tip.hash && *at >= tip.monotonic {
                    first.entry(*session).or_insert(*at);
                }
            }
            let deltas: Vec<f64> = first
                .values()
                .map(|at| at.saturating_duration_since(tip.monotonic).as_secs_f64() * 1000.0)
                .collect();
            let all_seen = first
                .values()
                .max()
                .map(|at| at.saturating_duration_since(tip.monotonic).as_secs_f64() * 1000.0);
            json!({
                "tip": tip.hash,
                "height": tip.height,
                "origin": tip.origin,
                "minted_at": tip.wall.to_rfc3339(),
                "sessions_with_work": first.len(),
                "sessions_total": sessions,
                "latency_milliseconds": measure::summarize(deltas, "client monotonic against the node's tip stamp"),
                "all_sessions_milliseconds": all_seen,
            })
        })
        .collect();
    json!({
        "definition": "t1 - t0, where t0 is the fake node's tip stamp and t1 is the first \
                       mining.notify whose prevhash resolves to that tip",
        "tips": entries,
    })
}

fn phase_report(
    phase: &PhaseRun,
    collected: &Collected,
    reconciliations: &[(String, digest::Reconciliation)],
    unexpected_by_phase: &HashMap<String, usize>,
) -> Value {
    let reconciliation = reconciliations
        .iter()
        .find(|(name, _)| *name == phase.plan.name)
        .map(|(_, rec)| rec);
    let latency = phase_latency(&collected.submits, &phase.plan.name);
    let seconds = phase.duration_millis as f64 / 1000.0;
    let acknowledged = reconciliation
        .map(|rec| rec.acknowledged.len())
        .unwrap_or(0);
    json!({
        "name": phase.plan.name,
        "in_artifact": phase.plan.in_artifact,
        "started_at": phase.started_wall.to_rfc3339(),
        "ended_at": phase.ended_wall.to_rfc3339(),
        "duration_seconds": seconds,
        "target_rate_shares_per_second": phase.plan.rate,
        "offered_tokens": phase.tokens,
        "dispatched": phase.dispatched,
        "shortfall": phase.shortfall,
        "achieved_rate_shares_per_second": acknowledged as f64 / seconds.max(f64::MIN_POSITIVE),
        "offered_rate_shares_per_second": phase.dispatched as f64 / seconds.max(f64::MIN_POSITIVE),
        "client_ack_latency": latency,
        "server_share_ack_seconds": phase.ack_deltas,
        "order_lock": phase.lock,
        "processes": phase.processes,
        "database_delay_milliseconds_configured": phase.proxy_delay_configured_ms,
        "min_mem_available_kib": phase.min_mem_available_kib,
        "scheduled_blocks": phase.scheduled_blocks,
        "frontend_restarts": phase.frontend_restarts,
        "rejected_valid_shares": rejected_valid_count(&collected.submits, &phase.plan.name),
        "harness_bug_rejections": harness_bug_count(&collected.submits, &phase.plan.name),
        "reconciliation": reconciliation.map(|rec| json!({
            "offered": rec.offered.len(),
            "acknowledged": rec.acknowledged.len(),
            "committed": rec.committed.len(),
            "missing": rec.missing.len(),
            "missing_sample": rec.missing.iter().take(10).collect::<Vec<_>>(),
            "unexpected": unexpected_by_phase.get(&phase.plan.name).copied().unwrap_or(0),
            "acknowledged_share_ids_sha256": rec.acknowledged_digest(),
            "postgres_share_ids_sha256": rec.committed_digest(),
        })),
    })
}

/// Split the gaps between what was acknowledged and what PostgreSQL holds into
/// the two failures they really are.
///
/// An acknowledged share the database does not hold is a loss. A share the
/// database holds that the server refused with `ledger-confirmation-failed` is
/// not a loss: the append committed after the commit deadline had already
/// answered the miner. They are reported separately because only the first
/// means a miner's credited work disappeared.
/// Which of the two failures a committed-but-unacknowledged share is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GapKind {
    /// PostgreSQL holds it, and the server had already refused it with
    /// `ledger-confirmation-failed`. Nothing was lost.
    AckCommitDivergence,
    /// PostgreSQL holds it and nothing explains why no acknowledgement
    /// covers it.
    DurabilityLoss,
}

/// Decide from the submit record the harness has, if any.
pub fn classify_committed_gap(record: Option<&SubmitRecord>) -> GapKind {
    let confirmation_failure = record
        .and_then(|record| match &record.outcome {
            Outcome::Rejected(rejection) => Some(rejection),
            _ => None,
        })
        .is_some_and(classify::is_confirmation_failure);
    if confirmation_failure {
        GapKind::AckCommitDivergence
    } else {
        GapKind::DurabilityLoss
    }
}

fn classify_gaps(
    runs: &[PhaseRun],
    reconciliations: &[(String, digest::Reconciliation)],
    attribution: &digest::UnexpectedAttribution,
    submits: &[SubmitRecord],
    share_commit_timeout_seconds: f64,
) -> (Value, Vec<Value>) {
    let mut findings = Vec::new();
    let mut divergences = Vec::new();
    let by_share: HashMap<&str, &SubmitRecord> = submits
        .iter()
        .filter(|record| !record.reoffer)
        .map(|record| (record.share_id.as_str(), record))
        .collect();
    for phase in runs {
        // Only a phase that deliberately tears a socket down can legitimately
        // produce an indeterminate share; everywhere else a gap is a finding.
        if phase.plan.mid_flight_kill {
            continue;
        }
        let Some((_, reconciliation)) = reconciliations
            .iter()
            .find(|(name, _)| *name == phase.plan.name)
        else {
            continue;
        };
        if !reconciliation.missing.is_empty() {
            findings.push(json!({
                "phase": phase.plan.name,
                "kind": "acknowledged share missing from PostgreSQL",
                "count": reconciliation.missing.len(),
                "sample": reconciliation.missing.iter().take(20).collect::<Vec<_>>(),
            }));
        }
        let Some((_, rows)) = attribution
            .by_phase
            .iter()
            .find(|(name, _)| *name == phase.plan.name)
        else {
            continue;
        };
        let mut unexplained = Vec::new();
        for share in rows {
            let record = by_share.get(share.as_str()).copied();
            let rejection = record.and_then(|record| match &record.outcome {
                Outcome::Rejected(rejection) => Some(rejection),
                _ => None,
            });
            match classify_committed_gap(record) {
                GapKind::AckCommitDivergence => divergences.push(json!({
                    "share_id": share,
                    "phase": phase.plan.name,
                    "frontend": record.map(|record| record.frontend),
                    "session": record.map(|record| record.session),
                    "job_id": record.map(|record| record.job_id.clone()),
                    "code": rejection.map(|r| r.code),
                    "reason_id": rejection.and_then(|r| r.reason_id.clone()),
                    "message": rejection.map(|r| r.message.clone()),
                    "send_to_response_milliseconds": record.and_then(|r| r.latency_millis),
                    "share_commit_timeout_milliseconds": share_commit_timeout_seconds * 1000.0,
                    "response_after_commit_deadline": record
                        .and_then(|r| r.latency_millis)
                        .map(|latency| latency >= share_commit_timeout_seconds * 1000.0),
                })),
                GapKind::DurabilityLoss => unexplained.push(share.clone()),
            }
        }
        if !unexplained.is_empty() {
            findings.push(json!({
                "phase": phase.plan.name,
                "kind": "committed share that was never acknowledged",
                "count": unexplained.len(),
                "sample": unexplained.iter().take(20).collect::<Vec<_>>(),
            }));
        }
    }
    (json!(findings), divergences)
}

async fn finish_blocked(
    args: &Args,
    ctx: &RunContext,
    mut frontends: Vec<Frontend>,
    blocked: Vec<BlockedLog>,
    error: String,
) -> Result<i32> {
    let report_path = args.out.join("load-harness-report.json");
    let document = json!({
        "schema": report::SCHEMA,
        "run_id": ctx.run_id.to_string(),
        "blocked": {
            "blocked": true,
            "error": error,
            "log_matches": blocked,
            "note": "Window sizes of 200k and above are refused until #273 (the PostgreSQL JSONB \
                     container ceiling), and found-block candidates are refused at 400k until \
                     #265. A refusal is a result, never something to work around.",
        },
        "topology": {
            "frontends": args.frontends,
            "sessions": args.sessions,
            "window_shares": args.window_shares,
        },
        "frontend_environment": frontends.iter().map(|child| json!({
            "instance_id": child.spec.instance_id,
            "stderr_log": child.stderr_path.display().to_string(),
            "environment": frontend::redacted(&child.environment),
        })).collect::<Vec<_>>(),
        "host": measure::host_facts(),
    });
    report::write_json(&report_path, &document)?;
    eprintln!("run blocked: {error}");
    eprintln!("side report: {}", report_path.display());
    for child in frontends.iter_mut() {
        child.kill();
    }
    Ok(EXIT_BLOCKED)
}

fn summary_text(report: &Value, verdict: &artifact::Verdict, command: &str) -> String {
    let mut text = String::new();
    text.push_str("=== qbit-prism-load ===\n");
    for phase in report["phases"].as_array().into_iter().flatten() {
        text.push_str(&format!(
            "phase {:<16} {:>8.1}s target={:<8} offered={:<8} acked={:<8} rate={:.1}/s \
             ack p50={:?} p99={:?} lock_waiters_max={} shortfall={}\n",
            phase["name"].as_str().unwrap_or_default(),
            phase["duration_seconds"].as_f64().unwrap_or_default(),
            phase["target_rate_shares_per_second"]
                .as_f64()
                .unwrap_or_default(),
            phase["dispatched"].as_u64().unwrap_or_default(),
            phase["reconciliation"]["acknowledged"]
                .as_u64()
                .unwrap_or_default(),
            phase["achieved_rate_shares_per_second"]
                .as_f64()
                .unwrap_or_default(),
            phase["client_ack_latency"]["p50"].as_f64(),
            phase["client_ack_latency"]["p99"].as_f64(),
            phase["order_lock"]["max_waiters"]
                .as_u64()
                .unwrap_or_default(),
            phase["shortfall"].as_u64().unwrap_or_default(),
        ));
    }
    text.push_str(&format!(
        "artifact verdict: {}\n",
        if verdict.valid {
            verdict.summary.clone().unwrap_or_default()
        } else {
            format!("INVALID: {}", verdict.error_chain.join(": "))
        }
    ));
    text.push_str("validate with:\n");
    text.push_str(command);
    text.push('\n');
    text
}
