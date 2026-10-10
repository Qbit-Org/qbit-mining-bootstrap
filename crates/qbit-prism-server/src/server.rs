use crate::{
    api::{ApiConfig, ApiState},
    config::{self, Config},
    coordinator::Coordinator,
    ledger::{HeartbeatHealth, HeartbeatStatus},
    listen::{bind_listener, reserve_address, ReservedAddress, HTTP_LISTEN_BACKLOG},
    metrics::{self, TaskKind},
    readiness::{
        admission::{Admission, AdmissionChange, AdmissionSignal},
        dual_writer::{DualWriterReport, WriterPath},
        endpoint,
    },
    stratum::{run_gated_listener, run_listener, StratumConfig, StratumStats},
};
use anyhow::{Context, Result};
use std::{
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, ToSocketAddrs},
    sync::watch,
    task::JoinSet,
};

const BLOB_PRUNE_BUDGET: Duration = Duration::from_secs(5);

pub async fn run(config: Config) -> Result<()> {
    config.ensure_pool_fee_settles_dust()?;
    let rollup_settings = crate::rollups::settings_from_env()?;
    let partition_settings = crate::partitions::settings_from_env()?;
    let landing_trim = crate::memory::landing_trim_from_env()?;
    let stratum_config = StratumConfig::from_env()?;
    let stats = stratum_config.stats.clone();
    // Validate and bind both Stratum listeners before coordinator startup can
    // write cluster state: a restart that loses the bind race to its
    // predecessor must exit without a `starting` instance row, which
    // `fatal-state clear` would refuse on. The audit listener binds later
    // because it serves the coordinator's ledger.
    let highdiff = stratum_config.highdiff_config()?;
    let mut api_config = ApiConfig::from_env()?;
    // 3.1: the readiness endpoint (decision D-7) and the grace that it and
    // the dual-writer Stratum gate decide admission with.
    let readiness_endpoint = endpoint::EndpointConfig::from_env()?;
    let admission_grace = crate::readiness::admission::grace_from_env()?;
    // 3.1: a dual-writer frontend binds its Stratum addresses without
    // listening; they accept connections only while it admits miners.
    let gated = config.dual_writer.is_some();
    let primary = StratumAddress::bind(
        (
            config::value("PRISM_STRATUM_BIND", "127.0.0.1"),
            config::number("PRISM_STRATUM_PORT", 3340u16)?,
        ),
        stratum_config.listen_backlog,
        gated,
    )
    .await
    .context("bind primary Stratum listener")?;
    // The kernel caps every listen backlog at the namespace's somaxconn
    // without an error, so say what the Stratum listeners actually got.
    let somaxconn = crate::listen::somaxconn();
    tracing::info!(address=%primary.local_addr()?,instance=%config.instance_id,workers=config.runtime_workers,listen_backlog=stratum_config.listen_backlog,somaxconn=?somaxconn,gated,"PRISM listening");
    if let Some(cap) = somaxconn.filter(|cap| *cap < stratum_config.listen_backlog) {
        tracing::warn!(
            requested = stratum_config.listen_backlog,
            somaxconn = cap,
            "net.core.somaxconn caps the Stratum listen backlog below PRISM_STRATUM_LISTEN_BACKLOG; raise it in the frontend's network namespace"
        );
    }
    let high_listener = if highdiff.is_some() {
        Some(
            StratumAddress::bind(
                (
                    config::optional("PRISM_STRATUM_HIGHDIFF_BIND")
                        .unwrap_or_else(|| config::value("PRISM_STRATUM_BIND", "127.0.0.1")),
                    config::number("PRISM_STRATUM_HIGHDIFF_PORT", 4334u16)?,
                ),
                stratum_config.listen_backlog,
                gated,
            )
            .await
            .context("bind high difficulty Stratum listener")?,
        )
    } else {
        None
    };
    let readiness_listener = match &readiness_endpoint {
        Some(endpoint) => Some(
            bind_listener((endpoint.bind.as_str(), endpoint.port), HTTP_LISTEN_BACKLOG)
                .await
                .context("bind readiness endpoint listener")?,
        ),
        None => None,
    };
    let registry = Arc::new(metrics::Metrics::default());
    // #291: 0 from the start with the switch off. With it on, unknown until
    // the first health publication has read the cluster's hold (#664).
    registry.publish_block_submission(config.block_submit_enabled, None);
    // #581: the accepted-share counter is rendered at scrape time, beside the
    // event-driven rejection counter, so the share-refusal rules see an
    // outage that stalls the health publisher.
    registry.read_accepted_shares_at_scrape({
        let stats = stats.clone();
        move || stats.accepted_submissions()
    });
    let coordinator = Coordinator::new(config, registry.clone()).await?;
    // 3.1 dual writer. A frontend never personalises its database
    // (`node-identity set` does, CONTRACT D-9): it checks it. A database
    // personalised as this node whose defaults or key sequences have been
    // changed since would write rows under the wrong identity, so it is
    // refused. One not personalised, or personalised as the other node, is
    // reported: the frontend runs, but the peer sync refuses to start and the
    // frontend never becomes ready. A single writer refuses a database that
    // has run as a dual-writer node, unless the rollback says so (D-12).
    match &coordinator.config.dual_writer {
        Some(dual) => {
            let node = dual.identity.node;
            match coordinator.ledger.check_node_identity(node).await? {
                crate::ledger::IdentityCheck::Ready(record) => tracing::info!(
                    node = %record.node,
                    carry_owner = dual.identity.carry_owner,
                    recorded_at = %record.recorded_at,
                    "dual-writer node identity"
                ),
                crate::ledger::IdentityCheck::Drifted(record, drift) => anyhow::bail!(
                    "this database is dual-writer node {}, but {} changed since it was \
                     personalised; run `qbit-prism-server node-identity set --index {}` again \
                     before starting",
                    record.node,
                    drift.join(", "),
                    node.index()
                ),
                crate::ledger::IdentityCheck::Unidentified => tracing::error!(
                    node = %node,
                    "ALERT: this database has no dual-writer node identity: run \
                     `qbit-prism-server node-identity set --index {}` on it; until then the peer \
                     sync does not run and this frontend admits no miners",
                    node.index()
                ),
                crate::ledger::IdentityCheck::OtherNode(record) => tracing::error!(
                    node = %node,
                    database_node = %record.node,
                    "ALERT: PRISM_DATABASE_URL names dual-writer node {}'s database, not this \
                     node's; the peer sync does not run and this frontend admits no miners. If \
                     it is a physical copy of that database rebuilt for this node, stop this \
                     frontend and run `qbit-prism-server node-identity repersonalise --index {}`",
                    record.node,
                    node.index()
                ),
            }
        }
        None => {
            coordinator
                .ledger
                .refuse_single_writer_on_dual_ledger(config::dual_writer_downgrade()?)
                .await?;
        }
    }
    coordinator.landing_trim.set_enabled(landing_trim);
    tracing::info!(
        enabled = landing_trim,
        supported = crate::memory::SUPPORTED,
        "post-landing malloc_trim (#600)"
    );
    // The share ledger has no DEFAULT partition, so an append whose sequence
    // value has run past the last attached bound is refused (#144). Attaching
    // the lead is a precondition of serving, not a background convenience: an
    // instance that cannot maintain its partitions must refuse to start
    // rather than accept shares until the lead runs out.
    let attached =
        crate::partitions::ensure_with_metrics(&coordinator.ledger.pool, Some(&registry))
            .await
            .context("attach the share ledger partition lead at startup")?;
    if attached > 0 {
        tracing::info!(created = attached, "share ledger partitions attached");
    }
    // The share ledger has no DEFAULT partition, so an append whose sequence
    // value has run past the last attached bound is refused (#144). Attaching
    // the lead is a precondition of serving, not a background convenience: an
    // instance that cannot maintain its partitions must refuse to start
    // rather than accept shares until the lead runs out.
    let config = &coordinator.config;
    let (shutdown, shutdown_rx) = watch::channel(false);
    api_config.rpc_url = config.rpc_url.clone();
    api_config.rpc_user = config.rpc_user.clone();
    api_config.rpc_password = config.rpc_password.clone();
    api_config.instance_id = config.instance_id.clone();
    if api_config.minimum_payout_bits == 0 {
        api_config.minimum_payout_bits = config.payout_policy.min_output_sats()?;
    }
    let api_state = ApiState::new(
        coordinator.ledger.pool.clone(),
        api_config,
        registry.clone(),
    );
    let metrics = api_state.metrics();
    let runtime = metrics.runtime();
    // 3.1: admission is decided only where something reads it, so a single
    // writer without the readiness endpoint runs as 3.0 did.
    let admission_exposed = gated || readiness_endpoint.is_some();
    if admission_exposed {
        registry.enable_admission();
    }
    let stale_after = api_state.config.health_stale_after();
    let (admission_decisions, admission) = watch::channel(AdmissionSignal::UNDECIDED);
    let api_listener = if config.audit_port > 0 {
        Some(
            bind_listener(
                (config.audit_bind.as_str(), config.audit_port),
                HTTP_LISTEN_BACKLOG,
            )
            .await
            .context("bind audit HTTP listener")?,
        )
    } else {
        None
    };
    let mut tasks = JoinSet::new();
    // 3.1 dual writer: the peer sync, and the own-log latch (D-8) that the
    // tasks writing this node's own rows wait for: the refresh (prepared
    // work, reconciliation) and the submit loop (landings). A node restored
    // from an old backup pulls its own rows back from the peer first, so none
    // of them reuses a key the peer already holds. The Stratum listeners
    // (share appends) wait through admission, which needs the latch too. A
    // single writer waits for nothing.
    let own_log = match &config.dual_writer {
        Some(dual) => {
            let (sync, status) = crate::peer_sync::PeerSync::new(
                (*coordinator.ledger).clone(),
                dual,
                Some(registry.clone()),
            );
            let _ = coordinator.peer_sync.set(status.clone());
            tasks.spawn(sync.run(shutdown_rx.clone()));
            Some(status)
        }
        None => None,
    };
    tasks.spawn(runtime.track(
        TaskKind::StratumListener,
        primary.serve(
            stratum_config,
            coordinator.clone(),
            coordinator.refresh.subscribe(),
            shutdown_rx.clone(),
            registry.clone(),
            admission.clone(),
            stale_after,
        ),
    ));
    if let (Some(listener), Some(highdiff)) = (high_listener, highdiff) {
        tasks.spawn(runtime.track(
            TaskKind::StratumListener,
            listener.serve(
                highdiff,
                coordinator.clone(),
                coordinator.refresh.subscribe(),
                shutdown_rx.clone(),
                registry.clone(),
                admission.clone(),
                stale_after,
            ),
        ));
    }
    if let (Some(listener), Some(config)) = (readiness_listener, &readiness_endpoint) {
        tracing::info!(address=%listener.local_addr()?, "PRISM readiness endpoint listening");
        tasks.spawn(endpoint::serve(
            listener,
            endpoint::Endpoint::new(config, admission.clone(), stale_after, registry.clone()),
            shutdown_rx.clone(),
        ));
    }
    tasks.spawn(runtime.track(TaskKind::Refresh, {
        let coordinator = coordinator.clone();
        let rx = shutdown_rx.clone();
        let caught_up = wait_for_own_log(own_log.clone(), shutdown_rx.clone());
        async move {
            if caught_up.await {
                coordinator.refresh_loop(rx).await;
            }
            Ok(())
        }
    }));
    // With PRISM_BLOCK_SUBMIT_ENABLED off (#291) this loop claims nothing and
    // waits for the shutdown: found blocks stay pending, never offered.
    tasks.spawn(runtime.track(TaskKind::Submit, {
        let coordinator = coordinator.clone();
        let rx = shutdown_rx.clone();
        let caught_up = wait_for_own_log(own_log.clone(), shutdown_rx.clone());
        async move {
            if caught_up.await {
                coordinator.submit_loop(rx).await;
            }
            Ok(())
        }
    }));
    if config.blockwait {
        tasks.spawn(runtime.track(TaskKind::BlockWait, {
            let coordinator = coordinator.clone();
            let rx = shutdown_rx.clone();
            async move {
                coordinator.blockwait_loop(rx).await;
                Ok(())
            }
        }));
    }
    // PRISM 3.1 dual writer: the carry owner guard, and adoption of pool
    // blocks whose finder died before their rows arrived (S8).
    if config.dual_writer.is_some() {
        let settings = crate::carry_owner::CarryOwnerSettings::from_config(config)?;
        tasks.spawn(crate::carry_owner::run(
            coordinator.ledger.clone(),
            settings,
            own_log.clone(),
            Some(registry.clone()),
            shutdown_rx.clone(),
        ));
        tasks.spawn({
            let coordinator = coordinator.clone();
            let rx = shutdown_rx.clone();
            async move {
                coordinator.adoption_loop(rx).await;
                Ok(())
            }
        });
    }
    match config.block_submission().ctv_broadcaster {
        config::CtvBroadcaster::On => {
            tasks.spawn(runtime.track(
                TaskKind::Broadcast,
                crate::broadcaster::run(coordinator.clone(), shutdown_rx.clone()),
            ));
        }
        config::CtvBroadcaster::Held => tracing::warn!("{}", config::CTV_BROADCASTER_HELD),
        config::CtvBroadcaster::Off => {}
    }
    if let Some(settings) = rollup_settings {
        let settings = if config.dual_writer.is_some() {
            settings.for_dual_writer()
        } else {
            settings
        };
        tasks.spawn(runtime.track(
            TaskKind::Rollup,
            crate::rollups::run_with_metrics(
                coordinator.ledger.pool.clone(),
                settings,
                shutdown_rx.clone(),
                Some(registry.clone()),
            ),
        ));
    }
    tasks.spawn(runtime.track(
        TaskKind::SharePartitions,
        crate::partitions::run_with_metrics(
            coordinator.ledger.pool.clone(),
            partition_settings,
            shutdown_rx.clone(),
            Some(registry.clone()),
        ),
    ));
    if let Some(listener) = api_listener {
        let mut rx = shutdown_rx.clone();
        let router = crate::api::router(api_state.clone());
        tasks.spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = rx.changed().await;
                })
                .await?;
            Ok(())
        });
    }
    tasks.spawn(runtime.clone().run(shutdown_rx.clone()));
    tasks.spawn(runtime.track(
        TaskKind::Collector,
        metrics::collectors::run(
            metrics,
            coordinator.ledger.pool.clone(),
            shutdown_rx.clone(),
        ),
    ));
    tasks.spawn(runtime.track(
        TaskKind::HealthPublisher,
        publish_health(
            coordinator.clone(),
            api_state,
            stats,
            shutdown_rx.clone(),
            AdmissionPublisher {
                admission: Admission::new(admission_grace),
                decisions: admission_decisions,
                exposed: admission_exposed,
            },
        ),
    ));
    tasks.spawn({
        let ledger = coordinator.ledger.clone();
        let shutdown = shutdown_rx.clone();
        async move {
            let cursor = tokio::sync::Mutex::new(crate::ledger::BlobPruneCursor::default());
            prune_jobs(
                || async {
                    // Expiry keeps its original statement budget and commits
                    // without holding the locks needed by shares and writers.
                    let jobs = ledger.prune_expired_jobs().await.context("job expiry")?;
                    if jobs > 0 {
                        tracing::info!(jobs, "expired PRISM jobs pruned");
                    }
                    let deadline = tokio::time::Instant::now() + BLOB_PRUNE_BUDGET;
                    let mut cursor = cursor.lock().await;
                    let blobs = ledger
                        .prune_unreferenced_blobs(&mut cursor, deadline)
                        .await?;
                    if blobs.templates > 0 || blobs.balances > 0 {
                        tracing::info!(
                            templates = blobs.templates,
                            balances = blobs.balances,
                            "unreferenced PRISM blobs pruned"
                        );
                    }
                    Ok(())
                },
                shutdown,
            )
            .await
        }
    });
    let failure = tokio::select! {
        result=signal()=>{result?;None},
        result=tasks.join_next()=>{Some(match result {Some(Ok(Err(error)))=>error,Some(Err(error))=>error.into(),_=>anyhow::anyhow!("critical PRISM task exited")})}
    };
    shutdown.send_replace(true);
    // A database restored under the running frontend (D-8): no task may
    // commit another own row, so nothing drains; every task is cancelled at
    // once, and its open transaction rolls back. The restart recovers first.
    if failure.as_ref().is_some_and(|failure| {
        failure
            .downcast_ref::<crate::peer_sync::OwnLogLostWhileRunning>()
            .is_some()
    }) {
        tasks.abort_all();
    }
    // Connections drain before pooled DB handles close; queued block intents
    // remain durable and can be claimed immediately after their lease expires.
    if tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(result) = tasks.join_next().await {
            if let Ok(Err(error)) = result {
                tracing::warn!(%error,"shutdown task failed");
            }
        }
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    // SessionOwner::stop() verifies that no active or pending guard remains.
    // Publish stopped first; only then is it safe to release this token's
    // reservations. On failure, retain reservations and close without a
    // stopped marker so a replacement cannot reclaim live IDs.
    if let Err(error) = coordinator.ledger.heartbeat(HeartbeatStatus::Stopped).await {
        coordinator.close_health_pool().await;
        coordinator.ledger.pool.close().await;
        if let Some(failure) = failure {
            return Err(anyhow::anyhow!(
                "shutdown marker failed: {error}; original failure: {failure}"
            ));
        }
        return Err(error);
    }
    let cleanup_error = coordinator
        .ledger
        .release_session_owner_reservations()
        .await
        .err();
    coordinator.close_health_pool().await;
    coordinator.ledger.pool.close().await;
    if let Some(error) = cleanup_error {
        return Err(anyhow::anyhow!(
            "session reservation cleanup failed after stopped marker: {error}"
        ));
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

/// 3.1 dual writer (D-8): true once the own log is caught up, false if the
/// shutdown comes first. A single writer (`None`) never waits.
async fn wait_for_own_log(
    status: Option<watch::Receiver<crate::peer_sync::PeerSyncStatus>>,
    mut shutdown: watch::Receiver<bool>,
) -> bool {
    let Some(mut status) = status else {
        return true;
    };
    tokio::select! {
        biased;
        _ = shutdown.wait_for(|stop| *stop) => false,
        caught_up = status.wait_for(|status| status.own_log_caught_up) => caught_up.is_ok(),
    }
}

// One operation ends at publication; subsequent database maintenance is not
// a missing publication. Use the same effective budget as the scrape contract.
async fn with_health_publication_progress<T>(
    state: &ApiState,
    publication: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let _progress = state
        .metrics()
        .runtime()
        .start_operation(TaskKind::HealthPublisher, state.config.health_stale_after());
    publication.await
}

/// Publish at the configured cadence that health and metrics readers age against.
fn publication_ticks(state: &ApiState) -> tokio::time::Interval {
    let mut tick = tokio::time::interval(state.config.health_refresh_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick
}

/// Keep bounded job cleanup at the original two-second cadence, independent
/// of health publication and heartbeat latency. Never overlap prune batches.
async fn prune_jobs<T, F: std::future::Future<Output = Result<T>>>(
    mut prune: impl FnMut() -> F,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            _ = tick.tick() => {}
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            result = prune() => {
                if let Err(error) = result {
                    tracing::warn!(error=%format!("{error:#}"), "job/blob cleanup failed");
                }
            }
        }
    }
    Ok(())
}

/// A Stratum address as the frontend serves it: listening from startup as
/// in 3.0, or, in dual-writer mode (3.1), bound and listening only while the
/// frontend admits miners.
enum StratumAddress {
    Listening(TcpListener),
    Gated(ReservedAddress),
}

impl StratumAddress {
    async fn bind(addr: impl ToSocketAddrs, backlog: u32, gated: bool) -> std::io::Result<Self> {
        Ok(if gated {
            Self::Gated(reserve_address(addr).await?)
        } else {
            Self::Listening(bind_listener(addr, backlog).await?)
        })
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        match self {
            Self::Listening(listener) => listener.local_addr(),
            Self::Gated(address) => Ok(address.local_addr()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn serve(
        self,
        config: StratumConfig,
        coordinator: Arc<Coordinator>,
        refresh: watch::Receiver<u64>,
        shutdown: watch::Receiver<bool>,
        registry: Arc<metrics::Metrics>,
        admission: watch::Receiver<AdmissionSignal>,
        stale_after: Duration,
    ) -> Result<()> {
        match self {
            Self::Listening(listener) => {
                run_listener(listener, config, coordinator, refresh, shutdown, registry).await
            }
            Self::Gated(address) => {
                run_gated_listener(
                    address,
                    config,
                    coordinator,
                    refresh,
                    shutdown,
                    registry,
                    admission,
                    stale_after,
                )
                .await
            }
        }
    }
}

/// 3.1: folds each health publication into the admission decision that the
/// dual-writer Stratum gate and the readiness endpoint read, and reports it
/// in the health payload and the metrics.
struct AdmissionPublisher {
    admission: Admission,
    decisions: watch::Sender<AdmissionSignal>,
    /// Dual-writer mode or the readiness endpoint; otherwise nothing reads
    /// admission and nothing is decided or reported.
    exposed: bool,
}

impl AdmissionPublisher {
    fn publish(
        &mut self,
        health: &mut serde_json::Value,
        dual: Option<&DualWriterReport>,
        runtime_stalled: bool,
        registry: &metrics::Metrics,
    ) {
        if !self.exposed {
            return;
        }
        let now = tokio::time::Instant::now();
        // The readiness this publication reports, as /healthz will serve it
        // short of staleness, which the readers age themselves.
        let ready = health["ready"] == true && !runtime_stalled;
        let change =
            self.admission
                .observe(now.into_std(), ready, dual.and_then(|dual| dual.withdrawal));
        let state = self.admission.state();
        self.decisions.send_replace(AdmissionSignal::of(state, now));
        match change {
            Some(AdmissionChange::Admitted) => {
                tracing::info!("PRISM admits miners: readiness endpoint ready, dual-writer Stratum listeners accepting")
            }
            Some(AdmissionChange::Withdrawn(reason)) => {
                registry.record_admission_withdrawal(reason.label());
                tracing::warn!(
                    reason = reason.as_str(),
                    "PRISM withdrew: readiness endpoint not ready, dual-writer Stratum listeners refusing new connections"
                );
            }
            None => {}
        }
        registry.publish_admission(state.label());
        if let Some(dual) = dual {
            registry.publish_dual_writer(
                dual.identity.node.index(),
                dual.identity.carry_owner,
                dual.writer_path.map(WriterPath::label),
            );
        }
        health["admission"] = serde_json::json!({
            "admitting": state.admits(),
            "state": state.as_str(),
            "reason": state.reason().map(|reason| reason.as_str()),
            "grace_seconds": self.admission.grace().as_secs(),
        });
    }
}

/// A dual-writer frontend's cluster heartbeat in flight (3.1): its own
/// task, so a slow or hung database never holds the next publication, which
/// is what withdraws the node. It is never cancelled while the publisher
/// runs, so a slow heartbeat still lands; the publisher awaits it before it
/// returns, so it lands before the stopped marker, and aborts it only if the
/// publisher is aborted itself.
struct HeartbeatTask(tokio::task::JoinHandle<()>);

impl Drop for HeartbeatTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn publish_health(
    coordinator: Arc<Coordinator>,
    state: ApiState,
    stats: Arc<StratumStats>,
    mut shutdown: watch::Receiver<bool>,
    mut admission: AdmissionPublisher,
) -> Result<()> {
    let mut tick = publication_ticks(&state);
    let mut missing_since = None::<Instant>;
    let dual_writer = coordinator.config.dual_writer.is_some();
    let mut heartbeat = None::<HeartbeatTask>;
    let published = async {
        loop {
            tokio::select! {_=shutdown.changed()=>break,_=tick.tick()=>{}}
            let health = with_health_publication_progress(&state, async {
                let (mut health, dual) = coordinator.health_report().await;
                let snapshot = stats.snapshot(health["template_generation"].as_u64().unwrap_or(0));
                if snapshot.authorized_missing_current_work == 0 {
                    missing_since = None;
                } else {
                    missing_since.get_or_insert_with(Instant::now);
                }
                let delivery_stalled = missing_since
                    .is_some_and(|at| at.elapsed() > coordinator.config.health_timeout)
                    && snapshot
                        .last_delivery_progress_age_seconds
                        .is_none_or(|age| age > coordinator.config.health_timeout.as_secs_f64());
                if delivery_stalled {
                    health["ok"] = false.into();
                    health["ready"] = false.into();
                    health["status"] = "job-delivery-stalled".into();
                }
                health["stratum"] = serde_json::to_value(&snapshot)?;
                metrics::add_known_health_fields(&mut health);
                let registry = state.metrics();
                admission.publish(
                    &mut health,
                    dual.as_ref(),
                    registry.runtime().snapshot().stalled(),
                    &registry,
                );
                state.publish_health(health.clone());
                registry.publish_stratum(
                    &snapshot,
                    health["ok"] == true,
                    coordinator.config.runtime_workers,
                    coordinator.blocks.load(Ordering::Relaxed),
                );
                registry.publish_delivery(stats.delivery_metrics());
                // #664: the switch, and the cluster hold as this publication read it.
                registry.publish_block_submission(
                    coordinator.config.block_submit_enabled,
                    health["block_submission_hold"]["held"].as_bool(),
                );
                registry.publish_work_refresh_stalled(coordinator.work_refresh_age());
                registry.publish_tip_poll_age(coordinator.tip_poll_age().await);
                state.publish_metrics(registry.render())?;
                Ok(health)
            })
            .await?;
            let health: HeartbeatHealth = serde_json::from_value(health)?;
            if !dual_writer {
                // A single writer's heartbeat, as in 3.0: in line.
                if let Err(error) = coordinator
                    .ledger
                    .heartbeat(HeartbeatStatus::Health(health))
                    .await
                {
                    tracing::warn!(%error,"cluster heartbeat failed");
                }
            } else if heartbeat.as_ref().is_some_and(|task| !task.0.is_finished()) {
                tracing::debug!("cluster heartbeat skipped: the previous one is still running");
            } else {
                let ledger = coordinator.ledger.clone();
                heartbeat = Some(HeartbeatTask(tokio::spawn(async move {
                    if let Err(error) = ledger.heartbeat(HeartbeatStatus::Health(health)).await {
                        tracing::warn!(%error,"cluster heartbeat failed");
                    }
                })));
            }
        }
        Ok(())
    }
    .await;
    if let Some(mut task) = heartbeat.take() {
        let _ = (&mut task.0).await;
    }
    published
}

pub(crate) async fn signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {result=tokio::signal::ctrl_c()=>result?,_=terminate.recv()=>{}}
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use serde_json::json;
    use tower::ServiceExt;

    fn state() -> ApiState {
        state_with(ApiConfig::default())
    }

    fn state_with(config: ApiConfig) -> ApiState {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://invalid@127.0.0.1:1/invalid")
            .unwrap();
        ApiState::new(pool, config, Arc::new(metrics::Metrics::default()))
    }

    async fn assert_healthy(state: ApiState) {
        let response = crate::api::router(state)
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn publication_guard_finishes_before_slow_maintenance() {
        for phase in ["heartbeat", "prune"] {
            let state = state();
            let runtime = state.metrics().runtime();
            let task_state = state.clone();
            let (entered, maintenance_started) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                with_health_publication_progress(&task_state, async {
                    task_state.publish_health(json!({"ok":true,"ready":true}));
                    task_state.publish_metrics("qbit_prism_health_state 1\n".into())?;
                    Ok(())
                })
                .await?;
                entered.send(()).unwrap();
                released.await?; // Simulate the later asynchronous database call.
                Ok::<_, anyhow::Error>(())
            });
            maintenance_started.await.unwrap();
            assert!(
                !runtime
                    .snapshot_at(Instant::now() + Duration::from_secs(3600))
                    .stalled(),
                "{phase} must not retain the publication progress guard"
            );
            assert_healthy(state).await;
            release.send(()).unwrap();
            task.await.unwrap().unwrap();
        }
    }

    async fn health_age(state: &ApiState) -> f64 {
        let response = crate::api::router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["snapshot_age_seconds"]
            .as_f64()
            .unwrap()
    }

    #[tokio::test]
    async fn publisher_ticks_reset_health_age_at_the_configured_cadence() {
        const CADENCE: Duration = Duration::from_secs(3);
        let state = state_with(ApiConfig {
            health_refresh_interval: CADENCE,
            ..ApiConfig::default()
        });
        let (stop, mut shutdown) = watch::channel(false);
        let (published, mut publications) = tokio::sync::mpsc::unbounded_channel();
        let publisher = tokio::spawn({
            let state = state.clone();
            async move {
                // The runtime publisher's loop, without its coordinator inputs.
                let mut tick = publication_ticks(&state);
                loop {
                    tokio::select! {_=shutdown.changed()=>break,_=tick.tick()=>{}}
                    with_health_publication_progress(&state, async {
                        state.publish_health(json!({"ok":true,"ready":true}));
                        Ok(())
                    })
                    .await?;
                    published.send(Instant::now()).unwrap();
                }
                Ok::<_, anyhow::Error>(())
            }
        });
        let bound = Duration::from_secs(10);
        let first = tokio::time::timeout(bound, publications.recv())
            .await
            .unwrap()
            .unwrap();
        let fresh = health_age(&state).await;
        assert!(fresh < 1.0, "first publication age {fresh}");
        // Beyond the default 2-second cadence, no publication has reset the age.
        tokio::time::sleep_until((first + Duration::from_millis(2200)).into()).await;
        assert!(
            publications.try_recv().is_err(),
            "published before the configured cadence"
        );
        let aged = health_age(&state).await;
        assert!(aged >= 2.1 && aged > fresh, "age did not rise: {aged}");
        let second = tokio::time::timeout(bound, publications.recv())
            .await
            .unwrap()
            .unwrap();
        let interval = second - first;
        assert!(
            interval >= CADENCE - Duration::from_millis(100)
                && interval < CADENCE + Duration::from_secs(1),
            "publication interval {interval:?}"
        );
        let reset = health_age(&state).await;
        assert!(reset < 1.0 && reset < aged, "age did not reset: {reset}");
        stop.send_replace(true);
        publisher.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn publication_progress_uses_configured_freshness_budget() {
        let state = state_with(ApiConfig {
            health_refresh_interval: Duration::from_secs(6),
            ..ApiConfig::default()
        });
        assert_eq!(
            publication_ticks(&state).period(),
            Duration::from_secs(6),
            "the publisher must run at the cadence readers age against"
        );
        assert_eq!(
            publication_ticks(&self::state()).period(),
            Duration::from_secs(2)
        );
        let budget = state.config.health_stale_after();
        assert_eq!(budget, Duration::from_secs(18));
        // A snapshot older than the default 15 seconds is still fresh at this cadence.
        state.publish_health(json!({"ok":true,"ready":true}));
        *state.health_published_at_for_test().write().unwrap() =
            Instant::now() - Duration::from_secs(16);
        assert_healthy(state.clone()).await;
        let runtime = state.metrics().runtime();
        let task_state = state.clone();
        let (entered, publication_started) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let before_registration = Instant::now();
        let task = tokio::spawn(async move {
            with_health_publication_progress(&task_state, async {
                entered.send(Instant::now()).unwrap();
                released.await?; // The required publication is blocked.
                task_state.publish_health(json!({"ok":true,"ready":true}));
                task_state.publish_metrics("qbit_prism_health_state 1\n".into())?;
                Ok(())
            })
            .await
        });
        let after_registration = publication_started.await.unwrap();
        assert!(!runtime.snapshot_at(before_registration + budget).stalled());
        let expired = runtime.snapshot_at(after_registration + budget);
        assert!(
            expired.stalled(),
            "the registered budget must match the real freshness reader"
        );
        let mut health = json!({"ok":true,"ready":true});
        expired.apply_health(&mut health);
        assert_eq!(health["status"], "runtime-stalled");
        assert_eq!(health["ok"], false);
        release.send(()).unwrap();
        task.await.unwrap().unwrap();
        assert!(!runtime.snapshot_at(after_registration + budget).stalled());
        assert_healthy(state).await;
    }

    #[tokio::test(start_paused = true)]
    async fn job_pruning_retries_independently_of_a_daily_health_cadence() {
        use futures_util::FutureExt;

        let state = state_with(ApiConfig {
            health_refresh_interval: Duration::from_secs(86400),
            ..ApiConfig::default()
        });
        let mut health = publication_ticks(&state);
        health.tick().await;
        let (stop, shutdown) = watch::channel(false);
        let (attempted, mut attempts) = tokio::sync::mpsc::unbounded_channel();
        let mut count = 0;
        let pruner = tokio::spawn(prune_jobs(
            move || {
                count += 1;
                attempted.send(tokio::time::Instant::now()).unwrap();
                std::future::ready(if count == 1 {
                    Err(anyhow::anyhow!("temporary cleanup failure"))
                } else {
                    Ok(4096)
                })
            },
            shutdown,
        ));
        let first = attempts.recv().await.unwrap();
        for seconds in [2, 4, 6] {
            let next = attempts.recv().await.unwrap();
            assert_eq!(next - first, Duration::from_secs(seconds));
            assert!(health.tick().now_or_never().is_none());
        }
        stop.send_replace(true);
        pruner.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn job_pruning_serializes_slow_batches_and_skips_missed_ticks() {
        let (stop, shutdown) = watch::channel(false);
        let (attempted, mut attempts) = tokio::sync::mpsc::unbounded_channel();
        let mut first_batch = true;
        let pruner = tokio::spawn(prune_jobs(
            move || {
                attempted.send(tokio::time::Instant::now()).unwrap();
                let slow = std::mem::take(&mut first_batch);
                async move {
                    if slow {
                        tokio::time::sleep(Duration::from_secs(9)).await;
                    }
                    Ok(4096)
                }
            },
            shutdown,
        ));
        let first = attempts.recv().await.unwrap();
        // The overdue tick may run once after the slow batch, but its missed
        // successors must not create a burst of database work.
        for seconds in [9, 10, 12] {
            assert_eq!(
                attempts.recv().await.unwrap() - first,
                Duration::from_secs(seconds)
            );
        }
        stop.send_replace(true);
        pruner.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn job_pruning_shutdown_cancels_an_in_flight_batch() {
        let (stop, shutdown) = watch::channel(false);
        let (entered, started) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered);
        let pruner = tokio::spawn(prune_jobs(
            move || {
                entered.take().unwrap().send(()).unwrap();
                std::future::pending::<Result<()>>()
            },
            shutdown,
        ));
        started.await.unwrap();
        stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), pruner)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn job_pruning_does_not_start_after_shutdown() {
        let (_stop, shutdown) = watch::channel(true);
        prune_jobs(
            || -> std::future::Ready<Result<u64>> {
                panic!("cleanup must not start after shutdown")
            },
            shutdown,
        )
        .await
        .unwrap();
    }
}
