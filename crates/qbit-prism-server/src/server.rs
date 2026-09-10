use crate::{
    api::{ApiConfig, ApiState},
    config::{self, Config},
    coordinator::Coordinator,
    metrics::{self, TaskKind},
    stratum::{run_listener, StratumConfig, StratumStats},
};
use anyhow::{Context, Result};
use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::watch, task::JoinSet};

pub async fn run(config: Config) -> Result<()> {
    let rollup_settings = crate::rollups::settings_from_env()?;
    let mut stratum_config = StratumConfig::from_env()?;
    let stats = stratum_config.stats.clone();
    let coordinator = Coordinator::new(config).await?;
    let registry = coordinator.metrics.clone();
    stratum_config.metrics = registry.clone();
    let highdiff = stratum_config.highdiff_config()?;
    let config = &coordinator.config;
    let (shutdown, shutdown_rx) = watch::channel(false);
    let primary = TcpListener::bind((
        config::value("PRISM_STRATUM_BIND", "127.0.0.1"),
        config::number("PRISM_STRATUM_PORT", 3340u16)?,
    ))
    .await
    .context("bind primary Stratum listener")?;
    tracing::info!(address=%primary.local_addr()?,instance=%config.instance_id,workers=config.runtime_workers,"PRISM listening");
    let high_listener = if highdiff.is_some() {
        Some(
            TcpListener::bind((
                config::optional("PRISM_STRATUM_HIGHDIFF_BIND")
                    .unwrap_or_else(|| config::value("PRISM_STRATUM_BIND", "127.0.0.1")),
                config::number("PRISM_STRATUM_HIGHDIFF_PORT", 4334u16)?,
            ))
            .await
            .context("bind high difficulty Stratum listener")?,
        )
    } else {
        None
    };
    let mut api_config = ApiConfig::from_env();
    api_config.rpc_url = config.rpc_url.clone();
    api_config.rpc_user = config.rpc_user.clone();
    api_config.rpc_password = config.rpc_password.clone();
    api_config.instance_id = config.instance_id.clone();
    if api_config.minimum_payout_bits == 0 {
        api_config.minimum_payout_bits = config.payout_policy.min_output_sats()?;
    }
    let api_state =
        ApiState::new(coordinator.ledger.pool.clone(), api_config).with_metrics(registry);
    let metrics = api_state.metrics();
    let runtime = metrics.runtime();
    let api_listener = if config.audit_port > 0 {
        Some(
            TcpListener::bind((config.audit_bind.as_str(), config.audit_port))
                .await
                .context("bind audit HTTP listener")?,
        )
    } else {
        None
    };
    let mut tasks = JoinSet::new();
    tasks.spawn(runtime.track(
        TaskKind::StratumListener,
        run_listener(
            primary,
            stratum_config,
            coordinator.clone(),
            coordinator.refresh.subscribe(),
            shutdown_rx.clone(),
        ),
    ));
    if let (Some(listener), Some(highdiff)) = (high_listener, highdiff) {
        tasks.spawn(runtime.track(
            TaskKind::StratumListener,
            run_listener(
                listener,
                highdiff,
                coordinator.clone(),
                coordinator.refresh.subscribe(),
                shutdown_rx.clone(),
            ),
        ));
    }
    tasks.spawn(runtime.track(TaskKind::Refresh, {
        let coordinator = coordinator.clone();
        let rx = shutdown_rx.clone();
        async move {
            coordinator.refresh_loop(rx).await;
            Ok(())
        }
    }));
    tasks.spawn(runtime.track(TaskKind::Submit, {
        let coordinator = coordinator.clone();
        let rx = shutdown_rx.clone();
        async move {
            coordinator.submit_loop(rx).await;
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
    if config.ctv_broadcast {
        tasks.spawn(runtime.track(
            TaskKind::Broadcast,
            crate::broadcaster::run(coordinator.clone(), shutdown_rx.clone()),
        ));
    }
    if let Some(settings) = rollup_settings {
        tasks.spawn(runtime.track(
            TaskKind::Rollup,
            crate::rollups::run(
                coordinator.ledger.pool.clone(),
                settings,
                shutdown_rx.clone(),
            ),
        ));
    }
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
        publish_health(coordinator.clone(), api_state, stats, shutdown_rx.clone()),
    ));
    let failure = tokio::select! {
        result=signal()=>{result?;None},
        result=tasks.join_next()=>{Some(match result {Some(Ok(Err(error)))=>error,Some(Err(error))=>error.into(),_=>anyhow::anyhow!("critical PRISM task exited")})}
    };
    shutdown.send_replace(true);
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
    coordinator
        .ledger
        .heartbeat(serde_json::json!({"state":"stopped"}))
        .await?;
    coordinator.ledger.pool.close().await;
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

async fn publish_health(
    coordinator: Arc<Coordinator>,
    state: ApiState,
    stats: Arc<StratumStats>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut missing_since = None::<Instant>;
    loop {
        tokio::select! {_=shutdown.changed()=>break,_=tick.tick()=>{}}
        let _progress = state
            .metrics()
            .runtime()
            .start_operation(TaskKind::HealthPublisher, coordinator.config.health_timeout);
        let mut health = coordinator.health().await;
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
        state.publish_health(health.clone());
        let registry = state.metrics();
        registry.publish_stratum(
            &snapshot,
            health["ok"] == true,
            coordinator.config.runtime_workers,
            coordinator.blocks.load(Ordering::Relaxed),
        );
        registry.publish_delivery(
            stats.delivery_metrics(missing_since.map_or(Duration::ZERO, |at| at.elapsed())),
        );
        state.publish_metrics(registry.render())?;
        if let Err(error) = coordinator.ledger.heartbeat(health).await {
            tracing::warn!(%error,"cluster heartbeat failed");
        }
        if let Err(error) = coordinator.ledger.prune_expired_jobs().await {
            tracing::warn!(%error,"job expiry failed");
        }
    }
    Ok(())
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
