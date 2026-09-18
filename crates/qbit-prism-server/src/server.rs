use crate::{
    api::{ApiConfig, ApiState},
    config::{self, Config},
    coordinator::Coordinator,
    ledger::{HeartbeatHealth, HeartbeatStatus},
    metrics::{self, TaskKind},
    stratum::{run_listener, StratumConfig, StratumStats},
};
use anyhow::{Context, Result};
use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::watch, task::JoinSet};

const BLOB_PRUNE_BUDGET: Duration = Duration::from_secs(5);

pub async fn run(config: Config) -> Result<()> {
    let rollup_settings = crate::rollups::settings_from_env()?;
    let partition_settings = crate::partitions::settings_from_env()?;
    let stratum_config = StratumConfig::from_env()?;
    let stats = stratum_config.stats.clone();
    // Validate and bind both Stratum listeners before coordinator startup can
    // write cluster state: a restart that loses the bind race to its
    // predecessor must exit without a `starting` instance row, which
    // `fatal-state clear` would refuse on. The audit listener binds later
    // because it serves the coordinator's ledger.
    let highdiff = stratum_config.highdiff_config()?;
    let mut api_config = ApiConfig::from_env()?;
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
    let registry = Arc::new(metrics::Metrics::default());
    let coordinator = Coordinator::new(config, registry.clone()).await?;
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
    let attached = crate::partitions::ensure(&coordinator.ledger.pool)
        .await
        .context("attach the share ledger partition lead at startup")?;
    if attached > 0 {
        tracing::info!(created = attached, "share ledger partitions attached");
    }
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
            registry.clone(),
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
                registry.clone(),
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
        publish_health(coordinator.clone(), api_state, stats, shutdown_rx.clone()),
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

async fn publish_health(
    coordinator: Arc<Coordinator>,
    state: ApiState,
    stats: Arc<StratumStats>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut tick = publication_ticks(&state);
    let mut missing_since = None::<Instant>;
    loop {
        tokio::select! {_=shutdown.changed()=>break,_=tick.tick()=>{}}
        let health = with_health_publication_progress(&state, async {
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
            registry.publish_delivery(stats.delivery_metrics());
            state.publish_metrics(registry.render())?;
            Ok(health)
        })
        .await?;
        let health: HeartbeatHealth = serde_json::from_value(health)?;
        if let Err(error) = coordinator
            .ledger
            .heartbeat(HeartbeatStatus::Health(health))
            .await
        {
            tracing::warn!(%error,"cluster heartbeat failed");
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
