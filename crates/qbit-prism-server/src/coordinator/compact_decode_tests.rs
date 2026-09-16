//! Actual Coordinator + PostgreSQL metadata decoder cancellation ownership.
use super::*;
use futures_util::{future::BoxFuture, FutureExt};
use qbit_prism_test_gate as gate;
use tokio_util::task::AbortOnDropHandle;

struct Release(Arc<prepared_storage::RepairProbe>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn issued(
    coordinator: &Arc<Coordinator>,
    worker: &Worker,
    ttl: Duration,
) -> Result<MiningJob<JobContext>> {
    let job = coordinator
        .build_job(worker, "00000001", 1e-12, 0.0)
        .await
        .map_err(|error| anyhow::anyhow!("build: {error:?}"))?;
    coordinator
        .persist_issued_job(worker, &job, 0, ttl)
        .await
        .map_err(|error| anyhow::anyhow!("persist: {error:?}"))?;
    Ok(job)
}

async fn cancellation_cases(coordinator: &Arc<Coordinator>, worker: &Worker) -> Result<()> {
    coordinator.refresh_once().await?;
    for expire in [false, true] {
        let original = issued(coordinator, worker, Duration::from_secs(30)).await?;
        let first_job = if expire {
            issued(coordinator, worker, Duration::from_secs(1)).await?
        } else {
            original.clone()
        };
        let probe = Release(Arc::new(prepared_storage::RepairProbe::default()));
        let block = probe.0.clone();
        *coordinator.ledger.compact_decode_hook.lock().unwrap() =
            Some(Arc::new(move || block.block()));
        let first = AbortOnDropHandle::new(tokio::spawn({
            let coordinator = coordinator.clone();
            async move {
                coordinator
                    .resume_job(&first_job.context.worker, &first_job.wire.job_id)
                    .await
            }
        }));
        tokio::time::timeout(Duration::from_secs(5), probe.0.entered.notified()).await?;
        if expire {
            assert!(tokio::time::timeout(Duration::from_secs(2), first)
                .await?
                .unwrap()
                .unwrap()
                .is_none());
        } else {
            first.abort();
            assert!(matches!(first.await, Err(error) if error.is_cancelled()));
        }
        assert_eq!(
            coordinator.build_slots.available_permits(),
            1,
            "the held production decoder precedes reconstruction build admission"
        );
        // Last-waiter cancellation must not make another decoder admissible.
        // Repeat on a still-live child sharing the same immutable dependency.
        for _ in 0..3 {
            assert!(tokio::time::timeout(
                Duration::from_millis(20),
                coordinator.resume_job(worker, &original.wire.job_id)
            )
            .await
            .is_err());
            assert_eq!(probe.0.calls.load(Ordering::SeqCst), 1);
        }
        // The final decoder/cleanup owner must notify capacity waiters even
        // though its ResumeFlight and original async caller already vanished.
        let recovery = coordinator.resume_job(worker, &original.wire.job_id);
        tokio::pin!(recovery);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut recovery)
                .await
                .is_err()
        );
        probe.0.release();
        let resumed = tokio::time::timeout(Duration::from_secs(5), recovery)
            .await?
            .map_err(|error| anyhow::anyhow!("resume: {error:?}"))?
            .context("live original job did not recover")?;
        assert_eq!(probe.0.calls.load(Ordering::SeqCst), 2);
        assert_eq!(resumed.wire.coinb1, original.wire.coinb1);
        assert_eq!(resumed.wire.coinb2, original.wire.coinb2);
        assert_eq!(resumed.wire.share_target, original.wire.share_target);
        *coordinator.ledger.compact_decode_hook.lock().unwrap() = None;
    }
    Ok(())
}

async fn snapshot_cancellation_cases(
    coordinator: &Arc<Coordinator>,
    worker: &Worker,
) -> Result<()> {
    for sequence in 1..=2 {
        coordinator
            .ledger
            .append(
                AcceptedShare {
                    share_seq: 0,
                    share_id: format!("snapshot:{sequence:064x}"),
                    miner_id: worker.username.clone(),
                    order_key: worker.username.clone(),
                    p2mr_program_hex: worker.p2mr_program_hex.clone(),
                    share_difficulty: 1,
                    network_difficulty: 100,
                    template_height: 100,
                    job_id: "snapshot-seed".into(),
                    job_issued_at_ms: 1,
                    accepted_at_ms: 0,
                    ntime: 1_800_000_000,
                    credit_policy: None,
                },
                None,
            )
            .await?;
    }
    coordinator.refresh_once().await?;
    for phase in ["balances", "shares"] {
        let original = coordinator.prepared.read().await.clone().unwrap();
        let probe = Release(Arc::new(prepared_storage::RepairProbe::default()));
        let block = probe.0.clone();
        *coordinator.ledger.snapshot_decode_hook.lock().unwrap() = Some(Arc::new(move |current| {
            if current == phase {
                block.block();
            }
        }));
        let first = AbortOnDropHandle::new(tokio::spawn({
            let coordinator = coordinator.clone();
            async move { coordinator.refresh_once().await }
        }));
        tokio::time::timeout(Duration::from_secs(5), probe.0.entered.notified()).await?;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(coordinator.build_slots.available_permits(), 0, "{phase}");
        assert!(Arc::ptr_eq(
            coordinator.prepared.read().await.as_ref().unwrap(),
            &original
        ));
        let mut next = Box::pin(coordinator.refresh_once());
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut next)
            .await
            .is_err());
        assert_eq!(probe.0.calls.load(Ordering::SeqCst), 1, "{phase}");
        probe.0.release();
        tokio::time::timeout(Duration::from_secs(5), next).await??;
        assert_eq!(probe.0.calls.load(Ordering::SeqCst), 2, "{phase}");
        assert_eq!(coordinator.build_slots.available_permits(), 1, "{phase}");
        let current = coordinator.prepared.read().await.clone().unwrap();
        assert_ne!(current.storage_key, original.storage_key);
        assert_eq!(current.window.shares.unwrap().share_count, 2);
        *coordinator.ledger.snapshot_decode_hook.lock().unwrap() = None;
    }
    Ok(())
}

type RuntimeCase = for<'a> fn(&'a Arc<Coordinator>, &'a Worker) -> BoxFuture<'a, Result<()>>;

async fn with_coordinator(raw: &str, case: RuntimeCase) -> Result<()> {
    let _serial = test_serial::TEST_LOCK.lock().await;
    let admin = sqlx::PgPool::connect(raw).await?;
    let schema = format!("compact_decode_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    // Reuse only the node server/configuration; the tested Coordinator opens
    // a real Ledger and drives refresh, issued save and public resume itself.
    let node = miner_tests::Fixture::new(Duration::from_secs(30)).await;
    let mut config = (*node.coordinator.config).clone();
    config.database_url = url.to_string();
    config.initialize_schema = true;
    config.build_workers = 1;
    config.snapshot_interval = Duration::ZERO;
    let coordinator = Coordinator::new(config, Arc::default()).await?;
    let worker = node.job(1, 0, "original.worker").context.worker.clone();
    let result = std::panic::AssertUnwindSafe(case(&coordinator, &worker))
        .catch_unwind()
        .await;
    coordinator.ledger.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
async fn cancelled_and_expired_runtime_metadata_decoders_retain_capacity() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    with_coordinator(&raw, |coordinator, worker| {
        Box::pin(cancellation_cases(coordinator, worker))
    })
    .await
}

#[tokio::test]
async fn cancelled_runtime_snapshot_decoders_retain_build_capacity() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    with_coordinator(&raw, |coordinator, worker| {
        Box::pin(snapshot_cancellation_cases(coordinator, worker))
    })
    .await
}
