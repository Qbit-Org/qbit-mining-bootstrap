//! Actual compact refresh and A-to-B resume at the JSONB regression boundary.
use anyhow::{ensure, Context, Result};
use futures_util::{future::LocalBoxFuture, FutureExt};
use qbit_prism_server::{
    coordinator::Coordinator, ledger::Ledger, metrics::Metrics, stratum::MiningBackend,
};
use qbit_prism_test_gate as gate;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant},
};
#[path = "support/prepared_work_assertions.rs"]
#[allow(dead_code)]
mod assertions;
#[path = "support/fake_qbitd.rs"]
#[allow(dead_code)]
mod fake_qbitd;
#[path = "support/jsonb_inventory.rs"]
#[allow(dead_code)]
mod jsonb_inventory;
#[path = "support/compact_runtime_observer.rs"]
mod observer;
#[path = "support/ledger_execution_proxy.rs"]
mod proxy;
#[path = "support/wal_primary.rs"]
mod wal_primary;
#[path = "support/window_fixture.rs"]
#[allow(dead_code)]
mod window_fixture;

struct Database {
    admin: PgPool,
    direct: PgPool,
    ledger: Ledger,
    proxy: proxy::ExecutionProxy,
    schema: String,
    url: String,
    primary: wal_primary::Primary,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
            return Ok(None);
        };
        let primary = wal_primary::Primary::start(bin).await?;
        let raw = primary.url.clone();
        let admin = PgPool::connect(&raw).await?;
        let (version, fsync): (String, String) =
            sqlx::query_as("SELECT current_setting('server_version_num'),current_setting('fsync')")
                .fetch_one(&admin)
                .await?;
        ensure!(
            version.parse::<u32>()? / 10_000 == 16 && fsync == "on",
            "runtime qualification requires PostgreSQL 16 with fsync on"
        );
        let schema = format!("compact_runtime_scale_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let direct = PgPoolOptions::new()
            .max_connections(2)
            .connect(url.as_str())
            .await?;
        let upstream = url
            .socket_addrs(|| Some(5432))?
            .into_iter()
            .next()
            .context("database address missing")?;
        let proxy = proxy::ExecutionProxy::start(upstream).await?;
        let url = proxy.rewrite_url(url.as_str())?;
        let ledger = Ledger::connect(&url, "runtime-scale".into(), 4, true).await?;
        Ok(Some(Self {
            admin,
            direct,
            ledger,
            proxy,
            schema,
            url,
            primary,
        }))
    }

    async fn close(self) -> Result<()> {
        self.ledger.pool.close().await;
        self.direct.close().await;
        let proxy = self.proxy.finish().await;
        let schema = sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await;
        self.admin.close().await;
        let primary = self.primary.close().await;
        proxy?;
        schema?;
        primary
    }
}

async fn run(
    body: impl for<'a> FnOnce(&'a Database) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = AssertUnwindSafe(body(&db)).catch_unwind().await;
    let cleanup = db.close().await;
    match result {
        Ok(result) => match (result, cleanup) {
            (Ok(()), cleanup) => cleanup,
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(cleanup)) => {
                Err(error.context(format!("cleanup also failed: {cleanup:#}")))
            }
        },
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn qualify(n: u64) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("qbit_prism_server::coordinator::compact_resume=debug")
        .with_ansi(false)
        .with_test_writer()
        .try_init();
    run(|db| { async move {
        let node = fake_qbitd::FakeNode::open().await?;
        let mut frontends = Vec::new();
        for instance in ["runtime-scale-a", "runtime-scale-b"] {
            let mut config = fake_qbitd::coordinator_config(db.url.clone(), &node, instance)?;
            config.database_connections = 8;
            config.build_workers = 2;
            config.template_max_age = Duration::from_secs(3600);
            config.submit_tip_max_age = Duration::from_secs(3600);
            config.template_refresh_failure_exit = Duration::from_secs(3600);
            config.snapshot_interval = Duration::from_secs(3600);
            config.health_timeout = Duration::from_secs(3600);
            frontends.push(Coordinator::new(config, Arc::new(Metrics::default())).await?);
        }
        let result = async {
            let (a,b) = (&frontends[0], &frontends[1]);
            let plan = window_fixture::WindowPlan::new(n)?;
            let loaded = plan.load(&db.direct, "compact-runtime-scale").await?;
            // Bulk loading a fresh primary leaves no share distribution stats.
            // Prepare them before measurement so page plans do not depend on
            // whether the background auto-analyze interval has elapsed.
            sqlx::query("ANALYZE qbit_share_ledger").execute(&db.direct).await?;
            plan.verify_round_trip(&db.direct, &[1,n/2,n]).await?;
            // Observer installation, fixture writes and checkpoint are outside
            // the insert-LSN bracket. This disposable primary has no workers.
            let probe = observer::JsonbProbe::install(&db.direct, &db.schema).await?;
            sqlx::query("CHECKPOINT").execute(&db.direct).await?;
            let mark = db.proxy.mark();
            let before = observer::insert_lsn(&db.direct).await?;
            let clock = Instant::now();
            a.refresh_once().await?;
            let elapsed = clock.elapsed();
            let after = observer::insert_lsn(&db.direct).await?;
            let measured = probe.measure(&db.proxy, mark)?;
            let wal = observer::wal_bytes(&db.direct, &before, &after).await?;
            let prepared = a.prepared.read().await.clone().context("A published no work")?;
            let count = prepared.window.shares.context("nonempty reference absent")?.share_count;
            eprintln!("compact refresh observation: shares={n}, published_shares={count}, max_jsonb={}, insert_wal_bytes={wal}, refresh_seconds={:.3}", measured.max_uncompressed_bytes, elapsed.as_secs_f64());
            assertions::assert_refresh_measurements(n,count,Some(measured.max_uncompressed_bytes),Some(wal))?;
            let payload: Value = sqlx::query_scalar("SELECT payload FROM qbit_prism_jobs WHERE job_id=$1")
                .bind(&prepared.storage_key).fetch_one(&db.direct).await?;
            assertions::assert_no_materialized_shares(&payload)?;
            assertions::assert_no_materialized_shares(&serde_json::to_value(&prepared.snapshot)?)?;
            assertions::assert_no_materialized_shares(&serde_json::to_value(&prepared.bundle)?)?;
            let original = a.ledger.compact_prepared(&prepared.storage_key).await?.context("typed original missing")?;
            let hashes = original.record.audit_hashes.as_ref().context("original canonical hashes absent")?;
            ensure!(hashes.audit_bundle_sha256.len()==64 && hashes.coinbase_manifest_sha256.len()==64);
            // A normal cached poll writes no replacement dependency.
            let cached = db.proxy.mark();
            a.refresh_once().await?;
            ensure!(probe.measure(&db.proxy,cached)?.rows==0, "cached refresh wrote JSONB");
            ensure!(a.prepared.read().await.as_ref().unwrap().storage_key==prepared.storage_key);
            let worker = a.authorize("scale-worker.rig").await?;
            let issued = a.build_job(&worker,"12345678",1.0,0.0).await?;
            a.persist_issued_job(&worker,&issued,0,Duration::from_secs(600)).await?;
            let child: Value = sqlx::query_scalar("SELECT payload FROM qbit_prism_jobs WHERE job_id=$1")
                .bind(&issued.wire.job_id).fetch_one(&db.direct).await?;
            assertions::assert_no_materialized_shares(&child)?;
            let b_refresh_clock = Instant::now();
            b.refresh_once().await.context("frontend B refresh failed")?;
            eprintln!("compact scale phase: shares={n}, phase=frontend_b_refresh, seconds={:.3}", b_refresh_clock.elapsed().as_secs_f64());
            let resume_mark = db.proxy.mark();
            let resume_clock = Instant::now();
            // Every waiter has its own outer budget; common work contains no
            // worker, issued expiry, or first-waiter timeout.
            let resume = || async {
                tokio::time::timeout(Duration::from_secs(25), b.resume_job(&worker,&issued.wire.job_id))
                    .await.context("cross-frontend resume exceeded its original 25-second budget")?
                    .context("cross-frontend resume returned a backend error")?
                    .context("cross-frontend resume missed")
            };
            let (r1,r2,r3,r4) = tokio::try_join!(resume(),resume(),resume(),resume())?;
            let resume_elapsed = resume_clock.elapsed();
            let executions = db.proxy.executions_since(resume_mark)?;
            let mut row_count = 0;
            let mut page_count = 0;
            for execution in executions.iter().filter(|entry| entry.sql.contains("FROM qbit_share_ledger") && entry.sql.contains("payout_order_key")) {
                row_count += execution.returned_rows()?;
                page_count += 1;
            }
            ensure!(row_count==n, "four resumes read {row_count} rows rather than one {n}-row reconstruction");
            for resumed in [&r1,&r2,&r3,&r4] {
                ensure!(resumed.context.prepared.storage_key==issued.context.prepared.storage_key);
                ensure!(resumed.context.prepared.window==issued.context.prepared.window);
                ensure!(resumed.context.prepared.inputs==issued.context.prepared.inputs);
                ensure!(resumed.context.prepared.snapshot.payout_revision==issued.context.prepared.snapshot.payout_revision);
                ensure!(resumed.wire.coinb1==issued.wire.coinb1 && resumed.wire.coinb2==issued.wire.coinb2);
                ensure!(resumed.wire.share_target==issued.wire.share_target && resumed.wire.extranonce1==issued.wire.extranonce1);
                ensure!(resumed.wire.resume_expires_at.is_some() && !resumed.wire.clean_jobs);
                ensure!(Arc::ptr_eq(&r1.context.prepared,&resumed.context.prepared), "resumes did not share slim reconstruction");
            }
            let survivor = a.ledger.compact_prepared(&prepared.storage_key).await?.context("dependency vanished")?;
            ensure!(survivor.record==original.record && survivor.original_expires_at_ms==original.original_expires_at_ms);
            let child_after: Value = sqlx::query_scalar("SELECT payload FROM qbit_prism_jobs WHERE job_id=$1")
                .bind(&issued.wire.job_id).fetch_one(&db.direct).await?;
            ensure!(child_after==child, "resume rewrote original issued expiry or bytes");
            println!("ACTUAL_COMPACT_RUNTIME n={n} pg=16 fsync=on checkpoint=before_refresh load_seconds={:.3} refresh_seconds={:.3} max_jsonb_bytes={} wal_insert_bytes={wal} jsonb_rows={} resume_seconds={:.3} resumed_waiters=4 actual_share_rows={row_count} pages={page_count} audit_sha256={} coinbase_sha256={} rss=unavailable",
                loaded.seconds,elapsed.as_secs_f64(),measured.max_uncompressed_bytes,measured.rows,resume_elapsed.as_secs_f64(),hashes.audit_bundle_sha256,hashes.coinbase_manifest_sha256);
            Ok(())
        }.await;
        for frontend in frontends { frontend.ledger.pool.close().await; }
        result
    }.boxed_local() }).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_refresh_and_shared_cross_frontend_resume() -> Result<()> {
    qualify(5_000).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit 400k real runtime qualification on a disposable PostgreSQL16 primary"]
async fn actual_400k_refresh_and_cross_frontend_resume() -> Result<()> {
    qualify(400_000).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit 500k real runtime headroom qualification on a disposable PostgreSQL16 primary"]
async fn actual_500k_refresh_and_cross_frontend_resume() -> Result<()> {
    qualify(500_000).await
}
