//! Small real-runtime fixtures. Only accepted shares are seeded; prepared and
//! issued records must be produced by Coordinator's public runtime methods.
use anyhow::{ensure, Context, Result};
use futures_util::future::LocalBoxFuture;
use qbit_prism_server::{
    coordinator::{Coordinator, JobContext},
    metrics::Metrics,
    stratum::{MiningBackend, MiningJob, Worker},
};
use qbit_prism_test_gate as gate;
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::time::timeout;

pub mod assertions;
#[path = "../ledger_execution_proxy.rs"]
pub mod execution;
#[allow(dead_code)]
#[path = "../fake_qbitd.rs"]
mod fake_qbitd;
pub mod socket;
#[allow(dead_code)]
#[path = "../window_fixture.rs"]
mod window_fixture;

pub const SHARES: u64 = 16;
pub const MASK: u32 = 0x0000_e000;
pub const DIFFICULTY: f64 = 1e-12;
pub const SETTLEMENT_LOCK: i64 = 0x5052_4953_4d00_0003;
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub struct Fixture {
    pub a: Arc<Coordinator>,
    pub b: Arc<Coordinator>,
    pub proxy: execution::ExecutionProxy,
    pub node: fake_qbitd::FakeNode,
    admin: PgPool,
    schema: String,
}

impl Fixture {
    async fn open(raw: &str) -> Result<Self> {
        let admin = PgPool::connect(raw).await?;
        let settings = sqlx::query("SELECT current_setting('server_version_num')::int AS version, current_setting('fsync') AS fsync, current_setting('full_page_writes') AS full_page_writes, current_setting('synchronous_commit') AS synchronous_commit")
            .fetch_one(&admin).await?;
        ensure!(
            (160000..170000).contains(&settings.try_get::<i32, _>("version")?),
            "compact runtime evidence requires PostgreSQL 16"
        );
        for setting in ["fsync", "full_page_writes", "synchronous_commit"] {
            ensure!(
                settings.try_get::<String, _>(setting)? == "on",
                "{setting} must be on"
            );
        }
        let schema = format!("prism_compact_runtime_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut frontends = Vec::new();
        let opened = async {
            let mut url = url::Url::parse(raw)?;
            url.query_pairs_mut()
                .append_pair("options", &format!("-csearch_path={schema}"));
            let upstream: SocketAddr = tokio::net::lookup_host((
                url.host_str().context("database host")?,
                url.port().unwrap_or(5432),
            ))
            .await?
            .next()
            .context("database address")?;
            let proxy = execution::ExecutionProxy::start(upstream).await?;
            let database_url = proxy.rewrite_url(url.as_str())?;
            let node = fake_qbitd::FakeNode::open().await?;
            for instance in ["runtime-a", "runtime-b"] {
                frontends.push(
                    Coordinator::new(
                        fake_qbitd::coordinator_config(database_url.clone(), &node, instance)?,
                        Arc::new(Metrics::default()),
                    )
                    .await?,
                );
            }
            // Row triggers count committed row writes, including rewrites of
            // unchanged payloads. The proxy separately observes unknown commits.
            sqlx::raw_sql(
                r#"
                CREATE TABLE runtime_job_writes (
                    ordinal bigint GENERATED ALWAYS AS IDENTITY,
                    job_id text NOT NULL, operation text NOT NULL,
                    uncompressed_jsonb_bytes integer NOT NULL
                );
                CREATE FUNCTION runtime_observe_job() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    INSERT INTO runtime_job_writes(job_id, operation, uncompressed_jsonb_bytes)
                    VALUES (NEW.job_id, TG_OP, pg_column_size(NEW.payload::text::jsonb));
                    RAISE NOTICE 'prism-execution-marker qbit_prism_jobs %', TG_OP;
                    RETURN NEW;
                END $$;
                CREATE TRIGGER runtime_observe_job AFTER INSERT OR UPDATE ON qbit_prism_jobs
                    FOR EACH ROW EXECUTE FUNCTION runtime_observe_job();
            "#,
            )
            .execute(&frontends[0].ledger.pool)
            .await?;
            Ok::<_, anyhow::Error>((proxy, node))
        }
        .await;
        match opened {
            Ok((proxy, node)) => Ok(Self {
                a: frontends[0].clone(),
                b: frontends[1].clone(),
                proxy,
                node,
                admin,
                schema,
            }),
            Err(error) => {
                for frontend in frontends {
                    frontend.ledger.pool.close().await;
                }
                let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
                    .execute(&admin)
                    .await;
                admin.close().await;
                cleanup.context(format!("setup failed: {error:#}; schema cleanup failed"))?;
                Err(error)
            }
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.a.ledger.pool
    }

    pub async fn refresh(&self, nonempty: bool) -> Result<()> {
        if nonempty {
            window_fixture::WindowPlan::new(SHARES)?
                .load(self.pool(), "runtime-fixture")
                .await?;
        }
        self.a.refresh_once().await?;
        self.b.refresh_once().await?;
        for frontend in [&self.a, &self.b] {
            let prepared = frontend
                .prepared
                .read()
                .await
                .clone()
                .context("refresh published no work")?;
            ensure!(
                prepared.snapshot.shares.len() as u64 == if nonempty { SHARES } else { 0 },
                "refresh selected the wrong window"
            );
            ensure!(
                prepared.bundle.is_some() == nonempty,
                "bootstrap must remain per-worker"
            );
        }
        Ok(())
    }

    pub async fn issue(&self, worker: &Worker, ttl: Duration) -> Result<MiningJob<JobContext>> {
        let mut job = self
            .a
            .build_job(worker, "1a2b3c4d", DIFFICULTY, 0.0)
            .await?;
        job.wire.version_mask = MASK;
        self.a.persist_issued_job(worker, &job, MASK, ttl).await?;
        Ok(job)
    }

    pub async fn payload(&self, id: &str) -> Result<Value> {
        Ok(
            sqlx::query_scalar("SELECT payload FROM qbit_prism_jobs WHERE job_id=$1")
                .bind(id)
                .fetch_one(self.pool())
                .await?,
        )
    }

    pub async fn replace_payload(&self, id: &str, value: &Value) -> Result<()> {
        ensure!(
            sqlx::query("UPDATE qbit_prism_jobs SET payload=$2 WHERE job_id=$1")
                .bind(id)
                .bind(value)
                .execute(self.pool())
                .await?
                .rows_affected()
                == 1,
            "fault injection did not change a row"
        );
        Ok(())
    }

    /// Simulate disk corruption only in this disposable fixture. Ordinary
    /// UPDATE is intentionally rejected by the immutable-blob trigger.
    pub async fn replace_template_blob(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("DELETE FROM qbit_prism_templates WHERE template_sha256=$1")
            .bind(key)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO qbit_prism_templates(template_sha256,template_bytes) VALUES($1,$2)",
        )
        .bind(key)
        .bind(bytes)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn now_ms(&self) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
                .fetch_one(self.pool())
                .await?,
        )
    }

    pub async fn wait_for_settlement_waiter(&self) -> Result<()> {
        timeout(Duration::from_secs(5), async {
            loop {
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=$1::bigint::oid AND objid=$2::bigint::oid AND objsubid=1 AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))")
                    .bind(SETTLEMENT_LOCK >> 32).bind(SETTLEMENT_LOCK & 0xffff_ffff).fetch_one(&self.admin).await?;
                if waiting { return Ok::<_, anyhow::Error>(()); }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.context("no runtime SQL lock waiter observed")?
    }

    async fn close(self) -> Result<()> {
        timeout(Duration::from_secs(10), async {
            self.a.ledger.pool.close().await;
            self.b.ledger.pool.close().await;
        })
        .await
        .context("frontend pools did not close")?;
        let observed = self.proxy.finish().await;
        let cleanup = timeout(
            Duration::from_secs(10),
            sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema)).execute(&self.admin),
        )
        .await;
        self.admin.close().await;
        cleanup.context("schema cleanup timed out")??;
        observed
    }
}

pub async fn run(
    site: gate::Site,
    body: impl for<'a> FnOnce(&'a Fixture) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let _serial = SERIAL.lock().await;
    let Some(raw) = gate::database_url(site)? else {
        return Ok(());
    };
    let fixture = Fixture::open(&raw).await?;
    let result = timeout(Duration::from_secs(45), body(&fixture))
        .await
        .context("runtime test exceeded 45 seconds")
        .and_then(|result| result);
    let cleanup = fixture.close().await;
    match (result, cleanup) {
        (Ok(()), cleanup) => cleanup,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("cleanup also failed: {cleanup:#}")))
        }
    }
}

pub fn worker(name: &str) -> Worker {
    Worker {
        username: format!("{name}.rig"),
        payout_address: name.into(),
        worker_name: Some("rig".into()),
        p2mr_program_hex: "11".repeat(32),
    }
}
