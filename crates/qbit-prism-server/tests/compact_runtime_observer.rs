//! Independent PostgreSQL/HTTP controls for real compact runtime qualification.
use anyhow::{ensure, Context, Result};
use futures_util::{future::LocalBoxFuture, FutureExt};
use qbit_prism_server::{
    coordinator::Coordinator,
    ledger::{BalanceSource, Ledger, WindowRef},
    metrics::Metrics,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

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
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let (version, fsync): (String, String) =
            sqlx::query_as("SELECT current_setting('server_version_num'),current_setting('fsync')")
                .fetch_one(&admin)
                .await?;
        ensure!(
            version.parse::<u32>()? / 10_000 == 16 && fsync == "on",
            "observer qualification requires PostgreSQL 16 with fsync on"
        );
        let schema = format!("compact_runtime_observer_{}", uuid::Uuid::new_v4().simple());
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
        let ledger = Ledger::connect(&url, "runtime-observer".into(), 4, true).await?;
        Ok(Some(Self {
            admin,
            direct,
            ledger,
            proxy,
            schema,
            url,
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
        proxy?;
        schema?;
        Ok(())
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

#[tokio::test]
async fn jsonb_probe_counts_unchanged_and_intermediate_row_writes() -> Result<()> {
    run(|db| async move {
        sqlx::raw_sql("CREATE TABLE observed_values(id integer PRIMARY KEY,payload jsonb,extra jsonb,note integer)").execute(&db.direct).await?;
        let probe = observer::JsonbProbe::install(&db.direct, &db.schema).await?;
        let payload = json!({"large": "x".repeat(100_000)});
        sqlx::query("INSERT INTO observed_values VALUES(1,$1,NULL,0)").bind(&payload).execute(&db.ledger.pool).await?;
        let expected: i32 = sqlx::query_scalar("SELECT pg_column_size(payload::text::jsonb) FROM observed_values").fetch_one(&db.direct).await?;
        let mut inventory = jsonb_inventory::Inventory::discover(&db.direct, &db.schema).await?;
        inventory.observe(&db.direct, jsonb_inventory::Observe::Baseline).await?;

        let mark = db.proxy.mark();
        sqlx::query("UPDATE observed_values SET note=1 WHERE id=1").execute(&db.ledger.pool).await?;
        let measured = probe.measure(&db.proxy, mark)?;
        ensure!(measured.rows == 1 && measured.values == 1);
        ensure!(measured.max_uncompressed_bytes == expected as u64);
        ensure!(inventory.observe(&db.direct, jsonb_inventory::Observe::Phase("unchanged")).await?.is_empty());
        ensure!(inventory.rewritten_unchanged.len() == 1);

        let mark = db.proxy.mark();
        let mut tx = db.ledger.pool.begin().await?;
        sqlx::query("UPDATE observed_values SET payload=$1 WHERE id=1").bind(json!({"larger": "y".repeat(200_000)})).execute(&mut *tx).await?;
        sqlx::query("UPDATE observed_values SET payload=$1 WHERE id=1").bind(&payload).execute(&mut *tx).await?;
        tx.commit().await?;
        let measured = probe.measure(&db.proxy, mark)?;
        ensure!(measured.rows == 2 && measured.values == 2 && measured.max_uncompressed_bytes > expected as u64);
        ensure!(inventory.observe(&db.direct, jsonb_inventory::Observe::Phase("restored")).await?.is_empty());

        let mark = db.proxy.mark();
        sqlx::query("INSERT INTO observed_values VALUES(1,$1,NULL,0) ON CONFLICT DO NOTHING").bind(&payload).execute(&db.ledger.pool).await?;
        let measured = probe.measure(&db.proxy, mark)?;
        ensure!(measured.rows == 0 && measured.values == 0 && measured.max_uncompressed_bytes == 0);
        Ok(())
    }.boxed_local()).await
}

#[tokio::test]
async fn row_observer_distinguishes_real_window_pages_from_probes_and_metadata() -> Result<()> {
    run(|db| {
        async move {
            let plan = window_fixture::WindowPlan::new(5_000)?;
            plan.load(&db.direct, "observer-window").await?;
            let snapshot = db.ledger.snapshot(plan.window_network_difficulty()).await?;
            let reference = WindowRef::from_snapshot(&snapshot)?;
            let mark = db.proxy.mark();
            let window = db
                .ledger
                .read_window(&reference, BalanceSource::Current)
                .await?;
            ensure!(window.shares.len() == 5_000);
            let executions = db.proxy.executions_since(mark)?;
            let mut pages = Vec::new();
            let mut probes = 0;
            let mut metadata = 0;
            for execution in &executions {
                if execution.sql.contains("FROM qbit_share_ledger")
                    && execution.sql.contains("payout_order_key")
                {
                    pages.push(execution.returned_rows()?);
                } else if execution
                    .sql
                    .contains("SELECT EXISTS(SELECT 1 FROM qbit_share_ledger")
                {
                    probes += execution.returned_rows()?;
                } else if execution
                    .completion()
                    .is_some_and(|tag| tag.starts_with("SELECT "))
                {
                    metadata += execution.returned_rows()?;
                }
            }
            ensure!(pages == [4096, 904], "observed pages: {pages:?}");
            ensure!(
                probes == 1 && metadata >= 1,
                "probes={probes}, metadata={metadata}"
            );
            Ok(())
        }
        .boxed_local()
    })
    .await
}

#[tokio::test]
async fn observer_keeps_failed_and_unavailable_operations_distinct_from_zero() -> Result<()> {
    run(|db| {
        async move {
            let probe = observer::JsonbProbe::install(&db.direct, &db.schema).await?;
            let mark = db.proxy.mark();
            let rows = sqlx::query("SELECT 1 WHERE false")
                .fetch_all(&db.ledger.pool)
                .await?;
            ensure!(rows.is_empty());
            let execution = db
                .proxy
                .executions_since(mark)?
                .into_iter()
                .find(|e| e.sql == "SELECT 1 WHERE false")
                .context("SELECT missing")?;
            ensure!(execution.returned_rows()? == 0);
            ensure!(probe.measure(&db.proxy, mark)?.rows == 0);

            let mark = db.proxy.mark();
            ensure!(sqlx::query("SELECT 1/0")
                .execute(&db.ledger.pool)
                .await
                .is_err());
            ensure!(probe.measure(&db.proxy, mark).is_err());
            for execution in db.proxy.executions_since(mark)? {
                ensure!(execution.returned_rows().is_err());
            }
            ensure!(!db.proxy.rejections_since(mark)?.is_empty());

            let mut blocker = db.direct.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock(273901)")
                .execute(&mut *blocker)
                .await?;
            let mark = db.proxy.mark();
            let pool = db.ledger.pool.clone();
            let pending = tokio::spawn(async move {
                sqlx::query("SELECT pg_advisory_xact_lock(273901)")
                    .fetch_all(&pool)
                    .await
            });
            let observed = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(execution) = db
                        .proxy
                        .executions_since(mark)?
                        .into_iter()
                        .find(|execution| execution.sql == "SELECT pg_advisory_xact_lock(273901)")
                    {
                        break Ok::<_, anyhow::Error>(execution);
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await??;
            ensure!(observed.returned_rows().is_err());
            ensure!(probe.measure(&db.proxy, mark).is_err());
            blocker.rollback().await?;
            pending.await??;
            Ok(())
        }
        .boxed_local()
    })
    .await
}

#[tokio::test]
async fn a_broken_write_observer_never_recovers_as_a_healthy_zero() -> Result<()> {
    run(|db| {
        async move {
            let probe = observer::JsonbProbe::install(&db.direct, &db.schema).await?;
            let mark = db.proxy.mark();
            ensure!(
                sqlx::raw_sql("DO $$ BEGIN RAISE NOTICE 'prism-jsonb-write {}'; END $$")
                    .execute(&db.ledger.pool)
                    .await
                    .is_err()
            );
            ensure!(probe.measure(&db.proxy, mark).is_err());
            ensure!(probe.measure(&db.proxy, mark).is_err());
            Ok(())
        }
        .boxed_local()
    })
    .await
}

#[tokio::test]
async fn coordinator_refresh_is_observed_with_insert_lsn_and_real_jsonb_writes() -> Result<()> {
    run(|db| async move {
        let node = fake_qbitd::FakeNode::open().await?;
        let coordinator = Coordinator::new(
            fake_qbitd::coordinator_config(db.url.clone(), &node, "observed-refresh")?,
            Arc::new(Metrics::default()),
        ).await?;
        let result = async {
            let plan = window_fixture::WindowPlan::new(4_000)?;
            plan.load(&db.direct, "observer-refresh").await?;
            let probe = observer::JsonbProbe::install(&db.direct, &db.schema).await?;
            let mark = db.proxy.mark();
            let before = observer::insert_lsn(&db.direct).await?;
            coordinator.refresh_once().await?;
            let after = observer::insert_lsn(&db.direct).await?;
            let measured = probe.measure(&db.proxy, mark)?;
            ensure!(measured.rows > 0 && measured.values > 0 && measured.max_uncompressed_bytes > 0);
            ensure!(observer::wal_bytes(&db.direct, &before, &after).await? > 0);
            let key = coordinator.prepared.read().await.as_ref().context("refresh did not publish")?.storage_key.clone();
            let expected: i32 = sqlx::query_scalar("SELECT pg_column_size(payload::text::jsonb) FROM qbit_prism_jobs WHERE job_id=$1").bind(key).fetch_one(&db.direct).await?;
            ensure!(measured.max_uncompressed_bytes >= expected as u64);
            ensure!(observer::wal_bytes(&db.direct, &after, &before).await.is_err());
            ensure!(observer::wal_bytes(&db.direct, &after, &after).await? == 0);
            Ok(())
        }.await;
        coordinator.ledger.pool.close().await;
        result
    }.boxed_local()).await
}

async fn rpc(url: String, method: &str) -> Result<Value> {
    Ok(reqwest::Client::new()
        .post(url)
        .json(&json!({"id":1,"method":method,"params":[]}))
        .send()
        .await?
        .json()
        .await?)
}

#[tokio::test]
async fn node_pause_preserves_captured_reply_and_releases_cancelled_guards() -> Result<()> {
    let node = fake_qbitd::FakeNode::open().await?;
    let mut pause = node.pause_next("getbestblockhash")?;
    let pending = tokio::spawn(rpc(node.url.clone(), "getbestblockhash"));
    tokio::time::timeout(Duration::from_secs(3), pause.entered()).await??;
    node.set_tip(&"ef".repeat(32), &"ab".repeat(32), 101, "02");
    let next_pause = node.pause_next("getbestblockhash")?;
    pause.release();
    ensure!(pending.await??["result"] == "ab".repeat(32));
    ensure!(
        node.pause_next("getbestblockhash").is_err(),
        "old guard removed the new pause"
    );
    drop(next_pause);
    ensure!(rpc(node.url.clone(), "getbestblockhash").await?["result"] == "ef".repeat(32));
    node.set_template(Some(json!({"fixed":"template"})));
    ensure!(
        rpc(node.url.clone(), "getblocktemplate").await?["result"] == json!({"fixed":"template"})
    );
    node.set_template(None);
    ensure!(rpc(node.url.clone(), "getblocktemplate").await?["result"]["height"] == 102);
    Ok(())
}

#[tokio::test]
async fn completed_commit_pause_releases_database_locks_before_reply_delivery() -> Result<()> {
    run(|db| async move {
        sqlx::raw_sql("CREATE TABLE paused_commits(id integer PRIMARY KEY);
            CREATE FUNCTION mark_paused_commit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE NOTICE 'prism-execution-marker paused_commits INSERT'; RETURN NULL; END $$;
            CREATE TRIGGER mark_paused_commit AFTER INSERT ON paused_commits
            FOR EACH STATEMENT EXECUTE FUNCTION mark_paused_commit()")
            .execute(&db.direct).await?;
        for release_explicitly in [false, true] {
            let pause = db.proxy.pause_after_commit("paused_commits", "INSERT")?;
            let mut tx = db.ledger.pool.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock(427301)").execute(&mut *tx).await?;
            sqlx::query("INSERT INTO paused_commits VALUES($1)")
                .bind(i32::from(release_explicitly)).execute(&mut *tx).await?;
            let mut commit = Box::pin(tx.commit());
            let seq = tokio::select! {
                entered = tokio::time::timeout(Duration::from_secs(5), pause.entered()) => entered?,
                result = &mut commit => { result?; anyhow::bail!("commit reply escaped its delivery pause"); }
            };
            let observed = db.proxy.executions_since(seq - 1)?;
            let completed = observed.iter().find(|execution| execution.seq == seq).context("missing commit")?;
            ensure!(completed.is_commit() && !completed.delivered());
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM paused_commits").fetch_one(&db.direct).await?;
            ensure!(count == 1 + i64::from(release_explicitly), "committed row must be visible before the reply");
            let mut independent = db.direct.begin().await?;
            let unlocked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(427301)")
                .fetch_one(&mut *independent).await?;
            ensure!(unlocked, "completed transaction must release its locks");
            independent.rollback().await?;
            ensure!(tokio::time::timeout(Duration::from_millis(10), &mut commit).await.is_err(),
                "reply must remain paused after unrelated SQL completes");
            if release_explicitly { pause.release(); }
            drop(pause);
            tokio::time::timeout(Duration::from_secs(5), commit).await??;
            let observed = db.proxy.executions_since(seq - 1)?;
            ensure!(observed.iter().find(|execution| execution.seq == seq).context("missing delivered commit")?.delivered());
        }
        Ok(())
    }.boxed_local()).await
}
