//! Share appenders must queue before consuming refresh's pool headroom.
use anyhow::{bail, ensure, Context, Result};
use qbit_prism::AcceptedShare;
use qbit_prism_server::ledger::{CommitGateClosed, Ledger};
use qbit_prism_test_gate as gate;
use sqlx::{postgres::PgPoolOptions, Connection, PgConnection, PgPool};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::{sleep, timeout};

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

#[path = "support/ledger_execution_proxy.rs"]
mod execution_proxy;
use execution_proxy::{Execution, ExecutionProxy, Fault, FaultPhase};

const ORDER_LOCK: i64 = 0x505249534d000002;
const WAIT: Duration = Duration::from_secs(10);

fn share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("worker:{id:064x}"),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

async fn waiting_appends(observer: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted \
         AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) \
         AND classid=($1::bigint >> 32)::oid AND objid=($1::bigint & 4294967295)::oid AND objsubid=1",
    ).bind(ORDER_LOCK).fetch_one(observer).await?)
}

async fn wait_for_appends(observer: &mut PgConnection, count: i64) -> Result<()> {
    timeout(WAIT, async {
        while waiting_appends(observer).await? < count {
            sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("appenders never reached ORDER_LOCK")??;
    Ok(())
}

async fn pool_headroom(pool_size: u32) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = FixtureDatabase::open(&raw, "prism_admission_").await?;
    let ledger = Ledger::connect(&db.url, "append-admission".into(), pool_size, true).await?;
    let result = async {
        let mut observer = PgConnection::connect(&db.url).await?;
        let durable: bool = sqlx::query_scalar("SELECT current_setting('fsync')='on' AND current_setting('full_page_writes')='on' AND current_setting('synchronous_commit')='on'").fetch_one(&mut observer).await?;
        ensure!(durable, "fixture must use durable PostgreSQL settings");
        let mut blocker = observer.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(ORDER_LOCK).execute(&mut *blocker).await?;
        let mut monitor = PgConnection::connect(&db.url).await?;
        let mut appenders = tokio::task::JoinSet::new();
        for id in 1..=u64::from(pool_size) * 2 {
            let ledger = ledger.clone();
            appenders.spawn(async move { ledger.append(share(id), None).await });
        }
        wait_for_appends(&mut monitor, i64::from(pool_size - 1)).await?;
        // Give every runnable appender a chance to enter. The blocker still
        // owns the exact ORDER_LOCK; no successful append can free a slot.
        sleep(Duration::from_millis(50)).await;
        let waiting = waiting_appends(&mut monitor).await?;
        let read = timeout(Duration::from_millis(500), ledger.payout_revision()).await;
        eprintln!("pool={pool_size} queued={} ORDER_LOCK_waiters={waiting} control_read_completed={}", pool_size * 2, read.is_ok());
        blocker.rollback().await?;
        while let Some(result) = timeout(WAIT, appenders.join_next()).await? {
            ensure!(result??.inserted, "new share was not inserted");
        }
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger").fetch_one(&ledger.pool).await?;
        ensure!(count == i64::from(pool_size * 2), "durable row count differs");
        read.context("ORDER_LOCK waiters exhausted the pool needed by refresh")??;
        ensure!(waiting == i64::from(pool_size - 1), "append admission exceeded its cap");
        Ok(())
    }.await;
    ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test]
async fn minimum_pool_keeps_read_headroom() -> Result<()> {
    pool_headroom(2).await
}

#[tokio::test]
async fn frontend_pool_keeps_read_headroom() -> Result<()> {
    pool_headroom(16).await
}

struct Harness {
    db: FixtureDatabase,
    ledger: Ledger,
    control: PgPool,
    proxy: ExecutionProxy,
}

impl Harness {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let db = FixtureDatabase::open(&raw, "prism_admission_").await?;
        let url = url::Url::parse(&db.url)?;
        let upstream = tokio::net::lookup_host((
            url.host_str().context("database host")?,
            url.port().unwrap_or(5432),
        ))
        .await?
        .next()
        .context("database address")?;
        let proxy = ExecutionProxy::start(upstream).await?;
        let ledger = Ledger::connect(
            &proxy.rewrite_url(&db.url)?,
            "append-admission".into(),
            2,
            true,
        )
        .await?;
        let control = PgPoolOptions::new()
            .max_connections(2)
            .connect(&db.url)
            .await?;
        Ok(Some(Self {
            db,
            ledger,
            control,
            proxy,
        }))
    }

    async fn hold_order(&self) -> Result<sqlx::Transaction<'static, sqlx::Postgres>> {
        let mut tx = self.control.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(ORDER_LOCK)
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }

    async fn wait_for_appends(&self, count: i64) -> Result<()> {
        let mut observer = self.control.acquire().await?;
        wait_for_appends(&mut observer, count).await
    }

    async fn ids(&self) -> Result<Vec<String>> {
        Ok(
            sqlx::query_scalar("SELECT share_id FROM qbit_share_ledger ORDER BY share_id")
                .fetch_all(&self.control)
                .await?,
        )
    }

    async fn close(self, result: Result<()>) -> Result<()> {
        self.ledger.pool.close().await;
        self.control.close().await;
        let observed = self.proxy.finish().await;
        self.db.close(result.and(observed)).await
    }
}

struct Running<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Running<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn append(ledger: &Ledger, id: u64) -> Running<Result<qbit_prism_server::ledger::AppendResult>> {
    let ledger = ledger.clone();
    Running(tokio::spawn(
        async move { ledger.append(share(id), None).await },
    ))
}

/// Clone handles and separately constructed ledgers using the same public
/// pool must share one cap. A queued timeout must leave no transaction behind.
#[tokio::test]
async fn shared_pool_queue_uses_original_deadline_and_cancels_without_sql() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        let mut alias = Ledger::connect(&h.db.url, "pool-alias".into(), 16, false).await?;
        alias.pool.close().await;
        alias.pool = h.ledger.pool.clone();
        let blocker = h.hold_order().await?;
        let mut first = append(&h.ledger, 1);
        h.wait_for_appends(1).await?;
        let mark = h.proxy.mark();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(150);
        let expired = tokio::time::timeout_at(deadline, alias.append(share(2), None)).await;
        ensure!(
            expired.is_err(),
            "queued append did not use the caller's deadline"
        );
        ensure!(
            h.proxy
                .executions_since(mark)?
                .iter()
                .all(|e| !e.begins_transaction()),
            "queued append checked out a connection and began a transaction"
        );
        ensure!(
            timeout(Duration::from_millis(500), h.ledger.payout_revision())
                .await?
                .is_ok(),
            "control read failed"
        );
        blocker.rollback().await?;
        ensure!(timeout(WAIT, &mut first.0).await???.inserted);
        ensure!(
            timeout(WAIT, alias.append(share(3), None))
                .await
                .context("cancelled waiter leaked admission")??
                .inserted
        );
        ensure!(h.ids().await? == vec![share(1).share_id, share(3).share_id]);
        Ok(())
    }
    .await;
    h.close(result).await
}

#[tokio::test]
async fn separate_pools_have_separate_admission_and_closing_wakes_queued_appends() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let other = Ledger::connect(&h.db.url, "other-pool".into(), 2, false).await?;
    let result = async {
        let blocker = h.hold_order().await?;
        let mut first = append(&h.ledger, 1);
        let mut second = append(&other, 2);
        // Same URL, independent pools: each can admit its own transaction.
        h.wait_for_appends(2).await?;
        timeout(Duration::from_millis(500), h.ledger.payout_revision()).await??;
        timeout(Duration::from_millis(500), other.payout_revision()).await??;
        let mut queued = append(&other, 3);
        ensure!(timeout(Duration::from_millis(100), &mut queued.0)
            .await
            .is_err());
        let mut closing = {
            let pool = other.pool.clone();
            Running(tokio::spawn(async move { pool.close().await }))
        };
        let error = timeout(Duration::from_millis(500), &mut queued.0)
            .await??
            .unwrap_err();
        ensure!(
            matches!(
                error.downcast_ref::<sqlx::Error>(),
                Some(sqlx::Error::PoolClosed)
            ),
            "{error:#}"
        );
        blocker.rollback().await?;
        ensure!(timeout(WAIT, &mut first.0).await???.inserted);
        ensure!(timeout(WAIT, &mut second.0).await???.inserted);
        timeout(WAIT, &mut closing.0).await??;
        ensure!(h.ids().await? == vec![share(1).share_id, share(2).share_id]);
        Ok(())
    }
    .await;
    other.pool.close().await;
    h.close(result).await
}

/// SQLx has to consume the blocked statement before it can flush rollback.
/// Cancelling the caller alone must not admit a replacement into the spare slot.
#[tokio::test]
async fn cancelled_transaction_keeps_admission_until_connection_cleanup() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        let blocker = h.hold_order().await?;
        let mut first = append(&h.ledger, 1);
        h.wait_for_appends(1).await?;
        first.0.abort();
        ensure!((&mut first.0).await.unwrap_err().is_cancelled());
        let mark = h.proxy.mark();
        let mut next = append(&h.ledger, 2);
        ensure!(timeout(Duration::from_millis(150), &mut next.0)
            .await
            .is_err());
        ensure!(
            h.proxy
                .executions_since(mark)?
                .iter()
                .all(|e| !e.begins_transaction()),
            "permit released before rollback cleanup"
        );
        ensure!(
            timeout(Duration::from_millis(500), h.ledger.payout_revision())
                .await?
                .is_ok()
        );
        blocker.rollback().await?;
        ensure!(timeout(WAIT, &mut next.0).await???.inserted);
        ensure!(
            h.ids().await? == vec![share(2).share_id],
            "cancelled transaction left credit"
        );
        Ok(())
    }
    .await;
    h.close(result).await
}

/// Cancellation can drop an in-flight append from a thread that never
/// entered the runtime. The guard must not panic there, and its permit must
/// still outlive the queued rollback and the connection's return to the pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_dropped_outside_runtime_context_keeps_admission_until_cleanup() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        let blocker = h.hold_order().await?;
        let ledger = h.ledger.clone();
        let mut first = Box::pin(async move { ledger.append(share(1), None).await });
        // Poll the append on the runtime until it holds a connection and is
        // waiting for ORDER_LOCK, then hand the pending future to a plain
        // thread with no runtime context and drop it there.
        tokio::select! {
            result = &mut first => bail!("append finished under a held ORDER_LOCK: {result:?}"),
            waited = h.wait_for_appends(1) => waited?,
        }
        let mark = h.proxy.mark();
        let dropper = std::thread::spawn(move || drop(first));
        let joined = tokio::task::spawn_blocking(move || dropper.join()).await?;
        ensure!(
            joined.is_ok(),
            "guard dropped outside runtime context panicked"
        );
        let mut next = append(&h.ledger, 2);
        ensure!(timeout(Duration::from_millis(150), &mut next.0)
            .await
            .is_err());
        ensure!(
            h.proxy
                .executions_since(mark)?
                .iter()
                .all(|e| !e.begins_transaction()),
            "permit released before rollback cleanup"
        );
        ensure!(
            timeout(Duration::from_millis(500), h.ledger.payout_revision())
                .await?
                .is_ok()
        );
        blocker.rollback().await?;
        ensure!(timeout(WAIT, &mut next.0).await???.inserted);
        ensure!(
            h.ids().await? == vec![share(2).share_id],
            "dropped transaction left credit"
        );
        Ok(())
    }
    .await;
    h.close(result).await
}

/// A caller-supplied pool with `min_connections > 0` still reaches SQLx's own
/// off-context spawn after the guard has queued its cleanup. SQLx panics once
/// there, exactly as it did before admission existed; the guard must not add
/// a second panic during that unwind, which would abort the process, and its
/// cleanup must still finish on the captured runtime. The one SQLx panic
/// message on stderr is expected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn min_connections_pool_dropped_outside_runtime_panics_once_like_sqlx_and_still_cleans_up(
) -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        let mut ledger = Ledger::connect(&h.db.url, "min-connections".into(), 2, false).await?;
        ledger.pool.close().await;
        ledger.pool = PgPoolOptions::new()
            .max_connections(2)
            .min_connections(1)
            .connect(&h.proxy.rewrite_url(&h.db.url)?)
            .await?;
        let blocker = h.hold_order().await?;
        let appender = ledger.clone();
        let mut first = Box::pin(async move { appender.append(share(1), None).await });
        tokio::select! {
            result = &mut first => bail!("append finished under a held ORDER_LOCK: {result:?}"),
            waited = h.wait_for_appends(1) => waited?,
        }
        let mark = h.proxy.mark();
        let dropper = std::thread::spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(first)))
        });
        let outcome = tokio::task::spawn_blocking(move || dropper.join()).await?;
        let Ok(Err(panic)) = outcome else {
            bail!("expected exactly SQLx's off-context panic, got a clean drop or thread failure");
        };
        let message = panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        ensure!(
            message.contains("Tokio context"),
            "panic did not come from SQLx's own drop: {message}"
        );
        let mut next = append(&ledger, 2);
        ensure!(timeout(Duration::from_millis(150), &mut next.0)
            .await
            .is_err());
        ensure!(
            h.proxy
                .executions_since(mark)?
                .iter()
                .all(|e| !e.begins_transaction()),
            "permit released before rollback cleanup"
        );
        blocker.rollback().await?;
        ensure!(timeout(WAIT, &mut next.0).await???.inserted);
        ensure!(
            h.ids().await? == vec![share(2).share_id],
            "dropped transaction left credit"
        );
        ledger.pool.close().await;
        Ok(())
    }
    .await;
    h.close(result).await
}

#[tokio::test]
async fn queued_appends_recheck_revision_and_commit_gate() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        for change_revision in [false, true] {
            let revision = h.ledger.payout_revision().await?;
            let blocker = h.hold_order().await?;
            let mut first = append(&h.ledger, if change_revision { 3 } else { 1 });
            h.wait_for_appends(1).await?;
            let allowed = Arc::new(AtomicBool::new(true));
            let mut queued = {
                let ledger = h.ledger.clone();
                let allowed = allowed.clone();
                Running(tokio::spawn(async move {
                    ledger.append_at_revision_gated(share(2), None, revision, &|| allowed.load(Ordering::SeqCst)).await
                }))
            };
            ensure!(timeout(Duration::from_millis(100), &mut queued.0).await.is_err());
            if change_revision {
                sqlx::query("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton").execute(&h.control).await?;
            } else {
                allowed.store(false, Ordering::SeqCst);
            }
            blocker.rollback().await?;
            ensure!(timeout(WAIT, &mut first.0).await???.inserted);
            let error = timeout(WAIT, &mut queued.0).await??.unwrap_err();
            if change_revision {
                ensure!(error.to_string().contains("payout revision changed"), "{error:#}");
            } else {
                ensure!(error.downcast_ref::<CommitGateClosed>().is_some(), "{error:#}");
            }
        }
        ensure!(h.ids().await? == vec![share(1).share_id, share(3).share_id]);
        // A closed gate cannot undo a matching row that was already durable.
        let replay = h.ledger.append_at_revision_gated(share(1), None, h.ledger.payout_revision().await?, &|| false).await?;
        ensure!(!replay.inserted);
        Ok(())
    }.await;
    h.close(result).await
}

async fn mark_share_inserts(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        "CREATE FUNCTION mark_share_insert() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE NOTICE 'prism-execution-marker qbit_share_ledger INSERT'; RETURN NULL; END $$;
        CREATE TRIGGER mark_share_insert AFTER INSERT ON qbit_share_ledger
        FOR EACH STATEMENT EXECUTE FUNCTION mark_share_insert();",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn uncertain_commit_holds_admission_and_durable_replay_is_not_recredited() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        mark_share_inserts(&h.control).await?;
        let reply = h.proxy.pause_after_commit("qbit_share_ledger", "INSERT")?;
        let mut first = append(&h.ledger, 1);
        timeout(WAIT, reply.entered()).await?;
        ensure!(h.ids().await? == vec![share(1).share_id]);
        // COMMIT is durable, but the caller cannot know that while its reply
        // is withheld. Cancellation must retain the permit through cleanup.
        first.0.abort();
        ensure!((&mut first.0).await.unwrap_err().is_cancelled());
        let mark = h.proxy.mark();
        let mut next = append(&h.ledger, 2);
        ensure!(timeout(Duration::from_millis(150), &mut next.0)
            .await
            .is_err());
        ensure!(
            h.proxy
                .executions_since(mark)?
                .iter()
                .all(|e| !e.begins_transaction()),
            "uncertain COMMIT released admission before connection cleanup"
        );
        ensure!(
            timeout(Duration::from_millis(500), h.ledger.payout_revision())
                .await?
                .is_ok()
        );
        reply.release();
        ensure!(timeout(WAIT, &mut next.0).await???.inserted);
        ensure!(!h.ledger.append(share(1), None).await?.inserted);

        let mark = h.proxy.mark();
        h.proxy.plan(Fault {
            table: "qbit_share_ledger".into(),
            op: "INSERT".into(),
            phase: FaultPhase::AfterCommit,
        });
        ensure!(
            h.ledger.append(share(3), None).await.is_err(),
            "lost COMMIT reply reported success"
        );
        ensure!(h.proxy.fired().is_some());
        let executions = h.proxy.executions_since(mark)?;
        ensure!(
            executions
                .iter()
                .filter(|e| e.marked("qbit_share_ledger", "INSERT"))
                .count()
                == 1,
            "uncertain commit was retried internally"
        );
        ensure!(executions.iter().any(|e| e.is_commit() && !e.delivered()));
        ensure!(
            !timeout(WAIT, h.ledger.append(share(3), None))
                .await??
                .inserted
        );
        ensure!(h.ids().await? == vec![share(1).share_id, share(2).share_id, share(3).share_id]);
        Ok(())
    }
    .await;
    h.close(result).await
}

#[tokio::test]
async fn missing_partition_retry_reacquires_admission_and_preserves_gate_deadline() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        // A real SQLSTATE 23514 must release the first attempt, attach the
        // lead, and enter again even with a one-permit append cap.
        let bound: i64 = sqlx::query_scalar(
            "SELECT max(upper_seq) FROM qbit_prism_share_partitions WHERE state='attached'",
        )
        .fetch_one(&h.control)
        .await?;
        sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1,false)")
            .bind(bound)
            .execute(&h.control)
            .await?;
        let mark = h.proxy.mark();
        let first = timeout(WAIT, h.ledger.append(share(1), None)).await??;
        ensure!(first.inserted && first.share.share_seq == (bound + 1) as u64);
        ensure!(
            h.proxy
                .rejections_since(mark)?
                .iter()
                .filter(|e| e.code == "23514")
                .count()
                == 1
        );
        ensure!(!h.ledger.append(share(1), None).await?.inserted);

        // Partition repair waits behind the fixture's catalog lock. Its
        // delay must not renew the caller's one original pre-COMMIT deadline.
        let bound: i64 = sqlx::query_scalar(
            "SELECT max(upper_seq) FROM qbit_prism_share_partitions WHERE state='attached'",
        )
        .fetch_one(&h.control)
        .await?;
        sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1,false)")
            .bind(bound)
            .execute(&h.control)
            .await?;
        let mut blocker = h.control.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(1347571027, hashtext(current_schema()))")
            .execute(&mut *blocker)
            .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let revision = h.ledger.payout_revision().await?;
        let mut retry = {
            let ledger = h.ledger.clone();
            Running(tokio::spawn(async move {
                ledger
                    .append_at_revision_gated(share(2), None, revision, &|| {
                        tokio::time::Instant::now() < deadline
                    })
                    .await
            }))
        };
        timeout(WAIT, async {
            loop {
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=1347571027::oid AND objsubid=2 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))")
                    .fetch_one(&h.control).await?;
                if waiting { break; }
                sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        }).await.context("partition repair never reached its catalog lock")??;
        tokio::time::sleep_until(deadline).await;
        ensure!(!retry.0.is_finished(), "partition repair did not wait");
        blocker.rollback().await?;
        let error = timeout(WAIT, &mut retry.0).await??.unwrap_err();
        ensure!(
            error.downcast_ref::<CommitGateClosed>().is_some(),
            "{error:#}"
        );
        ensure!(h.ids().await? == vec![share(1).share_id]);
        ensure!(
            timeout(WAIT, h.ledger.append(share(3), None))
                .await
                .context("retry leaked admission")??
                .inserted
        );
        Ok(())
    }
    .await;
    h.close(result).await
}

/// The statements a share append ran on its connection while it held
/// ORDER_LOCK: every execution after its lock statement and before the
/// COMMIT that ended its transaction, oldest first.
fn statements_under_order_lock(executions: &[Execution]) -> Result<Vec<String>> {
    let begin = executions
        .iter()
        .position(|e| e.sql.starts_with("BEGIN; SET LOCAL plan_cache_mode"))
        .context("no share append transaction was observed")?;
    let connection = executions[begin].connection;
    let mut on_connection = executions[begin + 1..]
        .iter()
        .filter(|e| e.connection == connection);
    let lock = on_connection
        .next()
        .context("the append sent nothing after its BEGIN")?;
    ensure!(
        lock.sql == "SELECT pg_advisory_xact_lock($1)",
        "the append's first statement after BEGIN was {:?}, not its ORDER_LOCK",
        lock.sql
    );
    let mut statements = Vec::new();
    for execution in on_connection {
        if execution.is_commit() {
            return Ok(statements);
        }
        statements.push(execution.sql.clone());
    }
    bail!("the share append never committed after {statements:#?}")
}

/// #711: every client round trip a share append makes while it holds
/// ORDER_LOCK is time every other share waits behind it. A new share's
/// append reads its write guard, its payout-revision fence and its probe
/// bounds in one statement, probes for its share_id in a second, and writes
/// the clock, the row and the header mapping in a third; a duplicate stops
/// after the probe. Pin those counts, so a round trip cannot creep back in.
#[tokio::test]
async fn a_share_append_holds_order_lock_for_three_statements_and_its_commit() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        let revision = h.ledger.payout_revision().await?;
        for (id, fenced) in [(1, true), (2, false)] {
            let mark = h.proxy.mark();
            let appended = if fenced {
                h.ledger
                    .append_at_revision(share(id), None, revision)
                    .await?
            } else {
                h.ledger.append(share(id), None).await?
            };
            ensure!(appended.inserted, "share {id} was not appended");
            let statements = statements_under_order_lock(&h.proxy.executions_since(mark)?)?;
            ensure!(
                statements.len() == 3,
                "a new share's append ran {} statements under ORDER_LOCK, not 3: {statements:#?}",
                statements.len()
            );
            ensure!(
                statements[0].contains("FOR SHARE OF c") == fenced,
                "the first statement's payout-revision fence is wrong for fenced={fenced}: {:?}",
                statements[0]
            );
            ensure!(
                statements[2].starts_with("WITH clock AS (UPDATE qbit_prism_cluster"),
                "the append's last statement is not its write: {:?}",
                statements[2]
            );
        }
        let mark = h.proxy.mark();
        ensure!(
            !h.ledger
                .append_at_revision(share(1), None, revision)
                .await?
                .inserted,
            "a replayed share was appended again"
        );
        let statements = statements_under_order_lock(&h.proxy.executions_since(mark)?)?;
        ensure!(
            statements.len() == 2,
            "a duplicate's append ran {} statements under ORDER_LOCK, not 2: {statements:#?}",
            statements.len()
        );
        Ok(())
    }
    .await;
    h.close(result).await
}

/// The ledger clock, the share sequence and the count of header mappings:
/// everything a share append's write statement changes besides the row.
async fn ledger_write_state(pool: &PgPool) -> Result<(i64, i64, bool, i64)> {
    Ok(sqlx::query_as(
        "SELECT (SELECT ledger_clock_ms FROM qbit_prism_cluster WHERE singleton),last_value,is_called,\
         (SELECT count(*) FROM qbit_prism_share_hashes) FROM qbit_share_ledger_share_seq_seq",
    )
    .fetch_one(pool)
    .await?)
}

/// A share whose job was issued after the ledger clock is refused, and the
/// one statement that advanced the clock leaves nothing behind (#711): no
/// row, no header mapping, no share_seq handed out, and the clock back where
/// it was.
#[tokio::test]
async fn a_share_from_a_future_job_is_refused_and_its_write_rolls_back() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        ensure!(h.ledger.append(share(1), None).await?.inserted);
        let before = ledger_write_state(&h.control).await?;
        // The refused statement's clock would be at least this much later.
        sleep(Duration::from_millis(20)).await;
        let mut future = share(2);
        future.job_issued_at_ms = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )? + 86_400_000;
        let error = h.ledger.append(future, None).await.unwrap_err();
        ensure!(
            format!("{error:#}").contains("share references a job from the future"),
            "{error:#}"
        );
        let after = ledger_write_state(&h.control).await?;
        ensure!(
            after == before,
            "the refused share changed the ledger's clock, sequence or mappings: {before:?} -> {after:?}"
        );
        ensure!(h.ids().await? == vec![share(1).share_id]);
        ensure!(h.ledger.append(share(3), None).await?.inserted);
        Ok(())
    }
    .await;
    h.close(result).await
}

/// The append's first statement still refuses a halted cluster and a live
/// legacy Python writer before it compares the payout revision, fenced or
/// not, as the separate write guard did (#711), and writes nothing.
#[tokio::test]
async fn a_halted_cluster_or_a_live_legacy_writer_refuses_the_append_before_its_fence() -> Result<()>
{
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        let revision = h.ledger.payout_revision().await?;
        let refusals = |expected: &'static str| {
            let ledger = h.ledger.clone();
            async move {
                for fenced in [true, false] {
                    // A fenced append's expected revision is not the current
                    // one, so a revision check first would answer that.
                    let attempt = if fenced {
                        ledger.append_at_revision(share(1), None, revision + 1).await
                    } else {
                        ledger.append(share(1), None).await
                    };
                    let error = attempt.err().context("a refused append succeeded")?;
                    ensure!(
                        format!("{error:#}").contains(expected),
                        "fenced={fenced}: {error:#}"
                    );
                }
                Ok::<_, anyhow::Error>(())
            }
        };
        sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='test halt' WHERE singleton")
            .execute(&h.control)
            .await?;
        refusals("cluster halted: test halt").await?;
        sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL WHERE singleton")
            .execute(&h.control)
            .await?;
        sqlx::raw_sql(
            "ALTER TABLE qbit_ledger_writer_lease DISABLE TRIGGER qbit_prism_no_legacy_writer; \
             INSERT INTO qbit_ledger_writer_lease(singleton,writer_id,writer_epoch,writer_session_token,lease_expires_at) \
             VALUES(true,'python',1,'session',clock_timestamp()+interval '1 hour')",
        )
        .execute(&h.control)
        .await?;
        refusals("live legacy Python writer lease").await?;
        sqlx::raw_sql(
            "DELETE FROM qbit_ledger_writer_lease; \
             ALTER TABLE qbit_ledger_writer_lease ENABLE TRIGGER qbit_prism_no_legacy_writer",
        )
        .execute(&h.control)
        .await?;
        ensure!(h.ids().await?.is_empty(), "a refused append wrote a share");
        let error = h
            .ledger
            .append_at_revision(share(1), None, revision + 1)
            .await
            .err()
            .context("an append at a stale revision succeeded")?;
        ensure!(
            error.to_string().contains("payout revision changed"),
            "{error:#}"
        );
        ensure!(
            h.ledger
                .append_at_revision(share(1), None, revision)
                .await?
                .inserted
        );
        Ok(())
    }
    .await;
    h.close(result).await
}

/// A fenced append whose first statement finds a revision writer holding the
/// cluster row waits it out and compares the revision that writer committed,
/// and goes ahead when the writer rolls back: `FOR SHARE` still rechecks the
/// latest row although the statement also reads the probe's bounds (#711).
#[tokio::test]
async fn a_fenced_append_waits_out_a_revision_writer_and_compares_what_it_commits() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        for (id, commit) in [(1, true), (2, false)] {
            let revision = h.ledger.payout_revision().await?;
            let mut writer = h.control.begin().await?;
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
            )
            .execute(&mut *writer)
            .await?;
            let mut append = {
                let ledger = h.ledger.clone();
                Running(tokio::spawn(async move {
                    ledger.append_at_revision(share(id), None, revision).await
                }))
            };
            let mut observer = h.control.acquire().await?;
            timeout(WAIT, async {
                loop {
                    let waiting: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() \
                         AND wait_event_type='Lock' AND query LIKE 'WITH probe AS MATERIALIZED%')",
                    )
                    .fetch_one(&mut *observer)
                    .await?;
                    if waiting {
                        return Ok::<_, anyhow::Error>(());
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("the append's first statement never waited on the revision writer")??;
            drop(observer);
            if commit {
                writer.commit().await?;
                let error = timeout(WAIT, &mut append.0)
                    .await??
                    .err()
                    .context("an append fenced at a superseded revision succeeded")?;
                ensure!(
                    error.to_string().contains("payout revision changed"),
                    "{error:#}"
                );
            } else {
                writer.rollback().await?;
                ensure!(
                    timeout(WAIT, &mut append.0).await???.inserted,
                    "the append did not land after the revision writer rolled back"
                );
            }
        }
        ensure!(h.ids().await? == vec![share(2).share_id]);
        Ok(())
    }
    .await;
    h.close(result).await
}

/// The fenced first statement's probe columns keep the snapshot taken before
/// it waited on the cluster row. When the writer it waited on commits a
/// change to that row but leaves the revision alone, the append reads its
/// probe again (#711). Here the writer also mapped the share's header to
/// another share, so the append must refuse the header as already credited,
/// not take it for a new one and collide with the mapping on insert.
#[tokio::test]
async fn a_fenced_append_rereads_its_probe_after_waiting_on_a_cluster_row_writer() -> Result<()> {
    let Some(h) = Harness::open().await? else {
        return Ok(());
    };
    let result = async {
        let revision = h.ledger.payout_revision().await?;
        let mut writer = h.control.begin().await?;
        sqlx::query("UPDATE qbit_prism_cluster SET updated_at=clock_timestamp() WHERE singleton")
            .execute(&mut *writer)
            .await?;
        sqlx::query(
            "INSERT INTO qbit_prism_share_hashes(header_hash,share_id) VALUES($1,'other:'||$1)",
        )
        .bind(format!("{:064x}", 1))
        .execute(&mut *writer)
        .await?;
        let mut append = {
            let ledger = h.ledger.clone();
            Running(tokio::spawn(async move {
                ledger.append_at_revision(share(1), None, revision).await
            }))
        };
        let mut observer = h.control.acquire().await?;
        timeout(WAIT, async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() \
                     AND wait_event_type='Lock' AND query LIKE 'WITH probe AS MATERIALIZED%')",
                )
                .fetch_one(&mut *observer)
                .await?;
                if waiting {
                    return Ok::<_, anyhow::Error>(());
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("the append's first statement never waited on the cluster row's writer")??;
        drop(observer);
        writer.commit().await?;
        let error = timeout(WAIT, &mut append.0)
            .await??
            .err()
            .context("a share whose header another share holds was appended")?;
        ensure!(
            format!("{error:#}").contains("duplicate-share: header already credited globally"),
            "{error:#}"
        );
        ensure!(h.ids().await?.is_empty(), "the refused share was written");
        Ok(())
    }
    .await;
    h.close(result).await
}
