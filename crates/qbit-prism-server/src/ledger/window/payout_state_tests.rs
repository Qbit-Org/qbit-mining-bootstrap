//! Real PostgreSQL tests for the private refresh observation.
use super::*;
use futures_util::future::LocalBoxFuture;
use qbit_prism_test_gate as gate;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_util::task::AbortOnDropHandle;

#[allow(dead_code)]
#[path = "../../../tests/support/ledger_database.rs"]
mod database;

use crate::ledger::execution_proxy as proxy;

tokio::task_local! {
    pub(super) static HASH_CALLS: Arc<AtomicUsize>;
    pub(super) static HASH_GATE: Arc<dyn Fn() + Send + Sync>;
}

async fn run(
    site: gate::Site,
    body: impl for<'a> FnOnce(&'a Ledger, &'a PgPool) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let Some(raw) = gate::database_url(site)? else {
        return Ok(());
    };
    let fixture = database::FixtureDatabase::open(&raw, "refresh_probe_").await?;
    let mut ledger = match Ledger::connect(&fixture.url, "refresh-probe".into(), 1, true).await {
        Ok(ledger) => ledger,
        Err(error) => return Err(fixture.abandon(error).await),
    };
    // Ledger sizes its runtime pool above one; these tests need one exact
    // session for checkout cancellation and session-setting recovery.
    ledger.pool.close().await;
    ledger.pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&fixture.url)
        .await?;
    let admin = PgPool::connect(&fixture.url).await?;
    let result = body(&ledger, &admin).await;
    ledger.pool.close().await;
    admin.close().await;
    fixture.close(result).await
}

fn balance() -> CarryForwardBalance {
    CarryForwardBalance {
        recipient_id: "prior".into(),
        order_key: "prior".into(),
        p2mr_program_hex: "22".repeat(32),
        balance_sats: 12345,
    }
}

async fn seed(pool: &PgPool) -> Result<()> {
    sqlx::query("INSERT INTO qbit_payout_carry_forward_current(miner_id,payout_order_key,p2mr_program,balance_sats,active_row_count) VALUES('prior','prior',decode(repeat('22',32),'hex'),12345,1)")
        .execute(pool).await?;
    Ok(())
}

async fn insert_share(connection: &mut sqlx::PgConnection, seq: i64, accepted: bool) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted,reject_reason,writer_id,writer_epoch) VALUES($1,$1::text,'miner','miner',decode(repeat('11',32),'hex'),1,1000,100,'job',now(),100,$2,CASE WHEN $2 THEN NULL ELSE 'low-difficulty' END,'test',0)")
        .bind(seq).bind(accepted).execute(connection).await?;
    Ok(())
}

#[tokio::test]
async fn same_revision_balance_identity_and_accepted_cutoff_are_fresh() -> Result<()> {
    run(gate::site!(), |ledger, admin| Box::pin(async move {
        let empty = ledger.refresh_probe(ReadAdmission::default()).await?;
        assert_eq!(empty.accepted_share_seq, 0);
        assert_eq!(empty.payout_state.prior_balances_digest, qbit_prism::prior_balances_digest(&[]));
        seed(admin).await?;
        let mut connection = admin.acquire().await?;
        insert_share(&mut connection, 7, true).await?;
        insert_share(&mut connection, 9, false).await?;
        let original = ledger.refresh_probe(ReadAdmission::default()).await?;
        assert_eq!(original.accepted_share_seq, 7);
        assert_eq!(original.payout_state, ledger.payout_state().await?);
        let mut expected = balance();
        for sql in [
            "UPDATE qbit_payout_carry_forward_current SET balance_sats=67890",
            "UPDATE qbit_payout_carry_forward_current SET miner_id='changed'",
            "UPDATE qbit_payout_carry_forward_current SET payout_order_key='changed'",
            "UPDATE qbit_payout_carry_forward_current SET p2mr_program=decode(repeat('33',32),'hex')",
        ] {
            sqlx::query(sql).execute(&mut *connection).await?;
            if sql.contains("balance_sats=") { expected.balance_sats = 67890; }
            if sql.contains("miner_id=") { expected.recipient_id = "changed".into(); }
            if sql.contains("payout_order_key=") { expected.order_key = "changed".into(); }
            if sql.contains("p2mr_program=") { expected.p2mr_program_hex = "33".repeat(32); }
            let changed = ledger.refresh_probe(ReadAdmission::default()).await?;
            assert_eq!(changed.payout_state.payout_revision, original.payout_state.payout_revision);
            assert_eq!(changed.accepted_share_seq, 7);
            assert_ne!(changed.payout_state.prior_balances_digest, original.payout_state.prior_balances_digest);
            assert_eq!(changed.payout_state.prior_balances_digest, qbit_prism::prior_balances_digest(&[expected.clone()]));
        }
        // Public admission still works without either share history or stored economics.
        sqlx::raw_sql("ALTER TABLE qbit_share_ledger RENAME TO hidden_history; ALTER TABLE qbit_prism_balance_snapshots RENAME TO hidden_snapshots")
            .execute(&mut *connection).await?;
        assert_eq!(ledger.payout_state().await?.prior_balances_digest, qbit_prism::prior_balances_digest(&[expected]));
        assert!(matches!(ledger.refresh_probe(ReadAdmission::default()).await, Err(WindowError::Database(_))));
        Ok(())
    })).await
}

async fn wait_for_advisory_gate(admin: &PgPool, key: i64) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=0 AND objid::bigint=$1)")
                .bind(key).fetch_one(admin).await?;
            if blocked { return Ok::<_, anyhow::Error>(()); }
            sleep(Duration::from_millis(10)).await;
        }
    }).await.context("metadata did not reach the first-SELECT gate")?
}

#[tokio::test]
async fn revision_cutoff_and_digest_share_one_snapshot_with_weaker_isolation_control() -> Result<()>
{
    run(gate::site!(), |ledger, admin| Box::pin(async move {
        seed(admin).await?;
        let key: i64 = sqlx::query_scalar("SELECT oid::bigint FROM pg_namespace WHERE nspname=current_schema()")
            .fetch_one(admin).await?;
        sqlx::raw_sql(&format!(
            "ALTER TABLE qbit_prism_cluster RENAME TO probe_state_source;
             CREATE FUNCTION delayed_probe_revision(revision bigint) RETURNS bigint
             LANGUAGE plpgsql VOLATILE AS 'BEGIN PERFORM pg_advisory_xact_lock({key}); RETURN revision; END;';
             CREATE VIEW qbit_prism_cluster AS SELECT singleton,fatal_error,
                 delayed_probe_revision(payout_revision) AS payout_revision FROM probe_state_source"
        )).execute(admin).await?;
        for repeatable_read in [true, false] {
            let original = ledger.refresh_probe(ReadAdmission::default()).await?;
            let mut change = admin.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(key).execute(&mut *change).await?;
            let source = ledger.clone();
            let reader = AbortOnDropHandle::new(tokio::spawn(async move {
                if repeatable_read { return source.refresh_probe(ReadAdmission::default()).await; }
                // Negative control: same metadata SQL and balance decoder but
                // weaker isolation, with the writer committing before SELECT 2.
                let mut tx = source.begin().await?;
                sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").execute(&mut *tx).await?;
                let payout_revision = sqlx::query_scalar(PAYOUT_REVISION_SQL).fetch_one(&mut *tx).await?;
                let (accepted_share_seq, prior_balances_digest) = refresh_balances(&mut tx, &ReadAdmission::default()).await?;
                tx.commit().await?;
                Ok(RefreshProbe { payout_state: PayoutState { payout_revision, prior_balances_digest }, accepted_share_seq })
            }));
            wait_for_advisory_gate(admin, key).await?;
            sqlx::raw_sql("UPDATE probe_state_source SET payout_revision=payout_revision+1; UPDATE qbit_payout_carry_forward_current SET balance_sats=balance_sats+1")
                .execute(&mut *change).await?;
            insert_share(&mut change, original.accepted_share_seq as i64 + 1, true).await?;
            change.commit().await?;
            let during = timeout(Duration::from_secs(5), reader).await???;
            let after = ledger.refresh_probe(ReadAdmission::default()).await?;
            assert_eq!(after.payout_state.payout_revision, original.payout_state.payout_revision + 1);
            assert_eq!(after.accepted_share_seq, original.accepted_share_seq + 1);
            assert_ne!(after.payout_state.prior_balances_digest, original.payout_state.prior_balances_digest);
            assert_eq!(during.payout_state.payout_revision, original.payout_state.payout_revision);
            if repeatable_read {
                assert_eq!(during, original, "mixed metadata and balance snapshots");
            } else {
                assert_ne!(during, original, "negative control failed to detect the weaker isolation");
                assert_eq!(during.payout_state.prior_balances_digest, after.payout_state.prior_balances_digest);
                assert_eq!(during.accepted_share_seq, after.accepted_share_seq);
            }
        }
        Ok(())
    })).await
}

#[tokio::test]
async fn unavailable_read_only_and_invalid_values_are_errors() -> Result<()> {
    run(gate::site!(), |ledger, admin| {
        Box::pin(async move {
            sqlx::query("SET default_transaction_read_only=on")
                .execute(&ledger.pool)
                .await?;
            let read_only = ledger.refresh_probe(ReadAdmission::default()).await;
            sqlx::query("SET default_transaction_read_only=off")
                .execute(&ledger.pool)
                .await?;
            assert!(matches!(
                read_only,
                Err(WindowError::Database(sqlx::Error::RowNotFound))
            ));
            sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='test'")
                .execute(admin)
                .await?;
            assert!(matches!(
                ledger.refresh_probe(ReadAdmission::default()).await,
                Err(WindowError::Database(sqlx::Error::RowNotFound))
            ));
            sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL")
                .execute(admin)
                .await?;
            let mut connection = admin.acquire().await?;
            insert_share(&mut connection, -1, true).await?;
            assert!(matches!(
                ledger.refresh_probe(ReadAdmission::default()).await,
                Err(WindowError::Decode(_))
            ));
            insert_share(&mut connection, 0, true).await?;
            seed(admin).await?;
            sqlx::query(
                "UPDATE qbit_payout_carry_forward_current SET balance_sats=power(10::numeric,50)",
            )
            .execute(admin)
            .await?;
            assert!(matches!(
                ledger.refresh_probe(ReadAdmission::default()).await,
                Err(WindowError::Decode(_))
            ));
            sqlx::query("ALTER TABLE qbit_payout_carry_forward_current RENAME TO hidden_current")
                .execute(admin)
                .await?;
            assert!(matches!(
                ledger.refresh_probe(ReadAdmission::default()).await,
                Err(WindowError::Database(_))
            ));
            sqlx::query("DELETE FROM qbit_prism_cluster")
                .execute(admin)
                .await?;
            assert!(matches!(
                ledger.refresh_probe(ReadAdmission::default()).await,
                Err(WindowError::Database(sqlx::Error::RowNotFound))
            ));
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn cancellation_and_sql_timeout_release_probe_connection() -> Result<()> {
    run(gate::site!(), |ledger, admin| Box::pin(async move {
        seed(admin).await?;
        let original = ledger.refresh_probe(ReadAdmission::default()).await?;
        // Cancellation during checkout never starts a transaction.
        let connection = ledger.pool.acquire().await?;
        assert!(timeout(Duration::from_millis(20), ledger.refresh_probe(ReadAdmission::default())).await.is_err());
        drop(connection);
        assert_eq!(ledger.refresh_probe(ReadAdmission::default()).await?, original);
        for table in ["qbit_share_ledger", "qbit_payout_carry_forward_current"] {
            for cancel in [false, true] {
                sqlx::query(if cancel { "SET statement_timeout='5s'" } else { "SET statement_timeout='80ms'" })
                    .execute(&ledger.pool).await?;
                let mut lock = admin.begin().await?;
                sqlx::query(&format!("LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")).execute(&mut *lock).await?;
                if cancel {
                    let source = ledger.clone();
                    let reader = AbortOnDropHandle::new(tokio::spawn(async move { source.refresh_probe(ReadAdmission::default()).await }));
                    timeout(Duration::from_secs(5), async {
                        loop {
                            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation=$1::regclass AND NOT granted)")
                                .bind(table).fetch_one(admin).await?;
                            if blocked { return Ok::<_, anyhow::Error>(()); }
                            sleep(Duration::from_millis(10)).await;
                        }
                    }).await??;
                    reader.abort();
                    assert!(reader.await.unwrap_err().is_cancelled());
                } else {
                    let error = ledger.refresh_probe(ReadAdmission::default()).await.unwrap_err();
                    assert!(matches!(error, WindowError::Database(sqlx::Error::Database(ref error)) if error.code().as_deref() == Some("57014")), "{error}");
                }
                lock.rollback().await?;
                assert_eq!(timeout(Duration::from_secs(2), ledger.refresh_probe(ReadAdmission::default())).await??, original);
            }
        }
        Ok(())
    })).await
}

async fn proxied(ledger: &Ledger) -> Result<(Ledger, proxy::ExecutionProxy)> {
    let options = (*ledger.pool.connect_options()).clone();
    let upstream = format!("{}:{}", options.get_host(), options.get_port()).parse()?;
    let observer = proxy::ExecutionProxy::start(upstream).await?;
    let endpoint = url::Url::parse(&observer.rewrite_url("postgresql://localhost/postgres")?)?;
    let mut source = ledger.clone();
    source.pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .connect_with(
            options
                .host(endpoint.host_str().unwrap())
                .port(endpoint.port().unwrap())
                .ssl_mode(sqlx::postgres::PgSslMode::Disable),
        )
        .await?;
    source.metrics = Some(Arc::new(crate::metrics::Metrics::default()));
    Ok((source, observer))
}

#[tokio::test]
async fn one_checkout_five_executions_and_one_hash_per_probe() -> Result<()> {
    run(gate::site!(), |ledger, admin| Box::pin(async move {
        let (source, observer) = proxied(ledger).await?;
        let hashes = Arc::new(AtomicUsize::new(0));
        HASH_CALLS.scope(hashes.clone(), async {
            for (calls, recipients) in [(1, 0), (2, 1), (3, 2)] {
                if recipients == 1 { seed(admin).await?; }
                if recipients == 2 {
                    sqlx::query("INSERT INTO qbit_payout_carry_forward_current(miner_id,payout_order_key,p2mr_program,balance_sats,active_row_count) VALUES('second','second',decode(repeat('33',32),'hex'),987,1)").execute(admin).await?;
                }
                let expected = ledger.payout_state().await?;
                let mark = observer.mark();
                let started = std::time::Instant::now();
                let probe = source.refresh_probe(ReadAdmission::default()).await?;
                assert_eq!(probe.payout_state, expected);
                let elapsed = started.elapsed();
                let executions = observer.executions_since(mark)?;
                assert_eq!(executions.len(), 5, "{executions:?}");
                assert!(executions.iter().all(proxy::Execution::complete_response));
                assert_eq!(executions.iter().filter(|e| e.sql.contains(ACCEPTED_CUTOFF_SQL)).count(), 1);
                let balances: Vec<_> = executions.iter().filter(|e| e.sql.contains("qbit_current_carry_forward_balances()")).collect();
                assert_eq!(balances.len(), 1);
                assert_eq!(balances[0].returned_rows()?, recipients.max(1));
                assert_eq!(executions.iter().map(|e| e.rows_received).sum::<u64>(), recipients.max(1) + 1);
                assert_eq!(hashes.load(Ordering::SeqCst), calls);
                let checkout = format!("qbit_prism_database_pool_acquire_seconds_count{{result=\"success\"}} {calls}");
                assert!(source.metrics.as_ref().unwrap().render().lines().any(|line| line == checkout));
                eprintln!("refresh probe recipients={recipients}: executions=5 checkouts=1 hashes=1 rows={} wall_us={}", recipients.max(1) + 1, elapsed.as_micros());
            }
            Ok::<_, anyhow::Error>(())
        }).await?;
        source.pool.close().await;
        observer.finish().await
    })).await
}

#[tokio::test]
async fn lost_or_cancelled_commit_reply_never_returns_a_successful_probe() -> Result<()> {
    run(gate::site!(), |ledger, admin| Box::pin(async move {
        seed(admin).await?;
        let original = ledger.refresh_probe(ReadAdmission::default()).await?;
        // Emit an observer marker without changing the production balance
        // result or adding a write to the read-only probe transaction.
        sqlx::raw_sql("ALTER FUNCTION qbit_current_carry_forward_balances() RENAME TO probe_balance_source;
            CREATE FUNCTION qbit_current_carry_forward_balances() RETURNS TABLE(miner_id text,payout_order_key text,p2mr_program bytea,balance_sats numeric)
            LANGUAGE plpgsql STABLE AS $$ BEGIN RAISE NOTICE 'prism-execution-marker refresh_probe SELECT'; RETURN QUERY SELECT * FROM probe_balance_source(); END $$")
            .execute(admin).await?;
        let (source, observer) = proxied(ledger).await?;
        for phase in [proxy::FaultPhase::AfterExecution, proxy::FaultPhase::AfterCommit] {
            observer.plan(proxy::Fault { table: "refresh_probe".into(), op: "SELECT".into(), phase });
            assert!(matches!(source.refresh_probe(ReadAdmission::default()).await, Err(WindowError::Database(_))));
            assert!(observer.fired().is_some(), "fault never reached the intended acknowledgement");
            assert_eq!(timeout(Duration::from_secs(2), source.refresh_probe(ReadAdmission::default())).await??, original);
        }
        let pause = observer.pause_after_commit("refresh_probe", "SELECT")?;
        let pending = source.clone();
        let reader = AbortOnDropHandle::new(tokio::spawn(async move { pending.refresh_probe(ReadAdmission::default()).await }));
        timeout(Duration::from_secs(5), pause.entered()).await?;
        reader.abort();
        assert!(reader.await.unwrap_err().is_cancelled());
        pause.release();
        assert_eq!(timeout(Duration::from_secs(2), source.refresh_probe(ReadAdmission::default())).await??, original);
        source.pool.close().await;
        observer.finish().await
    })).await
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_hash_keeps_build_admission_until_blocking_cleanup_finishes() -> Result<()> {
    run(gate::site!(), |ledger, admin| {
        Box::pin(async move {
            seed(admin).await?;
            let admission = Arc::new(tokio::sync::Semaphore::new(1));
            let completion = ReadAdmission::new(admission.clone().acquire_owned().await?);
            let ready = Arc::new(tokio::sync::Notify::new());
            let (release, receiver) = std::sync::mpsc::channel();
            let receiver = std::sync::Mutex::new(receiver);
            let runtime_thread = std::thread::current().id();
            let gate: Arc<dyn Fn() + Send + Sync> = {
                let ready = ready.clone();
                Arc::new(move || {
                    assert_ne!(std::thread::current().id(), runtime_thread);
                    ready.notify_one();
                    // Dropping the sender on test failure also releases cleanup.
                    let _ = receiver.lock().unwrap().recv();
                })
            };
            let source = ledger.clone();
            let reader = AbortOnDropHandle::new(tokio::spawn(
                HASH_GATE.scope(gate, async move { source.refresh_probe(completion).await }),
            ));
            timeout(Duration::from_secs(5), ready.notified()).await?;
            reader.abort();
            assert!(reader.await.unwrap_err().is_cancelled());
            assert_eq!(
                admission.available_permits(),
                0,
                "detached hash released admission early"
            );
            let recovered: i32 = timeout(
                Duration::from_secs(2),
                sqlx::query_scalar("SELECT 1").fetch_one(&ledger.pool),
            )
            .await??;
            assert_eq!(
                recovered, 1,
                "cancelled transaction did not release its connection"
            );
            release.send(())?;
            let permit =
                timeout(Duration::from_secs(2), admission.clone().acquire_owned()).await??;
            drop(permit);
            assert_eq!(admission.available_permits(), 1);
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn failed_guard_precedes_locked_share_history() -> Result<()> {
    run(gate::site!(), |ledger, admin| Box::pin(async move {
        sqlx::query("SET statement_timeout='200ms'").execute(&ledger.pool).await?;
        let mut lock = admin.begin().await?;
        sqlx::query("LOCK TABLE qbit_share_ledger IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await?;
        sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='compound-fault'").execute(admin).await?;
        // Negative control: the previous combined metadata SELECT waits for a
        // relation lock before it can evaluate the fatal-state predicate.
        let mut tx = ledger.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *tx).await?;
        let started = std::time::Instant::now();
        let error = sqlx::query(&format!("SELECT payout_revision, ({ACCEPTED_CUTOFF_SQL}) AS accepted_share_seq FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL AND NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off'"))
            .fetch_one(&mut *tx).await.unwrap_err();
        eprintln!("old combined guard under fatal+locked shares: {error}, wall_us={}", started.elapsed().as_micros());
        assert!(matches!(error, sqlx::Error::Database(ref error) if error.code().as_deref() == Some("57014")));
        tx.rollback().await?;
        for state in ["fatal", "read-only", "missing"] {
            if state == "read-only" {
                sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL").execute(admin).await?;
                sqlx::query("SET default_transaction_read_only=on").execute(&ledger.pool).await?;
            } else if state == "missing" {
                sqlx::query("SET default_transaction_read_only=off").execute(&ledger.pool).await?;
                sqlx::query("DELETE FROM qbit_prism_cluster").execute(admin).await?;
            }
            let started = std::time::Instant::now();
            let error = ledger.refresh_probe(ReadAdmission::default()).await.unwrap_err();
            eprintln!("guard-first {state}+locked shares: {error}, wall_us={}", started.elapsed().as_micros());
            assert!(matches!(error, WindowError::Database(sqlx::Error::RowNotFound)), "{error}");
            // The old public payout path is the reference guard contract.
            assert!(matches!(ledger.payout_state().await, Err(WindowError::Database(sqlx::Error::RowNotFound))));
        }
        lock.rollback().await?;
        Ok(())
    })).await
}
