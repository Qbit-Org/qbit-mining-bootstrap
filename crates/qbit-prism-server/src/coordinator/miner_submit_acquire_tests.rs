//! The coordinator clock and block-only probes use the real checkout boundary.
use super::*;
use crate::coordinator::{miner_tests::Fixture, work_ledger::WorkLedger};
use crate::ledger_test_database as ledger_database;
use crate::metrics::Metrics;
use futures_util::{future::LocalBoxFuture, FutureExt};
use sqlx::{pool::PoolConnection, postgres::PgPoolOptions, PgPool, Postgres};
use std::panic::{resume_unwind, AssertUnwindSafe};
use tokio::time::Instant as TokioInstant;

const WAIT: Duration = Duration::from_secs(10);

fn sample(metrics: &Metrics, outcome: &str, suffix: &str) -> f64 {
    let key = format!("qbit_prism_database_pool_acquire_seconds_{suffix}{{result=\"{outcome}\"}} ");
    let rendered = metrics.render();
    let values: Vec<_> = rendered
        .lines()
        .filter_map(|line| line.strip_prefix(&key))
        .collect();
    assert_eq!(values.len(), 1, "missing or duplicate series {key}");
    values[0].parse().unwrap()
}

fn counts(metrics: &Metrics) -> (f64, f64) {
    (
        sample(metrics, "success", "count"),
        sample(metrics, "failure", "count"),
    )
}

fn family(metrics: &Metrics) -> Vec<String> {
    metrics
        .render()
        .lines()
        .filter(|line| line.contains("qbit_prism_database_pool_acquire_seconds"))
        .map(str::to_owned)
        .collect()
}

struct Harness {
    fixture: Fixture,
    side: PgPool,
    plain: Ledger,
    share: AcceptedShare,
    candidate: Candidate,
}

impl Harness {
    fn coordinator(&self) -> &Coordinator {
        &self.fixture.coordinator
    }
    fn ledger(&self) -> &Ledger {
        &self.coordinator().ledger
    }
    fn metrics(&self) -> &Metrics {
        &self.coordinator().metrics
    }
    fn persist(&self, start: TokioInstant) -> impl std::future::Future<Output = SaveOutcome> + '_ {
        self.coordinator().persist_block_only(
            &self.share,
            Some(self.candidate.clone()),
            Some(1234),
            &self.candidate.block_hash,
            start,
        )
    }
}

async fn with_database<F>(case: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a mut Harness) -> LocalBoxFuture<'a, Result<()>>,
{
    let Some(raw) = qbit_prism_test_gate::database_url(qbit_prism_test_gate::site!())? else {
        return Ok(());
    };
    let database = ledger_database::FixtureDatabase::open(&raw, "coordinator_acquire_").await?;
    let mut pools = Vec::new();
    let result = AssertUnwindSafe(async {
        let mut fixture =
            Fixture::build(WAIT, |config| config.block_only_ack_timeout = WAIT, None).await;
        let metrics = fixture.coordinator.metrics.clone();
        let mut ledger = Ledger::connect_with_metrics(
            &database.url,
            "coordinator-acquire".into(),
            2,
            true,
            Some(metrics),
        )
        .await?;
        pools.push(ledger.pool.clone());
        // One slot exposes retention across enqueue, another probe or sleep.
        // Explicit pg_catalog ordering lets the clock test inject SQL waits/errors.
        let search_path = format!("SET search_path TO {}, pg_catalog", database.schema);
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(WAIT)
            .after_connect(move |connection, _| {
                let statement = search_path.clone();
                Box::pin(async move {
                    sqlx::query(&statement).execute(connection).await?;
                    Ok(())
                })
            })
            .connect(&database.url)
            .await?;
        pools.push(pool.clone());
        let old = std::mem::replace(&mut ledger.pool, pool);
        old.close().await;
        let side = PgPool::connect(&database.url).await?;
        pools.push(side.clone());
        let mut plain =
            Ledger::connect(&database.url, "coordinator-no-metrics".into(), 2, false).await?;
        // Both fixture writers use the unmeasured side pool. Complete writes
        // before taking a conflicting SQL lock, even if using different pools.
        let old = std::mem::replace(&mut plain.pool, side.clone());
        old.close().await;
        let job = fixture.job(1, 0, "checkout.worker");
        let mut proof = fixture.proof(&job, 0);
        proof.share_pass = false;
        let share = AcceptedShare {
            share_seq: 0,
            share_id: format!("checkout:{}", proof.block_hash_hex),
            miner_id: "checkout-miner".into(),
            order_key: "checkout-miner".into(),
            p2mr_program_hex: "ab".repeat(32),
            share_difficulty: 1_000_000,
            network_difficulty: 1_000_000,
            template_height: 100,
            job_id: job.wire.job_id.clone(),
            job_issued_at_ms: 100_000,
            accepted_at_ms: 0,
            ntime: proof.ntime,
            credit_policy: None,
        };
        let candidate = submission_candidate(&job, proof, share.clone()).await?;
        Arc::get_mut(&mut fixture.coordinator)
            .expect("fixture coordinator has no other owners before the test starts")
            .ledger = Arc::new(ledger);
        case(&mut Harness {
            fixture,
            side,
            plain,
            share,
            candidate,
        })
        .await
    })
    .catch_unwind()
    .await;
    for pool in pools {
        pool.close().await;
    }
    match result {
        Ok(result) => database.close(result).await,
        Err(panic) => {
            let _ = database.close(Ok(())).await;
            resume_unwind(panic)
        }
    }
}

struct ResumeClock;
impl Drop for ResumeClock {
    fn drop(&mut self) {
        tokio::time::resume();
    }
}

fn sql_code(error: &anyhow::Error) -> Option<String> {
    error
        .downcast_ref::<sqlx::Error>()
        .and_then(sqlx::Error::as_database_error)
        .and_then(|error| error.code())
        .map(|code| code.into_owned())
}

fn state_values(state: crate::ledger::ChainObservationState) -> (i64, i64, Option<String>) {
    (
        state.payout_revision,
        state.chain_epoch,
        state.best_tip_hash,
    )
}

async fn wait_counts(metrics: &Metrics, expected: (f64, f64)) -> Result<()> {
    tokio::time::timeout(WAIT, async {
        while counts(metrics) != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("checkout boundary not reached")
}

#[tokio::test]
async fn clock_and_block_only_duplicate_checkouts_are_lazy_and_cancel_once() -> Result<()> {
    with_database(|h| {
        Box::pin(async move {
            // A real ledger row makes the first block-only probe terminal.
            h.ledger().append(h.share.clone(), None).await?;
            for clock in [true, false] {
                let run = || async {
                    if clock {
                        assert!(h.ledger().now_ms().await? > 0);
                    } else {
                        assert!(matches!(
                            h.persist(TokioInstant::now()).await,
                            SaveOutcome::Duplicate
                        ));
                    }
                    anyhow::Ok(())
                };
                let held = h.ledger().pool.acquire().await?;
                let before = counts(h.metrics());
                let sum = sample(h.metrics(), "failure", "sum");
                tokio::time::pause();
                let resume = ResumeClock;
                // Construct the actual caller, particularly now_ms's boxed
                // future, so an eager observation cannot hide behind run().
                if clock {
                    let unpolled = h.ledger().now_ms();
                    tokio::time::advance(Duration::from_secs(60)).await;
                    drop(unpolled);
                } else {
                    let unpolled = h.persist(TokioInstant::now());
                    tokio::time::advance(Duration::from_secs(60)).await;
                    drop(unpolled);
                }
                assert_eq!(counts(h.metrics()), before);
                let mut acquiring = Box::pin(run());
                tokio::time::advance(Duration::from_secs(60)).await;
                assert!(futures_util::poll!(&mut acquiring).is_pending());
                assert_eq!(counts(h.metrics()), before);
                tokio::time::advance(Duration::from_millis(75)).await;
                drop(acquiring);
                drop(resume);
                assert_eq!(counts(h.metrics()), (before.0, before.1 + 1.));
                assert!((sample(h.metrics(), "failure", "sum") - sum - 0.075).abs() < 0.000001);
                drop(held);
                tokio::time::timeout(WAIT, run()).await??;
                assert_eq!(counts(h.metrics()), (before.0 + 1., before.1 + 1.));
                drop(tokio::time::timeout(WAIT, h.ledger().pool.acquire()).await??);
            }
            h.ledger().pool.close().await;
            let before = counts(h.metrics());
            let error = h.ledger().now_ms().await.unwrap_err();
            assert!(matches!(
                error.downcast_ref::<sqlx::Error>(),
                Some(sqlx::Error::PoolClosed)
            ));
            let SaveOutcome::Failed(error) = h.persist(TokioInstant::now()).await else {
                panic!("closed pool must fail before enqueue")
            };
            assert!(matches!(
                error.downcast_ref::<sqlx::Error>(),
                Some(sqlx::Error::PoolClosed)
            ));
            assert_eq!(counts(h.metrics()), (before.0, before.1 + 2.));
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn clock_sql_wait_error_and_cancel_keep_the_successful_checkout() -> Result<()> {
    with_database(|h| Box::pin(async move {
        let before_time: i64 = sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint").fetch_one(&h.side).await?;
        let measured = h.ledger().now_ms().await?;
        let after_time: i64 = sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint").fetch_one(&h.side).await?;
        assert!((before_time..=after_time).contains(&measured));
        sqlx::raw_sql("CREATE FUNCTION clock_timestamp() RETURNS timestamptz LANGUAGE plpgsql AS $$
                BEGIN
                    PERFORM pg_advisory_xact_lock(352);
                    RETURN pg_catalog.clock_timestamp();
                END;
            $$").execute(&h.side).await?;
        // The earlier real-clock statement may be prepared against pg_catalog.
        // Reparse it under the fixture's explicit search_path after injection.
        let mut connection = h.ledger().pool.acquire().await?;
        sqlx::Connection::clear_cached_statements(&mut *connection).await?;
        drop(connection);
        let mut lock = h.side.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(352)").execute(&mut *lock).await?;
        let before = counts(h.metrics());
        let mut clock = Box::pin(h.ledger().now_ms());
        tokio::select! {
            result = &mut clock => panic!("clock escaped SQL lock: {result:?}"),
            result = wait_counts(h.metrics(), (before.0 + 1., before.1)) => result?,
        }
        let acquired = family(h.metrics());
        tokio::select! {
            result = &mut clock => panic!("clock escaped SQL lock: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(75)) => {},
        }
        drop(clock);
        assert_eq!(family(h.metrics()), acquired);
        lock.rollback().await?;
        tokio::time::timeout(WAIT, h.ledger().now_ms()).await??;
        sqlx::raw_sql("CREATE OR REPLACE FUNCTION clock_timestamp() RETURNS timestamptz LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'clock test refusal' USING ERRCODE='22012';
                END;
            $$").execute(&h.side).await?;
        let before = counts(h.metrics());
        assert_eq!(sql_code(&h.ledger().now_ms().await.unwrap_err()).as_deref(), Some("22012"));
        assert_eq!(counts(h.metrics()), (before.0 + 1., before.1));
        Ok(())
    })).await
}

#[tokio::test]
async fn block_only_initial_probe_sql_error_cancel_and_original_deadline() -> Result<()> {
    with_database(|h| {
        Box::pin(async move {
            let before = family(h.metrics());
            let missing = h
                .coordinator()
                .persist_block_only(
                    &h.share,
                    None,
                    None,
                    &h.candidate.block_hash,
                    TokioInstant::now(),
                )
                .await;
            let SaveOutcome::Failed(error) = missing else {
                panic!("missing candidate must fail")
            };
            assert_eq!(error.to_string(), "missing candidate");
            assert_eq!(family(h.metrics()), before);
            let mut lock = h.side.begin().await?;
            sqlx::query("LOCK TABLE qbit_share_ledger IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *lock)
                .await?;
            let before = counts(h.metrics());
            let mut persist = Box::pin(h.persist(TokioInstant::now()));
            tokio::select! {
                result = &mut persist => panic!("probe escaped lock: {result:?}"),
                result = wait_counts(h.metrics(), (before.0 + 1., before.1)) => result?,
            }
            let acquired = family(h.metrics());
            tokio::select! {
                result = &mut persist => panic!("probe escaped lock: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(75)) => {},
            }
            drop(persist);
            assert_eq!(family(h.metrics()), acquired);
            lock.rollback().await?;
            sqlx::query("ALTER TABLE qbit_share_ledger RENAME COLUMN share_id TO hidden_share_id")
                .execute(&h.side)
                .await?;
            let before = counts(h.metrics());
            let SaveOutcome::Failed(error) = h.persist(TokioInstant::now()).await else {
                panic!("SQL error before enqueue must be definite")
            };
            assert_eq!(sql_code(&error).as_deref(), Some("42703"));
            assert_eq!(counts(h.metrics()), (before.0 + 1., before.1));
            sqlx::query("ALTER TABLE qbit_share_ledger RENAME COLUMN hidden_share_id TO share_id")
                .execute(&h.side)
                .await?;
            // Spend time in checkout, then in SQL, under the original bound.
            let held = h.ledger().pool.acquire().await?;
            let mut lock = h.side.begin().await?;
            sqlx::query("LOCK TABLE qbit_share_ledger IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *lock)
                .await?;
            let before = counts(h.metrics());
            let start = TokioInstant::now() - WAIT + Duration::from_secs(2);
            let bound = start + WAIT;
            let mut persist = Box::pin(h.persist(start));
            assert!(futures_util::poll!(&mut persist).is_pending());
            tokio::time::sleep(Duration::from_millis(75)).await;
            assert_eq!(counts(h.metrics()), before);
            drop(held);
            tokio::select! {
                result = &mut persist => panic!("SQL wait escaped: {result:?}"),
                result = wait_counts(h.metrics(), (before.0 + 1., before.1)) => result?,
            }
            let acquired = family(h.metrics());
            tokio::time::pause();
            let resume = ResumeClock;
            let remaining = bound
                .checked_duration_since(TokioInstant::now())
                .context("fixture missed original bound")?;
            tokio::time::advance(remaining - Duration::from_millis(1)).await;
            assert!(futures_util::poll!(&mut persist).is_pending());
            tokio::time::advance(Duration::from_millis(1)).await;
            let SaveOutcome::Failed(error) = persist.await else {
                panic!("SQL timeout before enqueue must be definite")
            };
            assert_eq!(
                error.to_string(),
                "block-only acknowledgement bound passed before the candidate was enqueued"
            );
            assert_eq!(family(h.metrics()), acquired);
            drop(resume);
            lock.rollback().await?;
            let held = h.ledger().pool.acquire().await?;
            let before = counts(h.metrics());
            tokio::time::pause();
            let resume = ResumeClock;
            let start = TokioInstant::now();
            tokio::time::advance(WAIT - Duration::from_millis(75)).await;
            let mut persist = Box::pin(h.persist(start));
            assert!(futures_util::poll!(&mut persist).is_pending());
            tokio::time::advance(Duration::from_millis(75)).await;
            let SaveOutcome::Failed(error) = persist.await else {
                panic!("checkout deadline before enqueue must be definite")
            };
            assert_eq!(
                error.to_string(),
                "block-only acknowledgement bound passed before the candidate was enqueued"
            );
            drop(resume);
            assert_eq!(counts(h.metrics()), (before.0, before.1 + 1.));
            drop(held);
            let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_block_candidate_outbox")
                .fetch_one(&h.side)
                .await?;
            assert_eq!(rows, 0);
            Ok(())
        })
    })
    .await
}

// Drive exactly through the initial probe, enqueue transaction and first
// disposition probe. Taking the sole slot while the caller sleeps proves it
// released at the statement boundary, and lets tests stop the next checkout.
async fn first_poll<'a>(
    h: &'a Harness,
    persist: &mut std::pin::Pin<Box<impl std::future::Future<Output = SaveOutcome> + 'a>>,
) -> Result<PoolConnection<Postgres>> {
    let before = counts(h.metrics());
    tokio::select! {
        biased;
        result = wait_counts(h.metrics(), (before.0 + 3., before.1)) => result?,
        result = &mut *persist => panic!("pending candidate answered: {result:?}"),
    }
    let connection = tokio::select! {
        biased;
        result = tokio::time::timeout(WAIT, h.ledger().pool.acquire()) => result??,
        result = &mut *persist => panic!("pending candidate answered: {result:?}"),
    };
    assert_eq!(counts(h.metrics()), (before.0 + 3., before.1));
    Ok(connection)
}

#[tokio::test]
async fn block_only_poll_checkout_cancel_and_deadline_stay_unknown() -> Result<()> {
    with_database(|h| {
        Box::pin(async move {
            let start = TokioInstant::now();
            let mut persist = Box::pin(h.persist(start));
            let held = first_poll(h, &mut persist).await?;
            let before = counts(h.metrics());
            let sum = sample(h.metrics(), "failure", "sum");
            tokio::time::pause();
            let resume = ResumeClock;
            tokio::time::advance(Duration::from_millis(50)).await;
            assert!(futures_util::poll!(&mut persist).is_pending());
            assert_eq!(counts(h.metrics()), before);
            tokio::time::advance(Duration::from_millis(75)).await;
            drop(persist);
            assert_eq!(counts(h.metrics()), (before.0, before.1 + 1.));
            assert!((sample(h.metrics(), "failure", "sum") - sum - 0.075).abs() < 0.000001);
            drop(resume);
            drop(held);
            let deferred: Value = sqlx::query_scalar(
                "SELECT share FROM qbit_prism_deferred_shares WHERE block_hash=$1",
            )
            .bind(&h.candidate.block_hash)
            .fetch_one(&h.side)
            .await?;
            assert_eq!(deferred, serde_json::to_value(&h.share)?);
            // Cancellation leaves the durable outbox, so replay is a duplicate.
            let before = counts(h.metrics());
            assert!(matches!(
                h.persist(TokioInstant::now()).await,
                SaveOutcome::Duplicate
            ));
            assert_eq!(counts(h.metrics()), (before.0 + 2., before.1));
            Ok(())
        })
    })
    .await?;
    with_database(|h| {
        Box::pin(async move {
            let start = TokioInstant::now();
            let mut persist = Box::pin(h.persist(start));
            let held = first_poll(h, &mut persist).await?;
            let before = counts(h.metrics());
            tokio::time::pause();
            let resume = ResumeClock;
            tokio::time::advance(Duration::from_millis(50)).await;
            assert!(futures_util::poll!(&mut persist).is_pending());
            tokio::time::advance((start + WAIT).saturating_duration_since(TokioInstant::now()))
                .await;
            assert!(matches!(
                persist.await,
                SaveOutcome::Unknown {
                    phase: "candidate-pending",
                    ..
                }
            ));
            assert_eq!(counts(h.metrics()), (before.0, before.1 + 1.));
            drop(resume);
            drop(held);
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn block_only_poll_terminal_results_preserve_checkout_success() -> Result<()> {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Disposition {
        Accepted,
        Abandoned,
        Orphaned,
        Rejected,
    }
    for disposition in [
        Disposition::Accepted,
        Disposition::Abandoned,
        Disposition::Orphaned,
        Disposition::Rejected,
    ] {
        eprintln!("block-only terminal disposition: {disposition:?}");
        with_database(|h| Box::pin(async move {
            let mut persist = Box::pin(h.persist(TokioInstant::now()));
            let held = first_poll(h, &mut persist).await?;
            match disposition {
                Disposition::Accepted => {
                    h.plain.append(h.share.clone(), None).await?;
                }
                Disposition::Abandoned | Disposition::Orphaned => {
                    let state = match disposition {
                        Disposition::Abandoned => "abandoned",
                        _ => "orphaned",
                    };
                    sqlx::query(
                        "UPDATE qbit_block_candidate_outbox SET
                            state=$1,completed_at=clock_timestamp(),candidate=NULL,
                            block_bytes=NULL,window_anchor_ms=NULL,
                            window_prior_balances_sha256=NULL,window_first_share_seq=NULL,
                            window_last_share_seq=NULL,window_share_count=NULL,
                            window_snapshot_sha256=NULL,
                            offer_reserved_at=CASE WHEN $1='orphaned' THEN clock_timestamp() END,
                            offer_reserved_by=CASE WHEN $1='orphaned' THEN 'test' END,
                            offer_outcome=CASE WHEN $1='orphaned' THEN 'unknown' END,
                            last_error='test disposition'",
                    ).bind(state).execute(&h.side).await?;
                }
                Disposition::Rejected => {
                    sqlx::query(
                        "UPDATE qbit_block_candidate_outbox SET
                            state='reconciliation',offer_outcome='rejected',
                            offer_reserved_at=clock_timestamp(),offer_reserved_by='test',
                            offered_at_ms=1234,offer_reply='test refusal',last_error='test refusal'",
                    ).execute(&h.side).await?;
                }
            }
            let before = counts(h.metrics());
            drop(held);
            let answer = tokio::time::timeout(WAIT, persist).await?;
            if disposition == Disposition::Accepted {
                assert!(matches!(answer, SaveOutcome::Accepted));
            } else {
                let SaveOutcome::Failed(error) = answer else {
                    panic!("terminal disposition {disposition:?}: {answer:?}")
                };
                assert_eq!(error.to_string(), "block-only proof was not accepted on the active chain");
            }
            assert_eq!(counts(h.metrics()), (before.0 + 1., before.1));
            drop(tokio::time::timeout(WAIT, h.ledger().pool.acquire()).await??);
            Ok(())
        })).await?;
    }
    Ok(())
}

#[tokio::test]
async fn block_only_poll_missing_and_sql_error_stay_unknown() -> Result<()> {
    for (setup, expected_phase) in [
        (
            "DELETE FROM qbit_prism_deferred_shares; DELETE FROM qbit_block_candidate_outbox",
            "candidate-pending",
        ),
        (
            "ALTER TABLE qbit_block_candidate_outbox RENAME COLUMN offer_outcome TO hidden_outcome",
            "poll-error",
        ),
    ] {
        eprintln!("block-only indeterminate poll: {expected_phase}");
        with_database(|h| {
            Box::pin(async move {
                let start = TokioInstant::now();
                let mut persist = Box::pin(h.persist(start));
                let held = first_poll(h, &mut persist).await?;
                sqlx::raw_sql(setup).execute(&h.side).await?;
                let before = counts(h.metrics());
                drop(held);
                tokio::select! {
                    biased;
                    result = wait_counts(h.metrics(), (before.0 + 1., before.1)) => result?,
                    answer = &mut persist => panic!("nonterminal disposition: {answer:?}"),
                }
                let acquired = family(h.metrics());
                // Drain that statement and hold the returned slot across
                // the next sleep: errors/missing rows remain indeterminate.
                let held = tokio::select! {
                    biased;
                    connection = h.ledger().pool.acquire() => connection?,
                    answer = &mut persist => panic!("nonterminal: {answer:?}"),
                };
                assert_eq!(family(h.metrics()), acquired);
                tokio::time::pause();
                let resume = ResumeClock;
                tokio::time::advance(Duration::from_millis(50)).await;
                assert!(futures_util::poll!(&mut persist).is_pending());
                tokio::time::advance((start + WAIT).saturating_duration_since(TokioInstant::now()))
                    .await;
                let answer = persist.await;
                let SaveOutcome::Unknown { phase, .. } = answer else {
                    panic!("nonterminal: {answer:?}")
                };
                assert_eq!(phase, expected_phase);
                assert_eq!(counts(h.metrics()), (before.0 + 1., before.1 + 1.));
                drop(resume);
                drop(held);
                drop(tokio::time::timeout(WAIT, h.ledger().pool.acquire()).await??);
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn block_only_poll_sql_cancel_preserves_checkout_success() -> Result<()> {
    with_database(|h| {
        Box::pin(async move {
            let mut persist = Box::pin(h.persist(TokioInstant::now()));
            let held = first_poll(h, &mut persist).await?;
            let mut lock = h.side.begin().await?;
            sqlx::query("LOCK TABLE qbit_block_candidate_outbox IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *lock)
                .await?;
            let before = counts(h.metrics());
            drop(held);
            tokio::select! {
                biased;
                result = wait_counts(h.metrics(), (before.0 + 1., before.1)) => result?,
                answer = &mut persist => panic!("nonterminal disposition: {answer:?}"),
            }
            let acquired = family(h.metrics());
            tokio::select! {
                answer = &mut persist => panic!("SQL lock escaped: {answer:?}"),
                _ = tokio::time::sleep(Duration::from_millis(75)) => {},
            }
            drop(persist);
            assert_eq!(family(h.metrics()), acquired);
            lock.rollback().await?;
            drop(tokio::time::timeout(WAIT, h.ledger().pool.acquire()).await??);
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn chain_state_checkout_preserves_snapshot_errors_and_cancellation() -> Result<()> {
    with_database(|h| {
        Box::pin(async move {
            let initial = state_values(h.ledger().chain_observation_state().await?);
            let mut change = h.side.begin().await?;
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=7,chain_epoch=11,best_tip_hash=$1",
            )
            .bind("ab".repeat(32))
            .execute(&mut *change)
            .await?;
            let before = counts(h.metrics());
            assert_eq!(
                state_values(h.ledger().chain_observation_state().await?),
                initial
            );
            assert_eq!(counts(h.metrics()), (before.0 + 1., before.1));
            change.commit().await?;
            let current = h.ledger().chain_observation_state().await?;
            assert_eq!(
                (
                    current.payout_revision,
                    current.chain_epoch,
                    current.best_tip_hash
                ),
                (7, 11, Some("ab".repeat(32)))
            );
            assert_eq!(counts(h.metrics()), (before.0 + 2., before.1));
            let held = h.ledger().pool.acquire().await?;
            let before = counts(h.metrics());
            let sum = sample(h.metrics(), "failure", "sum");
            tokio::time::pause();
            let resume = ResumeClock;
            let unpolled = h.ledger().chain_observation_state();
            tokio::time::advance(Duration::from_secs(60)).await;
            drop(unpolled);
            assert_eq!(counts(h.metrics()), before);
            let mut state = Box::pin(h.ledger().chain_observation_state());
            assert!(futures_util::poll!(&mut state).is_pending());
            tokio::time::advance(Duration::from_millis(75)).await;
            drop(state);
            drop(resume);
            assert_eq!(counts(h.metrics()), (before.0, before.1 + 1.));
            assert!((sample(h.metrics(), "failure", "sum") - sum - 0.075).abs() < 0.000001);
            drop(held);
            let mut lock = h.side.begin().await?;
            sqlx::query("LOCK TABLE qbit_prism_cluster IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *lock)
                .await?;
            let before = counts(h.metrics());
            let mut state = Box::pin(h.ledger().chain_observation_state());
            tokio::select! {
                result = &mut state => panic!("state escaped SQL lock: {result:?}"),
                result = wait_counts(h.metrics(), (before.0 + 1., before.1)) => result?,
            }
            let acquired = family(h.metrics());
            tokio::select! {
                result = &mut state => panic!("state escaped SQL lock: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(75)) => {},
            }
            drop(state);
            assert_eq!(family(h.metrics()), acquired);
            lock.rollback().await?;
            sqlx::query("ALTER TABLE qbit_prism_cluster RENAME COLUMN chain_epoch TO hidden_epoch")
                .execute(&h.side)
                .await?;
            let before = counts(h.metrics());
            assert_eq!(
                sql_code(&h.ledger().chain_observation_state().await.unwrap_err()).as_deref(),
                Some("42703")
            );
            assert_eq!(counts(h.metrics()), (before.0 + 1., before.1));
            sqlx::query("ALTER TABLE qbit_prism_cluster RENAME COLUMN hidden_epoch TO chain_epoch")
                .execute(&h.side)
                .await?;
            tokio::time::timeout(WAIT, h.ledger().chain_observation_state()).await??;
            h.ledger().pool.close().await;
            let before = counts(h.metrics());
            let error = h.ledger().chain_observation_state().await.unwrap_err();
            assert!(matches!(
                error.downcast_ref::<sqlx::Error>(),
                Some(sqlx::Error::PoolClosed)
            ));
            assert_eq!(counts(h.metrics()), (before.0, before.1 + 1.));
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn coordinator_reads_without_metrics_keep_results_and_emit_nothing() -> Result<()> {
    with_database(|h| {
        Box::pin(async move {
            h.plain.append(h.share.clone(), None).await?;
            let expected = state_values(h.ledger().chain_observation_state().await?);
            // Keep the original registry, but give the coordinator a ledger with
            // no telemetry owner. Its one-slot pool is unchanged.
            let mut plain = h.plain.clone();
            plain.pool = h.ledger().pool.clone();
            Arc::get_mut(&mut h.fixture.coordinator)
                .expect("fixture coordinator has no other owners before replacing its ledger")
                .ledger = Arc::new(plain);
            let before = family(h.metrics());
            assert!(h.ledger().now_ms().await? > 0);
            assert_eq!(
                state_values(h.ledger().chain_observation_state().await?),
                expected
            );
            assert!(matches!(
                h.persist(TokioInstant::now()).await,
                SaveOutcome::Duplicate
            ));
            assert_eq!(family(h.metrics()), before);
            Ok(())
        })
    })
    .await
}
