//! Real runtime and durable database fixture, with no SQL proxy or write trigger.
use anyhow::{ensure, Context, Result};
use futures_util::{
    future::{join_all, LocalBoxFuture},
    stream, FutureExt, StreamExt, TryStreamExt,
};
use qbit_prism_server::{
    coordinator::Coordinator, ledger::CompactDependency, metrics::Metrics, stratum::MiningBackend,
};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{
    collections::{BTreeMap, HashSet},
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};
use tokio::time::{timeout, timeout_at, Instant};

#[allow(dead_code)]
#[path = "../fake_qbitd.rs"]
mod fake_qbitd;
mod socket;
#[allow(dead_code)]
#[path = "../window_fixture.rs"]
mod window_fixture;

const BASELINE: &str = "1b0d409344c1b99f5f4bd04970890b9b034a9eeb";
const SHARES: u64 = 16;
const SETTLEMENT_LOCK: i64 = 0x5052_4953_4d00_0003;
// Advisory locks are database-wide, so smoke cases in this binary serialize.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub struct Fixture {
    frontends: Vec<Arc<Coordinator>>,
    listeners: Vec<socket::Listener>,
    node: fake_qbitd::FakeNode,
    direct: PgPool,
    admin: PgPool,
    schema: String,
    settings: Value,
}

impl Fixture {
    async fn open(raw: &str, count: usize) -> Result<Self> {
        ensure!((1..=2).contains(&count));
        let admin = PgPoolOptions::new().max_connections(2).connect(raw).await?;
        let row = sqlx::query("SELECT current_setting('server_version_num')::int AS version, current_setting('fsync') AS fsync, current_setting('full_page_writes') AS full_page_writes, current_setting('synchronous_commit') AS synchronous_commit")
            .fetch_one(&admin).await?;
        let version: i32 = row.try_get("version")?;
        ensure!(
            (160000..170000).contains(&version),
            "measurement requires PostgreSQL16"
        );
        for key in ["fsync", "full_page_writes", "synchronous_commit"] {
            ensure!(row.try_get::<String, _>(key)? == "on", "{key} must be on");
        }
        let settings = json!({"server_version_num":version,
            "fsync":row.try_get::<String, _>("fsync")?,
            "full_page_writes":row.try_get::<String, _>("full_page_writes")?,
            "synchronous_commit":row.try_get::<String, _>("synchronous_commit")?});
        let schema = format!("b275_measure_{}", uuid::Uuid::new_v4().simple());
        let mut url = url::Url::parse(raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"))
            .append_pair("application_name", &schema);
        // Open fallible resources before creating the schema. Once it exists,
        // both partial setup and normal completion use the same teardown.
        let direct = PgPoolOptions::new()
            .max_connections(2)
            .connect(url.as_str())
            .await?;
        let node = fake_qbitd::FakeNode::open().await?;
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut fixture = Self {
            frontends: Vec::new(),
            listeners: Vec::new(),
            node,
            direct,
            admin,
            schema,
            settings,
        };
        let opened = async {
            for index in 0..count {
                let mut config = fake_qbitd::coordinator_config(
                    url.to_string(),
                    &fixture.node,
                    &format!("b275-{index}"),
                )?;
                config.template_max_age = Duration::from_secs(600);
                config.submit_tip_max_age = Duration::from_secs(600);
                config.snapshot_interval = Duration::from_secs(600);
                config.health_timeout = Duration::from_secs(600);
                fixture
                    .frontends
                    .push(Coordinator::new(config, Arc::new(Metrics::default())).await?);
            }
            window_fixture::WindowPlan::new(SHARES)?
                .load(&fixture.direct, "b275")
                .await?;
            for frontend in &fixture.frontends {
                frontend.refresh_once().await?;
                fixture
                    .listeners
                    .push(socket::Listener::start(frontend).await?);
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        match opened {
            Ok(()) => Ok(fixture),
            Err(error) => {
                fixture
                    .close()
                    .await
                    .context(format!("setup failed: {error:#}; cleanup failed"))?;
                Err(error)
            }
        }
    }

    async fn close(mut self) -> Result<()> {
        // Try every cleanup even when an earlier listener failed to shut down.
        let mut errors = Vec::new();
        for listener in &mut self.listeners {
            if let Err(error) = listener.close().await {
                errors.push(format!("{error:#}"));
            }
        }
        drop(self.listeners);
        for frontend in &self.frontends {
            frontend.ledger.pool.close().await;
        }
        drop(self.frontends);
        self.direct.close().await;
        let cleanup = timeout(
            Duration::from_secs(15),
            sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema)).execute(&self.admin),
        )
        .await;
        if let Err(error) = cleanup
            .context("schema cleanup timeout")
            .and_then(|r| r.map_err(Into::into))
        {
            errors.push(format!("{error:#}"));
        }
        self.admin.close().await;
        ensure!(errors.is_empty(), "cleanup failed: {}", errors.join("; "));
        Ok(())
    }

    fn metrics(&self) -> Result<Vec<BTreeMap<String, f64>>> {
        self.frontends
            .iter()
            .map(|frontend| {
                frontend
                    .metrics
                    .render()
                    .lines()
                    .filter(|line| {
                        let name = line.split(['{', ' ']).next().unwrap_or("");
                        name.starts_with("qbit_prism_database_")
                            && (name.ends_with("_count") || name.ends_with("_sum"))
                    })
                    .map(|line| {
                        let (key, value) = line
                            .rsplit_once(' ')
                            .context("metric sample missing value")?;
                        Ok((key.into(), value.parse()?))
                    })
                    .collect()
            })
            .collect()
    }

    async fn settlement_waiter(&self) -> Result<i32> {
        timeout(Duration::from_secs(2), async {
            loop {
                let waiting: Option<i32> = sqlx::query_scalar("SELECT l.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE locktype='advisory' AND classid=$1::bigint::oid AND objid=$2::bigint::oid AND objsubid=1 AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND a.application_name=$3 LIMIT 1")
                    .bind(SETTLEMENT_LOCK >> 32).bind(SETTLEMENT_LOCK & 0xffff_ffff).bind(&self.schema).fetch_optional(&self.direct).await?;
                if let Some(pid) = waiting { return Ok::<_, anyhow::Error>(pid); }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.context("no settlement lock waiter observed")?
    }
}

pub async fn run(
    raw: &str,
    frontends: usize,
    body: impl for<'a> FnOnce(&'a Fixture, Instant) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let _serial = SERIAL.lock().await;
    let fixture = Fixture::open(raw, frontends).await?;
    let case_deadline = Instant::now() + Duration::from_secs(240);
    let result = AssertUnwindSafe(timeout_at(case_deadline, body(&fixture, case_deadline)))
        .catch_unwind()
        .await;
    let cleanup = fixture.close().await;
    let result = match result {
        Ok(result) => result
            .context("measurement case exceeded 240 seconds")
            .and_then(|r| r),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    match (result, cleanup) {
        (Ok(()), cleanup) => cleanup,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("cleanup also failed: {cleanup:#}")))
        }
    }
}

fn metric_delta(
    before: Vec<BTreeMap<String, f64>>,
    after: Vec<BTreeMap<String, f64>>,
) -> Result<Vec<BTreeMap<String, f64>>> {
    before
        .into_iter()
        .zip(after)
        .map(|(before, after)| {
            after
                .into_iter()
                .map(|(key, value)| {
                    let delta = value - before.get(&key).copied().unwrap_or(0.0);
                    ensure!(
                        delta >= 0.0 && delta.is_finite(),
                        "metric counter reset: {key}"
                    );
                    Ok((key, delta))
                })
                .collect()
        })
        .collect()
}

pub async fn delivery(fixture: &Fixture, sessions: usize, case_deadline: Instant) -> Result<()> {
    let old_parent = "ab".repeat(32);
    let parent = "ef".repeat(32);
    let frontends = fixture.frontends.len();
    ensure!(sessions > 0 && sessions.is_multiple_of(frontends));
    // Reserve one second for reporting and validation before the outer budget.
    let evidence_deadline = case_deadline - Duration::from_secs(1);
    let login = stream::iter(0..sessions)
        .map(|index| {
            let address = fixture.listeners[index % frontends].address;
            let old_parent = &old_parent;
            async move {
                Ok::<_, anyhow::Error>((
                    index % frontends,
                    socket::Client::login(address, index, old_parent).await?,
                ))
            }
        })
        .buffer_unordered(32)
        .try_collect::<Vec<_>>();
    let mut clients = match timeout_at(evidence_deadline, login)
        .await
        .context("case login budget elapsed")
        .and_then(|r| r)
    {
        Ok(clients) => clients,
        Err(error) => {
            println!(
                "B275_SETUP_FAILURE {}",
                json!({"frontends":frontends,"sessions_total":sessions,"stage":"login","error":format!("{error:#}"),"delivery_started":false})
            );
            return Err(error);
        }
    };
    let deliveries_before: Vec<_> = fixture
        .listeners
        .iter()
        .map(|l| l.stats.snapshot(0))
        .collect();
    let before = fixture.metrics()?;
    fixture.node.set_tip(&parent, &old_parent, 101, "02");
    // Common monotonic boundary immediately before first poll of refresh_once
    // on every frontend. No timer, poll-loop, or metrics publisher runs for us.
    let start = Instant::now();
    let deadline = (start + Duration::from_secs(120)).min(evidence_deadline);
    let refresh = join_all(fixture.frontends.iter().map(|frontend| async {
        timeout_at(deadline, frontend.refresh_once())
            .await
            .context("refresh deadline elapsed")??;
        Ok::<_, anyhow::Error>(start.elapsed().as_secs_f64())
    }));
    let receives = join_all(clients.iter_mut().map(|(frontend, client)| {
        let parent = &parent;
        async move { (*frontend, client.receive(parent, start, deadline).await) }
    }));
    let (refreshes, deliveries) = tokio::join!(refresh, receives);
    let metrics = metric_delta(before, fixture.metrics()?)?;
    let delivery_attempts: Vec<_> = fixture
        .listeners
        .iter()
        .zip(deliveries_before)
        .map(|(listener, before)| {
            let after = listener.stats.snapshot(0);
            json!({"successes":after.job_delivery_successes-before.job_delivery_successes,
            "failures":after.job_delivery_failures-before.job_delivery_failures})
        })
        .collect();
    let failure_free = delivery_attempts.iter().all(|d| d["failures"] == 0);
    let refresh_errors: Vec<_> = refreshes
        .iter()
        .filter_map(|r| r.as_ref().err().map(|e| format!("{e:#}")))
        .collect();
    let delivery_errors: Vec<_> = deliveries
        .iter()
        .filter_map(|(_, r)| r.as_ref().err().map(|e| format!("{e:#}")))
        .collect();
    let mut seconds: Vec<_> = deliveries
        .iter()
        .filter_map(|(_, r)| r.as_ref().ok().map(|(_, s)| *s))
        .collect();
    seconds.sort_by(f64::total_cmp);
    let complete = seconds.len() == sessions && refresh_errors.is_empty() && failure_free;
    let max = seconds.last().copied();
    let report = json!({
        "schema":"b275.delivery.v2", "baseline_sha":BASELINE,
        "frontends":frontends,"sessions_total":sessions,"sessions_per_frontend":sessions/frontends,
        "fixture_shares":SHARES,"database":fixture.settings,
        "runtime_threads":tokio::runtime::Handle::current().metrics().num_workers(),
        "frontend_settings":fixture.frontends.iter().zip(&fixture.listeners).map(|(f,l)| json!({"build_workers":f.config.build_workers,"database_connections":f.config.database_connections,"listener":l.settings})).collect::<Vec<_>>(),
        "delivery_attempt_deltas_per_frontend":delivery_attempts,"delivery_failure_free":failure_free,
        "boundary":"before concurrent refresh_once polling to client decoded new-parent mining.notify",
        "deadline_seconds":120,"effective_delivery_budget_seconds":deadline.saturating_duration_since(start).as_secs_f64(),"received":seconds.len(),"complete":complete,
        "refresh_return_seconds":refreshes.iter().map(|r| r.as_ref().ok().copied()).collect::<Vec<_>>(),
        "p50_delivery_seconds":seconds.get(seconds.len().saturating_sub(1)/2),
        "max_observed_delivery_seconds":max,
        "all_sessions_within_one_second":complete && max.is_some_and(|s| s <= 1.0),
        "refresh_errors":refresh_errors,"delivery_errors":delivery_errors,
        "database_metric_deltas_per_frontend":metrics,
        "metric_scope":"all instrumented frontend checkout/lock calls during refresh plus fanout; sums overlap across concurrent sessions",
        "commit_seconds":null,"commit_timing":"not exposed by baseline",
        "performance_acceptance":"not established by this harness result alone"
    });
    println!("B275_MEASUREMENT {report}");
    ensure!(complete, "incomplete delivery measurement: {report}");
    let mut ids = HashSet::new();
    for (_, received) in &deliveries {
        ensure!(
            ids.insert(received.as_ref().unwrap().0.clone()),
            "duplicate job ID"
        );
    }
    // All SQL validation is after the measurement bracket, using a separate
    // uninstrumented pool. Confirm actual durable rows, not just notifications.
    for (index, frontend) in fixture.frontends.iter().enumerate() {
        let prepared = frontend
            .prepared
            .read()
            .await
            .clone()
            .context("no prepared work")?;
        ensure!(prepared.template["previousblockhash"] == parent);
        ensure!(
            prepared
                .window
                .shares
                .context("no window range")?
                .share_count
                == SHARES
        );
        let original = frontend
            .ledger
            .compact_prepared(&prepared.storage_key)
            .await?
            .context("durable prepared record missing")?;
        ensure!(original.record.payout_revision == prepared.snapshot.payout_revision);
        ensure!(original.record.window == prepared.window);
        let hashes = original
            .record
            .audit_hashes
            .as_ref()
            .context("audit hashes absent")?;
        ensure!(
            hashes.audit_bundle_sha256.len() == 64 && hashes.coinbase_manifest_sha256.len() == 64
        );
        let jobs: Vec<_> = deliveries
            .iter()
            .filter(|(f, _)| *f == index)
            .map(|(_, r)| r.as_ref().unwrap().0.clone())
            .collect();
        let valid: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_jobs WHERE job_id=ANY($1) AND parent_hash=$2 AND payout_revision=$3 AND payload->>'prepared_key'=$4 AND expires_at>clock_timestamp()")
            .bind(&jobs).bind(&parent).bind(prepared.snapshot.payout_revision).bind(&prepared.storage_key).fetch_one(&fixture.direct).await?;
        ensure!(
            valid as usize == jobs.len(),
            "issued rows lost prepared identity or revision"
        );
        let issued: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_jobs WHERE instance_id=$1 AND parent_hash=$2 AND payload ? 'prepared_key'")
            .bind(&frontend.config.instance_id).bind(&parent).fetch_one(&fixture.direct).await?;
        println!(
            "B275_ISSUED_ROWS {}",
            json!({"frontend":index,"delivered":jobs.len(),"issued_rows":issued,"unmatched_rows":issued-valid})
        );
        ensure!(issued == valid, "committed issued rows were not delivered");
        let after = frontend
            .ledger
            .compact_prepared(&prepared.storage_key)
            .await?
            .context("prepared dependency vanished")?;
        ensure!(
            after.record == original.record,
            "readback rewrote immutable prepared bytes"
        );
    }
    println!("B275_DURABILITY frontends={frontends} sessions={sessions} verified=true");
    Ok(())
}

pub async fn landing_lock(fixture: &Fixture) -> Result<()> {
    let frontend = &fixture.frontends[0];
    let worker = frontend.authorize("b275.lock").await?;
    let job = frontend.build_job(&worker, "11223344", 1.0, 0.0).await?;
    let mut tx = fixture.direct.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SETTLEMENT_LOCK)
        .execute(&mut *tx)
        .await?;
    let before = fixture.metrics()?;
    let start = Instant::now();
    let persist = async {
        frontend
            .persist_issued_job(&worker, &job, 0, Duration::from_secs(60))
            .await?;
        Ok::<_, anyhow::Error>(start.elapsed().as_secs_f64())
    };
    let release = async {
        let observed = fixture.settlement_waiter().await;
        tokio::time::sleep_until(start + Duration::from_secs(3)).await;
        tx.rollback().await?;
        let pid = observed?;
        Ok::<_, anyhow::Error>((start.elapsed().as_secs_f64(), pid))
    };
    let (persist, released) = tokio::join!(persist, release);
    let elapsed = persist?;
    let (released, waiter_pid) = released?;
    ensure!(
        elapsed >= 3.0,
        "persistence completed before the controlled hold elapsed"
    );
    let delta = metric_delta(before, fixture.metrics()?)?;
    ensure!(
        frontend.ledger.job(&job.wire.job_id).await?.is_some(),
        "persist succeeded without durable row"
    );
    println!(
        "B275_LOCK {}",
        json!({"baseline_sha":BASELINE,"stub_hold_target_seconds":3,"release_observed_seconds":released,"persist_seconds":elapsed,"settlement_waiter_observed":true,"waiter_pid":waiter_pid,"database_metric_deltas":delta,"commit_seconds":null,"lock_free_acceptance_met":false})
    );
    Ok(())
}

/// A transient issued-row rejection must remain visible after the listener retries.
/// The constraint exists only in this regression's disposable schema, never in
/// either measurement topology. Prepared publication and old-parent login work
/// remain available so the failure occurs inside the delivery bracket.
pub async fn retry_attribution(fixture: &Fixture, deadline: Instant) -> Result<()> {
    let parent = "ef".repeat(32);
    sqlx::query(&format!(
        "ALTER TABLE qbit_prism_jobs ADD CONSTRAINT b275_reject_new_issued CHECK (parent_hash <> '{parent}' OR NOT (payload ? 'prepared_key')) NOT VALID"
    )).execute(&fixture.direct).await?;
    let release = async {
        timeout(Duration::from_secs(10), async {
            while fixture.listeners[0].stats.snapshot(0).job_delivery_failures == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("injected delivery failure was not observed")?;
        sqlx::query("ALTER TABLE qbit_prism_jobs DROP CONSTRAINT b275_reject_new_issued")
            .execute(&fixture.direct)
            .await?;
        Ok::<_, anyhow::Error>(())
    };
    let (result, released) = tokio::join!(delivery(fixture, 8, deadline), release);
    released?;
    let error = result.expect_err("retried delivery was reported as failure-free");
    let message = error.to_string();
    ensure!(
        message.contains("incomplete delivery measurement"),
        "{error:#}"
    );
    ensure!(
        message.contains("\"received\":8"),
        "clients did not recover: {error:#}"
    );
    ensure!(
        message.contains("\"delivery_failure_free\":false"),
        "failure attribution lost: {error:#}"
    );
    Ok(())
}

pub async fn revision_fence(fixture: &Fixture) -> Result<()> {
    let frontend = &fixture.frontends[0];
    let worker = frontend.authorize("b275.fence").await?;
    let job = frontend.build_job(&worker, "55667788", 1.0, 0.0).await?;
    let original = job.context.prepared.snapshot.payout_revision;
    let prepared_key = &job.context.prepared.storage_key;
    let stored = frontend
        .ledger
        .compact_prepared(prepared_key)
        .await?
        .context("prepared dependency missing")?;
    let expires_at_ms: i64 =
        sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint+60000")
            .fetch_one(&fixture.direct)
            .await?;
    let mut tx = fixture.direct.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SETTLEMENT_LOCK)
        .execute(&mut *tx)
        .await?;
    // Drive the actual compact-issued transaction under its own lock wait.
    // The public coordinator deliberately erases the underlying error cause.
    let payload = json!({"prepared_key":prepared_key,"expires_at_ms":expires_at_ms});
    let stale_save = frontend.ledger.save_issued_job_compact(
        "b275-stale",
        &payload,
        original,
        &job.wire.previousblockhash,
        expires_at_ms,
        CompactDependency {
            key: prepared_key,
            original_revision: stored.record.payout_revision,
            parent: &stored.record.parent_hash,
            original_expires_at_ms: stored.original_expires_at_ms,
            template_sha256: &stored.record.template_sha256,
            prior_balances_digest: stored.record.window.prior_balances_digest,
        },
        None,
    );
    let bump = async {
        fixture.settlement_waiter().await?;
        sqlx::query(
            "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok::<_, anyhow::Error>(())
    };
    let (saved, bumped) = tokio::join!(stale_save, bump);
    bumped?;
    let error = saved.expect_err("stale revision persisted");
    ensure!(
        error
            .to_string()
            .contains("payout revision changed while observing chain state"),
        "{error:#}"
    );
    ensure!(
        frontend
            .persist_issued_job(&worker, &job, 0, Duration::from_secs(60))
            .await
            .is_err(),
        "stale runtime job persisted"
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_jobs WHERE job_id=ANY($1)")
            .bind(vec!["b275-stale", job.wire.job_id.as_str()])
            .fetch_one(&fixture.direct)
            .await?;
    ensure!(count == 0, "stale job row exists");
    Ok(())
}
