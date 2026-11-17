//! #872: a difficulty-hint write is one transaction on one checkout, and
//! what it asks the server to run is counted on the wire, not estimated.
//!
//! The ledger's pool talks to PostgreSQL through
//! `support/ledger_execution_proxy.rs`, which records every `Execute` and
//! `Query` frame (one round trip each) with the connection that sent it. The
//! pool's checkouts are read from the ledger's own
//! `qbit_prism_database_pool_acquire_seconds` counter. A write whose
//! statements all finish within the margin never sets its timeout again, so
//! the counts below are the write's whole cost; the pool's release and
//! checkout pings are `Sync` frames, not statements, and are not counted.
use anyhow::{ensure, Context, Result};
use futures_util::FutureExt;
use qbit_prism::AcceptedShare;
use qbit_prism_server::ledger::{hint_timed_out, HintDeadline, Ledger};
use qbit_prism_server::metrics::Metrics;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::panic::{resume_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;

#[path = "support/ledger_execution_proxy.rs"]
mod proxy;
use proxy::{Execution, ExecutionProxy, Protocol};

#[path = "support/live_host_tools.rs"]
#[allow(dead_code)]
mod host_tools;

#[path = "support/live_private_postgres.rs"]
#[allow(dead_code)]
mod private_postgres;
use private_postgres::{ClusterOptions, PrivateCluster};

const LISTENER: &str = "primary";
const WORKER: &str = "worker";

fn share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("{WORKER}:{id:064x}"),
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

/// Successful checkouts the ledger's pool has recorded.
fn checkouts(metrics: &Metrics) -> Result<f64> {
    let key = "qbit_prism_database_pool_acquire_seconds_count{result=\"success\"} ";
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .context("no checkout counter")?
        .parse()
        .map_err(Into::into)
}

/// The statements of one write, as `(protocol, text)`, all on one
/// connection.
fn statements(executions: &[Execution]) -> Result<Vec<(Protocol, String)>> {
    let connection = executions.first().map(|first| first.connection);
    ensure!(
        executions
            .iter()
            .all(|execution| Some(execution.connection) == connection),
        "one write used more than one connection: {executions:#?}"
    );
    Ok(executions
        .iter()
        .map(|execution| (execution.protocol, execution.sql.clone()))
        .collect())
}

/// Whether `sent` opens its transaction with the hint's settings and writes
/// nothing but the hint row: every statement but the `BEGIN`, the write and
/// `COMMIT` is a `SELECT`, and the write is the hint row's (#872: the hint
/// commits at `synchronous_commit = local`).
fn writes_only_the_hint_row(sent: &[(Protocol, String)]) -> bool {
    sent.first()
        .is_some_and(|(_, begin)| begin.ends_with("; SET LOCAL synchronous_commit = local"))
        && sent[1..].iter().all(|(_, sql)| {
            sql.starts_with("SELECT ")
                || sql == "COMMIT"
                || sql.starts_with("INSERT INTO qbit_worker_difficulty(")
                || sql.starts_with("UPDATE qbit_worker_difficulty SET ")
        })
}

/// Whether `sent` is `expected`, statement by statement: the same protocol,
/// and text that starts with the expected text (all of it when it ends with
/// `$`).
fn sent_as(sent: &[(Protocol, String)], expected: &[(Protocol, &str)]) -> bool {
    sent.len() == expected.len()
        && sent
            .iter()
            .zip(expected)
            .all(|((protocol, sql), (want, text))| {
                protocol == want
                    && match text.strip_suffix('$') {
                        Some(whole) => sql == whole,
                        None => sql.starts_with(text),
                    }
            })
}

struct Fixture {
    admin: PgPool,
    schema: String,
    proxy: ExecutionProxy,
    ledger: Ledger,
    metrics: Arc<Metrics>,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_hint_trips_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let opened = async {
            let mut url = url::Url::parse(&raw)?;
            url.query_pairs_mut()
                .append_pair("options", &format!("-csearch_path={schema}"));
            let upstream: SocketAddr = tokio::net::lookup_host((
                url.host_str().context("database host")?,
                url.port().unwrap_or(5432),
            ))
            .await?
            .next()
            .context("database address")?;
            let proxy = ExecutionProxy::start(upstream).await?;
            let metrics = Arc::new(Metrics::default());
            let ledger = Ledger::connect_with_metrics(
                &proxy.rewrite_url(url.as_str())?,
                "hint-trips".into(),
                2,
                true,
                Some(metrics.clone()),
            )
            .await?;
            Ok::<_, anyhow::Error>((proxy, ledger, metrics))
        }
        .await;
        match opened {
            Ok((proxy, ledger, metrics)) => Ok(Some(Self {
                admin,
                schema,
                proxy,
                ledger,
                metrics,
            })),
            Err(error) => {
                let _ = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
                    .execute(&admin)
                    .await;
                admin.close().await;
                Err(error)
            }
        }
    }

    async fn close(self) -> Result<()> {
        self.ledger.pool.close().await;
        let finished = self.proxy.finish().await;
        let dropped = sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await;
        self.admin.close().await;
        finished?;
        dropped?;
        Ok(())
    }

    /// One write with a generous bound, and what it cost: its statements
    /// and its checkouts.
    #[allow(clippy::type_complexity)]
    async fn write(
        &self,
        difficulty: f64,
        share_id: Option<&str>,
    ) -> Result<(Option<f64>, Vec<(Protocol, String)>, f64)> {
        let before = checkouts(&self.metrics)?;
        let mark = self.proxy.mark();
        let hint = self
            .ledger
            .write_worker_difficulty_hint(
                LISTENER,
                WORKER,
                difficulty,
                share_id,
                900,
                HintDeadline::after(Duration::from_secs(30)),
            )
            .await?;
        let executions = self.proxy.executions_since(mark)?;
        Ok((
            hint.map(|hint| hint.difficulty),
            statements(&executions)?,
            checkouts(&self.metrics)? - before,
        ))
    }
}

const BEGIN: (Protocol, &str) = (Protocol::Simple, "BEGIN; SET LOCAL statement_timeout = ");
const GUARD: (Protocol, &str) = (
    Protocol::Extended,
    "SELECT c.fatal_error,EXISTS(SELECT 1 FROM qbit_ledger_writer_lease",
);
const RECENT_PROBE: (Protocol, &str) = (Protocol::Extended, "SELECT floor(extract(epoch FROM accepted_at)*1000)::bigint FROM qbit_share_ledger WHERE share_id=$1 AND accepted AND share_seq>=qbit_prism_share_probe_floor()$");
const PROBE: (Protocol, &str) = (Protocol::Extended, "SELECT floor(extract(epoch FROM accepted_at)*1000)::bigint FROM qbit_share_ledger WHERE share_id=$1 AND accepted$");
const RECORD: (Protocol, &str) = (Protocol::Extended, "INSERT INTO qbit_worker_difficulty(");
const LOWER: (Protocol, &str) = (
    Protocol::Extended,
    "UPDATE qbit_worker_difficulty SET difficulty=",
);
const READ_BACK: (Protocol, &str) = (Protocol::Extended, "SELECT difficulty::text AS difficulty,");
const COMMIT: (Protocol, &str) = (Protocol::Simple, "COMMIT$");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hint_write_is_one_checkout_and_its_statements_are_counted_on_the_wire() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = AssertUnwindSafe(async {
        // Evidence of a share the ledger holds: BEGIN with the timeout, the
        // recent-partition probe, the write guard, the record, the read-back
        // and COMMIT. Six round trips on one checkout, as before #872 (the
        // probe, BEGIN, guard, record and COMMIT, then the read-back), which
        // took three checkouts.
        let accepted = share(1);
        fixture.ledger.append(accepted.clone(), None).await?;
        let (hint, sent, taken) = fixture.write(32.0, Some(&accepted.share_id)).await?;
        ensure!(hint == Some(32.0), "{hint:?}");
        ensure!(
            sent_as(
                &sent,
                &[BEGIN, RECENT_PROBE, GUARD, RECORD, READ_BACK, COMMIT]
            ) && writes_only_the_hint_row(&sent),
            "{sent:#?}"
        );
        ensure!(taken == 1., "evidence took {taken} checkouts");

        // A decrease: BEGIN with the timeout, the guard, the update, the
        // read-back and COMMIT. Five, as before (BEGIN, guard, update,
        // COMMIT, then the read-back on a second checkout).
        let (hint, sent, taken) = fixture.write(8.0, None).await?;
        ensure!(hint == Some(8.0), "{hint:?}");
        ensure!(
            sent_as(&sent, &[BEGIN, GUARD, LOWER, READ_BACK, COMMIT])
                && writes_only_the_hint_row(&sent),
            "{sent:#?}"
        );
        ensure!(taken == 1., "a decrease took {taken} checkouts");

        // Evidence of a share the ledger does not hold: both probes, no
        // guard and no write. Five, two more than before (the two probes
        // and the read-back ran outside a transaction), on one checkout
        // instead of two.
        let absent = share(2);
        let (hint, sent, taken) = fixture.write(64.0, Some(&absent.share_id)).await?;
        ensure!(hint == Some(8.0), "{hint:?}");
        ensure!(
            sent_as(&sent, &[BEGIN, RECENT_PROBE, PROBE, READ_BACK, COMMIT])
                && writes_only_the_hint_row(&sent),
            "{sent:#?}"
        );
        ensure!(taken == 1., "a missing share took {taken} checkouts");

        // A write with no time left takes no connection and sends nothing.
        let before = checkouts(&fixture.metrics)?;
        let mark = fixture.proxy.mark();
        let spent = HintDeadline::after(Duration::from_millis(1));
        tokio::time::sleep(Duration::from_millis(2)).await;
        let error = fixture
            .ledger
            .write_worker_difficulty_hint(LISTENER, WORKER, 4.0, None, 900, spent)
            .await
            .err()
            .context("a spent write ran")?;
        ensure!(hint_timed_out(&error), "{error:#}");
        let sent = fixture.proxy.executions_since(mark)?;
        ensure!(sent.is_empty(), "a spent write sent {sent:#?}");
        ensure!(checkouts(&fixture.metrics)? - before == 0.);
        let kept = fixture
            .ledger
            .worker_difficulty(LISTENER, WORKER, 900)
            .await?;
        ensure!(
            kept.as_ref().map(|hint| hint.difficulty) == Some(8.0),
            "{kept:?}"
        );
        Ok(())
    })
    .catch_unwind()
    .await;
    // The proxy, pools and schema are cleaned up whether the body fails or
    // panics.
    let closed = fixture.close().await;
    match result {
        Err(panic) => resume_unwind(panic),
        Ok(result) => result.and(closed),
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// Whether a commit at the session's own level, on `probe`, waits for a
/// synchronous standby now: it has not returned within 300 ms. The waiting
/// commit is left behind until the standby list is reset.
async fn commits_wait_for_the_standby(admin: &PgPool) -> Result<bool> {
    let mut probe = admin.acquire().await?;
    let insert = sqlx::query("INSERT INTO prism_sync_probe VALUES (1)").execute(&mut *probe);
    Ok(tokio::time::timeout(Duration::from_millis(300), insert)
        .await
        .is_err())
}

// PostgreSQL turns the statement timer off before it commits (#529), so no
// `statement_timeout` ends a commit's wait for a synchronous standby. A hint
// commits at `synchronous_commit = local` for that reason (#872). On a
// disposable cluster whose `synchronous_standby_names` names a standby that
// never connects, every commit at the session's level waits; the hint still
// commits within its bound and its row is there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hint_commits_without_waiting_for_a_synchronous_standby() -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let home = tempfile::Builder::new().prefix("h872-").tempdir()?;
    let cluster = PrivateCluster::start(ClusterOptions {
        bin: std::path::PathBuf::from(bin),
        data: home.path().join("data"),
        home: home.path().to_path_buf(),
        initdb: &[],
        settings: "",
        env: Vec::new(),
    })?;
    let url = cluster.url();
    let admin = PgPool::connect(&url).await?;
    let ledger = Ledger::connect(&url, "hint-sync".into(), 2, true).await?;
    let result = AssertUnwindSafe(async {
        ledger
            .record_worker_difficulty(LISTENER, WORKER, 64.0, now_ms())
            .await?;
        sqlx::query("CREATE TABLE prism_sync_probe(x int)")
            .execute(&admin)
            .await?;
        sqlx::query("ALTER SYSTEM SET synchronous_standby_names = 'prism_absent_standby'")
            .execute(&admin)
            .await?;
        sqlx::query("SELECT pg_reload_conf()")
            .execute(&admin)
            .await?;
        // The reload reaches the backends and the standby bookkeeping in
        // its own time: wait until a commit at the session's level waits.
        let mut waiting = false;
        for _ in 0..20 {
            if commits_wait_for_the_standby(&admin).await? {
                waiting = true;
                break;
            }
        }
        ensure!(waiting, "commits never waited for the absent standby");
        let started = tokio::time::Instant::now();
        let bound = Duration::from_secs(2);
        let written = tokio::time::timeout(
            Duration::from_secs(10),
            ledger.write_worker_difficulty_hint(
                LISTENER,
                WORKER,
                32.0,
                None,
                900,
                HintDeadline::after(bound),
            ),
        )
        .await
        .context("the hint's commit waited for the standby")??;
        let took = started.elapsed();
        ensure!(
            written.as_ref().map(|hint| hint.difficulty) == Some(32.0),
            "{written:?}"
        );
        ensure!(took < bound, "the hint took {took:?}");
        let row: String = sqlx::query_scalar(
            "SELECT difficulty::text FROM qbit_worker_difficulty WHERE listener=$1 AND worker_username=$2",
        )
        .bind(LISTENER)
        .bind(WORKER)
        .fetch_one(&admin)
        .await?;
        ensure!(row.parse::<f64>()? == 32.0, "the row holds {row}");
        Ok(())
    })
    .catch_unwind()
    .await;
    // Release every waiting commit before anything closes.
    let reset = async {
        sqlx::query("ALTER SYSTEM RESET synchronous_standby_names")
            .execute(&admin)
            .await?;
        sqlx::query("SELECT pg_reload_conf()")
            .execute(&admin)
            .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    ledger.pool.close().await;
    admin.close().await;
    drop(cluster);
    match result {
        Err(panic) => resume_unwind(panic),
        Ok(result) => result.and(reset),
    }
}
