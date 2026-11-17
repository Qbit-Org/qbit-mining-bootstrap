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
use qbit_prism::AcceptedShare;
use qbit_prism_server::ledger::{hint_timed_out, HintDeadline, Ledger};
use qbit_prism_server::metrics::Metrics;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[path = "support/ledger_execution_proxy.rs"]
mod proxy;
use proxy::{Execution, ExecutionProxy, Protocol};

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
    let result = async {
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
            ),
            "{sent:#?}"
        );
        ensure!(taken == 1., "evidence took {taken} checkouts");

        // A decrease: BEGIN with the timeout, the guard, the update, the
        // read-back and COMMIT. Five, as before (BEGIN, guard, update,
        // COMMIT, then the read-back on a second checkout).
        let (hint, sent, taken) = fixture.write(8.0, None).await?;
        ensure!(hint == Some(8.0), "{hint:?}");
        ensure!(
            sent_as(&sent, &[BEGIN, GUARD, LOWER, READ_BACK, COMMIT]),
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
            sent_as(&sent, &[BEGIN, RECENT_PROBE, PROBE, READ_BACK, COMMIT]),
            "{sent:#?}"
        );
        ensure!(taken == 1., "a missing share took {taken} checkouts");

        // A write with no time left sends nothing at all.
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
        ensure!(checkouts(&fixture.metrics)? - before == 1.);
        let kept = fixture
            .ledger
            .worker_difficulty(LISTENER, WORKER, 900)
            .await?;
        ensure!(
            kept.as_ref().map(|hint| hint.difficulty) == Some(8.0),
            "{kept:?}"
        );
        Ok(())
    }
    .await;
    let closed = fixture.close().await;
    result.and(closed)
}
