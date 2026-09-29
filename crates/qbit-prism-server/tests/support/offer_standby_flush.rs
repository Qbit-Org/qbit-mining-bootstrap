//! #529 on a disposable primary and its dedicated asynchronous standby
//! (`synchronous_standby_names=''`): the found-block offer's standby wait
//! confirms against the standby's `flush_lsn`, returns at its bound while the
//! standby is behind, does not wait for a standby that stopped, and cannot
//! confirm for a writer role without `pg_monitor`. `self-check`'s report
//! refuses the last two. The writer is an ordinary role that owns the
//! database, as in production.
//!
//! The standby falls behind for real: its WAL receiver is suspended, so it
//! neither flushes nor reports, while its connection stays `streaming` on the
//! primary. The primary's WAL sender still sends into the socket, so
//! `sent_lsn` passes the target while `flush_lsn` does not.
use super::*;
use qbit_prism_server::ledger::{OfferStandbyWait, StandbyDurability};

const STANDBY: &str = "prism_standby_1";
const WRITER: &str = "prism_writer";

/// Dropped in field order: the standby stops before its primary, and the
/// directory holding both goes last.
struct Pair {
    standby: Option<Cluster>,
    _primary: Cluster,
    admin: PgPool,
    writer: Ledger,
    standby_port: u16,
    username: String,
    next_share: u64,
    _dir: tempfile::TempDir,
}

impl Pair {
    async fn start(bin: PathBuf) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let primary = Cluster {
            bin: bin.clone(),
            data: dir.path().join("primary"),
        };
        let standby = Cluster {
            bin,
            data: dir.path().join("standby"),
        };
        primary.command(
            "initdb",
            &[
                "-D",
                primary.data.to_str().context("invalid test path")?,
                "-A",
                "trust",
                "--no-locale",
                "-E",
                "UTF8",
            ],
        )?;
        let (primary_port, standby_port) = (port()?, port()?);
        primary.start(primary_port, dir.path())?;
        let username = String::from_utf8(Command::new("id").arg("-un").output()?.stdout)?
            .trim()
            .to_owned();
        let admin = PgPool::connect(&format!(
            "postgresql://{username}@127.0.0.1:{primary_port}/postgres"
        ))
        .await?;
        let names: String = sqlx::query_scalar("SHOW synchronous_standby_names")
            .fetch_one(&admin)
            .await?;
        ensure!(names.is_empty(), "D3 is asynchronous, found {names:?}");
        sqlx::raw_sql(&format!(
            "CREATE ROLE {WRITER} LOGIN NOSUPERUSER; ALTER DATABASE postgres OWNER TO {WRITER}; \
             ALTER SCHEMA public OWNER TO {WRITER}"
        ))
        .execute(&admin)
        .await?;
        let writer = Ledger::connect(
            &format!("postgresql://{WRITER}@127.0.0.1:{primary_port}/postgres"),
            "offer-standby".into(),
            4,
            true,
        )
        .await?;
        standby.command(
            "pg_basebackup",
            &[
                "-D",
                standby.data.to_str().context("invalid test path")?,
                "-d",
                &format!(
                    "host=127.0.0.1 port={primary_port} user={username} application_name={STANDBY}"
                ),
                "-R",
                "-X",
                "stream",
                "-c",
                "fast",
            ],
        )?;
        standby.start(standby_port, dir.path())?;
        let pair = Self {
            standby: Some(standby),
            _primary: primary,
            admin,
            writer,
            standby_port,
            username,
            next_share: 1,
            _dir: dir,
        };
        pair.until_streaming(1).await?;
        Ok(pair)
    }

    async fn until_streaming(&self, rows: i64) -> Result<()> {
        timeout(Duration::from_secs(30), async {
            loop {
                let streaming: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_stat_replication WHERE application_name=$1 AND state='streaming'",
                )
                .bind(STANDBY)
                .fetch_one(&self.admin)
                .await?;
                if streaming == rows {
                    return Ok::<_, anyhow::Error>(());
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .with_context(|| format!("the standby never showed {rows} streaming rows"))?
    }

    /// Commit a share on the primary: new WAL the standby has to flush.
    async fn commit(&mut self) -> Result<()> {
        self.writer.append(proof(self.next_share), None).await?;
        self.next_share += 1;
        Ok(())
    }

    /// Suspend the standby's WAL receiver until the guard drops.
    async fn stall_standby(&self) -> Result<Suspended> {
        let standby = PgPool::connect(&format!(
            "postgresql://{}@127.0.0.1:{}/postgres",
            self.username, self.standby_port
        ))
        .await?;
        let pid: i32 = sqlx::query_scalar("SELECT pid FROM pg_stat_wal_receiver")
            .fetch_one(&standby)
            .await?;
        standby.close().await;
        Suspended::pid(pid)
    }
}

struct Suspended(i32);
impl Suspended {
    fn pid(pid: i32) -> Result<Self> {
        let status = Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status()?;
        ensure!(
            status.success(),
            "could not suspend the standby's WAL receiver"
        );
        Ok(Self(pid))
    }
}
impl Drop for Suspended {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-CONT", &self.0.to_string()])
            .status();
    }
}

fn wait(application_name: &str, bound: Duration) -> OfferStandbyWait {
    OfferStandbyWait {
        application_name: application_name.into(),
        bound,
    }
}

/// One pair, in the order an operator meets these states: a writer role
/// without `pg_monitor`, the grant, a healthy standby, a lagging one, and a
/// stopped one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offer_standby_wait_confirms_the_flush_returns_at_the_bound_and_never_waits_for_a_stopped_standby(
) -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let mut pair = Pair::start(bin.into()).await?;
    let long = wait(STANDBY, Duration::from_secs(10));

    // Without pg_read_all_stats the positions read NULL: the wait reports
    // `failed` at once, and self-check refuses it, naming the grant.
    pair.commit().await?;
    let outcome = pair.writer.await_standby_flush(&long).await;
    ensure!(
        matches!(&outcome, StandbyDurability::Failed { error, .. } if error.contains("pg_monitor"))
            && outcome.waited() < Duration::from_secs(2),
        "a writer that cannot read replication positions must fail fast: {outcome:?}"
    );
    let report = pair.writer.offer_standby_report(&long).await?;
    let refused = report.ensure_usable().unwrap_err().to_string();
    ensure!(
        !report.role_can_read_positions && refused.contains("GRANT pg_monitor"),
        "self-check accepted a writer without pg_monitor: {report:?} {refused}"
    );

    sqlx::query(&format!("GRANT pg_monitor TO {WRITER}"))
        .execute(&pair.admin)
        .await?;

    // Healthy: confirmed well inside the bound, and self-check accepts.
    pair.commit().await?;
    let outcome = pair.writer.await_standby_flush(&long).await;
    ensure!(
        matches!(outcome, StandbyDurability::Confirmed { .. })
            && outcome.waited() < Duration::from_secs(5),
        "a healthy standby did not confirm: {outcome:?}"
    );
    let report = pair.writer.offer_standby_report(&long).await?;
    ensure!(
        report.role_can_read_positions && report.streaming == 1,
        "{report:?}"
    );
    report.ensure_usable()?;
    // Another application name is not the failover standby.
    let other = pair
        .writer
        .await_standby_flush(&wait("prism_public_replica", long.bound))
        .await;
    ensure!(
        matches!(other, StandbyDurability::Absent { .. }),
        "a standby under another application_name counted: {other:?}"
    );

    // Lagging: the standby flushes nothing; the wait ends at its bound, never
    // before and never much after, with the lag, and the caller offers.
    let bound = Duration::from_millis(400);
    {
        let _stalled = pair.stall_standby().await?;
        pair.commit().await?;
        let outcome = pair.writer.await_standby_flush(&wait(STANDBY, bound)).await;
        ensure!(
            matches!(outcome, StandbyDurability::Lagging { lag_bytes, .. } if lag_bytes > 0)
                && outcome.waited() >= bound
                && outcome.waited() < bound + Duration::from_secs(2),
            "a standby that has not flushed the reservation must hold the offer for the bound, then let it go: {outcome:?}"
        );
    }
    // Resumed, it catches up and confirms.
    let outcome = pair.writer.await_standby_flush(&long).await;
    ensure!(
        matches!(outcome, StandbyDurability::Confirmed { .. }),
        "the resumed standby did not confirm: {outcome:?}"
    );

    // Stopped: nothing is streaming, so the offer does not wait at all, and
    // self-check refuses a wait that protects nothing.
    drop(pair.standby.take());
    pair.until_streaming(0).await?;
    pair.commit().await?;
    let outcome = pair.writer.await_standby_flush(&long).await;
    ensure!(
        matches!(outcome, StandbyDurability::Absent { .. })
            && outcome.waited() < Duration::from_secs(2),
        "a stopped standby held the offer: {outcome:?}"
    );
    let report = pair.writer.offer_standby_report(&long).await?;
    let refused = report.ensure_usable().unwrap_err().to_string();
    ensure!(
        report.streaming == 0 && refused.contains("matches 0 streaming standbys"),
        "{report:?} {refused}"
    );
    pair.writer.pool.close().await;
    pair.admin.close().await;
    Ok(())
}
