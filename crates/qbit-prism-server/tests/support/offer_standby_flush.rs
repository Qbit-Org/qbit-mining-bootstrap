//! #529 on a disposable primary and its dedicated asynchronous standby
//! (`synchronous_standby_names=''`): the found-block offer's standby wait
//! confirms against the standby's `flush_lsn`, returns at its bound while the
//! standby is behind, does not wait for a standby that stopped, and cannot
//! confirm for a writer role without `pg_monitor` or for a second standby that
//! shares the name and lags. `self-check`'s report refuses each of those. The writer is an ordinary role
//! that owns the database, as in production.
//!
//! A standby falls behind for real: its WAL receiver is suspended, so it
//! neither flushes nor reports, while its connection stays `streaming` on the
//! primary. The primary's WAL sender still sends into the socket, so
//! `sent_lsn` passes the target while `flush_lsn` does not.
use super::*;
use qbit_prism_server::ledger::{OfferStandbyWait, StandbyDurability, StandbyWait};

const STANDBY: &str = "prism_standby_1";
const WRITER: &str = "prism_writer";

/// A disposable standby of the pair's primary.
struct Standby {
    _cluster: Cluster,
    port: u16,
}

/// Dropped in field order: the standbys stop before their primary, and the
/// directory holding them all goes last.
struct Pair {
    standby: Option<Standby>,
    _primary: Cluster,
    admin: PgPool,
    writer: Ledger,
    bin: PathBuf,
    primary_port: u16,
    username: String,
    next_share: u64,
    dir: tempfile::TempDir,
}

impl Pair {
    async fn start(bin: PathBuf) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let primary = Cluster {
            bin: bin.clone(),
            data: dir.path().join("primary"),
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
        let primary_port = port()?;
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
        let mut pair = Self {
            standby: None,
            _primary: primary,
            admin,
            writer,
            bin,
            primary_port,
            username,
            next_share: 1,
            dir,
        };
        pair.standby = Some(pair.add_standby("standby", STANDBY, "").await?);
        pair.until_connected(STANDBY, 1).await?;
        Ok(pair)
    }

    /// Base-backup and start a standby under `name`, with `settings` appended
    /// to its configuration.
    async fn add_standby(&self, directory: &str, name: &str, settings: &str) -> Result<Standby> {
        let cluster = Cluster {
            bin: self.bin.clone(),
            data: self.dir.path().join(directory),
        };
        cluster.command(
            "pg_basebackup",
            &[
                "-D",
                cluster.data.to_str().context("invalid test path")?,
                "-d",
                &format!(
                    "host=127.0.0.1 port={} user={} application_name={name}",
                    self.primary_port, self.username
                ),
                "-R",
                "-X",
                "stream",
                "-c",
                "fast",
            ],
        )?;
        std::fs::write(
            cluster.data.join("postgresql.auto.conf"),
            format!(
                "{}\n{settings}\n",
                std::fs::read_to_string(cluster.data.join("postgresql.auto.conf"))?
            ),
        )?;
        let port = port()?;
        cluster.start(port, self.dir.path())?;
        Ok(Standby {
            _cluster: cluster,
            port,
        })
    }

    async fn until_connected(&self, name: &str, rows: i64) -> Result<()> {
        timeout(Duration::from_secs(30), async {
            loop {
                let streaming: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_stat_replication WHERE application_name=$1 AND state='streaming'",
                )
                .bind(name)
                .fetch_one(&self.admin)
                .await?;
                if streaming == rows {
                    return Ok::<_, anyhow::Error>(());
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .with_context(|| format!("{name} never showed {rows} streaming rows"))?
    }

    /// Commit a share on the primary: new WAL the standby has to flush.
    async fn commit(&mut self) -> Result<()> {
        self.writer.append(proof(self.next_share), None).await?;
        self.next_share += 1;
        Ok(())
    }

    async fn wait(&self, name: &str, bound: Duration) -> StandbyWait {
        self.writer
            .await_standby_flush(&OfferStandbyWait {
                application_name: name.into(),
                bound,
            })
            .await
    }

    async fn report(&self, name: &str) -> Result<qbit_prism_server::ledger::OfferStandbyReport> {
        self.writer
            .offer_standby_report(&OfferStandbyWait {
                application_name: name.into(),
                bound: Duration::from_secs(10),
            })
            .await
    }

    /// Suspend a standby's WAL receiver until the guard drops.
    async fn stall(&self, standby: &Standby) -> Result<Suspended> {
        let pool = PgPool::connect(&format!(
            "postgresql://{}@127.0.0.1:{}/postgres",
            self.username, standby.port
        ))
        .await?;
        let pid: i32 = sqlx::query_scalar("SELECT pid FROM pg_stat_wal_receiver")
            .fetch_one(&pool)
            .await?;
        pool.close().await;
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

fn refused(report: &qbit_prism_server::ledger::OfferStandbyReport) -> String {
    report
        .ensure_usable()
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default()
}

/// One pair, in the order an operator meets these states: a writer role
/// without `pg_monitor`, the grant, a healthy standby, a lagging one, a second
/// standby sharing the name, and a stopped one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offer_standby_wait_confirms_the_flush_returns_at_the_bound_and_never_waits_for_a_stopped_standby(
) -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let mut pair = Pair::start(bin.into()).await?;
    let long = Duration::from_secs(10);
    let bound = Duration::from_millis(400);

    // Without pg_read_all_stats the positions read NULL: the wait reports
    // `failed` at once, and self-check refuses it, naming the grant.
    pair.commit().await?;
    let wait = pair.wait(STANDBY, long).await;
    ensure!(
        matches!(&wait.durability, StandbyDurability::Failed { error } if error.contains("pg_monitor"))
            && wait.waited < Duration::from_secs(2),
        "a writer that cannot read replication positions must fail fast: {wait:?}"
    );
    let report = pair.report(STANDBY).await?;
    ensure!(
        !report.role_can_read_positions && refused(&report).contains("GRANT pg_monitor"),
        "self-check accepted a writer without pg_monitor: {report:?}"
    );

    sqlx::query(&format!("GRANT pg_monitor TO {WRITER}"))
        .execute(&pair.admin)
        .await?;

    // Healthy: confirmed well inside the bound, and self-check accepts.
    pair.commit().await?;
    let wait = pair.wait(STANDBY, long).await;
    ensure!(
        matches!(wait.durability, StandbyDurability::Confirmed)
            && wait.waited < Duration::from_secs(5),
        "a healthy standby did not confirm: {wait:?}"
    );
    let report = pair.report(STANDBY).await?;
    ensure!(
        report.role_can_read_positions && report.standbys == 1 && report.reporting_flush == 1,
        "{report:?}"
    );
    report.ensure_usable()?;
    // Another application name is not the failover standby.
    let other = pair.wait("prism_public_replica", long).await;
    ensure!(
        matches!(other.durability, StandbyDurability::Absent),
        "a standby under another application_name counted: {other:?}"
    );

    // Lagging: the standby flushes nothing; the wait ends at its bound, never
    // before and never much after, with the lag, and the caller offers.
    {
        let standby = pair.standby.as_ref().context("the standby")?;
        let _stalled = pair.stall(standby).await?;
        pair.commit().await?;
        let wait = pair.wait(STANDBY, bound).await;
        ensure!(
            matches!(wait.durability, StandbyDurability::Lagging { lag_bytes } if lag_bytes > 0)
                && wait.waited >= bound
                && wait.waited < bound + Duration::from_secs(2),
            "a standby that has not flushed the reservation must hold the offer for the bound, then let it go: {wait:?}"
        );
    }
    // Resumed, it catches up and confirms.
    let wait = pair.wait(STANDBY, long).await;
    ensure!(
        matches!(wait.durability, StandbyDurability::Confirmed),
        "the resumed standby did not confirm: {wait:?}"
    );

    // A second standby under the same name that falls behind: the healthy one
    // has flushed, but which of the two would be promoted is unknown, so the
    // wait must not confirm, and self-check refuses the ambiguous name.
    {
        let twin = pair.add_standby("twin", STANDBY, "").await?;
        pair.until_connected(STANDBY, 2).await?;
        let _stalled = pair.stall(&twin).await?;
        pair.commit().await?;
        let wait = pair.wait(STANDBY, bound).await;
        ensure!(
            matches!(wait.durability, StandbyDurability::Lagging { .. }) && wait.waited >= bound,
            "a lagging second standby under the name was confirmed by the healthy one: {wait:?}"
        );
        let report = pair.report(STANDBY).await?;
        ensure!(
            report.standbys == 2 && refused(&report).contains("matches 2 connected standbys"),
            "{report:?}"
        );
    }
    pair.until_connected(STANDBY, 1).await?;

    // Stopped: nothing is connected, so the offer does not wait at all, and
    // self-check refuses a wait that protects nothing.
    drop(pair.standby.take());
    pair.until_connected(STANDBY, 0).await?;
    pair.commit().await?;
    let wait = pair.wait(STANDBY, long).await;
    ensure!(
        matches!(wait.durability, StandbyDurability::Absent)
            && wait.waited < Duration::from_secs(2),
        "a stopped standby held the offer: {wait:?}"
    );
    let report = pair.report(STANDBY).await?;
    ensure!(
        report.standbys == 0 && refused(&report).contains("matches 0 connected standbys"),
        "{report:?}"
    );
    pair.writer.pool.close().await;
    pair.admin.close().await;
    Ok(())
}
