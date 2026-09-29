//! #529: a found block's candidate row and offer reservation reach the
//! dedicated failover standby before its one `submitblock`, within a bound;
//! share appends stay asynchronous (D3).
//!
//! The primary keeps `synchronous_standby_names=''`, so no commit ever waits
//! for the standby and nothing here changes a share acknowledgement. After the
//! reservation commits, the offering frontend reads the primary's WAL flush
//! position, which covers both the candidate enqueue and the reservation, and
//! polls `pg_stat_replication` until the named standby reports a flush
//! position at or past it. `flush_lsn`, not `sent_lsn` or `write_lsn`: only
//! WAL the standby has flushed survives its promotion. A standby that is not
//! streaming is not waited for. A standby still behind at the bound, or a
//! position that cannot be read, never holds the block: it is offered anyway,
//! and the caller logs its hash, since a failover before the standby catches
//! up loses the rows.
//!
//! The writer role must be able to read other sessions' replication positions
//! (`pg_monitor`, or `pg_read_all_stats` alone). Without that, PostgreSQL shows
//! only the walsender's pid and the wait could never confirm, so it is
//! reported as `failed` before any poll; `self-check` refuses it.
use super::Ledger;
use std::time::{Duration, Instant};

/// The poll interval: a healthy loopback standby confirms within about a
/// millisecond, and each poll is one read of a small in-memory view.
const POLL: Duration = Duration::from_millis(1);

/// `PRISM_OFFER_STANDBY_APPLICATION_NAME` and `PRISM_OFFER_STANDBY_FLUSH_WAIT_MS`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfferStandbyWait {
    /// The failover standby's `application_name` in `pg_stat_replication`.
    pub application_name: String,
    /// The most a found block's offer waits for it. Always positive: a zero
    /// bound turns the wait off.
    pub bound: Duration,
}

/// How the wait before an offer ended. The offer follows in every case.
#[derive(Debug)]
pub enum StandbyDurability {
    /// The standby flushed through the reservation's WAL.
    Confirmed { waited: Duration },
    /// The named standby is not streaming: there is nothing to wait for.
    Absent { waited: Duration },
    /// The bound passed with the standby still `lag_bytes` behind.
    Lagging { waited: Duration, lag_bytes: i64 },
    /// The positions could not be read (a database error, a role without
    /// `pg_read_all_stats`, or no answer before the bound).
    Failed { waited: Duration, error: String },
}

impl StandbyDurability {
    /// The `outcome` label of `qbit_prism_block_offer_standby_wait_total`.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Confirmed { .. } => "confirmed",
            Self::Absent { .. } => "absent",
            Self::Lagging { .. } => "lagging",
            Self::Failed { .. } => "failed",
        }
    }

    pub fn waited(&self) -> Duration {
        match self {
            Self::Confirmed { waited }
            | Self::Absent { waited }
            | Self::Lagging { waited, .. }
            | Self::Failed { waited, .. } => *waited,
        }
    }
}

/// What `self-check` reads about the configured standby (#529).
#[derive(Debug, serde::Serialize)]
pub struct OfferStandbyReport {
    pub application_name: String,
    pub bound_ms: u64,
    /// Whether the connected role can read other sessions' replication
    /// positions (`pg_read_all_stats`, which `pg_monitor` includes).
    pub role_can_read_positions: bool,
    /// Streaming `pg_stat_replication` rows with this application name.
    pub streaming: i64,
    /// Bytes the standby's flush position trails the primary's, when one
    /// streaming row was read.
    pub flush_lag_bytes: Option<i64>,
}

const READABLE: &str = "pg_has_role(current_user,'pg_read_all_stats','USAGE')";

impl Ledger {
    /// Wait, at most `wait.bound`, for the failover standby to flush every WAL
    /// record committed on the primary so far. Never fails and never holds
    /// the caller past the bound: an unconfirmed wait is reported, and the
    /// caller offers regardless.
    pub async fn await_standby_flush(&self, wait: &OfferStandbyWait) -> StandbyDurability {
        let started = Instant::now();
        let mut lag_bytes = None;
        // One deadline covers every checkout and read, the first included.
        let polled = tokio::time::timeout(wait.bound, async {
            let (target, readable): (String, bool) = sqlx::query_as(&format!(
                "SELECT pg_current_wal_flush_lsn()::text,{READABLE}"
            ))
            .fetch_one(&mut *self.acquire().await?)
            .await?;
            if !readable {
                return Ok(Err(
                    "the database role cannot read pg_stat_replication positions; grant it pg_monitor"
                        .to_owned(),
                ));
            }
            // Each poll takes a pooled connection and returns it before the
            // pause, so the wait never holds a connection share appends could
            // use.
            loop {
                let row: Option<(bool, i64)> = sqlx::query_as(
                    "SELECT flush_lsn>=$1::pg_lsn,pg_wal_lsn_diff($1::pg_lsn,flush_lsn)::bigint \
                     FROM pg_catalog.pg_stat_replication WHERE application_name=$2 AND state='streaming' \
                     AND flush_lsn IS NOT NULL ORDER BY flush_lsn DESC LIMIT 1",
                )
                .bind(&target)
                .bind(&wait.application_name)
                .fetch_optional(&mut *self.acquire().await?)
                .await?;
                match row {
                    None => return Ok::<_, sqlx::Error>(Ok(false)),
                    Some((true, _)) => return Ok(Ok(true)),
                    Some((false, lag)) => lag_bytes = Some(lag),
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await;
        let waited = started.elapsed();
        match polled {
            Ok(Ok(Ok(true))) => StandbyDurability::Confirmed { waited },
            Ok(Ok(Ok(false))) => StandbyDurability::Absent { waited },
            Ok(Ok(Err(error))) => StandbyDurability::Failed { waited, error },
            Ok(Err(error)) => StandbyDurability::Failed {
                waited,
                error: error.to_string(),
            },
            Err(_) => match lag_bytes {
                Some(lag_bytes) => StandbyDurability::Lagging { waited, lag_bytes },
                None => StandbyDurability::Failed {
                    waited,
                    error: "no replication position was read before the bound".into(),
                },
            },
        }
    }

    /// The configured standby as this ledger's connections see it.
    pub async fn offer_standby_report(
        &self,
        wait: &OfferStandbyWait,
    ) -> anyhow::Result<OfferStandbyReport> {
        let (role_can_read_positions, streaming, flush_lag_bytes): (bool, i64, Option<i64>) =
            sqlx::query_as(&format!(
                "SELECT {READABLE},count(*),CASE WHEN count(*)=1 THEN \
                 max(pg_wal_lsn_diff(pg_current_wal_flush_lsn(),flush_lsn))::bigint END \
                 FROM pg_catalog.pg_stat_replication WHERE application_name=$1 AND state='streaming'"
            ))
            .bind(&wait.application_name)
            .fetch_one(&mut *self.acquire().await?)
            .await?;
        Ok(OfferStandbyReport {
            application_name: wait.application_name.clone(),
            bound_ms: wait.bound.as_millis() as u64,
            role_can_read_positions,
            streaming,
            flush_lag_bytes,
        })
    }
}

impl OfferStandbyReport {
    /// `self-check` refuses a wait that cannot protect anything: a role that
    /// cannot read the positions, or no single streaming standby by that name.
    pub fn ensure_usable(&self) -> anyhow::Result<()> {
        let name = &self.application_name;
        anyhow::ensure!(
            self.role_can_read_positions,
            "PRISM_OFFER_STANDBY_APPLICATION_NAME={name} needs a database role that can read \
             pg_stat_replication positions: GRANT pg_monitor TO the writer role, or unset it"
        );
        anyhow::ensure!(
            self.streaming == 1,
            "PRISM_OFFER_STANDBY_APPLICATION_NAME={name} matches {} streaming standbys on this \
             primary, not exactly one: found-block offers would not wait for a failover copy",
            self.streaming
        );
        Ok(())
    }
}
