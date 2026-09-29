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
//! WAL the standby has flushed survives its promotion (a standby's own crash
//! can lose what it wrote but did not flush). A standby that is not connected
//! is not waited for. A standby still behind at the bound, or a
//! position that cannot be read, never holds the block: it is offered anyway,
//! and the caller logs its hash, since a failover before the standby catches
//! up loses the rows.
//!
//! The writer role must be able to read other sessions' replication positions
//! (`pg_monitor`, or `pg_read_all_stats` alone). Without that, PostgreSQL shows
//! only the walsender's pid and the wait could never confirm, so it is
//! reported as `failed` before any poll; `self-check` refuses it.
//!
//! The wait sits between the durable reservation and the `submitblock`, which
//! widens the window in which a crash or a lost claim leaves the block
//! reserved but never sent (recovery then treats it as delivery unknown and
//! never offers it) from one database round trip to at most the bound. That is
//! the price of keeping the reservation on the standby, so that a held
//! attempt still records its outcome on the promoted primary; the bound caps
//! it at 10 s and defaults to 250 ms.
use super::Ledger;
use crate::metrics::StandbyWaitOutcome;
use std::time::{Duration, Instant};

/// The poll interval: a healthy loopback standby confirms within about a
/// millisecond. Each poll is one read of a small in-memory view, so a standby
/// that stays behind costs at most `bound / POLL` reads per found block (250
/// at the default bound, 10,000 at the largest).
const POLL: Duration = Duration::from_millis(1);

/// The connected standbys under the configured name (`$1`): streaming, or
/// catching up after a reconnect, which is still worth waiting for. Every
/// query here reads the same rows.
const STANDBY_ROWS: &str = "FROM pg_catalog.pg_stat_replication \
     WHERE application_name=$1 AND state IN ('streaming','catchup')";

const READABLE: &str = "pg_has_role(current_user,'pg_read_all_stats','USAGE')";

/// `PRISM_OFFER_STANDBY_APPLICATION_NAME` and `PRISM_OFFER_STANDBY_FLUSH_WAIT_MS`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfferStandbyWait {
    /// The failover standby's `application_name` in `pg_stat_replication`.
    pub application_name: String,
    /// The most a found block's offer waits for it. Always positive: a zero
    /// bound turns the wait off.
    pub bound: Duration,
}

/// How long the wait before an offer took, and how it ended.
#[derive(Debug)]
pub struct StandbyWait {
    pub waited: Duration,
    pub durability: StandbyDurability,
}

/// How the wait before an offer ended. The offer follows in every case.
#[derive(Debug)]
pub enum StandbyDurability {
    /// Every connected standby under the name flushed through the
    /// reservation's WAL.
    Confirmed,
    /// No standby under the name is connected: there is nothing to wait for.
    Absent,
    /// The bound passed with a standby still `lag_bytes` behind.
    Lagging { lag_bytes: i64 },
    /// The positions could not be read (a database error, a role without
    /// `pg_read_all_stats`, or no position reported before the bound).
    Failed { error: String },
}

impl StandbyDurability {
    /// The `outcome` label of `qbit_prism_block_offer_standby_wait_total`.
    pub fn outcome(&self) -> StandbyWaitOutcome {
        match self {
            Self::Confirmed => StandbyWaitOutcome::Confirmed,
            Self::Absent => StandbyWaitOutcome::Absent,
            Self::Lagging { .. } => StandbyWaitOutcome::Lagging,
            Self::Failed { .. } => StandbyWaitOutcome::Failed,
        }
    }

    pub fn lag_bytes(&self) -> Option<i64> {
        match self {
            Self::Lagging { lag_bytes } => Some(*lag_bytes),
            _ => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Failed { error } => Some(error),
            _ => None,
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
    /// Connected standbys (streaming or catching up) under this name.
    pub standbys: i64,
    /// How many of them report a flush position.
    pub reporting_flush: i64,
    /// Bytes the furthest-behind standby's flush position trails the
    /// primary's, when one was reported.
    pub flush_lag_bytes: Option<i64>,
}

impl Ledger {
    /// Wait, at most `wait.bound`, for the failover standby to flush every WAL
    /// record committed on the primary so far. Never fails and never holds
    /// the caller past the bound: an unconfirmed wait is reported, and the
    /// caller offers regardless.
    ///
    /// Confirmation needs every connected standby under the name to have
    /// flushed: a second standby sharing the name (a misconfiguration
    /// `self-check` refuses) or a lingering walsender from a dropped
    /// connection can delay it to the bound, never confirm it falsely.
    pub async fn await_standby_flush(&self, wait: &OfferStandbyWait) -> StandbyWait {
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
                return Ok(StandbyDurability::Failed {
                    error: "the database role cannot read pg_stat_replication positions; \
                            grant it pg_monitor"
                        .into(),
                });
            }
            // Each poll takes a pooled connection and returns it before the
            // pause, so the wait never holds a connection share appends could
            // use.
            loop {
                let (standbys, flushed, lag): (i64, bool, Option<i64>) = sqlx::query_as(&format!(
                    "SELECT count(*),coalesce(bool_and(coalesce(flush_lsn>=$2::pg_lsn,false)),false),\
                     max(pg_wal_lsn_diff($2::pg_lsn,flush_lsn))::bigint {STANDBY_ROWS}"
                ))
                .bind(&wait.application_name)
                .bind(&target)
                .fetch_one(&mut *self.acquire().await?)
                .await?;
                if standbys == 0 {
                    return Ok::<_, sqlx::Error>(StandbyDurability::Absent);
                }
                if flushed {
                    return Ok(StandbyDurability::Confirmed);
                }
                lag_bytes = lag.or(lag_bytes);
                tokio::time::sleep(POLL).await;
            }
        })
        .await;
        let durability = match polled {
            Ok(Ok(durability)) => durability,
            Ok(Err(error)) => StandbyDurability::Failed {
                error: error.to_string(),
            },
            Err(_) => match lag_bytes {
                Some(lag_bytes) => StandbyDurability::Lagging { lag_bytes },
                None => StandbyDurability::Failed {
                    error: "no replication position was read before the bound".into(),
                },
            },
        };
        StandbyWait {
            waited: started.elapsed(),
            durability,
        }
    }

    /// The configured standby as this ledger's connections see it.
    pub async fn offer_standby_report(
        &self,
        wait: &OfferStandbyWait,
    ) -> anyhow::Result<OfferStandbyReport> {
        let (role_can_read_positions, standbys, reporting_flush, flush_lag_bytes): (
            bool,
            i64,
            i64,
            Option<i64>,
        ) = sqlx::query_as(&format!(
            "SELECT {READABLE},count(*),count(flush_lsn),\
             max(pg_wal_lsn_diff(pg_current_wal_flush_lsn(),flush_lsn))::bigint {STANDBY_ROWS}"
        ))
        .bind(&wait.application_name)
        .fetch_one(&mut *self.acquire().await?)
        .await?;
        Ok(OfferStandbyReport {
            application_name: wait.application_name.clone(),
            bound_ms: wait.bound.as_millis() as u64,
            role_can_read_positions,
            standbys,
            reporting_flush,
            flush_lag_bytes,
        })
    }
}

impl OfferStandbyReport {
    /// `self-check` refuses a wait that cannot protect anything: a role that
    /// cannot read the positions, not exactly one connected standby by that
    /// name, or a standby that has not reported a flush position yet.
    pub fn ensure_usable(&self) -> anyhow::Result<()> {
        let name = &self.application_name;
        anyhow::ensure!(
            self.role_can_read_positions,
            "PRISM_OFFER_STANDBY_APPLICATION_NAME={name} needs a database role that can read \
             pg_stat_replication positions: GRANT pg_monitor TO the writer role, or unset it"
        );
        anyhow::ensure!(
            self.standbys == 1,
            "PRISM_OFFER_STANDBY_APPLICATION_NAME={name} matches {} connected standbys on this \
             primary, not exactly one: found-block offers would not wait for one failover copy",
            self.standbys
        );
        anyhow::ensure!(
            self.reporting_flush == 1,
            "the standby {name} has not reported a flush position yet: found-block offers \
             cannot confirm until it does; run self-check again once it streams"
        );
        Ok(())
    }
}
