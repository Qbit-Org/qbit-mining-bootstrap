//! CONTRACT D-19: before a found block's `submitblock`, a dual-writer node
//! waits up to `PRISM_PEER_INGEST_WAIT_MS` for the peer to hold what adopting
//! the block would need if this node died now (S8): this node's own shares
//! through the block's window, and the prepared record the block was built
//! on (whose template and balance blob the peer applies in the same
//! transaction). Both are read from the peer's own cursors over this node's
//! streams: every row of this node's at or below a cursor is on the peer.
//! The wait never holds a block: on its bound, or with the peer unreachable,
//! the block is offered anyway and the outcome counted. The bound covers the
//! whole wait, this node's own read of what adoption needs included.
use crate::config::DualWriterConfig;
use crate::ledger::peer_sync::{peer, PeerCursors};
use anyhow::{Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, PgPool};
use std::future::Future;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// How often the peer's cursors are read while waiting.
const POLL: Duration = Duration::from_millis(5);
/// How long a path's warm connection lives before it is replaced.
const CONNECTION_LIFETIME: Duration = Duration::from_secs(30 * 60);

/// What adopting a found block needs the peer to hold.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdoptionNeeds {
    /// This node's own shares through this `share_seq`.
    pub share_seq: Option<i64>,
    /// This node's prepared jobs through this `sync_seq`.
    pub prepared_sync_seq: Option<i64>,
}

impl AdoptionNeeds {
    fn met_by(&self, cursors: &PeerCursors) -> bool {
        let covered = |need: Option<i64>, cursor: Option<i64>| match need {
            None => true,
            Some(need) => cursor.is_some_and(|cursor| cursor >= need),
        };
        covered(self.share_seq, cursors.shares) && covered(self.prepared_sync_seq, cursors.prepared)
    }
}

/// How the wait before an offer ended; the offer follows in every case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerIngest {
    /// The peer held everything adoption needs.
    Confirmed,
    /// The bound passed before it did.
    TimedOut,
    /// The peer's database could not be read on either path.
    Unreachable(String),
}

impl PeerIngest {
    /// The `outcome` label of `qbit_prism_peer_sync_offer_waits_total`.
    pub fn outcome(&self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::TimedOut => "timed_out",
            Self::Unreachable(_) => "unreachable",
        }
    }
}

/// The wait, with one warm connection to the peer on each path. Blocks are
/// minutes apart, and a connection opened for each one would spend the bound
/// on TCP, TLS and authentication.
#[derive(Debug)]
pub struct PeerIngestWait {
    bound: Duration,
    pools: Vec<PgPool>,
    /// The path that answered last, tried first.
    preferred: AtomicUsize,
}

impl PeerIngestWait {
    /// `None` when `PRISM_PEER_INGEST_WAIT_MS` is 0. A peer URL the client
    /// cannot use is an error, never a wait silently off (the settings
    /// check refuses one first).
    pub fn new(config: &DualWriterConfig) -> Result<Option<Self>> {
        if config.peer_ingest_wait.is_zero() {
            return Ok(None);
        }
        let pools = config
            .peer_database_urls()
            .map(|url| {
                let options = PgConnectOptions::from_str(url)
                    .context("a peer database URL the PostgreSQL client cannot use")?
                    .application_name("qbit-prism-peer-ingest-wait")
                    .options(super::engine::PEER_SESSION_OPTIONS)
                    .disable_statement_logging();
                Ok(PgPoolOptions::new()
                    .max_connections(1)
                    .min_connections(1)
                    .acquire_timeout(config.peer_ingest_wait)
                    .idle_timeout(None)
                    .max_lifetime(Some(CONNECTION_LIFETIME))
                    .connect_lazy_with(options))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(Self {
            bound: config.peer_ingest_wait,
            pools,
            preferred: AtomicUsize::new(0),
        }))
    }

    pub fn bound(&self) -> Duration {
        self.bound
    }

    /// Wait until the peer's cursors cover what `needs` resolves to, or the
    /// bound passes. The bound starts before `needs`, this node's own read,
    /// so a busy local pool cannot hold the offer past it either; a read
    /// that fails ends the wait at once, unconfirmed, since nothing the peer
    /// shows could confirm what this node could not read. Returns the time
    /// waited, the outcome, and the needs when they were read.
    pub async fn wait(
        &self,
        needs: impl Future<Output = Result<AdoptionNeeds>>,
    ) -> (Duration, PeerIngest, Option<AdoptionNeeds>) {
        let started = Instant::now();
        let deadline = started + self.bound;
        let needs = match tokio::time::timeout_at(deadline.into(), needs).await {
            Ok(Ok(needs)) => needs,
            Ok(Err(error)) => {
                return (
                    started.elapsed(),
                    PeerIngest::Unreachable(format!(
                        "this node could not read what adopting the block needs: {error:#}"
                    )),
                    None,
                )
            }
            Err(_) => return (started.elapsed(), PeerIngest::TimedOut, None),
        };
        let mut last_error = None;
        loop {
            match tokio::time::timeout_at(deadline.into(), self.read_cursors(deadline)).await {
                Ok(Ok(Some(cursors))) => {
                    last_error = None;
                    if needs.met_by(&cursors) {
                        return (started.elapsed(), PeerIngest::Confirmed, Some(needs));
                    }
                }
                Ok(Ok(None)) => {}
                Ok(Err(error)) => last_error = Some(format!("{error:#}")),
                Err(_) => break,
            }
            if Instant::now() + POLL >= deadline {
                break;
            }
            tokio::time::sleep(POLL).await;
        }
        let outcome = match last_error {
            Some(error) => PeerIngest::Unreachable(error),
            None => PeerIngest::TimedOut,
        };
        (started.elapsed(), outcome, Some(needs))
    }

    /// The peer's cursors over this node's streams, from the first path that
    /// answers, starting with the one that answered last; `None` when no path
    /// answered in time and none failed. Each path gets an even share of the
    /// time left, so a path that hangs never uses up the next one's. A path
    /// that runs out of time has not failed: the wait's bound decides.
    async fn read_cursors(&self, deadline: Instant) -> Result<Option<PeerCursors>> {
        let paths = self.pools.len();
        let first = self.preferred.load(Ordering::Relaxed);
        let mut last = None;
        for attempt in 0..paths {
            let index = (first + attempt) % paths;
            let share =
                deadline.saturating_duration_since(Instant::now()) / (paths - attempt) as u32;
            let read = async {
                let mut connection = self.pools[index].acquire().await?;
                peer::cursors(&mut connection).await
            };
            match tokio::time::timeout(share, read).await {
                Ok(Ok(cursors)) => {
                    self.preferred.store(index, Ordering::Relaxed);
                    return Ok(Some(cursors));
                }
                Ok(Err(error)) => last = Some(error),
                Err(_) => {}
            }
        }
        last.map_or(Ok(None), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn needs_are_met_only_by_cursors_at_or_past_them() {
        let cursors = PeerCursors {
            shares: Some(10),
            blocks: None,
            prepared: Some(5),
        };
        assert!(AdoptionNeeds::default().met_by(&PeerCursors::default()));
        assert!(AdoptionNeeds {
            share_seq: Some(10),
            prepared_sync_seq: Some(5)
        }
        .met_by(&cursors));
        assert!(!AdoptionNeeds {
            share_seq: Some(11),
            prepared_sync_seq: None
        }
        .met_by(&cursors));
        assert!(!AdoptionNeeds {
            share_seq: None,
            prepared_sync_seq: Some(6)
        }
        .met_by(&cursors));
        assert!(!AdoptionNeeds {
            share_seq: Some(1),
            prepared_sync_seq: None
        }
        .met_by(&PeerCursors::default()));
        assert_eq!(PeerIngest::TimedOut.outcome(), "timed_out");
    }
}
