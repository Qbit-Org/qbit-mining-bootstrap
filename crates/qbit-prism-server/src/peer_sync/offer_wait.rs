//! CONTRACT D-19: before a found block's `submitblock`, a dual-writer node
//! waits up to `PRISM_PEER_INGEST_WAIT_MS` for the peer to hold what adopting
//! the block would need if this node died now (S8): this node's own shares
//! through the block's window, and the prepared record the block was built
//! on (whose template and balance blob the peer applies in the same
//! transaction). Both are read from the peer's own cursors over this node's
//! streams: every row of this node's at or below a cursor is on the peer.
//! The wait never holds a block: on its bound, or with the peer unreachable,
//! the block is offered anyway and the outcome counted.
use crate::config::DualWriterConfig;
use crate::ledger::peer_sync::{peer, PeerCursors};
use anyhow::{Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, PgPool};
use std::str::FromStr;
use std::time::{Duration, Instant};

/// How often the peer's cursors are read while waiting.
const POLL: Duration = Duration::from_millis(5);

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

/// The wait, with its own small pools on the peer's paths.
#[derive(Debug)]
pub struct PeerIngestWait {
    bound: Duration,
    pools: Vec<PgPool>,
}

impl PeerIngestWait {
    /// `None` when `PRISM_PEER_INGEST_WAIT_MS` is 0, or a peer URL cannot be
    /// parsed (it was validated at startup).
    pub fn new(config: &DualWriterConfig) -> Option<Self> {
        if config.peer_ingest_wait.is_zero() {
            return None;
        }
        let pools = config
            .peer_database_urls()
            .map(|url| {
                let options = PgConnectOptions::from_str(url)
                    .ok()?
                    .application_name("qbit-prism-peer-ingest-wait")
                    .options([("default_transaction_read_only", "on")])
                    .disable_statement_logging();
                Some(
                    PgPoolOptions::new()
                        .max_connections(1)
                        .min_connections(0)
                        .acquire_timeout(config.peer_ingest_wait)
                        .idle_timeout(Some(Duration::from_secs(60)))
                        .connect_lazy_with(options),
                )
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            bound: config.peer_ingest_wait,
            pools,
        })
    }

    pub fn bound(&self) -> Duration {
        self.bound
    }

    /// Wait until the peer's cursors cover `needs`, or the bound passes.
    pub async fn wait(&self, needs: AdoptionNeeds) -> (Duration, PeerIngest) {
        let started = Instant::now();
        let deadline = started + self.bound;
        let mut last_error = None;
        loop {
            match tokio::time::timeout_at(deadline.into(), self.read_cursors()).await {
                Ok(Ok(cursors)) => {
                    last_error = None;
                    if needs.met_by(&cursors) {
                        return (started.elapsed(), PeerIngest::Confirmed);
                    }
                }
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
        (started.elapsed(), outcome)
    }

    /// The peer's cursors over this node's streams, from the first path
    /// that answers.
    async fn read_cursors(&self) -> Result<PeerCursors> {
        let mut last = None;
        for pool in &self.pools {
            match pool.acquire().await {
                Ok(mut connection) => match peer::cursors(&mut connection).await {
                    Ok(cursors) => return Ok(cursors),
                    Err(error) => last = Some(error),
                },
                Err(error) => last = Some(error.into()),
            }
        }
        Err(last.context("no peer path is configured")?)
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
