//! #752: wake this frontend's refresh when another one moves the payout
//! revision under the work it publishes.
//!
//! Each pool block moves the payout revision twice on the new tip: the first
//! frontend to record the tip bumps it, and the landing bumps it again when it
//! confirms the block. A share on work at a superseded revision is refused
//! `stale-job` (`miner_submit.rs`, `StaleJobCause::PayoutRevision`), so until
//! a frontend republishes, every share on its new-tip work is refused. The
//! landing frontend wakes its own refresh; every other one used to wait for
//! its blockpoll tick, up to `PRISM_BLOCKPOLL_SECONDS`.
//!
//! Every same-tip bump (a block's confirmation, abandonment, orphan
//! settlement, reorg reconciliation and maturity in `ledger/blocks.rs`, and a
//! carry claim or transfer through `ledger::bump_payout_revision`) announces
//! itself with [`ANNOUNCE`] in its own transaction. NOTIFY is transactional:
//! listeners hear a bump once it has committed, and never one that rolled
//! back. The tip's own bump (`ledger/window.rs`) is not announced: each
//! frontend learns the tip from its own node, and a wake would only send one
//! whose node is behind to refresh against the older tip.
//!
//! Each frontend's refresh loop runs [`listen`] beside it, on one dedicated
//! connection outside the ledger pool and its sizing, and wakes the refresh
//! on every announcement. A wake is only a hint: the refresh reads the
//! durable revision itself, so a late, repeated or foreign announcement can
//! never republish older work. Announcements fold into the refresh's single
//! wake permit, and wakes are at least [`WAKE_SPACING`] apart, so a burst
//! costs one refresh.
//!
//! The listener never holds the refresh up. While it is lost the refresh
//! keeps its tick, as before #752, and the listener reconnects with backoff;
//! a reconnect wakes the refresh once, for an announcement it may have
//! missed. A probe every [`PROBE_INTERVAL`] finds a connection that died
//! without closing.
//!
//! In dual-writer mode (3.1) each node's frontends listen on their own
//! database. The peer sync never copies the payout revision: a peer's block
//! counts here only once this node's reconciler confirms it, and that
//! confirmation's bump is announced on this node's database.
use super::refresh_window::AlertEveryMinute;
use anyhow::{anyhow, Result};
use sqlx::postgres::{PgConnectOptions, PgListener, PgPoolOptions};
use std::{str::FromStr, time::Duration};
use tokio::{
    sync::{watch, Notify},
    time::{sleep, sleep_until, timeout, Instant},
};

/// The channel every same-tip payout revision bump notifies.
pub const CHANNEL: &str = "qbit_prism_payout_revision";
/// The `RETURNING` item of a same-tip bump of `qbit_prism_cluster`: it
/// announces the new revision on [`CHANNEL`]. The payload is for logs only.
pub(crate) const ANNOUNCE: &str = "pg_notify('qbit_prism_payout_revision',payout_revision::text)";
/// The listener's `application_name`, which names its session in
/// `pg_stat_activity`.
pub const APPLICATION_NAME: &str = "qbit-prism-revision-wake";
/// One connection attempt, its LISTEN included.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long an idle connection goes before a probe checks it.
const PROBE_INTERVAL: Duration = Duration::from_secs(30);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// The first wait before a reconnect, doubled after each attempt up to
/// [`RETRY_LONGEST`]. It starts over, and so does the once-a-minute warning,
/// only once a connection has lasted a probe interval, so a connection
/// dropped as soon as it is made can neither wake the refresh more often
/// than that nor warn more than once a minute.
const RETRY_FIRST: Duration = Duration::from_millis(250);
pub const RETRY_LONGEST: Duration = Duration::from_secs(5);
/// The shortest time between two wakes. The first announcement after a
/// quiet spell wakes at once; any within this of a wake fold into one wake
/// at its end.
const WAKE_SPACING: Duration = Duration::from_millis(250);

/// Wake `wake` on every announcement on [`CHANNEL`] at `database_url`, and
/// once after every reconnect, until `shutdown` changes. Never returns
/// before that: a failure only delays the next attempt.
pub(super) async fn listen(database_url: &str, wake: &Notify, mut shutdown: watch::Receiver<bool>) {
    let mut retry = RETRY_FIRST;
    let mut connected = false;
    let mut wakes = Wakes::default();
    let mut alert = AlertEveryMinute::default();
    let mut reported = false;
    loop {
        let subscribed = tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            subscribed = timeout(CONNECT_TIMEOUT, subscribe(database_url)) => subscribed
                .unwrap_or_else(|_| Err(anyhow!("connecting timed out after {CONNECT_TIMEOUT:?}"))),
        };
        let error = match subscribed {
            Ok(listener) => {
                // The first connection starts with the refresh loop, whose
                // first refresh runs at once. A later one may have missed
                // announcements while it was down.
                if connected {
                    wake.notify_one();
                    wakes.woke(Instant::now());
                }
                if std::mem::take(&mut reported) {
                    tracing::info!(
                        "payout-revision listener reconnected; refresh woken once (#752)"
                    );
                }
                connected = true;
                let started = Instant::now();
                let error = match serve(listener, wake, &mut wakes, &mut shutdown).await {
                    Some(error) => error,
                    None => return,
                };
                if started.elapsed() >= PROBE_INTERVAL {
                    retry = RETRY_FIRST;
                    alert.clear();
                }
                error
            }
            Err(error) => error,
        };
        if alert.due(std::time::Instant::now()) {
            reported = true;
            tracing::warn!(
                %error,
                retry_in = ?retry,
                "payout-revision listener unavailable: this frontend republishes after another frontend's block on its blockpoll tick until it reconnects (#752)"
            );
        } else {
            tracing::debug!(%error, retry_in = ?retry, "payout-revision listener unavailable");
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            _ = sleep(retry) => {}
        }
        retry = (retry * 2).min(RETRY_LONGEST);
    }
}

/// A listener on its own one-connection pool, never shared with the ledger.
async fn subscribe(database_url: &str) -> Result<PgListener> {
    let options = PgConnectOptions::from_str(database_url)?.application_name(APPLICATION_NAME);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(CONNECT_TIMEOUT)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_lazy_with(options);
    let mut listener = PgListener::connect_with(&pool).await?;
    // A lost connection ends `serve`; `listen` reconnects, so it knows to
    // wake the refresh.
    listener.eager_reconnect(false);
    listener.listen(CHANNEL).await?;
    Ok(listener)
}

/// When to wake the refresh for the announcements heard.
#[derive(Debug, Default)]
struct Wakes {
    last: Option<Instant>,
    due: Option<Instant>,
}

impl Wakes {
    /// An announcement heard at `now`: due at once after a quiet spell, or
    /// when [`WAKE_SPACING`] since the last wake ends. One wake covers every
    /// announcement heard before it is due.
    fn heard(&mut self, now: Instant) {
        let earliest = self.last.map_or(now, |last| (last + WAKE_SPACING).max(now));
        self.due.get_or_insert(earliest);
    }

    /// The refresh was woken at `now`.
    fn woke(&mut self, now: Instant) {
        self.last = Some(now);
        self.due = None;
    }
}

/// Wake on announcements until the connection is lost, returning why, or
/// until `shutdown` changes, returning `None`.
async fn serve(
    mut listener: PgListener,
    wake: &Notify,
    wakes: &mut Wakes,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<anyhow::Error> {
    let mut probe_at = Instant::now() + PROBE_INTERVAL;
    let error = loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => return None,
            _ = sleep_until(wakes.due.unwrap_or_else(Instant::now)), if wakes.due.is_some() => {
                wake.notify_one();
                wakes.woke(Instant::now());
            }
            received = listener.try_recv() => match received {
                Ok(Some(notification)) => {
                    tracing::debug!(revision = notification.payload(), "payout revision announced");
                    let now = Instant::now();
                    wakes.heard(now);
                    probe_at = now + PROBE_INTERVAL;
                }
                Ok(None) => break anyhow!("the listener's connection was closed"),
                Err(error) => break error.into(),
            },
            _ = sleep_until(probe_at) => {
                match timeout(PROBE_TIMEOUT, sqlx::query("SELECT 1").execute(&mut listener)).await {
                    Ok(Ok(_)) => probe_at = Instant::now() + PROBE_INTERVAL,
                    Ok(Err(error)) => break error.into(),
                    Err(_) => break anyhow!("the listener's probe timed out after {PROBE_TIMEOUT:?}"),
                }
            }
        }
    };
    // An announcement heard before the loss still wakes, now.
    if wakes.due.is_some() {
        wake.notify_one();
        wakes.woke(Instant::now());
    }
    Some(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn announcements_fold_into_spaced_wakes() {
        let start = Instant::now();
        let at = |millis| start + Duration::from_millis(millis);
        let mut wakes = Wakes::default();
        // The first after a quiet spell is due at once, and a burst before
        // it is served adds nothing.
        for millis in 0..10 {
            wakes.heard(at(millis));
        }
        assert_eq!(wakes.due, Some(at(0)));
        wakes.woke(at(10));
        // A hundred within the spacing are one wake, at its end.
        for millis in 20..120 {
            wakes.heard(at(millis));
        }
        assert_eq!(wakes.due, Some(at(10) + WAKE_SPACING));
        wakes.woke(at(260));
        assert_eq!(wakes.due, None);
        // After a quiet spell the next one is due at once again.
        wakes.heard(at(900));
        assert_eq!(wakes.due, Some(at(900)));
    }
}
