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
//! settlement, reorg reconciliation and maturity in `ledger/blocks.rs`, a
//! carry gate change, release or transfer through
//! `ledger::bump_payout_revision`, and `fatal-state clear`) announces itself
//! with [`ANNOUNCE`] in its own transaction. NOTIFY is transactional:
//! listeners hear a bump once it has committed, and never one that rolled
//! back. Two bumps are not announced. The tip's own (`ledger/window.rs`):
//! each frontend learns the tip from its own node, and a wake would only send
//! one whose node is behind to refresh against the older tip. And a policy
//! transition's (`ledger/policy_transition.rs`), which requires every
//! frontend stopped, so no listener is there to hear it.
//!
//! Each frontend's refresh loop [`start`]s [`listen`] beside it, on one
//! dedicated connection outside the ledger pool and its sizing, and the
//! listener wakes the refresh on every announcement. A wake is only a hint: the refresh reads the
//! durable revision itself, so a late, repeated or foreign announcement can
//! never republish older work. Announcements fold into the refresh's single
//! wake permit, and wakes are at least [`WAKE_SPACING`] apart, so a burst
//! costs one refresh.
//!
//! The listener never holds the refresh up. While it is lost the refresh
//! keeps its tick, as before #752, and the listener reconnects with backoff.
//! Every LISTEN wakes the refresh once, for an announcement it may have
//! missed, unless it was in place before the loop's first refresh read the
//! revision: the loop waits up to [`STARTUP_WAIT`] for the listener's first
//! attempt, so normally the first refresh already runs under LISTEN. A probe
//! every [`PROBE_INTERVAL`] finds a connection that died without closing.
//!
//! In dual-writer mode (3.1) each node's frontends listen on their own
//! database. The peer sync never copies the payout revision: a peer's block
//! counts here only once this node's reconciler confirms it, and that
//! confirmation's bump is announced on this node's database.
use super::{refresh_window::AlertEveryMinute, Coordinator};
use anyhow::{anyhow, Result};
use sqlx::postgres::{PgConnectOptions, PgListener, PgPoolOptions};
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{watch, Notify},
    time::{sleep, sleep_until, timeout, Instant},
};
use tokio_util::task::AbortOnDropHandle;

/// The channel every same-tip payout revision bump notifies.
pub const CHANNEL: &str = "qbit_prism_payout_revision";
/// The `RETURNING` item of a same-tip bump of `qbit_prism_cluster`: it
/// announces the new revision on [`CHANNEL`]. The payload is for logs only.
pub const ANNOUNCE: &str = "pg_notify('qbit_prism_payout_revision',payout_revision::text)";
/// The listener's `application_name`, which names its session in
/// `pg_stat_activity`.
pub const APPLICATION_NAME: &str = "qbit-prism-revision-wake";
/// One connection attempt, its LISTEN included.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the refresh loop waits for the listener's first attempt before
/// its first refresh. A database that answers takes milliseconds; one that
/// does not is the tick's, as before #752, and the listener wakes the
/// refresh once it connects.
const STARTUP_WAIT: Duration = Duration::from_secs(1);
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

/// What the refresh loop and its listener share.
#[derive(Default)]
struct Startup {
    /// Set before the refresh loop's first refresh reads the revision.
    refreshing: AtomicBool,
    /// Notified when the listener's first attempt has ended, either way.
    attempted: Notify,
}

/// Start `coordinator`'s listener, wait up to [`STARTUP_WAIT`] for its
/// first attempt, and return the handle that stops it when dropped. The
/// caller's first refresh follows.
pub(super) async fn start(
    coordinator: Arc<Coordinator>,
    shutdown: watch::Receiver<bool>,
) -> AbortOnDropHandle<()> {
    let startup = Arc::new(Startup::default());
    let listener = AbortOnDropHandle::new(tokio::spawn({
        let startup = startup.clone();
        async move {
            listen(
                &coordinator.config.database_url,
                &coordinator.wake,
                &startup,
                shutdown,
            )
            .await;
        }
    }));
    let _ = timeout(STARTUP_WAIT, startup.attempted.notified()).await;
    startup.refreshing.store(true, Ordering::SeqCst);
    listener
}

/// Wake `wake` on every announcement on [`CHANNEL`] at `database_url`, and
/// once after every LISTEN that `startup`'s first refresh did not follow,
/// until `shutdown` changes. Never returns before that: a failure only delays
/// the next attempt.
async fn listen(
    database_url: &str,
    wake: &Notify,
    startup: &Startup,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut retry = RETRY_FIRST;
    let mut wakes = Wakes::default();
    let mut alert = AlertEveryMinute::default();
    let mut reported = false;
    let mut first = true;
    // Parsed once; the pool is not, so a connection that died without
    // closing cannot hold the next attempt's (see `subscribe`).
    let options = PgConnectOptions::from_str(database_url)
        .map(|options| options.application_name(APPLICATION_NAME));
    loop {
        let subscribed = tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            subscribed = timeout(CONNECT_TIMEOUT, subscribe(&options)) => subscribed
                .unwrap_or_else(|_| Err(anyhow!("connecting timed out after {CONNECT_TIMEOUT:?}"))),
        };
        // Read before the loop is released: one that has not refreshed yet
        // reads the revision after this LISTEN, so it misses nothing.
        let refreshing = startup.refreshing.load(Ordering::SeqCst);
        if std::mem::take(&mut first) {
            startup.attempted.notify_one();
        }
        let error = match subscribed {
            Ok(listener) => {
                // A bump may have committed after the refresh read the
                // revision and before this LISTEN: while the listener was
                // down or connecting, or before its first attempt.
                if refreshing {
                    wake.notify_one();
                    wakes.woke(Instant::now());
                }
                if std::mem::take(&mut reported) {
                    tracing::info!(
                        "payout-revision listener reconnected; refresh woken once (#752)"
                    );
                }
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

/// A listener on a one-connection pool of its own, never shared with the
/// ledger. Each attempt has a new pool: a dropped `PgListener` hands its
/// connection back to its pool only after an UNLISTEN, which never finishes
/// on a connection that died without closing, so a shared pool's one
/// connection could stay held until TCP gave up on it, minutes later.
async fn subscribe(options: &sqlx::Result<PgConnectOptions>) -> Result<PgListener> {
    let options = match options {
        Ok(options) => options.clone(),
        Err(error) => return Err(anyhow!("invalid PRISM_DATABASE_URL: {error}")),
    };
    // No idle or lifetime limit: either would start a maintenance task per
    // attempt that sleeps for minutes, and the listener holds its connection.
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
