//! The supervised loop of the peer sync: one pass every
//! `PRISM_PEER_SYNC_INTERVAL_MS`, or at once while a pass found a full
//! batch. A pass whose peer reads fail is counted and logged, and the next
//! one starts over on the other path; peer rows that fail to apply here fail
//! only their stream, which tries them again next pass and alerts when that
//! goes on. The loop ends at shutdown, or with a [`FrontendStop`] when the
//! database was restored, promoted or changed identity under the running
//! frontend, which stops the frontend: its restart runs every startup check
//! again (D-8, D-9).
//!
//! Each pass, in order:
//! 1. this database must be personalised as this node (D-9, checked every
//!    `IDENTITY_RECHECK`) and hold a valid (origin_node, share_seq) index
//!    (031, D-14), and while the own log is caught up its rollback evidence
//!    must not have changed (D-8, D-17);
//! 2. the peer, on the first path that answers, must be personalised as the
//!    other node, hold a valid 031 index, run this cluster's fingerprint
//!    (D-6), and carry the same columns of every copied table;
//! 3. until the own log is caught up, this node's own rows the peer holds
//!    and this database lacks are pulled back and the own sequences raised
//!    above everything the peer has seen of them; then the latch is set;
//! 4. the peer's shares, then its journal, landed blocks and prepared jobs
//!    (D-5: shares before landings); the last two up to the mark read at
//!    the peer's sync barrier.
use super::{PeerSyncPublisher, PeerSyncStatus, TableSyncStatus};
use crate::config::DualWriterConfig;
use crate::ledger::peer_sync::{
    peer, Applied, BlockBundle, CarriedColumns, PeerFacts, BLOCKS, ORIGIN_INDEX, PREPARED, SHARES,
};
use crate::ledger::{IdentityCheck, Ledger, LineageEvidence};
use crate::metrics::Metrics;
use crate::node_identity::NodeIndex;
use anyhow::{bail, ensure, Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, PgPool};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// How long one statement against the peer may take before the path is
/// treated as dead: a black-holed network answers nothing.
const PEER_STATEMENT_TIMEOUT: Duration = Duration::from_secs(10);
/// The peer session's settings. Every statement is read-only and ends on
/// the server before the client's own bound gives up on it, so an abandoned
/// one never runs on; no transaction is held open, and the server drops a
/// session left idle in one at once. The keepalives and user timeout let the
/// peer's server notice a client that vanished. The sync role sets the same
/// (status/D1.md), so a frontend's options only ever tighten them.
pub(super) const PEER_SESSION_OPTIONS: [(&str, &str); 8] = [
    ("default_transaction_read_only", "on"),
    ("statement_timeout", "8s"),
    ("lock_timeout", "2s"),
    ("idle_in_transaction_session_timeout", "5s"),
    ("tcp_keepalives_idle", "5"),
    ("tcp_keepalives_interval", "2"),
    ("tcp_keepalives_count", "3"),
    ("tcp_user_timeout", "10000"),
];
/// How long connecting to the peer may take.
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// While on the fallback path, how often the first path is tried again.
const PRIMARY_RETRY: Duration = Duration::from_secs(30);
/// How often this database's personalisation is checked again.
const IDENTITY_RECHECK: Duration = Duration::from_secs(30);
/// The most landed blocks one pass applies, each in its own transaction.
const BLOCKS_PER_PASS: i64 = 20;
/// The most own block keys one recovery read compares.
const BLOCK_KEYS_PER_READ: i64 = 1_000;
/// The most peer rows counted for a lag: one index-only count, well inside
/// the peer's statement timeout even on a peer far ahead.
const LAG_COUNT_CAP: i64 = 100_000;
/// The longest pause after failed passes.
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// Consecutive passes in which a stream's next peer rows fail to apply here
/// before the failure is alerted, and how often the alert repeats while it
/// lasts. The stream tries them again every pass and never skips them.
const STREAM_FAILURE_ALERT: u32 = 20;
const STREAM_ALERT_REPEAT: Duration = Duration::from_secs(60);

/// The streams, and the copied tables each carries, for the status.
const STREAM_TABLES: &[(&str, &[&str])] = &[
    (SHARES, &["qbit_share_ledger", "qbit_prism_share_hashes"]),
    (
        BLOCKS,
        &[
            "qbit_pool_blocks",
            "qbit_prism_audit_snapshots",
            "qbit_pool_audit_bundles",
            "qbit_pool_payout_entries",
            "qbit_payout_carry_forward",
            "qbit_ctv_fanout_sets",
            "qbit_ctv_fanout_artifacts",
        ],
    ),
    (
        PREPARED,
        &[
            "qbit_prism_templates",
            "qbit_prism_balance_snapshots",
            "qbit_prism_jobs",
        ],
    ),
    (ROLES, &["qbit_prism_node_roles"]),
];
const ROLES: &str = "roles";

/// Why a pass synced nothing although it may have reached the peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// This database is not personalised as this node, or has drifted.
    LocalIdentity(String),
    /// The peer's database is not personalised as the other node.
    PeerIdentity(String),
    /// The peer runs another cluster fingerprint (D-6).
    Fingerprint,
    /// The peer's copied tables carry other columns.
    Schema(Vec<String>),
    /// The (origin_node, share_seq) index of migration 031 is missing or not
    /// valid yet in this database (`true`) or the peer's (`false`): every
    /// share read by origin would walk the ledger.
    OriginIndex { local: bool },
}

impl Refusal {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::LocalIdentity(_) => "local_identity",
            Self::PeerIdentity(_) => "peer_identity",
            Self::Fingerprint => "fingerprint",
            Self::Schema(_) | Self::OriginIndex { .. } => "schema",
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalIdentity(why) => write!(f, "this database: {why}"),
            Self::PeerIdentity(why) => write!(f, "the peer's database: {why}"),
            Self::Fingerprint => f.write_str("the peer runs another cluster fingerprint"),
            Self::Schema(tables) => write!(
                f,
                "the peer's copied tables carry other columns: {}",
                tables.join("; ")
            ),
            Self::OriginIndex { local } => write!(
                f,
                "{} has no valid {ORIGIN_INDEX} (migration 031): every share read by origin would \
                 walk the ledger",
                if *local {
                    "this database"
                } else {
                    "the peer's database"
                }
            ),
        }
    }
}

/// The database changed under the running frontend in a way no task started
/// before it can be trusted with: [`PeerSync::run`] ends with it, which stops
/// the frontend, and its restart runs every startup check again.
#[derive(Debug)]
pub enum FrontendStop {
    /// Restored or promoted: a new system identifier or WAL timeline since
    /// the own log was verified (D-8, D-17). The restart serves only once
    /// own-log recovery completes.
    OwnLogLost,
    /// Its node identity, an origin default or a key sequence's parity
    /// changed since the frontend saw it ready (D-9). The restart refuses a
    /// drifted database, and admits no miners on one that is not this
    /// node's, until it is repaired.
    IdentityChanged(String),
}

impl std::fmt::Display for FrontendStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OwnLogLost => f.write_str(
                "this node's database was restored or promoted under the running frontend: it \
                 stops, and its restart serves only once own-log recovery completes (D-8)",
            ),
            Self::IdentityChanged(why) => write!(
                f,
                "this node's database changed identity under the running frontend ({why}): it \
                 stops, and serves again only on a database personalised as this node (D-9)"
            ),
        }
    }
}

impl std::error::Error for FrontendStop {}

/// What one pass did.
#[derive(Clone, Debug, Default)]
pub struct PassReport {
    /// The pass reached the peer's database.
    pub reached_peer: bool,
    /// Which path answered: 0 the first, 1 the fallback.
    pub path: Option<usize>,
    pub refused: Option<Refusal>,
    pub own_log_caught_up: bool,
    /// Rows applied and conflicts recorded, by table.
    pub applied: Applied,
    /// A batch was full: pass again at once.
    pub more: bool,
    /// Streams whose next peer rows could not be applied here; each keeps
    /// its cursor and is tried again next pass, and the others went on.
    pub failed_streams: Vec<&'static str>,
}

/// The own-log latch (D-8): false until this node's own rows are proved
/// complete, then true until a local rollback is detected.
#[derive(Clone, Copy, Debug, Default)]
struct Latch {
    caught_up: bool,
    evidence: Option<LineageEvidence>,
    rollback_alerted: bool,
}

/// One path to the peer: its DSN and, once connected, its pool.
struct Path {
    url: String,
    pool: Option<PgPool>,
}

#[derive(Clone, Copy, Debug, Default)]
struct StreamState {
    lag_rows: u64,
    caught_up_at: Option<Instant>,
    last_success: Option<chrono::DateTime<chrono::Utc>>,
    /// Consecutive passes whose apply of this stream's rows failed.
    failures: u32,
    alerted_at: Option<Instant>,
}

/// The peer sync of one dual-writer frontend.
pub struct PeerSync {
    ledger: Ledger,
    node: NodeIndex,
    interval: Duration,
    batch_rows: i64,
    publisher: PeerSyncPublisher,
    metrics: Option<Arc<Metrics>>,
    paths: Vec<Path>,
    active: usize,
    on_fallback_since: Option<Instant>,
    identity_checked: Option<Instant>,
    identity_refusal: Option<Refusal>,
    /// This database was found personalised as this node, undrifted.
    identity_ready: bool,
    latch: Latch,
    /// Own-log recovery met an own row this database holds with other
    /// content: the latch stays down until an operator resolves it.
    own_log_diverged: bool,
    /// Own-log recovery found own rows missing here and forgot the last
    /// verification before inserting them.
    own_rows_missing: bool,
    safe_sync_mark: Option<i64>,
    streams: BTreeMap<&'static str, StreamState>,
    started: Instant,
}

impl PeerSync {
    /// A peer sync for `ledger`'s database, which must be `config`'s node's.
    /// Nothing runs until [`PeerSync::run`] or [`PeerSync::pass`].
    pub fn new(
        ledger: Ledger,
        config: &DualWriterConfig,
        metrics: Option<Arc<Metrics>>,
    ) -> (Self, watch::Receiver<PeerSyncStatus>) {
        let (publisher, status) = PeerSyncPublisher::new();
        if let Some(metrics) = &metrics {
            metrics.start_peer_sync();
        }
        let sync = Self {
            ledger,
            node: config.identity.node,
            interval: config.peer_sync_interval,
            batch_rows: i64::from(config.peer_sync_batch_rows),
            publisher,
            metrics,
            paths: config
                .peer_database_urls()
                .map(|url| Path {
                    url: url.to_owned(),
                    pool: None,
                })
                .collect(),
            active: 0,
            on_fallback_since: None,
            identity_checked: None,
            identity_refusal: None,
            identity_ready: false,
            latch: Latch::default(),
            own_log_diverged: false,
            own_rows_missing: false,
            safe_sync_mark: None,
            streams: BTreeMap::new(),
            started: Instant::now(),
        };
        (sync, status)
    }

    /// Another receiver of the status this sync publishes.
    pub fn subscribe(&self) -> watch::Receiver<PeerSyncStatus> {
        self.publisher.subscribe()
    }

    /// Pass until `shutdown` turns true. A failed pass is logged and counted,
    /// and the next one starts over on the other path after a pause that
    /// grows with consecutive failures. The one error it returns is a
    /// [`FrontendStop`], which stops the frontend.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut failures: u32 = 0;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let pause = match self.pass().await {
                Ok(report) => {
                    failures = 0;
                    if report.more {
                        Duration::ZERO
                    } else {
                        self.interval
                    }
                }
                Err(error) if error.downcast_ref::<FrontendStop>().is_some() => {
                    for path in &mut self.paths {
                        if let Some(pool) = path.pool.take() {
                            pool.close().await;
                        }
                    }
                    return Err(error);
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    tracing::warn!(
                        error = %format!("{error:#}"),
                        path = self.path_name(),
                        failures,
                        "peer sync pass failed"
                    );
                    if let Some(metrics) = &self.metrics {
                        metrics.record_peer_sync_failure(self.path_name());
                    }
                    self.drop_active_path();
                    self.publish(false, None);
                    self.interval
                        .saturating_mul(1 << failures.min(5))
                        .min(MAX_BACKOFF.max(self.interval))
                }
            };
            if pause.is_zero() {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                biased;
                _ = shutdown.changed() => {}
                _ = tokio::time::sleep(pause) => {}
            }
        }
        for path in &mut self.paths {
            if let Some(pool) = path.pool.take() {
                pool.close().await;
            }
        }
        Ok(())
    }

    fn path_name(&self) -> &'static str {
        if self.active == 0 {
            "primary"
        } else {
            "fallback"
        }
    }

    /// Close the active path's pool and move to the next path.
    fn drop_active_path(&mut self) {
        self.safe_sync_mark = None;
        if let Some(pool) = self.paths[self.active].pool.take() {
            tokio::spawn(async move { pool.close().await });
        }
        if self.paths.len() > 1 {
            self.active = (self.active + 1) % self.paths.len();
            self.on_fallback_since = (self.active != 0).then(Instant::now);
        }
    }

    /// A connection on the active path, connecting it first if needed.
    async fn peer_connection(&mut self) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>> {
        if self.active != 0
            && self
                .on_fallback_since
                .is_some_and(|since| since.elapsed() >= PRIMARY_RETRY)
        {
            // Back to the first path; the fallback is tried again if it fails.
            if let Some(pool) = self.paths[self.active].pool.take() {
                tokio::spawn(async move { pool.close().await });
            }
            self.active = 0;
            self.on_fallback_since = None;
        }
        let path = &mut self.paths[self.active];
        if path.pool.is_none() {
            // A new connection may reach a peer that was restored or failed
            // over meanwhile: a mark read from the old one proves nothing.
            self.safe_sync_mark = None;
            let options = PgConnectOptions::from_str(&path.url)
                .map_err(|_| anyhow::anyhow!("invalid peer database URL"))?
                .application_name("qbit-prism-peer-sync")
                .options(PEER_SESSION_OPTIONS)
                .disable_statement_logging();
            path.pool = Some(
                PgPoolOptions::new()
                    .max_connections(2)
                    .min_connections(0)
                    .acquire_timeout(PEER_CONNECT_TIMEOUT)
                    .idle_timeout(Some(Duration::from_secs(60)))
                    .connect_lazy_with(options),
            );
        }
        let pool = path.pool.as_ref().expect("connected above");
        tokio::time::timeout(PEER_CONNECT_TIMEOUT, pool.acquire())
            .await
            .context("connecting to the peer's database timed out")?
            .context("connecting to the peer's database")
    }

    /// The database changed under the running frontend: refuse every share
    /// from now until the process exits (the server cancels every other task
    /// as the sync ends with the returned error), drop the latch and say so,
    /// so that health and the gauges stop reporting this node ready.
    fn stop_frontend(&mut self, stop: FrontendStop, refusal: Option<&Refusal>) -> anyhow::Error {
        tracing::error!(reason = %stop, "ALERT: the frontend stops");
        self.ledger.set_own_log_lost(true);
        self.latch = Latch::default();
        self.publish(false, refusal);
        stop.into()
    }

    /// Check this database's identity at the next pass, whatever the time
    /// since the last check. For tests.
    #[doc(hidden)]
    pub fn recheck_identity(&mut self) {
        self.identity_checked = None;
    }

    /// One pass. Public so that tests can drive the sync step by step.
    pub async fn pass(&mut self) -> Result<PassReport> {
        let mut report = PassReport::default();
        // First, before anything may refuse: a database restored or promoted
        // under the running frontend stops it (D-8). Its restart serves
        // nothing until own-log recovery completes, which no task started
        // before the restore could guarantee; until it exits, no share is
        // appended.
        if self.latch.caught_up {
            let now = self.ledger.lineage_evidence().await?;
            if Some(now) != self.latch.evidence {
                tracing::error!(previous = ?self.latch.evidence, now = ?now, "evidence of a restore or promotion");
                return Err(self.stop_frontend(FrontendStop::OwnLogLost, None));
            }
        }
        if let Some(refusal) = self.check_local_identity().await? {
            report.refused = Some(refusal.clone());
            self.publish(false, Some(&refusal));
            return Ok(report);
        }
        // This database's copied columns, read before the peer is touched, so
        // that a local failure fails the pass before any peer read.
        let local_columns = self.ledger.carried_columns().await?;
        let mut connection = match self.peer_connection().await {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), path = self.path_name(), "peer database unreachable");
                if let Some(metrics) = &self.metrics {
                    metrics.record_peer_sync_failure(self.path_name());
                }
                self.drop_active_path();
                self.latch_without_peer().await?;
                report.own_log_caught_up = self.latch.caught_up;
                self.publish(false, None);
                return Ok(report);
            }
        };
        report.reached_peer = true;
        report.path = Some(self.active);
        let on_peer = self
            .pass_on_peer(&mut connection, &mut report, &local_columns)
            .await;
        if let Err(error) = on_peer {
            // A peer that answers the connection but cannot be read is as
            // good as unreachable for the latch (D-8): without it, the latch
            // follows the local rollback evidence, unless the own log was
            // found to have diverged from the peer's copy of it.
            if !self.own_log_diverged {
                self.latch_without_peer().await?;
            }
            self.publish(false, None);
            return Err(error);
        }
        if let Some(metrics) = &self.metrics {
            metrics.record_peer_sync_applied(&report.applied);
        }
        self.publish(true, report.refused.as_ref());
        Ok(report)
    }

    /// The part of a pass that reads the peer: its identity and fingerprint,
    /// the own log until the latch is set, then the peer's streams.
    async fn pass_on_peer(
        &mut self,
        connection: &mut sqlx::PgConnection,
        report: &mut PassReport,
        local_columns: &CarriedColumns,
    ) -> Result<()> {
        let facts = bounded(peer::facts(connection)).await?;
        if let Some(refusal) = self.check_peer(connection, &facts, local_columns).await? {
            tracing::error!(%refusal, "ALERT: the peer sync refuses this peer");
            self.latch_without_peer().await?;
            report.own_log_caught_up = self.latch.caught_up;
            report.refused = Some(refusal);
            return Ok(());
        }
        if !self.latch.caught_up {
            report
                .applied
                .merge(self.recover_own_log(connection, &facts).await?);
        }
        report.own_log_caught_up = self.latch.caught_up;
        if self.latch.caught_up {
            self.pull_peer(connection, &facts, report).await?;
        }
        Ok(())
    }

    /// D-9: refuse unless this database is personalised as this node, with
    /// nothing drifted, and holds a valid 031 index (D-14). Checked every
    /// `IDENTITY_RECHECK`. Once seen ready, a database that is no longer
    /// this node's stops the frontend ([`FrontendStop::IdentityChanged`]).
    async fn check_local_identity(&mut self) -> Result<Option<Refusal>> {
        if self
            .identity_checked
            .is_some_and(|at| at.elapsed() < IDENTITY_RECHECK)
        {
            return Ok(self.identity_refusal.clone());
        }
        let check = self.ledger.check_node_identity(self.node).await?;
        if matches!(check, IdentityCheck::Ready(_)) {
            self.identity_ready = true;
        }
        let refusal = match check {
            IdentityCheck::Ready(_) => None,
            IdentityCheck::Unidentified => Some(Refusal::LocalIdentity(format!(
                "no node identity; run `qbit-prism-server node-identity set --index {}`",
                self.node.index()
            ))),
            IdentityCheck::OtherNode(record) => Some(Refusal::LocalIdentity(format!(
                "it is node {}'s, not node {}'s",
                record.node, self.node
            ))),
            IdentityCheck::Drifted(_, drift) => Some(Refusal::LocalIdentity(format!(
                "its personalisation drifted: {}",
                drift.join(", ")
            ))),
        };
        let refusal = match refusal {
            None if !self.ledger.origin_index_valid().await? => {
                Some(Refusal::OriginIndex { local: true })
            }
            refusal => refusal,
        };
        // Seen ready once, then not: the identity changed under the running
        // frontend, which every one of its writers was started on. It stops.
        if let (Some(Refusal::LocalIdentity(why)), true) = (&refusal, self.identity_ready) {
            let stop = FrontendStop::IdentityChanged(why.clone());
            return Err(self.stop_frontend(stop, refusal.as_ref()));
        }
        if let Some(refusal) = &refusal {
            tracing::error!(%refusal, "ALERT: the peer sync does not run");
        }
        self.identity_checked = Some(Instant::now());
        self.identity_refusal = refusal.clone();
        Ok(refusal)
    }

    /// The peer must be the other node, hold a valid 031 index, run this
    /// cluster's fingerprint (D-6) and carry the same columns of every copied
    /// table.
    async fn check_peer(
        &mut self,
        connection: &mut sqlx::PgConnection,
        facts: &PeerFacts,
        ours: &CarriedColumns,
    ) -> Result<Option<Refusal>> {
        let peer = self.node.peer();
        match facts.node {
            Some(node) if node == peer => {}
            Some(node) => {
                return Ok(Some(Refusal::PeerIdentity(format!(
                    "it is personalised as node {node}, this node's own identity"
                ))))
            }
            None => {
                return Ok(Some(Refusal::PeerIdentity(
                    "it has no node identity".into(),
                )))
            }
        }
        if facts.share_seq_floor.is_none() || facts.sync_seq_floor.is_none() {
            return Ok(Some(Refusal::PeerIdentity("it has no lineage".into())));
        }
        if !facts.origin_index_valid {
            return Ok(Some(Refusal::OriginIndex { local: false }));
        }
        if self.ledger.stored_config_fingerprint().await? != facts.config_fingerprint {
            return Ok(Some(Refusal::Fingerprint));
        }
        // Both sides' columns, every pass, before any pull: a rolling
        // migration that gives one node a copied column first stops the sync
        // before a cursor passes rows whose new column this node would drop.
        let theirs = bounded(CarriedColumns::read(connection)).await?;
        let differences = ours.differences(&theirs);
        if !differences.is_empty() {
            return Ok(Some(Refusal::Schema(differences)));
        }
        Ok(None)
    }

    /// Without the peer, the latch is set unless this database shows
    /// evidence of a rollback since its own log was last verified (D-17).
    async fn latch_without_peer(&mut self) -> Result<()> {
        if self.latch.caught_up || self.own_log_diverged {
            return Ok(());
        }
        let now = self.ledger.lineage_evidence().await?;
        let verified = self
            .ledger
            .node_lineage()
            .await?
            .and_then(|lineage| lineage.verified);
        match verified {
            Some((evidence, _)) if evidence == now => {
                tracing::info!(
                    system_identifier = now.system_identifier,
                    timeline = now.timeline,
                    "own log caught up without the peer: the database is on the server its own log was last verified on"
                );
                self.latch = Latch {
                    caught_up: true,
                    evidence: Some(now),
                    rollback_alerted: false,
                };
            }
            _ => {
                if !self.latch.rollback_alerted {
                    tracing::error!(
                        verified = ?verified,
                        now = ?now,
                        "ALERT: rollback evidence: this database's system identifier or WAL timeline \
                         differs from its last own-log verification, or it has none; this node does \
                         not serve until the peer confirms its own log"
                    );
                    self.latch.rollback_alerted = true;
                }
            }
        }
        Ok(())
    }

    /// Pull back this node's own rows the peer holds and this database
    /// lacks, raise the own sequences and the ledger clock above everything
    /// the peer has seen of this node, record the verification, and set the
    /// latch (D-8, D-14). A conflict among the own rows leaves the latch
    /// unset: the own log has diverged, and an operator must decide.
    async fn recover_own_log(
        &mut self,
        connection: &mut sqlx::PgConnection,
        facts: &PeerFacts,
    ) -> Result<Applied> {
        let node = self.node;
        let mut applied = Applied::default();
        let mut clock_ms = None;
        // Every own row the peer holds came through its pulls, which its
        // cursor over this node's shares covers, or was held when it was
        // personalised, below its floor: together they bound the keys taken
        // back.
        let own_rows_through = bounded(peer::cursors(connection))
            .await?
            .shares
            .max(facts.share_seq_floor);
        loop {
            let (held, ..) = self.ledger.highest_held_of(node).await?;
            let batch = bounded(peer::shares_of(
                connection,
                held.unwrap_or(0),
                self.batch_rows,
                node,
            ))
            .await?;
            if batch.row_count == 0 {
                break;
            }
            self.own_rows_missing().await?;
            clock_ms = clock_ms.max(batch.highest_accepted_at_ms);
            applied.merge(
                self.ledger
                    .apply_share_batch(node, node, &batch, None, own_rows_through)
                    .await?,
            );
            self.ensure_progress(&applied).await?;
        }
        // Landed blocks and prepared jobs commit out of sync_seq order, so
        // the highest one held here proves no prefix: a backup can hold a
        // block whose earlier-numbered sibling was still open. The key and
        // facts digest of every block of this node's the peer holds since it
        // was personalised are compared; only the blocks missing here, or
        // held with other facts (a conflict: the own log has diverged), are
        // read whole.
        let floor = self
            .ledger
            .node_lineage()
            .await?
            .map_or(0, |lineage| lineage.sync_seq_floor);
        let mut after = floor;
        loop {
            let keys = bounded(peer::block_keys(
                connection,
                node,
                after,
                BLOCK_KEYS_PER_READ,
            ))
            .await?;
            let Some(last) = keys.last().map(|(sync_seq, ..)| *sync_seq) else {
                break;
            };
            let keys: Vec<(String, String)> = keys
                .into_iter()
                .map(|(_, hash, digest)| (hash, digest))
                .collect();
            let unlike = self.ledger.missing_blocks(&keys).await?;
            if unlike.iter().any(|(_, held)| !held) {
                self.own_rows_missing().await?;
            }
            let hashes: Vec<String> = unlike.into_iter().map(|(hash, _)| hash).collect();
            for some in hashes.chunks(BLOCKS_PER_PASS as usize) {
                let blocks = bounded(peer::blocks_with_hashes(connection, node, some)).await?;
                ensure!(
                    blocks.len() == some.len(),
                    "the peer no longer holds {} of the own blocks it listed; recovery starts again",
                    some.len() - blocks.len()
                );
                for block in &blocks {
                    applied.merge(self.ledger.apply_block(block, None).await?);
                }
                self.ensure_progress(&applied).await?;
            }
            after = last;
        }
        // Only the prepared jobs not yet expired: this node prunes its own at
        // expiry (the peer may keep its copy longer), so an expired one
        // missing here is no row lost.
        let mut after = floor;
        loop {
            let batch = bounded(peer::prepared(
                connection,
                node,
                after,
                None,
                self.batch_rows,
                true,
            ))
            .await?;
            let Some(last) = batch.highest else {
                break;
            };
            if self.ledger.missing_prepared(&batch).await? > 0 {
                self.own_rows_missing().await?;
            }
            applied.merge(self.ledger.apply_prepared(&batch, node, None).await?);
            self.ensure_progress(&applied).await?;
            after = last;
        }
        let roles = bounded(peer::node_roles(connection, node)).await?;
        let journal = self.ledger.apply_node_roles(&roles, node).await?;
        if !journal.inserted.is_empty() {
            // Missing too, so the verification goes, as above; the rows are
            // already back, which is all a crash here could leave behind.
            self.own_rows_missing().await?;
        }
        applied.merge(journal);
        self.ensure_progress(&applied).await?;
        // Above everything the peer has seen of this node: its cursors over
        // this node's streams, and the highest own rows it holds.
        let cursors = bounded(peer::cursors(connection)).await?;
        let (peer_share, peer_sync, _) = bounded(peer::highest_of(connection, node)).await?;
        let share_floor = cursors.shares.max(peer_share);
        let sync_floor = cursors.blocks.max(cursors.prepared).max(peer_sync);
        self.ledger
            .raise_own_sequences(node, share_floor, sync_floor, clock_ms)
            .await?;
        let evidence = self.ledger.lineage_evidence().await?;
        self.ledger.record_own_log_verified(evidence).await?;
        self.own_rows_missing = false;
        tracing::info!(
            recovered = ?applied.inserted,
            system_identifier = evidence.system_identifier,
            timeline = evidence.timeline,
            "own log caught up: every row this node originated that the peer holds is here"
        );
        self.latch = Latch {
            caught_up: true,
            evidence: Some(evidence),
            rollback_alerted: false,
        };
        Ok(applied)
    }

    /// An own-log conflict: the own log has diverged from the peer's copy
    /// of it, and the latch stays unset until an operator resolves it. The
    /// verification goes too, so no start without the peer latches on it.
    async fn ensure_progress(&mut self, applied: &Applied) -> Result<()> {
        if applied.total_conflicts() > 0 {
            self.own_log_diverged = true;
            self.ledger.forget_own_log_verification().await?;
            bail!(
                "own-log recovery met rows this node holds with other content ({:?}); the own log \
                 has diverged from the peer's copy of it and this node does not serve until an \
                 operator resolves qbit_prism_peer_sync_conflicts",
                applied.conflicts
            );
        }
        Ok(())
    }

    /// Own rows the peer holds are missing here, so whatever verification
    /// this database recorded no longer proves it whole: a restore that kept
    /// the timeline (S6) shows the evidence it was verified on. Forget it
    /// before the rows are inserted, so that if recovery stops partway, on
    /// a peer that answers but fails or a crash, the latch cannot set from
    /// that evidence; it sets when recovery completes.
    async fn own_rows_missing(&mut self) -> Result<()> {
        if !self.own_rows_missing {
            if self.ledger.forget_own_log_verification().await? {
                tracing::error!(
                    "ALERT: own rows the peer holds are missing here although this database \
                     showed the evidence of its last own-log verification (a restore that kept \
                     the timeline): the verification is forgotten, and this node does not serve \
                     until it has recovered them from the peer"
                );
            }
            self.own_rows_missing = true;
        }
        Ok(())
    }

    /// Pull the peer's rows: shares, the journal, then landed blocks and
    /// prepared jobs (D-5), into `report`. A peer read that fails fails the
    /// pass; rows that fail to apply here fail only their stream.
    async fn pull_peer(
        &mut self,
        connection: &mut sqlx::PgConnection,
        facts: &PeerFacts,
        report: &mut PassReport,
    ) -> Result<()> {
        let peer = self.node.peer();
        // Shares.
        let cursor = match self.ledger.peer_sync_cursor(SHARES).await? {
            Some((position, _)) => position,
            None => {
                let (held, ..) = self.ledger.highest_held_of(peer).await?;
                let start = start_position(held, facts.share_seq_floor);
                self.ledger
                    .start_peer_sync_cursor(SHARES, peer, start)
                    .await?;
                start
            }
        };
        let batch = bounded(peer::shares_scanned(
            connection,
            cursor,
            self.batch_rows,
            peer,
        ))
        .await?;
        // Whether the share mark moved: blocks waiting for it pass again at
        // once only then.
        let mut shares_moved = false;
        match self
            .ledger
            .apply_share_batch(self.node, peer, &batch, Some(SHARES), None)
            .await
        {
            Ok(applied) => {
                report.applied.merge(applied);
                report.more |= batch.scanned >= self.batch_rows;
                shares_moved = batch.through.is_some_and(|through| through > cursor);
                let through = batch.through.unwrap_or(cursor);
                let lag = bounded(peer::shares_beyond(
                    connection,
                    through,
                    peer,
                    LAG_COUNT_CAP,
                ))
                .await?;
                self.stream_done(SHARES, u64::try_from(lag).unwrap_or_default());
            }
            Err(error) => self.stream_failed(SHARES, batch.row_count as u64, &error, report),
        }
        // The carry-owner journal.
        let roles = bounded(peer::node_roles(connection, peer)).await?;
        match self.ledger.apply_node_roles(&roles, peer).await {
            Ok(applied) => {
                report.applied.merge(applied);
                self.stream_done(ROLES, 0);
            }
            Err(error) => self.stream_failed(ROLES, 1, &error, report),
        }
        // The sync_seq streams stop at the mark read at the sync barrier;
        // while a writer holds it, the last mark stands.
        if let Some(position) = bounded(peer::sync_barrier(connection)).await? {
            self.safe_sync_mark = self.safe_sync_mark.max(position);
        }
        let Some(safe) = self.safe_sync_mark else {
            self.stream_done(BLOCKS, 0);
            self.stream_done(PREPARED, 0);
            return Ok(());
        };
        // Landed blocks, each whole and in order: one that fails to apply
        // stops the stream there, so the cursor never passes it.
        let cursor = self
            .sync_cursor(BLOCKS, peer, facts.sync_seq_floor, true)
            .await?;
        let blocks: Vec<BlockBundle> = bounded(peer::blocks(
            connection,
            peer,
            cursor,
            Some(safe),
            BLOCKS_PER_PASS,
        ))
        .await?;
        // D-5: a block waits until the share mark covers its window, so its
        // audit is verifiable here when this node confirms it.
        let share_mark = self
            .ledger
            .peer_sync_cursor(SHARES)
            .await?
            .map(|(mark, _)| mark);
        let covered = blocks
            .iter()
            .take_while(|block| window_covered(block, share_mark))
            .count();
        let mut failed = None;
        for (index, block) in blocks[..covered].iter().enumerate() {
            match self.ledger.apply_block(block, Some((BLOCKS, peer))).await {
                Ok(applied) => report.applied.merge(applied),
                Err(error) => {
                    failed = Some((blocks.len() - index, error));
                    break;
                }
            }
        }
        if let Some((pending, error)) = failed {
            self.stream_failed(BLOCKS, pending as u64, &error, report);
        } else if covered < blocks.len() {
            // Never at once while the share mark stands still, as when the
            // share stream fails: the interval paces the retry.
            report.more |= shares_moved;
            self.stream_done(BLOCKS, (blocks.len() - covered) as u64);
        } else if (blocks.len() as i64) < BLOCKS_PER_PASS {
            self.ledger
                .settle_peer_sync_cursor(BLOCKS, peer, safe)
                .await?;
            self.stream_done(BLOCKS, 0);
        } else {
            report.more = true;
            self.stream_done(BLOCKS, BLOCKS_PER_PASS as u64);
        }
        // Prepared jobs with their blobs.
        let cursor = self
            .sync_cursor(PREPARED, peer, facts.sync_seq_floor, false)
            .await?;
        let batch = bounded(peer::prepared(
            connection,
            peer,
            cursor,
            Some(safe),
            self.batch_rows,
            false,
        ))
        .await?;
        match self
            .ledger
            .apply_prepared(&batch, peer, Some(PREPARED))
            .await
        {
            Ok(applied) => {
                report.applied.merge(applied);
                if (batch.count as i64) < self.batch_rows {
                    self.ledger
                        .settle_peer_sync_cursor(PREPARED, peer, safe)
                        .await?;
                    self.stream_done(PREPARED, 0);
                } else {
                    report.more = true;
                    self.stream_done(PREPARED, batch.count as u64);
                }
            }
            Err(error) => self.stream_failed(PREPARED, batch.count as u64, &error, report),
        }
        Ok(())
    }

    /// A `sync_seq` stream's cursor, started where the peer's rows begin if
    /// it has none: the lower of the highest peer root row held here and the
    /// peer's floor.
    async fn sync_cursor(
        &self,
        stream: &'static str,
        peer: NodeIndex,
        floor: Option<i64>,
        blocks: bool,
    ) -> Result<i64> {
        if let Some((position, _)) = self.ledger.peer_sync_cursor(stream).await? {
            return Ok(position);
        }
        let (_, held_blocks, held_prepared, _) = self.ledger.highest_held_of(peer).await?;
        let start = start_position(if blocks { held_blocks } else { held_prepared }, floor);
        self.ledger
            .start_peer_sync_cursor(stream, peer, start)
            .await?;
        Ok(start)
    }

    fn stream_done(&mut self, stream: &'static str, lag_rows: u64) {
        let state = self.streams.entry(stream).or_default();
        if state.failures >= STREAM_FAILURE_ALERT {
            tracing::info!(
                stream,
                failures = state.failures,
                "peer sync stream applies again"
            );
        }
        state.lag_rows = lag_rows;
        state.last_success = Some(chrono::Utc::now());
        state.failures = 0;
        state.alerted_at = None;
        if lag_rows == 0 {
            state.caught_up_at = Some(Instant::now());
        }
    }

    /// A stream's next peer rows could not be applied here (a lock or
    /// statement timeout, say). Nothing of the failed unit was kept and its
    /// cursor stands, so the next pass applies it again; the other streams
    /// go on. The stream counts as at least `pending` rows behind, so its
    /// lag grows, and a run of failures is alerted.
    fn stream_failed(
        &mut self,
        stream: &'static str,
        pending: u64,
        error: &anyhow::Error,
        report: &mut PassReport,
    ) {
        report.failed_streams.push(stream);
        let state = self.streams.entry(stream).or_default();
        state.lag_rows = state.lag_rows.max(pending).max(1);
        state.failures = state.failures.saturating_add(1);
        let error = format!("{error:#}");
        if state.failures >= STREAM_FAILURE_ALERT
            && state
                .alerted_at
                .is_none_or(|at| at.elapsed() >= STREAM_ALERT_REPEAT)
        {
            state.alerted_at = Some(Instant::now());
            tracing::error!(
                stream,
                failures = state.failures,
                %error,
                "ALERT: the peer sync cannot apply this stream's next peer rows; it tries them \
                 again every pass and never skips them"
            );
        } else if state.failures == 1 {
            tracing::warn!(
                stream,
                %error,
                "peer sync could not apply a stream's next peer rows; the next pass tries again"
            );
        }
    }

    /// Publish the status and its gauges.
    fn publish(&mut self, reachable: bool, refusal: Option<&Refusal>) {
        let since_start = self.started.elapsed();
        let mut per_table = BTreeMap::new();
        let mut stream_lags = Vec::new();
        for (stream, tables) in STREAM_TABLES {
            let state = self.streams.get(stream).copied().unwrap_or_default();
            let lag_seconds = state
                .caught_up_at
                .map_or(since_start, |at| {
                    if state.lag_rows == 0 {
                        Duration::ZERO
                    } else {
                        at.elapsed()
                    }
                })
                .as_secs_f64();
            stream_lags.push((*stream, state.lag_rows, lag_seconds));
            for table in *tables {
                per_table.insert(
                    (*table).to_owned(),
                    TableSyncStatus {
                        lag_rows: state.lag_rows,
                        lag_seconds,
                        last_success: state.last_success,
                    },
                );
            }
        }
        let caught_up = self.latch.caught_up;
        self.publisher.update(|status| {
            status.peer_reachable = reachable;
            status.own_log_caught_up = caught_up;
            status.per_table = per_table;
        });
        if let Some(metrics) = &self.metrics {
            metrics.publish_peer_sync(
                reachable,
                caught_up,
                self.latch.rollback_alerted,
                self.path_name(),
                refusal.map(Refusal::reason),
                &stream_lags,
            );
        }
    }
}

/// Whether every share of `block`'s window is at or below the share mark:
/// its snapshot's last `share_seq`, which no window share is above. A block
/// with no snapshot (an empty window) needs none.
fn window_covered(block: &BlockBundle, share_mark: Option<i64>) -> bool {
    let last = block
        .snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.get("last_share_seq"))
        .and_then(serde_json::Value::as_i64);
    match last {
        None => true,
        Some(last) => share_mark.is_some_and(|mark| mark >= last),
    }
}

/// Where a stream's cursor starts: the lower of the highest peer row held
/// here and the peer's floor, so a peer row neither has seen is never
/// skipped.
fn start_position(held: Option<i64>, floor: Option<i64>) -> i64 {
    match (held, floor) {
        (Some(held), Some(floor)) => held.min(floor),
        (Some(position), None) | (None, Some(position)) => position,
        (None, None) => 0,
    }
}

/// One statement against the peer, bounded so a dead path fails the pass.
async fn bounded<T>(statement: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(PEER_STATEMENT_TIMEOUT, statement)
        .await
        .context("a statement against the peer's database timed out")?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_starts_at_the_lower_of_the_held_row_and_the_floor() {
        assert_eq!(start_position(Some(10), Some(7)), 7);
        assert_eq!(start_position(Some(5), Some(7)), 5);
        assert_eq!(start_position(None, Some(7)), 7);
        assert_eq!(start_position(Some(5), None), 5);
        assert_eq!(start_position(None, None), 0);
    }

    #[test]
    fn refusals_name_their_reason() {
        assert_eq!(Refusal::Fingerprint.reason(), "fingerprint");
        assert_eq!(Refusal::Schema(vec![]).reason(), "schema");
        assert_eq!(
            Refusal::LocalIdentity(String::new()).reason(),
            "local_identity"
        );
        assert_eq!(
            Refusal::PeerIdentity(String::new()).reason(),
            "peer_identity"
        );
    }
}
