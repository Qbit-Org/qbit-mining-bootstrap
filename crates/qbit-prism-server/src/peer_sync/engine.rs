//! The supervised loop of the peer sync: one pass every
//! `PRISM_PEER_SYNC_INTERVAL_MS`, or at once while a pass found a full
//! batch. A failed pass is counted and logged, and the next one starts over
//! on the other path; the loop ends only at shutdown.
//!
//! Each pass, in order:
//! 1. this database must be personalised as this node (D-9, checked every
//!    `IDENTITY_RECHECK`), and while the own log is caught up its rollback
//!    evidence must not have changed (D-8, D-17);
//! 2. the peer, on the first path that answers, must be personalised as the
//!    other node, run this cluster's fingerprint (D-6), and carry the same
//!    columns of every copied table;
//! 3. until the own log is caught up, this node's own rows the peer holds
//!    and this database lacks are pulled back and the own sequences raised
//!    above everything the peer has seen of them; then the latch is set;
//! 4. the peer's shares, then its journal, landed blocks and prepared jobs
//!    (D-5: shares before landings); the last two up to the mark read at
//!    the peer's sync barrier.
use super::{PeerSyncPublisher, PeerSyncStatus, TableSyncStatus};
use crate::config::DualWriterConfig;
use crate::ledger::peer_sync::{
    peer, Applied, BlockBundle, CarriedColumns, PeerFacts, BLOCKS, PREPARED, SHARES,
};
use crate::ledger::{IdentityCheck, Ledger, LineageEvidence};
use crate::metrics::Metrics;
use crate::node_identity::NodeIndex;
use anyhow::{bail, Context, Result};
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
/// How long connecting to the peer may take.
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// While on the fallback path, how often the first path is tried again.
const PRIMARY_RETRY: Duration = Duration::from_secs(30);
/// How often this database's personalisation is checked again.
const IDENTITY_RECHECK: Duration = Duration::from_secs(30);
/// The most landed blocks one pass applies, each in its own transaction.
const BLOCKS_PER_PASS: i64 = 20;
/// The most peer rows counted for a lag.
const LAG_COUNT_CAP: i64 = 1_000_000;
/// The longest pause after failed passes.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

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
}

impl Refusal {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::LocalIdentity(_) => "local_identity",
            Self::PeerIdentity(_) => "peer_identity",
            Self::Fingerprint => "fingerprint",
            Self::Schema(_) => "schema",
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
        }
    }
}

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
    local_columns: Option<CarriedColumns>,
    peer_columns_checked: Option<usize>,
    identity_checked: Option<Instant>,
    identity_refusal: Option<Refusal>,
    latch: Latch,
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
            local_columns: None,
            peer_columns_checked: None,
            identity_checked: None,
            identity_refusal: None,
            latch: Latch::default(),
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

    /// Pass until `shutdown` turns true. Never returns an error: a failed
    /// pass is logged and counted, and the next one starts over on the other
    /// path after a pause that grows with consecutive failures.
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
        if let Some(pool) = self.paths[self.active].pool.take() {
            tokio::spawn(async move { pool.close().await });
        }
        self.peer_columns_checked = None;
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
            self.peer_columns_checked = None;
        }
        let path = &mut self.paths[self.active];
        if path.pool.is_none() {
            let options = PgConnectOptions::from_str(&path.url)
                .map_err(|_| anyhow::anyhow!("invalid peer database URL"))?
                .application_name("qbit-prism-peer-sync")
                .options([
                    ("default_transaction_read_only", "on"),
                    ("statement_timeout", "30s"),
                ])
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

    /// One pass. Public so that tests can drive the sync step by step.
    pub async fn pass(&mut self) -> Result<PassReport> {
        let mut report = PassReport::default();
        if let Some(refusal) = self.check_local_identity().await? {
            report.refused = Some(refusal.clone());
            self.publish(false, Some(&refusal));
            return Ok(report);
        }
        if self.latch.caught_up {
            let now = self.ledger.lineage_evidence().await?;
            if Some(now) != self.latch.evidence {
                tracing::error!(
                    previous = ?self.latch.evidence,
                    now = ?now,
                    "ALERT: this database's system identifier or WAL timeline changed while the \
                     frontend ran: a restore or promotion; the own log is checked against the peer \
                     again before this node serves"
                );
                self.latch = Latch::default();
            }
        }
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
        let facts = bounded(peer::facts(&mut connection)).await?;
        if let Some(refusal) = self.check_peer(&mut connection, &facts).await? {
            tracing::error!(%refusal, "ALERT: the peer sync refuses this peer");
            self.latch_without_peer().await?;
            report.own_log_caught_up = self.latch.caught_up;
            report.refused = Some(refusal.clone());
            self.publish(true, Some(&refusal));
            return Ok(report);
        }
        if !self.latch.caught_up {
            report
                .applied
                .merge(self.recover_own_log(&mut connection).await?);
        }
        report.own_log_caught_up = self.latch.caught_up;
        if self.latch.caught_up {
            let (applied, more) = self.pull_peer(&mut connection, &facts).await?;
            report.applied.merge(applied);
            report.more = more;
        }
        if let Some(metrics) = &self.metrics {
            metrics.record_peer_sync_applied(&report.applied);
        }
        self.publish(true, None);
        Ok(report)
    }

    /// D-9: refuse unless this database is personalised as this node, with
    /// nothing drifted. Checked every `IDENTITY_RECHECK`.
    async fn check_local_identity(&mut self) -> Result<Option<Refusal>> {
        if self
            .identity_checked
            .is_some_and(|at| at.elapsed() < IDENTITY_RECHECK)
        {
            return Ok(self.identity_refusal.clone());
        }
        let refusal = match self.ledger.check_node_identity(self.node).await? {
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
        if let Some(refusal) = &refusal {
            tracing::error!(%refusal, "ALERT: the peer sync does not run");
        }
        if self.local_columns.is_none() && refusal.is_none() {
            self.local_columns = Some(self.ledger.carried_columns().await?);
        }
        self.identity_checked = Some(Instant::now());
        self.identity_refusal = refusal.clone();
        Ok(refusal)
    }

    /// The peer must be the other node, run this cluster's fingerprint (D-6)
    /// and carry the same columns of every copied table.
    async fn check_peer(
        &mut self,
        connection: &mut sqlx::PgConnection,
        facts: &PeerFacts,
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
        if self.ledger.stored_config_fingerprint().await? != facts.config_fingerprint {
            return Ok(Some(Refusal::Fingerprint));
        }
        if self.peer_columns_checked != Some(self.active) {
            let theirs = bounded(CarriedColumns::read(connection)).await?;
            let ours = self
                .local_columns
                .as_ref()
                .context("this database's carried columns were not read")?;
            let differences = ours.differences(&theirs);
            if !differences.is_empty() {
                return Ok(Some(Refusal::Schema(differences)));
            }
            self.peer_columns_checked = Some(self.active);
        }
        Ok(None)
    }

    /// Without the peer, the latch is set unless this database shows
    /// evidence of a rollback since its own log was last verified (D-17).
    async fn latch_without_peer(&mut self) -> Result<()> {
        if self.latch.caught_up {
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
    async fn recover_own_log(&mut self, connection: &mut sqlx::PgConnection) -> Result<Applied> {
        let node = self.node;
        let mut applied = Applied::default();
        let mut clock_ms = None;
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
            clock_ms = clock_ms.max(batch.highest_accepted_at_ms);
            applied.merge(
                self.ledger
                    .apply_share_batch(node, node, &batch, None)
                    .await?,
            );
            ensure_progress(&applied)?;
        }
        loop {
            let (_, held, ..) = self.ledger.highest_held_of(node).await?;
            let blocks = bounded(peer::blocks(
                connection,
                node,
                held.unwrap_or(0),
                None,
                BLOCKS_PER_PASS,
            ))
            .await?;
            if blocks.is_empty() {
                break;
            }
            for block in &blocks {
                applied.merge(self.ledger.apply_block(block, None).await?);
            }
            ensure_progress(&applied)?;
        }
        loop {
            let (_, _, held, _) = self.ledger.highest_held_of(node).await?;
            let batch = bounded(peer::prepared(
                connection,
                node,
                held.unwrap_or(0),
                None,
                self.batch_rows,
            ))
            .await?;
            if batch.count == 0 {
                break;
            }
            applied.merge(self.ledger.apply_prepared(&batch, node, None).await?);
            ensure_progress(&applied)?;
        }
        let roles = bounded(peer::node_roles(connection, node)).await?;
        applied.merge(self.ledger.apply_node_roles(&roles, node).await?);
        ensure_progress(&applied)?;
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

    /// Pull the peer's rows: shares, the journal, then landed blocks and
    /// prepared jobs (D-5). Returns what was applied and whether a batch was
    /// full.
    async fn pull_peer(
        &mut self,
        connection: &mut sqlx::PgConnection,
        facts: &PeerFacts,
    ) -> Result<(Applied, bool)> {
        let peer = self.node.peer();
        let mut applied = Applied::default();
        let mut more = false;
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
        applied.merge(
            self.ledger
                .apply_share_batch(self.node, peer, &batch, Some(SHARES))
                .await?,
        );
        more |= batch.scanned >= self.batch_rows;
        let through = batch.through.unwrap_or(cursor);
        let lag = bounded(peer::shares_beyond(
            connection,
            through,
            peer,
            LAG_COUNT_CAP,
        ))
        .await?;
        self.stream_done(SHARES, u64::try_from(lag).unwrap_or_default());
        // The carry-owner journal.
        let roles = bounded(peer::node_roles(connection, peer)).await?;
        applied.merge(self.ledger.apply_node_roles(&roles, peer).await?);
        self.stream_done(ROLES, 0);
        // The sync_seq streams stop at the mark read at the sync barrier;
        // while a writer holds it, the last mark stands.
        if let Some(position) = bounded(peer::sync_barrier(connection)).await? {
            self.safe_sync_mark = self.safe_sync_mark.max(position);
        }
        let Some(safe) = self.safe_sync_mark else {
            self.stream_done(BLOCKS, 0);
            self.stream_done(PREPARED, 0);
            return Ok((applied, more));
        };
        // Landed blocks, each whole.
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
        for block in &blocks {
            applied.merge(self.ledger.apply_block(block, Some((BLOCKS, peer))).await?);
        }
        if (blocks.len() as i64) < BLOCKS_PER_PASS {
            self.ledger
                .settle_peer_sync_cursor(BLOCKS, peer, safe)
                .await?;
            self.stream_done(BLOCKS, 0);
        } else {
            more = true;
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
        ))
        .await?;
        applied.merge(
            self.ledger
                .apply_prepared(&batch, peer, Some(PREPARED))
                .await?,
        );
        if (batch.count as i64) < self.batch_rows {
            self.ledger
                .settle_peer_sync_cursor(PREPARED, peer, safe)
                .await?;
            self.stream_done(PREPARED, 0);
        } else {
            more = true;
            self.stream_done(PREPARED, batch.count as u64);
        }
        Ok((applied, more))
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
        state.lag_rows = lag_rows;
        state.last_success = Some(chrono::Utc::now());
        if lag_rows == 0 {
            state.caught_up_at = Some(Instant::now());
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

/// An own-log conflict leaves the latch unset.
fn ensure_progress(applied: &Applied) -> Result<()> {
    if applied.total_conflicts() > 0 {
        bail!(
            "own-log recovery met rows this node holds with other content ({:?}); the own log has \
             diverged from the peer's copy of it and this node does not serve until an operator \
             resolves qbit_prism_peer_sync_conflicts",
            applied.conflicts
        );
    }
    Ok(())
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
