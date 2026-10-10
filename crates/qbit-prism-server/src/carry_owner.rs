//! The carry owner guard (PRISM 3.1 dual writer, CONTRACT.md §1, §3 and D-4).
//!
//! Exactly one node of the pair, the carry owner, pays carried balances
//! down; the other builds carry-free work (`ledger/carry.rs`). Two owners
//! could pay one carried balance twice, so ownership is decided from three
//! sources that must agree, and anything short of agreement is carry-free:
//!
//! - **The setting.** `PRISM_CARRY_OWNER`, the static configuration, for the
//!   node `PRISM_NODE_INDEX` names, which must match the database's own
//!   never-copied identity, `qbit_prism_node_identity` (D-9).
//! - **The journal.** `qbit_prism_node_roles`, an append-only log of each
//!   node's ownership claims (`seed`, `release`, `acquire`), copied between
//!   the nodes by peer sync. A node's role is its highest-epoch row.
//! - **The peer.** A live read of the peer's journal through
//!   `PRISM_PEER_DATABASE_URL`, which must show the peer not claiming
//!   ownership, at least once since this process started.
//!
//! The owner keeps paying while the peer is unreachable: the peer can only
//! acquire ownership after it has read this node's `release` row
//! (`carry-owner transfer`), so a dead or cut-off peer can never become a
//! second owner. A node that has not seen the peer since it started cannot
//! know that its own journal is current (a restore from an old backup could
//! have resurrected a claim the pair has since moved), so it waits,
//! carry-free, for one live read. Whenever both nodes claim ownership, both
//! go carry-free and alert.
//!
//! Once the peer has released, only a claim that `carry-owner transfer`
//! made after reading that release pays: transfer is what waits for the
//! release to be buried and checks that every pool block is landed here. Any
//! other claim facing a release (a node re-seeded after a restore, or the
//! survivor of two claims after the operator released the other) stays
//! carry-free and alerts until the operator runs `release` and then
//! `transfer` on it.
//!
//! The journal is append-only, so a node's latest epoch never falls. If it
//! falls under a running frontend, the database was rolled back in a way the
//! peer sync's lineage check (D-17) cannot see, such as a filesystem snapshot
//! on the same WAL timeline, and may have lost a release the peer acted on:
//! carry-free, with an alert, until the journal is back (a restart's own-log
//! check pulls the lost rows back from the peer). A node seeds its
//! first row only once its own log is caught up (D-8) and the peer, read
//! live, holds no row of this node either.
//!
//! Every decision other than paying is safe: carry-free work never reduces a
//! balance (invariant 2), so it only delays carry payments.
use crate::{
    ledger::Ledger,
    metrics::{CarryOwnerState, Metrics, PeerCheckResult},
};
use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, PgConnection, PgPool, Row};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::watch;

pub mod transfer;

/// One row of `qbit_prism_node_roles`, as the guard reads it.
#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct RoleRow {
    pub origin_node: i16,
    pub epoch: i64,
    pub carry_owner: bool,
    pub action: String,
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub detail: serde_json::Value,
}

impl RoleRow {
    /// The chain tip height a `release` or `acquire` row recorded.
    pub fn tip_height(&self) -> Option<u64> {
        self.detail.get("tip_height")?.as_u64()
    }

    /// Whether this is an `acquire` that `carry-owner transfer` wrote after
    /// reading `release`: the peer epoch it recorded is at least the
    /// release's.
    pub fn acquired_after(&self, release: &RoleRow) -> bool {
        self.action == "acquire"
            && self
                .detail
                .get("peer_epoch")
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|epoch| epoch >= release.epoch)
    }
}

/// What the live read of the peer's journal returned.
#[derive(Clone, Debug)]
pub enum PeerRead {
    /// The peer answered: its latest own row, and its copy of this node's
    /// latest row.
    Answered {
        peer: Option<RoleRow>,
        own_at_peer: Option<RoleRow>,
    },
    /// The peer could not be read, for any reason.
    Failed,
}

/// Everything one decision reads.
#[derive(Clone, Debug)]
pub struct GuardInputs {
    /// `PRISM_NODE_INDEX`.
    pub node_index: i16,
    /// The database's own identity, `qbit_prism_node_identity.node_index`.
    pub database_node: Option<i16>,
    /// `PRISM_CARRY_OWNER`.
    pub env_owner: bool,
    /// This node's latest row in its own journal.
    pub own: Option<RoleRow>,
    /// The peer's latest row in this node's synced copy of its journal.
    pub peer_synced: Option<RoleRow>,
    pub peer_read: PeerRead,
}

/// Why work is carry-free. Every reason but `NotOwner` is an alert.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CarryFreeReason {
    /// The setting and the journal agree that this node is not the owner.
    NotOwner,
    /// The database's node identity is missing or is not `PRISM_NODE_INDEX`.
    NodeUnidentified,
    /// This node has no journal row yet.
    NoJournalRow,
    /// `PRISM_CARRY_OWNER` and this node's journal disagree, and the journal
    /// row is not a `release` or `acquire` (a transfer in progress).
    ConfigMismatch,
    /// This node released or acquired ownership through `carry-owner`, and
    /// `PRISM_CARRY_OWNER` has not been changed to match yet.
    SettingPending,
    /// The peer holds a newer row of this node's than this node does: its
    /// journal was rolled back, for example by a restore.
    OwnJournalBehindPeer,
    /// This node's latest journal row is older than one this process has
    /// already read: its database was rolled back under the running
    /// frontend.
    OwnJournalRolledBack,
    /// The peer released ownership, and this node's claim is not an
    /// `acquire` that `carry-owner transfer` wrote after that release, so
    /// nothing has checked that the peer's blocks are landed here.
    ClaimNotVetted,
    /// The peer claims ownership too.
    PeerClaimsOwnership,
    /// The peer answered but has never written a journal row, so it may be
    /// running without the dual-writer rules.
    PeerNotSeeded,
    /// The peer has not been read since this process started.
    PeerUnconfirmed,
}

impl CarryFreeReason {
    pub const ALL: [Self; 11] = [
        Self::NotOwner,
        Self::NodeUnidentified,
        Self::NoJournalRow,
        Self::ConfigMismatch,
        Self::SettingPending,
        Self::OwnJournalBehindPeer,
        Self::OwnJournalRolledBack,
        Self::ClaimNotVetted,
        Self::PeerClaimsOwnership,
        Self::PeerNotSeeded,
        Self::PeerUnconfirmed,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotOwner => "not_owner",
            Self::NodeUnidentified => "node_unidentified",
            Self::NoJournalRow => "no_journal_row",
            Self::ConfigMismatch => "config_mismatch",
            Self::SettingPending => "setting_pending",
            Self::OwnJournalBehindPeer => "own_journal_behind_peer",
            Self::OwnJournalRolledBack => "own_journal_rolled_back",
            Self::ClaimNotVetted => "claim_not_vetted",
            Self::PeerClaimsOwnership => "peer_claims_ownership",
            Self::PeerNotSeeded => "peer_not_seeded",
            Self::PeerUnconfirmed => "peer_unconfirmed",
        }
    }

    /// Whether this state needs an operator: everything except a non-owner
    /// working as configured, an owner waiting for its first peer read, and
    /// a transfer waiting for its setting change (which the loop escalates
    /// once it has lasted [`SETTING_PENDING_ALERT_AFTER`]). The loop also
    /// holds back the alert of the states a fresh start passes through
    /// ([`alerting`]).
    pub const fn alerts(self) -> bool {
        !matches!(
            self,
            Self::NotOwner | Self::PeerUnconfirmed | Self::SettingPending
        )
    }
}

/// What work may do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "mode", content = "reason")]
pub enum CarryDecision {
    Paying,
    CarryFree(CarryFreeReason),
}

impl CarryDecision {
    pub fn paying(self) -> bool {
        self == Self::Paying
    }

    /// The `qbit_prism_carry_owner_state` sample this decision sets.
    pub fn state(self) -> CarryOwnerState {
        match self {
            Self::Paying => CarryOwnerState::Paying,
            Self::CarryFree(reason) => match reason {
                CarryFreeReason::NotOwner => CarryOwnerState::NotOwner,
                CarryFreeReason::NodeUnidentified => CarryOwnerState::NodeUnidentified,
                CarryFreeReason::NoJournalRow => CarryOwnerState::NoJournalRow,
                CarryFreeReason::ConfigMismatch => CarryOwnerState::ConfigMismatch,
                CarryFreeReason::SettingPending => CarryOwnerState::SettingPending,
                CarryFreeReason::OwnJournalBehindPeer => CarryOwnerState::OwnJournalBehindPeer,
                CarryFreeReason::OwnJournalRolledBack => CarryOwnerState::OwnJournalRolledBack,
                CarryFreeReason::ClaimNotVetted => CarryOwnerState::ClaimNotVetted,
                CarryFreeReason::PeerClaimsOwnership => CarryOwnerState::PeerClaimsOwnership,
                CarryFreeReason::PeerNotSeeded => CarryOwnerState::PeerNotSeeded,
                CarryFreeReason::PeerUnconfirmed => CarryOwnerState::PeerUnconfirmed,
            },
        }
    }

    /// Whether this decision needs an operator.
    pub fn alerts(self) -> bool {
        matches!(self, Self::CarryFree(reason) if reason.alerts())
    }
}

/// The decision state one process keeps between reads.
#[derive(Debug, Default)]
pub struct Guard {
    /// A live read since process start showed the peer seeded and not
    /// claiming ownership, and none has shown a claim since.
    peer_confirmed: bool,
    /// The highest epoch of this node's own rows this process has read.
    highest_own_epoch: Option<i64>,
}

impl Guard {
    pub fn peer_confirmed(&self) -> bool {
        self.peer_confirmed
    }

    /// Whether this process has read a row of this node's. A journal that
    /// shows none afterwards was rolled back, and is never seeded again.
    pub fn seen_own_row(&self) -> bool {
        self.highest_own_epoch.is_some()
    }

    /// Decide from one read of every source (module comment).
    pub fn decide(&mut self, inputs: &GuardInputs) -> CarryDecision {
        use CarryFreeReason::*;
        // A node that does not claim ownership now must read the peer again
        // before it pays once it claims again: its journal may have been
        // rolled back meanwhile, which only the peer's copy can show.
        let not_claiming = |guard: &mut Self, reason| {
            guard.peer_confirmed = false;
            CarryDecision::CarryFree(reason)
        };
        if inputs.database_node != Some(inputs.node_index) {
            return not_claiming(self, NodeUnidentified);
        }
        // An append-only journal's latest epoch never falls (module comment).
        let rolled_back = match (&inputs.own, self.highest_own_epoch) {
            (None, Some(_)) => true,
            (Some(own), Some(highest)) => own.epoch < highest,
            (_, None) => false,
        };
        if rolled_back {
            return not_claiming(self, OwnJournalRolledBack);
        }
        let Some(own) = &inputs.own else {
            return not_claiming(self, NoJournalRow);
        };
        self.highest_own_epoch = Some(own.epoch);
        if own.carry_owner != inputs.env_owner {
            return not_claiming(
                self,
                if matches!(own.action.as_str(), "release" | "acquire") {
                    SettingPending
                } else {
                    ConfigMismatch
                },
            );
        }
        if !own.carry_owner {
            return not_claiming(self, NotOwner);
        }
        let (peer_live, read) = match &inputs.peer_read {
            PeerRead::Answered { peer, own_at_peer } => {
                if own_at_peer
                    .as_ref()
                    .is_some_and(|theirs| theirs.epoch > own.epoch)
                {
                    self.peer_confirmed = false;
                    return CarryDecision::CarryFree(OwnJournalBehindPeer);
                }
                (peer.as_ref(), true)
            }
            PeerRead::Failed => (None, false),
        };
        // The newest of the peer's rows this node can see, live or synced.
        let peer = [peer_live, inputs.peer_synced.as_ref()]
            .into_iter()
            .flatten()
            .max_by_key(|row| row.epoch);
        if peer.is_some_and(|row| row.carry_owner) {
            self.peer_confirmed = false;
            return CarryDecision::CarryFree(PeerClaimsOwnership);
        }
        if peer.is_some_and(|row| row.action == "release" && !own.acquired_after(row)) {
            return not_claiming(self, ClaimNotVetted);
        }
        if read {
            if peer.is_none() {
                self.peer_confirmed = false;
                return CarryDecision::CarryFree(PeerNotSeeded);
            }
            self.peer_confirmed = true;
        }
        if self.peer_confirmed {
            CarryDecision::Paying
        } else {
            CarryDecision::CarryFree(PeerUnconfirmed)
        }
    }
}

/// The latest row of each of the two nodes in one journal.
const LATEST_ROLES_SQL: &str = "SELECT DISTINCT ON (origin_node) origin_node,epoch,carry_owner,action,detail FROM qbit_prism_node_roles WHERE origin_node=ANY($1::smallint[]) ORDER BY origin_node,epoch DESC";

/// One node's latest claim of ownership in one journal.
const LATEST_CLAIM_SQL: &str = "SELECT origin_node,epoch,carry_owner,action,detail FROM qbit_prism_node_roles WHERE origin_node=$1 AND carry_owner ORDER BY epoch DESC LIMIT 1";

/// One node's latest row in one journal, every column, as `to_jsonb` gives
/// it: what `transfer` copies (D1's journal tail).
const LATEST_ROW_JSON_SQL: &str = "SELECT to_jsonb(r) FROM qbit_prism_node_roles r WHERE origin_node=$1 ORDER BY epoch DESC LIMIT 1";

/// The database's own, never-copied node identity (D-9).
const NODE_IDENTITY_SQL: &str = "SELECT node_index FROM qbit_prism_node_identity WHERE singleton";

/// The other node of the pair.
pub fn peer_index(node_index: i16) -> i16 {
    1 - node_index
}

/// The latest rows one journal holds for this node and its peer.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct LatestRoles {
    pub own: Option<RoleRow>,
    pub peer: Option<RoleRow>,
}

/// Read the latest row of each node from one journal, local or the peer's.
pub async fn read_latest_roles(
    connection: &mut PgConnection,
    node_index: i16,
) -> sqlx::Result<LatestRoles> {
    let peer = peer_index(node_index);
    let rows = sqlx::query(LATEST_ROLES_SQL)
        .bind(vec![node_index, peer])
        .fetch_all(&mut *connection)
        .await?;
    let mut latest = LatestRoles::default();
    for row in rows {
        let role = role_row(&row)?;
        if role.origin_node == node_index {
            latest.own = Some(role);
        } else {
            latest.peer = Some(role);
        }
    }
    Ok(latest)
}

/// The latest row in one journal, local or the peer's, in which `origin`
/// claimed ownership (a `seed` as owner or an `acquire`).
pub async fn read_latest_claim(
    connection: &mut PgConnection,
    origin: i16,
) -> sqlx::Result<Option<RoleRow>> {
    sqlx::query(LATEST_CLAIM_SQL)
        .bind(origin)
        .fetch_optional(connection)
        .await?
        .as_ref()
        .map(role_row)
        .transpose()
}

fn role_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<RoleRow> {
    Ok(RoleRow {
        origin_node: row.try_get("origin_node")?,
        epoch: row.try_get("epoch")?,
        carry_owner: row.try_get("carry_owner")?,
        action: row.try_get("action")?,
        detail: row.try_get("detail")?,
    })
}

/// The database's node identity, if one was ever set.
pub async fn read_node_identity(connection: &mut PgConnection) -> sqlx::Result<Option<i16>> {
    sqlx::query_scalar(NODE_IDENTITY_SQL)
        .fetch_optional(connection)
        .await
}

/// The epoch of a new journal row: above every row the database holds and
/// at least the wall clock in milliseconds, so a node rebuilt with an empty
/// journal never reuses an epoch its old rows had.
async fn next_epoch(connection: &mut PgConnection) -> sqlx::Result<i64> {
    sqlx::query_scalar("SELECT GREATEST(floor(extract(epoch FROM clock_timestamp())*1000)::bigint,COALESCE(max(epoch),0)+1) FROM qbit_prism_node_roles")
        .fetch_one(connection)
        .await
}

/// Append one journal row for this node. Returns its epoch.
pub async fn append_role(
    connection: &mut PgConnection,
    node_index: i16,
    carry_owner: bool,
    action: &str,
    recorded_by: &str,
    detail: &serde_json::Value,
) -> sqlx::Result<i64> {
    let epoch = next_epoch(connection).await?;
    sqlx::query("INSERT INTO qbit_prism_node_roles(origin_node,epoch,carry_owner,action,recorded_by,detail) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(node_index)
        .bind(epoch)
        .bind(carry_owner)
        .bind(action)
        .bind(recorded_by)
        .bind(detail)
        .execute(connection)
        .await?;
    Ok(epoch)
}

/// Write this node's first journal row from `PRISM_CARRY_OWNER`, unless it
/// has one. Serialized with every other journal write by `SETTLEMENT_LOCK`
/// ([`Ledger::settlement_transaction`]).
pub async fn seed_role(ledger: &Ledger, node_index: i16, carry_owner: bool) -> Result<Option<i64>> {
    let mut tx = ledger.settlement_transaction().await?;
    let latest = read_latest_roles(&mut tx, node_index).await?;
    if latest.own.is_some() {
        tx.rollback().await?;
        return Ok(None);
    }
    let epoch = append_role(
        &mut tx,
        node_index,
        carry_owner,
        "seed",
        &ledger.instance_id,
        &serde_json::json!({"setting": "PRISM_CARRY_OWNER"}),
    )
    .await?;
    tx.commit().await?;
    Ok(Some(epoch))
}

/// A read-only view of the peer's database, through
/// `PRISM_PEER_DATABASE_URL` and then its fallback: its journal for the guard
/// and the carry-owner commands, and its newest work for the broadcaster.
pub struct PeerJournal {
    pools: Vec<PgPool>,
    timeout: Duration,
}

impl PeerJournal {
    pub fn new(urls: &[String], timeout: Duration) -> Result<Self> {
        let pools = urls
            .iter()
            .map(|url| {
                PgPoolOptions::new()
                    .max_connections(1)
                    .min_connections(0)
                    .acquire_timeout(timeout)
                    .connect_lazy(url)
                    .context("PRISM_PEER_DATABASE_URL is not a valid PostgreSQL URL")
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { pools, timeout })
    }

    /// Read the peer's latest rows, trying each URL in order.
    pub async fn read(&self, node_index: i16) -> PeerRead {
        match self
            .first_answer(|mut connection| async move {
                read_latest_roles(&mut connection, node_index).await
            })
            .await
        {
            Some(latest) => PeerRead::Answered {
                peer: latest.peer,
                own_at_peer: latest.own,
            },
            None => PeerRead::Failed,
        }
    }

    /// The latest row in which `origin` claimed ownership in the peer's
    /// journal; `None` if the peer could not be read.
    pub async fn latest_claim(&self, origin: i16) -> Option<Option<RoleRow>> {
        self.first_answer(|mut connection| async move {
            read_latest_claim(&mut connection, origin).await
        })
        .await
    }

    /// How long ago node `origin` published its newest work the peer's
    /// database holds, by the peer's own clock (`ledger::WORK_AGE_SQL`),
    /// read through every URL: the youngest answer, so a lagging path cannot
    /// make the work look older than it is. `Some(None)` when none holds any
    /// work, `None` if no URL answered.
    pub async fn work_age(&self, origin: i16) -> Option<Option<Duration>> {
        let answers = self
            .every_answer(|mut connection| async move {
                sqlx::query_scalar::<_, f64>(crate::ledger::WORK_AGE_SQL)
                    .bind(origin)
                    .fetch_optional(&mut *connection)
                    .await
            })
            .await;
        let ages = answers
            .into_iter()
            .map(|seconds| seconds.map(|seconds| Duration::from_secs_f64(seconds.max(0.))));
        ages.reduce(|youngest, age| match (youngest, age) {
            (Some(known), Some(age)) => Some(known.min(age)),
            (known, age) => known.or(age),
        })
    }

    /// `origin`'s latest row in the peer's journal, every column; `None` if
    /// the peer could not be read.
    pub async fn latest_row_json(&self, origin: i16) -> Option<Option<serde_json::Value>> {
        self.first_answer(|mut connection| async move {
            sqlx::query_scalar(LATEST_ROW_JSON_SQL)
                .bind(origin)
                .fetch_optional(&mut *connection)
                .await
        })
        .await
    }

    /// The first answer to `read` through the peer's URLs, tried in order;
    /// `None` when none answered.
    async fn first_answer<T, Read, Answer>(&self, read: Read) -> Option<T>
    where
        Read: Fn(sqlx::pool::PoolConnection<sqlx::Postgres>) -> Answer,
        Answer: Future<Output = sqlx::Result<T>>,
    {
        for pool in &self.pools {
            if let Some(answer) = self.answer(pool, &read).await {
                return Some(answer);
            }
        }
        None
    }

    /// Every answer to `read` through the peer's URLs, read at once.
    async fn every_answer<T, Read, Answer>(&self, read: Read) -> Vec<T>
    where
        Read: Fn(sqlx::pool::PoolConnection<sqlx::Postgres>) -> Answer,
        Answer: Future<Output = sqlx::Result<T>>,
    {
        futures_util::future::join_all(self.pools.iter().map(|pool| self.answer(pool, &read)))
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// `read` through one URL, bounded by the timeout; `None`, logged at
    /// debug, on any failure.
    async fn answer<T, Read, Answer>(&self, pool: &PgPool, read: &Read) -> Option<T>
    where
        Read: Fn(sqlx::pool::PoolConnection<sqlx::Postgres>) -> Answer,
        Answer: Future<Output = sqlx::Result<T>>,
    {
        let attempt = async { read(pool.acquire().await?).await };
        match tokio::time::timeout(self.timeout, attempt).await {
            Ok(Ok(answer)) => Some(answer),
            Ok(Err(error)) => {
                tracing::debug!(error = %error, "peer database: read failed");
                None
            }
            Err(_) => {
                tracing::debug!("peer database: read timed out");
                None
            }
        }
    }
}

/// How long a finished `release` or `transfer` may wait for its
/// `PRISM_CARRY_OWNER` change before the guard alerts: carry payments stall
/// while neither node pays.
pub const SETTING_PENDING_ALERT_AFTER: Duration = Duration::from_secs(30 * 60);

/// How long after the own-log latch is first set the states every fresh
/// start passes through (no journal row yet, a peer that has not seeded yet)
/// stay quiet: both nodes seed within a few checks of their latches.
pub const STARTUP_ALERT_AFTER: Duration = Duration::from_secs(120);

/// How long after the start those states stay quiet if the own-log latch is
/// never set (the peer sync alerts on its own as well).
pub const STARTUP_ALERT_CEILING: Duration = Duration::from_secs(600);

/// A guard process's start-up: when it started, and when it first saw the
/// own-log latch set. Later changes of the latch do not reopen it.
#[derive(Clone, Copy, Debug)]
pub struct StartUp {
    pub started: std::time::Instant,
    pub first_latched: Option<std::time::Instant>,
}

impl StartUp {
    /// Whether the start-up is still under way: within
    /// [`STARTUP_ALERT_AFTER`] of the first latch, or, with none yet, within
    /// [`STARTUP_ALERT_CEILING`] of the start.
    pub fn under_way(&self) -> bool {
        match self.first_latched {
            Some(latched) => latched.elapsed() < STARTUP_ALERT_AFTER,
            None => self.started.elapsed() < STARTUP_ALERT_CEILING,
        }
    }
}

/// Whether `decision`, held since `since`, alerts now. Most states alert at
/// once, and a pending setting after [`SETTING_PENDING_ALERT_AFTER`]. The
/// states of a fresh start are quiet while the start-up is under way
/// ([`StartUp::under_way`]), and alert whenever they are entered after it.
pub fn alerting(decision: CarryDecision, since: std::time::Instant, start_up: StartUp) -> bool {
    match decision {
        CarryDecision::CarryFree(CarryFreeReason::SettingPending) => {
            since.elapsed() >= SETTING_PENDING_ALERT_AFTER
        }
        CarryDecision::CarryFree(
            CarryFreeReason::NoJournalRow | CarryFreeReason::PeerNotSeeded,
        ) => !start_up.under_way(),
        decision => decision.alerts(),
    }
}

/// What the guard needs from the dual-writer settings.
#[derive(Clone, Debug)]
pub struct CarryOwnerSettings {
    /// `PRISM_NODE_INDEX`.
    pub node_index: i16,
    /// `PRISM_CARRY_OWNER`.
    pub carry_owner: bool,
    /// `PRISM_PEER_DATABASE_URL`, then `PRISM_PEER_DATABASE_URL_FALLBACK`.
    pub peer_urls: Vec<String>,
    /// How often the guard decides.
    pub interval: Duration,
    /// The bound on one live read of the peer.
    pub peer_timeout: Duration,
}

impl CarryOwnerSettings {
    pub const INTERVAL: Duration = Duration::from_secs(2);
    pub const PEER_TIMEOUT: Duration = Duration::from_secs(2);

    /// The guard's settings from a dual-writer configuration; an error in
    /// single-writer mode, which has no carry owner.
    pub fn from_config(config: &crate::config::Config) -> Result<Self> {
        let dual = config
            .dual_writer
            .as_ref()
            .context("the carry owner exists only in dual-writer mode: set PRISM_DUAL_WRITER=1 and the dual-writer settings")?;
        Ok(Self {
            node_index: dual.identity.node.index(),
            carry_owner: dual.identity.carry_owner,
            peer_urls: std::iter::once(dual.peer_database_url.clone())
                .chain(dual.peer_database_url_fallback.clone())
                .collect(),
            interval: Self::INTERVAL,
            peer_timeout: Self::PEER_TIMEOUT,
        })
    }
}

/// One decision from fresh reads: the identity, the local journal (seeding
/// it first if this node may) and the peer. `own_log_caught_up` is the peer
/// sync's own-log latch (D-8).
pub async fn check(
    ledger: &Ledger,
    settings: &CarryOwnerSettings,
    peer: &PeerJournal,
    guard: &mut Guard,
    own_log_caught_up: bool,
    metrics: Option<&Metrics>,
) -> Result<CarryDecision> {
    let mut connection = ledger.pool.acquire().await?;
    let database_node = read_node_identity(&mut connection).await?;
    let mut local = read_latest_roles(&mut connection, settings.node_index).await?;
    drop(connection);
    let identified = database_node == Some(settings.node_index);
    let mut peer_read = None;
    if identified && local.own.is_none() && !guard.seen_own_row() && own_log_caught_up {
        // Seed only a journal that never had a row of this node's: a node
        // restored from a backup older than its first row gets its rows back
        // from the peer (own-log recovery), and must not claim anew. Seeding
        // takes the settlement lock, so only a node with no row pays for it;
        // `seed_role` re-reads under the lock.
        let read = read_peer(peer, settings.node_index, metrics).await;
        if let PeerRead::Answered {
            own_at_peer: None, ..
        } = read
        {
            if let Some(epoch) =
                seed_role(ledger, settings.node_index, settings.carry_owner).await?
            {
                tracing::info!(
                    node_index = settings.node_index,
                    carry_owner = settings.carry_owner,
                    epoch,
                    "carry owner guard: seeded this node's journal from PRISM_CARRY_OWNER"
                );
            }
            let mut connection = ledger.pool.acquire().await?;
            local = read_latest_roles(&mut connection, settings.node_index).await?;
        }
        peer_read = Some(read);
    }
    // Only a node that claims ownership, by setting and journal, needs the
    // peer: nothing the peer says makes any other node pay.
    let claims =
        identified && settings.carry_owner && local.own.as_ref().is_some_and(|own| own.carry_owner);
    let peer_read = match peer_read {
        _ if !claims => PeerRead::Failed,
        Some(read) => read,
        None => read_peer(peer, settings.node_index, metrics).await,
    };
    Ok(guard.decide(&GuardInputs {
        node_index: settings.node_index,
        database_node,
        env_owner: settings.carry_owner,
        own: local.own,
        peer_synced: local.peer,
        peer_read,
    }))
}

/// One live read of the peer's journal, counted.
async fn read_peer(peer: &PeerJournal, node_index: i16, metrics: Option<&Metrics>) -> PeerRead {
    let read = peer.read(node_index).await;
    if let Some(metrics) = metrics {
        metrics.record_carry_owner_peer_check(match read {
            PeerRead::Answered { .. } => PeerCheckResult::Answered,
            PeerRead::Failed => PeerCheckResult::Failed,
        });
    }
    read
}

/// The guard's loop: decide every `interval`, apply the decision to the
/// carry gate, and fence every change with a payout revision bump. A failed
/// local read keeps the gate as it is (nothing can be built without the
/// database), and a pending fence is retried at every turn.
pub async fn run(
    ledger: Arc<Ledger>,
    settings: CarryOwnerSettings,
    own_log: Option<watch::Receiver<crate::peer_sync::PeerSyncStatus>>,
    metrics: Option<Arc<Metrics>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let peer = PeerJournal::new(&settings.peer_urls, settings.peer_timeout)?;
    let mut guard = Guard::default();
    // The decision, since when it holds, and whether it alerted last time.
    let mut current: Option<(CarryDecision, std::time::Instant, bool)> = None;
    let mut start_up = StartUp {
        started: std::time::Instant::now(),
        first_latched: None,
    };
    loop {
        let caught_up = own_log
            .as_ref()
            .is_none_or(|status| status.borrow().own_log_caught_up);
        if caught_up && start_up.first_latched.is_none() {
            start_up.first_latched = Some(std::time::Instant::now());
        }
        match check(
            &ledger,
            &settings,
            &peer,
            &mut guard,
            caught_up,
            metrics.as_deref(),
        )
        .await
        {
            Ok(decision) => {
                let (since, alerted) = match current {
                    Some((previous, since, alerted)) if previous == decision => (since, alerted),
                    _ => (std::time::Instant::now(), false),
                };
                let alert = alerting(decision, since, start_up);
                let changed = current.is_none_or(|(previous, ..)| previous != decision);
                if changed || (alert && !alerted) {
                    log_decision(decision, alert, &settings);
                }
                current = Some((decision, since, alert));
                if let Some(metrics) = &metrics {
                    metrics.record_carry_owner_state(decision.state(), alert);
                }
                if let Err(error) = ledger.set_carry_paying(decision.paying()).await {
                    tracing::warn!(error = %error, paying = decision.paying(), "carry owner guard: the gate moved but its payout revision bump failed; retrying");
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, "carry owner guard: could not read this node's journal; the carry gate is unchanged")
            }
        }
        if ledger.carry_fence_pending() {
            if let Err(error) = ledger.fence_carry_change().await {
                tracing::warn!(error = %error, "carry owner guard: payout revision bump still failing");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(settings.interval) => {}
            _ = shutdown.changed() => return Ok(()),
        }
    }
}

fn log_decision(decision: CarryDecision, alert: bool, settings: &CarryOwnerSettings) {
    let node_index = settings.node_index;
    match decision {
        CarryDecision::Paying => tracing::info!(
            node_index,
            "carry owner guard: this node is the carry owner; work pays carried balances"
        ),
        CarryDecision::CarryFree(CarryFreeReason::SettingPending) if alert => tracing::error!(
            node_index,
            "ALERT: carry owner guard: a carry-owner release or transfer has waited 30 minutes for PRISM_CARRY_OWNER to match this node's journal; carry payments wait until the setting is changed and the frontend restarted"
        ),
        CarryDecision::CarryFree(reason) if alert => tracing::error!(
            node_index,
            reason = reason.as_str(),
            "ALERT: carry owner guard: work is carry-free until an operator resolves the carry owner state; see docs/prism-ledger-ops.md, Carry owner"
        ),
        CarryDecision::CarryFree(reason) => tracing::info!(
            node_index,
            reason = reason.as_str(),
            "carry owner guard: work is carry-free"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(origin_node: i16, epoch: i64, carry_owner: bool, action: &str) -> RoleRow {
        RoleRow {
            origin_node,
            epoch,
            carry_owner,
            action: action.into(),
            detail: serde_json::Value::Null,
        }
    }

    /// An `acquire` that `carry-owner transfer` wrote after reading the peer's
    /// row at `peer_epoch`.
    fn acquire(origin_node: i16, epoch: i64, peer_epoch: i64) -> RoleRow {
        RoleRow {
            detail: serde_json::json!({"peer_epoch": peer_epoch}),
            ..row(origin_node, epoch, true, "acquire")
        }
    }

    fn owner_inputs(peer_read: PeerRead) -> GuardInputs {
        GuardInputs {
            node_index: 0,
            database_node: Some(0),
            env_owner: true,
            own: Some(row(0, 10, true, "seed")),
            peer_synced: Some(row(1, 11, false, "seed")),
            peer_read,
        }
    }

    fn answered(peer: Option<RoleRow>) -> PeerRead {
        PeerRead::Answered {
            peer,
            own_at_peer: Some(row(0, 10, true, "seed")),
        }
    }

    #[test]
    fn the_owner_pays_once_the_peer_answers_and_keeps_paying_when_it_goes_away() {
        let mut guard = Guard::default();
        // Not since this process started: wait.
        assert_eq!(
            guard.decide(&owner_inputs(PeerRead::Failed)),
            CarryDecision::CarryFree(CarryFreeReason::PeerUnconfirmed)
        );
        assert_eq!(
            guard.decide(&owner_inputs(answered(Some(row(1, 11, false, "seed"))))),
            CarryDecision::Paying
        );
        // S3: the peer dies; it cannot acquire without this node's release.
        for _ in 0..3 {
            assert_eq!(
                guard.decide(&owner_inputs(PeerRead::Failed)),
                CarryDecision::Paying
            );
        }
    }

    #[test]
    fn an_unidentified_database_is_carry_free() {
        let mut guard = Guard::default();
        let mut inputs = owner_inputs(answered(Some(row(1, 11, false, "seed"))));
        assert!(guard.decide(&inputs).paying());
        for database_node in [None, Some(1)] {
            inputs.database_node = database_node;
            assert_eq!(
                guard.decide(&inputs),
                CarryDecision::CarryFree(CarryFreeReason::NodeUnidentified)
            );
        }
    }

    #[test]
    fn a_non_owner_is_carry_free_whatever_the_peer_says() {
        let mut guard = Guard::default();
        let mut inputs = owner_inputs(answered(Some(row(0, 10, true, "seed"))));
        inputs.node_index = 1;
        inputs.database_node = Some(1);
        inputs.env_owner = false;
        inputs.own = Some(row(1, 11, false, "seed"));
        for read in [PeerRead::Failed, answered(None), inputs.peer_read.clone()] {
            inputs.peer_read = read;
            assert_eq!(
                guard.decide(&inputs),
                CarryDecision::CarryFree(CarryFreeReason::NotOwner)
            );
        }
    }

    #[test]
    fn the_setting_and_the_journal_must_agree() {
        let mut guard = Guard::default();
        let mut inputs = owner_inputs(answered(Some(row(1, 11, false, "seed"))));
        inputs.env_owner = false;
        assert_eq!(
            guard.decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::ConfigMismatch)
        );
        // Acquired in the journal, but the setting still says no: a transfer
        // waiting for its setting change.
        inputs.own = Some(row(0, 12, true, "acquire"));
        inputs.env_owner = false;
        assert_eq!(
            guard.decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::SettingPending)
        );
        // Released in the journal while the setting still says yes.
        inputs.own = Some(row(0, 12, false, "release"));
        inputs.env_owner = true;
        assert_eq!(
            guard.decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::SettingPending)
        );
        // A seed that disagrees with the setting is a misconfiguration.
        inputs.own = Some(row(0, 12, false, "seed"));
        assert_eq!(
            guard.decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::ConfigMismatch)
        );
        // No row yet, in a process that never read one.
        inputs.own = None;
        assert_eq!(
            Guard::default().decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::NoJournalRow)
        );
    }

    #[test]
    fn both_claiming_is_carry_free_and_needs_a_fresh_confirmation_after() {
        let mut guard = Guard::default();
        assert!(guard
            .decide(&owner_inputs(answered(Some(row(1, 11, false, "seed")))))
            .paying());
        // S5-like misconfiguration: the peer claims ownership as well.
        assert_eq!(
            guard.decide(&owner_inputs(answered(Some(row(1, 20, true, "seed"))))),
            CarryDecision::CarryFree(CarryFreeReason::PeerClaimsOwnership)
        );
        // An unreachable peer afterwards does not restore the old confirmation.
        assert_eq!(
            guard.decide(&owner_inputs(PeerRead::Failed)),
            CarryDecision::CarryFree(CarryFreeReason::PeerUnconfirmed)
        );
        // The operator released the peer. This node's seed was never checked
        // against that release, so it does not pay on it.
        let released = answered(Some(row(1, 21, false, "release")));
        assert_eq!(
            guard.decide(&owner_inputs(released.clone())),
            CarryDecision::CarryFree(CarryFreeReason::ClaimNotVetted)
        );
        // `release` and then `transfer` on this node: an acquire made after
        // the peer's release pays.
        let mut transferred = owner_inputs(released);
        transferred.own = Some(acquire(0, 23, 21));
        assert!(guard.decide(&transferred).paying());
    }

    #[test]
    fn a_node_that_stopped_claiming_needs_a_fresh_peer_read_to_pay_again() {
        let mut guard = Guard::default();
        assert!(guard
            .decide(&owner_inputs(answered(Some(row(1, 11, false, "seed")))))
            .paying());
        // Released: the setting still says owner, the journal does not.
        let mut released = owner_inputs(PeerRead::Failed);
        released.own = Some(row(0, 12, false, "release"));
        assert_eq!(
            guard.decide(&released),
            CarryDecision::CarryFree(CarryFreeReason::SettingPending)
        );
        // Ownership comes back by transfer after the peer's own release,
        // which the sync copied, while the peer is unreachable: the claim
        // was vetted, but the old confirmation does not carry over.
        let mut reacquired = owner_inputs(PeerRead::Failed);
        reacquired.own = Some(acquire(0, 14, 13));
        reacquired.peer_synced = Some(row(1, 13, false, "release"));
        assert_eq!(
            guard.decide(&reacquired),
            CarryDecision::CarryFree(CarryFreeReason::PeerUnconfirmed)
        );
        reacquired.peer_read = answered(Some(row(1, 13, false, "release")));
        assert!(guard.decide(&reacquired).paying());
    }

    #[test]
    fn a_claim_in_the_synced_copy_counts_while_the_peer_is_unreachable() {
        let mut guard = Guard::default();
        assert!(guard
            .decide(&owner_inputs(answered(Some(row(1, 11, false, "seed")))))
            .paying());
        let mut inputs = owner_inputs(PeerRead::Failed);
        inputs.peer_synced = Some(row(1, 30, true, "acquire"));
        assert_eq!(
            guard.decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::PeerClaimsOwnership)
        );
    }

    #[test]
    fn the_newest_peer_row_wins_between_live_and_synced() {
        let mut guard = Guard::default();
        // The synced copy still holds the peer's old claim, but the live read
        // shows its later release, which this node's transfer read.
        let mut inputs = owner_inputs(answered(Some(row(1, 31, false, "release"))));
        inputs.own = Some(acquire(0, 32, 31));
        inputs.peer_synced = Some(row(1, 30, true, "acquire"));
        assert!(guard.decide(&inputs).paying());
        // And the other way round: a newer synced claim beats an older live row.
        inputs.peer_read = answered(Some(row(1, 29, false, "seed")));
        assert_eq!(
            guard.decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::PeerClaimsOwnership)
        );
    }

    #[test]
    fn a_rolled_back_own_journal_is_carry_free() {
        let mut guard = Guard::default();
        // The peer holds this node's release at epoch 12; this node, restored
        // from an older backup, still shows its claim at epoch 10.
        let inputs = owner_inputs(PeerRead::Answered {
            peer: Some(row(1, 13, true, "acquire")),
            own_at_peer: Some(row(0, 12, false, "release")),
        });
        assert_eq!(
            guard.decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::OwnJournalBehindPeer)
        );
    }

    #[test]
    fn a_journal_that_falls_behind_what_this_process_read_is_rolled_back() {
        let mut guard = Guard::default();
        let mut inputs = owner_inputs(answered(Some(row(1, 11, false, "seed"))));
        assert!(guard.decide(&inputs).paying());
        // A release at epoch 12 is read, then lost to a rollback that keeps
        // the WAL timeline while the peer is unreachable: the old claim is
        // back, and nothing but this process saw it go.
        inputs.own = Some(row(0, 12, false, "release"));
        assert_eq!(
            guard.decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::SettingPending)
        );
        let mut rolled_back = owner_inputs(answered(Some(row(1, 11, false, "seed"))));
        for read in [PeerRead::Failed, rolled_back.peer_read.clone()] {
            rolled_back.peer_read = read;
            assert_eq!(
                guard.decide(&rolled_back),
                CarryDecision::CarryFree(CarryFreeReason::OwnJournalRolledBack)
            );
        }
        // A journal emptied by the rollback is not a fresh one.
        rolled_back.own = None;
        assert_eq!(
            guard.decide(&rolled_back),
            CarryDecision::CarryFree(CarryFreeReason::OwnJournalRolledBack)
        );
        assert!(guard.seen_own_row());
        // Own-log recovery brings the release back: the journal leads again.
        assert_eq!(
            guard.decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::SettingPending)
        );
        // A new process has no history to compare with; the peer's copy of
        // this node's journal is then what shows the rollback.
        let mut restarted = Guard::default();
        rolled_back.own = Some(row(0, 10, true, "seed"));
        rolled_back.peer_read = PeerRead::Answered {
            peer: Some(row(1, 11, false, "seed")),
            own_at_peer: Some(row(0, 12, false, "release")),
        };
        assert_eq!(
            restarted.decide(&rolled_back),
            CarryDecision::CarryFree(CarryFreeReason::OwnJournalBehindPeer)
        );
    }

    #[test]
    fn after_a_peer_release_only_an_acquire_that_read_it_pays() {
        let release = row(1, 20, false, "release");
        let mut inputs = owner_inputs(answered(Some(release.clone())));
        // A re-seeded claim, an acquire from before the release, and an
        // acquire with no recorded peer epoch: none was checked against it.
        for own in [
            row(0, 25, true, "seed"),
            acquire(0, 25, 19),
            row(0, 25, true, "acquire"),
        ] {
            inputs.own = Some(own);
            assert_eq!(
                Guard::default().decide(&inputs),
                CarryDecision::CarryFree(CarryFreeReason::ClaimNotVetted)
            );
        }
        // The release only in the synced copy, the peer unreachable: the same.
        let mut unreachable = owner_inputs(PeerRead::Failed);
        unreachable.peer_synced = Some(release);
        let mut guard = Guard::default();
        assert!(guard
            .decide(&owner_inputs(answered(Some(row(1, 11, false, "seed")))))
            .paying());
        assert_eq!(
            guard.decide(&unreachable),
            CarryDecision::CarryFree(CarryFreeReason::ClaimNotVetted)
        );
        // `transfer` read the release: paying, once a live read confirms.
        inputs.own = Some(acquire(0, 25, 20));
        assert!(Guard::default().decide(&inputs).paying());
        unreachable.own = Some(acquire(0, 25, 20));
        assert_eq!(
            Guard::default().decide(&unreachable),
            CarryDecision::CarryFree(CarryFreeReason::PeerUnconfirmed)
        );
    }

    #[test]
    fn an_unseeded_peer_is_not_a_confirmation() {
        let mut guard = Guard::default();
        let mut inputs = owner_inputs(answered(None));
        inputs.peer_synced = None;
        assert_eq!(
            guard.decide(&inputs),
            CarryDecision::CarryFree(CarryFreeReason::PeerNotSeeded)
        );
        // Rows the peer lost (its disk was rebuilt) are still known from the
        // synced copy, so the peer has been seeded.
        inputs.peer_synced = Some(row(1, 11, false, "seed"));
        assert!(guard.decide(&inputs).paying());
    }

    #[test]
    fn the_states_of_a_fresh_start_are_quiet_only_while_it_is_under_way() {
        use std::time::Instant;
        let ago = |seconds| Instant::now() - Duration::from_secs(seconds);
        let start_up = |started, latched: Option<u64>| StartUp {
            started: ago(started),
            first_latched: latched.map(ago),
        };
        for reason in [
            CarryFreeReason::NoJournalRow,
            CarryFreeReason::PeerNotSeeded,
        ] {
            let decision = CarryDecision::CarryFree(reason);
            assert!(decision.alerts());
            // Within two minutes of the first latch: quiet, however long the
            // state has lasted.
            assert!(!alerting(decision, ago(3600), start_up(3600, Some(30))));
            // Past them: the alert, also for a state just entered, so a
            // state that comes and goes cannot keep itself quiet.
            assert!(alerting(decision, ago(1), start_up(3600, Some(121))));
            // No latch yet: quiet for the first ten minutes only.
            assert!(!alerting(decision, ago(300), start_up(300, None)));
            assert!(alerting(decision, ago(1), start_up(601, None)));
        }
        let pending = CarryDecision::CarryFree(CarryFreeReason::SettingPending);
        assert!(!alerting(pending, ago(1799), start_up(3600, Some(3600))));
        assert!(alerting(pending, ago(1801), start_up(1, None)));
        // Every other state alerts, or not, at once, start-up or not.
        for reason in CarryFreeReason::ALL {
            if matches!(
                reason,
                CarryFreeReason::NoJournalRow
                    | CarryFreeReason::PeerNotSeeded
                    | CarryFreeReason::SettingPending
            ) {
                continue;
            }
            let decision = CarryDecision::CarryFree(reason);
            assert_eq!(
                alerting(decision, Instant::now(), start_up(0, None)),
                reason.alerts()
            );
        }
        assert!(!alerting(
            CarryDecision::Paying,
            ago(3600),
            start_up(3600, Some(3600))
        ));
    }

    #[test]
    fn only_a_configured_non_owner_or_a_wait_for_the_peer_is_quiet() {
        let alerting: Vec<_> = CarryFreeReason::ALL
            .into_iter()
            .filter(|reason| reason.alerts())
            .collect();
        assert_eq!(alerting.len(), CarryFreeReason::ALL.len() - 3);
        assert!(!CarryFreeReason::NotOwner.alerts());
        assert!(!CarryFreeReason::PeerUnconfirmed.alerts());
        assert!(!CarryFreeReason::SettingPending.alerts());
        // Every reason has a metric state of its own.
        let states: std::collections::BTreeSet<_> = CarryFreeReason::ALL
            .into_iter()
            .map(|reason| CarryDecision::CarryFree(reason).state())
            .chain([CarryDecision::Paying.state()])
            .collect();
        assert_eq!(states.len(), CarryOwnerState::ALL.len());
    }
}
