//! PostgreSQL failover under load (#554, faults 1 and 2): the primary lost
//! with its asynchronous standby promoted, the fenced planned switch, and a
//! found block mid-landing across the failover.
//!
//! The run's managed cluster streams its standby through a replication link
//! the harness can cut, and every writer (the frontends, through the delay
//! proxy, and the harness's own side pool) reaches the primary through the
//! writer endpoint ([`super::endpoint`]). A failover follows the operator
//! procedure of `docs/prism-ha-reference-architecture.md`: the primary goes,
//! the standby is promoted (`pg_ctl promote` waits until it leaves
//! recovery), and only then is the writer endpoint moved to it. The
//! frontends are never restarted; they reconnect to the same address. A new
//! standby is then built from the new primary, as the operator rebuilds the
//! old one, so the faults after it and the run's replication premise still
//! have one.
//!
//! - `primary-kill` (D3's asynchronous loss): a barrier first shows the
//!   standby has flushed everything the primary had flushed; replication is
//!   then cut, the primary keeps acknowledging for `cut` seconds, and it is
//!   stopped immediately. Every acknowledged share the promoted primary
//!   lacks is listed; none may predate the barrier, and each must be absent
//!   from what the standby had received, which is exactly what promotion
//!   kept.
//! - `primary-switch`: the writer endpoint is fenced first, the standby
//!   replays through the old primary's flush position, and only then is the
//!   primary stopped. Nothing acknowledged may be lost.
//! - `block-failover`: the relay holds the next found block's `submitblock`;
//!   as it arrives replication is cut and the primary stopped, then the call
//!   goes on to the node. With #529's standby wait (the frontends are given
//!   the standby's name), the block's reservation was on the standby before
//!   the call, so the promoted primary holds it and lands the block exactly
//!   once, within 30 s of the promotion (#585).

use super::{
    endpoint::Endpoint,
    rpc_relay::{Arm, Seen},
    CandidateRow, FaultEnv, FaultTools, Spawned,
};
use crate::{
    client::{self, Outcome},
    cluster::{ManagedPostgres, STANDBY_NAME},
};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{atomic::Ordering, Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

/// The managed cluster, shared between the run (which stops it) and the
/// faults that act on it. Every operation on it is a blocking `pg_ctl` or
/// `pg_basebackup`, run under `spawn_blocking`.
pub type Cluster = Arc<Mutex<ManagedPostgres>>;

/// What the database faults act on.
pub struct FailoverControl {
    pub cluster: Cluster,
    /// The writer endpoint every writer reaches the primary through, when
    /// the plan lists a failover.
    pub writer: Option<Arc<Endpoint>>,
    /// The read endpoint the public reader reaches the standby through,
    /// moved to each rebuilt standby as a read-replica address would be.
    pub reader: Option<Arc<Endpoint>>,
}

/// How soon after the promotion each frontend must accept a share on the
/// new primary, and a found block mid-landing must land (#585).
pub const SERVE_BOUND: Duration = Duration::from_secs(30);
/// How long the standby may take to show it flushed the barrier, or to
/// settle after the cut.
const STANDBY_WAIT: Duration = Duration::from_secs(15);
/// How long a rebuilt standby may take to stream.
const REBUILD_WAIT: Duration = Duration::from_secs(120);
/// How long after the promotion the block's landing is watched for, beyond
/// the bound it is held to, so a late landing is measured, not missed.
const LANDING_WATCH: Duration = Duration::from_secs(90);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Async,
    Fenced,
    Block,
}

/// Where the barrier left the standby.
struct Barrier {
    /// Sent just before the primary's flush position was read: a share
    /// acknowledged before this was flushed at or before it.
    at: Instant,
    flush_lsn: String,
}

struct Killed {
    at: Instant,
    /// The old primary's flush position just before it went.
    primary_flush_lsn: Option<String>,
}

/// The standby as promotion found it.
struct Frozen {
    receive_lsn: Option<String>,
    /// WAL the old primary had flushed that the standby never received.
    gap_bytes: Option<i64>,
    /// Of the shares acknowledged so far, the ones the standby holds.
    present: BTreeSet<String>,
    asked: usize,
}

struct Promoted {
    at: Instant,
    moved_at: Instant,
}

enum Stage {
    Start,
    Barrier(Spawned<Result<Barrier>>),
    AwaitingOffer {
        receivers: Vec<oneshot::Receiver<Seen>>,
        deadline: Instant,
    },
    Cut,
    Gap {
        until: Instant,
    },
    Fencing(Spawned<Result<String>>),
    Killing(Spawned<Result<Killed>>),
    Freezing(Spawned<Result<Frozen>>),
    Promoting(Spawned<Result<Promoted>>),
    Rebuilding(Spawned<Result<Instant>>),
    Done,
}

pub struct Failover {
    pub mode: Mode,
    stage: Stage,
    cluster: Cluster,
    writer: Arc<Endpoint>,
    reader: Option<Arc<Endpoint>>,
    cut_seconds: u64,
    started_at: Option<Instant>,
    pub pids_before: Vec<Option<u32>>,
    pub pids_after: Vec<Option<u32>>,
    pub barrier_at: Option<Instant>,
    pub barrier_lsn: Option<String>,
    pub seen: Option<Seen>,
    pub cut_at: Option<Instant>,
    pub fenced_at: Option<Instant>,
    pub fence_flush_lsn: Option<String>,
    pub killed_at: Option<Instant>,
    pub primary_flush_lsn: Option<String>,
    pub released_at: Option<Instant>,
    pub standby_receive_lsn: Option<String>,
    pub gap_bytes: Option<i64>,
    pub frozen_present: BTreeSet<String>,
    /// Whether the standby was read before promotion: without it nothing is
    /// shown to lie in the gap.
    pub frozen_read: bool,
    pub frozen_asked: usize,
    pub promoted_at: Option<Instant>,
    pub moved_at: Option<Instant>,
    pub rebuilt_at: Option<Instant>,
    /// The block's row on the promoted primary, read at once.
    row_after_promotion: Option<Spawned<Result<Option<CandidateRow>>>>,
    pub row_at_promotion: Option<Option<CandidateRow>>,
    landing: Option<Spawned<Result<Option<CandidateRow>>>>,
    next_landing_read: Instant,
    pub landed: Option<(Instant, CandidateRow)>,
    pub last_row: Option<CandidateRow>,
    /// After the recovery window: every block the relay forwarded from the
    /// barrier to the promotion, with its row's state on the new primary
    /// (`None` when the gap took the row).
    candidates: Option<Spawned<Result<BTreeMap<String, String>>>>,
    pub gap_candidates: Option<Vec<(String, Option<String>, bool)>>,
    pub problems: Vec<String>,
}

/// One connection straight to a server, bounded.
async fn connect(url: &str) -> Result<PgConnection> {
    tokio::time::timeout(Duration::from_secs(5), PgConnection::connect(url))
        .await
        .context("connecting timed out")?
        .map_err(Into::into)
}

async fn barrier(primary: String, standby: String) -> Result<Barrier> {
    let mut primary = connect(&primary).await.context("the barrier's primary")?;
    let at = Instant::now();
    let flush_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_flush_lsn()::text")
        .fetch_one(&mut primary)
        .await?;
    let _ = primary.close().await;
    let mut standby = connect(&standby).await.context("the barrier's standby")?;
    let deadline = Instant::now() + STANDBY_WAIT;
    loop {
        let flushed: bool =
            sqlx::query_scalar("SELECT COALESCE(pg_last_wal_receive_lsn() >= $1::pg_lsn, false)")
                .bind(&flush_lsn)
                .fetch_one(&mut standby)
                .await?;
        if flushed {
            break;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "the standby did not flush the primary's position {flush_lsn} within {STANDBY_WAIT:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = standby.close().await;
    Ok(Barrier { at, flush_lsn })
}

/// Fence the writers, then wait for the standby to replay through the old
/// primary's flush position: the planned switch's precondition.
async fn fence_and_catch_up(primary: String, standby: String) -> Result<String> {
    let mut primary = connect(&primary).await.context("the fenced primary")?;
    let flush_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_flush_lsn()::text")
        .fetch_one(&mut primary)
        .await?;
    let _ = primary.close().await;
    let mut standby = connect(&standby).await.context("the standby")?;
    let deadline = Instant::now() + STANDBY_WAIT;
    loop {
        let replayed: bool =
            sqlx::query_scalar("SELECT COALESCE(pg_last_wal_replay_lsn() >= $1::pg_lsn, false)")
                .bind(&flush_lsn)
                .fetch_one(&mut standby)
                .await?;
        if replayed {
            break;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "the standby did not replay through the fenced primary's flush position \
             {flush_lsn} within {STANDBY_WAIT:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = standby.close().await;
    Ok(flush_lsn)
}

/// The primary's flush position (when it still answers), then its loss: an
/// immediate stop, or for the planned switch a fast one, whose walsenders
/// ship everything up to the shutdown checkpoint first.
async fn kill(cluster: Cluster, primary: String, planned: bool) -> Result<Killed> {
    let primary_flush_lsn = match connect(&primary).await {
        Ok(mut connection) => {
            let lsn: Option<String> = sqlx::query_scalar("SELECT pg_current_wal_flush_lsn()::text")
                .fetch_one(&mut connection)
                .await
                .ok();
            let _ = connection.close().await;
            lsn
        }
        Err(_) => None,
    };
    tokio::task::spawn_blocking(move || {
        let mut cluster = cluster.lock().expect("cluster lock");
        if planned {
            cluster.stop_primary_cleanly()
        } else {
            cluster.kill_primary()
        }
    })
    .await
    .context("the kill task")??;
    Ok(Killed {
        at: Instant::now(),
        primary_flush_lsn,
    })
}

/// The standby once it has replayed all it received, and which of `ids` it
/// holds.
async fn freeze(
    standby: String,
    primary_flush: Option<String>,
    ids: Vec<String>,
) -> Result<Frozen> {
    let mut standby = connect(&standby).await.context("the frozen standby")?;
    let deadline = Instant::now() + STANDBY_WAIT;
    loop {
        let settled: bool = sqlx::query_scalar(
            "SELECT COALESCE(pg_last_wal_replay_lsn() >= pg_last_wal_receive_lsn(), true)",
        )
        .fetch_one(&mut standby)
        .await?;
        if settled {
            break;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "the standby did not replay what it received within {STANDBY_WAIT:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let receive_lsn: Option<String> = sqlx::query_scalar("SELECT pg_last_wal_receive_lsn()::text")
        .fetch_one(&mut standby)
        .await?;
    let gap_bytes = match &primary_flush {
        Some(flush) => sqlx::query_scalar(
            "SELECT pg_wal_lsn_diff($1::pg_lsn, pg_last_wal_receive_lsn())::bigint",
        )
        .bind(flush)
        .fetch_one(&mut standby)
        .await
        .ok(),
        None => None,
    };
    let asked = ids.len();
    let present = present_among(&mut standby, ids).await?;
    let _ = standby.close().await;
    Ok(Frozen {
        receive_lsn,
        gap_bytes,
        present,
        asked,
    })
}

/// Which of `ids` the ledger on `connection` holds.
async fn present_among(
    connection: &mut PgConnection,
    ids: Vec<String>,
) -> Result<BTreeSet<String>> {
    let mut present = BTreeSet::new();
    for chunk in ids.chunks(10_000) {
        let found: Vec<String> =
            sqlx::query_scalar("SELECT share_id FROM qbit_share_ledger WHERE share_id = ANY($1)")
                .bind(chunk)
                .fetch_all(&mut *connection)
                .await?;
        present.extend(found);
    }
    Ok(present)
}

/// Promote the standby, then move the writer endpoint to it.
async fn promote(cluster: Cluster, writer: Arc<Endpoint>) -> Result<Promoted> {
    let port = tokio::task::spawn_blocking(move || {
        let mut cluster = cluster.lock().expect("cluster lock");
        cluster.promote_standby()?;
        Ok::<u16, anyhow::Error>(cluster.primary_port)
    })
    .await
    .context("the promotion task")??;
    let at = Instant::now();
    writer.route_to(port);
    Ok(Promoted {
        at,
        moved_at: Instant::now(),
    })
}

/// Build a new standby, wait until it streams, and move the read endpoint
/// to it.
async fn rebuild(cluster: Cluster, side: PgPool, reader: Option<Arc<Endpoint>>) -> Result<Instant> {
    let port = tokio::task::spawn_blocking(move || {
        let mut cluster = cluster.lock().expect("cluster lock");
        cluster.rebuild_standby()?;
        cluster
            .standby_port
            .context("the rebuilt standby has no port")
    })
    .await
    .context("the rebuild task")??;
    let deadline = Instant::now() + REBUILD_WAIT;
    loop {
        let streaming: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_replication \
             WHERE application_name=$1 AND state='streaming')",
        )
        .bind(STANDBY_NAME)
        .fetch_one(&side)
        .await
        .unwrap_or(false);
        if streaming {
            if let Some(reader) = &reader {
                reader.route_to(port);
            }
            return Ok(Instant::now());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "the rebuilt standby did not stream within {REBUILD_WAIT:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

impl Failover {
    pub fn new(mode: Mode, tools: &FaultTools) -> Result<Self> {
        let control = tools
            .failover
            .as_ref()
            .context("a failover fault needs the managed cluster")?;
        let writer = control
            .writer
            .clone()
            .context("a failover fault needs the writer endpoint")?;
        Ok(Self {
            mode,
            stage: Stage::Start,
            cluster: control.cluster.clone(),
            writer,
            reader: control.reader.clone(),
            cut_seconds: tools.cut_seconds,
            started_at: None,
            pids_before: Vec::new(),
            pids_after: Vec::new(),
            barrier_at: None,
            barrier_lsn: None,
            seen: None,
            cut_at: None,
            fenced_at: None,
            fence_flush_lsn: None,
            killed_at: None,
            primary_flush_lsn: None,
            released_at: None,
            standby_receive_lsn: None,
            gap_bytes: None,
            frozen_present: BTreeSet::new(),
            frozen_read: false,
            frozen_asked: 0,
            promoted_at: None,
            moved_at: None,
            rebuilt_at: None,
            row_after_promotion: None,
            row_at_promotion: None,
            landing: None,
            next_landing_read: Instant::now(),
            landed: None,
            last_row: None,
            candidates: None,
            gap_candidates: None,
            problems: Vec::new(),
        })
    }

    fn urls(&self) -> Result<(String, String)> {
        let cluster = self.cluster.lock().expect("cluster lock");
        Ok((
            cluster.primary_url().to_owned(),
            cluster
                .standby_url
                .clone()
                .context("there is no standby to fail over to")?,
        ))
    }

    /// The shares acknowledged since `from`, as the collector has them now.
    fn acknowledged_since(env: &FaultEnv<'_>, from: Instant) -> Vec<String> {
        let collected = env.collected.lock().expect("collector lock");
        collected
            .submits
            .iter()
            .filter(|record| {
                matches!(record.outcome, Outcome::Accepted)
                    && record.responded.is_some_and(|at| at >= from)
            })
            .map(|record| record.share_id.clone())
            .collect()
    }

    /// `Ok(true)` once the standby is promoted, the endpoint moved and a new
    /// standby streams: the fault's injection and removal are one action.
    pub fn poll(&mut self, env: &mut FaultEnv<'_>, tools: &FaultTools) -> Result<bool> {
        self.poll_row_after_promotion();
        loop {
            match &mut self.stage {
                Stage::Start => {
                    self.started_at = Some(Instant::now());
                    self.pids_before = env.frontends.iter().map(|child| child.pid()).collect();
                    let (primary, standby) = self.urls()?;
                    self.stage = if self.mode == Mode::Fenced {
                        self.writer.fence();
                        self.fenced_at = Some(Instant::now());
                        Stage::Fencing(Spawned::spawn(fence_and_catch_up(primary, standby)))
                    } else {
                        Stage::Barrier(Spawned::spawn(barrier(primary, standby)))
                    };
                    return Ok(false);
                }
                Stage::Barrier(task) => {
                    let Some(result) = task.poll() else {
                        return Ok(false);
                    };
                    let barrier = match result {
                        Ok(barrier) => barrier,
                        Err(error) => anyhow::bail!("the failover's barrier: {error:#}"),
                    };
                    self.barrier_at = Some(barrier.at);
                    self.barrier_lsn = Some(barrier.flush_lsn.clone());
                    self.stage = if self.mode == Mode::Block {
                        let receivers = (0..env.frontends.len())
                            .map(|index| tools.relay.arm(index, Arm::Hold))
                            .collect();
                        if let Some(session) = env
                            .sessions
                            .iter()
                            .find(|session| !session.paused.load(Ordering::Relaxed))
                        {
                            let _ = session.control.send(client::Control::ScheduledBlock);
                        }
                        Stage::AwaitingOffer {
                            receivers,
                            deadline: Instant::now() + super::frontend::OFFER_WAIT,
                        }
                    } else {
                        Stage::Cut
                    };
                    continue;
                }
                Stage::AwaitingOffer {
                    receivers,
                    deadline,
                } => {
                    let seen = receivers
                        .iter_mut()
                        .find_map(|receiver| receiver.try_recv().ok());
                    if seen.is_none() && Instant::now() < *deadline {
                        return Ok(false);
                    }
                    tools.relay.disarm_all();
                    if seen.is_none() {
                        self.problems.push(format!(
                            "no found block's submitblock reached the relay within {:?}; the \
                             failover ran without a block mid-landing",
                            super::frontend::OFFER_WAIT
                        ));
                    }
                    self.seen = seen;
                    self.stage = Stage::Cut;
                    continue;
                }
                Stage::Cut => {
                    self.cluster
                        .lock()
                        .expect("cluster lock")
                        .cut_replication()?;
                    self.cut_at = Some(Instant::now());
                    self.stage = if self.mode == Mode::Async {
                        Stage::Gap {
                            until: Instant::now() + Duration::from_secs(self.cut_seconds),
                        }
                    } else {
                        let (primary, _) = self.urls()?;
                        Stage::Killing(Spawned::spawn(kill(
                            self.cluster.clone(),
                            primary,
                            self.mode == Mode::Fenced,
                        )))
                    };
                    return Ok(false);
                }
                Stage::Gap { until } => {
                    if Instant::now() < *until {
                        return Ok(false);
                    }
                    let (primary, _) = self.urls()?;
                    self.stage = Stage::Killing(Spawned::spawn(kill(
                        self.cluster.clone(),
                        primary,
                        self.mode == Mode::Fenced,
                    )));
                    return Ok(false);
                }
                Stage::Fencing(task) => {
                    let Some(result) = task.poll() else {
                        return Ok(false);
                    };
                    match result {
                        Ok(lsn) => self.fence_flush_lsn = Some(lsn.clone()),
                        Err(error) => anyhow::bail!("the fenced switch: {error:#}"),
                    }
                    let (primary, _) = self.urls()?;
                    self.stage = Stage::Killing(Spawned::spawn(kill(
                        self.cluster.clone(),
                        primary,
                        self.mode == Mode::Fenced,
                    )));
                    return Ok(false);
                }
                Stage::Killing(task) => {
                    let Some(result) = task.poll() else {
                        return Ok(false);
                    };
                    let killed = match result {
                        Ok(killed) => killed,
                        Err(error) => anyhow::bail!("stopping the primary: {error:#}"),
                    };
                    self.killed_at = Some(killed.at);
                    self.primary_flush_lsn = killed
                        .primary_flush_lsn
                        .clone()
                        .or_else(|| self.fence_flush_lsn.clone());
                    if self.mode == Mode::Block {
                        // The node gets the block now; the offering frontend
                        // cannot record its answer on a primary that is gone.
                        tools.relay.release_held();
                        self.released_at = Some(Instant::now());
                    }
                    let (_, standby) = self.urls()?;
                    let from = self.started_at.unwrap_or_else(Instant::now);
                    let ids = Self::acknowledged_since(env, from - Duration::from_secs(120));
                    self.stage = Stage::Freezing(Spawned::spawn(freeze(
                        standby,
                        self.primary_flush_lsn.clone(),
                        ids,
                    )));
                    return Ok(false);
                }
                Stage::Freezing(task) => {
                    let Some(result) = task.poll() else {
                        return Ok(false);
                    };
                    match result {
                        Ok(frozen) => {
                            self.standby_receive_lsn = frozen.receive_lsn.clone();
                            self.gap_bytes = frozen.gap_bytes;
                            self.frozen_present = frozen.present.clone();
                            self.frozen_asked = frozen.asked;
                            self.frozen_read = true;
                        }
                        Err(error) => self
                            .problems
                            .push(format!("reading the standby before promotion: {error:#}")),
                    }
                    self.stage = Stage::Promoting(Spawned::spawn(promote(
                        self.cluster.clone(),
                        self.writer.clone(),
                    )));
                    return Ok(false);
                }
                Stage::Promoting(task) => {
                    let Some(result) = task.poll() else {
                        return Ok(false);
                    };
                    let promoted = match result {
                        Ok(promoted) => promoted,
                        Err(error) => anyhow::bail!("promoting the standby: {error:#}"),
                    };
                    self.promoted_at = Some(promoted.at);
                    self.moved_at = Some(promoted.moved_at);
                    if let Some(seen) = &self.seen {
                        self.row_after_promotion = Some(Spawned::spawn(CandidateRow::read(
                            tools.side.clone(),
                            seen.block_hash.clone(),
                        )));
                    }
                    self.stage = Stage::Rebuilding(Spawned::spawn(rebuild(
                        self.cluster.clone(),
                        tools.side.clone(),
                        self.reader.clone(),
                    )));
                    return Ok(false);
                }
                Stage::Rebuilding(task) => {
                    let Some(result) = task.poll() else {
                        return Ok(false);
                    };
                    match result {
                        Ok(at) => self.rebuilt_at = Some(*at),
                        // Not fatal to the run: the premise check at its end
                        // reports the missing standby.
                        Err(error) => self
                            .problems
                            .push(format!("rebuilding the standby: {error:#}")),
                    }
                    self.stage = Stage::Done;
                    return Ok(true);
                }
                Stage::Done => return Ok(true),
            }
        }
    }

    /// The phase ended mid-failover: let a held call go on, and leave the
    /// writers a primary to reach. A primary already lost is replaced by its
    /// standby, as the fault would have done.
    pub fn abandon(&mut self, tools: &FaultTools) {
        tools.relay.release_held();
        let mut cluster = self.cluster.lock().expect("cluster lock");
        if self.killed_at.is_some() && self.promoted_at.is_none() {
            if let Err(error) = cluster.promote_standby() {
                self.problems.push(format!(
                    "promoting the standby at the phase's end: {error:#}"
                ));
            }
        }
        if self.writer.is_fenced() || self.killed_at.is_some() {
            self.writer.route_to(cluster.primary_port);
        }
    }

    fn poll_row_after_promotion(&mut self) {
        if let Some(read) = self.row_after_promotion.as_mut() {
            if let Some(result) = read.poll() {
                match result {
                    Ok(row) => self.row_at_promotion = Some(row.clone()),
                    Err(error) => self.problems.push(format!(
                        "reading the block's row after promotion: {error:#}"
                    )),
                }
                self.row_after_promotion = None;
            }
        }
    }

    /// After the recovery window: the rows of the blocks the relay forwarded
    /// around the failover, and for a block mid-landing, its landing.
    /// `Ok(true)` once both are read.
    pub fn poll_settle(&mut self, env: &FaultEnv<'_>, tools: &FaultTools) -> Result<bool> {
        self.poll_row_after_promotion();
        let landed = self.poll_landing(tools);
        if self.gap_candidates.is_none() {
            let hashes = self.forwarded_around(tools);
            match self.candidates.as_mut() {
                None => {
                    self.candidates = Some(Spawned::spawn(super::backlog::states(
                        tools.side.clone(),
                        hashes,
                    )));
                    return Ok(false);
                }
                Some(read) => {
                    let Some(result) = read.poll() else {
                        return Ok(false);
                    };
                    let rows = match result {
                        Ok(rows) => rows.clone(),
                        Err(error) => {
                            self.problems
                                .push(format!("reading the failover's candidates: {error:#}"));
                            BTreeMap::new()
                        }
                    };
                    let submits = tools.relay.submits();
                    self.gap_candidates = Some(
                        hashes
                            .into_iter()
                            .map(|hash| {
                                let accepted = submits.iter().any(|submit| {
                                    submit.block_hash == hash && submit.node_accepted()
                                });
                                let state = rows.get(&hash).cloned();
                                (hash, state, accepted)
                            })
                            .collect(),
                    );
                    self.pids_after = env.frontends.iter().map(|child| child.pid()).collect();
                }
            }
        }
        Ok(landed && self.gap_candidates.is_some())
    }

    /// Every block the relay forwarded to the node from the barrier (or the
    /// fence) to the endpoint's move, once each.
    fn forwarded_around(&self, tools: &FaultTools) -> Vec<String> {
        let from = self.barrier_at.or(self.fenced_at).or(self.started_at);
        let mut hashes: Vec<String> = tools
            .relay
            .submits()
            .into_iter()
            .filter(|submit| {
                submit.forwarded_at.is_some()
                    && from.is_some_and(|from| submit.at >= from)
                    && self.moved_at.is_some_and(|moved| submit.at <= moved)
            })
            .map(|submit| submit.block_hash)
            .collect();
        hashes.sort();
        hashes.dedup();
        hashes
    }

    /// For a block mid-landing: poll its row on the new primary until it
    /// lands or the watch ends. `true` once settled either way.
    fn poll_landing(&mut self, tools: &FaultTools) -> bool {
        let Some(seen) = &self.seen else {
            return true;
        };
        if self.landed.is_some() {
            return true;
        }
        let Some(promoted) = self.promoted_at else {
            return true;
        };
        if let Some(read) = self.landing.as_mut() {
            let Some(result) = read.poll() else {
                return false;
            };
            match result {
                Ok(Some(row)) => {
                    self.last_row = Some(row.clone());
                    if row.landed() {
                        self.landed = Some((Instant::now(), row.clone()));
                        return true;
                    }
                }
                Ok(None) => self.last_row = None,
                Err(_) => {}
            }
            self.landing = None;
        }
        if Instant::now() >= promoted + LANDING_WATCH {
            return true;
        }
        if Instant::now() >= self.next_landing_read {
            self.next_landing_read = Instant::now() + Duration::from_millis(250);
            self.landing = Some(Spawned::spawn(CandidateRow::read(
                tools.side.clone(),
                seen.block_hash.clone(),
            )));
        }
        false
    }

    /// The shares acknowledged from `from` to the promotion that PostgreSQL
    /// does not hold at the end of the run: after the promotion, every
    /// acknowledgement came from the primary that is still there.
    pub fn lost<'a>(
        &self,
        submits: &'a [client::SubmitRecord],
        committed: &BTreeSet<String>,
        from: Instant,
    ) -> Vec<&'a client::SubmitRecord> {
        self.acknowledged(submits, from)
            .into_iter()
            .filter(|record| !committed.contains(&record.share_id))
            .collect()
    }

    /// The shares acknowledged from `from` to the promotion.
    pub fn acknowledged<'a>(
        &self,
        submits: &'a [client::SubmitRecord],
        from: Instant,
    ) -> Vec<&'a client::SubmitRecord> {
        let Some(to) = self.promoted_at.or(self.killed_at) else {
            return Vec::new();
        };
        submits
            .iter()
            .filter(|record| {
                !record.reoffer
                    && matches!(record.outcome, Outcome::Accepted)
                    && record.responded.is_some_and(|at| at >= from && at <= to)
            })
            .collect()
    }

    /// Whether a lost share lies in the replication gap: acknowledged after
    /// the barrier, and absent from what the standby held when promoted.
    pub fn in_gap(&self, record: &client::SubmitRecord) -> bool {
        self.frozen_read
            && lies_in_gap(
                self.mode,
                self.barrier_at,
                record.responded,
                self.frozen_present.contains(&record.share_id),
            )
    }

    /// The lost shares the verdict proves lie in the replication gap. Only
    /// these are excused from the run's own durability finding; any other
    /// loss is still one.
    pub fn excused(
        &self,
        submits: &[client::SubmitRecord],
        committed: &BTreeSet<String>,
        from: Instant,
    ) -> BTreeSet<String> {
        self.lost(submits, committed, from)
            .into_iter()
            .filter(|record| self.in_gap(record))
            .map(|record| record.share_id.clone())
            .collect()
    }

    /// The window in which a share's commit could have met the loss of the
    /// primary: its answers may honestly be `ledger-outcome-unknown`.
    pub fn outage(&self) -> Option<(Instant, Instant)> {
        // From the primary's loss (or, for the planned switch, the fence):
        // before it the primary answers, and an unknown outcome then is not
        // this fault's.
        let from = self.fenced_at.or(self.killed_at)?;
        let to = self.moved_at.unwrap_or_else(Instant::now) + SERVE_BOUND;
        Some((from, to))
    }

    pub fn evidence(
        &self,
        origin: Instant,
        lost: &[&client::SubmitRecord],
        checked: usize,
    ) -> Value {
        let at = |instant: Option<Instant>| {
            instant.map(|at| at.saturating_duration_since(origin).as_secs_f64())
        };
        let excused = lost.iter().filter(|record| self.in_gap(record)).count();
        let lost: Vec<Value> = lost
            .iter()
            .map(|record| {
                json!({
                    "share_id": record.share_id,
                    "session": record.session,
                    "frontend": record.frontend,
                    "acknowledged_after_seconds": at(record.responded),
                    "acknowledged_after_the_barrier": self.barrier_at
                        .zip(record.responded)
                        .map(|(barrier, ack)| ack > barrier),
                    "on_the_standby_at_promotion": self.frozen_present.contains(&record.share_id),
                })
            })
            .collect();
        json!({
            "mode": match self.mode {
                Mode::Async => "async loss",
                Mode::Fenced => "fenced switch",
                Mode::Block => "block mid-landing",
            },
            "barrier_after_seconds": at(self.barrier_at),
            "barrier_flush_lsn": self.barrier_lsn,
            "replication_cut_after_seconds": at(self.cut_at),
            "writers_fenced_after_seconds": at(self.fenced_at),
            "primary_stopped_after_seconds": at(self.killed_at),
            "old_primary_flush_lsn": self.primary_flush_lsn,
            "standby_receive_lsn_at_promotion": self.standby_receive_lsn,
            "replication_gap_bytes": self.gap_bytes,
            "promoted_after_seconds": at(self.promoted_at),
            "endpoint_moved_after_seconds": at(self.moved_at),
            "standby_rebuilt_after_seconds": at(self.rebuilt_at),
            "offer_held": self.seen.as_ref().map(|seen| json!({
                "frontend": seen.frontend,
                "block_hash": seen.block_hash,
            })),
            "held_call_released_after_seconds": at(self.released_at),
            "block_row_at_promotion": self.row_at_promotion,
            "block_landed": self.landed.as_ref().map(|(_, row)| row),
            "block_landed_after_promotion_seconds": self.landed.as_ref().zip(self.promoted_at)
                .map(|((landed, _), promoted)| landed.saturating_duration_since(promoted).as_secs_f64()),
            "block_last_row": self.last_row,
            "acknowledged_shares_checked": checked,
            "acknowledged_shares_lost": lost.len(),
            "lost_shares": lost,
            "excused_as_the_replication_gap": excused,
            "blocks_forwarded_around_the_failover": self.gap_candidates.as_ref().map(|rows| rows
                .iter()
                .map(|(hash, state, accepted)| json!({
                    "block_hash": hash,
                    "state_on_the_new_primary": state,
                    "node_accepted": accepted,
                }))
                .collect::<Vec<_>>()),
            "gap_lost_candidates": self.gap_candidates.as_ref().map(|rows| rows
                .iter()
                .filter(|(_, state, _)| state.is_none())
                .count()),
            "frontend_pids_before": self.pids_before,
            "frontend_pids_after": self.pids_after,
            "problems": self.problems,
        })
    }
}

/// D3's loss policy for one lost share: an asynchronous failover may lose a
/// share acknowledged after the barrier (the last moment the standby was
/// shown to hold everything the primary had flushed) that the standby did
/// not hold when it was promoted. A fenced switch may lose nothing.
pub fn lies_in_gap(
    mode: Mode,
    barrier: Option<Instant>,
    answered: Option<Instant>,
    on_standby: bool,
) -> bool {
    mode != Mode::Fenced
        && !on_standby
        && barrier
            .zip(answered)
            .is_some_and(|(barrier, answered)| answered > barrier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_share_acknowledged_after_the_barrier_and_absent_from_the_standby_is_the_gap() {
        let barrier = Instant::now();
        let before = barrier - Duration::from_millis(1);
        let after = barrier + Duration::from_millis(1);
        for mode in [Mode::Async, Mode::Block] {
            assert!(lies_in_gap(mode, Some(barrier), Some(after), false));
            assert!(
                !lies_in_gap(mode, Some(barrier), Some(before), false),
                "before the barrier"
            );
            assert!(
                !lies_in_gap(mode, Some(barrier), Some(barrier), false),
                "at the barrier"
            );
            assert!(
                !lies_in_gap(mode, Some(barrier), Some(after), true),
                "promotion dropped it"
            );
            assert!(
                !lies_in_gap(mode, None, Some(after), false),
                "no barrier was shown"
            );
            assert!(
                !lies_in_gap(mode, Some(barrier), None, false),
                "never answered"
            );
        }
        assert!(!lies_in_gap(
            Mode::Fenced,
            Some(barrier),
            Some(after),
            false
        ));
    }
}
