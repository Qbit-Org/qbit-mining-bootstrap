//! A full disk on the primary's WAL volume under load (#554, fault 5).
//!
//! A run whose plan lists `wal-disk-full` starts its managed primary with
//! `pg_wal` on a small fuse2fs volume, #575's `DiskFullInjector` (included
//! from the server's test support, so there is one injector), with 1 MiB
//! segments that are never recycled ([`crate::cluster::WAL_VOLUME_SETTINGS`]).
//! The fault fills the volume and forces segment switches until PostgreSQL
//! cannot create the next segment: a `PANIC` on the WAL write and every
//! backend gone. Its crash recovery may find room again (the segments a
//! checkpoint no longer needs are deleted, never recycled), so while it holds
//! the fault for `hold` seconds, with the sessions mining, it fills the
//! volume again each time PostgreSQL is seen accepting connections: the disk
//! stays full, and PostgreSQL goes down again as soon as it writes. It then
//! frees the volume and starts the primary if it exited, as its supervisor
//! would. The frontends are never restarted.
//!
//! When PostgreSQL was down is read from its own log (stamped in UTC): from
//! each `PANIC` to the next "ready to accept connections". The verdict:
//! nothing acknowledged is lost, nothing is acknowledged inside one of those
//! intervals, every frontend accepts shares again without a restart,
//! and every frontend's `/metrics` showed the condition of #575's paging rule
//! for a database outage, `PrismBlockCandidateMetricsUnavailable`
//! (`qbit_prism_collector_available{collector="database"} == 0`).

#[path = "../../../qbit-prism-server/tests/support/live_host_tools.rs"]
mod host_tools;

#[allow(dead_code)]
#[path = "../../../qbit-prism-server/tests/support/disk_full_injector.rs"]
mod disk_full_injector;

pub(crate) use disk_full_injector::DiskFullInjector;

use super::{failover::Cluster, FaultEnv, FaultTools, Spawned};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

/// The fault volume's size: normal running keeps at most `max_wal_size`
/// (128 MB) and a checkpoint's worth more on it.
pub const WAL_VOLUME_MIB: u64 = 1024;
/// How long the fill may take to bring PostgreSQL down.
pub const PANIC_WAIT: Duration = Duration::from_secs(60);
/// How long PostgreSQL may take to accept connections again once the volume
/// is freed (its crash recovery), before the fault fails.
pub const RESTART_WAIT: Duration = Duration::from_secs(120);
/// A share answered this long after PostgreSQL's `PANIC` may still have been
/// committed before it: its answer was on its way.
pub const ANSWER_GRACE: Duration = Duration::from_millis(250);
/// The database collector's cycle: its gauge may show an outage this long
/// after PostgreSQL returned.
pub const COLLECTOR_LAG: Duration = Duration::from_secs(12);
/// The #575 paging rule's input: the database collector's availability.
pub const COLLECTOR_GAUGE: &str = "collector_available";
pub const COLLECTOR_LABEL: &str = "collector=\"database\"";

/// Samples each frontend's `collector_available{collector="database"}`
/// every second.
type CollectorSamples = Arc<Mutex<Vec<(String, Instant, Option<f64>)>>>;

struct CollectorSampler {
    samples: CollectorSamples,
    task: JoinHandle<()>,
}

impl Drop for CollectorSampler {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CollectorSampler {
    fn start(targets: Vec<(String, String)>) -> Self {
        let samples: CollectorSamples = Arc::new(Mutex::new(Vec::new()));
        let task = {
            let samples = samples.clone();
            tokio::spawn(async move {
                let Ok(client) = reqwest::Client::builder()
                    .timeout(Duration::from_secs(2))
                    .build()
                else {
                    return;
                };
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tick.tick().await;
                    for (instance, url) in &targets {
                        let at = Instant::now();
                        let value = match client.get(url).send().await {
                            Ok(response) => response.text().await.ok().and_then(|text| {
                                super::metric_sum(&text, COLLECTOR_GAUGE, Some(COLLECTOR_LABEL))
                            }),
                            Err(_) => None,
                        };
                        samples.lock().expect("collector samples lock").push((
                            instance.clone(),
                            at,
                            value,
                        ));
                    }
                }
            })
        };
        Self { samples, task }
    }

    fn samples(&self) -> Vec<(String, Instant, Option<f64>)> {
        self.samples.lock().expect("collector samples lock").clone()
    }
}

enum Stage {
    Start,
    Filling(Spawned<Result<u64>>),
    AwaitingPanic { deadline: Instant },
    Down,
    Freeing(Spawned<Result<Restarted>>),
    Done,
}

/// What freeing the volume took.
struct Restarted {
    /// Whether the postmaster had exited and was started again.
    restarted: bool,
    /// When PostgreSQL accepted a connection again.
    up_at: Instant,
}

pub struct WalDiskFull {
    stage: Stage,
    cluster: Cluster,
    direct_url: String,
    probe: Option<Spawned<bool>>,
    next_probe: Instant,
    sampler: Option<CollectorSampler>,
    /// A fill after PostgreSQL came back during the hold.
    refill: Option<Spawned<Result<u64>>>,
    /// The segment switches after a fill; one at a time.
    switches: Option<JoinHandle<()>>,
    /// Fills during the hold, each after PostgreSQL was seen accepting.
    pub refills: usize,
    /// One instant on both clocks, to place the log's lines.
    anchor: (Instant, chrono::DateTime<chrono::Utc>),
    log: Option<std::path::PathBuf>,
    /// When PostgreSQL was down, from its log: each `PANIC` to the next
    /// "ready to accept connections" (or the fault's end).
    pub down_intervals: Vec<(Instant, Instant)>,
    pub ballast_bytes: Option<u64>,
    pub filled_at: Option<Instant>,
    /// When the harness first saw PostgreSQL refuse a connection.
    pub down_at: Option<Instant>,
    pub freed_at: Option<Instant>,
    pub up_at: Option<Instant>,
    pub restarted: Option<bool>,
    pub pids_before: Vec<Option<u32>>,
    pub pids_after: Vec<Option<u32>>,
    pub collector: Vec<(String, Instant, Option<f64>)>,
    pub problems: Vec<String>,
}

/// One probe of whether PostgreSQL accepts a connection and a write-free
/// query, bounded, so a hung server reads as down.
async fn accepts(url: String) -> bool {
    let attempt = async {
        let mut connection = PgConnection::connect(&url).await.ok()?;
        let ok = sqlx::query("SELECT 1")
            .execute(&mut connection)
            .await
            .is_ok();
        let _ = connection.close().await;
        Some(ok)
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(2), attempt).await,
        Ok(Some(true))
    )
}

/// Force segment switches until the server cannot write the next segment.
/// Errors are the point: the connection dies with the server.
async fn force_switches(url: String, until: Instant) {
    let Ok(mut connection) = PgConnection::connect(&url).await else {
        return;
    };
    while Instant::now() < until {
        if sqlx::query("SELECT pg_switch_wal()")
            .execute(&mut connection)
            .await
            .is_err()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

impl WalDiskFull {
    pub fn new(tools: &FaultTools) -> Result<Self> {
        let cluster = tools
            .failover
            .as_ref()
            .map(|failover| failover.cluster.clone())
            .context("wal-disk-full needs the managed cluster")?;
        Ok(Self {
            stage: Stage::Start,
            cluster,
            direct_url: String::new(),
            probe: None,
            next_probe: Instant::now(),
            sampler: None,
            refill: None,
            switches: None,
            refills: 0,
            anchor: (Instant::now(), chrono::Utc::now()),
            log: None,
            down_intervals: Vec::new(),
            ballast_bytes: None,
            filled_at: None,
            down_at: None,
            freed_at: None,
            up_at: None,
            restarted: None,
            pids_before: Vec::new(),
            pids_after: Vec::new(),
            collector: Vec::new(),
            problems: Vec::new(),
        })
    }

    /// `Ok(true)` once PostgreSQL is down.
    pub fn poll_inject(&mut self, env: &mut FaultEnv<'_>) -> Result<bool> {
        if matches!(self.stage, Stage::Start) {
            self.pids_before = env.frontends.iter().map(|child| child.pid()).collect();
            let (url, on_volume, log) = {
                let cluster = self.cluster.lock().expect("cluster lock");
                (
                    cluster.primary_url().to_owned(),
                    cluster.wal_on_volume(),
                    cluster.primary_log_path(),
                )
            };
            self.log = Some(log);
            self.anchor = (Instant::now(), chrono::Utc::now());
            if !on_volume {
                self.problems.push(
                    "the primary's WAL is not on the fault volume (a promotion moved the \
                     primary off it); nothing was filled"
                        .into(),
                );
                self.stage = Stage::Done;
                return Ok(true);
            }
            self.direct_url = url;
            self.sampler = Some(CollectorSampler::start(
                env.frontends
                    .iter()
                    .map(|child| (child.spec.instance_id.clone(), child.metrics_url()))
                    .collect(),
            ));
            let cluster = self.cluster.clone();
            self.stage = Stage::Filling(Spawned::spawn(async move {
                tokio::task::spawn_blocking(move || {
                    cluster.lock().expect("cluster lock").fill_wal_volume()
                })
                .await
                .context("the fill task")?
            }));
            return Ok(false);
        }
        match &mut self.stage {
            Stage::Filling(fill) => {
                let Some(result) = fill.poll() else {
                    return Ok(false);
                };
                match result {
                    Ok(bytes) => self.ballast_bytes = Some(*bytes),
                    Err(error) => {
                        self.problems
                            .push(format!("filling the WAL volume: {error:#}"));
                        self.stage = Stage::Done;
                        return Ok(true);
                    }
                }
                self.filled_at = Some(Instant::now());
                let deadline = Instant::now() + PANIC_WAIT;
                self.switches = Some(tokio::spawn(force_switches(
                    self.direct_url.clone(),
                    deadline,
                )));
                self.stage = Stage::AwaitingPanic { deadline };
                Ok(false)
            }
            Stage::AwaitingPanic { deadline } => {
                let deadline = *deadline;
                if let Some(down) = self.poll_probe() {
                    if !down {
                        self.down_at = Some(Instant::now());
                        self.stage = Stage::Down;
                        return Ok(true);
                    }
                }
                if Instant::now() >= deadline {
                    self.problems.push(format!(
                        "PostgreSQL still accepted connections {PANIC_WAIT:?} after its WAL \
                         volume was full"
                    ));
                    self.stage = Stage::Down;
                    return Ok(true);
                }
                Ok(false)
            }
            Stage::Start | Stage::Down | Stage::Freeing(_) | Stage::Done => Ok(true),
        }
    }

    /// While the fault holds: fill the volume again whenever PostgreSQL is
    /// seen accepting connections, so the disk stays full.
    pub fn poll_hold(&mut self) {
        if !matches!(self.stage, Stage::Down) {
            return;
        }
        if let Some(refill) = self.refill.as_mut() {
            let Some(result) = refill.poll() else {
                return;
            };
            if let Err(error) = result {
                self.problems
                    .push(format!("filling the WAL volume again: {error:#}"));
            }
            self.refill = None;
            // Its segment switches bring the server down again.
            if self.switches.as_ref().is_none_or(JoinHandle::is_finished) {
                self.switches = Some(tokio::spawn(force_switches(
                    self.direct_url.clone(),
                    Instant::now() + Duration::from_secs(5),
                )));
            }
        }
        if self.poll_probe() == Some(true) && self.refill.is_none() {
            self.refills += 1;
            let cluster = self.cluster.clone();
            self.refill = Some(Spawned::spawn(async move {
                tokio::task::spawn_blocking(move || {
                    cluster.lock().expect("cluster lock").fill_wal_volume()
                })
                .await
                .context("the refill task")?
            }));
        }
    }

    /// The next probe's answer (`true` when PostgreSQL accepted), polled
    /// every 100 ms.
    fn poll_probe(&mut self) -> Option<bool> {
        if let Some(probe) = self.probe.as_mut() {
            let answer = *probe.poll()?;
            self.probe = None;
            return Some(answer);
        }
        if Instant::now() >= self.next_probe {
            self.next_probe = Instant::now() + Duration::from_millis(100);
            self.probe = Some(Spawned::spawn(accepts(self.direct_url.clone())));
        }
        None
    }

    /// Free the volume and bring PostgreSQL back. `Ok(true)` once it accepts
    /// connections again; an error, with the primary's log, when it cannot
    /// be brought back, which ends the run: nothing after it could be
    /// measured.
    pub fn poll_remove(&mut self, env: &mut FaultEnv<'_>) -> Result<bool> {
        match &mut self.stage {
            // A refill still running holds the cluster; the volume is freed
            // once it is done.
            Stage::Down
                if self
                    .refill
                    .as_mut()
                    .is_some_and(|refill| refill.poll().is_none()) =>
            {
                Ok(false)
            }
            Stage::Down => {
                self.refill = None;
                self.freed_at = Some(Instant::now());
                let cluster = self.cluster.clone();
                let url = self.direct_url.clone();
                self.stage = Stage::Freeing(Spawned::spawn(async move {
                    let freed = {
                        let cluster = cluster.clone();
                        tokio::task::spawn_blocking(move || {
                            let guard = cluster.lock().expect("cluster lock");
                            guard.free_wal_volume()?;
                            Ok::<bool, anyhow::Error>(guard.primary_running())
                        })
                        .await
                        .context("the free task")??
                    };
                    let mut restarted = false;
                    if !freed {
                        // The postmaster exited: its supervisor starts it.
                        let cluster = cluster.clone();
                        tokio::task::spawn_blocking(move || {
                            cluster.lock().expect("cluster lock").restart_primary()
                        })
                        .await
                        .context("the restart task")?
                        .context("starting PostgreSQL again after the WAL volume was freed")?;
                        restarted = true;
                    }
                    let deadline = Instant::now() + RESTART_WAIT;
                    loop {
                        if accepts(url.clone()).await {
                            return Ok(Restarted {
                                restarted,
                                up_at: Instant::now(),
                            });
                        }
                        if Instant::now() >= deadline {
                            let tail = cluster
                                .lock()
                                .expect("cluster lock")
                                .primary_log_tail()
                                .unwrap_or_default();
                            anyhow::bail!(
                                "PostgreSQL did not accept connections {RESTART_WAIT:?} after \
                                 its WAL volume was freed; the last lines of its log:\n{tail}"
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }));
                Ok(false)
            }
            Stage::Freeing(freeing) => {
                let Some(result) = freeing.poll() else {
                    return Ok(false);
                };
                match result {
                    Ok(done) => {
                        self.restarted = Some(done.restarted);
                        self.up_at = Some(done.up_at);
                        self.read_log();
                    }
                    Err(error) => {
                        // Loud: the run cannot go on without its database.
                        anyhow::bail!("wal-disk-full: {error:#}");
                    }
                }
                self.pids_after = env.frontends.iter().map(|child| child.pid()).collect();
                self.stage = Stage::Done;
                Ok(true)
            }
            Stage::Start | Stage::Filling(_) | Stage::AwaitingPanic { .. } => {
                // Removed before it took effect: free the volume anyway.
                let _ = self.cluster.lock().expect("cluster lock").free_wal_volume();
                self.stage = Stage::Done;
                Ok(true)
            }
            Stage::Done => {
                let _ = self.cluster.lock().expect("cluster lock").free_wal_volume();
                Ok(true)
            }
        }
    }

    /// Place PostgreSQL's own record of when it was down on the harness's
    /// clock: each `PANIC` after the fill starts an interval, and the next
    /// "ready to accept connections" ends it.
    fn read_log(&mut self) {
        let Some(path) = &self.log else {
            return;
        };
        let text = match std::fs::read(path) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(error) => {
                self.problems.push(format!(
                    "reading PostgreSQL's log {}: {error}",
                    path.display()
                ));
                return;
            }
        };
        // From the fill's start: PostgreSQL can PANIC while the ballast is
        // still being written.
        let (anchor, wall) = self.anchor;
        let since = anchor;
        let end = self.up_at.unwrap_or_else(Instant::now);
        self.down_intervals = down_intervals(&text, anchor, wall, since, end);
    }

    /// Stop sampling `/metrics` once the recovery window has closed.
    pub fn stop_sampling(&mut self) {
        if let Some(sampler) = self.sampler.take() {
            self.collector = sampler.samples();
        }
    }

    /// Free the volume at the phase's end, whatever stage the fault reached.
    pub fn abandon(&mut self) {
        let _ = self.cluster.lock().expect("cluster lock").free_wal_volume();
        self.stop_sampling();
    }

    pub fn collector_samples(&self) -> Vec<(String, Instant, Option<f64>)> {
        match &self.sampler {
            Some(sampler) => sampler.samples(),
            None => self.collector.clone(),
        }
    }

    /// When the fill began, the start of every window the fault answers for.
    pub fn fill_started_at(&self) -> Option<Instant> {
        (!matches!(self.stage, Stage::Start)).then_some(self.anchor.0)
    }

    pub fn evidence(&self, origin: Instant) -> Value {
        let at = |instant: Option<Instant>| {
            instant.map(|at| at.saturating_duration_since(origin).as_secs_f64())
        };
        json!({
            "wal_volume_mib": WAL_VOLUME_MIB,
            "ballast_bytes": self.ballast_bytes,
            "filled_after_seconds": at(self.filled_at),
            "postgres_down_after_seconds": at(self.down_at),
            "freed_after_seconds": at(self.freed_at),
            "postgres_up_after_seconds": at(self.up_at),
            "outage_seconds": match (self.down_at, self.up_at) {
                (Some(down), Some(up)) => Some(up.saturating_duration_since(down).as_secs_f64()),
                _ => None,
            },
            "postmaster_restarted": self.restarted,
            "refills_while_holding": self.refills,
            "down_intervals_from_the_postgres_log": self.down_intervals.iter().map(|(from, to)| json!({
                "from_seconds": at(Some(*from)),
                "to_seconds": at(Some(*to)),
            })).collect::<Vec<_>>(),
            "down_seconds_from_the_postgres_log": self.down_intervals.iter()
                .map(|(from, to)| to.saturating_duration_since(*from).as_secs_f64())
                .sum::<f64>(),
            "problems": self.problems,
        })
    }
}

/// The intervals PostgreSQL's log says it was down, on the monotonic clock:
/// each `PANIC` at or after `since` to the next "ready to accept
/// connections", or `end` when none follows. Lines carry `%m` in UTC
/// (`2026-09-30 22:04:51.586 UTC`); `anchor` and `wall` are one instant on
/// both clocks.
pub fn down_intervals(
    text: &str,
    anchor: Instant,
    wall: chrono::DateTime<chrono::Utc>,
    since: Instant,
    end: Instant,
) -> Vec<(Instant, Instant)> {
    let place = |line: &str| -> Option<Instant> {
        let stamp = line.get(..23)?;
        let at = chrono::NaiveDateTime::parse_from_str(stamp, "%Y-%m-%d %H:%M:%S%.3f")
            .ok()?
            .and_utc();
        let offset = at.signed_duration_since(wall).to_std().ok();
        match offset {
            Some(after) => Some(anchor + after),
            None => anchor.checked_sub(wall.signed_duration_since(at).to_std().ok()?),
        }
    };
    let mut intervals = Vec::new();
    let mut down: Option<Instant> = None;
    for line in text.lines() {
        let Some(at) = place(line) else {
            continue;
        };
        if at < since {
            continue;
        }
        if line.contains(" PANIC: ") && down.is_none() {
            down = Some(at);
        } else if line.contains("database system is ready to accept connections") {
            if let Some(from) = down.take() {
                intervals.push((from, at));
            }
        }
    }
    if let Some(from) = down {
        intervals.push((from, end.max(from)));
    }
    intervals
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_down_intervals_run_from_each_panic_to_the_next_ready() {
        let anchor = Instant::now();
        let wall = chrono::NaiveDateTime::parse_from_str(
            "2026-09-30 22:04:50.000",
            "%Y-%m-%d %H:%M:%S%.3f",
        )
        .unwrap()
        .and_utc();
        let log = "\
2026-09-30 22:04:40.000 UTC [1] LOG:  database system is ready to accept connections
2026-09-30 22:04:51.500 UTC [2] PANIC:  could not write to log file 00000001: No space left on device
2026-09-30 22:04:51.600 UTC [1] LOG:  all server processes terminated; reinitializing
2026-09-30 22:04:52.100 UTC [3] PANIC:  could not write to log file 00000001: No space left on device
2026-09-30 22:04:53.000 UTC [1] LOG:  database system is ready to accept connections
2026-09-30 22:04:55.250 UTC [4] PANIC:  could not write to log file 00000002: No space left on device
";
        let seconds = |s: f64| anchor + Duration::from_secs_f64(s);
        let intervals = down_intervals(log, anchor, wall, anchor, seconds(20.0));
        assert_eq!(
            intervals,
            vec![(seconds(1.5), seconds(3.0)), (seconds(5.25), seconds(20.0))],
            "the ready line before the fill is not an interval's end, and a PANIC with no \
             ready after it runs to the end"
        );
    }
}
