//! A drained frontend restart, driven from the scheduler loop without
//! stalling it.
//!
//! The reconnect phase measures the offered rate *while one frontend is
//! unavailable*. Awaiting the whole drain, kill, restart and readiness wait
//! inside the 1 ms scheduling loop would stop every other frontend's traffic
//! for the outage and then turn the missed interval into one burst plus
//! shortfall, which is the failover behaviour the phase exists to measure.
//! So the restart is a state machine the loop polls: every `poll` returns
//! after bounded, synchronous work (a counter read, a channel `try_recv`, a
//! kill-and-spawn) and the slow parts run in spawned tasks.
//!
//! A paused session is ineligible for offers: `SessionHandle::paused` is
//! set here before the pause is sent, `SessionHandle::try_offer` refuses
//! while it is set, and the retarget at the end clears it. Without that the
//! scheduler would keep filling each paused session's queue, the queued
//! offers would count as outstanding and hold the drain open to its deadline,
//! and the queue would go out as one burst on resume.
//!
//! The drain waits at least the configured share-commit timeout, and a
//! frontend whose sessions still have submits outstanding after that is not
//! restarted: killing it then would turn the harness's own in-flight requests
//! into `NoResponse` records inside the phase and misclassify the experiment
//! (EP-ERRORS). The failure is returned to the caller, which aborts the run.
//!
//! Each side of the restart is scraped, so the phase's server-side
//! histogram can be assembled across the counter reset (see
//! `measure::ack_delta_across_restarts`).

use crate::{
    client::{self, SessionHandle},
    frontend::Frontend,
    measure::{self, AckSplit, MetricsScrape, ProcessSampler},
};
use anyhow::{bail, Result};
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
use tokio::sync::oneshot::{self, error::TryRecvError};

/// How often the readiness probe is retried while the new process starts.
const READY_PROBE_INTERVAL: Duration = Duration::from_millis(200);

/// What a completed restart produced.
#[derive(Clone, Debug)]
pub struct RestartRecord {
    /// The frontend that was restarted.
    pub index: usize,
    /// From the pause to the last outstanding submit settling.
    pub drain_seconds: f64,
    /// From the kill to the new process answering `/healthz`.
    pub outage_seconds: f64,
    /// The scrapes on each side of the reset.
    pub split: AckSplit,
}

enum Stage {
    Draining,
    ScrapingBeforeKill(oneshot::Receiver<MetricsScrape>),
    WaitingReady {
        next_probe: Instant,
        probe: Option<oneshot::Receiver<bool>>,
    },
    ScrapingAfterRestart(oneshot::Receiver<MetricsScrape>),
    Done,
}

pub struct RestartDriver {
    index: usize,
    stage: Stage,
    started: Instant,
    drain_limit: Duration,
    drain_deadline: Instant,
    ready_limit: Duration,
    ready_deadline: Instant,
    drained_at: Option<Instant>,
    killed_at: Option<Instant>,
    end_of_previous: Option<MetricsScrape>,
}

fn outstanding_on(sessions: &[SessionHandle], index: usize) -> usize {
    sessions
        .iter()
        .filter(|session| session.frontend.load(Ordering::Relaxed) == index)
        .map(|session| session.outstanding.load(Ordering::Relaxed))
        .sum()
}

fn spawn_scrape(frontend: &Frontend) -> oneshot::Receiver<MetricsScrape> {
    let (tx, rx) = oneshot::channel();
    let instance_id = frontend.spec.instance_id.clone();
    let url = frontend.metrics_url();
    tokio::spawn(async move {
        let _ = tx.send(measure::scrape_metrics(&instance_id, &url).await);
    });
    rx
}

fn spawn_probe(frontend: &Frontend) -> oneshot::Receiver<bool> {
    let (tx, rx) = oneshot::channel();
    let url = frontend.health_url();
    tokio::spawn(async move {
        let healthy = reqwest::Client::new()
            .get(url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        let _ = tx.send(healthy);
    });
    rx
}

/// A scrape whose task ended without answering is a failed scrape, not an
/// empty one.
fn scrape_or_failed(
    result: Result<MetricsScrape, TryRecvError>,
    frontend: &Frontend,
) -> MetricsScrape {
    result.unwrap_or_else(|_| MetricsScrape {
        instance_id: frontend.spec.instance_id.clone(),
        ok: false,
        error: Some("the scrape task ended without a result".into()),
        ..Default::default()
    })
}

impl RestartDriver {
    /// Pause `index`'s sessions and begin draining. `drain_limit` must be at
    /// least the configured share-commit timeout; `ready_limit` bounds the
    /// new process's start-up.
    pub fn start(
        index: usize,
        sessions: &[SessionHandle],
        drain_limit: Duration,
        ready_limit: Duration,
    ) -> Self {
        for session in sessions {
            if session.frontend.load(Ordering::Relaxed) == index {
                // The flag goes up before the message goes out: the scheduler
                // reads it on the very next offer, while the session task
                // reads the message whenever it next polls its control
                // channel. Between those two moments nothing is queued into
                // the session, so the drain below waits only for submits that
                // are genuinely in flight.
                session.paused.store(true, Ordering::Relaxed);
                let _ = session.control.send(client::Control::Pause);
            }
        }
        let now = Instant::now();
        Self {
            index,
            stage: Stage::Draining,
            started: now,
            drain_limit,
            drain_deadline: now + drain_limit,
            ready_limit,
            ready_deadline: now + ready_limit,
            drained_at: None,
            killed_at: None,
            end_of_previous: None,
        }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    /// Advance without blocking. `Ok(None)` is "still in progress",
    /// `Ok(Some(record))` is done, and an error means the restart could not
    /// be performed as promised and the caller must not pretend it was.
    pub fn poll(
        &mut self,
        sessions: &[SessionHandle],
        frontends: &mut [Frontend],
        samplers: &[ProcessSampler],
    ) -> Result<Option<RestartRecord>> {
        let index = self.index;
        loop {
            match &mut self.stage {
                Stage::Draining => {
                    let outstanding = outstanding_on(sessions, index);
                    if outstanding == 0 {
                        self.drained_at = Some(Instant::now());
                        self.stage = Stage::ScrapingBeforeKill(spawn_scrape(&frontends[index]));
                        continue;
                    }
                    if Instant::now() >= self.drain_deadline {
                        bail!(
                            "{} still had {outstanding} submits outstanding after {:.1?}, the \
                             configured share-commit timeout plus a margin; the drained restart \
                             was not performed, because killing the process now would turn the \
                             harness's own in-flight submits into lost acknowledgements",
                            frontends[index].spec.instance_id,
                            self.drain_limit
                        );
                    }
                    return Ok(None);
                }
                Stage::ScrapingBeforeKill(rx) => {
                    let scrape = match rx.try_recv() {
                        Err(TryRecvError::Empty) => return Ok(None),
                        other => scrape_or_failed(other, &frontends[index]),
                    };
                    self.end_of_previous = Some(scrape);
                    // Kill and spawn are synchronous and take milliseconds:
                    // the process group gets SIGKILL and is reaped, and the
                    // new one is forked.
                    frontends[index].restart()?;
                    let now = Instant::now();
                    self.killed_at = Some(now);
                    self.ready_deadline = now + self.ready_limit;
                    if let Some(sampler) = samplers.get(index) {
                        sampler.set_pid(frontends[index].pid());
                    }
                    self.stage = Stage::WaitingReady {
                        next_probe: now,
                        probe: None,
                    };
                    continue;
                }
                Stage::WaitingReady { next_probe, probe } => {
                    if let Some(status) = frontends[index].exited() {
                        bail!(
                            "{} exited with {status} while restarting; see {}",
                            frontends[index].spec.instance_id,
                            frontends[index].stderr_path.display()
                        );
                    }
                    match probe {
                        Some(rx) => match rx.try_recv() {
                            Err(TryRecvError::Empty) => return Ok(None),
                            Ok(true) => {
                                self.stage =
                                    Stage::ScrapingAfterRestart(spawn_scrape(&frontends[index]));
                                continue;
                            }
                            Ok(false) | Err(TryRecvError::Closed) => {
                                *probe = None;
                                *next_probe = Instant::now() + READY_PROBE_INTERVAL;
                            }
                        },
                        None => {
                            if Instant::now() >= self.ready_deadline {
                                bail!(
                                    "{} did not become ready within {:?} after its restart; see {}",
                                    frontends[index].spec.instance_id,
                                    self.ready_limit,
                                    frontends[index].stderr_path.display()
                                );
                            }
                            if Instant::now() >= *next_probe {
                                *probe = Some(spawn_probe(&frontends[index]));
                            }
                        }
                    }
                    return Ok(None);
                }
                Stage::ScrapingAfterRestart(rx) => {
                    let start_of_next = match rx.try_recv() {
                        Err(TryRecvError::Empty) => return Ok(None),
                        other => scrape_or_failed(other, &frontends[index]),
                    };
                    let address = frontends[index].stratum_address();
                    for session in sessions {
                        if session.frontend.load(Ordering::Relaxed) == index {
                            let _ = session.control.send(client::Control::Retarget {
                                frontend: index,
                                address: address.clone(),
                                reconnect: false,
                            });
                            // Eligible again from this offer on; anything
                            // queued before the task handles the retarget is
                            // sent to the new process, as the queue is for.
                            session.paused.store(false, Ordering::Relaxed);
                        }
                    }
                    let now = Instant::now();
                    let drained_at = self.drained_at.unwrap_or(now);
                    let killed_at = self.killed_at.unwrap_or(now);
                    let record = RestartRecord {
                        index,
                        drain_seconds: drained_at
                            .saturating_duration_since(self.started)
                            .as_secs_f64(),
                        outage_seconds: now.saturating_duration_since(killed_at).as_secs_f64(),
                        split: AckSplit {
                            end_of_previous: self.end_of_previous.take().unwrap_or_default(),
                            start_of_next,
                        },
                    };
                    self.stage = Stage::Done;
                    return Ok(Some(record));
                }
                Stage::Done => return Ok(None),
            }
        }
    }
}
