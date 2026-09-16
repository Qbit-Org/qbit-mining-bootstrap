//! The mid-flight kill, driven from the scheduler loop without stalling it.
//!
//! The kill phase SIGKILLs a frontend while its sessions have submits
//! outstanding, relaunches it, points the sessions back at it, and
//! re-offers every share whose answer the kill destroyed, so the server can
//! say what it did with each. The helper that did this used to be awaited
//! inside the 1 ms scheduling loop: across the wait for work, the kill, the
//! relaunch, the readiness wait, a three second settle, the re-offers and
//! another three seconds, no other frontend was offered anything, and the
//! next tick turned the whole gap into one burst plus a shortfall. That is
//! the defect the drained restart had already been fixed for twice; this is
//! the same fix in the one place it had not been applied (EP-STATE).
//!
//! So, as [`crate::restart::RestartDriver`], it is a state machine the loop
//! polls: every `poll` returns after bounded, synchronous work, and the
//! readiness probe runs in a spawned task through the same
//! [`crate::restart::ReadyWait`] the restart uses.

use crate::{
    client::{self, SessionHandle, SubmitRecord},
    frontend::Frontend,
    measure::ProcessSampler,
    restart::ReadyWait,
    run::{indeterminate_after_kill, Collected},
};
use anyhow::{bail, Context, Result};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

/// How long the kill waits for the target frontend to hold work. A kill with
/// nothing in flight tears down an idle socket and proves nothing, so the
/// harness reports what it found rather than assuming the scenario happened.
pub const WORK_WAIT: Duration = Duration::from_secs(20);
/// The pause between the kill and the relaunch.
pub const RELAUNCH_DELAY: Duration = Duration::from_millis(500);

/// What a completed kill produced.
#[derive(Clone, Debug)]
pub struct KillRecord {
    /// The frontend that was killed.
    pub index: usize,
    /// Submits outstanding on its sessions at the instant of the kill. Zero
    /// means the scenario did not exercise.
    pub outstanding_at_kill: usize,
    /// The shares whose answer the kill destroyed, each re-offered.
    pub indeterminate: Vec<SubmitRecord>,
}

enum Stage {
    WaitingForWork {
        deadline: Instant,
    },
    Killed {
        relaunch_at: Instant,
    },
    WaitingReady(ReadyWait),
    Settling {
        deadline: Instant,
    },
    Collecting {
        deadline: Instant,
        acknowledgements: mpsc::UnboundedReceiver<()>,
        remaining: usize,
    },
    Done,
}

pub struct KillDriver {
    index: usize,
    stage: Stage,
    /// The run's kill fence, shared with every session through
    /// [`client::SessionShared`]. Bumped once, immediately before the
    /// SIGKILL.
    kill_fence: Arc<AtomicU64>,
    /// The value the bump published. Every record whose own `fence` is at
    /// least this one was built after the kill was decided; every record
    /// below it was built before, whatever order the collector applied them
    /// in. Set in `WaitingForWork`, read once in `Collecting`.
    fence_at_kill: Option<u64>,
    outstanding_at_kill: Option<usize>,
    ready_limit: Duration,
    census_limit: Duration,
}

fn outstanding_on(sessions: &[SessionHandle], index: usize) -> usize {
    sessions
        .iter()
        .filter(|session| session.frontend.load(Ordering::Relaxed) == index)
        .map(|session| session.outstanding.load(Ordering::Relaxed))
        .sum()
}

impl KillDriver {
    /// Begin: wait for `index` to hold work, for up to [`WORK_WAIT`].
    /// `ready_limit` bounds the relaunched process's start-up.
    /// `census_limit` is the configured share-commit timeout plus drain margin.
    /// `kill_fence` is the run's fence, the same counter the sessions stamp
    /// their records with.
    ///
    /// Nothing about the census is decided here. The census boundary is the
    /// fence value published in `poll`, at the kill itself; a length or an
    /// instant taken now would only describe when the driver was constructed.
    pub fn start(
        index: usize,
        ready_limit: Duration,
        census_limit: Duration,
        kill_fence: Arc<AtomicU64>,
    ) -> Self {
        Self {
            index,
            stage: Stage::WaitingForWork {
                deadline: Instant::now() + WORK_WAIT,
            },
            kill_fence,
            fence_at_kill: None,
            outstanding_at_kill: None,
            ready_limit,
            census_limit,
        }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    /// Advance without blocking. `Ok(None)` is "still in progress",
    /// `Ok(Some(record))` is done, and an error means the relaunch failed:
    /// the caller aborts the phase, as it does for a failed restart.
    pub fn poll(
        &mut self,
        sessions: &[SessionHandle],
        frontends: &mut [Frontend],
        samplers: &[ProcessSampler],
        collected: &Mutex<Collected>,
    ) -> Result<Option<KillRecord>> {
        let index = self.index;
        loop {
            match &mut self.stage {
                Stage::WaitingForWork { deadline } => {
                    let outstanding = outstanding_on(sessions, index);
                    if outstanding == 0 && Instant::now() < *deadline {
                        return Ok(None);
                    }
                    // Freeze new offers, but do not drain before killing: the
                    // scenario must interrupt real pending submits. Pause also
                    // discards queued offers that were never sent.
                    for session in sessions {
                        if session.frontend.load(Ordering::Relaxed) == index {
                            session.paused.store(true, Ordering::Relaxed);
                            session
                                .control
                                .send(client::Control::Pause)
                                .context("pausing a killed session for its census")?;
                        }
                    }
                    // The census boundary, published the instant before the
                    // signal. A pause stops a session taking new work; it does
                    // not fence a no-response the session has already emitted
                    // and that is still queued behind a slow collector, and
                    // the post-kill barriers then flush that record into the
                    // suffix of the submit log. Membership is therefore the
                    // record's own identity, not its arrival order: every
                    // record built from here on reads at least `fence`.
                    //
                    // Bumped before the kill rather than after it. A bump
                    // after `kill()` returned would leave a genuinely
                    // kill-induced no-response emitted in between outside the
                    // census, so it would not be re-offered and its committed
                    // row would be reported as an ordinary mid-run
                    // acknowledgement loss. Over-including a nanosecond of
                    // pre-kill time is the safe side of that trade;
                    // under-including invents durability findings.
                    //
                    // Nothing may come between these two statements: no
                    // await, no syscall, no lock.
                    self.fence_at_kill = Some(self.kill_fence.fetch_add(1, Ordering::SeqCst) + 1);
                    // SIGKILL and reap: synchronous, milliseconds.
                    frontends[index].kill();
                    self.outstanding_at_kill = Some(outstanding);
                    self.stage = Stage::Killed {
                        relaunch_at: Instant::now() + RELAUNCH_DELAY,
                    };
                    return Ok(None);
                }
                Stage::Killed { relaunch_at } => {
                    if Instant::now() < *relaunch_at {
                        return Ok(None);
                    }
                    frontends[index].restart()?;
                    if let Some(sampler) = samplers.get(index) {
                        sampler.set_pid(frontends[index].pid());
                    }
                    self.stage = Stage::WaitingReady(ReadyWait::start(
                        self.ready_limit,
                        "its relaunch after the mid-flight kill",
                    ));
                    continue;
                }
                Stage::WaitingReady(wait) => {
                    if !wait.poll(&mut frontends[index])? {
                        return Ok(None);
                    }
                    self.stage = Stage::Settling {
                        deadline: Instant::now() + self.census_limit,
                    };
                    return Ok(None);
                }
                Stage::Settling { deadline } => {
                    let outstanding = outstanding_on(sessions, index);
                    if Instant::now() >= *deadline {
                        bail!("mid-flight kill census timed out after {:?}: {outstanding} submits still outstanding", self.census_limit);
                    }
                    if outstanding != 0 {
                        return Ok(None);
                    }
                    // A zero counter means the session sent its records, not
                    // that Collected has applied them. Each paused session
                    // forwards a marker on that same FIFO event channel. The
                    // collector acknowledges it after applying prior records.
                    let (ack, acknowledgements) = mpsc::unbounded_channel();
                    let mut remaining = 0;
                    for session in sessions {
                        if session.frontend.load(Ordering::Relaxed) == index {
                            session
                                .control
                                .send(client::Control::CensusBarrier(ack.clone()))
                                .context("requesting a killed session's census barrier")?;
                            remaining += 1;
                        }
                    }
                    self.stage = Stage::Collecting {
                        deadline: *deadline,
                        acknowledgements,
                        remaining,
                    };
                    return Ok(None);
                }
                Stage::Collecting {
                    deadline,
                    acknowledgements,
                    remaining,
                } => {
                    while *remaining > 0 {
                        match acknowledgements.try_recv() {
                            Ok(()) => *remaining -= 1,
                            Err(mpsc::error::TryRecvError::Empty) => {
                                if Instant::now() >= *deadline {
                                    bail!("mid-flight kill census timed out after {:?}: {remaining} session barriers not collected", self.census_limit);
                                }
                                return Ok(None);
                            }
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                bail!("mid-flight kill census lost a session or its collector before accounting completed");
                            }
                        }
                    }
                    // The barriers above prove every record the killed
                    // sessions emitted has been applied; the fence decides
                    // which of them the kill owns. Completeness and
                    // membership are two facts, and the census needs both.
                    let fence_at_kill = self
                        .fence_at_kill
                        .expect("the census is read only after the kill published its fence");
                    let indeterminate = {
                        let state = collected.lock().expect("collector lock");
                        indeterminate_after_kill(&state.submits, fence_at_kill, index)
                    };
                    // Only now resume these sessions. Resuming at /healthz
                    // allowed new work to hide whether old work had settled.
                    let address = frontends[index].stratum_address();
                    for session in sessions {
                        if session.frontend.load(Ordering::Relaxed) == index {
                            session
                                .control
                                .send(client::Control::Retarget {
                                    frontend: index,
                                    address: address.clone(),
                                    reconnect: false,
                                })
                                .context("resuming a killed session after its census")?;
                        }
                    }
                    for record in &indeterminate {
                        if let Some(session) = sessions.get(record.session) {
                            let _ = session.control.send(client::Control::Reoffer {
                                share_id: record.share_id.clone(),
                                job_id: record.job_id.clone(),
                                extranonce2_hex: record.extranonce2_hex.clone(),
                                ntime_hex: record.ntime_hex.clone(),
                                nonce_hex: record.nonce_hex.clone(),
                                header_hex: record.header_hex.clone(),
                            });
                        }
                    }
                    self.stage = Stage::Done;
                    return Ok(Some(KillRecord {
                        index,
                        outstanding_at_kill: self.outstanding_at_kill.unwrap_or(0),
                        indeterminate,
                    }));
                }
                Stage::Done => return Ok(None),
            }
        }
    }
}
