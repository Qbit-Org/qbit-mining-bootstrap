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
use anyhow::Result;
use std::{
    sync::{atomic::Ordering, Mutex},
    time::{Duration, Instant},
};

/// How long the kill waits for the target frontend to hold work. A kill with
/// nothing in flight tears down an idle socket and proves nothing, so the
/// harness reports what it found rather than assuming the scenario happened.
pub const WORK_WAIT: Duration = Duration::from_secs(20);
/// The pause between the kill and the relaunch.
pub const RELAUNCH_DELAY: Duration = Duration::from_millis(500);
/// How long the retargeted sessions are given to report the no-responses
/// the kill produced before the census is taken and the re-offers go out.
pub const SETTLE: Duration = Duration::from_secs(3);

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
    WaitingForWork { deadline: Instant },
    Killed { relaunch_at: Instant },
    WaitingReady(ReadyWait),
    Settling { until: Instant },
    Done,
}

pub struct KillDriver {
    index: usize,
    stage: Stage,
    /// The submit log's length when the kill began: the census is every
    /// no-response the killed frontend's sessions recorded after it.
    submits_before: usize,
    outstanding_at_kill: Option<usize>,
    ready_limit: Duration,
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
    pub fn start(index: usize, ready_limit: Duration, collected: &Mutex<Collected>) -> Self {
        let submits_before = collected.lock().expect("collector lock").submits.len();
        Self {
            index,
            stage: Stage::WaitingForWork {
                deadline: Instant::now() + WORK_WAIT,
            },
            submits_before,
            outstanding_at_kill: None,
            ready_limit,
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
                    let address = frontends[index].stratum_address();
                    for session in sessions {
                        if session.frontend.load(Ordering::Relaxed) == index {
                            let _ = session.control.send(client::Control::Retarget {
                                frontend: index,
                                address: address.clone(),
                                reconnect: false,
                            });
                        }
                    }
                    self.stage = Stage::Settling {
                        until: Instant::now() + SETTLE,
                    };
                    return Ok(None);
                }
                Stage::Settling { until } => {
                    if Instant::now() < *until {
                        return Ok(None);
                    }
                    let indeterminate = {
                        let state = collected.lock().expect("collector lock");
                        indeterminate_after_kill(&state.submits, self.submits_before, index)
                    };
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
