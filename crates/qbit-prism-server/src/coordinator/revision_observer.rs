//! Fresh scalar reads for closed issuance cohorts, never a revision cache.
use super::{tip_observation::PreparedIdentity, work_ledger::WorkLedger};
use anyhow::{anyhow, Result};
use futures_util::future::join_all;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

const COHORT: usize = 64;
const ADMITTED: usize = 128;

#[derive(Clone, Copy)]
pub(super) enum Boundary {
    BuildEntry,
    PostMaterialization,
    PrePersistence,
    PostPersistence,
}

#[derive(PartialEq, Eq)]
struct Group {
    identity: Arc<PreparedIdentity>,
    readiness_epoch: u64,
    publication: Option<(String, u64)>,
}

#[derive(Clone, Debug)]
struct SharedFailure(Arc<anyhow::Error>);

impl std::fmt::Display for SharedFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:#}", self.0)
    }
}

impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

struct Entry {
    group: Group,
    response: oneshot::Sender<Result<i64, SharedFailure>>,
    // Counts both queued and active callers, including canceled active reads.
    _permit: OwnedSemaphorePermit,
}

pub(super) struct RevisionObserver {
    senders: [mpsc::Sender<Entry>; 4],
    slots: Arc<Semaphore>,
}

impl RevisionObserver {
    pub(super) fn new(ledger: Arc<dyn WorkLedger>) -> Self {
        let senders = std::array::from_fn(|_| {
            let (sender, receiver) = mpsc::channel(ADMITTED);
            tokio::spawn(run(ledger.clone(), receiver));
            sender
        });
        Self {
            senders,
            slots: Arc::new(Semaphore::new(ADMITTED)),
        }
    }

    pub(super) async fn observe(
        &self,
        boundary: Boundary,
        identity: Arc<PreparedIdentity>,
        readiness_epoch: u64,
        publication: Option<(String, u64)>,
    ) -> Result<i64> {
        // The caller's existing timeout owns enrollment and this await. There
        // is no new clock or leader deadline that can renew a peer's budget.
        let permit = self.slots.clone().acquire_owned().await?;
        let (response, result) = oneshot::channel();
        self.senders[boundary as usize]
            .send(Entry {
                group: Group {
                    identity,
                    readiness_epoch,
                    publication,
                },
                response,
                _permit: permit,
            })
            .await
            .map_err(|_| anyhow!("revision observer shut down"))?;
        Ok(result
            .await
            .map_err(|_| anyhow!("revision observer shut down"))??)
    }
}

async fn run(ledger: Arc<dyn WorkLedger>, mut receiver: mpsc::Receiver<Entry>) {
    let mut pending: VecDeque<Entry> = VecDeque::new();
    loop {
        pending.retain(|entry| !entry.response.is_closed());
        let first = match pending.pop_front() {
            Some(first) => first,
            None => match receiver.recv().await {
                Some(first) => first,
                None => return,
            },
        };
        if first.response.is_closed() {
            continue;
        }
        // Give already runnable enrollees one scheduling turn; no timer/dwell
        // extends an operation. The bounded drain below closes the cohort.
        tokio::task::yield_now().await;
        let mut cohort = vec![first];
        let mut index = 0;
        while index < pending.len() && cohort.len() < COHORT {
            if pending[index].group == cohort[0].group {
                cohort.push(pending.remove(index).unwrap());
            } else {
                index += 1;
            }
        }
        for _ in 0..ADMITTED {
            if cohort.len() == COHORT {
                break;
            }
            let Ok(entry) = receiver.try_recv() else {
                break;
            };
            if entry.response.is_closed() {
                continue;
            }
            if entry.group == cohort[0].group {
                cohort.push(entry);
            } else {
                pending.push_back(entry);
            }
        }
        cohort.retain(|entry| !entry.response.is_closed());
        if cohort.is_empty() {
            continue;
        }
        // Enrollment is CLOSED before creating/polling the query. A later
        // arrival stays queued even while this read waits on pool or database.
        // One canceled caller cannot strand live peers. The last cancellation
        // drops the read just as the original scalar caller would: do not keep
        // detached database work alive after every original timeout has fired.
        let mut read = ledger.payout_revision();
        let result = tokio::select! {
            result = &mut read => Some(result),
            _ = join_all(cohort.iter_mut().map(|entry| entry.response.closed())) => None,
        };
        // End the query future before releasing entry permits. SQLx retains
        // ownership of physical connection return/cleanup on cancellation.
        drop(read);
        let Some(result) = result else {
            continue;
        };
        let result = result.map_err(|error| SharedFailure(Arc::new(error)));
        for entry in cohort {
            let _ = entry.response.send(result.clone());
        }
    }
}

#[cfg(test)]
#[path = "revision_observer_tests.rs"]
mod tests;
