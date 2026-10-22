//! Per-frontend bounded fan-in for hot compact writes; owns no Coordinator.
use super::work_ledger::WorkLedger;
use crate::ledger::{CompactBatchAttempt, CompactDependency, CompactIssuedJob, IssuedJobSave};
use anyhow::{anyhow, Context, Result};
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

const BATCH: usize = 64;
const ADMITTED: usize = 128;
const DWELL: Duration = Duration::from_millis(1);

#[derive(PartialEq, Eq)]
struct Group {
    key: String,
    original_revision: i64,
    parent: String,
    original_expires_at_ms: i64,
    template_sha256: String,
    prior_balances_digest: [u8; 32],
    revision: i64,
}

impl Group {
    fn dependency(&self) -> CompactDependency<'_> {
        CompactDependency {
            key: &self.key,
            original_revision: self.original_revision,
            parent: &self.parent,
            original_expires_at_ms: self.original_expires_at_ms,
            template_sha256: &self.template_sha256,
            prior_balances_digest: self.prior_balances_digest,
        }
    }
}

struct Entry {
    group: Group,
    job: CompactIssuedJob,
    deadline: Instant,
    admitted: Instant,
    canceled: CancellationToken,
    response: oneshot::Sender<Result<IssuedJobSave>>,
    _permit: OwnedSemaphorePermit,
}

pub(super) struct IssuedBatcher {
    sender: mpsc::Sender<Entry>,
    slots: Arc<Semaphore>,
    shutdown: CancellationToken,
}

impl Drop for IssuedBatcher {
    fn drop(&mut self) {
        self.slots.close();
        self.shutdown.cancel();
    }
}

impl IssuedBatcher {
    pub(super) fn new(ledger: Arc<dyn WorkLedger>) -> Self {
        Self::with_dwell(ledger, DWELL)
    }

    // Existing session fixtures freeze time and deliberately prevent automatic
    // clock advancement. Keep their batch execution real without requiring a
    // timer tick; dedicated collector tests exercise the production dwell.
    #[cfg(test)]
    pub(super) fn without_dwell_for_tests(ledger: Arc<dyn WorkLedger>) -> Self {
        Self::with_dwell(ledger, Duration::ZERO)
    }

    #[cfg(test)]
    pub(super) async fn wait_for_idle_for_tests(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.slots.available_permits() != ADMITTED {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("issued collector did not finish");
    }

    fn with_dwell(ledger: Arc<dyn WorkLedger>, dwell: Duration) -> Self {
        let (sender, receiver) = mpsc::channel(ADMITTED);
        let shutdown = CancellationToken::new();
        tokio::spawn(run(ledger, receiver, shutdown.clone(), dwell));
        Self {
            sender,
            slots: Arc::new(Semaphore::new(ADMITTED)),
            shutdown,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn save(
        &self,
        id: &str,
        payload: &serde_json::Value,
        revision: i64,
        parent: &str,
        expires_at_ms: i64,
        dependency: CompactDependency<'_>,
        deadline: Instant,
    ) -> Result<IssuedJobSave> {
        anyhow::ensure!(
            parent == dependency.parent,
            "issued job dependency or deadline mismatch"
        );
        // Backpressure and cloning belong to the original caller's timeout.
        // The permit counts pending plus active entries, not just channel slots.
        let permit = tokio::time::timeout_at(deadline, self.slots.clone().acquire_owned())
            .await
            .context("issued job deadline elapsed")??;
        let canceled = CancellationToken::new();
        let _enrollment = canceled.clone().drop_guard();
        let (response, result) = oneshot::channel();
        let entry = Entry {
            group: Group {
                key: dependency.key.into(),
                original_revision: dependency.original_revision,
                parent: parent.into(),
                original_expires_at_ms: dependency.original_expires_at_ms,
                template_sha256: dependency.template_sha256.into(),
                prior_balances_digest: dependency.prior_balances_digest,
                revision,
            },
            job: CompactIssuedJob {
                job_id: id.into(),
                payload: payload.clone(),
                expires_at_ms,
            },
            deadline,
            admitted: Instant::now(),
            canceled,
            response,
            _permit: permit,
        };
        tokio::time::timeout_at(deadline, self.sender.send(entry))
            .await
            .context("issued job deadline elapsed")?
            .map_err(|_| anyhow!("compact issued batcher shut down"))?;
        // A caller keeps its own deadline even while SQL or cleanup is pending.
        // Dropping enrollment leaves active work to live peers; the last caller
        // cancels it. A lost reply cannot prove that COMMIT rolled back.
        tokio::time::timeout_at(deadline, result).await
            .context("compact issued batch deadline elapsed; durability unknown; reconcile original job identity")?
            .context("compact issued batcher shut down")?
    }
}

fn live(entry: &Entry) -> bool {
    !entry.canceled.is_cancelled() && !entry.response.is_closed() && Instant::now() < entry.deadline
}

fn discard_expired(pending: &mut VecDeque<Entry>) {
    let mut retained = VecDeque::new();
    while let Some(entry) = pending.pop_front() {
        if live(&entry) {
            retained.push_back(entry);
        } else {
            let _ = entry.response.send(Err(anyhow!(
                "issued job deadline elapsed or caller canceled"
            )));
        }
    }
    *pending = retained;
}

async fn run(
    ledger: Arc<dyn WorkLedger>,
    mut receiver: mpsc::Receiver<Entry>,
    shutdown: CancellationToken,
    dwell: Duration,
) {
    let mut pending = VecDeque::new();
    loop {
        discard_expired(&mut pending);
        let first = if let Some(first) = pending.pop_front() {
            first
        } else {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                first = receiver.recv() => match first { Some(first) => first, None => break },
            }
        };
        if !live(&first) {
            let _ = first.response.send(Err(anyhow!(
                "issued job deadline elapsed or caller canceled"
            )));
            continue;
        }
        let mut flush = (first.admitted + dwell).min(first.deadline);
        let mut batch = vec![first];
        // Select the oldest group first. Other groups retain FIFO order and may
        // independently commit after this attempt fails. No connection is held.
        let mut index = 0;
        while index < pending.len() && batch.len() < BATCH {
            if pending[index].group == batch[0].group {
                let entry = pending.remove(index).unwrap();
                flush = flush.min(entry.deadline);
                batch.push(entry);
            } else {
                index += 1;
            }
        }
        while batch.len() < BATCH {
            let entry = if let Ok(entry) = receiver.try_recv() {
                entry
            } else if Instant::now() >= flush {
                break;
            } else {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep_until(flush) => break,
                    entry = receiver.recv() => match entry { Some(entry) => entry, None => break },
                }
            };
            if entry.group == batch[0].group {
                flush = flush.min(entry.deadline);
                batch.push(entry);
            } else {
                pending.push_back(entry);
            }
        }
        batch = batch
            .into_iter()
            .filter_map(|entry| {
                if live(&entry) {
                    Some(entry)
                } else {
                    let _ = entry.response.send(Err(anyhow!(
                        "issued job deadline elapsed or caller canceled"
                    )));
                    None
                }
            })
            .collect();
        if batch.is_empty() {
            continue;
        }
        let deadline = batch.iter().map(|entry| entry.deadline).min().unwrap();
        let attempt = CompactBatchAttempt::new(deadline);
        let group = &batch[0].group;
        let jobs: Vec<_> = batch.iter().map(|entry| entry.job.clone()).collect();
        let mut cancellations: FuturesUnordered<_> = batch
            .iter()
            .map(|entry| entry.canceled.cancelled())
            .collect();
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => Err(interrupted(&attempt, "shutdown")),
            // Once SQL owns the immutable children, a caller losing interest
            // must not abort live peers. A canceled child may commit undelivered;
            // its original expiry and the shared minimum deadline still apply.
            _ = async { while cancellations.next().await.is_some() {} } => Err(interrupted(&attempt, "all callers canceled")),
            _ = tokio::time::sleep_until(deadline) => Err(interrupted(&attempt, "deadline elapsed")),
            result = ledger.save_issued_jobs_compact(&jobs, group.revision, &group.parent, group.dependency(), &attempt) => result,
        };
        // This measures the actual shared storage attempt, including its pool
        // and lock waits; it is not an exclusive database-service-time estimate.
        tracing::debug!(
            children = batch.len(),
            storage_ms = started.elapsed().as_secs_f64() * 1000.0,
            success = result.is_ok(),
            commit_started = attempt.commit_started(),
            "compact issued batch completed"
        );
        // Hold admission through rollback draining, including on shutdown.
        attempt.wait_for_cleanup().await;
        drop(cancellations);
        let result = result.map_err(Arc::new);
        for entry in batch {
            let reply = result
                .as_ref()
                .copied()
                .map_err(|error| anyhow::Error::new(SharedFailure(error.clone())));
            let _ = entry.response.send(reply);
        }
        if shutdown.is_cancelled() {
            break;
        }
    }
    // Dropping pending entries and receiver resolves all response channels and
    // releases permits. The task never holds its own sender or Coordinator.
}

fn interrupted(attempt: &CompactBatchAttempt, reason: &str) -> anyhow::Error {
    if attempt.commit_started() {
        anyhow!("compact issued batch {reason}; commit outcome uncertain; reconcile original job identities")
    } else {
        anyhow!("compact issued batch {reason} before commit")
    }
}

#[derive(Debug)]
struct SharedFailure(Arc<anyhow::Error>);
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

#[cfg(test)]
#[path = "issued_batcher_tests.rs"]
mod tests;
