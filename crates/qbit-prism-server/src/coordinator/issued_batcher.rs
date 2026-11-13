//! Per-frontend bounded fan-in for hot compact writes; owns no Coordinator.
use super::work_ledger::WorkLedger;
use crate::ledger::{CompactBatchAttempt, CompactDependency, CompactIssuedJob, IssuedJobSave};
use anyhow::{anyhow, Context, Result};
use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use std::{collections::VecDeque, panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// Children per transaction: `Ledger::save_issued_jobs_compact`'s own cap. A
/// batch costs about fourteen fixed round trips and a COMMIT whatever its
/// size, so smaller batches would only add transactions to a fan-out.
const BATCH: usize = 64;
/// The most batch transactions in flight at once (#860), started in waves.
/// Each holds one connection of the frontend's ledger pool from checkout
/// through rollback cleanup, the pool where share appends may hold all but
/// one connection, so the bound is small and fixed; the Coordinator passes
/// one flight where the pool cannot spare a second
/// (`coordinator::issued_batch_flights`). Overlapping batches take the same
/// row locks in the same order, and of the advisory locks only migration
/// 027's sync barrier, shared, through each child's `sync_seq` default; see
/// `save_issued_jobs_compact`.
pub(super) const IN_FLIGHT: usize = 2;
/// How long after a wave's first batch starts another may still join it. A
/// fan-out's batches are queued already and join within microseconds; a later
/// batch waits for the wave, so a FOR UPDATE queued on the cluster row behind
/// the wave waits for its first batch and a second that started with it.
/// The window bounds when the second batch starts, not when it takes its
/// KEY SHARE: one that first waits for a pool connection can still take it
/// past the queued writer, which then waits for that batch too (on a
/// saturated pool, up to about two batch durations).
const JOIN: Duration = Duration::from_millis(2);
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
    /// Entries admitted: every in-flight batch plus the one being collected.
    #[cfg(test)]
    admitted: usize,
    shutdown: CancellationToken,
}

impl Drop for IssuedBatcher {
    fn drop(&mut self) {
        self.slots.close();
        self.shutdown.cancel();
    }
}

impl IssuedBatcher {
    /// `flights` batch transactions may be in flight at once, 1 to IN_FLIGHT.
    pub(super) fn new(ledger: Arc<dyn WorkLedger>, flights: usize) -> Self {
        let batcher = Self::with_options(ledger, DWELL, flights, JOIN);
        tracing::info!(
            flights = flights.clamp(1, IN_FLIGHT),
            "compact issued batches: at most this many transactions in flight"
        );
        batcher
    }

    // Existing session fixtures freeze time and deliberately prevent automatic
    // clock advancement. Keep their batch execution real without requiring a
    // timer tick; dedicated collector tests exercise the production dwell and
    // join window.
    #[cfg(test)]
    pub(super) fn without_dwell_for_tests(ledger: Arc<dyn WorkLedger>) -> Self {
        Self::with_options(ledger, Duration::ZERO, IN_FLIGHT, Duration::MAX)
    }

    /// Lets a test that holds a batch on real time join a second one later.
    #[cfg(test)]
    pub(super) fn with_join_window_for_tests(
        ledger: Arc<dyn WorkLedger>,
        flights: usize,
        join: Duration,
    ) -> Self {
        Self::with_options(ledger, DWELL, flights, join)
    }

    /// Every admission slot is free again. Unless it panicked, a batch task
    /// releases its flight in the same poll that answers its last entry; the
    /// collector keeps an idle wave's unspent flights, so flights are not a
    /// sign of idleness.
    #[cfg(test)]
    pub(super) async fn wait_for_idle_for_tests(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.slots.available_permits() != self.admitted {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("issued collector did not finish");
    }

    fn with_options(
        ledger: Arc<dyn WorkLedger>,
        dwell: Duration,
        flights: usize,
        join: Duration,
    ) -> Self {
        let wave_size = flights.clamp(1, IN_FLIGHT);
        let admitted = (wave_size + 1) * BATCH;
        let (sender, receiver) = mpsc::channel(admitted);
        let shutdown = CancellationToken::new();
        let waves = Waves {
            flights: Arc::new(Semaphore::new(wave_size)),
            size: wave_size,
            join,
        };
        tokio::spawn(run(ledger, receiver, shutdown.clone(), dwell, waves));
        Self {
            sender,
            slots: Arc::new(Semaphore::new(admitted)),
            #[cfg(test)]
            admitted,
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

/// The collector's flights: `size` permits, one per batch in flight, and how
/// long after a wave's first batch starts another may join it.
struct Waves {
    flights: Arc<Semaphore>,
    size: usize,
    join: Duration,
}

async fn run(
    ledger: Arc<dyn WorkLedger>,
    mut receiver: mpsc::Receiver<Entry>,
    shutdown: CancellationToken,
    dwell: Duration,
    waves: Waves,
) {
    // Each batch runs in its own task holding one of these flights until it
    // has answered its entries, after any rollback cleanup, so the batcher
    // never holds more than `flights` connections.
    //
    // Batches start in waves. Every batch holds the cluster row FOR KEY
    // SHARE, and PostgreSQL grants a new KEY SHARE past a queued FOR UPDATE
    // while only compatible holders remain, so back-to-back overlapping
    // batches would keep an authority writer or blob GC (which wait there
    // holding ORDER_LOCK, and so every share append) out for a whole
    // fan-out. A batch joins the current wave only while the wave has an
    // unspent flight, none of its batches has finished, and its first batch
    // started at most JOIN ago; otherwise it opens a new wave, which waits
    // until every batch has finished. This frontend's batches therefore keep
    // such a writer waiting for one wave: its first batch and a second that
    // started with it (see JOIN for a second batch's pool wait).
    let Waves {
        flights,
        size: wave_size,
        join,
    } = waves;
    // The current wave's unspent flights. The collector holds every flight
    // not in a batch, so `flights` has one available only once a batch of
    // the wave has finished.
    let mut reserve: Option<OwnedSemaphorePermit> = None;
    // When the current wave's first batch started.
    let mut wave_started: Option<Instant> = None;
    let mut pending = VecDeque::new();
    'collect: loop {
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
        // Decided with the batch's first entry in hand, so a flight left
        // over from a finished burst never makes the next burst's first
        // batch run alone. While a wave drains, this entry waits here; its
        // caller's own deadline still answers it, and the batch is filtered
        // for live entries before it starts.
        let joins = reserve.is_some()
            && flights.available_permits() == 0
            && wave_started.is_none_or(|started| started.elapsed() <= join);
        if !joins {
            drop(reserve.take());
            reserve = Some(tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                all = flights.clone().acquire_many_owned(wave_size as u32) => {
                    all.expect("issued batch flights are never closed")
                }
            });
            wave_started = None;
        }
        let mut unspent = reserve.take().expect("a wave is open");
        let flight = if unspent.num_permits() > 1 {
            let flight = unspent.split(1).expect("an open wave has a flight");
            reserve = Some(unspent);
            flight
        } else {
            unspent
        };
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
                    _ = shutdown.cancelled() => break 'collect,
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
            // Not spent after all: the flight goes back to its wave.
            match reserve.as_mut() {
                Some(unspent) => unspent.merge(flight),
                None => reserve = Some(flight),
            }
            continue;
        }
        if shutdown.is_cancelled() {
            break;
        }
        wave_started.get_or_insert_with(Instant::now);
        let ledger = ledger.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let children = batch.len();
            let deadline = batch.iter().map(|entry| entry.deadline).min().unwrap();
            let attempt = CompactBatchAttempt::new(deadline);
            let saved = AssertUnwindSafe(save_issued_batch_in_flight(
                ledger, batch, shutdown, &attempt,
            ))
            .catch_unwind()
            .await;
            // Hold this flight through rollback draining, also after a panic,
            // so a cleanup still holding its connection counts against the
            // bound. A panic drops the batch's entries: their callers read
            // the batcher as shut down although the collector carries on.
            attempt.wait_for_cleanup().await;
            drop(flight);
            if saved.is_err() {
                tracing::error!(
                    children,
                    commit_started = attempt.commit_started(),
                    "compact issued batch task panicked; its callers were refused as shut down"
                );
            }
        });
    }
    // Dropping pending entries and receiver resolves their response channels
    // and releases their permits. Each in-flight batch answers its own
    // entries once shutdown interrupts it and its cleanup ends. No task holds
    // the batcher's sender or Coordinator.
}

/// One batch's transaction and its answers. Its result goes only to its own
/// entries, and each caller then revalidates its own publication authority
/// (`save_with_dependency`), so batches may complete in any order.
async fn save_issued_batch_in_flight(
    ledger: Arc<dyn WorkLedger>,
    batch: Vec<Entry>,
    shutdown: CancellationToken,
    attempt: &CompactBatchAttempt,
) {
    let deadline = attempt.deadline;
    let group = &batch[0].group;
    let jobs: Vec<_> = batch.iter().map(|entry| entry.job.clone()).collect();
    let mut cancellations: FuturesUnordered<_> = batch
        .iter()
        .map(|entry| entry.canceled.cancelled())
        .collect();
    let started = Instant::now();
    // From the oldest child's admission: its wait for a flight, a draining
    // wave and the collection dwell, on this frontend's monotonic clock.
    let queued = batch
        .iter()
        .map(|entry| started.saturating_duration_since(entry.admitted))
        .max()
        .unwrap();
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => Err(interrupted(attempt, "shutdown")),
        // Once SQL owns the immutable children, a caller losing interest
        // must not abort live peers. A canceled child may commit undelivered;
        // its original expiry and the shared minimum deadline still apply.
        _ = async { while cancellations.next().await.is_some() {} } => Err(interrupted(attempt, "all callers canceled")),
        _ = tokio::time::sleep_until(deadline) => Err(interrupted(attempt, "deadline elapsed")),
        result = ledger.save_issued_jobs_compact(&jobs, group.revision, &group.parent, group.dependency(), attempt) => result,
    };
    // This measures the actual shared storage attempt, including its pool
    // and lock waits (another in-flight batch's renewal of the same
    // dependency row among them); it is not an exclusive database-service-
    // time estimate.
    tracing::debug!(
        children = batch.len(),
        queued_ms = queued.as_secs_f64() * 1000.0,
        storage_ms = started.elapsed().as_secs_f64() * 1000.0,
        success = result.is_ok(),
        commit_started = attempt.commit_started(),
        "compact issued batch completed"
    );
    // Hold admission through rollback draining, including on shutdown. The
    // caller holds the flight until then too.
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
