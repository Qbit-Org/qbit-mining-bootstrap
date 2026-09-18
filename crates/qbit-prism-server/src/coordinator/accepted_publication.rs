//! #458's accepted-block publication observations: how long a pool block the
//! node accepted waits for this frontend to publish work whose payout
//! revision carries its landing, and how old the oldest landing still waiting
//! is. Both are observations only: nothing here decides when, whether or with
//! which revision work is published, and every failure records nothing and
//! logs.
//!
//! The measured interval is `offered_at_ms` (the offering frontend's wall
//! clock immediately before its one `submitblock` call, durable in
//! `qbit_block_candidate_outbox` since migration 011) to this frontend's wall
//! clock immediately after its publication returned. Every frontend reads the
//! same durable column, so an offer made by another frontend is measured here
//! too; the two hosts' clocks may disagree, and a negative interval is skew,
//! dropped with a warning exactly as `Coordinator::observe_first_offer` drops
//! one, never clamped to zero.
//!
//! Observations run in publication order: each publication enqueues its
//! revision and instant, and one drain task at a time observes them first in,
//! first out, so a block is always claimed by the earliest publication whose
//! revision carries it and its sample ends at that publication's instant.
//!
//! The histogram samples only rows offered since this process started, so a
//! restart never samples a block twice. The pending gauge counts landed rows
//! only: an accepted block that has not landed (offered, in reconciliation, a
//! lost race awaiting its orphan proof) is the candidate gauges' subject, not
//! this one's. It reads every retained accepted row, and is unknown (-1)
//! until this process has seen one tick at which its published revision
//! equals the cluster's; that seeding tick covers every landing then
//! retained, so the gauge never reports the age of history a restart cannot
//! attribute, and a restart that cannot publish reads unknown, which the
//! readiness and coverage alerts own. After it, a landing counts until a tick
//! shows the cluster at this frontend's published revision, or until this
//! process samples it. Covered rows are remembered: an unrelated later bump
//! (maturity, another block's landing) moves the cluster ahead of the
//! published work for a moment, and without the memory every landing this
//! frontend already published would count as waiting again. Both remembered
//! sets are pruned to the rows the latest read still returns.
use super::{unix_ms_now, Coordinator};
use crate::{
    ledger::{AcceptedOffer, AcceptedOffers, Ledger},
    metrics::{Metrics, PendingAge, PublicationResult},
};
use anyhow::Result;
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::Duration,
};

#[cfg(test)]
mod seam_tests;

/// How long one health tick may spend deriving the pending-age gauge; well
/// inside the default two-second health publication cadence.
const PENDING_AGE_BUDGET: Duration = Duration::from_secs(1);

tokio::task_local! {
    /// Set only around `build_job`'s first work admission, so the refusal
    /// counter observes that admission and never a resume, a repair or a
    /// revalidation that shares the same authority check.
    static BUILD_ADMISSION: ();
}

/// The per-process state both observations share.
pub(crate) struct AcceptedPublication {
    /// This process's start on its own wall clock. Rows offered before it
    /// belong to an earlier process, which either sampled them or lost the
    /// sample with its memory; either way this process never samples them.
    /// That lost window is the approved at-most-once exception, the same one
    /// `observe_first_offer` documents. It bounds the histogram only.
    started_at_ms: i64,
    seen: Mutex<Seen>,
    /// Publications awaiting observation, oldest first.
    queue: Mutex<Queue>,
    /// How many publication observations have finished, recorded or not.
    /// Tests wait on it; nothing in the observation path reads it.
    completed: AtomicU64,
    /// A test seam between a refresh's reservation and its publication.
    #[cfg(test)]
    pub(super) publication_probe: Mutex<Option<std::sync::Arc<super::OfferProbe>>>,
}

/// What this process knows about accepted blocks, in memory.
#[derive(Default)]
struct Seen {
    /// Every block hash this process has already produced a sample for,
    /// including the ones a skewed interval dropped: a block is sampled at
    /// most once per process, so a retried or repeated observation records
    /// nothing twice.
    sampled: BTreeSet<String>,
    /// Landed blocks the gauge has seen carried by this frontend's published
    /// work (a snapshot whose cluster revision equalled the published one).
    covered: BTreeSet<String>,
    /// Whether a gauge tick has seen this frontend's published revision equal
    /// the cluster's; before that the gauge is unknown.
    seeded: bool,
}

/// Publications awaiting observation, and whether a drain task owns them.
#[derive(Default)]
struct Queue {
    pending: VecDeque<(i64, i64)>,
    draining: bool,
}

impl Queue {
    /// Enqueue one publication; true when the caller must start the drain.
    fn push(&mut self, publication: (i64, i64)) -> bool {
        self.pending.push_back(publication);
        !std::mem::replace(&mut self.draining, true)
    }

    /// The oldest publication not yet observed; `None` ends the drain, and
    /// the next push starts another.
    fn next(&mut self) -> Option<(i64, i64)> {
        let next = self.pending.pop_front();
        if next.is_none() {
            self.draining = false;
        }
        next
    }
}

impl AcceptedPublication {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            started_at_ms: unix_ms_now()?,
            seen: Mutex::new(Seen::default()),
            queue: Mutex::new(Queue::default()),
            completed: AtomicU64::new(0),
            #[cfg(test)]
            publication_probe: Mutex::new(None),
        })
    }

    fn seen(&self) -> std::sync::MutexGuard<'_, Seen> {
        self.seen.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn queue(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Keep only the hashes the latest read still returns: a row retention
/// deleted never comes back, so remembering it only grows the set.
fn retain_present(set: &mut BTreeSet<String>, offers: &AcceptedOffers) {
    if set.is_empty() {
        return;
    }
    let present: BTreeSet<&str> = offers
        .offers
        .iter()
        .map(|offer| offer.block_hash.as_str())
        .collect();
    set.retain(|hash| present.contains(hash.as_str()));
}

/// The measurable rows of one snapshot, newest landing first. `completed_at`
/// is the database clock of the terminal row; a row chain reconciliation
/// landed before its outbox row finished has none, and reconciliation runs
/// inside the refresh that publishes, so it sorts as the newest landing.
fn landed_newest_first(offers: &AcceptedOffers) -> Vec<&AcceptedOffer> {
    let mut landed: Vec<&AcceptedOffer> = offers
        .offers
        .iter()
        .filter(|offer| offer.landed && !offer.settled_without_landing)
        .collect();
    landed.sort_by(|left, right| {
        right
            .completed_at_ms
            .unwrap_or(i64::MAX)
            .cmp(&left.completed_at_ms.unwrap_or(i64::MAX))
            .then_with(|| right.offered_at_ms.cmp(&left.offered_at_ms))
            .then_with(|| left.block_hash.cmp(&right.block_hash))
    });
    landed
}

/// The oldest pending landing's age at `now_ms`, from one snapshot of every
/// retained accepted row and this frontend's published revision.
///
/// Only landed rows that are not settled without a landing can be pending.
/// A landed row is covered when this process sampled it, when the gauge
/// already saw it covered, or when the snapshot's cluster revision is the
/// revision this frontend currently publishes: that work carries every
/// landing the snapshot shows. Before the first such tick the value is
/// unknown. A second landing cannot reset the value, and it returns to zero
/// only when the oldest one is covered.
fn pending_age(
    offers: &AcceptedOffers,
    published_revision: Option<i64>,
    seen: &mut Seen,
    now_ms: i64,
) -> PendingAge {
    retain_present(&mut seen.covered, offers);
    let current = published_revision == Some(offers.payout_revision);
    seen.seeded |= current;
    let mut oldest = None::<i64>;
    for offer in offers
        .offers
        .iter()
        .filter(|offer| offer.landed && !offer.settled_without_landing)
    {
        if seen.sampled.contains(&offer.block_hash) || seen.covered.contains(&offer.block_hash) {
            continue;
        }
        if current {
            seen.covered.insert(offer.block_hash.clone());
            continue;
        }
        oldest = Some(oldest.map_or(offer.offered_at_ms, |at| at.min(offer.offered_at_ms)));
    }
    if !seen.seeded {
        return PendingAge::Unknown;
    }
    match oldest {
        None => PendingAge::None,
        Some(offered_at_ms) => match u64::try_from(now_ms.saturating_sub(offered_at_ms)) {
            Ok(millis) => PendingAge::Oldest(Duration::from_millis(millis)),
            // Cross-host wall clocks: this frontend is behind the offering
            // one. The age is unknown, which is not zero and not healthy.
            Err(_) => PendingAge::Unknown,
        },
    }
}

/// The histogram interval of one sample, or `None` for a negative one: the
/// two ends are different hosts' wall clocks, so a negative interval is skew
/// and yields no sample, never a zero.
fn publication_interval(offered_at_ms: i64, published_at_ms: i64) -> Option<Duration> {
    u64::try_from(published_at_ms.saturating_sub(offered_at_ms))
        .ok()
        .map(Duration::from_millis)
}

/// Record one sample, or warn and record nothing for a skewed interval.
fn record_sample(
    metrics: &Metrics,
    block: &str,
    result: PublicationResult,
    offered_at_ms: i64,
    published_at_ms: i64,
) {
    match publication_interval(offered_at_ms, published_at_ms) {
        Some(elapsed) => metrics.observe_accepted_publication(result, elapsed),
        None => tracing::warn!(
            %block,
            offered_at_ms,
            published_at_ms,
            "accepted publication: this frontend's wall clock is behind the offering frontend's; skew, no latency sample"
        ),
    }
}

impl Coordinator {
    /// Queue the publication that has just returned for observation, without
    /// holding up the refresh that produced it: the caller captures the
    /// published revision and the publication instant, and one drain task
    /// observes the queue in publication order, so no database work happens
    /// on the refresh path or under a publication lock.
    pub fn enqueue_accepted_publication_observation(
        &self,
        published_revision: i64,
        published_at_ms: i64,
    ) {
        let state = &self.accepted_publication;
        if !state.queue().push((published_revision, published_at_ms)) {
            return;
        }
        let ledger = self.ledger.clone();
        let metrics = self.metrics.clone();
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                // The queue guard ends with this statement, before any await.
                let next = state.queue().next();
                let Some((published_revision, published_at_ms)) = next else {
                    break;
                };
                if let Err(error) = observe(
                    &ledger,
                    &metrics,
                    &state,
                    published_revision,
                    published_at_ms,
                )
                .await
                {
                    tracing::warn!(
                        %error,
                        published_revision,
                        "accepted publication: the observation failed; no latency sample"
                    );
                }
                state.completed.fetch_add(1, Ordering::Release);
            }
        });
    }

    /// Run `build_job`'s first work admission with the stale-revision refusal
    /// observation armed. The admission itself is unchanged.
    pub(super) async fn build_admission<F: std::future::Future>(admission: F) -> F::Output {
        BUILD_ADMISSION.scope((), admission).await
    }

    /// Observation only, at the one branch that refuses work because the
    /// work's payout revision is not the cluster's: counted for `build_job`'s
    /// first admission and nowhere else.
    pub(super) fn observe_stale_revision_refusal(&self) {
        if BUILD_ADMISSION.try_with(|_| ()).is_ok() {
            self.metrics.record_stale_revision_refusal();
        }
    }

    #[cfg(test)]
    pub(super) async fn publication_probe(&self) {
        let probe = self
            .accepted_publication
            .publication_probe
            .lock()
            .unwrap()
            .clone();
        if let Some(probe) = probe {
            probe.entered.notify_one();
            probe.release.notified().await;
        }
    }

    /// Recompute the oldest-unpublished gauge from durable rows and record
    /// it. Called once per health tick: the gauge is a stored scalar, but it
    /// is recomputed from the ledger every tick, so it cannot freeze between
    /// ticks, and a tick whose read failed or overran its budget records the
    /// unknown value (-1) rather than an invented zero. The budget keeps a
    /// slow ledger from delaying the health publication that calls this.
    pub async fn publish_accepted_pending_age(&self) -> PendingAge {
        let read = tokio::time::timeout(PENDING_AGE_BUDGET, self.read_accepted_pending_age());
        let age = match read.await {
            Ok(Ok(age)) => age,
            Ok(Err(error)) => {
                tracing::warn!(
                    %error,
                    "accepted publication: the pending-age derivation failed; the gauge is unknown"
                );
                PendingAge::Unknown
            }
            Err(_) => {
                tracing::warn!(
                    budget_ms = PENDING_AGE_BUDGET.as_millis() as u64,
                    "accepted publication: the pending-age derivation overran its budget; the gauge is unknown"
                );
                PendingAge::Unknown
            }
        };
        self.metrics.set_accepted_pending_age(age);
        age
    }

    async fn read_accepted_pending_age(&self) -> Result<PendingAge> {
        // The revision this frontend's miners are issued work at, the value
        // `health` reports as `payout_state_generation`. Read before the
        // snapshot: equality with the snapshot's cluster revision proves the
        // published work carries every landing the snapshot shows.
        let published_revision = self
            .prepared
            .read()
            .await
            .as_ref()
            .map(|prepared| prepared.snapshot.payout_revision);
        let offers = self.ledger.accepted_offers(None).await?;
        let now_ms = unix_ms_now()?;
        let mut seen = self.accepted_publication.seen();
        Ok(pending_age(&offers, published_revision, &mut seen, now_ms))
    }

    /// The observation body, awaited directly so a test can drive it without
    /// the queue.
    pub async fn observe_accepted_publication(
        &self,
        published_revision: i64,
        published_at_ms: i64,
    ) -> Result<()> {
        observe(
            &self.ledger,
            &self.metrics,
            &self.accepted_publication,
            published_revision,
            published_at_ms,
        )
        .await
    }

    /// How many queued publication observations have finished in this
    /// process, recorded or not.
    pub fn accepted_publication_observations(&self) -> u64 {
        self.accepted_publication.completed.load(Ordering::Acquire)
    }
}

async fn observe(
    ledger: &Ledger,
    metrics: &Metrics,
    state: &AcceptedPublication,
    published_revision: i64,
    published_at_ms: i64,
) -> Result<()> {
    let offers = ledger.accepted_offers(Some(state.started_at_ms)).await?;
    // Only observations insert into `sampled`, and the queue runs them one at
    // a time, so pruning here to this read's rows cannot race an insert.
    retain_present(&mut state.seen().sampled, &offers);
    // EP-STATE: the publication authorized this observation at its revision.
    // If the cluster has moved past it, the landings this snapshot shows are
    // not all in the work that was published; record nothing and leave them
    // to the publication that catches up.
    if offers.payout_revision != published_revision {
        tracing::debug!(
            published_revision,
            cluster_revision = offers.payout_revision,
            "accepted publication: the cluster revision moved past the published one; no sample"
        );
        return Ok(());
    }
    let landed = landed_newest_first(&offers);
    // One critical section decides what this observation owns. The metrics
    // lock is taken afterwards, never under this one.
    let mut samples = Vec::new();
    {
        let mut seen = state.seen();
        for offer in landed {
            if seen.sampled.insert(offer.block_hash.clone()) {
                // The newest landing is the one this publication first
                // carried; an older landing a superseded revision already
                // carried is a legitimate `superseded` sample, not a failure.
                let result = if samples.is_empty() {
                    PublicationResult::Published
                } else {
                    PublicationResult::Superseded
                };
                samples.push((offer.block_hash.clone(), offer.offered_at_ms, result));
            }
        }
    }
    for (block, offered_at_ms, result) in samples {
        record_sample(metrics, &block, result, offered_at_ms, published_at_ms);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offer(
        hash: &str,
        offered_at_ms: i64,
        landed: bool,
        completed: Option<i64>,
    ) -> AcceptedOffer {
        AcceptedOffer {
            block_hash: hash.into(),
            offered_at_ms,
            landed,
            settled_without_landing: false,
            completed_at_ms: completed,
        }
    }

    /// A process that has already seen one current tick.
    fn seeded() -> Seen {
        Seen {
            seeded: true,
            ..Seen::default()
        }
    }

    #[test]
    fn a_skewed_read_is_unknown_and_a_fresh_empty_read_is_zero() {
        let mut seen = seeded();
        let empty = AcceptedOffers::default();
        assert_eq!(
            pending_age(&empty, None, &mut seen, 10_000),
            PendingAge::None
        );
        let skewed = AcceptedOffers {
            payout_revision: 2,
            offers: vec![offer("aa", 20_000, true, None)],
        };
        assert_eq!(
            pending_age(&skewed, Some(1), &mut seen, 10_000),
            PendingAge::Unknown
        );
    }

    #[test]
    fn an_accepted_row_that_never_landed_is_not_pending() {
        // Offered, in reconciliation, or a lost race awaiting its orphan
        // proof: the candidate gauges own it, whatever its age.
        let offers = AcceptedOffers {
            payout_revision: 5,
            offers: vec![offer("aa", 1_000, false, None)],
        };
        assert_eq!(
            pending_age(&offers, Some(4), &mut seeded(), 900_000),
            PendingAge::None
        );
    }

    #[test]
    fn a_second_landing_never_resets_the_pending_age_and_a_sample_clears_it() {
        let offers = AcceptedOffers {
            payout_revision: 4,
            offers: vec![
                offer("aa", 1_000, true, Some(2_000)),
                offer("bb", 5_000, true, Some(6_000)),
            ],
        };
        // Published work is behind the cluster, so both landings are pending.
        let mut seen = seeded();
        assert_eq!(
            pending_age(&offers, Some(3), &mut seen, 9_000),
            PendingAge::Oldest(Duration::from_secs(8))
        );
        seen.sampled.insert("aa".to_owned());
        assert_eq!(
            pending_age(&offers, Some(3), &mut seen, 9_000),
            PendingAge::Oldest(Duration::from_secs(4))
        );
        seen.sampled.insert("bb".to_owned());
        assert_eq!(
            pending_age(&offers, Some(3), &mut seen, 9_000),
            PendingAge::None
        );
    }

    #[test]
    fn the_gauge_is_unknown_until_a_current_tick_seeds_it_and_then_counts_only_new_landings() {
        // A restart: history this process cannot attribute is not an age.
        let history = AcceptedOffers {
            payout_revision: 9,
            offers: vec![offer("aa", 1_000, true, Some(2_000))],
        };
        let mut seen = Seen::default();
        for behind in [None, Some(8)] {
            assert_eq!(
                pending_age(&history, behind, &mut seen, 9_000),
                PendingAge::Unknown,
                "published {behind:?} is behind cluster revision 9"
            );
        }
        // The seeding tick covers every landing it shows.
        assert_eq!(
            pending_age(&history, Some(9), &mut seen, 9_000),
            PendingAge::None
        );
        // An unrelated later bump does not make that landing wait again; a
        // new landing this frontend has not published does.
        let later = AcceptedOffers {
            payout_revision: 11,
            offers: vec![
                offer("aa", 1_000, true, Some(2_000)),
                offer("bb", 6_000, true, Some(7_000)),
            ],
        };
        assert_eq!(
            pending_age(&later, Some(9), &mut seen, 9_000),
            PendingAge::Oldest(Duration::from_secs(3))
        );
        assert_eq!(
            pending_age(&later, Some(11), &mut seen, 9_000),
            PendingAge::None
        );
    }

    #[test]
    fn a_terminal_failure_is_neither_pending_nor_measurable() {
        let offers = AcceptedOffers {
            payout_revision: 2,
            offers: vec![AcceptedOffer {
                settled_without_landing: true,
                landed: true,
                ..offer("aa", 1_000, true, Some(2_000))
            }],
        };
        assert_eq!(
            pending_age(&offers, Some(1), &mut seeded(), 9_000),
            PendingAge::None
        );
        assert!(landed_newest_first(&offers).is_empty());
    }

    #[test]
    fn remembered_hashes_are_pruned_to_the_rows_the_read_still_returns() {
        let offers = AcceptedOffers {
            payout_revision: 3,
            offers: vec![offer("bb", 1_000, true, Some(2_000))],
        };
        let mut set: BTreeSet<String> = ["aa", "bb", "cc"].map(str::to_owned).into();
        retain_present(&mut set, &offers);
        assert_eq!(set, BTreeSet::from(["bb".to_owned()]));
        // The gauge prunes `covered` on every read.
        let mut seen = seeded();
        seen.covered = ["aa", "bb"].map(str::to_owned).into();
        pending_age(&offers, Some(2), &mut seen, 9_000);
        assert_eq!(seen.covered, BTreeSet::from(["bb".to_owned()]));
    }

    #[test]
    fn a_negative_interval_yields_no_sample_never_a_zero() {
        assert_eq!(publication_interval(1_000, 999), None);
        assert_eq!(publication_interval(1_000, 1_000), Some(Duration::ZERO));
        assert_eq!(
            publication_interval(1_000, 1_250),
            Some(Duration::from_millis(250))
        );
        let metrics = Metrics::default();
        record_sample(&metrics, "aa", PublicationResult::Published, 1_000, 999);
        let body = metrics.render();
        assert!(
            !body
                .lines()
                .any(|line| line
                    .starts_with("qbit_prism_accepted_block_work_publication_seconds_count")),
            "a skewed interval produced a sample:\n{body}"
        );
        record_sample(&metrics, "aa", PublicationResult::Published, 1_000, 1_000);
        assert!(metrics.render().contains(
            "qbit_prism_accepted_block_work_publication_seconds_count{result=\"published\"} 1\n"
        ));
    }

    #[test]
    fn the_queue_observes_publications_first_in_first_out_with_one_drain() {
        let mut queue = Queue::default();
        assert!(queue.push((1, 100)), "the first push starts the drain");
        assert!(!queue.push((1, 200)), "a running drain takes later pushes");
        assert!(!queue.push((2, 300)));
        assert_eq!(queue.next(), Some((1, 100)));
        assert_eq!(queue.next(), Some((1, 200)));
        assert_eq!(queue.next(), Some((2, 300)));
        assert_eq!(queue.next(), None);
        assert!(queue.push((3, 400)), "an ended drain is started again");
    }

    #[test]
    fn the_newest_landing_sorts_first_and_an_unfinished_row_lands_newest() {
        let offers = AcceptedOffers {
            payout_revision: 7,
            offers: vec![
                offer("aa", 1_000, true, Some(2_000)),
                offer("bb", 1_500, true, Some(4_000)),
                offer("cc", 1_800, true, None),
                offer("dd", 1_900, false, None),
            ],
        };
        let order: Vec<&str> = landed_newest_first(&offers)
            .iter()
            .map(|offer| offer.block_hash.as_str())
            .collect();
        assert_eq!(order, ["cc", "bb", "aa"]);
    }
}
