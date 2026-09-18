//! #458's accepted-block publication observations: how long a pool block the
//! node accepted waits for this frontend to publish work whose payout
//! revision carries its landing, and how old the oldest block still waiting
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
use super::{unix_ms_now, Coordinator};
use crate::{
    ledger::{AcceptedOffer, AcceptedOffers, Ledger},
    metrics::{Metrics, PendingAge, PublicationResult},
};
use anyhow::Result;
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::Duration,
};

/// How long one health tick may spend deriving the pending-age gauge; well
/// inside the default two-second health publication cadence.
const PENDING_AGE_BUDGET: Duration = Duration::from_secs(1);

/// The per-process state both observations share.
pub(crate) struct AcceptedPublication {
    /// This process's start on its own wall clock. Rows offered before it
    /// belong to an earlier process, which either sampled them or lost the
    /// sample with its memory; either way this process never samples them.
    /// That lost window is the approved at-most-once exception, the same one
    /// `observe_first_offer` documents.
    started_at_ms: i64,
    /// Every block hash this process has already produced a sample for,
    /// including the ones a skewed interval dropped: a block is sampled at
    /// most once per process, so a retried or repeated observation records
    /// nothing twice.
    sampled: Mutex<BTreeSet<String>>,
    /// How many detached observations have finished, recorded or not. Tests
    /// wait on it; nothing in the observation path reads it.
    completed: AtomicU64,
}

impl AcceptedPublication {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            started_at_ms: unix_ms_now()?,
            sampled: Mutex::new(BTreeSet::new()),
            completed: AtomicU64::new(0),
        })
    }

    fn sampled(&self) -> std::sync::MutexGuard<'_, BTreeSet<String>> {
        self.sampled.lock().unwrap_or_else(|e| e.into_inner())
    }
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

/// The oldest pending block's age at `now_ms`, from one snapshot and the set
/// already sampled. A row is pending when it was accepted, is not settled
/// without a landing, and this process has not sampled it: offered,
/// reconciliation and landed-but-unsampled rows all count, so a second
/// acceptance cannot reset the value and it returns to zero only when the
/// oldest one is published.
fn pending_age(offers: &AcceptedOffers, sampled: &BTreeSet<String>, now_ms: i64) -> PendingAge {
    let oldest = offers
        .offers
        .iter()
        .filter(|offer| !offer.settled_without_landing && !sampled.contains(&offer.block_hash))
        .map(|offer| offer.offered_at_ms)
        .min();
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

impl Coordinator {
    /// Observe the publication that has just returned, without holding up the
    /// refresh that produced it: the caller captures the published revision
    /// and the publication instant and hands them to a detached task, so no
    /// database work happens on the refresh path or under a publication lock.
    pub(super) fn spawn_accepted_publication_observation(
        &self,
        published_revision: i64,
        published_at_ms: i64,
    ) {
        let ledger = self.ledger.clone();
        let metrics = self.metrics.clone();
        let state = self.accepted_publication.clone();
        tokio::spawn(async move {
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
        });
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
        let state = &self.accepted_publication;
        let offers = self
            .ledger
            .accepted_offers_since(state.started_at_ms)
            .await?;
        let now_ms = unix_ms_now()?;
        let sampled = state.sampled();
        Ok(pending_age(&offers, &sampled, now_ms))
    }

    /// The observation body, awaited directly so a test can drive it without
    /// racing the detached task.
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

    /// How many detached publication observations have finished in this
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
    let offers = ledger.accepted_offers_since(state.started_at_ms).await?;
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
    // One critical section decides what this observation owns, so two
    // publications cannot sample the same block twice. The metrics lock is
    // taken afterwards, never under this one.
    let mut samples = Vec::new();
    {
        let mut sampled = state.sampled();
        for offer in landed {
            if sampled.insert(offer.block_hash.clone()) {
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
        match u64::try_from(published_at_ms.saturating_sub(offered_at_ms)) {
            Ok(millis) => {
                metrics.observe_accepted_publication(result, Duration::from_millis(millis))
            }
            Err(_) => tracing::warn!(
                %block,
                offered_at_ms,
                published_at_ms,
                "accepted publication: this frontend's wall clock is behind the offering frontend's; skew, no latency sample"
            ),
        }
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

    #[test]
    fn a_skewed_read_is_unknown_and_a_fresh_empty_read_is_zero() {
        let sampled = BTreeSet::new();
        let empty = AcceptedOffers::default();
        assert_eq!(pending_age(&empty, &sampled, 10_000), PendingAge::None);
        let skewed = AcceptedOffers {
            payout_revision: 1,
            offers: vec![offer("aa", 20_000, false, None)],
        };
        assert_eq!(pending_age(&skewed, &sampled, 10_000), PendingAge::Unknown);
    }

    #[test]
    fn a_second_acceptance_never_resets_the_pending_age_and_a_sample_clears_it() {
        let offers = AcceptedOffers {
            payout_revision: 4,
            offers: vec![
                offer("aa", 1_000, true, Some(2_000)),
                offer("bb", 5_000, false, None),
            ],
        };
        let mut sampled = BTreeSet::new();
        assert_eq!(
            pending_age(&offers, &sampled, 9_000),
            PendingAge::Oldest(Duration::from_secs(8))
        );
        sampled.insert("aa".to_owned());
        assert_eq!(
            pending_age(&offers, &sampled, 9_000),
            PendingAge::Oldest(Duration::from_secs(4))
        );
        sampled.insert("bb".to_owned());
        assert_eq!(pending_age(&offers, &sampled, 9_000), PendingAge::None);
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
            pending_age(&offers, &BTreeSet::new(), 9_000),
            PendingAge::None
        );
        assert!(landed_newest_first(&offers).is_empty());
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
