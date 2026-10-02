//! #581: a candidate claim is taken over only once the taker has itself
//! watched it go unrenewed for its whole lease, on this process's monotonic
//! clock.
//!
//! The database clock decides nothing here. Before 021 a claim was taken
//! over once `claim_expires_at` was in the past by `clock_timestamp()`, so a
//! database clock step moved every lease by the step: forward, a live
//! holder's row was taken while it was still offering the block; backward, a
//! dead holder's row waited lease + step.
//!
//! Every claim poll reads the claimed unfinished rows and gives each one's
//! version, its (`claim_token`, `claim_renewals`), to [`ClaimObserver`]. A
//! version first seen in a reply starts its clock when that reply arrived,
//! which is after the commit that wrote the version, and that commit is after
//! the instant the holder started the renewal its own lease is measured from
//! (`Coordinator::with_tracked_heartbeat`). So the holder's lease always ends
//! first, whatever either clock reads. A renewal is a new version, and a
//! version that is no longer claimed is forgotten. The takeover itself is a
//! compare and set on the version the observer timed.
//!
//! A process that starts while a dead holder's claim is in the table times it
//! from its own first reply: a takeover can come up to one lease later than
//! the claim's true end, never earlier.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// How long a claim taken before migration 021, which recorded no lease, is
/// timed: the most any writer can take (every claim and renewal accepts 1 to
/// 600 seconds).
pub const UNRECORDED_LEASE: Duration = Duration::from_secs(600);

/// One claimed unfinished row as a survey read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimVersion {
    pub block_hash: String,
    pub token: String,
    pub renewals: i64,
    /// `claim_lease_seconds`: `None` for a claim taken before 021, `Some(0)`
    /// for a revoked one.
    pub lease_seconds: Option<i32>,
    pub instance_id: Option<String>,
    /// Whether the row may be taken over once its lease is over: it is not
    /// parked (see `TAKEOVER_SQL`).
    pub due: bool,
}

impl ClaimVersion {
    pub fn lease(&self) -> Duration {
        self.lease_seconds.map_or(UNRECORDED_LEASE, |seconds| {
            Duration::from_secs(u64::try_from(seconds).unwrap_or(0))
        })
    }
}

#[derive(Debug)]
struct Watched {
    token: String,
    renewals: i64,
    since: Instant,
}

/// The claims this process has watched, by block hash. Shared by every clone
/// of one [`super::Ledger`], so one frontend keeps one clock per claim.
#[derive(Debug, Default)]
pub struct ClaimObserver {
    watched: Mutex<HashMap<String, Watched>>,
}

impl ClaimObserver {
    /// Record one complete read of the claimed unfinished rows, whose reply
    /// arrived at `replied`. A version not watched before starts its clock
    /// there; a row the read no longer shows claimed is forgotten.
    pub fn survey(&self, claims: &[ClaimVersion], replied: Instant) {
        let mut watched = self.watched.lock().unwrap_or_else(|e| e.into_inner());
        watched.retain(|hash, _| claims.iter().any(|claim| &claim.block_hash == hash));
        for claim in claims {
            Self::record(&mut watched, claim, replied);
        }
    }

    /// Record one row's claim, read by a reply that arrived at `replied`,
    /// leaving every other watched claim as it is: the operator recovery
    /// command reads only the row it was given.
    pub fn watch(&self, claim: &ClaimVersion, replied: Instant) {
        let mut watched = self.watched.lock().unwrap_or_else(|e| e.into_inner());
        Self::record(&mut watched, claim, replied);
    }

    fn record(watched: &mut HashMap<String, Watched>, claim: &ClaimVersion, replied: Instant) {
        let same = watched
            .get(&claim.block_hash)
            .is_some_and(|seen| seen.token == claim.token && seen.renewals == claim.renewals);
        if !same {
            watched.insert(
                claim.block_hash.clone(),
                Watched {
                    token: claim.token.clone(),
                    renewals: claim.renewals,
                    since: replied,
                },
            );
        }
    }

    /// How much longer `claim` must stay unrenewed before this process may
    /// take it over: zero once its whole lease has passed since this process
    /// first saw the version, and `None` when this process is not watching
    /// that version (it was renewed, released or never surveyed).
    pub fn remaining(&self, claim: &ClaimVersion, now: Instant) -> Option<Duration> {
        let watched = self.watched.lock().unwrap_or_else(|e| e.into_inner());
        let seen = watched
            .get(&claim.block_hash)
            .filter(|seen| seen.token == claim.token && seen.renewals == claim.renewals)?;
        Some(
            claim
                .lease()
                .saturating_sub(now.saturating_duration_since(seen.since)),
        )
    }

    /// Whether this process has watched `claim` unrenewed for its whole lease.
    pub fn expired(&self, claim: &ClaimVersion, now: Instant) -> bool {
        self.remaining(claim, now)
            .is_some_and(|left| left.is_zero())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim(token: &str, renewals: i64, lease: Option<i32>) -> ClaimVersion {
        ClaimVersion {
            block_hash: "b".into(),
            token: token.into(),
            renewals,
            lease_seconds: lease,
            instance_id: None,
            due: true,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_version_expires_one_lease_after_its_first_reply_and_never_before() {
        let observer = ClaimObserver::default();
        let held = claim("t", 0, Some(120));
        let first = Instant::now();
        observer.survey(std::slice::from_ref(&held), first);
        tokio::time::advance(Duration::from_secs(119)).await;
        observer.survey(std::slice::from_ref(&held), Instant::now());
        assert!(!observer.expired(&held, Instant::now()));
        assert_eq!(
            observer.remaining(&held, Instant::now()),
            Some(Duration::from_secs(1))
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(observer.expired(&held, Instant::now()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_renewal_or_a_new_token_restarts_the_clock() {
        let observer = ClaimObserver::default();
        observer.survey(&[claim("t", 0, Some(120))], Instant::now());
        tokio::time::advance(Duration::from_secs(100)).await;
        let renewed = claim("t", 1, Some(120));
        observer.survey(std::slice::from_ref(&renewed), Instant::now());
        tokio::time::advance(Duration::from_secs(100)).await;
        assert!(!observer.expired(&renewed, Instant::now()));
        // An observation of the old version is no longer one this process
        // is watching: its takeover would fail the compare and set anyway.
        assert_eq!(
            observer.remaining(&claim("t", 0, Some(120)), Instant::now()),
            None
        );
        let replaced = claim("u", 0, Some(120));
        observer.survey(std::slice::from_ref(&replaced), Instant::now());
        tokio::time::advance(Duration::from_secs(119)).await;
        assert!(!observer.expired(&replaced, Instant::now()));
    }

    #[tokio::test(start_paused = true)]
    async fn an_unrecorded_lease_is_the_maximum_and_a_revoked_one_is_over_at_once() {
        let observer = ClaimObserver::default();
        let old = claim("t", 0, None);
        observer.survey(std::slice::from_ref(&old), Instant::now());
        tokio::time::advance(Duration::from_secs(599)).await;
        assert!(!observer.expired(&old, Instant::now()));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(observer.expired(&old, Instant::now()));
        let revoked = claim("t", 0, Some(0));
        assert!(observer.expired(&revoked, Instant::now()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_released_row_is_forgotten_and_timed_afresh_if_claimed_again() {
        let observer = ClaimObserver::default();
        let held = claim("t", 0, Some(120));
        observer.survey(std::slice::from_ref(&held), Instant::now());
        tokio::time::advance(Duration::from_secs(200)).await;
        observer.survey(&[], Instant::now());
        // The same token and count claimed again (only a test does that)
        // still starts from its new first reply.
        observer.survey(std::slice::from_ref(&held), Instant::now());
        assert!(!observer.expired(&held, Instant::now()));
    }
}
