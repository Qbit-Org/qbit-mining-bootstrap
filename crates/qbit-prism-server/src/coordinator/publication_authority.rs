//! Ordered local authority guards and fixed database-derived deadlines.
use super::tip_observation::{LeaseSelection, PublishedLease};
use super::*;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};
use tokio::time::Instant as MonotonicInstant;

/// Translate once, counting the clock request's own wait conservatively.
/// Callers choose whether elapsed authority is a miss or an error.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct AbsoluteDeadline(MonotonicInstant);

impl AbsoluteDeadline {
    pub(super) fn from_database(
        now_ms: i64,
        requested_at: MonotonicInstant,
        expires_at_ms: i64,
    ) -> Result<Self> {
        let remaining = if expires_at_ms <= now_ms {
            0
        } else {
            expires_at_ms
                .checked_sub(now_ms)
                .context("expiry clock overflow")? as u64
        };
        Ok(Self(
            requested_at
                .checked_add(Duration::from_millis(remaining))
                .context("expiry deadline overflow")?,
        ))
    }

    pub(super) fn live(&self) -> bool {
        MonotonicInstant::now() < self.0
    }

    pub(super) fn instant(&self) -> Instant {
        self.0.into_std()
    }
}

pub(super) struct AuthorityView<'a> {
    pub prepared: RwLockReadGuard<'a, Option<Arc<Prepared>>>,
    pub readiness: RwLockReadGuard<'a, ReadinessState>,
    pub tip: RwLockReadGuard<'a, TipState>,
}

/// The ordinary submit selector needs only the coupled publication, not a
/// readiness guard. Never acquire readiness while holding this subset view.
pub(super) struct TipPublicationView<'a> {
    pub prepared: RwLockReadGuard<'a, Option<Arc<Prepared>>>,
    pub tip: RwLockReadGuard<'a, TipState>,
}

impl<'a> AuthorityView<'a> {
    async fn read(
        prepared: &'a RwLock<Option<Arc<Prepared>>>,
        readiness: &'a RwLock<ReadinessState>,
        tip: &'a RwLock<TipState>,
    ) -> Self {
        Self {
            prepared: prepared.read().await,
            readiness: readiness.read().await,
            tip: tip.read().await,
        }
    }

    fn try_read(
        prepared: &'a RwLock<Option<Arc<Prepared>>>,
        readiness: &'a RwLock<ReadinessState>,
        tip: &'a RwLock<TipState>,
    ) -> Option<Self> {
        Some(Self {
            prepared: prepared.try_read().ok()?,
            readiness: readiness.try_read().ok()?,
            tip: tip.try_read().ok()?,
        })
    }
}

pub(super) struct AuthorityViewMut<'a> {
    pub prepared: RwLockWriteGuard<'a, Option<Arc<Prepared>>>,
    pub readiness: RwLockWriteGuard<'a, ReadinessState>,
    pub tip: RwLockWriteGuard<'a, TipState>,
}

impl Coordinator {
    pub(super) async fn tip_publication_view(&self) -> TipPublicationView<'_> {
        TipPublicationView {
            prepared: self.prepared.read().await,
            tip: self.observed_tip.read().await,
        }
    }

    pub(super) async fn authority_view(&self) -> AuthorityView<'_> {
        AuthorityView::read(&self.prepared, &self.readiness, &self.observed_tip).await
    }

    pub(super) async fn authority_view_mut(&self) -> AuthorityViewMut<'_> {
        AuthorityViewMut {
            prepared: self.prepared.write().await,
            readiness: self.readiness.write().await,
            tip: self.observed_tip.write().await,
        }
    }

    pub(super) fn try_authority_view_mut(&self) -> Option<AuthorityViewMut<'_>> {
        Some(AuthorityViewMut {
            prepared: self.prepared.try_write().ok()?,
            readiness: self.readiness.try_write().ok()?,
            tip: self.observed_tip.try_write().ok()?,
        })
    }

    pub(super) fn lease_commit_fence(
        &self,
        lease: PublishedLease,
        expires_at: Option<Instant>,
    ) -> LeaseCommitFence {
        LeaseCommitFence {
            prepared: self.prepared.clone(),
            readiness: self.readiness.clone(),
            tip: self.observed_tip.clone(),
            config: self.config.clone(),
            lease,
            expires_at,
        }
    }
}

/// Owned by the append task, even when its acknowledgement waiter disappears.
pub(super) struct LeaseCommitFence {
    prepared: Arc<RwLock<Option<Arc<Prepared>>>>,
    readiness: Arc<RwLock<ReadinessState>>,
    tip: Arc<RwLock<TipState>>,
    config: Arc<Config>,
    lease: PublishedLease,
    expires_at: Option<Instant>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LeaseCommitRefusal {
    /// The original publication/expiry no longer authorizes this append.
    Stale,
    /// Unavailable locks, readiness or tip observations prove no staleness.
    Unavailable,
}

impl LeaseCommitFence {
    /// The synchronous pre-COMMIT hook never waits for a lock. A contended or
    /// revoked authority refuses before COMMIT; the guards only span the gate
    /// transition and are released before any database I/O or blocking work.
    pub(super) fn with_authority(
        &self,
        commit: impl FnOnce() -> bool,
    ) -> std::result::Result<bool, LeaseCommitRefusal> {
        let Some(view) = AuthorityView::try_read(&self.prepared, &self.readiness, &self.tip) else {
            return Err(LeaseCommitRefusal::Unavailable);
        };
        if self
            .expires_at
            .is_some_and(|at| MonotonicInstant::now().into_std() >= at)
        {
            return Err(LeaseCommitRefusal::Stale);
        }
        match self.lease.select_with_cause(&view, &self.config) {
            Ok(LeaseSelection::Selected(..)) => {}
            Ok(LeaseSelection::Stale) => return Err(LeaseCommitRefusal::Stale),
            Ok(LeaseSelection::Unavailable) | Err(_) => {
                return Err(LeaseCommitRefusal::Unavailable)
            }
        }
        let won = commit();
        drop(view);
        Ok(won)
    }
}
