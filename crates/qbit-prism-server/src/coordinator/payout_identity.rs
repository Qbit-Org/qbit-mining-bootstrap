//! Payout identities for `mining.authorize` (#823).
//!
//! A username is `address[.worker]` and only the address decides the payout,
//! so a resolved identity is cached by address: a new worker suffix on a
//! known address costs no node RPC. A miss validates the address through the
//! node, one validation per address at a time, so sessions naming the same
//! new address read the first one's result, and at most
//! [`MAX_CONCURRENT_VALIDATIONS`] across the frontend, so authorization cannot
//! fill the node's RPC work queue ahead of templates and block submission.
//! Nothing but authorization waits on these: `getblocktemplate`,
//! `submitblock` and other sessions' share submission never do.
use super::*;
use std::sync::{Mutex as StdMutex, Weak};

/// Address validations one frontend runs at once for authorization. The
/// node answers one in about a millisecond, so this still validates
/// thousands of new addresses a second.
pub(super) const MAX_CONCURRENT_VALIDATIONS: usize = 4;
/// How long an authorization waits, for another session's validation of its
/// address and then for a validation slot, before it is refused with a
/// retryable backend error, well inside the session's own deadline
/// (`PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS`, 30 s by default).
pub(super) const VALIDATION_WAIT: Duration = Duration::from_secs(5);
pub(super) const IDENTITY_TTL: Duration = Duration::from_secs(3600);
pub(super) const CACHE_CAPACITY: usize = 4096;

/// What an address resolved to: the payout address recorded for its shares
/// (the configured fallback's, for an address the node definitively refused)
/// and the P2MR program paid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PayoutIdentity {
    pub payout_address: String,
    pub p2mr_program_hex: String,
}

pub(super) struct PayoutIdentities {
    cache: StdMutex<HashMap<String, (PayoutIdentity, Instant)>>,
    flights: StdMutex<HashMap<String, Weak<Mutex<()>>>>,
    validations: Semaphore,
}

impl PayoutIdentities {
    pub fn new() -> Self {
        Self {
            cache: StdMutex::new(HashMap::new()),
            flights: StdMutex::new(HashMap::new()),
            validations: Semaphore::new(MAX_CONCURRENT_VALIDATIONS),
        }
    }

    pub fn cached(&self, address: &str) -> Option<PayoutIdentity> {
        let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache
            .get(address)
            .filter(|(_, at)| at.elapsed() < IDENTITY_TTL)
            .map(|(identity, _)| identity.clone())
    }

    /// Caches a definitive resolution only; a failed validation is never
    /// remembered, so it can neither redirect nor block a later attempt.
    pub fn remember(&self, address: &str, identity: PayoutIdentity) {
        self.remember_at(address, identity, Instant::now());
    }

    /// [`Self::remember`] as of `now`, which tests move forward.
    pub(super) fn remember_at(&self, address: &str, identity: PayoutIdentity, now: Instant) {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        let age = |at: &Instant| now.saturating_duration_since(*at);
        if cache.len() >= CACHE_CAPACITY {
            // Expired entries go first: `cached` already reads them as misses.
            cache.retain(|_, (_, at)| age(at) < IDENTITY_TTL);
        }
        if cache.len() >= CACHE_CAPACITY {
            // Then the entries that resolved to the configured fallback: any
            // text the node refuses lands there, so a run of it cannot push
            // out the addresses that pay as typed.
            cache.retain(|address, (identity, _)| identity.payout_address == *address);
        }
        if cache.len() >= CACHE_CAPACITY {
            cache.retain(|_, (_, at)| age(at) < Duration::from_secs(300));
            if cache.len() >= CACHE_CAPACITY {
                cache.clear();
            }
        }
        cache.insert(address.to_owned(), (identity, now));
    }

    /// The lock every validation of `address` holds. The registry keeps weak
    /// references and drops dead keys on insertion, so it stays bounded by
    /// the validations in flight.
    pub fn flight(&self, address: &str) -> Arc<Mutex<()>> {
        let mut flights = self.flights.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(flight) = flights.get(address).and_then(Weak::upgrade) {
            return flight;
        }
        flights.retain(|_, flight| flight.strong_count() > 0);
        let flight = Arc::new(Mutex::new(()));
        flights.insert(address.to_owned(), Arc::downgrade(&flight));
        flight
    }

    /// One of the frontend's validation slots, held for one authorization's
    /// node calls, if one frees before `deadline`.
    pub async fn validation_slot(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, StratumError> {
        match tokio::time::timeout_at(deadline, self.validations.acquire()).await {
            Ok(Ok(slot)) => Ok(slot),
            // The semaphore is never closed; a wait that runs out is retryable.
            Ok(Err(_)) | Err(_) => Err(busy()),
        }
    }
}

/// The refusal for an authorization that waited [`VALIDATION_WAIT`].
pub(super) fn busy() -> StratumError {
    StratumError::backend("payout address validation is busy; retry")
}
