//! Frontend-local landing observations, never publication authority.
//!
//! The process retains at most LIMIT block identities and LIMIT revision
//! observations. Completed identities retire only behind the existing mature
//! checkpoint; a watermark rejects late proofs. Saturation is unknown until
//! restart, rather than silently recounting a delayed duplicate. A restart
//! begins a new local knowledge horizon; no peer acceptance clock is inferred.
use super::{Family, Labels, Metrics, Registry, RevisionWorkResult};
use std::collections::{BTreeMap, HashMap};
use tokio::time::Instant;

const LIMIT: usize = 4096;

struct Acceptance {
    at: Instant,
    height: u64,
    revision: Option<i64>,
    closed: bool,
    degraded: Option<Instant>,
    awaiting_settlement: bool,
    unknown_revision: bool,
    orphan_uncertain: bool,
    existing_revision: bool,
}

#[derive(Default)]
struct Revision {
    observed: Option<Instant>,
    deliveries: Vec<(u64, Instant)>,
}

impl Revision {
    fn first(&self) -> Option<Instant> {
        self.observed
            .into_iter()
            .chain(self.deliveries.first().map(|(_, at)| *at))
            .min()
    }
}

#[derive(Default)]
pub(super) struct Landing {
    blocks: HashMap<[u8; 32], Acceptance>,
    revisions: BTreeMap<i64, Revision>,
    saturated: bool,
    ordering_lost: bool,
    observation: u64,
    completed: u64,
    failed: bool,
    pending_count: usize,
    retired_height: u64,
    acceptance_epoch: u64,
    delivery_count: usize,
    terminal_observation: u64,
    terminal_completed: u64,
    terminal_failed: bool,
}

impl Landing {
    fn pending(&self) -> bool {
        self.pending_count > 0
    }

    /// Open waits whose delivery target is still knowable: not closed, and
    /// neither a lost settlement reply nor an unsettled orphan verdict.
    fn known_waits(&self) -> impl Iterator<Item = &Acceptance> {
        self.blocks
            .values()
            .filter(|block| !block.closed && !block.unknown_revision && !block.orphan_uncertain)
    }

    /// Whether a known wait is open, which is the only state that attributes
    /// a build deadline to accepted-block revision work. Unknown tracking (a
    /// lost COMMIT reply, an unsettled orphan, saturation, lost ordering) is
    /// reported by `unknown()`; it is never counted as a delivery failure.
    fn known_pending(&self) -> bool {
        !self.ordering_lost && self.known_waits().next().is_some()
    }

    pub(super) fn age(&self) -> f64 {
        if self.ordering_lost {
            return -1.;
        }
        self.known_waits()
            .map(|block| block.at)
            .min()
            .map_or(if self.unknown() { -1. } else { 0. }, |at| {
                at.elapsed().as_secs_f64()
            })
    }

    pub(super) fn unknown(&self) -> bool {
        self.saturated
            || self.failed
            || (self.pending() && self.terminal_failed)
            || self
                .blocks
                .values()
                .any(|block| block.unknown_revision || block.orphan_uncertain)
    }

    fn orphaned(&mut self, hash: &str) {
        let mut identity = [0; 32];
        if hex::decode_to_slice(hash, &mut identity).is_ok() {
            if let Some(block) = self.blocks.get_mut(&identity).filter(|block| !block.closed) {
                block.closed = true;
                block.unknown_revision = false;
                block.orphan_uncertain = false;
                block.awaiting_settlement = false;
                self.pending_count -= 1;
            }
        }
        if !self.pending() {
            self.revisions.clear();
            self.delivery_count = 0;
        }
    }

    fn revision(&mut self, revision: i64) -> Option<&mut Revision> {
        if self.revisions.len() == LIMIT && !self.revisions.contains_key(&revision) {
            self.saturated = true;
            self.ordering_lost = true;
            return None;
        }
        Some(self.revisions.entry(revision).or_default())
    }

    fn resolve(&mut self, registry: &mut Registry) {
        if !self.pending() || self.ordering_lost {
            return;
        }
        // Compute eligible writes once, newest revision first. A delayed write
        // of an obsolete revision is not current replacement work. This scan
        // avoids nesting a revision scan inside every delivery/block pair.
        let mut deliveries = Vec::with_capacity(self.delivery_count);
        let mut newer_at: Option<Instant> = None;
        for (&revision, event) in self.revisions.iter().rev() {
            deliveries.extend(
                event
                    .deliveries
                    .iter()
                    .filter(|(_, at)| newer_at.is_none_or(|newer| newer > *at))
                    .map(|(_, at)| (revision, *at)),
            );
            newer_at = newer_at.into_iter().chain(event.first()).min();
        }
        for block in self.blocks.values_mut().filter(|block| !block.closed) {
            if block.unknown_revision || block.orphan_uncertain {
                continue;
            }
            let Some(target) = block.revision else {
                continue;
            };
            // Keep actual event clocks: binding a COMMIT's revision can run
            // after a concurrent successful socket write or newer refresh.
            let delivery = deliveries
                .iter()
                .copied()
                .filter(|(revision, at)| *revision >= target && *at >= block.at)
                .min_by_key(|(_, at)| *at);
            // Every result ends at a successful delivery. Supersession only
            // chooses its label; it must not stop the landing clock early.
            if let Some((revision, at)) = delivery {
                let result = if revision != target {
                    RevisionWorkResult::Superseded
                } else if block.degraded.is_some_and(|deadline| deadline <= at) {
                    RevisionWorkResult::Degraded
                } else {
                    RevisionWorkResult::Published
                };
                registry.observe(
                    Family::RevisionWork,
                    Labels::One(("result", result.as_str())),
                    at.saturating_duration_since(block.at).as_secs_f64(),
                );
            }
            // An obsolete write after supersession cannot settle pending age.
            // Replacement work must reach a socket.
            block.closed = delivery.is_some();
            self.pending_count -= usize::from(block.closed);
        }
        if !self.pending() {
            self.revisions.clear();
            self.delivery_count = 0;
        }
    }
}

impl Metrics {
    fn landing_event<T>(&self, event: impl FnOnce(&mut Landing, &mut Registry) -> T) -> T {
        // Same lock order as current_registry; coupled state and samples move
        // together, with no I/O, await or effect on the observed operation.
        let mut landing = self.landing.lock().unwrap_or_else(|e| e.into_inner());
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        event(&mut landing, &mut registry)
    }

    pub(crate) fn accepted_block(&self, hash: &str, height: u64) {
        self.accept_block(hash, height, false);
    }

    /// First local proof of an already-confirmed block may associate the
    /// coherent current revision. It cannot upgrade an earlier acceptance.
    pub(crate) fn accepted_landed_block(&self, hash: &str, height: u64) {
        self.accept_block(hash, height, true);
    }

    /// A committed proven orphan has no delivery target. Close its wait without
    /// a delivery sample, retaining its identity through the mature watermark.
    pub(crate) fn revision_work_orphaned(&self, hash: &str) {
        self.landing_event(|state, _| state.orphaned(hash));
    }

    /// Arm only at the existing COMMIT boundary. Until its acknowledgement or
    /// durable terminal evidence, no delivery can guess this orphan outcome.
    pub(crate) fn revision_work_orphan_settlement<'a>(
        &'a self,
        hash: &'a str,
    ) -> OrphanSettlement<'a> {
        self.landing_event(|state, _| {
            let mut identity = [0; 32];
            if hex::decode_to_slice(hash, &mut identity).is_ok() {
                if let Some(block) = state
                    .blocks
                    .get_mut(&identity)
                    .filter(|block| !block.closed)
                {
                    block.orphan_uncertain = true;
                }
            }
        });
        OrphanSettlement {
            metrics: self,
            hash,
        }
    }

    /// Only locally unresolved identities, bounded by LIMIT. The collector
    /// owns the existing cadence and deadline; rendering performs no I/O.
    pub(crate) fn revision_work_terminal_probe(&self) -> TerminalProbe<'_> {
        let mut state = self.landing.lock().unwrap_or_else(|e| e.into_inner());
        state.terminal_observation = state
            .terminal_observation
            .checked_add(1)
            .expect("terminal observation sequence exhausted");
        TerminalProbe {
            metrics: self,
            observation: state.terminal_observation,
            hashes: state
                .blocks
                .iter()
                .filter(|(_, block)| !block.closed)
                .map(|(hash, _)| hex::encode(hash))
                .collect(),
            succeeded: false,
        }
    }

    fn accept_block(&self, hash: &str, height: u64, existing_revision: bool) {
        self.landing_event(|state, _| {
            let mut identity = [0; 32];
            if hex::decode_to_slice(hash, &mut identity).is_err() {
                state.saturated = true;
                return;
            }
            if state.blocks.contains_key(&identity) {
                return;
            }
            if state.saturated || height <= state.retired_height {
                return;
            }
            if state.blocks.len() == LIMIT {
                state.saturated = true;
                return;
            }
            state.blocks.insert(
                identity,
                Acceptance {
                    at: Instant::now(),
                    height,
                    revision: None,
                    closed: false,
                    degraded: None,
                    awaiting_settlement: false,
                    unknown_revision: false,
                    orphan_uncertain: false,
                    existing_revision,
                },
            );
            state.pending_count += 1;
            state.acceptance_epoch = state.acceptance_epoch.saturating_add(1);
        });
    }

    /// The caller proved this committed revision includes the block. A later
    /// proof cannot move the original target or restart its acceptance clock.
    #[cfg(test)]
    pub(crate) fn landed_block(&self, hash: &str, revision: i64) {
        self.bind_landing(hash, revision, true, true);
    }

    #[cfg(test)]
    pub(crate) fn observed_landed_block(&self, hash: &str, revision: i64) {
        self.bind_landing(hash, revision, false, false);
    }

    fn bind_landing(&self, hash: &str, revision: i64, settled: bool, first: bool) {
        self.landing_event(|state, registry| {
            let mut identity = [0; 32];
            if hex::decode_to_slice(hash, &mut identity).is_err() {
                return;
            }
            if let Some(block) = state
                .blocks
                .get_mut(&identity)
                .filter(|block| !block.closed)
            {
                if settled && !first && block.revision.is_none() && !block.existing_revision {
                    block.unknown_revision = true;
                }
                if first && block.unknown_revision {
                    block.unknown_revision = false;
                    block.revision = None;
                }
                if !block.unknown_revision && (settled || !block.awaiting_settlement) {
                    block.revision.get_or_insert(revision);
                }
                if settled {
                    block.awaiting_settlement = false;
                }
            }
            if state.pending() {
                if let Some(event) = state.revision(revision) {
                    event.observed.get_or_insert_with(Instant::now);
                }
            }
            state.resolve(registry);
        });
    }

    pub(crate) fn revision_work_observed(&self, revision: i64) {
        self.landing_event(|state, registry| {
            if state.pending() {
                if let Some(event) = state.revision(revision) {
                    event.observed.get_or_insert_with(Instant::now);
                }
                state.resolve(registry);
            }
        });
    }

    /// The existing coherent mature checkpoint ends the history horizon.
    /// Retain unresolved intervals; reject delayed proofs below the watermark
    /// even after their completed identity has been released.
    pub(crate) fn revision_work_matured(&self, height: u64) {
        let mut state = self.landing.lock().unwrap_or_else(|e| e.into_inner());
        state.retired_height = state.retired_height.max(height);
        let floor = state.retired_height;
        state
            .blocks
            .retain(|_, block| !block.closed || block.height > floor);
    }

    /// Called immediately after write_all of the complete mining.notify frame
    /// succeeds. Prepared work, queued writes and failed writes never call it.
    pub(crate) fn revision_work_delivered(&self, revision: i64) {
        self.landing_event(|state, registry| {
            if state.pending() {
                let epoch = state.acceptance_epoch;
                let duplicate = state.revisions.get(&revision).is_some_and(|event| {
                    event
                        .deliveries
                        .last()
                        .is_some_and(|(seen, _)| *seen == epoch)
                });
                if duplicate {
                    return;
                }
                if state.delivery_count == LIMIT {
                    state.saturated = true;
                    state.ordering_lost = true;
                } else if let Some(event) = state.revision(revision) {
                    event.deliveries.push((epoch, Instant::now()));
                    state.delivery_count += 1;
                }
                state.resolve(registry);
            }
        });
    }

    pub(crate) fn revision_work_build(&self) -> Build<'_> {
        let state = self.landing.lock().unwrap_or_else(|e| e.into_inner());
        Build {
            metrics: self,
            at: Instant::now(),
            pending: state.known_pending(),
        }
    }

    pub(crate) fn revision_work_refresh(&self) -> Refresh<'_> {
        let mut state = self.landing.lock().unwrap_or_else(|e| e.into_inner());
        state.observation = state.observation.saturating_add(1);
        Refresh {
            metrics: self,
            observation: state.observation,
            succeeded: false,
        }
    }

    pub(crate) fn revision_work_settlement<'a>(&'a self, hash: &'a str) -> Settlement<'a> {
        let owns = self.landing_event(|state, _| {
            let mut identity = [0; 32];
            if hex::decode_to_slice(hash, &mut identity).is_ok() {
                if let Some(block) = state.blocks.get_mut(&identity) {
                    if !block.awaiting_settlement {
                        block.awaiting_settlement = true;
                        return true;
                    }
                }
            }
            false
        });
        Settlement {
            metrics: self,
            hash,
            completed: false,
            owns,
        }
    }
}

/// Uncertainty is armed before COMMIT. Dropping on cancellation or a lost reply
/// leaves it armed; a later durable orphan proof closes the interval.
pub(crate) struct OrphanSettlement<'a> {
    metrics: &'a Metrics,
    hash: &'a str,
}
impl OrphanSettlement<'_> {
    pub(crate) fn committed(self) {
        self.metrics.revision_work_orphaned(self.hash);
    }
}

pub(crate) struct TerminalProbe<'a> {
    metrics: &'a Metrics,
    observation: u64,
    pub(crate) hashes: Vec<String>,
    succeeded: bool,
}
impl TerminalProbe<'_> {
    pub(crate) fn succeeded(mut self, orphaned: &[String]) {
        self.metrics.landing_event(|state, _| {
            // Orphaned is a monotonic processing disposition, including after
            // reactivation for credit. Revalidate each still-owned identity;
            // late evidence cannot reopen a tombstone or emit a sample.
            for hash in orphaned {
                state.orphaned(hash);
            }
            if self.observation >= state.terminal_completed {
                state.terminal_completed = self.observation;
                state.terminal_failed = false;
            }
        });
        self.succeeded = true;
    }
}
impl Drop for TerminalProbe<'_> {
    fn drop(&mut self) {
        if self.succeeded {
            return;
        }
        let mut state = self
            .metrics
            .landing
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.observation >= state.terminal_completed {
            state.terminal_completed = self.observation;
            state.terminal_failed = !self.hashes.is_empty();
        }
    }
}

/// A lost/cancelled COMMIT reply cannot be replaced with a guess at the current
/// revision. A later first confirmation proves that earlier attempt did not
/// commit; otherwise the original revision remains unknown in this horizon.
pub(crate) struct Settlement<'a> {
    metrics: &'a Metrics,
    hash: &'a str,
    completed: bool,
    owns: bool,
}
impl Settlement<'_> {
    pub(crate) fn committed(mut self, first: bool, revision: i64) {
        // Arrival order is not commit order: a competing transaction can
        // prove the first confirmation while the owning observer is delayed.
        if self.owns || first {
            self.metrics.bind_landing(self.hash, revision, true, first);
        } else {
            self.metrics.revision_work_observed(revision);
        }
        self.completed = true;
    }
}
impl Drop for Settlement<'_> {
    fn drop(&mut self) {
        if self.completed || !self.owns {
            return;
        }
        self.metrics.landing_event(|state, _| {
            let mut identity = [0; 32];
            if hex::decode_to_slice(self.hash, &mut identity).is_ok() {
                if let Some(block) = state.blocks.get_mut(&identity) {
                    block.awaiting_settlement = false;
                    if !block.closed && block.revision.is_none() {
                        block.unknown_revision = true;
                    }
                }
            }
        });
    }
}

/// Consumption gives each actual timeout branch one event. Drop (including
/// cancellation and successful builds) deliberately records no timeout.
/// `pending` is the known-wait state at the build's start: a deadline hit
/// while only unknown tracking remained is an ordinary job-delivery failure,
/// not a revision-work failure, and leaves that tracking unknown (#493).
pub(crate) struct Build<'a> {
    metrics: &'a Metrics,
    at: Instant,
    pending: bool,
}
impl Build<'_> {
    pub(crate) fn deadline_hit(self) {
        if !self.pending {
            return;
        }
        self.metrics.landing_event(|state, registry| {
            registry.increment(Family::RevisionWorkTimeouts, Labels::Empty);
            for block in state
                .blocks
                .values_mut()
                .filter(|block| !block.closed && block.at <= self.at)
            {
                block.degraded.get_or_insert_with(Instant::now);
            }
        });
    }
}

pub(crate) struct Refresh<'a> {
    metrics: &'a Metrics,
    observation: u64,
    succeeded: bool,
}
impl Refresh<'_> {
    pub(crate) fn succeeded(mut self) {
        self.succeeded = true;
    }
}
impl Drop for Refresh<'_> {
    fn drop(&mut self) {
        let mut state = self
            .metrics
            .landing
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.observation >= state.completed {
            state.completed = self.observation;
            state.failed = !self.succeeded;
        }
    }
}

#[cfg(test)]
mod tests;
