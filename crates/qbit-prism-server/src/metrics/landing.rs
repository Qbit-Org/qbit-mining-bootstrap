//! Frontend-local landing observations, never publication authority.
//!
//! A definitive `submitblock` acceptance opens an identity as unlanded until
//! an active-chain observation lands it or a proven orphan closes it; only
//! landed identities are known delivery waits (#493). The process retains at
//! most LIMIT block identities and LIMIT revision observations. Completed identities retire only behind the existing mature
//! checkpoint; a watermark rejects late proofs. Saturation is unknown until
//! restart, rather than silently recounting a delayed duplicate. A restart
//! begins a new local knowledge horizon; no peer acceptance clock is inferred.
//! Revisions no open wait can still need are released, so waits left open on
//! a frontend that writes nothing cannot exhaust that history (#758). Behind
//! the same checkpoint, a wait only a write could close retires unmeasured
//! when no authorized session existed since its acceptance, so such waits
//! cannot exhaust the identities either and leave a later stall at -1.
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
    unknown_revision: bool,
    orphan_uncertain: bool,
    existing_revision: bool,
    /// Accepted by the node's `submitblock` reply with no active-chain
    /// observation on this frontend yet: a lost tip race until its orphan
    /// proof, or a winning offer until its first active-chain observation
    /// (#493). Never a known wait: an unlanded block has no revision to
    /// deliver. An active-chain observation lands it on this same acceptance
    /// clock; a proven orphan closes it. Reported by `unlanded_age`.
    unlanded: bool,
    /// The lowest committed revision proven to include this block's landing
    /// when its first revision is unknown (#758): the revision of a settlement
    /// that found the block already confirmed, or the one after a first
    /// confirmation that committed without a bump. It cannot start a known
    /// wait, whose age would only be an upper bound, but an eligible write at
    /// or above it proves this frontend delivered the landing and closes the
    /// unknown wait without a sample.
    covering_revision: Option<i64>,
    /// A revision assigned before the landing: its first confirmation
    /// committed without a bump (the fatal early return), so no revision at
    /// or below this one may cover it.
    landed_after: Option<i64>,
    /// Local settlement observers of this identity still to report. The one
    /// that took it from zero owns the outcome. While any is out, a delayed
    /// first confirmation may yet bind the exact revision, so a covering
    /// revision closes nothing.
    settling: u32,
    /// The newest revision observed when the earliest observer still out
    /// started, or MIN if none was: its revision is at least that, so the
    /// history from there on stays while one is out (#758).
    settling_from: i64,
    /// Revisions below this were released while the wait was open: a first
    /// revision below it can no longer find its first write.
    history_from: i64,
    /// This acceptance's place in `acceptance_epoch`, compared with
    /// `Landing::attended_through`.
    epoch: u64,
}

impl Acceptance {
    /// A known wait: open, landed on the active chain, and neither a lost
    /// settlement reply nor an unsettled orphan verdict. Only known waits
    /// carry a pending age, resolve against deliveries and attribute build
    /// deadlines; an unlanded block never has a revision to resolve.
    fn known(&self) -> bool {
        !self.closed && !self.unlanded && !self.unknown_revision && !self.orphan_uncertain
    }

    /// An unknown wait that a write at or above this revision may close: no
    /// orphan verdict is pending and no local first confirmation can still
    /// arrive to bind the exact revision (#758).
    fn covered(&self) -> Option<i64> {
        self.covering_revision.filter(|_| {
            !self.closed
                && self.unknown_revision
                && !self.orphan_uncertain
                && !self.unlanded
                && self.settling == 0
        })
    }

    /// Bind the first revision that includes the landing. If writes that
    /// could have delivered it were released, measuring would miss them, so
    /// the wait is covered by it instead, unknown until a later write (#758).
    fn bind(&mut self, revision: i64) {
        if self.revision.is_some() {
            return;
        }
        if revision < self.history_from {
            self.unknown_revision = true;
            self.cover(revision);
        } else {
            self.revision = Some(revision);
        }
    }

    fn cover(&mut self, revision: i64) {
        let floor = self.landed_after.map_or(i64::MIN, |before| before + 1);
        let lowest = self
            .covering_revision
            .map_or(revision, |known| known.min(revision));
        self.covering_revision = Some(lowest.max(floor));
    }

    fn observer_done(&mut self) {
        self.settling = self.settling.saturating_sub(1);
        if self.settling == 0 {
            self.settling_from = i64::MAX;
        }
    }
}

/// What the caller observed about a block when it opened or revisited its
/// identity (#493).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Observation {
    /// A definitive `submitblock` acceptance with no active-chain observation
    /// yet: tracked as unlanded until an observation lands it or a proven
    /// orphan closes it.
    Unlanded,
    /// A coherent active-chain observation of a block that is not yet
    /// confirmed; on an unlanded identity it lands the original acceptance.
    Active,
    /// First local proof of an already-confirmed block: it may associate the
    /// coherent current revision, but cannot upgrade an earlier acceptance.
    Confirmed,
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
    /// Whether the Stratum listener reports its authorized sessions. Until
    /// it does, any acceptance may have had a miner to write to.
    sessions_tracked: bool,
    /// Authorized Stratum sessions, as their observations report them: the
    /// sessions `qbit_prism_authorized_clients` counts.
    sessions: usize,
    /// Acceptances up to this `acceptance_epoch` had an authorized session,
    /// or untracked sessions, at some instant after them.
    attended_through: u64,
}

impl Landing {
    fn pending(&self) -> bool {
        self.pending_count > 0
    }

    /// Open waits whose delivery target is still knowable, see
    /// [`Acceptance::known`].
    fn known_waits(&self) -> impl Iterator<Item = &Acceptance> {
        self.blocks.values().filter(|block| block.known())
    }

    /// Age of the oldest open acceptance the node accepted but this frontend
    /// has not observed on its active chain (#493). Zero when none; -1 when
    /// tracking is saturated, because an unlanded acceptance may then be
    /// untracked. Independent of `age()`, which never includes these, and of
    /// `unknown()`: an unlanded block is a known state, not lost knowledge.
    pub(super) fn unlanded_age(&self) -> f64 {
        self.blocks
            .values()
            .filter(|block| !block.closed && block.unlanded)
            .map(|block| block.at)
            .min()
            .map_or(if self.saturated { -1. } else { 0. }, |at| {
                at.elapsed().as_secs_f64()
            })
    }

    /// Whether a known wait is open, which is the only state that attributes
    /// a build deadline to accepted-block revision work. Unknown tracking (a
    /// lost COMMIT reply, an unsettled orphan, saturation, lost ordering) is
    /// reported by `unknown()` and an unlanded acceptance by `unlanded_age()`;
    /// neither is counted as a delivery failure.
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
                self.pending_count -= 1;
            }
        }
        if !self.pending() {
            self.revisions.clear();
            self.delivery_count = 0;
        }
    }

    fn open_block(&mut self, hash: &str) -> Option<&mut Acceptance> {
        let mut identity = [0; 32];
        hex::decode_to_slice(hash, &mut identity).ok()?;
        self.blocks.get_mut(&identity).filter(|block| !block.closed)
    }

    /// What a committed settlement proves about the landing's revision.
    /// `required` is the revision the transaction ran at and `committed` the
    /// one it left. Only a first confirmation that bumped the revision names
    /// the first revision including the landing. One that did not (the fatal
    /// early return) proves only the revisions after `required`, which the
    /// next settlement-lock holder assigns. A settlement that found the block
    /// already confirmed proves `committed`: a first confirmation either
    /// bumps or halts the cluster in its own transaction, every settlement
    /// requires the cluster writable, and clearing a halt bumps (#758).
    fn settle(
        &mut self,
        hash: &str,
        first: bool,
        required: i64,
        committed: i64,
        owner: bool,
        counted: bool,
    ) {
        if let Some(block) = self.open_block(hash) {
            if counted {
                block.observer_done();
            }
            if first && committed > required {
                // A proven first confirmation restores the exact revision.
                if block.unknown_revision {
                    block.unknown_revision = false;
                    block.revision = None;
                }
                block.bind(committed);
            } else {
                if first {
                    block.landed_after =
                        Some(block.landed_after.map_or(required, |r| r.max(required)));
                }
                block.cover(if first { required + 1 } else { committed });
                // The owner's outcome decides the wait; another observer only
                // adds proof. A newly observed already-confirmed block may
                // begin at its current proof revision (#493).
                if block.revision.is_none() && (owner || first) {
                    if block.existing_revision && !first && !block.unknown_revision {
                        block.bind(committed);
                    } else {
                        block.unknown_revision = true;
                    }
                }
            }
        }
        if self.pending() {
            self.observe(committed);
        }
    }

    /// An active-chain observation that is not a settlement: it binds only
    /// while no settlement observer is out.
    #[cfg(test)]
    fn observe_landing(&mut self, hash: &str, revision: i64) {
        if let Some(block) = self.open_block(hash) {
            if !block.unknown_revision && block.settling == 0 {
                block.bind(revision);
            }
        }
        if self.pending() {
            self.observe(revision);
        }
    }

    fn observe(&mut self, revision: i64) {
        if let Some(event) = self.revision(revision) {
            event.observed.get_or_insert_with(Instant::now);
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
        // One pass closes what the writes prove and finds the lowest revision
        // an open wait may still need. A bound or covered wait has already
        // been matched against every recorded write, and a later write below
        // the newest observed revision is obsolete, so it needs only what is
        // still to come. A settlement observer still out needs the history
        // from the newest revision it saw; a binding still to come is at or
        // above the newest observed revision (#758).
        let mut keep_from = self
            .revisions
            .keys()
            .next_back()
            .copied()
            .unwrap_or(i64::MAX);
        for block in self.blocks.values_mut().filter(|block| !block.closed) {
            if let Some(target) = block.revision.filter(|_| block.known()) {
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
                    // An obsolete write after supersession cannot settle
                    // pending age. Replacement work must reach a socket.
                    block.closed = true;
                    self.pending_count -= 1;
                    continue;
                }
            } else if let Some(covering) = block.covered() {
                // A covered unknown wait ends at an eligible write of its
                // covering revision or later, with no sample: its first
                // revision, and so its true end, stays unknown (#758).
                if deliveries
                    .iter()
                    .any(|(revision, at)| *revision >= covering && *at >= block.at)
                {
                    block.closed = true;
                    block.unknown_revision = false;
                    self.pending_count -= 1;
                    continue;
                }
            }
            if block.settling > 0 {
                keep_from = keep_from.min(block.settling_from);
            }
            // A target bound before its block can match: only test-only
            // observations bind an unlanded block (#493).
            if let Some(target) = block
                .revision
                .filter(|_| !block.known() && !block.orphan_uncertain)
            {
                keep_from = keep_from.min(target);
            }
        }
        if !self.pending() {
            self.revisions.clear();
            self.delivery_count = 0;
        } else {
            self.release(keep_from);
        }
    }

    /// Release the revisions below `keep_from`, which no open wait can still
    /// match: waits that stay open on a frontend that writes nothing, such as
    /// an idle backup, can then never exhaust revision history and leave its
    /// later known waits at -1 (#758). A wait that later binds below what was
    /// released is covered instead of measured, see [`Acceptance::bind`].
    fn release(&mut self, keep_from: i64) {
        if self
            .revisions
            .keys()
            .next()
            .is_none_or(|&oldest| oldest >= keep_from)
        {
            return;
        }
        self.revisions = self.revisions.split_off(&keep_from);
        self.delivery_count = self
            .revisions
            .values()
            .map(|event| event.deliveries.len())
            .sum();
        for block in self.blocks.values_mut().filter(|block| !block.closed) {
            block.history_from = block.history_from.max(keep_from);
        }
    }

    /// Close, without a sample, the open waits at or below `floor` that only
    /// a write could close but that no authorized session existed to receive
    /// since their acceptance, as on an idle backup: no delivery was due, so
    /// there is nothing to measure. Left open, they would hold identities
    /// until saturation hid every later wait at -1. A wait a miner may have
    /// needed stays, so a real stall still saturates; an unlanded acceptance
    /// or an unsettled orphan verdict ends without a write either way. A wait
    /// with a settlement observer still out stays until it reports, which
    /// must find it open, and retires at a later checkpoint.
    ///
    /// Nothing is left to release: a retired wait pins no revision history.
    /// With no observer out it keeps no `settling_from`, and a landed wait
    /// with an unknown first revision has no target (see `resolve`).
    fn retire_unattended(&mut self, floor: u64) {
        if !self.sessions_tracked || self.sessions > 0 {
            return;
        }
        let attended_through = self.attended_through;
        for block in self.blocks.values_mut().filter(|block| {
            !block.closed
                && !block.unlanded
                && !block.orphan_uncertain
                && block.settling == 0
                && block.height <= floor
                && block.epoch > attended_through
        }) {
            block.closed = true;
            block.unknown_revision = false;
            self.pending_count -= 1;
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

    /// See [`Observation::Active`]. Also opens a share acknowledgement
    /// landing window (#602).
    pub(crate) fn accepted_block(&self, hash: &str, height: u64) {
        self.accept_block(hash, height, Observation::Active);
        self.landing_acks.opened(Instant::now(), hash);
    }

    /// See [`Observation::Unlanded`] (#493). Also opens a share
    /// acknowledgement landing window (#602).
    pub(crate) fn accepted_unlanded_block(&self, hash: &str, height: u64) {
        self.accept_block(hash, height, Observation::Unlanded);
        self.landing_acks.opened(Instant::now(), hash);
    }

    /// See [`Observation::Confirmed`]. `at_tip` is whether the block is the
    /// chain's tip in the observation. Only then does it open a landing
    /// window (#602): a peer frontend's block that settled before this
    /// frontend's reconciler first saw it is still landing now, while the
    /// buried blocks a restart first sees landed long ago. A restart whose
    /// tip is a pool block opens one window.
    pub(crate) fn accepted_landed_block(&self, hash: &str, height: u64, at_tip: bool) {
        self.accept_block(hash, height, Observation::Confirmed);
        if at_tip {
            self.landing_acks.opened(Instant::now(), hash);
        }
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

    fn accept_block(&self, hash: &str, height: u64, observation: Observation) {
        self.landing_event(|state, _| {
            let mut identity = [0; 32];
            if hex::decode_to_slice(hash, &mut identity).is_err() {
                state.saturated = true;
                return;
            }
            if let Some(block) = state.blocks.get_mut(&identity) {
                // An active-chain observation lands an earlier accepted offer
                // on its original acceptance clock. Nothing downgrades a
                // landed block, and a tombstone stays closed.
                if observation != Observation::Unlanded && !block.closed {
                    block.unlanded = false;
                }
                return;
            }
            if state.saturated || height <= state.retired_height {
                return;
            }
            if state.blocks.len() == LIMIT {
                state.saturated = true;
                return;
            }
            let epoch = state.acceptance_epoch.saturating_add(1);
            state.blocks.insert(
                identity,
                Acceptance {
                    at: Instant::now(),
                    height,
                    revision: None,
                    closed: false,
                    degraded: None,
                    unknown_revision: false,
                    orphan_uncertain: false,
                    existing_revision: observation == Observation::Confirmed,
                    unlanded: observation == Observation::Unlanded,
                    covering_revision: None,
                    landed_after: None,
                    settling: 0,
                    settling_from: i64::MAX,
                    history_from: i64::MIN,
                    epoch,
                },
            );
            state.pending_count += 1;
            state.acceptance_epoch = epoch;
        });
    }

    /// The caller proved this committed revision is the first to include the
    /// block. A later proof cannot move the original target or restart its
    /// acceptance clock.
    #[cfg(test)]
    pub(crate) fn landed_block(&self, hash: &str, revision: i64) {
        self.landing_event(|state, registry| {
            state.settle(hash, true, revision - 1, revision, false, false);
            state.resolve(registry);
        });
    }

    #[cfg(test)]
    pub(crate) fn observed_landed_block(&self, hash: &str, revision: i64) {
        self.landing_event(|state, registry| {
            state.observe_landing(hash, revision);
            state.resolve(registry);
        });
    }

    pub(crate) fn revision_work_observed(&self, revision: i64) {
        self.landing_event(|state, registry| {
            if state.pending() {
                state.observe(revision);
                state.resolve(registry);
            }
        });
    }

    /// The existing coherent mature checkpoint ends the history horizon.
    /// Retain unresolved intervals a miner may have needed, and retire the
    /// rest unmeasured (#758); reject delayed proofs below the watermark even
    /// after their identity has been released.
    pub(crate) fn revision_work_matured(&self, height: u64) {
        let mut state = self.landing.lock().unwrap_or_else(|e| e.into_inner());
        state.retired_height = state.retired_height.max(height);
        let floor = state.retired_height;
        state.retire_unattended(floor);
        state
            .blocks
            .retain(|_, block| !block.closed || block.height > floor);
    }

    /// The Stratum listener reports its authorized sessions from now on; an
    /// acceptance before this may have had a miner (#758).
    pub(crate) fn revision_work_sessions_tracked(&self) {
        let mut state = self.landing.lock().unwrap_or_else(|e| e.into_inner());
        if !state.sessions_tracked {
            state.attended_through = state.acceptance_epoch;
            state.sessions_tracked = true;
        }
    }

    /// A Stratum session authorized, as `qbit_prism_authorized_clients`
    /// counts it: every wait open while it stays may need its write.
    pub(crate) fn revision_work_session_authorized(&self) {
        let mut state = self.landing.lock().unwrap_or_else(|e| e.into_inner());
        state.sessions += 1;
    }

    /// An authorized session ended. It existed after every acceptance so far.
    pub(crate) fn revision_work_session_ended(&self) {
        let mut state = self.landing.lock().unwrap_or_else(|e| e.into_inner());
        state.attended_through = state.acceptance_epoch;
        state.sessions = state.sessions.saturating_sub(1);
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
        let (owner, counted) = self.landing_event(|state, _| {
            let newest = state.revisions.keys().next_back().copied();
            let Some(block) = state.open_block(hash) else {
                return (false, false);
            };
            // Only an active-chain observation reaches a settlement COMMIT:
            // whatever its reply, the block is landed (#493).
            block.unlanded = false;
            let owner = block.settling == 0;
            block.settling += 1;
            // Its transaction runs under the settlement lock at a revision no
            // older than any observed so far, so it can need nothing older.
            block.settling_from = block.settling_from.min(newest.unwrap_or(i64::MIN));
            (owner, true)
        });
        Settlement {
            metrics: self,
            hash,
            completed: false,
            owner,
            counted,
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
/// A later non-first confirmation proves only a covering revision, which
/// closes the wait at a write without restoring its original revision (#758).
pub(crate) struct Settlement<'a> {
    metrics: &'a Metrics,
    hash: &'a str,
    completed: bool,
    /// The observer that took the identity's `settling` from zero: its
    /// outcome decides the wait, and its lost reply makes it unknown.
    owner: bool,
    /// Counted in the identity's `settling` until it reports or drops.
    counted: bool,
}
impl Settlement<'_> {
    /// `required` is the revision the transaction required and `committed`
    /// the one it committed; the tracker decides what they prove (#758).
    pub(crate) fn committed(mut self, first: bool, required: i64, committed: i64) {
        self.metrics.landing_event(|state, registry| {
            // Arrival order is not commit order: a competing transaction can
            // prove the first confirmation while the owning observer is delayed.
            state.settle(
                self.hash,
                first,
                required,
                committed,
                self.owner,
                self.counted,
            );
            state.resolve(registry);
        });
        self.completed = true;
    }
}
impl Drop for Settlement<'_> {
    fn drop(&mut self) {
        if self.completed || !self.counted {
            return;
        }
        self.metrics.landing_event(|state, registry| {
            if let Some(block) = state.open_block(self.hash) {
                block.observer_done();
                if self.owner && block.revision.is_none() {
                    block.unknown_revision = true;
                }
            }
            // The last observer out may release a covered wait whose write
            // was already seen (#758).
            state.resolve(registry);
        });
    }
}

/// Consumption gives each actual timeout branch one event. Drop (including
/// cancellation and successful builds) deliberately records no timeout.
/// `pending` is the known-wait state at the build's start; the deadline also
/// looks at the state when it fires. A deadline hit while only unknown
/// tracking remained at both instants is an ordinary job-delivery failure,
/// not a revision-work failure, and leaves that tracking unknown (#493).
pub(crate) struct Build<'a> {
    metrics: &'a Metrics,
    at: Instant,
    pending: bool,
}
impl Build<'_> {
    pub(crate) fn deadline_hit(self) {
        self.metrics.landing_event(|state, registry| {
            // Attributed if a known wait was open at the build's start, or if
            // one accepted before the build is open now: a wait that recovered
            // from unknown mid-build would have received this build's work.
            // Unknown-only tracking at both instants stays unknown (#493).
            let attributed = self.pending
                || (!state.ordering_lost && state.known_waits().any(|block| block.at <= self.at));
            if !attributed {
                return;
            }
            registry.increment(Family::RevisionWorkTimeouts, Labels::Empty);
            // An unlanded block has no revision work yet, so a deadline hit
            // before it lands cannot have degraded its delivery (#493).
            for block in state
                .blocks
                .values_mut()
                .filter(|block| !block.closed && !block.unlanded && block.at <= self.at)
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
