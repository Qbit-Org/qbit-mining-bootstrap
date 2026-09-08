#!/usr/bin/env python3
"""Candidate-storm characterization instrument for PRISM.

The 2026-08-20 testnet4 incident produced 3,120 durable block candidates,
well above both the 32-entry live queue and the 1,024-row replay-enumeration
page.  This instrument reproduces that cardinality against the shipped
in-memory ledger and coordinator paths, then reports the component counts on
both sides of a process restart.

The ownership counts are deliberately first-class output.  Live admission
marks a candidate outstanding *before* attempting the bounded queue, so the
marker covers coalesced wakeups as well as queued ones.  Restart adoption
marks every restored row replay-inflight before it is processed.  Those
markers mean "this process still owns eventual disposition", not "qbitd has
already been offered this candidate".  Any maintenance selector that treats
either whole set as active node-offer evidence will exclude the whole storm.
A wakeup parked in the retry holder is the same kind of marker for the same
reason: ``_retain_block_candidate_for_retry`` also runs when the fast-lane
reservation declines, before any submitblock.  So is a claimed disposition
lease: ``submit_next`` takes it on the dequeued hash *before* the terminal,
capacity and fast-lane checks, each of which releases it again having
offered nothing.  So, less obviously, is a recorded terminal outcome: when
pool capacity is already closed the deferred split builds a node submission
with ``attempted=False`` and lets accounting terminalize the durable row
without calling qbitd once, and the durable ``attempt_count`` is marked on
that same zero-RPC path.  The rig therefore reports the attested offers
(``node_offer_evidence``) apart from the conservative must-not-abandon union
(``selector_evidence``) that a selector owes the #183 contract, so neither a
retry holder, nor a held lease, nor a terminal seal is ever mistaken for a
node call.

This is a component instrument, not a performance threshold.  It uses the
real coordinator admission, durable codec, pagination, payout barriers, and
``SingleWriterShareLedger``; it does not include PostgreSQL wait or qbitd.
Issues #187 and #185 can extend the same rig with wall-clock restart/drain
assertions and component-memory gauges without rebuilding the storm fixture.

The decided-height extension models the instant after the storm height is
settled: ``decide_height`` makes one candidate the active block at that
height (``DecidedHeightRpc``), ``ownership`` reads every candidate's
ownership and node-offer-evidence facts from the shipped state so a
maintenance selector can be evaluated against the real population,
``perturb`` installs one selector-evidence shape at a time through the
shipped entry points, and ``drain_per_row`` drives the shipped per-row
disposition path in the production split -- ``submit_next`` with
``defer_accounting=True`` handing every node offer to the real accounting
queue and task runner -- reporting the exact hash sets it terminalized and
what they cost.  The per-row drain is the oracle any set-oriented selector
has to agree with; the instrument does not contain a selector itself, and
suppresses the shipped #183 collapse for the duration of the drain so the
oracle stays the per-row path even where the drain re-enumerates.

Driving both lanes from one thread is what makes the drain deterministic,
and it costs nothing in fidelity: the two lanes are already serialized per
hash by the disposition lease, and the drain stamps the accounting-thread
identity the shipped loop stamps, so retry pacing records a not-before
deadline exactly as it does on the real accounting thread.  Splitting the
drive this way also matters for correctness, not just fidelity:
``submit_next`` claims the disposition lease *blocking* unless accounting
is deferred, and ``_restore_replayed_candidate_acceptance_evidence`` runs
only inside the accounting task.

The cleanup-backlog extension (issue #198) measures the one collapse state
with no durable replay source: the in-memory retry record a won row leaves
behind when its post-write cleanup fails.  ``CleanupFaultInjector`` breaks
exactly one cleanup dependency (``CLEANUP_FAULTS``) at the shipped seam
while counting every seam per hash, and ``measure_cleanup_backlog`` drives
the shipped collapse walk under that fault, reports the backlog it produces
-- depth, oldest age, retained pending-share holders, terminal-outcome pins,
deep and traced bytes per record, and the rows the admission bound
preserved -- holds the fault for a bounded run of failing retry passes,
heals it, and then proves recovery: the backlog drains through the shipped
retry pass at a measured throughput, admission resumes, every collapsed
hash runs each cleanup seam exactly once more (no double cleanup), no
terminal row is replay-adopted or re-offered to the node, and the decided
winner keeps its pending-share floor authority.
"""

from __future__ import annotations

import argparse
import gc
import json
import linecache
import os
import platform
import queue
import random
import subprocess
import sys
import threading
import time
import tracemalloc
from contextlib import ExitStack, nullcontext, redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ContextManager, Iterable, Sequence

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.block_candidates import (  # noqa: E402
    BLOCK_CANDIDATE_COLLAPSE_CLEANUP_PAYOUT_STEP,
    BLOCK_CANDIDATE_COLLAPSE_CLEANUP_STEPS,
    COLLAPSE_DIFFICULTY_SCALE,
    COLLAPSE_POW_LIMIT_BITS,
    MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
    MAX_PENDING_BLOCK_CANDIDATES,
    _BlockCandidateNodeSubmission,
)
from lab.prism.coordinator_config import (  # noqa: E402
    DEFAULT_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX,
)
from lab.prism.process_telemetry import (  # noqa: E402
    MallocTrimmer,
    ProcessHeapTelemetry,
    read_anonymous_map_shape,
    read_malloc_arena_count,
    read_malloc_info_summary,
    read_resident_memory_bytes,
)
from lab.prism.share_ledger import (  # noqa: E402
    PendingShare,
    SingleWriterShareLedger,
)
from lab.prism.template_artifacts import scaled_network_difficulty  # noqa: E402
from tests.prism_vardiff_test_support import (  # noqa: E402
    FakeRpc,
    block_candidate,
    submit_coordinator,
)


OBSERVED_TESTNET_CANDIDATE_STORM = 3_120
STORM_PARENT_HASH = "11" * 32

# The selector-evidence shapes record_abandoned and submit_next consult,
# each installable on one seeded candidate through CandidateStormRig.perturb.
PERTURBATIONS = (
    "lease_held",
    "retained_success",
    "tip_observed",
    "retry_slot",
    "prepared_pool_block",
    "terminal",
)

# Issue #198: every in-memory dependency one won row's terminal cleanup runs
# through, each injectable as a fault through CleanupFaultInjector.  The
# first seven are the shipped cleanup steps, named exactly as the retry
# registry names what a hash still owes; ``floor-index`` is the page-scope
# pending-share holder scan that precedes them, whose failure aborts the
# whole apply's cleanup and is the path the retry re-indexes for.
CLEANUP_FAULTS = tuple(BLOCK_CANDIDATE_COLLAPSE_CLEANUP_STEPS) + ("floor-index",)
# Install the seam counters without breaking anything: the clean baseline
# every marginal per-record figure is taken against.
CLEANUP_FAULT_NONE = "none"
# seam -> (owner, attribute, successful calls one collapsed hash is owed).
# The payout withdrawal is two calls (invalidate, then drop the tombstone);
# the floor release is once per retained holder and the holder index once
# per page, so neither has a per-hash expectation.
_CLEANUP_SEAMS: dict[str, tuple[str, str, int | None]] = {
    BLOCK_CANDIDATE_COLLAPSE_CLEANUP_PAYOUT_STEP: (
        "server",
        "_clear_accepted_block_payout_preview",
        2,
    ),
    "finalize-retry": ("service", "finalize_retries", 1),
    "retry-state": ("server", "_clear_block_candidate_retry_state", 1),
    "outstanding-and-tip-observation": (
        "server",
        "_discard_outstanding_block_candidate",
        1,
    ),
    "terminal-outcome": ("server", "_record_block_candidate_terminal_outcome", 1),
    "abandonment-accounting": (
        "server",
        "_record_committed_block_candidate_abandonment",
        1,
    ),
    "pending-share-floor": ("server", "_finish_pending_share_commit", None),
    "floor-index": ("service", "_collapsed_candidate_floor_holders", None),
}
assert set(_CLEANUP_SEAMS) == set(CLEANUP_FAULTS)


@dataclass(frozen=True)
class CandidateStormSnapshot:
    """Bounded component gauges after one phase of the storm."""

    durable_pending: int
    live_queue_capacity: int
    live_queued: int
    live_coalesced: int
    outstanding_marked: int
    outstanding_covers_all_durable: bool
    replay_queued: int
    replay_inflight_marked: int
    replay_inflight_covers_all_durable: bool
    accepted_parent_previews: int
    replay_enumeration_owed: bool
    # Candidates the #183 must-not-abandon contract covers, i.e. carrying
    # any evidence that a node offer happened, is in flight, or cannot be
    # ruled out (see CandidateOwnership.selector_evidence). The
    # outstanding and replay-inflight markers above are deliberately NOT
    # part of it.
    selector_evidence_marked: int = 0
    # How that union splits: rows something actually attests a node offer
    # for, and the three widenings that attest nothing -- a wakeup parked in
    # a retry holder, a claimed disposition lease, and a recorded terminal
    # outcome.  None is by itself evidence qbitd was ever called; production
    # reaches all three without a submitblock.  They can coincide on one row,
    # so they are counted separately rather than folded into one residue.
    node_offer_evidence_marked: int = 0
    unoffered_retry_marked: int = 0
    unoffered_lease_marked: int = 0
    unoffered_terminal_marked: int = 0


@dataclass(frozen=True)
class CandidateOwnership:
    """Ownership and evidence facts for one durable candidate at one instant.

    Every field is read from the shipped coordinator/ledger state, so a
    maintenance selector can be evaluated against the real population
    instead of a hand-built corpus. ``outstanding`` and ``replay_inflight``
    are process-ownership markers; the fields folded into
    ``node_offer_evidence`` are the ones that mean a node offer happened,
    is happening, or left acceptance evidence behind. ``retry_held``,
    ``disposition_held`` and ``terminal_outcome`` are none of those: they
    are a third category, covered by the conservative ``selector_evidence``
    union without attesting an offer.
    """

    block_hash: str
    expected_height: int
    parent_hash: str
    network_difficulty: float | None
    outbox_state: str | None
    attempt_count: int
    live_queued: bool
    replay_queued: bool
    outstanding: bool
    replay_inflight: bool
    preview_barrier: bool
    disposition_held: bool
    retry_held: bool
    node_acceptance_retained: bool
    tip_observed: bool
    accounted_accepted: bool
    terminal_outcome: bool | None
    pool_block_chain_state: str | None

    @property
    def node_offer_evidence(self) -> bool:
        """Whether anything attests that qbitd was offered this hash.

        Every fact here is written by the offer or downstream of it: a
        retained submission records an offer that already returned, a tip
        observation and an accounted acceptance are readings of an accepted
        offer, and a pool-block row is what an accepted offer's payout
        preparation left behind.  ``retry_held``, ``disposition_held`` and
        ``terminal_outcome`` are deliberately absent; see
        :attr:`retry_held_without_offer`, :attr:`lease_held_without_offer`
        and :attr:`terminal_without_offer`.
        """
        return bool(
            self.node_acceptance_retained
            or self.tip_observed
            or self.accounted_accepted
            or self.pool_block_chain_state is not None
        )

    @property
    def retry_held_without_offer(self) -> bool:
        """A wakeup parked in a retry holder with no offer attested for it.

        ``_retain_block_candidate_for_retry`` also runs on paths that never
        reached qbitd -- most plainly when ``_reserve_block_fast_lane_slot``
        declines and ``submit_next`` parks the candidate *before* any
        submitblock.  Retry retention on its own therefore says only "this
        process still owes this row a disposition", exactly like
        ``outstanding`` and ``replay_inflight``, and reading it as an offer
        would credit the rig with a node call that never happened.
        """
        return bool(self.retry_held) and not self.node_offer_evidence

    @property
    def lease_held_without_offer(self) -> bool:
        """A claimed disposition lease with no offer attested for it.

        ``submit_next`` claims the lease on the dequeued hash *before* it
        consults the terminal outcome, before the accounted/capacity close,
        and before ``_reserve_block_fast_lane_slot``.  Each of those paths
        releases the lease again without ever reaching submitblock, and the
        deferred split hands the still-held lease to accounting even when the
        node submission is ``attempted=False``.  The registry this field is
        read from is weaker still: ``_claim_block_candidate_disposition``
        registers a user on the flight *before* it takes the flight's lock,
        so the key is present for a blocked waiter as well as for the holder.
        A held lease therefore says only "some pass owns this row's eventual
        disposition", exactly like ``outstanding`` and ``replay_inflight``,
        and reading it as an offer would credit the rig with a node call that
        never happened.
        """
        return bool(self.disposition_held) and not self.node_offer_evidence

    @property
    def terminal_without_offer(self) -> bool:
        """A recorded terminal outcome with no offer attested for it.

        ``_record_block_candidate_terminal_outcome`` means "this process
        finished deciding this row", not "this process offered it".  The
        deferred submitter split reaches it having called nothing: when pool
        capacity is already closed -- ``accepted_block_count`` has reached
        ``max_blocks``, or ``stop_after_block`` is set and one block landed
        -- ``submit_next`` builds a ``_BlockCandidateNodeSubmission(
        attempted=False)`` and hands it straight to accounting, which
        terminalizes the durable outbox row without a single ``submitblock``
        (the already-accounted and finalize-only replay paths do the same).

        ``attempt_count`` is no substitute for the missing RPC either:
        ``submit_writer`` calls ``_mark_block_candidate_attempted`` *before*
        the offer branch, and the ledger documents the mark as "admission to
        a real processing phase", so the durable row records an attempt on
        exactly the same zero-RPC path.

        Excluding the terminal registry from :attr:`node_offer_evidence`
        costs that gauge no attestation it legitimately had: finalization
        adds an accepted hash to ``_accounted_accepted_block_hashes`` and
        persists its pool-block row before the terminal outcome is recorded,
        so a candidate that really was offered and accepted still attests it
        in its own right.
        """
        return self.terminal_outcome is not None and not self.node_offer_evidence

    @property
    def selector_evidence(self) -> bool:
        """The conservative must-not-abandon union the #183 contract needs.

        This is :attr:`node_offer_evidence` widened by ``retry_held``, by
        ``disposition_held``, and by a recorded ``terminal_outcome``.  Each
        widening covers an ambiguity rather than an attestation: a parked
        wakeup may equally have come from a path that *did* offer and is
        retrying its tail, a claimed lease may equally span an offer already
        in flight, and a terminal outcome may equally be the seal on an offer
        that returned.  Nothing the selector can read tells the two apart, so
        it has to leave the row to the per-row path either way -- and a row
        whose lease it cannot claim, or whose disposition this process has
        already fixed, is not one it could dispose of anyway.  Because the
        widening is deliberately conservative, a true value here is not a
        claim that qbitd was offered the hash -- read
        :attr:`node_offer_evidence` for that, and
        :attr:`retry_held_without_offer` / :attr:`lease_held_without_offer` /
        :attr:`terminal_without_offer` for the rows the widenings add.
        """
        return (
            self.node_offer_evidence
            or bool(self.retry_held)
            or bool(self.disposition_held)
            or self.terminal_outcome is not None
        )


@dataclass(frozen=True)
class PerRowDrainReport:
    """What the shipped per-row disposition path did, and cost, over a storm.

    Every field reports *one* invocation of :meth:`CandidateStormRig.drain_per_row`.
    ``abandoned_hashes`` is the oracle a set-oriented selector has to agree
    with: the exact set of durable rows that call terminalized as abandoned,
    not a count, and not every abandoned row standing in the rig.  It and
    ``submitted_hashes`` are transitions -- a row already terminal when the
    call started is left out -- so successive bounded drains (``max_rounds``)
    partition the work between them instead of each claiming the whole
    history.  ``pending_hashes`` is the complementary residue: the rows still
    awaiting disposition when the call returned, which is a state rather than
    a transition because nothing ever transitions *into* pending.

    ``deferred_hashes`` and ``lease_blocked_hashes`` are the oracle's
    counterweight -- rows the drain reached and the shipped path deliberately
    refused to terminalize -- so a selector that claims one of them can be
    caught rather than silently believed.

    ``rounds`` is the number of node offers the call actually performed --
    the ``submitblock`` delta in the rig's own RPC log -- not the number of
    wakeups it consumed.  ``submit_next_block_candidate`` returns True on
    several paths that never reach qbitd: a disposition lease held elsewhere
    parks the wakeup in ``_block_disposition_waiting_retries``, a same-hash
    duplicate whose disposition already landed is dropped, a declined
    fast-lane reservation retains the candidate for retry, and the
    capacity-closed split hands accounting an ``attempted=False`` submission
    that terminalizes the durable row without calling qbitd once.  Counting
    those would make ``rounds`` disagree with this report's own ``rpc_calls``,
    ``accounting_tasks`` and ``ledger_attempt_marks``, and would let a
    ``max_rounds`` budget be spent without a single offer being made.
    """

    rounds: int
    accounting_tasks: int
    replay_enumerations: int
    abandoned_hashes: frozenset[str]
    submitted_hashes: frozenset[str]
    pending_hashes: frozenset[str]
    withheld_hashes: frozenset[str]
    lease_blocked_hashes: frozenset[str]
    deferred_hashes: frozenset[str]
    rpc_calls: dict[str, int]
    ledger_attempt_marks: int
    wall_seconds: float

    @property
    def abandoned_rows(self) -> int:
        return len(self.abandoned_hashes)

    @property
    def submitted_rows(self) -> int:
        return len(self.submitted_hashes)

    @property
    def pending_rows(self) -> int:
        return len(self.pending_hashes)


class _FaultableRegistry(dict):
    """``finalize_retries`` with a breakable ``pop``.

    The finalize-retry cleanup step reaches no coordinator seam -- it pops
    the service's own registry under the lock -- so the fault has to live
    on the container.  Membership reads stay plain dict reads.
    """

    def __init__(self, injector: "CleanupFaultInjector", seam: str, contents: Any) -> None:
        super().__init__(contents)
        self._injector = injector
        self._seam = seam

    def pop(self, key: Any, *default: Any) -> Any:  # type: ignore[override]
        self._injector._guard(self._seam)
        result = super().pop(key, *default)
        self._injector._count(self._seam, str(key).lower())
        return result


class CleanupFaultInjector:
    """Break one cleanup dependency until healed; count every seam meanwhile.

    Every seam the terminal cleanup runs through is wrapped so a *successful*
    call is counted per hash (per holder for the floor release, per page for
    the holder index), and exactly one of them -- ``fault`` -- raises while
    ``on`` is true.  Nothing else is replaced: the collapse, the fence, the
    retry registry and the retry pass all stay as shipped, so the backlog
    this produces is the backlog production would produce under the same
    fault.  ``CLEANUP_FAULT_NONE`` installs the counters alone, which is the
    clean baseline the marginal memory figure is taken against.

    The counters are what make recovery provable rather than believed: after
    healing, every collapsed hash must show exactly the calls one terminal
    cleanup is owed at every seam -- one more than that is a double cleanup,
    one fewer is a step the retry never finished -- and a floor holder must
    be released at most once by identity.
    """

    def __init__(self, server: Any, fault: str) -> None:
        if fault != CLEANUP_FAULT_NONE and fault not in CLEANUP_FAULTS:
            raise ValueError(f"unknown cleanup fault: {fault}")
        self.server = server
        self.service = server._ensure_block_candidate_service()
        self.fault = fault
        self.on = fault != CLEANUP_FAULT_NONE
        self.failed_calls = 0
        # seam -> key -> successful calls.
        self.calls: dict[str, dict[str, int]] = {seam: {} for seam in _CLEANUP_SEAMS}
        # Floor releases by holder identity.
        self.floor_releases: dict[int, int] = {}
        self._originals: dict[str, tuple[Any, str, Any]] = {}

    def install(self) -> None:
        if self._originals:
            raise RuntimeError("cleanup faults are already installed")
        for seam, (owner_name, attribute, _expected) in _CLEANUP_SEAMS.items():
            owner = self.server if owner_name == "server" else self.service
            original = getattr(owner, attribute)
            self._originals[seam] = (owner, attribute, original)
            if seam == "finalize-retry":
                setattr(owner, attribute, _FaultableRegistry(self, seam, original))
                continue
            setattr(owner, attribute, self._wrap(seam, original))

    def uninstall(self) -> None:
        for seam, (owner, attribute, original) in self._originals.items():
            if seam == "finalize-retry":
                # Hand the live contents back to the shipped container.
                current = getattr(owner, attribute)
                original.clear()
                original.update(current)
            setattr(owner, attribute, original)
        self._originals.clear()

    def heal(self) -> None:
        self.on = False

    def _wrap(self, seam: str, original: Any) -> Any:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            self._guard(seam)
            result = original(*args, **kwargs)
            self._count(seam, self._key(seam, args, kwargs))
            return result

        return wrapped

    def _guard(self, seam: str) -> None:
        if self.on and seam == self.fault:
            self.failed_calls += 1
            raise RuntimeError(f"injected cleanup fault: {seam}")

    def _key(self, seam: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        if seam == "pending-share-floor":
            share = args[0] if args else kwargs["pending_share"]
            self.floor_releases[id(share)] = self.floor_releases.get(id(share), 0) + 1
            # The rig stamps ``share_id`` as ``miner:<hash>``.
            return str(getattr(share, "share_id", "")).rsplit(":", 1)[-1].lower()
        if seam == "floor-index":
            return "page"
        value = args[0] if args else kwargs.get("block_hash", "")
        return str(value).lower()

    def _count(self, seam: str, key: str) -> None:
        counts = self.calls[seam]
        counts[key] = counts.get(key, 0) + 1

    def snapshot_calls(self) -> dict[str, dict[str, int]]:
        return {seam: dict(counts) for seam, counts in self.calls.items()}

    def _expected(self, seam: str, expected: int) -> int:
        # An aborted apply -- the holder index raising -- has already run the
        # payout withdrawal for the whole page before it stops, and the
        # retry then re-runs every step because the abort proved nothing
        # about which ones completed. That documented repeat of an
        # idempotent step is the shipped abort contract, not a double
        # cleanup, so the expectation carries it.
        if self.fault == "floor-index" and seam == BLOCK_CANDIDATE_COLLAPSE_CLEANUP_PAYOUT_STEP:
            return 2 * expected
        return expected

    def excess_calls(self, hashes: Iterable[str]) -> dict[str, int]:
        """Per seam, how many of ``hashes`` were cleaned up more than once."""
        wanted = {str(value).lower() for value in hashes}
        return {
            seam: sum(
                1
                for key in wanted
                if self.calls[seam].get(key, 0) > self._expected(seam, expected)
            )
            for seam, (_owner, _attribute, expected) in _CLEANUP_SEAMS.items()
            if expected is not None
        }

    def calls_since(
        self,
        earlier: dict[str, dict[str, int]],
        hashes: Iterable[str],
    ) -> int:
        """Successful seam calls for ``hashes`` made after ``earlier`` was taken."""
        wanted = {str(value).lower() for value in hashes}
        return sum(
            counts.get(key, 0) - earlier.get(seam, {}).get(key, 0)
            for seam, counts in self.calls.items()
            for key in wanted
        )

    def missing_calls(self, hashes: Iterable[str]) -> dict[str, int]:
        """Per seam, how many of ``hashes`` still lack their one cleanup."""
        wanted = {str(value).lower() for value in hashes}
        return {
            seam: sum(1 for key in wanted if self.calls[seam].get(key, 0) < expected)
            for seam, (_owner, _attribute, expected) in _CLEANUP_SEAMS.items()
            if expected is not None
        }

    @property
    def floor_double_releases(self) -> int:
        return sum(1 for count in self.floor_releases.values() if count > 1)


def _deep_size(value: Any, seen: set[int]) -> int:
    """Bytes reachable from ``value`` through containers and instance dicts.

    Each object is charged once per ``seen`` set, so a string or holder
    shared by two records is counted for the first and free for the second
    -- the same accounting the process pays.
    """
    if id(value) in seen:
        return 0
    seen.add(id(value))
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        for key, item in value.items():
            size += _deep_size(key, seen) + _deep_size(item, seen)
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            size += _deep_size(item, seen)
    elif hasattr(value, "__dict__"):
        size += _deep_size(vars(value), seen)
    slots = getattr(type(value), "__slots__", ())
    if isinstance(slots, str):
        slots = (slots,)
    for name in slots:
        if hasattr(value, name):
            size += _deep_size(getattr(value, name), seen)
    return size


@dataclass(frozen=True)
class CleanupBacklogReport:
    """One faulted collapse walk, its backlog, and the recovery that drained it.

    Every figure is from one :meth:`CandidateStormRig.measure_cleanup_backlog`
    call.  The memory figures are two independent readings of the same
    state: ``registry_bytes`` walks the shipped registry with
    ``sys.getsizeof`` (records, their step sets, hash keys, and the exact
    holder objects they retain, each object charged once), while
    ``traced_walk_bytes`` is ``tracemalloc``'s change in live traced bytes
    across the faulted walk -- which also carries everything else the walk
    allocated (fences, counters, the adopted remainder), so the per-record
    marginal figure is taken against a ``CLEANUP_FAULT_NONE`` walk of the
    same storm by :func:`cleanup_backlog_marginal_bytes_per_record`.

    The recovery block is the contract this measurement exists to prove:
    ``excess_cleanup_calls`` and ``floor_double_releases`` must be zero (no
    double cleanup), ``missing_cleanup_calls`` must be zero (every owed
    step ran), ``terminal_rows_adopted`` must be zero (no replay adoption of
    a terminal row), ``node_offers`` and ``drain_rounds`` must be zero (no
    re-offer), and ``winner_floor_holder_retained`` must hold when the rig
    seeded credit shares (no loss of payout-floor authority).
    """

    fault: str
    view: str
    candidates: int
    credit_shares: bool
    backlog_max: int
    # -- the faulted walk --------------------------------------------------
    walk_seconds: float
    collapsed_rows: int
    deferred_records: int
    preserved_rows: int
    backpressure_engagements: int
    backpressure_active: bool
    pending_share_holders: int
    terminal_outcome_pins: int
    oldest_age_seconds: float
    registry_bytes: int
    holder_bytes: int
    registry_bytes_per_record: float
    holder_bytes_per_holder: float
    traced_walk_bytes: int
    traced_owner_bytes: int
    traced_peak_bytes: int
    # -- sustained failure -------------------------------------------------
    sustained_passes: int
    sustained_failed_passes: int
    depth_after_sustained: int
    holders_after_sustained: int
    pins_after_sustained: int
    # -- recovery ----------------------------------------------------------
    recovery_seconds: float
    recovery_passes: int
    recovered_records: int
    retry_records_per_second: float
    depth_after_recovery: int
    second_walk_seconds: float
    collapsed_rows_after_recovery: int
    excess_cleanup_calls: dict[str, int]
    missing_cleanup_calls: dict[str, int]
    post_recovery_cleanup_calls: int
    floor_double_releases: int
    terminal_rows_adopted: int
    node_offers: int
    drain_rounds: int
    pending_rows_after_drain: int
    floor_holders_before: int
    floor_holders_after: int
    winner_floor_holder_retained: bool

    @property
    def recovered_cleanly(self) -> bool:
        return (
            self.depth_after_recovery == 0
            and not any(self.excess_cleanup_calls.values())
            and not any(self.missing_cleanup_calls.values())
            and self.post_recovery_cleanup_calls == 0
            and self.floor_double_releases == 0
            and self.terminal_rows_adopted == 0
            and self.node_offers == 0
            and self.drain_rounds == 0
        )


def cleanup_backlog_marginal_bytes_per_record(
    faulted: CleanupBacklogReport,
    clean: CleanupBacklogReport,
    *,
    owner_only: bool = False,
) -> float:
    """Traced bytes one retry record cost over a clean walk of the same storm.

    ``owner_only`` restricts both readings to allocations made by the
    block-candidate owner module, which is where the record, its step set
    and its registry entry are allocated.
    """
    if faulted.deferred_records <= 0:
        return 0.0
    if owner_only:
        return (faulted.traced_owner_bytes - clean.traced_owner_bytes) / faulted.deferred_records
    return (faulted.traced_walk_bytes - clean.traced_walk_bytes) / faulted.deferred_records


def _warm_traceback_caches() -> None:
    """Load the source lines a faulted cleanup's tracebacks will print.

    ``traceback.print_exc`` fills ``linecache`` with the whole source of
    every frame's file the first time it runs, hundreds of kilobytes that
    would otherwise land inside the first faulted walk's traced delta.
    """
    import lab.prism.block_candidates as block_candidates

    for path in (block_candidates.__file__, __file__):
        linecache.getlines(path)


def _owner_traced_bytes(snapshot: Any) -> int:
    """Live traced bytes allocated by the block-candidate owner module."""
    import lab.prism.block_candidates as block_candidates

    filtered = snapshot.filter_traces(
        (tracemalloc.Filter(True, block_candidates.__file__),)
    )
    return sum(statistic.size for statistic in filtered.statistics("filename"))


class DecidedHeightRpc(FakeRpc):
    """A chain whose best tip is one storm candidate at the storm height.

    Models the instant after a height is decided: ``getbestblockhash`` and
    ``getblockhash(height)`` name the winner, the winner's header is active
    with one confirmation, every other candidate header is unknown, and a
    further ``submitblock`` answers ``inconclusive`` (a valid block that did
    not become the tip). Calls are counted per method so a drain's RPC cost
    is measurable; the counter is thread-safe because the shipped fast lane
    runs ``submitblock`` on a bounded worker thread.
    """

    def __init__(
        self,
        *,
        winner_hash: str,
        height: int,
        bits: str = COLLAPSE_POW_LIMIT_BITS,
    ) -> None:
        self.winner_hash = winner_hash.lower()
        self.height = int(height)
        # The decided block's compact bits as qbitd would report them in
        # getblockheader. Work is stated in bits, not in the raw difficulty
        # float, because a candidate row carries scaled difficulty
        # (raw * COLLAPSE_DIFFICULTY_SCALE) and only a bits-derived value is
        # in the same units. The default is the qbit powLimit, at or below
        # every sibling in the storm, so the decided block never reads as
        # the weaker chain.
        self.bits = str(bits).lower()
        self.difficulty = (
            scaled_network_difficulty(self.bits) / COLLAPSE_DIFFICULTY_SCALE
        )
        self.calls: dict[str, int] = {}
        self._lock = threading.Lock()

    def _count(self, method: str) -> None:
        with self._lock:
            self.calls[method] = self.calls.get(method, 0) + 1

    def call(self, method: str, params: list[object] | None = None) -> object:
        self._count(method)
        if method == "getbestblockhash":
            return self.winner_hash
        if method == "getblockcount":
            return self.height
        if method == "getblockhash":
            height = int((params or [0])[0])
            if height == self.height:
                return self.winner_hash
            raise RuntimeError(f"qbit RPC getblockhash failed: -8 unknown height {height}")
        if method == "getblockheader":
            block_hash = str((params or [""])[0]).lower()
            if block_hash == self.winner_hash:
                return {
                    "hash": self.winner_hash,
                    "height": self.height,
                    "confirmations": 1,
                    "bits": self.bits,
                    "difficulty": self.difficulty,
                }
            raise RuntimeError("qbit RPC getblockheader failed: -5 Block not found")
        if method == "submitblock":
            return "inconclusive"
        return super().call(method, params)


class CandidateStormRig:
    """Seed one same-parent storm and observe live admission plus restart."""

    def __init__(
        self,
        *,
        candidates: int = OBSERVED_TESTNET_CANDIDATE_STORM,
        queue_depth: int = MAX_PENDING_BLOCK_CANDIDATES,
        quiet: bool = True,
        credit_shares: bool = False,
        backlog_max: int | None = None,
    ) -> None:
        if candidates <= 0:
            raise ValueError("candidates must be positive")
        if queue_depth <= 0:
            raise ValueError("queue_depth must be positive")
        if backlog_max is not None and backlog_max <= 0:
            raise ValueError("backlog_max must be positive")
        self.candidates = candidates
        self.queue_depth = queue_depth
        self.quiet = quiet
        # Issue #198. ``credit_shares`` seeds every candidate as credit-bearing
        # and registers its pending share on the writer's floor, so the
        # collapse cleanup has real floor authority to release and the
        # decided winner has real authority to keep; the restart view adopts
        # holders through the shipped decode exactly as production does.
        # ``backlog_max`` pins the cleanup-retry admission bound on every
        # coordinator the rig owns (the shipped default otherwise).
        self.credit_shares = bool(credit_shares)
        self.backlog_max = backlog_max
        self.ledger = SingleWriterShareLedger()
        self.live_server: Any | None = None
        self.restarted_server: Any | None = None
        self.block_hashes: list[str] = []
        self.decided_rpc: DecidedHeightRpc | None = None

    def _coordinator(self) -> tuple[Any, Any]:
        server, state, _recording = submit_coordinator(tip=STORM_PARENT_HASH)
        server.max_blocks = self.candidates + 1
        server.stop_after_block = False
        server.block_candidate_queue = queue.Queue(maxsize=self.queue_depth)
        server.ledger = self.ledger
        # A shipped tunable, set to zero so a parked or retained retry neither
        # sleeps on the submitter lane nor parks behind a not-before deadline.
        # This is a component oracle, not a pacing test: every wakeup must be
        # re-offerable in the same round it was parked.
        server.block_candidate_retry_initial_seconds = 0.0
        if self.backlog_max is not None:
            server.block_candidate_cleanup_retry_backlog_max = int(self.backlog_max)
        if self.decided_rpc is not None:
            server.rpc = self.decided_rpc
        return server, state

    def _silence(self) -> ContextManager[Any]:
        if not self.quiet:
            return nullcontext()
        return redirect_stdout(StringIO())

    def _silence_all(self) -> ContextManager[Any]:
        """Silence both streams: a faulted cleanup prints tracebacks to stderr."""
        if not self.quiet:
            return nullcontext()
        stack = ExitStack()
        stack.enter_context(redirect_stdout(StringIO()))
        stack.enter_context(redirect_stderr(StringIO()))
        return stack

    def _candidate(self, server: Any, state: Any, index: int) -> tuple[Any, Any]:
        block_hash = f"{index:064x}"
        pending = PendingShare(
            share_id=f"miner-a:{block_hash}",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=index,
            ntime=1,
        )
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
            credit_share_on_accept=self.credit_shares,
        )
        return candidate, (pending, server.block_candidate_intent(candidate))

    def seed_live(self) -> CandidateStormSnapshot:
        """Persist the storm, then admit every live candidate wakeup."""

        if self.live_server is not None:
            raise RuntimeError("live storm has already been seeded")
        server, state = self._coordinator()
        candidates = []
        durable_records = []
        for index in range(1, self.candidates + 1):
            candidate, durable_record = self._candidate(server, state, index)
            candidates.append(candidate)
            durable_records.append(durable_record)
        self.block_hashes = [
            str(candidate.submission.block_hash_hex).lower()
            for candidate in candidates
        ]
        self.ledger.append_batch(durable_records)
        if self.credit_shares:
            # The live stamp path registers a credit-bearing share on the
            # floor before its candidate is admitted; the rig appends the
            # batch directly, so it registers the same holder here.
            writer = server._ensure_share_writer_service()
            for candidate in candidates:
                writer.adopt_pending_share(candidate.pending_share)
        with self._silence():
            for candidate in candidates:
                server.enqueue_block_candidate(candidate)
        self.live_server = server
        return self.snapshot(server)

    def restart_and_enumerate(self) -> CandidateStormSnapshot:
        """Create a fresh coordinator and enumerate every durable row."""

        if self.live_server is None:
            raise RuntimeError("seed_live must run before restart")
        if self.restarted_server is not None:
            raise RuntimeError("storm has already been restarted")
        server, _state = self._coordinator()
        # The restart view exists to hand a set-oriented selector the full
        # replay-adopted population, so the shipped #183 collapse is
        # suppressed for this enumeration exactly as it is for the per-row
        # drain. Without that, deciding the height before the restart would
        # let the enumeration dispose of the storm itself and leave the
        # instrument with nothing to characterize.
        server._ensure_block_candidate_service()._collapse_superseded_block_candidates = (
            lambda durable_rows, **_kwargs: durable_rows
        )
        server._note_block_replay_enumeration_owed()
        try:
            with self._silence():
                restored = server.replay_pending_block_candidates()
        finally:
            server._ensure_block_candidate_service().__dict__.pop(
                "_collapse_superseded_block_candidates",
                None,
            )
        if restored != self.candidates:
            raise AssertionError(
                f"restart restored {restored} of {self.candidates} candidates"
            )
        self.restarted_server = server
        return self.snapshot(server)

    def snapshot(self, server: Any) -> CandidateStormSnapshot:
        """Capture fixed-cardinality gauges without retaining payload copies."""

        service = server._ensure_block_candidate_service()
        service._ensure_block_replay_state()
        durable_rows = self.ledger.pending_block_candidate_rows(
            limit=self.candidates + 1
        )
        durable_hashes = {
            str(row["block_hash"]).lower()
            for row in durable_rows
        }
        outstanding = set(service._outstanding_block_candidate_hashes)
        replay_inflight = set(service._block_replay_inflight_hashes)
        ownership = self.ownership(server)
        return CandidateStormSnapshot(
            durable_pending=len(durable_hashes),
            live_queue_capacity=service.candidate_queue.maxsize,
            live_queued=service.candidate_queue.qsize(),
            live_coalesced=int(service.wakeups_coalesced),
            outstanding_marked=len(outstanding),
            outstanding_covers_all_durable=durable_hashes <= outstanding,
            replay_queued=service._block_replay_candidate_queue.qsize(),
            replay_inflight_marked=len(replay_inflight),
            replay_inflight_covers_all_durable=durable_hashes <= replay_inflight,
            accepted_parent_previews=len(server._accepted_block_payout_previews),
            replay_enumeration_owed=server._block_replay_enumeration_owed(),
            selector_evidence_marked=sum(
                1 for record in ownership if record.selector_evidence
            ),
            node_offer_evidence_marked=sum(
                1 for record in ownership if record.node_offer_evidence
            ),
            unoffered_retry_marked=sum(
                1 for record in ownership if record.retry_held_without_offer
            ),
            unoffered_lease_marked=sum(
                1 for record in ownership if record.lease_held_without_offer
            ),
            unoffered_terminal_marked=sum(
                1 for record in ownership if record.terminal_without_offer
            ),
        )

    # -- decided-height extension ------------------------------------------

    @property
    def storm_height(self) -> int:
        """The template height every storm candidate was built for."""
        server = self.live_server or self.restarted_server
        if server is None:
            raise RuntimeError("seed_live must run before the height is known")
        return int(server.jobs["job-1"].template["height"])

    def decide_height(self, *, winner_index: int | None = None) -> str:
        """Make one storm candidate the active block at the storm height.

        Installs :class:`DecidedHeightRpc` on every coordinator this rig
        owns (and on ones created later), so both the live and the restart
        views observe the same decided chain. The default winner is the
        LAST seeded candidate: it is a coalesced wakeup in the live view (it
        never entered the bounded queue) and the last row of the restart
        replay order, so a per-row drain reaches every sibling before it
        would reach the winner.
        """
        if not self.block_hashes:
            raise RuntimeError("seed_live must run before a height can be decided")
        if winner_index is None:
            winner_index = self.candidates
        if not 1 <= winner_index <= self.candidates:
            raise ValueError("winner_index must name a seeded candidate")
        winner_hash = self.block_hashes[winner_index - 1]
        self.decided_rpc = DecidedHeightRpc(
            winner_hash=winner_hash,
            height=self.storm_height,
        )
        for server in (self.live_server, self.restarted_server):
            if server is not None:
                server.rpc = self.decided_rpc
        return winner_hash

    def ownership(self, server: Any) -> list[CandidateOwnership]:
        """Read every seeded candidate's ownership/evidence facts at once."""
        service = server._ensure_block_candidate_service()
        service._ensure_block_replay_state()
        service._ensure_block_candidate_disposition_state()
        outbox = self.ledger._block_candidate_outbox
        live_queued = {
            str(item.submission.block_hash_hex).lower()
            for item in list(service.candidate_queue.queue)
        }
        replay_queued = {
            str(item.submission.block_hash_hex).lower()
            for item in list(service._block_replay_candidate_queue.queue)
        }
        with server.lock:
            outstanding = set(service._outstanding_block_candidate_hashes)
            replay_inflight = set(service._block_replay_inflight_hashes)
            tip_observed = set(service._tip_observed_accepted_block_hashes)
            retained = set(
                getattr(service, "_block_candidate_retained_node_submissions", {})
            )
            accounted = set(server._accounted_accepted_block_hashes)
            terminal = dict(service._block_candidate_terminal_outcomes)
            retry_held: set[str] = set()
            for holder in (
                service.retry_candidate,
                getattr(service, "_block_accounting_deferred_retry_candidate", None),
            ):
                if holder is not None:
                    retry_held.add(str(holder.submission.block_hash_hex).lower())
            retry_held.update(service._block_disposition_waiting_retries)
            retry_held.update(service.finalize_retries)
        with service._block_candidate_disposition_registry_lock:
            disposition_held = set(service._block_candidate_disposition_flights)
        with server._accepted_block_payout_preview_condition:
            previews = set(server._accepted_block_payout_previews)
        records = []
        for block_hash in self.block_hashes:
            row = outbox.get(block_hash)
            intent = row["candidate"] if row is not None else None
            pool_block = self.ledger.pool_block_state(block_hash=block_hash)
            records.append(
                CandidateOwnership(
                    block_hash=block_hash,
                    expected_height=(
                        int(intent["expected_height"])
                        if isinstance(intent, dict)
                        else self.storm_height
                    ),
                    parent_hash=(
                        str(intent["parent_hash"]).lower()
                        if isinstance(intent, dict)
                        else STORM_PARENT_HASH
                    ),
                    network_difficulty=(
                        float(intent["found_block"]["network_difficulty"])
                        if isinstance(intent, dict)
                        and isinstance(intent.get("found_block"), dict)
                        and intent["found_block"].get("network_difficulty") is not None
                        else None
                    ),
                    outbox_state=str(row["state"]) if row is not None else None,
                    attempt_count=int(row["attempt_count"]) if row is not None else 0,
                    live_queued=block_hash in live_queued,
                    replay_queued=block_hash in replay_queued,
                    outstanding=block_hash in outstanding,
                    replay_inflight=block_hash in replay_inflight,
                    preview_barrier=block_hash in previews,
                    disposition_held=block_hash in disposition_held,
                    retry_held=block_hash in retry_held,
                    node_acceptance_retained=block_hash in retained,
                    tip_observed=block_hash in tip_observed,
                    accounted_accepted=block_hash in accounted,
                    terminal_outcome=terminal.get(block_hash),
                    pool_block_chain_state=(
                        str(pool_block["chain_state"])
                        if pool_block is not None
                        else None
                    ),
                )
            )
        return records

    # -- perturbation -------------------------------------------------------

    def _take_queued_candidate(self, server: Any, block_hash: str) -> Any:
        """Remove one queued wakeup from whichever lane currently holds it."""
        service = server._ensure_block_candidate_service()
        service._ensure_block_replay_state()
        for queue_obj in (
            service.candidate_queue,
            service._block_replay_candidate_queue,
        ):
            for item in list(queue_obj.queue):
                if str(item.submission.block_hash_hex).lower() == block_hash:
                    # Single-threaded instrument: removing in place from the
                    # backing deque keeps both queue order and qsize() exact.
                    queue_obj.queue.remove(item)
                    return item
        raise KeyError(f"no queued wakeup for {block_hash}")

    def perturb(self, server: Any, kind: str, block_hash: str) -> Any:
        """Install one selector-evidence shape on a seeded candidate.

        Each kind reproduces one of the facts ``submit_next`` and
        ``record_abandoned`` consult, through the shipped entry point rather
        than a hand-written marker, and each shows up in :meth:`ownership` as
        ``selector_evidence``.  Three kinds show up there *without*
        ``node_offer_evidence``, because none reaches qbitd: ``retry_slot``
        moves an already-queued, never-offered wakeup into the retry holder,
        which is exactly what the shipped path does when the fast-lane
        reservation declines; ``lease_held`` claims the disposition lease
        by itself, which is all ``submit_next`` holds while it runs the
        terminal, capacity and fast-lane checks that precede any submitblock;
        and ``terminal`` records the disposition seal that the capacity-closed
        path installs with ``attempted=False`` and no submitblock at all.
        Returns a callable that undoes the perturbation where undoing is
        meaningful (the held disposition lease), otherwise ``None``.
        """
        service = server._ensure_block_candidate_service()
        service._ensure_block_replay_state()
        service._ensure_block_candidate_disposition_state()
        key = str(block_hash).lower()
        if kind == "lease_held":
            lease = server._claim_block_candidate_disposition(key, blocking=False)
            if lease is None:
                raise RuntimeError(f"disposition lease already held for {key}")
            return lambda: server._release_block_candidate_disposition(lease)
        if kind == "retained_success":
            service._stash_retained_block_candidate_node_submission(
                key,
                _BlockCandidateNodeSubmission(attempted=True, result=None),
            )
            return None
        if kind == "tip_observed":
            # Only an outstanding hash can register a tip observation, and
            # replay adoption alone never makes one outstanding -- the node
            # offer does (see test_replay_adoption_alone_drops_a_tip_observation).
            # Model "offered, then seen as the tip" through both shipped entry
            # points so the shape holds in the restart view too.
            server._register_outstanding_block_candidate(key)
            server._note_tip_observation_for_candidates(key)
            return None
        if kind == "retry_slot":
            candidate = self._take_queued_candidate(server, key)
            with server.lock:
                service.retry_candidate = candidate
            return None
        if kind == "prepared_pool_block":
            self.ledger.persist_accepted_block(
                block_hash=key,
                block_height=self.storm_height,
                parent_hash=STORM_PARENT_HASH,
                final_bundle={},
                audit_report={},
            )
            return None
        if kind == "terminal":
            service._record_block_candidate_terminal_outcome(key, accepted=False)
            return None
        raise ValueError(f"unknown perturbation: {kind}")

    # -- per-row drain ------------------------------------------------------

    # A head that neither terminalizes a row nor parks on a held lease is
    # being deferred by the shipped path (acceptance evidence).  Offer it
    # once more, then withhold it: re-offering forever is what the shipped
    # loop does behind its backoff, and this drain has no backoff.
    _DRAIN_STALL_LIMIT = 2

    def _dequeue_head(self, server: Any) -> tuple[str | None, str | None]:
        """Peek the hash and lane ``submit_next`` would take next, in order."""
        service = server._ensure_block_candidate_service()
        service._ensure_block_replay_state()
        service._ensure_block_candidate_disposition_state()
        with server.lock:
            retry = service.retry_candidate
            if retry is not None and service._block_candidate_retry_ready_locked(
                retry
            ):
                return str(retry.submission.block_hash_hex).lower(), "retry"
        for lane, queue_obj in (
            ("live", service.candidate_queue),
            ("replay", service._block_replay_candidate_queue),
        ):
            items = list(queue_obj.queue)
            if items:
                return str(items[0].submission.block_hash_hex).lower(), lane
        with server.lock:
            waiting = service._block_disposition_waiting_retries
            ready = [
                key
                for key in waiting
                if service._block_candidate_retry_ready_locked(waiting[key])
            ]
            if ready:
                return (
                    min(
                        ready,
                        key=lambda key: int(
                            waiting[key].context.template["height"]
                        ),
                    ),
                    "waiting",
                )
        return None, None

    def _withhold_head(
        self,
        server: Any,
        block_hash: str,
        lane: str,
    ) -> Any | None:
        """Lift one withheld hash out of the lane that would dequeue it next.

        Returns the exact wakeup object removed, so the drain can put it back
        before it returns.  Discarding it instead would strand the row for
        good: replay adoption keeps the hash in
        ``_block_replay_inflight_hashes`` until a terminal outcome is
        recorded, and ``_enqueue_replayed_block_candidate`` drops any
        re-adoption of an in-flight hash as a duplicate, so no later
        enumeration can rebuild the wakeup this call threw away.
        """
        service = server._ensure_block_candidate_service()
        if lane == "retry":
            with server.lock:
                candidate = service.retry_candidate
                service.retry_candidate = None
            return candidate
        if lane == "live":
            return service.candidate_queue.get_nowait()
        if lane == "replay":
            return service._block_replay_candidate_queue.get_nowait()
        if lane == "waiting":
            with server.lock:
                return service._block_disposition_waiting_retries.pop(
                    block_hash,
                    None,
                )
        return None

    def _restore_withheld_wakeup(
        self,
        server: Any,
        block_hash: str,
        lane: str,
        wakeup: Any,
    ) -> bool:
        """Put one lifted wakeup back where it was lifted from.

        Returns False when an equivalent wakeup already stands for the hash,
        which makes this object a dropped same-hash duplicate for the caller
        to release.

        Queue lanes are restored by pushing onto the front of the backing
        deque rather than through ``put_nowait``.  The wakeup was the head
        when it was lifted and everything ahead of it has since been
        consumed, so the front is its exact position; and because
        ``get_nowait`` never touched ``unfinished_tasks``, putting it back in
        place leaves that counter matching the item's original ``put``
        instead of double-counting it.  This is the same in-place idiom
        :meth:`_take_queued_candidate` uses, for the same reason.
        """
        service = server._ensure_block_candidate_service()
        service._ensure_block_replay_state()
        service._ensure_block_candidate_disposition_state()
        if lane in ("retry", "waiting"):
            with server.lock:
                if lane == "retry" and service.retry_candidate is None:
                    service.retry_candidate = wakeup
                    return True
                # Either this is a waiting-registry entry, or the single
                # retry head slot has been taken since. The shipped merge
                # parks a displaced hash in the waiting registry for exactly
                # this reason, so park it there rather than drop it.
                waiting = service._block_disposition_waiting_retries
                if block_hash in waiting:
                    return False
                waiting[block_hash] = wakeup
                return True
        queue_obj = (
            service._block_replay_candidate_queue
            if lane == "replay"
            else service.candidate_queue
        )
        queue_obj.queue.appendleft(wakeup)
        return True

    def _next_drainable(
        self,
        server: Any,
        withheld: set[str],
        held: list[tuple[str, str, Any]] | None = None,
    ) -> tuple[str | None, str | None]:
        """Return the next hash to drive, lifting withheld ones out on the way.

        Every wakeup lifted out is appended to ``held`` in lift order so the
        drain can restore it; see :meth:`_restore_withheld_wakeup`.
        """
        while True:
            head, lane = self._dequeue_head(server)
            if head is None or head not in withheld:
                return head, lane
            wakeup = self._withhold_head(server, head, str(lane))
            if wakeup is not None and held is not None:
                held.append((str(lane), head, wakeup))

    @staticmethod
    def _node_offers_made(rpc: Any) -> int:
        """Total ``submitblock`` calls the rig's RPC log has counted so far.

        :class:`DecidedHeightRpc` counts per method under a lock, and the
        shipped fast lane runs ``submitblock`` on a bounded worker thread
        whose completion ``submit_next`` waits for, so a delta taken across
        one ``submit_next`` names exactly the offers that call performed.

        The counter is required rather than optional: a drain that could not
        read it would report zero offers forever, which would silently make
        ``max_rounds`` unbounded instead of stopping the drain.  Every drain
        runs against a decided height, so the counter is always installed.
        """
        calls = getattr(rpc, "calls", None)
        if not isinstance(calls, dict):
            raise TypeError(
                "drain_per_row needs the per-method RPC counter installed by "
                "decide_height; rounds and max_rounds are measured from its "
                "submitblock count"
            )
        return int(calls.get("submitblock", 0))

    def _run_block_accounting_tasks(self, server: Any) -> int:
        """Run the shipped accounting lane to quiescence, on this thread.

        Mirrors ``BlockCandidateService.block_accounting_loop``: the
        definitive-acceptance lane first while its dispatch quota lasts, then
        the primary queue, then the unbounded spillover, then the
        invalid-intent quarantine, with the loop's own retain-on-failure
        behaviour.  The quota is read from the service rather than assumed so
        a scenario that pins it also pins what this drain does.
        """
        service = server._ensure_block_candidate_service()
        service._ensure_block_accounting_state()
        # block_accounting_loop stamps this on entry, and
        # _pace_block_candidate_retry reads it to record a not-before deadline
        # instead of sleeping while the lease and writer admission are held.
        service._block_accounting_thread_ident = threading.get_ident()
        ran = 0
        accepted_run = 0
        while True:
            source_queue = None
            if accepted_run < service._block_accounting_accepted_dispatch_quota():
                try:
                    _priority, _sequence, task = (
                        service._block_accounting_accepted_queue.get_nowait()
                    )
                    source_queue = service._block_accounting_accepted_queue
                    accepted_run += 1
                except queue.Empty:
                    pass
            if source_queue is None:
                try:
                    _priority, _sequence, task = (
                        service._block_accounting_queue.get_nowait()
                    )
                    source_queue = service._block_accounting_queue
                except queue.Empty:
                    try:
                        _priority, _sequence, task = (
                            service._block_accounting_overflow_queue.get_nowait()
                        )
                        source_queue = service._block_accounting_overflow_queue
                    except queue.Empty:
                        pass
                if source_queue is not None:
                    accepted_run = 0
            if source_queue is None:
                try:
                    _priority, _sequence, task = (
                        service._block_accounting_accepted_queue.get_nowait()
                    )
                    source_queue = service._block_accounting_accepted_queue
                    accepted_run = 0
                except queue.Empty:
                    while server._run_one_invalid_block_candidate_quarantine():
                        pass
                    return ran
            try:
                server._run_block_accounting_task(task)
            except Exception:
                server._retain_block_candidate_for_retry(task.candidate)
            finally:
                source_queue.task_done()
            ran += 1

    def drain_per_row(
        self,
        server: Any,
        *,
        stop_before_hash: str | None = None,
        preserve_hashes: Iterable[str] | None = None,
        max_rounds: int | None = None,
        dequeue_skip: bool = False,
    ) -> PerRowDrainReport:
        """Drive the shipped per-row path in the production submitter split.

        Each round runs ``submit_next_block_candidate(defer_accounting=True)``
        -- the call ``BlockCandidateService.run`` makes -- and then drains the
        real accounting queue through ``_run_block_accounting_task``, so the
        node offer, the fast-lane reservation, the replayed-candidate
        acceptance-evidence restore and the durable finalization all run on
        their shipped code paths.  When both lanes are empty the outbox replay
        query recovers coalesced wakeups, exactly as the loop does.

        ``defer_accounting=True`` is not a detail: with accounting inline
        ``submit_next`` claims each disposition lease *blocking*, so a single
        candidate whose lease is held elsewhere stops the drain forever, and
        ``_restore_replayed_candidate_acceptance_evidence`` -- which is what
        keeps a replayed candidate with a prepared pool-block row from being
        abandoned -- never runs at all.

        ``stop_before_hash`` ends the drain when that hash reaches the head
        (used for the decided winner, whose finalization tail is outside this
        instrument).  ``preserve_hashes`` instead withholds hashes for the
        whole drain: they are lifted out of the dequeue order without being
        offered or disposed, so their durable row and evidence markers stay
        exactly as the caller arranged them while every other row drains.
        Any head the shipped path parks on a held lease, or repeatedly
        declines to terminalize, is withheld the same way and reported, so
        the drain is bounded no matter what evidence the caller installed.

        Withholding is scoped to the invocation, so every wakeup this call
        lifted out is put back before it returns, on every exit -- exhausted,
        ``max_rounds``, ``stop_before_hash``, or an exception.  Restoring is
        not a courtesy: a withheld row keeps its ownership markers, and a
        hash marked replay-inflight is one no enumeration will re-adopt, so
        a discarded wakeup leaves the row pending with nothing left to
        dispose of it and no later drain able to reach it.

        ``max_rounds`` stops the drain after that many *node offers* -- the
        ``submitblock`` calls this invocation actually made, measured across
        each ``submit_next`` from the rig's own RPC log -- so a caller can
        step the oracle one offered row at a time.  It is deliberately not
        the count of consumed wakeups: ``submit_next`` also returns True for
        a row it parked on a lease held elsewhere, a same-hash duplicate it
        dropped, a fast-lane reservation it declined, and the capacity-closed
        ``attempted=False`` split that terminalizes a durable row without
        calling qbitd, and spending the budget on those would stop a bounded
        drain before it performed the offers it promised.  Boundedness for
        those rows comes from the withholding above, not from the budget.
        The report then covers only the rows *this* call moved: it is
        compared against the durable state captured on entry, so a second
        bounded call never re-reports the first one's abandonment.

        ``dequeue_skip`` is off by default, and off is what keeps this the
        per-row oracle.  Issue #181 item 2 added a second set-oriented
        disposition inside ``submit_next`` itself -- the dequeue-time stale
        sibling skip -- which would otherwise dispose of rows this drain is
        meant to measure the per-row path against, exactly as #183's
        replay-adoption collapse would.  Suppressed the same way and for the
        same reason.  Turning it on drives the shipped skip instead, so the
        same report shape (abandoned hashes, per-method RPC counts, attempt
        marks) can be compared against the oracle the default produces.
        """
        service = server._ensure_block_candidate_service()
        service._ensure_block_candidate_disposition_state()
        # Issue #183 landed a set-oriented collapse inside ``replay_pending``.
        # This drain exists to be the *per-row* oracle that selector is
        # measured against, and it re-enumerates the outbox whenever both
        # lanes empty, so the collapse is suppressed for its duration:
        # otherwise the enumeration would dispose of rows itself and the
        # comparison would be the selector against the selector.
        service._collapse_superseded_block_candidates = (
            lambda durable_rows, **_kwargs: durable_rows
        )
        if not dequeue_skip:
            # Issue #181 item 2's dequeue-time skip is a second shipped
            # disposition inside ``submit_next``.  The oracle is the per-row
            # path, so it is suppressed exactly as the #183 collapse is.
            service._skip_superseded_block_candidate_at_dequeue = (
                lambda _candidate, **_kwargs: False
            )
        rpc = server.rpc
        calls_before = dict(getattr(rpc, "calls", {}))
        outbox = self.ledger._block_candidate_outbox
        attempts_before = sum(int(row["attempt_count"]) for row in outbox.values())
        # The durable state of every row on entry.  A row is only ever
        # finished out of "pending" once, so comparing against this is what
        # makes the report name this call's transitions rather than every
        # terminal row the rig has accumulated across earlier drains.
        states_before = {
            str(block_hash).lower(): str(row["state"])
            for block_hash, row in outbox.items()
        }
        withheld = {str(item).lower() for item in (preserve_hashes or ())}
        caller_withheld = frozenset(withheld)
        stop_hash = None if stop_before_hash is None else str(stop_before_hash).lower()
        lease_blocked: set[str] = set()
        deferred: set[str] = set()
        stalls: dict[str, int] = {}
        # Node offers this call performed.  The rig's RPC log is the only
        # authoritative source: ``submit_next``'s boolean is True for
        # no-offer paths too (see PerRowDrainReport).
        rounds = 0
        accounting_tasks = 0
        enumerations = 0
        # Every wakeup withholding lifted out, in lift order, so the finally
        # below can put each one back exactly where it came from.
        held_wakeups: list[tuple[str, str, Any]] = []
        started = time.monotonic()
        try:
            with self._silence():
                while True:
                    if max_rounds is not None and rounds >= max_rounds:
                        break
                    head, _lane = self._next_drainable(server, withheld, held_wakeups)
                    if head is None:
                        enumerations += 1
                        if server.replay_pending_block_candidates() == 0:
                            break
                        if self._next_drainable(
                            server,
                            withheld,
                            held_wakeups,
                        )[0] is None:
                            # The enumeration restored only withheld rows; the
                            # shipped loop would re-adopt and re-park them for as
                            # long as their evidence stands.
                            break
                        continue
                    if stop_hash is not None and head == stop_hash:
                        break
                    with server.lock:
                        terminal_before = len(service._block_candidate_terminal_outcomes)
                    offers_before = self._node_offers_made(rpc)
                    server.submit_next_block_candidate(defer_accounting=True)
                    # submit_next waits for its own submitblock worker before
                    # returning, so this delta is complete and is either 0 or
                    # 1 for the row just driven.
                    rounds += self._node_offers_made(rpc) - offers_before
                    accounting_tasks += self._run_block_accounting_tasks(server)
                    with server.lock:
                        progressed = (
                            len(service._block_candidate_terminal_outcomes)
                            != terminal_before
                        )
                        parked = head in service._block_disposition_waiting_retries
                    if progressed:
                        stalls.pop(head, None)
                        continue
                    if parked:
                        # submit_next could not claim this hash's lease without
                        # blocking and parked the wakeup, exactly as the shipped
                        # loop does. Withhold it instead of spinning behind a
                        # lease this drain does not own.
                        withheld.add(head)
                        lease_blocked.add(head)
                        continue
                    stalls[head] = stalls.get(head, 0) + 1
                    if stalls[head] >= self._DRAIN_STALL_LIMIT:
                        withheld.add(head)
                        deferred.add(head)
        finally:
            try:
                with self._silence():
                    # Newest lift first: a mid-drain enumeration can re-adopt a
                    # row this call already lifted, and the re-adopted object is
                    # the one the shipped markers now describe. Restoring
                    # newest-first also rebuilds the original lane order,
                    # because each lifted wakeup goes back to its lane's front.
                    restored: set[str] = set()
                    for lane, key, wakeup in reversed(held_wakeups):
                        if key in restored or not self._restore_withheld_wakeup(
                            server,
                            key,
                            lane,
                            wakeup,
                        ):
                            # A wakeup for this hash already stands, so this
                            # object is a dropped same-hash duplicate and must
                            # release the credit floor it carries -- exactly
                            # what the shipped duplicate paths do.
                            service._release_dropped_duplicate_candidate_floor(
                                wakeup
                            )
                            continue
                        restored.add(key)
            finally:
                service.__dict__.pop(
                    "_collapse_superseded_block_candidates",
                    None,
                )
                service.__dict__.pop(
                    "_skip_superseded_block_candidate_at_dequeue",
                    None,
                )
        terminalized: dict[str, set[str]] = {"submitted": set(), "abandoned": set()}
        still_pending: set[str] = set()
        for block_hash, row in outbox.items():
            key = str(block_hash).lower()
            state = str(row["state"])
            if state == "pending":
                still_pending.add(key)
            elif states_before.get(key) != state:
                terminalized.setdefault(state, set()).add(key)
        calls_after = dict(getattr(rpc, "calls", {}))
        return PerRowDrainReport(
            rounds=rounds,
            accounting_tasks=accounting_tasks,
            replay_enumerations=enumerations,
            abandoned_hashes=frozenset(terminalized["abandoned"]),
            submitted_hashes=frozenset(terminalized["submitted"]),
            pending_hashes=frozenset(still_pending),
            withheld_hashes=(
                caller_withheld
                | frozenset(lease_blocked)
                | frozenset(deferred)
            ),
            lease_blocked_hashes=frozenset(lease_blocked),
            deferred_hashes=frozenset(deferred),
            rpc_calls={
                method: calls_after.get(method, 0) - calls_before.get(method, 0)
                for method in sorted(set(calls_before) | set(calls_after))
            },
            ledger_attempt_marks=(
                sum(int(row["attempt_count"]) for row in outbox.values())
                - attempts_before
            ),
            wall_seconds=time.monotonic() - started,
        )

    # -- cleanup-retry backlog (issue #198) ---------------------------------

    def _pending_hashes(self) -> set[str]:
        return {
            str(row["block_hash"]).lower()
            for row in self.ledger.pending_block_candidate_rows(
                limit=self.candidates + 1
            )
        }

    def _registry_bytes(self, server: Any) -> tuple[int, int, int]:
        """(registry bytes, holder bytes within them, retained holders).

        Read under the coordinator lock like every other registry reader.
        Holders are charged inside the registry total first, then measured
        again on their own so the per-holder figure can be reported apart
        from the per-record one.
        """
        service = server._ensure_block_candidate_service()
        with server.lock:
            registry = service._block_candidate_collapse_cleanup_retries
            total = _deep_size(registry, set())
            holders: dict[int, Any] = {}
            for record in registry.values():
                for share in record.shares:
                    holders[id(share)] = share
            holder_seen: set[int] = set()
            holder_bytes = sum(_deep_size(share, holder_seen) for share in holders.values())
        return total, holder_bytes, len(holders)

    def _collapse_walk(self, server: Any) -> tuple[set[str], float, int]:
        """Run the shipped enumeration walk; return (rows it won, seconds, adoptions)."""
        before = self._pending_hashes()
        started = time.monotonic()
        with self._silence_all():
            server._note_block_replay_enumeration_owed()
            adopted = int(server.replay_pending_block_candidates())
        elapsed = time.monotonic() - started
        return before - self._pending_hashes(), elapsed, adopted

    def _drain_backpressured_replay_page(
        self,
        server: Any,
        *,
        stop_before_hash: str,
    ) -> set[str]:
        """Drive the bounded spill page through the shipped dequeue collapse."""
        service = server._ensure_block_candidate_service()
        if not service._block_replay_backpressure_drain_pending:
            return set()
        terminalized: set[str] = set()
        replay_queue = service._block_replay_candidate_queue
        # The queue cannot grow in this synchronous rig. Bound the driver by
        # the captured page size so a failed disposition cannot spin.
        rounds = replay_queue.qsize()
        with self._silence_all():
            for _ in range(rounds):
                if replay_queue.empty():
                    break
                candidate = replay_queue.queue[0]
                block_hash = str(candidate.submission.block_hash_hex).lower()
                if block_hash == stop_before_hash:
                    break
                row = self.ledger._block_candidate_outbox[block_hash]
                before = str(row["state"])
                if not server.submit_next_block_candidate(defer_accounting=True):
                    break
                self._run_block_accounting_tasks(server)
                after = str(row["state"])
                if before == "pending" and after != "pending":
                    terminalized.add(block_hash)
        return terminalized

    def measure_cleanup_backlog(
        self,
        fault: str,
        *,
        view: str = "restart",
        sustained_passes: int = 64,
        trace_memory: bool = True,
    ) -> CleanupBacklogReport:
        """Drive the shipped collapse under one cleanup fault, then recover it.

        Phases, each on the shipped code path:

        1. Seed the storm (and restart, for the restart view), decide the
           height, and install ``fault`` at its seam.
        2. Run the enumeration walk.  Every row the fenced write wins whose
           cleanup the fault breaks becomes one retry record; rows the
           admission bound declines stay durable and are counted as
           preserved.  The registry is measured in place.
        3. With the fault still on, run up to ``sustained_passes`` retry
           passes: each must fail, re-register its record, and leave depth,
           holders and pins unchanged -- the backlog is never shed.
        4. Heal the fault and drain the backlog through the shipped retry
           pass until it reports nothing due, timing the drain.
        5. Drain the one replay page adopted at backpressure through the
           shipped dequeue collapse, run the walk again so admission can
           resume over whatever remains durable, then drive ``drain_per_row``
           over every queued wakeup to prove no terminal row reaches the
           node.

        ``view`` selects the coordinator: ``live`` holds the storm's first
        ``queue_depth`` wakeups in the bounded queue with the rest coalesced,
        so only that many holders can be retained; ``restart`` holds every
        row in the replay queue, so every record retains a holder -- the
        heavier and more instructive case.
        """
        if fault != CLEANUP_FAULT_NONE and fault not in CLEANUP_FAULTS:
            raise ValueError(f"unknown cleanup fault: {fault}")
        if view not in ("live", "restart"):
            raise ValueError("view must be 'live' or 'restart'")
        if self.live_server is None:
            self.seed_live()
        if view == "restart" and self.restarted_server is None:
            self.restart_and_enumerate()
        server = self.restarted_server if view == "restart" else self.live_server
        assert server is not None
        winner = (
            self.decide_height()
            if self.decided_rpc is None
            else self.decided_rpc.winner_hash
        )
        service = server._ensure_block_candidate_service()
        service._ensure_block_replay_state()
        service._ensure_block_candidate_disposition_state()
        writer = server._ensure_share_writer_service()
        floor_before = len(writer._pending_share_commit_floor)
        injector = CleanupFaultInjector(server, fault)
        injector.install()
        traced_walk = -1
        traced_owner = -1
        traced_peak = -1
        try:
            counts_before = service.block_candidate_collapse_snapshot()
            with server.lock:
                inflight_before = set(service._block_replay_inflight_hashes)
            if trace_memory:
                _warm_traceback_caches()
                gc.collect()
                tracemalloc.start()
                tracemalloc.reset_peak()
                traced_before = tracemalloc.get_traced_memory()[0]
                owner_before = _owner_traced_bytes(tracemalloc.take_snapshot())
            collapsed, walk_seconds, _adopted = self._collapse_walk(server)
            if trace_memory:
                gc.collect()
                traced_after, traced_peak_after = tracemalloc.get_traced_memory()
                owner_after = _owner_traced_bytes(tracemalloc.take_snapshot())
                tracemalloc.stop()
                traced_walk = traced_after - traced_before
                traced_owner = owner_after - owner_before
                traced_peak = traced_peak_after - traced_before
            counts_after = service.block_candidate_collapse_snapshot()
            snapshot = service.collapsed_candidate_cleanup_backlog_snapshot()
            registry_bytes, holder_bytes, holders = self._registry_bytes(server)
            records = int(snapshot["depth"])
            # -- sustained failure ------------------------------------------
            sustained = 0
            failed_before = counts_after["cleanup_retry_failed"]
            with self._silence_all():
                for _ in range(min(int(sustained_passes), records)):
                    if not service._run_one_collapsed_block_candidate_cleanup_retry():
                        break
                    sustained += 1
            sustained_failed = (
                service.block_candidate_collapse_snapshot()["cleanup_retry_failed"]
                - failed_before
            )
            after_sustained = service.collapsed_candidate_cleanup_backlog_snapshot()
            # -- recovery ---------------------------------------------------
            injector.heal()
            recovered_before = service.block_candidate_collapse_snapshot()[
                "cleanup_recovered"
            ]
            passes = 0
            started = time.monotonic()
            with self._silence_all():
                while service._run_one_collapsed_block_candidate_cleanup_retry():
                    passes += 1
            recovery_seconds = time.monotonic() - started
            recovered = (
                service.block_candidate_collapse_snapshot()["cleanup_recovered"]
                - recovered_before
            )
            after_recovery = service.collapsed_candidate_cleanup_backlog_snapshot()
            recovered_calls = injector.snapshot_calls()
            resumed_at_dequeue = self._drain_backpressured_replay_page(
                server,
                stop_before_hash=winner,
            )
            collapsed_after, second_walk_seconds, _adopted = self._collapse_walk(server)
            with server.lock:
                inflight_after = set(service._block_replay_inflight_hashes)
            every_collapsed = collapsed | resumed_at_dequeue | collapsed_after
            excess = injector.excess_calls(every_collapsed)
            missing = injector.missing_calls(every_collapsed)
            terminal_adopted = len((inflight_after - inflight_before) & every_collapsed)
            drain = self.drain_per_row(server, stop_before_hash=winner)
            # A hash whose record was discharged must never be cleaned up
            # again: not by the second walk, not by the queued duplicates
            # the drain drops.
            post_recovery_calls = injector.calls_since(recovered_calls, collapsed)
        finally:
            if trace_memory and tracemalloc.is_tracing():
                tracemalloc.stop()
            injector.uninstall()
        # The winner's holder is the one the shipped path still owns: in the
        # restart view the object adopted at restart, in the live view the
        # object the first walk's recovery enumeration adopted (the seeded
        # live wakeup coalesced and was never queued). Looked up after the
        # walks for exactly that reason.
        winner_share = next(
            (
                item.pending_share
                for queue_obj in (
                    service.candidate_queue,
                    service._block_replay_candidate_queue,
                )
                for item in list(queue_obj.queue)
                if str(item.submission.block_hash_hex).lower() == winner
            ),
            None,
        )
        floor_after = len(writer._pending_share_commit_floor)
        return CleanupBacklogReport(
            fault=fault,
            view=view,
            candidates=self.candidates,
            credit_shares=self.credit_shares,
            backlog_max=int(snapshot["backlog_max"]),
            walk_seconds=walk_seconds,
            collapsed_rows=len(collapsed),
            deferred_records=records,
            preserved_rows=(
                counts_after["backlog_deferred"] - counts_before["backlog_deferred"]
            ),
            backpressure_engagements=int(snapshot["backpressure_engagements"]),
            backpressure_active=bool(snapshot["backpressure_active"]),
            pending_share_holders=int(snapshot["pending_share_holders"]),
            terminal_outcome_pins=int(snapshot["terminal_outcome_pins"]),
            oldest_age_seconds=float(snapshot["oldest_age_seconds"]),
            registry_bytes=registry_bytes,
            holder_bytes=holder_bytes,
            registry_bytes_per_record=(registry_bytes / records) if records else 0.0,
            holder_bytes_per_holder=(holder_bytes / holders) if holders else 0.0,
            traced_walk_bytes=traced_walk,
            traced_owner_bytes=traced_owner,
            traced_peak_bytes=traced_peak,
            sustained_passes=sustained,
            sustained_failed_passes=int(sustained_failed),
            depth_after_sustained=int(after_sustained["depth"]),
            holders_after_sustained=int(after_sustained["pending_share_holders"]),
            pins_after_sustained=int(after_sustained["terminal_outcome_pins"]),
            recovery_seconds=recovery_seconds,
            recovery_passes=passes,
            recovered_records=int(recovered),
            retry_records_per_second=(
                (recovered / recovery_seconds) if recovery_seconds > 0 else 0.0
            ),
            depth_after_recovery=int(after_recovery["depth"]),
            second_walk_seconds=second_walk_seconds,
            collapsed_rows_after_recovery=len(
                resumed_at_dequeue | collapsed_after
            ),
            excess_cleanup_calls=excess,
            missing_cleanup_calls=missing,
            post_recovery_cleanup_calls=post_recovery_calls,
            floor_double_releases=injector.floor_double_releases,
            terminal_rows_adopted=terminal_adopted,
            node_offers=int(self.decided_rpc.calls.get("submitblock", 0)),
            drain_rounds=drain.rounds,
            pending_rows_after_drain=drain.pending_rows,
            floor_holders_before=floor_before,
            floor_holders_after=floor_after,
            winner_floor_holder_retained=(
                winner_share is not None
                and id(winner_share) in writer._pending_share_commit_floor
            ),
        )


# -- heap and allocator readings (issue #226) ------------------------------
#
# #185 measured the coordinator at 125 MiB before a storm and 613 MiB after
# the drain, and #226 found the mainnet resident set growing 390 MB/h with a
# memory map shaped like glibc per-thread arenas. These readings put the same
# instrument around the storm so both can be re-run on any host: the resident
# set, the interpreter's live-block count, glibc's arena/in-use/free bytes and
# arena count, and the anonymous-map band histogram, at every phase boundary,
# then after the storm's objects are released and collected, then after one
# malloc_trim. Every number is host-specific -- absolutes vary by up to an
# order of magnitude across hosts -- and the instrument reports the platform
# beside them; what transfers is the mechanism: does the live-block count
# return, does the free-bytes reading grow, does the trim return it.

ARENA_PROBE_MIN_BYTES = 4 * 1024
# Under glibc's initial 128 KiB mmap threshold, so the buffers come from the
# arenas rather than from direct mmap; over pymalloc's 512-byte ceiling, so
# they come from glibc at all.
ARENA_PROBE_MAX_BYTES = 120 * 1024
ARENA_PROBE_LIVE_BUFFERS = 64
# A worker's wait at the barrier, and half the join ceiling. 64 threads x
# 2,000 rounds take about a second on a development host; a slow host has
# an order of magnitude of headroom before the probe reports a failure.
ARENA_PROBE_BARRIER_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True)
class HeapReading:
    """One phase boundary's resident-set and allocator readings."""

    phase: str
    elapsed_seconds: float
    resident_bytes: int
    allocated_blocks: int
    threads: int
    malloc_info_available: bool
    malloc_arena_bytes: int
    malloc_in_use_bytes: int
    malloc_free_bytes: int
    malloc_mmapped_bytes: int
    malloc_arena_count: int
    # Free bytes split by where they sit: interior bins, which malloc_trim
    # returns in every arena, and per-thread heap tops, which it cannot.
    malloc_free_top_bytes: int
    malloc_free_bin_bytes: int
    anonymous_regions: int
    anonymous_bytes: int
    anonymous_regions_4mib_to_64mib: int
    heap_segment_bytes: int


def take_heap_reading(
    phase: str,
    telemetry: ProcessHeapTelemetry,
    started_monotonic: float,
) -> HeapReading:
    sample = telemetry.sample()
    shape = read_anonymous_map_shape()
    malloc_info = read_malloc_info_summary()
    return HeapReading(
        phase=phase,
        elapsed_seconds=round(time.monotonic() - started_monotonic, 3),
        resident_bytes=read_resident_memory_bytes(),
        allocated_blocks=sample.allocated_blocks,
        threads=sample.threads,
        malloc_info_available=sample.malloc_info_available,
        malloc_arena_bytes=sample.malloc_arena_bytes,
        malloc_in_use_bytes=sample.malloc_in_use_bytes,
        malloc_free_bytes=sample.malloc_free_bytes,
        malloc_mmapped_bytes=sample.malloc_mmapped_bytes,
        malloc_arena_count=malloc_info.arenas,
        malloc_free_top_bytes=malloc_info.top_bytes,
        malloc_free_bin_bytes=malloc_info.bin_bytes,
        anonymous_regions=shape.regions,
        anonymous_bytes=shape.total_bytes,
        anonymous_regions_4mib_to_64mib=shape.band_regions.get("4mib_to_64mib", -1),
        heap_segment_bytes=shape.heap_segment_bytes,
    )


def allocator_environment() -> dict[str, str]:
    """The glibc and CPython allocator variables this process started with."""
    return {
        name: value
        for name, value in sorted(os.environ.items())
        if name.startswith("MALLOC_") or name in ("GLIBC_TUNABLES", "PYTHONMALLOC")
    }


def run_arena_probe(
    threads: int,
    rounds: int,
    *,
    seed: int = 226,
    telemetry: ProcessHeapTelemetry | None = None,
    allocate: Any = bytes,
) -> dict[str, Any]:
    """Reproduce the per-thread arena shape and report what it costs.

    ``threads`` Python threads each allocate ``rounds`` medium buffers
    (4-120 KiB: over pymalloc's ceiling, under glibc's initial mmap
    threshold) into a small ring so a bounded number stay live at once, then
    drop the ring and exit. glibc assigns a thread its own arena on its first
    allocation (up to ``MALLOC_ARENA_MAX``) only while no exited thread's
    arena is waiting on the free list, so the threads meet at a barrier
    before any exits -- the coordinator's 64 threads are long-lived, and
    that is the shape under test. The arena count after the probe is the
    mechanism: it should approach ``threads + 1`` at glibc's default and stop
    at the configured cap otherwise. The rings' freed buffers stay on each
    arena's free lists after the threads exit -- arenas outlive threads --
    which is the retained-free-bytes reading the trim that follows is judged
    by. ``allocate`` is the buffer constructor (``bytes``); a test injects a
    failing one to prove a broken worker cannot wedge the probe.
    """
    telemetry = telemetry if telemetry is not None else ProcessHeapTelemetry()
    before = telemetry.sample()
    resident_before = read_resident_memory_bytes()
    arenas_before = read_malloc_arena_count()
    started = time.monotonic()
    # Bounded: a worker that fails breaks the barrier for everyone, a worker
    # that stalls trips the barrier timeout, and the join below has its own
    # ceiling, so one bad thread cannot wedge the instrument. Every such
    # event is reported in ``failures`` rather than raised mid-probe.
    all_allocated = threading.Barrier(threads)
    failures: list[str] = []

    def worker(index: int) -> None:
        rng = random.Random(seed + index)
        ring: list[bytes | None] = [None] * ARENA_PROBE_LIVE_BUFFERS
        try:
            for round_index in range(rounds):
                size = rng.randint(ARENA_PROBE_MIN_BYTES, ARENA_PROBE_MAX_BYTES)
                ring[round_index % ARENA_PROBE_LIVE_BUFFERS] = allocate(size)
            all_allocated.wait(timeout=ARENA_PROBE_BARRIER_TIMEOUT_SECONDS)
        except threading.BrokenBarrierError:
            failures.append(f"worker {index}: barrier broken or timed out")
        except Exception as exc:
            all_allocated.abort()
            failures.append(f"worker {index}: {exc!r}")
        finally:
            ring.clear()

    workers = [
        threading.Thread(target=worker, args=(index,), name=f"arena-probe-{index}")
        for index in range(threads)
    ]
    for thread in workers:
        thread.start()
    join_deadline = time.monotonic() + ARENA_PROBE_BARRIER_TIMEOUT_SECONDS * 2
    for thread in workers:
        thread.join(timeout=max(0.0, join_deadline - time.monotonic()))
        if thread.is_alive():
            all_allocated.abort()
            failures.append(f"{thread.name}: did not finish before the join ceiling")
    seconds = time.monotonic() - started
    after = telemetry.sample()
    return {
        "threads": threads,
        "rounds": rounds,
        "buffer_bytes": [ARENA_PROBE_MIN_BYTES, ARENA_PROBE_MAX_BYTES],
        "live_buffers_per_thread": ARENA_PROBE_LIVE_BUFFERS,
        "failures": failures,
        "seconds": round(seconds, 6),
        "arena_count_before": arenas_before,
        "arena_count_after": read_malloc_arena_count(),
        "resident_before": resident_before,
        "resident_after": read_resident_memory_bytes(),
        "malloc_info_available": before.malloc_info_available and after.malloc_info_available,
        "malloc_arena_before": before.malloc_arena_bytes,
        "malloc_arena_after": after.malloc_arena_bytes,
        "malloc_free_before": before.malloc_free_bytes,
        "malloc_free_after": after.malloc_free_bytes,
        "malloc_in_use_before": before.malloc_in_use_bytes,
        "malloc_in_use_after": after.malloc_in_use_bytes,
    }


def allocator_experiment_child_args(argv: Sequence[str]) -> list[str]:
    """The child invocation: this run's arguments minus the experiment options.

    ``--verbose`` is dropped too: a child's coordinator logs would precede
    the JSON document on the same stdout the parent parses, and the parse
    would fail only after the whole storm had run.
    """
    child: list[str] = []
    skip_next = False
    for token in argv:
        if skip_next:
            skip_next = False
            continue
        if token in ("--allocator-experiment", "--allocator-mmap-threshold"):
            skip_next = True
            continue
        if token.startswith("--allocator-experiment=") or token.startswith(
            "--allocator-mmap-threshold="
        ):
            continue
        if token == "--verbose":
            continue
        child.append(token)
    if "--heap-report" not in child:
        child.append("--heap-report")
    return child


def run_allocator_experiment(
    settings: Sequence[str],
    child_args: Sequence[str],
    *,
    mmap_threshold: int | None = None,
) -> list[dict[str, Any]]:
    """Run this instrument once per ``MALLOC_ARENA_MAX`` setting, in a child each.

    glibc reads its environment once at process start, so every setting
    needs its own process; ``default`` runs with the variable unset. The
    children inherit everything else, and ``mmap_threshold`` (when given)
    pins ``MALLOC_MMAP_THRESHOLD_`` in each of them, which also switches
    glibc's dynamic threshold off.
    """
    results: list[dict[str, Any]] = []
    for setting in settings:
        env = dict(os.environ)
        env.pop("MALLOC_ARENA_MAX", None)
        if setting != "default":
            env["MALLOC_ARENA_MAX"] = str(int(setting))
        if mmap_threshold is not None:
            env["MALLOC_MMAP_THRESHOLD_"] = str(int(mmap_threshold))
        completed = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), *child_args],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        child = json.loads(completed.stdout)
        decided = child.get("decided") or {}
        results.append(
            {
                "malloc_arena_max": setting,
                "malloc_mmap_threshold": mmap_threshold,
                "heap": child["heap"],
                "per_row_drain": decided.get("per_row_drain"),
            }
        )
    return results


def _run_storm_phases(
    args: argparse.Namespace,
    read_heap: Any,
    phase_seconds: dict[str, float],
) -> dict[str, Any]:
    """Seed, restart, optionally decide/drain, optionally fault; return the report.

    Everything the storm allocates is referenced only from this frame, so a
    caller that wants the post-storm resident set reads it after this
    returns. ``read_heap`` is called at every phase boundary and
    ``phase_seconds`` receives each phase's wall-clock.
    """
    rig = CandidateStormRig(
        candidates=args.candidates,
        queue_depth=args.queue_depth,
        quiet=not args.verbose,
    )
    phase_started = time.monotonic()
    live = rig.seed_live()
    phase_seconds["seed_live"] = round(time.monotonic() - phase_started, 6)
    read_heap("after_seed_live")
    phase_started = time.monotonic()
    restarted = rig.restart_and_enumerate()
    phase_seconds["restart_enumerate"] = round(time.monotonic() - phase_started, 6)
    read_heap("after_restart_enumerate")
    report: dict[str, Any] = {
        "bounds": {
            "live_queue": MAX_PENDING_BLOCK_CANDIDATES,
            "replay_page": MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
        },
        "input": {
            "candidates": args.candidates,
            "queue_depth": args.queue_depth,
        },
        "live": asdict(live),
        "restart": asdict(restarted),
    }
    if args.decide or args.drain_per_row:
        winner = rig.decide_height()
        decided = rig.snapshot(rig.restarted_server)
        report["decided"] = {
            "winner_hash": winner,
            "restart": asdict(decided),
        }
        read_heap("after_decide")
        if args.drain_per_row:
            phase_started = time.monotonic()
            drain = rig.drain_per_row(
                rig.restarted_server,
                stop_before_hash=winner,
            )
            phase_seconds["drain_per_row"] = round(time.monotonic() - phase_started, 6)
            read_heap("after_drain")
            # The abandoned/submitted hash sets are the programmatic output
            # and are storm-sized; only the bounded sets are printed.
            report["decided"]["per_row_drain"] = {
                "rounds": drain.rounds,
                "accounting_tasks": drain.accounting_tasks,
                "replay_enumerations": drain.replay_enumerations,
                "abandoned_rows": drain.abandoned_rows,
                "submitted_rows": drain.submitted_rows,
                "pending_rows": drain.pending_rows,
                "pending_hashes": sorted(drain.pending_hashes),
                "withheld_hashes": sorted(drain.withheld_hashes),
                "lease_blocked_hashes": sorted(drain.lease_blocked_hashes),
                "deferred_hashes": sorted(drain.deferred_hashes),
                "rpc_calls": drain.rpc_calls,
                "ledger_attempt_marks": drain.ledger_attempt_marks,
                "wall_seconds": drain.wall_seconds,
            }
    if args.cleanup_fault:
        faults: list[str] = []
        for fault in args.cleanup_fault:
            if fault == "all":
                faults.extend([CLEANUP_FAULT_NONE, *CLEANUP_FAULTS])
            else:
                faults.append(fault)
        runs: list[dict[str, Any]] = []
        clean: CleanupBacklogReport | None = None
        for fault in dict.fromkeys(faults):
            # A fresh storm per run: the backlog and its memory are only
            # comparable across runs that started from the same state.
            fault_rig = CandidateStormRig(
                candidates=args.candidates,
                queue_depth=args.queue_depth,
                quiet=not args.verbose,
                credit_shares=args.credit_shares,
                backlog_max=args.backlog_max,
            )
            measured = fault_rig.measure_cleanup_backlog(
                fault,
                view=args.cleanup_view,
                trace_memory=not args.no_tracemalloc,
            )
            if fault == CLEANUP_FAULT_NONE:
                clean = measured
            entry = asdict(measured)
            entry["recovered_cleanly"] = measured.recovered_cleanly
            entry["traced_marginal_bytes_per_record"] = (
                cleanup_backlog_marginal_bytes_per_record(measured, clean)
                if clean is not None and fault != CLEANUP_FAULT_NONE
                else None
            )
            entry["traced_owner_marginal_bytes_per_record"] = (
                cleanup_backlog_marginal_bytes_per_record(
                    measured,
                    clean,
                    owner_only=True,
                )
                if clean is not None and fault != CLEANUP_FAULT_NONE
                else None
            )
            runs.append(entry)
        report["cleanup_backlog"] = {
            "python": sys.version.split()[0],
            "platform": sys.platform,
            "runs": runs,
        }
        read_heap("after_cleanup_faults")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidates",
        type=int,
        default=OBSERVED_TESTNET_CANDIDATE_STORM,
    )
    parser.add_argument(
        "--queue-depth",
        type=int,
        default=MAX_PENDING_BLOCK_CANDIDATES,
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="show coordinator coalescing and replay logs before the JSON report",
    )
    parser.add_argument(
        "--decide",
        action="store_true",
        help="after restart enumeration, make the last candidate the active "
        "block at the storm height and report ownership/evidence gauges",
    )
    parser.add_argument(
        "--drain-per-row",
        action="store_true",
        help="with --decide, also drive the shipped per-row disposition path "
        "over every sibling on the restarted coordinator and report its cost",
    )
    parser.add_argument(
        "--cleanup-fault",
        action="append",
        choices=(CLEANUP_FAULT_NONE, "all", *CLEANUP_FAULTS),
        help="issue #198: run the shipped collapse walk under this injected "
        "cleanup fault on a fresh storm, measure the retry backlog, and prove "
        "its recovery; repeatable, 'all' runs the clean baseline and every fault",
    )
    parser.add_argument(
        "--cleanup-view",
        choices=("live", "restart"),
        default="restart",
        help="coordinator view the cleanup-fault runs use (default: restart)",
    )
    parser.add_argument(
        "--backlog-max",
        type=int,
        default=None,
        help="pin the cleanup-retry admission bound for the cleanup-fault runs "
        f"(default: the shipped {DEFAULT_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX})",
    )
    parser.add_argument(
        "--credit-shares",
        action="store_true",
        help="seed credit-bearing candidates so the cleanup-fault runs exercise "
        "real pending-share floor authority",
    )
    parser.add_argument(
        "--no-tracemalloc",
        action="store_true",
        help="skip the tracemalloc reading in the cleanup-fault runs",
    )
    parser.add_argument(
        "--heap-report",
        action="store_true",
        help="issue #226: report resident set, glibc arena, and anonymous-map "
        "readings at every phase, after the storm is released and collected, "
        "and after one malloc_trim (host-specific numbers; the platform is "
        "reported beside them)",
    )
    parser.add_argument(
        "--arena-probe-threads",
        type=int,
        default=0,
        help="with --heap-report, run this many allocating threads after the "
        "storm to reproduce the per-thread arena shape (0 skips the probe)",
    )
    parser.add_argument(
        "--arena-probe-rounds",
        type=int,
        default=2_000,
        help="medium-buffer allocations per arena-probe thread (default 2000)",
    )
    parser.add_argument(
        "--allocator-experiment",
        default=None,
        help="comma-separated MALLOC_ARENA_MAX settings ('default', '2', '4'); "
        "runs this instrument once per setting in a child process with "
        "--heap-report and reports every child's heap section side by side",
    )
    parser.add_argument(
        "--allocator-mmap-threshold",
        type=int,
        default=None,
        help="with --allocator-experiment, also pin MALLOC_MMAP_THRESHOLD_ in "
        "every child (which switches glibc's dynamic threshold off)",
    )
    args = parser.parse_args()
    if args.allocator_experiment:
        settings = [
            token.strip()
            for token in args.allocator_experiment.split(",")
            if token.strip()
        ]
        runs = run_allocator_experiment(
            settings,
            allocator_experiment_child_args(sys.argv[1:]),
            mmap_threshold=args.allocator_mmap_threshold,
        )
        print(
            json.dumps(
                {
                    "allocator_experiment": {
                        "python": sys.version.split()[0],
                        "platform": sys.platform,
                        "libc": " ".join(platform.libc_ver()),
                        "machine": platform.machine(),
                        "cpu_count": os.cpu_count(),
                        "runs": runs,
                    }
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    telemetry = ProcessHeapTelemetry() if args.heap_report else None
    readings: list[HeapReading] = []
    phase_seconds: dict[str, float] = {}
    instrument_started = time.monotonic()

    def read_heap(phase: str) -> None:
        if telemetry is not None:
            readings.append(take_heap_reading(phase, telemetry, instrument_started))

    read_heap("start")
    # The storm runs in its own frame so that, when it returns, nothing in
    # this function still references the rig, the drain, or a fault run.
    report = _run_storm_phases(args, read_heap, phase_seconds)
    if telemetry is not None:
        probe: dict[str, Any] | None = None
        if args.arena_probe_threads > 0:
            probe = run_arena_probe(
                args.arena_probe_threads,
                args.arena_probe_rounds,
                telemetry=telemetry,
            )
            read_heap("after_arena_probe")
        # #185's question: once the storm's own objects are gone, does the
        # process return to its pre-storm size? Every reference to the storm
        # died with _run_storm_phases' frame; collect, and read. The gap
        # between "after_release_gc" and "start" that survives is what the
        # interpreter still holds (allocated_blocks) plus what glibc has not
        # returned (malloc_free_bytes and the resident set); the trim that
        # follows shows how much of the latter one malloc_trim recovers.
        gc.collect()
        read_heap("after_release_gc")
        trim = MallocTrimmer(telemetry, log=lambda _line: None).trim_once("storm")
        read_heap("after_malloc_trim")
        report["heap"] = {
            "python": sys.version.split()[0],
            "platform": sys.platform,
            "libc": " ".join(platform.libc_ver()),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
            "allocator_env": allocator_environment(),
            "phase_seconds": phase_seconds,
            "readings": [asdict(reading) for reading in readings],
            "arena_probe": probe,
            "malloc_trim": trim.as_dict() if trim is not None else None,
        }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
