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
offered nothing.  The rig therefore reports the attested offers
(``node_offer_evidence``) apart from the conservative must-not-abandon union
(``selector_evidence``) that a selector owes the #183 contract, so neither a
retry holder nor a held lease is ever mistaken for a node call.

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
"""

from __future__ import annotations

import argparse
import json
import queue
import sys
import threading
import time
from contextlib import nullcontext, redirect_stdout
from dataclasses import asdict, dataclass
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ContextManager, Iterable

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.block_candidates import (  # noqa: E402
    MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
    MAX_PENDING_BLOCK_CANDIDATES,
    _BlockCandidateNodeSubmission,
)
from lab.prism.share_ledger import (  # noqa: E402
    PendingShare,
    SingleWriterShareLedger,
)
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
    # for, and the two widenings that attest nothing -- a wakeup parked in a
    # retry holder, and a claimed disposition lease.  Neither is by itself
    # evidence qbitd was ever called.  The two can coincide on one row, so
    # they are counted separately rather than folded into one residue.
    node_offer_evidence_marked: int = 0
    unoffered_retry_marked: int = 0
    unoffered_lease_marked: int = 0


@dataclass(frozen=True)
class CandidateOwnership:
    """Ownership and evidence facts for one durable candidate at one instant.

    Every field is read from the shipped coordinator/ledger state, so a
    maintenance selector can be evaluated against the real population
    instead of a hand-built corpus. ``outstanding`` and ``replay_inflight``
    are process-ownership markers; the fields folded into
    ``node_offer_evidence`` are the ones that mean a node offer happened,
    is happening, or left acceptance evidence behind. ``retry_held`` and
    ``disposition_held`` are neither: they are a third category, covered by
    the conservative ``selector_evidence`` union without attesting an offer.
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
        offer, and a terminal outcome or a pool-block row is what an offer's
        disposition left behind.  ``retry_held`` and ``disposition_held`` are
        deliberately absent; see :attr:`retry_held_without_offer` and
        :attr:`lease_held_without_offer`.
        """
        return bool(
            self.node_acceptance_retained
            or self.tip_observed
            or self.accounted_accepted
            or self.terminal_outcome is not None
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
    def selector_evidence(self) -> bool:
        """The conservative must-not-abandon union the #183 contract needs.

        This is :attr:`node_offer_evidence` widened by ``retry_held`` and by
        ``disposition_held``.  Each widening covers an ambiguity rather than
        an attestation: a parked wakeup may equally have come from a path
        that *did* offer and is retrying its tail, and a claimed lease may
        equally span an offer already in flight.  Neither the holder nor the
        flight registry can tell the two apart, so a selector has to leave
        the row to the per-row path either way -- and a row whose lease it
        cannot claim is not one it could dispose of anyway.  Because the
        widening is deliberately conservative, a true value here is not a
        claim that qbitd was offered the hash -- read
        :attr:`node_offer_evidence` for that, and
        :attr:`retry_held_without_offer` / :attr:`lease_held_without_offer`
        for the rows the widenings add.
        """
        return (
            self.node_offer_evidence
            or bool(self.retry_held)
            or bool(self.disposition_held)
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
        difficulty: float = 1.0,
    ) -> None:
        self.winner_hash = winner_hash.lower()
        self.height = int(height)
        # The decided block's difficulty as qbitd would report it in
        # getblockheader; the storm template is built at the same value, so
        # by default the decided block and every sibling carry equal work.
        self.difficulty = float(difficulty)
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
    ) -> None:
        if candidates <= 0:
            raise ValueError("candidates must be positive")
        if queue_depth <= 0:
            raise ValueError("queue_depth must be positive")
        self.candidates = candidates
        self.queue_depth = queue_depth
        self.quiet = quiet
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
        if self.decided_rpc is not None:
            server.rpc = self.decided_rpc
        return server, state

    def _silence(self) -> ContextManager[Any]:
        if not self.quiet:
            return nullcontext()
        return redirect_stdout(StringIO())

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
        ``selector_evidence``.  Two kinds show up there *without*
        ``node_offer_evidence``, because neither reaches qbitd: ``retry_slot``
        moves an already-queued, never-offered wakeup into the retry holder,
        which is exactly what the shipped path does when the fast-lane
        reservation declines, and ``lease_held`` claims the disposition lease
        by itself, which is all ``submit_next`` holds while it runs the
        terminal, capacity and fast-lane checks that precede any submitblock.
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

    def _withhold_head(self, server: Any, block_hash: str, lane: str) -> None:
        """Lift one withheld hash out of the lane that would dequeue it next."""
        service = server._ensure_block_candidate_service()
        if lane == "retry":
            with server.lock:
                service.retry_candidate = None
        elif lane == "live":
            service.candidate_queue.get_nowait()
        elif lane == "replay":
            service._block_replay_candidate_queue.get_nowait()
        elif lane == "waiting":
            with server.lock:
                service._block_disposition_waiting_retries.pop(block_hash, None)

    def _next_drainable(
        self,
        server: Any,
        withheld: set[str],
    ) -> tuple[str | None, str | None]:
        """Return the next hash to drive, lifting withheld ones out on the way."""
        while True:
            head, lane = self._dequeue_head(server)
            if head is None or head not in withheld:
                return head, lane
            self._withhold_head(server, head, str(lane))

    def _run_block_accounting_tasks(self, server: Any) -> int:
        """Run the shipped accounting lane to quiescence, on this thread.

        Mirrors ``BlockCandidateService.block_accounting_loop``: primary
        queue first, then the unbounded spillover, then the invalid-intent
        quarantine, with the loop's own retain-on-failure behaviour.
        """
        service = server._ensure_block_candidate_service()
        service._ensure_block_accounting_state()
        # block_accounting_loop stamps this on entry, and
        # _pace_block_candidate_retry reads it to record a not-before deadline
        # instead of sleeping while the lease and writer admission are held.
        service._block_accounting_thread_ident = threading.get_ident()
        ran = 0
        while True:
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

        ``max_rounds`` stops the drain after that many rows have been offered,
        so a caller can step the oracle one row at a time.  The report then
        covers only the rows *this* call moved: it is compared against the
        durable state captured on entry, so a second bounded call never
        re-reports the first one's abandonment.
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
        rounds = 0
        accounting_tasks = 0
        enumerations = 0
        started = time.monotonic()
        try:
            with self._silence():
                while True:
                    if max_rounds is not None and rounds >= max_rounds:
                        break
                    head, _lane = self._next_drainable(server, withheld)
                    if head is None:
                        enumerations += 1
                        if server.replay_pending_block_candidates() == 0:
                            break
                        if self._next_drainable(server, withheld)[0] is None:
                            # The enumeration restored only withheld rows; the
                            # shipped loop would re-adopt and re-park them for as
                            # long as their evidence stands.
                            break
                        continue
                    if stop_hash is not None and head == stop_hash:
                        break
                    with server.lock:
                        terminal_before = len(service._block_candidate_terminal_outcomes)
                    if server.submit_next_block_candidate(defer_accounting=True):
                        rounds += 1
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
            service.__dict__.pop(
                "_collapse_superseded_block_candidates",
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
    args = parser.parse_args()
    rig = CandidateStormRig(
        candidates=args.candidates,
        queue_depth=args.queue_depth,
        quiet=not args.verbose,
    )
    live = rig.seed_live()
    restarted = rig.restart_and_enumerate()
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
        if args.drain_per_row:
            drain = rig.drain_per_row(
                rig.restarted_server,
                stop_before_hash=winner,
            )
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
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
