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

This is a component instrument, not a performance threshold.  It uses the
real coordinator admission, durable codec, pagination, payout barriers, and
``SingleWriterShareLedger``; it does not include PostgreSQL wait or qbitd.
Issues #187 and #185 can extend the same rig with wall-clock restart/drain
assertions and component-memory gauges without rebuilding the storm fixture.

The decided-height extension models the instant after the storm height is
settled: ``decide_height`` makes one candidate the active block at that
height (``DecidedHeightRpc``), ``ownership`` reads every candidate's
ownership and node-offer-evidence facts from the shipped state so a
maintenance selector can be evaluated against the real population, and
``drain_per_row`` drives the shipped per-row disposition path the way the
submitter loop does and reports what it cost (RPC calls by method, ledger
attempt marks, replay enumerations).  The per-row drain is the oracle any
set-oriented selector has to agree with; the instrument does not contain a
selector itself.
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
from typing import Any, ContextManager

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.block_candidates import (  # noqa: E402
    MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
    MAX_PENDING_BLOCK_CANDIDATES,
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
    # Candidates carrying any evidence that a node offer happened or is in
    # flight (see CandidateOwnership.node_offer_evidence). The outstanding
    # and replay-inflight markers above are deliberately NOT part of it.
    offer_evidence_marked: int = 0


@dataclass(frozen=True)
class CandidateOwnership:
    """Ownership and evidence facts for one durable candidate at one instant.

    Every field is read from the shipped coordinator/ledger state, so a
    maintenance selector can be evaluated against the real population
    instead of a hand-built corpus. ``outstanding`` and ``replay_inflight``
    are process-ownership markers; the fields folded into
    ``node_offer_evidence`` are the ones that mean a node offer happened,
    is happening, or left acceptance evidence behind.
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
        """Whether anything says this hash was (or is being) offered to qbitd."""
        return bool(
            self.disposition_held
            or self.retry_held
            or self.node_acceptance_retained
            or self.tip_observed
            or self.accounted_accepted
            or self.terminal_outcome is not None
            or self.pool_block_chain_state is not None
        )


@dataclass(frozen=True)
class PerRowDrainReport:
    """What the shipped per-row disposition path cost to drain a storm."""

    rounds: int
    replay_enumerations: int
    abandoned_rows: int
    submitted_rows: int
    pending_rows: int
    rpc_calls: dict[str, int]
    ledger_attempt_marks: int
    wall_seconds: float


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
        server._note_block_replay_enumeration_owed()
        with self._silence():
            restored = server.replay_pending_block_candidates()
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
            offer_evidence_marked=sum(
                1
                for record in self.ownership(server)
                if record.node_offer_evidence
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

    def _next_dequeued_hash(self, server: Any) -> str | None:
        """Peek the hash ``submit_next`` would take next, in its order."""
        service = server._ensure_block_candidate_service()
        service._ensure_block_replay_state()
        with server.lock:
            retry = service.retry_candidate
        if retry is not None:
            return str(retry.submission.block_hash_hex).lower()
        for queue_obj in (service.candidate_queue, service._block_replay_candidate_queue):
            items = list(queue_obj.queue)
            if items:
                return str(items[0].submission.block_hash_hex).lower()
        return None

    def drain_per_row(
        self,
        server: Any,
        *,
        stop_before_hash: str | None = None,
        max_rounds: int | None = None,
    ) -> PerRowDrainReport:
        """Drive the shipped per-row path the way the submitter loop does.

        Alternates ``submit_next_block_candidate()`` with the outbox replay
        query exactly as ``BlockCandidateService.run`` does, so coalesced
        wakeups are recovered through the real enumeration. Stops before the
        given hash would be dequeued (the decided winner's own finalization
        tail is outside this instrument), at ``max_rounds``, or when nothing
        is left to dequeue and the enumeration restores nothing new.
        """
        rpc = server.rpc
        calls_before = dict(getattr(rpc, "calls", {}))
        outbox = self.ledger._block_candidate_outbox
        attempts_before = sum(int(row["attempt_count"]) for row in outbox.values())
        rounds = 0
        enumerations = 0
        started = time.monotonic()
        with self._silence():
            while True:
                if max_rounds is not None and rounds >= max_rounds:
                    break
                if (
                    stop_before_hash is not None
                    and self._next_dequeued_hash(server) == stop_before_hash.lower()
                ):
                    break
                ran = server.submit_next_block_candidate()
                if ran:
                    rounds += 1
                    continue
                enumerations += 1
                if server.replay_pending_block_candidates() == 0:
                    break
        counts = {"pending": 0, "submitted": 0, "abandoned": 0}
        for row in outbox.values():
            counts[str(row["state"])] = counts.get(str(row["state"]), 0) + 1
        calls_after = dict(getattr(rpc, "calls", {}))
        return PerRowDrainReport(
            rounds=rounds,
            replay_enumerations=enumerations,
            abandoned_rows=counts["abandoned"],
            submitted_rows=counts["submitted"],
            pending_rows=counts["pending"],
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
            report["decided"]["per_row_drain"] = asdict(drain)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
