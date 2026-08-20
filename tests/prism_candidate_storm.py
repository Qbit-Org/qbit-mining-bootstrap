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
``SingleWriterShareLedger``; it does not include PostgreSQL wait, qbitd, or a
full drain.  Issues #187 and #185 can extend the same rig with wall-clock
restart/drain assertions and component-memory gauges without rebuilding the
storm fixture.
"""

from __future__ import annotations

import argparse
import json
import queue
import sys
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

    def _coordinator(self) -> tuple[Any, Any]:
        server, state, _recording = submit_coordinator(tip=STORM_PARENT_HASH)
        server.max_blocks = self.candidates + 1
        server.stop_after_block = False
        server.block_candidate_queue = queue.Queue(maxsize=self.queue_depth)
        server.ledger = self.ledger
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
    args = parser.parse_args()
    rig = CandidateStormRig(
        candidates=args.candidates,
        queue_depth=args.queue_depth,
        quiet=not args.verbose,
    )
    live = rig.seed_live()
    restarted = rig.restart_and_enumerate()
    print(
        json.dumps(
            {
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
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
