#!/usr/bin/env python3
"""Acceptance-to-preview-publication latency, driven by accepted-block cadence.

Issue #181 item 3.  The 2026-08-21 testnet4 validation run had #190/#191/#196
deployed and demonstrably active, and the accepted-parent tail still dominated:
25 accepted-parent preview wait timeouts, the first two at only 0.32 PH/s,
semantic current-work coverage repeatedly at 0.0, and the coordinator using
about 1.26 CPU cores at ~8.5 PH/s.  That comment states the distinguishing
input directly: "The distinguishing input should be **accepted-block cadence**,
not just PH/s: this testnet difficulty turns the hashrate into rapid
consecutive accepted tips, which is what exercises the tail."

So this instrument is parameterised on **accepted tips per minute**, never on
hashrate.  It releases one definitively accepted winner per tip, each at the
next height and parented on the previous winner, with a fixed number of
same-height siblings behind it, and measures the shipped
``qbit_prism_accepted_block_preview_publication_seconds`` histogram end to end
through the real coordinator: the real bounded FIFO ``candidate_queue``, the
real submitter split (``submit_next`` with ``defer_accounting=True``, the call
``BlockCandidateService.run`` makes), the real bounded accounting handoff and
its unbounded spillover, the shipped ``block_accounting_loop`` on its own
thread, and the shipped payout-preview publication boundary.

Why the node latency is modelled rather than left at zero
---------------------------------------------------------
The quantity under test is a wall-clock queueing latency, and the cost that
fills the accounting lane is node round-trips: the #181 spike measured a stale
sibling at "1 ``submitblock`` + 3 ``getbestblockhash`` + 3 ``getblockheader`` +
2 ledger writes".  Against an in-process fake those calls cost microseconds, so
a zero-latency rig reports a p95 near zero for *every* dispatch order and can
therefore distinguish nothing.  ``rpc_latency_seconds`` restores the only cost
that makes ordering observable.  It is an explicit, reported parameter of the
instrument: the tips-per-minute numbers below are cadences relative to *this*
modelled drain rate, and are not claims about testnet4's block interval.

What is deliberately not modelled: PostgreSQL wait (the 2026-08-21 run had
Postgres at ~12% CPU and the host at ~1.26 cores, so the tail was not storage
or CPU bound), qbitd itself, and the child job-build side of the wait.  This
measures the producer half of the 5 s child wait budget --
``DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS`` -- which is the half
#181 is about.  That budget is not tuned here and must not be.

Relationship to the shipped instruments
---------------------------------------
``tests/prism_candidate_storm.py`` is the per-row oracle for a *single* decided
height: it seeds one same-parent storm, and its per-row drain explicitly stops
before the winner because "the decided winner, whose finalization tail is
outside this instrument".  The accepted tail *is* what this file measures, and
it needs consecutive heights over a wall clock, so the cadence driver lives
here rather than being bolted onto a single-height characterization rig.  It
reuses that rig's proven fixtures directly -- ``SingleWriterShareLedger`` with
durable rows appended before admission, the ``PendingShare``/intent seeding
shape, and a per-method-counting RPC.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import tempfile
import threading
import time
import unittest
from contextlib import nullcontext, redirect_stdout
from dataclasses import asdict, dataclass, field
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ContextManager

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.block_candidates import (  # noqa: E402
    COLLAPSE_DIFFICULTY_SCALE,
    COLLAPSE_POW_LIMIT_BITS,
    PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS,
    PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS,
)
from lab.prism.payout_state import (  # noqa: E402
    DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS,
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
    verified_audit_report,
    verified_block_bundle,
)


CADENCE_PARENT_HASH = "22" * 32
CADENCE_BASE_HEIGHT = 10
# One node round-trip. Sized from the #181 spike's per-row measurement (a stale
# sibling costs about six of these) so that a single stale accounting task
# costs roughly a fifth of a second, the order the 2026-08-21 run's 5-12 s
# build_superseded waves imply for a handful of them.
DEFAULT_CADENCE_RPC_LATENCY_SECONDS = 0.03
# Same-height candidates released behind each accepted winner. The 2026-08-20
# storm reached 3,120; #196 brought the durable peak to 158. Four per tip is
# well inside what #196 leaves for the per-row lane and keeps the instrument's
# own runtime bounded.
DEFAULT_CADENCE_SIBLINGS_PER_TIP = 4
# The acceptance bar, restated from the 2026-08-21 comment: "p95
# acceptance-to-preview publication remains below the 5 s child wait budget at
# the tested accepted-block cadence".
PREVIEW_PUBLICATION_P95_BUDGET_SECONDS = (
    DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS
)


def bucket_percentile(
    buckets: dict[float, int],
    count: int,
    quantile: float,
) -> float:
    """Smallest histogram boundary whose cumulative count covers ``quantile``.

    Prometheus buckets are cumulative, so a percentile read from them is a
    bucket boundary, not an interpolated sample. Returning the boundary is the
    honest answer: it means "at least ``quantile`` of observations completed
    within this many seconds". ``inf`` means only the ``+Inf`` bucket covers
    it, i.e. the percentile ran off the top of the tuple.
    """
    if count <= 0:
        return 0.0
    target = quantile * count
    for boundary in sorted(buckets):
        if int(buckets[boundary]) >= target:
            return float(boundary)
    return math.inf


@dataclass(frozen=True)
class CadenceReport:
    """One cadence sweep's inputs and measured publication latency."""

    tips: int
    tips_per_minute: float
    inter_acceptance_seconds: float
    siblings_per_tip: int
    rpc_latency_seconds: float
    accepted_block_count: int
    published_count: int
    degraded_count: int
    published_p95_seconds: float
    published_p50_seconds: float
    published_mean_seconds: float
    slowest_bucket_seconds: float
    durable_pending_rows: int
    node_calls: dict[str, int] = field(default_factory=dict)

    @property
    def observed_count(self) -> int:
        return self.published_count + self.degraded_count


class CadenceChainRpc(FakeRpc):
    """A node that accepts registered winners and calls every sibling stale.

    Each registered winner's ``submitblock`` returns ``None`` -- the definitive
    acceptance the #181 discriminator keys on -- and occupies its height, which
    moves the best tip. Every other ``submitblock`` answers ``inconclusive``: a
    valid block that did not become the tip, which is what a same-height
    sibling of an already-accepted winner gets. Calls are counted per method,
    as ``DecidedHeightRpc`` counts them, and every call pays
    ``latency_seconds`` so a node round-trip has the cost that makes lane
    ordering observable in wall clock.

    ``getblockheader`` reports the occupant's compact ``bits`` (and the raw
    ``difficulty`` float qbitd reports beside it) because #181 item 2's
    dequeue-time skip reads clause 4b from them. Without ``bits`` the clause
    has no work value to compare and every evaluation fails closed, so the
    instrument would silently measure a coordinator whose skip never fires
    -- and would still report a changed p95, because failing closed is not
    free. The default is the qbit powLimit, exactly as ``DecidedHeightRpc``
    defaults, which is at or above the raw ``network_difficulty`` of 1 this
    rig stamps its candidates with.
    """

    def __init__(
        self,
        *,
        parent: str,
        height: int,
        latency_seconds: float = DEFAULT_CADENCE_RPC_LATENCY_SECONDS,
        bits: str = COLLAPSE_POW_LIMIT_BITS,
    ) -> None:
        self.tip = str(parent).lower()
        self.height = int(height)
        self.bits = str(bits).lower()
        self.latency_seconds = max(0.0, float(latency_seconds))
        self.active: dict[int, str] = {}
        self.winners: dict[str, int] = {}
        self.calls: dict[str, int] = {}
        self._lock = threading.Lock()

    def register_winner(self, block_hash: str, height: int) -> None:
        with self._lock:
            self.winners[str(block_hash).lower()] = int(height)

    def call_count(self) -> dict[str, int]:
        with self._lock:
            return dict(self.calls)

    def call(self, method: str, params: list[object] | None = None) -> object:
        with self._lock:
            self.calls[method] = self.calls.get(method, 0) + 1
        if self.latency_seconds:
            time.sleep(self.latency_seconds)
        if method == "getbestblockhash":
            with self._lock:
                return self.tip
        if method == "getblockcount":
            with self._lock:
                return self.height
        if method == "submitblock":
            # The candidate carries its own hash as its block bytes, so one
            # fake node can tell the registered winner from its siblings.
            block_hex = str((params or [""])[0]).lower()
            with self._lock:
                height = self.winners.get(block_hex)
                if height is None:
                    return "inconclusive"
                self.active[height] = block_hex
                self.tip = block_hex
                self.height = max(self.height, height)
                return None
        if method == "getblockhash":
            height = int((params or [0])[0])
            with self._lock:
                found = self.active.get(height)
            if found is None:
                raise RuntimeError(
                    f"qbit RPC getblockhash failed: -8 unknown height {height}"
                )
            return found
        if method == "getblockheader":
            block_hash = str((params or [""])[0]).lower()
            with self._lock:
                heights = {
                    active: height for height, active in self.active.items()
                }
                height = heights.get(block_hash)
                top = self.height
            if height is None:
                raise RuntimeError(
                    "qbit RPC getblockheader failed: -5 Block not found"
                )
            return {
                "hash": block_hash,
                "height": height,
                "confirmations": top - height + 1,
                "bits": self.bits,
                # Reported beside bits, as qbitd does, and a million times
                # smaller than the scaled units a candidate row carries: a
                # reader that took this float instead would make an
                # equal-work occupant read as weaker.
                "difficulty": (
                    scaled_network_difficulty(self.bits)
                    / COLLAPSE_DIFFICULTY_SCALE
                ),
            }
        return super().call(method, params)


class AcceptedTipCadenceRig:
    """Release accepted tips at a fixed cadence and measure publication.

    The lanes run in the shipped production split: the submitter thread calls
    ``submit_next`` with ``defer_accounting=True`` and hands every node offer
    to the accounting lane, and ``block_accounting_loop`` drains that lane on
    its own thread. Nothing about either lane's ordering is simulated here --
    the queues, the spillover rule and the dequeue discipline are the shipped
    ones -- so what the histogram records is the shipped dispatch order.
    """

    def __init__(
        self,
        *,
        tips: int,
        tips_per_minute: float,
        siblings_per_tip: int = DEFAULT_CADENCE_SIBLINGS_PER_TIP,
        rpc_latency_seconds: float = DEFAULT_CADENCE_RPC_LATENCY_SECONDS,
        quiet: bool = True,
    ) -> None:
        if tips <= 0:
            raise ValueError("tips must be positive")
        if tips_per_minute <= 0:
            raise ValueError("tips_per_minute must be positive")
        if siblings_per_tip < 0:
            raise ValueError("siblings_per_tip must not be negative")
        self.tips = int(tips)
        self.tips_per_minute = float(tips_per_minute)
        self.siblings_per_tip = int(siblings_per_tip)
        self.rpc_latency_seconds = float(rpc_latency_seconds)
        self.quiet = quiet
        self.ledger = SingleWriterShareLedger()
        self.rpc = CadenceChainRpc(
            parent=CADENCE_PARENT_HASH,
            height=CADENCE_BASE_HEIGHT - 1,
            latency_seconds=self.rpc_latency_seconds,
        )
        self._lane_failures: list[str] = []

    @property
    def inter_acceptance_seconds(self) -> float:
        return 60.0 / self.tips_per_minute

    def _silence(self) -> ContextManager[Any]:
        if not self.quiet:
            return nullcontext()
        return redirect_stdout(StringIO())

    # -- fixture -----------------------------------------------------------

    def _coordinator(self) -> tuple[Any, Any]:
        server, state, _recording = submit_coordinator(tip=CADENCE_PARENT_HASH)
        # Every released winner must be admissible: max-block admission is not
        # what this instrument is measuring.
        server.max_blocks = self.tips + 1
        server.stop_after_block = False
        server.accepted_block_count = 0
        server.ledger = self.ledger
        server.rpc = self.rpc
        # A shipped tunable, set to zero so a wakeup parked by a transient
        # failure is re-offerable in the same round rather than parking behind
        # a not-before deadline that would show up as publication latency it
        # did not cause.
        server.block_candidate_retry_initial_seconds = 0.0
        server._ensure_job_cache_state()
        server.build_audit_bundle = lambda **_kwargs: verified_block_bundle()
        server.verify_bundle = self._verify_bundle
        return server, state

    @staticmethod
    def _verify_bundle(
        *_args: Any,
        expected_block_height: int | None = None,
        **_kwargs: Any,
    ) -> dict[str, object]:
        # The shipped verifier report is height-checked against the candidate;
        # consecutive tips mean consecutive heights, so the stub answers the
        # height it was asked about rather than a fixed one.
        report = verified_audit_report()
        if expected_block_height is not None:
            report["block_height"] = int(expected_block_height)
        return report

    def _candidate(
        self,
        server: Any,
        state: Any,
        *,
        block_hash: str,
        height: int,
        parent: str,
        ordinal: int,
    ) -> tuple[Any, tuple[Any, Any]]:
        job_id = f"job-{height}-{ordinal}"
        context = SimpleNamespace(
            job=server.jobs["job-1"].job,
            template={
                "previousblockhash": parent,
                "height": height,
                "coinbasevalue": 50_00000000,
            },
            found_block={"network_difficulty": 1},
            issued_at_ms=12345,
            collection_only=False,
            worker=server.jobs["job-1"].worker,
            shares_json=[],
            prior_balances=[],
            # The compact issued-job preview. Its presence is what makes the
            # accepted tail publish a payout preview at all
            # (block_finalization.py's issued_preview branch), which is the
            # end of the interval under measurement.
            prospective_prior_balances=(),
        )
        server.jobs[job_id] = context
        pending = PendingShare(
            share_id=f"miner-a:{block_hash}",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=height,
            job_id=job_id,
            job_issued_at_ms=1,
            accepted_at_ms=ordinal,
            ntime=1,
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex=block_hash,
            share_pass=True,
            block_pass=True,
        )
        candidate = block_candidate(
            server,
            state,
            submission,
            job_id=job_id,
            pending_share=pending,
        )
        return candidate, (pending, server.block_candidate_intent(candidate))

    @staticmethod
    def _hash_for(tip_index: int, ordinal: int) -> str:
        return f"{((tip_index + 1) << 8) | ordinal:064x}"

    # -- lanes -------------------------------------------------------------

    def _submitter_loop(self, server: Any) -> None:
        service = server._ensure_block_candidate_service()
        # block_submit_loop stamps this on entry; the progress boundary stamps
        # are gated to it.
        service._block_submitter_thread_ident = threading.get_ident()
        while not server.stop_event.is_set():
            try:
                server.submit_next_block_candidate(
                    timeout=0.05,
                    defer_accounting=True,
                )
            except Exception as exc:  # noqa: BLE001
                # Reported, never swallowed: a submitter that dies silently
                # would look like a cadence the instrument simply never
                # reached.
                self._lane_failures.append(f"submitter: {exc!r}")
                return

    # -- driver ------------------------------------------------------------

    def _build_tips(
        self,
        server: Any,
        state: Any,
    ) -> list[tuple[int, list[Any], list[Any]]]:
        """Build every tip's candidates before either lane starts.

        Job contexts are registered in ``server.jobs``, which shipped paths
        iterate. Building them all up front keeps the driver from mutating
        that dictionary underneath a running lane; only the durable append and
        the queue admission happen on the cadence, which is what the cadence
        is supposed to control.
        """
        built: list[tuple[int, list[Any], list[Any]]] = []
        for tip_index in range(self.tips):
            height = CADENCE_BASE_HEIGHT + tip_index
            parent = (
                CADENCE_PARENT_HASH
                if tip_index == 0
                else self._hash_for(tip_index - 1, 0)
            )
            candidates = []
            durable_records = []
            for ordinal in range(self.siblings_per_tip + 1):
                candidate, durable_record = self._candidate(
                    server,
                    state,
                    block_hash=self._hash_for(tip_index, ordinal),
                    height=height,
                    parent=parent,
                    ordinal=ordinal,
                )
                candidates.append(candidate)
                durable_records.append(durable_record)
            built.append((height, candidates, durable_records))
        return built

    def _release_tip(
        self,
        server: Any,
        tip_index: int,
        built: list[tuple[int, list[Any], list[Any]]],
    ) -> None:
        height, candidates, durable_records = built[tip_index]
        # Registered before admission: the winner has to be acceptable the
        # instant the submitter offers it.
        self.rpc.register_winner(self._hash_for(tip_index, 0), height)
        # Durable first, exactly as live admission does: the outbox row is
        # what makes a coalesced wakeup replayable. Appending per tip rather
        # than up front is also what keeps a replay enumeration from pulling
        # a later tip's candidates forward and dissolving the cadence.
        self.ledger.append_batch(durable_records)
        for candidate in candidates:
            server.enqueue_block_candidate(candidate)

    def _drive(
        self,
        server: Any,
        built: list[tuple[int, list[Any], list[Any]]],
    ) -> None:
        interval = self.inter_acceptance_seconds
        started = time.monotonic()
        for tip_index in range(self.tips):
            delay = (started + tip_index * interval) - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self._release_tip(server, tip_index, built)

    def _await_publication(self, server: Any, *, timeout: float) -> None:
        """Wait until every released winner has published, or time out.

        The deadline is generous on purpose: a cadence that overruns it is a
        result, not a flake, and the report carries the shortfall so the test
        can say what did not publish.
        """
        service = server._ensure_block_candidate_service()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            snapshot = service.accepted_block_preview_publication_snapshot()
            observed = sum(
                int(histogram["count"]) for histogram in snapshot.values()
            )
            if observed >= self.tips:
                return
            time.sleep(0.05)

    def run(self) -> CadenceReport:
        server, state = self._coordinator()
        service = server._ensure_block_candidate_service()
        service._ensure_block_accounting_state()
        accounting = threading.Thread(
            target=server.block_accounting_loop,
            name="cadence-block-accounting",
            daemon=True,
        )
        submitter = threading.Thread(
            target=self._submitter_loop,
            args=(server,),
            name="cadence-block-submitter",
            daemon=True,
        )
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            with self._silence():
                built = self._build_tips(server, state)
                accounting.start()
                submitter.start()
                try:
                    self._drive(server, built)
                    # The drain budget scales with the work released, not with
                    # the cadence: a saturated lane still has to be allowed to
                    # finish so its latencies land in the histogram instead of
                    # being truncated into a flattering p95.
                    self._await_publication(
                        server,
                        timeout=30.0
                        + self.tips
                        * (self.siblings_per_tip + 1)
                        * 12.0
                        * max(self.rpc_latency_seconds, 0.001),
                    )
                finally:
                    server.stop_event.set()
                    submitter.join(timeout=60.0)
                    accounting.join(timeout=60.0)
            snapshot = service.accepted_block_preview_publication_snapshot()
            pending = self.ledger.block_candidate_pending_metrics()
        published = snapshot["published"]
        degraded = snapshot["degraded"]
        published_count = int(published["count"])
        return CadenceReport(
            tips=self.tips,
            tips_per_minute=self.tips_per_minute,
            inter_acceptance_seconds=self.inter_acceptance_seconds,
            siblings_per_tip=self.siblings_per_tip,
            rpc_latency_seconds=self.rpc_latency_seconds,
            accepted_block_count=int(server.accepted_block_count),
            published_count=published_count,
            degraded_count=int(degraded["count"]),
            published_p95_seconds=bucket_percentile(
                published["buckets"], published_count, 0.95
            ),
            published_p50_seconds=bucket_percentile(
                published["buckets"], published_count, 0.50
            ),
            published_mean_seconds=(
                float(published["sum"]) / published_count
                if published_count
                else 0.0
            ),
            slowest_bucket_seconds=bucket_percentile(
                published["buckets"], published_count, 1.0
            ),
            durable_pending_rows=int(pending.get("pending_count", -1)),
            node_calls=self.rpc.call_count(),
        )

    @property
    def lane_failures(self) -> tuple[str, ...]:
        return tuple(self._lane_failures)


def accepted_lane_is_present(server: Any) -> bool:
    """Whether item 1's dedicated definitive-acceptance lane has landed.

    On the #181 base (``ce4a1f3``) every accounting handoff shares one bounded
    primary queue and its unbounded spillover, and a definitively accepted task
    is dispatched strictly behind whatever stale work is already queued. The
    high-cadence assertion below is written against whichever of those two
    worlds it is running in, so that this file is a truthful statement in both
    and becomes the bar the moment the lane exists.
    """
    service = server._ensure_block_candidate_service()
    service._ensure_block_accounting_state()
    return hasattr(service, "_block_accounting_accepted_queue")


class AcceptedPreviewPublicationHistogramTests(unittest.TestCase):
    """The metric itself: labels, boundaries, and what closes an interval."""

    def _service(self) -> Any:
        server, _state, _ledger = submit_coordinator()
        return server, server._ensure_block_candidate_service()

    def test_five_second_child_wait_budget_is_a_bucket_boundary(self) -> None:
        # The acceptance criterion is a p95 below the 5 s child wait budget,
        # and a Prometheus histogram can only answer that at a boundary it
        # carries. The range must also extend past it, or a saturated tail
        # folds into +Inf and stops being measurable.
        self.assertIn(
            PREVIEW_PUBLICATION_P95_BUDGET_SECONDS,
            PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS,
        )
        self.assertGreater(
            max(PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS),
            PREVIEW_PUBLICATION_P95_BUDGET_SECONDS,
        )
        self.assertEqual(
            tuple(sorted(PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS)),
            PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS,
        )
        self.assertEqual(
            PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS,
            ("published", "degraded"),
        )

    def test_publication_without_an_acceptance_stamp_observes_nothing(
        self,
    ) -> None:
        # A replayed candidate confirmed by chain probe never made a
        # definitive offer in this process. Reporting a zero for it would
        # understate the very percentile the bar is read from.
        server, service = self._service()
        server._observe_accepted_block_preview_publication(
            "ab" * 32,
            result="published",
        )
        snapshot = service.accepted_block_preview_publication_snapshot()
        self.assertEqual(snapshot["published"]["count"], 0)
        self.assertEqual(snapshot["degraded"]["count"], 0)

    def test_only_the_first_publication_of_a_hash_is_measured(self) -> None:
        server, service = self._service()
        service._note_accepted_block_preview_acceptance("AB" * 32)
        server._observe_accepted_block_preview_publication(
            "ab" * 32,
            result="published",
        )
        # A matching republication -- the early return with an existing
        # published generation, or a deferred retry's republish -- is not a
        # second interval.
        server._observe_accepted_block_preview_publication(
            "ab" * 32,
            result="published",
        )
        server._observe_accepted_block_preview_publication(
            "ab" * 32,
            result="degraded",
        )
        # Nor does a second definitive offer of the same hash restart it.
        service._note_accepted_block_preview_acceptance("ab" * 32)
        server._observe_accepted_block_preview_publication(
            "ab" * 32,
            result="published",
        )
        snapshot = service.accepted_block_preview_publication_snapshot()
        self.assertEqual(snapshot["published"]["count"], 1)
        self.assertEqual(snapshot["degraded"]["count"], 0)

    def test_degraded_publication_is_labelled_apart(self) -> None:
        server, service = self._service()
        service._note_accepted_block_preview_acceptance("cd" * 32)
        server._observe_accepted_block_preview_publication(
            "cd" * 32,
            result="degraded",
        )
        snapshot = service.accepted_block_preview_publication_snapshot()
        self.assertEqual(snapshot["degraded"]["count"], 1)
        self.assertEqual(snapshot["published"]["count"], 0)

    def test_acceptance_stamps_are_bounded(self) -> None:
        from lab.prism.block_candidates import (
            MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS,
        )

        _server, service = self._service()
        for index in range(MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS + 64):
            service._note_accepted_block_preview_acceptance(f"{index:064x}")
        self.assertLessEqual(
            len(service._accepted_block_preview_acceptance_monotonic),
            MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS,
        )

    def test_failed_offer_is_not_a_definitive_acceptance(self) -> None:
        # The submitblock error path also builds ``result=None``; a two-clause
        # test would stamp a failed offer as an acceptance and then measure a
        # publication that a later, successful offer produced.
        from lab.prism.block_candidates import _BlockCandidateNodeSubmission

        failed = _BlockCandidateNodeSubmission(
            attempted=True,
            error=RuntimeError("submitblock failed"),
        )
        self.assertIsNone(failed.result)
        self.assertFalse(
            failed.attempted
            and failed.error is None
            and failed.result is None
        )


class AcceptedTipCadenceRegressionTests(unittest.TestCase):
    """The bar: p95 publication latency at a low and a high accepted cadence."""

    # A cadence comfortably slower than the modelled per-tip accounting cost,
    # so the lane drains between tips. This is the "normal forward progress"
    # case from the 2026-08-21 acceptance criteria.
    LOW_CADENCE_TIPS_PER_MINUTE = 40.0
    LOW_CADENCE_TIPS = 8
    # A cadence faster than the lane can drain, which is what the run's tip
    # bursts produced. On the #181 base this is the inversion itself: the
    # accepted task for height H+n sorts behind every stale sibling of heights
    # H..H+n-1 already in the accounting queue.
    HIGH_CADENCE_TIPS_PER_MINUTE = 150.0
    HIGH_CADENCE_TIPS = 20

    def _report(self, **kwargs: Any) -> CadenceReport:
        rig = AcceptedTipCadenceRig(**kwargs)
        report = rig.run()
        self.assertEqual(
            rig.lane_failures,
            (),
            f"a coordinator lane died during the sweep: {rig.lane_failures}",
        )
        self.assertEqual(
            report.published_count + report.degraded_count,
            report.tips,
            "every released winner must close an acceptance-to-publication "
            f"interval; report={asdict(report)}",
        )
        return report

    def test_low_cadence_publishes_well_inside_the_child_wait_budget(
        self,
    ) -> None:
        report = self._report(
            tips=self.LOW_CADENCE_TIPS,
            tips_per_minute=self.LOW_CADENCE_TIPS_PER_MINUTE,
        )
        self.assertEqual(report.degraded_count, 0, asdict(report))
        self.assertLessEqual(
            report.published_p95_seconds,
            PREVIEW_PUBLICATION_P95_BUDGET_SECONDS,
            "p95 acceptance-to-preview publication must stay below the 5 s "
            f"child wait budget at a low accepted cadence; report={asdict(report)}",
        )
        # "Comfortably": at a cadence the lane keeps up with, the accepted tail
        # should not be spending a meaningful fraction of the budget at all.
        self.assertLessEqual(
            report.published_p95_seconds,
            2.0,
            f"low-cadence p95 is not comfortable; report={asdict(report)}",
        )

    def test_high_cadence_exercises_the_accepted_parent_tail(self) -> None:
        server, _state, _ledger = submit_coordinator()
        lane_present = accepted_lane_is_present(server)
        report = self._report(
            tips=self.HIGH_CADENCE_TIPS,
            tips_per_minute=self.HIGH_CADENCE_TIPS_PER_MINUTE,
        )
        if lane_present:
            # Item 1 has landed: this is the bar #181 closes against.
            self.assertLessEqual(
                report.published_p95_seconds,
                PREVIEW_PUBLICATION_P95_BUDGET_SECONDS,
                "p95 acceptance-to-preview publication must stay below the "
                "5 s child wait budget at a high accepted-block cadence; "
                f"report={asdict(report)}",
            )
            return
        # On the #181 base there is no accepted lane, and this assertion is
        # the honest statement of what the instrument sees: the tail really is
        # exercised. If this ever fails on the base, the instrument has
        # stopped reproducing the failure mode and the bar above would be one
        # items 1 and 2 could pass without fixing anything -- which is exactly
        # the outcome the contract says to report rather than paper over.
        self.assertGreater(
            report.published_p95_seconds,
            PREVIEW_PUBLICATION_P95_BUDGET_SECONDS,
            "without a dedicated definitive-acceptance lane the high-cadence "
            "sweep must drive p95 publication latency past the 5 s child "
            "wait budget; if it no longer does, this instrument has stopped "
            f"exercising the accepted-parent tail. report={asdict(report)}",
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Report acceptance-to-preview-publication latency at one or more "
            "accepted-block cadences."
        )
    )
    parser.add_argument(
        "--tips-per-minute",
        type=float,
        action="append",
        default=None,
        help="accepted tips per minute; repeatable (default: 40 and 150)",
    )
    parser.add_argument("--tips", type=int, default=None)
    parser.add_argument(
        "--siblings-per-tip",
        type=int,
        default=DEFAULT_CADENCE_SIBLINGS_PER_TIP,
    )
    parser.add_argument(
        "--rpc-latency-seconds",
        type=float,
        default=DEFAULT_CADENCE_RPC_LATENCY_SECONDS,
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    cadences = args.tips_per_minute or [
        AcceptedTipCadenceRegressionTests.LOW_CADENCE_TIPS_PER_MINUTE,
        AcceptedTipCadenceRegressionTests.HIGH_CADENCE_TIPS_PER_MINUTE,
    ]
    reports = []
    for tips_per_minute in cadences:
        if args.tips is not None:
            tips = args.tips
        elif tips_per_minute >= 100:
            tips = AcceptedTipCadenceRegressionTests.HIGH_CADENCE_TIPS
        else:
            tips = AcceptedTipCadenceRegressionTests.LOW_CADENCE_TIPS
        rig = AcceptedTipCadenceRig(
            tips=tips,
            tips_per_minute=tips_per_minute,
            siblings_per_tip=args.siblings_per_tip,
            rpc_latency_seconds=args.rpc_latency_seconds,
            quiet=not args.verbose,
        )
        report = rig.run()
        reports.append(asdict(report))
        if rig.lane_failures:
            reports[-1]["lane_failures"] = list(rig.lane_failures)
    print(json.dumps(reports, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        del sys.argv[1]
        raise SystemExit(main())
    unittest.main()
