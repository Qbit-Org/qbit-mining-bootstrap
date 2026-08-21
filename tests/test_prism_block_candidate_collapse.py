#!/usr/bin/env python3
"""Decided-height block-candidate collapse (issue #183).

The 2026-08-20 testnet4 incident left 3,120 durable block candidates behind
one decided height.  The shipped per-row disposition path terminalizes them
correctly but at one node offer, one disposition lease, and one durable
write each; ``tests/prism_candidate_storm.py`` measures that cost and, in
``drain_per_row``, is the oracle every set-oriented selector has to agree
with.

These tests own the selector and its apply: predicate S clause by clause,
its fail-closed behaviour, the bounded chain reads, the non-blocking lease,
the immediate pre-write revalidation, the exact-returned-set cleanup and its
failure modes, the page partition both replay enumeration shapes must
perform, the bounded logs and fixed-label metrics, and the storm comparisons
against the shipped per-row oracle at 100 and 3,120 candidates in both the
live and the restart view.

The safety property under test is one-directional and absolute: for every
comparison ``S - oracle_abandoned`` must be empty.  A conservative
``oracle_abandoned - S`` delta is allowed and reported; an S-only hash is a
bug that would destroy a block the per-row path deliberately kept.
"""

from __future__ import annotations

import queue
import sys
import threading
import time
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.block_candidates import (  # noqa: E402
    BLOCK_CANDIDATE_COLLAPSE_CLEANUP_FLOOR_STEP,
    BLOCK_CANDIDATE_COLLAPSE_CLEANUP_PAYOUT_STEP,
    BLOCK_CANDIDATE_COLLAPSE_CLEANUP_STEPS,
    BLOCK_CANDIDATE_COLLAPSE_LOG_FAILURES,
    BLOCK_CANDIDATE_COLLAPSE_LOG_GROUPS,
    BLOCK_CANDIDATE_COLLAPSE_LOG_SAMPLE_HASHES,
    BLOCK_CANDIDATE_TERMINAL_OUTCOME_EVICTION_SCAN,
    COLLAPSE_DIFFICULTY_SCALE,
    COLLAPSE_POW_LIMIT_BITS,
    DEFAULT_BLOCK_ACCOUNTING_CLEANUP_RETRY_WORK_ITEMS,
    MAX_BLOCK_CANDIDATE_TERMINAL_OUTCOMES,
    MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
    MAX_PENDING_BLOCK_CANDIDATES,
    PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES,
    PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON,
    PRISM_BLOCK_CANDIDATE_COLLAPSE_STALE_JOB_CLASS,
    PRISM_STALE_JOB_ABANDON_CLASSES,
    _BlockCandidateAccountingTask,
    _BlockCandidateChainView,
    _BlockCandidateNodeSubmission,
    _collapse_scaled_difficulty,
)
from lab.prism.metrics import MetricsRenderer  # noqa: E402
from lab.prism.share_ledger import (  # noqa: E402
    PendingShare,
    SingleWriterShareLedger,
)
from lab.prism.template_artifacts import scaled_network_difficulty  # noqa: E402
from lab.prism.share_submission import PRISM_REJECTION_STALE_JOB  # noqa: E402
from tests.prism_candidate_storm import (  # noqa: E402
    PERTURBATIONS,
    STORM_PARENT_HASH,
    CandidateStormRig,
)
from tests.prism_vardiff_test_support import (  # noqa: E402
    FakeRpc,
    block_candidate,
    submit_coordinator,
)


STORM_PARENT = STORM_PARENT_HASH
OTHER_PARENT = "22" * 32
DECIDED_HEIGHT = 10
DECIDED_WINNER = "dd" * 32
# Compact header bits, the only work fact these tests state. A candidate's
# durable ``found_block.network_difficulty`` is
# ``scaled_network_difficulty(template["bits"])`` -- the raw difficulty times
# COLLAPSE_DIFFICULTY_SCALE -- so a chain-side work value is only comparable
# with it once it is re-derived from the occupant's own bits. Spelling the
# fixture in bits is what makes "equal work" mean the same integer on both
# sides instead of two numbers a million apart.
MIN_WORK_BITS = COLLAPSE_POW_LIMIT_BITS
# A retargeted header: raw difficulty ~2.1e9, scaled ~2.1e15.
RETARGETED_BITS = "1d00ffff"
# Scaled work above 2**53, where widening the stored integer to float rounds
# it *up* and an equal-work occupant would read as strictly weaker.
HIGH_WORK_BITS = "1b0404cb"


def _hash(index: int) -> str:
    return f"{index:064x}"


def _scaled(bits: str) -> int:
    """The PRISM-scaled difficulty a header with ``bits`` carries."""
    return scaled_network_difficulty(bits)


def _raw(bits: str) -> float:
    """The raw ``getblockheader.difficulty`` float a node reports for ``bits``.

    A million times smaller than the scaled units every candidate row is
    stamped in; the fixture reports it alongside ``bits`` exactly so a
    selector that read it again would fail the equal-work tests.
    """
    return _scaled(bits) / COLLAPSE_DIFFICULTY_SCALE


class CollapseChainRpc(FakeRpc):
    """A controllable decided-height chain view with per-method call counts.

    Every read predicate S makes is separately overridable -- absent, raising,
    or returning an unusable value -- so each fail-closed branch can be driven
    without reaching for a mock framework.
    """

    def __init__(
        self,
        *,
        tip: str = DECIDED_WINNER,
        tip_height: int = DECIDED_HEIGHT,
        active: dict[int, str] | None = None,
        bits: dict[str, str] | None = None,
    ) -> None:
        self.tip = tip.lower()
        self.tip_height = int(tip_height)
        self.active = {
            int(height): str(value).lower()
            for height, value in (active or {DECIDED_HEIGHT: DECIDED_WINNER}).items()
        }
        # Per-block compact bits; every unlisted block sits at the qbit
        # powLimit, the weakest header the chain can carry.
        self.bits = {
            str(key).lower(): str(value).lower()
            for key, value in (bits or {}).items()
        }
        self.default_bits = MIN_WORK_BITS
        self.failures: set[str] = set()
        self.results: dict[str, object] = {}
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.before_call: Any | None = None

    def call(self, method: str, params: list[object] | None = None) -> object:
        self.calls.append((method, tuple(params or ())))
        if self.before_call is not None:
            self.before_call(self, method, params)
        if method in self.failures:
            raise RuntimeError(f"qbit RPC {method} failed")
        if method in self.results:
            return self.results[method]
        if method == "getbestblockhash":
            return self.tip
        if method == "getblockcount":
            return self.tip_height
        if method == "getblockhash":
            height = int((params or [0])[0])
            if height in self.active:
                return self.active[height]
            raise RuntimeError(
                f"qbit RPC getblockhash failed: -8 unknown height {height}"
            )
        if method == "getblockheader":
            block_hash = str((params or [""])[0]).lower()
            if block_hash not in set(self.active.values()) | {self.tip}:
                raise RuntimeError(
                    "qbit RPC getblockheader failed: -5 Block not found"
                )
            bits = self.bits.get(block_hash, self.default_bits)
            return {
                "hash": block_hash,
                "height": self.tip_height,
                "confirmations": 1,
                "bits": bits,
                # qbitd reports both; ``difficulty`` is the raw float, a
                # million times smaller than the units the candidate rows
                # carry, and the selector must not read it.
                "difficulty": _raw(bits),
            }
        return super().call(method, params)

    def counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for method, _params in self.calls:
            counts[method] = counts.get(method, 0) + 1
        return counts


class CollapseLedger:
    """A real in-memory ledger with controllable page reads and batch writes."""

    def __init__(self, inner: Any | None = None) -> None:
        self.inner = inner if inner is not None else SingleWriterShareLedger()
        self.page_hook: Any | None = None
        self.abandon_hook: Any | None = None
        self.abandon_calls: list[tuple[tuple[str, ...], str]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def pending_block_candidate_rows(
        self,
        *,
        limit: int = 32,
        after_cursor: object | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.inner.pending_block_candidate_rows(
            limit=limit,
            after_cursor=after_cursor,
        )
        if self.page_hook is not None:
            rows = self.page_hook(rows)
        return rows

    def mark_block_candidates_abandoned(
        self,
        *,
        block_hashes: Any,
        error: str,
    ) -> tuple[str, ...]:
        requested = tuple(str(value).lower() for value in block_hashes)
        self.abandon_calls.append((requested, error))
        if self.abandon_hook is not None:
            return self.abandon_hook(requested, error)
        return self.inner.mark_block_candidates_abandoned(
            block_hashes=requested,
            error=error,
        )


class NoBatchCollapseLedger(CollapseLedger):
    """A compatibility ledger with no fenced batch abandonment at all."""

    def __getattr__(self, name: str) -> Any:
        if name == "mark_block_candidates_abandoned":
            raise AttributeError(name)
        return super().__getattr__(name)

    mark_block_candidates_abandoned = None  # type: ignore[assignment]


class WindowCollapseLedger(CollapseLedger):
    """A ledger whose page reader cannot paginate, forcing the legacy window."""

    def pending_block_candidate_rows(  # type: ignore[override]
        self,
        *,
        limit: int = 32,
    ) -> list[dict[str, Any]]:
        rows = self.inner.pending_block_candidate_rows(limit=limit)
        if self.page_hook is not None:
            rows = self.page_hook(rows)
        return rows


class CollapseFixture:
    """One coordinator, ledger, and chain view seeded with durable rows."""

    def __init__(
        self,
        *,
        ledger: CollapseLedger | None = None,
        rpc: CollapseChainRpc | None = None,
    ) -> None:
        server, state, _recording = submit_coordinator(tip=STORM_PARENT)
        server.max_blocks = 1 << 30
        server.stop_after_block = False
        server.block_candidate_queue = queue.Queue(
            maxsize=MAX_PENDING_BLOCK_CANDIDATES
        )
        server.block_candidate_retry_initial_seconds = 0.0
        self.ledger = ledger if ledger is not None else CollapseLedger()
        server.ledger = self.ledger
        self.rpc = rpc if rpc is not None else CollapseChainRpc()
        server.rpc = self.rpc
        self.server = server
        self.state = state
        self.service = server._ensure_block_candidate_service()
        self.service._ensure_block_replay_state()
        self.service._ensure_block_candidate_disposition_state()
        self._jobs: set[str] = set()

    # -- seeding -----------------------------------------------------------

    def job(
        self,
        *,
        parent: str = STORM_PARENT,
        height: int = DECIDED_HEIGHT,
        network_difficulty: int = 1,
    ) -> str:
        job_id = f"job-{parent}-{height}-{network_difficulty}"
        if job_id in self._jobs:
            return job_id
        base = self.server.jobs["job-1"]
        self.server.jobs[job_id] = SimpleNamespace(
            job=SimpleNamespace(
                job_id=job_id,
                share_target=base.job.share_target,
                share_difficulty=base.job.share_difficulty,
                transaction_hexes=(),
            ),
            template={
                "previousblockhash": parent,
                "height": int(height),
                "coinbasevalue": 50_00000000,
            },
            found_block={"network_difficulty": int(network_difficulty)},
            issued_at_ms=12345,
            collection_only=False,
            worker=base.worker,
            shares_json=[],
            prior_balances=[],
        )
        self._jobs.add(job_id)
        return job_id

    def seed(
        self,
        hashes: list[str],
        *,
        parent: str = STORM_PARENT,
        height: int = DECIDED_HEIGHT,
        network_difficulty: int = 1,
        credit_share_on_accept: bool = False,
    ) -> list[Any]:
        job_id = self.job(
            parent=parent,
            height=height,
            network_difficulty=network_difficulty,
        )
        candidates = []
        entries = []
        for block_hash in hashes:
            pending = PendingShare(
                share_id=f"miner-a:{block_hash}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=int(network_difficulty),
                template_height=int(height) - 1,
                job_id=job_id,
                job_issued_at_ms=1,
                accepted_at_ms=1,
                ntime=1,
            )
            candidate = block_candidate(
                self.server,
                self.state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
                job_id=job_id,
                pending_share=pending,
                credit_share_on_accept=credit_share_on_accept,
            )
            candidates.append(candidate)
            entries.append((pending, self.server.block_candidate_intent(candidate)))
        self.ledger.inner.append_batch(entries)
        return candidates

    # -- driving -----------------------------------------------------------

    def page(self, limit: int = 512) -> list[dict[str, Any]]:
        return self.ledger.pending_block_candidate_rows(limit=limit)

    def collapse(
        self,
        rows: list[Any] | None = None,
        *,
        call_class: str = "fast",
    ) -> tuple[list[Any], str]:
        """Run one apply over a page and return (retained rows, log text)."""
        if rows is None:
            rows = self.page()
        buffer = StringIO()
        with redirect_stdout(buffer):
            retained = self.service._collapse_superseded_block_candidates(
                rows,
                timeout_seconds=None,
                call_class=call_class,
            )
        return retained, buffer.getvalue()

    def replay(self, *, enumeration_owed: bool = True) -> tuple[int, str]:
        if enumeration_owed:
            self.server._note_block_replay_enumeration_owed()
        buffer = StringIO()
        with redirect_stdout(buffer):
            queued = self.server.replay_pending_block_candidates()
        return queued, buffer.getvalue()

    def retry_cleanup(self, *, force_due: bool = False) -> tuple[bool, str]:
        """Run one deferred cleanup retry pass on the accounting lane's runner.

        ``force_due`` clears the per-hash backoff deadline so a second and
        later attempt can be driven without sleeping; a freshly deferred
        hash is due immediately and needs it not at all.
        """
        service = self.service
        if force_due:
            with self.server.lock:
                for record in (
                    service._block_candidate_collapse_cleanup_retries.values()
                ):
                    record.not_before_monotonic = 0.0
        buffer = StringIO()
        with redirect_stdout(buffer):
            ran = service._run_one_collapsed_block_candidate_cleanup_retry()
        return ran, buffer.getvalue()

    # -- observation -------------------------------------------------------

    def cleanup_backlog(self) -> dict[str, frozenset[str]]:
        return self.service.collapsed_candidate_cleanup_backlog()

    def pending(self) -> set[str]:
        return {
            str(row["block_hash"]).lower()
            for row in self.ledger.inner.pending_block_candidate_rows(limit=1 << 16)
        }

    def counts(self) -> dict[str, int]:
        return self.service.block_candidate_collapse_snapshot()

    def floor(self) -> dict[int, Any]:
        writer = self.server._ensure_share_writer_service()
        return dict(writer._pending_share_commit_floor)


def _selected(fixture: CollapseFixture, rows: list[Any] | None = None) -> set[str]:
    """The hashes one apply actually removed from the durable outbox."""
    before = fixture.pending()
    fixture.collapse(rows)
    return before - fixture.pending()


class BlockCandidateCollapsePredicateTests(unittest.TestCase):
    """Predicate S, clause by clause, against the shipped durable page."""

    def test_decided_height_sibling_is_selected(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        self.assertEqual(_selected(fixture), {_hash(1), _hash(2)})
        self.assertEqual(fixture.counts()["selected"], 2)
        self.assertEqual(fixture.counts()["abandoned"], 2)

    def test_ownership_markers_alone_do_not_exclude(self) -> None:
        """Outstanding/replay-inflight mean owned, not offered (#183 note)."""
        fixture = CollapseFixture()
        fixture.seed([_hash(1)])
        fixture.server._register_outstanding_block_candidate(_hash(1))
        with fixture.server.lock:
            fixture.service._block_replay_inflight_hashes.add(_hash(1))
        self.assertEqual(_selected(fixture), {_hash(1)})

    def test_attempt_count_alone_does_not_exclude(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1)])
        fixture.ledger.inner.mark_block_candidate_attempted(block_hash=_hash(1))
        row = fixture.ledger.inner._block_candidate_outbox[_hash(1)]
        self.assertEqual(int(row["attempt_count"]), 1)
        self.assertEqual(_selected(fixture), {_hash(1)})

    def test_each_evidence_source_excludes_its_hash(self) -> None:
        target = _hash(1)
        other = _hash(2)

        def flight(fixture: CollapseFixture) -> None:
            lease = fixture.server._claim_block_candidate_disposition(
                target,
                blocking=False,
            )
            self.assertIsNotNone(lease)
            self.addCleanup(
                fixture.server._release_block_candidate_disposition,
                lease,
            )

        def retry_candidate(fixture: CollapseFixture) -> None:
            candidate = fixture.seed([target])[0]
            with fixture.server.lock:
                fixture.service.retry_candidate = candidate

        def waiting_retry(fixture: CollapseFixture) -> None:
            candidate = fixture.seed([target])[0]
            with fixture.server.lock:
                fixture.service._block_disposition_waiting_retries[target] = candidate

        def finalize_retry(fixture: CollapseFixture) -> None:
            with fixture.server.lock:
                fixture.service.finalize_retries[target] = (False, "boom")

        def deferred_retry(fixture: CollapseFixture) -> None:
            candidate = fixture.seed([target])[0]
            with fixture.server.lock:
                fixture.service._block_accounting_deferred_retry_candidate = candidate

        def retained_submission(fixture: CollapseFixture) -> None:
            fixture.service._stash_retained_block_candidate_node_submission(
                target,
                _BlockCandidateNodeSubmission(attempted=True, result=None),
            )

        def tip_observed(fixture: CollapseFixture) -> None:
            fixture.server._register_outstanding_block_candidate(target)
            fixture.server._note_tip_observation_for_candidates(target)

        def accounted(fixture: CollapseFixture) -> None:
            fixture.server._ensure_job_cache_state()
            with fixture.server.lock:
                fixture.server._accounted_accepted_block_hashes.add(target)

        def terminal(fixture: CollapseFixture) -> None:
            fixture.server._record_block_candidate_terminal_outcome(
                target,
                accepted=False,
            )

        sources = {
            "_block_candidate_disposition_flights": flight,
            "retry_candidate": retry_candidate,
            "_block_disposition_waiting_retries": waiting_retry,
            "finalize_retries": finalize_retry,
            "_block_accounting_deferred_retry_candidate": deferred_retry,
            "_block_candidate_retained_node_submissions": retained_submission,
            "_tip_observed_accepted_block_hashes": tip_observed,
            "_accounted_accepted_block_hashes": accounted,
            "_block_candidate_terminal_outcomes": terminal,
        }
        for name, install in sources.items():
            with self.subTest(evidence=name):
                fixture = CollapseFixture()
                fixture.seed([target, other])
                with redirect_stdout(StringIO()):
                    install(fixture)
                self.assertEqual(_selected(fixture), {other})

    def test_pool_block_row_excludes_its_hash(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        fixture.ledger.inner.persist_accepted_block(
            block_hash=_hash(1),
            block_height=DECIDED_HEIGHT,
            parent_hash=STORM_PARENT,
            final_bundle={},
            audit_report={},
        )
        self.assertEqual(_selected(fixture), {_hash(2)})

    def test_missing_pool_block_fact_fails_the_whole_page_closed(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        fixture.ledger.page_hook = lambda rows: [
            {key: value for key, value in row.items() if key != "pool_block_exists"}
            if index == 0
            else row
            for index, row in enumerate(rows)
        ]
        rows = fixture.page()
        retained, _log = fixture.collapse(rows)
        self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
        self.assertEqual(len(retained), len(rows))
        self.assertEqual(fixture.counts()["fail_closed"], 2)
        self.assertEqual(fixture.counts()["selected"], 0)

    def test_a_page_without_the_pool_block_fact_spends_no_chain_read(self) -> None:
        """A compatibility page reader must not cost a tip read per poll."""
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        fixture.ledger.page_hook = lambda rows: [
            {"block_hash": row["block_hash"], "candidate": row["candidate"]}
            for row in rows
        ]
        fixture.collapse()
        self.assertEqual(fixture.rpc.calls, [])
        self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
        self.assertEqual(fixture.counts()["fail_closed"], 2)

    def test_malformed_pool_block_fact_fails_the_whole_page_closed(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        fixture.ledger.page_hook = lambda rows: [
            {**row, "pool_block_exists": "no"} if index == 1 else row
            for index, row in enumerate(rows)
        ]
        fixture.collapse()
        self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
        self.assertEqual(fixture.counts()["fail_closed"], 2)

    def test_parent_equal_to_best_tip_excludes_its_hash(self) -> None:
        fixture = CollapseFixture(
            rpc=CollapseChainRpc(
                tip=OTHER_PARENT,
                tip_height=DECIDED_HEIGHT,
                active={DECIDED_HEIGHT: DECIDED_WINNER},
            )
        )
        fixture.seed([_hash(1)], parent=OTHER_PARENT)
        fixture.seed([_hash(2)], parent=STORM_PARENT)
        self.assertEqual(_selected(fixture), {_hash(2)})

    def test_height_above_the_tip_excludes_its_hash(self) -> None:
        fixture = CollapseFixture(
            rpc=CollapseChainRpc(
                tip=DECIDED_WINNER,
                tip_height=DECIDED_HEIGHT,
                active={DECIDED_HEIGHT: DECIDED_WINNER},
            )
        )
        fixture.seed([_hash(1)], height=DECIDED_HEIGHT + 1)
        fixture.seed([_hash(2)], height=DECIDED_HEIGHT)
        self.assertEqual(_selected(fixture), {_hash(2)})
        # A height above the tip must not even be looked up: getblockhash
        # would fail there and fail the whole page closed.
        self.assertEqual(
            [params for method, params in fixture.rpc.calls if method == "getblockhash"],
            [(DECIDED_HEIGHT,), (DECIDED_HEIGHT,)],
        )

    def test_candidate_that_is_the_active_block_is_never_selected(self) -> None:
        fixture = CollapseFixture(
            rpc=CollapseChainRpc(
                tip=_hash(1),
                tip_height=DECIDED_HEIGHT,
                active={DECIDED_HEIGHT: _hash(1)},
            )
        )
        fixture.seed([_hash(1), _hash(2)])
        self.assertEqual(_selected(fixture), {_hash(2)})

    def test_lower_work_occupant_never_destroys_a_heavier_sibling(self) -> None:
        """Clause 4b: a testnet4 minimum-difficulty occupant is not a decision."""
        fixture = CollapseFixture(
            rpc=CollapseChainRpc(
                tip=DECIDED_WINNER,
                tip_height=DECIDED_HEIGHT,
                active={DECIDED_HEIGHT: DECIDED_WINNER},
                bits={DECIDED_WINNER: MIN_WORK_BITS},
            )
        )
        occupant = _scaled(MIN_WORK_BITS)
        fixture.seed([_hash(1)], network_difficulty=occupant * 8)
        fixture.seed([_hash(2)], network_difficulty=occupant)
        self.assertEqual(_selected(fixture), {_hash(2)})

    def test_equal_work_occupant_still_supersedes(self) -> None:
        fixture = CollapseFixture(
            rpc=CollapseChainRpc(
                tip=DECIDED_WINNER,
                tip_height=DECIDED_HEIGHT,
                active={DECIDED_HEIGHT: DECIDED_WINNER},
                bits={DECIDED_WINNER: RETARGETED_BITS},
            )
        )
        fixture.seed([_hash(1)], network_difficulty=_scaled(RETARGETED_BITS))
        self.assertEqual(_selected(fixture), {_hash(1)})

    def test_header_work_is_read_in_the_candidates_scaled_units(self) -> None:
        """Regression: the row is scaled, ``getblockheader.difficulty`` is raw.

        A candidate stamped with ``scaled_network_difficulty`` and an
        active sibling of exactly equal work must collapse. Reading the
        header's raw ``difficulty`` float instead of re-deriving from its
        bits makes the occupant look COLLAPSE_DIFFICULTY_SCALE times
        weaker, so clause 4b/5 rejects it and a decided height never
        collapses at all.
        """
        for bits in (MIN_WORK_BITS, RETARGETED_BITS, HIGH_WORK_BITS):
            with self.subTest(bits=bits):
                fixture = CollapseFixture(
                    rpc=CollapseChainRpc(
                        tip=DECIDED_WINNER,
                        tip_height=DECIDED_HEIGHT,
                        active={DECIDED_HEIGHT: DECIDED_WINNER},
                        bits={DECIDED_WINNER: bits},
                    )
                )
                scaled = _scaled(bits)
                # The stored fact really is COLLAPSE_DIFFICULTY_SCALE times
                # the float the node reports for the very same header --
                # only approximately so, which is itself why bits and not
                # the float are authoritative here.
                self.assertAlmostEqual(
                    _raw(bits) * COLLAPSE_DIFFICULTY_SCALE / scaled,
                    1.0,
                    places=9,
                )
                fixture.seed([_hash(1)], network_difficulty=scaled)
                self.assertEqual(_selected(fixture), {_hash(1)})

    def test_candidate_heavier_than_the_occupant_is_preserved(self) -> None:
        """One scaled unit of extra work is still a heavier sibling."""
        for bits in (RETARGETED_BITS, HIGH_WORK_BITS):
            with self.subTest(bits=bits):
                fixture = CollapseFixture(
                    rpc=CollapseChainRpc(
                        tip=DECIDED_WINNER,
                        tip_height=DECIDED_HEIGHT,
                        active={DECIDED_HEIGHT: DECIDED_WINNER},
                        bits={DECIDED_WINNER: bits},
                    )
                )
                scaled = _scaled(bits)
                fixture.seed([_hash(1)], network_difficulty=scaled + 1)
                fixture.seed([_hash(2)], network_difficulty=scaled)
                self.assertEqual(_selected(fixture), {_hash(2)})
                self.assertEqual(fixture.pending(), {_hash(1)})

    def test_collapse_reuses_the_production_scaled_difficulty_formula(self) -> None:
        """The restated formula must not drift from ``template_artifacts``."""
        for bits in (
            MIN_WORK_BITS,
            RETARGETED_BITS,
            HIGH_WORK_BITS,
            "1f00ffff",
            "1e00ffff",
            "1c7fffff",
        ):
            with self.subTest(bits=bits):
                self.assertEqual(
                    _collapse_scaled_difficulty(bits),
                    scaled_network_difficulty(bits),
                )

    def test_unusable_header_bits_fails_the_page_closed(self) -> None:
        for value in (
            None,
            "",
            "n/a",
            "not-hex!",
            "1d00ff",
            "1d00ffff00",
            "00000000",
            "00000001",
            0x1D00FFFF,
            True,
            b"1d00ffff",
        ):
            with self.subTest(bits=value):
                fixture = CollapseFixture()
                fixture.seed([_hash(1), _hash(2)])
                fixture.rpc.results["getblockheader"] = {
                    "hash": DECIDED_WINNER,
                    "bits": value,
                    # A usable raw float is present and must not rescue the
                    # read: the scaled comparison has no raw fallback.
                    "difficulty": 1.0,
                }
                fixture.collapse()
                self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
                self.assertEqual(fixture.counts()["fail_closed"], 2)

    def test_header_without_bits_fails_the_page_closed(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        fixture.rpc.results["getblockheader"] = {
            "hash": DECIDED_WINNER,
            "height": DECIDED_HEIGHT,
            "confirmations": 1,
            "difficulty": 1.0,
        }
        fixture.collapse()
        self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
        self.assertEqual(fixture.counts()["fail_closed"], 2)
        self.assertEqual(fixture.ledger.abandon_calls, [])

    def test_each_chain_read_failure_fails_the_page_closed(self) -> None:
        for method in (
            "getbestblockhash",
            "getblockcount",
            "getblockhash",
            "getblockheader",
        ):
            with self.subTest(method=method):
                fixture = CollapseFixture()
                fixture.seed([_hash(1), _hash(2)])
                fixture.rpc.failures.add(method)
                retained, _log = fixture.collapse()
                self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
                self.assertEqual(len(retained), 2)
                self.assertEqual(fixture.counts()["fail_closed"], 2)
                self.assertEqual(fixture.counts()["selected"], 0)
                self.assertEqual(fixture.ledger.abandon_calls, [])

    def test_unknown_chain_read_results_fail_the_page_closed(self) -> None:
        for method, value in (
            ("getbestblockhash", None),
            ("getbestblockhash", "not-a-hash"),
            ("getblockcount", "abc"),
            ("getblockcount", True),
            ("getblockhash", ""),
            ("getblockheader", "not-an-object"),
        ):
            with self.subTest(method=method, value=value):
                fixture = CollapseFixture()
                fixture.seed([_hash(1)])
                fixture.rpc.results[method] = value
                fixture.collapse()
                self.assertEqual(fixture.pending(), {_hash(1)})
                self.assertGreaterEqual(fixture.counts()["fail_closed"], 1)

    def test_malformed_intent_fails_the_whole_page_closed(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])

        def corrupt(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            patched = []
            for row in rows:
                if str(row["block_hash"]).lower() == _hash(1):
                    intent = dict(row["candidate"])
                    intent["expected_height"] = "not-a-height"
                    row = {**row, "candidate": intent}
                patched.append(row)
            return patched

        fixture.ledger.page_hook = corrupt
        self.assertEqual(_selected(fixture), set())
        self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
        self.assertEqual(fixture.counts()["fail_closed"], 2)
        self.assertEqual(fixture.ledger.abandon_calls, [])

    def test_intent_disagreeing_with_template_fails_the_page_closed(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])

        def corrupt(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            patched = []
            for row in rows:
                if str(row["block_hash"]).lower() == _hash(1):
                    intent = dict(row["candidate"])
                    intent["template"] = {
                        **intent["template"],
                        "height": DECIDED_HEIGHT + 5,
                    }
                    row = {**row, "candidate": intent}
                patched.append(row)
            return patched

        fixture.ledger.page_hook = corrupt
        self.assertEqual(_selected(fixture), set())
        self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
        self.assertEqual(fixture.counts()["fail_closed"], 2)

    def test_unusable_candidate_difficulty_fails_the_page_closed(self) -> None:
        for value in (None, "n/a", float("nan"), True, float("inf"), 0, -1):
            with self.subTest(difficulty=value):
                fixture = CollapseFixture()
                fixture.seed([_hash(1), _hash(2)])

                def corrupt(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
                    intent = dict(rows[0]["candidate"])
                    intent["found_block"] = {
                        **intent["found_block"],
                        "network_difficulty": value,
                    }
                    rows[0] = {**rows[0], "candidate": intent}
                    return rows

                fixture.ledger.page_hook = corrupt
                self.assertEqual(_selected(fixture), set())
                self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
                self.assertEqual(fixture.counts()["fail_closed"], 2)

    def test_chain_is_read_once_per_page_and_once_per_distinct_height(self) -> None:
        fixture = CollapseFixture(
            rpc=CollapseChainRpc(
                tip=DECIDED_WINNER,
                tip_height=DECIDED_HEIGHT,
                active={
                    DECIDED_HEIGHT: DECIDED_WINNER,
                    DECIDED_HEIGHT - 1: "ee" * 32,
                },
            )
        )
        fixture.seed([_hash(index) for index in range(1, 51)])
        fixture.seed(
            [_hash(index) for index in range(51, 101)],
            height=DECIDED_HEIGHT - 1,
        )
        fixture.collapse()
        counts = fixture.rpc.counts()
        # One tip read for the page plus one for the pre-write revalidation;
        # one active-block read per distinct height in each pass; the
        # occupant header only in the selection pass, because revalidation
        # found both occupants unchanged.
        self.assertEqual(counts["getbestblockhash"], 2)
        self.assertEqual(counts["getblockcount"], 1)
        self.assertEqual(counts["getblockhash"], 4)
        self.assertEqual(counts["getblockheader"], 2)
        self.assertEqual(fixture.counts()["abandoned"], 100)


class BlockCandidateCollapseApplyTests(unittest.TestCase):
    """Lease claiming, revalidation, the single write, and its result set."""

    def test_lease_is_claimed_without_blocking(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        claims: list[bool] = []
        original = fixture.server._claim_block_candidate_disposition

        def record(block_hash: str, *, blocking: bool):
            claims.append(blocking)
            return original(block_hash, blocking=blocking)

        fixture.server._claim_block_candidate_disposition = record
        fixture.collapse()
        self.assertTrue(claims)
        self.assertEqual(set(claims), {False})

    def test_unclaimable_lease_is_skipped_not_waited_on(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        original = fixture.server._claim_block_candidate_disposition

        def refuse(block_hash: str, *, blocking: bool):
            if block_hash == _hash(1):
                return None
            return original(block_hash, blocking=blocking)

        fixture.server._claim_block_candidate_disposition = refuse
        self.assertEqual(_selected(fixture), {_hash(2)})
        self.assertEqual(fixture.counts()["lease_skipped"], 1)
        self.assertEqual(fixture.ledger.abandon_calls[0][0], (_hash(2),))

    def test_apply_leases_do_not_disqualify_their_own_rows(self) -> None:
        """The apply's own flights must not read back as busy evidence."""
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        seen: list[frozenset[str]] = []
        original = fixture.service._block_candidate_collapse_evidence

        def record(hashes, *, ignore_leases=frozenset()):
            seen.append(frozenset(ignore_leases))
            return original(hashes, ignore_leases=ignore_leases)

        fixture.service._block_candidate_collapse_evidence = record
        self.assertEqual(_selected(fixture), {_hash(1), _hash(2)})
        self.assertEqual(seen[0], frozenset())
        self.assertEqual(seen[1], frozenset({_hash(1), _hash(2)}))

    def test_every_lease_is_released_after_the_apply(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        fixture.collapse()
        with fixture.service._block_candidate_disposition_registry_lock:
            self.assertEqual(fixture.service._block_candidate_disposition_flights, {})

    def test_leases_are_released_when_the_write_fails(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1)])

        def explode(hashes: tuple[str, ...], error: str):
            raise RuntimeError("ledger down")

        fixture.ledger.abandon_hook = explode
        fixture.collapse()
        with fixture.service._block_candidate_disposition_registry_lock:
            self.assertEqual(fixture.service._block_candidate_disposition_flights, {})
        self.assertEqual(fixture.pending(), {_hash(1)})
        self.assertEqual(fixture.counts()["fail_closed"], 1)

    def test_leases_are_released_when_cleanup_fails(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1)])
        fixture.server._clear_accepted_block_payout_preview = (
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("nope"))
        )
        fixture.collapse()
        with fixture.service._block_candidate_disposition_registry_lock:
            self.assertEqual(fixture.service._block_candidate_disposition_flights, {})
        self.assertEqual(fixture.counts()["cleanup_failed"], 1)

    def test_revalidation_drops_a_candidate_that_became_the_active_block(self) -> None:
        """The immediate pre-write re-read closes the select-to-write gap."""
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        state = {"selected": False}
        rpc = fixture.rpc

        def move_chain(chain: CollapseChainRpc, method: str, params) -> None:
            if method != "getbestblockhash":
                return
            if state["selected"]:
                # Second best-tip read: the revalidation. Candidate 1 was
                # offered, accepted, and is now the active block.
                chain.tip = _hash(1)
                chain.active = {DECIDED_HEIGHT: _hash(1)}
            state["selected"] = True

        rpc.before_call = move_chain
        self.assertEqual(_selected(fixture), {_hash(2)})
        self.assertEqual(fixture.counts()["revalidation_dropped"], 1)
        self.assertEqual(fixture.ledger.abandon_calls[0][0], (_hash(2),))

    def test_revalidation_drops_a_candidate_whose_parent_became_the_tip(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1)], parent=STORM_PARENT)
        fixture.seed([_hash(2)], parent=OTHER_PARENT)
        state = {"seen": 0}

        def move_chain(chain: CollapseChainRpc, method: str, params) -> None:
            if method != "getbestblockhash":
                return
            state["seen"] += 1
            if state["seen"] == 2:
                chain.tip = OTHER_PARENT

        fixture.rpc.before_call = move_chain
        self.assertEqual(_selected(fixture), {_hash(1)})
        self.assertEqual(fixture.counts()["revalidation_dropped"], 1)

    def test_revalidation_reads_a_changed_occupants_work(self) -> None:
        """A lighter replacement occupant drops the row, in scaled units."""
        fixture = CollapseFixture()
        fixture.seed([_hash(1)], network_difficulty=_scaled(RETARGETED_BITS))
        fixture.rpc.bits[DECIDED_WINNER] = RETARGETED_BITS
        fixture.rpc.bits["ee" * 32] = MIN_WORK_BITS
        state = {"seen": 0}

        def move_chain(chain: CollapseChainRpc, method: str, params) -> None:
            if method != "getbestblockhash":
                return
            state["seen"] += 1
            if state["seen"] == 2:
                chain.tip = "ee" * 32
                chain.active = {DECIDED_HEIGHT: "ee" * 32}

        fixture.rpc.before_call = move_chain
        self.assertEqual(_selected(fixture), set())
        self.assertEqual(fixture.counts()["revalidation_dropped"], 1)
        headers = [
            params for method, params in fixture.rpc.calls if method == "getblockheader"
        ]
        self.assertEqual(headers, [(DECIDED_WINNER,), ("ee" * 32,)])

    def test_revalidation_keeps_an_equal_work_changed_occupant(self) -> None:
        """The changed-occupant re-read normalizes exactly as selection does.

        The replacement occupant carries different bits from the one
        selection read but exactly the candidate's scaled work, so the row
        still collapses. A revalidation that compared the replacement's raw
        header float against the row's scaled value would drop it instead.
        """
        fixture = CollapseFixture()
        fixture.seed([_hash(1)], network_difficulty=_scaled(HIGH_WORK_BITS))
        fixture.rpc.bits[DECIDED_WINNER] = HIGH_WORK_BITS
        fixture.rpc.bits["ee" * 32] = HIGH_WORK_BITS
        state = {"seen": 0}

        def move_chain(chain: CollapseChainRpc, method: str, params) -> None:
            if method != "getbestblockhash":
                return
            state["seen"] += 1
            if state["seen"] == 2:
                chain.active = {DECIDED_HEIGHT: "ee" * 32}

        fixture.rpc.before_call = move_chain
        self.assertEqual(_selected(fixture), {_hash(1)})
        self.assertEqual(fixture.counts()["revalidation_dropped"], 0)
        headers = [
            params for method, params in fixture.rpc.calls if method == "getblockheader"
        ]
        self.assertEqual(headers, [(DECIDED_WINNER,), ("ee" * 32,)])

    def test_revalidation_of_a_changed_occupant_fails_closed_without_bits(
        self,
    ) -> None:
        """The pre-write header re-read has no raw-difficulty fallback either."""
        fixture = CollapseFixture()
        fixture.seed([_hash(1)], network_difficulty=_scaled(RETARGETED_BITS))
        fixture.rpc.bits[DECIDED_WINNER] = RETARGETED_BITS
        state = {"seen": 0}

        def move_chain(chain: CollapseChainRpc, method: str, params) -> None:
            if method != "getbestblockhash":
                return
            state["seen"] += 1
            if state["seen"] == 2:
                chain.active = {DECIDED_HEIGHT: "ee" * 32}
                chain.results["getblockheader"] = {
                    "hash": "ee" * 32,
                    "difficulty": _raw(RETARGETED_BITS),
                }

        fixture.rpc.before_call = move_chain
        fixture.collapse()
        self.assertEqual(fixture.pending(), {_hash(1)})
        self.assertEqual(fixture.counts()["fail_closed"], 1)
        self.assertEqual(fixture.ledger.abandon_calls, [])

    def test_revalidation_failure_fails_the_page_closed(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        state = {"seen": 0}

        def fail_second(chain: CollapseChainRpc, method: str, params) -> None:
            if method != "getbestblockhash":
                return
            state["seen"] += 1
            if state["seen"] == 2:
                chain.failures.add("getbestblockhash")

        fixture.rpc.before_call = fail_second
        retained, _log = fixture.collapse()
        self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
        self.assertEqual(len(retained), 2)
        self.assertEqual(fixture.ledger.abandon_calls, [])
        self.assertEqual(fixture.counts()["fail_closed"], 2)

    def test_one_batch_write_replaces_the_per_row_writes(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(index) for index in range(1, 21)])
        fixture.collapse()
        self.assertEqual(len(fixture.ledger.abandon_calls), 1)
        requested, error = fixture.ledger.abandon_calls[0]
        self.assertEqual(len(requested), 20)
        self.assertIn("tip moved before submit", error)
        self.assertIn(DECIDED_WINNER, error)

    def test_cleanup_follows_the_returned_set_not_the_request(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2), _hash(3)])
        service = fixture.service

        def partial(hashes: tuple[str, ...], error: str) -> tuple[str, ...]:
            won = tuple(value for value in hashes if value != _hash(2))
            return fixture.ledger.inner.mark_block_candidates_abandoned(
                block_hashes=won,
                error=error,
            )

        fixture.ledger.abandon_hook = partial
        retained, _log = fixture.collapse()
        self.assertEqual(fixture.pending(), {_hash(2)})
        self.assertEqual(fixture.counts()["abandoned"], 2)
        self.assertEqual(fixture.counts()["write_lost"], 1)
        with fixture.server.lock:
            terminal = dict(service._block_candidate_terminal_outcomes)
        self.assertEqual(set(terminal), {_hash(1), _hash(3)})
        self.assertNotIn(_hash(2), terminal)
        self.assertEqual(
            {str(row["block_hash"]).lower() for row in retained},
            {_hash(2)},
        )

    def test_a_hash_outside_the_request_is_never_cleaned_up(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1)])
        stranger = _hash(99)

        def stray(hashes: tuple[str, ...], error: str) -> tuple[str, ...]:
            fixture.ledger.inner.mark_block_candidates_abandoned(
                block_hashes=hashes,
                error=error,
            )
            return hashes + (stranger,)

        fixture.ledger.abandon_hook = stray
        fixture.collapse()
        with fixture.server.lock:
            terminal = dict(fixture.service._block_candidate_terminal_outcomes)
        self.assertEqual(set(terminal), {_hash(1)})

    def test_an_empty_returned_set_preserves_the_whole_page(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        fixture.ledger.abandon_hook = lambda hashes, error: ()
        rows = fixture.page()
        retained, _log = fixture.collapse(rows)
        self.assertEqual(len(retained), 2)
        self.assertEqual(fixture.counts()["abandoned"], 0)
        self.assertEqual(fixture.counts()["write_lost"], 2)
        with fixture.server.lock:
            self.assertEqual(fixture.service._block_candidate_terminal_outcomes, {})

    def test_a_non_hash_set_result_fails_the_page_closed(self) -> None:
        """A count or a boolean says nothing about which rows were won."""
        for value in (True, 2, None, "01" * 32):
            with self.subTest(returned=value):
                fixture = CollapseFixture()
                fixture.seed([_hash(1), _hash(2)])

                def answer(hashes: tuple[str, ...], error: str, value=value):
                    fixture.ledger.inner.mark_block_candidates_abandoned(
                        block_hashes=hashes,
                        error=error,
                    )
                    return value

                fixture.ledger.abandon_hook = answer
                rows = fixture.page()
                retained, _log = fixture.collapse(rows)
                self.assertEqual(len(retained), 2)
                self.assertEqual(fixture.counts()["abandoned"], 0)
                self.assertEqual(fixture.counts()["fail_closed"], 2)
                with fixture.server.lock:
                    self.assertEqual(
                        fixture.service._block_candidate_terminal_outcomes,
                        {},
                    )

    def test_no_write_is_issued_for_an_empty_selection(self) -> None:
        fixture = CollapseFixture(
            rpc=CollapseChainRpc(tip=STORM_PARENT, tip_height=DECIDED_HEIGHT)
        )
        fixture.seed([_hash(1)])
        fixture.collapse()
        self.assertEqual(fixture.ledger.abandon_calls, [])
        self.assertEqual(fixture.pending(), {_hash(1)})

    def test_a_ledger_without_the_batch_method_is_left_entirely_alone(self) -> None:
        fixture = CollapseFixture(ledger=NoBatchCollapseLedger())
        fixture.seed([_hash(1)])
        rows = fixture.page()
        retained, _log = fixture.collapse(rows)
        self.assertEqual(retained, rows)
        self.assertEqual(fixture.rpc.calls, [])
        self.assertEqual(fixture.counts()["considered"], 0)
        self.assertEqual(fixture.pending(), {_hash(1)})


class BlockCandidateCollapseCleanupTests(unittest.TestCase):
    """Cleanup list M: everything terminal per-row abandonment releases."""

    def _fixture_with_state(self) -> tuple[CollapseFixture, str]:
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.seed([target])
        service = fixture.service
        server = fixture.server
        server._register_outstanding_block_candidate(target)
        server._begin_accepted_block_payout_preview(
            target,
            block_height=DECIDED_HEIGHT,
        )
        server._mark_accepted_block_payout_landed(
            target,
            block_height=DECIDED_HEIGHT,
        )
        with server.lock:
            service._block_fast_lane_reservations.add(target)
            service._block_replay_inflight_hashes.add(target)
            service.retry_delays[target] = 4.0
            # Both registries are created on first touch by the pacing and
            # landing-timeout paths; seed them the way those paths would.
            service._block_candidate_retry_not_before = {target: 1.0}
            service._block_landing_timeout_counts = {target: 3}
        return fixture, target

    def test_cleanup_releases_every_owned_state(self) -> None:
        fixture, target = self._fixture_with_state()
        service = fixture.service
        server = fixture.server
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertEqual(fixture.pending(), set())
        with server.lock:
            self.assertIs(service._block_candidate_terminal_outcomes[target], False)
            self.assertNotIn(target, service._block_fast_lane_reservations)
            self.assertNotIn(target, service._block_replay_inflight_hashes)
            self.assertNotIn(target, service._outstanding_block_candidate_hashes)
            self.assertNotIn(target, service._tip_observed_accepted_block_hashes)
            self.assertNotIn(target, service.retry_delays)
            self.assertNotIn(target, service._block_candidate_retry_not_before)
            self.assertNotIn(target, service._block_landing_timeout_counts)
            self.assertNotIn(target, service.finalize_retries)
        with server._accepted_block_payout_preview_condition:
            self.assertNotIn(target, server._accepted_block_payout_previews)
            self.assertNotIn(
                target,
                server._invalidated_accepted_block_payout_previews,
            )
        self.assertEqual(
            server.block_candidate_abandoned_counts,
            {PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON: 1},
        )
        self.assertEqual(
            server.stale_job_abandon_counts[
                PRISM_BLOCK_CANDIDATE_COLLAPSE_STALE_JOB_CLASS
            ],
            1,
        )

    def test_cleanup_leaves_no_payout_tombstone_behind(self) -> None:
        fixture, target = self._fixture_with_state()
        with redirect_stdout(StringIO()):
            fixture.collapse()
        with fixture.server._accepted_block_payout_preview_condition:
            self.assertEqual(
                fixture.server._invalidated_accepted_block_payout_previews,
                {},
            )

    def test_finalize_retry_state_is_removed_by_cleanup(self) -> None:
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.seed([target])
        with fixture.server.lock:
            fixture.service.finalize_retries[target] = (False, "stale")
        with redirect_stdout(StringIO()):
            fixture.service._clean_up_collapsed_block_candidates((target,))
        with fixture.server.lock:
            self.assertEqual(fixture.service.finalize_retries, {})

    def test_abandonment_is_counted_exactly_once(self) -> None:
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.seed([target])
        with redirect_stdout(StringIO()):
            fixture.service._clean_up_collapsed_block_candidates((target,))
            fixture.service._clean_up_collapsed_block_candidates((target,))
        self.assertEqual(
            fixture.server.block_candidate_abandoned_counts,
            {PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON: 1},
        )
        self.assertEqual(
            fixture.server.stale_job_abandon_counts[
                PRISM_BLOCK_CANDIDATE_COLLAPSE_STALE_JOB_CLASS
            ],
            1,
        )

    def test_cleanup_is_idempotent(self) -> None:
        fixture, target = self._fixture_with_state()
        with redirect_stdout(StringIO()):
            fixture.service._clean_up_collapsed_block_candidates((target,))
            fixture.service._clean_up_collapsed_block_candidates((target,))
        self.assertEqual(fixture.counts()["cleanup_failed"], 0)

    def test_one_failing_cleanup_does_not_strand_later_steps_or_rows(self) -> None:
        fixture = CollapseFixture()
        hashes = [_hash(1), _hash(2), _hash(3)]
        candidates = fixture.seed(hashes, credit_share_on_accept=True)
        writer = fixture.server._ensure_share_writer_service()
        writer.adopt_pending_share(candidates[1].pending_share)
        fixture.service.candidate_queue.put_nowait(candidates[1])
        original = fixture.server._clear_block_candidate_retry_state

        def explode(block_hash: str) -> None:
            if block_hash == _hash(2):
                raise RuntimeError("cleanup boom")
            original(block_hash)

        fixture.server._clear_block_candidate_retry_state = explode
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertEqual(fixture.pending(), set())
        with fixture.server.lock:
            terminal = dict(fixture.service._block_candidate_terminal_outcomes)
        self.assertEqual(set(terminal), set(hashes))
        self.assertNotIn(id(candidates[1].pending_share), fixture.floor())
        self.assertEqual(fixture.counts()["cleanup_failed"], 1)
        self.assertEqual(fixture.counts()["abandoned"], 3)

    def test_systemic_cleanup_failure_logging_stays_bounded(self) -> None:
        fixture = CollapseFixture()
        hashes = [_hash(index) for index in range(1, 41)]
        fixture.seed(hashes)
        fixture.server._clear_block_candidate_retry_state = (
            lambda block_hash: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        _retained, log = fixture.collapse()
        detailed = [
            line
            for line in log.splitlines()
            if "cleanup failed step=" in line
        ]
        self.assertEqual(len(detailed), BLOCK_CANDIDATE_COLLAPSE_LOG_FAILURES)
        self.assertIn("cleanup failed rows=40 of 40", log)
        self.assertEqual(fixture.counts()["cleanup_failed"], 40)

    def test_an_aborted_cleanup_still_partitions_the_terminal_rows(self) -> None:
        """A terminal row must never be replay-adopted, cleanup or not."""
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        fixture.service._collapsed_candidate_floor_holders = (
            lambda abandoned: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        rows = fixture.page()
        retained, log = fixture.collapse(rows)
        self.assertEqual(retained, [])
        self.assertEqual(fixture.pending(), set())
        self.assertIn("cleanup aborted rows=2", log)
        self.assertEqual(fixture.counts()["cleanup_failed"], 2)

    def test_a_log_failure_does_not_double_count_cleanup_failures(self) -> None:
        """cleanup_failed counts affected hashes, not diagnostic failures.

        A contained per-step failure belongs to the one hash it happened to,
        while a later summary formatter fault does not change any hash's
        cleanup state. Treating both as cleanup reported four affected hashes
        for a three-row page even though only one cleanup actually failed.
        """
        fixture = CollapseFixture()
        hashes = [_hash(1), _hash(2), _hash(3)]
        fixture.seed(hashes)
        original = fixture.server._clear_block_candidate_retry_state

        def explode(block_hash: str) -> None:
            if block_hash == _hash(2):
                raise RuntimeError("cleanup boom")
            original(block_hash)

        fixture.server._clear_block_candidate_retry_state = explode
        # The grouped formatter runs only after every cleanup step.
        fixture.service._log_collapsed_block_candidates = (
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("log boom"))
        )
        rows = fixture.page()
        retained, log = fixture.collapse(rows)
        counts = fixture.counts()
        self.assertEqual(counts["abandoned"], 3)
        self.assertEqual(counts["cleanup_failed"], 1)
        self.assertLessEqual(counts["cleanup_failed"], counts["abandoned"])
        # Both faults are visible, every cleanup attempt still ran, and the
        # terminal rows are still partitioned out of the page.
        self.assertIn("cleanup failed rows=1 of 3", log)
        self.assertIn("logging failed rows=3", log)
        self.assertNotIn("cleanup aborted rows=3", log)
        self.assertEqual(retained, [])
        self.assertEqual(fixture.pending(), set())
        with fixture.server.lock:
            self.assertEqual(
                set(fixture.service._block_candidate_terminal_outcomes),
                set(hashes),
            )
        with fixture.service._block_candidate_disposition_registry_lock:
            self.assertEqual(fixture.service._block_candidate_disposition_flights, {})

    def test_a_queued_candidates_floor_holder_is_released(self) -> None:
        fixture = CollapseFixture()
        target = _hash(1)
        candidate = fixture.seed([target], credit_share_on_accept=True)[0]
        writer = fixture.server._ensure_share_writer_service()
        writer.adopt_pending_share(candidate.pending_share)
        fixture.service.candidate_queue.put_nowait(candidate)
        self.assertIn(id(candidate.pending_share), fixture.floor())
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertNotIn(id(candidate.pending_share), fixture.floor())

    def test_a_replay_queued_candidates_floor_holder_is_released(self) -> None:
        fixture = CollapseFixture()
        target = _hash(1)
        candidate = fixture.seed([target], credit_share_on_accept=True)[0]
        writer = fixture.server._ensure_share_writer_service()
        writer.adopt_pending_share(candidate.pending_share)
        fixture.service._block_replay_candidate_queue.put_nowait(candidate)
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertNotIn(id(candidate.pending_share), fixture.floor())

    def test_another_candidates_floor_holder_is_never_released(self) -> None:
        fixture = CollapseFixture()
        collapsed = fixture.seed([_hash(1)], credit_share_on_accept=True)[0]
        # A same-hash duplicate object and an unrelated hash both keep their
        # own identity-keyed holders; only the collapsed object's is dropped.
        duplicate = fixture.seed(
            [_hash(1)],
            credit_share_on_accept=True,
        )[0]
        untouched = fixture.seed(
            [_hash(2)],
            parent=OTHER_PARENT,
            credit_share_on_accept=True,
        )[0]
        writer = fixture.server._ensure_share_writer_service()
        for candidate in (collapsed, duplicate, untouched):
            writer.adopt_pending_share(candidate.pending_share)
        fixture.service.candidate_queue.put_nowait(collapsed)
        fixture.rpc.tip = OTHER_PARENT
        with redirect_stdout(StringIO()):
            fixture.collapse()
        floor = fixture.floor()
        self.assertNotIn(id(collapsed.pending_share), floor)
        self.assertIn(id(duplicate.pending_share), floor)
        self.assertIn(id(untouched.pending_share), floor)

    def test_no_floor_holder_is_invented_for_an_unadopted_row(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1)], credit_share_on_accept=True)
        before = set(fixture.floor())
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertEqual(set(fixture.floor()), before)

    def test_cleanup_is_safe_for_an_already_terminal_hash(self) -> None:
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.seed([target])
        fixture.server._record_block_candidate_terminal_outcome(
            target,
            accepted=True,
        )
        with redirect_stdout(StringIO()):
            fixture.service._clean_up_collapsed_block_candidates((target,))
        with fixture.server.lock:
            self.assertIs(
                fixture.service._block_candidate_terminal_outcomes[target],
                False,
            )
        self.assertEqual(fixture.counts()["cleanup_failed"], 0)

    def test_collapse_reason_matches_the_per_row_stale_job_reason(self) -> None:
        self.assertEqual(
            PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON,
            PRISM_REJECTION_STALE_JOB,
        )
        self.assertIn(
            PRISM_BLOCK_CANDIDATE_COLLAPSE_STALE_JOB_CLASS,
            PRISM_STALE_JOB_ABANDON_CLASSES,
        )


def _break_cleanup_step(target: Any, name: str, *, only: str | None = None) -> dict:
    """Make one cleanup step raise until the returned switch is flipped."""
    original = getattr(target, name)
    broken = {"on": True}

    def failing(block_hash: str, *args: Any, **kwargs: Any) -> Any:
        if broken["on"] and (only is None or block_hash == only):
            raise RuntimeError(f"{name} boom")
        return original(block_hash, *args, **kwargs)

    setattr(target, name, failing)
    return broken


class AccountingLaneRig:
    """Drive the shipped accounting loop against controllable lane traffic.

    Only the two sinks the loop reaches through the coordinator are replaced
    -- the ordinary accounting task runner and the invalid-candidate
    quarantine drain -- so the loop body, its cleanup-retry cadence, and the
    real deferred cleanup all stay exactly as shipped.  ``mode`` chooses
    which lane is kept permanently busy:

    ``accounting``
        Each finished task re-arms the primary handoff queue before it
        returns, so the loop's next queue read can never see an empty lane.
    ``quarantine``
        Both handoff queues stay empty and the quarantine drain always
        reports an item, so the loop never reaches its idle branch either.
    ``both``
        The two alternate: each quarantine item re-arms the accounting
        queue, so one work item of each kind completes per pair.
    ``idle``
        No traffic at all, so the loop falls straight through to the idle
        branch that predates the cadence.

    In every busy mode the idle branch is unreachable by construction, which
    is what makes these fixtures a starvation test rather than a race.
    """

    ACCOUNTING = "accounting"
    QUARANTINE = "quarantine"
    BOTH = "both"
    IDLE = "idle"

    def __init__(self, fixture: CollapseFixture, mode: str) -> None:
        self.fixture = fixture
        self.server = fixture.server
        self.service = fixture.service
        self.mode = mode
        self.accounting_items = 0
        self.quarantine_items = 0
        # (accounting items, quarantine items) completed at each offer.
        self.offers: list[tuple[int, int]] = []
        self.attempts = 0
        self.log = ""
        self.thread_alive = False
        self._sequence = 0
        self.service._ensure_block_accounting_state()
        self.service._ensure_block_replay_state()
        self.server._run_block_accounting_task = self._run_task
        self.server._run_one_invalid_block_candidate_quarantine = self._run_quarantine
        shipped = self.service._run_one_collapsed_block_candidate_cleanup_retry

        def offer() -> bool:
            self.offers.append((self.accounting_items, self.quarantine_items))
            ran = shipped()
            if ran:
                self.attempts += 1
            return ran

        self.service._run_one_collapsed_block_candidate_cleanup_retry = offer
        if mode in (self.ACCOUNTING, self.BOTH):
            self._arm_accounting()

    @property
    def work_items(self) -> int:
        return self.accounting_items + self.quarantine_items

    def _arm_accounting(self) -> None:
        self._sequence += 1
        self.service._block_accounting_queue.put_nowait(
            (0, self._sequence, SimpleNamespace())
        )

    def _run_task(self, task: Any) -> None:
        self.accounting_items += 1
        if self.mode == self.ACCOUNTING:
            self._arm_accounting()

    def _run_quarantine(self) -> bool:
        if self.mode not in (self.QUARANTINE, self.BOTH):
            return False
        self.quarantine_items += 1
        if self.mode == self.BOTH:
            self._arm_accounting()
        return True

    def run_until(self, predicate: Any, *, timeout: float = 10.0) -> bool:
        """Run the lane until ``predicate`` holds or the deadline expires.

        Bounded on purpose: a starved retry must end the test with a report,
        not with an unbounded wait.  Counters are only read by callers after
        the loop thread has been stopped and joined, so every assertion sees
        one consistent snapshot.
        """
        thread = threading.Thread(
            target=self.service.block_accounting_loop,
            daemon=True,
        )
        buffer = StringIO()
        with redirect_stdout(buffer):
            thread.start()
            try:
                deadline = time.monotonic() + timeout
                while not predicate() and time.monotonic() < deadline:
                    time.sleep(0.002)
            finally:
                self.server.stop_event.set()
                thread.join(timeout=timeout)
        self.log = buffer.getvalue()
        self.thread_alive = thread.is_alive()
        return predicate()


class BlockCandidateCollapseCleanupRetryTests(unittest.TestCase):
    """A failed cleanup is the only state with no durable replay source.

    Once the fenced batch write returns a hash, that row is terminal: it is
    partitioned out of the replay page and no later enumeration can hand it
    back.  A cleanup step that failed therefore has nothing left to retry
    it, and whatever it did not tear down -- a payout preview or its
    tombstone, a pending-share floor holder, an outstanding-hash marker --
    would stay installed for the process lifetime.  These tests own the
    bounded, idempotent retry that closes that gap, and the accounting of
    it: the affected-hash series still counts hashes, never attempts.
    """

    @staticmethod
    def _breaker(target: Any, name: str, *, only: str | None = None) -> dict:
        """Make one cleanup step raise until the returned switch is flipped."""
        return _break_cleanup_step(target, name, only=only)

    # -- payout preview and tombstone --------------------------------------

    def test_a_failed_payout_withdrawal_is_retried_until_it_recovers(self) -> None:
        """The wait storm the collapse exists to end must not be re-armed."""
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.seed([target])
        server = fixture.server
        server._begin_accepted_block_payout_preview(
            target,
            block_height=DECIDED_HEIGHT,
        )
        server._mark_accepted_block_payout_landed(
            target,
            block_height=DECIDED_HEIGHT,
        )
        broken = self._breaker(server, "_clear_accepted_block_payout_preview")
        with redirect_stdout(StringIO()):
            fixture.collapse()
        # The durable row is terminal and gone from every replay source
        # while the landed barrier it armed is still installed: descendants
        # keep waiting on a transition nothing else will ever clear.
        self.assertEqual(fixture.pending(), set())
        self.assertTrue(server._accepted_block_payout_transition_landed(target))
        self.assertEqual(fixture.counts()["cleanup_failed"], 1)
        self.assertEqual(
            fixture.cleanup_backlog(),
            {target: frozenset({BLOCK_CANDIDATE_COLLAPSE_CLEANUP_PAYOUT_STEP})},
        )
        broken["on"] = False
        ran, log = fixture.retry_cleanup()
        self.assertTrue(ran)
        with server._accepted_block_payout_preview_condition:
            self.assertNotIn(target, server._accepted_block_payout_previews)
            self.assertNotIn(
                target,
                server._invalidated_accepted_block_payout_previews,
            )
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertIn("cleanup recovered", log)
        counts = fixture.counts()
        self.assertEqual(counts["cleanup_recovered"], 1)
        # The affected-hash series counts the hash once, whatever it took.
        self.assertEqual(counts["cleanup_failed"], 1)

    def test_a_failed_tombstone_drop_is_retried(self) -> None:
        """A retained tombstone fails descendants closed just as hard."""
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.seed([target])
        server = fixture.server
        server._begin_accepted_block_payout_preview(
            target,
            block_height=DECIDED_HEIGHT,
        )
        server._mark_accepted_block_payout_landed(
            target,
            block_height=DECIDED_HEIGHT,
        )
        original = server._clear_accepted_block_payout_preview
        broken = {"on": True}

        def clear(block_hash: str, *, invalidate_published: bool = False) -> None:
            # The withdrawal lands and installs the tombstone; only the
            # second half -- the drop -- fails.
            if broken["on"] and not invalidate_published:
                raise RuntimeError("tombstone drop boom")
            original(block_hash, invalidate_published=invalidate_published)

        server._clear_accepted_block_payout_preview = clear
        with redirect_stdout(StringIO()):
            fixture.collapse()
        with server._accepted_block_payout_preview_condition:
            self.assertIn(
                target,
                server._invalidated_accepted_block_payout_previews,
            )
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup()[0])
        with server._accepted_block_payout_preview_condition:
            self.assertEqual(server._invalidated_accepted_block_payout_previews, {})
        self.assertEqual(fixture.cleanup_backlog(), {})

    # -- pending-share floor -----------------------------------------------

    def test_a_failed_floor_release_survives_the_queue_it_was_read_from(self) -> None:
        """The floor keys holders by identity, so the retry must carry them."""
        fixture = CollapseFixture()
        target = _hash(1)
        candidate = fixture.seed([target], credit_share_on_accept=True)[0]
        server = fixture.server
        server._ensure_share_writer_service().adopt_pending_share(
            candidate.pending_share
        )
        fixture.service.candidate_queue.put_nowait(candidate)
        original = server._finish_pending_share_commit
        broken = {"on": True}

        def finish(pending_share: Any) -> None:
            if broken["on"]:
                raise RuntimeError("floor release boom")
            original(pending_share)

        server._finish_pending_share_commit = finish
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertIn(id(candidate.pending_share), fixture.floor())
        self.assertEqual(
            fixture.cleanup_backlog(),
            {target: frozenset({BLOCK_CANDIDATE_COLLAPSE_CLEANUP_FLOOR_STEP})},
        )
        # The queue the holder was indexed from drains before the retry
        # runs, so a fresh scan would find nothing to release.
        self.assertIs(fixture.service.candidate_queue.get_nowait(), candidate)
        self.assertEqual(
            fixture.service._collapsed_candidate_floor_holders((target,)),
            {},
        )
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup()[0])
        self.assertNotIn(id(candidate.pending_share), fixture.floor())
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertEqual(fixture.counts()["cleanup_recovered"], 1)

    def test_another_candidates_floor_holder_is_never_released_by_a_retry(
        self,
    ) -> None:
        fixture = CollapseFixture()
        collapsed = fixture.seed([_hash(1)], credit_share_on_accept=True)[0]
        untouched = fixture.seed(
            [_hash(2)],
            parent=OTHER_PARENT,
            credit_share_on_accept=True,
        )[0]
        server = fixture.server
        writer = server._ensure_share_writer_service()
        for candidate in (collapsed, untouched):
            writer.adopt_pending_share(candidate.pending_share)
        fixture.service.candidate_queue.put_nowait(collapsed)
        fixture.rpc.tip = OTHER_PARENT
        original = server._finish_pending_share_commit
        broken = {"on": True}

        def finish(pending_share: Any) -> None:
            if broken["on"]:
                raise RuntimeError("floor release boom")
            original(pending_share)

        server._finish_pending_share_commit = finish
        with redirect_stdout(StringIO()):
            fixture.collapse()
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup()[0])
        floor = fixture.floor()
        self.assertNotIn(id(collapsed.pending_share), floor)
        self.assertIn(id(untouched.pending_share), floor)

    # -- what a retry repeats ----------------------------------------------

    def test_a_retry_repeats_only_the_steps_that_are_still_owed(self) -> None:
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.seed([target])
        server = fixture.server
        discarded: list[str] = []
        original_discard = server._discard_outstanding_block_candidate

        def discard(block_hash: str) -> None:
            discarded.append(block_hash)
            original_discard(block_hash)

        server._discard_outstanding_block_candidate = discard
        broken = self._breaker(server, "_clear_block_candidate_retry_state")
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertEqual(discarded, [target])
        self.assertEqual(
            fixture.cleanup_backlog(),
            {target: frozenset({"retry-state"})},
        )
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup()[0])
        # A step that already completed is never run a second time.
        self.assertEqual(discarded, [target])
        self.assertEqual(fixture.cleanup_backlog(), {})

    def test_an_aborted_cleanup_is_deferred_and_retried_in_full(self) -> None:
        """An abort proves nothing, so every won hash owes every step again."""
        fixture = CollapseFixture()
        target = _hash(1)
        candidate = fixture.seed([target], credit_share_on_accept=True)[0]
        server = fixture.server
        server._ensure_share_writer_service().adopt_pending_share(
            candidate.pending_share
        )
        fixture.service.candidate_queue.put_nowait(candidate)
        scan = fixture.service._collapsed_candidate_floor_holders
        fixture.service._collapsed_candidate_floor_holders = (
            lambda abandoned: (_ for _ in ()).throw(RuntimeError("index boom"))
        )
        retained, log = fixture.collapse()
        self.assertEqual(retained, [])
        self.assertEqual(fixture.pending(), set())
        self.assertIn("cleanup aborted rows=1", log)
        self.assertEqual(fixture.counts()["cleanup_failed"], 1)
        self.assertEqual(
            fixture.cleanup_backlog(),
            {target: frozenset(BLOCK_CANDIDATE_COLLAPSE_CLEANUP_STEPS)},
        )
        # The abort ran the withdrawal and then stopped: the floor holder
        # and every terminal accounting step are still owed.
        self.assertIn(id(candidate.pending_share), fixture.floor())
        self.assertIn("terminal-outcome", fixture.cleanup_backlog()[target])
        with server.lock:
            # The apply published the terminal fence before cleanup, so the
            # durably terminal row is already unofferable even though the
            # step that normally publishes it never ran.
            self.assertIs(
                fixture.service._block_candidate_terminal_outcomes[target],
                False,
            )
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        fixture.service._collapsed_candidate_floor_holders = scan
        self.assertTrue(fixture.retry_cleanup()[0])
        # The retry re-indexes the holders the abort never got to read.
        self.assertNotIn(id(candidate.pending_share), fixture.floor())
        with server.lock:
            self.assertIs(
                fixture.service._block_candidate_terminal_outcomes[target],
                False,
            )
            self.assertNotIn(
                target,
                fixture.service._outstanding_block_candidate_hashes,
            )
        self.assertEqual(
            server.block_candidate_abandoned_counts,
            {PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON: 1},
        )
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertEqual(fixture.counts()["cleanup_recovered"], 1)

    def test_a_cleanup_retry_never_re_adopts_or_re_offers_the_row(self) -> None:
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.seed([target])
        server = fixture.server
        broken = self._breaker(server, "_record_block_candidate_terminal_outcome")
        with redirect_stdout(StringIO()):
            fixture.collapse()
        abandon_calls = len(fixture.ledger.abandon_calls)
        rpc_calls = len(fixture.rpc.calls)
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup()[0])
        # No durable read or write, no node offer, no queued candidate.
        self.assertEqual(fixture.pending(), set())
        self.assertEqual(len(fixture.ledger.abandon_calls), abandon_calls)
        self.assertEqual(len(fixture.rpc.calls), rpc_calls)
        self.assertTrue(fixture.service.candidate_queue.empty())
        self.assertTrue(fixture.service._block_replay_candidate_queue.empty())
        with server.lock:
            self.assertEqual(fixture.service._block_replay_inflight_hashes, set())
            self.assertIsNone(fixture.service.retry_candidate)
            self.assertIs(
                fixture.service._block_candidate_terminal_outcomes[target],
                False,
            )

    # -- bounds, pacing, and accounting ------------------------------------

    def test_the_backlog_is_bounded_by_the_affected_terminal_hashes(self) -> None:
        fixture = CollapseFixture()
        hashes = [_hash(1), _hash(2), _hash(3)]
        fixture.seed(hashes)
        server = fixture.server
        original = server._clear_block_candidate_retry_state
        broken = {"on": True}

        def retry_state(block_hash: str) -> None:
            if broken["on"] and block_hash != _hash(3):
                raise RuntimeError("retry state boom")
            original(block_hash)

        server._clear_block_candidate_retry_state = retry_state
        with redirect_stdout(StringIO()):
            fixture.collapse()
        # Only the hashes whose cleanup actually failed are retained.
        self.assertEqual(set(fixture.cleanup_backlog()), {_hash(1), _hash(2)})
        broken["on"] = False
        # One hash per pass, and the lane idles once the backlog drains.
        self.assertTrue(fixture.retry_cleanup()[0])
        self.assertEqual(len(fixture.cleanup_backlog()), 1)
        self.assertTrue(fixture.retry_cleanup()[0])
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertFalse(fixture.retry_cleanup()[0])
        counts = fixture.counts()
        self.assertEqual(counts["cleanup_recovered"], 2)
        self.assertEqual(counts["cleanup_failed"], 2)
        self.assertLessEqual(counts["cleanup_recovered"], counts["cleanup_failed"])

    def test_a_persistently_failing_retry_backs_off_without_recounting(self) -> None:
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.seed([target])
        fixture.service.retry_initial_seconds = 0.5
        fixture.service.retry_max_seconds = 2.0
        fixture.server._clear_block_candidate_retry_state = (
            lambda block_hash: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertEqual(fixture.counts()["cleanup_failed"], 1)
        for attempt, delay in ((1, 1.0), (2, 2.0), (3, 2.0)):
            self.assertTrue(fixture.retry_cleanup(force_due=True)[0])
            self.assertEqual(fixture.counts()["cleanup_retry_failed"], attempt)
            record = fixture.service._block_candidate_collapse_cleanup_retries[
                target
            ]
            self.assertEqual(record.attempts, attempt)
            self.assertEqual(record.delay_seconds, delay)
        # Attempts never inflate the affected-hash series.
        counts = fixture.counts()
        self.assertEqual(counts["cleanup_failed"], 1)
        self.assertEqual(counts["cleanup_recovered"], 0)
        self.assertEqual(set(fixture.cleanup_backlog()), {target})
        # A record parked behind its own backoff is not due.
        self.assertFalse(fixture.retry_cleanup()[0])

    def test_the_backlog_only_ever_holds_fixed_step_labels(self) -> None:
        fixture = CollapseFixture()
        hashes = [_hash(1), _hash(2)]
        fixture.seed(hashes)
        fixture.service._collapsed_candidate_floor_holders = (
            lambda abandoned: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        with redirect_stdout(StringIO()):
            fixture.collapse()
        backlog = fixture.cleanup_backlog()
        self.assertEqual(set(backlog), set(hashes))
        for steps in backlog.values():
            self.assertLessEqual(
                steps,
                frozenset(BLOCK_CANDIDATE_COLLAPSE_CLEANUP_STEPS),
            )
        # A step name outside the closed set is refused, not stored.
        with redirect_stdout(StringIO()):
            fixture.service._defer_collapsed_candidate_cleanup(
                _hash(9),
                ("not-a-cleanup-step",),
            )
        self.assertNotIn(_hash(9), fixture.cleanup_backlog())
        self.assertEqual(
            set(fixture.counts()),
            set(PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES),
        )

    def test_the_accounting_lane_drives_the_deferred_cleanup(self) -> None:
        """The retry must reach a shipped loop, not just a direct call."""
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.seed([target])
        server = fixture.server
        broken = self._breaker(server, "_clear_block_candidate_retry_state")
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertEqual(set(fixture.cleanup_backlog()), {target})
        broken["on"] = False
        service = fixture.service
        service._ensure_block_accounting_state()
        thread = threading.Thread(target=service.block_accounting_loop, daemon=True)
        with redirect_stdout(StringIO()):
            thread.start()
            try:
                # Bounded: the lane either drains the backlog quickly or the
                # deadline expires and the assertions below report it.
                deadline = time.monotonic() + 5.0
                while fixture.cleanup_backlog() and time.monotonic() < deadline:
                    time.sleep(0.01)
            finally:
                server.stop_event.set()
                thread.join(timeout=5.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertEqual(fixture.counts()["cleanup_recovered"], 1)


class BlockCandidateCollapseCleanupCadenceTests(unittest.TestCase):
    """The accounting lane must reach a due cleanup while it is busy.

    A collapsed row whose cleanup failed has no durable replay source, so
    the accounting lane is its only driver.  Driving it purely from that
    lane's idle branch made the retry hostage to the lane going idle:
    sustained ordinary accounting traffic, or a continuously replenished
    invalid-candidate quarantine queue, starved it for as long as the
    traffic lasted.  These tests own the explicit work-item cadence that
    closes the gap in both directions -- the cleanup reaches its record
    within a bounded number of work items, and the cleanup backlog in turn
    can spend no more than one attempt per cadence, so it cannot starve
    accounting or quarantine in return.
    """

    def _deferred(self, fixture: CollapseFixture, hashes: list[str]) -> dict:
        """Collapse ``hashes`` with one cleanup step broken; return the switch."""
        fixture.seed(hashes)
        broken = _break_cleanup_step(
            fixture.server,
            "_clear_block_candidate_retry_state",
        )
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertEqual(set(fixture.cleanup_backlog()), set(hashes))
        return broken

    def test_the_cadence_bound_is_the_shipped_default(self) -> None:
        fixture = CollapseFixture()
        self.assertEqual(
            fixture.service._block_accounting_cleanup_retry_work_items(),
            DEFAULT_BLOCK_ACCOUNTING_CLEANUP_RETRY_WORK_ITEMS,
        )
        self.assertGreaterEqual(DEFAULT_BLOCK_ACCOUNTING_CLEANUP_RETRY_WORK_ITEMS, 1)

    def test_a_non_positive_cadence_offers_after_every_work_item(self) -> None:
        """A misconfigured bound degrades to offering more, never never."""
        fixture = CollapseFixture()
        fixture.server.block_accounting_cleanup_retry_work_items = 0
        self.assertEqual(
            fixture.service._block_accounting_cleanup_retry_work_items(),
            1,
        )
        broken = self._deferred(fixture, [_hash(1)])
        broken["on"] = False
        rig = AccountingLaneRig(fixture, AccountingLaneRig.ACCOUNTING)
        self.assertTrue(rig.run_until(lambda: not fixture.cleanup_backlog()))
        self.assertLessEqual(rig.offers[0][0], 1)

    # -- starvation by ordinary accounting traffic -------------------------

    def test_a_due_cleanup_runs_while_the_accounting_queues_stay_busy(self) -> None:
        """The handoff queues never empty, so only the cadence can run it."""
        fixture = CollapseFixture()
        target = _hash(1)
        broken = self._deferred(fixture, [target])
        broken["on"] = False
        bound = fixture.service._block_accounting_cleanup_retry_work_items()
        rig = AccountingLaneRig(fixture, AccountingLaneRig.ACCOUNTING)
        self.assertTrue(
            rig.run_until(lambda: not fixture.cleanup_backlog()),
            f"cleanup starved behind accounting traffic; log={rig.log}",
        )
        self.assertFalse(rig.thread_alive)
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertEqual(fixture.counts()["cleanup_recovered"], 1)
        # The lane was never idle: every offer came from the cadence, and
        # the first one landed inside the documented bound.
        self.assertGreater(rig.accounting_items, 0)
        self.assertEqual(rig.quarantine_items, 0)
        self.assertLessEqual(rig.offers[0][0], bound)

    # -- starvation by invalid-candidate quarantine traffic ----------------

    def test_a_due_cleanup_runs_while_quarantine_is_replenished(self) -> None:
        """Quarantine always reports work, so the idle branch never runs."""
        fixture = CollapseFixture()
        target = _hash(1)
        broken = self._deferred(fixture, [target])
        broken["on"] = False
        bound = fixture.service._block_accounting_cleanup_retry_work_items()
        rig = AccountingLaneRig(fixture, AccountingLaneRig.QUARANTINE)
        self.assertTrue(
            rig.run_until(lambda: not fixture.cleanup_backlog()),
            f"cleanup starved behind quarantine traffic; log={rig.log}",
        )
        self.assertFalse(rig.thread_alive)
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertEqual(fixture.counts()["cleanup_recovered"], 1)
        self.assertEqual(rig.accounting_items, 0)
        self.assertGreater(rig.quarantine_items, 0)
        self.assertLessEqual(rig.offers[0][1], bound)

    # -- the cadence cannot be starved *by* the cleanup --------------------

    def test_a_cleanup_backlog_spends_one_attempt_per_cadence(self) -> None:
        """A permanently failing backlog must not take the lane over."""
        fixture = CollapseFixture()
        hashes = [_hash(index) for index in range(1, 6)]
        self._deferred(fixture, hashes)
        # retry_initial_seconds is 0 in this fixture, so the doubled backoff
        # stays 0 and every record in the backlog is due on every offer.
        self.assertEqual(fixture.service.retry_initial_seconds, 0.0)
        bound = fixture.service._block_accounting_cleanup_retry_work_items()
        rig = AccountingLaneRig(fixture, AccountingLaneRig.BOTH)
        self.assertTrue(rig.run_until(lambda: rig.work_items >= 20 * bound))
        self.assertFalse(rig.thread_alive)
        # One offer per completed cadence, and one attempt per offer at most.
        self.assertEqual(len(rig.offers), rig.work_items // bound)
        self.assertLessEqual(rig.attempts, rig.work_items // bound)
        self.assertEqual(fixture.counts()["cleanup_retry_failed"], rig.attempts)
        # Both lanes kept draining throughout; neither was taken over.
        self.assertGreaterEqual(rig.accounting_items, 5 * bound)
        self.assertGreaterEqual(rig.quarantine_items, 5 * bound)
        # Round-robin re-registration keeps the whole backlog owed.
        self.assertEqual(set(fixture.cleanup_backlog()), set(hashes))
        self.assertEqual(fixture.counts()["cleanup_recovered"], 0)

    # -- the cadence offers, the record's own deadline decides -------------

    def test_a_record_behind_its_backoff_is_offered_but_not_run(self) -> None:
        fixture = CollapseFixture()
        target = _hash(1)
        self._deferred(fixture, [target])
        registry = fixture.service._block_candidate_collapse_cleanup_retries
        with fixture.server.lock:
            registry[target].not_before_monotonic = time.monotonic() + 3600.0
        bound = fixture.service._block_accounting_cleanup_retry_work_items()
        rig = AccountingLaneRig(fixture, AccountingLaneRig.ACCOUNTING)
        self.assertTrue(rig.run_until(lambda: len(rig.offers) >= 3))
        self.assertFalse(rig.thread_alive)
        # Offered on cadence...
        self.assertLessEqual(rig.offers[0][0], bound)
        # ...and refused by the record's own deadline, not run early.
        self.assertEqual(rig.attempts, 0)
        self.assertEqual(registry[target].attempts, 0)
        self.assertEqual(set(fixture.cleanup_backlog()), {target})
        counts = fixture.counts()
        self.assertEqual(counts["cleanup_retry_failed"], 0)
        self.assertEqual(counts["cleanup_recovered"], 0)

    # -- the pre-existing idle path is unchanged ---------------------------

    def test_a_truly_idle_lane_still_runs_the_cleanup_immediately(self) -> None:
        """The idle branch must not wait for a cadence's worth of items."""
        fixture = CollapseFixture()
        target = _hash(1)
        broken = self._deferred(fixture, [target])
        broken["on"] = False
        rig = AccountingLaneRig(fixture, AccountingLaneRig.IDLE)
        self.assertTrue(
            rig.run_until(lambda: not fixture.cleanup_backlog()),
            f"idle lane did not finish the cleanup; log={rig.log}",
        )
        self.assertFalse(rig.thread_alive)
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertEqual(fixture.counts()["cleanup_recovered"], 1)
        # No work item of either kind was needed.
        self.assertEqual(rig.accounting_items, 0)
        self.assertEqual(rig.quarantine_items, 0)
        self.assertEqual(rig.offers[0], (0, 0))

    # -- regression proof for the cadence itself ---------------------------

    def test_without_the_cadence_both_lanes_starve_the_cleanup(self) -> None:
        """Neutralising the cadence must break the two starvation tests.

        Raising the bound out of reach removes every cadence offer, which is
        exactly what deleting the cadence calls does to the loop: in both
        busy modes the idle branch is unreachable, so nothing is left to
        drive the retry.  If this test ever stops starving, the two tests
        above have stopped proving the cadence.
        """
        bound = DEFAULT_BLOCK_ACCOUNTING_CLEANUP_RETRY_WORK_ITEMS
        for mode in (AccountingLaneRig.ACCOUNTING, AccountingLaneRig.QUARANTINE):
            with self.subTest(mode=mode):
                fixture = CollapseFixture()
                target = _hash(1)
                broken = self._deferred(fixture, [target])
                broken["on"] = False
                fixture.server.block_accounting_cleanup_retry_work_items = 1 << 30
                rig = AccountingLaneRig(fixture, mode)
                rig.run_until(lambda: rig.work_items >= 50 * bound)
                self.assertFalse(rig.thread_alive)
                self.assertGreaterEqual(rig.work_items, 50 * bound)
                self.assertEqual(rig.offers, [])
                self.assertEqual(rig.attempts, 0)
                self.assertEqual(set(fixture.cleanup_backlog()), {target})
                self.assertEqual(fixture.counts()["cleanup_recovered"], 0)


class BlockCandidateCollapseSubmissionFenceTests(unittest.TestCase):
    """A won row is fenced from the node by the write, not by its cleanup.

    The fenced batch write is the moment a row becomes durably terminal, but
    the in-memory fence the rest of the submitter honours -- the recorded
    terminal outcome -- used to be installed only by the ``terminal-outcome``
    cleanup step.  A cleanup that failed at that step, or a page-level abort
    that never reached it, therefore left a same-hash candidate in the live,
    replay, retry, or waiting lane free to be dequeued the moment the apply
    released its disposition lease: ``submit_next`` found no terminal
    outcome, consulted nothing else, and offered qbitd a block whose durable
    row was already abandoned.

    These tests own the invariant that closes it.  The apply publishes the
    false terminal outcome for the exact returned hash set while every won
    hash's lease is still held and before any cleanup runs, so the fence is
    up from the write onwards and no cleanup outcome -- success, contained
    per-step failure, page-level abort, or a retry that keeps failing -- can
    make a won row offerable again.
    """

    _breaker = staticmethod(BlockCandidateCollapseCleanupRetryTests._breaker)

    # -- harness -----------------------------------------------------------

    @staticmethod
    def _arm_node(fixture: CollapseFixture) -> None:
        """Let a node offer succeed, so an escape is loud rather than lost.

        With ``submitblock`` erroring, a fence regression could be mistaken
        for an ordinary transport failure; answering it makes the offer land
        exactly as it would in production.
        """
        fixture.rpc.results["submitblock"] = None
        fixture.server.block_submit_rpc_timeout_seconds = 0.5

    @staticmethod
    def _offers(fixture: CollapseFixture) -> int:
        return sum(1 for method, _ in fixture.rpc.calls if method == "submitblock")

    @staticmethod
    def _queue(
        fixture: CollapseFixture,
        candidate: Any,
        *,
        replay: bool = False,
    ) -> None:
        service = fixture.service
        service._ensure_block_replay_state()
        if replay:
            service._block_replay_candidate_queue.put_nowait(candidate)
        else:
            service.candidate_queue.put_nowait(candidate)

    @staticmethod
    def _submit(fixture: CollapseFixture, *, defer_accounting: bool = True) -> bool:
        """Drive the shipped queue-to-node path exactly as the loop does."""
        with redirect_stdout(StringIO()):
            return fixture.server.submit_next_block_candidate(
                defer_accounting=defer_accounting
            )

    def _seed(self, fixture: CollapseFixture, target: str) -> Any:
        """One durable row whose credit-bearing candidate owns a floor holder."""
        candidate = fixture.seed([target], credit_share_on_accept=True)[0]
        fixture.server._ensure_share_writer_service().adopt_pending_share(
            candidate.pending_share
        )
        self._arm_node(fixture)
        return candidate

    def _seed_queued(
        self,
        fixture: CollapseFixture,
        target: str,
        *,
        replay: bool = False,
    ) -> Any:
        candidate = self._seed(fixture, target)
        self._queue(fixture, candidate, replay=replay)
        return candidate

    def _assert_unofferable(self, fixture: CollapseFixture, target: str) -> None:
        """Nothing was offered, nothing was resurrected, nothing re-adopted."""
        self.assertEqual(self._offers(fixture), 0)
        self.assertEqual(fixture.pending(), set())
        self.assertEqual(len(fixture.ledger.abandon_calls), 1)
        service = fixture.service
        self.assertTrue(service.candidate_queue.empty())
        self.assertTrue(service._block_replay_candidate_queue.empty())
        with fixture.server.lock:
            self.assertIsNone(service.retry_candidate)
            self.assertNotIn(target, service._block_replay_inflight_hashes)
            self.assertNotIn(target, service._block_disposition_waiting_retries)

    # -- the two cleanup outcomes that used to unfence a won row -----------

    def test_a_failed_terminal_outcome_still_fences_the_queued_candidate(
        self,
    ) -> None:
        """The P1: the step that publishes the fence is the step that failed."""
        fixture = CollapseFixture()
        target = _hash(1)
        candidate = self._seed_queued(fixture, target)
        broken = self._breaker(
            fixture.server,
            "_record_block_candidate_terminal_outcome",
        )
        with redirect_stdout(StringIO()):
            fixture.collapse()
        # Exactly the failure this fence exists for: the durable row is gone
        # and its terminal cleanup is still owed, with the candidate object
        # still sitting in the live queue.
        self.assertEqual(fixture.pending(), set())
        self.assertEqual(
            fixture.cleanup_backlog(),
            {target: frozenset({"terminal-outcome"})},
        )
        self.assertFalse(fixture.service.candidate_queue.empty())
        # The submitter reaches the candidate before the accounting lane
        # reaches the retry -- the ordering the incident actually produced.
        self.assertTrue(self._submit(fixture))
        self._assert_unofferable(fixture, target)
        # Only now does the retry finish the step that failed.
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup()[0])
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertEqual(self._offers(fixture), 0)
        self.assertNotIn(id(candidate.pending_share), fixture.floor())
        counts = fixture.counts()
        self.assertEqual(counts["cleanup_failed"], 1)
        self.assertEqual(counts["cleanup_recovered"], 1)

    def test_an_aborted_cleanup_still_fences_the_queued_candidate(self) -> None:
        """A page-level abort never reaches the terminal-outcome step at all."""
        fixture = CollapseFixture()
        target = _hash(1)
        candidate = self._seed_queued(fixture, target)
        scan = fixture.service._collapsed_candidate_floor_holders
        fixture.service._collapsed_candidate_floor_holders = (
            lambda abandoned: (_ for _ in ()).throw(RuntimeError("index boom"))
        )
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertEqual(
            fixture.cleanup_backlog(),
            {target: frozenset(BLOCK_CANDIDATE_COLLAPSE_CLEANUP_STEPS)},
        )
        # The abort ran nothing past the withdrawal, so the queued object
        # still owns its identity-keyed floor holder.
        self.assertIn(id(candidate.pending_share), fixture.floor())
        self.assertTrue(self._submit(fixture))
        self._assert_unofferable(fixture, target)
        # The dropped duplicate released its own holder on the way out, which
        # is what lets the retry re-scan the drained queues and lose nothing.
        self.assertNotIn(id(candidate.pending_share), fixture.floor())
        fixture.service._collapsed_candidate_floor_holders = scan
        self.assertTrue(fixture.retry_cleanup()[0])
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertEqual(self._offers(fixture), 0)

    # -- lane ownership ----------------------------------------------------

    def test_the_replay_lane_is_fenced_by_the_same_published_outcome(self) -> None:
        """Durable replay dequeues through the same guard as a live solve."""
        fixture = CollapseFixture()
        target = _hash(1)
        self._seed_queued(fixture, target, replay=True)
        service = fixture.service
        with fixture.server.lock:
            service._block_replay_inflight_hashes.add(target)
        broken = self._breaker(
            fixture.server,
            "_record_block_candidate_terminal_outcome",
        )
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertFalse(service._block_replay_candidate_queue.empty())
        self.assertTrue(self._submit(fixture))
        self.assertEqual(self._offers(fixture), 0)
        self.assertTrue(service._block_replay_candidate_queue.empty())
        self.assertEqual(fixture.pending(), set())
        # A freshly decoded same-hash replay object is refused admission by
        # the same published outcome rather than queued behind it.
        sibling = fixture.seed([target], credit_share_on_accept=True)[0]
        with redirect_stdout(StringIO()):
            self.assertFalse(service._enqueue_replayed_block_candidate(sibling))
        self.assertTrue(service._block_replay_candidate_queue.empty())
        self.assertEqual(self._offers(fixture), 0)
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup()[0])
        self.assertEqual(fixture.cleanup_backlog(), {})

    def test_the_waiting_retry_lane_is_fenced_when_the_lease_releases(self) -> None:
        """A wakeup parked behind the apply's own lease must not be offered."""
        fixture = CollapseFixture()
        target = _hash(1)
        candidate = self._seed(fixture, target)
        service = fixture.service
        inner = fixture.ledger.inner.mark_block_candidates_abandoned

        def park_then_write(requested: tuple[str, ...], error: str) -> Any:
            # A parked wakeup is collapse evidence, so the only ordering that
            # produces one for a won hash is this: the submitter dequeued the
            # hash, could not claim the apply's lease, and parked it after the
            # pre-write revalidation re-read the evidence and before the
            # fenced write landed.
            with fixture.server.lock:
                service._block_disposition_waiting_retries[target] = candidate
            return inner(block_hashes=requested, error=error)

        fixture.ledger.abandon_hook = park_then_write
        broken = self._breaker(
            fixture.server,
            "_record_block_candidate_terminal_outcome",
        )
        with redirect_stdout(StringIO()):
            fixture.collapse()
        # The failed step is also the one that drops parked wakeups, so the
        # candidate is still parked and will be dequeued from there.
        with fixture.server.lock:
            self.assertIn(target, service._block_disposition_waiting_retries)
        self.assertTrue(self._submit(fixture))
        self._assert_unofferable(fixture, target)
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup()[0])
        self.assertEqual(fixture.cleanup_backlog(), {})

    # -- the non-queue seams ------------------------------------------------

    def test_the_direct_writer_seam_refuses_a_fenced_hash(self) -> None:
        """The historical direct entrypoint takes the guard and answers False."""
        fixture = CollapseFixture()
        target = _hash(1)
        candidate = self._seed_queued(fixture, target)
        fixture.service.candidate_queue.get_nowait()
        broken = self._breaker(
            fixture.server,
            "_record_block_candidate_terminal_outcome",
        )
        with redirect_stdout(StringIO()):
            fixture.collapse()
        with redirect_stdout(StringIO()):
            landed = fixture.server._submit_next_block_candidate_writer(candidate)
        self.assertFalse(landed)
        self.assertEqual(self._offers(fixture), 0)
        self.assertEqual(fixture.pending(), set())
        self.assertEqual(len(fixture.ledger.abandon_calls), 1)
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup()[0])

    def test_the_synchronous_seam_refuses_a_fenced_hash(self) -> None:
        """The miner-facing resubmit joins the same terminal disposition."""
        fixture = CollapseFixture()
        target = _hash(1)
        candidate = self._seed_queued(fixture, target)
        fixture.service.candidate_queue.get_nowait()
        broken = self._breaker(
            fixture.server,
            "_record_block_candidate_terminal_outcome",
        )
        with redirect_stdout(StringIO()):
            fixture.collapse()
        with redirect_stdout(StringIO()):
            landed = fixture.server._submit_synchronous_block_candidate(candidate)
        self.assertFalse(landed)
        self.assertEqual(self._offers(fixture), 0)
        self.assertEqual(fixture.pending(), set())
        self.assertNotIn(id(candidate.pending_share), fixture.floor())
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup()[0])

    # -- durability of the fence across the retry series -------------------

    def test_the_fence_survives_every_retry_failure_and_its_discharge(self) -> None:
        """Repeated failures, then recovery, and never one node offer."""
        fixture = CollapseFixture()
        target = _hash(1)
        candidate = self._seed(fixture, target)
        fixture.service.retry_initial_seconds = 0.5
        fixture.service.retry_max_seconds = 2.0
        broken = self._breaker(
            fixture.server,
            "_record_block_candidate_terminal_outcome",
        )
        with redirect_stdout(StringIO()):
            fixture.collapse()
        for attempt in (1, 2, 3):
            self.assertTrue(fixture.retry_cleanup(force_due=True)[0])
            self.assertEqual(fixture.counts()["cleanup_retry_failed"], attempt)
            self.assertEqual(set(fixture.cleanup_backlog()), {target})
            # The fence is unaffected by the attempt that just failed: the
            # same-hash wakeup is dropped, not offered, on every round.
            self._queue(fixture, candidate)
            self.assertTrue(self._submit(fixture))
            self._assert_unofferable(fixture, target)
        broken["on"] = False
        self.assertTrue(fixture.retry_cleanup(force_due=True)[0])
        self.assertEqual(fixture.cleanup_backlog(), {})
        # Discharging the backlog leaves the ordinary terminal fence behind,
        # so the durably terminal row is still never re-offered.
        self._queue(fixture, candidate)
        self.assertTrue(self._submit(fixture))
        self._assert_unofferable(fixture, target)
        with fixture.server.lock:
            self.assertIs(
                fixture.service._block_candidate_terminal_outcomes[target],
                False,
            )
        # Accounting stayed bounded by the affected hashes, and the label
        # space is still the closed outcome set.
        counts = fixture.counts()
        self.assertEqual(counts["cleanup_failed"], 1)
        self.assertEqual(counts["cleanup_recovered"], 1)
        self.assertEqual(counts["cleanup_retry_failed"], 3)
        self.assertLessEqual(counts["cleanup_recovered"], counts["cleanup_failed"])
        self.assertEqual(
            set(counts),
            set(PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES),
        )

    def test_a_clean_collapse_fences_its_queued_candidate_too(self) -> None:
        """The fence is a property of the write, not of a cleanup failure."""
        fixture = CollapseFixture()
        target = _hash(1)
        self._seed_queued(fixture, target)
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertEqual(fixture.counts()["cleanup_failed"], 0)
        self.assertTrue(self._submit(fixture))
        self._assert_unofferable(fixture, target)

    def test_a_preserved_row_is_never_fenced_by_another_pages_win(self) -> None:
        """Only the exact returned hash set is published, never the request."""
        fixture = CollapseFixture()
        won, lost = _hash(1), _hash(2)
        fixture.seed([won, lost], credit_share_on_accept=True)
        fixture.ledger.abandon_hook = lambda requested, error: (won,)
        with redirect_stdout(StringIO()):
            fixture.collapse()
        service = fixture.service
        with fixture.server.lock:
            outcomes = dict(service._block_candidate_terminal_outcomes)
        self.assertEqual(outcomes, {won: False})
        self.assertEqual(fixture.counts()["write_lost"], 1)


class _CountingRegistry(dict):
    """A registry that counts membership probes and reports being walked.

    The page-bounded evidence read has to answer from probes alone; a
    regression that goes back to copying a registry into a set shows up here
    as a walk (optionally a hard failure) rather than as a wall-clock wobble
    that a fast enough machine hides.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.probes = 0
        self.walks = 0
        self.forbid_walks = False

    def __contains__(self, key: object) -> bool:
        self.probes += 1
        return super().__contains__(key)

    def __iter__(self) -> Any:
        self.walks += 1
        if self.forbid_walks:
            raise AssertionError("terminal outcome registry was walked")
        return super().__iter__()


def _history_hashes(count: int, *, start: int = 1 << 20) -> list[str]:
    """``count`` distinct hashes standing in for already-disposed blocks."""
    return [f"{index:064x}" for index in range(start, start + count)]


class BlockCandidateCollapseEvidenceCostTests(unittest.TestCase):
    """One evidence probe per hash the caller is deciding, and no more.

    ``_block_candidate_terminal_outcomes`` is the one registry the collapse
    evidence read touches that grows with every block the process has ever
    disposed: the 2026-08-20 storm left 312,000 entries behind, and every
    poll folded all of them into a set while holding ``coordinator.lock``
    -- about 124 ms per page, linear in history, with every unrelated share
    ack queued behind the same lock.  The selector never needed the union:
    it asks one question, "is this page's hash already offered", for at most
    a page of hashes at a time.

    These tests pin that shape with a registry that counts its probes and
    refuses to be walked, so the cost claim is a deterministic assertion
    about work performed rather than a timing measurement.
    """

    HISTORY = 200_000

    @staticmethod
    def _install_history(
        fixture: CollapseFixture,
        size: int,
        *,
        terminal: Iterable[str] = (),
    ) -> _CountingRegistry:
        registry = _CountingRegistry(
            (block_hash, False) for block_hash in _history_hashes(size)
        )
        for block_hash in terminal:
            registry[block_hash] = False
        with fixture.server.lock:
            fixture.service._block_candidate_terminal_outcomes = registry
        registry.probes = 0
        registry.walks = 0
        return registry

    def test_evidence_answers_a_page_without_walking_the_history(self) -> None:
        fixture = CollapseFixture()
        page = [_hash(1), _hash(2)]
        fixture.seed(page)
        registry = self._install_history(
            fixture,
            self.HISTORY,
            terminal=[_hash(2)],
        )
        registry.forbid_walks = True
        evidence = fixture.service._block_candidate_collapse_evidence(page)
        # Semantics unchanged: the historical terminal outcome still speaks
        # for its hash, it is simply asked about rather than enumerated.
        self.assertEqual(evidence, frozenset({_hash(2)}))
        self.assertEqual(registry.walks, 0)
        self.assertEqual(registry.probes, len(page))

    def test_selection_work_does_not_grow_with_the_history(self) -> None:
        """The same page costs the same probes at 1,000 and at 200,000."""
        page = [_hash(index) for index in range(1, 6)]
        probes: dict[int, int] = {}
        for size in (1_000, self.HISTORY):
            with self.subTest(history=size):
                fixture = CollapseFixture()
                fixture.seed(page)
                rows = fixture.page()
                registry = self._install_history(fixture, size)
                registry.forbid_walks = True
                chain = _BlockCandidateChainView(fixture.service)
                selected = fixture.service._select_superseded_block_candidates(
                    rows,
                    chain,
                )
                self.assertEqual(
                    {row.block_hash for row in selected},
                    set(page),
                )
                self.assertEqual(registry.walks, 0)
                probes[size] = registry.probes
        self.assertEqual(probes[1_000], probes[self.HISTORY])
        self.assertEqual(probes[self.HISTORY], len(page))

    def test_revalidation_probes_only_the_leased_subset(self) -> None:
        """The pre-write re-read is bounded by the leases, not by the page."""
        fixture = CollapseFixture()
        page = [_hash(index) for index in range(1, 6)]
        fixture.seed(page)
        chain = _BlockCandidateChainView(fixture.service)
        selected = fixture.service._select_superseded_block_candidates(
            fixture.page(),
            chain,
        )
        leased = selected[:2]
        registry = self._install_history(fixture, 1_000)
        registry.forbid_walks = True
        qualified, _tip = fixture.service._revalidate_superseded_block_candidates(
            leased,
            chain,
        )
        self.assertEqual(
            {row.block_hash for row in qualified},
            {row.block_hash for row in leased},
        )
        self.assertEqual(registry.walks, 0)
        self.assertEqual(registry.probes, len(leased))

    def test_a_collapse_over_a_large_history_bounds_it(self) -> None:
        """The apply still wins its page, and leaves the registry bounded."""
        fixture = CollapseFixture()
        page = [_hash(index) for index in range(1, 6)]
        fixture.seed(page)
        with fixture.server.lock:
            fixture.service._block_candidate_terminal_outcomes = {
                block_hash: False
                for block_hash in _history_hashes(self.HISTORY)
            }
        self.assertEqual(_selected(fixture), set(page))
        outcomes = fixture.service._block_candidate_terminal_outcomes
        self.assertEqual(len(outcomes), MAX_BLOCK_CANDIDATE_TERMINAL_OUTCOMES)
        for block_hash in page:
            self.assertIs(outcomes[block_hash], False)


class BlockCandidateTerminalOutcomeBoundTests(unittest.TestCase):
    """The terminal-outcome registry is bounded, and never unfences a copy.

    The same-hash disposition guard used to remember every outcome for the
    life of the process.  That is what made the collapse evidence read
    expensive, but it is also unbounded memory in its own right, so the
    registry is trimmed oldest-first once it passes
    ``MAX_BLOCK_CANDIDATE_TERMINAL_OUTCOMES``.

    Eviction is the dangerous half.  A forgotten outcome reads exactly like
    an outcome that never happened, so a same-hash candidate still sitting
    in the live, replay, retry, parked, or quarantine lane would find no
    fence and offer qbitd a block whose durable row is already terminal --
    the very escape ``_publish_collapsed_candidate_terminal_fence`` exists
    to close.  Every lane that can still be holding such a copy therefore
    pins its hash, and these tests own that pin, lane by lane.
    """

    CAP = MAX_BLOCK_CANDIDATE_TERMINAL_OUTCOMES

    # -- harness -----------------------------------------------------------

    @staticmethod
    def _fill(fixture: CollapseFixture, count: int) -> None:
        """Accumulate historical outcomes behind whatever is already there."""
        with fixture.server.lock:
            outcomes = fixture.service._block_candidate_terminal_outcomes
            for block_hash in _history_hashes(count):
                outcomes[block_hash] = False

    @staticmethod
    def _record(fixture: CollapseFixture, count: int) -> None:
        """Drive ``count`` real terminal outcomes, each one a trim occasion."""
        for block_hash in _history_hashes(count, start=1 << 40):
            fixture.server._record_block_candidate_terminal_outcome(
                block_hash,
                accepted=False,
            )

    def _pressure(self, fixture: CollapseFixture) -> None:
        """Push the registry past its bound through the shipped writer."""
        self._fill(fixture, self.CAP)
        self._record(fixture, 64)

    # -- the bound ---------------------------------------------------------

    def test_the_registry_stops_growing_at_its_bound(self) -> None:
        fixture = CollapseFixture()
        self._record(fixture, self.CAP + 512)
        outcomes = fixture.service._block_candidate_terminal_outcomes
        self.assertEqual(len(outcomes), self.CAP)
        history = _history_hashes(self.CAP + 512, start=1 << 40)
        # Oldest-first: the first outcomes are gone, the newest are kept.
        self.assertIsNone(
            fixture.server._block_candidate_terminal_outcome(history[0])
        )
        self.assertIs(
            fixture.server._block_candidate_terminal_outcome(history[-1]),
            False,
        )

    def test_a_re_recorded_outcome_is_treated_as_the_newest(self) -> None:
        """Reasserting an outcome must not leave it at the eviction front."""
        fixture = CollapseFixture()
        target = _hash(1)
        fixture.server._record_block_candidate_terminal_outcome(
            target,
            accepted=False,
        )
        self._fill(fixture, 8)
        fixture.server._record_block_candidate_terminal_outcome(
            target,
            accepted=False,
        )
        outcomes = fixture.service._block_candidate_terminal_outcomes
        self.assertEqual(list(outcomes)[-1], target)

    def test_pinned_entries_do_not_stall_the_trim(self) -> None:
        """A run of pinned hashes longer than the scan window still drains."""
        fixture = CollapseFixture()
        pinned = _history_hashes(
            BLOCK_CANDIDATE_TERMINAL_OUTCOME_EVICTION_SCAN * 2,
            start=1 << 30,
        )
        for block_hash in pinned:
            fixture.server._record_block_candidate_terminal_outcome(
                block_hash,
                accepted=False,
            )
        with fixture.server.lock:
            fixture.service._block_replay_inflight_hashes.update(pinned)
        self._pressure(fixture)
        outcomes = fixture.service._block_candidate_terminal_outcomes
        self.assertEqual(len(outcomes), self.CAP)
        for block_hash in pinned:
            self.assertIn(block_hash, outcomes)

    def test_a_fence_page_larger_than_the_bound_keeps_its_publication(
        self,
    ) -> None:
        """A storm-sized fence never evicts the outcomes it just published."""
        fixture = CollapseFixture()
        self._fill(fixture, self.CAP)
        page = tuple(_history_hashes(self.CAP + 100, start=1 << 50))
        with redirect_stdout(StringIO()):
            fixture.service._publish_collapsed_candidate_terminal_fence(page)
        outcomes = fixture.service._block_candidate_terminal_outcomes
        for block_hash in page:
            self.assertIs(outcomes[block_hash], False)
        # Only history was dropped; the registry stays over its bound
        # rather than unfencing a row the apply is still cleaning up.
        self.assertEqual(len(outcomes), len(page))

    def test_the_trim_is_skipped_rather_than_waiting_on_a_lane(self) -> None:
        """Nothing is evicted against a lane that could not be read."""
        fixture = CollapseFixture()
        self._fill(fixture, self.CAP + 16)
        service = fixture.service
        with service.candidate_queue.mutex:
            with fixture.server.lock:
                self.assertEqual(
                    service._bound_block_candidate_terminal_outcomes(),
                    0,
                )
        self.assertEqual(
            len(service._block_candidate_terminal_outcomes),
            self.CAP + 16,
        )
        with fixture.server.lock:
            self.assertEqual(
                service._bound_block_candidate_terminal_outcomes(),
                16,
            )

    # -- the pins ----------------------------------------------------------

    def test_every_lane_that_holds_a_copy_pins_its_outcome(self) -> None:
        """Each lane the fence protects keeps its hash out of the eviction."""
        target = _hash(1)

        def outstanding(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.server._register_outstanding_block_candidate(target)

        def replay_inflight(fixture: CollapseFixture, candidate: Any) -> None:
            with fixture.server.lock:
                fixture.service._block_replay_inflight_hashes.add(target)

        def quarantined(fixture: CollapseFixture, candidate: Any) -> None:
            with fixture.server.lock:
                fixture.service._block_quarantine_hashes.add(target)

        def fast_lane(fixture: CollapseFixture, candidate: Any) -> None:
            with fixture.server.lock:
                fixture.service._block_fast_lane_reservations.add(target)

        def waiting_retry(fixture: CollapseFixture, candidate: Any) -> None:
            with fixture.server.lock:
                fixture.service._block_disposition_waiting_retries[target] = (
                    candidate
                )

        def finalize_retry(fixture: CollapseFixture, candidate: Any) -> None:
            with fixture.server.lock:
                fixture.service.finalize_retries[target] = (False, "boom")

        def retained_submission(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.service._stash_retained_block_candidate_node_submission(
                target,
                _BlockCandidateNodeSubmission(attempted=True, result=None),
            )

        def cleanup_retry(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.service._defer_collapsed_candidate_cleanup(
                target,
                frozenset({"terminal-outcome"}),
            )

        def retry_holder(fixture: CollapseFixture, candidate: Any) -> None:
            with fixture.server.lock:
                fixture.service.retry_candidate = candidate

        def deferred_retry_holder(
            fixture: CollapseFixture,
            candidate: Any,
        ) -> None:
            with fixture.server.lock:
                fixture.service._block_accounting_deferred_retry_candidate = (
                    candidate
                )

        def live_queue(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.service.candidate_queue.put_nowait(candidate)

        def replay_queue(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.service._block_replay_candidate_queue.put_nowait(candidate)

        def disposition_flight(fixture: CollapseFixture, candidate: Any) -> None:
            lease = fixture.server._claim_block_candidate_disposition(
                target,
                blocking=False,
            )
            self.assertIsNotNone(lease)

        def dequeued_pin(fixture: CollapseFixture, candidate: Any) -> None:
            # The instant between leaving a lane and owning a flight.
            with fixture.server.lock:
                fixture.service._pin_dequeued_block_candidate_locked(candidate)

        lanes = {
            "_outstanding_block_candidate_hashes": outstanding,
            "_block_replay_inflight_hashes": replay_inflight,
            "_block_quarantine_hashes": quarantined,
            "_block_fast_lane_reservations": fast_lane,
            "_block_disposition_waiting_retries": waiting_retry,
            "finalize_retries": finalize_retry,
            "_block_candidate_retained_node_submissions": retained_submission,
            "_block_candidate_collapse_cleanup_retries": cleanup_retry,
            "retry_candidate": retry_holder,
            "_block_accounting_deferred_retry_candidate": deferred_retry_holder,
            "candidate_queue": live_queue,
            "_block_replay_candidate_queue": replay_queue,
            "_block_candidate_disposition_flights": disposition_flight,
            "_block_candidate_dequeued_hashes": dequeued_pin,
        }
        for name, install in lanes.items():
            with self.subTest(lane=name):
                fixture = CollapseFixture()
                candidate = fixture.seed([target])[0]
                # The outcome is recorded first, exactly as a terminal
                # disposition does, and the lane is populated afterwards:
                # recording clears the reservation, replay, and waiting
                # markers for its own hash by design.
                fixture.server._record_block_candidate_terminal_outcome(
                    target,
                    accepted=False,
                )
                with redirect_stdout(StringIO()):
                    install(fixture, candidate)
                self._pressure(fixture)
                outcomes = fixture.service._block_candidate_terminal_outcomes
                self.assertEqual(len(outcomes), self.CAP)
                self.assertIs(
                    fixture.server._block_candidate_terminal_outcome(target),
                    False,
                )

    def test_a_queued_duplicate_keeps_its_fence_under_eviction_pressure(
        self,
    ) -> None:
        """The escape the fence closes must survive the registry's bound.

        A won row's cleanup clears the ownership markers -- outstanding,
        replay-inflight -- for the hash it just terminalized, so a same-hash
        candidate still queued behind the apply is named by no registry at
        all.  Its fence would be the oldest evictable entry in the registry
        and the dequeue would then find nothing.  The queue itself is read,
        so it does not.
        """
        fixture = CollapseFixture()
        queued, forgotten = _hash(1), _hash(2)
        candidate = fixture.seed([queued], credit_share_on_accept=True)[0]
        fixture.seed([forgotten])
        fixture.server._ensure_share_writer_service().adopt_pending_share(
            candidate.pending_share
        )
        # A working node offer, so an escape lands as a real submitblock
        # rather than being lost in a transport error.
        fixture.rpc.results["submitblock"] = None
        fixture.server.block_submit_rpc_timeout_seconds = 0.5
        fixture.service.candidate_queue.put_nowait(candidate)
        with redirect_stdout(StringIO()):
            fixture.collapse()
        self.assertEqual(fixture.pending(), set())
        self.assertEqual(fixture.cleanup_backlog(), {})
        with fixture.server.lock:
            service = fixture.service
            self.assertNotIn(
                queued,
                service._outstanding_block_candidate_hashes,
            )
            self.assertNotIn(queued, service._block_replay_inflight_hashes)
        self._pressure(fixture)
        # The unheld hash ages out; the queued copy's fence does not.
        self.assertIsNone(
            fixture.server._block_candidate_terminal_outcome(forgotten)
        )
        self.assertIs(
            fixture.server._block_candidate_terminal_outcome(queued),
            False,
        )
        with redirect_stdout(StringIO()):
            self.assertTrue(
                fixture.server.submit_next_block_candidate(defer_accounting=True)
            )
        offers = [
            method for method, _ in fixture.rpc.calls if method == "submitblock"
        ]
        self.assertEqual(offers, [])
        self.assertTrue(fixture.service.candidate_queue.empty())

    # -- the dequeue-to-disposition pin ------------------------------------

    WAIT_SECONDS = 10.0

    def _park_claim(
        self,
        fixture: CollapseFixture,
        target: str,
    ) -> tuple[threading.Event, threading.Event]:
        """Park the submitter inside the old dequeue-to-disposition gap.

        Wraps the disposition claim so the pass that dequeued ``target`` is
        held after it has left its lane and before any flight exists for
        it -- exactly where ``submit_next``'s local variable used to be the
        only thing holding the candidate.  The park is deterministic: only
        the test releases it, after it has applied eviction pressure and
        looked at the registry.
        """
        service = fixture.service
        original = service._claim_block_candidate_disposition
        in_gap = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)

        def park_then_claim(block_hash: str, *, blocking: bool) -> Any:
            if block_hash == target:
                in_gap.set()
                if not release.wait(self.WAIT_SECONDS):
                    raise AssertionError("the parked claim was never released")
            return original(block_hash, blocking=blocking)

        service._claim_block_candidate_disposition = park_then_claim
        return in_gap, release

    @staticmethod
    def _submit_in_background(
        fixture: CollapseFixture,
    ) -> tuple[threading.Thread, list[Any]]:
        """Run one submitter pass on its own thread; collect its result."""
        results: list[Any] = []

        def run() -> None:
            try:
                results.append(
                    fixture.server.submit_next_block_candidate(
                        defer_accounting=True
                    )
                )
            except BaseException as error:  # reported to the test, not raised here
                results.append(error)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return thread, results

    @staticmethod
    def _dequeue_lanes_empty(fixture: CollapseFixture, target: str) -> bool:
        """Whether no dequeue source still holds a copy of ``target``."""
        service = fixture.service
        with fixture.server.lock:
            return (
                service.retry_candidate is None
                and service.candidate_queue.empty()
                and service._block_replay_candidate_queue.empty()
                and target not in service._block_disposition_waiting_retries
            )

    def test_a_dequeued_duplicate_keeps_its_fence_across_the_claim_gap(
        self,
    ) -> None:
        """Eviction inside the dequeue-to-disposition gap finds the hash pinned.

        Every lane pins its hash while a candidate sits in it, and the
        disposition flight pins it from the claim onward.  Between the two
        the candidate used to be held only by ``submit_next``'s local
        variable: the retry holder cleared, the queue entry consumed, or
        the waiting entry popped, and no flight installed yet.  An eviction
        in that window dropped the oldest unpinned outcome -- this
        duplicate's fence -- and the pass then found nothing and offered a
        durably terminal block to the node.

        The dequeue now moves the hash into the dequeued-hash pin under the
        same lock hold that empties the lane, and hands it to the flight
        (or back to the waiting registry) under the lock again, so there is
        no instant at which nothing names it.  The barrier parks the pass
        in the old gap, applies real eviction pressure from the other side,
        and proves the fence survives -- for each of the four sources a
        pass can dequeue from.
        """
        target = _hash(1)

        def retry_holder(fixture: CollapseFixture, candidate: Any) -> None:
            with fixture.server.lock:
                fixture.service.retry_candidate = candidate

        def live_queue(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.service.candidate_queue.put_nowait(candidate)

        def replay_queue(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.service._block_replay_candidate_queue.put_nowait(candidate)

        def waiting_retry(fixture: CollapseFixture, candidate: Any) -> None:
            with fixture.server.lock:
                fixture.service._block_disposition_waiting_retries[target] = (
                    candidate
                )

        sources = {
            "retry_candidate": retry_holder,
            "candidate_queue": live_queue,
            "_block_replay_candidate_queue": replay_queue,
            "_block_disposition_waiting_retries": waiting_retry,
        }
        for name, install in sources.items():
            with self.subTest(source=name):
                fixture = CollapseFixture()
                service = fixture.service
                candidate = fixture.seed([target])[0]
                # A working node offer, so an escape would land as a real
                # submitblock rather than being lost in a transport error.
                fixture.rpc.results["submitblock"] = None
                fixture.server.block_submit_rpc_timeout_seconds = 0.5
                # Recorded first, exactly as a terminal disposition does;
                # the duplicate arrives in its lane afterwards.
                fixture.server._record_block_candidate_terminal_outcome(
                    target,
                    accepted=False,
                )
                install(fixture, candidate)
                # The duplicate's fence is now the oldest entry and the
                # registry sits one over its bound: the next recorded
                # outcome evicts exactly the oldest unpinned hash.
                self._fill(fixture, self.CAP)
                in_gap, release = self._park_claim(fixture, target)
                with redirect_stdout(StringIO()):
                    thread, results = self._submit_in_background(fixture)
                    self.assertTrue(in_gap.wait(self.WAIT_SECONDS))
                    # This is the old gap: the lane is empty, no flight
                    # exists, and the candidate object is the submitter's
                    # local variable.
                    self.assertTrue(self._dequeue_lanes_empty(fixture, target))
                    with service._block_candidate_disposition_registry_lock:
                        self.assertNotIn(
                            target,
                            service._block_candidate_disposition_flights,
                        )
                    self._record(fixture, 64)
                    # The fence survived the pressure; history did not.
                    self.assertIs(
                        fixture.server._block_candidate_terminal_outcome(target),
                        False,
                    )
                    self.assertEqual(
                        len(service._block_candidate_terminal_outcomes),
                        self.CAP,
                    )
                    # ...because the dequeue itself is what names the hash.
                    with fixture.server.lock:
                        self.assertEqual(
                            service._block_candidate_dequeued_hashes,
                            {target: 1},
                        )
                    release.set()
                    thread.join(self.WAIT_SECONDS)
                self.assertFalse(thread.is_alive())
                self.assertEqual(results, [True])
                offers = [
                    method
                    for method, _ in fixture.rpc.calls
                    if method == "submitblock"
                ]
                self.assertEqual(offers, [])
                # The duplicate was dropped against its fence: nothing holds
                # it, no pin outlived the pass, and the fence still stands.
                self.assertTrue(self._dequeue_lanes_empty(fixture, target))
                with fixture.server.lock:
                    self.assertEqual(service._block_candidate_dequeued_hashes, {})
                with service._block_candidate_disposition_registry_lock:
                    self.assertEqual(service._block_candidate_disposition_flights, {})
                self.assertIs(
                    fixture.server._block_candidate_terminal_outcome(target),
                    False,
                )

    def test_the_dequeued_pin_never_outlives_the_claim(self) -> None:
        """Every way the claim comes back hands the hash on and unpins it.

        A pin that leaked would be a hash the eviction could never reclaim
        -- unbounded growth by another name -- and a pin dropped early is
        the window the pin exists to close.  A lease hands the hash to the
        installed flight (covered above); a miss parks the candidate in the
        waiting registry in the same critical section; an exception leaves
        nothing behind because the candidate object leaves with it; and a
        parked retry that is not yet due is never dequeued, so it is never
        pinned.
        """
        target = _hash(1)

        with self.subTest(outcome="lease-miss"):
            fixture = CollapseFixture()
            service = fixture.service
            candidate = fixture.seed([target])[0]
            # Another same-hash pass spans the offer-to-finalize region.
            lease = fixture.server._claim_block_candidate_disposition(
                target,
                blocking=False,
            )
            self.assertIsNotNone(lease)
            self.addCleanup(
                fixture.server._release_block_candidate_disposition,
                lease,
            )
            service.candidate_queue.put_nowait(candidate)
            with redirect_stdout(StringIO()):
                self.assertTrue(
                    fixture.server.submit_next_block_candidate(
                        defer_accounting=True
                    )
                )
            with fixture.server.lock:
                self.assertIs(
                    service._block_disposition_waiting_retries.get(target),
                    candidate,
                )
                self.assertEqual(service._block_candidate_dequeued_hashes, {})
            self.assertTrue(service.candidate_queue.empty())

        with self.subTest(outcome="claim-raises"):
            fixture = CollapseFixture()
            service = fixture.service
            candidate = fixture.seed([target])[0]

            def explode(block_hash: str, *, blocking: bool) -> Any:
                raise RuntimeError("claim exploded")

            service._claim_block_candidate_disposition = explode
            service.candidate_queue.put_nowait(candidate)
            with redirect_stdout(StringIO()):
                with self.assertRaises(RuntimeError):
                    fixture.server.submit_next_block_candidate(
                        defer_accounting=True
                    )
            with fixture.server.lock:
                self.assertEqual(service._block_candidate_dequeued_hashes, {})
            with service._block_candidate_disposition_registry_lock:
                self.assertEqual(service._block_candidate_disposition_flights, {})
            self.assertTrue(self._dequeue_lanes_empty(fixture, target))

        with self.subTest(outcome="retry-not-due"):
            fixture = CollapseFixture()
            service = fixture.service
            candidate = fixture.seed([target])[0]
            with fixture.server.lock:
                service.retry_candidate = candidate
                service._block_candidate_retry_not_before = {
                    target: time.monotonic() + 60.0
                }
            with redirect_stdout(StringIO()):
                self.assertFalse(
                    fixture.server.submit_next_block_candidate(
                        defer_accounting=True
                    )
                )
            with fixture.server.lock:
                self.assertIs(service.retry_candidate, candidate)
                self.assertEqual(service._block_candidate_dequeued_hashes, {})

    # -- the accounting lane's exception handoff ---------------------------

    @staticmethod
    def _accounting_task(
        fixture: CollapseFixture,
        target: str,
        candidate: Any,
    ) -> Any:
        """One queued accounting task owning ``target``'s disposition lease.

        Built exactly as the submitter's deferred handoff leaves it: the
        node offer is done and the lease travels with the task until the
        accounting lane releases it.
        """
        server = fixture.server
        lease = server._claim_block_candidate_disposition(target, blocking=False)
        assert lease is not None
        task = _BlockCandidateAccountingTask(
            candidate=candidate,
            node_submission=_BlockCandidateNodeSubmission(
                attempted=True,
                result=None,
            ),
            disposition_lease=lease,
        )
        assert server._enqueue_block_accounting_task(task)
        return task

    def _drain_accounting_lane(
        self,
        fixture: CollapseFixture,
        *,
        before_stop: Any = None,
    ) -> None:
        """Run the shipped accounting loop until its queue is drained.

        Bounded: the lane either finishes the queued task quickly or the
        deadline expires and the assertions that follow report it.
        """
        service = fixture.service
        server = fixture.server
        service._ensure_block_accounting_state()
        thread = threading.Thread(target=service.block_accounting_loop, daemon=True)
        with redirect_stdout(StringIO()):
            thread.start()
            try:
                if before_stop is not None:
                    before_stop()
                deadline = time.monotonic() + self.WAIT_SECONDS
                while (
                    service._block_accounting_queue.unfinished_tasks
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.01)
            finally:
                server.stop_event.set()
                thread.join(self.WAIT_SECONDS)
        self.assertFalse(thread.is_alive())
        self.assertEqual(service._block_accounting_queue.unfinished_tasks, 0)

    def test_an_accounting_failure_hands_its_candidate_off_without_a_gap(
        self,
    ) -> None:
        """The lane's unexpected-failure retry leaves no unnamed instant.

        ``_run_block_accounting_task`` owns the task's disposition lease and
        releases it in its ``finally``; ``block_accounting_loop`` used to
        retain the candidate only afterwards, in its own catch.  Between
        the release and that retain the candidate was the loop's local
        variable and nothing named its hash -- the dequeue gap above in its
        accounting-lane form.  The lane now retains while it still holds
        the disposition, which routes the candidate into the accounting
        owner's deferred holder; the ``finally`` then releases the lease
        and merges that holder into the retry slot under
        ``coordinator.lock``, exactly as it always did for a writer-side
        retain.  The loop's catch sees the task already retained and does
        not retain it again.

        The barrier parks the lane the instant its lease release returns,
        applies eviction pressure, and proves the fence survives; afterwards
        the candidate sits in the retry slot exactly once, with one retry
        counted and its floor holder intact.
        """
        target = _hash(1)
        fixture = CollapseFixture()
        service, server = fixture.service, fixture.server
        candidate = fixture.seed([target], credit_share_on_accept=True)[0]
        server._ensure_share_writer_service().adopt_pending_share(
            candidate.pending_share
        )
        self.assertEqual(len(fixture.floor()), 1)
        # The fence is the oldest entry and the registry sits one over its
        # bound: the next recorded outcome evicts the oldest unpinned hash.
        server._record_block_candidate_terminal_outcome(target, accepted=False)
        self._fill(fixture, self.CAP)
        writer_calls = [0]

        def exploding_writer(
            candidate_arg: Any,
            *,
            node_submission: Any,
            disposition_held: bool,
        ) -> bool:
            writer_calls[0] += 1
            raise RuntimeError("accounting writer exploded")

        server._call_block_candidate_writer = exploding_writer
        task = self._accounting_task(fixture, target, candidate)
        # Park the lane the instant its lease release returns.
        in_gap = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)
        original_release = service._release_block_candidate_disposition

        def release_then_park(lease: Any) -> None:
            original_release(lease)
            if lease.block_hash == target:
                in_gap.set()
                if not release.wait(self.WAIT_SECONDS):
                    raise AssertionError("the parked accounting lane was never released")

        service._release_block_candidate_disposition = release_then_park

        def in_the_gap() -> None:
            try:
                self.assertTrue(in_gap.wait(self.WAIT_SECONDS))
                # The old gap: the lease is gone and the loop's catch has
                # not run; the candidate is a local variable of the lane.
                with service._block_candidate_disposition_registry_lock:
                    self.assertNotIn(
                        target,
                        service._block_candidate_disposition_flights,
                    )
                self._record(fixture, 64)
                # The fence survived the pressure; history did not.
                self.assertIs(
                    server._block_candidate_terminal_outcome(target),
                    False,
                )
                self.assertEqual(
                    len(service._block_candidate_terminal_outcomes),
                    self.CAP,
                )
                # ...because the lane retained into the deferred holder
                # before it let go of the lease.
                with server.lock:
                    self.assertIs(
                        service._block_accounting_deferred_retry_candidate,
                        candidate,
                    )
            finally:
                # A failed assertion must not leave the lane parked for the
                # whole ceiling before the test can report it.
                release.set()

        self._drain_accounting_lane(fixture, before_stop=in_the_gap)
        self.assertEqual(writer_calls[0], 1)
        self.assertTrue(task.retained_for_retry.is_set())
        # Handed off exactly once: in the retry slot, the deferred holder
        # cleared, one retry counted, one floor holder, nothing offered.
        with server.lock:
            self.assertIs(service.retry_candidate, candidate)
            self.assertIsNone(service._block_accounting_deferred_retry_candidate)
            self.assertFalse(
                getattr(service, "_block_accounting_holds_disposition", False)
            )
            self.assertEqual(service.retries, 1)
            self.assertIn(target, service._outstanding_block_candidate_hashes)
        self.assertEqual(len(fixture.floor()), 1)
        with service._block_candidate_disposition_registry_lock:
            self.assertEqual(service._block_candidate_disposition_flights, {})
        self.assertIs(server._block_candidate_terminal_outcome(target), False)
        offers = [
            method for method, _ in fixture.rpc.calls if method == "submitblock"
        ]
        self.assertEqual(offers, [])

    def test_a_failure_ahead_of_the_accounting_lane_is_still_retained_once(
        self,
    ) -> None:
        """The loop's catch still retains when the lane itself never ran.

        The lane's own exception path marks the task once it has retained.
        A failure that fires before the lane can -- here the coordinator
        delegate raising ahead of it -- leaves the mark unset, and the loop
        retains the candidate exactly as it always did: once.
        """
        target = _hash(1)
        fixture = CollapseFixture()
        service, server = fixture.service, fixture.server
        candidate = fixture.seed([target])[0]
        task = self._accounting_task(fixture, target, candidate)
        # The lane never runs, so nothing releases the task's lease; the
        # test owns it from here.
        self.addCleanup(
            server._release_block_candidate_disposition,
            task.disposition_lease,
        )

        def delegate_explodes(task_arg: Any) -> None:
            raise RuntimeError("delegate exploded")

        server._run_block_accounting_task = delegate_explodes
        self._drain_accounting_lane(fixture)
        self.assertFalse(task.retained_for_retry.is_set())
        with server.lock:
            self.assertIs(service.retry_candidate, candidate)
            self.assertIsNone(
                getattr(service, "_block_accounting_deferred_retry_candidate", None)
            )
            self.assertEqual(service.retries, 1)


class BlockCandidateAbandonmentDedupBoundTests(unittest.TestCase):
    """The counted-abandonment dedup set retires with the fence it guards.

    ``_counted_block_candidate_abandonments`` exists because one hash can
    reach the terminal accounting path more than once: a collapse whose
    cleanup failed retries every owed step, and a finalize failure freezes a
    false finalize-only disposition that replays.  Abandonment metrics count
    candidates, not attempts, so the key deduplicates them.

    Nothing ever retired those keys.  One key per candidate the process had
    ever abandoned is the same unbounded history the terminal-outcome
    registry was just bounded away from, and a collapsed storm writes them
    3,120 at a time.  The key is now retired inside the eviction pass, in
    the same ``coordinator.lock`` critical section that has already proved
    the hash unpinned by every live and cleanup-owing lane, and only for a
    hash whose fence that pass actually drops.

    Retiring one early is the dangerous half: a lane still owed its
    accounting would count its candidate a second time and the abandoned
    series would exceed the candidates it describes.  Both writer orderings
    have to survive that -- the direct finalize counts before it publishes
    its terminal outcome, the collapse publishes first and counts during
    cleanup -- so these tests own the bound and the pins in both.
    """

    CAP = MAX_BLOCK_CANDIDATE_TERMINAL_OUTCOMES
    REASON = PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON
    STALE_CLASS = PRISM_BLOCK_CANDIDATE_COLLAPSE_STALE_JOB_CLASS
    STORM = 3_120
    STORMS = 4
    # Well clear of the fixture's own hashes and of the pressure histories.
    STORM_START = 1 << 60

    # -- harness -----------------------------------------------------------

    def _count(self, fixture: CollapseFixture, block_hash: str) -> None:
        """One committed abandonment, with the outcome the collapse builds."""
        fixture.server._record_committed_block_candidate_abandonment(
            block_hash,
            SimpleNamespace(reason=self.REASON, stale_job_class=self.STALE_CLASS),
        )

    @staticmethod
    def _fence(fixture: CollapseFixture, block_hash: str) -> None:
        fixture.server._record_block_candidate_terminal_outcome(
            block_hash,
            accepted=False,
        )

    def _abandon(
        self,
        fixture: CollapseFixture,
        block_hash: str,
        *,
        terminal_first: bool,
    ) -> None:
        """Drive one terminal abandonment in one of the two shipped orders.

        ``terminal_first`` is the collapse: the fenced write publishes the
        terminal outcome and the cleanup's accounting step follows it.  The
        other order is ``finalize``, which counts as soon as the durable
        outbox mark returns and records the process-local outcome at its
        tail.
        """
        if terminal_first:
            self._fence(fixture, block_hash)
            self._count(fixture, block_hash)
        else:
            self._count(fixture, block_hash)
            self._fence(fixture, block_hash)

    @staticmethod
    def _counted(fixture: CollapseFixture) -> set[str]:
        with fixture.server.lock:
            return set(fixture.service._counted_block_candidate_abandonments)

    @staticmethod
    def _outcomes(fixture: CollapseFixture) -> set[str]:
        with fixture.server.lock:
            return set(fixture.service._block_candidate_terminal_outcomes)

    def _pressure(self, fixture: CollapseFixture, *, round_index: int = 0) -> None:
        """Push the registry past its bound through the shipped writer.

        The history is uncounted, so its own eviction retires nothing; it is
        here purely to make the pass reach whatever the test is protecting.
        Each round uses a disjoint range so a second round is real pressure
        rather than a re-stamp of the first.
        """
        offset = round_index * (1 << 18)
        with fixture.server.lock:
            outcomes = fixture.service._block_candidate_terminal_outcomes
            for block_hash in _history_hashes(self.CAP, start=(1 << 20) + offset):
                outcomes[block_hash] = False
        for block_hash in _history_hashes(64, start=(1 << 40) + offset):
            self._fence(fixture, block_hash)

    # -- the bound ---------------------------------------------------------

    def test_repeated_storms_do_not_grow_the_dedup_history(self) -> None:
        """Storm after storm, the dedup set tracks the fences, not the past."""
        for terminal_first in (False, True):
            with self.subTest(terminal_first=terminal_first):
                fixture = CollapseFixture()
                sizes: list[int] = []
                for index in range(self.STORMS):
                    start = self.STORM_START + index * self.STORM
                    for block_hash in _history_hashes(self.STORM, start=start):
                        self._abandon(
                            fixture,
                            block_hash,
                            terminal_first=terminal_first,
                        )
                    sizes.append(len(self._counted(fixture)))
                # It stops growing where the fences do, and holds there
                # however many further storms arrive.
                self.assertEqual(sizes[-1], self.CAP)
                self.assertEqual(sizes[-1], sizes[-2])
                self.assertLessEqual(max(sizes), self.CAP)
                self.assertEqual(self._counted(fixture), self._outcomes(fixture))
                # Bounding it cost no accuracy: every candidate counted once.
                counts = fixture.server.block_candidate_abandoned_counts
                self.assertEqual(counts[self.REASON], self.STORM * self.STORMS)
                self.assertEqual(
                    fixture.server.stale_job_abandon_counts[self.STALE_CLASS],
                    self.STORM * self.STORMS,
                )

    def test_a_collapse_page_retires_its_keys_when_its_fences_age_out(
        self,
    ) -> None:
        """The shipped apply, not a hand-rolled ordering, ends up bounded."""
        fixture = CollapseFixture()
        page = [_hash(index) for index in range(1, 5)]
        fixture.seed(page)
        self.assertEqual(_selected(fixture), set(page))
        self.assertEqual(fixture.cleanup_backlog(), {})
        self.assertEqual(
            fixture.server.block_candidate_abandoned_counts[self.REASON],
            len(page),
        )
        self.assertEqual(self._counted(fixture), set(page))
        self._pressure(fixture)
        for block_hash in page:
            self.assertIsNone(
                fixture.server._block_candidate_terminal_outcome(block_hash)
            )
        self.assertEqual(self._counted(fixture), set())

    def test_the_key_retires_with_its_fence_and_not_before(self) -> None:
        """A pass that evicts nothing retires nothing."""
        fixture = CollapseFixture()
        target = _hash(1)
        self._abandon(fixture, target, terminal_first=True)
        with fixture.server.lock:
            self.assertEqual(
                fixture.service._bound_block_candidate_terminal_outcomes(),
                0,
            )
        self.assertEqual(self._counted(fixture), {target})
        self._pressure(fixture)
        self.assertIsNone(fixture.server._block_candidate_terminal_outcome(target))
        self.assertEqual(self._counted(fixture), set())

    # -- the two orderings -------------------------------------------------

    def test_both_writer_orderings_converge_on_the_same_state(self) -> None:
        """Count-then-fence and fence-then-count are indistinguishable."""
        states = []
        for terminal_first in (False, True):
            fixture = CollapseFixture()
            history = _history_hashes(self.CAP + 64, start=self.STORM_START)
            for block_hash in history:
                self._abandon(fixture, block_hash, terminal_first=terminal_first)
            states.append(
                (
                    self._counted(fixture),
                    self._outcomes(fixture),
                    dict(fixture.server.block_candidate_abandoned_counts),
                    dict(fixture.server.stale_job_abandon_counts),
                )
            )
        self.assertEqual(states[0], states[1])
        counted, outcomes, counts, stale = states[0]
        self.assertEqual(counted, outcomes)
        self.assertEqual(len(counted), self.CAP)
        self.assertEqual(counts[self.REASON], self.CAP + 64)
        self.assertEqual(stale[self.STALE_CLASS], self.CAP + 64)

    def test_a_repeated_disposition_is_still_counted_once(self) -> None:
        """The dedup the key exists for survives in both orderings."""
        for terminal_first in (False, True):
            with self.subTest(terminal_first=terminal_first):
                fixture = CollapseFixture()
                target = _hash(1)
                for _attempt in range(3):
                    self._abandon(fixture, target, terminal_first=terminal_first)
                self.assertEqual(
                    fixture.server.block_candidate_abandoned_counts[self.REASON],
                    1,
                )
                self.assertEqual(self._counted(fixture), {target})

    def test_a_false_finalize_failure_stays_counted_before_any_outcome(
        self,
    ) -> None:
        """The ambiguous branch counts with no fence to retire it by.

        ``finalize`` freezes a false finalize-only disposition when the
        durable abandonment mark raises: it counts the candidate and leaves
        the hash in ``finalize_retries`` without ever recording a terminal
        outcome.  There is nothing for the eviction pass to drop, and the
        registry that pins it is itself bounded, so the key simply waits for
        the replay that publishes its outcome.
        """
        fixture = CollapseFixture()
        target = _hash(1)
        self._count(fixture, target)
        with fixture.server.lock:
            fixture.service.finalize_retries[target] = (False, "boom")
        self._pressure(fixture)
        self.assertIsNone(fixture.server._block_candidate_terminal_outcome(target))
        self.assertEqual(self._counted(fixture), {target})
        # The paced finalize-only replay re-enters the same accounting call.
        self._count(fixture, target)
        self.assertEqual(
            fixture.server.block_candidate_abandoned_counts[self.REASON],
            1,
        )
        # Its retry succeeds: the tail clears the registry and publishes the
        # outcome, and only then can the key age out with it.
        with fixture.server.lock:
            fixture.service.finalize_retries.pop(target, None)
        self._fence(fixture, target)
        self._pressure(fixture, round_index=1)
        self.assertIsNone(fixture.server._block_candidate_terminal_outcome(target))
        self.assertEqual(self._counted(fixture), set())

    # -- the pins ----------------------------------------------------------

    def test_pressure_cannot_forget_a_pinned_hash_and_double_count_it(
        self,
    ) -> None:
        """Every lane that can still run the accounting keeps its key."""
        target = _hash(1)

        def live_queue(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.service.candidate_queue.put_nowait(candidate)

        def replay_queue(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.service._block_replay_candidate_queue.put_nowait(candidate)

        def cleanup_owed(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.service._defer_collapsed_candidate_cleanup(
                target,
                frozenset({"abandonment-accounting"}),
            )

        def finalize_retry(fixture: CollapseFixture, candidate: Any) -> None:
            with fixture.server.lock:
                fixture.service.finalize_retries[target] = (False, "boom")

        def disposition_flight(fixture: CollapseFixture, candidate: Any) -> None:
            lease = fixture.server._claim_block_candidate_disposition(
                target,
                blocking=False,
            )
            self.assertIsNotNone(lease)
            self.addCleanup(
                fixture.server._release_block_candidate_disposition,
                lease,
            )

        def outstanding(fixture: CollapseFixture, candidate: Any) -> None:
            fixture.server._register_outstanding_block_candidate(target)

        def retry_holder(fixture: CollapseFixture, candidate: Any) -> None:
            with fixture.server.lock:
                fixture.service.retry_candidate = candidate

        lanes = {
            "candidate_queue": live_queue,
            "_block_replay_candidate_queue": replay_queue,
            "_block_candidate_collapse_cleanup_retries": cleanup_owed,
            "finalize_retries": finalize_retry,
            "_block_candidate_disposition_flights": disposition_flight,
            "_outstanding_block_candidate_hashes": outstanding,
            "retry_candidate": retry_holder,
        }
        for terminal_first in (False, True):
            for name, install in lanes.items():
                with self.subTest(lane=name, terminal_first=terminal_first):
                    fixture = CollapseFixture()
                    candidate = fixture.seed([target])[0]
                    # The outcome is recorded before the lane is populated:
                    # recording clears the reservation, replay, and waiting
                    # markers for its own hash by design.
                    self._abandon(fixture, target, terminal_first=terminal_first)
                    with redirect_stdout(StringIO()):
                        install(fixture, candidate)
                    self._pressure(fixture)
                    self.assertEqual(
                        len(fixture.service._block_candidate_terminal_outcomes),
                        self.CAP,
                    )
                    self.assertIs(
                        fixture.server._block_candidate_terminal_outcome(target),
                        False,
                    )
                    self.assertIn(target, self._counted(fixture))
                    # The lane now runs the accounting it was still owed.
                    # The surviving key is the only thing standing between
                    # that and a second count of one candidate.
                    self._count(fixture, target)
                    self.assertEqual(
                        fixture.server.block_candidate_abandoned_counts[
                            self.REASON
                        ],
                        1,
                    )


class BlockCandidateCollapseEnumerationTests(unittest.TestCase):
    """Both replay enumeration shapes collapse before they adopt."""

    def _adopted(self, fixture: CollapseFixture) -> set[str]:
        service = fixture.service
        service._ensure_block_replay_state()
        with fixture.server.lock:
            return set(service._block_replay_inflight_hashes)

    def test_keyset_pagination_collapses_then_adopts_the_remainder(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(index) for index in range(1, 6)])
        fixture.seed([_hash(index) for index in range(6, 9)], parent=OTHER_PARENT)
        fixture.rpc.tip = OTHER_PARENT
        queued, log = fixture.replay(enumeration_owed=True)
        self.assertIn("pending block candidate enumeration page=1", log)
        self.assertEqual(fixture.pending(), {_hash(6), _hash(7), _hash(8)})
        self.assertEqual(
            self._adopted(fixture),
            {_hash(6), _hash(7), _hash(8)},
        )
        self.assertEqual(queued, 3)

    def test_legacy_window_collapses_then_adopts_the_remainder(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(index) for index in range(1, 6)])
        fixture.seed([_hash(index) for index in range(6, 9)], parent=OTHER_PARENT)
        fixture.rpc.tip = OTHER_PARENT
        # Enumeration is not owed, so replay_pending takes the widening
        # window even against a cursor-capable ledger.
        queued, log = fixture.replay(enumeration_owed=False)
        self.assertNotIn("enumeration page=", log)
        self.assertEqual(fixture.pending(), {_hash(6), _hash(7), _hash(8)})
        self.assertEqual(
            self._adopted(fixture),
            {_hash(6), _hash(7), _hash(8)},
        )
        self.assertEqual(queued, 3)

    def test_cursorless_ledger_collapses_then_adopts_the_remainder(self) -> None:
        fixture = CollapseFixture(ledger=WindowCollapseLedger())
        fixture.seed([_hash(index) for index in range(1, 6)])
        fixture.seed([_hash(index) for index in range(6, 9)], parent=OTHER_PARENT)
        fixture.rpc.tip = OTHER_PARENT
        queued, log = fixture.replay(enumeration_owed=True)
        self.assertNotIn("enumeration page=", log)
        self.assertEqual(fixture.pending(), {_hash(6), _hash(7), _hash(8)})
        self.assertEqual(self._adopted(fixture), {_hash(6), _hash(7), _hash(8)})
        self.assertEqual(queued, 3)

    def test_a_collapsed_row_is_never_adopted_or_re_armed(self) -> None:
        """A won row's fetched payload is a stale copy of a terminal row."""
        for owed in (True, False):
            with self.subTest(enumeration_owed=owed):
                fixture = CollapseFixture()
                fixture.seed([_hash(1), _hash(2)])
                decoded: list[str] = []
                original = fixture.server.block_candidate_from_intent

                def record(intent):
                    decoded.append(str(intent["block_hash_hex"]).lower())
                    return original(intent)

                fixture.server.block_candidate_from_intent = record
                queued, _log = fixture.replay(enumeration_owed=owed)
                self.assertEqual(queued, 0)
                self.assertEqual(decoded, [])
                self.assertEqual(self._adopted(fixture), set())
                with fixture.server._accepted_block_payout_preview_condition:
                    self.assertEqual(
                        fixture.server._accepted_block_payout_previews,
                        {},
                    )

    def test_a_partly_returned_request_still_adopts_every_other_row(self) -> None:
        for owed in (True, False):
            with self.subTest(enumeration_owed=owed):
                fixture = CollapseFixture()
                fixture.seed([_hash(1), _hash(2), _hash(3)])

                def partial(hashes: tuple[str, ...], error: str) -> tuple[str, ...]:
                    won = tuple(value for value in hashes if value != _hash(2))
                    return fixture.ledger.inner.mark_block_candidates_abandoned(
                        block_hashes=won,
                        error=error,
                    )

                fixture.ledger.abandon_hook = partial
                queued, _log = fixture.replay(enumeration_owed=owed)
                self.assertEqual(fixture.pending(), {_hash(2)})
                self.assertEqual(self._adopted(fixture), {_hash(2)})
                self.assertEqual(queued, 1)

    def test_a_fail_closed_page_adopts_every_row(self) -> None:
        for owed in (True, False):
            with self.subTest(enumeration_owed=owed):
                fixture = CollapseFixture()
                fixture.seed([_hash(1), _hash(2), _hash(3)])
                fixture.rpc.failures.add("getblockcount")
                queued, _log = fixture.replay(enumeration_owed=owed)
                self.assertEqual(
                    fixture.pending(),
                    {_hash(1), _hash(2), _hash(3)},
                )
                self.assertEqual(
                    self._adopted(fixture),
                    {_hash(1), _hash(2), _hash(3)},
                )
                self.assertEqual(queued, 3)

    def test_a_failed_write_adopts_every_row(self) -> None:
        for owed in (True, False):
            with self.subTest(enumeration_owed=owed):
                fixture = CollapseFixture()
                fixture.seed([_hash(1), _hash(2)])
                fixture.ledger.abandon_hook = lambda hashes, error: (
                    (_ for _ in ()).throw(RuntimeError("ledger down"))
                )
                queued, _log = fixture.replay(enumeration_owed=owed)
                self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
                self.assertEqual(self._adopted(fixture), {_hash(1), _hash(2)})
                self.assertEqual(queued, 2)

    def test_a_timed_out_write_that_lands_late_preserves_then_converges(self) -> None:
        """The caller's deadline expires while the batch write is still out.

        This drives the real submitter ledger-call seam rather than raising
        from the ledger hook: the batch write runs on the bounded worker
        ``_run_block_submitter_ledger_call`` spawns, the coordinator-side
        wait expires first and raises ``BlockSubmitterDatabaseTimeout``, and
        the worker completes the write afterwards. The apply learns nothing
        from a call that never answered it, so the whole page fails closed
        and stays adoptable, and the write that lands later is still fenced
        by the durable pool-block fact rather than by what the selector read
        a page earlier. Whatever that write transitions, no cleanup and no
        payout authority may follow from a hash the call did not return
        synchronously.
        """
        wait_seconds = 10.0
        fixture = CollapseFixture()
        abandoned_late = _hash(1)
        landed_meanwhile = _hash(2)
        fixture.seed([abandoned_late, landed_meanwhile], credit_share_on_accept=True)
        # Short enough to keep the test quick, wide enough that the page
        # read ahead of it is never the call that expires.
        fixture.server.block_submit_db_timeout_seconds = 0.25
        started = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)

        def stall_then_write(hashes: tuple[str, ...], error: str) -> tuple[str, ...]:
            # Deterministic: the write cannot land before the deadline
            # because only the test releases it, and it is released only
            # after the timed-out apply has returned.
            started.set()
            if not release.wait(wait_seconds):
                raise AssertionError("the stalled ledger call was never released")
            return fixture.ledger.inner.mark_block_candidates_abandoned(
                block_hashes=hashes,
                error=error,
            )

        fixture.ledger.abandon_hook = stall_then_write
        queued, log = fixture.replay(enumeration_owed=False)
        self.assertTrue(started.is_set())
        self.assertIn("ledger phase timed out phase=collapse-superseded", log)
        # Failed closed: every row is preserved, adopted, and left to the
        # per-row path exactly as an outright write failure leaves it.
        self.assertEqual(queued, 2)
        self.assertEqual(fixture.pending(), {abandoned_late, landed_meanwhile})
        self.assertEqual(self._adopted(fixture), {abandoned_late, landed_meanwhile})
        counts = fixture.counts()
        self.assertEqual(counts["fail_closed"], 2)
        self.assertEqual(counts["abandoned"], 0)
        self.assertEqual(counts["write_lost"], 0)
        self.assertEqual(counts["cleanup_failed"], 0)
        self.assertEqual(
            fixture.service.block_ledger_call_class_metrics()["fast"][
                "timeouts_total"
            ],
            1,
        )
        with fixture.service._block_candidate_disposition_registry_lock:
            self.assertEqual(fixture.service._block_candidate_disposition_flights, {})
        adopted_floor = set(fixture.floor())
        self.assertEqual(len(adopted_floor), 2)
        with fixture.service._block_submitter_ledger_calls_lock:
            stalled = [
                call
                for key, call in fixture.service._block_submitter_ledger_calls.items()
                if key[0] == "collapse-superseded"
            ]
        self.assertEqual(len(stalled), 1)
        # One of the two rows really did land while the call was out. Only
        # the durable pool-block row proves it, and the fenced write re-asks
        # for that fact itself.
        fixture.ledger.inner.persist_accepted_block(
            block_hash=landed_meanwhile,
            block_height=DECIDED_HEIGHT,
            parent_hash=STORM_PARENT,
            final_bundle={},
            audit_report={},
        )
        release.set()
        self.assertTrue(stalled[0].done.wait(wait_seconds))
        self.assertEqual(len(fixture.ledger.abandon_calls), 1)
        self.assertEqual(
            set(fixture.ledger.abandon_calls[0][0]),
            {abandoned_late, landed_meanwhile},
        )
        self.assertEqual(set(stalled[0].result), {abandoned_late})
        self.assertEqual(fixture.pending(), {landed_meanwhile})
        # Nothing was cleaned up for the hash the call never returned to
        # anybody: its in-memory state still belongs to the per-row path.
        server = fixture.server
        with server.lock:
            self.assertEqual(fixture.service._block_candidate_terminal_outcomes, {})
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertEqual(set(fixture.floor()), adopted_floor)
        # A later enumeration is idempotent: the terminal row is gone from
        # the outbox, the landed one is excluded by its pool-block row, and
        # no second fenced write is issued.
        second_queued, _second_log = fixture.replay(enumeration_owed=True)
        self.assertEqual(second_queued, 0)
        self.assertEqual(len(fixture.ledger.abandon_calls), 1)
        self.assertEqual(fixture.pending(), {landed_meanwhile})
        self.assertEqual(self._adopted(fixture), {abandoned_late, landed_meanwhile})
        counts = fixture.counts()
        self.assertEqual(counts["selected"], 2)
        self.assertEqual(counts["abandoned"], 0)
        self.assertEqual(counts["cleanup_failed"], 0)
        # The barrier replay armed is still only a barrier: no landed
        # transition and no published preview were invented for a row whose
        # fate the coordinator never observed.
        with server._accepted_block_payout_preview_condition:
            previews = dict(server._accepted_block_payout_previews)
            self.assertEqual(
                dict(server._invalidated_accepted_block_payout_previews),
                {},
            )
        self.assertEqual(set(previews), {abandoned_late, landed_meanwhile})
        self.assertFalse(previews[abandoned_late].landed)
        self.assertIsNone(previews[abandoned_late].preview)
        self.assertFalse(
            server._accepted_block_payout_transition_landed(abandoned_late)
        )

    def test_repeated_late_collapse_writes_leave_no_ledger_calls(self) -> None:
        """A page key that never recurs must still leave the registry.

        Each fenced write is keyed by the exact page of hashes it abandons,
        so the write that lands after the caller's deadline is the very
        thing that empties that page out of the outbox: nothing invokes that
        key again to observe the completion and drop the entry. Several such
        pages in a row must leave the registry back at empty, not one
        completed call per page each pinning its whole hash tuple for the
        life of the process.
        """
        fixture = CollapseFixture()
        # Wide enough that the page read ahead of the write is never the
        # call that expires, short enough to keep the test quick.
        fixture.server.block_submit_db_timeout_seconds = 0.25
        rounds = 4
        page_keys: list[tuple[object, ...]] = []
        for round_index in range(rounds):
            fixture.seed(
                [_hash(2 * round_index + 1), _hash(2 * round_index + 2)]
            )
            release = threading.Event()
            self.addCleanup(release.set)

            def stall_then_write(
                hashes: tuple[str, ...],
                error: str,
                release: threading.Event = release,
            ) -> tuple[str, ...]:
                if not release.wait(10):
                    raise AssertionError(
                        "the stalled ledger call was never released"
                    )
                return fixture.ledger.inner.mark_block_candidates_abandoned(
                    block_hashes=hashes,
                    error=error,
                )

            fixture.ledger.abandon_hook = stall_then_write
            _retained, log = fixture.collapse()
            self.assertIn(
                "ledger phase timed out phase=collapse-superseded",
                log,
            )
            with fixture.service._block_submitter_ledger_calls_lock:
                stalled = [
                    key
                    for key, call in (
                        fixture.service._block_submitter_ledger_calls.items()
                    )
                    if not call.done.is_set()
                ]
            self.assertEqual(len(stalled), 1)
            page_keys.append(stalled[0])
            worker = next(
                thread
                for thread in threading.enumerate()
                if thread.name == "prism-block-ledger-collapse-superseded"
            )
            release.set()
            worker.join(10)
            self.assertFalse(worker.is_alive())
            # The write committed on the worker, and took its entry with it.
            with fixture.service._block_submitter_ledger_calls_lock:
                self.assertEqual(
                    dict(fixture.service._block_submitter_ledger_calls),
                    {},
                )

        # Every page really was a distinct key, and every delayed write
        # really did commit.
        self.assertEqual(len(fixture.ledger.abandon_calls), rounds)
        self.assertEqual(len(set(page_keys)), rounds)
        self.assertEqual(fixture.pending(), set())

    def test_pagination_still_completes_after_a_full_page_collapses(self) -> None:
        """Collapse must not break the short-page completeness proof."""
        fixture = CollapseFixture()
        total = MAX_BLOCK_REPLAY_ENUMERATION_ROWS + 7
        fixture.seed([_hash(index) for index in range(1, total + 1)])
        queued, log = fixture.replay(enumeration_owed=True)
        self.assertEqual(fixture.pending(), set())
        self.assertEqual(queued, 0)
        self.assertIn("enumeration page=2", log)
        self.assertFalse(fixture.server._block_replay_enumeration_owed())
        self.assertEqual(len(fixture.ledger.abandon_calls), 2)
        self.assertEqual(
            [len(call[0]) for call in fixture.ledger.abandon_calls],
            [MAX_BLOCK_REPLAY_ENUMERATION_ROWS, 7],
        )


class BlockCandidateCollapseObservabilityTests(unittest.TestCase):
    """Bounded logs and bounded, hash-free metrics."""

    def test_the_bulk_path_does_not_log_one_line_per_candidate(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(index) for index in range(1, 201)])
        _retained, log = fixture.collapse()
        lines = [line for line in log.splitlines() if line.strip()]
        self.assertLessEqual(len(lines), 4 + BLOCK_CANDIDATE_COLLAPSE_LOG_GROUPS)
        self.assertNotIn("block candidate abandoned reason=", log)
        summary = [line for line in lines if "collapsed superseded" in line]
        self.assertEqual(len(summary), 1)
        self.assertIn("considered=200", summary[0])
        self.assertIn("abandoned=200", summary[0])

    def test_group_lines_and_hash_samples_stay_bounded(self) -> None:
        fixture = CollapseFixture()
        groups = BLOCK_CANDIDATE_COLLAPSE_LOG_GROUPS + 3
        index = 1
        for group in range(groups):
            parent = f"{group:064x}"
            fixture.seed(
                [_hash(index + offset) for offset in range(5)],
                parent=parent,
            )
            index += 5
        _retained, log = fixture.collapse()
        group_lines = [
            line for line in log.splitlines() if "collapsed candidate group " in line
        ]
        self.assertEqual(len(group_lines), BLOCK_CANDIDATE_COLLAPSE_LOG_GROUPS)
        self.assertIn("collapsed candidate groups not shown count=3", log)
        for line in group_lines:
            sample = line.split("sample=", 1)[1].strip()
            self.assertLessEqual(
                len(sample.split(",")),
                BLOCK_CANDIDATE_COLLAPSE_LOG_SAMPLE_HASHES,
            )

    def test_fail_closed_logging_is_rate_limited(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1)])
        fixture.rpc.failures.add("getblockcount")
        _retained, first = fixture.collapse()
        _retained, second = fixture.collapse()
        self.assertIn("collapse failed", first)
        self.assertNotIn("collapse failed", second)
        self.assertEqual(fixture.counts()["fail_closed"], 2)

    def test_the_snapshot_carries_exactly_the_fixed_outcome_keys(self) -> None:
        fixture = CollapseFixture()
        self.assertEqual(
            tuple(fixture.counts()),
            PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES,
        )

    def test_metrics_render_bounded_hash_free_collapse_series(self) -> None:
        fixture = CollapseFixture()
        fixture.seed([_hash(1), _hash(2)])
        with redirect_stdout(StringIO()):
            fixture.collapse()
        lines = MetricsRenderer(fixture.server).block_submitter_metrics_lines()
        series = [
            line
            for line in lines
            if line.startswith("qbit_prism_block_candidate_collapse_total")
        ]
        self.assertEqual(len(series), len(PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES))
        rendered = "\n".join(lines)
        self.assertIn(
            'qbit_prism_block_candidate_collapse_total{outcome="abandoned"} 2',
            rendered,
        )
        self.assertIn(
            'qbit_prism_block_candidate_collapse_total{outcome="considered"} 2',
            rendered,
        )
        for forbidden in (_hash(1), _hash(2), STORM_PARENT, DECIDED_WINNER, "job-"):
            self.assertNotIn(forbidden, rendered)

    def test_per_row_diagnostics_survive_for_preserved_candidates(self) -> None:
        """A candidate the collapse declines still logs its own abandonment."""
        fixture = CollapseFixture()
        preserved = _hash(1)
        candidate = fixture.seed([preserved], parent=OTHER_PARENT)[0]
        fixture.rpc.tip = OTHER_PARENT
        fixture.collapse()
        self.assertEqual(fixture.pending(), {preserved})
        buffer = StringIO()
        with redirect_stdout(buffer):
            fixture.service.record_abandoned(
                PRISM_REJECTION_STALE_JOB,
                "tip moved before submit: deadbeef",
                block_hash=preserved,
                worker="miner-a",
                expected_height=DECIDED_HEIGHT,
                stale_job_class="tip_moved",
            )
        self.assertIn("block candidate abandoned reason=stale-job", buffer.getvalue())
        self.assertIs(candidate.credit_share_on_accept, False)


class BlockCandidateCollapseStormTests(unittest.TestCase):
    """Compare the selector against the shipped per-row oracle at storm sizes."""

    def _oracle(self, candidates: int, view: str) -> tuple[set[str], str]:
        rig = CandidateStormRig(candidates=candidates)
        rig.seed_live()
        if view == "restart":
            rig.restart_and_enumerate()
        server = rig.restarted_server if view == "restart" else rig.live_server
        # ``drain_per_row`` suppresses the collapse itself, so this stays
        # the shipped per-row oracle without any patching here.
        winner = rig.decide_height()
        report = rig.drain_per_row(server, stop_before_hash=winner)
        return set(report.abandoned_hashes), winner

    def _selector(
        self,
        candidates: int,
        view: str,
        *,
        perturbation: str | None = None,
        target: str | None = None,
    ) -> tuple[set[str], Any, str]:
        rig = CandidateStormRig(candidates=candidates)
        rig.seed_live()
        if view == "restart":
            rig.restart_and_enumerate()
        server = rig.restarted_server if view == "restart" else rig.live_server
        winner = rig.decide_height()
        if perturbation is not None:
            with redirect_stdout(StringIO()):
                rig.perturb(server, perturbation, str(target))
        before = {
            str(row["block_hash"]).lower()
            for row in rig.ledger.pending_block_candidate_rows(limit=candidates + 1)
        }
        server._note_block_replay_enumeration_owed()
        with redirect_stdout(StringIO()):
            server.replay_pending_block_candidates()
        after = {
            str(row["block_hash"]).lower()
            for row in rig.ledger.pending_block_candidate_rows(limit=candidates + 1)
        }
        return before - after, server, winner

    def _compare(self, candidates: int, view: str) -> set[str]:
        oracle, winner = self._oracle(candidates, view)
        selected, server, winner_again = self._selector(candidates, view)
        self.assertEqual(winner, winner_again)
        self.assertEqual(
            selected - oracle,
            set(),
            f"selector abandoned {len(selected - oracle)} hash(es) the shipped "
            f"per-row path refused ({candidates} candidates, {view} view)",
        )
        # The baseline storm is the exact-agreement case: every sibling of the
        # decided block is superseded and carries no offer evidence.
        self.assertEqual(oracle - selected, set())
        self.assertNotIn(winner, selected)
        self.assertEqual(
            server.block_candidate_abandoned_counts,
            {PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON: len(selected)},
        )
        return selected

    def test_efficacy_and_agreement_at_one_hundred_candidates(self) -> None:
        for view in ("restart", "live"):
            with self.subTest(view=view):
                selected = self._compare(100, view)
                # Every candidate but the decided winner; the storm's whole
                # point is that the shipped path pays 99 node offers for it.
                self.assertGreaterEqual(len(selected), 90)
                self.assertEqual(len(selected), 99)

    def test_agreement_at_the_observed_testnet_storm(self) -> None:
        for view in ("restart", "live"):
            with self.subTest(view=view):
                selected = self._compare(3_120, view)
                self.assertEqual(len(selected), 3_119)

    def test_the_storm_costs_a_bounded_number_of_chain_reads(self) -> None:
        selected, server, _winner = self._selector(3_120, "restart")
        self.assertEqual(len(selected), 3_119)
        pages = -(-3_120 // MAX_BLOCK_REPLAY_ENUMERATION_ROWS)
        calls = server.rpc.calls
        # Two tip reads and two active-block reads per page (selection plus
        # revalidation), one tip-height read and one occupant header per page.
        self.assertEqual(calls["getbestblockhash"], 2 * pages)
        self.assertEqual(calls["getblockcount"], pages)
        self.assertEqual(calls["getblockhash"], 2 * pages)
        self.assertEqual(calls["getblockheader"], pages)
        self.assertNotIn("submitblock", calls)

    def test_no_perturbation_ever_produces_a_selector_only_hash(self) -> None:
        candidates = 40
        target = _hash(7)
        for view in ("restart", "live"):
            for kind in PERTURBATIONS:
                with self.subTest(view=view, perturbation=kind):
                    rig = CandidateStormRig(candidates=candidates)
                    rig.seed_live()
                    if view == "restart":
                        rig.restart_and_enumerate()
                    server = (
                        rig.restarted_server
                        if view == "restart"
                        else rig.live_server
                    )
                    winner = rig.decide_height()
                    with redirect_stdout(StringIO()):
                        undo = rig.perturb(server, kind, target)
                    report = rig.drain_per_row(server, stop_before_hash=winner)
                    oracle = set(report.abandoned_hashes)
                    if undo is not None:
                        undo()
                    selected, _server, _winner = self._selector(
                        candidates,
                        view,
                        perturbation=kind,
                        target=target,
                    )
                    self.assertEqual(
                        selected - oracle,
                        set(),
                        f"{kind} produced a selector-only hash in the {view} view",
                    )
                    self.assertNotIn(target, selected)
                    self.assertLessEqual(len(oracle - selected), 1)


if __name__ == "__main__":
    unittest.main()
