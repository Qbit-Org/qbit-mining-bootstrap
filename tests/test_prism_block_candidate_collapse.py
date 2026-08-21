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
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.block_candidates import (  # noqa: E402
    BLOCK_CANDIDATE_COLLAPSE_LOG_FAILURES,
    BLOCK_CANDIDATE_COLLAPSE_LOG_GROUPS,
    BLOCK_CANDIDATE_COLLAPSE_LOG_SAMPLE_HASHES,
    MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
    MAX_PENDING_BLOCK_CANDIDATES,
    PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES,
    PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON,
    PRISM_BLOCK_CANDIDATE_COLLAPSE_STALE_JOB_CLASS,
    PRISM_STALE_JOB_ABANDON_CLASSES,
    _BlockCandidateNodeSubmission,
)
from lab.prism.metrics import MetricsRenderer  # noqa: E402
from lab.prism.share_ledger import (  # noqa: E402
    PendingShare,
    SingleWriterShareLedger,
)
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


def _hash(index: int) -> str:
    return f"{index:064x}"


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
        difficulty: dict[str, float] | None = None,
    ) -> None:
        self.tip = tip.lower()
        self.tip_height = int(tip_height)
        self.active = {
            int(height): str(value).lower()
            for height, value in (active or {DECIDED_HEIGHT: DECIDED_WINNER}).items()
        }
        self.difficulty = {
            str(key).lower(): float(value)
            for key, value in (difficulty or {}).items()
        }
        self.default_difficulty = 1.0
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
            return {
                "hash": block_hash,
                "height": self.tip_height,
                "confirmations": 1,
                "difficulty": self.difficulty.get(
                    block_hash,
                    self.default_difficulty,
                ),
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

    # -- observation -------------------------------------------------------

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
                difficulty={DECIDED_WINNER: 1.0},
            )
        )
        fixture.seed([_hash(1)], network_difficulty=8)
        fixture.seed([_hash(2)], network_difficulty=1)
        self.assertEqual(_selected(fixture), {_hash(2)})

    def test_equal_work_occupant_still_supersedes(self) -> None:
        fixture = CollapseFixture(
            rpc=CollapseChainRpc(
                tip=DECIDED_WINNER,
                tip_height=DECIDED_HEIGHT,
                active={DECIDED_HEIGHT: DECIDED_WINNER},
                difficulty={DECIDED_WINNER: 4.0},
            )
        )
        fixture.seed([_hash(1)], network_difficulty=4)
        self.assertEqual(_selected(fixture), {_hash(1)})

    def test_unusable_header_difficulty_fails_the_page_closed(self) -> None:
        for value in (
            None,
            "n/a",
            float("nan"),
            True,
            float("inf"),
            0,
            -1,
            10**10_000,
        ):
            with self.subTest(difficulty=value):
                fixture = CollapseFixture()
                fixture.seed([_hash(1), _hash(2)])
                fixture.rpc.results["getblockheader"] = {
                    "hash": DECIDED_WINNER,
                    "difficulty": value,
                }
                fixture.collapse()
                self.assertEqual(fixture.pending(), {_hash(1), _hash(2)})
                self.assertEqual(fixture.counts()["fail_closed"], 2)

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

        def record(*, ignore_leases=frozenset()):
            seen.append(frozenset(ignore_leases))
            return original(ignore_leases=ignore_leases)

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
        fixture = CollapseFixture()
        fixture.seed([_hash(1)], network_difficulty=8)
        fixture.rpc.difficulty[DECIDED_WINNER] = 16.0
        fixture.rpc.difficulty["ee" * 32] = 1.0
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
