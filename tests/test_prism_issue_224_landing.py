#!/usr/bin/env python3
"""Accepted-landing attribution for issue #224.

``PrismAcceptedPreviewPublicationLatencyHigh`` proves a tail exists between
definitive node acceptance and the visible payout preview, but until this lane
nothing said *which stretch of the landing* owned a sample.  These tests pin
the properties that make the answer trustworthy rather than merely present:

* the phase label vocabulary is closed -- the seven names of
  ``PRISM_ACCEPTED_LANDING_PHASES`` and nothing a call site can compute;
* the interval keeps its shipped definition -- it starts at definitive node
  acceptance, not at the pre-RPC landed fence that is armed before qbitd has
  seen the block, and it closes once at the first publication;
* one publication yields exactly one retained diagnostic and one structured
  line, and a retry or republication yields none;
* a block hash and height reach the diagnostic and the log line only, never a
  metric label;
* the in-flight per-hash timing state cannot leak -- terminal abandonment
  drops it and the map is bounded exactly as the acceptance stamps are;
* the landing itself is unchanged apart from the telemetry: the balance
  serializer is still taken once and released once around the same body, and
  a landing still lands, confirms and publishes.
"""

from __future__ import annotations

import ast
import json
import threading
import unittest
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator
from unittest import mock

import lab.prism.block_candidates as block_candidates_module
import lab.prism.block_finalization as block_finalization_module
from lab.prism.accepted_preview_telemetry import (
    AcceptedPreviewPublicationDiagnostic,
    LANDING_PHASE_BALANCE_LOCK_WAIT,
    LANDING_PHASE_CHAIN_PROBE,
    LANDING_PHASE_LANE_WAIT,
    LANDING_PHASE_PREVIEW_PREPARE,
    LANDING_PHASE_PREVIEW_PUBLISH,
    LANDING_PHASE_PRIOR_BALANCES_CHECK,
    LANDING_PHASE_RECONCILE,
    PRISM_ACCEPTED_LANDING_PHASES,
    RECONCILE_CALLER_LANDING,
    ensure_accepted_preview_telemetry,
)
from lab.prism.block_candidates import (
    MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS,
    PRISM_ACCEPTED_LANDING_LOG_EVENT,
)
from tests.prism_vardiff_test_support import (
    SubmitRpc,
    block_candidate,
    submit_coordinator,
    tempfile,
    verified_audit_report,
    verified_block_bundle,
)


OWNED_MODULES = (block_candidates_module, block_finalization_module)
# Every call whose argument list carries a landing phase name. A phase that is
# a computed string anywhere in this list is a new metric series.
PHASE_ARGUMENT_CALLS = frozenset(
    {
        "observe_landing_phase",
        "landing_phase",
        "_landing_phase",
        "_accepted_landing_phase",
        "_commit_accepted_landing_phase",
    }
)


def _module_tree(module: Any) -> ast.Module:
    return ast.parse(Path(module.__file__).read_text(encoding="utf-8"))


def _builds_a_string(node: ast.AST) -> bool:
    """Whether this expression produces a string the source did not fix."""
    if isinstance(node, ast.Constant):
        return isinstance(node.value, str)
    if isinstance(node, ast.JoinedStr):
        return True
    if isinstance(node, ast.BinOp):
        return any(
            _builds_a_string(operand) for operand in (node.left, node.right)
        )
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return node.func.attr in {"format", "join", "lower", "upper"}
    return False


def _call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    if isinstance(node.func, ast.Name):
        return node.func.id
    return None


class LandingPhaseVocabularyTests(unittest.TestCase):
    """The seven fixed labels, and no way for a call site to invent an eighth."""

    def test_owned_modules_reference_every_contract_phase_constant(self) -> None:
        referenced: set[str] = set()
        for module in OWNED_MODULES:
            for node in ast.walk(_module_tree(module)):
                if isinstance(node, ast.Name) and node.id.startswith(
                    "LANDING_PHASE_"
                ):
                    referenced.add(node.id)
        self.assertEqual(
            referenced,
            {
                "LANDING_PHASE_LANE_WAIT",
                "LANDING_PHASE_BALANCE_LOCK_WAIT",
                "LANDING_PHASE_RECONCILE",
                "LANDING_PHASE_PRIOR_BALANCES_CHECK",
                "LANDING_PHASE_CHAIN_PROBE",
                "LANDING_PHASE_PREVIEW_PREPARE",
                "LANDING_PHASE_PREVIEW_PUBLISH",
            },
        )
        self.assertEqual(
            tuple(sorted(PRISM_ACCEPTED_LANDING_PHASES)),
            tuple(
                sorted(
                    (
                        LANDING_PHASE_LANE_WAIT,
                        LANDING_PHASE_BALANCE_LOCK_WAIT,
                        LANDING_PHASE_RECONCILE,
                        LANDING_PHASE_PRIOR_BALANCES_CHECK,
                        LANDING_PHASE_CHAIN_PROBE,
                        LANDING_PHASE_PREVIEW_PREPARE,
                        LANDING_PHASE_PREVIEW_PUBLISH,
                    )
                )
            ),
        )

    def test_no_phase_argument_is_a_computed_string(self) -> None:
        # A literal, an f-string, or a concatenation reaching one of these
        # calls is how a dynamic label gets born. Names are the only shape
        # allowed, and the recorder validates the name against the contract.
        offenders: list[str] = []
        for module in OWNED_MODULES:
            for node in ast.walk(_module_tree(module)):
                if not isinstance(node, ast.Call):
                    continue
                if _call_name(node) not in PHASE_ARGUMENT_CALLS:
                    continue
                arguments = list(node.args) + [
                    keyword.value for keyword in node.keywords
                ]
                for argument in arguments:
                    if _builds_a_string(argument):
                        offenders.append(
                            f"{Path(module.__file__).name}:{node.lineno}"
                        )
        self.assertEqual(offenders, [])

    def test_unknown_phase_names_are_refused_before_the_body_runs(self) -> None:
        server, _state, _ledger = submit_coordinator()
        service = server._ensure_block_candidate_service()
        entered = False

        with self.assertRaises(ValueError):
            with service._accepted_landing_phase("ab" * 32, "not_a_phase"):
                entered = True  # pragma: no cover - the guard runs first
        self.assertFalse(entered)

        telemetry = ensure_accepted_preview_telemetry(server)
        with self.assertRaises(ValueError):
            telemetry.observe_landing_phase("not_a_phase", 1.0)

    def test_phase_metric_cells_carry_only_the_seven_fixed_labels(self) -> None:
        server, _state, _ledger = submit_coordinator()
        service = server._ensure_block_candidate_service()
        block_hash = "ab" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        for phase in PRISM_ACCEPTED_LANDING_PHASES:
            with service._accepted_landing_phase(block_hash, phase):
                pass

        snapshot = ensure_accepted_preview_telemetry(server).snapshot()
        self.assertEqual(
            set(snapshot["landing_phases"]),
            set(PRISM_ACCEPTED_LANDING_PHASES),
        )
        for phase in PRISM_ACCEPTED_LANDING_PHASES:
            self.assertEqual(snapshot["landing_phases"][phase]["count"], 1)
        # Nothing anywhere in the metric cells names a block.
        self.assertNotIn(block_hash, repr(snapshot))


class LandingLaneWaitTests(unittest.TestCase):
    """``lane_wait`` measures from acceptance, not from the landed fence."""

    def _service(self) -> tuple[Any, Any]:
        server, _state, _ledger = submit_coordinator()
        return server, server._ensure_block_candidate_service()

    def test_lane_wait_starts_at_definitive_acceptance(self) -> None:
        server, service = self._service()
        block_hash = "ab" * 32
        with mock.patch(
            "lab.prism.block_candidates.time.monotonic",
            side_effect=[100.0, 104.5],
        ):
            service._note_accepted_block_preview_acceptance(block_hash)
            service._close_accepted_landing_lane_wait(block_hash, block_height=10)

        snapshot = ensure_accepted_preview_telemetry(server).snapshot()
        lane_wait = snapshot["landing_phases"][LANDING_PHASE_LANE_WAIT]
        self.assertEqual(lane_wait["count"], 1)
        self.assertAlmostEqual(float(lane_wait["sum"]), 4.5)
        self.assertEqual(
            service.accepted_landing_attribution_snapshot()[block_hash][
                LANDING_PHASE_LANE_WAIT
            ],
            4.5,
        )

    def test_pre_rpc_landed_fencing_does_not_start_the_lane_wait(self) -> None:
        # _mark_accepted_block_payout_landed arms the landed barrier *before*
        # the submitblock RPC, so a lane wait measured from it would include
        # the offer itself and would restart on every retry. Only the
        # definitive-acceptance stamp opens an interval.
        server, service = self._service()
        block_hash = "cd" * 32
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)

        service._close_accepted_landing_lane_wait(block_hash, block_height=10)

        snapshot = ensure_accepted_preview_telemetry(server).snapshot()
        self.assertEqual(
            snapshot["landing_phases"][LANDING_PHASE_LANE_WAIT]["count"],
            0,
        )
        self.assertEqual(service.accepted_landing_attribution_snapshot(), {})

    def test_lane_wait_is_recorded_once_per_open_interval(self) -> None:
        # A retried landing re-enters the lane. Measuring again from the same
        # acceptance stamp would charge the retry's queue time to a stretch
        # the first attempt already owns, and would keep growing it.
        server, service = self._service()
        block_hash = "ef" * 32
        with mock.patch(
            "lab.prism.block_candidates.time.monotonic",
            side_effect=[10.0, 11.0, 30.0, 90.0],
        ):
            service._note_accepted_block_preview_acceptance(block_hash)
            service._close_accepted_landing_lane_wait(block_hash)
            service._close_accepted_landing_lane_wait(block_hash)
            service._close_accepted_landing_lane_wait(block_hash)

        lane_wait = ensure_accepted_preview_telemetry(server).snapshot()[
            "landing_phases"
        ][LANDING_PHASE_LANE_WAIT]
        self.assertEqual(lane_wait["count"], 1)
        self.assertAlmostEqual(float(lane_wait["sum"]), 1.0)

    def test_a_candidate_shape_without_a_hash_starts_nothing(self) -> None:
        _server, service = self._service()
        service._note_accepted_landing_lane_start(SimpleNamespace())
        self.assertEqual(service.accepted_landing_attribution_snapshot(), {})

    def _candidate(self, block_hash: str, height: int = 10) -> Any:
        return SimpleNamespace(
            submission=SimpleNamespace(block_hash_hex=block_hash),
            context=SimpleNamespace(template={"height": height}),
        )

    def test_a_lane_closed_before_the_stamp_is_closed_again_after_it(
        self,
    ) -> None:
        # A fresh candidate carries no acceptance stamp until the node offer
        # the landing itself makes returns definitively, so the close that
        # runs before that offer finds no interval to measure. The second
        # close is what turns a first-pass landing into a recorded sample
        # instead of a silent gap: a zero sample and a missing sample read
        # identically in the diagnostic, and only the family count separates
        # them.
        server, service = self._service()
        block_hash = "5c" * 32
        candidate = self._candidate(block_hash)

        service._note_accepted_landing_lane_start(candidate)
        service._note_accepted_block_preview_acceptance(block_hash)
        service._note_accepted_landing_lane_start(candidate)

        lane_wait = ensure_accepted_preview_telemetry(server).snapshot()[
            "landing_phases"
        ][LANDING_PHASE_LANE_WAIT]
        self.assertEqual(lane_wait["count"], 1)
        self.assertEqual(
            service.accepted_landing_attribution_snapshot()[block_hash][
                LANDING_PHASE_LANE_WAIT
            ],
            float(lane_wait["sum"]),
        )

    def test_a_retained_acceptance_keeps_the_earlier_tighter_sample(
        self,
    ) -> None:
        # The reverse case: the stamp already existed, so the close before
        # the retained offer is looked up owns the sample and the one after
        # it must not extend the stretch with the landing's own work.
        server, service = self._service()
        block_hash = "6d" * 32
        candidate = self._candidate(block_hash)
        with mock.patch(
            "lab.prism.block_candidates.time.monotonic",
            side_effect=[10.0, 12.0, 45.0],
        ):
            service._note_accepted_block_preview_acceptance(block_hash)
            service._note_accepted_landing_lane_start(candidate)
            service._note_accepted_landing_lane_start(candidate)

        lane_wait = ensure_accepted_preview_telemetry(server).snapshot()[
            "landing_phases"
        ][LANDING_PHASE_LANE_WAIT]
        self.assertEqual(lane_wait["count"], 1)
        self.assertAlmostEqual(float(lane_wait["sum"]), 2.0)

    def test_the_lane_closes_before_the_attempt_marker_is_written(self) -> None:
        # ``_mark_block_candidate_attempted`` is the landing's own PostgreSQL
        # write, not queue time. Closing the lane after it charges the write
        # to ``lane_wait`` -- and a marker that times out keeps charging the
        # retry backoff there too, so the one stretch that exonerates the
        # landing grows precisely when the writer is degraded. The ordering is
        # the whole guarantee, so it is pinned where it is written.
        tree = _module_tree(block_candidates_module)
        ordered = 0
        for function in ast.walk(tree):
            if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            lane_starts = [
                node.lineno
                for node in ast.walk(function)
                if isinstance(node, ast.Call)
                and _call_name(node) == "_note_accepted_landing_lane_start"
            ]
            markers = [
                node.lineno
                for node in ast.walk(function)
                if isinstance(node, ast.Call)
                and _call_name(node) == "_mark_block_candidate_attempted"
            ]
            if not lane_starts or not markers:
                continue
            offers = [
                node.lineno
                for node in ast.walk(function)
                if isinstance(node, ast.Call)
                and _call_name(node)
                == "_node_submission_for_candidate_or_retained"
            ]
            ordered += 1
            self.assertLess(
                max(lane_starts),
                min(markers),
                f"{function.name} closes the lane after its attempt marker",
            )
            # A fresh candidate is stamped by the offer itself, so a path that
            # only closed the lane before making it would record no sample at
            # all for every first-pass landing.
            self.assertTrue(
                offers and max(lane_starts) > max(offers),
                f"{function.name} never closes the lane after its node offer",
            )
        # Both queued landing paths write the marker; a rename that loses one
        # would otherwise make this test vacuously true.
        self.assertEqual(ordered, 2)


class LandingPublicationDiagnosticTests(unittest.TestCase):
    """One closed interval, one retained diagnostic, one structured line."""

    def _service(self) -> tuple[Any, Any]:
        server, _state, _ledger = submit_coordinator()
        return server, server._ensure_block_candidate_service()

    def _publish(self, server: Any, block_hash: str, result: str) -> list[dict]:
        stream = StringIO()
        with mock.patch("sys.stdout", stream):
            server._observe_accepted_block_preview_publication(
                block_hash,
                result=result,
            )
        lines = []
        for line in stream.getvalue().splitlines():
            prefix, _, payload = line.partition("prism coordinator: ")
            if prefix or not payload.startswith("{"):
                continue
            record = json.loads(payload)
            if record.get("event") == PRISM_ACCEPTED_LANDING_LOG_EVENT:
                lines.append(record)
        return lines

    def test_first_published_publication_closes_one_interval(self) -> None:
        server, service = self._service()
        block_hash = "ab" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        with service._accepted_landing_phase(
            block_hash,
            LANDING_PHASE_RECONCILE,
            block_height=10,
        ):
            pass

        logged = self._publish(server, block_hash, "published")

        telemetry = ensure_accepted_preview_telemetry(server)
        diagnostics = telemetry.diagnostics_snapshot()
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(telemetry.snapshot()["diagnostics_recorded"], 1)
        diagnostic = diagnostics[0]
        self.assertEqual(diagnostic.block_hash, block_hash)
        self.assertEqual(diagnostic.block_height, 10)
        self.assertEqual(diagnostic.result, "published")
        self.assertEqual(diagnostic.reconcile_caller, RECONCILE_CALLER_LANDING)
        self.assertGreaterEqual(diagnostic.acceptance_to_publication_seconds, 0.0)
        self.assertIsNotNone(diagnostic.recorded_monotonic)
        self.assertEqual(
            set(diagnostic.phase_seconds),
            set(PRISM_ACCEPTED_LANDING_PHASES),
        )
        self.assertEqual(len(logged), 1)
        self.assertEqual(logged[0]["block_hash"], block_hash)
        self.assertEqual(logged[0]["block_height"], 10)
        self.assertEqual(logged[0]["result"], "published")
        # The shipped histogram still closed exactly one interval.
        histograms = service.accepted_block_preview_publication_snapshot()
        self.assertEqual(histograms["published"]["count"], 1)
        self.assertEqual(histograms["degraded"]["count"], 0)

    def test_first_degraded_publication_closes_one_interval(self) -> None:
        # The fenced local-retention branch also makes the preview visible to
        # waiting children, so it closes the interval and earns a diagnostic
        # of its own -- labelled apart, never merged into the healthy one.
        server, service = self._service()
        block_hash = "cd" * 32
        service._note_accepted_block_preview_acceptance(block_hash)

        logged = self._publish(server, block_hash, "degraded")

        telemetry = ensure_accepted_preview_telemetry(server)
        diagnostics = telemetry.diagnostics_snapshot()
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0].result, "degraded")
        self.assertEqual(len(logged), 1)
        self.assertEqual(logged[0]["result"], "degraded")
        histograms = service.accepted_block_preview_publication_snapshot()
        self.assertEqual(histograms["degraded"]["count"], 1)
        self.assertEqual(histograms["published"]["count"], 0)

    def test_retry_and_republication_do_not_double_count(self) -> None:
        server, service = self._service()
        block_hash = "ef" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        first = self._publish(server, block_hash, "published")

        # A matching republication, a degraded relabel, and a second
        # definitive offer of the same hash: none of them is a new interval.
        again = self._publish(server, block_hash, "published")
        relabelled = self._publish(server, block_hash, "degraded")
        service._note_accepted_block_preview_acceptance(block_hash)
        reoffered = self._publish(server, block_hash, "published")

        telemetry = ensure_accepted_preview_telemetry(server)
        self.assertEqual(len(first), 1)
        self.assertEqual(again, [])
        self.assertEqual(relabelled, [])
        self.assertEqual(reoffered, [])
        self.assertEqual(len(telemetry.diagnostics_snapshot()), 1)
        self.assertEqual(telemetry.snapshot()["diagnostics_recorded"], 1)
        histograms = service.accepted_block_preview_publication_snapshot()
        self.assertEqual(histograms["published"]["count"], 1)
        self.assertEqual(histograms["degraded"]["count"], 0)

    def test_phases_after_the_interval_closed_reach_no_diagnostic(self) -> None:
        # A retry's stretches still belong in the process-wide family -- they
        # are landing work that ran -- but they can never be claimed by a
        # landing whose interval is already measured and recorded.
        server, service = self._service()
        block_hash = "12" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        self._publish(server, block_hash, "published")

        with service._accepted_landing_phase(
            block_hash,
            LANDING_PHASE_RECONCILE,
            block_height=10,
        ):
            pass

        telemetry = ensure_accepted_preview_telemetry(server)
        self.assertEqual(len(telemetry.diagnostics_snapshot()), 1)
        self.assertEqual(
            telemetry.diagnostics_snapshot()[0].phase_seconds[
                LANDING_PHASE_RECONCILE
            ],
            0.0,
        )
        self.assertEqual(
            telemetry.snapshot()["landing_phases"][LANDING_PHASE_RECONCILE][
                "count"
            ],
            1,
        )
        self.assertEqual(service.accepted_landing_attribution_snapshot(), {})

    def test_publication_without_an_acceptance_stamp_records_nothing(self) -> None:
        server, _service = self._service()
        logged = self._publish(server, "ab" * 32, "published")
        telemetry = ensure_accepted_preview_telemetry(server)
        self.assertEqual(logged, [])
        self.assertEqual(telemetry.diagnostics_snapshot(), ())

    def test_publish_span_is_closed_by_the_visibility_instant(self) -> None:
        # The publication closes the interval from inside the publish call, so
        # a plain timer would record ``preview_publish`` after the diagnostic
        # was already built from an empty cell.
        server, service = self._service()
        block_hash = "34" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        with service._accepted_landing_publish_span(block_hash, block_height=11):
            server._observe_accepted_block_preview_publication(
                block_hash,
                result="published",
            )

        telemetry = ensure_accepted_preview_telemetry(server)
        diagnostics = telemetry.diagnostics_snapshot()
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0].block_height, 11)
        self.assertGreater(
            diagnostics[0].phase_seconds[LANDING_PHASE_PREVIEW_PUBLISH],
            0.0,
        )
        # Closed exactly once: the observer took the span, and the context
        # manager's exit found nothing left to record.
        self.assertEqual(
            telemetry.snapshot()["landing_phases"][
                LANDING_PHASE_PREVIEW_PUBLISH
            ]["count"],
            1,
        )

    def test_a_publish_that_publishes_nothing_still_records_its_phase(self) -> None:
        server, service = self._service()
        block_hash = "56" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        with service._accepted_landing_publish_span(block_hash):
            pass

        telemetry = ensure_accepted_preview_telemetry(server)
        self.assertEqual(
            telemetry.snapshot()["landing_phases"][
                LANDING_PHASE_PREVIEW_PUBLISH
            ]["count"],
            1,
        )
        self.assertEqual(telemetry.diagnostics_snapshot(), ())

    def test_hash_and_height_reach_the_diagnostic_and_log_only(self) -> None:
        server, service = self._service()
        block_hash = "78" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        with service._accepted_landing_phase(
            block_hash,
            LANDING_PHASE_CHAIN_PROBE,
            block_height=4242,
        ):
            pass
        logged = self._publish(server, block_hash, "published")

        telemetry = ensure_accepted_preview_telemetry(server)
        self.assertEqual(len(logged), 1)
        self.assertEqual(logged[0]["block_hash"], block_hash)
        self.assertEqual(logged[0]["block_height"], 4242)
        for phase in PRISM_ACCEPTED_LANDING_PHASES:
            self.assertIn(f"phase_{phase}_seconds", logged[0])

        rendered = repr(telemetry.snapshot())
        self.assertNotIn(block_hash, rendered)
        self.assertNotIn("4242", rendered)
        self.assertNotIn(
            block_hash,
            repr(service.accepted_block_preview_publication_snapshot()),
        )

    def test_the_shipped_metrics_render_carries_no_hash_or_height(self) -> None:
        # End to end through the renderer the alert is read from: the phase
        # family is there, its only label is the fixed phase name, and the
        # block this landing measured is nowhere in the exposition.
        server, service = self._service()
        block_hash = "bc" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        with service._accepted_landing_phase(
            block_hash,
            LANDING_PHASE_RECONCILE,
            block_height=4242,
        ):
            pass
        self._publish(server, block_hash, "published")

        rendered = server._ensure_metrics_renderer().render()

        self.assertIn(
            'qbit_prism_accepted_block_landing_phase_seconds_count{phase="reconcile"} 1',
            rendered,
        )
        for phase in PRISM_ACCEPTED_LANDING_PHASES:
            self.assertIn(
                "qbit_prism_accepted_block_landing_phase_seconds_sum"
                f'{{phase="{phase}"}}',
                rendered,
            )
        self.assertNotIn(block_hash, rendered)
        self.assertNotIn("4242", rendered)
        self.assertNotIn(PRISM_ACCEPTED_LANDING_LOG_EVENT, rendered)

    def test_rescan_and_ledger_totals_stay_at_the_neutral_values(self) -> None:
        # This lane can obtain no per-landing causal evidence for either
        # without reading a process-wide delta, and such a read would claim
        # whatever else the process was doing as this block's work. The
        # separately attributed families answer those questions instead.
        server, service = self._service()
        block_hash = "9a" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        logged = self._publish(server, block_hash, "published")

        diagnostic = ensure_accepted_preview_telemetry(server).diagnostics_snapshot()[0]
        neutral = AcceptedPreviewPublicationDiagnostic(
            block_hash=block_hash,
            block_height=None,
            result="published",
            acceptance_to_publication_seconds=0.0,
        )
        self.assertEqual(diagnostic.full_rescan_reason, neutral.full_rescan_reason)
        self.assertEqual(diagnostic.full_rescan_path, neutral.full_rescan_path)
        self.assertEqual(diagnostic.full_rescan_seconds, neutral.full_rescan_seconds)
        self.assertEqual(
            diagnostic.ledger_gate_wait_seconds,
            neutral.ledger_gate_wait_seconds,
        )
        self.assertEqual(
            diagnostic.ledger_execute_seconds,
            neutral.ledger_execute_seconds,
        )
        self.assertIsNone(logged[0]["full_rescan_reason"])
        self.assertEqual(logged[0]["ledger_execute_seconds"], 0.0)


class LandingAttributionCleanupTests(unittest.TestCase):
    """In-flight per-hash timing state is bounded and cannot leak."""

    def _service(self) -> tuple[Any, Any]:
        server, _state, _ledger = submit_coordinator()
        return server, server._ensure_block_candidate_service()

    def test_terminal_abandonment_discards_the_in_flight_state(self) -> None:
        server, service = self._service()
        block_hash = "ab" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        with service._accepted_landing_phase(
            block_hash,
            LANDING_PHASE_RECONCILE,
        ):
            pass
        self.assertIn(block_hash, service.accepted_landing_attribution_snapshot())

        service._record_committed_block_candidate_abandonment(
            block_hash,
            SimpleNamespace(reason="submitblock-rejected", stale_job_class=None),
        )

        self.assertEqual(service.accepted_landing_attribution_snapshot(), {})
        # The metric family keeps the stretch that actually ran.
        self.assertEqual(
            ensure_accepted_preview_telemetry(server).snapshot()["landing_phases"][
                LANDING_PHASE_RECONCILE
            ]["count"],
            1,
        )

    def test_a_retryable_abandonment_keeps_the_open_interval(self) -> None:
        # A retryable disposition is not terminal: the same interval is still
        # running and its next attempt will close it.
        _server, service = self._service()
        block_hash = "cd" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        with service._accepted_landing_phase(
            block_hash,
            LANDING_PHASE_RECONCILE,
        ):
            pass
        retryable = next(iter(service.retryable_reasons))

        service._record_committed_block_candidate_abandonment(
            block_hash,
            SimpleNamespace(reason=retryable, stale_job_class=None),
        )

        self.assertIn(block_hash, service.accepted_landing_attribution_snapshot())

    def test_in_flight_state_is_bounded_like_the_acceptance_stamps(self) -> None:
        _server, service = self._service()
        for index in range(MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS + 64):
            block_hash = f"{index:064x}"
            service._note_accepted_block_preview_acceptance(block_hash)
            with service._accepted_landing_phase(
                block_hash,
                LANDING_PHASE_RECONCILE,
            ):
                pass

        retained = service.accepted_landing_attribution_snapshot()
        self.assertLessEqual(
            len(retained),
            MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS,
        )
        self.assertLessEqual(
            len(service._accepted_block_preview_acceptance_monotonic),
            MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS,
        )
        # Eviction is oldest-first, so the newest landing is the survivor.
        newest = f"{MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS + 63:064x}"
        self.assertIn(newest, retained)

    def test_an_unstamped_publication_leaves_nothing_in_flight(self) -> None:
        # A startup replay or a chain-proven candidate publishes without an
        # in-process acceptance stamp, so the observer returns before it can
        # pop anything. A record opened by the publish span would then report
        # a finished publication as in flight for as long as the bound kept
        # it.
        server, service = self._service()
        block_hash = "7b" * 32
        with service._accepted_landing_publish_span(block_hash, block_height=12):
            server._observe_accepted_block_preview_publication(
                block_hash,
                result="published",
            )

        self.assertEqual(service.accepted_landing_attribution_snapshot(), {})
        telemetry = ensure_accepted_preview_telemetry(server)
        self.assertEqual(telemetry.diagnostics_snapshot(), ())
        # The stretch still ran, so the phase family still owns its sample.
        self.assertEqual(
            telemetry.snapshot()["landing_phases"][
                LANDING_PHASE_PREVIEW_PUBLISH
            ]["count"],
            1,
        )

    def test_unstamped_publications_never_evict_a_live_landing(self) -> None:
        # The failure this guards is the replay storm: enough unattributable
        # publications to fill the shared cap would push live landings out of
        # it before their diagnostics were ever emitted.
        server, service = self._service()
        live = "01" * 32
        service._note_accepted_block_preview_acceptance(live)
        with service._accepted_landing_phase(live, LANDING_PHASE_RECONCILE):
            pass
        for index in range(MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS + 8):
            with service._accepted_landing_publish_span(f"{index:064x}"):
                pass

        self.assertEqual(
            list(service.accepted_landing_attribution_snapshot()),
            [live],
        )
        server._observe_accepted_block_preview_publication(
            live,
            result="published",
        )
        diagnostics = ensure_accepted_preview_telemetry(server).diagnostics_snapshot()
        self.assertEqual([entry.block_hash for entry in diagnostics], [live])

    def test_publication_pops_the_in_flight_state(self) -> None:
        server, service = self._service()
        block_hash = "ef" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        with service._accepted_landing_phase(
            block_hash,
            LANDING_PHASE_RECONCILE,
        ):
            pass
        server._observe_accepted_block_preview_publication(
            block_hash,
            result="published",
        )
        self.assertEqual(service.accepted_landing_attribution_snapshot(), {})

    def test_a_raising_stretch_still_records_and_is_not_masked(self) -> None:
        server, service = self._service()
        block_hash = "12" * 32
        service._note_accepted_block_preview_acceptance(block_hash)

        with self.assertRaisesRegex(RuntimeError, "reconcile exploded"):
            with service._accepted_landing_phase(
                block_hash,
                LANDING_PHASE_RECONCILE,
            ):
                raise RuntimeError("reconcile exploded")

        telemetry = ensure_accepted_preview_telemetry(server)
        self.assertEqual(
            telemetry.snapshot()["landing_phases"][LANDING_PHASE_RECONCILE][
                "count"
            ],
            1,
        )
        self.assertIn(
            LANDING_PHASE_RECONCILE,
            service.accepted_landing_attribution_snapshot()[block_hash],
        )

    def test_a_raising_publish_span_still_records_and_is_not_masked(self) -> None:
        server, service = self._service()
        block_hash = "34" * 32
        service._note_accepted_block_preview_acceptance(block_hash)

        with self.assertRaisesRegex(RuntimeError, "publication exploded"):
            with service._accepted_landing_publish_span(block_hash):
                raise RuntimeError("publication exploded")

        self.assertEqual(
            ensure_accepted_preview_telemetry(server).snapshot()["landing_phases"][
                LANDING_PHASE_PREVIEW_PUBLISH
            ]["count"],
            1,
        )

    def test_a_refused_diagnostic_never_fails_a_publication(self) -> None:
        # Attribution is observability: the preview is already visible to
        # waiting children by the time the record is built.
        server, service = self._service()
        block_hash = "56" * 32
        service._note_accepted_block_preview_acceptance(block_hash)
        with mock.patch.object(
            block_candidates_module,
            "AcceptedPreviewPublicationDiagnostic",
            side_effect=ValueError("refused"),
        ):
            server._observe_accepted_block_preview_publication(
                block_hash,
                result="published",
            )

        telemetry = ensure_accepted_preview_telemetry(server)
        self.assertEqual(telemetry.diagnostics_snapshot(), ())
        # The shipped interval still closed exactly once.
        self.assertEqual(
            service.accepted_block_preview_publication_snapshot()["published"][
                "count"
            ],
            1,
        )


class LandingBehaviourCompatibilityTests(unittest.TestCase):
    """The landing itself is unchanged apart from the telemetry it emits."""

    def test_balance_serializer_is_taken_once_around_the_same_body(self) -> None:
        # The acquisition is timed through an ExitStack rather than a plain
        # ``with``. The lock must still be entered once before the body and
        # released once after it, on the early-return path too.
        server, state, _ledger = submit_coordinator()
        service = server._ensure_block_finalization_service()
        events: list[str] = []
        real_lock = server._block_submitter_lock

        @contextmanager
        def recording(lock: Any, name: str) -> Iterator[None]:
            events.append(f"enter:{name}")
            try:
                with real_lock(lock, name):
                    yield
            finally:
                events.append(f"exit:{name}")

        server._block_submitter_lock = recording  # type: ignore[method-assign]
        server._defer_for_pending_parent_payout_transition = (  # type: ignore[method-assign]
            lambda **_kwargs: (events.append("body") or True)
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="94" * 32,
            block_hex="00",
        )
        candidate = block_candidate(server, state, submission)

        landed = service._land_and_confirm_block_candidate(
            candidate,
            current_tip="00" * 32,
            already_active=False,
            worker="miner-a",
            node_submission=block_candidates_module._BlockCandidateNodeSubmission(
                attempted=False
            ),
        )

        self.assertIsNone(landed)
        self.assertEqual(
            events,
            [
                "enter:payout-balance-mutation",
                "body",
                "exit:payout-balance-mutation",
            ],
        )
        # The wait was attributed even though the landing deferred.
        self.assertEqual(
            ensure_accepted_preview_telemetry(server).snapshot()["landing_phases"][
                LANDING_PHASE_BALANCE_LOCK_WAIT
            ]["count"],
            1,
        )
        # And the serializer is genuinely free again.
        self.assertTrue(server._payout_balance_mutation_lock.acquire(blocking=False))
        server._payout_balance_mutation_lock.release()

    def test_prior_balances_reread_keeps_its_stamps_and_verdict(self) -> None:
        server, _state, _ledger = submit_coordinator()
        service = server._ensure_block_finalization_service()
        stamps: list[str] = []
        server._record_block_submitter_phase = (  # type: ignore[method-assign]
            lambda phase: stamps.append(phase)
        )
        server.prior_balances_match_current = (  # type: ignore[method-assign]
            lambda _balances: False
        )

        without_context = service._stamped_prior_balances_match_current([])
        with_context = service._stamped_prior_balances_match_current(
            [],
            block_hash="ab" * 32,
            block_height=10,
        )

        self.assertFalse(without_context)
        self.assertFalse(with_context)
        self.assertEqual(
            stamps,
            [
                "prior-balances-check",
                "prior-balances-check:complete",
                "prior-balances-check",
                "prior-balances-check:complete",
            ],
        )
        # Only the landing-scoped call is a landing phase; the same helper
        # called without a hash still times nothing.
        self.assertEqual(
            ensure_accepted_preview_telemetry(server).snapshot()["landing_phases"][
                LANDING_PHASE_PRIOR_BALANCES_CHECK
            ]["count"],
            1,
        )

    def test_a_landing_still_lands_confirms_and_publishes(self) -> None:
        server, state, ledger = submit_coordinator()
        block_hash = "94" * 32
        with tempfile.TemporaryDirectory() as tmp:
            server.audit_dir = Path(tmp)
            server.evidence_path = Path(tmp) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server._ensure_audit_artifact_store()
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            candidate = block_candidate(server, state, submission)

            self.assertTrue(server.submit_block_candidate(candidate))

        self.assertEqual(server.accepted_block_count, 1)
        service = server._ensure_block_candidate_service()
        histograms = service.accepted_block_preview_publication_snapshot()
        published = int(histograms["published"]["count"])
        degraded = int(histograms["degraded"]["count"])
        self.assertEqual(published + degraded, 1)

        telemetry = ensure_accepted_preview_telemetry(server)
        diagnostics = telemetry.diagnostics_snapshot()
        self.assertEqual(len(diagnostics), 1)
        diagnostic = diagnostics[0]
        self.assertEqual(diagnostic.block_hash, block_hash)
        self.assertEqual(diagnostic.block_height, 10)
        self.assertEqual(diagnostic.reconcile_caller, RECONCILE_CALLER_LANDING)
        # The measured stretches are stretches *of* the interval they close,
        # so they can never sum past it.
        self.assertLessEqual(
            sum(diagnostic.phase_seconds.values()),
            diagnostic.acceptance_to_publication_seconds + 1e-6,
        )
        # Nothing is left in flight once the landing completed.
        self.assertEqual(service.accepted_landing_attribution_snapshot(), {})

    def test_the_service_records_nothing_without_the_b1_seam(self) -> None:
        # The focused fakes that drive finalization directly carry no
        # block-candidate service; the timers must degrade to no-ops rather
        # than reaching through a runtime that has no seam.
        service = block_finalization_module.BlockFinalizationService(
            SimpleNamespace()
        )
        self.assertIsNone(service._accepted_landing_owner())
        with service._landing_phase(
            LANDING_PHASE_RECONCILE,
            block_hash="ab" * 32,
        ):
            pass
        with service._landing_publish_span(block_hash="ab" * 32):
            pass
        self.assertNotIn(
            "_accepted_preview_telemetry",
            vars(service.runtime),
        )


class LandingAttributionThreadingTests(unittest.TestCase):
    """The recorders never nest the leaf lock under anything of their own."""

    def test_concurrent_landings_keep_their_own_attribution(self) -> None:
        server, _state, _ledger = submit_coordinator()
        service = server._ensure_block_candidate_service()
        hashes = [f"{index:064x}" for index in range(8)]
        for block_hash in hashes:
            service._note_accepted_block_preview_acceptance(block_hash)

        barrier = threading.Barrier(len(hashes))

        def land(block_hash: str) -> None:
            barrier.wait()
            with service._accepted_landing_phase(
                block_hash,
                LANDING_PHASE_RECONCILE,
                block_height=10,
            ):
                pass
            server._observe_accepted_block_preview_publication(
                block_hash,
                result="published",
            )

        threads = [
            threading.Thread(target=land, args=(block_hash,))
            for block_hash in hashes
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        telemetry = ensure_accepted_preview_telemetry(server)
        diagnostics = telemetry.diagnostics_snapshot()
        self.assertEqual(
            sorted(diagnostic.block_hash for diagnostic in diagnostics),
            sorted(hashes),
        )
        self.assertEqual(
            telemetry.snapshot()["landing_phases"][LANDING_PHASE_RECONCILE][
                "count"
            ],
            len(hashes),
        )
        self.assertEqual(service.accepted_landing_attribution_snapshot(), {})


if __name__ == "__main__":
    unittest.main()
