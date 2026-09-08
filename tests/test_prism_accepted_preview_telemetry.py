#!/usr/bin/env python3
"""Issue #224 Wave 0: the shared accepted-preview telemetry contract.

These tests pin the contract the three Wave 1 lanes build on: every
vocabulary is closed and label-safe, the rescan reasons mirror the
payout_state literals, the recorder refuses labels outside the contract,
normalizes the two runtime strings onto ``other``, keeps its cells
thread-safe, and the renderer exports every cell from zero without ever
carrying a block hash or height into a label.
"""

from __future__ import annotations

import ast
import json
import math
from pathlib import Path
import re
import threading
from types import SimpleNamespace
import unittest

from lab.prism import accepted_preview_telemetry as contract
from lab.prism.accepted_preview_telemetry import (
    AcceptedPreviewPublicationDiagnostic,
    AcceptedPreviewTelemetry,
    PRISM_ACCEPTED_LANDING_PHASES,
    PRISM_ACCEPTED_PREVIEW_DIAGNOSTICS_CAPACITY,
    PRISM_LEDGER_READ_OPERATIONS,
    PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS,
    PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS,
    PRISM_REORG_RECONCILE_CALLERS,
    PRISM_REORG_RECONCILE_STEPS,
    empty_duration_stats,
    ensure_accepted_preview_telemetry,
    fold_ledger_read_stats,
    normalize_ledger_read_operation,
    normalize_payout_window_full_rescan_reason,
)
from lab.prism.block_candidates import (
    PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS,
)
from lab.prism.metrics import MetricsRenderer
from lab.prism.reorg_reconciler import PRISM_REORG_RECONCILE_LOOKUP_PATHS

REPO_ROOT = Path(__file__).resolve().parents[1]
LABEL_VALUE = re.compile(r"^[a-z][a-z0-9_]*$")
VOCABULARIES = {
    "PRISM_ACCEPTED_LANDING_PHASES": PRISM_ACCEPTED_LANDING_PHASES,
    "PRISM_REORG_RECONCILE_CALLERS": PRISM_REORG_RECONCILE_CALLERS,
    "PRISM_REORG_RECONCILE_STEPS": PRISM_REORG_RECONCILE_STEPS,
    "PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS": PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS,
    "PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS": PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS,
    "PRISM_LEDGER_READ_OPERATIONS": PRISM_LEDGER_READ_OPERATIONS,
}


def _string_constants(node: ast.AST) -> set[str]:
    return {
        child.value
        for child in ast.walk(node)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    }


def payout_state_full_rescan_reason_literals() -> set[str]:
    """Every reason literal payout_state can hand to a full rescan.

    The source of truth keeps them as scattered literals rather than a
    tuple (and that module is not this wave's to edit), so the contract
    tuple is checked against the module text: assignments to
    ``full_reason`` / ``check_reason`` / the cached-window invalidation
    reason, the first argument of ``_DaemonWindowRebuildRequired``, and
    the ``reason=`` keyword of ``_full_payout_window_materialization``.
    """
    source = (REPO_ROOT / "lab" / "prism" / "payout_state.py").read_text()
    found: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in (
                    "full_reason",
                    "check_reason",
                ):
                    found |= _string_constants(node.value)
                elif (
                    isinstance(target, ast.Attribute)
                    and target.attr
                    == "_incremental_payout_artifact_window_invalidation_reason"
                ):
                    found |= _string_constants(node.value)
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                name = func.id
            elif isinstance(func, ast.Attribute):
                name = func.attr
            else:
                continue
            if name == "_DaemonWindowRebuildRequired" and node.args:
                found |= _string_constants(node.args[0])
            elif name == "_full_payout_window_materialization":
                for keyword in node.keywords:
                    if keyword.arg == "reason":
                        found |= _string_constants(keyword.value)
    return found


def _clock() -> tuple[list[float], "callable"]:
    ticks: list[float] = [100.0]

    def monotonic() -> float:
        return ticks[0]

    return ticks, monotonic


class VocabularyContractTests(unittest.TestCase):
    def test_every_vocabulary_is_closed_unique_and_label_safe(self) -> None:
        for name, vocabulary in VOCABULARIES.items():
            with self.subTest(vocabulary=name):
                self.assertIsInstance(vocabulary, tuple)
                self.assertGreater(len(vocabulary), 0)
                self.assertEqual(len(vocabulary), len(set(vocabulary)))
                for member in vocabulary:
                    self.assertRegex(member, LABEL_VALUE)

    def test_runtime_string_vocabularies_carry_the_other_bucket(self) -> None:
        self.assertEqual(PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS[-1], "other")
        self.assertEqual(PRISM_LEDGER_READ_OPERATIONS[-1], "other")
        self.assertIn("other", PRISM_REORG_RECONCILE_CALLERS)
        # Programmer-chosen vocabularies have no fold-in bucket: a typo is
        # a ValueError at the call site, not a silently merged series.
        self.assertNotIn("other", PRISM_ACCEPTED_LANDING_PHASES)
        self.assertNotIn("other", PRISM_REORG_RECONCILE_STEPS)
        self.assertNotIn("other", PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS)

    def test_landing_phases_follow_the_landing_order(self) -> None:
        self.assertEqual(
            PRISM_ACCEPTED_LANDING_PHASES,
            (
                "lane_wait",
                "balance_lock_wait",
                "reconcile",
                "prior_balances_check",
                "chain_probe",
                "preview_prepare",
                "preview_publish",
            ),
        )

    def test_reconcile_callers_join_the_existing_lookup_paths(self) -> None:
        # The demand-accounting family already labels tip_refresh and
        # job_build; the caller vocabulary spells them identically so the
        # two families join on the label value.
        for path in PRISM_REORG_RECONCILE_LOOKUP_PATHS:
            self.assertIn(path, PRISM_REORG_RECONCILE_CALLERS)
        self.assertEqual(
            PRISM_REORG_RECONCILE_STEPS,
            (
                "admission_wait",
                "watch_query",
                "chain_probe",
                "mutations",
                "candidate_prepare",
                "publish",
            ),
        )

    def test_full_rescan_reasons_mirror_payout_state_literals(self) -> None:
        literals = payout_state_full_rescan_reason_literals()
        # The collector must find the reasons the issue was diagnosed on;
        # an empty or partial harvest would make the drift check vacuous.
        for expected in (
            "reconcile_invalidation",
            "cold_start",
            "window_daemon_state_lost",
            "periodic_self_check",
        ):
            self.assertIn(expected, literals)
        self.assertEqual(
            sorted(literals - set(PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS)),
            [],
            "payout_state grew a full-rescan reason the contract does not carry",
        )
        self.assertEqual(
            sorted(set(PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS) - literals),
            ["other"],
            "the contract carries a reason payout_state no longer produces",
        )

    def test_ledger_read_operations_cover_every_attributed_statement(self) -> None:
        source = (REPO_ROOT / "lab" / "prism" / "share_ledger.py").read_text()
        literals = set(re.findall(r'operation="([^"]+)"', source))
        self.assertIn("pending_block_candidate_rows", literals)
        self.assertEqual(
            sorted(literals - set(PRISM_LEDGER_READ_OPERATIONS)),
            [],
            "share_ledger attributes a read under a name outside the contract",
        )

    def test_normalizers_bound_runtime_strings(self) -> None:
        self.assertEqual(
            normalize_payout_window_full_rescan_reason("reconcile_invalidation"),
            "reconcile_invalidation",
        )
        for value in ("hash=" + "ab" * 32, "", None, 7, "Other"):
            self.assertEqual(normalize_payout_window_full_rescan_reason(value), "other")
        self.assertEqual(
            normalize_ledger_read_operation("payout_window_snapshot"),
            "payout_window_snapshot",
        )
        self.assertEqual(normalize_ledger_read_operation("snapshot_at_job_issue"), "other")


class TelemetryRecorderTests(unittest.TestCase):
    def test_empty_owner_snapshot_covers_every_closed_product_at_zero(self) -> None:
        snapshot = AcceptedPreviewTelemetry().snapshot()
        self.assertEqual(
            set(snapshot["landing_phases"]), set(PRISM_ACCEPTED_LANDING_PHASES)
        )
        self.assertEqual(
            set(snapshot["reconcile_passes"]), set(PRISM_REORG_RECONCILE_CALLERS)
        )
        self.assertEqual(
            set(snapshot["reconcile_steps"]),
            {
                (caller, step)
                for caller in PRISM_REORG_RECONCILE_CALLERS
                for step in PRISM_REORG_RECONCILE_STEPS
            },
        )
        self.assertEqual(
            set(snapshot["full_rescans"]),
            {
                (reason, path)
                for reason in PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS
                for path in PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS
            },
        )
        for family in ("landing_phases", "reconcile_passes", "reconcile_steps", "full_rescans"):
            for stats in snapshot[family].values():
                self.assertEqual(stats, empty_duration_stats())
        self.assertEqual(snapshot["diagnostics_retained"], 0)
        self.assertEqual(snapshot["diagnostics_recorded"], 0)

    def test_programmer_labels_outside_the_contract_raise(self) -> None:
        telemetry = AcceptedPreviewTelemetry()
        with self.assertRaises(ValueError):
            telemetry.observe_landing_phase("reorg-reconcile", 0.1)
        with self.assertRaises(ValueError):
            telemetry.observe_reconcile_pass("replay", 0.1)
        with self.assertRaises(ValueError):
            telemetry.observe_reconcile_step("landing", "getblockhash", 0.1)
        with self.assertRaises(ValueError):
            telemetry.observe_reconcile_step("replay", "watch_query", 0.1)
        with self.assertRaises(ValueError):
            telemetry.observe_payout_window_full_rescan(
                "reconcile_invalidation", "rust", 0.1
            )
        with self.assertRaises(ValueError):
            with telemetry.landing_phase("submitblock-rpc"):
                pass
        with self.assertRaises(ValueError):
            with telemetry.reconcile_step("landing", "sweep"):
                pass
        # Nothing was recorded by the refused calls.
        snapshot = telemetry.snapshot()
        for family in ("landing_phases", "reconcile_passes", "reconcile_steps", "full_rescans"):
            for stats in snapshot[family].values():
                self.assertEqual(int(stats["count"]), 0)

    def test_runtime_rescan_reason_folds_into_other(self) -> None:
        telemetry = AcceptedPreviewTelemetry()
        telemetry.observe_payout_window_full_rescan("reconcile_invalidation", "daemon", 3.0)
        telemetry.observe_payout_window_full_rescan("hash=" + "ab" * 32, "daemon", 1.0)
        telemetry.observe_payout_window_full_rescan(None, "in_process", 2.0)
        rescans = telemetry.snapshot()["full_rescans"]
        self.assertEqual(rescans[("reconcile_invalidation", "daemon")]["count"], 1)
        self.assertEqual(rescans[("other", "daemon")]["count"], 1)
        self.assertEqual(rescans[("other", "in_process")]["count"], 1)
        self.assertEqual(
            {key for key, stats in rescans.items() if stats["count"]},
            {("reconcile_invalidation", "daemon"), ("other", "daemon"), ("other", "in_process")},
        )

    def test_accumulates_sum_count_max_and_clamps_bad_durations(self) -> None:
        telemetry = AcceptedPreviewTelemetry()
        for seconds in (0.25, 1.5, 0.75):
            telemetry.observe_landing_phase("reconcile", seconds)
        for bad in (-1.0, math.nan, math.inf):
            telemetry.observe_landing_phase("reconcile", bad)
        stats = telemetry.snapshot()["landing_phases"]["reconcile"]
        self.assertEqual(stats["count"], 6)
        self.assertAlmostEqual(stats["sum"], 2.5)
        self.assertEqual(stats["max"], 1.5)
        self.assertIsInstance(stats["count"], int)

    def test_snapshot_is_a_copy(self) -> None:
        telemetry = AcceptedPreviewTelemetry()
        first = telemetry.snapshot()
        first["landing_phases"]["reconcile"]["count"] = 99
        first["reconcile_steps"][("landing", "publish")]["sum"] = 99.0
        second = telemetry.snapshot()
        self.assertEqual(second["landing_phases"]["reconcile"]["count"], 0)
        self.assertEqual(second["reconcile_steps"][("landing", "publish")]["sum"], 0.0)

    def test_context_managers_record_elapsed_even_when_raising(self) -> None:
        ticks, monotonic = _clock()
        telemetry = AcceptedPreviewTelemetry(monotonic=monotonic)
        with telemetry.landing_phase("balance_lock_wait"):
            ticks[0] += 0.5
        with self.assertRaises(RuntimeError):
            with telemetry.reconcile_step("landing", "mutations"):
                ticks[0] += 0.25
                raise RuntimeError("fenced mutation failed")
        snapshot = telemetry.snapshot()
        self.assertEqual(
            snapshot["landing_phases"]["balance_lock_wait"],
            {"count": 1, "sum": 0.5, "max": 0.5},
        )
        self.assertEqual(
            snapshot["reconcile_steps"][("landing", "mutations")],
            {"count": 1, "sum": 0.25, "max": 0.25},
        )

    def test_concurrent_observers_never_lose_a_sample(self) -> None:
        telemetry = AcceptedPreviewTelemetry()
        per_thread = 500
        threads = 8

        def worker(index: int) -> None:
            for _ in range(per_thread):
                telemetry.observe_landing_phase("preview_publish", 0.001)
                telemetry.observe_reconcile_step("job_build", "chain_probe", 0.002)
                telemetry.observe_payout_window_full_rescan(
                    "cold_start" if index % 2 else "bogus", "in_process", 0.003
                )
                telemetry.record_publication_diagnostic(
                    AcceptedPreviewPublicationDiagnostic(
                        block_hash=f"{index:02x}" * 32,
                        block_height=index,
                        result="published",
                        acceptance_to_publication_seconds=0.5,
                    )
                )

        pool = [threading.Thread(target=worker, args=(index,)) for index in range(threads)]
        for thread in pool:
            thread.start()
        for thread in pool:
            thread.join()
        snapshot = telemetry.snapshot()
        total = per_thread * threads
        self.assertEqual(snapshot["landing_phases"]["preview_publish"]["count"], total)
        self.assertEqual(
            snapshot["reconcile_steps"][("job_build", "chain_probe")]["count"], total
        )
        rescans = snapshot["full_rescans"]
        self.assertEqual(
            rescans[("cold_start", "in_process")]["count"]
            + rescans[("other", "in_process")]["count"],
            total,
        )
        self.assertEqual(snapshot["diagnostics_recorded"], total)
        self.assertEqual(
            snapshot["diagnostics_retained"], PRISM_ACCEPTED_PREVIEW_DIAGNOSTICS_CAPACITY
        )

    def test_diagnostics_ring_is_bounded_oldest_first_and_stamped(self) -> None:
        ticks, monotonic = _clock()
        telemetry = AcceptedPreviewTelemetry(diagnostics_capacity=3, monotonic=monotonic)
        for height in range(5):
            ticks[0] += 1.0
            telemetry.record_publication_diagnostic(
                AcceptedPreviewPublicationDiagnostic(
                    block_hash=f"{height:02x}" * 32,
                    block_height=height,
                    result="published",
                    acceptance_to_publication_seconds=float(height),
                )
            )
        retained = telemetry.diagnostics_snapshot()
        self.assertEqual([record.block_height for record in retained], [2, 3, 4])
        self.assertEqual([record.recorded_monotonic for record in retained], [103.0, 104.0, 105.0])
        snapshot = telemetry.snapshot()
        self.assertEqual(snapshot["diagnostics_retained"], 3)
        self.assertEqual(snapshot["diagnostics_recorded"], 5)
        # A caller's own stamp is kept, and the retained record is what the
        # recorder handed back.
        stamped = telemetry.record_publication_diagnostic(
            AcceptedPreviewPublicationDiagnostic(
                block_hash="ff" * 32,
                block_height=9,
                result="degraded",
                acceptance_to_publication_seconds=6.0,
                recorded_monotonic=42.0,
            )
        )
        self.assertEqual(stamped.recorded_monotonic, 42.0)
        self.assertIs(telemetry.diagnostics_snapshot()[-1], stamped)
        with self.assertRaises(TypeError):
            telemetry.record_publication_diagnostic({"block_hash": "ab" * 32})  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            AcceptedPreviewTelemetry(diagnostics_capacity=0)

    def test_ensure_owner_attaches_once_and_is_shared(self) -> None:
        owner = SimpleNamespace()
        first = ensure_accepted_preview_telemetry(owner)
        self.assertIs(ensure_accepted_preview_telemetry(owner), first)
        self.assertIs(owner.__dict__["_accepted_preview_telemetry"], first)
        # Recording through one handle is visible through the other: the
        # renderer and every Wave 1 lane share one owner per coordinator.
        first.observe_landing_phase("lane_wait", 0.1)
        self.assertEqual(
            ensure_accepted_preview_telemetry(owner).snapshot()["landing_phases"]["lane_wait"]["count"],
            1,
        )
        self.assertIsNot(ensure_accepted_preview_telemetry(SimpleNamespace()), first)
        with self.assertRaises(TypeError):
            ensure_accepted_preview_telemetry(object())

    def test_ensure_owner_is_race_free(self) -> None:
        owner = SimpleNamespace()
        seen: list[AcceptedPreviewTelemetry] = []
        seen_lock = threading.Lock()
        barrier = threading.Barrier(16)

        def worker() -> None:
            barrier.wait()
            telemetry = ensure_accepted_preview_telemetry(owner)
            with seen_lock:
                seen.append(telemetry)

        pool = [threading.Thread(target=worker) for _ in range(16)]
        for thread in pool:
            thread.start()
        for thread in pool:
            thread.join()
        self.assertEqual(len(seen), 16)
        self.assertEqual(len({id(telemetry) for telemetry in seen}), 1)


class DiagnosticRecordTests(unittest.TestCase):
    def test_normalizes_hash_and_bounds_every_field(self) -> None:
        record = AcceptedPreviewPublicationDiagnostic(
            block_hash="  " + "AB" * 32 + " ",
            block_height="12",  # type: ignore[arg-type]
            result="published",
            acceptance_to_publication_seconds=-3.0,
            phase_seconds={"reconcile": 1.25, "preview_publish": math.nan},
            reconcile_caller="landing",
            full_rescan_reason="not-a-contract-reason",
            full_rescan_path="daemon",
            full_rescan_seconds=math.inf,
            ledger_gate_wait_seconds=0.5,
            ledger_execute_seconds=-0.5,
        )
        self.assertEqual(record.block_hash, "ab" * 32)
        self.assertEqual(record.block_height, 12)
        self.assertEqual(record.acceptance_to_publication_seconds, 0.0)
        self.assertEqual(tuple(record.phase_seconds), PRISM_ACCEPTED_LANDING_PHASES)
        self.assertEqual(record.phase_seconds["reconcile"], 1.25)
        self.assertEqual(record.phase_seconds["preview_publish"], 0.0)
        self.assertEqual(record.phase_seconds["lane_wait"], 0.0)
        self.assertEqual(record.full_rescan_reason, "other")
        self.assertEqual(record.full_rescan_seconds, 0.0)
        self.assertEqual(record.ledger_execute_seconds, 0.0)
        with self.assertRaises(TypeError):
            record.phase_seconds["reconcile"] = 9.0  # type: ignore[index]

    def test_refuses_values_outside_the_vocabularies(self) -> None:
        base = dict(
            block_hash="ab" * 32,
            block_height=1,
            result="published",
            acceptance_to_publication_seconds=1.0,
        )
        for result in PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS:
            AcceptedPreviewPublicationDiagnostic(**dict(base, result=result))
        with self.assertRaises(ValueError):
            AcceptedPreviewPublicationDiagnostic(**dict(base, result="timeout"))
        with self.assertRaises(ValueError):
            AcceptedPreviewPublicationDiagnostic(**dict(base, block_hash="   "))
        with self.assertRaises(ValueError):
            AcceptedPreviewPublicationDiagnostic(
                **dict(base, phase_seconds={"submitblock-rpc": 0.1})
            )
        with self.assertRaises(ValueError):
            AcceptedPreviewPublicationDiagnostic(**dict(base, reconcile_caller="replay"))
        with self.assertRaises(ValueError):
            AcceptedPreviewPublicationDiagnostic(**dict(base, full_rescan_path="rust"))

    def test_log_fields_are_flat_json_serializable_and_fixed(self) -> None:
        record = AcceptedPreviewPublicationDiagnostic(
            block_hash="cd" * 32,
            block_height=7,
            result="degraded",
            acceptance_to_publication_seconds=4.123456789,
            phase_seconds={"reconcile": 3.0},
            reconcile_caller="post_confirm",
            full_rescan_reason="reconcile_invalidation",
            full_rescan_path="daemon",
            full_rescan_seconds=2.5,
        )
        fields = record.log_fields()
        json.dumps(fields, sort_keys=True)
        self.assertEqual(
            set(fields),
            {
                "block_hash",
                "block_height",
                "result",
                "acceptance_to_publication_seconds",
                "reconcile_caller",
                "full_rescan_reason",
                "full_rescan_path",
                "full_rescan_seconds",
                "ledger_gate_wait_seconds",
                "ledger_execute_seconds",
                *(f"phase_{phase}_seconds" for phase in PRISM_ACCEPTED_LANDING_PHASES),
            },
        )
        self.assertEqual(fields["acceptance_to_publication_seconds"], 4.123457)
        self.assertEqual(fields["phase_reconcile_seconds"], 3.0)
        self.assertEqual(fields["phase_lane_wait_seconds"], 0.0)
        summary = record.summary()
        self.assertIn("hash=" + "cd" * 32, summary)
        self.assertIn("rescan=reconcile_invalidation/daemon=2.500s", summary)
        self.assertIn("reconcile=3.000s", summary)


class LedgerReadFoldTests(unittest.TestCase):
    STATS = {
        "calls_total": 2,
        "gate_wait_seconds_total": 0.25,
        "gate_wait_seconds_max": 0.2,
        "gate_timeouts_total": 1,
        "execute_seconds_total": 1.0,
        "execute_seconds_max": 0.75,
        "execute_timeouts_total": 0,
    }

    def test_contract_operations_pass_through_as_copies(self) -> None:
        source = {
            "pending_block_candidate_rows": dict(self.STATS),
            "payout_window_snapshot": dict(self.STATS, calls_total=5),
        }
        folded = fold_ledger_read_stats(source)
        self.assertEqual(folded, source)
        folded["payout_window_snapshot"]["calls_total"] = 99
        self.assertEqual(source["payout_window_snapshot"]["calls_total"], 5)
        self.assertEqual(fold_ledger_read_stats({}), {})

    def test_out_of_contract_operations_merge_into_other(self) -> None:
        folded = fold_ledger_read_stats(
            {
                "snapshot_at_job_issue": dict(self.STATS),
                "pending_block_candidate_rows": dict(self.STATS),
                "replay_probe": dict(self.STATS, gate_wait_seconds_max=0.9, execute_timeouts_total=3),
            }
        )
        self.assertEqual(set(folded), {"pending_block_candidate_rows", "other"})
        other = folded["other"]
        self.assertEqual(other["calls_total"], 4)
        self.assertIsInstance(other["calls_total"], int)
        self.assertAlmostEqual(other["gate_wait_seconds_total"], 0.5)
        self.assertEqual(other["gate_wait_seconds_max"], 0.9)
        self.assertEqual(other["execute_seconds_max"], 0.75)
        self.assertEqual(other["gate_timeouts_total"], 2)
        self.assertEqual(other["execute_timeouts_total"], 3)


class RendererContractTests(unittest.TestCase):
    def _labels(self, line: str) -> dict[str, str]:
        return dict(re.findall(r'(\w+)="([^"]*)"', line))

    def test_empty_owner_renders_deterministically_at_zero(self) -> None:
        port = SimpleNamespace()
        renderer = MetricsRenderer(port)  # type: ignore[arg-type]
        first = renderer.accepted_preview_attribution_metrics_lines()
        second = renderer.accepted_preview_attribution_metrics_lines()
        self.assertEqual(first, second)
        families = [
            line.split()[2] for line in first if line.startswith("# TYPE ")
        ]
        self.assertEqual(
            families,
            [
                "qbit_prism_accepted_block_landing_phase_seconds",
                "qbit_prism_reorg_reconcile_pass_seconds",
                "qbit_prism_reorg_reconcile_step_seconds",
                "qbit_prism_payout_window_full_rescan_seconds",
            ],
        )
        samples = [line for line in first if not line.startswith("#")]
        for line in samples:
            value = line.rsplit(" ", 1)[1]
            self.assertIn(value, ("0", "0.000000"), line)
        # Sample lines are unique: no cell renders twice.
        self.assertEqual(len(samples), len({line.split(" ")[0] for line in samples}))

    def test_rendered_labels_stay_inside_the_vocabularies(self) -> None:
        port = SimpleNamespace()
        telemetry = ensure_accepted_preview_telemetry(port)
        block_hash = "ab" * 32
        telemetry.observe_landing_phase("chain_probe", 0.3)
        telemetry.observe_reconcile_pass("other", 0.1)
        telemetry.observe_reconcile_step("other", "publish", 0.05)
        telemetry.observe_payout_window_full_rescan("hash=" + block_hash, "daemon", 2.0)
        telemetry.record_publication_diagnostic(
            AcceptedPreviewPublicationDiagnostic(
                block_hash=block_hash,
                block_height=4242,
                result="published",
                acceptance_to_publication_seconds=4.5,
            )
        )
        lines = MetricsRenderer(port).accepted_preview_attribution_metrics_lines()  # type: ignore[arg-type]
        payload = "\n".join(lines)
        self.assertNotIn(block_hash, payload)
        self.assertNotIn("4242", payload)
        allowed = {
            "phase": set(PRISM_ACCEPTED_LANDING_PHASES),
            "caller": set(PRISM_REORG_RECONCILE_CALLERS),
            "step": set(PRISM_REORG_RECONCILE_STEPS),
            "reason": set(PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS),
            "path": set(PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS),
        }
        for line in lines:
            if line.startswith("#"):
                continue
            labels = self._labels(line)
            self.assertTrue(labels, line)
            for name, value in labels.items():
                self.assertIn(value, allowed[name], line)
        self.assertIn(
            'qbit_prism_payout_window_full_rescan_seconds_count{reason="other",path="daemon"} 1',
            lines,
        )
        self.assertIn(
            'qbit_prism_accepted_block_landing_phase_seconds_sum{phase="chain_probe"} 0.300000',
            lines,
        )

    def test_public_surface_is_declared(self) -> None:
        for name in contract.__all__:
            self.assertTrue(hasattr(contract, name), name)
        for name in VOCABULARIES:
            self.assertIn(name, contract.__all__)


if __name__ == "__main__":
    unittest.main()
