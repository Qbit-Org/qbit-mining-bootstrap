#!/usr/bin/env python3
"""Direct tests for the PRISM accepted-block finalization owner."""

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock

import lab.prism.block_finalization as block_finalization_module
from lab.prism.block_finalization import (
    FINALIZATION_PHASES,
    BlockFinalizationService,
)


class RecordingFinalizationService(BlockFinalizationService):
    def __init__(self, *, admission: bool = True, landing: bool = True) -> None:
        super().__init__(SimpleNamespace())
        self.events: list[str] = []
        self.admission_result: object = (
            SimpleNamespace(block_hash="ab" * 32) if admission else None
        )
        self.landing_result = object() if landing else None
        self.already_accounted = False

    def _record_block_candidate_progress(
        self,
        phase: str = "accounting-progress",
    ) -> None:
        return None

    def _admit_candidate(
        self,
        candidate: object,
        *,
        node_submission: object,
    ) -> object | None:
        self.events.append("admission")
        return self.admission_result

    def _land_candidate(self, admission: object) -> object | None:
        self.events.append("land_confirm")
        return self.landing_result

    def _candidate_already_accounted(self, block_hash: str) -> bool:
        self.events.append("accounted_check")
        return self.already_accounted

    def _persist_ctv_and_credit(
        self,
        admission: object,
        landed: object,
    ) -> dict[str, object]:
        self.events.append("ctv_credit")
        return {"stored": True}

    def _build_finalization_evidence(
        self,
        admission: object,
        landed: object,
        ctv_persistence: dict[str, object],
    ) -> object:
        self.events.append("evidence")
        return object()

    def _publish_finalization_evidence(
        self,
        landed: object,
        prepared: object,
    ) -> dict[str, object]:
        self.events.append("audit_publish")
        return {"published": True}

    def _account_finalized_candidate(
        self,
        admission: object,
        landed: object,
        published_evidence: dict[str, object],
    ) -> bool:
        self.events.append("accounting")
        return True


class BlockFinalizationServiceTests(unittest.TestCase):
    def test_success_runs_named_phases_once_in_durable_order(self) -> None:
        service = RecordingFinalizationService()
        monotonic_values = iter(range(13))

        with mock.patch(
            "lab.prism.block_finalization.time.monotonic",
            side_effect=lambda: float(next(monotonic_values)),
        ):
            self.assertTrue(
                service._submit_block_candidate_serialized(
                    object(),  # type: ignore[arg-type]
                    node_submission=object(),  # type: ignore[arg-type]
                )
            )

        self.assertEqual(
            service.events,
            [
                "admission",
                "land_confirm",
                "accounted_check",
                "ctv_credit",
                "evidence",
                "audit_publish",
                "accounting",
            ],
        )
        snapshot = service.metrics_snapshot()
        for phase in FINALIZATION_PHASES:
            self.assertEqual(snapshot["phases"][phase]["count"], 1)
            self.assertEqual(snapshot["phases"][phase]["sum"], 1.0)
            self.assertEqual(snapshot["phases"][phase]["max"], 1.0)

    def test_terminal_boundaries_do_not_run_later_phases(self) -> None:
        rejected = RecordingFinalizationService(admission=False)
        self.assertFalse(
            rejected._submit_block_candidate_serialized(
                object(),  # type: ignore[arg-type]
                node_submission=object(),  # type: ignore[arg-type]
            )
        )
        self.assertEqual(rejected.events, ["admission"])

        # A tri-state admission verdict (already-recorded acceptance or an
        # accepted race win) is terminal for the whole run: the ordered
        # phases after admission never execute in either direction.
        for terminal_result in (True, False):
            short_circuit = RecordingFinalizationService()
            short_circuit.admission_result = terminal_result
            self.assertIs(
                short_circuit._submit_block_candidate_serialized(
                    object(),  # type: ignore[arg-type]
                    node_submission=object(),  # type: ignore[arg-type]
                ),
                terminal_result,
            )
            self.assertEqual(short_circuit.events, ["admission"])

        not_landed = RecordingFinalizationService(landing=False)
        self.assertFalse(
            not_landed._submit_block_candidate_serialized(
                object(),  # type: ignore[arg-type]
                node_submission=object(),  # type: ignore[arg-type]
            )
        )
        self.assertEqual(not_landed.events, ["admission", "land_confirm"])

        replay = RecordingFinalizationService()
        replay.already_accounted = True
        self.assertTrue(
            replay._submit_block_candidate_serialized(
                object(),  # type: ignore[arg-type]
                node_submission=object(),  # type: ignore[arg-type]
            )
        )
        self.assertEqual(
            replay.events,
            ["admission", "land_confirm", "accounted_check"],
        )

    def test_candidate_interarrival_metric_is_bounded_aggregate_evidence(self) -> None:
        service = BlockFinalizationService(SimpleNamespace())
        with mock.patch(
            "lab.prism.block_finalization.time.monotonic",
            side_effect=[3.0, 7.5, 13.0],
        ):
            service._note_candidate_started()
            service._note_candidate_started()
            service._note_candidate_started()

        intervals = service.metrics_snapshot()["candidate_intervals"]
        self.assertEqual(intervals["count"], 2)
        self.assertEqual(intervals["sum"], 10.0)
        self.assertEqual(intervals["min"], 4.5)
        lines = service.metrics_lines()
        self.assertIn(
            "qbit_prism_block_candidate_interarrival_seconds_count 2",
            lines,
        )
        self.assertIn(
            "qbit_prism_block_candidate_interarrival_seconds_min 4.500000",
            lines,
        )


class BlockFinalizationPortSurfaceTests(unittest.TestCase):
    """Mechanically compare runtime accesses against the declared port."""

    # The verify_bundle override is read through runtime.__dict__ on purpose
    # (a per-instance escape hatch kept out of the typed port), so the AST
    # sees a "__dict__" attribute access that no Protocol should declare.
    PORT_ACCESS_ALLOWLIST = frozenset({"__dict__"})

    @staticmethod
    def _module_tree() -> ast.Module:
        source_path = Path(block_finalization_module.__file__)
        return ast.parse(source_path.read_text(encoding="utf-8"))

    @classmethod
    def _class_node(cls, tree: ast.Module, name: str) -> ast.ClassDef:
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == name:
                return node
        raise AssertionError(f"class {name} not found in module source")

    def test_every_runtime_access_is_declared_in_the_port(self) -> None:
        tree = self._module_tree()
        service = self._class_node(tree, "BlockFinalizationService")
        port = self._class_node(tree, "BlockFinalizationPort")

        port_members: set[str] = set()
        for statement in port.body:
            if isinstance(
                statement, (ast.FunctionDef, ast.AsyncFunctionDef)
            ):
                port_members.add(statement.name)
            elif isinstance(statement, ast.AnnAssign) and isinstance(
                statement.target, ast.Name
            ):
                port_members.add(statement.target.id)
        self.assertTrue(port_members, "port declared no members")

        runtime_accesses: set[str] = set()
        for node in ast.walk(service):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Attribute)
                and isinstance(node.value.value, ast.Name)
                and node.value.value.id == "self"
                and node.value.attr == "runtime"
            ):
                runtime_accesses.add(node.attr)
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "getattr"
                and len(node.args) >= 2
                and isinstance(node.args[0], ast.Attribute)
                and isinstance(node.args[0].value, ast.Name)
                and node.args[0].value.id == "self"
                and node.args[0].attr == "runtime"
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)
            ):
                runtime_accesses.add(node.args[1].value)
        self.assertTrue(runtime_accesses, "service accessed nothing via runtime")

        undeclared = (
            runtime_accesses - port_members - self.PORT_ACCESS_ALLOWLIST
        )
        self.assertEqual(
            undeclared,
            set(),
            "runtime accesses missing from BlockFinalizationPort: "
            f"{sorted(undeclared)}",
        )

    def test_service_no_longer_forwards_attribute_reads(self) -> None:
        self.assertNotIn("__getattr__", vars(BlockFinalizationService))
        self.assertNotIn("__setattr__", vars(BlockFinalizationService))


if __name__ == "__main__":
    unittest.main()
