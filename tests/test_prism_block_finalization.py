from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest import mock

from lab.prism.block_finalization import (
    FINALIZATION_PHASES,
    BlockFinalizationService,
)


class RecordingFinalizationService(BlockFinalizationService):
    def __init__(self, *, admission: bool = True, landing: bool = True) -> None:
        super().__init__(SimpleNamespace())
        self.events: list[str] = []
        self.admission_result = (
            SimpleNamespace(block_hash="ab" * 32) if admission else None
        )
        self.landing_result = object() if landing else None
        self.already_accounted = False

    def _admit_candidate(self, candidate: object) -> object | None:
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
            self.assertTrue(service.submit_block_candidate(object()))  # type: ignore[arg-type]

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
        self.assertFalse(rejected.submit_block_candidate(object()))  # type: ignore[arg-type]
        self.assertEqual(rejected.events, ["admission"])

        not_landed = RecordingFinalizationService(landing=False)
        self.assertFalse(not_landed.submit_block_candidate(object()))  # type: ignore[arg-type]
        self.assertEqual(not_landed.events, ["admission", "land_confirm"])

        replay = RecordingFinalizationService()
        replay.already_accounted = True
        self.assertTrue(replay.submit_block_candidate(object()))  # type: ignore[arg-type]
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


if __name__ == "__main__":
    unittest.main()
