#!/usr/bin/env python3
"""Direct tests for the PRISM vardiff owner."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
import threading
import time
import unittest

from lab.auxpow import vardiff
from lab.prism.vardiff_service import VardiffService


def config(*, interval: str = "1") -> vardiff.VardiffConfig:
    return vardiff.VardiffConfig(
        enabled=True,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("1"),
        max_difficulty=Decimal("1024"),
        retarget_interval_seconds=Decimal(interval),
        max_step_factor=Decimal("4"),
        startup_difficulty=Decimal("4"),
        max_step_down_factor=Decimal("4"),
        ewma_alpha=Decimal("0.4"),
        retarget_tolerance=Decimal("0.25"),
    )


class Runtime:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.clients: set[object] = set()
        self.stop_event = threading.Event()
        self.vardiff_config = config()
        self.share_difficulty = Decimal("2")
        self.vardiff_idle_sweep_seconds = 1.0
        self.retargets: list[dict[str, object]] = []

    def retarget_client(self, client: object, **kwargs: object) -> bool:
        self.retargets.append({"client": client, **kwargs})
        return True


def client() -> SimpleNamespace:
    return SimpleNamespace(
        vardiff_config=None,
        listener_vardiff_config=None,
        minimum_advertised_difficulty=Decimal("0"),
        pending_share_difficulty=None,
        share_difficulty=Decimal("4"),
        vardiff_window_started_monotonic=time.monotonic() - 2,
        vardiff_window_accepted=0,
        vardiff_window_submitted=1,
        vardiff_window_work=Decimal("0"),
        vardiff_difficulty_estimate=None,
    )


class VardiffServiceTests(unittest.TestCase):
    def test_accepted_window_is_captured_and_reset_before_retarget(self) -> None:
        runtime = Runtime()
        service = VardiffService(runtime)  # type: ignore[arg-type]
        state = client()

        service.note_accepted(state, Decimal("3"))  # type: ignore[arg-type]

        self.assertEqual(len(runtime.retargets), 1)
        retarget = runtime.retargets[0]
        self.assertEqual(retarget["accepted_shares"], 1)
        self.assertEqual(retarget["submitted_shares"], 1)
        self.assertEqual(retarget["accepted_difficulty"], Decimal("3"))
        self.assertEqual(state.vardiff_window_accepted, 0)
        self.assertEqual(state.vardiff_window_submitted, 0)
        self.assertEqual(state.vardiff_window_work, Decimal("0"))

    def test_speculative_idle_rollback_requires_unchanged_reset_stamp(self) -> None:
        state = client()
        original = (10.0, 0, 0, Decimal("0"))
        state.vardiff_window_started_monotonic = 20.0
        state.vardiff_window_submitted = 0

        VardiffService.restore_idle_window_state(state, original, 20.0)  # type: ignore[arg-type]
        self.assertEqual(state.vardiff_window_started_monotonic, 10.0)

        state.vardiff_window_started_monotonic = 30.0
        state.vardiff_window_submitted = 1
        VardiffService.restore_idle_window_state(state, original, 30.0)  # type: ignore[arg-type]
        self.assertEqual(state.vardiff_window_started_monotonic, 30.0)
        self.assertEqual(state.vardiff_window_submitted, 1)

    def test_idle_metrics_are_service_owned(self) -> None:
        service = VardiffService(Runtime())  # type: ignore[arg-type]
        service.record_idle_skip("busy")
        service.observe_idle_seconds("sweep", 0.01)

        metrics = "\n".join(service.metrics_lines())

        self.assertIn(
            'qbit_prism_vardiff_idle_skips_total{reason="busy"} 1',
            metrics,
        )
        self.assertIn("qbit_prism_vardiff_idle_sweep_seconds_count 1", metrics)


if __name__ == "__main__":
    unittest.main()
