#!/usr/bin/env python3
"""Direct tests for the PRISM vardiff owner."""

from __future__ import annotations

from contextlib import redirect_stdout
from decimal import Decimal
from types import SimpleNamespace
import io
import threading
import time
import unittest

from lab.auxpow import vardiff
from lab.prism.payout_state import PayoutStatePublicationBlocked
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

        self.retarget_error: BaseException | None = None

    def retarget_client(self, client: object, **kwargs: object) -> bool:
        self.retargets.append({"client": client, **kwargs})
        if self.retarget_error is not None:
            raise self.retarget_error
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

    def test_payout_publication_block_skips_retarget_without_raising(self) -> None:
        # #414 (page #284): a share-driven retarget builds a paired job, and
        # that build can be fenced behind a pending payout publication (a
        # landed accepted-block transition). The fence must not escape
        # note_accepted -- it would propagate out of handle_submit and kill
        # the client thread before the share's own ack -- so the retarget is
        # skipped, counted under a bounded reason, and logged once per
        # connection.
        runtime = Runtime()
        runtime.retarget_error = PayoutStatePublicationBlocked(
            "accepted block payout confirmation is still pending"
        )
        service = VardiffService(runtime)  # type: ignore[arg-type]
        state = client()

        with redirect_stdout(io.StringIO()) as captured:
            service.note_accepted(state, Decimal("3"))  # type: ignore[arg-type]
            # The window was captured and reset before the retarget ran,
            # exactly as for an applied retarget.
            self.assertEqual(len(runtime.retargets), 1)
            self.assertEqual(state.vardiff_window_accepted, 0)
            self.assertEqual(state.vardiff_window_submitted, 0)
            # A second share inside the same hold skips again, silently.
            state.vardiff_window_submitted = 1
            state.vardiff_window_started_monotonic = time.monotonic() - 2
            service.note_accepted(state, Decimal("3"))  # type: ignore[arg-type]
        self.assertEqual(len(runtime.retargets), 2)
        log = captured.getvalue()
        self.assertEqual(
            log.count("vardiff retarget skipped reason=payout_publication_blocked"),
            1,
        )
        metrics = "\n".join(service.metrics_lines())
        self.assertIn(
            'qbit_prism_vardiff_retargets_skipped_total{reason="payout_publication_blocked"} 2',
            metrics,
        )
        # Any other failure keeps propagating: the fence is the one benign
        # coordination state, not a blanket swallow.
        runtime.retarget_error = RuntimeError("template fetch failed")
        state.vardiff_window_submitted = 1
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        with self.assertRaises(RuntimeError):
            service.note_accepted(state, Decimal("3"))  # type: ignore[arg-type]

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
