#!/usr/bin/env python3
"""Bounded safety tests for ``tests/perf/window_paging_allocation.py``.

The measurement driver talks to a disposable ``--serve`` daemon over blocking
pipes. A stalled daemon must never hang a benchmark: every exchange, the
handshake included, runs under a wall-clock deadline that kills and reaps the
child and records the failure, and every exit path closes the pipes and the
stderr capture. These tests exercise that with a fake daemon that stalls in
three ways -- no handshake, after consuming the request, and without ever
reading it (so the request writer is blocked on a full pipe) -- and stay
well under a few seconds each.
"""

from __future__ import annotations

import os
import stat
import tempfile
import time
import unittest
from pathlib import Path

from tests.perf import window_paging_allocation as harness


FAKE_DAEMON = """#!/usr/bin/env python3
import json, os, sys, time
mode = os.environ.get("FAKE_DAEMON_MODE", "stall_after_handshake")
if mode != "no_handshake":
    sys.stdout.write(json.dumps({
        "event": "handshake",
        "tool": "qbit-prism-build-audit-bundle",
        "protocol": %d,
    }) + "\\n")
    sys.stdout.flush()
if mode == "stall_after_handshake":
    sys.stdin.readline()
time.sleep(120)
""" % harness.PRISM_SERVE_BUILDER_PROTOCOL_VERSION


def _assert_reaped(test: unittest.TestCase, pid: int) -> None:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return
    except PermissionError:  # pragma: no cover - pid reused by another user
        return
    test.fail(f"fake daemon pid {pid} is still present after close()")


class StalledDaemonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.binary = Path(cls.tmp.name) / "fake-daemon"
        cls.binary.write_text(FAKE_DAEMON)
        cls.binary.chmod(cls.binary.stat().st_mode | stat.S_IXUSR)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tmp.cleanup()

    def _daemon(self, mode: str, timeout: float = 0.5) -> harness.Daemon:
        os.environ["FAKE_DAEMON_MODE"] = mode
        try:
            return harness.Daemon(
                self.binary,
                memory_limit_mb=None,
                stderr_path=Path(self.tmp.name) / f"{mode}.stderr",
                exchange_timeout=timeout,
                shutdown_timeout=2.0,
            )
        finally:
            os.environ.pop("FAKE_DAEMON_MODE", None)

    def test_exchange_that_stalls_after_reading_times_out_and_is_reaped(self) -> None:
        daemon = self._daemon("stall_after_handshake")
        started = time.perf_counter()
        exchange = daemon.exchange(b'{"request":"prepare_window"}\n', label="full")
        elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 5.0)
        self.assertIsNone(exchange.envelope)
        self.assertIn("full exceeded 0.5s; daemon killed", exchange.error or "")
        outcome = daemon.close()
        self.assertEqual(outcome["timed_out"], "full")
        self.assertEqual(outcome["signal"], 9)
        self.assertTrue(daemon.process.stdin.closed)
        self.assertTrue(daemon.process.stdout.closed)
        self.assertTrue(daemon._stderr.closed)
        _assert_reaped(self, outcome["pid"])
        # Idempotent, and a later exchange never touches the dead child.
        self.assertIs(daemon.close(), outcome)
        self.assertEqual(daemon.exchange(b"{}\n").error, "daemon is not running")

    def test_stall_without_reading_unblocks_the_request_writer(self) -> None:
        daemon = self._daemon("stall_without_reading")
        request = b'{"records":"' + b"x" * (4 << 20) + b'"}\n'  # far beyond one pipe capacity
        started = time.perf_counter()
        exchange = daemon.exchange(request, label="full")
        elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 5.0)
        self.assertIn("daemon killed", exchange.error or "")
        self.assertIn("write:", exchange.error or "")
        outcome = daemon.close()
        self.assertEqual(outcome["signal"], 9)
        _assert_reaped(self, outcome["pid"])

    def test_missing_handshake_times_out_and_is_reaped(self) -> None:
        started = time.perf_counter()
        with self.assertRaises(harness.DaemonTimeout) as caught:
            self._daemon("no_handshake")
        self.assertLess(time.perf_counter() - started, 5.0)
        outcome = caught.exception.daemon_outcome  # type: ignore[attr-defined]
        self.assertEqual(outcome["timed_out"], "handshake")
        self.assertEqual(outcome["signal"], 9)
        _assert_reaped(self, outcome["pid"])

    def test_run_variant_records_the_timeout_and_reaps(self) -> None:
        fixture = harness.build_fixture(64, miners=2, page_size=16, small=2, large=3)
        os.environ["FAKE_DAEMON_MODE"] = "stall_after_handshake"
        try:
            started = time.perf_counter()
            result = harness.run_variant(
                "fake",
                self.binary,
                fixture,
                oracle=None,
                memory_limit_mb=None,
                memory_margin_mb=0,
                sample_interval=0.05,
                exchange_timeout=0.5,
                stderr_dir=Path(self.tmp.name),
                log=lambda _text: None,
            )
        finally:
            os.environ.pop("FAKE_DAEMON_MODE", None)
        self.assertLess(time.perf_counter() - started, 10.0)
        self.assertIsNone(result.skipped)
        self.assertEqual([phase.phase for phase in result.phases], ["full"])
        self.assertEqual(result.phases[0].status, "timeout")
        self.assertIsNone(result.phases[0].residual_wait_seconds)
        self.assertEqual(result.outcome["timed_out"], "full")
        self.assertEqual(result.outcome["signal"], 9)
        _assert_reaped(self, result.outcome["pid"])
        # The tables render a killed run rather than choking on it.
        self.assertIn("killed on full timeout", harness.render_summary([result]))
        self.assertIn("| timeout", harness.render_runs([result]))

    def test_residual_wait_is_an_approximation_clamped_at_zero(self) -> None:
        phase = harness.PhaseResult(
            phase="full",
            records_sent=1,
            request_bytes=1,
            request_write_seconds=0.0,
            response_wait_seconds=1.0,
            response_read_seconds=0.0,
            daemon_metrics={"input_deserialization_seconds": 0.4, "fold_seconds": 0.7, "output_serialization_seconds": 0.2},
        )
        self.assertEqual(phase.residual_wait_seconds, 0.0)
        phase.daemon_metrics = {}
        self.assertIsNone(phase.residual_wait_seconds)


if __name__ == "__main__":
    unittest.main()
