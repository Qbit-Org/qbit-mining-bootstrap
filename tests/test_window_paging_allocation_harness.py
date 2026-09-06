#!/usr/bin/env python3
"""Bounded safety tests for ``tests/perf/window_paging_allocation.py``.

The measurement driver talks to a disposable ``--serve`` daemon over blocking
pipes. A stalled daemon must never hang a benchmark: every exchange, the
handshake included, runs under a wall-clock deadline that kills and reaps the
child and records the failure, and every exit path closes the pipes and the
stderr capture. These tests exercise that with a fake daemon that stalls in
three ways -- no handshake, after consuming the request, and without ever
reading it (so the request writer is blocked on a full pipe) -- and stay
well under a few seconds each. They also pin the provenance of the reported
peak resident set: every observation made before process exit, including
kernel ``VmHWM`` before a kill or normal close, is a lower bound rendered
with ``≥``; nothing observed is ``unavailable``.
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
if mode == "exit_after_handshake":
    sys.exit(0)
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
        # The watchdog sampled the kernel high-water mark before killing.
        if Path("/proc/self/status").exists():
            self.assertEqual(outcome["peak_rss_source"], "vmhwm_before_kill")
            self.assertIsInstance(outcome["peak_rss_mb"], float)
        else:
            self.assertEqual(outcome["peak_rss_source"], "unavailable")
            self.assertIsNone(outcome["peak_rss_mb"])
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

    def test_handshake_timeout_preserves_outcome_in_result_and_summary(self) -> None:
        fixture = harness.build_fixture(64, miners=2, page_size=16, small=2, large=3)
        os.environ["FAKE_DAEMON_MODE"] = "no_handshake"
        try:
            result = harness.run_variant(
                "fake", self.binary, fixture, oracle=None,
                memory_limit_mb=None, memory_margin_mb=0,
                sample_interval=0.01, exchange_timeout=0.5,
                stderr_dir=Path(self.tmp.name), log=lambda _text: None,
            )
        finally:
            os.environ.pop("FAKE_DAEMON_MODE", None)
        self.assertEqual(result.phases, [])
        self.assertIn("daemon failed to start", result.error or "")
        self.assertEqual(result.outcome["timed_out"], "handshake")
        self.assertEqual(result.outcome["signal"], 9)
        self.assertIn("stderr_tail", result.outcome)
        self.assertIn("peak_rss_source", result.outcome)
        _assert_reaped(self, result.outcome["pid"])
        summary = harness.render_summary([result])
        self.assertIn("signal 9, killed on handshake timeout", summary)
        self.assertIn("failed: daemon failed to start", summary)
        self.assertIn(harness.peak_source_label(result.outcome["peak_rss_source"]), summary)

    def test_close_on_a_live_daemon_reports_the_kernel_high_water_mark(self) -> None:
        daemon = self._daemon("stall_after_handshake")
        outcome = daemon.close()  # alive at close: VmHWM read live, then killed
        if Path("/proc/self/status").exists():
            self.assertEqual(outcome["peak_rss_source"], "vmhwm")
            self.assertIsInstance(outcome["peak_rss_mb"], float)
        else:
            self.assertEqual(outcome["peak_rss_source"], "unavailable")
            self.assertIsNone(outcome["peak_rss_mb"])
        self.assertEqual(outcome["signal"], 9)
        _assert_reaped(self, outcome["pid"])

    def test_self_exited_daemon_peak_is_a_lower_bound_or_unavailable(self) -> None:
        daemon = self._daemon("exit_after_handshake")
        daemon.process.wait(timeout=5.0)
        outcome = daemon.close()
        self.assertEqual(outcome["exit_code"], 0)
        self.assertEqual(outcome["peak_rss_source"], "unavailable")
        self.assertIsNone(outcome["peak_rss_mb"])
        _assert_reaped(self, outcome["pid"])

        fixture = harness.build_fixture(64, miners=2, page_size=16, small=2, large=3)
        os.environ["FAKE_DAEMON_MODE"] = "exit_after_handshake"
        try:
            result = harness.run_variant(
                "fake",
                self.binary,
                fixture,
                oracle=None,
                memory_limit_mb=None,
                memory_margin_mb=0,
                sample_interval=0.01,
                exchange_timeout=5.0,
                stderr_dir=Path(self.tmp.name),
                log=lambda _text: None,
            )
        finally:
            os.environ.pop("FAKE_DAEMON_MODE", None)
        self.assertEqual(result.outcome["exit_code"], 0)
        self.assertEqual(result.phases[0].status, "no_response")
        # Whatever the sampler caught before the exit, the figure is never
        # presented as a kernel high-water mark.
        source = result.outcome["peak_rss_source"]
        self.assertIn(source, ("lower_bound", "unavailable"))
        summary = harness.render_summary([result])
        if source == "lower_bound":
            self.assertIsInstance(result.outcome["peak_rss_mb"], float)
            self.assertIn("| ≥ ", summary)
            self.assertIn("lower bound", summary)
        else:
            self.assertIsNone(result.outcome["peak_rss_mb"])
            self.assertIn("| - | unavailable |", summary)
        self.assertNotIn("kernel VmHWM", summary)

    def test_resolve_peak_rss_provenance(self) -> None:
        exact = harness.resolve_peak_rss({"peak_rss_mb": 601.0, "peak_rss_source": "vmhwm"}, [900.0])
        self.assertEqual((exact["peak_rss_mb"], exact["peak_rss_source"]), (601.0, "vmhwm"))
        before_kill = harness.resolve_peak_rss(
            {"peak_rss_mb": 601.0, "peak_rss_source": "vmhwm_before_kill"}, [900.0]
        )
        self.assertEqual(before_kill["peak_rss_source"], "vmhwm_before_kill")
        legacy = harness.resolve_peak_rss({"peak_rss_mb": 601.0}, [])
        self.assertEqual(legacy["peak_rss_source"], "vmhwm")
        bound = harness.resolve_peak_rss({"peak_rss_mb": None}, [435.0, None, 0.0, 601.0, 12.5])
        self.assertEqual((bound["peak_rss_mb"], bound["peak_rss_source"]), (601.0, "lower_bound"))
        nothing = harness.resolve_peak_rss({"peak_rss_mb": None}, [None, 0.0])
        self.assertEqual((nothing["peak_rss_mb"], nothing["peak_rss_source"]), (None, "unavailable"))

    def test_peak_rendering_marks_all_live_observations_as_lower_bounds(self) -> None:
        self.assertEqual(harness.format_peak(601.4, "vmhwm"), "≥ 601")
        self.assertEqual(harness.format_peak(601.4, "vmhwm_before_kill"), "≥ 601")
        self.assertEqual(harness.format_peak(601.4, "lower_bound"), "≥ 601")
        self.assertEqual(harness.format_peak(2.857, "lower_bound", 2), "≥ 2.86")
        self.assertEqual(harness.format_peak(None, "unavailable"), "-")

        def run(source: str | None, peak: float | None) -> harness.RunResult:
            return harness.RunResult(
                "fixed", "bin", 210_000, 512, 16_000.0, None, 6144,
                [
                    harness.PhaseResult(
                        phase="full", records_sent=210_000, request_bytes=1,
                        request_write_seconds=0.15, response_wait_seconds=1.29,
                        response_read_seconds=0.12, daemon_metrics={"fold_seconds": 0.377},
                        status="prepared",
                    )
                ],
                {"exit_code": 0, "signal": None, "timed_out": None, "peak_rss_mb": peak,
                 "peak_rss_source": source, "peak_vsize_mb": 640.0},
                0.0, [],
            )

        exact_row = harness.render_summary([run("vmhwm", 601.0)]).splitlines()[-1]
        self.assertIn("| ≥ 601 | lower bound (kernel VmHWM at close) | ≥ 640 | ≥ 2.86 |", exact_row)
        kill_row = harness.render_summary([run("vmhwm_before_kill", 601.0)]).splitlines()[-1]
        self.assertIn("| ≥ 601 | lower bound (kernel VmHWM before watchdog kill) | ≥ 640 | ≥ 2.86 |", kill_row)
        bound_row = harness.render_summary([run("lower_bound", 601.0)]).splitlines()[-1]
        self.assertIn("| ≥ 601 | lower bound (highest earlier VmHWM/RSS observation) | ≥ 640 | ≥ 2.86 |", bound_row)
        none_row = harness.render_summary([run("unavailable", None)]).splitlines()[-1]
        self.assertIn("| - | unavailable | ≥ 640 | - |", none_row)
        header = harness.render_summary([]).splitlines()[0]
        self.assertIn("| peak RSS MiB | peak source |", header)
        self.assertNotIn("VmHWM", header)

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
