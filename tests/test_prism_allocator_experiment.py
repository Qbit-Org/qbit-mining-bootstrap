#!/usr/bin/env python3
"""Issue #226 part 2: the storm instrument's heap and allocator readings.

These pin the mechanism the allocator experiment relies on, not any byte
count: that a phase reading carries every field the soak runbook reads,
that the arena probe multiplies glibc arenas at the default and stops at a
configured cap, that the experiment driver strips its own options before
re-invoking the instrument, and that ``--heap-report`` produces the section
the runbook compares across settings.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import unittest
from dataclasses import fields
from pathlib import Path

from lab.prism.process_telemetry import ProcessHeapTelemetry, read_malloc_arena_count
from tests.prism_candidate_storm import (
    ARENA_PROBE_LIVE_BUFFERS,
    ARENA_PROBE_MAX_BYTES,
    ARENA_PROBE_MIN_BYTES,
    HeapReading,
    allocator_environment,
    allocator_experiment_child_args,
    run_arena_probe,
    take_heap_reading,
)

ROOT = Path(__file__).resolve().parents[1]
STORM = ROOT / "tests" / "prism_candidate_storm.py"

HEAP_READING_FIELDS = (
    "phase",
    "elapsed_seconds",
    "resident_bytes",
    "allocated_blocks",
    "threads",
    "malloc_info_available",
    "malloc_arena_bytes",
    "malloc_in_use_bytes",
    "malloc_free_bytes",
    "malloc_mmapped_bytes",
    "malloc_arena_count",
    "malloc_free_top_bytes",
    "malloc_free_bin_bytes",
    "anonymous_regions",
    "anonymous_bytes",
    "anonymous_regions_4mib_to_64mib",
    "heap_segment_bytes",
)


class HeapReadingTests(unittest.TestCase):
    def test_reading_carries_every_field_the_runbook_reads(self) -> None:
        self.assertEqual(tuple(field.name for field in fields(HeapReading)), HEAP_READING_FIELDS)
        reading = take_heap_reading("probe", ProcessHeapTelemetry(), 0.0)
        self.assertEqual(reading.phase, "probe")
        self.assertGreaterEqual(reading.elapsed_seconds, 0.0)
        self.assertGreater(reading.allocated_blocks, 0)
        self.assertGreaterEqual(reading.threads, 1)
        if Path("/proc/self/statm").exists():
            self.assertGreater(reading.resident_bytes, 0)
            self.assertGreaterEqual(reading.anonymous_regions, 1)
            self.assertGreaterEqual(reading.anonymous_regions_4mib_to_64mib, 0)
        if reading.malloc_info_available:
            self.assertGreaterEqual(reading.malloc_arena_count, 1)
            self.assertGreaterEqual(reading.malloc_arena_bytes, reading.malloc_in_use_bytes)

    def test_allocator_environment_reports_only_allocator_variables(self) -> None:
        with unittest.mock.patch.dict(
            os.environ,
            {"MALLOC_ARENA_MAX": "2", "PYTHONMALLOC": "malloc", "PRISM_UNRELATED": "x"},
        ):
            reported = allocator_environment()
        self.assertEqual(reported.get("MALLOC_ARENA_MAX"), "2")
        self.assertEqual(reported.get("PYTHONMALLOC"), "malloc")
        self.assertNotIn("PRISM_UNRELATED", reported)


class ArenaProbeTests(unittest.TestCase):
    def test_probe_buffers_land_in_glibc_arenas(self) -> None:
        """Over pymalloc's ceiling, under glibc's initial mmap threshold."""
        self.assertGreater(ARENA_PROBE_MIN_BYTES, 512)
        self.assertLess(ARENA_PROBE_MAX_BYTES, 128 * 1024)
        self.assertGreater(ARENA_PROBE_LIVE_BUFFERS, 0)

    def test_probe_multiplies_arenas_at_default_and_respects_the_cap(self) -> None:
        """glibc gives concurrent threads their own arenas until MALLOC_ARENA_MAX."""
        if read_malloc_arena_count() < 0:
            self.skipTest("malloc_info unavailable on this platform")
        script = (
            "import sys, json\n"
            f"sys.path.insert(0, {str(ROOT)!r})\n"
            "from tests.prism_candidate_storm import run_arena_probe\n"
            "print(json.dumps(run_arena_probe(12, 40)))\n"
        )
        uncapped = subprocess.run(
            [sys.executable, "-c", script],
            env={k: v for k, v in os.environ.items() if k != "MALLOC_ARENA_MAX"},
            capture_output=True,
            text=True,
            check=True,
        )
        capped = subprocess.run(
            [sys.executable, "-c", script],
            env={**os.environ, "MALLOC_ARENA_MAX": "3"},
            capture_output=True,
            text=True,
            check=True,
        )
        free_run = json.loads(uncapped.stdout.strip().splitlines()[-1])
        cap_run = json.loads(capped.stdout.strip().splitlines()[-1])
        self.assertEqual(free_run["threads"], 12)
        self.assertEqual(free_run["rounds"], 40)
        self.assertGreater(free_run["arena_count_after"], free_run["arena_count_before"])
        self.assertGreater(free_run["arena_count_after"], 3)
        self.assertLessEqual(cap_run["arena_count_after"], 3)
        for run in (free_run, cap_run):
            self.assertEqual(run["failures"], [])
            self.assertGreaterEqual(run["seconds"], 0.0)
            self.assertTrue(run["malloc_info_available"])
            # Freed rings stay on the arenas' free lists: the mechanism the
            # trim is later judged by.
            self.assertGreaterEqual(run["malloc_free_after"], run["malloc_free_before"])
        # In-process, with the cap of this process (whatever it is), the
        # probe still returns the full record.
        record = run_arena_probe(2, 5)
        self.assertEqual(set(record), set(free_run))
        self.assertEqual(record["failures"], [])

    def test_probe_reports_a_failing_worker_instead_of_wedging(self) -> None:
        """One worker that raises breaks the barrier for the rest; the probe returns."""
        calls = {"count": 0}

        def failing_allocate(size: int) -> bytes:
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("synthetic allocation failure")
            return bytes(size)

        started = time.monotonic()
        record = run_arena_probe(4, 3, allocate=failing_allocate)
        self.assertLess(time.monotonic() - started, 30.0)
        self.assertEqual(record["threads"], 4)
        self.assertTrue(record["failures"], record)
        self.assertTrue(
            any("synthetic allocation failure" in failure for failure in record["failures"]),
            record["failures"],
        )


class ExperimentDriverTests(unittest.TestCase):
    def test_child_args_strip_the_experiment_options_and_add_heap_report(self) -> None:
        self.assertEqual(
            allocator_experiment_child_args(
                ["--decide", "--allocator-experiment", "2,4", "--drain-per-row"]
            ),
            ["--decide", "--drain-per-row", "--heap-report"],
        )
        self.assertEqual(
            allocator_experiment_child_args(
                [
                    "--allocator-experiment=default",
                    "--allocator-mmap-threshold=131072",
                    "--heap-report",
                    "--candidates",
                    "8",
                ]
            ),
            ["--heap-report", "--candidates", "8"],
        )
        self.assertEqual(
            allocator_experiment_child_args(["--allocator-mmap-threshold", "131072"]),
            ["--heap-report"],
        )
        # --verbose never reaches a child: its logs would precede the JSON
        # the parent parses and the parse would fail after the whole storm.
        self.assertEqual(
            allocator_experiment_child_args(["--verbose", "--decide", "--allocator-experiment", "2"]),
            ["--decide", "--heap-report"],
        )

    def test_heap_report_section_is_produced_end_to_end(self) -> None:
        """A small storm with --heap-report carries the readings the runbook compares."""
        completed = subprocess.run(
            [
                sys.executable,
                str(STORM),
                "--candidates",
                "40",
                "--queue-depth",
                "8",
                "--heap-report",
                "--arena-probe-threads",
                "2",
                "--arena-probe-rounds",
                "10",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        report = json.loads(completed.stdout)
        heap = report["heap"]
        self.assertEqual(
            set(heap),
            {
                "python",
                "platform",
                "libc",
                "machine",
                "cpu_count",
                "allocator_env",
                "phase_seconds",
                "readings",
                "arena_probe",
                "malloc_trim",
            },
        )
        phases = [reading["phase"] for reading in heap["readings"]]
        self.assertEqual(
            phases,
            [
                "start",
                "after_seed_live",
                "after_restart_enumerate",
                "after_arena_probe",
                "after_release_gc",
                "after_malloc_trim",
            ],
        )
        for reading in heap["readings"]:
            self.assertEqual(set(reading), set(HEAP_READING_FIELDS))
        self.assertEqual(set(heap["phase_seconds"]), {"seed_live", "restart_enumerate"})
        self.assertEqual(heap["arena_probe"]["threads"], 2)
        self.assertEqual(heap["arena_probe"]["failures"], [])
        if heap["readings"][0]["malloc_info_available"]:
            trim = heap["malloc_trim"]
            self.assertIsNotNone(trim)
            self.assertEqual(trim["in_use_delta"], 0)
            self.assertLessEqual(trim["free_delta"], 0)
        # The storm sections the instrument already produced are untouched.
        self.assertEqual(report["input"], {"candidates": 40, "queue_depth": 8})
        self.assertIn("live", report)
        self.assertIn("restart", report)


if __name__ == "__main__":
    unittest.main()
