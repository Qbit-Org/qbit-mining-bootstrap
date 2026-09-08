#!/usr/bin/env python3
"""Issue #226 part 2: the bounded heap census, malloc_trim, allocator pins.

Every test here pins a safety property of an instrument that walks a
multi-gigabyte heap or takes glibc's arena locks: that it is off unless an
operator switches it on, that it is bounded when on, that no request path
can reach it, that the trim is paced and reports its effect through the
always-on telemetry, that the image's allocator environment cannot change
silently, and that the soak's resident-set bound is an automated verdict.
"""

from __future__ import annotations

import ast
import gc
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from lab.prism import process_telemetry
from lab.prism.coordinator_config import (
    DEFAULT_PRISM_HEAP_CENSUS_CONFIG,
    DEFAULT_PRISM_HEAP_CENSUS_MAX_BYTES,
    DEFAULT_PRISM_HEAP_CENSUS_MAX_SECONDS,
    DEFAULT_PRISM_HEAP_CENSUS_MIN_INTERVAL_SECONDS,
    DEFAULT_PRISM_HEAP_CENSUS_TOP_N,
    MAX_PRISM_HEAP_CENSUS_MAX_SECONDS,
    MAX_PRISM_HEAP_CENSUS_TOP_N,
    MIN_PRISM_MALLOC_TRIM_INTERVAL_SECONDS,
    env_heap_census_config,
    load_coordinator_config,
)
from lab.prism.metrics import MetricsRenderer
from lab.prism.process_telemetry import (
    HEAP_CENSUS_FORMAT,
    HEAP_CENSUS_RETAINED_FILES,
    HeapCensus,
    HeapCensusConfig,
    HeapCensusService,
    MallocTrimmer,
    ProcessHeapSample,
    ProcessHeapTelemetry,
    evaluate_rss_bound,
    heap_census_signal,
    install_heap_census,
    malloc_trim_signal,
    parse_malloc_info_xml,
    read_anonymous_map_shape,
    read_malloc_arena_count,
    read_malloc_info_summary,
    read_resident_memory_bytes,
)
from tests import prism_vardiff_test_support as support

ROOT = Path(__file__).resolve().parents[1]
PRISM_DIR = ROOT / "lab" / "prism"
DOCKERFILE = PRISM_DIR / "Dockerfile"
COMPOSE_FILE = ROOT / "compose.yaml"
ENV_EXAMPLE = ROOT / ".env.example"

# SIGRTMIN is Linux-only. The fake signal module carries a synthetic number so
# every fake-driven test runs on macOS too; only the test that delivers a real
# signal to this process gates its real-time-signal half on the platform.
_HAS_SIGRTMIN = hasattr(signal, "SIGRTMIN")
_SIGRTMIN = getattr(signal, "SIGRTMIN", 32)
_HAS_PROCFS = Path("/proc/self/statm").exists()

# The image's allocator environment, as decided in lab/prism/Dockerfile and
# argued in docs/mainnet-deployment.md. A change here must be a decision.
PINNED_MALLOC_ARENA_MAX = "2"

# Every name through which the walk or the trim can be reached. No module
# that serves a request may mention any of them.
CENSUS_ENTRY_POINTS = (
    "HeapCensus",
    "HeapCensusService",
    "install_heap_census",
    "MallocTrimmer",
    "resolve_malloc_trim",
    "gc.get_objects",
    "tracemalloc.take_snapshot",
    "malloc_trim",
)


# A malloc_info document in glibc's shape: per heap, one <size> row per
# non-empty size class (fastbin classes first), an <unsorted> row, the fast
# and rest totals (rest = bins + top, never fastbins), then the global totals.
_MALLOC_INFO_FIXTURE = """<malloc version="1">
<heap nr="0">
<sizes>
  <size from="33" to="33" total="198" count="6"/>
  <size from="49" to="49" total="294" count="6"/>
  <size from="145" to="145" total="145" count="1"/>
  <size from="20001" to="20001" total="40002" count="2"/>
  <unsorted from="5000" to="5000" total="5000" count="1"/>
</sizes>
<total type="fast" count="12" size="492"/>
<total type="rest" count="5" size="145147"/>
<system type="current" size="1000000"/>
<system type="max" size="1000000"/>
<aspace type="total" size="1000000"/>
<aspace type="mprotect" size="1000000"/>
</heap>
<heap nr="1">
<sizes>
</sizes>
<total type="fast" count="0" size="0"/>
<total type="rest" count="1" size="70000"/>
<system type="current" size="200000"/>
<system type="max" size="200000"/>
<aspace type="total" size="200000"/>
<aspace type="mprotect" size="200000"/>
</heap>
<total type="fast" count="12" size="492"/>
<total type="rest" count="6" size="215147"/>
<total type="mmap" count="0" size="0"/>
<system type="current" size="1200000"/>
<system type="max" size="1200000"/>
<aspace type="total" size="1200000"/>
<aspace type="mprotect" size="1200000"/>
</malloc>
"""


def _coordinator_service_environment() -> dict[str, str]:
    """The prism-coordinator service's environment mapping, from the raw text.

    Parsed without a YAML dependency: the service block runs from its key to
    the next two-space-indented key, and its environment entries are the
    six-space-indented ``NAME: value`` lines inside it.
    """
    lines = COMPOSE_FILE.read_text(encoding="utf-8").splitlines()
    start = lines.index("  prism-coordinator:")
    block: list[str] = []
    for line in lines[start + 1:]:
        if re.match(r"^  [A-Za-z0-9_-]+:\s*$", line):
            break
        block.append(line)
    environment: dict[str, str] = {}
    inside = False
    for line in block:
        if re.match(r"^    environment:\s*$", line):
            inside = True
            continue
        if inside and re.match(r"^    [A-Za-z0-9_-]+:", line):
            inside = False
        if inside:
            match = re.match(r"^      ([A-Z0-9_]+): (.*)$", line)
            if match:
                environment[match.group(1)] = match.group(2).strip()
    return environment


def _env_example_values() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def _config(**overrides: object) -> HeapCensusConfig:
    values: dict[str, object] = {
        "enabled": True,
        "output_dir": "unused",
        "top_n": 5,
        "max_bytes": 65_536,
        "max_seconds": 10.0,
        "min_interval_seconds": 60.0,
        "tracemalloc_enabled": False,
        "malloc_trim_signal_enabled": False,
        "malloc_trim_interval_seconds": 0.0,
    }
    values.update(overrides)
    return HeapCensusConfig(**values)  # type: ignore[arg-type]


def _sample(**overrides: int) -> ProcessHeapSample:
    values: dict[str, object] = {
        "allocated_blocks": 1_000,
        "gc_trigger_count": (1, 2, 3),
        "gc_collections": (4, 5, 6),
        "gc_collected": (7, 8, 9),
        "gc_uncollectable": (0, 0, 0),
        "threads": 64,
        "malloc_info_available": True,
        "malloc_arena_bytes": 10_000,
        "malloc_in_use_bytes": 6_000,
        "malloc_free_bytes": 4_000,
        "malloc_mmapped_bytes": 500,
    }
    values.update(overrides)
    return ProcessHeapSample(**values)  # type: ignore[arg-type]


class _FakeClock:
    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _FakeSignalModule:
    """Records registrations instead of installing real handlers."""

    SIGUSR1 = signal.SIGUSR1
    SIGRTMIN = _SIGRTMIN
    SIG_DFL = signal.SIG_DFL

    def __init__(self) -> None:
        self.registered: dict[int, object] = {}

    def signal(self, signum: int, handler: object) -> None:
        self.registered[signum] = handler

    def getsignal(self, signum: int) -> object:
        return self.registered.get(signum, self.SIG_DFL)


class HeapCensusDisabledByDefaultTests(unittest.TestCase):
    def test_heap_census_is_disabled_by_default(self) -> None:
        """Without the switch nothing is registered and the entry point is inert."""
        config = env_heap_census_config(environ={})
        self.assertFalse(config.enabled)
        self.assertFalse(config.tracemalloc_enabled)
        self.assertFalse(config.malloc_trim_signal_enabled)
        self.assertEqual(config.malloc_trim_interval_seconds, 0.0)
        self.assertFalse(config.anything_armed)
        # The shipped lifecycle default is what the loader produces from an
        # empty environment, switches and bounds alike.
        self.assertEqual(config, DEFAULT_PRISM_HEAP_CENSUS_CONFIG)
        fake_signal = _FakeSignalModule()
        with (
            mock.patch.object(
                gc,
                "get_objects",
                side_effect=AssertionError("gc.get_objects ran with the census off"),
            ) as get_objects,
            mock.patch.object(
                tracemalloc,
                "start",
                side_effect=AssertionError("tracemalloc started with the census off"),
            ) as tracemalloc_start,
        ):
            self.assertIsNone(install_heap_census(config, signal_module=fake_signal))
            self.assertEqual(fake_signal.registered, {})
            self.assertFalse(tracemalloc.is_tracing())
            with tempfile.TemporaryDirectory() as directory:
                census = HeapCensus(_config(enabled=False, output_dir=directory))
                self.assertIsNone(census.take("direct call"))
                self.assertIsNone(census.last_outcome)
                self.assertEqual(os.listdir(directory), [])
                service = HeapCensusService(_config(enabled=False, output_dir=directory))
                try:
                    self.assertFalse(service.request_census("signal"))
                    self.assertIsNone(service.dispatch("census", "direct"))
                    self.assertEqual(os.listdir(directory), [])
                finally:
                    service.close()
        get_objects.assert_not_called()
        tracemalloc_start.assert_not_called()
        # No worker thread exists for a census that never armed.
        self.assertFalse(
            any(
                thread.name == HeapCensusService.THREAD_NAME
                for thread in threading.enumerate()
            )
        )

    def test_census_switches_are_strict_and_bounded(self) -> None:
        """A typo or an unbounded value refuses startup rather than arming."""
        for bad in (
            {"PRISM_HEAP_CENSUS": "maybe"},
            {"PRISM_HEAP_CENSUS_TRACEMALLOC": "1"},
            {"PRISM_HEAP_CENSUS": "1", "PRISM_HEAP_CENSUS_TRACEMALLOC": "sometimes"},
            {"PRISM_HEAP_CENSUS_TOP_N": "0"},
            {"PRISM_HEAP_CENSUS_TOP_N": str(MAX_PRISM_HEAP_CENSUS_TOP_N + 1)},
            {"PRISM_HEAP_CENSUS_MAX_BYTES": "10"},
            {"PRISM_HEAP_CENSUS_MAX_BYTES": str(64 * 1024 * 1024)},
            {"PRISM_HEAP_CENSUS_MAX_SECONDS": "0"},
            {"PRISM_HEAP_CENSUS_MAX_SECONDS": str(MAX_PRISM_HEAP_CENSUS_MAX_SECONDS + 1)},
            {"PRISM_HEAP_CENSUS_MIN_INTERVAL_SECONDS": "-1"},
            {"PRISM_MALLOC_TRIM_SIGNAL": "yes please"},
            {
                "PRISM_MALLOC_TRIM_INTERVAL_SECONDS": str(
                    MIN_PRISM_MALLOC_TRIM_INTERVAL_SECONDS / 2
                )
            },
        ):
            with self.subTest(bad=bad), self.assertRaises(SystemExit):
                env_heap_census_config(environ=bad)
        armed = env_heap_census_config(
            environ={
                "PRISM_HEAP_CENSUS": "on",
                "PRISM_HEAP_CENSUS_TRACEMALLOC": "true",
                "PRISM_MALLOC_TRIM_SIGNAL": "1",
                "PRISM_MALLOC_TRIM_INTERVAL_SECONDS": "600",
                "PRISM_AUDIT_DIR": "/var/lib/qbit-prism/audit",
            }
        )
        self.assertTrue(armed.enabled)
        self.assertTrue(armed.tracemalloc_enabled)
        self.assertTrue(armed.malloc_trim_signal_enabled)
        self.assertEqual(armed.malloc_trim_interval_seconds, 600.0)
        self.assertEqual(armed.output_dir, "/var/lib/qbit-prism/audit/heap-census")
        self.assertEqual(armed.top_n, DEFAULT_PRISM_HEAP_CENSUS_TOP_N)
        self.assertEqual(armed.max_bytes, DEFAULT_PRISM_HEAP_CENSUS_MAX_BYTES)
        self.assertEqual(armed.max_seconds, DEFAULT_PRISM_HEAP_CENSUS_MAX_SECONDS)
        self.assertEqual(
            armed.min_interval_seconds, DEFAULT_PRISM_HEAP_CENSUS_MIN_INTERVAL_SECONDS
        )
        # The loader carries the validated snapshot, so a bad bound refuses
        # coordinator startup, not just a standalone read.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = {
                "QBIT_RPC_HOST": "qbit.example",
                "QBIT_RPC_USER": "rpc-user",
                "QBIT_RPC_PASSWORD": "rpc-password",
                "PRISM_ALLOW_MEMORY_LEDGER": "1",
                "PRISM_ALLOW_TEST_SIGNING_SEEDS": "1",
                "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY": "1",
                "PRISM_AUDIT_DIR": str(root),
                "PRISM_EVIDENCE_PATH": str(root / "evidence.json"),
            }
            shipped = load_coordinator_config(base)
            self.assertFalse(shipped.lifecycle.heap_census.enabled)
            self.assertFalse(shipped.lifecycle.heap_census.anything_armed)
            self.assertEqual(
                shipped.lifecycle.heap_census.output_dir, str(root / "heap-census")
            )
            with self.assertRaisesRegex(SystemExit, "PRISM_HEAP_CENSUS_TOP_N must be at most"):
                load_coordinator_config(
                    {**base, "PRISM_HEAP_CENSUS": "1", "PRISM_HEAP_CENSUS_TOP_N": "9999"}
                )
            with self.assertRaisesRegex(SystemExit, "PRISM_HEAP_CENSUS must be a boolean"):
                load_coordinator_config({**base, "PRISM_HEAP_CENSUS": "maybe"})
            loaded = load_coordinator_config({**base, "PRISM_HEAP_CENSUS": "1"})
            self.assertTrue(loaded.lifecycle.heap_census.enabled)
            self.assertEqual(
                loaded.lifecycle.heap_census.output_dir,
                str(loaded.audit.directory / "heap-census"),
            )


class HeapCensusBoundedReportTests(unittest.TestCase):
    def test_heap_census_writes_a_bounded_report_to_a_file(self) -> None:
        """On a heap with more types than N, the file holds exactly N and fits."""

        class Retained:
            pass

        # More distinct tracked types than the census may list, each with
        # enough instances to outrank the interpreter's own bookkeeping.
        kinds = [type(f"CensusProbe{index}", (Retained,), {}) for index in range(12)]
        keep = [kind() for kind in kinds for _ in range(50)]
        with tempfile.TemporaryDirectory() as directory:
            config = _config(output_dir=os.path.join(directory, "nested"), top_n=5, max_bytes=16_384)
            census = HeapCensus(config)
            outcome = census.take("test")
            self.assertIsNotNone(outcome)
            assert outcome is not None
            path = Path(outcome.path)
            self.assertTrue(path.exists())
            self.assertEqual(path.parent, Path(config.output_dir))
            self.assertLessEqual(path.stat().st_size, config.max_bytes)
            self.assertEqual(outcome.bytes_written, path.stat().st_size)
            report = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(report["format"], HEAP_CENSUS_FORMAT)
            self.assertEqual(report["reason"], "test")
            self.assertEqual(report["bounds"], {"top_n": 5, "max_bytes": 16_384, "max_seconds": 10.0})
            self.assertEqual(len(report["types_by_count"]), 5)
            self.assertEqual(len(report["types_by_bytes"]), 5)
            self.assertEqual(report["top_n_written"], 5)
            self.assertFalse(report["walk"]["truncated"])
            self.assertGreater(report["walk"]["distinct_types"], 5)
            self.assertEqual(report["walk"]["objects_walked"], report["walk"]["tracked_objects"])
            self.assertGreaterEqual(report["walk"]["tracked_objects"], len(keep))
            self.assertEqual(report["tracemalloc"], {"tracing": False})
            self.assertEqual(report["pid"], os.getpid())
            self.assertIn("resident_memory_bytes", report["process"])
            self.assertIn("malloc_arena_count", report["process"])
            self.assertIn("anonymous_map", report["process"])
            # Each entry is a name, a count and a shallow byte total; the
            # list is sorted by its key and nothing in it is a repr of an
            # object (no addresses, no payload).
            counts = [entry["count"] for entry in report["types_by_count"]]
            self.assertEqual(counts, sorted(counts, reverse=True))
            for entry in report["types_by_count"] + report["types_by_bytes"]:
                self.assertEqual(set(entry), {"type", "count", "shallow_bytes"})
                self.assertNotIn("0x", entry["type"])
            # The byte cap is met by shrinking N, never by cutting JSON.
            tight = HeapCensus(_config(output_dir=directory, top_n=200, max_bytes=4_096))
            tight_outcome = tight.take("tight")
            assert tight_outcome is not None
            tight_report = json.loads(Path(tight_outcome.path).read_text(encoding="utf-8"))
            self.assertLessEqual(Path(tight_outcome.path).stat().st_size, 4_096)
            self.assertLess(tight_report["top_n_written"], 200)
            self.assertTrue(tight_report["fit_to_max_bytes"])
            self.assertEqual(len(tight_report["types_by_count"]), tight_report["top_n_written"])
            # The walk cap: with no time at all the pass stops after its
            # first stride and says so, and the report is still written.
            clock = _FakeClock()
            capped = HeapCensus(
                _config(output_dir=directory, max_seconds=0.001),
                clock=_advancing(clock, 1.0),
            )
            capped_outcome = capped.take("capped")
            assert capped_outcome is not None
            self.assertTrue(capped_outcome.walk_truncated)
            self.assertLess(capped_outcome.objects_walked, capped_outcome.tracked_objects)
            capped_report = json.loads(Path(capped_outcome.path).read_text(encoding="utf-8"))
            self.assertTrue(capped_report["walk"]["truncated"])
            # Files are retained up to a fixed count; the oldest go first,
            # including the first report this census wrote.
            for index in range(HEAP_CENSUS_RETAINED_FILES + 3):
                self.assertIsNotNone(census.take(f"fill-{index}"))
            names = sorted(
                name
                for name in os.listdir(config.output_dir)
                if name.startswith("heap-census-")
            )
            self.assertEqual(len(names), HEAP_CENSUS_RETAINED_FILES)
            self.assertFalse(path.exists())
            self.assertFalse(any(name.endswith(".tmp") for name in os.listdir(config.output_dir)))
        del keep

    def test_types_sharing_a_printed_name_merge_rather_than_overwrite(self) -> None:
        """Two type objects with one printed name add up; neither hides the other."""

        def make_probe_class() -> type:
            class CensusMergeProbe:
                pass

            # The same short printed name for both, as two modules defining
            # one class name would have; the nested qualname would only
            # truncate into a third collision.
            CensusMergeProbe.__qualname__ = "CensusMergeProbe"
            return CensusMergeProbe

        first = make_probe_class()
        second = make_probe_class()
        self.assertIsNot(first, second)
        self.assertEqual(
            (first.__module__, first.__qualname__), (second.__module__, second.__qualname__)
        )
        # Two more whose long names truncate to the same printed name.
        long_a = type("L" * 150, (object,), {})
        long_b = type("L" * 149 + "Z", (object,), {})
        keep = [first() for _ in range(30)] + [second() for _ in range(20)]
        keep += [long_a() for _ in range(7)] + [long_b() for _ in range(5)]
        with tempfile.TemporaryDirectory() as directory:
            outcome = HeapCensus(_config(output_dir=directory, top_n=200)).take("merge")
            assert outcome is not None
            report = json.loads(Path(outcome.path).read_text(encoding="utf-8"))
        del keep
        by_name = {entry["type"]: entry for entry in report["types_by_count"]}
        merged = [name for name in by_name if name.endswith(".CensusMergeProbe")]
        self.assertEqual(len(merged), 1, merged)
        self.assertEqual(by_name[merged[0]]["count"], 50)
        self.assertGreater(by_name[merged[0]]["shallow_bytes"], 0)
        truncated = [name for name in by_name if name.endswith("...") and "LLLL" in name]
        self.assertEqual(len(truncated), 1, truncated)
        self.assertEqual(by_name[truncated[0]]["count"], 12)
        # The walk still reports how many type objects it saw, and how many
        # printed names they merged into.
        self.assertGreater(report["walk"]["distinct_types"], report["walk"]["distinct_type_names"])

    def test_retention_keeps_the_newest_reports_regardless_of_mtime(self) -> None:
        """A tight loop past the limit evicts the oldest even when every mtime ties."""
        with tempfile.TemporaryDirectory() as directory:
            census = HeapCensus(_config(output_dir=directory, top_n=3))
            total = HEAP_CENSUS_RETAINED_FILES + 8
            for index in range(total - 1):
                self.assertIsNotNone(census.take(f"fill-{index}"))
            # Every existing report now carries the same timestamp, so the
            # eviction that the final census triggers cannot lean on mtime.
            same_stamp = 1_700_000_000
            for name in os.listdir(directory):
                os.utime(os.path.join(directory, name), (same_stamp, same_stamp))
            self.assertIsNotNone(census.take(f"fill-{total - 1}"))
            names = sorted(
                name for name in os.listdir(directory) if name.startswith("heap-census-")
            )
            self.assertEqual(len(names), HEAP_CENSUS_RETAINED_FILES)
            reasons = [
                json.loads(Path(directory, name).read_text(encoding="utf-8"))["reason"]
                for name in names
            ]
            expected = [f"fill-{index}" for index in range(total - HEAP_CENSUS_RETAINED_FILES, total)]
            self.assertEqual(reasons, expected)
            # Name order is write order: the sequence is zero-padded, so the
            # tenth report does not sort before the ninth.
            sequences = [int(name.rsplit("-", 1)[1].split(".")[0]) for name in names]
            self.assertEqual(sequences, sorted(sequences))
            self.assertEqual(sequences, list(range(total - HEAP_CENSUS_RETAINED_FILES + 1, total + 1)))

    def test_heap_census_reports_tracemalloc_sites_only_while_tracing(self) -> None:
        """The site lists appear only when tracing, and are capped at N too."""
        self.assertFalse(tracemalloc.is_tracing())
        tracemalloc.start(1)
        try:
            junk = [bytearray(2_048) for _ in range(200)]
            with tempfile.TemporaryDirectory() as directory:
                outcome = HeapCensus(_config(output_dir=directory, top_n=3)).take("traced")
                assert outcome is not None
                self.assertTrue(outcome.tracemalloc_traced)
                report = json.loads(Path(outcome.path).read_text(encoding="utf-8"))
                traced = report["tracemalloc"]
                self.assertTrue(traced["tracing"])
                self.assertEqual(traced["frames"], 1)
                self.assertLessEqual(len(traced["by_line"]), 3)
                self.assertLessEqual(len(traced["by_file"]), 3)
                self.assertGreater(traced["traced_bytes"], 0)
                for entry in traced["by_line"] + traced["by_file"]:
                    self.assertEqual(set(entry), {"site", "bytes", "count"})
            del junk
        finally:
            tracemalloc.stop()

    def test_malloc_info_split_excludes_fastbins_from_bins(self) -> None:
        """Fastbin <size> rows are not part of <total type="rest">; top must not absorb them."""
        summary = parse_malloc_info_xml(_MALLOC_INFO_FIXTURE)
        self.assertTrue(summary.available)
        self.assertEqual(summary.arenas, 2)
        self.assertEqual(summary.fast_bytes, 492)
        # Heap 0: rows 198+294 (fast) + 145 + 40002 + unsorted 5000; rest is
        # bins plus top = 145147, so bins are 45147 and top is 100000.
        # Heap 1: nothing in any bin, rest 70000 is all top.
        self.assertEqual(summary.bin_bytes, 45_147)
        self.assertEqual(summary.top_bytes, 170_000)
        self.assertEqual(summary.system_bytes, 1_200_000)
        # The identity the split must keep: fast + bins + top is exactly the
        # sum of glibc's own fast and rest totals.
        self.assertEqual(summary.fast_bytes + summary.bin_bytes + summary.top_bytes, 492 + 215_147)
        # A document without heaps folds to zero arenas, not to a crash.
        empty = parse_malloc_info_xml('<malloc version="1">\n</malloc>\n')
        self.assertEqual((empty.arenas, empty.top_bytes, empty.bin_bytes), (0, 0, 0))
        # And the live reading on this host, where glibc offers it, is
        # internally consistent: no negative part, parts under system bytes.
        live = read_malloc_info_summary()
        if live.available:
            self.assertGreaterEqual(live.top_bytes, 0)
            self.assertGreaterEqual(live.bin_bytes, 0)
            self.assertGreaterEqual(live.fast_bytes, 0)
            self.assertLessEqual(live.top_bytes + live.bin_bytes + live.fast_bytes, live.system_bytes)

    def test_process_readings_are_available_on_this_host(self) -> None:
        """The readings the census and the storm instrument share are real here."""
        resident = read_resident_memory_bytes()
        shape = read_anonymous_map_shape()
        arenas = read_malloc_arena_count()
        if not Path("/proc/self/maps").exists():
            self.skipTest("no procfs on this platform")
        self.assertGreater(resident, 0)
        self.assertTrue(shape.available)
        self.assertGreaterEqual(shape.regions, 1)
        self.assertEqual(
            shape.regions, sum(shape.band_regions.values())
        )
        self.assertEqual(shape.total_bytes, sum(shape.band_bytes.values()))
        if ProcessHeapTelemetry().sample().malloc_info_available:
            self.assertGreaterEqual(arenas, 1)
        # A pid that does not exist reads as unavailable, not as zero.
        self.assertEqual(read_resident_memory_bytes(2**22 + 12345), -1)
        self.assertFalse(read_anonymous_map_shape(2**22 + 12345).available)


def _advancing(clock: _FakeClock, step: float):
    """A clock that moves ``step`` forward on every read."""

    def read() -> float:
        clock.advance(step)
        return clock.now

    return read


class HeapCensusRequestPathTests(unittest.TestCase):
    def test_heap_census_is_not_reachable_from_any_request_path(self) -> None:
        """Structurally: only main() in the coordinator can arm it, and no server module names it."""
        # 1. The coordinator references the installer in exactly one place,
        #    inside main(), after the shutdown handlers.
        source = (PRISM_DIR / "prism_coordinator.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        main_node = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "main"
        )
        main_span = range(main_node.lineno, main_node.end_lineno + 1)
        references = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Name) and node.id == "install_heap_census"
        ]
        self.assertEqual(len(references), 1, references)
        self.assertIn(references[0], main_span)
        for name in ("HeapCensus", "HeapCensusService", "MallocTrimmer", "gc.get_objects"):
            self.assertNotIn(name, source)
        # 2. No module that serves a request names any entry point. The
        #    census module itself, the config that validates its switches,
        #    and the coordinator's main() are the only exceptions.
        exempt = {"process_telemetry.py", "coordinator_config.py", "prism_coordinator.py"}
        offenders: dict[str, list[str]] = {}
        for module in sorted(PRISM_DIR.glob("*.py")):
            if module.name in exempt:
                continue
            text = module.read_text(encoding="utf-8")
            hits = [name for name in CENSUS_ENTRY_POINTS if name in text]
            if hits:
                offenders[module.name] = hits
        self.assertEqual(offenders, {})
        for served in ("audit_http.py", "public_read_service.py", "metrics.py"):
            self.assertTrue((PRISM_DIR / served).exists(), served)
        # 3. A full metrics render, the one path every scrape takes, never
        #    walks the heap or trims, even with the census armed in-process.
        server = support.coordinator()
        with tempfile.TemporaryDirectory() as directory:
            service = HeapCensusService(_config(output_dir=directory))
            try:
                with (
                    mock.patch.object(
                        gc,
                        "get_objects",
                        side_effect=AssertionError("gc.get_objects walked the heap on a scrape"),
                    ) as get_objects,
                    mock.patch.object(
                        tracemalloc,
                        "take_snapshot",
                        side_effect=AssertionError("tracemalloc snapshot on a scrape"),
                    ) as take_snapshot,
                    mock.patch.object(
                        HeapCensus,
                        "take",
                        side_effect=AssertionError("census taken on a scrape"),
                    ) as take,
                    mock.patch.object(
                        MallocTrimmer,
                        "trim_once",
                        side_effect=AssertionError("malloc_trim on a scrape"),
                    ) as trim,
                ):
                    document = MetricsRenderer(server).render()
                    self.assertIn("qbit_prism_process_allocated_blocks ", document)
                    # The signal handlers, run as the interpreter would run
                    # them, only wake the worker: no walk in handler context.
                    fake_signal = _FakeSignalModule()
                    armed = install_heap_census(
                        _config(output_dir=directory, malloc_trim_signal_enabled=True),
                        signal_module=fake_signal,
                        start_thread=False,
                        log=lambda _line: None,
                    )
                    assert armed is not None
                    try:
                        census_handler = fake_signal.registered[heap_census_signal(fake_signal)]
                        trim_handler = fake_signal.registered[malloc_trim_signal(fake_signal)]
                        census_handler(signal.SIGUSR1, None)
                        trim_handler(_SIGRTMIN + 1, None)
                    finally:
                        armed.close()
                get_objects.assert_not_called()
                take_snapshot.assert_not_called()
                take.assert_not_called()
                trim.assert_not_called()
            finally:
                service.close()
            self.assertEqual(
                [name for name in os.listdir(directory) if name.startswith("heap-census-")],
                [],
            )

    def test_signal_handlers_queue_work_for_the_worker_thread(self) -> None:
        """A real SIGUSR1 to this process produces a census on the worker, not inline.

        The trim half needs a real-time signal, which macOS does not have;
        there only the SIGUSR1 half runs.
        """
        with tempfile.TemporaryDirectory() as directory:
            previous_census = signal.getsignal(signal.SIGUSR1)
            previous_trim = signal.getsignal(_SIGRTMIN + 1) if _HAS_SIGRTMIN else None
            trimmer = MallocTrimmer(
                SimpleNamespace(sample=lambda: _sample()),  # type: ignore[arg-type]
                resolver=lambda: (lambda _pad: 1),
                rss_reader=lambda: 5,
                log=lambda _line: None,
            )
            service = HeapCensusService(
                _config(output_dir=directory, malloc_trim_signal_enabled=True, min_interval_seconds=0.0),
                trimmer=trimmer,
                log=lambda _line: None,
            )
            try:
                fake_signal = _FakeSignalModule()
                with mock.patch.object(
                    process_telemetry, "HeapCensusService", return_value=service
                ):
                    armed = install_heap_census(
                        service.config, signal_module=fake_signal, log=lambda _line: None
                    )
                self.assertIs(armed, service)
                # Install the recorded handlers for real, deliver the
                # signals to ourselves, and wait for the worker.
                signal.signal(signal.SIGUSR1, fake_signal.registered[signal.SIGUSR1])
                if _HAS_SIGRTMIN:
                    signal.signal(_SIGRTMIN + 1, fake_signal.registered[_SIGRTMIN + 1])
                try:
                    os.kill(os.getpid(), signal.SIGUSR1)
                    if _HAS_SIGRTMIN:
                        os.kill(os.getpid(), _SIGRTMIN + 1)
                    # Bounded wait: the worker has ten seconds to serve both.
                    deadline = time.monotonic() + 10.0
                    while time.monotonic() < deadline and (
                        service.census.last_outcome is None
                        or (_HAS_SIGRTMIN and trimmer.calls == 0)
                    ):
                        time.sleep(0.02)
                finally:
                    signal.signal(signal.SIGUSR1, previous_census)
                    if _HAS_SIGRTMIN:
                        signal.signal(_SIGRTMIN + 1, previous_trim)
                outcome = service.census.last_outcome
                self.assertIsNotNone(outcome)
                assert outcome is not None
                self.assertTrue(Path(outcome.path).exists())
                self.assertIn("signal:", json.loads(Path(outcome.path).read_text())["reason"])
                if _HAS_SIGRTMIN:
                    self.assertEqual(trimmer.calls, 1)
                    assert trimmer.last_result is not None
                    self.assertTrue(trimmer.last_result.reason.startswith("signal:"))
                else:
                    self.assertEqual(trimmer.calls, 0)
            finally:
                service.close()

    def test_close_disarms_signals_and_silences_wake(self) -> None:
        """After close() the previous handlers are back and a late signal writes nothing."""
        fake_signal = _FakeSignalModule()
        earlier_census = object()
        earlier_trim = object()
        fake_signal.registered[signal.SIGUSR1] = earlier_census
        fake_signal.registered[_SIGRTMIN + 1] = earlier_trim
        with tempfile.TemporaryDirectory() as directory:
            service = install_heap_census(
                _config(output_dir=directory, malloc_trim_signal_enabled=True),
                signal_module=fake_signal,
                start_thread=False,
                log=lambda _line: None,
            )
            assert service is not None
            census_handler = fake_signal.registered[signal.SIGUSR1]
            trim_handler = fake_signal.registered[_SIGRTMIN + 1]
            self.assertIsNot(census_handler, earlier_census)
            self.assertIsNot(trim_handler, earlier_trim)
            self.assertTrue(service.request_census("before close"))
            service.close()
            # The handlers this service armed are gone, and what they
            # replaced is back.
            self.assertIs(fake_signal.registered[signal.SIGUSR1], earlier_census)
            self.assertIs(fake_signal.registered[_SIGRTMIN + 1], earlier_trim)
            # A handler someone kept a reference to, or a request made after
            # close, is a no-op: no write into a recycled descriptor.
            with mock.patch.object(os, "write", side_effect=AssertionError("wrote after close")) as write:
                self.assertFalse(service.request_census("late"))
                self.assertFalse(service.request_trim("late"))
                self.assertFalse(service._wake(b"c"))
                census_handler(signal.SIGUSR1, None)
                trim_handler(_SIGRTMIN + 1, None)
            write.assert_not_called()
            service.close()  # idempotent
            self.assertEqual(
                [name for name in os.listdir(directory) if name.startswith("heap-census-")],
                [],
            )
        # With nothing registered beforehand the restore lands on SIG_DFL,
        # not on a stale handler.
        bare = _FakeSignalModule()
        service = install_heap_census(
            _config(output_dir="unused-never-written", malloc_trim_signal_enabled=True),
            signal_module=bare,
            start_thread=False,
            log=lambda _line: None,
        )
        assert service is not None
        service.close()
        self.assertIs(bare.registered[signal.SIGUSR1], signal.SIG_DFL)
        self.assertIs(bare.registered[_SIGRTMIN + 1], signal.SIG_DFL)


class MallocTrimTests(unittest.TestCase):
    def test_malloc_trim_action_is_bounded_and_reports_its_effect(self) -> None:
        """At most one trim per interval, deltas reported through the WP1a sample."""
        calls: list[int] = []
        samples = iter(
            [
                _sample(malloc_arena_bytes=10_000, malloc_in_use_bytes=6_000, malloc_free_bytes=4_000),
                _sample(malloc_arena_bytes=7_000, malloc_in_use_bytes=6_000, malloc_free_bytes=1_000),
                _sample(malloc_arena_bytes=7_000, malloc_in_use_bytes=6_000, malloc_free_bytes=1_000),
                _sample(malloc_arena_bytes=7_000, malloc_in_use_bytes=6_000, malloc_free_bytes=1_000),
            ]
        )
        rss = iter([9_000, 6_500, 6_500, 6_500])
        clock = _FakeClock()
        lines: list[str] = []

        def fake_trim(pad: int) -> int:
            calls.append(pad)
            return 1

        trimmer = MallocTrimmer(
            SimpleNamespace(sample=lambda: next(samples)),  # type: ignore[arg-type]
            resolver=lambda: fake_trim,
            clock=clock,
            rss_reader=lambda: next(rss),
            log=lines.append,
        )
        service = HeapCensusService(
            _config(enabled=False, min_interval_seconds=60.0, malloc_trim_interval_seconds=0.0),
            trimmer=trimmer,
            clock=clock,
            log=lines.append,
        )
        try:
            first = service.dispatch("trim", "signal:35")
            assert first is not None
            self.assertEqual(calls, [0])
            self.assertTrue(first.released)
            self.assertEqual(first.arena_delta, -3_000)
            self.assertEqual(first.free_delta, -3_000)
            self.assertEqual(first.in_use_delta, 0)
            self.assertEqual(first.resident_delta, -2_500)
            self.assertEqual(first.as_dict()["reason"], "signal:35")
            self.assertTrue(any(line.startswith("prism malloc_trim ") for line in lines))
            # Two more requests inside the interval are suppressed, and say so.
            clock.advance(10.0)
            self.assertIsNone(service.dispatch("trim", "signal:35"))
            clock.advance(10.0)
            self.assertIsNone(service.dispatch("trim", "signal:35"))
            self.assertEqual(calls, [0])
            self.assertEqual(service.suppressed["trim"], 2)
            self.assertTrue(any("malloc_trim suppressed" in line for line in lines))
            # Past the interval the next request runs.
            clock.advance(60.0)
            self.assertIsNotNone(service.dispatch("trim", "periodic"))
            self.assertEqual(calls, [0, 0])
            self.assertEqual(trimmer.calls, 2)
        finally:
            service.close()
        # The periodic action is paced by its own interval through the
        # same gate, and is not due before it.
        clock = _FakeClock()
        periodic = HeapCensusService(
            _config(enabled=False, min_interval_seconds=0.0, malloc_trim_interval_seconds=120.0),
            trimmer=MallocTrimmer(
                SimpleNamespace(sample=lambda: _sample()),  # type: ignore[arg-type]
                resolver=lambda: fake_trim,
                clock=clock,
                rss_reader=lambda: 1,
                log=lambda _line: None,
            ),
            clock=clock,
            log=lambda _line: None,
        )
        try:
            calls.clear()
            self.assertFalse(periodic.periodic_trim_due())
            self.assertTrue(periodic.run_once(0.0))
            self.assertEqual(calls, [])
            clock.advance(120.0)
            self.assertTrue(periodic.periodic_trim_due())
            self.assertTrue(periodic.run_once(0.0))
            self.assertEqual(calls, [0])
            self.assertFalse(periodic.periodic_trim_due())
        finally:
            periodic.close()
        # An absent symbol is remembered: one log line, no call, no retry.
        absent_lines: list[str] = []
        absent = MallocTrimmer(
            SimpleNamespace(sample=lambda: _sample()),  # type: ignore[arg-type]
            resolver=mock.Mock(side_effect=AttributeError("malloc_trim: symbol not found")),
            log=absent_lines.append,
        )
        self.assertIsNone(absent.trim_once("a"))
        self.assertIsNone(absent.trim_once("b"))
        self.assertEqual(absent.calls, 0)
        self.assertEqual(len(absent_lines), 1)

    def test_real_malloc_trim_releases_free_bytes_and_keeps_in_use(self) -> None:
        """On glibc the real call frees retained free chunks and touches nothing live."""
        telemetry = ProcessHeapTelemetry()
        if not telemetry.sample().malloc_info_available:
            self.skipTest("mallinfo2 unavailable on this platform")
        # Fragment the main arena a little: allocate and free medium chunks
        # that stay under the mmap threshold, so glibc keeps them.
        junk = [bytes(48 * 1024) for _ in range(512)]
        held = junk[::2]
        del junk
        result = MallocTrimmer(telemetry, log=lambda _line: None).trim_once("test")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsInstance(result.released, bool)
        self.assertEqual(result.in_use_delta, 0)
        self.assertLessEqual(result.free_delta, 0)
        self.assertLessEqual(result.arena_delta, 0)
        self.assertGreaterEqual(result.seconds, 0.0)
        del held


class AllocatorEnvironmentPinTests(unittest.TestCase):
    def test_allocator_environment_defaults_are_pinned(self) -> None:
        """The image sets exactly MALLOC_ARENA_MAX, at the decided value, overridably."""
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")
        args = dict(re.findall(r"^ARG\s+([A-Z0-9_]+)=(\S*)\s*$", dockerfile, re.MULTILINE))
        envs = dict(re.findall(r"^ENV\s+([A-Z0-9_]+)=(\S*)\s*$", dockerfile, re.MULTILINE))
        self.assertEqual(args.get("MALLOC_ARENA_MAX"), PINNED_MALLOC_ARENA_MAX)
        self.assertEqual(envs.get("MALLOC_ARENA_MAX"), "${MALLOC_ARENA_MAX}")
        allocator_envs = {
            name for name in envs
            if name.startswith("MALLOC_") or name in ("GLIBC_TUNABLES", "PYTHONMALLOC")
        }
        self.assertEqual(allocator_envs, {"MALLOC_ARENA_MAX"})
        self.assertNotIn("PYTHONMALLOC", dockerfile.replace("# PYTHONMALLOC is deliberately unset", ""))
        # Never an empty allocator value: glibc reads "" as 0.
        for name, value in {**args, **envs}.items():
            if name.startswith("MALLOC_"):
                self.assertNotEqual(value, "", name)
        # The ARG precedes the ENV that expands it, and both precede CMD.
        self.assertLess(dockerfile.index("ARG MALLOC_ARENA_MAX="), dockerfile.index("ENV MALLOC_ARENA_MAX="))
        self.assertLess(dockerfile.index("ENV MALLOC_ARENA_MAX="), dockerfile.index("CMD ["))
        # The deployment notes record the same decision.
        deployment = (ROOT / "docs" / "mainnet-deployment.md").read_text(encoding="utf-8")
        self.assertIn(f"MALLOC_ARENA_MAX={PINNED_MALLOC_ARENA_MAX}", deployment)
        # A dotenv value reaches the container only through the compose
        # passthrough, whose default must equal the image default so the
        # two cannot drift, and must never be empty (glibc reads "" as 0).
        environment = _coordinator_service_environment()
        self.assertEqual(
            environment.get("MALLOC_ARENA_MAX"),
            f"${{MALLOC_ARENA_MAX:-{PINNED_MALLOC_ARENA_MAX}}}",
        )
        self.assertEqual(_env_example_values().get("MALLOC_ARENA_MAX"), PINNED_MALLOC_ARENA_MAX)
        # glibc honours the variable: a child started with it capped cannot
        # grow past the cap however many threads allocate. This is the
        # mechanism the soak relies on, proven on this host.
        probe = (
            "import ctypes, threading, sys\n"
            "sys.path.insert(0, %r)\n"
            "from lab.prism.process_telemetry import read_malloc_arena_count\n"
            "barrier = threading.Barrier(16)\n"
            "def work():\n"
            "    keep = [bytes(8192) for _ in range(32)]\n"
            "    barrier.wait()\n"
            "    del keep\n"
            "threads = [threading.Thread(target=work) for _ in range(16)]\n"
            "[t.start() for t in threads]; [t.join() for t in threads]\n"
            "print(read_malloc_arena_count())\n"
        ) % str(ROOT)
        if read_malloc_arena_count() < 0:
            self.skipTest("malloc_info unavailable on this platform")
        capped = subprocess.run(
            [sys.executable, "-c", probe],
            env={**os.environ, "MALLOC_ARENA_MAX": PINNED_MALLOC_ARENA_MAX},
            capture_output=True,
            text=True,
            check=True,
        )
        uncapped_env = {k: v for k, v in os.environ.items() if k != "MALLOC_ARENA_MAX"}
        uncapped = subprocess.run(
            [sys.executable, "-c", probe],
            env=uncapped_env,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertLessEqual(int(capped.stdout.strip()), int(PINNED_MALLOC_ARENA_MAX))
        self.assertGreater(int(uncapped.stdout.strip()), int(PINNED_MALLOC_ARENA_MAX))


    def test_census_switches_pass_through_compose_with_the_shipped_defaults(self) -> None:
        """Every switch reaches the coordinator container, defaulting to what the config ships."""
        environment = _coordinator_service_environment()
        example = _env_example_values()
        expected_defaults = {
            "PRISM_HEAP_CENSUS": "0",
            "PRISM_HEAP_CENSUS_TOP_N": str(DEFAULT_PRISM_HEAP_CENSUS_TOP_N),
            "PRISM_HEAP_CENSUS_MAX_BYTES": str(DEFAULT_PRISM_HEAP_CENSUS_MAX_BYTES),
            "PRISM_HEAP_CENSUS_MAX_SECONDS": f"{DEFAULT_PRISM_HEAP_CENSUS_MAX_SECONDS:g}",
            "PRISM_HEAP_CENSUS_MIN_INTERVAL_SECONDS": f"{DEFAULT_PRISM_HEAP_CENSUS_MIN_INTERVAL_SECONDS:g}",
            "PRISM_HEAP_CENSUS_TRACEMALLOC": "0",
            "PRISM_MALLOC_TRIM_SIGNAL": "0",
            "PRISM_MALLOC_TRIM_INTERVAL_SECONDS": "0",
        }
        for name, default in expected_defaults.items():
            with self.subTest(name=name):
                self.assertEqual(environment.get(name), f"${{{name}:-{default}}}")
                self.assertEqual(example.get(name), default)
        # The directory is optional: an empty passthrough must reach the
        # coordinator as unset, and the reader must then use the default.
        self.assertEqual(environment.get("PRISM_HEAP_CENSUS_DIR"), "${PRISM_HEAP_CENSUS_DIR:-}")
        config = env_heap_census_config(
            environ={"PRISM_HEAP_CENSUS_DIR": "", "PRISM_AUDIT_DIR": "/var/lib/qbit-prism/audit"}
        )
        self.assertEqual(config.output_dir, "/var/lib/qbit-prism/audit/heap-census")
        # The defaults the passthroughs carry are the ones the loader
        # produces from an empty environment.
        shipped = env_heap_census_config(environ={})
        passthrough_environ = {
            name: default for name, default in expected_defaults.items()
        }
        passthrough_environ["PRISM_HEAP_CENSUS_DIR"] = ""
        self.assertEqual(env_heap_census_config(environ=passthrough_environ), shipped)


class RssBoundCheckTests(unittest.TestCase):
    def test_rss_bound_check_fails_when_the_stated_multiple_is_exceeded(self) -> None:
        """Synthetic series: the #226 slope fails, a flat post-warm-up series passes."""
        mib = 1024 * 1024
        hour = 3600.0
        # The mainnet shape: 140 MiB at start, +390 MB every hour, for 24 h.
        leaking = [(h * hour, int(140 * mib + h * 390_000_000)) for h in range(25)]
        verdict = evaluate_rss_bound(leaking, warmup_seconds=hour, multiple=2.0)
        self.assertFalse(verdict.passed)
        # The baseline is the warm-up peak (the one-hour sample), so the
        # bound is twice that and the third-hour sample is the first over it.
        self.assertEqual(verdict.baseline_bytes, int(140 * mib + 390_000_000))
        self.assertEqual(verdict.first_breach_at_seconds, 3 * hour)
        self.assertGreater(verdict.peak_ratio, 2.0)
        self.assertGreater(verdict.slope_bytes_per_hour or 0, 380_000_000)
        self.assertTrue(any("exceeded 2x" in reason for reason in verdict.reasons))
        # Bounded: warm-up to 400 MiB (the window materializes), then a
        # storm to 613 MiB that drains, then flat. Under 2x of 400 MiB.
        bounded = [(0.0, 140 * mib), (0.5 * hour, 400 * mib), (hour, 390 * mib)]
        bounded += [(h * hour, 613 * mib if h == 3 else 400 * mib + (h % 3) * mib) for h in range(2, 25)]
        verdict = evaluate_rss_bound(bounded, warmup_seconds=hour, multiple=2.0)
        self.assertTrue(verdict.passed, verdict.reasons)
        self.assertEqual(verdict.baseline_bytes, 400 * mib)
        self.assertEqual(verdict.bound_bytes, 800 * mib)
        self.assertEqual(verdict.peak_bytes, 613 * mib)
        self.assertIsNone(verdict.first_breach_at_seconds)
        # The same series fails a tighter multiple, at the storm.
        verdict = evaluate_rss_bound(bounded, warmup_seconds=hour, multiple=1.5)
        self.assertFalse(verdict.passed)
        self.assertEqual(verdict.first_breach_at_seconds, 3 * hour)
        # Absolute epoch timestamps are re-based; -1 samples are ignored.
        epoch = 1_788_000_000.0
        shifted = [(epoch + t, r) for t, r in bounded] + [(epoch + 5 * hour, -1)]
        self.assertTrue(evaluate_rss_bound(shifted, warmup_seconds=hour, multiple=2.0).passed)
        # Too short a series, or one with nothing after warm-up, is not a pass.
        short = evaluate_rss_bound(bounded[:3], warmup_seconds=hour, multiple=2.0)
        self.assertFalse(short.passed)
        self.assertIn("no sample after the warm-up window", short.reasons)
        brief = evaluate_rss_bound(bounded, warmup_seconds=hour, multiple=2.0, min_span_seconds=48 * hour)
        self.assertFalse(brief.passed)
        self.assertTrue(any("spans" in reason for reason in brief.reasons))
        self.assertFalse(evaluate_rss_bound([], warmup_seconds=hour, multiple=2.0).passed)
        # The optional slope guard catches a slow leak that stays under 2x
        # (15 MiB/h from a 420 MiB warm-up peak ends the day at 760 MiB).
        slow = [(h * hour, int(400 * mib + h * 15 * mib)) for h in range(25)]
        self.assertTrue(evaluate_rss_bound(slow, warmup_seconds=hour, multiple=2.0).passed)
        guarded = evaluate_rss_bound(
            slow, warmup_seconds=hour, multiple=2.0, max_slope_bytes_per_hour=10 * mib
        )
        self.assertFalse(guarded.passed)
        self.assertTrue(any("slope" in reason for reason in guarded.reasons))
        with self.assertRaises(ValueError):
            evaluate_rss_bound(bounded, warmup_seconds=hour, multiple=1.0)

    def test_rss_bound_cli_exit_status_is_the_verdict(self) -> None:
        """The operator's command exits 0 on pass, 1 on fail, 2 on bad input."""
        mib = 1024 * 1024
        with tempfile.TemporaryDirectory() as directory:
            passing = Path(directory) / "pass.csv"
            passing.write_text(
                "# epoch_seconds,rss_bytes\n"
                + "".join(f"{h * 3600},{400 * mib}\n" for h in range(25)),
                encoding="utf-8",
            )
            failing = Path(directory) / "fail.csv"
            failing.write_text(
                "".join(f"{h * 3600},{140 * mib + h * 390_000_000}\n" for h in range(25)),
                encoding="utf-8",
            )
            broken = Path(directory) / "broken.csv"
            broken.write_text("not,a,number\n", encoding="utf-8")
            base = [sys.executable, "-m", "lab.prism.process_telemetry", "rss-bound"]
            ok = subprocess.run(
                [*base, "--samples", str(passing)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(ok.returncode, 0, ok.stderr)
            self.assertTrue(json.loads(ok.stdout)["passed"])
            bad = subprocess.run(
                [*base, "--samples", str(failing)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(bad.returncode, 1, bad.stderr)
            self.assertFalse(json.loads(bad.stdout)["passed"])
            self.assertEqual(json.loads(bad.stdout)["first_breach_at_seconds"], 10800.0)
            usage = subprocess.run(
                [*base, "--samples", str(broken)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(usage.returncode, 2)
            sample = subprocess.run(
                [sys.executable, "-m", "lab.prism.process_telemetry", "rss-sample", "--pid", str(os.getpid())],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            epoch, rss = sample.stdout.strip().split(",")
            self.assertGreater(int(epoch), 0)
            if _HAS_PROCFS:
                self.assertEqual(sample.returncode, 0, sample.stderr)
                self.assertGreater(int(rss), 0)
            else:
                # Documented contract off Linux: the -1 sentinel and exit 1,
                # so a capture loop cannot mistake "unavailable" for a value.
                self.assertEqual(sample.returncode, 1, sample.stderr)
                self.assertEqual(int(rss), -1)


if __name__ == "__main__":
    unittest.main()
