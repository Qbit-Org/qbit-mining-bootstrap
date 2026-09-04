"""Issue #226 part 1: the process heap collector and its configuration switch.

The renderer-level contracts (family set, unavailable rendering, owner
reads, closed labels, no heap walk) live in tests/test_prism_metrics.py.
This module pins the collector itself: the glibc struct layout, the
resolve-once behaviour, the closed generation set, and the strict
PRISM_MALLOC_TELEMETRY switch through the coordinator config loader.
"""

from __future__ import annotations

import ctypes
import gc
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from lab.prism.coordinator_config import (
    DEFAULT_PRISM_MALLOC_TELEMETRY,
    env_malloc_telemetry,
    load_coordinator_config,
)
from lab.prism.metrics import MetricsRenderer
from lab.prism.process_telemetry import (
    PRISM_GC_GENERATIONS,
    MallInfo2,
    ProcessHeapTelemetry,
    resolve_mallinfo2,
)


def _host_has_mallinfo2() -> bool:
    try:
        resolve_mallinfo2()
    except Exception:  # noqa: BLE001 - any failure means "not here"
        return False
    return True


class MallInfo2LayoutTests(unittest.TestCase):
    def test_struct_matches_glibc_field_order(self) -> None:
        """Ten size_t fields in glibc's declared order; a wrong layout would
        read the wrong field as the arena on every scrape."""
        names = [name for name, _ctype in MallInfo2._fields_]
        self.assertEqual(
            names,
            [
                "arena",
                "ordblks",
                "smblks",
                "hblks",
                "hblkhd",
                "usmblks",
                "fsmblks",
                "uordblks",
                "fordblks",
                "keepcost",
            ],
        )
        for _name, ctype in MallInfo2._fields_:
            self.assertIs(ctype, ctypes.c_size_t)
        self.assertEqual(ctypes.sizeof(MallInfo2), 10 * ctypes.sizeof(ctypes.c_size_t))

    @unittest.skipUnless(
        sys.platform.startswith("linux") and _host_has_mallinfo2(),
        "glibc mallinfo2 is not available on this host",
    )
    def test_real_binding_reads_a_consistent_struct(self) -> None:
        """On a glibc 2.33+ host the bound symbol returns arena = in-use + free."""
        info = resolve_mallinfo2()()
        self.assertIsInstance(info, MallInfo2)
        self.assertGreater(int(info.arena), 0)
        self.assertGreaterEqual(int(info.uordblks), 0)
        self.assertGreaterEqual(int(info.fordblks), 0)
        self.assertEqual(int(info.arena), int(info.uordblks) + int(info.fordblks))


class ProcessHeapTelemetryTests(unittest.TestCase):
    def test_sample_carries_every_interpreter_reading(self) -> None:
        sample = ProcessHeapTelemetry(malloc_enabled=False).sample()
        self.assertGreater(sample.allocated_blocks, 0)
        self.assertGreaterEqual(sample.threads, 1)
        for reading in (
            sample.gc_trigger_count,
            sample.gc_collections,
            sample.gc_collected,
            sample.gc_uncollectable,
        ):
            self.assertEqual(len(reading), len(PRISM_GC_GENERATIONS))
            for value in reading:
                self.assertGreaterEqual(value, 0)
        self.assertFalse(sample.malloc_info_available)
        self.assertEqual(sample.malloc_arena_bytes, -1)
        self.assertEqual(sample.malloc_in_use_bytes, -1)
        self.assertEqual(sample.malloc_free_bytes, -1)
        self.assertEqual(sample.malloc_mmapped_bytes, -1)

    def test_generation_set_stays_closed_when_the_interpreter_reports_fewer(self) -> None:
        """A missing generation renders -1 rather than shrinking the series set."""
        with (
            mock.patch.object(gc, "get_count", return_value=(4, 2)),
            mock.patch.object(
                gc,
                "get_stats",
                return_value=[
                    {"collections": 9, "collected": 8, "uncollectable": 1},
                    {"collections": 3, "collected": 2, "uncollectable": 0},
                ],
            ),
        ):
            sample = ProcessHeapTelemetry(malloc_enabled=False).sample()
        self.assertEqual(sample.gc_trigger_count, (4, 2, -1))
        self.assertEqual(sample.gc_collections, (9, 3, -1))
        self.assertEqual(sample.gc_collected, (8, 2, -1))
        self.assertEqual(sample.gc_uncollectable, (1, 0, -1))

    def test_resolver_runs_once_and_its_failure_is_remembered(self) -> None:
        resolver = mock.Mock(side_effect=AttributeError("no mallinfo2"))
        telemetry = ProcessHeapTelemetry(mallinfo_resolver=resolver)
        for _ in range(3):
            sample = telemetry.sample()
            self.assertFalse(sample.malloc_info_available)
            self.assertEqual(sample.malloc_arena_bytes, -1)
        self.assertEqual(resolver.call_count, 1)

    def test_bound_symbol_that_fails_at_call_time_is_not_retried(self) -> None:
        bound = mock.Mock(side_effect=OSError("call failed"))
        telemetry = ProcessHeapTelemetry(mallinfo_resolver=lambda: bound)
        first = telemetry.sample()
        second = telemetry.sample()
        self.assertFalse(first.malloc_info_available)
        self.assertFalse(second.malloc_info_available)
        self.assertEqual(bound.call_count, 1)

    def test_bound_symbol_fields_map_to_the_sample(self) -> None:
        def fake_mallinfo2() -> MallInfo2:
            info = MallInfo2()
            info.arena = 20_000
            info.uordblks = 12_000
            info.fordblks = 8_000
            info.hblkhd = 1_024
            return info

        sample = ProcessHeapTelemetry(mallinfo_resolver=lambda: fake_mallinfo2).sample()
        self.assertTrue(sample.malloc_info_available)
        self.assertEqual(sample.malloc_arena_bytes, 20_000)
        self.assertEqual(sample.malloc_in_use_bytes, 12_000)
        self.assertEqual(sample.malloc_free_bytes, 8_000)
        self.assertEqual(sample.malloc_mmapped_bytes, 1_024)

    @unittest.skipUnless(
        sys.platform.startswith("linux") and _host_has_mallinfo2(),
        "glibc mallinfo2 is not available on this host",
    )
    def test_default_collector_binds_glibc_on_this_host(self) -> None:
        sample = ProcessHeapTelemetry().sample()
        self.assertTrue(sample.malloc_info_available)
        self.assertGreater(sample.malloc_arena_bytes, 0)
        self.assertGreaterEqual(sample.malloc_in_use_bytes, 0)
        self.assertGreaterEqual(sample.malloc_free_bytes, 0)
        self.assertGreaterEqual(sample.malloc_mmapped_bytes, 0)


class MallocTelemetrySwitchTests(unittest.TestCase):
    """PRISM_MALLOC_TELEMETRY: default on, strict values, renderer resolution."""

    def test_switch_defaults_to_on(self) -> None:
        self.assertTrue(DEFAULT_PRISM_MALLOC_TELEMETRY)
        self.assertTrue(env_malloc_telemetry(environ={}))

    def test_switch_accepts_boolean_tokens(self) -> None:
        for raw in ("1", "true", "YES", "On"):
            with self.subTest(raw=raw):
                self.assertTrue(
                    env_malloc_telemetry(environ={"PRISM_MALLOC_TELEMETRY": raw})
                )
        for raw in ("0", "false", "NO", "Off"):
            with self.subTest(raw=raw):
                self.assertFalse(
                    env_malloc_telemetry(environ={"PRISM_MALLOC_TELEMETRY": raw})
                )

    def test_switch_fails_closed_on_non_boolean_values(self) -> None:
        for raw in ("2", "banana", "tru"):
            with self.subTest(raw=raw):
                with self.assertRaisesRegex(
                    SystemExit, "PRISM_MALLOC_TELEMETRY must be a boolean"
                ):
                    env_malloc_telemetry(environ={"PRISM_MALLOC_TELEMETRY": raw})

    def test_coordinator_config_load_validates_the_switch_at_startup(self) -> None:
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
            config = load_coordinator_config(base)
            self.assertTrue(config.lifecycle.malloc_telemetry_enabled)
            disabled = load_coordinator_config(
                {**base, "PRISM_MALLOC_TELEMETRY": "0"}
            )
            self.assertFalse(disabled.lifecycle.malloc_telemetry_enabled)
            with self.assertRaisesRegex(
                SystemExit, "PRISM_MALLOC_TELEMETRY must be a boolean"
            ):
                load_coordinator_config(
                    {**base, "PRISM_MALLOC_TELEMETRY": "maybe"}
                )

    def test_renderer_resolves_the_switch_once_at_construction(self) -> None:
        # A port that pins the switch wins over the environment.
        pinned = MetricsRenderer(SimpleNamespace(malloc_telemetry_enabled=False))
        self.assertFalse(pinned._process_telemetry.malloc_enabled)
        # Otherwise the strict environment switch is read, once, at
        # construction; a later environment change does not reach a built
        # renderer.
        with mock.patch.dict(os.environ, {"PRISM_MALLOC_TELEMETRY": "0"}):
            from_environment = MetricsRenderer(SimpleNamespace())
        self.assertFalse(from_environment._process_telemetry.malloc_enabled)
        with mock.patch.dict(os.environ, {"PRISM_MALLOC_TELEMETRY": "1"}):
            self.assertFalse(from_environment._process_telemetry.malloc_enabled)
            self.assertTrue(
                MetricsRenderer(SimpleNamespace())._process_telemetry.malloc_enabled
            )
        with mock.patch.dict(os.environ, {"PRISM_MALLOC_TELEMETRY": ""}, clear=False):
            self.assertEqual(
                MetricsRenderer(SimpleNamespace())._process_telemetry.malloc_enabled,
                DEFAULT_PRISM_MALLOC_TELEMETRY,
            )

    def test_validated_config_snapshot_beats_the_ambient_environment(self) -> None:
        """A coordinator built with an explicit config is not overridden by env.

        The lifecycle snapshot is the value the config loader validated at
        startup. A caller that constructed the coordinator with telemetry
        disabled must not have mallinfo2 run anyway because the ambient
        environment happens to say otherwise, and a valid environment change
        after startup must not silently override the snapshot either.
        """
        disabled = SimpleNamespace(
            config=SimpleNamespace(
                lifecycle=SimpleNamespace(malloc_telemetry_enabled=False)
            )
        )
        with mock.patch.dict(os.environ, {"PRISM_MALLOC_TELEMETRY": "1"}):
            self.assertFalse(
                MetricsRenderer(disabled)._process_telemetry.malloc_enabled
            )
        enabled = SimpleNamespace(
            config=SimpleNamespace(
                lifecycle=SimpleNamespace(malloc_telemetry_enabled=True)
            )
        )
        with mock.patch.dict(os.environ, {"PRISM_MALLOC_TELEMETRY": "0"}):
            self.assertTrue(
                MetricsRenderer(enabled)._process_telemetry.malloc_enabled
            )
        # An explicit port pin still outranks the snapshot.
        pinned = SimpleNamespace(
            malloc_telemetry_enabled=False,
            config=SimpleNamespace(
                lifecycle=SimpleNamespace(malloc_telemetry_enabled=True)
            ),
        )
        self.assertFalse(MetricsRenderer(pinned)._process_telemetry.malloc_enabled)
        # With no snapshot at all the environment is still the fallback.
        with mock.patch.dict(os.environ, {"PRISM_MALLOC_TELEMETRY": "0"}):
            self.assertFalse(
                MetricsRenderer(
                    SimpleNamespace(config=SimpleNamespace())
                )._process_telemetry.malloc_enabled
            )

    def test_gc_trigger_count_is_not_described_as_retained_objects(self) -> None:
        """gc.get_count() is a trigger counter, and the family must say so.

        Naming it after retained objects sent the runbook's leak guidance at
        internal collector state; an operator reading it as a backlog during an
        incident would draw the wrong conclusion.
        """
        lines = MetricsRenderer(
            SimpleNamespace(), process_telemetry=ProcessHeapTelemetry()
        ).process_heap_metrics_lines()
        help_line = next(
            line
            for line in lines
            if line.startswith("# HELP qbit_prism_process_gc_trigger_count")
        )
        self.assertIn("trigger counters", help_line)
        self.assertIn("NOT a count of retained objects", help_line)
        self.assertFalse(
            [line for line in lines if "gc_pending_objects" in line],
            "the misleading family name must not come back",
        )


if __name__ == "__main__":
    unittest.main()
