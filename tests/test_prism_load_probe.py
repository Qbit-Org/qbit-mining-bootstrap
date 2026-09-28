"""Tests for scripts/prism_load_probe.py (#541)."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prism_load_matrix  # noqa: E402
import prism_load_probe as probe  # noqa: E402

PG_TEST_FSYNC = """\
5 seconds per test
O_DIRECT supported on this platform for open_datasync and open_sync.

Compare file sync methods using one 8kB write:
(in wal_sync_method preference order, except fdatasync is Linux's default)
        open_datasync                      2051.170 ops/sec     488 usecs/op
        fdatasync                          2034.433 ops/sec     492 usecs/op
        fsync                              1011.234 ops/sec     989 usecs/op
        fsync_writethrough                             n/a
        open_sync                          1003.030 ops/sec     997 usecs/op

Compare file sync methods using two 8kB writes:
(in wal_sync_method preference order, except fdatasync is Linux's default)
        open_datasync                      1025.100 ops/sec     976 usecs/op
        fdatasync                          1900.000 ops/sec     526 usecs/op
"""


class Matrix(unittest.TestCase):
    def test_every_class_runs_each_backend_and_the_fsync_vms(self) -> None:
        result = probe.matrices("8, 16,32", "both", "3")
        self.assertEqual(len(result["probe"]["include"]), 6)
        self.assertEqual(
            {(e["class"], e["target_cache"]) for e in result["probe"]["include"]},
            {(c, t) for c in (8, 16, 32) for t in probe.TARGET_CACHES},
        )
        self.assertEqual(result["probe"]["include"][0]["runner"], "blacksmith-8vcpu-ubuntu-2404")
        self.assertEqual(len(result["fsync"]["include"]), 9)
        self.assertTrue(result["fsync_enabled"])

    def test_no_fsync_vms_leaves_a_valid_matrix_and_a_false_flag(self) -> None:
        result = probe.matrices("8", "sticky-disk", "0")
        self.assertEqual(len(result["fsync"]["include"]), 1)
        self.assertFalse(result["fsync_enabled"])
        self.assertEqual([e["target_cache"] for e in result["probe"]["include"]], ["sticky-disk"])

    def test_inputs_outside_what_the_workflow_knows_are_refused(self) -> None:
        for classes, cache, vms in (
            ("12", "both", "1"),
            ("8,x", "both", "1"),
            (" , ", "both", "1"),
            ("8", "s3", "1"),
            ("8", "both", "11"),
            ("8", "both", "-1"),
        ):
            with self.subTest(classes=classes, cache=cache, vms=vms):
                with self.assertRaises(probe.ProbeError):
                    probe.matrices(classes, cache, vms)

    def test_the_probe_presets_are_checked_in(self) -> None:
        presets = prism_load_matrix.load(prism_load_matrix.PRESETS)
        for name in probe.PRESETS:
            self.assertIn(name, presets)
        self.assertEqual(presets["short-plan-20k-window-1fe"]["args"]["--plan"], "short")
        self.assertEqual(presets["throughput-20k-window-1fe"]["args"]["--plan"], "d1")
        for name in probe.PRESETS:
            self.assertEqual(presets[name]["args"]["--window-shares"], 20000, name)

    def test_short_20k_is_not_in_the_nightly_set(self) -> None:
        presets = prism_load_matrix.load(prism_load_matrix.PRESETS)
        names = [e["preset"] for e in prism_load_matrix.select(presets, "nightly")]
        self.assertNotIn("short-plan-20k-window-1fe", names)


class PgTestFsync(unittest.TestCase):
    def test_the_one_write_fdatasync_line_is_the_flush_cost(self) -> None:
        self.assertEqual(
            probe.parse_pg_test_fsync(PG_TEST_FSYNC),
            {"method": "fdatasync", "ops_per_second": 2034.433, "usecs_per_op": 492.0},
        )

    def test_a_failed_run_is_unknown_not_zero(self) -> None:
        parsed = probe.parse_pg_test_fsync("pg_test_fsync failed; see above\n")
        self.assertIsNone(parsed["ops_per_second"])
        self.assertIsNone(parsed["usecs_per_op"])


class Measure(unittest.TestCase):
    def test_exit_code_wall_time_and_memory_are_recorded(self) -> None:
        result = probe.measure([sys.executable, "-c", "import sys; sys.exit(3)"], interval=0.05)
        self.assertEqual(result["exit_code"], 3)
        self.assertGreaterEqual(result["wall_seconds"], 0)
        if Path("/proc/meminfo").exists():
            self.assertGreater(result["peak_used_mib"], 0)
            self.assertGreaterEqual(result["peak_used_mib"], result["baseline_used_mib"])

    def test_meminfo_parsing(self) -> None:
        info = probe.meminfo_kib("MemTotal:  1000 kB\nMemAvailable:  250 kB\nHugePages_Total: 0\n")
        self.assertEqual(probe.used_kib(info), 750)
        self.assertIsNone(probe.used_kib({"MemTotal": 1}))


def report(**overrides) -> dict:
    document = {
        "dirty": False,
        "artifact_kind": "qualification",
        "versions": {
            "server_build_profile": "release",
            "server_revision_evidence": {"status": "established"},
        },
        "window": {"seed": {"seconds": 12.5, "rows": 20000}},
        "phases": [
            {"name": "warm_up", "duration_seconds": 30.0, "shortfall": 0, "completed": True},
            {"name": "steady_state", "duration_seconds": 60.0, "shortfall": 2, "completed": True},
        ],
        "time_to_usable_work": {"tips": [
            {"all_sessions_milliseconds": 100},
            {"all_sessions_milliseconds": 300},
            {"all_sessions_milliseconds": None},
        ]},
    }
    document.update(overrides)
    return document


class Rows(unittest.TestCase):
    def test_provenance_names_each_bypassed_check(self) -> None:
        self.assertEqual(probe.provenance(report()), "pass")
        self.assertTrue(probe.provenance(None).startswith("no report"))
        verdict = probe.provenance(report(
            dirty=True,
            artifact_kind="example",
            versions={"server_build_profile": "debug",
                      "server_revision_evidence": {"status": "unestablished"}},
        ))
        for part in ("dirty tree", "revision unestablished", "server not release",
                     "artifact_kind example"):
            self.assertIn(part, verdict)

    def test_a_run_directory_becomes_its_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            (run / "load-harness-report.json").write_text(json.dumps(report()))
            (run / "harness-exit-code").write_text("0\n")
            (run / "probe-measure.json").write_text(json.dumps(
                {"exit_code": 1, "wall_seconds": 400.0, "peak_used_mib": 9000.0,
                 "baseline_used_mib": 1000.0}))
            (run / "pg_test_fsync.txt").write_text(PG_TEST_FSYNC)
            result = probe.run_result(run)
        self.assertEqual(result["harness_exit_code"], 0)
        self.assertEqual(result["gate_exit_code"], 1)
        self.assertEqual(result["seed_seconds"], 12.5)
        self.assertEqual(result["phases"]["steady_state"]["shortfall"], 2)
        self.assertEqual(result["tip_last_notify_p99_ms"], 300)
        self.assertEqual(result["tips_missing_a_session"], 1)
        self.assertEqual(result["fsync"]["usecs_per_op"], 492.0)
        self.assertEqual(result["provenance"], "pass")

    def test_a_run_that_never_started_is_unknown_throughout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = probe.run_result(Path(directory, "missing"))
        self.assertIsNone(result["harness_exit_code"])
        self.assertIsNone(result["gate_exit_code"])
        self.assertIsNone(result["seed_seconds"])
        self.assertIsNone(result["tip_last_notify_p99_ms"])

    def test_nearest_rank_matches_the_gate(self) -> None:
        self.assertEqual(probe.nearest_rank([5, 1, 3], 0.99), 5)
        self.assertEqual(probe.nearest_rank(list(range(1, 101)), 0.99), 99)
        self.assertIsNone(probe.nearest_rank([], 0.99))


class Table(unittest.TestCase):
    def test_rows_on_disk_become_one_table(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / "short-plan-20k-window-1fe"
            run.mkdir()
            (run / "load-harness-report.json").write_text(json.dumps(report()))
            (run / "pg_test_fsync.txt").write_text(PG_TEST_FSYNC)
            rows = root / "rows"
            rows.mkdir()
            for path in (root / "build.json", root / "host.json"):
                path.write_text("{}")
            status = probe.main([
                "row", "--out", str(rows / "8-sticky-disk.json"), "--class", "8",
                "--runner", "blacksmith-8vcpu-ubuntu-2404", "--target-cache", "sticky-disk",
                "--target-restored", "false", "--target-bytes-before", "0",
                "--target-bytes-after", "", "--restore-seconds", "1.5", "--save-seconds", "",
                "--build", str(root / "build.json"), "--host", str(root / "host.json"),
                "--run", f"short-plan-20k-window-1fe={run}",
            ])
            self.assertEqual(status, 0)
            for vm, ops in ((1, "2034.433"), (2, "1017.2")):
                fsync = root / f"fsync-{vm}.txt"
                fsync.write_text(PG_TEST_FSYNC.replace("2034.433", ops))
                self.assertEqual(probe.main([
                    "fsync-row", "--out", str(rows / f"fsync-8-{vm}.json"), "--class", "8",
                    "--runner", "blacksmith-8vcpu-ubuntu-2404", "--vm", str(vm),
                    "--host", str(root / "host.json"), "--fsync", str(fsync),
                ]), 0)
            (rows / "unrelated.json").write_text(json.dumps({"schema": "other"}))
            text = probe.table(probe.load_rows(rows))
        self.assertIn("### 8 vCPU · sticky-disk (cold target)", text)
        self.assertIn("2,034 ops/s", text)
        self.assertIn("| Measure | short-plan-20k-window-1fe | throughput-20k-window-1fe |", text)
        self.assertIn("not run", text)
        self.assertIn("steady_state 2", text)
        self.assertIn("over 2 VMs, max/min 2.00", text)

    def test_the_run_argument_names_a_probe_preset(self) -> None:
        with self.assertRaises(probe.ProbeError):
            probe.parse_run("growth-20x=/tmp/x")
        with self.assertRaises(probe.ProbeError):
            probe.parse_run("short-plan-20k-window-1fe")


if __name__ == "__main__":
    unittest.main()
