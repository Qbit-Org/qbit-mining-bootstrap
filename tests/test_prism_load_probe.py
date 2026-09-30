"""Tests for scripts/prism_load_probe.py (#541, #542)."""

from __future__ import annotations

import contextlib
import io
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

    def test_a_run_marked_failed_after_the_one_write_section_is_unknown(self) -> None:
        for marker in ("pg_test_fsync failed\n", "pg_test_fsync failed; see above\n"):
            parsed = probe.parse_pg_test_fsync(PG_TEST_FSYNC + marker)
            self.assertIsNone(parsed["ops_per_second"], marker)
            self.assertIsNone(parsed["usecs_per_op"], marker)

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


class Host(unittest.TestCase):
    def test_the_runner_label_leads_the_fingerprint_when_given(self) -> None:
        # #549: each nightly preset's artifact names the runner it asked for
        # beside the disk facts.
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "host.json"
            self.assertEqual(probe.main(["host", "--out", str(out), "--dir", tmp,
                                         "--runner", "blacksmith-8vcpu-ubuntu-2404"]), 0)
            facts = json.loads(out.read_text())
            self.assertEqual(next(iter(facts)), "runner_label")
            self.assertEqual(facts["runner_label"], "blacksmith-8vcpu-ubuntu-2404")
            self.assertIn("block_device", facts)
            self.assertIn("options", facts["filesystem"])

            self.assertEqual(probe.main(["host", "--out", str(out), "--dir", tmp]), 0)
            self.assertNotIn("runner_label", json.loads(out.read_text()))


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
        # A tip some session never got work on leaves the p99 unmeasured,
        # as the gate has it, while the missing count is kept.
        self.assertIsNone(result["tip_last_notify_p99_ms"])
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

    def test_the_tip_p99_is_measured_when_every_tip_was_served(self) -> None:
        served = {"tips": [{"all_sessions_milliseconds": 100},
                           {"all_sessions_milliseconds": 300}]}
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "load-harness-report.json").write_text(
                json.dumps(report(time_to_usable_work=served)))
            result = probe.run_result(Path(directory))
        self.assertEqual(result["tip_last_notify_p99_ms"], 300)
        self.assertEqual(result["tips_missing_a_session"], 0)

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
        self.assertIn("### 8 vCPU · sticky-disk · repeat 1 (cold target)", text)
        self.assertIn("2,034 ops/s", text)
        # Without the plan, the columns are the presets the rows ran.
        self.assertIn("| Measure | short-plan-20k-window-1fe |\n", text)
        self.assertNotIn("throughput-20k-window-1fe", text)
        self.assertIn("steady_state 2", text)
        self.assertIn("over 2 VMs, max/min 2.00", text)

    def test_a_vm_whose_pg_test_fsync_failed_is_counted_as_unknown(self) -> None:
        self.assertEqual(
            probe.spread([2000.0, None, 1000.0]),
            "1,000–2,000 over 2 VMs, max/min 2.00; 1 of 3 VMs unknown (pg_test_fsync failed or "
            "the job wrote no row)",
        )
        self.assertEqual(probe.spread([None, None]),
                         "unknown; 2 of 2 VMs unknown (pg_test_fsync failed or the job wrote no row)")

    def test_a_planned_job_that_wrote_no_row_is_named_and_counted(self) -> None:
        expected = probe.matrices("8,16", "sticky-disk", "3")
        rows = [
            {"schema": probe.ROW_SCHEMA, "kind": "fsync", "class": 8, "vm": vm,
             "runner": "r", "host": {}, "fsync": {"ops_per_second": ops}}
            for vm, ops in ((1, 2000.0), (2, 1000.0))
        ]
        text = probe.table(rows, expected)
        for size in (8, 16):
            self.assertIn(f"### {size} vCPU · sticky-disk · repeat 1: no row", text)
        self.assertIn("| 8 vCPU | 1,000–2,000 over 2 VMs, max/min 2.00; 1 of 3 VMs unknown", text)
        self.assertIn("| 16 vCPU | unknown; 3 of 3 VMs unknown", text)
        # With the plan, a planned run a row lacks is shown as not run.
        row = probe_row(1, [run()])
        row["class"] = 8
        planned = probe.matrices("8", "sticky-disk", "0", f"{PRESET},short-plan-20k-window-1fe")
        text = probe.table([row], planned)
        self.assertIn(f"| Measure | {PRESET} | short-plan-20k-window-1fe |", text)
        self.assertIn("| not run |", text)
        # With no fsync jobs planned, none is expected.
        self.assertNotIn("fdatasync across VMs", probe.table([], probe.matrices("8", "both", "0")))

    def test_the_run_argument_names_a_probe_preset(self) -> None:
        with self.assertRaises(probe.ProbeError):
            probe.parse_run("growth-20x=/tmp/x")
        with self.assertRaises(probe.ProbeError):
            probe.parse_run("short-plan-20k-window-1fe")


class RepeatPlan(unittest.TestCase):
    """#542: repeats of one commit per class, as separate jobs and in-job."""

    def test_repeats_are_separate_jobs_and_the_job_timeout_covers_every_run(self) -> None:
        plan = probe.matrices("8,32", "sticky-disk", "0", "throughput-20k-window-1fe",
                              "10", "2")
        self.assertEqual(len(plan["probe"]["include"]), 20)
        self.assertEqual({e["repeat"] for e in plan["probe"]["include"]}, set(range(1, 11)))
        self.assertEqual(plan["presets"], "throughput-20k-window-1fe")
        self.assertEqual(plan["in_job_repeats"], 2)
        # Two 60-minute runs plus slack, inside setup, build and reports.
        self.assertEqual(plan["runs_minutes"], 2 * 60 + probe.RUN_STEP_SLACK_MINUTES)
        self.assertEqual(plan["job_minutes"], probe.SETUP_MINUTES + probe.BUILD_MINUTES
                         + plan["runs_minutes"] + probe.REPORT_MINUTES)
        ceiling = plan["ceiling"]
        self.assertEqual(ceiling["probe_jobs"], 20)
        self.assertEqual(ceiling["two_vcpu_minutes"],
                         10 * plan["job_minutes"] * (4 + 16) + probe.OVERHEAD_JOB_MINUTES)
        self.assertAlmostEqual(ceiling["usd"], round(ceiling["two_vcpu_minutes"] * 0.004, 2))
        self.assertIn("Spend ceiling", probe.plan_summary(plan))

    def test_the_default_plan_is_541s(self) -> None:
        plan = probe.matrices("8,16,32", "both", "3")
        self.assertEqual(plan["presets"], ",".join(probe.PRESETS))
        self.assertEqual(plan["in_job_repeats"], 1)
        self.assertEqual({e["repeat"] for e in plan["probe"]["include"]}, {1})

    def test_a_deprecated_name_runs_its_preset_and_duplicates_collapse(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()) as warned:
            plan = probe.matrices("16", "sticky-disk", "0",
                                  "d1-473-400k-fe2-async, throughput-400k-window-2fe-async,"
                                  "throughput-400k-window-4fe-async")
        self.assertIn("d1-473-400k-fe2-async is deprecated", warned.getvalue())
        self.assertEqual(plan["presets"],
                         "throughput-400k-window-2fe-async,throughput-400k-window-4fe-async")

    def test_plans_the_workflow_cannot_run_are_refused_before_any_runner(self) -> None:
        for presets, repeats, in_job, needle in (
            ("throughput-20k-window-1fe", "0", "1", "repeats"),
            ("throughput-20k-window-1fe", "21", "1", "repeats"),
            ("throughput-20k-window-1fe", "1.5", "1", "repeats"),
            ("throughput-20k-window-1fe", "+2", "1", "repeats"),
            ("throughput-20k-window-1fe", "", "1", "repeats"),
            ("throughput-20k-window-1fe", "1", "6", "in-job repeats"),
            ("throughput-20k-window-1fe", "1", "0", "in-job repeats"),
            ("no-such-preset", "1", "1", "unknown preset"),
            (" , ", "1", "1", "names no preset"),
            ("short-plan-real-node", "1", "1", "real node"),
            ("soak-weekly", "1", "1", "ceiling"),
            ("throughput-20k-window-1fe,throughput-400k-window-1fe-async,"
             "throughput-400k-window-2fe-async", "1", "2", "ceiling"),
            ("throughput-20k-window-1fe", "11", "1", "over 64"),
        ):
            with self.subTest(presets=presets, repeats=repeats, in_job=in_job):
                with self.assertRaises(probe.ProbeError) as raised:
                    probe.matrices("8,16,32", "both", "0", presets, repeats, in_job)
                self.assertIn(needle, str(raised.exception))

    def test_the_cli_prints_every_output_the_workflow_reads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / "summary.md"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                status = probe.main(["matrix", "--classes", "8", "--target-cache", "sticky-disk",
                                     "--fsync-vms", "0", "--repeats", "3",
                                     "--summary", str(summary)])
            self.assertEqual(status, 0)
            outputs = dict(line.split("=", 1) for line in out.getvalue().splitlines())
            keys = set(outputs)
            # The workflow splits the preset list on commas, so it is bare text.
            self.assertEqual(outputs["presets"], ",".join(probe.PRESETS))
            self.assertEqual(json.loads(outputs["job_minutes"]), 200)
            self.assertEqual(json.loads(outputs["probe"])["include"][2]["repeat"], 3)
            self.assertEqual(keys, {"probe", "fsync", "fsync_enabled", "presets",
                                    "in_job_repeats", "runs_minutes", "job_minutes", "ceiling"})
            self.assertIn("3 probe job(s)", summary.read_text())
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(probe.main(["matrix", "--classes", "8", "--target-cache",
                                             "both", "--fsync-vms", "0", "--repeats", "x"]), 2)


class MeasureTimeout(unittest.TestCase):
    def test_a_timeout_ends_the_whole_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pid_file = Path(tmp) / "grandchild"
            script = f"sleep 120 & echo $! > {pid_file}; wait"
            result = probe.measure(["bash", "-c", script], interval=0.05, timeout_seconds=1)
            self.assertTrue(result["timed_out"])
            self.assertNotEqual(result["exit_code"], 0)
            grandchild = int(pid_file.read_text())
            # Gone, or a zombie a slow PID 1 has not reaped yet: not running.
            state = probe.process_state(grandchild)
            self.assertTrue(state is None or state[0] in ("Z", "X"), state)
            self.assertFalse(probe.group_alive(grandchild))

    def test_the_cli_exits_124_on_a_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "m.json"
            status = probe.main(["measure", "--out", str(out), "--timeout-seconds", "0.5",
                                 "--", "sleep", "30"])
            self.assertEqual(status, 124)
            self.assertTrue(json.loads(out.read_text())["timed_out"])
            status = probe.main(["measure", "--out", str(out), "--", "true"])
            self.assertEqual(status, 0)
            self.assertFalse(json.loads(out.read_text())["timed_out"])


def write_run(directory: Path, harness: int = 0, **report_overrides) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "load-harness-report.json").write_text(json.dumps(report(**report_overrides)))
    (directory / "harness-exit-code").write_text(f"{harness}\n")
    (directory / "probe-measure.json").write_text(json.dumps(
        {"exit_code": 0, "timed_out": False, "wall_seconds": 400.0, "peak_used_mib": 9000.0,
         "baseline_used_mib": 1000.0, "mem_total_mib": 32000.0}))
    (directory / "pg_test_fsync.txt").write_text(PG_TEST_FSYNC)
    return directory


class RowsV2(unittest.TestCase):
    def test_a_preset_given_again_is_its_next_in_job_repeat(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = write_run(root / "a" / "1")
            second = write_run(root / "a" / "2", harness=6)
            other = write_run(root / "b" / "1")
            for path in (root / "build.json", root / "host.json"):
                path.write_text("{}")
            out = root / "row.json"
            self.assertEqual(probe.main([
                "row", "--out", str(out), "--class", "32", "--runner", "r",
                "--target-cache", "sticky-disk", "--target-restored", "true",
                "--repeat", "4", "--commit", "abc",
                "--build", str(root / "build.json"), "--host", str(root / "host.json"),
                "--run", f"throughput-20k-window-1fe={first}",
                "--run", f"throughput-20k-window-1fe={second}",
                "--run", f"throughput-400k-window-2fe-async={other}",
            ]), 0)
            row = json.loads(out.read_text())
        self.assertEqual(row["schema"], probe.ROW_SCHEMA)
        self.assertEqual((row["repeat"], row["commit"]), (4, "abc"))
        runs = row["runs"]["throughput-20k-window-1fe"]
        self.assertEqual(len(runs), 2)
        self.assertIsNone(runs[0]["unmeasured_reason"])
        self.assertEqual(runs[1]["unmeasured_reason"], "harness exit 6")
        self.assertEqual(runs[0]["headroom_mib"], 23000.0)
        self.assertEqual(len(row["runs"]["throughput-400k-window-2fe-async"]), 1)

    def test_a_run_argument_must_name_a_checked_in_preset(self) -> None:
        known = {"throughput-400k-window-2fe-async"}
        self.assertEqual(probe.parse_run("throughput-400k-window-2fe-async=/x", known)[0],
                         "throughput-400k-window-2fe-async")
        for text in ("no-such=/x", "throughput-400k-window-2fe-async=", "x"):
            with self.subTest(text=text), self.assertRaises(probe.ProbeError):
                probe.parse_run(text, known)

    def test_phase_latency_and_rate_are_read_only_in_their_units(self) -> None:
        phases = [{"name": "steady_state", "shortfall": 0, "duration_seconds": 300.0,
                   "completed": True, "achieved_rate_shares_per_second": 499.5,
                   "client_ack_latency": {"unit": "milliseconds", "p50": 3.0, "p99": 12.5}},
                  {"name": "burst", "shortfall": 10, "achieved_rate_shares_per_second": None,
                   "client_ack_latency": {"unit": "seconds", "p50": 0.1, "p99": 0.5}}]
        with tempfile.TemporaryDirectory() as tmp:
            result = probe.run_result(write_run(Path(tmp), phases=phases))
        steady, burst = result["phases"]["steady_state"], result["phases"]["burst"]
        self.assertEqual((steady["ack_p50_ms"], steady["ack_p99_ms"]), (3.0, 12.5))
        self.assertEqual(steady["achieved_rate_shares_per_second"], 499.5)
        self.assertIsNone(burst["ack_p99_ms"])
        self.assertIsNone(burst["achieved_rate_shares_per_second"])

    def test_what_makes_a_run_a_measurement(self) -> None:
        base = {"harness_exit_code": 0, "provenance": "pass", "timed_out": False,
                "gate_exit_code": 0}
        self.assertIsNone(probe.unmeasured_reason(base))
        # A failed gate still measured the run.
        self.assertIsNone(probe.unmeasured_reason(dict(base, gate_exit_code=1)))
        self.assertEqual(probe.unmeasured_reason(dict(base, timed_out=True)), "timed out")
        self.assertEqual(probe.unmeasured_reason(dict(base, harness_exit_code=None)),
                         "no harness exit code")
        self.assertEqual(probe.unmeasured_reason(dict(base, harness_exit_code=3)),
                         "harness exit 3")
        self.assertTrue(probe.unmeasured_reason(dict(base, provenance="fail: dirty tree"))
                        .startswith("provenance"))
        # Only a gate verdict (0 or 1) makes the run a sample.
        for gate in (2, 101, None, -15):
            self.assertEqual(probe.unmeasured_reason(dict(base, gate_exit_code=gate)),
                             f"gate exit {gate}")

    def test_v1_rows_are_read_and_unknown_schemas_refused(self) -> None:
        v1 = {"schema": probe.ROW_SCHEMA_V1, "kind": "probe", "class": 8, "runner": "r",
              "target_cache": "sticky-disk", "runs": {"throughput-20k-window-1fe": {"x": 1}}}
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "v1.json").write_text(json.dumps(v1))
            (row,) = probe.load_rows(Path(tmp))
            self.assertEqual(row["schema"], probe.ROW_SCHEMA)
            self.assertEqual(row["repeat"], 1)
            self.assertIsNone(row["commit"])
            # A v1 run with no harness exit code is unknown, not a sample.
            self.assertEqual(row["runs"]["throughput-20k-window-1fe"],
                             [{"x": 1, "unmeasured_reason": "no harness exit code"}])
            Path(tmp, "v9.json").write_text(json.dumps(
                dict(v1, schema="qbit.prism.runner-probe-row.v9")))
            with self.assertRaises(probe.ProbeError):
                probe.load_rows(Path(tmp))


PRESET = "throughput-20k-window-1fe"


def run(rate: float | None = 500.0, ack99: float = 10.0, peak: float = 9000.0,
        usecs: float | None = 492.0, harness: int | None = 0, steady_shortfall: int = 0,
        burst_shortfall: int = 50, total: float = 32000.0) -> dict:
    """A run as run_result writes it."""
    result = {
        "harness_exit_code": harness, "gate_exit_code": 0, "timed_out": False,
        "wall_seconds": 600.0, "peak_used_mib": peak, "baseline_used_mib": 1000.0,
        "mem_total_mib": total, "headroom_mib": total - peak,
        "fsync": {"method": "fdatasync", "ops_per_second": None, "usecs_per_op": usecs},
        "provenance": "pass", "seed_seconds": 10.0, "seed_rows": 20000,
        "phases": {
            "steady_state": {"shortfall": steady_shortfall, "ack_p50_ms": 2.0,
                             "ack_p99_ms": ack99, "achieved_rate_shares_per_second": rate},
            # D1 presets gate steady_state only; the burst's shortfall is
            # reported, not gated (#473).
            "burst": {"shortfall": burst_shortfall, "ack_p50_ms": 5.0, "ack_p99_ms": 900.0,
                      "achieved_rate_shares_per_second": 1500.0},
        },
        "tip_last_notify_p99_ms": 800, "tips_missing_a_session": 0,
    }
    result["unmeasured_reason"] = probe.unmeasured_reason(result)
    return result


def probe_row(repeat: int, runs: list[dict], size: int = 8, commit: str | None = "c0ffee",
              write_cache: str = "write back") -> dict:
    return {"schema": probe.ROW_SCHEMA, "kind": "probe", "class": size,
            "runner": probe.runner_label(size), "target_cache": "sticky-disk",
            "repeat": repeat, "commit": commit, "target": {}, "build": {},
            "host": {"block_device": {"write_cache": write_cache, "fua": "0", "model": "m"}},
            "runs": {PRESET: runs}}


def group(document: dict, size: int, band: str, preset: str = PRESET) -> dict:
    (found,) = [g for g in document["groups"]
                if (g["class"], g["preset"], g["fsync_band"]) == (size, preset, band)]
    return found


class Variance(unittest.TestCase):
    def setUp(self) -> None:
        self.presets, _ = probe.load_presets()

    def expected(self, repeats: int, in_job: int = 2, classes: str = "8") -> dict:
        return probe.matrices(classes, "sticky-disk", "0", PRESET, str(repeats), str(in_job))

    def test_bands(self) -> None:
        self.assertEqual(probe.fsync_band(None), "unknown")
        self.assertEqual(probe.fsync_band(100), "0-125us")
        self.assertEqual(probe.fsync_band(125), "125-250us")
        self.assertEqual(probe.fsync_band(492), "250-500us")
        self.assertEqual(probe.fsync_band(4000), "ge4000us")

    def test_statistics(self) -> None:
        result = probe.stats([10.0, 12.0, 14.0, 100.0], planned=6)
        self.assertEqual(result["n"], 4)
        self.assertEqual(result["unknown"], 2)
        self.assertEqual(result["median"], 13.0)
        # |x - 13| = 3, 1, 1, 87: median 2.
        self.assertEqual(result["mad"], 2.0)
        # Sample standard deviation 44.03 over a mean of 34.
        self.assertAlmostEqual(result["cv"], 1.295, places=3)
        self.assertEqual((result["min"], result["max"]), (10.0, 100.0))
        self.assertIsNone(probe.stats([5.0])["cv"])
        self.assertIsNone(probe.stats([0.0, 0.0])["cv"])
        self.assertEqual(probe.stats([0.0, 0.0])["mad"], 0.0)
        empty = probe.stats([], planned=3)
        self.assertEqual((empty["n"], empty["unknown"], empty["median"]), (0, 3, None))

    def test_missing_and_failed_runs_are_unknown_never_zero(self) -> None:
        rows = [
            probe_row(1, [run(rate=500.0), run(rate=490.0)]),
            probe_row(2, [run(rate=480.0), run(rate=None, harness=6)]),
            probe_row(3, [run(rate=510.0, usecs=1500.0), run(rate=505.0, usecs=1500.0)]),
        ]
        # Repeat 4 was planned and wrote no row.
        document = probe.variance(rows, self.expected(4), self.presets)
        self.assertEqual(document["schema"], probe.VARIANCE_SCHEMA)
        self.assertEqual(document["commit"], "c0ffee")
        self.assertEqual(document["jobs"]["planned"], 4)
        self.assertEqual(document["jobs"]["missing"],
                         [{"class": 8, "target_cache": "sticky-disk", "repeat": 4}])
        everything = group(document, 8, "all")
        self.assertEqual(everything["runs"], {
            "planned": 8, "measured": 5, "unknown": 3,
            "unknown_reasons": {"job wrote no row": 2, "harness exit 6": 1}})
        rate = everything["metrics"]["steady_state.achieved_rate_shares_per_second"]
        self.assertEqual(rate["unit"], probe.METRIC_UNITS["achieved_rate_shares_per_second"])
        self.assertEqual(rate["all"]["n"], 5)
        self.assertEqual(rate["all"]["unknown"], 3)
        self.assertEqual(rate["all"]["median"], 500.0)
        self.assertEqual(rate["all"]["min"], 480.0)
        # VM-to-VM: one median per VM that measured (495, 480, 507.5), the
        # missing VM unknown.
        self.assertEqual(rate["vm_to_vm"]["n"], 3)
        self.assertEqual(rate["vm_to_vm"]["unknown"], 1)
        self.assertEqual(rate["vm_to_vm"]["median"], 495.0)
        # Run-to-run: only VMs with two measured runs.
        self.assertEqual(rate["run_to_run"]["jobs"], 2)
        self.assertEqual(rate["run_to_run"]["median_mad"], 3.75)
        # Bands: two VMs at ~0.5 ms, one at 1.5 ms, the missing one unknown.
        self.assertEqual(group(document, 8, "250-500us")["runs"]["planned"], 4)
        self.assertEqual(group(document, 8, "1000-2000us")["runs"]["measured"], 2)
        self.assertEqual(group(document, 8, "unknown")["runs"]["unknown_reasons"],
                         {"job wrote no row": 2})
        self.assertIsNone(group(document, 8, "unknown")["size_evidence"])
        evidence = everything["size_evidence"]
        self.assertIsNone(evidence["fits"])
        self.assertIn("3 of 8 run(s) unknown", evidence["why"])
        self.assertEqual(everything["disks"], [{"write_cache": "write back", "fua": "0",
                                                "model": "m", "vms": 3}])
        self.assertEqual({vm["fsync_band"] for vm in document["jobs"]["by_vm"]},
                         {"250-500us", "1000-2000us", "unknown"})

    def test_a_class_fits_only_on_zero_gated_shortfall_and_headroom(self) -> None:
        rows = [probe_row(r, [run(), run()]) for r in (1, 2)]
        evidence = group(probe.variance(rows, self.expected(2), self.presets), 8, "all")[
            "size_evidence"]
        # The burst's shortfall is not gated on the D1 preset.
        self.assertEqual(evidence["gated_phases"], ["steady_state"])
        self.assertEqual(evidence["gated_shortfall_max"], 0)
        self.assertEqual(evidence["headroom_mib_required"], 8000.0)
        self.assertTrue(evidence["fits"], evidence["why"])

        rows[1]["runs"][PRESET][0] = run(steady_shortfall=3)
        evidence = group(probe.variance(rows, self.expected(2), self.presets), 8, "all")[
            "size_evidence"]
        self.assertFalse(evidence["fits"])
        self.assertEqual(evidence["runs_with_gated_shortfall"], 1)

        rows[1]["runs"][PRESET][0] = run(peak=26000.0)
        evidence = group(probe.variance(rows, self.expected(2), self.presets), 8, "all")[
            "size_evidence"]
        self.assertFalse(evidence["fits"])
        self.assertIn("under the headroom", evidence["why"])

        # Each run against its own MemTotal: a 64 GiB VM with 10 GiB left
        # fails even when a 32 GiB VM of the class needs only 8 GiB.
        rows[1]["runs"][PRESET][0] = run(total=65536.0, peak=55536.0)
        evidence = group(probe.variance(rows, self.expected(2), self.presets), 8, "all")[
            "size_evidence"]
        self.assertFalse(evidence["fits"])
        self.assertEqual(evidence["headroom_mib_required"], 16384.0)

        # The preset's own memory floor (6 GiB) wins over a small fraction.
        evidence = group(probe.variance([probe_row(1, [run(peak=27000.0), run()])],
                                        self.expected(1), self.presets, headroom_fraction=0.1),
                         8, "all")["size_evidence"]
        self.assertEqual(evidence["headroom_mib_required"], 6144.0)
        self.assertFalse(evidence["fits"])

    def test_a_group_with_every_run_unknown_still_lists_its_metrics(self) -> None:
        rows = [probe_row(1, [run(harness=6), run(harness=6)])]
        everything = group(probe.variance(rows, self.expected(2), self.presets), 8, "all")
        rate = everything["metrics"]["steady_state.achieved_rate_shares_per_second"]
        self.assertEqual((rate["all"]["n"], rate["all"]["unknown"]), (0, 4))
        self.assertEqual((rate["vm_to_vm"]["n"], rate["vm_to_vm"]["unknown"]), (0, 2))
        self.assertIsNone(rate["run_to_run"])
        self.assertEqual(everything["metrics"]["peak_used_mib"]["all"]["unknown"], 4)

    def test_an_all_phase_preset_lists_the_phases_its_unknown_runs_reported(self) -> None:
        # pr-smoke's gates name no phases, so every driven phase is gated.
        self.assertIsNone(probe.gated_phases(self.presets["pr-smoke"]))
        row = probe_row(1, [run(harness=6)])
        row["runs"] = {"pr-smoke": row["runs"][PRESET]}
        planned = probe.matrices("8", "sticky-disk", "0", "pr-smoke", "1", "1")
        everything = group(probe.variance([row], planned, self.presets), 8, "all", "pr-smoke")
        for phase in ("steady_state", "burst"):
            metric = everything["metrics"][f"{phase}.shortfall"]
            self.assertEqual((metric["all"]["n"], metric["all"]["unknown"]), (0, 1))

    def test_one_commit_one_row_per_job(self) -> None:
        with self.assertRaises(probe.ProbeError):
            probe.variance([probe_row(1, [run()]), probe_row(2, [run()], commit="other")])
        with self.assertRaises(probe.ProbeError):
            probe.variance([probe_row(1, [run()]), probe_row(1, [run()])])
        with self.assertRaises(probe.ProbeError):
            probe.variance([], headroom_fraction=float("nan"))

    def test_without_the_plan_missing_jobs_cannot_be_counted_and_say_so(self) -> None:
        document = probe.variance([probe_row(1, [run()])], None, self.presets)
        self.assertIsNone(document["jobs"]["planned"])
        self.assertEqual(group(document, 8, "all")["runs"]["planned"], 1)

    def test_the_cli_writes_the_document_and_a_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            rows = Path(tmp) / "rows"
            rows.mkdir()
            for r in (1, 2):
                (rows / f"8-sticky-disk-{r}.json").write_text(
                    json.dumps(probe_row(r, [run(), run(rate=495.0)])))
            (rows / "fsync-8-1.json").write_text(json.dumps(
                {"schema": probe.ROW_SCHEMA, "kind": "fsync", "class": 8, "vm": 1,
                 "runner": "r", "host": {}, "fsync": {"ops_per_second": 1.0}}))
            expected = Path(tmp) / "expected.json"
            expected.write_text(json.dumps(self.expected(3)))
            out = Path(tmp) / "variance.json"
            text = io.StringIO()
            with contextlib.redirect_stdout(text):
                self.assertEqual(probe.main(["variance", str(rows), "--expected", str(expected),
                                             "--out", str(out), "--markdown"]), 0)
            document = json.loads(out.read_text())
            self.assertEqual(document["schema"], probe.VARIANCE_SCHEMA)
            self.assertEqual(document["jobs"]["missing"][0]["repeat"], 3)
            self.assertIn("| 8 vCPU | throughput-20k-window-1fe | 4 of 6 |", text.getvalue())
            self.assertIn("unknown: 2 of 6 run(s) unknown", text.getvalue())
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(probe.main(["variance", str(rows), "--markdown"]), 2)
            with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                probe.main(["variance", str(rows), "--headroom-fraction", "1.5"])


class Verdict(unittest.TestCase):
    def test_every_planned_run_must_exit_0(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "build.json").write_text(json.dumps({"exit_code": 0}))
            for i in (1, 2):
                path = root / PRESET / str(i) / "probe-measure.json"
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps({"exit_code": 0, "timed_out": False}))
            self.assertEqual(probe.verdict(root, [PRESET], 2), [])
            # A run that never wrote its measurement is a failure.
            self.assertEqual(probe.verdict(root, [PRESET], 3),
                             [f"{PRESET}/3/probe-measure: exit None"])
            (root / PRESET / "2" / "probe-measure.json").write_text(
                json.dumps({"exit_code": -15, "timed_out": True}))
            self.assertEqual(probe.verdict(root, [PRESET], 2),
                             [f"{PRESET}/2/probe-measure: exit -15 (timed out)"])
            self.assertEqual(probe.main(["verdict", "--probe", str(root), "--presets", PRESET,
                                         "--in-job-repeats", "1"]), 0)


if __name__ == "__main__":
    unittest.main()
