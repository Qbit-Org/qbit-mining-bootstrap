"""Tests for scripts/prism_load_ab.py, the A/B release benchmark driver (#511)."""

from __future__ import annotations

import contextlib
import io
import json
import os
import signal
import time
import subprocess
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prism_load_ab as ab  # noqa: E402

# The real preset check, before any test stubs it out.
VALIDATE_PRESET = ab.validate_preset

# `qbit-prism-load --help`'s long flags at 5d0042f6, #271's base and the
# oldest build #479 STEP 1 compared.
FLAGS_5D0042F6 = frozenset(
    """--ack-p99-limit-ms --allow-debug-server --allow-dirty-tree
    --allow-unverified-server-revision --blockpoll-seconds --burst-rate --burst-seconds
    --cadence --cadence-gaps --cadence-rate --cadence-seconds --database-url
    --db-max-connections --example-artifact --external-tips
    --forecast-peak-shares-per-second --frontends --keep-artifacts
    --lock-sample-interval-ms --max-outstanding-per-session --mid-flight-kill
    --min-mem-available-mib --out --pg-bin-dir --plan --process-sample-interval-ms --rate
    --reconnect-seconds --reconnect-target --replication --runtime-workers
    --scheduled-blocks --seed-share-bytes --server-bin --sessions
    --share-commit-timeout-seconds --slow-database-seconds --slow-db-delay-ms
    --steady-state-rate --steady-state-seconds --warmup-seconds --window-shares
    --work-timeout --help --version""".split()
)

# #479 STEP 1's argument list: #271's 20k list, unchanged.
ARGS_479 = (
    "--frontends 1 --sessions 2000 --window-shares 20000 --replication async --plan d1 "
    "--forecast-peak-shares-per-second 2000 --ack-p99-limit-ms 1000 --min-mem-available-mib 6144"
).split()


def running(pid: int) -> bool:
    """Whether `pid` is a live process; a zombie awaiting its reaper is not."""
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except (FileNotFoundError, ProcessLookupError):
        return False
    return state != "Z"


def number_or_text(word: str | bool) -> float | str | bool:
    try:
        return float(word)
    except (TypeError, ValueError):
        return word


def pairs(words: list[str]) -> dict[str, str | bool]:
    out: dict[str, str | bool] = {}
    index = 0
    while index < len(words):
        flag = words[index]
        if index + 1 < len(words) and not words[index + 1].startswith("--"):
            out[flag] = words[index + 1]
            index += 2
        else:
            out[flag] = True
            index += 1
    return out


class Resolution(unittest.TestCase):
    def setUp(self) -> None:
        self.legacy = ab.load_legacy()
        self.operational = ab.operational_flags()

    def preset(self, name: str) -> dict:
        return ab.load_preset(ab.PRESETS / f"{name}.json")

    def current_flags(self, preset: dict) -> frozenset[str]:
        return frozenset(preset["args"]) | self.operational | {"--help", "--version"}

    def test_the_operational_flags_are_read_from_preset_rs(self) -> None:
        self.assertIn("--server-bin", self.operational)
        self.assertIn("--preset", self.operational)
        self.assertNotIn("--sessions", self.operational)

    def test_d1_20k_runs_on_5d0042f6_with_479s_argument_list(self) -> None:
        preset = self.preset("throughput-20k-window-1fe")
        words, dropped = ab.resolve_argv(preset["args"], FLAGS_5D0042F6, self.legacy, self.operational, "base")
        resolved = pairs(words)
        expected = pairs(ARGS_479)
        for flag, value in expected.items():
            self.assertTrue(ab.same(number_or_text(resolved[flag]), number_or_text(value)), flag)
        self.assertTrue(set(resolved) <= FLAGS_5D0042F6)
        self.assertIn("--stratum-max-pending-initial-jobs", dropped)
        self.assertIn("--background-shares-per-second", dropped)
        self.assertEqual(set(dropped), set(preset["args"]) - FLAGS_5D0042F6)

    def test_a_current_build_runs_every_pinned_flag_and_leaves_nothing_off(self) -> None:
        preset = self.preset("throughput-20k-window-1fe")
        words, dropped = ab.resolve_argv(preset["args"], self.current_flags(preset), self.legacy, self.operational, "candidate")
        self.assertEqual(dropped, [])
        self.assertIn("--stratum-max-pending-initial-jobs", words)
        self.assertEqual(words[words.index("--stratum-max-pending-initial-jobs") + 1], "2016")
        self.assertNotIn("--database-url", words)
        self.assertNotIn("--mid-flight-kill", words)

    def test_a_workload_an_old_build_cannot_run_is_refused_with_every_reason(self) -> None:
        preset = self.preset("tip-delivery-2000-miners-400k-2fe-retarget")
        with self.assertRaises(ab.DriverError) as caught:
            ab.resolve_argv(preset["args"], FLAGS_5D0042F6, self.legacy, self.operational, "base")
        message = str(caught.exception)
        self.assertIn("--retarget-bits", message)
        self.assertIn("--background-shares-per-second", message)
        self.assertIn("--stratum-max-pending-initial-jobs", message)
        self.assertIn("needs 1016", message)

    def test_a_missing_flag_with_no_legacy_rule_is_refused(self) -> None:
        preset = self.preset("throughput-20k-window-1fe")
        flags = self.current_flags(preset) - {"--plan"}
        with self.assertRaisesRegex(ab.DriverError, "no --plan and legacy-flags.json has no rule"):
            ab.resolve_argv(preset["args"], flags, self.legacy, self.operational, "base")

    def test_a_newer_harness_flag_the_preset_does_not_pin_is_refused(self) -> None:
        preset = self.preset("throughput-20k-window-1fe")
        flags = self.current_flags(preset) | {"--some-new-knob"}
        with self.assertRaisesRegex(ab.DriverError, "result flag --some-new-knob"):
            ab.resolve_argv(preset["args"], flags, self.legacy, self.operational, "candidate")

    def test_inert_flags_are_dropped_only_while_their_phase_is_off(self) -> None:
        rule = self.legacy["--churn-rate"]
        self.assertTrue(ab.legacy_holds("--churn-rate", rule, {"--churn-rate": 50, "--churn-seconds": 0})[0])
        self.assertFalse(ab.legacy_holds("--churn-rate", rule, {"--churn-rate": 50, "--churn-seconds": 300})[0])
        self.assertFalse(ab.legacy_holds("--churn-rate", rule, {"--churn-rate": 50})[0])

    def test_numbers_compare_as_numbers_and_never_as_booleans(self) -> None:
        self.assertTrue(ab.same(0, 0.0))
        self.assertFalse(ab.same(False, 0))
        self.assertFalse(ab.same(None, 0))
        self.assertTrue(ab.same(None, None))

    def test_the_admission_formula_matches_the_old_harness(self) -> None:
        rule = self.legacy["--stratum-max-pending-initial-jobs"]
        flag = "--stratum-max-pending-initial-jobs"
        cases = [(2000, 1, 2016), (2000, 2, 1016), (2000, 4, 516), (200, 2, 128), (2001, 2, 1017)]
        for sessions, frontends, implied in cases:
            args = {"--sessions": sessions, "--frontends": frontends, flag: implied}
            self.assertTrue(ab.legacy_holds(flag, rule, args)[0], (sessions, frontends))
            args[flag] = implied + 1
            self.assertFalse(ab.legacy_holds(flag, rule, args)[0])

    def test_every_checked_in_d1_preset_runs_on_5d0042f6(self) -> None:
        for path in sorted(ab.PRESETS.glob("throughput-*.json")):
            if path.stem.endswith("-addresses"):
                continue
            with self.subTest(preset=path.stem):
                preset = ab.load_preset(path)
                ab.resolve_argv(preset["args"], FLAGS_5D0042F6, self.legacy, self.operational, "base")

    def test_help_flags_are_the_option_lines_never_a_flag_a_description_mentions(self) -> None:
        text = """Options:
      --pg-bin-dir <PG_BIN_DIR>
          Directory holding `initdb`. Defaults to `pg_config --bindir`
      --mid-flight-kill
  -h, --help
          Print help (see a summary with '-h')
  -V, --version
"""
        self.assertEqual(ab.parse_help_flags(text), {"--pg-bin-dir", "--mid-flight-kill", "--help", "--version"})

    def test_the_legacy_table_is_well_formed(self) -> None:
        for flag, rule in self.legacy.items():
            self.assertTrue(flag.startswith("--"))
            self.assertTrue(rule.get("why"), flag)
        with tempfile.TemporaryDirectory() as directory:
            broken = Path(directory) / "legacy.json"
            broken.write_text(json.dumps({"schema": ab.LEGACY_SCHEMA, "flags": {"--x": {"equals": 1, "formula": "y"}}}))
            with self.assertRaises(ab.DriverError):
                ab.load_legacy(broken)


class Series(unittest.TestCase):
    def setUp(self) -> None:
        # The comparator's preset check needs a Rust build; these tests
        # exercise the driver around it.
        patcher = mock.patch.object(ab, "validate_preset", lambda comparator, preset: None)
        patcher.start()
        self.addCleanup(patcher.stop)
        compare = mock.patch.object(ab, "compare_bin", lambda explicit: Path("/nonexistent/compare"))
        compare.start()
        self.addCleanup(compare.stop)

    def test_a_preset_the_comparator_rejects_stops_the_series_before_any_build(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fake = Path(directory) / "compare"
            fake.write_text("#!/bin/sh\necho 'preset x: max_shortfall is -1' >&2\nexit 2\n")
            fake.chmod(0o755)
            with self.assertRaisesRegex(ab.DriverError, "is not valid: preset x: max_shortfall is -1"):
                VALIDATE_PRESET(fake, "x.json")

    def test_the_preset_is_validated_before_anything_is_built(self) -> None:
        events: list[str] = []

        def build(label, ref, out, skip_build):
            events.append(f"build {label}")
            raise ab.DriverError("stop")

        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(ab, "validate_preset", lambda comparator, preset: events.append("validate")), \
                mock.patch.object(ab, "prepare_build", build), self.assertRaises(ab.DriverError):
            ab.main(["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe",
                     "--out", directory, "--dry-run"])
        self.assertEqual(events, ["validate", "build base"])

    def test_repeats_alternate_which_build_goes_first(self) -> None:
        self.assertEqual(
            ab.interleaved(3),
            [(1, "base"), (1, "candidate"), (2, "candidate"), (2, "base"), (3, "base"), (3, "candidate")],
        )

    def test_the_load_gate_waits_for_a_quiet_host_and_gives_up_at_its_deadline(self) -> None:
        clock = [0.0]
        loads = iter([5.0, 4.0, 2.5])
        seen = ab.wait_for_quiet(3.0, 100, 10, lambda: next(loads), lambda s: clock.__setitem__(0, clock[0] + s), lambda: clock[0])
        self.assertEqual(seen, 2.5)
        clock[0] = 0.0
        with self.assertRaises(ab.IncompleteSeries):
            ab.wait_for_quiet(3.0, 25, 10, lambda: 9.0, lambda s: clock.__setitem__(0, clock[0] + s), lambda: clock[0])

    def test_pg_test_fsync_is_read_from_the_one_write_section(self) -> None:
        text = """5 seconds per test
O_DIRECT supported on this platform for open_datasync and open_sync.

Compare file sync methods using one 8kB write:
(in "wal_sync_method" preference order, except fdatasync is Linux's default)
        open_datasync                      3937.123 ops/sec     254 usecs/op
        fdatasync                          3467.456 ops/sec     288 usecs/op
        fsync                              2123.000 ops/sec     471 usecs/op

Compare file sync methods using two 8kB writes:
(in "wal_sync_method" preference order, except fdatasync is Linux's default)
        open_datasync                      1900.000 ops/sec     526 usecs/op
        fdatasync                          1700.000 ops/sec     588 usecs/op
"""
        self.assertEqual(
            ab.parse_pg_test_fsync(text),
            {"fdatasync_ops_per_second": 3467.456, "fdatasync_usecs_per_op": 288.0},
        )
        self.assertIsNone(ab.parse_pg_test_fsync("could not open output file"))

    def test_nonfinite_and_non_positive_settings_are_refused(self) -> None:
        base = ["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe", "--out", "x"]
        for bad in (["--max-load", "nan"], ["--max-load", "0"], ["--repeats", "0"], ["--cooldown-seconds", "inf"],
                    ["--run-ceiling-seconds", "-1"]):
            with self.subTest(bad=bad), self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                ab.parse_args(base + bad)
        self.assertEqual(ab.parse_args(base + ["--cooldown-seconds", "0"]).cooldown_seconds, 0.0)

    def test_a_run_records_its_exit_code_and_host_samples(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runs = Path(directory)
            record = ab.execute_run("r1", [sys.executable, "-c", "print('hi'); raise SystemExit(3)"],
                                    runs, dict(os.environ), runs, 60, 0.05)
            self.assertEqual(record["exit_code"], 3)
            self.assertFalse(record["ceiling_hit"])
            self.assertIsNotNone(record["load_max"])
            self.assertIn("hi", (runs / "r1.log").read_text())
            self.assertTrue((runs / "r1.host.jsonl").read_text().strip())
            self.assertTrue((runs / "r1.ps-before.txt").exists() and (runs / "r1.ps-after.txt").exists())

    def test_a_run_past_its_ceiling_is_killed_with_its_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runs = Path(directory)
            record = ab.execute_run("r2", [sys.executable, "-c", "import time; time.sleep(60)"],
                                    runs, dict(os.environ), runs, 0.5, 0.1)
            self.assertIsNone(record["exit_code"])
            self.assertTrue(record["ceiling_hit"])

    def test_an_interrupted_run_takes_its_process_group_down_before_propagating(self) -> None:
        started: list[subprocess.Popen] = []

        class Interrupted(subprocess.Popen):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                started.append(self)

            def wait(self, timeout=None):
                if timeout == 30:
                    raise KeyboardInterrupt
                return super().wait(timeout)

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(ab.subprocess, "Popen", Interrupted):
            runs = Path(directory)
            with self.assertRaises(KeyboardInterrupt):
                ab.execute_run("r3", [sys.executable, "-c", "import time; time.sleep(60)"],
                               runs, dict(os.environ), runs, 30, 0.1)
        self.assertIsNotNone(started[-1].poll())

    def test_a_resume_that_moves_the_clusters_or_the_host_is_refused(self) -> None:
        base = ["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe", "--out", "x", "--resume"]
        options = ab.parse_args(base + ["--tmpdir", "/tmp/pload-ab"])
        previous = {"host": ab.host_facts(), "settings": ab.settings_of(options)}
        ab.check_resumable(previous, options)
        ab.check_resumable(previous, ab.parse_args(base + ["--tmpdir", "/tmp/pload-ab", "--skip-build"]))
        for moved in (["--max-load", "100"], ["--cooldown-seconds", "0"], ["--lock-file", "/tmp/other.lock"]):
            with self.subTest(moved=moved), self.assertRaisesRegex(ab.DriverError, "cannot resume"):
                ab.check_resumable(previous, ab.parse_args(base + ["--tmpdir", "/tmp/pload-ab"] + moved))
        with self.assertRaisesRegex(ab.DriverError, "--tmpdir was /tmp/pload-ab"):
            ab.check_resumable(previous, ab.parse_args(base + ["--tmpdir", "/mnt/other"]))
        with self.assertRaisesRegex(ab.DriverError, "--repeats was 5, not 3"):
            ab.check_resumable({**previous, "settings": {**previous["settings"], "repeats": 5}}, options)
        for key, value in (("hostname", "elsewhere"), ("nproc", 999), ("mem_total_mib", 1)):
            moved = {**previous, "host": {**previous["host"], key: value}}
            with self.subTest(key=key), self.assertRaisesRegex(ab.DriverError, f"the host's {key} was {value}"):
                ab.check_resumable(moved, options)

    def test_the_builds_run_under_the_benchmark_lock(self) -> None:
        events: list[str] = []

        @contextlib.contextmanager
        def lock(path):
            events.append("lock")
            try:
                yield
            finally:
                events.append("unlock")

        def build(label, ref, out, skip_build):
            events.append(f"build {label}")
            raise ab.DriverError("stop")

        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(ab, "benchmark_lock", lock), mock.patch.object(ab, "prepare_build", build):
            with self.assertRaises(ab.DriverError):
                ab.main(["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe",
                         "--out", directory, "--dry-run"])
        self.assertEqual(events, ["lock", "build base", "unlock"])

    def test_a_missing_pg_test_fsync_is_bad_input_not_a_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ab.DriverError, "pg_test_fsync, .*: not executable"):
                ab.main(["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe",
                         "--out", directory, "--pg-bin-dir", directory])
            with self.assertRaisesRegex(ab.DriverError, "running"):
                ab.record_fsync(Path(directory), Path(directory), Path(directory) / "f.txt", 1)

    def test_a_series_written_while_waiting_for_the_lock_needs_resume(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            @contextlib.contextmanager
            def lock(path):
                # The first invocation finishes while this one waits.
                (Path(directory) / "manifest.json").write_text("{}")
                yield

            with mock.patch.object(ab, "benchmark_lock", lock), \
                    self.assertRaisesRegex(ab.DriverError, "pass --resume"):
                ab.main(["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe",
                         "--out", directory, "--dry-run"])

    def test_relative_paths_are_made_absolute_before_anything_uses_them(self) -> None:
        seen = {}

        def series(options, *rest):
            seen["tmpdir"] = options.tmpdir
            return 0

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(ab, "run_series", series), \
                mock.patch.object(ab, "benchmark_lock", lambda path: contextlib.nullcontext()):
            ab.main(["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe",
                     "--out", directory, "--dry-run", "--tmpdir", "relative/tmp"])
        self.assertTrue(seen["tmpdir"].is_absolute())

    def test_an_unexpected_failure_exits_2_never_1(self) -> None:
        cases = [
            (RuntimeError("boom"), ab.EXIT_INPUT),
            (FileNotFoundError("no comparator"), ab.EXIT_INPUT),
            (ab.DriverError("bad"), ab.EXIT_INPUT),
            (ab.IncompleteSeries("loaded"), ab.EXIT_INCOMPLETE),
        ]
        for error, status in cases:
            def fail(argv, error=error):
                raise error

            with self.subTest(error=error), mock.patch.object(ab, "main", fail), \
                    contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(ab.run_cli([]), status)

    def test_a_relative_cargo_target_dir_is_anchored_where_cargo_ran(self) -> None:
        self.assertEqual(ab.comparator_path(None), ab.ROOT / "target" / "release" / "qbit-prism-load-compare")
        self.assertEqual(ab.comparator_path("out/t"), ab.ROOT / "out" / "t" / "release" / "qbit-prism-load-compare")
        self.assertEqual(ab.comparator_path("/abs/t"), Path("/abs/t/release/qbit-prism-load-compare"))

    def test_sigterm_to_the_driver_stops_the_detached_harness(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / "harness.pid"
            harness = f"import os, time; open({str(pid_file)!r}, 'w').write(str(os.getpid())); time.sleep(120)"
            driver = (
                f"import sys; sys.path.insert(0, {str(ab.ROOT / 'scripts')!r}); import prism_load_ab as ab\n"
                "from pathlib import Path\n"
                "import os\n"
                "ab.main = lambda argv: ab.execute_run('r', [sys.executable, '-c', "
                f"{harness!r}], Path({directory!r}), dict(os.environ), Path({directory!r}), 120, 0.1) and 0\n"
                "sys.exit(ab.run_cli([]))\n"
            )
            proc = subprocess.Popen([sys.executable, "-c", driver], stderr=subprocess.PIPE, text=True)
            deadline = time.monotonic() + 10
            while not pid_file.exists() or not pid_file.read_text():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.05)
            harness_pid = int(pid_file.read_text())
            proc.send_signal(signal.SIGTERM)
            _, stderr = proc.communicate(timeout=30)
            self.assertEqual(proc.returncode, 128 + signal.SIGTERM, stderr)
            with self.assertRaises(ProcessLookupError):
                os.kill(harness_pid, 0)

    def test_a_signal_during_the_spawn_is_raised_after_it_and_stops_the_child(self) -> None:
        started: list[subprocess.Popen] = []

        class SignalledDuringSpawn(subprocess.Popen):
            def __init__(self, args, *rest, **kwargs):
                super().__init__(args, *rest, **kwargs)
                if args[0] == sys.executable:  # the harness, not the ps snapshots
                    started.append(self)
                    # The signal lands before the constructor returns.
                    ab.on_termination(signal.SIGTERM, None)

        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(ab.subprocess, "Popen", SignalledDuringSpawn):
            runs = Path(directory)
            with self.assertRaises(ab.Terminated):
                ab.execute_run("r4", [sys.executable, "-c", "import time; time.sleep(60)"],
                               runs, dict(os.environ), runs, 60, 0.1)
        self.assertIsNotNone(started[-1].poll())
        with self.assertRaises(ab.Terminated):
            ab.on_termination(signal.SIGTERM, None)

    def test_a_descendant_that_outlives_the_harness_is_stopped_before_the_run_returns(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runs = Path(directory)
            pid_file = runs / "descendant.pid"
            # The harness starts a descendant that ignores SIGTERM, then exits.
            descendant = "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(120)"
            harness = (
                "import subprocess, sys; "
                f"p = subprocess.Popen([sys.executable, '-c', {descendant!r}]); "
                f"open({str(pid_file)!r}, 'w').write(str(p.pid))"
            )
            original = ab.stop_group
            with mock.patch.object(ab, "stop_group", lambda child: original(child, 1.0, 0.05)):
                record = ab.execute_run("r5", [sys.executable, "-c", harness], runs, dict(os.environ), runs, 60, 0.1)
            self.assertEqual(record["exit_code"], 0)
            self.assertFalse(running(int(pid_file.read_text())))

    def test_a_second_invocation_on_the_same_out_is_refused_whatever_its_lock_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with ab.series_lock(Path(directory) / ".series.lock"):
                with self.assertRaisesRegex(ab.DriverError, "another invocation is using"):
                    ab.main(["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe",
                             "--out", directory, "--dry-run", "--lock-file", str(Path(directory) / "other.lock")])

    def test_a_signal_just_after_the_deferred_spawn_still_stops_the_child(self) -> None:
        started: list[subprocess.Popen] = []
        real_deferred = ab.deferred_termination

        @contextlib.contextmanager
        def deferral_then_signal():
            with real_deferred() as pending:
                yield pending
            # The signal lands the instant the deferral ends.
            ab.on_termination(signal.SIGTERM, None)

        class Recording(subprocess.Popen):
            def __init__(self, args, *rest, **kwargs):
                super().__init__(args, *rest, **kwargs)
                if args[0] == sys.executable:
                    started.append(self)

        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(ab.subprocess, "Popen", Recording), \
                mock.patch.object(ab, "deferred_termination", deferral_then_signal):
            runs = Path(directory)
            with self.assertRaises(ab.Terminated):
                ab.execute_run("r6", [sys.executable, "-c", "import time; time.sleep(60)"],
                               runs, dict(os.environ), runs, 60, 0.1)
        self.assertIsNotNone(started[-1].poll())

    def test_a_signal_during_the_after_exit_sweep_still_stops_the_descendant(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runs = Path(directory)
            pid_file = runs / "descendant.pid"
            descendant = "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(120)"
            harness = (
                "import subprocess, sys; "
                f"p = subprocess.Popen([sys.executable, '-c', {descendant!r}]); "
                f"open({str(pid_file)!r}, 'w').write(str(p.pid))"
            )
            original = ab.stop_group

            def signalled_stop(child):
                # The signal lands as the sweep begins; it must be held.
                ab.on_termination(signal.SIGTERM, None)
                original(child, 1.0, 0.05)

            with mock.patch.object(ab, "stop_group", signalled_stop), self.assertRaises(ab.Terminated):
                ab.execute_run("r7", [sys.executable, "-c", harness], runs, dict(os.environ), runs, 60, 0.1)
            self.assertFalse(running(int(pid_file.read_text())))

    def test_a_setsid_server_left_by_a_killed_harness_is_found_and_stopped(self) -> None:
        # As qbit-prism-server and pg_ctl's postmaster do, the descendant
        # leaves the harness's process group with setsid() (and ignores
        # SIGTERM); the harness then dies without stopping it.
        self.assertTrue(ab.become_subreaper())
        with tempfile.TemporaryDirectory() as directory:
            runs = Path(directory)
            pid_file = runs / "server.pid"
            server = "import os, signal, time; os.setsid(); signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(120)"
            harness = (
                "import os, signal, subprocess, sys, time; "
                f"p = subprocess.Popen([sys.executable, '-c', {server!r}]); "
                f"open({str(pid_file)!r}, 'w').write(str(p.pid)); "
                "time.sleep(0.3); os.kill(os.getpid(), signal.SIGKILL)"
            )
            sweep = ab.sweep_descendants
            with mock.patch.object(ab, "sweep_descendants", lambda: sweep(1.0, 0.05)):
                record = ab.execute_run("r8", [sys.executable, "-c", harness], runs, dict(os.environ), runs, 60, 0.1)
            self.assertEqual(record["exit_code"], -signal.SIGKILL)
            server_pid = int(pid_file.read_text())
            self.assertFalse(running(server_pid))

    def test_a_series_is_refused_when_the_driver_cannot_become_a_subreaper(self) -> None:
        argv = ["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe"]
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(ab, "become_subreaper", lambda: False):
            for name in ("pg_test_fsync", *ab.required_pg_binaries()):
                binary = Path(directory) / name
                binary.write_text("#!/bin/sh\n")
                binary.chmod(0o755)
            with self.assertRaisesRegex(ab.DriverError, "child subreaper"):
                ab.main(argv + ["--out", directory, "--pg-bin-dir", directory])
            # A dry run still compiles both builds, so it needs one too.
            with self.assertRaisesRegex(ab.DriverError, "child subreaper"):
                ab.main(argv + ["--out", directory, "--dry-run"])
            # So does one over existing builds: it compiles the comparator.
            with self.assertRaisesRegex(ab.DriverError, "child subreaper"):
                ab.main(argv + ["--out", directory, "--dry-run", "--skip-build"])

    def test_a_build_interrupted_by_a_signal_leaves_nothing_running_under_the_lock(self) -> None:
        self.assertTrue(ab.become_subreaper())
        events: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / "rustc.pid"
            # "cargo" starts a "rustc" that ignores SIGTERM, then the driver
            # is signalled mid-build.
            rustc = "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(120)"
            cargo = (
                "import subprocess, sys, time; "
                f"p = subprocess.Popen([sys.executable, '-c', {rustc!r}]); "
                f"open({str(pid_file)!r}, 'w').write(str(p.pid)); time.sleep(120)"
            )

            def build(label, ref, out, skip_build):
                subprocess.Popen([sys.executable, "-c", cargo])
                while not pid_file.exists() or not pid_file.read_text():
                    time.sleep(0.05)
                raise ab.Terminated(signal.SIGTERM)

            @contextlib.contextmanager
            def lock(path):
                try:
                    yield
                finally:
                    events.append("unlock")
                    events.append("rustc alive" if running(int(pid_file.read_text())) else "rustc stopped")

            sweep = ab.sweep_descendants
            with mock.patch.object(ab, "prepare_build", build), mock.patch.object(ab, "benchmark_lock", lock), \
                    mock.patch.object(ab, "sweep_descendants", lambda: sweep(1.0, 0.05)), \
                    self.assertRaises(ab.Terminated):
                ab.main(["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe",
                         "--out", directory, "--dry-run"])
        self.assertEqual(events, ["unlock", "rustc stopped"])

    def test_every_server_binary_the_harness_needs_is_checked_before_the_series(self) -> None:
        self.assertEqual(ab.required_pg_binaries(), ("initdb", "pg_ctl", "pg_basebackup"))
        with tempfile.TemporaryDirectory() as directory:
            fsync = Path(directory) / "pg_test_fsync"
            fsync.write_text("#!/bin/sh\n")
            fsync.chmod(0o755)
            with self.assertRaisesRegex(ab.DriverError, "initdb, .*pg_ctl, .*pg_basebackup: not executable"):
                ab.main(["--base", "a", "--candidate", "b", "--preset", "throughput-20k-window-1fe",
                         "--out", directory, "--pg-bin-dir", directory])

    def test_a_skipped_build_reuses_only_binaries_the_driver_built_for_that_commit(self) -> None:
        commits = {"now": "a" * 40}

        def git(*args: str, cwd: Path = ab.ROOT) -> str:
            return commits["now"] if args[0] == "rev-parse" else ""

        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            release = out / "builds" / "base" / "target" / "release"
            release.mkdir(parents=True)
            for binary in ab.BINARIES:
                (release / binary).write_bytes(b"left here by " + binary.encode())
            built = subprocess.CompletedProcess([], 0)
            with mock.patch.object(ab, "git", git), \
                    mock.patch.object(ab.subprocess, "run", return_value=built) as cargo, \
                    contextlib.redirect_stderr(io.StringIO()):
                # Binaries the driver did not build are not reused.
                with self.assertRaisesRegex(ab.DriverError, "holds no build this driver made"):
                    ab.prepare_build("base", "v1", out, skip_build=True)
                cargo.assert_not_called()
                # The driver's own build records them, and then they are.
                ab.prepare_build("base", "v1", out, skip_build=False)
                self.assertEqual(cargo.call_count, 1)
                ab.prepare_build("base", "v1", out, skip_build=True)
                self.assertEqual(cargo.call_count, 1)
                # A binary replaced since, or a worktree moved to another
                # commit, is not.
                (release / "qbit-prism-load").write_bytes(b"copied in")
                with self.assertRaisesRegex(ab.DriverError, "holds no build this driver made"):
                    ab.prepare_build("base", "v1", out, skip_build=True)
                ab.prepare_build("base", "v1", out, skip_build=False)
                commits["now"] = "b" * 40
                with self.assertRaisesRegex(ab.DriverError, "holds no build this driver made"):
                    ab.prepare_build("base", "v2", out, skip_build=True)
                # A failed build leaves no record to reuse.
                cargo.return_value = subprocess.CompletedProcess([], 101)
                with self.assertRaisesRegex(ab.DriverError, "failed with exit 101"):
                    ab.prepare_build("base", "v2", out, skip_build=False)
                self.assertFalse((release / ab.BUILD_RECORD).exists())

    def test_a_named_preset_resolves_to_the_checked_in_file(self) -> None:
        self.assertEqual(ab.preset_path("throughput-20k-window-1fe"), ab.PRESETS / "throughput-20k-window-1fe.json")
        with contextlib.redirect_stderr(io.StringIO()) as warning:
            self.assertEqual(ab.preset_path("d1-20k"), ab.PRESETS / "throughput-20k-window-1fe.json")
        self.assertIn("deprecated", warning.getvalue())


if __name__ == "__main__":
    unittest.main()
