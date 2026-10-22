"""Tests for scripts/prism_load_trend.py (#551)."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prism_load_trend as trend  # noqa: E402

PG_TEST_FSYNC = """\
Compare file sync methods using one 8kB write:
(in wal_sync_method preference order, except fdatasync is Linux's default)
        open_datasync                      2051.170 ops/sec     488 usecs/op
        fdatasync                          2034.433 ops/sec     492 usecs/op
        fsync                              1011.234 ops/sec     989 usecs/op
"""


def ack_delta(count: float, seconds: float, buckets: dict[str, float], unavailable=None) -> dict:
    return {"instance_id": "fe", "counts": {"accepted": count}, "sums": {"accepted": seconds},
            "bucket_deltas": {"accepted": buckets}, "unavailable_reason": unavailable}


REPORT = {
    "schema": "qbit.prism.load-harness.v1",
    "run_id": "harness-1",
    "started_at": "2026-09-30T02:20:00Z",
    "finished_at": "2026-09-30T02:30:00Z",
    "dirty": False,
    "artifact_kind": "qualification",
    "versions": {"server_build_profile": "release",
                 "server_revision_evidence": {"status": "established"}},
    "durability_findings": [],
    "time_to_usable_work": {"tips": [{"all_sessions_milliseconds": 120.0},
                                     {"all_sessions_milliseconds": 300.0}]},
    "phases": [
        {
            "name": "steady_state", "in_artifact": True, "completed": True,
            "duration_seconds": 120.0, "target_rate_shares_per_second": 500,
            "achieved_rate_shares_per_second": 499.5, "shortfall": 0,
            "client_ack_latency": {"unit": "milliseconds", "p50": 9.0, "p99": 38.0},
            "server_share_ack_seconds": [
                ack_delta(100, 0.2, {"0.001": 10, "0.005": 99, "0.01": 100, "+Inf": 100}),
                ack_delta(100, 0.4, {"0.001": 0, "0.005": 98, "0.01": 100, "+Inf": 100}),
            ],
            "order_lock": {"max_waiters": 0, "mean_waiters": 0.0},
            "processes": [{"peak_rss_kib": 204800}, {"peak_rss_kib": 102400}],
            "rejected_valid_shares": 0,
            "reconciliation": {"missing": 0},
        },
        {
            "name": "warmup", "in_artifact": False, "completed": True,
            "client_ack_latency": {"unit": "seconds", "p99": 0.04},
            "server_share_ack_seconds": [ack_delta(10, 0.1, {}, unavailable="scrape failed")],
            "order_lock": {"max_waiters": None, "mean_waiters": None},
            "processes": [{"peak_rss_kib": None}],
        },
    ],
}


def run_args(**overrides) -> argparse.Namespace:
    values = dict(run_id=77, run_attempt=1, event="schedule", repository="Qbit-Org/qbit-mining-bootstrap",
                  server_url="https://github.com", ref="3.x.x", commit="a" * 40,
                  recorded_at="2026-09-30T02:40:00Z", presets=None)
    values.update(overrides)
    return argparse.Namespace(**values)


class Row(unittest.TestCase):
    def setUp(self) -> None:
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        base = Path(self.scratch.name)
        self.out = base / "out"
        self.out.mkdir()
        self.presets = base / "presets"
        self.presets.mkdir()
        (self.presets / "nightly-one.json").write_text(json.dumps({"name": "nightly-one", "schedule": "nightly"}))
        (self.presets / "soak-weekly.json").write_text(json.dumps({"name": "soak-weekly", "schedule": "weekly"}))

    def build(self, preset: str = "nightly-one") -> dict:
        args = run_args(presets=self.presets, dir=self.out, preset=preset, lane="auto",
                        runner_class="blacksmith-8vcpu-ubuntu-2404", artifact_name=f"prism-load-{preset}",
                        artifact_url="https://example.invalid/a/artifacts/1", artifact_id="",
                        config=trend.regress.CONFIG)
        return trend.build_row(args)

    def write_full_run(self) -> None:
        (self.out / "load-harness-report.json").write_text(json.dumps(REPORT))
        (self.out / "pg_test_fsync.txt").write_text(PG_TEST_FSYNC)
        (self.out / "harness-exit-code").write_text("0\n")
        (self.out / "gate-exit-code").write_text("0\n")
        (self.out / "host.json").write_text(json.dumps({
            "runner_label": "blacksmith-8vcpu-ubuntu-2404", "nproc": 8, "cpu_model": "EPYC",
            "mem_total_mib": 32000.0, "kernel": "6.8", "filesystem": {"type": "ext4", "options": "rw"},
            "block_device": {"write_cache": "write back", "fua": "0"}}))

    def test_a_full_run_has_its_headline_numbers(self) -> None:
        self.write_full_run()
        row = self.build()
        trend.validate(row)
        self.assertEqual(row["lane"], "L2")
        self.assertEqual(row["fsync"]["usecs_per_op"], 492.0)
        self.assertEqual(row["fsync"]["band"], "fast")
        self.assertEqual(row["outcome"], {"harness_exit_code": 0, "gate_exit_code": 0, "result": "pass",
                                          "provenance": "pass"})
        self.assertEqual(row["host"]["write_cache"], "write back")
        self.assertEqual(len(row["preset_sha256"]), 64)
        steady = row["headline"]["phases"]["steady_state"]
        self.assertAlmostEqual(steady["server_ack_mean_ms"], 3.0)
        self.assertEqual(steady["server_ack_p99_upper_ms"], 10.0)
        self.assertEqual(steady["client_ack_p99_ms"], 38.0)
        self.assertEqual(steady["peak_rss_mib"], 200.0)
        self.assertEqual(steady["order_lock_max_waiters"], 0)
        self.assertEqual(steady["order_lock_mean_waiters"], 0.0)
        self.assertEqual(row["headline"]["run"]["tip_last_notify_p99_ms"], 300.0)
        self.assertEqual(row["headline"]["run"]["durability_findings"], 0)
        self.assertEqual(row["artifact"]["name"], "prism-load-nightly-one")
        self.assertEqual(row["artifact"]["id"], 1)
        self.assertIsNone(row["asset_url"])

    def test_what_was_not_measured_is_null_not_zero(self) -> None:
        self.write_full_run()
        warmup = self.build()["headline"]["phases"]["warmup"]
        self.assertFalse(warmup["in_artifact"])
        # A latency in another unit, an unavailable server delta, a sampler
        # that saw nothing and a missing peak RSS are all unknown.
        for key in ("client_ack_p99_ms", "server_ack_mean_ms", "server_ack_p99_upper_ms",
                    "order_lock_max_waiters", "order_lock_mean_waiters", "peak_rss_mib",
                    "achieved_rate_shares_per_second", "shortfall"):
            with self.subTest(key=key):
                self.assertIsNone(warmup[key])

    def test_a_run_that_left_nothing_has_a_row_of_unknowns(self) -> None:
        (self.out / "pg_test_fsync.txt").write_text("pg_test_fsync failed; see above\n")
        row = self.build()
        self.assertEqual(row["outcome"]["result"], "no verdict")
        self.assertIsNone(row["outcome"]["harness_exit_code"])
        self.assertEqual(row["fsync"]["band"], "unknown")
        self.assertIsNone(row["fsync"]["usecs_per_op"])
        self.assertIsNone(row["host"])
        self.assertIsNone(row["report"])
        self.assertEqual(row["headline"]["phases"], {})
        self.assertIsNone(row["headline"]["run"]["tip_last_notify_p99_ms"])
        self.assertIsNone(row["headline"]["run"]["durability_findings"])

    def test_the_weekly_soak_is_l5(self) -> None:
        self.write_full_run()
        self.assertEqual(self.build("soak-weekly")["lane"], "weekly")

    def test_a_later_attempt_links_its_own_attempt(self) -> None:
        row = trend.base_row(run_args(run_attempt=2), "preset", "L2")
        self.assertTrue(row["run_url"].endswith("/runs/77/attempts/2"))


class RerunRows(unittest.TestCase):
    def test_a_rerun_preset_job_that_left_no_row_is_missing_at_its_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            base = Path(scratch)
            rows_dir, presets = base / "rows", base / "presets"
            rows_dir.mkdir()
            presets.mkdir()
            for name in ("a", "b"):
                (presets / f"{name}.json").write_text(json.dumps({"schedule": "nightly"}))
                stale = trend.base_row(run_args(run_attempt=1), "preset", "L2")
                stale["preset"] = name
                (rows_dir / f"{name}.json").write_text(json.dumps(stale))
            jobs = base / "jobs.jsonl"
            # `a` was re-run in attempt 2 and died before building its row.
            jobs.write_text('{"name": "a", "run_attempt": 2}\n{"name": "b", "run_attempt": 1}\n'
                            '{"name": "evidence", "run_attempt": 2}\n')
            args = run_args(run_attempt=2, presets=presets, needs="{}", rows=rows_dir, jobs=jobs,
                            matrix=json.dumps({"include": [{"preset": "a"}, {"preset": "b"}]}))
            rows = trend.job_rows(args)
        self.assertEqual([(r["kind"], r["preset"], r["run_attempt"]) for r in rows], [("missing", "a", 2)])
        self.assertTrue(rows[0]["run_url"].endswith("/runs/77/attempts/2"))

    def test_a_missing_row_links_the_preset_jobs_attempt_not_this_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            base = Path(scratch)
            (base / "rows").mkdir()
            (base / "presets").mkdir()
            (base / "presets" / "a.json").write_text(json.dumps({"schedule": "nightly"}))
            (base / "jobs.jsonl").write_text('{"name": "a", "run_attempt": 1}\n')
            args = run_args(run_attempt=2, presets=base / "presets", needs="{}", rows=base / "rows",
                            jobs=base / "jobs.jsonl", matrix=json.dumps({"include": [{"preset": "a"}]}))
            (row,) = trend.job_rows(args)
        self.assertEqual(row["run_attempt"], 1)
        self.assertTrue(row["run_url"].endswith("/runs/77"))


class PlanFilter(unittest.TestCase):
    def test_rows_outside_the_plan_and_job_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            base = Path(scratch)
            (base / "rows").mkdir()
            (base / "presets").mkdir()
            for name in ("kept", "dropped"):
                (base / "presets" / f"{name}.json").write_text(json.dumps({"schedule": "nightly"}))
                row = trend.base_row(run_args(run_attempt=1), "preset", "L2")
                row["preset"] = name
                (base / "rows" / f"{name}.json").write_text(json.dumps(row))
            needs = {"bridging": {"result": "success", "outputs": {"commit": "d" * 40, "attempt": "1"}}}
            args = run_args(run_attempt=3, presets=base / "presets", needs=json.dumps(needs),
                            rows=base / "rows", jobs=None,
                            matrix=json.dumps({"include": [{"preset": "kept"}]}))
            with mock.patch("sys.stderr"):
                rows = trend.job_rows(args)
            left = sorted(p.name for p in (base / "rows").glob("*.json"))
        self.assertEqual(left, ["kept.json"])
        (bridging,) = rows
        # Re-running only the evidence job (attempt 3) does not move the
        # bridging job's row off the attempt it ran in.
        self.assertEqual((bridging["job"], bridging["run_attempt"]), ("bridging", 1))
        self.assertTrue(bridging["run_url"].endswith("/runs/77"))


class Validate(unittest.TestCase):
    def row(self, **overrides) -> dict:
        row = trend.base_row(run_args(), "preset", "L2")
        row.update(preset="p")
        row.update(overrides)
        return row

    def test_pull_request_and_dispatch_rows_never_write(self) -> None:
        for event in ("pull_request", "workflow_dispatch", "pull_request_target", None):
            with self.subTest(event=event), self.assertRaisesRegex(trend.TrendError, "artifacts only"):
                trend.validate(self.row(event=event))

    def test_only_a_v_tag_push_writes(self) -> None:
        trend.validate(self.row(event="push", ref="refs/tags/v3.0.0"))
        for ref in ("refs/heads/3.x.x", "3.x.x", None, "refs/tags/ci-evidence/2026-09"):
            with self.subTest(ref=ref), self.assertRaisesRegex(trend.TrendError, "tag push"):
                trend.validate(self.row(event="push", ref=ref))

    def test_a_job_rerun_on_a_moved_branch_is_a_new_row(self) -> None:
        job = self.row(kind="job", lane="L4", job="live-nightly", outcome={"result": "success"})
        moved = dict(job, commit="b" * 40)
        self.assertNotEqual(trend.identity(job), trend.identity(moved))
        # A genuine re-run of the job, even with the same outcome, is a new row.
        self.assertNotEqual(trend.identity(job), trend.identity(dict(job, run_attempt=2)))
        self.assertEqual(trend.identity(job), trend.identity(dict(job, recorded_at="2026-10-01T00:00:00Z")))

    def test_malformed_rows_are_refused(self) -> None:
        for overrides in ({"schema": "v0"}, {"lane": "L9"}, {"kind": "other"}, {"run_id": 0},
                          {"run_attempt": True}, {"recorded_at": "2026-09-30"}, {"repository": "x"},
                          {"preset": None}, {"kind": "job", "job": "live-nightly"}):
            with self.subTest(overrides=overrides), self.assertRaises(trend.TrendError):
                trend.validate(self.row(**overrides))

    def test_a_promotion_row_is_written_by_cite_or_a_writer_job(self) -> None:
        trend.validate(self.row(kind="promotion", event="cite"))
        trend.validate(self.row(kind="promotion", event="schedule"))
        with self.assertRaises(trend.TrendError):
            trend.validate(self.row(kind="promotion", event="workflow_dispatch"))

    def test_months(self) -> None:
        self.assertEqual(trend.month_of(self.row()), "2026-09")


class JobRows(unittest.TestCase):
    def test_jobs_that_ran_and_presets_that_left_no_row(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            rows_dir = Path(scratch) / "rows"
            rows_dir.mkdir()
            (rows_dir / "a.json").write_text(json.dumps({"kind": "preset", "preset": "a", "run_attempt": 1}))
            presets = Path(scratch) / "presets"
            presets.mkdir()
            (presets / "b.json").write_text(json.dumps({"schedule": "nightly"}))
            needs = {
                "live-nightly": {"result": "failure", "outputs": {"commit": "c" * 40, "attempt": "1"}},
                "live-weekly": {"result": "skipped", "outputs": {}},
                "stratum-fuzz": {"result": "success", "outputs": {}},
                "run": {"result": "failure", "outputs": {}},
            }
            args = run_args(presets=presets, needs=json.dumps(needs), rows=rows_dir, jobs=None,
                            matrix=json.dumps({"include": [{"preset": "a"}, {"preset": "b"}]}))
            rows = trend.job_rows(args)
        by = {(r["kind"], r.get("job") or r.get("preset")): r for r in rows}
        self.assertEqual(set(by), {("job", "live-nightly"), ("job", "stratum-fuzz"), ("missing", "b")})
        self.assertEqual(by[("job", "live-nightly")]["lane"], "L4")
        self.assertEqual(by[("job", "live-nightly")]["commit"], "c" * 40)
        self.assertEqual(by[("job", "live-nightly")]["outcome"], {"result": "failure"})
        self.assertEqual(by[("job", "live-nightly")]["run_attempt"], 1)
        # The fuzz job did not say which commit it ran: unknown, not the plan's.
        self.assertIsNone(by[("job", "stratum-fuzz")]["commit"])
        missing = by[("missing", "b")]
        self.assertTrue(missing["outcome"]["result"].startswith("missing"))
        self.assertNotIn("headline", missing)
        # It can never take the identity of the preset's real row.
        real = dict(missing, kind="preset")
        self.assertNotEqual(trend.identity(missing), trend.identity(real))
        for row in rows:
            trend.validate(row)


def git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


class Remote(unittest.TestCase):
    """`append` and `fetch` against a local bare repository."""

    def setUp(self) -> None:
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        env = {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
               "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
               "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid"}
        patcher = mock.patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)
        base = Path(self.scratch.name)
        self.bare = base / "remote.git"
        git("init", "--bare", "-q", str(self.bare), cwd=base)
        self.url = self.bare.as_uri()
        self.writers = []
        for name in ("a", "b"):
            clone = base / name
            clone.mkdir()
            git("init", "-q", cwd=clone)
            git("remote", "add", "origin", self.url, cwd=clone)
            self.writers.append(clone)

    def branch(self, index: int = 0, cls=trend.Branch) -> trend.Branch:
        return cls(self.writers[index], "origin", trend.Deadline(60))

    def row(self, run_id: int, preset: str = "p", attempt: int = 1, lane: str = "L2",
            recorded_at: str = "2026-09-30T02:40:00Z") -> dict:
        row = trend.base_row(run_args(run_id=run_id, run_attempt=attempt, recorded_at=recorded_at), "preset", lane)
        row.update(preset=preset)
        return row

    def recorded(self, path: str = "trend/L2/2026-09.jsonl") -> list[dict]:
        text = git("--git-dir", str(self.bare), "show", f"{trend.BRANCH}:{path}", cwd=self.bare)
        return [json.loads(line) for line in text.splitlines()]

    def quiet(self, *args, **kwargs):
        return trend.append_rows(*args, backoff=0, log=lambda *_: None, **kwargs)

    def test_the_first_write_creates_the_orphan_branch(self) -> None:
        result = self.quiet(self.branch(), [self.row(1), self.row(1, preset="q")], attempts=3)
        self.assertEqual(result["added"], 2)
        self.assertEqual([r["preset"] for r in self.recorded()], ["p", "q"])
        readme = git("--git-dir", str(self.bare), "show", f"{trend.BRANCH}:README.md", cwd=self.bare)
        self.assertIn("append-only", readme)
        parents = git("--git-dir", str(self.bare), "rev-list", "--parents", "-n1", trend.BRANCH, cwd=self.bare)
        self.assertEqual(len(parents.split()), 1, "the branch's first commit has no parent")

    def test_a_retried_write_adds_nothing(self) -> None:
        self.quiet(self.branch(), [self.row(1)], attempts=3)
        result = self.quiet(self.branch(1), [self.row(1), self.row(1)], attempts=3)
        self.assertEqual(result["added"], 0)
        self.assertEqual(len(self.recorded()), 1)

    def test_a_row_rebuilt_months_later_is_still_recognised(self) -> None:
        self.quiet(self.branch(), [self.row(1, recorded_at="2026-01-31T23:00:00Z")], attempts=1)
        result = self.quiet(self.branch(), [self.row(1, recorded_at="2026-03-01T01:00:00Z")], attempts=1)
        self.assertEqual(result["added"], 0)
        listed = git("--git-dir", str(self.bare), "ls-tree", "-r", "--name-only", trend.BRANCH, cwd=self.bare)
        self.assertNotIn("2026-03", listed)

    def test_a_later_attempt_is_a_new_row(self) -> None:
        self.quiet(self.branch(), [self.row(1)], attempts=3)
        self.quiet(self.branch(), [self.row(1, attempt=2)], attempts=3)
        self.assertEqual([r["run_attempt"] for r in self.recorded()], [1, 2])

    def test_rows_go_to_their_lane_and_month(self) -> None:
        self.quiet(self.branch(), [self.row(1), self.row(2, lane="L5"),
                                   self.row(3, recorded_at="2026-10-01T00:05:00Z")], attempts=3)
        self.assertEqual(len(self.recorded("trend/L2/2026-09.jsonl")), 1)
        self.assertEqual(len(self.recorded("trend/L5/2026-09.jsonl")), 1)
        self.assertEqual(len(self.recorded("trend/L2/2026-10.jsonl")), 1)

    def test_concurrent_writers_lose_no_row(self) -> None:
        test = self

        class Racing(trend.Branch):
            raced = False

            def push(self, commit: str):
                # Another writer lands between this writer's fetch and push.
                if not Racing.raced:
                    Racing.raced = True
                    test.quiet(test.branch(1), [test.row(2)], attempts=1)
                return super().push(commit)

        self.quiet(self.branch(), [self.row(3)], attempts=1)
        result = self.quiet(self.branch(0, Racing), [self.row(1)], attempts=3)
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(sorted(r["run_id"] for r in self.recorded()), [1, 2, 3])

    def test_concurrent_creation_of_the_branch_loses_no_row(self) -> None:
        test = self

        class Racing(trend.Branch):
            raced = False

            def push(self, commit: str):
                if not Racing.raced:
                    Racing.raced = True
                    test.quiet(test.branch(1), [test.row(2)], attempts=1)
                return super().push(commit)

        self.quiet(self.branch(0, Racing), [self.row(1)], attempts=3)
        self.assertEqual(sorted(r["run_id"] for r in self.recorded()), [1, 2])

    def test_an_unconfirmed_push_that_landed_is_not_written_twice(self) -> None:
        class Unconfirmed(trend.Branch):
            calls = 0

            def push(self, commit: str):
                Unconfirmed.calls += 1
                pushed, error = super().push(commit)
                return (False, "timed out") if Unconfirmed.calls == 1 else (pushed, error)

        result = self.quiet(self.branch(0, Unconfirmed), [self.row(1)], attempts=3)
        self.assertEqual(result, {"added": 0, "skipped": 1, "attempts": 2})
        self.assertEqual(len(self.recorded()), 1)

    def test_a_promotion_recovered_by_a_rerun_is_its_own_row(self) -> None:
        failed = self.row(1)
        failed.update(promotion={"status": "failed", "release": "ci-evidence/2026-09", "error": "x"},
                      asset_url=None)
        self.quiet(self.branch(), [failed], attempts=1)
        recovered = self.row(1)
        recovered.update(promotion={"status": "promoted", "release": "ci-evidence/2026-09", "asset": "a"},
                         asset_url="https://example.invalid/a")
        self.quiet(self.branch(), [recovered], attempts=1)
        self.quiet(self.branch(), [recovered], attempts=1)
        rows = self.recorded()
        self.assertEqual([r["kind"] for r in rows], ["preset", "promotion"])
        self.assertEqual(rows[0]["promotion"]["status"], "failed")
        self.assertEqual(rows[1]["asset_url"], "https://example.invalid/a")
        self.assertEqual((rows[1]["run_id"], rows[1]["run_attempt"], rows[1]["preset"]), (1, 1, "p"))

    def test_bounded_attempts_end_in_an_error(self) -> None:
        class Refused(trend.Branch):
            def push(self, commit: str):
                return False, "rejected"

        with self.assertRaisesRegex(trend.TrendError, "not recorded after 2 attempts"):
            self.quiet(self.branch(0, Refused), [self.row(1)], attempts=2)

    def test_a_failed_listing_is_not_a_missing_branch(self) -> None:
        broken = trend.Branch(self.writers[0], "nowhere", trend.Deadline(60))
        with self.assertRaises(trend.TrendError):
            self.quiet(broken, [self.row(1)], attempts=1)

    def test_a_refused_row_writes_nothing(self) -> None:
        bad = self.row(1)
        bad["event"] = "pull_request"
        with self.assertRaises(trend.TrendError):
            self.quiet(self.branch(), [self.row(2), bad], attempts=1)
        listed = git("ls-remote", "--heads", self.url, cwd=self.writers[0])
        self.assertEqual(listed, "")

    def test_fetch_exports_the_trend_or_nothing(self) -> None:
        out = Path(self.scratch.name) / "history"
        self.assertIsNone(trend.export(self.branch(), out))
        self.assertEqual(list(out.iterdir()), [])
        self.quiet(self.branch(), [self.row(1)], attempts=1)
        self.assertIsNotNone(trend.export(self.branch(1), out))
        rows, notes = trend.regress.read_history(out)
        self.assertEqual((len(rows), notes), (1, []))


class Promotion(unittest.TestCase):
    def rows(self, scratch: str) -> Path:
        directory = Path(scratch)
        specs = [("l2-pass", "L2", "pass"), ("l2-fail", "L2", "fail"), ("l2-regressed", "L2", "pass"),
                 ("soak", "weekly", "pass"), ("l4-job", "L4", None)]
        for name, lane, result in specs:
            row = trend.base_row(run_args(), "job" if result is None else "preset", lane)
            row.update(preset=name, outcome={"result": result}, promotion=None, asset_url=None,
                       artifact={"name": f"prism-load-{name}", "url": None})
            (directory / f"{name}.json").write_text(json.dumps(row))
        return directory

    def test_the_policy_promotes_soaks_and_flagged_l2_runs(self) -> None:
        verdict = {"regressions": [{"run_id": 77, "run_attempt": 1, "preset": "l2-regressed"}]}
        calls = []

        def uploader(row, tag, repository, create, deadline):
            calls.append((row["preset"], tag, create))
            if row["preset"] == "l2-fail":
                raise trend.TrendError("upload refused")
            if row["preset"] == "l2-regressed":
                raise OSError(28, "No space left on device")
            return f"https://example.invalid/{trend.asset_name(row)}"

        with tempfile.TemporaryDirectory() as scratch:
            directory = self.rows(scratch)
            failed = trend.promote(directory, "policy", verdict, "monthly", "o/r", trend.Deadline(60),
                                   uploader=uploader, log=lambda *_: None)
            rows = {p.stem: json.loads(p.read_text()) for p in directory.glob("*.json")}
        self.assertEqual(sorted(c[0] for c in calls), ["l2-fail", "l2-regressed", "soak"])
        self.assertTrue(all(tag == "ci-evidence/2026-09" and create for _, tag, create in calls))
        self.assertEqual(failed, 2)
        self.assertEqual(rows["l2-regressed"]["promotion"]["status"], "failed")
        self.assertIn("No space left", rows["l2-regressed"]["promotion"]["error"])
        self.assertEqual(rows["soak"]["promotion"]["status"], "promoted")
        self.assertEqual(rows["soak"]["asset_url"],
                         "https://example.invalid/weekly-soak-run77-attempt1.tar.gz")
        self.assertEqual(rows["l2-fail"]["promotion"]["status"], "failed")
        self.assertIsNone(rows["l2-fail"]["asset_url"])
        self.assertIsNone(rows["l2-pass"]["promotion"])

    def test_a_tag_release_takes_every_row_and_is_not_created_here(self) -> None:
        calls = []
        with tempfile.TemporaryDirectory() as scratch:
            trend.promote(self.rows(scratch), "all", None, "v3.0.0", "o/r", trend.Deadline(60),
                          uploader=lambda row, tag, repo, create, d: calls.append((tag, create)) or "u",
                          log=lambda *_: None)
        self.assertEqual(calls, [("v3.0.0", False)] * 4)

    def test_the_artifact_id_comes_from_the_upload(self) -> None:
        self.assertEqual(trend.artifact_id_of("42", None), 42)
        self.assertEqual(trend.artifact_id_of("", "https://github.com/o/r/actions/runs/7/artifacts/99"), 99)
        self.assertIsNone(trend.artifact_id_of("", "https://example.invalid/x"))

    def test_a_bundle_is_fetched_only_by_its_exact_artifact(self) -> None:
        row = {"kind": "preset", "run_id": 7, "run_attempt": 2, "preset": "p",
               "artifact": {"name": "prism-load-p", "id": 99, "url": None}}
        with tempfile.TemporaryDirectory() as scratch, \
                mock.patch.object(trend, "gh_json", return_value={
                    "name": "prism-load-p", "workflow_run": {"id": 8}, "expired": False}), \
                self.assertRaisesRegex(trend.TrendError, "not the row's"):
            trend.download_artifact(row, "o/r", Path(scratch) / "b", trend.Deadline(60))
        row["artifact"]["id"] = None
        with self.assertRaisesRegex(trend.TrendError, "cannot be selected exactly"):
            trend.download_artifact(row, "o/r", Path("/nonexistent"), trend.Deadline(60))

    def test_a_cited_artifact_is_attributed_to_the_attempt_that_uploaded_it(self) -> None:
        answers = {
            "repos/o/r/actions/runs/7": {"run_attempt": 3},
            "repos/o/r/actions/runs/7/attempts/3": {"run_started_at": "2026-09-30T05:00:00Z"},
            "repos/o/r/actions/runs/7/attempts/2": {"run_started_at": "2026-09-30T03:00:00Z"},
        }
        meta = {"id": 99, "workflow_run": {"id": 7}, "created_at": "2026-09-30T03:30:00Z"}
        with mock.patch.object(trend, "gh_json", side_effect=lambda path, deadline: answers[path]):
            self.assertEqual(trend.attempt_of_artifact(meta, "o/r", trend.Deadline(60)), 2)

    def test_asset_names_are_safe_and_unique_per_attempt(self) -> None:
        row = {"lane": "L2", "preset": "a b/c", "run_id": 5, "run_attempt": 2}
        self.assertEqual(trend.asset_name(row), "L2-a_b_c-run5-attempt2.tar.gz")


if __name__ == "__main__":
    unittest.main()
