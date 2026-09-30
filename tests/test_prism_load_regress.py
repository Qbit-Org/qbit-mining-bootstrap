"""Tests for scripts/prism_load_regress.py (#551)."""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import io
import json
import subprocess
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prism_load_regress as regress  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "prism-load-trend" / "regression"
CONFIG = regress.load_config(regress.CONFIG)


def commit(n: int) -> str:
    return hashlib.sha1(f"synthetic commit {n:02d}".encode()).hexdigest()


def fixture_history() -> list[dict]:
    rows, notes = regress.read_history(FIXTURE / "history")
    assert not notes, notes
    return rows


def fixture_current() -> dict:
    return regress.read_row_files(FIXTURE / "current")[0]


def rerun(template: dict, n: int, **phase) -> dict:
    """The fixture's row as run 1000+n on synthetic commit n."""
    row = copy.deepcopy(template)
    row.update(run_id=1000 + n, commit=commit(n), recorded_at=f"2026-09-{n:02d}T02:40:00Z",
               run_url=f"https://github.com/Qbit-Org/qbit-mining-bootstrap/actions/runs/{1000 + n}")
    row["headline"]["phases"]["steady_state"].update(phase)
    return row


def by_metric(verdict: dict) -> dict[str, dict]:
    return {r["metric"]: r for r in verdict["regressions"]}


class SyntheticRegression(unittest.TestCase):
    """#551's acceptance shape: a sleep inside the share append."""

    def test_the_fixture_flags_the_share_append_numbers_with_the_commit_range(self) -> None:
        verdict = regress.evaluate([fixture_current()], fixture_history(), CONFIG, None, None)
        flagged = by_metric(verdict)
        self.assertEqual(set(flagged), {"server_ack_mean_ms", "client_ack_p99_ms", "client_ack_p50_ms"})
        for entry in flagged.values():
            self.assertEqual(entry["commit_range"], {"from": commit(12), "to": commit(13)})
            self.assertEqual(entry["last_good"]["run_id"], 1012)
            self.assertEqual(entry["first_bad"]["run_id"], 1013)
            self.assertEqual(entry["runs_in_streak"], 1)
            self.assertEqual(entry["phase"], "steady_state")
            self.assertEqual(entry["fsync_band"], "fast")
        self.assertEqual(flagged["server_ack_mean_ms"]["current"]["value"], 12.1)
        self.assertAlmostEqual(flagged["server_ack_mean_ms"]["baseline"]["median"], 2.0, places=2)
        self.assertTrue(verdict["provisional"])
        self.assertTrue(verdict["report_only"])

    def test_the_last_good_run_says_whether_it_was_itself_judged(self) -> None:
        # Seven earlier runs: runs 6 and 7 were judged within their own
        # baseline of five; with exactly five, run 13's range starts at an
        # unjudged run, which is within the limit that flagged run 13.
        judged = by_metric(regress.evaluate([fixture_current()], fixture_history()[-7:], CONFIG,
                                            None, None))["server_ack_mean_ms"]
        self.assertEqual(judged["last_good"]["basis"], "within its own baseline")
        first = regress.evaluate([fixture_current()], fixture_history()[-5:], CONFIG, None, None)
        entry = by_metric(first)["server_ack_mean_ms"]
        self.assertEqual(entry["commit_range"]["from"], commit(12))
        self.assertTrue(entry["last_good"]["basis"].startswith("within this run's limit (not itself judged"))
        self.assertIn("not itself judged", regress.markdown(first, None, "https://github.com"))

    def test_the_next_night_keeps_the_range_where_the_regression_began(self) -> None:
        history = fixture_history() + [fixture_current()]
        tonight = rerun(fixture_current(), 14, server_ack_mean_ms=11.8)
        entry = by_metric(regress.evaluate([tonight], history, CONFIG, None, None))["server_ack_mean_ms"]
        self.assertEqual(entry["commit_range"], {"from": commit(12), "to": commit(13)})
        self.assertEqual(entry["runs_in_streak"], 2)
        # The baseline is retaken from before the streak, not from itself.
        self.assertAlmostEqual(entry["baseline"]["median"], 2.0, places=2)

    def test_an_unmeasured_run_widens_the_range_and_never_splits_a_streak(self) -> None:
        history = fixture_history()
        # Run 12 completed but did not measure the number: it is no baseline
        # and no verdict. Before the regression, the range spans it (the
        # change may be its commit); inside one, first_bad stays at the
        # earlier measured bad run.
        history[-1]["headline"]["phases"]["steady_state"]["server_ack_mean_ms"] = None
        entry = by_metric(regress.evaluate([fixture_current()], history, CONFIG, None, None))[
            "server_ack_mean_ms"]
        self.assertEqual(entry["commit_range"], {"from": commit(11), "to": commit(13)})
        history[-2]["headline"]["phases"]["steady_state"]["server_ack_mean_ms"] = 12.0
        entry = by_metric(regress.evaluate([fixture_current()], history, CONFIG, None, None))[
            "server_ack_mean_ms"]
        self.assertEqual(entry["commit_range"], {"from": commit(10), "to": commit(11)})

    def test_a_lasting_regression_stays_flagged_until_it_has_lasted_a_window(self) -> None:
        history = fixture_history() + [fixture_current()]
        for n in range(14, 26):
            history.append(rerun(fixture_current(), n, server_ack_mean_ms=12.0))
        # Night 26: thirteen flagged runs so far, all out of the baseline.
        entry = by_metric(regress.evaluate([rerun(fixture_current(), 26, server_ack_mean_ms=12.0)],
                                           history, CONFIG, None, None))["server_ack_mean_ms"]
        self.assertEqual(entry["commit_range"], {"from": commit(12), "to": commit(13)})
        self.assertEqual(entry["runs_in_streak"], 14)
        self.assertAlmostEqual(entry["baseline"]["median"], 2.0, places=2)
        # Night 27: fourteen flagged runs in a row are the accepted level.
        history.append(rerun(fixture_current(), 26, server_ack_mean_ms=12.0))
        verdict = regress.evaluate([rerun(fixture_current(), 27, server_ack_mean_ms=12.0)],
                                   history, CONFIG, None, None)
        self.assertNotIn("server_ack_mean_ms", by_metric(verdict))

    def test_a_run_within_the_noise_is_not_flagged(self) -> None:
        current = rerun(fixture_current(), 13, server_ack_mean_ms=2.05, client_ack_p99_ms=41.0,
                        client_ack_p50_ms=10.2)
        verdict = regress.evaluate([current], fixture_history(), CONFIG, None, None)
        self.assertEqual(verdict["regressions"], [])
        self.assertEqual(verdict["runs"][0]["numbers"]["regressed"], 0)

    def test_a_rate_drop_is_a_regression_where_lower_is_worse(self) -> None:
        current = rerun(fixture_current(), 13, server_ack_mean_ms=2.0, client_ack_p99_ms=40.0,
                        client_ack_p50_ms=10.0, achieved_rate_shares_per_second=420.0)
        flagged = by_metric(regress.evaluate([current], fixture_history(), CONFIG, None, None))
        self.assertEqual(set(flagged), {"achieved_rate_shares_per_second"})
        self.assertEqual(flagged["achieved_rate_shares_per_second"]["worse"], "lower")

    def test_the_cli_reports_and_still_exits_zero(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            out, md, gh = (Path(scratch) / name for name in ("v.json", "v.md", "gh"))
            with redirect_stdout(io.StringIO()):
                code = regress.main(["--history", str(FIXTURE / "history"), "--rows", str(FIXTURE / "current"),
                                     "--json", str(out), "--markdown", str(md), "--github-output", str(gh),
                                     "--repository", "Qbit-Org/qbit-mining-bootstrap"])
            self.assertEqual(code, 0)
            self.assertEqual(gh.read_text(), "regressions=3\n")
            self.assertEqual(json.loads(out.read_text())["schema"], regress.VERDICT_SCHEMA)
            text = md.read_text()
            self.assertIn(f"/compare/{commit(12)}...{commit(13)}", text)
            self.assertIn("provisional", text)


class NoVerdictIsNotAPass(unittest.TestCase):
    def test_a_short_series_is_unknown(self) -> None:
        verdict = regress.evaluate([fixture_current()], fixture_history()[:3], CONFIG, None, None)
        self.assertEqual(verdict["regressions"], [])
        numbers = verdict["runs"][0]["numbers"]
        self.assertEqual(numbers["within"], 0)
        self.assertGreater(numbers["unknown"], 0)

    def test_runs_in_another_fsync_band_are_not_a_baseline(self) -> None:
        history = fixture_history()
        for row in history:
            row["fsync"]["usecs_per_op"] = 5000.0
        verdict = regress.evaluate([fixture_current()], history, CONFIG, None, None)
        self.assertEqual(verdict["regressions"], [])
        self.assertEqual(verdict["runs"][0]["fsync_band"], "fast")
        self.assertEqual(verdict["runs"][0]["numbers"]["within"], 0)

    def test_an_unmeasured_fsync_cost_gives_no_verdict(self) -> None:
        current = fixture_current()
        current["fsync"]["usecs_per_op"] = None
        entry = regress.evaluate([current], fixture_history(), CONFIG, None, None)["runs"][0]
        self.assertEqual(entry["status"], "unknown")
        self.assertIn("fsync", entry["reason"])

    def test_a_changed_preset_starts_a_new_baseline(self) -> None:
        current = fixture_current()
        current["preset_sha256"] = "0" * 64
        verdict = regress.evaluate([current], fixture_history(), CONFIG, None, None)
        self.assertEqual(verdict["regressions"], [])

    def test_an_incomplete_run_is_not_evaluated_nor_a_baseline(self) -> None:
        current = fixture_current()
        current["outcome"]["harness_exit_code"] = 4
        entry = regress.evaluate([current], fixture_history(), CONFIG, None, None)["runs"][0]
        self.assertEqual(entry["status"], "not evaluated")
        history = fixture_history()
        for row in history[:10]:
            row["outcome"]["harness_exit_code"] = 3
        verdict = regress.evaluate([fixture_current()], history, CONFIG, None, None)
        self.assertEqual(verdict["regressions"], [])

    def test_a_number_the_run_did_not_measure_is_unknown(self) -> None:
        current = rerun(fixture_current(), 13, server_ack_mean_ms=None)
        verdict = regress.evaluate([current], fixture_history(), CONFIG, None, None)
        self.assertNotIn("server_ack_mean_ms", by_metric(verdict))
        self.assertIn("steady_state.server_ack_mean_ms: not measured in this run",
                      verdict["runs"][0]["unknown"])

    def test_a_preset_that_left_no_row_is_shown(self) -> None:
        missing = {"schema": regress.ROW_SCHEMA, "kind": "missing", "lane": "L2", "run_id": 1013,
                   "run_attempt": 1, "repository": "Qbit-Org/qbit-mining-bootstrap",
                   "preset": "mainnet-shape-130-addresses"}
        current = rerun(fixture_current(), 13, server_ack_mean_ms=2.0, client_ack_p99_ms=40.0,
                        client_ack_p50_ms=10.0)
        verdict = regress.evaluate([current, missing], fixture_history(), CONFIG, None, None)
        entry = [e for e in verdict["runs"] if e["preset"] == "mainnet-shape-130-addresses"]
        self.assertEqual(len(entry), 1)
        self.assertEqual(entry[0]["status"], "not evaluated")
        text = regress.markdown(verdict, None, "https://github.com")
        self.assertIn("1 planned preset(s) left no measurement", text)

    def test_a_later_missing_attempt_hides_the_earlier_measurement(self) -> None:
        current = fixture_current()
        missing = {"schema": regress.ROW_SCHEMA, "kind": "missing", "lane": "L2",
                   "run_id": current["run_id"], "run_attempt": 2, "repository": current["repository"],
                   "preset": current["preset"]}
        verdict = regress.evaluate([current, missing], fixture_history(), CONFIG, None, None)
        self.assertEqual(verdict["regressions"], [])
        self.assertEqual([e["status"] for e in verdict["runs"]], ["not evaluated"])
        self.assertEqual(verdict["runs"][0]["run_attempt"], 2)

    def test_a_later_missing_attempt_in_another_lane_still_supersedes(self) -> None:
        current = fixture_current()
        missing = {"schema": regress.ROW_SCHEMA, "kind": "missing", "lane": "weekly",
                   "run_id": current["run_id"], "run_attempt": 2, "repository": current["repository"],
                   "preset": current["preset"]}
        verdict = regress.evaluate([current, missing], fixture_history(), CONFIG, None, None)
        self.assertEqual(verdict["regressions"], [])
        self.assertEqual([(e["status"], e["run_attempt"]) for e in verdict["runs"]], [("not evaluated", 2)])

    def test_a_lane_the_rule_does_not_read_is_named(self) -> None:
        current = fixture_current()
        current["lane"] = "L5"
        entry = regress.evaluate([current], fixture_history(), CONFIG, None, None)["runs"][0]
        self.assertEqual(entry["status"], "not evaluated")


class History(unittest.TestCase):
    def test_the_latest_attempt_of_a_run_stands(self) -> None:
        history = fixture_history()
        # Run 1012's first attempt was slow; its second is the one that counts.
        slow = copy.deepcopy(history[-1])
        slow["run_attempt"] = 0
        slow["headline"]["phases"]["steady_state"]["server_ack_mean_ms"] = 50.0
        kept = regress.latest_attempts(history + [slow])
        self.assertEqual(len(kept), len(history))
        self.assertNotIn(50.0, [r["headline"]["phases"]["steady_state"]["server_ack_mean_ms"] for r in kept])

    def test_the_current_run_is_never_its_own_baseline(self) -> None:
        history = fixture_history() + [fixture_current()]
        verdict = regress.evaluate([fixture_current()], history, CONFIG, None, None)
        self.assertEqual(by_metric(verdict)["server_ack_mean_ms"]["commit_range"]["from"], commit(12))

    def test_foreign_and_broken_lines_are_skipped_and_counted(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            lane = Path(scratch) / "trend" / "L2"
            lane.mkdir(parents=True)
            good = (FIXTURE / "history" / "trend" / "L2" / "2026-09.jsonl").read_text()
            (lane / "2026-09.jsonl").write_text(
                good + '{"schema": "qbit.prism.load-trend-row.v9"}\nnot json\n\n')
            rows, notes = regress.read_history(Path(scratch))
        self.assertEqual(len(rows), 12)
        self.assertEqual(len(notes), 2)


VARIANCE = ROOT / "tests" / "fixtures" / "prism-load-trend" / "variance"


def real_variance() -> dict:
    """#542's document as scripts/prism_load_probe.py `variance` writes it."""
    data, note = regress.load_variance(VARIANCE / "runner-probe-variance.json")
    assert note is None, note
    return data


def spread_group(data: dict, band: str) -> dict:
    (found,) = [g for g in data["groups"] if g["fsync_band"] == band]
    return found


class Variance(unittest.TestCase):
    def write(self, scratch: str, document: dict) -> Path:
        path = Path(scratch) / "variance.json"
        path.write_text(json.dumps(document))
        return path

    def test_the_fixture_is_what_the_probe_writes(self) -> None:
        # Regenerate with generate.py when #542's document changes shape.
        done = subprocess.run([sys.executable, str(VARIANCE / "generate.py"), "--check"])
        self.assertEqual(done.returncode, 0, "the variance fixture is stale; run generate.py")

    def test_other_schemas_and_versions_are_refused_by_name(self) -> None:
        for schema in ("something.v7", "qbit.prism.runner-probe-variance.v2", None):
            with self.subTest(schema=schema), tempfile.TemporaryDirectory() as scratch:
                data, note = regress.load_variance(self.write(scratch, dict(real_variance(), schema=schema)))
                self.assertIsNone(data)
                self.assertIn(repr(schema), note)
                self.assertIn("provisional", note)
        with tempfile.TemporaryDirectory() as scratch:
            data, note = regress.load_variance(self.write(scratch, {"schema": regress.VARIANCE_SCHEMA}))
        self.assertIsNone(data)
        self.assertIn("no groups", note)

    def test_the_spread_is_all_runs_cv_from_the_runs_own_band(self) -> None:
        data = real_variance()
        verdict = regress.evaluate([fixture_current()], fixture_history(), CONFIG, data, None)
        entry = by_metric(verdict)["client_ack_p99_ms"]
        cv = spread_group(data, "500-1000us")["metrics"]["steady_state.ack_p99_ms"]["all"]["cv"]
        self.assertEqual(entry["baseline"]["spread_source"], "variance (#542, 500-1000us band, n 8)")
        self.assertAlmostEqual(entry["baseline"]["spread"], cv * entry["baseline"]["median"])
        # #542 does not measure the server-side ACK: provisional, and named.
        self.assertTrue(by_metric(verdict)["server_ack_mean_ms"]["baseline"]["spread_source"]
                        .startswith("provisional"))
        self.assertTrue(verdict["provisional"])
        self.assertTrue(any("steady_state.server_ack_mean_ms" in n for n in verdict["notes"]))
        # VM 4 is slower: VM-to-VM over 2x run-to-run points at #511's A/B.
        self.assertTrue(any("#511" in n and "steady_state.client_ack_p99_ms" in n for n in verdict["notes"]))

    def test_a_band_with_no_spread_falls_back_to_all_bands_then_provisional(self) -> None:
        data = real_variance()
        band = spread_group(data, "500-1000us")
        band["metrics"]["steady_state.ack_p99_ms"]["all"].update(n=1, cv=None)
        verdict = regress.evaluate([fixture_current()], fixture_history(), CONFIG, data, None)
        self.assertEqual(by_metric(verdict)["client_ack_p99_ms"]["baseline"]["spread_source"],
                         "variance (#542, all bands, n 8)")
        for group in data["groups"]:
            group["metrics"]["steady_state.ack_p99_ms"]["all"].update(cv=None)
        verdict = regress.evaluate([fixture_current()], fixture_history(), CONFIG, data, None)
        self.assertTrue(by_metric(verdict)["client_ack_p99_ms"]["baseline"]["spread_source"]
                        .startswith("provisional"))

    def test_each_replayed_run_is_judged_with_its_own_band(self) -> None:
        # Earlier runs at 499 µs/op are the config's `fast` band like the
        # current run, but #542's 250-500us band: there the ack p99 spread is
        # made huge, so run 12's inflated p99 is within its own limit, and the
        # range of run 13's regression cannot start before it.
        data = real_variance()
        wide = copy.deepcopy(spread_group(data, "500-1000us"))
        wide["fsync_band"] = "250-500us"
        wide["metrics"]["steady_state.ack_p99_ms"]["all"]["cv"] = 5.0
        data["groups"].append(wide)
        history = fixture_history()
        history[-1]["fsync"]["usecs_per_op"] = 499.0
        history[-1]["headline"]["phases"]["steady_state"]["client_ack_p99_ms"] = 90.0
        verdict = regress.evaluate([fixture_current()], history, CONFIG, data, None)
        entry = by_metric(verdict)["client_ack_p99_ms"]
        self.assertEqual(entry["commit_range"], {"from": commit(12), "to": commit(13)})
        # In the current run's band, run 12 is flagged too: the regression
        # then began at run 12, after run 11.
        history[-1]["fsync"]["usecs_per_op"] = 500.0
        verdict = regress.evaluate([fixture_current()], history, CONFIG, data, None)
        self.assertEqual(by_metric(verdict)["client_ack_p99_ms"]["commit_range"],
                         {"from": commit(11), "to": commit(12)})

    def test_another_runner_class_has_no_spread(self) -> None:
        current = fixture_current()
        history = fixture_history()
        for row in [current, *history]:
            row["runner_class"] = "blacksmith-16vcpu-ubuntu-2404"
        verdict = regress.evaluate([current], history, CONFIG, real_variance(), None)
        self.assertTrue(by_metric(verdict)["client_ack_p99_ms"]["baseline"]["spread_source"]
                        .startswith("provisional"))

    def test_full_coverage_clears_provisional_and_a_measured_zero_counts(self) -> None:
        covered = tuple(m for m in CONFIG.metrics if m.key in regress.VARIANCE_METRICS
                        and m.key != "tip_last_notify_p99_ms")
        calibrated = dataclasses.replace(CONFIG, provisional=False, metrics=covered)
        data = real_variance()
        verdict = regress.evaluate([fixture_current()], fixture_history(), calibrated, data, None)
        self.assertFalse(verdict["provisional"], verdict["notes"])
        spread_group(data, "500-1000us")["metrics"]["steady_state.ack_p99_ms"]["all"]["cv"] = 0.0
        verdict = regress.evaluate([fixture_current()], fixture_history(), calibrated, data, None)
        self.assertFalse(verdict["provisional"])
        # A replayed (and judged) run whose #542 band, and the all-band
        # group, have no spread keeps it provisional, though this run's own
        # band has one.
        history = fixture_history()
        history[8]["fsync"]["usecs_per_op"] = 499.0
        partial = copy.deepcopy(data)
        del spread_group(partial, "all")["metrics"]["steady_state.ack_p99_ms"]
        verdict = regress.evaluate([fixture_current()], history, calibrated, partial, None)
        self.assertTrue(verdict["provisional"])
        self.assertTrue(any("steady_state.client_ack_p99_ms" in n for n in verdict["notes"]))
        del spread_group(data, "500-1000us")["metrics"]["steady_state.ack_p50_ms"]
        del spread_group(data, "all")["metrics"]["steady_state.ack_p50_ms"]
        verdict = regress.evaluate([fixture_current()], fixture_history(), calibrated, data, None)
        self.assertTrue(verdict["provisional"])
        self.assertTrue(any("steady_state.client_ack_p50_ms" in n for n in verdict["notes"]))


class Config(unittest.TestCase):
    def load(self, text: str) -> regress.Config:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "c.toml"
            path.write_text(text)
            return regress.load_config(path)

    def base(self) -> str:
        return regress.CONFIG.read_text()

    def test_the_checked_in_config_loads_and_is_provisional(self) -> None:
        self.assertTrue(CONFIG.provisional)
        self.assertEqual(CONFIG.lanes, ("L2",))
        self.assertEqual(CONFIG.band_names, ("fast", "medium", "slow"))

    def test_invalid_values_are_refused(self) -> None:
        for old, new in [
            ("k = 3.0", "k = 0.0"),
            ("k = 3.0", 'k = "3"'),
            ("min_baseline = 5", "min_baseline = 20"),
            ("window = 14", "window = 14\nwidow = 3"),
            ('names = ["fast", "medium", "slow"]', 'names = ["fast", "slow"]'),
            ("edges_usecs = [1000.0, 4000.0]", "edges_usecs = [4000.0, 1000.0]"),
            ('worse = "lower"', 'worse = "down"'),
            ("min_absolute_change = 1.0\n", "min_absolute_change = nan\n"),
            ('schema = "qbit.prism.load-regression-config.v1"', 'schema = "v0"'),
        ]:
            with self.subTest(new=new), self.assertRaises(regress.RegressError):
                self.load(self.base().replace(old, new, 1))

    def test_bands(self) -> None:
        for value, band in [(None, "unknown"), (0, "unknown"), (float("nan"), "unknown"), (True, "unknown"),
                            (500, "fast"), (1000.0, "medium"), (3999.9, "medium"), (4000, "slow")]:
            with self.subTest(value=value):
                self.assertEqual(regress.fsync_band(value, CONFIG), band)

    def test_no_rows_is_no_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            with redirect_stderr(io.StringIO()) as err:
                code = regress.main(["--history", scratch, "--rows", str(Path(scratch) / "absent")])
        self.assertEqual(code, 2)
        self.assertIn("no trend rows", err.getvalue())

    def test_rows_with_no_preset_run_say_nothing_was_compared(self) -> None:
        job = {"schema": regress.ROW_SCHEMA, "kind": "job", "lane": "L4", "run_id": 1}
        verdict = regress.evaluate([job], fixture_history(), CONFIG, None, None)
        self.assertIn("No verdict", regress.markdown(verdict, None, "https://github.com"))

    def test_a_run_with_every_number_unknown_is_no_verdict(self) -> None:
        verdict = regress.evaluate([fixture_current()], fixture_history()[:3], CONFIG, None, None)
        text = regress.markdown(verdict, None, "https://github.com")
        self.assertIn("No verdict", text)
        self.assertNotIn("No number regressed", text)

    def test_a_bad_config_exits_two(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "c.toml"
            path.write_text('schema = "nope"\n')
            with redirect_stderr(io.StringIO()):
                code = regress.main(["--history", scratch, "--rows", scratch, "--config", str(path)])
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
