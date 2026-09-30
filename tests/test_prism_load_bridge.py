"""Tests for scripts/prism_load_bridge.py, the bridging lane's row (#552)."""

from __future__ import annotations

import contextlib
import copy
import io
import json
from pathlib import Path
import re
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prism_load_bridge as bridge  # noqa: E402
import prism_load_matrix as matrix  # noqa: E402


def latency(p50: float | None, p99: float | None) -> dict:
    summary = {"unit": "milliseconds", "clock": "client monotonic", "samples": 10, "p50": p50, "p99": p99}
    if p50 is None:
        summary.update(samples=0, unavailable_reason="no samples were recorded")
    return summary


def report(side: str, ack: tuple[float | None, float | None] = (2.0, 9.0), tips=(40.0, 60.0, 80.0),
           rss=((100_000, 120_000), (110_000, 130_000))) -> dict:
    phases = []
    for index, name in enumerate(("warm_up", "steady_state", "reconnect")):
        phases.append({
            "name": name,
            "client_ack_latency": latency(*ack),
            "processes": [{"instance_id": f"fe{fe}", "peak_rss_kib": rss[fe][min(index, 1)]} for fe in range(2)],
        })
    node: dict = {"url": "http://127.0.0.1:1", "submissions": []}
    if side == "real":
        node = {
            "mode": "qbitd",
            "qbitd": {"peak_rss_kib": {"pool": 300_000, "peer": 280_000}},
            "relay": {"rpc_latency_milliseconds": {
                "submitblock": {"count": 2, "p50": 12.0, "p99": 30.0, "max": 30.0},
                "getblocktemplate": {"count": 50, "p50": 4.0, "p99": 11.0, "max": 15.0},
            }},
        }
    return {
        "schema": bridge.REPORT_SCHEMA,
        "preset": {"name": bridge.PRESETS[side]},
        "topology": {"frontends": 2},
        "phases": phases,
        "time_to_usable_work": {"tips": [
            {"all_sessions_milliseconds": value,
             "all_sessions_unavailable_reason": None if value is not None else "3 of 100 sessions got no usable work"}
            for value in tips
        ]},
        "node": node,
    }


class Runs:
    """Two run directories shaped as prism-load-run.sh leaves them."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.fake = root / "fake"
        self.real = root / "real"
        self.write(self.fake, report("fake"))
        self.write(self.real, report("real", ack=(5.0, 20.0), tips=(90.0, 100.0, 150.0),
                                     rss=((150_000, 160_000), (150_000, 170_000))))

    def write(self, directory: Path, body: dict | None, harness: str = "0", gate: str = "0",
              runner: str = "blacksmith-8vcpu-ubuntu-2404") -> None:
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "load-harness-report.json"
        if body is None:
            path.unlink(missing_ok=True)
        else:
            path.write_text(json.dumps(body), encoding="utf-8")
        (directory / "harness-exit-code").write_text(harness + "\n", encoding="utf-8")
        (directory / "gate-exit-code").write_text(gate + "\n", encoding="utf-8")
        (directory / "host.json").write_text(json.dumps({"runner_label": runner}), encoding="utf-8")

    def row(self, order: str = "fake,real") -> tuple[int, dict, str]:
        out = self.root / "row.json"
        summary = self.root / "summary.md"
        summary.unlink(missing_ok=True)
        with contextlib.redirect_stdout(io.StringIO()):
            code = bridge.main([
                "--fake", str(self.fake), "--real", str(self.real), "--order", order,
                "--out", str(out), "--summary", str(summary), "--commit", "abc123",
            ])
        return code, json.loads(out.read_text(encoding="utf-8")), summary.read_text(encoding="utf-8")


def by_name(row: dict) -> dict[str, dict]:
    return {entry["metric"]: entry for entry in row["metrics"]}


class ComparisonRow(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.runs = Runs(Path(self.directory.name))

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_each_metric_is_real_minus_fake_with_its_unit(self) -> None:
        code, row, summary = self.runs.row()
        self.assertEqual(code, 0)
        self.assertEqual(row["schema"], bridge.SCHEMA)
        self.assertTrue(row["trusted"], row["untrusted_reasons"])
        self.assertIn("Not a D1 verdict", row["statement"])
        self.assertEqual(row["order"], ["fake", "real"])
        self.assertEqual(row["runner"], "blacksmith-8vcpu-ubuntu-2404")
        metrics = by_name(row)
        ack = metrics["client_ack_latency p99 (steady_state)"]
        self.assertEqual((ack["fake"], ack["real"], ack["real_minus_fake"], ack["unit"]), (9.0, 20.0, 11.0, "milliseconds"))
        self.assertIn("client_ack_latency p50 (reconnect)", metrics)
        tip_p50 = metrics["time to usable work, slowest session, p50 over external tips"]
        self.assertEqual((tip_p50["fake"], tip_p50["real"], tip_p50["real_minus_fake"]), (60.0, 100.0, 40.0))
        self.assertEqual(metrics["time to usable work, slowest session, max over external tips"]["real_minus_fake"], 70.0)
        # VmHWM's run peak is the largest any phase saw, per frontend.
        fe1 = metrics["frontend 1 peak RSS"]
        self.assertEqual((fe1["fake"], fe1["real"], fe1["unit"]), (130_000, 170_000, "KiB"))
        self.assertEqual(metrics["frontends peak RSS, summed"]["real_minus_fake"], (160_000 + 170_000) - (120_000 + 130_000))
        self.assertIn("| client_ack_latency p99 (steady_state) | 9.0 ms | 20.0 ms | +11.0 ms |", summary)
        self.assertIn("Not a D1 verdict", summary)

    def test_what_only_the_real_node_measures_is_unknown_on_the_fake_side_never_zero(self) -> None:
        _, row, summary = self.runs.row()
        metrics = by_name(row)
        for name in ("node submitblock latency p50", "node submitblock latency p99",
                     "node getblocktemplate latency p99", "pool node peak RSS"):
            entry = metrics[name]
            self.assertIsNone(entry["fake"], name)
            self.assertIsNotNone(entry["real"], name)
            self.assertIsNone(entry["real_minus_fake"], name)
            self.assertTrue(entry["unavailable_reason"].startswith("fake: "), entry)
        self.assertEqual(metrics["node submitblock latency p99"]["real"], 30.0)
        self.assertEqual(metrics["pool node peak RSS"]["real"], 300_000)
        self.assertRegex(summary, r"\| node submitblock latency p50 \| unknown \| 12\.0 ms \| unknown \[\d+\] \|")
        self.assertIn(bridge.FAKE_HAS_NO_RELAY, summary)

    def test_a_real_zero_difference_stays_zero(self) -> None:
        self.runs.write(self.runs.real, report("real"))
        _, row, summary = self.runs.row()
        ack = by_name(row)["client_ack_latency p50 (warm_up)"]
        self.assertEqual(ack["real_minus_fake"], 0.0)
        self.assertIsNone(ack["unavailable_reason"])
        self.assertIn("| client_ack_latency p50 (warm_up) | 2.0 ms | 2.0 ms | 0.0 ms |", summary)

    def test_a_tip_some_session_missed_leaves_the_tip_statistics_unknown(self) -> None:
        self.runs.write(self.runs.real, report("real", tips=(90.0, None, 150.0)))
        _, row, _ = self.runs.row()
        for statistic in ("p50", "max"):
            entry = by_name(row)[f"time to usable work, slowest session, {statistic} over external tips"]
            self.assertIsNone(entry["real"])
            self.assertIsNone(entry["real_minus_fake"])
            self.assertIn("tip 1: 3 of 100 sessions got no usable work", entry["unavailable_reason"])

    def test_an_unmeasured_ack_latency_is_unknown_with_the_harness_reason(self) -> None:
        self.runs.write(self.runs.fake, report("fake", ack=(None, None)))
        _, row, _ = self.runs.row()
        entry = by_name(row)["client_ack_latency p99 (steady_state)"]
        self.assertIsNone(entry["fake"])
        self.assertIsNone(entry["real_minus_fake"])
        self.assertIn("no samples were recorded", entry["unavailable_reason"])

    def test_a_missing_side_still_writes_the_row_and_marks_it_untrusted(self) -> None:
        self.runs.write(self.runs.real, None, harness="6", gate="1")
        code, row, summary = self.runs.row(order="real,fake")
        self.assertEqual(code, 0)
        self.assertFalse(row["trusted"])
        self.assertIn("real: load-harness-report.json is missing", row["untrusted_reasons"])
        self.assertIn("real: harness_exit_code is 6", row["untrusted_reasons"])
        self.assertIn("real: gate_exit_code is 1", row["untrusted_reasons"])
        for entry in row["metrics"]:
            self.assertIsNone(entry["real"], entry["metric"])
            self.assertIsNone(entry["real_minus_fake"], entry["metric"])
        self.assertIn("**Not trusted:**", summary)
        self.assertIn("order real then fake", summary)

    def test_a_gate_that_left_no_exit_code_is_missing_not_a_pass(self) -> None:
        (self.runs.fake / "gate-exit-code").unlink()
        _, row, _ = self.runs.row()
        self.assertFalse(row["trusted"])
        self.assertIn("fake: gate_exit_code is missing", row["untrusted_reasons"])

    def test_another_schema_or_preset_is_refused_by_name(self) -> None:
        body = report("fake")
        body["schema"] = "qbit.prism.load-harness.v0"
        self.runs.write(self.runs.fake, body)
        _, row, _ = self.runs.row()
        self.assertIn(f"fake: load-harness-report.json has schema 'qbit.prism.load-harness.v0', not {bridge.REPORT_SCHEMA}",
                      row["untrusted_reasons"])
        swapped = copy.deepcopy(report("real"))
        self.runs.write(self.runs.fake, swapped)
        _, row, _ = self.runs.row()
        self.assertIn("fake: load-harness-report.json is from preset 'short-plan-real-node', not short-plan-fake-node",
                      row["untrusted_reasons"])

    def test_sides_on_different_runners_are_untrusted(self) -> None:
        self.runs.write(self.runs.real, report("real"), runner="blacksmith-16vcpu-ubuntu-2404")
        _, row, _ = self.runs.row()
        self.assertFalse(row["trusted"])
        self.assertIsNone(row["runner"])

    def test_an_order_other_than_the_two_sides_is_refused(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(bridge.main([
                "--fake", "f", "--real", "r", "--order", "fake,fake", "--out", str(self.runs.root / "x.json"),
            ]), 2)


class CheckedInPair(unittest.TestCase):
    """The row is only a node cost while the two presets differ only in --node."""

    def test_the_presets_differ_only_in_node(self) -> None:
        presets = matrix.load(matrix.PRESETS)
        fake, real = presets[bridge.PRESETS["fake"]], presets[bridge.PRESETS["real"]]
        self.assertEqual((fake["args"]["--node"], real["args"]["--node"]), ("fake", "qbitd"))
        differing = sorted(key for key in fake["args"] if fake["args"][key] != real["args"].get(key))
        self.assertEqual(differing, ["--node"])
        self.assertEqual(set(fake["args"]), set(real["args"]))
        for key in ("runner", "timeout_minutes", "gates"):
            self.assertEqual(fake[key], real[key], key)
        self.assertEqual(fake["args"]["--plan"], "short")


class Workflow(unittest.TestCase):
    """The bridging job in prism-load-nightly.yml runs both halves on one runner."""

    def setUp(self) -> None:
        text = (ROOT / ".github/workflows/prism-load-nightly.yml").read_text(encoding="utf-8")
        match = re.search(r"^  bridging:\n(.*?)(?=^  [a-z-]+:\n)", text, re.S | re.M)
        self.assertIsNotNone(match, "prism-load-nightly.yml has no bridging job")
        self.job = match.group(1)
        self.text = text

    def test_it_runs_both_presets_and_the_row_in_one_job(self) -> None:
        self.assertEqual(self.job.count(".github/scripts/prism-load-run.sh"), 1)
        for preset in bridge.PRESETS.values():
            self.assertIn(preset, self.job)
        self.assertIn("scripts/prism_load_bridge.py", self.job)
        # The real half needs the pinned qbitd, as the matrix's real-node job does.
        self.assertIn(".github/scripts/install-prism-qbit.sh", self.job)

    def test_each_half_runs_under_its_presets_own_timeout(self) -> None:
        self.assertIn('minutes="$(jq -er .timeout_minutes "crates/qbit-prism-load/presets/${preset[${side}]}.json")"', self.job)
        self.assertRegex(self.job, r'timeout --kill-after=\d+ "\$\{minutes\}m" \\\n\s+bash \.github/scripts/prism-load-run\.sh')

    def test_its_timeout_holds_both_presets_timeouts(self) -> None:
        presets = matrix.load(matrix.PRESETS)
        timeout = int(re.search(r"^    timeout-minutes: (\d+)$", self.job, re.M).group(1))
        both = sum(presets[name]["timeout_minutes"] for name in bridge.PRESETS.values())
        self.assertGreaterEqual(timeout, both + 10)
        self.assertLessEqual(timeout, 360)

    def test_the_runner_is_the_presets_runner(self) -> None:
        presets = matrix.load(matrix.PRESETS)
        runner = re.search(r"^    runs-on: (\S+)$", self.job, re.M).group(1)
        self.assertEqual(runner, presets[bridge.PRESETS["real"]]["runner"])
        # host.json records the label the job gives, so it must be this one.
        self.assertEqual(re.search(r"^          RUNNER_LABEL: (\S+)$", self.job, re.M).group(1), runner)

    def test_a_failed_bridging_run_reaches_the_failure_report(self) -> None:
        needs = re.search(r"^  report:\n(?:.*\n)*?    needs: \[(.*)\]", self.text, re.M).group(1)
        self.assertIn("bridging", [name.strip() for name in needs.split(",")])


if __name__ == "__main__":
    unittest.main()
