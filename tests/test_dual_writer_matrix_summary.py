#!/usr/bin/env python3
"""Tests for scripts/dual_writer_matrix_summary.py."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import dual_writer_matrix_summary as summary  # noqa: E402


def report(scenario: str, passed: bool, **extra: object) -> dict:
    return {
        "scenario": scenario,
        "passed": passed,
        "duration_ms": 95_000,
        "gaps": [{"fault_to_first_accept_ms": 3_400}],
        "shares": {"lost": 0, "excused_tail": 2},
        "expectations": [{"name": "miners moved to B", "passed": passed}],
        **extra,
    }


class MatrixSummary(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write(self, data: dict) -> None:
        path = self.root / str(data["scenario"]) / "report.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(data), encoding="utf-8")

    def test_each_scenario_is_a_row_with_its_verdict_gap_and_failures(self) -> None:
        self.write(report("s01-steady-state", True, gaps=[]))
        self.write(report("s02-a-dies-kill9", False, error="frontend A did not restart"))
        text = summary.render(summary.load_reports(self.root))
        self.assertIn("1 of 2 scenarios passed.", text)
        self.assertIn("| s01-steady-state | pass | 95.0 s | no fault | 0 | 2 |  |", text)
        self.assertIn(
            "| s02-a-dies-kill9 | **FAIL** | 95.0 s | 3.4 s | 0 | 2 | miners moved to B; did not complete |",
            text,
        )
        self.assertIn("- s02-a-dies-kill9: frontend A did not restart", text)

    def test_an_unreadable_report_is_a_failed_row_not_a_crash(self) -> None:
        path = self.root / "s04-link-cut" / "report.json"
        path.parent.mkdir(parents=True)
        path.write_text("{not json", encoding="utf-8")
        text = summary.render(summary.load_reports(self.root))
        self.assertIn("| s04-link-cut | **FAIL** |", text)
        self.assertIn("unreadable report", text)

    def test_no_report_is_an_error_and_out_writes_the_table(self) -> None:
        self.assertEqual(summary.main([str(self.root)]), 1)
        self.write(report("s10-single-writer", True))
        out = self.root / "matrix.md"
        self.assertEqual(summary.main([str(self.root), "--out", str(out)]), 0)
        self.assertIn("s10-single-writer", out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
