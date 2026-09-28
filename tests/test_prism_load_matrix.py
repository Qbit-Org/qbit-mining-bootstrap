"""Tests for scripts/prism_load_matrix.py (#521)."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prism_load_matrix as matrix  # noqa: E402


class CheckedInPresets(unittest.TestCase):
    def setUp(self) -> None:
        self.presets = matrix.load(matrix.PRESETS)

    def test_the_schedule_runs_the_nightly_presets_and_not_the_manual_or_smoke_ones(self) -> None:
        names = [entry["preset"] for entry in matrix.select(self.presets, "nightly")]
        for name in ("d1-20k", "tip-275", "mainnet-floor", "growth-5x"):
            self.assertIn(name, names)
        self.assertNotIn("growth-20x", names)
        self.assertNotIn("smoke", names)

    def test_all_adds_the_manual_presets(self) -> None:
        names = [entry["preset"] for entry in matrix.select(self.presets, "all")]
        self.assertIn("growth-20x", names)
        self.assertNotIn("smoke", names)

    def test_a_named_preset_runs_on_its_own_runner_for_its_own_timeout(self) -> None:
        [entry] = matrix.select(self.presets, "growth-20x")
        self.assertEqual(entry["runner"], self.presets["growth-20x"]["runner"])
        self.assertEqual(entry["timeout_minutes"], self.presets["growth-20x"]["timeout_minutes"])
        self.assertTrue(entry["runner"].startswith("blacksmith-"))

    def test_unknown_and_empty_selections_are_refused(self) -> None:
        with self.assertRaises(matrix.SelectionError):
            matrix.select(self.presets, "no-such-preset")
        with self.assertRaises(matrix.SelectionError):
            matrix.select(self.presets, " , ")

    def test_every_runner_label_is_known_to_actionlint(self) -> None:
        config = (ROOT / ".github" / "actionlint.yaml").read_text(encoding="utf-8")
        for preset in self.presets.values():
            self.assertIn(f"- {preset['runner']}", config, preset["name"])


class Malformed(unittest.TestCase):
    def test_a_preset_whose_name_is_not_its_file_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "a.json").write_text(
                json.dumps({"schema": matrix.SCHEMA, "name": "b"}), encoding="utf-8"
            )
            with self.assertRaises(matrix.SelectionError):
                matrix.load(Path(directory))


if __name__ == "__main__":
    unittest.main()
