"""Tests for scripts/prism_load_matrix.py (#521)."""

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

import prism_load_matrix as matrix  # noqa: E402


class CheckedInPresets(unittest.TestCase):
    def setUp(self) -> None:
        self.presets = matrix.load(matrix.PRESETS)

    def test_the_schedule_runs_the_nightly_presets_and_not_the_manual_or_smoke_ones(self) -> None:
        names = [entry["preset"] for entry in matrix.select(self.presets, "nightly")]
        for name in ("throughput-20k-window-1fe", "tip-delivery-2000-miners-400k-2fe-retarget", "mainnet-shape-130-addresses", "mainnet-shape-650-addresses"):
            self.assertIn(name, names)
        self.assertNotIn("mainnet-shape-2600-addresses", names)
        self.assertNotIn("pr-smoke", names)

    def test_the_weekly_schedule_runs_only_the_long_soak(self) -> None:
        entries = matrix.select(self.presets, "weekly")
        self.assertEqual([entry["preset"] for entry in entries], ["soak-weekly"])
        # Under the six-hour job limit, with the soak's own minutes inside it.
        [entry] = entries
        self.assertLess(entry["timeout_minutes"], 360)
        self.assertLess(self.presets["soak-weekly"]["soak"]["minutes"], entry["timeout_minutes"])
        nightly = [entry["preset"] for entry in matrix.select(self.presets, "nightly")]
        self.assertNotIn("soak-weekly", nightly)
        self.assertNotIn("soak-weekly", [entry["preset"] for entry in matrix.select(self.presets, "all")])

    def test_all_adds_the_manual_presets(self) -> None:
        names = [entry["preset"] for entry in matrix.select(self.presets, "all")]
        self.assertIn("mainnet-shape-2600-addresses", names)
        self.assertNotIn("pr-smoke", names)

    def test_a_named_preset_runs_on_its_own_runner_for_its_own_timeout(self) -> None:
        [entry] = matrix.select(self.presets, "mainnet-shape-2600-addresses")
        self.assertEqual(entry["runner"], self.presets["mainnet-shape-2600-addresses"]["runner"])
        self.assertEqual(entry["timeout_minutes"], self.presets["mainnet-shape-2600-addresses"]["timeout_minutes"])
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


class Aliases(unittest.TestCase):
    def test_every_deprecated_name_selects_the_preset_it_was_renamed_to(self) -> None:
        presets = matrix.load(matrix.PRESETS)
        aliases = matrix.load_aliases(matrix.PRESETS)
        self.assertEqual(aliases["d1-20k"], "throughput-20k-window-1fe")
        self.assertEqual(aliases["smoke"], "pr-smoke")
        for old, new in aliases.items():
            self.assertIn(new, presets, old)
            self.assertNotIn(old, presets, old)
            [entry] = matrix.select(presets, old, aliases)
            self.assertEqual(entry["preset"], new)

    def test_without_the_alias_file_an_old_name_is_unknown(self) -> None:
        with self.assertRaises(matrix.SelectionError):
            matrix.select(matrix.load(matrix.PRESETS), "tip-275", {})


class Malformed(unittest.TestCase):
    def test_a_preset_whose_name_is_not_its_file_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "a.json").write_text(
                json.dumps({"schema": matrix.SCHEMA, "name": "b"}), encoding="utf-8"
            )
            with self.assertRaises(matrix.SelectionError):
                matrix.load(Path(directory))


class Suites(unittest.TestCase):
    """The checked-in suites (#550) and what a malformed one is refused for."""

    def setUp(self) -> None:
        self.presets = matrix.load(matrix.PRESETS)
        self.suites = matrix.load_suites(matrix.PRESETS, self.presets)

    def test_the_full_l3_runs_every_473_cell_three_times_on_one_runner_class(self) -> None:
        entries = matrix.select(self.presets, "suite:l3-full", suites=self.suites)
        names = sorted({entry["preset"] for entry in entries})
        self.assertEqual(len(names), 11)
        for name in (
            "throughput-200k-window-1fe-async",
            "throughput-400k-window-1fe-async",
            "throughput-400k-window-2fe-sync",
            "throughput-400k-window-2fe-async-3-blocks",
            "dense-cadence-400k-window-1fe-async",
            "dense-cadence-400k-window-2fe-async",
            "throughput-500k-window-4fe-async",
        ):
            self.assertIn(name, names)
        self.assertEqual(len(entries), 33)
        self.assertEqual({entry["runner"] for entry in entries}, {"blacksmith-32vcpu-ubuntu-2404"})
        self.assertEqual(
            sorted(entry["id"] for entry in entries if entry["preset"] == names[0]),
            [f"{names[0]}-r1", f"{names[0]}-r2", f"{names[0]}-r3"],
        )
        # One job per repeat: every id is distinct.
        self.assertEqual(len({entry["id"] for entry in entries}), 33)
        for entry in entries:
            self.assertEqual(entry["timeout_minutes"], self.presets[entry["preset"]]["timeout_minutes"])

    def test_the_reduced_l3_is_400k_at_1_2_and_4_frontends_async_once(self) -> None:
        entries = matrix.select(self.presets, "suite:l3-reduced", suites=self.suites)
        names = [entry["preset"] for entry in entries]
        for fe in (1, 2, 4):
            self.assertIn(f"throughput-400k-window-{fe}fe-async", names)
        self.assertEqual({entry["repeat"] for entry in entries}, {1})

    def test_a_suite_preset_can_keep_its_own_schedule(self) -> None:
        # throughput-400k-window-1fe-async is nightly and in both L3 suites.
        self.assertEqual(self.presets["throughput-400k-window-1fe-async"]["schedule"], "nightly")
        nightly = [entry["preset"] for entry in matrix.select(self.presets, "nightly")]
        self.assertIn("throughput-400k-window-1fe-async", nightly)
        self.assertNotIn("repeat", matrix.select(self.presets, "nightly")[0])

    def test_every_suite_runner_is_known_to_actionlint(self) -> None:
        config = (ROOT / ".github" / "actionlint.yaml").read_text(encoding="utf-8")
        for name, suite in self.suites.items():
            if "runner" in suite:
                self.assertIn(f"- {suite['runner']}", config, name)

    def test_an_unknown_suite_is_refused(self) -> None:
        with self.assertRaises(matrix.SelectionError):
            matrix.select(self.presets, "suite:l3-nope", suites=self.suites)

    def test_a_malformed_suite_is_refused_and_only_a_suite_selection_reads_the_file(self) -> None:
        good = (
            'schema = "qbit.prism.load-suites.v1"\n[suites.s]\ndescription = "d"\nlane = "L3"\n'
            'issues = ["#550"]\nrepeats = 2\npresets = ["pr-smoke"]\n'
        )
        cases = {
            "schema": good.replace("load-suites.v1", "load-suites.v0"),
            "unknown key": good + "extra = 1\n",
            "missing key": good.replace('lane = "L3"\n', ""),
            "unknown preset": good.replace('["pr-smoke"]', '["no-such-preset"]'),
            "alias": good.replace('["pr-smoke"]', '["smoke"]'),
            "repeated preset": good.replace('["pr-smoke"]', '["pr-smoke", "pr-smoke"]'),
            "zero repeats": good.replace("repeats = 2", "repeats = 0"),
            "too many repeats": good.replace("repeats = 2", "repeats = 6"),
            "boolean repeats": good.replace("repeats = 2", "repeats = true"),
            "runner": good + 'runner = "ubuntu-latest"\n',
            "issue": good.replace('["#550"]', '["550"]'),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for preset in matrix.PRESETS.glob("*.json"):
                (root / preset.name).write_bytes(preset.read_bytes())
            (root / "suites.toml").write_text(good, encoding="utf-8")
            presets = matrix.load(root)
            self.assertEqual(len(matrix.select(presets, "suite:s", suites=matrix.load_suites(root, presets))), 2)
            for name, text in cases.items():
                with self.subTest(case=name):
                    (root / "suites.toml").write_text(text, encoding="utf-8")
                    with self.assertRaises(matrix.SelectionError):
                        matrix.load_suites(root, presets)
            # A broken suites file never stops the nightly's plan.
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(matrix.main(["nightly", "--presets", str(root)]), 0)
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(matrix.main(["suite:s", "--presets", str(root)]), 2)


if __name__ == "__main__":
    unittest.main()
