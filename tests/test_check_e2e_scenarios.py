#!/usr/bin/env python3
"""Tests for scripts/check_e2e_scenarios.py (#543)."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_e2e_scenarios.py"
sys.path.insert(0, str(ROOT / "scripts"))

import check_e2e_scenarios as scenarios  # noqa: E402


LIVE = "qbit-prism-server::live_regtest::two_node_tests::lost_race"
SMOKE = "qbit-prism-load::load_smoke::smoke_reconciles"
COMPONENT = "qbit-prism-server::ledger_postgres::alpha"
NIGHTLY = "qbit-prism-server::live_regtest::node_outage_tests::reindex"
UNIT = "crates/qbit-prism-server/tests/stratum_protocol.rs::oversized_frames_close"

LANES = """
schema = "qbit.e2e-scenarios.v1"

[lanes.pr]
title = "PR"
runs = true
workflow = ".github/workflows/ci.yml"

[lanes.nightly]
title = "Nightly"
runs = true
workflow = ".github/workflows/prism-load-nightly.yml"

[lanes.dispatch]
title = "Dispatch"
runs = true
workflow = ".github/workflows/prism-load-nightly.yml"

[lanes.L4]
title = "Real node under load"
runs = false
owner = "#553"
"""

SCENARIOS = f"""
[[scenario]]
id = "lost-race"
title = "Lost race"
owner = "#521"
lanes = ["pr"]
criteria = "Credited once."
runs = true
tests = ["{LIVE}"]

[[scenario]]
id = "smoke"
title = "Smoke"
owner = "#521"
lanes = ["pr"]
criteria = "Reconciles."
runs = true
tests = ["{SMOKE}"]
presets = ["smoke"]

[[scenario]]
id = "reindex"
title = "Reindex"
owner = "#521"
lanes = ["nightly"]
criteria = "Lands once."
runs = true
tests = ["{NIGHTLY}"]

[[scenario]]
id = "d1"
title = "D1"
owner = "#521"
lanes = ["nightly"]
criteria = "Gates."
runs = true
presets = ["d1-20k"]

[[scenario]]
id = "cells"
title = "Cells"
owner = "#521"
lanes = ["dispatch"]
criteria = "Gates."
runs = true
presets = ["d1-500k"]

[[scenario]]
id = "frames"
title = "Frames"
owner = "#468"
lanes = ["pr"]
criteria = "Closes."
runs = true
unit_tests = ["{UNIT}"]

[[scenario]]
id = "under-load"
title = "Under load"
owner = "#553"
lanes = ["L4"]
criteria = "Set by #553."
runs = false
reason = "Needs L4."
"""

RUST = """\
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_frames_close() {
}

#[test]
#[ignore = "opt-in"]
fn ignored_case() {}

fn helper_not_a_test() {}
"""


class Fixture:
    """A throwaway repository root holding just what the check reads."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.write("test/prism-gated-tests.txt", "\n".join(sorted([LIVE, SMOKE, COMPONENT])) + "\n")
        self.write("test/prism-nightly-gated-tests.txt", f"# opt-in\n{NIGHTLY}\n")
        for name, schedule in (("d1-20k", "nightly"), ("d1-500k", "manual"), ("smoke", "smoke")):
            self.write(
                f"crates/qbit-prism-load/presets/{name}.json",
                json.dumps({"schema": "qbit.prism.load-preset.v1", "name": name, "schedule": schedule}),
            )
        self.write("crates/qbit-prism-server/tests/stratum_protocol.rs", RUST)
        self.write(
            ".github/workflows/ci.yml",
            "run: python3 scripts/run_rust_test_shard.py\n--expected test/prism-gated-tests.txt\n",
        )
        self.write(
            ".github/workflows/prism-load-nightly.yml",
            "on:\n  schedule:\n    - cron: x\n  workflow_dispatch:\n"
            "scripts/prism_load_matrix.py\n--expected test/prism-nightly-gated-tests.txt\n",
        )
        self.manifest = LANES + SCENARIOS

    def write(self, relative: str, text: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def problems(self) -> list[str]:
        import tomllib

        return scenarios.check(
            tomllib.loads(self.manifest), scenarios.Lanes(self.root), self.root
        )


class CheckE2eScenarios(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.fixture = Fixture(Path(self.tmp.name))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def assertProblem(self, needle: str) -> None:
        problems = self.fixture.problems()
        self.assertTrue(any(needle in p for p in problems), problems)

    def replace(self, old: str, new: str) -> None:
        self.assertIn(old, self.fixture.manifest)
        self.fixture.manifest = self.fixture.manifest.replace(old, new, 1)

    def test_a_manifest_that_matches_the_lanes_passes(self) -> None:
        self.assertEqual(self.fixture.problems(), [])

    def test_a_scenario_test_the_manifest_does_not_name_fails(self) -> None:
        added = "qbit-prism-server::live_regtest::zz_new_scenario"
        self.fixture.write(
            "test/prism-gated-tests.txt",
            "\n".join(sorted([LIVE, SMOKE, COMPONENT, added])) + "\n",
        )
        self.assertProblem(f"pr runs {added}, which no running scenario names")

    def test_component_gated_tests_need_no_entry(self) -> None:
        self.assertFalse(any(COMPONENT in p for p in self.fixture.problems()))

    def test_an_unnamed_nightly_test_or_preset_fails(self) -> None:
        self.fixture.write(
            "crates/qbit-prism-load/presets/growth.json",
            json.dumps({"schema": "qbit.prism.load-preset.v1", "name": "growth", "schedule": "nightly"}),
        )
        self.assertProblem("nightly runs preset growth, which no running scenario names")
        self.replace(f'tests = ["{NIGHTLY}"]', f'tests = ["{LIVE}"]')
        self.assertProblem(f"nightly runs {NIGHTLY}, which no running scenario names")

    def test_a_claim_no_lane_runs_fails(self) -> None:
        self.replace(f'tests = ["{LIVE}"]', 'tests = ["qbit-prism-server::live_regtest::gone"]')
        self.assertProblem("is in neither")
        self.replace('presets = ["d1-20k"]', 'presets = ["no-such-preset"]')
        self.assertProblem("preset 'no-such-preset' is unknown or runs in no lane")

    def test_declared_lanes_must_match_where_the_evidence_runs(self) -> None:
        self.replace('lanes = ["nightly"]\ncriteria = "Gates."', 'lanes = ["pr"]\ncriteria = "Gates."')
        self.assertProblem("claims lane 'pr', but none of its evidence runs there")
        self.assertProblem("its evidence runs in 'nightly'; add it to lanes")

    def test_a_running_scenario_on_a_lane_that_does_not_run_fails(self) -> None:
        self.replace('lanes = ["pr"]\ncriteria = "Closes."', 'lanes = ["pr", "L4"]\ncriteria = "Closes."')
        self.assertProblem("lane 'L4' is marked as not running")

    def test_unit_tests_must_exist_be_tests_and_not_be_ignored(self) -> None:
        path = "crates/qbit-prism-server/tests/stratum_protocol.rs"
        for name, message in (
            ("ignored_case", "is #[ignore]d"),
            ("helper_not_a_test", "is not a #[test]"),
            ("missing", "defines no fn missing"),
        ):
            with self.subTest(name=name):
                self.fixture.manifest = LANES + SCENARIOS.replace(UNIT, f"{path}::{name}")
                self.assertProblem(message)
        self.fixture.manifest = LANES + SCENARIOS.replace(UNIT, "crates/nope.rs::x")
        self.assertProblem("crates/nope.rs does not exist")

    def test_an_unexercised_scenario_needs_a_reason_and_no_evidence(self) -> None:
        self.replace('reason = "Needs L4."', "")
        self.assertProblem("scenario under-load is unexercised and gives no reason")
        self.fixture.manifest = LANES + SCENARIOS.replace(
            'reason = "Needs L4."', f'reason = "x"\ntests = ["{LIVE}"]'
        )
        self.assertProblem("is unexercised but cites evidence")

    def test_a_running_scenario_needs_evidence(self) -> None:
        self.replace(f'tests = ["{NIGHTLY}"]', "")
        self.assertProblem("scenario reindex runs but cites no tests, presets or unit_tests")

    def test_malformed_entries_are_refused(self) -> None:
        cases = (
            ('owner = "#468"', 'owner = "468"', "owner '468' is not #<issue>"),
            ('id = "frames"', 'id = "lost-race"', "scenario id 'lost-race' appears 2 times"),
            ('criteria = "Closes."', 'criteria = " "', "criteria is missing or empty"),
            ('criteria = "Closes."', 'criteria = "Closes."\ntest = "typo"', "unknown key 'test'"),
            ('lanes = ["L4"]', 'lanes = ["L9"]', "lane 'L9' is not defined"),
            ('lanes = ["L4"]', "lanes = []", "lanes must be a non-empty list"),
            ('runs = false', 'runs = "no"', "runs must be true or false"),
            ('schema = "qbit.e2e-scenarios.v1"', 'schema = "v0"', "schema is 'v0'"),
        )
        for old, new, message in cases:
            with self.subTest(message=message):
                self.fixture.manifest = LANES + SCENARIOS
                self.replace(old, new)
                self.assertProblem(message)

    def test_a_lane_marked_running_needs_its_wiring(self) -> None:
        self.fixture.write(".github/workflows/prism-load-nightly.yml", "on:\n  workflow_dispatch:\n")
        self.assertProblem("prism-load-nightly.yml no longer contains 'cron:'")
        self.fixture.manifest = self.fixture.manifest.replace(
            'runs = false\nowner = "#553"', "runs = true"
        )
        self.assertProblem("lane L4 is marked running, but this check cannot enumerate it")

    def test_a_lane_not_running_names_its_owner(self) -> None:
        self.replace('runs = false\nowner = "#553"', "runs = false")
        self.assertProblem("lane L4 does not run yet and names no owner issue")

    def test_a_weekly_preset_runs_in_the_weekly_lane_only_while_it_is_wired(self) -> None:
        self.fixture.write(
            "crates/qbit-prism-load/presets/soak.json",
            json.dumps({"schema": "qbit.prism.load-preset.v1", "name": "soak", "schedule": "weekly"}),
        )
        self.assertProblem("weekly runs preset soak, which no running scenario names")
        self.fixture.manifest += (
            '\n[lanes.weekly]\ntitle = "Weekly soak"\nruns = true\n'
            'workflow = ".github/workflows/prism-load-nightly.yml"\n'
            '\n[[scenario]]\nid = "soak"\ntitle = "Soak"\nowner = "#575"\nlanes = ["weekly"]\n'
            'criteria = "Its gates."\nruns = true\npresets = ["soak"]\n'
        )
        self.assertProblem("lane weekly: .github/workflows/prism-load-nightly.yml no longer contains")
        workflow = self.fixture.root / ".github/workflows/prism-load-nightly.yml"
        workflow.write_text(
            workflow.read_text(encoding="utf-8")
            + "    - cron: \"41 5 * * 6\"\n"
            + "SELECTION: ${{ inputs.preset || (github.event.schedule == '41 5 * * 6' && 'weekly') }}\n",
            encoding="utf-8",
        )
        self.assertEqual(self.fixture.problems(), [])

    def test_the_smoke_preset_runs_in_pr_only_through_the_load_smoke_test(self) -> None:
        self.fixture.write("test/prism-gated-tests.txt", "\n".join(sorted([LIVE, COMPONENT])) + "\n")
        self.assertProblem("preset 'smoke' is unknown or runs in no lane")


class CheckedInManifest(unittest.TestCase):
    # This is how the check runs in CI: the required python-tests shards run
    # every module under tests/, so a manifest that disagrees with the lanes
    # fails the PR.
    def test_the_checked_in_manifest_passes(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPT)], cwd=ROOT, text=True, capture_output=True, check=False
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("each claim holds", result.stdout)

    def test_every_unexercised_scenario_links_an_issue_and_gives_a_reason(self) -> None:
        import tomllib

        manifest = tomllib.loads(scenarios.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
        for scenario in manifest["scenario"]:
            if scenario["runs"]:
                continue
            with self.subTest(id=scenario["id"]):
                self.assertRegex(scenario["owner"], r"^#[1-9][0-9]*$")
                self.assertTrue(scenario["reason"].strip())


if __name__ == "__main__":
    unittest.main()
