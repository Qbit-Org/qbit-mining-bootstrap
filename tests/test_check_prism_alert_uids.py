"""Regression cases for the published alert rule UID check (#757)."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from scripts.check_prism_alert_uids import GRAFANA_UID, check


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_prism_alert_uids.py"
NATIVE = "docs/prism-native-alert-rules.json"
POSTGRES = "docs/prism-postgres-alert-rules.json"


def run_check(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root)],
        cwd=ROOT, text=True, capture_output=True, check=False,
    )


class CheckPrismAlertUidsTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "docs").mkdir()
        self.write(NATIVE, "qbit-prism-a", "qbit-prism-b")
        self.write(POSTGRES, "qbit-prism-postgres-a")

    def write(self, name: str, *uids: object) -> None:
        rules = [{"title": f"Rule{index}", "uid": uid} for index, uid in enumerate(uids)]
        (self.root / name).write_text(json.dumps({"rules": rules}), encoding="utf-8")

    def test_valid_unique_uids_pass(self) -> None:
        self.assertEqual(check(self.root), ([], 3))
        result = run_check(self.root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("3 rule UIDs", result.stdout)

    def test_a_uid_over_forty_characters_fails_with_its_rule_and_length(self) -> None:
        self.write(NATIVE, "a" * 40, "qbit-prism-pool-resubmission-reject-ratio")
        problems, _ = check(self.root)
        self.assertEqual(problems, [
            f"{NATIVE}: rules[1] (Rule1): uid 'qbit-prism-pool-resubmission-reject-ratio' "
            "is 41 characters; Grafana allows at most 40"])
        result = run_check(self.root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("rules[1] (Rule1)", result.stderr)

    def test_characters_outside_grafanas_set_and_empty_uids_fail(self) -> None:
        bad = ["qbit.prism", "qbit prism", "qbit/prism", "qbit-prism\n", "", "qbit-prísm"]
        self.write(NATIVE, *bad)
        problems, _ = check(self.root)
        self.assertEqual(len(problems), len(bad), problems)
        for index, problem in enumerate(problems):
            self.assertIn(f"rules[{index}]", problem)
            self.assertIn("must be non-empty and use only [A-Za-z0-9_-]", problem)

    def test_a_missing_or_non_string_uid_fails(self) -> None:
        self.write(NATIVE, None, 7)
        problems, count = check(self.root)
        self.assertEqual(count, 1)
        self.assertEqual(problems, [
            f"{NATIVE}: rules[0] (Rule0): uid must be a string, got None",
            f"{NATIVE}: rules[1] (Rule1): uid must be a string, got 7"])

    def test_duplicates_fail_within_a_file_and_across_files(self) -> None:
        self.write(NATIVE, "qbit-prism-a", "qbit-prism-a", "qbit-prism-postgres-a")
        problems, _ = check(self.root)
        self.assertEqual(problems, [
            f"{NATIVE}: rules[1] (Rule1): uid 'qbit-prism-a' is also used by {NATIVE}: rules[0] (Rule0)",
            f"{POSTGRES}: rules[0] (Rule0): uid 'qbit-prism-postgres-a' is also used by "
            f"{NATIVE}: rules[2] (Rule2)"])

    def test_a_newly_published_rules_file_is_checked(self) -> None:
        self.write("docs/prism-other-alert-rules.json", "x" * 41, "qbit-prism-b")
        problems, count = check(self.root)
        self.assertEqual(count, 5)
        self.assertEqual(len(problems), 2, problems)
        self.assertTrue(all(problem.startswith("docs/prism-other-alert-rules.json:") for problem in problems))

    def test_a_missing_or_malformed_published_file_fails(self) -> None:
        (self.root / POSTGRES).unlink()
        self.assertEqual(check(self.root)[0], [f"{POSTGRES} is missing"])
        (self.root / POSTGRES).write_text("[]", encoding="utf-8")
        self.assertIn("not a", check(self.root)[0][0])
        (self.root / POSTGRES).write_text('{"rules": []}', encoding="utf-8")
        self.assertEqual(check(self.root)[0], [f'{POSTGRES}: "rules" must be a non-empty list'])

    def test_the_rendered_file_pattern_is_the_same_contract(self) -> None:
        for uid in ["a", "A_9-z", "x" * 40]:
            self.assertTrue(GRAFANA_UID.fullmatch(uid), uid)
        for uid in ["", "x" * 41, "a.b", "a\n"]:
            self.assertFalse(GRAFANA_UID.fullmatch(uid), uid)

    def test_the_checked_in_rules_pass(self) -> None:
        result = run_check(ROOT)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_ci_runs_the_check(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        self.assertIn("python3 scripts/check_prism_alert_uids.py", workflow)


if __name__ == "__main__":
    unittest.main()
