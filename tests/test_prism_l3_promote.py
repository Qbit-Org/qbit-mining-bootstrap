"""Tests for scripts/prism_l3_promote.py (#550): the tag's promote-or-rerun."""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prism_l3_promote as promote  # noqa: E402

REPO = "Qbit-Org/qbit-mining-bootstrap"
TREE = "a" * 40
OTHER_TREE = "b" * 40
THIS_RUN = 900


def verdict(**overrides) -> dict:
    value = {
        "schema": promote.VERDICT_SCHEMA,
        "suite": "l3-full",
        "tree": TREE,
        "complete": True,
        "passed": True,
    }
    value.update(overrides)
    return value


def run(run_id: int, **overrides) -> dict:
    value = {
        "id": run_id,
        "event": "pull_request",
        "path": promote.WORKFLOW_PATH,
        "status": "completed",
        "conclusion": "success",
        "head_repository": {"full_name": REPO},
        "html_url": f"https://github.com/{REPO}/actions/runs/{run_id}",
        "pull_requests": [{"number": 600}],
    }
    value.update(overrides)
    return value


class FakeGh:
    """Answers the three calls the script makes, from canned runs."""

    def __init__(self, runs: list[tuple[dict, dict | None, str]], fail_listing: bool = False):
        # (run, verdict or None for an unreadable one, artifact created_at)
        self.runs = {r["id"]: (r, v) for r, v, _ in runs}
        self.created = {r["id"]: created for r, _, created in runs}
        self.fail_listing = fail_listing
        self.downloads: list[int] = []

    def __call__(self, args: list[str]) -> str:
        if args[0] == "api" and "actions/artifacts" in args[1]:
            if self.fail_listing:
                raise subprocess.CalledProcessError(1, ["gh", *args], stderr="HTTP 502")
            self.listing_query = args[1]
            return json.dumps({"artifacts": [
                {"workflow_run": {"id": run_id}, "expired": False, "created_at": created}
                for run_id, created in self.created.items()
            ]})
        if args[0] == "api" and "actions/runs/" in args[1]:
            return json.dumps(self.runs[int(args[1].rsplit("/", 1)[1])][0])
        if args[:2] == ["run", "download"]:
            run_id = int(args[2])
            self.downloads.append(run_id)
            found = self.runs[run_id][1]
            if found is None:
                raise subprocess.CalledProcessError(1, ["gh", *args], stderr="no artifact")
            directory = Path(args[args.index("-D") + 1])
            (directory / "verdict.json").write_text(json.dumps(found), encoding="utf-8")
            return ""
        raise AssertionError(f"unexpected gh call {args}")


class Decide(unittest.TestCase):
    def test_a_matching_complete_pull_request_run_is_promoted(self) -> None:
        gh = FakeGh([(run(10), verdict(), "2026-09-30T10:00:00Z")])
        decision = promote.decide(gh, REPO, TREE, THIS_RUN)
        self.assertTrue(decision["promote"], decision)
        self.assertEqual(decision["source_run"], 10)
        self.assertEqual(decision["source_pull_requests"], [600])
        # Only the artifact named for this tree is listed.
        self.assertIn(f"name=prism-l3-verdict-{TREE}", gh.listing_query)

    def test_no_run_on_this_tree_reruns(self) -> None:
        decision = promote.decide(FakeGh([]), REPO, TREE, THIS_RUN)
        self.assertFalse(decision["promote"])
        self.assertTrue(decision["reason"].startswith("rerun:"))

    def test_a_verdict_for_another_tree_is_not_promoted(self) -> None:
        gh = FakeGh([(run(10), verdict(tree=OTHER_TREE), "2026-09-30T10:00:00Z")])
        decision = promote.decide(gh, REPO, TREE, THIS_RUN)
        self.assertFalse(decision["promote"])
        self.assertIn("tree", decision["considered"][0]["rejected"])

    def test_the_newest_eligible_run_wins_and_ineligible_ones_are_recorded(self) -> None:
        gh = FakeGh([
            (run(10), verdict(passed=False), "2026-09-29T10:00:00Z"),
            (run(11), verdict(), "2026-09-30T10:00:00Z"),
            (run(12, head_repository={"full_name": "someone/fork"}), verdict(), "2026-09-30T11:00:00Z"),
            (run(13, event="workflow_dispatch"), verdict(), "2026-09-30T12:00:00Z"),
            (run(14, conclusion="cancelled"), verdict(), "2026-09-30T13:00:00Z"),
            (run(15), verdict(complete=False), "2026-09-30T14:00:00Z"),
            (run(16), verdict(suite="l3-reduced"), "2026-09-30T15:00:00Z"),
            (run(17), None, "2026-09-30T16:00:00Z"),
            (run(18, path=".github/workflows/prism-load-nightly.yml"), verdict(), "2026-09-30T17:00:00Z"),
        ])
        decision = promote.decide(gh, REPO, TREE, THIS_RUN)
        self.assertTrue(decision["promote"], decision)
        self.assertEqual(decision["source_run"], 11)
        rejected = {entry["run"]: entry["rejected"] for entry in decision["considered"]}
        self.assertEqual(sorted(rejected), [12, 13, 14, 15, 16, 17, 18])
        self.assertIn("fork", rejected[12])
        self.assertIn("workflow_dispatch", rejected[13])
        self.assertIn("cancelled", rejected[14])
        self.assertIn("every planned run", rejected[15])
        self.assertIn("l3-reduced", rejected[16])
        self.assertIn("could not be read", rejected[17])
        self.assertIn("prism-load-nightly", rejected[18])
        # Runs that fail on their metadata are never downloaded.
        self.assertNotIn(12, gh.downloads)
        self.assertNotIn(10, gh.downloads)

    def test_a_complete_run_that_failed_a_gate_is_promoted_with_its_verdict(self) -> None:
        gh = FakeGh([(run(10, conclusion="failure"), verdict(passed=False), "2026-09-30T10:00:00Z")])
        decision = promote.decide(gh, REPO, TREE, THIS_RUN)
        self.assertTrue(decision["promote"])
        self.assertFalse(decision["verdict"]["passed"])
        self.assertIn("not every run passed", decision["reason"])

    def test_this_run_is_never_its_own_source(self) -> None:
        gh = FakeGh([(run(THIS_RUN), verdict(), "2026-09-30T10:00:00Z")])
        self.assertFalse(promote.decide(gh, REPO, TREE, THIS_RUN)["promote"])

    def test_a_failed_lookup_reruns_rather_than_promotes(self) -> None:
        decision = promote.decide(FakeGh([], fail_listing=True), REPO, TREE, THIS_RUN)
        self.assertFalse(decision["promote"])
        self.assertIn("lookup failed", decision["reason"])

    def test_an_artifact_naming_no_run_is_skipped_not_a_crash(self) -> None:
        gh = FakeGh([(run(10), verdict(), "2026-09-30T10:00:00Z")])
        listing = gh.__call__

        def with_orphan(args: list[str]) -> str:
            out = listing(args)
            if args[0] == "api" and "actions/artifacts" in args[1]:
                body = json.loads(out)
                body["artifacts"].insert(0, {"workflow_run": None, "expired": False, "created_at": "2026-10-01T00:00:00Z"})
                body["artifacts"].insert(0, {"expired": False, "created_at": "2026-10-01T00:00:00Z"})
                out = json.dumps(body)
            return out

        decision = promote.decide(with_orphan, REPO, TREE, THIS_RUN)
        self.assertTrue(decision["promote"], decision)
        self.assertEqual(decision["source_run"], 10)

    def test_a_verdict_without_a_pass_flag_is_not_promoted(self) -> None:
        broken = verdict()
        del broken["passed"]
        gh = FakeGh([(run(10), broken, "2026-09-30T10:00:00Z")])
        self.assertFalse(promote.decide(gh, REPO, TREE, THIS_RUN)["promote"])


class Arguments(unittest.TestCase):
    def test_a_tree_that_is_not_a_full_id_is_refused(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                promote.main(["--repo", REPO, "--run-id", "1"])
            self.assertEqual(promote.main(["--repo", REPO, "--tree", "HEAD", "--run-id", "1"]), 2)


if __name__ == "__main__":
    unittest.main()
