"""Tests for scripts/tree_pass_marker.py: skip CI when the tree under test already passed.

The decision table (same tree skips, a different tree runs, every lookup
failure runs, never on push or tags), the binding of a marker to the run that
saved it, the fail marker, the aggregate gate's jq rule, when a marker may be
recorded, and the wiring in ci.yml and prism-load-l3.yml that the decisions
rely on (docs/ci-same-tree-skip.md).
"""

from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import tree_pass_marker as marker  # noqa: E402

REPO = "Qbit-Org/qbit-mining-bootstrap"
CI_PATH = ".github/workflows/ci.yml"
L3_PATH = ".github/workflows/prism-load-l3.yml"
CI_REF = f"{REPO}/{CI_PATH}@refs/pull/840/merge"
L3_REF = f"{REPO}/{L3_PATH}@refs/pull/840/merge"
TREE = "a" * 40
OTHER_TREE = "b" * 40
SHA = "c" * 40
OTHER_SHA = "d" * 40
HEAD_SHA = "e" * 40
HEAD_TREE = "f" * 40
HEAD_REF = "kiwidream/ci-skip-same-tree"
PR = 840
WRITER = 500
CURRENT = 900
SAVE_JOB = "Record this tree's pass marker"
GATE_JOB = "Lint, compile, and test"
# The job's name as GitHub reads it. Its line goes on " #473's tables", which YAML takes as a comment.
COLLATE_JOB = "Collate the suite into"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
L3_WORKFLOW = ROOT / ".github" / "workflows" / "prism-load-l3.yml"
MARKED = "needs.tree-marker.result == 'success' && needs.tree-marker.outputs.skip == 'true'"
RUN = f"!({MARKED})"


def prefix(tree: str = TREE, salt: str = "1", workflow: str = "ci.yml") -> str:
    return marker.marker_prefix(workflow, salt, tree)


def fail(tree: str = TREE, salt: str = "1", workflow: str = "ci.yml") -> str:
    return marker.fail_key(workflow, salt, tree)


def written(run: int = WRITER, attempt: int = 1, at: str | None = None) -> str:
    return marker.marker_key(at or prefix(), str(run), str(attempt))


def job(name: str, conclusion: str = "success", status: str = "completed") -> dict:
    return {"name": name, "status": status, "conclusion": conclusion}


class FakeGitHub:
    """The REST calls decide makes, answered from canned records."""

    def __init__(self) -> None:
        self.runs: dict[int, dict] = {}
        self.commits: dict[str, str] = {HEAD_SHA: HEAD_TREE}
        self.artifacts: dict[int, list[dict]] = {}
        self.attempt_jobs: dict[tuple[int, int], list[dict]] = {}
        self.latest_jobs: dict[int, list[dict]] = {}
        self.pulls: list[dict] = [pull(PR)]
        self.timeline: dict[int, list[dict]] = {PR: [{"event": "labeled"}, {"event": "committed"}]}
        self.events: dict[int, list[dict]] = {PR: [{"event": "labeled"}]}
        self.caches: list[dict] = []
        self.fail_records: list[dict] = []
        self.failing: list[str] = []
        self.calls: list[str] = []

    def add_run(self, run_id: int = WRITER, attempt: int = 1, *, key: str | None = None,
                head_tree: str = HEAD_TREE, path: str = CI_PATH, jobs: list[str] | None = None, **overrides) -> dict:
        run = {
            "id": run_id, "repository": {"full_name": REPO}, "path": path, "event": "pull_request",
            "head_repository": {"full_name": REPO}, "head_branch": HEAD_REF,
            "head_commit": {"id": HEAD_SHA, "tree_id": head_tree}, "run_attempt": attempt,
            "status": "completed", "conclusion": "success", "pull_requests": [], "created_at": RUN_CREATED,
            "run_started_at": RUN_STARTED, "updated_at": RUN_UPDATED,
        }
        run.update(overrides)
        self.runs[run_id] = run
        name = marker.artifact_name(key or written(run_id, attempt))
        self.artifacts[run_id] = [{"name": name, "expired": False, "workflow_run": {"id": run_id}}]
        names = jobs or [SAVE_JOB, GATE_JOB, "Rust tests", "Deleted-test map names only open issues as gap owners (advisory)"]
        self.attempt_jobs[(run_id, attempt)] = [job(n) for n in names]
        self.latest_jobs[run_id] = [job(n) for n in names]
        return run

    def set_job(self, run_id: int, name: str, conclusion: str, attempt: int | None = None, latest: bool = True) -> None:
        lists = [self.attempt_jobs[(run_id, attempt)]] if attempt else []
        if latest:
            lists.append(self.latest_jobs[run_id])
        for jobs in lists:
            for entry in jobs:
                if entry["name"] == name:
                    entry["conclusion"] = conclusion

    def __call__(self, args: list[str]) -> str:
        assert args[0] == "api" and len(args) == 2, args
        path = args[1]
        self.calls.append(path)
        if any(fragment in path for fragment in self.failing):
            raise subprocess.CalledProcessError(1, ["gh", *args], stderr="HTTP 502")
        base = f"repos/{REPO}/"
        assert path.startswith(base), path
        rest = path[len(base):]
        if m := re.fullmatch(r"actions/runs/(\d+)", rest):
            if int(m.group(1)) not in self.runs:
                raise subprocess.CalledProcessError(1, ["gh", *args], stderr="HTTP 404")
            return json.dumps(self.runs[int(m.group(1))])
        if m := re.fullmatch(r"git/commits/([0-9a-f]+)", rest):
            return json.dumps({"sha": m.group(1), "tree": {"sha": self.commits[m.group(1)]}})
        if m := re.fullmatch(r"actions/runs/(\d+)/artifacts\?name=([^&]+)&per_page=100", rest):
            found = [a for a in self.artifacts.get(int(m.group(1)), []) if a["name"] == m.group(2)]
            return json.dumps({"total_count": len(found), "artifacts": found})
        if m := re.fullmatch(r"actions/runs/(\d+)/attempts/(\d+)/jobs\?per_page=100", rest):
            jobs = self.attempt_jobs.get((int(m.group(1)), int(m.group(2))), [])
            return json.dumps({"total_count": len(jobs), "jobs": jobs})
        if m := re.fullmatch(r"actions/runs/(\d+)/jobs\?filter=latest&per_page=100", rest):
            jobs = self.latest_jobs.get(int(m.group(1)), [])
            return json.dumps({"total_count": len(jobs), "jobs": jobs})
        if m := re.fullmatch(r"pulls\?head=([^&]+)&state=all&per_page=2", rest):
            owner, _, ref = unquote(m.group(1)).partition(":")
            found = [p for p in self.pulls if p["head"]["ref"] == ref and p["head"]["repo"]["full_name"].split("/")[0] == owner]
            return json.dumps(found[:2])
        if m := re.fullmatch(r"issues/(\d+)/timeline\?per_page=100&page=(\d+)", rest):
            return json.dumps(page_of(self.timeline.get(int(m.group(1)), []), int(m.group(2))))
        if m := re.fullmatch(r"issues/(\d+)/events\?per_page=100&page=(\d+)", rest):
            return json.dumps(page_of(self.events.get(int(m.group(1)), []), int(m.group(2))))
        if m := re.fullmatch(r"actions/caches\?key=([^&]+)&per_page=100&page=(\d+)", rest):
            found = [c for c in self.caches if c["key"].startswith(unquote(m.group(1)))]
            return json.dumps({"total_count": len(found), "actions_caches": page_of(found, int(m.group(2)))})
        if m := re.fullmatch(r"actions/artifacts\?name=([^&]+)&per_page=100&page=(\d+)", rest):
            found = [a for a in self.fail_records if a["name"] == unquote(m.group(1))]
            return json.dumps({"total_count": len(found), "artifacts": page_of(found, int(m.group(2)))})
        raise AssertionError(f"unexpected call {path}")


def page_of(items: list, page: int) -> list:
    return items[(page - 1) * 100:page * 100]


PR_CREATED = "2026-11-01T00:00:00Z"
RUN_CREATED = "2026-11-02T00:00:00Z"
# Fields that must never stand in for the two created_at: a run's
# run_started_at and updated_at move on a re-run, and a pull request's
# updated_at moves on every change. The pull request's is after RUN_CREATED, so
# reading it refuses every run that should skip.
RUN_STARTED = "2026-11-02T00:00:07Z"
RUN_UPDATED = "2026-11-02T00:40:00Z"
PR_UPDATED = "2026-11-03T00:00:00Z"


def pull(number: int, ref: str = HEAD_REF, state: str = "open", repo: str = REPO, merged: bool = False,
         created_at: str | None = PR_CREATED) -> dict:
    return {"number": number, "state": state, "merged_at": "2026-11-01T00:00:00Z" if merged else None,
            "created_at": created_at, "updated_at": PR_UPDATED,
            "head": {"ref": ref, "repo": {"full_name": repo}}, "base": {"ref": "3.x.x"}}


def world() -> FakeGitHub:
    github = FakeGitHub()
    github.add_run()
    return github


def decide(github: FakeGitHub, **overrides) -> dict:
    args = {
        "lookup": "true", "prefix": prefix(), "outcome": "success", "primary_key": prefix(),
        "matched_key": written(), "gh": github, "repository": REPO, "run_id": str(CURRENT),
        "workflow_ref": CI_REF, "head_repository": REPO, "head_ref": HEAD_REF, "head_sha": HEAD_SHA,
        "require_jobs": [SAVE_JOB, GATE_JOB], "latest_jobs": [GATE_JOB], "pr": str(PR),
        "fail_key_": fail(), "fail_outcome": "success", "fail_hit": "",
    }
    args.update(overrides)
    return marker.skip_decision(**args)


def lookup(**overrides) -> dict:
    """A same-repository pull request run of ci.yml with no labels."""
    args = {
        "event": "pull_request", "action": "synchronize", "repository": REPO, "head_repository": REPO,
        "pr": "840", "payload_labels": '["documentation"]', "fetch_live_labels": lambda: ["documentation"],
        "workflow_ref": CI_REF, "salt": "", "tree": TREE,
    }
    args.update(overrides)
    return marker.lookup_decision(**args)


def needs(results: dict[str, str], skip: str = "false", marker_result: str = "success") -> dict:
    value = {"tree-marker": {"result": marker_result,
                             "outputs": {"skip": skip, "prefix": prefix(), "save": "true", "fail_key": fail()}}}
    value.update({j: {"result": r, "outputs": {}} for j, r in results.items()})
    return value


def raises(error: BaseException):
    def fetch() -> list[str]:
        raise error
    return fetch


class KeyTests(unittest.TestCase):
    def test_the_keys_name_the_workflow_salt_tree_and_run(self) -> None:
        self.assertEqual(prefix(), f"tree-pass-v1-ci.yml-salt1-{TREE}-")
        self.assertEqual(fail(), f"tree-fail-v1-ci.yml-salt1-{TREE}")
        self.assertEqual(written(), f"tree-pass-v1-ci.yml-salt1-{TREE}-500-1")
        self.assertEqual(marker.marker_prefix("ci.yml", "1", "e" * 64), f"tree-pass-v1-ci.yml-salt1-{'e' * 64}-")
        self.assertRegex(marker.artifact_name(written()), r"^tree-pass-[0-9a-f]{64}$")
        self.assertNotEqual(marker.artifact_name(written()), marker.artifact_name(written(attempt=2)))

    def test_a_different_tree_salt_or_workflow_is_a_different_key(self) -> None:
        prefixes = {prefix(), prefix(tree=OTHER_TREE), prefix(salt="2"), prefix(workflow="prism-load-l3.yml")}
        fails = {fail(), fail(tree=OTHER_TREE), fail(salt="2"), fail(workflow="prism-load-l3.yml")}
        self.assertEqual((len(prefixes), len(fails)), (4, 4))
        for one in prefixes:
            for other in prefixes - {one}:
                self.assertFalse(other.startswith(one), (one, other))

    def test_a_malformed_part_has_no_key(self) -> None:
        for workflow, salt, tree in [
            ("ci.yml", "1", ""), ("ci.yml", "1", TREE[:39]), ("ci.yml", "1", TREE.upper()),
            ("ci.yml", "1", TREE + "\n"), ("ci.yml", "1", "g" * 40), ("ci.yml", "", TREE),
            ("ci.yml", "-1", TREE), ("ci.yml", "01", TREE), ("ci.yml", "1.5", TREE),
            ("ci.yml", "1" * 10, TREE), ("", "1", TREE), ("ci", "1", TREE), ("../ci.yml", "1", TREE),
        ]:
            with self.subTest(workflow=workflow, salt=salt, tree=tree):
                with self.assertRaises(marker.Invalid):
                    marker.marker_prefix(workflow, salt, tree)
                with self.assertRaises(marker.Invalid):
                    marker.fail_key(workflow, salt, tree)
        for run_id, attempt in [("0", "1"), ("1", "0"), ("01", "1"), ("x", "1"), ("1", ""), ("1\n", "1")]:
            with self.subTest(run_id=run_id, attempt=attempt):
                with self.assertRaises(marker.Invalid):
                    marker.marker_key(prefix(), run_id, attempt)

    def test_the_salt_is_the_repository_variable_or_one(self) -> None:
        self.assertEqual(marker.effective_salt(""), "1")
        self.assertEqual(marker.effective_salt("7"), "7")
        for salt in ["x", "-1", "01", " 1", "1\n", "1" * 10]:
            with self.subTest(salt=salt):
                with self.assertRaises(marker.Invalid):
                    marker.effective_salt(salt)

    def test_the_workflow_comes_from_this_repositorys_workflow_ref(self) -> None:
        self.assertEqual(marker.workflow_path(CI_REF, REPO), CI_PATH)
        self.assertEqual(marker.workflow_file(L3_REF, REPO), "prism-load-l3.yml")
        for ref in [
            "", "ci.yml", f"{REPO}/{CI_PATH}", f"{REPO}/{CI_PATH}@",
            "other/repo/.github/workflows/ci.yml@refs/heads/main",
            # Another repository whose name is as long as this one's.
            "Qbit-Org/qbit-mining-bootstrbp/.github/workflows/ci.yml@refs/heads/main",
            f"{REPO}/.github/workflows/nested/ci.yml@refs/heads/main",
            f"{REPO}/ci.yml@refs/heads/main",
        ]:
            with self.subTest(ref=ref):
                with self.assertRaises(marker.Invalid):
                    marker.workflow_path(ref, REPO)
        with self.assertRaises(marker.Invalid):
            marker.workflow_path(CI_REF, "not a repository")

    def test_a_matched_key_names_its_run_only_under_exactly_this_prefix(self) -> None:
        self.assertEqual(marker.parse_marker_key(prefix(), written(123, 4)), (123, 4))
        for matched in ["", prefix(), written(at=prefix(tree=OTHER_TREE)), written(at=prefix(salt="2")),
                        prefix() + "123", prefix() + "123-", prefix() + "0-1", prefix() + "1-0",
                        prefix() + "01-1", prefix() + "1-01", prefix() + "1-1-1", prefix() + "1-1\n",
                        prefix() + "x-1", written() + " "]:
            with self.subTest(matched=matched):
                self.assertIsNone(marker.parse_marker_key(prefix(), matched))
        self.assertIsNone(marker.parse_marker_key("not a prefix", written()))


class LookupTests(unittest.TestCase):
    def test_a_same_repository_pull_request_looks_up_and_may_record(self) -> None:
        result = lookup()
        self.assertEqual((result["lookup"], result["save"], result["prefix"], result["fail_key"]),
                         (True, True, prefix(), fail()))

    def test_the_repository_variable_salts_both_keys(self) -> None:
        result = lookup(salt="2")
        self.assertEqual((result["prefix"], result["fail_key"]), (prefix(salt="2"), fail(salt="2")))
        for salt in ["x", "01", "-3"]:
            with self.subTest(salt=salt):
                result = lookup(salt=salt)
                self.assertEqual((result["lookup"], result["save"], result["prefix"]), (False, False, ""))

    def test_no_other_event_looks_up_or_records(self) -> None:
        # A tag is a push event (refs/tags/v*); merge_group, schedule and
        # dispatch never run on a pull request's merge result either.
        for event in ["push", "merge_group", "schedule", "workflow_dispatch", "pull_request_target",
                      "workflow_run", "release", ""]:
            with self.subTest(event=event):
                result = lookup(event=event)
                self.assertFalse(result["lookup"], result)
                self.assertFalse(result["save"], result)

    def test_a_forks_pull_request_never_looks_up_or_records(self) -> None:
        for head in ["someone/qbit-mining-bootstrap", "", "qbit-org/qbit-mining-bootstrap"]:
            with self.subTest(head=head):
                result = lookup(head_repository=head)
                self.assertEqual((result["lookup"], result["save"]), (False, False), result)

    def test_the_force_label_in_the_event_or_on_the_pull_request_runs_everything(self) -> None:
        for payload, live in [('["run-full-ci"]', []), ("[]", ["run-full-ci"]),
                              ('["documentation", "run-full-ci"]', ["documentation"])]:
            with self.subTest(payload=payload, live=live):
                result = lookup(payload_labels=payload, fetch_live_labels=lambda live=live: live)
                self.assertFalse(result["lookup"], result)
                # A forced full run that passes may still record its tree.
                self.assertTrue(result["save"], result)

    def test_unreadable_labels_run_everything(self) -> None:
        for payload in ["", "null", "{}", '"run-full-ci"', "[1]", "[", "[null]"]:
            with self.subTest(payload=payload):
                self.assertFalse(lookup(payload_labels=payload)["lookup"])
        for error in [subprocess.CalledProcessError(1, ["gh"], stderr="HTTP 403"), OSError("no gh"),
                      subprocess.TimeoutExpired(["gh"], 60), marker.Invalid("bad")]:
            with self.subTest(error=error):
                self.assertFalse(lookup(fetch_live_labels=raises(error))["lookup"])
        self.assertFalse(lookup(fetch_live_labels=None)["lookup"])

    def test_live_labels_are_read_from_the_pull_request(self) -> None:
        calls = []

        def gh(args: list[str]) -> str:
            calls.append(args)
            return json.dumps({"labels": [{"name": "run-full-ci"}, {"name": "bug"}]})

        self.assertEqual(marker.live_labels(gh, REPO, "840"), ["run-full-ci", "bug"])
        self.assertEqual(calls, [["api", f"repos/{REPO}/pulls/840"]])
        for body in ["", "[]", '{"labels": null}', '{"labels": [{"id": 1}]}', '{"labels": ["x"]}']:
            with self.subTest(body=body):
                with self.assertRaises(marker.Invalid):
                    marker.live_labels(lambda args, body=body: body, REPO, "840")

    def test_malformed_inputs_have_no_key_and_run_everything(self) -> None:
        for override in [{"tree": ""}, {"tree": "HEAD"}, {"salt": "x"}, {"workflow_ref": ""},
                         {"repository": ""}, {"pr": ""}, {"pr": "0"}, {"pr": "84a"}]:
            with self.subTest(override=override):
                result = lookup(**override)
                self.assertEqual((result["lookup"], result["save"]), (False, False), result)

    def test_l3_label_events_run_and_may_record(self) -> None:
        result = lookup(action="labeled", skip_labeled=True, workflow_ref=L3_REF)
        self.assertEqual((result["lookup"], result["save"]), (False, True), result)
        # ci.yml never runs on label events, and does not pass --skip-labeled.
        self.assertTrue(lookup(action="labeled")["lookup"])

    def test_l3_skips_only_when_the_commit_under_test_is_the_events(self) -> None:
        self.assertTrue(lookup(workflow_ref=L3_REF, tested_sha=SHA, event_sha=SHA)["lookup"])
        for tested, event in [(SHA, OTHER_SHA), (SHA, None), (SHA, ""), ("", SHA), ("HEAD", SHA)]:
            with self.subTest(tested=tested, event=event):
                result = lookup(workflow_ref=L3_REF, tested_sha=tested, event_sha=event)
                self.assertEqual((result["lookup"], result["save"]), (False, False), result)


class DecideTests(unittest.TestCase):
    def test_a_marker_whose_run_github_vouches_for_skips(self) -> None:
        github = world()
        result = decide(github)
        self.assertTrue(result["skip"], result)
        self.assertEqual(github.calls, [
            f"repos/{REPO}/actions/caches?key={fail().replace('.', '.')}&per_page=100&page=1",
            f"repos/{REPO}/actions/artifacts?name={marker.fail_record_name(fail())}&per_page=100&page=1",
            f"repos/{REPO}/pulls?head=Qbit-Org%3Akiwidream%2Fci-skip-same-tree&state=all&per_page=2",
            f"repos/{REPO}/issues/{PR}/timeline?per_page=100&page=1",
            f"repos/{REPO}/issues/{PR}/timeline?per_page=100&page=2",
            f"repos/{REPO}/issues/{PR}/events?per_page=100&page=1",
            f"repos/{REPO}/issues/{PR}/events?per_page=100&page=2",
            f"repos/{REPO}/actions/runs/{WRITER}", f"repos/{REPO}/git/commits/{HEAD_SHA}",
            f"repos/{REPO}/actions/runs/{WRITER}/artifacts?name={marker.artifact_name(written())}&per_page=100",
            f"repos/{REPO}/actions/runs/{WRITER}/attempts/1/jobs?per_page=100",
            f"repos/{REPO}/actions/runs/{WRITER}/jobs?filter=latest&per_page=100",
        ])

    def test_no_pass_marker_runs(self) -> None:
        # The cache action sets no cache-matched-key on a miss.
        github = world()
        self.assertFalse(decide(github, matched_key="")["skip"])
        self.assertEqual(github.calls, [])

    def test_every_lookup_failure_runs(self) -> None:
        github = world()
        for outcome in ["failure", "cancelled", "skipped", ""]:
            with self.subTest(outcome=outcome):
                self.assertFalse(decide(github, outcome=outcome)["skip"])
                self.assertFalse(decide(github, fail_outcome=outcome)["skip"])
        for flag in ["false", "", "True", "1"]:
            with self.subTest(lookup=flag):
                self.assertFalse(decide(github, lookup=flag)["skip"])
        self.assertFalse(decide(github, primary_key=written())["skip"])
        self.assertFalse(decide(github, primary_key="")["skip"])
        self.assertFalse(decide(github, prefix="", primary_key="")["skip"])

    def test_a_fail_marker_for_this_tree_runs_everything(self) -> None:
        github = world()
        self.assertFalse(decide(github, fail_hit="true")["skip"])
        # A fail lookup's cache-hit is unset on a miss and false when the
        # cache service is unavailable; anything but true is no fail marker.
        for hit in ["", "false"]:
            self.assertTrue(decide(github, fail_hit=hit)["skip"])
        for key in [fail(tree=OTHER_TREE), fail(salt="2"), "", "tree-fail-v1-ci.yml", prefix()]:
            with self.subTest(key=key):
                self.assertFalse(decide(github, fail_key_=key)["skip"])

    def test_a_marker_for_another_tree_salt_or_workflow_never_skips(self) -> None:
        github = world()
        for other in [prefix(tree=OTHER_TREE), prefix(salt="2"), prefix(workflow="prism-load-l3.yml")]:
            with self.subTest(other=other):
                github.add_run(key=written(at=other))
                self.assertFalse(decide(github, matched_key=written(at=other))["skip"])

    def test_the_run_must_be_a_pull_request_run_of_this_workflow_on_this_head(self) -> None:
        for override in [
            {"repository": {"full_name": "someone/qbit-mining-bootstrap"}},
            {"path": L3_PATH}, {"path": f"{CI_PATH}@refs/heads/main"}, {"path": None},
            {"event": "push"}, {"event": "pull_request_target"},
            {"head_repository": {"full_name": "someone/qbit-mining-bootstrap"}},
            {"head_branch": "other"}, {"head_commit": None}, {"id": WRITER + 1}, {"id": True},
            {"run_attempt": 0}, {"run_attempt": True}, {"run_attempt": None},
        ]:
            with self.subTest(override=override):
                github = FakeGitHub()
                github.add_run(**override)
                self.assertFalse(decide(github)["skip"])

    def test_another_head_tree_never_skips(self) -> None:
        github = FakeGitHub()
        github.add_run(head_tree=OTHER_TREE)
        self.assertFalse(decide(github)["skip"])
        github = world()
        github.commits[HEAD_SHA] = OTHER_TREE
        self.assertFalse(decide(github)["skip"])
        github.commits[HEAD_SHA] = "not a tree"
        self.assertFalse(decide(github)["skip"])
        # An unreadable tree on both sides is never a match.
        for unreadable in ["not a tree", None]:
            with self.subTest(unreadable=unreadable):
                github = FakeGitHub()
                github.add_run(head_commit={"id": HEAD_SHA, "tree_id": unreadable})
                github.commits[HEAD_SHA] = unreadable
                self.assertFalse(decide(github)["skip"])

    def test_a_successful_run_with_no_artifact_never_skips(self) -> None:
        github = world()
        github.artifacts[WRITER] = []
        self.assertFalse(decide(github)["skip"])

    def test_an_artifact_under_another_keys_name_never_skips(self) -> None:
        github = world()
        for key in [written(attempt=2), written(at=prefix(tree=OTHER_TREE))]:
            with self.subTest(key=key):
                github.artifacts[WRITER] = [{"name": marker.artifact_name(key), "expired": False,
                                             "workflow_run": {"id": WRITER}}]
                self.assertFalse(decide(github)["skip"])

    def test_an_expired_or_foreign_artifact_never_skips(self) -> None:
        name = marker.artifact_name(written())
        for artifacts in [
            [{"name": name, "expired": True, "workflow_run": {"id": WRITER}}],
            [{"name": name, "expired": None, "workflow_run": {"id": WRITER}}],
            [{"name": name, "expired": False, "workflow_run": {"id": WRITER + 1}}],
            [{"name": name, "expired": False, "workflow_run": {"id": WRITER}}] * 2,
        ]:
            with self.subTest(artifacts=artifacts):
                github = world()
                github.artifacts[WRITER] = artifacts
                self.assertFalse(decide(github)["skip"])
        # A JSON true is not the count 1, and a missing name is not this one.
        github = world()

        def gh(args: list[str]) -> str:
            text = github(args)
            if f"/runs/{WRITER}/artifacts?" in args[1]:
                page = json.loads(text)
                page["total_count"] = True
                return json.dumps(page)
            return text

        self.assertFalse(decide(github, gh=gh)["skip"])

    def test_an_artifact_listing_that_ignores_the_name_filter_never_skips(self) -> None:
        github = world()
        other = marker.artifact_name(written(attempt=2))

        def gh(args: list[str]) -> str:
            # The run's own artifact listing, not the repository's fail records.
            if f"/runs/{WRITER}/artifacts?" in args[1]:
                return json.dumps({"total_count": 1, "artifacts": [
                    {"name": other, "expired": False, "workflow_run": {"id": WRITER}}]})
            return github(args)

        result = decide(github, gh=gh)
        self.assertFalse(result["skip"])
        self.assertIn("is not this key's", result["reason"])

    def test_a_failed_aggregate_in_the_saving_attempt_never_skips(self) -> None:
        for name, conclusion in [(GATE_JOB, "failure"), (GATE_JOB, "cancelled"), (SAVE_JOB, "skipped"),
                                 (SAVE_JOB, "failure")]:
            with self.subTest(name=name, conclusion=conclusion):
                github = world()
                github.set_job(WRITER, name, conclusion, attempt=1, latest=False)
                self.assertFalse(decide(github)["skip"])
        github = world()
        github.attempt_jobs[(WRITER, 1)] = [job("Rust tests")]
        self.assertFalse(decide(github)["skip"])
        github = world()
        github.attempt_jobs[(WRITER, 1)][1]["status"] = "in_progress"
        self.assertFalse(decide(github)["skip"])

    def test_the_advisory_job_failing_does_not_matter(self) -> None:
        # Judged by jobs, not the run's conclusion, which this job can turn red.
        github = world()
        github.runs[WRITER]["conclusion"] = "failure"
        github.set_job(WRITER, "Deleted-test map names only open issues as gap owners (advisory)", "failure",
                       attempt=1)
        self.assertTrue(decide(github)["skip"])

    def test_a_failed_rerun_of_the_saving_run_never_skips(self) -> None:
        # Finding 1: run A (attempt 1) passed and saved; run-full-ci and a
        # re-run (attempt 2) failed. The latest gate execution refuses.
        github = world()
        github.runs[WRITER]["run_attempt"] = 2
        github.set_job(WRITER, GATE_JOB, "failure", latest=True)
        self.assertFalse(decide(github)["skip"])
        # A re-run that passed keeps the skip.
        github.set_job(WRITER, GATE_JOB, "success", latest=True)
        self.assertTrue(decide(github)["skip"])
        github.latest_jobs[WRITER] = [job("Rust tests")]
        self.assertFalse(decide(github)["skip"])

    def test_another_pull_requests_run_on_the_same_head_branch_never_skips(self) -> None:
        # Same head repository, branch and tree, another base: its marker and
        # artifact are for its own merge tree. A forged entry under this
        # prefix naming it finds no artifact for this key.
        github = world()
        github.add_run(600, key=written(600, at=prefix(tree=OTHER_TREE)))
        self.assertFalse(decide(github, matched_key=written(600))["skip"])
        self.assertFalse(decide(github, matched_key=written(600, at=prefix(tree=OTHER_TREE)))["skip"])

    def test_a_run_this_marker_skipped_never_skips(self) -> None:
        # A skipped run's save job reports skipped and it uploads nothing.
        github = world()
        github.add_run(700)
        github.set_job(700, SAVE_JOB, "skipped", attempt=1)
        github.artifacts[700] = []
        self.assertFalse(decide(github, matched_key=written(700))["skip"])
        github.artifacts[700] = [{"name": marker.artifact_name(written(700)), "expired": False,
                                  "workflow_run": {"id": 700}}]
        self.assertFalse(decide(github, matched_key=written(700))["skip"])

    def test_a_prefix_that_names_no_tree_never_skips(self) -> None:
        # Even when a run uploaded an artifact for exactly the matched key.
        github = FakeGitHub()
        github.add_run(key="x-500-1")
        self.assertFalse(decide(github, prefix="x-", primary_key="x-", matched_key="x-500-1", fail_key_=None)["skip"])

    def test_a_marker_naming_this_run_never_skips(self) -> None:
        github = world()
        github.add_run(CURRENT)
        self.assertFalse(decide(github, matched_key=written(CURRENT))["skip"])

    def test_an_entry_that_fails_the_checks_never_skips_even_with_a_valid_older_one(self) -> None:
        # The prefix lookup returns the newest entry. A forged or cancelled
        # newer one hides the valid older one and costs a full run.
        github = world()
        github.add_run(800)
        github.set_job(800, GATE_JOB, "cancelled", attempt=1)
        self.assertTrue(decide(github)["skip"])
        self.assertFalse(decide(github, matched_key=written(800))["skip"])
        self.assertFalse(decide(github, matched_key=written(801))["skip"])

    def test_another_pull_request_from_the_same_head_branch_never_skips(self) -> None:
        # Open, closed or merged: any second pull request from this head
        # branch could have run a changed workflow on another base.
        for other in [pull(841), pull(841, state="closed"), pull(841, state="closed", merged=True)]:
            with self.subTest(other=other):
                github = world()
                github.pulls.append(other)
                self.assertFalse(decide(github)["skip"])
        # A pull request from another owner's branch of the same name is not one.
        github = world()
        github.pulls.append(pull(842, repo="someone/qbit-mining-bootstrap"))
        self.assertTrue(decide(github)["skip"])

    def test_a_run_not_created_after_this_pull_request_never_skips(self) -> None:
        # The vouching run must be created after this pull request: one created
        # a second before or in the same second refuses (no genuine pair came
        # closer than 3 s); one a second later or more skips. Its
        # run_started_at and updated_at are after the pull request's
        # created_at throughout, so reading either refuses nothing here.
        for created, skips in [("2026-10-31T23:59:59Z", False), (PR_CREATED, False),
                               ("2026-11-01T00:00:01Z", True), (RUN_CREATED, True)]:
            with self.subTest(created=created):
                github = FakeGitHub()
                github.add_run(created_at=created, run_started_at="2026-11-01T00:00:05Z",
                               updated_at="2026-11-01T00:30:00Z")
                result = decide(github)
                self.assertEqual(result["skip"], skips)
                if not skips:
                    self.assertIn("not after this pull request", result["reason"])

    def test_only_the_two_creation_times_are_compared(self) -> None:
        # A run created before the pull request whose run_started_at and
        # updated_at are after it (a re-run moves both) refuses; a run created
        # after the pull request but before the pull request's updated_at
        # skips. A read of run_started_at, updated_at (with or without a
        # fallback to created_at) or the pull request's updated_at fails one.
        github = FakeGitHub()
        github.add_run(created_at="2026-10-31T23:00:00Z", run_started_at="2026-11-01T00:00:10Z",
                       updated_at="2026-11-01T01:00:00Z")
        self.assertFalse(decide(github)["skip"])
        github = FakeGitHub()
        github.add_run(created_at="2026-11-01T00:00:03Z", run_started_at="2026-11-01T00:00:09Z",
                       updated_at="2026-11-01T00:20:00Z")
        github.pulls[0]["updated_at"] = "2026-11-01T06:00:00Z"
        self.assertTrue(decide(github)["skip"])
        for side in ["run", "pull request"]:
            with self.subTest(side=side):
                # Neither side's created_at falls back to another field when
                # missing, though the other field would let the run skip.
                github = world()
                github.pulls[0]["updated_at"] = "2026-11-01T12:00:00Z"
                self.assertTrue(decide(github)["skip"])
                del (github.runs[WRITER] if side == "run" else github.pulls[0])["created_at"]
                self.assertFalse(decide(github)["skip"])

    def test_unreadable_creation_times_never_skip(self) -> None:
        for value in [None, "", "missing", "2026-11-02 00:00:00", "2026-11-02T00:00:00+00:00",
                      "2026-11-02T00:00:00.000Z", "2026-11-02T00:00Z", "2026-13-01T00:00:00Z",
                      "2026-02-30T00:00:00Z", "２０２６-11-02T00:00:00Z", "2026-11-02T00:00:00z",
                      " 2026-11-02T00:00:00Z", "2026-11-02T00:00:00Z\n", 1762041600, True]:
            with self.subTest(side="run", value=value):
                github = FakeGitHub()
                run = github.add_run()
                if value == "missing":
                    del run["created_at"]
                else:
                    run["created_at"] = value
                self.assertFalse(decide(github)["skip"])
            with self.subTest(side="pull request", value=value):
                github = world()
                if value == "missing":
                    del github.pulls[0]["created_at"]
                else:
                    github.pulls[0]["created_at"] = value
                self.assertFalse(decide(github)["skip"])

    def test_a_run_of_an_earlier_pull_request_from_a_recreated_branch_never_skips(self) -> None:
        # Sixth Man's scenario. #A from branch X ran R_A on head tree H (its
        # changed workflow uploaded the artifact for this key); #A was closed,
        # X deleted and re-created, #B opened from X; #B's cache holds a key
        # naming R_A. Even if the listing (hypothetically) returned only #B,
        # R_A was created before #B and is refused on that alone.
        github = FakeGitHub()
        github.add_run(created_at="2026-10-20T10:00:00Z")      # R_A, in #A's time
        github.pulls = [pull(PR, created_at="2026-10-25T09:00:00Z")]  # only #B listed
        result = decide(github)
        self.assertFalse(result["skip"])
        self.assertIn("not after this pull request", result["reason"])
        # The same run, had it been created after #B, would vouch: the
        # creation time is what refused it.
        github.runs[WRITER]["created_at"] = "2026-10-25T09:00:03Z"
        self.assertTrue(decide(github)["skip"])

    def test_the_run_records_own_created_at_is_read_never_an_attempts(self) -> None:
        # A re-run keeps the run record's created_at (run 41093895738: 16:53:07
        # on the run, 17:24:09 on attempt 2). decide reads only the run record.
        github = FakeGitHub()
        github.add_run(created_at="2026-10-20T10:00:00Z")
        github.pulls = [pull(PR, created_at="2026-10-25T09:00:00Z")]
        decide(github)
        self.assertNotIn(f"repos/{REPO}/actions/runs/{WRITER}/attempts/1", github.calls)
        self.assertIn(f"repos/{REPO}/actions/runs/{WRITER}", github.calls)

    def test_a_listing_of_another_pull_request_or_none_never_skips(self) -> None:
        for pulls in [[], [pull(841)], [pull(PR, ref="other")], [pull(PR, repo="someone/qbit-mining-bootstrap")]]:
            with self.subTest(pulls=pulls):
                github = world()
                github.pulls = pulls
                gh = github
                if pulls and pulls[0]["head"]["ref"] != HEAD_REF or pulls and pulls[0]["head"]["repo"]["full_name"] != REPO:
                    # A listing that ignores its head filter.
                    def gh(args: list[str], github=github) -> str:
                        if "/pulls?head=" in args[1]:
                            return json.dumps(github.pulls)
                        return github(args)
                self.assertFalse(decide(github, gh=gh)["skip"])

    def test_a_malformed_pull_request_listing_refuses_without_crashing(self) -> None:
        for body in ["null", "{}", '{"number": 840}', '"x"', "1", "[1]", "[null]"]:
            with self.subTest(body=body):
                github = world()

                def gh(args: list[str], body=body, github=github) -> str:
                    return body if "/pulls?head=" in args[1] else github(args)

                result = decide(github, gh=gh)
                self.assertFalse(result["skip"])
                self.assertIn("pull request", result["reason"])

    def test_a_pull_request_whose_base_moved_never_skips(self) -> None:
        # Retargeted by hand, retargeted by GitHub when its base was merged
        # and deleted (only automatic_base_change_succeeded appears), its base
        # force-pushed or deleted, and any other event with these prefixes:
        # the names are undocumented.
        for event in ["base_ref_changed", "base_ref_force_pushed", "base_ref_deleted",
                      "automatic_base_change_succeeded", "automatic_base_change_failed",
                      "base_ref_renamed_someday", "automatic_base_change_queued"]:
            for where in ["timeline", "events"]:
                with self.subTest(event=event, where=where):
                    github = world()
                    getattr(github, where)[PR].append({"event": event})
                    self.assertFalse(decide(github)["skip"])

    def test_a_base_change_anywhere_in_either_listing_never_skips(self) -> None:
        # First, mid-page and on a later page, with other events after it.
        other = [{"event": "commented"}]
        base = [{"event": "automatic_base_change_succeeded"}]
        for name, events in {
            "first": base + other * 5,
            "mid-page": other * 40 + base + other * 40,
            "mid second page": other * 150 + base + other * 20,
            "last of a full page": other * 99 + base + other * 3,
        }.items():
            for where in ["timeline", "events"]:
                with self.subTest(name=name, where=where):
                    github = world()
                    setattr(github, where, {PR: events})
                    self.assertFalse(decide(github)["skip"])
        # A short page in the middle never ends the listing.
        github = world()

        def gh(args: list[str]) -> str:
            if "/timeline?" in args[1]:
                page = int(args[1].rsplit("=", 1)[1])
                return json.dumps({1: [{"event": "commented"}] * 30, 2: [{"event": "base_ref_changed"}]}.get(page, []))
            return github(args)

        self.assertFalse(decide(github, gh=gh)["skip"])
        # Events that only look alike do not count.
        for event in ["head_ref_force_pushed", "referenced", "base_ref", "automatic_base"]:
            with self.subTest(event=event):
                github = world()
                github.timeline[PR].append({"event": event})
                self.assertTrue(decide(github)["skip"])
        for where in ["timeline", "events"]:
            github = world()
            getattr(github, where)[PR].append({"id": 1})
            self.assertFalse(decide(github)["skip"])

    def test_a_pull_request_whose_head_branch_was_deleted_or_restored_never_skips(self) -> None:
        # Two pull requests open from one branch; the branch deleted (both
        # close) and created again; this one reopened. A run of the other
        # created after this one passes the time rule, but this one records
        # head_ref_deleted (and head_ref_restored when restored), in either
        # listing, at any position.
        other = [{"event": "commented"}]
        for event in ["head_ref_deleted", "head_ref_restored"]:
            mark = [{"event": event}]
            for name, events in {
                "only": mark,
                "first": mark + other * 5,
                "mid-page": other * 40 + mark + other * 40,
                "last of a full page": other * 99 + mark + other * 3,
                "mid second page": other * 150 + mark + other * 20,
                "last": other * 230 + mark,
            }.items():
                for where in ["timeline", "events"]:
                    with self.subTest(event=event, name=name, where=where):
                        github = world()
                        setattr(github, where, {PR: events})
                        result = decide(github)
                        self.assertFalse(result["skip"])
                        self.assertIn(f"head branch was deleted or restored ({event})", result["reason"])
        # Both, one in each listing.
        github = world()
        github.timeline[PR].append({"event": "head_ref_deleted"})
        github.events[PR].append({"event": "head_ref_restored"})
        self.assertIn("(head_ref_deleted, head_ref_restored)", decide(github)["reason"])
        # Names that only look alike do not count; a force-push of the head is
        # routine.
        for event in ["head_ref_force_pushed", "head_ref", "head_ref_delete", "head_ref_deleted_",
                      "Head_ref_deleted", "head_ref_restore", "deleted", "restored", "head_ref_deleted "]:
            with self.subTest(event=event):
                github = world()
                github.timeline[PR].append({"event": event})
                github.events[PR].append({"event": event})
                self.assertTrue(decide(github)["skip"])

    def test_a_truncated_or_failing_listing_never_skips(self) -> None:
        # The pull request listing is one request for two: any count but one refuses.
        github = world()
        github.pulls = [pull(PR)] + [pull(10_000 + n) for n in range(1100)]
        self.assertFalse(decide(github)["skip"])
        self.assertEqual([c for c in github.calls if "/pulls?" in c],
                         [f"repos/{REPO}/pulls?head=Qbit-Org%3Akiwidream%2Fci-skip-same-tree&state=all&per_page=2"])
        # A listing of more than ten non-empty pages is never complete; ten
        # full pages and an empty one is.
        for where in ["timeline", "events"]:
            with self.subTest(where=where):
                github = world()
                setattr(github, where, {PR: [{"event": "commented"}] * 1001})
                self.assertFalse(decide(github)["skip"])
                setattr(github, where, {PR: [{"event": "commented"}] * 1000})
                self.assertTrue(decide(github)["skip"])
        # A retarget on a later page is found.
        github = world()
        github.timeline[PR] = [{"event": "commented"}] * 250 + [{"event": "base_ref_changed"}]
        self.assertFalse(decide(github)["skip"])
        github.timeline[PR] = [{"event": "commented"}] * 250
        self.assertTrue(decide(github)["skip"])
        for fragment in ["/pulls?head=", "/timeline?", "/events?", "/actions/caches?"]:
            with self.subTest(fragment=fragment):
                github = world()
                github.failing.append(fragment)
                self.assertFalse(decide(github)["skip"])
        for body in ["{}", "null", "[1]", '{"actions_caches": null}']:
            with self.subTest(body=body):
                github = world()

                def gh(args: list[str], body=body, github=github) -> str:
                    if any(part in args[1] for part in ["/pulls?", "/timeline?", "/events?", "/caches?"]):
                        return body
                    return github(args)

                self.assertFalse(decide(github, gh=gh)["skip"])

    def test_a_listing_reads_every_page_and_only_objects(self) -> None:
        pages = {1: [{"n": i} for i in range(100)], 2: [{"n": 100}]}
        calls = []

        def gh(args: list[str]) -> str:
            calls.append(args[1])
            return json.dumps(pages.get(int(args[1].rsplit("=", 1)[1]), []))

        self.assertEqual(len(marker.listing(gh, "repos/x/y/things?a=b", lambda page: page)), 101)
        self.assertEqual(calls, [f"repos/x/y/things?a=b&per_page=100&page={n}" for n in (1, 2, 3)])
        # A short page in the middle is read past.
        pages = {1: [{"n": 1}], 2: [{"n": 2}] * 100, 3: [{"n": 3}]}
        self.assertEqual(len(marker.listing(gh, "repos/x/y/things?a=b", lambda page: page)), 102)
        for page in [[1], [None], ["x"], [[]], {"a": 1}, None]:
            with self.subTest(page=page):
                # The bad page first, then the end of the listing.
                def gh(args: list[str], page=page) -> str:
                    return json.dumps(page if args[1].endswith("&page=1") else [])

                with self.assertRaises(marker.Invalid):
                    marker.listing(gh, "repos/x/y/things", lambda value: value)
        # Never an empty page: more than ten non-empty pages is never complete.
        for size in [1, 100]:
            with self.subTest(size=size):
                with self.assertRaises(marker.Invalid):
                    marker.listing(lambda args, size=size: json.dumps([{}] * size), "repos/x/y/things",
                                   lambda value: value)

    def test_a_head_ref_is_encoded_into_the_listing(self) -> None:
        ref = "feature/a#b&c+d%e=f?g h"
        github = world()
        github.runs[WRITER]["head_branch"] = ref
        github.pulls = [pull(PR, ref=ref)]
        self.assertTrue(decide(github, head_ref=ref)["skip"])
        listed = [call for call in github.calls if "/pulls?head=" in call]
        self.assertEqual(listed, [f"repos/{REPO}/pulls?head=Qbit-Org%3Afeature%2Fa%23b%26c%2Bd%25e%3Df%3Fg%20h"
                                  "&state=all&per_page=2"])

    def test_a_fail_record_artifact_stops_the_skip_when_the_cache_save_failed(self) -> None:
        # Finding 3: the fail job's second record, in another store.
        record = marker.fail_record_name(fail())
        self.assertRegex(record, r"^tree-fail-[0-9a-f]{64}$")
        self.assertNotEqual(record, marker.fail_record_name(fail(tree=OTHER_TREE)))
        for name, records, skips in [
            ("a live record", [{"name": record, "expired": False}], False),
            ("an unreadable expired flag", [{"name": record}], False),
            ("an expired record", [{"name": record, "expired": True}], True),
            ("none", [], True),
        ]:
            with self.subTest(name):
                github = world()
                github.fail_records = records
                self.assertEqual(decide(github)["skip"], skips)
        # A listing that ignores its name filter only counts this tree's record.
        github = world()

        def gh(args: list[str]) -> str:
            if "/actions/artifacts?name=" in args[1]:
                page = int(args[1].rsplit("=", 1)[1])
                return json.dumps({"total_count": 1, "artifacts": [
                    {"name": marker.fail_record_name(fail(tree=OTHER_TREE)), "expired": False}] if page == 1 else []})
            return github(args)

        self.assertTrue(decide(github, gh=gh)["skip"])
        for body in ["{}", "null", '{"artifacts": null}', "[1]"]:
            with self.subTest(body=body):
                github = world()

                def gh(args: list[str], body=body, github=github) -> str:
                    return body if "/actions/artifacts?name=" in args[1] else github(args)

                self.assertFalse(decide(github, gh=gh)["skip"])
        github = world()
        github.failing.append("/actions/artifacts?name=")
        self.assertFalse(decide(github)["skip"])
        # Every page is read: a live record behind a full page of expired ones.
        github = world()
        github.fail_records = [{"name": record, "expired": True}] * 100 + [{"name": record, "expired": False}]
        self.assertFalse(decide(github)["skip"])
        github.fail_records = [{"name": record, "expired": True}] * 101
        self.assertTrue(decide(github)["skip"])

    def test_the_cache_services_own_list_of_fail_markers_is_read(self) -> None:
        # The fail lookup step reports a cache service error as a miss.
        github = world()
        github.caches = [{"key": fail(), "ref": f"refs/pull/{PR}/merge"}]
        self.assertFalse(decide(github)["skip"])
        github.caches = [{"key": fail() + "-other", "ref": f"refs/pull/{PR}/merge"}]
        self.assertTrue(decide(github)["skip"])
        # L3 has no fail marker and never asks.
        github = world()
        github.failing.append("/actions/caches?")
        self.assertFalse(decide(github)["skip"])

    def test_every_api_failure_runs(self) -> None:
        for fragment in [f"runs/{WRITER}", "git/commits/", f"/runs/{WRITER}/artifacts?", "/actions/artifacts?name=",
                         "/attempts/", "filter=latest"]:
            with self.subTest(fragment=fragment):
                github = world()
                github.failing.append(fragment)
                self.assertFalse(decide(github)["skip"])
        for body in ["", "{", "[]", "null"]:
            with self.subTest(body=body):
                self.assertFalse(decide(world(), gh=lambda args, body=body: body)["skip"])

    def test_an_incomplete_job_list_never_skips(self) -> None:
        github = world()

        def gh(args: list[str]) -> str:
            text = github(args)
            if "/jobs?" in args[1]:
                page = json.loads(text)
                page["total_count"] = 101
                return json.dumps(page)
            return text

        self.assertFalse(decide(github, gh=gh)["skip"])

    def test_malformed_run_inputs_never_skip(self) -> None:
        github = world()
        for override in [{"run_id": ""}, {"run_id": "x"}, {"head_sha": ""}, {"head_sha": "HEAD"},
                         {"head_repository": ""}, {"head_ref": ""}, {"head_ref": "a\nb"}, {"head_ref": "x" * 256},
                         {"workflow_ref": ""}, {"repository": ""}, {"require_jobs": []}, {"latest_jobs": []},
                         {"pr": ""}, {"pr": "0"}, {"pr": "84a"}]:
            with self.subTest(override=override):
                self.assertFalse(decide(github, **override)["skip"])

    def promotion(self, **overrides) -> Path:
        decision = {
            "tree": TREE, "promote": True, "source_run": WRITER, "considered": [], "malformed": [],
            "verdict": {"schema": "qbit.prism.l3-verdict.v1", "suite": "l3-full", "tree": TREE,
                        "complete": True, "passed": True},
        }
        decision.update(overrides)
        directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, directory)
        path = directory / "promotion.json"
        path.write_text(json.dumps(decision), encoding="utf-8")
        return path

    def l3(self, github: FakeGitHub, **overrides) -> dict:
        at = prefix(workflow="prism-load-l3.yml")
        args = {"prefix": at, "primary_key": at, "matched_key": written(at=at), "workflow_ref": L3_REF,
                "require_jobs": [SAVE_JOB, COLLATE_JOB], "latest_jobs": [COLLATE_JOB], "fail_key_": None,
                "tree": TREE}
        args.update(overrides)
        return decide(github, **args)

    def l3_world(self) -> FakeGitHub:
        github = FakeGitHub()
        github.add_run(key=written(at=prefix(workflow="prism-load-l3.yml")), path=L3_PATH,
                       jobs=[SAVE_JOB, COLLATE_JOB, "Build the release binaries once"])
        return github

    def test_l3_skips_only_when_bound_and_the_tags_promotion_carries_a_passing_run(self) -> None:
        self.assertTrue(self.l3(self.l3_world(), promotion=self.promotion())["skip"])
        failing = {"schema": "qbit.prism.l3-verdict.v1", "suite": "l3-full", "tree": TREE,
                   "complete": True, "passed": False}
        for override in [
            {"promote": False, "reason": "rerun: no finished run"},
            {"tree": OTHER_TREE},
            {"verdict": failing},
            {"verdict": {**failing, "passed": True, "tree": OTHER_TREE}},
            {"verdict": {**failing, "passed": True, "complete": False}},
            {"verdict": {**failing, "passed": True, "suite": "l3-reduced"}},
            {"verdict": None},
        ]:
            with self.subTest(override=override):
                self.assertFalse(self.l3(self.l3_world(), promotion=self.promotion(**override))["skip"])
        missing = Path(tempfile.gettempdir()) / "tree-pass-marker-no-such-promotion.json"
        self.assertFalse(self.l3(self.l3_world(), promotion=missing)["skip"])
        # Another run of this tree, a newer pass or another pull request's,
        # is not the marker's run.
        for source in [77, str(WRITER), None, True]:
            with self.subTest(source=source):
                self.assertFalse(self.l3(self.l3_world(), promotion=self.promotion(source_run=source))["skip"])
        self.assertFalse(self.l3(self.l3_world(), promotion=self.promotion(), tree=OTHER_TREE)["skip"])
        self.assertFalse(self.l3(self.l3_world(), promotion=self.promotion(), tree=None)["skip"])

    def test_l3_binding_applies_too(self) -> None:
        for created in ["2026-10-31T23:59:59Z", PR_CREATED]:
            github = self.l3_world()
            github.runs[WRITER]["created_at"] = created
            self.assertFalse(self.l3(github, promotion=self.promotion())["skip"])
        for event in ["head_ref_deleted", "head_ref_restored"]:
            github = self.l3_world()
            github.events[PR].append({"event": event})
            self.assertFalse(self.l3(github, promotion=self.promotion())["skip"])
        github = self.l3_world()
        github.pulls.append(pull(841, state="closed"))
        self.assertFalse(self.l3(github, promotion=self.promotion())["skip"])
        github = self.l3_world()
        github.timeline[PR].append({"event": "base_ref_changed"})
        self.assertFalse(self.l3(github, promotion=self.promotion())["skip"])
        github = self.l3_world()
        github.set_job(WRITER, COLLATE_JOB, "failure", attempt=1)
        self.assertFalse(self.l3(github, promotion=self.promotion())["skip"])
        github = self.l3_world()
        github.artifacts[WRITER] = []
        self.assertFalse(self.l3(github, promotion=self.promotion())["skip"])
        # A ci.yml run never vouches for an L3 marker.
        github = world()
        self.assertFalse(self.l3(github, matched_key=written(at=prefix(workflow="prism-load-l3.yml")),
                                 promotion=self.promotion())["skip"])

    def test_the_promotion_scripts_own_decision_is_what_decide_reads(self) -> None:
        import prism_l3_promote as promote

        def gh_for(passed: bool):
            run = {"id": WRITER, "event": "pull_request", "path": promote.WORKFLOW_PATH, "status": "completed",
                   "conclusion": "success" if passed else "failure", "head_repository": {"full_name": REPO},
                   "html_url": "https://example.invalid/77", "pull_requests": [{"number": 840}]}
            verdict = {"schema": promote.VERDICT_SCHEMA, "suite": "l3-full", "tree": TREE,
                       "complete": True, "passed": passed}

            def gh(args: list[str]) -> str:
                if "actions/artifacts" in args[1]:
                    return json.dumps({"artifacts": [{"workflow_run": {"id": WRITER}, "expired": False,
                                                      "created_at": "2026-11-10T00:00:00Z"}]})
                if "actions/runs/" in args[1]:
                    return json.dumps(run)
                (Path(args[args.index("-D") + 1]) / "verdict.json").write_text(json.dumps(verdict))
                return ""
            return gh

        for passed in [True, False]:
            with self.subTest(passed=passed):
                path = self.promotion()
                path.write_text(json.dumps(promote.decide(gh_for(passed), REPO, TREE, run_id=900)))
                self.assertEqual(self.l3(self.l3_world(), promotion=path)["skip"], passed)

    def test_a_promotion_that_passed_over_a_newer_run_never_skips(self) -> None:
        # Sixth Man (EP-STATE): run A (the marker's) passed; a newer run B of
        # the tree uploaded a verdict too. The tag's decision records B under
        # `considered` and promotes A whenever B cannot be read as a complete
        # pass; the skip must refuse then, whatever the reason.
        import prism_l3_promote as promote

        def base_run(run_id: int, **overrides) -> dict:
            run = {"id": run_id, "event": "pull_request", "path": promote.WORKFLOW_PATH, "status": "completed",
                   "conclusion": "success", "head_repository": {"full_name": REPO},
                   "html_url": f"https://example.invalid/{run_id}", "pull_requests": []}
            run.update(overrides)
            return run

        def verdict(**overrides) -> dict:
            value = {"schema": promote.VERDICT_SCHEMA, "suite": "l3-full", "tree": TREE, "complete": True,
                     "passed": True}
            value.update(overrides)
            return value

        def gh_for(newer: dict | None, newer_verdict, lookup_fails: bool = False, download_fails: bool = False,
                   newer_created: str = "2026-11-11T00:00:00Z"):
            runs = {WRITER: (base_run(WRITER), verdict())}
            if newer is not None:
                runs[newer["id"]] = (newer, newer_verdict)

            def gh(args: list[str]) -> str:
                if "actions/artifacts" in args[1]:
                    return json.dumps({"artifacts": [
                        {"workflow_run": {"id": run_id}, "expired": False,
                         "created_at": "2026-11-10T00:00:00Z" if run_id == WRITER else newer_created}
                        for run_id in runs]})
                if "actions/runs/" in args[1]:
                    run_id = int(args[1].rsplit("/", 1)[1])
                    if run_id != WRITER and lookup_fails:
                        raise subprocess.CalledProcessError(1, ["gh", *args], stderr="HTTP 502")
                    return json.dumps(runs[run_id][0])
                run_id = int(args[2])
                if run_id != WRITER and download_fails:
                    raise subprocess.CalledProcessError(1, ["gh", *args], stderr="no artifact")
                content = runs[run_id][1]
                (Path(args[args.index("-D") + 1]) / "verdict.json").write_text(
                    content if isinstance(content, str) else json.dumps(content))
                return ""
            return gh

        def skip(gh) -> bool:
            path = self.promotion()
            path.write_text(json.dumps(promote.decide(gh, REPO, TREE, run_id=CURRENT)))
            return self.l3(self.l3_world(), promotion=path)["skip"]

        self.assertTrue(skip(gh_for(None, None)))
        newer = 600
        for name, gh in {
            "the newer run's lookup failed": gh_for(base_run(newer), verdict(), lookup_fails=True),
            "its verdict could not be downloaded": gh_for(base_run(newer), verdict(), download_fails=True),
            "its verdict is unreadable": gh_for(base_run(newer), "{not json"),
            "its verdict is incomplete": gh_for(base_run(newer), verdict(complete=False, passed=False)),
            "its verdict is for another suite": gh_for(base_run(newer), verdict(suite="l3-reduced")),
            "its verdict is for another tree": gh_for(base_run(newer), verdict(tree=OTHER_TREE)),
            "it has not finished": gh_for(base_run(newer, status="in_progress", conclusion=None), verdict()),
            "it was cancelled": gh_for(base_run(newer, conclusion="cancelled"), verdict()),
            "it timed out": gh_for(base_run(newer, conclusion="timed_out"), verdict()),
            "it was not a pull_request run": gh_for(base_run(newer, event="workflow_dispatch"), verdict()),
            "it was a fork's": gh_for(base_run(newer, head_repository={"full_name": "someone/x"}), verdict()),
            # Read as complete: the tag promotes it, and it is not the marker's run.
            "it failed a gate": gh_for(base_run(newer, conclusion="failure"), verdict(passed=False)),
            "it passed, but is not the marker's run": gh_for(base_run(newer), verdict()),
            # Finding 1 (task_231eb76b6445): this run's own earlier attempt
            # (run-full-ci and "Re-run all jobs") measured the tree and failed.
            "it is this run's earlier attempt, and failed": gh_for(base_run(CURRENT), verdict(passed=False)),
            "it is this run's earlier attempt, and passed": gh_for(base_run(CURRENT), verdict()),
        }.items():
            with self.subTest(name):
                self.assertFalse(skip(gh))
        # This run's earlier attempt older than the marker's run: a tag would
        # promote the marker's run, and so does the skip.
        self.assertTrue(skip(gh_for(base_run(CURRENT), verdict(passed=False), newer_created="2026-11-09T00:00:00Z")))

    def test_a_malformed_verdict_artifact_refuses_the_skip(self) -> None:
        # Finding 5: an artifact a tag drops or ranks oldest because it cannot
        # be traced or ranked for certain is never silently passed over.
        import prism_l3_promote as promote
        writer = {"id": WRITER, "event": "pull_request", "path": promote.WORKFLOW_PATH, "status": "completed",
                  "conclusion": "success", "head_repository": {"full_name": REPO}, "html_url": "x",
                  "pull_requests": []}
        good = {"workflow_run": {"id": WRITER}, "expired": False, "created_at": "2026-11-10T00:00:00Z"}
        passing = {"schema": promote.VERDICT_SCHEMA, "suite": "l3-full", "tree": TREE, "complete": True,
                   "passed": True}

        def skip(*extra: object) -> bool:
            def gh(args: list[str]) -> str:
                if "actions/artifacts" in args[1]:
                    return json.dumps({"artifacts": [good, *extra]})
                if "actions/runs/" in args[1]:
                    return json.dumps(writer)
                (Path(args[args.index("-D") + 1]) / "verdict.json").write_text(json.dumps(passing))
                return ""
            path = self.promotion()
            path.write_text(json.dumps(promote.decide(gh, REPO, TREE, run_id=CURRENT)))
            return self.l3(self.l3_world(), promotion=path)["skip"]

        self.assertTrue(skip())
        for name, artifact in {
            "no run id": {"workflow_run": {}, "expired": False, "created_at": "2026-11-11T00:00:00Z"},
            "no workflow_run": {"expired": False, "created_at": "2026-11-11T00:00:00Z"},
            "a boolean run id": {"workflow_run": {"id": True}, "expired": False, "created_at": "2026-11-11T00:00:00Z"},
            "a string run id": {"workflow_run": {"id": "600"}, "expired": False, "created_at": "2026-11-11T00:00:00Z"},
            "expired as a string": {"workflow_run": {"id": 600}, "expired": "false", "created_at": "2026-11-11T00:00:00Z"},
            # Older than the writer's verdict, so the walk would not promote it: only `malformed` refuses.
            "no expired flag": {"workflow_run": {"id": 600}, "created_at": "2026-11-09T00:00:00Z"},
            "no created_at": {"workflow_run": {"id": 600}, "expired": False},
            "an empty created_at": {"workflow_run": {"id": 600}, "expired": False, "created_at": ""},
            "not an object": "artifact",
        }.items():
            with self.subTest(name):
                self.assertFalse(skip(artifact))
        # An expired artifact is well-formed and dropped, as before.
        self.assertTrue(skip({"workflow_run": {"id": 600}, "expired": True, "created_at": "2026-11-11T00:00:00Z"}))
        for malformed in ["missing", None, "x", {"a": 1}]:
            with self.subTest(malformed=malformed):
                path = self.promotion()
                decision = json.loads(path.read_text())
                if malformed == "missing":
                    del decision["malformed"]
                else:
                    decision["malformed"] = malformed
                path.write_text(json.dumps(decision))
                self.assertFalse(self.l3(self.l3_world(), promotion=path)["skip"])

    def test_a_promotion_decision_must_say_what_it_passed_over(self) -> None:
        self.assertTrue(self.l3(self.l3_world(), promotion=self.promotion())["skip"])
        for considered in ["missing", None, "x", [1], [None], {"run": 600}]:
            with self.subTest(considered=considered):
                path = self.promotion()
                decision = json.loads(path.read_text())
                if considered == "missing":
                    del decision["considered"]
                else:
                    decision["considered"] = considered
                path.write_text(json.dumps(decision))
                self.assertFalse(self.l3(self.l3_world(), promotion=path)["skip"])
        path = self.promotion(considered=[{"run": 600, "rejected": "lookup failed (HTTP 502)"}])
        result = self.l3(self.l3_world(), promotion=path)
        self.assertFalse(result["skip"])
        self.assertIn("run 600: lookup failed (HTTP 502)", result["reason"])

    def test_an_unreadable_promotion_decision_runs(self) -> None:
        path = self.promotion()
        for text in ["", "{", "[]", "null"]:
            with self.subTest(text=text):
                path.write_text(text, encoding="utf-8")
                self.assertFalse(self.l3(self.l3_world(), promotion=path)["skip"])


class SaveTests(unittest.TestCase):
    def save(self, value: dict | None = None, **overrides) -> dict:
        args = {
            "needs": value if value is not None else needs({"lint": "success", "shards": "success"}),
            "marker_job": "tree-marker", "skip": "false", "save": "true", "prefix": prefix(),
            "recomputed_prefix": prefix(), "event": "pull_request", "requirements": [],
        }
        args.update(overrides)
        return marker.save_decision(**args)

    def test_a_full_pass_records_its_tree(self) -> None:
        self.assertTrue(self.save()["save"])

    def test_nothing_is_recorded_unless_every_job_succeeded(self) -> None:
        for result in ["failure", "cancelled", "skipped"]:
            with self.subTest(result=result):
                self.assertFalse(self.save(needs({"lint": result, "shards": "success"}))["save"])
        self.assertFalse(self.save(needs({"lint": "success"}, marker_result="failure"))["save"])
        self.assertFalse(self.save(needs({}))["save"])

    def test_a_run_the_marker_skipped_never_records(self) -> None:
        for skip in ["true", "", "False"]:
            with self.subTest(skip=skip):
                self.assertFalse(self.save(skip=skip)["save"])

    def test_only_a_same_repository_pull_request_run_records(self) -> None:
        self.assertFalse(self.save(save="false")["save"])
        self.assertFalse(self.save(save="")["save"])
        for event in ["push", "workflow_dispatch", "schedule", "merge_group", ""]:
            with self.subTest(event=event):
                self.assertFalse(self.save(event=event)["save"])

    def test_the_prefix_must_be_this_checkouts_tree(self) -> None:
        self.assertFalse(self.save(recomputed_prefix=prefix(tree=OTHER_TREE))["save"])
        self.assertFalse(self.save(recomputed_prefix=prefix(salt="2"))["save"])
        self.assertFalse(self.save(prefix="", recomputed_prefix="")["save"])

    def test_required_outputs_must_match(self) -> None:
        value = needs({"collate": "success"})
        value["collate"]["outputs"] = {"status": "0", "verdict": "true"}
        required = [("collate", "status", "0"), ("collate", "verdict", "true")]
        self.assertTrue(self.save(value, requirements=required)["save"])
        for outputs in [{"status": "1", "verdict": "true"}, {"status": "0"}, {}]:
            with self.subTest(outputs=outputs):
                value["collate"]["outputs"] = outputs
                self.assertFalse(self.save(value, requirements=required)["save"])

    def run_save(self, *extra: str, tree: str = TREE, salt: str = "") -> tuple[dict, Path]:
        directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, directory)
        out = directory / "github-output"
        out.write_text("", encoding="utf-8")
        argv = [
            "save", "--needs", json.dumps(needs({"lint": "success"})), "--marker-job", "tree-marker",
            "--skip", "false", "--save", "true", "--prefix", prefix(), "--workflow-ref", CI_REF,
            "--repository", REPO, "--salt", salt, "--tree", tree, "--event", "pull_request",
            "--marker-dir", str(directory / "tree-pass-marker"), "--sha", SHA, "--run-id", "123",
            "--run-attempt", "2", *extra,
        ]
        old = os.environ.get("GITHUB_OUTPUT")
        os.environ["GITHUB_OUTPUT"] = str(out)
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(marker.main(argv), 0)
        finally:
            if old is None:
                del os.environ["GITHUB_OUTPUT"]
            else:
                os.environ["GITHUB_OUTPUT"] = old
        outputs = dict(line.split("=", 1) for line in out.read_text(encoding="utf-8").splitlines())
        return outputs, directory / "tree-pass-marker" / "marker.json"

    def test_the_save_command_writes_the_marker_its_key_and_artifact(self) -> None:
        outputs, path = self.run_save()
        key = written(123, 2)
        self.assertEqual(outputs, {"save": "true", "key": key, "artifact": marker.artifact_name(key)})
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {
            "schema": marker.MARKER_SCHEMA, "key": key, "artifact": marker.artifact_name(key), "tree": TREE,
            "commit": SHA, "run_id": 123, "run_attempt": 2})

    def test_the_save_command_recomputes_the_prefix_from_the_checkouts_tree_and_salt(self) -> None:
        for kwargs in [{"tree": OTHER_TREE}, {"salt": "2"}, {"salt": "x"}]:
            with self.subTest(kwargs=kwargs):
                outputs, path = self.run_save(**kwargs)
                self.assertEqual(outputs, {"save": "false", "key": "", "artifact": ""})
                self.assertFalse(path.exists())
        outputs, _ = self.run_save("--require", "not-a-requirement")
        self.assertEqual(outputs["save"], "false")


class FailKeyTests(unittest.TestCase):
    """The fail job's fallback when the marker job left no key."""

    def run_key(self, *extra: str, tree: str = TREE, event: str = "pull_request", head: str = REPO,
                salt: str = "") -> tuple[int, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = marker.main(["fail-key", "--workflow-ref", CI_REF, "--repository", REPO, "--salt", salt,
                                "--tree", tree, "--event", event, "--head-repository", head, *extra])
        return code, out.getvalue().strip()

    def test_the_key_is_this_checkouts_tree(self) -> None:
        self.assertEqual(self.run_key(), (0, fail()))
        self.assertEqual(self.run_key(salt="2"), (0, fail(salt="2")))

    def test_no_key_for_another_event_a_fork_or_an_unreadable_input(self) -> None:
        for kwargs in [{"event": "push"}, {"event": ""}, {"head": "someone/qbit-mining-bootstrap"},
                       {"tree": ""}, {"tree": "HEAD"}, {"salt": "x"}]:
            with self.subTest(kwargs=kwargs):
                self.assertEqual(self.run_key(**kwargs), (1, ""))


class CommandTests(unittest.TestCase):
    def run_main(self, argv: list[str], gh=None) -> dict[str, str]:
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "github-output"
            summary = Path(directory) / "summary"
            out.write_text("", encoding="utf-8")
            old = {name: os.environ.get(name) for name in ("GITHUB_OUTPUT", "GITHUB_STEP_SUMMARY")}
            os.environ["GITHUB_OUTPUT"] = str(out)
            os.environ["GITHUB_STEP_SUMMARY"] = str(summary)
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    kwargs = {"gh": gh} if gh else {}
                    self.assertEqual(marker.main(argv, **kwargs), 0)
            finally:
                for name, value in old.items():
                    if value is None:
                        os.environ.pop(name, None)
                    else:
                        os.environ[name] = value
            return dict(line.split("=", 1) for line in out.read_text(encoding="utf-8").splitlines())

    def lookup_argv(self, *extra: str) -> list[str]:
        return ["lookup", "--workflow-ref", CI_REF, "--repository", REPO, "--salt", "", "--tree", TREE,
                "--event", "pull_request", "--action", "synchronize", "--head-repository", REPO,
                "--pr", "840", "--labels", '[\n  "documentation"\n]', *extra]

    def test_lookup_writes_its_outputs(self) -> None:
        gh = lambda args: json.dumps({"labels": []})  # noqa: E731
        self.assertEqual(self.run_main(self.lookup_argv("--live-labels"), gh),
                         {"lookup": "true", "save": "true", "prefix": prefix(), "fail_key": fail()})
        # Without --live-labels a re-run could miss a label applied since.
        self.assertEqual(self.run_main(self.lookup_argv())["lookup"], "false")

    def test_lookup_with_a_failing_label_read_runs_everything(self) -> None:
        def gh(args: list[str]) -> str:
            raise subprocess.CalledProcessError(1, ["gh", *args], stderr="HTTP 502")

        self.assertEqual(self.run_main(self.lookup_argv("--live-labels"), gh),
                         {"lookup": "false", "save": "true", "prefix": prefix(), "fail_key": fail()})

    def test_lookup_on_a_push_records_nothing(self) -> None:
        argv = self.lookup_argv()
        argv[argv.index("pull_request")] = "push"
        self.assertEqual(self.run_main(argv)["lookup"], "false")
        self.assertEqual(self.run_main(argv)["save"], "false")

    def decide_argv(self, **values: str) -> list[str]:
        args = {"lookup": "true", "prefix": prefix(), "outcome": "success", "primary-key": prefix(),
                "matched-key": written(), "fail-key": fail(), "fail-outcome": "success", "fail-hit": "",
                "repository": REPO, "run-id": str(CURRENT), "workflow-ref": CI_REF, "head-repository": REPO,
                "head-ref": HEAD_REF, "head-sha": HEAD_SHA, "pr": str(PR)}
        args.update(values)
        argv = ["decide"]
        for name, value in args.items():
            argv += [f"--{name}", value]
        return argv + ["--require-job", SAVE_JOB, "--require-job", GATE_JOB, "--latest-job", GATE_JOB]

    def test_decide_writes_skip(self) -> None:
        self.assertEqual(self.run_main(self.decide_argv(), world()), {"skip": "true"})
        self.assertEqual(self.run_main(self.decide_argv(**{"matched-key": ""}), world()), {"skip": "false"})
        self.assertEqual(self.run_main(self.decide_argv(**{"fail-hit": "true"}), world()), {"skip": "false"})
        self.assertEqual(self.run_main(self.decide_argv()[:-6], world()), {"skip": "false"})
        self.assertEqual(self.run_main(["decide"], world()), {"skip": "false"})


class FakeCache:
    """GitHub's documented cache semantics for one pull request's merge ref:
    an existing key cannot be overwritten, and a prefix lookup matches the
    newest entry."""

    def __init__(self) -> None:
        self.entries: list[str] = []

    def save(self, key: str) -> None:
        if key not in self.entries:
            self.entries.append(key)

    def newest(self, at: str) -> str:
        return next((key for key in reversed(self.entries) if key.startswith(at)), "")


class OrderingTests(unittest.TestCase):
    """EP-STATE: runs that finish out of order, repeated saves, and a later
    failing run of the same tree (finding 1)."""

    def setUp(self) -> None:
        self.cache = FakeCache()
        self.github = FakeGitHub()
        self.trees = {TREE: HEAD_TREE, OTHER_TREE: "9" * 40}

    def run_ci(self, tree: str, run_id: int, passed: bool, **lookup_overrides) -> bool:
        head_sha = tree.replace("a", "e").replace("b", "8")
        self.github.commits[head_sha] = self.trees[tree]
        decided = lookup(tree=tree, **lookup_overrides)
        found = {"outcome": "skipped", "primary_key": "", "matched_key": "", "fail_outcome": "skipped",
                 "fail_hit": ""}
        if decided["lookup"]:
            found = {"outcome": "success", "primary_key": decided["prefix"],
                     "matched_key": self.cache.newest(decided["prefix"]), "fail_outcome": "success",
                     "fail_hit": "true" if decided["fail_key"] in self.cache.entries else ""}
        self.github.caches = [{"key": key} for key in self.cache.entries]
        skip = marker.skip_decision(lookup="true" if decided["lookup"] else "false", prefix=decided["prefix"],
                                    gh=self.github, repository=REPO, run_id=str(run_id), workflow_ref=CI_REF,
                                    head_repository=REPO, head_ref=HEAD_REF, head_sha=head_sha,
                                    require_jobs=[SAVE_JOB, GATE_JOB], latest_jobs=[GATE_JOB], pr=str(PR),
                                    fail_key_=decided["fail_key"], **found)["skip"]
        result = "success" if skip or passed else "failure"
        key = marker.marker_key(decided["prefix"], str(run_id), "1") if decided["prefix"] else ""
        saved = marker.save_decision(
            needs=needs({"lint": "skipped" if skip else result}, skip="true" if skip else "false"),
            marker_job="tree-marker", skip="true" if skip else "false", save="true" if decided["save"] else "false",
            prefix=decided["prefix"], recomputed_prefix=decided["prefix"],
            event=lookup_overrides.get("event", "pull_request"), requirements=[],
        )["save"]
        self.github.add_run(run_id, key=key or written(run_id), head_tree=self.trees[tree],
                            head_commit={"id": head_sha, "tree_id": self.trees[tree]})
        self.github.set_job(run_id, SAVE_JOB, "success" if saved else "skipped", attempt=1)
        self.github.set_job(run_id, GATE_JOB, "success" if skip or passed else "failure", attempt=1)
        if not saved:
            self.github.artifacts[run_id] = []
        if saved:
            self.cache.save(key)
        # tree-marker-fail: a full run that failed records the tree.
        if not skip and not passed and decided["save"]:
            self.cache.save(decided["fail_key"])
        return skip

    def test_an_older_run_finishing_last_cannot_mark_the_newer_tree(self) -> None:
        self.assertFalse(self.run_ci(OTHER_TREE, 2, passed=False))
        self.assertFalse(self.run_ci(TREE, 1, passed=True))
        self.assertTrue(self.run_ci(TREE, 3, passed=True))
        self.assertFalse(self.run_ci(OTHER_TREE, 4, passed=True))
        self.assertEqual(set(self.cache.entries),
                         {fail(tree=OTHER_TREE), written(1), written(4, at=prefix(tree=OTHER_TREE))})

    def test_a_fail_marker_the_lookup_missed_still_stops_the_skip(self) -> None:
        # Finding 4: a cache service error made the fail lookup report a miss.
        self.assertFalse(self.run_ci(TREE, 1, passed=True))
        self.cache.save(fail())
        real = self.cache.entries
        self.cache.entries = [key for key in real if key != fail()]
        self.github.caches = [{"key": key} for key in real]
        decided = lookup(tree=TREE)
        result = marker.skip_decision(lookup="true", prefix=decided["prefix"], outcome="success",
                                      primary_key=decided["prefix"], matched_key=written(1), gh=self.github,
                                      repository=REPO, run_id="5", workflow_ref=CI_REF, head_repository=REPO,
                                      head_ref=HEAD_REF, head_sha=HEAD_SHA, require_jobs=[SAVE_JOB, GATE_JOB],
                                      latest_jobs=[GATE_JOB], pr=str(PR), fail_key_=fail(), fail_outcome="success",
                                      fail_hit="")
        self.assertFalse(result["skip"], result)

    def test_a_later_failing_run_of_the_same_tree_stops_the_skip(self) -> None:
        # Finding 1: A passes and saves; a forced run B of the same tree fails
        # (cargo deny's live advisory database); the label comes off and the
        # squash push must not skip on A's marker.
        self.assertFalse(self.run_ci(TREE, 1, passed=True))
        self.assertFalse(self.run_ci(TREE, 2, passed=False, payload_labels='["run-full-ci"]'))
        self.assertIn(fail(), self.cache.entries)
        self.assertFalse(self.run_ci(TREE, 3, passed=True))
        self.assertFalse(self.run_ci(TREE, 4, passed=True))

    def test_a_skipped_run_records_nothing_and_a_repeated_pass_keeps_skipping(self) -> None:
        self.run_ci(TREE, 1, passed=True)
        self.assertTrue(self.run_ci(TREE, 2, passed=True))
        self.run_ci(TREE, 3, passed=True, payload_labels='["run-full-ci"]')
        self.assertTrue(self.run_ci(TREE, 4, passed=True))
        self.assertEqual([k for k in self.cache.entries if k.startswith("tree-pass")], [written(1), written(3)])

    def test_a_push_never_reads_or_writes_a_marker(self) -> None:
        self.run_ci(TREE, 1, passed=True)
        self.assertFalse(self.run_ci(TREE, 2, passed=True, event="push"))
        self.assertFalse(self.run_ci(TREE, 3, passed=False, event="push"))
        self.assertEqual(self.cache.entries, [written(1)])


def jobs(path: Path) -> dict[str, str]:
    body = path.read_text(encoding="utf-8").split("\njobs:\n", 1)[1]
    names = re.findall(r"^  ([A-Za-z0-9_-]+):$", body, flags=re.MULTILINE)
    parts = re.split(r"^  [A-Za-z0-9_-]+:$", body, flags=re.MULTILINE)[1:]
    return dict(zip(names, parts, strict=True))


def field(job: str, name: str) -> str | None:
    match = re.search(rf"^    {name}: (.*)$", job, flags=re.MULTILINE)
    return match.group(1) if match else None


def yaml_name(job: str) -> str | None:
    """A job's `name` as GitHub reads it. field() returns the rest of the line;
    an unquoted YAML value ends where " #" starts a comment. This reads what the
    workflows here use, not all of YAML: a quoted name followed by a comment
    holding the same quote, or a YAML-only escape, is misread or raises."""
    value = field(job, "name")
    if value is None:
        return None
    value = value.strip()
    if value[:1] == '"':
        return json.loads(value[:value.rindex('"') + 1])
    if value[:1] == "'":
        return value[1:value.rindex("'")].replace("''", "'")
    return value.split(" #", 1)[0].rstrip()


def asked_jobs(step: str) -> list[str]:
    """The job names a decide step passes as --require-job and --latest-job."""
    return re.findall(r'--(?:require|latest)-job "([^"]+)"', step)


def needs_of(job: str) -> list[str]:
    value = field(job, "needs")
    if value is None:
        return []
    return [name.strip() for name in value.strip("[]").split(",")]


def steps_of(job: str) -> list[str]:
    body = job.split("\n    steps:\n", 1)[1]
    return re.split(r"^(?=      - name: )", body, flags=re.MULTILINE)[1:]


def step_named(job: str, name: str) -> str:
    found = [step for step in steps_of(job) if step.startswith(f"      - name: {name}\n")]
    assert len(found) == 1, name
    return found[0]


def step_field(step: str, name: str) -> str | None:
    match = re.search(rf"^        {name}: (.*)$", step, flags=re.MULTILINE)
    return match.group(1) if match else None


def step_env(step: str) -> dict[str, str]:
    block = step.split("        env:\n", 1)[1] if "        env:\n" in step else ""
    env = {}
    for line in block.splitlines():
        if not line.startswith("          ") or line.startswith("           "):
            break
        name, value = line.strip().split(": ", 1)
        env[name] = value
    return env


def step_with(step: str) -> list[str]:
    """The `with:` inputs of a step, as `name: value` lines."""
    lines = step.splitlines()
    if "        with:" not in lines:
        return []
    found = []
    for line in lines[lines.index("        with:") + 1:]:
        if not line.startswith("          ") or line.startswith("           "):
            break
        found.append(line.strip())
    return found


def status_function(condition: str) -> bool:
    return bool(re.search(r"\b(?:always|cancelled|failure|success)\(", condition))


def jq_gate() -> str:
    step = step_named(jobs(CI_WORKFLOW)["checks"], "Require every check to pass")
    return re.search(r"jq -e '(.*?)'\n", step, flags=re.DOTALL).group(1)


class GateTests(unittest.TestCase):
    """The aggregate check's rule, run with jq as the workflow runs it."""

    def setUp(self) -> None:
        if shutil.which("jq") is None:
            if os.environ.get("CI"):
                self.fail("jq is required to test the aggregate check")
            self.skipTest("jq is not installed")
        self.skippable = re.search(r"\[([^\]]*)\] as \$skippable", jq_gate()).group(1)

    def gate(self, value: dict) -> int:
        return subprocess.run(["jq", "-e", jq_gate()], input=json.dumps(value), capture_output=True,
                              text=True).returncode

    def all_jobs(self, results: dict[str, str] | None = None, skip: str = "false",
                 marker_result: str = "success") -> dict:
        names = ["lint-compile-compose", "python-tests", "rust-tests", "rust-fmt", "rust-clippy", "cargo-deny",
                 "dockerfile-lint", "prism-dual-writer", "prism-native-postgres", "prism-integration-proof"]
        value = {"tree-marker": {"result": marker_result, "outputs": {"skip": skip}}}
        value.update({name: {"result": "success", "outputs": {}} for name in names})
        for name, result in (results or {}).items():
            value[name]["result"] = result
        return value

    def marked(self) -> dict[str, str]:
        return {name: "skipped" for name in self.skippable.replace('"', "").replace(",", " ").split()}

    def test_without_a_marker_skip_every_job_must_succeed(self) -> None:
        self.assertEqual(self.gate(self.all_jobs()), 0)
        for result in ["failure", "cancelled", "skipped"]:
            with self.subTest(result=result):
                self.assertEqual(self.gate(self.all_jobs({"rust-tests": result})), 1)

    def test_a_skipped_marker_job_is_a_push_or_a_fork_and_everything_must_succeed(self) -> None:
        self.assertEqual(self.gate(self.all_jobs(marker_result="skipped", skip="")), 0)
        self.assertEqual(self.gate(self.all_jobs(self.marked(), marker_result="skipped", skip="")), 1)

    def test_a_marker_skip_passes_with_only_the_job_level_skips(self) -> None:
        self.assertEqual(self.gate(self.all_jobs(self.marked(), skip="true")), 0)

    def test_a_marker_skip_still_fails_on_a_failure_a_cancellation_or_an_unexpected_skip(self) -> None:
        for extra in [{"python-tests": "skipped"}, {"prism-native-postgres": "skipped"},
                      {"python-tests": "failure"}, {"rust-tests": "failure"}, {"rust-tests": "cancelled"}]:
            with self.subTest(extra=extra):
                self.assertEqual(self.gate(self.all_jobs({**self.marked(), **extra}, skip="true")), 1)

    def test_a_marker_job_that_failed_is_never_a_marker_skip(self) -> None:
        # Finding 3: skip=true from a marker job that then failed is not a skip.
        self.assertEqual(self.gate(self.all_jobs(self.marked(), skip="true", marker_result="failure")), 1)
        self.assertEqual(self.gate(self.all_jobs(skip="true", marker_result="failure")), 0)
        self.assertEqual(self.gate(self.all_jobs(self.marked(), skip="")), 1)

    def test_a_missing_marker_job_is_never_a_marker_skip(self) -> None:
        value = self.all_jobs(self.marked(), skip="true")
        del value["tree-marker"]
        self.assertEqual(self.gate(value), 1)


class CiWorkflowTests(unittest.TestCase):
    ADVISORY = {"deleted-test-map-owner-issues"}
    JOB_IF = "${{ !cancelled() && " + RUN + " }}"

    def setUp(self) -> None:
        self.jobs = jobs(CI_WORKFLOW)
        self.text = CI_WORKFLOW.read_text(encoding="utf-8")

    def test_the_jobs_the_decision_asks_for_are_named_as_github_names_them(self) -> None:
        names = {yaml_name(job) for job in self.jobs.values()}
        asked = asked_jobs(step_named(self.jobs["tree-marker"], "Decide whether this run skips"))
        self.assertEqual(asked, [SAVE_JOB, GATE_JOB, GATE_JOB])
        self.assertLessEqual(set(asked), names)

    def classify(self) -> tuple[set[str], set[str]]:
        """(skipped at job level, matrix jobs skipped step by step)."""
        job_level, step_level = set(), set()
        for name, job in self.jobs.items():
            if name.startswith("tree-marker"):
                continue
            if MARKED in (field(job, "if") or ""):
                job_level.add(name)
            elif "tree-marker" in needs_of(job) and "\n    strategy:\n" in job:
                step_level.add(name)
        return job_level, step_level

    def test_the_marker_job_runs_only_for_a_same_repository_pull_request(self) -> None:
        job = self.jobs["tree-marker"]
        self.assertEqual(needs_of(job), [])
        self.assertEqual(field(job, "if"), "github.event_name == 'pull_request' && "
                         "github.event.pull_request.head.repo.full_name == github.repository")
        self.assertEqual(field(job, "runs-on"), "ubuntu-24.04")
        self.assertEqual(field(self.jobs["tree-marker-save"], "runs-on"), "ubuntu-24.04")
        self.assertEqual(field(self.jobs["tree-marker-fail"], "runs-on"), "ubuntu-24.04")
        self.assertIn("    permissions:\n      actions: read\n      contents: read\n      pull-requests: read\n"
                      "    outputs:", job)

    def test_every_marker_step_has_a_timeout_and_continues_on_error(self) -> None:
        job = self.jobs["tree-marker"]
        budget = 0
        # Finding 7: a failed checkout leaves the marker job green and runs everything.
        checkout = step_named(job, "Check out the marker script")
        self.assertEqual(step_field(checkout, "id"), "checkout")
        self.assertEqual(step_field(step_named(job, "Compute the keys from the tree under test"), "if"),
                         "steps.checkout.outcome == 'success'")
        for name, minutes in [("Check out the marker script", "3"),
                              ("Compute the keys from the tree under test", "2"), ("Look the pass marker up", "2"),
                              ("Look the fail marker up", "2"), ("Decide whether this run skips", "3")]:
            with self.subTest(step=name):
                step = step_named(job, name)
                self.assertEqual(step_field(step, "timeout-minutes"), minutes)
                self.assertEqual(step_field(step, "continue-on-error"), "true")
                budget += int(minutes)
        self.assertLess(budget + 2, int(field(job, "timeout-minutes")))

    def test_every_job_is_skipped_by_the_marker_or_deliberately_not(self) -> None:
        job_level, step_level = self.classify()
        self.assertEqual(step_level, {"python-tests", "prism-native-postgres"})
        rest = set(self.jobs) - job_level - step_level - {"tree-marker", "tree-marker-save", "tree-marker-fail",
                                                          "checks"}
        self.assertEqual(rest, self.ADVISORY)
        for name in job_level:
            with self.subTest(job=name):
                self.assertIn("tree-marker", needs_of(self.jobs[name]))
                self.assertNotIn("\n    strategy:\n", self.jobs[name])
                # The gate's own test, last, with nothing appended after it.
                expected = self.JOB_IF
                if name == "qbit-runtime-image":
                    expected = ("${{ !cancelled() && (github.event_name != 'push' || github.ref == "
                                "'refs/heads/main') && " + RUN + " }}")
                self.assertEqual(field(self.jobs[name], "if"), expected)

    def test_matrix_jobs_skip_every_step_and_their_service_on_a_github_hosted_runner(self) -> None:
        for name in ["python-tests", "prism-native-postgres"]:
            job = self.jobs[name]
            with self.subTest(job=name):
                self.assertEqual(field(job, "if"), "${{ !cancelled() }}")
                self.assertEqual(field(job, "runs-on"),
                                 f"${{{{ ({MARKED}) && 'ubuntu-24.04' || 'blacksmith-2vcpu-ubuntu-2404' }}}}")
                for step in steps_of(job):
                    condition = step_field(step, "if") or ""
                    with self.subTest(step=step.splitlines()[0]):
                        self.assertEqual(condition.count(RUN), 1, condition)
                        self.assertTrue(condition in ("${{ " + RUN + " }}",) or condition.endswith(" && " + RUN)
                                        or condition.endswith(" && " + RUN + " }}"), condition)
                        self.assertFalse(status_function(condition.replace("!cancelled()", "")), condition)
        self.assertIn(f"        image: ${{{{ {RUN} && 'postgres:16.15' || '' }}}}\n", self.jobs["prism-native-postgres"])

    def test_the_aggregate_check_needs_no_checkout_and_accepts_only_the_job_level_skips(self) -> None:
        job = self.jobs["checks"]
        self.assertEqual(field(job, "name"), GATE_JOB)
        self.assertEqual(field(job, "if"), "${{ always() }}")
        self.assertNotIn("uses:", job)
        gate_needs = needs_of(job)
        self.assertEqual(gate_needs[0], "tree-marker")
        job_level, step_level = self.classify()
        self.assertLessEqual(set(gate_needs) - {"tree-marker"}, job_level | step_level)
        program = jq_gate()
        self.assertIn('(.["tree-marker"].result == "success" and .["tree-marker"].outputs.skip == "true") as $marked',
                      program)
        skippable = set(re.findall(r'"([a-z0-9-]+)"', re.search(r"\[([^\]]*)\] as \$skippable", program).group(1)))
        self.assertEqual(skippable, set(gate_needs) & job_level)
        self.assertFalse(skippable & step_level)

    def test_the_save_job_needs_every_job_the_marker_stands_for_and_their_success(self) -> None:
        job = self.jobs["tree-marker-save"]
        job_level, step_level = self.classify()
        self.assertEqual(set(needs_of(job)), {"tree-marker", "checks"} | job_level | step_level)
        self.assertEqual(field(job, "name"), SAVE_JOB)
        self.assertEqual(field(job, "if"),
                         "needs.tree-marker.outputs.save == 'true' && needs.tree-marker.outputs.skip == 'false'")
        self.assertIn("    permissions:\n      contents: read\n    steps:", job)
        save = step_named(job, "Check that every marked job passed, and write the marker")
        self.assertIn("--marker-job tree-marker \\\n", save)
        self.assertIn('--prefix "${PREFIX}"', save)
        self.assertEqual(step_env(save), {
            "NEEDS": "${{ toJSON(needs) }}", "SKIP": "${{ needs.tree-marker.outputs.skip }}",
            "SAVE": "${{ needs.tree-marker.outputs.save }}", "PREFIX": "${{ needs.tree-marker.outputs.prefix }}"})
        upload = step_named(job, "Upload the marker's run record")
        self.assertIn("uses: actions/upload-artifact@v7\n", upload)
        self.assertEqual(step_field(upload, "if"), "steps.save.outputs.save == 'true'")
        self.assertIn("          name: ${{ steps.save.outputs.artifact }}\n          path: tree-pass-marker/marker.json\n",
                      upload)
        self.assertEqual(step_field(upload, "continue-on-error"), "true")
        cache = step_named(job, "Save the marker")
        self.assertEqual(step_field(cache, "if"),
                         "steps.save.outputs.save == 'true' && steps.upload.outcome == 'success'")
        self.assertIn("uses: actions/cache/save@v6\n", cache)
        self.assertIn("          path: tree-pass-marker\n          key: ${{ steps.save.outputs.key }}\n", cache)

    def test_a_failed_full_run_records_the_tree_as_failed(self) -> None:
        # Finding 2: any run the marker did not skip, its marker job finished
        # or not, with the counted jobs listed and tree-marker not among them.
        job = self.jobs["tree-marker-fail"]
        self.assertEqual(set(needs_of(job)), set(needs_of(self.jobs["tree-marker-save"])))
        self.assertEqual(field(job, "if"),
                         "${{ !cancelled() && github.event_name == 'pull_request' && "
                         "github.event.pull_request.head.repo.full_name == github.repository && "
                         f"{RUN} && (contains(needs.*.result, 'failure') || contains(needs.*.result, 'cancelled')) }}}}")
        self.assertIn("    permissions:\n      contents: read\n    steps:", job)
        self.assertEqual(field(job, "runs-on"), "ubuntu-24.04")
        # The checkout is only the fallback's, may fail, and checks out GITHUB_SHA.
        checkout = step_named(job, "Check out the marker script")
        self.assertEqual(step_field(checkout, "id"), "checkout")
        self.assertEqual(step_field(checkout, "continue-on-error"), "true")
        self.assertIn("        uses: actions/checkout@v7\n", checkout)
        self.assertEqual(step_with(checkout), ["sparse-checkout: scripts", "persist-credentials: false"])
        check = step_named(job, "Check which marked jobs failed, and write the fail marker")
        counted = json.loads(re.search(r"(\[[^\]]*\]) as \$counted", check).group(1))
        self.assertEqual(set(counted), set(needs_of(job)) - {"tree-marker"})
        self.assertEqual(len(counted), len(set(counted)))
        for outside in ["ckpool-rejects-observability", "docker-builds", "qbit-runtime-image", "checks"]:
            self.assertIn(outside, counted)
        self.assertIn('if [[ "${MARKER_RESULT}" == success && "${MARKER_FAIL_KEY}" =~ ^tree-fail-v1-', check)
        self.assertIn('tree="$(git rev-parse "${GITHUB_SHA}^{tree}")"', check)
        self.assertIn('--salt "${TREE_PASS_SALT}" --tree "${tree}"', check)
        self.assertIn("python3 scripts/tree_pass_marker.py fail-key", check)
        self.assertEqual(step_env(check), {"NEEDS": "${{ toJSON(needs) }}",
                                           "MARKER_RESULT": "${{ needs.tree-marker.result }}",
                                           "MARKER_FAIL_KEY": "${{ needs.tree-marker.outputs.fail_key }}",
                                           "HEAD_REPO": "${{ github.event.pull_request.head.repo.full_name }}"})
        self.assertIsNone(step_field(check, "if"))
        cache = step_named(job, "Save the fail marker")
        # Finding 3: the save is gated on exactly the step's decision, and a
        # save that fails never turns the job red.
        self.assertEqual(step_field(cache, "if"), "steps.fail.outputs.save == 'true'")
        self.assertEqual(step_field(cache, "continue-on-error"), "true")
        self.assertIn("uses: actions/cache/save@v6\n", cache)
        self.assertIn("          path: tree-fail-marker\n          key: ${{ steps.fail.outputs.key }}\n", cache)
        # Finding 3: a second record in another store, read by decide.
        upload = step_named(job, "Upload the fail record")
        self.assertEqual(step_field(upload, "if"), "steps.fail.outputs.save == 'true'")
        self.assertEqual(step_field(upload, "continue-on-error"), "true")
        self.assertIn("        uses: actions/upload-artifact@v7\n", upload)
        # Never overwrite: a re-run's name conflict fails harmlessly and keeps
        # the earlier attempt's record (overwrite: true deletes it first).
        self.assertEqual(step_with(upload), ["name: ${{ steps.fail.outputs.artifact }}",
                                             "path: tree-fail-marker/fail.json", "retention-days: 90",
                                             "overwrite: false", "if-no-files-found: error"])
        # Finding 1: the artifact first, every step with its own timeout, all
        # of them inside the job's.
        names = [step.splitlines()[0].split("- name: ", 1)[1] for step in steps_of(job)]
        self.assertEqual(names, ["Check out the marker script", "Check which marked jobs failed, and write the fail marker",
                                 "Upload the fail record", "Save the fail marker"])
        budget = 0
        for name, minutes in [("Check out the marker script", "3"),
                              ("Check which marked jobs failed, and write the fail marker", "2"),
                              ("Upload the fail record", "3"), ("Save the fail marker", "3")]:
            with self.subTest(step=name):
                self.assertEqual(step_field(step_named(job, name), "timeout-minutes"), minutes)
                budget += int(minutes)
        self.assertLess(budget + 2, int(field(job, "timeout-minutes")))
        self.assertIn('artifact="tree-fail-$(printf \'%s\' "${key}" | sha256sum | cut -d\' \' -f1)"', check)
        lookup_step = step_named(self.jobs["tree-marker"], "Look the fail marker up")
        self.assertIn("          path: tree-fail-marker\n          key: ${{ steps.lookup.outputs.fail_key }}\n",
                      lookup_step)
        self.assertNotIn("restore-keys", lookup_step)

    def test_every_marker_checkout_checks_out_the_commit_under_test(self) -> None:
        for workflow, job_name in [(CI_WORKFLOW, "tree-marker"), (CI_WORKFLOW, "tree-marker-save"),
                                   (CI_WORKFLOW, "tree-marker-fail"), (L3_WORKFLOW, "tree-marker-save")]:
            with self.subTest(job=job_name, workflow=workflow.name):
                checkout = step_named(jobs(workflow)[job_name], "Check out the marker script")
                self.assertIn("        uses: actions/checkout@v7\n", checkout)
                # No ref: GITHUB_SHA, the commit whose tree the keys name.
                self.assertEqual(step_with(checkout), ["sparse-checkout: scripts", "persist-credentials: false"])

    def test_the_lookups_are_lookup_only_and_never_restore(self) -> None:
        job = self.jobs["tree-marker"]
        restore = step_named(job, "Look the pass marker up")
        self.assertIn("uses: actions/cache/restore@v6\n", restore)
        self.assertEqual(step_field(restore, "if"), "steps.lookup.outputs.lookup == 'true'")
        self.assertIn("          path: tree-pass-marker\n          key: ${{ steps.lookup.outputs.prefix }}\n"
                      "          restore-keys: ${{ steps.lookup.outputs.prefix }}\n          lookup-only: true\n",
                      restore)
        # Every cache read in the marker job only looks up; nothing is extracted.
        restores = [step for step in steps_of(job) if "uses: actions/cache/restore@" in step]
        self.assertEqual(len(restores), 2)
        for step in restores:
            self.assertIn("          lookup-only: true\n", step)
        # The pins this repository already uses for the same actions.
        self.assertIn("uses: actions/cache/restore@v6\n", self.jobs["cargo-deny"])
        self.assertIn("uses: actions/cache/save@v6\n", self.jobs["cargo-deny"])
        self.assertIn("uses: actions/upload-artifact@v7\n", self.jobs["prism-integration-proof"])

    def test_the_decide_step_reads_exactly_the_lookup_steps_outputs(self) -> None:
        job = self.jobs["tree-marker"]
        decide_step = step_named(job, "Decide whether this run skips")
        # Finding 3: decide always runs; a missing lookup output reads as no lookup.
        self.assertIsNone(step_field(decide_step, "if"))
        self.assertEqual(step_env(decide_step), {
            "GH_TOKEN": "${{ github.token }}",
            "LOOKUP": "${{ steps.lookup.outputs.lookup }}",
            "PREFIX": "${{ steps.lookup.outputs.prefix }}",
            "OUTCOME": "${{ steps.cache.outcome }}",
            "PRIMARY_KEY": "${{ steps.cache.outputs.cache-primary-key }}",
            "MATCHED_KEY": "${{ steps.cache.outputs.cache-matched-key }}",
            "FAIL_KEY": "${{ steps.lookup.outputs.fail_key }}",
            "FAIL_OUTCOME": "${{ steps.fail.outcome }}",
            "FAIL_HIT": "${{ steps.fail.outputs.cache-hit }}",
            "HEAD_REPO": "${{ github.event.pull_request.head.repo.full_name }}",
            "HEAD_REF": "${{ github.event.pull_request.head.ref }}",
            "HEAD_SHA": "${{ github.event.pull_request.head.sha }}",
            "PR": "${{ github.event.pull_request.number }}",
        })
        for argument in ['--pr "${PR}"', '--lookup "${LOOKUP}"', '--prefix "${PREFIX}"', '--outcome "${OUTCOME}"',
                         '--primary-key "${PRIMARY_KEY}"', '--matched-key "${MATCHED_KEY}"',
                         '--fail-key "${FAIL_KEY}"', '--fail-outcome "${FAIL_OUTCOME}"', '--fail-hit "${FAIL_HIT}"',
                         '--run-id "${GITHUB_RUN_ID}"', '--head-sha "${HEAD_SHA}"', '--head-ref "${HEAD_REF}"']:
            self.assertIn(argument, decide_step)
        # The jobs decide asks GitHub about are this workflow's own.
        names = re.findall(r'--require-job "([^"]+)"', decide_step)
        self.assertEqual(names, [field(self.jobs["tree-marker-save"], "name"), field(self.jobs["checks"], "name")])
        self.assertEqual(re.findall(r'--latest-job "([^"]+)"', decide_step), [field(self.jobs["checks"], "name")])
        self.assertIn("      skip: ${{ steps.decide.outputs.skip }}\n      prefix: ${{ steps.lookup.outputs.prefix }}\n"
                      "      fail_key: ${{ steps.lookup.outputs.fail_key }}\n"
                      "      save: ${{ steps.lookup.outputs.save }}\n", job)

    def test_the_keys_are_the_tree_of_the_commit_under_test_and_the_repository_salt(self) -> None:
        self.assertRegex(self.text, r"(?m)^env:\n(?:  #.*\n)*  TREE_PASS_SALT: \$\{\{ vars\.TREE_PASS_SALT \}\}$")
        for job, step in [("tree-marker", "Compute the keys from the tree under test"),
                          ("tree-marker-save", "Check that every marked job passed, and write the marker")]:
            with self.subTest(job=job):
                text = step_named(self.jobs[job], step)
                self.assertIn('tree="$(git rev-parse "${GITHUB_SHA}^{tree}")"', text)
                self.assertIn('--salt "${TREE_PASS_SALT}" --tree "${tree}"', text)
                self.assertIn('--workflow-ref "${GITHUB_WORKFLOW_REF}"', text)
        self.assertIn("--live-labels", step_named(self.jobs["tree-marker"], "Compute the keys from the tree under test"))

    def test_no_run_script_of_the_marker_interpolates_an_expression(self) -> None:
        for name in ["tree-marker", "tree-marker-save", "tree-marker-fail", "checks"]:
            for step in steps_of(self.jobs[name]):
                run = step.split("        run: ", 1)[1] if "        run: " in step else ""
                with self.subTest(job=name, step=step.splitlines()[0]):
                    self.assertNotIn("${{", run)


class L3WorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.jobs = jobs(L3_WORKFLOW)
        self.text = L3_WORKFLOW.read_text(encoding="utf-8")

    def test_the_jobs_the_decision_asks_for_are_named_as_github_names_them(self) -> None:
        # jobs_problem compares names exactly, so a name GitHub never reports
        # would refuse every skip. The collation job's line reads
        # "Collate the suite into #473's tables", and YAML ends the name at " #".
        names = {yaml_name(job) for job in self.jobs.values()}
        self.assertEqual(yaml_name(self.jobs["collate"]), COLLATE_JOB)
        self.assertNotIn("#", COLLATE_JOB)
        asked = asked_jobs(step_named(self.jobs["plan"], "Decide whether this run skips the suite"))
        self.assertEqual(asked, [SAVE_JOB, COLLATE_JOB, COLLATE_JOB])
        self.assertLessEqual(set(asked), names)

    def test_the_plan_looks_the_pinned_tree_up_and_never_fails_on_it(self) -> None:
        plan = self.jobs["plan"]
        self.assertEqual(field(plan, "runs-on"), "${{ github.event_name == 'pull_request' && 'ubuntu-24.04' || "
                         "'blacksmith-2vcpu-ubuntu-2404' }}")
        self.assertEqual(field(self.jobs["tree-marker-save"], "runs-on"), "ubuntu-24.04")
        self.assertIn("      actions: read\n      pull-requests: read\n", plan)
        pin = step_named(plan, "Require the pinned commit to be the event's")
        self.assertEqual(step_field(pin, "run"), 'test "$(git rev-parse HEAD)" = "${GITHUB_SHA}"')
        self.assertEqual(step_field(pin, "continue-on-error"), "true")
        # Right after the checkout, before any script from it; only for a
        # pull request (the weekly run checks out 3.x.x, not GITHUB_SHA).
        names = [step.splitlines()[0] for step in steps_of(plan)]
        self.assertEqual(names[:2], ["      - name: Check out the ref under test",
                                     "      - name: Require the pinned commit to be the event's"])
        self.assertEqual(step_field(pin, "if"), "github.event_name == 'pull_request'")
        lookup_step = step_named(plan, "Compute the pass marker's key from the pinned tree")
        self.assertEqual(step_field(lookup_step, "if"), "steps.marker-pin.outcome == 'success'")
        for argument in ['--tree "${TREE}"', "--live-labels", "--skip-labeled", '--tested-sha "${SHA}"',
                         '--event-sha "${GITHUB_SHA}"', '--salt "${TREE_PASS_SALT}"']:
            self.assertIn(argument, lookup_step)
        self.assertEqual(step_env(lookup_step)["TREE"], "${{ steps.plan.outputs.tree }}")
        self.assertEqual(step_env(lookup_step)["SHA"], "${{ steps.plan.outputs.sha }}")
        restore = step_named(plan, "Look the pass marker up")
        self.assertIn("uses: actions/cache/restore@v6\n", restore)
        self.assertIn("          path: tree-pass-marker\n          key: ${{ steps.marker-lookup.outputs.prefix }}\n"
                      "          restore-keys: ${{ steps.marker-lookup.outputs.prefix }}\n          lookup-only: true\n",
                      restore)
        promotion = step_named(plan, "Check the tag's promotion for this tree")
        self.assertIn("python3 scripts/prism_l3_promote.py", promotion)
        self.assertEqual(step_field(promotion, "if"), "steps.marker-cache.outputs.cache-matched-key != ''")
        decide_step = step_named(plan, "Decide whether this run skips the suite")
        budget = 0
        for step, minutes in [(pin, "1"), (lookup_step, "2"), (restore, "2"), (promotion, "3"), (decide_step, "3")]:
            self.assertEqual(step_field(step, "continue-on-error"), "true")
            self.assertEqual(step_field(step, "timeout-minutes"), minutes)
            budget += int(minutes)
        self.assertLessEqual(budget + 5, int(field(plan, "timeout-minutes")))
        self.assertIn("      skip: ${{ steps.marker-decide.outputs.skip }}\n"
                      "      marker_prefix: ${{ steps.marker-lookup.outputs.prefix }}\n"
                      "      marker_save: ${{ steps.marker-lookup.outputs.save }}\n", plan)

    def test_the_l3_decide_step_reads_exactly_the_lookup_steps_outputs(self) -> None:
        decide_step = step_named(self.jobs["plan"], "Decide whether this run skips the suite")
        self.assertEqual(step_env(decide_step), {
            "GH_TOKEN": "${{ github.token }}",
            "LOOKUP": "${{ steps.marker-lookup.outputs.lookup }}",
            "PREFIX": "${{ steps.marker-lookup.outputs.prefix }}",
            "OUTCOME": "${{ steps.marker-cache.outcome }}",
            "PRIMARY_KEY": "${{ steps.marker-cache.outputs.cache-primary-key }}",
            "MATCHED_KEY": "${{ steps.marker-cache.outputs.cache-matched-key }}",
            "HEAD_REPO": "${{ github.event.pull_request.head.repo.full_name }}",
            "HEAD_REF": "${{ github.event.pull_request.head.ref }}",
            "HEAD_SHA": "${{ github.event.pull_request.head.sha }}",
            "PR": "${{ github.event.pull_request.number }}",
            "TREE": "${{ steps.plan.outputs.tree }}",
        })
        # Finding 1: a moved merge ref never runs the decision either.
        self.assertEqual(step_field(decide_step, "if"), "steps.marker-pin.outcome == 'success'")
        self.assertIn('--pr "${PR}"', decide_step)
        self.assertIn('--promotion "${RUNNER_TEMP}/marker-promotion.json" --tree "${TREE}"', decide_step)
        self.assertIn('--out "${RUNNER_TEMP}/marker-promotion.json"',
                      step_named(self.jobs["plan"], "Check the tag's promotion for this tree"))
        names = re.findall(r'--require-job "([^"]+)"', decide_step)
        self.assertEqual(names, [yaml_name(self.jobs["tree-marker-save"]), yaml_name(self.jobs["collate"])])
        self.assertEqual(re.findall(r'--latest-job "([^"]+)"', decide_step), [yaml_name(self.jobs["collate"])])

    def test_the_expensive_jobs_skip_on_the_marker_and_nothing_else_changes(self) -> None:
        gate = "needs.plan.outputs.skip != 'true'"
        self.assertEqual(field(self.jobs["build"], "if"),
                         "${{ needs.plan.outputs.promote != 'true' && " + gate + " }}")
        self.assertEqual(field(self.jobs["collate"], "if"),
                         "${{ !cancelled() && needs.plan.result == 'success' && needs.plan.outputs.promote != 'true' && "
                         + gate + " }}")
        # The preset matrix inherits the build's skip; it is never required.
        self.assertIsNone(field(self.jobs["run"], "if"))
        self.assertEqual(needs_of(self.jobs["run"]), ["plan", "build"])
        self.assertEqual(field(self.jobs["promote"], "if"), "${{ needs.plan.outputs.promote == 'true' }}")

    def test_the_save_job_needs_a_passing_verdict_for_this_tree(self) -> None:
        job = self.jobs["tree-marker-save"]
        self.assertEqual(needs_of(job), ["plan", "build", "run", "collate"])
        self.assertEqual(field(job, "if"),
                         "needs.plan.outputs.marker_save == 'true' && needs.plan.outputs.skip == 'false'")
        step = step_named(job, "Check that the suite passed, and write the marker")
        self.assertIn("--require collate.status=0 --require collate.verdict=true", step)
        self.assertIn("--marker-job plan \\\n", step)
        self.assertIn('tree="$(git rev-parse "${GITHUB_SHA}^{tree}")"', step)
        self.assertEqual(step_env(step)["PREFIX"], "${{ needs.plan.outputs.marker_prefix }}")
        collate = self.jobs["collate"]
        self.assertIn("      status: ${{ steps.collate.outputs.status }}\n", collate)
        self.assertIn("      verdict: ${{ steps.collate.outputs.verdict }}\n", collate)
        upload = step_named(job, "Upload the marker's run record")
        self.assertIn("          name: ${{ steps.save.outputs.artifact }}\n", upload)
        save = step_named(job, "Save the marker")
        self.assertEqual(step_field(save, "if"),
                         "steps.save.outputs.save == 'true' && steps.upload.outcome == 'success'")
        self.assertIn("          path: tree-pass-marker\n          key: ${{ steps.save.outputs.key }}\n", save)
        self.assertRegex(self.text, r"(?m)^  TREE_PASS_SALT: \$\{\{ vars\.TREE_PASS_SALT \}\}$")

    def test_the_nightly_and_the_runner_probe_never_skip(self) -> None:
        for name in ["prism-load-nightly.yml", "prism-load-runner-probe.yml"]:
            with self.subTest(workflow=name):
                text = (ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
                self.assertNotIn("tree_pass_marker", text)
                self.assertNotIn("TREE_PASS_SALT", text)


FAKE_GH = """
import json, os, re, sys
from urllib.parse import unquote
world = json.load(open(os.environ["FAKE_GH_WORLD"], encoding="utf-8"))
args = sys.argv[1:]
if args[:2] == ["run", "download"]:
    run_id, directory = args[2], args[args.index("-D") + 1]
    verdict = world.get("verdicts", {}).get(run_id)
    if verdict is None or run_id in world.get("download_fails", []):
        sys.exit(1)
    with open(os.path.join(directory, "verdict.json"), "w", encoding="utf-8") as handle:
        handle.write(verdict if isinstance(verdict, str) else json.dumps(verdict))
    sys.exit(0)
if args[0] != "api" or world.get("down"):
    sys.exit(1)
path = args[1].split("repos/%s/" % world["repo"], 1)[1]
def out(value):
    print(json.dumps(value))
    sys.exit(0)
def page(items, number):
    return items[(int(number) - 1) * 100:int(number) * 100]
if m := re.fullmatch(r"pulls/(\\d+)", path):
    if world["labels"] is None:
        sys.exit(1)
    out({"labels": [{"name": n} for n in world["labels"]]})
if m := re.fullmatch(r"pulls\\?head=([^&]+)&state=all&per_page=2", path):
    owner, _, ref = unquote(m.group(1)).partition(":")
    out([p for p in world["pulls"] if p["head"]["ref"] == ref][:2])
if m := re.fullmatch(r"issues/(\\d+)/timeline\\?per_page=100&page=(\\d+)", path):
    out(page(world["timeline"], m.group(2)))
if m := re.fullmatch(r"issues/(\\d+)/events\\?per_page=100&page=(\\d+)", path):
    out(page(world.get("events", []), m.group(2)))
if m := re.fullmatch(r"actions/caches\\?key=([^&]+)&per_page=100&page=(\\d+)", path):
    found = [c for c in world["caches"] if c["key"].startswith(unquote(m.group(1)))]
    out({"total_count": len(found), "actions_caches": page(found, m.group(2))})
if m := re.fullmatch(r"actions/artifacts\\?name=(prism-l3-verdict-[0-9a-f]+)&per_page=100", path):
    out({"artifacts": world.get("verdict_artifacts", [])})
if m := re.fullmatch(r"actions/artifacts\\?name=([^&]+)&per_page=100&page=(\\d+)", path):
    found = [a for a in world.get("fail_records", []) if a["name"] == unquote(m.group(1))]
    out({"total_count": len(found), "artifacts": page(found, m.group(2))})
if m := re.fullmatch(r"actions/runs/(\\d+)", path):
    if m.group(1) not in world["runs"] or m.group(1) in world.get("lookup_fails", []):
        sys.exit(1)
    out(world["runs"][m.group(1)])
if m := re.fullmatch(r"git/commits/([0-9a-f]+)", path):
    out({"sha": m.group(1), "tree": {"sha": world["commits"][m.group(1)]}})
if m := re.fullmatch(r"actions/runs/(\\d+)/artifacts\\?name=([^&]+)&per_page=100", path):
    found = [a for a in world["artifacts"].get(m.group(1), []) if a["name"] == m.group(2)]
    out({"total_count": len(found), "artifacts": found})
if m := re.fullmatch(r"actions/runs/(\\d+)/attempts/(\\d+)/jobs\\?per_page=100", path):
    jobs = world["attempt_jobs"].get(m.group(1) + "/" + m.group(2), [])
    out({"total_count": len(jobs), "jobs": jobs})
if m := re.fullmatch(r"actions/runs/(\\d+)/jobs\\?filter=latest&per_page=100", path):
    jobs = world["latest_jobs"].get(m.group(1), [])
    out({"total_count": len(jobs), "jobs": jobs})
sys.exit(1)
"""


def run_block(step: str) -> str:
    """The `run:` script of a step, as the runner hands it to bash."""
    lines = step.splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("        run: "))
    first = lines[start][len("        run: "):]
    if first != "|":
        return first
    body = []
    for line in lines[start + 1:]:
        if line.strip() and not line.startswith("          "):
            break
        body.append(line[10:])
    return "\n".join(body).rstrip("\n") + "\n"


def resolve(value: str, context: dict[str, str]) -> str:
    return re.sub(r"\$\{\{ (.*?) \}\}", lambda m: context[m.group(1)], value)


class WorkflowStepTests(unittest.TestCase):
    """The marker steps' real `run:` blocks, from the workflow files, under
    bash -eo pipefail with the runner's environment and a fake `gh` that
    answers the REST calls from a canned world (finding 3)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = Path(tempfile.mkdtemp())
        cls.workspace = cls.tmp / "workspace"
        (cls.workspace / "scripts").mkdir(parents=True)
        for name in ["tree_pass_marker.py", "prism_l3_promote.py"]:
            shutil.copy2(ROOT / "scripts" / name, cls.workspace / "scripts" / name)
        # A commit made with plumbing: `git commit` would follow the user's
        # signing configuration and ask a signer for a signature.
        git_env = {**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}

        def git(*args: str, stdin: str | None = None) -> str:
            return subprocess.run(["git", "-C", str(cls.workspace), *args], check=True, env=git_env, text=True,
                                  input=stdin, capture_output=True).stdout.strip()

        git("init", "-q")
        git("add", ".")
        tree = git("write-tree")
        cls.sha = git("hash-object", "-t", "commit", "-w", "--stdin", stdin=(
            f"tree {tree}\nauthor Test <test@example.invalid> 0 +0000\n"
            "committer Test <test@example.invalid> 0 +0000\n\nfixture\n"))
        git("update-ref", "HEAD", cls.sha)
        cls.tree = subprocess.check_output(["git", "-C", str(cls.workspace), "rev-parse", "HEAD^{tree}"], text=True,
                                           env=git_env).strip()
        cls.empty = cls.tmp / "no-checkout"
        cls.empty.mkdir()
        cls.bin = cls.tmp / "bin"
        cls.bin.mkdir()
        (cls.bin / "gh").write_text(f"#!{sys.executable}\n{FAKE_GH}", encoding="utf-8")
        (cls.bin / "gh").chmod(0o755)
        (cls.bin / "python3").symlink_to(sys.executable)
        cls.ci, cls.l3 = jobs(CI_WORKFLOW), jobs(L3_WORKFLOW)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmp)

    def setUp(self) -> None:
        for name in ["tree-pass-marker", "tree-fail-marker"]:
            shutil.rmtree(self.workspace / name, ignore_errors=True)

    def world(self, prefix: str, path: str, gate: str) -> dict:
        key = f"{prefix}{WRITER}-1"
        jobs_ = [job(SAVE_JOB), job(gate), job("Rust tests")]
        return {
            "repo": REPO, "labels": [], "pulls": [pull(PR)], "timeline": [{"event": "labeled"}], "caches": [],
            "runs": {str(WRITER): {"id": WRITER, "repository": {"full_name": REPO}, "path": path,
                                   "event": "pull_request", "head_repository": {"full_name": REPO},
                                   "head_branch": HEAD_REF, "head_commit": {"id": HEAD_SHA, "tree_id": HEAD_TREE},
                                   "run_attempt": 1, "status": "completed", "conclusion": "failure",
                                   "created_at": RUN_CREATED, "run_started_at": RUN_STARTED,
                                   "updated_at": RUN_UPDATED,
                                   "html_url": "https://example.invalid/500", "pull_requests": []}},
            "commits": {HEAD_SHA: HEAD_TREE},
            "artifacts": {str(WRITER): [{"name": marker.artifact_name(key), "expired": False,
                                         "workflow_run": {"id": WRITER}}]},
            "attempt_jobs": {f"{WRITER}/1": jobs_},
            "latest_jobs": {str(WRITER): copy_jobs(jobs_)},
        }

    def step(self, workflow: dict[str, str], job_name: str, step_name: str, context: dict[str, str],
             event: str = "pull_request", workflow_file: str = "ci.yml", world: dict | None = None,
             sha: str | None = None, extra_env: dict[str, str] | None = None,
             workspace: Path | None = None) -> tuple[int, dict[str, str], str]:
        text = step_named(workflow[job_name], step_name)
        out = self.tmp / "github-output"
        out.write_text("", encoding="utf-8")
        (self.tmp / "world.json").write_text(json.dumps(world or {"repo": REPO, "labels": []}), encoding="utf-8")
        env = {name: value for name, value in os.environ.items() if not name.startswith("GITHUB_")}
        env.update({
            "PATH": f"{self.bin}:{os.environ['PATH']}", "FAKE_GH_WORLD": str(self.tmp / "world.json"),
            "GIT_CONFIG_GLOBAL": "/dev/null", "GITHUB_SHA": sha or self.sha, "GITHUB_REPOSITORY": REPO,
            "GITHUB_EVENT_NAME": event, "GITHUB_RUN_ID": str(CURRENT), "GITHUB_RUN_ATTEMPT": "1",
            "GITHUB_WORKFLOW_REF": f"{REPO}/.github/workflows/{workflow_file}@refs/pull/{PR}/merge",
            "GITHUB_WORKSPACE": str(workspace or self.workspace), "GITHUB_OUTPUT": str(out),
            "GITHUB_STEP_SUMMARY": str(self.tmp / "summary"), "RUNNER_TEMP": str(self.tmp), "TREE_PASS_SALT": "",
        })
        env.update({name: resolve(value, context) for name, value in step_env(text).items()})
        env.update(extra_env or {})
        proc = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", run_block(text)],
                              cwd=workspace or self.workspace, env=env, capture_output=True, text=True)
        outputs = dict(line.split("=", 1) for line in out.read_text(encoding="utf-8").splitlines() if "=" in line)
        return proc.returncode, outputs, proc.stdout + proc.stderr

    def event(self, event: str = "pull_request", payload: tuple = (), head: str = REPO,
              action: str = "synchronize") -> dict[str, str]:
        pr = event == "pull_request"
        return {
            "github.token": "fake", "github.event.action": action if pr else "",
            "github.event.pull_request.head.repo.full_name": head if pr else "",
            "github.event.pull_request.number": str(PR) if pr else "",
            "github.event.pull_request.head.ref": HEAD_REF if pr else "",
            "github.event.pull_request.head.sha": HEAD_SHA if pr else "",
            "toJSON(github.event.pull_request.labels.*.name)": json.dumps(list(payload), indent=2) if pr else "[]",
            "github.repository": REPO, "github.run_id": str(CURRENT),
        }

    def ci_marker(self, event: str = "pull_request", payload: tuple = (), labels=(), cache: str = "hit",
                  fail_hit: bool = False, edit=None, head: str = REPO, checkout: bool = True) -> dict:
        context = self.event(event, payload, head)
        lookup_out: dict[str, str] = {}
        if checkout:
            code, lookup_out, log = self.step(self.ci, "tree-marker", "Compute the keys from the tree under test",
                                              context, event, world={"repo": REPO, "labels": labels})
            self.assertEqual(code, 0, log)
        context.update({f"steps.lookup.outputs.{name}": lookup_out.get(name, "")
                        for name in ["lookup", "prefix", "fail_key", "save"]})
        prefix = lookup_out.get("prefix", "")
        looked = lookup_out.get("lookup") == "true"
        context.update({
            "steps.cache.outcome": ("failure" if cache == "error" else "success") if looked else "skipped",
            "steps.cache.outputs.cache-primary-key": prefix if looked and cache != "error" else "",
            "steps.cache.outputs.cache-matched-key": f"{prefix}{WRITER}-1" if looked and cache == "hit" else "",
            "steps.fail.outcome": "success" if looked else "skipped",
            "steps.fail.outputs.cache-hit": "true" if looked and fail_hit else "",
        })
        world = self.world(prefix, CI_PATH, GATE_JOB)
        if edit:
            edit(world)
        code, decided, log = self.step(self.ci, "tree-marker", "Decide whether this run skips", context, event,
                                       world=world)
        self.assertEqual(code, 0, log)
        return {"lookup": lookup_out, "skip": decided.get("skip", ""), "log": log}

    def test_ci_marker_job(self) -> None:
        self.assertEqual(self.ci_marker()["skip"], "true")
        for name, kwargs in {
            "cache miss": {"cache": "miss"}, "cache error": {"cache": "error"},
            "cache unavailable": {"cache": "unavailable"}, "fail marker hit": {"fail_hit": True},
            "fail marker only in the cache service's list": {"edit": lambda w: w["caches"].append(
                {"key": fail(tree=self.tree)})},
            "no artifact": {"edit": lambda w: w["artifacts"].update({str(WRITER): []})},
            "failed re-run": {"edit": lambda w: w["latest_jobs"][str(WRITER)][1].update({"conclusion": "failure"})},
            "another head tree": {"edit": lambda w: w["runs"][str(WRITER)]["head_commit"].update(
                {"tree_id": "1" * 40})},
            "a second pull request from the head branch": {"edit": lambda w: w["pulls"].append(pull(841, state="closed"))},
            "a retargeted pull request": {"edit": lambda w: w["timeline"].append({"event": "base_ref_changed"})},
            "retargeted by GitHub after its base merged": {"edit": lambda w: w["timeline"].append(
                {"event": "automatic_base_change_succeeded"})},
            "a base change only in the issue events": {"edit": lambda w: w.update(
                {"events": [{"event": "automatic_base_change_succeeded"}, {"event": "labeled"}]})},
            "base force-pushed": {"edit": lambda w: w["timeline"].append({"event": "base_ref_force_pushed"})},
            "head branch deleted": {"edit": lambda w: w["timeline"].append({"event": "head_ref_deleted"})},
            "head branch restored, only in the issue events": {"edit": lambda w: w.update(
                {"events": [{"event": "labeled"}, {"event": "head_ref_restored"}]})},
            "a run created in this pull request's second": {"edit": lambda w: w["runs"][str(WRITER)].update(
                {"created_at": PR_CREATED})},
            "GitHub API down": {"edit": lambda w: w.update({"down": True})},
            "run-full-ci in the event": {"payload": ("run-full-ci",)},
            "run-full-ci applied since": {"labels": ["run-full-ci"]},
            "labels unreadable": {"labels": None},
            "checkout failed": {"checkout": False},
        }.items():
            with self.subTest(name):
                self.assertEqual(self.ci_marker(**kwargs)["skip"], "false")
        for event in ["push", "workflow_dispatch"]:
            with self.subTest(event):
                result = self.ci_marker(event=event)
                self.assertEqual((result["lookup"]["lookup"], result["lookup"]["save"], result["skip"]),
                                 ("false", "false", "false"))
        result = self.ci_marker(head="someone/fork")
        self.assertEqual((result["lookup"]["lookup"], result["lookup"]["save"]), ("false", "false"))

    def marker_needs(self, skip: str, results: dict[str, str] | None = None, marker_result: str = "success",
                     prefix: str = "", save: str = "true", fail_key: str = "") -> dict:
        names = ["lint-compile-compose", "python-tests", "rust-tests", "rust-fmt", "rust-clippy", "cargo-deny",
                 "dockerfile-lint", "prism-dual-writer", "prism-native-postgres", "prism-integration-proof",
                 "checks", "ckpool-rejects-observability", "docker-builds", "qbit-runtime-image"]
        value = {"tree-marker": {"result": marker_result, "outputs": {
            "skip": skip, "prefix": prefix, "save": save, "fail_key": fail_key}}}
        value.update({name: {"result": "success", "outputs": {}} for name in names})
        for name, result in (results or {}).items():
            value[name]["result"] = result
        return value

    def test_ci_aggregate_step(self) -> None:
        if shutil.which("jq") is None:
            if os.environ.get("CI"):
                self.fail("jq is required to run the aggregate step")
            self.skipTest("jq is not installed")
        marked = {name: "skipped" for name in ["lint-compile-compose", "rust-tests", "rust-fmt", "rust-clippy",
                                               "cargo-deny", "dockerfile-lint", "prism-dual-writer",
                                               "prism-integration-proof"]}
        for name, needs_, expected in [
            ("marker skip", self.marker_needs("true", marked), 0),
            ("skip=true from a failed marker job", self.marker_needs("true", marked, marker_result="failure"), 1),
            ("marker skip with a matrix job skipped", self.marker_needs("true", {**marked, "python-tests": "skipped"}), 1),
            ("full run", self.marker_needs("false"), 0),
            ("full run with a skip", self.marker_needs("false", {"rust-tests": "skipped"}), 1),
            ("push: marker job skipped", self.marker_needs("", marker_result="skipped"), 0),
        ]:
            with self.subTest(name):
                gate_needs = {k: v for k, v in needs_.items()
                              if k in needs_of(self.ci["checks"])}
                code, _, log = self.step(self.ci, "checks", "Require every check to pass",
                                         {"toJSON(needs)": json.dumps(gate_needs)})
                self.assertEqual(code != 0, expected == 1, log)

    def ci_save(self, needs_: dict, event: str = "pull_request") -> tuple[dict[str, str], dict | None]:
        outputs = needs_["tree-marker"]["outputs"]
        context = {"toJSON(needs)": json.dumps(needs_), "needs.tree-marker.outputs.skip": outputs["skip"],
                   "needs.tree-marker.outputs.save": outputs["save"], "needs.tree-marker.outputs.prefix": outputs["prefix"]}
        code, out, log = self.step(self.ci, "tree-marker-save", "Check that every marked job passed, and write the marker",
                                   context, event)
        self.assertEqual(code, 0, log)
        path = self.workspace / "tree-pass-marker" / "marker.json"
        return out, json.loads(path.read_text()) if path.exists() else None

    def test_ci_save_step(self) -> None:
        here = prefix(tree=self.tree)
        out, recorded = self.ci_save(self.marker_needs("false", prefix=here))
        key = f"{here}{CURRENT}-1"
        self.assertEqual(out, {"save": "true", "key": key, "artifact": marker.artifact_name(key)})
        self.assertEqual(recorded["tree"], self.tree)
        for name, needs_, event in [
            ("skipped run", self.marker_needs("true", prefix=here), "pull_request"),
            ("a marked job failed", self.marker_needs("false", {"docker-builds": "failure"}, prefix=here), "pull_request"),
            ("another tree's prefix", self.marker_needs("false", prefix=prefix(tree=OTHER_TREE)), "pull_request"),
            ("push", self.marker_needs("false", prefix=here), "push"),
        ]:
            with self.subTest(name):
                self.setUp()
                out, recorded = self.ci_save(needs_, event)
                self.assertEqual((out["save"], recorded), ("false", None))

    def ci_fail(self, needs_: dict, event: str = "pull_request", checkout: bool = True) -> tuple[int, dict[str, str]]:
        self.setUp()
        outputs = needs_["tree-marker"]["outputs"]
        context = {"toJSON(needs)": json.dumps(needs_), "github.event.pull_request.head.repo.full_name": REPO,
                   "needs.tree-marker.result": needs_["tree-marker"]["result"],
                   "needs.tree-marker.outputs.fail_key": outputs.get("fail_key", "")}
        workspace = self.workspace if checkout else self.empty
        shutil.rmtree(workspace / "tree-fail-marker", ignore_errors=True)
        code, out, log = self.step(self.ci, "tree-marker-fail", "Check which marked jobs failed, and write the fail marker",
                                   context, event, workspace=workspace)
        recorded = workspace / "tree-fail-marker" / "fail.json"
        if out.get("save") == "true":
            self.assertEqual(json.loads(recorded.read_text())["key"], out["key"], log)
        return code, out

    def test_ci_fail_step(self) -> None:
        here = fail(tree=self.tree)
        marker_key_ = {"fail_key": here}
        for name, needs_, checkout in [
            ("a failed job", self.marker_needs("false", {"rust-clippy": "failure"}, **marker_key_), True),
            ("a timed-out shard (cancelled)", self.marker_needs("false", {"prism-native-postgres": "cancelled"},
                                                                 **marker_key_), True),
            ("a job outside checks", self.marker_needs("false", {"qbit-runtime-image": "failure"}, **marker_key_), True),
            # Finding 3: the marker job's key needs no checkout here.
            ("the marker job's key, no checkout", self.marker_needs("false", {"rust-tests": "failure"}, **marker_key_),
             False),
            ("the marker job never finished: own checkout", self.marker_needs("", {"checks": "failure"},
                                                                              marker_result="failure", save=""), True),
            ("the marker job succeeded without a key: own checkout",
             self.marker_needs("", {"checks": "failure"}, save=""), True),
            ("the marker job's key is malformed: own checkout",
             self.marker_needs("false", {"checks": "failure"}, fail_key="tree-fail-v1-x"), True),
        ]:
            with self.subTest(name):
                # The record's name, from bash's sha256sum, is the one decide asks for.
                self.assertEqual(self.ci_fail(needs_, checkout=checkout),
                                 (0, {"save": "true", "key": here, "artifact": marker.fail_record_name(here)}))
        for name, needs_, event in [
            ("only the marker job failed", self.marker_needs("false", marker_result="failure", **marker_key_),
             "pull_request"),
            ("nothing failed", self.marker_needs("false", **marker_key_), "pull_request"),
        ]:
            with self.subTest(name):
                self.assertEqual(self.ci_fail(needs_, event), (0, {"save": "false"}))
        # No key from the marker job and no checkout: the step fails and records nothing.
        code, out = self.ci_fail(self.marker_needs("", {"checks": "failure"}, marker_result="failure", save=""),
                                 checkout=False)
        self.assertNotEqual(code, 0)
        self.assertNotIn("save", out)

    def l3_plan(self, action: str = "synchronize", payload: tuple = (), promotion: dict | str | None = None,
                sha: str | None = None, edit=None) -> dict:
        context = self.event("pull_request", payload, action=action)
        context.update({"steps.plan.outputs.sha": self.sha, "steps.plan.outputs.tree": self.tree})
        pin, _, _ = self.step(self.l3, "plan", "Require the pinned commit to be the event's", context,
                              workflow_file="prism-load-l3.yml", sha=sha)
        lookup_out: dict[str, str] = {}
        if pin == 0:
            code, lookup_out, log = self.step(self.l3, "plan", "Compute the pass marker's key from the pinned tree",
                                              context, workflow_file="prism-load-l3.yml", sha=sha,
                                              world={"repo": REPO, "labels": []})
            self.assertEqual(code, 0, log)
        prefix = lookup_out.get("prefix", "")
        looked = lookup_out.get("lookup") == "true"
        context.update({f"steps.marker-lookup.outputs.{name}": lookup_out.get(name, "")
                        for name in ["lookup", "prefix", "save"]})
        context.update({"steps.marker-cache.outcome": "success" if looked else "skipped",
                        "steps.marker-cache.outputs.cache-primary-key": prefix if looked else "",
                        "steps.marker-cache.outputs.cache-matched-key": f"{prefix}{WRITER}-1" if looked else ""})
        promotion_file = self.tmp / "marker-promotion.json"
        promotion_file.unlink(missing_ok=True)
        world = self.world(prefix, L3_PATH, COLLATE_JOB)
        if edit:
            edit(world)
        if looked and promotion == "real":
            # The real "Check the tag's promotion" step (if: a matched key),
            # with its own --run-id, against the world's verdict artifacts.
            code, _, log = self.step(self.l3, "plan", "Check the tag's promotion for this tree", context,
                                     workflow_file="prism-load-l3.yml", world=world, sha=sha)
            self.assertEqual(code, 0, log)
        elif looked and promotion is not None:
            promotion_file.write_text(json.dumps(promotion), encoding="utf-8")
        decided: dict[str, str] = {}
        if pin == 0:  # if: steps.marker-pin.outcome == 'success'
            code, decided, log = self.step(self.l3, "plan", "Decide whether this run skips the suite", context,
                                           workflow_file="prism-load-l3.yml", world=world, sha=sha)
            self.assertEqual(code, 0, log)
        decision = json.loads(promotion_file.read_text()) if promotion_file.exists() else None
        return {"lookup": lookup_out, "skip": decided.get("skip", ""), "pin": pin, "decided": decided,
                "promotion": decision}

    def test_l3_real_promotion_step(self) -> None:
        # Finding 4: the real promotion script, run by the workflow's step
        # with --run-id "${RUN_ID}" (this run, CURRENT).
        def verdict(**overrides) -> dict:
            value = {"schema": "qbit.prism.l3-verdict.v1", "suite": "l3-full", "tree": self.tree,
                     "complete": True, "passed": True}
            value.update(overrides)
            return value

        def artifacts(*runs: tuple[int, str]):
            def edit(w: dict) -> None:
                w["verdict_artifacts"] = [{"workflow_run": {"id": run_id}, "expired": False, "created_at": created}
                                          for run_id, created in runs]
                w.setdefault("verdicts", {})[str(WRITER)] = verdict()
                w["runs"]["600"] = {**w["runs"][str(WRITER)], "id": 600}
            return edit

        def with_(*edits):
            def edit(w: dict) -> None:
                for one in edits:
                    one(w)
            return edit

        writer_only = artifacts((WRITER, "2026-11-10T00:00:00Z"))
        result = self.l3_plan(promotion="real", edit=writer_only)
        self.assertEqual(result["skip"], "true", result)
        self.assertEqual((result["promotion"]["source_run"], result["promotion"]["considered"]), (WRITER, []))
        current_newer = artifacts((CURRENT, "2026-11-11T00:00:00Z"), (WRITER, "2026-11-10T00:00:00Z"))
        for name, edit in {
            "this run's earlier attempt failed": with_(current_newer, lambda w: w["verdicts"].update(
                {str(CURRENT): verdict(passed=False)})),
            "a newer run's lookup failed": with_(artifacts((600, "2026-11-11T00:00:00Z"), (WRITER, "2026-11-10T00:00:00Z")),
                                                 lambda w: w.update({"lookup_fails": ["600"]}),
                                                 lambda w: w["verdicts"].update({"600": verdict(passed=False)})),
            "a newer run's verdict is incomplete": with_(artifacts((600, "2026-11-11T00:00:00Z"),
                                                                   (WRITER, "2026-11-10T00:00:00Z")),
                                                         lambda w: w["verdicts"].update({"600": verdict(complete=False)})),
            "a newer run failed a gate": with_(artifacts((600, "2026-11-11T00:00:00Z"), (WRITER, "2026-11-10T00:00:00Z")),
                                               lambda w: w["verdicts"].update({"600": verdict(passed=False)})),
        }.items():
            with self.subTest(name):
                result = self.l3_plan(promotion="real", edit=edit)
                self.assertEqual(result["skip"], "false", result)
        result = self.l3_plan(promotion="real", edit=with_(current_newer, lambda w: w["verdicts"].update(
            {str(CURRENT): verdict(passed=False)})))
        self.assertIn({"run": CURRENT, "rejected": "is this run (an earlier attempt's verdict)"},
                      result["promotion"]["considered"])

    def test_l3_plan_marker_steps(self) -> None:
        passing = {"tree": self.tree, "promote": True, "source_run": WRITER, "considered": [], "malformed": [],
                   "verdict": {"schema": "qbit.prism.l3-verdict.v1", "suite": "l3-full", "tree": self.tree,
                               "complete": True, "passed": True}}
        self.assertEqual(self.l3_plan(promotion=passing)["skip"], "true")
        failing = json.loads(json.dumps(passing))
        failing["verdict"]["passed"] = False
        for name, kwargs in {
            "newest promotable run failed": {"promotion": failing},
            "promotion of another run": {"promotion": {**passing, "source_run": 77}},
            "no promotion decision": {"promotion": None},
            "saving attempt's collation failed": {"promotion": passing, "edit": lambda w: w["attempt_jobs"][
                f"{WRITER}/1"][1].update({"conclusion": "failure"})},
            "a retargeted pull request": {"promotion": passing,
                                          "edit": lambda w: w["timeline"].append({"event": "base_ref_changed"})},
            "a label event": {"promotion": passing, "action": "labeled", "payload": ("release-candidate",)},
        }.items():
            with self.subTest(name):
                self.assertEqual(self.l3_plan(**kwargs)["skip"], "false")
        moved = self.l3_plan(promotion=passing, sha="0" * 40)
        self.assertNotEqual(moved["pin"], 0)
        self.assertEqual((moved["lookup"], moved["decided"], moved["skip"]), ({}, {}, ""))

    def test_l3_save_step(self) -> None:
        here = prefix(tree=self.tree, workflow="prism-load-l3.yml")
        for name, collate, results, skip, saved in [
            ("full pass with a verdict", {"status": "0", "verdict": "true"}, {}, "false", "true"),
            ("no verdict", {"status": "0"}, {}, "false", "false"),
            ("skipped run", {}, {"build": "skipped", "run": "skipped", "collate": "skipped"}, "true", "false"),
        ]:
            with self.subTest(name):
                self.setUp()
                needs_ = {"plan": {"result": "success", "outputs": {"skip": skip, "marker_save": "true",
                                                                    "marker_prefix": here}},
                          "build": {"result": results.get("build", "success"), "outputs": {}},
                          "run": {"result": results.get("run", "success"), "outputs": {}},
                          "collate": {"result": results.get("collate", "success"), "outputs": collate}}
                context = {"toJSON(needs)": json.dumps(needs_), "needs.plan.outputs.skip": skip,
                           "needs.plan.outputs.marker_save": "true", "needs.plan.outputs.marker_prefix": here}
                code, out, log = self.step(self.l3, "tree-marker-save", "Check that the suite passed, and write the marker",
                                           context, workflow_file="prism-load-l3.yml")
                self.assertEqual(code, 0, log)
                self.assertEqual(out["save"], saved, log)


def copy_jobs(jobs_: list[dict]) -> list[dict]:
    return [dict(entry) for entry in jobs_]


if __name__ == "__main__":
    unittest.main()
