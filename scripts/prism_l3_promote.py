#!/usr/bin/env python3
"""Decide whether a `v*` tag promotes a pull request's L3 run (#550).

The full L3 suite (`suite:l3-full`, `.github/workflows/prism-load-l3.yml`)
runs on the version-bump pull request. When the release is tagged, the
tagged commit's tree is usually the tree that pull request's merge result
ran, so running the suite again measures nothing new (#487 cost lever P1).
This script looks for that run and prints the decision as JSON:

- **promote** the newest run of the L3 workflow that
  - ran on `pull_request` from this repository (not a fork),
  - finished (conclusion `success` or `failure`, never cancelled, skipped or
    timed out),
  - uploaded `prism-l3-verdict-<tree>` for exactly this tree, whose
    `verdict.json` (`qbit.prism.l3-verdict.v1`) is for suite `l3-full` and
    `complete`: every planned run reached its gate. A complete run that
    failed a gate is promoted too, and the tag run then fails with it: the
    same tree would measure the same, and the verdict is carried, not
    dropped;
- otherwise **rerun** the full suite. A lookup that fails for any reason is
  a rerun, never a promotion: promoting needs positive evidence.

The artifact's name carries the tree, so the lookup lists only candidates
for this tree and downloads the newest first.

Usage:
  python3 scripts/prism_l3_promote.py --repo OWNER/NAME --tree TREE \\
      --run-id ID [--out decision.json]

Needs `gh` authenticated with `actions: read` (GH_TOKEN in the workflow).
Exit 0 with a decision either way; 2 on bad arguments.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Callable

WORKFLOW_PATH = ".github/workflows/prism-load-l3.yml"
VERDICT_ARTIFACT = "prism-l3-verdict-{tree}"
VERDICT_SCHEMA = "qbit.prism.l3-verdict.v1"
FULL_SUITE = "l3-full"
FINISHED = ("success", "failure")
TREE = re.compile(r"^[0-9a-f]{40}$")

# gh(args) -> stdout; raises subprocess.CalledProcessError or OSError.
Gh = Callable[[list[str]], str]


def run_gh(args: list[str]) -> str:
    return subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True, timeout=120
    ).stdout


def download_verdict(gh: Gh, repo: str, run_id: int, name: str) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        gh(["run", "download", str(run_id), "--repo", repo, "-n", name, "-D", tmp])
        return json.loads((Path(tmp) / "verdict.json").read_text(encoding="utf-8"))


def run_rejection(run: dict, repo: str) -> str | None:
    """Why `run` cannot be the source of a promotion, or None."""
    if run.get("event") != "pull_request":
        return f"ran on {run.get('event')!r}, not pull_request"
    if not str(run.get("path", "")).startswith(WORKFLOW_PATH):
        return f"is a run of {run.get('path')!r}, not {WORKFLOW_PATH}"
    head_repo = (run.get("head_repository") or {}).get("full_name")
    if head_repo != repo:
        return f"ran from {head_repo!r}, not this repository"
    if run.get("status") != "completed" or run.get("conclusion") not in FINISHED:
        return f"did not finish (status {run.get('status')!r}, conclusion {run.get('conclusion')!r})"
    return None


def verdict_rejection(verdict: object, tree: str) -> str | None:
    """Why the run's `verdict.json` does not show the full suite on `tree`."""
    if not isinstance(verdict, dict):
        return "its verdict.json is not an object"
    if verdict.get("schema") != VERDICT_SCHEMA:
        return f"its verdict has schema {verdict.get('schema')!r}, not {VERDICT_SCHEMA}"
    if verdict.get("tree") != tree:
        return f"its verdict is for tree {verdict.get('tree')!r}"
    if verdict.get("suite") != FULL_SUITE:
        return f"ran suite {verdict.get('suite')!r}, not {FULL_SUITE}"
    if verdict.get("complete") is not True:
        return "did not bring every planned run to its gate"
    if not isinstance(verdict.get("passed"), bool):
        return "its verdict does not say whether the suite passed"
    return None


def decide(gh: Gh, repo: str, tree: str, run_id: int) -> dict:
    """The decision for `tree`: promote a pull request's run, or rerun."""
    name = VERDICT_ARTIFACT.format(tree=tree)
    decision: dict = {"tree": tree, "artifact": name, "promote": False, "considered": []}
    try:
        listing = json.loads(
            gh(["api", f"repos/{repo}/actions/artifacts?name={name}&per_page=100"])
        )
    except (subprocess.CalledProcessError, OSError, subprocess.TimeoutExpired, ValueError) as error:
        decision["reason"] = f"rerun: the artifact lookup failed ({error})"
        return decision
    # An artifact that names no run cannot be traced to one, so it is not a
    # candidate (and never crashes the lookup into a failed tag run).
    artifacts = [
        a for a in listing.get("artifacts", [])
        if isinstance(a, dict)
        and not a.get("expired")
        and isinstance((a.get("workflow_run") or {}).get("id"), int)
        and a["workflow_run"]["id"] != run_id
    ]
    # Newest first; the API already lists them so, but a stale run must
    # never shadow a newer one whatever order it returns.
    artifacts.sort(key=lambda a: a.get("created_at", ""), reverse=True)
    for artifact in artifacts:
        source = artifact["workflow_run"]["id"]
        try:
            run = json.loads(gh(["api", f"repos/{repo}/actions/runs/{source}"]))
        except (subprocess.CalledProcessError, OSError, subprocess.TimeoutExpired, ValueError) as error:
            decision["considered"].append({"run": source, "rejected": f"lookup failed ({error})"})
            continue
        why = run_rejection(run, repo)
        verdict = None
        if why is None:
            try:
                verdict = download_verdict(gh, repo, source, name)
            except (subprocess.CalledProcessError, OSError, subprocess.TimeoutExpired, ValueError) as error:
                why = f"its verdict could not be read ({error})"
            else:
                why = verdict_rejection(verdict, tree)
        if why is not None:
            decision["considered"].append({"run": source, "rejected": why})
            continue
        decision.update(
            promote=True,
            source_run=source,
            source_url=run.get("html_url"),
            source_pull_requests=[pr.get("number") for pr in run.get("pull_requests") or []],
            verdict=verdict,
            reason=f"promote: run {source} ran suite {FULL_SUITE} on this tree and every planned "
            f"run reached its gate ({'passed' if verdict['passed'] else 'not every run passed'})",
        )
        return decision
    decision["reason"] = (
        "rerun: no finished same-repository pull_request run of the full suite on this tree"
    )
    return decision


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--repo", required=True)
    parser.add_argument("--tree", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if not TREE.match(args.tree):
        print(f"prism-l3-promote: --tree {args.tree!r} is not a full tree id", file=sys.stderr)
        return 2
    decision = decide(run_gh, args.repo, args.tree, args.run_id)
    text = json.dumps(decision, indent=2) + "\n"
    if args.out:
        args.out.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
