#!/usr/bin/env python3
"""Skip a workflow's expensive jobs when the tree under test already passed it.

A pull request lands as one signed commit with exactly the tree its review
settled (`squash-sign apply`), and GitHub runs every workflow again on that
commit although nothing in the tree changed. `ci.yml` and `prism-load-l3.yml`
use this script to skip that repeated work. A run that did all of it and passed
records a **pass marker**: an Actions cache entry whose key names the tree and
the run that passed,

    tree-pass-v1-<workflow file>-salt<N>-<tree>-<run id>-<run attempt>

and an artifact of that run named `tree-pass-<sha256 of the key>`. `<tree>` is
the tree of the commit the workflow tests (for `pull_request`, the merge of the
head into the base, `GITHUB_SHA`), never a commit id. `<N>` is the repository
variable `TREE_PASS_SALT` (unset: 1). A `ci.yml` run that failed records a
**fail marker**, `tree-fail-v1-<workflow file>-salt<N>-<tree>`, and a tree with
one never skips again.

A marker is only looked up (`lookup-only`), never restored: actions/cache
extracts an entry with `tar -P`, so restoring one that a changed workflow saved
would let it write files into the job before this script decides. The run is
read from the key the lookup matched, and a skip needs GitHub's own records of
that run to vouch for it (`binding_problem`). The mechanism, the `run-full-ci`
label and when it never skips are in docs/ci-same-tree-skip.md.

Subcommands, in the order a run uses them:

  lookup  whether this run may look a marker up and may record one, the pass
          marker's key prefix and the fail marker's key. Only a
          same-repository `pull_request` run does either; the `run-full-ci`
          label (in the event or on the pull request now) or an unreadable
          label list means no lookup.
  decide  whether this run skips: no fail marker, a pass marker matched under
          exactly this prefix, the run it names vouched for by GitHub (and,
          for L3, a promotion that would carry exactly that run), in this
          pull request alone, whose timeline and issue events show no
          base change and no deletion or restoring of its head branch.
  save    whether this run may record its pass marker (every job it stands
          for succeeded, the run was not itself skipped, and the prefix is this
          checkout's tree), its key, its artifact's name and the marker file.
  fail-key  the fail marker's key, from this checkout's tree: the fail job's
          fallback when the marker job left none. Which jobs failed is a jq
          rule in ci.yml, so recording a failure needs no checkout.

They append `name=value` lines to $GITHUB_OUTPUT when it is set and always exit
0: any input they cannot read and any API call or step that failed is the
answer that runs everything and saves nothing. Exit 2 is a usage error, which a
workflow step also reads as "run everything".
"""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Callable
from urllib.parse import quote

PASS_PREFIX = "tree-pass-v1"
FAIL_PREFIX = "tree-fail-v1"
ARTIFACT_PREFIX = "tree-pass-"
FAIL_RECORD_PREFIX = "tree-fail-"
DEFAULT_SALT = "1"
FORCE_LABEL = "run-full-ci"
MARKER_SCHEMA = "qbit.ci.tree-pass-marker.v2"
MARKER_FILE = "marker.json"
L3_SUITE = "l3-full"
API_TIMEOUT_SECONDS = 30
# Listings are read 100 at a time until an empty page; one with more
# non-empty pages than this is never complete.
PER_PAGE = 100
MAX_PAGES = 10

OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
SALT = re.compile(r"(?:0|[1-9][0-9]{0,8})")
WORKFLOW_FILE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\.ya?ml")
REPOSITORY = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9._-]+")
PR_NUMBER = re.compile(r"[1-9][0-9]{0,9}")
JOB_ID = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")
RUN_ID = re.compile(r"[1-9][0-9]{0,19}")
RUN_ATTEMPT = re.compile(r"[1-9][0-9]{0,4}")
PREFIX = re.compile(
    rf"{PASS_PREFIX}-[A-Za-z0-9][A-Za-z0-9._-]*\.ya?ml-salt(?:0|[1-9][0-9]{{0,8}})-(?:[0-9a-f]{{40}}|[0-9a-f]{{64}})-"
)
FAIL_KEY = re.compile(
    rf"{FAIL_PREFIX}-[A-Za-z0-9][A-Za-z0-9._-]*\.ya?ml-salt(?:0|[1-9][0-9]{{0,8}})-(?:[0-9a-f]{{40}}|[0-9a-f]{{64}})"
)
# Timeline events that mean the base moved under the pull request: retargeted
# by hand (base_ref_changed) or by GitHub after its base was merged and deleted
# (automatic_base_change_succeeded, with no base_ref_changed), force-pushed or
# deleted. The names are undocumented, so every event with either prefix
# counts, known or not.
BASE_CHANGE_EVENT = re.compile(r"(?:base_ref_|automatic_base_change_).*")
# Timeline events that mean this pull request's head branch was deleted (and
# perhaps created again) while it existed: then a run of another pull request
# from that branch, created after this one, may have the head this one has
# now. Exact names: head_ref_force_pushed is routine and does not count.
HEAD_BRANCH_EVENTS = frozenset({"head_ref_deleted", "head_ref_restored"})
# GitHub's timestamps, exactly: ASCII digits, seconds, UTC.
GITHUB_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
REQUIREMENT = re.compile(r"([A-Za-z_][A-Za-z0-9_-]*)\.([A-Za-z_][A-Za-z0-9_-]*)=(.*)")

# gh(args) -> stdout; raises subprocess.CalledProcessError, OSError or
# subprocess.TimeoutExpired.
Gh = Callable[[list[str]], str]
API_ERRORS = (subprocess.CalledProcessError, OSError, subprocess.TimeoutExpired, ValueError)


class Invalid(ValueError):
    """An input this script cannot trust."""


def run_gh(args: list[str]) -> str:
    return subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True, timeout=API_TIMEOUT_SECONDS
    ).stdout


def github_time(value: object) -> datetime | None:
    """A GitHub `YYYY-MM-DDTHH:MM:SSZ` timestamp as a datetime, or None for
    anything else (another format, an impossible date, not a string)."""
    if not isinstance(value, str) or not GITHUB_TIME.fullmatch(value):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def workflow_path(workflow_ref: str, repository: str) -> str:
    """`.github/workflows/ci.yml` from GITHUB_WORKFLOW_REF, `<repository>/.github/workflows/ci.yml@<ref>`."""
    if not REPOSITORY.fullmatch(repository):
        raise Invalid(f"repository {repository!r} is not OWNER/NAME")
    path, at, ref = workflow_ref.partition("@")
    prefix = f"{repository}/"
    if not at or not ref or not path.startswith(prefix + ".github/workflows/"):
        raise Invalid(f"workflow ref {workflow_ref!r} is not a workflow of {repository}")
    path = path[len(prefix):]
    if not WORKFLOW_FILE.fullmatch(path.rsplit("/", 1)[1]) or path.count("/") != 2:
        raise Invalid(f"workflow ref {workflow_ref!r} names no workflow file")
    return path


def workflow_file(workflow_ref: str, repository: str) -> str:
    """`ci.yml`: the workflow file the keys name."""
    return workflow_path(workflow_ref, repository).rsplit("/", 1)[1]


def effective_salt(salt: str) -> str:
    """The repository variable TREE_PASS_SALT, or the default when it is unset."""
    value = salt or DEFAULT_SALT
    if not SALT.fullmatch(value):
        raise Invalid(f"TREE_PASS_SALT {salt!r} is not a non-negative integer")
    return value


def _key_parts(workflow: str, salt: str, tree: str) -> str:
    if not WORKFLOW_FILE.fullmatch(workflow):
        raise Invalid(f"workflow {workflow!r} is not a workflow file name")
    if not SALT.fullmatch(salt):
        raise Invalid(f"salt {salt!r} is not a non-negative integer")
    if not OBJECT_ID.fullmatch(tree):
        raise Invalid(f"tree {tree!r} is not a full tree id")
    return f"{workflow}-salt{salt}-{tree}"


def marker_prefix(workflow: str, salt: str, tree: str) -> str:
    """Every pass marker of this tree starts with this; the run follows it."""
    return f"{PASS_PREFIX}-{_key_parts(workflow, salt, tree)}-"


def fail_key(workflow: str, salt: str, tree: str) -> str:
    return f"{FAIL_PREFIX}-{_key_parts(workflow, salt, tree)}"


def marker_key(prefix: str, run_id: str, run_attempt: str) -> str:
    if not PREFIX.fullmatch(prefix):
        raise Invalid(f"prefix {prefix!r} is not a pass-marker prefix")
    if not (RUN_ID.fullmatch(run_id) and RUN_ATTEMPT.fullmatch(run_attempt)):
        raise Invalid(f"run {run_id!r} attempt {run_attempt!r} is unreadable")
    return f"{prefix}{run_id}-{run_attempt}"


def artifact_name(key: str) -> str:
    """The artifact the run that saved `key` uploaded; the name binds the key."""
    return ARTIFACT_PREFIX + hashlib.sha256(key.encode("utf-8")).hexdigest()


def fail_record_name(key: str) -> str:
    """The artifact a failed run uploads beside its fail marker: a second
    record, in another store, so one failed save never loses the failure."""
    return FAIL_RECORD_PREFIX + hashlib.sha256(key.encode("utf-8")).hexdigest()


def parse_marker_key(prefix: str, matched: str) -> tuple[int, int] | None:
    """(run id, attempt) of a matched key under exactly `prefix`, or None."""
    if not PREFIX.fullmatch(prefix):
        return None
    match = re.fullmatch(
        re.escape(prefix) + r"(?P<run>[1-9][0-9]{0,19})-(?P<attempt>[1-9][0-9]{0,4})", matched
    )
    if match is None:
        return None
    return int(match.group("run")), int(match.group("attempt"))


def parse_labels(text: str) -> list[str]:
    """The event's label names, `toJSON(github.event.pull_request.labels.*.name)`."""
    try:
        labels = json.loads(text)
    except ValueError as error:
        raise Invalid(f"the event's labels are not JSON ({error})") from None
    if not isinstance(labels, list) or not all(isinstance(name, str) for name in labels):
        raise Invalid("the event's labels are not a list of names")
    return labels


def live_labels(gh: Gh, repository: str, pr: str) -> list[str]:
    """The pull request's labels now. A re-run reads the original event, so a
    label applied since is only seen here."""
    try:
        data = json.loads(gh(["api", f"repos/{repository}/pulls/{pr}"]))
    except ValueError as error:
        raise Invalid(f"the pull request is not JSON ({error})") from None
    labels = data.get("labels") if isinstance(data, dict) else None
    if not isinstance(labels, list) or not all(
        isinstance(label, dict) and isinstance(label.get("name"), str) for label in labels
    ):
        raise Invalid("the pull request's labels are unreadable")
    return [label["name"] for label in labels]


def lookup_decision(
    *,
    event: str,
    action: str,
    repository: str,
    head_repository: str,
    pr: str,
    payload_labels: str,
    fetch_live_labels: Callable[[], list[str]] | None,
    workflow_ref: str,
    salt: str,
    tree: str,
    skip_labeled: bool = False,
    tested_sha: str | None = None,
    event_sha: str | None = None,
) -> dict:
    """{lookup, save, prefix, fail_key, reason}: whether to look markers up,
    whether a full run may record one, the pass prefix and the fail key."""
    no = {"lookup": False, "save": False, "prefix": "", "fail_key": ""}
    try:
        workflow = workflow_file(workflow_ref, repository)
        salt = effective_salt(salt)
        keys = {"prefix": marker_prefix(workflow, salt, tree), "fail_key": fail_key(workflow, salt, tree)}
    except Invalid as error:
        return {**no, "reason": f"run everything: no key ({error})"}
    no.update(keys)
    if event != "pull_request":
        return {**no, "reason": f"run everything: a {event or 'unnamed'} event never skips or records"}
    if head_repository != repository:
        return {**no, "reason": f"run everything: a pull request from {head_repository or 'an unknown repository'} "
                f"(a fork) never skips or records"}
    if not PR_NUMBER.fullmatch(pr):
        return {**no, "reason": f"run everything: pull request number {pr!r} is unreadable"}
    if tested_sha is not None:
        # L3 checks out refs/pull/N/merge again, which can have moved since the
        # event. Only when it has not is the running workflow file in the tree
        # the key names.
        if not (OBJECT_ID.fullmatch(tested_sha) and OBJECT_ID.fullmatch(event_sha or "")):
            return {**no, "reason": "run everything: the commit under test or the event's commit is unreadable"}
        if tested_sha != event_sha:
            return {**no, "reason": f"run everything: the commit under test, {tested_sha}, is not the "
                    f"event's {event_sha}"}
    yes_save = {"lookup": False, "save": True, **keys}
    if skip_labeled and action == "labeled":
        return {**yes_save, "reason": "run everything: a label event asked for this run"}
    try:
        labels = parse_labels(payload_labels)
    except Invalid as error:
        return {**yes_save, "reason": f"run everything: {error}"}
    if FORCE_LABEL in labels:
        return {**yes_save, "reason": f"run everything: the event carries the {FORCE_LABEL} label"}
    if fetch_live_labels is None:
        return {**yes_save, "reason": "run everything: the pull request's current labels were not read"}
    try:
        labels = fetch_live_labels()
    except (Invalid, subprocess.CalledProcessError, OSError, subprocess.TimeoutExpired) as error:
        return {**yes_save, "reason": f"run everything: the pull request's labels could not be read ({error})"}
    if FORCE_LABEL in labels:
        return {**yes_save, "reason": f"run everything: the pull request has the {FORCE_LABEL} label"}
    return {"lookup": True, "save": True, **keys, "reason": f"look up {keys['prefix']}*"}


def api_json(gh: Gh, path: str) -> object:
    return json.loads(gh(["api", path]))


def field(value: object, *path: str) -> object:
    for part in path:
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def same(observed: object, expected: object) -> bool:
    # Exact types: a JSON true is never the count 1. Every expected value is
    # validated before it gets here, so none is None.
    return type(observed) is type(expected) and observed == expected


def jobs_problem(listing: object, names: list[str], what: str) -> str | None:
    """Why `listing` (a jobs page) does not show every job in `names`
    completed with conclusion success, or None."""
    jobs = field(listing, "jobs")
    total = field(listing, "total_count")
    # One page of 100: a run with more jobs than that is never vouched for.
    if not isinstance(jobs, list) or not isinstance(total, int) or isinstance(total, bool) or total > len(jobs):
        return f"{what}: the job list is unreadable or incomplete"
    for name in names:
        found = [job for job in jobs if isinstance(job, dict) and job.get("name") == name]
        if not found:
            return f"{what}: no job {name!r}"
        for job in found:
            if job.get("status") != "completed" or job.get("conclusion") != "success":
                return f"{what}: {name!r} is {job.get('status')}/{job.get('conclusion')}"
    return None


def binding_problem(
    gh: Gh,
    *,
    repository: str,
    current_run: int,
    workflow_path_: str,
    head_repository: str,
    head_ref: str,
    head_sha: str,
    writer_run: int,
    writer_attempt: int,
    key: str,
    require_jobs: list[str],
    latest_jobs: list[str],
    pull_request_created: datetime,
) -> str | None:
    """Why the run a marker names cannot vouch for this tree, or None.

    A cache entry is only a claim: any run in the pull request's cache scope
    can save any key, including a run of a workflow an earlier push changed.
    GitHub's records are what is trusted: the run is a pull_request run of this
    workflow file on this head branch whose head commit has this head's tree
    (so it ran the workflow under review), it was created after this pull
    request, not in the same second (so it is no run created before this pull
    request was opened, whatever the pull request listing says), it uploaded
    the artifact named for exactly this key, the attempt that saved it shows
    the save job and the gate succeeded, and no later execution of the gate
    failed.
    """
    if writer_run == current_run:
        return "the marker names this run"
    try:
        run = api_json(gh, f"repos/{repository}/actions/runs/{writer_run}")
        head = api_json(gh, f"repos/{repository}/git/commits/{head_sha}")
    except API_ERRORS as error:
        return f"the run or head lookup failed ({error})"
    head_tree = field(head, "tree", "sha")
    if not isinstance(head_tree, str) or not OBJECT_ID.fullmatch(head_tree):
        return f"the head commit {head_sha} has no readable tree"
    for name, observed, expected in [
        ("id", field(run, "id"), writer_run),
        ("repository", field(run, "repository", "full_name"), repository),
        ("workflow", field(run, "path"), workflow_path_),
        ("event", field(run, "event"), "pull_request"),
        ("head repository", field(run, "head_repository", "full_name"), head_repository),
        ("head branch", field(run, "head_branch"), head_ref),
        ("head tree", field(run, "head_commit", "tree_id"), head_tree),
    ]:
        if not same(observed, expected):
            return f"run {writer_run} {name} is {observed!r}, not {expected!r}"
    # The run record's created_at, which stays the run's first creation
    # through every re-run (an attempt's own record moves; it is not read).
    created = github_time(field(run, "created_at"))
    if created is None:
        return f"run {writer_run} has no readable created_at ({field(run, 'created_at')!r})"
    if created <= pull_request_created:
        return (f"run {writer_run} was created at {field(run, 'created_at')}, not after this pull request "
                f"({pull_request_created:%Y-%m-%dT%H:%M:%SZ})")
    latest_attempt = field(run, "run_attempt")
    if not (isinstance(latest_attempt, int) and not isinstance(latest_attempt, bool)
            and latest_attempt >= writer_attempt):
        return f"run {writer_run} has no attempt {writer_attempt}"
    name = artifact_name(key)
    try:
        listing = api_json(gh, f"repos/{repository}/actions/runs/{writer_run}/artifacts?name={name}&per_page=100")
    except API_ERRORS as error:
        return f"the artifact lookup failed ({error})"
    artifacts = field(listing, "artifacts")
    if not (isinstance(artifacts, list) and len(artifacts) == 1 and same(field(listing, "total_count"), 1)):
        return f"run {writer_run} has no single artifact {name}"
    artifact = artifacts[0]
    if not (same(field(artifact, "name"), name) and field(artifact, "expired") is False
            and same(field(artifact, "workflow_run", "id"), writer_run)):
        return f"run {writer_run}'s artifact {name} is not this key's, or it expired"
    try:
        saving = api_json(gh, f"repos/{repository}/actions/runs/{writer_run}/attempts/{writer_attempt}/jobs?per_page=100")
        latest = api_json(gh, f"repos/{repository}/actions/runs/{writer_run}/jobs?filter=latest&per_page=100")
    except API_ERRORS as error:
        return f"the job lookup failed ({error})"
    return (jobs_problem(saving, require_jobs, f"run {writer_run} attempt {writer_attempt}")
            or jobs_problem(latest, latest_jobs, f"run {writer_run}'s latest jobs"))


def listing(gh: Gh, path: str, items: Callable[[object], object]) -> list:
    """Every item of a paginated listing, `PER_PAGE` at a time, read until an
    empty page: a short page in the middle never ends it. A page that is not a
    list of objects, or more than `MAX_PAGES` non-empty pages, raises Invalid:
    a listing that cannot be read to its end is never taken as complete."""
    found: list = []
    for page in range(1, MAX_PAGES + 2):
        separator = "&" if "?" in path else "?"
        page_items = items(api_json(gh, f"{path}{separator}per_page={PER_PAGE}&page={page}"))
        if not isinstance(page_items, list) or not all(isinstance(item, dict) for item in page_items):
            raise Invalid(f"page {page} of {path} is not a list of objects")
        if not page_items:
            return found
        found.extend(page_items)
    raise Invalid(f"{path} has more than {MAX_PAGES} pages")


def pull_request_problem(gh: Gh, *, repository: str, pr: int, head_repository: str,
                         head_ref: str) -> tuple[str | None, datetime | None]:
    """(Why this pull request cannot trust a run of its head branch, or None;
    when it can, the pull request's own created_at, which binding_problem
    holds the vouching run to.)

    A pull_request run takes its workflow file from the merge commit, and the
    workflow has no say over which base that is. A run of a changed workflow on
    another base (this pull request retargeted for a while, or a second pull
    request from the same head branch) would otherwise pass every check of the
    run record. So: the only pull request ever opened from this head branch is
    this one, and neither its timeline nor its issue events show an event that
    moved its base (BASE_CHANGE_EVENT) or deleted or restored its head branch
    (HEAD_BRANCH_EVENTS: two pull requests open from one branch, the branch
    deleted and created again, which closes both, and this one reopened would
    leave runs of the other created after this one). Needs pull-requests: read
    (or issues: read for the timeline and the events).
    """
    owner = head_repository.split("/", 1)[0]
    head = quote(f"{owner}:{head_ref}", safe="")
    try:
        # Two at most: any count but exactly one refuses, so no paging.
        pulls = api_json(gh, f"repos/{repository}/pulls?head={head}&state=all&per_page=2")
        timeline = listing(gh, f"repos/{repository}/issues/{pr}/timeline", lambda page: page)
        # GitHub's docs list base_ref_changed and automatic_base_change_* for
        # the issue events API only, though the timeline returns them too; read
        # both, so neither listing alone can hide a base change.
        events_api = listing(gh, f"repos/{repository}/issues/{pr}/events", lambda page: page)
    except (Invalid, *API_ERRORS) as error:
        return f"the pull request lookup failed ({error})", None
    if not isinstance(pulls, list) or not all(isinstance(item, dict) for item in pulls):
        return "the pull request listing is not a list of pull requests", None
    if len(pulls) != 1:
        return f"{len(pulls)} pull requests have head branch {head_ref!r}, not only this one", None
    only = pulls[0]
    if not (same(field(only, "number"), pr) and same(field(only, "head", "ref"), head_ref)
            and same(field(only, "head", "repo", "full_name"), head_repository)):
        return f"the pull request from {head_ref!r} is not this one (#{pr})", None
    created = github_time(field(only, "created_at"))
    if created is None:
        return f"this pull request has no readable created_at ({field(only, 'created_at')!r})", None
    events = [field(item, "event") for item in timeline + events_api]
    if not all(isinstance(event, str) for event in events):
        return "the pull request's timeline or events have an event without a name", None
    moved = sorted({event for event in events if BASE_CHANGE_EVENT.fullmatch(event)})
    if moved:
        return f"this pull request's base moved ({', '.join(moved)})", None
    head_events = sorted({event for event in events if event in HEAD_BRANCH_EVENTS})
    if head_events:
        return f"this pull request's head branch was deleted or restored ({', '.join(head_events)})", None
    return None, created


def fail_marker_problem(gh: Gh, *, repository: str, key: str) -> str | None:
    """Why GitHub's records say a full run of this tree failed, or cannot be
    read, or None. A failed run records it twice: the fail marker in the cache
    (whose lookup step reports a cache service error as a miss, so the cache
    service's own list is read here) and an artifact named for the key
    (fail_record_name), so a save that failed in one store is caught by the
    other. Needs actions: read."""
    record = fail_record_name(key)
    try:
        caches = listing(gh, f"repos/{repository}/actions/caches?key={quote(key, safe='')}",
                         lambda page: field(page, "actions_caches"))
        records = listing(gh, f"repos/{repository}/actions/artifacts?name={record}",
                          lambda page: field(page, "artifacts"))
    except (Invalid, *API_ERRORS) as error:
        return f"the fail-marker listing failed ({error})"
    if any(field(entry, "key") == key for entry in caches):
        return f"a full run of this tree failed ({key})"
    if any(field(entry, "name") == record and field(entry, "expired") is not True for entry in records):
        return f"a full run of this tree failed (artifact {record})"
    return None


def promotion_rejection(decision: object, tree: str, writer_run: int | None = None) -> str | None:
    """Why scripts/prism_l3_promote.py's decision for `tree` does not carry a
    passing full-suite run of exactly that tree, or None.

    The decision is a tag's: it reads one page of up to 100 artifacts named
    for the tree, drops expired ones and ones that name no run, walks the rest
    newest first, records every run it passes over before the one it promotes
    under `considered` (this run, a lookup or download that failed, an
    unreadable, incomplete or foreign verdict, a run unfinished, cancelled or
    not a same-repository pull_request run) and promotes the next. A skip must
    not let an older pass stand in for a newer run it could not read, so any
    passed-over run refuses here, and so does any listed artifact the walk
    could not rank or trace for certain (`malformed`); the tag's own promotion
    is unchanged.
    """
    if not isinstance(decision, dict):
        return "the promotion decision is not an object"
    if decision.get("promote") is not True:
        return f"a tag would not promote a run of this tree ({decision.get('reason', 'no reason given')})"
    if decision.get("tree") != tree:
        return f"the promotion decision is for tree {decision.get('tree')!r}"
    considered = decision.get("considered")
    if not isinstance(considered, list) or not all(isinstance(entry, dict) for entry in considered):
        return "the promotion decision does not say which newer runs it passed over"
    malformed = decision.get("malformed")
    if not isinstance(malformed, list):
        return "the promotion decision does not say whether every listed artifact was well-formed"
    if malformed:
        return f"the verdict listing has artifacts the promotion could not rank ({'; '.join(map(str, malformed))})"
    if considered:
        passed_over = "; ".join(f"run {entry.get('run')!r}: {entry.get('rejected')}" for entry in considered)
        return f"the promotion passed over newer runs of this tree ({passed_over})"
    verdict = decision.get("verdict")
    if not isinstance(verdict, dict):
        return "the promoted run's verdict is unreadable"
    if verdict.get("tree") != tree or verdict.get("suite") != L3_SUITE or verdict.get("complete") is not True:
        return "the promoted run's verdict is not a complete l3-full verdict for this tree"
    if verdict.get("passed") is not True:
        return f"the run a tag would promote, {decision.get('source_run')}, did not pass every gate"
    # The marker's own run, vouched for above, and no other: another pull
    # request's run of this tree could have uploaded any verdict.
    if writer_run is not None and not same(decision.get("source_run"), writer_run):
        return f"a tag would promote run {decision.get('source_run')!r}, not the marker's run {writer_run}"
    return None


def skip_decision(
    *,
    lookup: str,
    prefix: str,
    outcome: str,
    primary_key: str,
    matched_key: str,
    gh: Gh,
    repository: str,
    run_id: str,
    workflow_ref: str,
    head_repository: str,
    head_ref: str,
    head_sha: str,
    require_jobs: list[str],
    latest_jobs: list[str],
    pr: str = "",
    fail_key_: str | None = None,
    fail_outcome: str = "",
    fail_hit: str = "",
    promotion: Path | None = None,
    tree: str | None = None,
) -> dict:
    """{skip, reason}. Only a pass marker for exactly this prefix whose run
    GitHub vouches for, with no fail marker, skips; everything else runs."""
    def run(reason: str) -> dict:
        return {"skip": False, "reason": f"run everything: {reason}"}

    if lookup != "true":
        return run("the markers were not looked up")
    if not PREFIX.fullmatch(prefix):
        return run(f"{prefix!r} is not a pass-marker prefix")
    if fail_key_ is not None:
        if not FAIL_KEY.fullmatch(fail_key_) or fail_key_[len(FAIL_PREFIX):] != prefix[len(PASS_PREFIX):-1]:
            return run(f"{fail_key_!r} is not this tree's fail-marker key")
        if fail_outcome != "success":
            return run(f"the fail-marker lookup's outcome is {fail_outcome or 'unknown'}")
        if fail_hit == "true":
            return run(f"a full run of this tree failed ({fail_key_})")
    if outcome != "success":
        return run(f"the pass-marker lookup's outcome is {outcome or 'unknown'}")
    if primary_key != prefix:
        return run(f"the lookup used key {primary_key!r}, not {prefix!r}")
    if not matched_key:
        return run(f"no pass marker for {prefix}*")
    writer = parse_marker_key(prefix, matched_key)
    if writer is None:
        return run(f"the cache entry {matched_key!r} is not a pass marker under {prefix}")
    try:
        path = workflow_path(workflow_ref, repository)
        if not RUN_ID.fullmatch(run_id):
            raise Invalid(f"run id {run_id!r} is unreadable")
        if not OBJECT_ID.fullmatch(head_sha):
            raise Invalid(f"head commit {head_sha!r} is unreadable")
        if not REPOSITORY.fullmatch(head_repository):
            raise Invalid(f"head repository {head_repository!r} is unreadable")
        if not head_ref or len(head_ref) > 255 or any(ord(c) < 0x20 or c == "\x7f" for c in head_ref):
            raise Invalid(f"head branch {head_ref!r} is unreadable")
        if not require_jobs or not latest_jobs:
            raise Invalid("no jobs are named for the saving run to have passed")
        if not PR_NUMBER.fullmatch(pr):
            raise Invalid(f"pull request number {pr!r} is unreadable")
    except Invalid as error:
        return run(str(error))
    if fail_key_ is not None:
        why = fail_marker_problem(gh, repository=repository, key=fail_key_)
        if why is not None:
            return run(why)
    why, pull_request_created = pull_request_problem(gh, repository=repository, pr=int(pr),
                                                     head_repository=head_repository, head_ref=head_ref)
    if why is not None or pull_request_created is None:
        return run(why or "this pull request's creation time is unknown")
    why = binding_problem(
        gh, repository=repository, current_run=int(run_id), workflow_path_=path,
        head_repository=head_repository, head_ref=head_ref, head_sha=head_sha,
        writer_run=writer[0], writer_attempt=writer[1], key=matched_key,
        require_jobs=require_jobs, latest_jobs=latest_jobs, pull_request_created=pull_request_created,
    )
    if why is not None:
        return run(why)
    if promotion is not None:
        if tree is None or not prefix.endswith(f"-{tree}-"):
            return run("the promotion check names another tree")
        try:
            decision = json.loads(promotion.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            return run(f"no promotion decision ({error})")
        why = promotion_rejection(decision, tree, writer[0])
        if why is not None:
            return run(why)
    return {"skip": True, "reason": f"skip: run {writer[0]} (attempt {writer[1]}) passed this tree in this "
            f"pull request and saved {matched_key}"}


def parse_needs(text: str) -> dict[str, dict]:
    try:
        needs = json.loads(text)
    except ValueError as error:
        raise Invalid(f"needs is not JSON ({error})") from None
    if not isinstance(needs, dict) or not needs:
        raise Invalid("needs is not a non-empty object")
    for job, value in needs.items():
        if not JOB_ID.fullmatch(job) or not isinstance(value, dict) or not isinstance(value.get("result"), str):
            raise Invalid(f"needs.{job} has no result")
        if not isinstance(value.get("outputs", {}), dict):
            raise Invalid(f"needs.{job}.outputs is not an object")
    return needs


def save_decision(
    *,
    needs: dict[str, dict],
    marker_job: str,
    skip: str,
    save: str,
    prefix: str,
    recomputed_prefix: str,
    event: str,
    requirements: list[tuple[str, str, str]],
) -> dict:
    """{save, reason}: record the marker only for a pull request run that the
    marker did not skip, whose every needed job succeeded, for this tree."""
    if event != "pull_request":
        return {"save": False, "reason": f"save nothing: a {event or 'unnamed'} run never records a marker"}
    if save != "true":
        return {"save": False, "reason": "save nothing: the lookup did not allow this run to record a marker"}
    if skip != "false":
        return {"save": False, "reason": f"save nothing: this run was skipped by a marker (skip={skip!r})"}
    if not prefix or prefix != recomputed_prefix:
        return {"save": False, "reason": f"save nothing: the lookup's prefix {prefix!r} is not this tree's "
                f"{recomputed_prefix!r}"}
    if marker_job not in needs or len(needs) < 2:
        return {"save": False, "reason": f"save nothing: needs lists no jobs beside {marker_job}"}
    failed = sorted(f"{job} ({value['result']})" for job, value in needs.items() if value["result"] != "success")
    if failed:
        return {"save": False, "reason": f"save nothing: not every job succeeded: {', '.join(failed)}"}
    for job, output, expected in requirements:
        actual = (needs.get(job, {}).get("outputs") or {}).get(output)
        if actual != expected:
            return {"save": False, "reason": f"save nothing: needs.{job}.outputs.{output} is {actual!r}, not {expected!r}"}
    return {"save": True, "reason": "every job this marker stands for succeeded on this tree"}


def write_outputs(values: dict[str, object]) -> None:
    lines = []
    for name, value in values.items():
        text = ("true" if value else "false") if isinstance(value, bool) else str(value)
        if "\n" in text or "\r" in text:
            raise Invalid(f"output {name} is not one line")
        lines.append(f"{name}={text}\n")
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.writelines(lines)


def write_summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(text)


def command_lookup(args: argparse.Namespace, gh: Gh) -> int:
    fetch = None
    if args.live_labels:
        def fetch() -> list[str]:
            return live_labels(gh, args.repository, args.pr)
    result = lookup_decision(
        event=args.event, action=args.action, repository=args.repository,
        head_repository=args.head_repository, pr=args.pr, payload_labels=args.labels,
        fetch_live_labels=fetch, workflow_ref=args.workflow_ref, salt=args.salt, tree=args.tree,
        skip_labeled=args.skip_labeled, tested_sha=args.tested_sha, event_sha=args.event_sha,
    )
    print(f"tree-pass-marker lookup: {result['reason']}")
    write_outputs({"lookup": result["lookup"], "save": result["save"], "prefix": result["prefix"],
                   "fail_key": result["fail_key"]})
    return 0


def command_decide(args: argparse.Namespace, gh: Gh) -> int:
    result = skip_decision(
        lookup=args.lookup, prefix=args.prefix, outcome=args.outcome, primary_key=args.primary_key,
        matched_key=args.matched_key, gh=gh, repository=args.repository, run_id=args.run_id,
        workflow_ref=args.workflow_ref, head_repository=args.head_repository, head_ref=args.head_ref,
        head_sha=args.head_sha, require_jobs=args.require_job, latest_jobs=args.latest_job, pr=args.pr,
        fail_key_=args.fail_key, fail_outcome=args.fail_outcome, fail_hit=args.fail_hit,
        promotion=args.promotion, tree=args.tree,
    )
    print(f"tree-pass-marker decide: {result['reason']}")
    write_outputs({"skip": result["skip"]})
    if result["skip"]:
        write_summary(
            f"### This tree already passed\n\n{result['reason']}, so the jobs it stands for skip: "
            f"a job skipped at job level reports skipped, which GitHub counts as passing, and a "
            f"matrix shard runs with every step skipped. To run everything, apply the "
            f"`{FORCE_LABEL}` label and re-run all jobs (docs/ci-same-tree-skip.md).\n"
        )
    return 0


def command_save(args: argparse.Namespace) -> int:
    key = artifact = ""
    try:
        needs = parse_needs(args.needs)
        requirements = []
        for text in args.require:
            match = REQUIREMENT.fullmatch(text)
            if not match:
                raise Invalid(f"--require {text!r} is not JOB.OUTPUT=VALUE")
            requirements.append((match.group(1), match.group(2), match.group(3)))
        recomputed = marker_prefix(workflow_file(args.workflow_ref, args.repository),
                                   effective_salt(args.salt), args.tree)
    except Invalid as error:
        result = {"save": False, "reason": f"save nothing: {error}"}
    else:
        result = save_decision(
            needs=needs, marker_job=args.marker_job, skip=args.skip, save=args.save, prefix=args.prefix,
            recomputed_prefix=recomputed, event=args.event, requirements=requirements,
        )
    if result["save"]:
        try:
            if not OBJECT_ID.fullmatch(args.sha):
                raise Invalid(f"commit {args.sha!r} is unreadable")
            key = marker_key(args.prefix, args.run_id, args.run_attempt)
            artifact = artifact_name(key)
            directory = Path(args.marker_dir)
            directory.mkdir(parents=True, exist_ok=True)
            marker = {
                "schema": MARKER_SCHEMA, "key": key, "artifact": artifact, "tree": args.tree,
                "commit": args.sha, "run_id": int(args.run_id), "run_attempt": int(args.run_attempt),
            }
            (directory / MARKER_FILE).write_text(json.dumps(marker, indent=2) + "\n", encoding="utf-8")
        except (Invalid, OSError) as error:
            key = artifact = ""
            result = {"save": False, "reason": f"save nothing: the marker could not be written ({error})"}
        else:
            result["reason"] = f"save {key}: {result['reason']}"
    print(f"tree-pass-marker save: {result['reason']}")
    write_outputs({"save": result["save"], "key": key, "artifact": artifact})
    return 0


def command_fail_key(args: argparse.Namespace) -> int:
    """Print the fail marker's key for this checkout's tree; exit 1 if there
    is none (not a same-repository pull request, or an unreadable input)."""
    try:
        if args.event != "pull_request" or args.head_repository != args.repository:
            raise Invalid("only a same-repository pull_request run records a failure")
        print(fail_key(workflow_file(args.workflow_ref, args.repository), effective_salt(args.salt), args.tree))
    except Invalid as error:
        print(f"tree-pass-marker fail-key: {error}", file=sys.stderr)
        return 1
    return 0


def parser() -> argparse.ArgumentParser:
    top = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = top.add_subparsers(dest="command", required=True)

    lookup = commands.add_parser("lookup")
    lookup.add_argument("--workflow-ref", required=True)
    lookup.add_argument("--repository", required=True)
    lookup.add_argument("--salt", default="", help="the TREE_PASS_SALT repository variable; empty means 1")
    lookup.add_argument("--tree", required=True)
    lookup.add_argument("--event", required=True)
    lookup.add_argument("--action", default="")
    lookup.add_argument("--head-repository", default="")
    lookup.add_argument("--pr", default="")
    lookup.add_argument("--labels", default="")
    lookup.add_argument("--live-labels", action="store_true",
                        help="also read the pull request's current labels (needs pull-requests: read)")
    lookup.add_argument("--skip-labeled", action="store_true",
                        help="a `labeled` event asked for its run, so it never skips")
    lookup.add_argument("--tested-sha", help="the commit the workflow checked out to test")
    lookup.add_argument("--event-sha", help="GITHUB_SHA; required with --tested-sha")

    decide = commands.add_parser("decide")
    decide.add_argument("--lookup", default="")
    decide.add_argument("--prefix", default="")
    decide.add_argument("--outcome", default="")
    decide.add_argument("--primary-key", default="")
    decide.add_argument("--matched-key", default="")
    decide.add_argument("--fail-key", help="ci.yml: the fail marker's key; its lookup must find nothing")
    decide.add_argument("--fail-outcome", default="")
    decide.add_argument("--fail-hit", default="")
    decide.add_argument("--repository", default="")
    decide.add_argument("--run-id", default="")
    decide.add_argument("--workflow-ref", default="")
    decide.add_argument("--head-repository", default="")
    decide.add_argument("--head-ref", default="")
    decide.add_argument("--head-sha", default="")
    decide.add_argument("--pr", default="", help="this pull request's number (needs pull-requests: read)")
    decide.add_argument("--require-job", action="append", default=[], metavar="NAME",
                        help="a job the attempt that saved must show succeeded (needs actions: read)")
    decide.add_argument("--latest-job", action="append", default=[], metavar="NAME",
                        help="a job whose latest execution in that run must have succeeded")
    decide.add_argument("--promotion", type=Path,
                        help="L3: scripts/prism_l3_promote.py's decision, which must carry a passing run")
    decide.add_argument("--tree", help="L3: the tree the promotion decision must be for")

    save = commands.add_parser("save")
    save.add_argument("--needs", required=True)
    save.add_argument("--marker-job", required=True)
    save.add_argument("--skip", default="")
    save.add_argument("--save", default="")
    save.add_argument("--prefix", default="")
    save.add_argument("--workflow-ref", required=True)
    save.add_argument("--repository", required=True)
    save.add_argument("--salt", default="")
    save.add_argument("--tree", required=True)
    save.add_argument("--event", required=True)
    save.add_argument("--require", action="append", default=[], metavar="JOB.OUTPUT=VALUE")
    save.add_argument("--marker-dir", required=True)
    save.add_argument("--sha", required=True)
    save.add_argument("--run-id", required=True)
    save.add_argument("--run-attempt", required=True)

    fail = commands.add_parser("fail-key")
    fail.add_argument("--workflow-ref", required=True)
    fail.add_argument("--repository", required=True)
    fail.add_argument("--salt", default="")
    fail.add_argument("--tree", required=True)
    fail.add_argument("--event", required=True)
    fail.add_argument("--head-repository", default="")
    return top


def main(argv: list[str] | None = None, gh: Gh = run_gh) -> int:
    args = parser().parse_args(argv)
    if args.command == "lookup":
        return command_lookup(args, gh)
    if args.command == "decide":
        return command_decide(args, gh)
    if args.command == "fail-key":
        return command_fail_key(args)
    return command_save(args)


if __name__ == "__main__":
    raise SystemExit(main())
