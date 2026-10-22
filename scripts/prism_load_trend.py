#!/usr/bin/env python3
"""The load lanes' trend rows and evidence promotion (#551, S6 of #487).

#487 decision 3 keeps the lanes' evidence in this repository: Actions
artifacts (90 days) are the working set, one JSON line per run on the orphan
branch `ci-evidence` is the trend, and raw bundles that must outlive the
artifacts are promoted to release assets.

Layout of `ci-evidence`: `trend/<lane>/YYYY-MM.jsonl`, append-only, one row
per preset run (`kind: preset`), per planned preset whose job left no row
(`kind: missing`, its own kind so it can never stand in for, or block, the
preset's real row), per non-preset lane job of a scheduled run
(`kind: job`: the live regtest scenarios, the fuzzing, the shipped images) or
per later promotion (`kind: promotion`: a cited run, or a writer job's re-run
that promoted a bundle the recorded row lists as failed). The month is the
row's `recorded_at` month, in UTC. Rows follow ROW_SCHEMA; a reader skips
rows of another schema instead of guessing at them.

Only the scheduled writer job and a `v*` tag job write (`contents: write` on
those jobs alone); `append` refuses a row from any other event, so a pull
request or a dispatch can build rows as artifacts but never record them.

Subcommands:

- `row`: one preset run's row, from the directory prism-load-run.sh wrote
  (load-harness-report.json, host.json, pg_test_fsync.txt, the exit codes).
  A number the run did not measure is null, never 0.
- `job-rows`: rows for a scheduled run's non-preset jobs, from the
  workflow's `toJSON(needs)`, and a `kind: missing` row for each planned
  preset whose job left no row.
- `fetch`: export `ci-evidence`'s trend files into a directory (empty when
  the branch does not exist yet), for the regression rule to read.
- `promote`: upload the selected rows' bundles (their Actions artifacts,
  tarred) to a release and record the asset URL in the row.
- `append`: add rows to `ci-evidence`, creating the orphan branch on the
  first write. Each attempt fetches the tip, skips rows already there (by
  identity: kind, run id, run attempt and preset or job), commits on top and
  pushes without force; a rejected or unconfirmed push is retried from a
  fresh fetch, so concurrent writers never lose a row and a retry never
  duplicates one (EP-STATE). Attempts are bounded and share one deadline
  (EP-ERRORS).
- `cite`: a maintainer's tool: promote a finished run's artifact to the
  monthly pre-release and print (or, with --append, record) its
  `kind: promotion` row.

Promotion policy (`promote --select policy`): L3, L5 and `weekly` (the
Saturday soak, #575) rows always; an L2
row when it is flagged, that is, when the regression rule found a regressed
number in it or its gate failed; no other lane. Monthly bundles go to the
`ci-evidence/YYYY-MM` pre-release, a tag's full-suite bundles to the tag's
release (`--release <tag>`). Asset names carry lane, preset, run id and
attempt, so `gh release upload --clobber` only ever replaces the same bundle
on a retry.

Usage:
  prism_load_trend.py row --dir OUT --preset NAME --lane auto --runner-class LABEL \\
      --commit SHA --run-id N --run-attempt N --event EVENT --repository OWNER/NAME \\
      [--artifact-name NAME --artifact-url URL] --out ROW.json
  prism_load_trend.py job-rows --needs JSON --matrix JSON --rows DIR --run-id N ...
  prism_load_trend.py fetch --out DIR
  prism_load_trend.py promote --rows DIR --select policy --verdict VERDICT.json --repository R
  prism_load_trend.py append --rows DIR
  prism_load_trend.py cite --artifact-id N --lane L2 --preset NAME --repository R
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import time
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parent))

import prism_load_probe as probe  # noqa: E402
import prism_load_regress as regress  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PRESETS = ROOT / "crates" / "qbit-prism-load" / "presets"
ROW_SCHEMA = regress.ROW_SCHEMA
BRANCH = "ci-evidence"
# #487's lanes, plus the manifest's `weekly` (the Saturday soak, #575, which
# is not L5) and the nightly fuzzing.
LANES = ("L2", "L3", "L4", "L5", "L6", "weekly", "fuzz")
KINDS = ("preset", "missing", "job", "promotion")
# The events whose rows may be recorded: the schedules, and a `v*` tag push
# (a branch push never runs a lane that writes). `cite` is a maintainer's
# promotion of an earlier run, recorded only as a `promotion` row.
WRITE_EVENTS = ("schedule", "push")
# The non-preset jobs of prism-load-nightly.yml whose outcome is a trend row.
JOB_LANES = {
    # #552's bridging pair: its outcome; its comparison row
    # (scripts/prism_load_bridge.py) stays in its artifact for now.
    "bridging": "L2",
    "live-nightly": "L4",
    "live-weekly": "L4",
    "stratum-fuzz": "fuzz",
    "shipped-images": "L6",
}
PROMOTE_ALWAYS = ("L3", "L5", "weekly")
BOT_NAME = "github-actions[bot]"
BOT_EMAIL = "41898282+github-actions[bot]@users.noreply.github.com"
README = """\
# ci-evidence

The PRISM load lanes' trend rows (#551, S6 of #487; decision 3). This orphan
branch is written only by the scheduled and `v*` tag jobs of the lane
workflows, through `scripts/prism_load_trend.py append` on `3.x.x`. It is
append-only: rows are added, never edited or removed, and a push is never
forced.

- `trend/<lane>/YYYY-MM.jsonl`: one JSON row per line, schema
  `qbit.prism.load-trend-row.v1`, month of the row's `recorded_at` (UTC).
- Raw bundles that outlive the 90-day Actions artifacts are release assets:
  the rolling `ci-evidence/YYYY-MM` pre-release, or a tag's release for the
  full suite. A row's `asset_url` points at its bundle once promoted.

`scripts/prism_load_regress.py` reads these rows for the report-only
regression rule.
"""


class TrendError(Exception):
    """An input or an operation the trend tooling cannot complete."""


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def month_of(row: dict) -> str:
    recorded = row.get("recorded_at")
    if not isinstance(recorded, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", recorded):
        raise TrendError(f"row {identity(row)} has recorded_at {recorded!r}, not UTC YYYY-MM-DDTHH:MM:SSZ")
    return recorded[:7]


def identity(row: dict) -> tuple:
    """What makes two rows the same record: a retried write of one of them is
    skipped. A preset row is one run attempt of one preset. A job row is one
    attempt of a job, with its outcome and commit: each job exports the
    attempt it ran in, so a later attempt that re-ran the job has a new row,
    and one that did not re-run it repeats the same row and is skipped."""
    kind = row.get("kind")
    if kind in ("preset", "missing"):
        return (kind, row.get("repository"), row.get("run_id"), row.get("run_attempt"), row.get("preset"))
    if kind == "job":
        return (kind, row.get("repository"), row.get("run_id"), row.get("run_attempt"), row.get("job"),
                (row.get("outcome") or {}).get("result"), row.get("commit"))
    return (kind, row.get("repository"), row.get("run_id"), row.get("run_attempt"),
            row.get("preset"), row.get("asset_url"))


def validate(row: dict, events: tuple[str, ...] = WRITE_EVENTS) -> None:
    if not isinstance(row, dict) or row.get("schema") != ROW_SCHEMA:
        raise TrendError(f"not a {ROW_SCHEMA} row: {str(row)[:120]}")
    if row.get("kind") not in KINDS:
        raise TrendError(f"row {identity(row)}: kind {row.get('kind')!r} is not one of {KINDS}")
    if row.get("lane") not in LANES:
        raise TrendError(f"row {identity(row)}: lane {row.get('lane')!r} is not one of {LANES}")
    for field in ("run_id", "run_attempt"):
        if isinstance(row.get(field), bool) or not isinstance(row.get(field), int) or row[field] < 1:
            raise TrendError(f"row {identity(row)}: {field} is {row.get(field)!r}")
    if not isinstance(row.get("repository"), str) or "/" not in row["repository"]:
        raise TrendError(f"row {identity(row)}: repository is {row.get('repository')!r}")
    if row["kind"] == "job":
        if JOB_LANES.get(row.get("job")) != row["lane"]:
            raise TrendError(f"row {identity(row)}: job {row.get('job')!r} is not one of {sorted(JOB_LANES)} "
                             "in its lane")
    elif not isinstance(row.get("preset"), str) or not row["preset"]:
        raise TrendError(f"row {identity(row)}: preset is {row.get('preset')!r}")
    # A promotion row is a maintainer's `cite`, or a writer job's re-run
    # recovering a promotion its first attempt recorded as failed.
    allowed = ("cite", *events) if row["kind"] == "promotion" else events
    if row.get("event") not in allowed:
        raise TrendError(
            f"row {identity(row)}: event {row.get('event')!r} may not write to {BRANCH} "
            f"(only {', '.join(allowed)}); pull request and dispatch runs produce artifacts only"
        )
    # A `v*` tag push is the only push that writes; the ref is on the row.
    if row["event"] == "push" and not str(row.get("ref") or "").startswith("refs/tags/v"):
        raise TrendError(f"row {identity(row)}: a push of {row.get('ref')!r} may not write to {BRANCH} "
                         "(only a refs/tags/v* tag push)")
    month_of(row)


# --- row ----------------------------------------------------------------


def number(value) -> float | None:
    return regress.number(value)


def integer(value) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def server_ack(deltas) -> tuple[float | None, float | None]:
    """Mean and bucketed p99 upper bound, in ms, of the frontends' accepted
    share acknowledgements (qbit_prism_share_ack_seconds, receipt to response
    write, which includes the share append). Unknown if any frontend's delta
    is: a mean over the rest would describe a different set of shares."""
    if not isinstance(deltas, list) or not deltas:
        return None, None
    count = total = 0.0
    buckets: dict[float, float] = {}
    for delta in deltas:
        if not isinstance(delta, dict) or delta.get("unavailable_reason"):
            return None, None
        c = number((delta.get("counts") or {}).get("accepted"))
        s = number((delta.get("sums") or {}).get("accepted"))
        if c is None or s is None:
            return None, None
        count += c
        total += s
        for le, value in ((delta.get("bucket_deltas") or {}).get("accepted") or {}).items():
            try:
                bound = float(le)
            except ValueError:
                continue
            if number(value) is not None:
                buckets[bound] = buckets.get(bound, 0.0) + value
    if count <= 0:
        return None, None
    p99 = None
    for bound in sorted(buckets):
        if buckets[bound] >= 0.99 * count:
            p99 = bound * 1000 if math.isfinite(bound) else None
            break
    return total / count * 1000, p99


def peak_rss_mib(processes) -> float | None:
    """The largest frontend peak RSS; unknown when any frontend's is."""
    if not isinstance(processes, list) or not processes:
        return None
    peaks = [integer((p or {}).get("peak_rss_kib")) for p in processes]
    if any(p is None for p in peaks):
        return None
    return round(max(peaks) / 1024, 1)


def latency_ms(summary, field: str) -> float | None:
    if not isinstance(summary, dict) or summary.get("unit") != "milliseconds":
        return None
    return number(summary.get(field))


def phase_headline(phase: dict) -> dict:
    order = phase.get("order_lock") or {}
    ack = phase.get("client_ack_latency")
    mean, p99 = server_ack(phase.get("server_share_ack_seconds"))
    reconciliation = phase.get("reconciliation") or {}
    return {
        "in_artifact": phase.get("in_artifact") if isinstance(phase.get("in_artifact"), bool) else None,
        "completed": phase.get("completed") if isinstance(phase.get("completed"), bool) else None,
        "duration_seconds": number(phase.get("duration_seconds")),
        "target_rate_shares_per_second": number(phase.get("target_rate_shares_per_second")),
        "achieved_rate_shares_per_second": number(phase.get("achieved_rate_shares_per_second")),
        "shortfall": integer(phase.get("shortfall")),
        "client_ack_p50_ms": latency_ms(ack, "p50"),
        "client_ack_p99_ms": latency_ms(ack, "p99"),
        "server_ack_mean_ms": mean,
        "server_ack_p99_upper_ms": p99,
        "order_lock_max_waiters": integer(order.get("max_waiters")),
        "order_lock_mean_waiters": number(order.get("mean_waiters")),
        "peak_rss_mib": peak_rss_mib(phase.get("processes")),
        "rejected_valid_shares": integer(phase.get("rejected_valid_shares")),
        "missing_shares": integer(reconciliation.get("missing")),
    }


def preset_facts(name: str, directory: Path) -> tuple[str | None, str | None]:
    """The preset's schedule and file sha256 from the checkout."""
    path = directory / f"{name}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None, None
    return data.get("schedule"), hashlib.sha256(path.read_bytes()).hexdigest()


def lane_for(lane: str, schedule: str | None) -> str:
    if lane != "auto":
        return lane
    # The weekly soak (#575) is the manifest's `weekly` lane (not L5, which
    # is #556's soak and chaos); every other preset of the nightly
    # workflow is L2's.
    return "weekly" if schedule == "weekly" else "L2"


def host_summary(host: dict | None) -> dict | None:
    if not isinstance(host, dict) or "error" in host:
        return None
    filesystem = host.get("filesystem") or {}
    device = host.get("block_device") or {}
    return {
        "runner_label": host.get("runner_label"),
        "nproc": host.get("nproc"),
        "cpu_model": host.get("cpu_model"),
        "mem_total_mib": host.get("mem_total_mib"),
        "kernel": host.get("kernel"),
        "filesystem_type": filesystem.get("type"),
        "mount_options": filesystem.get("options"),
        "write_cache": device.get("write_cache"),
        "fua": device.get("fua"),
    }


def base_row(args: argparse.Namespace, kind: str, lane: str) -> dict:
    return {
        "schema": ROW_SCHEMA,
        "kind": kind,
        "recorded_at": args.recorded_at or utc_now(),
        "repository": args.repository,
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "run_url": f"{args.server_url}/{args.repository}/actions/runs/{args.run_id}"
                   + (f"/attempts/{args.run_attempt}" if args.run_attempt > 1 else ""),
        "event": args.event,
        "ref": args.ref or None,
        "commit": args.commit or None,
        "lane": lane,
    }


def build_row(args: argparse.Namespace) -> dict:
    directory: Path = args.dir
    report = probe.read_json(directory / "load-harness-report.json")
    result = probe.run_result(directory)
    gate = probe.read_int(directory / "gate-exit-code")
    harness = result["harness_exit_code"]
    schedule, sha256 = preset_facts(args.preset, args.presets)
    config = regress.load_config(args.config)
    fsync = result["fsync"]
    phases = {}
    if report is not None:
        for phase in report.get("phases") or []:
            if isinstance(phase, dict) and isinstance(phase.get("name"), str):
                phases[phase["name"]] = phase_headline(phase)
    if gate in (0, 1):
        verdict = "pass" if gate == 0 else "fail"
    else:
        verdict = "no verdict"
    durability = report.get("durability_findings") if report else None
    row = base_row(args, "preset", lane_for(args.lane, schedule))
    row.update({
        "preset": args.preset,
        "preset_sha256": sha256,
        "runner_class": args.runner_class,
        "fsync": {
            "method": fsync.get("method"),
            "ops_per_second": fsync.get("ops_per_second"),
            "usecs_per_op": fsync.get("usecs_per_op"),
            "band": regress.fsync_band(fsync.get("usecs_per_op"), config),
            "band_edges_usecs": list(config.band_edges),
        },
        "host": host_summary(probe.read_json(directory / "host.json")),
        "outcome": {
            "harness_exit_code": harness,
            "gate_exit_code": gate,
            "result": verdict,
            "provenance": result["provenance"],
        },
        "report": None if report is None else {
            "schema": report.get("schema"),
            "run_id": report.get("run_id"),
            "started_at": report.get("started_at"),
            "finished_at": report.get("finished_at"),
        },
        "headline": {
            "run": {
                "tip_last_notify_p99_ms": result["tip_last_notify_p99_ms"],
                "tips_missing_a_session": result["tips_missing_a_session"] if report else None,
                "durability_findings": len(durability) if isinstance(durability, list) else None,
            },
            "phases": phases,
        },
        "artifact": {"name": args.artifact_name or None, "id": artifact_id_of(args.artifact_id, args.artifact_url),
                     "url": args.artifact_url or None},
        "promotion": None,
        "asset_url": None,
    })
    return row


def artifact_id_of(given: str | None, url: str | None) -> int | None:
    """The artifact's ID: upload-artifact's `artifact-id`, else the tail of
    its `artifact-url` (.../actions/runs/<run>/artifacts/<id>)."""
    if given and str(given).isdigit():
        return int(given)
    match = re.search(r"/artifacts/(\d+)$", url or "")
    return int(match.group(1)) if match else None


def job_rows(args: argparse.Namespace) -> list[dict]:
    """A row per non-preset job that ran, and a `missing` row per planned
    preset that left none, so every scheduled run has rows."""
    try:
        needs = json.loads(args.needs)
    except ValueError as error:
        raise TrendError(f"--needs is not JSON: {error}") from error
    rows = []
    for job, lane in JOB_LANES.items():
        entry = needs.get(job) if isinstance(needs, dict) else None
        if not isinstance(entry, dict) or entry.get("result") in (None, "skipped"):
            continue
        outputs = entry.get("outputs") or {}
        # The attempt the job last ran in: a re-run of the evidence job alone
        # leaves the other jobs' outputs, and so their attempts, as they were.
        attempt = str(outputs.get("attempt") or "")
        row = base_row(argparse.Namespace(**{**vars(args), "run_attempt": int(attempt) if attempt.isdigit()
                                             and int(attempt) > 0 else args.run_attempt}), "job", lane)
        row["commit"] = outputs.get("commit") or None
        row.update({"job": job, "outcome": {"result": entry["result"]},
                    "promotion": None, "asset_url": None})
        rows.append(row)
    planned: list[str] = []
    if args.matrix:
        try:
            planned = [e["preset"] for e in json.loads(args.matrix)["include"]]
        except (ValueError, KeyError, TypeError) as error:
            raise TrendError(f"--matrix is not the plan's matrix: {error}") from error
    # A row for a preset this attempt's plan does not name is an earlier
    # attempt's (a re-run after the plan changed): it is not this run's
    # measurement, so it is dropped before anything reads the directory.
    for path in sorted(args.rows.glob("*.json")):
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if row.get("kind") == "preset" and row.get("preset") not in planned:
            print(f"prism-load-trend: dropping {path.name}: preset {row.get('preset')!r} is not in "
                  "this run's plan", file=sys.stderr)
            path.unlink()
    if args.matrix:
        # The attempt each preset job last ran in (the jobs API's
        # filter=latest, one {"name", "run_attempt"} per line): a row from an
        # earlier attempt of a job that was re-run is stale, and the re-run
        # that left no row is recorded as missing at its own attempt.
        latest: dict[str, int] = {}
        if args.jobs:
            for line in args.jobs.read_text(encoding="utf-8").splitlines():
                try:
                    job = json.loads(line)
                except ValueError as error:
                    raise TrendError(f"--jobs line {line[:80]!r} is not JSON") from error
                if isinstance(job.get("run_attempt"), int):
                    latest[job.get("name")] = max(job["run_attempt"], latest.get(job.get("name"), 0))
        present: dict[str, int] = {}
        for path in args.rows.glob("*.json"):
            try:
                row = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if row.get("kind") == "preset" and isinstance(row.get("run_attempt"), int):
                present[row["preset"]] = max(row["run_attempt"], present.get(row["preset"], 0))
        for preset in planned:
            if preset in present and present[preset] >= latest.get(preset, 0):
                continue
            schedule, sha256 = preset_facts(preset, args.presets)
            # The row (its attempt and its run URL) is the preset job's latest
            # attempt, which need not be this job's.
            attempt = latest.get(preset) or args.run_attempt
            row = base_row(argparse.Namespace(**{**vars(args), "run_attempt": attempt}),
                           "missing", lane_for("auto", schedule))
            row.update({
                "preset": preset, "preset_sha256": sha256,
                "outcome": {"result": "missing: the preset's job left no row"},
                "promotion": None, "asset_url": None,
            })
            rows.append(row)
    return rows


# --- git ----------------------------------------------------------------


class Deadline:
    def __init__(self, seconds: float) -> None:
        self.end = time.monotonic() + seconds

    def remaining(self) -> float:
        return self.end - time.monotonic()

    def check(self, what: str) -> float:
        left = self.remaining()
        if left <= 0:
            raise TrendError(f"the deadline passed before {what}")
        return left


def run(command: list[str], deadline: Deadline, *, cwd: Path | None = None, check: bool = True,
        input: bytes | None = None, env: dict | None = None,
        stdout_path: Path | None = None) -> subprocess.CompletedProcess:
    timeout = deadline.check(" ".join(command[:3]))
    try:
        if stdout_path is None:
            done = subprocess.run(command, cwd=cwd, input=input, capture_output=True, timeout=timeout,
                                  env=env, check=False)
        else:
            with stdout_path.open("wb") as out:
                done = subprocess.run(command, cwd=cwd, input=input, stdout=out, stderr=subprocess.PIPE,
                                      timeout=timeout, env=env, check=False)
    except subprocess.TimeoutExpired as error:
        raise TrendError(f"{' '.join(command[:4])} did not finish before the deadline") from error
    if check and done.returncode != 0:
        raise TrendError(f"{' '.join(command[:4])} exited {done.returncode}: "
                         f"{done.stderr.decode(errors='replace').strip()[-500:]}")
    return done


class Branch:
    """`ci-evidence` on a remote, read and written with plumbing only: no
    checkout, so a shallow clone of any branch can write it."""

    def __init__(self, repo: Path, remote: str, deadline: Deadline) -> None:
        self.repo, self.remote, self.deadline = repo, remote, deadline
        self.ref = f"refs/remotes/{remote}/{BRANCH}"

    def git(self, *args: str, check: bool = True, input: bytes | None = None,
            env: dict | None = None) -> subprocess.CompletedProcess:
        return run(["git", *args], self.deadline, cwd=self.repo, check=check, input=input, env=env)

    def fetch(self) -> str | None:
        """The branch's tip, or None when it does not exist. A failure to ask
        is an error, never read as a missing branch."""
        listed = self.git("ls-remote", "--heads", self.remote, f"refs/heads/{BRANCH}")
        if not listed.stdout.strip():
            return None
        self.git("fetch", "--no-tags", "--depth=1", self.remote, f"+refs/heads/{BRANCH}:{self.ref}")
        return self.git("rev-parse", self.ref).stdout.decode().strip()

    def read(self, tip: str | None, path: str) -> str:
        if tip is None:
            return ""
        done = self.git("cat-file", "-e", f"{tip}:{path}", check=False)
        if done.returncode != 0:
            return ""
        return self.git("cat-file", "blob", f"{tip}:{path}").stdout.decode("utf-8")

    def files(self, tip: str | None, prefix: str) -> list[str]:
        if tip is None:
            return []
        out = self.git("ls-tree", "-r", "--name-only", tip, "--", prefix).stdout.decode()
        return [line for line in out.splitlines() if line]

    def commit(self, tip: str | None, contents: dict[str, str], message: str) -> str:
        with tempfile.TemporaryDirectory() as scratch:
            env = {**os.environ, "GIT_INDEX_FILE": str(Path(scratch) / "index")}
            env.setdefault("GIT_AUTHOR_NAME", BOT_NAME)
            env.setdefault("GIT_AUTHOR_EMAIL", BOT_EMAIL)
            env.setdefault("GIT_COMMITTER_NAME", env["GIT_AUTHOR_NAME"])
            env.setdefault("GIT_COMMITTER_EMAIL", env["GIT_AUTHOR_EMAIL"])
            if tip is not None:
                self.git("read-tree", tip, env=env)
            else:
                self.git("read-tree", "--empty", env=env)
            for path, text in contents.items():
                blob = self.git("hash-object", "-w", "--stdin", input=text.encode("utf-8")).stdout.decode().strip()
                self.git("update-index", "--add", "--cacheinfo", f"100644,{blob},{path}", env=env)
            tree = self.git("write-tree", env=env).stdout.decode().strip()
            parents = ["-p", tip] if tip else []
            return self.git("commit-tree", tree, *parents, "-m", message, env=env).stdout.decode().strip()

    def push(self, commit: str) -> tuple[bool, str]:
        done = self.git("push", "--no-verify", self.remote, f"{commit}:refs/heads/{BRANCH}", check=False)
        return done.returncode == 0, done.stderr.decode(errors="replace").strip()


def lines_of(text: str) -> list[dict]:
    rows = []
    for line in text.splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def recovered_promotion(row: dict, recorded: dict) -> dict | None:
    """A `kind: promotion` row for a preset row whose recorded copy lacks
    the asset this copy now has, or None."""
    if row.get("kind") != "preset" or (row.get("promotion") or {}).get("status") != "promoted" \
            or not row.get("asset_url") or recorded.get("asset_url") == row["asset_url"]:
        return None
    return {key: row.get(key) for key in (
        "schema", "recorded_at", "repository", "run_id", "run_attempt", "run_url", "event",
        "ref", "commit", "lane", "preset", "artifact", "promotion", "asset_url")} | {"kind": "promotion"}


def append_rows(branch: Branch, rows: list[dict], attempts: int, backoff: float,
                log=print) -> dict:
    """Record `rows`, returning how many were added and skipped. Raises when
    the rows could not be confirmed on the branch within the attempts."""
    for row in rows:
        validate(row)
    # A row listed twice in one batch is one record.
    unique: dict[tuple, dict] = {}
    for row in rows:
        unique.setdefault(identity(row), row)
    last_error = "no attempt made"
    for attempt in range(1, attempts + 1):
        tip = branch.fetch()
        additions: dict[str, list[dict]] = {}
        skipped = 0
        # Every month of each lane is read: a row rebuilt by a re-run months
        # later has a later `recorded_at` but the same identity.
        lanes: dict[str, dict[tuple, dict]] = {}
        for key, row in unique.items():
            path = f"trend/{row['lane']}/{month_of(row)}.jsonl"
            if row["lane"] not in lanes:
                lanes[row["lane"]] = {identity(r): r for p in branch.files(tip, f"trend/{row['lane']}")
                                      for r in lines_of(branch.read(tip, p))}
            seen = lanes[row["lane"]]
            if key in seen:
                skipped += 1
                # A re-run that promoted a bundle the recorded row could not:
                # the row stays as written, and the promotion is its own row.
                recovered = recovered_promotion(row, seen[key])
                if recovered is not None and identity(recovered) not in seen:
                    additions.setdefault(path, []).append(recovered)
                continue
            additions.setdefault(path, []).append(row)
        if not additions:
            log(f"{BRANCH}: every row is already recorded ({skipped} skipped)")
            return {"added": 0, "skipped": skipped, "attempts": attempt}
        contents = {}
        for path, new in additions.items():
            existing = branch.read(tip, path)
            if existing and not existing.endswith("\n"):
                existing += "\n"
            contents[path] = existing + "".join(json.dumps(r, sort_keys=True) + "\n" for r in new)
        if tip is None:
            contents["README.md"] = README
        added = sum(len(v) for v in additions.values())
        runs = sorted({str(r["run_id"]) for v in additions.values() for r in v})
        commit = branch.commit(tip, contents, f"ci-evidence: {added} row(s) from run {', '.join(runs)}")
        pushed, error = branch.push(commit)
        if pushed:
            log(f"{BRANCH}: added {added} row(s), {skipped} already recorded"
                + (" (created the branch)" if tip is None else "") + f", attempt {attempt}")
            return {"added": added, "skipped": skipped, "attempts": attempt}
        # Rejected (another writer moved the tip, or created the branch) or
        # unconfirmed: the next attempt starts from a fresh fetch and skips
        # whatever did land.
        last_error = error
        log(f"{BRANCH}: push attempt {attempt} of {attempts} was not accepted: {error[-300:]}")
        if attempt < attempts:
            time.sleep(min(backoff * attempt, max(branch.deadline.remaining() - 1, 0)))
    raise TrendError(f"the rows were not recorded after {attempts} attempts: {last_error[-300:]}")


def export(branch: Branch, out: Path) -> str | None:
    """Write the branch's trend files under `out`; returns the tip."""
    tip = branch.fetch()
    out.mkdir(parents=True, exist_ok=True)
    for path in branch.files(tip, "trend"):
        target = out / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(branch.read(tip, path), encoding="utf-8")
    return tip


# --- promotion ----------------------------------------------------------


def asset_name(row: dict) -> str:
    name = f"{row['lane']}-{row.get('preset') or row.get('job')}-run{row['run_id']}-attempt{row['run_attempt']}.tar.gz"
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)


def flagged(row: dict, verdict: dict | None) -> bool:
    if (row.get("outcome") or {}).get("result") == "fail":
        return True
    for entry in (verdict or {}).get("regressions") or []:
        if (entry.get("run_id"), entry.get("run_attempt"), entry.get("preset")) == \
                (row.get("run_id"), row.get("run_attempt"), row.get("preset")):
            return True
    return False


def selected(row: dict, select: str, verdict: dict | None) -> bool:
    if row.get("kind") != "preset" or not (row.get("artifact") or {}).get("name"):
        return False
    if select == "none":
        return False
    if select == "all":
        return True
    if select == "flagged":
        return flagged(row, verdict)
    return row.get("lane") in PROMOTE_ALWAYS or (row.get("lane") == "L2" and flagged(row, verdict))


def gh(args: list[str], deadline: Deadline, check: bool = True,
       stdout_path: Path | None = None) -> subprocess.CompletedProcess:
    return run(["gh", *args], deadline, check=check, stdout_path=stdout_path)


def gh_json(path: str, deadline: Deadline) -> dict:
    done = gh(["api", path], deadline)
    try:
        return json.loads(done.stdout.decode())
    except ValueError as error:
        raise TrendError(f"gh api {path} did not return JSON") from error


def download_artifact(row: dict, repository: str, dest: Path, deadline: Deadline) -> None:
    """Fetch exactly the artifact the row names, by ID. `gh run download`
    selects by run and name only, and a re-run's upload of the same name
    replaces it, so a name could package another attempt's bundle under
    this row's attempt."""
    artifact = row.get("artifact") or {}
    ident = artifact_id_of(artifact.get("id"), artifact.get("url"))
    if ident is None:
        raise TrendError(f"row {identity(row)} has no artifact ID, so its bundle cannot be selected exactly")
    meta = gh_json(f"repos/{repository}/actions/artifacts/{ident}", deadline)
    if (meta.get("workflow_run") or {}).get("id") != row["run_id"] or meta.get("name") != artifact.get("name"):
        raise TrendError(f"artifact {ident} is {meta.get('name')!r} of run "
                         f"{(meta.get('workflow_run') or {}).get('id')}, not the row's")
    if meta.get("expired"):
        raise TrendError(f"artifact {ident} has expired")
    archive = dest.parent / f"{dest.name}.zip"
    gh(["api", f"repos/{repository}/actions/artifacts/{ident}/zip"], deadline, stdout_path=archive)
    with zipfile.ZipFile(archive) as bundle:
        bundle.extractall(dest)
    archive.unlink()


def attempt_of_artifact(meta: dict, repository: str, deadline: Deadline) -> int:
    """The run attempt that uploaded an artifact: the latest attempt that had
    started by the time the artifact was created."""
    run_id = (meta.get("workflow_run") or {}).get("id")
    latest = gh_json(f"repos/{repository}/actions/runs/{run_id}", deadline).get("run_attempt") or 1
    created = meta.get("created_at") or ""
    for attempt in range(latest, 0, -1):
        started = gh_json(f"repos/{repository}/actions/runs/{run_id}/attempts/{attempt}",
                          deadline).get("run_started_at") or ""
        if started and started <= created:
            return attempt
    raise TrendError(f"no attempt of run {run_id} started before artifact {meta.get('id')} was created")


def ensure_release(tag: str, repository: str, create: bool, deadline: Deadline) -> None:
    if gh(["release", "view", tag, "--repo", repository, "--json", "tagName"], deadline, check=False).returncode == 0:
        return
    if not create:
        raise TrendError(f"release {tag} does not exist, and only a ci-evidence/YYYY-MM pre-release is created here")
    made = gh(["release", "create", tag, "--repo", repository, "--prerelease", "--latest=false",
               "--title", f"CI evidence {tag.split('/', 1)[1]}",
               "--notes", "Raw bundles of the PRISM load lanes' reduced-L3 jobs, flagged L2 runs, "
                          "soaks and cited runs (#551, #487 decision 3). Rows: the ci-evidence "
                          "branch, trend/<lane>/YYYY-MM.jsonl."],
              deadline, check=False)
    # Another writer may have created it between the view and the create:
    # reconcile before calling it a failure.
    if made.returncode != 0 and gh(["release", "view", tag, "--repo", repository, "--json", "tagName"],
                                   deadline, check=False).returncode != 0:
        raise TrendError(f"could not create release {tag}: {made.stderr.decode(errors='replace').strip()}")


def upload_bundle(row: dict, tag: str, repository: str, create: bool, deadline: Deadline,
                  attempts: int = 3) -> str:
    """Upload the row's artifact as a tarball and return the asset's URL."""
    name = asset_name(row)
    ensure_release(tag, repository, create, deadline)
    with tempfile.TemporaryDirectory() as scratch:
        bundle = Path(scratch) / "bundle"
        download_artifact(row, repository, bundle, deadline)
        tarball = Path(scratch) / name
        with tarfile.open(tarball, "w:gz") as tar:
            tar.add(bundle, arcname=name.removesuffix(".tar.gz"))
        error = ""
        for attempt in range(1, attempts + 1):
            done = gh(["release", "upload", tag, str(tarball), "--clobber", "--repo", repository],
                      deadline, check=False)
            error = done.stderr.decode(errors="replace").strip()
            # An upload whose response was lost may still have landed: the
            # release's asset list is the answer either way.
            listed = gh(["release", "view", tag, "--repo", repository, "--json", "assets",
                         "--jq", f'.assets[] | select(.name == "{name}") | .url'], deadline, check=False)
            url = listed.stdout.decode().strip()
            if url:
                return url
            if attempt < attempts:
                time.sleep(min(5 * attempt, max(deadline.remaining() - 1, 0)))
        raise TrendError(f"asset {name} is not on release {tag} after {attempts} uploads: {error[-300:]}")


def promote(rows_dir: Path, select: str, verdict: dict | None, release: str, repository: str,
            deadline: Deadline, uploader=upload_bundle, log=print) -> int:
    """Promote the selected rows' bundles and write the result into each row
    file. A failed promotion is recorded as failed, not left looking like a
    row nobody selected; returns the number that failed."""
    failures = 0
    for path in sorted(rows_dir.glob("*.json")):
        row = json.loads(path.read_text(encoding="utf-8"))
        if not selected(row, select, verdict):
            continue
        monthly = release == "monthly"
        tag = release
        try:
            tag = f"{BRANCH}/{month_of(row)}" if monthly else release
            url = uploader(row, tag, repository, monthly, deadline)
            row["promotion"] = {"status": "promoted", "release": tag, "asset": asset_name(row)}
            row["asset_url"] = url
            log(f"promoted {row.get('preset')} to {tag}: {url}")
        # A full disk, a bad zip or a tar error while bundling is a failed
        # promotion like any other, recorded on this row; the rest still run.
        except (TrendError, OSError, zipfile.BadZipFile, tarfile.TarError) as error:
            failures += 1
            row["promotion"] = {"status": "failed", "release": tag, "error": str(error)[-500:]}
            row["asset_url"] = None
            log(f"could not promote {row.get('preset')} to {tag}: {error}")
        path.write_text(json.dumps(row, indent=2) + "\n", encoding="utf-8")
    return failures


def read_rows(directory: Path) -> list[dict]:
    rows = []
    for path in sorted(directory.glob("*.json")):
        try:
            rows.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError) as error:
            raise TrendError(f"{path}: {error}") from error
    return rows


# --- main ---------------------------------------------------------------


def add_run_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--run-attempt", type=int, default=1)
    parser.add_argument("--event", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--server-url", default="https://github.com")
    parser.add_argument("--ref", default="")
    parser.add_argument("--commit", default="")
    parser.add_argument("--recorded-at", default=None, help="UTC; defaults to now")
    parser.add_argument("--presets", type=Path, default=PRESETS)


def add_git_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="a clone with push access")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--deadline-seconds", type=float, default=300.0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="action", required=True)

    r = sub.add_parser("row")
    add_run_args(r)
    r.add_argument("--dir", type=Path, required=True)
    r.add_argument("--preset", required=True)
    r.add_argument("--lane", default="auto", choices=("auto", *LANES))
    r.add_argument("--runner-class", required=True)
    r.add_argument("--artifact-name", default="")
    r.add_argument("--artifact-url", default="")
    r.add_argument("--artifact-id", default="")
    r.add_argument("--config", type=Path, default=regress.CONFIG)
    r.add_argument("--out", type=Path, required=True)

    j = sub.add_parser("job-rows")
    add_run_args(j)
    j.add_argument("--needs", required=True, help="toJSON(needs)")
    j.add_argument("--matrix", default="", help="the plan's matrix, to add missing preset rows")
    j.add_argument("--rows", type=Path, required=True, help="the row directory to add to")
    j.add_argument("--jobs", type=Path, default=None,
                   help="the run's jobs as JSON lines of {name, run_attempt} (jobs API, filter=latest)")

    f = sub.add_parser("fetch")
    add_git_args(f)
    f.add_argument("--out", type=Path, required=True)

    p = sub.add_parser("promote")
    p.add_argument("--rows", type=Path, required=True)
    p.add_argument("--select", choices=("policy", "all", "flagged", "none"), default="policy")
    p.add_argument("--verdict", type=Path, help="the regression rule's verdict JSON")
    p.add_argument("--release", default="monthly", help="`monthly` or an existing release's tag")
    p.add_argument("--repository", required=True)
    p.add_argument("--deadline-seconds", type=float, default=1500.0)

    a = sub.add_parser("append")
    add_git_args(a)
    a.add_argument("--rows", type=Path, required=True)
    a.add_argument("--attempts", type=int, default=5)
    a.add_argument("--backoff-seconds", type=float, default=5.0)

    c = sub.add_parser("cite")
    add_git_args(c)
    # The download, the tarball and up to three uploads, as `promote` allows.
    c.set_defaults(deadline_seconds=1500.0)
    c.add_argument("--artifact-id", type=int, required=True,
                   help="the artifact's ID (its URL's last part); the run and attempt are read from it")
    c.add_argument("--lane", required=True, choices=LANES)
    c.add_argument("--preset", required=True)
    c.add_argument("--repository", required=True)
    c.add_argument("--server-url", default="https://github.com")
    c.add_argument("--append", action="store_true", help="also record the promotion row")

    args = parser.parse_args(argv)
    try:
        if args.action == "row":
            row = build_row(args)
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(row, indent=2) + "\n", encoding="utf-8")
            return 0
        if args.action == "job-rows":
            args.rows.mkdir(parents=True, exist_ok=True)
            for row in job_rows(args):
                name = row.get("job") or f"missing-{row['preset']}-attempt{row['run_attempt']}"
                (args.rows / f"{name}.json").write_text(json.dumps(row, indent=2) + "\n", encoding="utf-8")
            return 0
        if args.action == "fetch":
            tip = export(Branch(args.repo, args.remote, Deadline(args.deadline_seconds)), args.out)
            print(f"{BRANCH}: {'at ' + tip if tip else 'does not exist yet; no history'}")
            return 0
        if args.action == "promote":
            verdict = json.loads(args.verdict.read_text(encoding="utf-8")) if args.verdict else None
            failed = promote(args.rows, args.select, verdict, args.release, args.repository,
                             Deadline(args.deadline_seconds))
            return 1 if failed else 0
        if args.action == "append":
            if args.attempts < 1:
                raise TrendError("--attempts must be at least 1")
            rows = read_rows(args.rows)
            if not rows:
                print(f"{BRANCH}: no rows to record")
                return 0
            append_rows(Branch(args.repo, args.remote, Deadline(args.deadline_seconds)), rows,
                        args.attempts, args.backoff_seconds)
            return 0
        if args.action == "cite":
            deadline = Deadline(args.deadline_seconds)
            meta = gh_json(f"repos/{args.repository}/actions/artifacts/{args.artifact_id}", deadline)
            run_id = (meta.get("workflow_run") or {}).get("id")
            if not isinstance(run_id, int):
                raise TrendError(f"artifact {args.artifact_id} names no workflow run")
            attempt = attempt_of_artifact(meta, args.repository, deadline)
            row = {
                "schema": ROW_SCHEMA, "kind": "promotion", "recorded_at": utc_now(),
                "repository": args.repository, "run_id": run_id, "run_attempt": attempt,
                "run_url": f"{args.server_url}/{args.repository}/actions/runs/{run_id}"
                           + (f"/attempts/{attempt}" if attempt > 1 else ""),
                "event": "cite", "lane": args.lane, "preset": args.preset,
                "artifact": {"name": meta.get("name"), "id": args.artifact_id, "url": None},
            }
            tag = f"{BRANCH}/{month_of(row)}"
            row["asset_url"] = upload_bundle(row, tag, args.repository, True, deadline)
            row["promotion"] = {"status": "promoted", "release": tag, "asset": asset_name(row)}
            print(json.dumps(row, sort_keys=True))
            if args.append:
                append_rows(Branch(args.repo, args.remote, deadline), [row], 5, 5.0)
            return 0
    except TrendError as error:
        print(f"prism-load-trend: {error}", file=sys.stderr)
        return 2
    except regress.RegressError as error:
        print(f"prism-load-trend: {error}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
