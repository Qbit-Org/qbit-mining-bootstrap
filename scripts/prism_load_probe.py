#!/usr/bin/env python3
"""The Blacksmith runner probe's helpers (#541 and #542, S1 and S2 of #487).

`.github/workflows/prism-load-runner-probe.yml` measures what each runner
class is before any lane depends on it, and how noisy the harness is on it.
This script is every part of that workflow that is not a shell one-liner, so
it can be tested:

- `matrix`: the probe and fsync-spread job matrices from the dispatch inputs,
  refusing a class, a cache backend, a preset or a repeat count the workflow
  cannot run, and a plan whose jobs would outlast the per-job ceiling. It
  also prints the plan's spend ceiling;
- `measure`: run a command, sample the host's memory once a second while it
  runs, and write its wall time, exit code and peak memory as JSON. With
  `--timeout-seconds` it ends the command's whole process group at the
  deadline and records `timed_out`. It exits with the command's code (124 on
  a timeout);
- `row`: one probe job's result from the measurements, the harness's side
  reports, the gate's exit codes and `pg_test_fsync`'s output, for each
  preset and each in-job repeat;
- `fsync-row`: one fsync-spread VM's `pg_test_fsync` result;
- `host`: the host fingerprint, with what the kernel says about the block
  device under a directory (its write cache and FUA support), which is the
  evidence for whether `fsync` reaches stable storage;
- `table`: the per-class results table (Markdown) from every row;
- `variance`: per class, preset and fsync-cost band, the median, MAD and CV
  of each headline metric over every run, over the per-VM medians (VM-to-VM)
  and within each VM (run-to-run), as the JSON document `VARIANCE_SCHEMA`
  names (#542; the S6 regression rule in #551 reads it), with the runner-size
  evidence per class. A run that failed or never reported is unknown there,
  never a zero;
- `verdict`: fail when the build or any planned run did not exit 0.

Peak memory is the host's (MemTotal - MemAvailable), not one process's: the
harness, its frontends and both PostgreSQL clusters share the VM, and what
sizes a runner is whether all of them fit (#487 lesson 3).

Usage:
  prism_load_probe.py matrix --classes 8,16,32 --target-cache both --fsync-vms 3 \
      [--presets NAME,...] [--repeats N] [--in-job-repeats N]
  prism_load_probe.py measure --out build.json [--timeout-seconds S] -- cargo build ...
  prism_load_probe.py row --out row.json --class 8 ... --run PRESET=DIR [--run PRESET=DIR2 ...]
  prism_load_probe.py fsync-row --out row.json --class 8 --vm 1 --fsync FILE
  prism_load_probe.py host --out host.json --dir "$RUNNER_TEMP/pload" [--runner LABEL]
  prism_load_probe.py table DIR [--expected FILE]
  prism_load_probe.py variance DIR [--expected FILE] [--out FILE] [--markdown]
  prism_load_probe.py verdict --probe DIR --presets NAME,... --in-job-repeats N
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess
import sys
import time

import prism_load_matrix

# v2 (#542) adds the job's repeat index and commit and makes each preset's
# run a list, one entry per in-job repeat. v1 rows (#541) are still read:
# `load_rows` upgrades them to one repeat and one run per preset.
ROW_SCHEMA = "qbit.prism.runner-probe-row.v2"
ROW_SCHEMA_V1 = "qbit.prism.runner-probe-row.v1"
ROW_SCHEMA_PREFIX = "qbit.prism.runner-probe-row."
VARIANCE_SCHEMA = "qbit.prism.runner-probe-variance.v1"
# Blacksmith's x64 Ubuntu 24.04 classes; the probe is for 8, 16 and 32.
CLASSES = (2, 4, 8, 16, 32)
TARGET_CACHES = ("actions-cache", "sticky-disk")
MAX_FSYNC_VMS = 10
# #542 asks for at least 10 runs of one commit per class; 20 separate jobs
# is the most one dispatch may ask for per class and cache backend.
MAX_REPEATS = 20
MAX_IN_JOB_REPEATS = 5
# A dispatch that would start more probe jobs than this is refused before
# any runner is spent: it is a typo more often than a plan.
MAX_PROBE_JOBS = 64
# The default presets (#541: one short and one D1 at 20k).
PRESETS = ("short-plan-20k-window-1fe", "throughput-20k-window-1fe")
# A probe job's budget in minutes: setup (checkout, toolchain, PostgreSQL,
# the target restore), the release build, and the rows and uploads after
# the runs, around each run's own preset timeout. The job's timeout is their
# sum, so a run that hangs ends at its preset's timeout and the job still
# writes and uploads its row.
SETUP_MINUTES = 15
BUILD_MINUTES = 60
REPORT_MINUTES = 15
# Slack between the runs' summed timeouts and the run step's own timeout,
# for the cleanup after a run that timed out.
RUN_STEP_SLACK_MINUTES = 5
# The longest probe job the plan accepts. GitHub's hosted-runner limit is
# 360 minutes; Blacksmith's is what #541's duration job measures, so the
# probe stays inside the lower one until that is known.
MAX_JOB_MINUTES = 360
FSYNC_JOB_MINUTES = 20
# The plan and collate jobs, on 2 vCPU, for the spend ceiling.
OVERHEAD_JOB_MINUTES = 10 + 15
# Blacksmith's list price per 2 vCPU x64 minute, larger classes billed in
# proportion (#487's cost section; S1 confirms the multiplier).
PRICE_PER_2VCPU_MINUTE_USD = 0.004
# PostgreSQL's default wal_sync_method on Linux, so the line whose cost a
# commit pays.
WAL_SYNC_METHOD = "fdatasync"


class ProbeError(Exception):
    """An input the probe cannot use, worded for the workflow log."""


def runner_label(size: int) -> str:
    return f"blacksmith-{size}vcpu-ubuntu-2404"


# --- matrix -------------------------------------------------------------


def parse_classes(text: str) -> list[int]:
    sizes = []
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        if not item.isdigit() or int(item) not in CLASSES:
            raise ProbeError(
                f"runner class {item!r} is not one of {', '.join(map(str, CLASSES))}"
            )
        sizes.append(int(item))
    if not sizes:
        raise ProbeError(f"classes {text!r} names no runner class")
    return sorted(set(sizes))


def parse_target_caches(text: str) -> list[str]:
    text = text.strip()
    if text == "both":
        return list(TARGET_CACHES)
    if text not in TARGET_CACHES:
        raise ProbeError(
            f"target cache {text!r} is not one of both, {', '.join(TARGET_CACHES)}"
        )
    return [text]


def parse_count(text: str, name: str, low: int, high: int) -> int:
    """A whole number in low..high, read as decimal (a leading zero is not
    octal here, and a sign, a fraction or an exponent is refused)."""
    text = text.strip()
    if not re.fullmatch(r"[0-9]{1,4}", text) or not low <= int(text) <= high:
        raise ProbeError(f"{name} {text!r} is not a whole number {low}..{high}")
    return int(text)


def parse_fsync_vms(text: str) -> int:
    return parse_count(text, "fsync VMs", 0, MAX_FSYNC_VMS)


def load_presets(directory: Path | None = None) -> tuple[dict[str, dict], dict[str, str]]:
    directory = directory or prism_load_matrix.PRESETS
    try:
        return (prism_load_matrix.load(directory), prism_load_matrix.load_aliases(directory))
    except prism_load_matrix.SelectionError as error:
        raise ProbeError(str(error)) from error


def parse_presets(text: str, presets: dict[str, dict], aliases: dict[str, str]) -> list[str]:
    """Checked-in preset names, in the order given, a deprecated name
    resolved to the preset it was renamed to. The probe builds no qbitd, so a
    real-node preset is refused here rather than failing on the runner."""
    names: list[str] = []
    unknown = []
    for item in text.split(","):
        name = item.strip()
        if not name:
            continue
        if name not in presets and name in aliases:
            print(f"prism_load_probe: {name} is deprecated; running {aliases[name]}",
                  file=sys.stderr)
            name = aliases[name]
        if name not in presets:
            unknown.append(name)
        elif name not in names:
            names.append(name)
    if unknown:
        raise ProbeError(
            f"unknown preset(s) {', '.join(unknown)}; choose from {', '.join(presets)}"
        )
    if not names:
        raise ProbeError(f"presets {text!r} names no preset")
    real_node = [n for n in names if (presets[n].get("args") or {}).get("--node", "fake") != "fake"]
    if real_node:
        raise ProbeError(
            f"preset(s) {', '.join(real_node)} need a real node, which the probe does not build"
        )
    return names


def preset_minutes(preset: dict) -> int:
    minutes = preset.get("timeout_minutes")
    if not isinstance(minutes, int) or isinstance(minutes, bool) or minutes <= 0:
        raise ProbeError(f"preset {preset.get('name')!r} has no positive timeout_minutes")
    return minutes


def matrices(
    classes: str,
    target_cache: str,
    fsync_vms: str,
    presets: str = ",".join(PRESETS),
    repeats: str = "1",
    in_job_repeats: str = "1",
    presets_dir: Path | None = None,
) -> dict:
    sizes = parse_classes(classes)
    caches = parse_target_caches(target_cache)
    vms = parse_fsync_vms(fsync_vms)
    jobs_per_cell = parse_count(repeats, "repeats", 1, MAX_REPEATS)
    iterations = parse_count(in_job_repeats, "in-job repeats", 1, MAX_IN_JOB_REPEATS)
    known, aliases = load_presets(presets_dir)
    names = parse_presets(presets, known, aliases)
    run_minutes = {name: preset_minutes(known[name]) for name in names}
    runs_minutes = sum(run_minutes.values()) * iterations + RUN_STEP_SLACK_MINUTES
    job_minutes = SETUP_MINUTES + BUILD_MINUTES + runs_minutes + REPORT_MINUTES
    if job_minutes > MAX_JOB_MINUTES:
        overhead = job_minutes - sum(run_minutes.values()) * iterations
        raise ProbeError(
            f"a probe job would need {job_minutes} minutes ({len(names)} preset(s) x "
            f"{iterations} in-job repeat(s) at their timeouts, plus {overhead} "
            f"for setup, build and reports), over the {MAX_JOB_MINUTES}-minute ceiling; "
            "choose fewer presets or in-job repeats and dispatch the rest separately"
        )
    probe = [
        {"class": size, "runner": runner_label(size), "target_cache": cache, "repeat": repeat}
        for size in sizes
        for cache in caches
        for repeat in range(1, jobs_per_cell + 1)
    ]
    if len(probe) > MAX_PROBE_JOBS:
        raise ProbeError(
            f"the plan asks for {len(probe)} probe jobs ({len(sizes)} class(es) x "
            f"{len(caches)} cache backend(s) x {jobs_per_cell} repeat(s)), over {MAX_PROBE_JOBS}"
        )
    fsync = [
        {"class": size, "runner": runner_label(size), "vm": vm}
        for size in sizes
        for vm in range(1, vms + 1)
    ]
    two_vcpu_minutes = (
        sum(job_minutes * e["class"] // 2 for e in probe)
        + sum(FSYNC_JOB_MINUTES * e["class"] // 2 for e in fsync)
        + OVERHEAD_JOB_MINUTES
    )
    # An empty include list is not a valid matrix, so an fsync spread of 0
    # VMs is carried as a flag the workflow's `if:` reads instead.
    return {
        "probe": {"include": probe},
        "fsync": {"include": fsync or [{"class": 0, "runner": "", "vm": 0}]},
        "fsync_enabled": bool(fsync),
        "presets": ",".join(names),
        "in_job_repeats": iterations,
        "runs_minutes": runs_minutes,
        "job_minutes": job_minutes,
        # What the jobs' timeouts cap, not what the runs are expected to
        # cost; the duration hold is not included.
        "ceiling": {
            "probe_jobs": len(probe),
            "fsync_jobs": len(fsync),
            "two_vcpu_minutes": two_vcpu_minutes,
            "usd": round(two_vcpu_minutes * PRICE_PER_2VCPU_MINUTE_USD, 2),
            "price_per_two_vcpu_minute_usd": PRICE_PER_2VCPU_MINUTE_USD,
        },
    }


def plan_summary(plan: dict) -> str:
    ceiling = plan["ceiling"]
    return "\n".join([
        "### Probe plan",
        "",
        f"- {ceiling['probe_jobs']} probe job(s): presets {plan['presets']}, "
        f"{plan['in_job_repeats']} run(s) of each per job; each job times out at "
        f"{plan['job_minutes']} min (runs {plan['runs_minutes']} min).",
        f"- {ceiling['fsync_jobs']} fsync-spread job(s).",
        f"- Spend ceiling: {ceiling['two_vcpu_minutes']:,} 2 vCPU minutes, about "
        f"${ceiling['usd']:,.2f} at ${ceiling['price_per_two_vcpu_minute_usd']} per 2 vCPU "
        "minute (list price, before free minutes; every job at its timeout, the "
        "duration hold not included). Actual spend is the jobs' billed minutes.",
        "",
    ])


# --- measure ------------------------------------------------------------


def meminfo_kib(text: str) -> dict[str, int]:
    values = {}
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        fields = rest.split()
        if fields and fields[0].isdigit():
            values[key.strip()] = int(fields[0])
    return values


def read_meminfo() -> dict[str, int]:
    try:
        return meminfo_kib(Path("/proc/meminfo").read_text(encoding="ascii"))
    except OSError:
        return {}


def used_kib(info: dict[str, int]) -> int | None:
    if "MemTotal" in info and "MemAvailable" in info:
        return info["MemTotal"] - info["MemAvailable"]
    return None


def mib(kib: int | None) -> float | None:
    return None if kib is None else round(kib / 1024, 1)


def process_state(pid: int | str) -> tuple[str, int] | None:
    """(state, process group) from /proc/<pid>/stat, or None when it is gone."""
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="ascii", errors="replace")
    except OSError:
        return None
    fields = text[text.rindex(")") + 2:].split()
    return fields[0], int(fields[2])


def group_alive(pgid: int) -> bool:
    """Whether any process of the group is still running. A zombie is not:
    an orphan whose PID 1 does not reap it promptly stays in the group
    as a zombie, and a kill(0) probe would wait on it for the whole grace."""
    proc = Path("/proc")
    if proc.is_dir():
        for entry in proc.iterdir():
            if entry.name.isdigit():
                state = process_state(entry.name)
                if state is not None and state[1] == pgid and state[0] not in ("Z", "X"):
                    return True
        return False
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def end_group(child: subprocess.Popen, grace: float = 30.0) -> None:
    """SIGTERM the child's process group, then SIGKILL what is left after
    `grace` seconds: what a run that timed out started must not outlive it
    and skew the next run's memory and CPU. (A PostgreSQL postmaster that
    `pg_ctl` put in a session of its own is outside the group; the workflow
    ends those by their data directory.)"""
    for sig, wait in ((signal.SIGTERM, grace), (signal.SIGKILL, 5.0)):
        try:
            os.killpg(child.pid, sig)
        except ProcessLookupError:
            break
        deadline = time.monotonic() + wait
        while time.monotonic() < deadline:
            child.poll()  # reap the leader, so a zombie is not counted as alive
            if not group_alive(child.pid):
                return
            time.sleep(0.2)
    child.poll()


def measure(command: list[str], interval: float = 1.0,
            timeout_seconds: float | None = None) -> dict:
    baseline = read_meminfo()
    samples = [used_kib(baseline)]
    started = time.monotonic()
    # Its own process group, so a timeout, or the step being cancelled, ends
    # everything the command started and not just its first process.
    child = subprocess.Popen(command, start_new_session=True)

    def forward(signum, _frame):
        end_group(child, grace=10.0)
        raise SystemExit(128 + signum)

    previous = {sig: signal.signal(sig, forward) for sig in (signal.SIGTERM, signal.SIGINT)}
    timed_out = False
    try:
        while True:
            try:
                code = child.wait(timeout=interval)
                break
            except subprocess.TimeoutExpired:
                samples.append(used_kib(read_meminfo()))
            if timeout_seconds is not None and time.monotonic() - started >= timeout_seconds:
                timed_out = True
                end_group(child)
                code = child.wait()
                break
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    wall = time.monotonic() - started
    samples.append(used_kib(read_meminfo()))
    known = [s for s in samples if s is not None]
    return {
        "command": command,
        "exit_code": code,
        "timed_out": timed_out,
        "timeout_seconds": timeout_seconds,
        "wall_seconds": round(wall, 1),
        "samples": len(known),
        "mem_total_mib": mib(baseline.get("MemTotal")),
        "baseline_used_mib": mib(samples[0]),
        # None, not 0, when /proc/meminfo could not be read (EP-OBSERVABILITY).
        "peak_used_mib": mib(max(known)) if known else None,
    }


# --- host ---------------------------------------------------------------


def read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return None


def command_output(command: list[str]) -> str | None:
    try:
        done = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return done.stdout.strip() if done.returncode == 0 else None


def block_device(source: str | None) -> dict:
    """The kernel's view of the device a filesystem sits on: `write back` in
    write_cache with fua 0 means a flush has to be sent to reach media, and
    whether the hypervisor honours it is what the probe cannot see from here."""
    if not source or not source.startswith("/dev/"):
        return {"source": source}
    name = Path(source).resolve().name
    sys_block = Path("/sys/class/block") / name
    queue = sys_block / "queue"
    if not queue.exists():
        # A partition's queue is its parent disk's.
        queue = sys_block.resolve().parent / "queue"
    return {
        "source": source,
        "device": name,
        "write_cache": read_text(queue / "write_cache"),
        "fua": read_text(queue / "fua"),
        "rotational": read_text(queue / "rotational"),
        "model": read_text(sys_block.resolve().parent / "device" / "model")
        or read_text(sys_block / "device" / "model"),
    }


def host_facts(directory: Path) -> dict:
    cpuinfo = read_text(Path("/proc/cpuinfo")) or ""
    model = next((line.split(":", 1)[1].strip() for line in cpuinfo.splitlines()
                  if line.startswith("model name")), None)
    info = read_meminfo()
    mount = command_output(["findmnt", "-no", "SOURCE,FSTYPE,OPTIONS", "-T", str(directory)])
    source, fstype, options = (mount.split(None, 2) + [None, None, None])[:3] if mount else (None, None, None)
    return {
        "nproc": len([line for line in cpuinfo.splitlines() if line.startswith("processor")]) or None,
        "cpu_model": model,
        "mem_total_mib": mib(info.get("MemTotal")),
        "kernel": read_text(Path("/proc/sys/kernel/osrelease")),
        "directory": str(directory),
        "filesystem": {"type": fstype, "options": options},
        "block_device": block_device(source),
    }


# --- pg_test_fsync ------------------------------------------------------


def parse_pg_test_fsync(text: str) -> dict:
    """The one-8kB-write section's fdatasync line: the WAL flush a commit pays.

    Returns ops_per_second and usecs_per_op, both None when the output has no
    such line (the method is unsupported) or is marked failed: the probe and
    prism-load-run.sh append a line starting `pg_test_fsync failed` when it
    exits nonzero, and a VM whose run failed after printing the one-write
    section is unknown, not a sample."""
    if re.search(r"^pg_test_fsync failed", text, re.M):
        return {"method": WAL_SYNC_METHOD, "ops_per_second": None, "usecs_per_op": None}
    section = re.search(
        r"Compare file sync methods using one 8kB write:(.*?)(?:\n\s*\n\S|\Z)", text, re.S
    )
    body = section.group(1) if section else ""
    match = re.search(
        rf"^\s*{WAL_SYNC_METHOD}\s+([\d.]+)\s+ops/sec\s+([\d.]+)\s+usecs/op", body, re.M
    )
    if not match:
        return {"method": WAL_SYNC_METHOD, "ops_per_second": None, "usecs_per_op": None}
    return {
        "method": WAL_SYNC_METHOD,
        "ops_per_second": float(match.group(1)),
        "usecs_per_op": float(match.group(2)),
    }


def read_fsync(path: Path) -> dict:
    try:
        return parse_pg_test_fsync(path.read_text(encoding="utf-8", errors="replace"))
    except OSError:
        return parse_pg_test_fsync("")


# --- the harness's outputs ----------------------------------------------


def nearest_rank(values: list[float], quantile: float) -> float | None:
    """The gate's nearest-rank percentile (qbit-prism-load's gate.rs)."""
    if not values:
        return None
    ordered = sorted(values)
    rank = min(max(math.ceil(quantile * len(ordered)), 1), len(ordered))
    return ordered[rank - 1]


def read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def read_int(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="ascii").strip())
    except (OSError, ValueError):
        return None


def provenance(report: dict | None) -> str:
    """`pass` when the run earned a qualification artifact with no override.

    The probe passes no --allow-* flag, so a refusal ends the run before it
    writes a report; a report whose artifact_kind is `example` means one of
    the checks was bypassed some other way."""
    if report is None:
        return "no report (refused or failed before measuring)"
    evidence = (report.get("versions") or {}).get("server_revision_evidence") or {}
    problems = []
    if report.get("dirty"):
        problems.append("dirty tree")
    if evidence.get("status") != "established":
        problems.append(f"revision {evidence.get('status', 'unknown')}")
    if (report.get("versions") or {}).get("server_build_profile") != "release":
        problems.append("server not release")
    if report.get("artifact_kind") != "qualification":
        problems.append(f"artifact_kind {report.get('artifact_kind')}")
    return "pass" if not problems else "fail: " + ", ".join(problems)


def finite(value) -> float | int | None:
    """A reported number, or None: JSON may carry NaN or Infinity, and a
    boolean is not a measurement."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if math.isfinite(value) else None


def latency_ms(summary, key: str) -> float | None:
    """A client latency percentile, only when the report says it is in
    milliseconds: a value in another unit is unknown here, not rescaled."""
    if not isinstance(summary, dict) or summary.get("unit") != "milliseconds":
        return None
    return finite(summary.get(key))


def unmeasured_reason(result: dict) -> str | None:
    """Why a run's numbers are not a sample, or None when they are. Only a
    harness exit 0 with a report, every provenance check passed and a gate
    verdict is a measurement; a gate that failed on it (exit 1) still measured it, and
    its shortfall is exactly what the runner-size pick reads."""
    if result.get("timed_out"):
        return "timed out"
    code = result.get("harness_exit_code")
    if code is None:
        return "no harness exit code"
    if code != 0:
        return f"harness exit {code}"
    if result.get("provenance") != "pass":
        return f"provenance {result.get('provenance')}"
    # prism-load-run.sh's status is the gate's: 0 or 1 is a verdict, anything
    # else (2, a panic, a gate that would not start) is not.
    gate = result.get("gate_exit_code")
    if gate not in (0, 1) or isinstance(gate, bool):
        return f"gate exit {gate}"
    return None


def run_result(directory: Path) -> dict:
    report = read_json(directory / "load-harness-report.json")
    measured = read_json(directory / "probe-measure.json") or {}
    peak = finite(measured.get("peak_used_mib"))
    total = finite(measured.get("mem_total_mib"))
    result = {
        "harness_exit_code": read_int(directory / "harness-exit-code"),
        "gate_exit_code": measured.get("exit_code"),
        "timed_out": measured.get("timed_out"),
        "wall_seconds": measured.get("wall_seconds"),
        "peak_used_mib": peak,
        "baseline_used_mib": measured.get("baseline_used_mib"),
        "mem_total_mib": total,
        "headroom_mib": None if peak is None or total is None else round(total - peak, 1),
        "fsync": read_fsync(directory / "pg_test_fsync.txt"),
        "provenance": provenance(report),
        "seed_seconds": None,
        "seed_rows": None,
        "phases": {},
        "tip_last_notify_p99_ms": None,
        "tips_missing_a_session": None,
    }
    if report is not None:
        seed = ((report.get("window") or {}).get("seed")) or {}
        result["seed_seconds"] = seed.get("seconds")
        result["seed_rows"] = seed.get("rows")
        for phase in report.get("phases") or []:
            ack = phase.get("client_ack_latency")
            result["phases"][phase.get("name")] = {
                "seconds": phase.get("duration_seconds"),
                "shortfall": phase.get("shortfall"),
                "completed": phase.get("completed"),
                "achieved_rate_shares_per_second": finite(
                    phase.get("achieved_rate_shares_per_second")),
                "ack_p50_ms": latency_ms(ack, "p50"),
                "ack_p99_ms": latency_ms(ack, "p99"),
            }
        tips = ((report.get("time_to_usable_work") or {}).get("tips")) or []
        slowest = [t["all_sessions_milliseconds"] for t in tips
                   if isinstance(t.get("all_sessions_milliseconds"), (int, float))]
        # As the gate does, a tip some session never got work on leaves the p99
        # unmeasured: a percentile over the fully served tips alone would
        # understate delivery.
        result["tips_missing_a_session"] = len(tips) - len(slowest)
        if not result["tips_missing_a_session"]:
            result["tip_last_notify_p99_ms"] = nearest_rank(slowest, 0.99)
    result["unmeasured_reason"] = unmeasured_reason(result)
    return result


def parse_run(text: str, known: set[str] | None = None) -> tuple[str, Path]:
    known = set(PRESETS) | (known or set())
    name, sep, directory = text.partition("=")
    if not sep or not directory or name not in known:
        raise ProbeError(f"--run {text!r} is not <preset>=<directory> for a checked-in preset")
    return name, Path(directory)


def build_row(args: argparse.Namespace, known: set[str] | None = None) -> dict:
    # A preset given more than once is one run per in-job repeat, in order.
    runs: dict[str, list[dict]] = {}
    for text in args.run:
        name, directory = parse_run(text, known)
        runs.setdefault(name, []).append(run_result(directory))
    return {
        "schema": ROW_SCHEMA,
        "kind": "probe",
        "class": args.size,
        "runner": args.runner,
        "target_cache": args.target_cache,
        "repeat": args.repeat,
        "commit": args.commit or None,
        "target": {
            "restored": args.target_restored == "true",
            "bytes_before_build": args.target_bytes_before,
            "bytes_after_build": args.target_bytes_after,
            "restore_seconds": args.restore_seconds,
            "save_seconds": args.save_seconds,
        },
        "build": read_json(args.build) or {},
        "host": read_json(args.host) or {},
        "runs": runs,
    }


def build_fsync_row(args: argparse.Namespace) -> dict:
    return {
        "schema": ROW_SCHEMA,
        "kind": "fsync",
        "class": args.size,
        "runner": args.runner,
        "vm": args.vm,
        "host": read_json(args.host) or {},
        "fsync": read_fsync(args.fsync),
    }


def upgrade_row(row: dict, path: Path) -> dict:
    """A row in the current shape. A v1 row (#541) is one repeat whose
    presets each ran once, with no commit recorded; any other runner-probe
    schema is refused rather than misread."""
    schema = row.get("schema")
    if schema == ROW_SCHEMA:
        return row
    if schema == ROW_SCHEMA_V1:
        row = dict(row, schema=ROW_SCHEMA)
        if row.get("kind") == "probe":
            row["repeat"] = 1
            row["commit"] = None
            # v1 runs carry no memory total, so their headroom stays unknown,
            # and whether each is a sample is decided here as for v2 runs.
            row["runs"] = {name: [dict(run, unmeasured_reason=unmeasured_reason(run))]
                           for name, run in (row.get("runs") or {}).items()}
        return row
    raise ProbeError(f"{path}: row schema {schema!r} is not {ROW_SCHEMA} or {ROW_SCHEMA_V1}")


# --- table --------------------------------------------------------------


def cell(value, suffix: str = "") -> str:
    if value is None:
        return "unknown"
    if isinstance(value, float):
        value = f"{value:,.1f}" if value < 1000 else f"{value:,.0f}"
    return f"{value}{suffix}"


def disk_line(host: dict) -> str:
    fs = host.get("filesystem") or {}
    dev = host.get("block_device") or {}
    return (f"{cell(fs.get('type'))} on {cell(dev.get('source'))} "
            f"(write_cache {cell(dev.get('write_cache'))}, fua {cell(dev.get('fua'))}, "
            f"rotational {cell(dev.get('rotational'))}, model {cell(dev.get('model'))}; "
            f"options {cell(fs.get('options'))})")


def load_rows(directory: Path) -> list[dict]:
    rows = []
    for path in sorted(directory.rglob("*.json")):
        row = read_json(path)
        if isinstance(row, dict) and str(row.get("schema", "")).startswith(ROW_SCHEMA_PREFIX):
            rows.append(upgrade_row(row, path))
    return rows


def expected_presets(expected: dict | None) -> list[str]:
    presets = (expected or {}).get("presets")
    if isinstance(presets, str) and presets:
        return presets.split(",")
    return list(PRESETS)


def expected_iterations(expected: dict | None) -> int:
    value = (expected or {}).get("in_job_repeats")
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else 1


def probe_key(row: dict) -> tuple[int, str, int]:
    return (row["class"], row["target_cache"], row.get("repeat", 1))


def spread(samples: list[float | None]) -> str:
    """The range over the VMs that measured, naming any that did not: a
    failed pg_test_fsync is a missing sample, not a narrower range."""
    values = [v for v in samples if v is not None]
    unknown = len(samples) - len(values)
    missing = (f"; {unknown} of {len(samples)} VMs unknown (pg_test_fsync failed or "
               f"the job wrote no row)") if unknown else ""
    if not values:
        return f"unknown{missing}"
    low, high = min(values), max(values)
    ratio = f", max/min {high / low:.2f}" if low > 0 else ""
    return f"{cell(low)}–{cell(high)} over {len(values)} VMs{ratio}{missing}"


def table(rows: list[dict], expected: dict | None = None) -> str:
    """The results table. `expected` is the plan job's `matrices()` output:
    a job it lists that wrote no row (it failed before its row step, or its
    upload did) is named as missing, never left out of the count."""
    probes = sorted((r for r in rows if r.get("kind") == "probe"), key=probe_key)
    lines = ["## Runner probe (#541, #542)", ""]
    if expected:
        present = {probe_key(r) for r in probes}
        absent = [e for e in expected["probe"]["include"]
                  if (e["class"], e["target_cache"], e.get("repeat", 1)) not in present]
        for entry in absent:
            lines += [f"### {entry['class']} vCPU · {entry['target_cache']} · repeat "
                      f"{entry.get('repeat', 1)}: no row "
                      "(the job failed before writing it; see its log)", ""]
    if not probes:
        lines += ["No probe row was produced.", ""]
    # One column per preset and in-job repeat: the plan's, then any a row
    # carries that the plan did not name.
    iterations = expected_iterations(expected) if expected else 1
    for row in probes:
        for runs in (row.get("runs") or {}).values():
            iterations = max(iterations, len(runs))
    # Without the plan, the presets the rows ran (a job's own summary has
    # only its row), not the defaults, which a dispatch may not have asked for.
    names = expected_presets(expected) if expected else []
    for row in probes:
        names += [n for n in (row.get("runs") or {}) if n not in names]
    names = names or list(PRESETS)
    columns = [(name, i) for name in names for i in range(iterations)]

    def heading(name: str, i: int) -> str:
        return name if iterations == 1 else f"{name} #{i + 1}"

    for row in probes:
        build = row.get("build") or {}
        target = row.get("target") or {}
        host = row.get("host") or {}
        lines += [
            f"### {row['class']} vCPU · {row['target_cache']} · repeat {row.get('repeat', 1)} "
            f"({'warm' if target.get('restored') else 'cold'} target)",
            "",
            f"`{row['runner']}` · {host.get('cpu_model', 'unknown CPU')} · "
            f"{cell(host.get('nproc'))} CPUs · {cell(host.get('mem_total_mib'), ' MiB')}",
            "",
            f"Cluster filesystem: {disk_line(host)}",
            "",
            "| Measure | " + " | ".join(heading(n, i) for n, i in columns) + " |",
            "|---|" + "---|" * len(columns),
        ]
        runs = row.get("runs") or {}

        def per_run(label: str, pick) -> str:
            return f"| {label} | " + " | ".join(
                pick(runs[name][i]) if i < len(runs.get(name, [])) else "not run"
                for name, i in columns
            ) + " |"

        lines += [
            per_run("pg_test_fsync fdatasync, one 8 kB write",
                    lambda r: f"{cell(r['fsync']['ops_per_second'], ' ops/s')} "
                              f"({cell(r['fsync']['usecs_per_op'], ' µs/op')})"),
            per_run("seeding", lambda r: f"{cell(r['seed_seconds'], ' s')} "
                                         f"({cell(r['seed_rows'])} rows)"),
            per_run("run wall time (pg_test_fsync, harness, gate)",
                    lambda r: cell(r["wall_seconds"], " s")
                    + (" (timed out)" if r.get("timed_out") else "")),
            per_run("peak host memory used",
                    lambda r: f"{cell(r['peak_used_mib'], ' MiB')} "
                              f"(from {cell(r['baseline_used_mib'], ' MiB')})"),
            per_run("shortfall per phase", lambda r: ", ".join(
                f"{name} {cell(p.get('shortfall'))}" for name, p in r["phases"].items()
            ) or "unknown"),
            per_run("tip-to-last-notify p99",
                    lambda r: cell(r["tip_last_notify_p99_ms"], " ms")
                    + (f" ({r['tips_missing_a_session']} tips missing a session)"
                       if r.get("tips_missing_a_session") else "")),
            per_run("provenance without an override", lambda r: r["provenance"]),
            per_run("harness / gate exit",
                    lambda r: f"{cell(r['harness_exit_code'])} / {cell(r['gate_exit_code'])}"),
            "",
            f"Build: {cell(build.get('wall_seconds'), ' s')} (exit "
            f"{cell(build.get('exit_code'))}, peak host memory "
            f"{cell(build.get('peak_used_mib'), ' MiB')}). Target directory: "
            f"{cell(target.get('bytes_before_build'), ' B')} before, "
            f"{cell(target.get('bytes_after_build'), ' B')} after; restore "
            f"{cell(target.get('restore_seconds'), ' s')}, save "
            f"{cell(target.get('save_seconds'), ' s')}.",
            "",
        ]
    by_vm: dict[tuple[int, int], float | None] = {}
    for row in rows:
        if row.get("kind") == "fsync":
            by_vm[(row["class"], row["vm"])] = row["fsync"].get("ops_per_second")
    if expected and expected.get("fsync_enabled"):
        for entry in expected["fsync"]["include"]:
            by_vm.setdefault((entry["class"], entry["vm"]), None)
    by_class: dict[int, list[float | None]] = {}
    for (size, _vm), ops in sorted(by_vm.items()):
        by_class.setdefault(size, []).append(ops)
    if by_class:
        lines += [
            "### fdatasync across VMs of one class",
            "",
            "| Class | fdatasync ops/s, one 8 kB write |",
            "|---|---|",
        ]
        for size in sorted(by_class):
            lines.append(f"| {size} vCPU | {spread(by_class[size])} |")
        lines.append("")
    return "\n".join(lines)


# --- variance -----------------------------------------------------------

# fdatasync cost bands in µs per one-8kB-write op (pg_test_fsync), doubling
# from 125 µs: a VM's band is the median over its runs, so the regression
# rule compares a run only with runs whose commits cost about the same.
FSYNC_BAND_EDGES_USECS = (125, 250, 500, 1000, 2000, 4000)
UNKNOWN_BAND = "unknown"
ALL_BANDS = "all"
DEFAULT_HEADROOM_FRACTION = 0.25
# Each metric's unit and clock, by the name's last part (a phase metric is
# `<phase>.<metric>`).
METRIC_UNITS = {
    "wall_seconds": "seconds, the runner's monotonic clock, around pg_test_fsync, the harness and the gate",
    "seed_seconds": "seconds, as the harness reports the window seed",
    "peak_used_mib": "MiB, the host's MemTotal - MemAvailable sampled every second",
    "headroom_mib": "MiB, MemTotal minus the peak used",
    "tip_last_notify_p99_ms": "milliseconds, nearest-rank p99 of each tip's slowest session",
    "shortfall": "offers no session could take",
    "achieved_rate_shares_per_second": "shares per second, reconciled acknowledged count over the phase",
    "ack_p50_ms": "milliseconds, the client's monotonic clock, accepted shares",
    "ack_p99_ms": "milliseconds, the client's monotonic clock, accepted shares",
}
RUN_METRICS = ("wall_seconds", "seed_seconds", "peak_used_mib", "headroom_mib",
               "tip_last_notify_p99_ms")
PHASE_METRICS = ("shortfall", "achieved_rate_shares_per_second", "ack_p50_ms", "ack_p99_ms")


def fsync_band(usecs: float | None) -> str:
    if usecs is None:
        return UNKNOWN_BAND
    lower = 0
    for edge in FSYNC_BAND_EDGES_USECS:
        if usecs < edge:
            return f"{lower}-{edge}us"
        lower = edge
    return f"ge{lower}us"


def rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 4)


def stats(values: list[float], planned: int | None = None) -> dict:
    """n, unknown, median, MAD (median absolute deviation from the median,
    unscaled, in the metric's unit), CV (sample standard deviation over the
    mean: None under two samples or at a zero mean), min and max."""
    n = len(values)
    out = {"n": n, "unknown": None if planned is None else max(planned - n, 0),
           "median": None, "mad": None, "cv": None, "min": None, "max": None}
    if not n:
        return out
    median = statistics.median(values)
    mean = statistics.fmean(values)
    out.update(
        median=rounded(median),
        mad=rounded(statistics.median(abs(v - median) for v in values)),
        cv=rounded(statistics.stdev(values) / abs(mean)) if n >= 2 and mean else None,
        min=rounded(min(values)),
        max=rounded(max(values)),
    )
    return out


def run_metrics(run: dict) -> dict[str, float | None]:
    metrics = {name: finite(run.get(name)) for name in RUN_METRICS}
    for phase, values in (run.get("phases") or {}).items():
        for name in PHASE_METRICS:
            metrics[f"{phase}.{name}"] = finite((values or {}).get(name))
    return metrics


def metric_unit(name: str) -> str:
    return METRIC_UNITS.get(name.rsplit(".", 1)[-1], "unknown")


def job_band(row: dict | None) -> tuple[str, float | None]:
    """The VM's fsync-cost band: the median fdatasync µs/op over every run
    it made, whatever the preset. A VM with none measured is `unknown`."""
    if row is None:
        return UNKNOWN_BAND, None
    costs = [finite(run["fsync"].get("usecs_per_op"))
             for runs in (row.get("runs") or {}).values() for run in runs]
    costs = [c for c in costs if c is not None]
    usecs = statistics.median(costs) if costs else None
    return fsync_band(usecs), usecs


def gated_phases(preset: dict | None) -> list[str] | None:
    phases = ((preset or {}).get("gates") or {}).get("phases")
    return list(phases) if isinstance(phases, list) and phases else None


def size_evidence(runs: list[dict], planned: int, preset: dict | None,
                  headroom_fraction: float) -> dict:
    """Whether the class holds the preset (#487 P5): every planned run
    measured, zero shortfall in each gated phase of every run, and the
    host's headroom at peak at least `headroom_fraction` of MemTotal and at
    least the preset's own memory floor. False on any measured violation,
    None while a run is unknown and nothing violated, True otherwise."""
    measured = [r for r in runs if r.get("unmeasured_reason") is None]
    phases = gated_phases(preset)
    floor = finite(((preset or {}).get("args") or {}).get("--min-mem-available-mib"))
    totals = [t for t in (finite(r.get("mem_total_mib")) for r in measured) if t is not None]
    peaks = [p for p in (finite(r.get("peak_used_mib")) for r in measured) if p is not None]
    headrooms = [h for h in (finite(r.get("headroom_mib")) for r in measured) if h is not None]
    # Each run against its own VM's MemTotal: one class's VMs need not all
    # report the same total, and the smallest must not set a lenient bar
    # for the others.
    required_by_run = [
        (h, round(max(headroom_fraction * t, floor or 0), 1))
        for h, t in ((finite(r.get("headroom_mib")), finite(r.get("mem_total_mib")))
                     for r in measured)
        if h is not None and t is not None
    ]
    required = max((need for _h, need in required_by_run), default=None)
    shortfalls: list[float | None] = []
    for run in measured:
        names = phases or list((run.get("phases") or {}))
        values = [finite(((run.get("phases") or {}).get(n) or {}).get("shortfall")) for n in names]
        shortfalls.append(None if not values or None in values else max(values))
    known_shortfalls = [v for v in shortfalls if v is not None]
    problems = []
    if any(v > 0 for v in known_shortfalls):
        problems.append(f"{sum(v > 0 for v in known_shortfalls)} run(s) with gated shortfall")
    short = sum(h < need for h, need in required_by_run)
    if short:
        problems.append(f"{short} run(s) under the headroom")
    unknowns = []
    if len(measured) < planned:
        unknowns.append(f"{planned - len(measured)} of {planned} run(s) unknown")
    if len(known_shortfalls) < len(measured):
        unknowns.append(f"{len(measured) - len(known_shortfalls)} run(s) with no gated shortfall")
    if len(required_by_run) < len(measured) or required is None:
        unknowns.append("memory headroom not measured on every run")
    if problems:
        fits, why = False, "; ".join(problems)
    elif unknowns or not measured:
        fits, why = None, "; ".join(unknowns) or "no run measured"
    else:
        fits, why = True, f"all {planned} run(s) measured, no gated shortfall, headroom held"
    return {
        "runs_planned": planned,
        "runs_measured": len(measured),
        "gated_phases": phases,
        "gated_shortfall_max": max(known_shortfalls) if known_shortfalls else None,
        "runs_with_gated_shortfall": sum(v > 0 for v in known_shortfalls),
        "peak_used_mib_max": max(peaks) if peaks else None,
        "mem_total_mib_min": min(totals) if totals else None,
        "headroom_mib_min": min(headrooms) if headrooms else None,
        # The most any run needed: each run's is max(fraction x its own
        # MemTotal, the preset's floor).
        "headroom_mib_required": required,
        "headroom_fraction": headroom_fraction,
        "preset_min_mem_available_mib": floor,
        "fits": fits,
        "why": why,
    }


def group_document(size: int, preset_name: str, band: str, jobs: list[dict],
                   iterations: int, preset: dict | None, headroom_fraction: float) -> dict:
    """One class, preset and band. Each job is {"row": row or None, "key": ...};
    a job with no row, or a planned run it did not record or did not
    measure, is counted as unknown in every metric."""
    reasons: dict[str, int] = {}
    runs_all: list[dict] = []
    per_job: list[list[dict]] = []
    planned = 0
    for job in jobs:
        row = job["row"]
        runs = list(((row or {}).get("runs") or {}).get(preset_name, []))
        job_planned = max(iterations, len(runs))
        planned += job_planned
        if row is None:
            reasons["job wrote no row"] = reasons.get("job wrote no row", 0) + job_planned
        elif len(runs) < job_planned:
            reasons["run not recorded"] = reasons.get("run not recorded", 0) + job_planned - len(runs)
        for run in runs:
            reason = run.get("unmeasured_reason")
            if reason is not None:
                reasons[reason] = reasons.get(reason, 0) + 1
        runs_all += runs
        per_job.append([r for r in runs if r.get("unmeasured_reason") is None])
    measured = [r for jr in per_job for r in jr]
    # The headline metrics are always present, so a group whose every run is
    # unknown still says n 0 and how many are unknown for each.
    seeded = set(RUN_METRICS) | {f"{phase}.{name}" for phase in (gated_phases(preset) or [])
                                 for name in PHASE_METRICS}
    names = sorted(seeded | {n for r in measured for n in run_metrics(r)})
    metrics = {}
    for name in names:
        values = [v for v in (run_metrics(r).get(name) for r in measured) if v is not None]
        job_values = [[v for v in (run_metrics(r).get(name) for r in jr) if v is not None]
                      for jr in per_job]
        medians = [statistics.median(vs) for vs in job_values if vs]
        within = [stats(vs) for vs in job_values if len(vs) >= 2]
        cvs = [w["cv"] for w in within if w["cv"] is not None]
        metrics[name] = {
            "unit": metric_unit(name),
            "all": stats(values, planned),
            "vm_to_vm": stats(medians, len(jobs)),
            "run_to_run": {
                "jobs": len(within),
                "median_mad": rounded(statistics.median(w["mad"] for w in within)),
                "median_cv": rounded(statistics.median(cvs)) if cvs else None,
            } if within else None,
        }
    fsync_costs = [c for c in (finite(r["fsync"].get("usecs_per_op")) for r in runs_all)
                   if c is not None]
    disks: dict[tuple, int] = {}
    for job in jobs:
        dev = (((job["row"] or {}).get("host") or {}).get("block_device")) or {}
        key = (dev.get("write_cache"), dev.get("fua"), dev.get("model"))
        if job["row"] is not None:
            disks[key] = disks.get(key, 0) + 1
    return {
        "class": size,
        "runner": runner_label(size),
        "preset": preset_name,
        "fsync_band": band,
        "jobs": {"planned": len(jobs), "with_row": sum(j["row"] is not None for j in jobs)},
        "runs": {"planned": planned, "measured": len(measured),
                 "unknown": planned - len(measured), "unknown_reasons": reasons},
        "fsync_usecs_per_op": stats(fsync_costs, planned),
        "disks": [{"write_cache": k[0], "fua": k[1], "model": k[2], "vms": n}
                  for k, n in sorted(disks.items(), key=lambda kv: str(kv[0]))],
        "metrics": metrics,
        "size_evidence": size_evidence(runs_all, planned, preset, headroom_fraction)
        if band == ALL_BANDS else None,
    }


def variance(rows: list[dict], expected: dict | None = None,
             presets: dict[str, dict] | None = None,
             headroom_fraction: float = DEFAULT_HEADROOM_FRACTION) -> dict:
    """The variance document (`VARIANCE_SCHEMA`). Groups are per class,
    preset and fsync band, and per class and preset over every band
    (`fsync_band` "all", which also carries the runner-size evidence).
    Without `expected` a job that wrote no row cannot be counted, and the
    document says so (`jobs.planned` is null)."""
    if not math.isfinite(headroom_fraction) or not 0 <= headroom_fraction < 1:
        raise ProbeError(f"headroom fraction {headroom_fraction!r} is not in [0, 1)")
    presets = presets or {}
    probes = [r for r in rows if r.get("kind") == "probe"]
    commits = sorted({r["commit"] for r in probes if r.get("commit")})
    if len(commits) > 1:
        raise ProbeError(f"rows from more than one commit ({', '.join(commits)}): "
                         "variance is over repeats of one commit")
    by_key = {}
    for row in probes:
        key = probe_key(row)
        if key in by_key:
            raise ProbeError(f"two rows for {key[0]} vCPU, {key[1]}, repeat {key[2]}")
        by_key[key] = row
    planned_keys = ([(e["class"], e["target_cache"], e.get("repeat", 1))
                     for e in expected["probe"]["include"]] if expected else [])
    keys = planned_keys + sorted(k for k in by_key if k not in planned_keys)
    names = expected_presets(expected) if expected else []
    for row in probes:
        names += [n for n in (row.get("runs") or {}) if n not in names]
    iterations = expected_iterations(expected)
    jobs = []
    for key in keys:
        row = by_key.get(key)
        band, usecs = job_band(row)
        jobs.append({"key": key, "row": row, "band": band, "fsync_usecs_per_op": usecs})
    groups = []
    for size in sorted({k[0] for k in keys}):
        for name in names:
            class_jobs = [j for j in jobs if j["key"][0] == size]
            groups.append(group_document(size, name, ALL_BANDS, class_jobs, iterations,
                                         presets.get(name), headroom_fraction))
            for band in sorted({j["band"] for j in class_jobs}):
                groups.append(group_document(
                    size, name, band, [j for j in class_jobs if j["band"] == band],
                    iterations, presets.get(name), headroom_fraction))
    return {
        "schema": VARIANCE_SCHEMA,
        "commit": commits[0] if commits else None,
        "rows_without_commit": sum(1 for r in probes if not r.get("commit")),
        "in_job_repeats": iterations,
        "fsync_band_edges_usecs_per_op": list(FSYNC_BAND_EDGES_USECS),
        "jobs": {
            "planned": len(planned_keys) if expected else None,
            "with_row": len(by_key),
            "missing": [{"class": k[0], "target_cache": k[1], "repeat": k[2]}
                        for k in planned_keys if k not in by_key],
            "by_vm": [{"class": j["key"][0], "target_cache": j["key"][1],
                       "repeat": j["key"][2], "fsync_band": j["band"],
                       "fsync_usecs_per_op": rounded(j["fsync_usecs_per_op"])} for j in jobs],
        },
        "groups": groups,
    }


def variance_markdown(document: dict) -> str:
    """A one-screen view of the document's per-class groups."""
    def num(value):
        if value is None:
            return "unknown"
        return f"{value:,.0f}" if abs(value) >= 1000 else f"{value:.4g}"

    def spread(metric: dict | None) -> str:
        if not metric:
            return "unknown"
        rtr = (metric.get("run_to_run") or {}).get("median_cv")
        return (f"{num(metric['all']['median'])} (n {metric['all']['n']}, CV "
                f"{num(metric['all']['cv'])}; VM CV {num(metric['vm_to_vm']['cv'])}; "
                f"run CV {num(rtr)})")

    planned = document["jobs"]["planned"]
    lines = ["## Runner probe variance (#542)", "",
             f"Commit {document['commit'] or 'unknown'}; {document['jobs']['with_row']} job(s) "
             f"with a row of {'an unknown number' if planned is None else planned} planned. "
             "CV is the sample standard deviation over the mean; VM CV is over each VM's "
             "median, run CV the median within one VM. `unknown` is a run that failed, timed "
             "out or never reported, never a zero.", "",
             "| Class | Preset | Runs measured | fsync bands | steady_state rate, shares/s | "
             "steady_state ACK p99, ms | peak used, MiB (MemTotal) | gated shortfall max | fits |",
             "|---|---|---|---|---|---|---|---|---|"]
    for group in document["groups"]:
        if group["fsync_band"] != ALL_BANDS:
            continue
        bands = sorted({vm["fsync_band"] for vm in document["jobs"]["by_vm"]
                        if vm["class"] == group["class"]})
        m = group["metrics"]
        e = group["size_evidence"] or {}
        fits = {True: "yes", False: "no", None: "unknown"}[e.get("fits")]
        lines.append(
            f"| {group['class']} vCPU | {group['preset']} | {group['runs']['measured']} of "
            f"{group['runs']['planned']} | {', '.join(bands)} | "
            f"{spread(m.get('steady_state.achieved_rate_shares_per_second'))} | "
            f"{spread(m.get('steady_state.ack_p99_ms'))} | "
            f"{num(e.get('peak_used_mib_max'))} ({num(e.get('mem_total_mib_min'))}) | "
            f"{num(e.get('gated_shortfall_max'))} | {fits}: {e.get('why', '')} |")
    lines.append("")
    return "\n".join(lines)


# --- verdict ------------------------------------------------------------


def verdict(probe_dir: Path, presets: list[str], iterations: int) -> list[str]:
    """What failed: the build, or a planned run whose gate did not exit 0
    (#487 lesson 5: the exit code is the verdict). A run with no
    measurement file is a failure, not a pass."""
    failed = []
    names = ["build"] + [f"{name}/{i}/probe-measure" for name in presets
                         for i in range(1, iterations + 1)]
    for name in names:
        document = read_json(probe_dir / f"{name}.json")
        code = document.get("exit_code") if isinstance(document, dict) else None
        if code != 0 or (document or {}).get("timed_out"):
            failed.append(f"{name}: exit {code}"
                          + (" (timed out)" if (document or {}).get("timed_out") else ""))
    return failed


# --- entry point --------------------------------------------------------


def optional_number(text: str) -> float | None:
    return None if text.strip() == "" else float(text)


def optional_int(text: str) -> int | None:
    return None if text.strip() == "" else int(text)


def fraction(text: str) -> float:
    value = float(text)
    if not math.isfinite(value) or not 0 <= value < 1:
        raise argparse.ArgumentTypeError(f"{text!r} is not a fraction in [0, 1)")
    return value


def positive_seconds(text: str) -> float:
    value = float(text)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError(f"{text!r} is not a positive number of seconds")
    return value


def write_json(path: Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def read_expected(path: Path | None) -> dict | None:
    if path is None:
        return None
    expected = read_json(path)
    if expected is None:
        raise ProbeError(f"--expected {path} is not readable JSON")
    return expected


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="action", required=True)

    m = sub.add_parser("matrix")
    m.add_argument("--classes", required=True)
    m.add_argument("--target-cache", required=True)
    m.add_argument("--fsync-vms", required=True)
    m.add_argument("--presets", default=",".join(PRESETS),
                   help="checked-in fake-node preset names, comma separated")
    m.add_argument("--repeats", default="1",
                   help=f"probe jobs per class and cache backend, 1..{MAX_REPEATS}")
    m.add_argument("--in-job-repeats", default="1",
                   help=f"runs of each preset inside one job, 1..{MAX_IN_JOB_REPEATS}")
    m.add_argument("--summary", type=Path, default=None,
                   help="append the plan and its spend ceiling (Markdown) to this file")

    me = sub.add_parser("measure")
    me.add_argument("--out", type=Path, required=True)
    me.add_argument("--timeout-seconds", type=positive_seconds, default=None)
    me.add_argument("command", nargs=argparse.REMAINDER)

    r = sub.add_parser("row")
    r.add_argument("--out", type=Path, required=True)
    r.add_argument("--class", dest="size", type=int, required=True)
    r.add_argument("--runner", required=True)
    r.add_argument("--target-cache", choices=TARGET_CACHES, required=True)
    r.add_argument("--repeat", type=int, default=1)
    r.add_argument("--commit", default=None)
    r.add_argument("--target-restored", choices=("true", "false"), required=True)
    r.add_argument("--target-bytes-before", type=optional_int, default=None)
    r.add_argument("--target-bytes-after", type=optional_int, default=None)
    r.add_argument("--restore-seconds", type=optional_number, default=None)
    r.add_argument("--save-seconds", type=optional_number, default=None)
    r.add_argument("--build", type=Path, required=True)
    r.add_argument("--host", type=Path, required=True)
    r.add_argument("--run", action="append", default=[],
                   help="PRESET=DIR, once per run; a preset given again is its next in-job repeat")

    f = sub.add_parser("fsync-row")
    f.add_argument("--out", type=Path, required=True)
    f.add_argument("--class", dest="size", type=int, required=True)
    f.add_argument("--runner", required=True)
    f.add_argument("--vm", type=int, required=True)
    f.add_argument("--host", type=Path, required=True)
    f.add_argument("--fsync", type=Path, required=True)

    h = sub.add_parser("host")
    h.add_argument("--out", type=Path, required=True)
    h.add_argument("--dir", type=Path, required=True)
    h.add_argument("--runner", default=None,
                   help="the runner label the job asked for, recorded as runner_label")

    t = sub.add_parser("table")
    t.add_argument("directory", type=Path)
    t.add_argument("--expected", type=Path, default=None,
                   help="the plan job's matrices as JSON, so a job that wrote no row is counted")

    v = sub.add_parser("variance")
    v.add_argument("directory", type=Path)
    v.add_argument("--expected", type=Path, default=None,
                   help="the plan job's matrices as JSON, so a job that wrote no row is counted")
    v.add_argument("--out", type=Path, default=None,
                   help="write the JSON document here (default: standard output)")
    v.add_argument("--markdown", action="store_true",
                   help="print a Markdown summary on standard output (needs --out)")
    v.add_argument("--headroom-fraction", type=fraction, default=DEFAULT_HEADROOM_FRACTION,
                   help="share of MemTotal that must stay available at peak for a class to fit")

    ve = sub.add_parser("verdict")
    ve.add_argument("--probe", type=Path, required=True)
    ve.add_argument("--presets", required=True)
    ve.add_argument("--in-job-repeats", type=int, required=True)

    args = parser.parse_args(argv)
    try:
        if args.action == "matrix":
            result = matrices(args.classes, args.target_cache, args.fsync_vms,
                              args.presets, args.repeats, args.in_job_repeats)
            for key, value in result.items():
                # A string is an output the workflow reads as text (the
                # preset list), not JSON: no quotes around it.
                text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
                print(f"{key}={text}")
            if args.summary is not None:
                with args.summary.open("a", encoding="utf-8") as summary:
                    summary.write(plan_summary(result) + "\n")
            return 0
        if args.action == "measure":
            command = args.command[1:] if args.command[:1] == ["--"] else args.command
            if not command:
                raise ProbeError("measure needs a command after --")
            result = measure(command, timeout_seconds=args.timeout_seconds)
            write_json(args.out, result)
            if result["timed_out"]:
                return 124
            return result["exit_code"] if result["exit_code"] >= 0 else 128 - result["exit_code"]
        if args.action == "host":
            facts = host_facts(args.dir)
            if args.runner:
                facts = {"runner_label": args.runner, **facts}
            write_json(args.out, facts)
            return 0
        if args.action == "row":
            known, _aliases = load_presets()
            write_json(args.out, build_row(args, set(known)))
            return 0
        if args.action == "fsync-row":
            write_json(args.out, build_fsync_row(args))
            return 0
        if args.action == "variance":
            if args.markdown and args.out is None:
                raise ProbeError("variance --markdown needs --out for the JSON document")
            known, _aliases = load_presets()
            document = variance(load_rows(args.directory), read_expected(args.expected),
                                known, args.headroom_fraction)
            if args.out is None:
                print(json.dumps(document, indent=2))
            else:
                write_json(args.out, document)
            if args.markdown:
                print(variance_markdown(document))
            return 0
        if args.action == "verdict":
            failed = verdict(args.probe, [n for n in args.presets.split(",") if n],
                             args.in_job_repeats)
            if failed:
                print("; ".join(failed), file=sys.stderr)
                return 1
            return 0
        print(table(load_rows(args.directory), read_expected(args.expected)))
        return 0
    except ProbeError as error:
        print(f"prism_load_probe: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
