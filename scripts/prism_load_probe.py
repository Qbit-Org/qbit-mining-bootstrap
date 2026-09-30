#!/usr/bin/env python3
"""The Blacksmith runner probe's helpers (#541, S1 of #487).

`.github/workflows/prism-load-runner-probe.yml` measures what each runner
class is before any lane depends on it. This script is every part of that
workflow that is not a shell one-liner, so it can be tested:

- `matrix`: the probe and fsync-spread job matrices from the dispatch inputs,
  refusing a class or a cache backend the workflow does not know;
- `measure`: run a command, sample the host's memory once a second while it
  runs, and write its wall time, exit code and peak memory as JSON. It exits
  with the command's code;
- `row`: one runner class's result from the measurements, the harness's side
  report, the gate's exit code and `pg_test_fsync`'s output;
- `fsync-row`: one fsync-spread VM's `pg_test_fsync` result;
- `host`: the host fingerprint, with what the kernel says about the block
  device under a directory (its write cache and FUA support), which is the
  evidence for whether `fsync` reaches stable storage;
- `table`: the per-class results table (Markdown) from every row.

Peak memory is the host's (MemTotal - MemAvailable), not one process's: the
harness, its frontends and both PostgreSQL clusters share the VM, and what
sizes a runner is whether all of them fit (#487 lesson 3).

Usage:
  prism_load_probe.py matrix --classes 8,16,32 --target-cache both --fsync-vms 3
  prism_load_probe.py measure --out build.json -- cargo build ...
  prism_load_probe.py row --out row.json --class 8 ... --run short-plan-20k-window-1fe=DIR ...
  prism_load_probe.py fsync-row --out row.json --class 8 --vm 1 --fsync FILE
  prism_load_probe.py host --out host.json --dir "$RUNNER_TEMP/pload" [--runner LABEL]
  prism_load_probe.py table DIR
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import time

ROW_SCHEMA = "qbit.prism.runner-probe-row.v1"
# Blacksmith's x64 Ubuntu 24.04 classes; the probe is for 8, 16 and 32.
CLASSES = (2, 4, 8, 16, 32)
TARGET_CACHES = ("actions-cache", "sticky-disk")
MAX_FSYNC_VMS = 10
# The presets the probe runs, in order (#541: one short and one D1 at 20k).
PRESETS = ("short-plan-20k-window-1fe", "throughput-20k-window-1fe")
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


def parse_fsync_vms(text: str) -> int:
    text = text.strip()
    if not text.isdigit() or int(text) > MAX_FSYNC_VMS:
        raise ProbeError(f"fsync VMs {text!r} is not a whole number 0..{MAX_FSYNC_VMS}")
    return int(text)


def matrices(classes: str, target_cache: str, fsync_vms: str) -> dict[str, dict]:
    sizes = parse_classes(classes)
    caches = parse_target_caches(target_cache)
    vms = parse_fsync_vms(fsync_vms)
    probe = [
        {"class": size, "runner": runner_label(size), "target_cache": cache}
        for size in sizes
        for cache in caches
    ]
    fsync = [
        {"class": size, "runner": runner_label(size), "vm": vm}
        for size in sizes
        for vm in range(1, vms + 1)
    ]
    # An empty include list is not a valid matrix, so an fsync spread of 0
    # VMs is carried as a flag the workflow's `if:` reads instead.
    return {
        "probe": {"include": probe},
        "fsync": {"include": fsync or [{"class": 0, "runner": "", "vm": 0}]},
        "fsync_enabled": bool(fsync),
    }


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


def measure(command: list[str], interval: float = 1.0) -> dict:
    baseline = read_meminfo()
    samples = [used_kib(baseline)]
    started = time.monotonic()
    child = subprocess.Popen(command)
    while True:
        try:
            code = child.wait(timeout=interval)
            break
        except subprocess.TimeoutExpired:
            samples.append(used_kib(read_meminfo()))
    wall = time.monotonic() - started
    samples.append(used_kib(read_meminfo()))
    known = [s for s in samples if s is not None]
    return {
        "command": command,
        "exit_code": code,
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


def run_result(directory: Path) -> dict:
    report = read_json(directory / "load-harness-report.json")
    measured = read_json(directory / "probe-measure.json") or {}
    result = {
        "harness_exit_code": read_int(directory / "harness-exit-code"),
        "gate_exit_code": measured.get("exit_code"),
        "wall_seconds": measured.get("wall_seconds"),
        "peak_used_mib": measured.get("peak_used_mib"),
        "baseline_used_mib": measured.get("baseline_used_mib"),
        "fsync": read_fsync(directory / "pg_test_fsync.txt"),
        "provenance": provenance(report),
        "seed_seconds": None,
        "seed_rows": None,
        "phases": {},
        "tip_last_notify_p99_ms": None,
        "tips_missing_a_session": None,
    }
    if report is None:
        return result
    seed = ((report.get("window") or {}).get("seed")) or {}
    result["seed_seconds"] = seed.get("seconds")
    result["seed_rows"] = seed.get("rows")
    for phase in report.get("phases") or []:
        result["phases"][phase.get("name")] = {
            "seconds": phase.get("duration_seconds"),
            "shortfall": phase.get("shortfall"),
            "completed": phase.get("completed"),
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
    return result


def parse_run(text: str) -> tuple[str, Path]:
    name, sep, directory = text.partition("=")
    if not sep or name not in PRESETS:
        raise ProbeError(f"--run {text!r} is not <{'|'.join(PRESETS)}>=<directory>")
    return name, Path(directory)


def build_row(args: argparse.Namespace) -> dict:
    runs = dict(parse_run(text) for text in args.run)
    return {
        "schema": ROW_SCHEMA,
        "kind": "probe",
        "class": args.size,
        "runner": args.runner,
        "target_cache": args.target_cache,
        "target": {
            "restored": args.target_restored == "true",
            "bytes_before_build": args.target_bytes_before,
            "bytes_after_build": args.target_bytes_after,
            "restore_seconds": args.restore_seconds,
            "save_seconds": args.save_seconds,
        },
        "build": read_json(args.build) or {},
        "host": read_json(args.host) or {},
        "runs": {name: run_result(directory) for name, directory in runs.items()},
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
        if isinstance(row, dict) and row.get("schema") == ROW_SCHEMA:
            rows.append(row)
    return rows


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
    probes = sorted(
        (r for r in rows if r.get("kind") == "probe"),
        key=lambda r: (r["class"], r["target_cache"]),
    )
    lines = ["## Runner probe (#541)", ""]
    if expected:
        present = {(r["class"], r["target_cache"]) for r in probes}
        absent = [e for e in expected["probe"]["include"]
                  if (e["class"], e["target_cache"]) not in present]
        for entry in absent:
            lines += [f"### {entry['class']} vCPU · {entry['target_cache']}: no row "
                      "(the job failed before writing it; see its log)", ""]
    if not probes:
        lines += ["No probe row was produced.", ""]
    for row in probes:
        build = row.get("build") or {}
        target = row.get("target") or {}
        host = row.get("host") or {}
        lines += [
            f"### {row['class']} vCPU · {row['target_cache']} "
            f"({'warm' if target.get('restored') else 'cold'} target)",
            "",
            f"`{row['runner']}` · {host.get('cpu_model', 'unknown CPU')} · "
            f"{cell(host.get('nproc'))} CPUs · {cell(host.get('mem_total_mib'), ' MiB')}",
            "",
            f"Cluster filesystem: {disk_line(host)}",
            "",
            "| Measure | " + " | ".join(PRESETS) + " |",
            "|---|" + "---|" * len(PRESETS),
        ]
        runs = row.get("runs") or {}

        def per_run(label: str, pick) -> str:
            return f"| {label} | " + " | ".join(
                pick(runs[name]) if name in runs else "not run" for name in PRESETS
            ) + " |"

        lines += [
            per_run("pg_test_fsync fdatasync, one 8 kB write",
                    lambda r: f"{cell(r['fsync']['ops_per_second'], ' ops/s')} "
                              f"({cell(r['fsync']['usecs_per_op'], ' µs/op')})"),
            per_run("seeding", lambda r: f"{cell(r['seed_seconds'], ' s')} "
                                         f"({cell(r['seed_rows'])} rows)"),
            per_run("run wall time (pg_test_fsync, harness, gate)",
                    lambda r: cell(r["wall_seconds"], " s")),
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


# --- entry point --------------------------------------------------------


def optional_number(text: str) -> float | None:
    return None if text.strip() == "" else float(text)


def optional_int(text: str) -> int | None:
    return None if text.strip() == "" else int(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="action", required=True)

    m = sub.add_parser("matrix")
    m.add_argument("--classes", required=True)
    m.add_argument("--target-cache", required=True)
    m.add_argument("--fsync-vms", required=True)

    me = sub.add_parser("measure")
    me.add_argument("--out", type=Path, required=True)
    me.add_argument("command", nargs=argparse.REMAINDER)

    r = sub.add_parser("row")
    r.add_argument("--out", type=Path, required=True)
    r.add_argument("--class", dest="size", type=int, required=True)
    r.add_argument("--runner", required=True)
    r.add_argument("--target-cache", choices=TARGET_CACHES, required=True)
    r.add_argument("--target-restored", choices=("true", "false"), required=True)
    r.add_argument("--target-bytes-before", type=optional_int, default=None)
    r.add_argument("--target-bytes-after", type=optional_int, default=None)
    r.add_argument("--restore-seconds", type=optional_number, default=None)
    r.add_argument("--save-seconds", type=optional_number, default=None)
    r.add_argument("--build", type=Path, required=True)
    r.add_argument("--host", type=Path, required=True)
    r.add_argument("--run", action="append", default=[])

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

    args = parser.parse_args(argv)
    try:
        if args.action == "matrix":
            result = matrices(args.classes, args.target_cache, args.fsync_vms)
            for key, value in result.items():
                print(f"{key}={json.dumps(value, separators=(',', ':'))}")
            return 0
        if args.action == "measure":
            command = args.command[1:] if args.command[:1] == ["--"] else args.command
            if not command:
                raise ProbeError("measure needs a command after --")
            result = measure(command)
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
            return result["exit_code"] if result["exit_code"] >= 0 else 128 - result["exit_code"]
        if args.action == "host":
            args.out.parent.mkdir(parents=True, exist_ok=True)
            facts = host_facts(args.dir)
            if args.runner:
                facts = {"runner_label": args.runner, **facts}
            args.out.write_text(json.dumps(facts, indent=2) + "\n", encoding="utf-8")
            return 0
        if args.action in ("row", "fsync-row"):
            row = build_row(args) if args.action == "row" else build_fsync_row(args)
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(row, indent=2) + "\n", encoding="utf-8")
            return 0
        expected = None
        if args.expected is not None:
            expected = read_json(args.expected)
            if expected is None:
                raise ProbeError(f"--expected {args.expected} is not readable JSON")
        print(table(load_rows(args.directory), expected))
        return 0
    except ProbeError as error:
        print(f"prism_load_probe: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
