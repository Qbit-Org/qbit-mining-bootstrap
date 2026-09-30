#!/usr/bin/env python3
"""Run one load-harness preset against two builds, interleaved, on one host (#511).

The release benchmark: `--base` and `--candidate` are git refs. Each is built
in release mode in its own clean detached worktree, and each run uses that
build's own harness and server (a harness never drives another build's
server). The series:

- takes the host's benchmark lock for its whole length, so no other series
  shares the host;
- records `pg_test_fsync` on the filesystem the harness builds its clusters
  on, before and after the series;
- runs `--repeats` pairs, alternating which build goes first (base then
  candidate, then candidate then base, ...), so a drift in the host over the
  series does not land on one build;
- before each run waits `--cooldown-seconds`, then until the 1-minute load
  average is under `--max-load` (giving up after `--load-wait-seconds`);
- samples the load average and MemAvailable every `--sample-seconds` during
  each run and snapshots the process table at each end;
- kills a run that outlives `--run-ceiling-seconds`.

Every flag the result depends on comes from the preset, checked in beside
this script and read from this checkout, never from a build's defaults. A
build whose harness predates a flag the preset pins runs without it only
when `crates/qbit-prism-load/legacy-flags.json` says its value is what that
build ran anyway; otherwise the pair is refused before anything runs, as is
a build whose harness has a result flag the preset does not pin.

The series is written to `--out`: `manifest.json` (rewritten after every
run, so an interrupted series resumes with `--resume`), `builds/<label>/`
(the worktrees), `runs/<run id>/` (each harness's `--out`) with the run's
log, host samples and process snapshots beside it, `pg_test_fsync-*.txt`,
and `comparison.md`, `qbit-prism-load-compare`'s table. The exit status is
the comparison's: 0 the candidate meets #473's D1 rule in every gated phase,
1 it does not, 2 bad inputs or a failed build, 3 the series could not finish
(the host never went quiet).

Usage:
  python3 scripts/prism_load_ab.py --base 5d0042f6 --candidate origin/3.x.x \\
      --preset throughput-20k-window-1fe --out ab-throughput-20k-window-1fe
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from typing import Any, Callable, Iterator

from prism_load_matrix import load_aliases

ROOT = Path(__file__).resolve().parents[1]
CRATE = ROOT / "crates" / "qbit-prism-load"
PRESETS = CRATE / "presets"
LEGACY_FLAGS = CRATE / "legacy-flags.json"
PRESET_SOURCE = CRATE / "src" / "preset.rs"
PRESET_SCHEMA = "qbit.prism.load-preset.v1"
LEGACY_SCHEMA = "qbit.prism.load-legacy-flags.v1"
MANIFEST_SCHEMA = "qbit.prism.load-ab.v1"
LABELS = ("base", "candidate")
FORMULAS = ("sessions-per-frontend-plus-16-min-128",)

EXIT_PASS, EXIT_FAIL, EXIT_INPUT, EXIT_INCOMPLETE = 0, 1, 2, 3


class DriverError(Exception):
    """A refusal worded for the operator; exits 2."""


class IncompleteSeries(Exception):
    """The series could not finish on this host; exits 3."""


# --- presets and flags ------------------------------------------------------


def required_pg_binaries(source: Path = CRATE / "src" / "cluster.rs") -> tuple[str, ...]:
    """The server binaries the harness refuses to start without, read from
    `cluster.rs` so the list has one source."""
    match = re.search(r"pub const REQUIRED_BINARIES: \[&str; \d+\] = \[(.*?)\];", source.read_text(encoding="utf-8"))
    if not match:
        raise DriverError(f"{source}: REQUIRED_BINARIES not found")
    return tuple(re.findall(r'"([a-z_]+)"', match.group(1)))


def operational_flags(source: Path = PRESET_SOURCE) -> frozenset[str]:
    """The harness's operational flags, read from `preset.rs` so the list
    has one source."""
    text = source.read_text(encoding="utf-8")
    match = re.search(r"pub const OPERATIONAL_FLAGS: &\[&str\] = &\[(.*?)\];", text, re.S)
    if not match:
        raise DriverError(f"{source}: OPERATIONAL_FLAGS not found")
    flags = frozenset(re.findall(r'"(--[a-z0-9-]+)"', match.group(1)))
    if "--out" not in flags:
        raise DriverError(f"{source}: OPERATIONAL_FLAGS does not name --out")
    return flags


def load_preset(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        preset = json.loads(raw)
    except (OSError, ValueError) as error:
        raise DriverError(f"preset {path}: {error}") from error
    if preset.get("schema") != PRESET_SCHEMA:
        raise DriverError(f"preset {path}: schema is {preset.get('schema')!r}, not {PRESET_SCHEMA}")
    if preset.get("name") != path.stem or not isinstance(preset.get("args"), dict):
        raise DriverError(f"preset {path}: needs a name equal to its stem and an args object")
    preset["_sha256"] = hashlib.sha256(raw).hexdigest()
    preset["_path"] = str(path)
    return preset


def load_legacy(path: Path = LEGACY_FLAGS) -> dict[str, dict[str, Any]]:
    table = json.loads(path.read_text(encoding="utf-8"))
    if table.get("schema") != LEGACY_SCHEMA:
        raise DriverError(f"{path}: schema is {table.get('schema')!r}, not {LEGACY_SCHEMA}")
    flags = table["flags"]
    for flag, rule in flags.items():
        kinds = [k for k in ("equals", "equals_flag", "inert_when", "formula") if k in rule]
        if len(kinds) != 1:
            raise DriverError(f"{path}: {flag} needs exactly one rule, has {kinds}")
        if "formula" in rule and rule["formula"] not in FORMULAS:
            raise DriverError(f"{path}: {flag}: unknown formula {rule['formula']!r}")
    return flags


def same(a: Any, b: Any) -> bool:
    """JSON equality with numbers compared as numbers (0 == 0.0), and a
    boolean never equal to a number."""
    numeric = lambda v: isinstance(v, (int, float)) and not isinstance(v, bool)  # noqa: E731
    if numeric(a) and numeric(b):
        return float(a) == float(b)
    return type(a) is type(b) and a == b


def legacy_holds(flag: str, rule: dict[str, Any], args: dict[str, Any]) -> tuple[bool, str]:
    """Whether a build without `flag` runs what the preset's value asks."""
    value = args.get(flag)
    if "equals" in rule:
        return same(value, rule["equals"]), f"needs {json.dumps(rule['equals'])}"
    if "equals_flag" in rule:
        other = rule["equals_flag"]
        return (
            other in args and same(value, args[other]),
            f"needs the value of {other} ({json.dumps(args.get(other))})",
        )
    if "inert_when" in rule:
        wanted = rule["inert_when"]
        holds = all(k in args and same(args[k], v) for k, v in wanted.items())
        return holds, "is read only when " + ", ".join(
            f"{k} is not {json.dumps(v)}" for k, v in wanted.items()
        )
    # sessions-per-frontend-plus-16-min-128
    sessions, frontends = args.get("--sessions"), args.get("--frontends")
    if not all(isinstance(n, int) and not isinstance(n, bool) and n > 0 for n in (sessions, frontends)):
        return False, "needs --sessions and --frontends to derive the old admission"
    implied = max(math.ceil(sessions / frontends) + 16, 128)
    return same(value, implied), f"needs {implied} (ceil(sessions / frontends) + 16, at least 128)"


def argv_words(flag: str, value: Any) -> list[str]:
    """As `Preset::argv`: `true` bare, `false` and `null` omitted."""
    if value is True:
        return [flag]
    if value is False or value is None:
        return []
    if isinstance(value, str):
        return [flag, value]
    if isinstance(value, (int, float)):
        return [flag, json.dumps(value)]
    raise DriverError(f"{flag} is {value!r}, not a string, number, boolean or null")


def resolve_argv(
    args: dict[str, Any],
    build_flags: frozenset[str],
    legacy: dict[str, dict[str, Any]],
    operational: frozenset[str],
    label: str,
) -> tuple[list[str], list[str]]:
    """The preset's command line for one build, and the flags it left off."""
    words: list[str] = []
    dropped: list[str] = []
    problems: list[str] = []
    for flag in sorted(args):
        value = args[flag]
        if flag in build_flags:
            words += argv_words(flag, value)
            continue
        rule = legacy.get(flag)
        if rule is None:
            problems.append(
                f"its harness has no {flag} and legacy-flags.json has no rule for what it ran instead"
            )
            continue
        holds, needs = legacy_holds(flag, rule, args)
        if holds:
            dropped.append(flag)
        else:
            problems.append(
                f"its harness has no {flag}; the preset's {json.dumps(value)} is a workload it "
                f"cannot run (leaving the flag off {needs})"
            )
    unpinned = sorted(build_flags - set(args) - operational - {"--help", "--version"})
    for flag in unpinned:
        problems.append(f"its harness has result flag {flag}, which the preset does not pin")
    if problems:
        raise DriverError(f"the {label} build cannot run this preset:\n  - " + "\n  - ".join(problems))
    return words, dropped


def help_flags(harness: Path) -> frozenset[str]:
    """Every long flag a harness binary accepts, from its `--help`."""
    result = subprocess.run([str(harness), "--help"], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise DriverError(f"{harness} --help exited {result.returncode}: {result.stderr.strip()}")
    return parse_help_flags(result.stdout)


def parse_help_flags(text: str) -> frozenset[str]:
    """The long flags clap's `--help` defines: the first on an indented
    option line, never one a description mentions (`pg_config --bindir`)."""
    return frozenset(re.findall(r"^\s+(?:-[A-Za-z0-9], )?(--[a-z0-9][a-z0-9-]*)", text, re.M))


# --- host -------------------------------------------------------------------


def load1() -> float:
    return float(Path("/proc/loadavg").read_text().split()[0])


def mem_available_mib() -> int | None:
    with contextlib.suppress(OSError):
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) // 1024
    return None


def host_facts() -> dict[str, Any]:
    mem_total = None
    with contextlib.suppress(OSError):
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                mem_total = int(line.split()[1]) // 1024
    return {
        "hostname": socket.gethostname(),
        "nproc": os.cpu_count(),
        "mem_total_mib": mem_total,
        "kernel": platform.release(),
    }


FSYNC_LINE = re.compile(r"^\s*fdatasync\s+([\d.]+) ops/sec\s+(\d+) usecs/op", re.M)


def parse_pg_test_fsync(text: str) -> dict[str, float] | None:
    """The fdatasync line of the one-8 kB-write section, or `None`."""
    start = text.find("using one 8kB write")
    if start < 0:
        return None
    section = text[start:]
    end = section.find("\nCompare", 1)
    match = FSYNC_LINE.search(section if end < 0 else section[:end])
    if not match:
        return None
    return {"fdatasync_ops_per_second": float(match.group(1)), "fdatasync_usecs_per_op": float(match.group(2))}


def record_fsync(pg_bin: Path, tmpdir: Path, out: Path, seconds: int) -> dict[str, Any]:
    probe = tmpdir / "pg_test_fsync.out"
    try:
        result = subprocess.run(
            [str(pg_bin / "pg_test_fsync"), "-s", str(seconds), "-f", str(probe)],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as error:
        raise DriverError(f"running {pg_bin / 'pg_test_fsync'}: {error}") from error
    with contextlib.suppress(OSError):
        probe.unlink()
    out.write_text(f"# pg_test_fsync on {tmpdir}\n{result.stdout}{result.stderr}", encoding="utf-8")
    parsed = parse_pg_test_fsync(result.stdout) if result.returncode == 0 else None
    return {"file": out.name, "exit_code": result.returncode, **(parsed or {})}


@contextlib.contextmanager
def series_lock(path: Path) -> Iterator[None]:
    """The series directory's own lock; a second invocation on the same
    `--out` is refused rather than queued."""
    with open(path, "a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise DriverError(f"another invocation is using {path.parent}") from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


@contextlib.contextmanager
def benchmark_lock(path: Path) -> Iterator[None]:
    with open(path, "a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"waiting for the benchmark lock {path}", file=sys.stderr, flush=True)
            fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def wait_for_quiet(
    max_load: float,
    wait_seconds: float,
    poll_seconds: float = 10.0,
    read_load: Callable[[], float] = load1,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> float:
    """Block until the 1-minute load is under `max_load`; the load seen."""
    deadline = now() + wait_seconds
    while True:
        load = read_load()
        if load < max_load:
            return load
        if now() >= deadline:
            raise IncompleteSeries(
                f"the 1-minute load stayed at or above {max_load} for {wait_seconds:.0f} s "
                f"(last {load:.2f}); the series stops rather than run on a loaded host"
            )
        sleep(poll_seconds)


def interleaved(repeats: int) -> list[tuple[int, str]]:
    """(repeat, label) in run order: base first on odd repeats, the
    candidate first on even ones."""
    order = []
    for repeat in range(1, repeats + 1):
        pair = LABELS if repeat % 2 else tuple(reversed(LABELS))
        order += [(repeat, label) for label in pair]
    return order


# --- builds -----------------------------------------------------------------


def git(*args: str, cwd: Path = ROOT) -> str:
    result = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise DriverError(f"git {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout.strip()


def prepare_build(label: str, ref: str, out: Path, skip_build: bool) -> dict[str, Any]:
    commit = git("rev-parse", "--verify", f"{ref}^{{commit}}")
    tree = out / "builds" / label
    if tree.exists():
        head = git("rev-parse", "HEAD", cwd=tree)
        if head != commit:
            raise DriverError(f"{tree} is at {head}, not {ref} ({commit}); remove it or choose another --out")
    else:
        tree.parent.mkdir(parents=True, exist_ok=True)
        git("worktree", "add", "--detach", str(tree), commit)
    if git("status", "--porcelain", cwd=tree):
        raise DriverError(f"{tree} has local changes; the harness would refuse its dirty tree")
    if not skip_build:
        env = {k: v for k, v in os.environ.items() if k != "CARGO_TARGET_DIR"}
        print(f"building {label} ({ref}, {commit[:8]}) in {tree}", file=sys.stderr, flush=True)
        result = subprocess.run(
            ["cargo", "build", "--locked", "--release", "-p", "qbit-prism-server", "-p", "qbit-prism-load"],
            cwd=tree,
            env=env,
            check=False,
        )
        if result.returncode != 0:
            raise DriverError(f"building {label} ({ref}) failed with exit {result.returncode}")
    release = tree / "target" / "release"
    for binary in ("qbit-prism-load", "qbit-prism-server"):
        if not (release / binary).is_file():
            raise DriverError(f"{release / binary} is missing")
    return {"label": label, "ref": ref, "commit": commit, "worktree": str(tree.relative_to(out))}


# --- one run ----------------------------------------------------------------


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def process_snapshot(path: Path) -> None:
    result = subprocess.run(
        ["ps", "-eo", "pid,ppid,pcpu,rss,etime,args", "--sort=-pcpu"],
        capture_output=True,
        text=True,
        check=False,
    )
    path.write_text("\n".join(result.stdout.splitlines()[:60]) + "\n", encoding="utf-8")


def group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def stop_group(child: subprocess.Popen, grace_seconds: float = 60.0, poll_seconds: float = 0.2) -> None:
    """SIGTERM the child's process group, then SIGKILL whatever of it is
    left after `grace_seconds`. The group, not the leader, is waited for: a
    server or PostgreSQL the harness started can outlive the harness, and
    the benchmark lock must not be released while one does."""
    pgid = child.pid
    with contextlib.suppress(ProcessLookupError):
        os.killpg(pgid, signal.SIGTERM)
    deadline = time.monotonic() + grace_seconds
    while True:
        # Reap the leader, and any member orphaned to the driver as the
        # subreaper, so a zombie does not keep the group alive.
        child.poll()
        reap_orphans(keep=child.pid)
        if not group_alive(pgid):
            return
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_seconds)
    with contextlib.suppress(ProcessLookupError):
        os.killpg(pgid, signal.SIGKILL)
    child.wait()
    # SIGKILL cannot be refused; what can linger is a zombie no init reaps,
    # which holds no resources, so the wait for its reaping is bounded.
    reaped_by = time.monotonic() + 10.0
    while group_alive(pgid) and time.monotonic() < reaped_by:
        reap_orphans(keep=child.pid)
        time.sleep(poll_seconds)


PR_SET_CHILD_SUBREAPER = 36


def become_subreaper() -> bool:
    """Make the driver the reaper of every orphan below it (Linux). The
    harness's servers and PostgreSQL each call setsid(), so they leave the
    harness's process group; as a subreaper the driver still finds them, as
    its descendants, when the harness dies before stopping them."""
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        return libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) == 0
    except (OSError, AttributeError):
        return False


def descendants(root: int) -> list[int]:
    """Every live or unreaped process below `root`, from /proc."""
    children: dict[int, list[int]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
        except OSError:
            continue
        children.setdefault(int(fields[1]), []).append(int(entry.name))
    found, queue = [], list(children.get(root, []))
    while queue:
        pid = queue.pop()
        found.append(pid)
        queue.extend(children.get(pid, []))
    return found


def _alive(pid: int) -> bool:
    """Running, as opposed to gone or a zombie; reaps it if it is ours."""
    with contextlib.suppress(ChildProcessError, OSError):
        os.waitpid(pid, os.WNOHANG)
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except OSError:
        return False
    return state != "Z"


def reap_orphans(keep: int | None = None) -> None:
    """Collect the exit status of every finished process below the driver
    that has become its child, except `keep` (the harness, whose status its
    Popen collects)."""
    for pid in descendants(os.getpid()):
        if pid != keep:
            _alive(pid)


def sweep_descendants(grace_seconds: float = 60.0, poll_seconds: float = 0.2) -> None:
    """Stop every process still below the driver, whatever its session:
    SIGTERM, then SIGKILL after `grace_seconds`, reaping each. Runs only
    once a run's harness is gone, when nothing below the driver belongs to
    anything but that run."""
    me = os.getpid()
    live = lambda: [pid for pid in descendants(me) if _alive(pid)]  # noqa: E731
    for pid in live():
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + grace_seconds
    while live() and time.monotonic() < deadline:
        time.sleep(poll_seconds)
    killed_by = time.monotonic() + 10.0
    while (remaining := live()) and time.monotonic() < killed_by:
        for pid in remaining:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
        time.sleep(poll_seconds)


def execute_run(
    run_id: str,
    command: list[str],
    cwd: Path,
    env: dict[str, str],
    runs_dir: Path,
    ceiling_seconds: float,
    sample_seconds: float,
) -> dict[str, Any]:
    """Run one harness with host sampling and a ceiling."""
    stem = runs_dir / run_id
    samples: list[dict[str, Any]] = []
    stop = threading.Event()

    def sample() -> None:
        with open(f"{stem}.host.jsonl", "w", encoding="utf-8") as handle:
            while not stop.is_set():
                row = {"t": time.time(), "load1": load1(), "mem_available_mib": mem_available_mib()}
                samples.append(row)
                handle.write(json.dumps(row) + "\n")
                handle.flush()
                stop.wait(sample_seconds)

    process_snapshot(Path(f"{stem}.ps-before.txt"))
    started = dt.datetime.now(dt.timezone.utc)
    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    ceiling_hit = False
    exit_code: int | None = None
    with open(f"{stem}.log", "w", encoding="utf-8") as log:
        child: subprocess.Popen | None = None
        try:
            # A termination signal that lands while Popen is forking would
            # raise before `child` exists; it is held until the spawn
            # returns and raised after it, where the `finally` sees the child.
            with deferred_termination() as pending:
                child = subprocess.Popen(command, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT,
                                         start_new_session=True)
            if pending:
                raise Terminated(pending[0])
            try:
                exit_code = child.wait(timeout=ceiling_seconds)
            except subprocess.TimeoutExpired:
                ceiling_hit = True
        finally:
            # However the wait ended (exit, ceiling, Ctrl-C, a signal),
            # nothing of the run outlives it: a server or PostgreSQL the
            # harness started can outlive the harness, and the benchmark lock
            # must not be released while one does. Termination is held while
            # the group is stopped and raised once it is.
            with deferred_termination() as held:
                if child is not None and (child.poll() is None or group_alive(child.pid)):
                    stop_group(child)
                # The servers and PostgreSQL left the harness's group with
                # setsid(); as the subreaper the driver finds them below it.
                sweep_descendants()
            stop.set()
            if held:
                raise Terminated(held[0])
    stop.set()
    sampler.join()
    process_snapshot(Path(f"{stem}.ps-after.txt"))
    loads = [row["load1"] for row in samples]
    mems = [row["mem_available_mib"] for row in samples if row["mem_available_mib"] is not None]
    return {
        "started_at": started.isoformat(),
        "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "exit_code": exit_code,
        "ceiling_hit": ceiling_hit,
        "load_max": max(loads) if loads else None,
        "mem_available_min_mib": min(mems) if mems else None,
    }


# --- the series -------------------------------------------------------------


def positive(kind: type) -> Callable[[str], Any]:
    def parse(text: str) -> Any:
        value = kind(text)
        if not (value > 0 and math.isfinite(value)):
            raise argparse.ArgumentTypeError(f"{text!r} is not a positive finite number")
        return value

    return parse


def non_negative(text: str) -> float:
    value = float(text)
    if not (value >= 0 and math.isfinite(value)):
        raise argparse.ArgumentTypeError(f"{text!r} is not a non-negative finite number")
    return value


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", required=True, help="git ref of the build compared against")
    parser.add_argument("--candidate", required=True, help="git ref of the build under test")
    parser.add_argument("--preset", required=True, help="a preset name under crates/qbit-prism-load/presets, or a path")
    parser.add_argument("--out", required=True, type=Path, help="the series directory")
    parser.add_argument("--repeats", type=positive(int), default=3)
    parser.add_argument("--pg-bin-dir", type=Path, default=Path(os.environ.get("PG_BIN_DIR", "/usr/lib/postgresql/16/bin")))
    parser.add_argument("--cooldown-seconds", type=non_negative, default=60.0)
    parser.add_argument("--max-load", type=positive(float), default=3.0, help="start a run only under this 1-minute load")
    parser.add_argument("--load-wait-seconds", type=non_negative, default=3600.0)
    parser.add_argument("--sample-seconds", type=positive(float), default=10.0)
    parser.add_argument("--run-ceiling-seconds", type=positive(float), default=2400.0)
    parser.add_argument("--fsync-seconds", type=positive(int), default=5, help="pg_test_fsync -s")
    parser.add_argument("--lock-file", type=Path, default=Path("/tmp/qbit-prism-bench.lock"))
    parser.add_argument("--tmpdir", type=Path, default=Path(os.environ.get("PRISM_LOAD_TMPDIR", "/tmp/pload-ab")),
                        help="the harness's TMPDIR, where it builds its clusters; kept short for the socket path")
    parser.add_argument("--compare-bin", type=Path, help="a built qbit-prism-load-compare (default: build this checkout's)")
    parser.add_argument("--resume", action="store_true", help="keep the runs an interrupted series already recorded")
    parser.add_argument("--skip-build", action="store_true", help="use the worktrees' existing release builds")
    parser.add_argument("--dry-run", action="store_true", help="build and resolve both command lines, run nothing")
    return parser.parse_args(argv)


# Settings a resumed series may change: none of them touches what is
# measured or how the host is isolated. Every other setting (the load gate,
# cooldown, sampling, ceiling, lock, the clusters' filesystem, the repeat
# count) must stay as the series began.
RESUME_FREE = ("resume", "skip_build", "dry_run", "compare_bin")


def settings_of(options: argparse.Namespace) -> dict[str, Any]:
    """The settings the manifest records, as JSON values."""
    return {
        key: str(value) if isinstance(value, Path) else value
        for key, value in vars(options).items()
        if key not in ("base", "candidate", "preset", "out")
    }


def check_resumable(previous: dict[str, Any], options: argparse.Namespace) -> None:
    problems = []
    host = host_facts()
    for key, was in previous["host"].items():
        if host.get(key) != was:
            problems.append(f"the host's {key} was {was}, not {host.get(key)}")
    for key, now in settings_of(options).items():
        was = previous["settings"].get(key)
        if key not in RESUME_FREE and was != now:
            problems.append(f"--{key.replace('_', '-')} was {was}, not {now}")
    if problems:
        raise DriverError("cannot resume: " + "; ".join(problems) + "; start a new --out instead")


def preset_path(name: str) -> Path:
    """A path as given; a name from the presets directory, a deprecated one
    through `aliases.txt` with a warning, as the nightly matrix resolves it."""
    path = Path(name)
    if path.suffix == ".json" or os.sep in name:
        return path.resolve()
    if not (PRESETS / f"{name}.json").exists():
        renamed = load_aliases(PRESETS).get(name)
        if renamed:
            print(f"warning: preset {name} is deprecated; it is now {renamed}", file=sys.stderr)
            name = renamed
    return PRESETS / f"{name}.json"


_built_comparator: Path | None = None


def compare_bin(explicit: Path | None) -> Path:
    global _built_comparator
    if explicit:
        return explicit
    if _built_comparator is not None:
        return _built_comparator
    built = subprocess.run(
        ["cargo", "build", "--locked", "--release", "-p", "qbit-prism-load", "--bin", "qbit-prism-load-compare"],
        cwd=ROOT,
        check=False,
    )
    if built.returncode != 0:
        raise DriverError(f"building qbit-prism-load-compare failed with exit {built.returncode}")
    _built_comparator = comparator_path(os.environ.get("CARGO_TARGET_DIR"))
    return _built_comparator


def comparator_path(target_dir: str | None) -> Path:
    """Where cargo, run from ROOT, puts the comparator: a relative
    CARGO_TARGET_DIR is relative to that working directory, not ours."""
    target = ROOT / (target_dir or "target")
    return target / "release" / "qbit-prism-load-compare"


def validate_preset(comparator: Path, preset: str) -> None:
    try:
        result = subprocess.run([str(comparator), "--validate-preset", "--preset", preset],
                                capture_output=True, text=True, check=False)
    except OSError as error:
        raise DriverError(f"running {comparator}: {error}") from error
    if result.returncode != 0:
        raise DriverError(f"preset {preset} is not valid: {result.stderr.strip() or result.stdout.strip()}")


def main(argv: list[str]) -> int:
    options = parse_args(argv)
    # Every path absolute: the harness runs with its worktree as its working
    # directory, so a relative TMPDIR would name a different directory there
    # than the one pg_test_fsync probed.
    for key in ("tmpdir", "pg_bin_dir", "lock_file", "compare_bin"):
        if getattr(options, key) is not None:
            setattr(options, key, getattr(options, key).resolve())
    out: Path = options.out.resolve()
    preset = load_preset(preset_path(options.preset))
    legacy = load_legacy()
    operational = operational_flags()
    manifest_path = out / "manifest.json"
    if not options.dry_run:
        missing = [
            str(options.pg_bin_dir / name)
            for name in ("pg_test_fsync", *required_pg_binaries())
            if not os.access(options.pg_bin_dir / name, os.X_OK)
        ]
        if missing:
            raise DriverError(
                f"{', '.join(missing)}: not executable; pass --pg-bin-dir (PostgreSQL 16's bin directory)"
            )

    # The lock covers the builds and the comparator's build as well as the
    # runs: a compile on the host distorts another series' measurement as
    # much as a run does. The manifest is read under it too, so a second
    # invocation that waited sees the series the first one wrote.
    # One invocation per series, whatever --lock-file each names: the lock
    # lives in the series directory itself.
    out.mkdir(parents=True, exist_ok=True)
    # Every invocation starts processes the cleanup must be able to find:
    # the comparator and the builds compile, runs start servers.
    if not become_subreaper():
        raise DriverError(
            "could not become a child subreaper (prctl PR_SET_CHILD_SUBREAPER): without it a server or "
            "PostgreSQL left by a killed harness would escape the cleanup and outlive its run"
        )
    with series_lock(out / ".series.lock"), benchmark_lock(options.lock_file):
        if manifest_path.exists() and not options.resume:
            raise DriverError(f"{manifest_path} exists; pass --resume to continue that series or choose another --out")
        out.mkdir(parents=True, exist_ok=True)
        (out / "runs").mkdir(exist_ok=True)
        previous = json.loads(manifest_path.read_text()) if manifest_path.exists() else None
        if previous and previous["preset"]["sha256"] != preset["_sha256"]:
            raise DriverError(f"{manifest_path} is a series of another preset ({previous['preset']['name']})")
        if previous:
            check_resumable(previous, options)
        try:
            # The full preset check (gates, budgets, timeout, every flag this
            # harness would derive a setting or phase from), before any
            # build, and inside the cleanup: it may compile the comparator.
            validate_preset(compare_bin(options.compare_bin), preset["_path"])
            return run_series(options, out, preset, legacy, operational, manifest_path, previous)
        finally:
            # Whatever ended the series (a signal in a cargo build, a failed
            # probe), nothing it started outlives the locks: rustc, a linker
            # or a build script orphaned by an interrupted cargo is below the
            # driver, as its subreaper, and is stopped here.
            with deferred_termination() as held:
                sweep_descendants()
            if held:
                raise Terminated(held[0])


def run_series(
    options: argparse.Namespace,
    out: Path,
    preset: dict[str, Any],
    legacy: dict[str, dict[str, Any]],
    operational: frozenset[str],
    manifest_path: Path,
    previous: dict[str, Any] | None,
) -> int:
    builds = []
    for label, ref in zip(LABELS, (options.base, options.candidate)):
        build = prepare_build(label, ref, out, options.skip_build)
        harness = out / build["worktree"] / "target" / "release" / "qbit-prism-load"
        build["argv"], build["dropped_legacy_flags"] = resolve_argv(
            preset["args"], help_flags(harness), legacy, operational, label
        )
        builds.append(build)
    if previous:
        for old, new in zip(previous["builds"], builds):
            if old["commit"] != new["commit"]:
                raise DriverError(f"{manifest_path}: the {old['label']} build was {old['commit']}, not {new['commit']}")

    manifest = {
        "schema": MANIFEST_SCHEMA,
        "preset": {"name": preset["name"], "path": preset["_path"], "sha256": preset["_sha256"]},
        "host": host_facts(),
        "settings": settings_of(options),
        "pg_test_fsync": previous["pg_test_fsync"] if previous else {},
        "builds": builds,
        "runs": previous["runs"] if previous else [],
    }
    for build in builds:
        print(f"{build['label']} ({build['commit'][:8]}): {' '.join(build['argv'])}", file=sys.stderr)
        if build["dropped_legacy_flags"]:
            print(f"  left off as legacy: {', '.join(build['dropped_legacy_flags'])}", file=sys.stderr)
    if options.dry_run:
        return EXIT_PASS

    options.tmpdir.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "TMPDIR": str(options.tmpdir)}
    done = {run["id"] for run in manifest["runs"]}
    if "before" not in manifest["pg_test_fsync"]:
        manifest["pg_test_fsync"]["before"] = record_fsync(
            options.pg_bin_dir, options.tmpdir, out / "pg_test_fsync-before.txt", options.fsync_seconds)
        write_json(manifest_path, manifest)
    for repeat, label in interleaved(options.repeats):
        run_id = f"{preset['name']}-{label}-r{repeat}"
        if run_id in done:
            continue
        build = next(b for b in builds if b["label"] == label)
        run_dir = out / "runs" / run_id
        if run_dir.exists():
            shutil.rmtree(run_dir)
        time.sleep(options.cooldown_seconds)
        load_before = wait_for_quiet(options.max_load, options.load_wait_seconds)
        mem_before = mem_available_mib()
        release = out / build["worktree"] / "target" / "release"
        command = [
            str(release / "qbit-prism-load"),
            *build["argv"],
            "--server-bin", str(release / "qbit-prism-server"),
            "--pg-bin-dir", str(options.pg_bin_dir),
            "--out", str(run_dir),
        ]
        print(f"{run_id}: load {load_before:.2f}, running", file=sys.stderr, flush=True)
        record = execute_run(run_id, command, out / build["worktree"], env, out / "runs",
                             options.run_ceiling_seconds, options.sample_seconds)
        manifest["runs"].append({
            "id": run_id,
            "build": label,
            "repeat": repeat,
            "dir": f"runs/{run_id}",
            "load_before": load_before,
            "mem_available_before_mib": mem_before,
            **record,
        })
        write_json(manifest_path, manifest)
        print(f"{run_id}: exit {record['exit_code']}{' (ceiling)' if record['ceiling_hit'] else ''}",
              file=sys.stderr, flush=True)
    if "after" not in manifest["pg_test_fsync"]:
        manifest["pg_test_fsync"]["after"] = record_fsync(
            options.pg_bin_dir, options.tmpdir, out / "pg_test_fsync-after.txt", options.fsync_seconds)
        write_json(manifest_path, manifest)

    comparator = compare_bin(options.compare_bin)
    try:
        summary = subprocess.run(
            [str(comparator), "--manifest", str(manifest_path), "--preset", preset["_path"]],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as error:
        raise DriverError(f"running {comparator}: {error}") from error
    (out / "comparison.md").write_text(summary.stdout, encoding="utf-8")
    sys.stdout.write(summary.stdout)
    sys.stderr.write(summary.stderr)
    return {0: EXIT_PASS, 1: EXIT_FAIL}.get(summary.returncode, EXIT_INPUT)


class Terminated(BaseException):
    """SIGTERM or SIGHUP reached the driver. Raised in the main thread, it
    takes the same path as Ctrl-C: `execute_run` stops the harness's process
    group (which `start_new_session` keeps out of reach of any signal sent
    to ours) before the benchmark lock is released."""

    def __init__(self, signum: int) -> None:
        super().__init__(signal.Signals(signum).name)
        self.signum = signum


# Set while a signal must not interrupt: the handler records it here
# instead of raising.
_deferred: list[int] | None = None


def on_termination(signum: int, frame: Any) -> None:
    if _deferred is not None:
        _deferred.append(signum)
        return
    raise Terminated(signum)


@contextlib.contextmanager
def deferred_termination() -> Iterator[list[int]]:
    """Hold SIGTERM and SIGHUP for the block; the list yielded holds any
    that arrived, for the caller to raise once it can clean up."""
    global _deferred
    held: list[int] = []
    _deferred = held
    try:
        yield held
    finally:
        _deferred = None


def run_cli(argv: list[str]) -> int:
    """`main` with every failure mapped to the documented exit status."""
    previous = {signum: signal.signal(signum, on_termination) for signum in (signal.SIGTERM, signal.SIGHUP)}
    try:
        return main(argv)
    except DriverError as error:
        print(f"prism_load_ab: {error}", file=sys.stderr)
        return EXIT_INPUT
    except IncompleteSeries as error:
        print(f"prism_load_ab: {error}", file=sys.stderr)
        return EXIT_INCOMPLETE
    except Terminated as error:
        print(f"prism_load_ab: stopped by {error}", file=sys.stderr)
        return 128 + error.signum
    except Exception:  # noqa: BLE001
        # Python's own status for an uncaught exception is 1, the status of a
        # candidate FAIL; an unexpected failure is an infrastructure problem.
        traceback.print_exc()
        return EXIT_INPUT
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    sys.exit(run_cli(sys.argv[1:]))
