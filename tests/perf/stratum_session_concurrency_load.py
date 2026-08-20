#!/usr/bin/env python3
"""Real-thread concurrency load driver for the PRISM Stratum session path.

Issue #143 asked what the coordinator actually does when many miners are
connected at once. The deterministic baton harness in
``tests/prism_concurrency_harness.py`` cannot answer that: it exists to make
one interleaving reproducible, and it replaces the scheduler, the clock and
the lock to do it. Those are exactly the three things this question is about.
This driver is the other instrument -- **real OS threads, real sockets, real
wall clock, no baton** -- and the two are deliberately kept separate.

What is under test is the shipped session stack, driven through a real
``PrismCoordinator``:

===============================  ============================================
service                          reached through
===============================  ============================================
``StratumSessionService``        ``coordinator.accept_loop`` / ``handle_client``
``VardiffService``               difficulty assignment, retarget and reconnect
                                 resume, via the real submit path
``ShareSubmissionService``       ``mining.submit`` handling end to end
===============================  ============================================

Nothing in ``lab/`` is modified, monkeypatched for behaviour, or
reimplemented. The node RPC is a fake (this measures the coordinator, not
qbitd) and the audit-bundle subprocess is the repository's existing counting
fake; every other object on the path is the shipped one.

Three modes, which answer different questions:

``paced``
    The deployed design point: each connection submits one share every
    ``--share-interval-seconds`` (default 15, matching
    ``PRISM_STRATUM_VARDIFF_TARGET_SECONDS``). This is a steady-state
    measurement -- what the coordinator costs when miners behave.
``saturating``
    Every connection submits as fast as it is answered. This finds the
    ceiling and, more usefully, shows whether sessions can even be
    *established* while the share path is busy.
``herd``
    All sessions are dropped at once and immediately reconnect. This is the
    reconnect storm. The headline is **not** throughput: it is a per-session
    re-establishment census, including the exact number that never came back
    inside the window.

Metrics, per run: cores-used, shares/s, CPU microseconds per share, ack
latency p50/p95/p99/max, coordinator-lock acquisitions and contentions with
hold attribution by call site, watchdog misses, rejections by reason, and a
fixed-work GIL-wait probe.

``--json`` captures a run; ``--render PATH`` re-renders a captured run without
measuring, so a review never needs to put two load generators on the same
cores to get both output formats.

READ THE OUTPUT THIS WAY (the human report repeats all of this):

* **Driver overhead is included.** The miner threads, their sockets and the
  lock-attribution wrapper all run in this process and on these cores. This
  measures a host running the coordinator *and* its load, not a coordinator
  alone.
* **Absolute values are host-dependent.** Core count, load average and the
  Python build change every number here.
* **In-memory ledger results omit database wait.** ``--ledger memory`` exists
  to isolate interpreter cost. Only ``--ledger postgres`` includes the real
  write path.
* **This is evidence, not a CI threshold.** Nothing here should be asserted
  against in an automated test.

On demand only. The filename does not begin with ``test_``, so unittest
discovery never collects it, and it is never imported by the suite.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import socket
import statistics
import sys
import sysconfig
import threading
import time
import traceback
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Sequence

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.auxpow import stratum_codec, vardiff
from lab.prism import direct_stratum
from lab.prism.prism_coordinator import StratumListenerProfile, _ObservedRLock
from lab.prism.share_ledger import SingleWriterShareLedger
from tests import prism_coordinator_test_support as support


# --------------------------------------------------------------------------
# constants and disclosures
# --------------------------------------------------------------------------

#: The deployed design point named by issue #143: one share per connection per
#: vardiff target interval.
DEFAULT_SHARE_INTERVAL_SECONDS = 15.0

#: Historical calibration from the original #143 run. These pin what the
#: numbers *mean*; they are explicitly NOT host-independent thresholds and
#: nothing in this driver asserts against them.
CALIBRATION = {
    "saturating_n1024_settled": "0/5 settled; only 889-944/1024 established within a 50s settle",
    "herd_n1024_unreestablished": "660-768/1024 sessions never re-established, with zero rejections and no watchdog miss",
    "lock_contention": "0/~48k settled acquisitions rose to 30-82% contended during herd",
    "long_holders": "note_tip_work_delivered, accept_loop, schedule_initial_job",
}

INTERPRETATION_NOTES = (
    "Driver overhead is included: the miner threads, their sockets and the "
    "lock-attribution wrapper run in this process and compete for these "
    "cores. This is a host running the coordinator and its load, not a "
    "coordinator alone.",
    "Absolute values are host-dependent. Core count, load average and the "
    "Python build change every number here; compare runs on one host, not "
    "across hosts.",
    "An in-memory ledger omits database wait entirely. --ledger memory "
    "isolates interpreter cost; only --ledger postgres exercises the real "
    "durable write path.",
    "This is evidence, not a CI threshold. Do not assert against these "
    "numbers in an automated test.",
)

#: A share target below this is reached by essentially every nonce, so the
#: driver spends no measurable CPU searching for one. difficulty_target()
#: clamps at 2**256-1, and DIFF1_TARGET / 1e-12 is past that clamp.
FLOOR_DIFFICULTY = Decimal("1e-12")

CLOCK_RATIONALE = (
    "cores-used = process CPU / wall, both measured across the same window: "
    "time.process_time() sums CPU over every thread in this process, so "
    "dividing by time.perf_counter() elapsed gives cores actually kept busy. "
    "1.00 means one core's worth of work regardless of how many threads ran, "
    "which is the GIL-bound signature; values above 1.00 mean real overlap. "
    "Wall clock is the headline for latency because a miner waits in wall "
    "time, not in CPU time."
)


class DriverAbort(RuntimeError):
    """The run could not establish a trustworthy measurement environment."""


class LedgerUnavailable(RuntimeError):
    """A requested ledger backend is not reachable on this host."""


# --------------------------------------------------------------------------
# environment
# --------------------------------------------------------------------------


def _cpu_brand() -> str:
    brand = platform.processor()
    if sys.platform == "darwin":
        try:
            import subprocess

            probed = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
            if probed:
                brand = probed
        except Exception:  # pragma: no cover - informational only
            pass
    return brand


def _loadavg() -> tuple[float, float, float] | None:
    try:
        return os.getloadavg()
    except (OSError, AttributeError):
        return None


def _peak_rss_mb() -> float | None:
    try:
        import resource
    except ImportError:  # pragma: no cover - non-POSIX
        return None
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports kilobytes, macOS reports bytes.
    return usage / (1024 * 1024) if sys.platform == "darwin" else usage / 1024


def raise_descriptor_limit(wanted: int) -> dict[str, Any]:
    """Raise RLIMIT_NOFILE toward ``wanted``, reporting what was achieved.

    Each connection costs two descriptors in this process (the miner's socket
    and the coordinator's accepted socket), so an N=1024 run needs well over
    2,048. A run that silently hits the limit reads as coordinator
    backpressure when it is really the harness, so the achieved limit is
    always reported and checked against the connection count.
    """
    detail: dict[str, Any] = {"requested": wanted}
    try:
        import resource
    except ImportError:  # pragma: no cover - non-POSIX
        detail["status"] = "unavailable"
        return detail
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    detail["soft_before"] = soft
    detail["hard"] = hard
    target = min(wanted, hard) if hard != resource.RLIM_INFINITY else wanted
    if soft < target:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        except (ValueError, OSError) as exc:
            detail["status"] = f"raise-failed: {exc}"
            detail["soft_after"] = soft
            return detail
    detail["soft_after"] = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    detail["status"] = "ok"
    return detail


def describe_environment() -> dict[str, Any]:
    try:
        gil_enabled: bool | None = sys._is_gil_enabled()  # type: ignore[attr-defined]
    except AttributeError:
        gil_enabled = None
    return {
        "cpu": _cpu_brand(),
        "machine": platform.machine(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "gil_disabled_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "gil_enabled_at_runtime": gil_enabled,
        "loadavg_1_5_15": _loadavg(),
        "clock_rationale": CLOCK_RATIONALE,
        "clocks": {
            name: {
                "implementation": time.get_clock_info(name).implementation,
                "resolution_s": time.get_clock_info(name).resolution,
            }
            for name in ("process_time", "perf_counter", "monotonic")
        },
    }


# --------------------------------------------------------------------------
# statistics
# --------------------------------------------------------------------------


def percentile(values: Sequence[float], fraction: float) -> float:
    """Nearest-rank percentile. Empty input is undefined, not zero."""
    if not values:
        return float("nan")
    ordered = sorted(values)
    if fraction <= 0:
        return ordered[0]
    rank = max(1, min(len(ordered), int(round(fraction * len(ordered) + 0.5))))
    return ordered[rank - 1]


def latency_summary(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None, "mean_ms": None}
    return {
        "count": len(values),
        "p50_ms": percentile(values, 0.50) * 1000.0,
        "p95_ms": percentile(values, 0.95) * 1000.0,
        "p99_ms": percentile(values, 0.99) * 1000.0,
        "max_ms": max(values) * 1000.0,
        "mean_ms": statistics.fmean(values) * 1000.0,
    }


# --------------------------------------------------------------------------
# coordinator-lock hold attribution
# --------------------------------------------------------------------------


class AttributingLock:
    """Wrap the shipped ``_ObservedRLock`` and attribute holds to call sites.

    Production deliberately does not instrument hold *duration*: the class
    docstring in ``prism_coordinator`` explains that a clock read on every
    acquire and release is exactly the fast-path cost that lock exists to
    avoid, and that a re-entrant lock needs per-thread depth tracking to
    avoid double counting nested holds. Both objections are correct, and this
    PR changes no production code -- so the attribution lives here, in the
    driver, and is paid for only while measuring.

    Two consequences the report states rather than hides:

    * This wrapper adds a clock read and a frame inspection per acquisition.
      That cost lands inside the measurement. ``--no-lock-attribution``
      removes it, so the overhead can be quantified on the same host instead
      of argued about.
    * Re-entrant acquisitions are tracked per thread by depth, and only the
      outermost hold is timed. A nested acquire is counted as an acquisition
      but contributes no second hold interval, so hold seconds cannot exceed
      wall time per thread.

    The delegate's own ``contention_snapshot`` remains the source of truth for
    acquisitions and contentions; this class never recomputes them.
    """

    def __init__(self, delegate: _ObservedRLock, *, attribute: bool = True) -> None:
        self._delegate = delegate
        self._attribute = attribute
        self._state = threading.local()
        self._stats_lock = threading.Lock()
        self.holds: dict[str, dict[str, float]] = {}

    # -- call-site identification -----------------------------------------

    @staticmethod
    def _call_site(depth: int) -> str:
        """Name the shipped function that took the lock.

        Frames inside this module are skipped so the attribution names a
        coordinator call site (``accept_loop``, ``note_tip_work_delivered``,
        ``schedule_initial_job``, ...) rather than the wrapper.
        """
        frame = sys._getframe(depth)
        this_file = __file__
        while frame is not None and frame.f_code.co_filename == this_file:
            frame = frame.f_back
        if frame is None:
            return "<unknown>"
        module = Path(frame.f_code.co_filename).stem
        return f"{module}.{frame.f_code.co_name}"

    def _enter_hold(self, site_depth: int) -> None:
        depth = getattr(self._state, "depth", 0)
        self._state.depth = depth + 1
        if depth == 0:
            self._state.site = self._call_site(site_depth + 1)
            self._state.started = time.perf_counter()

    def _exit_hold(self) -> None:
        depth = getattr(self._state, "depth", 0)
        if depth <= 0:
            return
        self._state.depth = depth - 1
        if depth != 1:
            return
        held = time.perf_counter() - getattr(self._state, "started", time.perf_counter())
        site = getattr(self._state, "site", "<unknown>")
        with self._stats_lock:
            entry = self.holds.get(site)
            if entry is None:
                entry = {"holds": 0.0, "seconds": 0.0, "max_seconds": 0.0}
                self.holds[site] = entry
            entry["holds"] += 1
            entry["seconds"] += held
            entry["max_seconds"] = max(entry["max_seconds"], held)

    # -- lock protocol -----------------------------------------------------

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        acquired = self._delegate.acquire(blocking, timeout)
        if acquired and self._attribute:
            self._enter_hold(1)
        return acquired

    def release(self) -> None:
        if self._attribute:
            self._exit_hold()
        self._delegate.release()

    def __enter__(self) -> "AttributingLock":
        self._delegate.acquire()
        if self._attribute:
            self._enter_hold(1)
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()

    def _is_owned(self) -> bool:
        return self._delegate._is_owned()

    def contention_snapshot(self) -> tuple[int, int, float, float]:
        return self._delegate.contention_snapshot()

    # -- reporting ---------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        acquisitions, contentions, wait_sum, wait_max = self._delegate.contention_snapshot()
        with self._stats_lock:
            sites = [
                {
                    "site": site,
                    "holds": int(entry["holds"]),
                    "hold_seconds": entry["seconds"],
                    "max_hold_ms": entry["max_seconds"] * 1000.0,
                    "mean_hold_us": (entry["seconds"] / entry["holds"]) * 1e6 if entry["holds"] else None,
                }
                for site, entry in self.holds.items()
            ]
        sites.sort(key=lambda row: row["hold_seconds"], reverse=True)
        return {
            "acquisitions": acquisitions,
            "contentions": contentions,
            "contended_percent": (contentions / acquisitions * 100.0) if acquisitions else None,
            "wait_seconds_sum": wait_sum,
            "wait_seconds_max": wait_max,
            "attribution_enabled": self._attribute,
            "sites": sites,
        }


def diff_lock_snapshot(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Attribute only what happened between two snapshots.

    Startup and settle take the lock too; charging their acquisitions to the
    measurement window would understate the contended percentage during it.
    """
    acquisitions = after["acquisitions"] - before["acquisitions"]
    contentions = after["contentions"] - before["contentions"]
    prior = {row["site"]: row for row in before["sites"]}
    sites = []
    for row in after["sites"]:
        was = prior.get(row["site"])
        holds = row["holds"] - (was["holds"] if was else 0)
        seconds = row["hold_seconds"] - (was["hold_seconds"] if was else 0.0)
        if holds <= 0 and seconds <= 0:
            continue
        sites.append(
            {
                "site": row["site"],
                "holds": holds,
                "hold_seconds": seconds,
                "max_hold_ms": row["max_hold_ms"],
                "mean_hold_us": (seconds / holds) * 1e6 if holds else None,
            }
        )
    sites.sort(key=lambda row: row["hold_seconds"], reverse=True)
    return {
        "acquisitions": acquisitions,
        "contentions": contentions,
        "contended_percent": (contentions / acquisitions * 100.0) if acquisitions else None,
        "wait_seconds_sum": after["wait_seconds_sum"] - before["wait_seconds_sum"],
        "wait_seconds_max": after["wait_seconds_max"],
        "attribution_enabled": after["attribution_enabled"],
        "sites": sites,
    }


# --------------------------------------------------------------------------
# controls and probes
# --------------------------------------------------------------------------


def _sha256_work(payload: bytes, rounds: int) -> None:
    for _ in range(rounds):
        hashlib.sha256(payload).digest()


def _python_work(iterations: int) -> int:
    total = 0
    for index in range(iterations):
        total = (total + index * 7919) & 0xFFFFFFFF
    return total


def _measure_threaded(work: Callable[[], None], threads: int) -> dict[str, float]:
    started = threading.Barrier(threads + 1)
    workers = [threading.Thread(target=lambda: (started.wait(), work())) for _ in range(threads)]
    for worker in workers:
        worker.start()
    started.wait()
    cpu0, wall0 = time.process_time(), time.perf_counter()
    for worker in workers:
        worker.join()
    cpu = time.process_time() - cpu0
    wall = time.perf_counter() - wall0
    return {"cores_used": cpu / wall if wall > 0 else float("nan"), "wall_s": wall, "cpu_s": cpu}


def run_controls(*, threads: int, min_wall_seconds: float) -> dict[str, Any]:
    """Positive and negative controls, grown until they clear the wall floor.

    The positive control is a 1 MiB ``hashlib.sha256`` loop, which releases
    the GIL and therefore scales; the negative control is a small-buffer
    pure-Python loop, which cannot. Together they say whether this host is
    capable of showing parallelism at all right now -- a host already
    saturated by other work reports a low positive control, and every stage
    number measured beside it would be a statement about the host rather than
    about the coordinator.

    Thread start/join is charged against the measurement, so a configuration
    that finishes too fast reads low for reasons that have nothing to do with
    the GIL. Both controls are therefore grown until they clear
    ``min_wall_seconds``.
    """
    payload = b"\x5a" * (1024 * 1024)
    rounds = 64
    while True:
        positive = _measure_threaded(lambda: _sha256_work(payload, rounds), threads)
        if positive["wall_s"] >= min_wall_seconds or rounds >= 1 << 20:
            break
        rounds = max(rounds * 2, int(rounds * min_wall_seconds / max(positive["wall_s"], 1e-6)) + 1)
    iterations = 200_000
    while True:
        negative = _measure_threaded(lambda: _python_work(iterations), threads)
        if negative["wall_s"] >= min_wall_seconds or iterations >= 1 << 30:
            break
        iterations = max(iterations * 2, int(iterations * min_wall_seconds / max(negative["wall_s"], 1e-6)) + 1)
    return {
        "threads": threads,
        "positive_sha256_1mib": positive | {"rounds_per_thread": rounds},
        "negative_pure_python": negative | {"iterations_per_thread": iterations},
        "min_wall_seconds": min_wall_seconds,
    }


class GilWaitProbe:
    """Fixed-work probe: how long does a known unit of Python take under load?

    A single thread repeatedly runs an identical pure-Python work unit. The
    unit never changes, so any growth in its wall time is time the thread
    spent not holding the GIL. Calibrated on an idle process first, the ratio
    ``loaded / idle`` is a direct read of GIL pressure -- and unlike
    cores-used it does not need the load to be uniform, because each sample
    is independently comparable to the calibration.
    """

    def __init__(self, iterations: int = 40_000) -> None:
        self.iterations = iterations
        self.samples: list[float] = []
        self.baseline_s: float | None = None
        # The probe is a busy loop: it burns most of a core for the whole
        # measurement window, and time.process_time() sums every thread. Left
        # unaccounted it would land in cores-used and CPU-per-share as if the
        # coordinator had spent it. The probe therefore charges itself, using
        # per-thread CPU, and the load metrics subtract it.
        self.cpu_seconds = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def calibrate(self, repeats: int = 25) -> float:
        timings = []
        for _ in range(repeats):
            started = time.perf_counter()
            _python_work(self.iterations)
            timings.append(time.perf_counter() - started)
        self.baseline_s = min(timings)
        return self.baseline_s

    def _loop(self) -> None:
        cpu0 = time.thread_time()
        while not self._stop.is_set():
            started = time.perf_counter()
            _python_work(self.iterations)
            self.samples.append(time.perf_counter() - started)
            # Published every iteration, not just at exit: the load window is
            # only part of the probe's lifetime, so callers sample this at the
            # window boundaries and subtract the difference. Charging the
            # probe's whole run against one window over-subtracts and can
            # drive the coordinator's measured CPU to zero.
            self.cpu_seconds = time.thread_time() - cpu0

    def start(self) -> None:
        self.samples.clear()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="gil-wait-probe", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10.0)

    def result(self) -> dict[str, Any]:
        if not self.samples or self.baseline_s is None:
            return {"samples": 0, "baseline_ms": None, "note": "probe did not run"}
        median = statistics.median(self.samples)
        return {
            "samples": len(self.samples),
            "iterations_per_sample": self.iterations,
            "baseline_ms": self.baseline_s * 1000.0,
            "median_ms": median * 1000.0,
            "p95_ms": percentile(self.samples, 0.95) * 1000.0,
            "max_ms": max(self.samples) * 1000.0,
            "stretch_median": median / self.baseline_s,
            "stretch_p95": percentile(self.samples, 0.95) / self.baseline_s,
            "gil_wait_fraction_median": max(0.0, 1.0 - self.baseline_s / median),
            "probe_cpu_seconds": self.cpu_seconds,
        }


# --------------------------------------------------------------------------
# ledger selection
# --------------------------------------------------------------------------

#: The repository's real-server gate, in the order test/test-prism-postgres-*.sh
#: consult it. QBIT_PRISM_EXTERNAL_PSQL_COMMAND points at an already-running
#: server; the PRISM_* names are the coordinator's own runtime contract.
POSTGRES_GATE_ENV = (
    "QBIT_PRISM_EXTERNAL_PSQL_COMMAND",
    "PRISM_POSTGRES_PSQL_COMMAND",
    "PRISM_DATABASE_URL",
)


def postgres_command_from_gate() -> tuple[str, str] | None:
    """Return ``(env_name, psql_command)`` if the real-server gate is set."""
    for name in POSTGRES_GATE_ENV:
        value = os.environ.get(name, "").strip()
        if not value:
            continue
        if name == "PRISM_DATABASE_URL":
            import shlex

            return name, f"psql {shlex.quote(value)}"
        return name, value
    return None


def build_ledger(mode: str) -> tuple[Any, dict[str, Any]]:
    """Build the requested ledger, or explain exactly what is missing.

    Every output identifies the ledger mode, because a shares/s figure means
    something entirely different with and without a durable write behind it.
    """
    if mode == "memory":
        ledger = SingleWriterShareLedger()
        return ledger, {
            "mode": "memory",
            "backend_name": ledger.backend_name,
            "includes_database_wait": False,
            "detail": "shipped in-memory ledger; isolates interpreter cost and omits all database wait",
        }
    gate = postgres_command_from_gate()
    if gate is None:
        raise LedgerUnavailable(
            "PostgreSQL mode requires the repository's real-server gate. None of "
            + ", ".join(POSTGRES_GATE_ENV)
            + " is set. Start a server the way test/test-prism-postgres-ledger.sh does:\n"
            "  docker run --rm -d --name qbit-prism-perf-postgres "
            "-e POSTGRES_USER=qbit -e POSTGRES_PASSWORD=qbit -e POSTGRES_DB=qbit postgres:16-alpine\n"
            "then re-run with:\n"
            "  QBIT_PRISM_EXTERNAL_PSQL_COMMAND='docker exec -i qbit-prism-perf-postgres psql -U qbit -d qbit'"
        )
    env_name, psql_command = gate
    from lab.prism.share_ledger import (
        WRITER_LEASE_HEARTBEAT_SESSION_PREFIX,
        PsqlShareLedger,
    )
    import uuid

    ledger = PsqlShareLedger(
        psql_command=psql_command,
        database_url=os.environ.get("PRISM_DATABASE_URL") or None,
        writer_id="prism-perf-driver",
        writer_epoch=1,
        writer_session_token=f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}{uuid.uuid4().hex}",
        initialize_schema=True,
    )
    return ledger, {
        "mode": "postgres",
        "backend_name": ledger.backend_name,
        "includes_database_wait": True,
        "gate_env": env_name,
        "detail": "real PsqlShareLedger through the repository's real-server gate; includes durable write wait",
    }


# --------------------------------------------------------------------------
# template sizing
# --------------------------------------------------------------------------


def synthetic_transaction_hex(index: int, payload_bytes: int) -> str:
    """A structurally valid non-witness transaction of roughly ``payload_bytes``.

    Template size is an input to this driver because merkle-branch depth and
    per-job work both grow with it, and a 0-transaction template flatters the
    job-build path in a way no deployed pool would see.
    """
    script = (f"{index:08x}" * max(1, payload_bytes // 4))[: max(2, payload_bytes * 2)]
    if len(script) % 2:
        script += "0"
    script_bytes = bytes.fromhex(script)
    output_script = "51"
    return (
        "01000000"
        + "01"
        + f"{index + 1:064x}"
        + "00000000"
        + direct_stratum.compact_size(len(script_bytes)).hex()
        + script
        + "ffffffff"
        + "01"
        + (10_000).to_bytes(8, "little").hex()
        + direct_stratum.compact_size(len(bytes.fromhex(output_script))).hex()
        + output_script
        + "00000000"
    )


def build_template(transactions: int, transaction_bytes: int) -> dict[str, object]:
    template = support.base_template()
    template["transactions"] = [
        {"data": synthetic_transaction_hex(index, transaction_bytes)}
        for index in range(transactions)
    ]
    return template


# --------------------------------------------------------------------------
# the rig: one real coordinator behind one real listening socket
# --------------------------------------------------------------------------


@dataclass
class DriverConfig:
    connections: int = 64
    mode: str = "paced"
    share_interval_seconds: float = DEFAULT_SHARE_INTERVAL_SECONDS
    settle_seconds: float = 10.0
    measure_seconds: float = 15.0
    herd_return_seconds: float = 50.0
    ledger_mode: str = "memory"
    difficulty_mode: str = "resume"
    template_transactions: int = 0
    template_transaction_bytes: int = 250
    lock_attribution: bool = True
    control_threads: int = 8
    control_floor_fraction: float = 0.5
    control_min_wall_seconds: float = 0.5
    min_wall_seconds: float = 5.0
    max_rss_mb: float = 8192.0
    max_loadavg_per_core: float = 2.0
    watchdog_timeout_seconds: float = 30.0
    connect_timeout_seconds: float = 20.0
    skip_controls: bool = False


class Rig:
    """A real ``PrismCoordinator`` on a real socket, with real accept threads.

    The coordinator is assembled by the repository's own
    ``prism_coordinator_test_support.coordinator`` scaffold, which is how
    every unit test in this tree obtains one without a live qbitd. The
    difference here is what is layered on top: the shipped ``_ObservedRLock``
    (the scaffold installs a plain RLock, which would report no contention at
    all), a real listening socket, a real accept loop on its own thread, and
    real per-connection handler threads spawned by the shipped session
    service.
    """

    def __init__(self, config: DriverConfig) -> None:
        self.config = config
        self.template = build_template(
            config.template_transactions, config.template_transaction_bytes
        )
        self.server, self.rpc = support.coordinator(template=self.template)
        support.install_fake_bundle_builder(self.server)
        self.ledger, self.ledger_detail = build_ledger(config.ledger_mode)
        self.server.ledger = self.ledger

        base_lock = _ObservedRLock()
        self.lock = AttributingLock(base_lock, attribute=config.lock_attribution)
        self.server.lock = self.lock

        self._install_rpc_shim()
        self._install_difficulty_policy()

        self.server.watchdog_timeout_seconds = config.watchdog_timeout_seconds
        self.server.stratum_max_connections = 0  # unbounded; admission is what we measure
        self.server.vardiff_resume_enabled = config.difficulty_mode == "resume"

        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener_socket.bind(("127.0.0.1", 0))
        self.listener_socket.listen(max(128, min(config.connections * 2, 4096)))
        self.listener_socket.settimeout(0.25)
        self.port = self.listener_socket.getsockname()[1]
        self.profile = StratumListenerProfile(
            name="default",
            bind="127.0.0.1",
            port=self.port,
            vardiff_config=self.server.vardiff_config,
            share_difficulty=self.server.share_difficulty,
            minimum_advertised_difficulty=FLOOR_DIFFICULTY,
            heartbeat_name="stratum_accept",
        )
        self.accept_errors: list[str] = []
        self.watchdog_misses: dict[str, int] = {}
        self._accept_thread: threading.Thread | None = None
        self._watchdog_thread: threading.Thread | None = None
        self._watchdog_stop = threading.Event()

    # -- collaborators -----------------------------------------------------

    def _install_rpc_shim(self) -> None:
        """Answer the one RPC the session path needs that FakeRpc lacks.

        ``P2mrAddressValidator`` calls ``validateaddress`` for every distinct
        payout identity. The shipped validator, its LRU and its singleflight
        all still run; only the node answer is synthesized.
        """
        inner = self.rpc.call

        def call(method: str, params: list[object] | None = None) -> object:
            if method == "validateaddress":
                address = str((params or [""])[0])
                digest = hashlib.sha256(address.encode()).hexdigest()[:64]
                return {"isvalid": True, "scriptPubKey": "5220" + digest}
            return inner(method, params)

        self.rpc.call = call  # type: ignore[method-assign]

    def _install_difficulty_policy(self) -> None:
        """Set the vardiff band this run measures under.

        ``pinned`` clamps min == max so no nonce search is ever needed; it
        isolates coordinator cost but is explicitly not representative, and
        the report says so. ``resume`` and ``climb`` both leave a real band
        open so ``VardiffService`` retargets for real -- they differ in
        whether a reconnecting session may adopt its retained difficulty.
        """
        config = self.config
        floor = FLOOR_DIFFICULTY
        ceiling = floor if config.difficulty_mode == "pinned" else Decimal("1e-6")
        self.server.vardiff_config = vardiff.VardiffConfig(
            enabled=True,
            target_share_interval_seconds=Decimal(str(config.share_interval_seconds)),
            min_difficulty=floor,
            max_difficulty=ceiling,
            retarget_interval_seconds=Decimal("90"),
            max_step_factor=Decimal("4"),
            startup_difficulty=floor,
            max_step_down_factor=Decimal("4"),
            ewma_alpha=Decimal("0.4"),
            retarget_tolerance=Decimal("0.25"),
        )
        self.server.share_difficulty = floor

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        def accept() -> None:
            try:
                self.server.accept_loop(self.listener_socket, self.profile)
            except BaseException:  # noqa: BLE001 - recorded, not swallowed
                self.accept_errors.append(traceback.format_exc())

        self._accept_thread = threading.Thread(target=accept, name="prism-accept", daemon=True)
        self._accept_thread.start()

        def watch() -> None:
            while not self._watchdog_stop.wait(0.5):
                for name in self.server._overdue_heartbeats(time.monotonic()):
                    self.watchdog_misses[name] = self.watchdog_misses.get(name, 0) + 1

        self._watchdog_thread = threading.Thread(target=watch, name="prism-watchdog-probe", daemon=True)
        self._watchdog_thread.start()

    def stop(self) -> None:
        self._watchdog_stop.set()
        self.server.stop_event.set()
        if self._watchdog_thread is not None:
            self._watchdog_thread.join(timeout=3.0)
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=5.0)
        try:
            self.listener_socket.close()
        except OSError:
            pass
        for client in list(getattr(self.server, "clients", []) or []):
            try:
                client.sock.close()
            except OSError:
                pass

    # -- observation -------------------------------------------------------

    def rejection_counts(self) -> dict[str, int]:
        counts = {
            reason: int(count)
            for reason, count in dict(getattr(self.server, "rejection_counts_by_reason", {})).items()
            if count
        }
        limits = dict(getattr(self.server, "connection_limit_rejection_counts", {}) or {})
        for name, count in limits.items():
            if count:
                counts[f"connection-limit:{name}"] = int(count)
        return counts

    def vardiff_resume_outcomes(self) -> dict[str, int]:
        service = getattr(self.server, "_vardiff_service", None)
        counts = getattr(service, "vardiff_resume_outcome_counts", None)
        return {name: int(count) for name, count in dict(counts or {}).items()}


# --------------------------------------------------------------------------
# the miner side: one real socket and one real thread per connection
# --------------------------------------------------------------------------


@dataclass
class SessionOutcome:
    """What one connection did, kept per session for the herd census."""

    index: int
    established: bool = False
    establish_seconds: float | None = None
    failure: str | None = None
    accepted: int = 0
    rejected: int = 0
    reject_reasons: dict[str, int] = field(default_factory=dict)
    ack_latencies: list[float] = field(default_factory=list)
    nonce_hashes: int = 0


class MinerSession:
    """One Stratum client: real socket, real handshake, real submits.

    Deliberately hand-rolled rather than reusing ``tests/stratum_client.py``:
    that client is a diagnostic CLI with per-message reporting, and its
    formatting cost would land inside every ack latency measured here.
    """

    def __init__(self, index: int, port: int, config: DriverConfig) -> None:
        self.index = index
        self.port = port
        self.config = config
        self.username = f"{support.PAYOUT_ADDRESS}.perf{index}"
        self.sock: socket.socket | None = None
        self.stream: Any = None
        self.extranonce1 = ""
        self.extranonce2_size = 8
        self.difficulty = FLOOR_DIFFICULTY
        self.job: list[Any] | None = None
        self.outcome = SessionOutcome(index=index)
        self._request_id = 0
        self._nonce = (index * 0x9E3779B1) & 0xFFFFFFFF

    # -- wire --------------------------------------------------------------

    def _send(self, message: dict[str, Any]) -> None:
        assert self.stream is not None
        self.stream.write(json.dumps(message).encode() + b"\n")
        self.stream.flush()

    def _read(self) -> dict[str, Any] | None:
        assert self.stream is not None
        line = self.stream.readline()
        if not line:
            return None
        return json.loads(line)

    def _absorb(self, message: dict[str, Any]) -> None:
        """Apply a server notification. Both are load-bearing for submits."""
        method = message.get("method")
        if method == "mining.set_difficulty":
            params = message.get("params") or [None]
            if params[0] is not None:
                self.difficulty = Decimal(str(params[0]))
        elif method == "mining.notify":
            self.job = list(message.get("params") or [])

    def _await_response(self, request_id: int, deadline: float) -> dict[str, Any]:
        while True:
            if time.monotonic() > deadline:
                raise TimeoutError(f"no response to request {request_id}")
            message = self._read()
            if message is None:
                raise ConnectionError("server closed the connection")
            if message.get("id") == request_id:
                return message
            self._absorb(message)

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    # -- lifecycle ---------------------------------------------------------

    def connect(self) -> None:
        self.sock = socket.create_connection(
            ("127.0.0.1", self.port), timeout=self.config.connect_timeout_seconds
        )
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(self.config.connect_timeout_seconds)
        self.stream = self.sock.makefile("rwb")

    def establish(self) -> None:
        """Subscribe, authorize and wait for the first job.

        "Established" means the coordinator delivered work -- not merely that
        the TCP connection was accepted. A session that is accepted but never
        receives a job cannot mine, so counting it as established would hide
        exactly the failure the herd census exists to find.
        """
        started = time.monotonic()
        deadline = started + self.config.connect_timeout_seconds
        self.connect()

        request_id = self._next_id()
        self._send({"id": request_id, "method": "mining.subscribe", "params": ["prism-perf-driver/1"]})
        response = self._await_response(request_id, deadline)
        result = response.get("result") or []
        if len(result) >= 3:
            self.extranonce1 = str(result[1])
            self.extranonce2_size = int(result[2])

        request_id = self._next_id()
        self._send({"id": request_id, "method": "mining.authorize", "params": [self.username, "x"]})
        response = self._await_response(request_id, deadline)
        if response.get("result") is not True:
            raise ConnectionError(f"authorize refused: {response.get('error')}")

        while self.job is None:
            if time.monotonic() > deadline:
                raise TimeoutError("no job delivered")
            message = self._read()
            if message is None:
                raise ConnectionError("server closed before delivering work")
            self._absorb(message)

        self.outcome.established = True
        self.outcome.establish_seconds = time.monotonic() - started

    def close(self) -> None:
        for closer in (self.stream, self.sock):
            try:
                if closer is not None:
                    closer.close()
            except OSError:
                pass
        self.stream = None
        self.sock = None

    def drop(self) -> None:
        """Abandon the connection the way a herd does: close, no goodbye."""
        self.job = None
        self.outcome.established = False
        self.close()

    # -- work --------------------------------------------------------------

    def _find_nonce(self) -> tuple[str, str]:
        """Return an ``(extranonce2, nonce)`` that clears the share target.

        A real miner searches, so this driver searches too, using the shipped
        header assembly and hash. At the floor difficulty the target is the
        2**256-1 clamp and the first candidate always passes, which is the
        point: nonce search is not what this instrument is trying to measure,
        and the hash count is reported so its cost stays visible.
        """
        assert self.job is not None
        job_id, prevhash, coinb1, coinb2, branch, version, nbits, ntime = self.job[:8]
        target = direct_stratum.difficulty_target(self.difficulty)
        extranonce2 = f"{self.index & ((1 << (self.extranonce2_size * 8)) - 1):0{self.extranonce2_size * 2}x}"
        for _ in range(1 << 20):
            self._nonce = (self._nonce + 1) & 0xFFFFFFFF
            nonce_hex = f"{self._nonce:08x}"
            _coinbase, header = stratum_codec.assemble_header_from_notify_submit(
                coinb1_hex=coinb1,
                extranonce1_hex=self.extranonce1,
                extranonce2_hex=extranonce2,
                coinb2_hex=coinb2,
                merkle_branch_hex=list(branch),
                version_hex=version,
                prevhash_hex=prevhash,
                ntime_hex=ntime,
                nbits_hex=nbits,
                nonce_hex=nonce_hex,
            )
            self.outcome.nonce_hashes += 1
            if stratum_codec.header_hash_int(header) <= target:
                return extranonce2, nonce_hex
        raise RuntimeError("no nonce cleared the share target")

    def submit_share(self) -> None:
        """Submit one share and time the acknowledgement."""
        assert self.job is not None
        job_id = self.job[0]
        ntime = self.job[7]
        extranonce2, nonce_hex = self._find_nonce()
        request_id = self._next_id()
        started = time.perf_counter()
        self._send(
            {
                "id": request_id,
                "method": "mining.submit",
                "params": [self.username, job_id, extranonce2, ntime, nonce_hex],
            }
        )
        response = self._await_response(request_id, time.monotonic() + self.config.connect_timeout_seconds)
        self.outcome.ack_latencies.append(time.perf_counter() - started)
        if response.get("result") is True:
            self.outcome.accepted += 1
            return
        self.outcome.rejected += 1
        error = response.get("error") or []
        reason = "unknown"
        if isinstance(error, list) and len(error) >= 3 and isinstance(error[2], dict):
            reason = str(error[2].get("reason_id", "unknown"))
        elif isinstance(error, list) and len(error) >= 2:
            reason = str(error[1])
        self.outcome.reject_reasons[reason] = self.outcome.reject_reasons.get(reason, 0) + 1


# --------------------------------------------------------------------------
# run orchestration
# --------------------------------------------------------------------------


def _establish_all(
    sessions: Sequence[MinerSession], *, window_seconds: float
) -> dict[str, Any]:
    """Bring every session up concurrently and census the result.

    One thread per session, started together, is the point: a serial ramp
    would never produce the admission contention this driver exists to
    measure.
    """
    started = threading.Barrier(len(sessions) + 1)
    deadline_wall = time.monotonic() + window_seconds

    def bring_up(session: MinerSession) -> None:
        started.wait()
        try:
            session.establish()
        except BaseException as exc:  # noqa: BLE001 - the census wants the reason
            session.outcome.established = False
            session.outcome.failure = f"{type(exc).__name__}: {exc}"
            session.close()

    threads = [
        threading.Thread(target=bring_up, args=(session,), name=f"miner-{session.index}", daemon=True)
        for session in sessions
    ]
    for thread in threads:
        thread.start()
    began = time.perf_counter()
    started.wait()
    for thread in threads:
        thread.join(timeout=max(0.0, deadline_wall - time.monotonic()) + 5.0)
    elapsed = time.perf_counter() - began

    established = [s for s in sessions if s.outcome.established]
    failures: dict[str, int] = {}
    for session in sessions:
        if session.outcome.failure:
            key = session.outcome.failure.split(":")[0]
            failures[key] = failures.get(key, 0) + 1
    times = [s.outcome.establish_seconds for s in established if s.outcome.establish_seconds is not None]
    return {
        "requested": len(sessions),
        "established": len(established),
        "not_established": len(sessions) - len(established),
        "window_seconds": window_seconds,
        "wall_seconds": elapsed,
        "failure_kinds": failures,
        "establish_latency": latency_summary(times),
    }


def _run_share_load(
    sessions: Sequence[MinerSession],
    *,
    mode: str,
    duration_seconds: float,
    share_interval_seconds: float,
    probe: "GilWaitProbe | None" = None,
) -> dict[str, Any]:
    """Drive shares for ``duration_seconds`` and return window totals."""
    live = [s for s in sessions if s.outcome.established]
    if not live:
        return {"threads": 0, "note": "no established session could submit"}
    stop = threading.Event()
    barrier = threading.Barrier(len(live) + 1)
    errors: dict[str, int] = {}
    errors_lock = threading.Lock()

    def pump(session: MinerSession) -> None:
        barrier.wait()
        # Stagger paced miners across the interval so they do not all submit
        # on the same tick, which would manufacture a convoy that no real
        # pool sees.
        if mode == "paced":
            time.sleep(share_interval_seconds * (session.index % 100) / 100.0)
        while not stop.is_set():
            try:
                session.submit_share()
            except BaseException as exc:  # noqa: BLE001 - counted, not swallowed
                with errors_lock:
                    key = type(exc).__name__
                    errors[key] = errors.get(key, 0) + 1
                session.outcome.failure = session.outcome.failure or f"{type(exc).__name__}: {exc}"
                return
            if mode == "paced" and stop.wait(share_interval_seconds):
                return

    threads = [
        threading.Thread(target=pump, args=(session,), name=f"pump-{session.index}", daemon=True)
        for session in live
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    # The probe is sampled on exactly these two lines, so its discount covers
    # the measured window and nothing else. Sampling it around this call
    # instead would also charge thread creation, which can exceed the window's
    # own CPU and zero out the result.
    probe_cpu0 = probe.cpu_seconds if probe is not None else 0.0
    cpu0, wall0 = time.process_time(), time.perf_counter()
    time.sleep(duration_seconds)
    stop.set()
    for thread in threads:
        thread.join(timeout=30.0)
    cpu = time.process_time() - cpu0
    wall = time.perf_counter() - wall0
    probe_cpu = (probe.cpu_seconds - probe_cpu0) if probe is not None else 0.0

    accepted = sum(s.outcome.accepted for s in live)
    rejected = sum(s.outcome.rejected for s in live)
    latencies: list[float] = []
    for session in live:
        latencies.extend(session.outcome.ack_latencies)
    reject_reasons: dict[str, int] = {}
    for session in live:
        for reason, count in session.outcome.reject_reasons.items():
            reject_reasons[reason] = reject_reasons.get(reason, 0) + count
    return {
        "threads": len(live),
        "wall_seconds": wall,
        "process_cpu_seconds": cpu,
        "cores_used": cpu / wall if wall > 0 else float("nan"),
        "accepted_shares": accepted,
        "rejected_shares": rejected,
        "shares_per_second": accepted / wall if wall > 0 else float("nan"),
        "cpu_us_per_share": (cpu * 1e6 / accepted) if accepted else None,
        "ack_latency": latency_summary(latencies),
        "reject_reasons": reject_reasons,
        "pump_errors": errors,
        "nonce_hashes": sum(s.outcome.nonce_hashes for s in live),
        "probe_cpu_seconds_window": probe_cpu,
    }


def _run_herd(rig: Rig, sessions: Sequence[MinerSession], config: DriverConfig) -> dict[str, Any]:
    """Drop every session at once, reconnect, and census what came back.

    The headline is the census, not throughput. A reconnect storm that
    settles at high shares/s while a third of the pool never gets work back
    is a failure, and a throughput-only report would call it a success.
    """
    before_lock = rig.lock.snapshot()
    live_before = sum(1 for s in sessions if s.outcome.established)
    dropped_at = time.perf_counter()
    for session in sessions:
        session.drop()
    drop_seconds = time.perf_counter() - dropped_at

    # Give the coordinator a moment to observe the disconnects before the
    # herd returns; a reconnect that races its own teardown measures cleanup,
    # not admission.
    time.sleep(0.5)

    returning = [MinerSession(s.index, rig.port, config) for s in sessions]
    census = _establish_all(returning, window_seconds=config.herd_return_seconds)
    after_lock = rig.lock.snapshot()

    per_session = [
        {
            "index": session.index,
            "established": session.outcome.established,
            "establish_seconds": session.outcome.establish_seconds,
            "failure": session.outcome.failure,
        }
        for session in returning
    ]
    return {
        "dropped": live_before,
        "drop_seconds": drop_seconds,
        "return_window_seconds": config.herd_return_seconds,
        "census": census,
        "re_established": census["established"],
        "never_re_established": census["not_established"],
        "lock": diff_lock_snapshot(before_lock, after_lock),
        "vardiff_resume_outcomes": rig.vardiff_resume_outcomes(),
        "per_session": per_session,
        "sessions": returning,
    }


def _health_abort(config: DriverConfig, controls: dict[str, Any] | None) -> dict[str, Any] | None:
    """Decide whether this host can support a trustworthy measurement.

    Everything below is a statement about the *environment*, checked before
    any conclusion is drawn from it. If one fails the run reports the abort
    and no stage or herd numbers at all -- a number measured on a host that
    cannot show parallelism is not a weaker result, it is a different
    quantity.
    """
    load = _loadavg()
    cores = os.cpu_count() or 1
    if load is not None and load[0] / cores > config.max_loadavg_per_core:
        return {
            "reason": "load-average",
            "detail": (
                f"1-minute load average {load[0]:.2f} over {cores} cores is "
                f"{load[0] / cores:.2f} per core, above the "
                f"{config.max_loadavg_per_core:.2f} guard. Another workload owns "
                "this host; every number here would be about that workload."
            ),
        }
    if controls is None:
        return None
    positive = controls["positive_sha256_1mib"]
    negative = controls["negative_pure_python"]
    floor = config.control_floor_fraction * controls["threads"]
    if positive["cores_used"] < floor:
        return {
            "reason": "control-floor",
            "detail": (
                f"the 1 MiB sha256 positive control reached only "
                f"{positive['cores_used']:.2f} cores across {controls['threads']} "
                f"threads, below the {floor:.2f} floor "
                f"({config.control_floor_fraction:.0%} of thread count). This host "
                "cannot currently demonstrate parallelism, so a low cores-used "
                "reading from the coordinator would not be evidence about the "
                "coordinator."
            ),
        }
    if negative["cores_used"] > 1.5:
        return {
            "reason": "negative-control",
            "detail": (
                f"the pure-Python negative control reached "
                f"{negative['cores_used']:.2f} cores, which should pin near 1.00 "
                "under a GIL. The measurement model does not describe this "
                "interpreter."
            ),
        }
    return None


def _discount_probe_cpu(load: dict[str, Any] | None, probe_cpu_seconds: float) -> None:
    """Re-derive the load metrics with the probe's own CPU removed.

    The raw figures are kept beside the adjusted ones rather than replaced: a
    reader who wants to know what this whole process cost should be able to
    see it, and silently rewriting a measured number is how a driver starts
    lying. The adjusted values are the headline because the probe is
    measurement apparatus, not coordinator work.
    """
    if not load or not load.get("threads"):
        return
    wall = float(load.get("wall_seconds") or 0.0)
    raw_cpu = float(load.get("process_cpu_seconds") or 0.0)
    load["probe_cpu_seconds"] = probe_cpu_seconds
    load["process_cpu_seconds_raw"] = raw_cpu
    load["cores_used_raw"] = load.get("cores_used")
    adjusted = raw_cpu - probe_cpu_seconds
    if adjusted < 0.0:
        # Never silently report zero work: a negative discount means the
        # probe accounting is wrong, and that is a defect in this driver
        # rather than a coordinator that used no CPU.
        load["probe_discount_error"] = (
            f"probe CPU {probe_cpu_seconds:.3f}s exceeded window CPU "
            f"{raw_cpu:.3f}s; reporting the undiscounted figures"
        )
        load["probe_cpu_seconds"] = probe_cpu_seconds
        return
    adjusted = max(0.0, adjusted)
    load["process_cpu_seconds"] = adjusted
    load["cores_used"] = adjusted / wall if wall > 0 else float("nan")
    accepted = int(load.get("accepted_shares") or 0)
    load["cpu_us_per_share"] = (adjusted * 1e6 / accepted) if accepted else None


def run(config: DriverConfig) -> dict[str, Any]:
    """Execute one measurement and return the full captured result."""
    environment = describe_environment()
    descriptors = raise_descriptor_limit(max(4096, config.connections * 4))
    results: dict[str, Any] = {
        "environment": environment,
        "descriptors": descriptors,
        "config": {
            "connections": config.connections,
            "mode": config.mode,
            "share_interval_seconds": config.share_interval_seconds,
            "settle_seconds": config.settle_seconds,
            "measure_seconds": config.measure_seconds,
            "herd_return_seconds": config.herd_return_seconds,
            "ledger_mode": config.ledger_mode,
            "difficulty_mode": config.difficulty_mode,
            "template_transactions": config.template_transactions,
            "template_transaction_bytes": config.template_transaction_bytes,
            "lock_attribution": config.lock_attribution,
            "control_floor_fraction": config.control_floor_fraction,
            "min_wall_seconds": config.min_wall_seconds,
        },
        "calibration": CALIBRATION,
        "interpretation": list(INTERPRETATION_NOTES),
        "aborted": None,
    }

    controls = (
        None
        if config.skip_controls
        else run_controls(
            threads=config.control_threads, min_wall_seconds=config.control_min_wall_seconds
        )
    )
    results["controls"] = controls

    abort = _health_abort(config, controls)
    if abort is not None:
        results["aborted"] = abort
        results["loadavg_end"] = _loadavg()
        results["peak_rss_mb"] = _peak_rss_mb()
        return results

    soft = descriptors.get("soft_after") or 0
    if soft and soft < config.connections * 2 + 64:
        results["aborted"] = {
            "reason": "descriptor-limit",
            "detail": (
                f"RLIMIT_NOFILE is {soft}, below the {config.connections * 2 + 64} "
                f"this run needs for {config.connections} connections. Sessions "
                "would fail to establish for harness reasons and read as "
                "coordinator backpressure."
            ),
        }
        results["loadavg_end"] = _loadavg()
        results["peak_rss_mb"] = _peak_rss_mb()
        return results

    probe = GilWaitProbe()
    probe.calibrate()

    rig = Rig(config)
    results["ledger"] = rig.ledger_detail
    rig.start()
    sessions = [MinerSession(index, rig.port, config) for index in range(config.connections)]
    try:
        settle = _establish_all(sessions, window_seconds=max(config.settle_seconds, 1.0))
        results["settle"] = settle

        lock_before = rig.lock.snapshot()
        probe.start()
        if config.mode == "herd":
            herd = _run_herd(rig, sessions, config)
            returning = herd.pop("sessions")
            load = _run_share_load(
                returning,
                mode="paced",
                duration_seconds=config.measure_seconds,
                share_interval_seconds=config.share_interval_seconds,
                probe=probe,
            )
            results["herd"] = herd
            results["load"] = load
        else:
            results["load"] = _run_share_load(
                sessions,
                mode=config.mode,
                duration_seconds=config.measure_seconds,
                share_interval_seconds=config.share_interval_seconds,
                probe=probe,
            )
        probe.stop()
        lock_after = rig.lock.snapshot()

        _discount_probe_cpu(
            results.get("load"),
            float((results.get("load") or {}).get("probe_cpu_seconds_window") or 0.0),
        )
        results["gil_wait_probe"] = probe.result()
        results["lock"] = diff_lock_snapshot(lock_before, lock_after)
        results["lock_total"] = lock_after
        results["rejections"] = rig.rejection_counts()
        results["watchdog_misses"] = dict(rig.watchdog_misses)
        results["accept_errors"] = rig.accept_errors[:3]
        results["vardiff_resume_outcomes"] = rig.vardiff_resume_outcomes()
    finally:
        probe.stop()
        for session in sessions:
            session.close()
        rig.stop()

    peak_rss = _peak_rss_mb()
    results["peak_rss_mb"] = peak_rss
    results["loadavg_end"] = _loadavg()
    if peak_rss is not None and peak_rss > config.max_rss_mb:
        results["memory_guard"] = {
            "exceeded": True,
            "peak_rss_mb": peak_rss,
            "limit_mb": config.max_rss_mb,
            "detail": "peak RSS passed the guard; results may include swap wait",
        }
    else:
        results["memory_guard"] = {"exceeded": False, "peak_rss_mb": peak_rss, "limit_mb": config.max_rss_mb}

    measured_wall = float(results.get("load", {}).get("wall_seconds") or 0.0)
    if measured_wall and measured_wall < config.min_wall_seconds:
        results["wall_floor_warning"] = (
            f"measurement window was {measured_wall:.2f}s, below the "
            f"{config.min_wall_seconds:.2f}s floor; thread start/join is charged "
            "against it and cores-used reads low for that reason alone"
        )
    return results


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------


def _fmt(value: Any, digits: int = 2, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float) and value != value:
        return "n/a"
    if isinstance(value, (int, float)):
        return f"{value:,.{digits}f}{suffix}"
    return str(value)


def _rule(char: str = "=") -> str:
    return char * 78


def render_report(results: dict[str, Any]) -> str:
    out: list[str] = []
    add = out.append
    env = results["environment"]
    config = results["config"]

    add(_rule())
    add("PRISM Stratum session concurrency load (real threads, real sockets)")
    add(_rule())
    add("")
    add(f"CPU              {env['cpu']} x{env['cpu_count']} logical")
    add(f"Platform         {env['platform']}")
    add(f"Python           {env['python_version'].splitlines()[0]}")
    add(f"GIL              build Py_GIL_DISABLED={env['gil_disabled_build']}, runtime enabled={env['gil_enabled_at_runtime']}")
    load = env["loadavg_1_5_15"]
    if load:
        add(f"Load at start    {load[0]:.2f} / {load[1]:.2f} / {load[2]:.2f}")
    if results.get("loadavg_end"):
        end = results["loadavg_end"]
        add(f"Load at end      {end[0]:.2f} / {end[1]:.2f} / {end[2]:.2f}")
    add(f"Peak RSS         {_fmt(results.get('peak_rss_mb'), 1, ' MB')}")
    descriptors = results.get("descriptors", {})
    add(f"Descriptors      soft={descriptors.get('soft_after')} (was {descriptors.get('soft_before')}), status={descriptors.get('status')}")
    add("")

    ledger = results.get("ledger")
    add("RUN")
    add(f"  mode               {config['mode']}")
    add(f"  connections        {config['connections']:,}")
    if ledger:
        add(f"  LEDGER MODE        {ledger['mode']} ({ledger['backend_name']}) -- database wait included: {ledger['includes_database_wait']}")
    else:
        add(f"  LEDGER MODE        {config['ledger_mode']} (not constructed; run aborted before the rig was built)")
    add(f"  difficulty mode    {config['difficulty_mode']}")
    add(f"  share interval     {config['share_interval_seconds']:g}s")
    add(f"  template           {config['template_transactions']:,} transactions x {config['template_transaction_bytes']:,} bytes")
    add(f"  settle / measure   {config['settle_seconds']:g}s / {config['measure_seconds']:g}s")
    add(f"  lock attribution   {'on' if config['lock_attribution'] else 'off'}")
    add("")
    add(f"Clock            {env['clock_rationale']}")
    add("")

    controls = results.get("controls")
    add(_rule("-"))
    add("CONTROLS (this host, this process, immediately before the run)")
    add(_rule("-"))
    if controls is None:
        add("  skipped (--skip-controls): no evidence this host can show parallelism")
    else:
        positive = controls["positive_sha256_1mib"]
        negative = controls["negative_pure_python"]
        add(f"  positive: 1 MiB sha256 x{controls['threads']} threads   {positive['cores_used']:.2f} cores over {positive['wall_s']:.2f}s")
        add(f"  negative: pure-Python  x{controls['threads']} threads   {negative['cores_used']:.2f} cores over {negative['wall_s']:.2f}s")
        add(f"  floor required: {config['control_floor_fraction'] * controls['threads']:.2f} cores")
    add("")

    if results.get("aborted"):
        abort = results["aborted"]
        add(_rule())
        add(f"ABORTED: {abort['reason']}")
        add(_rule())
        add("")
        for line in _wrap(abort["detail"]):
            add(f"  {line}")
        add("")
        add("  No stage, load or herd conclusions are reported: the run could not")
        add("  establish a trustworthy measurement environment, so there is nothing")
        add("  here that would be evidence about the coordinator.")
        add("")
        return "\n".join(out)

    settle = results.get("settle")
    if settle:
        add(_rule("-"))
        add("SETTLE: session establishment")
        add(_rule("-"))
        add(f"  requested          {settle['requested']:,}")
        add(f"  established        {settle['established']:,}")
        add(f"  NOT established    {settle['not_established']:,}")
        add(f"  wall               {settle['wall_seconds']:.2f}s (window {settle['window_seconds']:g}s)")
        latency = settle["establish_latency"]
        if latency["count"]:
            add(f"  establish latency  p50 {_fmt(latency['p50_ms'])}ms  p95 {_fmt(latency['p95_ms'])}ms  p99 {_fmt(latency['p99_ms'])}ms  max {_fmt(latency['max_ms'])}ms")
        if settle["failure_kinds"]:
            add(f"  failure kinds      {settle['failure_kinds']}")
        add("")

    herd = results.get("herd")
    if herd:
        add(_rule("-"))
        add("HERD: reconnect census (the headline, not throughput)")
        add(_rule("-"))
        census = herd["census"]
        add(f"  dropped together        {herd['dropped']:,} in {herd['drop_seconds'] * 1000:.0f}ms")
        add(f"  re-established          {herd['re_established']:,} / {census['requested']:,}")
        add(f"  NEVER re-established    {herd['never_re_established']:,}  (within {herd['return_window_seconds']:g}s)")
        latency = census["establish_latency"]
        if latency["count"]:
            add(f"  return latency          p50 {_fmt(latency['p50_ms'])}ms  p95 {_fmt(latency['p95_ms'])}ms  p99 {_fmt(latency['p99_ms'])}ms  max {_fmt(latency['max_ms'])}ms")
        if census["failure_kinds"]:
            add(f"  failure kinds           {census['failure_kinds']}")
        if herd.get("vardiff_resume_outcomes"):
            add(f"  vardiff resume          {herd['vardiff_resume_outcomes']}")
        herd_lock = herd.get("lock") or {}
        add(f"  lock during herd        {herd_lock.get('acquisitions', 0):,} acquisitions, {_fmt(herd_lock.get('contended_percent'), 1)}% contended")
        add(f"  lock wait total / max   {_fmt(herd_lock.get('wait_seconds_sum'), 3)}s / {_fmt((herd_lock.get('wait_seconds_max') or 0) * 1000)}ms")
        for line in _render_hold_sites(herd_lock, indent="  "):
            add(line)
        add("")

    load_result = results.get("load") or {}
    if load_result.get("threads"):
        add(_rule("-"))
        add(f"LOAD: {config['mode']} share submission")
        add(_rule("-"))
        add(f"  submitting sessions {load_result['threads']:,}")
        add(f"  wall                {load_result['wall_seconds']:.2f}s")
        add(f"  cores-used          {_fmt(load_result['cores_used'])}  (of {env['cpu_count']} logical, GIL probe discounted)")
        if load_result.get("cores_used_raw") is not None:
            add(
                f"    whole process     {_fmt(load_result['cores_used_raw'])} "
                f"including {_fmt(load_result.get('probe_cpu_seconds'), 2, 's')} of GIL-probe CPU"
            )
        add(f"  accepted shares     {load_result['accepted_shares']:,}")
        add(f"  rejected shares     {load_result['rejected_shares']:,}")
        add(f"  shares/s            {_fmt(load_result['shares_per_second'])}")
        add(f"  CPU us/share        {_fmt(load_result['cpu_us_per_share'])}  (GIL probe discounted)")
        ack = load_result["ack_latency"]
        add(f"  ack latency         p50 {_fmt(ack['p50_ms'])}ms  p95 {_fmt(ack['p95_ms'])}ms  p99 {_fmt(ack['p99_ms'])}ms  max {_fmt(ack['max_ms'])}ms")
        add(f"  nonce hashes        {load_result['nonce_hashes']:,} (driver-side search cost, included above)")
        if load_result["reject_reasons"]:
            add(f"  reject reasons      {load_result['reject_reasons']}")
        if load_result["pump_errors"]:
            add(f"  submit errors       {load_result['pump_errors']}")
        add("")
    elif load_result:
        add(_rule("-"))
        add(f"LOAD: {config['mode']} share submission")
        add(_rule("-"))
        add(f"  {load_result.get('note', 'no load ran')}")
        add("")

    lock = results.get("lock")
    if lock:
        add(_rule("-"))
        add("COORDINATOR LOCK (measurement window only)")
        add(_rule("-"))
        add(f"  acquisitions        {lock['acquisitions']:,}")
        add(f"  contentions         {lock['contentions']:,}  ({_fmt(lock['contended_percent'], 1)}%)")
        add(f"  wait total / max    {_fmt(lock['wait_seconds_sum'], 3)}s / {_fmt(lock['wait_seconds_max'] * 1000)}ms")
        for line in _render_hold_sites(lock, indent=""):
            add(line)
        add("")
        total = results.get("lock_total")
        if total:
            add("  Whole run, including settle and any herd -- this is where the")
            add("  establishment path (accept_loop, schedule_initial_job) shows up:")
            add(f"    acquisitions      {total['acquisitions']:,}")
            add(f"    contentions       {total['contentions']:,}  ({_fmt(total['contended_percent'], 1)}%)")
            for line in _render_hold_sites(total, indent="  "):
                add(line)
            add("")

    probe = results.get("gil_wait_probe")
    if probe and probe.get("samples"):
        add(_rule("-"))
        add("GIL-WAIT PROBE (fixed work unit, one dedicated thread)")
        add(_rule("-"))
        add(f"  samples             {probe['samples']:,} x {probe['iterations_per_sample']:,} iterations")
        add(f"  idle baseline       {_fmt(probe['baseline_ms'], 3)}ms")
        add(f"  under load          median {_fmt(probe['median_ms'], 3)}ms  p95 {_fmt(probe['p95_ms'], 3)}ms  max {_fmt(probe['max_ms'], 3)}ms")
        add(f"  stretch             median {_fmt(probe['stretch_median'])}x  p95 {_fmt(probe['stretch_p95'])}x")
        add(f"  GIL wait (median)   {_fmt(probe['gil_wait_fraction_median'] * 100, 1)}% of the unit's wall time")
        add("")

    add(_rule("-"))
    add("HEALTH")
    add(_rule("-"))
    add(f"  watchdog misses     {results.get('watchdog_misses') or 'none'}")
    add(f"  rejections          {results.get('rejections') or 'none'}")
    if results.get("vardiff_resume_outcomes"):
        add(f"  vardiff resume      {results['vardiff_resume_outcomes']}")
    guard = results.get("memory_guard") or {}
    add(f"  peak RSS            {_fmt(guard.get('peak_rss_mb'), 1, ' MB')} (guard {_fmt(guard.get('limit_mb'), 0, ' MB')}){' EXCEEDED' if guard.get('exceeded') else ''}")
    if results.get("wall_floor_warning"):
        add(f"  wall floor          {results['wall_floor_warning']}")
    if results.get("accept_errors"):
        add(f"  accept errors       {len(results['accept_errors'])} (first shown in JSON)")
    add("")

    add(_rule("-"))
    add("HOW TO READ THIS")
    add(_rule("-"))
    for note in results.get("interpretation", INTERPRETATION_NOTES):
        for index, line in enumerate(_wrap(note)):
            add(f"  {'* ' if index == 0 else '  '}{line}")
    add("")
    add("  Historical calibration from the original #143 run, which pins what")
    add("  these numbers mean and is NOT a threshold for this host:")
    for key, value in results.get("calibration", CALIBRATION).items():
        for index, line in enumerate(_wrap(f"{key}: {value}", width=68)):
            add(f"    {'- ' if index == 0 else '  '}{line}")
    add("")
    return "\n".join(out)


def _render_hold_sites(lock: dict[str, Any], *, indent: str = "") -> list[str]:
    """Render the per-call-site hold table, or say why there isn't one."""
    if not lock:
        return []
    if not lock.get("attribution_enabled", True):
        return [f"{indent}  hold attribution    off (--no-lock-attribution)"]
    sites = lock.get("sites") or []
    if not sites:
        return [f"{indent}  hold attribution    no holds recorded in this window"]
    lines = [
        "",
        f"{indent}  hold attribution by call site (top 8 by total hold):",
        f"{indent}    {'call site':<54}{'holds':>9}{'total s':>10}{'mean us':>10}{'max ms':>9}",
    ]
    for row in sites[:8]:
        lines.append(
            f"{indent}    {row['site'][:53]:<54}{row['holds']:>9,}"
            f"{row['hold_seconds']:>10.3f}{_fmt(row['mean_hold_us'], 1):>10}"
            f"{_fmt(row['max_hold_ms'], 1):>9}"
        )
    return lines


def _wrap(text: str, width: int = 72) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current: list[str] = []
    for word in words:
        if current and sum(len(w) + 1 for w in current) + len(word) > width:
            lines.append(" ".join(current))
            current = [word]
        else:
            current.append(word)
    if current:
        lines.append(" ".join(current))
    return lines or [""]


# --------------------------------------------------------------------------
# self-test
# --------------------------------------------------------------------------


def _selftest(argv_json_path: str | None = None) -> int:
    """Smoke the driver itself at a size that fits any host.

    Not a unit test and not collected by discovery: this proves the
    instrument runs, captures, replays, and refuses to report conclusions
    after an abort. It is the check to run before trusting a real sweep.
    """
    failures: list[str] = []

    def check(name: str, condition: bool, detail: str = "") -> None:
        status = "PASS" if condition else "FAIL"
        print(f"  [{status}] {name}{'' if condition else f' -- {detail}'}", flush=True)
        if not condition:
            failures.append(name)

    print("selftest: discovery safety", flush=True)
    check(
        "filename is not collected by unittest discovery",
        not Path(__file__).name.startswith("test"),
        f"{Path(__file__).name} would match the default test*.py pattern",
    )

    base = dict(
        connections=4,
        settle_seconds=10.0,
        measure_seconds=2.0,
        herd_return_seconds=10.0,
        min_wall_seconds=0.0,
        control_threads=2,
        control_min_wall_seconds=0.2,
        control_floor_fraction=0.0,
        max_loadavg_per_core=1e9,
        connect_timeout_seconds=20.0,
    )

    captured: dict[str, Any] | None = None
    for mode in ("paced", "saturating", "herd"):
        print(f"selftest: {mode} mode at N=4", flush=True)
        config = DriverConfig(mode=mode, share_interval_seconds=0.5, **base)
        try:
            result = run(config)
        except BaseException as exc:  # noqa: BLE001 - the smoke wants the reason
            check(f"{mode} run completes", False, f"{type(exc).__name__}: {exc}")
            continue
        check(f"{mode} run completes", True)
        check(f"{mode} was not aborted", not result.get("aborted"), str(result.get("aborted")))
        check(f"{mode} established every session", result["settle"]["established"] == 4, str(result["settle"]))
        check(f"{mode} identifies the ledger mode", bool(result.get("ledger", {}).get("mode")))
        check(f"{mode} recorded lock acquisitions", (result.get("lock") or {}).get("acquisitions", 0) > 0)
        if mode == "herd":
            check("herd reports a per-session census", len(result["herd"]["per_session"]) == 4)
            check(
                "herd reports the exact count that did not return",
                isinstance(result["herd"]["never_re_established"], int),
            )
        else:
            check(f"{mode} accepted shares", result["load"]["accepted_shares"] > 0, str(result["load"]))
        check(f"{mode} renders", bool(render_report(result)))
        captured = result

    print("selftest: JSON capture and render replay", flush=True)
    if captured is not None:
        path = argv_json_path or os.path.join(
            os.environ.get("TMPDIR", "/tmp"), "prism_session_load_selftest.json"
        )
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(captured, handle, indent=2, default=str)
        with open(path, encoding="utf-8") as handle:
            replayed = json.load(handle)
        first = render_report(replayed)
        second = render_report(json.loads(json.dumps(replayed, default=str)))
        check("render replay is byte-identical across reloads", first == second)
        check("render replay names the ledger mode", "LEDGER MODE" in first)
        print(f"  capture written to {path}", flush=True)

    print("selftest: control-floor abort", flush=True)
    abort_config = DriverConfig(
        connections=2,
        measure_seconds=1.0,
        control_threads=2,
        control_min_wall_seconds=0.2,
        # Arithmetically unreachable rather than merely demanding: 2 threads
        # cannot exceed 2.00 cores-used, so a floor of 5x thread count can
        # never be met on any host. A floor that a fast idle machine could
        # occasionally clear would make this smoke pass or fail with load.
        control_floor_fraction=5.0,
        max_loadavg_per_core=1e9,
    )
    aborted = run(abort_config)
    check("aborts when the control floor is not met", bool(aborted.get("aborted")), str(aborted.get("aborted")))
    if aborted.get("aborted"):
        check("abort reason is the control floor", aborted["aborted"]["reason"] == "control-floor")
    report = render_report(aborted)
    check("abort report says ABORTED", "ABORTED" in report)
    for forbidden in ("SETTLE:", "LOAD:", "HERD:", "COORDINATOR LOCK"):
        check(f"abort report omits {forbidden!r}", forbidden not in report)

    print("", flush=True)
    if failures:
        print(f"selftest FAILED: {len(failures)} check(s): {failures}", flush=True)
        return 1
    print("selftest PASSED", flush=True)
    return 0


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--json", action="store_true", help="emit the full result set as JSON")
    parser.add_argument(
        "--render",
        metavar="PATH",
        default=None,
        help=(
            "render a previously captured --json file instead of measuring. "
            "Running the sweep twice to get both formats would put two load "
            "generators on the same cores and corrupt both."
        ),
    )
    parser.add_argument("--selftest", action="store_true", help="smoke the driver itself and exit")
    parser.add_argument("--connections", type=int, default=64, help="connection count (default 64; supports 1024+)")
    parser.add_argument(
        "--mode",
        choices=("paced", "saturating", "herd"),
        default="paced",
        help="paced: one share per interval per connection (the design point); "
        "saturating: submit as fast as answered; herd: drop all and census the return",
    )
    parser.add_argument(
        "--share-interval-seconds",
        type=float,
        default=DEFAULT_SHARE_INTERVAL_SECONDS,
        help=f"paced submit interval (default {DEFAULT_SHARE_INTERVAL_SECONDS:g}, the vardiff target)",
    )
    parser.add_argument("--settle-seconds", type=float, default=50.0, help="session establishment window (default 50)")
    parser.add_argument("--measure-seconds", type=float, default=30.0, help="measurement window (default 30)")
    parser.add_argument("--herd-return-seconds", type=float, default=50.0, help="herd re-establishment window (default 50)")
    parser.add_argument(
        "--ledger",
        dest="ledger_mode",
        choices=("memory", "postgres"),
        default="memory",
        help="memory isolates interpreter cost; postgres uses the repository's real-server gate",
    )
    parser.add_argument(
        "--difficulty-mode",
        choices=("resume", "climb", "pinned"),
        default="resume",
        help="resume: reconnects may adopt their retained difficulty (shipped default); "
        "climb: every reconnect retargets from the floor; "
        "pinned: min==max, isolation only and not representative",
    )
    parser.add_argument("--template-transactions", type=int, default=0, help="synthetic transactions in the template (default 0)")
    parser.add_argument("--template-transaction-bytes", type=int, default=250, help="approximate bytes per synthetic transaction")
    parser.add_argument(
        "--no-lock-attribution",
        dest="lock_attribution",
        action="store_false",
        help="skip per-call-site hold timing, so its own overhead can be quantified",
    )
    parser.add_argument("--control-threads", type=int, default=8, help="threads for the positive/negative controls")
    parser.add_argument(
        "--control-floor-fraction",
        type=float,
        default=0.5,
        help="the positive control must reach this fraction of control-thread count in cores-used, else abort",
    )
    parser.add_argument("--control-min-wall-seconds", type=float, default=0.5, help="wall floor per control measurement")
    parser.add_argument("--min-wall-seconds", type=float, default=5.0, help="warn below this measurement wall clock")
    parser.add_argument("--max-rss-mb", type=float, default=8192.0, help="peak RSS guard")
    parser.add_argument(
        "--max-loadavg-per-core",
        type=float,
        default=2.0,
        help="abort if the 1-minute load average per core exceeds this before measuring",
    )
    parser.add_argument("--connect-timeout-seconds", type=float, default=20.0, help="per-connection socket timeout")
    parser.add_argument("--skip-controls", action="store_true", help="skip controls (removes the parallelism evidence)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.selftest:
        return _selftest()

    if args.render:
        with open(args.render, encoding="utf-8") as handle:
            sys.stdout.write(render_report(json.load(handle)))
        sys.stdout.write("\n")
        return 0

    config = DriverConfig(
        connections=args.connections,
        mode=args.mode,
        share_interval_seconds=args.share_interval_seconds,
        settle_seconds=args.settle_seconds,
        measure_seconds=args.measure_seconds,
        herd_return_seconds=args.herd_return_seconds,
        ledger_mode=args.ledger_mode,
        difficulty_mode=args.difficulty_mode,
        template_transactions=args.template_transactions,
        template_transaction_bytes=args.template_transaction_bytes,
        lock_attribution=args.lock_attribution,
        control_threads=args.control_threads,
        control_floor_fraction=args.control_floor_fraction,
        control_min_wall_seconds=args.control_min_wall_seconds,
        min_wall_seconds=args.min_wall_seconds,
        max_rss_mb=args.max_rss_mb,
        max_loadavg_per_core=args.max_loadavg_per_core,
        connect_timeout_seconds=args.connect_timeout_seconds,
        skip_controls=args.skip_controls,
    )
    try:
        results = run(config)
    except LedgerUnavailable as exc:
        sys.stderr.write(f"{exc}\n")
        return 2

    if args.json:
        json.dump(results, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(render_report(results))
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
