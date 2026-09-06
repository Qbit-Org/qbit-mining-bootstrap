#!/usr/bin/env python3
"""Rust payout-window paging allocation: builder RSS and timing (#240).

``PayoutWindow::from_full_snapshot`` (``crates/qbit-prism/src/window.rs``)
paged the retained records with ``remaining.split_off(page_size)`` in a
loop. ``Vec::split_off`` keeps the original vector's allocation, so every
512-record page retained a backing allocation sized for every record still
unpaged when it was cut, and the tail was copied again on each iteration:
O(N^2/P) retained memory and copying. A prior synthetic reproduction for
#236 measured 9,613 MiB builder RSS and a 12 s response at 210k shares, and
a SIGKILL at 400k. This driver measures that path against the real
``--serve`` daemon binary, before and after the fix, with every experiment
bounded so it cannot exhaust the host.

Per binary and window size it runs one daemon through the coordinator's own
``prepare_window`` protocol over blocking pipes:

=====================  ======================================================
phase                  what it exercises
=====================  ======================================================
``full``               cold full-snapshot preparation of the whole fixture
``advance_small``      ``advance`` with a handful of appended records
``advance_large``      ``advance`` with a multi-page delta (the delta paging
                       loop had the same ``split_off`` shape)
``recenter``           a second full preparation on the same daemon at a
                       smaller weight -- what the coordinator's self-check
                       re-center sends while the previous window is held
``recenter_advance``   ``advance`` against the re-centered digest
=====================  ======================================================

and reports, per phase, wall time split into ``request_write`` (until the
pipe accepted the last request byte), ``response_wait`` (from then until the
envelope line arrived) and ``response_read`` (the raw canonical-items
section), the daemon's own ``metrics`` (JSON parse, fold/advance, canonical
serialization -- Rust processing measured inside the process, so transport
and processing are separated rather than inferred), the daemon's resident
set after the phase, and its lifetime peak (``VmHWM``). The derived
``residual`` column is ``response_wait`` minus the daemon's three timers; the
client's wall interval and the daemon's internal timers are independent
intervals on either side of a pipe, so it is an approximation of scheduling
and envelope handling, not a measured transport term. Every digest the
daemon returns is checked against its own items bytes, the coordinator's
mirror surgery (drop prefix, append suffix) is replayed and re-hashed, and
unless ``--skip-oracle`` is given the shipped Python fold
(``IncrementalShareWindow``) is run on the same inputs as an independent
oracle for every digest.

Bounding: the daemon runs under ``RLIMIT_AS`` (``--daemon-memory-limit-mb``,
default 6144), so a quadratic binary aborts on allocation failure instead of
taking the host down; a size is skipped, and the skip recorded, when
``MemAvailable`` is below the limit plus ``--memory-margin-mb``; and every
exchange -- the handshake included -- runs under a wall-clock deadline
(``--exchange-timeout``) after which the daemon is killed, reaped and the
phase recorded as a failure, so a stalled daemon cannot hang the run. The
known-quadratic baseline should only be run at sizes whose expected footprint
fits the limit (52k and 105k here); the 210k/400k baseline numbers are on
record in ``window_pipeline_gil_scaling.md`` section 9 and are not repeated.

Fixture: ``build_records`` from ``window_pipeline_gil_scaling.py`` -- the
same production-shaped ``share_id`` fixture (200 identities, difficulty
16384) the #236 probe and the cited baseline used -- with ``window_weight``
equal to the fixture's total difficulty so every record is retained.

Self-contained: standard library plus read-only imports from ``lab/``; no
database, network or coordinator. Example::

    cargo build --release --locked -p qbit-prism --bin qbit-prism-build-audit-bundle
    python3 tests/perf/window_paging_allocation.py \
        --daemon-binary target/release/qbit-prism-build-audit-bundle \
        --baseline-binary /path/to/old/qbit-prism-build-audit-bundle \
        --sizes 52000,105000,210000,400000 --baseline-sizes 52000,105000 \
        --json window_paging_allocation.json

This is an **on-demand instrument, not a test**: it asserts no thresholds
and is deliberately not named ``test_*`` so discovery never runs it (#160).
Nothing under ``lab/`` is modified.

Memory measurements use MiB (2**20 bytes). Existing ``_mb`` result keys and
``*-mb`` CLI option names are retained and use the same binary units.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism.bundle_compiler import (  # noqa: E402 - path fix-up above
    PRISM_SERVE_BUILDER_PROTOCOL_VERSION,
)
from lab.prism.share_ledger import (  # noqa: E402
    DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
    AcceptedShareRecord,
    IncrementalShareWindow,
)
from tests.perf.window_pipeline_gil_scaling import (  # noqa: E402
    benchmark_miner_programs,
    build_records,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DAEMON_BINARY = "qbit-prism-build-audit-bundle"
DEFAULT_TOOL_BIN_DIR = REPO_ROOT / "target" / "release"
DEFAULT_SIZES = "52000,105000,210000,400000"
DEFAULT_BASELINE_SIZES = "52000,105000"
DEFAULT_MINERS = 200
DEFAULT_SMALL_DELTA = 16
DEFAULT_LARGE_DELTA = 2 * DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE + 19
WRITE_CHUNK = 1 << 20
DEFAULT_EXCHANGE_TIMEOUT = 600.0
DEFAULT_SHUTDOWN_TIMEOUT = 30.0
PHASES = ("full", "advance_small", "advance_large", "recenter", "recenter_advance")


# ---------------------------------------------------------------------------
# host observation
# ---------------------------------------------------------------------------


def read_meminfo_mb(key: str) -> float | None:
    try:
        with open("/proc/meminfo", encoding="ascii") as handle:
            for line in handle:
                if line.startswith(key + ":"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        return None
    return None


def read_proc_status_mb(pid: int) -> dict[str, float]:
    """``VmRSS``/``VmHWM``/``VmSize``/``VmPeak`` of a live process, in MiB."""
    out: dict[str, float] = {}
    try:
        with open(f"/proc/{pid}/status", encoding="ascii") as handle:
            for line in handle:
                key, _, rest = line.partition(":")
                if key in ("VmRSS", "VmHWM", "VmSize", "VmPeak"):
                    out[key] = int(rest.split()[0]) / 1024.0
    except (OSError, ValueError):
        pass
    return out


def machine_info() -> dict[str, Any]:
    try:
        load = os.getloadavg()
    except OSError:
        load = None
    return {
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
        "cpu_count": os.cpu_count(),
        "mem_total_mb": read_meminfo_mb("MemTotal"),
        "mem_available_mb_at_start": read_meminfo_mb("MemAvailable"),
        "load_average_at_start": load,
        "malloc_arena_max": os.environ.get("MALLOC_ARENA_MAX"),
    }


def git_describe(path: Path) -> dict[str, Any]:
    """The source revision this driver runs from, and whether it is clean."""
    def run(*args: str) -> str | None:
        try:
            return subprocess.run(
                ["git", *args], cwd=path, capture_output=True, text=True, check=True
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            return None

    return {
        "commit": run("rev-parse", "HEAD"),
        "branch": run("rev-parse", "--abbrev-ref", "HEAD"),
        "dirty": bool(run("status", "--porcelain", "--untracked-files=no")),
    }


def binary_identity(binary: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with open(binary, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    stat = binary.stat()
    return {
        "path": str(binary),
        "sha256": digest.hexdigest(),
        "size_bytes": stat.st_size,
        "mtime": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(stat.st_mtime)),
    }


# ---------------------------------------------------------------------------
# RSS sampling
# ---------------------------------------------------------------------------


class RssSampler:
    """Samples ``VmRSS`` of one pid on a thread; peak comes from ``VmHWM``."""

    def __init__(self, pid: int, interval: float) -> None:
        self.pid = pid
        self.interval = interval
        self.samples: list[tuple[float, float]] = []
        self.max_sampled_mb = 0.0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="rss-sampler", daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            status = read_proc_status_mb(self.pid)
            rss = status.get("VmRSS")
            if rss is not None:
                self.samples.append((time.perf_counter(), rss))
                self.max_sampled_mb = max(self.max_sampled_mb, rss)
            self._stop.wait(self.interval)

    def __enter__(self) -> RssSampler:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join()


# ---------------------------------------------------------------------------
# the daemon exchange
# ---------------------------------------------------------------------------


@dataclass
class Exchange:
    request_bytes: int
    request_write_seconds: float
    response_wait_seconds: float
    response_read_seconds: float
    envelope: dict[str, Any] | None
    payload: bytes | None
    error: str | None = None


class DaemonTimeout(RuntimeError):
    """An exchange (or the handshake) exceeded its wall-clock deadline."""


class Daemon:
    """One ``--serve`` daemon over blocking pipes, memory- and time-bounded.

    Every exchange, the handshake included, runs under ``exchange_timeout``:
    a watchdog kills the daemon when it expires, which unblocks the pending
    pipe read (EOF) and the pending write (EPIPE), and the exchange is
    reported as a timeout failure. ``close`` is idempotent and always reaps
    the child and closes every pipe and the stderr capture; the constructor
    calls it before re-raising if the handshake fails.
    """

    def __init__(
        self,
        binary: Path,
        *,
        memory_limit_mb: int | None,
        stderr_path: Path,
        exchange_timeout: float = DEFAULT_EXCHANGE_TIMEOUT,
        shutdown_timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
    ) -> None:
        self.binary = binary
        self.stderr_path = stderr_path
        self.exchange_timeout = exchange_timeout
        self.shutdown_timeout = shutdown_timeout
        self.exit_code: int | None = None
        self.timed_out: str | None = None
        self.status_at_kill: dict[str, float] = {}
        self._outcome: dict[str, Any] | None = None
        self._lock = threading.Lock()
        limit_bytes = None if not memory_limit_mb else memory_limit_mb * 1024 * 1024

        def preexec() -> None:
            if limit_bytes is not None:
                resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))

        self._stderr = open(stderr_path, "wb")
        try:
            self.process = subprocess.Popen(
                [
                    str(binary),
                    "--serve",
                    "--signing-key-seed-hex",
                    "42" * 32,
                    "--ledger-signing-key-seed-hex",
                    "43" * 32,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self._stderr,
                preexec_fn=preexec,
            )
        except Exception:
            self._stderr.close()
            raise
        try:
            assert self.process.stdout is not None
            with self._watchdog("handshake"):
                handshake_line = self.process.stdout.readline()
            if self.timed_out:
                raise DaemonTimeout(
                    f"{binary} sent no handshake within {exchange_timeout:g}s; killed"
                )
            if not handshake_line:
                raise RuntimeError(f"{binary} produced no handshake (see {stderr_path})")
            handshake = json.loads(handshake_line)
            expected = {
                "event": "handshake",
                "tool": DAEMON_BINARY,
                "protocol": PRISM_SERVE_BUILDER_PROTOCOL_VERSION,
            }
            announced = {key: handshake.get(key) for key in expected}
            if announced != expected:
                raise RuntimeError(f"daemon announced {announced!r}, expected {expected!r}")
        except BaseException as error:
            outcome = self.close()
            if isinstance(error, Exception):
                # Let a caller (or a test) see how the child was reaped.
                error.daemon_outcome = outcome  # type: ignore[attr-defined]
            raise

    @property
    def pid(self) -> int:
        return self.process.pid

    def alive(self) -> bool:
        return self.process.poll() is None

    def status_mb(self) -> dict[str, float]:
        return read_proc_status_mb(self.process.pid)

    def _kill_on_timeout(self, label: str) -> None:
        with self._lock:
            if self.timed_out is None:
                self.timed_out = label
                # The high-water mark is lost once the child is reaped, so
                # sample it before the kill.
                self.status_at_kill = self.status_mb()
        if self.process.poll() is None:
            self.process.kill()

    class _Watchdog:
        def __init__(self, daemon: Daemon, label: str) -> None:
            self.timer = threading.Timer(daemon.exchange_timeout, daemon._kill_on_timeout, (label,))
            self.timer.daemon = True

        def __enter__(self) -> Daemon._Watchdog:
            self.timer.start()
            return self

        def __exit__(self, *_exc: object) -> None:
            self.timer.cancel()

    def _watchdog(self, label: str) -> Daemon._Watchdog:
        return Daemon._Watchdog(self, label)

    def exchange(self, request: bytes, label: str = "exchange") -> Exchange:
        """Write one request line, read the envelope and its raw section.

        Bounded by ``exchange_timeout`` end to end; a timeout kills the daemon
        and is reported in ``Exchange.error``.
        """
        process = self.process
        assert process.stdin is not None and process.stdout is not None
        if self.timed_out or process.poll() is not None:
            return Exchange(len(request), 0.0, 0.0, 0.0, None, None, error="daemon is not running")
        stdin_fd = process.stdin.fileno()
        stdout = process.stdout
        write_done: list[float] = []
        write_error: list[str] = []

        def writer() -> None:
            view = memoryview(request)
            offset = 0
            try:
                while offset < len(view):
                    offset += os.write(stdin_fd, view[offset : offset + WRITE_CHUNK])
            except OSError as exc:
                write_error.append(f"{type(exc).__name__}: {exc}")
            finally:
                write_done.append(time.perf_counter())

        def timeout_error(detail: str) -> str:
            return f"{label} exceeded {self.exchange_timeout:g}s; daemon killed ({detail})"

        with self._watchdog(label):
            started = time.perf_counter()
            thread = threading.Thread(target=writer, name="request-writer", daemon=True)
            thread.start()
            envelope_line = stdout.readline()
            envelope_at = time.perf_counter()
            # The write either finished, failed with EPIPE once the daemon
            # died, or is stuck behind a daemon that stopped reading, which
            # the watchdog kill also unblocks; the join is bounded regardless.
            thread.join(timeout=max(1.0, self.exchange_timeout))
            if thread.is_alive():
                write_done.append(time.perf_counter())
                write_error.append("request writer still blocked after the exchange")
            write_seconds = write_done[0] - started
            wait_seconds = envelope_at - write_done[0]
            if not envelope_line:
                detail = "closed stdout without an envelope" + (
                    f"; write: {write_error[0]}" if write_error else ""
                )
                error = timeout_error(detail) if self.timed_out else f"daemon {detail}"
                return Exchange(len(request), write_seconds, wait_seconds, 0.0, None, None, error=error)
            envelope = json.loads(envelope_line)
            payload: bytes | None = None
            read_seconds = 0.0
            if envelope.get("ok") is True:
                length_key = "window_items_len" if "window_items_len" in envelope else "appended_items_len"
                length = int(envelope[length_key])
                chunks: list[bytes] = []
                remaining = length
                while remaining > 0:
                    chunk = stdout.read(min(remaining, 1 << 22))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                newline = stdout.read(1)
                read_seconds = time.perf_counter() - envelope_at
                payload = b"".join(chunks)
                if remaining or newline != b"\n":
                    detail = f"short raw section: {len(payload)} of {length} bytes"
                    error = timeout_error(detail) if self.timed_out else detail
                    return Exchange(len(request), write_seconds, wait_seconds, read_seconds, envelope, payload, error=error)
            return Exchange(len(request), write_seconds, wait_seconds, read_seconds, envelope, payload)

    def close(self) -> dict[str, Any]:
        """Reap the daemon and close every handle; idempotent.

        Closing stdin asks a healthy daemon to exit; one that has not exited
        within ``shutdown_timeout`` is killed. ``VmHWM`` is read before the
        child is reaped (it is gone from ``/proc`` afterwards).
        """
        if self._outcome is not None:
            return self._outcome
        process = self.process
        final_status = self.status_mb() if process.poll() is None else {}
        if process.stdin is not None:
            try:
                process.stdin.close()
            except OSError:
                pass
        try:
            self.exit_code = process.wait(timeout=self.shutdown_timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            self.exit_code = process.wait(timeout=10.0)
        if process.stdout is not None:
            process.stdout.close()
        self._stderr.close()
        stderr_tail = ""
        try:
            stderr_tail = self.stderr_path.read_text(errors="replace")[-2000:]
        except OSError:
            pass
        peak = final_status.get("VmHWM", self.status_at_kill.get("VmHWM"))
        peak_vsize = final_status.get("VmPeak", self.status_at_kill.get("VmPeak"))
        self._outcome = {
            "pid": process.pid,
            "exit_code": self.exit_code,
            "signal": -self.exit_code if self.exit_code is not None and self.exit_code < 0 else None,
            "timed_out": self.timed_out,
            "peak_rss_mb": peak,
            "peak_vsize_mb": peak_vsize,
            "stderr_tail": stderr_tail,
        }
        return self._outcome


# ---------------------------------------------------------------------------
# fixture and requests
# ---------------------------------------------------------------------------


def delta_records(last: AcceptedShareRecord, count: int, *, miners: int, offset: int) -> list[AcceptedShareRecord]:
    """``count`` appended records after ``last``, shaped like the probe's delta."""
    programs = benchmark_miner_programs(miners)
    out: list[AcceptedShareRecord] = []
    for index in range(count):
        seq = int(last.share_seq) + offset + index + 1
        miner = seq % miners
        out.append(
            AcceptedShareRecord(
                share_seq=seq,
                share_id=f"bench-miner-{miner}.rig{seq % 512}:{'ab' * 32}",
                miner_id=f"bench-miner-{miner}",
                order_key=f"bench-miner-{miner:06d}",
                p2mr_program_hex=programs[miner],
                share_difficulty=int(last.share_difficulty),
                network_difficulty=int(last.network_difficulty),
                template_height=int(last.template_height),
                job_id=f"bench-job-{seq}",
                job_issued_at_ms=int(last.job_issued_at_ms) + offset + index + 1,
                accepted_at_ms=int(last.accepted_at_ms) + offset + index + 1,
                ntime=int(last.ntime) + offset + index + 1,
            )
        )
    return out


def encode_request(fields: dict[str, Any]) -> bytes:
    """The bytes the coordinator sends: one compact JSON line."""
    return json.dumps(fields, separators=(",", ":")).encode("ascii") + b"\n"


def full_request(records_json: list[dict[str, object]], *, anchor: int, weight: int, page_size: int) -> bytes:
    return encode_request(
        {
            "request": "prepare_window",
            "mode": "full",
            "append_invalidation_epoch": 0,
            "anchor_job_issued_at_ms": anchor,
            "records": records_json,
            "window_weight": weight,
            "page_size": page_size,
        }
    )


def advance_request(records_json: list[dict[str, object]], *, anchor: int, base_digest: str) -> bytes:
    return encode_request(
        {
            "request": "prepare_window",
            "mode": "advance",
            "append_invalidation_epoch": 0,
            "anchor_job_issued_at_ms": anchor,
            "records": records_json,
            "base_digest": base_digest,
        }
    )


def items_digest(items: bytes) -> str:
    return hashlib.sha256(b"[" + items + b"]").hexdigest()


@dataclass
class Fixture:
    size: int
    page_size: int
    records: list[AcceptedShareRecord]
    records_json: list[dict[str, object]]
    anchor: int
    weight: int
    recenter_weight: int
    small_delta: list[AcceptedShareRecord]
    small_anchor: int
    large_delta: list[AcceptedShareRecord]
    large_anchor: int
    recenter_delta: list[AcceptedShareRecord]
    recenter_delta_anchor: int
    build_seconds: float


def build_fixture(size: int, *, miners: int, page_size: int, small: int, large: int) -> Fixture:
    started = time.perf_counter()
    records = build_records(size, miners=miners, share_id_shape="production")
    anchor = int(records[-1].job_issued_at_ms)
    weight = sum(int(record.share_difficulty) for record in records)
    last = records[-1]
    small_delta = delta_records(last, small, miners=miners, offset=0)
    small_anchor = int(last.job_issued_at_ms) + small + 1
    large_delta = delta_records(last, large, miners=miners, offset=small + 1)
    large_anchor = int(last.job_issued_at_ms) + small + 1 + large + 1
    # The re-center starts again from the original snapshot at a smaller
    # live weight; its advance appends after the original last record.
    recenter_delta = delta_records(last, small, miners=miners, offset=0)
    return Fixture(
        size=size,
        page_size=page_size,
        records=records,
        records_json=[record.to_prism_json() for record in records],
        anchor=anchor,
        weight=weight,
        recenter_weight=(weight * 3) // 4,
        small_delta=small_delta,
        small_anchor=small_anchor,
        large_delta=large_delta,
        large_anchor=large_anchor,
        recenter_delta=recenter_delta,
        recenter_delta_anchor=small_anchor,
        build_seconds=time.perf_counter() - started,
    )


# ---------------------------------------------------------------------------
# the Python oracle
# ---------------------------------------------------------------------------


def python_oracle(fixture: Fixture) -> dict[str, Any]:
    """Every expected digest from the shipped in-process fold."""
    timings: dict[str, float] = {}
    digests: dict[str, str] = {}
    counts: dict[str, int] = {}

    def timed(name: str, run: Callable[[], IncrementalShareWindow]) -> IncrementalShareWindow:
        started = time.perf_counter()
        window = run()
        timings[name] = time.perf_counter() - started
        digests[name] = window.json_records().canonical_json_sha256()
        counts[name] = int(window.record_count)
        return window

    full = timed(
        "full",
        lambda: IncrementalShareWindow.from_full_snapshot(
            fixture.records,
            anchor_job_issued_at_ms=fixture.anchor,
            window_weight=fixture.weight,
            page_size=fixture.page_size,
        ),
    )
    small = timed(
        "advance_small",
        lambda: full.advance(fixture.small_delta, anchor_job_issued_at_ms=fixture.small_anchor)[0],
    )
    timed(
        "advance_large",
        lambda: small.advance(fixture.large_delta, anchor_job_issued_at_ms=fixture.large_anchor)[0],
    )
    recentered = timed(
        "recenter",
        lambda: IncrementalShareWindow.from_full_snapshot(
            fixture.records,
            anchor_job_issued_at_ms=fixture.anchor,
            window_weight=fixture.recenter_weight,
            page_size=fixture.page_size,
        ),
    )
    timed(
        "recenter_advance",
        lambda: recentered.advance(
            fixture.recenter_delta, anchor_job_issued_at_ms=fixture.recenter_delta_anchor
        )[0],
    )
    return {"digests": digests, "record_counts": counts, "seconds": timings}


# ---------------------------------------------------------------------------
# one daemon run
# ---------------------------------------------------------------------------


@dataclass
class PhaseResult:
    phase: str
    records_sent: int
    request_bytes: int
    request_write_seconds: float
    response_wait_seconds: float
    response_read_seconds: float
    daemon_metrics: dict[str, float] = field(default_factory=dict)
    status: str = ""
    digest: str | None = None
    record_count: int | None = None
    items_bytes: int | None = None
    self_consistent: bool | None = None
    oracle_match: bool | None = None
    rss_after_mb: float | None = None
    hwm_after_mb: float | None = None
    stats: dict[str, int] = field(default_factory=dict)
    error: str | None = None

    @property
    def residual_wait_seconds(self) -> float | None:
        """``response_wait`` minus the daemon's own timers -- an approximation.

        The client's wall interval (from the last request byte accepted to
        the envelope line received) and the daemon's ``Instant`` timers are
        independent intervals measured on either side of a pipe: the daemon
        may still be reading and parsing while the client clock starts, and
        the envelope write and scheduling are on neither timer. So this is
        roughly scheduling plus envelope handling, not a measured transport
        term, and it is clamped at zero. None when the daemon reported no
        metrics.
        """
        if not self.daemon_metrics:
            return None
        return max(0.0, self.response_wait_seconds - sum(self.daemon_metrics.values()))


@dataclass
class RunResult:
    variant: str
    binary: str
    size: int
    page_size: int
    mem_available_before_mb: float | None
    load_before: tuple[float, float, float] | None
    memory_limit_mb: int | None
    phases: list[PhaseResult]
    outcome: dict[str, Any]
    max_sampled_rss_mb: float
    rss_timeline: list[tuple[float, float]]
    skipped: str | None = None
    error: str | None = None


def run_variant(
    label: str,
    binary: Path,
    fixture: Fixture,
    *,
    oracle: dict[str, Any] | None,
    memory_limit_mb: int | None,
    memory_margin_mb: int,
    sample_interval: float,
    exchange_timeout: float,
    stderr_dir: Path,
    log: Callable[[str], None],
) -> RunResult:
    available = read_meminfo_mb("MemAvailable")
    try:
        load = os.getloadavg()
    except OSError:
        load = None
    required = (memory_limit_mb or 0) + memory_margin_mb
    if available is not None and available < required:
        reason = f"MemAvailable {available:.0f} MiB below required {required} MiB"
        log(f"    {label} @ {fixture.size}: skipped, {reason}")
        return RunResult(
            label, str(binary), fixture.size, fixture.page_size, available, load,
            memory_limit_mb, [], {}, 0.0, [], skipped=reason,
        )

    stderr_path = stderr_dir / f"{label}-{fixture.size}.stderr"
    try:
        daemon = Daemon(
            binary,
            memory_limit_mb=memory_limit_mb,
            stderr_path=stderr_path,
            exchange_timeout=exchange_timeout,
        )
    except Exception as exc:  # noqa: BLE001 - reported per run, not raised
        error = f"daemon failed to start: {type(exc).__name__}: {exc}"
        log(f"    {label} @ {fixture.size}: {error}")
        return RunResult(
            label, str(binary), fixture.size, fixture.page_size, available, load,
            memory_limit_mb, [], {}, 0.0, [], error=error,
        )
    phases: list[PhaseResult] = []
    expected = (oracle or {}).get("digests", {})

    def run_phase(name: str, request: bytes, records_sent: int, *, base_items: bytes | None) -> tuple[PhaseResult, bytes | None]:
        log(f"    {label} @ {fixture.size}: {name} ({records_sent} records, {len(request) / 2**20:.1f} MiB request)")
        exchange = daemon.exchange(request, label=name)
        status_after = daemon.status_mb() if daemon.alive() else {}
        result = PhaseResult(
            phase=name,
            records_sent=records_sent,
            request_bytes=exchange.request_bytes,
            request_write_seconds=exchange.request_write_seconds,
            response_wait_seconds=exchange.response_wait_seconds,
            response_read_seconds=exchange.response_read_seconds,
            rss_after_mb=status_after.get("VmRSS"),
            hwm_after_mb=status_after.get("VmHWM"),
            error=exchange.error,
        )
        envelope = exchange.envelope
        if envelope is None:
            result.status = "timeout" if daemon.timed_out else "no_response"
            return result, None
        metrics = envelope.get("metrics")
        if isinstance(metrics, dict):
            result.daemon_metrics = {key: float(value) for key, value in metrics.items()}
        if envelope.get("ok") is not True:
            for key in ("needs_full", "fallback", "fold_invalid", "out_of_range"):
                if envelope.get(key):
                    result.status = key
                    break
            else:
                result.status = "error"
            result.error = str(envelope.get("error"))
            return result, None
        if exchange.error:
            result.status = "timeout" if daemon.timed_out else "short_response"
            return result, None
        result.status = "prepared"
        result.digest = str(envelope["share_snapshot_sha256"])
        result.record_count = int(envelope["record_count"])
        payload = exchange.payload or b""
        if "window_items_len" in envelope:
            items = payload
        else:
            assert base_items is not None
            items = base_items[int(envelope["retained_drop_bytes"]) :] + payload
            result.stats = {
                key: int(envelope[key]) for key in ("added_rows", "expired_rows", "touched_pages")
            }
        result.items_bytes = len(items)
        result.self_consistent = items_digest(items) == result.digest
        if name in expected:
            result.oracle_match = expected[name] == result.digest
        return result, items

    run_error: str | None = None
    max_sampled = 0.0
    timeline: list[tuple[float, float]] = []
    try:
        with RssSampler(daemon.pid, sample_interval) as sampler:
            try:
                full, items = run_phase(
                    "full",
                    full_request(fixture.records_json, anchor=fixture.anchor, weight=fixture.weight, page_size=fixture.page_size),
                    fixture.size,
                    base_items=None,
                )
                phases.append(full)
                if full.status == "prepared" and items is not None and daemon.alive():
                    small, items_small = run_phase(
                        "advance_small",
                        advance_request([r.to_prism_json() for r in fixture.small_delta], anchor=fixture.small_anchor, base_digest=full.digest or ""),
                        len(fixture.small_delta),
                        base_items=items,
                    )
                    phases.append(small)
                    if small.status == "prepared" and items_small is not None and daemon.alive():
                        large, _ = run_phase(
                            "advance_large",
                            advance_request([r.to_prism_json() for r in fixture.large_delta], anchor=fixture.large_anchor, base_digest=small.digest or ""),
                            len(fixture.large_delta),
                            base_items=items_small,
                        )
                        phases.append(large)
                if daemon.alive() and full.status == "prepared":
                    recenter, items_recenter = run_phase(
                        "recenter",
                        full_request(fixture.records_json, anchor=fixture.anchor, weight=fixture.recenter_weight, page_size=fixture.page_size),
                        fixture.size,
                        base_items=None,
                    )
                    phases.append(recenter)
                    if recenter.status == "prepared" and items_recenter is not None and daemon.alive():
                        recenter_advance, _ = run_phase(
                            "recenter_advance",
                            advance_request([r.to_prism_json() for r in fixture.recenter_delta], anchor=fixture.recenter_delta_anchor, base_digest=recenter.digest or ""),
                            len(fixture.recenter_delta),
                            base_items=items_recenter,
                        )
                        phases.append(recenter_advance)
            except Exception as exc:  # noqa: BLE001 - reported per run, not raised
                run_error = f"{type(exc).__name__}: {exc}"
                log(f"    {label} @ {fixture.size}: aborted: {run_error}")
            finally:
                max_sampled = sampler.max_sampled_mb
                timeline = sampler.samples
    finally:
        # Always reaps the child and closes every pipe and the stderr
        # capture, whatever happened above.
        outcome = daemon.close()
    if outcome.get("peak_rss_mb") is None:
        candidates = [phase.hwm_after_mb or 0.0 for phase in phases] + [max_sampled]
        outcome["peak_rss_mb"] = max(candidates) if any(candidates) else None
    return RunResult(
        label,
        str(binary),
        fixture.size,
        fixture.page_size,
        available,
        load,
        memory_limit_mb,
        phases,
        outcome,
        max_sampled,
        timeline,
        error=run_error,
    )


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------


def _fmt(value: float | None, digits: int = 2) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def _flag(value: bool | None) -> str:
    if value is None:
        return "n/a"
    return "yes" if value else "NO"


def render_runs(runs: list[RunResult]) -> str:
    lines = [
        "| binary | shares | phase | records | request MiB | write s | wait s | read s | parse s | fold s | serialize s | residual s (approx.) | RSS after MiB | status | self-check | oracle |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for run in runs:
        if run.skipped or (run.error and not run.phases):
            lines.append(f"| {run.variant} | {run.size:,} | {'skipped' if run.skipped else 'failed'} | | | | | | | | | | | {run.skipped or run.error} | | |")
            continue
        for phase in run.phases:
            metrics = phase.daemon_metrics
            lines.append(
                f"| {run.variant} | {run.size:,} | {phase.phase} | {phase.records_sent:,} |"
                f" {phase.request_bytes / 2**20:.1f} | {_fmt(phase.request_write_seconds)} |"
                f" {_fmt(phase.response_wait_seconds)} | {_fmt(phase.response_read_seconds)} |"
                f" {_fmt(metrics.get('input_deserialization_seconds'), 3)} |"
                f" {_fmt(metrics.get('fold_seconds'), 3)} |"
                f" {_fmt(metrics.get('output_serialization_seconds'), 3)} |"
                f" {_fmt(phase.residual_wait_seconds, 3)} |"
                f" {_fmt(phase.rss_after_mb, 0)} | {phase.status}"
                f"{' (' + phase.error + ')' if phase.error else ''} |"
                f" {_flag(phase.self_consistent)} | {_flag(phase.oracle_match)} |"
            )
    return "\n".join(lines)


def render_summary(runs: list[RunResult]) -> str:
    lines = [
        "| binary | shares | peak RSS MiB (VmHWM) | peak VSZ MiB | MiB per 1k shares | full wait s | full fold s | daemon exit | available MiB before |",
        "|---|---:|---:|---:|---:|---:|---:|---|---:|",
    ]
    for run in runs:
        if run.skipped or (run.error and not run.phases):
            lines.append(f"| {run.variant} | {run.size:,} | {'skipped' if run.skipped else 'failed'}: {run.skipped or run.error} | | | | | | {_fmt(run.mem_available_before_mb, 0)} |")
            continue
        full = next((phase for phase in run.phases if phase.phase == "full"), None)
        peak = run.outcome.get("peak_rss_mb")
        exit_code = run.outcome.get("exit_code")
        signal = run.outcome.get("signal")
        ended = "clean (0)" if exit_code == 0 else (f"signal {signal}" if signal else f"exit {exit_code}")
        if run.outcome.get("timed_out"):
            ended += f", killed on {run.outcome['timed_out']} timeout"
        lines.append(
            f"| {run.variant} | {run.size:,} | {_fmt(peak, 0)} | {_fmt(run.outcome.get('peak_vsize_mb'), 0)} |"
            f" {_fmt(None if peak is None else peak / (run.size / 1000.0), 2)} |"
            f" {_fmt(full.response_wait_seconds if full else None)} |"
            f" {_fmt(full.daemon_metrics.get('fold_seconds') if full else None, 3)} |"
            f" {ended} | {_fmt(run.mem_available_before_mb, 0)} |"
        )
    return "\n".join(lines)


def parse_sizes(text: str) -> list[int]:
    return [int(float(item)) for item in text.split(",") if item.strip()]


def resolve_binary(text: str | None, *, default: Path | None) -> Path | None:
    if text:
        candidate = Path(text)
    elif default is not None:
        candidate = default
    else:
        return None
    if candidate.is_dir():
        candidate = candidate / DAEMON_BINARY
    if not (candidate.is_file() and os.access(candidate, os.X_OK)):
        raise SystemExit(f"{candidate} is not an executable daemon binary")
    return candidate.resolve()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--daemon-binary",
        default=os.environ.get("PRISM_TOOL_BIN_DIR") or str(DEFAULT_TOOL_BIN_DIR),
        help="the fixed qbit-prism-build-audit-bundle (a file, or a directory holding it; default $PRISM_TOOL_BIN_DIR or target/release)",
    )
    parser.add_argument("--daemon-label", default="fixed", help="row label for --daemon-binary (default fixed)")
    parser.add_argument("--baseline-binary", default=None, help="an unfixed daemon binary to measure at --baseline-sizes only")
    parser.add_argument("--baseline-label", default="baseline", help="row label for --baseline-binary")
    parser.add_argument("--sizes", default=DEFAULT_SIZES, help=f"window sizes for the fixed binary (default {DEFAULT_SIZES})")
    parser.add_argument("--baseline-sizes", default=DEFAULT_BASELINE_SIZES, help=f"window sizes for the baseline binary (default {DEFAULT_BASELINE_SIZES}; keep these inside the memory limit)")
    parser.add_argument("--page-size", type=int, default=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE)
    parser.add_argument("--miners", type=int, default=DEFAULT_MINERS, help=f"distinct identities in the fixture (default {DEFAULT_MINERS})")
    parser.add_argument("--small-delta", type=int, default=DEFAULT_SMALL_DELTA, help=f"records in the small advance (default {DEFAULT_SMALL_DELTA})")
    parser.add_argument("--large-delta", type=int, default=DEFAULT_LARGE_DELTA, help=f"records in the multi-page advance (default {DEFAULT_LARGE_DELTA})")
    parser.add_argument("--daemon-memory-limit-mb", type=int, default=6144, help="RLIMIT_AS for every daemon in MiB; 0 disables (default 6144)")
    parser.add_argument("--memory-margin-mb", type=int, default=2048, help="MemAvailable headroom required above the limit before a run (default 2048)")
    parser.add_argument("--sample-interval", type=float, default=0.02, help="RSS sampling interval in seconds (default 0.02)")
    parser.add_argument("--exchange-timeout", type=float, default=DEFAULT_EXCHANGE_TIMEOUT, help=f"wall-clock deadline per exchange, handshake included; the daemon is killed and the phase recorded as a timeout (default {DEFAULT_EXCHANGE_TIMEOUT:g})")
    parser.add_argument("--skip-oracle", action="store_true", help="do not run the Python fold as a digest oracle")
    parser.add_argument("--stderr-dir", default=None, help="directory for daemon stderr captures (default: a temporary directory)")
    parser.add_argument("--json", default=None, help="write structured results to this file")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    def log(text: str) -> None:
        if not args.quiet:
            print(text, flush=True)

    fixed = resolve_binary(args.daemon_binary, default=DEFAULT_TOOL_BIN_DIR)
    baseline = resolve_binary(args.baseline_binary, default=None)
    assert fixed is not None
    memory_limit = args.daemon_memory_limit_mb or None
    stderr_dir = Path(args.stderr_dir) if args.stderr_dir else Path(
        os.environ.get("TMPDIR", "/tmp")
    ) / f"window_paging_allocation-{os.getpid()}"
    stderr_dir.mkdir(parents=True, exist_ok=True)

    info = machine_info()
    source = git_describe(REPO_ROOT)
    binaries = {args.daemon_label: binary_identity(fixed)}
    if baseline is not None:
        binaries[args.baseline_label] = binary_identity(baseline)

    print("# window_paging_allocation")
    print()
    for key, value in info.items():
        print(f"- {key}: {value}")
    print(f"- source: {source}")
    for label, identity in binaries.items():
        print(f"- {label}: {identity['path']} (sha256 {identity['sha256'][:16]}..., {identity['mtime']})")
    print(f"- page_size: {args.page_size}; miners: {args.miners}; deltas: {args.small_delta} / {args.large_delta}")
    print(f"- daemon RLIMIT_AS: {memory_limit or 'none'} MiB; required MemAvailable: {(memory_limit or 0) + args.memory_margin_mb} MiB; exchange timeout: {args.exchange_timeout:g} s")
    print()

    plan: list[tuple[str, Path, int]] = []
    sizes = parse_sizes(args.sizes)
    baseline_sizes = parse_sizes(args.baseline_sizes) if baseline is not None else []
    for size in sorted(set(sizes) | set(baseline_sizes)):
        if baseline is not None and size in baseline_sizes:
            plan.append((args.baseline_label, baseline, size))
        if size in sizes:
            plan.append((args.daemon_label, fixed, size))

    runs: list[RunResult] = []
    oracles: dict[int, dict[str, Any]] = {}
    fixtures_seconds: dict[int, float] = {}
    current_size: int | None = None
    fixture: Fixture | None = None
    oracle: dict[str, Any] | None = None
    for label, binary, size in plan:
        if size != current_size:
            fixture = None
            log(f"  building {size:,}-record fixture ...")
            fixture = build_fixture(size, miners=args.miners, page_size=args.page_size, small=args.small_delta, large=args.large_delta)
            fixtures_seconds[size] = fixture.build_seconds
            oracle = None
            if not args.skip_oracle:
                log(f"  running the Python fold oracle at {size:,} ...")
                oracle = python_oracle(fixture)
                oracles[size] = oracle
            current_size = size
        assert fixture is not None
        runs.append(
            run_variant(
                label,
                binary,
                fixture,
                oracle=oracle,
                memory_limit_mb=memory_limit,
                memory_margin_mb=args.memory_margin_mb,
                sample_interval=args.sample_interval,
                exchange_timeout=args.exchange_timeout,
                stderr_dir=stderr_dir,
                log=log,
            )
        )
        last = runs[-1]
        if not last.skipped and last.phases:
            log(
                f"    -> peak RSS {_fmt(last.outcome.get('peak_rss_mb'), 0)} MiB, exit {last.outcome.get('exit_code')},"
                f" phases {[(p.phase, p.status) for p in last.phases]}"
            )

    print("## Summary per daemon")
    print()
    print(render_summary(runs))
    print()
    print("## Phases")
    print()
    print(render_runs(runs))
    print()
    if oracles:
        print("## Python oracle (in-process fold on the same inputs)")
        print()
        print("| shares | phase | seconds | records | digest |")
        print("|---:|---|---:|---:|---|")
        for size, oracle in sorted(oracles.items()):
            for phase in PHASES:
                print(f"| {size:,} | {phase} | {oracle['seconds'][phase]:.2f} | {oracle['record_counts'][phase]:,} | {oracle['digests'][phase][:16]}... |")
        print()

    if args.json:
        output = {
            "machine": info,
            "source": source,
            "binaries": binaries,
            "arguments": vars(args),
            "fixture_build_seconds": fixtures_seconds,
            "oracles": oracles,
            "runs": [
                {
                    **{key: value for key, value in asdict(run).items() if key != "rss_timeline"},
                    "phases": [
                        {**asdict(phase), "residual_wait_seconds": phase.residual_wait_seconds}
                        for phase in run.phases
                    ],
                    "rss_timeline_mb": [
                        [round(t - run.rss_timeline[0][0], 3), round(rss, 1)] for t, rss in run.rss_timeline
                    ] if run.rss_timeline else [],
                }
                for run in runs
            ],
        }
        Path(args.json).write_text(json.dumps(output, indent=2, default=str) + "\n")
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
