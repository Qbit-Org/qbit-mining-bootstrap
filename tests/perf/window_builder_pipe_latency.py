#!/usr/bin/env python3
"""Builder pipe transport latency: fixed polling sleeps vs readiness waits.

Issue #236's cold window builds spent 47.5-48.8 s of a 54-56 s materialization
inside ``daemon_prepare``. That timer covers lock admission, request encoding,
the pipe transport in both directions, the Rust fold, and response handling;
it does not say how much of it is transport. The shipped transport helpers in
``lab/prism/bundle_compiler.py`` slept a fixed 20 ms after every EAGAIN, once
per drained pipe capacity, so the suspicion was that a multi-megabyte
exchange paid far more in idle sleeps than in copying. A synthetic macOS probe
(146 ms vs 2 ms for a 4 MiB read) motivated the change; it is not production
evidence, and this driver exists to produce the Linux numbers.

Two sections, both running the **shipped callables** of two revisions of the
transport module side by side in one process:

1. **Raw transport** over real pipes against a Python peer subprocess:
   ``_serve_builder_read_exact``, ``_serve_builder_read_line``,
   ``_serve_builder_write`` and (Linux) ``_serve_builder_splice_spool``.
   Reported per helper and payload size: wall seconds (median / min over
   ``--reps``), throughput, **blocked waits** (readiness waits for the
   patched module, 20 ms sleeps for the baseline), bytes, and a byte-exact
   digest check against the peer.
2. **Real daemon** (``qbit-prism-build-audit-bundle --serve``, prebuilt via
   ``--tool-bin-dir`` / ``PRISM_TOOL_BIN_DIR``): one cold ``prepare_window``
   ``full`` round trip through ``BundleCompiler.prepare_payout_window`` at
   each ``--daemon-sizes`` window, attributed by wrapping the compiler's own
   transport methods:

   ===================  =======================================================
   phase                what the wall time contains
   ===================  =======================================================
   ``spawn``            daemon spawn + protocol handshake (line read)
   ``request_encode``   total minus every wrapped phase: unattributed
                        coordinator time outside the transport calls,
                        dominated by (not isolated to) the whole-request
                        ``json.dumps`` PR 2 targets
   ``request_write``    ``_serve_builder_write`` of the request line: elapsed
                        until the pipe accepted every byte -- not proof the
                        daemon consumed them; up to one pipe capacity may
                        still be unread
   ``response_wait``    ``_serve_builder_read_line`` of the envelope: elapsed
                        from write completion to envelope receipt -- daemon
                        scheduling and processing (remaining parse, fold,
                        canonical serialization) plus response readiness and
                        receipt; wall time, NOT measured Rust CPU. Splitting
                        it needs Rust-side profiling.
   ``response_read``    ``_serve_builder_read_exact`` of the raw canonical
                        items section + its newline: elapsed receipt of the
                        bytes the daemon produces after the envelope
   ===================  =======================================================

   Both revisions must return the same ``share_snapshot_sha256`` and the same
   canonical item bytes; a mismatch is reported as a correctness failure.

The baseline module is extracted with ``git show REV:lab/prism/bundle_compiler.py``
and imported under a private name; it resolves its own imports from the
current tree, which is fine for the transport code because the helpers only
depend on the module-level constants beside them. Wait counting hooks the
patched module's ``_PipeReadinessWaiter`` and the baseline's ``time.sleep``;
every peer is a subprocess so nothing else in the process sleeps during a
measurement.

Self-contained: no database, no network, no coordinator, standard library
only. ``python3 tests/perf/window_builder_pipe_latency.py --baseline-rev
e59bda34016f6dc0a6bc39f9b879b8df64f51f60`` prints the tables; ``--json FILE``
also captures the structured results.

This is an **on-demand instrument, not a test**: it asserts no thresholds and
is deliberately not named ``test_*`` so the discovery run never executes it.
Nothing under ``lab/`` is modified.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import importlib.util
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterator

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism.share_ledger import (  # noqa: E402 - path fix-up above
    DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
    AcceptedShareRecord,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPILER_RELATIVE_PATH = "lab/prism/bundle_compiler.py"
DEFAULT_TOOL_BIN_DIR = REPO_ROOT / "target" / "release"
DAEMON_BINARY = "qbit-prism-build-audit-bundle"
FIXED_NOW_MS = 1_760_000_000_000
F_GETPIPE_SZ = 1032


# ---------------------------------------------------------------------------
# module loading and wait counting
# ---------------------------------------------------------------------------


def load_compiler(label: str, rev: str | None) -> tuple[ModuleType, str]:
    """The transport module for one revision (None = the working tree)."""
    if rev is None:
        import lab.prism.bundle_compiler as module

        head = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "status", "--porcelain", COMPILER_RELATIVE_PATH],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        ).stdout.strip()
        return module, f"{head or 'unknown'}{' (working tree, modified)' if dirty else ''}"
    resolved = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", rev],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    source = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "show", f"{resolved}:{COMPILER_RELATIVE_PATH}"],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    directory = tempfile.mkdtemp(prefix="prism-236-transport-")
    path = Path(directory) / f"bundle_compiler_{label}.py"
    path.write_bytes(source)
    name = f"prism_bundle_compiler_{label}"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module, resolved


class WaitCounter:
    def __init__(self) -> None:
        self.count = 0


@contextlib.contextmanager
def counting_blocked_waits(module: ModuleType) -> Iterator[WaitCounter]:
    """Count blocked waits taken by ``module``'s transport loops.

    The patched module waits through ``_PipeReadinessWaiter.wait``; the
    baseline sleeps through ``time.sleep``, which its loops resolve at call
    time, so a process-global hook counts exactly those sleeps as long as
    nothing else in the process sleeps meanwhile (every peer is a
    subprocess).
    """
    counter = WaitCounter()
    waiter_class = getattr(module, "_PipeReadinessWaiter", None)
    if waiter_class is not None:

        class Counting(waiter_class):  # type: ignore[misc,valid-type]
            def wait(self, deadline: float) -> bool:
                counter.count += 1
                return super().wait(deadline)

        module._PipeReadinessWaiter = Counting
        try:
            yield counter
        finally:
            module._PipeReadinessWaiter = waiter_class
        return
    real_sleep = time.sleep

    def sleep(seconds: float) -> None:
        counter.count += 1
        real_sleep(seconds)

    time.sleep = sleep
    try:
        yield counter
    finally:
        time.sleep = real_sleep


class BenchRuntime:
    """The BundleCompilerRuntime port, reduced to counters and no-ops."""

    signing_seed_hex = "42" * 32
    ledger_attestation_signing_seed_hex = "43" * 32

    def __init__(self, timeout_seconds: float) -> None:
        self.bundle_build_timeout_seconds = timeout_seconds
        self._job_build_scheduler_lock = threading.Lock()
        self._tip_refresh_metrics_lock = threading.Lock()
        self.job_build_worker_counts = {
            "starts": 0,
            "restarts": 0,
            "crashes": 0,
            "terminations": 0,
        }
        self._job_build_worker_restart_pending = False
        self.tip_refresh_worker_restarts = 0
        self.tip_refresh_worker_failures = 0
        self._phases: dict[str, float] = {}

    def _ensure_job_cache_state(self) -> None:
        pass

    def _ensure_tip_refresh_state(self) -> None:
        pass

    def _job_build_checkpoint(self, phase: str, cancellation: Any) -> None:
        pass

    def _job_build_phases(self) -> dict[str, float]:
        return self._phases

    def prism_payout_policy(self) -> dict[str, object]:
        return {}

    def prism_ctv_settlement_config(self, **_kwargs: Any) -> None:
        return None

    def _observe_tip_refresh_build_phase(self, name: str, elapsed: float) -> None:
        pass

    def _record_tip_refresh_ipc_bytes(self, direction: str, byte_count: int) -> None:
        pass


class _Control:
    """Never instantiated; only its type is consulted."""


class _Serialization:
    def __init__(self) -> None:
        self.spool_failures = 0

    def mark_spool_failed(self) -> None:
        self.spool_failures += 1


def make_compiler(module: ModuleType, runtime: BenchRuntime, tool_bin_dir: Path | None) -> Any:
    tool_command = None
    if tool_bin_dir is not None:

        def tool_command(bin_name: str) -> list[str]:
            return [str(tool_bin_dir / bin_name)]

    return module.BundleCompiler(
        runtime,
        superseded_error=RuntimeError,
        cancellation_error_types=(RuntimeError,),
        build_control_type=_Control,
        tool_command=tool_command,
    )


# ---------------------------------------------------------------------------
# section 1: raw transport against a peer subprocess
# ---------------------------------------------------------------------------


# Writes `total` bytes to stdout in `chunk`-sized blocks (a single line when
# the mode is "line"), pausing `pause` seconds after each block, after a go
# byte arrives on stdin; the digest of what was written goes to stderr.
PEER_WRITE_STDOUT = r"""
import hashlib, os, sys, time
total = int(sys.argv[1]); chunk = int(sys.argv[2]); pause = float(sys.argv[3])
line = sys.argv[4] == "line"
os.read(0, 1)
digest = hashlib.sha256()
remaining = total
while remaining:
    n = min(chunk, remaining)
    block = os.urandom(n)
    if line:
        block = block.replace(b"\n", b"\x00")
    digest.update(block)
    view = memoryview(block)
    while view:
        written = os.write(1, view)
        view = view[written:]
    remaining -= n
    if pause:
        time.sleep(pause)
if line:
    os.write(1, b"\n")
sys.stderr.write(digest.hexdigest() + "\n")
sys.stderr.flush()
"""

# Reads stdin to EOF in `chunk`-sized reads, pausing `pause` seconds after
# each, then reports "<bytes> <digest>" on stdout.
PEER_READ_STDIN = r"""
import hashlib, os, sys, time
chunk = int(sys.argv[1]); pause = float(sys.argv[2])
digest = hashlib.sha256(); total = 0
while True:
    data = os.read(0, chunk)
    if not data:
        break
    digest.update(data); total += len(data)
    if pause:
        time.sleep(pause)
sys.stdout.write(f"{total} {digest.hexdigest()}\n")
sys.stdout.flush()
"""


@dataclass
class TransportSample:
    seconds: float
    waits: int
    bytes: int
    ok: bool


def _far_deadline() -> float:
    return time.monotonic() + 600.0


def run_read_exact(module: ModuleType, size: int, peer_chunk: int, peer_pause: float) -> TransportSample:
    peer = subprocess.Popen(
        [sys.executable, "-c", PEER_WRITE_STDOUT, str(size), str(peer_chunk), str(peer_pause), "raw"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert peer.stdin is not None and peer.stdout is not None
    os.set_blocking(peer.stdout.fileno(), False)
    client = module._ServeBuilderClient(process=peer)
    compiler = make_compiler(module, BenchRuntime(600.0), None)
    with counting_blocked_waits(module) as counter:
        peer.stdin.write(b"g")
        peer.stdin.flush()
        started = time.perf_counter()
        data = compiler._serve_builder_read_exact(client, size, _far_deadline(), None, None)
        elapsed = time.perf_counter() - started
    ok = len(data) == size and hashlib.sha256(data).hexdigest() == _writer_digest(peer)
    return TransportSample(elapsed, counter.count, size, ok)


def run_read_line(module: ModuleType, size: int, peer_chunk: int, peer_pause: float) -> TransportSample:
    peer = subprocess.Popen(
        [sys.executable, "-c", PEER_WRITE_STDOUT, str(size), str(peer_chunk), str(peer_pause), "line"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert peer.stdin is not None and peer.stdout is not None
    os.set_blocking(peer.stdout.fileno(), False)
    client = module._ServeBuilderClient(process=peer)
    compiler = make_compiler(module, BenchRuntime(600.0), None)
    with counting_blocked_waits(module) as counter:
        peer.stdin.write(b"g")
        peer.stdin.flush()
        started = time.perf_counter()
        line = compiler._serve_builder_read_line(client, _far_deadline(), None, None)
        elapsed = time.perf_counter() - started
    ok = len(line) == size and hashlib.sha256(line).hexdigest() == _writer_digest(peer)
    return TransportSample(elapsed, counter.count, size + 1, ok)


def _spawn_reader(peer_chunk: int, peer_pause: float) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-c", PEER_READ_STDIN, str(peer_chunk), str(peer_pause)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _reader_result(peer: subprocess.Popen[bytes]) -> tuple[int, str]:
    """The reader peer's "<bytes> <digest>" report, after its stdin closed."""
    assert peer.stdin is not None and peer.stdout is not None and peer.stderr is not None
    peer.stdin.close()
    stdout = peer.stdout.read()
    stderr = peer.stderr.read()
    peer.wait(timeout=120)
    if peer.returncode != 0:
        raise RuntimeError(f"reader peer failed: {stderr.decode(errors='replace')}")
    total, digest = stdout.decode().split()
    return int(total), digest


def _writer_digest(peer: subprocess.Popen[bytes]) -> str:
    """The writer peer's digest line, after its payload was fully read."""
    assert peer.stdin is not None and peer.stderr is not None
    peer.stdin.close()
    stderr = peer.stderr.read()
    peer.wait(timeout=120)
    if peer.returncode != 0:
        raise RuntimeError(f"writer peer failed: {stderr.decode(errors='replace')}")
    return stderr.decode().strip()


def run_write(module: ModuleType, size: int, peer_chunk: int, peer_pause: float) -> TransportSample:
    payload = os.urandom(size)
    expected = hashlib.sha256(payload).hexdigest()
    peer = _spawn_reader(peer_chunk, peer_pause)
    assert peer.stdin is not None
    os.set_blocking(peer.stdin.fileno(), False)
    client = module._ServeBuilderClient(process=peer)
    compiler = make_compiler(module, BenchRuntime(600.0), None)
    with counting_blocked_waits(module) as counter:
        started = time.perf_counter()
        written = compiler._serve_builder_write(client, payload, _far_deadline(), None, None)
        elapsed = time.perf_counter() - started
    total, digest = _reader_result(peer)
    ok = written == size and total == size and digest == expected
    return TransportSample(elapsed, counter.count, size, ok)


def run_splice(module: ModuleType, size: int, peer_chunk: int, peer_pause: float) -> TransportSample:
    payload = os.urandom(size)
    expected = hashlib.sha256(payload).hexdigest()
    spool = tempfile.TemporaryFile()
    spool.write(payload)
    spool.flush()
    peer = _spawn_reader(peer_chunk, peer_pause)
    assert peer.stdin is not None
    os.set_blocking(peer.stdin.fileno(), False)
    client = module._ServeBuilderClient(process=peer)
    compiler = make_compiler(module, BenchRuntime(600.0), None)
    serialization = _Serialization()
    try:
        with counting_blocked_waits(module) as counter:
            started = time.perf_counter()
            moved = compiler._serve_builder_splice_spool(
                client, serialization, spool, size, _far_deadline(), None, None
            )
            elapsed = time.perf_counter() - started
    finally:
        spool.close()
    total, digest = _reader_result(peer)
    ok = moved == size and total == size and digest == expected and serialization.spool_failures == 0
    return TransportSample(elapsed, counter.count, size, ok)


RAW_HELPERS: dict[str, Callable[[ModuleType, int, int, float], TransportSample]] = {
    "read_exact": run_read_exact,
    "read_line": run_read_line,
    "write": run_write,
}
if hasattr(os, "splice"):
    RAW_HELPERS["splice"] = run_splice


@dataclass
class RawResult:
    helper: str
    variant: str
    bytes: int
    reps: int
    median_seconds: float
    min_seconds: float
    max_seconds: float
    median_waits: float
    mib_per_second: float
    ok: bool
    samples: list[dict[str, Any]] = field(default_factory=list)


def run_raw_section(
    variants: list[tuple[str, ModuleType]],
    sizes: list[int],
    reps: int,
    peer_chunk: int,
    peer_pause: float,
    log: Callable[[str], None],
) -> list[RawResult]:
    results: list[RawResult] = []
    for helper, runner in RAW_HELPERS.items():
        for size in sizes:
            samples: dict[str, list[TransportSample]] = {label: [] for label, _ in variants}
            # Alternate variants per repetition so host drift lands on both.
            for rep in range(reps):
                for label, module in variants:
                    sample = runner(module, size, peer_chunk, peer_pause)
                    samples[label].append(sample)
                    log(
                        f"  {helper:10s} {size / (1 << 20):8.2f} MiB {label:9s} rep {rep + 1}:"
                        f" {sample.seconds * 1000:9.2f} ms, {sample.waits:6d} waits,"
                        f" {'ok' if sample.ok else 'MISMATCH'}"
                    )
            for label, _module in variants:
                seconds = [s.seconds for s in samples[label]]
                median = statistics.median(seconds)
                results.append(
                    RawResult(
                        helper=helper,
                        variant=label,
                        bytes=size,
                        reps=reps,
                        median_seconds=median,
                        min_seconds=min(seconds),
                        max_seconds=max(seconds),
                        median_waits=statistics.median(s.waits for s in samples[label]),
                        mib_per_second=(size / (1 << 20)) / median if median > 0 else float("inf"),
                        ok=all(s.ok for s in samples[label]),
                        samples=[asdict(s) for s in samples[label]],
                    )
                )
    return results


# ---------------------------------------------------------------------------
# section 2: the real daemon's cold prepare_window round trip
# ---------------------------------------------------------------------------


def build_records(count: int, *, miners: int, rigs_per_miner: int) -> list[AcceptedShareRecord]:
    """Records shaped like the ledger's rows (production share_id form)."""
    programs = [hashlib.sha256(f"bench-miner-{index}".encode()).hexdigest() for index in range(miners)]
    records: list[AcceptedShareRecord] = []
    for index in range(count):
        miner = index % miners
        block_hash = hashlib.sha256(f"{index}".encode()).hexdigest()
        records.append(
            AcceptedShareRecord(
                share_seq=index + 1,
                share_id=f"bench-miner-{miner}.rig{index % rigs_per_miner}:{block_hash}",
                miner_id=f"bench-miner-{miner}",
                order_key=f"bench-miner-{miner:06d}",
                p2mr_program_hex=programs[miner],
                share_difficulty=16384,
                network_difficulty=226646186,
                template_height=9,
                job_id=f"bench-job-{index + 1}",
                job_issued_at_ms=FIXED_NOW_MS - (count - index) * 1_000,
                accepted_at_ms=FIXED_NOW_MS - (count - index) * 1_000,
                ntime=FIXED_NOW_MS // 1000 - (count - index),
            )
        )
    return records


@dataclass
class PhaseSample:
    seconds: float = 0.0
    bytes: int = 0
    waits: int = 0
    calls: int = 0


@dataclass
class DaemonSample:
    variant: str
    records: int
    status: str
    total_seconds: float
    phases: dict[str, PhaseSample]
    request_bytes: int
    response_bytes: int
    digest: str | None
    items_sha256: str | None
    record_count: int


def run_daemon_prepare(
    module: ModuleType,
    label: str,
    records_json: list[dict[str, object]],
    *,
    anchor_job_issued_at_ms: int,
    window_weight: int,
    tool_bin_dir: Path,
    timeout_seconds: float,
) -> DaemonSample:
    runtime = BenchRuntime(timeout_seconds)
    compiler = make_compiler(module, runtime, tool_bin_dir)
    phases: dict[str, PhaseSample] = {
        name: PhaseSample()
        for name in ("spawn", "request_encode", "request_write", "response_wait", "response_read")
    }
    counter_holder: list[WaitCounter] = []
    state = {"in_spawn": False}

    def timed(name: str, method: Callable[..., Any], byte_count: Callable[[tuple, Any], int]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if state["in_spawn"] and name != "spawn":
                # The handshake line read runs inside the spawn wrapper and
                # is already attributed to spawn.
                return method(*args, **kwargs)
            counter = counter_holder[0]
            before = counter.count
            started = time.perf_counter()
            try:
                result = method(*args, **kwargs)
            finally:
                phase = phases[name]
                phase.seconds += time.perf_counter() - started
                phase.waits += counter.count - before
                phase.calls += 1
            phase.bytes += byte_count(args, result)
            return result

        return wrapper

    def spawn_wrapper(*args: Any, **kwargs: Any) -> Any:
        state["in_spawn"] = True
        try:
            return timed_spawn(*args, **kwargs)
        finally:
            state["in_spawn"] = False

    timed_spawn = timed("spawn", compiler._spawn_serve_builder_locked, lambda args, result: 0)
    compiler._spawn_serve_builder_locked = spawn_wrapper
    compiler._serve_builder_write = timed(
        "request_write", compiler._serve_builder_write, lambda args, result: int(result)
    )
    compiler._serve_builder_read_line = timed(
        "response_wait", compiler._serve_builder_read_line, lambda args, result: len(result) + 1
    )
    compiler._serve_builder_read_exact = timed(
        "response_read", compiler._serve_builder_read_exact, lambda args, result: len(result)
    )
    outcome = None
    error: str | None = None
    with counting_blocked_waits(module) as counter:
        counter_holder.append(counter)
        started = time.perf_counter()
        try:
            outcome = compiler.prepare_payout_window(
                mode="full",
                records_json=records_json,
                anchor_job_issued_at_ms=anchor_job_issued_at_ms,
                append_invalidation_epoch=0,
                window_weight=window_weight,
                page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
            )
        except Exception as exc:  # noqa: BLE001 - reported, not raised
            error = f"{type(exc).__name__}: {exc}"
        total = time.perf_counter() - started
    compiler.shutdown_serve_builder()
    measured = sum(
        phases[name].seconds for name in ("spawn", "request_write", "response_wait", "response_read")
    )
    phases["request_encode"].seconds = max(0.0, total - measured)
    phases["request_encode"].calls = 1
    if error is not None:
        status = f"error: {error}"
    elif outcome is None:
        status = "none (daemon anomaly, fell back)"
    else:
        status = str(outcome.status)
    items = getattr(outcome, "window_items", None) if outcome is not None else None
    return DaemonSample(
        variant=label,
        records=len(records_json),
        status=status,
        total_seconds=total,
        phases=phases,
        request_bytes=phases["request_write"].bytes,
        response_bytes=phases["response_wait"].bytes + phases["response_read"].bytes,
        digest=getattr(outcome, "share_snapshot_sha256", None) if outcome is not None else None,
        items_sha256=hashlib.sha256(items).hexdigest() if items is not None else None,
        record_count=int(getattr(outcome, "record_count", 0) or 0) if outcome is not None else 0,
    )


def run_daemon_section(
    variants: list[tuple[str, ModuleType]],
    sizes: list[int],
    reps: int,
    *,
    miners: int,
    rigs_per_miner: int,
    tool_bin_dir: Path,
    timeout_seconds: float,
    log: Callable[[str], None],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for size in sizes:
        log(f"  building {size} records ...")
        records = build_records(size, miners=miners, rigs_per_miner=rigs_per_miner)
        anchor = int(records[-1].job_issued_at_ms)
        weight = sum(int(record.share_difficulty) for record in records)
        records_json = [record.to_prism_json() for record in records]
        samples: dict[str, list[DaemonSample]] = {label: [] for label, _ in variants}
        for rep in range(reps):
            for label, module in variants:
                sample = run_daemon_prepare(
                    module,
                    label,
                    records_json,
                    anchor_job_issued_at_ms=anchor,
                    window_weight=weight,
                    tool_bin_dir=tool_bin_dir,
                    timeout_seconds=timeout_seconds,
                )
                samples[label].append(sample)
                phase_text = ", ".join(
                    f"{name} {phase.seconds:.3f}s/{phase.waits}w"
                    for name, phase in sample.phases.items()
                )
                log(
                    f"  daemon {size:7d} {label:9s} rep {rep + 1}: {sample.status},"
                    f" total {sample.total_seconds:.3f}s [{phase_text}]"
                )
        digests = {s.digest for label in samples for s in samples[label]}
        items = {s.items_sha256 for label in samples for s in samples[label]}
        parity = len(digests) == 1 and None not in digests and len(items) == 1 and None not in items
        for label, _module in variants:
            runs = samples[label]
            by_total = sorted(runs, key=lambda s: s.total_seconds)
            median_run = by_total[len(by_total) // 2]
            results.append(
                {
                    "records": size,
                    "variant": label,
                    "reps": reps,
                    "statuses": [s.status for s in runs],
                    "parity_across_variants": parity,
                    "digest": median_run.digest,
                    "record_count": median_run.record_count,
                    "request_bytes": median_run.request_bytes,
                    "response_bytes": median_run.response_bytes,
                    "median_total_seconds": statistics.median(s.total_seconds for s in runs),
                    "min_total_seconds": min(s.total_seconds for s in runs),
                    "median_run_phases": {
                        name: asdict(phase) for name, phase in median_run.phases.items()
                    },
                    "runs": [
                        {
                            "status": s.status,
                            "total_seconds": s.total_seconds,
                            "phases": {name: asdict(phase) for name, phase in s.phases.items()},
                        }
                        for s in runs
                    ],
                }
            )
    return results


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------


def machine_info() -> dict[str, Any]:
    read_end, write_end = os.pipe()
    try:
        pipe_capacity = fcntl.fcntl(read_end, F_GETPIPE_SZ)
    except OSError:
        pipe_capacity = None
    finally:
        os.close(read_end)
        os.close(write_end)
    try:
        load = os.getloadavg()
    except OSError:
        load = None
    return {
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
        "cpu_count": os.cpu_count(),
        "pipe_capacity_bytes": pipe_capacity,
        "load_average": load,
        "splice": hasattr(os, "splice"),
    }


def render_raw(results: list[RawResult]) -> str:
    lines = [
        "| helper | bytes | variant | median ms | min ms | max ms | median waits | MiB/s | exact |",
        "|---|---:|---|---:|---:|---:|---:|---:|---|",
    ]
    for result in results:
        lines.append(
            f"| {result.helper} | {result.bytes:,} | {result.variant} |"
            f" {result.median_seconds * 1000:.2f} | {result.min_seconds * 1000:.2f} |"
            f" {result.max_seconds * 1000:.2f} | {result.median_waits:.0f} |"
            f" {result.mib_per_second:.1f} | {'yes' if result.ok else 'NO'} |"
        )
    return "\n".join(lines)


def render_daemon(results: list[dict[str, Any]]) -> str:
    lines = [
        "| records | variant | status | total s | spawn s | encode s | write s (waits) | wait s (waits) | read s (waits) | req MiB | resp MiB | parity |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for result in results:
        phases = result["median_run_phases"]

        def cell(name: str) -> str:
            phase = phases[name]
            return f"{phase['seconds']:.3f} ({phase['waits']})"

        lines.append(
            f"| {result['records']:,} | {result['variant']} | {result['statuses'][-1]} |"
            f" {result['median_total_seconds']:.3f} | {phases['spawn']['seconds']:.3f} |"
            f" {phases['request_encode']['seconds']:.3f} | {cell('request_write')} |"
            f" {cell('response_wait')} | {cell('response_read')} |"
            f" {result['request_bytes'] / (1 << 20):.1f} | {result['response_bytes'] / (1 << 20):.1f} |"
            f" {'yes' if result['parity_across_variants'] else 'NO'} |"
        )
    return "\n".join(lines)


def parse_sizes(text: str) -> list[int]:
    return [int(float(item)) for item in text.split(",") if item.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--baseline-rev",
        default=None,
        help="git revision whose lab/prism/bundle_compiler.py is measured beside the working tree",
    )
    parser.add_argument(
        "--sizes-mib",
        default="1,4,16",
        help="raw transport payload sizes in MiB (default 1,4,16)",
    )
    parser.add_argument("--reps", type=int, default=5, help="raw transport repetitions (default 5)")
    parser.add_argument(
        "--peer-chunk",
        type=int,
        default=1 << 16,
        help="peer subprocess write/read chunk in bytes (default 65536)",
    )
    parser.add_argument(
        "--peer-pause",
        type=float,
        default=0.0,
        help="seconds the peer pauses after each chunk (default 0: pipe-bound)",
    )
    parser.add_argument(
        "--daemon-sizes",
        default="52000,210000",
        help="window sizes for the real-daemon cold prepare (default 52000,210000)",
    )
    parser.add_argument("--daemon-reps", type=int, default=2, help="cold prepares per size (default 2)")
    parser.add_argument("--miners", type=int, default=64, help="distinct miners in the window (default 64)")
    parser.add_argument("--rigs-per-miner", type=int, default=8, help="rig suffixes per miner (default 8)")
    parser.add_argument(
        "--tool-bin-dir",
        default=os.environ.get("PRISM_TOOL_BIN_DIR") or str(DEFAULT_TOOL_BIN_DIR),
        help="directory holding a prebuilt qbit-prism-build-audit-bundle (default $PRISM_TOOL_BIN_DIR or target/release)",
    )
    parser.add_argument("--daemon-timeout", type=float, default=900.0, help="per-prepare budget in seconds")
    parser.add_argument("--skip-raw", action="store_true", help="skip the raw transport section")
    parser.add_argument("--skip-daemon", action="store_true", help="skip the real-daemon section")
    parser.add_argument("--json", default=None, help="write structured results to this file")
    parser.add_argument("--quiet", action="store_true", help="suppress per-repetition progress lines")
    args = parser.parse_args(argv)

    def log(text: str) -> None:
        if not args.quiet:
            print(text, flush=True)

    variants: list[tuple[str, ModuleType]] = []
    revisions: dict[str, str] = {}
    if args.baseline_rev:
        module, resolved = load_compiler("baseline", args.baseline_rev)
        variants.append(("baseline", module))
        revisions["baseline"] = resolved
    module, resolved = load_compiler("patched", None)
    variants.append(("patched", module))
    revisions["patched"] = resolved

    info = machine_info()
    print("# window_builder_pipe_latency")
    print()
    for key, value in info.items():
        print(f"- {key}: {value}")
    for label, resolved in revisions.items():
        print(f"- {label}: {resolved}")
    print()

    output: dict[str, Any] = {
        "machine": info,
        "revisions": revisions,
        "arguments": vars(args),
    }

    if not args.skip_raw:
        sizes = [int(size * (1 << 20)) for size in (float(s) for s in args.sizes_mib.split(","))]
        print(f"## Raw transport (peer chunk {args.peer_chunk} B, peer pause {args.peer_pause} s, {args.reps} reps)")
        print()
        raw = run_raw_section(variants, sizes, args.reps, args.peer_chunk, args.peer_pause, log)
        print()
        print(render_raw(raw))
        print()
        output["raw"] = [asdict(result) for result in raw]

    if not args.skip_daemon:
        tool_bin_dir = Path(args.tool_bin_dir)
        binary = tool_bin_dir / DAEMON_BINARY
        if not (binary.is_file() and os.access(binary, os.X_OK)):
            message = (
                f"real-daemon section skipped: {binary} is not an executable;"
                " build it with `cargo build --release -p qbit-prism --bin"
                f" {DAEMON_BINARY}` or point --tool-bin-dir / PRISM_TOOL_BIN_DIR at a build"
            )
            print(f"## Real daemon\n\n{message}\n")
            output["daemon_skipped"] = message
        else:
            sizes = parse_sizes(args.daemon_sizes)
            print(f"## Real daemon cold prepare_window ({binary}, {args.daemon_reps} reps)")
            print()
            daemon = run_daemon_section(
                variants,
                sizes,
                args.daemon_reps,
                miners=args.miners,
                rigs_per_miner=args.rigs_per_miner,
                tool_bin_dir=tool_bin_dir,
                timeout_seconds=args.daemon_timeout,
                log=log,
            )
            print()
            print(render_daemon(daemon))
            print()
            output["daemon"] = daemon

    if args.json:
        Path(args.json).write_text(json.dumps(output, indent=2, default=str) + "\n")
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
