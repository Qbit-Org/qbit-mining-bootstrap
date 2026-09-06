#!/usr/bin/env python3
"""GIL scaling of the PRISM payout-window materialization pipeline.

Issue #131 profiles one window materialization as ~135 ms of **GIL-held**
Python at today's 21,868-share window (~555 ms at 100k, ~2.2 s at 400k), split
across four terms: window fold, canonical-JSON digest, spool serialization,
and record->JSON conversion. "GIL-held" was *asserted* for this pipeline and
never measured on it -- the status comment #162 left on #143 names exactly
this gap: "GIL behaviour at megabyte buffer sizes is unmeasured."

It matters because #143 section 1 bracketed CPython's GIL-release threshold at
**1-2 KiB** and reached **7.89 cores across 8 threads** on a 1 MiB
``hashlib.sha256`` positive control. Every buffer in this pipeline is far past
that threshold. A term built out of GIL-releasing primitives is already
running in parallel and is not evidence for a migration.

This driver answers, per stage: **how much of the profiled time is actually
GIL-held, and how much is already parallel?**

Method mirrors #143 section 1 so the numbers are comparable:

* **cores-used = (process CPU time) / (wall time)** with the stage running in
  N = 1, 2, 4, 8 threads, each thread driving its **own independent input**.
  1.00 flat from 1->8 threads means fully GIL-held; scaling toward N means the
  stage releases the GIL.
* a **1 MiB ``hashlib.sha256`` positive control** and a **small-buffer
  pure-Python negative control** are measured in the same process, at the same
  thread counts, in the same alternation as the stages, and are reported beside
  every table. The positive control sets this host's achievable ceiling; the
  negative control must pin at ~1.00.
* configurations are visited **alternating** (outer loop = repetition, inner
  loop = configuration) rather than in blocks, so host drift lands on every
  configuration equally rather than on whichever ran last.

**Thread-start amortization is load-bearing and was measured, not assumed.**
On this rig the same 1 MiB sha256 positive control reads 5.33 cores at 0.26 s
of wall and 7.42 cores at 0.90 s -- the shortfall is thread create/join
overhead charged against too short a measurement, not a GIL effect. Every
configuration is therefore grown until its wall clock clears
``--min-wall-seconds`` (default 1.0 s). Skipping this understates cores-used
on *every* stage, which would have manufactured a false confirmation of the
"GIL-held" premise. See ``AMORTIZATION_RATIONALE``.

Stages drive the **shipped** callables -- nothing is reimplemented:

===========================  ==================================================
stage                        callable
===========================  ==================================================
``fold``                     ``IncrementalShareWindow.from_full_snapshot``
``fold_pages``               ``_IncrementalShareWindowPage.from_records`` (all)
``digest``                   ``IncrementalShareJsonSequence.canonical_json_sha256``
``to_prism_json``            ``AcceptedShareRecord.to_prism_json`` (all records)
``spool_acquire``            ``_ShareWindowSerialization.acquire_spooled_tail``
``spool_compact``            ``_ShareWindowSerialization.compact_tail_chunks``
``spool_encode``             ``str.encode("utf-8")`` of both fragments
``spool_write``              ``TemporaryFile`` write/flush/seek of the payload
===========================  ==================================================

``fold_pages``, ``spool_compact``, ``spool_encode`` and ``spool_write`` are
sub-terms, reported to decompose the two stages that are internally mixed;
they are not additional profile rows.

Records are shaped like ``lab/prism/job_build_benchmark.py``'s defaults
(``--shares 21868 --miners 2``), whose values encode a live-host measurement.
``--share-id-shape production`` re-runs the byte accounting with the
``username:block_hash_hex`` share_id that ``lab/prism/share_writer.py`` builds,
because the benchmark's short synthetic ``share_id`` materially changes payload
bytes (but not, as measured, any GIL verdict).

Self-contained and re-runnable: no database, no network, no coordinator, no
third-party packages. ``python3 tests/perf/window_pipeline_gil_scaling.py``
prints the tables; ``--json`` emits the same data structured.

This is an **on-demand instrument, not a test**: it asserts no thresholds and
is deliberately not named ``test_*`` so the discovery run never executes it
(#160 -- a threshold assertion on a shared runner is a flaky test in waiting).
Nothing under ``lab/`` is imported for anything but read-only measurement.

``--latency-probe`` (#236) is a second mode over the same fixture: instead of
cores-used across threads it runs each serialization phase once at the
incident's window sizes (210k and 400k shares by default) in the main thread
while a monitor thread wakes on the writer-lease monitor's cadence
(``WRITER_LEASE_HEARTBEAT_MONITOR_SECONDS``) and records how late every wake
was. A GIL-held C call delays the monitor for its whole duration; a Python
loop of bounded C calls lets the interpreter switch at its normal interval.
Each bounded phase is paired with the historical whole-window call it
replaced, and ``--daemon-binary`` adds the real Rust builder's prepare
round trip split into request write, response wait and response read. See
``LATENCY_PROBE_RATIONALE``.
"""

from __future__ import annotations

import argparse
import contextlib
import gc
import hashlib
import json
import os
import platform
import sys
import sysconfig
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism.bundle_compiler import (
    BundleCompiler,
    _compact_share_payload,
    _compact_share_tail_chunks,
    _iter_prepare_window_request_chunks,
    _ShareWindowSerialization,
)
from lab.prism.share_json_stream import (
    canonical_share_array_sha256,
    iter_canonical_share_item_chunks,
    iter_json_object_text_chunks,
)
from lab.prism.share_ledger import (
    DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
    AcceptedShareRecord,
    DaemonShareJsonSequence,
    DaemonShareWindowMirror,
    IncrementalShareWindow,
    _IncrementalShareWindowPage,
    block_candidate_identity,
    block_candidate_identity_sha256,
    sha256_json_hex,
)
from lab.prism.writer_lease_timing import (
    WRITER_LEASE_HEARTBEAT_MONITOR_SECONDS,
    WRITER_LEASE_HEARTBEAT_SCHEDULER_SLACK_SECONDS,
)


CLOCK_RATIONALE = (
    "cores-used = process CPU / wall. time.process_time_ns() is the numerator "
    "because it sums CPU across every thread in the process, which is exactly "
    "the quantity that separates 'N threads made progress together' from 'N "
    "threads took turns under the GIL'. time.perf_counter_ns() is the "
    "denominator. thread_time is deliberately NOT used here: it is per-thread "
    "and cannot see the concurrency that is the whole question."
)

AMORTIZATION_RATIONALE = (
    "Thread create/join is charged to wall clock but contributes little "
    "process CPU, so a measurement whose wall is close to the thread-start "
    "cost reads LOW on cores-used regardless of GIL behaviour. Measured on "
    "this rig with the 1 MiB sha256 positive control: 5.33 cores at 0.26 s "
    "wall, 6.43 at 0.25 s, 7.42 at 0.90 s, 7.42 at 3.71 s. Every "
    "configuration is therefore grown until wall >= --min-wall-seconds. "
    "Without this the bias is one-sided: it pushes every stage toward 1.00, "
    "i.e. toward falsely confirming 'GIL-held'."
)

# ---------------------------------------------------------------------------
# published anchors
# ---------------------------------------------------------------------------

# #143 section 1: 1 MiB hashlib.sha256, 8 threads.
PUBLISHED_POSITIVE_CONTROL_CORES = 7.89
# #143 section 1 bracketed CPython's GIL-release threshold here.
PUBLISHED_RELEASE_THRESHOLD_BYTES = (1024, 2048)

# #131's profile table, per materialization, developer workstation. Keyed by
# window size; values are milliseconds attributed to each term.
PUBLISHED_PROFILE_MS: dict[int, dict[str, float]] = {
    21_868: {
        "fold": 71.0,
        "digest": 31.0,
        "spool_acquire": 28.0,
        "to_prism_json": 5.0,
    },
    100_000: {
        "fold": 271.0,
        "digest": 139.0,
        "spool_acquire": 122.0,
        "to_prism_json": 23.0,
    },
    400_000: {
        "fold": 1_057.0,
        "digest": 530.0,
        "spool_acquire": 476.0,
        "to_prism_json": 95.0,
    },
}

# #131 annotates the spool row with a byte size at each window size.
PUBLISHED_SPOOL_ROW_BYTES: dict[int, int] = {
    21_868: 8_000_000,
    100_000: 37_000_000,
    400_000: 151_000_000,
}

# job_build_benchmark.py defaults: --shares 21868, --miners 2.
BENCHMARK_SHARES = 21_868
BENCHMARK_MINERS = 2

DEFAULT_SIZES = (21_868, 100_000, 400_000)
DEFAULT_THREADS = (1, 2, 4, 8)
# Above this window size an 8-thread sweep holds ~0.7 GB of independent input
# per thread; the contract for this measurement allows the largest size to be
# reported at the endpoints only. Overridable with --large-size-threads.
LARGE_SIZE_BYTES_THRESHOLD = 200_000
LARGE_SIZE_THREADS = (1, 8)

POSITIVE_CONTROL_BYTES = 1 << 20
NEGATIVE_CONTROL_ITERATIONS = 20_000

# Fixed epoch so runs are reproducible; the pipeline only compares these.
FIXED_NOW_MS = 1_760_000_000_000


# ---------------------------------------------------------------------------
# measurement harness
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CoresResult:
    """One (stage, thread-count) measurement."""

    threads: int
    cores_used: float
    wall_seconds: float
    process_cpu_seconds: float
    iterations: int
    cpu_ms_per_call: float
    wall_ms_per_call: float
    loadavg_1: float | None
    grew: int

    def as_json(self) -> dict[str, Any]:
        return {
            "threads": self.threads,
            "cores_used": round(self.cores_used, 4),
            "wall_seconds": round(self.wall_seconds, 4),
            "process_cpu_seconds": round(self.process_cpu_seconds, 4),
            "iterations": self.iterations,
            "cpu_ms_per_call": round(self.cpu_ms_per_call, 4),
            "wall_ms_per_call": round(self.wall_ms_per_call, 4),
            "loadavg_1": self.loadavg_1,
            "grew": self.grew,
        }


def _loadavg_1() -> float | None:
    try:
        return os.getloadavg()[0]
    except (OSError, AttributeError):  # pragma: no cover - platform dependent
        return None


def measure_cores(
    make_callable: Callable[[int], Callable[[], Any]],
    threads: int,
    *,
    min_wall_seconds: float,
    max_wall_seconds: float,
    max_growths: int = 4,
) -> CoresResult:
    """Cores-used for ``threads`` copies of a stage on independent inputs.

    ``make_callable(i)`` returns the zero-argument callable thread ``i`` will
    drive; each index must own its own input so the only thing shared between
    threads is the interpreter itself.

    The iteration count is calibrated from one untimed warm call and then
    **grown until the wall clock clears ``min_wall_seconds``** -- see
    ``AMORTIZATION_RATIONALE``; an under-length measurement is biased toward
    1.00 and would read as "GIL-held" whatever the truth is. ``max_wall_seconds``
    bounds the opposite end: a fully GIL-held stage serializes, so its wall grows
    ~linearly in ``threads`` and would otherwise run for minutes at 400k shares.
    """

    callables = [make_callable(index) for index in range(threads)]

    # Warm one copy (imports, allocator arenas, branch predictors) and use it
    # to size the batch. The warm call is not part of any reported number.
    warm_start = time.perf_counter()
    callables[0]()
    per_call = max(time.perf_counter() - warm_start, 1e-9)

    iterations = max(1, int(min_wall_seconds / per_call))
    serialized_wall = per_call * iterations * threads
    if serialized_wall > max_wall_seconds:
        iterations = max(1, int(max_wall_seconds / (per_call * threads)))

    grew = 0
    while True:
        gate = threading.Barrier(threads + 1)

        def worker(fn: Callable[[], Any], count: int) -> None:
            gate.wait()
            for _ in range(count):
                fn()

        workers = [
            threading.Thread(target=worker, args=(fn, iterations), daemon=True)
            for fn in callables
        ]
        for thread in workers:
            thread.start()
        # Threads exist and are parked on the barrier before the clocks start,
        # so thread *creation* is outside the measured window. Start/join
        # scheduling still is not, which is what min_wall_seconds covers.
        gate.wait()
        load = _loadavg_1()
        cpu_start = time.process_time_ns()
        wall_start = time.perf_counter_ns()
        for thread in workers:
            thread.join()
        wall_ns = time.perf_counter_ns() - wall_start
        cpu_ns = time.process_time_ns() - cpu_start

        wall_seconds = wall_ns / 1e9
        if wall_seconds >= min_wall_seconds or grew >= max_growths:
            break
        # Under-length: grow toward the floor and re-measure from scratch.
        # Wall scales with the iteration count whether the stage is serialized
        # or parallel, so the projection needs no thread-count factor.
        scale = max(2, int(min_wall_seconds / max(wall_seconds, 1e-6)) + 1)
        if wall_seconds * scale > max_wall_seconds:
            break
        iterations = max(iterations + 1, iterations * scale)
        grew += 1

    calls = iterations * threads
    return CoresResult(
        threads=threads,
        cores_used=cpu_ns / wall_ns if wall_ns else float("nan"),
        wall_seconds=wall_ns / 1e9,
        process_cpu_seconds=cpu_ns / 1e9,
        iterations=iterations,
        cpu_ms_per_call=cpu_ns / 1e6 / calls,
        wall_ms_per_call=wall_ns / 1e6 / iterations,
        loadavg_1=load,
        grew=grew,
    )


# ---------------------------------------------------------------------------
# controls
# ---------------------------------------------------------------------------


def positive_control(index: int) -> Callable[[], Any]:
    """#143's control: 1 MiB hashlib.sha256, which must scale with threads.

    Each thread hashes a distinct buffer so nothing is shared but the
    interpreter.
    """
    # bytes([n]) -- a one-element list. bytes(n) would build n zero bytes and
    # give every thread a differently sized buffer, which desynchronizes them
    # and reads as a GIL effect it is not.
    buffer = bytes([(index * 37 + 11) % 251]) * POSITIVE_CONTROL_BYTES

    def run() -> bytes:
        return hashlib.sha256(buffer).digest()

    return run


def negative_control(index: int) -> Callable[[], Any]:
    """Small-buffer pure-Python CPU, which must pin at ~1.00 cores."""
    seed = index + 1

    def run() -> int:
        total = 0
        for value in range(NEGATIVE_CONTROL_ITERATIONS):
            total += value * seed
        return total

    return run


# ---------------------------------------------------------------------------
# inputs
# ---------------------------------------------------------------------------


def benchmark_miner_programs(count: int) -> tuple[str, ...]:
    """Same construction as ``lab/prism/job_build_benchmark.py``."""
    return tuple(
        hashlib.sha256(f"bench-miner-{index}".encode()).hexdigest()
        for index in range(count)
    )


def build_records(
    count: int,
    *,
    miners: int = BENCHMARK_MINERS,
    share_id_shape: str = "benchmark",
    salt: int = 0,
) -> list[AcceptedShareRecord]:
    """Records shaped like ``job_build_benchmark.BenchLedger``'s window.

    ``share_id_shape="production"`` swaps the benchmark's short synthetic
    ``share_id`` for the ``username:block_hash_hex`` form that
    ``lab/prism/share_writer.py`` actually builds (the ledger schema's
    ``length(share_id) >= 65`` index predicate corroborates the length). Only
    the byte accounting moves; the GIL verdicts do not.
    """

    programs = benchmark_miner_programs(miners)
    records: list[AcceptedShareRecord] = []
    for index in range(count):
        miner = index % miners
        if share_id_shape == "production":
            block_hash = hashlib.sha256(
                f"{salt}:{index}".encode()
            ).hexdigest()
            share_id = f"bench-miner-{miner}.rig{index % 512}:{block_hash}"
        else:
            share_id = f"bench-share-{index + 1}"
        records.append(
            AcceptedShareRecord(
                share_seq=index + 1,
                share_id=share_id,
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
class StageInputs:
    """One thread's private copy of every input the stages need."""

    records: list[AcceptedShareRecord]
    anchor_job_issued_at_ms: int
    window_weight: int
    window: IncrementalShareWindow
    page_records: tuple[tuple[AcceptedShareRecord, ...], ...]
    json_sequence: Any
    shares: list[dict[str, object]]
    snapshot_sha256: str
    identities_json: str
    compact_shares_json: str
    identities_bytes: bytes
    compact_shares_bytes: bytes
    canonical_json_bytes: int
    spool_bytes: int


def build_inputs(
    count: int,
    *,
    miners: int,
    share_id_shape: str,
    salt: int,
) -> StageInputs:
    records = build_records(
        count, miners=miners, share_id_shape=share_id_shape, salt=salt
    )
    anchor = int(records[-1].job_issued_at_ms)
    # Retain every record: the window-weight cutoff is not what is under test,
    # and a partial retention would silently shrink the measured window.
    weight = sum(int(record.share_difficulty) for record in records)
    window = IncrementalShareWindow.from_full_snapshot(
        records,
        anchor_job_issued_at_ms=anchor,
        window_weight=weight,
    )
    sequence = window.json_records()
    shares = list(sequence)
    snapshot = sequence.canonical_json_sha256()
    identities, compact_shares = _compact_share_payload(shares)
    identities_json = json.dumps(identities, separators=(",", ":"))
    compact_shares_json = json.dumps(compact_shares, separators=(",", ":"))
    identities_bytes = identities_json.encode("utf-8")
    compact_shares_bytes = compact_shares_json.encode("utf-8")
    canonical_bytes = sum(len(page.canonical_json_items) for page in window.pages)
    # Exactly the byte layout acquire_spooled_tail writes.
    spool_bytes = (
        len(b',"compact_share_identities":')
        + len(identities_bytes)
        + len(b',"compact_shares":')
        + len(compact_shares_bytes)
        + len(b"}")
    )
    return StageInputs(
        records=records,
        anchor_job_issued_at_ms=anchor,
        window_weight=weight,
        window=window,
        page_records=tuple(page.records for page in window.pages),
        json_sequence=sequence,
        shares=shares,
        snapshot_sha256=snapshot,
        identities_json=identities_json,
        compact_shares_json=compact_shares_json,
        identities_bytes=identities_bytes,
        compact_shares_bytes=compact_shares_bytes,
        canonical_json_bytes=canonical_bytes,
        spool_bytes=spool_bytes,
    )


# ---------------------------------------------------------------------------
# stages
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Stage:
    key: str
    callable_name: str
    profile_row: str | None
    parent: str | None
    note: str
    factory: Callable[[StageInputs], Callable[[], Any]]


def _fold(inputs: StageInputs) -> Callable[[], Any]:
    records = inputs.records
    anchor = inputs.anchor_job_issued_at_ms
    weight = inputs.window_weight

    def run() -> IncrementalShareWindow:
        return IncrementalShareWindow.from_full_snapshot(
            records,
            anchor_job_issued_at_ms=anchor,
            window_weight=weight,
        )

    return run


def _fold_pages(inputs: StageInputs) -> Callable[[], Any]:
    page_records = inputs.page_records

    def run() -> tuple[Any, ...]:
        return tuple(
            _IncrementalShareWindowPage.from_records(records)
            for records in page_records
        )

    return run


def _digest(inputs: StageInputs) -> Callable[[], Any]:
    sequence = inputs.json_sequence

    def run() -> str:
        return sequence.canonical_json_sha256()

    return run


def _to_prism_json(inputs: StageInputs) -> Callable[[], Any]:
    records = inputs.records

    def run() -> list[dict[str, object]]:
        return [record.to_prism_json() for record in records]

    return run


def _new_serialization(inputs: StageInputs) -> _ShareWindowSerialization:
    return _ShareWindowSerialization(
        key=(inputs.snapshot_sha256, len(inputs.records), inputs.window_weight),
        share_count=len(inputs.records),
        share_snapshot_sha256=inputs.snapshot_sha256,
    )


def _spool_acquire(inputs: StageInputs) -> Callable[[], Any]:
    shares = inputs.shares

    def run() -> None:
        # A fresh instance every call: acquire_spooled_tail memoizes, so a
        # reused instance would measure a dict lookup after the first call.
        serialization = _new_serialization(inputs)
        serialization.acquire_spooled_tail(shares)
        # Retire then release so the leased descriptor is closed by the
        # shipped teardown path rather than left to the GC.
        serialization.retire_spool()
        serialization.release_spooled_tail()

    return run


def _spool_compact(inputs: StageInputs) -> Callable[[], Any]:
    shares = inputs.shares

    def run() -> tuple[str, ...]:
        return _new_serialization(inputs).compact_tail_chunks(shares)

    return run


def _spool_encode(inputs: StageInputs) -> Callable[[], Any]:
    identities_json = inputs.identities_json
    compact_shares_json = inputs.compact_shares_json

    def run() -> tuple[bytes, bytes]:
        return (
            identities_json.encode("utf-8"),
            compact_shares_json.encode("utf-8"),
        )

    return run


def _spool_write(inputs: StageInputs) -> Callable[[], Any]:
    identities_bytes = inputs.identities_bytes
    compact_shares_bytes = inputs.compact_shares_bytes

    def run() -> None:
        spool = tempfile.TemporaryFile()
        try:
            spool.write(b',"compact_share_identities":')
            spool.write(identities_bytes)
            spool.write(b',"compact_shares":')
            spool.write(compact_shares_bytes)
            spool.write(b"}")
            spool.flush()
            spool.seek(0, os.SEEK_END)
        finally:
            spool.close()

    return run


STAGES: tuple[Stage, ...] = (
    Stage(
        "fold",
        "IncrementalShareWindow.from_full_snapshot",
        "Window fold",
        None,
        "sorted() + per-record validation + paging; paging calls to_prism_json "
        "and json.dumps per record",
        _fold,
    ),
    Stage(
        "fold_pages",
        "_IncrementalShareWindowPage.from_records (every page)",
        None,
        "fold",
        "the paging half of the fold: to_prism_json, json.dumps, b','.join",
        _fold_pages,
    ),
    Stage(
        "digest",
        "IncrementalShareJsonSequence.canonical_json_sha256",
        "Canonical-JSON digest",
        None,
        "one hashlib.sha256 fed pre-encoded per-page buffers",
        _digest,
    ),
    Stage(
        "to_prism_json",
        "AcceptedShareRecord.to_prism_json (every record)",
        "Record->JSON conversion",
        None,
        "measured standalone; note the fold already calls it internally",
        _to_prism_json,
    ),
    Stage(
        "spool_acquire",
        "_ShareWindowSerialization.acquire_spooled_tail (cold)",
        "Spool serialization",
        None,
        "compact_fragments + utf-8 encode + TemporaryFile writes",
        _spool_acquire,
    ),
    Stage(
        "spool_compact",
        "_ShareWindowSerialization.compact_tail_chunks (cold)",
        None,
        "spool_acquire",
        "identity dedup + batched json.dumps into bounded chunks (#236)",
        _spool_compact,
    ),
    Stage(
        "spool_encode",
        'str.encode("utf-8") of both fragments',
        None,
        "spool_acquire",
        "str -> bytes for the two fragment strings",
        _spool_encode,
    ),
    Stage(
        "spool_write",
        "TemporaryFile write/flush/seek of the spool payload",
        None,
        "spool_acquire",
        "the os.write half of the spool term",
        _spool_write,
    ),
)

STAGES_BY_KEY = {stage.key: stage for stage in STAGES}
PROFILE_ROW_STAGES = tuple(stage for stage in STAGES if stage.profile_row)


# ---------------------------------------------------------------------------
# environment
# ---------------------------------------------------------------------------


def _peak_rss_mb() -> float | None:
    try:
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:  # pragma: no cover - platform dependent
        return None
    # Linux reports KiB, macOS bytes.
    return peak / 1024.0 if sys.platform != "darwin" else peak / (1024.0 * 1024.0)


def _current_rss_mb() -> float | None:
    """Resident set size *right now*.

    Deliberately not ru_maxrss: that is a lifetime high-water mark that never
    falls, so after the smaller window sizes have run it would over-estimate
    the live footprint by several hundred MB and skip the large size for a
    shortage that does not exist.
    """
    try:
        with open("/proc/self/statm", encoding="ascii") as handle:
            resident_pages = int(handle.read().split()[1])
    except (OSError, IndexError, ValueError):  # pragma: no cover - platform
        return None
    return resident_pages * os.sysconf("SC_PAGE_SIZE") / (1024.0 * 1024.0)


def _available_memory_mb() -> float | None:
    try:
        with open("/proc/meminfo", encoding="ascii") as handle:
            for line in handle:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:  # pragma: no cover - platform dependent
        return None
    return None


def describe_environment() -> dict[str, Any]:
    try:
        load = os.getloadavg()
    except (OSError, AttributeError):  # pragma: no cover - platform dependent
        load = None
    try:
        gil_enabled: bool | None = sys._is_gil_enabled()  # type: ignore[attr-defined]
    except AttributeError:
        gil_enabled = None
    physical = None
    try:
        with open("/proc/cpuinfo", encoding="ascii") as handle:
            text = handle.read()
        cores = {
            line.split(":", 1)[1].strip()
            for line in text.splitlines()
            if line.startswith("cpu cores")
        }
        if cores:
            physical = sorted(cores)[0]
    except OSError:  # pragma: no cover - platform dependent
        pass
    return {
        "cpu": platform.processor() or platform.machine(),
        "machine": platform.machine(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "cpu_cores_per_socket": physical,
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "gil_disabled_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "gil_enabled_at_runtime": gil_enabled,
        "loadavg_1_5_15": load,
        "available_memory_mb": _available_memory_mb(),
        "page_size_records": DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        "clock_rationale": CLOCK_RATIONALE,
        "amortization_rationale": AMORTIZATION_RATIONALE,
    }


# ---------------------------------------------------------------------------
# analysis
# ---------------------------------------------------------------------------


def released_fraction(cores: float, ceiling: float) -> float | None:
    """Fraction of a stage's CPU that ran with the GIL released.

    Normalized against the positive control measured **on this host at the
    same thread count**, not against the nominal thread count: an 8-vCPU guest
    cannot reach 8.00 even on a perfectly parallel workload, so dividing by 8
    would understate every stage. 1.00 cores maps to 0.0, the control's
    ceiling maps to 1.0. Returns None when the ceiling offers no headroom.
    """
    headroom = ceiling - 1.0
    if headroom <= 0.05:
        return None
    return max(0.0, min(1.0, (cores - 1.0) / headroom))


def _fmt(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    if value != value:  # NaN
        return "n/a"
    return f"{value:.{digits}f}"


def _fmt_mb(value: float | None) -> str:
    return "n/a" if value is None else f"{value / 1e6:.2f}"


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    count = len(ordered)
    if not count:
        return float("nan")
    mid = count // 2
    if count % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------


def thread_counts_for(size: int, threads: Sequence[int], large: Sequence[int]) -> tuple[int, ...]:
    if size > LARGE_SIZE_BYTES_THRESHOLD:
        return tuple(t for t in threads if t in set(large))
    return tuple(threads)


def run(args: argparse.Namespace) -> dict[str, Any]:
    sizes = tuple(args.sizes)
    threads = tuple(args.threads)
    large_threads = tuple(args.large_size_threads)
    reps = args.reps

    environment = describe_environment()
    results: dict[str, Any] = {
        "environment": environment,
        "methodology": {
            "metric": "cores-used = process CPU / wall",
            "repetitions": reps,
            "statistic": "median of N alternating repetitions",
            "alternation": (
                "outer loop repetition, inner loop configuration, so host "
                "drift lands on every configuration equally"
            ),
            "min_wall_seconds": args.min_wall_seconds,
            "max_wall_seconds": args.max_wall_seconds,
            "independent_inputs_per_thread": True,
            "share_id_shape": args.share_id_shape,
            "miners": args.miners,
            "page_size": DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
            "published_positive_control_cores": PUBLISHED_POSITIVE_CONTROL_CORES,
        },
        "sizes": [],
        "controls": {},
        "peak_rss_mb": None,
        "aborted": None,
    }

    control_specs = (
        ("positive", positive_control, "1 MiB hashlib.sha256"),
        ("negative", negative_control, f"{NEGATIVE_CONTROL_ITERATIONS}-iteration pure-Python loop"),
    )

    # ---- controls, alternating across thread counts and repetitions --------
    control_samples: dict[str, dict[int, list[CoresResult]]] = {
        name: {count: [] for count in threads} for name, _, _ in control_specs
    }
    for _ in range(reps):
        for count in threads:
            for name, factory, _ in control_specs:
                control_samples[name][count].append(
                    measure_cores(
                        factory,
                        count,
                        min_wall_seconds=args.min_wall_seconds,
                        max_wall_seconds=args.max_wall_seconds,
                    )
                )

    ceilings: dict[int, float] = {}
    for name, _, description in control_specs:
        per_thread = []
        for count in threads:
            samples = control_samples[name][count]
            cores = _median([s.cores_used for s in samples])
            if name == "positive":
                ceilings[count] = cores
            per_thread.append(
                {
                    "threads": count,
                    "cores_used_median": cores,
                    "cores_used_min": min(s.cores_used for s in samples),
                    "cores_used_max": max(s.cores_used for s in samples),
                    "wall_seconds_median": _median([s.wall_seconds for s in samples]),
                    "loadavg_1_median": _median(
                        [s.loadavg_1 for s in samples if s.loadavg_1 is not None]
                    )
                    if any(s.loadavg_1 is not None for s in samples)
                    else None,
                    "samples": [s.as_json() for s in samples],
                }
            )
        results["controls"][name] = {
            "description": description,
            "by_threads": per_thread,
        }

    positive_at_max = ceilings.get(max(threads))
    results["controls"]["positive_control_reproduces"] = (
        None
        if positive_at_max is None
        else positive_at_max / PUBLISHED_POSITIVE_CONTROL_CORES
    )

    # The contract for this measurement: if the positive control does not
    # reproduce, the rig is wrong and the stage numbers must not be reported.
    # The floor is a fraction of the *nominal* thread count rather than an
    # absolute core count, so the gate stays meaningful when the sweep is run
    # at fewer threads: a 4-thread sweep can never reach 6 cores, and an
    # absolute floor would abort a perfectly healthy rig. #143 published
    # 7.89/8 = 0.986 of nominal.
    control_floor = args.control_floor_fraction * max(threads)
    results["methodology"]["control_floor_cores"] = control_floor
    results["methodology"]["control_floor_fraction"] = args.control_floor_fraction
    if positive_at_max is not None and positive_at_max < control_floor:
        results["aborted"] = (
            f"positive control reached {positive_at_max:.2f} cores at "
            f"{max(threads)} threads, below the floor of "
            f"{control_floor:.2f} (= {args.control_floor_fraction:g} x "
            f"{max(threads)} threads; #143 published "
            f"{PUBLISHED_POSITIVE_CONTROL_CORES} at 8). The rig is not "
            f"measuring parallelism correctly; stage numbers are withheld."
        )
        results["peak_rss_mb"] = _peak_rss_mb()
        return results

    # ---- stages -----------------------------------------------------------
    for size in sizes:
        size_threads = thread_counts_for(size, threads, large_threads)
        if not size_threads:
            continue
        max_threads = max(size_threads)

        rss_before = _current_rss_mb() or 0.0
        probe = build_inputs(
            size,
            miners=args.miners,
            share_id_shape=args.share_id_shape,
            salt=0,
        )
        rss_after = _current_rss_mb() or 0.0
        available = _available_memory_mb()
        # Footprint of exactly one thread's inputs, measured rather than
        # guessed, with 1.6x headroom for the transient window a running fold
        # allocates before the previous one is collected.
        per_input_mb = max(rss_after - rss_before, 1.0)
        estimate_mb = per_input_mb * max_threads * 1.6
        if available is not None and estimate_mb > available * 0.8:
            results["sizes"].append(
                {
                    "size": size,
                    "skipped": (
                        f"estimated {estimate_mb:.0f} MB for {max_threads} "
                        f"independent inputs ({per_input_mb:.0f} MB each) "
                        f"exceeds 80% of {available:.0f} MB available"
                    ),
                }
            )
            del probe
            continue

        inputs = [probe] + [
            build_inputs(
                size,
                miners=args.miners,
                share_id_shape=args.share_id_shape,
                salt=index,
            )
            for index in range(1, max_threads)
        ]

        samples: dict[str, dict[int, list[CoresResult]]] = {
            stage.key: {count: [] for count in size_threads} for stage in STAGES
        }
        for _ in range(reps):
            for count in size_threads:
                for stage in STAGES:
                    samples[stage.key][count].append(
                        measure_cores(
                            # noqa is for `inputs`: it is bound at the top of
                            # this loop body and freed by `del inputs` at the
                            # end of it, which is enough for Ruff to read the
                            # closure as unbound. measure_cores joins every
                            # thread before returning, so no invocation of this
                            # lambda outlives the binding.
                            lambda index, stage=stage: stage.factory(inputs[index]),  # noqa: F821
                            count,
                            min_wall_seconds=args.min_wall_seconds,
                            max_wall_seconds=args.max_wall_seconds,
                        )
                    )

        stage_rows = []
        for stage in STAGES:
            by_threads = []
            for count in size_threads:
                stage_samples = samples[stage.key][count]
                cores = _median([s.cores_used for s in stage_samples])
                by_threads.append(
                    {
                        "threads": count,
                        "cores_used_median": cores,
                        "cores_used_min": min(s.cores_used for s in stage_samples),
                        "cores_used_max": max(s.cores_used for s in stage_samples),
                        "cpu_ms_per_call_median": _median(
                            [s.cpu_ms_per_call for s in stage_samples]
                        ),
                        "wall_ms_per_call_median": _median(
                            [s.wall_ms_per_call for s in stage_samples]
                        ),
                        "released_fraction": released_fraction(
                            cores, ceilings.get(count, float("nan"))
                        )
                        if count > 1
                        else None,
                        "loadavg_1_median": _median(
                            [s.loadavg_1 for s in stage_samples if s.loadavg_1 is not None]
                        )
                        if any(s.loadavg_1 is not None for s in stage_samples)
                        else None,
                        "samples": [s.as_json() for s in stage_samples],
                    }
                )
            stage_rows.append(
                {
                    "key": stage.key,
                    "callable": stage.callable_name,
                    "profile_row": stage.profile_row,
                    "parent": stage.parent,
                    "note": stage.note,
                    "by_threads": by_threads,
                }
            )

        results["sizes"].append(
            {
                "size": size,
                "thread_counts": list(size_threads),
                "input_mb_per_thread": round(per_input_mb, 1),
                "bytes": {
                    "canonical_json_bytes": probe.canonical_json_bytes,
                    "spool_payload_bytes": probe.spool_bytes,
                    "canonical_bytes_per_record": probe.canonical_json_bytes / size,
                    "spool_bytes_per_record": probe.spool_bytes / size,
                    "page_count": len(probe.window.pages),
                    "canonical_bytes_per_page": (
                        probe.canonical_json_bytes / max(len(probe.window.pages), 1)
                    ),
                    "published_spool_row_bytes": PUBLISHED_SPOOL_ROW_BYTES.get(size),
                },
                "stages": stage_rows,
            }
        )

        del inputs
        del probe
        gc.collect()

    results["peak_rss_mb"] = _peak_rss_mb()
    try:
        results["loadavg_end"] = list(os.getloadavg())
    except (OSError, AttributeError):  # pragma: no cover - platform dependent
        results["loadavg_end"] = None
    return results


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------


def render_report(results: dict[str, Any]) -> str:
    out: list[str] = []
    env = results["environment"]
    method = results["methodology"]

    out.append("=" * 78)
    out.append("GIL scaling of the PRISM payout-window materialization pipeline")
    out.append("=" * 78)
    out.append("")
    out.append(f"CPU              {env['cpu']} x{env['cpu_count']} logical")
    out.append(f"Platform         {env['platform']}")
    out.append(f"Python           {env['python_version'].splitlines()[0]}")
    out.append(
        f"GIL              build Py_GIL_DISABLED={env['gil_disabled_build']}, "
        f"runtime enabled={env['gil_enabled_at_runtime']}"
    )
    load = env["loadavg_1_5_15"]
    out.append(
        "Load at start    "
        + ("n/a" if load is None else " / ".join(f"{v:.2f}" for v in load))
    )
    out.append(f"Page size        {env['page_size_records']} records")
    out.append(
        f"Statistic        median of {method['repetitions']} alternating repetitions; "
        f"wall floor {method['min_wall_seconds']}s"
    )
    out.append("")
    out.append("Metric: " + method["metric"])
    out.append("")

    # ---- controls ----------------------------------------------------------
    out.append("-" * 78)
    out.append("CONTROLS (measured in this process, same alternation as the stages)")
    out.append("-" * 78)
    out.append("")
    header = f"{'control':<34}" + "".join(
        f"{'N=' + str(row['threads']):>10}"
        for row in results["controls"]["positive"]["by_threads"]
    )
    out.append(header)
    out.append("-" * len(header))
    for name in ("positive", "negative"):
        control = results["controls"][name]
        label = f"{name}: {control['description']}"
        out.append(
            f"{label[:33]:<34}"
            + "".join(
                f"{_fmt(row['cores_used_median']):>10}"
                for row in control["by_threads"]
            )
        )
    out.append("")
    loads = [
        row["loadavg_1_median"]
        for row in results["controls"]["positive"]["by_threads"]
        if row["loadavg_1_median"] is not None
    ]
    if loads:
        out.append(f"load average (1m) during controls: {min(loads):.2f}-{max(loads):.2f}")
    reproduces = results["controls"].get("positive_control_reproduces")
    top_threads = results["controls"]["positive"]["by_threads"][-1]["threads"]
    if reproduces is not None and top_threads == 8:
        out.append(
            f"positive control vs #143's published {PUBLISHED_POSITIVE_CONTROL_CORES} "
            f"cores at 8 threads: ratio {reproduces:.3f}"
        )
    elif reproduces is not None:
        # #143's anchor is an 8-thread number; comparing a shorter sweep's top
        # thread count against it would read as a failure to reproduce when it
        # is only a different thread count.
        out.append(
            f"positive control top sweep point is {top_threads} threads; "
            f"#143's {PUBLISHED_POSITIVE_CONTROL_CORES}-core anchor is an "
            f"8-thread figure and is not comparable here"
        )
    out.append("")

    if results.get("aborted"):
        out.append("!" * 78)
        out.append("ABORTED: " + results["aborted"])
        out.append("!" * 78)
        return "\n".join(out)

    # ---- per size ----------------------------------------------------------
    for entry in results["sizes"]:
        size = entry["size"]
        out.append("=" * 78)
        out.append(f"WINDOW SIZE: {size:,} shares")
        out.append("=" * 78)
        if entry.get("skipped"):
            out.append(f"  SKIPPED: {entry['skipped']}")
            out.append("")
            continue

        counts = entry["thread_counts"]
        byte_info = entry["bytes"]
        out.append("")
        out.append(
            f"  canonical JSON {_fmt_mb(byte_info['canonical_json_bytes'])} MB "
            f"({byte_info['canonical_bytes_per_record']:.0f} B/record, "
            f"{byte_info['page_count']} pages, "
            f"{byte_info['canonical_bytes_per_page'] / 1024:.0f} KiB/page)"
        )
        published_bytes = byte_info.get("published_spool_row_bytes")
        out.append(
            f"  spool payload  {_fmt_mb(byte_info['spool_payload_bytes'])} MB "
            f"({byte_info['spool_bytes_per_record']:.0f} B/record)"
            + (
                f"   [#131 spool row says {published_bytes / 1e6:.0f} MB]"
                if published_bytes
                else ""
            )
        )
        out.append("")

        head = f"  {'stage':<30}" + "".join(f"{'N=' + str(c):>9}" for c in counts)
        head += f"{'CPU ms':>10}{'released':>10}"
        out.append(head)
        out.append("  " + "-" * (len(head) - 2))
        for stage_row in entry["stages"]:
            stage = STAGES_BY_KEY[stage_row["key"]]
            label = ("  " if stage.parent else "") + stage.key
            cells = ""
            for count in counts:
                match = next(
                    r for r in stage_row["by_threads"] if r["threads"] == count
                )
                cells += f"{_fmt(match['cores_used_median']):>9}"
            single = next(
                r for r in stage_row["by_threads"] if r["threads"] == counts[0]
            )
            top = next(
                r for r in stage_row["by_threads"] if r["threads"] == counts[-1]
            )
            released = top["released_fraction"]
            out.append(
                f"  {label:<30}{cells}"
                f"{single['cpu_ms_per_call_median']:>10.1f}"
                f"{(_fmt(released * 100, 0) + '%') if released is not None else 'n/a':>10}"
            )
        out.append("")
        loads = [
            r["loadavg_1_median"]
            for s in entry["stages"]
            for r in s["by_threads"]
            if r["loadavg_1_median"] is not None
        ]
        if loads:
            out.append(f"  load average (1m) during this table: {min(loads):.2f}-{max(loads):.2f}")
        out.append("")

        # ---- split of the published profile --------------------------------
        published = PUBLISHED_PROFILE_MS.get(size)
        if published:
            out.append(f"  #131 profile split for {size:,} shares")
            out.append(
                f"  {'profile row':<26}{'#131 ms':>9}{'released':>10}"
                f"{'GIL-held':>10}{'parallel':>10}"
            )
            out.append("  " + "-" * 65)
            held_total = 0.0
            parallel_total = 0.0
            for stage in PROFILE_ROW_STAGES:
                stage_row = next(
                    s for s in entry["stages"] if s["key"] == stage.key
                )
                top = next(
                    r for r in stage_row["by_threads"] if r["threads"] == counts[-1]
                )
                fraction = top["released_fraction"] or 0.0
                ms = published[stage.key]
                parallel_ms = ms * fraction
                held_ms = ms - parallel_ms
                held_total += held_ms
                parallel_total += parallel_ms
                out.append(
                    f"  {stage.profile_row:<26}{ms:>9.0f}"
                    f"{fraction * 100:>9.0f}%{held_ms:>10.0f}{parallel_ms:>10.0f}"
                )
            total = held_total + parallel_total
            out.append("  " + "-" * 65)
            out.append(
                f"  {'TOTAL':<26}{total:>9.0f}{'':>10}"
                f"{held_total:>10.0f}{parallel_total:>10.0f}"
            )
            if total:
                out.append(
                    f"  => {parallel_total / total * 100:.0f}% of the profiled "
                    f"time is already running in parallel"
                )
            out.append("")

    peak = results.get("peak_rss_mb")
    if peak:
        out.append(f"peak RSS: {peak:.0f} MB")
    end = results.get("loadavg_end")
    if end:
        out.append("load average at end: " + " / ".join(f"{v:.2f}" for v in end))
    return "\n".join(out)


# ---------------------------------------------------------------------------
# #236 monitor-lateness probe
# ---------------------------------------------------------------------------

LATENCY_PROBE_RATIONALE = (
    "The lease monitor thread must wake on its cadence while the payout "
    "window is being serialized in another thread of the same process. A "
    "single C call (json.dumps/json.loads over a whole 200k-share window) "
    "holds the GIL until it returns, so the monitor's wake is late by the "
    "call's duration; a Python loop of bounded C calls lets the interpreter "
    "switch threads at its normal interval. The probe therefore measures, "
    "for each phase, the maximum and p99 lateness of a thread that sleeps "
    "on WRITER_LEASE_HEARTBEAT_MONITOR_SECONDS and records wake - due. This "
    "is scheduling evidence on one host, not a proof of hard real-time "
    "behaviour, and not a production measurement."
)

PROBE_DEFAULT_SIZES = (210_000, 400_000)
# Distinct payout identities in the probe fixture: the compact payload's
# identity table and the fold's per-identity work depend on it.
PROBE_DEFAULT_MINERS = 200
# The plan's stricter engineering target for monitor wake lateness, giving
# headroom against the configured scheduler slack.
PROBE_STRICT_LATENESS_SECONDS = 0.25
PROBE_CONTROL_SECONDS = 1.0
# Records appended by the advance step of the daemon round trip.
PROBE_ADVANCE_RECORDS = 16


class MonitorLatenessProbe:
    """A thread waking on the lease monitor's cadence, recording lateness.

    Each wake is scheduled ``interval`` after the previous wake (a late
    wake does not owe a burst of catch-up wakes, exactly like a loop that
    sleeps for its interval), and its lateness is ``wake - due``. Samples
    are attributed to whichever phase the main thread has declared current;
    wakes between phases are discarded.
    """

    def __init__(self, interval: float) -> None:
        self._interval = float(interval)
        self._lock = threading.Lock()
        self._phase: str | None = None
        self._samples: dict[str, list[float]] = {}
        # Cyclic-GC pauses attributed to the current phase: (generation,
        # seconds) per collection, from gc.callbacks. A full collection over
        # a large live heap is itself one GIL-held stretch, and is the first
        # suspect whenever a bounded phase still shows a late wake.
        self._gc_pauses: dict[str, list[tuple[int, float]]] = {}
        self._gc_started: float | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="lease-monitor-lateness-probe",
            daemon=True,
        )

    def start(self) -> None:
        gc.callbacks.append(self._gc_callback)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()
        with contextlib.suppress(ValueError):
            gc.callbacks.remove(self._gc_callback)

    def _gc_callback(self, event: str, info: dict[str, Any]) -> None:
        # Runs on whichever thread triggered the collection, under the GIL.
        if event == "start":
            self._gc_started = time.perf_counter()
            return
        started = self._gc_started
        self._gc_started = None
        if started is None:
            return
        pause = time.perf_counter() - started
        with self._lock:
            phase = self._phase
            if phase is not None:
                self._gc_pauses[phase].append((int(info.get("generation", -1)), pause))

    def _run(self) -> None:
        due = time.monotonic() + self._interval
        while not self._stop.is_set():
            delay = due - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            now = time.monotonic()
            late = now - due
            with self._lock:
                phase = self._phase
                if phase is not None:
                    self._samples[phase].append(late)
            due = now + self._interval

    @contextlib.contextmanager
    def phase(self, name: str) -> Iterator[None]:
        with self._lock:
            self._phase = name
            self._samples[name] = []
            self._gc_pauses[name] = []
        try:
            yield
        finally:
            with self._lock:
                self._phase = None

    def summary(self, name: str) -> dict[str, Any]:
        with self._lock:
            samples = list(self._samples.get(name, ()))
            pauses = list(self._gc_pauses.get(name, ()))
        gc_summary = {
            "gc_collections": len(pauses),
            "gc_full_collections": sum(1 for gen, _ in pauses if gen >= 2),
            "gc_max_pause_ms": max((pause for _, pause in pauses), default=0.0) * 1e3,
            "gc_total_pause_ms": sum(pause for _, pause in pauses) * 1e3,
        }
        if not samples:
            return {
                "wakes": 0,
                "max_late_ms": None,
                "p99_late_ms": None,
                "mean_late_ms": None,
                "over_strict": 0,
                "over_slack": 0,
                **gc_summary,
            }
        ordered = sorted(samples)
        p99_index = min(len(ordered) - 1, int(round(0.99 * (len(ordered) - 1))))
        return {
            "wakes": len(samples),
            "max_late_ms": ordered[-1] * 1e3,
            "p99_late_ms": ordered[p99_index] * 1e3,
            "mean_late_ms": sum(samples) / len(samples) * 1e3,
            "over_strict": sum(
                1 for late in samples if late > PROBE_STRICT_LATENESS_SECONDS
            ),
            "over_slack": sum(
                1
                for late in samples
                if late > WRITER_LEASE_HEARTBEAT_SCHEDULER_SLACK_SECONDS
            ),
            **gc_summary,
        }


@dataclass(frozen=True)
class ProbePhaseResult:
    key: str
    kind: str
    note: str
    wall_seconds: float
    cpu_seconds: float
    bytes: int | None
    lateness: dict[str, Any]
    detail: dict[str, float] | None = None

    def as_json(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "kind": self.kind,
            "note": self.note,
            "wall_ms": round(self.wall_seconds * 1e3, 3),
            "cpu_ms": round(self.cpu_seconds * 1e3, 3),
            "bytes": self.bytes,
            "lateness": {
                key: (round(value, 3) if isinstance(value, float) else value)
                for key, value in self.lateness.items()
            },
            "detail_ms": (
                {key: round(value * 1e3, 3) for key, value in self.detail.items()}
                if self.detail
                else None
            ),
        }


class _ProbeRuntime:
    """The slice of the coordinator runtime port prepare_payout_window uses."""

    def __init__(self) -> None:
        self.signing_seed_hex = "42" * 32
        self.ledger_attestation_signing_seed_hex = "43" * 32
        self.bundle_build_timeout_seconds = 600.0
        self._job_build_scheduler_lock = threading.Lock()
        self.job_build_worker_counts = {
            "starts": 0,
            "restarts": 0,
            "crashes": 0,
            "terminations": 0,
        }
        self._job_build_worker_restart_pending = False
        self._tip_refresh_metrics_lock = threading.Lock()
        self.tip_refresh_worker_restarts = 0

    def _ensure_job_cache_state(self) -> None:
        return None


class _ProbeBuildControl:
    """Placeholder build-control type; the probe never registers one."""


def _count_bytes(chunks: Iterator[bytes | str]) -> int:
    total = 0
    for chunk in chunks:
        total += len(chunk)
    return total


def _probe_candidate(shares: list[dict[str, object]]) -> dict[str, Any]:
    """A durable block-candidate intent shaped like block_candidate_intent."""
    return {
        "schema": "qbit.prism.block-candidate-intent.v1",
        "block_hash_hex": "ab" * 32,
        "block_hex": "00" * 256,
        "coinbase_tx_hex": "01" * 128,
        "parent_hash": "cd" * 32,
        "expected_height": 800_001,
        "template": {
            "previousblockhash": "cd" * 32,
            "height": 800_001,
            "coinbasevalue": 50_00000000,
        },
        "shares_json": shares,
        "prior_balances": [],
        "found_block": {
            "block_height": 800_001,
            "coinbase_value_sats": 50_00000000,
            "network_difficulty": 226646186,
            "anchor_job_issued_at_ms": FIXED_NOW_MS,
        },
        "prospective_prior_balances": None,
        "witness_merkle_leaves_hex": [],
        "pending_share": {"share_id": "s", "accepted_at_ms": FIXED_NOW_MS},
        "username": "bench-miner-0",
    }


def _instrument_transport(
    compiler: BundleCompiler,
    timers: dict[str, float],
) -> None:
    """Time the compiler's transport helpers on this instance only."""
    for name in (
        "_serve_builder_write",
        "_serve_builder_read_line",
        "_serve_builder_read_exact",
    ):
        original = getattr(compiler, name)

        def wrapper(
            *args: Any,
            _original: Callable[..., Any] = original,
            _name: str = name,
            **kwargs: Any,
        ) -> Any:
            started = time.perf_counter()
            try:
                return _original(*args, **kwargs)
            finally:
                timers[_name] = timers.get(_name, 0.0) + (
                    time.perf_counter() - started
                )

        setattr(compiler, name, wrapper)


def run_latency_probe(args: argparse.Namespace) -> dict[str, Any]:
    daemon_binary = args.daemon_binary
    if daemon_binary is not None:
        daemon_path = Path(daemon_binary)
        if not (daemon_path.is_file() and os.access(daemon_path, os.X_OK)):
            raise SystemExit(f"--daemon-binary {daemon_binary} is not executable")
    results: dict[str, Any] = {
        "environment": describe_environment(),
        "latency_probe": {
            "monitor_interval_seconds": WRITER_LEASE_HEARTBEAT_MONITOR_SECONDS,
            "scheduler_slack_seconds": WRITER_LEASE_HEARTBEAT_SCHEDULER_SLACK_SECONDS,
            "strict_lateness_seconds": PROBE_STRICT_LATENESS_SECONDS,
            "miners": args.probe_miners,
            "share_id_shape": "production",
            "daemon_binary": daemon_binary,
            "rationale": LATENCY_PROBE_RATIONALE,
            "sizes": [],
        },
    }
    probe = MonitorLatenessProbe(WRITER_LEASE_HEARTBEAT_MONITOR_SECONDS)
    probe.start()
    try:
        for size in args.probe_sizes:
            results["latency_probe"]["sizes"].append(
                _probe_one_size(
                    size,
                    probe=probe,
                    miners=args.probe_miners,
                    daemon_binary=daemon_binary,
                    reps=args.probe_reps,
                )
            )
            gc.collect()
    finally:
        probe.stop()
    results["peak_rss_mb"] = _peak_rss_mb()
    return results


def _probe_one_size(
    size: int,
    *,
    probe: MonitorLatenessProbe,
    miners: int,
    daemon_binary: str | None,
    reps: int,
) -> dict[str, Any]:
    records = build_records(size, miners=miners, share_id_shape="production")
    anchor = int(records[-1].job_issued_at_ms)
    weight = sum(int(record.share_difficulty) for record in records)
    state: dict[str, Any] = {}
    phases: list[ProbePhaseResult] = []

    def measure(
        key: str,
        kind: str,
        note: str,
        run: Callable[[], int | None],
        *,
        detail: dict[str, float] | None = None,
    ) -> None:
        best: ProbePhaseResult | None = None
        for _ in range(max(1, reps)):
            gc.collect()
            with probe.phase(key):
                cpu_started = time.process_time()
                started = time.perf_counter()
                byte_count = run()
                wall = time.perf_counter() - started
                cpu = time.process_time() - cpu_started
            lateness = probe.summary(key)
            result = ProbePhaseResult(
                key=key,
                kind=kind,
                note=note,
                wall_seconds=wall,
                cpu_seconds=cpu,
                bytes=byte_count,
                lateness=lateness,
                detail=dict(detail) if detail else None,
            )
            # Keep the repetition with the worst lateness: the question is
            # whether the monitor can be late, not how fast the best run was.
            if best is None or (
                (result.lateness["max_late_ms"] or 0.0)
                > (best.lateness["max_late_ms"] or 0.0)
            ):
                best = result
        assert best is not None
        phases.append(best)

    # ---- controls ---------------------------------------------------------
    measure(
        "idle_control",
        "control",
        "main thread sleeps; the monitor's own wake jitter on this host",
        lambda: time.sleep(PROBE_CONTROL_SECONDS),
    )

    def busy_python() -> None:
        deadline = time.perf_counter() + PROBE_CONTROL_SECONDS
        total = 0
        while time.perf_counter() < deadline:
            for value in range(10_000):
                total += value
        state["busy_total"] = total

    measure(
        "busy_python_control",
        "control",
        "pure-Python loop; lateness is the interpreter's switch interval",
        busy_python,
    )

    # ---- conversion and fold ---------------------------------------------
    def convert() -> None:
        state["shares"] = [record.to_prism_json() for record in records]

    measure(
        "record_conversion",
        "bounded",
        "AcceptedShareRecord.to_prism_json per record (Python loop)",
        convert,
    )
    shares: list[dict[str, object]] = state["shares"]

    def fold() -> None:
        state["window"] = IncrementalShareWindow.from_full_snapshot(
            records,
            anchor_job_issued_at_ms=anchor,
            window_weight=weight,
        )

    measure(
        "fold_in_process",
        "bounded",
        "IncrementalShareWindow.from_full_snapshot: the in-process fallback "
        "fold, one bounded json.dumps per 512-record page",
        fold,
    )
    window: IncrementalShareWindow = state["window"]
    sequence = window.json_records()
    items = b",".join(
        page.canonical_json_items for page in window.pages if page.canonical_json_items
    )
    digest = sequence.canonical_json_sha256()
    count = window.record_count

    def fold_again_and_release() -> None:
        # A second fold whose result is dropped at once: the release of a
        # whole window's pages (records, dicts, bytes) is what a full rescan
        # or rotation pays when the previous window goes away.
        second = IncrementalShareWindow.from_full_snapshot(
            records,
            anchor_job_issued_at_ms=anchor,
            window_weight=weight,
        )
        state["second_window"] = second

    measure(
        "fold_in_process_keep",
        "bounded",
        "from_full_snapshot with the result retained (no release inside)",
        fold_again_and_release,
    )

    def release_second_window() -> None:
        del state["second_window"]

    measure(
        "fold_release",
        "residual",
        "del of a whole in-process window (pages, records, dicts, bytes)",
        release_second_window,
    )

    # ---- canonical encoding and digest -----------------------------------
    measure(
        "canonical_encode_stream",
        "bounded",
        "iter_canonical_share_item_chunks over the share dicts",
        lambda: _count_bytes(iter_canonical_share_item_chunks(shares)),
    )
    measure(
        "canonical_encode_whole",
        "historical",
        "json.dumps(shares, sort_keys=True, ...) in one call",
        lambda: len(
            json.dumps(
                shares, sort_keys=True, separators=(",", ":"), default=str
            ).encode("utf-8")
        ),
    )
    measure(
        "array_digest_stream",
        "bounded",
        "canonical_share_array_sha256 over a plain list (legacy digest path)",
        lambda: (canonical_share_array_sha256(shares), None)[1],
    )
    measure(
        "array_digest_whole",
        "historical",
        "sha256(json.dumps(shares, sort_keys=True, ...)) in one call",
        lambda: (
            hashlib.sha256(
                json.dumps(
                    shares, sort_keys=True, separators=(",", ":"), default=str
                ).encode("utf-8")
            ).hexdigest(),
            None,
        )[1],
    )

    # ---- compact payload and spool ---------------------------------------
    measure(
        "compact_tail_stream",
        "bounded",
        "_compact_share_tail_chunks: identities + compact rows in batches",
        lambda: _count_bytes(iter(_compact_share_tail_chunks(shares))),
    )

    def compact_whole() -> int:
        identities, compact_shares = _compact_share_payload(shares)
        return len(json.dumps(identities, separators=(",", ":"))) + len(
            json.dumps(compact_shares, separators=(",", ":"))
        )

    measure(
        "compact_tail_whole",
        "historical",
        "_compact_share_payload + two whole json.dumps (old compact_fragments)",
        compact_whole,
    )

    def spool_cold() -> int:
        serialization = _ShareWindowSerialization(
            key=(digest, count, weight),
            share_count=count,
            share_snapshot_sha256=digest,
        )
        lease = serialization.acquire_spooled_tail(shares)
        try:
            return int(lease[1]) if lease is not None else None
        finally:
            serialization.retire_spool()
            if lease is not None:
                serialization.release_spooled_tail()

    measure(
        "spool_write_stream",
        "bounded",
        "_ShareWindowSerialization.acquire_spooled_tail cold (encode + write)",
        spool_cold,
    )

    # ---- daemon prepare request ------------------------------------------
    request_fields: dict[str, object] = {
        "request": "prepare_window",
        "mode": "full",
        "append_invalidation_epoch": 0,
        "anchor_job_issued_at_ms": anchor,
        "records": shares,
        "window_weight": weight,
        "page_size": DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
    }
    measure(
        "prepare_request_stream",
        "bounded",
        "_iter_prepare_window_request_chunks (envelope + record batches)",
        lambda: _count_bytes(_iter_prepare_window_request_chunks(request_fields)),
    )
    measure(
        "prepare_request_whole",
        "historical",
        "json.dumps(request_fields) in one call (old prepare_payout_window)",
        lambda: len(
            json.dumps(request_fields, separators=(",", ":")).encode("utf-8")
        ),
    )

    # ---- mirror validation and lazy parse --------------------------------
    measure(
        "mirror_validate",
        "bounded",
        "DaemonShareWindowMirror.from_full_items: digest + record walk",
        lambda: (
            DaemonShareWindowMirror.from_full_items(
                anchor_job_issued_at_ms=anchor,
                window_weight=weight,
                page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
                record_count=count,
                canonical_items=items,
                share_snapshot_sha256=digest,
            ),
            len(items),
        )[1],
    )
    def parse_stream() -> int:
        state["parsed_stream"] = DaemonShareJsonSequence(items, count)._records()
        return len(items)

    measure(
        "mirror_parse_stream",
        "bounded",
        "DaemonShareJsonSequence._records: raw_decode per record, chunked decode",
        parse_stream,
    )

    def release_parsed_stream() -> None:
        # Dropping the parsed tuple frees every dict and string it holds in
        # one refcount cascade -- a single C call, measured on its own.
        del state["parsed_stream"]

    measure(
        "mirror_parse_release",
        "residual",
        "del of the parsed record tuple (one deallocation cascade)",
        release_parsed_stream,
    )

    def parse_whole() -> int:
        state["parsed_whole"] = json.loads(b"[" + items + b"]")
        return len(items)

    measure(
        "mirror_parse_whole",
        "historical",
        'json.loads(b"[" + items + b"]") in one call (old _records)',
        parse_whole,
    )

    def release_parsed_whole() -> None:
        del state["parsed_whole"]

    measure(
        "mirror_parse_whole_release",
        "residual",
        "del of json.loads' record list (one deallocation cascade)",
        release_parsed_whole,
    )

    # ---- durable candidate identity --------------------------------------
    candidate = _probe_candidate(shares)
    measure(
        "candidate_identity_stream",
        "bounded",
        "block_candidate_identity_sha256 (shares_json streamed)",
        lambda: (block_candidate_identity_sha256(candidate), None)[1],
    )
    measure(
        "candidate_identity_whole",
        "historical",
        "sha256_json_hex(block_candidate_identity(candidate)) in one call",
        lambda: (sha256_json_hex(block_candidate_identity(candidate)), None)[1],
    )

    # ---- one-shot canonical build input ----------------------------------
    payload = {
        "found_block": candidate["found_block"],
        "prior_balances": [],
        "payout_policy": {"policy": "day-one"},
        "coinbase_script_sig_suffix_hex": "00",
        "witness_merkle_leaves_hex": [],
        "shares": shares,
    }
    measure(
        "oneshot_payload_stream",
        "bounded",
        "iter_json_object_text_chunks(payload, array_keys=('shares',))",
        lambda: _count_bytes(
            iter_json_object_text_chunks(payload, array_keys=("shares",))
        ),
    )
    measure(
        "oneshot_payload_whole",
        "historical",
        "json.dumps(payload) in one call (the canonical build's share array)",
        lambda: len(json.dumps(payload, separators=(",", ":"))),
    )

    # ---- real daemon round trip ------------------------------------------
    daemon: dict[str, Any] | None = None
    if daemon_binary is not None:
        daemon = _probe_daemon(
            probe=probe,
            measure=measure,
            records=records,
            shares=shares,
            anchor=anchor,
            weight=weight,
            miners=miners,
            daemon_binary=daemon_binary,
        )

    return {
        "size": size,
        "record_count": count,
        "bytes": {
            "canonical_items": len(items),
            "compact_tail": sum(
                len(chunk) for chunk in _compact_share_tail_chunks(shares)
            ),
        },
        "phases": [phase.as_json() for phase in phases],
        "daemon": daemon,
    }


def _probe_daemon(
    *,
    probe: MonitorLatenessProbe,
    measure: Callable[..., None],
    records: list[AcceptedShareRecord],
    shares: list[dict[str, object]],
    anchor: int,
    weight: int,
    miners: int,
    daemon_binary: str,
) -> dict[str, Any]:
    """The real --serve builder's prepare_window round trip, attributed."""
    runtime = _ProbeRuntime()
    compiler = BundleCompiler(
        runtime,  # type: ignore[arg-type]
        superseded_error=RuntimeError,
        cancellation_error_types=(),
        build_control_type=_ProbeBuildControl,
        tool_command=lambda _name: [daemon_binary],
    )
    timers: dict[str, float] = {}
    _instrument_transport(compiler, timers)
    outcomes: dict[str, Any] = {}
    try:
        # Spawn and handshake outside the measured phases, with a
        # one-record window that does not collide with the real one.
        warm = compiler.prepare_payout_window(
            mode="full",
            records_json=[records[0].to_prism_json()],
            anchor_job_issued_at_ms=anchor,
            append_invalidation_epoch=0,
            window_weight=int(records[0].share_difficulty),
            page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        )
        outcomes["warm_status"] = getattr(warm, "status", None)
        timers.clear()

        def full() -> int:
            outcome = compiler.prepare_payout_window(
                mode="full",
                records_json=shares,
                anchor_job_issued_at_ms=anchor,
                append_invalidation_epoch=0,
                window_weight=weight,
                page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
            )
            outcomes["full"] = outcome
            return len(outcome.window_items or b"") if outcome is not None else None

        measure(
            "daemon_prepare_full",
            "daemon",
            "BundleCompiler.prepare_payout_window(mode='full'): streamed "
            "request, Rust fold, raw response",
            full,
            detail=timers,
        )
        full_detail = dict(timers)
        timers.clear()
        full_outcome = outcomes.get("full")
        status = getattr(full_outcome, "status", None)
        mirror_bytes = None
        if status == "prepared":
            measure(
                "daemon_mirror_validate",
                "bounded",
                "DaemonShareWindowMirror.from_full_items on the daemon's bytes",
                lambda: len(
                    DaemonShareWindowMirror.from_full_items(
                        anchor_job_issued_at_ms=anchor,
                        window_weight=weight,
                        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
                        record_count=full_outcome.record_count,
                        canonical_items=full_outcome.window_items or b"",
                        share_snapshot_sha256=full_outcome.share_snapshot_sha256 or "",
                    ).canonical_items
                ),
            )
            mirror_bytes = len(full_outcome.window_items or b"")
            programs = benchmark_miner_programs(miners)
            last = records[-1]
            delta = []
            for index in range(PROBE_ADVANCE_RECORDS):
                seq = int(last.share_seq) + index + 1
                miner = seq % miners
                delta.append(
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
                        job_issued_at_ms=int(last.job_issued_at_ms) + index + 1,
                        accepted_at_ms=int(last.accepted_at_ms) + index + 1,
                        ntime=int(last.ntime) + index + 1,
                    ).to_prism_json()
                )
            new_anchor = int(last.job_issued_at_ms) + PROBE_ADVANCE_RECORDS + 1

            def advance() -> int:
                outcome = compiler.prepare_payout_window(
                    mode="advance",
                    records_json=delta,
                    anchor_job_issued_at_ms=new_anchor,
                    append_invalidation_epoch=0,
                    base_digest=full_outcome.share_snapshot_sha256,
                )
                outcomes["advance"] = outcome
                return len(outcome.appended_items) if outcome is not None else None

            measure(
                "daemon_prepare_advance",
                "daemon",
                f"prepare_payout_window(mode='advance') with "
                f"{PROBE_ADVANCE_RECORDS} appended records",
                advance,
                detail=timers,
            )
    finally:
        compiler.shutdown_serve_builder()
    return {
        "binary": daemon_binary,
        "warm_status": outcomes.get("warm_status"),
        "full_status": getattr(outcomes.get("full"), "status", None),
        "full_error": getattr(outcomes.get("full"), "error", None),
        "full_transport_ms": {
            key: round(value * 1e3, 3) for key, value in full_detail.items()
        },
        "advance_status": getattr(outcomes.get("advance"), "status", None),
        "advance_stats": (
            {
                "added_rows": outcomes["advance"].added_rows,
                "expired_rows": outcomes["advance"].expired_rows,
                "touched_pages": outcomes["advance"].touched_pages,
            }
            if getattr(outcomes.get("advance"), "status", None) == "prepared"
            else None
        ),
        "mirror_bytes": mirror_bytes,
        "worker_counts": dict(runtime.job_build_worker_counts),
        "serve_counts": dict(compiler.serve_builder_counts),
    }


def render_latency_probe(results: dict[str, Any]) -> str:
    out: list[str] = []
    env = results["environment"]
    probe = results["latency_probe"]
    out.append("=" * 78)
    out.append("#236 monitor-lateness probe: PRISM payout-window serialization phases")
    out.append("=" * 78)
    out.append("")
    out.append(f"CPU              {env['cpu']} x{env['cpu_count']} logical")
    out.append(f"Platform         {env['platform']}")
    out.append(f"Python           {env['python_version'].splitlines()[0]}")
    load = env["loadavg_1_5_15"]
    out.append(
        "Load at start    "
        + ("n/a" if load is None else " / ".join(f"{v:.2f}" for v in load))
    )
    out.append(
        f"Monitor cadence  {probe['monitor_interval_seconds'] * 1e3:.0f} ms "
        f"(WRITER_LEASE_HEARTBEAT_MONITOR_SECONDS); scheduler slack "
        f"{probe['scheduler_slack_seconds'] * 1e3:.0f} ms; strict target "
        f"{probe['strict_lateness_seconds'] * 1e3:.0f} ms"
    )
    out.append(
        f"Fixture          production share_id shape, {probe['miners']} identities"
    )
    out.append(
        "Daemon           "
        + (probe["daemon_binary"] or "not measured (pass --daemon-binary)")
    )
    out.append("")
    out.append("Metric: per phase, wall time of the phase in the main thread and the")
    out.append("lateness (wake - due) of a monitor thread sleeping on the lease")
    out.append("monitor's cadence. 'historical' rows are the whole-window calls the")
    out.append("'bounded' rows replaced; both run the same input in the same process.")
    out.append("'gc full' counts generation-2 cyclic collections during the phase and")
    out.append("'gc max' is the longest single collection pause (ms), from gc.callbacks.")
    out.append("Lateness is what the monitor thread observed on this host, whatever the")
    out.append("cause (a GIL-held C call, a GC pause, or other load on a shared host).")
    out.append("")
    for entry in probe["sizes"]:
        out.append("=" * 78)
        out.append(
            f"WINDOW SIZE: {entry['size']:,} shares "
            f"({entry['record_count']:,} retained)"
        )
        out.append("=" * 78)
        out.append(
            f"  canonical items {entry['bytes']['canonical_items'] / 1e6:.1f} MB, "
            f"compact tail {entry['bytes']['compact_tail'] / 1e6:.1f} MB"
        )
        out.append("")
        head = (
            f"  {'phase':<28}{'kind':<11}{'wall ms':>9}{'cpu ms':>9}{'MB':>8}"
            f"{'wakes':>7}{'max late':>10}{'p99 late':>10}{'>250ms':>8}{'>500ms':>8}"
            f"{'gc full':>8}{'gc max':>8}"
        )
        out.append(head)
        out.append("  " + "-" * (len(head) - 2))
        for phase in entry["phases"]:
            late = phase["lateness"]
            out.append(
                f"  {phase['key']:<28}{phase['kind']:<11}"
                f"{phase['wall_ms']:>9.1f}{phase['cpu_ms']:>9.1f}"
                f"{(phase['bytes'] or 0) / 1e6 if phase['bytes'] else 0:>8.1f}"
                f"{late['wakes']:>7}"
                f"{_fmt(late['max_late_ms'], 1):>10}"
                f"{_fmt(late['p99_late_ms'], 1):>10}"
                f"{late['over_strict']:>8}{late['over_slack']:>8}"
                f"{late.get('gc_full_collections', 0):>8}"
                f"{_fmt(late.get('gc_max_pause_ms'), 1):>8}"
            )
            if phase.get("detail_ms"):
                detail = ", ".join(
                    f"{key.removeprefix('_serve_builder_')}={value:.1f} ms"
                    for key, value in phase["detail_ms"].items()
                )
                out.append(f"  {'':<28}transport: {detail}")
        out.append("")
        daemon = entry.get("daemon")
        if daemon:
            out.append(
                f"  daemon: full={daemon['full_status']} advance={daemon['advance_status']}"
                f" mirror_bytes={daemon['mirror_bytes']}"
                f" advance_stats={daemon['advance_stats']}"
                f" worker_counts={daemon['worker_counts']}"
            )
            if daemon.get("full_error"):
                out.append(f"  daemon error: {daemon['full_error']}")
            out.append("")
    peak = results.get("peak_rss_mb")
    if peak:
        out.append(f"peak RSS: {peak:.0f} MB")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _int_list(text: str) -> list[int]:
    return [int(part) for part in text.replace(",", " ").split()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--json", action="store_true", help="emit results as JSON")
    parser.add_argument(
        "--render",
        metavar="PATH",
        default=None,
        help=(
            "render the tables from a previously saved --json file instead of "
            "measuring. Running the sweep twice to get both output formats "
            "would put two 8-thread processes on the same cores and corrupt "
            "both, so capture --json once and render from it."
        ),
    )
    parser.add_argument(
        "--sizes",
        type=_int_list,
        default=list(DEFAULT_SIZES),
        help="window sizes in shares (default: 21868,100000,400000)",
    )
    parser.add_argument(
        "--threads",
        type=_int_list,
        default=list(DEFAULT_THREADS),
        help="thread counts to sweep (default: 1,2,4,8)",
    )
    parser.add_argument(
        "--large-size-threads",
        type=_int_list,
        default=list(LARGE_SIZE_THREADS),
        help=(
            f"thread counts used for sizes above {LARGE_SIZE_BYTES_THRESHOLD:,} "
            f"shares, where 8 independent inputs cost ~0.7 GB each "
            f"(default: 1,8)"
        ),
    )
    parser.add_argument("--reps", type=int, default=3, help="repetitions (default 3)")
    parser.add_argument(
        "--min-wall-seconds",
        type=float,
        default=1.0,
        help=(
            "wall-clock floor per configuration; below this, thread start/join "
            "biases cores-used downward (default 1.0)"
        ),
    )
    parser.add_argument(
        "--max-wall-seconds",
        type=float,
        default=12.0,
        help=(
            "bound on a single configuration's wall clock; a fully GIL-held "
            "stage serializes, so its wall grows with thread count (default 12)"
        ),
    )
    parser.add_argument(
        "--control-floor-fraction",
        type=float,
        default=0.75,
        help=(
            "the positive control must reach this fraction of the highest "
            "thread count in cores-used, else the run aborts without "
            "reporting stages (default 0.75, i.e. 6.0 cores on an 8-thread "
            "sweep; #143 published 0.986)"
        ),
    )
    parser.add_argument(
        "--miners",
        type=int,
        default=BENCHMARK_MINERS,
        help=f"distinct payout identities (default {BENCHMARK_MINERS})",
    )
    parser.add_argument(
        "--share-id-shape",
        choices=("benchmark", "production"),
        default="benchmark",
        help=(
            "benchmark: job_build_benchmark's short synthetic share_id; "
            "production: the username:block_hash_hex form share_writer builds"
        ),
    )
    parser.add_argument(
        "--latency-probe",
        action="store_true",
        help=(
            "#236 mode: run each serialization phase once per size in the "
            "main thread while a monitor thread on the lease monitor's "
            "cadence records its wake lateness; see LATENCY_PROBE_RATIONALE"
        ),
    )
    parser.add_argument(
        "--probe-sizes",
        type=_int_list,
        default=list(PROBE_DEFAULT_SIZES),
        help="--latency-probe window sizes in shares (default: 210000,400000)",
    )
    parser.add_argument(
        "--probe-reps",
        type=int,
        default=1,
        help=(
            "--latency-probe repetitions per phase; the repetition with the "
            "worst monitor lateness is reported (default 1)"
        ),
    )
    parser.add_argument(
        "--probe-miners",
        type=int,
        default=PROBE_DEFAULT_MINERS,
        help=f"--latency-probe distinct payout identities (default {PROBE_DEFAULT_MINERS})",
    )
    parser.add_argument(
        "--daemon-binary",
        default=None,
        help=(
            "--latency-probe: path to a prebuilt qbit-prism-build-audit-bundle; "
            "adds the real --serve daemon's prepare round trip"
        ),
    )
    args = parser.parse_args(argv)

    if args.render:
        with open(args.render, encoding="utf-8") as handle:
            loaded = json.load(handle)
        sys.stdout.write(
            render_latency_probe(loaded)
            if "latency_probe" in loaded
            else render_report(loaded)
        )
        sys.stdout.write("\n")
        return 0

    if args.latency_probe:
        results = run_latency_probe(args)
        if args.json:
            json.dump(results, sys.stdout, indent=2, default=str)
        else:
            sys.stdout.write(render_latency_probe(results))
        sys.stdout.write("\n")
        return 0

    results = run(args)
    if args.json:
        json.dump(results, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(render_report(results))
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
