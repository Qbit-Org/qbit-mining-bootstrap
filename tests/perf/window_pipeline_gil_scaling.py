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
``spool_compact``            ``_ShareWindowSerialization.compact_fragments``
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
"""

from __future__ import annotations

import argparse
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
from typing import Any, Callable, Sequence

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism.bundle_compiler import (
    _compact_share_payload,
    _ShareWindowSerialization,
)
from lab.prism.share_ledger import (
    DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
    AcceptedShareRecord,
    IncrementalShareWindow,
    _IncrementalShareWindowPage,
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

    def run() -> tuple[str, str]:
        return _new_serialization(inputs).compact_fragments(shares)

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
        "_ShareWindowSerialization.compact_fragments (cold)",
        None,
        "spool_acquire",
        "_compact_share_payload + two json.dumps",
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
    args = parser.parse_args(argv)

    if args.render:
        with open(args.render, encoding="utf-8") as handle:
            sys.stdout.write(render_report(json.load(handle)))
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
