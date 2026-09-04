"""Cheap, always-on interpreter and allocator telemetry for ``/metrics``.

Issue #226: the production coordinator's resident set grows by roughly
390 MB per hour and nothing in the tree can say whether that growth is live
Python objects, pymalloc fragmentation, or glibc per-thread arena
fragmentation across the process's 64 threads. This module collects the
fixed-cardinality, constant-cost signals that discriminate between those
hypotheses on a running coordinator:

- ``sys.getallocatedblocks()`` -- the live allocation count. Rising with RSS
  means retention; flat while RSS rises means fragmentation.
- ``gc.get_count()`` and ``gc.get_stats()`` -- collector pressure and the
  uncollectable count, per generation. ``get_count()`` returns the collector's
  *trigger* counters (generation 0 is allocations minus deallocations), which
  it compares against ``gc.get_threshold()``; they are not a count of retained
  objects and they reset on every collection. Only ``get_stats()``'s
  ``uncollectable`` counts objects.
- ``threading.active_count()`` -- every thread is a candidate glibc arena.
- glibc ``mallinfo2`` -- arena, in-use, free, and mmapped bytes across every
  arena, so arena-minus-in-use is the fragmentation glibc has not returned.

Everything the scrape collector reads is a snapshot. It never walks the
heap: no ``gc.get_objects()``, no ``tracemalloc``. It runs on every scrape.

``mallinfo2`` exists only in glibc 2.33 and later. musl and macOS have no
such symbol, and the older int-typed ``mallinfo`` is deliberately not used
as a fallback because its 32-bit fields wrap on a multi-gigabyte heap. The
binding is attempted once per collector, and an absent symbol is remembered
so a scrape never repeats the lookup and never logs. Absence renders as a
separate availability gauge at 0 with every byte gauge at -1; it never
renders as zero bytes, which an operator would read as an empty heap.

The second half of this module (issue #226 part 2) is the part that *does*
walk the heap, and it is built so that it can only ever do so on purpose:

- :class:`HeapCensus` takes a bounded type histogram over
  ``gc.get_objects()`` and, when ``tracemalloc`` is tracing, a top-N of
  allocation sites, and writes the result to a file. It is off unless
  ``PRISM_HEAP_CENSUS=1``, it is triggered only by an operator signal
  (``SIGUSR1``), it runs on its own thread (the signal handler only writes
  one byte to a pipe), and it is capped on top-N, on output bytes, on walk
  seconds, and on files kept. No HTTP route, Stratum message, or metrics
  scrape can reach it.
- :class:`MallocTrimmer` calls glibc ``malloc_trim(0)`` and reports the
  allocator's before/after readings through :class:`ProcessHeapTelemetry`.
  ``malloc_trim`` takes every arena's lock in turn while it walks that
  arena's free lists, so a thread allocating in that arena at that moment
  waits; on a fragmented multi-gigabyte heap that wait is a stall of tens
  to hundreds of milliseconds, not a wedge. It is exposed on ``SIGRTMIN+1``
  when ``PRISM_MALLOC_TRIM_SIGNAL=1`` and as a paced periodic action when
  ``PRISM_MALLOC_TRIM_INTERVAL_SECONDS`` is set, never below the floor.
- :func:`evaluate_rss_bound` is the automated soak check: a resident-set
  series must stay within a stated multiple of its post-warm-up baseline.
  ``python3 -m lab.prism.process_telemetry rss-bound`` runs it.
"""

from __future__ import annotations

import argparse
import ctypes
import datetime as _datetime
import gc
import json
import os
import re
import select
import sys
import tempfile
import threading
import time
import tracemalloc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

# Closed label set. CPython reports three entries from both gc.get_count()
# and gc.get_stats(); an interpreter reporting fewer renders -1 for the
# missing generation rather than growing or shrinking the series set. Since
# CPython 3.13 the collector is incremental and the third threshold is
# unused, so get_count()'s third entry reads 0 on the shipped interpreter.
# The series is kept regardless: a fixed family set across interpreter
# versions is worth more than dropping a known-zero gauge.
PRISM_GC_GENERATIONS = ("0", "1", "2")

# Field order of ``struct mallinfo2`` (glibc >= 2.33); every field is size_t.
_MALLINFO2_FIELDS = (
    "arena",
    "ordblks",
    "smblks",
    "hblks",
    "hblkhd",
    "usmblks",
    "fsmblks",
    "uordblks",
    "fordblks",
    "keepcost",
)


class MallInfo2(ctypes.Structure):
    """``struct mallinfo2`` as returned by glibc's ``mallinfo2()``."""

    _fields_ = [(name, ctypes.c_size_t) for name in _MALLINFO2_FIELDS]


def resolve_mallinfo2() -> Callable[[], MallInfo2]:
    """Bind glibc's ``mallinfo2`` through ctypes, or raise.

    Looks the symbol up in the running program's global namespace, which
    is where the libc Python itself links resolves. Raises ``AttributeError``
    where the symbol is absent (musl, macOS, glibc before 2.33) and whatever
    ``ctypes`` raises where the global handle cannot be opened at all. The
    caller treats every exception as "unavailable".
    """
    libc = ctypes.CDLL(None)
    function = libc.mallinfo2
    function.restype = MallInfo2
    function.argtypes = ()
    return function


@dataclass(frozen=True)
class ProcessHeapSample:
    """One scrape's worth of interpreter and allocator readings."""

    allocated_blocks: int
    gc_trigger_count: tuple[int, int, int]
    gc_collections: tuple[int, int, int]
    gc_collected: tuple[int, int, int]
    gc_uncollectable: tuple[int, int, int]
    threads: int
    malloc_info_available: bool
    # The four byte gauges are -1, never 0, while malloc_info_available is
    # False: a zero arena on a 17 GB process is a lie an operator would act on.
    malloc_arena_bytes: int
    malloc_in_use_bytes: int
    malloc_free_bytes: int
    malloc_mmapped_bytes: int


def _per_generation(values: tuple[int, ...] | list[int]) -> tuple[int, int, int]:
    """Pin a per-generation reading to the closed three-generation set."""
    padded = [int(value) for value in values[: len(PRISM_GC_GENERATIONS)]]
    while len(padded) < len(PRISM_GC_GENERATIONS):
        padded.append(-1)
    return (padded[0], padded[1], padded[2])


_UNRESOLVED = object()


class ProcessHeapTelemetry:
    """Collector for :class:`ProcessHeapSample`; one per renderer.

    ``malloc_enabled`` is the ``PRISM_MALLOC_TELEMETRY`` switch: off skips the
    ``mallinfo2`` call entirely (it walks glibc's free lists under each arena
    lock, which is cheap on a healthy heap but is the one reading here that
    is not a plain interpreter counter) and renders the allocator gauges as
    unavailable. The interpreter readings have no switch; they are pure
    counter loads and are always on.
    """

    def __init__(
        self,
        *,
        malloc_enabled: bool = True,
        mallinfo_resolver: Callable[[], Callable[[], MallInfo2]] = resolve_mallinfo2,
    ) -> None:
        self._malloc_enabled = bool(malloc_enabled)
        self._mallinfo_resolver = mallinfo_resolver
        # Resolved exactly once: the bound function, or None once the symbol
        # has proven absent or broken. Never re-probed and never logged, so
        # a platform without mallinfo2 costs nothing per scrape.
        self._mallinfo2: object = _UNRESOLVED
        self._resolve_lock = threading.Lock()

    @property
    def malloc_enabled(self) -> bool:
        return self._malloc_enabled

    def _resolved_mallinfo2(self) -> Callable[[], MallInfo2] | None:
        if not self._malloc_enabled:
            return None
        resolved = self._mallinfo2
        if resolved is _UNRESOLVED:
            with self._resolve_lock:
                resolved = self._mallinfo2
                if resolved is _UNRESOLVED:
                    try:
                        resolved = self._mallinfo_resolver()
                    except Exception:
                        # Absent symbol, unsupported loader, or anything
                        # else the C boundary raises: unavailable, for good.
                        resolved = None
                    self._mallinfo2 = resolved
        return resolved  # type: ignore[return-value]

    def sample(self) -> ProcessHeapSample:
        stats = gc.get_stats()
        arena = in_use = free = mmapped = -1
        available = False
        mallinfo2 = self._resolved_mallinfo2()
        if mallinfo2 is not None:
            try:
                info = mallinfo2()
            except Exception:
                # A symbol that binds but fails at call time is treated the
                # same as an absent one from now on: no per-scrape retry.
                self._mallinfo2 = None
            else:
                available = True
                arena = int(info.arena)
                in_use = int(info.uordblks)
                free = int(info.fordblks)
                mmapped = int(info.hblkhd)
        return ProcessHeapSample(
            allocated_blocks=int(sys.getallocatedblocks()),
            gc_trigger_count=_per_generation(gc.get_count()),
            gc_collections=_per_generation(
                [int(entry.get("collections", -1)) for entry in stats]
            ),
            gc_collected=_per_generation(
                [int(entry.get("collected", -1)) for entry in stats]
            ),
            gc_uncollectable=_per_generation(
                [int(entry.get("uncollectable", -1)) for entry in stats]
            ),
            threads=int(threading.active_count()),
            malloc_info_available=available,
            malloc_arena_bytes=arena,
            malloc_in_use_bytes=in_use,
            malloc_free_bytes=free,
            malloc_mmapped_bytes=mmapped,
        )


# ---------------------------------------------------------------------------
# Resident set and address-space readings (census, trim, and the storm
# instrument; never the scrape, which has its own reader on the coordinator).
# ---------------------------------------------------------------------------


def read_resident_memory_bytes(pid: int | str = "self") -> int:
    """RSS bytes from ``/proc/<pid>/statm``, or -1 where that is unavailable."""
    try:
        fields = Path(f"/proc/{pid}/statm").read_text(encoding="ascii").split()
        return int(fields[1]) * int(os.sysconf("SC_PAGE_SIZE"))
    except (OSError, ValueError, IndexError, AttributeError):
        return -1


_MIB = 1024 * 1024
_GIB = 1024 * _MIB

# The size bands the #226 capture was reported in: 658 of the 943 anonymous
# regions on union-mainnet fell in the 4-64 MiB band, which is the glibc
# per-thread heap (HEAP_MAX_SIZE = 64 MiB) signature.
ANONYMOUS_REGION_BANDS: tuple[tuple[str, int, int | None], ...] = (
    ("under_4mib", 0, 4 * _MIB),
    ("4mib_to_64mib", 4 * _MIB, 64 * _MIB),
    ("64mib_to_1gib", 64 * _MIB, _GIB),
    ("over_1gib", _GIB, None),
)


@dataclass(frozen=True)
class AnonymousMapShape:
    """Private writable anonymous regions of a process, by size band.

    ``heap_segment_bytes`` is the size of the ``[heap]`` (brk) segment, which
    is glibc's main arena; the per-thread arenas are the bare anonymous
    regions. Guard pages (``---p``), file mappings, and the stack are not
    counted. ``available`` is False where ``/proc/<pid>/maps`` cannot be read
    and every count is then -1.
    """

    available: bool
    regions: int
    total_bytes: int
    heap_segment_bytes: int
    band_regions: dict[str, int] = field(default_factory=dict)
    band_bytes: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "regions": self.regions,
            "total_bytes": self.total_bytes,
            "heap_segment_bytes": self.heap_segment_bytes,
            "band_regions": dict(self.band_regions),
            "band_bytes": dict(self.band_bytes),
        }


def read_anonymous_map_shape(pid: int | str = "self") -> AnonymousMapShape:
    """Histogram the private writable anonymous regions of ``/proc/<pid>/maps``."""
    try:
        text = Path(f"/proc/{pid}/maps").read_text(encoding="ascii", errors="replace")
    except OSError:
        return AnonymousMapShape(
            available=False,
            regions=-1,
            total_bytes=-1,
            heap_segment_bytes=-1,
        )
    band_regions = {name: 0 for name, _low, _high in ANONYMOUS_REGION_BANDS}
    band_bytes = {name: 0 for name, _low, _high in ANONYMOUS_REGION_BANDS}
    regions = 0
    total = 0
    heap_segment = 0
    for line in text.splitlines():
        parts = line.split(None, 5)
        if len(parts) < 5:
            continue
        span, perms = parts[0], parts[1]
        pathname = parts[5].strip() if len(parts) > 5 else ""
        if not perms.startswith("rw") or "p" not in perms:
            continue
        if pathname and pathname != "[heap]":
            continue
        try:
            start_text, end_text = span.split("-", 1)
            size = int(end_text, 16) - int(start_text, 16)
        except ValueError:
            continue
        if pathname == "[heap]":
            heap_segment += size
        regions += 1
        total += size
        for name, low, high in ANONYMOUS_REGION_BANDS:
            if size >= low and (high is None or size < high):
                band_regions[name] += 1
                band_bytes[name] += size
                break
    return AnonymousMapShape(
        available=True,
        regions=regions,
        total_bytes=total,
        heap_segment_bytes=heap_segment,
        band_regions=band_regions,
        band_bytes=band_bytes,
    )


@dataclass(frozen=True)
class MallocInfoSummary:
    """glibc ``malloc_info`` folded across every arena.

    ``arenas`` is the arena count (main plus every per-thread arena ever
    created; arenas outlive their threads). The free bytes are split by
    where they sit, because that decides what ``malloc_trim`` can do:
    ``bin_bytes`` are interior free chunks, which ``malloc_trim`` returns to
    the kernel with ``madvise`` in every arena; ``top_bytes`` are the free
    space at the end of each arena's heaps, which ``malloc_trim`` returns
    only for the main arena -- a per-thread heap's top shrinks only when the
    thread that owns the arena frees a chunk of 64 KiB or more while the top
    exceeds ``M_TRIM_THRESHOLD``. All -1 when ``available`` is False.
    """

    available: bool
    arenas: int
    top_bytes: int
    bin_bytes: int
    fast_bytes: int
    system_bytes: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "arenas": self.arenas,
            "top_bytes": self.top_bytes,
            "bin_bytes": self.bin_bytes,
            "fast_bytes": self.fast_bytes,
            "system_bytes": self.system_bytes,
        }


_MALLOC_INFO_UNAVAILABLE = MallocInfoSummary(
    available=False,
    arenas=-1,
    top_bytes=-1,
    bin_bytes=-1,
    fast_bytes=-1,
    system_bytes=-1,
)
_HEAP_ELEMENT = re.compile(r'<heap nr="\d+">(.*?)</heap>', re.S)
_REST_TOTAL = re.compile(r'<total type="rest" count="\d+" size="(\d+)"/>')
_FAST_TOTAL = re.compile(r'<total type="fast" count="\d+" size="(\d+)"/>')
_SYSTEM_CURRENT = re.compile(r'<system type="current" size="(\d+)"/>')
_SIZE_ROW = re.compile(r'<size from="\d+" to="\d+" total="(\d+)" count="\d+"/>')
_UNSORTED_ROW = re.compile(r'<unsorted from="\d+" to="\d+" total="(\d+)" count="\d+"/>')


def _first_int(pattern: re.Pattern[str], body: str) -> int:
    match = pattern.search(body)
    return int(match.group(1)) if match else 0


def parse_malloc_info_xml(text: str) -> MallocInfoSummary:
    """Fold one ``malloc_info`` document into :class:`MallocInfoSummary`.

    Per ``<heap>``: glibc prints one ``<size>`` row per non-empty size class
    -- the fastbin classes first, then the small and large bins -- and the
    unsorted list as its own ``<unsorted>`` row. ``<total type="fast">`` is
    exactly the sum of the fastbin rows, and ``<total type="rest">`` is the
    bins (small, large, unsorted) plus the top chunk; fastbins are not part
    of ``rest``. So the bin bytes are all rows plus unsorted minus the fast
    total, and top is ``rest`` minus that. Summing the fastbin rows into the
    bins would understate top by the fast total, and top is the number an
    operator reads to decide whether ``malloc_trim`` can help.
    """
    arenas = top = bins = fast = system = 0
    for heap in _HEAP_ELEMENT.finditer(text):
        body = heap.group(1)
        arenas += 1
        rest = _first_int(_REST_TOTAL, body)
        fast_here = _first_int(_FAST_TOTAL, body)
        rows = sum(int(total) for total in _SIZE_ROW.findall(body)) + sum(
            int(total) for total in _UNSORTED_ROW.findall(body)
        )
        in_bins = max(0, rows - fast_here)
        bins += in_bins
        top += max(0, rest - in_bins)
        fast += fast_here
        system += _first_int(_SYSTEM_CURRENT, body)
    return MallocInfoSummary(
        available=True,
        arenas=arenas,
        top_bytes=top,
        bin_bytes=bins,
        fast_bytes=fast,
        system_bytes=system,
    )


def read_malloc_info_summary() -> MallocInfoSummary:
    """Fold glibc ``malloc_info`` across arenas, or the unavailable sentinel.

    ``malloc_info`` writes one ``<heap nr="...">`` element per arena. Like
    ``mallinfo2`` it walks every arena under its lock -- and unlike it, it
    prints a row per size class per arena -- so it is a census and
    storm-instrument reading, never a scrape reading. The XML goes through a
    temporary file and the C ``FILE*`` API because that is the only
    interface glibc offers for it; :func:`parse_malloc_info_xml` does the
    folding.
    """
    try:
        libc = ctypes.CDLL(None)
        malloc_info = libc.malloc_info
        fopen = libc.fopen
        fclose = libc.fclose
    except (OSError, AttributeError):
        return _MALLOC_INFO_UNAVAILABLE
    malloc_info.restype = ctypes.c_int
    malloc_info.argtypes = (ctypes.c_int, ctypes.c_void_p)
    fopen.restype = ctypes.c_void_p
    fopen.argtypes = (ctypes.c_char_p, ctypes.c_char_p)
    fclose.restype = ctypes.c_int
    fclose.argtypes = (ctypes.c_void_p,)
    try:
        with tempfile.NamedTemporaryFile(prefix="prism-malloc-info-", suffix=".xml") as spool:
            handle = fopen(spool.name.encode(), b"w")
            if not handle:
                return _MALLOC_INFO_UNAVAILABLE
            try:
                if malloc_info(0, handle) != 0:
                    return _MALLOC_INFO_UNAVAILABLE
            finally:
                fclose(handle)
            text = Path(spool.name).read_text(encoding="ascii", errors="replace")
    except (OSError, ValueError):
        return _MALLOC_INFO_UNAVAILABLE
    return parse_malloc_info_xml(text)


def read_malloc_arena_count() -> int:
    """Count glibc malloc arenas through ``malloc_info``, or -1 when unavailable."""
    return read_malloc_info_summary().arenas


# ---------------------------------------------------------------------------
# Issue #226 part 2: the operator-triggered bounded heap census.
# ---------------------------------------------------------------------------

HEAP_CENSUS_FORMAT = "qbit-prism-heap-census/v1"
# Files kept per output directory. Each is capped at the configured byte
# ceiling, so the disk footprint is bounded without a further knob.
HEAP_CENSUS_RETAINED_FILES = 32
HEAP_CENSUS_FILE_PREFIX = "heap-census-"
HEAP_CENSUS_FILE_SUFFIX = ".json"
# Width of the per-process sequence in a report's file name; a million
# censuses at the 60 s floor is almost two years of one process.
HEAP_CENSUS_SEQUENCE_WIDTH = 6
# The walk checks the clock every this many objects. 1024 objects is a few
# hundred microseconds, so the walk cap is honoured to well under a
# millisecond without paying a clock read per object.
_HEAP_CENSUS_CLOCK_STRIDE = 1024
_HEAP_CENSUS_TYPE_NAME_LIMIT = 120
_HEAP_CENSUS_SITE_LIMIT = 240
# tracemalloc frame depth. One frame groups by file:line, which is what a
# top-N by site needs; deeper traces multiply the standing cost of tracing.
HEAP_CENSUS_TRACEMALLOC_FRAMES = 1
# The signals. SIGUSR2 belongs to faulthandler (prism_coordinator.main);
# SIGUSR1 is free. The trim action takes the first real-time signal above
# SIGRTMIN so it cannot be confused with the census.
HEAP_CENSUS_SIGNAL_NAME = "SIGUSR1"
MALLOC_TRIM_SIGNAL_NAME = "SIGRTMIN+1"


@dataclass(frozen=True)
class HeapCensusConfig:
    """The validated census, trim, and tracing switches and bounds.

    Built by ``lab.prism.coordinator_config.env_heap_census_config`` from the
    ``PRISM_HEAP_CENSUS*`` and ``PRISM_MALLOC_TRIM*`` environment, which is
    also where the defaults and their ceilings live. Every field is
    deliberate: there is no default here, so a caller cannot construct an
    enabled census without choosing its bounds.
    """

    enabled: bool
    output_dir: str
    top_n: int
    max_bytes: int
    max_seconds: float
    min_interval_seconds: float
    tracemalloc_enabled: bool
    malloc_trim_signal_enabled: bool
    malloc_trim_interval_seconds: float

    @property
    def anything_armed(self) -> bool:
        return bool(
            self.enabled
            or self.malloc_trim_signal_enabled
            or self.malloc_trim_interval_seconds > 0
        )


def _truncate(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _type_name(kind: type) -> str:
    module = getattr(kind, "__module__", None) or "?"
    qualname = getattr(kind, "__qualname__", None) or getattr(kind, "__name__", "?")
    return _truncate(f"{module}.{qualname}", _HEAP_CENSUS_TYPE_NAME_LIMIT)


def _utc_stamp(now: float) -> str:
    return (
        _datetime.datetime.fromtimestamp(now, tz=_datetime.timezone.utc)
        .strftime("%Y%m%dT%H%M%SZ")
    )


def _log_line(message: str) -> None:
    print(message, flush=True)


@dataclass(frozen=True)
class HeapCensusOutcome:
    """What one census produced: where it went and the bounds it met."""

    path: str
    bytes_written: int
    tracked_objects: int
    objects_walked: int
    walk_truncated: bool
    top_n_written: int
    seconds: float
    tracemalloc_traced: bool


class HeapCensus:
    """The bounded heap walk, and the file it writes.

    :meth:`take` is synchronous and is the only method that touches the heap.
    It is what the census thread calls, what a test calls directly, and it
    refuses to do anything at all when the configuration is not enabled --
    the entry point is inert, not merely unregistered, when the switch is
    off.

    The walk is ``gc.get_objects()`` followed by one pass over the returned
    list taking ``type()`` and ``sys.getsizeof()`` of each object. The
    ``gc.get_objects()`` call itself is a single C step that allocates one
    pointer per tracked object and holds the GIL for its duration; nothing
    can interrupt it, and on a heap of tens of millions of tracked objects
    it is a stall of seconds on every thread. The walk cap applies to the
    pass that follows: the clock is checked every
    ``_HEAP_CENSUS_CLOCK_STRIDE`` objects and the pass stops, marking the
    report truncated, when ``max_seconds`` is used up. That is why the
    census runs only on an operator's signal, on its own thread, and never
    on a request path.
    """

    def __init__(
        self,
        config: HeapCensusConfig,
        *,
        telemetry: ProcessHeapTelemetry | None = None,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        log: Callable[[str], None] = _log_line,
        malloc_info_reader: Callable[[], MallocInfoSummary] = read_malloc_info_summary,
    ) -> None:
        self._config = config
        self._telemetry = telemetry if telemetry is not None else ProcessHeapTelemetry()
        self._clock = clock
        self._wall_clock = wall_clock
        self._log = log
        self._malloc_info_reader = malloc_info_reader
        self._lock = threading.Lock()
        self._sequence = 0
        self.last_outcome: HeapCensusOutcome | None = None

    @property
    def config(self) -> HeapCensusConfig:
        return self._config

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    def take(self, reason: str) -> HeapCensusOutcome | None:
        """Walk the heap once, bounded, and write the report; None when disabled.

        Serialized: a second caller while one census is running waits for it
        rather than starting another walk. Never raises past this frame --
        a census that fails is logged and returns None, because the census
        thread must survive its own diagnostics.
        """
        if not self._config.enabled:
            return None
        with self._lock:
            try:
                outcome = self._take_locked(reason)
            except Exception as exc:  # pragma: no cover - defensive; logged
                self._log(f"prism heap census failed reason={reason} error={exc!r}")
                return None
            self.last_outcome = outcome
            return outcome

    def _take_locked(self, reason: str) -> HeapCensusOutcome:
        config = self._config
        started = self._clock()
        taken_at = self._wall_clock()
        deadline = started + config.max_seconds
        before = self._telemetry.sample()
        resident_before = read_resident_memory_bytes()
        malloc_info = self._malloc_info_reader()
        map_shape = read_anonymous_map_shape()
        get_objects_started = self._clock()
        objects = gc.get_objects()
        get_objects_seconds = self._clock() - get_objects_started
        tracked = len(objects)
        histogram, walked, truncated, distinct_type_objects = self._walk_types(
            objects, deadline
        )
        del objects
        walk_seconds = self._clock() - started
        traced = self._tracemalloc_section(config.top_n)
        by_count = sorted(histogram.items(), key=lambda item: (-item[1][0], item[0]))
        by_bytes = sorted(histogram.items(), key=lambda item: (-item[1][1], item[0]))
        report: dict[str, Any] = {
            "format": HEAP_CENSUS_FORMAT,
            "reason": _truncate(str(reason), 80),
            "taken_at": _datetime.datetime.fromtimestamp(
                taken_at, tz=_datetime.timezone.utc
            ).isoformat(),
            "pid": os.getpid(),
            "python": sys.version.split()[0],
            "platform": sys.platform,
            "bounds": {
                "top_n": config.top_n,
                "max_bytes": config.max_bytes,
                "max_seconds": config.max_seconds,
            },
            "walk": {
                "tracked_objects": tracked,
                "objects_walked": walked,
                # Type objects seen, and the printed names they merged into
                # (fewer when two types share a name).
                "distinct_types": distinct_type_objects,
                "distinct_type_names": len(histogram),
                "truncated": truncated,
                "get_objects_seconds": round(get_objects_seconds, 6),
                "seconds": round(walk_seconds, 6),
            },
            "process": {
                "resident_memory_bytes": resident_before,
                "allocated_blocks": before.allocated_blocks,
                "threads": before.threads,
                "gc_trigger_count": list(before.gc_trigger_count),
                "gc_collections": list(before.gc_collections),
                "gc_collected": list(before.gc_collected),
                "gc_uncollectable": list(before.gc_uncollectable),
                "malloc_info_available": before.malloc_info_available,
                "malloc_arena_bytes": before.malloc_arena_bytes,
                "malloc_in_use_bytes": before.malloc_in_use_bytes,
                "malloc_free_bytes": before.malloc_free_bytes,
                "malloc_mmapped_bytes": before.malloc_mmapped_bytes,
                "malloc_arena_count": malloc_info.arenas,
                # Where the free bytes sit decides what malloc_trim can do:
                # bins it returns everywhere, per-thread heap tops it cannot.
                "malloc_free_top_bytes": malloc_info.top_bytes,
                "malloc_free_bin_bytes": malloc_info.bin_bytes,
                "malloc_free_fast_bytes": malloc_info.fast_bytes,
                "anonymous_map": map_shape.as_dict(),
            },
            "tracemalloc": traced,
        }
        payload, top_n_written = self._fit_payload(report, by_count, by_bytes, config)
        path = self._write(payload, taken_at)
        self._retain_files(Path(config.output_dir))
        outcome = HeapCensusOutcome(
            path=str(path),
            bytes_written=len(payload),
            tracked_objects=tracked,
            objects_walked=walked,
            walk_truncated=truncated,
            top_n_written=top_n_written,
            seconds=self._clock() - started,
            tracemalloc_traced=bool(traced.get("tracing")),
        )
        self._log(
            "prism heap census written "
            f"reason={report['reason']} path={outcome.path} bytes={outcome.bytes_written} "
            f"tracked_objects={tracked} walked={walked} truncated={truncated} "
            f"top_n={top_n_written} seconds={outcome.seconds:.3f} "
            f"tracemalloc={'on' if outcome.tracemalloc_traced else 'off'}"
        )
        return outcome

    def _walk_types(
        self,
        objects: list[Any],
        deadline: float,
    ) -> tuple[dict[str, list[int]], int, bool, int]:
        histogram: dict[type, list[int]] = {}
        getsizeof = sys.getsizeof
        clock = self._clock
        walked = 0
        truncated = False
        for index, obj in enumerate(objects):
            if index % _HEAP_CENSUS_CLOCK_STRIDE == 0 and index and clock() > deadline:
                truncated = True
                break
            kind = type(obj)
            entry = histogram.get(kind)
            if entry is None:
                entry = histogram[kind] = [0, 0]
            entry[0] += 1
            try:
                entry[1] += getsizeof(obj)
            except Exception:
                # A __sizeof__ that raises is not the census's problem; the
                # count still stands.
                pass
            walked += 1
        # Two distinct type objects can share a printed name (a class defined
        # twice, or two long names that truncate alike). Merge them rather
        # than let one overwrite the other: a census that drops a retained
        # type is worse than no census, and this is the instrument for
        # finding the leak.
        named: dict[str, list[int]] = {}
        for kind, entry in histogram.items():
            name = _type_name(kind)
            merged = named.get(name)
            if merged is None:
                named[name] = [entry[0], entry[1]]
            else:
                merged[0] += entry[0]
                merged[1] += entry[1]
        return named, walked, truncated, len(histogram)

    def _tracemalloc_section(self, top_n: int) -> dict[str, Any]:
        if not tracemalloc.is_tracing():
            return {"tracing": False}
        traced_now, traced_peak = tracemalloc.get_traced_memory()
        snapshot = tracemalloc.take_snapshot().filter_traces(
            (tracemalloc.Filter(False, tracemalloc.__file__),)
        )
        by_line = [
            {
                "site": _truncate(str(stat.traceback), _HEAP_CENSUS_SITE_LIMIT),
                "bytes": int(stat.size),
                "count": int(stat.count),
            }
            for stat in snapshot.statistics("lineno")[:top_n]
        ]
        by_file = [
            {
                "site": _truncate(str(stat.traceback), _HEAP_CENSUS_SITE_LIMIT),
                "bytes": int(stat.size),
                "count": int(stat.count),
            }
            for stat in snapshot.statistics("filename")[:top_n]
        ]
        del snapshot
        return {
            "tracing": True,
            "frames": tracemalloc.get_traceback_limit(),
            "traced_bytes": int(traced_now),
            "traced_peak_bytes": int(traced_peak),
            "by_line": by_line,
            "by_file": by_file,
        }

    @staticmethod
    def _fit_payload(
        report: dict[str, Any],
        by_count: list[tuple[str, list[int]]],
        by_bytes: list[tuple[str, list[int]]],
        config: HeapCensusConfig,
    ) -> tuple[bytes, int]:
        """Serialize under ``max_bytes`` by shrinking top-N, never by cutting JSON."""
        top_n = config.top_n
        while True:
            report["types_by_count"] = [
                {"type": name, "count": entry[0], "shallow_bytes": entry[1]}
                for name, entry in by_count[:top_n]
            ]
            report["types_by_bytes"] = [
                {"type": name, "count": entry[0], "shallow_bytes": entry[1]}
                for name, entry in by_bytes[:top_n]
            ]
            traced = report["tracemalloc"]
            if traced.get("tracing"):
                traced["by_line"] = traced["by_line"][:top_n]
                traced["by_file"] = traced["by_file"][:top_n]
            report["top_n_written"] = top_n
            report["fit_to_max_bytes"] = top_n < config.top_n
            payload = json.dumps(report, indent=1, sort_keys=True).encode("utf-8")
            if len(payload) <= config.max_bytes or top_n == 0:
                return payload, top_n
            top_n //= 2

    def _write(self, payload: bytes, taken_at: float) -> Path:
        directory = Path(self._config.output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        # heap-census-<UTC stamp>-<pid>-<sequence>.json. The sequence is this
        # process's census count, zero-padded, so plain name order is the
        # order the reports were written: newest-first retention below never
        # consults a filesystem timestamp, whose granularity can tie.
        stem = f"{HEAP_CENSUS_FILE_PREFIX}{_utc_stamp(taken_at)}-{os.getpid()}"
        while True:
            self._sequence += 1
            path = directory / f"{stem}-{self._sequence:0{HEAP_CENSUS_SEQUENCE_WIDTH}d}{HEAP_CENSUS_FILE_SUFFIX}"
            if not path.exists():
                break
        temporary = path.with_suffix(".tmp")
        temporary.write_bytes(payload)
        os.replace(temporary, path)
        return path

    @staticmethod
    def _retain_files(directory: Path) -> None:
        """Keep the newest ``HEAP_CENSUS_RETAINED_FILES`` census files.

        Newest by name: the stamp, then the pid, then the zero-padded
        per-process sequence, so the order does not depend on the
        filesystem's timestamp granularity.
        """
        try:
            names = sorted(
                entry.name
                for entry in directory.iterdir()
                if entry.name.startswith(HEAP_CENSUS_FILE_PREFIX)
                and entry.name.endswith(HEAP_CENSUS_FILE_SUFFIX)
            )
        except OSError:
            return
        for name in names[: max(0, len(names) - HEAP_CENSUS_RETAINED_FILES)]:
            try:
                (directory / name).unlink()
            except OSError:
                pass


# ---------------------------------------------------------------------------
# malloc_trim, bounded and reported through the always-on telemetry.
# ---------------------------------------------------------------------------


def resolve_malloc_trim() -> Callable[[int], int]:
    """Bind glibc's ``malloc_trim`` through ctypes, or raise where absent."""
    libc = ctypes.CDLL(None)
    function = libc.malloc_trim
    function.restype = ctypes.c_int
    function.argtypes = (ctypes.c_size_t,)
    return function


@dataclass(frozen=True)
class MallocTrimResult:
    """One ``malloc_trim(0)`` call and the allocator readings around it.

    The deltas are the mechanism the trim is judged by: ``free`` bytes and
    RSS should fall while ``in_use`` bytes are unchanged (a trim frees
    nothing the program still holds). ``released`` is glibc's own return
    value, 1 when any memory went back to the kernel.
    """

    reason: str
    released: bool
    seconds: float
    resident_before: int
    resident_after: int
    before: ProcessHeapSample
    after: ProcessHeapSample

    @property
    def resident_delta(self) -> int:
        if self.resident_before < 0 or self.resident_after < 0:
            return 0
        return self.resident_after - self.resident_before

    @property
    def arena_delta(self) -> int:
        return self.after.malloc_arena_bytes - self.before.malloc_arena_bytes

    @property
    def in_use_delta(self) -> int:
        return self.after.malloc_in_use_bytes - self.before.malloc_in_use_bytes

    @property
    def free_delta(self) -> int:
        return self.after.malloc_free_bytes - self.before.malloc_free_bytes

    def as_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "released": self.released,
            "seconds": round(self.seconds, 6),
            "resident_before": self.resident_before,
            "resident_after": self.resident_after,
            "resident_delta": self.resident_delta,
            "malloc_info_available": self.before.malloc_info_available
            and self.after.malloc_info_available,
            "arena_before": self.before.malloc_arena_bytes,
            "arena_after": self.after.malloc_arena_bytes,
            "arena_delta": self.arena_delta,
            "in_use_before": self.before.malloc_in_use_bytes,
            "in_use_after": self.after.malloc_in_use_bytes,
            "in_use_delta": self.in_use_delta,
            "free_before": self.before.malloc_free_bytes,
            "free_after": self.after.malloc_free_bytes,
            "free_delta": self.free_delta,
        }


class MallocTrimmer:
    """Call ``malloc_trim(0)`` once per request and report its effect.

    Serialized by a lock so two trims never overlap. The symbol is resolved
    once and its absence remembered, exactly like ``mallinfo2`` in the
    collector: on musl or macOS :meth:`trim_once` returns None every time
    and never logs more than once.
    """

    def __init__(
        self,
        telemetry: ProcessHeapTelemetry | None = None,
        *,
        resolver: Callable[[], Callable[[int], int]] = resolve_malloc_trim,
        clock: Callable[[], float] = time.monotonic,
        rss_reader: Callable[[], int] = read_resident_memory_bytes,
        log: Callable[[str], None] = _log_line,
    ) -> None:
        self._telemetry = telemetry if telemetry is not None else ProcessHeapTelemetry()
        self._resolver = resolver
        self._clock = clock
        self._rss_reader = rss_reader
        self._log = log
        self._lock = threading.Lock()
        self._malloc_trim: object = _UNRESOLVED
        self.last_result: MallocTrimResult | None = None
        self.calls = 0

    def _resolved(self) -> Callable[[int], int] | None:
        resolved = self._malloc_trim
        if resolved is _UNRESOLVED:
            try:
                resolved = self._resolver()
            except Exception:
                resolved = None
                self._log("prism malloc_trim unavailable: glibc malloc_trim symbol not bound")
            self._malloc_trim = resolved
        return resolved  # type: ignore[return-value]

    def trim_once(self, reason: str) -> MallocTrimResult | None:
        with self._lock:
            malloc_trim = self._resolved()
            if malloc_trim is None:
                return None
            before = self._telemetry.sample()
            resident_before = self._rss_reader()
            started = self._clock()
            try:
                released = int(malloc_trim(0))
            except Exception as exc:
                self._malloc_trim = None
                self._log(f"prism malloc_trim failed reason={reason} error={exc!r}")
                return None
            seconds = self._clock() - started
            after = self._telemetry.sample()
            resident_after = self._rss_reader()
            result = MallocTrimResult(
                reason=_truncate(str(reason), 80),
                released=bool(released),
                seconds=seconds,
                resident_before=resident_before,
                resident_after=resident_after,
                before=before,
                after=after,
            )
            self.calls += 1
            self.last_result = result
            self._log(
                "prism malloc_trim "
                + " ".join(f"{key}={value}" for key, value in result.as_dict().items())
            )
            return result


# ---------------------------------------------------------------------------
# The service: one thread, one pipe, two signals, one periodic action.
# ---------------------------------------------------------------------------

_CENSUS_BYTE = b"c"
_TRIM_BYTE = b"t"
_STOP_BYTE = b"q"


class HeapCensusService:
    """Own the census thread and dispatch its requests with rate limits.

    A signal handler runs as Python code on the main thread between two
    bytecodes, and a second signal can interrupt it at any bytecode, so the
    handler must not take a lock it might already hold: ``Event.set`` would
    do exactly that. The handlers therefore only store the reason string
    (one attribute assignment) and write one byte to a non-blocking pipe,
    which is the same self-pipe shape ``signal.set_wakeup_fd`` uses. A full
    pipe means a request is already queued, and the byte is dropped: the
    requests coalesce rather than pile up.

    The worker reads the pipe, applies ``min_interval_seconds`` per action
    kind, runs the census or the trim, and runs the periodic trim when its
    interval is set. Nothing here is reachable from the metrics renderer,
    the audit HTTP handler, the public read service, or the Stratum
    dispatcher; the coordinator only calls :func:`install_heap_census` from
    ``main``.
    """

    THREAD_NAME = "prism-heap-census"

    def __init__(
        self,
        config: HeapCensusConfig,
        *,
        census: HeapCensus | None = None,
        trimmer: MallocTrimmer | None = None,
        telemetry: ProcessHeapTelemetry | None = None,
        clock: Callable[[], float] = time.monotonic,
        log: Callable[[str], None] = _log_line,
    ) -> None:
        self._config = config
        telemetry = telemetry if telemetry is not None else ProcessHeapTelemetry()
        self.census = census if census is not None else HeapCensus(
            config, telemetry=telemetry, clock=clock, log=log
        )
        self.trimmer = trimmer if trimmer is not None else MallocTrimmer(
            telemetry, clock=clock, log=log
        )
        self._clock = clock
        self._log = log
        self._read_fd, self._write_fd = os.pipe()
        os.set_blocking(self._write_fd, False)
        os.set_blocking(self._read_fd, False)
        self._census_reason = "unset"
        self._trim_reason = "unset"
        self._last_census_monotonic: float | None = None
        self._last_trim_monotonic: float | None = None
        self._next_periodic_trim: float | None = (
            clock() + config.malloc_trim_interval_seconds
            if config.malloc_trim_interval_seconds > 0
            else None
        )
        self._thread: threading.Thread | None = None
        self._closed = False
        # (signal module, signum, previous handler) for every handler this
        # service armed, so close() can disarm them before the pipe goes.
        self._armed: list[tuple[Any, int, Any]] = []
        self.suppressed = {"census": 0, "trim": 0}

    @property
    def config(self) -> HeapCensusConfig:
        return self._config

    # -- signal registration ------------------------------------------------

    def arm_signal(self, signal_module: Any, signum: int, handler: Any) -> None:
        """Register ``handler`` and remember what it replaced."""
        getsignal = getattr(signal_module, "getsignal", None)
        previous = getsignal(signum) if callable(getsignal) else None
        signal_module.signal(signum, handler)
        self._armed.append((signal_module, signum, previous))

    def disarm_signals(self) -> None:
        """Restore every handler :meth:`arm_signal` replaced."""
        while self._armed:
            signal_module, signum, previous = self._armed.pop()
            if previous is None:
                previous = getattr(signal_module, "SIG_DFL", None)
            if previous is None:
                continue
            try:
                signal_module.signal(signum, previous)
            except (OSError, ValueError, TypeError):
                pass

    # -- signal-safe entry points -------------------------------------------

    def _wake(self, byte: bytes) -> bool:
        # After close() the descriptor numbers may belong to something else;
        # a late signal must not write into a stranger's pipe.
        if self._closed:
            return False
        try:
            os.write(self._write_fd, byte)
        except (BlockingIOError, InterruptedError, OSError):
            return False
        return True

    def request_census(self, reason: str) -> bool:
        """Queue a census; safe to call from a signal handler."""
        if not self._config.enabled:
            return False
        self._census_reason = reason
        return self._wake(_CENSUS_BYTE)

    def request_trim(self, reason: str) -> bool:
        """Queue a trim; safe to call from a signal handler."""
        self._trim_reason = reason
        return self._wake(_TRIM_BYTE)

    # -- dispatch with rate limits ------------------------------------------

    def dispatch(self, kind: str, reason: str) -> object | None:
        """Run one action now unless its rate limit suppresses it.

        Returns the census outcome, the trim result, or None when suppressed
        or unavailable. The periodic trim goes through the same gate.
        """
        now = self._clock()
        floor = self._config.min_interval_seconds
        if kind == "census":
            if not self._config.enabled:
                return None
            last = self._last_census_monotonic
            if last is not None and now - last < floor:
                self.suppressed["census"] += 1
                self._log(
                    f"prism heap census suppressed reason={reason} "
                    f"min_interval_seconds={floor:g} since_last={now - last:.1f}"
                )
                return None
            outcome = self.census.take(reason)
            self._last_census_monotonic = self._clock()
            return outcome
        if kind == "trim":
            last = self._last_trim_monotonic
            if last is not None and now - last < floor:
                self.suppressed["trim"] += 1
                self._log(
                    f"prism malloc_trim suppressed reason={reason} "
                    f"min_interval_seconds={floor:g} since_last={now - last:.1f}"
                )
                return None
            result = self.trimmer.trim_once(reason)
            self._last_trim_monotonic = self._clock()
            return result
        raise ValueError(f"unknown census action {kind!r}")

    def periodic_trim_due(self) -> bool:
        return (
            self._next_periodic_trim is not None
            and self._clock() >= self._next_periodic_trim
        )

    def _timeout(self) -> float | None:
        if self._next_periodic_trim is None:
            return None
        return max(0.0, self._next_periodic_trim - self._clock())

    def run_once(self, timeout: float | None) -> bool:
        """One worker iteration: wait, then serve what arrived. False on stop."""
        try:
            ready, _w, _x = select.select([self._read_fd], [], [], timeout)
        except (OSError, ValueError):
            return False
        if ready:
            try:
                pending = os.read(self._read_fd, 64)
            except (BlockingIOError, InterruptedError):
                pending = b""
            except OSError:
                return False
            if _STOP_BYTE in pending:
                return False
            if _CENSUS_BYTE in pending:
                self.dispatch("census", self._census_reason)
            if _TRIM_BYTE in pending:
                self.dispatch("trim", self._trim_reason)
        if self.periodic_trim_due():
            self.dispatch("trim", "periodic")
            self._next_periodic_trim = (
                self._clock() + self._config.malloc_trim_interval_seconds
            )
        return True

    def run_forever(self) -> None:
        while not self._closed:
            try:
                if not self.run_once(self._timeout()):
                    return
            except Exception as exc:  # pragma: no cover - defensive; logged
                self._log(f"prism heap census thread error={exc!r}")

    def start(self) -> threading.Thread:
        if self._thread is None:
            self._thread = threading.Thread(
                target=self.run_forever,
                name=self.THREAD_NAME,
                daemon=True,
            )
            self._thread.start()
        return self._thread

    def close(self) -> None:
        """Disarm the signals, stop the worker, release the pipe.

        Order matters: handlers first, so no signal can reach ``_wake`` once
        the descriptors are gone; then the stop byte while the pipe is still
        open; then ``_closed`` so any handler already past the disarm is a
        no-op; then the descriptors. Tests call this; in the coordinator the
        daemon thread dies with the process.
        """
        if self._closed:
            return
        self.disarm_signals()
        self._wake(_STOP_BYTE)
        self._closed = True
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5.0)
        for descriptor in (self._read_fd, self._write_fd):
            try:
                os.close(descriptor)
            except OSError:
                pass


def heap_census_signal(signal_module: Any) -> int | None:
    return getattr(signal_module, "SIGUSR1", None)


def malloc_trim_signal(signal_module: Any) -> int | None:
    base = getattr(signal_module, "SIGRTMIN", None)
    if base is None:
        return None
    return int(base) + 1


def install_heap_census(
    config: HeapCensusConfig,
    *,
    signal_module: Any = None,
    log: Callable[[str], None] = _log_line,
    telemetry: ProcessHeapTelemetry | None = None,
    start_thread: bool = True,
) -> HeapCensusService | None:
    """Register the census and trim signals and start the worker; None when off.

    The one call ``prism_coordinator.main`` makes. With every switch off
    nothing is registered, no thread starts, tracemalloc is not started,
    and the return value is None: the walk is unreachable, not merely idle.
    ``signal_module`` is injectable so a test can prove which handlers were
    registered without sending real signals to itself.
    """
    if not config.anything_armed:
        return None
    if signal_module is None:
        import signal as signal_module  # noqa: PLC0415 - registration only
    if config.enabled and config.tracemalloc_enabled and not tracemalloc.is_tracing():
        tracemalloc.start(HEAP_CENSUS_TRACEMALLOC_FRAMES)
    service = HeapCensusService(config, telemetry=telemetry, log=log)
    armed: list[str] = []
    if config.enabled:
        census_signal = heap_census_signal(signal_module)
        if census_signal is None:
            log("prism heap census: SIGUSR1 unavailable on this platform; census not armed")
        else:

            def _on_census_signal(signum: int, _frame: Any) -> None:
                service.request_census(f"signal:{signum}")

            service.arm_signal(signal_module, census_signal, _on_census_signal)
            armed.append(f"census={HEAP_CENSUS_SIGNAL_NAME}")
    if config.malloc_trim_signal_enabled:
        trim_signal = malloc_trim_signal(signal_module)
        if trim_signal is None:
            log("prism malloc_trim: SIGRTMIN unavailable on this platform; trim signal not armed")
        else:

            def _on_trim_signal(signum: int, _frame: Any) -> None:
                service.request_trim(f"signal:{signum}")

            service.arm_signal(signal_module, trim_signal, _on_trim_signal)
            armed.append(f"trim={MALLOC_TRIM_SIGNAL_NAME}")
    if config.malloc_trim_interval_seconds > 0:
        armed.append(f"periodic_trim={config.malloc_trim_interval_seconds:g}s")
    if start_thread:
        service.start()
    log(
        "prism heap census armed "
        + " ".join(armed)
        + f" dir={config.output_dir} top_n={config.top_n} max_bytes={config.max_bytes}"
        f" max_seconds={config.max_seconds:g} min_interval_seconds={config.min_interval_seconds:g}"
        f" tracemalloc={'on' if config.enabled and config.tracemalloc_enabled else 'off'}"
    )
    return service


# ---------------------------------------------------------------------------
# The RSS bound: the automated soak check.
# ---------------------------------------------------------------------------

# The stated bound. The baseline is the largest resident set seen during the
# warm-up (one hour: long enough for the payout window to be materialized
# and the first full rescan to run), and every later sample must stay under
# twice it. The #226 slope (390 MB/h from a 125-145 MiB start) breaches this
# inside the second hour; #185's post-storm drain (125 -> 613 MiB) would
# breach it only if the storm fell outside the warm-up and did not drain.
RSS_BOUND_DEFAULT_MULTIPLE = 2.0
RSS_BOUND_DEFAULT_WARMUP_SECONDS = 3600.0
# A 24 h soak with an hour of tolerance for a late first sample.
RSS_BOUND_DEFAULT_MIN_SPAN_SECONDS = 23 * 3600.0


@dataclass(frozen=True)
class RssBoundVerdict:
    passed: bool
    reasons: tuple[str, ...]
    multiple: float
    warmup_seconds: float
    samples: int
    span_seconds: float
    baseline_bytes: int
    bound_bytes: int
    peak_bytes: int
    peak_at_seconds: float
    peak_ratio: float
    first_breach_at_seconds: float | None
    slope_bytes_per_hour: float | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "reasons": list(self.reasons),
            "multiple": self.multiple,
            "warmup_seconds": self.warmup_seconds,
            "samples": self.samples,
            "span_seconds": self.span_seconds,
            "baseline_bytes": self.baseline_bytes,
            "bound_bytes": self.bound_bytes,
            "peak_bytes": self.peak_bytes,
            "peak_at_seconds": self.peak_at_seconds,
            "peak_ratio": self.peak_ratio,
            "first_breach_at_seconds": self.first_breach_at_seconds,
            "slope_bytes_per_hour": self.slope_bytes_per_hour,
        }


def _least_squares_slope(points: Sequence[tuple[float, float]]) -> float | None:
    if len(points) < 2:
        return None
    n = float(len(points))
    mean_x = sum(x for x, _y in points) / n
    mean_y = sum(y for _x, y in points) / n
    denominator = sum((x - mean_x) ** 2 for x, _y in points)
    if denominator == 0:
        return None
    return sum((x - mean_x) * (y - mean_y) for x, y in points) / denominator


def evaluate_rss_bound(
    samples: Iterable[tuple[float, int]],
    *,
    warmup_seconds: float = RSS_BOUND_DEFAULT_WARMUP_SECONDS,
    multiple: float = RSS_BOUND_DEFAULT_MULTIPLE,
    min_span_seconds: float = 0.0,
    max_slope_bytes_per_hour: float | None = None,
) -> RssBoundVerdict:
    """Judge a ``(seconds, rss_bytes)`` series against the stated multiple.

    Times may be absolute or relative; they are re-based on the first
    sample. Samples with a negative RSS (the -1 "unavailable" sentinel) are
    ignored. The verdict fails when any post-warm-up sample exceeds
    ``multiple`` times the warm-up peak, when the series is too short to
    contain both a warm-up and a post-warm-up sample, when it spans less
    than ``min_span_seconds``, or when ``max_slope_bytes_per_hour`` is given
    and the least-squares slope over the post-warm-up samples exceeds it.
    """
    if multiple <= 1.0:
        raise ValueError("multiple must be greater than 1.0")
    if warmup_seconds < 0:
        raise ValueError("warmup_seconds must be non-negative")
    ordered = sorted(
        (float(seconds), int(rss)) for seconds, rss in samples if int(rss) >= 0
    )
    reasons: list[str] = []
    if not ordered:
        return RssBoundVerdict(
            passed=False,
            reasons=("no usable samples",),
            multiple=multiple,
            warmup_seconds=warmup_seconds,
            samples=0,
            span_seconds=0.0,
            baseline_bytes=-1,
            bound_bytes=-1,
            peak_bytes=-1,
            peak_at_seconds=0.0,
            peak_ratio=0.0,
            first_breach_at_seconds=None,
            slope_bytes_per_hour=None,
        )
    origin = ordered[0][0]
    rebased = [(seconds - origin, rss) for seconds, rss in ordered]
    span = rebased[-1][0]
    warmup = [(t, r) for t, r in rebased if t <= warmup_seconds]
    steady = [(t, r) for t, r in rebased if t > warmup_seconds]
    if not warmup:
        reasons.append("no sample inside the warm-up window")
    if not steady:
        reasons.append("no sample after the warm-up window")
    if span < min_span_seconds:
        reasons.append(
            f"series spans {span:.0f}s, below the required {min_span_seconds:.0f}s"
        )
    baseline = max((r for _t, r in warmup), default=-1)
    bound = int(baseline * multiple) if baseline >= 0 else -1
    peak_at, peak = max(rebased, key=lambda item: (item[1], -item[0]))
    peak_ratio = (peak / baseline) if baseline > 0 else 0.0
    first_breach: float | None = None
    if baseline >= 0:
        for t, r in steady:
            if r > bound:
                first_breach = t
                break
    if first_breach is not None:
        reasons.append(
            f"resident set exceeded {multiple:g}x the warm-up baseline "
            f"({baseline} bytes) at {first_breach:.0f}s"
        )
    slope_per_second = _least_squares_slope([(t, float(r)) for t, r in steady])
    slope_per_hour = slope_per_second * 3600.0 if slope_per_second is not None else None
    if (
        max_slope_bytes_per_hour is not None
        and slope_per_hour is not None
        and slope_per_hour > max_slope_bytes_per_hour
    ):
        reasons.append(
            f"post-warm-up slope {slope_per_hour:.0f} bytes/h exceeds "
            f"{max_slope_bytes_per_hour:.0f} bytes/h"
        )
    return RssBoundVerdict(
        passed=not reasons,
        reasons=tuple(reasons),
        multiple=multiple,
        warmup_seconds=warmup_seconds,
        samples=len(rebased),
        span_seconds=span,
        baseline_bytes=baseline,
        bound_bytes=bound,
        peak_bytes=peak,
        peak_at_seconds=peak_at,
        peak_ratio=peak_ratio,
        first_breach_at_seconds=first_breach,
        slope_bytes_per_hour=slope_per_hour,
    )


def read_rss_samples(path: Path) -> list[tuple[float, int]]:
    """Parse ``seconds,rss_bytes`` lines; blank lines and ``#`` comments are skipped."""
    samples: list[tuple[float, int]] = []
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = [part.strip() for part in line.replace("\t", ",").split(",")]
        if len(parts) < 2:
            raise ValueError(f"{path}:{number}: expected 'seconds,rss_bytes'")
        try:
            samples.append((float(parts[0]), int(float(parts[1]))))
        except ValueError as exc:
            raise ValueError(f"{path}:{number}: {exc}") from exc
    return samples


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m lab.prism.process_telemetry",
        description="Issue #226 soak tooling: sample a resident set, judge a series.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    sample = commands.add_parser(
        "rss-sample",
        help="print one 'epoch_seconds,rss_bytes' line for a pid",
    )
    sample.add_argument("--pid", default="self", help="process id (default: this process)")
    bound = commands.add_parser(
        "rss-bound",
        help="judge a 'seconds,rss_bytes' series against the stated multiple",
    )
    bound.add_argument("--samples", type=Path, required=True, help="CSV of seconds,rss_bytes")
    bound.add_argument(
        "--warmup-seconds",
        type=float,
        default=RSS_BOUND_DEFAULT_WARMUP_SECONDS,
        help=f"baseline window (default {RSS_BOUND_DEFAULT_WARMUP_SECONDS:g})",
    )
    bound.add_argument(
        "--multiple",
        type=float,
        default=RSS_BOUND_DEFAULT_MULTIPLE,
        help=f"allowed multiple of the warm-up peak (default {RSS_BOUND_DEFAULT_MULTIPLE:g})",
    )
    bound.add_argument(
        "--min-span-seconds",
        type=float,
        default=RSS_BOUND_DEFAULT_MIN_SPAN_SECONDS,
        help=f"refuse a series shorter than this (default {RSS_BOUND_DEFAULT_MIN_SPAN_SECONDS:g})",
    )
    bound.add_argument(
        "--max-slope-bytes-per-hour",
        type=float,
        default=None,
        help="also fail when the post-warm-up least-squares slope exceeds this",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_cli().parse_args(argv)
    if args.command == "rss-sample":
        rss = read_resident_memory_bytes(args.pid)
        print(f"{time.time():.0f},{rss}")
        return 0 if rss >= 0 else 1
    try:
        samples = read_rss_samples(args.samples)
    except (OSError, ValueError) as exc:
        print(f"rss-bound: {exc}", file=sys.stderr)
        return 2
    try:
        verdict = evaluate_rss_bound(
            samples,
            warmup_seconds=args.warmup_seconds,
            multiple=args.multiple,
            min_span_seconds=args.min_span_seconds,
            max_slope_bytes_per_hour=args.max_slope_bytes_per_hour,
        )
    except ValueError as exc:
        print(f"rss-bound: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(verdict.as_dict(), indent=2, sort_keys=True))
    return 0 if verdict.passed else 1


__all__ = [
    "ANONYMOUS_REGION_BANDS",
    "AnonymousMapShape",
    "HEAP_CENSUS_FORMAT",
    "HEAP_CENSUS_RETAINED_FILES",
    "HEAP_CENSUS_SIGNAL_NAME",
    "HEAP_CENSUS_TRACEMALLOC_FRAMES",
    "HeapCensus",
    "HeapCensusConfig",
    "HeapCensusOutcome",
    "HeapCensusService",
    "MALLOC_TRIM_SIGNAL_NAME",
    "MallInfo2",
    "MallocInfoSummary",
    "MallocTrimResult",
    "MallocTrimmer",
    "PRISM_GC_GENERATIONS",
    "ProcessHeapSample",
    "ProcessHeapTelemetry",
    "RSS_BOUND_DEFAULT_MIN_SPAN_SECONDS",
    "RSS_BOUND_DEFAULT_MULTIPLE",
    "RSS_BOUND_DEFAULT_WARMUP_SECONDS",
    "RssBoundVerdict",
    "evaluate_rss_bound",
    "heap_census_signal",
    "install_heap_census",
    "main",
    "malloc_trim_signal",
    "parse_malloc_info_xml",
    "read_anonymous_map_shape",
    "read_malloc_arena_count",
    "read_malloc_info_summary",
    "read_resident_memory_bytes",
    "read_rss_samples",
    "resolve_malloc_trim",
    "resolve_mallinfo2",
]


if __name__ == "__main__":
    raise SystemExit(main())
