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

Everything here is a snapshot read. The collector never walks the heap: no
``gc.get_objects()``, no ``tracemalloc``. It runs on every scrape.

``mallinfo2`` exists only in glibc 2.33 and later. musl and macOS have no
such symbol, and the older int-typed ``mallinfo`` is deliberately not used
as a fallback because its 32-bit fields wrap on a multi-gigabyte heap. The
binding is attempted once per collector, and an absent symbol is remembered
so a scrape never repeats the lookup and never logs. Absence renders as a
separate availability gauge at 0 with every byte gauge at -1; it never
renders as zero bytes, which an operator would read as an empty heap.
"""

from __future__ import annotations

import ctypes
import gc
import sys
import threading
from dataclasses import dataclass
from typing import Callable

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
    gc_pending: tuple[int, int, int]
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
            gc_pending=_per_generation(gc.get_count()),
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


__all__ = [
    "MallInfo2",
    "PRISM_GC_GENERATIONS",
    "ProcessHeapSample",
    "ProcessHeapTelemetry",
    "resolve_mallinfo2",
]
