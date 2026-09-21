"""Bounded weak ownership accounting; no heap walk and no owning references.

Every payout-window owner (a daemon mirror, a byte-backed sequence or a
page) registers weakly at construction with its ``kind`` and a bounded
creation-site label, so a retained owner is attributable from metrics alone
(#332, defect 4): which kind leaked, which call site minted it, and how long
the oldest one has been alive. Nothing here keeps an owner alive.
"""

from __future__ import annotations

import os
import sys
import threading
import time
import weakref
from typing import Any


MAX_TRACKED_WINDOW_OWNERS = 16384
# Creation sites are function names, so the population is small and fixed
# by the source; the cap guards the export against a runaway label set. It
# bounds the labels ever admitted, not the labels concurrently live: a site
# admitted once stays admitted after its owners retire, so the series
# population Prometheus retains can never exceed the cap plus ``other``.
MAX_TRACKED_WINDOW_SITES = 64
OTHER_WINDOW_SITE = "other"
# Frames that construct an owner on the owner's behalf rather than on the
# caller's: a site label names the first frame outside this set.
_SITE_PASSTHROUGH_NAMES = frozenset((
    "__init__", "__post_init__", "__new__", "replace", "track_window",
    "json_records", "_creation_site", "from_full_items", "advanced",
))
# Allocating an entry may trigger GC and invoke another owner's weakref
# callback on this same thread. Reentry must not deadlock the lease-bearing
# interpreter. Buffer reference counts mutate in place (without allocating a
# GC-tracked tuple between reading and updating a shared alias count).
_LOCK = threading.RLock()
# key -> [reference, buffer_key, kind, page_rows, parsed_rows, site, created]
# in insertion order, so the first entry of a kind is that kind's oldest.
_OWNERS: dict[int, list[Any]] = {}
_BUFFERS: dict[int, list[int]] = {}
_COUNTS = dict(owners=0, canonical_buffers=0, canonical_bytes=0,
               page_records=0, parsed_records=0, observations_dropped_total=0)
# (kind, site) -> [owners, parsed_records] for sites with live owners.
_SITES: dict[tuple[str, str], list[int]] = {}
# Every (kind, site) label ever admitted to the export; never shrinks.
_ADMITTED_SITES: set[tuple[str, str]] = set()
# Seam for deterministic ages in render-parity tests.
_clock = time.monotonic


def _creation_site() -> str:
    """``module.function`` of the frame that asked for this owner; bounded."""
    frame = sys._getframe(1)
    while frame is not None:
        code = frame.f_code
        if (code.co_name not in _SITE_PASSTHROUGH_NAMES
                and not code.co_filename.endswith("dataclasses.py")):
            module = os.path.basename(code.co_filename)
            if module.endswith(".py"):
                module = module[:-3]
            return f"{module}.{code.co_name}"
        frame = frame.f_back
    return OTHER_WINDOW_SITE


def track_window(owner: Any, data: bytes, *, kind: str, records: int = 0,
                 site: str | None = None) -> None:
    key, buffer_key = id(owner), id(data)
    if site is None:
        site = _creation_site()

    def retired(reference: Any) -> None:
        with _LOCK:
            entry = _OWNERS.pop(key, None)
            if entry is None:
                return
            _, backing, category, rows, parsed, label, _ = entry
            buffer = _BUFFERS[backing]
            if buffer[0] == 1:
                del _BUFFERS[backing]
                _COUNTS["canonical_buffers"] -= 1
                _COUNTS["canonical_bytes"] -= buffer[1]
            else:
                buffer[0] -= 1
            _COUNTS["owners"] -= 1
            _COUNTS["page_records"] -= rows if category == "page" else 0
            _COUNTS["parsed_records"] -= parsed
            site_entry = _SITES[(category, label)]
            site_entry[0] -= 1
            site_entry[1] -= parsed
            if site_entry[0] == 0:
                del _SITES[(category, label)]

    with _LOCK:
        if len(_OWNERS) >= MAX_TRACKED_WINDOW_OWNERS:
            _COUNTS["observations_dropped_total"] += 1
            return
        if (kind, site) not in _ADMITTED_SITES:
            if len(_ADMITTED_SITES) >= MAX_TRACKED_WINDOW_SITES:
                site = OTHER_WINDOW_SITE
            _ADMITTED_SITES.add((kind, site))
        site_entry = _SITES.get((kind, site))
        if site_entry is None:
            site_entry = [0, 0]
            _SITES[(kind, site)] = site_entry
        _OWNERS[key] = [weakref.ref(owner, retired), buffer_key, kind, records, 0,
                        site, _clock()]
        buffer = _BUFFERS.get(buffer_key)
        if buffer is None:
            buffer = [0, len(data)]
            _BUFFERS[buffer_key] = buffer
        buffer[0] += 1
        site_entry[0] += 1
        _COUNTS["owners"] += 1
        _COUNTS["page_records"] += records if kind == "page" else 0
        if buffer[0] == 1:
            _COUNTS["canonical_buffers"] += 1
            _COUNTS["canonical_bytes"] += buffer[1]


def note_parsed_window(owner: Any, records: int) -> None:
    """Record that ``owner`` currently holds ``records`` parsed dicts (0: none)."""
    with _LOCK:
        entry = _OWNERS.get(id(owner))
        if entry is not None:
            previous = entry[4]
            entry[4] = records
            _COUNTS["parsed_records"] += records - previous
            _SITES[(entry[2], entry[5])][1] += records - previous


def window_ownership_snapshot() -> dict[str, int]:
    with _LOCK:
        return dict(_COUNTS)


def window_ownership_breakdown(now: float | None = None) -> dict[str, Any]:
    """Owners and parsed records by ``(kind, site)``, and the oldest owner's age by kind.

    One pass over the insertion-ordered registry, so it costs the tracked
    population (at most :data:`MAX_TRACKED_WINDOW_OWNERS`) and allocates
    only the small result; it never touches an owner.
    """
    if now is None:
        now = _clock()
    with _LOCK:
        oldest: dict[str, float] = {}
        for entry in _OWNERS.values():
            kind = entry[2]
            if kind not in oldest:
                oldest[kind] = max(0.0, now - entry[6])
        return dict(
            owners={key: value[0] for key, value in _SITES.items()},
            parsed_records={key: value[1] for key, value in _SITES.items() if value[1]},
            oldest_owner_age_seconds=oldest,
        )
