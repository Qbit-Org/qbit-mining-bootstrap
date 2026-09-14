"""Bounded weak ownership accounting; no heap walk and no owning references."""

from __future__ import annotations

import threading
import weakref
from typing import Any


MAX_TRACKED_WINDOW_OWNERS = 16384
# Allocating an entry may trigger GC and invoke another owner's weakref
# callback on this same thread. Reentry must not deadlock the lease-bearing
# interpreter. Buffer reference counts mutate in place (without allocating a
# GC-tracked tuple between reading and updating a shared alias count).
_LOCK = threading.RLock()
_OWNERS: dict[int, tuple[Any, int, str, int, int]] = {}
_BUFFERS: dict[int, list[int]] = {}
_COUNTS = dict(owners=0, canonical_buffers=0, canonical_bytes=0,
               page_records=0, parsed_records=0, observations_dropped_total=0)


def track_window(owner: Any, data: bytes, *, kind: str, records: int = 0) -> None:
    key, buffer_key = id(owner), id(data)

    def retired(reference: Any) -> None:
        with _LOCK:
            entry = _OWNERS.pop(key, None)
            if entry is None:
                return
            _, backing, category, rows, parsed = entry
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

    with _LOCK:
        if len(_OWNERS) >= MAX_TRACKED_WINDOW_OWNERS:
            _COUNTS["observations_dropped_total"] += 1
            return
        _OWNERS[key] = (weakref.ref(owner, retired), buffer_key, kind, records, 0)
        buffer = _BUFFERS.get(buffer_key)
        if buffer is None:
            buffer = [0, len(data)]
            _BUFFERS[buffer_key] = buffer
        buffer[0] += 1
        _COUNTS["owners"] += 1
        _COUNTS["page_records"] += records if kind == "page" else 0
        if buffer[0] == 1:
            _COUNTS["canonical_buffers"] += 1
            _COUNTS["canonical_bytes"] += buffer[1]


def note_parsed_window(owner: Any, records: int) -> None:
    with _LOCK:
        entry = _OWNERS.get(id(owner))
        if entry is not None:
            reference, backing, kind, rows, previous = entry
            _OWNERS[id(owner)] = (reference, backing, kind, rows, records)
            _COUNTS["parsed_records"] += records - previous


def window_ownership_snapshot() -> dict[str, int]:
    with _LOCK:
        return dict(_COUNTS)
