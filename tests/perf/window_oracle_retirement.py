#!/usr/bin/env python3
"""Production-sized isolated oracle replay; no database or network access.

Run with GC enabled, a 50ms monitor, two legitimate historical windows and
changing canonical bytes. Every retired mirror must disappear without forced
GC; ownership must return to baseline after drain. This is a local replay,
not production qualification or a proof of an interpreter watchdog guarantee.
"""

from __future__ import annotations

import argparse
from collections import deque
import gc
import hashlib
import json
from pathlib import Path
import platform
import resource
import sys
import threading
import time
import weakref

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism.window_oracle import snapshot_window, WINDOW_ORACLE_BYTES
from lab.prism.window_ownership import window_ownership_snapshot


class ReplayLedger:
    def __init__(self, count: int, cycle: int):
        self.count, self.cycle = count, cycle

    def spool_snapshot_at_job_issue(self, anchor, *, window_weight, sink):
        for index in range(self.count):
            sequence = self.cycle + index + 1
            miner = "tq1z" + f"{index % 17:064x}"
            sink.append(dict(
                share_seq=sequence,
                share_id=miner + ".rig123:" + hashlib.sha256(str(sequence).encode()).hexdigest(),
                miner_id=miner, order_key=miner, p2mr_program_hex=f"{index % 17:064x}",
                share_difficulty="16384", network_difficulty="226646186",
                template_height=900000, job_id=f"prism-job-{sequence:016x}",
                job_issued_at_ms=anchor - self.count + index,
                accepted_at_ms=anchor - self.count + index, ntime=1700000000,
            ))


def run(count: int, cycles: int) -> dict:
    before = window_ownership_snapshot()
    history, references = deque(), []
    stopped = threading.Event()
    maximum = [0.0]
    late_wakes = [0]
    pauses = []
    gc_start = {}

    def gc_observer(phase, info):
        generation = info["generation"]
        if phase == "start":
            gc_start[generation] = time.monotonic()
        elif generation in gc_start:
            pauses.append((generation, time.monotonic() - gc_start.pop(generation)))

    def monitor():
        while not stopped.is_set():
            due = time.monotonic() + 0.05
            if stopped.wait(0.05):
                return
            lateness = max(0, time.monotonic() - due)
            maximum[0] = max(maximum[0], lateness)
            late_wakes[0] += lateness >= 0.4

    thread = threading.Thread(target=monitor)
    gc.callbacks.append(gc_observer)
    thread.start()
    started = time.monotonic()
    samples = []
    try:
        for cycle in range(cycles):
            result = snapshot_window(
                ReplayLedger(count, cycle), anchor=1800000000000 + cycle,
                weight=count * 16384, append_epoch=cycle,
            )
            assert result.window.record_count == count
            history.append(result.window)
            references.append(weakref.ref(result.window))
            payload_bytes = len(result.window.canonical_items)
            del result
            if len(history) > 2:
                history.popleft()
            assert all(ref() is None for ref in references[:-2])
            sample = window_ownership_snapshot()
            assert sample["canonical_buffers"] - before["canonical_buffers"] == len(history)
            assert sample["canonical_bytes"] - before["canonical_bytes"] <= 2 * WINDOW_ORACLE_BYTES
            assert sample["page_records"] == before["page_records"]
            assert sample["parsed_records"] == before["parsed_records"]
            samples.append(dict(cycle=cycle, payload_bytes=payload_bytes,
                                live_buffers=len(history), owned_bytes=sample["canonical_bytes"] - before["canonical_bytes"]))
        history.clear()
        assert all(ref() is None for ref in references)
        assert window_ownership_snapshot() == before
    finally:
        stopped.set()
        thread.join()
        gc.callbacks.remove(gc_observer)
    return dict(records=count, cycles=cycles, seconds=time.monotonic() - started,
                maximum_monitor_lateness_seconds=maximum[0], wakes_at_400ms=late_wakes[0],
                parent_gc_max_seconds={str(g): max((p for generation, p in pauses if generation == g), default=0)
                                       for g in range(3)},
                retired_without_forced_gc=True, samples=samples,
                process_ru_maxrss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, nargs="+", default=[228397, 400000])
    parser.add_argument("--cycles", type=int, default=6)
    args = parser.parse_args()
    print(json.dumps(dict(python=sys.version, platform=platform.platform(),
                          gc_enabled=gc.isenabled())), flush=True)
    for count in args.records:
        print(json.dumps(run(count, args.cycles)), flush=True)
