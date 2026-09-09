"""Isolated audit codec/ownership experiment; this is not a mined-block fixture.

Run serially in Docker with the coordinator's Python and allocator settings.
Each artifact has 375,000 shares by default (about 232 MB). The ordinary
phases run without allocation tracing; --trace measures allocations separately
because tracing substantially changes elapsed time and scheduling.
"""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import sys
import tempfile
import threading
import time
import tracemalloc
import weakref

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism.audit_bundle_view import CanonicalAuditBundleView, iter_json_byte_chunks
from tests.test_prism_audit_bundle_view import make_share


def fixture(path: Path, count: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    with path.open("wb") as output:
        def emit(value: str) -> None:
            encoded = value.encode("utf-8")
            output.write(encoded)
            digest.update(encoded)

        emit('{"schema":"qbit.prism.audit-bundle.v1","shares":[')
        for index in range(count):
            row = make_share(index)
            row["share_id"] += "x" * 147
            row["credit_policy"] = None
            emit(("," if index else "") + json.dumps(row, separators=(",", ":")))
        emit('],"found_block":{"block_height":10},"prior_balances":[],')
        emit('"reward_manifest":{"shares":[')
        for index in range(count):
            emit(("," if index else "") + json.dumps(
                {"share_seq": index + 1, "counted_difficulty": 5}, separators=(",", ":"),
            ))
        emit('],"entitlements":[]},"payout_policy_manifest":{"accounts":[]},')
        emit('"signed_coinbase_manifest":{"manifest":{"coinbase_tx_hex":"00"}}}')
    return path.stat().st_size, digest.hexdigest()


def memory() -> dict[str, int]:
    result = {}
    for line in Path("/proc/self/status").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "VmHWM"):
            result[key + "_bytes"] = int(value.strip().split()[0]) * 1024
    return result


def measure(label: str, operation, *, trace: bool):
    stop, ready = threading.Event(), threading.Event()
    wakes, pauses = [], []
    gc_started = {}

    def gc_event(phase, info):
        generation = info["generation"]
        if phase == "start":
            gc_started[generation] = time.monotonic()
        else:
            started = gc_started.pop(generation, None)
            if started is not None:
                pauses.append((generation, time.monotonic() - started))

    def observer():
        ready.set()
        while not stop.is_set():
            started = time.monotonic()
            stop.wait(0.005)
            wakes.append(max(0.0, time.monotonic() - started - 0.005))

    thread = threading.Thread(target=observer)
    thread.start()
    ready.wait()
    time.sleep(0.025)
    gc.callbacks.append(gc_event)
    if trace:
        tracemalloc.start()
    try:
        started = time.monotonic()
        result = operation()
        elapsed = time.monotonic() - started
        peak = tracemalloc.get_traced_memory()[1] if trace else None
        time.sleep(0.025)
    finally:
        stop.set()
        thread.join()
        gc.callbacks.remove(gc_event)
        if trace:
            tracemalloc.stop()
    ordered = sorted(wakes)
    print(json.dumps({
        "phase": label, "seconds": elapsed,
        "observer_max_seconds": max(wakes, default=0),
        "observer_p99_seconds": ordered[int((len(ordered) - 1) * 0.99)],
        "wakes_over_100ms": sum(value >= 0.1 for value in wakes),
        "gc_pause_count": len(pauses),
        "gc_max_pause_seconds": max((value for _, value in pauses), default=0),
        "peak_traced_bytes": peak, **memory(),
    }), flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shares", type=int, default=375000)
    parser.add_argument("--candidates", type=int, default=1)
    parser.add_argument("--gc", choices=("off", "on"), default="off")
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--baseline", action="store_true")
    args = parser.parse_args()
    if args.shares < 1 or args.candidates < 1:
        parser.error("share and candidate counts must be positive")
    print(json.dumps({
        "python": sys.version, "machine": platform.machine(),
        "gc": args.gc, "trace": args.trace, "candidates": args.candidates,
        "switch_interval": sys.getswitchinterval(), "gc_thresholds": gc.get_threshold(),
        "allocator": {name: os.environ.get(name) for name in (
            "MALLOC_ARENA_MAX", "PYTHONMALLOC", "GLIBC_TUNABLES",
        )},
    }), flush=True)
    with tempfile.TemporaryDirectory(prefix="audit-codec-probe-") as directory:
        path = Path(directory) / "audit.json"
        size, digest = fixture(path, args.shares)
        print(json.dumps({"shares": args.shares, "artifact_bytes": size, "sha256": digest}), flush=True)
        gc.collect()
        if args.gc == "off":
            gc.disable()
        for index in range(args.candidates):
            owner = {}
            owner["view"] = measure(f"scan-{index}", lambda: CanonicalAuditBundleView.scan_path(path), trace=args.trace)
            assert owner["view"].sha256_hex == digest
            view_ref = weakref.ref(owner["view"])
            source_ref = weakref.ref(owner["view"].source)
            count = measure(f"lazy-walk-{index}", lambda: sum(1 for _ in owner["view"]["shares"]), trace=args.trace)
            assert count == args.shares
            encoded = measure(f"encode-{index}", lambda: sum(len(chunk) for chunk in iter_json_byte_chunks(owner["view"])), trace=args.trace)
            assert encoded == size
            measure(f"retire-{index}", lambda: owner.pop("view").close(), trace=args.trace)
            assert view_ref() is None and source_ref() is None
        if args.baseline:
            aggregate = Path(directory) / "aggregate.json"
            with aggregate.open("wb") as destination:
                destination.write(b"[")
                for index in range(args.candidates):
                    if index:
                        destination.write(b",")
                    with path.open("rb") as source:
                        shutil.copyfileobj(source, destination, length=256 * 1024)
                destination.write(b"]")
            rows = measure("whole-decode-control", lambda: json.loads(aggregate.read_bytes()), trace=args.trace)
            assert all(len(row["shares"]) == args.shares for row in rows)
            measure("whole-reference-count-retirement", rows.clear, trace=args.trace)
        measure("cyclic-gc-control", gc.collect, trace=args.trace)


if __name__ == "__main__":
    main()
