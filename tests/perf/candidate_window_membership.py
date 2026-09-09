"""Disposable PostgreSQL comparison of bounded candidate membership and the old aggregate.

Example (after applying the ledger schema to a disposable database):
  python tests/perf/candidate_window_membership.py --database-url "$TEST_DATABASE_URL" \
      --shares 375000 --gc off --prepare-disposable-fixture

This replaces share rows in the supplied disposable database. It is a membership
and codec experiment, not a valid mined-block fixture or production timing proof.
The lease-guard actor is real; it records failures rather than terminating the
harness on a late wake. It reports scheduling delay separately from round-trip
duration; a short round trip alone does not imply a timely wake. Compare these
measurements with the integrated lease and
regtest gates before deployment. Run measurements serially in Docker Python3.14.7.
"""
import argparse
import gc
import json
import os
import platform
import sys
import threading
import time
import uuid
from pathlib import Path

# Executable directly from a repository checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import psycopg
from lab.prism.share_ledger import PsqlShareLedger, WRITER_LEASE_HEARTBEAT_SESSION_PREFIX

GC_PAUSES = {"active": {}, "max": {0: 0.0, 1: 0.0, 2: 0.0}, "count": {0: 0, 1: 0, 2: 0}}


def _gc_callback(phase: str, info: dict) -> None:
    gen = int(info.get("generation", 0))
    now = time.monotonic()
    if phase == "start":
        GC_PAUSES["active"][gen] = now
        return
    start = GC_PAUSES["active"].pop(gen, None)
    if start is None:
        return
    pause = now - start
    GC_PAUSES["max"][gen] = max(GC_PAUSES["max"][gen], pause)
    GC_PAUSES["count"][gen] += 1


def _reset_gc_pauses() -> None:
    GC_PAUSES["active"].clear()
    for gen in (0, 1, 2):
        GC_PAUSES["max"][gen] = 0.0
        GC_PAUSES["count"][gen] = 0


def _rss() -> dict[str, int]:
    out = {}
    try:
        with open("/proc/self/status") as handle:
            for line in handle:
                if line.startswith(("VmRSS:", "VmHWM:")):
                    key, value = line.split(":", 1)
                    out[key] = int(value.strip().split()[0]) // 1024
    except OSError:
        pass
    return out


def measure(label: str, function, *, note: str | None = None):
    stop, ready = threading.Event(), threading.Event()
    samples: list[float] = []

    def observer() -> None:
        ready.set()
        while not stop.is_set():
            start = time.monotonic()
            stop.wait(0.005)
            samples.append(max(0.0, time.monotonic() - start - 0.005))

    thread = threading.Thread(target=observer, name="observer")
    thread.start()
    ready.wait()
    time.sleep(0.025)
    _reset_gc_pauses()
    before = [s["collections"] for s in gc.get_stats()]
    start = time.monotonic()
    try:
        result = function()
        elapsed = time.monotonic() - start
        time.sleep(0.025)
    finally:
        stop.set()
        thread.join()
    record = dict(
        operation=label,
        wall_seconds=round(elapsed, 6),
        max_observer_lateness_seconds=round(max(samples), 6),
        wakes_over_100ms=sum(x >= 0.1 for x in samples),
        wakes_over_400ms=sum(x >= 0.4 for x in samples),
        gc_collection_delta=[s["collections"] - n for s, n in zip(gc.get_stats(), before)],
        gc_max_pause_seconds={g: round(GC_PAUSES["max"][g], 6) for g in (0, 1, 2)},
        gc_pause_count={g: GC_PAUSES["count"][g] for g in (0, 1, 2)},
        rss_mb=_rss(),
    )
    if note:
        record["note"] = note
    print(json.dumps(record), flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shares", type=int, default=375000)
    parser.add_argument("--gc", choices=("off", "on"), default="off")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--prepare-disposable-fixture", action="store_true", required=True)
    args = parser.parse_args()
    if args.shares < 1:
        parser.error("--shares must be positive")

    with psycopg.connect(args.database_url, autocommit=True) as connection:
        connection.execute("TRUNCATE qbit_share_ledger CASCADE")
        connection.execute("""
            INSERT INTO qbit_share_ledger (
                share_id, miner_id, payout_order_key, p2mr_program,
                share_difficulty, network_difficulty, template_height, job_id,
                job_issued_at, ntime, accepted_at, credit_policy, accepted,
                writer_id, writer_epoch
            ) SELECT
                'share-' || n, 'miner', 'miner', decode(repeat('aa', 32), 'hex'),
                1, 1000000, 1, 'job', to_timestamp(0), 0, to_timestamp(1),
                NULL, true, 'fixture', 1
            FROM generate_series(0, %s) AS n
        """, (args.shares - 1,))
    ledger = PsqlShareLedger(
        psql_command="psql", database_url=args.database_url,
        native_client_mode="native", writer_id="candidate-window-probe",
        writer_session_token=WRITER_LEASE_HEARTBEAT_SESSION_PREFIX + uuid.uuid4().hex,
    )

    class Rows:
        def __iter__(self):
            for index in range(args.shares):
                yield {"share_id": f"share-{index}"}

    stop = threading.Event()
    heartbeats: list[float] = []
    heartbeat_wakes: list[float] = []
    failures: list[str] = []

    def heartbeat() -> None:
        due = time.monotonic()
        while not stop.is_set():
            started = time.monotonic()
            heartbeat_wakes.append(max(0.0, started - due))
            try:
                ledger.prove_writer_lease_guard_session()
                heartbeats.append(time.monotonic() - started)
            except Exception as exc:
                failures.append(str(exc))
            due = time.monotonic() + 0.05
            stop.wait(max(0.0, due - time.monotonic()))

    def report_proofs(phase: str, before: int) -> None:
        print(json.dumps({
            "phase": phase,
            "proof_count": len(heartbeats) - before,
            "max_proof_seconds": max(heartbeats[before:], default=0),
            "max_heartbeat_wake_lateness_seconds": max(heartbeat_wakes[before:], default=0),
        }), flush=True)

    thread = threading.Thread(target=heartbeat, name="lease-proof")
    print(json.dumps({
        "python": sys.version, "machine": platform.machine(),
        "shares": args.shares, "gc": args.gc,
        "psycopg": psycopg.__version__,
        "libpq": psycopg.pq.version(),
        "switch_interval_seconds": sys.getswitchinterval(),
        "gc_thresholds": gc.get_threshold(),
        "allocator": {name: os.environ.get(name) for name in (
            "MALLOC_ARENA_MAX", "PYTHONMALLOC", "GLIBC_TUNABLES",
        )},
    }), flush=True)
    gc.collect()
    if args.gc == "off":
        gc.disable()
    gc.callbacks.append(_gc_callback)
    thread.start()
    try:
        before_proofs = len(heartbeats)
        result = measure("bounded-window-membership", lambda: ledger.candidate_window_covers(
            Rows(), anchor_job_issued_at_ms=2000, network_difficulty=1000000,
        ))
        assert result is True
        report_proofs("bounded-window-membership", before_proofs)

        before_proofs = len(heartbeats)
        rows = measure("baseline-audit-window-aggregate", lambda: ledger.audit_share_window(
            anchor_job_issued_at_ms=2000, network_difficulty=1000000,
        ))
        assert len(rows) == args.shares
        report_proofs("baseline-audit-window-aggregate", before_proofs)
        measure("baseline-reference-count-retirement", rows.clear)
    finally:
        stop.set()
        thread.join()
        gc.callbacks.remove(_gc_callback)
        try:
            ledger.release_writer_lease()
        finally:
            ledger.close()
    print(json.dumps({
        "heartbeat_count": len(heartbeats),
        "heartbeat_max_seconds": max(heartbeats, default=0),
        "heartbeat_max_wake_lateness_seconds": max(heartbeat_wakes, default=0),
        "heartbeat_errors": failures,
    }), flush=True)


if __name__ == "__main__":
    main()
