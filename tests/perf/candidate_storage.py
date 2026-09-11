"""Disposable native candidate storage/ownership probe, not a mined-block fixture.

Runs actual prepare, append, header replay, hydration, exact retry and cleanup
with a real writer-session proof actor. Creates and drops a private schema.
Run serially in Docker; ARM timings do not establish a production root cause.
"""
from __future__ import annotations

import argparse
from collections.abc import Sequence
import gc
import hashlib
import json
import platform
import sys
import threading
import time
import uuid
import weakref
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import psycopg
from psycopg.conninfo import make_conninfo
from lab.prism.candidate_codec import prepare_candidate_intent
from lab.prism.share_ledger import PsqlShareLedger, WRITER_LEASE_HEARTBEAT_SESSION_PREFIX
from tests.perf.candidate_window_membership import _gc_callback, measure
from tests.prism_postgres_candidate_gate import intent, pending_share_for, share


class Shares(Sequence):
    def __init__(self, count):
        self.count = count

    def __len__(self):
        return self.count

    def __getitem__(self, index):
        if not 0 <= index < self.count:
            raise IndexError(index)
        row = share(index)
        row["miner_id"] += "x" * 240
        return row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--shares", type=int, default=375000)
    parser.add_argument("--candidates", type=int, default=3)
    parser.add_argument("--generations", type=int, default=1)
    parser.add_argument("--gc", choices=("on", "off"), default="off")
    args = parser.parse_args()
    if min(args.shares, args.candidates, args.generations) < 1:
        parser.error("counts must be positive")
    schema = "candidate_perf_" + uuid.uuid4().hex
    with psycopg.connect(args.database_url, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
        url = make_conninfo(args.database_url, options=f"-csearch_path={schema}")
        ledger = None
        stop = threading.Event()
        proof_thread = None
        proofs, wakes, errors = [], [], []
        was_enabled = gc.isenabled()
        gc.callbacks.append(_gc_callback)
        try:
            ledger = PsqlShareLedger(
                psql_command="psql", database_url=url, native_client_mode="native",
                writer_id="candidate-storage-probe", initialize_schema=True,
                writer_session_token=WRITER_LEASE_HEARTBEAT_SESSION_PREFIX + uuid.uuid4().hex,
            )
            (gc.enable if args.gc == "on" else gc.disable)()

            def heartbeat():
                due = time.monotonic()
                while not stop.is_set():
                    started = time.monotonic()
                    wakes.append(max(0, started - due))
                    try:
                        ledger.prove_writer_lease_guard_session()
                        proofs.append(time.monotonic() - started)
                    except Exception as exc:
                        errors.append(str(exc))
                    due = time.monotonic() + 0.05
                    stop.wait(max(0, due - time.monotonic()))

            proof_thread = threading.Thread(target=heartbeat)
            proof_thread.start()

            def phase(name, function):
                before = len(proofs), len(wakes), len(errors)
                value = measure(name, function)
                print(json.dumps({
                    "phase": name, "proof_count": len(proofs) - before[0],
                    "max_proof_seconds": max(proofs[before[0]:], default=0),
                    "max_heartbeat_wake_lateness_seconds": max(wakes[before[1]:], default=0),
                    "proof_errors": errors[before[2]:],
                    "spool": ledger._candidate_spool.snapshot(),
                }), flush=True)
                return value

            print(json.dumps({"python": sys.version, "architecture": platform.machine(),
                              "shares": args.shares, "candidates": args.candidates,
                              "gc": args.gc, "generations": args.generations}), flush=True)
            source = Shares(args.shares)
            for index in range(args.candidates):
                block_hash = hashlib.sha256(f"{schema}-{index}".encode()).hexdigest()
                fields = intent(block_hash, 0, credit=False)
                fields["shares_json"] = source
                prepared = phase(f"prepare-{index}", lambda: prepare_candidate_intent(fields))
                print(json.dumps({"candidate": index, "bytes": prepared.manifest.byte_count,
                                  "chunks": prepared.manifest.chunk_count,
                                  "sha256": prepared.candidate_sha256}), flush=True)
                pending = pending_share_for(fields)
                phase(f"append-{index}", lambda: ledger.append_batch([(pending, prepared)]))
                phase(f"exact-retry-{index}", lambda: ledger.append_batch([(pending, prepared)]))
                del prepared, pending, fields
            page = phase("replay-headers", lambda: ledger.pending_block_candidate_headers(limit=args.candidates))
            assert page.exhausted and len(page.rows) == args.candidates
            for generation in range(args.generations):
                for index, row in enumerate(page.rows):
                    hydrated = phase(f"hydrate-{generation}-{index}", lambda: ledger.hydrate_block_candidate_intent(row))
                    sequence = hydrated["shares_json"]
                    def walk():
                        count = 0
                        for count, record in enumerate(sequence, 1):
                            assert record["share_id"] == source[count - 1]["share_id"]
                        assert count == args.shares
                    phase(f"walk-{generation}-{index}", walk)
                    observed = weakref.ref(hydrated.body)
                    phase(f"close-{generation}-{index}", hydrated.body.close)
                    del sequence, hydrated
                    assert observed() is None, "body survives refcount retirement"
                    assert ledger._candidate_spool.snapshot()["reserved_bytes"] == 0
            for row in page.rows:
                phase("terminalize", lambda: ledger.mark_block_candidate_submitted(block_hash=row["block_hash"]))

            def drain():
                steps = 0
                while ledger.reap_retired_candidate_bodies()["body_id"] is not None:
                    steps += 1
                    if steps > 100000:
                        raise AssertionError("janitor did not drain")
                return steps
            phase("janitor", drain)
            assert not errors, errors
            print(json.dumps({"result": "PASS", "proof_errors": errors}), flush=True)
        finally:
            stop.set()
            if proof_thread is not None:
                proof_thread.join()
            gc.callbacks.remove(_gc_callback)
            (gc.enable if was_enabled else gc.disable)()
            if ledger is not None:
                ledger.release_writer_lease()
                ledger.close()
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


if __name__ == "__main__":
    main()
