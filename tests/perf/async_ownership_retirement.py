#!/usr/bin/env python3
"""Isolated #341 failure/drain replay; normal GC, no heap walk or collection.

Run from the repository root with Python 3.14.7, preferably a network-disabled
container. Rows are synthetic canonical payloads of 623 bytes each (including
separator), not a database/native-builder throughput or lease qualification.
Each boundary uses the same real service methods as the lifetime regressions.
"""

from __future__ import annotations

import argparse
from concurrent.futures import Future
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import asdict, replace
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from lab.prism.job_bundle import JobBuildSuperseded, _await_job_build_promise
from lab.prism.process_telemetry import ProcessHeapTelemetry
from tests.prism_async_ownership_support import Ownership, RecordingExecutor, WAIT
from tests.prism_coordinator_test_support import coordinator
from tests.test_prism_initial_job_ownership import InitialJobOwnershipTests
from tests.test_prism_vardiff_idle_ownership import VardiffIdleOwnershipTests

try:
    from lab.prism.window_ownership import window_ownership_snapshot
except ImportError:
    window_ownership_snapshot = None  # #335 dependency, never copy it here.


def rss_bytes():
    statm = Path('/proc/self/statm')
    return (int(statm.read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
            if statm.exists() else None)


class SizedOwnership(Ownership):
    def __init__(self, rows, parsed):
        super().__init__()
        self.rows, self.parsed = rows, parsed
        self.peak = {}

    def window(self, rows=32, *, parsed=False):
        value = super().window(self.rows, parsed=self.parsed)
        counts = self.counts()
        if counts['canonical_bytes'] >= self.peak.get('canonical_bytes', 0):
            self.peak = counts
        return value


def payout_cycle(server, owner):
    executor = RecordingExecutor(owner)
    server._payout_artifact_executor = executor
    original = server._install_payout_ledger_artifact
    original_build = server._build_payout_ledger_artifact

    def build(*args, **kwargs):
        return owner.watch('artifact', replace(
            original_build(*args, **kwargs), shares_json=owner.window()))

    def install(artifact):
        raise ValueError('replay escaped payout install')

    server._build_payout_ledger_artifact = build
    server._install_payout_ledger_artifact = install
    try:
        server._schedule_payout_ledger_artifact_preparation(0, 1000)
        executor.drain()
        assert server._payout_artifact_future is None
        assert server._payout_artifact_requested is None
    finally:
        server._install_payout_ledger_artifact = original
        server._build_payout_ledger_artifact = original_build
        server._payout_ledger_artifact = None
        server._incremental_payout_artifact_window = None
        executor.futures.clear()
        executor.shutdown(wait=True)


def deferred_cycle(server, owner):
    blocker = owner.watch('blocker', Future())
    future = owner.watch('deferred', server._defer_job_build_locked(blocker))
    blocker.set_result(None)

    def consume():
        payload = owner.window()
        try:
            _await_job_build_promise(future, WAIT)
        except JobBuildSuperseded:
            return
        raise AssertionError('capacity completion lost')

    consume()
    consume()  # Late consumer of the same signal.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=400000)
    parser.add_argument('--cycles', type=int, default=6)
    parser.add_argument('--parsed', action='store_true')
    parser.add_argument('--history', type=int, default=2)
    args = parser.parse_args()
    if not 1 <= args.rows <= 400000 or not 1 <= args.cycles <= 20 or not 0 <= args.history <= 2:
        parser.error('bounded replay: rows 1..400000, cycles 1..20, history 0..2')
    assert gc.isenabled(), 'ordinary replay requires normal GC'
    identities = {str(path): hashlib.sha256((ROOT/path).read_bytes()).hexdigest()
                  for path in map(Path, (
                      'lab/prism/payout_state.py', 'lab/prism/job_delivery.py',
                      'lab/prism/job_bundle.py', 'lab/prism/vardiff_service.py',
                      'lab/prism/share_ledger.py', 'tests/prism_async_ownership_support.py',
                      'tests/test_prism_initial_job_ownership.py',
                      'tests/test_prism_vardiff_idle_ownership.py',
                      'tests/perf/async_ownership_retirement.py'))}
    print(json.dumps(dict(python=sys.version, platform=platform.platform(),
                          gc_enabled=True, source_sha256=identities)), flush=True)
    owner = SizedOwnership(args.rows, args.parsed)
    heap = ProcessHeapTelemetry()
    # Declared historical roots, including an alias that must not count twice.
    history = [owner.window() for _ in range(args.history)]
    aliases = list(history)
    baseline = owner.counts()
    retained_services = []
    server, _ = coordinator()
    retained_services.append(server)
    telemetry_baseline = window_ownership_snapshot() if window_ownership_snapshot else None
    with open(os.devnull, 'w') as quiet:
        for cycle in range(args.cycles):
            for flow in ('payout', 'deferred', 'initial', 'vardiff'):
                owner.peak = baseline
                with redirect_stdout(quiet), redirect_stderr(quiet):
                    if flow == 'payout':
                        payout_cycle(server, owner)
                    elif flow == 'deferred':
                        deferred_cycle(server, owner)
                    else:
                        case = (InitialJobOwnershipTests() if flow == 'initial'
                                else VardiffIdleOwnershipTests())
                        # Deliberately do not invoke lifetime-test setUp or
                        # tearDown: this replay never changes GC or collects.
                        case.owner = owner
                        try:
                            case.exercise('failure')
                            retained_services.append(case.server)
                        finally:
                            assert case.doCleanups()
                after = owner.counts()
                telemetry = window_ownership_snapshot() if window_ownership_snapshot else None
                print(json.dumps(dict(cycle=cycle + 1, flow=flow, rows=args.rows,
                    parsed=args.parsed, history=args.history, peak=owner.peak,
                    drained=after, rss_bytes=rss_bytes(),
                    allocator=asdict(heap.sample()),
                    maxrss_native_units=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                    weak_registry=telemetry)), flush=True)
                assert after == baseline, (flow, after, baseline)
                if telemetry is not None:
                    assert telemetry['observations_dropped_total'] == telemetry_baseline['observations_dropped_total']
                    assert telemetry['canonical_bytes'] == telemetry_baseline['canonical_bytes']
                    assert telemetry['parsed_records'] == telemetry_baseline['parsed_records']
                assert gc.isenabled()
    history.clear()
    aliases.clear()
    final = owner.counts()
    assert final == dict(live={}, buffers=0, canonical_bytes=0, parsed_rows=0)
    print(json.dumps(dict(final=final, services_kept_alive=len(retained_services),
                          rss_bytes=rss_bytes(), allocator=asdict(heap.sample()),
                          weak_registry=(window_ownership_snapshot() if window_ownership_snapshot else None),
                          gc_enabled=gc.isenabled())), flush=True)


if __name__ == '__main__':
    main()
