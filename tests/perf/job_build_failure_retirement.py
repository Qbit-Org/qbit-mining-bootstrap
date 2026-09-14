#!/usr/bin/env python3
"""Repeated real-scheduler failures/drains with ordinary GC and weak gauges.

Requires #335 in the integration checkout for its actual spool/oracle route
and ownership gauges. No database, network, or production connection. Run as
python -m tests.perf.job_build_failure_retirement --records 228397 400000 --cycles 6
"""

from __future__ import annotations

import argparse
import gc
import json
from pathlib import Path
import platform
import resource
import sys
import time

from tests import test_prism_job_build_exception_retention as harness
from tests.test_prism_job_build_oracle_failure_retention import configure_oracle_failure


def rss_bytes():
    # ru_maxrss is a high-water mark, not current retained ownership.
    if sys.platform == "linux":
        pages = int(Path("/proc/self/statm").read_text().split()[1])
        import os
        return pages * os.sysconf("SC_PAGE_SIZE")
    import subprocess
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(__import__("os").getpid())])) * 1024


def run(records, cycles):
    from lab.prism.window_ownership import window_ownership_snapshot

    assert gc.isenabled(), "workload must use ordinary GC"
    case = harness.JobBuildExceptionRetentionTests()
    case.initialize_scheduler()  # Does not call setUp/tearDown or collect.
    case.server.job_build_executor_workers = 1
    case.server.job_build_timeout_seconds = 180
    references = {}
    baseline = window_ownership_snapshot()
    started = time.monotonic()
    samples = []
    try:
        for cycle in range(cycles):
            before_requests = case.service.job_build_scheduler_counts["requests"]
            reached, release, refs, measured, _ = configure_oracle_failure(
                case, records=records, cycle=cycle, production_rows=True, wait_seconds=180,
            )
            count = 1 if cycle % 2 == 0 else 3
            threads, outcomes = case.run_waiters(count)
            assert reached.wait(180), "compiler boundary not reached"
            harness.wait_until(lambda: case.service.job_build_scheduler_counts["requests"] == before_requests + count)
            at_failure_rss = rss_bytes()
            assert measured["parsed_records"] == 0
            assert measured["at_failure"]["canonical_buffers"] == 1
            assert measured["at_failure"]["canonical_bytes"] == measured["payload_bytes"]
            release.set()
            for thread in threads:
                thread.join(20)
                assert not thread.is_alive()
            # Same long-lived coordinator, scheduler AND executor across all
            # rounds. A following task proves its previous callback returned.
            with case.service._job_build_scheduler_lock:
                executor = case.service._job_build_executor
            executor.submit(lambda: None).result(20)
            assert all(o["kind"] == "unexpected" and o["type"] is ValueError for o in outcomes), outcomes
            assert all(not o["stored_is_error"] for o in outcomes)
            references.update({f"cycle[{cycle}].{k}": v for k, v in refs.items()})
            references.update(case.tracked_references())
            retained = [name for name, ref in references.items() if ref() is not None]
            assert not retained, retained
            assert window_ownership_snapshot() == baseline
            assert case.service._job_build_active is None
            assert case.service._job_build_retiring is None
            assert case.service._job_build_pending is None
            assert case.server._payout_window_inflight_scan_anchors == {}
            sample = dict(cycle=cycle, waiters=count, payload_bytes=measured["payload_bytes"],
                          canonical_buffers_after_drain=0, canonical_bytes_after_drain=0,
                          parsed_records_after_drain=0, page_records_after_drain=0,
                          obsolete_references_after_drain=len(retained),
                          rss_at_failure_bytes=at_failure_rss, rss_after_drain_bytes=rss_bytes())
            samples.append(sample)
            print(json.dumps(dict(records=records, **sample)), flush=True)
        assert case.service.shared_bundle_build_counts["failed"] == cycles
        assert case.service.job_build_scheduler_counts["starts"] == cycles
        assert gc.isenabled()
    finally:
        case.server.shutdown_job_build_executor()
    return dict(records=records, cycles=cycles, seconds=time.monotonic() - started,
                gc_enabled=gc.isenabled(), forced_collections=0,
                ownership_plateau_bytes=0, parsed_rows_plateau=0,
                rss_drain_min_bytes=min(s["rss_after_drain_bytes"] for s in samples),
                rss_drain_max_bytes=max(s["rss_after_drain_bytes"] for s in samples),
                ru_maxrss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                ru_maxrss_unit="bytes" if sys.platform == "darwin" else "KiB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, nargs="+", default=[228397, 400000])
    parser.add_argument("--cycles", type=int, default=6)
    args = parser.parse_args()
    print(json.dumps(dict(python=sys.version, platform=platform.platform(), gc_enabled=gc.isenabled())), flush=True)
    for count in args.records:
        print(json.dumps(run(count, args.cycles)), flush=True)
