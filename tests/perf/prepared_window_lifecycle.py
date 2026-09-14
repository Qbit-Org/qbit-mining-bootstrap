#!/usr/bin/env python3
"""Isolated #339 coordinator/scheduler/real-daemon decision replay.

The in-memory ledger is real SingleWriterShareLedger; RPC and socket sends
are fixtures. Logical time advances 2s per template and 75s per tip; actual
work runs at host speed. GC stays enabled. No production connection.
Timing is diagnostic, not a production lease qualification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import threading
import time
import weakref
from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism.job_bundle import JobBuildCancellation, JobBuildCancelled
from lab.prism.share_ledger import PendingShare
from tests.perf.window_pipeline_gil_scaling import MonitorLatenessProbe
from tests.prism_coordinator_test_support import client
from tests.test_prism_prepared_window_lifecycle import lifecycle_server, unrelated_build

NOW = 1_760_000_000_000


def pending(index, count, difficulty, *, stamp=None):
    stamp = NOW - count + index - 100 if stamp is None else stamp
    row = PendingShare(
        share_id=f"replay-miner-{index % 3}:{index:064x}",
        miner_id=f"replay-miner-{index % 3}",
        order_key=f"replay-miner-{index % 3}",
        p2mr_program_hex=f"{index % 3 + 1:02x}" * 32,
        share_difficulty=difficulty,
        network_difficulty=65536,
        template_height=9,
        job_id=f"replay-job-{index}",
        job_issued_at_ms=stamp - 1,
        accepted_at_ms=stamp,
        ntime=1_700_000_000,
    )
    # Size-shaped synthetic identities, not captured production share data.
    payload = dict(asdict(row), share_seq=index)
    payload.pop("credit_policy")
    size = len(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return replace(row, job_id=row.job_id + "x" * max(0, 620 - size))


def source_identity():
    binary = Path(os.environ["PRISM_TOOL_BIN_DIR"]) / "qbit-prism-build-audit-bundle"
    with binary.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return {
        "head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "diff_sha256": hashlib.sha256(
            subprocess.check_output(["git", "diff", "HEAD"])
        ).hexdigest(),
        "python": sys.version,
        "platform": platform.platform(),
        "daemon_sha256": digest,
        "files": {
            name: hashlib.sha256(Path(name).read_bytes()).hexdigest()
            for name in (
                "crates/qbit-prism/src/bin/qbit-prism-build-audit-bundle.rs",
                "lab/prism/bundle_compiler.py",
                "lab/prism/payout_state.py",
                "lab/prism/job_bundle.py",
                "lab/prism/tip_refresh.py",
                "lab/prism/window_lifecycle.py",
                "tests/perf/prepared_window_lifecycle.py",
                "tests/test_prism_prepared_window_lifecycle.py",
            )
        },
    }


def owned_windows(server):
    """Declared roots only, no parsing or heap walk. Aliased bytes count once."""
    bundles = server._ensure_job_bundle_service()
    cached = server._incremental_payout_artifact_window
    roots = [server._payout_ledger_artifact, cached, server._prepared_ready_bundle]
    roots.extend(bundles._job_bundle_cache.values())
    roots.extend(server.jobs.values())
    roots.extend(entry.context for entry in server.evicted_job_graveyard.values())
    serialization = bundles._share_window_serialization
    roots.append(serialization._source_artifact if serialization else None)
    assert len(roots) < 1024, "fixture root inventory exceeded its declared bound"
    sequences = {
        id(root.shares_json): root.shares_json
        for root in roots
        if root is not None and hasattr(root, "shares_json")
    }
    buffers, pages, parsed = {}, {}, {}
    if cached is not None and hasattr(cached.window, "canonical_items"):
        buffers[id(cached.window.canonical_items)] = len(cached.window.canonical_items)
    for sequence in sequences.values():
        if hasattr(sequence, "canonical_items"):
            buffers[id(sequence.canonical_items)] = len(sequence.canonical_items)
        for page in getattr(sequence, "pages", ()):
            pages[id(page)] = len(page.records)
            buffers[id(page.canonical_json_items)] = len(page.canonical_json_items)
        tree = getattr(sequence, "_parsed", None)
        if tree is not None:
            parsed[id(tree)] = len(tree)
        elif isinstance(sequence, (list, tuple)):
            parsed[id(sequence)] = len(sequence)
    return {
        "canonical_buffers": len(buffers),
        "canonical_bytes": sum(buffers.values()),
        "pages": len(pages),
        "page_rows": sum(pages.values()),
        "parsed_sequences": len(parsed),
        "parsed_sequence_rows": sum(parsed.values()),
        "jobs": len(server.jobs),
        "graveyard": len(server.evicted_job_graveyard),
    }


def replay(count, cycles, pressure_reads, spool=False):
    measured_source = source_identity()
    server, ledger, artifacts = lifecycle_server()
    if spool:
        if not callable(
            getattr(
                server._ensure_payout_state_service(), "_isolated_window_oracle", None
            )
        ):
            raise RuntimeError("--spool requires an integration checkout with #335")

        # Exercise #335's real helper with a streamed fixture snapshot. The
        # fixture still owns its in-memory ledger; this is not PostgreSQL QA.
        def spool_snapshot(anchor, *, window_weight, sink):
            for record in ledger.snapshot_at_job_issue(
                anchor, window_weight=window_weight
            ):
                sink.append(record.to_prism_json())

        ledger.spool_snapshot_at_job_issue = spool_snapshot
    compiler = server._ensure_bundle_compiler()
    clock = [NOW]
    # Fill the reward window, retaining all requested rows in the 16*difficulty snapshot.
    share_weight = max(1, (8 * int(artifacts.network_difficulty) + count - 1) // count)
    for index in range(1, count + 1):
        ledger.append(pending(index, count, share_weight))
    monitor = MonitorLatenessProbe(0.05)
    stages = []
    drains = []
    artifact_refs = []
    socket_peers = []
    next_connection = [1]
    scan_reasons = Counter()
    original_log = server._payout_artifact_log

    def log(event, **fields):
        if event == "payout_artifact_built" and fields.get("full_rescan_reason"):
            scan_reasons[fields["full_rescan_reason"]] += 1
        original_log(event, **fields)

    server._payout_artifact_log = log

    @contextmanager
    def stage(name, expected):
        before = ledger.full_snapshot_calls
        reasons_before = scan_reasons.copy()
        started = time.monotonic()
        with monitor.phase(name):
            yield
        reads = ledger.full_snapshot_calls - before
        assert reads == expected, (name, reads, expected)
        measured = dict(
            stage=name,
            full_snapshots=reads,
            reasons=dict(scan_reasons - reasons_before),
            seconds=round(time.monotonic() - started, 6),
            **monitor.summary(name),
        )
        with monitor._lock:
            samples = monitor._samples.get(name, ())
            measured["wakes_ge_400ms"] = sum(late >= 0.4 for late in samples)
            measured["wakes_ge_550ms"] = sum(late >= 0.55 for late in samples)
        stages.append(measured)
        print(json.dumps(measured, sort_keys=True), flush=True)

    def window(**kwargs):
        result = server._build_payout_ledger_artifact(
            server._payout_state_generation,
            server._payout_state_generation,
            artifacts.network_difficulty,
            **kwargs,
        )
        assert result is not None
        artifact_refs.append(weakref.ref(result))
        return result

    def install(result):
        assert server._install_payout_ledger_artifact(result)

    def connect():
        state = client(next_connection[0])
        next_connection[0] += 1
        state.sock, peer = socket.socketpair()
        socket_peers.append(peer)
        state.send = lambda _message: (
            None
        )  # Exercise delivery/registry, discard wire bytes.
        server.clients.add(state)
        return state

    def refresh(*, new_tip=False):
        nonlocal artifacts
        clock[0] += 75_000 if new_tip else 2_000
        template = dict(server.rpc.template)
        template["curtime"] = int(template["curtime"]) + (75 if new_tip else 2)
        if new_tip:
            template["height"] = int(template["height"]) + 1
            template["previousblockhash"] = f"{int(template['height']):064x}"
            server.rpc.tip = template["previousblockhash"]
        server.rpc.template = template
        # Drive preparation at each simulated tip so background re-anchoring
        # at the 60s floor does not race this deterministic replay.
        if new_tip:
            install(window(force_prior_balances_read=True))
        server.poll_qbit_tip_template_once()
        artifacts = server.current_template_artifacts()
        assert all(state.active_job is not None for state in server.clients)

    monitor.start()
    try:
        with (
            patch.dict(
                os.environ,
                {"PRISM_WINDOW_PIPELINE_RUST": "1", "PRISM_BUILDER_SERVE": "1"},
            ),
            patch("lab.prism.prism_coordinator.now_ms", side_effect=lambda: clock[0]),
        ):
            with stage("cold", 1):
                initial = window()
                install(initial)
                canonical_bytes = (
                    len(
                        server._incremental_payout_artifact_window.window.canonical_items
                    )
                    + 2
                )
                assert len(initial.shares_json) == count
                initial_digest = initial.share_snapshot_sha256
                del initial
                connect()
                server.poll_qbit_tip_template_once()
            for cycle in range(cycles):
                prefix = f"cycle_{cycle}"
                with stage(prefix + "/templates_clients", 0):
                    for _ in range(3):
                        refresh()
                    old = next(iter(server.clients))
                    server.disconnect_client(old)
                    assert old.closing
                    del old
                    connect()
                    refresh()
                with stage(prefix + "/build_cache_pressure", pressure_reads):
                    process = compiler._serve_builder.process
                    for identity in range(100, 104):
                        unrelated_build(server, identity)
                    clock[0] += 20
                    refreshed = window()
                    assert compiler._serve_builder.process is process
                    install(refreshed)
                    del refreshed
                with stage(prefix + "/payout_tip", 0):
                    refresh(new_tip=True)
                    server._reserve_payout_state_source("payout_only")
                    with server._payout_state_prepare_lock:
                        candidate = server._prepared_payout_state_candidate(
                            server._capture_payout_state_source(),
                            force_full_window_rescan=False,
                            force_prior_balances_read=True,
                        )
                        server._block_payout_state_publication(force=True)
                    assert server._publish_payout_state_candidate(candidate) is not None
                    del candidate
                    if server._payout_artifact_future is not None:
                        server._payout_artifact_future.result(timeout=30)
                with stage(prefix + "/ordinary_append", 0):
                    appended = pending(
                        count + cycle * 2 + 1, count, share_weight, stamp=clock[0] + 1
                    )
                    ledger.append(appended)
                    clock[0] += 20
                    install(window())
                with stage(prefix + "/prepared_eviction", 1):
                    # Competing full preparations supersede the reserved base.
                    row = ledger.snapshot_between_job_issues(
                        NOW - count - 200, NOW - count - 98
                    )[0].to_prism_json()
                    for identity in (200, 201):
                        outcome = server.prepare_payout_window(
                            mode="full",
                            records_json=[
                                dict(row, share_id=f"replacement-{identity}")
                            ],
                            anchor_job_issued_at_ms=clock[0],
                            append_invalidation_epoch=0,
                            window_weight=16 * int(artifacts.network_difficulty),
                        )
                        assert outcome.status == "prepared"
                    del outcome, row
                    clock[0] += 20
                    install(window())
                with stage(prefix + "/process_replacement", 1):
                    compiler._serve_builder.process.kill()
                    compiler._serve_builder.process.wait(timeout=5)
                    clock[0] += 20
                    install(window())
                with stage(prefix + "/busy", 1):
                    process = compiler._serve_builder.process
                    clock[0] += 20
                    with compiler._serve_builder_lock:
                        busy = window(bypass_build_interval=True)
                    assert busy.window_full_rescan_reason == "window_daemon_busy"
                    assert compiler._serve_builder.process is process
                    install(busy)
                    del busy
                with stage(prefix + "/self_check", 1):
                    server.payout_artifact_full_rescan_seconds = 0.0
                    clock[0] += 20
                    checked = window()
                    assert checked.window_build_mode == "self_check_match"
                    install(checked)
                    del checked
                    server.payout_artifact_full_rescan_seconds = 3600.0
                with stage(prefix + "/after_check", 0):
                    clock[0] += 20
                    install(window())
                with stage(prefix + "/late_append", 1):
                    late = pending(
                        count + cycle * 2 + 2, count, share_weight, stamp=NOW - 1
                    )
                    ledger.append(late)
                    server._invalidate_incremental_payout_window_for_append(late)
                    clock[0] += 20
                    install(window())
                with stage(prefix + "/cancel_recover", 1):
                    # Stop only this replay's daemon, then cancel a real request.
                    process = compiler._serve_builder.process
                    os.kill(process.pid, signal.SIGSTOP)
                    cancellation = JobBuildCancellation(timeout_seconds=30)
                    wrote = threading.Event()
                    original_write = compiler._serve_builder_write
                    errors = []

                    def write(*args, _write=original_write, _wrote=wrote, **kwargs):
                        result = _write(*args, **kwargs)
                        _wrote.set()
                        return result

                    def build(_cancellation=cancellation, _errors=errors):
                        try:
                            unrelated_build(server, 300, cancellation=_cancellation)
                        except JobBuildCancelled:
                            _errors.append("cancelled")

                    with patch.object(
                        compiler, "_serve_builder_write", side_effect=write
                    ):
                        thread = threading.Thread(target=build)
                        thread.start()
                        try:
                            assert wrote.wait(5)
                            cancellation.cancel("replay cancellation")
                            thread.join(10)
                            assert not thread.is_alive()
                            assert errors == ["cancelled"]
                        finally:
                            if process.poll() is None:
                                process.kill()
                            thread.join(10)
                            process.wait(timeout=5)
                    clock[0] += 20
                    install(window())
                with stage(prefix + "/ready_refusal", 1):
                    # Deliberate artifact retirement exercises ready fallback.
                    with server._job_cache_lock:
                        server._ensure_payout_state_service()._disarm_payout_ledger_artifact_locked()
                    clock[0] += 20
                    artifacts = server.store_template_artifacts(
                        dict(
                            artifacts.template,
                            curtime=int(artifacts.template["curtime"]) + 1,
                        )
                    )
                    bundle = server.shared_job_bundle(artifacts, mode="ready")
                    assert bundle.prepared_ledger_artifact is not None
                    scan_reasons["ready_absent"] += 1
                    del bundle
                with stage(prefix + "/drain", 0):
                    assert server._job_build_active is None
                    assert server._job_build_retiring is None
                    assert server._job_build_pending is None
                    drains.append(owned_windows(server))
            for state in tuple(server.clients):
                server.disconnect_client(state)
            server.retire_share_window_spool()
            counts, events, suppressed = (
                compiler.window_lifecycle.snapshot()
                if hasattr(compiler, "window_lifecycle")
                else ({}, [], 0)
            )
            return {
                "records": count,
                "canonical_bytes": canonical_bytes,
                "initial_digest": initial_digest,
                "cycles": cycles,
                "spool_helper_route": spool,
                "full_snapshots": ledger.full_snapshot_calls,
                "reasons": dict(scan_reasons),
                "stages": stages,
                "drained_ownership": drains,
                "diagnostics": events,
                "suppressed_logs": suppressed,
                "lifecycle_counts": {"/".join(k): v for k, v in counts.items() if v},
                "tracked_artifacts_alive": sum(
                    ref() is not None for ref in artifact_refs
                ),
                "tracking_scope": "returned payout artifacts only; cache/history are legitimate owners",
                "source": measured_source,
            }
    finally:
        server.shutdown_tip_refresh_executor()
        server.shutdown_job_build_executor()
        compiler.shutdown_serve_builder()
        monitor.stop()
        for peer in socket_peers:
            peer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=375_000)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument(
        "--spool",
        action="store_true",
        help="exercise #335 helper in an integration checkout",
    )
    parser.add_argument(
        "--pressure-reads",
        type=int,
        choices=(0, 1),
        default=0,
        help="Use 1 for the unpatched baseline; 0 is the regression budget",
    )
    parser.add_argument("--json", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.cycles <= 6 or not 20 <= args.records <= 400_000:
        parser.error("bounded replay: 1..6 cycles and 20..400000 records")
    result = replay(args.records, args.cycles, args.pressure_reads, args.spool)
    args.json.write_text(json.dumps(result, indent=2) + "\n")
