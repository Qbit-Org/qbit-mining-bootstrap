#!/usr/bin/env python3
"""PRISM metrics renderer extraction tests.

The frozen reference implementations below are byte-copied from the
coordinator formatter bodies as they stood immediately before the PR 80
metrics extraction (``self`` renamed to ``server``).  The full-render parity
test proves the extracted ``MetricsRenderer`` reproduces the pre-extraction
Prometheus document byte-for-byte for identical inputs.
"""

from __future__ import annotations

from collections import OrderedDict
from contextlib import redirect_stdout
from decimal import Decimal
import gc
import hashlib
from io import StringIO
from pathlib import Path
import re
import tempfile
import time
import tracemalloc
import unittest
from types import SimpleNamespace
from unittest import mock

from lab.prism.coordinator_config import (
    DEFAULT_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX,
    DEFAULT_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX,
    DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
    DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS,
)
from lab.prism.accepted_preview_telemetry import (
    PRISM_ACCEPTED_LANDING_PHASES,
    PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS,
    PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS,
    PRISM_REORG_RECONCILE_CALLERS,
    PRISM_REORG_RECONCILE_STEPS,
    AcceptedPreviewTelemetry,
    ensure_accepted_preview_telemetry,
    fold_ledger_read_stats,
)
from lab.prism.block_candidates import (
    PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS,
    PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS,
    PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES,
    PRISM_STALE_JOB_ABANDON_CLASSES,
)
from lab.prism.bundle_compiler import _ServeBuilderClient, _ShareWindowSerialization
from lab.prism.job_bundle import (
    PRISM_JOB_BUILD_PHASES,
    PRISM_JOB_BUILD_SECONDS_BUCKETS,
    PRISM_JOB_CACHE_KINDS,
)
from lab.prism.job_delivery import (
    PRISM_EVICTED_JOB_CAPACITY_SCOPES,
    PRISM_EVICTED_JOB_CLASSES,
    PRISM_EVICTED_JOB_SUBMIT_OUTCOMES,
)
from lab.prism.metrics import (
    PRISM_COMPONENT_BYTE_KINDS,
    PRISM_COMPONENT_ENTRY_KINDS,
    MetricsRenderer,
)
from lab.prism.payout_state import (
    AcceptedBlockPayoutTransition,
    PayoutLedgerArtifact,
    _IncrementalPayoutArtifactWindow,
)
from lab.prism.prism_coordinator import PRISM_REJECTION_REASON_IDS
from lab.prism.process_telemetry import (
    PRISM_GC_GENERATIONS,
    MallInfo2,
    ProcessHeapSample,
    ProcessHeapTelemetry,
)
from lab.prism.reorg_reconciler import (
    PRISM_REORG_RECONCILE_LOOKUP_PATHS,
    PRISM_REORG_RECONCILE_LOOKUP_SOURCES,
    _ReconcileFlight,
)
from lab.prism.share_ledger import (
    DaemonShareWindowMirror,
    IncrementalShareJsonSequence,
    IncrementalShareWindow,
    _IncrementalShareWindowPage,
)
from lab.prism.share_submission import (
    PRISM_SHARE_ACK_RESULTS,
    empty_share_ack_histograms,
)
from lab.prism.vardiff_service import PRISM_VARDIFF_RESUME_OUTCOMES
from lab.prism.writer_lease_timing import (
    LEASE_HEARTBEAT_MODES,
    LEASE_HEARTBEAT_OUTCOMES,
    LEASE_HEARTBEAT_PHASES,
    LEASE_HEARTBEAT_POLICY_TERMS,
    LEASE_MONITOR_LATE_WAKE_SLACK_FRACTIONS,
    LEASE_MONITOR_WAKE_DELAY_BUCKETS,
)
from lab.prism.background_services import PRISM_GC_PAUSE_SECONDS_BUCKETS
from tests import prism_vardiff_test_support as support


def reference_share_ack_metrics_lines(server) -> list[str]:
    service = server.__dict__.get("_share_submission_service")
    if service is not None:
        histograms = service.share_ack_snapshot()
    else:
        legacy = server.__dict__.get("share_ack_histograms")
        histograms = (
            {
                result: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for result, histogram in legacy.items()
            }
            if legacy is not None
            else empty_share_ack_histograms()
        )
    lines = [
        "# HELP qbit_prism_share_ack_seconds mining.submit line arrival to Stratum response, by outcome.",
        "# TYPE qbit_prism_share_ack_seconds histogram",
    ]
    for result in PRISM_SHARE_ACK_RESULTS:
        histogram = histograms[result]
        buckets = histogram["buckets"]
        lines.extend(
            f'qbit_prism_share_ack_seconds_bucket{{result="{result}",le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
            for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS
        )
        lines.append(
            f'qbit_prism_share_ack_seconds_bucket{{result="{result}",le="+Inf"}} {histogram["count"]}'
        )
        lines.append(
            f'qbit_prism_share_ack_seconds_sum{{result="{result}"}} {histogram["sum"]:.6f}'
        )
        lines.append(
            f'qbit_prism_share_ack_seconds_count{{result="{result}"}} {histogram["count"]}'
        )
    return lines


def reference_coordinator_lock_metrics_lines(server) -> list[str]:
    snapshot = getattr(server.lock, "contention_snapshot", None)
    if callable(snapshot):
        acquisition_count, contention_count, wait_sum, wait_max = snapshot()
    else:
        acquisition_count, contention_count, wait_sum, wait_max = 0, 0, 0.0, 0.0
    return [
        "# HELP qbit_prism_coordinator_lock_acquisitions_total Coordinator control-plane lock acquisitions granted, contended or not; divide qbit_prism_coordinator_lock_contentions_total by this for the contended fraction. A timed-out wait is counted as contention without an acquisition, so that fraction is an upper bound where a caller passes a timeout.",
        "# TYPE qbit_prism_coordinator_lock_acquisitions_total counter",
        f"qbit_prism_coordinator_lock_acquisitions_total {int(acquisition_count)}",
        "# HELP qbit_prism_coordinator_lock_contentions_total Coordinator control-plane lock acquisitions that had to wait.",
        "# TYPE qbit_prism_coordinator_lock_contentions_total counter",
        f"qbit_prism_coordinator_lock_contentions_total {int(contention_count)}",
        "# HELP qbit_prism_coordinator_lock_wait_seconds Coordinator control-plane lock wait duration for contended acquisitions.",
        "# TYPE qbit_prism_coordinator_lock_wait_seconds summary",
        f"qbit_prism_coordinator_lock_wait_seconds_sum {float(wait_sum):.6f}",
        f"qbit_prism_coordinator_lock_wait_seconds_count {int(contention_count)}",
        "# HELP qbit_prism_coordinator_lock_wait_seconds_max Longest observed coordinator control-plane lock wait.",
        "# TYPE qbit_prism_coordinator_lock_wait_seconds_max gauge",
        f"qbit_prism_coordinator_lock_wait_seconds_max {float(wait_max):.6f}",
    ]


def reference_block_submitter_metrics_lines(server) -> list[str]:
    pending_metrics = {
        "pending_count": -1,
        "oldest_pending_age_seconds": -1.0,
        "oldest_unattempted_age_seconds": -1.0,
    }
    pending_snapshot = getattr(server.ledger, "block_candidate_pending_metrics", None)
    if callable(pending_snapshot):
        try:
            pending_metrics.update(pending_snapshot())
        except Exception:
            pass
    service = server._ensure_block_candidate_service()
    backoff_active, backoff_remaining, backoff_delay = (
        service.backoff_snapshot()
    )
    submit_buckets, submit_sum, submit_count = (
        service.block_submit_seconds_snapshot()
    )
    collapse_counts = service.block_candidate_collapse_snapshot()
    preview_publication = service.accepted_block_preview_publication_snapshot()
    preview_publication_lines: list[str] = []
    for result in PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS:
        histogram = preview_publication[result]
        buckets = histogram["buckets"]
        count = int(histogram["count"])
        preview_publication_lines.extend(
            f"qbit_prism_accepted_block_preview_publication_seconds_bucket"
            f'{{result="{result}",le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
            for bucket in PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS
        )
        preview_publication_lines.append(
            f"qbit_prism_accepted_block_preview_publication_seconds_bucket"
            f'{{result="{result}",le="+Inf"}} {count}'
        )
        preview_publication_lines.append(
            f"qbit_prism_accepted_block_preview_publication_seconds_sum"
            f'{{result="{result}"}} {float(histogram["sum"]):.6f}'
        )
        preview_publication_lines.append(
            f"qbit_prism_accepted_block_preview_publication_seconds_count"
            f'{{result="{result}"}} {count}'
        )
    return [
        "# HELP qbit_prism_block_submit_seconds Seconds from a block candidate landing in this process to its submitblock RPC returning.",
        "# TYPE qbit_prism_block_submit_seconds histogram",
        *[
            f'qbit_prism_block_submit_seconds_bucket{{le="{bucket:g}"}} {int(submit_buckets.get(bucket, 0))}'
            for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS
        ],
        f'qbit_prism_block_submit_seconds_bucket{{le="+Inf"}} {submit_count}',
        f"qbit_prism_block_submit_seconds_sum {submit_sum:.6f}",
        f"qbit_prism_block_submit_seconds_count {submit_count}",
        "# HELP qbit_prism_accepted_block_preview_publication_seconds Seconds from definitive qbitd acceptance of a block candidate to its payout preview becoming visible to waiting child work, by publication result.",
        "# TYPE qbit_prism_accepted_block_preview_publication_seconds histogram",
        *preview_publication_lines,
        "# HELP qbit_prism_block_candidates_pending Durable block candidates awaiting a terminal outcome, or -1 if unavailable.",
        "# TYPE qbit_prism_block_candidates_pending gauge",
        f"qbit_prism_block_candidates_pending {int(pending_metrics['pending_count'])}",
        "# HELP qbit_prism_block_candidate_oldest_pending_seconds Age of the oldest durable pending block candidate, or -1 if unavailable.",
        "# TYPE qbit_prism_block_candidate_oldest_pending_seconds gauge",
        f"qbit_prism_block_candidate_oldest_pending_seconds {float(pending_metrics['oldest_pending_age_seconds']):.6f}",
        "# HELP qbit_prism_block_candidate_oldest_unattempted_seconds Age of the oldest durable candidate that has never entered processing, or -1 if unavailable.",
        "# TYPE qbit_prism_block_candidate_oldest_unattempted_seconds gauge",
        f"qbit_prism_block_candidate_oldest_unattempted_seconds {float(pending_metrics['oldest_unattempted_age_seconds']):.6f}",
        "# HELP qbit_prism_block_submitter_retry_backoff_active Whether the submitter is in an intentional interruptible retry wait.",
        "# TYPE qbit_prism_block_submitter_retry_backoff_active gauge",
        f"qbit_prism_block_submitter_retry_backoff_active {1 if backoff_active else 0}",
        "# HELP qbit_prism_block_submitter_retry_backoff_remaining_seconds Remaining intentional submitter retry wait.",
        "# TYPE qbit_prism_block_submitter_retry_backoff_remaining_seconds gauge",
        f"qbit_prism_block_submitter_retry_backoff_remaining_seconds {backoff_remaining:.6f}",
        "# HELP qbit_prism_block_submitter_retry_backoff_seconds Current intentional submitter retry delay.",
        "# TYPE qbit_prism_block_submitter_retry_backoff_seconds gauge",
        f"qbit_prism_block_submitter_retry_backoff_seconds {backoff_delay:.6f}",
        "# HELP qbit_prism_block_candidate_collapse_total Durable pending block candidates handled by the decided-height collapse, by bounded outcome. The outcome label set is closed: no block hash, parent hash, or job ID is ever a label.",
        "# TYPE qbit_prism_block_candidate_collapse_total counter",
        *[
            f'qbit_prism_block_candidate_collapse_total{{outcome="{outcome}"}} {int(collapse_counts.get(outcome, 0))}'
            for outcome in PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES
        ],
    ]


def reference_block_candidate_cleanup_backlog_metrics_lines(server) -> list[str]:
    # Issue #198. Pinned deliberately: seven unlabelled single-series
    # families over the B1 owner's fixed-key backlog snapshot. Adding a
    # label, a key, or a family here is a metrics-contract change and must
    # be mirrored in docs/prism-overload-alerts.md.
    snapshot = (
        server._ensure_block_candidate_service()
        .collapsed_candidate_cleanup_backlog_snapshot()
    )
    return [
        "# HELP qbit_prism_block_candidate_cleanup_retry_backlog Durably terminal collapsed block candidates whose in-memory cleanup is still owed and retried by the accounting lane.",
        "# TYPE qbit_prism_block_candidate_cleanup_retry_backlog gauge",
        f"qbit_prism_block_candidate_cleanup_retry_backlog {int(snapshot['depth'])}",
        "# HELP qbit_prism_block_candidate_cleanup_retry_backlog_max Configured cleanup-retry backlog depth at which the decided-height collapse stops admitting rows to bulk terminalization.",
        "# TYPE qbit_prism_block_candidate_cleanup_retry_backlog_max gauge",
        f"qbit_prism_block_candidate_cleanup_retry_backlog_max {int(snapshot['backlog_max'])}",
        "# HELP qbit_prism_block_candidate_cleanup_retry_oldest_seconds Age of the oldest owed collapse cleanup since it was first deferred, or -1 when none is owed.",
        "# TYPE qbit_prism_block_candidate_cleanup_retry_oldest_seconds gauge",
        f"qbit_prism_block_candidate_cleanup_retry_oldest_seconds {float(snapshot['oldest_age_seconds']):.6f}",
        "# HELP qbit_prism_block_candidate_cleanup_retry_pending_share_holders Pending-share floor holders retained by owed collapse cleanups, by exact object identity.",
        "# TYPE qbit_prism_block_candidate_cleanup_retry_pending_share_holders gauge",
        f"qbit_prism_block_candidate_cleanup_retry_pending_share_holders {int(snapshot['pending_share_holders'])}",
        "# HELP qbit_prism_block_candidate_cleanup_retry_terminal_outcome_pins Terminal-outcome fences the cleanup-retry backlog pins against eviction.",
        "# TYPE qbit_prism_block_candidate_cleanup_retry_terminal_outcome_pins gauge",
        f"qbit_prism_block_candidate_cleanup_retry_terminal_outcome_pins {int(snapshot['terminal_outcome_pins'])}",
        "# HELP qbit_prism_block_candidate_cleanup_backpressure_active Whether the cleanup-retry backlog is at its bound and bulk terminalization is refusing new rows.",
        "# TYPE qbit_prism_block_candidate_cleanup_backpressure_active gauge",
        f"qbit_prism_block_candidate_cleanup_backpressure_active {1 if snapshot['backpressure_active'] else 0}",
        "# HELP qbit_prism_block_candidate_cleanup_backpressure_total Occasions on which the cleanup-retry backlog bound preserved at least one row from bulk terminalization.",
        "# TYPE qbit_prism_block_candidate_cleanup_backpressure_total counter",
        f"qbit_prism_block_candidate_cleanup_backpressure_total {int(snapshot['backpressure_engagements'])}",
    ]


# The closed family set issue #198 exports; a test pins the renderer to it.
BLOCK_CANDIDATE_CLEANUP_BACKLOG_METRIC_FAMILIES = (
    ("qbit_prism_block_candidate_cleanup_retry_backlog", "gauge"),
    ("qbit_prism_block_candidate_cleanup_retry_backlog_max", "gauge"),
    ("qbit_prism_block_candidate_cleanup_retry_oldest_seconds", "gauge"),
    ("qbit_prism_block_candidate_cleanup_retry_pending_share_holders", "gauge"),
    ("qbit_prism_block_candidate_cleanup_retry_terminal_outcome_pins", "gauge"),
    ("qbit_prism_block_candidate_cleanup_backpressure_active", "gauge"),
    ("qbit_prism_block_candidate_cleanup_backpressure_total", "counter"),
)


# Issue #226 part 1: the closed family sets of the always-on heap telemetry
# and the component-cardinality gauges; tests pin the renderer to both.
PROCESS_HEAP_METRIC_FAMILIES = (
    ("qbit_prism_process_allocated_blocks", "gauge"),
    ("qbit_prism_process_gc_trigger_count", "gauge"),
    ("qbit_prism_process_gc_collections_total", "counter"),
    ("qbit_prism_process_gc_collected_objects_total", "counter"),
    ("qbit_prism_process_gc_uncollectable_objects_total", "counter"),
    ("qbit_prism_process_threads", "gauge"),
    ("qbit_prism_process_malloc_info_available", "gauge"),
    ("qbit_prism_process_malloc_arena_bytes", "gauge"),
    ("qbit_prism_process_malloc_in_use_bytes", "gauge"),
    ("qbit_prism_process_malloc_free_bytes", "gauge"),
    ("qbit_prism_process_malloc_mmapped_bytes", "gauge"),
)
# The only labelled heap families; their sole label is the closed
# three-value generation set.
PROCESS_HEAP_GENERATION_FAMILIES = frozenset(
    {
        "qbit_prism_process_gc_trigger_count",
        "qbit_prism_process_gc_collections_total",
        "qbit_prism_process_gc_collected_objects_total",
        "qbit_prism_process_gc_uncollectable_objects_total",
    }
)
COMPONENT_CARDINALITY_METRIC_FAMILIES = (
    ("qbit_prism_component_entries", "gauge"),
    ("qbit_prism_component_bytes", "gauge"),
)
# The parity fixture pins the collector to this sample: allocation and GC
# counters move between the reference and the actual render.
PINNED_HEAP_SAMPLE = ProcessHeapSample(
    allocated_blocks=123_456,
    gc_trigger_count=(5, 6, 7),
    gc_collections=(100, 10, 1),
    gc_collected=(1_000, 100, 10),
    gc_uncollectable=(0, 0, 2),
    threads=64,
    malloc_info_available=True,
    malloc_arena_bytes=9_000,
    malloc_in_use_bytes=6_000,
    malloc_free_bytes=3_000,
    malloc_mmapped_bytes=500,
)


def reference_process_heap_metrics_lines(sample: ProcessHeapSample) -> list[str]:
    lines = [
        "# HELP qbit_prism_process_allocated_blocks Memory blocks currently allocated by the CPython allocator (sys.getallocatedblocks); a live-object proxy that rises with retention and stays flat under pure allocator fragmentation.",
        "# TYPE qbit_prism_process_allocated_blocks gauge",
        f"qbit_prism_process_allocated_blocks {int(sample.allocated_blocks)}",
        "# HELP qbit_prism_process_gc_trigger_count gc.get_count(): the cyclic collector's per-generation trigger counters, compared against gc.get_threshold() to decide when to collect. Generation 0 is allocations minus deallocations, NOT a count of retained objects. CPython 3.13 made the collector incremental, so the third entry is unused and reads 0; the series is kept for a fixed family set.",
        "# TYPE qbit_prism_process_gc_trigger_count gauge",
    ]
    lines.extend(
        f'qbit_prism_process_gc_trigger_count{{generation="{generation}"}} {int(count)}'
        for generation, count in zip(PRISM_GC_GENERATIONS, sample.gc_trigger_count)
    )
    lines.extend(
        [
            "# HELP qbit_prism_process_gc_collections_total Cyclic collector passes completed per generation since process start (gc.get_stats).",
            "# TYPE qbit_prism_process_gc_collections_total counter",
        ]
    )
    lines.extend(
        f'qbit_prism_process_gc_collections_total{{generation="{generation}"}} {int(count)}'
        for generation, count in zip(PRISM_GC_GENERATIONS, sample.gc_collections)
    )
    lines.extend(
        [
            "# HELP qbit_prism_process_gc_collected_objects_total Objects the cyclic collector freed per generation since process start.",
            "# TYPE qbit_prism_process_gc_collected_objects_total counter",
        ]
    )
    lines.extend(
        f'qbit_prism_process_gc_collected_objects_total{{generation="{generation}"}} {int(count)}'
        for generation, count in zip(PRISM_GC_GENERATIONS, sample.gc_collected)
    )
    lines.extend(
        [
            "# HELP qbit_prism_process_gc_uncollectable_objects_total Objects the cyclic collector found unreachable but could not free per generation since process start; growth is a leak the collector already knows about.",
            "# TYPE qbit_prism_process_gc_uncollectable_objects_total counter",
        ]
    )
    lines.extend(
        f'qbit_prism_process_gc_uncollectable_objects_total{{generation="{generation}"}} {int(count)}'
        for generation, count in zip(PRISM_GC_GENERATIONS, sample.gc_uncollectable)
    )
    lines.extend(
        [
            "# HELP qbit_prism_process_threads Live Python threads (threading.active_count); each one is a candidate glibc malloc arena.",
            "# TYPE qbit_prism_process_threads gauge",
            f"qbit_prism_process_threads {int(sample.threads)}",
            "# HELP qbit_prism_process_malloc_info_available Whether glibc mallinfo2 is bound and PRISM_MALLOC_TELEMETRY is on (1), or the allocator byte gauges are unavailable (0): the symbol is glibc 2.33+ only and absent on musl and macOS. While 0 the byte gauges render -1, never 0.",
            "# TYPE qbit_prism_process_malloc_info_available gauge",
            f"qbit_prism_process_malloc_info_available {1 if sample.malloc_info_available else 0}",
            "# HELP qbit_prism_process_malloc_arena_bytes Heap space glibc malloc holds for its arenas across every thread arena, excluding mmapped chunks (mallinfo2.arena), or -1 when unavailable.",
            "# TYPE qbit_prism_process_malloc_arena_bytes gauge",
            f"qbit_prism_process_malloc_arena_bytes {int(sample.malloc_arena_bytes)}",
            "# HELP qbit_prism_process_malloc_in_use_bytes Bytes in allocated chunks across every glibc malloc arena (mallinfo2.uordblks), or -1 when unavailable; arena minus this is the fragmentation glibc has not returned to the kernel.",
            "# TYPE qbit_prism_process_malloc_in_use_bytes gauge",
            f"qbit_prism_process_malloc_in_use_bytes {int(sample.malloc_in_use_bytes)}",
            "# HELP qbit_prism_process_malloc_free_bytes Bytes in free chunks glibc malloc retains across every arena (mallinfo2.fordblks), or -1 when unavailable.",
            "# TYPE qbit_prism_process_malloc_free_bytes gauge",
            f"qbit_prism_process_malloc_free_bytes {int(sample.malloc_free_bytes)}",
            "# HELP qbit_prism_process_malloc_mmapped_bytes Bytes in chunks glibc malloc served directly through mmap (mallinfo2.hblkhd), or -1 when unavailable.",
            "# TYPE qbit_prism_process_malloc_mmapped_bytes gauge",
            f"qbit_prism_process_malloc_mmapped_bytes {int(sample.malloc_mmapped_bytes)}",
        ]
    )
    return lines


def reference_component_cardinality_metrics_lines(server) -> list[str]:
    server._ensure_job_cache_state()
    bundles = server._ensure_job_bundle_service()
    payout = server._ensure_payout_state_service()
    writer = server._ensure_share_writer_service()
    compiler = server._ensure_bundle_compiler()
    candidates = server._ensure_block_candidate_service()
    candidates._ensure_block_replay_state()
    candidates._ensure_block_candidate_disposition_state()
    reconciler = server.__dict__.get("_reorg_reconciler_service")
    serialization = bundles._share_window_serialization
    cached_window = payout._incremental_payout_artifact_window
    artifact = payout._payout_ledger_artifact
    client = compiler._serve_builder
    window = cached_window.window if cached_window is not None else None
    pages = records = canonical = mirror_records = mirror_bytes = 0
    if isinstance(window, DaemonShareWindowMirror):
        # The mirror is the cached window on the Rust path, so it is accounted
        # once, under daemon_window_mirror_*; payout_window_* stays zero.
        mirror_records = int(window.record_count)
        mirror_bytes = len(window.canonical_items)
    elif window is not None:
        pages = len(window.pages)
        records = sum(len(page.records) for page in window.pages)
        canonical = sum(len(page.canonical_json_items) for page in window.pages)
    with server.lock:
        server._ensure_evicted_job_state()
        locked = {
            "evicted_job_graveyard": len(server.evicted_job_graveyard),
            "evicted_same_tip_job_ids": len(server.evicted_same_tip_job_ids),
            "outstanding_block_candidate_hashes": len(
                candidates._outstanding_block_candidate_hashes
            ),
            "tip_observed_accepted_block_hashes": len(
                candidates._tip_observed_accepted_block_hashes
            ),
            "counted_block_candidate_abandonments": len(
                candidates._counted_block_candidate_abandonments
            ),
            "ancestor_redrive_records": len(candidates._ancestor_redrive_records),
            "block_candidate_terminal_outcomes": len(
                candidates._block_candidate_terminal_outcomes
            ),
            "block_disposition_waiting_retries": len(
                candidates._block_disposition_waiting_retries
            ),
            "block_candidate_dequeued_hashes": len(
                candidates._block_candidate_dequeued_hashes
            ),
            "accounted_accepted_block_hashes": len(
                server._accounted_accepted_block_hashes
            ),
            "reconcile_trusted_memo": (
                len(reconciler._reorg_reconcile_trusted_memo)
                if reconciler is not None
                else 0
            ),
        }
    entries = {
        "payout_window_pages": pages,
        "payout_window_records": records,
        "payout_ledger_artifact_shares": (
            len(artifact.shares_json) if artifact is not None else 0
        ),
        "accepted_block_payout_previews": len(payout._accepted_block_payout_previews),
        "invalidated_accepted_block_payout_previews": len(
            payout._invalidated_accepted_block_payout_previews
        ),
        "payout_append_invalidation_stamps": len(
            payout._payout_append_invalidation_stamps
        ),
        "payout_window_inflight_scan_anchors": len(
            payout._payout_window_inflight_scan_anchors
        ),
        "payout_unfenced_append_inflight_stamps": len(
            payout._payout_unfenced_append_inflight_stamps
        ),
        "share_window_serialization_shares": (
            int(serialization.share_count) if serialization is not None else 0
        ),
        "job_contexts": len(getattr(server, "jobs", {})),
        "job_bundle_cache": len(bundles._job_bundle_cache),
        "job_build_issued_stamps": len(bundles._job_build_issued_at_ms),
        "bundle_preparation_flights": len(bundles._bundle_preparation_flights),
        "active_job_bundle_builds": len(bundles._active_job_bundle_builds),
        "block_candidate_queue": candidates.candidate_queue.qsize(),
        "block_replay_queue": candidates._block_replay_candidate_queue.qsize(),
        "block_replay_inflight_hashes": len(candidates._block_replay_inflight_hashes),
        "block_quarantine_queue": candidates._block_quarantine_queue.qsize(),
        "block_quarantine_hashes": len(candidates._block_quarantine_hashes),
        "accepted_block_preview_stamps": len(
            candidates._accepted_block_preview_acceptance_monotonic
        ),
        "block_candidate_disposition_flights": len(
            candidates._block_candidate_disposition_flights
        ),
        "reconcile_flights": (
            len(reconciler._reconcile_flights) if reconciler is not None else 0
        ),
        "pending_share_commit_floor": len(writer._pending_share_commit_floor),
        "daemon_window_mirror_records": mirror_records,
        "daemon_uploaded_windows": (
            len(client.uploaded_windows) if client is not None else 0
        ),
        **locked,
    }
    byte_sizes = {
        "payout_window_canonical_json": canonical,
        "daemon_window_mirror_canonical_items": mirror_bytes,
        "share_window_serialization_spool": (
            int(serialization._spool_size) if serialization is not None else 0
        ),
        "share_window_serialization_compact_json": (
            len(serialization._compact_shares_json or "")
            + len(serialization._compact_share_identities_json or "")
            if serialization is not None
            else 0
        ),
    }
    return [
        "# HELP qbit_prism_component_entries Entries retained by each in-process coordinator component; the component label set is closed and carries no job id, tip hash, generation, or worker name.",
        "# TYPE qbit_prism_component_entries gauge",
        *[
            f'qbit_prism_component_entries{{component="{kind}"}} {int(entries[kind])}'
            for kind in PRISM_COMPONENT_ENTRY_KINDS
        ],
        "# HELP qbit_prism_component_bytes Bytes retained by each byte-sized coordinator payload (the cached payout window's canonical JSON, the daemon window mirror, and the share-window serialization compact strings); the component label set is closed. All are resident bytes except share_window_serialization_spool, which is an on-disk temporary file and is not part of RSS.",
        "# TYPE qbit_prism_component_bytes gauge",
        *[
            f'qbit_prism_component_bytes{{component="{kind}"}} {int(byte_sizes[kind])}'
            for kind in PRISM_COMPONENT_BYTE_KINDS
        ],
    ]


def reference_landing_observability_metrics_lines(server) -> list[str]:
    lines: list[str] = [
        "# HELP qbit_prism_block_ledger_calls_total Submitter ledger calls by deadline class.",
        "# TYPE qbit_prism_block_ledger_calls_total counter",
        "# HELP qbit_prism_block_ledger_call_timeouts_total Submitter ledger deadline expiries by deadline class.",
        "# TYPE qbit_prism_block_ledger_call_timeouts_total counter",
        "# HELP qbit_prism_block_ledger_call_budget_seconds Most recent statement budget applied per deadline class (escalates for retried landings).",
        "# TYPE qbit_prism_block_ledger_call_budget_seconds gauge",
        "# HELP qbit_prism_block_ledger_call_last_duration_seconds Duration of the most recent call per deadline class.",
        "# TYPE qbit_prism_block_ledger_call_last_duration_seconds gauge",
        "# HELP qbit_prism_block_ledger_call_max_duration_seconds Longest observed call per deadline class since process start.",
        "# TYPE qbit_prism_block_ledger_call_max_duration_seconds gauge",
    ]
    for call_class, stats in sorted(
        server.block_ledger_call_class_metrics().items()
    ):
        label = server.prometheus_label_value(call_class)
        lines.extend(
            [
                f'qbit_prism_block_ledger_calls_total{{call_class="{label}"}} {int(stats["calls_total"])}',
                f'qbit_prism_block_ledger_call_timeouts_total{{call_class="{label}"}} {int(stats["timeouts_total"])}',
                f'qbit_prism_block_ledger_call_budget_seconds{{call_class="{label}"}} {float(stats["last_budget_seconds"]):.6f}',
                f'qbit_prism_block_ledger_call_last_duration_seconds{{call_class="{label}"}} {float(stats["last_duration_seconds"]):.6f}',
                f'qbit_prism_block_ledger_call_max_duration_seconds{{call_class="{label}"}} {float(stats["max_duration_seconds"]):.6f}',
            ]
        )
    unresolved_depth = server._accepted_parent_unresolved_depth()
    unresolved_depth_cap = server._accepted_parent_unresolved_depth_cap()
    unresolved_ages = server.accepted_parent_unresolved_ages_seconds()
    oldest_unresolved = max(unresolved_ages) if unresolved_ages else -1.0
    with server.lock:
        preview_wait_timeouts = int(
            getattr(server, "_accepted_parent_preview_wait_timeouts", 0)
        )
    lines.extend(
        [
            "# HELP qbit_prism_accepted_parent_unresolved_transitions Landed accepted-block transitions whose durable bookkeeping is unresolved.",
            "# TYPE qbit_prism_accepted_parent_unresolved_transitions gauge",
            f"qbit_prism_accepted_parent_unresolved_transitions {unresolved_depth}",
            "# HELP qbit_prism_accepted_parent_unresolved_depth_max Configured unresolved accepted-parent depth at which job admission fails closed.",
            "# TYPE qbit_prism_accepted_parent_unresolved_depth_max gauge",
            f"qbit_prism_accepted_parent_unresolved_depth_max {unresolved_depth_cap}",
            "# HELP qbit_prism_accepted_parent_unresolved_oldest_seconds Age of the oldest unresolved accepted-parent transition, or -1 when none.",
            "# TYPE qbit_prism_accepted_parent_unresolved_oldest_seconds gauge",
            f"qbit_prism_accepted_parent_unresolved_oldest_seconds {oldest_unresolved:.6f}",
            "# HELP qbit_prism_accepted_parent_preview_wait_timeouts_total Child job builds that timed out waiting for an accepted-parent payout preview.",
            "# TYPE qbit_prism_accepted_parent_preview_wait_timeouts_total counter",
            f"qbit_prism_accepted_parent_preview_wait_timeouts_total {preview_wait_timeouts}",
            "# HELP qbit_prism_accepted_parent_redrive_attempts_total In-process ancestor re-drives armed after repeated finalization deferrals on one pending accepted-parent transition.",
            "# TYPE qbit_prism_accepted_parent_redrive_attempts_total counter",
            "qbit_prism_accepted_parent_redrive_attempts_total "
            f"{int(getattr(server, 'accepted_parent_redrive_attempt_count', 0))}",
            "# HELP qbit_prism_accepted_parent_redrive_resolved_total Pending accepted-parent transitions that resolved after at least one in-process re-drive attempt.",
            "# TYPE qbit_prism_accepted_parent_redrive_resolved_total counter",
            "qbit_prism_accepted_parent_redrive_resolved_total "
            f"{int(getattr(server, 'accepted_parent_redrive_resolved_count', 0))}",
            "# HELP qbit_prism_accepted_parent_redrive_exhausted_total Ancestors whose re-drive attempt cap was exhausted with the transition still pending; the publication-progress watchdog remains the backstop.",
            "# TYPE qbit_prism_accepted_parent_redrive_exhausted_total counter",
            "qbit_prism_accepted_parent_redrive_exhausted_total "
            f"{int(getattr(server, 'accepted_parent_redrive_exhausted_count', 0))}",
        ]
    )
    prior_stats_fn = getattr(server.ledger, "prior_balances_read_stats", None)
    if callable(prior_stats_fn):
        prior_stats = prior_stats_fn()
        lines.extend(
            [
                "# HELP qbit_prism_prior_balances_reads_total Prior-balances reads served by the ledger.",
                "# TYPE qbit_prism_prior_balances_reads_total counter",
                f"qbit_prism_prior_balances_reads_total {int(prior_stats['reads_total'])}",
                "# HELP qbit_prism_prior_balances_read_last_seconds Duration of the most recent prior-balances read.",
                "# TYPE qbit_prism_prior_balances_read_last_seconds gauge",
                f"qbit_prism_prior_balances_read_last_seconds {float(prior_stats['last_seconds']):.6f}",
                "# HELP qbit_prism_prior_balances_read_max_seconds Longest prior-balances read since process start.",
                "# TYPE qbit_prism_prior_balances_read_max_seconds gauge",
                f"qbit_prism_prior_balances_read_max_seconds {float(prior_stats['max_seconds']):.6f}",
            ]
        )
    read_gate_fn = getattr(server.ledger, "ledger_read_gate_stats", None)
    read_gate_stats = (
        fold_ledger_read_stats(read_gate_fn() or {})
        if callable(read_gate_fn)
        else {}
    )
    if read_gate_stats:
        families = (
            (
                "qbit_prism_ledger_read_calls_total",
                "counter",
                "Ledger read-slot operations completed or failed, by operation.",
                "calls_total",
                True,
            ),
            (
                "qbit_prism_ledger_read_gate_wait_seconds_total",
                "counter",
                "Cumulative coordinator-local read-slot admission wait, by operation.",
                "gate_wait_seconds_total",
                False,
            ),
            (
                "qbit_prism_ledger_read_gate_wait_seconds_max",
                "gauge",
                "Longest coordinator-local read-slot admission wait since process start, by operation.",
                "gate_wait_seconds_max",
                False,
            ),
            (
                "qbit_prism_ledger_read_gate_timeouts_total",
                "counter",
                "Read-slot operations whose deadline expired before admission, so no statement was ever sent.",
                "gate_timeouts_total",
                True,
            ),
            (
                "qbit_prism_ledger_read_execute_seconds_total",
                "counter",
                "Cumulative PostgreSQL execution time for admitted read-slot statements, by operation.",
                "execute_seconds_total",
                False,
            ),
            (
                "qbit_prism_ledger_read_execute_seconds_max",
                "gauge",
                "Longest PostgreSQL execution time for an admitted read-slot statement since process start, by operation.",
                "execute_seconds_max",
                False,
            ),
            (
                "qbit_prism_ledger_read_execute_timeouts_total",
                "counter",
                "Admitted read-slot statements whose deadline expired inside PostgreSQL, cancel lag included.",
                "execute_timeouts_total",
                True,
            ),
        )
        operations = [
            (server.prometheus_label_value(str(operation)), stats)
            for operation, stats in sorted(read_gate_stats.items())
        ]
        for name, metric_type, help_text, field, integral in families:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {metric_type}")
            for label, stats in operations:
                value = stats.get(field, 0)
                rendered = f"{int(value)}" if integral else f"{float(value):.6f}"
                lines.append(f'{name}{{operation="{label}"}} {rendered}')
    startup_phases = server.startup_phase_seconds()
    if startup_phases:
        lines.extend(
            [
                "# HELP qbit_prism_startup_phase_seconds Seconds from serve() start to each startup phase, recorded once.",
                "# TYPE qbit_prism_startup_phase_seconds gauge",
            ]
        )
        for phase, seconds in sorted(startup_phases.items()):
            label = server.prometheus_label_value(phase)
            lines.append(
                f'qbit_prism_startup_phase_seconds{{phase="{label}"}} {float(seconds):.6f}'
            )
    return lines


def reference_accepted_stats_reconcile_metric_lines(server) -> list[str]:
    status_fn = getattr(server.ledger, "accepted_stats_reconcile_status", None)
    if not callable(status_fn):
        return []
    status = status_fn()
    lines = [
        "# HELP qbit_prism_accepted_stats_reconcile_failures_total Failed background accepted-stats reconcile passes.",
        "# TYPE qbit_prism_accepted_stats_reconcile_failures_total counter",
        f"qbit_prism_accepted_stats_reconcile_failures_total {int(status.get('failures') or 0)}",
    ]
    age = status.get("age_seconds")
    if age is not None:
        lines.extend(
            [
                "# HELP qbit_prism_accepted_stats_reconcile_age_seconds Seconds since the accepted-share counters were last reconciled against the ledger.",
                "# TYPE qbit_prism_accepted_stats_reconcile_age_seconds gauge",
                f"qbit_prism_accepted_stats_reconcile_age_seconds {float(age):.6f}",
            ]
        )
    return lines


def reference_lease_heartbeat_metrics_lines(server) -> list[str]:
    snapshot = server._ensure_lease_heartbeat_service().snapshot()
    attempts = snapshot["attempts"]
    outcomes = snapshot["outcomes"]
    last_phases = snapshot["last_phase_seconds"]
    worst_phases = snapshot["worst_phase_seconds"]
    policy_seconds = snapshot["policy_seconds"]
    wakes = snapshot["monitor_wakes"]
    probe = snapshot["stall_probe"]
    assert isinstance(attempts, dict)
    assert isinstance(outcomes, dict)
    assert isinstance(last_phases, dict)
    assert isinstance(worst_phases, dict)
    assert isinstance(policy_seconds, dict)
    assert isinstance(wakes, dict)
    assert isinstance(probe, dict)
    wake_buckets = wakes["buckets"]
    late_wakes = wakes["late_wakes"]
    assert isinstance(wake_buckets, dict)
    assert isinstance(late_wakes, dict)
    return [
        "# HELP qbit_prism_lease_heartbeat_running Whether the writer-lease heartbeat thread is alive.",
        "# TYPE qbit_prism_lease_heartbeat_running gauge",
        f"qbit_prism_lease_heartbeat_running {int(bool(snapshot['running']))}",
        "# HELP qbit_prism_lease_heartbeat_attempts_total Guard verifications by mode.",
        "# TYPE qbit_prism_lease_heartbeat_attempts_total counter",
        *[
            f'qbit_prism_lease_heartbeat_attempts_total{{mode="{mode}"}} {int(attempts.get(mode, 0))}'
            for mode in LEASE_HEARTBEAT_MODES
        ],
        "# HELP qbit_prism_lease_heartbeat_outcomes_total Guard verification outcomes.",
        "# TYPE qbit_prism_lease_heartbeat_outcomes_total counter",
        *[
            f'qbit_prism_lease_heartbeat_outcomes_total{{outcome="{outcome}"}} {int(outcomes.get(outcome, 0))}'
            for outcome in LEASE_HEARTBEAT_OUTCOMES
        ],
        "# HELP qbit_prism_lease_heartbeat_phase_seconds Phase breakdown of the latest guard verification.",
        "# TYPE qbit_prism_lease_heartbeat_phase_seconds gauge",
        *[
            f'qbit_prism_lease_heartbeat_phase_seconds{{phase="{phase}"}} {float(last_phases.get(phase, 0.0)):.6f}'
            for phase in LEASE_HEARTBEAT_PHASES
        ],
        "# HELP qbit_prism_lease_heartbeat_worst_phase_seconds Phase breakdown of the slowest guard verification observed.",
        "# TYPE qbit_prism_lease_heartbeat_worst_phase_seconds gauge",
        *[
            f'qbit_prism_lease_heartbeat_worst_phase_seconds{{phase="{phase}"}} {float(worst_phases.get(phase, 0.0)):.6f}'
            for phase in LEASE_HEARTBEAT_PHASES
        ],
        "# HELP qbit_prism_lease_heartbeat_activity_age_seconds Age of the newest monitor-visible heartbeat activity mark.",
        "# TYPE qbit_prism_lease_heartbeat_activity_age_seconds gauge",
        f"qbit_prism_lease_heartbeat_activity_age_seconds {float(snapshot['activity_age_seconds']):.6f}",
        "# HELP qbit_prism_lease_heartbeat_server_proven_age_seconds Age of the newest completed guard round trip.",
        "# TYPE qbit_prism_lease_heartbeat_server_proven_age_seconds gauge",
        f"qbit_prism_lease_heartbeat_server_proven_age_seconds {float(snapshot['server_proven_age_seconds']):.6f}",
        "# HELP qbit_prism_lease_heartbeat_monitor_wake_delay_seconds Worst observed lateness of the heartbeat monitor's own poll.",
        "# TYPE qbit_prism_lease_heartbeat_monitor_wake_delay_seconds gauge",
        f"qbit_prism_lease_heartbeat_monitor_wake_delay_seconds {float(snapshot['monitor_wake_delay_seconds']):.6f}",
        "# HELP qbit_prism_lease_heartbeat_policy_seconds Terms of the validated writer-lease heartbeat timing policy.",
        "# TYPE qbit_prism_lease_heartbeat_policy_seconds gauge",
        *[
            f'qbit_prism_lease_heartbeat_policy_seconds{{term="{term}"}} {float(policy_seconds.get(term, 0.0)):.6f}'
            for term in LEASE_HEARTBEAT_POLICY_TERMS
        ],
        "# HELP qbit_prism_lease_heartbeat_monitor_wake_lateness_seconds Lateness of every heartbeat-monitor poll wake (issue #227); express the wake-delay alert on this or on the late-wake counters, not on the lifetime gauge.",
        "# TYPE qbit_prism_lease_heartbeat_monitor_wake_lateness_seconds histogram",
        *[
            f'qbit_prism_lease_heartbeat_monitor_wake_lateness_seconds_bucket{{le="{bucket:g}"}} {int(wake_buckets.get(bucket, 0))}'
            for bucket in LEASE_MONITOR_WAKE_DELAY_BUCKETS
        ],
        f'qbit_prism_lease_heartbeat_monitor_wake_lateness_seconds_bucket{{le="+Inf"}} {int(wakes["count"])}',
        f'qbit_prism_lease_heartbeat_monitor_wake_lateness_seconds_sum {float(wakes["sum"]):.6f}',
        f'qbit_prism_lease_heartbeat_monitor_wake_lateness_seconds_count {int(wakes["count"])}',
        "# HELP qbit_prism_lease_heartbeat_monitor_late_wakes_total Monitor wakes at least the labelled fraction of the policy's scheduler slack late.",
        "# TYPE qbit_prism_lease_heartbeat_monitor_late_wakes_total counter",
        *[
            f'qbit_prism_lease_heartbeat_monitor_late_wakes_total{{slack_fraction="{fraction}"}} {int(late_wakes.get(fraction, 0))}'
            for fraction in LEASE_MONITOR_LATE_WAKE_SLACK_FRACTIONS
        ],
        "# HELP qbit_prism_lease_heartbeat_monitor_wake_delay_window_max_seconds Worst monitor wake lateness inside the trailing 300s window; falls back once a stall ages out, unlike the lifetime gauge.",
        "# TYPE qbit_prism_lease_heartbeat_monitor_wake_delay_window_max_seconds gauge",
        f"qbit_prism_lease_heartbeat_monitor_wake_delay_window_max_seconds {float(wakes['window_max_seconds']):.6f}",
        "# HELP qbit_prism_lease_heartbeat_monitor_wake_delay_record_age_seconds Seconds since the lifetime monitor wake-delay record was last raised.",
        "# TYPE qbit_prism_lease_heartbeat_monitor_wake_delay_record_age_seconds gauge",
        f"qbit_prism_lease_heartbeat_monitor_wake_delay_record_age_seconds {float(wakes['record_age_seconds']):.6f}",
        "# HELP qbit_prism_lease_heartbeat_monitor_exit_guarantee_breaches_total Monitor wakes later than the policy's max_guaranteed_monitor_lateness: beats on which exit-before-adoption was not guaranteed (issue #227).",
        "# TYPE qbit_prism_lease_heartbeat_monitor_exit_guarantee_breaches_total counter",
        f"qbit_prism_lease_heartbeat_monitor_exit_guarantee_breaches_total {int(snapshot['exit_guarantee_breaches'])}",
        "# HELP qbit_prism_lease_heartbeat_monitor_worst_exit_guarantee_overrun_seconds Worst amount by which a late monitor wake could have placed the hard exit after the successor's adoption edge.",
        "# TYPE qbit_prism_lease_heartbeat_monitor_worst_exit_guarantee_overrun_seconds gauge",
        f"qbit_prism_lease_heartbeat_monitor_worst_exit_guarantee_overrun_seconds {float(snapshot['worst_exit_guarantee_overrun_seconds']):.6f}",
        "# HELP qbit_prism_lease_heartbeat_stall_probe_samples_total Stack samples the stall probe took on monitor wakes at least half a scheduler slack late.",
        "# TYPE qbit_prism_lease_heartbeat_stall_probe_samples_total counter",
        f"qbit_prism_lease_heartbeat_stall_probe_samples_total {int(probe['samples'])}",
        "# HELP qbit_prism_lease_heartbeat_stall_probe_suppressed_total Stall-probe triggers refused by its rate limit (at most 3 samples per 60s).",
        "# TYPE qbit_prism_lease_heartbeat_stall_probe_suppressed_total counter",
        f"qbit_prism_lease_heartbeat_stall_probe_suppressed_total {int(probe['suppressed'])}",
        "# HELP qbit_prism_lease_heartbeat_monitor_breach_warnings_suppressed_total Lateness-beyond-slack warnings the monitor refused under its rate limit (at most 3 per 60s); the breaches themselves are still counted.",
        "# TYPE qbit_prism_lease_heartbeat_monitor_breach_warnings_suppressed_total counter",
        f"qbit_prism_lease_heartbeat_monitor_breach_warnings_suppressed_total {int(snapshot['slack_breach_warnings_suppressed'])}",
    ]


def reference_gc_pause_metrics_lines(server) -> list[str]:
    pauses = server._ensure_lease_heartbeat_service().snapshot()["gc_pauses"]
    assert isinstance(pauses, dict)
    lines = [
        "# HELP qbit_prism_process_gc_pause_seconds Cyclic collector pause duration per generation, from gc.callbacks; the qbit_prism_process_gc_* count families say how many passes ran, this says how long each held the interpreter.",
        "# TYPE qbit_prism_process_gc_pause_seconds histogram",
    ]
    for generation in PRISM_GC_GENERATIONS:
        entry = pauses[generation]
        buckets = entry["buckets"]
        lines.extend(
            f'qbit_prism_process_gc_pause_seconds_bucket{{generation="{generation}",le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
            for bucket in PRISM_GC_PAUSE_SECONDS_BUCKETS
        )
        lines.append(
            f'qbit_prism_process_gc_pause_seconds_bucket{{generation="{generation}",le="+Inf"}} {int(entry["count"])}'
        )
        lines.append(
            f'qbit_prism_process_gc_pause_seconds_sum{{generation="{generation}"}} {float(entry["sum"]):.6f}'
        )
        lines.append(
            f'qbit_prism_process_gc_pause_seconds_count{{generation="{generation}"}} {int(entry["count"])}'
        )
    lines.extend(
        [
            "# HELP qbit_prism_process_gc_last_pause_seconds Duration of the most recent cyclic collector pass per generation.",
            "# TYPE qbit_prism_process_gc_last_pause_seconds gauge",
            *[
                f'qbit_prism_process_gc_last_pause_seconds{{generation="{generation}"}} {float(pauses[generation]["last_seconds"]):.6f}'
                for generation in PRISM_GC_GENERATIONS
            ],
            "# HELP qbit_prism_process_gc_max_pause_seconds Longest cyclic collector pass observed per generation since the lease heartbeat armed.",
            "# TYPE qbit_prism_process_gc_max_pause_seconds gauge",
            *[
                f'qbit_prism_process_gc_max_pause_seconds{{generation="{generation}"}} {float(pauses[generation]["max_seconds"]):.6f}'
                for generation in PRISM_GC_GENERATIONS
            ],
        ]
    )
    return lines


def reference_shutdown_metrics_lines(server) -> list[str]:
    snapshot = server._ensure_shutdown_controller().snapshot()
    quiescence = snapshot["writer_quiescence_outcomes"]
    release = snapshot["lease_release_outcomes"]
    active = snapshot["active_writers"]
    assert isinstance(quiescence, dict)
    assert isinstance(release, dict)
    assert isinstance(active, dict)
    return [
        "# HELP qbit_prism_shutdowns_total Controlled coordinator shutdown sequences started.",
        "# TYPE qbit_prism_shutdowns_total counter",
        f"qbit_prism_shutdowns_total {int(snapshot['shutdowns_total'])}",
        "# HELP qbit_prism_shutdown_writer_operations Active admitted ledger-mutating operations by component.",
        "# TYPE qbit_prism_shutdown_writer_operations gauge",
        *[
            f'qbit_prism_shutdown_writer_operations{{component="{server.prometheus_label_value(str(component))}"}} {int(count)}'
            for component, count in sorted(active.items())
        ],
        "# HELP qbit_prism_shutdown_writer_quiescence_total Writer-quiescence outcomes.",
        "# TYPE qbit_prism_shutdown_writer_quiescence_total counter",
        *[
            f'qbit_prism_shutdown_writer_quiescence_total{{outcome="{outcome}"}} {int(quiescence.get(outcome, 0))}'
            for outcome in ("success", "timeout")
        ],
        "# HELP qbit_prism_shutdown_writer_quiescence_seconds Duration of the latest writer-quiescence barrier.",
        "# TYPE qbit_prism_shutdown_writer_quiescence_seconds gauge",
        f"qbit_prism_shutdown_writer_quiescence_seconds {float(snapshot['writer_quiescence_seconds']):.6f}",
        "# HELP qbit_prism_shutdown_lease_release_attempts_total Writer-lease release attempts.",
        "# TYPE qbit_prism_shutdown_lease_release_attempts_total counter",
        f"qbit_prism_shutdown_lease_release_attempts_total {int(snapshot['lease_release_attempts_total'])}",
        "# HELP qbit_prism_shutdown_lease_release_total Writer-lease release outcomes.",
        "# TYPE qbit_prism_shutdown_lease_release_total counter",
        *[
            f'qbit_prism_shutdown_lease_release_total{{outcome="{outcome}"}} {int(release.get(outcome, 0))}'
            for outcome in ("success", "not_held", "unsupported", "failure")
        ],
        "# HELP qbit_prism_shutdown_lease_release_seconds Duration of the latest writer-lease release attempt.",
        "# TYPE qbit_prism_shutdown_lease_release_seconds gauge",
        f"qbit_prism_shutdown_lease_release_seconds {float(snapshot['lease_release_seconds']):.6f}",
        "# HELP qbit_prism_shutdown_sigterm_to_lease_release_seconds Time from SIGTERM admission close to safe lease release, or -1 if unobserved.",
        "# TYPE qbit_prism_shutdown_sigterm_to_lease_release_seconds gauge",
        "qbit_prism_shutdown_sigterm_to_lease_release_seconds "
        + (
            f"{float(snapshot['sigterm_to_lease_release_seconds']):.6f}"
            if snapshot["sigterm_release_observed"]
            else "-1"
        ),
        "# HELP qbit_prism_shutdown_release_withheld_total Shutdowns that withheld lease release because a writer did not quiesce.",
        "# TYPE qbit_prism_shutdown_release_withheld_total counter",
        f"qbit_prism_shutdown_release_withheld_total {int(snapshot['release_withheld_total'])}",
        "# HELP qbit_prism_shutdown_non_writer_drain_seconds Duration of cleanup after writer lease handling.",
        "# TYPE qbit_prism_shutdown_non_writer_drain_seconds gauge",
        f"qbit_prism_shutdown_non_writer_drain_seconds {float(snapshot['non_writer_drain_seconds']):.6f}",
    ]


def reference_initial_delivery_metrics_lines(server) -> list[str]:
    server._ensure_initial_job_state()
    mining = server.mining_delivery_snapshot()
    with server.lock:
        counts = {
            "sent": server.initial_job_sent_count,
            "cancelled": server.initial_job_cancelled_count,
            "coalesced": server.initial_job_coalesced_count,
            "failed": server.initial_job_failed_count,
            "superseded": server.initial_job_superseded_count,
        }
        latency_sum = server.initial_job_delivery_latency_seconds_sum
        latency_count = server.initial_job_delivery_latency_count
        queue_capacity_reclaimed = (
            server.initial_job_queue_capacity_reclaimed_count
        )
    executor = getattr(server, "_initial_job_executor", None)
    queued, slots = executor.stats() if executor is not None else (0, 0)
    configured_workers = int(
        getattr(
            server,
            "initial_job_max_workers",
            DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
        )
    )
    with server._bundle_preparation_lock:
        build_counts = dict(server.shared_bundle_build_counts)
        preparation_sum = server.shared_bundle_preparation_seconds_sum
        preparation_count = server.shared_bundle_preparation_count
        waiters = server.shared_bundle_preparation_waiters
    return [
        "# HELP qbit_prism_stratum_subscribed_clients Subscribed Stratum clients.",
        "# TYPE qbit_prism_stratum_subscribed_clients gauge",
        f'qbit_prism_stratum_subscribed_clients {mining["subscribed_clients"]}',
        "# HELP qbit_prism_stratum_authorized_clients Subscribed and authorized Stratum clients.",
        "# TYPE qbit_prism_stratum_authorized_clients gauge",
        f'qbit_prism_stratum_authorized_clients {mining["authorized_clients"]}',
        "# HELP qbit_prism_clients_without_current_tip_job Authorized clients without usable current-tip work.",
        "# TYPE qbit_prism_clients_without_current_tip_job gauge",
        f'qbit_prism_clients_without_current_tip_job {mining["clients_without_current_tip_job"]}',
        "# HELP qbit_prism_clients_with_no_active_job Authorized clients with no active job at all.",
        "# TYPE qbit_prism_clients_with_no_active_job gauge",
        f'qbit_prism_clients_with_no_active_job {mining["clients_with_no_active_job"]}',
        "# HELP qbit_prism_clients_with_current_tip_job Authorized clients with usable current-tip work.",
        "# TYPE qbit_prism_clients_with_current_tip_job gauge",
        f'qbit_prism_clients_with_current_tip_job {mining["clients_with_current_tip_job"]}',
        "# HELP qbit_prism_current_tip_job_coverage_ratio Fraction of authorized clients with current-tip work.",
        "# TYPE qbit_prism_current_tip_job_coverage_ratio gauge",
        f'qbit_prism_current_tip_job_coverage_ratio {float(mining["current_tip_job_coverage_ratio"]):.12g}',
        "# HELP qbit_prism_initial_job_deliveries_pending Coalesced initial deliveries queued or running.",
        "# TYPE qbit_prism_initial_job_deliveries_pending gauge",
        f'qbit_prism_initial_job_deliveries_pending {mining["clients_pending_initial_job"]}',
        "# HELP qbit_prism_initial_job_delivery_tasks_inflight Bounded shared delivery slots currently occupied.",
        "# TYPE qbit_prism_initial_job_delivery_tasks_inflight gauge",
        f"qbit_prism_initial_job_delivery_tasks_inflight {slots}",
        "# HELP qbit_prism_initial_job_delivery_queue_depth Initial-job tasks waiting for a dedicated worker.",
        "# TYPE qbit_prism_initial_job_delivery_queue_depth gauge",
        f"qbit_prism_initial_job_delivery_queue_depth {queued}",
        "# HELP qbit_prism_initial_job_delivery_active_workers Dedicated initial-job workers currently running tasks.",
        "# TYPE qbit_prism_initial_job_delivery_active_workers gauge",
        f"qbit_prism_initial_job_delivery_active_workers {slots}",
        "# HELP qbit_prism_initial_job_delivery_configured_workers Configured dedicated initial-job worker count.",
        "# TYPE qbit_prism_initial_job_delivery_configured_workers gauge",
        f"qbit_prism_initial_job_delivery_configured_workers {configured_workers}",
        "# HELP qbit_prism_initial_job_delivery_seconds Authorization-to-current-job latency.",
        "# TYPE qbit_prism_initial_job_delivery_seconds summary",
        f"qbit_prism_initial_job_delivery_seconds_sum {latency_sum:.6f}",
        f"qbit_prism_initial_job_delivery_seconds_count {latency_count}",
        "# HELP qbit_prism_initial_job_requests_total Initial delivery outcomes.",
        "# TYPE qbit_prism_initial_job_requests_total counter",
        *[
            f'qbit_prism_initial_job_requests_total{{result="{result}"}} {count}'
            for result, count in sorted(counts.items())
        ],
        "# HELP qbit_prism_initial_job_queue_capacity_reclaimed_total Queued initial-job slots reclaimed immediately by cancellation.",
        "# TYPE qbit_prism_initial_job_queue_capacity_reclaimed_total counter",
        f"qbit_prism_initial_job_queue_capacity_reclaimed_total {queue_capacity_reclaimed}",
        "# HELP qbit_prism_shared_bundle_preparation_seconds Heavy shared bundle preparation wall time.",
        "# TYPE qbit_prism_shared_bundle_preparation_seconds summary",
        f"qbit_prism_shared_bundle_preparation_seconds_sum {preparation_sum:.6f}",
        f"qbit_prism_shared_bundle_preparation_seconds_count {preparation_count}",
        "# HELP qbit_prism_shared_bundle_preparation_waiters Callers waiting on the keyed shared preparation flight.",
        "# TYPE qbit_prism_shared_bundle_preparation_waiters gauge",
        f"qbit_prism_shared_bundle_preparation_waiters {waiters}",
        "# HELP qbit_prism_shared_bundle_builds_total Shared bundle builds by terminal outcome.",
        "# TYPE qbit_prism_shared_bundle_builds_total counter",
        *[
            f'qbit_prism_shared_bundle_builds_total{{result="{result}"}} {count}'
            for result, count in sorted(build_counts.items())
        ],
    ]


def reference_job_build_metrics_lines(server) -> list[str]:
    server._ensure_job_cache_state()
    with server._job_cache_lock:
        bucket_counts = dict(server.job_build_seconds_bucket_counts)
        build_sum = server.job_build_seconds_sum
        build_count = server.job_build_count
        phase_seconds = dict(server.job_build_phase_seconds)
        hit_counts = dict(server.job_cache_hit_counts)
        miss_counts = dict(server.job_cache_miss_counts)
        health_refresh_failures = (
            server._ensure_observability_service()
            .state()
            .health_snapshot_refresh_failure_count
        )
    with server._job_build_scheduler_lock:
        scheduler_counts = dict(server.job_build_scheduler_counts)
        priority_counts = dict(server.job_build_priority_counts)
        priority_admission_seconds = dict(
            server.job_build_priority_admission_seconds
        )
        initial_prepared_counts = dict(
            server.initial_job_prepared_work_counts
        )
        cancellation_seconds = dict(server.job_build_cancellation_seconds)
        replacement_seconds = dict(server.job_build_replacement_start_seconds)
        worker_counts = dict(server.job_build_worker_counts)
        active_builds = int(server._job_build_active is not None)
        pending_builds = int(server._job_build_pending is not None)
        priority_requests = tuple(
            request
            for request in (
                (
                    server._job_build_active.request
                    if server._job_build_active is not None
                    else None
                ),
                (
                    server._job_build_retiring.request
                    if server._job_build_retiring is not None
                    else None
                ),
                server._job_build_pending,
            )
            if request is not None
            and not request.cancellation.is_set()
            and server._job_build_is_publication_critical(request)
        )
        priority_preparations = tuple(
            server._job_build_priority_preparations.values()
        )
        priority_active = int(
            bool(priority_requests or priority_preparations)
        )
        priority_age_seconds = max(
            (
                time.monotonic() - request.requested_monotonic
                for request in priority_requests
            ),
            default=0.0,
        )
        priority_age_seconds = max(
            priority_age_seconds,
            max(
                (
                    time.monotonic() - started
                    for started in priority_preparations
                ),
                default=0.0,
            ),
        )
    lock = getattr(server, "lock", None)
    if lock is not None:
        with lock:
            connected_clients = len(getattr(server, "clients", ()))
    else:
        connected_clients = len(getattr(server, "clients", ()))
    lines = [
        "# HELP qbit_prism_job_build_seconds Wall time from client job build or prepared submission to completion, including skipped prepared tasks.",
        "# TYPE qbit_prism_job_build_seconds histogram",
    ]
    for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS:
        lines.append(
            f'qbit_prism_job_build_seconds_bucket{{le="{bucket:g}"}} {bucket_counts.get(bucket, 0)}'
        )
    lines.extend(
        [
            f'qbit_prism_job_build_seconds_bucket{{le="+Inf"}} {build_count}',
            f"qbit_prism_job_build_seconds_sum {build_sum:.6f}",
            f"qbit_prism_job_build_seconds_count {build_count}",
            "# HELP qbit_prism_job_build_phase_seconds_total Cumulative job build wall time by phase.",
            "# TYPE qbit_prism_job_build_phase_seconds_total counter",
            *[
                f'qbit_prism_job_build_phase_seconds_total{{phase="{phase}"}} {phase_seconds.get(phase, 0.0):.6f}'
                for phase in PRISM_JOB_BUILD_PHASES
            ],
            "# HELP qbit_prism_job_cache_hits_total Job build cache hits by cache kind.",
            "# TYPE qbit_prism_job_cache_hits_total counter",
            *[
                f'qbit_prism_job_cache_hits_total{{cache="{kind}"}} {int(hit_counts.get(kind, 0))}'
                for kind in PRISM_JOB_CACHE_KINDS
            ],
            "# HELP qbit_prism_job_cache_misses_total Job build cache misses by cache kind.",
            "# TYPE qbit_prism_job_cache_misses_total counter",
            *[
                f'qbit_prism_job_cache_misses_total{{cache="{kind}"}} {int(miss_counts.get(kind, 0))}'
                for kind in PRISM_JOB_CACHE_KINDS
            ],
            "# HELP qbit_prism_health_snapshot_refresh_failures_total Background health snapshot refreshes that raised.",
            "# TYPE qbit_prism_health_snapshot_refresh_failures_total counter",
            f"qbit_prism_health_snapshot_refresh_failures_total {health_refresh_failures}",
            "# HELP qbit_prism_connected_clients Currently connected Stratum clients.",
            "# TYPE qbit_prism_connected_clients gauge",
            f"qbit_prism_connected_clients {connected_clients}",
            "# HELP qbit_prism_job_build_requests_total Immutable job build requests admitted to the latest-wins scheduler.",
            "# TYPE qbit_prism_job_build_requests_total counter",
            f'qbit_prism_job_build_requests_total {int(scheduler_counts.get("requests", 0))}',
            "# HELP qbit_prism_job_build_starts_total Immutable job builds started by the bounded executor.",
            "# TYPE qbit_prism_job_build_starts_total counter",
            f'qbit_prism_job_build_starts_total {int(scheduler_counts.get("starts", 0))}',
            "# HELP qbit_prism_job_build_completions_total Immutable job build executions completed.",
            "# TYPE qbit_prism_job_build_completions_total counter",
            f'qbit_prism_job_build_completions_total {int(scheduler_counts.get("completions", 0))}',
            "# HELP qbit_prism_job_build_supersessions_total Active or pending builds replaced by a newer immutable key.",
            "# TYPE qbit_prism_job_build_supersessions_total counter",
            f'qbit_prism_job_build_supersessions_total {int(scheduler_counts.get("supersessions", 0))}',
            "# HELP qbit_prism_job_build_obsolete_results_total Obsolete build results discarded before cache or delivery.",
            "# TYPE qbit_prism_job_build_obsolete_results_total counter",
            f'qbit_prism_job_build_obsolete_results_total {int(scheduler_counts.get("obsolete_results", 0))}',
            "# HELP qbit_prism_job_build_orphan_evicted_total Finished flights evicted from scheduler slots after their completion never released them.",
            "# TYPE qbit_prism_job_build_orphan_evicted_total counter",
            f'qbit_prism_job_build_orphan_evicted_total {int(scheduler_counts.get("orphan_evicted", 0))}',
            "# HELP qbit_prism_job_build_active Current latest-generation build executions.",
            "# TYPE qbit_prism_job_build_active gauge",
            f"qbit_prism_job_build_active {active_builds}",
            "# HELP qbit_prism_job_build_pending Newest build request waiting for a bounded executor slot.",
            "# TYPE qbit_prism_job_build_pending gauge",
            f"qbit_prism_job_build_pending {pending_builds}",
            "# HELP qbit_prism_job_build_cancellation_seconds Cancellation signal to obsolete execution completion.",
            "# TYPE qbit_prism_job_build_cancellation_seconds summary",
            f'qbit_prism_job_build_cancellation_seconds_sum {float(cancellation_seconds.get("sum", 0.0)):.6f}',
            f'qbit_prism_job_build_cancellation_seconds_count {int(cancellation_seconds.get("count", 0))}',
            "# HELP qbit_prism_job_build_replacement_start_seconds Supersession signal to replacement build start.",
            "# TYPE qbit_prism_job_build_replacement_start_seconds summary",
            f'qbit_prism_job_build_replacement_start_seconds_sum {float(replacement_seconds.get("sum", 0.0)):.6f}',
            f'qbit_prism_job_build_replacement_start_seconds_count {int(replacement_seconds.get("count", 0))}',
            "# HELP qbit_prism_job_build_priority_events_total Publication-critical scheduler admissions and routine-work displacement.",
            "# TYPE qbit_prism_job_build_priority_events_total counter",
            *[
                f'qbit_prism_job_build_priority_events_total{{result="{result}"}} {int(priority_counts.get(result, 0))}'
                for result in (
                    "started",
                    "coalesced",
                    "queued",
                    "routine_deferred",
                    "routine_preempted",
                )
            ],
            "# HELP qbit_prism_job_build_priority_admission_seconds Publication-priority reservation to builder start or exact-flight coalescing.",
            "# TYPE qbit_prism_job_build_priority_admission_seconds summary",
            f'qbit_prism_job_build_priority_admission_seconds_sum {float(priority_admission_seconds.get("sum", 0.0)):.6f}',
            f'qbit_prism_job_build_priority_admission_seconds_count {int(priority_admission_seconds.get("count", 0))}',
            "# HELP qbit_prism_job_build_priority_active Whether publication-critical build work is preparing, running, retiring, or pending.",
            "# TYPE qbit_prism_job_build_priority_active gauge",
            f"qbit_prism_job_build_priority_active {priority_active}",
            "# HELP qbit_prism_job_build_priority_age_seconds Age of the oldest admitted publication-critical build request.",
            "# TYPE qbit_prism_job_build_priority_age_seconds gauge",
            f"qbit_prism_job_build_priority_age_seconds {priority_age_seconds:.6f}",
            "# HELP qbit_prism_initial_job_prepared_work_total Initial jobs that reused, coalesced behind, or deferred to prepared shared work.",
            "# TYPE qbit_prism_initial_job_prepared_work_total counter",
            *[
                f'qbit_prism_initial_job_prepared_work_total{{result="{result}"}} {int(initial_prepared_counts.get(result, 0))}'
                for result in (
                    "cache_hit",
                    "singleflight",
                    "deferred",
                    "subscribed",
                    "admission_deadline",
                )
            ],
            "# HELP qbit_prism_job_build_worker_events_total Pure builder subprocess lifecycle events.",
            "# TYPE qbit_prism_job_build_worker_events_total counter",
            *[
                f'qbit_prism_job_build_worker_events_total{{event="{event}"}} {int(worker_counts.get(event, 0))}'
                for event in ("starts", "terminations", "crashes", "restarts")
            ],
            "# HELP qbit_prism_job_build_configured_workers Configured bounded job-build executor worker count.",
            "# TYPE qbit_prism_job_build_configured_workers gauge",
            (
                "qbit_prism_job_build_configured_workers "
                f"{int(getattr(server, 'job_build_executor_workers', DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS))}"
            ),
        ]
    )
    return lines


def reference_vardiff_convergence_metrics_lines(server) -> list[str]:
    snapshot = server.vardiff_convergence_snapshot()
    lane_accepted = snapshot["lane_accepted_shares"]
    assert isinstance(lane_accepted, dict)
    resume_outcomes = snapshot["resume_outcomes"]
    assert isinstance(resume_outcomes, dict)
    lanes = [
        str(profile.name)
        for profile in getattr(server, "listener_profiles", ()) or ()
    ]
    lanes.extend(sorted(lane for lane in lane_accepted if lane not in lanes))
    elapsed = max(0.001, time.monotonic() - server.started_monotonic)
    return [
        "# HELP qbit_prism_vardiff_sessions_at_max_difficulty Sessions whose current difficulty sits at their effective vardiff ceiling.",
        "# TYPE qbit_prism_vardiff_sessions_at_max_difficulty gauge",
        f"qbit_prism_vardiff_sessions_at_max_difficulty {int(snapshot['sessions_at_max_difficulty'])}",
        "# HELP qbit_prism_vardiff_lane_accepted_shares_total Accepted shares by Stratum lane.",
        "# TYPE qbit_prism_vardiff_lane_accepted_shares_total counter",
        *[
            f'qbit_prism_vardiff_lane_accepted_shares_total{{lane="{server.prometheus_label_value(lane)}"}} {int(lane_accepted.get(lane, 0))}'
            for lane in lanes
        ],
        "# HELP qbit_prism_vardiff_lane_accepted_shares_per_second Accepted shares per second by Stratum lane, averaged since coordinator start; a long-run average cannot show a transient reconnect storm -- use rate(qbit_prism_vardiff_lane_accepted_shares_total[5m]) for that. The numerator counts shares this process newly committed on the live submission path, not the ledger lifetime total published by qbit_prism_accepted_shares_total: an exact replay of an already-durable share and a startup recovery-journal replay are both acked accepted without being lane-counted, so the lanes sum to this coordinator's own commit rate and need not reconcile against an accepted-ack count.",
        "# TYPE qbit_prism_vardiff_lane_accepted_shares_per_second gauge",
        *[
            f'qbit_prism_vardiff_lane_accepted_shares_per_second{{lane="{server.prometheus_label_value(lane)}"}} {int(lane_accepted.get(lane, 0)) / elapsed:.12g}'
            for lane in lanes
        ],
        "# HELP qbit_prism_vardiff_resume_total Reconnect difficulty resume attempts by outcome; clamped means the retained value was pulled into the lane's plausibility bounds, overridden means an adopted value was superseded by an explicit difficulty request in the same authorize, so resumed + clamped - overridden is the number that stuck.",
        "# TYPE qbit_prism_vardiff_resume_total counter",
        *[
            f'qbit_prism_vardiff_resume_total{{outcome="{outcome}"}} {int(resume_outcomes.get(outcome, 0))}'
            for outcome in PRISM_VARDIFF_RESUME_OUTCOMES
        ],
        "# HELP qbit_prism_vardiff_resume_retained_sessions Worker sessions holding a retained difficulty eligible for resume.",
        "# TYPE qbit_prism_vardiff_resume_retained_sessions gauge",
        f"qbit_prism_vardiff_resume_retained_sessions {int(snapshot['retained_sessions'])}",
    ]


def reference_accepted_preview_attribution_metrics_lines(server) -> list[str]:
    snapshot = ensure_accepted_preview_telemetry(server).snapshot()
    landing = snapshot["landing_phases"]
    passes = snapshot["reconcile_passes"]
    steps = snapshot["reconcile_steps"]
    rescans = snapshot["full_rescans"]

    def summary(name: str, labels: str, stats: dict[str, object]) -> list[str]:
        return [
            f'{name}_sum{{{labels}}} {float(stats["sum"]):.6f}',
            f'{name}_count{{{labels}}} {int(stats["count"])}',
            f'{name}_max{{{labels}}} {float(stats["max"]):.6f}',
        ]

    lines = [
        "# HELP qbit_prism_accepted_block_landing_phase_seconds Accepted-block landing wall time between definitive node acceptance and payout preview publication, by bounded sub-phase; prior_balances_check is a prior-balances reread, never a payout-window rescan.",
        "# TYPE qbit_prism_accepted_block_landing_phase_seconds summary",
    ]
    for phase in PRISM_ACCEPTED_LANDING_PHASES:
        lines.extend(
            summary(
                "qbit_prism_accepted_block_landing_phase_seconds",
                f'phase="{phase}"',
                landing[phase],
            )
        )
    lines.extend(
        [
            "# HELP qbit_prism_reorg_reconcile_pass_seconds Reorg reconciliation passes by bounded caller; _count is the pass count, and a pass minus the sum of its steps is the unattributed remainder.",
            "# TYPE qbit_prism_reorg_reconcile_pass_seconds summary",
        ]
    )
    for caller in PRISM_REORG_RECONCILE_CALLERS:
        lines.extend(
            summary(
                "qbit_prism_reorg_reconcile_pass_seconds",
                f'caller="{caller}"',
                passes[caller],
            )
        )
    lines.extend(
        [
            "# HELP qbit_prism_reorg_reconcile_step_seconds Reorg reconciliation wall time by bounded caller and step; admission_wait is writer admission plus the payout-balance mutation lock, and candidate_prepare is where a reconcile_invalidation full rescan executes.",
            "# TYPE qbit_prism_reorg_reconcile_step_seconds summary",
        ]
    )
    for caller in PRISM_REORG_RECONCILE_CALLERS:
        for step in PRISM_REORG_RECONCILE_STEPS:
            lines.extend(
                summary(
                    "qbit_prism_reorg_reconcile_step_seconds",
                    f'caller="{caller}",step="{step}"',
                    steps[(caller, step)],
                )
            )
    lines.extend(
        [
            "# HELP qbit_prism_payout_window_full_rescan_seconds Full payout-window oracle rescans by bounded reason and window pipeline path; a prior-balances reread is a ledger read operation, not an observation here.",
            "# TYPE qbit_prism_payout_window_full_rescan_seconds summary",
        ]
    )
    for reason in PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS:
        for path in PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS:
            lines.extend(
                summary(
                    "qbit_prism_payout_window_full_rescan_seconds",
                    f'reason="{reason}",path="{path}"',
                    rescans[(reason, path)],
                )
            )
    return lines


def reference_render_metrics_payload(server) -> str:
    ledger_metrics = server.ledger.metrics()
    audit_metrics = server.audit_artifact_metrics()
    mining_metrics = server.mining_delivery_snapshot()
    process_rss_bytes, process_open_fds = server.process_resource_metrics()
    accepted_share_count = server.accepted_share_stats()[0]
    server._ensure_share_hot_path_state()
    with server._share_accounting_lock:
        submitted_share_count = int(getattr(server, "submitted_share_count", 0))
        stale_share_count = int(getattr(server, "stale_share_count", 0))
        duplicate_share_count = int(getattr(server, "duplicate_share_count", 0))
        low_difficulty_share_count = int(
            getattr(server, "low_difficulty_share_count", 0)
        )
        collection_block_submission_count = int(
            getattr(server, "collection_block_submission_count", 0)
        )
        rejection_counts = dict(
            getattr(server, "rejection_counts_by_reason", {})
        )
        grace_credited_share_count = int(
            getattr(server, "grace_credited_share_count", 0)
        )
    block_solves_dropped_counts = dict(
        getattr(
            server,
            "block_solves_dropped_counts",
            {"stale_grace": 0},
        )
    )
    stale_percent = 0.0
    if submitted_share_count > 0:
        stale_percent = (stale_share_count / submitted_share_count) * 100.0
    idle_retarget_count = int(getattr(server, "idle_retarget_count", 0))
    with server.lock:
        server._ensure_connection_capacity_state()
        active_connection_count = len(server.clients)
        connection_limit_rejection_counts = dict(
            server.connection_limit_rejection_counts
        )
        accept_resource_exhaustion_count = int(
            getattr(server, "accept_resource_exhaustion_count", 0)
        )
        connection_setup_failure_count = int(
            getattr(server, "connection_setup_failure_count", 0)
        )
        server._ensure_evicted_job_state()
        server.prune_evicted_job_graveyard(force=False)
        same_tip_context_count = len(server.evicted_same_tip_job_ids)
        evicted_job_context_counts = {
            "same_tip": same_tip_context_count,
            "stale_grace": len(server.evicted_job_graveyard) - same_tip_context_count,
        }
        evicted_job_submit_counts = dict(server.evicted_job_submit_counts)
        evicted_job_expiration_counts = dict(server.evicted_job_expiration_counts)
        evicted_job_capacity_eviction_counts = dict(
            server.evicted_job_capacity_eviction_counts
        )
        stale_job_abandon_counts = dict(
            getattr(
                server,
                "stale_job_abandon_counts",
                {
                    abandon_class: 0
                    for abandon_class in PRISM_STALE_JOB_ABANDON_CLASSES
                },
            )
        )
    server._ensure_worker_metrics_state()
    with server.worker_metrics_lock:
        worker_share_counts = {
            label: dict(counts)
            for label, counts in server.worker_share_counts.items()
        }
        worker_rejection_counts = dict(server.worker_rejection_counts)
    coinbase_weight_headroom = 2_000_000
    latest_coinbase_size_bytes = getattr(server, "latest_coinbase_size_bytes", None)
    if latest_coinbase_size_bytes is not None:
        coinbase_weight_headroom = 2_000_000 - int(latest_coinbase_size_bytes)
    ctv_pending = 0
    ctv_broadcastable = 0
    ctv_failed = 0
    pending_ctv_fanouts = getattr(server.ledger, "pending_ctv_fanout_statuses", None)
    if callable(pending_ctv_fanouts):
        try:
            for fanout in pending_ctv_fanouts(limit=1_000):
                ctv_pending += 1
                status = str(fanout.get("settlement_status", ""))
                if status == "broadcastable":
                    ctv_broadcastable += 1
                elif status == "failed":
                    ctv_failed += 1
        except Exception:
            ctv_pending = -1
            ctv_broadcastable = -1
            ctv_failed = -1
    if ctv_failed >= 0:
        ctv_failed = int(ledger_metrics.get("ctv_fanouts_failed", ctv_failed))
    ibd = 0
    peers = 0
    try:
        blockchain_info = server.rpc.call("getblockchaininfo")
        if isinstance(blockchain_info, dict) and blockchain_info.get("initialblockdownload"):
            ibd = 1
    except Exception:
        ibd = -1
    try:
        network_info = server.rpc.call("getnetworkinfo")
        if isinstance(network_info, dict):
            peers = int(network_info.get("connections", 0))
    except Exception:
        peers = -1
    lines = [
        "# HELP qbit_prism_accepted_shares_total Accepted shares recorded by the canonical PRISM ledger.",
        "# TYPE qbit_prism_accepted_shares_total counter",
        f"qbit_prism_accepted_shares_total {accepted_share_count}",
        *reference_accepted_stats_reconcile_metric_lines(server),
        *reference_landing_observability_metrics_lines(server),
        "# HELP qbit_prism_submitted_shares_total Stratum share submissions seen by the PRISM coordinator.",
        "# TYPE qbit_prism_submitted_shares_total counter",
        f"qbit_prism_submitted_shares_total {submitted_share_count}",
        "# HELP qbit_prism_stratum_active_connections Active admitted Stratum connections across all listeners.",
        "# TYPE qbit_prism_stratum_active_connections gauge",
        f"qbit_prism_stratum_active_connections {active_connection_count}",
        "# HELP qbit_prism_stratum_connection_limit Configured global Stratum connection ceiling; zero means unlimited.",
        "# TYPE qbit_prism_stratum_connection_limit gauge",
        f"qbit_prism_stratum_connection_limit {mining_metrics['connection_capacity']}",
        "# HELP qbit_prism_stratum_peak_active_connections Peak admitted Stratum connections since process start.",
        "# TYPE qbit_prism_stratum_peak_active_connections gauge",
        f"qbit_prism_stratum_peak_active_connections {mining_metrics['peak_active_connections']}",
        "# HELP qbit_prism_stratum_subscribed_connections Active subscribed Stratum connections.",
        "# TYPE qbit_prism_stratum_subscribed_connections gauge",
        f"qbit_prism_stratum_subscribed_connections {mining_metrics['subscribed_connections']}",
        "# HELP qbit_prism_stratum_authorized_connections Active subscribed and authorized Stratum connections.",
        "# TYPE qbit_prism_stratum_authorized_connections gauge",
        f"qbit_prism_stratum_authorized_connections {mining_metrics['authorized_connections']}",
        "# HELP qbit_prism_stratum_pending_initial_jobs Authorized clients awaiting their first usable current-tip job.",
        "# TYPE qbit_prism_stratum_pending_initial_jobs gauge",
        f"qbit_prism_stratum_pending_initial_jobs {mining_metrics['pending_initial_jobs']}",
        "# HELP qbit_prism_stratum_pending_initial_job_limit Configured bound for clients awaiting their first usable job.",
        "# TYPE qbit_prism_stratum_pending_initial_job_limit gauge",
        f"qbit_prism_stratum_pending_initial_job_limit {mining_metrics['pending_initial_job_capacity']}",
        "# HELP qbit_prism_stratum_oldest_pending_initial_job_seconds Age of the oldest pending first-job request.",
        "# TYPE qbit_prism_stratum_oldest_pending_initial_job_seconds gauge",
        f"qbit_prism_stratum_oldest_pending_initial_job_seconds {mining_metrics['oldest_pending_initial_job_age_seconds']}",
        "# HELP qbit_prism_stratum_oldest_genuinely_pending_initial_job_seconds Age of the oldest authorized client that has never received usable work.",
        "# TYPE qbit_prism_stratum_oldest_genuinely_pending_initial_job_seconds gauge",
        f"qbit_prism_stratum_oldest_genuinely_pending_initial_job_seconds {mining_metrics['oldest_genuinely_pending_initial_job_age_seconds']}",
        "# HELP qbit_prism_stratum_current_tip_coverage_gap_seconds Continuous age of current-tip job coverage below 95 percent.",
        "# TYPE qbit_prism_stratum_current_tip_coverage_gap_seconds gauge",
        f"qbit_prism_stratum_current_tip_coverage_gap_seconds {mining_metrics['current_tip_coverage_gap_age_seconds']}",
        "# HELP qbit_prism_stratum_initial_job_queue_rejections_total Sessions closed because bounded first-job delivery was full.",
        "# TYPE qbit_prism_stratum_initial_job_queue_rejections_total counter",
        f"qbit_prism_stratum_initial_job_queue_rejections_total {mining_metrics['initial_job_queue_rejections']}",
        "# HELP qbit_prism_stratum_initial_job_timeouts_total Sessions disconnected after first-job delivery timed out.",
        "# TYPE qbit_prism_stratum_initial_job_timeouts_total counter",
        f"qbit_prism_stratum_initial_job_timeouts_total {mining_metrics['initial_job_timeout_disconnects']}",
        "# HELP qbit_prism_stratum_initial_job_tasks_total First-job tasks canceled or coalesced before duplicate work.",
        "# TYPE qbit_prism_stratum_initial_job_tasks_total counter",
        f'qbit_prism_stratum_initial_job_tasks_total{{result="cancelled"}} {mining_metrics["initial_job_cancelled_tasks"]}',
        f'qbit_prism_stratum_initial_job_tasks_total{{result="coalesced"}} {mining_metrics["initial_job_coalesced_tasks"]}',
        "# HELP qbit_prism_stratum_initial_job_queue_capacity_reclaimed_total Queued first-job admission slots reclaimed immediately by cancellation.",
        "# TYPE qbit_prism_stratum_initial_job_queue_capacity_reclaimed_total counter",
        f'qbit_prism_stratum_initial_job_queue_capacity_reclaimed_total {mining_metrics["initial_job_queue_capacity_reclaimed"]}',
        "# HELP qbit_prism_stratum_clients_with_current_tip_jobs Authorized clients holding usable current-tip work.",
        "# TYPE qbit_prism_stratum_clients_with_current_tip_jobs gauge",
        f"qbit_prism_stratum_clients_with_current_tip_jobs {mining_metrics['clients_with_current_tip_jobs']}",
        "# HELP qbit_prism_stratum_current_tip_job_coverage Ratio of authorized clients holding current-tip work.",
        "# TYPE qbit_prism_stratum_current_tip_job_coverage gauge",
        f"qbit_prism_stratum_current_tip_job_coverage {mining_metrics['current_tip_job_coverage']}",
        "# HELP qbit_prism_stratum_semantic_current_work_ratio Ratio of authorized clients whose work matches the current template fingerprint, payout generation, and client session.",
        "# TYPE qbit_prism_stratum_semantic_current_work_ratio gauge",
        f"qbit_prism_stratum_semantic_current_work_ratio {mining_metrics['semantic_current_work_ratio']}",
        "# HELP qbit_prism_stratum_handler_threads Active per-connection Stratum handler threads.",
        "# TYPE qbit_prism_stratum_handler_threads gauge",
        f"qbit_prism_stratum_handler_threads {mining_metrics['handler_threads']}",
        "# HELP qbit_prism_job_delivery_queue_depth Current bounded delivery executor queue depth.",
        "# TYPE qbit_prism_job_delivery_queue_depth gauge",
        f"qbit_prism_job_delivery_queue_depth {mining_metrics['delivery_executor_queue_depth']}",
        "# HELP qbit_prism_job_delivery_active_workers Delivery executor workers currently running tasks.",
        "# TYPE qbit_prism_job_delivery_active_workers gauge",
        f"qbit_prism_job_delivery_active_workers {mining_metrics['delivery_executor_active_workers']}",
        "# HELP qbit_prism_process_resident_memory_bytes Current process RSS bytes, or -1 when unavailable.",
        "# TYPE qbit_prism_process_resident_memory_bytes gauge",
        f"qbit_prism_process_resident_memory_bytes {process_rss_bytes}",
        "# HELP qbit_prism_process_open_file_descriptors Current process open descriptor count, or -1 when unavailable.",
        "# TYPE qbit_prism_process_open_file_descriptors gauge",
        f"qbit_prism_process_open_file_descriptors {process_open_fds}",
        "# HELP qbit_prism_stratum_connection_limit_rejections_total Stratum connections rejected by an explicitly configured admission limit.",
        "# TYPE qbit_prism_stratum_connection_limit_rejections_total counter",
        *[
            f'qbit_prism_stratum_connection_limit_rejections_total{{scope="{scope}"}} {int(connection_limit_rejection_counts.get(scope, 0))}'
            for scope in ("global", "username")
        ],
        "# HELP qbit_prism_stratum_accept_resource_exhaustions_total Recoverable Stratum accept or client-setup failures caused by process or system descriptor exhaustion.",
        "# TYPE qbit_prism_stratum_accept_resource_exhaustions_total counter",
        f"qbit_prism_stratum_accept_resource_exhaustions_total {accept_resource_exhaustion_count}",
        "# HELP qbit_prism_stratum_connection_setup_failures_total Admitted Stratum connections cleaned up after socket or handler-thread setup failure.",
        "# TYPE qbit_prism_stratum_connection_setup_failures_total counter",
        f"qbit_prism_stratum_connection_setup_failures_total {connection_setup_failure_count}",
        "# HELP qbit_prism_stale_shares_total Stratum shares rejected or ignored as stale.",
        "# TYPE qbit_prism_stale_shares_total counter",
        f"qbit_prism_stale_shares_total {stale_share_count}",
        "# HELP qbit_prism_duplicate_shares_total Duplicate Stratum shares rejected.",
        "# TYPE qbit_prism_duplicate_shares_total counter",
        f"qbit_prism_duplicate_shares_total {duplicate_share_count}",
        "# HELP qbit_prism_low_difficulty_shares_total Low-difficulty Stratum shares rejected.",
        "# TYPE qbit_prism_low_difficulty_shares_total counter",
        f"qbit_prism_low_difficulty_shares_total {low_difficulty_share_count}",
        "# HELP qbit_prism_collection_block_submissions_total Solver-pays-all block candidates submitted from collection-mode jobs.",
        "# TYPE qbit_prism_collection_block_submissions_total counter",
        f"qbit_prism_collection_block_submissions_total {collection_block_submission_count}",
        "# HELP qbit_prism_grace_credited_shares_total Accepted shares credited by the stale-grace policy.",
        "# TYPE qbit_prism_grace_credited_shares_total counter",
        f"qbit_prism_grace_credited_shares_total {grace_credited_share_count}",
        "# HELP qbit_prism_block_solves_dropped_total Block-passing submissions intentionally excluded from block submission by a bounded policy reason.",
        "# TYPE qbit_prism_block_solves_dropped_total counter",
        *[
            f'qbit_prism_block_solves_dropped_total{{reason="{reason}"}} {int(block_solves_dropped_counts.get(reason, 0))}'
            for reason in ("stale_grace",)
        ],
        "# HELP qbit_prism_rejections_total PRISM share or block rejections by canonical reason ID.",
        "# TYPE qbit_prism_rejections_total counter",
        *[
            f'qbit_prism_rejections_total{{reason_id="{reason}"}} {int(rejection_counts.get(reason, 0))}'
            for reason in PRISM_REJECTION_REASON_IDS
        ],
        "# HELP qbit_prism_worker_submitted_shares_total Stratum share submissions by bounded worker label.",
        "# TYPE qbit_prism_worker_submitted_shares_total counter",
        *[
            f'qbit_prism_worker_submitted_shares_total{{worker="{server.prometheus_label_value(label)}"}} {int(counts.get("submitted", 0))}'
            for label, counts in sorted(worker_share_counts.items())
        ],
        "# HELP qbit_prism_worker_accepted_shares_total Accepted shares by bounded worker label.",
        "# TYPE qbit_prism_worker_accepted_shares_total counter",
        *[
            f'qbit_prism_worker_accepted_shares_total{{worker="{server.prometheus_label_value(label)}"}} {int(counts.get("accepted", 0))}'
            for label, counts in sorted(worker_share_counts.items())
        ],
        "# HELP qbit_prism_worker_grace_credited_shares_total Stale-grace credited shares by bounded worker label.",
        "# TYPE qbit_prism_worker_grace_credited_shares_total counter",
        *[
            f'qbit_prism_worker_grace_credited_shares_total{{worker="{server.prometheus_label_value(label)}"}} {int(counts.get("grace", 0))}'
            for label, counts in sorted(worker_share_counts.items())
        ],
        "# HELP qbit_prism_worker_rejections_total PRISM share or block rejections by bounded worker label and reason ID.",
        "# TYPE qbit_prism_worker_rejections_total counter",
        *[
            f'qbit_prism_worker_rejections_total{{worker="{server.prometheus_label_value(label)}",reason_id="{reason}"}} {int(count)}'
            for (label, reason), count in sorted(worker_rejection_counts.items())
        ],
        "# HELP qbit_prism_job_build_failures_total Job builds skipped after a template/coinbase error without dropping the client.",
        "# TYPE qbit_prism_job_build_failures_total counter",
        f"qbit_prism_job_build_failures_total {server.job_build_failure_count}",
        "# HELP qbit_prism_block_candidates_dropped_total Legacy counter; durable candidate outbox rows are never dropped on queue overflow.",
        "# TYPE qbit_prism_block_candidates_dropped_total counter",
        f"qbit_prism_block_candidates_dropped_total {int(getattr(server, 'block_candidates_dropped', 0))}",
        "# HELP qbit_prism_block_candidate_wakeups_coalesced_total Candidate queue wakeups coalesced while the durable outbox retained the work.",
        "# TYPE qbit_prism_block_candidate_wakeups_coalesced_total counter",
        f"qbit_prism_block_candidate_wakeups_coalesced_total {int(getattr(server, 'block_candidate_wakeups_coalesced', 0))}",
        "# HELP qbit_prism_block_candidate_retries_total Transient candidate outcomes retained for durable retry.",
        "# TYPE qbit_prism_block_candidate_retries_total counter",
        f"qbit_prism_block_candidate_retries_total {int(getattr(server, 'block_candidate_retry_count', 0))}",
        "# HELP qbit_prism_block_candidate_accept_pending_defers_total Terminal abandonments refused because the candidate is (or was recently observed as) an active chain block; the candidate retries until its accepted success tail finalizes it as submitted.",
        "# TYPE qbit_prism_block_candidate_accept_pending_defers_total counter",
        f"qbit_prism_block_candidate_accept_pending_defers_total {int(getattr(server, 'block_candidate_accept_pending_defer_count', 0))}",
        "# HELP qbit_prism_block_candidate_poisoned_total Invalid durable candidate intents quarantined from replay.",
        "# TYPE qbit_prism_block_candidate_poisoned_total counter",
        f"qbit_prism_block_candidate_poisoned_total {int(getattr(server, 'block_candidate_poisoned_count', 0))}",
        "# HELP qbit_prism_block_candidates_abandoned_total Block candidates that did not land (lost tip race or failed submit), by reason. Not share rejections: the underlying share was accepted.",
        "# TYPE qbit_prism_block_candidates_abandoned_total counter",
        *[
            f'qbit_prism_block_candidates_abandoned_total{{reason_id="{reason}"}} {int(count)}'
            for reason, count in sorted(getattr(server, "block_candidate_abandoned_counts", {}).items())
        ],
        "# HELP qbit_prism_stale_job_abandons_total Terminal stale-job block candidate abandonments by bounded cause.",
        "# TYPE qbit_prism_stale_job_abandons_total counter",
        *[
            f'qbit_prism_stale_job_abandons_total{{class="{abandon_class}"}} {int(stale_job_abandon_counts.get(abandon_class, 0))}'
            for abandon_class in PRISM_STALE_JOB_ABANDON_CLASSES
        ],
        "# HELP qbit_prism_share_append_queue_depth Accepted shares waiting on the ledger writer thread.",
        "# TYPE qbit_prism_share_append_queue_depth gauge",
        f"qbit_prism_share_append_queue_depth {server.share_append_queue.qsize() if getattr(server, 'share_append_queue', None) is not None else 0}",
        "# HELP qbit_prism_share_append_failures_total Shares in group commits that failed before acknowledgement.",
        "# TYPE qbit_prism_share_append_failures_total counter",
        f"qbit_prism_share_append_failures_total {int(getattr(server, 'share_append_failure_count', 0))}",
        "# HELP qbit_prism_shares_recovered_to_disk_total Legacy pre-commit-ACK shares written to the upgrade recovery file.",
        "# TYPE qbit_prism_shares_recovered_to_disk_total counter",
        f"qbit_prism_shares_recovered_to_disk_total {int(getattr(server, 'shares_recovered_to_disk', 0))}",
        "# HELP qbit_prism_shares_replayed_total Recovery-file shares replayed into the ledger at startup.",
        "# TYPE qbit_prism_shares_replayed_total counter",
        f"qbit_prism_shares_replayed_total {int(getattr(server, 'shares_replayed', 0))}",
        "# HELP qbit_prism_share_replay_conflicts_total Recovery-file rows quarantined because the durable row disagrees with the journal payload.",
        "# TYPE qbit_prism_share_replay_conflicts_total counter",
        f"qbit_prism_share_replay_conflicts_total {int(getattr(server, 'share_replay_conflicts', 0))}",
        "# HELP qbit_prism_tip_refresh_jobs_total Client jobs refreshed after qbit tip/template changes.",
        "# TYPE qbit_prism_tip_refresh_jobs_total counter",
        f"qbit_prism_tip_refresh_jobs_total {server.tip_refresh_job_count}",
        "# HELP qbit_prism_active_job_contexts Current retained PRISM job contexts.",
        "# TYPE qbit_prism_active_job_contexts gauge",
        f"qbit_prism_active_job_contexts {len(getattr(server, 'jobs', {}))}",
        "# HELP qbit_prism_evicted_job_contexts Evicted job contexts retained by safety class.",
        "# TYPE qbit_prism_evicted_job_contexts gauge",
        *[
            f'qbit_prism_evicted_job_contexts{{class="{job_class}"}} {evicted_job_context_counts[job_class]}'
            for job_class in PRISM_EVICTED_JOB_CLASSES
        ],
        "# HELP qbit_prism_evicted_job_submits_total Accepted submits validated against an evicted job context.",
        "# TYPE qbit_prism_evicted_job_submits_total counter",
        *[
            f'qbit_prism_evicted_job_submits_total{{outcome="{outcome}"}} {int(evicted_job_submit_counts.get(outcome, 0))}'
            for outcome in PRISM_EVICTED_JOB_SUBMIT_OUTCOMES
        ],
        "# HELP qbit_prism_evicted_job_expirations_total Retained job contexts removed after their class TTL.",
        "# TYPE qbit_prism_evicted_job_expirations_total counter",
        *[
            f'qbit_prism_evicted_job_expirations_total{{class="{job_class}"}} {int(evicted_job_expiration_counts.get(job_class, 0))}'
            for job_class in PRISM_EVICTED_JOB_CLASSES
        ],
        "# HELP qbit_prism_evicted_job_capacity_evictions_total Same-tip retained contexts removed by a configured count limit.",
        "# TYPE qbit_prism_evicted_job_capacity_evictions_total counter",
        *[
            f'qbit_prism_evicted_job_capacity_evictions_total{{scope="{scope}"}} {int(evicted_job_capacity_eviction_counts.get(scope, 0))}'
            for scope in PRISM_EVICTED_JOB_CAPACITY_SCOPES
        ],
        "# HELP qbit_prism_post_accept_refresh_failures_total Immediate clean-job refreshes that failed after direct block acceptance.",
        "# TYPE qbit_prism_post_accept_refresh_failures_total counter",
        f"qbit_prism_post_accept_refresh_failures_total {server.post_accept_refresh_failure_count}",
        "# HELP qbit_prism_reorg_inactive_blocks_total PRISM pool blocks quarantined after leaving the active chain.",
        "# TYPE qbit_prism_reorg_inactive_blocks_total counter",
        f"qbit_prism_reorg_inactive_blocks_total {server.reorg_inactive_block_count}",
        "# HELP qbit_prism_reorg_reactivated_blocks_total Quarantined PRISM pool blocks restored after returning to the active chain.",
        "# TYPE qbit_prism_reorg_reactivated_blocks_total counter",
        f"qbit_prism_reorg_reactivated_blocks_total {server.reorg_reactivated_block_count}",
        "# HELP qbit_prism_reorg_reconcile_skips_total Reorg reconciliation passes skipped because qbitd chain view was not trusted.",
        "# TYPE qbit_prism_reorg_reconcile_skips_total counter",
        f"qbit_prism_reorg_reconcile_skips_total {server.reorg_reconcile_skip_count}",
        "# HELP qbit_prism_reorg_reconcile_errors_total Reorg reconciliation errors that prevented ordered job issuance.",
        "# TYPE qbit_prism_reorg_reconcile_errors_total counter",
        f"qbit_prism_reorg_reconcile_errors_total {server.reorg_reconcile_error_count}",
        "# HELP qbit_prism_reorg_reconcile_lookups_total Reconcile demand by caller path and the source that satisfied it.",
        "# TYPE qbit_prism_reorg_reconcile_lookups_total counter",
        *[
            f'qbit_prism_reorg_reconcile_lookups_total{{path="{path}",source="{source}"}} '
            f"{int(getattr(server, 'reorg_reconcile_lookup_counts', {}).get((path, source), 0))}"
            for path in PRISM_REORG_RECONCILE_LOOKUP_PATHS
            for source in PRISM_REORG_RECONCILE_LOOKUP_SOURCES
        ],
        "# HELP qbit_prism_matured_payouts_total Payout entries marked mature by the coordinator tip reconciliation path.",
        "# TYPE qbit_prism_matured_payouts_total counter",
        f"qbit_prism_matured_payouts_total {server.matured_payout_count}",
        "# HELP qbit_prism_vardiff_idle_retargets_total Vardiff retargets triggered by the idle zero-accepted-share sweep.",
        "# TYPE qbit_prism_vardiff_idle_retargets_total counter",
        f"qbit_prism_vardiff_idle_retargets_total {idle_retarget_count}",
        "# HELP qbit_prism_stale_share_percent Percent of submitted shares classified stale.",
        "# TYPE qbit_prism_stale_share_percent gauge",
        f"qbit_prism_stale_share_percent {stale_percent:.12g}",
        "# HELP qbit_prism_blocks_accepted_total Blocks accepted through the PRISM coordinator.",
        "# TYPE qbit_prism_blocks_accepted_total counter",
        f"qbit_prism_blocks_accepted_total {server.accepted_block_count}",
        "# HELP qbit_prism_persisted_blocks Persisted PRISM pool block rows.",
        "# TYPE qbit_prism_persisted_blocks gauge",
        f"qbit_prism_persisted_blocks {ledger_metrics['blocks']}",
        "# HELP qbit_prism_inactive_pool_blocks PRISM pool block rows currently quarantined as inactive.",
        "# TYPE qbit_prism_inactive_pool_blocks gauge",
        f"qbit_prism_inactive_pool_blocks {ledger_metrics.get('inactive_blocks', 0)}",
        "# HELP qbit_prism_reversed_pool_blocks PRISM pool block rows terminally reversed.",
        "# TYPE qbit_prism_reversed_pool_blocks gauge",
        f"qbit_prism_reversed_pool_blocks {ledger_metrics.get('reversed_blocks', 0)}",
        "# HELP qbit_prism_rejected_pool_blocks PRISM pool block rows rejected before confirmation.",
        "# TYPE qbit_prism_rejected_pool_blocks gauge",
        f"qbit_prism_rejected_pool_blocks {ledger_metrics.get('rejected_blocks', 0)}",
        "# HELP qbit_prism_owed_accounts Current accounts with positive carried owed balances.",
        "# TYPE qbit_prism_owed_accounts gauge",
        f"qbit_prism_owed_accounts {ledger_metrics['owed_accounts']}",
        "# HELP qbit_prism_coinbase_weight_headroom_bytes Remaining qbit block weight bytes after the latest pool coinbase.",
        "# TYPE qbit_prism_coinbase_weight_headroom_bytes gauge",
        f"qbit_prism_coinbase_weight_headroom_bytes {coinbase_weight_headroom}",
        "# HELP qbit_prism_ctv_fanouts_pending Pending non-terminal CTV fanouts known to the ledger, or -1 if unavailable.",
        "# TYPE qbit_prism_ctv_fanouts_pending gauge",
        f"qbit_prism_ctv_fanouts_pending {ctv_pending}",
        "# HELP qbit_prism_ctv_fanouts_broadcastable CTV fanouts that are mature enough to broadcast, or -1 if unavailable.",
        "# TYPE qbit_prism_ctv_fanouts_broadcastable gauge",
        f"qbit_prism_ctv_fanouts_broadcastable {ctv_broadcastable}",
        "# HELP qbit_prism_ctv_fanouts_failed CTV fanouts with failed or rejected broadcast state, or -1 if unavailable.",
        "# TYPE qbit_prism_ctv_fanouts_failed gauge",
        f"qbit_prism_ctv_fanouts_failed {ctv_failed}",
        "# HELP qbit_prism_vardiff_enabled Whether PRISM Stratum vardiff is enabled.",
        "# TYPE qbit_prism_vardiff_enabled gauge",
        f"qbit_prism_vardiff_enabled {1 if server.vardiff_config.enabled else 0}",
        "# HELP qbit_prism_qbitd_initial_block_download qbitd initialblockdownload status, or -1 if unavailable.",
        "# TYPE qbit_prism_qbitd_initial_block_download gauge",
        f"qbit_prism_qbitd_initial_block_download {ibd}",
        "# HELP qbit_prism_qbitd_peers qbitd peer count, or -1 if unavailable.",
        "# TYPE qbit_prism_qbitd_peers gauge",
        f"qbit_prism_qbitd_peers {peers}",
        "# HELP qbit_prism_audit_artifact_bytes Bytes used by PRISM audit artifacts in PRISM_AUDIT_DIR by artifact kind.",
        "# TYPE qbit_prism_audit_artifact_bytes gauge",
        *[
            f'qbit_prism_audit_artifact_bytes{{kind="{kind}"}} {audit_metrics[kind]["bytes"]}'
            for kind in ("body", "share_segment", "live_bundle", "candidate", "other")
        ],
        "# HELP qbit_prism_audit_artifact_files PRISM audit artifact file count in PRISM_AUDIT_DIR by artifact kind.",
        "# TYPE qbit_prism_audit_artifact_files gauge",
        *[
            f'qbit_prism_audit_artifact_files{{kind="{kind}"}} {audit_metrics[kind]["files"]}'
            for kind in ("body", "share_segment", "live_bundle", "candidate", "other")
        ],
        "# HELP qbit_prism_audit_artifact_scan_error Whether the latest PRISM_AUDIT_DIR metric scan failed.",
        "# TYPE qbit_prism_audit_artifact_scan_error gauge",
        f"qbit_prism_audit_artifact_scan_error {audit_metrics['scan_error']}",
    ]
    lines.extend(reference_lease_heartbeat_metrics_lines(server))
    lines.extend(reference_shutdown_metrics_lines(server))
    lines.extend(reference_coordinator_lock_metrics_lines(server))
    lines.extend(reference_block_submitter_metrics_lines(server))
    lines.extend(reference_block_candidate_cleanup_backlog_metrics_lines(server))
    lines.extend(reference_share_ack_metrics_lines(server))
    lines.extend(server.ctv_fanout_broadcaster_metrics_lines())
    lines.extend(server.vardiff_idle_metrics_lines())
    lines.extend(reference_vardiff_convergence_metrics_lines(server))
    lines.extend(server.block_finalization_metrics_lines())
    lines.extend(reference_job_build_metrics_lines(server))
    lines.extend(server.tip_refresh_metrics_lines())
    lines.extend(server.payout_state_metrics_lines())
    lines.extend(reference_initial_delivery_metrics_lines(server))
    lines.extend(server.progress_health_metrics_lines())
    lines.extend(reference_accepted_preview_attribution_metrics_lines(server))
    lines.extend(reference_gc_pause_metrics_lines(server))
    lines.extend(reference_process_heap_metrics_lines(PINNED_HEAP_SAMPLE))
    lines.extend(reference_component_cardinality_metrics_lines(server))
    return "\n".join(lines) + "\n"


class MetricsRenderParityTests(unittest.TestCase):
    """Byte-for-byte parity between the renderer and the frozen reference."""

    def _seeded_coordinator(self):
        server = support.coordinator()
        temporary = tempfile.TemporaryDirectory()
        server.audit_dir = Path(temporary.name) / "audit"
        server.evidence_path = Path(temporary.name) / "state" / "evidence.json"

        def cleanup() -> None:
            store = server.__dict__.get("_audit_artifact_store")
            if store is not None:
                store.close()
            temporary.cleanup()

        self.addCleanup(cleanup)
        # Pin the /proc-derived gauges: RSS moves between the two renders.
        server.process_resource_metrics = lambda: (123_456_789, 42)
        # Pin issue #226's heap collector the same way: allocation and GC
        # counters move between the reference and the actual render. The
        # pinned renderer is the one metrics_payload() resolves.
        server.__dict__["_metrics_renderer"] = MetricsRenderer(
            server,
            process_telemetry=SimpleNamespace(sample=lambda: PINNED_HEAP_SAMPLE),
        )
        # Issue #226's component gauges, seeded through the shipped owners
        # so the parity document carries non-zero values: a bundle-cache
        # entry, a cached two-page payout window, a serialization slot with
        # a spool, a reconcile flight and memo entry, a pending-share floor
        # holder, a terminal outcome, and one daemon-uploaded window.
        bundles = server._ensure_job_bundle_service()
        bundles._job_bundle_cache[("parity-bundle",)] = object()
        bundles._share_window_serialization = _ShareWindowSerialization(
            key=("parity-digest", 3, 0),
            share_count=3,
            share_snapshot_sha256="parity-digest",
        )
        bundles._share_window_serialization._spool_size = 4_096
        parity_pages = (
            _IncrementalShareWindowPage(
                records=(object(), object()),
                total_difficulty=0,
                prism_json_records=({}, {}),
                canonical_json_items=b'{"a":1},{"b":2}',
            ),
            _IncrementalShareWindowPage(
                records=(object(),),
                total_difficulty=0,
                prism_json_records=({},),
                canonical_json_items=b'{"c":3}',
            ),
        )
        payout = server._ensure_payout_state_service()
        payout._incremental_payout_artifact_window = _IncrementalPayoutArtifactWindow(
            window=IncrementalShareWindow(
                anchor_job_issued_at_ms=1,
                window_weight=1,
                page_size=512,
                pages=parity_pages,
                total_difficulty=0,
            ),
            shares_json=IncrementalShareJsonSequence(pages=parity_pages, record_count=3),
            share_snapshot_sha256="parity-digest",
            refreshed_monotonic=0.0,
            full_rescan_monotonic=0.0,
            full_rescan_attempt_monotonic=0.0,
        )
        reconciler = server._ensure_reorg_reconciler_service()
        reconciler._reconcile_flights["parity-tip"] = _ReconcileFlight()
        reconciler._reorg_reconcile_trusted_memo["parity-tip"] = 1.0
        server._ensure_share_writer_service().adopt_pending_share(
            SimpleNamespace(share_id="parity-share")
        )
        candidate_service = server._ensure_block_candidate_service()
        candidate_service._ensure_block_candidate_disposition_state()
        candidate_service._block_candidate_terminal_outcomes["ef" * 32] = True
        daemon_client = _ServeBuilderClient(process=SimpleNamespace())
        daemon_client.note_uploaded_window("parity-digest")
        server._ensure_bundle_compiler()._serve_builder = daemon_client
        # Share accounting and rejection counters.
        server.submitted_share_count = 10
        server.stale_share_count = 2
        server.duplicate_share_count = 1
        server.low_difficulty_share_count = 3
        server.collection_block_submission_count = 4
        server.grace_credited_share_count = 6
        server.idle_retarget_count = 7
        server.rejection_counts_by_reason["stale-job"] = 2
        server.rejection_counts_by_reason["duplicate-share"] = 1
        # Worker-labeled counters.
        server.worker_share_counts = {
            "alice": {"submitted": 5, "accepted": 4, "grace": 1},
            "bob": {"submitted": 2, "accepted": 1},
        }
        server.worker_rejection_counts = {("alice", "stale-job"): 2}
        # Connection admission counters.
        server.connection_limit_rejection_counts = {"global": 2, "username": 3}
        server.accept_resource_exhaustion_count = 4
        server.connection_setup_failure_count = 5
        # Share-ACK histograms via the submission owner.
        server._observe_share_ack_seconds("accepted", 0.05)
        server._observe_share_ack_seconds("rejected", 0.2)
        # Block-candidate producer state (routed to the B1 owner).
        server.block_candidate_retry_count = 2
        server.block_candidate_accept_pending_defer_count = 1
        server.block_candidate_poisoned_count = 1
        server.block_candidate_wakeups_coalesced = 3
        server.block_candidate_abandoned_counts = {"stale-job": 2}
        server.stale_job_abandon_counts = {
            abandon_class: index
            for index, abandon_class in enumerate(PRISM_STALE_JOB_ABANDON_CLASSES)
        }
        server.block_solves_dropped_counts = {"stale_grace": 2}
        server._observe_block_submit_seconds(0.5)
        # Acceptance-to-preview-publication needs both halves of the interval:
        # the submitter-thread acceptance stamp and the publication that
        # closes it. Seeding through the shipped B1 entry points keeps the
        # parity document exercising the real observer, not a hand-built
        # histogram.
        service = server._ensure_block_candidate_service()
        service._note_accepted_block_preview_acceptance("ab" * 32)
        server._observe_accepted_block_preview_publication(
            "ab" * 32,
            result="published",
        )
        # Issue #198's cleanup-retry backlog, seeded through the shipped
        # deferral and backpressure entry points: one owed hash carrying one
        # retained holder and a published fence, and one engagement that
        # preserved two rows, so every new family renders a non-zero value.
        service._record_block_candidate_terminal_outcome("cd" * 32, accepted=False)
        service._defer_collapsed_candidate_cleanup(
            "cd" * 32,
            ("retry-state",),
            shares=(SimpleNamespace(share_id="miner-a:" + "cd" * 32),),
        )
        with redirect_stdout(StringIO()):
            service._note_block_candidate_cleanup_backpressure(
                caller="replay-page",
                rows=2,
                admitted=0,
                depth=1,
                maximum=1,
            )
        server._record_block_ledger_call(
            call_class="landing",
            budget_seconds=30.0,
            duration_seconds=0.25,
            timed_out=False,
        )
        server._record_block_ledger_call(
            call_class="fast",
            budget_seconds=1.0,
            duration_seconds=1.2,
            timed_out=True,
        )
        # Reorg reconciler outcome and lookup counters.
        server.reorg_inactive_block_count = 2
        server.reorg_reactivated_block_count = 1
        server.reorg_reconcile_skip_count = 3
        server.reorg_reconcile_error_count = 1
        server.matured_payout_count = 4
        server._record_reorg_reconcile_lookup("job_build", "memo_hit")
        server._record_reorg_reconcile_lookup("job_build", "serial")
        server._record_reorg_reconcile_lookup("tip_refresh", "overlap")
        # Landing observability inputs.
        server._accepted_parent_preview_wait_timeouts = 2
        server._startup_phase_origin_monotonic = time.monotonic() - 1.0
        server._record_startup_phase_once("audit_listener_bound")
        server._record_startup_phase_once("node_rpc_ready")
        # Ledger-side optional snapshots.
        server.ledger.accepted_stats_reconcile_status = lambda: {
            "failures": 1,
            "age_seconds": 2.5,
        }
        server.ledger.prior_balances_read_stats = lambda: {
            "reads_total": 3,
            "last_seconds": 0.01,
            "max_seconds": 0.02,
        }
        # Issue #211's attribution: local admission and server execution are
        # separate series so a budget exhaustion names the half it went to.
        # Issue #224 Wave 0: a contract operation renders under its own
        # name, an operation outside PRISM_LEDGER_READ_OPERATIONS folds
        # into ``other`` instead of opening a series.
        server.ledger.ledger_read_gate_stats = lambda: {
            "pending_block_candidate_rows": {
                "calls_total": 7,
                "gate_wait_seconds_total": 0.125,
                "gate_wait_seconds_max": 0.1,
                "gate_timeouts_total": 1,
                "execute_seconds_total": 4.5,
                "execute_seconds_max": 2.25,
                "execute_timeouts_total": 2,
            },
            "current_prior_balances": {
                "calls_total": 3,
                "gate_wait_seconds_total": 0.5,
                "gate_wait_seconds_max": 0.4,
                "gate_timeouts_total": 0,
                "execute_seconds_total": 0.75,
                "execute_seconds_max": 0.5,
                "execute_timeouts_total": 0,
            },
            "legacy_probe": {
                "calls_total": 1,
                "gate_wait_seconds_total": 0.01,
                "gate_wait_seconds_max": 0.01,
                "gate_timeouts_total": 0,
                "execute_seconds_total": 0.02,
                "execute_seconds_max": 0.02,
                "execute_timeouts_total": 0,
            },
        }
        # Issue #224 Wave 0's attribution families, seeded through the
        # shared owner every lane will record into, including one
        # out-of-vocabulary rescan reason that must fold into ``other``.
        telemetry = ensure_accepted_preview_telemetry(server)
        telemetry.observe_landing_phase("reconcile", 0.25)
        telemetry.observe_landing_phase("reconcile", 0.75)
        telemetry.observe_landing_phase("preview_publish", 0.05)
        telemetry.observe_reconcile_pass("landing", 0.5)
        telemetry.observe_reconcile_step("landing", "watch_query", 0.125)
        telemetry.observe_reconcile_step("post_confirm", "candidate_prepare", 3.5)
        telemetry.observe_payout_window_full_rescan(
            "reconcile_invalidation", "daemon", 3.5
        )
        telemetry.observe_payout_window_full_rescan(
            "not-a-contract-reason", "in_process", 0.5
        )
        server.ledger.block_candidate_pending_metrics = lambda: {
            "pending_count": 2,
            "oldest_pending_age_seconds": 1.5,
            "oldest_unattempted_age_seconds": 0.5,
        }
        server.ledger.pending_ctv_fanout_statuses = lambda limit: [
            {"settlement_status": "broadcastable"},
            {"settlement_status": "failed"},
            {"settlement_status": "pending"},
        ]
        # Job-build histogram and phase state via the J1 owner.
        server._ensure_job_cache_state()
        server.observe_job_build_elapsed(0.05, {"assemble": 0.02, "payout": 0.01})
        server._record_job_cache_event("template", hit=True)
        server._record_job_cache_event("bundle", hit=False)
        # Vardiff convergence/resume state via the vardiff owner.
        server.listener_profiles = [
            SimpleNamespace(name="default"),
            SimpleNamespace(name="highdiff"),
        ]
        vardiff_service = server._ensure_vardiff_service()
        with vardiff_service._vardiff_convergence_lock:
            vardiff_service.vardiff_lane_accepted_counts["default"] = 9
            vardiff_service.vardiff_lane_accepted_counts["highdiff"] = 2
            vardiff_service.vardiff_resume_outcome_counts["resumed"] = 3
            vardiff_service.vardiff_resume_outcome_counts["clamped"] = 1
            vardiff_service.vardiff_resume_outcome_counts["overridden"] = 2
        vardiff_service.session_difficulty_store.record(
            ("default", "alice"),
            Decimal("32768"),
            now=time.monotonic(),
            share_backed=True,
        )
        return server

    def test_full_render_parity_with_frozen_reference(self) -> None:
        server = self._seeded_coordinator()
        frozen_now = time.monotonic()
        with mock.patch.object(time, "monotonic", return_value=frozen_now):
            expected = reference_render_metrics_payload(server)
            actual = server.metrics_payload()
        self.assertEqual(expected, actual)
        # The parity document must exercise the families the acceptance
        # checklist names; an accidentally empty fixture would prove nothing.
        for needle in (
            "qbit_prism_block_candidate_accept_pending_defers_total 1",
            'qbit_prism_share_ack_seconds_count{result="accepted"} 1',
            "qbit_prism_block_submit_seconds_count 1",
            'qbit_prism_accepted_block_preview_publication_seconds_count{result="published"} 1',
            'qbit_prism_reorg_reconcile_lookups_total{path="job_build",source="memo_hit"} 1',
            'qbit_prism_block_solves_dropped_total{reason="stale_grace"} 2',
            'qbit_prism_stale_job_abandons_total{class="',
            "qbit_prism_stratum_semantic_current_work_ratio",
            "qbit_prism_job_build_configured_workers",
            'qbit_prism_initial_job_prepared_work_total{result="admission_deadline"}',
            "qbit_prism_job_build_orphan_evicted_total",
            "qbit_prism_accepted_stats_reconcile_failures_total 1",
            "qbit_prism_accepted_stats_reconcile_age_seconds 2.500000",
            'qbit_prism_block_ledger_calls_total{call_class="fast"} 1',
            'qbit_prism_block_ledger_call_timeouts_total{call_class="fast"} 1',
            "qbit_prism_prior_balances_reads_total 3",
            'qbit_prism_ledger_read_calls_total{operation="pending_block_candidate_rows"} 7',
            'qbit_prism_ledger_read_gate_wait_seconds_max{operation="pending_block_candidate_rows"} 0.100000',
            'qbit_prism_ledger_read_gate_timeouts_total{operation="pending_block_candidate_rows"} 1',
            'qbit_prism_ledger_read_execute_seconds_max{operation="pending_block_candidate_rows"} 2.250000',
            'qbit_prism_ledger_read_execute_timeouts_total{operation="pending_block_candidate_rows"} 2',
            'qbit_prism_startup_phase_seconds{phase="audit_listener_bound"}',
            "qbit_prism_accepted_parent_preview_wait_timeouts_total 2",
            "qbit_prism_tip_refresh_wave_outcomes_total",
            "qbit_prism_payout_artifact_events_total",
            "qbit_prism_vardiff_sessions_at_max_difficulty",
            'qbit_prism_vardiff_resume_total{outcome="clamped"}',
            'qbit_prism_vardiff_resume_total{outcome="overridden"} 2',
            'qbit_prism_vardiff_lane_accepted_shares_per_second{lane="default"}',
            'qbit_prism_lease_heartbeat_attempts_total{mode="proof"}',
            'qbit_prism_lease_heartbeat_phase_seconds{phase="guard_slot_wait"}',
            'qbit_prism_lease_heartbeat_phase_seconds{phase="guard_client_resume"}',
            'qbit_prism_lease_heartbeat_policy_seconds{term="scheduler_slack"}',
            'qbit_prism_lease_heartbeat_policy_seconds{term="exit_envelope"}',
            'qbit_prism_lease_heartbeat_policy_seconds{term="server_proven_cap"}',
            'qbit_prism_lease_heartbeat_policy_seconds{term="max_guaranteed_monitor_lateness"} 0.550000',
            'qbit_prism_lease_heartbeat_monitor_wake_lateness_seconds_bucket{le="+Inf"}',
            'qbit_prism_lease_heartbeat_monitor_late_wakes_total{slack_fraction="1.0"}',
            "qbit_prism_lease_heartbeat_monitor_wake_delay_window_max_seconds",
            "qbit_prism_lease_heartbeat_monitor_wake_delay_record_age_seconds",
            "qbit_prism_lease_heartbeat_monitor_exit_guarantee_breaches_total",
            "qbit_prism_lease_heartbeat_stall_probe_samples_total",
            "qbit_prism_lease_heartbeat_monitor_breach_warnings_suppressed_total",
            'qbit_prism_process_gc_pause_seconds_bucket{generation="2",le="+Inf"}',
            'qbit_prism_process_gc_max_pause_seconds{generation="0"}',
            "qbit_prism_block_candidate_cleanup_retry_backlog 1",
            f"qbit_prism_block_candidate_cleanup_retry_backlog_max {DEFAULT_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX}",
            "qbit_prism_block_candidate_cleanup_retry_pending_share_holders 1",
            "qbit_prism_block_candidate_cleanup_retry_terminal_outcome_pins 1",
            "qbit_prism_block_candidate_cleanup_backpressure_active 0",
            "qbit_prism_block_candidate_cleanup_backpressure_total 1",
            'qbit_prism_block_candidate_collapse_total{outcome="backlog_deferred"} 2',
            'qbit_prism_ledger_read_calls_total{operation="current_prior_balances"} 3',
            'qbit_prism_ledger_read_calls_total{operation="other"} 1',
            'qbit_prism_accepted_block_landing_phase_seconds_count{phase="reconcile"} 2',
            'qbit_prism_accepted_block_landing_phase_seconds_max{phase="reconcile"} 0.750000',
            'qbit_prism_accepted_block_landing_phase_seconds_sum{phase="lane_wait"} 0.000000',
            'qbit_prism_reorg_reconcile_pass_seconds_count{caller="landing"} 1',
            'qbit_prism_reorg_reconcile_step_seconds_sum{caller="landing",step="watch_query"} 0.125000',
            'qbit_prism_reorg_reconcile_step_seconds_max{caller="post_confirm",step="candidate_prepare"} 3.500000',
            'qbit_prism_payout_window_full_rescan_seconds_count{reason="reconcile_invalidation",path="daemon"} 1',
            'qbit_prism_payout_window_full_rescan_seconds_count{reason="other",path="in_process"} 1',
            'qbit_prism_payout_window_full_rescan_seconds_count{reason="window_daemon_state_lost",path="daemon"} 0',
            "qbit_prism_process_allocated_blocks 123456",
            'qbit_prism_process_gc_trigger_count{generation="2"} 7',
            'qbit_prism_process_gc_uncollectable_objects_total{generation="2"} 2',
            "qbit_prism_process_threads 64",
            "qbit_prism_process_malloc_info_available 1",
            "qbit_prism_process_malloc_arena_bytes 9000",
            "qbit_prism_process_malloc_mmapped_bytes 500",
            'qbit_prism_component_entries{component="payout_window_pages"} 2',
            'qbit_prism_component_entries{component="payout_window_records"} 3',
            'qbit_prism_component_entries{component="share_window_serialization_shares"} 3',
            'qbit_prism_component_entries{component="job_bundle_cache"} 1',
            # The #198 seed above publishes one terminal outcome of its own.
            'qbit_prism_component_entries{component="block_candidate_terminal_outcomes"} 2',
            'qbit_prism_component_entries{component="reconcile_flights"} 1',
            'qbit_prism_component_entries{component="reconcile_trusted_memo"} 1',
            'qbit_prism_component_entries{component="pending_share_commit_floor"} 1',
            'qbit_prism_component_entries{component="daemon_uploaded_windows"} 1',
            'qbit_prism_component_bytes{component="payout_window_canonical_json"} 22',
            'qbit_prism_component_bytes{component="share_window_serialization_spool"} 4096',
        ):
            self.assertIn(needle, actual)
        self.assertNotIn('operation="legacy_probe"', actual)
        self.assertNotIn("not-a-contract-reason", actual)
        # The oldest-age gauge is a real elapsed interval, not the -1
        # sentinel, once a record is owed.
        oldest = [
            line
            for line in actual.splitlines()
            if line.startswith("qbit_prism_block_candidate_cleanup_retry_oldest_seconds ")
        ]
        self.assertEqual(len(oldest), 1)
        self.assertGreaterEqual(float(oldest[0].split()[1]), 0.0)

    def test_renderer_bypasses_cached_metrics_path(self) -> None:
        server = self._seeded_coordinator()
        server.refresh_metrics_snapshot = mock.Mock(  # type: ignore[method-assign]
            side_effect=AssertionError("renderer must not re-enter the cache")
        )
        server.cached_metrics_payload = mock.Mock(  # type: ignore[method-assign]
            side_effect=AssertionError("renderer must not re-enter the cache")
        )
        payload = MetricsRenderer(server).render()
        self.assertIn("qbit_prism_accepted_shares_total", payload)


class RemovedCompatibilitySurfaceTests(unittest.TestCase):
    """PR 80's deleted re-exports and health properties must stay absent."""

    REMOVED_MODULE_EXPORTS = (
        # coordinator_config re-exports
        "DEFAULT_HIGHDIFF_DIFFICULTY",
        "DEFAULT_HIGHDIFF_MAX_DIFFICULTY",
        "DEFAULT_MIN_OUTPUT_FEERATE_SATS_PER_BYTE",
        "DEFAULT_MIN_OUTPUT_SAFETY_MULTIPLIER",
        "DEFAULT_P2MR_SPEND_INPUT_BYTES",
        "DEFAULT_PRISM_COINBASE_TAG",
        "DEFAULT_PRISM_VARDIFF_IDLE_SWEEP_SECONDS",
        "DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS",
        "MAX_PRISM_COINBASE_TAG_BYTES",
        "default_prism_coinbase_tag_hex",
        "env_decimal",
        "env_nonnegative_int_with_legacy",
        "env_optional_bool",
        "env_seed_hex",
        "load_prism_highdiff_listener",
        "load_prism_vardiff_config",
        "production_mode",
        "require_production_env",
        "validate_prism_production_gate",
        "validate_same_tip_job_retention_limits",
        # stratum_session re-exports
        "parse_stratum_password_options",
        "parse_worker_username",
        "split_worker_username",
        # ctv re-exports
        "MAX_CTV_FANOUT_BROADCASTER_CHUNK_SIZE",
        "PRISM_CTV_BROADCASTER_CHUNK_ROWS_BUCKETS",
        "PRISM_CTV_BROADCASTER_CHUNK_SECONDS_BUCKETS",
        "PRISM_CTV_BROADCASTER_SECONDS_BUCKETS",
        # job_bundle re-exports
        "CollectionIdentityUnavailable",
        "JobBuildKey",
        "_JobBuildCancelled",
        # job_delivery re-exports
        "DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS",
        "InitialJobSnapshot",
        "InitialJobTracker",
        "MAX_ACTIVE_PRISM_JOBS_PER_CLIENT",
        "PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS",
        # tip_refresh re-exports
        "PRISM_TIP_REFRESH_CANCELLATION_STAGES",
        "PRISM_TIP_REFRESH_RESULTS",
        "PublishedTipSnapshot",
        # progress_health re-exports
        "BundleBuildToken",
        "RefreshActivityToken",
        # payout_state aliases and re-export
        "_AcceptedBlockPayoutTransition",
        "_PayoutDeliveryAdmission",
        "_PayoutStateDeliveryGate",
        "_PayoutStatePublicationBlocked",
        "PublishedPayoutState",
        # coordinator_shutdown re-export
        "_WriterOperationToken",
        # duplicate job-build registries (canonical in job_bundle)
        "PRISM_JOB_BUILD_PHASES",
        "PRISM_JOB_CACHE_KINDS",
        # reorg machinery moved to lab.prism.reorg_reconciler
        "_ReconcileFlight",
        "PRISM_REORG_RECONCILE_MEMO_MAX_TIPS",
        "PRISM_REORG_RECONCILE_LOOKUP_PATHS",
        "PRISM_REORG_RECONCILE_LOOKUP_SOURCES",
        "PRISM_RECONCILE_PREFETCH_JOIN_TIMEOUT_CEILING_SECONDS",
    )

    KEPT_MODULE_EXPORTS = (
        "BackgroundServiceRegistry",
        "BackgroundServiceSpec",
        "CoordinatorShutdownController",
        "ShutdownInProgress",
        "ledger_writer_operation",
        "DEFAULT_SHARE_COMMIT_BATCH_SIZE",
        "DEFAULT_SHARE_COMMIT_LINGER_MILLISECONDS",
        "DEFAULT_SHARE_COMMIT_TIMEOUT_SECONDS",
        "_BoundedPriorityExecutor",
        "_DeliveryQueueFull",
    )

    REMOVED_HEALTH_PROPERTIES = (
        "_health_snapshot",
        "_health_snapshot_monotonic",
        "_health_refresh_loop_running",
        "_health_snapshot_lock",
        "health_snapshot_refresh_failure_count",
    )

    def test_removed_reexports_are_absent_from_the_coordinator(self) -> None:
        import lab.prism.prism_coordinator as prism_coordinator

        for name in self.REMOVED_MODULE_EXPORTS:
            self.assertFalse(hasattr(prism_coordinator, name), name)
        for name in self.KEPT_MODULE_EXPORTS:
            self.assertTrue(hasattr(prism_coordinator, name), name)
        # The buckets survive only as an import of the canonical owner.
        self.assertIs(
            prism_coordinator.PRISM_JOB_BUILD_SECONDS_BUCKETS,
            PRISM_JOB_BUILD_SECONDS_BUCKETS,
        )

    def test_health_compatibility_properties_are_gone(self) -> None:
        from lab.prism.observability import ObservabilityService
        from lab.prism.prism_coordinator import PrismCoordinator

        for name in self.REMOVED_HEALTH_PROPERTIES:
            self.assertFalse(hasattr(PrismCoordinator, name), name)
        for name in (
            "lock",
            "set_health_snapshot_for_compatibility",
            "set_health_snapshot_monotonic_for_compatibility",
            "set_loop_running_for_compatibility",
            "set_refresh_failure_count_for_compatibility",
        ):
            self.assertFalse(hasattr(ObservabilityService, name), name)
        for name in (
            "replace_lock_for_test",
            "clear_health_snapshot_for_test",
            "set_health_snapshot_monotonic_for_test",
            "set_loop_running_for_test",
        ):
            self.assertTrue(hasattr(ObservabilityService, name), name)


class MetricsRendererTests(unittest.TestCase):
    def test_shutdown_formatter_consumes_one_owner_snapshot(self) -> None:
        snapshot_calls = 0

        def snapshot() -> dict[str, object]:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return {
                "shutdowns_total": 1,
                "writer_quiescence_outcomes": {"success": 1, "timeout": 0},
                "lease_release_outcomes": {
                    "success": 1,
                    "not_held": 0,
                    "unsupported": 0,
                    "failure": 0,
                },
                "active_writers": {"candidate": 2},
                "writer_quiescence_seconds": 0.25,
                "lease_release_attempts_total": 1,
                "lease_release_seconds": 0.125,
                "sigterm_release_observed": True,
                "sigterm_to_lease_release_seconds": 0.5,
                "release_withheld_total": 0,
                "non_writer_drain_seconds": 0.75,
            }

        port = SimpleNamespace(
            _ensure_shutdown_controller=lambda: SimpleNamespace(
                snapshot=snapshot
            ),
            prometheus_label_value=lambda value: value,
        )
        lines = MetricsRenderer(port).shutdown_metrics_lines()  # type: ignore[arg-type]

        self.assertEqual(snapshot_calls, 1)
        self.assertIn("qbit_prism_shutdowns_total 1", lines)
        self.assertIn(
            'qbit_prism_shutdown_writer_operations{component="candidate"} 2',
            lines,
        )
        self.assertIn(
            "qbit_prism_shutdown_sigterm_to_lease_release_seconds 0.500000",
            lines,
        )

    def test_block_submitter_formatter_consumes_one_merged_snapshot(self) -> None:
        snapshot_calls = 0

        def snapshot() -> dict[str, object]:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return {
                "pending_count": 2,
                "oldest_pending_age_seconds": 1.5,
                "oldest_unattempted_age_seconds": 0.5,
                "backoff_active": True,
                "backoff_remaining_seconds": 0.25,
                "backoff_delay_seconds": 1.0,
                "submit_seconds_buckets": {0.05: 1},
                "submit_seconds_sum": 0.04,
                "submit_seconds_count": 1,
                "accepted_preview_publication": {
                    "published": {
                        "buckets": {
                            bucket: (1 if bucket >= 0.5 else 0)
                            for bucket in (
                                PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS
                            )
                        },
                        "sum": 0.4,
                        "count": 1,
                    },
                    "degraded": {
                        "buckets": {
                            bucket: 0
                            for bucket in (
                                PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS
                            )
                        },
                        "sum": 0.0,
                        "count": 0,
                    },
                },
            }

        port = SimpleNamespace(
            block_submitter_snapshot=snapshot,
            block_candidate_collapse_snapshot=lambda: {
                outcome: 0 for outcome in PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES
            },
        )
        lines = MetricsRenderer(port).block_submitter_metrics_lines()  # type: ignore[arg-type]

        self.assertEqual(snapshot_calls, 1)
        self.assertIn("qbit_prism_block_candidates_pending 2", lines)
        self.assertIn("qbit_prism_block_submitter_retry_backoff_active 1", lines)
        self.assertIn("qbit_prism_block_submit_seconds_count 1", lines)
        self.assertIn(
            'qbit_prism_block_submit_seconds_bucket{le="0.05"} 1',
            lines,
        )
        self.assertIn(
            "qbit_prism_accepted_block_preview_publication_seconds_bucket"
            '{result="published",le="0.5"} 1',
            lines,
        )
        # The 5 s child wait budget must be an exact bucket boundary: the
        # acceptance criterion is a p95 below it, which a Prometheus
        # histogram can only answer at a boundary it carries.
        self.assertIn(
            "qbit_prism_accepted_block_preview_publication_seconds_bucket"
            '{result="published",le="5"} 1',
            lines,
        )
        self.assertIn(
            "qbit_prism_accepted_block_preview_publication_seconds_count"
            '{result="published"} 1',
            lines,
        )
        self.assertIn(
            "qbit_prism_accepted_block_preview_publication_seconds_count"
            '{result="degraded"} 0',
            lines,
        )

    def test_cleanup_backlog_formatter_consumes_one_owner_snapshot(self) -> None:
        """Issue #198: one snapshot read, seven unlabelled families."""
        snapshot_calls = 0

        def snapshot() -> dict[str, object]:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return {
                "depth": 3,
                "backlog_max": 4096,
                "oldest_age_seconds": 12.5,
                "pending_share_holders": 2,
                "terminal_outcome_pins": 3,
                "backpressure_active": False,
                "backpressure_engagements": 4,
            }

        port = SimpleNamespace(
            _ensure_block_candidate_service=lambda: SimpleNamespace(
                collapsed_candidate_cleanup_backlog_snapshot=snapshot,
            ),
        )
        lines = MetricsRenderer(port).block_candidate_cleanup_backlog_metrics_lines()  # type: ignore[arg-type]

        self.assertEqual(snapshot_calls, 1)
        self.assertIn("qbit_prism_block_candidate_cleanup_retry_backlog 3", lines)
        self.assertIn("qbit_prism_block_candidate_cleanup_retry_backlog_max 4096", lines)
        self.assertIn(
            "qbit_prism_block_candidate_cleanup_retry_oldest_seconds 12.500000",
            lines,
        )
        self.assertIn(
            "qbit_prism_block_candidate_cleanup_retry_pending_share_holders 2",
            lines,
        )
        self.assertIn(
            "qbit_prism_block_candidate_cleanup_retry_terminal_outcome_pins 3",
            lines,
        )
        self.assertIn("qbit_prism_block_candidate_cleanup_backpressure_active 0", lines)
        self.assertIn("qbit_prism_block_candidate_cleanup_backpressure_total 4", lines)
        # Fixed cardinality: no series in the family carries a label.
        for line in lines:
            if not line.startswith("#"):
                self.assertNotIn("{", line)
        self.assertEqual(
            [
                (line.split()[2], line.split()[3])
                for line in lines
                if line.startswith("# TYPE ")
            ],
            list(BLOCK_CANDIDATE_CLEANUP_BACKLOG_METRIC_FAMILIES),
        )

    def test_cleanup_backlog_families_render_from_the_shipped_owner(self) -> None:
        """An empty backlog renders the -1 age sentinel and the default bound."""
        server = support.coordinator()
        lines = MetricsRenderer(server).block_candidate_cleanup_backlog_metrics_lines()
        self.assertIn("qbit_prism_block_candidate_cleanup_retry_backlog 0", lines)
        self.assertIn(
            "qbit_prism_block_candidate_cleanup_retry_backlog_max "
            f"{DEFAULT_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX}",
            lines,
        )
        self.assertIn(
            "qbit_prism_block_candidate_cleanup_retry_oldest_seconds -1.000000",
            lines,
        )
        self.assertIn("qbit_prism_block_candidate_cleanup_backpressure_active 0", lines)
        # A pinned coordinator bound is the one exported, so an alert can be
        # written as a ratio against the running configuration.
        server.block_candidate_cleanup_retry_backlog_max = 12
        lines = MetricsRenderer(server).block_candidate_cleanup_backlog_metrics_lines()
        self.assertIn("qbit_prism_block_candidate_cleanup_retry_backlog_max 12", lines)
        self.assertEqual(lines, reference_block_candidate_cleanup_backlog_metrics_lines(server))

    def test_collapse_counter_carries_the_backlog_deferred_outcome(self) -> None:
        self.assertIn("backlog_deferred", PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES)
        server = support.coordinator()
        lines = MetricsRenderer(server).block_submitter_metrics_lines()
        self.assertIn(
            'qbit_prism_block_candidate_collapse_total{outcome="backlog_deferred"} 0',
            lines,
        )

    def test_accepted_preview_attribution_formatter_consumes_one_owner_snapshot(
        self,
    ) -> None:
        """Issue #224 Wave 0: one snapshot read, four closed-product families."""
        snapshot_calls = 0
        telemetry = AcceptedPreviewTelemetry()
        telemetry.observe_landing_phase("balance_lock_wait", 1.5)
        telemetry.observe_reconcile_pass("tip_refresh", 0.25)
        telemetry.observe_reconcile_step("tip_refresh", "admission_wait", 0.2)
        telemetry.observe_payout_window_full_rescan(
            "window_daemon_state_lost", "in_process", 2.0
        )
        real_snapshot = telemetry.snapshot

        def snapshot() -> dict[str, object]:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return real_snapshot()

        telemetry.snapshot = snapshot  # type: ignore[method-assign]
        port = SimpleNamespace(_accepted_preview_telemetry=telemetry)
        lines = MetricsRenderer(port).accepted_preview_attribution_metrics_lines()  # type: ignore[arg-type]

        self.assertEqual(snapshot_calls, 1)
        self.assertIn(
            'qbit_prism_accepted_block_landing_phase_seconds_max{phase="balance_lock_wait"} 1.500000',
            lines,
        )
        self.assertIn(
            'qbit_prism_reorg_reconcile_pass_seconds_count{caller="tip_refresh"} 1',
            lines,
        )
        self.assertIn(
            'qbit_prism_reorg_reconcile_step_seconds_sum{caller="tip_refresh",step="admission_wait"} 0.200000',
            lines,
        )
        self.assertIn(
            'qbit_prism_payout_window_full_rescan_seconds_count{reason="window_daemon_state_lost",path="in_process"} 1',
            lines,
        )
        # Every cell of every closed product renders, at zero when unobserved.
        expected_samples = 3 * (
            len(PRISM_ACCEPTED_LANDING_PHASES)
            + len(PRISM_REORG_RECONCILE_CALLERS)
            + len(PRISM_REORG_RECONCILE_CALLERS) * len(PRISM_REORG_RECONCILE_STEPS)
            + len(PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS)
            * len(PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS)
        )
        self.assertEqual(
            len([line for line in lines if not line.startswith("#")]),
            expected_samples,
        )

    def test_ledger_read_operations_outside_the_contract_fold_into_other(
        self,
    ) -> None:
        """Issue #224 Wave 0: the operation label cannot grow past the vocabulary."""
        stats = {
            "calls_total": 2,
            "gate_wait_seconds_total": 0.25,
            "gate_wait_seconds_max": 0.2,
            "gate_timeouts_total": 1,
            "execute_seconds_total": 1.0,
            "execute_seconds_max": 0.75,
            "execute_timeouts_total": 0,
        }
        port = SimpleNamespace(
            ledger=SimpleNamespace(
                ledger_read_gate_stats=lambda: {
                    "zeta_probe": dict(stats),
                    "pending_block_candidate_rows": dict(stats),
                    "alpha_probe": dict(stats, gate_wait_seconds_max=0.9),
                }
            ),
            prometheus_label_value=lambda value: value,
        )
        lines = MetricsRenderer(port)._ledger_read_gate_metric_lines()  # type: ignore[arg-type]

        operations = [
            line.split('operation="', 1)[1].split('"', 1)[0]
            for line in lines
            if line.startswith("qbit_prism_ledger_read_calls_total{")
        ]
        self.assertEqual(operations, ["other", "pending_block_candidate_rows"])
        self.assertIn('qbit_prism_ledger_read_calls_total{operation="other"} 4', lines)
        self.assertIn(
            'qbit_prism_ledger_read_gate_wait_seconds_max{operation="other"} 0.900000',
            lines,
        )
        self.assertIn(
            'qbit_prism_ledger_read_gate_timeouts_total{operation="other"} 2',
            lines,
        )
        self.assertIn(
            'qbit_prism_ledger_read_calls_total{operation="pending_block_candidate_rows"} 2',
            lines,
        )

    def test_share_ack_formatter_renders_owner_snapshot(self) -> None:
        histograms = empty_share_ack_histograms()
        histograms["accepted"]["buckets"][PRISM_JOB_BUILD_SECONDS_BUCKETS[0]] = 2
        histograms["accepted"]["sum"] = 0.01
        histograms["accepted"]["count"] = 2
        port = SimpleNamespace(share_ack_snapshot=lambda: histograms)
        lines = MetricsRenderer(port).share_ack_metrics_lines()  # type: ignore[arg-type]
        self.assertIn(
            f'qbit_prism_share_ack_seconds_bucket{{result="accepted",le="{PRISM_JOB_BUILD_SECONDS_BUCKETS[0]:g}"}} 2',
            lines,
        )
        self.assertIn(
            'qbit_prism_share_ack_seconds_count{result="accepted"} 2',
            lines,
        )
        for result in PRISM_SHARE_ACK_RESULTS:
            self.assertIn(
                f'qbit_prism_share_ack_seconds_count{{result="{result}"}} '
                f'{histograms[result]["count"]}',
                lines,
            )


class AcceptedParentUnresolvedDepthGaugeTests(unittest.TestCase):
    """The depth gauge must read exactly what the admission fence counts."""

    def _coordinator(self):
        server = support.coordinator()
        server._ensure_job_cache_state()
        return server

    def _landing_lines(self, server) -> list[str]:
        return MetricsRenderer(server).landing_observability_metrics_lines()

    def _arm_landed_without_timestamp(self, server, block_hash: str) -> None:
        """Reproduce the landed transitions that carry no monotonic stamp.

        Both the degraded preview-publication fallback and the durable
        candidate replay reconstruct a transition from the dataclass default,
        so ``landed`` is armed while ``landed_monotonic`` stays ``None``.
        """
        with server._accepted_block_payout_preview_condition:
            server._accepted_block_payout_previews[block_hash] = (
                AcceptedBlockPayoutTransition(block_height=11, landed=True)
            )

    def test_depth_gauge_counts_a_landed_transition_with_no_timestamp(self) -> None:
        server = self._coordinator()
        self._arm_landed_without_timestamp(server, "cd" * 32)
        # The age list cannot see this transition, so a gauge derived from it
        # would report zero while the fence already counts depth one.
        self.assertEqual(server.accepted_parent_unresolved_ages_seconds(), [])
        self.assertEqual(server._accepted_parent_unresolved_depth(), 1)
        lines = self._landing_lines(server)
        self.assertIn("qbit_prism_accepted_parent_unresolved_transitions 1", lines)
        # No timestamp means no age to invent: the oldest gauge reports -1
        # rather than fabricating an age for the transition depth just counted.
        self.assertIn(
            "qbit_prism_accepted_parent_unresolved_oldest_seconds -1.000000",
            lines,
        )

    def test_depth_gauge_matches_the_fence_across_mixed_transitions(self) -> None:
        server = self._coordinator()
        timestamped = "ce" * 32
        server._begin_accepted_block_payout_preview(timestamped, block_height=10)
        server._mark_accepted_block_payout_landed(timestamped, block_height=10)
        self._arm_landed_without_timestamp(server, "cf" * 32)
        # A pending (not yet landed) transition stays out of both signals.
        server._begin_accepted_block_payout_preview("d0" * 32, block_height=12)
        self.assertEqual(len(server.accepted_parent_unresolved_ages_seconds()), 1)
        self.assertEqual(server._accepted_parent_unresolved_depth(), 2)
        lines = self._landing_lines(server)
        self.assertIn("qbit_prism_accepted_parent_unresolved_transitions 2", lines)
        self.assertNotIn(
            "qbit_prism_accepted_parent_unresolved_oldest_seconds -1.000000",
            lines,
        )

    def test_depth_gauge_is_zero_without_landed_transitions(self) -> None:
        server = self._coordinator()
        lines = self._landing_lines(server)
        self.assertIn("qbit_prism_accepted_parent_unresolved_transitions 0", lines)
        self.assertIn(
            "qbit_prism_accepted_parent_unresolved_oldest_seconds -1.000000",
            lines,
        )

    def test_exported_cap_is_the_runtime_configured_cap(self) -> None:
        server = self._coordinator()
        # The unconfigured runtime falls back to the shipped default.
        self.assertEqual(
            server._accepted_parent_unresolved_depth_cap(),
            DEFAULT_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX,
        )
        self.assertIn(
            "qbit_prism_accepted_parent_unresolved_depth_max "
            f"{DEFAULT_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX}",
            self._landing_lines(server),
        )
        # A non-default configured cap must reach the exposition too, so an
        # alert can compare the depth against the limit actually enforced.
        non_default = DEFAULT_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX + 3
        server.accepted_parent_unresolved_depth_max = non_default
        self.assertEqual(server._accepted_parent_unresolved_depth_cap(), non_default)
        self.assertIn(
            f"qbit_prism_accepted_parent_unresolved_depth_max {non_default}",
            self._landing_lines(server),
        )


_SERIES_LINE = re.compile(r"^(?P<family>[a-z_]+)(?:\{(?P<labels>[^}]*)\})? (?P<value>\S+)$")


def _parse_series(line: str) -> tuple[str, dict[str, str], str]:
    """Split one exposition sample into (family, labels, value)."""
    match = _SERIES_LINE.match(line)
    if match is None:
        raise AssertionError(f"not a sample line: {line!r}")
    labels: dict[str, str] = {}
    if match.group("labels"):
        for pair in match.group("labels").split(","):
            key, _, quoted = pair.partition("=")
            labels[key] = quoted.strip('"')
    return match.group("family"), labels, match.group("value")


def _type_families(lines: list[str]) -> list[tuple[str, str]]:
    return [
        (line.split()[2], line.split()[3])
        for line in lines
        if line.startswith("# TYPE ")
    ]


def _series_value(lines: list[str], series: str) -> int:
    matches = [line for line in lines if line.startswith(series + " ")]
    if len(matches) != 1:
        raise AssertionError(f"{series!r} rendered {len(matches)} times")
    return int(matches[0].split()[-1])


class HeapAndComponentCardinalityTelemetryTests(unittest.TestCase):
    """Issue #226 part 1: always-on heap telemetry and component gauges.

    The five behaviours the work package pins: the exact family set, the
    mallinfo2-absent path rendering as unavailable rather than zero, every
    component gauge reading its shipped owner, no unbounded label anywhere
    in the component families, and a scrape that never walks the heap.
    """

    TIP = "aa" * 32

    def _seeded_owners(self) -> tuple[object, dict[str, int], dict[str, int]]:
        """Seed every component through its shipped owner; return the expectations.

        Each seed goes through the real owning structure -- the service
        field, the queue, the LRU, the floor -- never a stub on the port,
        so a renamed owner field breaks the read instead of reporting 0.
        """
        server = support.coordinator()
        server._ensure_job_cache_state()
        server.jobs = {"job-1": object(), "job-2": object()}
        bundles = server._ensure_job_bundle_service()
        bundles._job_bundle_cache[("bundle-a",)] = object()
        bundles._job_bundle_cache[("bundle-b",)] = object()
        bundles._job_build_issued_at_ms[7] = 7_000
        bundles._bundle_preparation_flights[("flight",)] = object()
        bundles._active_job_bundle_builds[("build",)] = object()
        serialization = _ShareWindowSerialization(
            key=("digest", 5, 0),
            share_count=5,
            share_snapshot_sha256="digest",
        )
        serialization._spool_size = 4_096
        serialization._compact_shares_json = "[" + "x" * 98 + "]"
        serialization._compact_share_identities_json = "y" * 20
        bundles._share_window_serialization = serialization
        pages = (
            _IncrementalShareWindowPage(
                records=(object(), object()),
                total_difficulty=0,
                prism_json_records=({}, {}),
                canonical_json_items=b'{"a":1},{"b":2}',
            ),
            _IncrementalShareWindowPage(
                records=(object(),),
                total_difficulty=0,
                prism_json_records=({},),
                canonical_json_items=b'{"c":3}',
            ),
        )
        payout = server._ensure_payout_state_service()
        payout._incremental_payout_artifact_window = _IncrementalPayoutArtifactWindow(
            window=IncrementalShareWindow(
                anchor_job_issued_at_ms=1,
                window_weight=1,
                page_size=512,
                pages=pages,
                total_difficulty=0,
            ),
            shares_json=IncrementalShareJsonSequence(pages=pages, record_count=3),
            share_snapshot_sha256="digest",
            refreshed_monotonic=0.0,
            full_rescan_monotonic=0.0,
            full_rescan_attempt_monotonic=0.0,
        )
        payout._payout_ledger_artifact = PayoutLedgerArtifact(
            generation=1,
            payout_state_generation=1,
            network_difficulty=1,
            accepted_share_count=3,
            shares_json=IncrementalShareJsonSequence(pages=pages, record_count=3),
            prior_balances=(),
            prepared_monotonic=0.0,
        )
        payout._accepted_block_payout_previews["b1" * 32] = AcceptedBlockPayoutTransition(
            block_height=11,
            landed=True,
        )
        payout._invalidated_accepted_block_payout_previews["b2" * 32] = 5
        payout._payout_append_invalidation_stamps[1] = 100
        payout._payout_append_invalidation_stamps[2] = 200
        payout._payout_window_inflight_scan_anchors[1] = 100
        payout._payout_unfenced_append_inflight_stamps[1] = 100
        server.evicted_job_graveyard = {
            "job-old": (
                SimpleNamespace(
                    job_id="job-old",
                    template={"previousblockhash": "bb" * 32},
                ),
                1,
                time.monotonic(),
            ),
        }
        candidates = server._ensure_block_candidate_service()
        candidates._ensure_block_replay_state()
        candidates._ensure_block_candidate_disposition_state()
        candidates.candidate_queue.put_nowait(object())
        candidates._block_replay_candidate_queue.put_nowait(object())
        candidates._block_replay_candidate_queue.put_nowait(object())
        candidates._block_replay_inflight_hashes.add("c1" * 32)
        candidates._block_quarantine_queue.put_nowait(object())
        candidates._block_quarantine_hashes.add("c2" * 32)
        candidates._outstanding_block_candidate_hashes.update({"c3" * 32, "c4" * 32})
        candidates._tip_observed_accepted_block_hashes["c5" * 32] = 1.0
        candidates._counted_block_candidate_abandonments.add("c6" * 32)
        candidates._accepted_block_preview_acceptance_monotonic["c7" * 32] = None
        candidates._ancestor_redrive_records["c8" * 32] = object()
        candidates._block_candidate_terminal_outcomes["c9" * 32] = True
        candidates._block_candidate_terminal_outcomes["ca" * 32] = False
        candidates._block_candidate_disposition_flights["cb" * 32] = object()
        candidates._block_disposition_waiting_retries["cc" * 32] = object()
        candidates._block_candidate_dequeued_hashes["cd" * 32] = 1
        server._accounted_accepted_block_hashes.add("ce" * 32)
        reconciler = server._ensure_reorg_reconciler_service()
        reconciler._reconcile_flights[self.TIP] = _ReconcileFlight()
        reconciler._reorg_reconcile_trusted_memo[self.TIP] = 1.0
        reconciler._reorg_reconcile_trusted_memo["dd" * 32] = 2.0
        writer = server._ensure_share_writer_service()
        writer.adopt_pending_share(SimpleNamespace(share_id="miner-a:s1"))
        writer.adopt_pending_share(SimpleNamespace(share_id="miner-a:s2"))
        daemon_client = _ServeBuilderClient(process=SimpleNamespace())
        daemon_client.note_uploaded_window("digest")
        daemon_client.note_uploaded_window("digest-2")
        server._ensure_bundle_compiler()._serve_builder = daemon_client
        expected_entries = {
            "payout_window_pages": 2,
            "payout_window_records": 3,
            "payout_ledger_artifact_shares": 3,
            "accepted_block_payout_previews": 1,
            "invalidated_accepted_block_payout_previews": 1,
            "payout_append_invalidation_stamps": 2,
            "payout_window_inflight_scan_anchors": 1,
            "payout_unfenced_append_inflight_stamps": 1,
            "share_window_serialization_shares": 5,
            "job_contexts": 2,
            "job_bundle_cache": 2,
            "job_build_issued_stamps": 1,
            "bundle_preparation_flights": 1,
            "active_job_bundle_builds": 1,
            "evicted_job_graveyard": 1,
            # With no published tip the delivery owner classes every retained
            # context as same-tip, so its same-tip index holds the entry too.
            "evicted_same_tip_job_ids": 1,
            "block_candidate_queue": 1,
            "block_replay_queue": 2,
            "block_replay_inflight_hashes": 1,
            "block_quarantine_queue": 1,
            "block_quarantine_hashes": 1,
            "outstanding_block_candidate_hashes": 2,
            "tip_observed_accepted_block_hashes": 1,
            "counted_block_candidate_abandonments": 1,
            "accepted_block_preview_stamps": 1,
            "ancestor_redrive_records": 1,
            "block_candidate_terminal_outcomes": 2,
            "block_candidate_disposition_flights": 1,
            "block_disposition_waiting_retries": 1,
            "block_candidate_dequeued_hashes": 1,
            "accounted_accepted_block_hashes": 1,
            "reconcile_flights": 1,
            "reconcile_trusted_memo": 2,
            "pending_share_commit_floor": 2,
            "daemon_window_mirror_records": 0,
            "daemon_uploaded_windows": 2,
        }
        expected_bytes = {
            "payout_window_canonical_json": 22,
            "daemon_window_mirror_canonical_items": 0,
            "share_window_serialization_spool": 4_096,
            "share_window_serialization_compact_json": 120,
        }
        return server, expected_entries, expected_bytes

    def test_process_heap_telemetry_families_are_pinned(self) -> None:
        """The exact (family, type) sequence renders, in order, fixed cardinality."""
        server = support.coordinator()
        renderer = MetricsRenderer(server)
        heap = renderer.process_heap_metrics_lines()
        self.assertEqual(_type_families(heap), list(PROCESS_HEAP_METRIC_FAMILIES))
        rendered_generations: dict[str, list[str]] = {}
        for line in heap:
            if line.startswith("#"):
                continue
            family, labels, _value = _parse_series(line)
            if family in PROCESS_HEAP_GENERATION_FAMILIES:
                # The one permitted label, drawn from the closed set.
                self.assertEqual(set(labels), {"generation"})
                self.assertIn(labels["generation"], PRISM_GC_GENERATIONS)
                rendered_generations.setdefault(family, []).append(labels["generation"])
            else:
                self.assertNotIn("{", line)
        for family in PROCESS_HEAP_GENERATION_FAMILIES:
            self.assertEqual(rendered_generations[family], list(PRISM_GC_GENERATIONS))
        components = renderer.component_cardinality_metrics_lines()
        self.assertEqual(
            _type_families(components),
            list(COMPONENT_CARDINALITY_METRIC_FAMILIES),
        )
        entry_kinds = [
            _parse_series(line)[1]["component"]
            for line in components
            if line.startswith("qbit_prism_component_entries{")
        ]
        byte_kinds = [
            _parse_series(line)[1]["component"]
            for line in components
            if line.startswith("qbit_prism_component_bytes{")
        ]
        self.assertEqual(entry_kinds, list(PRISM_COMPONENT_ENTRY_KINDS))
        self.assertEqual(byte_kinds, list(PRISM_COMPONENT_BYTE_KINDS))
        self.assertEqual(len(set(PRISM_COMPONENT_ENTRY_KINDS)), len(PRISM_COMPONENT_ENTRY_KINDS))
        # Every component the issue names has a gauge.
        for named in (
            "payout_window_pages",
            "share_window_serialization_shares",
            "job_contexts",
            "job_bundle_cache",
            "evicted_job_graveyard",
            "block_candidate_queue",
            "block_replay_queue",
            "reconcile_flights",
            "pending_share_commit_floor",
            "daemon_window_mirror_records",
        ):
            self.assertIn(named, PRISM_COMPONENT_ENTRY_KINDS)
        # The full document carries both blocks, in this order, at its tail.
        type_lines = [
            line for line in server.metrics_payload().splitlines()
            if line.startswith("# TYPE ")
        ]
        expected_tail = [
            f"# TYPE {family} {kind}"
            for family, kind in (
                PROCESS_HEAP_METRIC_FAMILIES + COMPONENT_CARDINALITY_METRIC_FAMILIES
            )
        ]
        self.assertEqual(type_lines[-len(expected_tail):], expected_tail)

    def test_mallinfo_absence_renders_unavailable_not_zero(self) -> None:
        """A failed ctypes lookup renders availability 0 and -1 bytes, never 0."""
        server = support.coordinator()
        resolver = mock.Mock(side_effect=AttributeError("mallinfo2: symbol not found"))
        telemetry = ProcessHeapTelemetry(mallinfo_resolver=resolver)
        renderer = MetricsRenderer(server, process_telemetry=telemetry)
        first = renderer.process_heap_metrics_lines()
        second = renderer.process_heap_metrics_lines()
        self.assertEqual(_series_value(first, "qbit_prism_process_malloc_info_available"), 0)
        for gauge in ("arena", "in_use", "free", "mmapped"):
            self.assertEqual(
                _series_value(first, f"qbit_prism_process_malloc_{gauge}_bytes"),
                -1,
            )
        # The interpreter readings are real even when glibc's are not.
        self.assertGreater(_series_value(first, "qbit_prism_process_allocated_blocks"), 0)
        self.assertGreaterEqual(_series_value(first, "qbit_prism_process_threads"), 1)
        # Resolved once and remembered: no per-scrape lookup, no per-scrape log.
        self.assertEqual(resolver.call_count, 1)
        self.assertEqual(_type_families(second), _type_families(first))
        self.assertEqual(_series_value(second, "qbit_prism_process_malloc_info_available"), 0)
        # The family set is identical to the available case, so a dashboard
        # built on either platform reads the same series.
        self.assertEqual(_type_families(first), list(PROCESS_HEAP_METRIC_FAMILIES))
        # A symbol that binds but fails at call time degrades the same way
        # and is not retried on the next scrape.
        broken = mock.Mock(side_effect=OSError("mallinfo2 call failed"))
        renderer = MetricsRenderer(
            server,
            process_telemetry=ProcessHeapTelemetry(mallinfo_resolver=lambda: broken),
        )
        lines = renderer.process_heap_metrics_lines()
        renderer.process_heap_metrics_lines()
        self.assertEqual(_series_value(lines, "qbit_prism_process_malloc_info_available"), 0)
        self.assertEqual(_series_value(lines, "qbit_prism_process_malloc_arena_bytes"), -1)
        self.assertEqual(broken.call_count, 1)
        # Where the symbol binds, the struct's fields render; the fake makes
        # this branch platform-independent too.
        def fake_mallinfo2() -> MallInfo2:
            info = MallInfo2()
            info.arena = 9_000
            info.uordblks = 6_000
            info.fordblks = 3_000
            info.hblkhd = 500
            return info

        lines = MetricsRenderer(
            server,
            process_telemetry=ProcessHeapTelemetry(mallinfo_resolver=lambda: fake_mallinfo2),
        ).process_heap_metrics_lines()
        self.assertEqual(_series_value(lines, "qbit_prism_process_malloc_info_available"), 1)
        self.assertEqual(_series_value(lines, "qbit_prism_process_malloc_arena_bytes"), 9_000)
        self.assertEqual(_series_value(lines, "qbit_prism_process_malloc_in_use_bytes"), 6_000)
        self.assertEqual(_series_value(lines, "qbit_prism_process_malloc_free_bytes"), 3_000)
        self.assertEqual(_series_value(lines, "qbit_prism_process_malloc_mmapped_bytes"), 500)
        # The operator switch renders unavailable without probing at all.
        resolver = mock.Mock()
        lines = MetricsRenderer(
            server,
            process_telemetry=ProcessHeapTelemetry(
                malloc_enabled=False,
                mallinfo_resolver=resolver,
            ),
        ).process_heap_metrics_lines()
        self.assertEqual(_series_value(lines, "qbit_prism_process_malloc_info_available"), 0)
        self.assertEqual(_series_value(lines, "qbit_prism_process_malloc_in_use_bytes"), -1)
        resolver.assert_not_called()

    def test_component_cardinality_gauges_render_from_the_shipped_owner(self) -> None:
        """Each gauge reads the real owning structure, not a stub."""
        server, expected_entries, expected_bytes = self._seeded_owners()
        self.assertEqual(set(expected_entries), set(PRISM_COMPONENT_ENTRY_KINDS))
        self.assertEqual(set(expected_bytes), set(PRISM_COMPONENT_BYTE_KINDS))
        lines = MetricsRenderer(server).component_cardinality_metrics_lines()
        for kind, value in expected_entries.items():
            self.assertIn(
                f'qbit_prism_component_entries{{component="{kind}"}} {value}',
                lines,
            )
        for kind, value in expected_bytes.items():
            self.assertIn(
                f'qbit_prism_component_bytes{{component="{kind}"}} {value}',
                lines,
            )
        self.assertEqual(lines, reference_component_cardinality_metrics_lines(server))
        # A mirror-backed window reports the daemon's bytes and records and
        # no in-process pages: the same slot, the other shipped window type.
        items = b'{"a":1},{"b":2}'
        digest = hashlib.sha256(b"[" + items + b"]").hexdigest()
        mirror = DaemonShareWindowMirror.from_full_items(
            anchor_job_issued_at_ms=1,
            window_weight=1,
            page_size=512,
            record_count=2,
            canonical_items=items,
            share_snapshot_sha256=digest,
        )
        payout = server._ensure_payout_state_service()
        payout._incremental_payout_artifact_window = _IncrementalPayoutArtifactWindow(
            window=mirror,
            shares_json=mirror.json_records(),
            share_snapshot_sha256=digest,
            refreshed_monotonic=0.0,
            full_rescan_monotonic=0.0,
            full_rescan_attempt_monotonic=0.0,
        )
        lines = MetricsRenderer(server).component_cardinality_metrics_lines()
        # Accounted exactly once. The mirror *is* the cached window here, so
        # its size appears only under daemon_window_mirror_*; reporting it
        # under payout_window_* as well would make an operator summing
        # qbit_prism_component_bytes for RSS attribution count it twice.
        self.assertIn('qbit_prism_component_entries{component="payout_window_pages"} 0', lines)
        self.assertIn('qbit_prism_component_entries{component="payout_window_records"} 0', lines)
        self.assertIn(
            'qbit_prism_component_entries{component="daemon_window_mirror_records"} 2',
            lines,
        )
        self.assertIn(
            'qbit_prism_component_bytes{component="payout_window_canonical_json"} 0',
            lines,
        )
        self.assertIn(
            f'qbit_prism_component_bytes{{component="daemon_window_mirror_canonical_items"}} {len(items)}',
            lines,
        )
        # The same property stated as a sum, so a future backing that reports
        # under both slots fails here rather than in an operator's dashboard.
        self.assertEqual(
            sum(
                int(line.rsplit(" ", 1)[1])
                for line in lines
                if line.startswith("qbit_prism_component_bytes{")
                and (
                    'component="payout_window_canonical_json"' in line
                    or 'component="daemon_window_mirror_canonical_items"' in line
                )
            ),
            len(items),
        )
        # An empty window slot and an absent daemon client are true zeros.
        payout._incremental_payout_artifact_window = None
        server._ensure_bundle_compiler()._serve_builder = None
        lines = MetricsRenderer(server).component_cardinality_metrics_lines()
        self.assertIn('qbit_prism_component_entries{component="payout_window_records"} 0', lines)
        self.assertIn('qbit_prism_component_entries{component="daemon_uploaded_windows"} 0', lines)
        # The reads are direct: renaming an owner field breaks the scrape
        # loudly instead of reporting a silent zero.
        reconciler = server._ensure_reorg_reconciler_service()
        flights = reconciler._reconcile_flights
        del reconciler._reconcile_flights
        try:
            with self.assertRaises(AttributeError):
                MetricsRenderer(server).component_cardinality_metrics_lines()
        finally:
            reconciler._reconcile_flights = flights
        candidates = server._ensure_block_candidate_service()
        outstanding = candidates._outstanding_block_candidate_hashes
        del candidates._outstanding_block_candidate_hashes
        try:
            with self.assertRaises(AttributeError):
                MetricsRenderer(server).component_cardinality_metrics_lines()
        finally:
            candidates._outstanding_block_candidate_hashes = outstanding

    def test_component_cardinality_gauges_carry_no_unbounded_label(self) -> None:
        """No series carries a label that varies per job, tip, hash, or generation."""
        server, _entries, _bytes = self._seeded_owners()

        def series_set(lines: list[str]) -> list[tuple[str, str]]:
            observed: list[tuple[str, str]] = []
            for line in lines:
                if line.startswith("#"):
                    continue
                family, labels, _value = _parse_series(line)
                self.assertEqual(set(labels), {"component"}, line)
                closed = (
                    PRISM_COMPONENT_ENTRY_KINDS
                    if family == "qbit_prism_component_entries"
                    else PRISM_COMPONENT_BYTE_KINDS
                )
                self.assertIn(labels["component"], closed, line)
                observed.append((family, labels["component"]))
            return observed

        before = MetricsRenderer(server).component_cardinality_metrics_lines()
        baseline = series_set(before)
        self.assertEqual(len(baseline), len(set(baseline)))
        self.assertEqual(
            len(baseline),
            len(PRISM_COMPONENT_ENTRY_KINDS) + len(PRISM_COMPONENT_BYTE_KINDS),
        )
        # Grow every unbounded population the issue names -- more jobs, more
        # tips, more candidate hashes, more generations, more workers -- and
        # the series set must not move by one.
        seeded_identifiers = []
        for index in range(5):
            job_id = f"job-growth-{index}"
            tip_hash = f"{index:02x}" * 32
            block_hash = f"e{index}" * 32
            seeded_identifiers.extend((job_id, tip_hash, block_hash, f"worker-{index}"))
            server.jobs[job_id] = object()
            reconciler = server._ensure_reorg_reconciler_service()
            reconciler._reorg_reconcile_trusted_memo[tip_hash] = float(index)
            reconciler._reconcile_flights[tip_hash] = _ReconcileFlight()
            candidates = server._ensure_block_candidate_service()
            candidates._outstanding_block_candidate_hashes.add(block_hash)
            candidates._block_candidate_terminal_outcomes[block_hash] = True
            server._ensure_payout_state_service()._payout_append_invalidation_stamps[
                10 + index
            ] = index
            server._ensure_share_writer_service().adopt_pending_share(
                SimpleNamespace(share_id=f"worker-{index}:{block_hash}")
            )
        after = MetricsRenderer(server).component_cardinality_metrics_lines()
        self.assertEqual(series_set(after), baseline)
        self.assertEqual(_type_families(after), _type_families(before))
        rendered = "\n".join(after)
        for identifier in seeded_identifiers:
            self.assertNotIn(identifier, rendered)
        # The values moved; only the label set stayed put.
        self.assertGreater(
            _series_value(after, 'qbit_prism_component_entries{component="job_contexts"}'),
            _series_value(before, 'qbit_prism_component_entries{component="job_contexts"}'),
        )

    def test_heap_telemetry_scrape_does_not_walk_the_heap(self) -> None:
        """The collector never calls gc.get_objects or starts tracemalloc."""
        server, _entries, _bytes = self._seeded_owners()
        self.assertFalse(tracemalloc.is_tracing())
        with (
            mock.patch.object(
                gc,
                "get_objects",
                side_effect=AssertionError("gc.get_objects walked the heap on a scrape"),
            ) as get_objects,
            mock.patch.object(
                gc,
                "collect",
                side_effect=AssertionError("gc.collect ran on a scrape"),
            ) as collect,
            mock.patch.object(
                tracemalloc,
                "start",
                side_effect=AssertionError("tracemalloc started on a scrape"),
            ) as start,
            mock.patch.object(
                tracemalloc,
                "take_snapshot",
                side_effect=AssertionError("tracemalloc snapshot on a scrape"),
            ) as take_snapshot,
        ):
            renderer = MetricsRenderer(server)
            heap = renderer.process_heap_metrics_lines()
            components = renderer.component_cardinality_metrics_lines()
            document = renderer.render()
        self.assertEqual(_type_families(heap), list(PROCESS_HEAP_METRIC_FAMILIES))
        self.assertEqual(
            _type_families(components),
            list(COMPONENT_CARDINALITY_METRIC_FAMILIES),
        )
        self.assertIn("qbit_prism_process_allocated_blocks ", document)
        get_objects.assert_not_called()
        collect.assert_not_called()
        start.assert_not_called()
        take_snapshot.assert_not_called()
        self.assertFalse(tracemalloc.is_tracing())


# Issue #227: the closed family sets of the monitor-lateness signals and the
# GC pause durations; tests pin the renderer to both.
LEASE_MONITOR_STALL_METRIC_FAMILIES = (
    ("qbit_prism_lease_heartbeat_monitor_wake_lateness_seconds", "histogram"),
    ("qbit_prism_lease_heartbeat_monitor_late_wakes_total", "counter"),
    ("qbit_prism_lease_heartbeat_monitor_wake_delay_window_max_seconds", "gauge"),
    ("qbit_prism_lease_heartbeat_monitor_wake_delay_record_age_seconds", "gauge"),
    ("qbit_prism_lease_heartbeat_monitor_exit_guarantee_breaches_total", "counter"),
    ("qbit_prism_lease_heartbeat_monitor_worst_exit_guarantee_overrun_seconds", "gauge"),
    ("qbit_prism_lease_heartbeat_stall_probe_samples_total", "counter"),
    ("qbit_prism_lease_heartbeat_stall_probe_suppressed_total", "counter"),
    ("qbit_prism_lease_heartbeat_monitor_breach_warnings_suppressed_total", "counter"),
)
GC_PAUSE_METRIC_FAMILIES = (
    ("qbit_prism_process_gc_pause_seconds", "histogram"),
    ("qbit_prism_process_gc_last_pause_seconds", "gauge"),
    ("qbit_prism_process_gc_max_pause_seconds", "gauge"),
)


class LeaseMonitorStallTelemetryTests(unittest.TestCase):
    """Issue #227: the re-armable monitor-lateness signals and GC pauses."""

    @staticmethod
    def _series_keys(lines: list[str]) -> list[tuple[str, tuple[tuple[str, str], ...]]]:
        return [
            (family, tuple(sorted(labels.items())))
            for family, labels, _value in (
                _parse_series(line) for line in lines if not line.startswith("#")
            )
        ]

    def test_monitor_wake_delay_histogram_and_threshold_counters_are_pinned(
        self,
    ) -> None:
        """Family set, bucket boundaries and the slack-fraction counters.

        All fixed cardinality: the series set after two thousand more
        observations is identical to the series set before them. The
        lifetime ``..._monitor_wake_delay_seconds`` gauge still renders.
        """
        server = support.coordinator()
        service = server._ensure_lease_heartbeat_service()
        slack = service.policy().scheduler_slack_seconds
        # The renderer's snapshot reads the service clock (time.monotonic
        # here), so the seeded wakes are stamped on that clock too; the
        # rolling window and the record age are then read at "now".
        now = time.monotonic() - 10.0
        for delay in [0.0] * 100 + [0.3] * 40 + [0.45] * 30 + [0.6] * 20:
            now += 0.05
            service.monitor_wakes.observe(delay, slack_seconds=slack, now=now)
        service.monitor_wakes.observe(0.648, slack_seconds=slack, now=now + 0.05)
        # The lifetime gauge is the monitor loop's own high-water mark.
        service.monitor_wake_delay_seconds = 0.648
        renderer = MetricsRenderer(server)
        lines = renderer.lease_heartbeat_metrics_lines()

        families = _type_families(lines)
        self.assertIn(
            ("qbit_prism_lease_heartbeat_monitor_wake_delay_seconds", "gauge"), families
        )
        self.assertIn("qbit_prism_lease_heartbeat_monitor_wake_delay_seconds 0.648000", lines)
        self.assertEqual(
            families[-len(LEASE_MONITOR_STALL_METRIC_FAMILIES):],
            list(LEASE_MONITOR_STALL_METRIC_FAMILIES),
        )
        # Bucket boundaries are the closed set plus +Inf, cumulative.
        histogram = "qbit_prism_lease_heartbeat_monitor_wake_lateness_seconds"
        bucket_lines = [line for line in lines if line.startswith(histogram + "_bucket{")]
        self.assertEqual(
            [_parse_series(line)[1] for line in bucket_lines],
            [{"le": f"{bucket:g}"} for bucket in LEASE_MONITOR_WAKE_DELAY_BUCKETS]
            + [{"le": "+Inf"}],
        )
        counts = [int(_parse_series(line)[2]) for line in bucket_lines]
        self.assertEqual(counts, sorted(counts))
        self.assertEqual(counts[-1], 191)
        self.assertEqual(_series_value(lines, histogram + "_count"), 191)
        by_bound = dict(zip(LEASE_MONITOR_WAKE_DELAY_BUCKETS, counts))
        self.assertEqual(by_bound[0.001], 100)
        self.assertEqual(by_bound[0.4], 140)
        self.assertEqual(by_bound[0.5], 170)
        self.assertEqual(by_bound[0.75], 191)
        # The 0.5 / 0.8 / 1.0 x slack counters, exactly that label set.
        counter = "qbit_prism_lease_heartbeat_monitor_late_wakes_total"
        fraction_lines = [line for line in lines if line.startswith(counter + "{")]
        self.assertEqual(
            [_parse_series(line)[1]["slack_fraction"] for line in fraction_lines],
            list(LEASE_MONITOR_LATE_WAKE_SLACK_FRACTIONS),
        )
        self.assertEqual(_series_value(lines, counter + '{slack_fraction="0.5"}'), 91)
        self.assertEqual(_series_value(lines, counter + '{slack_fraction="0.8"}'), 51)
        self.assertEqual(_series_value(lines, counter + '{slack_fraction="1.0"}'), 21)
        # Rolling maximum, record age, the breach counters and the probe.
        self.assertIn(
            "qbit_prism_lease_heartbeat_monitor_wake_delay_window_max_seconds 0.648000",
            lines,
        )
        self.assertTrue(
            any(
                line.startswith(
                    "qbit_prism_lease_heartbeat_monitor_wake_delay_record_age_seconds "
                )
                for line in lines
            )
        )
        self.assertEqual(
            _series_value(lines, "qbit_prism_lease_heartbeat_monitor_exit_guarantee_breaches_total"),
            0,
        )
        self.assertEqual(
            _series_value(lines, "qbit_prism_lease_heartbeat_stall_probe_samples_total"), 0
        )
        # Suppressed breach warnings are exported alongside the probe's.
        service.suppressed_slack_breach_warnings = 4
        self.assertEqual(
            _series_value(
                renderer.lease_heartbeat_metrics_lines(),
                "qbit_prism_lease_heartbeat_monitor_breach_warnings_suppressed_total",
            ),
            4,
        )
        # The policy term alerts compare against, and the new phase.
        self.assertIn(
            'qbit_prism_lease_heartbeat_policy_seconds{term="max_guaranteed_monitor_lateness"} 0.550000',
            lines,
        )
        self.assertEqual(
            [
                _parse_series(line)[1]["phase"]
                for line in lines
                if line.startswith("qbit_prism_lease_heartbeat_phase_seconds{")
            ],
            list(LEASE_HEARTBEAT_PHASES),
        )
        self.assertIn("guard_client_resume", LEASE_HEARTBEAT_PHASES)
        # Fixed cardinality: traffic moves values, never the series set.
        before = self._series_keys(lines)
        for _ in range(2_000):
            now += 0.05
            service.monitor_wakes.observe(1.7, slack_seconds=slack, now=now)
        self.assertEqual(self._series_keys(renderer.lease_heartbeat_metrics_lines()), before)

    def test_gc_pause_families_are_pinned_to_the_closed_generation_set(self) -> None:
        server = support.coordinator()
        service = server._ensure_lease_heartbeat_service()
        service.gc_pauses.record("2", 0.3)
        service.gc_pauses.record("0", 0.0004)
        service.gc_pauses.record("7", 9.0)  # not a CPython generation: dropped
        renderer = MetricsRenderer(server)
        lines = renderer.gc_pause_metrics_lines()
        self.assertEqual(_type_families(lines), list(GC_PAUSE_METRIC_FAMILIES))
        rendered: dict[str, list[str]] = {}
        for line in lines:
            if line.startswith("#"):
                continue
            family, labels, _value = _parse_series(line)
            self.assertEqual(set(labels) - {"le"}, {"generation"})
            self.assertIn(labels["generation"], PRISM_GC_GENERATIONS)
            if family == "qbit_prism_process_gc_pause_seconds_bucket":
                rendered.setdefault(labels["generation"], []).append(labels["le"])
        for generation in PRISM_GC_GENERATIONS:
            self.assertEqual(
                rendered[generation],
                [f"{bucket:g}" for bucket in PRISM_GC_PAUSE_SECONDS_BUCKETS] + ["+Inf"],
            )
        self.assertEqual(
            _series_value(lines, 'qbit_prism_process_gc_pause_seconds_count{generation="2"}'),
            1,
        )
        self.assertEqual(
            _series_value(lines, 'qbit_prism_process_gc_pause_seconds_count{generation="1"}'),
            0,
        )
        self.assertIn('qbit_prism_process_gc_last_pause_seconds{generation="2"} 0.300000', lines)
        self.assertIn('qbit_prism_process_gc_max_pause_seconds{generation="0"} 0.000400', lines)
        # In the full document the block sits immediately before the issue
        # #226 heap block it complements.
        type_lines = [
            line for line in server.metrics_payload().splitlines()
            if line.startswith("# TYPE ")
        ]
        index = type_lines.index("# TYPE qbit_prism_process_gc_pause_seconds histogram")
        self.assertEqual(
            type_lines[index : index + len(GC_PAUSE_METRIC_FAMILIES)],
            [f"# TYPE {family} {kind}" for family, kind in GC_PAUSE_METRIC_FAMILIES],
        )
        self.assertEqual(
            type_lines[index + len(GC_PAUSE_METRIC_FAMILIES)],
            "# TYPE qbit_prism_process_allocated_blocks gauge",
        )


if __name__ == "__main__":
    unittest.main()
