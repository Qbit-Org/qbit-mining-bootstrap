"""Read-only Prometheus assembly for the PRISM coordinator.

The renderer owns the complete live/base metrics document and the formatter
bodies PR 80 extracted from the coordinator.  It holds no producer state:
counters, histograms, and label registries stay with their owner services
(J1, B1, share submission, P1, reorg, ledger, observability) and reach the
renderer through the :class:`MetricsPort` as copied snapshots or bounded
per-domain ``*_metrics_lines()`` formatters.  The renderer never calls the
cached-metrics path, so the snapshot refresher can call it without recursion.
"""

from __future__ import annotations

import time
from typing import Any, Protocol

from lab.prism.accepted_preview_telemetry import (
    PRISM_ACCEPTED_LANDING_PHASES,
    PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS,
    PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS,
    PRISM_REORG_RECONCILE_CALLERS,
    PRISM_REORG_RECONCILE_STEPS,
    ensure_accepted_preview_telemetry,
    fold_ledger_read_stats,
)
from lab.prism.block_candidates import (
    PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS,
    PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS,
    PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES,
    PRISM_STALE_JOB_ABANDON_CLASSES,
)
from lab.prism.coordinator_config import (
    DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
    DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS,
    DEFAULT_PRISM_MALLOC_TELEMETRY,
    env_malloc_telemetry,
)
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
from lab.prism.process_telemetry import (
    PRISM_GC_GENERATIONS,
    ProcessHeapTelemetry,
)
from lab.prism.reorg_reconciler import (
    PRISM_REORG_RECONCILE_LOOKUP_PATHS,
    PRISM_REORG_RECONCILE_LOOKUP_SOURCES,
)
from lab.prism.share_ledger import DaemonShareWindowMirror
from lab.prism.share_submission import PRISM_SHARE_ACK_RESULTS
from lab.prism.vardiff_service import PRISM_VARDIFF_RESUME_OUTCOMES
from lab.prism.writer_lease_timing import (
    LEASE_HEARTBEAT_MODES,
    LEASE_HEARTBEAT_OUTCOMES,
    LEASE_HEARTBEAT_PHASES,
    LEASE_HEARTBEAT_POLICY_TERMS,
)

# Issue #226 (and the gauge list issue #185 asked for): the closed component
# label set of qbit_prism_component_entries. Every entry is one len() or
# Queue.qsize() over a structure its owner constructs in __init__ (or
# backfills through its own _ensure_* method), so a scrape costs a fixed
# number of constant-time reads and the series set cannot grow with jobs,
# tips, candidates, connections, or generations. Extend this tuple
# deliberately: tests/test_prism_metrics.py pins it as the family's exact
# series set.
PRISM_COMPONENT_ENTRY_KINDS = (
    # The payout window: the in-process incremental window's pages and
    # records, the armed ledger artifact's share sequence, and the
    # payout-state maps that are keyed by hash, epoch, or token.
    "payout_window_pages",
    "payout_window_records",
    "payout_ledger_artifact_shares",
    "accepted_block_payout_previews",
    "invalidated_accepted_block_payout_previews",
    "payout_append_invalidation_stamps",
    "payout_window_inflight_scan_anchors",
    "payout_unfenced_append_inflight_stamps",
    # The single-slot share-window serialization cache: shares it covers.
    "share_window_serialization_shares",
    # Job contexts and the bundle cache.
    "job_contexts",
    "job_bundle_cache",
    "job_build_issued_stamps",
    "bundle_preparation_flights",
    "active_job_bundle_builds",
    # The evicted-job graveyard.
    "evicted_job_graveyard",
    "evicted_same_tip_job_ids",
    # Candidate and replay state: the live, replay, and quarantine lanes
    # plus every hash-keyed registry the B1 owner retains.
    "block_candidate_queue",
    "block_replay_queue",
    "block_replay_inflight_hashes",
    "block_quarantine_queue",
    "block_quarantine_hashes",
    "outstanding_block_candidate_hashes",
    "tip_observed_accepted_block_hashes",
    "counted_block_candidate_abandonments",
    "accepted_block_preview_stamps",
    "ancestor_redrive_records",
    "block_candidate_terminal_outcomes",
    "block_candidate_disposition_flights",
    "block_disposition_waiting_retries",
    "block_candidate_dequeued_hashes",
    "accounted_accepted_block_hashes",
    # Reconcile flights and the trusted-tip memo.
    "reconcile_flights",
    "reconcile_trusted_memo",
    # Pending-share holders on the snapshot-anchor floor.
    "pending_share_commit_floor",
    # The daemon mirror: records in a mirror-backed window, and the
    # compiler's bounded mirror of the daemon's uploaded-window LRU.
    "daemon_window_mirror_records",
    "daemon_uploaded_windows",
)
# The closed component label set of qbit_prism_component_bytes: the retained
# byte-sized payloads whose size, not count, is what scales with the window.
PRISM_COMPONENT_BYTE_KINDS = (
    "payout_window_canonical_json",
    "daemon_window_mirror_canonical_items",
    "share_window_serialization_spool",
    "share_window_serialization_compact_json",
)


class MetricsPort(Protocol):
    """Explicit read capabilities used to assemble one metrics document.

    The renderer is observational but not pure: it still performs live
    RPC/ledger reads and calls bounded retained-job pruning, and several
    ``_ensure_*`` calls can initialize owner state on first touch. The
    issue #224 attribution owner is likewise attached to the port's
    instance ``__dict__`` on first render
    (``ensure_accepted_preview_telemetry``), so the port needs an
    instance dictionary but no dedicated attribute or method for it.
    """

    ledger: Any
    rpc: Any
    lock: Any
    clients: Any
    connection_limit_rejection_counts: Any
    evicted_job_capacity_eviction_counts: Any
    evicted_job_expiration_counts: Any
    evicted_job_graveyard: Any
    evicted_job_submit_counts: Any
    evicted_same_tip_job_ids: Any
    worker_metrics_lock: Any
    worker_share_counts: Any
    worker_rejection_counts: Any
    accepted_block_count: Any
    matured_payout_count: Any
    post_accept_refresh_failure_count: Any
    reorg_inactive_block_count: Any
    reorg_reactivated_block_count: Any
    reorg_reconcile_error_count: Any
    reorg_reconcile_skip_count: Any
    reorg_reconcile_lookup_counts: Any
    started_monotonic: Any
    tip_refresh_job_count: Any
    vardiff_config: Any
    listener_profiles: Any
    accept_resource_exhaustion_count: Any
    block_candidate_abandoned_counts: Any
    block_candidate_accept_pending_defer_count: Any
    block_candidate_poisoned_count: Any
    block_candidate_retry_count: Any
    block_candidate_wakeups_coalesced: Any
    block_candidates_dropped: Any
    block_solves_dropped_counts: Any
    stale_job_abandon_counts: Any
    connection_setup_failure_count: Any
    idle_retarget_count: Any
    jobs: Any
    jobs_lock: Any
    job_build_failure_count: Any
    latest_coinbase_size_bytes: Any
    share_append_queue: Any
    share_append_failure_count: Any
    shares_recovered_to_disk: Any
    shares_replayed: Any
    share_replay_conflicts: Any
    initial_job_sent_count: Any
    initial_job_cancelled_count: Any
    initial_job_coalesced_count: Any
    initial_job_failed_count: Any
    initial_job_superseded_count: Any
    initial_job_delivery_latency_seconds_sum: Any
    initial_job_delivery_latency_count: Any
    initial_job_queue_capacity_reclaimed_count: Any
    initial_job_max_workers: Any
    _initial_job_executor: Any
    _bundle_preparation_lock: Any
    shared_bundle_build_counts: Any
    shared_bundle_preparation_seconds_sum: Any
    shared_bundle_preparation_count: Any
    shared_bundle_preparation_waiters: Any
    _job_cache_lock: Any
    job_build_seconds_bucket_counts: Any
    job_build_seconds_sum: Any
    job_build_count: Any
    job_build_phase_seconds: Any
    job_cache_hit_counts: Any
    job_cache_miss_counts: Any
    _job_build_scheduler_lock: Any
    job_build_scheduler_counts: Any
    job_build_priority_counts: Any
    job_build_priority_admission_seconds: Any
    initial_job_prepared_work_counts: Any
    job_build_cancellation_seconds: Any
    job_build_replacement_start_seconds: Any
    job_build_worker_counts: Any
    job_build_executor_workers: Any
    _job_build_active: Any
    _job_build_pending: Any
    _job_build_retiring: Any
    _job_build_priority_preparations: Any
    _accepted_parent_preview_wait_timeouts: Any
    accepted_parent_redrive_attempt_count: Any
    accepted_parent_redrive_resolved_count: Any
    accepted_parent_redrive_exhausted_count: Any
    _accounted_accepted_block_hashes: Any

    def _accepted_parent_unresolved_depth(self) -> int: ...
    def _accepted_parent_unresolved_depth_cap(self) -> int: ...
    def _ensure_block_candidate_service(self) -> Any: ...
    def _ensure_bundle_compiler(self) -> Any: ...
    def _ensure_connection_capacity_state(self) -> Any: ...
    def _ensure_evicted_job_state(self) -> Any: ...
    def _ensure_initial_job_state(self) -> Any: ...
    def _ensure_job_bundle_service(self) -> Any: ...
    def _ensure_job_cache_state(self) -> Any: ...
    def _ensure_observability_service(self) -> Any: ...
    def _ensure_lease_heartbeat_service(self) -> Any: ...
    def _ensure_payout_state_service(self) -> Any: ...
    def _ensure_share_writer_service(self) -> Any: ...
    def _ensure_shutdown_controller(self) -> Any: ...
    def _ensure_worker_metrics_state(self) -> Any: ...
    def _job_build_is_publication_critical(self, request: Any) -> bool: ...
    def accepted_share_stats(self) -> Any: ...
    def accepted_parent_unresolved_ages_seconds(self) -> list[float]: ...
    def audit_artifact_metrics(self) -> Any: ...
    def block_candidate_collapse_snapshot(self) -> dict[str, int]: ...
    def block_finalization_metrics_lines(self) -> Any: ...
    def block_ledger_call_class_metrics(self) -> Any: ...
    def block_submitter_snapshot(self) -> dict[str, object]: ...
    def coordinator_lock_contention_snapshot(self) -> tuple[int, int, float, float]: ...
    def ctv_fanout_broadcaster_metrics_lines(self) -> Any: ...
    def mining_delivery_snapshot(self) -> Any: ...
    def payout_state_metrics_lines(self) -> Any: ...
    def process_resource_metrics(self) -> Any: ...
    def progress_health_metrics_lines(self) -> Any: ...
    def prometheus_label_value(self, value: str) -> str: ...
    def prune_evicted_job_graveyard(self, *, force: bool) -> Any: ...
    def rejection_reason_ids(self) -> tuple[str, ...]: ...
    def share_accounting_snapshot(self) -> dict[str, object]: ...
    def share_ack_snapshot(self) -> dict[str, dict[str, Any]]: ...
    def startup_phase_seconds(self) -> dict[str, float]: ...
    def tip_refresh_metrics_lines(self) -> Any: ...
    def vardiff_convergence_snapshot(self) -> Any: ...
    def vardiff_idle_metrics_lines(self) -> Any: ...


class MetricsRenderer:
    """Collect and format one complete metrics generation."""

    def __init__(
        self,
        port: MetricsPort,
        *,
        process_telemetry: ProcessHeapTelemetry | None = None,
    ) -> None:
        self.port = port
        if process_telemetry is None:
            # Resolution order, most authoritative first. A port attribute
            # lets an embedder or a test pin the switch outright. Otherwise
            # the validated lifecycle snapshot the coordinator was built with
            # wins: a caller that passed an explicit CoordinatorConfig with
            # telemetry disabled must not have mallinfo2 run anyway because
            # the ambient environment happens to say otherwise. Only with no
            # snapshot at all do we read the environment. Resolved once here
            # so a scrape never touches any of them.
            enabled = getattr(port, "malloc_telemetry_enabled", None)
            if enabled is None:
                config = getattr(port, "config", None)
                lifecycle = getattr(config, "lifecycle", None)
                enabled = getattr(lifecycle, "malloc_telemetry_enabled", None)
            if enabled is None:
                try:
                    enabled = env_malloc_telemetry()
                except BaseException:
                    # The config loader validated this switch at startup, so
                    # reaching here means the environment changed underneath a
                    # running process. env_malloc_telemetry is fail-closed and
                    # exits on a bad value, which is right at startup and wrong
                    # on a metrics thread: killing the coordinator over a
                    # telemetry switch would turn an observability knob into an
                    # outage. Fall back to the shipped default instead.
                    enabled = DEFAULT_PRISM_MALLOC_TELEMETRY
            process_telemetry = ProcessHeapTelemetry(malloc_enabled=bool(enabled))
        self._process_telemetry = process_telemetry

    def render(self) -> str:
        ledger_metrics = self.port.ledger.metrics()
        audit_metrics = self.port.audit_artifact_metrics()
        mining_metrics = self.port.mining_delivery_snapshot()
        process_rss_bytes, process_open_fds = self.port.process_resource_metrics()
        accepted_share_count = self.port.accepted_share_stats()[0]
        share_accounting = self.port.share_accounting_snapshot()
        submitted_share_count = int(share_accounting["submitted"])
        stale_share_count = int(share_accounting["stale"])
        duplicate_share_count = int(share_accounting["duplicate"])
        low_difficulty_share_count = int(share_accounting["low_difficulty"])
        collection_block_submission_count = int(
            share_accounting["collection_block"]
        )
        rejection_counts = share_accounting["rejections"]
        assert isinstance(rejection_counts, dict)
        grace_credited_share_count = int(share_accounting["grace_credited"])
        # The submission owner's copied snapshot (via the routing descriptor)
        # is read outside the accounting lock so owner locks never nest.
        block_solves_dropped_counts = dict(
            getattr(
                self.port,
                "block_solves_dropped_counts",
                {"stale_grace": 0},
            )
        )
        stale_percent = 0.0
        if submitted_share_count > 0:
            stale_percent = (stale_share_count / submitted_share_count) * 100.0
        idle_retarget_count = int(getattr(self.port, "idle_retarget_count", 0))
        with self.port.lock:
            self.port._ensure_connection_capacity_state()
            active_connection_count = len(self.port.clients)
            connection_limit_rejection_counts = dict(
                self.port.connection_limit_rejection_counts
            )
            accept_resource_exhaustion_count = int(
                getattr(self.port, "accept_resource_exhaustion_count", 0)
            )
            connection_setup_failure_count = int(
                getattr(self.port, "connection_setup_failure_count", 0)
            )
            self.port._ensure_evicted_job_state()
            self.port.prune_evicted_job_graveyard(force=False)
            same_tip_context_count = len(self.port.evicted_same_tip_job_ids)
            evicted_job_context_counts = {
                "same_tip": same_tip_context_count,
                "stale_grace": len(self.port.evicted_job_graveyard) - same_tip_context_count,
            }
            evicted_job_submit_counts = dict(self.port.evicted_job_submit_counts)
            evicted_job_expiration_counts = dict(self.port.evicted_job_expiration_counts)
            evicted_job_capacity_eviction_counts = dict(
                self.port.evicted_job_capacity_eviction_counts
            )
            stale_job_abandon_counts = dict(
                getattr(
                    self.port,
                    "stale_job_abandon_counts",
                    {
                        abandon_class: 0
                        for abandon_class in PRISM_STALE_JOB_ABANDON_CLASSES
                    },
                )
            )
        self.port._ensure_worker_metrics_state()
        with self.port.worker_metrics_lock:
            worker_share_counts = {
                label: dict(counts)
                for label, counts in self.port.worker_share_counts.items()
            }
            worker_rejection_counts = dict(self.port.worker_rejection_counts)
        coinbase_weight_headroom = 2_000_000
        latest_coinbase_size_bytes = getattr(
            self.port, "latest_coinbase_size_bytes", None
        )
        if latest_coinbase_size_bytes is not None:
            coinbase_weight_headroom = 2_000_000 - int(latest_coinbase_size_bytes)
        ctv_pending = 0
        ctv_broadcastable = 0
        ctv_failed = 0
        pending_ctv_fanouts = getattr(
            self.port.ledger, "pending_ctv_fanout_statuses", None
        )
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
            blockchain_info = self.port.rpc.call("getblockchaininfo")
            if isinstance(blockchain_info, dict) and blockchain_info.get("initialblockdownload"):
                ibd = 1
        except Exception:
            ibd = -1
        try:
            network_info = self.port.rpc.call("getnetworkinfo")
            if isinstance(network_info, dict):
                peers = int(network_info.get("connections", 0))
        except Exception:
            peers = -1
        rejection_reason_ids = self.port.rejection_reason_ids()
        lines = [
            "# HELP qbit_prism_accepted_shares_total Accepted shares recorded by the canonical PRISM ledger.",
            "# TYPE qbit_prism_accepted_shares_total counter",
            f"qbit_prism_accepted_shares_total {accepted_share_count}",
            *self.accepted_stats_reconcile_metric_lines(),
            *self.landing_observability_metrics_lines(),
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
                for reason in rejection_reason_ids
            ],
            "# HELP qbit_prism_worker_submitted_shares_total Stratum share submissions by bounded worker label.",
            "# TYPE qbit_prism_worker_submitted_shares_total counter",
            *[
                f'qbit_prism_worker_submitted_shares_total{{worker="{self.port.prometheus_label_value(label)}"}} {int(counts.get("submitted", 0))}'
                for label, counts in sorted(worker_share_counts.items())
            ],
            "# HELP qbit_prism_worker_accepted_shares_total Accepted shares by bounded worker label.",
            "# TYPE qbit_prism_worker_accepted_shares_total counter",
            *[
                f'qbit_prism_worker_accepted_shares_total{{worker="{self.port.prometheus_label_value(label)}"}} {int(counts.get("accepted", 0))}'
                for label, counts in sorted(worker_share_counts.items())
            ],
            "# HELP qbit_prism_worker_grace_credited_shares_total Stale-grace credited shares by bounded worker label.",
            "# TYPE qbit_prism_worker_grace_credited_shares_total counter",
            *[
                f'qbit_prism_worker_grace_credited_shares_total{{worker="{self.port.prometheus_label_value(label)}"}} {int(counts.get("grace", 0))}'
                for label, counts in sorted(worker_share_counts.items())
            ],
            "# HELP qbit_prism_worker_rejections_total PRISM share or block rejections by bounded worker label and reason ID.",
            "# TYPE qbit_prism_worker_rejections_total counter",
            *[
                f'qbit_prism_worker_rejections_total{{worker="{self.port.prometheus_label_value(label)}",reason_id="{reason}"}} {int(count)}'
                for (label, reason), count in sorted(worker_rejection_counts.items())
            ],
            "# HELP qbit_prism_job_build_failures_total Job builds skipped after a template/coinbase error without dropping the client.",
            "# TYPE qbit_prism_job_build_failures_total counter",
            f"qbit_prism_job_build_failures_total {self.port.job_build_failure_count}",
            "# HELP qbit_prism_block_candidates_dropped_total Legacy counter; durable candidate outbox rows are never dropped on queue overflow.",
            "# TYPE qbit_prism_block_candidates_dropped_total counter",
            f"qbit_prism_block_candidates_dropped_total {int(getattr(self.port, 'block_candidates_dropped', 0))}",
            "# HELP qbit_prism_block_candidate_wakeups_coalesced_total Candidate queue wakeups coalesced while the durable outbox retained the work.",
            "# TYPE qbit_prism_block_candidate_wakeups_coalesced_total counter",
            f"qbit_prism_block_candidate_wakeups_coalesced_total {int(getattr(self.port, 'block_candidate_wakeups_coalesced', 0))}",
            "# HELP qbit_prism_block_candidate_retries_total Transient candidate outcomes retained for durable retry.",
            "# TYPE qbit_prism_block_candidate_retries_total counter",
            f"qbit_prism_block_candidate_retries_total {int(getattr(self.port, 'block_candidate_retry_count', 0))}",
            "# HELP qbit_prism_block_candidate_accept_pending_defers_total Terminal abandonments refused because the candidate is (or was recently observed as) an active chain block; the candidate retries until its accepted success tail finalizes it as submitted.",
            "# TYPE qbit_prism_block_candidate_accept_pending_defers_total counter",
            f"qbit_prism_block_candidate_accept_pending_defers_total {int(getattr(self.port, 'block_candidate_accept_pending_defer_count', 0))}",
            "# HELP qbit_prism_block_candidate_poisoned_total Invalid durable candidate intents quarantined from replay.",
            "# TYPE qbit_prism_block_candidate_poisoned_total counter",
            f"qbit_prism_block_candidate_poisoned_total {int(getattr(self.port, 'block_candidate_poisoned_count', 0))}",
            "# HELP qbit_prism_block_candidates_abandoned_total Block candidates that did not land (lost tip race or failed submit), by reason. Not share rejections: the underlying share was accepted.",
            "# TYPE qbit_prism_block_candidates_abandoned_total counter",
            *[
                f'qbit_prism_block_candidates_abandoned_total{{reason_id="{reason}"}} {int(count)}'
                for reason, count in sorted(getattr(self.port, "block_candidate_abandoned_counts", {}).items())
            ],
            "# HELP qbit_prism_stale_job_abandons_total Terminal stale-job block candidate abandonments by bounded cause.",
            "# TYPE qbit_prism_stale_job_abandons_total counter",
            *[
                f'qbit_prism_stale_job_abandons_total{{class="{abandon_class}"}} {int(stale_job_abandon_counts.get(abandon_class, 0))}'
                for abandon_class in PRISM_STALE_JOB_ABANDON_CLASSES
            ],
            "# HELP qbit_prism_share_append_queue_depth Accepted shares waiting on the ledger writer thread.",
            "# TYPE qbit_prism_share_append_queue_depth gauge",
            f"qbit_prism_share_append_queue_depth {self.port.share_append_queue.qsize() if getattr(self.port, 'share_append_queue', None) is not None else 0}",
            "# HELP qbit_prism_share_append_failures_total Shares in group commits that failed before acknowledgement.",
            "# TYPE qbit_prism_share_append_failures_total counter",
            f"qbit_prism_share_append_failures_total {int(getattr(self.port, 'share_append_failure_count', 0))}",
            "# HELP qbit_prism_shares_recovered_to_disk_total Legacy pre-commit-ACK shares written to the upgrade recovery file.",
            "# TYPE qbit_prism_shares_recovered_to_disk_total counter",
            f"qbit_prism_shares_recovered_to_disk_total {int(getattr(self.port, 'shares_recovered_to_disk', 0))}",
            "# HELP qbit_prism_shares_replayed_total Recovery-file shares replayed into the ledger at startup.",
            "# TYPE qbit_prism_shares_replayed_total counter",
            f"qbit_prism_shares_replayed_total {int(getattr(self.port, 'shares_replayed', 0))}",
            "# HELP qbit_prism_share_replay_conflicts_total Recovery-file rows quarantined because the durable row disagrees with the journal payload.",
            "# TYPE qbit_prism_share_replay_conflicts_total counter",
            f"qbit_prism_share_replay_conflicts_total {int(getattr(self.port, 'share_replay_conflicts', 0))}",
            "# HELP qbit_prism_tip_refresh_jobs_total Client jobs refreshed after qbit tip/template changes.",
            "# TYPE qbit_prism_tip_refresh_jobs_total counter",
            f"qbit_prism_tip_refresh_jobs_total {self.port.tip_refresh_job_count}",
            "# HELP qbit_prism_active_job_contexts Current retained PRISM job contexts.",
            "# TYPE qbit_prism_active_job_contexts gauge",
            f"qbit_prism_active_job_contexts {len(getattr(self.port, 'jobs', {}))}",
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
            f"qbit_prism_post_accept_refresh_failures_total {self.port.post_accept_refresh_failure_count}",
            "# HELP qbit_prism_reorg_inactive_blocks_total PRISM pool blocks quarantined after leaving the active chain.",
            "# TYPE qbit_prism_reorg_inactive_blocks_total counter",
            f"qbit_prism_reorg_inactive_blocks_total {self.port.reorg_inactive_block_count}",
            "# HELP qbit_prism_reorg_reactivated_blocks_total Quarantined PRISM pool blocks restored after returning to the active chain.",
            "# TYPE qbit_prism_reorg_reactivated_blocks_total counter",
            f"qbit_prism_reorg_reactivated_blocks_total {self.port.reorg_reactivated_block_count}",
            "# HELP qbit_prism_reorg_reconcile_skips_total Reorg reconciliation passes skipped because qbitd chain view was not trusted.",
            "# TYPE qbit_prism_reorg_reconcile_skips_total counter",
            f"qbit_prism_reorg_reconcile_skips_total {self.port.reorg_reconcile_skip_count}",
            "# HELP qbit_prism_reorg_reconcile_errors_total Reorg reconciliation errors that prevented ordered job issuance.",
            "# TYPE qbit_prism_reorg_reconcile_errors_total counter",
            f"qbit_prism_reorg_reconcile_errors_total {self.port.reorg_reconcile_error_count}",
            "# HELP qbit_prism_reorg_reconcile_lookups_total Reconcile demand by caller path and the source that satisfied it.",
            "# TYPE qbit_prism_reorg_reconcile_lookups_total counter",
            *[
                f'qbit_prism_reorg_reconcile_lookups_total{{path="{path}",source="{source}"}} '
                f"{int(getattr(self.port, 'reorg_reconcile_lookup_counts', {}).get((path, source), 0))}"
                for path in PRISM_REORG_RECONCILE_LOOKUP_PATHS
                for source in PRISM_REORG_RECONCILE_LOOKUP_SOURCES
            ],
            "# HELP qbit_prism_matured_payouts_total Payout entries marked mature by the coordinator tip reconciliation path.",
            "# TYPE qbit_prism_matured_payouts_total counter",
            f"qbit_prism_matured_payouts_total {self.port.matured_payout_count}",
            "# HELP qbit_prism_vardiff_idle_retargets_total Vardiff retargets triggered by the idle zero-accepted-share sweep.",
            "# TYPE qbit_prism_vardiff_idle_retargets_total counter",
            f"qbit_prism_vardiff_idle_retargets_total {idle_retarget_count}",
            # qbit_prism_shares_per_second is deliberately absent. It divided
            # the ledger's lifetime accepted count by this process's uptime --
            # different domains, so the value tracked 1/uptime rather than any
            # share rate: enormous right after a restart, decaying toward zero
            # as the process aged, and never once the pool's throughput. Use
            # rate(qbit_prism_accepted_shares_total[5m]), which is what the
            # counter above exists for, or sum the per-lane gauge for this
            # process's own since-start commit rate.
            "# HELP qbit_prism_stale_share_percent Percent of submitted shares classified stale.",
            "# TYPE qbit_prism_stale_share_percent gauge",
            f"qbit_prism_stale_share_percent {stale_percent:.12g}",
            "# HELP qbit_prism_blocks_accepted_total Blocks accepted through the PRISM coordinator.",
            "# TYPE qbit_prism_blocks_accepted_total counter",
            f"qbit_prism_blocks_accepted_total {self.port.accepted_block_count}",
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
            f"qbit_prism_vardiff_enabled {1 if self.port.vardiff_config.enabled else 0}",
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
        lines.extend(self.lease_heartbeat_metrics_lines())
        lines.extend(self.shutdown_metrics_lines())
        lines.extend(self.coordinator_lock_metrics_lines())
        lines.extend(self.block_submitter_metrics_lines())
        lines.extend(self.block_candidate_cleanup_backlog_metrics_lines())
        lines.extend(self.share_ack_metrics_lines())
        lines.extend(self.port.ctv_fanout_broadcaster_metrics_lines())
        lines.extend(self.port.vardiff_idle_metrics_lines())
        lines.extend(self.vardiff_convergence_metrics_lines())
        lines.extend(self.port.block_finalization_metrics_lines())
        lines.extend(self.job_build_metrics_lines())
        lines.extend(self.port.tip_refresh_metrics_lines())
        lines.extend(self.port.payout_state_metrics_lines())
        lines.extend(self.initial_delivery_metrics_lines())
        lines.extend(self.port.progress_health_metrics_lines())
        lines.extend(self.accepted_preview_attribution_metrics_lines())
        lines.extend(self.process_heap_metrics_lines())
        lines.extend(self.component_cardinality_metrics_lines())
        return "\n".join(lines) + "\n"

    def share_ack_metrics_lines(self) -> list[str]:
        histograms = self.port.share_ack_snapshot()
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

    def vardiff_convergence_metrics_lines(self) -> list[str]:
        """Vardiff convergence and reconnect-resume observability.

        The lane label set is deterministic: configured listener profiles in
        order, then any additional lane names observed in the counters,
        sorted. The per-second gauge divides a process-local count by this
        process's uptime, so numerator and denominator share a domain -- but
        that numerator is narrower than "accepted". VardiffService.note_accepted
        runs only where the share writer sees a newly inserted ledger row, and
        the startup recovery-journal replay reaches the ledger without passing
        through it at all, so an exact replay and a recovered share are both
        acked accepted while never being lane-counted. The lanes therefore sum
        to this coordinator's own commit rate, not to its accepted-ack count.
        """
        snapshot = self.port.vardiff_convergence_snapshot()
        lane_accepted = snapshot["lane_accepted_shares"]
        assert isinstance(lane_accepted, dict)
        resume_outcomes = snapshot["resume_outcomes"]
        assert isinstance(resume_outcomes, dict)
        lanes = [
            str(profile.name)
            for profile in getattr(self.port, "listener_profiles", ()) or ()
        ]
        lanes.extend(sorted(lane for lane in lane_accepted if lane not in lanes))
        elapsed = max(0.001, time.monotonic() - self.port.started_monotonic)
        return [
            "# HELP qbit_prism_vardiff_sessions_at_max_difficulty Sessions whose current difficulty sits at their effective vardiff ceiling.",
            "# TYPE qbit_prism_vardiff_sessions_at_max_difficulty gauge",
            f"qbit_prism_vardiff_sessions_at_max_difficulty {int(snapshot['sessions_at_max_difficulty'])}",
            "# HELP qbit_prism_vardiff_lane_accepted_shares_total Accepted shares by Stratum lane.",
            "# TYPE qbit_prism_vardiff_lane_accepted_shares_total counter",
            *[
                f'qbit_prism_vardiff_lane_accepted_shares_total{{lane="{self.port.prometheus_label_value(lane)}"}} {int(lane_accepted.get(lane, 0))}'
                for lane in lanes
            ],
            "# HELP qbit_prism_vardiff_lane_accepted_shares_per_second Accepted shares per second by Stratum lane, averaged since coordinator start; a long-run average cannot show a transient reconnect storm -- use rate(qbit_prism_vardiff_lane_accepted_shares_total[5m]) for that. The numerator counts shares this process newly committed on the live submission path, not the ledger lifetime total published by qbit_prism_accepted_shares_total: an exact replay of an already-durable share and a startup recovery-journal replay are both acked accepted without being lane-counted, so the lanes sum to this coordinator's own commit rate and need not reconcile against an accepted-ack count.",
            "# TYPE qbit_prism_vardiff_lane_accepted_shares_per_second gauge",
            *[
                f'qbit_prism_vardiff_lane_accepted_shares_per_second{{lane="{self.port.prometheus_label_value(lane)}"}} {int(lane_accepted.get(lane, 0)) / elapsed:.12g}'
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

    def coordinator_lock_metrics_lines(self) -> list[str]:
        acquisition_count, contention_count, wait_sum, wait_max = (
            self.port.coordinator_lock_contention_snapshot()
        )
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

    @staticmethod
    def _accepted_block_preview_publication_lines(
        histograms: dict[str, Any],
    ) -> list[str]:
        """Per-result bucket/sum/count lines for the publication histogram."""
        lines: list[str] = []
        for result in PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS:
            histogram = histograms[result]
            buckets = histogram["buckets"]
            count = int(histogram["count"])
            lines.extend(
                f"qbit_prism_accepted_block_preview_publication_seconds_bucket"
                f'{{result="{result}",le="{bucket:g}"}} '
                f"{int(buckets.get(bucket, 0))}"
                for bucket in (
                    PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS
                )
            )
            lines.append(
                f"qbit_prism_accepted_block_preview_publication_seconds_bucket"
                f'{{result="{result}",le="+Inf"}} {count}'
            )
            lines.append(
                f"qbit_prism_accepted_block_preview_publication_seconds_sum"
                f'{{result="{result}"}} {float(histogram["sum"]):.6f}'
            )
            lines.append(
                f"qbit_prism_accepted_block_preview_publication_seconds_count"
                f'{{result="{result}"}} {count}'
            )
        return lines

    def block_submitter_metrics_lines(self) -> list[str]:
        snapshot = self.port.block_submitter_snapshot()
        submit_buckets = snapshot["submit_seconds_buckets"]
        assert isinstance(submit_buckets, dict)
        submit_sum = float(snapshot["submit_seconds_sum"])
        submit_count = int(snapshot["submit_seconds_count"])
        backoff_active = bool(snapshot["backoff_active"])
        backoff_remaining = float(snapshot["backoff_remaining_seconds"])
        backoff_delay = float(snapshot["backoff_delay_seconds"])
        # The B1 owner copies these under the coordinator lock; the key set
        # is fixed by PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES so the series
        # cardinality cannot grow with the candidate population.
        collapse_counts = self.port.block_candidate_collapse_snapshot()
        # Issue #181 item 3: the label set is closed by
        # PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS, so this series
        # carries no block hash or height and cannot grow with the candidate
        # population.
        preview_publication = snapshot["accepted_preview_publication"]
        assert isinstance(preview_publication, dict)
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
            *self._accepted_block_preview_publication_lines(preview_publication),
            "# HELP qbit_prism_block_candidates_pending Durable block candidates awaiting a terminal outcome, or -1 if unavailable.",
            "# TYPE qbit_prism_block_candidates_pending gauge",
            f"qbit_prism_block_candidates_pending {int(snapshot['pending_count'])}",
            "# HELP qbit_prism_block_candidate_oldest_pending_seconds Age of the oldest durable pending block candidate, or -1 if unavailable.",
            "# TYPE qbit_prism_block_candidate_oldest_pending_seconds gauge",
            f"qbit_prism_block_candidate_oldest_pending_seconds {float(snapshot['oldest_pending_age_seconds']):.6f}",
            "# HELP qbit_prism_block_candidate_oldest_unattempted_seconds Age of the oldest durable candidate that has never entered processing, or -1 if unavailable.",
            "# TYPE qbit_prism_block_candidate_oldest_unattempted_seconds gauge",
            f"qbit_prism_block_candidate_oldest_unattempted_seconds {float(snapshot['oldest_unattempted_age_seconds']):.6f}",
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

    def block_candidate_cleanup_backlog_metrics_lines(self) -> list[str]:
        """Issue #198: the collapse cleanup-retry backlog and its bound.

        Seven single-series families, none labelled: the B1 owner copies
        the values under the coordinator lock and the key set of its
        snapshot is closed, so the export cannot grow with the candidate
        population or carry a hash. The bound is exported beside the depth
        so an alert can be a ratio against the configured contract instead
        of a deployment-specific literal (the same shape as the
        accepted-parent depth cap).
        """
        snapshot = (
            self.port._ensure_block_candidate_service()
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

    def process_heap_metrics_lines(self) -> list[str]:
        """Issue #226: always-on interpreter and glibc allocator readings.

        Eleven families. Only the three GC families carry a label, and its
        value set is the closed PRISM_GC_GENERATIONS; nothing here can grow
        with traffic. The collector is a snapshot read -- allocation and
        GC counters, the thread count, and one mallinfo2 call -- and never
        walks the heap. Where mallinfo2 is unavailable (musl, macOS, glibc
        before 2.33, or PRISM_MALLOC_TELEMETRY=0) the availability gauge
        renders 0 and the four byte gauges render -1, never 0.
        """
        sample = self._process_telemetry.sample()
        return [
            "# HELP qbit_prism_process_allocated_blocks Memory blocks currently allocated by the CPython allocator (sys.getallocatedblocks); a live-object proxy that rises with retention and stays flat under pure allocator fragmentation.",
            "# TYPE qbit_prism_process_allocated_blocks gauge",
            f"qbit_prism_process_allocated_blocks {int(sample.allocated_blocks)}",
            "# HELP qbit_prism_process_gc_trigger_count gc.get_count(): the cyclic collector's per-generation trigger counters, compared against gc.get_threshold() to decide when to collect. Generation 0 is allocations minus deallocations, NOT a count of retained objects. CPython 3.13 made the collector incremental, so the third entry is unused and reads 0; the series is kept for a fixed family set.",
            "# TYPE qbit_prism_process_gc_trigger_count gauge",
            *[
                f'qbit_prism_process_gc_trigger_count{{generation="{generation}"}} {int(count)}'
                for generation, count in zip(PRISM_GC_GENERATIONS, sample.gc_trigger_count)
            ],
            "# HELP qbit_prism_process_gc_collections_total Cyclic collector passes completed per generation since process start (gc.get_stats).",
            "# TYPE qbit_prism_process_gc_collections_total counter",
            *[
                f'qbit_prism_process_gc_collections_total{{generation="{generation}"}} {int(count)}'
                for generation, count in zip(PRISM_GC_GENERATIONS, sample.gc_collections)
            ],
            "# HELP qbit_prism_process_gc_collected_objects_total Objects the cyclic collector freed per generation since process start.",
            "# TYPE qbit_prism_process_gc_collected_objects_total counter",
            *[
                f'qbit_prism_process_gc_collected_objects_total{{generation="{generation}"}} {int(count)}'
                for generation, count in zip(PRISM_GC_GENERATIONS, sample.gc_collected)
            ],
            "# HELP qbit_prism_process_gc_uncollectable_objects_total Objects the cyclic collector found unreachable but could not free per generation since process start; growth is a leak the collector already knows about.",
            "# TYPE qbit_prism_process_gc_uncollectable_objects_total counter",
            *[
                f'qbit_prism_process_gc_uncollectable_objects_total{{generation="{generation}"}} {int(count)}'
                for generation, count in zip(PRISM_GC_GENERATIONS, sample.gc_uncollectable)
            ],
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

    def component_cardinality_metrics_lines(self) -> list[str]:
        """Issue #226 (and #185's gauge list): retained entries per component.

        Two families, each a closed ``component`` label set: entry counts and
        retained bytes over the state that plausibly scales with the payout
        window, the job population, or a candidate storm. The reads are
        deliberately direct attribute loads on the owning services rather
        than tolerant ``getattr`` defaults: a renamed owner field must fail
        the shipped-owner test loudly, never report a silent zero. The owners
        are the ones the renderer already constructs through
        ``_ensure_job_cache_state``; the reorg reconciler is read only when
        it exists, because the coordinator builds it lazily and it is the
        sole holder of its flights, so an absent service is a true zero.

        Every read is one ``len()``, ``qsize()``, or attribute load, atomic
        under the GIL, so no owner lock is taken beyond the coordinator lock
        the renderer already holds for the graveyard: a gauge that
        tolerates one entry of skew must never queue the scrape behind a
        daemon round trip or a payout-window walk.

        The one exception is the page-backed payout window, where records and
        canonical bytes are summed over the window's pages: O(pages), about
        120 iterations of two ``len()`` calls for a 63k-share window at the
        default page size, not O(shares). A mirror-backed window costs neither
        sum. This is a bounded per-scrape cost, not a heap walk.
        """
        port = self.port
        port._ensure_job_cache_state()
        bundles = port._ensure_job_bundle_service()
        payout = port._ensure_payout_state_service()
        writer = port._ensure_share_writer_service()
        compiler = port._ensure_bundle_compiler()
        candidates = port._ensure_block_candidate_service()
        candidates._ensure_block_replay_state()
        candidates._ensure_block_candidate_disposition_state()
        reconciler = getattr(port, "__dict__", {}).get("_reorg_reconciler_service")
        serialization = bundles._share_window_serialization
        cached_window = payout._incremental_payout_artifact_window
        artifact = payout._payout_ledger_artifact
        daemon_client = compiler._serve_builder
        window = cached_window.window if cached_window is not None else None
        window_pages = window_records = window_canonical_bytes = 0
        mirror_records = mirror_bytes = 0
        # Exactly one component accounts for the cached window, because these
        # gauges exist to attribute resident bytes and an operator summing the
        # family must not count the same object twice. The two backings are
        # mutually exclusive -- the mirror *is* the cached window on the Rust
        # path -- so a mirror-backed window reports zero pages, records, and
        # canonical bytes under payout_window_*, and its size appears only
        # under daemon_window_mirror_*. Which pair is non-zero is also how an
        # operator reads the backing off /metrics.
        if isinstance(window, DaemonShareWindowMirror):
            mirror_records = int(window.record_count)
            mirror_bytes = len(window.canonical_items)
        elif window is not None:
            pages = window.pages
            window_pages = len(pages)
            window_records = sum(len(page.records) for page in pages)
            window_canonical_bytes = sum(
                len(page.canonical_json_items) for page in pages
            )
        with port.lock:
            port._ensure_evicted_job_state()
            graveyard_entries = len(port.evicted_job_graveyard)
            same_tip_entries = len(port.evicted_same_tip_job_ids)
            outstanding_hashes = len(candidates._outstanding_block_candidate_hashes)
            tip_observed_hashes = len(candidates._tip_observed_accepted_block_hashes)
            counted_abandonments = len(candidates._counted_block_candidate_abandonments)
            ancestor_redrives = len(candidates._ancestor_redrive_records)
            terminal_outcomes = len(candidates._block_candidate_terminal_outcomes)
            waiting_retries = len(candidates._block_disposition_waiting_retries)
            dequeued_hashes = len(candidates._block_candidate_dequeued_hashes)
            accounted_hashes = len(port._accounted_accepted_block_hashes)
            trusted_memo = (
                len(reconciler._reorg_reconcile_trusted_memo)
                if reconciler is not None
                else 0
            )
        entries = {
            "payout_window_pages": window_pages,
            "payout_window_records": window_records,
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
            "job_contexts": len(port.jobs),
            "job_bundle_cache": len(bundles._job_bundle_cache),
            "job_build_issued_stamps": len(bundles._job_build_issued_at_ms),
            "bundle_preparation_flights": len(bundles._bundle_preparation_flights),
            "active_job_bundle_builds": len(bundles._active_job_bundle_builds),
            "evicted_job_graveyard": graveyard_entries,
            "evicted_same_tip_job_ids": same_tip_entries,
            "block_candidate_queue": candidates.candidate_queue.qsize(),
            "block_replay_queue": candidates._block_replay_candidate_queue.qsize(),
            "block_replay_inflight_hashes": len(candidates._block_replay_inflight_hashes),
            "block_quarantine_queue": candidates._block_quarantine_queue.qsize(),
            "block_quarantine_hashes": len(candidates._block_quarantine_hashes),
            "outstanding_block_candidate_hashes": outstanding_hashes,
            "tip_observed_accepted_block_hashes": tip_observed_hashes,
            "counted_block_candidate_abandonments": counted_abandonments,
            "accepted_block_preview_stamps": len(
                candidates._accepted_block_preview_acceptance_monotonic
            ),
            "ancestor_redrive_records": ancestor_redrives,
            "block_candidate_terminal_outcomes": terminal_outcomes,
            "block_candidate_disposition_flights": len(
                candidates._block_candidate_disposition_flights
            ),
            "block_disposition_waiting_retries": waiting_retries,
            "block_candidate_dequeued_hashes": dequeued_hashes,
            "accounted_accepted_block_hashes": accounted_hashes,
            "reconcile_flights": (
                len(reconciler._reconcile_flights) if reconciler is not None else 0
            ),
            "reconcile_trusted_memo": trusted_memo,
            "pending_share_commit_floor": len(writer._pending_share_commit_floor),
            "daemon_window_mirror_records": mirror_records,
            "daemon_uploaded_windows": (
                len(daemon_client.uploaded_windows) if daemon_client is not None else 0
            ),
        }
        byte_sizes = {
            "payout_window_canonical_json": window_canonical_bytes,
            "daemon_window_mirror_canonical_items": mirror_bytes,
            "share_window_serialization_spool": (
                int(serialization._spool_size) if serialization is not None else 0
            ),
            # json.dumps with its ASCII default, so characters are bytes.
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

    def landing_observability_metrics_lines(self) -> list[str]:
        """Landing-path metrics for issue #188's pre-deadline alerting.

        The prior-balances read crossed the one-second submitter deadline
        silently over several weeks; these series make that growth, landing
        timeouts, and unresolved accepted-parent age visible before they
        become an outage.
        """
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
            self.port.block_ledger_call_class_metrics().items()
        ):
            label = self.port.prometheus_label_value(call_class)
            lines.extend(
                [
                    f'qbit_prism_block_ledger_calls_total{{call_class="{label}"}} {int(stats["calls_total"])}',
                    f'qbit_prism_block_ledger_call_timeouts_total{{call_class="{label}"}} {int(stats["timeouts_total"])}',
                    f'qbit_prism_block_ledger_call_budget_seconds{{call_class="{label}"}} {float(stats["last_budget_seconds"]):.6f}',
                    f'qbit_prism_block_ledger_call_last_duration_seconds{{call_class="{label}"}} {float(stats["last_duration_seconds"]):.6f}',
                    f'qbit_prism_block_ledger_call_max_duration_seconds{{call_class="{label}"}} {float(stats["max_duration_seconds"]):.6f}',
                ]
            )
        # The depth gauge must equal what the admission fence counts. The
        # fence counts every landed transition; the age list can only report
        # the ones carrying a monotonic landing stamp. A degraded preview
        # publication and a replayed durable candidate both arm ``landed``
        # without that stamp, so deriving depth from the ages would read zero
        # while the fence already refuses admission at depth one.
        unresolved_depth = self.port._accepted_parent_unresolved_depth()
        unresolved_depth_cap = self.port._accepted_parent_unresolved_depth_cap()
        unresolved_ages = self.port.accepted_parent_unresolved_ages_seconds()
        oldest_unresolved = max(unresolved_ages) if unresolved_ages else -1.0
        with self.port.lock:
            preview_wait_timeouts = int(
                getattr(self.port, "_accepted_parent_preview_wait_timeouts", 0)
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
                f"{int(getattr(self.port, 'accepted_parent_redrive_attempt_count', 0))}",
                "# HELP qbit_prism_accepted_parent_redrive_resolved_total Pending accepted-parent transitions that resolved after at least one in-process re-drive attempt.",
                "# TYPE qbit_prism_accepted_parent_redrive_resolved_total counter",
                "qbit_prism_accepted_parent_redrive_resolved_total "
                f"{int(getattr(self.port, 'accepted_parent_redrive_resolved_count', 0))}",
                "# HELP qbit_prism_accepted_parent_redrive_exhausted_total Ancestors whose re-drive attempt cap was exhausted with the transition still pending; the publication-progress watchdog remains the backstop.",
                "# TYPE qbit_prism_accepted_parent_redrive_exhausted_total counter",
                "qbit_prism_accepted_parent_redrive_exhausted_total "
                f"{int(getattr(self.port, 'accepted_parent_redrive_exhausted_count', 0))}",
            ]
        )
        prior_stats_fn = getattr(self.port.ledger, "prior_balances_read_stats", None)
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
        lines.extend(self._ledger_read_gate_metric_lines())
        startup_phases = self.port.startup_phase_seconds()
        if startup_phases:
            lines.extend(
                [
                    "# HELP qbit_prism_startup_phase_seconds Seconds from serve() start to each startup phase, recorded once.",
                    "# TYPE qbit_prism_startup_phase_seconds gauge",
                ]
            )
            for phase, seconds in sorted(startup_phases.items()):
                label = self.port.prometheus_label_value(phase)
                lines.append(
                    f'qbit_prism_startup_phase_seconds{{phase="{label}"}} {float(seconds):.6f}'
                )
        return lines

    def _ledger_read_gate_metric_lines(self) -> list[str]:
        """Split read-slot admission wait from SQL execution, per operation.

        A single duration per ledger call cannot say whether a budget went to
        coordinator-local admission or to PostgreSQL, and issue #211 turned on
        exactly that distinction: the replay enumeration reported
        ``exceeded 5s`` while its statement deadline was barely touched and
        the database showed no blocked backends. These two families answer it
        from a scrape -- ``..._gate_wait_seconds_*`` is contention inside this
        process, ``..._execute_seconds_*`` is the server (a cancelled
        statement's return tail included). The timeout counters are split the
        same way, so an admission expiry and a statement expiry never land on
        one series.
        """
        stats_fn = getattr(self.port.ledger, "ledger_read_gate_stats", None)
        if not callable(stats_fn):
            return []
        # Issue #224 Wave 0: the operation label is bounded by
        # PRISM_LEDGER_READ_OPERATIONS. Names outside the contract fold
        # into ``other`` (summed) instead of opening a series; members
        # pass through untouched, so the shipped operation renders
        # byte-for-byte as before.
        stats_by_operation = fold_ledger_read_stats(stats_fn() or {})
        if not stats_by_operation:
            return []
        # Samples stay grouped under their own family: the exposition format
        # wants every line of a metric emitted as one block, so the loop is
        # over families and the operations are the inner dimension.
        families: tuple[tuple[str, str, str, str, bool], ...] = (
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
            (self.port.prometheus_label_value(str(operation)), stats)
            for operation, stats in sorted(stats_by_operation.items())
        ]
        lines: list[str] = []
        for name, metric_type, help_text, field, integral in families:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {metric_type}")
            for label, stats in operations:
                value = stats.get(field, 0)
                rendered = (
                    f"{int(value)}" if integral else f"{float(value):.6f}"
                )
                lines.append(f'{name}{{operation="{label}"}} {rendered}')
        return lines

    @staticmethod
    def _duration_summary_lines(
        name: str,
        labels: str,
        stats: dict[str, Any],
    ) -> list[str]:
        """``_sum`` / ``_count`` / ``_max`` for one labelled cell."""
        return [
            f'{name}_sum{{{labels}}} {float(stats["sum"]):.6f}',
            f'{name}_count{{{labels}}} {int(stats["count"])}',
            f'{name}_max{{{labels}}} {float(stats["max"]):.6f}',
        ]

    def accepted_preview_attribution_metrics_lines(self) -> list[str]:
        """Issue #224 Wave 0: the shared acceptance-to-preview attribution.

        Four summary families, one owner snapshot per render, every label
        drawn from a closed vocabulary in
        ``lab.prism.accepted_preview_telemetry`` and every cell of every
        product rendered from zero. The export is therefore constant-size
        regardless of block cadence and carries no hash or height; the
        per-publication record that may carry those is the owner's
        diagnostics ring, which is deliberately not a metric.
        """
        snapshot = ensure_accepted_preview_telemetry(self.port).snapshot()
        landing = snapshot["landing_phases"]
        passes = snapshot["reconcile_passes"]
        steps = snapshot["reconcile_steps"]
        rescans = snapshot["full_rescans"]
        landing_name = "qbit_prism_accepted_block_landing_phase_seconds"
        pass_name = "qbit_prism_reorg_reconcile_pass_seconds"
        step_name = "qbit_prism_reorg_reconcile_step_seconds"
        rescan_name = "qbit_prism_payout_window_full_rescan_seconds"
        lines = [
            f"# HELP {landing_name} Accepted-block landing wall time between definitive node acceptance and payout preview publication, by bounded sub-phase; prior_balances_check is a prior-balances reread, never a payout-window rescan.",
            f"# TYPE {landing_name} summary",
        ]
        for phase in PRISM_ACCEPTED_LANDING_PHASES:
            lines.extend(
                self._duration_summary_lines(
                    landing_name, f'phase="{phase}"', landing[phase]
                )
            )
        lines.extend(
            [
                f"# HELP {pass_name} Reorg reconciliation passes by bounded caller; _count is the pass count, and a pass minus the sum of its steps is the unattributed remainder.",
                f"# TYPE {pass_name} summary",
            ]
        )
        for caller in PRISM_REORG_RECONCILE_CALLERS:
            lines.extend(
                self._duration_summary_lines(
                    pass_name, f'caller="{caller}"', passes[caller]
                )
            )
        lines.extend(
            [
                f"# HELP {step_name} Reorg reconciliation wall time by bounded caller and step; admission_wait is writer admission plus the payout-balance mutation lock, and candidate_prepare is where a reconcile_invalidation full rescan executes.",
                f"# TYPE {step_name} summary",
            ]
        )
        for caller in PRISM_REORG_RECONCILE_CALLERS:
            for step in PRISM_REORG_RECONCILE_STEPS:
                lines.extend(
                    self._duration_summary_lines(
                        step_name,
                        f'caller="{caller}",step="{step}"',
                        steps[(caller, step)],
                    )
                )
        lines.extend(
            [
                f"# HELP {rescan_name} Full payout-window oracle rescans by bounded reason and window pipeline path; a prior-balances reread is a ledger read operation, not an observation here.",
                f"# TYPE {rescan_name} summary",
            ]
        )
        for reason in PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS:
            for path in PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS:
                lines.extend(
                    self._duration_summary_lines(
                        rescan_name,
                        f'reason="{reason}",path="{path}"',
                        rescans[(reason, path)],
                    )
                )
        return lines

    def accepted_stats_reconcile_metric_lines(self) -> list[str]:
        """Surface reconcile liveness now that failures no longer raise.

        Serving maintained counters means a failing or wedged reconcile is
        invisible to callers, so its age and failure count must be visible
        to scrapes for alerting instead.
        """
        status_fn = getattr(self.port.ledger, "accepted_stats_reconcile_status", None)
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

    def lease_heartbeat_metrics_lines(self) -> list[str]:
        """Writer-lease heartbeat phase attribution and its timing policy.

        Issue #212 was diagnosable only from exit messages after the fact:
        nothing exported where a heartbeat's wall-clock went, or how much
        envelope the shipped policy actually leaves. Every series here has
        a fixed label vocabulary (modes, outcomes, phases and policy terms
        are closed sets in ``lab.prism.writer_lease_timing``), so scrape
        cardinality is constant regardless of traffic or restarts.
        """
        snapshot = self.port._ensure_lease_heartbeat_service().snapshot()
        attempts = snapshot["attempts"]
        outcomes = snapshot["outcomes"]
        last_phases = snapshot["last_phase_seconds"]
        worst_phases = snapshot["worst_phase_seconds"]
        policy_seconds = snapshot["policy_seconds"]
        assert isinstance(attempts, dict)
        assert isinstance(outcomes, dict)
        assert isinstance(last_phases, dict)
        assert isinstance(worst_phases, dict)
        assert isinstance(policy_seconds, dict)
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
        ]

    def shutdown_metrics_lines(self) -> list[str]:
        snapshot = self.port._ensure_shutdown_controller().snapshot()
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
                f'qbit_prism_shutdown_writer_operations{{component="{self.port.prometheus_label_value(str(component))}"}} {int(count)}'
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

    def initial_delivery_metrics_lines(self) -> list[str]:
        self.port._ensure_initial_job_state()
        mining = self.port.mining_delivery_snapshot()
        with self.port.lock:
            counts = {
                "sent": self.port.initial_job_sent_count,
                "cancelled": self.port.initial_job_cancelled_count,
                "coalesced": self.port.initial_job_coalesced_count,
                "failed": self.port.initial_job_failed_count,
                "superseded": self.port.initial_job_superseded_count,
            }
            latency_sum = self.port.initial_job_delivery_latency_seconds_sum
            latency_count = self.port.initial_job_delivery_latency_count
            queue_capacity_reclaimed = (
                self.port.initial_job_queue_capacity_reclaimed_count
            )
        executor = getattr(self.port, "_initial_job_executor", None)
        queued, slots = executor.stats() if executor is not None else (0, 0)
        configured_workers = int(
            getattr(
                self.port,
                "initial_job_max_workers",
                DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
            )
        )
        with self.port._bundle_preparation_lock:
            build_counts = dict(self.port.shared_bundle_build_counts)
            preparation_sum = self.port.shared_bundle_preparation_seconds_sum
            preparation_count = self.port.shared_bundle_preparation_count
            waiters = self.port.shared_bundle_preparation_waiters
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

    def job_build_metrics_lines(self) -> list[str]:
        self.port._ensure_job_cache_state()
        with self.port._job_cache_lock:
            bucket_counts = dict(self.port.job_build_seconds_bucket_counts)
            build_sum = self.port.job_build_seconds_sum
            build_count = self.port.job_build_count
            phase_seconds = dict(self.port.job_build_phase_seconds)
            hit_counts = dict(self.port.job_cache_hit_counts)
            miss_counts = dict(self.port.job_cache_miss_counts)
            # Health-refresh failure counts are owner state; read them from
            # the observability service directly (no coordinator property).
            health_refresh_failures = (
                self.port._ensure_observability_service()
                .state()
                .health_snapshot_refresh_failure_count
            )
        with self.port._job_build_scheduler_lock:
            scheduler_counts = dict(self.port.job_build_scheduler_counts)
            priority_counts = dict(self.port.job_build_priority_counts)
            priority_admission_seconds = dict(
                self.port.job_build_priority_admission_seconds
            )
            initial_prepared_counts = dict(
                self.port.initial_job_prepared_work_counts
            )
            cancellation_seconds = dict(self.port.job_build_cancellation_seconds)
            replacement_seconds = dict(
                self.port.job_build_replacement_start_seconds
            )
            worker_counts = dict(self.port.job_build_worker_counts)
            active_builds = int(self.port._job_build_active is not None)
            pending_builds = int(self.port._job_build_pending is not None)
            priority_requests = tuple(
                request
                for request in (
                    (
                        self.port._job_build_active.request
                        if self.port._job_build_active is not None
                        else None
                    ),
                    (
                        self.port._job_build_retiring.request
                        if self.port._job_build_retiring is not None
                        else None
                    ),
                    self.port._job_build_pending,
                )
                if request is not None
                and not request.cancellation.is_set()
                and self.port._job_build_is_publication_critical(request)
            )
            priority_preparations = tuple(
                self.port._job_build_priority_preparations.values()
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
        lock = getattr(self.port, "lock", None)
        if lock is not None:
            with lock:
                connected_clients = len(getattr(self.port, "clients", ()))
        else:
            connected_clients = len(getattr(self.port, "clients", ()))
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
                    f"{int(getattr(self.port, 'job_build_executor_workers', DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS))}"
                ),
            ]
        )
        return lines
