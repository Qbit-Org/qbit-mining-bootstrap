#!/usr/bin/env python3
"""PRISM metrics renderer extraction tests.

The frozen reference implementations below are byte-copied from the
coordinator formatter bodies as they stood immediately before the PR 80
metrics extraction (``self`` renamed to ``server``).  The full-render parity
test proves the extracted ``MetricsRenderer`` reproduces the pre-extraction
Prometheus document byte-for-byte for identical inputs.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from lab.prism.coordinator_config import (
    DEFAULT_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX,
    DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
    DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS,
)
from lab.prism.block_candidates import (
    PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS,
    PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS,
    PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES,
    PRISM_STALE_JOB_ABANDON_CLASSES,
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
from lab.prism.metrics import MetricsRenderer
from lab.prism.payout_state import AcceptedBlockPayoutTransition
from lab.prism.prism_coordinator import PRISM_REJECTION_REASON_IDS
from lab.prism.reorg_reconciler import (
    PRISM_REORG_RECONCILE_LOOKUP_PATHS,
    PRISM_REORG_RECONCILE_LOOKUP_SOURCES,
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
)
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
        "# HELP qbit_prism_stratum_semantic_current_work_ratio Ratio of authorized clients whose work matches the current template fingerprint and payout generation.",
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
            'qbit_prism_lease_heartbeat_policy_seconds{term="server_proven_cap"}',
        ):
            self.assertIn(needle, actual)

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


if __name__ == "__main__":
    unittest.main()
