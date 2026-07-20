#!/usr/bin/env python3
"""Minimal live direct qbit Stratum coordinator for PRISM regtest proof."""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import copy
import hashlib
import json
import os
import queue
import shlex
import signal
import socket
import subprocess
import threading
import time
import traceback
from dataclasses import replace as dataclass_replace
from decimal import Decimal, ROUND_CEILING
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, MutableMapping, Sequence

import sys

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.auxpow import stratum_codec, vardiff
from lab.prism import direct_stratum, public_api
# Compatibility re-exports; new callers should import lab.prism.background_services.
from lab.prism.background_services import (
    BackgroundServiceRegistry,  # noqa: F401 - compatibility re-export
    BackgroundServiceSpec,  # noqa: F401 - compatibility re-export
    WatchdogPorts,
    WatchdogService,
)
# Compatibility re-exports; new callers should import lab.prism.bounded_executor.
from lab.prism.bounded_executor import (
    _BoundedPriorityExecutor,  # noqa: F401 - compatibility re-export
    _DeliveryQueueFull,  # noqa: F401 - compatibility re-export
)
from lab.prism.audit_artifacts import (
    AuditArtifactConfig,
    AuditArtifactStore,
    AuditPublicationIdentity,
)
from lab.prism.audit_http import (
    AuditHttpConfig,
    AuditHttpFacade,
    AuditHttpPort,
)
from lab.prism.bundle_compiler import canonical_bundle_bytes
from lab.prism.block_candidates import (
    DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS,
    DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS,
    MAX_PENDING_BLOCK_CANDIDATES,
    BlockCandidateCompatibilityField,
    BlockCandidatePorts,
    BlockCandidateService,
    PrismBlockCandidate,
    block_candidate_from_intent as decode_block_candidate_intent,
    block_candidate_intent as encode_block_candidate_intent,
    compatibility_default as candidate_compatibility_default,
)
from lab.prism.block_finalization import (
    PRISM_REJECTION_BLOCK_STALE,
    PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
    PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
    PRISM_REJECTION_SUBMITBLOCK_REJECTED,
    BlockFinalizationService,
)
from lab.prism.share_submission import (
    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
    PRISM_REJECTION_DUPLICATE_SHARE,
    PRISM_REJECTION_INVALID_EXTRANONCE,
    PRISM_REJECTION_INVALID_NTIME_OR_NONCE,
    PRISM_REJECTION_LOW_DIFFICULTY,
    PRISM_REJECTION_MALFORMED_SUBMIT,
    PRISM_REJECTION_POOL_CLOSED,
    PRISM_REJECTION_STALE_JOB,
    PRISM_REJECTION_UNAUTHORIZED_WORKER,
    PRISM_REJECTION_UNKNOWN_JOB,
    RecentShareCompatibilityField,
    RecentShareIndex,
    ShareSubmissionPorts,
    ShareSubmissionService,
    SubmitControlSnapshot,
)
from lab.prism.ctv_broadcaster import CtvFanoutBroadcaster
from lab.prism.coordinator_config import (
    CoordinatorConfig,
    DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
    DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS,
    DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION,
    DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS,
    DEFAULT_PRISM_BLOCKPOLL_SECONDS,
    DEFAULT_PRISM_BLOCKWAIT_TIMEOUT_SECONDS,
    DEFAULT_PRISM_TIP_REFRESH_FAILURE_HOLDOFF_SECONDS,
    DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
    DEFAULT_PRISM_CTV_BROADCASTER_CHUNK_SIZE,
    DEFAULT_PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS,
    DEFAULT_PRISM_HEALTH_REFRESH_SECONDS,
    DEFAULT_PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS,
    DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS,
    DEFAULT_PRISM_JOB_BUILD_TIMEOUT_SECONDS,
    DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS,
    DEFAULT_PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS,
    DEFAULT_PRISM_METRICS_REFRESH_SECONDS,
    DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES,
    DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS,
    DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
    DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION,
    DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
    DEFAULT_PRISM_STALE_GRACE_SECONDS,
    DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS,
    DEFAULT_PRISM_STRATUM_BIND_RETRY_SECONDS,
    DEFAULT_PRISM_STRATUM_LISTEN_BACKLOG,
    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME,
    DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS,
    DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS,
    DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS,
    DEFAULT_PRISM_SUBMIT_TIP_MAX_AGE_SECONDS,
    DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
    DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS,
    DEFAULT_PRISM_WORKER_METRICS_LIMIT,
    DEFAULT_PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS,
    DEFAULT_SHARE_COMMIT_BATCH_SIZE,  # noqa: F401 - compatibility re-export
    DEFAULT_SHARE_COMMIT_LINGER_MILLISECONDS,  # noqa: F401 - compatibility re-export
    DEFAULT_SHARE_COMMIT_TIMEOUT_SECONDS,  # noqa: F401 - compatibility re-export
    StratumListenerProfile,
    TESTNET_QBIT_CHAINS,
    default_prism_payout_policy,
    default_prism_username_fallback_address,
    env,
    env_bool,
    env_int,
    env_nonnegative_float,
    env_nonnegative_int,
    env_optional,
    env_optional_positive_int_with_legacy,
    env_positive_float,
    env_positive_int,
    env_positive_int_with_legacy,
    load_coordinator_config,
    load_share_weights,
    validate_hex,
)
# Compatibility re-exports; session callers should import the owning module.
from lab.prism.stratum_session import (
    ClientState,
    JobDeliveryPort,
    P2mrAddressValidator,
    ProgressHealthPort,
    SessionRegistry,
    SessionRuntimePort,
    StratumError,
    StratumSessionService,
    WorkerIdentity,
    apply_stratum_send_timeout as apply_socket_send_timeout,
    client_vardiff_lock,
    difficulty_payload as stratum_difficulty_payload,
    error_payload as stratum_error_payload,
    job_payload as stratum_job_payload,
    result_payload as stratum_result_payload,
    stratum_accept_heartbeat_names as configured_accept_heartbeat_names,
)
from lab.prism.ctv_broadcaster_daemon import (
    CtvFanoutBroadcastDaemon,
    CtvFanoutChunkResult,
    CtvFanoutDaemonResult,
)
# Compatibility re-exports; new callers should import lab.prism.ctv_runtime.
from lab.prism.ctv_runtime import (
    CtvRuntimeConfig,
    CtvRuntimeService,
)
from lab.prism.rpc import JsonRpc
# Compatibility re-exports; new callers should import the owning J1 modules.
from lab.prism.bundle_compiler import BundleCompiler, BundleCompilerPorts
from lab.prism.job_bundle import (
    CachedJobBundle,
    JobBuildCancellation as _JobBuildCancellation,
    JobBuildFlight as _JobBuildFlight,
    JobBuildRequest as _JobBuildRequest,
    JobBuildSuperseded,
    JobBundleBuildControl as _JobBundleBuildControl,
    JobBundleBuildSuperseded as _JobBundleBuildSuperseded,
    JobBundleConfig,
    JobBundlePorts,
    JobBundleService,
)
# Compatibility re-exports; new callers should import lab.prism.job_delivery.
from lab.prism.job_delivery import (
    AdmittedIdleBundleSource,
    DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
    DeliveryCompatibilityHooks,
    EvictedJobEntry,
    IdleDeliveryAuthority,
    InitialJobConfig,
    InitialJobState,
    InitialJobRuntimePort,
    JobPreparationPort,
    JobDeliveryRuntime,
    JobDeliveryService,
    JobDeliveryTipRefreshPort,
    PRISM_CREDIT_POLICY_STALE_GRACE,
    PRISM_EVICTED_JOB_CAPACITY_SCOPES,
    PRISM_EVICTED_JOB_CLASSES,
    PRISM_EVICTED_JOB_SUBMIT_OUTCOMES,
    PendingInitialJob,
    PayoutDeliveryPort,
    PrismJobContext,
    ProgressDeliveryPort,
    RetentionAuthority,
    RetainedJobIndex,
    TipAuthorityPort,
)
from lab.prism.template_artifacts import (
    CachedTemplateArtifacts,
    QbitTipTemplateSnapshot,
    TemplateArtifactEventSink,
    TemplateArtifactPorts,
    TemplateArtifactRepository,
)
# Compatibility re-exports; new callers should import lab.prism.tip_refresh.
from lab.prism.tip_refresh import (
    FanoutCancellation as _FanoutCancellation,
    PRISM_TIP_REFRESH_BUILD_PHASES,
    PRISM_TIP_REFRESH_SECONDS_BUCKETS,
    RefreshResult,
    TipRefreshConfig,
    TipRefreshPorts,
    TipRefreshService,
    TipRefreshValidationToken,
)
from lab.prism.vardiff_service import (
    PRISM_VARDIFF_IDLE_SECONDS_BUCKETS,
    PRISM_VARDIFF_IDLE_SKIP_REASONS,
    VARDIFF_COMPATIBILITY_FIELDS,
    IdleRetargetRequest as _IdleRetargetRequest,
    VardiffCompatibilityField,
    VardiffService,
)
# Compatibility re-exports; new callers should import lab.prism.progress_health.
from lab.prism.progress_health import (
    DeliveryProof,
    EligibilitySnapshot,
    PROGRESS_HEALTH_REASONS,
    ProgressHealthConfig,
    ProgressHealthService,
    ProgressHealthSnapshot,
    WorkGeneration,
)
from lab.prism.observability import (
    MiningDeliveryInputs,
    ObservabilityPort,
    ObservabilityService,
)
from lab.prism.metrics import MetricsRenderer
from lab.prism.reorg_reconciler import (
    ReorgCompatibilityField,
    ReorgPorts,
    ReorgReconcilerService,
    qbit_chain_view_untrusted as reorg_chain_view_untrusted,
)
# Compatibility re-exports; new callers should import lab.prism.payout_state.
from lab.prism.payout_state import (
    PayoutLedgerArtifact,
    PayoutStateArtifact,
    PayoutStateCandidate,
    PayoutStateConfig,
    PayoutStatePorts,
    PayoutStateService,
    TemplateRefreshBlocked,
    TemplateRefreshSuperseded,
)
# Compatibility re-exports; new callers should import lab.prism.coordinator_shutdown.
from lab.prism.coordinator_shutdown import (
    CoordinatorShutdownController,  # noqa: F401 - compatibility re-export
    ShutdownInProgress,  # noqa: F401 - compatibility re-export
    ledger_writer_operation,  # noqa: F401 - compatibility re-export
)
# Compatibility re-exports; new callers should import lab.prism.share_writer.
from lab.prism.share_writer import (
    MAX_PENDING_SHARE_APPENDS,
    PENDING_SHARE_COMMIT_WARN_SECONDS as PRISM_PENDING_SHARE_COMMIT_WARN_SECONDS,
    PendingShareAppend,
    PendingShareInput,
    ShareWriter,
    ShareWriterCompatibilityField,
    ShareWriterConfig,
    ShareWriterError,
    ShareWriterPorts,
    ShareWriterQueueFull,
)
from lab.prism.share_ledger import (
    DEFAULT_AUDIT_SHARE_SEGMENT_SIZE,
    DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
    DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
    PendingShare,
    PsqlShareLedger,
    SingleWriterShareLedger,
)

MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES = 128
PRISM_JOB_BUILD_EXECUTOR_WORKERS = 2
# Block candidates queue to a dedicated submitter thread so the miner's share
# ack never waits on audit/submitblock after the share and intent commit. The
# bound limits RAM; overflow only coalesces a wakeup because Postgres retains
# the authoritative pending candidate.
# The reward window is 8x network difficulty (must match PRISM_WINDOW_MULTIPLIER
# in crates/qbit-prism/src/lib.rs and the SQL). The job-build snapshot only needs
# the shares that window can cover; requesting a margin above it returns a
# guaranteed superset (the audit bundle re-selects the exact 8x window, so the
# digest is unchanged) while keeping the query O(window), not O(ledger history).
PRISM_REWARD_WINDOW_MULTIPLIER = 8
PRISM_SNAPSHOT_WINDOW_MARGIN = 2
# Evicted jobs remain tied to their immutable validation context. Current-tip
# entries use an independent bounded TTL; once their tip is replaced, only the
# existing stale-grace lifetime and eligibility rules can retain/credit them.
# Extranonce1 placeholder used for the shared per-template job build. The
# stratum coinbase split cuts the whole extranonce window (extranonce1 +
# zeroed extranonce2) out of coinb1/coinb2, so the placeholder value never
# reaches miners; real connections stamp their own extranonce1 into the job.
# Client extranonce1 values start at 1, so the placeholder never collides.
PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX = "00000000"
PRISM_BUILDER_PHASE_METRICS_PREFIX = "qbit-prism-build-phase-metrics "
PRISM_PAYOUT_DELIVERY_GENERATIONS = ("current", "stale", "future")
DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES = 8
PRISM_PROGRESS_HEALTH_REASONS = PROGRESS_HEALTH_REASONS
PRISM_REJECTION_INTERNAL_ERROR = "internal-error"
PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS = frozenset(
    {
        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
        PRISM_REJECTION_INTERNAL_ERROR,
        PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
        PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
    }
)
# Used only by lightweight embedders that bypass dataclass/coordinator
# construction. Production instances install these locks eagerly in __init__.
# Serializing the fallback prevents concurrent first-touch callers from ever
# publishing different lock objects for the same state.
_HOT_PATH_LOCK_INITIALIZATION_LOCK = threading.Lock()
DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS = 5.0
# Credit policies recorded on accepted ledger rows. Normal shares carry no
# policy; a policy marks a share that was credited by an explicit pool rule
# (documented in docs/prism-rejections.md) so audits can distinguish them.
# Aggregation bucket for per-worker share metrics once the distinct-worker
# label budget is exhausted.
PRISM_WORKER_METRICS_OVERFLOW_LABEL = "_other"
PRISM_REJECTION_REASON_IDS = (
    PRISM_REJECTION_STALE_JOB,
    PRISM_REJECTION_DUPLICATE_SHARE,
    PRISM_REJECTION_LOW_DIFFICULTY,
    PRISM_REJECTION_MALFORMED_SUBMIT,
    PRISM_REJECTION_UNAUTHORIZED_WORKER,
    PRISM_REJECTION_UNKNOWN_JOB,
    PRISM_REJECTION_INVALID_EXTRANONCE,
    PRISM_REJECTION_INVALID_NTIME_OR_NONCE,
    PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
    PRISM_REJECTION_SUBMITBLOCK_REJECTED,
    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
    PRISM_REJECTION_INTERNAL_ERROR,
    PRISM_REJECTION_POOL_CLOSED,
    PRISM_REJECTION_BLOCK_STALE,
    PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
)


class _ObservedRLock:
    """RLock with zero shared-metrics work on uncontended acquisitions.

    The coordinator lock protects control-plane publication state. Its
    contention counters intentionally record only acquisitions that fail an
    immediate probe, so observing it cannot recreate the share-path convoy the
    metrics are meant to diagnose.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._metrics_lock = threading.Lock()
        self._contention_count = 0
        self._wait_seconds_sum = 0.0
        self._wait_seconds_max = 0.0

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if not blocking:
            return self._lock.acquire(blocking=False)
        if self._lock.acquire(blocking=False):
            return True
        started = time.monotonic()
        acquired = self._lock.acquire(blocking=True, timeout=timeout)
        waited = max(0.0, time.monotonic() - started)
        with self._metrics_lock:
            self._contention_count += 1
            self._wait_seconds_sum += waited
            self._wait_seconds_max = max(self._wait_seconds_max, waited)
        return acquired

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> _ObservedRLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()

    def _is_owned(self) -> bool:
        return self._lock._is_owned()  # type: ignore[attr-defined]

    def contention_snapshot(self) -> tuple[int, float, float]:
        with self._metrics_lock:
            return (
                self._contention_count,
                self._wait_seconds_sum,
                self._wait_seconds_max,
            )
PRISM_TEMPLATE_FINGERPRINT_VOLATILE_KEYS = frozenset(
    {
        # qbit can legitimately advance these without making already issued
        # jobs stale. Rebuilding every miner job for clock-only changes would
        # turn the poller into continuous audit-bundle churn.
        "curtime",
        "longpollid",
        "mintime",
    }
)


def now_ms() -> int:
    return int(time.time() * 1000)


def qbit_gbt_rules(chain: str) -> list[str]:
    rules = ["segwit"]
    if chain.strip().lower() == "signet":
        rules.append("signet")
    return rules


def qbit_template_fingerprint(template: dict[str, Any]) -> str:
    stable_template = {
        key: value
        for key, value in template.items()
        if key not in PRISM_TEMPLATE_FINGERPRINT_VOLATILE_KEYS
    }
    encoded = json.dumps(
        stable_template,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def canonical_json_text(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def canonical_json_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_text(value).encode()).hexdigest()


class _BundlePreparationSuperseded(TemplateRefreshSuperseded):
    """The exact work identity lost to a newer tip/template observation.

    Subclasses TemplateRefreshSuperseded: losing the shared-bundle build race
    to a newer tip/template observation is coordination churn, so it escapes
    the poll without arming the template-refresh failure budget.
    """


class _CoordinatorSessionRuntime(SessionRuntimePort):
    """Dynamic compatibility adapter for the extracted session service."""

    def __init__(self, coordinator: PrismCoordinator) -> None:
        self.coordinator = coordinator

    def running(self) -> bool:
        coordinator = self.coordinator
        stop_event = getattr(coordinator, "stop_event", None)
        if stop_event is not None and stop_event.is_set():
            return False
        return coordinator._ensure_shutdown_controller().phase == "running"

    def record_heartbeat(self, name: str) -> None:
        self.coordinator._record_heartbeat(name)

    def wait_after_resource_failure(self, heartbeat_name: str) -> None:
        coordinator = self.coordinator
        remaining_seconds = max(
            0.0,
            float(
                getattr(
                    coordinator,
                    "stratum_accept_resource_exhaustion_backoff_seconds",
                    DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS,
                )
            ),
        )
        watchdog_timeout_seconds = max(
            0.001, float(getattr(coordinator, "watchdog_timeout_seconds", 120.0))
        )
        heartbeat_interval_seconds = max(
            0.001, min(1.0, watchdog_timeout_seconds / 2.0)
        )
        deadline = time.monotonic() + remaining_seconds
        while not coordinator.stop_event.is_set():
            coordinator._record_heartbeat(heartbeat_name)
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                return
            if coordinator.stop_event.wait(
                min(remaining_seconds, heartbeat_interval_seconds)
            ):
                return

    def record_resource_exhaustion(
        self,
        *,
        listener_name: str,
        location: str,
        error_number: int | None,
    ) -> None:
        coordinator = self.coordinator
        with coordinator.lock:
            coordinator.accept_resource_exhaustion_count = int(
                getattr(coordinator, "accept_resource_exhaustion_count", 0)
            ) + 1
            exhaustion_count = coordinator.accept_resource_exhaustion_count
        if exhaustion_count == 1 or exhaustion_count % 100 == 0:
            print(
                "prism coordinator: stratum resource exhaustion "
                f"listener={listener_name} location={location} errno={error_number} "
                f"count={exhaustion_count}",
                flush=True,
            )

    def record_setup_failure(self) -> int:
        coordinator = self.coordinator
        with coordinator.lock:
            coordinator.connection_setup_failure_count = int(
                getattr(coordinator, "connection_setup_failure_count", 0)
            ) + 1
            return coordinator.connection_setup_failure_count

    def sync_registry_metrics(self, registry: SessionRegistry) -> None:
        coordinator = self.coordinator
        coordinator.clients = registry.clients
        coordinator.connection_counter = registry.connection_generation
        coordinator.connection_limit_rejection_counts = registry.rejection_counts
        coordinator.peak_active_connection_count = registry.peak_active_connections
        coordinator.handler_thread_count = registry.handler_thread_count

    def max_connections(self) -> int:
        return int(
            getattr(
                self.coordinator,
                "stratum_max_connections",
                DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
            )
        )

    def max_connections_per_username(self) -> int:
        return int(
            getattr(
                self.coordinator,
                "stratum_max_connections_per_username",
                DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME,
            )
        )

    def client_startup_difficulty(self, profile: StratumListenerProfile) -> Decimal:
        return self.coordinator.client_startup_difficulty(profile)

    def apply_send_timeout(self, sock: socket.socket) -> None:
        self.coordinator.apply_stratum_send_timeout(sock)

    def make_client_thread(self, client: ClientState) -> threading.Thread:
        return threading.Thread(
            target=self.coordinator.handle_client,
            args=(client,),
            name=f"prism-stratum-client-{client.connection_id}",
            daemon=True,
        )

    def extranonce2_size(self) -> int:
        return int(self.coordinator.extranonce2_size)

    def version_mask(self) -> int:
        return int(self.coordinator.version_mask)

    def username_fallback_address(self) -> str | None:
        return getattr(
            self.coordinator,
            "username_fallback_address",
            default_prism_username_fallback_address(),
        )

    def resolve_worker(
        self, username: str, fallback: Callable[[], WorkerIdentity]
    ) -> WorkerIdentity:
        override = self.coordinator.__dict__.get("resolve_worker")
        return override(username) if override is not None else fallback()

    def reserve_client_username(
        self,
        client: ClientState,
        worker: WorkerIdentity,
        fallback: Callable[[], bool],
    ) -> bool:
        override = self.coordinator.__dict__.get("reserve_client_username")
        return bool(override(client, worker)) if override is not None else fallback()

    def send_result(
        self, client: ClientState, request_id: object, result: object
    ) -> None:
        override = self.coordinator.__dict__.get("send_result")
        if override is not None:
            override(client, request_id, result)
        else:
            client.send(stratum_result_payload(request_id, result))

    def send_error(
        self,
        client: ClientState,
        request_id: object,
        code: int,
        message: str,
        *,
        reason: str | None,
    ) -> None:
        override = self.coordinator.__dict__.get("send_error")
        if override is not None:
            override(client, request_id, code, message, reason=reason)
        else:
            client.send(stratum_error_payload(request_id, code, message, reason=reason))

    def disconnect_client(
        self, client: ClientState, fallback: Callable[[], None]
    ) -> None:
        override = self.coordinator.__dict__.get("disconnect_client")
        if override is not None:
            override(client)
        else:
            fallback()


class _CoordinatorSessionJobs(JobDeliveryPort):
    def __init__(self, coordinator: PrismCoordinator) -> None:
        self.coordinator = coordinator

    def note_collection_identity_available(self, client: ClientState) -> None:
        self.coordinator._note_collection_identity_available(client)

    def request_initial_job_delivery(self, client: ClientState) -> None:
        self.coordinator.request_initial_job_delivery(client)

    def reauthorization_has_capacity(self, client: ClientState) -> bool:
        return self.coordinator._ensure_job_delivery_service().reauthorization_has_capacity(
            client
        )

    def apply_client_difficulty_requests(self, client: ClientState) -> Decimal | None:
        return self.coordinator.apply_client_difficulty_requests(client)

    def advertise_client_difficulty(self, client: ClientState, target: Decimal) -> bool:
        return self.coordinator.advertise_client_difficulty(client, target)

    def handle_submit(self, client: ClientState, params: list[object]) -> bool:
        return self.coordinator.handle_submit(client, params)

    def refresh_jobs_after_pending_accepted_block(self, client: ClientState) -> None:
        self.coordinator.refresh_jobs_after_pending_accepted_block(client)

    def cancel_pending_initial_job_locked(
        self,
        client: ClientState,
    ) -> Callable[[], object] | None:
        request = self.coordinator._cancel_pending_initial_job_locked(
            client,
            count=True,
        )
        if request is None or request.future is None:
            return None
        return lambda: self.coordinator._cancel_initial_job_future(request.future)

    def cleanup_disconnected_client(self, client: ClientState) -> None:
        coordinator = self.coordinator
        with coordinator.lock:
            coordinator._ensure_job_delivery_service().retire_client_locked(client)
            client.authorized = False
            client.worker = None
            client.username = ""

    def retain_current_collection_refresh_if_unrepresented(self) -> None:
        self.coordinator._retain_current_collection_refresh_if_unrepresented()


class _CoordinatorSessionProgress(ProgressHealthPort):
    def __init__(self, coordinator: PrismCoordinator) -> None:
        self.coordinator = coordinator

    def record_delivery(
        self,
        client: ClientState,
        context: object,
        delivered_monotonic: float,
    ) -> None:
        self.coordinator._record_progress_delivery_to_health(
            client,
            context,  # type: ignore[arg-type]
            delivered_monotonic,
        )

    def reconcile_eligibility(self) -> None:
        coordinator = self.coordinator
        service = getattr(coordinator, "progress_health_service", None)
        if service is not None:
            service.reconcile_pending(coordinator._progress_eligibility_snapshot())


class _CoordinatorJobPreparation(JobPreparationPort):
    def __init__(
        self,
        *,
        ensure_reorg_current: Callable[[], bool],
        issuance_artifacts: Callable[[], CachedTemplateArtifacts],
        shared_bundle: Callable[..., CachedJobBundle],
        artifacts_current: Callable[[CachedTemplateArtifacts], bool],
        clear_artifacts: Callable[[CachedTemplateArtifacts], None],
        record_failure: Callable[[], None],
        phases: Callable[[], dict[str, float]],
        retained_artifacts: Callable[[], CachedTemplateArtifacts | None],
        chain_view_untrusted: Callable[[], bool],
        admit_idle_bundle_source: Callable[..., AdmittedIdleBundleSource | None],
        observe_elapsed: Callable[[float, Mapping[str, float]], None],
        collection_identity: Callable[[WorkerIdentity], object],
        ready_latched: Callable[[], bool],
        template_fingerprint: Callable[[Mapping[str, object]], str],
    ) -> None:
        self._ensure_reorg_current = ensure_reorg_current
        self._issuance_artifacts = issuance_artifacts
        self._shared_bundle = shared_bundle
        self._artifacts_current = artifacts_current
        self._clear_artifacts = clear_artifacts
        self._record_failure = record_failure
        self._phases = phases
        self._retained_artifacts = retained_artifacts
        self._chain_view_untrusted = chain_view_untrusted
        self._admit_idle_bundle_source = admit_idle_bundle_source
        self._observe_elapsed = observe_elapsed
        self._collection_identity = collection_identity
        self._ready_latched = ready_latched
        self._template_fingerprint = template_fingerprint

    def ensure_reorg_current(self) -> bool:
        return self._ensure_reorg_current()

    def issuance_artifacts(self) -> CachedTemplateArtifacts:
        return self._issuance_artifacts()

    def shared_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentity,
        *,
        cancelled: Callable[[], bool] | None = None,
        request_source: str = "routine",
    ) -> CachedJobBundle:
        return self._shared_bundle(
            artifacts,
            worker,
            cancelled=cancelled,
            request_source=request_source,
        )

    def artifacts_current(self, artifacts: CachedTemplateArtifacts) -> bool:
        return self._artifacts_current(artifacts)

    def clear_artifacts(self, artifacts: CachedTemplateArtifacts) -> None:
        self._clear_artifacts(artifacts)

    def record_failure(self) -> None:
        self._record_failure()

    def phases(self) -> dict[str, float]:
        return self._phases()

    def retained_artifacts(self) -> CachedTemplateArtifacts | None:
        return self._retained_artifacts()

    def chain_view_untrusted(self) -> bool:
        return self._chain_view_untrusted()

    def admit_idle_bundle_source(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        *,
        allow_uncached: bool,
    ) -> AdmittedIdleBundleSource | None:
        return self._admit_idle_bundle_source(
            client,
            bundle,
            allow_uncached=allow_uncached,
        )

    def observe_elapsed(
        self,
        elapsed_seconds: float,
        phases: Mapping[str, float],
    ) -> None:
        self._observe_elapsed(elapsed_seconds, phases)

    def collection_identity(self, worker: WorkerIdentity) -> object:
        return self._collection_identity(worker)

    def ready_latched(self) -> bool:
        return self._ready_latched()

    def template_fingerprint(self, template: Mapping[str, object]) -> str:
        return self._template_fingerprint(template)


class _CoordinatorTipAuthority(TipAuthorityPort):
    def __init__(
        self,
        *,
        live_tip: Callable[[], str],
        observe_tip: Callable[[str], object],
        published_authority: Callable[[], tuple[str, float | None] | None],
        published_authoritative: Callable[[float], bool],
        current_tip_locked: Callable[[], str | None],
        published_template_locked: Callable[[], QbitTipTemplateSnapshot | None],
        snapshot_current_locked: Callable[[QbitTipTemplateSnapshot, int], bool],
        artifacts_parent_current_locked: Callable[..., bool],
        ensure_artifacts_parent_observed: Callable[..., bool],
        schedule_retry: Callable[[], None],
        prepared_obsolete: Callable[..., bool],
        prepared_token_current_locked: Callable[..., bool],
        record_cancellation: Callable[[str], None],
        retention_authority_locked: Callable[[], RetentionAuthority],
        consume_retained_refresh: Callable[[PrismJobContext], None],
        published_current_locked: Callable[..., bool],
    ) -> None:
        self._live_tip = live_tip
        self._observe_tip = observe_tip
        self._published_authority = published_authority
        self._published_authoritative = published_authoritative
        self._current_tip_locked = current_tip_locked
        self._published_template_locked = published_template_locked
        self._snapshot_current_locked = snapshot_current_locked
        self._artifacts_parent_current_locked = artifacts_parent_current_locked
        self._ensure_artifacts_parent_observed = ensure_artifacts_parent_observed
        self._schedule_retry = schedule_retry
        self._prepared_obsolete = prepared_obsolete
        self._prepared_token_current_locked = prepared_token_current_locked
        self._record_cancellation = record_cancellation
        self._retention_authority_locked = retention_authority_locked
        self._consume_retained_refresh = consume_retained_refresh
        self._published_current_locked = published_current_locked

    def live_tip(self) -> str:
        return self._live_tip()

    def observe_tip(self, tip_hash: str) -> object:
        return self._observe_tip(tip_hash)

    def published_authority(self) -> tuple[str, float | None] | None:
        return self._published_authority()

    def published_authoritative(self, now: float) -> bool:
        return self._published_authoritative(now)

    def current_tip_locked(self) -> str | None:
        return self._current_tip_locked()

    def published_template_locked(self) -> QbitTipTemplateSnapshot | None:
        return self._published_template_locked()

    def snapshot_current_locked(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> bool:
        return self._snapshot_current_locked(snapshot, observation_sequence)

    def artifacts_parent_current_locked(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        return self._artifacts_parent_current_locked(artifacts)

    def ensure_artifacts_parent_observed(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        return self._ensure_artifacts_parent_observed(artifacts)

    def schedule_retry(self) -> None:
        self._schedule_retry()

    def prepared_obsolete(
        self,
        validation_token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: _FanoutCancellation | None,
    ) -> bool:
        return self._prepared_obsolete(
            validation_token,
            bundle,
            snapshot,
            cancel_event,
        )

    def prepared_token_current_locked(
        self,
        validation_token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        payout_snapshot: object,
    ) -> bool:
        return self._prepared_token_current_locked(
            validation_token,
            bundle,
            snapshot,
            payout_snapshot,
        )

    def record_cancellation(self, stage: str) -> None:
        self._record_cancellation(stage)

    def retention_authority_locked(self) -> RetentionAuthority:
        return self._retention_authority_locked()

    def consume_retained_refresh(self, context: PrismJobContext) -> None:
        self._consume_retained_refresh(context)

    def published_current_locked(
        self,
        context_parent: str,
        *,
        template_fingerprint: str | None,
        template_generation: int,
        lapsed_live_validated: bool,
        payout_generation: int,
    ) -> bool:
        return self._published_current_locked(
            context_parent,
            template_fingerprint=template_fingerprint,
            template_generation=template_generation,
            lapsed_live_validated=lapsed_live_validated,
            payout_generation=payout_generation,
        )


class _CoordinatorPayoutDelivery(PayoutDeliveryPort):
    def __init__(
        self,
        *,
        snapshot: Callable[[], object],
        generation: Callable[[], int],
        initial_admission: Callable[..., object],
        admission: Callable[..., object],
        observe_admission: Callable[..., None],
        record_first_delivery: Callable[[int, float], None],
    ) -> None:
        self._snapshot = snapshot
        self._generation = generation
        self._initial_admission = initial_admission
        self._admission = admission
        self._observe_admission = observe_admission
        self._record_first_delivery = record_first_delivery

    def snapshot(self) -> object:
        return self._snapshot()

    def generation(self) -> int:
        return self._generation()

    def initial_admission(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int,
    ) -> object:
        return self._initial_admission(cancelled, generation=generation)

    def admission(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int,
        priority: bool,
    ) -> object:
        return self._admission(
            cancelled, generation=generation, priority=priority
        )

    def observe_admission(
        self,
        admission: object,
        *,
        generation: int,
        fallback_wait_seconds: float,
    ) -> None:
        self._observe_admission(
            admission,
            generation=generation,
            fallback_wait_seconds=fallback_wait_seconds,
        )

    def record_first_delivery(
        self,
        generation: int,
        delivered_monotonic: float,
    ) -> None:
        self._record_first_delivery(generation, delivered_monotonic)

class _CoordinatorInitialJobRuntime(InitialJobRuntimePort):
    def __init__(
        self,
        *,
        stopping: Callable[[], bool],
        wait: Callable[[float], bool],
        disconnect: Callable[[ClientState], None],
        submit_initial: Callable[..., Future[Any]],
    ) -> None:
        self._stopping = stopping
        self._wait = wait
        self._disconnect = disconnect
        self._submit_initial = submit_initial

    def stopping(self) -> bool:
        return self._stopping()

    def wait(self, timeout: float) -> bool:
        return self._wait(timeout)

    def disconnect(self, client: ClientState) -> None:
        self._disconnect(client)

    def submit_initial(
        self,
        function: Callable[[PendingInitialJob], bool],
        request: PendingInitialJob,
        *,
        priority: int,
    ) -> Future[Any]:
        return self._submit_initial(function, request, priority=priority)


class _CoordinatorProgressDelivery(ProgressDeliveryPort):
    def __init__(
        self,
        *,
        record_health_delivery: Callable[[ClientState, PrismJobContext, float], None],
        reconcile_health_eligibility: Callable[[], None],
    ) -> None:
        self._record_health_delivery = record_health_delivery
        self._reconcile_health_eligibility = reconcile_health_eligibility

    def record_health_delivery(
        self,
        client: ClientState,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> None:
        self._record_health_delivery(client, context, delivered_monotonic)

    def reconcile_health_eligibility(self) -> None:
        self._reconcile_health_eligibility()


class _CoordinatorObservability(ObservabilityPort):
    """Collect immutable health inputs without exporting coordinator state."""

    def __init__(self, coordinator: PrismCoordinator) -> None:
        self.coordinator = coordinator

    def monotonic(self) -> float:
        return time.monotonic()

    def mining_delivery_inputs(self, now: float) -> MiningDeliveryInputs:
        coordinator = self.coordinator
        delivery_service = coordinator._ensure_job_delivery_service()
        current_job_source = delivery_service.current_job_source()
        with coordinator.lock:
            initial_state = coordinator._ensure_initial_job_state()
            initial_snapshot = initial_state.snapshot()
            clients = tuple(coordinator.clients)
            active = len(clients)
            current_tip = coordinator._current_published_tip_hash_locked()
            published_snapshot = getattr(coordinator, "tip_template_snapshot", None)
            subscribed = sum(1 for client in clients if client.subscribed)
            authorized_clients = tuple(
                client
                for client in clients
                if client.subscribed and client.authorized and client.worker is not None
            )
            clients_with_current_work = tuple(
                client
                for client in authorized_clients
                if delivery_service.client_has_current_tip_job_locked(
                    client,
                    current_job_source,
                )
            )
            current = len(clients_with_current_work)
            pending_requests = tuple(initial_state.pending.values())
            oldest_pending_age = max(
                (
                    max(0.0, now - request.requested_monotonic)
                    for request in pending_requests
                ),
                default=0.0,
            )
            genuinely_pending_initial_clients = tuple(
                client
                for client in authorized_clients
                if not delivery_service.client_has_delivered_work_locked(client)
            )
            genuine_initial_started = tuple(
                started
                for client in genuinely_pending_initial_clients
                for started in (
                    client.authorized_monotonic,
                    (
                        initial_state.pending[client].requested_monotonic
                        if client in initial_state.pending
                        else None
                    ),
                )
                if started is not None
            )
            oldest_genuine_initial_age = max(
                (max(0.0, now - started) for started in genuine_initial_started),
                default=0.0,
            )
            connection_limit = int(
                getattr(
                    coordinator,
                    "stratum_max_connections",
                    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
                )
            )
            pending_limit = int(initial_state.config.max_pending)
            peak = coordinator.peak_active_connection_count
            handlers = coordinator.handler_thread_count

        (
            prepared_bundle,
            prepared_snapshot,
            preparation_pending,
        ) = coordinator._ensure_job_bundle_service().prepared_ready_snapshot()
        payout_generation = int(
            coordinator._ensure_payout_state_service().snapshot().generation
        )
        prepared_current = bool(
            prepared_bundle is not None
            and prepared_snapshot is published_snapshot
            and published_snapshot is not None
            and current_tip is not None
            and published_snapshot.bestblockhash == current_tip
            and published_snapshot.template_artifacts is not None
            and not prepared_bundle.collection_only
            and prepared_bundle.template
            is published_snapshot.template_artifacts.template
            and prepared_bundle.template_fingerprint
            == published_snapshot.template_fingerprint
            and prepared_bundle.template_generation
            == published_snapshot.template_generation
            and prepared_bundle.payout_state_generation == payout_generation
        )
        queue_depth, active_workers = (
            coordinator._ensure_tip_refresh_service().executor_stats()
        )
        rejection_counts = getattr(coordinator, "rejection_counts_by_reason", {})
        stale_unknown = int(rejection_counts.get(PRISM_REJECTION_STALE_JOB, 0)) + int(
            rejection_counts.get(PRISM_REJECTION_UNKNOWN_JOB, 0)
        )
        return MiningDeliveryInputs(
            active_connections=active,
            connection_capacity=connection_limit,
            peak_active_connections=peak,
            subscribed_connections=subscribed,
            authorized_connections=len(authorized_clients),
            pending_initial_jobs=initial_snapshot.pending_count,
            pending_initial_job_capacity=pending_limit,
            oldest_pending_initial_job_age_seconds=oldest_pending_age,
            oldest_genuinely_pending_initial_job_age_seconds=(
                oldest_genuine_initial_age
            ),
            clients_with_current_tip_jobs=current,
            clients_with_no_active_job=sum(
                1 for client in authorized_clients if client.active_job is None
            ),
            last_initial_job_delivery_monotonic=(
                initial_snapshot.last_delivery_monotonic
            ),
            initial_job_timeout_seconds=float(initial_state.config.timeout_seconds),
            initial_job_queue_rejections=initial_snapshot.queue_rejection_count,
            initial_job_timeout_disconnects=initial_snapshot.timeout_count,
            initial_job_cancelled_tasks=initial_snapshot.cancelled_count,
            initial_job_coalesced_tasks=initial_snapshot.coalesced_count,
            initial_job_queue_capacity_reclaimed=(
                initial_snapshot.queue_capacity_reclaimed_count
            ),
            handler_threads=handlers,
            delivery_executor_queue_depth=queue_depth,
            delivery_executor_active_workers=active_workers,
            started_monotonic=float(getattr(coordinator, "started_monotonic", now)),
            startup_grace_seconds=float(
                getattr(
                    coordinator,
                    "mining_health_startup_grace_seconds",
                    DEFAULT_PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS,
                )
            ),
            stale_unknown_rejections=stale_unknown,
            submitted_shares=int(getattr(coordinator, "submitted_share_count", 0)),
            job_preparation_pending=preparation_pending,
            current_observed_tip=current_tip,
            prepared_bundle_current=prepared_current,
            prepared_bundle_tip=(
                prepared_snapshot.bestblockhash
                if prepared_snapshot is not None
                else None
            ),
            prepared_bundle_template_generation=(
                prepared_bundle.template_generation
                if prepared_bundle is not None
                else None
            ),
            prepared_bundle_payout_generation=(
                prepared_bundle.payout_state_generation
                if prepared_bundle is not None
                else None
            ),
        )

    def accepted_share_stats(self) -> tuple[int, int]:
        return self.coordinator.accepted_share_stats()

    def ledger_backend(self) -> str:
        return str(self.coordinator.ledger.backend_name)

    def block_counts(self) -> tuple[int, int]:
        coordinator = self.coordinator
        lock = getattr(coordinator, "lock", None)
        if lock is None:
            return int(coordinator.accepted_block_count), int(coordinator.max_blocks)
        with lock:
            return int(coordinator.accepted_block_count), int(coordinator.max_blocks)

    def progress_health(self) -> Mapping[str, object]:
        return self.coordinator.progress_health_snapshot()

    def health_refresh_seconds(self) -> float:
        return float(
            getattr(
                self.coordinator,
                "health_refresh_seconds",
                DEFAULT_PRISM_HEALTH_REFRESH_SECONDS,
            )
        )

    def render_metrics_payload(self) -> str:
        return self.coordinator._render_metrics_payload()

    def metrics_refresh_seconds(self) -> float:
        return float(
            getattr(
                self.coordinator,
                "metrics_refresh_seconds",
                DEFAULT_PRISM_METRICS_REFRESH_SECONDS,
            )
        )

    def stop_requested(self) -> bool:
        return self.coordinator.stop_event.is_set()

    def wait_for_stop(self, timeout: float) -> bool:
        return self.coordinator.stop_event.wait(timeout)

    def log(self, message: str) -> None:
        print(message, flush=True)

    def log_exception(self) -> None:
        traceback.print_exc()


class _CoordinatorAuditHttp(AuditHttpPort):
    """Expose purpose-specific dynamic reads to the HTTP facade."""

    def __init__(
        self,
        coordinator: PrismCoordinator,
        *,
        allow_uncached_compatibility: bool = False,
    ) -> None:
        self.coordinator = coordinator
        self.allow_uncached_compatibility = allow_uncached_compatibility

    def cached_health_payload(self) -> tuple[int, Mapping[str, object]]:
        cached = getattr(self.coordinator, "cached_health_payload", None)
        if callable(cached):
            return cached()
        if self.allow_uncached_compatibility:
            return 200, self.coordinator.health_payload()
        raise RuntimeError("cached health is unavailable")

    def cached_metrics_payload(self) -> tuple[int, str]:
        cached = getattr(self.coordinator, "cached_metrics_payload", None)
        if callable(cached):
            status, payload = cached()
            if status != 503 or not self.allow_uncached_compatibility:
                return status, payload
        if self.allow_uncached_compatibility:
            return 200, self.coordinator.metrics_payload()
        raise RuntimeError("cached metrics are unavailable")

    def latest_evidence_payload(self) -> Mapping[str, object] | None:
        return self.coordinator.latest_evidence_payload()

    def owed_balances_payload(self) -> Mapping[str, object]:
        return self.coordinator.owed_balances_payload()

    def carry_forward_integrity_payload(self) -> Mapping[str, object]:
        return self.coordinator.carry_forward_integrity_payload()

    def miner_status_payload(self, recipient_id: str) -> Mapping[str, object]:
        return self.coordinator.miner_status_payload(recipient_id)

    def public_payload(
        self,
        path: str,
        query: Mapping[str, list[str]],
    ) -> tuple[int, object]:
        return public_api.dispatch(self.coordinator, path, dict(query))

    def ledger_backend(self) -> str:
        return str(self.coordinator.ledger.backend_name)

    def audit_share_window(
        self,
        *,
        anchor_job_issued_at_ms: int,
        network_difficulty: int,
    ) -> list[dict[str, object]]:
        return self.coordinator.ledger.audit_share_window(
            anchor_job_issued_at_ms=anchor_job_issued_at_ms,
            network_difficulty=network_difficulty,
        )

    def audit_block_payouts(
        self,
        *,
        block_hash: str,
    ) -> list[dict[str, object]]:
        return self.coordinator.ledger.audit_block_payouts(block_hash=block_hash)

    def audit_ctv_fanouts(
        self,
        *,
        block_hash: str,
    ) -> list[dict[str, object]]:
        return self.coordinator.ledger.audit_ctv_fanouts(block_hash=block_hash)

    def audit_ctv_fanout_manifest_set(
        self,
        *,
        block_hash: str,
    ) -> Mapping[str, object] | None:
        return self.coordinator.ledger.audit_ctv_fanout_manifest_set(
            block_hash=block_hash
        )

    def ctv_fanout_status(
        self,
        *,
        fanout_txid: str,
    ) -> Mapping[str, object] | None:
        return self.coordinator.ledger.ctv_fanout_status(fanout_txid=fanout_txid)

    def pending_ctv_fanout_statuses(
        self,
        *,
        limit: int,
    ) -> list[dict[str, object]]:
        return self.coordinator.ledger.pending_ctv_fanout_statuses(limit=limit)

    def audit_bundle(
        self,
        *,
        block_hash: str,
    ) -> Mapping[str, object] | None:
        return self.coordinator.ledger.audit_bundle(block_hash=block_hash)

    def audit_bundle_by_commitment(
        self,
        *,
        commitment_leaf_hex: str,
    ) -> Mapping[str, object] | None:
        return self.coordinator.ledger.audit_bundle_by_commitment(
            commitment_leaf_hex=commitment_leaf_hex
        )


class _InitialJobPendingCompatibility:
    """Compatibility alias that adopts replacements into S2 ownership."""

    _backing = "_pending_initial_jobs_compat"

    def __get__(
        self,
        instance: PrismCoordinator | None,
        owner: type[PrismCoordinator],
    ) -> MutableMapping[ClientState, PendingInitialJob] | _InitialJobPendingCompatibility:
        if instance is None:
            return self
        state = instance.__dict__.get("_initial_job_state")
        if state is not None:
            return state.pending
        pending = instance.__dict__.get(self._backing)
        if pending is None:
            pending = instance.__dict__.setdefault(self._backing, {})
        return pending

    def __set__(
        self,
        instance: PrismCoordinator,
        value: MutableMapping[ClientState, PendingInitialJob],
    ) -> None:
        state = instance.__dict__.get("_initial_job_state")
        if state is None:
            instance.__dict__[self._backing] = value
            return
        state.adopt_pending(value)
        instance.__dict__["_initial_job_tracker"] = state.tracker


class _InitialJobConfigCompatibility:
    def __init__(self, field_name: str, default: int | float, cast: type) -> None:
        self.field_name = field_name
        self.default = default
        self.cast = cast
        self.backing = f"_{field_name}_compat"

    def __get__(self, instance: PrismCoordinator | None, owner: type[PrismCoordinator]) -> Any:
        if instance is None:
            return self
        state = instance.__dict__.get("_initial_job_state")
        if state is not None:
            return getattr(state.config, self.field_name)
        return instance.__dict__.get(self.backing, self.default)

    def __set__(self, instance: PrismCoordinator, value: object) -> None:
        converted = self.cast(value)
        state = instance.__dict__.get("_initial_job_state")
        if state is None:
            instance.__dict__[self.backing] = converted
            return
        state.reconfigure(**{self.field_name: converted})


class _InitialJobMetricCompatibility:
    def __init__(self, field_name: str, default: object, cast: type | None) -> None:
        self.field_name = field_name
        self.default = default
        self.cast = cast
        self.backing = f"_{field_name}_compat"

    def __get__(self, instance: PrismCoordinator | None, owner: type[PrismCoordinator]) -> Any:
        if instance is None:
            return self
        state = instance.__dict__.get("_initial_job_state")
        if state is not None:
            return getattr(state, self.field_name)
        return instance.__dict__.get(self.backing, self.default)

    def __set__(self, instance: PrismCoordinator, value: object) -> None:
        converted = value if self.cast is None else self.cast(value)
        state = instance.__dict__.get("_initial_job_state")
        if state is None:
            instance.__dict__[self.backing] = converted
            return
        setattr(state, self.field_name, converted)


class _JobCounterCompatibility:
    _backing = "_job_counter_compat"

    def __get__(self, instance: PrismCoordinator | None, owner: type[PrismCoordinator]) -> Any:
        if instance is None:
            return self
        service = instance.__dict__.get("_job_delivery_service")
        if service is not None:
            return service.job_counter
        return int(instance.__dict__.get(self._backing, 0))

    def __set__(self, instance: PrismCoordinator, value: object) -> None:
        converted = int(value)
        service = instance.__dict__.get("_job_delivery_service")
        if service is None:
            instance.__dict__[self._backing] = converted
            return
        service.adopt_job_counter(converted)


class _JobsCompatibility:
    _backing = "_jobs_compat"

    def __get__(self, instance: PrismCoordinator | None, owner: type[PrismCoordinator]) -> Any:
        if instance is None:
            return self
        service = instance.__dict__.get("_job_delivery_service")
        if service is not None:
            return service.jobs
        jobs = instance.__dict__.get(self._backing)
        if jobs is None:
            jobs = instance.__dict__.setdefault(self._backing, {})
        return jobs

    def __set__(
        self,
        instance: PrismCoordinator,
        value: MutableMapping[str, PrismJobContext],
    ) -> None:
        service = instance.__dict__.get("_job_delivery_service")
        if service is None:
            instance.__dict__[self._backing] = value
            return
        service.adopt_jobs(value)


class _ClientsCompatibility:
    _backing = "_clients_compat"

    def __get__(
        self,
        instance: PrismCoordinator | None,
        owner: type[PrismCoordinator],
    ) -> Any:
        if instance is None:
            return self
        registry = instance.__dict__.get("_session_registry")
        if registry is not None:
            return registry.clients
        clients = instance.__dict__.get(self._backing)
        if clients is None:
            clients = instance.__dict__.setdefault(self._backing, set())
        return clients

    def __set__(self, instance: PrismCoordinator, value: object) -> None:
        registry = instance.__dict__.get("_session_registry")
        if registry is None:
            instance.__dict__[self._backing] = value
            return
        registry.adopt_clients(value)


class _RetainedConfigCompatibility:
    def __init__(self, field_name: str, default: int | float, cast: type) -> None:
        self.field_name = field_name
        self.default = default
        self.cast = cast
        self.backing = f"_{field_name}_compat"

    def __get__(self, instance: PrismCoordinator | None, owner: type[PrismCoordinator]) -> Any:
        if instance is None:
            return self
        index = instance.__dict__.get("_retained_job_index")
        if index is not None:
            return getattr(index, self.field_name)
        return instance.__dict__.get(self.backing, self.default)

    def __set__(self, instance: PrismCoordinator, value: object) -> None:
        converted = self.cast(value)
        index = instance.__dict__.get("_retained_job_index")
        if index is None:
            instance.__dict__[self.backing] = converted
            return
        setattr(index, self.field_name, converted)


class _RetainedStateCompatibility:
    _adopted_fields = {
        "graveyard",
        "by_connection",
        "same_tip_by_connection",
        "same_tip_job_ids",
    }

    def __init__(
        self,
        field_name: str,
        default_factory: Callable[[], object],
    ) -> None:
        self.field_name = field_name
        self.default_factory = default_factory
        self.backing = f"_evicted_{field_name}_compat"

    def __get__(self, instance: PrismCoordinator | None, owner: type[PrismCoordinator]) -> Any:
        if instance is None:
            return self
        index = instance.__dict__.get("_retained_job_index")
        if index is not None:
            return getattr(index, self.field_name)
        if self.backing not in instance.__dict__:
            instance.__dict__[self.backing] = self.default_factory()
        return instance.__dict__[self.backing]

    def __set__(self, instance: PrismCoordinator, value: object) -> None:
        index = instance.__dict__.get("_retained_job_index")
        if index is None:
            instance.__dict__[self.backing] = value
            return
        if self.field_name not in self._adopted_fields:
            setattr(index, self.field_name, value)
            return
        replacements = {
            "graveyard": index.graveyard,
            "by_connection": index.by_connection,
            "same_tip_by_connection": index.same_tip_by_connection,
            "same_tip_job_ids": index.same_tip_job_ids,
        }
        replacements[self.field_name] = value
        index.adopt(
            graveyard=replacements["graveyard"],
            by_connection=replacements["by_connection"],
            same_tip_by_connection=replacements["same_tip_by_connection"],
            same_tip_job_ids=replacements["same_tip_job_ids"],
            current_tip=instance._job_delivery_current_tip_locked(),
        )


class PrismCoordinator:
    reorg_reconciler_enabled = ReorgCompatibilityField("enabled", True)
    reorg_reconcile_cache_seconds = ReorgCompatibilityField(
        "cache_seconds",
        DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
    )
    reorg_inactive_block_count = ReorgCompatibilityField(
        "inactive_block_count",
        0,
    )
    reorg_reactivated_block_count = ReorgCompatibilityField(
        "reactivated_block_count",
        0,
    )
    reorg_reconcile_skip_count = ReorgCompatibilityField(
        "reconcile_skip_count",
        0,
    )
    reorg_reconcile_error_count = ReorgCompatibilityField(
        "reconcile_error_count",
        0,
    )
    matured_payout_count = ReorgCompatibilityField("matured_payout_count", 0)
    last_reorg_reconciled_tip_hash = ReorgCompatibilityField(
        "last_tip_hash",
        None,
    )
    last_reorg_reconciled_trusted = ReorgCompatibilityField(
        "last_trusted",
        False,
    )
    last_reorg_reconciled_monotonic = ReorgCompatibilityField(
        "last_monotonic",
        None,
    )

    recent_share_keys = RecentShareCompatibilityField()
    idle_retarget_count = VardiffCompatibilityField(
        "idle_retarget_count",
        lambda: 0,
    )
    _vardiff_idle_lock = VardiffCompatibilityField(
        "_vardiff_idle_lock",
        threading.Lock,
    )
    _vardiff_idle_executor = VardiffCompatibilityField(
        "_vardiff_idle_executor",
        lambda: None,
    )
    _vardiff_idle_executor_shutdown = VardiffCompatibilityField(
        "_vardiff_idle_executor_shutdown",
        lambda: False,
    )
    _vardiff_idle_pending = VardiffCompatibilityField(
        "_vardiff_idle_pending",
        set,
    )
    vardiff_idle_queue_depth = VardiffCompatibilityField(
        "vardiff_idle_queue_depth",
        lambda: 0,
    )
    vardiff_idle_inflight = VardiffCompatibilityField(
        "vardiff_idle_inflight",
        lambda: 0,
    )
    vardiff_idle_clients_inspected = VardiffCompatibilityField(
        "vardiff_idle_clients_inspected",
        lambda: 0,
    )
    vardiff_idle_skip_counts = VardiffCompatibilityField(
        "vardiff_idle_skip_counts",
        lambda: {reason: 0 for reason in PRISM_VARDIFF_IDLE_SKIP_REASONS},
    )
    vardiff_idle_task_failures = VardiffCompatibilityField(
        "vardiff_idle_task_failures",
        lambda: 0,
    )
    vardiff_idle_sweep_histogram = VardiffCompatibilityField(
        "vardiff_idle_sweep_histogram",
        lambda: {
            "buckets": {
                bucket: 0 for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS
            },
            "sum": 0.0,
            "count": 0,
        },
    )
    vardiff_idle_task_histogram = VardiffCompatibilityField(
        "vardiff_idle_task_histogram",
        lambda: {
            "buckets": {
                bucket: 0 for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS
            },
            "sum": 0.0,
            "count": 0,
        },
    )

    pending_initial_jobs = _InitialJobPendingCompatibility()
    stratum_max_pending_initial_jobs = _InitialJobConfigCompatibility(
        "max_pending",
        DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS,
        int,
    )
    stratum_initial_job_timeout_seconds = _InitialJobConfigCompatibility(
        "timeout_seconds",
        DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS,
        float,
    )
    initial_job_max_workers = _InitialJobConfigCompatibility(
        "max_workers",
        DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
        int,
    )
    initial_job_queue_rejection_count = _InitialJobMetricCompatibility(
        "queue_rejection_count", 0, int
    )
    initial_job_timeout_count = _InitialJobMetricCompatibility("timeout_count", 0, int)
    initial_job_cancelled_count = _InitialJobMetricCompatibility(
        "cancelled_count", 0, int
    )
    initial_job_coalesced_count = _InitialJobMetricCompatibility(
        "coalesced_count", 0, int
    )
    initial_job_sent_count = _InitialJobMetricCompatibility("sent_count", 0, int)
    initial_job_failed_count = _InitialJobMetricCompatibility("failed_count", 0, int)
    initial_job_superseded_count = _InitialJobMetricCompatibility(
        "superseded_count", 0, int
    )
    initial_job_queue_capacity_reclaimed_count = _InitialJobMetricCompatibility(
        "queue_capacity_reclaimed_count", 0, int
    )
    initial_job_delivery_latency_seconds_sum = _InitialJobMetricCompatibility(
        "delivery_latency_seconds_sum", 0.0, float
    )
    initial_job_delivery_latency_count = _InitialJobMetricCompatibility(
        "delivery_latency_count", 0, int
    )
    last_initial_job_delivery_monotonic = _InitialJobMetricCompatibility(
        "last_delivery_monotonic", None, None
    )
    job_counter = _JobCounterCompatibility()
    jobs = _JobsCompatibility()
    clients = _ClientsCompatibility()
    block_candidate_queue = BlockCandidateCompatibilityField(
        "block_candidate_queue",
        candidate_compatibility_default(
            lambda: queue.Queue(maxsize=MAX_PENDING_BLOCK_CANDIDATES)
        ),
    )
    block_candidates_dropped = BlockCandidateCompatibilityField(
        "block_candidates_dropped", 0
    )
    block_candidate_wakeups_coalesced = BlockCandidateCompatibilityField(
        "block_candidate_wakeups_coalesced", 0
    )
    block_candidate_retry_count = BlockCandidateCompatibilityField(
        "block_candidate_retry_count", 0
    )
    block_candidate_poisoned_count = BlockCandidateCompatibilityField(
        "block_candidate_poisoned_count", 0
    )
    block_candidate_retry_initial_seconds = BlockCandidateCompatibilityField(
        "block_candidate_retry_initial_seconds",
        DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS,
    )
    block_candidate_retry_max_seconds = BlockCandidateCompatibilityField(
        "block_candidate_retry_max_seconds",
        DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS,
    )
    block_candidate_retry_delays = BlockCandidateCompatibilityField(
        "block_candidate_retry_delays",
        candidate_compatibility_default(lambda: {}),
    )
    block_candidate_abandoned_counts = BlockCandidateCompatibilityField(
        "block_candidate_abandoned_counts",
        candidate_compatibility_default(lambda: {}),
    )
    _retry_block_candidate = BlockCandidateCompatibilityField(
        "_retry_block_candidate", None
    )
    _block_candidate_outcome = BlockCandidateCompatibilityField(
        "_block_candidate_outcome",
        candidate_compatibility_default(lambda: threading.local()),
    )
    _block_candidate_finalize_retries = BlockCandidateCompatibilityField(
        "_block_candidate_finalize_retries",
        candidate_compatibility_default(lambda: {}),
    )
    share_append_queue = ShareWriterCompatibilityField("share_append_queue", None)
    share_commit_batch_size = ShareWriterCompatibilityField(
        "share_commit_batch_size", DEFAULT_SHARE_COMMIT_BATCH_SIZE
    )
    share_commit_linger_seconds = ShareWriterCompatibilityField(
        "share_commit_linger_seconds",
        DEFAULT_SHARE_COMMIT_LINGER_MILLISECONDS / 1000.0,
    )
    share_commit_timeout_seconds = ShareWriterCompatibilityField(
        "share_commit_timeout_seconds", DEFAULT_SHARE_COMMIT_TIMEOUT_SECONDS
    )
    share_writer_active = ShareWriterCompatibilityField("share_writer_active", False)
    share_append_failure_count = ShareWriterCompatibilityField(
        "share_append_failure_count", 0
    )
    share_recovery_path = ShareWriterCompatibilityField("share_recovery_path", None)
    share_recovery_lock = ShareWriterCompatibilityField("share_recovery_lock", None)
    shares_recovered_to_disk = ShareWriterCompatibilityField(
        "shares_recovered_to_disk", 0
    )
    shares_replayed = ShareWriterCompatibilityField("shares_replayed", 0)
    _pending_share_commit_lock = ShareWriterCompatibilityField(
        "_pending_share_commit_lock", None
    )
    _pending_share_commit_floor = ShareWriterCompatibilityField(
        "_pending_share_commit_floor", None
    )

    same_tip_job_retention_seconds = _RetainedConfigCompatibility(
        "same_tip_ttl_seconds",
        DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
        float,
    )
    same_tip_job_retention_per_connection = _RetainedConfigCompatibility(
        "same_tip_per_connection",
        DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION,
        int,
    )
    stale_grace_seconds = _RetainedConfigCompatibility(
        "stale_grace_seconds",
        DEFAULT_PRISM_STALE_GRACE_SECONDS,
        float,
    )
    evicted_job_graveyard = _RetainedStateCompatibility("graveyard", OrderedDict)
    evicted_jobs_by_connection = _RetainedStateCompatibility("by_connection", dict)
    evicted_same_tip_by_connection = _RetainedStateCompatibility(
        "same_tip_by_connection", dict
    )
    evicted_same_tip_job_ids = _RetainedStateCompatibility(
        "same_tip_job_ids", OrderedDict
    )
    evicted_job_index_tip_hash = _RetainedStateCompatibility(
        "index_tip_hash", lambda: None
    )
    evicted_job_next_prune_monotonic = _RetainedStateCompatibility(
        "next_prune_monotonic", lambda: 0.0
    )
    evicted_job_expiration_counts = _RetainedStateCompatibility(
        "expiration_counts", lambda: {name: 0 for name in PRISM_EVICTED_JOB_CLASSES}
    )
    evicted_job_capacity_eviction_counts = _RetainedStateCompatibility(
        "capacity_eviction_counts",
        lambda: {name: 0 for name in PRISM_EVICTED_JOB_CAPACITY_SCOPES},
    )
    evicted_job_submit_counts = _RetainedStateCompatibility(
        "submit_counts",
        lambda: {name: 0 for name in PRISM_EVICTED_JOB_SUBMIT_OUTCOMES},
    )

    @property
    def current_tip_first_seen(self) -> tuple[str, float | None] | None:
        return self._ensure_tip_refresh_service().published_snapshot().first_seen

    @current_tip_first_seen.setter
    def current_tip_first_seen(self, value: tuple[str, float | None] | None) -> None:
        self._ensure_tip_refresh_service().seed_published_for_test(first_seen=value)

    @property
    def current_tip_parent(self) -> tuple[str, str] | None:
        return self._ensure_tip_refresh_service().published_snapshot().parent

    @current_tip_parent.setter
    def current_tip_parent(self, value: tuple[str, str] | None) -> None:
        self._ensure_tip_refresh_service().seed_published_for_test(parent=value)

    @property
    def current_tip_observation_sequence(self) -> int:
        return self._ensure_tip_refresh_service().published_snapshot().observation_sequence

    @current_tip_observation_sequence.setter
    def current_tip_observation_sequence(self, value: int) -> None:
        self._ensure_tip_refresh_service().seed_published_for_test(
            observation_sequence=value
        )

    @property
    def current_tip_observed_monotonic(self) -> float | None:
        return self._ensure_tip_refresh_service().published_snapshot().observed_monotonic

    @current_tip_observed_monotonic.setter
    def current_tip_observed_monotonic(self, value: float | None) -> None:
        self._ensure_tip_refresh_service().seed_published_for_test(
            observed_monotonic=value
        )

    @property
    def tip_template_snapshot(self) -> QbitTipTemplateSnapshot | None:
        return self._ensure_tip_refresh_service().published_snapshot().template

    @tip_template_snapshot.setter
    def tip_template_snapshot(self, value: QbitTipTemplateSnapshot | None) -> None:
        self._ensure_tip_refresh_service().seed_published_for_test(template=value)

    @property
    def latest_detected_tip(self) -> tuple[str, int] | None:
        return self._ensure_tip_refresh_service().snapshot().latest_detected_tip

    @latest_detected_tip.setter
    def latest_detected_tip(self, value: tuple[str, int] | None) -> None:
        self._ensure_tip_refresh_service().seed_state_for_test(
            latest_detected_tip=value
        )

    @property
    def tip_refresh_divergence_started_monotonic(self) -> float | None:
        return (
            self._ensure_tip_refresh_service()
            .snapshot()
            .divergence_started_monotonic
        )

    @tip_refresh_divergence_started_monotonic.setter
    def tip_refresh_divergence_started_monotonic(self, value: float | None) -> None:
        self._ensure_tip_refresh_service().seed_state_for_test(
            divergence_started_monotonic=value
        )

    @property
    def tip_observation_sequence(self) -> int:
        return self._ensure_tip_refresh_service().snapshot().observation_sequence

    @tip_observation_sequence.setter
    def tip_observation_sequence(self, value: int) -> None:
        self._ensure_tip_refresh_service().seed_state_for_test(
            observation_sequence=value
        )

    @property
    def last_successful_template_refresh_monotonic(self) -> float | None:
        return (
            self._ensure_tip_refresh_service()
            .snapshot()
            .last_successful_refresh_monotonic
        )

    @last_successful_template_refresh_monotonic.setter
    def last_successful_template_refresh_monotonic(self, value: float | None) -> None:
        self._ensure_tip_refresh_service().seed_state_for_test(
            last_successful_refresh_monotonic=value
        )

    @property
    def template_refresh_failure_started_monotonic(self) -> float | None:
        return self._ensure_tip_refresh_service().snapshot().failure_started_monotonic

    @template_refresh_failure_started_monotonic.setter
    def template_refresh_failure_started_monotonic(self, value: float | None) -> None:
        self._ensure_tip_refresh_service().seed_state_for_test(
            failure_started_monotonic=value
        )

    @property
    def tip_refresh_job_count(self) -> int:
        return self._ensure_tip_refresh_service().snapshot().refresh_job_count

    @tip_refresh_job_count.setter
    def tip_refresh_job_count(self, value: int) -> None:
        self._ensure_tip_refresh_service().seed_state_for_test(refresh_job_count=value)

    @property
    def post_accept_refresh_failure_count(self) -> int:
        return (
            self._ensure_tip_refresh_service()
            .snapshot()
            .post_accept_refresh_failure_count
        )

    @post_accept_refresh_failure_count.setter
    def post_accept_refresh_failure_count(self, value: int) -> None:
        self._ensure_tip_refresh_service().seed_state_for_test(
            post_accept_refresh_failure_count=value
        )

    def __init__(self, config: CoordinatorConfig | None = None) -> None:
        self.config = load_coordinator_config() if config is None else config
        rpc_config = self.config.rpc
        stratum_config = self.config.stratum
        job_config = self.config.jobs
        ledger_config = self.config.ledger
        audit_config = self.config.audit
        ctv_config = self.config.ctv
        lifecycle_config = self.config.lifecycle

        self.rpc = JsonRpc(
            host=rpc_config.host,
            port=rpc_config.port,
            user=rpc_config.user,
            password=rpc_config.password,
        )
        self.qbit_chain = rpc_config.chain
        self.bind = stratum_config.bind
        self.port = stratum_config.port
        self.extranonce2_size = stratum_config.extranonce2_size
        self.blockpoll_seconds = job_config.blockpoll_seconds
        self.blockwait_enabled = job_config.blockwait_enabled
        self.blockwait_timeout_seconds = job_config.blockwait_timeout_seconds
        self.tip_refresh_failure_holdoff_seconds = (
            job_config.tip_refresh_failure_holdoff_seconds
        )
        self.stale_grace_seconds = stratum_config.stale_grace_seconds
        self.hot_path_log_enabled = self.config.hot_path_log_enabled
        self.submit_tip_max_age_seconds = job_config.submit_tip_max_age_seconds
        self.same_tip_job_retention_seconds = stratum_config.same_tip_job_retention_seconds
        self.same_tip_job_retention_per_connection = (
            stratum_config.same_tip_job_retention_per_connection
        )
        self.tip_refresh_max_workers = job_config.tip_refresh_max_workers
        self.job_build_timeout_seconds = job_config.job_build_timeout_seconds
        self.job_build_cancel_grace_seconds = job_config.job_build_cancel_grace_seconds
        self.vardiff_idle_sweep_seconds = stratum_config.vardiff_idle_sweep_seconds
        self.worker_metrics_limit = job_config.worker_metrics_limit
        self.reorg_reconciler_enabled = job_config.reorg_reconciler_enabled
        self.job_bundle_cache_seconds = job_config.job_bundle_cache_seconds
        self.bundle_build_timeout_seconds = job_config.bundle_build_timeout_seconds
        self.template_cache_seconds = job_config.template_cache_seconds
        self.template_refresh_failure_exit_seconds = (
            job_config.template_refresh_failure_exit_seconds
        )
        self.reorg_reconcile_cache_seconds = job_config.reorg_reconcile_cache_seconds
        self.health_refresh_seconds = lifecycle_config.health_refresh_seconds
        self.metrics_refresh_seconds = lifecycle_config.metrics_refresh_seconds
        self.health_pending_refresh_max_age_seconds = (
            lifecycle_config.pending_refresh_health_deadline_seconds
        )
        self.health_tip_poll_max_age_seconds = (
            lifecycle_config.coherent_tip_poll_health_deadline_seconds
        )
        self.stratum_send_timeout_seconds = stratum_config.send_timeout_seconds
        self.stratum_max_connections = stratum_config.max_connections
        self.stratum_max_connections_per_username = stratum_config.max_connections_per_username
        self.stratum_max_pending_initial_jobs = stratum_config.max_pending_initial_jobs
        self.stratum_initial_job_timeout_seconds = stratum_config.initial_job_timeout_seconds
        self.mining_health_startup_grace_seconds = (
            lifecycle_config.mining_health_startup_grace_seconds
        )
        self.stratum_accept_resource_exhaustion_backoff_seconds = (
            stratum_config.accept_resource_exhaustion_backoff_seconds
        )
        self.stratum_listen_backlog = stratum_config.listen_backlog
        self.stratum_bind_retry_seconds = stratum_config.bind_retry_seconds
        self.payout_address_cache_max_entries = stratum_config.payout_address_cache_max_entries
        self.payout_address_cache_ttl_seconds = stratum_config.payout_address_cache_ttl_seconds
        self.coinbase_tag_hex = self.config.coinbase_tag_hex
        self.share_difficulty = stratum_config.share_difficulty
        self.vardiff_config = stratum_config.vardiff_config
        self.listener_profiles = list(stratum_config.listener_profiles)
        self.default_share_weight = stratum_config.default_share_weight
        self.share_weights_by_username = dict(stratum_config.share_weights_by_username)
        self.username_fallback_address = stratum_config.username_fallback_address
        self.min_ready_miners = job_config.min_ready_miners
        self.signing_seed_hex = ledger_config.signing_seed_hex
        self.ledger_attestation_signing_seed_hex = ledger_config.attestation_signing_seed_hex
        self.ledger_writer_public_key_hex = ledger_config.writer_public_key_hex
        self.evidence_path = audit_config.evidence_path
        self.audit_dir = audit_config.directory
        self.audit_share_segment_size = audit_config.share_segment_size
        self.audit_live_bundle_retention = audit_config.live_bundle_retention
        self.audit_candidate_retention_seconds = audit_config.candidate_retention_seconds
        self.ctv_broadcast_attempt_detail_limit = ctv_config.broadcast_attempt_detail_limit
        self.ctv_broadcast_retry_backoff_seconds = ctv_config.broadcast_retry_backoff_seconds
        self.audit_bind = audit_config.bind
        self.audit_port = audit_config.port
        self.stop_after_block = self.config.stop_after_block
        self.max_blocks = self.config.max_blocks
        self.version_mask_selection = self.resolve_version_rolling_mask(
            stratum_config.fallback_version_mask
        )
        self.version_mask = self.version_mask_selection.selected_mask
        self.writer_quiescence_timeout_seconds = (
            lifecycle_config.writer_quiescence_timeout_seconds
        )
        self.ledger = self.make_ledger()
        self._upgrade_legacy_audit_evidence()
        self._ctv_fanout_market_fee_rate_cache: dict[tuple[int | None, str | None], int] = {}
        self.lock = _ObservedRLock()
        self.clients: set[ClientState] = set()
        self.connection_limit_rejection_counts = {"global": 0, "username": 0}
        self.peak_active_connection_count = 0
        self.handler_thread_count = 0
        self.accept_resource_exhaustion_count = 0
        self.connection_setup_failure_count = 0
        self.pending_initial_jobs: dict[ClientState, PendingInitialJob] = {}
        self.initial_job_queue_rejection_count = 0
        self.initial_job_timeout_count = 0
        self.initial_job_cancelled_count = 0
        self.initial_job_coalesced_count = 0
        self.last_initial_job_delivery_monotonic: float | None = None
        self._p2mr_address_cache_lock = threading.Lock()
        self._p2mr_address_cache: OrderedDict[
            str, tuple[float, tuple[str, str]]
        ] = OrderedDict()
        self._p2mr_address_validation_inflight: dict[str, object] = {}
        self.jobs: dict[str, PrismJobContext] = {}
        # Share-path accounting is deliberately disjoint from the coordinator
        # control-plane lock. The submission owner holds the process-wide
        # deduplication index for exact replays across sessions.
        self.recent_share_keys: set[tuple[object, ...]] = set()
        self._share_accounting_lock = threading.Lock()
        self.connection_counter = 0
        self.job_counter = 0
        self.accepted_block_count = 0
        # A durable outbox terminal update can fail after the accepted-block
        # success tail has completed.  Same-process replay must not count or
        # announce that hash twice; a fresh process intentionally starts with
        # an empty set and reconstructs its process-local count from replay.
        self._accounted_accepted_block_hashes: set[str] = set()
        self.started_monotonic = time.monotonic()
        self.submitted_share_count = 0
        self.stale_share_count = 0
        self.duplicate_share_count = 0
        self.low_difficulty_share_count = 0
        self.collection_block_submission_count = 0
        self.grace_credited_share_count = 0
        self.idle_retarget_count = 0
        self._ensure_vardiff_idle_state()
        self.rejection_counts_by_reason = {reason: 0 for reason in PRISM_REJECTION_REASON_IDS}
        # Per-worker share accounting with a bounded label set; see
        # worker_metric_label for the admission rule.
        self.worker_metrics_lock = threading.Lock()
        self.worker_share_counts: dict[str, dict[str, int]] = {}
        self.worker_rejection_counts: dict[tuple[str, str], int] = {}
        # Globally insertion ordered, with per-connection indexes for the
        # independent TTL and capacity limits. Prior-tip entries never consume
        # the same-tip cap while stale-grace still protects them.
        self.evicted_job_graveyard: OrderedDict[str, EvictedJobEntry] = OrderedDict()
        self.evicted_jobs_by_connection: dict[int, OrderedDict[str, None]] = {}
        self.evicted_same_tip_by_connection: dict[int, OrderedDict[str, None]] = {}
        self.evicted_same_tip_job_ids: OrderedDict[str, None] = OrderedDict()
        self.evicted_job_index_tip_hash: str | None = None
        self.evicted_job_next_prune_monotonic = 0.0
        self.evicted_job_expiration_counts = {
            job_class: 0 for job_class in PRISM_EVICTED_JOB_CLASSES
        }
        self.evicted_job_capacity_eviction_counts = {
            scope: 0 for scope in PRISM_EVICTED_JOB_CAPACITY_SCOPES
        }
        self.evicted_job_submit_counts = {
            outcome: 0 for outcome in PRISM_EVICTED_JOB_SUBMIT_OUTCOMES
        }
        # Block candidates are landed by a dedicated submitter thread so a
        # winning share's ack (and every other client's) never waits on
        # audit/persist/submitblock; see enqueue_block_candidate.
        self.block_candidate_queue: queue.Queue[PrismBlockCandidate] = queue.Queue(
            maxsize=MAX_PENDING_BLOCK_CANDIDATES
        )
        self.block_candidates_dropped = 0
        self.block_candidate_wakeups_coalesced = 0
        self.block_candidate_retry_count = 0
        self.block_candidate_poisoned_count = 0
        self.block_candidate_retry_initial_seconds = (
            DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS
        )
        self.block_candidate_retry_max_seconds = DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS
        self.block_candidate_retry_delays: dict[str, float] = {}
        self._block_candidate_finalize_retries: dict[str, tuple[bool, str]] = {}
        # A block candidate that loses its tip race (or fails to submit) is a
        # BLOCK-path event, not a share rejection: under the async model the
        # share was already accepted and credited, so it must not touch the
        # share-reject counters (that would inflate stale_share_percent with
        # block-race losses). Tracked here by reason instead.
        self.block_candidate_abandoned_counts: dict[str, int] = {}
        # Accepted shares drain through a bounded group-commit writer.  A
        # submitting client waits on its entry's completion event, making the
        # database commit the acknowledgement boundary without paying one
        # process/transaction round trip per share during bursts.
        self.share_append_queue: queue.Queue[PendingShareAppend] = queue.Queue(
            maxsize=MAX_PENDING_SHARE_APPENDS
        )
        self.share_commit_batch_size = ledger_config.share_commit_batch_size
        self.share_commit_linger_seconds = ledger_config.share_commit_linger_seconds
        self.share_commit_timeout_seconds = ledger_config.share_commit_timeout_seconds
        self.share_writer_active = False
        self.share_append_failure_count = 0
        # Retain the historical recovery-file reader for clean upgrades from a
        # release that could acknowledge before Postgres commit.  New shares
        # are never written here: an unavailable ledger produces no success
        # acknowledgement and an exact retry is idempotent.
        self.share_recovery_path = ledger_config.share_recovery_path
        self.share_recovery_lock = threading.Lock()
        self.shares_recovered_to_disk = 0
        self.shares_replayed = 0
        self.reorg_inactive_block_count = 0
        self.reorg_reactivated_block_count = 0
        self.reorg_reconcile_skip_count = 0
        self.reorg_reconcile_error_count = 0
        self.matured_payout_count = 0
        # The full accepted-block bundle is durable in the audit store.  Keeping
        # it here only to derive one metric pinned the complete share window for
        # the lifetime of the coordinator.
        self.latest_coinbase_size_bytes: int | None = None
        self.last_reorg_reconciled_tip_hash: str | None = None
        self.last_reorg_reconciled_trusted = False
        self.last_reorg_reconciled_monotonic: float | None = None
        self._prism_payout_policy_cache: dict[str, object] | None = None
        self._ensure_job_cache_state()
        self._ensure_reorg_reconciler_service()
        self.stop_event = threading.Event()
        self._shutdown_controller = CoordinatorShutdownController(
            self.writer_quiescence_timeout_seconds
        )
        # Liveness watchdog: each monitored loop stamps a monotonic heartbeat;
        # if any goes stale past the timeout the process exits non-zero so the
        # container/systemd restart policy recovers a *hung* coordinator (a
        # healthcheck alone does not restart it under plain compose).
        self._heartbeats: dict[str, float] = {}
        self._watchdog_pauses: dict[str, int] = {}
        self._heartbeats_lock = threading.Lock()
        self.watchdog_enabled = lifecycle_config.watchdog_enabled
        self.watchdog_timeout_seconds = lifecycle_config.watchdog_timeout_seconds
        self.watchdog_interval_seconds = lifecycle_config.watchdog_interval_seconds
        self._ctv_runtime_init_lock = threading.Lock()
        self._ctv_runtime = self._make_ctv_runtime_service(
            CtvRuntimeConfig.from_coordinator_config(ctv_config)
        )
        self._ensure_block_candidate_service()
        self._ensure_block_finalization_service()
        self._background_services = self._make_background_service_registry()

    def _ensure_share_hot_path_state(self) -> None:
        """Backfill dedicated accounting state for lightweight embedders."""
        if hasattr(self, "_share_accounting_lock"):
            return
        with _HOT_PATH_LOCK_INITIALIZATION_LOCK:
            if not hasattr(self, "_share_accounting_lock"):
                self._share_accounting_lock = threading.Lock()

    @staticmethod
    def _client_vardiff_lock(client: ClientState) -> threading.RLock:
        return client_vardiff_lock(client)

    def _reserve_recent_share_key(self, share_key: tuple[object, ...]) -> bool:
        self._ensure_share_hot_path_state()
        return self._ensure_share_submission_service().recent_shares.reserve(
            share_key  # type: ignore[arg-type]
        )

    def _forget_recent_share_key(self, share_key: tuple[object, ...]) -> None:
        self._ensure_share_submission_service().recent_shares.release(
            share_key  # type: ignore[arg-type]
        )

    def record_rejection(self, reason: str, *, worker: str | None = None) -> None:
        if reason not in PRISM_REJECTION_REASON_IDS:
            raise ValueError(f"unknown PRISM rejection reason: {reason}")
        self._ensure_share_hot_path_state()
        with self._share_accounting_lock:
            counts = getattr(self, "rejection_counts_by_reason", None)
            if counts is None:
                counts = {reason_id: 0 for reason_id in PRISM_REJECTION_REASON_IDS}
                self.rejection_counts_by_reason = counts
            counts[reason] = int(counts.get(reason, 0)) + 1
            if reason in {PRISM_REJECTION_STALE_JOB, PRISM_REJECTION_UNKNOWN_JOB, PRISM_REJECTION_BLOCK_STALE}:
                self.stale_share_count += 1
            elif reason == PRISM_REJECTION_DUPLICATE_SHARE:
                self.duplicate_share_count += 1
            elif reason == PRISM_REJECTION_LOW_DIFFICULTY:
                self.low_difficulty_share_count += 1
        if worker is not None:
            self._ensure_worker_metrics_state()
            with self.worker_metrics_lock:
                label = self._worker_metric_label_locked(worker)
                key = (label, reason)
                self.worker_rejection_counts[key] = (
                    int(self.worker_rejection_counts.get(key, 0)) + 1
                )

    def reject_stratum(self, code: int, reason: str, message: str, *, worker: str | None = None) -> None:
        self.record_rejection(reason, worker=worker)
        raise StratumError(code, message, reason=reason)

    def worker_metric_label(self, worker: str) -> str:
        """Metric label for one worker, from a bounded label set.

        The label is the stratum username as authorized (payout address plus
        optional worker suffix). Usernames are miner-supplied, so the set of
        distinct labels is capped: new workers past the cap aggregate into the
        overflow label instead of growing metric cardinality without bound.
        """
        self._ensure_worker_metrics_state()
        with self.worker_metrics_lock:
            return self._worker_metric_label_locked(worker)

    def _worker_metric_label_locked(self, worker: str) -> str:
        label = worker or "_unauthenticated"
        if len(label) > 128:
            label = label[:128]
        share_counts = self.worker_share_counts
        if label in share_counts:
            return label
        limit = getattr(self, "worker_metrics_limit", DEFAULT_PRISM_WORKER_METRICS_LIMIT)
        if len(share_counts) >= max(0, int(limit)):
            label = PRISM_WORKER_METRICS_OVERFLOW_LABEL
        share_counts.setdefault(label, {"submitted": 0, "accepted": 0, "grace": 0})
        return label

    def note_worker_submitted_share(self, worker: str) -> None:
        self._ensure_worker_metrics_state()
        with self.worker_metrics_lock:
            label = self._worker_metric_label_locked(worker)
            self.worker_share_counts[label]["submitted"] += 1

    def note_worker_accepted_share(self, worker: str, credit_policy: str | None) -> None:
        self._ensure_worker_metrics_state()
        with self.worker_metrics_lock:
            label = self._worker_metric_label_locked(worker)
            counts = self.worker_share_counts[label]
            counts["accepted"] += 1
            if credit_policy == PRISM_CREDIT_POLICY_STALE_GRACE:
                counts["grace"] += 1
        if credit_policy == PRISM_CREDIT_POLICY_STALE_GRACE:
            self._ensure_share_hot_path_state()
            with self._share_accounting_lock:
                self.grace_credited_share_count = (
                    int(getattr(self, "grace_credited_share_count", 0)) + 1
                )

    @staticmethod
    def prometheus_label_value(value: str) -> str:
        return (
            value.replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace('"', '\\"')
        )

    @staticmethod
    def rejection_reason_ids() -> tuple[str, ...]:
        return PRISM_REJECTION_REASON_IDS

    def resolve_version_rolling_mask(self, fallback_mask: int) -> direct_stratum.VersionRollingMaskSelection:
        try:
            template = self.rpc.call("getblocktemplate", [{"rules": qbit_gbt_rules(self.qbit_chain)}])
            if not isinstance(template, dict):
                raise RuntimeError("getblocktemplate returned non-object")
        except Exception as exc:
            return direct_stratum.VersionRollingMaskSelection(
                fallback_mask,
                "fallback",
                f"probe_error:{exc}",
            )
        try:
            return direct_stratum.select_version_rolling_mask(template, fallback_mask)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    def parse_share_weights(self) -> dict[str, int]:
        config = getattr(self, "config", None)
        if config is not None:
            return dict(config.stratum.share_weights_by_username)
        return load_share_weights()

    def share_weight_for_worker(self, worker: WorkerIdentity) -> int:
        return self.share_weights_by_username.get(
            worker.username,
            self.share_weights_by_username.get(worker.payout_address, self.default_share_weight),
        )

    def _ensure_audit_artifact_store(self) -> AuditArtifactStore:
        init_lock = self.__dict__.setdefault(
            "_audit_artifact_store_init_lock",
            threading.Lock(),
        )
        assert isinstance(init_lock, type(threading.Lock()))
        with init_lock:
            audit_dir = Path(
                self.__dict__.get("audit_dir", Path("prism-audit"))
            )
            evidence_path = Path(
                self.__dict__.get(
                    "evidence_path",
                    audit_dir / "prism-live-stratum-evidence.json",
                )
            )
            live_retention = int(
                self.__dict__.get("audit_live_bundle_retention", 5)
            )
            candidate_retention = int(
                self.__dict__.get(
                    "audit_candidate_retention_seconds",
                    24 * 60 * 60,
                )
            )
            share_segment_size = int(
                self.__dict__.get(
                    "audit_share_segment_size",
                    DEFAULT_AUDIT_SHARE_SEGMENT_SIZE,
                )
            )
            store = self.__dict__.get("_audit_artifact_store")
            if not isinstance(store, AuditArtifactStore):
                store = AuditArtifactStore(
                    AuditArtifactConfig(
                        root=audit_dir,
                        evidence_path=evidence_path,
                        live_bundle_retention=live_retention,
                        candidate_retention_seconds=candidate_retention,
                        share_segment_size=share_segment_size,
                    ),
                    canonicalizer=canonical_bundle_bytes,
                )
                self.__dict__["_audit_artifact_store"] = store
                if "_audit_latest_evidence_seed" in self.__dict__:
                    store.set_latest_evidence_for_compatibility(
                        self.__dict__.pop("_audit_latest_evidence_seed")
                    )
            else:
                updates: dict[str, Any] = {}
                if store.root != audit_dir.expanduser().absolute().resolve():
                    updates["root"] = audit_dir
                expected_evidence = (
                    evidence_path.expanduser().absolute().parent.resolve()
                    / evidence_path.name
                )
                if store.evidence_path != expected_evidence:
                    updates["evidence_path"] = evidence_path
                if store.live_bundle_retention != live_retention:
                    updates["live_bundle_retention"] = live_retention
                if store.candidate_retention_seconds != candidate_retention:
                    updates["candidate_retention_seconds"] = candidate_retention
                if store.share_segment_size != share_segment_size:
                    updates["share_segment_size"] = share_segment_size
                if updates:
                    store.reconfigure(**updates)
            return store

    def _upgrade_legacy_audit_evidence(self) -> None:
        store = self._ensure_audit_artifact_store()
        with self._ensure_payout_state_service().balance_mutation_lock:
            with store.publication_order_guard():
                legacy = store.legacy_evidence_identity()
                if legacy is None:
                    return
                reader = getattr(self.ledger, "pool_block_state", None)
                floor_reader = getattr(
                    self.ledger,
                    "audit_publication_sequence_floor",
                    None,
                )
                if not callable(reader) or not callable(floor_reader):
                    store.invalidate_unprovable_legacy_evidence()
                    return
                state = reader(block_hash=legacy.block_hash)
                if not isinstance(state, dict):
                    store.invalidate_unprovable_legacy_evidence()
                    return
                sequence = state.get("audit_publication_sequence")
                state_block_hash = state.get("block_hash")
                state_block_height = state.get("block_height")
                if (
                    sequence is None
                    or isinstance(sequence, bool)
                    or not isinstance(sequence, int)
                    or sequence <= 0
                    or not isinstance(state_block_hash, str)
                    or state_block_hash != legacy.block_hash
                    or isinstance(state_block_height, bool)
                    or not isinstance(state_block_height, int)
                    or state_block_height != legacy.block_height
                    or str(state.get("chain_state") or "") != "confirmed"
                    or str(state.get("maturity_state") or "")
                    not in {"immature", "mature"}
                ):
                    store.invalidate_unprovable_legacy_evidence()
                    return
                publication_floor_sequence = floor_reader()
                store.adopt_legacy_publication_identity(
                    AuditPublicationIdentity(
                        int(sequence),
                        legacy.block_height,
                        legacy.block_hash,
                    ),
                    publication_floor_sequence=publication_floor_sequence,
                )

    def _audit_publication_identity(
        self,
        *,
        block_hash: str,
        block_height: int,
        confirmation: Mapping[str, Any],
    ) -> AuditPublicationIdentity:
        sequence = confirmation.get("audit_publication_sequence")
        if sequence is None:
            # Compatibility-only fake ledgers in unit tests predate the durable
            # ordinal.  Production Postgres and the memory ledger always return
            # it; never synthesize for an identified durable backend.
            if str(confirmation.get("backend") or "") not in {"", "fake"}:
                raise RuntimeError(
                    "ledger confirmation omitted audit publication sequence"
                )
            sequences = self.__dict__.setdefault(
                "_compat_audit_publication_sequences",
                {},
            )
            assert isinstance(sequences, dict)
            sequence = sequences.get(block_hash)
            if sequence is None:
                sequence = max(
                    [
                        self._ensure_audit_artifact_store().publication_sequence_floor(),
                        *(int(value) for value in sequences.values()),
                    ]
                ) + 1
                sequences[block_hash] = sequence
        if isinstance(sequence, bool) or not isinstance(sequence, int):
            raise RuntimeError("ledger confirmation returned invalid publication sequence")
        if isinstance(block_height, bool) or not isinstance(block_height, int):
            raise RuntimeError("ledger confirmation returned invalid block height")
        canonical_block_hash = str(block_hash).lower()
        if canonical_block_hash != block_hash:
            raise RuntimeError("ledger confirmation returned non-canonical block hash")
        return AuditPublicationIdentity(
            sequence,
            block_height,
            canonical_block_hash,
        )

    def make_ledger(self) -> SingleWriterShareLedger | PsqlShareLedger:
        config = getattr(self, "config", None)
        ledger_config = config.ledger if config is not None else None
        psql_command = (
            ledger_config.psql_command
            if ledger_config is not None
            else env_optional("PRISM_POSTGRES_PSQL_COMMAND") or ""
        )
        database_url = (
            ledger_config.database_url or ""
            if ledger_config is not None
            else env_optional("PRISM_DATABASE_URL") or ""
        )
        if not psql_command and database_url:
            psql_command = f"psql {shlex.quote(database_url)}"
        if not psql_command:
            allow_memory_ledger = (
                ledger_config.allow_memory_ledger
                if ledger_config is not None
                else env_bool("PRISM_ALLOW_MEMORY_LEDGER", "0")
            )
            if not allow_memory_ledger:
                raise SystemExit(
                    "PRISM_DATABASE_URL or PRISM_POSTGRES_PSQL_COMMAND is required; "
                    "set PRISM_ALLOW_MEMORY_LEDGER=1 only for local tests"
                )
            return SingleWriterShareLedger(
                ctv_broadcast_attempt_detail_limit=getattr(
                    self,
                    "ctv_broadcast_attempt_detail_limit",
                    DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
                ),
                ctv_broadcast_retry_backoff_seconds=getattr(
                    self,
                    "ctv_broadcast_retry_backoff_seconds",
                    DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
                ),
            )
        writer_session_token = (
            ledger_config.writer_session_token
            if ledger_config is not None
            else env_optional("PRISM_LEDGER_WRITER_SESSION_TOKEN")
        )
        if (
            ledger_config is None
            and writer_session_token is not None
            and not env_bool("PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN", "0")
        ):
            raise SystemExit(
                "PRISM_LEDGER_WRITER_SESSION_TOKEN requires "
                "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN=1 for local tests"
            )
        audit_store = self._ensure_audit_artifact_store()
        return PsqlShareLedger(
            psql_command=psql_command,
            database_url=database_url or None,
            native_client_mode=(
                ledger_config.native_client_mode
                if ledger_config is not None
                else env("PRISM_POSTGRES_NATIVE_CLIENT", "auto")
            ),
            writer_id=(
                ledger_config.writer_id
                if ledger_config is not None
                else env("PRISM_LEDGER_WRITER_ID", "prism-coordinator")
            ),
            writer_epoch=(
                ledger_config.writer_epoch
                if ledger_config is not None
                else env_int("PRISM_LEDGER_WRITER_EPOCH", 1)
            ),
            writer_session_token=writer_session_token,
            initialize_schema=(
                ledger_config.initialize_schema
                if ledger_config is not None
                else env("PRISM_POSTGRES_INIT_SCHEMA", "0") in {"1", "true", "yes"}
            ),
            lease_ttl_seconds=(
                ledger_config.lease_ttl_seconds
                if ledger_config is not None
                else env_positive_float("PRISM_LEDGER_LEASE_TTL_SECONDS", 60.0)
            ),
            read_concurrency=(
                ledger_config.read_concurrency
                if ledger_config is not None
                else env_positive_int("PRISM_POSTGRES_READ_CONCURRENCY", 4)
            ),
            accepted_stats_cache_seconds=(
                ledger_config.accepted_stats_cache_seconds
                if ledger_config is not None
                else env_nonnegative_float("PRISM_ACCEPTED_STATS_CACHE_SECONDS", 60.0)
            ),
            reward_window_cache_seconds=(
                ledger_config.reward_window_cache_seconds
                if ledger_config is not None
                else env_nonnegative_float(
                    "PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS",
                    30.0,
                )
            ),
            audit_artifact_store=audit_store,
            ctv_broadcast_attempt_detail_limit=getattr(
                self,
                "ctv_broadcast_attempt_detail_limit",
                DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
            ),
            ctv_broadcast_retry_backoff_seconds=getattr(
                self,
                "ctv_broadcast_retry_backoff_seconds",
                DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
            ),
        )

    def load_trusted_ledger_writer_public_key(self) -> str | None:
        config = getattr(self, "config", None)
        if config is not None:
            return config.ledger.writer_public_key_hex
        configured = env_optional("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX")
        if configured is not None:
            return validate_hex(configured, name="PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX", expected_bytes=32)
        if env_bool("PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY", "0"):
            return None
        raise SystemExit(
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX is required; "
            "set PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY=1 only for local tests"
        )

    def prism_payout_policy(self) -> dict[str, object]:
        cached = getattr(self, "_prism_payout_policy_cache", None)
        if cached is not None:
            return cached

        config = getattr(self, "config", None)
        if config is None:
            policy = default_prism_payout_policy()
            fee_bps_raw = env_optional("PRISM_POOL_FEE_BPS")
            fee_enabled = env_bool("PRISM_POOL_FEE_ENABLED", "0")
            fee_address = env_optional("PRISM_POOL_FEE_ADDRESS")
            fee_program_hex = env_optional("PRISM_POOL_FEE_P2MR_PROGRAM_HEX")
            fee_recipient_id = env_optional("PRISM_POOL_FEE_RECIPIENT_ID")
            fee_order_key = env_optional("PRISM_POOL_FEE_ORDER_KEY")
        else:
            job_config = config.jobs
            policy = default_prism_payout_policy(
                environ=dict(job_config.payout_environment)
            )
            fee_bps_raw = job_config.pool_fee_bps_raw
            fee_enabled = env_bool(
                "PRISM_POOL_FEE_ENABLED",
                "0",
                environ=(
                    {}
                    if job_config.pool_fee_enabled_raw is None
                    else {"PRISM_POOL_FEE_ENABLED": job_config.pool_fee_enabled_raw}
                ),
            )
            fee_address = job_config.pool_fee_address
            fee_program_hex = job_config.pool_fee_program_hex
            fee_recipient_id = job_config.pool_fee_recipient_id
            fee_order_key = job_config.pool_fee_order_key
        has_fee_config = any(
            value is not None
            for value in (fee_bps_raw, fee_address, fee_program_hex, fee_recipient_id, fee_order_key)
        )
        if not fee_enabled:
            if has_fee_config:
                raise SystemExit("set PRISM_POOL_FEE_ENABLED=1 when configuring pool fees")
            self._prism_payout_policy_cache = policy
            return policy
        if fee_bps_raw is None:
            raise SystemExit("PRISM_POOL_FEE_BPS is required when pool fees are enabled")

        try:
            fee_bps = int(fee_bps_raw)
        except ValueError as exc:
            raise SystemExit("PRISM_POOL_FEE_BPS must be an integer") from exc
        if fee_bps < 0 or fee_bps > 10_000:
            raise SystemExit("PRISM_POOL_FEE_BPS must be between 0 and 10000")
        if (fee_address is None) == (fee_program_hex is None):
            raise SystemExit("set exactly one of PRISM_POOL_FEE_ADDRESS or PRISM_POOL_FEE_P2MR_PROGRAM_HEX")

        if fee_address is not None:
            validation = self.rpc.call("validateaddress", [fee_address])
            if not isinstance(validation, dict) or not validation.get("isvalid"):
                raise SystemExit(f"PRISM_POOL_FEE_ADDRESS is not a valid qbit address: {fee_address}")
            script = str(validation.get("scriptPubKey") or "")
            if not script.startswith("5220") or len(script) != 68:
                raise SystemExit("PRISM_POOL_FEE_ADDRESS must resolve to a P2MR script")
            fee_policy = {
                "fee_bps": fee_bps,
                "recipient_id": fee_address,
                "order_key": fee_address,
                "p2mr_program_hex": script[4:],
            }
        else:
            program_hex = validate_hex(
                fee_program_hex or "",
                name="PRISM_POOL_FEE_P2MR_PROGRAM_HEX",
                expected_bytes=32,
            )
            if fee_recipient_id is None:
                raise SystemExit("PRISM_POOL_FEE_RECIPIENT_ID is required with PRISM_POOL_FEE_P2MR_PROGRAM_HEX")
            fee_policy = {
                "fee_bps": fee_bps,
                "recipient_id": fee_recipient_id,
                "order_key": fee_order_key or fee_recipient_id,
                "p2mr_program_hex": program_hex,
            }

        policy["pool_fee_policy"] = fee_policy
        self._prism_payout_policy_cache = policy
        return policy

    def prism_ctv_settlement_config(
        self,
        *,
        block_height: int | None = None,
        parent_hash: str | None = None,
    ) -> dict[str, object] | None:
        coordinator_config = getattr(self, "config", None)
        ctv_config = coordinator_config.ctv if coordinator_config is not None else None
        settlement_environment = (
            dict(ctv_config.settlement_environment) if ctv_config is not None else None
        )
        settlement_enabled = (
            env_bool(
                "PRISM_CTV_SETTLEMENT_ENABLED",
                "0",
                environ=(
                    {}
                    if ctv_config.settlement_enabled_raw is None
                    else {
                        "PRISM_CTV_SETTLEMENT_ENABLED": ctv_config.settlement_enabled_raw
                    }
                ),
            )
            if ctv_config is not None
            else env_bool("PRISM_CTV_SETTLEMENT_ENABLED", "0")
        )
        if not settlement_enabled:
            return None
        direct_floor_sats = (
            env_positive_int_with_legacy(
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_SATS",
                DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
                environ=settlement_environment,
            )
            if settlement_environment is not None
            else env_positive_int_with_legacy(
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_SATS",
                DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
            )
        )
        reserved_coinbase_outputs = (
            env_int(
                "PRISM_RESERVED_COINBASE_OUTPUTS", 0, environ=settlement_environment
            )
            if settlement_environment is not None
            else env_int("PRISM_RESERVED_COINBASE_OUTPUTS", 0)
        )
        if reserved_coinbase_outputs < 0:
            raise SystemExit("PRISM_RESERVED_COINBASE_OUTPUTS must be non-negative")
        config: dict[str, object] = {
            "direct_floor_sats": direct_floor_sats,
            "config": {
                "max_coinbase_settlement_outputs": (
                    env_positive_int(
                        "PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS",
                        DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS,
                        environ=settlement_environment,
                    )
                    if settlement_environment is not None
                    else env_positive_int(
                        "PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS",
                        DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS,
                    )
                ),
                "max_direct_coinbase_outputs": (
                    env_positive_int(
                        "PRISM_MAX_DIRECT_COINBASE_OUTPUTS",
                        DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS,
                        environ=settlement_environment,
                    )
                    if settlement_environment is not None
                    else env_positive_int(
                        "PRISM_MAX_DIRECT_COINBASE_OUTPUTS",
                        DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS,
                    )
                ),
                "max_fanout_recipients_per_transaction": (
                    env_positive_int(
                        "PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION",
                        DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION,
                        environ=settlement_environment,
                    )
                    if settlement_environment is not None
                    else env_positive_int(
                        "PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION",
                        DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION,
                    )
                ),
                "reserved_coinbase_outputs": reserved_coinbase_outputs,
            },
        }
        config["fanout_fee_rate_policy"] = {
            "market_fee_rate_sats_per_1000_weight": self.ctv_fanout_market_fee_rate_bits_per_1000_weight(
                block_height=block_height,
                parent_hash=parent_hash,
            ),
            "premium_bps": (
                env_positive_int(
                    "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS",
                    12_000,
                    environ=settlement_environment,
                )
                if settlement_environment is not None
                else env_positive_int("PRISM_CTV_FANOUT_FEE_PREMIUM_BPS", 12_000)
            ),
        }
        return config

    def ctv_fanout_market_fee_rate_bits_per_1000_weight(
        self,
        *,
        block_height: int | None = None,
        parent_hash: str | None = None,
    ) -> int:
        coordinator_config = getattr(self, "config", None)
        ctv_config = coordinator_config.ctv if coordinator_config is not None else None
        settlement_environment = (
            dict(ctv_config.settlement_environment) if ctv_config is not None else None
        )
        configured_rate = (
            env_optional_positive_int_with_legacy(
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_SATS_PER_1000_WEIGHT",
                environ=settlement_environment,
            )
            if settlement_environment is not None
            else env_optional_positive_int_with_legacy(
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_SATS_PER_1000_WEIGHT",
            )
        )
        if configured_rate is not None:
            return configured_rate
        fee_rate_cache = getattr(self, "_ctv_fanout_market_fee_rate_cache", None)
        if fee_rate_cache is None:
            fee_rate_cache = {}
            self._ctv_fanout_market_fee_rate_cache = fee_rate_cache
        cache_key = (block_height, parent_hash)
        if cache_key in fee_rate_cache:
            return fee_rate_cache[cache_key]
        try:
            estimate_target_blocks = (
                env_positive_int(
                    "PRISM_CTV_FANOUT_FEE_ESTIMATE_TARGET_BLOCKS",
                    2,
                    environ=settlement_environment,
                )
                if settlement_environment is not None
                else env_positive_int("PRISM_CTV_FANOUT_FEE_ESTIMATE_TARGET_BLOCKS", 2)
            )
            estimate = self.rpc.call("estimatesmartfee", [estimate_target_blocks])
            if not isinstance(estimate, dict):
                raise RuntimeError("estimatesmartfee returned non-object")
            errors = estimate.get("errors")
            if errors:
                raise RuntimeError(f"estimatesmartfee returned errors: {errors}")
            feerate = Decimal(str(estimate.get("feerate", "")))
            if not feerate.is_finite() or feerate <= 0:
                raise RuntimeError(f"estimatesmartfee returned invalid feerate: {estimate.get('feerate')!r}")
            rate = int((feerate * Decimal(100_000_000)).to_integral_value(rounding=ROUND_CEILING))
            if rate <= 0:
                raise RuntimeError("estimatesmartfee rounded to a non-positive rate")
        except Exception as exc:
            raise RuntimeError(
                "unable to compute PRISM CTV fanout fee rate; set "
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT or fix "
                f"estimatesmartfee: {exc}"
            ) from exc
        fee_rate_cache[cache_key] = rate
        return rate

    def _ensure_share_writer_service(self) -> ShareWriter:
        service = self.__dict__.get("_share_writer_service")
        if service is not None:
            return service

        init_lock = self.__dict__.setdefault(
            "_share_writer_service_init_lock",
            threading.Lock(),
        )
        with init_lock:
            service = self.__dict__.get("_share_writer_service")
            if service is not None:
                return service
            append_queue = self.__dict__.get("share_append_queue")
            if append_queue is None:
                append_queue = queue.Queue(maxsize=MAX_PENDING_SHARE_APPENDS)
            floor_lock = self.__dict__.get("_pending_share_commit_lock")
            if floor_lock is None:
                floor_lock = threading.Lock()
            floor = self.__dict__.get("_pending_share_commit_floor")
            if floor is None:
                floor = {}
            recovery_lock = self.__dict__.get("share_recovery_lock")
            if recovery_lock is None:
                recovery_lock = threading.Lock()
            service = ShareWriter(
                ShareWriterConfig(
                    batch_size=int(
                        self.__dict__.get(
                            "share_commit_batch_size",
                            DEFAULT_SHARE_COMMIT_BATCH_SIZE,
                        )
                    ),
                    linger_seconds=float(
                        self.__dict__.get(
                            "share_commit_linger_seconds",
                            DEFAULT_SHARE_COMMIT_LINGER_MILLISECONDS / 1000.0,
                        )
                    ),
                    enqueue_timeout_seconds=float(
                        self.__dict__.get(
                            "share_commit_timeout_seconds",
                            DEFAULT_SHARE_COMMIT_TIMEOUT_SECONDS,
                        )
                    ),
                    pending_floor_warn_seconds=PRISM_PENDING_SHARE_COMMIT_WARN_SECONDS,
                    recovery_path=self.__dict__.get("share_recovery_path"),
                ),
                ShareWriterPorts(
                    ledger=lambda: self.ledger,
                    writer_operation=lambda component: self._writer_operation(component),
                    reserve_writer=lambda component: (
                        self._ensure_shutdown_controller().reserve_writer(component)
                    ),
                    writer_admission_closed=lambda: (
                        self._ensure_shutdown_controller().writer_admission_closed()
                    ),
                    has_active_writer=lambda components: (
                        self._ensure_shutdown_controller().has_active_writer(components)
                    ),
                    heartbeat=lambda name: self._record_heartbeat(name),
                    monotonic=lambda: time.monotonic(),
                    wall_time_ms=lambda: now_ms(),
                    stop_is_set=lambda: bool(
                        getattr(self, "stop_event", threading.Event()).is_set()
                    ),
                    stop_wait=lambda delay: bool(
                        getattr(self, "stop_event", threading.Event()).wait(delay)
                    ),
                    log=lambda message: print(message, flush=True),
                    log_exception=traceback.print_exc,
                    hot_path_log_enabled=lambda: bool(
                        getattr(self, "hot_path_log_enabled", False)
                    ),
                ),
                append_queue=append_queue,
                floor_lock=floor_lock,
                floor=floor,
                recovery_lock=recovery_lock,
                active=bool(self.__dict__.get("share_writer_active", False)),
                append_failures=int(
                    self.__dict__.get("share_append_failure_count", 0)
                ),
                recovered_to_disk=int(
                    self.__dict__.get("shares_recovered_to_disk", 0)
                ),
                replayed=int(self.__dict__.get("shares_replayed", 0)),
            )
            self.__dict__["_share_writer_service"] = service
            for name in (
                "share_append_queue",
                "share_commit_batch_size",
                "share_commit_linger_seconds",
                "share_commit_timeout_seconds",
                "share_writer_active",
                "share_append_failure_count",
                "share_recovery_path",
                "share_recovery_lock",
                "shares_recovered_to_disk",
                "shares_replayed",
                "_pending_share_commit_lock",
                "_pending_share_commit_floor",
            ):
                self.__dict__.pop(name, None)
            return service

    def _ensure_block_candidate_service(self) -> BlockCandidateService:
        service = self.__dict__.get("_block_candidate_service")
        if service is not None:
            return service
        init_lock = self.__dict__.setdefault(
            "_block_candidate_service_init_lock",
            threading.Lock(),
        )
        with init_lock:
            service = self.__dict__.get("_block_candidate_service")
            if service is not None:
                return service
            candidate_queue = self.__dict__.get("block_candidate_queue")
            if candidate_queue is None:
                candidate_queue = queue.Queue(maxsize=MAX_PENDING_BLOCK_CANDIDATES)
            stop_event = self.__dict__.get("stop_event")
            if stop_event is None:
                stop_event = threading.Event()
                self.stop_event = stop_event
            service = BlockCandidateService(
                BlockCandidatePorts(
                    ledger=lambda: self.ledger,
                    stop_event=lambda: self.stop_event,
                    writer_operation=lambda component: self._writer_operation(component),
                    submit_candidate=lambda candidate: self.submit_block_candidate(
                        candidate
                    ),
                    reject_terminal_prepared=(
                        lambda candidate: self._reject_terminal_prepared_block_candidate(
                            candidate
                        )
                    ),
                    begin_preview=lambda block_hash, block_height: (
                        self._begin_accepted_block_payout_preview(
                            block_hash,
                            block_height=block_height,
                        )
                    ),
                    clear_preview=lambda block_hash, invalidate: (
                        self._clear_accepted_block_payout_preview(
                            block_hash,
                            invalidate_published=invalidate,
                        )
                    ),
                    share_writer=lambda: self._ensure_share_writer_service(),
                    finish_pending_candidate=(
                        lambda pending: self._finish_pending_share_candidate(pending)
                    ),
                    refresh_after_accept=lambda client: (
                        self.refresh_jobs_after_pending_accepted_block(
                            client,
                            heartbeat_name="block_submitter",
                        )
                    ),
                    record_heartbeat=lambda name: self._record_heartbeat(name),
                    replay_entrypoint=lambda: self.replay_pending_block_candidates(),
                    submit_next_entrypoint=(
                        lambda timeout: self.submit_next_block_candidate(timeout=timeout)
                    ),
                    next_retry_delay=lambda block_hash: (
                        self._next_block_candidate_retry_delay(block_hash)
                    ),
                    log=lambda message: print(message, flush=True),
                ),
                candidate_queue=candidate_queue,
                retry_initial_seconds=float(
                    self.__dict__.get(
                        "block_candidate_retry_initial_seconds",
                        DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS,
                    )
                ),
                retry_max_seconds=float(
                    self.__dict__.get(
                        "block_candidate_retry_max_seconds",
                        DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS,
                    )
                ),
                retryable_reasons=PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS,
            )
            service.dropped = int(
                self.__dict__.get("block_candidates_dropped", 0)
            )
            service.wakeups_coalesced = int(
                self.__dict__.get("block_candidate_wakeups_coalesced", 0)
            )
            service.retries = int(
                self.__dict__.get("block_candidate_retry_count", 0)
            )
            service.poisoned = int(
                self.__dict__.get("block_candidate_poisoned_count", 0)
            )
            service.retry_delays = self.__dict__.get(
                "block_candidate_retry_delays",
                {},
            )
            service.finalize_retries = self.__dict__.get(
                "_block_candidate_finalize_retries",
                {},
            )
            service.abandoned_counts = self.__dict__.get(
                "block_candidate_abandoned_counts",
                {},
            )
            service.retry_candidate = self.__dict__.get(
                "_retry_block_candidate"
            )
            service.outcome = self.__dict__.get(
                "_block_candidate_outcome",
                threading.local(),
            )
            self.__dict__["_block_candidate_service"] = service
            for name in (
                "block_candidate_queue",
                "block_candidates_dropped",
                "block_candidate_wakeups_coalesced",
                "block_candidate_retry_count",
                "block_candidate_poisoned_count",
                "block_candidate_retry_initial_seconds",
                "block_candidate_retry_max_seconds",
                "block_candidate_retry_delays",
                "_block_candidate_finalize_retries",
                "block_candidate_abandoned_counts",
                "_retry_block_candidate",
                "_block_candidate_outcome",
            ):
                self.__dict__.pop(name, None)
            return service

    def _ensure_vardiff_service(self) -> VardiffService:
        service = self.__dict__.get("_vardiff_service")
        if service is not None:
            return service
        init_lock = self.__dict__.setdefault(
            "_vardiff_service_init_lock",
            threading.RLock(),
        )
        with init_lock:
            service = self.__dict__.get("_vardiff_service")
            if service is not None:
                return service
            initial = {
                name: self.__dict__.pop(name)
                for name in VARDIFF_COMPATIBILITY_FIELDS
                if name in self.__dict__
            }
            service = VardiffService(self)
            for name, value in initial.items():
                setattr(service, name, value)
            self.__dict__["_vardiff_service"] = service
            return service

    def _ensure_block_finalization_service(self) -> BlockFinalizationService:
        service = self.__dict__.get("_block_finalization_service")
        if service is not None:
            return service
        init_lock = self.__dict__.setdefault(
            "_block_finalization_service_init_lock",
            threading.Lock(),
        )
        with init_lock:
            service = self.__dict__.get("_block_finalization_service")
            if service is None:
                service = BlockFinalizationService(self)
                self.__dict__["_block_finalization_service"] = service
            return service

    def _share_submit_control_snapshot(
        self,
        client: ClientState,
        job_id: str,
    ) -> SubmitControlSnapshot:
        pool_closed, context, published_tip = self._submit_control_snapshot(
            client,
            job_id,
        )
        return SubmitControlSnapshot(
            pool_open=not pool_closed,
            active_context=context,
            published_tip=published_tip,
        )

    def _release_submit_share_key(self, share_key: tuple[str, str]) -> None:
        self._ensure_share_submission_service().recent_shares.release(share_key)

    def _note_collection_block_candidate(
        self,
        context: PrismJobContext,
        submission: Any,
    ) -> None:
        self._ensure_share_hot_path_state()
        with self._share_accounting_lock:
            self.collection_block_submission_count = (
                getattr(self, "collection_block_submission_count", 0) + 1
            )
        print(
            "prism coordinator: collection-mode block candidate settles "
            f"solver-pays-all miner={context.worker.payout_address} "
            f"hash={submission.block_hash_hex}",
            flush=True,
        )

    def _note_submit_accounting(
        self,
        worker_name: str,
        client: ClientState,
    ) -> None:
        self.note_worker_submitted_share(worker_name)
        self.note_vardiff_submitted_share(client)

    def _ensure_share_submission_service(self) -> ShareSubmissionService:
        service = self.__dict__.get("_share_submission_service")
        if service is not None:
            return service
        init_lock = self.__dict__.setdefault(
            "_share_submission_service_init_lock",
            threading.Lock(),
        )
        with init_lock:
            service = self.__dict__.get("_share_submission_service")
            if service is not None:
                return service
            initial_recent_shares = self.__dict__.pop("recent_share_keys", set())
            service = ShareSubmissionService(
                ShareSubmissionPorts(
                reject=lambda rejected, worker: self.reject_stratum(
                    rejected.code,
                    rejected.reason,
                    rejected.message,
                    worker=worker,
                ),
                control_snapshot=self._share_submit_control_snapshot,
                note_submitted=self._note_submit_accounting,
                retained_entry=lambda client, job_id: self.evicted_job_entry(
                    client,
                    job_id,
                ),
                live_tip=lambda: str(self.rpc.call("getbestblockhash")),
                stale_grace_eligible=(
                    lambda client, context, current_tip: (
                        self.context_eligible_for_stale_grace(
                            client,
                            context,
                            current_tip,
                        )
                    )
                ),
                assemble=lambda client, context, request: (
                    direct_stratum.assemble_submission(
                        context.job,
                        extranonce2_hex=request.extranonce2_hex,
                        ntime_hex=request.ntime_hex,
                        nonce_hex=request.nonce_hex,
                        version_bits_hex=request.version_bits_hex,
                        version_mask=client.version_mask,
                    )
                ),
                pending_share=lambda context, submission, ntime_hex, credit_policy: (
                    self.pending_share_from_submission(
                        context=context,
                        submission=submission,
                        ntime_hex=ntime_hex,
                        credit_policy=credit_policy,
                    )
                ),
                append_share=(
                    lambda client, context, submission, pending, policy, intent: (
                        self.append_accepted_share(
                            client,
                            context,
                            submission,
                            pending,
                            credit_policy=policy,
                            candidate_intent=intent,
                        )
                    )
                ),
                note_retained_submit=lambda policy: self.note_evicted_job_submit(
                    policy
                ),
                note_collection_candidate=(
                    lambda context, submission: self._note_collection_block_candidate(
                        context,
                        submission,
                    )
                ),
                ledger=lambda: self.ledger,
                share_writer=lambda: self._ensure_share_writer_service(),
                finish_pending_attempt=lambda pending: self._finish_pending_share_attempt(
                    pending
                ),
                submit_synchronous_candidate=(
                    lambda candidate, share_key, worker, retained, policy: (
                        self._submit_synchronous_credit_candidate(
                            candidate,
                            share_key=share_key,
                            worker_name=worker,
                            evicted_entry=retained,
                            credit_policy=policy,
                        )
                    )
                ),
                enqueue_candidate=lambda candidate: self.enqueue_block_candidate(
                    candidate
                ),
                log=lambda message: print(message, flush=True),
                log_exception=traceback.print_exc,
                ),
                extranonce2_size=int(self.extranonce2_size),
                recent_shares=RecentShareIndex(initial=initial_recent_shares),
            )
            self.__dict__["_share_submission_service"] = service
            return service

    def _ensure_job_cache_state(self) -> None:
        self._ensure_job_bundle_service()
        self._ensure_share_writer_service()
        if not hasattr(self, "_accounted_accepted_block_hashes"):
            self._accounted_accepted_block_hashes: set[str] = set()
        self._ensure_payout_state_service()
        self._ensure_observability_service()
        self._ensure_progress_health_service()

    def _ensure_job_bundle_service(self) -> JobBundleService:
        service = getattr(self, "_job_bundle_service", None)
        if service is not None:
            return service
        init_lock = self.__dict__.setdefault(
            "_job_bundle_service_init_lock",
            threading.Lock(),
        )
        with init_lock:
            service = getattr(self, "_job_bundle_service", None)
            if service is not None:
                return service
            repository = TemplateArtifactRepository(
                TemplateArtifactPorts(
                    fetch_template=lambda: self.rpc.call(
                        "getblocktemplate",
                        [{"rules": qbit_gbt_rules(getattr(self, "qbit_chain", "regtest"))}],
                    ),
                    fetch_bestblockhash=lambda: str(
                        self.rpc.call("getbestblockhash")
                    ),
                    newest_observed_tip=self._job_bundle_newest_observed_tip,
                    observe_tip=self._submit_tip_observation_for_refresh,
                    schedule_refresh_retry=self._schedule_tip_refresh_retry,
                    pinned_issuance_artifacts=(
                        self._job_bundle_pinned_issuance_artifacts
                    ),
                    repinned_issuance_artifacts=(
                        self._job_bundle_repinned_issuance_artifacts
                    ),
                    record_tip=lambda tip_hash: (
                        self._ensure_tip_refresh_service().observe_tip(tip_hash)
                    ),
                ),
                cache_seconds=float(
                    getattr(self, "template_cache_seconds", DEFAULT_PRISM_BLOCKPOLL_SECONDS)
                ),
                scale_network_difficulty=scaled_network_difficulty,
            )
            service = JobBundleService(
                JobBundleConfig(
                    cache_seconds=float(
                        getattr(
                            self,
                            "job_bundle_cache_seconds",
                            DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS,
                        )
                    ),
                    build_timeout_seconds=float(
                        getattr(
                            self,
                            "job_build_timeout_seconds",
                            DEFAULT_PRISM_JOB_BUILD_TIMEOUT_SECONDS,
                        )
                    ),
                    cancel_grace_seconds=float(
                        getattr(
                            self,
                            "job_build_cancel_grace_seconds",
                            DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS,
                        )
                    ),
                    min_ready_miners=int(getattr(self, "min_ready_miners", 3)),
                    extranonce2_size=int(getattr(self, "extranonce2_size", 8)),
                    share_difficulty=getattr(self, "share_difficulty", Decimal("1")),
                ),
                JobBundlePorts(
                    payout_state=self._ensure_payout_state_service,
                    accepted_share_stats=lambda: self.accepted_share_stats(),
                    snapshot_at_job_issue=lambda anchor, window: (
                        self.ledger.snapshot_at_job_issue(
                            anchor,
                            window_weight=window,
                        )
                    ),
                    snapshot_anchor_ms=lambda value: self._job_snapshot_anchor_ms(
                        value
                    ),
                    payout_policy=lambda: self.prism_payout_policy(),
                    ctv_settlement=lambda height, parent: (
                        self.prism_ctv_settlement_config(
                            block_height=height,
                            parent_hash=parent,
                        )
                    ),
                    coinbase_suffix=lambda first, second: (
                        self.coinbase_script_sig_suffix_hex(first, second)
                    ),
                    signing_seed_hex=lambda: str(
                        getattr(self, "signing_seed_hex", "")
                    ),
                    ledger_signing_seed_hex=lambda: str(
                        getattr(self, "ledger_attestation_signing_seed_hex", "")
                    ),
                    await_parent_preview=lambda parent, height: (
                        self._await_pending_parent_payout_preview(
                            parent,
                            parent_height=height,
                        )
                    ),
                    prior_balances_for_parent=lambda parent, height, fallback: (
                        self._prior_balances_for_job_parent(
                            parent,
                            parent_height=height,
                            fallback_balances=fallback,
                        )
                    ),
                    serialize_prior_balance_preview=lambda balances: (
                        self._serialize_prior_balance_preview(balances)
                    ),
                    accepted_block_preview_from_bundle=lambda bundle, balances: (
                        self._accepted_block_payout_preview_from_bundle(
                            bundle,
                            prior_balances=balances,
                        )
                    ),
                    schedule_refresh_retry=lambda: self._schedule_tip_refresh_retry(),
                    idle_tip_diverged=lambda: self._job_bundle_idle_tip_diverged(),
                    artifacts_buildable=lambda artifacts: (
                        self._job_bundle_artifacts_buildable(artifacts)
                    ),
                    published_snapshot_artifacts=(
                        self._job_bundle_published_snapshot_artifacts
                    ),
                    published_artifacts=self._job_bundle_published_artifacts,
                    note_tip_refresh_superseded=lambda: (
                        self._record_job_bundle_tip_superseded()
                    ),
                    record_tip_refresh_phase=lambda phase, elapsed: (
                        self._observe_tip_refresh_build_phase(phase, elapsed)
                    ),
                    clear_retained_collection_refresh=lambda: (
                        self._clear_retained_collection_refresh()
                    ),
                    readiness_promoted=self._on_job_readiness_promoted,
                    start_bundle_build=lambda: (
                        self._ensure_progress_health_service().start_bundle_build()
                    ),
                    wall_time_ms=lambda: now_ms(),
                ),
                repository,
            )
            compiler = self._new_bundle_compiler(service)
            repository.bind_event_sink(
                TemplateArtifactEventSink(
                    record_cache_event=lambda hit: service.record_cache_event(
                        "template",
                        hit=hit,
                    ),
                    record_build_phase=service.record_phase,
                    artifacts_changed=lambda artifacts, fingerprint_changed: (
                        self._on_template_artifacts_changed(
                            service,
                            artifacts,
                            fingerprint_changed,
                        )
                    ),
                    artifacts_cleared=service.on_template_artifacts_cleared,
                )
            )
            service.bind_bundle_compiler(compiler)
            self._bundle_compiler = compiler
            self._job_bundle_service = service
            return service

    def _on_job_readiness_promoted(self) -> None:
        self._progress_note_refresh_pending()
        self._ensure_tip_refresh_service().readiness_promoted()

    def _on_template_artifacts_changed(
        self,
        service: JobBundleService,
        artifacts: CachedTemplateArtifacts,
        fingerprint_changed: bool,
    ) -> None:
        service.on_template_artifacts_changed(artifacts, fingerprint_changed)
        if fingerprint_changed:
            self._ensure_tip_refresh_service().template_artifacts_changed(artifacts)

    def _job_bundle_newest_observed_tip(self) -> str | None:
        self._ensure_tip_refresh_state()
        with self.lock:
            return self._newest_observed_tip_locked()

    def _job_bundle_pinned_issuance_artifacts(
        self,
    ) -> CachedTemplateArtifacts | None:
        self._ensure_tip_refresh_state()
        with self.lock:
            published = getattr(self, "current_tip_first_seen", None)
            latest_detected = getattr(self, "latest_detected_tip", None)
            published_snapshot = getattr(self, "tip_template_snapshot", None)
            if (
                published is not None
                and latest_detected is not None
                and latest_detected[0] != published[0]
                and published_snapshot is not None
                and published_snapshot.bestblockhash == published[0]
                and published_snapshot.template_artifacts is not None
                and self._published_tip_authoritative_locked(time.monotonic())
            ):
                return published_snapshot.template_artifacts
        return None

    def _job_bundle_repinned_issuance_artifacts(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> CachedTemplateArtifacts | None:
        with self.lock:
            published = getattr(self, "current_tip_first_seen", None)
            published_snapshot = getattr(self, "tip_template_snapshot", None)
            if (
                published is not None
                and artifacts.previousblockhash != published[0]
                and published_snapshot is not None
                and published_snapshot.bestblockhash == published[0]
                and published_snapshot.template_artifacts is not None
                and self._published_tip_authoritative_locked(time.monotonic())
            ):
                return published_snapshot.template_artifacts
        return None

    def _job_bundle_artifacts_buildable(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        with self.lock:
            return self._artifacts_buildable_locked(artifacts)

    def _job_bundle_published_snapshot_artifacts(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        with self.lock:
            return self._published_snapshot_artifacts_locked(artifacts)

    def _job_bundle_published_artifacts(
        self,
    ) -> CachedTemplateArtifacts | None:
        with self.lock:
            snapshot = getattr(self, "tip_template_snapshot", None)
            return None if snapshot is None else snapshot.template_artifacts

    def _job_bundle_idle_tip_diverged(self) -> bool:
        with self.lock:
            return self._vardiff_idle_tip_divergence_locked()

    def _record_job_bundle_tip_superseded(self) -> None:
        self._ensure_tip_refresh_service().record_superseded_result()

    def _new_bundle_compiler(self, service: JobBundleService) -> BundleCompiler:
        return BundleCompiler(
            BundleCompilerPorts(
                payout_policy=self.prism_payout_policy,
                ctv_settlement=lambda height, parent: (
                    self.prism_ctv_settlement_config(
                        block_height=height,
                        parent_hash=parent,
                    )
                ),
                signing_seed_hex=lambda: str(
                    getattr(self, "signing_seed_hex", "")
                ),
                ledger_signing_seed_hex=lambda: str(
                    getattr(self, "ledger_attestation_signing_seed_hex", "")
                ),
                bundle_timeout_seconds=lambda: float(
                    getattr(
                        self,
                        "bundle_build_timeout_seconds",
                        DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
                    )
                ),
                cancel_grace_seconds=lambda: float(
                    getattr(
                        self,
                        "job_build_cancel_grace_seconds",
                        DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS,
                    )
                ),
                phases=service.phases,
                record_tip_refresh_phase=self._observe_tip_refresh_build_phase,
                record_ipc_bytes=self._record_tip_refresh_ipc_bytes,
                record_worker_failure=self._record_bundle_compiler_failure,
                record_worker_event=service.record_worker_event,
                tip_refresh_metrics_enabled=service.tip_refresh_metrics_enabled,
                active_build_control=service.active_build_control,
                register_process=lambda control, process: service.register_process(
                    control,  # type: ignore[arg-type]
                    process,
                ),
                superseded_error=lambda message: _JobBundleBuildSuperseded(
                    message
                ),
            )
        )

    def _ensure_bundle_compiler(self) -> BundleCompiler:
        compiler = getattr(self, "_bundle_compiler", None)
        if compiler is not None:
            return compiler
        self._ensure_job_bundle_service()
        compiler = getattr(self, "_bundle_compiler", None)
        if compiler is None:
            raise RuntimeError("job bundle compiler binding was not published")
        return compiler

    def _record_bundle_compiler_failure(self) -> None:
        self._ensure_tip_refresh_service().record_worker_failure()

    def _ensure_payout_state_service(self) -> PayoutStateService:
        service = getattr(self, "_payout_state_service", None)
        if service is not None:
            return service
        init_lock = self.__dict__.setdefault(
            "_payout_state_service_init_lock",
            threading.Lock(),
        )
        with init_lock:
            service = getattr(self, "_payout_state_service", None)
            if service is not None:
                return service
            service = PayoutStateService(
                PayoutStatePorts(
                    accepted_share_stats=lambda: self.accepted_share_stats(),
                    snapshot_at_job_issue=lambda anchor, window: (
                        self.ledger.snapshot_at_job_issue(
                            anchor,
                            window_weight=window,
                        )
                    ),
                    current_prior_balances=lambda: (
                        self.ledger.current_prior_balances()
                    ),
                    snapshot_anchor_ms=lambda issued_at_ms: (
                        self._job_snapshot_anchor_ms(issued_at_ms)
                    ),
                    current_template_network_difficulty=(
                        self._payout_template_network_difficulty
                    ),
                    pool_ready=lambda: self._ensure_job_bundle_service().ready_latched(),
                    record_build_phase=self._record_payout_build_phase,
                    invalidate_job_cache=self._invalidate_payout_job_cache,
                    clear_retained_collection_refresh=(
                        self._clear_retained_collection_refresh
                    ),
                    cancel_obsolete_job_builds=self._cancel_obsolete_job_builds,
                    cancel_obsolete_bundle_builds=lambda generation: (
                        self._cancel_obsolete_job_bundle_builds(
                            payout_state_generation=generation
                        )
                    ),
                    payout_invalidated=self._on_payout_state_invalidated,
                    payout_published=self._on_payout_state_published,
                    schedule_refresh_retry=self._schedule_tip_refresh_retry,
                    chain_block_hash=lambda height: str(
                        self.rpc.call("getblockhash", [height])
                    ),
                    stop_requested=lambda: bool(
                        getattr(self, "stop_event", threading.Event()).is_set()
                    ),
                ),
                wall_time_ms=lambda: now_ms(),
                histogram_buckets=PRISM_TIP_REFRESH_SECONDS_BUCKETS,
                config=PayoutStateConfig(
                    accepted_block_preview_wait_seconds=float(
                        getattr(
                            self,
                            "accepted_block_payout_preview_wait_seconds",
                            DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS,
                        )
                    ),
                    reconcile_supersession_retries=int(
                        getattr(
                            self,
                            "payout_reconcile_supersession_retries",
                            DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
                        )
                    ),
                ),
            )
            self._payout_state_service = service
            return service

    def _ensure_reorg_reconciler_service(self) -> ReorgReconcilerService:
        service = self.__dict__.get("_reorg_reconciler_service")
        if service is not None:
            return service
        init_lock = self.__dict__.setdefault(
            "_reorg_reconciler_service_init_lock",
            threading.Lock(),
        )
        with init_lock:
            service = self.__dict__.get("_reorg_reconciler_service")
            if service is not None:
                return service
            service = ReorgReconcilerService(
                ReorgPorts(
                    rpc_call=lambda *args, **kwargs: self.rpc.call(
                        *args,
                        **kwargs,
                    ),
                    ledger=lambda: self.ledger,
                    ensure_job_cache_state=self._ensure_job_cache_state,
                    source_tip=self._reorg_payout_source_tip,
                    reserve_external_tip=lambda tip: self._reserve_payout_state_source(
                        "external_tip",
                        tip_hash=tip,
                    ),
                    max_supersession_retries=lambda: (
                        self._ensure_payout_state_service().reconcile_supersession_retries
                    ),
                    prepare_lock=lambda: (
                        self._ensure_payout_state_service().prepare_lock
                    ),
                    capture_source=self._capture_payout_state_source,
                    prepared_candidate=self._prepared_payout_state_candidate,
                    publication_required=self._payout_source_requires_publication,
                    block_publication=self._block_payout_state_publication,
                    publication_guard=lambda: (
                        self._ensure_audit_artifact_store().publication_order_guard()
                    ),
                    publish_candidate=self._publish_payout_state_candidate,
                    observe_preparation=lambda elapsed: (
                        self._observe_payout_state_seconds("preparation", elapsed)
                    ),
                    chain_view_untrusted=lambda: self.qbit_chain_view_untrusted(),
                    monotonic=time.monotonic,
                    reconcile_with_admission=lambda tip: (
                        self.reconcile_prism_pool_blocks_once(tip_hash=tip)
                    ),
                ),
                enabled=bool(self.reorg_reconciler_enabled),
                cache_seconds=float(self.reorg_reconcile_cache_seconds),
                inactive_block_count=int(self.reorg_inactive_block_count),
                reactivated_block_count=int(self.reorg_reactivated_block_count),
                reconcile_skip_count=int(self.reorg_reconcile_skip_count),
                reconcile_error_count=int(self.reorg_reconcile_error_count),
                matured_payout_count=int(self.matured_payout_count),
                last_tip_hash=self.last_reorg_reconciled_tip_hash,
                last_trusted=bool(self.last_reorg_reconciled_trusted),
                last_monotonic=self.last_reorg_reconciled_monotonic,
            )
            self.__dict__["_reorg_reconciler_service"] = service
            for name in (
                "enabled",
                "cache_seconds",
                "inactive_block_count",
                "reactivated_block_count",
                "reconcile_skip_count",
                "reconcile_error_count",
                "matured_payout_count",
                "last_tip_hash",
                "last_trusted",
                "last_monotonic",
            ):
                self.__dict__.pop(f"_reorg_compat_{name}", None)
            return service

    def _reorg_payout_source_tip(self) -> str | None:
        with self.lock:
            return self._ensure_payout_state_service().snapshot().source[1]

    def _payout_template_network_difficulty(self) -> int | None:
        service = getattr(self, "_job_bundle_service", None)
        if service is None:
            return None
        artifacts = service.template_repository.current_artifacts()
        return None if artifacts is None else artifacts.network_difficulty

    def _record_payout_build_phase(self, phase: str, elapsed: float) -> None:
        if phase in PRISM_TIP_REFRESH_BUILD_PHASES:
            self._observe_tip_refresh_build_phase(phase, elapsed)
        if phase == "payout_artifact":
            phases = self._job_build_phases()
            phases[phase] = phases.get(phase, 0.0) + elapsed

    def _invalidate_payout_job_cache(self) -> None:
        self._ensure_job_bundle_service().clear_cache()

    def _clear_retained_collection_refresh(self) -> None:
        self._ensure_tip_refresh_service().clear_retained_collection_refresh()

    def _on_payout_state_invalidated(
        self,
        generation: int,
        invalidated_monotonic: float,
    ) -> None:
        self._record_progress_payout_generation(
            generation,
            invalidated_monotonic,
        )
        self._ensure_tip_refresh_service().payout_generation_invalidated(generation)

    def _on_payout_state_published(
        self,
        generation: int,
        invalidated_monotonic: float,
    ) -> None:
        self._record_progress_payout_generation(
            generation,
            invalidated_monotonic,
        )
        self._ensure_tip_refresh_service().payout_generation_changed(generation)

    def _ensure_progress_health_service(self) -> ProgressHealthService:
        service = getattr(self, "progress_health_service", None)
        if service is None:
            started = float(getattr(self, "started_monotonic", time.monotonic()))
            service = ProgressHealthService(
                ProgressHealthConfig(
                    pending_refresh_deadline_seconds=float(
                        getattr(
                            self,
                            "health_pending_refresh_max_age_seconds",
                            DEFAULT_PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS,
                        )
                    ),
                    tip_poll_deadline_seconds=float(
                        getattr(
                            self,
                            "health_tip_poll_max_age_seconds",
                            DEFAULT_PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS,
                        )
                    ),
                    bundle_build_deadline_seconds=float(
                        getattr(
                            self,
                            "bundle_build_timeout_seconds",
                            DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
                        )
                    ),
                ),
                started_monotonic=started,
                initial_payout_generation=(
                    self._ensure_payout_state_service().snapshot().generation
                ),
            )
            self.progress_health_service = service
        return service

    def _ensure_observability_service(self) -> ObservabilityService:
        service = self.__dict__.get("_observability_service")
        if service is not None:
            return service
        init_lock = self.__dict__.setdefault(
            "_observability_service_init_lock",
            threading.Lock(),
        )
        with init_lock:
            service = self.__dict__.get("_observability_service")
            if service is None:
                service = ObservabilityService(_CoordinatorObservability(self))
                self.__dict__["_observability_service"] = service
            return service

    def _ensure_metrics_renderer(self) -> MetricsRenderer:
        renderer = self.__dict__.get("_metrics_renderer")
        if renderer is None:
            renderer = MetricsRenderer(self)
            self.__dict__["_metrics_renderer"] = renderer
        return renderer

    def _ensure_audit_http_facade(self) -> AuditHttpFacade:
        facade = self.__dict__.get("_audit_http_facade")
        if facade is not None:
            return facade
        init_lock = self.__dict__.setdefault(
            "_audit_http_facade_init_lock",
            threading.Lock(),
        )
        with init_lock:
            facade = self.__dict__.get("_audit_http_facade")
            if facade is None:
                facade = AuditHttpFacade(
                    _CoordinatorAuditHttp(self),
                    AuditHttpConfig(
                        bind=str(getattr(self, "audit_bind", None) or "127.0.0.1"),
                        port=int(getattr(self, "audit_port", 0)),
                    ),
                )
                self.__dict__["_audit_http_facade"] = facade
            return facade

    def _job_build_phases(self) -> dict[str, float]:
        return self._ensure_job_bundle_service().phases()

    def _cancel_obsolete_job_bundle_builds(
        self,
        *,
        current_tip: str | None = None,
        payout_state_generation: int | None = None,
    ) -> None:
        self._ensure_job_bundle_service().cancel_obsolete_bundle_processes(
            current_tip=current_tip,
            payout_state_generation=payout_state_generation,
        )

    def _register_job_bundle_process(
        self,
        control: _JobBundleBuildControl,
        process: subprocess.Popen[str],
    ) -> None:
        self._ensure_job_bundle_service().register_process(control, process)

    def _build_payout_ledger_artifact(
        self,
        expected_payout_state_generation: int,
        artifact_payout_state_generation: int,
        network_difficulty: int,
    ) -> PayoutLedgerArtifact | None:
        return self._ensure_payout_state_service().build_ledger_artifact(
            expected_payout_state_generation, artifact_payout_state_generation, network_difficulty
        )

    def _prepare_payout_ledger_artifact(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> None:
        self._ensure_payout_state_service().prepare_ledger_artifact(
            payout_state_generation, network_difficulty
        )

    def _payout_artifact_preparation_loop(self) -> None:
        self._ensure_payout_state_service()._artifact_preparation_loop()

    def _schedule_payout_ledger_artifact_preparation(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> None:
        self._ensure_payout_state_service().schedule_ledger_artifact_preparation(
            payout_state_generation, network_difficulty
        )

    def _usable_payout_ledger_artifact(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> PayoutLedgerArtifact | None:
        return self._ensure_payout_state_service().usable_ledger_artifact(
            payout_state_generation, network_difficulty
        )

    def _schedule_current_payout_ledger_artifact_if_missing(self) -> None:
        self._ensure_payout_state_service().schedule_current_ledger_artifact_if_missing()

    def shutdown_payout_artifact_executor(self) -> None:
        self._ensure_payout_state_service().shutdown()

    def _job_build_checkpoint(
        self,
        phase: str,
        cancellation: _JobBuildCancellation,
    ) -> None:
        cancellation.raise_if_cancelled(phase)

    def _record_job_cache_event(self, kind: str, *, hit: bool) -> None:
        self._ensure_job_bundle_service().record_cache_event(kind, hit=hit)

    def _prepare_payout_state_artifact(
        self,
        *,
        generation: int,
        source_generation: int,
        cancellation: _JobBuildCancellation | None = None,
    ) -> PayoutStateArtifact:
        return self._ensure_payout_state_service().prepare_artifact(
            generation=generation, source_generation=source_generation, cancellation=cancellation
        )

    def _payout_state_artifact_from_balances(
        self,
        *,
        generation: int,
        source_generation: int,
        balances: list[dict[str, object]],
    ) -> PayoutStateArtifact:
        return self._ensure_payout_state_service().artifact_from_balances(
            generation=generation, source_generation=source_generation, balances=balances
        )

    def _current_payout_state_artifact(
        self,
        cancellation: _JobBuildCancellation | None = None,
    ) -> PayoutStateArtifact:
        return self._ensure_payout_state_service().current_artifact(cancellation)

    def _job_build_executor_locked(self) -> ThreadPoolExecutor:
        return self._ensure_job_bundle_service()._executor_locked()

    def _start_job_build_locked(self, request: _JobBuildRequest) -> _JobBuildFlight:
        return self._ensure_job_bundle_service()._start_locked(request)

    def _arm_job_build_locked(self, flight: _JobBuildFlight) -> None:
        self._ensure_job_bundle_service()._arm_locked(flight)

    def _execute_job_build_request(
        self,
        request: _JobBuildRequest,
    ) -> CachedJobBundle:
        return self._ensure_job_bundle_service()._execute_request(request)

    @staticmethod
    def _collection_job_builds_are_independent(
        first: _JobBuildRequest,
        second: _JobBuildRequest,
    ) -> bool:
        return JobBundleService.collection_builds_independent(first, second)

    @staticmethod
    def _job_build_requests_can_share(
        first: _JobBuildRequest,
        second: _JobBuildRequest,
    ) -> bool:
        return JobBundleService.requests_can_share(first, second)

    @staticmethod
    def _ready_job_build_precedes_collection(
        first: _JobBuildRequest,
        second: _JobBuildRequest,
    ) -> bool:
        return JobBundleService.ready_precedes_collection(first, second)

    @staticmethod
    def _defer_collection_job_build_locked(
        *blockers: Future[CachedJobBundle],
    ) -> Future[CachedJobBundle]:
        return JobBundleService.defer_collection(*blockers)

    def _cancel_job_build_flight_locked(
        self,
        flight: _JobBuildFlight,
        reason: str,
        *,
        now: float | None = None,
    ) -> bool:
        return self._ensure_job_bundle_service()._cancel_flight_locked(
            flight,
            reason,
            now=now,
        )

    def _promote_pending_job_build_locked(self) -> None:
        self._ensure_job_bundle_service()._promote_pending_locked()

    def _job_build_done(
        self,
        flight: _JobBuildFlight,
        future: Future[CachedJobBundle],
    ) -> None:
        self._ensure_job_bundle_service()._build_done(flight, future)

    def _request_job_build(
        self,
        request: _JobBuildRequest,
    ) -> Future[CachedJobBundle]:
        return self._ensure_job_bundle_service().request_build(request)

    def _cancel_obsolete_job_builds(
        self,
        reason: str,
        *,
        keep_published_snapshot: bool = False,
    ) -> None:
        self._ensure_job_bundle_service().cancel_obsolete_builds(
            reason,
            keep_published_snapshot=keep_published_snapshot,
        )

    def shutdown_job_build_executor(self) -> None:
        self._ensure_job_bundle_service().shutdown()

    def _job_bundle_payout_state_current(self, bundle: CachedJobBundle) -> bool:
        return self._ensure_job_bundle_service().bundle_payout_state_current(bundle)

    @contextmanager
    def _payout_balance_mutation(self) -> Iterator[None]:
        with self._ensure_payout_state_service().balance_mutation():
            yield

    def _begin_accepted_block_payout_preview(
        self,
        block_hash: str,
        *,
        block_height: int | None = None,
    ) -> None:
        self._ensure_payout_state_service().begin_accepted_block_preview(
            block_hash, block_height=block_height
        )

    def _mark_accepted_block_payout_landed(
        self,
        block_hash: str,
        *,
        block_height: int,
    ) -> None:
        self._ensure_payout_state_service().mark_accepted_block_landed(
            block_hash, block_height=block_height
        )

    def _publish_accepted_block_payout_preview(
        self,
        block_hash: str,
        balances: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        return self._ensure_payout_state_service().publish_accepted_block_preview(
            block_hash, balances
        )

    def _accepted_block_preview_candidate(
        self,
        candidate: PayoutStateCandidate,
        *,
        block_hash: str,
        preview: tuple[tuple[str, str, str, int], ...],
    ) -> PayoutStateCandidate:
        return self._ensure_payout_state_service().accepted_block_preview_candidate(
            candidate, block_hash=block_hash, preview=preview
        )

    def _serialize_prior_balance_preview(
        self,
        balances: list[dict[str, object]],
    ) -> tuple[tuple[str, str, str, int], ...]:
        return self._ensure_payout_state_service().serialize_prior_balance_preview(balances)

    def _accepted_block_payout_preview_from_bundle(
        self,
        final_bundle: dict[str, Any],
        *,
        prior_balances: list[dict[str, object]] | None = None,
    ) -> list[dict[str, object]]:
        return self._ensure_payout_state_service().accepted_block_preview_from_bundle(
            final_bundle, prior_balances=prior_balances
        )

    def _materialize_prior_balance_preview(
        self,
        preview: tuple[tuple[str, str, str, int], ...],
    ) -> list[dict[str, object]]:
        return self._ensure_payout_state_service().materialize_prior_balance_preview(preview)

    def _clear_accepted_block_payout_preview(
        self,
        block_hash: str,
        *,
        invalidate_published: bool = False,
    ) -> None:
        self._ensure_payout_state_service().clear_accepted_block_preview(
            block_hash, invalidate_published=invalidate_published
        )

    def _accepted_block_payout_transition_landed(self, block_hash: str) -> bool:
        return self._ensure_payout_state_service().accepted_block_transition_landed(block_hash)

    def _accepted_block_payout_transition_for_parent(
        self,
        parent_hash: str,
        *,
        parent_height: int | None = None,
    ) -> tuple[str, bool] | None:
        return self._ensure_payout_state_service().accepted_block_transition_for_parent(
            parent_hash, parent_height=parent_height
        )

    def _await_pending_parent_payout_preview(
        self,
        parent_hash: str,
        *,
        parent_height: int | None = None,
    ) -> list[dict[str, object]] | None:
        return self._ensure_payout_state_service().await_pending_parent_preview(
            parent_hash, parent_height=parent_height
        )

    def _prior_balances_for_job_parent(
        self,
        parent_hash: str,
        *,
        parent_height: int | None = None,
        fallback_balances: Sequence[dict[str, object]] | None = None,
    ) -> list[dict[str, object]]:
        return self._ensure_payout_state_service().prior_balances_for_parent(
            parent_hash, parent_height=parent_height, fallback_balances=fallback_balances
        )

    def _observe_payout_state_seconds(
        self,
        name: str,
        elapsed_seconds: float,
        *,
        relation: str | None = None,
    ) -> None:
        self._ensure_payout_state_service().observe_seconds(
            name, elapsed_seconds, relation=relation
        )

    def _observe_payout_gate_admission(
        self,
        admission: object,
        *,
        generation: int,
        fallback_wait_seconds: float,
    ) -> None:
        self._ensure_payout_state_service().observe_gate_admission(
            admission, generation=generation, fallback_wait_seconds=fallback_wait_seconds
        )

    def _reserve_payout_state_source(
        self,
        cause: str,
        *,
        tip_hash: str | None = None,
        invalidated_monotonic: float | None = None,
    ) -> int:
        return self._ensure_payout_state_service().reserve_source(
            cause, tip_hash=tip_hash, invalidated_monotonic=invalidated_monotonic
        )

    def _reserve_payout_state_source_if_current(
        self,
        expected_source_generation: int,
        cause: str,
        *,
        tip_hash: str | None = None,
        invalidated_monotonic: float | None = None,
    ) -> tuple[int, int, str | None, str, float] | None:
        return self._ensure_payout_state_service().reserve_source_if_current(
            expected_source_generation, cause, tip_hash=tip_hash, invalidated_monotonic=invalidated_monotonic
        )

    def _capture_payout_state_source(
        self,
    ) -> tuple[int, int, str | None, str, float]:
        return self._ensure_payout_state_service().capture_source()

    def _prepared_payout_state_candidate(
        self,
        captured: tuple[int, int, str | None, str, float],
    ) -> PayoutStateCandidate:
        return self._ensure_payout_state_service().prepared_candidate(captured)

    def _current_payout_state_candidate(self) -> PayoutStateCandidate:
        return self._ensure_payout_state_service().current_candidate()

    def _record_discarded_payout_candidate(self) -> None:
        self._ensure_payout_state_service()._record_discarded_candidate()

    def _block_payout_state_publication(
        self,
        *,
        force: bool = False,
        supersede_with: tuple[int, str | None, str, float] | None = None,
    ) -> None:
        self._ensure_payout_state_service().block_publication(
            force=force, supersede_with=supersede_with
        )

    def _payout_state_publication_fenced(self) -> bool:
        return self._ensure_payout_state_service().publication_fenced()

    def _payout_source_requires_publication(
        self,
        candidate: PayoutStateCandidate | None = None,
    ) -> bool:
        return self._ensure_payout_state_service().source_requires_publication(candidate)

    def _publish_payout_state_candidate(
        self,
        candidate: PayoutStateCandidate,
    ) -> int | None:
        return self._ensure_payout_state_service().publish_candidate(candidate)

    def _record_first_payout_delivery(
        self,
        generation: int,
        delivered_monotonic: float,
    ) -> None:
        self._ensure_payout_state_service().record_first_delivery(
            generation, delivered_monotonic
        )

    def _advance_payout_state_generation(self) -> int:
        return self._ensure_payout_state_service().advance_generation()

    def _publish_current_payout_state_with_retry_budget(
        self,
        *,
        initial_attempted: bool = False,
    ) -> int | None:
        return self._ensure_payout_state_service().publish_current_with_retry_budget(
            initial_attempted=initial_attempted
        )

    def observe_job_build_elapsed(
        self,
        elapsed_seconds: float,
        phases: dict[str, float],
    ) -> None:
        self._ensure_job_bundle_service().observe_elapsed(elapsed_seconds, phases)

    def _reserve_template_artifact_generation(self) -> int:
        return self._ensure_job_bundle_service().template_repository.reserve_generation()

    def _derive_template_artifacts(
        self,
        template: dict[str, Any],
        *,
        generation: int,
    ) -> CachedTemplateArtifacts:
        return self._ensure_job_bundle_service().template_repository.derive(
            template,
            generation=generation,
        )

    def _store_template_artifacts(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        return self._ensure_job_bundle_service().template_repository.store_artifacts(
            artifacts
        )

    def store_template_artifacts(
        self,
        template: dict[str, Any],
        *,
        generation: int | None = None,
    ) -> CachedTemplateArtifacts | None:
        return self._ensure_job_bundle_service().template_repository.store(
            template,
            generation=generation,
        )

    def job_issuance_template_artifacts(self) -> CachedTemplateArtifacts:
        return self._ensure_job_bundle_service().template_repository.issuance()

    def current_template_artifacts(self) -> CachedTemplateArtifacts:
        return self._ensure_job_bundle_service().template_repository.current()

    @staticmethod
    def _collection_bundle_identity(worker: WorkerIdentity) -> tuple[str, str]:
        return JobBundleService.collection_identity(worker)

    def _job_bundle_key(
        self,
        artifacts: CachedTemplateArtifacts,
        *,
        mode: str,
        payout_state_generation: int,
        payout_artifact_generation: int = 0,
        worker: WorkerIdentity | None,
    ) -> tuple[object, ...]:
        return self._ensure_job_bundle_service().job_bundle_key(
            artifacts,
            mode=mode,
            payout_state_generation=payout_state_generation,
            payout_artifact_generation=payout_artifact_generation,
            worker=worker,
        )

    def _job_bundle_mode(self, requested_mode: str | None) -> str:
        return self._ensure_job_bundle_service().job_bundle_mode(requested_mode)

    def _lookup_job_bundle(
        self,
        key: tuple[object, ...],
    ) -> CachedJobBundle | None:
        return self._ensure_job_bundle_service().lookup_bundle(key)

    def _job_bundle_entry_usable(
        self,
        cached: CachedJobBundle | None,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        return self._ensure_job_bundle_service().bundle_entry_usable(
            cached,
            artifacts,
        )

    def _bind_cached_bundle_to_artifacts(
        self,
        cached: CachedJobBundle,
        artifacts: CachedTemplateArtifacts,
    ) -> CachedJobBundle:
        return self._ensure_job_bundle_service().bind_cached_bundle(
            cached,
            artifacts,
        )

    def _new_job_build_request(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentity | None,
        *,
        mode: str,
        payout_state_generation: int,
        cache_key: tuple[object, ...],
        payout_ledger_artifact: PayoutLedgerArtifact | None = None,
        idle_retarget: bool = False,
    ) -> _JobBuildRequest:
        return self._ensure_job_bundle_service().new_build_request(
            artifacts,
            worker,
            mode=mode,
            payout_state_generation=payout_state_generation,
            cache_key=cache_key,
            payout_ledger_artifact=payout_ledger_artifact,
            idle_retarget=idle_retarget,
        )

    def _newest_observed_tip_locked(self) -> str | None:
        """Newest live-tip observation, ahead of published submit authority.

        Detection and publication are split: a winning refresh builds for a
        detected tip while the previous tip remains published. Build-pipeline
        supersession checks must compare against this detection view, or a
        replacement build for a freshly detected tip would classify itself as
        obsolete before it could ever be published.
        """
        detected = getattr(self, "latest_detected_tip", None)
        if detected is not None:
            return detected[0]
        published = getattr(self, "current_tip_first_seen", None)
        return published[0] if published is not None else None

    def _artifacts_buildable_locked(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        """Whether work for these artifacts may still be built and cached.

        The newest detected tip covers replacement construction. Exactly the
        published snapshot additionally stays buildable while the published
        tip retains share-classification authority, so pinned direct issuance
        can rebuild published work (for example after a payout-generation
        prune) instead of classifying itself superseded for the whole
        unpublished window. Anything else -- including other templates for
        the published parent -- is superseded construction and must stop.
        """
        newest = self._newest_observed_tip_locked()
        if newest is None or artifacts.previousblockhash == newest:
            return True
        return self._published_snapshot_artifacts_locked(artifacts)

    def _published_snapshot_artifacts_locked(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        """Whether these artifacts are exactly the still-authoritative published snapshot."""
        published = getattr(self, "current_tip_first_seen", None)
        published_snapshot = getattr(self, "tip_template_snapshot", None)
        return bool(
            published is not None
            and published_snapshot is not None
            and published_snapshot.bestblockhash == published[0]
            and artifacts.previousblockhash == published[0]
            and published_snapshot.template_fingerprint == artifacts.fingerprint
            and self._published_tip_authoritative_locked(time.monotonic())
        )

    def _cache_job_bundle_if_current(
        self,
        built: CachedJobBundle,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        return self._ensure_job_bundle_service().cache_bundle_if_current(
            built,
            artifacts,
        )

    def shared_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentity | None = None,
        *,
        mode: str | None = None,
        cancelled: Callable[[], bool] | None = None,
        retry_superseded: bool = True,
        idle_retarget: bool = False,
        publication_critical: bool = False,
        request_source: str = "routine",
        priority_requested_monotonic: float | None = None,
    ) -> CachedJobBundle:
        self._ensure_tip_refresh_state()
        return self._ensure_job_bundle_service().shared_job_bundle(
            artifacts,
            worker,
            mode=mode,
            cancelled=cancelled,
            retry_superseded=retry_superseded,
            idle_retarget=idle_retarget,
            publication_critical=publication_critical,
            request_source=request_source,
            priority_requested_monotonic=priority_requested_monotonic,
        )

    def build_shared_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentity | None = None,
        *,
        mode: str | None = None,
        payout_state_generation: int | None = None,
        payout_artifact: PayoutLedgerArtifact | None = None,
        key: tuple[object, ...] | None = None,
        build_request: _JobBuildRequest | None = None,
    ) -> CachedJobBundle:
        return self._ensure_job_bundle_service().build_shared_job_bundle(
            artifacts,
            worker,
            mode=mode,
            payout_state_generation=payout_state_generation,
            payout_artifact=payout_artifact,
            key=key,
            build_request=build_request,
        )

    def stamp_job_for_client(
        self,
        client: ClientState,
        cached: CachedJobBundle,
        *,
        clean_jobs: bool,
    ) -> PrismJobContext:
        return self._ensure_job_delivery_service().stamp(
            client,
            cached,
            clean_jobs=clean_jobs,
        )

    def accepted_share_stats(self) -> tuple[int, int]:
        """Return (accepted share count, distinct miner count) cheaply.

        Prefers the ledger's aggregate query; falls back to materializing
        all_shares for ledgers that do not implement it.
        """
        stats = getattr(self.ledger, "accepted_share_stats", None)
        if callable(stats):
            payload = stats()
            return (
                int(payload["accepted_share_count"]),
                int(payload["distinct_miner_count"]),
            )
        shares = self.ledger.all_shares()
        miner_ids = {getattr(share, "miner_id", None) for share in shares}
        miner_ids.discard(None)
        return len(shares), len(miner_ids)

    def _ensure_watchdog_state(self) -> None:
        if not hasattr(self, "_heartbeats_lock"):
            self._heartbeats_lock = threading.Lock()
        if not hasattr(self, "_heartbeats"):
            self._heartbeats = {}
        if not hasattr(self, "_watchdog_pauses"):
            self._watchdog_pauses = {}

    def _legacy_ctv_runtime_config(self) -> CtvRuntimeConfig:
        coordinator_config = getattr(self, "config", None)
        ctv_config = getattr(coordinator_config, "ctv", None)
        if ctv_config is not None:
            config = CtvRuntimeConfig.from_coordinator_config(ctv_config)
        else:
            config = CtvRuntimeConfig(
                enabled=False,
                wallet=None,
                fee_sats=0,
                limit=100,
                chunk_size=DEFAULT_PRISM_CTV_BROADCASTER_CHUNK_SIZE,
                interval_seconds=30.0,
            )
        overrides = self.__dict__.get("_ctv_runtime_compat_config", {})
        if overrides:
            config = dataclass_replace(config, **overrides)
        return config

    def _make_ctv_runtime_service(
        self,
        config: CtvRuntimeConfig | None = None,
    ) -> CtvRuntimeService:
        stop_event = getattr(self, "stop_event", None)
        if stop_event is None:
            stop_event = threading.Event()
            self.stop_event = stop_event
        runtime = CtvRuntimeService(
            rpc_call=lambda *args, **kwargs: self.rpc.call(*args, **kwargs),
            ledger=getattr(self, "ledger", None),
            writer_admission=lambda component: self._writer_operation(component),
            tip_refresh_pending=lambda: self.tip_refresh_is_pending(),
            heartbeat=lambda: self._record_heartbeat("ctv_fanout_broadcaster"),
            stop_event=stop_event,
            config=self._legacy_ctv_runtime_config() if config is None else config,
            daemon_type=CtvFanoutBroadcastDaemon,
            broadcaster_type=CtvFanoutBroadcaster,
            # Preserve temporary coordinator patch points for focused tests.
            monotonic=lambda: time.monotonic(),
            print_exception=lambda: traceback.print_exc(),
        )
        compat_daemon = self.__dict__.pop("_ctv_runtime_compat_daemon", None)
        if compat_daemon is not None:
            runtime.daemon = compat_daemon
        return runtime

    def _ensure_ctv_runtime(self) -> CtvRuntimeService:
        runtime = self.__dict__.get("_ctv_runtime")
        if runtime is not None:
            return runtime
        init_lock = self.__dict__.get("_ctv_runtime_init_lock")
        if init_lock is None:
            # CPython's setdefault is atomic under the GIL. Focused tests may
            # construct through __new__, while normal instances install this
            # lock in __init__ before any process thread can start.
            init_lock = self.__dict__.setdefault(
                "_ctv_runtime_init_lock",
                threading.Lock(),
            )
        with init_lock:
            runtime = self.__dict__.get("_ctv_runtime")
            if runtime is not None:
                return runtime
            config = self._legacy_ctv_runtime_config()
            runtime = self._make_ctv_runtime_service(config)
            self.__dict__["_ctv_runtime"] = runtime
            # The service is now the sole configuration owner. Removing the
            # pre-init store prevents an old override from being replayed if a
            # later compatibility property updates the live service.
            self.__dict__.pop("_ctv_runtime_compat_config", None)
        return runtime

    def _ctv_runtime_config_value(self, name: str) -> object:
        runtime = self.__dict__.get("_ctv_runtime")
        if runtime is not None:
            return getattr(runtime.config, name)
        return getattr(self._legacy_ctv_runtime_config(), name)

    def _set_ctv_runtime_config_value(self, name: str, value: object) -> None:
        runtime = self.__dict__.get("_ctv_runtime")
        if runtime is not None:
            runtime.replace_config(**{name: value})
            return
        init_lock = self.__dict__.get("_ctv_runtime_init_lock")
        if init_lock is None:
            init_lock = self.__dict__.setdefault(
                "_ctv_runtime_init_lock",
                threading.Lock(),
            )
        with init_lock:
            runtime = self.__dict__.get("_ctv_runtime")
            if runtime is not None:
                runtime.replace_config(**{name: value})
                return
            overrides = self.__dict__.setdefault("_ctv_runtime_compat_config", {})
            overrides[name] = value

    @property
    def ctv_broadcaster_enabled(self) -> bool:
        return bool(self._ctv_runtime_config_value("enabled"))

    @ctv_broadcaster_enabled.setter
    def ctv_broadcaster_enabled(self, value: bool) -> None:
        self._set_ctv_runtime_config_value("enabled", bool(value))

    @property
    def ctv_broadcaster_wallet(self) -> str | None:
        value = self._ctv_runtime_config_value("wallet")
        return None if value is None else str(value)

    @ctv_broadcaster_wallet.setter
    def ctv_broadcaster_wallet(self, value: str | None) -> None:
        self._set_ctv_runtime_config_value("wallet", value)

    @property
    def ctv_broadcaster_fee_sats(self) -> int:
        return int(self._ctv_runtime_config_value("fee_sats"))

    @ctv_broadcaster_fee_sats.setter
    def ctv_broadcaster_fee_sats(self, value: int) -> None:
        self._set_ctv_runtime_config_value("fee_sats", int(value))

    @property
    def ctv_broadcaster_limit(self) -> int:
        return int(self._ctv_runtime_config_value("limit"))

    @ctv_broadcaster_limit.setter
    def ctv_broadcaster_limit(self, value: int) -> None:
        self._set_ctv_runtime_config_value("limit", int(value))

    @property
    def ctv_broadcaster_chunk_size(self) -> int:
        return int(self._ctv_runtime_config_value("chunk_size"))

    @ctv_broadcaster_chunk_size.setter
    def ctv_broadcaster_chunk_size(self, value: int) -> None:
        self._set_ctv_runtime_config_value("chunk_size", int(value))

    @property
    def ctv_broadcaster_interval_seconds(self) -> float:
        return float(self._ctv_runtime_config_value("interval_seconds"))

    @ctv_broadcaster_interval_seconds.setter
    def ctv_broadcaster_interval_seconds(self, value: float) -> None:
        self._set_ctv_runtime_config_value("interval_seconds", float(value))

    @property
    def ctv_fanout_broadcast_daemon(self) -> CtvFanoutBroadcastDaemon | None:
        runtime = self.__dict__.get("_ctv_runtime")
        if runtime is not None:
            return runtime.daemon
        init_lock = self.__dict__.get("_ctv_runtime_init_lock")
        if init_lock is None:
            init_lock = self.__dict__.setdefault(
                "_ctv_runtime_init_lock",
                threading.Lock(),
            )
        with init_lock:
            runtime = self.__dict__.get("_ctv_runtime")
            if runtime is None:
                return self.__dict__.get("_ctv_runtime_compat_daemon")
            return runtime.daemon

    @ctv_fanout_broadcast_daemon.setter
    def ctv_fanout_broadcast_daemon(
        self,
        daemon: CtvFanoutBroadcastDaemon | None,
    ) -> None:
        runtime = self.__dict__.get("_ctv_runtime")
        if runtime is not None:
            runtime.daemon = daemon
            return
        init_lock = self.__dict__.get("_ctv_runtime_init_lock")
        if init_lock is None:
            init_lock = self.__dict__.setdefault(
                "_ctv_runtime_init_lock",
                threading.Lock(),
            )
        with init_lock:
            runtime = self.__dict__.get("_ctv_runtime")
            if runtime is None:
                self.__dict__["_ctv_runtime_compat_daemon"] = daemon
            else:
                runtime.daemon = daemon

    @property
    def ctv_broadcaster_processed_rows_total(self) -> int:
        return self._ensure_ctv_runtime().processed_rows_total

    @property
    def ctv_broadcaster_pass_count(self) -> int:
        return self._ensure_ctv_runtime().pass_count

    def _ensure_ctv_broadcaster_metrics_state(self) -> None:
        self._ensure_ctv_runtime()

    def _record_ctv_fanout_broadcaster_progress(self) -> None:
        self._ensure_ctv_runtime().record_progress()

    def observe_ctv_fanout_broadcaster_pass(self, elapsed_seconds: float) -> None:
        self._ensure_ctv_runtime().observe_pass(elapsed_seconds)

    def observe_ctv_fanout_broadcaster_chunk(
        self,
        result: CtvFanoutChunkResult,
    ) -> None:
        self._ensure_ctv_runtime().observe_chunk(result)

    def _record_ctv_fanout_broadcaster_yield(self) -> None:
        self._ensure_ctv_runtime().record_yield()

    def _ensure_worker_metrics_state(self) -> None:
        if not hasattr(self, "worker_metrics_lock"):
            self.worker_metrics_lock = threading.Lock()
        if not hasattr(self, "worker_share_counts"):
            self.worker_share_counts = {}
        if not hasattr(self, "worker_rejection_counts"):
            self.worker_rejection_counts = {}

    def _ensure_initial_job_state(self) -> InitialJobState:
        state = self.__dict__.get("_initial_job_state")
        if state is None:
            pending = self.__dict__.get("_pending_initial_jobs_compat")
            if pending is None:
                pending = {}
                self.__dict__["_pending_initial_jobs_compat"] = pending
            state = InitialJobState(
                InitialJobConfig(
                    max_pending=int(
                        self.__dict__.get(
                            "_max_pending_compat",
                            DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS,
                        )
                    ),
                    timeout_seconds=float(
                        self.__dict__.get(
                            "_timeout_seconds_compat",
                            DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS,
                        )
                    ),
                    max_workers=int(
                        self.__dict__.get(
                            "_max_workers_compat",
                            DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
                        )
                    ),
                ),
                pending,
            )
            state.queue_rejection_count = int(
                self.__dict__.get("_queue_rejection_count_compat", 0)
            )
            state.timeout_count = int(self.__dict__.get("_timeout_count_compat", 0))
            state.cancelled_count = int(
                self.__dict__.get("_cancelled_count_compat", 0)
            )
            state.coalesced_count = int(
                self.__dict__.get("_coalesced_count_compat", 0)
            )
            state.sent_count = int(self.__dict__.get("_sent_count_compat", 0))
            state.failed_count = int(self.__dict__.get("_failed_count_compat", 0))
            state.superseded_count = int(
                self.__dict__.get("_superseded_count_compat", 0)
            )
            state.queue_capacity_reclaimed_count = int(
                self.__dict__.get(
                    "_queue_capacity_reclaimed_count_compat",
                    0,
                )
            )
            state.delivery_latency_seconds_sum = float(
                self.__dict__.get("_delivery_latency_seconds_sum_compat", 0.0)
            )
            state.delivery_latency_count = int(
                self.__dict__.get("_delivery_latency_count_compat", 0)
            )
            state.last_delivery_monotonic = self.__dict__.get(
                "_last_delivery_monotonic_compat"
            )
            state = self.__dict__.setdefault("_initial_job_state", state)
        self.__dict__["_initial_job_tracker"] = state.tracker
        if not hasattr(self, "handler_thread_count"):
            self.handler_thread_count = 0
        if not hasattr(self, "peak_active_connection_count"):
            self.peak_active_connection_count = len(getattr(self, "clients", ()))
        return state

    def delivery_queue_limit(self) -> int:
        pending_limit = int(
            getattr(
                self,
                "stratum_max_pending_initial_jobs",
                DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS,
            )
        )
        connection_limit = int(
            getattr(
                self,
                "stratum_max_connections",
                DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
            )
        )
        return max(
            int(getattr(self, "tip_refresh_max_workers", DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS)),
            pending_limit,
            connection_limit if connection_limit > 0 else pending_limit,
        )

    def _tip_refresh_prune_evicted_jobs(
        self,
        now: float,
        force: bool,
    ) -> None:
        """Resolve the retained-prune compatibility seam at call time."""
        override = self.__dict__.get("prune_evicted_job_graveyard")
        if callable(override):
            override(now=now, force=force)
            return
        self._ensure_job_delivery_service().prune_retained(
            now=now,
            force=force,
        )

    def _ensure_tip_refresh_service(self) -> TipRefreshService:
        service = self.__dict__.get("_tip_refresh_service")
        if service is not None:
            return service
        init_lock = self.__dict__.setdefault(
            "_tip_refresh_service_init_lock",
            threading.Lock(),
        )
        with init_lock:
            service = self.__dict__.get("_tip_refresh_service")
            if service is not None:
                return service
            service = TipRefreshService(
                TipRefreshConfig(
                    blockpoll_seconds=float(
                        getattr(self, "blockpoll_seconds", DEFAULT_PRISM_BLOCKPOLL_SECONDS)
                    ),
                    blockwait_timeout_seconds=float(
                        getattr(
                            self,
                            "blockwait_timeout_seconds",
                            DEFAULT_PRISM_BLOCKWAIT_TIMEOUT_SECONDS,
                        )
                    ),
                    failure_holdoff_seconds=float(
                        getattr(
                            self,
                            "tip_refresh_failure_holdoff_seconds",
                            DEFAULT_PRISM_TIP_REFRESH_FAILURE_HOLDOFF_SECONDS,
                        )
                    ),
                    max_workers=int(
                        getattr(
                            self,
                            "tip_refresh_max_workers",
                            DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS,
                        )
                    ),
                    submit_tip_max_age_seconds=float(
                        getattr(
                            self,
                            "submit_tip_max_age_seconds",
                            DEFAULT_PRISM_SUBMIT_TIP_MAX_AGE_SECONDS,
                        )
                    ),
                    failure_exit_seconds=float(
                        getattr(
                            self,
                            "template_refresh_failure_exit_seconds",
                            DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
                        )
                    ),
                    watchdog_timeout_seconds=float(
                        getattr(self, "watchdog_timeout_seconds", 120.0)
                    ),
                    payout_reconcile_supersession_retries=int(
                        getattr(
                            self,
                            "payout_reconcile_supersession_retries",
                            DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
                        )
                    ),
                ),
                TipRefreshPorts(
                    rpc_call=self._tip_refresh_rpc_call,
                    rpc_call_with_timeout=lambda method, params, timeout: (
                        self.rpc.call(method, params, timeout=timeout)
                    ),
                    payout_state=self._ensure_payout_state_service,
                    job_bundles=self._ensure_job_bundle_service,
                    delivery=JobDeliveryTipRefreshPort(
                        registry=self._ensure_session_registry,
                        delivery=self._ensure_job_delivery_service(),
                        submit_task=self._submit_delivery_task,
                        disconnect=lambda client: self.disconnect_client(client),
                    ),
                    mark_progress_pending=self._progress_note_refresh_pending,
                    observe_progress_tip_poll=self._record_progress_tip_poll,
                    publish_progress_work=self._record_progress_publication,
                    start_progress_refresh=lambda: (
                        self._ensure_progress_health_service().start_refresh()
                    ),
                    cancel_obsolete_bundle_builds=lambda tip, generation: (
                        self._cancel_obsolete_job_bundle_builds(
                            current_tip=tip,
                            payout_state_generation=generation,
                        )
                    ),
                    cancel_obsolete_job_builds=self._cancel_obsolete_job_builds,
                    prune_evicted_jobs=self._tip_refresh_prune_evicted_jobs,
                    delivery_queue_limit=self.delivery_queue_limit,
                    stop_requested=self._tip_refresh_stop_requested,
                    heartbeat=self._record_heartbeat,
                    remove_heartbeat=self._remove_watchdog_heartbeat,
                    chain_view_untrusted=lambda: bool(
                        getattr(self, "reorg_reconciler_enabled", True)
                        and self.qbit_chain_view_untrusted()
                    ),
                    ensure_reorg_current=lambda tip: (
                        self.ensure_reorg_reconciled_for_current_tip(
                            expected_tip_hash=tip
                        )
                    ),
                    observe_job_build_elapsed=self.observe_job_build_elapsed,
                    fetch_snapshot=lambda: (
                        self._ensure_job_bundle_service()
                        .template_repository.fetch_coherent_snapshot()
                    ),
                    ensure_reorg_tip=lambda tip: self.ensure_reorg_reconciled_for_tip(tip),
                    wait_for_execution_permit=lambda timeout: (
                        self._ensure_shutdown_controller().wait_for_no_active_writer(
                            {"accepted_block_handling"},
                            timeout,
                        )
                    ),
                    wait_for_stop=self._tip_refresh_wait_for_stop,
                    hard_exit=lambda code: os._exit(code),
                    fetch_snapshot_for_tip=lambda observed_tip: (
                        self._ensure_job_bundle_service()
                        .template_repository.fetch_coherent_snapshot(observed_tip)
                    ),
                ),
                monotonic=lambda: time.monotonic(),
                state_lock=self._ensure_session_registry().lock,
            )
            object.__setattr__(self, "_tip_refresh_service", service)
            return service

    def _tip_refresh_rpc_call(
        self,
        method: str,
        params: list[object] | None,
    ) -> object:
        if params is None:
            return self.rpc.call(method)
        return self.rpc.call(method, params)

    def _tip_refresh_stop_requested(self) -> bool:
        stop_event = getattr(self, "stop_event", None)
        return bool(stop_event is not None and stop_event.is_set())

    def _tip_refresh_wait_for_stop(self, seconds: float) -> bool:
        stop_event = getattr(self, "stop_event", None)
        return bool(stop_event is not None and stop_event.wait(seconds))

    def _ensure_tip_refresh_state(self) -> None:
        self._ensure_tip_refresh_service()

    def _retain_collection_refresh(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
        payout_state_generation: int,
    ) -> None:
        self._ensure_tip_refresh_service().retain_collection_refresh(
            snapshot,
            observation_sequence,
            payout_state_generation,
        )

    def _retained_collection_artifacts(self) -> CachedTemplateArtifacts | None:
        """Return retained artifacts while their published work stays current.

        A same-tip poll advances its observation sequence before atomically
        replacing ``tip_template_snapshot``. The published snapshot remains
        reusable on both sides of that handoff even if the retained marker has
        not yet been updated; a new tip or payout generation still invalidates
        it immediately.
        """
        return self._ensure_tip_refresh_service().retained_collection_artifacts()

    def _retain_current_collection_refresh_if_unrepresented(self) -> None:
        """Keep the last published collection work when the fleet empties."""
        self._ensure_tip_refresh_service().retain_current_collection_refresh_if_unrepresented()

    def _note_collection_identity_available(self, client: ClientState) -> None:
        """Wake a retained collection refresh as soon as a client is eligible."""
        self._ensure_tip_refresh_service().note_collection_identity_available(client)

    def _consume_retained_collection_refresh(
        self,
        context: PrismJobContext,
    ) -> None:
        """Consume retention only after its collection work was delivered."""
        self._ensure_tip_refresh_service().consume_retained_collection_refresh(context)

    def tip_refresh_is_pending(self) -> bool:
        return self._tip_refresh_pending()

    def _tip_refresh_pending(self) -> bool:
        return self._ensure_tip_refresh_service().pending()

    def _mark_tip_refresh_pending(self, _observation: object) -> int:
        return self._ensure_tip_refresh_service().mark_pending(_observation)

    def _claim_tip_refresh_pending(self) -> int | None:
        """Snapshot pending work without replacing a newer producer's token."""
        return self._ensure_tip_refresh_service().claim_pending()

    def _mark_tip_refresh_pending_for_poll(
        self,
        owned_token: int | None,
        _observation: object,
    ) -> int | None:
        """Mark poll-owned work only while no newer producer has superseded it."""
        return self._ensure_tip_refresh_service().mark_pending_for_poll(
            owned_token,
            _observation,
        )

    def _clear_tip_refresh_pending(self, token: int) -> None:
        self._ensure_tip_refresh_service().clear_pending(token)

    def _clear_tip_refresh_pending_for_completed_refresh(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
        payout_state_generation: int,
        pending_signal_token: int | None = None,
    ) -> bool:
        """Atomically acknowledge pending work handled by a completed poll."""
        return self._ensure_tip_refresh_service().clear_pending_for_completed_refresh(
            snapshot,
            observation_sequence,
            payout_state_generation,
            pending_signal_token,
        )

    def _schedule_tip_refresh_retry(self) -> None:
        self._ensure_tip_refresh_service().schedule_retry()

    def _observe_tip_refresh_seconds(self, name: str, elapsed_seconds: float) -> None:
        self._ensure_tip_refresh_service().observe_seconds(name, elapsed_seconds)

    def _observe_tip_refresh_build_phase(
        self,
        phase: str,
        elapsed_seconds: float,
    ) -> None:
        self._ensure_tip_refresh_service().observe_build_phase(
            phase,
            elapsed_seconds,
        )

    def _record_tip_refresh_ipc_bytes(self, direction: str, byte_count: int) -> None:
        self._ensure_tip_refresh_service().record_ipc_bytes(direction, byte_count)

    def _record_tip_refresh_client_result(self, result: str) -> None:
        self._ensure_tip_refresh_service().record_client_result(result)

    def _record_tip_refresh_cancellation(self, stage: str) -> None:
        self._ensure_tip_refresh_service().record_cancellation(stage)

    def _tip_refresh_future_started(self) -> None:
        self._ensure_tip_refresh_service().future_started()

    def _tip_refresh_future_finished(self, _future: Future[RefreshResult]) -> None:
        self._ensure_tip_refresh_service().future_finished(_future)

    def tip_refresh_executor(self) -> _BoundedPriorityExecutor:
        return self._ensure_tip_refresh_service().executor()

    def initial_job_executor(self) -> _BoundedPriorityExecutor:
        return self._ensure_job_delivery_service().initial_executor()

    def shutdown_initial_job_executor(self) -> None:
        self._ensure_job_delivery_service().shutdown_initial_executor()

    def _cancel_initial_job_future(self, future: Future[bool]) -> bool:
        return self._ensure_job_delivery_service().cancel_initial_future(future)

    def shutdown_tip_refresh_executor(self) -> None:
        self._ensure_job_delivery_service().shutdown_initial_executor()
        self._ensure_tip_refresh_service().shutdown()
        # These owners must close even if the refresh scheduler exceeded its
        # bounded join. Closing them cancels work that can otherwise keep the
        # scheduler, its non-daemon executor workers, and the process alive.
        self.shutdown_job_build_executor()
        self.shutdown_payout_artifact_executor()

    def _initial_request_current_locked(self, request: PendingInitialJob) -> bool:
        return self._ensure_job_delivery_service().initial_request_current_locked(
            request
        )

    def _initial_request_cancelled(self, request: PendingInitialJob) -> bool:
        return self._ensure_job_delivery_service().initial_request_cancelled(request)

    def _cancel_pending_initial_job_locked(
        self,
        client: ClientState,
        *,
        count: bool,
    ) -> PendingInitialJob | None:
        return self._ensure_job_delivery_service().cancel_initial_job_locked(
            client,
            count=count,
        )

    def _client_has_current_tip_job_locked(self, client: ClientState) -> bool:
        return self._ensure_job_delivery_service().client_has_current_tip_job_locked(
            client,
            self._ensure_job_delivery_service().current_job_source(),
        )

    def _reset_delivery_failure_if_coverage_restored_locked(self) -> None:
        authorized_clients = [
            client
            for client in self.clients
            if client.subscribed and client.authorized and client.worker is not None
        ]
        if not authorized_clients:
            self._ensure_observability_service().reset_delivery_failure()
            return
        delivery_service = self._ensure_job_delivery_service()
        source = delivery_service.current_job_source()
        current = sum(
            1
            for client in authorized_clients
            if delivery_service.client_has_current_tip_job_locked(client, source)
        )
        if current / len(authorized_clients) >= 0.95:
            self._ensure_observability_service().reset_delivery_failure()

    def note_initial_job_delivered(
        self,
        client: ClientState,
        *,
        validated_current: bool = False,
    ) -> None:
        self._ensure_job_delivery_service().note_initial_job_delivered(
            client,
            validated_current=validated_current,
        )

    def schedule_initial_job(self, client: ClientState) -> bool:
        return self._ensure_job_delivery_service().schedule_initial_job(client)

    def request_initial_job_delivery(self, client: ClientState) -> bool:
        return self._ensure_job_delivery_service().schedule_initial_job(client)

    def cancel_initial_job_delivery(self, client: ClientState) -> None:
        self._ensure_job_delivery_service().cancel_initial_job(client, count=True)

    def _submit_initial_job_request(self, request: PendingInitialJob) -> bool:
        return self._ensure_job_delivery_service().submit_initial_job_request(request)

    def _initial_job_future_finished(
        self,
        request: PendingInitialJob,
        future: Future[bool],
    ) -> None:
        self._ensure_job_delivery_service().initial_job_future_finished(
            request,
            future,
        )

    def _run_initial_job(self, request: PendingInitialJob) -> bool:
        return self._ensure_job_delivery_service().run_initial_job(request)

    def _template_artifacts_are_current(self, artifacts: CachedTemplateArtifacts) -> bool:
        current = (
            self._ensure_job_bundle_service()
            .template_repository.current_artifacts()
        )
        return (
            current is artifacts
            or (
                current is not None
                and current.fingerprint == artifacts.fingerprint
                and current.generation == artifacts.generation
            )
        )

    def _issuance_artifacts_current(self, artifacts: CachedTemplateArtifacts) -> bool:
        """Issuance-side currency for direct job delivery.

        Current means either the live template view (the newest stored
        artifacts) or exactly the published snapshot while the published tip
        still owns share classification. During a detected-but-unpublished
        refresh, pinned published-snapshot work must stay deliverable; judging
        it against the detected-tip globals would defer every direct issuance
        for the entire construction window that publication is deliberately
        decoupled from.
        """
        if self._template_artifacts_are_current(artifacts):
            return True
        with self.lock:
            published = getattr(self, "current_tip_first_seen", None)
            published_snapshot = getattr(self, "tip_template_snapshot", None)
            return bool(
                published is not None
                and published_snapshot is not None
                and published_snapshot.bestblockhash == published[0]
                and artifacts.previousblockhash == published[0]
                and published_snapshot.template_fingerprint == artifacts.fingerprint
                and self._published_tip_authoritative_locked(time.monotonic())
            )

    def _payout_delivery(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int,
    ) -> Any:
        """Use cancellable admission while retaining focused gate test seams."""
        gate = self._ensure_payout_state_service().delivery_gate
        delivery_cancelable = getattr(gate, "delivery_cancelable", None)
        if callable(delivery_cancelable):
            return delivery_cancelable(
                cancelled,
                generation=generation,
                priority=True,
            )
        delivery = gate.delivery
        try:
            return delivery(cancelled)
        except TypeError:
            return delivery()

    @staticmethod
    def _submit_delivery_task(
        executor: object,
        function: Callable[..., Any],
        *args: object,
        priority: int,
    ) -> Future[Any]:
        submit = getattr(executor, "submit")
        if isinstance(executor, _BoundedPriorityExecutor):
            return submit(function, *args, priority=priority)
        return submit(function, *args)

    def _deliver_initial_bundle(
        self,
        request: PendingInitialJob,
        artifacts: CachedTemplateArtifacts,
        bundle: CachedJobBundle,
    ) -> bool | None:
        return self._ensure_job_delivery_service().deliver_initial_bundle(
            request,
            artifacts,
            bundle,
        )

    def sweep_initial_job_timeouts(self, *, now: float | None = None) -> int:
        return self._ensure_job_delivery_service().sweep_initial_job_timeouts(now=now)

    def initial_job_timeout_loop(self) -> None:
        self._ensure_job_delivery_service().initial_job_timeout_loop()

    def _make_background_service_registry(self) -> BackgroundServiceRegistry:
        """Describe process loops in their historical shutdown-join order."""
        specifications = [
            BackgroundServiceSpec(
                name="qbit_blockpoll",
                thread_name="prism-qbit-block-poll",
                target=self.blockpoll_loop,
                daemon=True,
                join_timeout=1.0,
                watchdog_monitored=True,
            ),
            BackgroundServiceSpec(
                name="block_submitter",
                thread_name="prism-block-submitter",
                target=self.block_submit_loop,
                daemon=True,
                join_timeout=1.0,
                watchdog_monitored=True,
            ),
        ]
        if bool(getattr(self, "blockwait_enabled", False)):
            specifications.append(
                BackgroundServiceSpec(
                    name="qbit_blockwait",
                    thread_name="prism-qbit-block-wait",
                    target=self.blockwait_loop,
                    daemon=True,
                    join_timeout=1.0,
                    watchdog_monitored=True,
                )
            )
        if float(getattr(self, "vardiff_idle_sweep_seconds", 0.0)) > 0:
            specifications.append(
                BackgroundServiceSpec(
                    name="vardiff_idle_sweep",
                    thread_name="prism-vardiff-idle-sweep",
                    target=self.vardiff_idle_sweep_loop,
                    daemon=True,
                    join_timeout=1.0,
                    watchdog_monitored=True,
                )
            )
        if float(getattr(self, "stratum_initial_job_timeout_seconds", 0.0)) > 0:
            specifications.append(
                BackgroundServiceSpec(
                    name="initial_job_timeout_sweep",
                    thread_name="prism-initial-job-timeouts",
                    target=self.initial_job_timeout_loop,
                    daemon=True,
                    join_timeout=1.0,
                    watchdog_monitored=False,
                )
            )
        specifications.append(
            BackgroundServiceSpec(
                name="share_writer",
                thread_name="prism-share-writer",
                target=self.share_append_loop,
                daemon=True,
                join_timeout=5.0,
                watchdog_monitored=True,
            )
        )
        if bool(getattr(self, "ctv_broadcaster_enabled", False)):
            specifications.append(self._ensure_ctv_runtime().background_service_spec())
        if bool(getattr(self, "watchdog_enabled", False)):
            specifications.append(
                BackgroundServiceSpec(
                    name="watchdog",
                    thread_name="prism-watchdog",
                    target=self.watchdog_loop,
                    daemon=True,
                    join_timeout=1.0,
                    watchdog_monitored=False,
                )
            )
        if bool(getattr(self, "audit_bind", None)) and bool(
            getattr(self, "audit_port", 0)
        ):
            specifications.append(self._health_snapshot_service_spec())
            specifications.append(self._metrics_snapshot_service_spec())
        return BackgroundServiceRegistry(specifications)

    def _health_snapshot_service_spec(self) -> BackgroundServiceSpec:
        return BackgroundServiceSpec(
            name="health_snapshot_refresher",
            thread_name="prism-health-snapshot-refresher",
            target=self.health_snapshot_loop,
            daemon=True,
            join_timeout=1.0,
            watchdog_monitored=False,
        )

    def _metrics_snapshot_service_spec(self) -> BackgroundServiceSpec:
        return BackgroundServiceSpec(
            name="metrics_snapshot_refresher",
            thread_name="prism-metrics-snapshot-refresher",
            target=self.metrics_snapshot_loop,
            daemon=True,
            join_timeout=1.0,
            watchdog_monitored=False,
        )

    def _ensure_background_services(self) -> BackgroundServiceRegistry:
        registry = getattr(self, "_background_services", None)
        if registry is None:
            registry = self._make_background_service_registry()
            self._background_services = registry
        return registry

    def _ensure_watchdog_service(self) -> WatchdogService:
        service = self.__dict__.get("_watchdog_service")
        if service is None:
            service = WatchdogService(
                WatchdogPorts(
                    wait_for_stop=lambda timeout: self.stop_event.wait(timeout),
                    interval_seconds=lambda: float(self.watchdog_interval_seconds),
                    monotonic=time.monotonic,
                    publication_failure_expired=(
                        self.publication_progress_failure_expired
                    ),
                    publication_budget_seconds=lambda: float(
                        getattr(
                            self,
                            "template_refresh_failure_exit_seconds",
                            DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
                        )
                    ),
                    liveness_enabled=lambda: bool(
                        getattr(self, "watchdog_enabled", True)
                    ),
                    overdue_heartbeats=self._overdue_heartbeats,
                    liveness_timeout_seconds=lambda: float(
                        self.watchdog_timeout_seconds
                    ),
                    log=lambda message: print(message, flush=True),
                    exit_process=lambda code: os._exit(code),
                )
            )
            self.__dict__["_watchdog_service"] = service
        return service

    def _start_background_service(self, name: str) -> threading.Thread:
        registry = self._ensure_background_services()
        return registry.start(
            name,
            on_started=lambda specification: (
                self._record_heartbeat(specification.name)
                if specification.watchdog_monitored
                else None
            ),
        )

    def _start_secondary_accept_service(
        self,
        server: socket.socket,
        profile: StratumListenerProfile,
    ) -> threading.Thread:
        registry = self._ensure_background_services()
        service_name = profile.heartbeat_name
        registry.register_if_absent(
            BackgroundServiceSpec(
                name=service_name,
                thread_name=f"prism-stratum-accept-{profile.name}",
                target=lambda: self.accept_loop(server, profile),
                daemon=True,
                join_timeout=1.0,
                watchdog_monitored=True,
                registration_identity=(
                    "secondary_stratum_accept",
                    id(server),
                    id(profile),
                ),
            )
        )
        return self._start_background_service(service_name)

    def _record_heartbeat(self, name: str) -> None:
        self._ensure_watchdog_state()
        with self._heartbeats_lock:
            self._heartbeats[name] = time.monotonic()

    def _overdue_heartbeats(self, now: float) -> list[str]:
        self._ensure_watchdog_state()
        with self._heartbeats_lock:
            paused = set(self._watchdog_pauses)
            return sorted(
                name
                for name, last in self._heartbeats.items()
                if name not in paused and now - last > self.watchdog_timeout_seconds
            )

    def _pause_watchdog_heartbeat(self, name: str) -> None:
        self._ensure_watchdog_state()
        with self._heartbeats_lock:
            self._watchdog_pauses[name] = self._watchdog_pauses.get(name, 0) + 1
            self._heartbeats[name] = time.monotonic()

    def _resume_watchdog_heartbeat(self, name: str) -> None:
        self._ensure_watchdog_state()
        with self._heartbeats_lock:
            depth = self._watchdog_pauses.get(name, 0)
            if depth <= 1:
                self._watchdog_pauses.pop(name, None)
            else:
                self._watchdog_pauses[name] = depth - 1
            self._heartbeats[name] = time.monotonic()

    def _remove_watchdog_heartbeat(self, name: str) -> None:
        self._ensure_watchdog_state()
        with self._heartbeats_lock:
            self._heartbeats.pop(name, None)
            self._watchdog_pauses.pop(name, None)

    def _registered_watchdog_heartbeat_names(self, *names: str) -> tuple[str, ...]:
        self._ensure_watchdog_state()
        with self._heartbeats_lock:
            return tuple(name for name in names if name in self._heartbeats)

    def stratum_accept_heartbeat_names(self) -> tuple[str, ...]:
        return configured_accept_heartbeat_names(
            getattr(self, "listener_profiles", None)
        )

    def _ensure_session_registry(self) -> SessionRegistry:
        registry = getattr(self, "_session_registry", None)
        if registry is not None:
            clients = getattr(self, "clients", registry.clients)
            if clients is not registry.clients:
                registry.adopt_clients(clients)
            rejection_counts = getattr(
                self,
                "connection_limit_rejection_counts",
                registry.rejection_counts,
            )
            if rejection_counts is not registry.rejection_counts:
                registry.rejection_counts = rejection_counts
            _CoordinatorSessionRuntime(self).sync_registry_metrics(registry)
            return registry
        lock = getattr(self, "lock", None)
        if lock is None:
            lock = threading.RLock()
            self.lock = lock
        clients = getattr(self, "clients", None)
        if clients is None:
            clients = set()
            self.clients = clients
        rejection_counts = getattr(self, "connection_limit_rejection_counts", None)
        if not isinstance(rejection_counts, dict):
            rejection_counts = {"global": 0, "username": 0}
            self.connection_limit_rejection_counts = rejection_counts
        candidate = SessionRegistry(
            lock=lock,
            clients=clients,
            connection_generation=int(getattr(self, "connection_counter", 0)),
            rejection_counts=rejection_counts,
        )
        candidate.peak_active_connections = max(
            candidate.peak_active_connections,
            int(getattr(self, "peak_active_connection_count", 0)),
        )
        candidate.handler_thread_count = int(
            getattr(self, "handler_thread_count", candidate.handler_thread_count)
        )
        registry = self.__dict__.setdefault("_session_registry", candidate)
        _CoordinatorSessionRuntime(self).sync_registry_metrics(registry)
        return registry

    def _job_delivery_current_tip_locked(self) -> str | None:
        tip_service = self.__dict__.get("_tip_refresh_service")
        if tip_service is None:
            return None
        published = tip_service.published_snapshot()
        first_seen = published.first_seen
        if first_seen is not None:
            return str(first_seen[0])
        snapshot = published.template
        if snapshot is not None:
            return str(snapshot.bestblockhash)
        return None

    def _ensure_retained_job_index(self) -> RetainedJobIndex:
        index = self.__dict__.get("_retained_job_index")
        if index is None:
            index = RetainedJobIndex(
                graveyard=self.__dict__.get("_evicted_graveyard_compat"),
                by_connection=self.__dict__.get("_evicted_by_connection_compat"),
                same_tip_by_connection=self.__dict__.get(
                    "_evicted_same_tip_by_connection_compat"
                ),
                same_tip_job_ids=self.__dict__.get(
                    "_evicted_same_tip_job_ids_compat"
                ),
                same_tip_ttl_seconds=float(
                    self.__dict__.get(
                        "_same_tip_ttl_seconds_compat",
                        DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
                    )
                ),
                same_tip_per_connection=int(
                    self.__dict__.get(
                        "_same_tip_per_connection_compat",
                        DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION,
                    )
                ),
                stale_grace_seconds=float(
                    self.__dict__.get(
                        "_stale_grace_seconds_compat",
                        DEFAULT_PRISM_STALE_GRACE_SECONDS,
                    )
                ),
            )
            index.expiration_counts = self.__dict__.get(
                "_evicted_expiration_counts_compat", index.expiration_counts
            )
            index.capacity_eviction_counts = self.__dict__.get(
                "_evicted_capacity_eviction_counts_compat",
                index.capacity_eviction_counts,
            )
            index.submit_counts = self.__dict__.get(
                "_evicted_submit_counts_compat", index.submit_counts
            )
            index.next_prune_monotonic = float(
                self.__dict__.get("_evicted_next_prune_monotonic_compat", 0.0)
            )
            index.index_tip_hash = self.__dict__.get(
                "_evicted_index_tip_hash_compat"
            )
            index = self.__dict__.setdefault("_retained_job_index", index)
        current_tip = self._job_delivery_current_tip_locked()
        index.adopt(
            graveyard=index.graveyard,
            by_connection=index.by_connection,
            same_tip_by_connection=index.same_tip_by_connection,
            same_tip_job_ids=index.same_tip_job_ids,
            current_tip=current_tip,
        )
        return index

    def _sync_retained_job_index_compatibility(self) -> None:
        self._ensure_retained_job_index()

    def _next_job_delivery_id(self) -> str:
        return self._ensure_job_delivery_service().next_job_id()

    def _job_delivery_send_difficulty(
        self, client: ClientState, job: direct_stratum.DirectQbitStratumJob
    ) -> None:
        override = self.__dict__.get("send_difficulty")
        if override is not None:
            override(client, job)
        else:
            client.send(stratum_difficulty_payload(job.share_difficulty))

    def _job_delivery_send_job(
        self, client: ClientState, job: direct_stratum.DirectQbitStratumJob
    ) -> None:
        override = self.__dict__.get("send_job")
        if override is not None:
            override(client, job)
        else:
            client.send(stratum_job_payload(job))

    def _live_tip_hash(self) -> str:
        return str(self.rpc.call("getbestblockhash"))

    def _clear_job_template_if_current(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> None:
        self._ensure_job_bundle_service().template_repository.clear_if_current(
            artifacts
        )

    def _record_job_build_failure(self) -> None:
        self._ensure_job_bundle_service().record_failure()

    def _current_payout_generation(self) -> int:
        return int(self._ensure_payout_state_service().snapshot().generation)

    def _payout_delivery_snapshot(self) -> object:
        return self._ensure_payout_state_service().snapshot()

    def _payout_delivery_cancelable(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int,
        priority: bool,
    ) -> object:
        return self._ensure_payout_state_service().delivery_gate.delivery_cancelable(
            cancelled,
            generation=generation,
            priority=priority,
        )

    def _job_delivery_observe_payout_admission(
        self,
        admission: object,
        *,
        generation: int,
        fallback_wait_seconds: float,
    ) -> None:
        override = self.__dict__.get("_observe_payout_gate_admission")
        if callable(override):
            override(
                admission,
                generation=generation,
                fallback_wait_seconds=fallback_wait_seconds,
            )
            return
        self._ensure_payout_state_service().observe_gate_admission(
            admission,
            generation=generation,
            fallback_wait_seconds=fallback_wait_seconds,
        )

    def _run_initial_job_override(
        self,
    ) -> Callable[[PendingInitialJob], bool] | None:
        override = self.__dict__.get("_run_initial_job")
        return override if callable(override) else None

    def _deliver_initial_bundle_override(self) -> Callable[..., bool | None] | None:
        override = self.__dict__.get("_deliver_initial_bundle")
        return override if callable(override) else None

    def _submit_initial_job_override(
        self,
    ) -> Callable[[PendingInitialJob], bool] | None:
        override = self.__dict__.get("_submit_initial_job_request")
        return override if callable(override) else None

    def _maybe_send_job_override(self) -> Callable[..., bool] | None:
        override = self.__dict__.get("maybe_send_job")
        return override if callable(override) else None

    def _send_prepared_job_override(
        self,
    ) -> Callable[..., RefreshResult] | None:
        override = self.__dict__.get("send_prepared_job")
        return override if callable(override) else None

    def _build_job_override(self) -> Callable[..., PrismJobContext] | None:
        override = self.__dict__.get("build_job_for_client")
        return override if callable(override) else None

    def _stamp_job_override(self) -> Callable[..., PrismJobContext] | None:
        override = self.__dict__.get("stamp_job_for_client")
        return override if callable(override) else None

    def _apply_job_difficulty_override(self) -> Callable[..., None] | None:
        override = self.__dict__.get("apply_job_difficulty")
        return override if callable(override) else None

    def _send_job_update_override(self) -> Callable[..., None] | None:
        override = self.__dict__.get("send_job_update")
        return override if callable(override) else None

    def _client_needs_refresh_override(self) -> Callable[..., bool] | None:
        override = self.__dict__.get("client_needs_tip_template_refresh")
        return override if callable(override) else None

    def _retained_classify_override(self) -> Callable[..., str] | None:
        override = self.__dict__.get("_evicted_job_class_locked")
        return override if callable(override) else None

    def _ensure_job_delivery_hooks(self) -> DeliveryCompatibilityHooks:
        hooks = self.__dict__.get("_job_delivery_hooks")
        if hooks is None:
            hooks = DeliveryCompatibilityHooks(
                run_initial_override=self._run_initial_job_override,
                deliver_initial_override=self._deliver_initial_bundle_override,
                submit_initial_override=self._submit_initial_job_override,
                maybe_send_override=self._maybe_send_job_override,
                send_prepared_override=self._send_prepared_job_override,
                build_job_override=self._build_job_override,
                stamp_job_override=self._stamp_job_override,
                apply_difficulty_override=self._apply_job_difficulty_override,
                send_update_override=self._send_job_update_override,
                needs_refresh_override=self._client_needs_refresh_override,
                retained_classify_override=self._retained_classify_override,
                split_send_enabled=lambda: (
                    "send_difficulty" in self.__dict__
                    or "send_job" in self.__dict__
                ),
                hot_path_logging_enabled=lambda: bool(
                    getattr(self, "hot_path_log_enabled", False)
                ),
                reorg_reconciler_enabled=lambda: bool(
                    getattr(self, "reorg_reconciler_enabled", True)
                ),
            )
            hooks = self.__dict__.setdefault("_job_delivery_hooks", hooks)
        return hooks

    def _job_delivery_retention_authority_locked(self) -> RetentionAuthority:
        current_tip = self._job_delivery_current_tip_locked()
        first_seen = self.current_tip_first_seen
        cached_parent = self.current_tip_parent
        return RetentionAuthority(
            current_tip=current_tip,
            current_tip_first_delivery=(
                float(first_seen[1])
                if first_seen is not None and first_seen[1] is not None
                else None
            ),
            cached_parent=(
                str(cached_parent[1])
                if cached_parent is not None and cached_parent[0] == current_tip
                else None
            ),
        )

    def _job_delivery_artifacts_parent_current_locked(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        return self._ensure_tip_refresh_service().artifacts_parent_current_locked(
            artifacts,
            now=time.monotonic(),
        )

    def _job_delivery_published_current_locked(
        self,
        context_parent: str,
        *,
        template_fingerprint: str | None,
        template_generation: int,
        lapsed_live_validated: bool,
        payout_generation: int,
    ) -> bool:
        service = self._ensure_tip_refresh_service()
        published = service.published_snapshot()
        snapshot = published.template
        if published.first_seen is None or snapshot is None:
            return False
        if context_parent != published.first_seen[0]:
            return False
        if (
            template_fingerprint is not None
            and snapshot.template_fingerprint != template_fingerprint
        ):
            return False
        if (
            template_generation > 0
            and snapshot.template_generation != template_generation
        ):
            return False
        return bool(
            service.published_tip_authoritative(time.monotonic())
            or lapsed_live_validated
        )

    def _submit_initial_delivery(
        self,
        function: Callable[[PendingInitialJob], bool],
        request: PendingInitialJob,
        *,
        priority: int,
    ) -> Future[Any]:
        return self._submit_delivery_task(
            self.tip_refresh_executor(),
            function,
            request,
            priority=priority,
        )

    def _reconcile_progress_delivery_health(self) -> None:
        service = self.__dict__.get("progress_health_service")
        if service is not None:
            service.reconcile_pending(self._progress_eligibility_snapshot())

    def _ensure_job_delivery_service(self) -> JobDeliveryService:
        initial_state = self._ensure_initial_job_state()
        registry = self._ensure_session_registry()
        retained = self._ensure_retained_job_index()
        preparation = self.__dict__.setdefault(
            "_job_preparation_port",
            _CoordinatorJobPreparation(
                ensure_reorg_current=lambda: (
                    self.ensure_reorg_reconciled_for_current_tip()
                ),
                issuance_artifacts=lambda: self.job_issuance_template_artifacts(),
                shared_bundle=lambda artifacts, worker, cancelled=None, request_source="routine": (
                    self.shared_job_bundle(
                        artifacts,
                        worker,
                        cancelled=cancelled,
                        request_source=request_source,
                    )
                ),
                artifacts_current=lambda artifacts: (
                    self._issuance_artifacts_current(artifacts)
                ),
                clear_artifacts=lambda artifacts: (
                    self._clear_job_template_if_current(artifacts)
                ),
                record_failure=lambda: self._record_job_build_failure(),
                phases=lambda: self._job_build_phases(),
                retained_artifacts=lambda: self._retained_collection_artifacts(),
                chain_view_untrusted=lambda: self.qbit_chain_view_untrusted(),
                admit_idle_bundle_source=lambda client, bundle, allow_uncached: (
                    self._admit_idle_bundle_source(
                        client,
                        bundle,
                        allow_uncached=allow_uncached,
                    )
                ),
                observe_elapsed=lambda elapsed, phases: (
                    self.observe_job_build_elapsed(elapsed, dict(phases))
                ),
                collection_identity=lambda worker: (
                    self._collection_bundle_identity(worker)
                ),
                ready_latched=lambda: (
                    self._ensure_job_bundle_service().ready_latched()
                ),
                template_fingerprint=lambda template: (
                    qbit_template_fingerprint(dict(template))
                ),
            ),
        )
        tip_authority = self.__dict__.setdefault(
            "_job_tip_authority_port",
            _CoordinatorTipAuthority(
                live_tip=lambda: self._live_tip_hash(),
                observe_tip=lambda tip_hash: (
                    self._submit_tip_observation_for_refresh(tip_hash)
                ),
                published_authority=lambda: self.current_tip_first_seen,
                published_authoritative=lambda now: (
                    self._published_tip_authoritative_locked(now)
                ),
                current_tip_locked=lambda: self._job_delivery_current_tip_locked(),
                published_template_locked=lambda: self.tip_template_snapshot,
                snapshot_current_locked=lambda snapshot, sequence: (
                    self._tip_refresh_snapshot_current_locked(snapshot, sequence)
                ),
                artifacts_parent_current_locked=(
                    self._job_delivery_artifacts_parent_current_locked
                ),
                ensure_artifacts_parent_observed=lambda artifacts: (
                    self._ensure_tip_refresh_service()
                    .ensure_artifacts_parent_observed(artifacts)
                ),
                schedule_retry=lambda: self._schedule_tip_refresh_retry(),
                prepared_obsolete=lambda *args: (
                    self._prepared_tip_refresh_obsolete(*args)
                ),
                prepared_token_current_locked=lambda *args: (
                    self._ensure_tip_refresh_service()
                    .token_current_for_payout_snapshot(*args)
                ),
                record_cancellation=lambda stage: (
                    self._record_tip_refresh_cancellation(stage)
                ),
                retention_authority_locked=(
                    self._job_delivery_retention_authority_locked
                ),
                consume_retained_refresh=lambda context: (
                    self._consume_retained_collection_refresh(context)
                ),
                published_current_locked=(
                    self._job_delivery_published_current_locked
                ),
            ),
        )
        payout = self.__dict__.setdefault(
            "_job_payout_delivery_port",
            _CoordinatorPayoutDelivery(
                snapshot=lambda: self._ensure_payout_state_service().snapshot(),
                generation=lambda: int(
                    self._ensure_payout_state_service().snapshot().generation
                ),
                initial_admission=lambda cancelled, generation: (
                    self._payout_delivery(cancelled, generation=generation)
                ),
                admission=lambda cancelled, generation, priority: (
                    self._ensure_payout_state_service()
                    .delivery_gate.delivery_cancelable(
                        cancelled,
                        generation=generation,
                        priority=priority,
                    )
                ),
                observe_admission=lambda admission, generation, fallback_wait_seconds: (
                    self._job_delivery_observe_payout_admission(
                        admission,
                        generation=generation,
                        fallback_wait_seconds=fallback_wait_seconds,
                    )
                ),
                record_first_delivery=lambda generation, delivered: (
                    self._ensure_payout_state_service().record_first_delivery(
                        generation, delivered
                    )
                ),
            ),
        )
        initial_runtime = self.__dict__.setdefault(
            "_initial_job_runtime_port",
            _CoordinatorInitialJobRuntime(
                stopping=lambda: self.stop_event.is_set(),
                wait=lambda timeout: self.stop_event.wait(timeout),
                disconnect=lambda client: self.disconnect_client(client),
                submit_initial=self._submit_initial_delivery,
            ),
        )
        progress = self.__dict__.setdefault(
            "_job_progress_delivery_port",
            _CoordinatorProgressDelivery(
                record_health_delivery=self._record_progress_delivery_to_health,
                reconcile_health_eligibility=(
                    self._reconcile_progress_delivery_health
                ),
            ),
        )
        hooks = self._ensure_job_delivery_hooks()
        service = self.__dict__.get("_job_delivery_service")
        jobs = getattr(self, "jobs", None)
        if jobs is None:
            jobs = {}
            self.jobs = jobs
        if service is not None:
            service.registry = registry
            service.retained = retained
            service.adopt_ports(
                preparation=preparation,
                tip_authority=tip_authority,
                payout=payout,
                initial_runtime=initial_runtime,
                hooks=hooks,
                progress=progress,
                initial_state=initial_state,
                delivery_health_updated=(
                    self._note_delivery_health_updated_locked
                ),
            )
            if jobs is not service.jobs:
                service.adopt_jobs(jobs)
            return service
        candidate = JobDeliveryService(
            registry=registry,
            runtime=JobDeliveryRuntime(
                desired_share_difficulty_fn=lambda client: (
                    self.desired_client_share_difficulty(client)
                ),
                minimum_advertised_difficulty_fn=lambda client: (
                    self.client_minimum_advertised_difficulty(client)
                ),
                share_weight_fn=lambda worker: self.share_weight_for_worker(worker),
                vardiff_config_fn=lambda client: self.client_vardiff_config(client),
                send_difficulty_fn=self._job_delivery_send_difficulty,
                send_job_fn=self._job_delivery_send_job,
                send_job_batch_fn=lambda client, job: client.send_batch(
                    [
                        stratum_difficulty_payload(job.share_difficulty),
                        stratum_job_payload(job),
                    ]
                ),
            ),
            jobs=jobs,
            retained=retained,
            preparation=preparation,
            tip_authority=tip_authority,
            payout=payout,
            initial_runtime=initial_runtime,
            hooks=hooks,
            progress=progress,
            initial_state=initial_state,
            job_counter=int(self.__dict__.get("_job_counter_compat", 0)),
            delivery_health_updated=self._note_delivery_health_updated_locked,
        )
        return self.__dict__.setdefault("_job_delivery_service", candidate)

    def _adopt_legacy_delivery_client(self, client: ClientState) -> None:
        """Register explicit ``__new__`` focused-test clients.

        Production coordinators always admit through S1. Legacy focused
        coordinators have no loaded config and historically called the direct
        delivery facade with an otherwise empty compatibility collection.
        Exact S1 membership remains mandatory once a real coordinator exists.
        """
        if "config" in self.__dict__:
            return
        registry = self._ensure_session_registry()
        with registry.lock:
            if client in registry.clients or registry.clients:
                return
            registry._add_client_locked(client)
            self.clients = registry.clients

    def _ensure_stratum_session_service(self) -> StratumSessionService:
        service = getattr(self, "_stratum_session_service", None)
        if service is not None:
            self._ensure_session_registry()
            return service
        self._ensure_p2mr_address_cache_state(create_service=False)
        validator = P2mrAddressValidator(
            rpc_call=lambda method, params: self.rpc.call(method, params),
            max_entries=lambda: int(
                getattr(
                    self,
                    "payout_address_cache_max_entries",
                    DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES,
                )
            ),
            ttl_seconds=lambda: float(
                getattr(
                    self,
                    "payout_address_cache_ttl_seconds",
                    DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS,
                )
            ),
            cache_lock=self._p2mr_address_cache_lock,
            cache=self._p2mr_address_cache,
            inflight=self._p2mr_address_validation_inflight,
        )
        candidate = StratumSessionService(
            registry=self._ensure_session_registry(),
            runtime=_CoordinatorSessionRuntime(self),
            jobs=_CoordinatorSessionJobs(self),
            progress=_CoordinatorSessionProgress(self),
            address_validator=validator,
            pool_closed_reason=PRISM_REJECTION_POOL_CLOSED,
        )
        return self.__dict__.setdefault("_stratum_session_service", candidate)

    @contextmanager
    def _watchdog_paused(self, *names: str) -> Iterator[None]:
        for name in names:
            self._pause_watchdog_heartbeat(name)
        try:
            yield
        finally:
            for name in reversed(names):
                self._resume_watchdog_heartbeat(name)

    def watchdog_loop(self) -> None:
        self._ensure_watchdog_service().run()

    def publication_progress_failure_expired(self, now: float) -> bool:
        """Bound detected-tip divergence independently of delivery health."""
        return self._ensure_tip_refresh_service().publication_failure_expired(
            now,
            budget_seconds=float(
                getattr(
                    self,
                    "template_refresh_failure_exit_seconds",
                    DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
                )
            ),
        )

    def _ensure_shutdown_controller(self) -> CoordinatorShutdownController:
        controller = getattr(self, "_shutdown_controller", None)
        if controller is not None:
            return controller
        candidate = CoordinatorShutdownController(
            float(
                getattr(
                    self,
                    "writer_quiescence_timeout_seconds",
                    DEFAULT_PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS,
                )
            )
        )
        # CPython's setdefault is atomic under the GIL. This lazy path exists
        # for focused tests that construct a coordinator with __new__; normal
        # instances create the controller in __init__ before threads start.
        return self.__dict__.setdefault("_shutdown_controller", candidate)

    @contextmanager
    def _writer_operation(self, component: str) -> Iterator[None]:
        controller = self._ensure_shutdown_controller()
        token = controller.enter_writer(component)
        try:
            yield
        finally:
            controller.exit_writer(token)

    def request_shutdown(self, signum: int | None = None) -> None:
        """Signal-safe-sized shutdown request; the ordered work runs elsewhere."""
        self._ensure_shutdown_controller().request_shutdown(signum)
        self.stop_event.set()

    @staticmethod
    def _shutdown_log(event: str, **fields: object) -> None:
        print(
            "prism coordinator: "
            + json.dumps({"event": event, **fields}, sort_keys=True),
            flush=True,
        )

    def _cancel_active_tip_refresh_for_shutdown(self) -> None:
        self._ensure_tip_refresh_service().cancel_active()

    def shutdown(self, *, reason: str = "graceful") -> bool:
        """Quiesce every ledger writer and release its lease exactly once.

        Returns true when release completed safely (including a ledger without
        lease support or an already-absent exact session lease). A timeout
        deliberately withholds release while a tracked writer may still run.
        """
        controller = self._ensure_shutdown_controller()
        if not controller.begin_shutdown(reason):
            return controller.wait_for_lease_handling()

        self.stop_event.set()
        self._cancel_active_tip_refresh_for_shutdown()
        self._shutdown_log(
            "shutdown_start",
            reason=reason,
            signal=controller.signal_number,
            writer_quiescence_timeout_seconds=controller.writer_quiescence_timeout_seconds,
        )

        quiesced, elapsed, blockers = controller.wait_for_writer_quiescence()
        self._shutdown_log(
            "writer_quiescence",
            duration_seconds=round(elapsed, 6),
            outcome="success" if quiesced else "timeout",
            blockers=blockers,
        )
        if not quiesced:
            for component, active_count in blockers.items():
                self._shutdown_log(
                    "lease_release_withheld",
                    component=component,
                    active_operations=active_count,
                    reason="writer_quiescence_timeout",
                )
            return False
        return self.release_ledger_lease()

    def release_ledger_lease(self) -> bool:
        """Release a quiesced writer lease at most once.

        The exact-session database fence makes an already-absent lease safe.
        Exceptions remain best-effort: they are observable, never retried from
        a duplicate finally block, and leave TTL fencing intact.
        """
        controller = self._ensure_shutdown_controller()
        claimed, blockers = controller.claim_lease_release()
        if not claimed:
            if blockers:
                self._shutdown_log(
                    "lease_release_withheld",
                    reason="active_writer_operations",
                    blockers=blockers,
                )
            return controller.lease_release_succeeded

        release = getattr(self.ledger, "release_writer_lease", None)
        self._shutdown_log(
            "lease_release_attempt",
            supported=release is not None,
        )
        if release is None:
            controller.finish_lease_release("unsupported", 0.0)
            self._shutdown_log(
                "lease_release",
                duration_seconds=0.0,
                outcome="unsupported",
                released=False,
            )
            return True
        started = time.monotonic()
        try:
            released = release()
        except Exception:
            elapsed = max(0.0, time.monotonic() - started)
            controller.finish_lease_release("failure", elapsed)
            self._shutdown_log(
                "lease_release",
                duration_seconds=round(elapsed, 6),
                outcome="failure",
                released=False,
            )
            traceback.print_exc()
            return False
        elapsed = max(0.0, time.monotonic() - started)
        outcome = "success" if released else "not_held"
        controller.finish_lease_release(outcome, elapsed)
        snapshot = controller.snapshot()
        self._shutdown_log(
            "lease_release",
            duration_seconds=round(elapsed, 6),
            outcome=outcome,
            released=bool(released),
            sigterm_to_release_seconds=(
                round(float(snapshot["sigterm_to_lease_release_seconds"]), 6)
                if snapshot["sigterm_release_observed"]
                else None
            ),
        )
        return True

    def drain_non_writer_components(
        self,
        threads: list[tuple[threading.Thread, float]] | None = None,
    ) -> None:
        """Drain threads, fanout sends, and executors only after lease handling."""
        controller = self._ensure_shutdown_controller()
        if not controller.claim_non_writer_drain():
            return
        started = time.monotonic()
        drain_threads: Sequence[tuple[threading.Thread, float]]
        if threads is None:
            drain_threads = self._ensure_background_services().threads_to_drain()
        else:
            # Temporary compatibility for focused shutdown callers. Process
            # startup itself is fully registry-owned.
            drain_threads = threads
        audit_http = self.__dict__.get("_audit_http_facade")
        if audit_http is not None and not audit_http.stop():
            self._shutdown_log(
                "audit_http_stop",
                outcome="timeout",
            )
        for thread, timeout in drain_threads:
            thread.join(timeout=timeout)
        self.shutdown_vardiff_idle_executor()
        self.shutdown_tip_refresh_executor()
        elapsed = max(0.0, time.monotonic() - started)
        controller.finish_non_writer_drain(elapsed)
        self._shutdown_log(
            "non_writer_drain",
            duration_seconds=round(elapsed, 6),
            lease_release_succeeded=controller.lease_release_succeeded,
            outcome="complete",
        )

    def open_stratum_listeners(
        self, listener_stack: ExitStack
    ) -> list[tuple[socket.socket, StratumListenerProfile]] | None:
        return StratumSessionService.open_stratum_listeners(
            listener_stack,
            self.listener_profiles,
            backlog=int(
                getattr(
                    self,
                    "stratum_listen_backlog",
                    DEFAULT_PRISM_STRATUM_LISTEN_BACKLOG,
                )
            ),
            retry_seconds=float(
                getattr(
                    self,
                    "stratum_bind_retry_seconds",
                    DEFAULT_PRISM_STRATUM_BIND_RETRY_SECONDS,
                )
            ),
            stop_event=getattr(self, "stop_event", None),
            socket_factory=socket.socket,
        )

    def serve(self) -> None:
        with ExitStack() as listener_stack:
            self._serve_with_listener_stack(listener_stack)

    def _serve_with_listener_stack(self, listener_stack: ExitStack) -> None:
        # Listeners come up first: connections complete their TCP handshake in
        # the kernel backlog while the rest of startup runs, so a fast restart
        # never bounces miners with connection refused. accept() still starts
        # only after block-work recovery below.
        listeners = self.open_stratum_listeners(listener_stack)
        if listeners is None:
            return
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                self.rpc.call("getblockcount")
                break
            except Exception:
                # A shutdown signal during the readiness wait must release the
                # bound ports promptly, or a successor's bind retry window can
                # expire against this process.
                if self.stop_event.wait(1):
                    return
        if self.stop_event.is_set():
            return
        self.validate_live_chain_identity()
        self.validate_live_template_and_fee_policy()
        self.prism_payout_policy()
        print(
            f"prism coordinator: listening on {self.bind}:{self.port} "
            f"share_diff={self.share_difficulty} ready_miners={self.min_ready_miners} "
            f"vardiff={'on' if self.vardiff_config.enabled else 'off'} "
            f"max_blocks={self.max_blocks} "
            f"blockpoll={self.blockpoll_seconds:g}s "
            f"version_mask={stratum_codec.format_mask_hex(self.version_mask)} "
            f"version_mask_source={self.version_mask_selection.source}:{self.version_mask_selection.detail} "
            f"ledger={self.ledger.backend_name} "
            f"ledger_execution={getattr(self.ledger, 'execution_backend', self.ledger.backend_name)} "
            f"hot_path_log={'on' if self.hot_path_log_enabled else 'off'}",
            flush=True,
        )
        for profile in self.listener_profiles[1:]:
            print(
                f"prism coordinator: {profile.name} listener on {profile.bind}:{profile.port} "
                f"start_diff={profile.vardiff_config.startup_difficulty} "
                f"min_diff={profile.vardiff_config.min_difficulty} "
                f"max_diff={profile.vardiff_config.max_difficulty} "
                f"share_diff={profile.share_difficulty}",
                flush=True,
            )
        if self.audit_bind and self.audit_port:
            self.start_audit_server()
        # Recover block work before accepting Stratum connections.  New miners
        # can only add wakeups after every previously committed candidate has
        # had a chance to re-enter the submit queue.  The listener sockets are
        # already bound above, so reconnecting miners wait in the accept
        # backlog through this recovery instead of being refused.
        if not self._run_startup_writer_replay(self.replay_pending_block_candidates):
            return
        if self.stop_event.is_set():
            return
        prepared = self.prewarm_startup_jobs()
        print(
            "prism coordinator: startup job preparation "
            f"status={'complete' if prepared is not None else 'deferred'} "
            f"mode={'ready' if prepared is not None else 'collection'} "
            f"tip={self.tip_template_snapshot.bestblockhash if self.tip_template_snapshot else 'unknown'}",
            flush=True,
        )
        # Seed listener liveness before accepting so the watchdog never fires
        # during startup. Background-loop heartbeats derive from their service
        # specifications at each named start below.
        for _, profile in listeners:
            self._record_heartbeat(profile.heartbeat_name)
        self._start_background_service("qbit_blockpoll")
        if self.blockwait_enabled:
            self._start_background_service("qbit_blockwait")
        if self.vardiff_idle_sweep_seconds > 0:
            self._start_background_service("vardiff_idle_sweep")
        if self.stratum_initial_job_timeout_seconds > 0:
            self._start_background_service("initial_job_timeout_sweep")
        share_writer = self._ensure_share_writer_service()
        share_writer.begin_startup_recovery()
        self._start_background_service("block_submitter")
        # Replay any shares stranded on disk by a prior ledger-outage
        # shutdown before serving, so no acked share is lost across restart.
        try:
            replay_ready = self._run_startup_writer_replay(
                self.replay_recovered_shares,
                drain_background_services=True,
                before_shutdown=share_writer.cancel_startup_recovery,
            )
        finally:
            share_writer.finish_startup_recovery()
        if not replay_ready:
            return
        self.share_writer_active = True
        self._start_background_service("share_writer")
        if self.ctv_broadcaster_enabled:
            self._start_background_service("ctv_fanout_broadcaster")
            print(self._ensure_ctv_runtime().startup_summary(), flush=True)
        if self.watchdog_enabled:
            self._start_background_service("watchdog")
            print(
                "prism coordinator: liveness watchdog enabled "
                f"timeout={self.watchdog_timeout_seconds:g}s "
                f"interval={self.watchdog_interval_seconds:g}s",
                flush=True,
            )
        for extra_server, extra_profile in listeners[1:]:
            self._start_secondary_accept_service(extra_server, extra_profile)
        try:
            self.accept_loop(*listeners[0])
        finally:
            # Free the listen ports the moment accepting stops so a successor
            # process can bind while the shutdown drain below runs.
            for server, _ in listeners:
                try:
                    server.close()
                except OSError:
                    pass
            # The writer barrier and lease release intentionally precede
            # joins and the tip-refresh executor drain: those may be stuck
            # in unrelated client delivery or obsolete fanout work.
            self.shutdown(reason="serve_exit")
            self.drain_non_writer_components()

    def _run_startup_writer_replay(
        self,
        replay: Callable[[], int],
        *,
        drain_threads: list[tuple[threading.Thread, float]] | None = None,
        drain_background_services: bool = False,
        before_shutdown: Callable[[], None] | None = None,
    ) -> bool:
        """Run startup ledger replay, stopping cleanly if shutdown wins."""
        try:
            replay()
        except ShutdownInProgress:
            if before_shutdown is not None:
                before_shutdown()
            if drain_threads is not None or drain_background_services:
                self.shutdown(reason="serve_startup_exit")
                if drain_background_services:
                    self.drain_non_writer_components()
                else:
                    self.drain_non_writer_components(drain_threads)
            return False
        return True

    def accept_loop(self, server: socket.socket, profile: StratumListenerProfile) -> None:
        self._ensure_stratum_session_service().accept_loop(server, profile)

    def _record_stratum_resource_exhaustion(
        self,
        *,
        listener_name: str,
        location: str,
        error_number: int | None,
    ) -> int:
        _CoordinatorSessionRuntime(self).record_resource_exhaustion(
            listener_name=listener_name,
            location=location,
            error_number=error_number,
        )
        return self.accept_resource_exhaustion_count

    def _wait_after_stratum_resource_failure(self, heartbeat_name: str) -> None:
        _CoordinatorSessionRuntime(self).wait_after_resource_failure(heartbeat_name)

    def _ensure_connection_capacity_state(self) -> None:
        self._ensure_session_registry()

    def _note_connection_limit_rejection_locked(self, scope: str) -> int:
        return self._ensure_session_registry()._note_rejection_locked(scope)

    def reserve_client_username(self, client: ClientState, worker: WorkerIdentity) -> bool:
        return self._ensure_stratum_session_service().reserve_client_username(
            client, worker
        )

    def start_audit_server(self) -> None:
        self.start_health_snapshot_refresher()
        self.start_metrics_snapshot_refresher()
        self._ensure_audit_http_facade().start()
        print(
            f"prism coordinator: audit HTTP listening on {self.audit_bind}:{self.audit_port}",
            flush=True,
        )

    def apply_stratum_send_timeout(self, sock: socket.socket) -> None:
        apply_socket_send_timeout(
            sock,
            float(
                getattr(
                    self,
                    "stratum_send_timeout_seconds",
                    DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS,
                )
            ),
        )

    def _wait_for_blockpoll_trigger(self) -> bool:
        return self._ensure_tip_refresh_service().wait_for_blockpoll_trigger()

    def blockpoll_loop(self) -> None:
        self._ensure_tip_refresh_service().blockpoll_loop()

    def template_refresh_failure_expired(self, now: float) -> bool:
        return self._ensure_tip_refresh_service().template_refresh_failure_expired(now)

    def _record_template_refresh_failure(self, now: float) -> None:
        self._ensure_tip_refresh_service().record_template_refresh_failure(now)

    def blockwait_once(self, known_tip: str) -> str:
        """One waitfornewblock round: returns the tip after the wait.

        qbitd returns as soon as its tip differs from ``known_tip`` (or after
        the server-side timeout, echoing the current tip), so a tip observed
        between our last poll and this call is reported immediately rather
        than being missed for a cycle.
        """
        return self._ensure_tip_refresh_service().blockwait_once(known_tip)

    def blockwait_loop(self) -> None:
        self._ensure_tip_refresh_service().blockwait_loop()

    @staticmethod
    def _blockwait_unsupported(exc: Exception) -> bool:
        return TipRefreshService.blockwait_unsupported(exc)

    def make_ctv_fanout_broadcast_daemon(self) -> CtvFanoutBroadcastDaemon:
        return self._ensure_ctv_runtime().make_daemon()

    def run_ctv_fanout_broadcaster_once(
        self,
        *,
        progress_callback: Callable[[], None] | None = None,
    ) -> CtvFanoutDaemonResult:
        return self._ensure_ctv_runtime().run_once(
            progress_callback=progress_callback,
            chunk_callback=self.observe_ctv_fanout_broadcaster_chunk,
        )

    def ctv_fanout_broadcaster_loop(self) -> None:
        self._ensure_ctv_runtime().loop(
            run_once=self.run_ctv_fanout_broadcaster_once,
            progress_callback=self._record_ctv_fanout_broadcaster_progress,
            observe_pass=self.observe_ctv_fanout_broadcaster_pass,
            record_yield=self._record_ctv_fanout_broadcaster_yield,
        )

    def _tip_refresh_artifacts(
        self,
        snapshot: QbitTipTemplateSnapshot,
    ) -> CachedTemplateArtifacts:
        return self._ensure_tip_refresh_service().artifacts(snapshot)

    def prepare_tip_refresh_bundle(
        self,
        snapshot: QbitTipTemplateSnapshot,
    ) -> CachedJobBundle:
        return self._ensure_tip_refresh_service().prepare_bundle(snapshot)

    def prewarm_current_tip_ready_bundle(self) -> CachedJobBundle | None:
        return self._ensure_tip_refresh_service().prewarm_current_tip_ready_bundle()

    def prewarm_startup_jobs(self) -> CachedJobBundle | None:
        return self._ensure_tip_refresh_service().prewarm_startup_jobs()

    def _tip_refresh_token_current_locked(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return self._ensure_tip_refresh_service().token_current(token, bundle, snapshot)

    def _tip_refresh_token_prepublication_current_locked(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return self._ensure_tip_refresh_service().token_prepublication_current(
            token,
            bundle,
            snapshot,
        )

    def _tip_refresh_snapshot_current_locked(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> bool:
        return self._ensure_tip_refresh_service().snapshot_current(
            snapshot,
            observation_sequence,
        )

    def _validate_prepared_tip_refresh(
        self,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> TipRefreshValidationToken:
        return self._ensure_tip_refresh_service().validate_prepared(
            bundle,
            snapshot,
            observation_sequence,
        )

    def _activate_tip_refresh(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: _FanoutCancellation,
    ) -> None:
        self._ensure_tip_refresh_service().activate(
            token,
            bundle,
            snapshot,
            cancel_event,
        )

    def _publish_prepared_tip_refresh(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        *,
        parent_hash: str | None,
    ) -> _FanoutCancellation:
        return self._ensure_tip_refresh_service().publish_prepared(
            token,
            bundle,
            snapshot,
            parent_hash=parent_hash,
        )

    def _clear_active_tip_refresh(
        self,
        token: TipRefreshValidationToken,
        cancel_event: _FanoutCancellation,
    ) -> None:
        self._ensure_tip_refresh_service().clear_active(token, cancel_event)

    def _prepared_tip_refresh_obsolete(
        self,
        validation_token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: _FanoutCancellation | None,
    ) -> bool:
        return self._ensure_tip_refresh_service().prepared_obsolete(
            validation_token,
            bundle,
            snapshot,
            cancel_event,
        )

    def send_prepared_job(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        validation_token: TipRefreshValidationToken,
        expected_connection_id: int,
        expected_active_job: PrismJobContext | None,
        cancel_event: _FanoutCancellation | None = None,
        submitted_monotonic: float | None = None,
    ) -> RefreshResult:
        return self._ensure_job_delivery_service().send_prepared_job(
            client,
            bundle,
            snapshot,
            validation_token,
            expected_connection_id,
            expected_active_job,
            cancel_event,
            submitted_monotonic,
        )

    def _fanout_prepared_tip_refresh(
        self,
        clients: list[ClientState],
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        *,
        observation_sequence: int | None = None,
        validation_token: TipRefreshValidationToken | None = None,
        preactivated_cancel_event: _FanoutCancellation | None = None,
        executor: ThreadPoolExecutor | None = None,
        expected_active_jobs: dict[ClientState, PrismJobContext | None] | None = None,
        heartbeat_name: str,
    ) -> tuple[int, float | None, float | None, int]:
        return self._ensure_tip_refresh_service().fanout_prepared(
            list(clients),
            bundle,
            snapshot,
            observation_sequence=observation_sequence,
            validation_token=validation_token,
            preactivated_cancel_event=preactivated_cancel_event,
            executor=executor,
            expected_active_jobs=expected_active_jobs,
            heartbeat_name=heartbeat_name,
        )

    def poll_qbit_tip_template_once(
        self,
        *,
        heartbeat_name: str = "qbit_blockpoll",
    ) -> int:
        return self._ensure_tip_refresh_service().poll_once(
            heartbeat_name=heartbeat_name
        )

    def _probe_tip_while_refresh_waiting(self) -> None:
        self._ensure_tip_refresh_service()._probe_tip_while_waiting()

    def _detected_tip_supersedes_locked(
        self,
        tip_hash: str,
        observation_sequence: int,
    ) -> bool:
        latest = self._ensure_tip_refresh_service().snapshot().latest_detected_tip
        return bool(latest and latest[0] != tip_hash and latest[1] > observation_sequence)

    def _raise_if_tip_refresh_superseded(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> None:
        self._ensure_tip_refresh_service()._raise_if_superseded(
            snapshot,
            observation_sequence,
        )

    def _reserve_tip_observation_sequence(self) -> int:
        return self._ensure_tip_refresh_service().reserve_observation_sequence()

    def observe_tip_for_refresh(
        self,
        tip_hash: str,
        *,
        observation_sequence: int | None = None,
        mark_pending: bool = True,
    ) -> bool:
        return self._ensure_tip_refresh_service().observe_tip(
            tip_hash,
            observation_sequence=observation_sequence,
            mark_pending=mark_pending,
        )

    def _submit_tip_observation_for_refresh(self, tip_hash: str) -> bool:
        return self._ensure_tip_refresh_service().submit_tip_observation(
            tip_hash,
            reason="blockpoll",
        )

    def observe_tip_first_seen(
        self,
        tip_hash: str,
        *,
        observation_sequence: int | None = None,
        publish_refresh_observation: bool = False,
        published_snapshot: QbitTipTemplateSnapshot | None = None,
    ) -> bool:
        return self._ensure_tip_refresh_service().publish_tip(
            tip_hash,
            observation_sequence=observation_sequence,
            publish_refresh_observation=publish_refresh_observation,
            published_snapshot=published_snapshot,
        )

    def _fetch_tip_parent_hash(self, tip_hash: str) -> str | None:
        return self._ensure_tip_refresh_service()._fetch_parent_hash(tip_hash)

    def current_tip_parent_hash(self, tip_hash: str) -> str | None:
        return self._ensure_tip_refresh_service().current_tip_parent_hash(tip_hash)

    def submit_stale_check_tip(self) -> str:
        """Best-known chain tip for per-share submit classification.

        Prefers the tip for which the refresh path already published coherent
        work (reconfirmed at least every PRISM_BLOCKPOLL_SECONDS while healthy)
        so mining.submit never blocks on a getbestblockhash RPC per share. This
        also removes the submit-races-ahead-of-the-refresh failure mode: a
        submit-path RPC can observe a new tip seconds before jobs refresh, and
        with PRISM_STRATUM_STALE_GRACE_SECONDS=0 (mainnet-forced) that
        rejected every in-flight share on the old tip. Classifying against the
        published tip keeps shares valid until the coordinator has prepared,
        validated, and published the flip, and it is the same tip source the
        stale-grace window and evicted-job classification are anchored to.

        During a detected-but-unpublished replacement, the published tip stays
        authoritative beyond the ordinary freshness age so a large healthy
        build cannot recreate the reject outage. That extension is bounded by
        PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS and is anchored to the
        first unpublished divergence; failed refreshes therefore still fall
        back to the live RPC instead of accepting frozen work indefinitely.
        """
        return self._ensure_tip_refresh_service().submit_authority()

    def _submit_stale_check_tip_locked(self, now: float) -> str | None:
        """Return the authoritative published submit tip while holding self.lock."""
        if not self._published_tip_authoritative_locked(now):
            return None
        observed = getattr(self, "current_tip_first_seen", None)
        assert observed is not None
        return str(observed[0])

    def _submit_control_snapshot(
        self,
        client: ClientState,
        job_id: str,
    ) -> tuple[bool, PrismJobContext | None, str | None]:
        """Snapshot normal-submit control state in one bounded lock hold.

        Pool closure, active-job membership, and published-tip authority must
        each be point-in-time consistent with their control-plane writers, but
        they do not need three separate admissions through the same lock. The
        caller performs live-tip RPC fallback, stale-grace classification,
        hashing, persistence, and accounting only after this lock is released.
        """
        with self.lock:
            pool_closed = self.accepted_block_count >= self.max_blocks
            context = self.jobs.get(job_id)
            if context is not None and job_id not in client.active_job_ids:
                context = None
            published_tip = self._submit_stale_check_tip_locked(time.monotonic())
        return pool_closed, context, published_tip

    def _published_tip_authoritative_locked(self, now: float) -> bool:
        """True while the published tip still owns share classification.

        Either the ordinary freshness window (reconfirmed within
        PRISM_SUBMIT_TIP_MAX_AGE_SECONDS) or the bounded detected-but-
        unpublished replacement lease is open. Job issuance uses the same
        predicate so work handed to miners is never classified against a
        different tip than the one it was issued for.
        """
        return self._ensure_tip_refresh_service().published_tip_authoritative(now)

    def stale_grace_deadline_open(
        self,
        client: ClientState,
        current_tip: str,
        now: float | None = None,
    ) -> bool:
        grace_seconds = float(getattr(self, "stale_grace_seconds", DEFAULT_PRISM_STALE_GRACE_SECONDS))
        if grace_seconds <= 0:
            return False
        now = time.monotonic() if now is None else now
        with self.lock:
            first_seen = getattr(self, "current_tip_first_seen", None)
            delivered = client.tip_work_delivered
        # Only successful refresh publication anchors current_tip_first_seen.
        # If this tip is merely detected, the window is not open: self-healing
        # from a lagging submit's RPC read would extend grace arbitrarily past
        # the real publication boundary. Fall through to stale-job instead.
        if first_seen is None or first_seen[0] != current_tip:
            return False
        # A None stamp is the startup baseline (see observe_tip_first_seen): the
        # tip did not just flip, so there is no in-flight prior-tip work to
        # rescue and the window stays closed.
        if first_seen[1] is None:
            return False
        if delivered is not None and delivered[0] == current_tip:
            # This connection already received current-tip work: its window runs
            # from that delivery, so a slow refresh pass cannot strand shares
            # that were in flight when replacement work finally arrived.
            return now - delivered[1] <= grace_seconds
        # The refresh path saw the flip but has not delivered current-tip work
        # to this connection yet (slow pass, aborted reorg reconcile, transient
        # build failure). Its prior-tip shares are still in flight; keep the
        # window open. Bounded by the exactly-one-tip-back parent rule at the
        # next flip, by delivery (which starts the grace clock above), and by
        # disconnect when sends to the client fail.
        return True

    def context_eligible_for_stale_grace(
        self,
        client: ClientState,
        context: PrismJobContext,
        current_tip: str,
    ) -> bool:
        if not self.stale_grace_deadline_open(client, current_tip):
            return False
        parent_hash = self.current_tip_parent_hash(current_tip)
        return bool(parent_hash) and str(context.template["previousblockhash"]) == parent_hash

    def note_tip_work_delivered(self, client: ClientState, job_parent_hash: str) -> None:
        self._ensure_job_delivery_service().note_tip_work_delivered(
            client, job_parent_hash
        )

    def _note_delivery_health_updated_locked(self, job_parent_hash: str) -> None:
        self._ensure_initial_job_state()
        if job_parent_hash == self._current_published_tip_hash_locked():
            self._reset_delivery_failure_if_coverage_restored_locked()

    def _ensure_evicted_job_state(self) -> None:
        self._ensure_retained_job_index()

    def _current_published_tip_hash_locked(self) -> str | None:
        first_seen = getattr(self, "current_tip_first_seen", None)
        if first_seen is not None:
            return str(first_seen[0])
        snapshot = getattr(self, "tip_template_snapshot", None)
        if snapshot is not None:
            return str(snapshot.bestblockhash)
        return None

    def _evicted_job_class_locked(self, entry: EvictedJobEntry) -> str:
        return self._ensure_job_delivery_service().retained_job_class(entry)

    def bury_evicted_job(
        self,
        client: ClientState,
        job_id: str,
        *,
        now: float | None = None,
        prune: bool = True,
    ) -> None:
        self._ensure_job_delivery_service().bury_retained(
            client, job_id, now=now, prune=prune
        )

    def prune_evicted_job_graveyard(
        self,
        *,
        now: float | None = None,
        force: bool = True,
    ) -> None:
        self._ensure_job_delivery_service().prune_retained(now=now, force=force)

    def evicted_job_entry(
        self,
        client: ClientState,
        job_id: str,
    ) -> EvictedJobEntry | None:
        return self._ensure_job_delivery_service().retained_entry(client, job_id)

    def evicted_submit_context(
        self,
        client: ClientState,
        entry: EvictedJobEntry,
        current_tip: str,
    ) -> tuple[PrismJobContext, str | None] | None:
        context = entry.context
        if str(context.template["previousblockhash"]) == current_tip:
            return context, None
        if not self.context_eligible_for_stale_grace(client, context, current_tip):
            return None
        return context, PRISM_CREDIT_POLICY_STALE_GRACE

    def note_evicted_job_submit(self, credit_policy: str | None) -> None:
        self._ensure_job_delivery_service().note_retained_submit(credit_policy)

    def refresh_jobs_after_pending_accepted_block(
        self,
        client: ClientState,
        *,
        heartbeat_name: str = "qbit_blockpoll",
    ) -> int:
        return self._ensure_tip_refresh_service().refresh_after_pending_accepted_block(
            client,
            heartbeat_name=heartbeat_name,
        )

    def refresh_jobs_after_accepted_block(
        self, *, block_height: int, block_hash: str, heartbeat_name: str = "qbit_blockpoll"
    ) -> int:
        return self._ensure_tip_refresh_service().refresh_after_accepted_block(
            block_height=block_height,
            block_hash=block_hash,
            heartbeat_name=heartbeat_name,
        )

    def fetch_qbit_tip_template_snapshot(self) -> QbitTipTemplateSnapshot:
        return (
            self._ensure_job_bundle_service()
            .template_repository.fetch_coherent_snapshot()
        )

    def ensure_reorg_reconciled_for_current_tip(
        self,
        *,
        expected_tip_hash: str | None = None,
    ) -> bool:
        return self._ensure_reorg_reconciler_service().ensure_current(
            expected_tip_hash=expected_tip_hash
        )

    def ensure_reorg_reconciled_for_tip(self, tip_hash: str) -> bool:
        return self._ensure_reorg_reconciler_service().ensure_tip(tip_hash)

    def qbit_chain_view_untrusted(self) -> bool:
        return reorg_chain_view_untrusted(
            lambda *args, **kwargs: self.rpc.call(*args, **kwargs),
            str(getattr(self, "qbit_chain", "regtest")),
        )

    def validate_live_chain_identity(self) -> None:
        """Fail closed when a public-chain node is wrong, isolated, or behind."""
        configured = str(getattr(self, "qbit_chain", "regtest")).strip().lower()
        info = self.rpc.call("getblockchaininfo")
        if not isinstance(info, dict):
            raise RuntimeError("getblockchaininfo returned non-object")
        reported = str(info.get("chain", "")).strip().lower()
        aliases = {
            "main": {"main", "mainnet"},
            "mainnet": {"main", "mainnet"},
        }
        allowed = aliases.get(configured, {configured})
        if reported not in allowed:
            raise RuntimeError(
                f"configured qbit chain {configured!r} does not match RPC chain {reported!r}"
            )

        config = getattr(self, "config", None)
        expected_genesis = (
            config.rpc.expected_genesis_hash
            if config is not None
            else env_optional("QBIT_EXPECTED_GENESIS_HASH")
        )
        if configured in {"main", "mainnet"} and expected_genesis is None:
            raise RuntimeError("QBIT_EXPECTED_GENESIS_HASH is required on mainnet")
        if expected_genesis is not None:
            expected_genesis = validate_hex(
                expected_genesis,
                name="QBIT_EXPECTED_GENESIS_HASH",
                expected_bytes=32,
            )
            live_genesis = str(self.rpc.call("getblockhash", [0])).lower()
            if live_genesis != expected_genesis:
                raise RuntimeError(
                    "QBIT_EXPECTED_GENESIS_HASH does not match the connected qbit node"
                )

        if configured not in {"main", "mainnet", *TESTNET_QBIT_CHAINS}:
            return
        if info.get("initialblockdownload") is not False:
            raise RuntimeError("public-chain qbitd is still in initial block download")
        try:
            blocks = int(info["blocks"])
            headers = int(info["headers"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("public-chain qbitd did not report numeric blocks and headers") from exc
        if blocks < 0 or headers < 0:
            raise RuntimeError("public-chain qbitd reported negative blocks or headers")
        if blocks != headers:
            raise RuntimeError(f"public-chain qbitd is not caught up: blocks={blocks}, headers={headers}")
        network_info = self.rpc.call("getnetworkinfo")
        if not isinstance(network_info, dict):
            raise RuntimeError("getnetworkinfo returned non-object")
        try:
            connections = int(network_info["connections"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("public-chain qbitd did not report a numeric peer count") from exc
        minimum_peers = (
            env_positive_int(
                "PRISM_MIN_PEERS",
                1,
                environ=(
                    {}
                    if config.rpc.minimum_peers_raw is None
                    else {"PRISM_MIN_PEERS": config.rpc.minimum_peers_raw}
                ),
            )
            if config is not None
            else env_positive_int("PRISM_MIN_PEERS", 1)
        )
        if connections < minimum_peers:
            raise RuntimeError(
                f"public-chain qbitd has {connections} peers, requires at least {minimum_peers}"
            )

    @staticmethod
    def rpc_fee_rate_bits_per_1000_weight(value: object, *, field: str) -> int:
        try:
            fee_rate = Decimal(str(value))
        except Exception as exc:
            raise RuntimeError(f"{field} is not a decimal fee rate") from exc
        if not fee_rate.is_finite() or fee_rate <= 0:
            raise RuntimeError(f"{field} is not a positive fee rate")
        return int(
            (fee_rate * Decimal(100_000_000)).to_integral_value(rounding=ROUND_CEILING)
        )

    def validate_live_template_and_fee_policy(self) -> None:
        artifacts = self.current_template_artifacts()
        template = artifacts.template
        if not artifacts.previousblockhash:
            raise RuntimeError("getblocktemplate.previousblockhash was missing")
        try:
            template_time = int(template["curtime"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("getblocktemplate.curtime was missing or not numeric") from exc
        config = getattr(self, "config", None)
        max_age = (
            env_nonnegative_int(
                "PRISM_TEMPLATE_MAX_AGE_SECONDS",
                DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
                environ=(
                    {}
                    if config.jobs.template_max_age_raw is None
                    else {
                        "PRISM_TEMPLATE_MAX_AGE_SECONDS": config.jobs.template_max_age_raw
                    }
                ),
            )
            if config is not None
            else env_nonnegative_int(
                "PRISM_TEMPLATE_MAX_AGE_SECONDS",
                DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
            )
        )
        template_age = int(time.time()) - template_time
        if template_age > max_age:
            raise RuntimeError(
                f"qbit block template is stale: age={template_age}s exceeds {max_age}s"
            )
        self._ensure_tip_refresh_service().record_successful_refresh(time.monotonic())

        settlement = self.prism_ctv_settlement_config(
            block_height=int(template["height"]) if "height" in template else None,
            parent_hash=artifacts.previousblockhash,
        )
        if settlement is None:
            return
        policy = settlement["fanout_fee_rate_policy"]
        assert isinstance(policy, dict)
        configured_rate = int(policy["market_fee_rate_sats_per_1000_weight"])
        mempool_info = self.rpc.call("getmempoolinfo")
        if not isinstance(mempool_info, dict):
            raise RuntimeError("getmempoolinfo returned non-object")
        relay_floors = [
            self.rpc_fee_rate_bits_per_1000_weight(mempool_info[name], field=name)
            for name in ("minrelaytxfee", "mempoolminfee")
            if mempool_info.get(name) is not None
        ]
        if not relay_floors:
            raise RuntimeError("getmempoolinfo did not report a relay fee floor")
        required_rate = max(relay_floors)
        if configured_rate < required_rate:
            raise RuntimeError(
                "PRISM CTV fanout fee rate is below the connected node relay floor: "
                f"configured={configured_rate} required={required_rate} bits/1000 weight"
            )

    @ledger_writer_operation("payout_reconciliation")
    def reconcile_prism_pool_blocks_once(
        self,
        *,
        tip_hash: str | None = None,
        _force_publish: bool = False,
        _source_reserved: bool = False,
    ) -> dict[str, object]:
        """Serialize reconciliation against accepted-block finalization."""
        with self._payout_balance_mutation():
            return self._ensure_reorg_reconciler_service().reconcile(
                tip_hash=tip_hash,
                force_publish=_force_publish,
                source_reserved=_source_reserved,
            )

    def client_can_receive_jobs(self, client: ClientState) -> bool:
        return self._ensure_job_delivery_service().client_can_receive_jobs(client)

    def pool_readiness_latched(self) -> bool:
        return self._ensure_job_bundle_service().pool_readiness_latched()

    def client_needs_tip_template_refresh(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return self._ensure_job_delivery_service().client_needs_refresh(
            client,
            snapshot,
        )

    def intervening_job_supersedes_snapshot(
        self,
        active_job: PrismJobContext | None,
        expected_active_job: PrismJobContext | None,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return self._ensure_job_delivery_service().intervening_supersedes(
            active_job,
            expected_active_job,
            snapshot,
        )

    def client_tip_changed_for_snapshot(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return self._ensure_job_delivery_service().tip_changed(client, snapshot)

    def handle_client(self, client: ClientState) -> None:
        self._ensure_stratum_session_service().handle_client(client)

    def disconnect_client(self, client: ClientState) -> None:
        self._ensure_stratum_session_service().disconnect_client(client)

    def handle_request(self, client: ClientState, request: dict[str, object]) -> None:
        self._ensure_stratum_session_service().handle_request(client, request)

    def _handle_request(self, client: ClientState, request: dict[str, object]) -> None:
        self._ensure_stratum_session_service()._handle_request(client, request)

    def handle_suggest_difficulty(self, client: ClientState, request_id: object, params: list[object]) -> None:
        self._ensure_stratum_session_service().handle_suggest_difficulty(
            client, request_id, params
        )

    def handle_configure(self, client: ClientState, request_id: object, params: list[object]) -> None:
        self._ensure_stratum_session_service().handle_configure(
            client, request_id, params
        )

    def send_result(self, client: ClientState, request_id: object, result: object) -> None:
        client.send(stratum_result_payload(request_id, result))

    def send_error(self, client: ClientState, request_id: object, code: int, message: str, *, reason: str | None = None) -> None:
        client.send(stratum_error_payload(request_id, code, message, reason=reason))

    def resolve_worker(self, username: str) -> WorkerIdentity:
        return self._ensure_stratum_session_service().resolve_worker(username)

    def validate_p2mr_address(self, address: str, *, label: str) -> tuple[str, str]:
        return self._ensure_stratum_session_service().address_validator.validate(
            address, label=label
        )

    @staticmethod
    def _raise_shared_p2mr_address_validation_error(error: BaseException) -> None:
        if isinstance(error, StratumError):
            raise StratumError(
                error.code,
                error.message,
                reason=error.reason,
                disconnect=error.disconnect,
            ) from error
        raise RuntimeError(str(error)) from error

    def _ensure_p2mr_address_cache_state(self, *, create_service: bool = True) -> None:
        if not hasattr(self, "_p2mr_address_cache_lock"):
            self._p2mr_address_cache_lock = threading.Lock()
        if not hasattr(self, "_p2mr_address_cache"):
            self._p2mr_address_cache = OrderedDict()
        if not hasattr(self, "_p2mr_address_validation_inflight"):
            self._p2mr_address_validation_inflight = {}
        if create_service:
            self._ensure_stratum_session_service()

    def maybe_send_job(
        self,
        client: ClientState,
        *,
        clean_jobs: bool,
        raise_on_reorg_failure: bool = False,
        raise_on_build_failure: bool = False,
        tip_refresh_snapshot: QbitTipTemplateSnapshot | None = None,
        tip_refresh_observation_sequence: int | None = None,
    ) -> bool:
        self._adopt_legacy_delivery_client(client)
        return self._ensure_job_delivery_service().maybe_send_job(
            client,
            clean_jobs=clean_jobs,
            raise_on_reorg_failure=raise_on_reorg_failure,
            raise_on_build_failure=raise_on_build_failure,
            tip_refresh_snapshot=tip_refresh_snapshot,
            tip_refresh_observation_sequence=tip_refresh_observation_sequence,
        )

    def _maybe_send_job_locked(
        self,
        client: ClientState,
        *,
        clean_jobs: bool,
        raise_on_reorg_failure: bool = False,
        raise_on_build_failure: bool = False,
        tip_refresh_snapshot: QbitTipTemplateSnapshot | None = None,
        tip_refresh_observation_sequence: int | None = None,
        prepared_bundle: CachedJobBundle | None = None,
        idle_authority: IdleDeliveryAuthority | None = None,
        prepared_bundle_allow_uncached: bool = False,
    ) -> bool:
        return self._ensure_job_delivery_service().maybe_send_job_locked(
            client,
            clean_jobs=clean_jobs,
            raise_on_reorg_failure=raise_on_reorg_failure,
            raise_on_build_failure=raise_on_build_failure,
            tip_refresh_snapshot=tip_refresh_snapshot,
            tip_refresh_observation_sequence=tip_refresh_observation_sequence,
            prepared_bundle=prepared_bundle,
            idle_authority=idle_authority,
            prepared_bundle_allow_uncached=prepared_bundle_allow_uncached,
        )

    def prune_client_active_jobs(self, client: ClientState) -> None:
        self._ensure_job_delivery_service().prune_active(client)

    def send_difficulty(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        self.send_difficulty_value(client, job.share_difficulty)

    def send_difficulty_value(self, client: ClientState, difficulty: Decimal) -> None:
        client.send(self.difficulty_payload(difficulty))

    @staticmethod
    def difficulty_payload(difficulty: Decimal) -> dict[str, object]:
        return stratum_difficulty_payload(difficulty)

    def client_vardiff_config(self, client: ClientState) -> vardiff.VardiffConfig:
        return self._ensure_vardiff_service().client_config(client)

    def client_startup_difficulty(self, profile: StratumListenerProfile | None = None) -> Decimal:
        return self._ensure_vardiff_service().startup_difficulty(profile)

    def desired_client_share_difficulty(self, client: ClientState) -> Decimal:
        return self._ensure_vardiff_service().desired_difficulty(client)

    def client_minimum_advertised_difficulty(self, client: ClientState) -> Decimal:
        return self._ensure_vardiff_service().minimum_advertised_difficulty(client)

    def apply_job_difficulty(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        self._ensure_job_delivery_service().apply_job_difficulty(
            client,
            job,
            config=self.client_vardiff_config(client),
        )

    def apply_client_difficulty_requests(self, client: ClientState) -> Decimal | None:
        """Specialize the client's difficulty policy from its recorded requests
        (password ``d=``/``md=`` and ``mining.suggest_difficulty``), clamped to
        the pristine listener bounds. The listener floor always wins: on a
        high-diff listener no request can drop a client below the configured
        minimum. Explicit ``d=`` outranks a suggestion. Returns the resolved
        target difficulty, or None when the client requested nothing."""
        return self._ensure_job_delivery_service().apply_client_difficulty_requests(
            client,
            base=client.listener_vardiff_config or self.vardiff_config,
        )

    def advertise_client_difficulty(
        self,
        client: ClientState,
        target: Decimal,
    ) -> bool:
        return self._ensure_job_delivery_service().advertise_client_difficulty(
            client,
            target,
        )

    def _advertise_client_difficulty_locked(
        self,
        client: ClientState,
        target: Decimal,
    ) -> bool:
        return self._ensure_job_delivery_service().advertise_client_difficulty_locked(
            client,
            target,
        )

    def normalized_prior_balances(self, balances: list[dict[str, object]]) -> list[dict[str, object]]:
        return self._ensure_payout_state_service().normalized_prior_balances(balances)

    def prior_balances_match_current(self, prior_balances: list[dict[str, object]]) -> bool:
        return self._ensure_payout_state_service().prior_balances_match_current(prior_balances)

    def send_job(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        client.send(self.job_payload(job))

    @staticmethod
    def job_payload(job: direct_stratum.DirectQbitStratumJob) -> dict[str, object]:
        return stratum_job_payload(job)

    def send_job_update(
        self,
        client: ClientState,
        job: direct_stratum.DirectQbitStratumJob,
    ) -> None:
        self._ensure_job_delivery_service().send_update(
            client,
            job,
            split_send=(
                "send_difficulty" in self.__dict__ or "send_job" in self.__dict__
            ),
        )

    def build_job_for_client(
        self,
        client: ClientState,
        *,
        clean_jobs: bool,
    ) -> PrismJobContext:
        return self._ensure_job_delivery_service().build_job_for_client(
            client,
            clean_jobs=clean_jobs,
        )

    def build_job_for_client_from_artifacts(
        self,
        client: ClientState,
        artifacts: CachedTemplateArtifacts,
        *,
        clean_jobs: bool,
    ) -> PrismJobContext:
        return self._ensure_job_delivery_service().build_job_for_client_from_artifacts(
            client,
            artifacts,
            clean_jobs=clean_jobs,
        )

    def build_collection_bundle(
        self,
        *,
        template: dict[str, Any],
        transaction_hexes: tuple[str, ...],
        worker: WorkerIdentity,
        network_difficulty: int,
        issued_at_ms: int,
        suffix_hex: str,
        summary_only: bool = False,
        payout_policy: dict[str, object] | None = None,
        ctv_settlement: dict[str, object] | None = None,
        cancellation: _JobBuildCancellation | None = None,
    ) -> dict[str, Any]:
        return self._ensure_job_bundle_service().build_collection_bundle(
            template=template,
            transaction_hexes=transaction_hexes,
            worker=worker,
            network_difficulty=network_difficulty,
            issued_at_ms=issued_at_ms,
            suffix_hex=suffix_hex,
            summary_only=summary_only,
            payout_policy=payout_policy,
            ctv_settlement=ctv_settlement,
            cancellation=cancellation,
        )

    def build_audit_bundle(
        self,
        *,
        shares: list[dict[str, object]],
        found_block: dict[str, object],
        prior_balances: list[dict[str, object]],
        coinbase_script_sig_suffix_hex: str,
        witness_merkle_leaves_hex: list[str] | None = None,
        ctv_fee_parent_hash: str | None = None,
        canonical_output_path: Path | None = None,
        canonical_output_parent_fd: int | None = None,
        canonical_output_adopter: Callable[[Path, os.stat_result], None] | None = None,
        summary_only: bool = False,
        payout_policy: dict[str, object] | None = None,
        ctv_settlement: dict[str, object] | None = None,
        cancellation: _JobBuildCancellation | None = None,
    ) -> dict[str, Any]:
        self._ensure_tip_refresh_state()
        return self._ensure_bundle_compiler().build_audit_bundle(
            shares=shares,
            found_block=found_block,
            prior_balances=prior_balances,
            coinbase_script_sig_suffix_hex=coinbase_script_sig_suffix_hex,
            witness_merkle_leaves_hex=witness_merkle_leaves_hex,
            ctv_fee_parent_hash=ctv_fee_parent_hash,
            canonical_output_path=canonical_output_path,
            canonical_output_parent_fd=canonical_output_parent_fd,
            canonical_output_adopter=canonical_output_adopter,
            summary_only=summary_only,
            payout_policy=payout_policy,
            ctv_settlement=ctv_settlement,
            cancellation=cancellation,
        )

    def coinbase_script_sig_suffix_hex(self, extranonce1_hex: str, extranonce2_hex: str) -> str:
        extranonce1_hex = validate_hex(extranonce1_hex, name="extranonce1")
        extranonce2_hex = validate_hex(extranonce2_hex, name="extranonce2")
        return self.coinbase_tag_hex + extranonce1_hex + extranonce2_hex

    @ledger_writer_operation("share_submission")
    def handle_submit(self, client: ClientState, params: list[object]) -> bool:
        return self._ensure_share_submission_service().handle(client, params)

    def _submit_synchronous_credit_candidate(
        self,
        candidate: PrismBlockCandidate,
        *,
        share_key: tuple[str, str],
        worker_name: str,
        evicted_entry: EvictedJobEntry | None,
        credit_policy: str | None,
    ) -> bool:
        """Resolve one below-target block while its active S3 actor is held."""
        submission = candidate.submission
        try:
            block_landed = self.submit_block_candidate(candidate)
        except BaseException:
            self._retain_block_candidate_for_retry(candidate)
            self._release_submit_share_key(share_key)
            raise
        if not block_landed:
            outcome = getattr(self, "_block_candidate_outcome", None)
            reason = getattr(outcome, "reason", None) if outcome is not None else None
            retryable_reasons = {None, *PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS}
            if reason in retryable_reasons:
                # The durable outbox may still land and credit this block.
                # Close without a Stratum result instead of issuing a false
                # definitive rejection for an uncertain outcome.
                self._retain_block_candidate_for_retry(candidate)
                self._release_submit_share_key(share_key)
                raise RuntimeError("block candidate outcome is pending durable retry")
            # This process will never credit the candidate share only after the
            # durable outbox update becomes terminal. The actor remains held
            # across that update, so a concurrent same-hash actor cannot lose
            # its older acceptance floor when this stable holder is removed.
            finish = getattr(self.ledger, "mark_block_candidate_abandoned", None)
            if callable(finish):
                try:
                    finish(block_hash=submission.block_hash_hex, error=reason)
                except BaseException:
                    self._ensure_share_writer_service().adopt_pending_share(
                        candidate.pending_share
                    )
                    raise
            self._finish_pending_share_candidate(candidate.pending_share)
            # Once the durable outbox cannot replay this candidate, its landed
            # transition tombstone no longer protects a crash seam.
            self._clear_accepted_block_payout_preview(submission.block_hash_hex)
            self._release_submit_share_key(share_key)
            self.reject_stratum(
                23,
                PRISM_REJECTION_LOW_DIFFICULTY,
                "low difficulty share",
                worker=worker_name,
            )
        finish = getattr(self.ledger, "mark_block_candidate_submitted", None)
        if callable(finish):
            finish(block_hash=submission.block_hash_hex)
        self._finish_pending_share_candidate(candidate.pending_share)
        if evicted_entry is not None:
            self.note_evicted_job_submit(credit_policy)
        return False

    @staticmethod
    def block_candidate_intent(candidate: PrismBlockCandidate) -> dict[str, Any]:
        return encode_block_candidate_intent(candidate)

    def block_candidate_from_intent(
        self,
        intent: dict[str, Any] | None = None,
    ) -> PrismBlockCandidate:
        # This helper was historically a static method. Preserve class-level
        # decode calls while instance calls additionally adopt S3's durable
        # credit-candidate holder before the reconstructed value is published.
        coordinator: PrismCoordinator | None
        if intent is None:
            if not isinstance(self, dict):
                raise TypeError("block candidate intent must be an object")
            intent = self
            coordinator = None
        else:
            coordinator = self
        candidate = decode_block_candidate_intent(intent)
        if candidate.credit_share_on_accept and coordinator is not None:
            # A below-target candidate can credit this older accepted stamp
            # after durable replay. Adopt its stable logical floor before
            # startup prewarm/job issuance. Ordinary asynchronous candidates
            # already committed their share and need no floor.
            coordinator._ensure_block_candidate_service().adopt_replayed_candidate(
                candidate
            )
        return candidate

    def _ensure_pending_share_commit_state(self) -> None:
        self._ensure_share_writer_service()

    def _finish_pending_share_commit(self, pending_share: PendingShare) -> None:
        """Drop a share from the snapshot anchor floor.

        Called once the share's ledger row reached a terminal outcome in this
        process: durably committed, rejected back to the miner, recovered to
        the on-disk replay file, or its block candidate terminally abandoned.
        Idempotent. Credit-bearing candidate intents are adopted under their
        durable share ID during replay, so a reconstructed PendingShare can
        release the same logical lease; ordinary already-credited candidate
        replays remain unregistered no-ops.
        """
        self._ensure_share_writer_service().finish_pending_share(pending_share)

    def _finish_pending_share_attempt(self, pending_share: PendingShare) -> None:
        """Release only one stamped submission's process-local floor holder."""
        self._ensure_share_writer_service().finish_pending_attempt(pending_share)

    def _finish_pending_share_candidate(self, pending_share: PendingShare) -> None:
        """Release only a terminal durable credit-candidate floor holder."""
        self._ensure_share_writer_service().finish_pending_candidate(pending_share)

    def _job_snapshot_anchor_ms(self, issued_at_ms: int) -> int:
        """Clamp a share-snapshot anchor below every pending share commit.

        The reward-window contract lets an auditor replay
        qbit_audit_share_window(anchor) against the durable ledger and expect
        exactly the shares the published bundle counted. A share whose
        accepted_at_ms is already assigned but whose row has not committed yet
        (group-commit queue, in-flight batch, or a block-candidate credit
        linked after landing) would violate that: it is invisible to the MVCC
        snapshot now but joins later replays at any anchor at or above its
        accepted_at_ms. Anchoring strictly below every such share keeps the
        issued snapshot reproducible without making job builds wait behind the
        writer connection.
        """
        return self._ensure_share_writer_service().snapshot_anchor_ms(issued_at_ms)

    def pending_share_from_submission(
        self,
        *,
        context: PrismJobContext,
        submission: direct_stratum.DirectQbitSubmission,
        ntime_hex: str,
        credit_policy: str | None = None,
    ) -> PendingShare:
        share_difficulty = self.accepted_share_difficulty(context)
        return self._ensure_share_writer_service().make_pending_share(
            PendingShareInput(
                share_id=f"{context.worker.username}:{submission.block_hash_hex}",
                miner_id=context.worker.payout_address,
                order_key=context.worker.payout_address,
                p2mr_program_hex=context.worker.p2mr_program_hex,
                share_difficulty=share_difficulty,
                network_difficulty=max(
                    1,
                    int(context.found_block["network_difficulty"]),
                ),
                template_height=int(context.template["height"]) - 1,
                job_id=context.job.job_id,
                job_issued_at_ms=context.issued_at_ms,
                ntime=int(ntime_hex, 16),
                credit_policy=credit_policy,
            )
        )

    def append_accepted_share(
        self,
        client: ClientState,
        context: PrismJobContext,
        submission: direct_stratum.DirectQbitSubmission,
        pending_share: PendingShare,
        *,
        credit_policy: str | None = None,
        candidate_intent: dict[str, Any] | None = None,
    ) -> None:
        entry = PendingShareAppend(
            pending_share=pending_share,
            username=context.worker.username,
            job_id=context.job.job_id,
            block_hash_hex=submission.block_hash_hex,
            collection_only=bool(context.collection_only),
            credit_policy=credit_policy,
            candidate_intent=candidate_intent,
        )
        try:
            self._ensure_share_writer_service().append_and_wait(entry)
        except ShareWriterQueueFull as exc:
            raise StratumError(
                20,
                str(exc),
                reason=PRISM_REJECTION_INTERNAL_ERROR,
            ) from exc
        except Exception as exc:
            if isinstance(exc, ShareWriterError):
                raise StratumError(
                    20,
                    str(exc),
                    reason=PRISM_REJECTION_INTERNAL_ERROR,
                ) from exc
            raise
        # Only committed shares affect public accounting, vardiff, and the
        # response that handle_request sends immediately after this returns.
        self.note_worker_accepted_share(context.worker.username, credit_policy)
        self.note_vardiff_accepted_share(client, context.job)

    def enqueue_share_append(self, entry: PendingShareAppend, *, wait: bool = False) -> None:
        try:
            self._ensure_share_writer_service().enqueue(entry, wait=wait)
        except ShareWriterQueueFull as exc:
            raise StratumError(
                20,
                str(exc),
                reason=PRISM_REJECTION_INTERNAL_ERROR,
            ) from exc

    def share_append_loop(self) -> None:
        self._ensure_share_writer_service().run()

    def _append_share_batch(self, batch: list[PendingShareAppend]) -> bool:
        return self._ensure_share_writer_service().append_batch(batch)

    def _recover_share_to_disk(self, entry: PendingShareAppend, reason: str) -> None:
        self._ensure_share_writer_service().recover_to_disk(entry, reason)

    def replay_recovered_shares(self) -> int:
        return self._ensure_share_writer_service().replay_recovery_file()

    def _append_share_entry(self, entry: PendingShareAppend, *, retry_until_stopped: bool = False) -> bool:
        return self._ensure_share_writer_service().append_entry(
            entry,
            retry_until_stopped=retry_until_stopped,
        )

    def accepted_share_difficulty(self, context: PrismJobContext) -> int:
        override = self.share_weights_by_username.get(
            context.worker.username,
            self.share_weights_by_username.get(context.worker.payout_address),
        )
        if override is not None:
            return max(1, int(override))
        return scaled_target_difficulty(context.job.share_target)

    def note_vardiff_submitted_share(self, client: ClientState) -> None:
        self._ensure_share_hot_path_state()
        with self._share_accounting_lock:
            self.submitted_share_count += 1
        self._ensure_vardiff_service().note_submitted(client)

    def note_vardiff_accepted_share(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        self._ensure_vardiff_service().note_accepted(client, job.share_difficulty)

    def _ensure_vardiff_idle_state(self) -> None:
        self._ensure_vardiff_service()

    def _record_vardiff_idle_skip(self, reason: str) -> None:
        self._ensure_vardiff_service().record_idle_skip(reason)

    def _observe_vardiff_idle_seconds(self, name: str, elapsed_seconds: float) -> None:
        self._ensure_vardiff_service().observe_idle_seconds(name, elapsed_seconds)

    def _idle_bundle_cache_key(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        *,
        artifacts: CachedTemplateArtifacts | None = None,
    ) -> tuple[object, ...] | None:
        if artifacts is None:
            artifacts = self._idle_job_issuance_artifacts_locked()
        payout = self._ensure_payout_state_service().snapshot()
        if artifacts is None or payout.publication_blocked:
            return None
        payout_artifact = payout.published.artifact
        if (
            bundle.build_key is None
            or payout_artifact is None
            or bundle.build_key.payout_artifact_sha256
            != payout_artifact.prior_balances_sha256
        ):
            return None
        observed_tip = getattr(self, "current_tip_first_seen", None)
        if observed_tip is not None and observed_tip[0] != artifacts.previousblockhash:
            return None
        if (
            bundle.template_fingerprint != artifacts.fingerprint
            or bundle.payout_state_generation != payout.generation
            or str(bundle.template.get("previousblockhash", ""))
            != artifacts.previousblockhash
        ):
            return None
        published_tip = payout.published.source_tip_hash
        if published_tip is not None and published_tip != artifacts.previousblockhash:
            return None
        worker = client.worker
        if worker is None:
            return None
        mode = (
            "ready"
            if self._ensure_job_bundle_service().ready_latched()
            else "collection"
        )
        if bundle.collection_only != (mode == "collection"):
            return None
        if (
            bundle.template is not artifacts.template
            or bundle.template_generation != artifacts.generation
        ):
            return None
        return self._job_bundle_key(
            artifacts,
            mode=mode,
            payout_state_generation=payout.generation,
            payout_artifact_generation=bundle.payout_artifact_generation,
            worker=worker,
        )

    @contextmanager
    def _idle_bundle_admission(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        *,
        allow_uncached: bool = False,
    ) -> Iterator[AdmittedIdleBundleSource | None]:
        artifacts = self._idle_job_issuance_artifacts_locked()
        if artifacts is None:
            yield None
            return
        key = self._idle_bundle_cache_key(
            client,
            bundle,
            artifacts=artifacts,
        )
        if key is None:
            yield None
            return
        with self._ensure_job_bundle_service().cache_admission(
            key,
            bundle,
            allow_uncached=allow_uncached,
        ) as admitted:
            yield (
                AdmittedIdleBundleSource(
                    artifacts=artifacts,
                    bundle=bundle,
                    cache_identity=key,
                    allow_uncached=allow_uncached,
                )
                if admitted
                else None
            )

    def _admit_idle_bundle_source(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        *,
        allow_uncached: bool = False,
    ) -> AdmittedIdleBundleSource | None:
        """Consume J1 admission and return an immutable exact-source lease."""
        artifacts = self._idle_job_issuance_artifacts_locked()
        if artifacts is None:
            return None
        key = self._idle_bundle_cache_key(
            client,
            bundle,
            artifacts=artifacts,
        )
        if key is None:
            return None
        with self._ensure_job_bundle_service().cache_admission(
            key,
            bundle,
            allow_uncached=allow_uncached,
        ) as admitted:
            if not admitted:
                return None
        return AdmittedIdleBundleSource(
            artifacts=artifacts,
            bundle=bundle,
            cache_identity=key,
            allow_uncached=allow_uncached,
        )

    def _idle_bundle_current_locked(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        *,
        allow_uncached: bool = False,
    ) -> bool:
        """Compatibility predicate; S2 consumes an immutable J1 source lease."""
        with self._idle_bundle_admission(
            client,
            bundle,
            allow_uncached=allow_uncached,
        ) as admitted:
            return admitted is not None

    def _idle_job_issuance_artifacts_locked(
        self,
    ) -> CachedTemplateArtifacts | None:
        """Cache-only counterpart of ``job_issuance_template_artifacts``."""
        artifacts = (
            self._ensure_job_bundle_service()
            .template_repository.current_artifacts()
        )
        with self.lock:
            published = getattr(self, "current_tip_first_seen", None)
            latest_detected = getattr(self, "latest_detected_tip", None)
            published_snapshot = getattr(self, "tip_template_snapshot", None)
            pinned = bool(
                published is not None
                and published_snapshot is not None
                and published_snapshot.bestblockhash == published[0]
                and published_snapshot.template_artifacts is not None
                and self._published_tip_authoritative_locked(time.monotonic())
                and (
                    (
                        latest_detected is not None
                        and latest_detected[0] != published[0]
                    )
                    or (
                        artifacts is not None
                        and artifacts.previousblockhash != published[0]
                    )
                )
            )
        if pinned:
            assert published_snapshot is not None
            assert published_snapshot.template_artifacts is not None
            return published_snapshot.template_artifacts
        return artifacts

    def _cached_idle_job_bundle(self, client: ClientState) -> CachedJobBundle | None:
        """Return only an exact issuance bundle; never build or query."""
        artifacts = self._idle_job_issuance_artifacts_locked()
        worker = client.worker
        if artifacts is None or worker is None:
            return None
        service = self._ensure_job_bundle_service()
        mode = "ready" if service.ready_latched() else "collection"
        payout = self._ensure_payout_state_service().snapshot()
        payout_artifact = payout.ledger_artifact
        payout_artifact_generation = (
            payout_artifact.generation
            if mode == "ready"
            and payout_artifact is not None
            and payout_artifact.payout_state_generation == payout.generation
            and payout_artifact.network_difficulty == artifacts.network_difficulty
            else 0
        )
        key = self._job_bundle_key(
            artifacts,
            mode=mode,
            payout_state_generation=payout.generation,
            payout_artifact_generation=payout_artifact_generation,
            worker=worker,
        )
        bundle = service.cached_bundle_for_key(key)
        if bundle is None or not self._idle_bundle_current_locked(client, bundle):
            return None
        return bundle

    def _build_idle_job_bundle(
        self,
        request: _IdleRetargetRequest,
    ) -> CachedJobBundle:
        """Build on the dedicated idle executor without holding a client lock."""
        with self.lock:
            if self._vardiff_idle_tip_divergence_locked():
                raise JobBuildSuperseded(
                    "idle retarget deferred during unpublished tip refresh"
                )
        artifacts = (
            self._retained_collection_artifacts()
            or self.job_issuance_template_artifacts()
        )
        return self.shared_job_bundle(
            artifacts,
            request.worker,
            retry_superseded=False,
            idle_retarget=True,
        )

    def _vardiff_idle_tip_divergence_locked(self) -> bool:
        return self._ensure_vardiff_service().idle_tip_diverged_locked()

    def _idle_request_skip_reason(
        self,
        request: _IdleRetargetRequest,
    ) -> str | None:
        return self._ensure_vardiff_service().request_skip_reason(request)

    def shutdown_vardiff_idle_executor(self) -> None:
        self._ensure_vardiff_service().shutdown_idle_executor()

    def vardiff_idle_sweep_loop(self) -> None:
        self._ensure_vardiff_service().idle_sweep_loop()

    def vardiff_idle_sweep_once(self) -> int:
        return self._ensure_vardiff_service().idle_sweep_once()

    def retarget_client(
        self,
        client: ClientState,
        *,
        current_difficulty: Decimal,
        accepted_shares: int,
        submitted_shares: int,
        accepted_difficulty: Decimal,
        elapsed_seconds: Decimal,
        require_idle: bool = False,
        prepared_bundle: CachedJobBundle | None = None,
        expected_connection_id: int | None = None,
        expected_worker: WorkerIdentity | None = None,
        expected_active_job: PrismJobContext | None = None,
        expected_window_started: float | None = None,
    ) -> bool:
        return self._ensure_vardiff_service().retarget(
            client,
            current_difficulty=current_difficulty,
            accepted_shares=accepted_shares,
            submitted_shares=submitted_shares,
            accepted_difficulty=accepted_difficulty,
            elapsed_seconds=elapsed_seconds,
            require_idle=require_idle,
            prepared_bundle=prepared_bundle,
            expected_connection_id=expected_connection_id,
            expected_worker=expected_worker,
            expected_active_job=expected_active_job,
            expected_window_started=expected_window_started,
        )

    def enqueue_block_candidate(self, candidate: PrismBlockCandidate) -> bool:
        return self._ensure_block_candidate_service().enqueue(candidate)

    @ledger_writer_operation("accepted_block_handling")
    def replay_pending_block_candidates(self) -> int:
        """Replay durable candidate intents through the B1 owner."""
        return self._ensure_block_candidate_service().replay_pending()

    def _ensure_block_submitter_retry_state(self) -> None:
        self._ensure_block_candidate_service()

    def _wait_for_block_candidate_retry(self, delay_seconds: float) -> bool:
        """Wait for intentional backoff without impersonating stuck work.

        Only this bounded retry wait refreshes the submitter heartbeat. SQL,
        RPC, audit/finalization, and socket phases call no helper here, so a
        genuinely blocked candidate phase remains watchdog-eligible.
        """
        return self._ensure_block_candidate_service().wait_for_retry(delay_seconds)

    def _mark_block_candidate_attempted(self, block_hash: str) -> None:
        self._ensure_block_candidate_service().mark_attempted(block_hash)

    def block_submit_loop(self) -> None:
        self._ensure_block_candidate_service().run()

    def submit_next_block_candidate(self, timeout: float | None = None) -> bool:
        """Run one queued or retained candidate through the B1 owner."""
        return self._ensure_block_candidate_service().submit_next(timeout).ran

    def _submit_next_block_candidate_writer(
        self,
        candidate: PrismBlockCandidate,
    ) -> bool:
        """Run one candidate with an independent active credit-floor actor."""
        return self._ensure_block_candidate_service().submit_writer(candidate)
    def _retain_block_candidate_for_retry(
        self,
        candidate: PrismBlockCandidate,
    ) -> None:
        self._ensure_block_candidate_service().retain_for_retry(candidate)

    def _reject_terminal_prepared_block_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> None:
        """Reject durable prepared deltas before abandoning a stale candidate."""
        state_reader = getattr(self.ledger, "pool_block_state", None)
        if not callable(state_reader):
            return
        block_hash = str(candidate.submission.block_hash_hex).lower()
        state = state_reader(block_hash=block_hash)
        if state is None or str(state.get("chain_state", "")) != "prepared":
            return
        active_tip_height = int(self.rpc.call("getblockcount"))
        result = self.reject_prepared_block(
            block_hash=block_hash,
            active_tip_height=active_tip_height,
        )
        if int(result.get("rejected_count", 0)) == 1:
            return
        state = state_reader(block_hash=block_hash)
        if state is not None and str(state.get("chain_state", "")) == "prepared":
            raise RuntimeError(
                f"ledger did not reject prepared block candidate {block_hash}"
            )

    def _next_block_candidate_retry_delay(self, block_hash: str) -> float:
        return self._ensure_block_candidate_service().next_retry_delay(block_hash)

    def _defer_block_candidate(
        self,
        reason: str,
        message: str,
        *,
        worker: str | None,
    ) -> None:
        self._ensure_block_candidate_service().record_deferred(
            reason,
            message,
            worker=worker,
        )

    def _abandon_block_candidate(
        self,
        reason: str,
        message: str,
        *,
        worker: str | None,
    ) -> None:
        """Record a terminal or retryable block-path outcome."""
        self._ensure_block_candidate_service().record_abandoned(
            reason,
            message,
            worker=worker,
        )

    def active_block_candidate_height(self, block_hash: str) -> int | None:
        """Return the active-chain height for a previously submitted candidate."""
        try:
            header = self.rpc.call("getblockheader", [block_hash])
        except Exception as exc:
            detail = str(exc).lower()
            if "block not found" in detail or "not found" in detail or "-5" in detail:
                return None
            raise
        if not isinstance(header, dict):
            return None
        try:
            confirmations = int(header.get("confirmations", 0))
            height = int(header["height"])
        except (KeyError, TypeError, ValueError):
            return None
        return height if confirmations > 0 else None

    def _defer_for_pending_parent_payout_transition(
        self,
        *,
        parent_hash: str,
        parent_height: int,
        worker: str | None,
        active_candidate_hash: str | None = None,
        active_candidate_height: int | None = None,
    ) -> bool:
        """Defer finalization while an active payout ancestor is not durable."""
        if (active_candidate_hash is None) != (active_candidate_height is None):
            raise ValueError("active candidate hash and height must be provided together")

        def preserve_active_candidate_barrier() -> None:
            if active_candidate_hash is None or active_candidate_height is None:
                return
            self._begin_accepted_block_payout_preview(
                active_candidate_hash,
                block_height=active_candidate_height,
            )
            self._mark_accepted_block_payout_landed(
                active_candidate_hash,
                block_height=active_candidate_height,
            )

        try:
            pending_parent_transition = (
                self._accepted_block_payout_transition_for_parent(
                    parent_hash,
                    parent_height=parent_height,
                )
            )
        except TemplateRefreshBlocked as exc:
            preserve_active_candidate_barrier()
            self._abandon_block_candidate(
                PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                f"could not validate pending ancestor payout state: {exc}",
                worker=worker,
            )
            return True
        if pending_parent_transition is None:
            return False
        preserve_active_candidate_barrier()
        self._abandon_block_candidate(
            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
            "parent or ancestor payout confirmation is still pending",
            worker=worker,
        )
        return True

    @ledger_writer_operation("accepted_block_handling")
    def submit_block_candidate(self, candidate: PrismBlockCandidate) -> bool:
        return self._ensure_block_finalization_service().submit_block_candidate(
            candidate
        )

    @ledger_writer_operation("accepted_block_handling")
    def reject_prepared_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        reject = getattr(self.ledger, "reject_prepared_block", None)
        if callable(reject):
            return reject(block_hash=block_hash, active_tip_height=active_tip_height)
        return self.ledger.reverse_immature_block(
            block_hash=block_hash,
            active_tip_height=active_tip_height,
        )

    @staticmethod
    def verified_canonical_bundle_path(
        candidate_bundle_path: Path,
        report: dict[str, Any],
    ) -> Path | None:
        return AuditArtifactStore.verified_canonical_bundle_path(
            candidate_bundle_path,
            report,
        )

    def prune_audit_artifacts(self, *, keep_live_path: Path | None = None) -> None:
        self._ensure_audit_artifact_store().prune_best_effort(
            keep_live_path=keep_live_path
        )

    def verify_bundle(
        self,
        bundle_path: Path,
        coinbase_tx_hex: str,
        ledger_writer_public_key_hex: str,
        *,
        expected_coinbase_value_sats: int,
        expected_block_height: int | None = None,
    ) -> dict[str, Any]:
        return self._ensure_audit_artifact_store().verify_bundle(
            bundle_path,
            coinbase_tx_hex,
            ledger_writer_public_key_hex,
            expected_coinbase_value_sats=expected_coinbase_value_sats,
            expected_block_height=expected_block_height,
        )

    def trusted_ledger_writer_public_key_hex(self, bundle: dict[str, Any]) -> str:
        return AuditArtifactStore.trusted_writer_key(
            getattr(self, "ledger_writer_public_key_hex", None),
            bundle,
            allow_embedded_test_key=(
                getattr(self, "ledger_writer_public_key_hex", None) is None
            ),
        )

    @staticmethod
    def _progress_work_generation(
        snapshot: QbitTipTemplateSnapshot,
        payout_generation: int,
    ) -> WorkGeneration:
        return WorkGeneration(
            template_generation=int(snapshot.template_generation),
            template_fingerprint=snapshot.template_fingerprint,
            payout_generation=int(payout_generation),
        )

    def _progress_note_refresh_pending(
        self,
        started_monotonic: float | None = None,
    ) -> None:
        self._ensure_progress_health_service().mark_refresh_pending(
            started_monotonic
        )

    def _record_progress_tip_poll(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observed_monotonic: float | None = None,
    ) -> None:
        """Publish a coherent qbit tip/template observation to health state."""
        payout_generation = int(self._ensure_payout_state_service().snapshot().generation)
        self._ensure_progress_health_service().observe_tip(
            self._progress_work_generation(snapshot, payout_generation),
            observed_monotonic,
        )

    def _record_progress_payout_generation(
        self,
        generation: int,
        invalidated_monotonic: float | None = None,
    ) -> None:
        self._ensure_progress_health_service().observe_payout_generation(
            generation,
            invalidated_monotonic,
        )

    def _record_progress_publication(
        self,
        snapshot: QbitTipTemplateSnapshot,
        payout_generation: int,
    ) -> None:
        """Record that current in-memory work is available for delivery."""
        service = self._ensure_progress_health_service()
        published = service.publish_work(
            self._progress_work_generation(snapshot, payout_generation)
        )
        if published:
            service.reconcile_pending(self._progress_eligibility_snapshot())

    def _record_progress_delivery(
        self,
        client: ClientState,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> None:
        self._ensure_stratum_session_service().record_successful_delivery(
            client, context, delivered_monotonic
        )

    def _complete_job_delivery(
        self,
        client: ClientState,
        authority: object,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> bool:
        return self._ensure_job_delivery_service().complete_delivery(
            client,
            authority,  # type: ignore[arg-type]
            context,
            delivered_monotonic,
        )

    def _record_progress_delivery_to_health(
        self,
        client: ClientState,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> None:
        """Record a completed current-generation socket delivery."""
        fingerprint = getattr(context, "template_fingerprint", None)
        if fingerprint is None:
            fingerprint = qbit_template_fingerprint(context.template)
        work = WorkGeneration(
            template_generation=int(getattr(context, "template_generation", 0)),
            template_fingerprint=fingerprint,
            payout_generation=int(
                getattr(context, "payout_state_generation", 0)
            ),
        )
        ready_mode_required = self._ensure_job_bundle_service().ready_latched()
        with self.lock:
            client._progress_delivered_template_fingerprint = fingerprint
            client._progress_delivered_template_generation = (
                work.template_generation
            )
            client._progress_delivered_payout_generation = work.payout_generation
        service = self._ensure_progress_health_service()
        service.record_delivery(
            DeliveryProof(
                connection_id=client.connection_id,
                delivered_work=work,
                collection_only=bool(
                    getattr(context, "collection_only", False)
                ),
                delivered_monotonic=delivered_monotonic,
            ),
            ready_mode_required,
        )

    def _progress_eligibility_snapshot(self) -> EligibilitySnapshot:
        sessions = self._ensure_session_registry().eligible_snapshot()
        ready_mode_required = self._ensure_job_bundle_service().ready_latched()

        proofs: list[DeliveryProof] = []
        for connection_id, session in sessions.items():
            if session.delivered is None:
                continue
            delivered_context = session.delivered.context
            delivered_monotonic = session.delivered.delivered_monotonic
            fingerprint = getattr(
                delivered_context,
                "template_fingerprint",
                None,
            )
            if fingerprint is None:
                fingerprint = qbit_template_fingerprint(
                    delivered_context.template
                )
            proofs.append(
                DeliveryProof(
                    connection_id=connection_id,
                    delivered_work=WorkGeneration(
                        template_generation=int(
                            getattr(
                                delivered_context,
                                "template_generation",
                                0,
                            )
                        ),
                        template_fingerprint=fingerprint,
                        payout_generation=int(
                            getattr(
                                delivered_context,
                                "payout_state_generation",
                                0,
                            )
                        ),
                    ),
                    collection_only=bool(
                        getattr(delivered_context, "collection_only", False)
                    ),
                    delivered_monotonic=float(delivered_monotonic or 0.0),
                )
            )
        return EligibilitySnapshot(
            eligible_connection_ids=tuple(sessions),
            delivery_proofs=tuple(proofs),
            ready_mode_required=ready_mode_required,
        )

    def _progress_health_value(
        self,
        *,
        now: float | None = None,
    ) -> ProgressHealthSnapshot:
        eligibility = self._progress_eligibility_snapshot()
        payout_generation = int(self._ensure_payout_state_service().snapshot().generation)
        return self._ensure_progress_health_service().snapshot(
            eligibility,
            payout_generation,
            now=now,
        )

    def progress_health_snapshot(
        self,
        *,
        now: float | None = None,
    ) -> dict[str, object]:
        """Return the existing bounded progress-health payload."""
        return self._progress_health_value(now=now).as_mapping()

    @staticmethod
    def _apply_progress_health(
        payload: dict[str, object],
        progress: dict[str, object],
    ) -> dict[str, object]:
        return ObservabilityService.apply_progress_health(payload, progress)

    def progress_health_metrics_lines(self) -> list[str]:
        return list(
            self._ensure_progress_health_service().metrics_lines(
                self._progress_health_value()
            )
        )

    def ready_miner_count(self) -> int:
        return self.accepted_share_stats()[1]

    def mining_delivery_snapshot(
        self,
        *,
        now: float | None = None,
    ) -> dict[str, object]:
        return self._ensure_observability_service().mining_delivery_snapshot(now=now)

    def health_payload(self) -> dict[str, object]:
        return self._ensure_observability_service().health_payload()

    def refresh_health_snapshot(self) -> dict[str, object]:
        return self._ensure_observability_service().refresh_health_snapshot()

    def cached_health_payload(self) -> tuple[int, dict[str, object]]:
        return self._ensure_observability_service().cached_health_payload()

    def health_snapshot_loop(self) -> None:
        self._ensure_observability_service().health_snapshot_loop()

    def start_health_snapshot_refresher(self) -> None:
        service = self._ensure_observability_service()
        if not service.begin_refresh_loop():
            return
        try:
            service.refresh_health_snapshot()
        except Exception:
            print("prism coordinator: initial health snapshot refresh failed", flush=True)
            traceback.print_exc()
        registry = self._ensure_background_services()
        if not registry.contains("health_snapshot_refresher"):
            registry.register(self._health_snapshot_service_spec())
        self._start_background_service("health_snapshot_refresher")

    def refresh_metrics_snapshot(self) -> str:
        return self._ensure_observability_service().refresh_metrics_snapshot()

    def cached_metrics_payload(self) -> tuple[int, str]:
        return self._ensure_observability_service().cached_metrics_payload()

    def metrics_snapshot_loop(self) -> None:
        self._ensure_observability_service().metrics_snapshot_loop()

    def start_metrics_snapshot_refresher(self) -> None:
        service = self._ensure_observability_service()
        if not service.begin_metrics_refresh_loop():
            return
        try:
            service.refresh_metrics_snapshot()
        except Exception:
            print("prism coordinator: initial metrics snapshot refresh failed", flush=True)
            traceback.print_exc()
        registry = self._ensure_background_services()
        if not registry.contains("metrics_snapshot_refresher"):
            registry.register(self._metrics_snapshot_service_spec())
        self._start_background_service("metrics_snapshot_refresher")

    @property
    def latest_evidence(self) -> dict[str, Any] | None:
        if (
            "_audit_artifact_store" not in self.__dict__
            and "audit_dir" not in self.__dict__
            and "evidence_path" not in self.__dict__
        ):
            value = self.__dict__.get("_audit_latest_evidence_seed")
            return copy.deepcopy(value) if isinstance(value, dict) else None
        return self._ensure_audit_artifact_store().latest_evidence()

    @latest_evidence.setter
    def latest_evidence(self, payload: Mapping[str, Any] | None) -> None:
        if (
            "_audit_artifact_store" not in self.__dict__
            and "audit_dir" not in self.__dict__
            and "evidence_path" not in self.__dict__
        ):
            self.__dict__["_audit_latest_evidence_seed"] = (
                copy.deepcopy(dict(payload)) if payload is not None else None
            )
            return
        self._ensure_audit_artifact_store().set_latest_evidence_for_compatibility(
            payload
        )

    def latest_evidence_payload(self) -> dict[str, object] | None:
        return self._ensure_audit_artifact_store().latest_evidence()

    def owed_balances_payload(self) -> dict[str, object]:
        return {
            "schema": "qbit.prism.owed-balances.v1",
            "ledger_backend": self.ledger.backend_name,
            "balances": self.ledger.current_owed_balances(),
        }

    def carry_forward_integrity_payload(self) -> dict[str, object]:
        report = self.ledger.carry_forward_integrity_report()
        report["ledger_backend"] = self.ledger.backend_name
        return report

    def miner_status_payload(self, recipient_id: str) -> dict[str, object]:
        recipient_id = recipient_id.strip()
        if not recipient_id:
            raise ValueError("recipient_id is required")
        balances = [
            balance
            for balance in self.ledger.current_owed_balances()
            if str(balance.get("recipient_id", "")) == recipient_id
        ]
        owed_balance_sats = sum(int(balance.get("balance_sats", 0)) for balance in balances)
        return {
            "schema": "qbit.prism.miner-status.v1",
            "ledger_backend": self.ledger.backend_name,
            "recipient_id": recipient_id,
            "owed_balance_sats": owed_balance_sats,
            "owed_balances": balances,
            "recent_payouts": self.ledger.recipient_payout_history(recipient_id=recipient_id),
        }

    @staticmethod
    def process_resource_metrics() -> tuple[int, int]:
        """Return cheap Linux RSS/descriptor gauges without extra processes."""
        rss_bytes = -1
        open_descriptors = -1
        try:
            statm = Path("/proc/self/statm").read_text(encoding="ascii").split()
            rss_bytes = int(statm[1]) * int(os.sysconf("SC_PAGE_SIZE"))
        except (OSError, ValueError, IndexError):
            pass
        try:
            open_descriptors = len(tuple(Path("/proc/self/fd").iterdir()))
        except OSError:
            pass
        return rss_bytes, open_descriptors

    def share_accounting_snapshot(self) -> dict[str, object]:
        """Copy hot-path counters under their dedicated accounting lock."""
        self._ensure_share_hot_path_state()
        with self._share_accounting_lock:
            return {
                "submitted": int(getattr(self, "submitted_share_count", 0)),
                "stale": int(getattr(self, "stale_share_count", 0)),
                "duplicate": int(getattr(self, "duplicate_share_count", 0)),
                "low_difficulty": int(
                    getattr(self, "low_difficulty_share_count", 0)
                ),
                "collection_block": int(
                    getattr(self, "collection_block_submission_count", 0)
                ),
                "rejections": dict(
                    getattr(self, "rejection_counts_by_reason", {})
                ),
                "grace_credited": int(
                    getattr(self, "grace_credited_share_count", 0)
                ),
            }

    def coordinator_lock_contention_snapshot(self) -> tuple[int, float, float]:
        snapshot = getattr(self.lock, "contention_snapshot", None)
        if callable(snapshot):
            return snapshot()
        return 0, 0.0, 0.0

    def coordinator_lock_metrics_lines(self) -> list[str]:
        """Compatibility wrapper for the extracted metrics owner."""
        return self._ensure_metrics_renderer().coordinator_lock_metrics_lines()

    def block_submitter_snapshot(self) -> dict[str, object]:
        pending_metrics = {
            "pending_count": -1,
            "oldest_pending_age_seconds": -1.0,
            "oldest_unattempted_age_seconds": -1.0,
        }
        pending_snapshot = getattr(self.ledger, "block_candidate_pending_metrics", None)
        if callable(pending_snapshot):
            try:
                pending_metrics.update(pending_snapshot())
            except Exception:
                # Metrics collection is diagnostic. Candidate processing and
                # its watchdog remain authoritative when this read is down.
                pass
        backoff_active, backoff_remaining, backoff_delay = (
            self._ensure_block_candidate_service().backoff_snapshot()
        )
        return {
            **pending_metrics,
            "backoff_active": backoff_active,
            "backoff_remaining_seconds": backoff_remaining,
            "backoff_delay_seconds": backoff_delay,
        }

    def block_submitter_metrics_lines(self) -> list[str]:
        """Compatibility wrapper for the extracted metrics owner."""
        return self._ensure_metrics_renderer().block_submitter_metrics_lines()

    def _render_metrics_payload(self) -> str:
        return self._ensure_metrics_renderer().render()

    def metrics_payload(self) -> str:
        """Compatibility renderer; HTTP uses the complete cached snapshot."""

        return self._render_metrics_payload()

    def shutdown_metrics_lines(self) -> list[str]:
        return self._ensure_metrics_renderer().shutdown_metrics_lines()

    def audit_artifact_metrics(self) -> dict[str, dict[str, int] | int]:
        return self._ensure_audit_artifact_store().metrics_snapshot()

    @staticmethod
    def audit_artifact_kind(name: str) -> str:
        return AuditArtifactStore.artifact_kind(name)

    def ctv_fanout_broadcaster_metrics_lines(self) -> list[str]:
        return self._ensure_ctv_runtime().metrics_lines()

    def initial_delivery_metrics_lines(self) -> list[str]:
        return self._ensure_metrics_renderer().initial_delivery_metrics_lines()

    def vardiff_idle_metrics_lines(self) -> list[str]:
        return self._ensure_vardiff_service().metrics_lines()

    def block_finalization_metrics_lines(self) -> list[str]:
        return self._ensure_block_finalization_service().metrics_lines()

    def tip_refresh_metrics_lines(self) -> list[str]:
        return self._ensure_tip_refresh_service().metrics_lines()

    def job_build_metrics_lines(self) -> list[str]:
        return self._ensure_metrics_renderer().job_build_metrics_lines()

    def payout_state_metrics_lines(self) -> list[str]:
        return self._ensure_payout_state_service().metrics_lines()


def make_audit_handler(coordinator: PrismCoordinator) -> type[BaseHTTPRequestHandler]:
    """Compatibility factory backed by the coordinator-free H1 facade."""

    return AuditHttpFacade(
        _CoordinatorAuditHttp(
            coordinator,
            allow_uncached_compatibility=True,
        )
    ).handler_type()


def target_from_compact(bits_hex: str) -> int:
    return direct_stratum.target_from_compact_hex(bits_hex)


def scaled_network_difficulty(bits_hex: str) -> int:
    template_target = target_from_compact(bits_hex)
    return scaled_target_difficulty(template_target)


def scaled_target_difficulty(target: int) -> int:
    if target <= 0:
        raise ValueError("target must be positive")
    pow_limit_target = target_from_compact("207fffff")
    return max(1, (pow_limit_target * 1_000_000) // target)


def main() -> int:
    coordinator = PrismCoordinator()

    def _request_shutdown(signum: int, _frame: Any) -> None:
        # Keep the handler to an atomic admission close plus wakeup. Writer
        # quiescence, lease I/O, logging, and thread drainage run in normal
        # control flow after serve observes the event.
        coordinator.request_shutdown(signum)

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)
    try:
        coordinator.serve()
    finally:
        coordinator.shutdown(reason="main_finally")
        coordinator.drain_non_writer_components()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
