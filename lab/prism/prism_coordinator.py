#!/usr/bin/env python3
"""Minimal live direct qbit Stratum coordinator for PRISM regtest proof."""

from __future__ import annotations

import copy
from collections import OrderedDict
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
)
from contextlib import ExitStack, contextmanager
import faulthandler
import hashlib
import json
import os
import queue
import random  # noqa: F401 - patch seam for tip-refresh holdoff jitter tests
import shlex
import signal
import socket
import subprocess
import tempfile
import threading
import time
import traceback
import uuid
from dataclasses import replace as dataclass_replace
from decimal import Decimal, ROUND_CEILING
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

import sys

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.auxpow import stratum_codec, vardiff
from lab.prism import direct_stratum
# Compatibility re-exports; new callers should import lab.prism.bounded_executor.
from lab.prism.bounded_executor import (
    _BoundedPriorityExecutor,  # noqa: F401 - compatibility re-export
    _DeliveryQueueFull,  # noqa: F401 - compatibility re-export
)
from lab.prism.prism_tools import prism_tool_command
# Compatibility re-exports; new callers should import lab.prism.background_services.
from lab.prism.background_services import (
    BackgroundServiceRegistry,  # noqa: F401 - compatibility re-export
    BackgroundServiceSpec,  # noqa: F401 - compatibility re-export
    LedgerLeaseHeartbeatPorts,
    LedgerLeaseHeartbeatService,
    WatchdogPorts,
    WatchdogService,
    _LeaseHeartbeatStateField,
    guard_session_verifier as _lease_guard_session_verifier,
)
# Compatibility re-exports; new callers should import lab.prism.coordinator_shutdown.
from lab.prism.coordinator_shutdown import (
    CoordinatorShutdownController,  # noqa: F401 - compatibility re-export
    ShutdownInProgress,  # noqa: F401 - compatibility re-export
    ledger_writer_operation,  # noqa: F401 - compatibility re-export
)
from lab.prism.coordinator_config import (
    BLOCK_LANDING_DB_TIMEOUT_WATCHDOG_FRACTION,  # noqa: F401
    CoordinatorConfig,
    apply_python_switch_interval,
    DEFAULT_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX,  # noqa: F401
    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_MAX_SECONDS,  # noqa: F401
    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_SECONDS,  # noqa: F401
    DEFAULT_BLOCK_SUBMIT_LOCK_WAIT_LOG_SECONDS,  # noqa: F401
    DEFAULT_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS,
    DEFAULT_BLOCK_SUBMIT_STUCK_CALL_EXIT_SECONDS,  # noqa: F401
    DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS,
    DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
    DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS,
    DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION,
    DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS,
    DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
    DEFAULT_PRISM_COORDINATION_BLOCKED_EXIT_SECONDS,
    DEFAULT_PRISM_CTV_BROADCASTER_CHUNK_SIZE,
    DEFAULT_PRISM_DISCONNECTED_JOB_RETENTION,  # noqa: F401
    DEFAULT_PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS,
    DEFAULT_PRISM_HEALTH_REFRESH_SECONDS,  # noqa: F401 - compatibility re-export
    DEFAULT_PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS,
    DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
    DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS,
    DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS,
    DEFAULT_PRISM_JOB_BUILD_TIMEOUT_SECONDS,
    DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS,
    DEFAULT_PRISM_LEDGER_LEASE_EXTERNAL_FENCE_TIMEOUT_SECONDS,
    DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_EXIT_TIMEOUT_SECONDS,
    DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_FAILURE_SECONDS,
    DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_MONITOR_SECONDS,
    DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_SECONDS,
    DEFAULT_PRISM_METRICS_REFRESH_SECONDS,  # noqa: F401 - compatibility re-export
    DEFAULT_PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS,  # noqa: F401
    DEFAULT_PRISM_OBSERVED_TIP_ACCEPT_WINDOW_SECONDS,  # noqa: F401
    DEFAULT_PRISM_PAYOUT_ARTIFACT_FULL_RESCAN_SECONDS,  # noqa: F401
    DEFAULT_PRISM_PAYOUT_ARTIFACT_MAX_ANCHOR_AGE_SECONDS,  # noqa: F401
    DEFAULT_PRISM_PAYOUT_ARTIFACT_MIN_BUILD_INTERVAL_SECONDS,  # noqa: F401
    DEFAULT_PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS,  # noqa: F401
    DEFAULT_PRISM_PAYOUT_ARTIFACT_REARM_MIN_SECONDS,  # noqa: F401
    DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
    DEFAULT_PRISM_ROUTINE_ADMISSION_DEADLINE_SECONDS,  # noqa: F401
    DEFAULT_PRISM_STALE_GRACE_SECONDS,
    DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS,  # noqa: F401
    DEFAULT_PRISM_STRATUM_BIND_RETRY_SECONDS,  # noqa: F401
    DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS,
    DEFAULT_PRISM_STRATUM_LISTEN_BACKLOG,  # noqa: F401
    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME,  # noqa: F401
    DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS,
    DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS,
    DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
    DEFAULT_PRISM_TIP_REFRESH_FAILURE_HOLDOFF_SECONDS,  # noqa: F401
    DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS,
    DEFAULT_PRISM_WATCHDOG_LEASE_RELEASE_TIMEOUT_SECONDS,
    DEFAULT_PRISM_WATCHDOG_TIMEOUT_SECONDS,  # noqa: F401
    DEFAULT_PRISM_WORKER_METRICS_LIMIT,
    DEFAULT_PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS,
    LEASE_AUTHORITY_MARGIN_HEADROOM_SECONDS,
    DEFAULT_SHARE_COMMIT_BATCH_SIZE,  # noqa: F401 - compatibility re-export
    DEFAULT_SHARE_COMMIT_LINGER_MILLISECONDS,  # noqa: F401 - compatibility re-export
    DEFAULT_SHARE_COMMIT_TIMEOUT_SECONDS,  # noqa: F401 - compatibility re-export
    StratumListenerProfile,  # noqa: F401 - compatibility re-export
    TESTNET_QBIT_CHAINS,  # noqa: F401 - compatibility re-export
    VALID_COINBASE_OUTPUT_POLICIES,
    default_prism_payout_policy,
    default_prism_username_fallback_address,  # noqa: F401 - compatibility re-export
    env,
    env_bool,
    env_int,
    env_nonnegative_float,
    env_nonnegative_int,
    env_optional,
    env_optional_positive_int,
    env_optional_positive_int_with_legacy,
    env_positive_float,
    env_positive_int,
    env_positive_int_with_legacy,
    load_coordinator_config,
    load_share_weights,
    resolve_initial_job_max_workers,  # noqa: F401 - compatibility re-export
    validate_hex,
    validate_initial_job_max_workers,  # noqa: F401 - compatibility re-export
    validate_job_build_executor_workers,  # noqa: F401 - compatibility re-export
    validate_payout_artifact_age_bounds,  # noqa: F401 - compatibility re-export
)
from lab.prism.ctv_broadcaster import CtvFanoutBroadcaster
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
# Compatibility re-exports; new callers should import the owning J1 modules.
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
from lab.prism.observability import (
    METRICS_STATE_FRESH,
    METRICS_STATE_UNAVAILABLE,
    MetricsSnapshotResponse,
    MiningDeliveryInputs,
    ObservabilityPort,
    ObservabilityService,
)
from lab.prism.block_candidates import (
    BLOCK_SUBMITTER_WAIT_HEARTBEAT_SLICE_SECONDS,
    BlockCandidateAttemptResult,  # noqa: F401 - compatibility re-export
    BlockCandidateCompatibilityField,
    BlockCandidatePorts,
    BlockCandidateRunResult,  # noqa: F401 - compatibility re-export
    BlockCandidateService,
    BlockCandidateStateField,
    BlockSubmitterDatabaseTimeout,  # noqa: F401 - compatibility re-export
    DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH,  # noqa: F401 - compatibility re-export
    DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS,
    DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS,
    MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS,  # noqa: F401 - compatibility re-export
    MAX_PENDING_BLOCK_CANDIDATES,
    PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
    PRISM_REJECTION_LEDGER_CONFIRMATION_SUPERSEDED,
    PRISM_STALE_JOB_ABANDON_CLASSES,
    PrismBlockCandidate,
    _BlockCandidateAccountingTask,  # noqa: F401 - compatibility re-export
    _BlockCandidateDispositionFlight,  # noqa: F401 - compatibility re-export
    _BlockCandidateDispositionLease,  # noqa: F401 - compatibility re-export
    _BlockCandidateNodeSubmission,
    _BlockSubmitterLedgerCall,  # noqa: F401 - compatibility re-export
    _BlockSubmitterRpcCall,  # noqa: F401 - compatibility re-export
    _STATE_FIELD_MAP as _BLOCK_CANDIDATE_STATE_FIELD_MAP,
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
from lab.prism.metrics import MetricsRenderer
from lab.prism.reorg_reconciler import (
    DEFAULT_PRISM_RECONCILE_FLIGHT_WAIT_SECONDS,
    PRISM_RECONCILE_PREFETCH_JOIN_TIMEOUT_SECONDS,
    ReorgCompatibilityField,
    ReorgPorts,
    ReorgReconcilerService,
    qbit_chain_view_untrusted as reorg_chain_view_untrusted,
)
from lab.prism.vardiff_service import (
    PRISM_VARDIFF_IDLE_SECONDS_BUCKETS,
    PRISM_VARDIFF_IDLE_SKIP_REASONS,
    VARDIFF_COMPATIBILITY_FIELDS,
    IdleRetargetRequest as _IdleRetargetRequest,
    VardiffCompatibilityField,
    VardiffService,
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
    BlockSolvesDroppedCompatibilityField,
    RecentShareCompatibilityField,
    RecentShareIndex,
    ShareSubmissionPorts,
    ShareSubmissionService,
    SubmitControlSnapshot,
    empty_share_ack_histograms,
)
from lab.prism.bundle_compiler import (
    BundleCompiler,
    PRISM_BUILDER_PHASE_METRICS_PREFIX,  # noqa: F401 - compatibility re-export
    PRISM_SERVE_BUILDER_PROTOCOL_VERSION,  # noqa: F401 - compatibility re-export
    PRISM_SERVE_BUILDER_WINDOW_CACHE_ENTRIES,  # noqa: F401 - compatibility re-export
    PRISM_SPOOL_SPLICE_CHUNK_BYTES,  # noqa: F401 - compatibility re-export
    _ServeBuilderClient,  # noqa: F401 - compatibility re-export
    _ServeBuilderUnavailable,  # noqa: F401 - compatibility re-export
    _ShareWindowSerialization,  # noqa: F401 - compatibility re-export
    _compact_share_payload,  # noqa: F401 - compatibility re-export
    _share_window_spool_file,  # noqa: F401 - compatibility re-export
    canonical_bundle_bytes,
)
from lab.prism.job_bundle import (
    CachedJobBundle,
    JobBuildAdmissionDeadlineExceeded,  # noqa: F401 - compatibility re-export
    PRISM_JOB_BUILD_SECONDS_BUCKETS,
    JobBuildCancellation as _JobBuildCancellation,
    JobBuildCancelled,
    JobBuildFlight as _JobBuildFlight,
    JobBuildRequest as _JobBuildRequest,
    JobBuildSuperseded,
    JobBundleBuildControl as _JobBundleBuildControl,
    JobBundleBuildSuperseded as _JobBundleBuildSuperseded,
    JobBundleService,
)
from lab.prism.job_delivery import (
    EvictedJobEntry,
    JobBuildFailed as _JobBuildFailed,
    JobDeliveryService,
    PRISM_CREDIT_POLICY_STALE_GRACE,
    PRISM_DELIVERY_PRIORITY_INITIAL,
    PRISM_DELIVERY_PRIORITY_NEW_TIP,
    PRISM_DELIVERY_PRIORITY_SAME_TIP,
    PRISM_EVICTED_JOB_CAPACITY_SCOPES,
    PRISM_EVICTED_JOB_CLASSES,
    PRISM_EVICTED_JOB_SUBMIT_OUTCOMES,
    PendingInitialJob,
    PrismJobContext,
)
from lab.prism.payout_state import (
    PayoutStatePublicationBlocked,
    TemplateRefreshBlocked,
    TemplateRefreshSuperseded,
)
from lab.prism.template_artifacts import (
    CachedTemplateArtifacts,
    QbitTipTemplateSnapshot,
    TemplateArtifactRepository,  # noqa: F401 - compatibility re-export
)
from lab.prism.tip_refresh import (
    FanoutCancellation as _FanoutCancellation,
    PRISM_TIP_REFRESH_BUILD_PHASES,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_COVERAGE_TARGETS,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_FAILURE_HOLDOFF_JITTER_FRACTION,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_REENTRY_BACKOFF_SECONDS,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_SECONDS_BUCKETS,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_WAVE_OUTCOMES,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_WAVE_PASS_BUDGET,  # noqa: F401 - compatibility re-export
    RefreshResult,
    RetainedCollectionRefresh,
    TipRefreshService,
    TipRefreshValidationToken,
    _TipRefreshEpochCoverage,  # noqa: F401 - compatibility re-export
    _TipRefreshFanoutSuperseded,  # noqa: F401 - compatibility re-export
    _TipRefreshTrustBlocked,  # noqa: F401 - compatibility re-export
)
# Compatibility re-exports; new callers should import lab.prism.payout_state.
from lab.prism.payout_state import (
    DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS,  # noqa: F401 - compatibility re-export
    DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
    PRISM_PAYOUT_ARTIFACT_REARM_BACKOFF_CAP,  # noqa: F401 - compatibility re-export
    PRISM_PAYOUT_DELIVERY_GENERATIONS,  # noqa: F401 - compatibility re-export
    PayoutLedgerArtifact,
    PayoutStateArtifact,
    PayoutStateCandidate,
    PayoutStateService,
    PayoutStateSnapshot,  # noqa: F401 - compatibility re-export
    _IncrementalPayoutArtifactWindow,  # noqa: F401 - compatibility re-export
    _PayoutWindowMaterialization,
)
# Compatibility re-exports; new callers should import lab.prism.share_writer.
from lab.prism.share_writer import (
    MAX_PENDING_SHARE_APPENDS,
    PENDING_SHARE_COMMIT_WARN_SECONDS as PRISM_PENDING_SHARE_COMMIT_WARN_SECONDS,  # noqa: F401
    PendingShareAppend,
    ShareWriter,
    ShareWriterCompatibilityField,
    ShareWriterError,  # noqa: F401 - compatibility re-export
    ShareWriterQueueFull,  # noqa: F401 - compatibility re-export
)
# Compatibility re-exports; new callers should import lab.prism.stratum_session.
from lab.prism.stratum_session import (
    ClientState,
    P2mrAddressValidator,
    SessionRegistry,  # noqa: F401 - compatibility re-export
    StratumError,
    StratumSessionService,
    WorkerIdentity,
    _P2mrAddressValidationFlight,  # noqa: F401 - compatibility re-export
    apply_stratum_send_timeout as apply_socket_send_timeout,
    client_vardiff_lock,
    difficulty_payload as stratum_difficulty_payload,
    error_payload as stratum_error_payload,
    job_payload as stratum_job_payload,
    result_payload as stratum_result_payload,
    stratum_accept_heartbeat_names as configured_accept_heartbeat_names,
)
# Compatibility re-exports; new callers should import lab.prism.progress_health.
from lab.prism.progress_health import (
    EligibilitySnapshot,  # noqa: F401 - compatibility re-export
    ProgressHealthConfig,
    ProgressHealthService,
    ProgressHealthSnapshot,
    WorkGeneration,
)
from lab.prism.rpc import (
    DEFAULT_QBIT_RPC_CALL_TIMEOUT_SECONDS,
    JsonRpc,
    _QBIT_RPC_NO_TRANSPORT_RETRY_METHODS,  # noqa: F401 - compatibility re-export
)
from lab.prism.share_ledger import (
    DEFAULT_AUDIT_SHARE_SEGMENT_SIZE,
    DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
    DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
    DEFAULT_LEASE_ACQUIRE_ATTEMPTS,
    DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS,
    DEFAULT_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS,
    DEFAULT_POSTGRES_TCP_KEEPALIVES_COUNT,
    DEFAULT_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS,
    DEFAULT_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS,
    DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,  # noqa: F401 - compatibility re-export
    DaemonWindowMirrorDivergence,
    PendingShare,
    PsqlShareLedger,
    SingleWriterShareLedger,
    WRITER_LEASE_HEARTBEAT_SESSION_PREFIX,
    WRITER_LEASE_VERIFICATION_MAX_STATEMENTS,  # noqa: F401 - compatibility re-export
    WriterLeaseRenewalDeferred,  # noqa: F401 - compatibility re-export
)

MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES = 128
# The reorg reconcile memo/flight/prefetch constants and machinery moved to
# lab.prism.reorg_reconciler; the two runtime-override defaults the port
# wiring reads are re-imported above.
# Read-to-ack latency labels for mining.submit now live with the
# share-submission owner (PRISM_SHARE_ACK_RESULTS is re-exported above).
# Cancellation-check slice while an initial request rides a subscribed
# publication-priority build promise. Promise completion wakes the waiter
# immediately; this only bounds how stale a cancellation can go unnoticed.
PRISM_INITIAL_JOB_SUBSCRIBE_POLL_SECONDS = 0.25
# A flight whose executor future is finished normally leaves its slot inside
# the future's done callback. The admission sweep only treats such a flight
# as orphaned after this grace, so a callback that is merely between future
# completion and slot cleanup is never mistaken for a dead one.
PRISM_JOB_BUILD_ORPHAN_SWEEP_GRACE_SECONDS = 1.0
# The vardiff idle worker/pending limits moved to lab.prism.vardiff_service;
# the bucket/skip registries are re-imported above for metric consumers.
# Accepted shares use a small, bounded group-commit queue.  Every submitter
# waits for its batch's Postgres commit before receiving Stratum success, so
# this is a latency-smoothing bound rather than a durable backlog.
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
# The job-build seconds buckets, phase registry, and cache-kind registry are
# canonical in lab.prism.job_bundle; the metrics renderer imports them there.
# The ten submit-path rejection reasons moved to lab.prism.share_submission
# and the four accepted-block finalization reasons moved to
# lab.prism.block_finalization; both groups are re-exported above. Only the
# process-internal reason stays here.
PRISM_REJECTION_INTERNAL_ERROR = "internal-error"
PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS = frozenset(
    {
        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
        PRISM_REJECTION_INTERNAL_ERROR,
        PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
        PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
        PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
    }
)
# Used only by lightweight embedders that bypass dataclass/coordinator
# construction. Production instances install these locks eagerly in __init__.
# Serializing the fallback prevents concurrent first-touch callers from ever
# publishing different lock objects for the same state.
_HOT_PATH_LOCK_INITIALIZATION_LOCK = threading.Lock()
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
    PRISM_REJECTION_LEDGER_CONFIRMATION_SUPERSEDED,
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


class _ObservedRLock:
    """RLock with no mutex acquisition on uncontended acquisitions.

    The coordinator lock protects control-plane publication state. Its wait
    counters intentionally record only acquisitions that fail an immediate
    probe, and the shared mutex guarding them is therefore never taken on an
    uncontended acquisition, so observing this lock cannot recreate the
    share-path convoy the metrics are meant to diagnose.

    A contended count on its own is an absolute number with no denominator, so
    a contended percentage cannot be derived from a live process. The
    acquisition counter that supplies that denominator does run on every
    acquisition, contended or not, and preserves the property above by where
    it runs rather than by how often: it is incremented only while the
    underlying RLock is already held, which puts it inside a critical section
    the lock has already serialised. It therefore adds no mutex acquisition
    and no cross-thread ordering that the lock did not already impose, and it
    cannot convoy the fast path.

    That placement is also why this counter does not rest on an increment
    being atomic under the GIL -- the lock, not the interpreter, is what makes
    it safe, so it stays correct on a free-threaded build (see #150, which
    audits that class of assumption in this tree). The one unsynchronised
    access is the read in contention_snapshot, which holds only
    _metrics_lock: it assumes an attribute read yields a whole value rather
    than a torn one, and may observe the counter one increment stale. Both are
    acceptable for a monotonic counter read by a metrics scrape, and neither
    is load-bearing for correctness anywhere else.

    Only granted acquisitions are counted. A wait that times out records
    contention without an acquisition, so where a caller passes a timeout the
    derived contended percentage is an upper bound. Hold *duration* is
    deliberately not instrumented: a re-entrant lock would need per-thread
    depth tracking to avoid double counting nested holds, and a clock read on
    every acquire and release is exactly the fast-path work this class exists
    to avoid.
    """

    def __init__(
        self,
        *,
        wait_observer: Callable[[float], None] | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._metrics_lock = threading.Lock()
        # Guarded by _lock itself, never by _metrics_lock. See the class
        # docstring: every write below happens with _lock held.
        self._acquisition_count = 0
        self._contention_count = 0
        self._wait_seconds_sum = 0.0
        self._wait_seconds_max = 0.0
        self._wait_observer = wait_observer

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if not blocking:
            if not self._lock.acquire(blocking=False):
                return False
            self._acquisition_count += 1
            return True
        if self._lock.acquire(blocking=False):
            self._acquisition_count += 1
            return True
        started = time.monotonic()
        observer = self._wait_observer
        if observer is None:
            acquired = self._lock.acquire(blocking=True, timeout=timeout)
        else:
            deadline = None if timeout < 0 else started + max(0.0, timeout)
            acquired = False
            while not acquired:
                wait_slice = BLOCK_SUBMITTER_WAIT_HEARTBEAT_SLICE_SECONDS
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    wait_slice = min(wait_slice, remaining)
                acquired = self._lock.acquire(
                    blocking=True,
                    timeout=wait_slice,
                )
                if not acquired:
                    observer(max(0.0, time.monotonic() - started))
        waited = max(0.0, time.monotonic() - started)
        if acquired:
            # Still under _lock, as on the uncontended path above.
            self._acquisition_count += 1
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

    def contention_snapshot(self) -> tuple[int, int, float, float]:
        """Return (acquisitions, contentions, wait_sum, wait_max).

        The acquisition count is read without _lock on purpose -- taking the
        coordinator lock to publish a metric would be the observation cost
        this class refuses to pay.
        """
        with self._metrics_lock:
            return (
                self._acquisition_count,
                self._contention_count,
                self._wait_seconds_sum,
                self._wait_seconds_max,
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
    incremental_digest = getattr(value, "canonical_json_sha256", None)
    if callable(incremental_digest):
        return str(incremental_digest())
    return hashlib.sha256(canonical_json_text(value).encode()).hexdigest()


class _BundlePreparationSuperseded(TemplateRefreshSuperseded):
    """The exact work identity lost to a newer tip/template observation.

    Subclasses TemplateRefreshSuperseded: losing the shared-bundle build race
    to a newer tip/template observation is coordination churn, so it escapes
    the poll without arming the template-refresh failure budget.
    """


class _ProgressHealthStateField:
    """Route a legacy raw ``_progress_*`` coordinator attribute to its owner.

    G1 owns the aggregate progress state. Reads and writes deliberately take
    no lock: the historical white-box protocol holds
    ``coordinator._progress_health_lock`` (the service lock) around direct
    field access, and the service's own methods lock internally.
    """

    def __init__(self, attribute: str) -> None:
        self._attribute = attribute

    def __get__(
        self,
        instance: "PrismCoordinator | None",
        owner: type | None = None,
    ) -> object:
        if instance is None:
            return self
        return getattr(
            instance._ensure_progress_health_service(),
            self._attribute,
        )

    def __set__(self, instance: "PrismCoordinator", value: object) -> None:
        setattr(
            instance._ensure_progress_health_service(),
            self._attribute,
            value,
        )


class _JobBundleStateField:
    """Route one legacy J1 coordinator field to its owner service.

    The descriptor keeps the historical attribute name readable and writable
    on the coordinator while :class:`JobBundleService` owns the single
    mutable copy, mirroring the progress-health extraction pattern. Direct
    test assignment through the legacy name is thereby adopted by the owner.
    """

    def __init__(self, attribute: str) -> None:
        self._attribute = attribute

    def __get__(
        self,
        instance: "PrismCoordinator | None",
        owner: type | None = None,
    ) -> object:
        if instance is None:
            return self
        return getattr(
            instance._ensure_job_bundle_service(),
            self._attribute,
        )

    def __set__(self, instance: "PrismCoordinator", value: object) -> None:
        setattr(
            instance._ensure_job_bundle_service(),
            self._attribute,
            value,
        )


class _TemplateArtifactStateField:
    """Route the legacy template-artifact fields to their repository owner."""

    def __init__(self, attribute: str) -> None:
        self._attribute = attribute

    def __get__(
        self,
        instance: "PrismCoordinator | None",
        owner: type | None = None,
    ) -> object:
        if instance is None:
            return self
        repository = instance._ensure_job_bundle_service().template_repository
        return getattr(repository, self._attribute)

    def __set__(self, instance: "PrismCoordinator", value: object) -> None:
        repository = instance._ensure_job_bundle_service().template_repository
        if self._attribute == "_template_artifacts":
            repository.adopt_template_artifacts(value)
            return
        repository.adopt_template_artifact_generation(value)


class _BundleCompilerStateField:
    """Route the legacy serve-builder daemon fields to the compiler owner."""

    def __init__(self, attribute: str) -> None:
        self._attribute = attribute

    def __get__(
        self,
        instance: "PrismCoordinator | None",
        owner: type | None = None,
    ) -> object:
        if instance is None:
            return self
        return getattr(instance._ensure_bundle_compiler(), self._attribute)

    def __set__(self, instance: "PrismCoordinator", value: object) -> None:
        setattr(instance._ensure_bundle_compiler(), self._attribute, value)


class _TipRefreshStateField:
    """Route one legacy R1 coordinator field to its owner service.

    The descriptor keeps the historical attribute name readable and writable
    on the coordinator while :class:`TipRefreshService` owns the single
    mutable copy. The twelve historical plain fields (and the tip-detection
    epoch) are lazily created on the service, so ``hasattr``/``getattr``
    -with-default semantics of the pre-extraction coordinator are preserved
    exactly: reading an unset field raises AttributeError until first write.
    """

    def __init__(self, attribute: str) -> None:
        self._attribute = attribute

    def __get__(
        self,
        instance: "PrismCoordinator | None",
        owner: type | None = None,
    ) -> object:
        if instance is None:
            return self
        return getattr(
            instance._ensure_tip_refresh_service(),
            self._attribute,
        )

    def __set__(self, instance: "PrismCoordinator", value: object) -> None:
        setattr(
            instance._ensure_tip_refresh_service(),
            self._attribute,
            value,
        )


class _JobDeliveryStateField:
    """Route one legacy S2 coordinator field to its owner service.

    The descriptor keeps the historical attribute name readable and writable
    on the coordinator while :class:`JobDeliveryService` owns the single
    mutable copy, mirroring the J1/G1 extraction pattern. Direct test
    assignment through the legacy name is thereby adopted by the owner.
    """

    def __init__(self, attribute: str) -> None:
        self._attribute = attribute

    def __get__(
        self,
        instance: "PrismCoordinator | None",
        owner: type | None = None,
    ) -> object:
        if instance is None:
            return self
        return getattr(
            instance._ensure_job_delivery_service(),
            self._attribute,
        )

    def __set__(self, instance: "PrismCoordinator", value: object) -> None:
        setattr(
            instance._ensure_job_delivery_service(),
            self._attribute,
            value,
        )


class _PayoutStateStateField:
    """Route one legacy P1 coordinator field to its owner service.

    The descriptor keeps the historical attribute name readable and writable
    on the coordinator while :class:`PayoutStateService` owns the single
    mutable copy, mirroring the S1/S2/S3/J1/G1 extraction pattern. Direct
    test assignment through the legacy name is thereby adopted by the owner.
    """

    def __init__(self, attribute: str) -> None:
        self._attribute = attribute

    def __get__(
        self,
        instance: "PrismCoordinator | None",
        owner: type | None = None,
    ) -> object:
        if instance is None:
            return self
        return getattr(
            instance._ensure_payout_state_service(),
            self._attribute,
        )

    def __set__(self, instance: "PrismCoordinator", value: object) -> None:
        setattr(
            instance._ensure_payout_state_service(),
            self._attribute,
            value,
        )


class _StratumSessionStateField:
    """Route one legacy S1 coordinator field to its owner service.

    The descriptor keeps the historical attribute name readable and writable
    on the coordinator while :class:`StratumSessionService` (its
    ``SessionRegistry`` and ``P2mrAddressValidator``) owns the single mutable
    copy, mirroring the S2/J1/G1 extraction pattern. Direct test assignment
    through the legacy name is thereby adopted by the owner.
    """

    def __init__(self, attribute: str) -> None:
        self._attribute = attribute

    def __get__(
        self,
        instance: "PrismCoordinator | None",
        owner: type | None = None,
    ) -> object:
        if instance is None:
            return self
        return getattr(
            instance._ensure_stratum_session_service(),
            self._attribute,
        )

    def __set__(self, instance: "PrismCoordinator", value: object) -> None:
        setattr(
            instance._ensure_stratum_session_service(),
            self._attribute,
            value,
        )


class _CoordinatorObservability(ObservabilityPort):
    """Collect immutable health inputs without exporting coordinator state."""

    def __init__(self, coordinator: "PrismCoordinator") -> None:
        self.coordinator = coordinator

    def monotonic(self) -> float:
        return time.monotonic()

    def mining_delivery_inputs(self, now: float) -> MiningDeliveryInputs:
        coordinator = self.coordinator
        coordinator._ensure_initial_job_state()
        # The first-job admission domain is owned by its own lock in the S2
        # service. Copy it once, before the coordinator lock is taken, so this
        # refresh never holds both locks and never reads a half-committed
        # replacement (#159).
        admission = (
            coordinator._ensure_job_delivery_service()
            .initial_job_admission_snapshot()
        )
        pending_jobs = admission.pending
        with coordinator.lock:
            active = len(coordinator.clients)
            current_tip = coordinator._current_published_tip_hash_locked()
            published_snapshot = getattr(coordinator, "tip_template_snapshot", None)
            subscribed = sum(
                1 for client in coordinator.clients if client.subscribed
            )
            authorized_clients = [
                client
                for client in coordinator.clients
                if client.subscribed
                and client.authorized
                and client.worker is not None
            ]
            authorized = len(authorized_clients)
            clients_with_current_work = [
                client
                for client in authorized_clients
                if coordinator._client_has_current_tip_job_locked(client)
            ]
            current = len(clients_with_current_work)
            # Semantic currency alongside the strict gauge above: fingerprint
            # and payout generation only, no template-generation or object
            # identity terms. The strict predicate reads 0 whenever a
            # generation was minted since the last fanout (it also gates
            # mining health, so its fail-closed shape stays untouched);
            # this gauge answers the operational question the 2026-07-31
            # review had to reconstruct from share-acceptance rates --
            # whether miners actually hold work for the current template
            # content.
            payout_generation_now = int(
                getattr(coordinator, "_payout_state_generation", 0)
            )
            semantic_current = sum(
                1
                for client in authorized_clients
                if client.active_job is not None
                and published_snapshot is not None
                and getattr(client.active_job, "template_fingerprint", None)
                == published_snapshot.template_fingerprint
                and int(
                    getattr(client.active_job, "payout_state_generation", -1)
                )
                == payout_generation_now
            )
            clients_with_no_active_job = sum(
                1 for client in authorized_clients if client.active_job is None
            )
            pending_requests = list(pending_jobs.values())
            pending = len(pending_requests)
            oldest_age = max(
                (
                    max(0.0, now - request.requested_monotonic)
                    for request in pending_requests
                ),
                default=0.0,
            )
            genuinely_pending_initial_clients = [
                client
                for client in authorized_clients
                if not JobDeliveryService._client_has_delivered_work_locked(client)
            ]
            genuine_initial_started = [
                started
                for client in genuinely_pending_initial_clients
                for started in (
                    client.authorized_monotonic,
                    (
                        pending_jobs[client].requested_monotonic
                        if client in pending_jobs
                        else None
                    ),
                )
                if started is not None
            ]
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
            pending_limit = int(coordinator.stratum_max_pending_initial_jobs)
            timeout = float(coordinator.stratum_initial_job_timeout_seconds)
            timeout_disconnects = admission.timeout_disconnects
            queue_rejections = admission.queue_rejections
            cancelled = admission.cancelled
            coalesced = admission.coalesced
            queue_capacity_reclaimed = admission.queue_capacity_reclaimed
            peak = coordinator.peak_active_connection_count
            handlers = coordinator.handler_thread_count
            last_delivery = admission.last_delivery_monotonic

        # This census is the fleet's only current-tip coverage scan: job
        # delivery used to repeat it per delivered job under the coordinator
        # lock, which made every reconnect herd O(N^2) inside the lock the herd
        # was already queued on (#159). Restored coverage resets the delivery
        # failure here, after the snapshot and outside the lock.
        if authorized == 0 or current / authorized >= 0.95:
            coordinator._ensure_observability_service().reset_delivery_failure()

        coordinator._ensure_job_cache_state()
        with coordinator._job_cache_lock:
            prepared_bundle = coordinator._prepared_ready_bundle
            prepared_snapshot = coordinator._prepared_ready_snapshot
            preparation_pending = bool(coordinator.job_preparation_pending)
            payout_generation = int(coordinator._payout_state_generation)
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
        rejection_counts = getattr(coordinator, "rejection_counts_by_reason", {})
        stale_unknown = int(
            rejection_counts.get(PRISM_REJECTION_STALE_JOB, 0)
        ) + int(rejection_counts.get(PRISM_REJECTION_UNKNOWN_JOB, 0))
        executor = getattr(coordinator, "_tip_refresh_executor", None)
        queue_depth, active_workers = (
            executor.stats() if executor is not None else (0, 0)
        )
        return MiningDeliveryInputs(
            active_connections=active,
            connection_capacity=connection_limit,
            peak_active_connections=peak,
            subscribed_connections=subscribed,
            authorized_connections=authorized,
            pending_initial_jobs=pending,
            pending_initial_job_capacity=pending_limit,
            oldest_pending_initial_job_age_seconds=oldest_age,
            oldest_genuinely_pending_initial_job_age_seconds=(
                oldest_genuine_initial_age
            ),
            clients_with_current_tip_jobs=current,
            clients_with_semantically_current_work=semantic_current,
            clients_with_no_active_job=clients_with_no_active_job,
            last_initial_job_delivery_monotonic=last_delivery,
            initial_job_timeout_seconds=timeout,
            initial_job_queue_rejections=queue_rejections,
            initial_job_timeout_disconnects=timeout_disconnects,
            initial_job_cancelled_tasks=cancelled,
            initial_job_coalesced_tasks=coalesced,
            initial_job_queue_capacity_reclaimed=queue_capacity_reclaimed,
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

    def record_startup_phase(self, phase: str) -> None:
        # The startup-phase recorder stays coordinator runtime state; owners
        # stamp through this dynamic seam (upstream #120, issue #188).
        self.coordinator._record_startup_phase_once(phase)

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
        coordinator: "PrismCoordinator",
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

    def cached_metrics_payload(self) -> MetricsSnapshotResponse:
        cached = getattr(self.coordinator, "cached_metrics_payload", None)
        if callable(cached):
            response = self._as_metrics_response(cached())
            if response.status != 503 or not self.allow_uncached_compatibility:
                # Carried through whole rather than unpacked: the freshness
                # state and age this read decided are what /metrics publishes
                # out of band (issue #184), and rebuilding them here from the
                # status alone would lose the fresh/stale distinction that
                # both now answer 200.
                return response
        if self.allow_uncached_compatibility:
            # Legacy make_audit_handler path only: no refresher owns the
            # snapshot, so this renders inline and the result is by definition
            # freshly collected.
            return MetricsSnapshotResponse(
                status=200,
                body=self.coordinator.metrics_payload(),
                state=METRICS_STATE_FRESH,
                age_seconds=0,
            )
        raise RuntimeError("cached metrics are unavailable")

    @staticmethod
    def _as_metrics_response(cached: object) -> MetricsSnapshotResponse:
        """Accept the response type, or adapt a legacy ``(status, body)`` pair.

        Coordinator doubles predating the #184 response type still answer with
        the bare pair. A pair carries no freshness beyond its status, so the
        state is derived from exactly that and nothing is invented.
        """

        if isinstance(cached, MetricsSnapshotResponse):
            return cached
        status, body = cached  # type: ignore[misc]
        return MetricsSnapshotResponse(
            status=int(status),
            body=str(body),
            state=(
                METRICS_STATE_UNAVAILABLE
                if int(status) == 503
                else METRICS_STATE_FRESH
            ),
            age_seconds=None if int(status) == 503 else 0,
        )

    def latest_evidence_payload(self) -> Mapping[str, object] | None:
        return self.coordinator.latest_evidence_payload()

    def owed_balances_payload(self) -> Mapping[str, object]:
        return self.coordinator.owed_balances_payload()

    def carry_forward_integrity_payload(self) -> Mapping[str, object]:
        return self.coordinator.carry_forward_integrity_payload()

    def miner_status_payload(self, recipient_id: str) -> Mapping[str, object]:
        return self.coordinator.miner_status_payload(recipient_id)

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


class PrismCoordinator:
    # Share-submission owner state routed through descriptors; the
    # ShareSubmissionService holds the single mutable copy once it exists
    # (see lab/prism/share_submission.py). Pre-service writes land in the
    # instance dict and are adopted at service construction.
    recent_share_keys = RecentShareCompatibilityField()
    block_solves_dropped_counts = BlockSolvesDroppedCompatibilityField()

    # Reorg reconciler owner state routed through descriptors; the
    # ReorgReconcilerService holds the single mutable copy once it exists
    # (see lab/prism/reorg_reconciler.py). Pre-service writes land in
    # backing slots and are adopted at service construction.
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

    # J1/template/compiler owner state routed through descriptors; the owner
    # services hold the single mutable copy (see lab/prism/job_bundle.py,
    # lab/prism/template_artifacts.py, and lab/prism/bundle_compiler.py).
    _job_cache_lock = _JobBundleStateField("_job_cache_lock")
    _active_job_bundle_builds = _JobBundleStateField("_active_job_bundle_builds")
    _job_build_lock = _JobBundleStateField("_job_build_lock")
    _job_build_scheduler_lock = _JobBundleStateField("_job_build_scheduler_lock")
    _job_build_priority_preparations = _JobBundleStateField(
        "_job_build_priority_preparations"
    )
    _job_build_priority_preparation_sequence = _JobBundleStateField(
        "_job_build_priority_preparation_sequence"
    )
    _job_build_routine_preparations = _JobBundleStateField(
        "_job_build_routine_preparations"
    )
    _job_build_routine_preparation_sequence = _JobBundleStateField(
        "_job_build_routine_preparation_sequence"
    )
    _job_build_priority_changed = _JobBundleStateField("_job_build_priority_changed")
    _job_build_executor = _JobBundleStateField("_job_build_executor")
    _job_build_executor_shutdown = _JobBundleStateField("_job_build_executor_shutdown")
    _job_build_active = _JobBundleStateField("_job_build_active")
    _job_build_retiring = _JobBundleStateField("_job_build_retiring")
    _job_build_pending = _JobBundleStateField("_job_build_pending")
    _job_build_issued_at_ms = _JobBundleStateField("_job_build_issued_at_ms")
    _job_bundle_cache = _JobBundleStateField("_job_bundle_cache")
    _job_build_phase_local = _JobBundleStateField("_job_build_phase_local")
    job_cache_hit_counts = _JobBundleStateField("job_cache_hit_counts")
    job_cache_miss_counts = _JobBundleStateField("job_cache_miss_counts")
    job_build_seconds_bucket_counts = _JobBundleStateField(
        "job_build_seconds_bucket_counts"
    )
    job_build_seconds_sum = _JobBundleStateField("job_build_seconds_sum")
    job_build_count = _JobBundleStateField("job_build_count")
    job_build_phase_seconds = _JobBundleStateField("job_build_phase_seconds")
    job_build_scheduler_counts = _JobBundleStateField("job_build_scheduler_counts")
    job_build_priority_counts = _JobBundleStateField("job_build_priority_counts")
    job_build_priority_admission_seconds = _JobBundleStateField(
        "job_build_priority_admission_seconds"
    )
    initial_job_prepared_work_counts = _JobBundleStateField(
        "initial_job_prepared_work_counts"
    )
    _admission_deadline_last_log_monotonic = _JobBundleStateField(
        "_admission_deadline_last_log_monotonic"
    )
    job_build_cancellation_seconds = _JobBundleStateField(
        "job_build_cancellation_seconds"
    )
    job_build_replacement_start_seconds = _JobBundleStateField(
        "job_build_replacement_start_seconds"
    )
    job_build_worker_counts = _JobBundleStateField("job_build_worker_counts")
    _job_build_worker_restart_pending = _JobBundleStateField(
        "_job_build_worker_restart_pending"
    )
    _share_window_serialization_lock = _JobBundleStateField(
        "_share_window_serialization_lock"
    )
    _share_window_serialization = _JobBundleStateField("_share_window_serialization")
    _bundle_preparation_lock = _JobBundleStateField("_bundle_preparation_lock")
    _bundle_preparation_flights = _JobBundleStateField("_bundle_preparation_flights")
    shared_bundle_build_counts = _JobBundleStateField("shared_bundle_build_counts")
    shared_bundle_preparation_seconds_sum = _JobBundleStateField(
        "shared_bundle_preparation_seconds_sum"
    )
    shared_bundle_preparation_count = _JobBundleStateField(
        "shared_bundle_preparation_count"
    )
    shared_bundle_preparation_waiters = _JobBundleStateField(
        "shared_bundle_preparation_waiters"
    )
    _prepared_ready_bundle = _JobBundleStateField("_prepared_ready_bundle")
    _prepared_ready_snapshot = _JobBundleStateField("_prepared_ready_snapshot")
    job_preparation_pending = _JobBundleStateField("job_preparation_pending")
    _template_artifacts = _TemplateArtifactStateField("_template_artifacts")
    _template_artifact_generation = _TemplateArtifactStateField(
        "_template_artifact_generation"
    )
    _serve_builder_lock = _BundleCompilerStateField("_serve_builder_lock")
    _serve_builder = _BundleCompilerStateField("_serve_builder")
    _serve_builder_shutdown = _BundleCompilerStateField("_serve_builder_shutdown")
    _serve_builder_metrics_lock = _BundleCompilerStateField(
        "_serve_builder_metrics_lock"
    )
    serve_builder_counts = _BundleCompilerStateField("serve_builder_counts")
    serve_builder_window_cache_counts = _BundleCompilerStateField(
        "serve_builder_window_cache_counts"
    )

    # R1 owner state routed through descriptors; TipRefreshService holds
    # the single mutable copy (see lab/prism/tip_refresh.py). The twelve
    # historical plain fields and the tip-detection epoch stay lazily
    # created on the owner, preserving legacy hasattr semantics.
    _tip_refresh_lock = _TipRefreshStateField("_tip_refresh_lock")
    _tip_refresh_singleflight_lock = _TipRefreshStateField(
        "_tip_refresh_singleflight_lock"
    )
    _tip_refresh_executor_lock = _TipRefreshStateField("_tip_refresh_executor_lock")
    _tip_refresh_executor = _TipRefreshStateField("_tip_refresh_executor")
    _tip_refresh_executor_shutdown = _TipRefreshStateField(
        "_tip_refresh_executor_shutdown"
    )
    _tip_refresh_metrics_lock = _TipRefreshStateField("_tip_refresh_metrics_lock")
    tip_refresh_histograms = _TipRefreshStateField("tip_refresh_histograms")
    tip_refresh_build_phase_histograms = _TipRefreshStateField(
        "tip_refresh_build_phase_histograms"
    )
    tip_refresh_client_counts = _TipRefreshStateField("tip_refresh_client_counts")
    tip_refresh_cancellation_counts = _TipRefreshStateField(
        "tip_refresh_cancellation_counts"
    )
    tip_refresh_wave_outcome_counts = _TipRefreshStateField(
        "tip_refresh_wave_outcome_counts"
    )
    tip_refresh_coverage_histograms = _TipRefreshStateField(
        "tip_refresh_coverage_histograms"
    )
    tip_refresh_inflight = _TipRefreshStateField("tip_refresh_inflight")
    tip_refresh_build_inflight = _TipRefreshStateField("tip_refresh_build_inflight")
    tip_refresh_build_queue_depth = _TipRefreshStateField(
        "tip_refresh_build_queue_depth"
    )
    tip_refresh_singleflight_hits = _TipRefreshStateField(
        "tip_refresh_singleflight_hits"
    )
    tip_refresh_superseded_results = _TipRefreshStateField(
        "tip_refresh_superseded_results"
    )
    tip_refresh_worker_failures = _TipRefreshStateField("tip_refresh_worker_failures")
    tip_refresh_worker_restarts = _TipRefreshStateField("tip_refresh_worker_restarts")
    tip_refresh_ipc_bytes = _TipRefreshStateField("tip_refresh_ipc_bytes")
    _tip_refresh_pending_event = _TipRefreshStateField("_tip_refresh_pending_event")
    _tip_refresh_pending_counter = _TipRefreshStateField("_tip_refresh_pending_counter")
    _tip_refresh_pending_token = _TipRefreshStateField("_tip_refresh_pending_token")
    _tip_refresh_retry = _TipRefreshStateField("_tip_refresh_retry")
    _tip_refresh_retry_counter = _TipRefreshStateField("_tip_refresh_retry_counter")
    _tip_refresh_retry_consumed = _TipRefreshStateField("_tip_refresh_retry_consumed")
    _tip_refresh_failure_holdoff_until = _TipRefreshStateField(
        "_tip_refresh_failure_holdoff_until"
    )
    _tip_refresh_failure_tip = _TipRefreshStateField("_tip_refresh_failure_tip")
    _active_tip_refresh = _TipRefreshStateField("_active_tip_refresh")
    _tip_refresh_epoch_sequence = _TipRefreshStateField("_tip_refresh_epoch_sequence")
    _tip_refresh_epoch_tip_hash = _TipRefreshStateField("_tip_refresh_epoch_tip_hash")
    _tip_refresh_epoch_payout_generation = _TipRefreshStateField(
        "_tip_refresh_epoch_payout_generation"
    )
    _tip_refresh_epoch_coverage = _TipRefreshStateField("_tip_refresh_epoch_coverage")
    _published_tip_refresh_epoch_identity = _TipRefreshStateField(
        "_published_tip_refresh_epoch_identity"
    )
    _retained_collection_refresh = _TipRefreshStateField("_retained_collection_refresh")
    current_tip_first_seen = _TipRefreshStateField("current_tip_first_seen")
    current_tip_parent = _TipRefreshStateField("current_tip_parent")
    current_tip_observation_sequence = _TipRefreshStateField(
        "current_tip_observation_sequence"
    )
    current_tip_observed_monotonic = _TipRefreshStateField(
        "current_tip_observed_monotonic"
    )
    tip_template_snapshot = _TipRefreshStateField("tip_template_snapshot")
    latest_detected_tip = _TipRefreshStateField("latest_detected_tip")
    tip_refresh_divergence_started_monotonic = _TipRefreshStateField(
        "tip_refresh_divergence_started_monotonic"
    )
    tip_observation_sequence = _TipRefreshStateField("tip_observation_sequence")
    last_successful_template_refresh_monotonic = _TipRefreshStateField(
        "last_successful_template_refresh_monotonic"
    )
    template_refresh_failure_started_monotonic = _TipRefreshStateField(
        "template_refresh_failure_started_monotonic"
    )
    tip_refresh_job_count = _TipRefreshStateField("tip_refresh_job_count")
    post_accept_refresh_failure_count = _TipRefreshStateField(
        "post_accept_refresh_failure_count"
    )
    tip_detection_epoch = _TipRefreshStateField("tip_detection_epoch")

    # S2 owner state routed through descriptors; JobDeliveryService holds
    # the single mutable copy (see lab/prism/job_delivery.py).
    pending_initial_jobs = _JobDeliveryStateField("pending_initial_jobs")
    initial_job_queue_rejection_count = _JobDeliveryStateField(
        "initial_job_queue_rejection_count"
    )
    initial_job_timeout_count = _JobDeliveryStateField("initial_job_timeout_count")
    initial_job_cancelled_count = _JobDeliveryStateField("initial_job_cancelled_count")
    initial_job_coalesced_count = _JobDeliveryStateField("initial_job_coalesced_count")
    initial_job_queue_capacity_reclaimed_count = _JobDeliveryStateField(
        "initial_job_queue_capacity_reclaimed_count"
    )
    initial_job_sent_count = _JobDeliveryStateField("initial_job_sent_count")
    initial_job_failed_count = _JobDeliveryStateField("initial_job_failed_count")
    initial_job_superseded_count = _JobDeliveryStateField(
        "initial_job_superseded_count"
    )
    initial_job_delivery_latency_seconds_sum = _JobDeliveryStateField(
        "initial_job_delivery_latency_seconds_sum"
    )
    initial_job_delivery_latency_count = _JobDeliveryStateField(
        "initial_job_delivery_latency_count"
    )
    last_initial_job_delivery_monotonic = _JobDeliveryStateField(
        "last_initial_job_delivery_monotonic"
    )
    _initial_job_executor_lock = _JobDeliveryStateField("_initial_job_executor_lock")
    _initial_job_executor = _JobDeliveryStateField("_initial_job_executor")
    _initial_job_executor_shutdown = _JobDeliveryStateField(
        "_initial_job_executor_shutdown"
    )
    jobs = _JobDeliveryStateField("jobs")
    job_counter = _JobDeliveryStateField("job_counter")
    evicted_job_graveyard = _JobDeliveryStateField("evicted_job_graveyard")
    evicted_jobs_by_connection = _JobDeliveryStateField("evicted_jobs_by_connection")
    evicted_same_tip_by_connection = _JobDeliveryStateField(
        "evicted_same_tip_by_connection"
    )
    evicted_same_tip_job_ids = _JobDeliveryStateField("evicted_same_tip_job_ids")
    evicted_job_index_tip_hash = _JobDeliveryStateField("evicted_job_index_tip_hash")
    evicted_job_next_prune_monotonic = _JobDeliveryStateField(
        "evicted_job_next_prune_monotonic"
    )
    evicted_job_expiration_counts = _JobDeliveryStateField(
        "evicted_job_expiration_counts"
    )
    evicted_job_capacity_eviction_counts = _JobDeliveryStateField(
        "evicted_job_capacity_eviction_counts"
    )
    evicted_job_submit_counts = _JobDeliveryStateField("evicted_job_submit_counts")
    _disconnected_evicted_job_ids = _JobDeliveryStateField(
        "_disconnected_evicted_job_ids"
    )

    # S1 owner state routed through descriptors; StratumSessionService (its
    # SessionRegistry and P2mrAddressValidator) holds the single mutable copy
    # (see lab/prism/stratum_session.py).
    clients = _StratumSessionStateField("clients")
    connection_counter = _StratumSessionStateField("connection_counter")
    connection_limit_rejection_counts = _StratumSessionStateField(
        "connection_limit_rejection_counts"
    )
    peak_active_connection_count = _StratumSessionStateField(
        "peak_active_connection_count"
    )
    handler_thread_count = _StratumSessionStateField("handler_thread_count")
    accept_resource_exhaustion_count = _StratumSessionStateField(
        "accept_resource_exhaustion_count"
    )
    connection_setup_failure_count = _StratumSessionStateField(
        "connection_setup_failure_count"
    )
    _p2mr_address_cache_lock = _StratumSessionStateField("_p2mr_address_cache_lock")
    _p2mr_address_cache = _StratumSessionStateField("_p2mr_address_cache")
    _p2mr_address_validation_inflight = _StratumSessionStateField(
        "_p2mr_address_validation_inflight"
    )

    # S3 owner state routed through descriptors; ShareWriter holds the single
    # mutable copy (see lab/prism/share_writer.py).
    share_append_queue = ShareWriterCompatibilityField("share_append_queue")
    share_commit_batch_size = ShareWriterCompatibilityField("share_commit_batch_size")
    share_commit_linger_seconds = ShareWriterCompatibilityField(
        "share_commit_linger_seconds"
    )
    share_commit_timeout_seconds = ShareWriterCompatibilityField(
        "share_commit_timeout_seconds"
    )
    share_writer_active = ShareWriterCompatibilityField("share_writer_active")
    share_append_failure_count = ShareWriterCompatibilityField(
        "share_append_failure_count"
    )
    share_recovery_path = ShareWriterCompatibilityField("share_recovery_path")
    share_recovery_lock = ShareWriterCompatibilityField("share_recovery_lock")
    shares_recovered_to_disk = ShareWriterCompatibilityField(
        "shares_recovered_to_disk"
    )
    shares_replayed = ShareWriterCompatibilityField("shares_replayed")
    share_replay_conflicts = ShareWriterCompatibilityField(
        "share_replay_conflicts"
    )
    _pending_share_commit_lock = ShareWriterCompatibilityField(
        "_pending_share_commit_lock"
    )
    _pending_share_commit_floor = ShareWriterCompatibilityField(
        "_pending_share_commit_floor"
    )

    # P1 owner state routed through descriptors; PayoutStateService holds
    # the single mutable copy (see lab/prism/payout_state.py).
    _payout_state_generation = _PayoutStateStateField(
        "_payout_state_generation"
    )
    _payout_state_prepare_lock = _PayoutStateStateField(
        "_payout_state_prepare_lock"
    )
    _payout_state_source = _PayoutStateStateField("_payout_state_source")
    _published_payout_state = _PayoutStateStateField("_published_payout_state")
    _payout_ledger_artifact = _PayoutStateStateField("_payout_ledger_artifact")
    _payout_ledger_artifact_generation = _PayoutStateStateField(
        "_payout_ledger_artifact_generation"
    )
    _payout_ledger_append_invalidation_epoch = _PayoutStateStateField(
        "_payout_ledger_append_invalidation_epoch"
    )
    _payout_append_landing_fence_lock = _PayoutStateStateField(
        "_payout_append_landing_fence_lock"
    )
    _payout_window_inflight_scan_anchors = _PayoutStateStateField(
        "_payout_window_inflight_scan_anchors"
    )
    _payout_window_inflight_scan_anchor_token = _PayoutStateStateField(
        "_payout_window_inflight_scan_anchor_token"
    )
    _payout_unfenced_append_inflight_stamps = _PayoutStateStateField(
        "_payout_unfenced_append_inflight_stamps"
    )
    _payout_unfenced_append_inflight_token = _PayoutStateStateField(
        "_payout_unfenced_append_inflight_token"
    )
    _payout_unfenced_append_drained = _PayoutStateStateField(
        "_payout_unfenced_append_drained"
    )
    _payout_published_job_window_anchor_ms = _PayoutStateStateField(
        "_payout_published_job_window_anchor_ms"
    )
    _incremental_payout_artifact_window = _PayoutStateStateField(
        "_incremental_payout_artifact_window"
    )
    _incremental_payout_artifact_window_invalidation_reason = _PayoutStateStateField(
        "_incremental_payout_artifact_window_invalidation_reason"
    )
    _payout_prior_balances_reuse_invalidated_sha256 = _PayoutStateStateField(
        "_payout_prior_balances_reuse_invalidated_sha256"
    )
    _payout_artifact_executor_lock = _PayoutStateStateField(
        "_payout_artifact_executor_lock"
    )
    _payout_artifact_executor = _PayoutStateStateField(
        "_payout_artifact_executor"
    )
    _payout_artifact_future = _PayoutStateStateField("_payout_artifact_future")
    _payout_artifact_requested = _PayoutStateStateField(
        "_payout_artifact_requested"
    )
    _payout_artifact_requested_bypass = _PayoutStateStateField(
        "_payout_artifact_requested_bypass"
    )
    _payout_artifact_executor_shutdown = _PayoutStateStateField(
        "_payout_artifact_executor_shutdown"
    )
    _payout_artifact_last_schedule_monotonic = _PayoutStateStateField(
        "_payout_artifact_last_schedule_monotonic"
    )
    _payout_artifact_rearm_backoff = _PayoutStateStateField(
        "_payout_artifact_rearm_backoff"
    )
    _payout_state_delivery_gate = _PayoutStateStateField(
        "_payout_state_delivery_gate"
    )
    _payout_balance_mutation_lock = _PayoutStateStateField(
        "_payout_balance_mutation_lock"
    )
    _accepted_block_payout_preview_condition = _PayoutStateStateField(
        "_accepted_block_payout_preview_condition"
    )
    _accepted_block_payout_previews = _PayoutStateStateField(
        "_accepted_block_payout_previews"
    )
    _invalidated_accepted_block_payout_previews = _PayoutStateStateField(
        "_invalidated_accepted_block_payout_previews"
    )
    _payout_state_metrics_lock = _PayoutStateStateField(
        "_payout_state_metrics_lock"
    )
    payout_state_histograms = _PayoutStateStateField("payout_state_histograms")
    payout_gate_wait_histograms = _PayoutStateStateField(
        "payout_gate_wait_histograms"
    )
    payout_state_candidates_discarded = _PayoutStateStateField(
        "payout_state_candidates_discarded"
    )
    _payout_first_delivery_pending = _PayoutStateStateField(
        "_payout_first_delivery_pending"
    )
    _payout_state_publication_blocked = _PayoutStateStateField(
        "_payout_state_publication_blocked"
    )
    payout_artifact_event_counts = _PayoutStateStateField(
        "payout_artifact_event_counts"
    )

    _progress_current_template_generation = _ProgressHealthStateField(
        "_current_template_generation"
    )
    _progress_current_template_fingerprint = _ProgressHealthStateField(
        "_current_template_fingerprint"
    )
    _progress_current_payout_generation = _ProgressHealthStateField(
        "_current_payout_generation"
    )
    _progress_published_template_generation = _ProgressHealthStateField(
        "_published_template_generation"
    )
    _progress_published_template_fingerprint = _ProgressHealthStateField(
        "_published_template_fingerprint"
    )
    _progress_published_payout_generation = _ProgressHealthStateField(
        "_published_payout_generation"
    )
    _progress_has_published_work = _ProgressHealthStateField("_has_published_work")
    _progress_last_tip_poll_monotonic = _ProgressHealthStateField(
        "_last_tip_poll_monotonic"
    )
    _progress_last_delivery_template_generation = _ProgressHealthStateField(
        "_last_delivery_template_generation"
    )
    _progress_last_delivery_template_fingerprint = _ProgressHealthStateField(
        "_last_delivery_template_fingerprint"
    )
    _progress_last_delivery_payout_generation = _ProgressHealthStateField(
        "_last_delivery_payout_generation"
    )
    _progress_last_delivery_monotonic = _ProgressHealthStateField(
        "_last_delivery_monotonic"
    )
    _progress_pending_since_monotonic = _ProgressHealthStateField(
        "_pending_since_monotonic"
    )
    _progress_publication_divergence_since_monotonic = _ProgressHealthStateField(
        "_publication_divergence_since_monotonic"
    )
    _progress_refresh_signal_pending = _ProgressHealthStateField(
        "_refresh_signal_pending"
    )
    _progress_active_refresh_count = _ProgressHealthStateField(
        "_active_refresh_count"
    )
    _progress_last_refresh_activity_monotonic = _ProgressHealthStateField(
        "_last_refresh_activity_monotonic"
    )
    _progress_bundle_build_counter = _ProgressHealthStateField(
        "_bundle_build_counter"
    )

    @property
    def _progress_health_lock(self) -> threading.Lock:
        return self._ensure_progress_health_service()._lock

    @property
    def _progress_bundle_builds(self) -> dict[int, float]:
        return self._ensure_progress_health_service()._bundle_builds

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
    # The #113-era block-candidate fields route to the B1 owner one-to-one
    # (the pre-#113 backoff quartet keeps its historical service spelling).
    _block_replay_candidate_queue = BlockCandidateStateField(
        "_block_replay_candidate_queue"
    )
    _block_replay_inflight_hashes = BlockCandidateStateField(
        "_block_replay_inflight_hashes"
    )
    _block_quarantine_queue = BlockCandidateStateField("_block_quarantine_queue")
    _block_quarantine_hashes = BlockCandidateStateField("_block_quarantine_hashes")
    _block_candidate_disposition_registry_lock = BlockCandidateStateField(
        "_block_candidate_disposition_registry_lock"
    )
    _block_candidate_disposition_flights = BlockCandidateStateField(
        "_block_candidate_disposition_flights"
    )
    _block_candidate_terminal_outcomes = BlockCandidateStateField(
        "_block_candidate_terminal_outcomes"
    )
    _block_fast_lane_reservations = BlockCandidateStateField(
        "_block_fast_lane_reservations"
    )
    _block_disposition_waiting_retries = BlockCandidateStateField(
        "_block_disposition_waiting_retries"
    )
    _block_candidate_dequeued_hashes = BlockCandidateStateField(
        "_block_candidate_dequeued_hashes"
    )
    _block_accounting_state_lock = BlockCandidateStateField(
        "_block_accounting_state_lock"
    )
    _block_accounting_queue = BlockCandidateStateField("_block_accounting_queue")
    _block_accounting_overflow_queue = BlockCandidateStateField(
        "_block_accounting_overflow_queue"
    )
    _block_accounting_sequence = BlockCandidateStateField(
        "_block_accounting_sequence"
    )
    _block_accounting_thread = BlockCandidateStateField("_block_accounting_thread")
    _block_accounting_thread_ident = BlockCandidateStateField(
        "_block_accounting_thread_ident"
    )
    _block_accounting_holds_disposition = BlockCandidateStateField(
        "_block_accounting_holds_disposition"
    )
    _block_accounting_deferred_retry_candidate = BlockCandidateStateField(
        "_block_accounting_deferred_retry_candidate"
    )
    _block_accounting_phase = BlockCandidateStateField("_block_accounting_phase")
    _block_submitter_thread_ident = BlockCandidateStateField(
        "_block_submitter_thread_ident"
    )
    _block_submitter_phase = BlockCandidateStateField("_block_submitter_phase")
    _block_submitter_last_lock_wait_log_monotonic = BlockCandidateStateField(
        "_block_submitter_last_lock_wait_log_monotonic"
    )
    _block_submitter_retry_state_lock = BlockCandidateStateField(
        "_block_submitter_retry_state_lock", "_state_lock"
    )
    _block_submitter_backoff_started_monotonic = BlockCandidateStateField(
        "_block_submitter_backoff_started_monotonic", "_backoff_started_monotonic"
    )
    _block_submitter_backoff_deadline_monotonic = BlockCandidateStateField(
        "_block_submitter_backoff_deadline_monotonic", "_backoff_deadline_monotonic"
    )
    _block_submitter_backoff_delay_seconds = BlockCandidateStateField(
        "_block_submitter_backoff_delay_seconds", "_backoff_delay_seconds"
    )
    _block_submitter_ledger_calls_lock = BlockCandidateStateField(
        "_block_submitter_ledger_calls_lock"
    )
    _block_submitter_ledger_calls = BlockCandidateStateField(
        "_block_submitter_ledger_calls"
    )
    _block_submitter_ledger_worker_slots = BlockCandidateStateField(
        "_block_submitter_ledger_worker_slots"
    )
    _block_submitter_rpc_calls_lock = BlockCandidateStateField(
        "_block_submitter_rpc_calls_lock"
    )
    _block_submitter_rpc_calls = BlockCandidateStateField(
        "_block_submitter_rpc_calls"
    )
    _block_submitter_rpc_worker_slots = BlockCandidateStateField(
        "_block_submitter_rpc_worker_slots"
    )
    _block_landing_timeout_counts = BlockCandidateStateField(
        "_block_landing_timeout_counts"
    )
    _block_ledger_call_metrics_lock = BlockCandidateStateField(
        "_block_ledger_call_metrics_lock"
    )
    _block_ledger_call_metrics = BlockCandidateStateField(
        "_block_ledger_call_metrics"
    )
    _block_candidate_retry_not_before = BlockCandidateStateField(
        "_block_candidate_retry_not_before"
    )
    _block_candidate_retained_node_submissions = BlockCandidateStateField(
        "_block_candidate_retained_node_submissions"
    )
    _block_candidate_retained_submission_monotonic = BlockCandidateStateField(
        "_block_candidate_retained_submission_monotonic"
    )
    _counted_block_candidate_abandonments = BlockCandidateStateField(
        "_counted_block_candidate_abandonments"
    )
    _outstanding_block_candidate_hashes = BlockCandidateStateField(
        "_outstanding_block_candidate_hashes"
    )
    _tip_observed_accepted_block_hashes = BlockCandidateStateField(
        "_tip_observed_accepted_block_hashes"
    )
    block_candidate_accept_pending_defer_count = BlockCandidateStateField(
        "block_candidate_accept_pending_defer_count"
    )
    stale_job_abandon_counts = BlockCandidateStateField("stale_job_abandon_counts")
    _block_submit_metrics_lock = BlockCandidateStateField(
        "_block_submit_metrics_lock"
    )
    block_submit_seconds_histogram = BlockCandidateStateField(
        "block_submit_seconds_histogram"
    )
    _accepted_block_preview_publication_lock = BlockCandidateStateField(
        "_accepted_block_preview_publication_lock"
    )
    accepted_block_preview_publication_seconds_histogram = (
        BlockCandidateStateField(
            "accepted_block_preview_publication_seconds_histogram"
        )
    )
    _accepted_block_preview_acceptance_monotonic = BlockCandidateStateField(
        "_accepted_block_preview_acceptance_monotonic"
    )
    _block_replay_enumeration_owed_flag = BlockCandidateStateField(
        "_block_replay_enumeration_owed_flag"
    )

    # Vardiff owner state routed through descriptors; VardiffService holds
    # the single mutable copy (see lab/prism/vardiff_service.py). Before the
    # lazy service exists, reads/writes land in the instance dict and are
    # adopted at single-flight construction, preserving embedders, bare
    # __new__ test instances, and concurrent first use.
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
                    runtime=self,
                    ledger=lambda: self.ledger,
                    stop_event=lambda: self.stop_event,
                    writer_operation=lambda component: self._writer_operation(component),
                    submit_candidate=(
                        lambda candidate, *, node_submission=None, disposition_held=False: (
                            self._land_block_candidate_submission(
                                candidate,
                                node_submission=node_submission,
                                disposition_held=disposition_held,
                            )
                        )
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
                submit_seconds_buckets=PRISM_JOB_BUILD_SECONDS_BUCKETS,
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
                service.retry_delays,
            )
            service.finalize_retries = self.__dict__.get(
                "_block_candidate_finalize_retries",
                service.finalize_retries,
            )
            service.abandoned_counts = self.__dict__.get(
                "block_candidate_abandoned_counts",
                service.abandoned_counts,
            )
            service.retry_candidate = self.__dict__.get(
                "_retry_block_candidate"
            )
            service.outcome = self.__dict__.get(
                "_block_candidate_outcome",
                service.outcome,
            )
            # Adopt any remaining pre-service legacy values (direct __dict__
            # installs by focused embedders) before removing them; ordinary
            # attribute writes already route through the class descriptors.
            for legacy_name, service_attribute in (
                _BLOCK_CANDIDATE_STATE_FIELD_MAP.items()
            ):
                if legacy_name in self.__dict__:
                    setattr(service, service_attribute, self.__dict__[legacy_name])
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
                *_BLOCK_CANDIDATE_STATE_FIELD_MAP,
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
            # Adopt pre-service legacy fields (production __init__ writes and
            # direct __dict__ installs by focused embedders) before removing
            # them; ordinary attribute access already routes through the
            # class descriptors under this same lock.
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

    # The five PR 79 health compatibility properties were removed by this
    # layer; health snapshot state is read and poked only through the
    # observability service's state() and narrow *_for_test accessors.

    def __init__(self, config: CoordinatorConfig | None = None) -> None:
        self.config = load_coordinator_config() if config is None else config
        rpc_config = self.config.rpc
        stratum_config = self.config.stratum
        job_config = self.config.jobs
        block_config = self.config.block
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
        # Push-style tip detection rides waitfornewblock; the poll loop above
        # stays as the fallback and still covers same-tip template refreshes.
        self.blockwait_enabled = job_config.blockwait_enabled
        self.blockwait_timeout_seconds = job_config.blockwait_timeout_seconds
        self.block_submit_rpc_timeout_seconds = block_config.submit_rpc_timeout_seconds
        self.block_submit_db_timeout_seconds = block_config.submit_db_timeout_seconds
        self.block_landing_db_timeout_seconds = block_config.landing_db_timeout_seconds
        self.block_landing_db_timeout_max_seconds = (
            block_config.landing_db_timeout_max_seconds
        )
        self.accepted_parent_unresolved_depth_max = (
            block_config.accepted_parent_unresolved_depth_max
        )
        self.block_submit_lock_wait_log_seconds = (
            block_config.submit_lock_wait_log_seconds
        )
        self.block_submit_stuck_call_exit_seconds = (
            block_config.submit_stuck_call_exit_seconds
        )
        # An own-hash tip observation is acceptance evidence even when the
        # direct submitblock ack was lost; this window bounds how long that
        # evidence blocks a terminal abandonment while fresh chain probes
        # cannot prove the candidate active.
        self.observed_tip_accept_window_seconds = (
            block_config.observed_tip_accept_window_seconds
        )
        # After a FAILED refresh pass, the fallback poller re-attempts no
        # sooner than this many seconds (plus jitter) while the tip the failed
        # pass worked against is still current, so a persistent blockage or
        # sustained payout churn costs ~1 attempt/second instead of one per
        # 0.25s trigger slice. A successful pass or a newly observed tip
        # re-arms immediately. Zero restores unspaced retries.
        self.tip_refresh_failure_holdoff_seconds = (
            job_config.tip_refresh_failure_holdoff_seconds
        )
        # Zero disables stale-grace crediting (every prior-tip share rejects,
        # the pre-grace behavior).
        self.stale_grace_seconds = stratum_config.stale_grace_seconds
        # Per-share/per-job stdout logging is debug-only: at production share
        # rates each print is a journald flush on the Stratum hot path.
        self.hot_path_log_enabled = self.config.hot_path_log_enabled
        # Zero disables the observed-tip reuse (every submit re-reads the tip
        # over RPC, the legacy behavior).
        self.submit_tip_max_age_seconds = job_config.submit_tip_max_age_seconds
        self.same_tip_job_retention_seconds = (
            stratum_config.same_tip_job_retention_seconds
        )
        self.same_tip_job_retention_per_connection = (
            stratum_config.same_tip_job_retention_per_connection
        )
        self.disconnected_job_retention = stratum_config.disconnected_job_retention
        self.tip_refresh_max_workers = job_config.tip_refresh_max_workers
        self.tip_refresh_epoch_fanout = job_config.tip_refresh_epoch_fanout
        self.job_build_timeout_seconds = job_config.job_build_timeout_seconds
        self.job_build_cancel_grace_seconds = job_config.job_build_cancel_grace_seconds
        self.vardiff_idle_sweep_seconds = stratum_config.vardiff_idle_sweep_seconds
        # Reconnect difficulty retention: a reconnecting worker resumes at
        # its last converged difficulty instead of the lane start. Off
        # restores lane-start behavior exactly.
        self.vardiff_resume_enabled = stratum_config.vardiff_resume_enabled
        self.vardiff_resume_ttl_seconds = stratum_config.vardiff_resume_ttl_seconds
        self.vardiff_resume_max_entries = stratum_config.vardiff_resume_max_entries
        self.vardiff_resume_max_start_factor = (
            stratum_config.vardiff_resume_max_start_factor
        )
        # Zero collapses every worker into the overflow label (per-worker
        # metrics effectively off) without touching the aggregate counters.
        self.worker_metrics_limit = job_config.worker_metrics_limit
        self.reorg_reconciler_enabled = job_config.reorg_reconciler_enabled
        # Per-template job caching. A zero disables the corresponding cache
        # (every build redoes that stage), which is also the legacy behavior.
        self.job_bundle_cache_seconds = job_config.job_bundle_cache_seconds
        self.bundle_build_timeout_seconds = job_config.bundle_build_timeout_seconds
        self.payout_artifact_rearm_min_seconds = (
            job_config.payout_artifact_rearm_min_seconds
        )
        # Master kill-switch for the payout-artifact reuse line. Off
        # restores the pre-reuse behavior (every ready build pays the
        # synchronous reward-window walk and no background walks run), so a
        # production regression is an env flip instead of a rollback.
        self.payout_artifact_reuse_enabled = job_config.payout_artifact_reuse_enabled
        self.payout_artifact_reanchor_seconds = (
            job_config.payout_artifact_reanchor_seconds
        )
        self.payout_artifact_min_build_interval_seconds = (
            job_config.payout_artifact_min_build_interval_seconds
        )
        self.payout_artifact_full_rescan_seconds = (
            job_config.payout_artifact_full_rescan_seconds
        )
        self.payout_artifact_max_anchor_age_seconds = (
            job_config.payout_artifact_max_anchor_age_seconds
        )
        self.routine_admission_deadline_seconds = (
            job_config.routine_admission_deadline_seconds
        )
        self.template_cache_seconds = job_config.template_cache_seconds
        self.template_refresh_failure_exit_seconds = (
            job_config.template_refresh_failure_exit_seconds
        )
        self.coordination_blocked_exit_seconds = (
            lifecycle_config.coordination_blocked_exit_seconds
        )
        self.last_successful_template_refresh_monotonic: float | None = None
        self.template_refresh_failure_started_monotonic: float | None = None
        # Coordination-blocked streak state moved to the WatchdogService
        # owner (see lab/prism/background_services.py).
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
        # Zero remains an explicit unlimited option for focused local/regtest
        # use. Deployments default to a conservative ceiling above PRISM's
        # normal 200-250 connection population.
        self.stratum_max_connections = stratum_config.max_connections
        self.stratum_max_connections_per_username = (
            stratum_config.max_connections_per_username
        )
        self.stratum_max_pending_initial_jobs = stratum_config.max_pending_initial_jobs
        if config is None:
            # The worker knobs stay visibly env-wired in this module for the
            # zero-argument production path (an upstream lost-fix detector
            # asserts the source); the values are identical to what the
            # snapshot loader just validated. A supplied snapshot never
            # rereads the environment.
            self.initial_job_max_workers = resolve_initial_job_max_workers(
                env_optional_positive_int("PRISM_INITIAL_JOB_MAX_WORKERS"),
                self.stratum_max_pending_initial_jobs,
            )
            self.job_build_executor_workers = validate_job_build_executor_workers(
                env_positive_int(
                    "PRISM_JOB_BUILD_EXECUTOR_WORKERS",
                    DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS,
                )
            )
        else:
            self.initial_job_max_workers = stratum_config.initial_job_max_workers
            self.job_build_executor_workers = job_config.job_build_executor_workers
        self.stratum_initial_job_timeout_seconds = (
            stratum_config.initial_job_timeout_seconds
        )
        self.mining_health_startup_grace_seconds = (
            lifecycle_config.mining_health_startup_grace_seconds
        )
        self.stratum_accept_resource_exhaustion_backoff_seconds = (
            stratum_config.accept_resource_exhaustion_backoff_seconds
        )
        self.stratum_listen_backlog = stratum_config.listen_backlog
        self.stratum_bind_retry_seconds = stratum_config.bind_retry_seconds
        self.payout_address_cache_max_entries = (
            stratum_config.payout_address_cache_max_entries
        )
        self.payout_address_cache_ttl_seconds = (
            stratum_config.payout_address_cache_ttl_seconds
        )
        self.coinbase_tag_hex = self.config.coinbase_tag_hex
        self.share_difficulty = stratum_config.share_difficulty
        self.vardiff_config = stratum_config.vardiff_config
        self.listener_profiles = list(stratum_config.listener_profiles)
        self.default_share_weight = stratum_config.default_share_weight
        self.share_weights_by_username = dict(stratum_config.share_weights_by_username)
        self.username_fallback_address = stratum_config.username_fallback_address
        self.min_ready_miners = job_config.min_ready_miners
        self.signing_seed_hex = ledger_config.signing_seed_hex
        self.ledger_attestation_signing_seed_hex = (
            ledger_config.attestation_signing_seed_hex
        )
        self.ledger_writer_public_key_hex = ledger_config.writer_public_key_hex
        self.evidence_path = audit_config.evidence_path
        self.audit_dir = audit_config.directory
        self.audit_share_segment_size = audit_config.share_segment_size
        self.audit_live_bundle_retention = audit_config.live_bundle_retention
        self.audit_candidate_retention_seconds = (
            audit_config.candidate_retention_seconds
        )
        self.ctv_broadcast_attempt_detail_limit = (
            ctv_config.broadcast_attempt_detail_limit
        )
        self.ctv_broadcast_retry_backoff_seconds = (
            ctv_config.broadcast_retry_backoff_seconds
        )
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
        self.lock = _ObservedRLock(
            wait_observer=self._observe_coordinator_lock_wait,
        )
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
        self.initial_job_queue_capacity_reclaimed_count = 0
        self.last_initial_job_delivery_monotonic: float | None = None
        self._p2mr_address_cache_lock = threading.Lock()
        self._p2mr_address_cache: OrderedDict[
            str, tuple[float, tuple[str, str]]
        ] = OrderedDict()
        self._p2mr_address_validation_inflight: dict[
            str, _P2mrAddressValidationFlight
        ] = {}
        self.jobs: dict[str, PrismJobContext] = {}
        # Share-path ownership is deliberately disjoint from the coordinator
        # control-plane lock. Deduplication remains process-wide so an exact
        # header replay across reauthorization/connections is still rejected;
        # the submission owner adopts this seed on first hot-path touch.
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
        self.block_solves_dropped_counts = {"stale_grace": 0}
        self._pool_ready_latched = False
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
        # Published share-validation authority. Detection is kept separately
        # in latest_detected_tip so a waiter can cancel obsolete refresh work
        # without invalidating jobs before their replacement is ready.
        # The flip stamp is None for the startup baseline, which never opens
        # the stale-grace window.
        self.current_tip_first_seen: tuple[str, float | None] | None = None
        self.current_tip_parent: tuple[str, str] | None = None
        self.latest_detected_tip: tuple[str, int] | None = None
        # Start of the current detected-vs-published divergence epoch. Unlike
        # latest_detected_tip, this stamp is not renewed by B -> C -> D while
        # published work is still on A; submit authority therefore has a
        # bounded lease even during repeated refresh failures.
        self.tip_refresh_divergence_started_monotonic: float | None = None
        # When the refresh path last published or reconfirmed the authoritative
        # tip. Bounds normal submit reuse when no replacement is actively
        # pending (see submit_stale_check_tip).
        self.current_tip_observed_monotonic: float | None = None
        # Block candidates are landed by a dedicated submitter thread so a
        # winning share's ack (and every other client's) never waits on
        # audit/persist/submitblock; see enqueue_block_candidate.
        self.block_candidate_queue: queue.Queue[PrismBlockCandidate] = queue.Queue(
            maxsize=MAX_PENDING_BLOCK_CANDIDATES
        )
        # Durable recovery work is separate and lower priority: a newly found
        # live block must never sit behind a restart batch. The in-flight set
        # lets batch replay expose later rows while an older row is still in
        # accounting without repeatedly queueing the older hash.
        self._block_replay_candidate_queue: queue.Queue[
            PrismBlockCandidate
        ] = queue.Queue()
        self._block_replay_inflight_hashes: set[str] = set()
        self._block_quarantine_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self._block_quarantine_hashes: set[str] = set()
        self._block_candidate_disposition_registry_lock = threading.Lock()
        self._block_candidate_disposition_flights: dict[
            str, _BlockCandidateDispositionFlight
        ] = {}
        self._block_candidate_terminal_outcomes: dict[str, bool] = {}
        self._block_fast_lane_reservations: set[str] = set()
        self._block_disposition_waiting_retries: dict[
            str, PrismBlockCandidate
        ] = {}
        self._block_accounting_queue: queue.PriorityQueue[
            tuple[int, int, _BlockCandidateAccountingTask]
        ] = queue.PriorityQueue()
        self._block_accounting_overflow_queue: queue.PriorityQueue[
            tuple[int, int, _BlockCandidateAccountingTask]
        ] = queue.PriorityQueue()
        self._block_accounting_sequence = 0
        self._block_accounting_state_lock = threading.Lock()
        self._block_accounting_thread: threading.Thread | None = None
        self.block_candidates_dropped = 0
        self.block_candidate_wakeups_coalesced = 0
        self.block_candidate_retry_count = 0
        self.block_candidate_poisoned_count = 0
        self.block_candidate_accept_pending_defer_count = 0
        # Hashes of block candidates this process may still land (durable
        # outbox pending, queued, retained for retry, or mid-disposition).
        # Membership lets every tip-observation channel recognize the pool's
        # own block the moment it becomes the chain tip.
        self._outstanding_block_candidate_hashes: set[str] = set()
        # Outstanding candidate hashes observed as the chain tip
        # (hash -> monotonic stamp). qbitd only reports a candidate hash as
        # its tip after accepting that block, so an entry here is acceptance
        # evidence that outlives transient fork views and lost submitblock
        # acks; disposition/abandon paths consult it before treating any
        # instantaneous chain probe as terminal truth.
        self._tip_observed_accepted_block_hashes: dict[str, float] = {}
        self.block_candidate_retry_initial_seconds = (
            DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS
        )
        self.block_candidate_retry_max_seconds = DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS
        self.block_candidate_retry_delays: dict[str, float] = {}
        self._block_submitter_retry_state_lock = threading.Lock()
        self._block_submitter_backoff_started_monotonic: float | None = None
        self._block_submitter_backoff_deadline_monotonic: float | None = None
        self._block_submitter_backoff_delay_seconds = 0.0
        self._block_submitter_ledger_calls_lock = threading.Lock()
        self._block_submitter_ledger_calls: dict[
            tuple[object, ...], _BlockSubmitterLedgerCall
        ] = {}
        self._block_submitter_ledger_worker_slots = threading.BoundedSemaphore(
            MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS
        )
        self._block_submitter_rpc_calls_lock = threading.Lock()
        self._block_submitter_rpc_calls: dict[str, _BlockSubmitterRpcCall] = {}
        self._block_submitter_rpc_worker_slots = threading.BoundedSemaphore(
            MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS
        )
        self._fatal_exit_requested = False
        self._block_submitter_last_lock_wait_log_monotonic = 0.0
        # Terminal candidates whose durable outbox update failed; replays for
        # these run finalize-only (see _finalize_block_candidate).
        self._block_candidate_finalize_retries: dict[str, tuple[bool, str]] = {}
        # A block candidate that loses its tip race (or fails to submit) is a
        # BLOCK-path event, not a share rejection: under the async model the
        # share was already accepted and credited, so it must not touch the
        # share-reject counters (that would inflate stale_share_percent with
        # block-race losses). Tracked here by reason instead.
        self.block_candidate_abandoned_counts: dict[str, int] = {}
        # Durable cleanup can fail after a terminal decision and force the
        # same hash through that decision again; abandonment metrics count
        # candidates, not cleanup attempts.
        self._counted_block_candidate_abandonments: set[str] = set()
        self.stale_job_abandon_counts = {
            abandon_class: 0
            for abandon_class in PRISM_STALE_JOB_ABANDON_CLASSES
        }
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
        self.share_replay_conflicts = 0
        self.job_build_failure_count = 0
        self.tip_refresh_job_count = 0
        self.post_accept_refresh_failure_count = 0
        self.reorg_inactive_block_count = 0
        self.reorg_reactivated_block_count = 0
        self.reorg_reconcile_skip_count = 0
        self.reorg_reconcile_error_count = 0
        self.matured_payout_count = 0
        # The full accepted-block bundle is durable in the audit store.  Keeping
        # it here only to derive one metric pinned the complete share window for
        # the lifetime of the coordinator.
        self.latest_coinbase_size_bytes: int | None = None
        self.tip_template_snapshot: QbitTipTemplateSnapshot | None = None
        self._tip_refresh_lock = threading.Lock()
        self._tip_refresh_singleflight_lock = threading.Lock()
        self._tip_refresh_executor_lock = threading.Lock()
        self._tip_refresh_executor: _BoundedPriorityExecutor | None = None
        self._tip_refresh_executor_shutdown = False
        self._initial_job_executor_lock = threading.Lock()
        self._initial_job_executor: _BoundedPriorityExecutor | None = None
        self._initial_job_executor_shutdown = False
        self._tip_refresh_pending_event = threading.Event()
        self._tip_refresh_pending_counter = 0
        self._tip_refresh_pending_token: int | None = None
        self._tip_refresh_retry = threading.Event()
        self._tip_refresh_retry_counter = 0
        self._tip_refresh_retry_consumed = 0
        self._tip_refresh_failure_holdoff_until: float | None = None
        self._tip_refresh_failure_tip: str | None = None
        self._active_tip_refresh: tuple[
            TipRefreshValidationToken,
            _FanoutCancellation,
        ] | None = None
        self._retained_collection_refresh: RetainedCollectionRefresh | None = None
        self.last_reorg_reconciled_tip_hash: str | None = None
        self.last_reorg_reconciled_trusted = False
        self.last_reorg_reconciled_monotonic: float | None = None
        self._prism_payout_policy_cache: dict[str, object] | None = None
        self._ensure_job_cache_state()
        # Eager reorg owner root: the reconciler service adopts the compat
        # seed values assigned above and owns every flight/memo/lookup/
        # prefetch container from here on.
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
        self._heartbeat_phases: dict[str, str] = {}
        self._watchdog_pauses: dict[str, int] = {}
        self._heartbeats_lock = threading.Lock()
        self.watchdog_enabled = lifecycle_config.watchdog_enabled
        self.watchdog_timeout_seconds = lifecycle_config.watchdog_timeout_seconds
        self.watchdog_interval_seconds = lifecycle_config.watchdog_interval_seconds
        # Lease/watchdog timing envelopes are projected from the immutable
        # snapshot; services read the live attributes only because focused
        # timing tests override them per instance after construction.
        self.watchdog_lease_release_timeout_seconds = (
            lifecycle_config.watchdog_lease_release_timeout_seconds
        )
        self.ledger_lease_heartbeat_seconds = (
            lifecycle_config.ledger_lease_heartbeat_seconds
        )
        self.ledger_lease_heartbeat_failure_seconds = (
            lifecycle_config.ledger_lease_heartbeat_failure_seconds
        )
        self.ledger_lease_heartbeat_monitor_seconds = (
            lifecycle_config.ledger_lease_heartbeat_monitor_seconds
        )
        self.ledger_lease_heartbeat_exit_timeout_seconds = (
            lifecycle_config.ledger_lease_heartbeat_exit_timeout_seconds
        )
        self.ledger_lease_external_fence_timeout_seconds = (
            lifecycle_config.ledger_lease_external_fence_timeout_seconds
        )
        self._ctv_runtime_init_lock = threading.Lock()
        self._ctv_runtime = self._make_ctv_runtime_service(
            CtvRuntimeConfig.from_coordinator_config(ctv_config)
        )
        # Eager B1 owner root: the block-candidate service adopted the legacy
        # fields installed above (the first routed write constructs it) and
        # owns every candidate queue/retry/disposition container from here on.
        self._ensure_block_candidate_service()
        # Eager B3 owner root: post-offer accepted-block finalization and its
        # bounded phase/interarrival metrics live on the finalization service;
        # B1's accounting actor reaches it through the coordinator seams.
        self._ensure_block_finalization_service()
        # Eager background-service root: process loop specifications, their
        # start-once state, and the exact drain handles live in the registry;
        # serve() starts named services at the existing recovery boundaries.
        self._background_services = self._make_background_service_registry()

    def _ensure_share_hot_path_state(self) -> None:
        """Backfill dedicated accounting state for lightweight embedders.

        Reduced first-touch shim: the recent-share duplicate window and the
        share-ACK histograms are owned by the share-submission service, so
        only the legacy accounting-counter lock is ensured here.
        """
        if hasattr(self, "_share_accounting_lock"):
            return
        with _HOT_PATH_LOCK_INITIALIZATION_LOCK:
            if not hasattr(self, "_share_accounting_lock"):
                self._share_accounting_lock = threading.Lock()

    def _observe_share_ack_seconds(
        self,
        result: str,
        elapsed_seconds: float,
    ) -> None:
        # Coordinator/session routing seam: the session owner stamps request
        # receipt and reports the accepted/rejected response boundary here;
        # the share-submission owner holds the histogram state.
        self._ensure_share_submission_service().observe_share_ack_seconds(
            result,
            elapsed_seconds,
        )

    def share_ack_snapshot(self) -> dict[str, dict[str, Any]]:
        """Copied share-ACK histograms for the metrics renderer.

        The data is the submission owner's copied snapshot. A coordinator
        whose submission service was never touched has observed nothing, so
        it reports zeroed histograms (or a legacy embedder's seeded state)
        without forcing service construction.
        """
        service = self.__dict__.get("_share_submission_service")
        if service is not None:
            return service.share_ack_snapshot()
        legacy = self.__dict__.get("share_ack_histograms")
        return (
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

    def share_ack_metrics_lines(self) -> list[str]:
        # Compatibility wrapper for the extracted metrics owner.
        return self._ensure_metrics_renderer().share_ack_metrics_lines()

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
            f"prism coordinator: collection-mode block candidate settles "
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
            initial_dropped_solves = self.__dict__.pop(
                "block_solves_dropped_counts", None
            )
            initial_share_ack = self.__dict__.pop("share_ack_histograms", None)
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
                assemble=lambda client, context, request, version_mask: (
                    direct_stratum.assemble_submission(
                        context.job,
                        extranonce2_hex=request.extranonce2_hex,
                        ntime_hex=request.ntime_hex,
                        nonce_hex=request.nonce_hex,
                        version_bits_hex=request.version_bits_hex,
                        version_mask=version_mask,
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
                note_retained_submit=(
                    lambda policy, cross_connection: self.note_evicted_job_submit(
                        policy,
                        cross_connection=cross_connection,
                    )
                ),
                note_collection_candidate=(
                    lambda context, submission: self._note_collection_block_candidate(
                        context,
                        submission,
                    )
                ),
                candidate_intent=lambda candidate: self.block_candidate_intent(
                    candidate
                ),
                finish_pending_commit=lambda pending: (
                    self._finish_pending_share_commit(pending)
                ),
                record_terminal_outcome=lambda block_hash, accepted: (
                    self._record_block_candidate_terminal_outcome(
                        block_hash,
                        accepted=accepted,
                    )
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
            if initial_dropped_solves is not None:
                service.replace_dropped_solves(initial_dropped_solves)
            if initial_share_ack is not None:
                service.replace_share_ack_histograms(initial_share_ack)
            self.__dict__["_share_submission_service"] = service
            return service

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
            configured_audit_dir = self.__dict__.get("audit_dir")
            if configured_audit_dir is None:
                # Normal construction always sets audit_dir before any store
                # use; only bare test instances reach this fallback. Root it
                # under a per-instance temporary directory so a focused test
                # never litters the process working directory.
                fallback_root = self.__dict__.get("_audit_artifact_fallback_root")
                if fallback_root is None:
                    fallback_root = Path(
                        tempfile.mkdtemp(prefix="prism-audit-fallback-")
                    )
                    self.__dict__["_audit_artifact_fallback_root"] = fallback_root
                configured_audit_dir = Path(fallback_root) / "prism-audit"
            audit_dir = Path(configured_audit_dir)
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
            # The verifier subprocess honors the coordinator's configured
            # bundle-build budget rather than a store-local fixed default.
            verifier_timeout_seconds = max(
                0.001,
                float(
                    self.__dict__.get(
                        "bundle_build_timeout_seconds",
                        DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
                    )
                ),
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
                        verifier_timeout_seconds=verifier_timeout_seconds,
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
                if store.verifier_timeout_seconds != verifier_timeout_seconds:
                    updates["verifier_timeout_seconds"] = verifier_timeout_seconds
                if updates:
                    store.reconfigure(**updates)
            return store

    def _upgrade_legacy_audit_evidence(self) -> None:
        store = self._ensure_audit_artifact_store()
        with self._payout_balance_mutation_lock:
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
            else os.environ.get("PRISM_POSTGRES_PSQL_COMMAND", "")
        )
        database_url = (
            ledger_config.database_url or ""
            if ledger_config is not None
            else os.environ.get("PRISM_DATABASE_URL", "")
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
        if writer_session_token is None:
            writer_session_token = (
                f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}{uuid.uuid4().hex}"
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
            lease_acquire_lock_timeout_seconds=(
                ledger_config.lease_acquire_lock_timeout_seconds
                if ledger_config is not None
                else env_positive_float(
                    "PRISM_LEDGER_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS",
                    DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS,
                )
            ),
            lease_acquire_attempts=(
                ledger_config.lease_acquire_attempts
                if ledger_config is not None
                else env_positive_int(
                    "PRISM_LEDGER_LEASE_ACQUIRE_ATTEMPTS",
                    DEFAULT_LEASE_ACQUIRE_ATTEMPTS,
                )
            ),
            postgres_idle_in_transaction_timeout_seconds=(
                ledger_config.postgres_idle_in_transaction_timeout_seconds
                if ledger_config is not None
                else env_positive_float(
                    "PRISM_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS",
                    DEFAULT_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS,
                )
            ),
            postgres_tcp_keepalives_idle_seconds=(
                ledger_config.postgres_tcp_keepalives_idle_seconds
                if ledger_config is not None
                else env_positive_int(
                    "PRISM_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS",
                    DEFAULT_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS,
                )
            ),
            postgres_tcp_keepalives_interval_seconds=(
                ledger_config.postgres_tcp_keepalives_interval_seconds
                if ledger_config is not None
                else env_positive_int(
                    "PRISM_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS",
                    DEFAULT_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS,
                )
            ),
            postgres_tcp_keepalives_count=(
                ledger_config.postgres_tcp_keepalives_count
                if ledger_config is not None
                else env_positive_int(
                    "PRISM_POSTGRES_TCP_KEEPALIVES_COUNT",
                    DEFAULT_POSTGRES_TCP_KEEPALIVES_COUNT,
                )
            ),
            # The own-write deferral margin must cover the longest RPC the
            # lease fence can authorize — submitblock's dedicated deadline
            # and the CTV broadcast RPCs riding JsonRpc.call's default —
            # plus fixed headroom for fence-to-RPC scheduling and clock
            # drift (see LEASE_AUTHORITY_MARGIN_HEADROOM_SECONDS; the RPCs
            # themselves cannot outlive their wall-clock deadlines). The
            # ledger floors this at half the TTL, so short deadlines keep
            # today's behavior and only an operator raising a guarded
            # deadline widens the deferral window with it.
            lease_authority_margin_seconds=(
                LEASE_AUTHORITY_MARGIN_HEADROOM_SECONDS
                + max(
                    float(
                        config.block.submit_rpc_timeout_seconds
                        if config is not None
                        else getattr(
                            self,
                            "block_submit_rpc_timeout_seconds",
                            DEFAULT_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS,
                        )
                    ),
                    DEFAULT_QBIT_RPC_CALL_TIMEOUT_SECONDS,
                )
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
                    "PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS", 30.0
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

        policy = default_prism_payout_policy()
        output_policy = env_optional("PRISM_COINBASE_OUTPUT_POLICY") or "canonical"
        if output_policy not in VALID_COINBASE_OUTPUT_POLICIES:
            raise SystemExit(
                "PRISM_COINBASE_OUTPUT_POLICY must be one of: "
                + ", ".join(sorted(VALID_COINBASE_OUTPUT_POLICIES))
            )
        fee_bps_raw = env_optional("PRISM_POOL_FEE_BPS")
        fee_enabled = env_bool("PRISM_POOL_FEE_ENABLED", "0")
        fee_address = env_optional("PRISM_POOL_FEE_ADDRESS")
        fee_program_hex = env_optional("PRISM_POOL_FEE_P2MR_PROGRAM_HEX")
        fee_recipient_id = env_optional("PRISM_POOL_FEE_RECIPIENT_ID")
        fee_order_key = env_optional("PRISM_POOL_FEE_ORDER_KEY")
        has_fee_config = any(
            value is not None
            for value in (fee_bps_raw, fee_address, fee_program_hex, fee_recipient_id, fee_order_key)
        )
        if not fee_enabled:
            if has_fee_config:
                raise SystemExit("set PRISM_POOL_FEE_ENABLED=1 when configuring pool fees")
            if output_policy == "pool-fee-first":
                raise SystemExit(
                    "PRISM_COINBASE_OUTPUT_POLICY=pool-fee-first requires PRISM_POOL_FEE_ENABLED=1"
                    " and a configured pool fee"
                )
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
        if output_policy != "canonical":
            # Omitted for canonical so canonical payloads and signed audit
            # artifacts keep their historical bytes.
            policy["coinbase_output_policy"] = output_policy
        self._prism_payout_policy_cache = policy
        return policy

    def prism_ctv_settlement_config(
        self,
        *,
        block_height: int | None = None,
        parent_hash: str | None = None,
    ) -> dict[str, object] | None:
        if not env_bool("PRISM_CTV_SETTLEMENT_ENABLED", "0"):
            return None
        direct_floor_sats = env_positive_int_with_legacy(
            "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS",
            "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_SATS",
            DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
        )
        reserved_coinbase_outputs = env_int("PRISM_RESERVED_COINBASE_OUTPUTS", 0)
        if reserved_coinbase_outputs < 0:
            raise SystemExit("PRISM_RESERVED_COINBASE_OUTPUTS must be non-negative")
        config: dict[str, object] = {
            "direct_floor_sats": direct_floor_sats,
            "config": {
                "max_coinbase_settlement_outputs": env_positive_int(
                    "PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS",
                    DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS,
                ),
                "max_direct_coinbase_outputs": env_positive_int(
                    "PRISM_MAX_DIRECT_COINBASE_OUTPUTS",
                    DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS,
                ),
                "max_fanout_recipients_per_transaction": env_positive_int(
                    "PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION",
                    DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION,
                ),
                "reserved_coinbase_outputs": reserved_coinbase_outputs,
            },
        }
        config["fanout_fee_rate_policy"] = {
            "market_fee_rate_sats_per_1000_weight": self.ctv_fanout_market_fee_rate_bits_per_1000_weight(
                block_height=block_height,
                parent_hash=parent_hash,
            ),
            "premium_bps": env_positive_int(
                "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS",
                DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS,
            ),
        }
        return config

    def ctv_fanout_market_fee_rate_bits_per_1000_weight(
        self,
        *,
        block_height: int | None = None,
        parent_hash: str | None = None,
    ) -> int:
        configured_rate = env_optional_positive_int_with_legacy(
            "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
            "PRISM_CTV_FANOUT_FEE_MARKET_RATE_SATS_PER_1000_WEIGHT",
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
            estimate = self.rpc.call(
                "estimatesmartfee",
                [env_positive_int("PRISM_CTV_FANOUT_FEE_ESTIMATE_TARGET_BLOCKS", 2)],
            )
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

    def _ensure_job_bundle_service(self) -> JobBundleService:
        service = self.__dict__.get("job_bundle_service")
        if service is not None:
            return service
        init_lock = self.__dict__.get("_job_bundle_service_init_lock")
        if init_lock is None:
            # CPython's setdefault is atomic under the GIL. Focused tests may
            # construct through __new__, while normal instances build the
            # service on the first legacy-field touch during __init__.
            init_lock = self.__dict__.setdefault(
                "_job_bundle_service_init_lock",
                threading.Lock(),
            )
        with init_lock:
            service = self.__dict__.get("job_bundle_service")
            if service is not None:
                return service
            service = JobBundleService(
                self,
                # Resolve the spool-file factory through this module's global
                # at call time so the historical
                # ``prism_coordinator._share_window_spool_file`` patch seam
                # keeps steering spool creation.
                spool_factory=lambda: _share_window_spool_file(),
                payout_ledger_artifact_type=PayoutLedgerArtifact,
                now_ms=lambda: now_ms(),
                canonical_json_sha256_override=(
                    lambda value: canonical_json_sha256(value)
                ),
            )
            self.__dict__["job_bundle_service"] = service
        return service

    def _ensure_bundle_compiler(self) -> BundleCompiler:
        compiler = self.__dict__.get("bundle_compiler")
        if compiler is not None:
            return compiler
        init_lock = self.__dict__.get("_bundle_compiler_init_lock")
        if init_lock is None:
            init_lock = self.__dict__.setdefault(
                "_bundle_compiler_init_lock",
                threading.Lock(),
            )
        with init_lock:
            compiler = self.__dict__.get("bundle_compiler")
            if compiler is not None:
                return compiler
            compiler = BundleCompiler(
                self,
                superseded_error=_JobBundleBuildSuperseded,
                cancellation_error_types=(
                    JobBuildCancelled,
                    _JobBundleBuildSuperseded,
                ),
                build_control_type=_JobBundleBuildControl,
                # Resolve the builder command through this module's global at
                # call time so the historical patch seam keeps working.
                tool_command=lambda name: prism_tool_command(name),
            )
            self.__dict__["bundle_compiler"] = compiler
        return compiler

    def _ensure_tip_refresh_service(self) -> TipRefreshService:
        service = self.__dict__.get("tip_refresh_service")
        if service is not None:
            return service
        init_lock = self.__dict__.get("_tip_refresh_service_init_lock")
        if init_lock is None:
            # CPython's setdefault is atomic under the GIL. Focused tests may
            # construct through __new__, while normal instances build the
            # service on the first legacy-field touch during __init__.
            init_lock = self.__dict__.setdefault(
                "_tip_refresh_service_init_lock",
                threading.Lock(),
            )
        with init_lock:
            service = self.__dict__.get("tip_refresh_service")
            if service is not None:
                return service
            service = TipRefreshService(
                self,
                # Coordinator-owned exception types and the S2 delivery
                # priorities are injected so the R1 owner never imports this
                # module (or job_delivery, which sits above it).
                shutdown_error=ShutdownInProgress,
                job_build_failed_error=_JobBuildFailed,
                delivery_priority_initial=PRISM_DELIVERY_PRIORITY_INITIAL,
                delivery_priority_new_tip=PRISM_DELIVERY_PRIORITY_NEW_TIP,
                delivery_priority_same_tip=PRISM_DELIVERY_PRIORITY_SAME_TIP,
            )
            self.__dict__["tip_refresh_service"] = service
        return service

    def _ensure_job_delivery_service(self) -> JobDeliveryService:
        service = self.__dict__.get("job_delivery_service")
        if service is not None:
            return service
        init_lock = self.__dict__.get("_job_delivery_service_init_lock")
        if init_lock is None:
            init_lock = self.__dict__.setdefault(
                "_job_delivery_service_init_lock",
                threading.Lock(),
            )
        with init_lock:
            service = self.__dict__.get("job_delivery_service")
            if service is not None:
                return service
            service = JobDeliveryService(
                self,
                # The Stratum error type is owned by the session module; it
                # is injected so the S2 owner never imports the session owner
                # (or this module).
                stratum_error=StratumError,
            )
            self.__dict__["job_delivery_service"] = service
        return service

    def _ensure_payout_state_service(self) -> PayoutStateService:
        service = self.__dict__.get("payout_state_service")
        if service is not None:
            return service
        init_lock = self.__dict__.get("_payout_state_service_init_lock")
        if init_lock is None:
            init_lock = self.__dict__.setdefault(
                "_payout_state_service_init_lock",
                threading.Lock(),
            )
        with init_lock:
            service = self.__dict__.get("payout_state_service")
            if service is not None:
                return service
            service = PayoutStateService(
                self,
                # The shutdown exception type stays coordinator-owned until
                # the shutdown controller is extracted; the wall-clock stamp
                # and canonical serialization resolve this module's globals
                # at call time so existing monkeypatches keep intercepting.
                shutdown_error=ShutdownInProgress,
                now_ms=lambda: now_ms(),
                canonical_json_text_override=(
                    lambda value: canonical_json_text(value)
                ),
                canonical_json_sha256_override=(
                    lambda value: canonical_json_sha256(value)
                ),
            )
            self.__dict__["payout_state_service"] = service
        return service

    def _ensure_share_writer_service(self) -> ShareWriter:
        service = self.__dict__.get("share_writer_service")
        if service is not None:
            return service
        init_lock = self.__dict__.get("_share_writer_service_init_lock")
        if init_lock is None:
            init_lock = self.__dict__.setdefault(
                "_share_writer_service_init_lock",
                threading.Lock(),
            )
        with init_lock:
            service = self.__dict__.get("share_writer_service")
            if service is not None:
                return service
            service = ShareWriter(
                self,
                # The rejection-reason registry stays coordinator-owned; the
                # wall-clock stamp resolves this module's now_ms global at
                # call time so existing monkeypatches keep intercepting.
                internal_error_reason=PRISM_REJECTION_INTERNAL_ERROR,
                now_ms=lambda: now_ms(),
            )
            self.__dict__["share_writer_service"] = service
        return service

    def _ensure_stratum_session_service(self) -> StratumSessionService:
        service = self.__dict__.get("stratum_session_service")
        if service is not None:
            return service
        init_lock = self.__dict__.get("_stratum_session_service_init_lock")
        if init_lock is None:
            init_lock = self.__dict__.setdefault(
                "_stratum_session_service_init_lock",
                threading.Lock(),
            )
        with init_lock:
            service = self.__dict__.get("stratum_session_service")
            if service is not None:
                return service
            service = StratumSessionService(
                self,
                # The shutdown exception type stays coordinator-owned until
                # the shutdown controller is extracted; inject it so the S1
                # owner never imports this module.
                shutdown_error=ShutdownInProgress,
                pool_closed_reason=PRISM_REJECTION_POOL_CLOSED,
            )
            self.__dict__["stratum_session_service"] = service
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
                    # The memo/counter state lock is the coordinator's
                    # control-plane lock (resolved live: focused tests
                    # replace it), preserving the atomicity tip observation
                    # relies on when it evicts memo entries under self.lock.
                    state_lock=lambda: self.lock,
                    source_tip=self._reorg_payout_source_tip,
                    reserve_external_tip=lambda tip: self._reserve_payout_state_source(
                        "external_tip",
                        tip_hash=tip,
                    ),
                    max_supersession_retries=lambda: getattr(
                        self,
                        "payout_reconcile_supersession_retries",
                        DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
                    ),
                    # #113's heartbeat-aware acquisition of the payout-state
                    # preparation lock.
                    prepare_lock=lambda: self._block_submitter_lock(
                        self._payout_state_prepare_lock,
                        "payout-state-prepare",
                    ),
                    capture_source=lambda: self._capture_payout_state_source(),
                    prepared_candidate=lambda captured, **kwargs: (
                        self._prepared_payout_state_candidate(
                            captured,
                            **kwargs,
                        )
                    ),
                    captured_publication_required=lambda captured: (
                        self._captured_payout_source_requires_publication(
                            captured
                        )
                    ),
                    block_publication=lambda **kwargs: (
                        self._block_payout_state_publication(**kwargs)
                    ),
                    publication_guard=lambda: (
                        self._ensure_audit_artifact_store().publication_order_guard()
                    ),
                    publish_candidate=lambda candidate: (
                        self._publish_payout_state_candidate(candidate)
                    ),
                    observe_preparation=lambda elapsed: (
                        self._observe_payout_state_seconds(
                            "preparation",
                            elapsed,
                        )
                    ),
                    chain_view_untrusted=lambda: self.qbit_chain_view_untrusted(),
                    reorg_proof_snapshot=lambda: (
                        self._ensure_tip_refresh_service().reorg_proof_snapshot()
                    ),
                    flight_wait_seconds=lambda: float(
                        getattr(
                            self,
                            "reconcile_flight_wait_seconds",
                            DEFAULT_PRISM_RECONCILE_FLIGHT_WAIT_SECONDS,
                        )
                    ),
                    prefetch_join_timeout_seconds=lambda: getattr(
                        self,
                        "reconcile_prefetch_join_timeout_seconds",
                        PRISM_RECONCILE_PREFETCH_JOIN_TIMEOUT_SECONDS,
                    ),
                    reconcile_with_admission=lambda **kwargs: (
                        self.reconcile_prism_pool_blocks_once(**kwargs)
                    ),
                    reconcile_serialized=lambda **kwargs: (
                        self._reconcile_prism_pool_blocks_serialized(**kwargs)
                    ),
                    ensure_tip=lambda tip: self.ensure_reorg_reconciled_for_tip(
                        tip
                    ),
                    # Accepted-block landings run a reconcile pass inline on
                    # the watchdog-monitored block-work thread. The recorder
                    # stamps only from that thread's registered owner, so
                    # background reconciliation passes and per-client callers
                    # keep recording nothing at all.
                    record_progress=lambda phase: (
                        self._record_block_submitter_phase(phase)
                    ),
                ),
                enabled=bool(self.reorg_reconciler_enabled),
                cache_seconds=self.reorg_reconcile_cache_seconds,
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
        """Payout-state snapshot source tip under the control-plane lock."""
        with self.lock:
            return self._payout_state_source[1]

    def _ensure_job_cache_state(self) -> None:
        # J1 scheduler/cache/template state lives in its owner service; the
        # legacy raw fields route there through class descriptors and the
        # service adopts direct test assignments through those descriptors.
        self._ensure_job_bundle_service()
        if not hasattr(self, "job_build_timeout_seconds"):
            self.job_build_timeout_seconds = DEFAULT_PRISM_JOB_BUILD_TIMEOUT_SECONDS
        if not hasattr(self, "job_build_cancel_grace_seconds"):
            self.job_build_cancel_grace_seconds = (
                DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS
            )
        # P1 payout state lives in its owner service; the legacy raw
        # fields route there through class descriptors and the service
        # adopts direct test assignments through those descriptors (see
        # lab/prism/payout_state.py).
        self._ensure_payout_state_service()
        # The S3 owner holds the pending-commit floor lock and dictionary
        # (see lab/prism/share_writer.py); ensure it exists for anchor reads.
        self._ensure_share_writer_service()
        # The serve-builder daemon state lives in the compiler owner; the
        # legacy raw fields route there through class descriptors.
        self._ensure_bundle_compiler()
        # Reorg flight/memo/lookup/prefetch state lives in its owner service
        # (see lab/prism/reorg_reconciler.py); the service constructor is the
        # single initializer for those containers.
        if not hasattr(self, "_accounted_accepted_block_hashes"):
            self._accounted_accepted_block_hashes: set[str] = set()
        # The B1 owner (created on first touch) holds the block-submit
        # histogram, acceptance-evidence containers, and abandonment dedupe
        # set; the legacy raw fields route there through class descriptors.
        self._ensure_block_candidate_service()
        # Health snapshot and mining-delivery timer state lives in the
        # observability owner; the legacy raw fields route there through the
        # class compatibility properties.
        self._ensure_observability_service()
        # The aggregate progress-health state lives in its owner service; the
        # legacy raw ``_progress_*`` fields route there through descriptors.
        self._ensure_progress_health_service()

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
        """Per-thread scratch dict of phase timings for the current build."""
        return self._ensure_job_bundle_service()._job_build_phases()

    def _cancel_obsolete_job_bundle_builds(
        self,
        *,
        current_tip: str | None = None,
        payout_state_generation: int | None = None,
    ) -> None:
        """Cancel only builds proven obsolete by a newer exact generation."""
        self._ensure_job_bundle_service()._cancel_obsolete_job_bundle_builds(
            current_tip=current_tip,
            payout_state_generation=payout_state_generation,
        )

    def _register_job_bundle_process(
        self,
        control: _JobBundleBuildControl,
        process: subprocess.Popen[str],
    ) -> None:
        self._ensure_job_bundle_service()._register_job_bundle_process(control, process)

    def _unregister_job_bundle_process(
        self,
        control: _JobBundleBuildControl,
        process: Any,
    ) -> None:
        """Detach a shared daemon from a build control after its request."""
        self._ensure_job_bundle_service()._unregister_job_bundle_process(control, process)

    @staticmethod
    def _incremental_window_records_supported(records: Sequence[object]) -> bool:
        """Whether a ledger snapshot exposes the append-only window contract."""
        return PayoutStateService._incremental_window_records_supported(records)

    def _full_payout_window_materialization(self, *, snapshot_anchor_ms: int, snapshot_window_weight: int, reason: str, observed_monotonic: float, append_invalidation_epoch: int, bypass_build_interval: bool=False) -> _PayoutWindowMaterialization:
        """Run the exact ledger oracle and atomically replace cached pages."""
        return self._ensure_payout_state_service()._full_payout_window_materialization(snapshot_anchor_ms=snapshot_anchor_ms, snapshot_window_weight=snapshot_window_weight, reason=reason, observed_monotonic=observed_monotonic, append_invalidation_epoch=append_invalidation_epoch, bypass_build_interval=bypass_build_interval)

    def _incremental_payout_window_materialization(self, *, snapshot_anchor_ms: int, snapshot_window_weight: int, force_full_rescan: bool, bypass_build_interval: bool, append_invalidation_epoch: int, reused_prior_balances_sha256: str | None=None) -> _PayoutWindowMaterialization:
        """Return an exact window using debounce, delta folding, or the oracle."""
        return self._ensure_payout_state_service()._incremental_payout_window_materialization(snapshot_anchor_ms=snapshot_anchor_ms, snapshot_window_weight=snapshot_window_weight, force_full_rescan=force_full_rescan, bypass_build_interval=bypass_build_interval, append_invalidation_epoch=append_invalidation_epoch, reused_prior_balances_sha256=reused_prior_balances_sha256)

    def _build_payout_ledger_artifact(self, expected_payout_state_generation: int, artifact_payout_state_generation: int, network_difficulty: int, force_full_rescan: bool=False, bypass_build_interval: bool=False, during_publication: bool=False) -> PayoutLedgerArtifact | None:
        """Build a stable ledger snapshot without publishing it."""
        return self._ensure_payout_state_service()._build_payout_ledger_artifact(expected_payout_state_generation, artifact_payout_state_generation, network_difficulty, force_full_rescan, bypass_build_interval, during_publication)

    def _prepare_payout_ledger_artifact(self, payout_state_generation: int, network_difficulty: int, *, bypass_build_interval: bool=False) -> None:
        """Prepare and atomically publish an artifact for a current generation."""
        return self._ensure_payout_state_service()._prepare_payout_ledger_artifact(payout_state_generation, network_difficulty, bypass_build_interval=bypass_build_interval)

    def _install_payout_ledger_artifact(self, artifact: PayoutLedgerArtifact) -> bool:
        """Atomically publish a prepared artifact for its own generation."""
        return self._ensure_payout_state_service()._install_payout_ledger_artifact(artifact)

    def _install_payout_ledger_artifact_outcome(self, artifact: PayoutLedgerArtifact) -> bool:
        return self._ensure_payout_state_service()._install_payout_ledger_artifact_outcome(artifact)

    def _install_payout_ledger_artifact_locked(self, artifact: PayoutLedgerArtifact) -> tuple[str, int | None]:
        """Ordering core of the install; returns (outcome, armed generation)."""
        return self._ensure_payout_state_service()._install_payout_ledger_artifact_locked(artifact)

    def _payout_artifact_preparation_loop(self) -> None:
        return self._ensure_payout_state_service()._payout_artifact_preparation_loop()

    def _schedule_payout_ledger_artifact_preparation(self, payout_state_generation: int, network_difficulty: int, *, min_interval_seconds: float | None=None, bypass_build_interval: bool=False) -> None:
        """Latest-generation-wins scheduling with one worker and one slot."""
        return self._ensure_payout_state_service()._schedule_payout_ledger_artifact_preparation(payout_state_generation, network_difficulty, min_interval_seconds=min_interval_seconds, bypass_build_interval=bypass_build_interval)

    def _payout_artifact_max_anchor_age_ms(self) -> float:
        """Audit ceiling on a served window's wall-clock anchor age."""
        return self._ensure_payout_state_service()._payout_artifact_max_anchor_age_ms()

    def _payout_artifact_reanchor_seconds(self) -> float:
        """Anchor age that triggers a background re-anchor while reuse keeps serving."""
        return self._ensure_payout_state_service()._payout_artifact_reanchor_seconds()

    def _payout_artifact_min_build_interval_seconds(self) -> float:
        """Minimum normal cadence; found-block settlement bypasses it."""
        return self._ensure_payout_state_service()._payout_artifact_min_build_interval_seconds()

    def _payout_artifact_reuse_active(self) -> bool:
        """Master kill-switch (PRISM_PAYOUT_ARTIFACT_REUSE) for the reuse line."""
        return self._ensure_payout_state_service()._payout_artifact_reuse_active()

    @staticmethod
    def _payout_artifact_log(event: str, **fields: object) -> None:
        """Single-line JSON lifecycle log; the incident was invisible here."""
        return PayoutStateService._payout_artifact_log(event, **fields)

    def _record_payout_artifact_event(self, event: str) -> None:
        return self._ensure_payout_state_service()._record_payout_artifact_event(event)

    def _note_window_mirror_divergence(self) -> None:
        """Invalidate a refuted daemon window mirror and count the event."""
        return self._ensure_payout_state_service()._note_window_mirror_divergence()

    def _usable_payout_ledger_artifact(self, payout_state_generation: int, network_difficulty: int, *, rearm_on_fence_failure: bool=True) -> PayoutLedgerArtifact | None:
        """Return the armed artifact when reuse is valid for NEW work."""
        return self._ensure_payout_state_service()._usable_payout_ledger_artifact(payout_state_generation, network_difficulty, rearm_on_fence_failure=rearm_on_fence_failure)

    def _rearm_payout_ledger_artifact_after_fence_failure(self, payout_state_generation: int, network_difficulty: int) -> None:
        """Debounced rebuild scheduling for an artifact past the re-anchor floor or ceiling."""
        return self._ensure_payout_state_service()._rearm_payout_ledger_artifact_after_fence_failure(payout_state_generation, network_difficulty)

    def _schedule_current_payout_ledger_artifact_if_missing(self) -> None:
        """Resume speculative preparation after a durable preview catches up."""
        return self._ensure_payout_state_service()._schedule_current_payout_ledger_artifact_if_missing()

    def shutdown_payout_artifact_executor(self) -> None:
        return self._ensure_payout_state_service().shutdown_payout_artifact_executor()

    def _job_build_checkpoint(
        self,
        phase: str,
        cancellation: _JobBuildCancellation,
    ) -> None:
        """Cooperative boundary around coordinator and isolated-worker phases."""

        cancellation.raise_if_cancelled(phase)

    def _record_job_cache_event(self, kind: str, *, hit: bool) -> None:
        self._ensure_job_bundle_service()._record_job_cache_event(kind, hit=hit)

    def _prepare_payout_state_artifact(self, *, generation: int, source_generation: int, cancellation: _JobBuildCancellation | None=None) -> PayoutStateArtifact:
        """Snapshot carry-forward balances once for a payout generation."""
        return self._ensure_payout_state_service()._prepare_payout_state_artifact(generation=generation, source_generation=source_generation, cancellation=cancellation)

    def _payout_state_artifact_from_balances(self, *, generation: int, source_generation: int, balances: list[dict[str, object]]) -> PayoutStateArtifact:
        return self._ensure_payout_state_service()._payout_state_artifact_from_balances(generation=generation, source_generation=source_generation, balances=balances)

    def _publish_self_check_repaired_balances(self, expected_payout_state_generation: int, *, stale_prior_balances_sha256: str, balances: Sequence[dict[str, object]]) -> bool:
        """Replace the published balance snapshot the periodic oracle refuted."""
        return self._ensure_payout_state_service()._publish_self_check_repaired_balances(expected_payout_state_generation, stale_prior_balances_sha256=stale_prior_balances_sha256, balances=balances)

    def _current_payout_state_artifact(self, cancellation: _JobBuildCancellation | None=None) -> PayoutStateArtifact:
        """Return the immutable artifact published for the current generation."""
        return self._ensure_payout_state_service()._current_payout_state_artifact(cancellation)

    def _job_build_executor_locked(self) -> ThreadPoolExecutor:
        return self._ensure_job_bundle_service()._job_build_executor_locked()

    def _start_job_build_locked(self, request: _JobBuildRequest) -> _JobBuildFlight:
        return self._ensure_job_bundle_service()._start_job_build_locked(request)

    def _arm_job_build_locked(self, flight: _JobBuildFlight) -> None:
        self._ensure_job_bundle_service()._arm_job_build_locked(flight)

    def _execute_job_build_request(
        self,
        request: _JobBuildRequest,
    ) -> CachedJobBundle:
        return self._ensure_job_bundle_service()._execute_job_build_request(request)

    _collection_job_builds_are_independent = staticmethod(
        JobBundleService._collection_job_builds_are_independent
    )

    _job_build_requests_can_share = staticmethod(
        JobBundleService._job_build_requests_can_share
    )

    _ready_job_build_precedes_collection = staticmethod(
        JobBundleService._ready_job_build_precedes_collection
    )

    _defer_job_build_locked = staticmethod(
        JobBundleService._defer_job_build_locked
    )

    _job_build_is_publication_critical = staticmethod(
        JobBundleService._job_build_is_publication_critical
    )

    _job_build_promise_done = staticmethod(
        JobBundleService._job_build_promise_done
    )

    def _record_priority_admission_locked(
        self,
        request: _JobBuildRequest,
        result: str,
    ) -> None:
        """Observe first builder admission from publication-priority reservation."""
        self._ensure_job_bundle_service()._record_priority_admission_locked(request, result)

    def _record_initial_prepared_work_locked(self, result: str) -> None:
        self._ensure_job_bundle_service()._record_initial_prepared_work_locked(result)

    def _new_job_build_cancellation(self) -> _JobBuildCancellation:
        return self._ensure_job_bundle_service()._new_job_build_cancellation()

    def _begin_job_build_priority_preparation(
        self,
        requested_monotonic: float | None = None,
    ) -> tuple[int, float]:
        """Reserve publication priority before immutable request construction."""
        return self._ensure_job_bundle_service()._begin_job_build_priority_preparation(requested_monotonic)

    def _finish_job_build_priority_preparation(self, token: int) -> None:
        self._ensure_job_bundle_service()._finish_job_build_priority_preparation(token)

    def _begin_routine_job_build_preparation(
        self,
        *,
        request_source: str,
        cancelled: Callable[[], bool] | None,
    ) -> tuple[int, _JobBuildCancellation, CachedJobBundle | None]:
        """Atomically admit cancellable routine request construction."""
        return self._ensure_job_bundle_service()._begin_routine_job_build_preparation(
            request_source=request_source,
            cancelled=cancelled,
        )

    def _publication_priority_promises_locked(
        self,
    ) -> tuple[Future[CachedJobBundle], ...]:
        """Promises of live publication-critical build requests."""
        return self._ensure_job_bundle_service()._publication_priority_promises_locked()

    def _finish_routine_job_build_preparation(self, token: int) -> None:
        self._ensure_job_bundle_service()._finish_routine_job_build_preparation(token)

    def _publication_priority_scheduled_locked(self) -> bool:
        return self._ensure_job_bundle_service()._publication_priority_scheduled_locked()

    def _job_build_can_inherit_publication_priority(
        self,
        existing: _JobBuildRequest,
        incoming: _JobBuildRequest,
    ) -> bool:
        """Reject an almost-expired or stalled routine flight as a critical owner."""
        return self._ensure_job_bundle_service()._job_build_can_inherit_publication_priority(
            existing,
            incoming,
        )

    _resolve_cancelled_job_build_promise = staticmethod(
        JobBundleService._resolve_cancelled_job_build_promise
    )

    _job_build_flight_outcome = staticmethod(
        JobBundleService._job_build_flight_outcome
    )

    def _evict_orphaned_job_build_flights_locked(self) -> list[str]:
        """Evict finished flights whose completion never released their slot."""
        return self._ensure_job_bundle_service()._evict_orphaned_job_build_flights_locked()

    def _cancel_job_build_flight_locked(
        self,
        flight: _JobBuildFlight,
        reason: str,
        *,
        now: float | None = None,
    ) -> bool:
        return self._ensure_job_bundle_service()._cancel_job_build_flight_locked(flight, reason, now=now)

    def _promote_pending_job_build_locked(self) -> None:
        self._ensure_job_bundle_service()._promote_pending_job_build_locked()

    def _job_build_done(
        self,
        flight: _JobBuildFlight,
        future: Future[CachedJobBundle],
    ) -> None:
        self._ensure_job_bundle_service()._job_build_done(flight, future)

    def _request_job_build(self, request: _JobBuildRequest) -> Future[CachedJobBundle]:
        return self._ensure_job_bundle_service()._request_job_build(request)

    def _cancel_obsolete_job_builds(
        self,
        reason: str,
        *,
        keep_published_snapshot: bool = False,
    ) -> None:
        self._ensure_job_bundle_service()._cancel_obsolete_job_builds(
            reason,
            keep_published_snapshot=keep_published_snapshot,
        )

    def shutdown_job_build_executor(self) -> None:
        self._ensure_job_bundle_service().shutdown_job_build_executor()

    def _job_bundle_payout_state_current(self, bundle: CachedJobBundle) -> bool:
        return self._ensure_job_bundle_service()._job_bundle_payout_state_current(bundle)

    def _payout_balance_mutation(self) -> Iterator[None]:
        """Serialize durable balance changes without excluding delivery."""
        return self._ensure_payout_state_service()._payout_balance_mutation()

    def _begin_accepted_block_payout_preview(self, block_hash: str, *, block_height: int | None=None) -> None:
        """Prevent child work from snapshotting pre-accept balances."""
        return self._ensure_payout_state_service()._begin_accepted_block_payout_preview(block_hash, block_height=block_height)

    def _mark_accepted_block_payout_landed(self, block_hash: str, *, block_height: int) -> None:
        """Bar reconciliation after submitblock makes a candidate active."""
        return self._ensure_payout_state_service()._mark_accepted_block_payout_landed(block_hash, block_height=block_height)

    def _unmark_accepted_block_payout_landed(self, block_hash: str) -> None:
        """Withdraw a landed bar armed for an attempt that never reached RPC."""
        return self._ensure_payout_state_service()._unmark_accepted_block_payout_landed(block_hash)

    def _publish_accepted_block_payout_preview(self, block_hash: str, balances: list[dict[str, object]]) -> list[dict[str, object]]:
        """Publish the balances child work must observe after confirmation."""
        return self._ensure_payout_state_service()._publish_accepted_block_payout_preview(block_hash, balances)

    def _accepted_block_preview_candidate(self, candidate: PayoutStateCandidate, *, block_hash: str, preview: tuple[tuple[str, str, str, int], ...]) -> PayoutStateCandidate:
        """Bind a compact preview to its prepared artifact before gating."""
        return self._ensure_payout_state_service()._accepted_block_preview_candidate(candidate, block_hash=block_hash, preview=preview)

    @staticmethod
    def _serialize_prior_balance_preview(balances: list[dict[str, object]]) -> tuple[tuple[str, str, str, int], ...]:
        return PayoutStateService._serialize_prior_balance_preview(balances)

    def _accepted_block_payout_preview_from_bundle(self, final_bundle: dict[str, Any], *, prior_balances: list[dict[str, object]] | None=None) -> list[dict[str, object]]:
        """Derive the confirmed carry-forward view from a verified bundle."""
        return self._ensure_payout_state_service()._accepted_block_payout_preview_from_bundle(final_bundle, prior_balances=prior_balances)

    @staticmethod
    def _materialize_prior_balance_preview(preview: tuple[tuple[str, str, str, int], ...]) -> list[dict[str, object]]:
        return PayoutStateService._materialize_prior_balance_preview(preview)

    def _clear_accepted_block_payout_preview(self, block_hash: str, *, invalidate_published: bool=False) -> None:
        return self._ensure_payout_state_service()._clear_accepted_block_payout_preview(block_hash, invalidate_published=invalidate_published)

    def _accepted_block_payout_transition_landed(self, block_hash: str) -> bool:
        return self._ensure_payout_state_service()._accepted_block_payout_transition_landed(block_hash)

    def _accepted_block_payout_transition_for_parent(self, parent_hash: str, *, parent_height: int | None=None) -> tuple[str, bool] | None:
        """Select the highest active exact/ancestor payout transition."""
        return self._ensure_payout_state_service()._accepted_block_payout_transition_for_parent(parent_hash, parent_height=parent_height)

    def _accepted_parent_unresolved_depth_cap(self) -> int:
        return self._ensure_payout_state_service()._accepted_parent_unresolved_depth_cap()

    def accepted_parent_unresolved_ages_seconds(self) -> list[float]:
        """Ages of landed transitions whose bookkeeping is still unresolved."""
        return self._ensure_payout_state_service().accepted_parent_unresolved_ages_seconds()

    def _accepted_parent_unresolved_depth(self) -> int:
        return self._ensure_payout_state_service()._accepted_parent_unresolved_depth()

    def _await_pending_parent_payout_preview(self, parent_hash: str, *, parent_height: int | None=None) -> list[dict[str, object]] | None:
        """Wait out a pending accepted-parent transition, returning its preview."""
        return self._ensure_payout_state_service()._await_pending_parent_payout_preview(parent_hash, parent_height=parent_height)

    def _prior_balances_for_job_parent(self, parent_hash: str, *, parent_height: int | None=None, fallback_balances: Sequence[dict[str, object]] | None=None) -> list[dict[str, object]]:
        """Return prospective balances, otherwise a prepared/ledger fallback."""
        return self._ensure_payout_state_service()._prior_balances_for_job_parent(parent_hash, parent_height=parent_height, fallback_balances=fallback_balances)

    def _observe_payout_state_seconds(self, name: str, elapsed_seconds: float, *, relation: str | None=None) -> None:
        return self._ensure_payout_state_service()._observe_payout_state_seconds(name, elapsed_seconds, relation=relation)

    def _observe_payout_gate_admission(self, admission: object, *, generation: int, fallback_wait_seconds: float) -> None:
        return self._ensure_payout_state_service()._observe_payout_gate_admission(admission, generation=generation, fallback_wait_seconds=fallback_wait_seconds)

    def _reserve_payout_state_source(self, cause: str, *, tip_hash: str | None=None, invalidated_monotonic: float | None=None) -> int:
        return self._ensure_payout_state_service()._reserve_payout_state_source(cause, tip_hash=tip_hash, invalidated_monotonic=invalidated_monotonic)

    def _reserve_payout_state_source_if_current(self, expected_source_generation: int, cause: str, *, tip_hash: str | None=None, invalidated_monotonic: float | None=None) -> tuple[int, int, str | None, str, float] | None:
        """Reserve and capture a source only if preparation was not superseded."""
        return self._ensure_payout_state_service()._reserve_payout_state_source_if_current(expected_source_generation, cause, tip_hash=tip_hash, invalidated_monotonic=invalidated_monotonic)

    def _capture_payout_state_source(self) -> tuple[int, int, str | None, str, float]:
        return self._ensure_payout_state_service()._capture_payout_state_source()

    def _prepared_payout_state_candidate(self, captured: tuple[int, int, str | None, str, float], *, force_full_window_rescan: bool=False, bypass_build_interval: bool=False) -> PayoutStateCandidate:
        return self._ensure_payout_state_service()._prepared_payout_state_candidate(captured, force_full_window_rescan=force_full_window_rescan, bypass_build_interval=bypass_build_interval)

    def _cached_found_block_payout_artifact(self, *, base_generation: int, artifact_payout_state_generation: int, network_difficulty: int, fallback_reason: str) -> PayoutLedgerArtifact | None:
        """Retag an exact armed window when immediate preparation is unavailable."""
        return self._ensure_payout_state_service()._cached_found_block_payout_artifact(base_generation=base_generation, artifact_payout_state_generation=artifact_payout_state_generation, network_difficulty=network_difficulty, fallback_reason=fallback_reason)

    def _current_payout_state_candidate(self) -> PayoutStateCandidate:
        return self._ensure_payout_state_service()._current_payout_state_candidate()

    def _record_discarded_payout_candidate(self) -> None:
        return self._ensure_payout_state_service()._record_discarded_payout_candidate()

    def _block_payout_state_publication(self, *, force: bool=False, supersede_with: tuple[int, str | None, str, float] | None=None) -> None:
        """Atomically close delivery, optionally reserving a newer source."""
        return self._ensure_payout_state_service()._block_payout_state_publication(force=force, supersede_with=supersede_with)

    def _payout_state_publication_fenced(self) -> bool:
        """Report whether delivery is still blocked awaiting a publication."""
        return self._ensure_payout_state_service()._payout_state_publication_fenced()

    def _payout_source_requires_publication(self, candidate: PayoutStateCandidate | None=None) -> bool:
        """Report whether an invalidation source still lacks a publication."""
        return self._ensure_payout_state_service()._payout_source_requires_publication(candidate)

    def _captured_payout_source_requires_publication(self, captured: tuple[int, int, str | None, str, float]) -> bool:
        """Publication check for a captured source, without a candidate."""
        return self._ensure_payout_state_service()._captured_payout_source_requires_publication(captured)

    def _publish_payout_state_candidate(self, candidate: PayoutStateCandidate) -> int | None:
        """Publish a prepared candidate, or reject it if its source moved."""
        return self._ensure_payout_state_service()._publish_payout_state_candidate(candidate)

    def _record_first_payout_delivery(self, generation: int, delivered_monotonic: float) -> None:
        return self._ensure_payout_state_service()._record_first_payout_delivery(generation, delivered_monotonic)

    def _advance_payout_state_generation(self) -> int:
        """Publish a payout-only invalidation with no expensive gate work."""
        return self._ensure_payout_state_service()._advance_payout_state_generation()

    def _publish_current_payout_state_with_retry_budget(self, *, initial_attempted: bool=False) -> int | None:
        """Publish the current source with a bounded supersession budget."""
        return self._ensure_payout_state_service()._publish_current_payout_state_with_retry_budget(initial_attempted=initial_attempted)

    def observe_job_build_elapsed(self, elapsed_seconds: float, phases: dict[str, float]) -> None:
        self._ensure_job_bundle_service().observe_job_build_elapsed(elapsed_seconds, phases)

    def _flush_job_build_phases(self, phases: dict[str, float]) -> None:
        """Fold a worker thread's phase accruals into the exported counters."""
        self._ensure_job_bundle_service()._flush_job_build_phases(phases)

    def _reserve_template_artifact_generation(self) -> int:
        """Reserve template ordering when a fetch starts, not when it finishes."""
        return self._ensure_job_bundle_service().template_repository.reserve_generation()

    def _derive_template_artifacts(
        self,
        template: dict[str, Any],
        *,
        generation: int,
    ) -> CachedTemplateArtifacts:
        return self._ensure_job_bundle_service().template_repository.derive(template, generation=generation)

    def _store_template_artifacts(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        return self._ensure_job_bundle_service().template_repository.store_artifacts(artifacts)

    def store_template_artifacts(
        self,
        template: dict[str, Any],
        *,
        generation: int | None = None,
    ) -> CachedTemplateArtifacts | None:
        """Best-effort cache fill from an already-fetched template (blockpoll)."""
        return self._ensure_job_bundle_service().template_repository.store(template, generation=generation)

    def job_issuance_template_artifacts(self) -> CachedTemplateArtifacts:
        """Template artifacts for direct (non-refresh) job issuance."""
        return self._ensure_job_bundle_service().template_repository.issuance()

    def current_template_artifacts(self) -> CachedTemplateArtifacts:
        """Return fresh template artifacts, fetching a template on cache miss."""
        return self._ensure_job_bundle_service().template_repository.current()

    _collection_bundle_identity = staticmethod(
        JobBundleService._collection_bundle_identity
    )

    def _job_bundle_key(
        self,
        artifacts: CachedTemplateArtifacts,
        *,
        mode: str,
        payout_state_generation: int,
        payout_artifact_generation: int = 0,
        worker: WorkerIdentity | None,
    ) -> tuple[object, ...]:
        return self._ensure_job_bundle_service()._job_bundle_key(
            artifacts,
            mode=mode,
            payout_state_generation=payout_state_generation,
            payout_artifact_generation=payout_artifact_generation,
            worker=worker,
        )

    def _job_bundle_mode(self, requested_mode: str | None) -> str:
        return self._ensure_job_bundle_service()._job_bundle_mode(requested_mode)

    def _lookup_job_bundle(
        self,
        key: tuple[object, ...],
    ) -> CachedJobBundle | None:
        return self._ensure_job_bundle_service()._lookup_job_bundle(key)

    def _no_artifact_job_bundle_key(
        self,
        artifacts: CachedTemplateArtifacts,
        *,
        mode: str,
        payout_state_generation: int,
        worker: WorkerIdentity | None,
    ) -> tuple[object, ...]:
        """Fallback cache identity for work built before an artifact re-key."""
        return self._ensure_job_bundle_service()._no_artifact_job_bundle_key(
            artifacts,
            mode=mode,
            payout_state_generation=payout_state_generation,
            worker=worker,
        )

    _no_artifact_bundle_matches_artifact = staticmethod(
        JobBundleService._no_artifact_bundle_matches_artifact
    )

    def _job_bundle_entry_usable(
        self,
        cached: CachedJobBundle | None,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        """Re-validate freshness and readiness for cached bundles."""
        return self._ensure_job_bundle_service()._job_bundle_entry_usable(cached, artifacts)

    def _bind_cached_bundle_to_artifacts(
        self,
        cached: CachedJobBundle,
        artifacts: CachedTemplateArtifacts,
    ) -> CachedJobBundle:
        """Return the cached heavy bundle bound to this exact observation."""
        return self._ensure_job_bundle_service()._bind_cached_bundle_to_artifacts(cached, artifacts)

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
        publication_critical: bool = False,
        request_source: str = "routine",
        priority_requested_monotonic: float | None = None,
        preparation_cancellation: _JobBuildCancellation | None = None,
    ) -> _JobBuildRequest:
        return self._ensure_job_bundle_service()._new_job_build_request(
            artifacts,
            worker,
            mode=mode,
            payout_state_generation=payout_state_generation,
            cache_key=cache_key,
            payout_ledger_artifact=payout_ledger_artifact,
            idle_retarget=idle_retarget,
            publication_critical=publication_critical,
            request_source=request_source,
            priority_requested_monotonic=priority_requested_monotonic,
            preparation_cancellation=preparation_cancellation,
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
        """Cache only current state; report whether payout state stayed valid."""
        return self._ensure_job_bundle_service()._cache_job_bundle_if_current(built, artifacts)

    def _probe_initial_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentity | None,
        mode: str | None,
    ) -> CachedJobBundle | None:
        """Serve an initial request straight from the bundle cache."""
        return self._ensure_job_bundle_service()._probe_initial_job_bundle(artifacts, worker, mode)

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
        """Return one immutable heavy build through a work-identity flight."""
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

    def _shared_job_bundle_after_priority_admission(
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
        return self._ensure_job_bundle_service()._shared_job_bundle_after_priority_admission(
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

    def _share_window_serialization_for_artifact(
        self,
        payout_artifact: PayoutLedgerArtifact,
        shares: list[dict[str, object]],
    ) -> _ShareWindowSerialization:
        """Cached digest and builder fragments for the artifact's share window."""
        return self._ensure_job_bundle_service()._share_window_serialization_for_artifact(
            payout_artifact,
            shares,
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
        tip_refresh_epoch_sequence: int | None = None,
    ) -> PrismJobContext:
        return self._ensure_job_delivery_service().stamp_job_for_client(
            client,
            cached,
            clean_jobs=clean_jobs,
            tip_refresh_epoch_sequence=tip_refresh_epoch_sequence,
        )

    def _current_payout_generation(self) -> int:
        """Narrow P1 adapter: the current payout-state generation."""
        return int(self._ensure_payout_state_service()._payout_state_generation)

    def _payout_delivery_snapshot(self) -> PayoutStateSnapshot:
        """Narrow P1 adapter: copied identity facts for delivery decisions."""
        return self._ensure_payout_state_service().snapshot()

    def _payout_delivery_cancelable(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int,
        priority: bool,
    ) -> Any:
        """Narrow P1 adapter: cancellable delivery-gate admission."""
        gate = self._ensure_payout_state_service()._payout_state_delivery_gate
        return gate.delivery_cancelable(
            cancelled,
            generation=generation,
            priority=priority,
        )

    def _job_delivery_observe_payout_admission(
        self,
        admission: Any,
        *,
        generation: int,
        fallback_wait_seconds: float,
    ) -> None:
        """Narrow P1 adapter: record a delivery-gate admission observation."""
        self._observe_payout_gate_admission(
            admission,
            generation=generation,
            fallback_wait_seconds=fallback_wait_seconds,
        )

    def _payout_template_network_difficulty(self) -> int | None:
        """Narrow J1 adapter for P1: current template network difficulty."""
        service = self.__dict__.get("job_bundle_service")
        if service is None:
            return None
        artifacts = service.template_repository.current_artifacts()
        return None if artifacts is None else artifacts.network_difficulty

    def _invalidate_payout_job_cache(self) -> None:
        """Narrow J1 adapter for P1: drop cached bundles after invalidation."""
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            self._job_bundle_cache.clear()

    def _on_payout_state_invalidated(
        self,
        generation: int,
        invalidated_monotonic: float,
    ) -> None:
        """Narrow G1/R1 adapter for P1: a payout generation was invalidated.

        The verbatim publication bodies keep their inline calls; this seam
        exists for later stack layers that consume P1 through ports.
        """
        self._record_progress_payout_generation(
            generation,
            invalidated_monotonic,
        )

    def _on_payout_state_published(
        self,
        generation: int,
        invalidated_monotonic: float,
    ) -> None:
        """Narrow G1/R1 adapter for P1: a payout generation was published.

        The verbatim publication bodies keep their inline calls; this seam
        exists for later stack layers that consume P1 through ports.
        """
        self._record_progress_payout_generation(
            generation,
            invalidated_monotonic,
        )

    def _next_job_delivery_id(self) -> str:
        """Allocate the next delivery job id through the S2 owner."""
        return self._ensure_job_delivery_service().next_job_id()

    def _complete_job_delivery(
        self,
        client: ClientState,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> None:
        """Commit S2 delivery bookkeeping after a successful socket write."""
        self._ensure_job_delivery_service().complete_delivery(
            client,
            context,
            delivered_monotonic,
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
        if not hasattr(self, "_heartbeat_phases"):
            self._heartbeat_phases = {}
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
        runtime = CtvRuntimeService(
            rpc_call=lambda *args, **kwargs: self.rpc.call(*args, **kwargs),
            ledger_source=lambda: getattr(self, "ledger", None),
            writer_admission=lambda component: self._writer_operation(component),
            tip_refresh_pending=lambda: self.tip_refresh_is_pending(),
            heartbeat=lambda: self._record_heartbeat("ctv_fanout_broadcaster"),
            stop_signal=lambda: self.stop_event,
            config=self._legacy_ctv_runtime_config() if config is None else config,
            before_external_side_effect=(
                lambda *args, **kwargs: (
                    self._require_fresh_ledger_lease_for_external_side_effect(
                        *args, **kwargs
                    )
                )
            ),
            shutdown_exception=ShutdownInProgress,
            # Resolve the process types through this module's globals at call
            # time so existing patch("lab.prism.prism_coordinator....") test
            # seams keep intercepting daemon/broadcaster construction.
            daemon_type=lambda *args, **kwargs: CtvFanoutBroadcastDaemon(
                *args, **kwargs
            ),
            broadcaster_type=lambda *args, **kwargs: CtvFanoutBroadcaster(
                *args, **kwargs
            ),
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

    def _ensure_initial_job_state(self) -> None:
        # S2 initial-job state lives in its owner service; the legacy raw
        # fields route there through class descriptors and the service adopts
        # direct test assignments through those descriptors. Only the live
        # configuration attributes and S1/health fields remain seeded here.
        self._ensure_job_delivery_service()
        if not hasattr(self, "stratum_max_pending_initial_jobs"):
            self.stratum_max_pending_initial_jobs = (
                DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS
            )
        if not hasattr(self, "stratum_initial_job_timeout_seconds"):
            self.stratum_initial_job_timeout_seconds = (
                DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS
            )
        if not hasattr(self, "initial_job_max_workers"):
            self.initial_job_max_workers = DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS
        if not hasattr(self, "handler_thread_count"):
            self.handler_thread_count = 0
        if not hasattr(self, "peak_active_connection_count"):
            self.peak_active_connection_count = len(getattr(self, "clients", ()))

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

    def _ensure_tip_refresh_state(self) -> None:
        # R1 owner state lives in TipRefreshService; the legacy raw fields
        # route there through class descriptors and the service adopts direct
        # test assignments through those descriptors. Only the live
        # configuration attributes and the two historically seeded lazy
        # fields remain here.
        self._ensure_tip_refresh_service()
        if not hasattr(self, "tip_refresh_max_workers"):
            self.tip_refresh_max_workers = DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS
        if not hasattr(self, "tip_refresh_epoch_fanout"):
            self.tip_refresh_epoch_fanout = False
        if not hasattr(self, "latest_detected_tip"):
            self.latest_detected_tip = None
        if not hasattr(self, "tip_refresh_divergence_started_monotonic"):
            self.tip_refresh_divergence_started_monotonic = None

    def _tip_refresh_prune_evicted_jobs(self, now: float, force: bool) -> None:
        """Resolve the retained-prune compatibility seam at call time."""
        override = self.__dict__.get("prune_evicted_job_graveyard")
        if callable(override):
            override(now=now, force=force)
            return
        self._ensure_job_delivery_service().prune_evicted_job_graveyard(
            now=now,
            force=force,
        )

    _tip_refresh_hashrate_proxy = staticmethod(
        TipRefreshService._tip_refresh_hashrate_proxy
    )

    def _payout_epoch_tip_hash_locked(self) -> str | None:
        return self._ensure_tip_refresh_service()._payout_epoch_tip_hash_locked()

    def _mint_tip_refresh_epoch_locked(
        self,
        *,
        tip_hash: str,
        payout_state_generation: int,
        started_monotonic: float,
    ) -> int:
        return self._ensure_tip_refresh_service()._mint_tip_refresh_epoch_locked(
            tip_hash=tip_hash,
            payout_state_generation=payout_state_generation,
            started_monotonic=started_monotonic,
        )

    def _tip_refresh_epoch_for_bundle_locked(
        self,
        cached: CachedJobBundle,
    ) -> int:
        return self._ensure_tip_refresh_service()._tip_refresh_epoch_for_bundle_locked(
            cached,
        )

    def _publish_tip_refresh_epoch_identity_locked(
        self,
        snapshot: QbitTipTemplateSnapshot,
    ) -> None:
        return self._ensure_tip_refresh_service()._publish_tip_refresh_epoch_identity_locked(
            snapshot,
        )

    def _admit_client_tip_refresh_epoch_locked(
        self,
        client: ClientState,
        epoch_sequence: int,
    ) -> bool:
        return self._ensure_tip_refresh_service()._admit_client_tip_refresh_epoch_locked(
            client,
            epoch_sequence,
        )

    _client_tip_refresh_epoch_blocked_locked = staticmethod(
        TipRefreshService._client_tip_refresh_epoch_blocked_locked
    )

    def _tip_refresh_epoch_coverage_reached_locked(
        self,
        client: ClientState,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> list[tuple[str, float]]:
        return self._ensure_tip_refresh_service()._tip_refresh_epoch_coverage_reached_locked(
            client,
            context,
            delivered_monotonic,
        )

    def _record_tip_refresh_epoch_coverage(
        self,
        reached: list[tuple[str, float]],
    ) -> None:
        return self._ensure_tip_refresh_service()._record_tip_refresh_epoch_coverage(
            reached,
        )

    def _record_tip_refresh_wave_outcome(self, outcome: str) -> None:
        return self._ensure_tip_refresh_service()._record_tip_refresh_wave_outcome(
            outcome,
        )

    def _tip_refresh_epoch_fixpoint_reached(self) -> bool:
        return self._ensure_tip_refresh_service()._tip_refresh_epoch_fixpoint_reached()

    def _retain_collection_refresh(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
        payout_state_generation: int,
    ) -> None:
        return self._ensure_tip_refresh_service()._retain_collection_refresh(
            snapshot,
            observation_sequence,
            payout_state_generation,
        )

    def _retained_collection_artifacts(self) -> CachedTemplateArtifacts | None:
        return self._ensure_tip_refresh_service()._retained_collection_artifacts()

    def _retain_current_collection_refresh_if_unrepresented(self) -> None:
        return self._ensure_tip_refresh_service()._retain_current_collection_refresh_if_unrepresented(
        )

    def _note_collection_identity_available(self, client: ClientState) -> None:
        return self._ensure_tip_refresh_service()._note_collection_identity_available(
            client,
        )

    def _consume_retained_collection_refresh(
        self,
        context: PrismJobContext,
    ) -> None:
        return self._ensure_tip_refresh_service()._consume_retained_collection_refresh(
            context,
        )

    def tip_refresh_is_pending(self) -> bool:
        return self._tip_refresh_pending()

    def _tip_refresh_pending(self) -> bool:
        return self._ensure_tip_refresh_service()._tip_refresh_pending()

    def _mark_tip_refresh_pending(self, _observation: object) -> int:
        return self._ensure_tip_refresh_service()._mark_tip_refresh_pending(
            _observation,
        )

    def _claim_tip_refresh_pending(self) -> int | None:
        return self._ensure_tip_refresh_service()._claim_tip_refresh_pending()

    def _mark_tip_refresh_pending_for_poll(
        self,
        owned_token: int | None,
        _observation: object,
    ) -> int | None:
        return self._ensure_tip_refresh_service()._mark_tip_refresh_pending_for_poll(
            owned_token,
            _observation,
        )

    def _clear_tip_refresh_pending(self, token: int) -> None:
        return self._ensure_tip_refresh_service()._clear_tip_refresh_pending(token)

    def _clear_tip_refresh_pending_for_completed_refresh(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
        payout_state_generation: int,
        pending_signal_token: int | None = None,
    ) -> bool:
        return self._ensure_tip_refresh_service()._clear_tip_refresh_pending_for_completed_refresh(
            snapshot,
            observation_sequence,
            payout_state_generation,
            pending_signal_token,
        )

    def _schedule_tip_refresh_retry(self) -> None:
        return self._ensure_tip_refresh_service()._schedule_tip_refresh_retry()

    def _consume_tip_refresh_retry(self) -> bool:
        return self._ensure_tip_refresh_service()._consume_tip_refresh_retry()

    def _note_tip_refresh_attempt_failed(
        self,
        observed_tip: str | None = None,
    ) -> None:
        return self._ensure_tip_refresh_service()._note_tip_refresh_attempt_failed(
            observed_tip,
        )

    def _tip_refresh_failure_holdoff_remaining(self) -> float:
        return self._ensure_tip_refresh_service()._tip_refresh_failure_holdoff_remaining(
        )

    def _observe_tip_refresh_seconds(self, name: str, elapsed_seconds: float) -> None:
        return self._ensure_tip_refresh_service()._observe_tip_refresh_seconds(
            name,
            elapsed_seconds,
        )

    def _observe_tip_refresh_build_phase(
        self,
        phase: str,
        elapsed_seconds: float,
    ) -> None:
        return self._ensure_tip_refresh_service()._observe_tip_refresh_build_phase(
            phase,
            elapsed_seconds,
        )

    def _record_tip_refresh_ipc_bytes(self, direction: str, byte_count: int) -> None:
        return self._ensure_tip_refresh_service()._record_tip_refresh_ipc_bytes(
            direction,
            byte_count,
        )

    def _record_tip_refresh_client_result(self, result: str) -> None:
        return self._ensure_tip_refresh_service()._record_tip_refresh_client_result(
            result,
        )

    def _record_tip_refresh_cancellation(self, stage: str) -> None:
        return self._ensure_tip_refresh_service()._record_tip_refresh_cancellation(
            stage,
        )

    def _tip_refresh_future_started(self) -> None:
        return self._ensure_tip_refresh_service()._tip_refresh_future_started()

    def _tip_refresh_future_finished(self, _future: Future[RefreshResult]) -> None:
        return self._ensure_tip_refresh_service()._tip_refresh_future_finished(_future)

    def tip_refresh_executor(self) -> _BoundedPriorityExecutor:
        return self._ensure_tip_refresh_service().tip_refresh_executor()

    def initial_job_executor(self) -> _BoundedPriorityExecutor:
        return self._ensure_job_delivery_service().initial_job_executor()

    def shutdown_initial_job_executor(self) -> None:
        return self._ensure_job_delivery_service().shutdown_initial_job_executor()

    def shutdown_tip_refresh_executor(self) -> None:
        return self._ensure_tip_refresh_service().shutdown_tip_refresh_executor()

    def retire_share_window_spool(self) -> None:
        """Release the cached share-window spool during shutdown."""
        self._ensure_job_bundle_service().retire_share_window_spool()

    def _cancel_initial_job_future(self, future: Future[bool]) -> bool:
        return self._ensure_job_delivery_service()._cancel_initial_job_future(future)

    def _initial_request_current_locked(self, request: PendingInitialJob) -> bool:
        return self._ensure_job_delivery_service()._initial_request_current_locked(
            request,
        )

    def _initial_request_cancelled(self, request: PendingInitialJob) -> bool:
        return self._ensure_job_delivery_service()._initial_request_cancelled(request)

    def _cancel_pending_initial_job_locked(
        self,
        client: ClientState,
        *,
        count: bool,
    ) -> PendingInitialJob | None:
        return self._ensure_job_delivery_service()._cancel_pending_initial_job_locked(
            client,
            count=count,
        )

    def _client_has_current_tip_job_locked(self, client: ClientState) -> bool:
        return self._ensure_job_delivery_service()._client_has_current_tip_job_locked(
            client,
        )

    def note_initial_job_delivered(
        self,
        client: ClientState,
        *,
        validated_current: bool = False,
    ) -> None:
        return self._ensure_job_delivery_service().note_initial_job_delivered(
            client,
            validated_current=validated_current,
        )

    def schedule_initial_job(self, client: ClientState) -> bool:
        return self._ensure_job_delivery_service().schedule_initial_job(client)

    def request_initial_job_delivery(self, client: ClientState) -> bool:
        return self._ensure_job_delivery_service().request_initial_job_delivery(client)

    def cancel_initial_job_delivery(self, client: ClientState) -> None:
        return self._ensure_job_delivery_service().cancel_initial_job_delivery(client)

    def _submit_initial_job_request(self, request: PendingInitialJob) -> bool:
        return self._ensure_job_delivery_service()._submit_initial_job_request(request)

    def _initial_job_future_finished(
        self,
        request: PendingInitialJob,
        future: Future[bool],
    ) -> None:
        return self._ensure_job_delivery_service()._initial_job_future_finished(
            request,
            future,
        )

    def _run_initial_job(self, request: PendingInitialJob) -> bool:
        return self._ensure_job_delivery_service()._run_initial_job(request)

    def _template_artifacts_are_current(self, artifacts: CachedTemplateArtifacts) -> bool:
        return self._ensure_job_bundle_service().template_repository.artifacts_are_current(artifacts)

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

    def _payout_delivery(self, cancelled: Callable[[], bool], *, generation: int) -> Any:
        """Use cancellable admission while retaining focused gate test seams."""
        return self._ensure_payout_state_service()._payout_delivery(cancelled, generation=generation)

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
        return self._ensure_job_delivery_service()._deliver_initial_bundle(
            request,
            artifacts,
            bundle,
        )

    def sweep_initial_job_timeouts(self, *, now: float | None = None) -> int:
        return self._ensure_job_delivery_service().sweep_initial_job_timeouts(now=now)

    def initial_job_timeout_loop(self) -> None:
        return self._ensure_job_delivery_service().initial_job_timeout_loop()

    def _record_heartbeat(self, name: str, *, phase: str | None = None) -> None:
        self._ensure_watchdog_state()
        with self._heartbeats_lock:
            self._heartbeats[name] = time.monotonic()
            if phase is not None:
                self._heartbeat_phases[name] = phase

    def _block_work_heartbeat_owner(self) -> tuple[str, str] | None:
        """Return the independent heartbeat/phase slots owned by this thread."""
        return self._ensure_block_candidate_service()._block_work_heartbeat_owner()

    def _record_block_work_heartbeat(self, name: str, phase: str) -> None:
        """Record a phase while preserving one-argument heartbeat embedders."""
        self._ensure_block_candidate_service()._record_block_work_heartbeat(
            name,
            phase,
        )

    def _record_block_submitter_phase(self, phase: str) -> None:
        """Stamp a named phase only from a dedicated block-work owner."""
        self._ensure_block_candidate_service()._record_block_submitter_phase(phase)

    def _record_block_submitter_wait(self, phase: str) -> None:
        """Heartbeat owner waits while preserving lightweight test behavior."""
        self._ensure_block_candidate_service()._record_block_submitter_wait(phase)

    def _block_work_wait_slice(self) -> float:
        """Choose a polling slice that stays inside the configured watchdog."""
        return self._ensure_block_candidate_service()._block_work_wait_slice()

    def _observe_coordinator_lock_wait(self, elapsed_seconds: float) -> None:
        """Keep a sliced coordinator-lock wait visible and watchdog-safe."""
        self._ensure_block_candidate_service()._observe_coordinator_lock_wait(
            elapsed_seconds
        )

    def _acquire_block_submitter_lock(self, lock: Any, name: str) -> None:
        """Acquire a submit-path lock in heartbeat/logging slices."""
        self._ensure_block_candidate_service()._acquire_block_submitter_lock(
            lock,
            name,
        )

    def _block_submitter_lock(self, lock: Any, name: str) -> Iterator[None]:
        return self._ensure_block_candidate_service()._block_submitter_lock(
            lock,
            name,
        )

    def _overdue_heartbeats(self, now: float) -> list[str]:
        self._ensure_watchdog_state()
        with self._heartbeats_lock:
            paused = set(self._watchdog_pauses)
            overdue: list[str] = []
            for name, last in self._heartbeats.items():
                if name in paused or now - last <= self.watchdog_timeout_seconds:
                    continue
                phase = self._heartbeat_phases.get(name)
                overdue.append(f"{name}:{phase}" if phase else name)
            return sorted(overdue)

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
            self._heartbeat_phases.pop(name, None)
            self._watchdog_pauses.pop(name, None)

    def _registered_watchdog_heartbeat_names(self, *names: str) -> tuple[str, ...]:
        self._ensure_watchdog_state()
        with self._heartbeats_lock:
            return tuple(name for name in names if name in self._heartbeats)

    def stratum_accept_heartbeat_names(self) -> tuple[str, ...]:
        return configured_accept_heartbeat_names(
            getattr(self, "listener_profiles", None)
        )

    def _make_background_service_registry(self) -> BackgroundServiceRegistry:
        """Describe process loops in their historical shutdown-join order.

        Every specification is registered unconditionally with a
        call-time-resolved target so post-construction test monkeypatches of
        the loop methods and of the enable/interval attributes keep working;
        serve() applies the live conditional at each named start. The
        watchdog specification is deliberately absent here: serve() registers
        it inline at its start boundary so the mandatory
        publication-progress watchdog thread is created before any
        synchronous startup prewarm/recovery work.
        """
        specifications = [
            BackgroundServiceSpec(
                name="qbit_blockpoll",
                thread_name="prism-qbit-block-poll",
                target=lambda: self.blockpoll_loop(),
                daemon=True,
                join_timeout=1.0,
                watchdog_monitored=True,
                registration_identity=("qbit_blockpoll",),
            ),
            BackgroundServiceSpec(
                name="block_submitter",
                thread_name="prism-block-submitter",
                target=lambda: self.block_submit_loop(),
                daemon=True,
                join_timeout=1.0,
                watchdog_monitored=True,
                registration_identity=("block_submitter",),
            ),
            BackgroundServiceSpec(
                name="qbit_blockwait",
                thread_name="prism-qbit-block-wait",
                target=lambda: self.blockwait_loop(),
                daemon=True,
                join_timeout=1.0,
                watchdog_monitored=True,
                registration_identity=("qbit_blockwait",),
            ),
            BackgroundServiceSpec(
                name="vardiff_idle_sweep",
                thread_name="prism-vardiff-idle-sweep",
                target=lambda: self.vardiff_idle_sweep_loop(),
                daemon=True,
                join_timeout=1.0,
                watchdog_monitored=True,
                registration_identity=("vardiff_idle_sweep",),
            ),
            BackgroundServiceSpec(
                name="initial_job_timeout_sweep",
                thread_name="prism-initial-job-timeouts",
                target=lambda: self.initial_job_timeout_loop(),
                daemon=True,
                join_timeout=1.0,
                watchdog_monitored=False,
                registration_identity=("initial_job_timeout_sweep",),
            ),
            BackgroundServiceSpec(
                name="share_writer",
                thread_name="prism-share-writer",
                target=lambda: self.share_append_loop(),
                daemon=True,
                join_timeout=5.0,
                watchdog_monitored=True,
                registration_identity=("share_writer",),
            ),
            BackgroundServiceSpec(
                name="ctv_fanout_broadcaster",
                thread_name="prism-ctv-fanout-broadcaster",
                target=lambda: self.ctv_fanout_broadcaster_loop(),
                daemon=True,
                join_timeout=1.0,
                watchdog_monitored=True,
                registration_identity=("ctv_fanout_broadcaster",),
            ),
            self._health_snapshot_service_spec(),
            self._metrics_snapshot_service_spec(),
        ]
        return BackgroundServiceRegistry(specifications)

    def _health_snapshot_service_spec(self) -> BackgroundServiceSpec:
        return BackgroundServiceSpec(
            name="health_snapshot_refresher",
            thread_name="prism-health-snapshot-refresher",
            target=lambda: self.health_snapshot_loop(),
            daemon=True,
            join_timeout=1.0,
            watchdog_monitored=False,
            registration_identity=("health_snapshot_refresher",),
        )

    def _metrics_snapshot_service_spec(self) -> BackgroundServiceSpec:
        return BackgroundServiceSpec(
            name="metrics_snapshot_refresher",
            thread_name="prism-metrics-snapshot-refresher",
            target=lambda: self.metrics_snapshot_loop(),
            daemon=True,
            join_timeout=1.0,
            watchdog_monitored=False,
            registration_identity=("metrics_snapshot_refresher",),
        )

    def _ensure_background_services(self) -> BackgroundServiceRegistry:
        registry = getattr(self, "_background_services", None)
        if registry is None:
            registry = self._make_background_service_registry()
            self._background_services = registry
        return registry

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

    # Lease heartbeat state is owned by LedgerLeaseHeartbeatService; these
    # descriptors keep the historical private coordinator attribute names
    # readable/writable for tests and embedders with exactly one owner copy.
    _ledger_lease_heartbeat_freshness_lock = _LeaseHeartbeatStateField(
        "freshness_lock"
    )
    _ledger_lease_heartbeat_failure_lock = _LeaseHeartbeatStateField(
        "failure_lock"
    )
    _ledger_lease_heartbeat_last_success_monotonic = _LeaseHeartbeatStateField(
        "last_success_monotonic"
    )
    _ledger_lease_heartbeat_last_progress_monotonic = _LeaseHeartbeatStateField(
        "last_progress_monotonic"
    )
    _ledger_lease_heartbeat_last_server_proven_monotonic = (
        _LeaseHeartbeatStateField("last_server_proven_monotonic")
    )
    _ledger_lease_heartbeat_failed = _LeaseHeartbeatStateField("failed")
    _ledger_lease_heartbeat_ready = _LeaseHeartbeatStateField("ready")
    _ledger_lease_heartbeat_stop_event = _LeaseHeartbeatStateField("stop_event")
    _ledger_lease_heartbeat_thread = _LeaseHeartbeatStateField("thread")
    _ledger_lease_heartbeat_monitor_thread = _LeaseHeartbeatStateField(
        "monitor_thread"
    )
    _ledger_lease_heartbeat_exit_thread = _LeaseHeartbeatStateField(
        "exit_thread"
    )
    _ledger_lease_heartbeat_failure_reason = _LeaseHeartbeatStateField(
        "failure_reason"
    )
    _ledger_lease_heartbeat_failure_has_traceback = _LeaseHeartbeatStateField(
        "failure_has_traceback"
    )

    def _ensure_lease_heartbeat_service(self) -> LedgerLeaseHeartbeatService:
        """Single-flight lazy root for the lease heartbeat owner.

        Every port resolves live coordinator state at call time so the
        documented monkeypatch seams (test timing attributes, patched
        ``_ledger_lease_heartbeat_hard_exit`` / ``_watchdog_hard_exit``
        methods, replaced loop methods, swapped ledgers) keep working.
        """
        service = self.__dict__.get("_lease_heartbeat_service")
        if service is not None:
            return service
        # CPython's setdefault is atomic under the GIL. This lazy path exists
        # for focused tests that construct a coordinator with __new__; normal
        # instances reach it before any process thread starts.
        init_lock = self.__dict__.setdefault(
            "_lease_heartbeat_service_init_lock",
            threading.Lock(),
        )
        with init_lock:
            service = self.__dict__.get("_lease_heartbeat_service")
            if service is not None:
                return service
            service = LedgerLeaseHeartbeatService(
                LedgerLeaseHeartbeatPorts(
                    ledger=lambda: getattr(self, "ledger", None),
                    heartbeat_seconds=lambda: float(
                        getattr(
                            self,
                            "ledger_lease_heartbeat_seconds",
                            DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_SECONDS,
                        )
                    ),
                    failure_seconds=lambda: float(
                        getattr(
                            self,
                            "ledger_lease_heartbeat_failure_seconds",
                            DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_FAILURE_SECONDS,
                        )
                    ),
                    monitor_seconds=lambda: float(
                        getattr(
                            self,
                            "ledger_lease_heartbeat_monitor_seconds",
                            DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_MONITOR_SECONDS,
                        )
                    ),
                    exit_timeout_seconds=lambda: float(
                        getattr(
                            self,
                            "ledger_lease_heartbeat_exit_timeout_seconds",
                            DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_EXIT_TIMEOUT_SECONDS,
                        )
                    ),
                    external_fence_timeout_seconds=lambda: float(
                        getattr(
                            self,
                            "ledger_lease_external_fence_timeout_seconds",
                            DEFAULT_PRISM_LEDGER_LEASE_EXTERNAL_FENCE_TIMEOUT_SECONDS,
                        )
                    ),
                    lease_hard_exit=(
                        lambda message, *, include_traceback: (
                            self._ledger_lease_heartbeat_hard_exit(
                                message,
                                include_traceback=include_traceback,
                            )
                        )
                    ),
                    watchdog_hard_exit=(
                        lambda reason, *, timeout_seconds: (
                            self._watchdog_hard_exit(
                                reason,
                                timeout_seconds=timeout_seconds,
                            )
                        )
                    ),
                    heartbeat_loop=lambda: self.ledger_lease_heartbeat_loop(),
                    monitor_loop=lambda: (
                        self.ledger_lease_heartbeat_monitor_loop()
                    ),
                )
            )
            # Adopt pre-service fixture fields once and remove the legacy
            # keys; the descriptors above route all later access.
            for legacy_key, attribute in (
                ("_ledger_lease_heartbeat_freshness_lock", "freshness_lock"),
                ("_ledger_lease_heartbeat_failure_lock", "failure_lock"),
                (
                    "_ledger_lease_heartbeat_last_success_monotonic",
                    "last_success_monotonic",
                ),
                (
                    "_ledger_lease_heartbeat_last_progress_monotonic",
                    "last_progress_monotonic",
                ),
                (
                    "_ledger_lease_heartbeat_last_server_proven_monotonic",
                    "last_server_proven_monotonic",
                ),
                ("_ledger_lease_heartbeat_failed", "failed"),
                ("_ledger_lease_heartbeat_ready", "ready"),
                ("_ledger_lease_heartbeat_stop_event", "stop_event"),
                ("_ledger_lease_heartbeat_thread", "thread"),
                ("_ledger_lease_heartbeat_monitor_thread", "monitor_thread"),
                ("_ledger_lease_heartbeat_exit_thread", "exit_thread"),
                ("_ledger_lease_heartbeat_failure_reason", "failure_reason"),
                (
                    "_ledger_lease_heartbeat_failure_has_traceback",
                    "failure_has_traceback",
                ),
            ):
                if legacy_key in self.__dict__:
                    setattr(service, attribute, self.__dict__.pop(legacy_key))
            self.__dict__["_lease_heartbeat_service"] = service
        return service

    def _record_ledger_lease_heartbeat_success(
        self,
        renewal_started_monotonic: float,
    ) -> None:
        """Advance the conservative freshness edge without regression."""
        self._ensure_lease_heartbeat_service().record_success(
            renewal_started_monotonic
        )

    def _record_ledger_lease_heartbeat_progress(self) -> None:
        """Stamp mid-verification progress for the staleness monitor only."""
        self._ensure_lease_heartbeat_service().record_progress()

    def _record_ledger_lease_heartbeat_server_proven(self) -> None:
        """Stamp a completed server round trip: progress plus envelope edge."""
        self._ensure_lease_heartbeat_service().record_server_proven()

    def _ledger_lease_adoption_silence_seconds(self) -> float:
        """Resolve the ledger's adoption-silence window for timing checks."""
        return self._ensure_lease_heartbeat_service().adoption_silence_seconds()

    def _ledger_lease_heartbeat_activity_age_seconds(self) -> float:
        """Age of the newest monitor-visible heartbeat activity mark."""
        return self._ensure_lease_heartbeat_service().activity_age_seconds()

    @staticmethod
    def _ledger_lease_guard_session_verifier(
        ledger: object,
    ) -> Callable[..., object] | None:
        """Return the guarded-session liveness check for ``ledger``."""
        return _lease_guard_session_verifier(ledger)

    def _start_ledger_lease_heartbeat(self) -> threading.Thread | None:
        return self._ensure_lease_heartbeat_service().start()

    def _ledger_lease_heartbeat_hard_exit(
        self,
        message: str,
        *,
        include_traceback: bool,
    ) -> None:
        self._ensure_lease_heartbeat_service().hard_exit(
            message,
            include_traceback=include_traceback,
        )

    def ledger_lease_heartbeat_loop(self) -> None:
        self._ensure_lease_heartbeat_service().heartbeat_loop()

    def ledger_lease_heartbeat_monitor_loop(self) -> None:
        self._ensure_lease_heartbeat_service().monitor_loop()

    def _stop_ledger_lease_heartbeat(
        self,
        *,
        deadline: float | None = None,
    ) -> bool:
        return self._ensure_lease_heartbeat_service().stop(deadline=deadline)

    def _require_fresh_ledger_lease_for_external_side_effect(
        self,
        component: str,
    ) -> None:
        """Synchronously verify the exact guarded session before an RPC effect."""
        self._ensure_lease_heartbeat_service(
        ).require_fresh_lease_for_external_side_effect(component)

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

    def _watchdog_hard_exit(
        self,
        reason: str,
        *,
        timeout_seconds: float | None = None,
    ) -> None:
        """Dynamic hard-exit seam; the bounded-exit body is service-owned.

        Both WatchdogService.run and the ledger-lease heartbeat service
        trigger this coordinator method at call time so per-instance
        monkeypatches intercept every exit path.
        """
        self._ensure_watchdog_service().hard_exit(
            reason,
            timeout_seconds=timeout_seconds,
        )

    def _ensure_watchdog_service(self) -> WatchdogService:
        service = self.__dict__.get("_watchdog_service")
        if service is None:
            # Idempotent construction; no init lock is needed.
            service = WatchdogService(
                WatchdogPorts(
                    wait_for_stop=lambda timeout: self.stop_event.wait(timeout),
                    interval_seconds=lambda: self.watchdog_interval_seconds,
                    fatal_exit_requested=lambda: bool(
                        getattr(self, "_fatal_exit_requested", False)
                    ),
                    publication_state=lambda now: (
                        self._publication_watchdog_state(now)
                    ),
                    hard_exit=lambda reason: self._watchdog_hard_exit(reason),
                    liveness_enabled=lambda: bool(
                        getattr(self, "watchdog_enabled", True)
                    ),
                    overdue_heartbeats=lambda now: self._overdue_heartbeats(now),
                    liveness_timeout_seconds=lambda: (
                        self.watchdog_timeout_seconds
                    ),
                    coordination_budget_seconds=lambda: float(
                        getattr(
                            self,
                            "coordination_blocked_exit_seconds",
                            DEFAULT_PRISM_COORDINATION_BLOCKED_EXIT_SECONDS,
                        )
                    ),
                    publication_budget_seconds=lambda: float(
                        getattr(
                            self,
                            "template_refresh_failure_exit_seconds",
                            DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
                        )
                    ),
                    ensure_job_cache_state=self._ensure_job_cache_state,
                    publication_failure_expired=lambda now: (
                        self.publication_progress_failure_expired(now)
                    ),
                    publication_divergence_since=(
                        self._publication_divergence_since_locked
                    ),
                    lease_release_timeout_seconds=lambda: float(
                        getattr(
                            self,
                            "watchdog_lease_release_timeout_seconds",
                            DEFAULT_PRISM_WATCHDOG_LEASE_RELEASE_TIMEOUT_SECONDS,
                        )
                    ),
                    shutdown_controller=lambda: self._ensure_shutdown_controller(),
                    request_shutdown=lambda: self.request_shutdown(None),
                    release_ledger_lease=lambda deadline: (
                        self.release_ledger_lease(
                            fresh_connection=True,
                            deadline=deadline,
                            emit_logs=False,
                        )
                    ),
                    lease_failure_reason=lambda: getattr(
                        self,
                        "_ledger_lease_heartbeat_failure_reason",
                        None,
                    ),
                    exit_process=lambda code: os._exit(code),
                    log=lambda message: print(message, flush=True),
                )
            )
            self.__dict__["_watchdog_service"] = service
        return service

    def _publication_divergence_since_locked(self) -> float | None:
        """Locked snapshot of the progress-health divergence timestamp.

        Progress-health keeps ownership of the divergence state; the
        watchdog reads it through this port while holding its own
        coordination lock (watchdog lock before progress-health lock,
        never the reverse).
        """
        with self._progress_health_lock:
            return self._progress_publication_divergence_since_monotonic

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
        block_work_owner = self._block_work_heartbeat_owner() is not None
        phase = f"writer-admission:{component}"
        if block_work_owner:
            self._record_block_submitter_phase(phase)
        if block_work_owner:
            token = controller.enter_writer(
                component,
                wait_callback=lambda: self._record_block_submitter_phase(phase),
            )
        else:
            # Keep the historical one-argument seam for focused embedders and
            # test controllers; only dedicated block-work owners need sliced
            # admission heartbeats.
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
        return self._ensure_tip_refresh_service()._cancel_active_tip_refresh_for_shutdown(
        )

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

    def release_ledger_lease(
        self,
        *,
        fresh_connection: bool = False,
        deadline: float | None = None,
        emit_logs: bool = True,
    ) -> bool:
        """Release a quiesced writer lease at most once.

        The exact-session database fence makes an already-absent lease safe.
        Exceptions remain best-effort: they are observable, never retried from
        a duplicate finally block, and leave TTL fencing intact.
        """
        shutdown_log: Callable[..., None] = (
            self._shutdown_log if emit_logs else lambda *_args, **_kwargs: None
        )
        controller = self._ensure_shutdown_controller()
        claimed, blockers = controller.claim_lease_release()
        if not claimed:
            if blockers:
                shutdown_log(
                    "lease_release_withheld",
                    reason="active_writer_operations",
                    blockers=blockers,
                )
            return controller.lease_release_succeeded

        if not self._stop_ledger_lease_heartbeat(deadline=deadline):
            controller.finish_lease_release("failure", 0.0)
            shutdown_log(
                "lease_release",
                duration_seconds=0.0,
                outcome="heartbeat_stop_timeout",
                released=False,
            )
            return False

        ledger = getattr(self, "ledger", None)
        release = (
            getattr(ledger, "release_writer_lease_fresh_connection", None)
            if fresh_connection
            else None
        )
        if release is None:
            release = getattr(ledger, "release_writer_lease", None)
        shutdown_log(
            "lease_release_attempt",
            supported=release is not None,
            fresh_connection=fresh_connection,
        )
        if release is None:
            controller.finish_lease_release("unsupported", 0.0)
            shutdown_log(
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
            shutdown_log(
                "lease_release",
                duration_seconds=round(elapsed, 6),
                outcome="failure",
                released=False,
            )
            if emit_logs:
                traceback.print_exc()
            return False
        elapsed = max(0.0, time.monotonic() - started)
        outcome = "success" if released else "not_held"
        controller.finish_lease_release(outcome, elapsed)
        snapshot = controller.snapshot()
        shutdown_log(
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
            # Registry-owned drain: only started registered services, in
            # registration order. serve() passes an explicit list combining
            # this with its non-registry threads; focused shutdown callers
            # keep their historical explicit-thread seam.
            drain_threads = self._ensure_background_services().threads_to_drain()
        else:
            drain_threads = threads
        # The audit HTTP facade is demand-created; stop it only if it ever
        # existed. Its bounded stop runs after lease release and before the
        # non-writer thread joins so the listener port frees promptly.
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
        """Bind and listen on every stratum listener profile.

        Called before the slow parts of startup (qbit readiness, policy
        validation, block-work recovery) so miners reconnecting through a
        restart park in the kernel accept backlog instead of getting
        connection refused, which sends firmware into reconnect backoff or
        failover and costs hashrate. bind() retries EADDRINUSE for a bounded
        window because a predecessor process may still hold the port while
        draining its shutdown. Returns None when shutdown is requested during
        the retry, so startup can abort gracefully.
        """
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
        )

    def serve(self) -> None:
        with ExitStack() as listener_stack:
            self._serve_with_listener_stack(listener_stack)

    def _serve_with_listener_stack(self, listener_stack: ExitStack) -> None:
        self._startup_phase_origin_monotonic = time.monotonic()
        lease_heartbeat_thread = self._start_ledger_lease_heartbeat()
        self._record_startup_phase_once("lease_heartbeat_started")
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
        self._record_startup_phase_once("node_rpc_ready")
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
        # Make a best-effort attempt to recover one block candidate before
        # accepting Stratum connections. If a slow ledger exhausts the short
        # database budget, finish startup and let the dedicated submitter loop
        # retry every durable outbox row with its ordinary paced backoff.
        if not self._run_startup_block_candidate_replay():
            return
        if self.stop_event.is_set():
            return
        self._record_block_work_heartbeat("block_submitter", "starting")
        block_accounting_thread = self._start_block_accounting_thread()
        self._start_background_service("block_submitter")
        # Threads owned outside the background-service registry (the block
        # accounting worker and the lease heartbeat pair below) keep their
        # historical bounded joins alongside the registry-owned drains.
        extra_drain_threads: list[tuple[threading.Thread, float]] = [
            (block_accounting_thread, 1.0),
        ]
        # Publication progress is mandatory even when the operator disables
        # ordinary heartbeat checks. Start the watchdog before any synchronous
        # startup prewarm/recovery work: a timeout-ignoring block call can ask
        # for fail-stop while the main thread is itself wedged in that work.
        # The specification is registered at this exact boundary (not in
        # _make_background_service_registry) so serve() keeps owning the
        # watchdog thread's creation order.
        self._ensure_background_services().register_if_absent(
            BackgroundServiceSpec(
                name="watchdog",
                thread_name="prism-watchdog",
                target=self.watchdog_loop,
                daemon=True,
                join_timeout=1.0,
                watchdog_monitored=False,
                registration_identity=("watchdog_loop",),
            )
        )
        self._start_background_service("watchdog")
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
        if lease_heartbeat_thread is not None:
            extra_drain_threads.append((lease_heartbeat_thread, 1.0))
            lease_heartbeat_monitor_thread = getattr(
                self,
                "_ledger_lease_heartbeat_monitor_thread",
                None,
            )
            if lease_heartbeat_monitor_thread is not None:
                extra_drain_threads.append((lease_heartbeat_monitor_thread, 1.0))
        # Replay any shares stranded on disk by a prior ledger-outage
        # shutdown before serving, so no acked share is lost across restart.
        if not self._run_startup_writer_replay(
            self.replay_recovered_shares,
            drain_threads=[
                *self._ensure_background_services().threads_to_drain(),
                *extra_drain_threads,
            ],
        ):
            return
        self.share_writer_active = True
        self._start_background_service("share_writer")
        if self.ctv_broadcaster_enabled:
            self._start_background_service("ctv_fanout_broadcaster")
            print(
                "prism coordinator: CTV fanout broadcaster enabled "
                f"mode={'cpfp' if self.ctv_broadcaster_fee_sats > 0 else 'direct'} "
                f"fee_bits={self.ctv_broadcaster_fee_sats} "
                f"wallet={'configured' if self.ctv_broadcaster_wallet else 'none'} "
                f"interval={self.ctv_broadcaster_interval_seconds:g}s "
                f"limit={self.ctv_broadcaster_limit} "
                f"chunk_size={self.ctv_broadcaster_chunk_size}",
                flush=True,
            )
        if self.watchdog_enabled:
            print(
                "prism coordinator: liveness and publication-progress watchdog enabled "
                f"timeout={self.watchdog_timeout_seconds:g}s "
                f"publication_budget={self.template_refresh_failure_exit_seconds:g}s "
                f"coordination_blocked_budget={self.coordination_blocked_exit_seconds:g}s "
                f"interval={self.watchdog_interval_seconds:g}s",
                flush=True,
            )
        else:
            print(
                "prism coordinator: publication-progress watchdog enabled "
                f"budget={self.template_refresh_failure_exit_seconds:g}s "
                f"coordination_blocked_budget={self.coordination_blocked_exit_seconds:g}s "
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
            self.drain_non_writer_components(
                [
                    *self._ensure_background_services().threads_to_drain(),
                    *extra_drain_threads,
                ]
            )

    def _record_startup_phase_once(self, phase: str) -> None:
        """Record seconds from serve() start to a named startup phase, once.

        Lease adoption, accepted-stat warm-up, audit binding, replay
        enumeration, first tip template, and first delivered job are timed
        separately so a slow phase is attributable instead of folding into
        one opaque time-to-ready figure (issue #188).
        """
        origin = getattr(self, "_startup_phase_origin_monotonic", None)
        if origin is None:
            return
        elapsed = max(0.0, time.monotonic() - float(origin))
        # Lightweight embedders (signal-handling startup tests) can reach
        # this before dataclass construction installs self.lock.
        with getattr(self, "lock", None) or _HOT_PATH_LOCK_INITIALIZATION_LOCK:
            phases = getattr(self, "_startup_phase_seconds", None)
            if phases is None:
                phases = {}
                self._startup_phase_seconds = phases
            if phase not in phases:
                phases[phase] = elapsed

    def startup_phase_seconds(self) -> dict[str, float]:
        with getattr(self, "lock", None) or _HOT_PATH_LOCK_INITIALIZATION_LOCK:
            phases = getattr(self, "_startup_phase_seconds", None)
            return dict(phases) if phases is not None else {}

    def _note_block_replay_enumeration_owed(self) -> None:
        """Mark that pending durable candidates have not been enumerated yet."""
        self._ensure_block_candidate_service()._note_block_replay_enumeration_owed()

    def _clear_block_replay_enumeration_owed(self) -> None:
        self._ensure_block_candidate_service()._clear_block_replay_enumeration_owed()

    def _block_replay_enumeration_owed(self) -> bool:
        return self._ensure_block_candidate_service()._block_replay_enumeration_owed()

    def _run_startup_writer_replay(
        self,
        replay: Callable[[], int],
        *,
        drain_threads: list[tuple[threading.Thread, float]] | None = None,
    ) -> bool:
        """Run startup ledger replay, stopping cleanly if shutdown wins."""
        try:
            replay()
        except ShutdownInProgress:
            if drain_threads is not None:
                self.shutdown(reason="serve_startup_exit")
                self.drain_non_writer_components(drain_threads)
            return False
        return True

    def _run_startup_block_candidate_replay(self) -> bool:
        """Run best-effort pre-accept replay without dying on a slow ledger."""
        return self._ensure_block_candidate_service()._run_startup_block_candidate_replay()

    def accept_loop(self, server: socket.socket, profile: StratumListenerProfile) -> None:
        self._ensure_stratum_session_service().accept_loop(server, profile)

    def _record_stratum_resource_exhaustion(
        self,
        *,
        listener_name: str,
        location: str,
        error_number: int | None,
    ) -> int:
        return self._ensure_stratum_session_service().record_stratum_resource_exhaustion(
            listener_name=listener_name,
            location=location,
            error_number=error_number,
        )

    def _wait_after_stratum_resource_failure(self, heartbeat_name: str) -> None:
        self._ensure_stratum_session_service().wait_after_stratum_resource_failure(
            heartbeat_name
        )

    def _ensure_connection_capacity_state(self) -> None:
        self._ensure_stratum_session_service()

    def _note_connection_limit_rejection_locked(self, scope: str) -> int:
        return self._ensure_stratum_session_service().registry.note_rejection_locked(
            scope
        )

    def reserve_client_username(self, client: ClientState, worker: WorkerIdentity) -> bool:
        """Atomically reserve an exact Stratum username for one connection."""
        return self._ensure_stratum_session_service().reserve_client_username(
            client, worker
        )

    def start_audit_server(self) -> None:
        # Mark the nonblocking refreshers running before the listener can
        # dispatch a request: a probe retrying connection-refused queues in
        # the accept backlog at listen() time, and a handler serving it
        # before the flag is set would take cached_health_payload's legacy
        # inline path -- running the minutes-long accepted-share aggregate
        # on the handler thread and racing the refresher with a duplicate
        # query instead of returning the documented starting state.
        self.start_health_snapshot_refresher()
        self.start_metrics_snapshot_refresher()
        # Bind before the health snapshot warm-up completes: the cold
        # accepted-share aggregate can take minutes on a grown ledger, and
        # for that whole window the listener must answer with an explicit
        # starting state instead of connection refused (issue #188 fix 4).
        self._ensure_audit_http_facade().start()
        self._record_startup_phase_once("audit_listener_bound")
        print(
            f"prism coordinator: audit HTTP listening on {self.audit_bind}:{self.audit_port}",
            flush=True,
        )

    def apply_stratum_send_timeout(self, sock: socket.socket) -> None:
        """Bound blocking sends to miners without touching receive semantics.

        Job refreshes use a bounded executor, but an unresponsive peer whose
        TCP buffer is full must still release its worker eventually.
        SO_SNDTIMEO turns that into an OSError, which the refresh path treats
        as a dead client without failing delivery to other miners.
        A plain socket timeout is not usable here: it would also apply to
        recv, disconnecting idle-but-healthy miners.
        """
        apply_socket_send_timeout(
            sock,
            getattr(
                self,
                "stratum_send_timeout_seconds",
                DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS,
            ),
        )

    def _wait_for_blockpoll_trigger(self) -> bool:
        return self._ensure_tip_refresh_service()._wait_for_blockpoll_trigger()

    def blockpoll_loop(self) -> None:
        return self._ensure_tip_refresh_service().blockpoll_loop()

    def template_refresh_failure_expired(self, now: float) -> bool:
        return self._ensure_tip_refresh_service().template_refresh_failure_expired(now)

    def publication_progress_failure_expired(self, now: float) -> bool:
        return self._ensure_tip_refresh_service().publication_progress_failure_expired(
            now,
        )

    def _record_coordination_blocked_refresh(self, now: float) -> None:
        self._ensure_watchdog_service().record_coordination_blocked_refresh(now)

    def _clear_coordination_blocked_streak(self) -> None:
        self._ensure_watchdog_service().clear_coordination_blocked_streak()

    def coordination_blocked_streak_age_seconds(
        self,
        now: float | None = None,
    ) -> float:
        return (
            self._ensure_watchdog_service().coordination_blocked_streak_age_seconds(
                now
            )
        )

    def coordination_blocked_streak_expired(self, now: float) -> bool:
        return (
            self._ensure_watchdog_service().coordination_blocked_streak_expired(
                now
            )
        )

    def _publication_watchdog_state(
        self,
        now: float,
    ) -> tuple[str | None, float, float, float]:
        return self._ensure_watchdog_service().publication_watchdog_state(now)

    def _record_template_refresh_failure(self, now: float) -> None:
        return self._ensure_tip_refresh_service()._record_template_refresh_failure(now)

    def blockwait_once(self, known_tip: str) -> str:
        return self._ensure_tip_refresh_service().blockwait_once(known_tip)

    def blockwait_loop(self) -> None:
        return self._ensure_tip_refresh_service().blockwait_loop()

    @staticmethod
    def _blockwait_unsupported(exc: Exception) -> bool:
        detail = str(exc).lower()
        return (
            "-32601" in detail
            or "-32602" in detail
            or "method not found" in detail
            or "unknown method" in detail
            or "invalid params" in detail
            or "invalid parameter" in detail
            or "wrong number of" in detail
            or "too many parameters" in detail
            or "incorrect number of" in detail
        )

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
        return self._ensure_tip_refresh_service()._tip_refresh_artifacts(snapshot)

    def prepare_tip_refresh_bundle(
        self,
        snapshot: QbitTipTemplateSnapshot,
        *,
        priority_requested_monotonic: float | None = None,
    ) -> CachedJobBundle:
        return self._ensure_tip_refresh_service().prepare_tip_refresh_bundle(
            snapshot,
            priority_requested_monotonic=priority_requested_monotonic,
        )

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
        return self._ensure_tip_refresh_service()._tip_refresh_token_current_locked(
            token,
            bundle,
            snapshot,
        )

    def _tip_refresh_token_prepublication_current_locked(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return self._ensure_tip_refresh_service()._tip_refresh_token_prepublication_current_locked(
            token,
            bundle,
            snapshot,
        )

    def _tip_refresh_snapshot_current_locked(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> bool:
        return self._ensure_tip_refresh_service()._tip_refresh_snapshot_current_locked(
            snapshot,
            observation_sequence,
        )

    def _validate_prepared_tip_refresh(
        self,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> TipRefreshValidationToken:
        return self._ensure_tip_refresh_service()._validate_prepared_tip_refresh(
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
        return self._ensure_tip_refresh_service()._activate_tip_refresh(
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
        return self._ensure_tip_refresh_service()._publish_prepared_tip_refresh(
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
        return self._ensure_tip_refresh_service()._clear_active_tip_refresh(
            token,
            cancel_event,
        )

    def _prepared_tip_refresh_obsolete(
        self,
        validation_token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: _FanoutCancellation | None,
    ) -> bool:
        return self._ensure_tip_refresh_service()._prepared_tip_refresh_obsolete(
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
        return self._ensure_tip_refresh_service()._fanout_prepared_tip_refresh(
            clients,
            bundle,
            snapshot,
            observation_sequence=observation_sequence,
            validation_token=validation_token,
            preactivated_cancel_event=preactivated_cancel_event,
            executor=executor,
            expected_active_jobs=expected_active_jobs,
            heartbeat_name=heartbeat_name,
        )

    def _interrupted_wave_outcome(self, superseded_outcome: str) -> str:
        return self._ensure_tip_refresh_service()._interrupted_wave_outcome(
            superseded_outcome,
        )

    def _tip_refresh_wave_reenters(self, completed_passes: int) -> bool:
        return self._ensure_tip_refresh_service()._tip_refresh_wave_reenters(
            completed_passes,
        )

    def poll_qbit_tip_template_once(
        self,
        *,
        heartbeat_name: str = "qbit_blockpoll",
    ) -> int:
        return self._ensure_tip_refresh_service().poll_qbit_tip_template_once(
            heartbeat_name=heartbeat_name,
        )

    def _poll_qbit_tip_template_pass_once(
        self,
        *,
        heartbeat_name: str,
        refresh_started: float,
        observation_sequence: int | None = None,
        observed_best_tip: str | None = None,
    ) -> int:
        return self._ensure_tip_refresh_service()._poll_qbit_tip_template_pass_once(
            heartbeat_name=heartbeat_name,
            refresh_started=refresh_started,
            observation_sequence=observation_sequence,
            observed_best_tip=observed_best_tip,
        )

    def _probe_tip_while_refresh_waiting(self) -> None:
        return self._ensure_tip_refresh_service()._probe_tip_while_refresh_waiting()

    def _detected_tip_supersedes_locked(
        self,
        tip_hash: str,
        observation_sequence: int,
    ) -> bool:
        return self._ensure_tip_refresh_service()._detected_tip_supersedes_locked(
            tip_hash,
            observation_sequence,
        )

    def _raise_if_tip_refresh_superseded(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> None:
        return self._ensure_tip_refresh_service()._raise_if_tip_refresh_superseded(
            snapshot,
            observation_sequence,
        )

    def _reserve_tip_observation_sequence(self) -> int:
        return self._ensure_tip_refresh_service()._reserve_tip_observation_sequence()

    def observe_tip_for_refresh(
        self,
        tip_hash: str,
        *,
        observation_sequence: int | None = None,
        mark_pending: bool = True,
    ) -> bool:
        return self._ensure_tip_refresh_service().observe_tip_for_refresh(
            tip_hash,
            observation_sequence=observation_sequence,
            mark_pending=mark_pending,
        )

    def observe_tip_first_seen(
        self,
        tip_hash: str,
        *,
        observation_sequence: int | None = None,
        publish_refresh_observation: bool = False,
        published_snapshot: QbitTipTemplateSnapshot | None = None,
    ) -> bool:
        return self._ensure_tip_refresh_service().observe_tip_first_seen(
            tip_hash,
            observation_sequence=observation_sequence,
            publish_refresh_observation=publish_refresh_observation,
            published_snapshot=published_snapshot,
        )

    def _fetch_tip_parent_hash(self, tip_hash: str) -> str | None:
        return self._ensure_tip_refresh_service()._fetch_tip_parent_hash(tip_hash)

    def current_tip_parent_hash(self, tip_hash: str) -> str | None:
        return self._ensure_tip_refresh_service().current_tip_parent_hash(tip_hash)

    def submit_stale_check_tip(self) -> str:
        return self._ensure_tip_refresh_service().submit_stale_check_tip()

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
        return self._ensure_tip_refresh_service()._published_tip_authoritative_locked(
            now,
        )

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
        return self._ensure_job_delivery_service().note_tip_work_delivered(
            client,
            job_parent_hash,
        )

    def _ensure_evicted_job_state(self) -> None:
        # Compatibility initializer: the retained-job index is owned by
        # JobDeliveryService. Ensuring it adopts legacy/test-seeded graveyard
        # state (including pre-owner tuple entries) and rebuilds the
        # per-connection/same-tip/disconnected indexes for the current tip.
        self._ensure_job_delivery_service().ensure_evicted_job_state()

    def _current_published_tip_hash_locked(self) -> str | None:
        first_seen = getattr(self, "current_tip_first_seen", None)
        if first_seen is not None:
            return str(first_seen[0])
        snapshot = getattr(self, "tip_template_snapshot", None)
        if snapshot is not None:
            return str(snapshot.bestblockhash)
        return None

    def _evicted_job_class_locked(self, entry: EvictedJobEntry) -> str:
        return self._ensure_job_delivery_service()._evicted_job_class_locked(entry)

    def bury_evicted_job(
        self,
        client: ClientState,
        job_id: str,
        *,
        now: float | None = None,
        prune: bool = True,
    ) -> None:
        return self._ensure_job_delivery_service().bury_evicted_job(
            client,
            job_id,
            now=now,
            prune=prune,
        )

    def prune_evicted_job_graveyard(
        self,
        *,
        now: float | None = None,
        force: bool = True,
    ) -> None:
        return self._ensure_job_delivery_service().prune_evicted_job_graveyard(
            now=now,
            force=force,
        )

    def evicted_job_entry(
        self,
        client: ClientState,
        job_id: str,
    ) -> EvictedJobEntry | None:
        return self._ensure_job_delivery_service().evicted_job_entry(client, job_id)

    def evicted_submit_context(
        self,
        client: ClientState,
        entry: EvictedJobEntry,
        current_tip: str,
    ) -> tuple[PrismJobContext, str | None] | None:
        context = entry.context
        if str(context.template["previousblockhash"]) == current_tip:
            return context, None
        if entry.connection_id != client.connection_id:
            # Cross-connection resumes are same-tip only. A tip that moves
            # between the graveyard lookup and this classification must not
            # fall through to stale grace: that window anchors on the
            # submitting connection's delivery state, which says nothing
            # about work another (dead) connection delivered.
            return None
        if not self.context_eligible_for_stale_grace(client, context, current_tip):
            return None
        return context, PRISM_CREDIT_POLICY_STALE_GRACE

    def note_evicted_job_submit(
        self,
        credit_policy: str | None,
        *,
        cross_connection: bool = False,
    ) -> None:
        return self._ensure_job_delivery_service().note_evicted_job_submit(
            credit_policy,
            cross_connection=cross_connection,
        )

    def refresh_jobs_after_pending_accepted_block(
        self,
        client: ClientState,
        *,
        heartbeat_name: str = "qbit_blockpoll",
    ) -> int:
        return self._ensure_tip_refresh_service().refresh_jobs_after_pending_accepted_block(
            client,
            heartbeat_name=heartbeat_name,
        )

    def refresh_jobs_after_accepted_block(
        self, *, block_height: int, block_hash: str, heartbeat_name: str = "qbit_blockpoll"
    ) -> int:
        return self._ensure_tip_refresh_service().refresh_jobs_after_accepted_block(
            block_height=block_height,
            block_hash=block_hash,
            heartbeat_name=heartbeat_name,
        )

    def fetch_qbit_tip_template_snapshot(self) -> QbitTipTemplateSnapshot:
        return self._ensure_job_bundle_service().template_repository.fetch_coherent_snapshot()

    def _note_reorg_reconcile_outcome(
        self,
        tip_hash: str | None,
        *,
        trusted: bool,
        clear_memo: bool = False,
        evict_others: bool = False,
        proof_epoch: int | None = None,
    ) -> None:
        """Compatibility seam for the extracted reorg reconciler owner."""
        self._ensure_reorg_reconciler_service().note_outcome(
            tip_hash,
            trusted=trusted,
            clear_memo=clear_memo,
            evict_others=evict_others,
            proof_epoch=proof_epoch,
        )

    def _evict_reorg_reconcile_memo_for_new_tip_locked(
        self,
        tip_hash: str,
    ) -> None:
        self._ensure_reorg_reconciler_service().evict_memo_for_new_tip_locked(
            tip_hash
        )

    def ensure_reorg_reconciled_for_current_tip(
        self,
        *,
        expected_tip_hash: str | None = None,
    ) -> bool:
        return self._ensure_reorg_reconciler_service().ensure_current(
            expected_tip_hash=expected_tip_hash
        )

    def _reorg_reconcile_memo_fresh(self, tip_hash: str) -> bool:
        return self._ensure_reorg_reconciler_service().memo_fresh(tip_hash)

    def _record_reorg_reconcile_lookup(self, path: str, source: str) -> None:
        self._ensure_reorg_reconciler_service().record_lookup(path, source)

    @property
    def reorg_reconcile_lookup_counts(self) -> dict[tuple[str, str], int]:
        """Copied lookup counters from the reorg owner (metrics consumer)."""
        return (
            self._ensure_reorg_reconciler_service()
            .reorg_reconcile_lookup_snapshot()
        )

    def _submit_reconcile_prefetch(
        self,
        tip_hash: str,
        *,
        prove: bool = False,
    ) -> Future[bool] | None:
        return self._ensure_reorg_reconciler_service().submit_prefetch(
            tip_hash,
            prove=prove,
        )

    @staticmethod
    def _discard_stale_reconcile_prefetch(future: Future[bool]) -> None:
        ReorgReconcilerService.discard_stale_prefetch(future)

    def _join_reconcile_prefetch_bounded(self, prefetch: Future[bool]) -> bool:
        return self._ensure_reorg_reconciler_service().join_prefetch_bounded(
            prefetch
        )

    def _reconcile_snapshot_tip_bounded(self, tip_hash: str) -> bool:
        return self._ensure_reorg_reconciler_service().snapshot_tip_bounded(
            tip_hash
        )

    def shutdown_reconcile_prefetch_executor(self) -> None:
        self._ensure_reorg_reconciler_service().shutdown_prefetch_executor()

    def ensure_reorg_reconciled_for_tip(
        self,
        tip_hash: str,
        *,
        _coalesce_same_tip: bool = True,
    ) -> bool:
        """Reconcile one tip, optionally bypassing same-tip flight reuse.

        Lock-owning accepted-block callers disable waiting for an existing
        leader because it may itself be waiting for the payout-balance
        mutation lock. Their own pass remains visible to ordinary followers.
        """
        return self._ensure_reorg_reconciler_service().ensure_tip(
            tip_hash,
            _coalesce_same_tip=_coalesce_same_tip,
        )

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

        expected_genesis = env_optional("QBIT_EXPECTED_GENESIS_HASH")
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
        minimum_peers = env_positive_int("PRISM_MIN_PEERS", 1)
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
        max_age = env_nonnegative_int(
            "PRISM_TEMPLATE_MAX_AGE_SECONDS",
            DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
        )
        template_age = int(time.time()) - template_time
        if template_age > max_age:
            raise RuntimeError(
                f"qbit block template is stale: age={template_age}s exceeds {max_age}s"
            )
        self.last_successful_template_refresh_monotonic = time.monotonic()

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

    def reconcile_prism_pool_blocks_once(
        self,
        *,
        tip_hash: str | None = None,
        _force_publish: bool = False,
        _source_reserved: bool = False,
        _wait_for_same_tip_flight: bool = True,
    ) -> dict[str, object]:
        """Public reconcile entrypoint: service flight admission runs first.

        Same-tip flight coalescing happens *before* the serialized adapter's
        writer admission so followers never queue behind it; see the service
        docstring for the full contract.
        """
        return self._ensure_reorg_reconciler_service().reconcile_with_flights(
            tip_hash=tip_hash,
            _force_publish=_force_publish,
            _source_reserved=_source_reserved,
            _wait_for_same_tip_flight=_wait_for_same_tip_flight,
        )

    @ledger_writer_operation("payout_reconciliation")
    def _reconcile_prism_pool_blocks_serialized(
        self,
        *,
        tip_hash: str | None = None,
        _force_publish: bool = False,
        _source_reserved: bool = False,
    ) -> dict[str, object]:
        """Serialize reconciliation against accepted-block finalization.

        Cross-owner adapter retained by the coordinator: writer-admission
        decorator, then the direct payout-balance mutation lock, then the
        landed-preview fail-closed check, then the service core. Never
        replace this sequence with the _payout_balance_mutation() wrapper.
        """
        self._ensure_job_cache_state()
        with self._payout_balance_mutation_lock:
            with self._accepted_block_payout_preview_condition:
                if any(
                    transition.landed
                    for transition in self._accepted_block_payout_previews.values()
                ):
                    raise PayoutStatePublicationBlocked(
                        "accepted block payout confirmation is still pending"
                    )
            return self._ensure_reorg_reconciler_service().reconcile(
                tip_hash=tip_hash,
                force_publish=_force_publish,
                source_reserved=_source_reserved,
            )

    def client_can_receive_jobs(self, client: ClientState) -> bool:
        return self._ensure_job_delivery_service().client_can_receive_jobs(client)

    def pool_readiness_latched(self) -> bool:
        """Latch, once, the transition past min_ready_miners.

        Readiness is monotonic (a lifetime distinct-accepted-miner count), so
        a single observation is permanent and later checks stay ledger-free.
        The poll loop refreshes the latch outside the coordinator lock.
        """
        if getattr(self, "_pool_ready_latched", False):
            return True
        try:
            _, ready_miner_count = self.accepted_share_stats()
        except Exception:
            return False
        if ready_miner_count >= getattr(self, "min_ready_miners", 3):
            became_ready = False
            with self.lock:
                if not getattr(self, "_pool_ready_latched", False):
                    self._pool_ready_latched = True
                    self._retained_collection_refresh = None
                    became_ready = True
            if became_ready:
                # Collection work becomes obsolete without changing the qbit
                # template fingerprint or payout generation. Start the health
                # deadline at the readiness transition itself.
                self._progress_note_refresh_pending()
            return True
        return False

    def client_needs_tip_template_refresh(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return self._ensure_job_delivery_service().client_needs_tip_template_refresh(
            client,
            snapshot,
        )

    def intervening_job_supersedes_snapshot(
        self,
        active_job: PrismJobContext | None,
        expected_active_job: PrismJobContext | None,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return self._ensure_job_delivery_service().intervening_job_supersedes_snapshot(
            active_job,
            expected_active_job,
            snapshot,
        )

    def client_tip_changed_for_snapshot(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return self._ensure_job_delivery_service().client_tip_changed_for_snapshot(
            client,
            snapshot,
        )

    def handle_client(self, client: ClientState) -> None:
        self._ensure_stratum_session_service().handle_client(client)

    def disconnect_client(self, client: ClientState) -> None:
        # Retire admission and fanout eligibility without waiting behind job
        # delivery. Only the first caller owns socket close and final cleanup.
        self._ensure_stratum_session_service().disconnect_client(client)

    def handle_request(self, client: ClientState, request: dict[str, object]) -> None:
        """Dispatch one request, translating shutdown races to Stratum errors."""
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
        P2mrAddressValidator._raise_shared_error(error)

    def _ensure_p2mr_address_cache_state(self) -> None:
        # The validator (created on first touch) owns the cache trio; legacy
        # assignments through the descriptor-routed names are adopted by it.
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
        commit_guard: Callable[[], bool] | None = None,
        commit_guard_lock: threading.RLock | None = None,
        prepared_bundle_allow_uncached: bool = False,
    ) -> bool:
        return self._ensure_job_delivery_service()._maybe_send_job_locked(
            client,
            clean_jobs=clean_jobs,
            raise_on_reorg_failure=raise_on_reorg_failure,
            raise_on_build_failure=raise_on_build_failure,
            tip_refresh_snapshot=tip_refresh_snapshot,
            tip_refresh_observation_sequence=tip_refresh_observation_sequence,
            prepared_bundle=prepared_bundle,
            commit_guard=commit_guard,
            commit_guard_lock=commit_guard_lock,
            prepared_bundle_allow_uncached=prepared_bundle_allow_uncached,
        )

    def prune_client_active_jobs(self, client: ClientState) -> None:
        return self._ensure_job_delivery_service().prune_client_active_jobs(client)

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

    def resume_client_difficulty(self, client: ClientState) -> Decimal | None:
        return self._ensure_vardiff_service().apply_resumed_difficulty(client)

    def note_vardiff_resume_overridden(self) -> None:
        self._ensure_vardiff_service().note_resume_overridden()

    def record_session_difficulty(self, client: ClientState) -> None:
        # Only a connection that produced an accepted share may refresh the
        # retained value's TTL. Without that evidence a reconnect loop of
        # silent sessions would re-stamp the entry forever and defeat the TTL.
        self._ensure_vardiff_service().record_session_difficulty(
            client,
            share_backed=bool(getattr(client, "vardiff_accepted_any", False)),
        )

    def apply_job_difficulty(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        return self._ensure_job_delivery_service().apply_job_difficulty(client, job)

    def apply_client_difficulty_requests(self, client: ClientState) -> Decimal | None:
        return self._ensure_job_delivery_service().apply_client_difficulty_requests(
            client,
        )

    def advertise_client_difficulty(self, client: ClientState, target: Decimal) -> bool:
        return self._ensure_job_delivery_service().advertise_client_difficulty(
            client,
            target,
        )

    def _advertise_client_difficulty_locked(
        self,
        client: ClientState,
        target: Decimal,
    ) -> bool:
        return self._ensure_job_delivery_service()._advertise_client_difficulty_locked(
            client,
            target,
        )

    def normalized_prior_balances(self, balances: list[dict[str, object]]) -> list[dict[str, object]]:
        return self._ensure_payout_state_service().normalized_prior_balances(balances)

    @staticmethod
    def settlement_balances_by_program(balances: list[dict[str, object]]) -> dict[str, int]:
        """Aggregate owed balances by P2MR program for settlement equality."""
        return PayoutStateService.settlement_balances_by_program(balances)

    def prior_balances_match_current(self, prior_balances: list[dict[str, object]]) -> bool:
        ensure = getattr(self, "_ensure_payout_state_service", None)
        if ensure is not None:
            return ensure().prior_balances_match_current(prior_balances)
        # Unbound duck-typed callers (the settlement comparison tests) supply
        # only settlement_balances_by_program and the ledger; keep the exact
        # #87 settlement-equality comparison for them.
        return self.settlement_balances_by_program(
            prior_balances
        ) == self.settlement_balances_by_program(
            self.ledger.current_prior_balances()
        )

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
        return self._ensure_job_delivery_service().send_job_update(client, job)

    def build_job_for_client(self, client: ClientState, *, clean_jobs: bool) -> PrismJobContext:
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
        publication_critical: bool = False,
        request_source: str = "routine",
    ) -> PrismJobContext:
        return self._ensure_job_delivery_service().build_job_for_client_from_artifacts(
            client,
            artifacts,
            clean_jobs=clean_jobs,
            publication_critical=publication_critical,
            request_source=request_source,
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

    def shutdown_serve_builder(self) -> None:
        """Retire the persistent audit builder; builds revert to one-shot."""
        self._ensure_bundle_compiler().shutdown_serve_builder()

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
        share_serialization: _ShareWindowSerialization | None = None,
        append_invalidation_epoch: int | None = None,
    ) -> dict[str, Any]:
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
            share_serialization=share_serialization,
            append_invalidation_epoch=append_invalidation_epoch,
        )

    def prepare_payout_window(
        self,
        *,
        mode: str,
        records_json: list[dict[str, object]],
        anchor_job_issued_at_ms: int,
        append_invalidation_epoch: int,
        window_weight: int | None = None,
        page_size: int | None = None,
        base_digest: str | None = None,
        wait_for_daemon: bool = True,
    ) -> Any:
        """Fold/advance one payout window through the persistent builder."""
        return self._ensure_bundle_compiler().prepare_payout_window(
            mode=mode,
            records_json=records_json,
            anchor_job_issued_at_ms=anchor_job_issued_at_ms,
            append_invalidation_epoch=append_invalidation_epoch,
            window_weight=window_weight,
            page_size=page_size,
            base_digest=base_digest,
            wait_for_daemon=wait_for_daemon,
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
        """Land one below-target block synchronously with its share credit.

        The hash solved a block but missed the assigned share target
        (possible only while the listener floor sits above network
        difficulty). It is a valid share ONLY if the block lands, so land
        it synchronously: the miner's accept/reject and the ledger credit
        then both reflect the real outcome -- never an "accepted" ack with
        no ledger row. This path is rare (an honest miner does not submit
        below its assigned target), so it does not affect the async
        common-path latency. On failure the submitter already recorded the
        specific block-failure reason in block_candidate_abandoned_counts;
        reject_stratum additionally counts the miner-facing rejection
        (globally and per worker) so this rare synchronous path is not
        missing from the rejection metrics.
        """
        submission = candidate.submission
        pending_share = candidate.pending_share
        persist_intent = getattr(self.ledger, "persist_block_candidate_intent", None)
        durable_candidate_state: str | None = None
        try:
            candidate_intent = self.block_candidate_intent(candidate)
            if callable(persist_intent):
                persist_result = self._run_block_submitter_ledger_call(
                    (
                        "persist-candidate-intent",
                        str(submission.block_hash_hex).lower(),
                    ),
                    "persist-candidate-intent",
                    lambda: persist_intent(candidate_intent),
                )
                result_state = getattr(persist_result, "state", None)
                if result_state is not None:
                    durable_candidate_state = str(result_state)
        except BaseException:
            # No retry slot is safe until the pre-submit outbox boundary is
            # durable. Let the miner retry this submission instead. Without
            # a durable intent nothing can commit this stamped share, so
            # stop holding the snapshot anchor floor under it.
            self._finish_pending_share_commit(pending_share)
            self._release_submit_share_key(share_key)
            raise
        if durable_candidate_state in {"submitted", "abandoned"}:
            # A process restart clears the in-memory disposition cache,
            # but the existing outbox row remains authoritative. Join its
            # terminal result before any new raw node offer.
            block_landed = durable_candidate_state == "submitted"
            self._record_block_candidate_terminal_outcome(
                submission.block_hash_hex,
                accepted=block_landed,
            )
            self._finish_pending_share_commit(pending_share)
        else:
            try:
                block_landed = self._submit_synchronous_block_candidate(candidate)
            except BaseException:
                self._release_submit_share_key(share_key)
                raise
        if not block_landed:
            self._release_submit_share_key(share_key)
            self.reject_stratum(
                23,
                PRISM_REJECTION_LOW_DIFFICULTY,
                "low difficulty share",
                worker=worker_name,
            )
        elif evicted_entry is not None:
            self.note_evicted_job_submit(
                credit_policy,
                cross_connection=(
                    evicted_entry.connection_id
                    != candidate.client.connection_id
                ),
            )
        return False

    def block_candidate_intent(self, candidate: PrismBlockCandidate) -> dict[str, Any]:
        """Return the immutable JSON needed to resume a candidate after restart.

        The durable JSON boundary is where a daemon-mirror share sequence is
        forced to real dicts, so it is also where a refuted mirror would
        first be seen on the landing path. Route it before it propagates:
        the candidate cannot be persisted from a window the coordinator no
        longer trusts, and the next build must not resume from that window
        either.
        """
        try:
            return encode_block_candidate_intent(candidate)
        except DaemonWindowMirrorDivergence:
            self._note_window_mirror_divergence()
            raise

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
        # The S3 owner (created on first touch) holds the floor lock and the
        # pending-commit floor; legacy assignments through the descriptor
        # routed names are adopted by it.
        self._ensure_share_writer_service()

    def _finish_pending_share_commit(self, pending_share: PendingShare) -> None:
        """Drop a share from the snapshot anchor floor."""
        self._ensure_share_writer_service().finish_pending_share(pending_share)

    def _finish_pending_share_attempt(self, pending_share: PendingShare) -> None:
        """Release one stamped submission attempt's floor authority (seam)."""
        self._ensure_share_writer_service().finish_pending_attempt(pending_share)

    def _finish_pending_share_candidate(self, pending_share: PendingShare) -> None:
        """Release the durable candidate actor's floor authority (seam)."""
        self._ensure_share_writer_service().finish_pending_candidate(pending_share)

    def _job_snapshot_anchor_ms(self, issued_at_ms: int) -> int:
        """Clamp a share-snapshot anchor below every coverable share stamp."""
        return self._ensure_share_writer_service().snapshot_anchor_ms(issued_at_ms)

    def pending_share_from_submission(
        self,
        *,
        context: PrismJobContext,
        submission: direct_stratum.DirectQbitSubmission,
        ntime_hex: str,
        credit_policy: str | None = None,
    ) -> PendingShare:
        return self._ensure_share_writer_service().pending_share_from_submission(
            context=context,
            submission=submission,
            ntime_hex=ntime_hex,
            credit_policy=credit_policy,
        )

    def _expose_inflight_scan_anchor(self, anchor_ms: int) -> int:
        """Publish a ledger walk's anchor to the append-side invalidation."""
        return self._ensure_payout_state_service()._expose_inflight_scan_anchor(anchor_ms)

    def _retire_inflight_scan_anchor(self, token: int | None) -> None:
        """Withdraw an exposed walk anchor once its result became visible"""
        return self._ensure_payout_state_service()._retire_inflight_scan_anchor(token)

    def _publish_seedless_job_window_anchor_locked(self, anchor_ms: int) -> None:
        """Keep a seedless published window's anchor visible to appends."""
        return self._ensure_payout_state_service()._publish_seedless_job_window_anchor_locked(anchor_ms)

    @staticmethod
    def _pending_share_predates_anchor(pending_share: PendingShare, anchor_ms: int | None) -> bool:
        return PayoutStateService._pending_share_predates_anchor(pending_share, anchor_ms)

    def _pending_share_predates_live_anchor_locked(self, pending_share: PendingShare) -> bool:
        return self._ensure_payout_state_service()._pending_share_predates_live_anchor_locked(pending_share)

    def _landing_fence_for_predating_append(self, pending_shares: list[PendingShare]) -> Iterator[bool]:
        """Hold the landing fence across a durable append of predating rows."""
        return self._ensure_payout_state_service()._landing_fence_for_predating_append(pending_shares)

    def _await_unfenced_appends_predating_anchor(self, anchor_ms: int) -> None:
        """Wait until no unfenced in-flight append can predate ``anchor_ms``."""
        return self._ensure_payout_state_service()._await_unfenced_appends_predating_anchor(anchor_ms)

    def _record_late_visible_payout_append(self, pending_share: PendingShare, *, landing_fence_owned: bool=False) -> int | None:
        """Advance the append-invalidation epoch for a newly durable row."""
        return self._ensure_payout_state_service()._record_late_visible_payout_append(pending_share, landing_fence_owned=landing_fence_owned)

    def _append_epoch_invalidated_declared_anchor(self, *, baseline_epoch: int, live_epoch: int, declared_anchor_ms: int | None) -> bool:
        """Whether an append between two epochs invalidated this declared anchor."""
        return self._ensure_payout_state_service()._append_epoch_invalidated_declared_anchor(
            baseline_epoch=baseline_epoch,
            live_epoch=live_epoch,
            declared_anchor_ms=declared_anchor_ms,
        )

    def _invalidate_incremental_payout_window_for_append(self, pending_share: PendingShare) -> None:
        """Disarm payout work that predates a newly visible eligible row."""
        return self._ensure_payout_state_service()._invalidate_incremental_payout_window_for_append(pending_share)

    def _retire_payout_windows_for_late_append(self, pending_share: PendingShare, invalidation_epoch: int) -> None:
        """Disarm cached payout windows after a late-append epoch bump."""
        return self._ensure_payout_state_service()._retire_payout_windows_for_late_append(pending_share, invalidation_epoch)

    @ledger_writer_operation("share_persistence")
    def append_accepted_share(
        self,
        client: ClientState,
        context: PrismJobContext,
        submission: direct_stratum.DirectQbitSubmission,
        pending_share: PendingShare,
        *,
        credit_policy: str | None = None,
        candidate_intent: dict[str, Any] | None = None,
    ) -> str | None:
        return self._ensure_share_writer_service().append_accepted_share(
            client,
            context,
            submission,
            pending_share,
            credit_policy=credit_policy,
            candidate_intent=candidate_intent,
        )

    def enqueue_share_append(self, entry: PendingShareAppend, *, wait: bool = False) -> None:
        self._ensure_share_writer_service().enqueue_share_append(entry, wait=wait)

    def share_append_loop(self) -> None:
        self._ensure_share_writer_service().share_append_loop()

    def _append_share_batch(self, batch: list[PendingShareAppend]) -> bool:
        """Commit a writer batch, then release every waiting submitter."""
        return self._ensure_share_writer_service().append_share_batch(batch)

    def _recover_share_to_disk(self, entry: PendingShareAppend, reason: str) -> None:
        self._ensure_share_writer_service().recover_share_to_disk(entry, reason)

    @ledger_writer_operation("share_recovery_replay")
    def replay_recovered_shares(self) -> int:
        """Replay any recovery-file shares into the ledger at startup."""
        return self._ensure_share_writer_service().replay_recovered_shares()

    def _append_share_entry(self, entry: PendingShareAppend, *, retry_until_stopped: bool = False) -> bool:
        return self._ensure_share_writer_service().append_share_entry(
            entry, retry_until_stopped=retry_until_stopped
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

    def _idle_bundle_current_locked(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        *,
        allow_uncached: bool = False,
    ) -> bool:
        """Check one exact issuance observation. Caller holds ``_job_cache_lock``.

        Ready bundles can reuse an older same-tip heavy cache entry, but the
        prepared copy must be rebound to the current template artifacts before
        delivery. During a detected-but-unpublished refresh, issuance stays
        pinned to the published snapshot. Accept that copy only when it still
        shares the immutable heavy payload with the cache entry from which it
        was derived.
        """
        artifacts = self._idle_job_issuance_artifacts_locked()
        if artifacts is None or self._payout_state_publication_blocked:
            return False
        payout_artifact = self._published_payout_state.artifact
        if (
            bundle.build_key is None
            or payout_artifact is None
            or (
                not bundle.collection_only
                and int(
                    getattr(
                        bundle.build_key,
                        "payout_append_invalidation_epoch",
                        0,
                    )
                )
                != self._payout_ledger_append_invalidation_epoch
            )
            or bundle.build_key.payout_artifact_sha256
            != payout_artifact.prior_balances_sha256
        ):
            return False
        observed_tip = getattr(self, "current_tip_first_seen", None)
        if observed_tip is not None and observed_tip[0] != artifacts.previousblockhash:
            return False
        if (
            bundle.template_fingerprint != artifacts.fingerprint
            or bundle.payout_state_generation != self._payout_state_generation
            or str(bundle.template.get("previousblockhash", ""))
            != artifacts.previousblockhash
        ):
            return False
        if not bundle.collection_only:
            # Idle issuance is a NEW serving decision, so the audit ceiling
            # applies here exactly as it does at the reuse probe and the
            # cached-bundle lookup -- without this gate the idle fast path
            # was the one issuance route that could hand out a window past
            # the ceiling.
            found_block = getattr(bundle, "found_block", None)
            declared_anchor_ms = (
                found_block.get("anchor_job_issued_at_ms")
                if isinstance(found_block, dict)
                else None
            )
            if declared_anchor_ms is not None and (
                now_ms() - int(declared_anchor_ms)
                > self._payout_artifact_max_anchor_age_ms()
            ):
                return False
        published_tip = self._published_payout_state.source_tip_hash
        if published_tip is not None and published_tip != artifacts.previousblockhash:
            return False
        worker = client.worker
        if worker is None:
            return False
        mode = "ready" if getattr(self, "_pool_ready_latched", False) else "collection"
        if bundle.collection_only != (mode == "collection"):
            return False
        if (
            bundle.template is not artifacts.template
            or bundle.template_generation != artifacts.generation
        ):
            # Collection manifests sign the template ntime, while ready
            # bundles can cheaply rebind their base job. Either way, the
            # object stamped for delivery must represent this observation.
            return False
        key = self._job_bundle_key(
            artifacts,
            mode=mode,
            payout_state_generation=self._payout_state_generation,
            payout_artifact_generation=bundle.payout_artifact_generation,
            worker=worker,
        )
        ttl = float(
            getattr(
                self,
                "job_bundle_cache_seconds",
                DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS,
            )
        )
        if ttl <= 0:
            # A zero TTL deliberately disables global bundle retention. The
            # dedicated worker may still deliver the exact bundle it just
            # built after every live template/payout/client guard above has
            # passed; the cache-only sweep never opts into this exception.
            return allow_uncached and bundle.key == key
        cached = self._job_bundle_cache.get(key)
        if cached is None:
            return False
        if cached is not bundle and not (
            bundle.key == cached.key
            and bundle.coinbase_manifest is cached.coinbase_manifest
            and bundle.shares_json is cached.shares_json
            and bundle.prior_balances is cached.prior_balances
            and bundle.found_block is cached.found_block
            and bundle.collection_only == cached.collection_only
            and bundle.issued_at_ms == cached.issued_at_ms
            and bundle.built_monotonic == cached.built_monotonic
            and bundle.payout_state_generation
            == cached.payout_state_generation
            and bundle.payout_artifact_generation
            == cached.payout_artifact_generation
            and bundle.collection_identity == cached.collection_identity
        ):
            return False
        return time.monotonic() - cached.built_monotonic <= ttl

    def _idle_job_issuance_artifacts_locked(
        self,
    ) -> CachedTemplateArtifacts | None:
        """Cache-only counterpart of ``job_issuance_template_artifacts``.

        The idle sweep and its final delivery guard cannot fetch a template.
        They must still honor the published-snapshot pin used by every other
        direct issuance path while a replacement tip is detected but has not
        yet been published.
        """
        artifacts = self._template_artifacts
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
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            artifacts = self._idle_job_issuance_artifacts_locked()
            worker = client.worker
            if artifacts is None or worker is None:
                return None
            mode = "ready" if getattr(self, "_pool_ready_latched", False) else "collection"
            payout_artifact = getattr(self, "_payout_ledger_artifact", None)
            payout_artifact_generation = (
                payout_artifact.generation
                if mode == "ready"
                and payout_artifact is not None
                and payout_artifact.payout_state_generation
                == self._payout_state_generation
                and payout_artifact.network_difficulty
                == artifacts.network_difficulty
                else 0
            )
            key = self._job_bundle_key(
                artifacts,
                mode=mode,
                payout_state_generation=self._payout_state_generation,
                payout_artifact_generation=payout_artifact_generation,
                worker=worker,
            )
            bundle = self._job_bundle_cache.get(key)
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
        """Whether detected tip work still lacks published submit authority."""
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

    def _ensure_block_replay_state(self) -> None:
        """Backfill replay/maintenance queues for lightweight coordinators."""
        self._ensure_block_candidate_service()._ensure_block_replay_state()

    def _enqueue_replayed_block_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> bool:
        """Queue one durable replay behind live solves, once per process."""
        return self._ensure_block_candidate_service()._enqueue_replayed_block_candidate(
            candidate
        )

    def _queue_invalid_block_candidate_for_quarantine(
        self,
        block_hash: str,
        error: str,
        *,
        pending_share: PendingShare | None = None,
    ) -> None:
        """Move malformed-row cleanup off the node-offer lane."""
        self._ensure_block_candidate_service()._queue_invalid_block_candidate_for_quarantine(
            block_hash,
            error,
            pending_share=pending_share,
        )

    @ledger_writer_operation("accepted_block_handling")
    def replay_pending_block_candidates(self) -> int:
        """Queue durable candidate intents not completed by an earlier process."""
        return self._ensure_block_candidate_service().replay_pending()

    def _ensure_block_submitter_retry_state(self) -> None:
        self._ensure_block_candidate_service()

    def _ensure_block_submitter_ledger_call_state(self) -> None:
        self._ensure_block_candidate_service()._ensure_block_submitter_ledger_call_state()

    def _block_submitter_db_timeout(self) -> float:
        return self._ensure_block_candidate_service()._block_submitter_db_timeout()

    def _block_landing_db_timeout(self, block_hash: str | None = None) -> float:
        """Landing-class deadline, escalated after observed landing timeouts."""
        return self._ensure_block_candidate_service()._block_landing_db_timeout(
            block_hash
        )

    def _note_block_landing_timeout(self, block_hash: str | None) -> None:
        self._ensure_block_candidate_service()._note_block_landing_timeout(block_hash)

    def _ensure_block_ledger_call_metrics(self) -> None:
        self._ensure_block_candidate_service()._ensure_block_ledger_call_metrics()

    def _record_block_ledger_call(
        self,
        *,
        call_class: str,
        budget_seconds: float,
        duration_seconds: float,
        timed_out: bool,
    ) -> None:
        """Track per-call-class submitter ledger latency and timeout counts."""
        self._ensure_block_candidate_service()._record_block_ledger_call(
            call_class=call_class,
            budget_seconds=budget_seconds,
            duration_seconds=duration_seconds,
            timed_out=timed_out,
        )

    def block_ledger_call_class_metrics(self) -> dict[str, dict[str, float | int]]:
        return self._ensure_block_candidate_service().block_ledger_call_class_metrics()

    def block_candidate_collapse_snapshot(self) -> dict[str, int]:
        return self._ensure_block_candidate_service().block_candidate_collapse_snapshot()

    def _block_submitter_stuck_call_exit_timeout(self) -> float:
        return self._ensure_block_candidate_service()._block_submitter_stuck_call_exit_timeout()

    def _maybe_restart_for_stuck_block_call(
        self,
        *,
        kind: str,
        started_monotonic: float,
    ) -> None:
        """Fail stop when a poisoned worker pool stays exhausted."""
        self._ensure_block_candidate_service()._maybe_restart_for_stuck_block_call(
            kind=kind,
            started_monotonic=started_monotonic,
        )

    def _maybe_restart_for_exhausted_block_call_pool(
        self,
        *,
        kind: str,
        calls_lock: threading.Lock,
        calls: dict[Any, Any],
    ) -> None:
        """Age an exhausted pool even when retries reuse existing calls."""
        self._ensure_block_candidate_service()._maybe_restart_for_exhausted_block_call_pool(
            kind=kind,
            calls_lock=calls_lock,
            calls=calls,
        )

    def _block_submitter_ledger_timeout_scope(
        self,
        timeout_seconds: float | None = None,
    ) -> Iterator[None]:
        """Apply the submitter's PostgreSQL deadline when the ledger supports it."""
        return self._ensure_block_candidate_service()._block_submitter_ledger_timeout_scope(
            timeout_seconds
        )

    def _block_submitter_ledger_statement_timeout_scope(self) -> Iterator[None]:
        """Give each post-submit ledger step a fresh short deadline."""
        return self._ensure_block_candidate_service()._block_submitter_ledger_statement_timeout_scope()

    def _block_landing_ledger_statement_timeout_scope(
        self,
        block_hash: str | None = None,
    ) -> Iterator[None]:
        """Give each landing-class ledger step the landing budget."""
        return self._ensure_block_candidate_service()._block_landing_ledger_statement_timeout_scope(
            block_hash
        )

    def _run_block_submitter_ledger_call(
        self,
        key: tuple[object, ...],
        phase: str,
        operation: Callable[[], Any],
        *,
        timeout_seconds: float | None = None,
        call_class: str = "fast",
    ) -> Any:
        """Run one direct outbox call without letting its driver wedge us."""
        return self._ensure_block_candidate_service()._run_block_submitter_ledger_call(
            key,
            phase,
            operation,
            timeout_seconds=timeout_seconds,
            call_class=call_class,
        )

    def _wait_for_block_candidate_retry(self, delay_seconds: float) -> bool:
        """Wait for intentional backoff without impersonating stuck work.

        Only this bounded retry wait refreshes the submitter heartbeat by
        itself; SQL, RPC and lock phases go through their own phase-aware
        helpers, so a genuinely blocked candidate phase remains
        watchdog-eligible.
        """
        return self._ensure_block_candidate_service().wait_for_retry(delay_seconds)

    def _mark_block_candidate_attempted(self, block_hash: str) -> None:
        self._ensure_block_candidate_service().mark_attempted(block_hash)

    def _rpc_call_with_timeout(
        self,
        method: str,
        params: list[object],
        *,
        timeout_seconds: float,
    ) -> Any:
        """Pass an explicit timeout to production RPCs and capable test doubles."""
        return self._ensure_block_candidate_service()._rpc_call_with_timeout(
            method,
            params,
            timeout_seconds=timeout_seconds,
        )

    def _ensure_block_submitter_rpc_call_state(self) -> None:
        self._ensure_block_candidate_service()._ensure_block_submitter_rpc_call_state()

    def _run_submitblock_rpc_with_hard_deadline(
        self,
        *,
        block_hash: str,
        block_hex: str,
        timeout_seconds: float,
    ) -> Any:
        """Bound wall time even when an RPC adapter ignores its timeout."""
        return self._ensure_block_candidate_service()._run_submitblock_rpc_with_hard_deadline(
            block_hash=block_hash,
            block_hex=block_hex,
            timeout_seconds=timeout_seconds,
        )

    def _arm_block_candidate_after_node_offer(
        self,
        candidate: PrismBlockCandidate,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> None:
        """Fence child payout work as soon as node acceptance is possible."""
        self._ensure_block_candidate_service()._arm_block_candidate_after_node_offer(
            candidate,
            node_submission,
        )

    def _submit_block_candidate_to_node(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Offer the durable candidate to qbitd before any accounting work."""
        return self._ensure_block_candidate_service()._submit_block_candidate_to_node(
            candidate
        )

    def _node_submission_for_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Choose the node fast lane unless the pool was already closed."""
        return self._ensure_block_candidate_service()._node_submission_for_candidate(
            candidate
        )

    def _node_submission_for_candidate_or_retained(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Reuse a retained definitive acceptance instead of re-offering."""
        return self._ensure_block_candidate_service()._node_submission_for_candidate_or_retained(
            candidate
        )

    def _node_submission_for_direct_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Preserve active-replay semantics for non-queue embedders."""
        return self._ensure_block_candidate_service()._node_submission_for_direct_candidate(
            candidate
        )

    def _account_block_candidate_after_node_submit(
        self,
        candidate: PrismBlockCandidate,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> bool:
        """Pass fast-lane evidence while tolerating legacy test embedders."""
        return self._ensure_block_candidate_service()._account_block_candidate_after_node_submit(
            candidate,
            node_submission,
        )

    def _submit_synchronous_block_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> bool:
        """Run the rare miner-facing path under one same-hash disposition."""
        return self._ensure_block_candidate_service()._submit_synchronous_block_candidate(
            candidate
        )

    def _ensure_block_candidate_disposition_state(self) -> None:
        """Backfill same-hash submission guards for lightweight embedders."""
        self._ensure_block_candidate_service()._ensure_block_candidate_disposition_state()

    def _claim_block_candidate_disposition(
        self,
        block_hash: str,
        *,
        blocking: bool,
    ) -> _BlockCandidateDispositionLease | None:
        """Claim one hash without making unrelated node offers wait."""
        return self._ensure_block_candidate_service()._claim_block_candidate_disposition(
            block_hash,
            blocking=blocking,
        )

    def _drop_block_candidate_disposition_user(
        self,
        key: str,
        flight: _BlockCandidateDispositionFlight,
    ) -> None:
        self._ensure_block_candidate_service()._drop_block_candidate_disposition_user(
            key,
            flight,
        )

    def _release_block_candidate_disposition(
        self,
        lease: _BlockCandidateDispositionLease,
    ) -> None:
        self._ensure_block_candidate_service()._release_block_candidate_disposition(
            lease
        )

    def _block_candidate_terminal_outcome(self, block_hash: str) -> bool | None:
        return self._ensure_block_candidate_service()._block_candidate_terminal_outcome(
            block_hash
        )

    def _record_block_candidate_terminal_outcome(
        self,
        block_hash: str,
        *,
        accepted: bool,
    ) -> None:
        self._ensure_block_candidate_service()._record_block_candidate_terminal_outcome(
            block_hash,
            accepted=accepted,
        )

    def _record_committed_block_candidate_abandonment(
        self,
        block_hash: str,
        outcome: threading.local,
    ) -> None:
        """Count an abandonment only after its terminal cleanup is fixed."""
        self._ensure_block_candidate_service()._record_committed_block_candidate_abandonment(
            block_hash,
            outcome,
        )

    def _reserve_block_fast_lane_slot(self, block_hash: str) -> bool:
        """Reserve pool capacity while a node offer awaits terminal accounting."""
        return self._ensure_block_candidate_service()._reserve_block_fast_lane_slot(
            block_hash
        )

    def _release_block_fast_lane_slot(self, block_hash: str) -> None:
        self._ensure_block_candidate_service()._release_block_fast_lane_slot(
            block_hash
        )

    def _block_candidate_disposition(
        self,
        block_hash: str,
    ) -> Iterator[_BlockCandidateDispositionLease]:
        """Serialize the full accepted/abandoned decision for one hash."""
        return self._ensure_block_candidate_service()._block_candidate_disposition(
            block_hash
        )

    def _ensure_block_accounting_state(self) -> None:
        self._ensure_block_candidate_service()._ensure_block_accounting_state()

    def _start_block_accounting_thread(self) -> threading.Thread:
        return self._ensure_block_candidate_service()._start_block_accounting_thread()

    def _enqueue_block_accounting_task(
        self,
        task: _BlockCandidateAccountingTask,
    ) -> bool:
        return self._ensure_block_candidate_service()._enqueue_block_accounting_task(
            task
        )

    def _run_one_invalid_block_candidate_quarantine(self) -> bool:
        return self._ensure_block_candidate_service()._run_one_invalid_block_candidate_quarantine()

    def _call_block_candidate_writer(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission,
        disposition_held: bool,
    ) -> bool:
        """Invoke the writer while preserving duck-typed test integrations."""
        return self._ensure_block_candidate_service()._call_block_candidate_writer(
            candidate,
            node_submission=node_submission,
            disposition_held=disposition_held,
        )

    def _restore_replayed_candidate_acceptance_evidence(
        self,
        candidate: PrismBlockCandidate,
    ) -> None:
        self._ensure_block_candidate_service()._restore_replayed_candidate_acceptance_evidence(
            candidate
        )

    def _run_block_accounting_task(
        self,
        task: _BlockCandidateAccountingTask,
    ) -> None:
        self._ensure_block_candidate_service()._run_block_accounting_task(task)

    def block_accounting_loop(self) -> None:
        self._ensure_block_candidate_service().block_accounting_loop()

    def block_submit_loop(self) -> None:
        self._ensure_block_candidate_service().run()

    def submit_next_block_candidate(
        self,
        timeout: float | None = None,
        *,
        defer_accounting: bool = False,
    ) -> bool:
        """Dequeue and land one block candidate; returns True when one ran.

        The block-submitter loop calls this continuously; tests call it
        directly to drain the queue deterministically.
        """
        return self._ensure_block_candidate_service().submit_next(
            timeout,
            defer_accounting=defer_accounting,
        ).ran

    def _submit_next_block_candidate_writer(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission | None = None,
        disposition_held: bool = False,
    ) -> bool:
        """Land one dequeued block candidate inside writer admission."""
        return self._ensure_block_candidate_service().submit_writer(
            candidate,
            node_submission=node_submission,
            disposition_held=disposition_held,
        )

    def _finalize_block_candidate(
        self,
        candidate: PrismBlockCandidate,
        *,
        block_hash: str,
        accepted: bool,
        error: str,
        outcome: threading.local,
    ) -> bool:
        """Drive a terminal candidate's durable outbox update, with backoff."""
        return self._ensure_block_candidate_service().finalize(
            candidate,
            block_hash=block_hash,
            accepted=accepted,
            error=error,
            outcome=outcome,
        )

    def _merge_block_candidate_retry_locked(
        self,
        attribute: str,
        candidate: PrismBlockCandidate,
    ) -> None:
        """Merge one retry by parent-first order. Caller holds self.lock."""
        self._ensure_block_candidate_service()._merge_block_candidate_retry_locked(
            attribute,
            candidate,
        )

    def _retain_block_candidate_for_retry(self, candidate: PrismBlockCandidate) -> None:
        """Keep the oldest unresolved candidate ahead of queued descendants."""
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

    def _pace_block_candidate_retry(self, block_hash: str) -> None:
        """Apply per-candidate retry backoff without convoying accounting."""
        self._ensure_block_candidate_service()._pace_block_candidate_retry(block_hash)

    def _block_candidate_retry_ready_locked(
        self,
        candidate: PrismBlockCandidate,
    ) -> bool:
        """Return whether a parked retry's backoff deadline has passed."""
        return self._ensure_block_candidate_service()._block_candidate_retry_ready_locked(
            candidate
        )

    def _stash_retained_block_candidate_node_submission(
        self,
        block_hash: str,
        node_submission: _BlockCandidateNodeSubmission | None,
    ) -> None:
        """Record a definitive node acceptance for in-process retries."""
        self._ensure_block_candidate_service()._stash_retained_block_candidate_node_submission(
            block_hash,
            node_submission,
        )

    def _retained_block_candidate_node_submission(
        self,
        block_hash: str,
    ) -> _BlockCandidateNodeSubmission | None:
        return self._ensure_block_candidate_service()._retained_block_candidate_node_submission(
            block_hash
        )

    def _block_candidate_acceptance_retained(self, block_hash: str) -> bool:
        """Whether this process holds fresh first-party acceptance evidence."""
        return self._ensure_block_candidate_service()._block_candidate_acceptance_retained(
            block_hash
        )

    def _clear_block_candidate_retry_state(self, block_hash: str) -> None:
        self._ensure_block_candidate_service().clear_retry_state(block_hash)

    def _defer_block_candidate(self, reason: str, message: str, *, worker: str | None) -> None:
        """Record a retryable outcome without counting a terminal abandonment."""
        self._ensure_block_candidate_service().record_deferred(
            reason,
            message,
            worker=worker,
        )

    def _block_candidate_acceptance_recorded(self, block_hash: str) -> bool:
        """Return whether this process completed the candidate success tail."""
        return self._ensure_block_candidate_service()._block_candidate_acceptance_recorded(
            block_hash
        )

    def _register_outstanding_block_candidate(self, block_hash: str) -> None:
        """Track a candidate this process may still land, for tip matching."""
        self._ensure_block_candidate_service()._register_outstanding_block_candidate(
            block_hash
        )

    def _discard_outstanding_block_candidate(self, block_hash: str) -> None:
        """Stop matching tip observations once a candidate is terminal."""
        self._ensure_block_candidate_service()._discard_outstanding_block_candidate(
            block_hash
        )

    def _note_tip_observation_for_candidates(self, tip_hash: str) -> None:
        """Register a tip observation that matches an outstanding candidate."""
        self._ensure_block_candidate_service()._note_tip_observation_for_candidates(
            tip_hash
        )

    def _block_candidate_acceptance_observed(self, block_hash: str) -> bool:
        """Whether a recent tip observation already proved this candidate landed."""
        return self._ensure_block_candidate_service()._block_candidate_acceptance_observed(
            block_hash
        )

    def _block_candidate_chain_probe(
        self,
        block_hash: str,
        *,
        expected_height: int | None = None,
    ) -> bool | None:
        """Fresh chain verdict: proven active, proven wrong, or unknown."""
        return self._ensure_block_candidate_service()._block_candidate_chain_probe(
            block_hash,
            expected_height=expected_height,
        )

    def _block_candidate_acceptance_pending(
        self,
        block_hash: str,
        *,
        expected_height: int | None = None,
    ) -> bool:
        """Return whether abandoning this candidate would discard an accepted block."""
        return self._ensure_block_candidate_service()._block_candidate_acceptance_pending(
            block_hash,
            expected_height=expected_height,
        )

    def _count_accept_pending_defer(self) -> None:
        self._ensure_block_candidate_service()._count_accept_pending_defer()

    def _abandon_block_candidate(
        self,
        reason: str,
        message: str,
        *,
        block_hash: str,
        worker: str | None,
        preserve_if_accepted: bool = False,
        expected_height: int | None = None,
        stale_job_class: str | None = None,
    ) -> bool:
        """Record a lost/failed block candidate as a BLOCK-path event."""
        return self._ensure_block_candidate_service().record_abandoned(
            reason,
            message,
            block_hash=block_hash,
            worker=worker,
            preserve_if_accepted=preserve_if_accepted,
            expected_height=expected_height,
            stale_job_class=stale_job_class,
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
        block_hash: str,
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
                block_hash=block_hash,
                worker=worker,
            )
            return True
        if pending_parent_transition is None:
            return False
        preserve_active_candidate_barrier()
        self._abandon_block_candidate(
            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
            "parent or ancestor payout confirmation is still pending",
            block_hash=block_hash,
            worker=worker,
        )
        return True

    def _payout_balance_serializer_released(self) -> Iterator[None]:
        """Temporarily release the balance serializer around candidate audit"""
        return self._ensure_payout_state_service()._payout_balance_serializer_released()

    def _replayed_payout_window_reproducible(self, context: PrismJobContext) -> bool:
        """Whether a reconstructed candidate's payout window replays intact."""
        return self._ensure_payout_state_service()._replayed_payout_window_reproducible(context)

    def _land_and_confirm_block_candidate(
        self,
        candidate: PrismBlockCandidate,
        *,
        current_tip: str,
        already_active: bool,
        worker: str | None,
        node_submission: _BlockCandidateNodeSubmission,
        revalidated_append_epoch: int | None = None,
    ) -> tuple[
        dict[str, Any],
        dict[str, Any],
        dict[str, Any],
        dict[str, Any],
        AuditPublicationIdentity,
        dict[str, Any],
    ] | None:
        """Land, verify, publish, persist, and confirm one candidate (B3)."""
        return self._ensure_block_finalization_service()._land_and_confirm_block_candidate(
            candidate,
            current_tip=current_tip,
            already_active=already_active,
            worker=worker,
            node_submission=node_submission,
            revalidated_append_epoch=revalidated_append_epoch,
        )

    def _land_block_candidate_submission(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission | None = None,
        disposition_held: bool = False,
    ) -> bool:
        """Run one candidate's landing tail behind the named submit port.

        Embeddings and tests replace the bound ``submit_block_candidate`` to
        stand in for the node submission, so which of the two landing tails
        applies is a fact about this coordinator rather than about the
        block-candidate owner. Deciding it here keeps that owner from reaching
        back through the runtime seam to inspect the entrypoint, and keeps the
        decision per call: the replacement is installed on the instance after
        the block-candidate service is constructed.

        ``node_submission`` is an already-created offer result when the caller
        made one, and None when none was created -- which keeps the historical
        bare entrypoint call. ``disposition_held`` states that the caller
        already owns the same-hash disposition, which is what makes the
        serialized inner tail, rather than the public entrypoint that would
        take that guard again, the correct production call.
        """
        submit = self.submit_block_candidate
        production_submit = (
            getattr(submit, "__func__", None)
            is PrismCoordinator.submit_block_candidate
        )
        if disposition_held and production_submit:
            assert node_submission is not None
            return self._submit_block_candidate_serialized(
                candidate,
                node_submission=node_submission,
            )
        if node_submission is None:
            return bool(submit(candidate))
        return self._account_block_candidate_after_node_submit(
            candidate,
            node_submission,
        )

    @ledger_writer_operation("accepted_block_handling")
    def submit_block_candidate(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission | None = None,
    ) -> bool:
        """Land one block candidate, then finalize its audit and payout state."""
        return self._ensure_block_finalization_service().submit_block_candidate(
            candidate,
            node_submission=node_submission,
        )

    def _record_block_candidate_progress(
        self,
        phase: str = "accounting-progress",
    ) -> None:
        """Stamp the submitter heartbeat at a candidate-disposition boundary."""
        self._ensure_block_candidate_service()._record_block_candidate_progress(phase)

    def _submit_block_candidate_serialized(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> bool:
        """Process a candidate while its same-hash disposition guard is held."""
        return self._ensure_block_finalization_service()._submit_block_candidate_serialized(
            candidate,
            node_submission=node_submission,
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

    def _progress_now(self) -> float:
        clock = getattr(self, "_progress_monotonic", None)
        return float(clock() if callable(clock) else time.monotonic())

    def _ensure_progress_health_service(self) -> ProgressHealthService:
        service = self.__dict__.get("progress_health_service")
        if service is not None:
            return service
        init_lock = self.__dict__.get("_progress_health_init_lock")
        if init_lock is None:
            # CPython's setdefault is atomic under the GIL. Focused tests may
            # construct through __new__, while normal instances build the
            # service during __init__ before any process thread can start.
            init_lock = self.__dict__.setdefault(
                "_progress_health_init_lock",
                threading.Lock(),
            )
        with init_lock:
            service = self.__dict__.get("progress_health_service")
            if service is not None:
                return service
            service = ProgressHealthService(
                started_monotonic=float(
                    getattr(self, "started_monotonic", time.monotonic())
                ),
                initial_payout_generation=int(
                    getattr(self, "_payout_state_generation", 0)
                ),
                # Resolve the clock through the coordinator seam at call time
                # so the historical ``_progress_monotonic`` test override
                # keeps steering the service after construction.
                monotonic=self._progress_now,
            )
            self.__dict__["progress_health_service"] = service
        return service

    def _progress_health_config(self) -> ProgressHealthConfig:
        return ProgressHealthConfig(
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
        )

    def _progress_work_generation(
        self,
        snapshot: QbitTipTemplateSnapshot,
        payout_generation: int,
    ) -> WorkGeneration:
        return WorkGeneration(
            template_generation=int(snapshot.template_generation),
            template_fingerprint=snapshot.template_fingerprint,
            payout_generation=int(payout_generation),
        )

    def _progress_note_refresh_pending(self, started_monotonic: float | None = None) -> None:
        self._ensure_job_cache_state()
        self._ensure_progress_health_service().mark_refresh_pending(
            started_monotonic
        )

    def _record_progress_tip_poll(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observed_monotonic: float | None = None,
    ) -> None:
        """Publish a coherent qbit tip/template observation to health state."""
        self._ensure_job_cache_state()
        payout_generation = int(getattr(self, "_payout_state_generation", 0))
        self._ensure_progress_health_service().observe_tip(
            self._progress_work_generation(snapshot, payout_generation),
            observed_monotonic,
        )

    def _record_progress_payout_generation(
        self,
        generation: int,
        invalidated_monotonic: float | None = None,
    ) -> None:
        self._ensure_job_cache_state()
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
        self._ensure_job_cache_state()
        service = self._ensure_progress_health_service()
        with self.lock:
            latest_detected = getattr(self, "latest_detected_tip", None)
            published = service.publish_work(
                self._progress_work_generation(snapshot, payout_generation),
                matches_latest_tip=bool(
                    latest_detected is None
                    or latest_detected[0] == snapshot.bestblockhash
                ),
            )
        if published:
            self._progress_note_refresh_activity()
            self._progress_reconcile_pending()

    def _record_progress_delivery(
        self,
        client: ClientState,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> None:
        """Record a completed current-generation socket delivery.

        Cross-owner adapter by design: the session owner commits exact S1
        registry proof first, then reports the delivery to G1.
        """
        self._ensure_stratum_session_service().record_successful_delivery(
            client, context, delivered_monotonic
        )

    def _progress_refresh_started(self) -> None:
        """Track a coherent poll pass while it continues making progress."""
        self._ensure_job_cache_state()
        self._ensure_progress_health_service().refresh_started()

    def _progress_note_refresh_activity(
        self,
        observed_monotonic: float | None = None,
    ) -> None:
        self._ensure_job_cache_state()
        self._ensure_progress_health_service().note_refresh_activity(
            observed_monotonic
        )

    def _progress_refresh_finished(self) -> None:
        self._ensure_job_cache_state()
        self._ensure_progress_health_service().refresh_finished()

    def _progress_bundle_build_started(self) -> int:
        self._ensure_job_cache_state()
        return self._ensure_progress_health_service().bundle_build_started()

    def _progress_bundle_build_finished(self, token: int) -> None:
        self._ensure_job_cache_state()
        self._ensure_progress_health_service().bundle_build_finished(token)

    def _progress_eligible_client_counts(
        self,
        template_fingerprint: str | None,
        payout_generation: int,
    ) -> tuple[int, int]:
        with self.lock:
            eligible = [
                client
                for client in self.clients
                if self.client_can_receive_jobs(client)
            ]
            delivered_work = [
                client._progress_delivered_context
                for client in eligible
            ]
            ready_mode_required = bool(
                getattr(self, "_pool_ready_latched", False)
            )
        requiring_refresh = 0
        for delivered_context in delivered_work:
            delivered_fingerprint = None
            delivered_payout_generation = -1
            if delivered_context is not None:
                delivered_fingerprint = getattr(
                    delivered_context,
                    "template_fingerprint",
                    None,
                )
                if delivered_fingerprint is None:
                    delivered_fingerprint = qbit_template_fingerprint(
                        delivered_context.template
                    )
                delivered_payout_generation = int(
                    getattr(delivered_context, "payout_state_generation", 0)
                )
            if (
                delivered_fingerprint != template_fingerprint
                or delivered_payout_generation != payout_generation
                or (
                    ready_mode_required
                    and bool(
                        getattr(delivered_context, "collection_only", False)
                    )
                )
            ):
                requiring_refresh += 1
        return len(eligible), requiring_refresh

    def _progress_reconcile_pending(self, *, now: float | None = None) -> None:
        """Clear pending state exactly when publication/delivery is sufficient."""
        self._ensure_job_cache_state()
        self._ensure_progress_health_service().reconcile_pending(
            self._progress_eligible_client_counts,
            now=now,
        )

    def _progress_health_value(
        self,
        *,
        now: float | None = None,
    ) -> ProgressHealthSnapshot:
        self._ensure_job_cache_state()
        started_monotonic = getattr(self, "started_monotonic", None)
        return self._ensure_progress_health_service().snapshot(
            self._progress_eligible_client_counts,
            int(getattr(self, "_payout_state_generation", 0)),
            self._progress_health_config(),
            None if started_monotonic is None else float(started_monotonic),
            now=now,
        )

    def progress_health_snapshot(self, *, now: float | None = None) -> dict[str, object]:
        """Return a bounded, monotonic-only mining progress health snapshot."""
        return self._progress_health_value(now=now).as_mapping()

    @staticmethod
    def _apply_progress_health(
        payload: dict[str, object],
        progress: dict[str, object],
    ) -> dict[str, object]:
        return ObservabilityService.apply_progress_health(payload, progress)

    def progress_health_metrics_lines(self) -> list[str]:
        return list(
            ProgressHealthService.metrics_lines(
                self._progress_health_value(),
                coordination_blocked_age_seconds=(
                    self.coordination_blocked_streak_age_seconds()
                ),
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
        # The first refresh seeds the exact accepted-share aggregate, which
        # can take minutes on a grown ledger. It runs inside the background
        # loop so the audit listener bind path never blocks on it; until it
        # completes, cached_health_payload reports an explicit starting
        # state (issue #188 fix 4). The running flag above is set before the
        # registry can start the thread, preserving the #120
        # mark-before-listener-dispatch contract for start_audit_server.
        self._start_background_service("health_snapshot_refresher")

    def refresh_metrics_snapshot(self) -> str:
        return self._ensure_observability_service().refresh_metrics_snapshot()

    def cached_metrics_payload(self) -> MetricsSnapshotResponse:
        return self._ensure_observability_service().cached_metrics_payload()

    def metrics_snapshot_loop(self) -> None:
        self._ensure_observability_service().metrics_snapshot_loop()

    def start_metrics_snapshot_refresher(self) -> None:
        service = self._ensure_observability_service()
        if not service.begin_metrics_refresh_loop():
            return
        # Like the health refresher above, the collector is marked running
        # before its thread can be dispatched and never collects on the
        # scrape path: /metrics serves only the cached complete snapshot.
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

    def coordinator_lock_contention_snapshot(self) -> tuple[int, int, float, float]:
        """Raw (acquisitions, contentions, wait_sum, wait_max) from the lock."""
        snapshot = getattr(self.lock, "contention_snapshot", None)
        if callable(snapshot):
            acquisition_count, contention_count, wait_sum, wait_max = snapshot()
            return (
                int(acquisition_count),
                int(contention_count),
                float(wait_sum),
                float(wait_max),
            )
        return 0, 0, 0.0, 0.0

    def coordinator_lock_metrics_lines(self) -> list[str]:
        # Compatibility wrapper for the extracted metrics owner.
        return self._ensure_metrics_renderer().coordinator_lock_metrics_lines()

    def _observe_block_submit_seconds(self, elapsed_seconds: float) -> None:
        self._ensure_block_candidate_service()._observe_block_submit_seconds(
            elapsed_seconds
        )

    def _observe_accepted_block_preview_publication(
        self,
        block_hash: str,
        *,
        result: str,
    ) -> None:
        # Called from the P1 publication boundary; the B1 owner holds the
        # acceptance stamp taken on the submitter thread.
        service = self._ensure_block_candidate_service()
        service._observe_accepted_block_preview_publication(
            block_hash,
            result=result,
        )

    def block_submitter_snapshot(self) -> dict[str, object]:
        """Merged durable pending-candidate and B1 owner snapshot.

        Combines the ledger's durable pending aggregates with the block-
        candidate service's copied backoff, submit-histogram, and
        acceptance-to-preview-publication snapshots; each owner copies under
        its own lock, so no locks nest here.
        """
        pending_metrics: dict[str, object] = {
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
        service = self._ensure_block_candidate_service()
        backoff_active, backoff_remaining, backoff_delay = (
            service.backoff_snapshot()
        )
        submit_buckets, submit_sum, submit_count = (
            service.block_submit_seconds_snapshot()
        )
        accepted_preview_publication = (
            service.accepted_block_preview_publication_snapshot()
        )
        pending_metrics.update(
            {
                "backoff_active": backoff_active,
                "backoff_remaining_seconds": backoff_remaining,
                "backoff_delay_seconds": backoff_delay,
                "submit_seconds_buckets": submit_buckets,
                "submit_seconds_sum": submit_sum,
                "submit_seconds_count": submit_count,
                "accepted_preview_publication": accepted_preview_publication,
            }
        )
        return pending_metrics

    def block_submitter_metrics_lines(self) -> list[str]:
        # Compatibility wrapper for the extracted metrics owner.
        return self._ensure_metrics_renderer().block_submitter_metrics_lines()

    def landing_observability_metrics_lines(self) -> list[str]:
        # Compatibility wrapper for the extracted metrics owner.
        return (
            self._ensure_metrics_renderer().landing_observability_metrics_lines()
        )

    def _accepted_stats_reconcile_metric_lines(self) -> list[str]:
        # Compatibility wrapper for the extracted metrics owner.
        return (
            self._ensure_metrics_renderer().accepted_stats_reconcile_metric_lines()
        )

    def share_accounting_snapshot(self) -> dict[str, object]:
        """Copy the hot-path share counters under the accounting lock."""
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

    @staticmethod
    def rejection_reason_ids() -> tuple[str, ...]:
        # The renderer must not import coordinator globals.
        return PRISM_REJECTION_REASON_IDS

    def _ensure_metrics_renderer(self) -> MetricsRenderer:
        renderer = self.__dict__.get("_metrics_renderer")
        if renderer is None:
            # Idempotent construction; no init lock is needed.
            renderer = MetricsRenderer(self)
            self.__dict__["_metrics_renderer"] = renderer
        return renderer

    def _render_metrics_payload(self) -> str:
        return self._ensure_metrics_renderer().render()

    def metrics_payload(self) -> str:
        """Compatibility renderer; HTTP uses the complete cached snapshot."""

        return self._render_metrics_payload()

    def shutdown_metrics_lines(self) -> list[str]:
        # Compatibility wrapper for the extracted metrics owner.
        return self._ensure_metrics_renderer().shutdown_metrics_lines()

    def audit_artifact_metrics(self) -> dict[str, dict[str, int] | int]:
        return self._ensure_audit_artifact_store().metrics_snapshot()

    @staticmethod
    def audit_artifact_kind(name: str) -> str:
        return AuditArtifactStore.artifact_kind(name)

    def ctv_fanout_broadcaster_metrics_lines(self) -> list[str]:
        return self._ensure_ctv_runtime().metrics_lines()

    def initial_delivery_metrics_lines(self) -> list[str]:
        # Compatibility wrapper for the extracted metrics owner.
        return self._ensure_metrics_renderer().initial_delivery_metrics_lines()

    def vardiff_idle_metrics_lines(self) -> list[str]:
        return self._ensure_vardiff_service().metrics_lines()

    def vardiff_convergence_snapshot(self) -> dict[str, object]:
        return self._ensure_vardiff_service().convergence_snapshot()

    def block_finalization_metrics_lines(self) -> list[str]:
        return self._ensure_block_finalization_service().metrics_lines()

    def tip_refresh_metrics_lines(self) -> list[str]:
        return self._ensure_tip_refresh_service().tip_refresh_metrics_lines()

    def job_build_metrics_lines(self) -> list[str]:
        # Compatibility wrapper for the extracted metrics owner.
        return self._ensure_metrics_renderer().job_build_metrics_lines()

    def payout_state_metrics_lines(self) -> list[str]:
        return self._ensure_payout_state_service().payout_state_metrics_lines()


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
    # A native fault (SIGSEGV/SIGABRT/SIGBUS/SIGFPE) otherwise kills the
    # process with zero Python-side output; enable unconditionally so the
    # tracebacks of every thread reach stderr even when PYTHONFAULTHANDLER
    # is unset in the deployment environment. SIGUSR2 gives an on-demand
    # all-thread dump for live triage without disturbing the process.
    faulthandler.enable(all_threads=True)
    faulthandler.register(signal.SIGUSR2, all_threads=True)
    # Before the coordinator, because constructing it starts threads and the
    # switch interval is process-global: applied afterwards, already-running
    # threads would observe the change at an arbitrary point in their own
    # scheduling. Silent when unset -- an unconfigured pool must not log a
    # value it did not choose.
    switch_interval = apply_python_switch_interval()
    if switch_interval is not None:
        print(
            "prism interpreter switch interval set to "
            f"{switch_interval:g}s (default is 5ms)",
            flush=True,
        )
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
    return 1 if getattr(coordinator, "_fatal_exit_requested", False) else 0


if __name__ == "__main__":
    raise SystemExit(main())
