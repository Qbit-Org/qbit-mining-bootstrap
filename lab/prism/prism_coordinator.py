#!/usr/bin/env python3
"""Minimal live direct qbit Stratum coordinator for PRISM regtest proof."""

from __future__ import annotations

import base64
import copy
from collections import OrderedDict
from concurrent.futures import (
    FIRST_COMPLETED,
    CancelledError as FuturesCancelledError,
    Future,
    InvalidStateError,
    ThreadPoolExecutor,
    wait,
)
from contextlib import ExitStack, contextmanager
import dataclasses
import errno
import faulthandler
from functools import wraps
import hashlib
import heapq
import http.client
import inspect
import json
import math
import os
import queue
import random
import shlex
import signal
import socket
import subprocess
import tempfile
import threading
import time
import traceback
import urllib.parse
import urllib.request
import uuid
import weakref
from types import SimpleNamespace
from dataclasses import dataclass, field, replace as dataclass_replace
from decimal import Context, Decimal, InvalidOperation, ROUND_CEILING, getcontext, localcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

import sys

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.auxpow import stratum_codec, vardiff
from lab.prism import direct_stratum, public_api
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
    _LeaseHeartbeatStateField,
    guard_session_verifier as _lease_guard_session_verifier,
)
# Compatibility re-exports; new callers should import lab.prism.coordinator_shutdown.
from lab.prism.coordinator_shutdown import (
    CoordinatorShutdownController,  # noqa: F401 - compatibility re-export
    ShutdownInProgress,  # noqa: F401 - compatibility re-export
    _WriterOperationToken,  # noqa: F401 - compatibility re-export
    ledger_writer_operation,  # noqa: F401 - compatibility re-export
)
from lab.prism.coordinator_config import (
    CoordinatorConfig,
    DEFAULT_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX,  # noqa: F401
    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_MAX_SECONDS,  # noqa: F401
    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_SECONDS,  # noqa: F401
    DEFAULT_BLOCK_SUBMIT_DB_TIMEOUT_SECONDS,
    DEFAULT_BLOCK_SUBMIT_LOCK_WAIT_LOG_SECONDS,  # noqa: F401
    DEFAULT_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS,
    DEFAULT_BLOCK_SUBMIT_STUCK_CALL_EXIT_SECONDS,  # noqa: F401
    DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS,  # noqa: F401 - compatibility re-export
    DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS,
    DEFAULT_HIGHDIFF_DIFFICULTY,  # noqa: F401 - compatibility re-export
    DEFAULT_HIGHDIFF_MAX_DIFFICULTY,  # noqa: F401 - compatibility re-export
    DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS,
    DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION,
    DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS,
    DEFAULT_MIN_OUTPUT_FEERATE_SATS_PER_BYTE,  # noqa: F401 - compatibility re-export
    DEFAULT_MIN_OUTPUT_SAFETY_MULTIPLIER,  # noqa: F401 - compatibility re-export
    DEFAULT_P2MR_SPEND_INPUT_BYTES,  # noqa: F401 - compatibility re-export
    DEFAULT_PRISM_BLOCKPOLL_SECONDS,
    DEFAULT_PRISM_BLOCKWAIT_TIMEOUT_SECONDS,
    DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
    DEFAULT_PRISM_COINBASE_TAG,  # noqa: F401 - compatibility re-export
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
    DEFAULT_PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS,  # noqa: F401
    DEFAULT_PRISM_OBSERVED_TIP_ACCEPT_WINDOW_SECONDS,  # noqa: F401
    DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES,
    DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS,
    DEFAULT_PRISM_PAYOUT_ARTIFACT_FULL_RESCAN_SECONDS,  # noqa: F401
    DEFAULT_PRISM_PAYOUT_ARTIFACT_MAX_ANCHOR_AGE_SECONDS,  # noqa: F401
    DEFAULT_PRISM_PAYOUT_ARTIFACT_MIN_BUILD_INTERVAL_SECONDS,  # noqa: F401
    DEFAULT_PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS,  # noqa: F401
    DEFAULT_PRISM_PAYOUT_ARTIFACT_REARM_MIN_SECONDS,  # noqa: F401
    DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
    DEFAULT_PRISM_ROUTINE_ADMISSION_DEADLINE_SECONDS,  # noqa: F401
    DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION,
    DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
    DEFAULT_PRISM_STALE_GRACE_SECONDS,
    DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS,  # noqa: F401
    DEFAULT_PRISM_STRATUM_BIND_RETRY_SECONDS,  # noqa: F401
    DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS,
    DEFAULT_PRISM_STRATUM_LISTEN_BACKLOG,  # noqa: F401
    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME,  # noqa: F401
    DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS,
    DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS,
    DEFAULT_PRISM_SUBMIT_TIP_MAX_AGE_SECONDS,
    DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
    DEFAULT_PRISM_TIP_REFRESH_FAILURE_HOLDOFF_SECONDS,  # noqa: F401
    DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS,
    DEFAULT_PRISM_VARDIFF_IDLE_SWEEP_SECONDS,  # noqa: F401 - compatibility re-export
    DEFAULT_PRISM_WATCHDOG_LEASE_RELEASE_TIMEOUT_SECONDS,
    DEFAULT_PRISM_WORKER_METRICS_LIMIT,
    DEFAULT_PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS,
    LEASE_AUTHORITY_MARGIN_HEADROOM_SECONDS,
    DEFAULT_SHARE_COMMIT_BATCH_SIZE,  # noqa: F401 - compatibility re-export
    DEFAULT_SHARE_COMMIT_LINGER_MILLISECONDS,  # noqa: F401 - compatibility re-export
    DEFAULT_SHARE_COMMIT_TIMEOUT_SECONDS,  # noqa: F401 - compatibility re-export
    DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS,  # noqa: F401 - compatibility re-export
    MAX_PRISM_COINBASE_TAG_BYTES,  # noqa: F401 - compatibility re-export
    StratumListenerProfile,  # noqa: F401 - compatibility re-export
    TESTNET_QBIT_CHAINS,  # noqa: F401 - compatibility re-export
    VALID_COINBASE_OUTPUT_POLICIES,
    default_prism_coinbase_tag_hex,  # noqa: F401 - compatibility re-export
    default_prism_payout_policy,
    default_prism_username_fallback_address,  # noqa: F401 - compatibility re-export
    env,
    env_bool,
    env_decimal,  # noqa: F401 - compatibility re-export
    env_int,
    env_nonnegative_float,
    env_nonnegative_int,
    env_nonnegative_int_with_legacy,  # noqa: F401 - compatibility re-export
    env_optional,
    env_optional_bool,  # noqa: F401 - compatibility re-export
    env_optional_positive_int,  # noqa: F401 - compatibility re-export
    env_optional_positive_int_with_legacy,
    env_positive_float,
    env_positive_int,
    env_positive_int_with_legacy,
    env_seed_hex,  # noqa: F401 - compatibility re-export
    load_coordinator_config,
    load_share_weights,
    load_prism_highdiff_listener,  # noqa: F401 - compatibility re-export
    load_prism_vardiff_config,  # noqa: F401 - compatibility re-export
    production_mode,  # noqa: F401 - compatibility re-export
    require_production_env,  # noqa: F401 - compatibility re-export
    resolve_initial_job_max_workers,  # noqa: F401 - compatibility re-export
    validate_hex,
    validate_initial_job_max_workers,  # noqa: F401 - compatibility re-export
    validate_job_build_executor_workers,  # noqa: F401 - compatibility re-export
    validate_payout_artifact_age_bounds,  # noqa: F401 - compatibility re-export
    validate_prism_production_gate,  # noqa: F401 - compatibility re-export
    validate_same_tip_job_retention_limits,  # noqa: F401 - compatibility re-export
)
from lab.prism.ctv_broadcaster import CtvFanoutBroadcaster
from lab.prism.ctv_broadcaster_daemon import (
    CtvFanoutBroadcastDaemon,
    CtvFanoutChunkResult,
    CtvFanoutDaemonResult,
    MAX_CTV_FANOUT_BROADCASTER_CHUNK_SIZE,
)
# Compatibility re-exports; new callers should import lab.prism.ctv_runtime.
from lab.prism.ctv_runtime import (
    CtvRuntimeConfig,
    CtvRuntimeService,
    PRISM_CTV_BROADCASTER_CHUNK_ROWS_BUCKETS,  # noqa: F401
    PRISM_CTV_BROADCASTER_CHUNK_SECONDS_BUCKETS,  # noqa: F401
    PRISM_CTV_BROADCASTER_SECONDS_BUCKETS,  # noqa: F401
)
# Compatibility re-exports; new callers should import the owning J1 modules.
from lab.prism.audit_artifacts import (
    AuditArtifactConfig,
    AuditArtifactStore,
    AuditPublicationIdentity,
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
    CollectionIdentityUnavailable,  # noqa: F401 - compatibility re-export
    JobBuildAdmissionDeadlineExceeded,  # noqa: F401 - compatibility re-export
    JobBuildCancellation as _JobBuildCancellation,
    JobBuildCancelled,
    JobBuildFlight as _JobBuildFlight,
    JobBuildKey,  # noqa: F401 - compatibility re-export
    JobBuildRequest as _JobBuildRequest,
    JobBuildSuperseded,
    JobBuildWaiterCancelled as _JobBuildCancelled,  # noqa: F401
    JobBundleBuildControl as _JobBundleBuildControl,
    JobBundleBuildSuperseded as _JobBundleBuildSuperseded,
    JobBundleService,
)
from lab.prism.job_delivery import (
    DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS,  # noqa: F401 - compatibility re-export
    EvictedJobEntry,
    JobBuildFailed as _JobBuildFailed,
    JobDeliveryService,
    MAX_ACTIVE_PRISM_JOBS_PER_CLIENT,  # noqa: F401 - compatibility re-export
    PRISM_CREDIT_POLICY_STALE_GRACE,
    PRISM_DELIVERY_PRIORITY_INITIAL,
    PRISM_DELIVERY_PRIORITY_NEW_TIP,
    PRISM_DELIVERY_PRIORITY_SAME_TIP,
    PRISM_EVICTED_JOB_CAPACITY_SCOPES,
    PRISM_EVICTED_JOB_CLASSES,
    PRISM_EVICTED_JOB_SUBMIT_OUTCOMES,
    PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS,
    PendingInitialJob,
    PrismJobContext,
)
from lab.prism.payout_state import (
    PayoutStatePublicationBlocked as _PayoutStatePublicationBlocked,  # noqa: F401
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
    PRISM_TIP_REFRESH_CANCELLATION_STAGES,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_COVERAGE_TARGETS,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_FAILURE_HOLDOFF_JITTER_FRACTION,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_REENTRY_BACKOFF_SECONDS,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_RESULTS,  # noqa: F401 - compatibility re-export
    PRISM_TIP_REFRESH_SECONDS_BUCKETS,
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
    AcceptedBlockPayoutTransition as _AcceptedBlockPayoutTransition,  # noqa: F401
    DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS,  # noqa: F401 - compatibility re-export
    DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
    PRISM_PAYOUT_ARTIFACT_REARM_BACKOFF_CAP,  # noqa: F401 - compatibility re-export
    PRISM_PAYOUT_DELIVERY_GENERATIONS,  # noqa: F401 - compatibility re-export
    PayoutDeliveryAdmission as _PayoutDeliveryAdmission,  # noqa: F401
    PayoutLedgerArtifact,
    PayoutStateArtifact,
    PayoutStateCandidate,
    PayoutStateDeliveryGate as _PayoutStateDeliveryGate,  # noqa: F401
    PayoutStateService,
    PayoutStateSnapshot,  # noqa: F401 - compatibility re-export
    PublishedPayoutState,
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
    parse_stratum_password_options,  # noqa: F401 - compatibility re-export
    parse_worker_username,  # noqa: F401 - compatibility re-export
    result_payload as stratum_result_payload,
    split_worker_username,  # noqa: F401 - compatibility re-export
    stratum_accept_heartbeat_names as configured_accept_heartbeat_names,
)
# Compatibility re-exports; new callers should import lab.prism.progress_health.
from lab.prism.progress_health import (
    BundleBuildToken,  # noqa: F401 - compatibility re-export
    DeliveryProof,
    EligibilitySnapshot,  # noqa: F401 - compatibility re-export
    PROGRESS_HEALTH_REASONS as PRISM_PROGRESS_HEALTH_REASONS,
    ProgressHealthConfig,
    ProgressHealthService,
    ProgressHealthSnapshot,
    RefreshActivityToken,  # noqa: F401 - compatibility re-export
    WorkGeneration,
    overlay_progress_health,
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
    DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,  # noqa: F401 - compatibility re-export
    IncrementalShareWindow,
    IncrementalShareJsonSequence,
    IncrementalWindowAdvanceStats,
    IncrementalWindowFallback,
    LedgerOperationTimeout,
    PendingShare,
    PsqlShareLedger,
    SingleWriterShareLedger,
    WRITER_LEASE_HEARTBEAT_SESSION_PREFIX,
    WRITER_LEASE_VERIFICATION_MAX_STATEMENTS,  # noqa: F401 - compatibility re-export
    WriterLeaseRenewalDeferred,
    sha256_json_hex,
)

MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES = 128
# Trusted reconcile outcomes are memoized per tip so an untrusted outcome for
# one tip cannot unarm the cache for every other tip. The map only ever
# answers for the current best hash; a small bound merely caps growth while
# stale entries age out of the TTL.
PRISM_REORG_RECONCILE_MEMO_MAX_TIPS = 8
# Same-tip reconcile callers coalesce behind one in-flight pass. The wait is
# a liveness backstop, not a pacing knob: a follower that outwaits it simply
# runs its own serialized pass, exactly as every caller did before flights.
DEFAULT_PRISM_RECONCILE_FLIGHT_WAIT_SECONDS = 30.0
# Reconcile demand observability: which caller lane asked, and what satisfied
# it. The tip-refresh lane can be satisfied by the per-tip trusted memo, by a
# pass overlapped with the template fetch on the prefetch worker, or by a
# serial pass on the refresh thread; per-client job builds only ever see a
# memo hit or their own serial pass.
PRISM_REORG_RECONCILE_LOOKUP_PATHS = ("tip_refresh", "job_build")
PRISM_REORG_RECONCILE_LOOKUP_SOURCES = ("memo_hit", "overlap", "serial")
# Read-to-ack latency labels for mining.submit. Accepted shares wait for the
# group commit before their ack; rejected shares (measured when the reject
# decision is made) skip it, so the pair separates commit pressure from
# thread-scheduling/GIL pressure as connection count grows.
PRISM_SHARE_ACK_RESULTS = ("accepted", "rejected")
# The overlapped reconcile pass runs ledger reads that can crawl while the
# chain churns. The tip-refresh join must never park the poll loop on it
# past the liveness budget: a bounded join leaves the pass running in its
# single slot (the paced retry re-joins the same future) and keeps the poll
# loop's heartbeat live while the pass catches up.
PRISM_RECONCILE_PREFETCH_JOIN_TIMEOUT_SECONDS = 20.0
# Ceiling for operator overrides of the join budget: a value at or above the
# poll loop's failure/watchdog budgets would silently reinstate the very park
# the bound exists to prevent.
PRISM_RECONCILE_PREFETCH_JOIN_TIMEOUT_CEILING_SECONDS = 60.0
# Cancellation-check slice while an initial request rides a subscribed
# publication-priority build promise. Promise completion wakes the waiter
# immediately; this only bounds how stale a cancellation can go unnoticed.
PRISM_INITIAL_JOB_SUBSCRIBE_POLL_SECONDS = 0.25
# A flight whose executor future is finished normally leaves its slot inside
# the future's done callback. The admission sweep only treats such a flight
# as orphaned after this grace, so a callback that is merely between future
# completion and slot cleanup is never mistaken for a dead one.
PRISM_JOB_BUILD_ORPHAN_SWEEP_GRACE_SECONDS = 1.0
PRISM_VARDIFF_IDLE_RETARGET_MAX_WORKERS = 2
MAX_PENDING_VARDIFF_IDLE_RETARGETS = 8
# Block candidates queue to a dedicated submitter thread so the miner's share
# ack never waits on audit/submitblock after the share and intent commit. The
# bound limits RAM; overflow only coalesces a wakeup because Postgres retains
# the authoritative pending candidate.
MAX_PENDING_BLOCK_CANDIDATES = 32
# Startup replay must observe every durable pending candidate before child
# job builds unblock (issue #188 fix 4): a truncated enumeration could hide
# the active parent whose payout transition is not yet armed. A full batch
# therefore re-queries with a doubled window until the result is provably
# untruncated. This cap bounds one enumeration pass's memory; at the cap the
# gate simply stays closed while the queued batch drains, and the submitter
# loop re-enumerates the shrinking remainder.
MAX_BLOCK_REPLAY_ENUMERATION_ROWS = 1024
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
PRISM_JOB_BUILD_SECONDS_BUCKETS = (
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
)
PRISM_VARDIFF_IDLE_SECONDS_BUCKETS = PRISM_JOB_BUILD_SECONDS_BUCKETS
PRISM_VARDIFF_IDLE_SKIP_REASONS = (
    "busy",
    "disconnected",
    "not_idle",
    "cache_miss",
    "queue_full",
    "superseded",
)
PRISM_JOB_BUILD_PHASES = (
    "reorg",
    "template",
    "merkle",
    "ledger",
    "payout_artifact",
    "payout",
    "ctv",
    "input_serialization",
    "worker",
    "output_serialization",
    "assembly",
    "bundle",
    "preparation_wait",
    "executor_queue",
    "client_lock",
    "payout_gate",
    "stamp",
    "socket_send",
    "send",
)
PRISM_JOB_CACHE_KINDS = ("template", "bundle")
PRISM_STALE_JOB_ABANDON_CLASSES = (
    "tip_moved",
    "balance_stale",
    "append_epoch_stale",
)
PRISM_REJECTION_STALE_JOB = "stale-job"
PRISM_REJECTION_DUPLICATE_SHARE = "duplicate-share"
PRISM_REJECTION_LOW_DIFFICULTY = "low-difficulty"
PRISM_REJECTION_MALFORMED_SUBMIT = "malformed-submit"
PRISM_REJECTION_UNAUTHORIZED_WORKER = "unauthorized-worker"
PRISM_REJECTION_UNKNOWN_JOB = "unknown-job"
PRISM_REJECTION_INVALID_EXTRANONCE = "invalid-extranonce"
PRISM_REJECTION_INVALID_NTIME_OR_NONCE = "invalid-ntime-or-nonce"
PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH = "candidate-audit-mismatch"
PRISM_REJECTION_SUBMITBLOCK_REJECTED = "submitblock-rejected"
PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE = "backend-rpc-unavailable"
PRISM_REJECTION_INTERNAL_ERROR = "internal-error"
PRISM_REJECTION_POOL_CLOSED = "pool-closed"
PRISM_REJECTION_BLOCK_STALE = "block-stale"
PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED = "ledger-confirmation-failed"
# The ledger reported the candidate's row terminally disposed (reorg
# quarantine, rejection, or reversal) before the confirmation landed. That is
# a routine race around a chain reorganization: terminal for this candidate,
# benign for the pool, so it must never escalate to a coordinator shutdown
# the way an unexplained confirmation failure does.
PRISM_REJECTION_LEDGER_CONFIRMATION_SUPERSEDED = "ledger-confirmation-superseded"
# A would-be terminal abandonment refused because the candidate's own block
# hash is (or was recently observed as) part of the active chain: qbitd
# accepted the block even though this process has not completed the accepted
# success tail (for example when the direct submitblock ack was lost in
# transport and acceptance was learned from a blockwait tip observation).
# The candidate defers and retries until the tail finalizes it as submitted.
PRISM_REJECTION_BLOCK_ACCEPT_PENDING = "accepted-pending-finalization"
PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS = frozenset(
    {
        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
        PRISM_REJECTION_INTERNAL_ERROR,
        PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
        PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
        PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
    }
)
DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS = 0.25
DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS = 30.0
# The primary accounting handoff queue must be bounded or the documented
# result-preserving spillover ordering can never engage; the overflow queue
# stays unbounded by design so an already-offered block is never converted
# back into a raw-submit retry.
DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH = 8
BLOCK_SUBMITTER_WAIT_HEARTBEAT_SLICE_SECONDS = 0.25
MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS = 2
BLOCK_CANDIDATE_RETRY_HEARTBEAT_SLICE_SECONDS = 0.25
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
    """RLock with zero shared-metrics work on uncontended acquisitions.

    The coordinator lock protects control-plane publication state. Its
    contention counters intentionally record only acquisitions that fail an
    immediate probe, so observing it cannot recreate the share-path convoy the
    metrics are meant to diagnose.
    """

    def __init__(
        self,
        *,
        wait_observer: Callable[[float], None] | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._metrics_lock = threading.Lock()
        self._contention_count = 0
        self._wait_seconds_sum = 0.0
        self._wait_seconds_max = 0.0
        self._wait_observer = wait_observer

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if not blocking:
            return self._lock.acquire(blocking=False)
        if self._lock.acquire(blocking=False):
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


@dataclass(frozen=True)
class PrismBlockCandidate:
    """A block-worthy submission queued for the block-submitter thread.

    A share that met its target is acknowledged and credited on the client
    thread, then queued here for the submitter to land the block off the hot
    path. When the hash solved the block but missed the share target (floor
    above network difficulty), credit_share_on_accept is set and the candidate
    is instead submitted synchronously by handle_submit: that share is valid
    only if the block lands, so its credit and the miner's accept/reject follow
    the block outcome directly rather than being queued.
    """

    context: PrismJobContext
    submission: direct_stratum.DirectQbitSubmission
    extranonce1_hex: str
    extranonce2_hex: str
    pending_share: PendingShare
    client: ClientState
    credit_share_on_accept: bool = False
    durable_replay: bool = False
    # When this in-process attempt became runnable: live candidates stamp
    # share-accept time, durable outbox replays stamp row-restore time. The
    # block-submit histogram measures from here to submitblock's return --
    # the race-critical interval a lost block round is decided in.
    landed_monotonic: float = field(default_factory=time.monotonic)


@dataclass
class _BlockCandidateDispositionFlight:
    """One same-hash submission guard shared by its holder and waiters."""

    # The node-offer thread acquires this guard and the accounting thread
    # releases it after durable finalization. A plain Lock permits that
    # deliberate ownership transfer; RLock does not.
    lock: threading.Lock = field(default_factory=threading.Lock)
    users: int = 0


@dataclass(frozen=True)
class _BlockCandidateDispositionLease:
    """A same-hash guard held across node offer and durable finalization."""

    block_hash: str
    flight: _BlockCandidateDispositionFlight


@dataclass(frozen=True)
class _BlockCandidateNodeSubmission:
    """Result of the latency-critical qbitd fast-lane call."""

    attempted: bool
    result: object = None
    error: BaseException | None = None


@dataclass
class _BlockSubmitterLedgerCall:
    """One still-running direct outbox call, reused across paced retries."""

    started_monotonic: float = field(default_factory=time.monotonic)
    done: threading.Event = field(default_factory=threading.Event)
    result: object = None
    error: BaseException | None = None


@dataclass
class _BlockSubmitterRpcCall:
    """One hard-deadline, single-flight submitblock transport call."""

    started_monotonic: float = field(default_factory=time.monotonic)
    done: threading.Event = field(default_factory=threading.Event)
    result: object = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _BlockCandidateAccountingTask:
    """A node-offered candidate awaiting serialized durable accounting."""

    candidate: PrismBlockCandidate
    node_submission: _BlockCandidateNodeSubmission
    disposition_lease: _BlockCandidateDispositionLease


class BlockSubmitterDatabaseTimeout(TimeoutError):
    """A submitter ledger phase exceeded its coordinator-side deadline."""


@dataclass(frozen=True)
class _IdleRetargetRequest:
    """Immutable idle-window identity captured by one bounded sweep."""

    client: ClientState
    connection_id: int
    worker: WorkerIdentity
    active_job: PrismJobContext
    window_started_monotonic: float
    current_difficulty: Decimal
    elapsed_seconds: Decimal


class _ReconcileFlight:
    """One in-flight reconcile pass shared by concurrent same-tip callers."""

    __slots__ = ("event", "summary", "exception")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.summary: dict[str, object] | None = None
        self.exception: BaseException | None = None


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


class PrismCoordinator:
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
        self._coordination_blocked_lock = threading.Lock()
        self._coordination_blocked_since_monotonic: float | None = None
        self._publication_watchdog_exit_claimed = False
        self.reorg_reconcile_cache_seconds = job_config.reorg_reconcile_cache_seconds
        self.health_refresh_seconds = lifecycle_config.health_refresh_seconds
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
        self._mining_overload_started_monotonic: float | None = None
        self._mining_delivery_failure_started_monotonic: float | None = None
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
        # header replay across reauthorization/connections is still rejected.
        self._recent_share_lock = threading.Lock()
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
        # Eager background-service root: process loop specifications, their
        # start-once state, and the exact drain handles live in the registry;
        # serve() starts named services at the existing recovery boundaries.
        self._background_services = self._make_background_service_registry()

    def _ensure_share_hot_path_state(self) -> None:
        """Backfill dedicated hot-path locks for lightweight test embedders."""
        if (
            hasattr(self, "_recent_share_lock")
            and hasattr(self, "_share_accounting_lock")
            and hasattr(self, "recent_share_keys")
            and hasattr(self, "share_ack_histograms")
        ):
            return
        with _HOT_PATH_LOCK_INITIALIZATION_LOCK:
            if not hasattr(self, "_recent_share_lock"):
                self._recent_share_lock = threading.Lock()
            if not hasattr(self, "_share_accounting_lock"):
                self._share_accounting_lock = threading.Lock()
            if not hasattr(self, "recent_share_keys"):
                self.recent_share_keys = set()
            if not hasattr(self, "share_ack_histograms"):
                self.share_ack_histograms = {
                    result: {
                        "buckets": {
                            bucket: 0
                            for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS
                        },
                        "sum": 0.0,
                        "count": 0,
                    }
                    for result in PRISM_SHARE_ACK_RESULTS
                }

    def _observe_share_ack_seconds(
        self,
        result: str,
        elapsed_seconds: float,
    ) -> None:
        if result not in PRISM_SHARE_ACK_RESULTS:
            raise ValueError(f"unknown share ack result: {result}")
        self._ensure_share_hot_path_state()
        with self._share_accounting_lock:
            histogram = self.share_ack_histograms[result]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + max(
                0.0, elapsed_seconds
            )
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def share_ack_metrics_lines(self) -> list[str]:
        self._ensure_share_hot_path_state()
        with self._share_accounting_lock:
            histograms = {
                result: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for result, histogram in self.share_ack_histograms.items()
            }
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

    @staticmethod
    def _client_vardiff_lock(client: ClientState) -> threading.RLock:
        return client_vardiff_lock(client)

    def _reserve_recent_share_key(self, share_key: tuple[object, ...]) -> bool:
        self._ensure_share_hot_path_state()
        with self._recent_share_lock:
            if share_key in self.recent_share_keys:
                return False
            if len(self.recent_share_keys) > 50_000:
                self.recent_share_keys.clear()
            self.recent_share_keys.add(share_key)
            return True

    def _forget_recent_share_key(self, share_key: tuple[object, ...]) -> None:
        self._ensure_share_hot_path_state()
        with self._recent_share_lock:
            self.recent_share_keys.discard(share_key)

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
        if not hasattr(self, "_reconcile_flight_lock"):
            self._reconcile_flight_lock = threading.Lock()
        if not hasattr(self, "_reconcile_flights"):
            # In-flight reconcile passes keyed by tip hash. Unflagged callers
            # for a tip already being reconciled await that pass's summary
            # instead of queueing a redundant serialized pass of their own.
            self._reconcile_flights: dict[str, _ReconcileFlight] = {}
        if not hasattr(self, "_reorg_reconcile_trusted_memo"):
            # Monotonic completion time of the last trusted reconcile pass,
            # per tip hash, guarded by self.lock. Entries are unarmed only by
            # an untrusted outcome for their own tip (or a reconcile error,
            # which clears the whole map); see _note_reorg_reconcile_outcome.
            self._reorg_reconcile_trusted_memo: OrderedDict[str, float] = (
                OrderedDict()
            )
        if not hasattr(self, "_reconcile_prefetch_executor_lock"):
            self._reconcile_prefetch_executor_lock = threading.Lock()
        if not hasattr(self, "_reconcile_prefetch_executor"):
            # Single-worker lane that overlaps the tip-refresh reconcile pass
            # with the template fetch. Only the refresh singleflight owner
            # submits, so at most one prefetched pass is in flight; same-tip
            # followers still coalesce through _reconcile_flights.
            self._reconcile_prefetch_executor: ThreadPoolExecutor | None = None
        if not hasattr(self, "_reconcile_prefetch_executor_shutdown"):
            self._reconcile_prefetch_executor_shutdown = False
        if not hasattr(self, "_reconcile_prefetch_pending"):
            # At most one outstanding prefetch, keyed by tip, guarded by
            # _reconcile_prefetch_executor_lock. Failed refresh attempts
            # (for example a template-RPC outage) reuse it instead of
            # queueing another serialized pass per retry.
            self._reconcile_prefetch_pending: (
                tuple[str, Future[bool], bool] | None
            ) = None
        if not hasattr(self, "reorg_reconcile_lookup_counts"):
            # Reconcile demand by (caller path, satisfying source), guarded
            # by self.lock.
            self.reorg_reconcile_lookup_counts = {
                (path, source): 0
                for path in PRISM_REORG_RECONCILE_LOOKUP_PATHS
                for source in PRISM_REORG_RECONCILE_LOOKUP_SOURCES
            }
        if not hasattr(self, "_block_submit_metrics_lock"):
            self._block_submit_metrics_lock = threading.Lock()
        if not hasattr(self, "block_submit_seconds_histogram"):
            # Landed candidate -> submitblock-return interval. Post-submit
            # bookkeeping (audit build, persistence, outbox finalize) is
            # deliberately excluded; the outbox created_at/completed_at span
            # already covers it.
            self.block_submit_seconds_histogram = {
                "buckets": {
                    bucket: 0 for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS
                },
                "sum": 0.0,
                "count": 0,
            }
        if not hasattr(self, "_accounted_accepted_block_hashes"):
            self._accounted_accepted_block_hashes: set[str] = set()
        if not hasattr(self, "_counted_block_candidate_abandonments"):
            self._counted_block_candidate_abandonments: set[str] = set()
        if not hasattr(self, "_outstanding_block_candidate_hashes"):
            self._outstanding_block_candidate_hashes: set[str] = set()
        if not hasattr(self, "_tip_observed_accepted_block_hashes"):
            self._tip_observed_accepted_block_hashes: dict[str, float] = {}
        if not hasattr(self, "_health_snapshot"):
            self._health_snapshot: dict[str, object] | None = None
        if not hasattr(self, "_health_snapshot_monotonic"):
            self._health_snapshot_monotonic: float | None = None
        if not hasattr(self, "_health_refresh_loop_running"):
            self._health_refresh_loop_running = False
        if not hasattr(self, "health_snapshot_refresh_failure_count"):
            self.health_snapshot_refresh_failure_count = 0
        # The aggregate progress-health state lives in its owner service; the
        # legacy raw ``_progress_*`` fields route there through descriptors.
        self._ensure_progress_health_service()

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

    def _full_payout_window_materialization(self, *, snapshot_anchor_ms: int, snapshot_window_weight: int, reason: str, observed_monotonic: float, append_invalidation_epoch: int) -> _PayoutWindowMaterialization:
        """Run the exact ledger oracle and atomically replace cached pages."""
        return self._ensure_payout_state_service()._full_payout_window_materialization(snapshot_anchor_ms=snapshot_anchor_ms, snapshot_window_weight=snapshot_window_weight, reason=reason, observed_monotonic=observed_monotonic, append_invalidation_epoch=append_invalidation_epoch)

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
        if not hasattr(self, "_mining_overload_started_monotonic"):
            self._mining_overload_started_monotonic = None
        if not hasattr(self, "_mining_delivery_failure_started_monotonic"):
            self._mining_delivery_failure_started_monotonic = None

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

    def _reset_delivery_failure_if_coverage_restored_locked(self) -> None:
        authorized_clients = [
            client
            for client in self.clients
            if client.subscribed and client.authorized and client.worker is not None
        ]
        if not authorized_clients:
            self._mining_delivery_failure_started_monotonic = None
            return
        current = sum(
            1
            for client in authorized_clients
            if self._client_has_current_tip_job_locked(client)
        )
        if current / len(authorized_clients) >= 0.95:
            self._mining_delivery_failure_started_monotonic = None

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
        current = threading.get_ident()
        if current == getattr(self, "_block_submitter_thread_ident", None):
            return "block_submitter", "_block_submitter_phase"
        if current == getattr(self, "_block_accounting_thread_ident", None):
            return "block_accounting", "_block_accounting_phase"
        return None

    def _record_block_work_heartbeat(self, name: str, phase: str) -> None:
        """Record a phase while preserving one-argument heartbeat embedders."""
        heartbeat = self._record_heartbeat
        try:
            heartbeat(name, phase=phase)
        except TypeError as exc:
            # Preserve the historical one-argument heartbeat seam used by
            # focused embedders. Do not hide TypeErrors raised by a heartbeat
            # implementation that did accept the keyword.
            if "unexpected keyword argument 'phase'" not in str(exc):
                raise
            heartbeat(name)

    def _record_block_submitter_phase(self, phase: str) -> None:
        """Stamp a named phase only from a dedicated block-work owner."""
        owner = self._block_work_heartbeat_owner()
        if owner is None:
            return
        heartbeat_name, phase_attribute = owner
        setattr(self, phase_attribute, phase)
        self._record_block_work_heartbeat(heartbeat_name, phase)

    def _record_block_submitter_wait(self, phase: str) -> None:
        """Heartbeat owner waits while preserving lightweight test behavior."""
        owner = self._block_work_heartbeat_owner()
        if owner is None and not hasattr(self, "_block_submitter_thread_ident"):
            self._record_heartbeat("block_submitter")
            return
        self._record_block_submitter_phase(phase)

    def _block_work_wait_slice(self) -> float:
        """Choose a polling slice that stays inside the configured watchdog."""
        watchdog_budget = max(
            0.001,
            float(getattr(self, "watchdog_timeout_seconds", 120.0)),
        )
        return min(
            BLOCK_SUBMITTER_WAIT_HEARTBEAT_SLICE_SECONDS,
            max(0.001, watchdog_budget * 0.9),
        )

    def _observe_coordinator_lock_wait(self, elapsed_seconds: float) -> None:
        """Keep a sliced coordinator-lock wait visible and watchdog-safe."""
        owner = self._block_work_heartbeat_owner()
        if owner is None:
            return
        heartbeat_name, phase_attribute = owner
        current_phase = getattr(self, phase_attribute, "unknown")
        wait_phase = f"wait-lock:coordinator-state:{current_phase}"
        self._record_block_work_heartbeat(heartbeat_name, wait_phase)
        now = time.monotonic()
        log_interval = float(
            getattr(
                self,
                "block_submit_lock_wait_log_seconds",
                DEFAULT_BLOCK_SUBMIT_LOCK_WAIT_LOG_SECONDS,
            )
        )
        last_log = float(
            getattr(self, "_block_submitter_last_lock_wait_log_monotonic", 0.0)
        )
        if last_log <= 0 or now - last_log >= log_interval:
            self._block_submitter_last_lock_wait_log_monotonic = now
            print(
                "prism coordinator: block submitter waiting on lock "
                f"lock=coordinator-state phase={current_phase} "
                f"elapsed={elapsed_seconds:.3f}s",
                flush=True,
            )

    def _acquire_block_submitter_lock(self, lock: Any, name: str) -> None:
        """Acquire a submit-path lock in heartbeat/logging slices."""
        owner = self._block_work_heartbeat_owner()
        if owner is None and (
            hasattr(self, "_block_submitter_thread_ident")
            or hasattr(self, "_block_accounting_thread_ident")
        ):
            lock.acquire()
            return
        started = time.monotonic()
        last_log = started
        log_interval = float(
            getattr(
                self,
                "block_submit_lock_wait_log_seconds",
                DEFAULT_BLOCK_SUBMIT_LOCK_WAIT_LOG_SECONDS,
            )
        )
        while not lock.acquire(timeout=self._block_work_wait_slice()):
            phase = f"wait-lock:{name}"
            self._record_block_submitter_wait(phase)
            now = time.monotonic()
            if now - last_log >= log_interval:
                print(
                    "prism coordinator: block submitter waiting on lock "
                    f"lock={name} elapsed={now - started:.3f}s",
                    flush=True,
                )
                last_log = now

    @contextmanager
    def _block_submitter_lock(self, lock: Any, name: str) -> Iterator[None]:
        self._acquire_block_submitter_lock(lock, name)
        try:
            yield
        finally:
            lock.release()

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
        while True:
            if self.stop_event.wait(self.watchdog_interval_seconds):
                if getattr(self, "_fatal_exit_requested", False):
                    print(
                        "prism coordinator: fatal block-work restart requested; "
                        "exiting non-zero even if the main thread is blocked",
                        flush=True,
                    )
                    os._exit(1)
                return
            now = time.monotonic()
            (
                publication_failure,
                coordination_age,
                coordination_budget,
                publication_budget,
            ) = self._publication_watchdog_state(now)
            if publication_failure == "coordination":
                self._watchdog_failure_detail = (
                    "prism coordinator: publication-progress watchdog firing; "
                    "template refresh remained coordination-blocked past the "
                    f"coordination budget={coordination_budget:g}s "
                    f"streak_age={coordination_age:.3f}s"
                )
                self._watchdog_hard_exit("coordination")
            if publication_failure == "publication":
                self._watchdog_failure_detail = (
                    "prism coordinator: publication-progress watchdog firing; "
                    "current tip/generation remained unpublished past the "
                    f"template refresh failure budget={publication_budget:g}s"
                )
                self._watchdog_hard_exit("publication")
            overdue = (
                self._overdue_heartbeats(now)
                if getattr(self, "watchdog_enabled", True)
                else []
            )
            if overdue:
                self._watchdog_failure_detail = (
                    "prism coordinator: liveness watchdog firing; unresponsive "
                    f"subsystems={overdue} "
                    f"timeout={self.watchdog_timeout_seconds:g}s"
                )
                # Queued shares have not been acknowledged. Miners reconnect
                # and retry them after restart; exact-payload replay is
                # idempotent if Postgres committed just before this exit.
                self._watchdog_hard_exit("liveness")

    def _watchdog_hard_exit(
        self,
        reason: str,
        *,
        timeout_seconds: float | None = None,
    ) -> None:
        """Bound a fresh-thread lease release, then terminate unconditionally."""
        try:
            if timeout_seconds is None:
                timeout_seconds = float(
                    getattr(
                        self,
                        "watchdog_lease_release_timeout_seconds",
                        DEFAULT_PRISM_WATCHDOG_LEASE_RELEASE_TIMEOUT_SECONDS,
                    )
                )
            deadline = time.monotonic() + max(0.0, timeout_seconds)
            release_thread = threading.Thread(
                target=self._watchdog_release_ledger_lease,
                args=(reason, deadline),
                name="prism-watchdog-lease-release",
                daemon=True,
            )
            release_thread.start()
            release_thread.join(max(0.0, deadline - time.monotonic()))
        finally:
            # Nothing, including timeout logging or thread-start failure, may
            # extend or suppress the watchdog's terminal action.
            os._exit(1)

    def _watchdog_release_ledger_lease(
        self,
        reason: str,
        deadline: float,
    ) -> bool:
        """Use only the shutdown controller and a fresh DB connection.

        Do not call ``shutdown`` here: cancellation takes ``self.lock``, which
        may belong to the subsystem that triggered the watchdog. Closing
        writer admission plus the controller's tracked-writer barrier retains
        the graceful path's release-withheld invariant without that lock.

        Any best-effort diagnostic is emitted only after lease handling on this
        daemon worker. A blocked container log pipe may park the worker, but it
        cannot precede the release attempt or extend the caller's hard deadline.
        """
        controller = self._ensure_shutdown_controller()
        controller.request_shutdown(None)
        self.stop_event.set()
        if not controller.begin_shutdown(f"watchdog_{reason}"):
            handled = controller.wait_for_lease_handling()
            self._watchdog_exit_diagnostic(reason, lease_handled=handled)
            return handled

        quiesced, _elapsed, _blockers = controller.wait_for_writer_quiescence(
            max(0.0, deadline - time.monotonic())
        )
        if not quiesced:
            self._watchdog_exit_diagnostic(reason, lease_handled=False)
            return False
        if time.monotonic() >= deadline:
            self._watchdog_exit_diagnostic(reason, lease_handled=False)
            return False
        handled = self.release_ledger_lease(
            fresh_connection=True,
            deadline=deadline,
            emit_logs=False,
        )
        self._watchdog_exit_diagnostic(reason, lease_handled=handled)
        return handled

    def _watchdog_exit_diagnostic(
        self,
        reason: str,
        *,
        lease_handled: bool,
    ) -> None:
        """Best-effort logging after the watchdog's safety-critical work."""
        detail = (
            getattr(self, "_ledger_lease_heartbeat_failure_reason", None)
            if reason == "lease_heartbeat"
            else getattr(self, "_watchdog_failure_detail", None)
        )
        try:
            print(
                (detail or f"prism coordinator: {reason} watchdog firing")
                + ". Exiting non-zero so the restart policy recovers the process. "
                + f"lease_handled={lease_handled}",
                flush=True,
            )
        except Exception:
            pass

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
        with self.lock:
            self._block_replay_enumeration_owed_flag = True

    def _clear_block_replay_enumeration_owed(self) -> None:
        with self.lock:
            self._block_replay_enumeration_owed_flag = False

    def _block_replay_enumeration_owed(self) -> bool:
        with self.lock:
            return bool(
                getattr(self, "_block_replay_enumeration_owed_flag", False)
            )

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
        # Until the durable outbox has been enumerated once, this process
        # cannot know whether a pending accepted candidate exists. Child job
        # builds fail closed on that uncertainty instead of snapshotting a
        # payout base that may omit a pending parent's carry (issue #188);
        # replay_pending_block_candidates clears the flag on success.
        self._note_block_replay_enumeration_owed()
        try:
            return self._run_startup_writer_replay(
                self.replay_pending_block_candidates
            )
        except TimeoutError:
            print(
                "prism coordinator: startup block candidate replay timed out "
                "phase=replay-outbox-query "
                f"timeout={self._block_landing_db_timeout():g}s; "
                "continuing startup; block submitter loop will retry and "
                "job builds stay blocked until pending candidates are known",
                flush=True,
            )
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
        # Mark the nonblocking refresher running before the listener can
        # dispatch a request: a probe retrying connection-refused queues in
        # the accept backlog at listen() time, and a handler serving it
        # before the flag is set would take cached_health_payload's legacy
        # inline path -- running the minutes-long accepted-share aggregate
        # on the handler thread and racing the refresher with a duplicate
        # query instead of returning the documented starting state.
        self.start_health_snapshot_refresher()
        # Bind before the health snapshot warm-up completes: the cold
        # accepted-share aggregate can take minutes on a grown ledger, and
        # for that whole window the listener must answer with an explicit
        # starting state instead of connection refused (issue #188 fix 4).
        handler_cls = make_audit_handler(self)
        httpd = ThreadingHTTPServer((self.audit_bind or "127.0.0.1", self.audit_port), handler_cls)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
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

    def _ensure_coordination_blocked_state(self) -> None:
        if not hasattr(self, "_coordination_blocked_lock"):
            self._coordination_blocked_lock = threading.Lock()
        if not hasattr(self, "_coordination_blocked_since_monotonic"):
            self._coordination_blocked_since_monotonic: float | None = None
        if not hasattr(self, "_publication_watchdog_exit_claimed"):
            self._publication_watchdog_exit_claimed = False

    def _record_coordination_blocked_refresh(self, now: float) -> None:
        self._ensure_coordination_blocked_state()
        with self._coordination_blocked_lock:
            if self._publication_watchdog_exit_claimed:
                return
            if self._coordination_blocked_since_monotonic is None:
                self._coordination_blocked_since_monotonic = now

    def _clear_coordination_blocked_streak(self) -> None:
        self._ensure_coordination_blocked_state()
        with self._coordination_blocked_lock:
            # Once the watchdog claims an expired generation, its hard exit is
            # the terminal action. Otherwise a refresh completing between the
            # deadline check and os._exit could appear to cancel an exit that
            # already won the streak lock.
            if not self._publication_watchdog_exit_claimed:
                self._coordination_blocked_since_monotonic = None

    def coordination_blocked_streak_age_seconds(
        self,
        now: float | None = None,
    ) -> float:
        self._ensure_coordination_blocked_state()
        now = time.monotonic() if now is None else now
        with self._coordination_blocked_lock:
            started = self._coordination_blocked_since_monotonic
        return 0.0 if started is None else max(0.0, now - started)

    def coordination_blocked_streak_expired(self, now: float) -> bool:
        budget = float(
            getattr(
                self,
                "coordination_blocked_exit_seconds",
                DEFAULT_PRISM_COORDINATION_BLOCKED_EXIT_SECONDS,
            )
        )
        self._ensure_coordination_blocked_state()
        with self._coordination_blocked_lock:
            started = self._coordination_blocked_since_monotonic
        return bool(
            started is not None
            and (budget <= 0 or now - started >= budget)
        )

    def _publication_watchdog_state(
        self,
        now: float,
    ) -> tuple[str | None, float, float, float]:
        """Arbitrate coordination and ordinary publication deadlines.

        The final decision is serialized with coordination streak changes.
        A streak recorded while the ordinary publication check is in flight
        therefore owns the longer coordination deadline; once either deadline
        is claimed, later refresh results cannot cancel the terminal exit.
        """
        coordination_budget = float(
            getattr(
                self,
                "coordination_blocked_exit_seconds",
                DEFAULT_PRISM_COORDINATION_BLOCKED_EXIT_SECONDS,
            )
        )
        publication_budget = float(
            getattr(
                self,
                "template_refresh_failure_exit_seconds",
                DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
            )
        )
        self._ensure_job_cache_state()
        self._ensure_coordination_blocked_state()

        # This preflight keeps the common healthy case cheap. Its result is
        # revalidated under both state locks before an ordinary exit is
        # claimed, so a concurrent publication cannot leave a stale verdict.
        publication_expired = self.publication_progress_failure_expired(now)
        with self._coordination_blocked_lock:
            started = self._coordination_blocked_since_monotonic
            if started is not None:
                age = max(0.0, now - started)
                if coordination_budget <= 0 or age >= coordination_budget:
                    self._publication_watchdog_exit_claimed = True
                    return (
                        "coordination",
                        age,
                        coordination_budget,
                        publication_budget,
                    )
                return None, age, coordination_budget, publication_budget

            if publication_expired:
                with self._progress_health_lock:
                    divergence_since = (
                        self._progress_publication_divergence_since_monotonic
                    )
                    publication_expired = bool(
                        publication_budget > 0
                        and divergence_since is not None
                        and now - divergence_since >= publication_budget
                    )
                if publication_expired:
                    self._publication_watchdog_exit_claimed = True
                    return (
                        "publication",
                        0.0,
                        coordination_budget,
                        publication_budget,
                    )
            return None, 0.0, coordination_budget, publication_budget

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
        """Record a reconcile outcome in the per-tip trusted memo.

        A trusted pass arms the memo for its own tip. An untrusted outcome
        (superseded publication, untrusted chain view) unarms only its own
        tip: it proves nothing about reconciliations already completed for
        other tips, and unarming them globally forces every job build into a
        redundant full pass. A pass that applied orphan/maturity row
        mutations passes evict_others=True: cached proofs for other tips
        were taken against pre-mutation rows and no longer hold, even if the
        chain later flips back before any tip observation lands. A reconcile
        error passes clear_memo=True; a partially applied ledger mutation
        invalidates every cached outcome. ``proof_epoch`` carries the
        tip-detection epoch the pass started its reads in: arming is refused
        when the epoch moved during the pass, so a flip away and back can
        never re-arm an entry with a proof from the closed epoch (the
        latest-detected-hash guard alone cannot see the round trip).
        """

        self._ensure_job_cache_state()
        now = time.monotonic()
        with self.lock:
            self.last_reorg_reconciled_tip_hash = tip_hash
            self.last_reorg_reconciled_trusted = trusted
            self.last_reorg_reconciled_monotonic = now
            memo = self._reorg_reconcile_trusted_memo
            if clear_memo:
                memo.clear()
                return
            if evict_others:
                for cached_tip in list(memo):
                    if cached_tip != tip_hash:
                        del memo[cached_tip]
            if tip_hash is None:
                return
            if trusted:
                latest = getattr(self, "latest_detected_tip", None)
                if latest is not None and latest[0] != tip_hash:
                    # This pass finished for a tip that is no longer the
                    # newest detected one; its epoch is over. Arming would
                    # re-add an entry the newer observation already evicted
                    # and let a flip-back reuse a pre-flip outcome.
                    return
                if proof_epoch is not None and proof_epoch != int(
                    getattr(self, "tip_detection_epoch", 0)
                ):
                    # The pass spanned a detection cycle; every memo
                    # consumer (refresh joins, initial-job and vardiff-idle
                    # builds) must see a full re-proof instead.
                    return
                memo[tip_hash] = now
                memo.move_to_end(tip_hash)
                while len(memo) > PRISM_REORG_RECONCILE_MEMO_MAX_TIPS:
                    memo.popitem(last=False)
            else:
                memo.pop(tip_hash, None)

    def _evict_reorg_reconcile_memo_for_new_tip_locked(
        self,
        tip_hash: str,
    ) -> None:
        """Drop trusted-reconcile entries for every tip except ``tip_hash``.

        Called under self.lock when a newer tip is detected. A detected flip
        ends the epoch of every previously cached outcome: if the chain later
        flips back to an earlier hash within the cache TTL, pool-block chain
        state must be re-proven by a fresh pass, not assumed from a pre-flip
        reconciliation. Detection is observation-sequenced, so only genuinely
        newer observations evict.
        """

        self._ensure_job_cache_state()
        memo = self._reorg_reconcile_trusted_memo
        for cached_tip in list(memo):
            if cached_tip != tip_hash:
                del memo[cached_tip]

    def ensure_reorg_reconciled_for_current_tip(
        self,
        *,
        expected_tip_hash: str | None = None,
    ) -> bool:
        reconciler_enabled = getattr(self, "reorg_reconciler_enabled", True)
        if not reconciler_enabled and expected_tip_hash is None:
            return True
        current_tip = str(self.rpc.call("getbestblockhash"))
        if expected_tip_hash is not None and current_tip != expected_tip_hash:
            raise TemplateRefreshSuperseded(
                "qbit tip changed while prepared work was queued "
                f"expected={expected_tip_hash} current={current_tip}"
            )
        if not reconciler_enabled:
            return True
        if self._reorg_reconcile_memo_fresh(current_tip):
            self._record_reorg_reconcile_lookup("job_build", "memo_hit")
            return True
        self._record_reorg_reconcile_lookup("job_build", "serial")
        return self.ensure_reorg_reconciled_for_tip(current_tip)

    def _reorg_reconcile_memo_fresh(self, tip_hash: str) -> bool:
        """True when a trusted pass for ``tip_hash`` is inside the cache TTL
        and the live chain view is still trusted.

        A fresh memo entry lets a caller reuse the completed pass instead of
        queueing a redundant serialized one. The memo is per tip: an
        untrusted outcome recorded for another tip never unarms this one.
        The chain-view trust check is NOT cached: headers can run ahead of
        the validated tip without the best block hash changing (an arriving
        reorg), and job issuance must pause immediately, not a TTL later.
        """
        ttl = getattr(
            self,
            "reorg_reconcile_cache_seconds",
            DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
        )
        if ttl <= 0:
            return False
        self._ensure_job_cache_state()
        with self.lock:
            reconciled_monotonic = self._reorg_reconcile_trusted_memo.get(
                tip_hash
            )
        return bool(
            reconciled_monotonic is not None
            and time.monotonic() - reconciled_monotonic <= ttl
            and not self.qbit_chain_view_untrusted()
        )

    def _record_reorg_reconcile_lookup(self, path: str, source: str) -> None:
        if path not in PRISM_REORG_RECONCILE_LOOKUP_PATHS:
            raise ValueError(f"unknown reorg reconcile lookup path: {path}")
        if source not in PRISM_REORG_RECONCILE_LOOKUP_SOURCES:
            raise ValueError(f"unknown reorg reconcile lookup source: {source}")
        self._ensure_job_cache_state()
        with self.lock:
            self.reorg_reconcile_lookup_counts[(path, source)] += 1

    def _reconcile_prefetch_pass(
        self,
        tip_hash: str,
        prove: bool = False,
    ) -> bool:
        """One prefetched reconcile, honoring the memo like the join does.

        A prefetch that queued behind a completed same-tip pass (abandoned
        refresh attempts reuse the slot, but a replaced tip can leave one
        queued) would otherwise re-run the full serialized pass for nothing.
        A proving pass (the serial re-prove branches) bypasses the memo:
        those branches exist precisely because the entry cannot be trusted.
        """
        if not prove and self._reorg_reconcile_memo_fresh(tip_hash):
            return True
        return self.ensure_reorg_reconciled_for_tip(tip_hash)

    def _submit_reconcile_prefetch(
        self,
        tip_hash: str,
        *,
        prove: bool = False,
    ) -> Future[bool] | None:
        """Run one reconcile pass on the prefetch worker so it overlaps the
        caller's template fetch.

        Returns ``None`` once shutdown has retired the executor; the caller
        falls back to its serial pass. At most one prefetch is outstanding:
        a refresh attempt that failed before its join (for example a
        template-RPC outage) leaves its future in the slot, and the retry
        reuses it for the same tip instead of queueing another serialized
        pass behind the first.
        """
        self._ensure_job_cache_state()
        stale_future: Future[bool] | None = None
        future: Future[bool] | None = None
        with self._reconcile_prefetch_executor_lock:
            if self._reconcile_prefetch_executor_shutdown:
                return None
            pending = self._reconcile_prefetch_pending
            if pending is not None:
                pending_tip, pending_future, pending_proves = pending
                if (
                    not pending_future.done()
                    and pending_tip == tip_hash
                    and (pending_proves or not prove)
                ):
                    # A proving pass satisfies both kinds of caller; a
                    # memo-honoring pass cannot satisfy a prove request and
                    # is replaced below like a tip change.
                    return pending_future
                # Replaced tip or completed future: hand the old future off
                # for disposal outside this lock -- cancellation runs done
                # callbacks inline on this thread, and _clear_slot below
                # re-takes the lock.
                self._reconcile_prefetch_pending = None
                if not pending_future.done():
                    stale_future = pending_future
            executor = self._reconcile_prefetch_executor
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="prism-reconcile-prefetch",
                )
                self._reconcile_prefetch_executor = executor
            try:
                future = executor.submit(
                    self._reconcile_prefetch_pass, tip_hash, prove
                )
                self._reconcile_prefetch_pending = (tip_hash, future, prove)
            except RuntimeError:
                # Executor shutdown raced this submit; the serial path
                # covers it (after the stale future is disposed below).
                future = None
        if stale_future is not None and not stale_future.cancel():
            # Already running for a replaced tip; let it finish detached.
            # The slot holds the new tip, so at most one task ever waits
            # behind the running one.
            self._discard_stale_reconcile_prefetch(stale_future)
        if future is None:
            return None

        def _clear_slot(done: Future[bool]) -> None:
            with self._reconcile_prefetch_executor_lock:
                pending_now = self._reconcile_prefetch_pending
                if pending_now is not None and pending_now[1] is done:
                    self._reconcile_prefetch_pending = None

        # Registered outside the slot lock: a completed future runs the
        # callback inline on this thread.
        future.add_done_callback(_clear_slot)
        return future

    @staticmethod
    def _discard_stale_reconcile_prefetch(future: Future[bool]) -> None:
        """Detach a prefetched pass whose tip was superseded during the
        template fetch.

        The pass cannot be cancelled mid-ledger-mutation and already records
        its own outcome/error accounting; the serial pass for the current
        tip re-surfaces any condition that still applies. Consuming the
        result here only prevents an unretrieved-exception warning.
        """

        def _consume(done: Future[bool]) -> None:
            try:
                done.result()
            except BaseException:
                pass

        future.add_done_callback(_consume)

    def _join_reconcile_prefetch_bounded(self, prefetch: Future[bool]) -> bool:
        """Join a reconcile pass under the poll loop's bounded budget.

        On a genuine expiry the pass keeps running in its single prefetch
        slot -- the paced retry re-joins the same future -- and the timeout
        surfaces the normal blocked-retry path, keeping the poll loop's
        liveness heartbeat fed while the pass catches up. The budget is
        clamped below the loop's failure budget so a misconfigured override
        cannot reinstate the park this bound exists to prevent.
        """

        join_timeout = min(
            PRISM_RECONCILE_PREFETCH_JOIN_TIMEOUT_CEILING_SECONDS,
            max(
                0.001,
                float(
                    getattr(
                        self,
                        "reconcile_prefetch_join_timeout_seconds",
                        PRISM_RECONCILE_PREFETCH_JOIN_TIMEOUT_SECONDS,
                    )
                ),
            ),
        )
        try:
            return prefetch.result(timeout=join_timeout)
        except TimeoutError:
            if prefetch.done():
                # The pass itself raised TimeoutError (socket.timeout is
                # TimeoutError here): not a join expiry. Propagate silently
                # so diagnosis points at the pass, not the join.
                raise
            print(
                "prism coordinator: reconcile prefetch join exceeded "
                f"{join_timeout:g}s; retrying refresh pass while it "
                "completes",
                flush=True,
            )
            raise

    def _reconcile_snapshot_tip_bounded(self, tip_hash: str) -> bool:
        """Run a snapshot-tip re-prove off-thread with the bounded join.

        The serial re-prove branches run the same crawling pass as the
        overlapped prefetch; routing them through the prefetch slot gives
        the poll loop one uniform bounded wait. Falls back to the direct
        pass only when the prefetch executor has already been retired at
        shutdown, whose exceptions the caller already maps to a clean exit.
        """

        prefetch = self._submit_reconcile_prefetch(tip_hash, prove=True)
        if prefetch is None:
            return self.ensure_reorg_reconciled_for_tip(tip_hash)
        return self._join_reconcile_prefetch_bounded(prefetch)

    def shutdown_reconcile_prefetch_executor(self) -> None:
        self._ensure_job_cache_state()
        with self._reconcile_prefetch_executor_lock:
            executor = self._reconcile_prefetch_executor
            self._reconcile_prefetch_executor = None
            self._reconcile_prefetch_executor_shutdown = True
            self._reconcile_prefetch_pending = None
        if executor is not None:
            # A pass blocked on writer admission aborts via
            # ShutdownInProgress on its own; never hold shutdown for it.
            executor.shutdown(wait=False, cancel_futures=True)

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
        if not getattr(self, "reorg_reconciler_enabled", True):
            return True
        if _coalesce_same_tip:
            summary = self.reconcile_prism_pool_blocks_once(tip_hash=tip_hash)
        else:
            summary = self.reconcile_prism_pool_blocks_once(
                tip_hash=tip_hash,
                _wait_for_same_tip_flight=False,
            )
        return not bool(summary.get("untrusted") or summary.get("superseded"))

    def qbit_chain_view_untrusted(self) -> bool:
        blockchain_info = self.rpc.call("getblockchaininfo")
        if not isinstance(blockchain_info, dict):
            raise RuntimeError("getblockchaininfo returned non-object")
        public_chain = str(getattr(self, "qbit_chain", "regtest")).lower() in {
            "main",
            "mainnet",
            *TESTNET_QBIT_CHAINS,
        }
        if (
            blockchain_info.get("initialblockdownload") is not False
            if public_chain
            else bool(blockchain_info.get("initialblockdownload"))
        ):
            return True
        blocks_raw = blockchain_info.get("blocks")
        headers_raw = blockchain_info.get("headers")
        if public_chain and (blocks_raw is None or headers_raw is None):
            return True
        if blocks_raw is not None and headers_raw is not None:
            try:
                blocks = int(blocks_raw)
                headers = int(headers_raw)
                if blocks < 0 or headers < 0 or headers != blocks:
                    return True
            except (TypeError, ValueError) as exc:
                raise RuntimeError("getblockchaininfo blocks/headers are not integers") from exc
        return False

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
        """Reconcile pool blocks, coalescing same-tip concurrent callers.

        Unflagged callers asking about a tip whose pass is already in flight
        await that pass and share its summary instead of queueing another
        full serialized pass. Callers carrying side-effect obligations (a
        forced publication or an already-reserved source) and callers without
        a tip always run their own pass. Lock-owning callers may disable
        waiting for an existing flight while still registering as the visible
        leader when no same-tip flight exists.
        """
        self._ensure_job_cache_state()
        if tip_hash is None or _force_publish or _source_reserved:
            return self._reconcile_prism_pool_blocks_serialized(
                tip_hash=tip_hash,
                _force_publish=_force_publish,
                _source_reserved=_source_reserved,
            )
        with self._reconcile_flight_lock:
            flight = self._reconcile_flights.get(tip_hash)
            leading = flight is None
            if leading:
                flight = _ReconcileFlight()
                self._reconcile_flights[tip_hash] = flight
        if not leading:
            if not _wait_for_same_tip_flight:
                return self._reconcile_prism_pool_blocks_serialized(
                    tip_hash=tip_hash
                )
            wait_seconds = float(
                getattr(
                    self,
                    "reconcile_flight_wait_seconds",
                    DEFAULT_PRISM_RECONCILE_FLIGHT_WAIT_SECONDS,
                )
            )
            if flight.event.wait(timeout=wait_seconds):
                exception = flight.exception
                if exception is not None:
                    raise exception
                summary = flight.summary
                assert summary is not None
                # Followers get a copy: summaries are mutable dicts and the
                # leader's caller already holds the original.
                return dict(summary)
            # Liveness backstop: the leader outlived the wait. Run our own
            # pass; the writer lock still serializes the actual work.
            return self._reconcile_prism_pool_blocks_serialized(
                tip_hash=tip_hash
            )
        try:
            summary = self._reconcile_prism_pool_blocks_serialized(
                tip_hash=tip_hash
            )
            flight.summary = summary
            return summary
        except BaseException as exc:
            flight.exception = exc
            raise
        finally:
            with self._reconcile_flight_lock:
                self._reconcile_flights.pop(tip_hash, None)
            flight.event.set()

    @ledger_writer_operation("payout_reconciliation")
    def _reconcile_prism_pool_blocks_serialized(
        self,
        *,
        tip_hash: str | None = None,
        _force_publish: bool = False,
        _source_reserved: bool = False,
    ) -> dict[str, object]:
        """Serialize reconciliation against accepted-block finalization."""
        self._ensure_job_cache_state()
        with self._payout_balance_mutation_lock:
            with self._accepted_block_payout_preview_condition:
                if any(
                    transition.landed
                    for transition in self._accepted_block_payout_previews.values()
                ):
                    raise _PayoutStatePublicationBlocked(
                        "accepted block payout confirmation is still pending"
                    )
            return self._reconcile_prism_pool_blocks_once(
                tip_hash=tip_hash,
                _force_publish=_force_publish,
                _source_reserved=_source_reserved,
            )

    def _reconcile_prism_pool_blocks_once(
        self,
        *,
        tip_hash: str | None = None,
        _force_publish: bool = False,
        _source_reserved: bool = False,
    ) -> dict[str, object]:
        summary: dict[str, object] = {
            "enabled": bool(getattr(self, "reorg_reconciler_enabled", True)),
            "untrusted": False,
            "superseded": False,
            "published_generation": None,
            "watched_blocks": 0,
            "inactive_blocks": 0,
            "reactivated_blocks": 0,
            "matured_payouts": 0,
        }
        if not getattr(self, "reorg_reconciler_enabled", True):
            return summary
        self._ensure_job_cache_state()
        if not _source_reserved and tip_hash is not None:
            # Tip observation normally reserves this source before queueing
            # reconciliation. Direct callers only need a new source when they
            # are asking about a different tip; repeated reconciliation of the
            # same tip must not supersede otherwise valid prepared work.
            with self.lock:
                current_source_tip = self._payout_state_source[1]
            if current_source_tip != tip_hash:
                self._reserve_payout_state_source(
                    "external_tip",
                    tip_hash=tip_hash,
                )

        inactive_blocks_total = 0
        reactivated_blocks_total = 0
        matured_payouts_total = 0
        supersession_retries = 0
        skip_recorded = False
        max_supersession_retries = max(
            0,
            int(
                getattr(
                    self,
                    "payout_reconcile_supersession_retries",
                    DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
                )
            ),
        )

        proof_epoch = int(getattr(self, "tip_detection_epoch", 0))

        def finish(*, trusted: bool) -> dict[str, object]:
            with self.lock:
                self.reorg_inactive_block_count += inactive_blocks_total
                self.reorg_reactivated_block_count += reactivated_blocks_total
                self.matured_payout_count += matured_payouts_total
            self._note_reorg_reconcile_outcome(
                tip_hash,
                trusted=trusted,
                # Row mutations invalidate proofs cached for other tips even
                # when the mutating pass's tip was never observed (per-client
                # callers reconcile straight off getbestblockhash).
                evict_others=bool(
                    inactive_blocks_total
                    or reactivated_blocks_total
                    or matured_payouts_total
                ),
                proof_epoch=proof_epoch,
            )
            summary["inactive_blocks"] = inactive_blocks_total
            summary["reactivated_blocks"] = reactivated_blocks_total
            summary["matured_payouts"] = matured_payouts_total
            return summary

        def retry_superseded_candidate() -> bool:
            nonlocal supersession_retries, tip_hash
            supersession_retries += 1
            if supersession_retries > max_supersession_retries:
                summary["superseded"] = True
                self._block_payout_state_publication()
                return False
            with self.lock:
                latest_tip = self._payout_state_source[1]
            tip_hash = latest_tip or tip_hash
            return True

        while True:
            candidate_to_publish: PayoutStateCandidate | None = None
            error_candidate: PayoutStateCandidate | None = None
            attempt_trusted = True
            # The memo entry this attempt may arm must prove state for the
            # epoch its reads happen in; a detection cycle during the pass
            # (away, or away and back) refuses the arm in
            # _note_reorg_reconcile_outcome.
            proof_epoch = int(getattr(self, "tip_detection_epoch", 0))
            try:
                with self._block_submitter_lock(
                    self._payout_state_prepare_lock,
                    "payout-state-prepare",
                ):
                    prepared_started = time.monotonic()
                    captured_source = self._capture_payout_state_source()
                    payout_changed = False
                    payout_mutation_attempted = False
                    inactive_blocks = 0
                    reactivated_blocks = 0
                    matured_payouts = 0
                    summary["untrusted"] = False
                    summary["watched_blocks"] = 0
                    try:
                        if self.qbit_chain_view_untrusted():
                            if not skip_recorded:
                                with self.lock:
                                    self.reorg_reconcile_skip_count += 1
                                skip_recorded = True
                            summary["untrusted"] = True
                            attempt_trusted = False
                            if _force_publish:
                                candidate_to_publish = (
                                    self._prepared_payout_state_candidate(
                                        captured_source
                                    )
                                )
                        else:
                            active_tip_height = int(self.rpc.call("getblockcount"))
                            watch_blocks = getattr(
                                self.ledger,
                                "reorg_watch_blocks",
                                None,
                            )
                            if not callable(watch_blocks):
                                if (
                                    _force_publish
                                    or self._captured_payout_source_requires_publication(
                                        captured_source
                                    )
                                ):
                                    candidate_to_publish = (
                                        self._prepared_payout_state_candidate(
                                            captured_source,
                                            force_full_window_rescan=payout_changed,
                                        )
                                    )
                            else:
                                rows = watch_blocks(
                                    active_tip_height=active_tip_height
                                )
                                summary["watched_blocks"] = len(rows)

                                for row in rows:
                                    block_height = int(row["block_height"])
                                    block_hash = str(row["block_hash"]).lower()
                                    chain_state = str(row.get("chain_state", ""))
                                    if block_height > active_tip_height:
                                        if chain_state == "confirmed":
                                            payout_mutation_attempted = True
                                            inactive = (
                                                self.ledger.mark_pool_block_inactive(
                                                    block_hash=block_hash,
                                                    active_tip_height=active_tip_height,
                                                )
                                            )
                                            inactive_count = int(
                                                inactive.get("inactive_count", 0)
                                            )
                                            inactive_blocks += inactive_count
                                            payout_changed = (
                                                payout_changed
                                                or bool(inactive_count)
                                            )
                                        continue
                                    active_hash = str(
                                        self.rpc.call(
                                            "getblockhash",
                                            [block_height],
                                        )
                                    ).lower()
                                    on_active_chain = active_hash == block_hash
                                    if (
                                        on_active_chain
                                        and chain_state == "inactive"
                                    ):
                                        payout_mutation_attempted = True
                                        with self._ensure_audit_artifact_store().publication_order_guard():
                                            reactivated = self.ledger.reactivate_pool_block(
                                                block_hash=block_hash,
                                                active_tip_height=active_tip_height,
                                            )
                                        reactivated_count = int(
                                            reactivated.get(
                                                "reactivated_count",
                                                0,
                                            )
                                        )
                                        reactivated_blocks += reactivated_count
                                        payout_changed = (
                                            payout_changed
                                            or bool(reactivated_count)
                                        )
                                    elif (
                                        not on_active_chain
                                        and chain_state == "confirmed"
                                    ):
                                        payout_mutation_attempted = True
                                        inactive = (
                                            self.ledger.mark_pool_block_inactive(
                                                block_hash=block_hash,
                                                active_tip_height=active_tip_height,
                                            )
                                        )
                                        inactive_count = int(
                                            inactive.get("inactive_count", 0)
                                        )
                                        inactive_blocks += inactive_count
                                        payout_changed = (
                                            payout_changed
                                            or bool(inactive_count)
                                        )

                                mark_mature = getattr(
                                    self.ledger,
                                    "mark_mature_pool_payouts",
                                    None,
                                )
                                if callable(mark_mature):
                                    payout_mutation_attempted = True
                                    matured = mark_mature(
                                        active_tip_height=active_tip_height
                                    )
                                    matured_payouts = int(
                                        matured.get("matured_count", 0)
                                    )
                                    payout_changed = (
                                        payout_changed
                                        or bool(matured_payouts)
                                    )

                                inactive_blocks_total += inactive_blocks
                                reactivated_blocks_total += reactivated_blocks
                                matured_payouts_total += matured_payouts
                                if (
                                    payout_changed
                                    or _force_publish
                                    or self._captured_payout_source_requires_publication(
                                        captured_source
                                    )
                                ):
                                    # Candidate preparation embeds the ledger
                                    # snapshot artifact; a pass that will not
                                    # publish must not pay for one only to
                                    # discard it.
                                    candidate_to_publish = (
                                        self._prepared_payout_state_candidate(
                                            captured_source,
                                            force_full_window_rescan=payout_changed,
                                        )
                                    )
                    except Exception:
                        inactive_blocks_total += inactive_blocks
                        reactivated_blocks_total += reactivated_blocks
                        matured_payouts_total += matured_payouts
                        # Durable partial mutations close admission before the
                        # preparation lock is released. Publication drains old
                        # socket sends afterward without blocking new ledger
                        # preparation or snapshot acquisition.
                        if payout_mutation_attempted:
                            # A mutator can commit server-side and still raise
                            # locally when its response is lost. The observed
                            # row counts are then unavailable, so conservatively
                            # force the same ledger re-read as a confirmed
                            # mutation. Read-only failures never reach this flag.
                            payout_changed = True
                        if payout_changed:
                            error_candidate = (
                                self._prepared_payout_state_candidate(
                                    captured_source,
                                    force_full_window_rescan=True,
                                )
                            )
                            self._block_payout_state_publication(force=True)
                        with self.lock:
                            self.reorg_inactive_block_count += (
                                inactive_blocks_total
                            )
                            self.reorg_reactivated_block_count += (
                                reactivated_blocks_total
                            )
                            self.matured_payout_count += matured_payouts_total
                            self.reorg_reconcile_error_count += 1
                        # A pass that errored mid-mutation invalidates every
                        # cached outcome, not just its own tip's.
                        self._note_reorg_reconcile_outcome(
                            tip_hash,
                            trusted=False,
                            clear_memo=True,
                        )
                        raise
                    finally:
                        self._observe_payout_state_seconds(
                            "preparation",
                            max(0.0, time.monotonic() - prepared_started),
                        )

                    if candidate_to_publish is not None:
                        # Atomically fence cache/build/delivery admission before
                        # releasing the ledger snapshot lock. The potentially
                        # slow drain then happens in publication() below.
                        self._block_payout_state_publication(force=True)
            except Exception:
                if error_candidate is not None:
                    if (
                        self._publish_payout_state_candidate(error_candidate)
                        is None
                    ):
                        self._block_payout_state_publication()
                raise

            if candidate_to_publish is not None:
                published = self._publish_payout_state_candidate(
                    candidate_to_publish
                )
                if published is None:
                    # Preserve durable counts and retry iteratively against the
                    # newest source. The explicit budget prevents tip churn
                    # from monopolizing preparation indefinitely; the fence
                    # stays closed between attempts.
                    if retry_superseded_candidate():
                        continue
                    return finish(trusted=False)
                summary["published_generation"] = published
            return finish(trusted=attempt_trusted)

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
        """The difficulty policy for one client: its per-client specialization
        if any, else its listener profile, else the default listener's config
        (clients created without one: tests, legacy callers)."""
        with self._client_vardiff_lock(client):
            return client.vardiff_config or client.listener_vardiff_config or self.vardiff_config

    def client_startup_difficulty(self, profile: StratumListenerProfile | None = None) -> Decimal:
        config = profile.vardiff_config if profile is not None else self.vardiff_config
        fixed_difficulty = profile.share_difficulty if profile is not None else self.share_difficulty
        if not config.enabled:
            return fixed_difficulty
        return vardiff.clamp(
            config.startup_difficulty,
            config.min_difficulty,
            config.max_difficulty,
        )

    def desired_client_share_difficulty(self, client: ClientState) -> Decimal:
        # pending_share_difficulty is set by vardiff retargets and by explicit
        # difficulty requests (d=/suggest_difficulty); either way it applies to
        # the next stamped job regardless of whether vardiff is enabled.
        with self._client_vardiff_lock(client):
            return client.pending_share_difficulty or client.share_difficulty

    def client_minimum_advertised_difficulty(self, client: ClientState) -> Decimal:
        """The difficulty stamped jobs never advertise below for this client.

        Zero everywhere except floor-bearing listeners (the high-diff port),
        where the effective policy floor governs: the listener minimum, raised
        by any md= specialization. The floor overrides the network-difficulty
        cap because the listener's marketplace contract is checked against the
        first advertised difficulty, even while qbit network difficulty sits
        below the floor.
        """
        with self._client_vardiff_lock(client):
            if client.minimum_advertised_difficulty <= 0:
                return Decimal("0")
            config = (
                client.vardiff_config
                or client.listener_vardiff_config
                or self.vardiff_config
            )
            return max(client.minimum_advertised_difficulty, config.min_difficulty)

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
        )

    def coinbase_script_sig_suffix_hex(self, extranonce1_hex: str, extranonce2_hex: str) -> str:
        extranonce1_hex = validate_hex(extranonce1_hex, name="extranonce1")
        extranonce2_hex = validate_hex(extranonce2_hex, name="extranonce2")
        return self.coinbase_tag_hex + extranonce1_hex + extranonce2_hex

    @ledger_writer_operation("share_submission")
    def handle_submit(self, client: ClientState, params: list[object]) -> bool:
        if len(params) < 5:
            self.reject_stratum(
                20,
                PRISM_REJECTION_MALFORMED_SUBMIT,
                "submit params are incomplete",
                worker=client.username or None,
            )
        worker_name, job_id, extranonce2_hex, ntime_hex, nonce_hex = [str(item) for item in params[:5]]
        version_bits_hex = str(params[5]) if len(params) > 5 else None
        if worker_name != client.username:
            self.reject_stratum(
                20,
                PRISM_REJECTION_UNAUTHORIZED_WORKER,
                "submit username does not match authorized username",
                worker=client.username or None,
            )
        # Normal submits cross the coordinator lock once: pool admission, job
        # membership, and published-tip authority are copied atomically. All
        # miner bookkeeping and any live-tip RPC fallback stay outside it.
        pool_closed, context, published_tip = self._submit_control_snapshot(
            client,
            job_id,
        )
        # A closed pool rejects before any share accounting: post-close submits
        # must not inflate global/per-worker submitted totals (the stale-percent
        # denominator) or vardiff windows they can never contribute to.
        if pool_closed:
            self.reject_stratum(
                21,
                PRISM_REJECTION_POOL_CLOSED,
                "pool is no longer accepting shares",
                worker=worker_name,
            )
        if len(extranonce2_hex) != self.extranonce2_size * 2:
            self.reject_stratum(
                20,
                PRISM_REJECTION_INVALID_EXTRANONCE,
                "unexpected extranonce2 size",
                worker=worker_name,
            )
        if len(ntime_hex) != 8 or len(nonce_hex) != 8:
            self.reject_stratum(
                20,
                PRISM_REJECTION_INVALID_NTIME_OR_NONCE,
                "ntime and nonce must be 4-byte hex strings",
                worker=worker_name,
            )
        # Count submitted shares once, after the format checks, so the
        # per-worker counter and the aggregate qbit_prism_submitted_shares_total
        # (via note_vardiff_submitted_share) cover the same population; malformed
        # extranonce/ntime submits are recorded only as rejections, not submits.
        self.note_worker_submitted_share(worker_name)
        self.note_vardiff_submitted_share(client)
        credit_policy: str | None = None
        evicted_entry: EvictedJobEntry | None = None
        if context is None:
            evicted_entry = self.evicted_job_entry(client, job_id)
            if evicted_entry is None:
                self.reject_stratum(
                    21,
                    PRISM_REJECTION_UNKNOWN_JOB,
                    "stale job",
                    worker=worker_name,
                )
        current_tip = (
            published_tip
            if published_tip is not None
            else str(self.rpc.call("getbestblockhash"))
        )
        # Share classification (normal and stale-grace alike) is deliberately
        # point-in-time against this single tip read: a tip that advances
        # between here and the ledger append does not retroactively invalidate
        # the share, exactly as a normal current-tip share stays credited when
        # the tip moves during processing. Re-checking would add an RPC per
        # share during post-block bursts only to reject valid work over
        # processing latency. Block submission is different (chain state):
        # submit_block_candidate re-checks the tip under lock before
        # submitblock, and stale-grace shares never reach it.
        if context is None:
            try:
                evicted_context = self.evicted_submit_context(client, evicted_entry, current_tip)
            except Exception:
                print("prism coordinator: failed to classify evicted submit context", flush=True)
                traceback.print_exc()
                self.reject_stratum(
                    20,
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "failed to classify stale-grace parent tip",
                    worker=worker_name,
                )
            if evicted_context is None:
                self.reject_stratum(
                    21,
                    PRISM_REJECTION_STALE_JOB,
                    "stale job",
                    worker=worker_name,
                )
            context, credit_policy = evicted_context
        elif str(context.template["previousblockhash"]) != current_tip:
            try:
                eligible_for_grace = self.context_eligible_for_stale_grace(client, context, current_tip)
            except Exception:
                print("prism coordinator: failed to classify stale-grace parent tip", flush=True)
                traceback.print_exc()
                self.reject_stratum(
                    20,
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "failed to classify stale-grace parent tip",
                    worker=worker_name,
                )
            if eligible_for_grace:
                credit_policy = PRISM_CREDIT_POLICY_STALE_GRACE
            else:
                self.reject_stratum(
                    21,
                    PRISM_REJECTION_STALE_JOB,
                    "stale job",
                    worker=worker_name,
                )

        submit_version_mask = client.version_mask
        if (
            evicted_entry is not None
            and evicted_entry.connection_id != client.connection_id
        ):
            # In-flight work from the dead connection was rolled under the
            # mask negotiated there; the replacement's own mask (often still
            # 0 before mining.configure) must not judge those version bits.
            submit_version_mask = int(getattr(context, "version_mask", 0))
        try:
            submission = direct_stratum.assemble_submission(
                context.job,
                extranonce2_hex=extranonce2_hex,
                ntime_hex=ntime_hex,
                nonce_hex=nonce_hex,
                version_bits_hex=version_bits_hex,
                version_mask=submit_version_mask,
            )
        except ValueError as exc:
            self.reject_stratum(
                20,
                PRISM_REJECTION_MALFORMED_SUBMIT,
                f"malformed submit: {exc}",
                worker=worker_name,
            )
        # A retained job keeps its original worker even if the connection is
        # later re-authorized. Deduplication must use that immutable identity:
        # otherwise the same header can be replayed under each new username.
        share_key = (context.worker.username, submission.header_hex)
        if not self._reserve_recent_share_key(share_key):
            self.reject_stratum(
                22,
                PRISM_REJECTION_DUPLICATE_SHARE,
                "duplicate share",
                worker=worker_name,
            )
        # A floor-bearing listener holds the advertised share target above the
        # qbit network target while network difficulty sits below the floor,
        # so a submission can solve a block yet miss the share target. Never
        # discard a block over share bookkeeping: reject as low-difficulty
        # only when the hash is not block-worthy. Collection-mode jobs are
        # block-worthy too: their signed bootstrap manifest already commits
        # the whole coinbase to the submitting worker, so the solve settles
        # solver-pays-all instead of being silently ledgered as a share -- a
        # fresh ledger would otherwise withhold every solved block until some
        # later job delivery, stalling a bootstrapping chain.
        block_worthy = (
            submission.block_pass
            and credit_policy != PRISM_CREDIT_POLICY_STALE_GRACE
        )
        if (
            submission.block_pass
            and credit_policy == PRISM_CREDIT_POLICY_STALE_GRACE
        ):
            self._ensure_share_hot_path_state()
            with self._share_accounting_lock:
                dropped_counts = getattr(
                    self,
                    "block_solves_dropped_counts",
                    None,
                )
                if dropped_counts is None:
                    dropped_counts = {"stale_grace": 0}
                    self.block_solves_dropped_counts = dropped_counts
                dropped_counts["stale_grace"] = (
                    int(dropped_counts.get("stale_grace", 0)) + 1
                )
            print(
                "prism coordinator: stale-grace block solve dropped "
                f"hash={submission.block_hash_hex} "
                f"parent={context.template['previousblockhash']}",
                flush=True,
            )
        if block_worthy and context.collection_only:
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
        if not submission.share_pass and not block_worthy:
            self.reject_stratum(
                23,
                PRISM_REJECTION_LOW_DIFFICULTY,
                "low difficulty share",
                worker=worker_name,
            )

        pending_share = self.pending_share_from_submission(
            context=context,
            submission=submission,
            ntime_hex=ntime_hex,
            credit_policy=credit_policy,
        )
        if not block_worthy:
            try:
                self.append_accepted_share(
                    client,
                    context,
                    submission,
                    pending_share,
                    credit_policy=credit_policy,
                )
                if evicted_entry is not None:
                    self.note_evicted_job_submit(
                        credit_policy,
                        cross_connection=(
                            evicted_entry.connection_id
                            != client.connection_id
                        ),
                    )
            except BaseException:
                self._forget_recent_share_key(share_key)
                raise
            return False
        candidate = PrismBlockCandidate(
            context=context,
            submission=submission,
            # The mined coinbase embeds the extranonce1 the job was stamped
            # with. A cross-connection resume submits through a client whose
            # own extranonce1 differs from the retained job's, and the audit
            # bundle suffix must match the coinbase actually in the block,
            # so the job's value is authoritative whenever it carries one.
            extranonce1_hex=str(
                getattr(context.job, "extranonce1_hex", None)
                or client.extranonce1_hex
            ),
            extranonce2_hex=extranonce2_hex,
            pending_share=pending_share,
            client=client,
            credit_share_on_accept=not submission.share_pass,
        )
        if candidate.credit_share_on_accept:
            # The hash solved a block but missed the assigned share target
            # (possible only while the listener floor sits above network
            # difficulty). It is a valid share ONLY if the block lands, so land
            # it synchronously: the miner's accept/reject and the ledger credit
            # then both reflect the real outcome -- never an "accepted" ack with
            # no ledger row. This path is rare (an honest miner does not submit
            # below its assigned target), so it does not affect the async
            # common-path latency. On failure the submitter already recorded
            # the specific block-failure reason; reject the miner as
            # low-difficulty (the share was, after all, below its target). The
        # submitter already recorded the specific block-failure reason in
        # block_candidate_abandoned_counts; reject_stratum additionally counts
        # the miner-facing rejection (globally and per worker) so this rare
            # synchronous path is not missing from the rejection metrics.
            persist_intent = getattr(self.ledger, "persist_block_candidate_intent", None)
            durable_candidate_state: str | None = None
            try:
                candidate_intent = self.block_candidate_intent(candidate)
                if callable(persist_intent):
                    persist_result = self._run_block_submitter_ledger_call(
                        (
                            "persist-candidate-intent",
                            str(candidate.submission.block_hash_hex).lower(),
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
                self._forget_recent_share_key(share_key)
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
                    self._forget_recent_share_key(share_key)
                    raise
            if not block_landed:
                self._forget_recent_share_key(share_key)
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
                        evicted_entry.connection_id != client.connection_id
                    ),
                )
            return False
        # A block-worthy submission that met the share target is a valid share
        # regardless of the block's fate: credit it now, acknowledge the miner
        # immediately, and land the block from the dedicated submitter thread
        # (ckpool/btcpool/StratumV2 semantics). An orphaned candidate keeps its
        # share credit.
        try:
            candidate_intent = self.block_candidate_intent(candidate)
            durable_candidate_state = self.append_accepted_share(
                client,
                context,
                submission,
                pending_share,
                credit_policy=credit_policy,
                candidate_intent=candidate_intent,
            )
            if evicted_entry is not None:
                self.note_evicted_job_submit(
                    credit_policy,
                    cross_connection=(
                        evicted_entry.connection_id != client.connection_id
                    ),
                )
        except BaseException:
            # Idempotent with append_accepted_share's own release; also covers
            # an intent serialization failure before the append started.
            self._finish_pending_share_commit(pending_share)
            self._forget_recent_share_key(share_key)
            raise
        if durable_candidate_state in {"submitted", "abandoned"}:
            self._record_block_candidate_terminal_outcome(
                submission.block_hash_hex,
                accepted=durable_candidate_state == "submitted",
            )
            return False
        self.enqueue_block_candidate(candidate)
        return False

    @staticmethod
    def block_candidate_intent(candidate: PrismBlockCandidate) -> dict[str, Any]:
        """Return the immutable JSON needed to resume a candidate after restart."""
        context = candidate.context
        submission = candidate.submission
        intent = {
            "schema": "qbit.prism.block-candidate-intent.v1",
            "block_hash_hex": str(submission.block_hash_hex).lower(),
            "block_hex": str(getattr(submission, "block_hex", "")),
            "coinbase_tx_hex": str(getattr(submission, "coinbase_tx_hex", "")),
            "parent_hash": str(context.template["previousblockhash"]).lower(),
            "expected_height": int(context.template["height"]),
            "template": {
                "previousblockhash": context.template["previousblockhash"],
                "height": int(context.template["height"]),
                "coinbasevalue": int(context.template["coinbasevalue"]),
            },
            "shares_json": context.shares_json,
            "prior_balances": context.prior_balances,
            "found_block": context.found_block,
            "prospective_prior_balances": (
                [
                    list(row)
                    for row in getattr(
                        context,
                        "prospective_prior_balances",
                        (),
                    )
                ]
                if getattr(context, "prospective_prior_balances", None) is not None
                else None
            ),
            "witness_merkle_leaves_hex": direct_stratum.witness_merkle_leaves_hex(
                getattr(context.job, "transaction_hexes", ())
            ),
            "extranonce1_hex": candidate.extranonce1_hex,
            "extranonce2_hex": candidate.extranonce2_hex,
            "username": context.worker.username,
            "pending_share": dataclasses.asdict(candidate.pending_share),
            "credit_share_on_accept": candidate.credit_share_on_accept,
            "collection_only": bool(context.collection_only),
        }
        # Fail on the client thread before committing a share if a future field
        # introduces a value that cannot survive the durable JSON boundary.
        json.dumps(intent, separators=(",", ":"), sort_keys=True)
        return intent

    @staticmethod
    def block_candidate_from_intent(intent: dict[str, Any]) -> PrismBlockCandidate:
        if intent.get("schema") != "qbit.prism.block-candidate-intent.v1":
            raise ValueError("unsupported block candidate intent schema")
        block_hash = str(intent["block_hash_hex"]).lower()
        template = dict(intent["template"])
        if str(template.get("previousblockhash", "")).lower() != str(intent["parent_hash"]).lower():
            raise ValueError("block candidate parent hash does not match template")
        if int(template.get("height", -1)) != int(intent["expected_height"]):
            raise ValueError("block candidate height does not match template")
        submission = direct_stratum.DirectQbitSubmission(
            coinbase_tx_hex=str(intent["coinbase_tx_hex"]),
            coinbase_txid_preimage_hex="",
            header_hex="",
            block_hex=str(intent["block_hex"]),
            block_hash_hex=block_hash,
            block_hash_int=int(block_hash, 16),
            share_pass=True,
            block_pass=True,
            applied_version_hex="",
        )
        context = PrismJobContext(
            job=SimpleNamespace(
                transaction_hexes=(),
                witness_merkle_leaves_hex=tuple(
                    intent.get("witness_merkle_leaves_hex", [])
                ),
            ),
            template=template,
            shares_json=list(intent["shares_json"]),
            prior_balances=list(intent["prior_balances"]),
            found_block=dict(intent["found_block"]),
            share_weight=0,
            collection_only=bool(intent.get("collection_only", False)),
            worker=WorkerIdentity(
                username=str(intent["username"]),
                payout_address="",
                worker_name=None,
                script_pubkey_hex="",
                p2mr_program_hex="",
            ),
            issued_at_ms=0,
            prospective_prior_balances=(
                tuple(
                    (str(row[0]), str(row[1]), str(row[2]), int(row[3]))
                    for row in intent["prospective_prior_balances"]
                )
                if isinstance(intent.get("prospective_prior_balances"), list)
                else None
            ),
            # Append-invalidation epochs are process-local counters, so a
            # stamp from the process that built this candidate is meaningless
            # after a restart. The negative sentinel tells the landing epoch
            # fence to stand down and instead revalidate the recorded window
            # against the durable ledger at its declared anchor (carry
            # balances do not move on a share append, so the prior-balance
            # fence alone cannot see an omitted late row).
            payout_append_invalidation_epoch=-1,
        )
        return PrismBlockCandidate(
            context=context,
            submission=submission,
            extranonce1_hex=str(intent["extranonce1_hex"]),
            extranonce2_hex=str(intent["extranonce2_hex"]),
            pending_share=PendingShare(**dict(intent["pending_share"])),
            client=SimpleNamespace(username=str(intent["username"])),
            credit_share_on_accept=bool(intent.get("credit_share_on_accept", False)),
        )

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
        with self._client_vardiff_lock(client):
            config = (
                client.vardiff_config
                or client.listener_vardiff_config
                or self.vardiff_config
            )
            if not config.enabled:
                return
            client.vardiff_window_submitted += 1

    def note_vardiff_accepted_share(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        now = time.monotonic()
        with self._client_vardiff_lock(client):
            config = (
                client.vardiff_config
                or client.listener_vardiff_config
                or self.vardiff_config
            )
            if not config.enabled:
                return
            client.vardiff_window_accepted += 1
            client.vardiff_window_work += job.share_difficulty
            elapsed_seconds = Decimal(str(max(0.001, now - client.vardiff_window_started_monotonic)))
            if elapsed_seconds < config.retarget_interval_seconds:
                return
            accepted_shares = client.vardiff_window_accepted
            submitted_shares = client.vardiff_window_submitted
            accepted_difficulty = client.vardiff_window_work
            current_difficulty = client.pending_share_difficulty or client.share_difficulty
            client.vardiff_window_started_monotonic = now
            client.vardiff_window_accepted = 0
            client.vardiff_window_submitted = 0
            client.vardiff_window_work = Decimal("0")
        self.retarget_client(
            client,
            current_difficulty=current_difficulty,
            accepted_shares=accepted_shares,
            submitted_shares=submitted_shares,
            accepted_difficulty=accepted_difficulty,
            elapsed_seconds=elapsed_seconds,
        )

    def _ensure_vardiff_idle_state(self) -> None:
        if not hasattr(self, "_vardiff_idle_lock"):
            self._vardiff_idle_lock = threading.Lock()
        if not hasattr(self, "_vardiff_idle_executor"):
            self._vardiff_idle_executor: ThreadPoolExecutor | None = None
        if not hasattr(self, "_vardiff_idle_executor_shutdown"):
            self._vardiff_idle_executor_shutdown = False
        if not hasattr(self, "_vardiff_idle_pending"):
            self._vardiff_idle_pending: set[tuple[ClientState, int]] = set()
        if not hasattr(self, "vardiff_idle_queue_depth"):
            self.vardiff_idle_queue_depth = 0
        if not hasattr(self, "vardiff_idle_inflight"):
            self.vardiff_idle_inflight = 0
        if not hasattr(self, "vardiff_idle_clients_inspected"):
            self.vardiff_idle_clients_inspected = 0
        if not hasattr(self, "vardiff_idle_skip_counts"):
            self.vardiff_idle_skip_counts = {
                reason: 0 for reason in PRISM_VARDIFF_IDLE_SKIP_REASONS
            }
        if not hasattr(self, "vardiff_idle_task_failures"):
            self.vardiff_idle_task_failures = 0
        for attribute in (
            "vardiff_idle_sweep_histogram",
            "vardiff_idle_task_histogram",
        ):
            if not hasattr(self, attribute):
                setattr(
                    self,
                    attribute,
                    {
                        "buckets": {
                            bucket: 0
                            for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS
                        },
                        "sum": 0.0,
                        "count": 0,
                    },
                )

    def _record_vardiff_idle_skip(self, reason: str) -> None:
        if reason not in PRISM_VARDIFF_IDLE_SKIP_REASONS:
            raise ValueError(f"unknown vardiff idle skip reason: {reason}")
        self._ensure_vardiff_idle_state()
        with self._vardiff_idle_lock:
            self.vardiff_idle_skip_counts[reason] += 1

    def _observe_vardiff_idle_seconds(self, name: str, elapsed_seconds: float) -> None:
        self._ensure_vardiff_idle_state()
        if name not in {"sweep", "task"}:
            raise ValueError(f"unknown vardiff idle histogram: {name}")
        with self._vardiff_idle_lock:
            histogram = getattr(self, f"vardiff_idle_{name}_histogram")
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + elapsed_seconds
            buckets = histogram["buckets"]
            for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

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
        published = getattr(self, "current_tip_first_seen", None)
        latest_detected = getattr(self, "latest_detected_tip", None)
        return bool(
            latest_detected is not None
            and (published is None or latest_detected[0] != published[0])
        )

    def _idle_request_skip_reason(
        self,
        request: _IdleRetargetRequest,
    ) -> str | None:
        client = request.client
        # Take the per-client lock before coordinator admission. A share can
        # delay this client's idle retarget, but it can never make the retarget
        # hold the coordinator lock while waiting and convoy tip publication.
        with self._client_vardiff_lock(client):
            with self.lock:
                if self._vardiff_idle_tip_divergence_locked():
                    return "superseded"
                if (
                    client not in self.clients
                    or getattr(client, "closing", False)
                    or not self.client_can_receive_jobs(client)
                ):
                    return "disconnected"
                if (
                    client.connection_id != request.connection_id
                    or client.worker != request.worker
                    or client.active_job is not request.active_job
                    or (client.pending_share_difficulty or client.share_difficulty)
                    != request.current_difficulty
                ):
                    return "superseded"
                if (
                    client.vardiff_window_started_monotonic
                    != request.window_started_monotonic
                    or client.vardiff_window_accepted != 0
                    or client.vardiff_window_submitted != 0
                ):
                    return "not_idle"
        return None

    def _idle_request_pending(self, request: _IdleRetargetRequest) -> bool:
        self._ensure_vardiff_idle_state()
        with self._vardiff_idle_lock:
            return (
                request.client,
                request.connection_id,
            ) in self._vardiff_idle_pending

    def _finish_idle_retarget_task(
        self,
        key: tuple[ClientState, int],
        queued_monotonic: float,
        *,
        started: bool,
    ) -> None:
        self._ensure_vardiff_idle_state()
        with self._vardiff_idle_lock:
            if key not in self._vardiff_idle_pending:
                return
            self._vardiff_idle_pending.discard(key)
            if started:
                self.vardiff_idle_inflight = max(0, self.vardiff_idle_inflight - 1)
            else:
                self.vardiff_idle_queue_depth = max(
                    0,
                    self.vardiff_idle_queue_depth - 1,
                )
        self._observe_vardiff_idle_seconds(
            "task",
            max(0.0, time.monotonic() - queued_monotonic),
        )

    def _run_idle_retarget_task(
        self,
        request: _IdleRetargetRequest,
        bundle: CachedJobBundle | None,
        queued_monotonic: float,
    ) -> None:
        key = (request.client, request.connection_id)
        self._ensure_vardiff_idle_state()
        with self._vardiff_idle_lock:
            if key not in self._vardiff_idle_pending:
                return
            self.vardiff_idle_queue_depth = max(
                0,
                self.vardiff_idle_queue_depth - 1,
            )
            self.vardiff_idle_inflight += 1
        client = request.client
        delivery_attempted = False
        try:
            reason = self._idle_request_skip_reason(request)
            if reason is not None:
                self._record_vardiff_idle_skip(reason)
                return
            # Readiness may have crossed in the ledger after the sweep's
            # cache-only snapshot. Refresh it on this bounded worker so a
            # cached collection bundle cannot be delivered after the pool is
            # ready for normal payout work.
            self.pool_readiness_latched()
            # Canonicalize the sweep's cache-only snapshot on the dedicated
            # worker. shared_job_bundle() selects the current payout-artifact
            # key and rebinds a ready heavy bundle to the latest same-tip
            # template observation; a miss may build here, never on the sweep.
            bundle = self._build_idle_job_bundle(request)
            reason = self._idle_request_skip_reason(request)
            if reason is not None:
                self._record_vardiff_idle_skip(reason)
                return
            # Prepared bundles bypass _maybe_send_job_locked's normal build
            # admission, so preserve its live reorg/headers/IBD trust guard on
            # the dedicated worker before taking the client lock or sending.
            if not self.ensure_reorg_reconciled_for_current_tip():
                self._record_vardiff_idle_skip("superseded")
                return
            if not client.job_update_lock.acquire(blocking=False):
                self._record_vardiff_idle_skip("busy")
                return
            try:
                reason = self._idle_request_skip_reason(request)
                if reason is not None:
                    self._record_vardiff_idle_skip(reason)
                    return
                with self._job_cache_lock:
                    bundle_current = self._idle_bundle_current_locked(
                        client,
                        bundle,
                        allow_uncached=True,
                    )
                if not bundle_current:
                    self._record_vardiff_idle_skip("superseded")
                    return
                # Everything above this point is coordinator preparation. An
                # OSError there belongs to qbit RPC/ledger I/O, not the miner
                # socket. Only retire the connection after entering the paired
                # client delivery path below.
                delivery_attempted = True
                retargeted = self._retarget_client_locked(
                    client,
                    current_difficulty=request.current_difficulty,
                    accepted_shares=0,
                    submitted_shares=0,
                    accepted_difficulty=Decimal("0"),
                    elapsed_seconds=request.elapsed_seconds,
                    require_idle=True,
                    prepared_bundle=bundle,
                    expected_connection_id=request.connection_id,
                    expected_worker=request.worker,
                    expected_active_job=request.active_job,
                    expected_window_started=request.window_started_monotonic,
                    prepared_bundle_allow_uncached=True,
                )
            finally:
                client.job_update_lock.release()
            if retargeted:
                with self.lock:
                    self.idle_retarget_count = int(
                        getattr(self, "idle_retarget_count", 0)
                    ) + 1
                return
            reason = self._idle_request_skip_reason(request)
            if reason is not None:
                self._record_vardiff_idle_skip(reason)
                return
            with self._job_cache_lock:
                bundle_current = self._idle_bundle_current_locked(
                    client,
                    bundle,
                    allow_uncached=True,
                )
            if not bundle_current:
                self._record_vardiff_idle_skip("superseded")
        except JobBuildSuperseded:
            self._record_vardiff_idle_skip("superseded")
        except OSError:
            with self._vardiff_idle_lock:
                self.vardiff_idle_task_failures += 1
            if delivery_attempted:
                self.disconnect_client(client)
                return
            print(
                "prism coordinator: idle vardiff retarget preparation failed; "
                "keeping client connected",
                flush=True,
            )
            traceback.print_exc()
        except Exception:
            with self._vardiff_idle_lock:
                self.vardiff_idle_task_failures += 1
            print("prism coordinator: idle vardiff retarget task failed", flush=True)
            traceback.print_exc()

    def _enqueue_idle_retarget(
        self,
        request: _IdleRetargetRequest,
        bundle: CachedJobBundle | None,
    ) -> str | None:
        self._ensure_vardiff_idle_state()
        key = (request.client, request.connection_id)
        queued_monotonic = time.monotonic()
        with self._vardiff_idle_lock:
            if self._vardiff_idle_executor_shutdown or key in self._vardiff_idle_pending:
                return "superseded"
            if len(self._vardiff_idle_pending) >= MAX_PENDING_VARDIFF_IDLE_RETARGETS:
                return "queue_full"
            executor = self._vardiff_idle_executor
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=PRISM_VARDIFF_IDLE_RETARGET_MAX_WORKERS,
                    thread_name_prefix="prism-vardiff-idle",
                )
                self._vardiff_idle_executor = executor
            self._vardiff_idle_pending.add(key)
            self.vardiff_idle_queue_depth += 1
            try:
                future = executor.submit(
                    self._run_idle_retarget_task,
                    request,
                    bundle,
                    queued_monotonic,
                )
            except RuntimeError:
                self._vardiff_idle_pending.discard(key)
                self.vardiff_idle_queue_depth = max(
                    0,
                    self.vardiff_idle_queue_depth - 1,
                )
                return "queue_full"

        def finish_task(completed: Future[None]) -> None:
            self._finish_idle_retarget_task(
                key,
                queued_monotonic,
                started=not completed.cancelled(),
            )

        future.add_done_callback(finish_task)
        return None

    def shutdown_vardiff_idle_executor(self) -> None:
        self._ensure_vardiff_idle_state()
        with self._vardiff_idle_lock:
            executor = self._vardiff_idle_executor
            self._vardiff_idle_executor = None
            self._vardiff_idle_executor_shutdown = True
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    def vardiff_idle_sweep_loop(self) -> None:
        while not self.stop_event.wait(self.vardiff_idle_sweep_seconds):
            self._record_heartbeat("vardiff_idle_sweep")
            try:
                queued = self.vardiff_idle_sweep_once()
                if queued:
                    print(
                        f"prism coordinator: idle vardiff sweep queued {queued} client(s)",
                        flush=True,
                    )
            except Exception:
                print("prism coordinator: idle vardiff sweep failed", flush=True)
                traceback.print_exc()

    def vardiff_idle_sweep_once(self) -> int:
        sweep_started = time.monotonic()
        now = time.monotonic()
        queued = 0
        try:
            with self.lock:
                clients = tuple(self.clients)
            self._ensure_vardiff_idle_state()
            with self._vardiff_idle_lock:
                self.vardiff_idle_clients_inspected += len(clients)
            for client in clients:
                self._record_heartbeat("vardiff_idle_sweep")
                with self._client_vardiff_lock(client), self.lock:
                    if self._vardiff_idle_tip_divergence_locked():
                        reason = "superseded"
                        request = None
                    elif (
                        client not in self.clients
                        or not self.client_can_receive_jobs(client)
                    ):
                        reason = "disconnected"
                        request = None
                    else:
                        active_job = client.active_job
                        worker = client.worker
                        config = (
                            client.vardiff_config
                            or client.listener_vardiff_config
                            or self.vardiff_config
                        )
                        if not config.enabled:
                            continue
                        if active_job is None or worker is None:
                            reason = "superseded"
                            request = None
                        else:
                            elapsed = Decimal(
                                str(
                                    max(
                                        0.001,
                                        now
                                        - client.vardiff_window_started_monotonic,
                                    )
                                )
                            )
                            if (
                                elapsed < config.retarget_interval_seconds
                                or client.vardiff_window_accepted != 0
                                or client.vardiff_window_submitted != 0
                            ):
                                reason = "not_idle"
                                request = None
                            else:
                                reason = None
                                request = _IdleRetargetRequest(
                                    client=client,
                                    connection_id=client.connection_id,
                                    worker=worker,
                                    active_job=active_job,
                                    window_started_monotonic=(
                                        client.vardiff_window_started_monotonic
                                    ),
                                    current_difficulty=(
                                        client.pending_share_difficulty
                                        or client.share_difficulty
                                    ),
                                    elapsed_seconds=elapsed,
                                )
                if reason is not None:
                    self._record_vardiff_idle_skip(reason)
                    continue
                assert request is not None
                if self._idle_request_pending(request):
                    self._record_vardiff_idle_skip("superseded")
                    continue
                if not client.job_update_lock.acquire(blocking=False):
                    self._record_vardiff_idle_skip("busy")
                    continue
                try:
                    reason = self._idle_request_skip_reason(request)
                finally:
                    client.job_update_lock.release()
                if reason is not None:
                    self._record_vardiff_idle_skip(reason)
                    continue
                bundle = self._cached_idle_job_bundle(client)
                if bundle is None:
                    # The sweep itself stays cache-only. A missing/expired
                    # bundle is rebuilt only by the dedicated bounded worker,
                    # so the client still makes eventual vardiff progress.
                    self._record_vardiff_idle_skip("cache_miss")
                reason = self._enqueue_idle_retarget(request, bundle)
                if reason is not None:
                    self._record_vardiff_idle_skip(reason)
                    continue
                queued += 1
            return queued
        finally:
            self._record_heartbeat("vardiff_idle_sweep")
            self._observe_vardiff_idle_seconds(
                "sweep",
                max(0.0, time.monotonic() - sweep_started),
            )

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
        acquired = client.job_update_lock.acquire(blocking=not require_idle)
        if not acquired:
            return False
        try:
            if require_idle and prepared_bundle is None:
                prepared_bundle = self._cached_idle_job_bundle(client)
                if prepared_bundle is None:
                    return False
            return self._retarget_client_locked(
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
        finally:
            client.job_update_lock.release()

    def _retarget_client_locked(
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
        prepared_bundle_allow_uncached: bool = False,
    ) -> bool:
        config = self.client_vardiff_config(client)
        if not config.enabled:
            return False
        if require_idle:
            if prepared_bundle is None:
                return False
            with self._client_vardiff_lock(client), self.lock:
                if expected_connection_id is None:
                    expected_connection_id = client.connection_id
                if expected_worker is None:
                    expected_worker = client.worker
                if expected_active_job is None:
                    expected_active_job = client.active_job
                if expected_window_started is None:
                    expected_window_started = client.vardiff_window_started_monotonic
                if (
                    client not in self.clients
                    or getattr(client, "closing", False)
                    or not self.client_can_receive_jobs(client)
                    or client.connection_id != expected_connection_id
                    or client.worker != expected_worker
                    or client.active_job is not expected_active_job
                    or client.vardiff_window_started_monotonic
                    != expected_window_started
                    or client.vardiff_window_accepted != 0
                    or client.vardiff_window_submitted != 0
                ):
                    return False
        observed_difficulty = vardiff.observed_difficulty(
            accepted_difficulty=accepted_difficulty,
            elapsed_seconds=elapsed_seconds,
            target_share_interval_seconds=config.target_share_interval_seconds,
        )
        with self._client_vardiff_lock(client):
            previous_estimate = client.vardiff_difficulty_estimate
        if observed_difficulty is None:
            difficulty_estimate = None
            with self._client_vardiff_lock(client):
                client.vardiff_difficulty_estimate = None
        else:
            difficulty_estimate = vardiff.smooth_difficulty_estimate(
                observed=observed_difficulty,
                previous=previous_estimate,
                config=config,
            )
            with self._client_vardiff_lock(client):
                client.vardiff_difficulty_estimate = difficulty_estimate
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=current_difficulty,
            accepted_shares=accepted_shares,
            elapsed_seconds=elapsed_seconds,
            config=config,
            accepted_difficulty=accepted_difficulty,
            difficulty_estimate=difficulty_estimate,
        )
        if not vardiff.should_retarget(
            current_difficulty,
            next_difficulty,
            config.retarget_tolerance,
        ):
            return False
        idle_window_state: tuple[float, int, int, Decimal] | None = None
        idle_window_reset_at: float | None = None
        with self._client_vardiff_lock(client), self.lock:
            previous_difficulty = client.pending_share_difficulty or client.share_difficulty
            if previous_difficulty != current_difficulty:
                return False
            if require_idle and (
                client not in self.clients
                or getattr(client, "closing", False)
                or not self.client_can_receive_jobs(client)
                or client.connection_id != expected_connection_id
                or client.worker != expected_worker
                or client.active_job is not expected_active_job
                or client.vardiff_window_started_monotonic
                != expected_window_started
                or client.vardiff_window_accepted != 0
                or client.vardiff_window_submitted != 0
            ):
                # A share landed since the idle snapshot; the accept path owns
                # this window. Abort the speculative step-down rather than
                # overriding a client that just resumed submitting.
                return False
            if require_idle:
                idle_window_state = (
                    client.vardiff_window_started_monotonic,
                    client.vardiff_window_accepted,
                    client.vardiff_window_submitted,
                    client.vardiff_window_work,
                )
            prior_pending = client.pending_share_difficulty
            client.pending_share_difficulty = next_difficulty
        # Advertise the new difficulty only with its corresponding job. Idle
        # retargets stamp an already-cached bundle; normal share-driven
        # retargets retain the existing build path. Either path sends the pair
        # together or restores the prior pending difficulty/window state.
        def idle_commit_guard() -> bool:
            nonlocal idle_window_reset_at
            if not require_idle:
                return True
            # _maybe_send_job_locked holds vardiff_lock before entering the
            # coordinator commit section, so this guard never waits under
            # self.lock.
            if (
                self.stop_event.is_set()
                or client not in self.clients
                or getattr(client, "closing", False)
                or not self.client_can_receive_jobs(client)
                or client.connection_id != expected_connection_id
                or client.worker != expected_worker
                or client.active_job is not expected_active_job
                or client.vardiff_window_started_monotonic
                != expected_window_started
                or client.vardiff_window_accepted != 0
                or client.vardiff_window_submitted != 0
                or client.pending_share_difficulty != next_difficulty
            ):
                return False
            idle_window_reset_at = time.monotonic()
            client.vardiff_window_started_monotonic = idle_window_reset_at
            client.vardiff_window_accepted = 0
            client.vardiff_window_submitted = 0
            client.vardiff_window_work = Decimal("0")
            return True

        def restore_speculative_retarget() -> None:
            with self._client_vardiff_lock(client), self.lock:
                if client.pending_share_difficulty == next_difficulty:
                    client.pending_share_difficulty = prior_pending
                self._restore_idle_window_state(
                    client,
                    idle_window_state,
                    idle_window_reset_at,
                )

        # A same-tip retarget only needs to flush in-flight work on a
        # step-down: firmware that applies mining.set_difficulty retroactively
        # would otherwise submit sub-target shares against the old job. On a
        # step-up the old job stays valid at its own stamped share_target, so
        # keeping it avoids discarding the miner's in-flight work.
        clean_jobs = next_difficulty < current_difficulty
        try:
            if require_idle:
                sent = self._maybe_send_job_locked(
                    client,
                    clean_jobs=clean_jobs,
                    raise_on_build_failure=True,
                    prepared_bundle=prepared_bundle,
                    commit_guard=idle_commit_guard,
                    commit_guard_lock=self._client_vardiff_lock(client),
                    prepared_bundle_allow_uncached=(
                        prepared_bundle_allow_uncached
                    ),
                )
            else:
                sent = bool(
                    client.authorized
                    and client.subscribed
                    and not self.stop_event.is_set()
                    and self.maybe_send_job(client, clean_jobs=clean_jobs)
                )
            # A completed paired send is the commit point. Shutdown may race
            # immediately afterward, but it cannot make already-delivered work
            # speculative again.
            if sent:
                return True
        except Exception:
            # Cached stamping can surface _JobBuildFailed before delivery, and
            # socket errors can surface during the paired send. Both must undo
            # every speculative client mutation before the task reports failure.
            restore_speculative_retarget()
            raise
        restore_speculative_retarget()
        return False

    @staticmethod
    def _restore_idle_window_state(
        client: ClientState,
        idle_window_state: tuple[float, int, int, Decimal] | None,
        idle_window_reset_at: float | None,
    ) -> None:
        """Un-restart the idle vardiff window after a step-down that never
        reached the miner (skipped build/send), so the next sweep can retry
        immediately instead of waiting out another full retarget interval.
        Caller must hold the client's vardiff_lock. No-op unless this retarget did the reset
        and nothing else has restarted the window since."""
        if idle_window_reset_at is None or idle_window_state is None:
            return
        if (
            client.vardiff_window_started_monotonic == idle_window_reset_at
            and client.vardiff_window_accepted == 0
            and client.vardiff_window_submitted == 0
            and client.vardiff_window_work == 0
        ):
            (
                client.vardiff_window_started_monotonic,
                client.vardiff_window_accepted,
                client.vardiff_window_submitted,
                client.vardiff_window_work,
            ) = idle_window_state

    def enqueue_block_candidate(self, candidate: PrismBlockCandidate) -> bool:
        # Outstanding from admission (even when the wakeup coalesces below:
        # the durable outbox row keeps the candidate replayable), so a tip
        # observation can register acceptance before the submitter drains it.
        self._register_outstanding_block_candidate(
            str(candidate.submission.block_hash_hex)
        )
        queue_obj = getattr(self, "block_candidate_queue", None)
        if queue_obj is None:
            queue_obj = queue.Queue(maxsize=MAX_PENDING_BLOCK_CANDIDATES)
            self.block_candidate_queue = queue_obj
        try:
            queue_obj.put_nowait(candidate)
            return True
        except queue.Full:
            # The candidate is already durable. A full queue merely coalesces
            # this wakeup; the submitter re-reads pending outbox rows whenever
            # it drains the queue, so no candidate is discarded.
            with self.lock:
                self.block_candidate_wakeups_coalesced = int(
                    getattr(self, "block_candidate_wakeups_coalesced", 0)
                ) + 1
            print(
                "prism coordinator: block candidate wakeup coalesced "
                f"hash={candidate.submission.block_hash_hex} (submitter queue full)",
                flush=True,
            )
            return False

    def _ensure_block_replay_state(self) -> None:
        """Backfill replay/maintenance queues for lightweight coordinators."""
        with _HOT_PATH_LOCK_INITIALIZATION_LOCK:
            if not hasattr(self, "_block_replay_candidate_queue"):
                self._block_replay_candidate_queue = queue.Queue()
            if not hasattr(self, "_block_replay_inflight_hashes"):
                self._block_replay_inflight_hashes: set[str] = set()
            if not hasattr(self, "_block_quarantine_queue"):
                self._block_quarantine_queue = queue.Queue()
            if not hasattr(self, "_block_quarantine_hashes"):
                self._block_quarantine_hashes: set[str] = set()

    def _enqueue_replayed_block_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> bool:
        """Queue one durable replay behind live solves, once per process."""
        self._ensure_block_replay_state()
        block_hash = str(candidate.submission.block_hash_hex).lower()
        with self.lock:
            if (
                block_hash in self._block_replay_inflight_hashes
                or block_hash
                in getattr(self, "_block_candidate_terminal_outcomes", {})
            ):
                return False
            self._block_replay_inflight_hashes.add(block_hash)
        try:
            # This is an in-memory condition update, not accounting. Install
            # it before making the candidate visible to the raw lane so
            # startup prewarm cannot build a child from the old payout base
            # after qbitd accepts but before the RPC response returns.
            self._begin_accepted_block_payout_preview(
                block_hash,
                block_height=int(candidate.context.template["height"]),
            )
            self._block_replay_candidate_queue.put_nowait(candidate)
        except BaseException:
            with self.lock:
                self._block_replay_inflight_hashes.discard(block_hash)
            raise
        return True

    def _queue_invalid_block_candidate_for_quarantine(
        self,
        block_hash: str,
        error: str,
    ) -> None:
        """Move malformed-row cleanup off the node-offer lane."""
        if not block_hash:
            return
        self._ensure_block_replay_state()
        key = block_hash.lower()
        with self.lock:
            if key in self._block_quarantine_hashes:
                return
            self._block_quarantine_hashes.add(key)
        self._block_quarantine_queue.put_nowait((key, error))

    @ledger_writer_operation("accepted_block_handling")
    def replay_pending_block_candidates(self) -> int:
        """Queue durable candidate intents not completed by an earlier process."""
        self._record_block_submitter_phase("replay-check-memory")
        # While startup enumeration is still owed, correctness requires the
        # outbox query even if live candidates are queued: job builds stay
        # blocked until pending candidates are known, and only a successful
        # enumeration can unblock them.
        enumeration_owed = self._block_replay_enumeration_owed()
        with self.lock:
            if (
                not enumeration_owed
                and getattr(self, "_retry_block_candidate", None) is not None
            ):
                return 0
        # A live wakeup is already the lowest-latency route to qbitd. Never
        # park it behind the outbox query that exists only to recover missing
        # wakeups after queue pressure or restart.
        queue_obj = getattr(self, "block_candidate_queue", None)
        if not enumeration_owed and queue_obj is not None and not queue_obj.empty():
            return 0
        self._ensure_block_replay_state()
        if not enumeration_owed and not self._block_replay_candidate_queue.empty():
            return 0
        # The startup enumeration gates job issuance, so it runs with the
        # landing-class budget instead of the poll budget (issue #188 fix 4);
        # the periodic steady-state poll keeps the tight budget. The metrics
        # class follows the budget so a slow or timed-out startup enumeration
        # surfaces on the landing series the alerts watch.
        replay_query_timeout = (
            self._block_landing_db_timeout() if enumeration_owed else None
        )
        replay_query_call_class = "landing" if enumeration_owed else "fast"
        pending_rows = getattr(self.ledger, "pending_block_candidate_rows", None)
        if callable(pending_rows):

            def fetch_durable_rows(limit: int) -> list[Any]:
                return self._run_block_submitter_ledger_call(
                    ("replay-outbox-query", limit),
                    "replay-outbox-query",
                    # Restore a batch with no per-row database work. In-flight
                    # dedupe lets later rows reach qbitd even while the oldest
                    # candidate is still accounting.
                    lambda: pending_rows(limit=limit),
                    timeout_seconds=replay_query_timeout,
                    call_class=replay_query_call_class,
                )

        else:
            pending = getattr(self.ledger, "pending_block_candidates", None)
            if not callable(pending):
                self._clear_block_replay_enumeration_owed()
                return 0

            def fetch_durable_rows(limit: int) -> list[Any]:
                pending_intents = self._run_block_submitter_ledger_call(
                    ("replay-outbox-query", limit),
                    "replay-outbox-query",
                    lambda: pending(limit=limit),
                    timeout_seconds=replay_query_timeout,
                    call_class=replay_query_call_class,
                )
                return [
                    {
                        "block_hash": (
                            intent.get("block_hash_hex", "")
                            if isinstance(intent, dict)
                            else ""
                        ),
                        "candidate": intent,
                    }
                    for intent in pending_intents
                ]

        queued = 0
        enumeration_truncated = False
        enumeration_limit = MAX_PENDING_BLOCK_CANDIDATES
        while True:
            durable_rows = fetch_durable_rows(enumeration_limit)
            self._record_block_submitter_phase("replay-restore")
            for durable_row in durable_rows:
                durable_block_hash = ""
                try:
                    if not isinstance(durable_row, dict):
                        raise ValueError("durable block candidate row is not an object")
                    durable_block_hash = str(durable_row["block_hash"]).lower()
                    intent = durable_row["candidate"]
                    if not isinstance(intent, dict):
                        raise ValueError("durable block candidate intent is not an object")
                    intent_block_hash = str(intent.get("block_hash_hex", "")).lower()
                    if not durable_block_hash or intent_block_hash != durable_block_hash:
                        raise ValueError("durable block candidate row key does not match intent")
                    candidate = dataclass_replace(
                        self.block_candidate_from_intent(intent),
                        durable_replay=True,
                    )
                    # Durable acceptance-state reads stay in accounting. The
                    # separate replay queue keeps these recovered rows behind any
                    # live solve while still exposing the whole batch to qbitd.
                    if self._enqueue_replayed_block_candidate(candidate):
                        queued += 1
                except Exception:
                    print("prism coordinator: invalid durable block candidate intent", flush=True)
                    traceback.print_exc()
                    self._queue_invalid_block_candidate_for_quarantine(
                        durable_block_hash,
                        "invalid durable candidate intent",
                    )
            if len(durable_rows) < enumeration_limit or not enumeration_owed:
                break
            # A full batch may hide more pending rows, and a hidden row could
            # be the active parent whose carry a child job must not omit.
            # Re-query with a doubled window until the result is provably
            # untruncated; in-flight dedupe makes re-seen rows free.
            if enumeration_limit >= MAX_BLOCK_REPLAY_ENUMERATION_ROWS:
                enumeration_truncated = True
                print(
                    "prism coordinator: pending block candidate enumeration "
                    f"still truncated at {enumeration_limit} rows; job builds "
                    "stay blocked until a complete enumeration succeeds",
                    flush=True,
                )
                break
            enumeration_limit = min(
                enumeration_limit * 2,
                MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
            )
        if not enumeration_truncated:
            # Every pending candidate is now known and its payout barrier armed
            # (or quarantined), so child job builds may proceed. A truncated
            # pass instead leaves enumeration owed: the queued batch drains,
            # and the submitter loop re-enumerates the remainder.
            self._clear_block_replay_enumeration_owed()
            self._record_startup_phase_once("block_replay_enumerated")
        if queued:
            print(
                f"prism coordinator: replayed {queued} pending block candidate(s)",
                flush=True,
            )
        return queued

    def _ensure_block_submitter_retry_state(self) -> None:
        if not hasattr(self, "_block_submitter_retry_state_lock"):
            self._block_submitter_retry_state_lock = threading.Lock()
        if not hasattr(self, "_block_submitter_backoff_started_monotonic"):
            self._block_submitter_backoff_started_monotonic = None
        if not hasattr(self, "_block_submitter_backoff_deadline_monotonic"):
            self._block_submitter_backoff_deadline_monotonic = None
        if not hasattr(self, "_block_submitter_backoff_delay_seconds"):
            self._block_submitter_backoff_delay_seconds = 0.0

    def _ensure_block_submitter_ledger_call_state(self) -> None:
        if not hasattr(self, "_block_submitter_ledger_calls_lock"):
            self._block_submitter_ledger_calls_lock = threading.Lock()
        if not hasattr(self, "_block_submitter_ledger_calls"):
            self._block_submitter_ledger_calls = {}
        if not hasattr(self, "_block_submitter_ledger_worker_slots"):
            self._block_submitter_ledger_worker_slots = threading.BoundedSemaphore(
                MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS
            )

    def _block_submitter_db_timeout(self) -> float:
        return max(
            0.001,
            float(
                getattr(
                    self,
                    "block_submit_db_timeout_seconds",
                    DEFAULT_BLOCK_SUBMIT_DB_TIMEOUT_SECONDS,
                )
            ),
        )

    def _block_landing_db_timeout(self, block_hash: str | None = None) -> float:
        """Landing-class deadline, escalated after observed landing timeouts.

        The first attempt already receives the full landing budget; a known
        landing-class operation never begins at the one-second poll budget.
        Escalation doubles per timed-out landing attempt for the same block
        hash up to the reviewed cap.
        """
        base = max(
            0.001,
            float(
                getattr(
                    self,
                    "block_landing_db_timeout_seconds",
                    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_SECONDS,
                )
            ),
        )
        cap = max(
            base,
            float(
                getattr(
                    self,
                    "block_landing_db_timeout_max_seconds",
                    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_MAX_SECONDS,
                )
            ),
        )
        timeouts = 0
        if block_hash is not None:
            with self.lock:
                counts = getattr(self, "_block_landing_timeout_counts", None)
                if counts is not None:
                    timeouts = int(counts.get(block_hash, 0))
        return min(cap, base * (2.0 ** min(timeouts, 8)))

    def _note_block_landing_timeout(self, block_hash: str | None) -> None:
        if block_hash is None:
            return
        with self.lock:
            counts = getattr(self, "_block_landing_timeout_counts", None)
            if counts is None:
                counts = {}
                self._block_landing_timeout_counts = counts
            counts[block_hash] = int(counts.get(block_hash, 0)) + 1

    def _ensure_block_ledger_call_metrics(self) -> None:
        if not hasattr(self, "_block_ledger_call_metrics_lock"):
            self._block_ledger_call_metrics_lock = threading.Lock()
        if not hasattr(self, "_block_ledger_call_metrics"):
            self._block_ledger_call_metrics = {}

    def _record_block_ledger_call(
        self,
        *,
        call_class: str,
        budget_seconds: float,
        duration_seconds: float,
        timed_out: bool,
    ) -> None:
        """Track per-call-class submitter ledger latency and timeout counts."""
        self._ensure_block_ledger_call_metrics()
        with self._block_ledger_call_metrics_lock:
            stats = self._block_ledger_call_metrics.setdefault(
                call_class,
                {
                    "calls_total": 0,
                    "timeouts_total": 0,
                    "last_budget_seconds": 0.0,
                    "last_duration_seconds": 0.0,
                    "max_duration_seconds": 0.0,
                },
            )
            stats["calls_total"] = int(stats["calls_total"]) + 1
            if timed_out:
                stats["timeouts_total"] = int(stats["timeouts_total"]) + 1
            stats["last_budget_seconds"] = float(budget_seconds)
            stats["last_duration_seconds"] = float(duration_seconds)
            stats["max_duration_seconds"] = max(
                float(stats["max_duration_seconds"]), float(duration_seconds)
            )

    def block_ledger_call_class_metrics(self) -> dict[str, dict[str, float | int]]:
        self._ensure_block_ledger_call_metrics()
        with self._block_ledger_call_metrics_lock:
            return {
                call_class: dict(stats)
                for call_class, stats in self._block_ledger_call_metrics.items()
            }

    def _block_submitter_stuck_call_exit_timeout(self) -> float:
        return max(
            0.001,
            float(
                getattr(
                    self,
                    "block_submit_stuck_call_exit_seconds",
                    DEFAULT_BLOCK_SUBMIT_STUCK_CALL_EXIT_SECONDS,
                )
            ),
        )

    def _maybe_restart_for_stuck_block_call(
        self,
        *,
        kind: str,
        started_monotonic: float,
    ) -> None:
        """Fail stop when a poisoned worker pool stays exhausted."""
        age_seconds = max(0.0, time.monotonic() - started_monotonic)
        exit_seconds = self._block_submitter_stuck_call_exit_timeout()
        if age_seconds < exit_seconds:
            return
        stop_event = getattr(self, "stop_event", None)
        if stop_event is not None and stop_event.is_set():
            return
        print(
            "prism coordinator: block work call remained stuck; requesting "
            f"restart kind={kind} age={age_seconds:.3f}s "
            f"budget={exit_seconds:g}s",
            flush=True,
        )
        self._fatal_exit_requested = True
        self.request_shutdown()

    def _maybe_restart_for_exhausted_block_call_pool(
        self,
        *,
        kind: str,
        calls_lock: threading.Lock,
        calls: dict[Any, Any],
    ) -> None:
        """Age an exhausted pool even when retries reuse existing calls."""
        with calls_lock:
            active_starts = [
                pending.started_monotonic
                for pending in calls.values()
                if not pending.done.is_set()
            ]
        if len(active_starts) < MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS:
            return
        self._maybe_restart_for_stuck_block_call(
            kind=kind,
            started_monotonic=min(active_starts),
        )

    @contextmanager
    def _block_submitter_ledger_timeout_scope(
        self,
        timeout_seconds: float | None = None,
    ) -> Iterator[None]:
        """Apply the submitter's PostgreSQL deadline when the ledger supports it."""
        operation_timeout = getattr(self.ledger, "operation_timeout", None)
        if not callable(operation_timeout):
            yield
            return
        with operation_timeout(
            self._block_submitter_db_timeout()
            if timeout_seconds is None
            else timeout_seconds
        ):
            yield

    @contextmanager
    def _block_submitter_ledger_statement_timeout_scope(self) -> Iterator[None]:
        """Give each post-submit ledger step a fresh short deadline."""
        statement_timeout = getattr(self.ledger, "statement_timeout", None)
        if callable(statement_timeout):
            with statement_timeout(self._block_submitter_db_timeout()):
                yield
            return
        # Duck-typed ledgers predating per-statement scopes still receive a
        # bounded operation, even though their budget spans the whole tail.
        with self._block_submitter_ledger_timeout_scope():
            yield

    @contextmanager
    def _block_landing_ledger_statement_timeout_scope(
        self,
        block_hash: str | None = None,
    ) -> Iterator[None]:
        """Give each landing-class ledger step the landing budget.

        The accounting tail that lands an accepted block (persist, prior
        balances, confirm, and the prepared-state rejection of a terminal
        candidate) runs under this scope instead of the poll-class one.
        Timed-out steps are recorded so the next attempt for the same block
        hash escalates its budget; the ledger backends already guarantee
        server-side cancellation completes and the pooled session is rolled
        back or replaced before the paced retry re-enters here.

        The guarded body also runs node RPCs and audit/build work. Only a
        ledger-originated deadline may escalate the next landing budget or
        fire the landing-timeout alert: a node RPC timeout is not a database
        cancellation, and escalating on it would page and widen PostgreSQL
        deadlines for a database that never missed one.
        """
        timeout_seconds = self._block_landing_db_timeout(block_hash)
        scope = getattr(self.ledger, "statement_timeout", None)
        if not callable(scope):
            scope = getattr(self.ledger, "operation_timeout", None)
        started = time.monotonic()
        timed_out = False
        try:
            if callable(scope):
                with scope(timeout_seconds):
                    yield
            else:
                yield
        except (LedgerOperationTimeout, BlockSubmitterDatabaseTimeout):
            timed_out = True
            self._note_block_landing_timeout(block_hash)
            raise
        finally:
            self._record_block_ledger_call(
                call_class="landing",
                budget_seconds=timeout_seconds,
                duration_seconds=max(0.0, time.monotonic() - started),
                timed_out=timed_out,
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
        """Run one direct outbox call without letting its driver wedge us.

        A timed-out call remains registered and is reused by the next paced
        retry. This bounds the coordinator-side wait without spawning an
        unbounded pile of threads when a fake/misbehaving driver ignores the
        real PostgreSQL statement deadline. Candidate outbox mutations are
        idempotent, so a late completion converges with replay.

        call_class labels the per-class latency/timeout metrics and must
        match the budget in use: a call given the landing deadline records
        as "landing" so the landing-timeout alert covers it, instead of
        inflating the fast-call budget gauge.
        """
        if timeout_seconds is None:
            timeout_seconds = self._block_submitter_db_timeout()
        else:
            timeout_seconds = max(0.001, float(timeout_seconds))
        self._ensure_block_submitter_ledger_call_state()
        with self._block_submitter_ledger_calls_lock:
            call = self._block_submitter_ledger_calls.get(key)
            if call is None:
                if not self._block_submitter_ledger_worker_slots.acquire(
                    blocking=False
                ):
                    oldest_started = min(
                        (
                            pending.started_monotonic
                            for pending in self._block_submitter_ledger_calls.values()
                            if not pending.done.is_set()
                        ),
                        default=time.monotonic(),
                    )
                    self._maybe_restart_for_stuck_block_call(
                        kind="ledger-worker-pool",
                        started_monotonic=oldest_started,
                    )
                    raise BlockSubmitterDatabaseTimeout(
                        f"{phase} could not acquire a bounded ledger worker"
                    )
                call = _BlockSubmitterLedgerCall()
                self._block_submitter_ledger_calls[key] = call

                def run() -> None:
                    try:
                        with self._block_submitter_ledger_timeout_scope(
                            timeout_seconds
                        ):
                            call.result = operation()
                    except BaseException as exc:
                        call.error = exc
                    finally:
                        call.done.set()
                        self._block_submitter_ledger_worker_slots.release()

                threading.Thread(
                    target=run,
                    name=f"prism-block-ledger-{phase}",
                    daemon=True,
                ).start()

        deadline = time.monotonic() + timeout_seconds
        while not call.done.is_set():
            self._record_block_submitter_wait(phase)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._maybe_restart_for_exhausted_block_call_pool(
                    kind="ledger-worker-pool",
                    calls_lock=self._block_submitter_ledger_calls_lock,
                    calls=self._block_submitter_ledger_calls,
                )
                print(
                    "prism coordinator: block submitter ledger phase timed out "
                    f"phase={phase} timeout={timeout_seconds:g}s",
                    flush=True,
                )
                self._record_block_ledger_call(
                    call_class=call_class,
                    budget_seconds=timeout_seconds,
                    duration_seconds=timeout_seconds,
                    timed_out=True,
                )
                raise BlockSubmitterDatabaseTimeout(
                    f"{phase} exceeded {timeout_seconds:g}s"
                )
            call.done.wait(
                min(
                    remaining,
                    self._block_work_wait_slice(),
                )
            )
        self._record_block_submitter_wait(f"{phase}:complete")
        # A server-side deadline normally completes the worker with a ledger
        # timeout error before the coordinator-side wait expires. Every
        # operation behind this wrapper is a database call, so a completed
        # call carrying a timeout error is still a timed-out call for the
        # per-class alert series.
        self._record_block_ledger_call(
            call_class=call_class,
            budget_seconds=timeout_seconds,
            duration_seconds=max(0.0, time.monotonic() - call.started_monotonic),
            timed_out=isinstance(call.error, TimeoutError),
        )
        with self._block_submitter_ledger_calls_lock:
            if self._block_submitter_ledger_calls.get(key) is call:
                self._block_submitter_ledger_calls.pop(key, None)
        if call.error is not None:
            raise call.error
        return call.result

    def _wait_for_block_candidate_retry(self, delay_seconds: float) -> bool:
        """Wait for intentional backoff without impersonating stuck work.

        Retry waits heartbeat in bounded slices. Direct outbox calls and lock
        admission use the same phase-aware pattern; work that is not covered
        by an explicit deadline remains watchdog-eligible.
        """
        delay_seconds = max(0.0, float(delay_seconds))
        if delay_seconds <= 0:
            return self.stop_event.is_set()
        self._ensure_block_submitter_retry_state()
        started = time.monotonic()
        with self._block_submitter_retry_state_lock:
            self._block_submitter_backoff_started_monotonic = started
            self._block_submitter_backoff_deadline_monotonic = started + delay_seconds
            self._block_submitter_backoff_delay_seconds = delay_seconds
        remaining = delay_seconds
        try:
            while remaining > 0:
                self._record_block_submitter_wait("retry-backoff")
                wait_slice = min(remaining, self._block_work_wait_slice())
                if self.stop_event.wait(wait_slice):
                    return True
                remaining = max(0.0, remaining - wait_slice)
            self._record_block_submitter_wait("retry-backoff:complete")
            return False
        finally:
            with self._block_submitter_retry_state_lock:
                self._block_submitter_backoff_started_monotonic = None
                self._block_submitter_backoff_deadline_monotonic = None
                self._block_submitter_backoff_delay_seconds = 0.0

    def _mark_block_candidate_attempted(self, block_hash: str) -> None:
        mark_attempted = getattr(self.ledger, "mark_block_candidate_attempted", None)
        if callable(mark_attempted):
            self._run_block_submitter_ledger_call(
                ("mark-attempted", block_hash),
                "mark-attempted",
                lambda: mark_attempted(block_hash=block_hash),
            )

    def _rpc_call_with_timeout(
        self,
        method: str,
        params: list[object],
        *,
        timeout_seconds: float,
    ) -> Any:
        """Pass an explicit timeout to production RPCs and capable test doubles."""
        call = self.rpc.call
        supports_timeout = isinstance(self.rpc, JsonRpc)
        if not supports_timeout:
            try:
                parameters = inspect.signature(call).parameters.values()
                supports_timeout = any(
                    parameter.name == "timeout"
                    or parameter.kind is inspect.Parameter.VAR_KEYWORD
                    for parameter in parameters
                )
            except (TypeError, ValueError):
                supports_timeout = False
        if supports_timeout:
            return call(method, params, timeout=timeout_seconds)
        return call(method, params)

    def _ensure_block_submitter_rpc_call_state(self) -> None:
        if not hasattr(self, "_block_submitter_rpc_calls_lock"):
            self._block_submitter_rpc_calls_lock = threading.Lock()
        if not hasattr(self, "_block_submitter_rpc_calls"):
            self._block_submitter_rpc_calls = {}
        if not hasattr(self, "_block_submitter_rpc_worker_slots"):
            self._block_submitter_rpc_worker_slots = threading.BoundedSemaphore(
                MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS
            )

    def _run_submitblock_rpc_with_hard_deadline(
        self,
        *,
        block_hash: str,
        block_hex: str,
        timeout_seconds: float,
    ) -> Any:
        """Bound wall time even when an RPC adapter ignores its timeout."""
        self._ensure_block_submitter_rpc_call_state()
        with self._block_submitter_rpc_calls_lock:
            call = self._block_submitter_rpc_calls.get(block_hash)
            if call is None:
                if not self._block_submitter_rpc_worker_slots.acquire(
                    blocking=False
                ):
                    oldest_started = min(
                        (
                            pending.started_monotonic
                            for pending in self._block_submitter_rpc_calls.values()
                            if not pending.done.is_set()
                        ),
                        default=time.monotonic(),
                    )
                    self._maybe_restart_for_stuck_block_call(
                        kind="rpc-worker-pool",
                        started_monotonic=oldest_started,
                    )
                    raise TimeoutError(
                        "submitblock could not acquire a bounded RPC worker"
                    )
                call = _BlockSubmitterRpcCall()
                self._block_submitter_rpc_calls[block_hash] = call

                def run() -> None:
                    try:
                        call.result = self._rpc_call_with_timeout(
                            "submitblock",
                            [block_hex],
                            timeout_seconds=timeout_seconds,
                        )
                    except BaseException as exc:
                        call.error = exc
                    finally:
                        call.done.set()
                        self._block_submitter_rpc_worker_slots.release()

                threading.Thread(
                    target=run,
                    name=f"prism-block-rpc-{block_hash[:12]}",
                    daemon=True,
                ).start()

        deadline = time.monotonic() + timeout_seconds
        while not call.done.is_set():
            self._record_block_submitter_wait("submitblock-rpc")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._maybe_restart_for_exhausted_block_call_pool(
                    kind="rpc-worker-pool",
                    calls_lock=self._block_submitter_rpc_calls_lock,
                    calls=self._block_submitter_rpc_calls,
                )
                raise TimeoutError(
                    f"submitblock exceeded {timeout_seconds:g}s"
                )
            call.done.wait(
                min(remaining, self._block_work_wait_slice())
            )
        with self._block_submitter_rpc_calls_lock:
            if self._block_submitter_rpc_calls.get(block_hash) is call:
                self._block_submitter_rpc_calls.pop(block_hash, None)
        if call.error is not None:
            raise call.error
        return call.result

    def _arm_block_candidate_after_node_offer(
        self,
        candidate: PrismBlockCandidate,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> None:
        """Fence child payout work as soon as node acceptance is possible."""
        self._stash_retained_block_candidate_node_submission(
            str(candidate.submission.block_hash_hex),
            node_submission,
        )
        ambiguous_or_landed = (
            node_submission.error is not None
            or node_submission.result in (None, "duplicate")
        )
        if not ambiguous_or_landed:
            self._release_block_fast_lane_slot(
                str(candidate.submission.block_hash_hex)
            )
            return
        block_hash = str(candidate.submission.block_hash_hex).lower()
        expected_height = int(candidate.context.template["height"])
        self._begin_accepted_block_payout_preview(
            block_hash,
            block_height=expected_height,
        )

    def _submit_block_candidate_to_node(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Offer the durable candidate to qbitd before any accounting work."""
        block_hash = str(candidate.submission.block_hash_hex).lower()
        self._begin_accepted_block_payout_preview(
            block_hash,
            block_height=int(candidate.context.template["height"]),
        )
        self._register_outstanding_block_candidate(block_hash)
        self._record_block_submitter_phase("submitblock-rpc")
        timeout_seconds = max(
            0.001,
            float(
                getattr(
                    self,
                    "block_submit_rpc_timeout_seconds",
                    DEFAULT_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS,
                )
            ),
        )
        try:
            result = self._run_submitblock_rpc_with_hard_deadline(
                block_hash=block_hash,
                block_hex=str(candidate.submission.block_hex),
                timeout_seconds=timeout_seconds,
            )
        except BaseException as exc:
            self._record_block_submitter_phase("submitblock-rpc:error")
            node_submission = _BlockCandidateNodeSubmission(
                attempted=True,
                error=exc,
            )
            self._arm_block_candidate_after_node_offer(candidate, node_submission)
            return node_submission
        self._record_block_submitter_phase("submitblock-rpc:complete")
        landed_monotonic = getattr(candidate, "landed_monotonic", None)
        if landed_monotonic is not None:
            self._observe_block_submit_seconds(
                time.monotonic() - float(landed_monotonic)
            )
        node_submission = _BlockCandidateNodeSubmission(
            attempted=True,
            result=result,
        )
        self._arm_block_candidate_after_node_offer(candidate, node_submission)
        return node_submission

    def _node_submission_for_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Choose the node fast lane unless the pool was already closed."""
        block_hash = str(candidate.submission.block_hash_hex).lower()
        self._record_block_submitter_phase("fast-lane-admission")
        with self.lock:
            accounted_hashes = getattr(
                self,
                "_accounted_accepted_block_hashes",
                set(),
            )
            accepted_count = int(self.accepted_block_count)
            pool_closed = block_hash not in accounted_hashes and (
                accepted_count >= int(self.max_blocks)
                or (bool(self.stop_after_block) and accepted_count >= 1)
            )
        if pool_closed:
            return _BlockCandidateNodeSubmission(attempted=False)
        return self._submit_block_candidate_to_node(candidate)

    def _node_submission_for_candidate_or_retained(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Reuse a retained definitive acceptance instead of re-offering.

        An in-process retry of a candidate whose earlier offer already
        returned success must not ask the node again: the re-offer answers
        "duplicate", which downgrades the classification to the moved live
        tip and leans on chain probes that may be unavailable under the
        same saturation that caused the retry. The stashed result reruns
        the landing tail as if the first pass had continued.
        """
        retained = self._retained_block_candidate_node_submission(
            str(candidate.submission.block_hash_hex)
        )
        if retained is not None:
            return retained
        return self._node_submission_for_candidate(candidate)

    def _node_submission_for_direct_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Preserve active-replay semantics for non-queue embedders.

        The dedicated submitter always uses the unconditional fast lane. A
        direct caller can instead be resuming a durable active ancestor, for
        which another submit is unnecessary and some integrations do not
        retain block bytes. This compatibility probe is not on the incident
        queue-to-node path.
        """
        block_hash = str(candidate.submission.block_hash_hex).lower()
        expected_height = int(candidate.context.template["height"])
        try:
            if str(self.rpc.call("getbestblockhash")).lower() == block_hash:
                return _BlockCandidateNodeSubmission(attempted=False)
        except Exception:
            pass
        try:
            if self.active_block_candidate_height(block_hash) == expected_height:
                return _BlockCandidateNodeSubmission(attempted=False)
        except Exception:
            pass
        if not hasattr(candidate.submission, "block_hex"):
            return _BlockCandidateNodeSubmission(attempted=False)
        return self._node_submission_for_candidate(candidate)

    def _account_block_candidate_after_node_submit(
        self,
        candidate: PrismBlockCandidate,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> bool:
        """Pass fast-lane evidence while tolerating legacy test embedders."""
        submit = self.submit_block_candidate
        supports_node_submission = True
        try:
            parameters = inspect.signature(submit).parameters.values()
            supports_node_submission = any(
                parameter.name == "node_submission"
                or parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters
            )
        except (TypeError, ValueError):
            pass
        if supports_node_submission:
            return bool(submit(candidate, node_submission=node_submission))
        return bool(submit(candidate))

    def _submit_synchronous_block_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> bool:
        """Run the rare miner-facing path under one same-hash disposition."""
        block_hash = str(candidate.submission.block_hash_hex).lower()
        with self._block_candidate_disposition(block_hash):
            terminal_outcome = self._block_candidate_terminal_outcome(block_hash)
            if terminal_outcome is not None:
                self._finish_pending_share_commit(candidate.pending_share)
                return terminal_outcome
            outcome = getattr(self, "_block_candidate_outcome", None)
            if outcome is None:
                outcome = threading.local()
                self._block_candidate_outcome = outcome
            # The accepted/rejected accounting tail may already be complete
            # while only its durable outbox terminal update is retrying. A
            # synchronous same-hash waiter must join that finalize-only state;
            # another node offer/accounting pass could invert the outcome.
            with self.lock:
                registry = getattr(self, "_block_candidate_finalize_retries", None)
                pending_finalize = (
                    registry.get(block_hash) if registry is not None else None
                )
            if pending_finalize is not None:
                accepted, error = pending_finalize
                self._finalize_block_candidate(
                    candidate,
                    block_hash=block_hash,
                    accepted=accepted,
                    error=error,
                    outcome=outcome,
                )
                return accepted
            try:
                if not self._reserve_block_fast_lane_slot(block_hash):
                    raise RuntimeError(
                        "block candidate is waiting for fast-lane capacity"
                    )
                node_submission = self._node_submission_for_candidate_or_retained(candidate)
                self._mark_block_candidate_attempted(block_hash)
                with self._block_landing_ledger_statement_timeout_scope(block_hash):
                    production_submit = (
                        getattr(self.submit_block_candidate, "__func__", None)
                        is PrismCoordinator.submit_block_candidate
                    )
                    if production_submit:
                        block_landed = self._submit_block_candidate_serialized(
                            candidate,
                            node_submission=node_submission,
                        )
                    else:
                        block_landed = self._account_block_candidate_after_node_submit(
                            candidate,
                            node_submission,
                        )
            except BaseException:
                self._retain_block_candidate_for_retry(candidate)
                raise

            if not block_landed:
                reason = getattr(outcome, "reason", None)
                if reason in {None, *PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS}:
                    self._retain_block_candidate_for_retry(candidate)
                    raise RuntimeError(
                        "block candidate outcome is pending durable retry"
                    )
                abandon_error = getattr(outcome, "error", None)
                try:
                    self._record_block_submitter_phase(
                        "reject-prepared-block"
                    )
                    with self._block_landing_ledger_statement_timeout_scope(
                        block_hash
                    ):
                        self._reject_terminal_prepared_block_candidate(candidate)
                    self._record_block_submitter_phase(
                        "reject-prepared-block:complete"
                    )
                except Exception as exc:
                    # A prior attempt may have persisted prepared payout rows
                    # before this synchronous resubmit reached a false terminal
                    # verdict. Keep the outbox pending until those rows can be
                    # rejected; otherwise restart replay is removed while its
                    # balance transition remains live.
                    self._defer_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "could not reject prepared state for terminal candidate",
                        worker=candidate.client.username or None,
                    )
                    self._retain_block_candidate_for_retry(candidate)
                    raise RuntimeError(
                        "could not reject prepared state for terminal candidate"
                    ) from exc
                self._finalize_block_candidate(
                    candidate,
                    block_hash=block_hash,
                    accepted=False,
                    error=str(abandon_error or reason),
                    outcome=outcome,
                )
                return False

            self._finalize_block_candidate(
                candidate,
                block_hash=block_hash,
                accepted=True,
                error="",
                outcome=outcome,
            )
            return True

    def _ensure_block_candidate_disposition_state(self) -> None:
        """Backfill same-hash submission guards for lightweight embedders."""
        if (
            hasattr(self, "_block_candidate_disposition_registry_lock")
            and hasattr(self, "_block_candidate_disposition_flights")
            and hasattr(self, "_block_candidate_terminal_outcomes")
            and hasattr(self, "_block_fast_lane_reservations")
            and hasattr(self, "_block_disposition_waiting_retries")
        ):
            return
        with _HOT_PATH_LOCK_INITIALIZATION_LOCK:
            if not hasattr(self, "_block_candidate_disposition_registry_lock"):
                self._block_candidate_disposition_registry_lock = threading.Lock()
            if not hasattr(self, "_block_candidate_disposition_flights"):
                self._block_candidate_disposition_flights: dict[
                    str, _BlockCandidateDispositionFlight
                ] = {}
            if not hasattr(self, "_block_candidate_terminal_outcomes"):
                self._block_candidate_terminal_outcomes: dict[str, bool] = {}
            if not hasattr(self, "_block_fast_lane_reservations"):
                self._block_fast_lane_reservations: set[str] = set()
            if not hasattr(self, "_block_disposition_waiting_retries"):
                self._block_disposition_waiting_retries: dict[
                    str, PrismBlockCandidate
                ] = {}

    def _claim_block_candidate_disposition(
        self,
        block_hash: str,
        *,
        blocking: bool,
    ) -> _BlockCandidateDispositionLease | None:
        """Claim one hash without making unrelated node offers wait."""
        key = block_hash.lower()
        self._ensure_block_candidate_disposition_state()
        with self._block_submitter_lock(
            self._block_candidate_disposition_registry_lock,
            "candidate-disposition-registry",
        ):
            flight = self._block_candidate_disposition_flights.get(key)
            if flight is None:
                flight = _BlockCandidateDispositionFlight()
                self._block_candidate_disposition_flights[key] = flight
            flight.users += 1
        if blocking:
            self._acquire_block_submitter_lock(
                flight.lock,
                f"candidate-disposition:{key}",
            )
            acquired = True
        else:
            acquired = flight.lock.acquire(blocking=False)
        if acquired:
            return _BlockCandidateDispositionLease(key, flight)
        self._drop_block_candidate_disposition_user(key, flight)
        return None

    def _drop_block_candidate_disposition_user(
        self,
        key: str,
        flight: _BlockCandidateDispositionFlight,
    ) -> None:
        with self._block_submitter_lock(
            self._block_candidate_disposition_registry_lock,
            "candidate-disposition-registry",
        ):
            flight.users -= 1
            if (
                flight.users == 0
                and self._block_candidate_disposition_flights.get(key) is flight
            ):
                self._block_candidate_disposition_flights.pop(key, None)

    def _release_block_candidate_disposition(
        self,
        lease: _BlockCandidateDispositionLease,
    ) -> None:
        lease.flight.lock.release()
        self._drop_block_candidate_disposition_user(
            lease.block_hash,
            lease.flight,
        )

    def _block_candidate_terminal_outcome(self, block_hash: str) -> bool | None:
        self._ensure_block_candidate_disposition_state()
        with self.lock:
            return self._block_candidate_terminal_outcomes.get(block_hash.lower())

    def _record_block_candidate_terminal_outcome(
        self,
        block_hash: str,
        *,
        accepted: bool,
    ) -> None:
        self._ensure_block_candidate_disposition_state()
        with self.lock:
            key = block_hash.lower()
            self._block_candidate_terminal_outcomes[key] = accepted
            self._block_fast_lane_reservations.discard(key)
            replay_hashes = getattr(self, "_block_replay_inflight_hashes", None)
            if replay_hashes is not None:
                replay_hashes.discard(key)
            waiting = getattr(self, "_block_disposition_waiting_retries", None)
            if waiting is not None:
                waiting.pop(key, None)

    def _record_committed_block_candidate_abandonment(
        self,
        block_hash: str,
        outcome: threading.local,
    ) -> None:
        """Count an abandonment only after its terminal cleanup is fixed.

        ``_abandon_block_candidate`` seals a proposed rejection before any
        prepared payout rows are removed. That cleanup can fail, in which
        case the candidate is deliberately re-registered and can still prove
        accepted on a later pass. Counting at the seal would then expose the
        same hash as both abandoned and accepted. The writer calls this only
        after cleanup succeeds or after a false finalize-only disposition is
        installed; direct accounting callers invoke it only after the full
        serialized rejection path returns.
        """
        reason = getattr(outcome, "reason", None)
        if not isinstance(reason, str) or not reason:
            return
        if reason in PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS:
            return
        stale_job_class = getattr(outcome, "stale_job_class", None)
        key = block_hash.lower()
        with self.lock:
            counted_abandonments = getattr(
                self,
                "_counted_block_candidate_abandonments",
                None,
            )
            if counted_abandonments is None:
                counted_abandonments = set()
                self._counted_block_candidate_abandonments = counted_abandonments
            if key in counted_abandonments:
                return
            counted_abandonments.add(key)
            counts = getattr(self, "block_candidate_abandoned_counts", None)
            if counts is None:
                counts = {}
                self.block_candidate_abandoned_counts = counts
            counts[reason] = int(counts.get(reason, 0)) + 1
            if stale_job_class is not None:
                stale_counts = getattr(self, "stale_job_abandon_counts", None)
                if stale_counts is None:
                    stale_counts = {
                        abandon_class: 0
                        for abandon_class in PRISM_STALE_JOB_ABANDON_CLASSES
                    }
                    self.stale_job_abandon_counts = stale_counts
                stale_counts[stale_job_class] = (
                    int(stale_counts.get(stale_job_class, 0)) + 1
                )

    def _reserve_block_fast_lane_slot(self, block_hash: str) -> bool:
        """Reserve pool capacity while a node offer awaits terminal accounting."""
        key = block_hash.lower()
        with self.lock:
            reservations = getattr(self, "_block_fast_lane_reservations", None)
            if reservations is None:
                reservations = set()
                self._block_fast_lane_reservations = reservations
            if key in reservations:
                return True
            accepted_count = int(getattr(self, "accepted_block_count", 0))
            capacity = int(getattr(self, "max_blocks", 2**31 - 1))
            stop_after_one = bool(getattr(self, "stop_after_block", False))
            reserved_count = len(reservations)
            if accepted_count + reserved_count >= capacity:
                return False
            if stop_after_one and accepted_count + reserved_count >= 1:
                return False
            reservations.add(key)
            return True

    def _release_block_fast_lane_slot(self, block_hash: str) -> None:
        with self.lock:
            reservations = getattr(self, "_block_fast_lane_reservations", None)
            if reservations is not None:
                reservations.discard(block_hash.lower())

    @contextmanager
    def _block_candidate_disposition(
        self,
        block_hash: str,
    ) -> Iterator[_BlockCandidateDispositionLease]:
        """Serialize the full accepted/abandoned decision for one hash.

        A below-share-target solve submits synchronously while the durable
        outbox can concurrently replay that same candidate. Keep both attempts
        ordered until the accepted success tail records its process-local
        completion; otherwise the replay can terminally abandon the outbox
        during the gap after durable confirmation but before audit/share
        evidence is complete.
        """
        lease = self._claim_block_candidate_disposition(
            block_hash,
            blocking=True,
        )
        assert lease is not None
        try:
            yield lease
        finally:
            self._release_block_candidate_disposition(lease)

    def _ensure_block_accounting_state(self) -> None:
        if not hasattr(self, "_block_accounting_state_lock"):
            self._block_accounting_state_lock = threading.Lock()
        if not hasattr(self, "_block_accounting_queue"):
            depth = max(
                1,
                int(
                    getattr(
                        self,
                        "block_accounting_queue_depth",
                        DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH,
                    )
                ),
            )
            self._block_accounting_queue = queue.PriorityQueue(maxsize=depth)
        if not hasattr(self, "_block_accounting_overflow_queue"):
            self._block_accounting_overflow_queue = queue.PriorityQueue()
        if not hasattr(self, "_block_accounting_sequence"):
            self._block_accounting_sequence = 0
        if not hasattr(self, "_block_accounting_thread"):
            self._block_accounting_thread = None

    def _start_block_accounting_thread(self) -> threading.Thread:
        self._ensure_block_accounting_state()
        with self._block_accounting_state_lock:
            thread = self._block_accounting_thread
            if thread is not None and thread.is_alive():
                return thread
            self._record_block_work_heartbeat("block_accounting", "starting")
            thread = threading.Thread(
                target=self.block_accounting_loop,
                name="prism-block-accounting",
                daemon=True,
            )
            self._block_accounting_thread = thread
            thread.start()
            return thread

    def _enqueue_block_accounting_task(
        self,
        task: _BlockCandidateAccountingTask,
    ) -> bool:
        self._ensure_block_accounting_state()
        with self._block_accounting_state_lock:
            sequence = self._block_accounting_sequence
            self._block_accounting_sequence += 1
        priority = int(task.candidate.context.template["height"])
        item = (priority, sequence, task)
        if not self._block_accounting_overflow_queue.empty():
            # Once spillover begins, keep later handoffs behind it instead of
            # repeatedly refilling the primary queue and starving older spill
            # entries.
            self._block_accounting_overflow_queue.put_nowait(item)
            return True
        try:
            self._block_accounting_queue.put_nowait(item)
            return True
        except queue.Full:
            # A node offer has already happened and must never be converted
            # back into a raw-submit retry. Preserve its result and lease in
            # an unbounded, process-local overflow queue; max-block admission
            # bounds the number of unresolved real offers.
            self._block_accounting_overflow_queue.put_nowait(item)
            print(
                "prism coordinator: block accounting handoff spilled "
                f"hash={task.candidate.submission.block_hash_hex}",
                flush=True,
            )
            return True

    def _run_one_invalid_block_candidate_quarantine(self) -> bool:
        self._ensure_block_replay_state()
        try:
            block_hash, error = self._block_quarantine_queue.get_nowait()
        except queue.Empty:
            return False
        completed = False
        try:
            self._record_block_submitter_phase("replay-quarantine")
            quarantine = getattr(
                self.ledger,
                "mark_block_candidate_abandoned",
                None,
            )
            if callable(quarantine):
                quarantined = self._run_block_submitter_ledger_call(
                    ("replay-quarantine", block_hash),
                    "replay-quarantine",
                    lambda: quarantine(block_hash=block_hash, error=error),
                )
                self._clear_accepted_block_payout_preview(block_hash)
                if quarantined:
                    self._clear_block_candidate_retry_state(block_hash)
                    self._discard_outstanding_block_candidate(block_hash)
                    with self.lock:
                        self.block_candidate_poisoned_count = int(
                            getattr(self, "block_candidate_poisoned_count", 0)
                        ) + 1
            completed = True
            return True
        except Exception:
            print(
                "prism coordinator: invalid candidate quarantine failed "
                f"hash={block_hash}",
                flush=True,
            )
            traceback.print_exc()
            return True
        finally:
            self._block_quarantine_queue.task_done()
            if completed:
                with self.lock:
                    self._block_quarantine_hashes.discard(block_hash)
            elif not self.stop_event.is_set():
                self._block_quarantine_queue.put_nowait((block_hash, error))

    def _call_block_candidate_writer(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission,
        disposition_held: bool,
    ) -> bool:
        """Invoke the writer while preserving duck-typed test integrations."""
        writer = self._submit_next_block_candidate_writer
        supports_disposition_held = True
        try:
            parameters = inspect.signature(writer).parameters.values()
            supports_disposition_held = any(
                parameter.name == "disposition_held"
                or parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters
            )
        except (TypeError, ValueError):
            pass
        if supports_disposition_held:
            return bool(
                writer(
                    candidate,
                    node_submission=node_submission,
                    disposition_held=disposition_held,
                )
            )
        return bool(writer(candidate, node_submission=node_submission))

    def _restore_replayed_candidate_acceptance_evidence(
        self,
        candidate: PrismBlockCandidate,
    ) -> None:
        if not candidate.durable_replay:
            return
        block_hash = str(candidate.submission.block_hash_hex).lower()
        block_state = None
        state_read_failed = False
        state_reader = getattr(self.ledger, "pool_block_state", None)
        if callable(state_reader):
            try:
                block_state = self._run_block_submitter_ledger_call(
                    ("replay-pool-block-state", block_hash),
                    "replay-pool-block-state",
                    lambda: state_reader(block_hash=block_hash),
                )
            except Exception:
                traceback.print_exc()
                state_read_failed = True
        durable_chain_state = (
            str(block_state.get("chain_state", ""))
            if block_state is not None
            else ""
        )
        if state_read_failed or durable_chain_state in {"prepared", "confirmed"}:
            self._register_outstanding_block_candidate(block_hash)
            with self.lock:
                self._tip_observed_accepted_block_hashes[block_hash] = (
                    time.monotonic()
                )
            print(
                "prism coordinator: restored acceptance evidence for "
                f"replayed block candidate hash={block_hash} "
                + (
                    "after a failed durable-state read"
                    if state_read_failed
                    else f"chain_state={durable_chain_state}"
                ),
                flush=True,
            )

    def _run_block_accounting_task(
        self,
        task: _BlockCandidateAccountingTask,
    ) -> None:
        candidate = task.candidate
        with self.lock:
            self._block_accounting_holds_disposition = True
            self._block_accounting_deferred_retry_candidate = None
        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.refresh_client = None
        try:
            self._restore_replayed_candidate_acceptance_evidence(candidate)
            with self._writer_operation("accepted_block_handling"):
                self._call_block_candidate_writer(
                    candidate,
                    node_submission=task.node_submission,
                    disposition_held=True,
                )
                refresh_client = getattr(outcome, "refresh_client", None)
                outcome.refresh_client = None
        except ShutdownInProgress:
            return
        finally:
            self._release_block_candidate_disposition(task.disposition_lease)
            with self.lock:
                self._block_accounting_holds_disposition = False
                deferred_retry = getattr(
                    self,
                    "_block_accounting_deferred_retry_candidate",
                    None,
                )
                self._block_accounting_deferred_retry_candidate = None
                if deferred_retry is not None:
                    self._merge_block_candidate_retry_locked(
                        "_retry_block_candidate",
                        deferred_retry,
                    )
        if refresh_client is not None and not self.stop_event.is_set():
            self._record_block_submitter_phase("refresh-jobs")
            self.refresh_jobs_after_pending_accepted_block(
                refresh_client,
                heartbeat_name="block_accounting",
            )
            self._record_block_submitter_phase("refresh-jobs:complete")

    def block_accounting_loop(self) -> None:
        self._block_accounting_thread_ident = threading.get_ident()
        self._ensure_block_accounting_state()
        while not self.stop_event.is_set():
            self._record_block_submitter_phase("accounting-queue")
            source_queue = None
            try:
                _priority, _sequence, task = self._block_accounting_queue.get_nowait()
                source_queue = self._block_accounting_queue
            except queue.Empty:
                try:
                    _priority, _sequence, task = (
                        self._block_accounting_overflow_queue.get_nowait()
                    )
                    source_queue = self._block_accounting_overflow_queue
                except queue.Empty:
                    if self._run_one_invalid_block_candidate_quarantine():
                        continue
                    self.stop_event.wait(self._block_work_wait_slice())
                    continue
            try:
                self._run_block_accounting_task(task)
            except Exception:
                print(
                    "prism coordinator: block accounting iteration failed; "
                    "durable candidate remains pending",
                    flush=True,
                )
                traceback.print_exc()
                self._retain_block_candidate_for_retry(task.candidate)
            finally:
                assert source_queue is not None
                source_queue.task_done()

    def block_submit_loop(self) -> None:
        # Boundary stamps in _record_block_candidate_progress are gated to
        # this thread so client-thread dispositions cannot refresh the
        # submitter's liveness budget on its behalf.
        self._block_submitter_thread_ident = threading.get_ident()
        self._start_block_accounting_thread()
        while not self.stop_event.is_set():
            self._record_block_submitter_phase("loop")
            try:
                # The in-memory wakeup is already backed by the durable
                # outbox. Drain it before any recovery query so a saturated
                # database cannot delay the first node submission.
                with self.lock:
                    retry_ready = (
                        getattr(self, "_retry_block_candidate", None) is not None
                    )
                queue_obj = getattr(self, "block_candidate_queue", None)
                replay_queue = getattr(
                    self,
                    "_block_replay_candidate_queue",
                    None,
                )
                wakeup_ready = bool(
                    (queue_obj is not None and not queue_obj.empty())
                    or (replay_queue is not None and not replay_queue.empty())
                )
                if (retry_ready or wakeup_ready) and self.submit_next_block_candidate(
                    defer_accounting=True
                ):
                    continue
                self.replay_pending_block_candidates()
                self.submit_next_block_candidate(
                    timeout=1.0,
                    defer_accounting=True,
                )
            except ShutdownInProgress:
                # Admission can close after the loop condition. Durable block
                # candidates remain in the outbox for the replacement writer.
                return
            except Exception:
                phase = getattr(self, "_block_submitter_phase", "unknown")
                print(
                    "prism coordinator: block submitter iteration failed "
                    f"phase={phase}; durable candidates remain pending",
                    flush=True,
                )
                traceback.print_exc()
                retry_delay = float(
                    getattr(
                        self,
                        "block_candidate_retry_initial_seconds",
                        DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS,
                    )
                )
                if self._wait_for_block_candidate_retry(retry_delay):
                    return

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
        self._record_block_submitter_phase("dequeue-retry")
        with self.lock:
            candidate = getattr(self, "_retry_block_candidate", None)
            if candidate is not None and not self._block_candidate_retry_ready_locked(
                candidate
            ):
                # Parked by _pace_block_candidate_retry: honor the backoff
                # deadline without sleeping on the submitter or accounting
                # lane.
                candidate = None
            if candidate is not None:
                self._retry_block_candidate = None
        if candidate is None:
            queue_obj = getattr(self, "block_candidate_queue", None)
            self._ensure_block_replay_state()
            replay_queue = self._block_replay_candidate_queue
            if queue_obj is None and replay_queue is None:
                return False
            deadline = (
                None
                if timeout is None
                else time.monotonic() + max(0.0, timeout)
            )
            while candidate is None:
                self._record_block_submitter_phase("dequeue-queue")
                # Live discoveries always outrank durable restart replay.
                for candidate_queue in (queue_obj, replay_queue):
                    if candidate_queue is None:
                        continue
                    try:
                        candidate = candidate_queue.get_nowait()
                        break
                    except queue.Empty:
                        pass
                if candidate is None:
                    self._ensure_block_candidate_disposition_state()
                    with self.lock:
                        waiting = self._block_disposition_waiting_retries
                        ready_hashes = [
                            key
                            for key in waiting
                            if self._block_candidate_retry_ready_locked(
                                waiting[key]
                            )
                        ]
                        if ready_hashes:
                            waiting_hash = min(
                                ready_hashes,
                                key=lambda key: int(
                                    waiting[key].context.template["height"]
                                ),
                            )
                            candidate = waiting.pop(waiting_hash)
                if candidate is not None:
                    break
                if deadline is None:
                    return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                if self.stop_event.wait(
                    min(remaining, self._block_work_wait_slice())
                ):
                    return False

        block_hash = str(candidate.submission.block_hash_hex).lower()
        lease = self._claim_block_candidate_disposition(
            block_hash,
            blocking=not defer_accounting,
        )
        if lease is None:
            # Another same-hash pass already spans node offer through durable
            # finalization. Keep this wakeup outside the global parent retry
            # slot until that lease transfers/releases: consuming it can lose
            # an accounting retry, while repeatedly prioritizing it can starve
            # unrelated live blocks.
            with self.lock:
                self._block_disposition_waiting_retries[block_hash] = candidate
            self._wait_for_block_candidate_retry(
                float(
                    getattr(
                        self,
                        "block_candidate_retry_initial_seconds",
                        DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS,
                    )
                )
            )
            return True
        transferred = False
        if self._block_candidate_terminal_outcome(block_hash) is not None:
            self._release_block_candidate_disposition(lease)
            return True

        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.refresh_client = None
        self._record_block_submitter_phase("finalize-registry")
        with self.lock:
            registry = getattr(self, "_block_candidate_finalize_retries", None)
            pending_finalize = (
                registry.get(block_hash) if registry is not None else None
            )
        if pending_finalize is None:
            permanently_closed = False
            already_accounted = False
            if defer_accounting:
                with self.lock:
                    accounted_hashes = getattr(
                        self,
                        "_accounted_accepted_block_hashes",
                        set(),
                    )
                    already_accounted = block_hash in accounted_hashes
                    accepted_count = int(getattr(self, "accepted_block_count", 0))
                    permanently_closed = not already_accounted and (
                        accepted_count >= int(getattr(self, "max_blocks", 2**31 - 1))
                        or (
                            bool(getattr(self, "stop_after_block", False))
                            and accepted_count >= 1
                        )
                    )
                if permanently_closed or already_accounted:
                    # Accounting must terminalize a durable outbox row even
                    # after pool capacity closes. An already-accounted hash
                    # likewise needs only its exact-idempotent/finalize tail.
                    node_submission = _BlockCandidateNodeSubmission(
                        attempted=False
                    )
                elif not self._reserve_block_fast_lane_slot(block_hash):
                    # Capacity is provisionally occupied by another unresolved
                    # node offer. Preserve strict max-block semantics until
                    # that offer either accounts or terminates.
                    self._release_block_candidate_disposition(lease)
                    self._retain_block_candidate_for_retry(candidate)
                    self._wait_for_block_candidate_retry(
                        float(
                            getattr(
                                self,
                                "block_candidate_retry_initial_seconds",
                                DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS,
                            )
                        )
                    )
                    return True
                else:
                    try:
                        node_submission = self._node_submission_for_candidate_or_retained(candidate)
                    except BaseException:
                        try:
                            self._retain_block_candidate_for_retry(candidate)
                        finally:
                            self._release_block_candidate_disposition(lease)
                        raise
            else:
                try:
                    node_submission = self._node_submission_for_candidate_or_retained(candidate)
                except BaseException:
                    try:
                        self._retain_block_candidate_for_retry(candidate)
                    finally:
                        self._release_block_candidate_disposition(lease)
                    raise
        else:
            node_submission = _BlockCandidateNodeSubmission(attempted=False)

        if defer_accounting:
            task = _BlockCandidateAccountingTask(
                candidate=candidate,
                node_submission=node_submission,
                disposition_lease=lease,
            )
            try:
                enqueued = self._enqueue_block_accounting_task(task)
            except BaseException:
                try:
                    self._retain_block_candidate_for_retry(candidate)
                finally:
                    self._release_block_candidate_disposition(lease)
                raise
            if enqueued:
                transferred = True
                return True
            self._release_block_candidate_disposition(lease)
            self._retain_block_candidate_for_retry(candidate)
            self._wait_for_block_candidate_retry(
                float(
                    getattr(
                        self,
                        "block_candidate_retry_initial_seconds",
                        DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS,
                    )
                )
            )
            return True

        try:
            with self._writer_operation("accepted_block_handling"):
                ran = self._call_block_candidate_writer(
                    candidate,
                    node_submission=node_submission,
                    disposition_held=True,
                )
                refresh_client = getattr(outcome, "refresh_client", None)
                outcome.refresh_client = None
        except ShutdownInProgress:
            # The durable outbox remains pending and the replacement process
            # will replay it. Dequeuing the in-memory wakeup during the
            # admission-close race cannot lose candidate work.
            return False
        finally:
            if not transferred:
                self._release_block_candidate_disposition(lease)
        # Fresh-job fanout is deliberately outside the writer admission. Once
        # the candidate outbox is finalized it cannot mutate the ledger, so a
        # blocked client send must not hold the writer lease during shutdown.
        if refresh_client is not None and not self.stop_event.is_set():
            self._record_block_submitter_phase("refresh-jobs")
            self.refresh_jobs_after_pending_accepted_block(
                refresh_client,
                heartbeat_name="block_submitter",
            )
            self._record_block_submitter_phase("refresh-jobs:complete")
        return ran

    def _submit_next_block_candidate_writer(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission | None = None,
        disposition_held: bool = False,
    ) -> bool:
        """Land one dequeued block candidate inside writer admission."""
        block_hash = str(candidate.submission.block_hash_hex).lower()
        if not disposition_held:
            # Preserve the historical direct-writer seam while keeping its
            # node offer and terminal outbox decision inside the same-hash
            # guard. Production queue/accounting calls transfer an existing
            # lease and skip this wrapper.
            with self._block_candidate_disposition(block_hash):
                terminal_outcome = self._block_candidate_terminal_outcome(
                    block_hash
                )
                if terminal_outcome is not None:
                    return terminal_outcome
                if node_submission is None:
                    node_submission = (
                        self._node_submission_for_candidate_or_retained(
                            candidate
                        )
                    )
                return self._submit_next_block_candidate_writer(
                    candidate,
                    node_submission=node_submission,
                    disposition_held=True,
                )
        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.reason = None
        outcome.error = None
        outcome.stale_job_class = None
        self._record_block_submitter_phase("finalize-registry")
        with self.lock:
            registry = getattr(self, "_block_candidate_finalize_retries", None)
            pending_finalize = (
                registry.get(block_hash) if registry is not None else None
            )
        if pending_finalize is not None:
            # Finalize-only replay: node submission, terminal accounting, and
            # payout persistence already completed on the pass that armed
            # this entry. It bypasses both submitblock and attempt marking.
            accepted, error = pending_finalize
            return self._finalize_block_candidate(
                candidate,
                block_hash=block_hash,
                accepted=accepted,
                error=error,
                outcome=outcome,
            )
        if node_submission is None:
            node_submission = self._node_submission_for_candidate_or_retained(candidate)
        try:
            self._mark_block_candidate_attempted(block_hash)
        except Exception:
            print(
                "prism coordinator: could not record block candidate attempt "
                f"hash={block_hash}",
                flush=True,
            )
            traceback.print_exc()
            self._retain_block_candidate_for_retry(candidate)
            self._pace_block_candidate_retry(block_hash)
            return True
        accepted = False
        error = "candidate became stale or submission failed"
        try:
            self._record_block_submitter_phase("accounting")
            with self._block_landing_ledger_statement_timeout_scope(block_hash):
                production_submit = (
                    getattr(self.submit_block_candidate, "__func__", None)
                    is PrismCoordinator.submit_block_candidate
                )
                if disposition_held and production_submit:
                    assert node_submission is not None
                    accepted = self._submit_block_candidate_serialized(
                        candidate,
                        node_submission=node_submission,
                    )
                elif node_submission is None:
                    accepted = bool(self.submit_block_candidate(candidate))
                else:
                    accepted = self._account_block_candidate_after_node_submit(
                        candidate,
                        node_submission,
                    )
        except Exception:
            error = "candidate submission raised an exception"
            print(
                "prism coordinator: block candidate submission failed "
                f"hash={candidate.submission.block_hash_hex}",
                flush=True,
            )
            traceback.print_exc()
        abandon_reason = getattr(outcome, "reason", None) if outcome is not None else None
        abandon_error = getattr(outcome, "error", None) if outcome is not None else None
        if not accepted and abandon_error:
            error = str(abandon_error)
        retryable = not accepted and (
            abandon_reason is None
            or abandon_reason in PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS
        )
        if retryable:
            # Leave the outbox row pending. It will replay after a short pause
            # or on process restart. Keep this parent ahead of queued children:
            # a child built from its prospective balances cannot be validated
            # against the database until the parent confirmation catches up.
            print(
                "prism coordinator: retained block candidate for retry "
                f"hash={block_hash} reason={abandon_reason or 'exception'}",
                flush=True,
            )
            self._retain_block_candidate_for_retry(candidate)
            self._pace_block_candidate_retry(block_hash)
            return True
        if not accepted:
            try:
                self._record_block_submitter_phase("reject-prepared-block")
                with self._block_landing_ledger_statement_timeout_scope(block_hash):
                    self._reject_terminal_prepared_block_candidate(candidate)
                self._record_block_submitter_phase(
                    "reject-prepared-block:complete"
                )
            except Exception:
                # Persistence may have committed before a later RPC/transport
                # failure. Do not terminally discard the outbox row until its
                # prepared balance deltas have also reached a terminal state.
                print(
                    "prism coordinator: prepared block cleanup failed "
                    f"hash={block_hash}",
                    flush=True,
                )
                traceback.print_exc()
                self._defer_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "could not reject prepared state for terminal candidate",
                    worker=candidate.client.username or None,
                )
                self._retain_block_candidate_for_retry(candidate)
                self._wait_for_block_candidate_retry(
                    self._next_block_candidate_retry_delay(block_hash)
                )
                return True
        return self._finalize_block_candidate(
            candidate,
            block_hash=block_hash,
            accepted=accepted,
            error=error,
            outcome=outcome,
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
        """Drive a terminal candidate's durable outbox update, with backoff.

        Failure retains the candidate as a finalize-only replay: the next
        paced attempt re-enters here directly, never submit_block_candidate,
        so terminal abandonment accounting stays once-per-candidate and an
        accepted candidate's audit/persist work is not redone per retry.
        """
        self._record_block_submitter_phase("finalize-preview")
        self._clear_accepted_block_payout_preview(
            block_hash,
            invalidate_published=not accepted,
        )
        finish_name = (
            "mark_block_candidate_submitted"
            if accepted
            else "mark_block_candidate_abandoned"
        )
        finish = getattr(self.ledger, finish_name, None)
        if callable(finish):
            try:
                if accepted:
                    self._run_block_submitter_ledger_call(
                        ("finalize", block_hash, "submitted"),
                        "finalize-outbox-submitted",
                        lambda: finish(block_hash=block_hash),
                    )
                else:
                    self._run_block_submitter_ledger_call(
                        ("finalize", block_hash, "abandoned"),
                        "finalize-outbox-abandoned",
                        lambda: finish(block_hash=block_hash, error=error),
                    )
                    self._record_committed_block_candidate_abandonment(
                        block_hash,
                        outcome,
                    )
                    # The invalidation tombstone is needed until the durable
                    # outbox becomes terminal. A normal return (including an
                    # already-terminal/missing row) means there is no pending
                    # replay source left for this process to guard.
                    self._clear_accepted_block_payout_preview(block_hash)
            except Exception:
                # Keep the coordinator alive. The terminal-state update
                # failed, so the durable row stays pending and its replay
                # must pace like any other retained retry.
                print(
                    "prism coordinator: could not finalize durable block candidate "
                    f"hash={block_hash}",
                    flush=True,
                )
                traceback.print_exc()
                with self.lock:
                    registry = getattr(
                        self, "_block_candidate_finalize_retries", None
                    )
                    if registry is None:
                        registry = {}
                        self._block_candidate_finalize_retries = registry
                    first_failure = block_hash not in registry
                    registry[block_hash] = (accepted, error)
                if not accepted:
                    # The durable update is ambiguous, but this process has
                    # now frozen a false finalize-only disposition. It cannot
                    # return to chain-state evaluation until restart, where
                    # these process-local counters start fresh.
                    self._record_committed_block_candidate_abandonment(
                        block_hash,
                        outcome,
                    )
                # The share row already reached its terminal outcome in this
                # process; only the outbox mark is pending. Release the
                # snapshot anchor floor now (idempotent) -- holding it across
                # paced retries would clamp job snapshot anchors and
                # under-count already-durable shares in reward windows.
                self._finish_pending_share_commit(candidate.pending_share)
                self._retain_block_candidate_for_retry(candidate)
                if accepted and first_failure:
                    # The block is active regardless of the outbox update;
                    # post-accept fleet refresh must wait for neither ledger
                    # recovery nor the first backoff. Return unpaced so the
                    # caller refreshes immediately; the paced ladder starts
                    # from the first finalize-only replay.
                    outcome.refresh_client = candidate.client
                    return True
                self._wait_for_block_candidate_retry(
                    self._next_block_candidate_retry_delay(block_hash)
                )
                return True
        elif not accepted:
            # Compatibility ledgers without a durable candidate outbox have
            # no restart replay source that could require the tombstone.
            self._clear_accepted_block_payout_preview(block_hash)
            self._record_committed_block_candidate_abandonment(
                block_hash,
                outcome,
            )
        with self.lock:
            registry = getattr(self, "_block_candidate_finalize_retries", None)
            if registry is not None:
                registry.pop(block_hash, None)
        self._clear_block_candidate_retry_state(block_hash)
        self._discard_outstanding_block_candidate(block_hash)
        self._record_block_candidate_terminal_outcome(
            block_hash,
            accepted=accepted,
        )
        # Terminal for this process either way: an accepted candidate credited
        # its share during the success tail (a no-op release here), and an
        # abandoned one can only be credited by restart replay, which stamps a
        # fresh PendingShare. Stop holding the snapshot anchor floor.
        self._finish_pending_share_commit(candidate.pending_share)
        if accepted:
            outcome.refresh_client = candidate.client
        return True

    def _merge_block_candidate_retry_locked(
        self,
        attribute: str,
        candidate: PrismBlockCandidate,
    ) -> None:
        """Merge one retry by parent-first order. Caller holds self.lock."""
        candidate_height = int(candidate.context.template["height"])
        candidate_hash = str(candidate.submission.block_hash_hex).lower()
        existing = getattr(self, attribute, None)
        if existing is None:
            setattr(self, attribute, candidate)
            return
        existing_height = int(existing.context.template["height"])
        existing_hash = str(existing.submission.block_hash_hex).lower()
        if candidate_hash == existing_hash:
            setattr(self, attribute, candidate)
            return
        if attribute != "_retry_block_candidate":
            if candidate_height < existing_height:
                setattr(self, attribute, candidate)
            return

        # The raw lane has one parent-first head slot, but every displaced
        # hash still needs an in-memory wakeup. Durable replay dedupe keeps a
        # replayed descendant marked in-flight, so relying on a later outbox
        # scan here could otherwise suppress it forever.
        self._ensure_block_candidate_disposition_state()
        waiting = self._block_disposition_waiting_retries
        if candidate_height < existing_height:
            waiting[existing_hash] = existing
            setattr(self, attribute, candidate)
        else:
            waiting[candidate_hash] = candidate

    def _retain_block_candidate_for_retry(self, candidate: PrismBlockCandidate) -> None:
        """Keep the oldest unresolved candidate ahead of queued descendants."""
        candidate_hash = str(candidate.submission.block_hash_hex).lower()
        # A retained candidate will be re-disposed, so the disposition seal
        # (which stopped tip-observation matching at a terminal commit) no
        # longer applies: the terminal work did not complete. Re-register
        # immediately -- not at the next disposition -- so acceptance
        # evidence arriving during the retry backoff is not lost.
        self._register_outstanding_block_candidate(candidate_hash)
        with self.lock:
            self.block_candidate_retry_count = int(
                getattr(self, "block_candidate_retry_count", 0)
            ) + 1
            accounting_owner = (
                threading.get_ident()
                == getattr(self, "_block_accounting_thread_ident", None)
                and bool(
                    getattr(
                        self,
                        "_block_accounting_holds_disposition",
                        False,
                    )
                )
            )
            retry_attribute = (
                "_block_accounting_deferred_retry_candidate"
                if accounting_owner
                else "_retry_block_candidate"
            )
            self._merge_block_candidate_retry_locked(
                retry_attribute,
                candidate,
            )

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
        initial = max(
            0.0,
            float(
                getattr(
                    self,
                    "block_candidate_retry_initial_seconds",
                    DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS,
                )
            ),
        )
        maximum = max(
            initial,
            float(
                getattr(
                    self,
                    "block_candidate_retry_max_seconds",
                    DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS,
                )
            ),
        )
        with self.lock:
            delays = getattr(self, "block_candidate_retry_delays", None)
            if delays is None:
                delays = {}
                self.block_candidate_retry_delays = delays
            delay = float(delays.get(block_hash, initial))
            delays[block_hash] = min(maximum, max(initial, delay * 2))
        return min(delay, maximum)

    def _pace_block_candidate_retry(self, block_hash: str) -> None:
        """Apply per-candidate retry backoff without convoying accounting.

        On the block_accounting thread the disposition lease and writer
        admission stay held until the accounting task's finally clause, so
        sleeping here would stall every queued accounting task and keep an
        armed payout barrier blocking balance mutation for the whole backoff
        window. Record a not-before deadline instead; the dequeue path honors
        it, and replay_pending_block_candidates already short-circuits while
        the retained candidate occupies the retry slot.
        """
        delay_seconds = self._next_block_candidate_retry_delay(block_hash)
        accounting_owner = (
            threading.get_ident()
            == getattr(self, "_block_accounting_thread_ident", None)
            and bool(
                getattr(
                    self,
                    "_block_accounting_holds_disposition",
                    False,
                )
            )
        )
        if not accounting_owner:
            self._wait_for_block_candidate_retry(delay_seconds)
            return
        with self.lock:
            not_before = getattr(
                self,
                "_block_candidate_retry_not_before",
                None,
            )
            if not_before is None:
                not_before = {}
                self._block_candidate_retry_not_before = not_before
            not_before[str(block_hash).lower()] = (
                time.monotonic() + delay_seconds
            )

    def _block_candidate_retry_ready_locked(
        self,
        candidate: PrismBlockCandidate,
    ) -> bool:
        """Return whether a parked retry's backoff deadline has passed.

        Caller holds self.lock. A ready entry is dropped so a candidate that
        later lands terminally leaves no stale pacing behind.
        """
        not_before = getattr(self, "_block_candidate_retry_not_before", None)
        if not not_before:
            return True
        block_hash = str(candidate.submission.block_hash_hex).lower()
        deadline = not_before.get(block_hash)
        if deadline is None:
            return True
        if time.monotonic() < deadline:
            return False
        not_before.pop(block_hash, None)
        return True

    def _stash_retained_block_candidate_node_submission(
        self,
        block_hash: str,
        node_submission: _BlockCandidateNodeSubmission | None,
    ) -> None:
        """Record a definitive node acceptance for in-process retries.

        Recorded at the offer itself (the universal post-offer hook) so
        every retention path — writer failures, defer-accounting handoff
        failures, the accounting loop — is covered without each site having
        to remember. A retryable failure after a successful offer would
        otherwise re-offer on retry and read "duplicate", which classifies
        against the moved live tip and can only rescue the block through
        chain probes that may be unavailable under the same saturation.
        The entry is read without consuming and lives until the candidate
        reaches a terminal outcome, so repeated retryable failures keep
        reusing the same known acceptance.
        """
        if node_submission is None or not node_submission.attempted:
            return
        if (
            node_submission.error is not None
            or node_submission.result is not None
        ):
            # Only a definitive success is safe to reuse: an ambiguous or
            # rejected offer must be re-offered so the node can resolve it.
            return
        with self.lock:
            retained = getattr(
                self,
                "_block_candidate_retained_node_submissions",
                None,
            )
            if retained is None:
                retained = {}
                self._block_candidate_retained_node_submissions = retained
            retained[str(block_hash).lower()] = node_submission
            stamped = getattr(
                self,
                "_block_candidate_retained_submission_monotonic",
                None,
            )
            if stamped is None:
                stamped = {}
                self._block_candidate_retained_submission_monotonic = stamped
            stamped[str(block_hash).lower()] = time.monotonic()

    def _retained_block_candidate_node_submission(
        self,
        block_hash: str,
    ) -> _BlockCandidateNodeSubmission | None:
        with self.lock:
            retained = getattr(
                self,
                "_block_candidate_retained_node_submissions",
                None,
            )
            if not retained:
                return None
            return retained.get(str(block_hash).lower())

    def _block_candidate_acceptance_retained(self, block_hash: str) -> bool:
        """Whether this process holds fresh first-party acceptance evidence.

        The retained stash records only definitive submitblock successes,
        so its presence proves qbitd accepted this candidate — evidence of
        the same strength as a recent own-hash tip observation, and
        available precisely when saturation makes the chain probes answer
        "unknown" (the observation registry can be empty after a definitive
        ack: blockwait only reports the newest of rapid connects). It ages
        on the same window as tip observations: acceptance at offer time
        does not prove the block stayed canonical, and an orphaned block
        never probes False — it is merely absent — so a candidate whose
        probes stay inconclusive past the window must regain
        abandonability instead of deferring forever behind a stale ack.
        """
        with self.lock:
            stamped = getattr(
                self,
                "_block_candidate_retained_submission_monotonic",
                None,
            )
            if not stamped:
                return False
            recorded = stamped.get(str(block_hash).lower())
        if recorded is None:
            return False
        window = float(
            getattr(
                self,
                "observed_tip_accept_window_seconds",
                DEFAULT_PRISM_OBSERVED_TIP_ACCEPT_WINDOW_SECONDS,
            )
        )
        if window <= 0:
            return True
        return (time.monotonic() - recorded) <= window

    def _clear_block_candidate_retry_state(self, block_hash: str) -> None:
        with self.lock:
            delays = getattr(self, "block_candidate_retry_delays", None)
            if delays is not None:
                delays.pop(block_hash, None)
            landing_timeouts = getattr(self, "_block_landing_timeout_counts", None)
            if landing_timeouts is not None:
                landing_timeouts.pop(block_hash, None)
            not_before = getattr(
                self,
                "_block_candidate_retry_not_before",
                None,
            )
            if not_before is not None:
                not_before.pop(block_hash, None)
            retained = getattr(
                self,
                "_block_candidate_retained_node_submissions",
                None,
            )
            if retained is not None:
                retained.pop(str(block_hash).lower(), None)
            stamped = getattr(
                self,
                "_block_candidate_retained_submission_monotonic",
                None,
            )
            if stamped is not None:
                stamped.pop(str(block_hash).lower(), None)

    def _defer_block_candidate(self, reason: str, message: str, *, worker: str | None) -> None:
        """Record a retryable outcome without counting a terminal abandonment."""
        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.reason = reason
        outcome.error = None
        outcome.stale_job_class = None
        print(
            f"prism coordinator: block candidate deferred reason={reason}: {message}",
            flush=True,
        )

    def _block_candidate_acceptance_recorded(self, block_hash: str) -> bool:
        """Return whether this process completed the candidate success tail."""
        self._ensure_job_cache_state()
        with self.lock:
            return block_hash.lower() in self._accounted_accepted_block_hashes

    def _register_outstanding_block_candidate(self, block_hash: str) -> None:
        """Track a candidate this process may still land, for tip matching."""
        self._ensure_job_cache_state()
        with self.lock:
            self._outstanding_block_candidate_hashes.add(block_hash.lower())

    def _discard_outstanding_block_candidate(self, block_hash: str) -> None:
        """Stop matching tip observations once a candidate is terminal."""
        self._ensure_job_cache_state()
        key = block_hash.lower()
        with self.lock:
            self._outstanding_block_candidate_hashes.discard(key)
            self._tip_observed_accepted_block_hashes.pop(key, None)

    def _note_tip_observation_for_candidates(self, tip_hash: str) -> None:
        """Register a tip observation that matches an outstanding candidate.

        qbitd only ever reports the pool's own candidate hash as its chain
        tip after accepting that block, so the observation itself is
        acceptance evidence -- even when the direct submitblock ack was lost
        in transport and the accepted success tail has not run (blockwait
        typically learns of the tip before, or instead of, the ack). Every
        tip-observation channel funnels through here so later disposition
        and abandon checks can outlive transient fork views.
        """
        key = tip_hash.lower()
        self._ensure_job_cache_state()
        newly_observed = False
        with self.lock:
            if key in self._outstanding_block_candidate_hashes:
                newly_observed = (
                    key not in self._tip_observed_accepted_block_hashes
                )
                self._tip_observed_accepted_block_hashes[key] = time.monotonic()
        if newly_observed:
            print(
                "prism coordinator: chain tip observation matches pool block "
                f"candidate hash={key}; acceptance registered pending "
                "finalization",
                flush=True,
            )

    def _block_candidate_acceptance_observed(self, block_hash: str) -> bool:
        """Whether a recent tip observation already proved this candidate landed."""
        self._ensure_job_cache_state()
        with self.lock:
            observed = self._tip_observed_accepted_block_hashes.get(
                block_hash.lower()
            )
        if observed is None:
            return False
        window = float(
            getattr(
                self,
                "observed_tip_accept_window_seconds",
                DEFAULT_PRISM_OBSERVED_TIP_ACCEPT_WINDOW_SECONDS,
            )
        )
        if window <= 0:
            return True
        return (time.monotonic() - observed) <= window

    def _block_candidate_chain_probe(
        self,
        block_hash: str,
        *,
        expected_height: int | None = None,
    ) -> bool | None:
        """Fresh chain verdict for a candidate: proven active, proven wrong, or unknown.

        Returns True when the candidate's own hash is the fresh best tip or
        an active chain header at its expected height, False when it is
        provably active at the wrong height (a corrupt intent, never a tip
        race), and None when this instantaneous view proves nothing (the
        hash absent during a tip race, or the probe itself failing).
        """
        key = block_hash.lower()
        # The two probes are independent: a best-tip lookup failure must not
        # suppress the active-header check, which subsumes it (the tip block
        # itself reports one confirmation) and can prove acceptance alone.
        try:
            if str(self.rpc.call("getbestblockhash")).lower() == key:
                self._note_tip_observation_for_candidates(key)
                return True
        except Exception:
            print(
                "prism coordinator: acceptance re-check best-tip probe "
                f"failed hash={key}; trying the active-header probe",
                flush=True,
            )
            traceback.print_exc()
        height: int | None = None
        try:
            height = self.active_block_candidate_height(key)
        except Exception:
            print(
                "prism coordinator: acceptance re-check header probe failed "
                f"hash={key}; falling back to tip-observation evidence",
                flush=True,
            )
            traceback.print_exc()
        if height is None:
            return None
        if expected_height is None or int(height) == int(expected_height):
            self._note_tip_observation_for_candidates(key)
            return True
        return False

    def _block_candidate_acceptance_pending(
        self,
        block_hash: str,
        *,
        expected_height: int | None = None,
    ) -> bool:
        """Return whether abandoning this candidate would discard an accepted block.

        A fresh probe wins in both directions: a candidate proven active is
        accepted even if its tip observation was missed, and a candidate
        proven active at the wrong height stays abandonable. When the probe
        cannot prove either way, a recent own-hash tip observation keeps the
        candidate deferring instead of terminal: qbitd accepted it once, so
        only a durably settled chain view may discard it.
        """
        probe = self._block_candidate_chain_probe(
            block_hash,
            expected_height=expected_height,
        )
        if probe is not None:
            return probe
        return self._block_candidate_acceptance_observed(block_hash)

    def _count_accept_pending_defer(self) -> None:
        with self.lock:
            self.block_candidate_accept_pending_defer_count = int(
                getattr(self, "block_candidate_accept_pending_defer_count", 0)
            ) + 1

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
        """Record a lost/failed block candidate as a BLOCK-path event.

        The share that produced the candidate was acknowledged and, when it met
        the share target, credited at submit time; the block losing its race
        afterwards does not un-earn it and is NOT a share rejection. It is
        counted under a dedicated block-abandonment counter (by reason, so a
        benign 'tip moved' race is distinguishable from a real
        submitblock-rejected/ledger failure) rather than the share-reject
        counters, which stay a true measure of shares refused to miners.

        Every terminal abandonment withdraws its payout-preview transition
        before the outcome becomes final. ``preserve_if_accepted`` closes the
        moved-tip race: if another attempt completed this hash's accepted
        success tail while withdrawal was in flight, the accepted disposition
        wins and the caller must finalize the outbox as submitted. Returns
        whether that accepted disposition won.

        Independent of that completed-tail record, a candidate whose own
        block hash is the fresh best tip, an active chain header at its
        expected height, a recent own-hash tip observation, or a fresh
        retained definitive submitblock success is an ACCEPTED block whose
        finalization is still pending (for example after a lost submitblock
        ack, or when saturation makes both probes answer "unknown" at this
        instant). Terminal abandonment would discard its payout
        accounting and withdraw its landed preview -- fencing payout
        publication for work qbitd already accepted -- so such candidates
        defer for retry instead; only hashes provably absent from the active
        chain (past the observation and retained-acceptance windows)
        abandon terminally. The terminal
        seal re-reads observation evidence atomically, so callers can order
        follow-up durable work (rejecting prepared payout rows) strictly
        afterward. Abandonment metrics commit only once that cleanup succeeds
        or a false finalize-only disposition is frozen.
        """
        if reason in PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS:
            self._defer_block_candidate(reason, message, worker=worker)
            return False
        if (
            stale_job_class is not None
            and stale_job_class not in PRISM_STALE_JOB_ABANDON_CLASSES
        ):
            raise ValueError(
                f"unknown stale job abandon class: {stale_job_class}"
            )
        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        if (
            preserve_if_accepted
            and self._block_candidate_acceptance_recorded(block_hash)
        ):
            # Durable accepted state already equals the prospective view, so
            # any transition recreated by the losing attempt is a no-op
            # override, not a withdrawal.
            self._clear_accepted_block_payout_preview(block_hash)
            outcome.reason = None
            outcome.error = None
            return True
        chain_probe = self._block_candidate_chain_probe(
            block_hash,
            expected_height=expected_height,
        )
        if chain_probe is True or (
            chain_probe is None
            and (
                self._block_candidate_acceptance_observed(block_hash)
                or self._block_candidate_acceptance_retained(block_hash)
            )
        ):
            self._count_accept_pending_defer()
            self._defer_block_candidate(
                PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
                "candidate is on (or was recently observed or definitively "
                "accepted as) the active chain; refusing terminal "
                f"abandonment (was {reason}: {message})",
                worker=worker,
            )
            return False

        # Own the cleanup invariant here rather than relying on every caller to
        # remember it. Invalidation can block behind another candidate pass;
        # recheck the accepted record afterward before committing abandonment.
        # Capture the transition first: if late acceptance evidence forces a
        # defer below, its published preview must be restorable.
        with self._accepted_block_payout_preview_condition:
            withdrawn_transition = self._accepted_block_payout_previews.get(
                block_hash.lower()
            )
        self._clear_accepted_block_payout_preview(
            block_hash,
            invalidate_published=True,
        )
        # The invalidation above can block long enough for the chain view to
        # heal (a buried accepted block is not always re-observed as the tip
        # while blockwait only reports the newest of rapid connects), so an
        # unknown pre-withdrawal verdict must re-probe before the terminal
        # commit. A provably wrong-height verdict is immutable (headers
        # cannot change height) and is never re-probed.
        late_probe = (
            chain_probe
            if chain_probe is False
            else self._block_candidate_chain_probe(
                block_hash,
                expected_height=expected_height,
            )
        )
        with self.lock:
            accepted_race_won = bool(
                preserve_if_accepted
                and block_hash.lower() in self._accounted_accepted_block_hashes
            )
            # A blockwait observation can also register during the blocking
            # invalidation. The disposition seal must consult that evidence
            # atomically or the same blind spot reopens inside this window;
            # the probe still wins both directions. Metrics are committed
            # later, after prepared-state cleanup can no longer reverse this
            # decision.
            late_acceptance_observed = bool(
                not accepted_race_won
                and (
                    late_probe is True
                    or (
                        late_probe is not False
                        and (
                            self._block_candidate_acceptance_observed(
                                block_hash
                            )
                            or self._block_candidate_acceptance_retained(
                                block_hash
                            )
                        )
                    )
                )
            )
            if not accepted_race_won and not late_acceptance_observed:
                outcome.reason = reason
                outcome.error = message
                outcome.stale_job_class = stale_job_class
                # Seal the disposition in the same critical section that
                # commits it: stop matching tip observations for this hash so
                # no acceptance evidence can register between this terminal
                # decision and the caller's follow-up durable work (rejecting
                # prepared payout rows). Observation registration takes this
                # same lock, so exclusion across the gap is total. A crash
                # before the durable outbox update replays the candidate,
                # which re-registers and re-evaluates from live chain state.
                self._outstanding_block_candidate_hashes.discard(
                    block_hash.lower()
                )
                self._tip_observed_accepted_block_hashes.pop(
                    block_hash.lower(),
                    None,
                )
        if accepted_race_won:
            self._clear_accepted_block_payout_preview(block_hash)
            outcome.reason = None
            outcome.error = None
            return True
        if late_acceptance_observed:
            # Restore the landed barrier the withdrawal just removed (and pop
            # its fail-closed tombstone): the candidate is still an accepted
            # block pending finalization, so descendant builders must keep
            # waiting on its preview -- not fail closed -- until the deferred
            # retry's accepted tail republishes it. Without this, retries
            # that keep deferring on observation evidence alone would leave
            # the tombstone fencing template refreshes, recreating the
            # coordination-blocked stall this path exists to prevent.
            self._begin_accepted_block_payout_preview(
                block_hash,
                block_height=expected_height,
            )
            if expected_height is not None:
                self._mark_accepted_block_payout_landed(
                    block_hash,
                    block_height=expected_height,
                )
            if (
                withdrawn_transition is not None
                and withdrawn_transition.preview is not None
            ):
                # The withdrawal superseded payout publication and, on a
                # lost publication race, left delivery fenced. Republish the
                # withdrawn preview now so admission reopens with the defer
                # rather than staying coordination-blocked across the
                # deferral cycles until the accepted tail republishes it.
                try:
                    self._publish_accepted_block_payout_preview(
                        block_hash,
                        self._materialize_prior_balance_preview(
                            withdrawn_transition.preview
                        ),
                    )
                except Exception:
                    print(
                        "prism coordinator: could not republish withdrawn "
                        f"payout preview hash={block_hash}; the scheduled "
                        "refresh will retry publication",
                        flush=True,
                    )
                    traceback.print_exc()
            self._count_accept_pending_defer()
            self._defer_block_candidate(
                PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
                "acceptance evidence arrived during payout-preview "
                f"withdrawal; refusing terminal abandonment (was {reason}: "
                f"{message})",
                worker=worker,
            )
            outcome.error = None
            return False

        print(
            f"prism coordinator: block candidate abandoned reason={reason}: {message}",
            flush=True,
        )
        return False

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
        """Land, verify, publish, persist, and confirm one candidate.

        The balance serializer spans the last prior-state check through durable
        confirmation. Reconciliation therefore cannot change the base beneath
        the accepted coinbase, while ordinary job delivery remains unblocked.
        The caller has already run submitblock on the lock/DB-free fast lane.
        The audit bundle build and verification execute with the serializer
        temporarily released (the landed fence stays armed), so neither block
        announcement nor job delivery waits on audit construction.
        """
        context = candidate.context
        submission = candidate.submission
        expected_height = int(context.template["height"])
        block_hash = str(submission.block_hash_hex).lower()
        parent_hash = str(context.template["previousblockhash"])
        self._ensure_job_cache_state()
        durable_payout_state = bool(
            getattr(self.ledger, "durable_payout_state", False)
        )
        with self._block_submitter_lock(
            self._payout_balance_mutation_lock,
            "payout-balance-mutation",
        ):
            if self._defer_for_pending_parent_payout_transition(
                block_hash=block_hash,
                parent_hash=parent_hash,
                parent_height=expected_height - 1,
                worker=worker,
                active_candidate_hash=block_hash if already_active else None,
                active_candidate_height=expected_height if already_active else None,
            ):
                return None
            block_state: dict[str, object] | None = None
            block_state_reader = getattr(self.ledger, "pool_block_state", None)
            transition_already_landed = self._accepted_block_payout_transition_landed(
                block_hash
            )
            reorg_reconciled: bool | None = None
            if already_active and not transition_already_landed:
                # A replayed active ancestor may coexist with balances from an
                # orphaned pool block. Reconcile that global state before this
                # transition becomes a landed barrier and before validating its
                # payout base.
                try:
                    reorg_reconciled = self.ensure_reorg_reconciled_for_tip(
                        current_tip,
                        _coalesce_same_tip=False,
                    )
                except Exception:
                    traceback.print_exc()
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "reorg reconciliation failed before block replay",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
                if not reorg_reconciled:
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "reorg reconciliation reported an untrusted chain view",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
            if already_active and callable(block_state_reader):
                self._record_block_submitter_phase("pool-block-state")
                block_state = block_state_reader(block_hash=block_hash)
                self._record_block_submitter_phase("pool-block-state:complete")
            already_confirmed = bool(
                block_state is not None
                and str(block_state.get("chain_state", "")) == "confirmed"
                and str(block_state.get("maturity_state", "")) != "reversed"
            )
            if already_confirmed:
                # The outbox terminal update can fail after a fully durable
                # confirmation. Do not replace later global balances with an
                # ancestor-only preview during exact-idempotent replay.
                self._clear_accepted_block_payout_preview(block_hash)
                reorg_reconciled = True
            elif already_active:
                self._begin_accepted_block_payout_preview(
                    block_hash,
                    block_height=expected_height,
                )
                self._mark_accepted_block_payout_landed(
                    block_hash,
                    block_height=expected_height,
                )
                reorg_reconciled = True
            elif transition_already_landed:
                # A prior attempt reached submitblock while holding this
                # serializer. External reconciliation is barred until it
                # confirms or is withdrawn, so retry its durable steps directly.
                reorg_reconciled = True
            else:
                try:
                    reorg_reconciled = self.ensure_reorg_reconciled_for_tip(
                        current_tip,
                        _coalesce_same_tip=False,
                    )
                except Exception:
                    traceback.print_exc()
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "reorg reconciliation failed before block submit",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
            if not reorg_reconciled:
                self._abandon_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "reorg reconciliation reported an untrusted chain view",
                    block_hash=block_hash,
                    worker=worker,
                )
                return None
            if (
                already_active
                and not already_confirmed
                and self._defer_for_pending_parent_payout_transition(
                    block_hash=block_hash,
                    parent_hash=parent_hash,
                    parent_height=expected_height - 1,
                    worker=worker,
                )
            ):
                return None
            # A late-visible append advances only the append-invalidation
            # epoch: the tip, payout generation, and balance digest all
            # survive, yet this candidate's coinbase pays from a window that
            # omitted the late row. The refresh wave retires the job
            # asynchronously; until it does, membership admission still lets
            # the pre-append job submit, so landing fails closed here.
            # Collection candidates settle solver-pays-all and are exempt, as
            # at every other epoch fence. A negative stamp is a candidate
            # reconstructed from durable intent: the caller revalidated its
            # recorded window against the durable ledger before this landing
            # and rebased it onto the live epoch sequence at that read, so
            # from here both kinds of candidate share one epoch fence. A
            # reconstructed candidate on a backend that cannot revalidate
            # carries no epoch and the fence stands down.
            context_append_epoch = int(
                getattr(context, "payout_append_invalidation_epoch", 0)
            )
            effective_append_epoch = (
                context_append_epoch
                if context_append_epoch >= 0
                else revalidated_append_epoch
            )
            with self._job_cache_lock:
                live_append_epoch = int(
                    self._payout_ledger_append_invalidation_epoch
                )
            if (
                not already_active
                and not getattr(context, "collection_only", False)
                and not node_submission.attempted
                and effective_append_epoch is not None
                and effective_append_epoch != live_append_epoch
            ):
                # Fail closed only while nothing has been offered: a
                # fast-lane candidate already reached qbitd, so a moved
                # epoch can no longer withhold its coinbase — the offer's
                # own classification below decides (an error retries
                # duplicate-safe, a rejection stays terminal, and an
                # accepted or duplicate result proceeds to the post-offer
                # fence, which keeps the as-issued snapshot for
                # accounting).
                self._abandon_block_candidate(
                    PRISM_REJECTION_STALE_JOB,
                    "payout window was invalidated by a late-visible share append",
                    block_hash=block_hash,
                    worker=worker,
                    expected_height=expected_height,
                    stale_job_class="append_epoch_stale",
                )
                return None
            if (
                durable_payout_state
                and not already_active
                and not self.prior_balances_match_current(context.prior_balances)
            ):
                self._abandon_block_candidate(
                    PRISM_REJECTION_STALE_JOB,
                    "prior balances changed since the job was issued",
                    block_hash=block_hash,
                    worker=worker,
                    expected_height=expected_height,
                    stale_job_class="balance_stale",
                )
                return None
            if not already_active and not node_submission.attempted:
                before_height = int(self.rpc.call("getblockcount"))
                if before_height + 1 != expected_height:
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BLOCK_STALE,
                        f"stale block height: template={expected_height} tip={before_height}",
                        block_hash=block_hash,
                        worker=worker,
                        expected_height=expected_height,
                    )
                    return None
            if not already_active:
                # Register before a fallback submitblock can expose this hash as the new
                # tip. Child builders will wait for the verified preview rather
                # than reading balances that omit their new parent.
                self._begin_accepted_block_payout_preview(
                    block_hash,
                    block_height=expected_height,
                )
                # Treat the submit outcome as uncertain before entering RPC.
                # If transport fails after qbitd accepted the block, this
                # conservative barrier preserves the coinbase's payout base.
                self._mark_accepted_block_payout_landed(
                    block_hash,
                    block_height=expected_height,
                )
                fallback_submit_under_fence = not node_submission.attempted
                if not node_submission.attempted:

                    def _verify_lease_before_submitblock() -> None:
                        # Runs at the RPC boundary — after any wait on the
                        # landing fence lock below — so the lease runway the
                        # verification proves is still intact when the RPC
                        # starts. A predating append_batch can hold that
                        # lock across its ledger commit and epoch bump far
                        # longer than any scheduling headroom, and a
                        # verification taken before the wait would measure
                        # runway the wait then consumes.
                        try:
                            self._require_fresh_ledger_lease_for_external_side_effect(
                                "submitblock"
                            )
                        except WriterLeaseRenewalDeferred:
                            if not transition_already_landed:
                                # The refusal fired before submitblock, so
                                # this attempt's outcome is not uncertain:
                                # qbitd provably never saw the block (no
                                # fast-lane node offer either — attempted is
                                # False). Unwind the landed bar this attempt
                                # armed — leaving it would bar reconciliation
                                # and payout-state publication for as long as
                                # the writer's own fenced write keeps the
                                # renewal deferred, though nothing needs
                                # preserving. The begun preview stays and the
                                # retry re-arms both. A bar landed by an
                                # earlier attempt that did reach submitblock
                                # keeps standing for that attempt's uncertain
                                # outcome.
                                self._unmark_accepted_block_payout_landed(
                                    block_hash
                                )
                            raise

                    # The epoch fence above is advisory: it releases the lock
                    # after one read, so an append-side bump could still commit
                    # between that read and the RPC below. This one is
                    # authoritative -- the bump acquires the same fence lock, so
                    # holding it across submitblock means no late-visible append
                    # can advance the epoch between this comparison and the
                    # block entering qbitd. The lock spans the lease
                    # verification and one RPC, and only on this boundary;
                    # ordinary share commits never touch it (the append side
                    # takes it only for rows that predate a live anchor, and
                    # this landing's own declared anchor stays exposed for
                    # the landing's duration).
                    append_epoch_raced = False
                    if (
                        effective_append_epoch is None
                        or getattr(context, "collection_only", False)
                    ):
                        _verify_lease_before_submitblock()
                        node_submission = self._submit_block_candidate_to_node(
                            candidate
                        )
                    else:
                        with self._payout_append_landing_fence_lock:
                            with self._job_cache_lock:
                                live_append_epoch = int(
                                    self._payout_ledger_append_invalidation_epoch
                                )
                            if live_append_epoch != effective_append_epoch:
                                append_epoch_raced = True
                            else:
                                _verify_lease_before_submitblock()
                                node_submission = (
                                    self._submit_block_candidate_to_node(candidate)
                                )
                    if append_epoch_raced:
                        self._abandon_block_candidate(
                            PRISM_REJECTION_STALE_JOB,
                            "payout window was invalidated by a late-visible share append",
                            block_hash=block_hash,
                            worker=worker,
                            expected_height=expected_height,
                            stale_job_class="append_epoch_stale",
                        )
                        return None
                if node_submission.error is not None:
                    raise node_submission.error
                result = node_submission.result
                if result not in (None, "duplicate"):
                    self._abandon_block_candidate(
                        PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                        f"submitblock rejected candidate: {result}",
                        block_hash=block_hash,
                        worker=worker,
                        expected_height=expected_height,
                    )
                    return None
                if (
                    not fallback_submit_under_fence
                    and effective_append_epoch is not None
                    and not getattr(context, "collection_only", False)
                ):
                    # The fast lane offered this block to qbitd without any
                    # ledger synchronization, so the landing fence can no
                    # longer gate submitblock itself. It still orders the
                    # accounting decision: wait out any fenced predating
                    # append whose durable commit is in flight (the commit
                    # holds this lock across its epoch bump). A moved epoch
                    # is no longer terminal here, though: every candidate
                    # reaching this point was accepted or already known by
                    # the node (rejections returned above), so its coinbase
                    # paid the as-issued window the moment it landed and no
                    # rebuild can change it. Abandoning would permanently
                    # discard payout accounting for a live block — the
                    # ledger would then re-pay the same carried balances in
                    # a later block — so the as-issued snapshot is kept for
                    # accounting and the predating share simply rides the
                    # next window (its ledger credit is untouched). The
                    # active-height check below still owns the
                    # did-it-actually-land verdict, and the pre-offer fence
                    # above still fails closed while nothing has been
                    # submitted.
                    with self._payout_append_landing_fence_lock:
                        with self._job_cache_lock:
                            live_append_epoch = int(
                                self._payout_ledger_append_invalidation_epoch
                            )
                    if live_append_epoch != effective_append_epoch:
                        print(
                            "prism coordinator: payout window was "
                            "invalidated by a late-visible share append "
                            f"after the node offer hash={block_hash}; "
                            "keeping the as-issued payout snapshot for "
                            "accounting",
                            flush=True,
                        )
                active_hash = str(
                    self.rpc.call("getblockhash", [expected_height])
                ).lower()
                if active_hash != block_hash:
                    self._abandon_block_candidate(
                        PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                        f"submitted block is not active at height {expected_height}",
                        block_hash=block_hash,
                        worker=worker,
                        expected_height=expected_height,
                    )
                    return None
                self._cancel_obsolete_job_builds("direct PRISM block accepted")
                self._mark_tip_refresh_pending(block_hash)
                self._schedule_tip_refresh_retry()

            preview: list[dict[str, object]] | None = None
            issued_preview = getattr(context, "prospective_prior_balances", None)
            if not already_confirmed and issued_preview is not None:
                # The compact preview came from the immutable issued job
                # summary. Publish it before rebuilding/canonicalizing the full
                # audit bundle, without retaining that bundle's shares tree.
                preview = self._materialize_prior_balance_preview(issued_preview)
                if durable_payout_state and not self.prior_balances_match_current(
                    context.prior_balances
                ):
                    self.request_shutdown()
                    self._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    self._abandon_block_candidate(
                        PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                        "accepted block payout base changed before preview publication",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
                self._publish_accepted_block_payout_preview(block_hash, preview)

            self._record_block_submitter_phase("audit-build")
            # The bundle derives only from inputs frozen on the candidate
            # (share window, prior balances, extranonces, template fields),
            # so the serializer is released around the builder/verifier
            # subprocess work: submitblock above has already run for fresh
            # candidates -- announcement is never delayed by audit
            # construction -- and the landed fence keeps reconciliation out
            # while job delivery proceeds. Child builds consume the compact
            # preview published above until the verified preview lands.
            with self._payout_balance_serializer_released():
                audit_store = self._ensure_audit_artifact_store()
                candidate_artifact = audit_store.issue_candidate(
                    block_hash=submission.block_hash_hex
                )
                candidate_bundle_path = candidate_artifact.path
                compiler_transferred_candidate = False

                def adopt_compiler_output(path: Path, value: os.stat_result) -> None:
                    nonlocal compiler_transferred_candidate
                    audit_store.adopt_compiler_candidate(
                        candidate_artifact,
                        path=path,
                        value=value,
                    )
                    compiler_transferred_candidate = True

                compiler_parent_fd = audit_store.duplicate_root_directory_fd()
                try:
                    final_bundle = self.build_audit_bundle(
                        shares=context.shares_json,
                        found_block=context.found_block,
                        prior_balances=context.prior_balances,
                        coinbase_script_sig_suffix_hex=self.coinbase_script_sig_suffix_hex(
                            candidate.extranonce1_hex,
                            candidate.extranonce2_hex,
                        ),
                        witness_merkle_leaves_hex=list(
                            getattr(context.job, "witness_merkle_leaves_hex", ())
                        )
                        or direct_stratum.witness_merkle_leaves_hex(
                            getattr(context.job, "transaction_hexes", ())
                        ),
                        ctv_fee_parent_hash=parent_hash,
                        canonical_output_path=candidate_bundle_path,
                        canonical_output_parent_fd=compiler_parent_fd,
                        canonical_output_adopter=adopt_compiler_output,
                    )
                except BaseException:
                    audit_store.discard_candidate(candidate_artifact)
                    raise
                finally:
                    os.close(compiler_parent_fd)
                # Compatibility builders used by tests and older integrations
                # may ignore canonical_output_path. Persist their logical
                # bundle via the normal canonicalization fallback without
                # mislabeling bytes.
                try:
                    if not candidate_bundle_path.exists():
                        candidate_bundle_path = audit_store.write_compatibility_candidate(
                            candidate_artifact,
                            final_bundle,
                        )
                    else:
                        if not compiler_transferred_candidate:
                            raise RuntimeError(
                                "audit builder created an output path without exact inode transfer"
                            )
                    final_manifest = final_bundle["signed_coinbase_manifest"]["manifest"]
                    final_coinbase_tx_hex_raw = final_manifest["coinbase_tx_hex"]
                    if not isinstance(final_coinbase_tx_hex_raw, str):
                        raise ValueError(
                            "final audit bundle coinbase_tx_hex is not a string"
                        )
                    final_coinbase_tx_hex = final_coinbase_tx_hex_raw.lower()
                except BaseException:
                    audit_store.discard_candidate(candidate_artifact)
                    raise
                if final_coinbase_tx_hex != submission.coinbase_tx_hex.lower():
                    audit_store.discard_candidate(candidate_artifact)
                    self.request_shutdown()
                    self._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    self._abandon_block_candidate(
                        PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
                        "final audit bundle coinbase does not match submitted coinbase",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
            payout_commit_started: float | None = None
            payout_commit_source: int | None = None
            try:
                with self._payout_balance_serializer_released():
                    self._record_block_submitter_phase("audit-verify")
                    verifier_override = self.__dict__.get("verify_bundle")
                    configured_writer_key = getattr(
                        self,
                        "ledger_writer_public_key_hex",
                        None,
                    )
                    verified_audit = audit_store.verify_candidate(
                        candidate_artifact,
                        coinbase_tx_hex=submission.coinbase_tx_hex,
                        expected_coinbase_value_sats=int(
                            context.template["coinbasevalue"]
                        ),
                        expected_block_height=expected_height,
                        trusted_writer_public_key_hex=(
                            self.trusted_ledger_writer_public_key_hex(final_bundle)
                        ),
                        trust_source=(
                            "configured"
                            if configured_writer_key is not None
                            else "embedded_test_only"
                        ),
                        verifier=(
                            verifier_override
                            if callable(verifier_override)
                            else None
                        ),
                    )
                    audit_store.require_current_verified_candidate(
                        verified_audit,
                        candidate_artifact,
                    )
                    report = dict(verified_audit.report)
                    persistence_canonical_bundle_path = (
                        candidate_bundle_path
                        if verified_audit.canonical_copy_eligible
                        else None
                    )
                    verified_preview = (
                        self._accepted_block_payout_preview_from_bundle(
                            final_bundle,
                            prior_balances=context.prior_balances,
                        )
                    )
                self._record_block_submitter_phase("audit-verify:complete")
                if not already_confirmed:
                    if preview is None and durable_payout_state:
                        live_prior_balances = self.settlement_balances_by_program(
                            self.ledger.current_prior_balances()
                        )
                        expected_prior_balances = self.settlement_balances_by_program(
                            context.prior_balances
                        )
                        if live_prior_balances != expected_prior_balances:
                            self.request_shutdown()
                            self._clear_accepted_block_payout_preview(
                                block_hash,
                                invalidate_published=True,
                            )
                            self._abandon_block_candidate(
                                PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                                "accepted block payout base changed before preview publication",
                                block_hash=block_hash,
                                worker=worker,
                            )
                            return None
                    try:
                        self._publish_accepted_block_payout_preview(
                            block_hash,
                            verified_preview,
                        )
                    except RuntimeError as exc:
                        self.request_shutdown()
                        self._clear_accepted_block_payout_preview(
                            block_hash,
                            invalidate_published=True,
                        )
                        self._abandon_block_candidate(
                            PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
                            "verified final payout preview does not match the "
                            f"issued block job: {exc}",
                            block_hash=block_hash,
                            worker=worker,
                        )
                        return None
                preview = verified_preview

                # The verified preview is now the effective balance snapshot,
                # so persistence can do canonicalization, body writes, copies,
                # and bulk SQL without owning the delivery gate.
                payout_commit_started = time.monotonic()
                payout_commit_source = self._capture_payout_state_source()[1]
                self._record_block_submitter_phase("persist-accepted-block")
                persistence = self.ledger.persist_accepted_block(
                    block_hash=submission.block_hash_hex,
                    block_height=expected_height,
                    parent_hash=parent_hash,
                    final_bundle=final_bundle,
                    audit_report=report,
                    canonical_bundle_path=persistence_canonical_bundle_path,
                )
                self._record_block_submitter_phase("persist-accepted-block:complete")
                active_hash = str(
                    self.rpc.call("getblockhash", [expected_height])
                ).lower()
                if active_hash != block_hash:
                    if already_confirmed:
                        self._abandon_block_candidate(
                            PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                            "accepted ancestor left the active chain during replay",
                            block_hash=block_hash,
                            worker=worker,
                        )
                        return None
                    # Seal the disposition BEFORE touching the prepared
                    # payout rows: a rejected row can never be promoted by a
                    # later confirmation (and reactivation only covers
                    # inactive rows), so rejection must follow -- never
                    # precede -- a terminal decision. The abandon defers on
                    # live acceptance evidence; its terminal commit consults
                    # that evidence atomically with the commit AND seals the
                    # hash against further observation matching, so no
                    # evidence can register anywhere in the gap between the
                    # sealed decision and this rejection. A crash in between
                    # leaves the outbox row pending; restart replay
                    # re-registers the hash and re-evaluates from live chain
                    # state before rejecting.
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BLOCK_STALE,
                        "accepted block left the active chain before ledger confirmation",
                        block_hash=block_hash,
                        worker=worker,
                        expected_height=expected_height,
                    )
                    outcome = getattr(self, "_block_candidate_outcome", None)
                    sealed_reason = (
                        getattr(outcome, "reason", None)
                        if outcome is not None
                        else None
                    )
                    if sealed_reason == PRISM_REJECTION_BLOCK_STALE:
                        active_tip_height = int(self.rpc.call("getblockcount"))
                        self.reject_prepared_block(
                            block_hash=block_hash,
                            active_tip_height=active_tip_height,
                        )
                    return None
                self._record_block_submitter_phase("confirm-accepted-block")
                with audit_store.publication_order_guard():
                    confirmation = self.ledger.confirm_accepted_block(
                        block_hash=block_hash,
                        # The ledger confirmation function matches this value
                        # against the candidate row's own height. An accepted
                        # ancestor can be finalized after newer blocks arrive.
                        active_tip_height=expected_height,
                    )
                    confirmed_count = int(confirmation.get("confirmed_count", 0))
                    if confirmed_count == 1:
                        audit_publication_identity = (
                            self._audit_publication_identity(
                                block_hash=block_hash,
                                block_height=expected_height,
                                confirmation=confirmation,
                            )
                        )
                self._record_block_submitter_phase(
                    "confirm-accepted-block:complete"
                )
                if confirmed_count != 1:
                    # -1 is the ledger's superseded disposition: the row was
                    # terminally disposed (reorg quarantine, rejection, or
                    # reversal) before this confirmation landed. That is
                    # terminal for the candidate but benign for the pool, so
                    # the coordinator keeps serving; the shutdown escalation
                    # is reserved for a genuinely unexplained failure.
                    superseded = confirmed_count == -1
                    if not superseded:
                        self.request_shutdown()
                    self._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    if superseded:
                        self._abandon_block_candidate(
                            PRISM_REJECTION_LEDGER_CONFIRMATION_SUPERSEDED,
                            "ledger row for accepted block "
                            f"{block_hash} was superseded before confirmation",
                            block_hash=block_hash,
                            worker=worker,
                        )
                    else:
                        self._abandon_block_candidate(
                            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                            f"ledger did not confirm accepted block {block_hash}",
                            block_hash=block_hash,
                            worker=worker,
                        )
                    return None

                if durable_payout_state:
                    # Compare the durable active-chain view as of this block,
                    # not the global latest view: an exact replay may finalize
                    # ancestor A after later pool block B is already confirmed.
                    # This also preserves the invariant across restart after a
                    # prior post-confirm mismatch instead of silently accepting
                    # the already-confirmed row on the next attempt.
                    as_of_reader = getattr(
                        self.ledger,
                        "prior_balances_after_pool_block",
                        None,
                    )
                    confirmed_balances = self.normalized_prior_balances(
                        as_of_reader(block_hash=block_hash)
                        if callable(as_of_reader)
                        else self.ledger.current_prior_balances()
                    )
                    if self.settlement_balances_by_program(
                        confirmed_balances
                    ) != self.settlement_balances_by_program(preview):
                        self.request_shutdown()
                        self._clear_accepted_block_payout_preview(
                            block_hash,
                            invalidate_published=True,
                        )
                        self._abandon_block_candidate(
                            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                            "confirmed payout balances do not match the published "
                            f"preview for accepted block {block_hash}",
                            block_hash=block_hash,
                            worker=worker,
                        )
                        return None
                # Durability caught up to the already-published logical state;
                # clearing the parent override needs no second generation bump.
                self._clear_accepted_block_payout_preview(block_hash)
                self._schedule_current_payout_ledger_artifact_if_missing()
                payout_publication_required = (
                    self._payout_source_requires_publication()
                )
                payout_publication_fenced = (
                    self._payout_state_publication_fenced()
                )
                if payout_publication_required or payout_publication_fenced:
                    # A covered replay normally has no publication work. The
                    # exception is a leaked delivery fence whose source already
                    # published: force one republish so the replay heals it.
                    covered_replay_fence = (
                        payout_publication_fenced
                        and not payout_publication_required
                    )
                    with self.lock:
                        pending_cause = self._payout_state_source[2]
                    # A bounded preview-publication loss already left the gate
                    # fenced and its retry scheduled. Do not monopolize the
                    # submitter with a second retry budget. Uncertain commits,
                    # ordinary unfenced tip sources, and a covered replay's
                    # leaked fence still reconcile now.
                    publish_now = (
                        covered_replay_fence
                        or pending_cause == "direct_block_uncertain"
                        or not payout_publication_fenced
                    )
                    published: int | None = None
                    if publish_now and getattr(
                        self,
                        "reorg_reconciler_enabled",
                        True,
                    ):
                        with self.lock:
                            latest_tip = self._payout_state_source[1]
                        summary = self.reconcile_prism_pool_blocks_once(
                            tip_hash=latest_tip,
                            _force_publish=True,
                            _source_reserved=True,
                        )
                        reconciled_generation = summary.get("published_generation")
                        if isinstance(reconciled_generation, int):
                            published = reconciled_generation
                    elif publish_now:
                        published = (
                            self._publish_current_payout_state_with_retry_budget()
                        )
                    if publish_now and published is None:
                        # The block is durably confirmed; only the payout
                        # publication lost its race. Aborting would keep the
                        # outbox row pending and replay persist/confirm churn
                        # for an already-final block. Keep delivery fenced and
                        # let the scheduled tip refresh publish the newest
                        # source; this candidate's durable work is complete.
                        self._block_payout_state_publication()
                        print(
                            "prism coordinator: accepted block confirmed "
                            "durably; payout publication deferred to the "
                            f"scheduled refresh hash={block_hash}",
                            flush=True,
                        )
                return (
                    final_bundle,
                    report,
                    persistence,
                    confirmation,
                    audit_publication_identity,
                    dict(verified_audit.verification_identity),
                )
            except Exception:
                if payout_commit_started is not None and payout_commit_source is not None:
                    # Persistence/confirmation can report failure after a
                    # durable partial commit. Supersede every prepared source
                    # and keep all delivery fenced until replay/reconciliation
                    # proves the resulting ledger state.
                    self._block_payout_state_publication(
                        supersede_with=(
                            payout_commit_source,
                            block_hash,
                            "direct_block_uncertain",
                            payout_commit_started,
                        )
                    )
                raise
            finally:
                if payout_commit_started is not None:
                    self._observe_payout_state_seconds(
                        "preparation",
                        max(0.0, time.monotonic() - payout_commit_started),
                    )
                audit_store.discard_candidate(candidate_artifact)

    @ledger_writer_operation("accepted_block_handling")
    def submit_block_candidate(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission | None = None,
    ) -> bool:
        """Land one block candidate, then finalize its audit and payout state.

        Runs on the block-submitter thread (tests call it synchronously). It
        never raises for a lost race and holds self.lock only for short
        in-memory state mutation -- never across RPC, psql, subprocess, or
        file I/O -- so share acks and job pushes stay fast while a block
        lands. The durable candidate outbox is the pre-submit recovery boundary;
        full audit and payout persistence happens after the latency-sensitive
        ``submitblock`` call and is replayable after a crash. Returns True only
        after that finalization completes.
        """
        block_hash = str(candidate.submission.block_hash_hex).lower()
        with self._block_candidate_disposition(block_hash):
            terminal_outcome = self._block_candidate_terminal_outcome(block_hash)
            if terminal_outcome is not None:
                return terminal_outcome
            if node_submission is None:
                node_submission = self._node_submission_for_direct_candidate(candidate)
            accepted = self._submit_block_candidate_serialized(
                candidate,
                node_submission=node_submission,
            )
            if not accepted:
                outcome = getattr(self, "_block_candidate_outcome", None)
                if outcome is not None:
                    # Direct embedders do not use the outbox-finalization
                    # wrapper. A normal return means the serialized path also
                    # completed any prepared-state rejection it initiated.
                    self._record_committed_block_candidate_abandonment(
                        block_hash,
                        outcome,
                    )
            return accepted

    def _record_block_candidate_progress(
        self,
        phase: str = "accounting-progress",
    ) -> None:
        """Stamp the submitter heartbeat at a candidate-disposition boundary.

        Stamps come only from the dedicated submitter thread: dispositions
        also run on client connection threads (synchronous below-target
        solves), and a stamp from those threads would refresh the
        ``block_submitter`` budget while the dedicated thread might be
        wedged elsewhere. A disposition crosses several ledger and
        filesystem writes that slow down together under database pressure;
        stamping each completed phase on the owner thread keeps a
        progressing disposition inside the liveness budget while a wedged
        phase still leaves the watchdog able to recover the process -- the
        same shape as the CTV broadcaster's per-row stamping, whose name
        likewise maps to a single thread.
        """
        self._record_block_submitter_phase(phase)

    def _submit_block_candidate_serialized(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> bool:
        """Process a candidate while its same-hash disposition guard is held."""
        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.reason = None
        outcome.error = None
        outcome.stale_job_class = None
        context = candidate.context
        submission = candidate.submission
        worker = candidate.client.username or None
        expected_height = int(context.template["height"])
        block_hash = str(submission.block_hash_hex).lower()
        parent_hash = str(context.template["previousblockhash"])
        self._ensure_job_cache_state()
        # Every disposition (queue drain, synchronous below-target submit,
        # outbox replay, retained retry) marks its hash outstanding so tip
        # observations arriving on other threads can register acceptance.
        self._register_outstanding_block_candidate(block_hash)
        self._record_block_candidate_progress("disposition-start")
        if (
            self._block_candidate_acceptance_recorded(block_hash)
            and node_submission.error is not None
        ):
            # A concurrent same-hash pass completed the success tail while
            # this duplicate-safe node offer waited for disposition. Do not
            # recreate its payout transition or accounting work.
            self._clear_accepted_block_payout_preview(block_hash)
            return True
        with self.lock:
            accepted_count = int(self.accepted_block_count)
            pool_closed = (
                block_hash not in self._accounted_accepted_block_hashes
                and (
                    accepted_count >= int(self.max_blocks)
                    or (bool(self.stop_after_block) and accepted_count >= 1)
                )
            )
        if pool_closed and self._block_candidate_chain_probe(
            block_hash,
            expected_height=expected_height,
        ) is True:
            # The chain provably contains this block; its payout accounting
            # must complete regardless of when the pool stopped accepting
            # new work. Fall through to the normal disposition below, which
            # resumes the accepted success tail. Observation evidence alone
            # deliberately does not open this gate -- an unprovable view
            # defers via the abandon path instead, so a closed pool can
            # never fall through to submitblock on stale evidence.
            pool_closed = False
        if pool_closed:
            self._abandon_block_candidate(
                PRISM_REJECTION_POOL_CLOSED,
                "pool is no longer accepting blocks",
                block_hash=block_hash,
                worker=worker,
                expected_height=expected_height,
            )
            return False
        self._record_block_candidate_progress("current-tip-rpc")
        observed_tip = str(self.rpc.call("getbestblockhash"))
        self._record_block_candidate_progress("current-tip-rpc:complete")
        # A successful or transport-ambiguous fast-lane call can change the
        # tip before this post-submit probe. It is still a *fresh* attempt,
        # not an active replay: run the normal validation/persistence tail
        # against the candidate's stamped parent. A later getblockhash check
        # proves a successful acknowledgement, while an ambiguous transport
        # outcome stays pending for duplicate-safe replay. Duplicate replies
        # are replay evidence and retain the live-tip classification; the
        # abandonment tie-breaker separately consults this process's own
        # recorded offer evidence so an unprovable chain view cannot
        # terminally discard a block the node already told us it has.
        fresh_or_uncertain_submit = bool(
            node_submission.attempted
            and (
                node_submission.error is not None
                or node_submission.result is None
            )
        )
        current_tip = parent_hash if fresh_or_uncertain_submit else observed_tip
        landed_height: int | None = None
        if current_tip.lower() == block_hash:
            landed_height = expected_height
        elif current_tip != parent_hash:
            try:
                landed_height = self.active_block_candidate_height(block_hash)
            except Exception:
                traceback.print_exc()
                self._abandon_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "could not determine whether a prior candidate is active",
                    block_hash=block_hash,
                    worker=worker,
                )
                return False
        already_active = landed_height == expected_height
        if landed_height is not None and not already_active:
            self._abandon_block_candidate(
                PRISM_REJECTION_BLOCK_STALE,
                f"candidate active at unexpected height {landed_height}",
                block_hash=block_hash,
                worker=worker,
                expected_height=expected_height,
            )
            return False
        if already_active:
            # A disposition probe is a tip observation too: remember it so a
            # later attempt cannot terminally abandon this hash on a racing
            # chain snapshot after this attempt fails mid-tail.
            self._note_tip_observation_for_candidates(block_hash)
            print(
                "prism coordinator: resuming finalization for active block candidate "
                f"height={landed_height} hash={submission.block_hash_hex}",
                flush=True,
            )
        elif parent_hash != current_tip:
            if self._block_candidate_acceptance_recorded(block_hash):
                # A duplicate wakeup can reach this check after the accepted
                # success tail but after a newer tip hides the candidate from
                # the active-header probe. Its durable work is already done;
                # let the caller finalize the outbox as submitted.
                self._clear_accepted_block_payout_preview(block_hash)
                return True
            accepted_race_won = self._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                f"tip moved before submit: {current_tip}",
                block_hash=block_hash,
                worker=worker,
                preserve_if_accepted=True,
                expected_height=expected_height,
                stale_job_class="tip_moved",
            )
            return accepted_race_won
        if (
            node_submission.attempted
            and node_submission.error is None
            and node_submission.result not in (None, "duplicate")
            and not already_active
        ):
            self._abandon_block_candidate(
                PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                f"submitblock rejected candidate: {node_submission.result}",
                block_hash=block_hash,
                worker=worker,
                expected_height=expected_height,
            )
            return False
        # A reconstructed candidate revalidates BEFORE the balance
        # serializer: the audit share-window replay is the slow oracle walk
        # and takes the ledger writer lock, so running it inside
        # _payout_balance_mutation_lock would stall share persistence and
        # every other landing for the walk's duration. The read is safe out
        # here because the window is append-only content -- a pass can only
        # be invalidated by a later append, and any such append after the
        # baseline epoch captured below advances the live epoch (this
        # landing's declared anchor stays exposed to the append-side
        # predates() checks), which the landing's fences compare against.
        collection_only = bool(getattr(context, "collection_only", False))
        context_append_epoch = int(
            getattr(context, "payout_append_invalidation_epoch", 0)
        )
        revalidated_append_epoch: int | None = None
        landing_anchor_token: int | None = None
        if not already_active and not collection_only:
            found_block = getattr(context, "found_block", None)
            declared_anchor_ms = (
                found_block.get("anchor_job_issued_at_ms")
                if isinstance(found_block, dict)
                else None
            )
            if declared_anchor_ms is not None:
                # With no armed artifact and no in-flight walk (an outbox
                # replay at startup, or a landing after the artifact was
                # disarmed), a replay-shaped append would skip the epoch
                # bump entirely; exposing the landing window's own anchor
                # guarantees the bump the fences below check for.
                landing_anchor_token = self._expose_inflight_scan_anchor(
                    int(declared_anchor_ms)
                )
                # An append classified as unfenced before that exposure
                # commits outside the landing fence, so its row could
                # become durable mid-submitblock while its epoch bump
                # queues behind the fence this landing holds across the
                # RPC. Wait those commits (and their bump attempts
                # against the now-exposed anchor) out before any window
                # revalidation or epoch fence runs; no fence is held
                # here, so the bump can always proceed.
                self._await_unfenced_appends_predating_anchor(
                    int(declared_anchor_ms)
                )
        try:
            reconstructed_needs_revalidation = (
                not already_active
                and not collection_only
                and context_append_epoch < 0
                and bool(getattr(self.ledger, "durable_payout_state", False))
            )
            if reconstructed_needs_revalidation and node_submission.attempted:
                # Revalidation guards an offer the node has not yet seen: a
                # reconstructed window that omits a durably appended share
                # must not mint a coinbase. Once the fast lane has offered
                # the durable bytes, the coinbase is the node's to judge —
                # an accepted or duplicate result proceeds with the
                # as-issued snapshot (the post-offer epoch fences log
                # rather than abandon), a rejection stayed terminal above,
                # and an ambiguous transport error re-offers duplicate-
                # safely on retry. Abandoning an already-offered candidate
                # here would permanently discard payout accounting for a
                # block qbitd may have accepted, and the audit walk is the
                # slow oracle whose deadline under saturation would
                # otherwise defer-loop an accepted block forever.
                print(
                    "prism coordinator: reconstructed candidate was already "
                    f"offered to the node hash={block_hash}; keeping the "
                    "as-issued payout snapshot (window revalidation "
                    "skipped)",
                    flush=True,
                )
            if (
                reconstructed_needs_revalidation
                and not node_submission.attempted
            ):
                with self._job_cache_lock:
                    revalidation_base_epoch = int(
                        self._payout_ledger_append_invalidation_epoch
                    )
                try:
                    window_reproducible = (
                        self._replayed_payout_window_reproducible(context)
                    )
                except Exception:
                    traceback.print_exc()
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "durable share-window replay failed for the "
                        "reconstructed candidate",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return False
                if not window_reproducible:
                    self._abandon_block_candidate(
                        PRISM_REJECTION_STALE_JOB,
                        "replayed payout window omits a durably appended share",
                        block_hash=block_hash,
                        worker=worker,
                        expected_height=expected_height,
                        stale_job_class="append_epoch_stale",
                    )
                    return False
                revalidated_append_epoch = revalidation_base_epoch
            landed = self._land_and_confirm_block_candidate(
                candidate,
                # Fresh-attempt classification may use the stamped parent, but
                # payout reconciliation must follow the observed post-submit tip.
                current_tip=observed_tip,
                already_active=already_active,
                worker=worker,
                node_submission=node_submission,
                revalidated_append_epoch=revalidated_append_epoch,
            )
        finally:
            self._retire_inflight_scan_anchor(landing_anchor_token)
        if landed is None:
            return False
        (
            final_bundle,
            report,
            persistence,
            confirmation,
            audit_publication_identity,
            audit_verification_identity,
        ) = landed
        self._record_block_candidate_progress("durable-accounting:complete")
        with self.lock:
            already_accounted = block_hash in self._accounted_accepted_block_hashes
        if already_accounted:
            # The previous attempt completed every success side effect but its
            # durable outbox terminal update failed. submit_next will retry that
            # update after this exact-idempotent confirmation without double
            # counting the block or replacing newer evidence/work.
            return True
        ctv_persistence = None
        ctv_manifest_set = final_bundle.get("ctv_fanout_manifest_set")
        if isinstance(ctv_manifest_set, dict):
            self._record_block_candidate_progress("ctv-manifest-persist")
            ctv_persistence = self.ledger.persist_ctv_fanout_manifest_set(
                block_hash=block_hash,
                manifest_set=ctv_manifest_set,
                manifest_set_sha256=sha256_json_hex(ctv_manifest_set),
            )
            self._record_block_candidate_progress("ctv-manifest-persist:complete")
        if candidate.credit_share_on_accept:
            self._record_block_candidate_progress("accepted-share-credit")
            self.append_accepted_share(
                candidate.client,
                context,
                submission,
                candidate.pending_share,
                candidate_intent=self.block_candidate_intent(candidate),
            )
            # The preview window was intentionally clamped below this pending
            # winning share. Once the append is durable, enqueue an urgent
            # delta fold so a rapidly found child does not wait for the normal
            # 60-second cadence to include its payout obligation.
            with self._job_cache_lock:
                payout_generation = self._payout_state_generation
                template_artifacts = self._template_artifacts
            if template_artifacts is not None:
                self._schedule_payout_ledger_artifact_preparation(
                    payout_generation,
                    template_artifacts.network_difficulty,
                    bypass_build_interval=True,
                )
            self._record_block_candidate_progress("accepted-share-credit:complete")
        # Aggregate counts only: materializing the whole share history
        # (all_shares) here would scan the full ledger twice per block,
        # and would grow without bound as the ledger grows. The counters are
        # served from the ledger's maintained cache; a cold cache (first
        # read after process start) runs the exact aggregate synchronously
        # and stays watchdog-eligible on purpose, so a wedged read keeps the
        # exit-and-replay recovery path instead of hanging the disposition
        # invisibly.
        self._record_block_candidate_progress("accepted-share-stats")
        evidence_share_count, evidence_distinct_miners = self.accepted_share_stats()
        self._record_block_candidate_progress("accepted-share-stats:complete")
        evidence = {
            "schema": "qbit.prism.live-stratum-evidence.v1",
            "block_hash": block_hash,
            "block_height": expected_height,
            "coinbase_tx_hex": submission.coinbase_tx_hex,
            "audit_report": report,
            "ledger_backend": self.ledger.backend_name,
            "persistence": persistence,
            "confirmation": confirmation,
            "audit_verification_identity": audit_verification_identity,
            "ctv_persistence": ctv_persistence,
            "accepted_share_count": evidence_share_count,
            "distinct_miner_count": evidence_distinct_miners,
            "job_share_count": len(context.shares_json),
        }
        publication_persistence = dict(persistence)
        publication_persistence.setdefault(
            "audit_bundle_sha256",
            report.get("audit_bundle_sha256_hex"),
        )
        publication_persistence.setdefault("body_uri", "")
        evidence["persistence"] = publication_persistence
        audit_store = self._ensure_audit_artifact_store()
        self._record_block_candidate_progress("evidence-write")
        with self._payout_balance_mutation_lock:
            with audit_store.publication_order_guard():
                publication_floor_reader = getattr(
                    self.ledger,
                    "audit_publication_sequence_floor",
                    None,
                )
                if callable(publication_floor_reader):
                    # This is deliberately a fresh durable-row read immediately
                    # before A1 publication. Confirmation-time state or a raw
                    # sequence value cannot fence rollback gaps and restart
                    # replays. P1's local serializer plus A1's process guard
                    # prevent another confirmation/reactivation from allocating
                    # between this read and the durable publication decision.
                    publication_floor_sequence = publication_floor_reader()
                else:
                    # Compatibility-only ledgers used by legacy embeddings/tests
                    # do not own durable ordinal state. Production memory/Postgres
                    # backends implement the reader above.
                    publication_floor_sequence = (
                        audit_publication_identity.sequence
                    )
                publication = audit_store.publish_success(
                    identity=audit_publication_identity,
                    publication_floor_sequence=publication_floor_sequence,
                    report=report,
                    persistence=publication_persistence,
                    evidence=evidence,
                    verification_identity=audit_verification_identity,
                    created_at=public_api.utc_now_iso(),
                )
        evidence = dict(publication.evidence)
        self._record_block_candidate_progress("evidence-write:complete")
        with self.lock:
            newly_accounted = block_hash not in self._accounted_accepted_block_hashes
            if newly_accounted:
                self._accounted_accepted_block_hashes.add(block_hash)
                self.accepted_block_count += 1
                # Replace this hash's provisional capacity reservation with
                # its durable accounted slot atomically. Keeping both until
                # the outbox terminal write would double-count the block and
                # unnecessarily reject an unrelated next solve.
                self._block_fast_lane_reservations.discard(block_hash)
            self.latest_coinbase_size_bytes = len(
                str(
                    final_bundle["signed_coinbase_manifest"]["manifest"][
                        "coinbase_tx_hex"
                    ]
                )
            ) // 2
            should_stop = (
                newly_accounted
                and (self.stop_after_block or self.accepted_block_count >= self.max_blocks)
            )
        if not newly_accounted:
            return True
        print(
            "prism coordinator: qbit accepted direct PRISM block "
            f"height={expected_height} hash={block_hash}",
            flush=True,
        )
        if should_stop:
            self.request_shutdown()
        else:
            # The public submitter wrapper performs this fanout only after its
            # writer scope (including outbox finalization) exits. The rare
            # synchronous share path consumes the same marker after sending
            # the Stratum result.
            candidate.client.post_accept_refresh_block = (
                expected_height,
                block_hash,
            )
        return True

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
        # A cached snapshot's top-level ``ok`` already includes the progress
        # state from snapshot time. Recompute that half from the stable mining
        # readiness field so a fresh in-memory progress overlay can both fail
        # and recover immediately without hiding an independent mining fault.
        return dict(overlay_progress_health(payload, progress))

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

    def mining_delivery_snapshot(self, *, now: float | None = None) -> dict[str, object]:
        now = time.monotonic() if now is None else now
        with self.lock:
            self._ensure_initial_job_state()
            active = len(self.clients)
            current_tip = self._current_published_tip_hash_locked()
            published_snapshot = getattr(self, "tip_template_snapshot", None)
            subscribed = sum(1 for client in self.clients if client.subscribed)
            authorized_clients = [
                client
                for client in self.clients
                if client.subscribed and client.authorized and client.worker is not None
            ]
            authorized = len(authorized_clients)
            clients_with_current_work = [
                client
                for client in authorized_clients
                if self._client_has_current_tip_job_locked(client)
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
                getattr(self, "_payout_state_generation", 0)
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
            pending_requests = list(self.pending_initial_jobs.values())
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
                        self.pending_initial_jobs[client].requested_monotonic
                        if client in self.pending_initial_jobs
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
                    self,
                    "stratum_max_connections",
                    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
                )
            )
            pending_limit = int(self.stratum_max_pending_initial_jobs)
            coverage = current / authorized if authorized else 1.0
            semantic_coverage = (
                semantic_current / authorized if authorized else 1.0
            )
            cap_saturated = connection_limit > 0 and active >= connection_limit
            pending_saturated = pending >= pending_limit
            # A reconnect incident is operationally significant well before
            # nearly every miner is missing work. Treat any sustained loss of
            # at least five percent of current-job coverage as degraded.
            poor_coverage = authorized > 0 and coverage < 0.95
            if poor_coverage:
                if self._mining_delivery_failure_started_monotonic is None:
                    self._mining_delivery_failure_started_monotonic = now
            else:
                self._mining_delivery_failure_started_monotonic = None
            delivery_failure_age = (
                max(0.0, now - self._mining_delivery_failure_started_monotonic)
                if self._mining_delivery_failure_started_monotonic is not None
                else 0.0
            )
            overload_now = pending_saturated or (cap_saturated and poor_coverage)
            if overload_now:
                if self._mining_overload_started_monotonic is None:
                    self._mining_overload_started_monotonic = now
            else:
                self._mining_overload_started_monotonic = None
            overload_age = (
                max(0.0, now - self._mining_overload_started_monotonic)
                if self._mining_overload_started_monotonic is not None
                else 0.0
            )
            timeout = float(self.stratum_initial_job_timeout_seconds)
            timeout_disconnects = self.initial_job_timeout_count
            queue_rejections = self.initial_job_queue_rejection_count
            cancelled = self.initial_job_cancelled_count
            coalesced = self.initial_job_coalesced_count
            queue_capacity_reclaimed = (
                self.initial_job_queue_capacity_reclaimed_count
            )
            peak = self.peak_active_connection_count
            handlers = self.handler_thread_count

        self._ensure_job_cache_state()
        with self._job_cache_lock:
            prepared_bundle = self._prepared_ready_bundle
            prepared_snapshot = self._prepared_ready_snapshot
            preparation_pending = bool(self.job_preparation_pending)
            payout_generation = int(self._payout_state_generation)
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

        deadline = timeout if timeout > 0 else None
        startup_age = max(0.0, now - getattr(self, "started_monotonic", now))
        startup_grace = float(
            getattr(
                self,
                "mining_health_startup_grace_seconds",
                DEFAULT_PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS,
            )
        )
        in_startup_grace = startup_age < startup_grace
        initial_job_starved = bool(
            deadline is not None
            and oldest_genuine_initial_age >= deadline
            and genuinely_pending_initial_clients
        )
        current_tip_coverage_stalled = bool(
            deadline is not None
            and poor_coverage
            and delivery_failure_age >= deadline
        )
        no_delivery_progress = bool(
            initial_job_starved or current_tip_coverage_stalled
        )
        stale_unknown = int(
            getattr(self, "rejection_counts_by_reason", {}).get(
                PRISM_REJECTION_STALE_JOB,
                0,
            )
        ) + int(
            getattr(self, "rejection_counts_by_reason", {}).get(
                PRISM_REJECTION_UNKNOWN_JOB,
                0,
            )
        )
        submitted = int(getattr(self, "submitted_share_count", 0))
        reject_storm = (
            poor_coverage
            and submitted > 0
            and stale_unknown / submitted >= 0.95
        )
        persistent_overload = deadline is not None and overload_age >= deadline
        unhealthy_reasons: list[str] = []
        if not in_startup_grace:
            if no_delivery_progress:
                unhealthy_reasons.append("initial-delivery-stalled")
            if pending_saturated and persistent_overload:
                unhealthy_reasons.append("pending-initial-jobs-saturated")
            if cap_saturated and poor_coverage and persistent_overload:
                unhealthy_reasons.append("connection-capacity-saturated")
            if reject_storm:
                unhealthy_reasons.append("stale-unknown-rejection-storm")
        mining_ready = not unhealthy_reasons
        executor = getattr(self, "_tip_refresh_executor", None)
        queue_depth, active_workers = executor.stats() if executor is not None else (0, 0)
        return {
            "mining_ready": mining_ready,
            "mining_delivery_healthy": mining_ready,
            "mining_health_startup_grace": in_startup_grace,
            "active_connections": active,
            "connection_capacity": connection_limit,
            "peak_active_connections": peak,
            "subscribed_connections": subscribed,
            "authorized_connections": authorized,
            "pending_initial_jobs": pending,
            "pending_initial_job_capacity": pending_limit,
            "oldest_pending_initial_job_age_seconds": round(oldest_age, 3),
            "oldest_genuinely_pending_initial_job_age_seconds": round(
                oldest_genuine_initial_age,
                3,
            ),
            "clients_with_current_tip_jobs": current,
            "current_tip_job_coverage": round(coverage, 6),
            "clients_with_semantically_current_work": semantic_current,
            "semantic_current_work_ratio": round(semantic_coverage, 6),
            "current_tip_coverage_gap_age_seconds": round(
                delivery_failure_age,
                3,
            ),
            "connection_capacity_saturated": cap_saturated,
            "pending_initial_jobs_saturated": pending_saturated,
            "initial_delivery_stalled": no_delivery_progress,
            "overload": bool(overload_now or reject_storm),
            "overload_age_seconds": round(overload_age, 3),
            "unhealthy_reasons": unhealthy_reasons,
            "initial_job_queue_rejections": queue_rejections,
            "initial_job_timeout_disconnects": timeout_disconnects,
            "initial_job_cancelled_tasks": cancelled,
            "initial_job_coalesced_tasks": coalesced,
            "initial_job_queue_capacity_reclaimed": queue_capacity_reclaimed,
            "handler_threads": handlers,
            "delivery_executor_queue_depth": queue_depth,
            "delivery_executor_active_workers": active_workers,
            # Compatibility aliases and preparation visibility introduced by
            # the prewarm work. These retain the original bounded-pipeline
            # names above for existing dashboards.
            "subscribed_clients": subscribed,
            "authorized_clients": authorized,
            "clients_with_no_active_job": sum(
                1 for client in authorized_clients if client.active_job is None
            ),
            "clients_without_current_tip_job": authorized - current,
            "clients_with_current_tip_job": current,
            "clients_pending_initial_job": pending,
            "current_tip_job_coverage_ratio": coverage,
            # Compatibility alias: this now reports only genuine first-job
            # starvation. Current-tip fanout lag has its own age above.
            "oldest_initial_job_pending_seconds": round(
                oldest_genuine_initial_age,
                3,
            ),
            "job_preparation_pending": preparation_pending,
            "current_observed_tip": current_tip,
            "prepared_bundle_current": prepared_current,
            "prepared_bundle_tip": (
                prepared_snapshot.bestblockhash
                if prepared_snapshot is not None
                else None
            ),
            "prepared_bundle_template_generation": (
                prepared_bundle.template_generation
                if prepared_bundle is not None
                else None
            ),
            "prepared_bundle_payout_generation": (
                prepared_bundle.payout_state_generation
                if prepared_bundle is not None
                else None
            ),
        }

    def health_payload(self) -> dict[str, object]:
        accepted_share_count, ready_miner_count = self.accepted_share_stats()
        mining = self.mining_delivery_snapshot()
        payload = {
            "ok": bool(mining["mining_ready"]),
            "schema": "qbit.prism.audit-health.v1",
            "ledger_backend": self.ledger.backend_name,
            "accepted_share_count": accepted_share_count,
            "ready_miner_count": ready_miner_count,
            "accepted_block": self.accepted_block_count > 0,
            "accepted_block_count": self.accepted_block_count,
            "max_blocks": self.max_blocks,
            **mining,
        }
        return self._apply_progress_health(payload, self.progress_health_snapshot())

    def refresh_health_snapshot(self) -> dict[str, object]:
        payload = self.health_payload()
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            self._health_snapshot = payload
            self._health_snapshot_monotonic = time.monotonic()
        self._record_startup_phase_once("health_snapshot_warm")
        return payload

    def cached_health_payload(self) -> tuple[int, dict[str, object]]:
        """Health response served from the background snapshot.

        The HTTP handler must never run ledger queries synchronously: under
        job-build load those starve behind the GIL and the ledger lock, health
        checks time out, and the container is flagged unhealthy exactly when
        it is busiest. A snapshot older than the staleness budget flips the
        endpoint to 503 so a genuinely wedged ledger still surfaces.
        """
        self._ensure_job_cache_state()
        refresh_seconds = getattr(
            self, "health_refresh_seconds", DEFAULT_PRISM_HEALTH_REFRESH_SECONDS
        )
        with self._job_cache_lock:
            snapshot = self._health_snapshot
            snapshot_monotonic = self._health_snapshot_monotonic
            loop_running = self._health_refresh_loop_running
        if snapshot is None or snapshot_monotonic is None:
            if not loop_running:
                # No refresher (tests, or audit HTTP without serve()): compute
                # inline like the legacy endpoint did.
                payload = self.refresh_health_snapshot()
                return (200 if payload.get("ok") else 503), payload
            payload = self._apply_progress_health(
                {
                    "ok": False,
                    "schema": "qbit.prism.audit-health.v1",
                    "state": "starting",
                    "error": "health snapshot warm-up has not completed yet",
                },
                self.progress_health_snapshot(),
            )
            payload["ok"] = False
            return 503, payload
        age_seconds = time.monotonic() - snapshot_monotonic
        stale_after = max(3 * refresh_seconds, 15.0)
        if age_seconds > stale_after:
            payload = self._apply_progress_health(
                {
                    "ok": False,
                    "schema": "qbit.prism.audit-health.v1",
                    "error": "health snapshot is stale",
                    "snapshot_age_seconds": round(age_seconds, 3),
                },
                self.progress_health_snapshot(),
            )
            payload["ok"] = False
            return 503, payload
        # Ledger-backed fields stay cached, but progress state is an in-memory
        # monotonic snapshot and must be overlaid on every request. Otherwise a
        # cached ok=true response can mask a known failed refresh for another
        # full cache cycle (the production incident this endpoint must expose).
        payload = self._apply_progress_health(
            snapshot,
            self.progress_health_snapshot(),
        )
        payload["snapshot_age_seconds"] = round(age_seconds, 3)
        return (200 if payload.get("ok") else 503), payload

    def health_snapshot_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.refresh_health_snapshot()
            except Exception:
                with self._job_cache_lock:
                    self.health_snapshot_refresh_failure_count += 1
                print("prism coordinator: health snapshot refresh failed", flush=True)
                traceback.print_exc()
            if self.stop_event.wait(
                getattr(self, "health_refresh_seconds", DEFAULT_PRISM_HEALTH_REFRESH_SECONDS)
            ):
                break

    def start_health_snapshot_refresher(self) -> None:
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            if self._health_refresh_loop_running:
                return
            self._health_refresh_loop_running = True
        # The first refresh seeds the exact accepted-share aggregate, which
        # can take minutes on a grown ledger. It runs inside the background
        # loop so the audit listener bind path never blocks on it; until it
        # completes, cached_health_payload reports an explicit starting
        # state (issue #188 fix 4). The running flag above is set before the
        # registry can start the thread, preserving the #120
        # mark-before-listener-dispatch contract for start_audit_server.
        self._start_background_service("health_snapshot_refresher")

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

    def coordinator_lock_metrics_lines(self) -> list[str]:
        snapshot = getattr(self.lock, "contention_snapshot", None)
        if callable(snapshot):
            contention_count, wait_sum, wait_max = snapshot()
        else:
            contention_count, wait_sum, wait_max = 0, 0.0, 0.0
        return [
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

    def _observe_block_submit_seconds(self, elapsed_seconds: float) -> None:
        self._ensure_job_cache_state()
        with self._block_submit_metrics_lock:
            histogram = self.block_submit_seconds_histogram
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + max(
                0.0, elapsed_seconds
            )
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def block_submitter_metrics_lines(self) -> list[str]:
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
        self._ensure_block_submitter_retry_state()
        self._ensure_job_cache_state()
        now = time.monotonic()
        with self._block_submitter_retry_state_lock:
            deadline = self._block_submitter_backoff_deadline_monotonic
            backoff_active = deadline is not None
            backoff_remaining = (
                max(0.0, float(deadline) - now) if deadline is not None else 0.0
            )
            backoff_delay = float(self._block_submitter_backoff_delay_seconds)
        with self._block_submit_metrics_lock:
            submit_buckets = dict(self.block_submit_seconds_histogram["buckets"])
            submit_sum = float(self.block_submit_seconds_histogram["sum"])
            submit_count = int(self.block_submit_seconds_histogram["count"])
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
            self.block_ledger_call_class_metrics().items()
        ):
            label = self.prometheus_label_value(call_class)
            lines.extend(
                [
                    f'qbit_prism_block_ledger_calls_total{{call_class="{label}"}} {int(stats["calls_total"])}',
                    f'qbit_prism_block_ledger_call_timeouts_total{{call_class="{label}"}} {int(stats["timeouts_total"])}',
                    f'qbit_prism_block_ledger_call_budget_seconds{{call_class="{label}"}} {float(stats["last_budget_seconds"]):.6f}',
                    f'qbit_prism_block_ledger_call_last_duration_seconds{{call_class="{label}"}} {float(stats["last_duration_seconds"]):.6f}',
                    f'qbit_prism_block_ledger_call_max_duration_seconds{{call_class="{label}"}} {float(stats["max_duration_seconds"]):.6f}',
                ]
            )
        unresolved_ages = self.accepted_parent_unresolved_ages_seconds()
        oldest_unresolved = max(unresolved_ages) if unresolved_ages else -1.0
        with self.lock:
            preview_wait_timeouts = int(
                getattr(self, "_accepted_parent_preview_wait_timeouts", 0)
            )
        lines.extend(
            [
                "# HELP qbit_prism_accepted_parent_unresolved_transitions Landed accepted-block transitions whose durable bookkeeping is unresolved.",
                "# TYPE qbit_prism_accepted_parent_unresolved_transitions gauge",
                f"qbit_prism_accepted_parent_unresolved_transitions {len(unresolved_ages)}",
                "# HELP qbit_prism_accepted_parent_unresolved_oldest_seconds Age of the oldest unresolved accepted-parent transition, or -1 when none.",
                "# TYPE qbit_prism_accepted_parent_unresolved_oldest_seconds gauge",
                f"qbit_prism_accepted_parent_unresolved_oldest_seconds {oldest_unresolved:.6f}",
                "# HELP qbit_prism_accepted_parent_preview_wait_timeouts_total Child job builds that timed out waiting for an accepted-parent payout preview.",
                "# TYPE qbit_prism_accepted_parent_preview_wait_timeouts_total counter",
                f"qbit_prism_accepted_parent_preview_wait_timeouts_total {preview_wait_timeouts}",
            ]
        )
        prior_stats_fn = getattr(self.ledger, "prior_balances_read_stats", None)
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
        startup_phases = self.startup_phase_seconds()
        if startup_phases:
            lines.extend(
                [
                    "# HELP qbit_prism_startup_phase_seconds Seconds from serve() start to each startup phase, recorded once.",
                    "# TYPE qbit_prism_startup_phase_seconds gauge",
                ]
            )
            for phase, seconds in sorted(startup_phases.items()):
                label = self.prometheus_label_value(phase)
                lines.append(
                    f'qbit_prism_startup_phase_seconds{{phase="{label}"}} {float(seconds):.6f}'
                )
        return lines

    def _accepted_stats_reconcile_metric_lines(self) -> list[str]:
        """Surface reconcile liveness now that failures no longer raise.

        Serving maintained counters means a failing or wedged reconcile is
        invisible to callers, so its age and failure count must be visible
        to scrapes for alerting instead.
        """
        status_fn = getattr(self.ledger, "accepted_stats_reconcile_status", None)
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

    def metrics_payload(self) -> str:
        ledger_metrics = self.ledger.metrics()
        audit_metrics = self.audit_artifact_metrics()
        mining_metrics = self.mining_delivery_snapshot()
        process_rss_bytes, process_open_fds = self.process_resource_metrics()
        accepted_share_count = self.accepted_share_stats()[0]
        elapsed = max(0.001, time.monotonic() - self.started_monotonic)
        shares_per_second = accepted_share_count / elapsed
        self._ensure_share_hot_path_state()
        with self._share_accounting_lock:
            submitted_share_count = int(getattr(self, "submitted_share_count", 0))
            stale_share_count = int(getattr(self, "stale_share_count", 0))
            duplicate_share_count = int(getattr(self, "duplicate_share_count", 0))
            low_difficulty_share_count = int(
                getattr(self, "low_difficulty_share_count", 0)
            )
            collection_block_submission_count = int(
                getattr(self, "collection_block_submission_count", 0)
            )
            rejection_counts = dict(
                getattr(self, "rejection_counts_by_reason", {})
            )
            grace_credited_share_count = int(
                getattr(self, "grace_credited_share_count", 0)
            )
            block_solves_dropped_counts = dict(
                getattr(
                    self,
                    "block_solves_dropped_counts",
                    {"stale_grace": 0},
                )
            )
        stale_percent = 0.0
        if submitted_share_count > 0:
            stale_percent = (stale_share_count / submitted_share_count) * 100.0
        idle_retarget_count = int(getattr(self, "idle_retarget_count", 0))
        with self.lock:
            self._ensure_connection_capacity_state()
            active_connection_count = len(self.clients)
            connection_limit_rejection_counts = dict(
                self.connection_limit_rejection_counts
            )
            accept_resource_exhaustion_count = int(
                getattr(self, "accept_resource_exhaustion_count", 0)
            )
            connection_setup_failure_count = int(
                getattr(self, "connection_setup_failure_count", 0)
            )
            self._ensure_evicted_job_state()
            self.prune_evicted_job_graveyard(force=False)
            same_tip_context_count = len(self.evicted_same_tip_job_ids)
            evicted_job_context_counts = {
                "same_tip": same_tip_context_count,
                "stale_grace": len(self.evicted_job_graveyard) - same_tip_context_count,
            }
            evicted_job_submit_counts = dict(self.evicted_job_submit_counts)
            evicted_job_expiration_counts = dict(self.evicted_job_expiration_counts)
            evicted_job_capacity_eviction_counts = dict(
                self.evicted_job_capacity_eviction_counts
            )
            stale_job_abandon_counts = dict(
                getattr(
                    self,
                    "stale_job_abandon_counts",
                    {
                        abandon_class: 0
                        for abandon_class in PRISM_STALE_JOB_ABANDON_CLASSES
                    },
                )
            )
        self._ensure_worker_metrics_state()
        with self.worker_metrics_lock:
            worker_share_counts = {
                label: dict(counts)
                for label, counts in self.worker_share_counts.items()
            }
            worker_rejection_counts = dict(self.worker_rejection_counts)
        coinbase_weight_headroom = 2_000_000
        latest_coinbase_size_bytes = getattr(self, "latest_coinbase_size_bytes", None)
        if latest_coinbase_size_bytes is not None:
            coinbase_weight_headroom = 2_000_000 - int(latest_coinbase_size_bytes)
        ctv_pending = 0
        ctv_broadcastable = 0
        ctv_failed = 0
        pending_ctv_fanouts = getattr(self.ledger, "pending_ctv_fanout_statuses", None)
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
            blockchain_info = self.rpc.call("getblockchaininfo")
            if isinstance(blockchain_info, dict) and blockchain_info.get("initialblockdownload"):
                ibd = 1
        except Exception:
            ibd = -1
        try:
            network_info = self.rpc.call("getnetworkinfo")
            if isinstance(network_info, dict):
                peers = int(network_info.get("connections", 0))
        except Exception:
            peers = -1
        lines = [
            "# HELP qbit_prism_accepted_shares_total Accepted shares recorded by the canonical PRISM ledger.",
            "# TYPE qbit_prism_accepted_shares_total counter",
            f"qbit_prism_accepted_shares_total {accepted_share_count}",
            *self._accepted_stats_reconcile_metric_lines(),
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
                f'qbit_prism_worker_submitted_shares_total{{worker="{self.prometheus_label_value(label)}"}} {int(counts.get("submitted", 0))}'
                for label, counts in sorted(worker_share_counts.items())
            ],
            "# HELP qbit_prism_worker_accepted_shares_total Accepted shares by bounded worker label.",
            "# TYPE qbit_prism_worker_accepted_shares_total counter",
            *[
                f'qbit_prism_worker_accepted_shares_total{{worker="{self.prometheus_label_value(label)}"}} {int(counts.get("accepted", 0))}'
                for label, counts in sorted(worker_share_counts.items())
            ],
            "# HELP qbit_prism_worker_grace_credited_shares_total Stale-grace credited shares by bounded worker label.",
            "# TYPE qbit_prism_worker_grace_credited_shares_total counter",
            *[
                f'qbit_prism_worker_grace_credited_shares_total{{worker="{self.prometheus_label_value(label)}"}} {int(counts.get("grace", 0))}'
                for label, counts in sorted(worker_share_counts.items())
            ],
            "# HELP qbit_prism_worker_rejections_total PRISM share or block rejections by bounded worker label and reason ID.",
            "# TYPE qbit_prism_worker_rejections_total counter",
            *[
                f'qbit_prism_worker_rejections_total{{worker="{self.prometheus_label_value(label)}",reason_id="{reason}"}} {int(count)}'
                for (label, reason), count in sorted(worker_rejection_counts.items())
            ],
            "# HELP qbit_prism_job_build_failures_total Job builds skipped after a template/coinbase error without dropping the client.",
            "# TYPE qbit_prism_job_build_failures_total counter",
            f"qbit_prism_job_build_failures_total {self.job_build_failure_count}",
            "# HELP qbit_prism_block_candidates_dropped_total Legacy counter; durable candidate outbox rows are never dropped on queue overflow.",
            "# TYPE qbit_prism_block_candidates_dropped_total counter",
            f"qbit_prism_block_candidates_dropped_total {int(getattr(self, 'block_candidates_dropped', 0))}",
            "# HELP qbit_prism_block_candidate_wakeups_coalesced_total Candidate queue wakeups coalesced while the durable outbox retained the work.",
            "# TYPE qbit_prism_block_candidate_wakeups_coalesced_total counter",
            f"qbit_prism_block_candidate_wakeups_coalesced_total {int(getattr(self, 'block_candidate_wakeups_coalesced', 0))}",
            "# HELP qbit_prism_block_candidate_retries_total Transient candidate outcomes retained for durable retry.",
            "# TYPE qbit_prism_block_candidate_retries_total counter",
            f"qbit_prism_block_candidate_retries_total {int(getattr(self, 'block_candidate_retry_count', 0))}",
            "# HELP qbit_prism_block_candidate_accept_pending_defers_total Terminal abandonments refused because the candidate is (or was recently observed as) an active chain block; the candidate retries until its accepted success tail finalizes it as submitted.",
            "# TYPE qbit_prism_block_candidate_accept_pending_defers_total counter",
            f"qbit_prism_block_candidate_accept_pending_defers_total {int(getattr(self, 'block_candidate_accept_pending_defer_count', 0))}",
            "# HELP qbit_prism_block_candidate_poisoned_total Invalid durable candidate intents quarantined from replay.",
            "# TYPE qbit_prism_block_candidate_poisoned_total counter",
            f"qbit_prism_block_candidate_poisoned_total {int(getattr(self, 'block_candidate_poisoned_count', 0))}",
            "# HELP qbit_prism_block_candidates_abandoned_total Block candidates that did not land (lost tip race or failed submit), by reason. Not share rejections: the underlying share was accepted.",
            "# TYPE qbit_prism_block_candidates_abandoned_total counter",
            *[
                f'qbit_prism_block_candidates_abandoned_total{{reason_id="{reason}"}} {int(count)}'
                for reason, count in sorted(getattr(self, "block_candidate_abandoned_counts", {}).items())
            ],
            "# HELP qbit_prism_stale_job_abandons_total Terminal stale-job block candidate abandonments by bounded cause.",
            "# TYPE qbit_prism_stale_job_abandons_total counter",
            *[
                f'qbit_prism_stale_job_abandons_total{{class="{abandon_class}"}} {int(stale_job_abandon_counts.get(abandon_class, 0))}'
                for abandon_class in PRISM_STALE_JOB_ABANDON_CLASSES
            ],
            "# HELP qbit_prism_share_append_queue_depth Accepted shares waiting on the ledger writer thread.",
            "# TYPE qbit_prism_share_append_queue_depth gauge",
            f"qbit_prism_share_append_queue_depth {self.share_append_queue.qsize() if getattr(self, 'share_append_queue', None) is not None else 0}",
            "# HELP qbit_prism_share_append_failures_total Shares in group commits that failed before acknowledgement.",
            "# TYPE qbit_prism_share_append_failures_total counter",
            f"qbit_prism_share_append_failures_total {int(getattr(self, 'share_append_failure_count', 0))}",
            "# HELP qbit_prism_shares_recovered_to_disk_total Legacy pre-commit-ACK shares written to the upgrade recovery file.",
            "# TYPE qbit_prism_shares_recovered_to_disk_total counter",
            f"qbit_prism_shares_recovered_to_disk_total {int(getattr(self, 'shares_recovered_to_disk', 0))}",
            "# HELP qbit_prism_shares_replayed_total Recovery-file shares replayed into the ledger at startup.",
            "# TYPE qbit_prism_shares_replayed_total counter",
            f"qbit_prism_shares_replayed_total {int(getattr(self, 'shares_replayed', 0))}",
            "# HELP qbit_prism_share_replay_conflicts_total Recovery-file rows quarantined because the durable row disagrees with the journal payload.",
            "# TYPE qbit_prism_share_replay_conflicts_total counter",
            f"qbit_prism_share_replay_conflicts_total {int(getattr(self, 'share_replay_conflicts', 0))}",
            "# HELP qbit_prism_tip_refresh_jobs_total Client jobs refreshed after qbit tip/template changes.",
            "# TYPE qbit_prism_tip_refresh_jobs_total counter",
            f"qbit_prism_tip_refresh_jobs_total {self.tip_refresh_job_count}",
            "# HELP qbit_prism_active_job_contexts Current retained PRISM job contexts.",
            "# TYPE qbit_prism_active_job_contexts gauge",
            f"qbit_prism_active_job_contexts {len(getattr(self, 'jobs', {}))}",
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
            f"qbit_prism_post_accept_refresh_failures_total {self.post_accept_refresh_failure_count}",
            "# HELP qbit_prism_reorg_inactive_blocks_total PRISM pool blocks quarantined after leaving the active chain.",
            "# TYPE qbit_prism_reorg_inactive_blocks_total counter",
            f"qbit_prism_reorg_inactive_blocks_total {self.reorg_inactive_block_count}",
            "# HELP qbit_prism_reorg_reactivated_blocks_total Quarantined PRISM pool blocks restored after returning to the active chain.",
            "# TYPE qbit_prism_reorg_reactivated_blocks_total counter",
            f"qbit_prism_reorg_reactivated_blocks_total {self.reorg_reactivated_block_count}",
            "# HELP qbit_prism_reorg_reconcile_skips_total Reorg reconciliation passes skipped because qbitd chain view was not trusted.",
            "# TYPE qbit_prism_reorg_reconcile_skips_total counter",
            f"qbit_prism_reorg_reconcile_skips_total {self.reorg_reconcile_skip_count}",
            "# HELP qbit_prism_reorg_reconcile_errors_total Reorg reconciliation errors that prevented ordered job issuance.",
            "# TYPE qbit_prism_reorg_reconcile_errors_total counter",
            f"qbit_prism_reorg_reconcile_errors_total {self.reorg_reconcile_error_count}",
            "# HELP qbit_prism_reorg_reconcile_lookups_total Reconcile demand by caller path and the source that satisfied it.",
            "# TYPE qbit_prism_reorg_reconcile_lookups_total counter",
            *[
                f'qbit_prism_reorg_reconcile_lookups_total{{path="{path}",source="{source}"}} '
                f"{int(getattr(self, 'reorg_reconcile_lookup_counts', {}).get((path, source), 0))}"
                for path in PRISM_REORG_RECONCILE_LOOKUP_PATHS
                for source in PRISM_REORG_RECONCILE_LOOKUP_SOURCES
            ],
            "# HELP qbit_prism_matured_payouts_total Payout entries marked mature by the coordinator tip reconciliation path.",
            "# TYPE qbit_prism_matured_payouts_total counter",
            f"qbit_prism_matured_payouts_total {self.matured_payout_count}",
            "# HELP qbit_prism_vardiff_idle_retargets_total Vardiff retargets triggered by the idle zero-accepted-share sweep.",
            "# TYPE qbit_prism_vardiff_idle_retargets_total counter",
            f"qbit_prism_vardiff_idle_retargets_total {idle_retarget_count}",
            "# HELP qbit_prism_shares_per_second Accepted shares per second since coordinator start.",
            "# TYPE qbit_prism_shares_per_second gauge",
            f"qbit_prism_shares_per_second {shares_per_second:.12g}",
            "# HELP qbit_prism_stale_share_percent Percent of submitted shares classified stale.",
            "# TYPE qbit_prism_stale_share_percent gauge",
            f"qbit_prism_stale_share_percent {stale_percent:.12g}",
            "# HELP qbit_prism_blocks_accepted_total Blocks accepted through the PRISM coordinator.",
            "# TYPE qbit_prism_blocks_accepted_total counter",
            f"qbit_prism_blocks_accepted_total {self.accepted_block_count}",
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
            f"qbit_prism_vardiff_enabled {1 if self.vardiff_config.enabled else 0}",
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
        lines.extend(self.shutdown_metrics_lines())
        lines.extend(self.coordinator_lock_metrics_lines())
        lines.extend(self.block_submitter_metrics_lines())
        lines.extend(self.share_ack_metrics_lines())
        lines.extend(self.ctv_fanout_broadcaster_metrics_lines())
        lines.extend(self.vardiff_idle_metrics_lines())
        lines.extend(self.job_build_metrics_lines())
        lines.extend(self.tip_refresh_metrics_lines())
        lines.extend(self.payout_state_metrics_lines())
        lines.extend(self.initial_delivery_metrics_lines())
        lines.extend(self.progress_health_metrics_lines())
        return "\n".join(lines) + "\n"

    def shutdown_metrics_lines(self) -> list[str]:
        snapshot = self._ensure_shutdown_controller().snapshot()
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
                f'qbit_prism_shutdown_writer_operations{{component="{self.prometheus_label_value(str(component))}"}} {int(count)}'
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

    def audit_artifact_metrics(self) -> dict[str, dict[str, int] | int]:
        return self._ensure_audit_artifact_store().metrics_snapshot()

    @staticmethod
    def audit_artifact_kind(name: str) -> str:
        return AuditArtifactStore.artifact_kind(name)

    def ctv_fanout_broadcaster_metrics_lines(self) -> list[str]:
        return self._ensure_ctv_runtime().metrics_lines()

    def initial_delivery_metrics_lines(self) -> list[str]:
        self._ensure_initial_job_state()
        mining = self.mining_delivery_snapshot()
        with self.lock:
            counts = {
                "sent": self.initial_job_sent_count,
                "cancelled": self.initial_job_cancelled_count,
                "coalesced": self.initial_job_coalesced_count,
                "failed": self.initial_job_failed_count,
                "superseded": self.initial_job_superseded_count,
            }
            latency_sum = self.initial_job_delivery_latency_seconds_sum
            latency_count = self.initial_job_delivery_latency_count
            queue_capacity_reclaimed = (
                self.initial_job_queue_capacity_reclaimed_count
            )
        executor = getattr(self, "_initial_job_executor", None)
        queued, slots = executor.stats() if executor is not None else (0, 0)
        configured_workers = int(
            getattr(
                self,
                "initial_job_max_workers",
                DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
            )
        )
        with self._bundle_preparation_lock:
            build_counts = dict(self.shared_bundle_build_counts)
            preparation_sum = self.shared_bundle_preparation_seconds_sum
            preparation_count = self.shared_bundle_preparation_count
            waiters = self.shared_bundle_preparation_waiters
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

    def vardiff_idle_metrics_lines(self) -> list[str]:
        self._ensure_vardiff_idle_state()
        with self._vardiff_idle_lock:
            sweep = {
                "buckets": dict(self.vardiff_idle_sweep_histogram["buckets"]),
                "sum": float(self.vardiff_idle_sweep_histogram["sum"]),
                "count": int(self.vardiff_idle_sweep_histogram["count"]),
            }
            task = {
                "buckets": dict(self.vardiff_idle_task_histogram["buckets"]),
                "sum": float(self.vardiff_idle_task_histogram["sum"]),
                "count": int(self.vardiff_idle_task_histogram["count"]),
            }
            inspected = self.vardiff_idle_clients_inspected
            skip_counts = dict(self.vardiff_idle_skip_counts)
            queue_depth = self.vardiff_idle_queue_depth
            inflight = self.vardiff_idle_inflight
            failures = self.vardiff_idle_task_failures

        lines = [
            "# HELP qbit_prism_vardiff_idle_clients_inspected_total Clients inspected by bounded vardiff idle sweeps.",
            "# TYPE qbit_prism_vardiff_idle_clients_inspected_total counter",
            f"qbit_prism_vardiff_idle_clients_inspected_total {inspected}",
            "# HELP qbit_prism_vardiff_idle_skips_total Idle retargets skipped by a bounded reason.",
            "# TYPE qbit_prism_vardiff_idle_skips_total counter",
            *[
                f'qbit_prism_vardiff_idle_skips_total{{reason="{reason}"}} {int(skip_counts.get(reason, 0))}'
                for reason in PRISM_VARDIFF_IDLE_SKIP_REASONS
            ],
            "# HELP qbit_prism_vardiff_idle_queue_depth Cache-only idle retarget tasks waiting for a dedicated worker.",
            "# TYPE qbit_prism_vardiff_idle_queue_depth gauge",
            f"qbit_prism_vardiff_idle_queue_depth {queue_depth}",
            "# HELP qbit_prism_vardiff_idle_inflight Cache-only idle retarget tasks currently running.",
            "# TYPE qbit_prism_vardiff_idle_inflight gauge",
            f"qbit_prism_vardiff_idle_inflight {inflight}",
            "# HELP qbit_prism_vardiff_idle_task_failures_total Idle retarget tasks that failed during cached delivery.",
            "# TYPE qbit_prism_vardiff_idle_task_failures_total counter",
            f"qbit_prism_vardiff_idle_task_failures_total {failures}",
        ]
        for metric_name, description, histogram in (
            (
                "qbit_prism_vardiff_idle_sweep_seconds",
                "Wall time of one bounded vardiff idle sweep.",
                sweep,
            ),
            (
                "qbit_prism_vardiff_idle_retarget_task_seconds",
                "Queue plus execution latency for cache-only idle retarget tasks.",
                task,
            ),
        ):
            lines.extend(
                [
                    f"# HELP {metric_name} {description}",
                    f"# TYPE {metric_name} histogram",
                    *[
                        f'{metric_name}_bucket{{le="{bucket:g}"}} {histogram["buckets"].get(bucket, 0)}'
                        for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS
                    ],
                    f'{metric_name}_bucket{{le="+Inf"}} {histogram["count"]}',
                    f'{metric_name}_sum {float(histogram["sum"]):.6f}',
                    f'{metric_name}_count {histogram["count"]}',
                ]
            )
        return lines
    def tip_refresh_metrics_lines(self) -> list[str]:
        return self._ensure_tip_refresh_service().tip_refresh_metrics_lines()

    def job_build_metrics_lines(self) -> list[str]:
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            bucket_counts = dict(self.job_build_seconds_bucket_counts)
            build_sum = self.job_build_seconds_sum
            build_count = self.job_build_count
            phase_seconds = dict(self.job_build_phase_seconds)
            hit_counts = dict(self.job_cache_hit_counts)
            miss_counts = dict(self.job_cache_miss_counts)
            health_refresh_failures = self.health_snapshot_refresh_failure_count
        with self._job_build_scheduler_lock:
            scheduler_counts = dict(self.job_build_scheduler_counts)
            priority_counts = dict(self.job_build_priority_counts)
            priority_admission_seconds = dict(
                self.job_build_priority_admission_seconds
            )
            initial_prepared_counts = dict(
                self.initial_job_prepared_work_counts
            )
            cancellation_seconds = dict(self.job_build_cancellation_seconds)
            replacement_seconds = dict(self.job_build_replacement_start_seconds)
            worker_counts = dict(self.job_build_worker_counts)
            active_builds = int(self._job_build_active is not None)
            pending_builds = int(self._job_build_pending is not None)
            priority_requests = tuple(
                request
                for request in (
                    (
                        self._job_build_active.request
                        if self._job_build_active is not None
                        else None
                    ),
                    (
                        self._job_build_retiring.request
                        if self._job_build_retiring is not None
                        else None
                    ),
                    self._job_build_pending,
                )
                if request is not None
                and not request.cancellation.is_set()
                and self._job_build_is_publication_critical(request)
            )
            priority_preparations = tuple(
                self._job_build_priority_preparations.values()
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
        lock = getattr(self, "lock", None)
        if lock is not None:
            with lock:
                connected_clients = len(getattr(self, "clients", ()))
        else:
            connected_clients = len(getattr(self, "clients", ()))
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
                    f"{int(getattr(self, 'job_build_executor_workers', DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS))}"
                ),
            ]
        )
        return lines

    def payout_state_metrics_lines(self) -> list[str]:
        return self._ensure_payout_state_service().payout_state_metrics_lines()


def make_audit_handler(coordinator: PrismCoordinator) -> type[BaseHTTPRequestHandler]:
    public_response_cache = public_api.PublicResponseCache()

    class AuditHandler(BaseHTTPRequestHandler):
        server_version = "QbitPrismAudit/0.1"

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = urllib.parse.parse_qs(parsed.query)
            try:
                if path == "/healthz":
                    cached_health = getattr(coordinator, "cached_health_payload", None)
                    if callable(cached_health):
                        status, payload = cached_health()
                        self.write_json(status, payload)
                    else:
                        self.write_json(200, coordinator.health_payload())
                    return
                if path == "/metrics":
                    self.write_text(200, coordinator.metrics_payload(), "text/plain; version=0.0.4")
                    return
                if path == "/public/v1" or path.startswith("/public/v1/"):
                    self.handle_public(path, query)
                    return
                if path == "/audit/latest":
                    payload = coordinator.latest_evidence_payload()
                    if payload is None:
                        self.write_json(404, {"error": "no PRISM evidence has been produced"})
                    else:
                        self.write_json(200, payload)
                    return
                if path in {"/owed", "/owed-balances"}:
                    self.write_json(200, coordinator.owed_balances_payload())
                    return
                if path in {"/audit/carry-forward-integrity", "/audit/ledger-integrity"}:
                    self.write_json(200, coordinator.carry_forward_integrity_payload())
                    return
                if path.startswith("/miners/") and path.endswith("/status"):
                    recipient_id = urllib.parse.unquote(path.removeprefix("/miners/").removesuffix("/status"))
                    self.write_json(200, coordinator.miner_status_payload(recipient_id))
                    return
                if path.startswith("/payouts/") and path.endswith("/status"):
                    recipient_id = urllib.parse.unquote(path.removeprefix("/payouts/").removesuffix("/status"))
                    self.write_json(200, coordinator.miner_status_payload(recipient_id))
                    return
                if path == "/audit/share-window":
                    self.handle_share_window(query)
                    return
                if path.startswith("/audit/blocks/") and path.endswith("/payouts"):
                    block_hash = path.removeprefix("/audit/blocks/").removesuffix("/payouts")
                    self.handle_block_payouts(block_hash)
                    return
                if path.startswith("/audit/blocks/") and path.endswith("/ctv-fanouts"):
                    block_hash = path.removeprefix("/audit/blocks/").removesuffix("/ctv-fanouts")
                    self.handle_block_ctv_fanouts(block_hash)
                    return
                if path.startswith("/audit/blocks/") and path.endswith("/ctv-fanout-manifest-set"):
                    block_hash = path.removeprefix("/audit/blocks/").removesuffix("/ctv-fanout-manifest-set")
                    self.handle_block_ctv_fanout_manifest_set(block_hash)
                    return
                if path == "/audit/fanouts/pending":
                    self.handle_pending_ctv_fanouts(query)
                    return
                if path.startswith("/audit/fanouts/") and path.endswith("/status"):
                    fanout_txid = path.removeprefix("/audit/fanouts/").removesuffix("/status")
                    self.handle_ctv_fanout_status(fanout_txid)
                    return
                if path.startswith("/audit/commitments/") and path.endswith("/bundle"):
                    commitment_leaf_hex = path.removeprefix("/audit/commitments/").removesuffix("/bundle")
                    self.handle_commitment_bundle(commitment_leaf_hex)
                    return
                if path.startswith("/audit/block/"):
                    block_hash = path.removeprefix("/audit/block/")
                    self.handle_block_payouts(block_hash)
                    return
                if path.startswith("/audit/blocks/") and path.endswith("/bundle"):
                    block_hash = path.removeprefix("/audit/blocks/").removesuffix("/bundle")
                    self.handle_block_bundle(block_hash)
                    return
                self.write_json(404, {"error": "unknown endpoint"})
            except public_api.PublicApiError as exc:
                self.write_json(
                    exc.status,
                    public_api.error_payload(exc.code, exc.message),
                    headers=public_api.public_error_headers(),
                )
            except ValueError as exc:
                if path == "/public/v1" or path.startswith("/public/v1/"):
                    self.write_json(
                        500,
                        public_api.error_payload("internal_error", "internal server error"),
                        headers=public_api.public_error_headers(),
                    )
                else:
                    self.write_json(400, {"error": str(exc)})
            except Exception as exc:
                if path == "/public/v1" or path.startswith("/public/v1/"):
                    self.write_json(
                        500,
                        public_api.error_payload("internal_error", "internal server error"),
                        headers=public_api.public_error_headers(),
                    )
                else:
                    self.write_json(500, {"error": str(exc)})

        def handle_public(self, path: str, query: dict[str, list[str]]) -> None:
            cache_policy = public_api.public_cache_policy(path)
            status, payload, cache_state, age_seconds = public_response_cache.get_or_compute(
                key=public_api.public_cache_key(path, query),
                ttl_seconds=cache_policy.ttl_seconds,
                compute=lambda: public_api.dispatch(coordinator, path, query),
            )
            self.write_json(
                status,
                payload,
                headers=public_api.public_cache_headers(
                    cache_policy,
                    cache_state=cache_state,
                    age_seconds=age_seconds,
                ),
            )

        def handle_share_window(self, query: dict[str, list[str]]) -> None:
            anchor_raw = self.first_query_value(query, "anchor_job_issued_at_ms", "anchor")
            difficulty_raw = self.first_query_value(query, "network_difficulty")
            if anchor_raw is None or difficulty_raw is None:
                raise ValueError("anchor_job_issued_at_ms and network_difficulty are required")
            rows = coordinator.ledger.audit_share_window(
                anchor_job_issued_at_ms=int(anchor_raw),
                network_difficulty=int(difficulty_raw),
            )
            self.write_json(
                200,
                {
                    "schema": "qbit.prism.audit-share-window.v1",
                    "ledger_backend": coordinator.ledger.backend_name,
                    "rows": rows,
                },
            )

        def handle_block_payouts(self, block_hash: str) -> None:
            block_hash = self.clean_hash(block_hash)
            rows = coordinator.ledger.audit_block_payouts(block_hash=block_hash)
            if not rows:
                self.write_json(404, {"error": "unknown PRISM block", "block_hash": block_hash})
                return
            self.write_json(
                200,
                {
                    "schema": "qbit.prism.audit-block-payouts.v1",
                    "ledger_backend": coordinator.ledger.backend_name,
                    "block_hash": block_hash,
                    "rows": rows,
                },
            )

        def handle_block_ctv_fanouts(self, block_hash: str) -> None:
            block_hash = self.clean_hash(block_hash, name="block hash")
            rows = coordinator.ledger.audit_ctv_fanouts(block_hash=block_hash)
            if not rows:
                self.write_json(404, {"error": "unknown CTV fanout block", "block_hash": block_hash})
                return
            self.write_json(
                200,
                {
                    "schema": "qbit.prism.audit-ctv-fanouts.v1",
                    "ledger_backend": coordinator.ledger.backend_name,
                    "block_hash": block_hash,
                    "rows": rows,
                },
            )

        def handle_block_ctv_fanout_manifest_set(self, block_hash: str) -> None:
            block_hash = self.clean_hash(block_hash, name="block hash")
            payload = coordinator.ledger.audit_ctv_fanout_manifest_set(block_hash=block_hash)
            if payload is None:
                self.write_json(404, {"error": "unknown CTV fanout block", "block_hash": block_hash})
                return
            self.write_json(200, payload)

        def handle_ctv_fanout_status(self, fanout_txid: str) -> None:
            fanout_txid = self.clean_hash(fanout_txid, name="fanout txid")
            payload = coordinator.ledger.ctv_fanout_status(fanout_txid=fanout_txid)
            if payload is None:
                self.write_json(404, {"error": "unknown CTV fanout", "fanout_txid": fanout_txid})
                return
            self.write_json(200, payload)

        def handle_pending_ctv_fanouts(self, query: dict[str, list[str]]) -> None:
            limit_raw = self.first_query_value(query, "limit")
            limit = int(limit_raw) if limit_raw is not None else 100
            rows = coordinator.ledger.pending_ctv_fanout_statuses(limit=limit)
            self.write_json(
                200,
                {
                    "schema": "qbit.prism.pending-ctv-fanouts.v1",
                    "ledger_backend": coordinator.ledger.backend_name,
                    "count": len(rows),
                    "rows": rows,
                },
            )

        def handle_block_bundle(self, block_hash: str) -> None:
            block_hash = self.clean_hash(block_hash, name="block hash")
            payload = coordinator.ledger.audit_bundle(block_hash=block_hash)
            if payload is None:
                self.write_json(404, {"error": "unknown PRISM block", "block_hash": block_hash})
                return
            self.write_json(200, payload)

        def handle_commitment_bundle(self, commitment_leaf_hex: str) -> None:
            commitment_leaf_hex = self.clean_hash(commitment_leaf_hex, name="audit commitment leaf")
            payload = coordinator.ledger.audit_bundle_by_commitment(commitment_leaf_hex=commitment_leaf_hex)
            if payload is None:
                self.write_json(
                    404,
                    {
                        "error": "unknown PRISM audit commitment",
                        "audit_commitment_leaf_hex": commitment_leaf_hex,
                    },
                )
                return
            self.write_json(200, payload)

        def write_json(self, status: int, payload: object, headers: dict[str, str] | None = None) -> None:
            body = json.dumps(payload, sort_keys=True).encode() + b"\n"
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # The client (typically a health checker with a short timeout)
                # hung up before the response was written; nothing to salvage.
                return

        def write_text(self, status: int, payload: str, content_type: str) -> None:
            body = payload.encode()
            try:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                return

        def log_message(self, format: str, *args: object) -> None:
            return

        @staticmethod
        def first_query_value(query: dict[str, list[str]], *keys: str) -> str | None:
            for key in keys:
                values = query.get(key)
                if values:
                    return values[0]
            return None

        @staticmethod
        def clean_hash(value: str, *, name: str = "block hash") -> str:
            value = urllib.parse.unquote(value).strip()
            if len(value) != 64 or any(char not in "0123456789abcdefABCDEF" for char in value):
                raise ValueError(f"{name} must be 64 hex characters")
            return value.lower()

    return AuditHandler


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
