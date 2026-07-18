#!/usr/bin/env python3
"""Minimal live direct qbit Stratum coordinator for PRISM regtest proof."""

from __future__ import annotations

import base64
import copy
from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import ExitStack, contextmanager
import dataclasses
import errno
from functools import wraps
import hashlib
import http.client
import json
import math
import os
import queue
import shlex
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import traceback
import urllib.parse
import urllib.request
import uuid
from types import SimpleNamespace
from dataclasses import dataclass, field, replace as dataclass_replace
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterator

import sys

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.auxpow import stratum_codec, vardiff
from lab.prism import direct_stratum, public_api
from lab.prism.prism_tools import prism_tool_command
from lab.prism.ctv_broadcaster import CtvFanoutBroadcaster
from lab.prism.ctv_broadcaster_daemon import (
    CtvFanoutBroadcastDaemon,
    CtvFanoutChunkResult,
    CtvFanoutDaemonResult,
    MAX_CTV_FANOUT_BROADCASTER_CHUNK_SIZE,
)
from lab.prism.share_ledger import (
    DEFAULT_AUDIT_SHARE_SEGMENT_SIZE,
    DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
    DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
    PendingShare,
    PsqlShareLedger,
    SingleWriterShareLedger,
    sha256_json_hex,
)

DEFAULT_P2MR_SPEND_INPUT_BYTES = 3_680
DEFAULT_MIN_OUTPUT_FEERATE_SATS_PER_BYTE = 1
DEFAULT_MIN_OUTPUT_SAFETY_MULTIPLIER = 4
DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS = (
    "tq1zlsq9dpxz8mennhdpr9nf9s0f2tjtq6gxs9m84k6xglhkfp92q2zszzu4m3"
)
DEFAULT_PRISM_COINBASE_TAG = "/PRISM/"
MAX_PRISM_COINBASE_TAG_BYTES = 40
DEFAULT_DIRECT_COINBASE_PAYOUT_FLOOR_SATS = 10_485_760
DEFAULT_MAX_COINBASE_SETTLEMENT_OUTPUTS = 16
DEFAULT_MAX_DIRECT_COINBASE_OUTPUTS = 12
DEFAULT_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION = 1_000
DEFAULT_CTV_FANOUT_FEE_PREMIUM_BPS = 12_000
TESTNET_QBIT_CHAINS = {"testnet", "testnet3", "testnet4", "signet"}
DEFAULT_PRISM_BLOCKPOLL_SECONDS = 2.0
DEFAULT_PRISM_BLOCKWAIT_TIMEOUT_SECONDS = 5.0
DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS = 10.0
DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS = 60.0
MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES = 128
DEFAULT_PRISM_CTV_BROADCASTER_CHUNK_SIZE = 5
DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS = 5.0
DEFAULT_PRISM_HEALTH_REFRESH_SECONDS = 5.0
DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS = 20.0
DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS = 384
DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME = 0
DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS = 128
DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS = 30.0
DEFAULT_PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS = 30.0
DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS = 1.0
DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES = 4_096
DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS = 3_600.0
DEFAULT_PRISM_STALE_GRACE_SECONDS = 3.0
# How old the poller/blockwait-observed tip may be before mining.submit stops
# trusting it and falls back to a live getbestblockhash per share. Healthy
# coordinators re-observe the tip every blockpoll interval, so the fallback
# only engages when tip observation is genuinely failing (fail-safe, never
# fail-open on a frozen snapshot).
DEFAULT_PRISM_SUBMIT_TIP_MAX_AGE_SECONDS = 10.0
DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS = 30.0
DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION = 64
DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS = 1.0
DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS = 16
PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS = 0.05
DEFAULT_PRISM_VARDIFF_IDLE_SWEEP_SECONDS = 15.0
DEFAULT_PRISM_WORKER_METRICS_LIMIT = 100
MAX_ACTIVE_PRISM_JOBS_PER_CLIENT = 16
# Block candidates queue to a dedicated submitter thread so the miner's share
# ack never waits on audit/submitblock after the share and intent commit. The
# bound limits RAM; overflow only coalesces a wakeup because Postgres retains
# the authoritative pending candidate.
MAX_PENDING_BLOCK_CANDIDATES = 32
# Accepted shares use a small, bounded group-commit queue.  Every submitter
# waits for its batch's Postgres commit before receiving Stratum success, so
# this is a latency-smoothing bound rather than a durable backlog.
MAX_PENDING_SHARE_APPENDS = 4_096
DEFAULT_SHARE_COMMIT_BATCH_SIZE = 64
DEFAULT_SHARE_COMMIT_LINGER_MILLISECONDS = 5.0
DEFAULT_SHARE_COMMIT_TIMEOUT_SECONDS = 15.0
DEFAULT_PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS = 15.0
DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS = 120
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
PRISM_CTV_BROADCASTER_SECONDS_BUCKETS = (
    1.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
)
PRISM_CTV_BROADCASTER_CHUNK_SECONDS_BUCKETS = PRISM_JOB_BUILD_SECONDS_BUCKETS
PRISM_CTV_BROADCASTER_CHUNK_ROWS_BUCKETS = (1, 2, 5, 10, 25, 50, 100)
PRISM_TIP_REFRESH_SECONDS_BUCKETS = PRISM_JOB_BUILD_SECONDS_BUCKETS
PRISM_TIP_REFRESH_BUILD_PHASES = (
    "ledger_snapshot",
    "payout_state_derivation",
    "ctv_manifest_construction",
    "coinbase_bundle_construction",
    "signing_verification",
    "serialization_copy",
    "singleflight_wait",
)
PRISM_BUILDER_PHASE_METRICS_PREFIX = "qbit-prism-build-phase-metrics "
PRISM_PAYOUT_DELIVERY_GENERATIONS = ("current", "stale", "future")
DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES = 8
PRISM_JOB_BUILD_PHASES = (
    "reorg",
    "template",
    "merkle",
    "ledger",
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
PRISM_TIP_REFRESH_RESULTS = ("sent", "skipped", "disconnected", "failed")
PRISM_TIP_REFRESH_CANCELLATION_STAGES = (
    "executor_queue",
    "client_lock",
    "payout_gate",
)
PRISM_DELIVERY_PRIORITY_INITIAL = 0
PRISM_DELIVERY_PRIORITY_NEW_TIP = 1
PRISM_DELIVERY_PRIORITY_SAME_TIP = 2
PRISM_EVICTED_JOB_CLASSES = ("same_tip", "stale_grace")
PRISM_EVICTED_JOB_SUBMIT_OUTCOMES = ("accepted_same_tip", "credited_stale_grace")
PRISM_EVICTED_JOB_CAPACITY_SCOPES = ("connection",)
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
PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS = frozenset(
    {
        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
        PRISM_REJECTION_INTERNAL_ERROR,
        PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
        PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
    }
)
DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS = 0.25
DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS = 30.0
# Credit policies recorded on accepted ledger rows. Normal shares carry no
# policy; a policy marks a share that was credited by an explicit pool rule
# (documented in docs/prism-rejections.md) so audits can distinguish them.
PRISM_CREDIT_POLICY_STALE_GRACE = "stale-grace"
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


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise SystemExit(f"{name} is required")
    return value


def env_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def env_positive_int(name: str, default: int) -> int:
    try:
        value = env_int(name, default)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer") from exc
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


def env_positive_int_with_legacy(primary_name: str, legacy_name: str, default: int) -> int:
    if env_optional(primary_name) is not None:
        return env_positive_int(primary_name, default)
    return env_positive_int(legacy_name, default)


def env_nonnegative_int(name: str, default: int) -> int:
    try:
        value = env_int(name, default)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer") from exc
    if value < 0:
        raise SystemExit(f"{name} must be non-negative")
    return value


def env_nonnegative_int_with_legacy(primary_name: str, legacy_name: str, default: int) -> int:
    if env_optional(primary_name) is not None:
        return env_nonnegative_int(primary_name, default)
    return env_nonnegative_int(legacy_name, default)


def env_positive_float(name: str, default: float) -> float:
    try:
        value = float(env(name, str(default)))
    except ValueError as exc:
        raise SystemExit(f"{name} must be a number") from exc
    if not math.isfinite(value):
        raise SystemExit(f"{name} must be finite")
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


def env_nonnegative_float(name: str, default: float) -> float:
    try:
        value = float(env(name, str(default)))
    except ValueError as exc:
        raise SystemExit(f"{name} must be a number") from exc
    if not math.isfinite(value):
        raise SystemExit(f"{name} must be finite")
    if value < 0:
        raise SystemExit(f"{name} must be non-negative")
    return value


def env_optional_positive_int(name: str) -> int | None:
    raw = env_optional(name)
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer") from exc
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


def env_optional_positive_int_with_legacy(primary_name: str, legacy_name: str) -> int | None:
    value = env_optional_positive_int(primary_name)
    if value is not None:
        return value
    return env_optional_positive_int(legacy_name)


def env_decimal(name: str, default: str) -> Decimal:
    value = Decimal(env(name, default))
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


def env_bool(name: str, default: str) -> bool:
    return env(name, default).lower() in {"1", "true", "yes", "on"}


def env_optional_bool(name: str) -> bool | None:
    raw = env_optional(name)
    if raw is None:
        return None
    return raw.lower() in {"1", "true", "yes", "on"}


def env_optional(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None or value == "":
        return None
    return value


def production_mode() -> bool:
    return (
        env_bool("QBIT_PRODUCTION", "0")
        or env_bool("QBIT_TOOLS_PRODUCTION", "0")
        or env("QBIT_CHAIN", "regtest").lower() in {"main", "mainnet"}
    )


def validate_same_tip_job_retention_limits(
    *,
    retention_seconds: float,
    per_connection: int,
    max_connections: int,
    production: bool,
) -> None:
    if retention_seconds <= 0:
        return
    if per_connection <= 0:
        raise SystemExit(
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION must be positive "
            "when same-tip retention is enabled"
        )
    if production and max_connections <= 0:
        raise SystemExit(
            "production mode requires a positive PRISM_STRATUM_MAX_CONNECTIONS "
            "when same-tip retention is enabled"
        )


def require_production_env(name: str) -> str:
    value = env_optional(name)
    if value is None:
        raise SystemExit(f"production mode requires {name}")
    return value


def validate_prism_production_gate() -> None:
    if not production_mode():
        return

    for name in (
        "PRISM_ALLOW_MEMORY_LEDGER",
        "PRISM_ALLOW_TEST_SIGNING_SEEDS",
        "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY",
        "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN",
    ):
        if env_bool(name, "0"):
            raise SystemExit(f"production mode rejects {name}=1")

    if env("QBIT_CHAIN", "regtest").lower() in {"main", "mainnet"} and env_nonnegative_float(
        "PRISM_STRATUM_STALE_GRACE_SECONDS",
        DEFAULT_PRISM_STALE_GRACE_SECONDS,
    ) != 0:
        raise SystemExit(
            "mainnet requires PRISM_STRATUM_STALE_GRACE_SECONDS=0"
        )

    production_difficulties: dict[str, Decimal] = {}
    for name in (
        "PRISM_STRATUM_SHARE_DIFF",
        "PRISM_STRATUM_VARDIFF_MIN_DIFF",
        "PRISM_STRATUM_VARDIFF_START_DIFF",
        "PRISM_STRATUM_VARDIFF_MAX_DIFF",
    ):
        raw_value = require_production_env(name)
        if not raw_value:
            raise SystemExit(f"production mode requires an explicit {name}")
        try:
            value = Decimal(raw_value)
        except InvalidOperation as exc:
            raise SystemExit(f"{name} must be a decimal number") from exc
        if not value.is_finite() or value <= 0:
            raise SystemExit(f"{name} must be positive")
        if value == Decimal("0.000000001"):
            raise SystemExit(f"{name} cannot use the lab-only 1e-9 difficulty")
        production_difficulties[name] = value
    if (
        production_difficulties["PRISM_STRATUM_VARDIFF_MIN_DIFF"]
        > production_difficulties["PRISM_STRATUM_VARDIFF_START_DIFF"]
    ):
        raise SystemExit("production vardiff minimum exceeds its start difficulty")
    if (
        production_difficulties["PRISM_STRATUM_VARDIFF_START_DIFF"]
        > production_difficulties["PRISM_STRATUM_VARDIFF_MAX_DIFF"]
    ):
        raise SystemExit("production vardiff start exceeds its maximum difficulty")

    prism_database_url = env_optional("PRISM_DATABASE_URL")
    if prism_database_url is None and env_optional("PRISM_POSTGRES_PSQL_COMMAND") is None:
        raise SystemExit("production mode requires PRISM_DATABASE_URL or PRISM_POSTGRES_PSQL_COMMAND")
    if env_optional("PRISM_POSTGRES_PASSWORD") == "change-this":
        raise SystemExit("production mode requires a non-default PRISM_POSTGRES_PASSWORD")
    if prism_database_url is not None and "change-this" in prism_database_url:
        raise SystemExit("production mode requires a non-default PRISM_DATABASE_URL")

    require_production_env("PRISM_MANIFEST_SIGNING_SEED_HEX")
    require_production_env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX")
    require_production_env("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX")
    require_production_env("PRISM_LEDGER_WRITER_ID")
    require_production_env("PRISM_LEDGER_WRITER_EPOCH")
    require_production_env("PRISM_AUDIT_DIR")
    require_production_env("PRISM_EVIDENCE_PATH")

    if env_optional("PRISM_LEDGER_WRITER_SESSION_TOKEN") is not None:
        raise SystemExit("production mode requires managed ledger session tokens; unset PRISM_LEDGER_WRITER_SESSION_TOKEN")

    if env_nonnegative_int(
        "PRISM_STRATUM_MAX_CONNECTIONS",
        DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
    ) <= 0:
        raise SystemExit(
            "production mode requires a positive PRISM_STRATUM_MAX_CONNECTIONS"
        )
    env_positive_int(
        "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS",
        DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS,
    )
    if env_nonnegative_float(
        "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS",
        DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS,
    ) <= 0:
        raise SystemExit(
            "production mode requires a positive "
            "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS"
        )

    require_production_env("QBIT_RPC_USER")
    qbit_rpc_password = require_production_env("QBIT_RPC_PASSWORD")
    if qbit_rpc_password == "change-this":
        raise SystemExit("production mode requires a non-default QBIT_RPC_PASSWORD")

    if env("QBIT_CHAIN", "regtest").lower() in {"main", "mainnet"} and env_bool(
        "PRISM_CTV_SETTLEMENT_ENABLED", "0"
    ):
        require_production_env("PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT")
        env_positive_int("PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT", 0)

    validate_same_tip_job_retention_limits(
        retention_seconds=env_nonnegative_float(
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS",
            DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
        ),
        per_connection=env_nonnegative_int(
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION",
            DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION,
        ),
        max_connections=env_nonnegative_int(
            "PRISM_STRATUM_MAX_CONNECTIONS",
            DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
        ),
        production=True,
    )


def validate_hex(value: str, *, name: str, expected_bytes: int | None = None) -> str:
    try:
        bytes.fromhex(value)
    except ValueError as exc:
        raise SystemExit(f"{name} must be hex") from exc
    if expected_bytes is not None and len(value) != expected_bytes * 2:
        raise SystemExit(f"{name} must be {expected_bytes * 2} hex chars")
    return value.lower()


def env_seed_hex(name: str, *, test_default: str) -> str:
    value = env_optional(name)
    if value is None:
        if env_bool("PRISM_ALLOW_TEST_SIGNING_SEEDS", "0"):
            value = test_default
        else:
            raise SystemExit(f"{name} is required")
    return validate_hex(value, name=name, expected_bytes=32)


def now_ms() -> int:
    return int(time.time() * 1000)


def load_prism_vardiff_config(startup_difficulty: Decimal) -> vardiff.VardiffConfig:
    return vardiff.VardiffConfig(
        enabled=env_bool("PRISM_STRATUM_VARDIFF", "1"),
        target_share_interval_seconds=env_decimal("PRISM_STRATUM_VARDIFF_TARGET_SECONDS", "15"),
        min_difficulty=env_decimal("PRISM_STRATUM_VARDIFF_MIN_DIFF", str(startup_difficulty)),
        max_difficulty=env_decimal("PRISM_STRATUM_VARDIFF_MAX_DIFF", "1024"),
        retarget_interval_seconds=env_decimal("PRISM_STRATUM_VARDIFF_RETARGET_SECONDS", "90"),
        max_step_factor=env_decimal("PRISM_STRATUM_VARDIFF_MAX_STEP_UP", "4"),
        startup_difficulty=env_decimal("PRISM_STRATUM_VARDIFF_START_DIFF", str(startup_difficulty)),
        max_step_down_factor=env_decimal("PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN", "4"),
        ewma_alpha=env_decimal("PRISM_STRATUM_VARDIFF_EWMA_ALPHA", "0.4"),
        retarget_tolerance=env_decimal("PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE", "0.25"),
    )


@dataclass(frozen=True)
class StratumListenerProfile:
    """One stratum listener with its own difficulty policy.

    Every listener feeds the same coordinator, ledger, and settlement path;
    the profile only decides where to listen and which difficulty bounds its
    clients get.
    """

    name: str
    bind: str
    port: int
    share_difficulty: Decimal
    vardiff_config: vardiff.VardiffConfig
    heartbeat_name: str
    # Difficulty this listener never advertises below, even when the qbit
    # network target is easier. Zero means no floor: stamped jobs keep the
    # network cap (a share is never required to be harder than a block). The
    # high-diff listener sets its configured minimum because marketplace
    # verification (NiceHash-style) checks the first advertised difficulty,
    # which must hold even while network difficulty sits below the floor.
    minimum_advertised_difficulty: Decimal = Decimal("0")


DEFAULT_HIGHDIFF_DIFFICULTY = "500000"
DEFAULT_HIGHDIFF_MAX_DIFFICULTY = "4294967296"


def load_prism_highdiff_listener(
    base_bind: str,
    base_vardiff_config: vardiff.VardiffConfig,
) -> StratumListenerProfile | None:
    """Optional high-difficulty listener for rental-scale miners.

    Disabled unless PRISM_STRATUM_HIGHDIFF_PORT is set. The 500k default floor
    matches the NiceHash SHA-256 pool-verification minimum, which must hold
    from the first mining.set_difficulty a client sees.
    """

    port_value = env_optional("PRISM_STRATUM_HIGHDIFF_PORT")
    if port_value is None:
        return None
    try:
        port = int(port_value)
    except ValueError as exc:
        raise SystemExit("PRISM_STRATUM_HIGHDIFF_PORT must be an integer") from exc
    if not 0 < port < 65536:
        raise SystemExit("PRISM_STRATUM_HIGHDIFF_PORT must be a valid TCP port")
    min_difficulty = env_decimal("PRISM_STRATUM_HIGHDIFF_MIN_DIFF", DEFAULT_HIGHDIFF_DIFFICULTY)
    start_difficulty = env_decimal("PRISM_STRATUM_HIGHDIFF_START_DIFF", DEFAULT_HIGHDIFF_DIFFICULTY)
    max_difficulty = env_decimal("PRISM_STRATUM_HIGHDIFF_MAX_DIFF", DEFAULT_HIGHDIFF_MAX_DIFFICULTY)
    if min_difficulty > start_difficulty:
        raise SystemExit("PRISM_STRATUM_HIGHDIFF_MIN_DIFF exceeds PRISM_STRATUM_HIGHDIFF_START_DIFF")
    if start_difficulty > max_difficulty:
        raise SystemExit("PRISM_STRATUM_HIGHDIFF_START_DIFF exceeds PRISM_STRATUM_HIGHDIFF_MAX_DIFF")
    # The fixed difficulty (used when vardiff is disabled) tracks the start
    # difficulty unless explicitly set, and must respect the listener bounds:
    # advertising below the floor would break the marketplace verification
    # this listener exists for.
    share_value = env_optional("PRISM_STRATUM_HIGHDIFF_SHARE_DIFF")
    if share_value is None:
        share_difficulty = start_difficulty
    else:
        try:
            share_difficulty = Decimal(share_value)
        except Exception as exc:
            raise SystemExit("PRISM_STRATUM_HIGHDIFF_SHARE_DIFF must be a decimal") from exc
        if not share_difficulty.is_finite() or share_difficulty <= 0:
            raise SystemExit("PRISM_STRATUM_HIGHDIFF_SHARE_DIFF must be positive")
        if share_difficulty < min_difficulty:
            raise SystemExit("PRISM_STRATUM_HIGHDIFF_SHARE_DIFF is below PRISM_STRATUM_HIGHDIFF_MIN_DIFF")
        if share_difficulty > max_difficulty:
            raise SystemExit("PRISM_STRATUM_HIGHDIFF_SHARE_DIFF exceeds PRISM_STRATUM_HIGHDIFF_MAX_DIFF")
    try:
        config = dataclass_replace(
            base_vardiff_config,
            min_difficulty=min_difficulty,
            max_difficulty=max_difficulty,
            startup_difficulty=start_difficulty,
        )
    except ValueError as exc:
        raise SystemExit(f"invalid PRISM_STRATUM_HIGHDIFF_* difficulty bounds: {exc}") from exc
    return StratumListenerProfile(
        name="highdiff",
        # env_optional so an empty value (compose default passthrough) means
        # "inherit the default listener bind" instead of a startup failure.
        bind=env_optional("PRISM_STRATUM_HIGHDIFF_BIND") or base_bind,
        port=port,
        share_difficulty=share_difficulty,
        vardiff_config=config,
        heartbeat_name="stratum_accept_highdiff",
        minimum_advertised_difficulty=min_difficulty,
    )


def parse_stratum_password_options(password: str) -> tuple[Decimal | None, Decimal | None]:
    """Extract the pool-side d=N / md=N difficulty convention from a password.

    Unknown tokens and malformed values are ignored: miners routinely send
    junk passwords ("x") and rejecting them would break every such rig.
    Returns (requested_difficulty, requested_min_difficulty).
    """

    requested: Decimal | None = None
    requested_min: Decimal | None = None
    for token in password.split(","):
        key, separator, raw_value = token.strip().partition("=")
        if not separator:
            continue
        key = key.strip().lower()
        if key not in {"d", "md"}:
            continue
        try:
            value = Decimal(raw_value.strip())
        except Exception:
            continue
        if not value.is_finite() or value <= 0:
            continue
        if key == "d":
            requested = value
        else:
            requested_min = value
    return requested, requested_min


def default_prism_payout_policy() -> dict[str, object]:
    policy: dict[str, object] = {
        "p2mr_spend_input_bytes": env_positive_int(
            "PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES",
            DEFAULT_P2MR_SPEND_INPUT_BYTES,
        ),
        "target_feerate_sats_per_byte": env_positive_int_with_legacy(
            "PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE",
            "PRISM_PAYOUT_TARGET_FEERATE_SATS_PER_BYTE",
            DEFAULT_MIN_OUTPUT_FEERATE_SATS_PER_BYTE,
        ),
        "safety_multiplier": env_positive_int(
            "PRISM_PAYOUT_SAFETY_MULTIPLIER",
            DEFAULT_MIN_OUTPUT_SAFETY_MULTIPLIER,
        ),
    }
    min_output_sats = env_optional_positive_int_with_legacy(
        "PRISM_PAYOUT_MIN_OUTPUT_BITS",
        "PRISM_PAYOUT_MIN_OUTPUT_SATS",
    )
    if min_output_sats is not None:
        policy["min_output_sats"] = min_output_sats
    return policy


def default_prism_coinbase_tag_hex() -> str:
    tag = os.environ.get("PRISM_COINBASE_TAG", DEFAULT_PRISM_COINBASE_TAG)
    try:
        tag_bytes = tag.encode("ascii")
    except UnicodeEncodeError as exc:
        raise SystemExit("PRISM_COINBASE_TAG must be ASCII") from exc
    if len(tag_bytes) > MAX_PRISM_COINBASE_TAG_BYTES:
        raise SystemExit(
            f"PRISM_COINBASE_TAG must be at most {MAX_PRISM_COINBASE_TAG_BYTES} bytes"
        )
    if any(byte < 0x20 or byte > 0x7e for byte in tag_bytes):
        raise SystemExit("PRISM_COINBASE_TAG must contain printable ASCII only")
    return tag_bytes.hex()


def default_prism_username_fallback_address() -> str | None:
    configured = env_optional("PRISM_USERNAME_FALLBACK_ADDRESS")
    if configured is not None:
        return configured
    if (os.environ.get("QBIT_CHAIN") or "regtest").lower() in TESTNET_QBIT_CHAINS:
        return DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS
    return None


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


class JsonRpc:
    def __init__(self, *, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.url = f"http://{host}:{port}"
        credentials = f"{user}:{password}".encode()
        self.auth = f"Basic {base64.b64encode(credentials).decode()}"
        # Keep-alive connections, one per calling thread. qbitd is called on
        # the hot share/block paths (a fresh getaddrinfo + TCP connect per call
        # was ~seconds of overhead under load); reusing the connection removes
        # that. threading.local keeps each thread's HTTPConnection private, so
        # concurrent callers never share a non-thread-safe connection.
        self._connections = threading.local()

    def _acquire_connection(self, timeout: float) -> http.client.HTTPConnection:
        conn = getattr(self._connections, "conn", None)
        if conn is None:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
            self._connections.conn = conn
        else:
            # Reuse: refresh the deadline for this call on the live socket.
            conn.timeout = timeout
            if conn.sock is not None:
                conn.sock.settimeout(timeout)
        return conn

    def _drop_connection(self) -> None:
        conn = getattr(self._connections, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._connections.conn = None

    def call(
        self,
        method: str,
        params: list[object] | None = None,
        *,
        wallet: str | None = None,
        timeout: float = 10,
    ) -> Any:
        body = json.dumps(
            {
                "jsonrpc": "1.0",
                "id": method,
                "method": method,
                "params": params or [],
            }
        ).encode()
        path = "/"
        if wallet is not None:
            path = f"/wallet/{urllib.parse.quote(wallet, safe='')}"
        headers = {
            "Authorization": self.auth,
            "Content-Type": "application/json",
            "User-Agent": "qbit-prism-coordinator/0.1",
        }
        # One retry with a fresh connection on a transport error. The usual
        # cause is the server having closed an idle keep-alive connection, in
        # which case the request never reached qbitd, so retrying is safe; the
        # only state-changing RPC (submitblock) is idempotent (duplicate ->
        # "duplicate") regardless. A second failure raises to the caller, which
        # treats it as backend-rpc-unavailable (a rejected share/block, never a
        # lost or double-counted block).
        last_exc: Exception | None = None
        for attempt in range(2):
            conn = self._acquire_connection(timeout)
            try:
                conn.request("POST", path, body=body, headers=headers)
                response = conn.getresponse()
                data = response.read()  # drain so the connection can be reused
            except (http.client.HTTPException, OSError) as exc:
                last_exc = exc
                self._drop_connection()
                if attempt == 0:
                    continue
                raise
            if response.status != 200:
                # Non-200 bodies may hold a JSON-RPC error (qbitd returns the
                # error object with a 500 for some methods); surface it as the
                # same RuntimeError text callers already match on (e.g. the
                # "-32601 / Method not found" blockwait-unsupported probe).
                self._drop_connection()
                detail = data.decode("utf-8", "replace")
                try:
                    error = json.loads(detail).get("error")
                except Exception:
                    error = None
                if error is not None:
                    raise RuntimeError(f"qbit RPC {method} failed: {error}")
                raise RuntimeError(f"qbit RPC {method} HTTP {response.status}: {detail[:200]}")
            payload = json.loads(data)
            if payload["error"] is not None:
                raise RuntimeError(f"qbit RPC {method} failed: {payload['error']}")
            return payload["result"]
        raise last_exc if last_exc is not None else RuntimeError("qbit RPC call failed")


@dataclass(frozen=True)
class WorkerIdentity:
    username: str
    payout_address: str
    worker_name: str | None
    script_pubkey_hex: str
    p2mr_program_hex: str


@dataclass
class _P2mrAddressValidationFlight:
    event: threading.Event = field(default_factory=threading.Event)
    result: tuple[str, str] | None = None
    error: BaseException | None = None
    waiters: int = 0


@dataclass(frozen=True)
class PrismJobContext:
    job: direct_stratum.DirectQbitStratumJob
    template: dict[str, Any]
    shares_json: list[dict[str, object]]
    prior_balances: list[dict[str, object]]
    found_block: dict[str, object]
    share_weight: int
    collection_only: bool
    worker: WorkerIdentity
    issued_at_ms: int
    template_fingerprint: str | None = None
    template_generation: int = 0
    payout_state_generation: int = 0
    payout_artifact_generation: int = 0
    connection_id: int = 0
    authorization_generation: int = 0
    difficulty_generation: int = 0


@dataclass
class PendingShareAppend:
    """A share waiting for the ledger group-commit writer.

    The client thread does not count or acknowledge this share until
    ``committed`` is set successfully.  A block candidate intent, when
    present, is inserted in the same transaction as the share.
    """

    pending_share: PendingShare
    username: str
    job_id: str
    block_hash_hex: str
    collection_only: bool
    credit_policy: str | None
    candidate_intent: dict[str, Any] | None = None
    committed: threading.Event = field(default_factory=threading.Event)
    record: Any | None = None
    error: BaseException | None = None
    writer_token: _WriterOperationToken | None = None


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


@dataclass(frozen=True)
class CachedTemplateArtifacts:
    """Template plus everything derivable from it alone, shared by all clients.

    Derived fields are keyed by the template fingerprint: a refetch whose
    fingerprint matches (only clock fields moved) reuses the previously
    computed transaction hexes and witness merkle leaves instead of re-hashing
    the full template. Generation records observation-start order so a slow,
    older fetch cannot supersede a newer observation merely by finishing last.
    """

    template: dict[str, Any]
    fingerprint: str
    previousblockhash: str
    transaction_hexes: tuple[str, ...]
    witness_merkle_leaves_hex: tuple[str, ...]
    network_difficulty: int
    fetched_monotonic: float
    generation: int = 0


@dataclass(frozen=True)
class QbitTipTemplateSnapshot:
    bestblockhash: str
    previousblockhash: str
    template_fingerprint: str
    template_generation: int = 0
    # The observation owns this exact artifact object.  It deliberately does
    # not participate in snapshot equality: callers compare the stable
    # identity fields above, while refresh preparation consumes the exact
    # template and derivations that were observed even if the mutable cache's
    # current pointer is replaced concurrently.
    template_artifacts: CachedTemplateArtifacts | None = field(
        default=None,
        compare=False,
        repr=False,
    )


@dataclass(frozen=True)
class RetainedCollectionRefresh:
    """Current immutable preparation waiting for a collection identity."""

    snapshot: QbitTipTemplateSnapshot
    observation_sequence: int
    payout_state_generation: int


@dataclass(frozen=True)
class CachedJobBundle:
    """One heavy job build (ledger snapshot + signed manifest + base job)
    shared across every client on the same template.

    The base job is built with the extranonce1 placeholder; per-client jobs
    are stamped from it by swapping job_id, extranonce1, difficulty, and the
    clean_jobs flag. All other fields are byte-identical across clients
    because the stratum coinbase split excludes the extranonce window.
    """

    key: tuple[object, ...]
    template: dict[str, Any]
    template_fingerprint: str
    coinbase_manifest: dict[str, Any]
    shares_json: list[dict[str, object]]
    prior_balances: list[dict[str, object]]
    found_block: dict[str, object]
    collection_only: bool
    issued_at_ms: int
    base_job: direct_stratum.DirectQbitStratumJob
    built_monotonic: float
    template_generation: int = 0
    payout_state_generation: int = 0
    payout_artifact_generation: int = 0
    # Collection coinbases commit a synthetic share to this exact payout
    # identity. Ready bundles have no worker-specific inputs and keep this
    # unset, which makes accidental cross-worker stamping fail closed.
    collection_identity: tuple[str, str] | None = None


@dataclass(frozen=True)
class EvictedJobEntry:
    context: PrismJobContext
    connection_id: int
    evicted_monotonic: float
    previousblockhash: str
    client: ClientState | None = None


@dataclass(frozen=True)
class RefreshResult:
    result: str
    delivered_monotonic: float | None = None


@dataclass(frozen=True, eq=False)
class TipRefreshValidationToken:
    """Immutable proof that one prepared refresh passed its expensive guard."""

    tip_hash: str
    template_fingerprint: str
    template_generation: int
    payout_state_generation: int
    observation_sequence: int
    snapshot: QbitTipTemplateSnapshot = field(repr=False)


@dataclass(frozen=True)
class PayoutStateCandidate:
    """Immutable result of payout work prepared outside delivery admission."""

    base_generation: int
    source_generation: int
    source_tip_hash: str | None
    cause: str
    invalidated_monotonic: float
    prepared_monotonic: float
    ledger_artifact: PayoutLedgerArtifact | None = field(
        default=None,
        compare=False,
        repr=False,
    )


@dataclass(frozen=True)
class PublishedPayoutState:
    """The payout snapshot identity to which cached jobs are stamped."""

    generation: int
    source_generation: int
    source_tip_hash: str | None
    published_monotonic: float


@dataclass(frozen=True)
class PayoutLedgerArtifact:
    """Immutable ledger input prepared independently of a qbit template."""

    generation: int
    payout_state_generation: int
    network_difficulty: int
    accepted_share_count: int
    shares_json: tuple[dict[str, object], ...] = field(repr=False)
    prior_balances: tuple[dict[str, object], ...] = field(repr=False)
    prepared_monotonic: float


@dataclass
class _PayoutDeliveryAdmission:
    admitted: bool
    wait_seconds: float
    generation: int
    published_generation: int
    relation: str
    delivered: bool = False

    def __bool__(self) -> bool:
        return self.admitted

    def mark_delivered(self) -> None:
        if not self.admitted:
            raise RuntimeError("payout delivery completed without admission")
        self.delivered = True


@dataclass(eq=False)
class PendingInitialJob:
    client: ClientState
    authorization_generation: int
    worker: WorkerIdentity
    requested_monotonic: float
    deadline_monotonic: float | None
    connection_id: int | None = None
    difficulty_generation: int | None = None
    cancelled: threading.Event = field(default_factory=threading.Event)
    future: Future[bool] | None = None
    predecessor: Future[bool] | None = None


class _DeliveryQueueFull(RuntimeError):
    """The bounded delivery executor cannot admit another task."""


class _JobBuildCancelled(RuntimeError):
    """A bundle waiter became obsolete before it acquired preparation."""


class _BoundedPriorityExecutor:
    """Small Future-compatible executor with bounded, priority-ordered work."""

    def __init__(self, *, max_workers: int, max_queue_size: int) -> None:
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        self._queue: queue.PriorityQueue[tuple[object, ...]] = queue.PriorityQueue(
            maxsize=max_queue_size
        )
        self._lock = threading.Lock()
        self._sequence = 0
        self._active_workers = 0
        self._shutdown = False
        self._threads = [
            threading.Thread(
                target=self._worker,
                name=f"prism-job-delivery-{index + 1}",
                daemon=True,
            )
            for index in range(max_workers)
        ]
        for thread in self._threads:
            thread.start()

    def submit(
        self,
        function: Callable[..., Any],
        /,
        *args: object,
        priority: int = PRISM_DELIVERY_PRIORITY_SAME_TIP,
        **kwargs: object,
    ) -> Future[Any]:
        future: Future[Any] = Future()
        with self._lock:
            if self._shutdown:
                raise RuntimeError("delivery executor is shut down")
            self._sequence += 1
            item = (
                int(priority),
                self._sequence,
                future,
                function,
                args,
                kwargs,
            )
            try:
                self._queue.put_nowait(item)
            except queue.Full as exc:
                raise _DeliveryQueueFull("delivery executor queue is full") from exc
        return future

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            _, _, future, function, args, kwargs = item
            if function is None:
                self._queue.task_done()
                return
            assert isinstance(future, Future)
            if not future.set_running_or_notify_cancel():
                self._queue.task_done()
                continue
            with self._lock:
                self._active_workers += 1
            try:
                result = function(*args, **kwargs)
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)
            finally:
                with self._lock:
                    self._active_workers -= 1
                self._queue.task_done()

    def stats(self) -> tuple[int, int]:
        with self._lock:
            return self._queue.qsize(), self._active_workers

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        with self._lock:
            if self._shutdown:
                threads = list(self._threads)
                already_shutdown = True
            else:
                self._shutdown = True
                threads = list(self._threads)
                already_shutdown = False
        if already_shutdown:
            if wait:
                for thread in threads:
                    thread.join()
            return
        if cancel_futures:
            while True:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                future = item[2]
                if isinstance(future, Future):
                    future.cancel()
                self._queue.task_done()
        for index in range(len(threads)):
            self._queue.put((math.inf, index, None, None, (), {}))
        if wait:
            for thread in threads:
                thread.join()


class _FanoutCancellation:
    """Cancel a fanout without racing already-admitted deliveries.

    ``cancel`` closes admission without waiting, so workers can call it while
    holding a client lock. The fanout coordinator calls ``set`` outside client
    locks to wait for deliveries that already passed the final gate.
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._cancelling = False
        self._active_deliveries = 0

    def is_set(self) -> bool:
        with self._condition:
            return self._cancelling

    def begin_delivery(self) -> bool:
        with self._condition:
            if self._cancelling:
                return False
            self._active_deliveries += 1
            return True

    def end_delivery(self) -> None:
        with self._condition:
            if self._active_deliveries <= 0:
                raise RuntimeError("fanout delivery gate released without admission")
            self._active_deliveries -= 1
            if self._active_deliveries == 0:
                self._condition.notify_all()

    def cancel(self) -> None:
        with self._condition:
            self._cancelling = True

    def set(self) -> None:
        self.cancel()
        with self._condition:
            while self._active_deliveries:
                self._condition.wait()


@dataclass
class _JobBundleBuildControl:
    key: tuple[object, ...]
    previousblockhash: str
    payout_state_generation: int
    payout_artifact_generation: int
    cancel_event: threading.Event = field(default_factory=threading.Event)
    process: subprocess.Popen[str] | None = None


class _PayoutStateDeliveryGate:
    """Order delivery admission around a very short payout publication.

    A publisher first closes admission and drains sends that already crossed
    the boundary.  It does not own the atomic publication section while that
    drain is in progress.  Once drained, publication ownership is transferred
    to the caller for the generation/cache pointer swap only.
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._active_deliveries = 0
        self._publisher_waiting = False
        self._mutation_owner: int | None = None
        self._mutation_depth = 0
        self._published_generation = 0
        self._priority_generation: int | None = None
        self._delivery_blocked = False

    @staticmethod
    def _generation_relation(generation: int, published_generation: int) -> str:
        if generation < published_generation:
            return "stale"
        if generation > published_generation:
            return "future"
        return "current"

    @contextmanager
    def delivery(self) -> Iterator[None]:
        with self.delivery_cancelable(lambda: False, priority=True) as admission:
            if not admission:
                raise RuntimeError("uncancelled payout delivery was not admitted")
            yield
            admission.mark_delivered()

    @contextmanager
    def delivery_cancelable(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int | None = None,
        priority: bool = False,
        poll_seconds: float = PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS,
    ) -> Iterator[_PayoutDeliveryAdmission]:
        """Admit a delivery unless cancellation wins while mutation owns the gate."""

        started = time.monotonic()
        admitted = False
        with self._condition:
            if generation is None:
                generation = self._published_generation
            while True:
                if cancelled():
                    break
                if self._delivery_blocked:
                    break
                if generation < self._published_generation:
                    break
                publication_blocked = (
                    self._publisher_waiting or self._mutation_owner is not None
                )
                priority_blocked = (
                    self._priority_generation is not None
                    and generation == self._priority_generation
                    and not priority
                )
                if priority_blocked:
                    # A same-generation job that is not for the published tip
                    # must not occupy a waiter slot indefinitely. Reject it so
                    # its caller can rebuild current-tip work; the reserved
                    # first-delivery lane remains available to priority work.
                    break
                future_blocked = generation > self._published_generation
                if (
                    not publication_blocked
                    and not future_blocked
                ):
                    self._active_deliveries += 1
                    admitted = True
                    break
                self._condition.wait(timeout=poll_seconds)
            published_generation = self._published_generation
            relation = self._generation_relation(generation, published_generation)
            admission = _PayoutDeliveryAdmission(
                admitted=admitted,
                wait_seconds=max(0.0, time.monotonic() - started),
                generation=generation,
                published_generation=published_generation,
                relation=relation,
            )
        try:
            yield admission
        finally:
            if admitted:
                with self._condition:
                    if self._active_deliveries <= 0:
                        raise RuntimeError("payout delivery gate released without admission")
                    self._active_deliveries -= 1
                    if (
                        priority
                        and admission.delivered
                        and generation == self._priority_generation
                    ):
                        # Keep routine same-generation sends queued until the
                        # first prioritized current-tip socket delivery exits.
                        # Privileged synchronization admissions do not consume
                        # this reservation merely by leaving the gate.
                        self._priority_generation = None
                    if self._active_deliveries == 0:
                        self._condition.notify_all()
                    elif self._priority_generation is None:
                        self._condition.notify_all()

    @contextmanager
    def publication(self) -> Iterator[None]:
        owner = threading.get_ident()
        with self._condition:
            if self._mutation_owner == owner:
                self._mutation_depth += 1
            else:
                while self._mutation_owner is not None or self._publisher_waiting:
                    self._condition.wait()
                self._publisher_waiting = True
                while self._active_deliveries:
                    self._condition.wait()
                self._mutation_owner = owner
                self._mutation_depth = 1
                self._publisher_waiting = False
        try:
            yield
        finally:
            with self._condition:
                if self._mutation_owner != owner or self._mutation_depth <= 0:
                    raise RuntimeError("payout mutation gate released by non-owner")
                self._mutation_depth -= 1
                if self._mutation_depth == 0:
                    self._mutation_owner = None
                    self._condition.notify_all()

    def publish_generation(self, generation: int, *, prioritize_delivery: bool) -> None:
        owner = threading.get_ident()
        with self._condition:
            if self._mutation_owner != owner:
                raise RuntimeError("payout generation published outside atomic section")
            if generation <= self._published_generation:
                raise RuntimeError("payout generation did not advance")
            self._published_generation = generation
            self._priority_generation = generation if prioritize_delivery else None
            self._delivery_blocked = False

    def block_delivery(
        self,
        mark_blocked: Callable[[], bool] | None = None,
    ) -> bool:
        """Reject admission until publication, atomically with caller state."""

        with self._condition:
            # A publisher drops the condition while swapping its immutable
            # pointer. Wait only for that short section, never for admitted
            # socket sends or fanout cancellation.
            while self._mutation_owner is not None:
                self._condition.wait()
            if mark_blocked is not None and not mark_blocked():
                return False
            self._delivery_blocked = True
            self._condition.notify_all()
            return True

    @contextmanager
    def mutation(self) -> Iterator[None]:
        """Compatibility alias for tests and callers that only need exclusion."""

        with self.publication():
            yield


@dataclass
class _SharedBundlePreparationFlight:
    event: threading.Event = field(default_factory=threading.Event)
    result: CachedJobBundle | None = None
    error: BaseException | None = None
    waiters: int = 0


@dataclass(eq=False)
class ClientState:
    sock: socket.socket
    address: tuple[str, int]
    connection_id: int
    extranonce1_hex: str
    subscribed: bool = False
    authorized: bool = False
    authorization_generation: int = 0
    difficulty_generation: int = 0
    authorized_monotonic: float | None = None
    username: str = ""
    worker: WorkerIdentity | None = None
    version_mask: int = 0
    active_job: PrismJobContext | None = None
    listener_name: str = "default"
    # Pristine difficulty policy of the accepting listener; never mutated.
    listener_vardiff_config: vardiff.VardiffConfig | None = None
    # Floor below which stamped jobs never advertise, copied from the
    # accepting listener profile. Zero (default listener) keeps the network
    # cap authoritative.
    minimum_advertised_difficulty: Decimal = Decimal("0")
    # Per-client specialization of the listener policy (password d=/md= or
    # mining.suggest_difficulty); recomputed from the pristine base on every
    # request so repeat applications cannot compound.
    vardiff_config: vardiff.VardiffConfig | None = None
    requested_difficulty: Decimal | None = None
    requested_min_difficulty: Decimal | None = None
    suggested_difficulty: Decimal | None = None
    share_difficulty: Decimal = Decimal("1")
    pending_share_difficulty: Decimal | None = None
    vardiff_window_started_monotonic: float = field(default_factory=time.monotonic)
    vardiff_window_accepted: int = 0
    vardiff_window_submitted: int = 0
    vardiff_window_work: Decimal = Decimal("0")
    vardiff_difficulty_estimate: Decimal | None = None
    active_job_ids: set[str] = field(default_factory=set)
    post_accept_refresh_block: tuple[int, str] | None = None
    # (job previousblockhash, monotonic) of the FIRST job this connection was
    # sent for that tip. Anchors the per-connection stale-grace window: a
    # prior-tip share is in flight until shortly after this connection
    # received replacement work, however long the refresh pass took to reach
    # it. See stale_grace_deadline_open.
    tip_work_delivered: tuple[str, float] | None = None
    # Protected by the coordinator lock. Disconnect retirement sets this before
    # waiting for any per-client job update so queued work can reject the client.
    closing: bool = False
    # Serializes every job build/register/send transition for this connection.
    # The coordinator lock may be acquired while this lock is held, never in
    # the reverse order. RLock permits authorize/retarget helpers to call the
    # common maybe_send_job path while retaining the same serialization scope.
    job_update_lock: threading.RLock = field(default_factory=threading.RLock)
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    handler_thread_registered: bool = False

    def send(self, payload: dict[str, object]) -> None:
        data = json.dumps(payload).encode() + b"\n"
        with self.send_lock:
            self.sock.sendall(data)

    def send_batch(self, payloads: list[dict[str, object]]) -> None:
        # Tests and embedders may replace ``send`` with an in-memory recorder;
        # retain that seam while production sockets write the whole difficulty
        # + notify pair under one send lock with no response interleaving.
        if "send" in self.__dict__:
            for payload in payloads:
                self.send(payload)
            return
        data = b"".join(json.dumps(payload).encode() + b"\n" for payload in payloads)
        with self.send_lock:
            self.sock.sendall(data)

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class StratumError(RuntimeError):
    def __init__(
        self,
        code: int,
        message: str,
        *,
        reason: str | None = None,
        disconnect: bool = False,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.reason = reason
        self.disconnect = disconnect


class ShutdownInProgress(RuntimeError):
    """Raised when work that could mutate the ledger arrives after shutdown."""


class _WriterOperationToken:
    """One transferable writer admission held until durable work completes."""

    def __init__(self, controller: "CoordinatorShutdownController", component: str):
        self.controller = controller
        self.component = component
        self.finished = False

    def finish(self) -> None:
        self.controller.finish_token(self)


class CoordinatorShutdownController:
    """Coordinates the writer barrier, one-shot lease release, and final drain.

    Writer operations enter through :meth:`enter_writer` before shutdown or
    inherit an already-admitted operation on the same thread. Queue admissions
    use transferable tokens so a share remains visible to the barrier while it
    moves from a client thread to the group-commit writer.
    """

    def __init__(self, writer_quiescence_timeout_seconds: float):
        self.writer_quiescence_timeout_seconds = writer_quiescence_timeout_seconds
        self.condition = threading.Condition(threading.RLock())
        self.local = threading.local()
        self.phase = "running"
        self.reason: str | None = None
        self.signal_number: int | None = None
        self.sigterm_monotonic: float | None = None
        self.shutdown_started_monotonic: float | None = None
        self.active_writers: dict[str, int] = {}
        self.shutdowns_total = 0
        self.writer_quiescence_outcomes = {"success": 0, "timeout": 0}
        self.writer_quiescence_seconds = 0.0
        self.lease_release_attempts_total = 0
        self.lease_release_outcomes = {
            "success": 0,
            "not_held": 0,
            "unsupported": 0,
            "failure": 0,
        }
        self.lease_release_seconds = 0.0
        self.lease_release_attempted = False
        self.lease_release_succeeded = False
        self.lease_release_withheld = False
        self.sigterm_to_lease_release_seconds = 0.0
        self.sigterm_release_observed = False
        self.release_withheld_total = 0
        self.non_writer_drain_seconds = 0.0
        self.non_writer_drains_total = 0
        self._drain_claimed = False

    def request_shutdown(self, signum: int | None) -> None:
        """Close admission atomically; the caller only needs to set its event."""
        now = time.monotonic()
        with self.condition:
            if signum == signal.SIGTERM and self.sigterm_monotonic is None:
                self.sigterm_monotonic = now
            if self.signal_number is None and signum is not None:
                self.signal_number = signum
            if self.phase == "running":
                self.phase = "requested"
            self.condition.notify_all()

    def begin_shutdown(self, reason: str) -> bool:
        with self.condition:
            if self.phase not in {"running", "requested"}:
                return False
            self.phase = "quiescing_writers"
            self.reason = reason
            self.shutdown_started_monotonic = time.monotonic()
            self.shutdowns_total += 1
            self.condition.notify_all()
            return True

    def wait_for_lease_handling(self) -> bool:
        """Wait for the one shutdown owner to release or safely withhold."""
        in_progress = {
            "requested",
            "quiescing_writers",
            "writers_quiesced",
            "releasing_lease",
        }
        with self.condition:
            while self.phase in in_progress:
                self.condition.wait()
            return self.lease_release_succeeded

    def _thread_writer_depth(self) -> int:
        return int(getattr(self.local, "writer_depth", 0))

    def _admit_writer_locked(self, component: str, *, inherited: bool) -> _WriterOperationToken:
        if self.lease_release_attempted:
            raise ShutdownInProgress("PRISM writer lease release has already started")
        if self.phase != "running" and not inherited:
            raise ShutdownInProgress("PRISM coordinator is shutting down")
        self.active_writers[component] = self.active_writers.get(component, 0) + 1
        return _WriterOperationToken(self, component)

    def enter_writer(self, component: str) -> _WriterOperationToken:
        depth = self._thread_writer_depth()
        with self.condition:
            token = self._admit_writer_locked(component, inherited=depth > 0)
        self.local.writer_depth = depth + 1
        return token

    def exit_writer(self, token: _WriterOperationToken) -> None:
        depth = self._thread_writer_depth()
        self.local.writer_depth = max(0, depth - 1)
        token.finish()

    def reserve_writer(self, component: str) -> _WriterOperationToken:
        """Reserve work that will finish on another thread."""
        with self.condition:
            return self._admit_writer_locked(
                component,
                inherited=self._thread_writer_depth() > 0,
            )

    def finish_token(self, token: _WriterOperationToken) -> None:
        with self.condition:
            if token.finished:
                return
            token.finished = True
            remaining = self.active_writers.get(token.component, 0) - 1
            if remaining > 0:
                self.active_writers[token.component] = remaining
            else:
                self.active_writers.pop(token.component, None)
            self.condition.notify_all()

    def has_active_writer(self, components: set[str]) -> bool:
        with self.condition:
            return any(self.active_writers.get(component, 0) for component in components)

    def writer_admission_closed(self) -> bool:
        with self.condition:
            return self.phase != "running"

    def wait_for_writer_quiescence(self) -> tuple[bool, float, dict[str, int]]:
        started = time.monotonic()
        deadline = started + self.writer_quiescence_timeout_seconds
        with self.condition:
            while self.active_writers:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.condition.wait(remaining)
            elapsed = max(0.0, time.monotonic() - started)
            quiesced = not self.active_writers
            blockers = dict(sorted(self.active_writers.items()))
            outcome = "success" if quiesced else "timeout"
            self.writer_quiescence_outcomes[outcome] += 1
            self.writer_quiescence_seconds = elapsed
            if quiesced:
                self.phase = "writers_quiesced"
            else:
                self.phase = "release_withheld"
                self.lease_release_withheld = True
                self.release_withheld_total += 1
            self.condition.notify_all()
            return quiesced, elapsed, blockers

    def claim_lease_release(self) -> tuple[bool, dict[str, int]]:
        with self.condition:
            if self.lease_release_attempted or self.lease_release_withheld:
                return False, {}
            if self.active_writers:
                return False, dict(sorted(self.active_writers.items()))
            self.lease_release_attempted = True
            self.lease_release_attempts_total += 1
            self.phase = "releasing_lease"
            self.condition.notify_all()
            return True, {}

    def finish_lease_release(self, outcome: str, elapsed: float) -> None:
        with self.condition:
            self.lease_release_outcomes[outcome] += 1
            self.lease_release_seconds = elapsed
            self.lease_release_succeeded = outcome != "failure"
            self.phase = "lease_released" if outcome != "failure" else "lease_release_failed"
            if outcome != "failure" and self.sigterm_monotonic is not None:
                self.sigterm_to_lease_release_seconds = max(
                    0.0,
                    time.monotonic() - self.sigterm_monotonic,
                )
                self.sigterm_release_observed = True
            self.condition.notify_all()

    def claim_non_writer_drain(self) -> bool:
        with self.condition:
            if self._drain_claimed:
                return False
            if self.phase not in {
                "lease_released",
                "lease_release_failed",
                "release_withheld",
            }:
                return False
            self._drain_claimed = True
            self.phase = "draining_non_writers"
            return True

    def finish_non_writer_drain(self, elapsed: float) -> None:
        with self.condition:
            self.non_writer_drain_seconds = elapsed
            self.non_writer_drains_total += 1
            self.phase = "complete"
            self.condition.notify_all()

    def snapshot(self) -> dict[str, Any]:
        with self.condition:
            return {
                "phase": self.phase,
                "active_writers": dict(self.active_writers),
                "shutdowns_total": self.shutdowns_total,
                "writer_quiescence_outcomes": dict(self.writer_quiescence_outcomes),
                "writer_quiescence_seconds": self.writer_quiescence_seconds,
                "lease_release_attempts_total": self.lease_release_attempts_total,
                "lease_release_outcomes": dict(self.lease_release_outcomes),
                "lease_release_seconds": self.lease_release_seconds,
                "lease_release_withheld": self.lease_release_withheld,
                "sigterm_to_lease_release_seconds": self.sigterm_to_lease_release_seconds,
                "sigterm_release_observed": self.sigterm_release_observed,
                "release_withheld_total": self.release_withheld_total,
                "non_writer_drain_seconds": self.non_writer_drain_seconds,
                "non_writer_drains_total": self.non_writer_drains_total,
            }


def ledger_writer_operation(component: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorate an entry point that can mutate the PRISM ledger."""

    def decorate(method: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(method)
        def guarded(self: "PrismCoordinator", *args: Any, **kwargs: Any) -> Any:
            with self._writer_operation(component):
                return method(self, *args, **kwargs)

        return guarded

    return decorate


class TemplateRefreshBlocked(RuntimeError):
    """A live template was fetched, but safe work could not be issued."""


class TemplateRefreshSuperseded(TemplateRefreshBlocked):
    """Concurrent tip/payout progress invalidated this refresh attempt.

    Raised only for coordination races that a scheduled retry resolves on its
    own: the tip advanced mid-refresh, the payout-state generation moved, or a
    newer observation superseded the prepared work. Unlike its parent, this
    subclass never arms the template-refresh failure budget -- a genuine
    RPC/build/trust failure must raise plain TemplateRefreshBlocked so
    sustained unhealthiness still takes the budgeted restart path.
    """


class _PayoutStatePublicationBlocked(TemplateRefreshBlocked):
    """Job construction is waiting for a prepared payout publication."""


class _JobBundleBuildSuperseded(TemplateRefreshBlocked):
    """A newer tip or payout generation canceled this deterministic build."""


class CollectionIdentityUnavailable(TemplateRefreshBlocked):
    """Current collection work is waiting for an authorized worker identity."""


class _JobBuildFailed(RuntimeError):
    """Internal signal used to distinguish a skipped build from a no-op."""


class _BundlePreparationSuperseded(TemplateRefreshSuperseded):
    """The exact work identity lost to a newer tip/template observation.

    Subclasses TemplateRefreshSuperseded: losing the shared-bundle build race
    to a newer tip/template observation is coordination churn, so it escapes
    the poll without arming the template-refresh failure budget.
    """


def parse_worker_username(username: str) -> tuple[str, str | None]:
    payout_address, worker_name = split_worker_username(username)
    if not payout_address:
        raise StratumError(20, "username base is empty")
    return payout_address, worker_name


def split_worker_username(username: str) -> tuple[str, str | None]:
    payout_address, separator, worker_name = username.partition(".")
    return payout_address, worker_name if separator else None


class PrismCoordinator:
    def __init__(self) -> None:
        validate_prism_production_gate()
        self.rpc = JsonRpc(
            host=env("QBIT_RPC_HOST"),
            port=env_int("QBIT_RPC_PORT", 18452),
            user=env("QBIT_RPC_USER"),
            password=env("QBIT_RPC_PASSWORD"),
        )
        self.qbit_chain = env("QBIT_CHAIN", "regtest")
        self.bind = env("PRISM_STRATUM_BIND", "127.0.0.1")
        self.port = env_int("PRISM_STRATUM_PORT", 3340)
        self.extranonce2_size = env_int("PRISM_STRATUM_EXTRANONCE2_SIZE", 8)
        self.blockpoll_seconds = env_positive_float(
            "PRISM_BLOCKPOLL_SECONDS",
            DEFAULT_PRISM_BLOCKPOLL_SECONDS,
        )
        # Push-style tip detection rides waitfornewblock; the poll loop above
        # stays as the fallback and still covers same-tip template refreshes.
        self.blockwait_enabled = env_bool("PRISM_BLOCKWAIT_ENABLED", "1")
        self.blockwait_timeout_seconds = env_positive_float(
            "PRISM_BLOCKWAIT_TIMEOUT_SECONDS",
            DEFAULT_PRISM_BLOCKWAIT_TIMEOUT_SECONDS,
        )
        # Zero disables stale-grace crediting (every prior-tip share rejects,
        # the pre-grace behavior).
        self.stale_grace_seconds = env_nonnegative_float(
            "PRISM_STRATUM_STALE_GRACE_SECONDS",
            DEFAULT_PRISM_STALE_GRACE_SECONDS,
        )
        # Per-share/per-job stdout logging is debug-only: at production share
        # rates each print is a journald flush on the Stratum hot path.
        self.hot_path_log_enabled = env_bool("PRISM_HOT_PATH_LOG", "0")
        # Zero disables the observed-tip reuse (every submit re-reads the tip
        # over RPC, the legacy behavior).
        self.submit_tip_max_age_seconds = env_nonnegative_float(
            "PRISM_SUBMIT_TIP_MAX_AGE_SECONDS",
            DEFAULT_PRISM_SUBMIT_TIP_MAX_AGE_SECONDS,
        )
        self.same_tip_job_retention_seconds = env_nonnegative_float(
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS",
            DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
        )
        self.same_tip_job_retention_per_connection = env_nonnegative_int(
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION",
            DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION,
        )
        self.tip_refresh_max_workers = env_positive_int(
            "PRISM_TIP_REFRESH_MAX_WORKERS",
            DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS,
        )
        if self.tip_refresh_max_workers > DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS:
            raise SystemExit(
                "PRISM_TIP_REFRESH_MAX_WORKERS cannot exceed "
                f"{DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS}"
            )
        self.vardiff_idle_sweep_seconds = env_nonnegative_float(
            "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS",
            DEFAULT_PRISM_VARDIFF_IDLE_SWEEP_SECONDS,
        )
        # Zero collapses every worker into the overflow label (per-worker
        # metrics effectively off) without touching the aggregate counters.
        self.worker_metrics_limit = env_nonnegative_int(
            "PRISM_WORKER_METRICS_LIMIT",
            DEFAULT_PRISM_WORKER_METRICS_LIMIT,
        )
        self.reorg_reconciler_enabled = env_bool("PRISM_REORG_RECONCILER_ENABLED", "1")
        # Per-template job caching. A zero disables the corresponding cache
        # (every build redoes that stage), which is also the legacy behavior.
        self.job_bundle_cache_seconds = env_nonnegative_float(
            "PRISM_JOB_BUNDLE_CACHE_SECONDS",
            DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS,
        )
        self.bundle_build_timeout_seconds = env_positive_float(
            "PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS",
            DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
        )
        self.template_cache_seconds = env_nonnegative_float(
            "PRISM_TEMPLATE_CACHE_SECONDS",
            self.blockpoll_seconds,
        )
        self.template_refresh_failure_exit_seconds = env_nonnegative_float(
            "PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS",
            DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
        )
        if production_mode() and self.template_refresh_failure_exit_seconds <= 0:
            raise SystemExit(
                "production mode requires a positive "
                "PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS"
            )
        self.last_successful_template_refresh_monotonic: float | None = None
        self.template_refresh_failure_started_monotonic: float | None = None
        self.reorg_reconcile_cache_seconds = env_nonnegative_float(
            "PRISM_REORG_RECONCILE_CACHE_SECONDS",
            DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
        )
        self.health_refresh_seconds = env_positive_float(
            "PRISM_HEALTH_REFRESH_SECONDS",
            DEFAULT_PRISM_HEALTH_REFRESH_SECONDS,
        )
        self.stratum_send_timeout_seconds = env_nonnegative_float(
            "PRISM_STRATUM_SEND_TIMEOUT_SECONDS",
            DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS,
        )
        # Zero remains an explicit unlimited option for focused local/regtest
        # use. Deployments default to a conservative ceiling above PRISM's
        # normal 200-250 connection population.
        self.stratum_max_connections = env_nonnegative_int(
            "PRISM_STRATUM_MAX_CONNECTIONS",
            DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
        )
        self.stratum_max_connections_per_username = env_nonnegative_int(
            "PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME",
            DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME,
        )
        self.stratum_max_pending_initial_jobs = env_positive_int(
            "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS",
            DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS,
        )
        if (
            self.stratum_max_connections > 0
            and self.stratum_max_pending_initial_jobs > self.stratum_max_connections
        ):
            raise SystemExit(
                "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS cannot exceed "
                "PRISM_STRATUM_MAX_CONNECTIONS"
            )
        self.stratum_initial_job_timeout_seconds = env_nonnegative_float(
            "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS",
            DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS,
        )
        self.mining_health_startup_grace_seconds = env_nonnegative_float(
            "PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS",
            DEFAULT_PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS,
        )
        validate_same_tip_job_retention_limits(
            retention_seconds=self.same_tip_job_retention_seconds,
            per_connection=self.same_tip_job_retention_per_connection,
            max_connections=self.stratum_max_connections,
            production=production_mode(),
        )
        self.stratum_accept_resource_exhaustion_backoff_seconds = env_positive_float(
            "PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS",
            DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS,
        )
        self.payout_address_cache_max_entries = env_nonnegative_int(
            "PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES",
            DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES,
        )
        self.payout_address_cache_ttl_seconds = env_nonnegative_float(
            "PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS",
            DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS,
        )
        self.coinbase_tag_hex = default_prism_coinbase_tag_hex()
        self.share_difficulty = env_decimal("PRISM_STRATUM_SHARE_DIFF", "0.000000001")
        self.vardiff_config = load_prism_vardiff_config(self.share_difficulty)
        self.listener_profiles = [
            StratumListenerProfile(
                name="default",
                bind=self.bind,
                port=self.port,
                share_difficulty=self.share_difficulty,
                vardiff_config=self.vardiff_config,
                heartbeat_name="stratum_accept",
            )
        ]
        highdiff_profile = load_prism_highdiff_listener(self.bind, self.vardiff_config)
        if highdiff_profile is not None:
            if highdiff_profile.port == self.port and highdiff_profile.bind == self.bind:
                raise SystemExit("PRISM_STRATUM_HIGHDIFF_PORT must differ from PRISM_STRATUM_PORT")
            self.listener_profiles.append(highdiff_profile)
        self.default_share_weight = env_int("PRISM_STRATUM_SHARE_WEIGHT", 1)
        if self.default_share_weight <= 0:
            raise SystemExit("PRISM_STRATUM_SHARE_WEIGHT must be positive")
        self.share_weights_by_username = self.parse_share_weights()
        self.username_fallback_address = default_prism_username_fallback_address()
        self.min_ready_miners = env_int("PRISM_MIN_READY_MINERS", 3)
        self.signing_seed_hex = env_seed_hex(
            "PRISM_MANIFEST_SIGNING_SEED_HEX",
            test_default="42" * 32,
        )
        self.ledger_attestation_signing_seed_hex = env_seed_hex(
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX",
            test_default="43" * 32,
        )
        self.ledger_writer_public_key_hex = self.load_trusted_ledger_writer_public_key()
        self.evidence_path = Path(env("PRISM_EVIDENCE_PATH", "prism-live-evidence.json"))
        self.audit_dir = Path(env("PRISM_AUDIT_DIR", str(self.evidence_path.parent)))
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.audit_share_segment_size = env_nonnegative_int(
            "PRISM_AUDIT_SHARE_SEGMENT_SIZE",
            DEFAULT_AUDIT_SHARE_SEGMENT_SIZE,
        )
        self.audit_live_bundle_retention = env_nonnegative_int("PRISM_AUDIT_LIVE_BUNDLE_RETENTION", 5)
        self.audit_candidate_retention_seconds = env_nonnegative_int(
            "PRISM_AUDIT_CANDIDATE_RETENTION_SECONDS",
            24 * 60 * 60,
        )
        self.ctv_broadcast_attempt_detail_limit = env_nonnegative_int(
            "PRISM_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT",
            DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
        )
        self.ctv_broadcast_retry_backoff_seconds = env_nonnegative_int(
            "PRISM_CTV_BROADCAST_RETRY_BACKOFF_SECONDS",
            DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
        )
        self.audit_bind = os.environ.get("PRISM_AUDIT_BIND")
        self.audit_port = int(os.environ.get("PRISM_AUDIT_PORT", "0") or "0")
        self.stop_after_block = env("PRISM_STOP_AFTER_BLOCK", "1") in {"1", "true", "yes"}
        self.max_blocks = env_int("PRISM_MAX_BLOCKS", 1)
        if self.max_blocks <= 0:
            raise SystemExit("PRISM_MAX_BLOCKS must be positive")
        try:
            fallback_version_mask = direct_stratum.normalize_version_rolling_mask(
                env("PRISM_VERSION_ROLLING_MASK", direct_stratum.QBIT_VERSION_ROLLING_MASK_HEX),
                field_name="PRISM_VERSION_ROLLING_MASK",
            )
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
        self.version_mask_selection = self.resolve_version_rolling_mask(fallback_version_mask)
        self.version_mask = self.version_mask_selection.selected_mask
        self.writer_quiescence_timeout_seconds = env_positive_float(
            "PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS",
            DEFAULT_PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS,
        )
        self.ledger = self.make_ledger()
        configured_ctv_broadcaster_enabled = env_optional_bool("PRISM_CTV_BROADCASTER_ENABLED")
        self.ctv_broadcaster_enabled = (
            configured_ctv_broadcaster_enabled
            if configured_ctv_broadcaster_enabled is not None
            else env_bool("PRISM_CTV_SETTLEMENT_ENABLED", "0")
        )
        self.ctv_broadcaster_wallet = env_optional("PRISM_CTV_BROADCASTER_WALLET")
        self.ctv_broadcaster_fee_sats = env_nonnegative_int_with_legacy(
            "PRISM_CTV_BROADCASTER_FEE_BITS",
            "PRISM_CTV_BROADCASTER_FEE_SATS",
            0,
        )
        if (
            self.ctv_broadcaster_enabled
            and self.ctv_broadcaster_fee_sats > 0
            and not self.ctv_broadcaster_wallet
        ):
            raise SystemExit(
                "PRISM_CTV_BROADCASTER_WALLET is required when "
                "PRISM_CTV_BROADCASTER_FEE_BITS is positive"
            )
        self.ctv_broadcaster_limit = env_positive_int("PRISM_CTV_BROADCASTER_LIMIT", 100)
        self.ctv_broadcaster_chunk_size = env_positive_int(
            "PRISM_CTV_BROADCASTER_CHUNK_SIZE",
            DEFAULT_PRISM_CTV_BROADCASTER_CHUNK_SIZE,
        )
        if self.ctv_broadcaster_chunk_size > MAX_CTV_FANOUT_BROADCASTER_CHUNK_SIZE:
            raise SystemExit(
                "PRISM_CTV_BROADCASTER_CHUNK_SIZE must be at most "
                f"{MAX_CTV_FANOUT_BROADCASTER_CHUNK_SIZE}"
            )
        self.ctv_broadcaster_interval_seconds = env_positive_float(
            "PRISM_CTV_BROADCASTER_INTERVAL_SECONDS",
            30.0,
        )
        self._ctv_broadcaster_metrics_lock = threading.Lock()
        self.ctv_broadcaster_pass_seconds_bucket_counts = {
            bucket: 0 for bucket in PRISM_CTV_BROADCASTER_SECONDS_BUCKETS
        }
        self.ctv_broadcaster_pass_seconds_sum = 0.0
        self.ctv_broadcaster_pass_count = 0
        self.ctv_broadcaster_processed_rows_total = 0
        self._ctv_fanout_market_fee_rate_cache: dict[tuple[int | None, str | None], int] = {}
        self.ctv_fanout_broadcast_daemon: CtvFanoutBroadcastDaemon | None = None
        self.lock = threading.RLock()
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
        self.recent_share_keys: set[tuple[object, ...]] = set()
        self.connection_counter = 0
        self.job_counter = 0
        self.accepted_block_count = 0
        self.started_monotonic = time.monotonic()
        self.submitted_share_count = 0
        self.stale_share_count = 0
        self.duplicate_share_count = 0
        self.low_difficulty_share_count = 0
        self.collection_block_submission_count = 0
        self._pool_ready_latched = False
        self.grace_credited_share_count = 0
        self.idle_retarget_count = 0
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
        # (tip_hash, flip_monotonic_or_None) / (tip_hash, parent_hash) caches.
        # The stamp is when the refresh path saw the tip CHANGE; None marks the
        # startup baseline tip, which never opens the stale-grace window.
        self.current_tip_first_seen: tuple[str, float | None] | None = None
        self.current_tip_parent: tuple[str, str] | None = None
        # When the poller/blockwait last confirmed the observed tip against
        # qbit (including same-tip re-observations). Bounds how long
        # mining.submit may classify against the observed tip before falling
        # back to a live RPC read (see submit_stale_check_tip).
        self.current_tip_observed_monotonic: float | None = None
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
        self.share_commit_batch_size = env_positive_int(
            "PRISM_SHARE_COMMIT_BATCH_SIZE", DEFAULT_SHARE_COMMIT_BATCH_SIZE
        )
        self.share_commit_linger_seconds = (
            env_nonnegative_float(
                "PRISM_SHARE_COMMIT_LINGER_MILLISECONDS",
                DEFAULT_SHARE_COMMIT_LINGER_MILLISECONDS,
            )
            / 1000.0
        )
        self.share_commit_timeout_seconds = env_positive_float(
            "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS", DEFAULT_SHARE_COMMIT_TIMEOUT_SECONDS
        )
        self.share_writer_active = False
        self.share_append_failure_count = 0
        # Retain the historical recovery-file reader for clean upgrades from a
        # release that could acknowledge before Postgres commit.  New shares
        # are never written here: an unavailable ledger produces no success
        # acknowledgement and an exact retry is idempotent.
        self.share_recovery_path = Path(
            env("PRISM_SHARE_RECOVERY_PATH", str(self.audit_dir / "prism-unpersisted-shares.jsonl"))
        )
        self.share_recovery_lock = threading.Lock()
        self.shares_recovered_to_disk = 0
        self.shares_replayed = 0
        self.job_build_failure_count = 0
        self.tip_refresh_job_count = 0
        self.post_accept_refresh_failure_count = 0
        self.reorg_inactive_block_count = 0
        self.reorg_reactivated_block_count = 0
        self.reorg_reconcile_skip_count = 0
        self.reorg_reconcile_error_count = 0
        self.matured_payout_count = 0
        self.latest_evidence: dict[str, Any] | None = None
        # The full accepted-block bundle is durable in the audit store.  Keeping
        # it here only to derive one metric pinned the complete share window for
        # the lifetime of the coordinator.
        self.latest_coinbase_size_bytes: int | None = None
        self.tip_template_snapshot: QbitTipTemplateSnapshot | None = None
        self._tip_refresh_lock = threading.Lock()
        self._tip_refresh_executor_lock = threading.Lock()
        self._tip_refresh_executor: _BoundedPriorityExecutor | None = None
        self._tip_refresh_executor_shutdown = False
        self._tip_refresh_pending_event = threading.Event()
        self._tip_refresh_pending_counter = 0
        self._tip_refresh_pending_token: int | None = None
        self._tip_refresh_retry = threading.Event()
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
        self._watchdog_pauses: dict[str, int] = {}
        self._heartbeats_lock = threading.Lock()
        self.watchdog_enabled = env_bool("PRISM_WATCHDOG_ENABLED", "1")
        self.watchdog_timeout_seconds = env_positive_float("PRISM_WATCHDOG_TIMEOUT_SECONDS", 120.0)
        self.watchdog_interval_seconds = env_positive_float("PRISM_WATCHDOG_INTERVAL_SECONDS", 15.0)

    def record_rejection(self, reason: str, *, worker: str | None = None) -> None:
        if reason not in PRISM_REJECTION_REASON_IDS:
            raise ValueError(f"unknown PRISM rejection reason: {reason}")
        with self.lock:
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
        raw = os.environ.get("PRISM_STRATUM_SHARE_WEIGHTS_JSON", "")
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"PRISM_STRATUM_SHARE_WEIGHTS_JSON is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise SystemExit("PRISM_STRATUM_SHARE_WEIGHTS_JSON must be an object")
        weights: dict[str, int] = {}
        for username, weight in parsed.items():
            parsed_weight = int(weight)
            if parsed_weight <= 0:
                raise SystemExit(f"share weight for {username} must be positive")
            weights[str(username)] = parsed_weight
        return weights

    def share_weight_for_worker(self, worker: WorkerIdentity) -> int:
        return self.share_weights_by_username.get(
            worker.username,
            self.share_weights_by_username.get(worker.payout_address, self.default_share_weight),
        )

    def make_ledger(self) -> SingleWriterShareLedger | PsqlShareLedger:
        psql_command = os.environ.get("PRISM_POSTGRES_PSQL_COMMAND", "")
        database_url = os.environ.get("PRISM_DATABASE_URL", "")
        if not psql_command and database_url:
            psql_command = f"psql {shlex.quote(database_url)}"
        if not psql_command:
            if not env_bool("PRISM_ALLOW_MEMORY_LEDGER", "0"):
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
        writer_session_token = env_optional("PRISM_LEDGER_WRITER_SESSION_TOKEN")
        if writer_session_token is not None and not env_bool("PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN", "0"):
            raise SystemExit(
                "PRISM_LEDGER_WRITER_SESSION_TOKEN requires "
                "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN=1 for local tests"
            )
        audit_body_dir = getattr(self, "audit_dir", None)
        return PsqlShareLedger(
            psql_command=psql_command,
            database_url=database_url or None,
            native_client_mode=env("PRISM_POSTGRES_NATIVE_CLIENT", "auto"),
            writer_id=env("PRISM_LEDGER_WRITER_ID", "prism-coordinator"),
            writer_epoch=env_int("PRISM_LEDGER_WRITER_EPOCH", 1),
            writer_session_token=writer_session_token,
            initialize_schema=env("PRISM_POSTGRES_INIT_SCHEMA", "0") in {"1", "true", "yes"},
            lease_ttl_seconds=env_positive_float("PRISM_LEDGER_LEASE_TTL_SECONDS", 60.0),
            read_concurrency=env_positive_int("PRISM_POSTGRES_READ_CONCURRENCY", 4),
            accepted_stats_cache_seconds=env_nonnegative_float("PRISM_ACCEPTED_STATS_CACHE_SECONDS", 60.0),
            audit_body_dir=str(audit_body_dir) if audit_body_dir is not None else None,
            audit_share_segment_size=getattr(self, "audit_share_segment_size", DEFAULT_AUDIT_SHARE_SEGMENT_SIZE),
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

    def _ensure_job_cache_state(self) -> None:
        if not hasattr(self, "_job_cache_lock"):
            self._job_cache_lock = threading.Lock()
        if not hasattr(self, "_active_job_bundle_builds"):
            self._active_job_bundle_builds: dict[
                tuple[object, ...], _JobBundleBuildControl
            ] = {}
        if not hasattr(self, "_template_artifacts"):
            self._template_artifacts: CachedTemplateArtifacts | None = None
        if not hasattr(self, "_template_artifact_generation"):
            self._template_artifact_generation = int(
                getattr(self._template_artifacts, "generation", 0)
            )
        if not hasattr(self, "_job_bundle_cache"):
            self._job_bundle_cache: OrderedDict[
                tuple[object, ...], CachedJobBundle
            ] = OrderedDict()
        if not hasattr(self, "_payout_state_generation"):
            self._payout_state_generation = 0
        if not hasattr(self, "_payout_state_prepare_lock"):
            # Ledger mutations and the ledger reads used to build signed jobs
            # share this lock. They may be expensive, but they never block
            # delivery of an already-published immutable generation.
            self._payout_state_prepare_lock = threading.RLock()
        if not hasattr(self, "_payout_state_source"):
            self._payout_state_source: tuple[int, str | None, str, float] = (
                0,
                None,
                "startup",
                time.monotonic(),
            )
        if not hasattr(self, "_published_payout_state"):
            self._published_payout_state = PublishedPayoutState(
                generation=self._payout_state_generation,
                source_generation=0,
                source_tip_hash=None,
                published_monotonic=time.monotonic(),
            )
        if not hasattr(self, "_payout_ledger_artifact"):
            self._payout_ledger_artifact: PayoutLedgerArtifact | None = None
        if not hasattr(self, "_payout_ledger_artifact_generation"):
            self._payout_ledger_artifact_generation = 0
        if not hasattr(self, "_payout_artifact_executor_lock"):
            self._payout_artifact_executor_lock = threading.Lock()
        if not hasattr(self, "_payout_artifact_executor"):
            self._payout_artifact_executor: ThreadPoolExecutor | None = None
        if not hasattr(self, "_payout_artifact_future"):
            self._payout_artifact_future: Future[None] | None = None
        if not hasattr(self, "_payout_artifact_requested"):
            self._payout_artifact_requested: tuple[int, int] | None = None
        if not hasattr(self, "_payout_artifact_executor_shutdown"):
            self._payout_artifact_executor_shutdown = False
        if not hasattr(self, "_payout_state_delivery_gate"):
            # Orders reconciliation mutations against final job-delivery
            # admission while preserving parallel sends to different miners.
            self._payout_state_delivery_gate = _PayoutStateDeliveryGate()
        if not hasattr(self, "_payout_state_metrics_lock"):
            self._payout_state_metrics_lock = threading.Lock()
        if not hasattr(self, "payout_state_histograms"):
            self.payout_state_histograms = {
                name: {
                    "buckets": {
                        bucket: 0 for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                    },
                    "sum": 0.0,
                    "count": 0,
                }
                for name in ("preparation", "publish", "first_delivery")
            }
        if not hasattr(self, "payout_gate_wait_histograms"):
            self.payout_gate_wait_histograms = {
                relation: {
                    "buckets": {
                        bucket: 0 for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                    },
                    "sum": 0.0,
                    "count": 0,
                }
                for relation in PRISM_PAYOUT_DELIVERY_GENERATIONS
            }
        if not hasattr(self, "payout_state_candidates_discarded"):
            self.payout_state_candidates_discarded = 0
        if not hasattr(self, "_payout_first_delivery_pending"):
            self._payout_first_delivery_pending: tuple[int, float] | None = None
        if not hasattr(self, "_payout_state_publication_blocked"):
            self._payout_state_publication_blocked = False
        if not hasattr(self, "_job_build_phase_local"):
            self._job_build_phase_local = threading.local()
        if not hasattr(self, "job_cache_hit_counts"):
            self.job_cache_hit_counts = {kind: 0 for kind in PRISM_JOB_CACHE_KINDS}
        if not hasattr(self, "job_cache_miss_counts"):
            self.job_cache_miss_counts = {kind: 0 for kind in PRISM_JOB_CACHE_KINDS}
        if not hasattr(self, "job_build_seconds_bucket_counts"):
            self.job_build_seconds_bucket_counts = {
                bucket: 0 for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS
            }
        if not hasattr(self, "job_build_seconds_sum"):
            self.job_build_seconds_sum = 0.0
        if not hasattr(self, "job_build_count"):
            self.job_build_count = 0
        if not hasattr(self, "job_build_phase_seconds"):
            self.job_build_phase_seconds = {phase: 0.0 for phase in PRISM_JOB_BUILD_PHASES}
        if not hasattr(self, "_health_snapshot"):
            self._health_snapshot: dict[str, object] | None = None
        if not hasattr(self, "_health_snapshot_monotonic"):
            self._health_snapshot_monotonic: float | None = None
        if not hasattr(self, "_health_refresh_loop_running"):
            self._health_refresh_loop_running = False
        if not hasattr(self, "health_snapshot_refresh_failure_count"):
            self.health_snapshot_refresh_failure_count = 0
        if not hasattr(self, "_bundle_preparation_lock"):
            self._bundle_preparation_lock = threading.Lock()
        if not hasattr(self, "_bundle_preparation_flights"):
            self._bundle_preparation_flights: dict[
                tuple[object, ...], _SharedBundlePreparationFlight
            ] = {}
        if not hasattr(self, "shared_bundle_build_counts"):
            self.shared_bundle_build_counts = {
                outcome: 0
                for outcome in ("started", "completed", "superseded", "failed")
            }
        if not hasattr(self, "shared_bundle_preparation_seconds_sum"):
            self.shared_bundle_preparation_seconds_sum = 0.0
        if not hasattr(self, "shared_bundle_preparation_count"):
            self.shared_bundle_preparation_count = 0
        if not hasattr(self, "shared_bundle_preparation_waiters"):
            self.shared_bundle_preparation_waiters = 0
        if not hasattr(self, "_prepared_ready_bundle"):
            self._prepared_ready_bundle: CachedJobBundle | None = None
        if not hasattr(self, "_prepared_ready_snapshot"):
            self._prepared_ready_snapshot: QbitTipTemplateSnapshot | None = None
        if not hasattr(self, "job_preparation_pending"):
            self.job_preparation_pending = False

    def _job_build_phases(self) -> dict[str, float]:
        """Per-thread scratch dict of phase timings for the current build."""
        self._ensure_job_cache_state()
        phases = getattr(self._job_build_phase_local, "phases", None)
        if phases is None:
            phases = {}
            self._job_build_phase_local.phases = phases
        return phases

    def _cancel_obsolete_job_bundle_builds(
        self,
        *,
        current_tip: str | None = None,
        payout_state_generation: int | None = None,
    ) -> None:
        """Cancel only builds proven obsolete by a newer exact generation."""
        self._ensure_job_cache_state()
        processes: list[subprocess.Popen[str]] = []
        with self._job_cache_lock:
            for control in self._active_job_bundle_builds.values():
                obsolete = (
                    current_tip is not None
                    and control.previousblockhash != current_tip
                ) or (
                    payout_state_generation is not None
                    and control.payout_state_generation
                    != int(payout_state_generation)
                )
                if not obsolete or control.cancel_event.is_set():
                    continue
                control.cancel_event.set()
                if control.process is not None:
                    processes.append(control.process)
        for process in processes:
            if process.poll() is not None:
                continue
            try:
                process.terminate()
            except ProcessLookupError:
                pass

    def _register_job_bundle_process(
        self,
        control: _JobBundleBuildControl,
        process: subprocess.Popen[str],
    ) -> None:
        terminate = False
        with self._job_cache_lock:
            if (
                self._active_job_bundle_builds.get(control.key) is not control
                or control.cancel_event.is_set()
            ):
                terminate = True
            else:
                control.process = process
        if terminate and process.poll() is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass

    def _build_payout_ledger_artifact(
        self,
        expected_payout_state_generation: int,
        artifact_payout_state_generation: int,
        network_difficulty: int,
    ) -> PayoutLedgerArtifact | None:
        """Build a stable ledger snapshot without publishing it.

        Accepted-share counts fence both sides of the snapshot. If a writer
        commits concurrently, this attempt is discarded rather than publishing
        an artifact with an ambiguous cutoff; the normal inline path remains
        the fail-closed fallback.
        """
        self._ensure_job_cache_state()
        ledger_started = time.monotonic()
        try:
            accepted_before, _ = self.accepted_share_stats()
            with self._payout_state_prepare_lock:
                with self._job_cache_lock:
                    if (
                        expected_payout_state_generation
                        != self._payout_state_generation
                    ):
                        return None
                snapshot_window_weight = (
                    PRISM_REWARD_WINDOW_MULTIPLIER
                    * PRISM_SNAPSHOT_WINDOW_MARGIN
                    * int(network_difficulty)
                )
                records = list(
                    self.ledger.snapshot_at_job_issue(
                        now_ms(),
                        window_weight=snapshot_window_weight,
                    )
                )
                prior_balances = self.ledger.current_prior_balances()
            accepted_after, _ = self.accepted_share_stats()
        except Exception:
            # Artifact preparation is speculative. The synchronous bundle path
            # still owns errors when current work actually requires a snapshot.
            return None
        finally:
            self._observe_tip_refresh_build_phase(
                "ledger_snapshot",
                time.monotonic() - ledger_started,
            )
        if accepted_before != accepted_after or not records:
            return None
        copy_started = time.monotonic()
        shares_json = tuple(record.to_prism_json() for record in records)
        frozen_balances = tuple(prior_balances)
        self._observe_tip_refresh_build_phase(
            "serialization_copy",
            time.monotonic() - copy_started,
        )
        return PayoutLedgerArtifact(
            generation=0,
            payout_state_generation=artifact_payout_state_generation,
            network_difficulty=int(network_difficulty),
            accepted_share_count=accepted_after,
            shares_json=shares_json,
            prior_balances=frozen_balances,
            prepared_monotonic=time.monotonic(),
        )

    def _prepare_payout_ledger_artifact(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> None:
        """Prepare and atomically publish an artifact for a current generation."""
        artifact = self._build_payout_ledger_artifact(
            payout_state_generation,
            payout_state_generation,
            network_difficulty,
        )
        if artifact is None:
            return
        with self._job_cache_lock:
            if payout_state_generation != self._payout_state_generation:
                return
            self._payout_ledger_artifact_generation += 1
            self._payout_ledger_artifact = dataclass_replace(
                artifact,
                generation=self._payout_ledger_artifact_generation,
            )

    def _payout_artifact_preparation_loop(self) -> None:
        while True:
            with self._payout_artifact_executor_lock:
                request = self._payout_artifact_requested
                self._payout_artifact_requested = None
                if request is None:
                    self._payout_artifact_future = None
                    return
            self._prepare_payout_ledger_artifact(*request)

    def _schedule_payout_ledger_artifact_preparation(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> None:
        """Latest-generation-wins scheduling with one worker and one slot."""
        self._ensure_job_cache_state()
        with self._payout_artifact_executor_lock:
            if self._payout_artifact_executor_shutdown:
                return
            self._payout_artifact_requested = (
                int(payout_state_generation),
                int(network_difficulty),
            )
            if self._payout_artifact_future is not None:
                return
            executor = self._payout_artifact_executor
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="prism-payout-artifact",
                )
                self._payout_artifact_executor = executor
            self._payout_artifact_future = executor.submit(
                self._payout_artifact_preparation_loop
            )

    def _usable_payout_ledger_artifact(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> PayoutLedgerArtifact | None:
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            artifact = self._payout_ledger_artifact
        if (
            artifact is None
            or artifact.payout_state_generation != payout_state_generation
            or artifact.network_difficulty != int(network_difficulty)
        ):
            return None
        try:
            accepted_share_count, _ = self.accepted_share_stats()
        except Exception:
            return None
        if accepted_share_count != artifact.accepted_share_count:
            return None
        return artifact

    def shutdown_payout_artifact_executor(self) -> None:
        self._ensure_job_cache_state()
        with self._payout_artifact_executor_lock:
            executor = self._payout_artifact_executor
            self._payout_artifact_executor = None
            self._payout_artifact_executor_shutdown = True
            self._payout_artifact_requested = None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    def _record_job_cache_event(self, kind: str, *, hit: bool) -> None:
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            counts = self.job_cache_hit_counts if hit else self.job_cache_miss_counts
            counts[kind] = int(counts.get(kind, 0)) + 1

    def _job_bundle_payout_state_current(self, bundle: CachedJobBundle) -> bool:
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            return bundle.payout_state_generation == self._payout_state_generation

    def _observe_payout_state_seconds(
        self,
        name: str,
        elapsed_seconds: float,
        *,
        relation: str | None = None,
    ) -> None:
        self._ensure_job_cache_state()
        with self._payout_state_metrics_lock:
            if name == "gate_wait":
                if relation not in PRISM_PAYOUT_DELIVERY_GENERATIONS:
                    raise ValueError(f"unknown payout delivery generation: {relation}")
                histogram = self.payout_gate_wait_histograms[str(relation)]
            else:
                histogram = self.payout_state_histograms[name]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + elapsed_seconds
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def _observe_payout_gate_admission(
        self,
        admission: object,
        *,
        generation: int,
        fallback_wait_seconds: float,
    ) -> None:
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            published_generation = self._payout_state_generation
        relation = getattr(admission, "relation", None)
        if relation not in PRISM_PAYOUT_DELIVERY_GENERATIONS:
            relation = _PayoutStateDeliveryGate._generation_relation(
                generation,
                published_generation,
            )
        wait_seconds = float(
            getattr(admission, "wait_seconds", fallback_wait_seconds)
        )
        self._observe_payout_state_seconds(
            "gate_wait",
            max(0.0, wait_seconds),
            relation=relation,
        )

    def _reserve_payout_state_source(
        self,
        cause: str,
        *,
        tip_hash: str | None = None,
        invalidated_monotonic: float | None = None,
    ) -> int:
        self._ensure_job_cache_state()
        invalidated = (
            time.monotonic()
            if invalidated_monotonic is None
            else invalidated_monotonic
        )
        with self.lock:
            generation = self._payout_state_source[0] + 1
            self._payout_state_source = (
                generation,
                tip_hash,
                cause,
                invalidated,
            )
            return generation

    def _reserve_payout_state_source_if_current(
        self,
        expected_source_generation: int,
        cause: str,
        *,
        tip_hash: str | None = None,
        invalidated_monotonic: float | None = None,
    ) -> tuple[int, int, str | None, str, float] | None:
        """Reserve and capture a source only if preparation was not superseded."""

        self._ensure_job_cache_state()
        invalidated = (
            time.monotonic()
            if invalidated_monotonic is None
            else invalidated_monotonic
        )
        # Match publication's lock order so the returned base generation and
        # newly reserved source form one atomic candidate identity.
        with self._job_cache_lock:
            with self.lock:
                if self._payout_state_source[0] != expected_source_generation:
                    return None
                source_generation = expected_source_generation + 1
                self._payout_state_source = (
                    source_generation,
                    tip_hash,
                    cause,
                    invalidated,
                )
                return (
                    self._payout_state_generation,
                    source_generation,
                    tip_hash,
                    cause,
                    invalidated,
                )

    def _capture_payout_state_source(
        self,
    ) -> tuple[int, int, str | None, str, float]:
        self._ensure_job_cache_state()
        with self.lock:
            source_generation, source_tip, cause, invalidated = (
                self._payout_state_source
            )
        with self._job_cache_lock:
            base_generation = self._payout_state_generation
        return (
            base_generation,
            source_generation,
            source_tip,
            cause,
            invalidated,
        )

    def _prepared_payout_state_candidate(
        self,
        captured: tuple[int, int, str | None, str, float],
    ) -> PayoutStateCandidate:
        base_generation, source_generation, source_tip, cause, invalidated = captured
        ledger_artifact: PayoutLedgerArtifact | None = None
        with self._job_cache_lock:
            template_artifacts = self._template_artifacts
        if (
            template_artifacts is not None
            and getattr(self, "_pool_ready_latched", False)
        ):
            ledger_artifact = self._build_payout_ledger_artifact(
                base_generation,
                base_generation + 1,
                template_artifacts.network_difficulty,
            )
        return PayoutStateCandidate(
            base_generation=base_generation,
            source_generation=source_generation,
            source_tip_hash=source_tip,
            cause=cause,
            invalidated_monotonic=invalidated,
            prepared_monotonic=time.monotonic(),
            ledger_artifact=ledger_artifact,
        )

    def _current_payout_state_candidate(self) -> PayoutStateCandidate:
        return self._prepared_payout_state_candidate(
            self._capture_payout_state_source()
        )

    def _record_discarded_payout_candidate(self) -> None:
        self._ensure_job_cache_state()
        with self._payout_state_metrics_lock:
            self.payout_state_candidates_discarded += 1

    def _block_payout_state_publication(
        self,
        *,
        force: bool = False,
        supersede_with: tuple[int, str | None, str, float] | None = None,
    ) -> None:
        """Atomically close delivery, optionally reserving a newer source."""

        self._ensure_job_cache_state()

        pending_source: int | None = None

        def mark_blocked() -> bool:
            nonlocal pending_source
            with self._job_cache_lock:
                with self.lock:
                    if supersede_with is not None:
                        (
                            expected_source,
                            fallback_tip,
                            cause,
                            invalidated,
                        ) = supersede_with
                        current_source, current_tip, _, _ = (
                            self._payout_state_source
                        )
                        # A newer tip/source wins its identity, but it must be
                        # superseded so no candidate prepared before an
                        # uncertain durable commit can publish afterward.
                        source_tip = (
                            fallback_tip
                            if current_source == expected_source
                            else current_tip
                        )
                        pending_source = current_source + 1
                        self._payout_state_source = (
                            pending_source,
                            source_tip,
                            cause,
                            invalidated,
                        )
                    else:
                        pending_source = self._payout_state_source[0]
                    if (
                        not force
                        and supersede_with is None
                        and pending_source
                        == self._published_payout_state.source_generation
                    ):
                        return False
                    self._payout_state_publication_blocked = True
                    self._job_bundle_cache.clear()
                    return True

        # Close fleet admission atomically with the cache fence. Escaped
        # immutable bundles remain stamped with the old generation, but cannot
        # cross the boundary while the ledger has newer unpublished state.
        if not self._payout_state_delivery_gate.block_delivery(mark_blocked):
            return
        with self._job_cache_lock:
            next_payout_generation = self._payout_state_generation + 1
        with self.lock:
            active = getattr(self, "_active_tip_refresh", None)
        if (
            active is not None
            and active[0].payout_state_generation < next_payout_generation
        ):
            active[1].cancel()
        elif active is not None:
            return
        assert pending_source is not None
        self._mark_tip_refresh_pending(next_payout_generation)
        self._schedule_tip_refresh_retry()

    def _payout_source_requires_publication(
        self,
        candidate: PayoutStateCandidate | None = None,
    ) -> bool:
        """Report whether an invalidation source still lacks a publication."""

        self._ensure_job_cache_state()
        with self._job_cache_lock:
            published_source = self._published_payout_state.source_generation
            if candidate is not None:
                return candidate.source_generation != published_source
            with self.lock:
                return self._payout_state_source[0] != published_source

    def _publish_payout_state_candidate(
        self,
        candidate: PayoutStateCandidate,
    ) -> int | None:
        """Publish a prepared candidate, or reject it if its source moved."""

        self._ensure_job_cache_state()
        published_generation: int | None = None
        schedule_retry = False
        active_to_cancel: _FanoutCancellation | None = None
        publish_started = 0.0
        with self._job_cache_lock:
            with self.lock:
                if (
                    candidate.source_generation != self._payout_state_source[0]
                    or candidate.base_generation != self._payout_state_generation
                ):
                    self._record_discarded_payout_candidate()
                    return None
        with self._payout_state_delivery_gate.publication():
            # publication() has already drained admitted old sends. Start the
            # critical-section timer only now; drain latency is delivery wait,
            # not time spent holding the atomic payout mutation section.
            publish_started = time.monotonic()
            with self._job_cache_lock:
                with self.lock:
                    source_generation = self._payout_state_source[0]
                    if (
                        candidate.source_generation == source_generation
                        and candidate.base_generation
                        == self._payout_state_generation
                    ):
                        self._payout_state_generation += 1
                        published_generation = self._payout_state_generation
                        prepared_artifact = candidate.ledger_artifact
                        if (
                            prepared_artifact is not None
                            and prepared_artifact.payout_state_generation
                            == published_generation
                        ):
                            self._payout_ledger_artifact_generation += 1
                            self._payout_ledger_artifact = dataclass_replace(
                                prepared_artifact,
                                generation=self._payout_ledger_artifact_generation,
                            )
                        else:
                            self._payout_ledger_artifact = None
                        self._published_payout_state = PublishedPayoutState(
                            generation=published_generation,
                            source_generation=candidate.source_generation,
                            source_tip_hash=candidate.source_tip_hash,
                            published_monotonic=publish_started,
                        )
                        self._payout_state_publication_blocked = False
                        self._job_bundle_cache.clear()
                        self._retained_collection_refresh = None
                        active = getattr(self, "_active_tip_refresh", None)
                        if active is None:
                            schedule_retry = True
                        elif active[0].payout_state_generation < published_generation:
                            # The payout gate itself rejects this old generation.
                            # Signal its fanout only after atomic publication.
                            active_to_cancel = active[1]
                            schedule_retry = True
                        with self._payout_state_metrics_lock:
                            self._payout_first_delivery_pending = (
                                published_generation,
                                candidate.invalidated_monotonic,
                            )
            if published_generation is not None:
                # The mutation owner still blocks every delivery admission,
                # so the pointer swap and gate generation remain one atomic
                # publication boundary. Do not acquire the gate condition
                # while holding coordinator locks: cancellation callbacks take
                # those locks after entering the gate wait loop.
                self._payout_state_delivery_gate.publish_generation(
                    published_generation,
                    prioritize_delivery=True,
                )
        self._observe_payout_state_seconds(
            "publish",
            max(0.0, time.monotonic() - publish_started),
        )
        if published_generation is None:
            self._record_discarded_payout_candidate()
            return None
        self._cancel_obsolete_job_bundle_builds(
            payout_state_generation=published_generation
        )
        if active_to_cancel is not None:
            active_to_cancel.cancel()
        if schedule_retry:
            self._mark_tip_refresh_pending(published_generation)
            self._schedule_tip_refresh_retry()
        with self._job_cache_lock:
            current_artifacts = self._template_artifacts
        published_artifact_usable = (
            self._usable_payout_ledger_artifact(
                published_generation,
                current_artifacts.network_difficulty,
            )
            if current_artifacts is not None
            else None
        )
        if current_artifacts is not None and published_artifact_usable is None:
            self._schedule_payout_ledger_artifact_preparation(
                published_generation,
                current_artifacts.network_difficulty,
            )
        return published_generation

    def _record_first_payout_delivery(
        self,
        generation: int,
        delivered_monotonic: float,
    ) -> None:
        self._ensure_job_cache_state()
        elapsed: float | None = None
        with self._payout_state_metrics_lock:
            pending = self._payout_first_delivery_pending
            if pending is not None and pending[0] == generation:
                elapsed = max(0.0, delivered_monotonic - pending[1])
                self._payout_first_delivery_pending = None
        if elapsed is not None:
            self._observe_payout_state_seconds("first_delivery", elapsed)

    def _advance_payout_state_generation(self) -> int:
        """Publish a payout-only invalidation with no expensive gate work."""
        self._ensure_job_cache_state()
        self._reserve_payout_state_source("payout_only")
        prepared_started = time.monotonic()
        with self._payout_state_prepare_lock:
            # Close build/delivery admission before releasing snapshot readers.
            # Publication may then drain already-admitted sends without holding
            # the preparation lock needed by later ledger work.
            self._block_payout_state_publication(force=True)
            self._observe_payout_state_seconds(
                "preparation",
                max(0.0, time.monotonic() - prepared_started),
            )
        generation = self._publish_current_payout_state_with_retry_budget()
        if generation is None:
            raise TemplateRefreshSuperseded(
                "payout-only invalidation was superseded; immediate retry scheduled"
            )
        return generation

    def _publish_current_payout_state_with_retry_budget(
        self,
        *,
        initial_attempted: bool = False,
    ) -> int | None:
        """Publish the current source with a bounded supersession budget."""

        max_retries = max(
            0,
            int(
                getattr(
                    self,
                    "payout_reconcile_supersession_retries",
                    DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
                )
            ),
        )
        attempts = max_retries + (0 if initial_attempted else 1)
        for _attempt in range(attempts):
            candidate = self._current_payout_state_candidate()
            published = self._publish_payout_state_candidate(candidate)
            if published is not None:
                return published
        self._block_payout_state_publication()
        return None

    def observe_job_build_elapsed(self, elapsed_seconds: float, phases: dict[str, float]) -> None:
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            self.job_build_count += 1
            self.job_build_seconds_sum += elapsed_seconds
            for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    self.job_build_seconds_bucket_counts[bucket] += 1
            for phase, duration in phases.items():
                if phase in self.job_build_phase_seconds:
                    self.job_build_phase_seconds[phase] += duration

    def _reserve_template_artifact_generation(self) -> int:
        """Reserve template ordering when a fetch starts, not when it finishes."""
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            self._template_artifact_generation += 1
            return self._template_artifact_generation

    def _derive_template_artifacts(
        self,
        template: dict[str, Any],
        *,
        generation: int,
    ) -> CachedTemplateArtifacts:
        # Detach the observation from mutable RPC/test caller state. The
        # dataclass then owns this exact tree for the lifetime of its snapshot.
        template = copy.deepcopy(template)
        fingerprint = qbit_template_fingerprint(template)
        with self._job_cache_lock:
            previous = self._template_artifacts
        if previous is not None and previous.fingerprint == fingerprint:
            return CachedTemplateArtifacts(
                template=template,
                fingerprint=fingerprint,
                previousblockhash=str(template.get("previousblockhash", "")),
                transaction_hexes=previous.transaction_hexes,
                witness_merkle_leaves_hex=previous.witness_merkle_leaves_hex,
                network_difficulty=previous.network_difficulty,
                fetched_monotonic=time.monotonic(),
                generation=generation,
            )
        phases = self._job_build_phases()
        started = time.monotonic()
        transaction_hexes = direct_stratum.transaction_hexes_from_template(template)
        witness_leaves = tuple(direct_stratum.witness_merkle_leaves_hex(transaction_hexes))
        network_difficulty = scaled_network_difficulty(str(template["bits"]))
        phases["merkle"] = phases.get("merkle", 0.0) + (time.monotonic() - started)
        return CachedTemplateArtifacts(
            template=template,
            fingerprint=fingerprint,
            previousblockhash=str(template.get("previousblockhash", "")),
            transaction_hexes=transaction_hexes,
            witness_merkle_leaves_hex=witness_leaves,
            network_difficulty=network_difficulty,
            fetched_monotonic=time.monotonic(),
            generation=generation,
        )

    def _store_template_artifacts(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        with self._job_cache_lock:
            previous = self._template_artifacts
            if previous is not None and artifacts.generation < previous.generation:
                return False
            self._template_artifacts = artifacts
            if previous is not None and previous.fingerprint != artifacts.fingerprint:
                self._job_bundle_cache = OrderedDict(
                    (key, entry)
                    for key, entry in self._job_bundle_cache.items()
                    if entry.template_fingerprint == artifacts.fingerprint
                )
            return True

    def store_template_artifacts(
        self,
        template: dict[str, Any],
        *,
        generation: int | None = None,
    ) -> CachedTemplateArtifacts | None:
        """Best-effort cache fill from an already-fetched template (blockpoll).

        Returns None instead of raising so a template the derivation cannot
        digest degrades to the legacy per-build fetch path rather than failing
        the poll. The returned artifacts describe this exact observation even
        if a newer observation already won the cache-write race; blockpoll then
        detects the mismatch before fanout.
        """
        self._ensure_job_cache_state()
        if generation is None:
            generation = self._reserve_template_artifact_generation()
        try:
            artifacts = self._derive_template_artifacts(
                template,
                generation=generation,
            )
        except Exception:
            return None
        self._store_template_artifacts(artifacts)
        return artifacts

    def current_template_artifacts(self) -> CachedTemplateArtifacts:
        """Return fresh template artifacts, fetching a template on cache miss."""
        self._ensure_job_cache_state()
        ttl = getattr(self, "template_cache_seconds", DEFAULT_PRISM_BLOCKPOLL_SECONDS)
        now = time.monotonic()
        with self._job_cache_lock:
            cached = self._template_artifacts
        if cached is not None and ttl > 0 and now - cached.fetched_monotonic <= ttl:
            self._record_job_cache_event("template", hit=True)
            return cached
        self._record_job_cache_event("template", hit=False)
        generation = self._reserve_template_artifact_generation()
        phases = self._job_build_phases()
        started = time.monotonic()
        template = self.rpc.call(
            "getblocktemplate",
            [{"rules": qbit_gbt_rules(getattr(self, "qbit_chain", "regtest"))}],
        )
        if not isinstance(template, dict):
            raise RuntimeError("getblocktemplate returned non-object")
        phases["template"] = phases.get("template", 0.0) + (time.monotonic() - started)
        artifacts = self._derive_template_artifacts(
            template,
            generation=generation,
        )
        if self._store_template_artifacts(artifacts):
            return artifacts
        # A later fetch completed first. Build from that current observation,
        # never from the stale response that lost the cache-write race.
        with self._job_cache_lock:
            current = self._template_artifacts
        if current is None:
            raise RuntimeError("newer template artifacts disappeared after cache race")
        return current

    @staticmethod
    def _collection_bundle_identity(worker: WorkerIdentity) -> tuple[str, str]:
        return worker.payout_address, worker.p2mr_program_hex

    def _job_bundle_key(
        self,
        artifacts: CachedTemplateArtifacts,
        *,
        mode: str,
        payout_state_generation: int,
        payout_artifact_generation: int,
        worker: WorkerIdentity | None,
    ) -> tuple[object, ...]:
        if mode == "ready":
            return (
                artifacts.fingerprint,
                artifacts.previousblockhash,
                "ready",
                payout_state_generation,
                payout_artifact_generation,
            )
        if mode != "collection":
            raise ValueError(f"unknown PRISM job-bundle mode: {mode}")
        if worker is None:
            raise CollectionIdentityUnavailable(
                "collection-mode worker identity is temporarily unavailable"
            )
        return (
            artifacts.fingerprint,
            artifacts.previousblockhash,
            "collection",
            artifacts.generation,
            payout_state_generation,
            payout_artifact_generation,
            *self._collection_bundle_identity(worker),
        )

    def _job_bundle_mode(self, requested_mode: str | None) -> str:
        if requested_mode is not None:
            if requested_mode not in {"ready", "collection"}:
                raise ValueError(
                    f"unknown PRISM job-bundle mode: {requested_mode}"
                )
            return requested_mode
        return "ready" if self.pool_readiness_latched() else "collection"

    def _lookup_job_bundle(
        self,
        key: tuple[object, ...],
    ) -> CachedJobBundle | None:
        ttl = getattr(self, "job_bundle_cache_seconds", DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS)
        now = time.monotonic()
        with self._job_cache_lock:
            if ttl <= 0:
                self._job_bundle_cache.clear()
                return None
            # The entry-count cap is not a memory bound: one production entry
            # can reference more than 100k shares.  Expired entries must release
            # their snapshots instead of remaining resident until count eviction.
            expired = [
                cache_key
                for cache_key, entry in self._job_bundle_cache.items()
                if now - entry.built_monotonic > ttl
            ]
            for cache_key in expired:
                self._job_bundle_cache.pop(cache_key, None)
            return self._job_bundle_cache.get(key)
        return None

    def _job_bundle_entry_usable(
        self,
        cached: CachedJobBundle | None,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        """Re-validate readiness for cached collection bundles.

        Readiness is monotonic in practice (the distinct accepted-miner count
        only grows), so submit-capable ready bundles are served as-is. A cached
        collection bundle is re-checked against the cheap aggregate stats:
        once the pool is ready it must stop being served, or jobs would keep
        collecting winning shares without submitting blocks for up to the cache
        TTL.
        """
        if cached is None:
            return False
        with self._job_cache_lock:
            if self._payout_state_publication_blocked:
                return False
        if not self._job_bundle_payout_state_current(cached):
            return False
        if not cached.collection_only:
            return True
        # Collection bundles sign a synthetic bootstrap share containing the
        # exact template ntime. A clock-only observation keeps the stable work
        # fingerprint, but it must rebuild this signed bundle instead of
        # rebinding the old manifest to a new template generation.
        if (
            cached.template is not artifacts.template
            or cached.template_generation != artifacts.generation
        ):
            return False
        try:
            _, ready_miner_count = self.accepted_share_stats()
        except Exception:
            # If readiness cannot be proven, force the normal build path. That
            # path will either build an up-to-date bundle or surface the ledger
            # failure instead of continuing to issue no-submit collection jobs.
            return False
        return ready_miner_count < self.min_ready_miners

    def _bind_cached_bundle_to_artifacts(
        self,
        cached: CachedJobBundle,
        artifacts: CachedTemplateArtifacts,
    ) -> CachedJobBundle:
        """Return the cached heavy bundle bound to this exact observation.

        Clock-only template changes intentionally keep the stable fingerprint.
        Ready bundles may reuse their ledger snapshot and signed manifest, but
        the Stratum base job must still carry the observing template's exact
        ntime and generation. Collection bundles are filtered before this point
        because their signed synthetic share contains the template ntime.
        """
        if (
            cached.template is artifacts.template
            and cached.template_generation == artifacts.generation
        ):
            return cached
        manifest = cached.coinbase_manifest
        base_job = direct_stratum.make_job_from_builder_manifest(
            job_id="prism-template-base",
            template=artifacts.template,
            manifest=manifest,
            extranonce1_hex=PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
            extranonce2_size=self.extranonce2_size,
            desired_share_difficulty=self.share_difficulty,
            clean_jobs=True,
            transaction_hexes=artifacts.transaction_hexes,
        )
        return dataclass_replace(
            cached,
            template=artifacts.template,
            base_job=base_job,
            template_generation=artifacts.generation,
        )

    def _cache_job_bundle_if_current(
        self,
        built: CachedJobBundle,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        """Cache only current state; report whether payout state stayed valid."""
        with self._job_cache_lock:
            if built.payout_state_generation != self._payout_state_generation:
                return False
            current = self._template_artifacts
            if (
                current is None
                or current.fingerprint != artifacts.fingerprint
                or current.generation != artifacts.generation
            ):
                # Snapshot-owned artifacts remain usable even if a newer
                # template won the global cache race; just do not retain them.
                return True
            self._job_bundle_cache[built.key] = built
            self._job_bundle_cache.move_to_end(built.key)
            while len(self._job_bundle_cache) > MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES:
                oldest_key = next(iter(self._job_bundle_cache))
                self._job_bundle_cache.pop(oldest_key, None)
            return True

    def shared_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentity | None = None,
        *,
        mode: str | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> CachedJobBundle:
        """Return one immutable heavy build through a work-identity flight.

        Ready-pool work is deliberately worker-independent. Collection work
        retains the payout identity in both the flight and cache keys. Mode is
        resolved before selecting the flight, so readiness and payout-state
        transitions cannot reuse an obsolete preparation.
        """
        self._ensure_job_cache_state()
        self._ensure_tip_refresh_state()
        while True:
            resolved_mode = self._job_bundle_mode(mode)
            if resolved_mode == "collection" and worker is None:
                raise CollectionIdentityUnavailable(
                    "collection-mode worker identity is temporarily unavailable"
                )
            with self._job_cache_lock:
                payout_state_generation = self._payout_state_generation
            payout_artifact = (
                self._usable_payout_ledger_artifact(
                    payout_state_generation,
                    artifacts.network_difficulty,
                )
                if resolved_mode == "ready"
                else None
            )
            payout_artifact_generation = (
                payout_artifact.generation if payout_artifact is not None else 0
            )
            key = self._job_bundle_key(
                artifacts,
                mode=resolved_mode,
                payout_state_generation=payout_state_generation,
                payout_artifact_generation=payout_artifact_generation,
                worker=worker,
            )
            cached = self._lookup_job_bundle(key)
            if self._job_bundle_entry_usable(cached, artifacts):
                self._record_job_cache_event("bundle", hit=True)
                assert cached is not None
                return self._bind_cached_bundle_to_artifacts(cached, artifacts)
            flight_key = (
                artifacts.previousblockhash,
                artifacts.fingerprint,
                artifacts.generation,
                *key,
            )
            with self._bundle_preparation_lock:
                flight = self._bundle_preparation_flights.get(flight_key)
                leader = flight is None
                if flight is None:
                    flight = _SharedBundlePreparationFlight()
                    self._bundle_preparation_flights[flight_key] = flight
                    self.shared_bundle_build_counts["started"] += 1
                else:
                    flight.waiters += 1
                    self.shared_bundle_preparation_waiters += 1
                    with self._tip_refresh_metrics_lock:
                        self.tip_refresh_build_queue_depth += 1

            if not leader:
                wait_started = time.monotonic()
                try:
                    while not flight.event.wait(timeout=0.1):
                        if cancelled is not None and cancelled():
                            raise _JobBuildCancelled(
                                "job bundle waiter was superseded during preparation"
                            )
                finally:
                    wait_elapsed = time.monotonic() - wait_started
                    with self._bundle_preparation_lock:
                        self.shared_bundle_preparation_waiters = max(
                            0, self.shared_bundle_preparation_waiters - 1
                        )
                    with self._tip_refresh_metrics_lock:
                        self.tip_refresh_build_queue_depth = max(
                            0, self.tip_refresh_build_queue_depth - 1
                        )
                    self._observe_tip_refresh_build_phase(
                        "singleflight_wait",
                        wait_elapsed,
                    )
                self._job_build_phases()["preparation_wait"] = (
                    self._job_build_phases().get("preparation_wait", 0.0)
                    + wait_elapsed
                )
                if self._job_bundle_entry_usable(flight.result, artifacts):
                    self._record_job_cache_event("bundle", hit=True)
                    with self._tip_refresh_metrics_lock:
                        self.tip_refresh_singleflight_hits += 1
                    assert flight.result is not None
                    return self._bind_cached_bundle_to_artifacts(
                        flight.result,
                        artifacts,
                    )
                if flight.result is not None:
                    continue
                if isinstance(flight.error, _BundlePreparationSuperseded):
                    raise flight.error
                if isinstance(flight.error, _PayoutStatePublicationBlocked):
                    raise flight.error
                if isinstance(flight.error, TemplateRefreshBlocked):
                    continue
                if flight.error is not None:
                    raise RuntimeError("shared job bundle preparation failed") from flight.error
                raise RuntimeError("shared job bundle flight completed without a result")

            build_started = time.monotonic()
            try:
                with self._job_cache_lock:
                    current_payout_generation = self._payout_state_generation
                if current_payout_generation != payout_state_generation:
                    raise TemplateRefreshBlocked(
                        "payout state superseded shared bundle preparation"
                    )
                if self._job_bundle_mode(mode) != resolved_mode:
                    raise TemplateRefreshBlocked(
                        "readiness superseded shared bundle preparation"
                    )
                # A flight owns this exact work identity. Different identities
                # need not queue behind an obsolete global build lock.
                self._record_job_cache_event("bundle", hit=False)
                control = _JobBundleBuildControl(
                    key=flight_key,
                    previousblockhash=artifacts.previousblockhash,
                    payout_state_generation=payout_state_generation,
                    payout_artifact_generation=payout_artifact_generation,
                )
                with self._job_cache_lock:
                    self._active_job_bundle_builds[flight_key] = control
                previous_control = getattr(
                    self._job_build_phase_local,
                    "bundle_build_control",
                    None,
                )
                self._job_build_phase_local.bundle_build_control = control
                with self._tip_refresh_metrics_lock:
                    self.tip_refresh_build_inflight += 1
                try:
                    if control.cancel_event.is_set():
                        raise _JobBundleBuildSuperseded(
                            "shared bundle build was superseded before execution"
                        )
                    built = self.build_shared_job_bundle(
                        artifacts,
                        worker,
                        mode=resolved_mode,
                        payout_state_generation=payout_state_generation,
                        payout_artifact=payout_artifact,
                        key=key,
                    )
                    if control.cancel_event.is_set():
                        raise _JobBundleBuildSuperseded(
                            "shared bundle result was superseded before publication"
                        )
                except _JobBundleBuildSuperseded as exc:
                    with self._tip_refresh_metrics_lock:
                        self.tip_refresh_superseded_results += 1
                    with self._job_cache_lock:
                        current_payout_generation = self._payout_state_generation
                    with self.lock:
                        current_tip = getattr(self, "current_tip_first_seen", None)
                    if current_payout_generation != payout_state_generation and (
                        current_tip is None
                        or current_tip[0] == artifacts.previousblockhash
                    ):
                        raise TemplateRefreshBlocked(
                            "payout generation superseded shared bundle preparation"
                        ) from exc
                    raise _BundlePreparationSuperseded(
                        "newer tip superseded shared bundle preparation"
                    ) from exc
                finally:
                    self._job_build_phase_local.bundle_build_control = previous_control
                    with self._job_cache_lock:
                        if self._active_job_bundle_builds.get(flight_key) is control:
                            self._active_job_bundle_builds.pop(flight_key, None)
                        control.process = None
                    with self._tip_refresh_metrics_lock:
                        self.tip_refresh_build_inflight = max(
                            0, self.tip_refresh_build_inflight - 1
                        )
                if payout_artifact is not None and (
                    self._usable_payout_ledger_artifact(
                        payout_state_generation,
                        artifacts.network_difficulty,
                    )
                    is not payout_artifact
                ):
                    with self._tip_refresh_metrics_lock:
                        self.tip_refresh_superseded_results += 1
                    raise TemplateRefreshBlocked(
                        "payout artifact superseded shared bundle preparation"
                    )
                # A reconciliation may have committed while this expensive
                # build was running. Never cache or return its stale signed
                # payout snapshot; retry against the new generation instead.
                with self.lock:
                    observed_tip = getattr(self, "current_tip_first_seen", None)
                    published_snapshot = self.tip_template_snapshot
                if observed_tip is not None and observed_tip[0] != artifacts.previousblockhash:
                    with self._tip_refresh_metrics_lock:
                        self.tip_refresh_superseded_results += 1
                    raise _BundlePreparationSuperseded(
                        "newer tip superseded shared bundle preparation"
                    )
                if (
                    published_snapshot is not None
                    and published_snapshot.bestblockhash == artifacts.previousblockhash
                    and published_snapshot.template_generation > artifacts.generation
                ):
                    with self._tip_refresh_metrics_lock:
                        self.tip_refresh_superseded_results += 1
                    raise _BundlePreparationSuperseded(
                        "newer template superseded shared bundle preparation"
                    )
                if self._job_bundle_mode(mode) != resolved_mode:
                    raise TemplateRefreshBlocked(
                        "readiness superseded shared bundle preparation"
                    )
                if not self._cache_job_bundle_if_current(built, artifacts):
                    with self._tip_refresh_metrics_lock:
                        self.tip_refresh_superseded_results += 1
                    raise TemplateRefreshBlocked(
                        "payout state superseded shared bundle preparation"
                    )
                flight.result = built
                with self._bundle_preparation_lock:
                    self.shared_bundle_build_counts["completed"] += 1
                if cancelled is not None and cancelled():
                    raise _JobBuildCancelled(
                        "job bundle request was superseded during preparation"
                    )
                return built
            except _BundlePreparationSuperseded as exc:
                flight.error = exc
                with self._bundle_preparation_lock:
                    self.shared_bundle_build_counts["superseded"] += 1
                raise
            except _PayoutStatePublicationBlocked as exc:
                flight.error = exc
                with self._bundle_preparation_lock:
                    self.shared_bundle_build_counts["superseded"] += 1
                raise
            except TemplateRefreshBlocked as exc:
                flight.error = exc
                with self._bundle_preparation_lock:
                    self.shared_bundle_build_counts["superseded"] += 1
                continue
            except _JobBuildCancelled:
                # The completed shared result remains available to other
                # waiters even though this request no longer needs it.
                raise
            except BaseException as exc:
                flight.error = exc
                with self._bundle_preparation_lock:
                    self.shared_bundle_build_counts["failed"] += 1
                raise
            finally:
                elapsed = time.monotonic() - build_started
                with self._bundle_preparation_lock:
                    self.shared_bundle_preparation_seconds_sum += elapsed
                    self.shared_bundle_preparation_count += 1
                    if self._bundle_preparation_flights.get(flight_key) is flight:
                        self._bundle_preparation_flights.pop(flight_key, None)
                    flight.event.set()

    def build_shared_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentity | None = None,
        *,
        mode: str | None = None,
        payout_state_generation: int | None = None,
        payout_artifact: PayoutLedgerArtifact | None = None,
        key: tuple[object, ...] | None = None,
    ) -> CachedJobBundle:
        phases = self._job_build_phases()
        template = artifacts.template
        resolved_mode = self._job_bundle_mode(mode)
        if resolved_mode == "collection" and worker is None:
            raise CollectionIdentityUnavailable(
                "collection-mode worker identity is temporarily unavailable"
            )
        started = time.monotonic()
        share_records: list[object] = []
        with self._payout_state_prepare_lock:
            with self._job_cache_lock:
                publication_blocked = self._payout_state_publication_blocked
            if publication_blocked:
                raise _PayoutStatePublicationBlocked(
                    "payout state invalidation is pending publication"
                )
            if payout_state_generation is None:
                with self._job_cache_lock:
                    payout_state_generation = self._payout_state_generation
            if key is None:
                if payout_artifact is None and resolved_mode == "ready":
                    payout_artifact = self._usable_payout_ledger_artifact(
                        payout_state_generation,
                        artifacts.network_difficulty,
                    )
                key = self._job_bundle_key(
                    artifacts,
                    mode=resolved_mode,
                    payout_state_generation=payout_state_generation,
                    payout_artifact_generation=(
                        payout_artifact.generation
                        if payout_artifact is not None
                        else 0
                    ),
                    worker=worker,
                )
            issued_at_ms = now_ms()
            # Bound the snapshot to a superset of the 8x reward window rather
            # than the whole accepted history: same audit bundle and digest,
            # but the ledger phase no longer scales with total ledger size.
            snapshot_window_weight = (
                PRISM_REWARD_WINDOW_MULTIPLIER
                * PRISM_SNAPSHOT_WINDOW_MARGIN
                * int(artifacts.network_difficulty)
            )
            if resolved_mode == "ready" and payout_artifact is None:
                share_records = list(
                    self.ledger.snapshot_at_job_issue(
                        issued_at_ms, window_weight=snapshot_window_weight
                    )
                )
            prior_balances = (
                list(payout_artifact.prior_balances)
                if payout_artifact is not None
                else self.ledger.current_prior_balances()
            )
        ledger_elapsed = time.monotonic() - started
        phases["ledger"] = phases.get("ledger", 0.0) + ledger_elapsed
        if resolved_mode == "ready":
            self._observe_tip_refresh_build_phase("ledger_snapshot", ledger_elapsed)
        copy_started = time.monotonic()
        shares: list[dict[str, object]] = (
            list(payout_artifact.shares_json)
            if payout_artifact is not None
            else [
                record.to_prism_json()  # type: ignore[union-attr]
                for record in share_records
            ]
        )
        if resolved_mode == "ready":
            self._observe_tip_refresh_build_phase(
                "serialization_copy",
                time.monotonic() - copy_started,
            )
        started = time.monotonic()
        placeholder_suffix_hex = self.coinbase_script_sig_suffix_hex(
            PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
            "00" * self.extranonce2_size,
        )
        collection_identity: tuple[str, str] | None = None
        previous_metrics_scope = bool(
            getattr(self._job_build_phase_local, "tip_refresh_metrics", False)
        )
        self._job_build_phase_local.tip_refresh_metrics = resolved_mode == "ready"
        try:
            if resolved_mode == "ready":
                if not shares:
                    raise RuntimeError(
                        "ready-pool ledger snapshot contained no payout shares"
                    )
                bundle = self.build_audit_bundle(
                    shares=shares,
                    found_block={
                        "block_height": int(template["height"]),
                        "coinbase_value_sats": int(template["coinbasevalue"]),
                        "network_difficulty": artifacts.network_difficulty,
                        "anchor_job_issued_at_ms": issued_at_ms,
                    },
                    prior_balances=prior_balances,
                    coinbase_script_sig_suffix_hex=placeholder_suffix_hex,
                    witness_merkle_leaves_hex=list(artifacts.witness_merkle_leaves_hex),
                    ctv_fee_parent_hash=str(template["previousblockhash"]),
                    summary_only=True,
                )
                collection_only = False
            else:
                assert worker is not None
                bundle = self.build_collection_bundle(
                    template=template,
                    transaction_hexes=artifacts.transaction_hexes,
                    worker=worker,
                    network_difficulty=artifacts.network_difficulty,
                    issued_at_ms=issued_at_ms,
                    suffix_hex=placeholder_suffix_hex,
                    summary_only=True,
                )
                shares = []
                collection_only = True
                collection_identity = self._collection_bundle_identity(worker)
        finally:
            self._job_build_phase_local.tip_refresh_metrics = previous_metrics_scope
        manifest = bundle["signed_coinbase_manifest"]["manifest"]
        base_job = direct_stratum.make_job_from_builder_manifest(
            job_id="prism-template-base",
            template=template,
            manifest=manifest,
            extranonce1_hex=PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
            extranonce2_size=self.extranonce2_size,
            desired_share_difficulty=self.share_difficulty,
            clean_jobs=True,
            transaction_hexes=artifacts.transaction_hexes,
        )
        phases["bundle"] = phases.get("bundle", 0.0) + (time.monotonic() - started)
        return CachedJobBundle(
            key=key,
            template=template,
            template_fingerprint=artifacts.fingerprint,
            # Only this manifest is needed to bind later clock-only template
            # observations.  Retaining the returned logical bundle duplicated
            # the entire shares tree already held in shares_json.
            coinbase_manifest=manifest,
            shares_json=shares,
            prior_balances=prior_balances,
            found_block=bundle["found_block"],
            collection_only=collection_only,
            issued_at_ms=issued_at_ms,
            base_job=base_job,
            built_monotonic=time.monotonic(),
            template_generation=artifacts.generation,
            payout_state_generation=payout_state_generation,
            payout_artifact_generation=(
                payout_artifact.generation if payout_artifact is not None else 0
            ),
            collection_identity=collection_identity,
        )

    def stamp_job_for_client(
        self,
        client: ClientState,
        cached: CachedJobBundle,
        *,
        clean_jobs: bool,
    ) -> PrismJobContext:
        if client.worker is None:
            raise StratumError(20, "client is not authorized")
        if cached.collection_only and cached.collection_identity != (
            self._collection_bundle_identity(client.worker)
        ):
            raise StratumError(
                20,
                "collection bundle payout identity no longer matches client authorization",
            )
        with self.lock:
            self.job_counter += 1
            job_id = f"prism-{self.job_counter}"
        share_target = direct_stratum.effective_share_target(
            self.desired_client_share_difficulty(client),
            cached.base_job.qbit_target,
            minimum_advertised_difficulty=self.client_minimum_advertised_difficulty(client),
        )
        job = dataclass_replace(
            cached.base_job,
            job_id=job_id,
            extranonce1_hex=client.extranonce1_hex,
            share_target=share_target,
            share_difficulty=direct_stratum.target_difficulty(share_target),
            clean_jobs=clean_jobs,
        )
        return PrismJobContext(
            job=job,
            template=cached.template,
            shares_json=cached.shares_json,
            prior_balances=cached.prior_balances,
            found_block=cached.found_block,
            share_weight=self.share_weight_for_worker(client.worker),
            collection_only=cached.collection_only,
            worker=client.worker,
            issued_at_ms=cached.issued_at_ms,
            template_fingerprint=cached.template_fingerprint,
            template_generation=cached.template_generation,
            payout_state_generation=cached.payout_state_generation,
            payout_artifact_generation=cached.payout_artifact_generation,
            connection_id=client.connection_id,
            authorization_generation=int(
                getattr(client, "authorization_generation", 0)
            ),
            difficulty_generation=int(
                getattr(client, "difficulty_generation", 0)
            ),
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

    def _ensure_ctv_broadcaster_metrics_state(self) -> None:
        if not hasattr(self, "_ctv_broadcaster_metrics_lock"):
            self._ctv_broadcaster_metrics_lock = threading.Lock()
        if not hasattr(self, "ctv_broadcaster_pass_seconds_bucket_counts"):
            self.ctv_broadcaster_pass_seconds_bucket_counts = {
                bucket: 0 for bucket in PRISM_CTV_BROADCASTER_SECONDS_BUCKETS
            }
        if not hasattr(self, "ctv_broadcaster_pass_seconds_sum"):
            self.ctv_broadcaster_pass_seconds_sum = 0.0
        if not hasattr(self, "ctv_broadcaster_pass_count"):
            self.ctv_broadcaster_pass_count = 0
        if not hasattr(self, "ctv_broadcaster_processed_rows_total"):
            self.ctv_broadcaster_processed_rows_total = 0
        if not hasattr(self, "ctv_broadcaster_yielded_total"):
            self.ctv_broadcaster_yielded_total = 0
        if not hasattr(self, "ctv_broadcaster_chunk_seconds_bucket_counts"):
            self.ctv_broadcaster_chunk_seconds_bucket_counts = {
                bucket: 0 for bucket in PRISM_CTV_BROADCASTER_CHUNK_SECONDS_BUCKETS
            }
        if not hasattr(self, "ctv_broadcaster_chunk_rows_bucket_counts"):
            self.ctv_broadcaster_chunk_rows_bucket_counts = {
                bucket: 0 for bucket in PRISM_CTV_BROADCASTER_CHUNK_ROWS_BUCKETS
            }
        if not hasattr(self, "ctv_broadcaster_chunk_seconds_sum"):
            self.ctv_broadcaster_chunk_seconds_sum = 0.0
        if not hasattr(self, "ctv_broadcaster_chunk_rows_sum"):
            self.ctv_broadcaster_chunk_rows_sum = 0
        if not hasattr(self, "ctv_broadcaster_chunk_count"):
            self.ctv_broadcaster_chunk_count = 0

    def _record_ctv_fanout_broadcaster_progress(self) -> None:
        self._record_heartbeat("ctv_fanout_broadcaster")
        self._ensure_ctv_broadcaster_metrics_state()
        with self._ctv_broadcaster_metrics_lock:
            self.ctv_broadcaster_processed_rows_total += 1

    def observe_ctv_fanout_broadcaster_pass(self, elapsed_seconds: float) -> None:
        self._ensure_ctv_broadcaster_metrics_state()
        with self._ctv_broadcaster_metrics_lock:
            self.ctv_broadcaster_pass_count += 1
            self.ctv_broadcaster_pass_seconds_sum += elapsed_seconds
            for bucket in PRISM_CTV_BROADCASTER_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    self.ctv_broadcaster_pass_seconds_bucket_counts[bucket] += 1

    def observe_ctv_fanout_broadcaster_chunk(
        self,
        result: CtvFanoutChunkResult,
    ) -> None:
        self._record_heartbeat("ctv_fanout_broadcaster")
        self._ensure_ctv_broadcaster_metrics_state()
        with self._ctv_broadcaster_metrics_lock:
            self.ctv_broadcaster_chunk_count += 1
            self.ctv_broadcaster_chunk_seconds_sum += result.elapsed_seconds
            self.ctv_broadcaster_chunk_rows_sum += result.processed_count
            for bucket in PRISM_CTV_BROADCASTER_CHUNK_SECONDS_BUCKETS:
                if result.elapsed_seconds <= bucket:
                    self.ctv_broadcaster_chunk_seconds_bucket_counts[bucket] += 1
            for bucket in PRISM_CTV_BROADCASTER_CHUNK_ROWS_BUCKETS:
                if result.processed_count <= bucket:
                    self.ctv_broadcaster_chunk_rows_bucket_counts[bucket] += 1

    def _record_ctv_fanout_broadcaster_yield(self) -> None:
        self._ensure_ctv_broadcaster_metrics_state()
        with self._ctv_broadcaster_metrics_lock:
            self.ctv_broadcaster_yielded_total += 1

    def _ensure_worker_metrics_state(self) -> None:
        if not hasattr(self, "worker_metrics_lock"):
            self.worker_metrics_lock = threading.Lock()
        if not hasattr(self, "worker_share_counts"):
            self.worker_share_counts = {}
        if not hasattr(self, "worker_rejection_counts"):
            self.worker_rejection_counts = {}

    def _ensure_initial_job_state(self) -> None:
        if not hasattr(self, "pending_initial_jobs"):
            self.pending_initial_jobs: dict[ClientState, PendingInitialJob] = {}
        if not hasattr(self, "stratum_max_pending_initial_jobs"):
            self.stratum_max_pending_initial_jobs = (
                DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS
            )
        if not hasattr(self, "stratum_initial_job_timeout_seconds"):
            self.stratum_initial_job_timeout_seconds = (
                DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS
            )
        if not hasattr(self, "initial_job_queue_rejection_count"):
            self.initial_job_queue_rejection_count = 0
        if not hasattr(self, "initial_job_timeout_count"):
            self.initial_job_timeout_count = 0
        if not hasattr(self, "initial_job_cancelled_count"):
            self.initial_job_cancelled_count = 0
        if not hasattr(self, "initial_job_coalesced_count"):
            self.initial_job_coalesced_count = 0
        if not hasattr(self, "initial_job_sent_count"):
            self.initial_job_sent_count = 0
        if not hasattr(self, "initial_job_failed_count"):
            self.initial_job_failed_count = 0
        if not hasattr(self, "initial_job_superseded_count"):
            self.initial_job_superseded_count = 0
        if not hasattr(self, "initial_job_delivery_latency_seconds_sum"):
            self.initial_job_delivery_latency_seconds_sum = 0.0
        if not hasattr(self, "initial_job_delivery_latency_count"):
            self.initial_job_delivery_latency_count = 0
        if not hasattr(self, "last_initial_job_delivery_monotonic"):
            self.last_initial_job_delivery_monotonic = None
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
        if not hasattr(self, "_tip_refresh_lock"):
            self._tip_refresh_lock = threading.Lock()
        if not hasattr(self, "_tip_refresh_executor_lock"):
            self._tip_refresh_executor_lock = threading.Lock()
        if not hasattr(self, "_tip_refresh_executor"):
            self._tip_refresh_executor: _BoundedPriorityExecutor | None = None
        if not hasattr(self, "_tip_refresh_executor_shutdown"):
            self._tip_refresh_executor_shutdown = False
        if not hasattr(self, "tip_refresh_max_workers"):
            self.tip_refresh_max_workers = DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS
        if not hasattr(self, "_tip_refresh_metrics_lock"):
            self._tip_refresh_metrics_lock = threading.Lock()
        if not hasattr(self, "tip_refresh_histograms"):
            self.tip_refresh_histograms = {
                name: {
                    "buckets": {bucket: 0 for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS},
                    "sum": 0.0,
                    "count": 0,
                }
                for name in ("refresh", "bundle_build", "first_delivery", "last_delivery")
            }
        if not hasattr(self, "tip_refresh_build_phase_histograms"):
            self.tip_refresh_build_phase_histograms = {
                phase: {
                    "buckets": {
                        bucket: 0 for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                    },
                    "sum": 0.0,
                    "count": 0,
                }
                for phase in PRISM_TIP_REFRESH_BUILD_PHASES
            }
        if not hasattr(self, "tip_refresh_client_counts"):
            self.tip_refresh_client_counts = {
                result: 0 for result in PRISM_TIP_REFRESH_RESULTS
            }
        if not hasattr(self, "tip_refresh_cancellation_counts"):
            self.tip_refresh_cancellation_counts = {
                stage: 0 for stage in PRISM_TIP_REFRESH_CANCELLATION_STAGES
            }
        if not hasattr(self, "tip_refresh_inflight"):
            self.tip_refresh_inflight = 0
        if not hasattr(self, "tip_refresh_build_inflight"):
            self.tip_refresh_build_inflight = 0
        if not hasattr(self, "tip_refresh_build_queue_depth"):
            self.tip_refresh_build_queue_depth = 0
        if not hasattr(self, "tip_refresh_singleflight_hits"):
            self.tip_refresh_singleflight_hits = 0
        if not hasattr(self, "tip_refresh_superseded_results"):
            self.tip_refresh_superseded_results = 0
        if not hasattr(self, "tip_refresh_worker_failures"):
            self.tip_refresh_worker_failures = 0
        if not hasattr(self, "tip_refresh_worker_restarts"):
            self.tip_refresh_worker_restarts = 0
        if not hasattr(self, "tip_refresh_ipc_bytes"):
            self.tip_refresh_ipc_bytes = {"input": 0, "output": 0}
        if not hasattr(self, "_tip_refresh_pending_event"):
            self._tip_refresh_pending_event = threading.Event()
        if not hasattr(self, "_tip_refresh_pending_counter"):
            self._tip_refresh_pending_counter = 0
        if not hasattr(self, "_tip_refresh_pending_token"):
            self._tip_refresh_pending_token: int | None = None
        if not hasattr(self, "_tip_refresh_retry"):
            self._tip_refresh_retry = threading.Event()
        if not hasattr(self, "_active_tip_refresh"):
            self._active_tip_refresh: tuple[
                TipRefreshValidationToken,
                _FanoutCancellation,
            ] | None = None
        if not hasattr(self, "_retained_collection_refresh"):
            self._retained_collection_refresh: RetainedCollectionRefresh | None = None

    def _retain_collection_refresh(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
        payout_state_generation: int,
    ) -> None:
        """Retain reusable current work until an eligible identity appears."""
        retained = RetainedCollectionRefresh(
            snapshot=snapshot,
            observation_sequence=observation_sequence,
            payout_state_generation=payout_state_generation,
        )
        should_log = False
        with self.lock:
            if not self._tip_refresh_snapshot_current_locked(
                snapshot,
                observation_sequence,
            ):
                return
            if any(
                self.client_can_receive_jobs(client)
                for client in self.clients
            ):
                return
            previous = self._retained_collection_refresh
            self._retained_collection_refresh = retained
            should_log = previous is None or (
                previous.snapshot.bestblockhash != snapshot.bestblockhash
                or previous.payout_state_generation != payout_state_generation
            )
        if should_log:
            print(
                "prism coordinator: collection refresh retained while no "
                "authorized worker identity is available",
                flush=True,
            )

    def _retained_collection_artifacts(self) -> CachedTemplateArtifacts | None:
        """Return retained artifacts while their published work stays current.

        A same-tip poll advances its observation sequence before atomically
        replacing ``tip_template_snapshot``. The published snapshot remains
        reusable on both sides of that handoff even if the retained marker has
        not yet been updated; a new tip or payout generation still invalidates
        it immediately.
        """
        self._ensure_job_cache_state()
        self._ensure_tip_refresh_state()
        with self._job_cache_lock:
            payout_state_generation = self._payout_state_generation
        with self.lock:
            retained = self._retained_collection_refresh
            if retained is None:
                return None
            if retained.payout_state_generation != payout_state_generation:
                return None
            current_tip = getattr(self, "current_tip_first_seen", None)
            published_snapshot = self.tip_template_snapshot
            if (
                published_snapshot is None
                or current_tip is None
                or current_tip[0] != published_snapshot.bestblockhash
            ):
                return None
            return self._tip_refresh_artifacts(published_snapshot)

    def _retain_current_collection_refresh_if_unrepresented(self) -> None:
        """Keep the last published collection work when the fleet empties."""
        self._ensure_tip_refresh_state()
        if getattr(self, "_pool_ready_latched", False):
            return
        with self.lock:
            if any(
                self.client_can_receive_jobs(client)
                for client in self.clients
            ):
                return
            snapshot = self.tip_template_snapshot
            observation_sequence = int(
                getattr(self, "current_tip_observation_sequence", 0)
            )
        if snapshot is None:
            return
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            payout_state_generation = self._payout_state_generation
        self._retain_collection_refresh(
            snapshot,
            observation_sequence,
            payout_state_generation,
        )

    def _note_collection_identity_available(self, client: ClientState) -> None:
        """Wake a retained collection refresh as soon as a client is eligible."""
        if not self.client_can_receive_jobs(client):
            return
        if self._retained_collection_artifacts() is None:
            return
        self._mark_tip_refresh_pending(client.connection_id)
        self._schedule_tip_refresh_retry()

    def _consume_retained_collection_refresh(
        self,
        context: PrismJobContext,
    ) -> None:
        """Consume retention only after its collection work was delivered."""
        if not context.collection_only:
            return
        with self.lock:
            retained = self._retained_collection_refresh
            published_snapshot = self.tip_template_snapshot
            artifacts = (
                published_snapshot.template_artifacts
                if published_snapshot is not None
                else None
            )
            if (
                retained is not None
                and retained.payout_state_generation
                == context.payout_state_generation
                and artifacts is not None
                and context.template is artifacts.template
                and context.template_fingerprint == artifacts.fingerprint
                and context.template_generation == artifacts.generation
            ):
                self._retained_collection_refresh = None

    def tip_refresh_is_pending(self) -> bool:
        return self._tip_refresh_pending()

    def _tip_refresh_pending(self) -> bool:
        self._ensure_tip_refresh_state()
        return self._tip_refresh_pending_event.is_set()

    def _mark_tip_refresh_pending(self, _observation: object) -> int:
        self._ensure_tip_refresh_state()
        with self.lock:
            self._tip_refresh_pending_counter += 1
            token = self._tip_refresh_pending_counter
            self._tip_refresh_pending_token = token
            self._tip_refresh_pending_event.set()
            return token

    def _claim_tip_refresh_pending(self) -> int | None:
        """Snapshot pending work without replacing a newer producer's token."""
        self._ensure_tip_refresh_state()
        with self.lock:
            if not self._tip_refresh_pending_event.is_set():
                return None
            return self._tip_refresh_pending_token

    def _mark_tip_refresh_pending_for_poll(
        self,
        owned_token: int | None,
        _observation: object,
    ) -> int | None:
        """Mark poll-owned work only while no newer producer has superseded it."""
        self._ensure_tip_refresh_state()
        with self.lock:
            if self._tip_refresh_pending_token != owned_token:
                return owned_token
            if owned_token is not None:
                self._tip_refresh_pending_event.set()
                return owned_token
            self._tip_refresh_pending_counter += 1
            token = self._tip_refresh_pending_counter
            self._tip_refresh_pending_token = token
            self._tip_refresh_pending_event.set()
            return token

    def _clear_tip_refresh_pending(self, token: int) -> None:
        self._ensure_tip_refresh_state()
        with self.lock:
            if self._tip_refresh_pending_token == token:
                self._tip_refresh_pending_token = None
                self._tip_refresh_pending_event.clear()

    def _clear_tip_refresh_pending_for_completed_refresh(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
        payout_state_generation: int,
    ) -> bool:
        """Atomically acknowledge pending work handled by a completed poll."""
        self._ensure_job_cache_state()
        with self._payout_state_delivery_gate.delivery_cancelable(
            lambda: self._payout_state_generation != payout_state_generation,
            generation=payout_state_generation,
            priority=True,
        ) as admission:
            if not admission:
                return False
            with self._job_cache_lock:
                payout_state_current = (
                    self._payout_state_generation == payout_state_generation
                )
            with self.lock:
                refresh_current = self._tip_refresh_snapshot_current_locked(
                    snapshot,
                    observation_sequence,
                )
                if not payout_state_current or not refresh_current:
                    return False
                self._tip_refresh_pending_token = None
                self._tip_refresh_pending_event.clear()
                return True

    def _schedule_tip_refresh_retry(self) -> None:
        self._ensure_tip_refresh_state()
        self._tip_refresh_retry.set()

    def _observe_tip_refresh_seconds(self, name: str, elapsed_seconds: float) -> None:
        self._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            histogram = self.tip_refresh_histograms[name]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + elapsed_seconds
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def _observe_tip_refresh_build_phase(
        self,
        phase: str,
        elapsed_seconds: float,
    ) -> None:
        if phase not in PRISM_TIP_REFRESH_BUILD_PHASES:
            raise ValueError(f"unknown tip refresh build phase: {phase}")
        self._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            histogram = self.tip_refresh_build_phase_histograms[phase]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + max(
                0.0, elapsed_seconds
            )
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def _record_tip_refresh_ipc_bytes(self, direction: str, byte_count: int) -> None:
        if direction not in {"input", "output"}:
            raise ValueError(f"unknown tip refresh IPC direction: {direction}")
        self._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_ipc_bytes[direction] += max(0, int(byte_count))

    def _record_tip_refresh_client_result(self, result: str) -> None:
        if result not in PRISM_TIP_REFRESH_RESULTS:
            raise ValueError(f"unknown tip refresh result: {result}")
        self._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_client_counts[result] += 1

    def _record_tip_refresh_cancellation(self, stage: str) -> None:
        if stage not in PRISM_TIP_REFRESH_CANCELLATION_STAGES:
            raise ValueError(f"unknown tip refresh cancellation stage: {stage}")
        self._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_cancellation_counts[stage] += 1

    def _tip_refresh_future_started(self) -> None:
        self._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_inflight += 1

    def _tip_refresh_future_finished(self, _future: Future[RefreshResult]) -> None:
        self._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_inflight = max(0, self.tip_refresh_inflight - 1)

    def tip_refresh_executor(self) -> _BoundedPriorityExecutor:
        self._ensure_tip_refresh_state()
        with self._tip_refresh_executor_lock:
            if self._tip_refresh_executor_shutdown:
                raise RuntimeError("tip refresh executor is shut down")
            executor = self._tip_refresh_executor
            if executor is None:
                executor = _BoundedPriorityExecutor(
                    max_workers=self.tip_refresh_max_workers,
                    max_queue_size=self.delivery_queue_limit(),
                )
                self._tip_refresh_executor = executor
            return executor

    def shutdown_tip_refresh_executor(self) -> None:
        self._ensure_tip_refresh_state()
        with self.lock:
            self._ensure_initial_job_state()
            for request in tuple(self.pending_initial_jobs.values()):
                request.cancelled.set()
                if request.future is not None:
                    request.future.cancel()
            self.pending_initial_jobs.clear()
        with self._tip_refresh_executor_lock:
            executor = self._tip_refresh_executor
            self._tip_refresh_executor = None
            self._tip_refresh_executor_shutdown = True
        if executor is not None:
            # Running workers may already hold client/job state or be inside a
            # socket send. Drain them before serve returns and the writer lease
            # is released; queued workers are cancelled without starting.
            executor.shutdown(wait=True, cancel_futures=True)
        self.shutdown_payout_artifact_executor()

    def _initial_request_current_locked(self, request: PendingInitialJob) -> bool:
        client = request.client
        return (
            self.pending_initial_jobs.get(client) is request
            and client in self.clients
            and (
                request.connection_id is None
                or client.connection_id == request.connection_id
            )
            and client.authorized
            and client.subscribed
            and client.worker == request.worker
            and int(getattr(client, "authorization_generation", 0))
            == request.authorization_generation
            and (
                request.difficulty_generation is None
                or int(getattr(client, "difficulty_generation", 0))
                == request.difficulty_generation
            )
            and not request.cancelled.is_set()
        )

    def _initial_request_cancelled(self, request: PendingInitialJob) -> bool:
        if request.cancelled.is_set() or self.stop_event.is_set():
            return True
        with self.lock:
            self._ensure_initial_job_state()
            return not self._initial_request_current_locked(request)

    def _cancel_pending_initial_job_locked(
        self,
        client: ClientState,
        *,
        count: bool,
    ) -> PendingInitialJob | None:
        self._ensure_initial_job_state()
        request = self.pending_initial_jobs.pop(client, None)
        if request is None:
            return None
        request.cancelled.set()
        if request.future is not None:
            request.future.cancel()
        if count:
            self.initial_job_cancelled_count += 1
        return request

    def _client_has_current_tip_job_locked(self, client: ClientState) -> bool:
        context = client.active_job
        if context is None:
            return False
        payout_generation = int(getattr(self, "_payout_state_generation", 0))
        if int(getattr(context, "payout_state_generation", payout_generation)) != payout_generation:
            return False
        current_tip = self._current_observed_tip_hash_locked()
        if current_tip is None:
            # An active job is not proof that tip observation is alive. Keep
            # coverage fail-closed until blockpoll/blockwait has published the
            # tip that makes the job current.
            return False
        snapshot = getattr(self, "tip_template_snapshot", None)
        if snapshot is None:
            # Focused embedders may only publish the observed tip. Production
            # startup and blockpoll publish a full snapshot, in which case the
            # exact template identity checks below are mandatory.
            return str(context.template.get("previousblockhash", "")) == current_tip
        if snapshot.bestblockhash != current_tip or snapshot.template_artifacts is None:
            return False
        return bool(
            str(context.template.get("previousblockhash", "")) == current_tip
            and getattr(context, "template_fingerprint", None)
            == snapshot.template_fingerprint
            and int(getattr(context, "template_generation", 0))
            == snapshot.template_generation
            and context.template is snapshot.template_artifacts.template
            and int(getattr(context, "connection_id", client.connection_id))
            == client.connection_id
            and int(getattr(context, "authorization_generation", 0))
            == int(getattr(client, "authorization_generation", 0))
            and int(getattr(context, "difficulty_generation", 0))
            == int(getattr(client, "difficulty_generation", 0))
        )

    def note_initial_job_delivered(
        self,
        client: ClientState,
        *,
        validated_current: bool = False,
    ) -> None:
        with self.lock:
            self._ensure_initial_job_state()
            if not validated_current and not self._client_has_current_tip_job_locked(client):
                return
            request = self.pending_initial_jobs.pop(client, None)
            if request is not None:
                request.cancelled.set()
                if request.future is not None:
                    request.future.cancel()
                delivered = time.monotonic()
                self.initial_job_sent_count += 1
                self.initial_job_delivery_latency_seconds_sum += max(
                    0.0, delivered - request.requested_monotonic
                )
                self.initial_job_delivery_latency_count += 1
                self.last_initial_job_delivery_monotonic = delivered

    def schedule_initial_job(self, client: ClientState) -> bool:
        """Coalesce and enqueue one first-job request without blocking its handler."""
        # Focused tests and embedders replace maybe_send_job on the instance as
        # a synchronous seam. Preserve it without affecting the production
        # class path, which always uses the bounded executor below.
        if "maybe_send_job" in self.__dict__:
            return bool(self.maybe_send_job(client, clean_jobs=True))

        now = time.monotonic()
        reject = False
        deferred = False
        superseded_future: Future[bool] | None = None
        with self.lock:
            self._ensure_initial_job_state()
            if (
                not client.subscribed
                or not client.authorized
                or client.worker is None
                or getattr(client, "closing", False)
            ):
                return True
            generation = int(getattr(client, "authorization_generation", 0))
            difficulty_generation = int(
                getattr(client, "difficulty_generation", 0)
            )
            existing = self.pending_initial_jobs.get(client)
            if (
                existing is not None
                and existing.connection_id == client.connection_id
                and existing.authorization_generation == generation
                and existing.difficulty_generation == difficulty_generation
                and existing.worker == client.worker
            ):
                self.initial_job_coalesced_count += 1
                return True
            if existing is not None:
                existing.cancelled.set()
                superseded_future = existing.future
                self.initial_job_cancelled_count += 1
                self.initial_job_superseded_count += 1
            if self._client_has_current_tip_job_locked(client):
                if existing is not None:
                    self.pending_initial_jobs.pop(client, None)
                    if superseded_future is not None:
                        superseded_future.cancel()
                return True
            if (
                existing is None
                and len(self.pending_initial_jobs)
                >= self.stratum_max_pending_initial_jobs
            ):
                self.initial_job_queue_rejection_count += 1
                reject = True
                request = None
            else:
                timeout = float(self.stratum_initial_job_timeout_seconds)
                predecessor = None
                if existing is not None:
                    for candidate in (existing.future, existing.predecessor):
                        if candidate is not None and not candidate.done():
                            predecessor = candidate
                            break
                request = PendingInitialJob(
                    client=client,
                    connection_id=client.connection_id,
                    authorization_generation=generation,
                    difficulty_generation=difficulty_generation,
                    worker=client.worker,
                    requested_monotonic=now,
                    deadline_monotonic=now + timeout if timeout > 0 else None,
                    predecessor=predecessor,
                )
                self.pending_initial_jobs[client] = request
                deferred = predecessor is not None
        if superseded_future is not None:
            # Install the replacement before cancellation callbacks can run;
            # the predecessor callback then hands off exactly one client slot
            # instead of mistaking the obsolete request for a terminal failure.
            superseded_future.cancel()
        if reject or request is None:
            self.disconnect_client(client)
            return False
        if deferred:
            return True

        return self._submit_initial_job_request(request)

    def request_initial_job_delivery(self, client: ClientState) -> bool:
        """Compatibility name for the single bounded initial-job pipeline."""
        return self.schedule_initial_job(client)

    def cancel_initial_job_delivery(self, client: ClientState) -> None:
        with self.lock:
            self._ensure_initial_job_state()
            self._cancel_pending_initial_job_locked(client, count=True)

    def _submit_initial_job_request(self, request: PendingInitialJob) -> bool:
        client = request.client
        try:
            future = self._submit_delivery_task(
                self.tip_refresh_executor(),
                self._run_initial_job,
                request,
                priority=PRISM_DELIVERY_PRIORITY_INITIAL,
            )
        except (_DeliveryQueueFull, RuntimeError):
            with self.lock:
                if self.pending_initial_jobs.get(client) is request:
                    self.pending_initial_jobs.pop(client, None)
                    request.cancelled.set()
                    self.initial_job_queue_rejection_count += 1
            self.disconnect_client(client)
            return False
        with self.lock:
            if self.pending_initial_jobs.get(client) is request:
                request.future = future
            else:
                future.cancel()
        future.add_done_callback(
            lambda completed: self._initial_job_future_finished(request, completed)
        )
        return True

    def _initial_job_future_finished(
        self,
        request: PendingInitialJob,
        future: Future[bool],
    ) -> None:
        """Release failed first-job requests instead of stranding capacity."""
        delivered = False
        if not future.cancelled():
            try:
                delivered = bool(future.result())
            except Exception:
                with self.lock:
                    self.job_build_failure_count = int(
                        getattr(self, "job_build_failure_count", 0)
                    ) + 1
                print(
                    "prism coordinator: initial job task failed "
                    f"connection={request.client.connection_id}",
                    flush=True,
                )
                traceback.print_exc()

        disconnect = False
        replacement: PendingInitialJob | None = None
        with self.lock:
            self._ensure_initial_job_state()
            current = self.pending_initial_jobs.get(request.client)
            if current is not request:
                if (
                    current is not None
                    and current.future is None
                    and current.predecessor is future
                ):
                    current.predecessor = None
                    replacement = current
            elif delivered and self._client_has_current_tip_job_locked(request.client):
                self.pending_initial_jobs.pop(request.client, None)
                request.cancelled.set()
                self.last_initial_job_delivery_monotonic = time.monotonic()
            elif current is request:
                self.pending_initial_jobs.pop(request.client, None)
                request.cancelled.set()
                self.initial_job_failed_count += 1
                disconnect = True
        if replacement is not None:
            self._submit_initial_job_request(replacement)
        if disconnect:
            self.disconnect_client(request.client)

    def _run_initial_job(self, request: PendingInitialJob) -> bool:
        """Prepare outside client locks, then atomically stamp and send current work."""
        retry_delay = 0.05
        last_failure_log_monotonic: float | None = None

        def retry_later() -> bool:
            nonlocal retry_delay
            request.cancelled.wait(retry_delay)
            retry_delay = min(1.0, retry_delay * 2)
            return not self._initial_request_cancelled(request)

        try:
            while not self._initial_request_cancelled(request):
                try:
                    if not self.ensure_reorg_reconciled_for_current_tip():
                        if not retry_later():
                            return False
                        continue
                    artifacts = self.current_template_artifacts()
                    if self._initial_request_cancelled(request):
                        return False
                    bundle = self.shared_job_bundle(
                        artifacts,
                        request.worker,
                        cancelled=lambda: (
                            self._initial_request_cancelled(request)
                            or not self._template_artifacts_are_current(artifacts)
                        ),
                    )
                    live_tip = str(self.rpc.call("getbestblockhash"))
                    if artifacts.previousblockhash != live_tip:
                        with self._job_cache_lock:
                            if self._template_artifacts is artifacts:
                                self._template_artifacts = None
                        if not retry_later():
                            return False
                        continue
                except _JobBuildCancelled:
                    if self._initial_request_cancelled(request):
                        return False
                    if not retry_later():
                        return False
                    continue
                except TemplateRefreshBlocked:
                    if self._initial_request_cancelled(request):
                        return False
                    if not retry_later():
                        return False
                    continue
                except Exception:
                    with self.lock:
                        self.job_build_failure_count = int(
                            getattr(self, "job_build_failure_count", 0)
                        ) + 1
                    now = time.monotonic()
                    if (
                        last_failure_log_monotonic is None
                        or now - last_failure_log_monotonic >= 5.0
                    ):
                        last_failure_log_monotonic = now
                        print(
                            "prism coordinator: initial job preparation failed "
                            f"connection={request.client.connection_id}; retrying",
                            flush=True,
                        )
                        traceback.print_exc()
                    if not retry_later():
                        return False
                    continue
                delivered = self._deliver_initial_bundle(request, artifacts, bundle)
                if delivered is None:
                    if not retry_later():
                        return False
                    continue
                return delivered
            return False
        except OSError:
            self.disconnect_client(request.client)
            return False

    def _template_artifacts_are_current(self, artifacts: CachedTemplateArtifacts) -> bool:
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            current = self._template_artifacts
            return (
                current is artifacts
                or (
                    current is not None
                    and current.fingerprint == artifacts.fingerprint
                    and current.generation == artifacts.generation
                )
            )

    def _acquire_client_job_lock(
        self,
        client: ClientState,
        cancelled: Callable[[], bool],
    ) -> bool:
        while not cancelled():
            if client.job_update_lock.acquire(timeout=0.1):
                return True
        return False

    @contextmanager
    def _cancellable_client_job_lock(
        self,
        client: ClientState,
        cancelled: Callable[[], bool],
    ) -> Iterator[bool]:
        acquired = self._acquire_client_job_lock(client, cancelled)
        try:
            yield acquired
        finally:
            if acquired:
                client.job_update_lock.release()

    def _payout_delivery(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int,
    ) -> Any:
        """Use cancellable admission while retaining focused gate test seams."""
        gate = self._payout_state_delivery_gate
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
        client = request.client
        cancelled = lambda: self._initial_request_cancelled(request)
        if not self._acquire_client_job_lock(client, cancelled):
            return False
        try:
            if cancelled():
                return False
            if not self._template_artifacts_are_current(artifacts):
                return None
            self._ensure_job_cache_state()
            gate_started = time.monotonic()
            with self._payout_delivery(
                cancelled,
                generation=bundle.payout_state_generation,
            ) as admitted:
                self._observe_payout_gate_admission(
                    admitted,
                    generation=bundle.payout_state_generation,
                    fallback_wait_seconds=time.monotonic() - gate_started,
                )
                if not admitted or cancelled():
                    return False
                with self._job_cache_lock:
                    payout_current = (
                        bundle.payout_state_generation == self._payout_state_generation
                    )
                if not payout_current or not self._template_artifacts_are_current(artifacts):
                    return None
                with self.lock:
                    if not self._initial_request_current_locked(request):
                        return False
                    context = self.stamp_job_for_client(
                        client,
                        bundle,
                        clean_jobs=True,
                    )
                    client.active_job = context
                    for job_id in tuple(client.active_job_ids):
                        self.bury_evicted_job(client, job_id, prune=False)
                        self.jobs.pop(job_id, None)
                    client.active_job_ids.clear()
                    self.prune_evicted_job_graveyard(force=False)
                    self.jobs[context.job.job_id] = context
                    client.active_job_ids.add(context.job.job_id)
                    self.prune_client_active_jobs(client)

                send_started = time.monotonic()
                self.send_job_update(client, context.job)
                mark_delivered = getattr(admitted, "mark_delivered", None)
                if callable(mark_delivered):
                    mark_delivered()
                self.apply_job_difficulty(client, context.job)
                self.note_tip_work_delivered(
                    client,
                    str(context.template["previousblockhash"]),
                )
                self._record_first_payout_delivery(
                    bundle.payout_state_generation,
                    time.monotonic(),
                )
                self.note_initial_job_delivered(client, validated_current=True)
                return True
        finally:
            client.job_update_lock.release()

    def sweep_initial_job_timeouts(self, *, now: float | None = None) -> int:
        now = time.monotonic() if now is None else now
        timed_out: list[PendingInitialJob] = []
        with self.lock:
            self._ensure_initial_job_state()
            expired = [
                request
                for request in self.pending_initial_jobs.values()
                if request.deadline_monotonic is not None
                and request.deadline_monotonic <= now
            ]
            for request in expired:
                if self.pending_initial_jobs.get(request.client) is not request:
                    continue
                self.pending_initial_jobs.pop(request.client, None)
                request.cancelled.set()
                if request.future is not None:
                    request.future.cancel()
                # Commit teardown while this request still owns the pending
                # slot. A concurrent reauthorization will observe closing and
                # cannot install a replacement between expiry and disconnect.
                request.client.closing = True
                self.initial_job_timeout_count += 1
                timed_out.append(request)
        for request in timed_out:
            self.disconnect_client(request.client)
        return len(timed_out)

    def initial_job_timeout_loop(self) -> None:
        while not self.stop_event.wait(1.0):
            self.sweep_initial_job_timeouts()

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
        profiles = getattr(self, "listener_profiles", None)
        if not profiles:
            return ("stratum_accept",)
        return tuple(profile.heartbeat_name for profile in profiles)

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
        while not self.stop_event.wait(self.watchdog_interval_seconds):
            overdue = self._overdue_heartbeats(time.monotonic())
            if overdue:
                print(
                    "prism coordinator: liveness watchdog firing; unresponsive "
                    f"subsystems={overdue} timeout={self.watchdog_timeout_seconds:g}s. "
                    "Exiting non-zero so the restart policy recovers the process.",
                    flush=True,
                )
                # Queued shares have not been acknowledged. Miners reconnect
                # and retry them after restart; exact-payload replay is
                # idempotent if Postgres committed just before this exit.
                os._exit(1)

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
        self._ensure_tip_refresh_state()
        with self.lock:
            active = self._active_tip_refresh
            if active is not None:
                active[1].cancel()
            self._tip_refresh_retry.clear()

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
        for thread, timeout in threads or []:
            thread.join(timeout=timeout)
        self.shutdown_tip_refresh_executor()
        elapsed = max(0.0, time.monotonic() - started)
        controller.finish_non_writer_drain(elapsed)
        self._shutdown_log(
            "non_writer_drain",
            duration_seconds=round(elapsed, 6),
            lease_release_succeeded=controller.lease_release_succeeded,
            outcome="complete",
        )

    def serve(self) -> None:
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                self.rpc.call("getblockcount")
                break
            except Exception:
                time.sleep(1)
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
        # Recover block work before opening Stratum listeners.  New miners can
        # only add wakeups after every previously committed candidate has had a
        # chance to re-enter the submit queue.
        if not self._run_startup_writer_replay(self.replay_pending_block_candidates):
            return
        prepared = self.prewarm_startup_jobs()
        print(
            "prism coordinator: startup job preparation "
            f"status={'complete' if prepared is not None else 'deferred'} "
            f"mode={'ready' if prepared is not None else 'collection'} "
            f"tip={self.tip_template_snapshot.bestblockhash if self.tip_template_snapshot else 'unknown'}",
            flush=True,
        )
        with ExitStack() as listener_stack:
            listeners: list[tuple[socket.socket, StratumListenerProfile]] = []
            for profile in self.listener_profiles:
                server = listener_stack.enter_context(
                    socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                )
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.bind((profile.bind, profile.port))
                server.listen()
                server.settimeout(1)
                listeners.append((server, profile))
            # Seed liveness before starting monitored loops so the watchdog
            # never fires during startup.
            for _, profile in listeners:
                self._record_heartbeat(profile.heartbeat_name)
            self._record_heartbeat("qbit_blockpoll")
            blockpoll_thread = threading.Thread(target=self.blockpoll_loop, daemon=True)
            blockpoll_thread.start()
            blockwait_thread: threading.Thread | None = None
            if self.blockwait_enabled:
                self._record_heartbeat("qbit_blockwait")
                blockwait_thread = threading.Thread(target=self.blockwait_loop, daemon=True)
                blockwait_thread.start()
            vardiff_idle_sweep_thread: threading.Thread | None = None
            if self.vardiff_idle_sweep_seconds > 0:
                self._record_heartbeat("vardiff_idle_sweep")
                vardiff_idle_sweep_thread = threading.Thread(
                    target=self.vardiff_idle_sweep_loop,
                    daemon=True,
                )
                vardiff_idle_sweep_thread.start()
            initial_job_timeout_thread: threading.Thread | None = None
            if self.stratum_initial_job_timeout_seconds > 0:
                initial_job_timeout_thread = threading.Thread(
                    target=self.initial_job_timeout_loop,
                    name="prism-initial-job-timeouts",
                    daemon=True,
                )
                initial_job_timeout_thread.start()
            self._record_heartbeat("block_submitter")
            block_submitter_thread = threading.Thread(
                target=self.block_submit_loop,
                daemon=True,
            )
            block_submitter_thread.start()
            drain_threads: list[tuple[threading.Thread, float]] = [
                (blockpoll_thread, 1.0),
                (block_submitter_thread, 1.0),
            ]
            if blockwait_thread is not None:
                drain_threads.append((blockwait_thread, 1.0))
            if vardiff_idle_sweep_thread is not None:
                drain_threads.append((vardiff_idle_sweep_thread, 1.0))
            if initial_job_timeout_thread is not None:
                drain_threads.append((initial_job_timeout_thread, 1.0))
            # Replay any shares stranded on disk by a prior ledger-outage
            # shutdown before serving, so no acked share is lost across restart.
            if not self._run_startup_writer_replay(
                self.replay_recovered_shares,
                drain_threads=drain_threads,
            ):
                return
            self._record_heartbeat("share_writer")
            self.share_writer_active = True
            share_writer_thread = threading.Thread(
                target=self.share_append_loop,
                daemon=True,
            )
            share_writer_thread.start()
            drain_threads.append((share_writer_thread, 5.0))
            ctv_broadcaster_thread: threading.Thread | None = None
            if self.ctv_broadcaster_enabled:
                self._record_heartbeat("ctv_fanout_broadcaster")
                ctv_broadcaster_thread = threading.Thread(
                    target=self.ctv_fanout_broadcaster_loop,
                    daemon=True,
                )
                ctv_broadcaster_thread.start()
                drain_threads.append((ctv_broadcaster_thread, 1.0))
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
                threading.Thread(target=self.watchdog_loop, daemon=True).start()
                print(
                    "prism coordinator: liveness watchdog enabled "
                    f"timeout={self.watchdog_timeout_seconds:g}s "
                    f"interval={self.watchdog_interval_seconds:g}s",
                    flush=True,
                )
            for extra_server, extra_profile in listeners[1:]:
                threading.Thread(
                    target=self.accept_loop,
                    args=(extra_server, extra_profile),
                    daemon=True,
                ).start()
            try:
                self.accept_loop(*listeners[0])
            finally:
                # The writer barrier and lease release intentionally precede
                # joins and the tip-refresh executor drain: those may be stuck
                # in unrelated client delivery or obsolete fanout work.
                self.shutdown(reason="serve_exit")
                self.drain_non_writer_components(drain_threads)

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

    def accept_loop(self, server: socket.socket, profile: StratumListenerProfile) -> None:
        while not self.stop_event.is_set():
            self._record_heartbeat(profile.heartbeat_name)
            try:
                sock, address = server.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                # The listener socket is torn down by serve()'s ExitStack on
                # shutdown while secondary accept threads may still be blocked
                # in accept(). Descriptor exhaustion is recoverable: keep the
                # accept loop alive and refresh its watchdog heartbeat while
                # waiting for client/RPC descriptors to drain.
                if self.stop_event.is_set():
                    return
                if exc.errno in {errno.EMFILE, errno.ENFILE}:
                    self._record_stratum_resource_exhaustion(
                        listener_name=profile.name,
                        location="accept",
                        error_number=exc.errno,
                    )
                    self._wait_after_stratum_resource_failure(profile.heartbeat_name)
                    continue
                raise

            if (
                self.stop_event.is_set()
                or self._ensure_shutdown_controller().phase != "running"
            ):
                try:
                    sock.close()
                except OSError:
                    pass
                return

            with self.lock:
                if (
                    self.stop_event.is_set()
                    or self._ensure_shutdown_controller().phase != "running"
                ):
                    try:
                        sock.close()
                    except OSError:
                        pass
                    return
                max_connections = int(
                    getattr(
                        self,
                        "stratum_max_connections",
                        DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
                    )
                )
                if max_connections > 0 and len(self.clients) >= max_connections:
                    rejection_count = self._note_connection_limit_rejection_locked("global")
                    client = None
                else:
                    self.connection_counter += 1
                    connection_id = self.connection_counter
                    client = ClientState(
                        sock=sock,
                        address=address,
                        connection_id=connection_id,
                        extranonce1_hex=f"{connection_id & 0xFFFFFFFF:08x}",
                        listener_name=profile.name,
                        listener_vardiff_config=profile.vardiff_config,
                        minimum_advertised_difficulty=profile.minimum_advertised_difficulty,
                        share_difficulty=self.client_startup_difficulty(profile),
                    )
                    self.clients.add(client)
                    self._ensure_initial_job_state()
                    self.peak_active_connection_count = max(
                        self.peak_active_connection_count,
                        len(self.clients),
                    )
            if client is None:
                try:
                    sock.close()
                except OSError:
                    pass
                if rejection_count == 1 or rejection_count % 100 == 0:
                    print(
                        "prism coordinator: rejected stratum connection at global limit "
                        f"limit={max_connections} count={rejection_count}",
                        flush=True,
                    )
                continue
            try:
                sock.settimeout(None)
                self.apply_stratum_send_timeout(sock)
                thread = threading.Thread(target=self.handle_client, args=(client,), daemon=True)
                with self.lock:
                    client.handler_thread_registered = True
                    self.handler_thread_count += 1
                thread.start()
            except (OSError, RuntimeError) as exc:
                # Admission is atomic with the global count. Undo it if socket
                # setup or thread creation fails before a handler owns cleanup,
                # then keep this listener alive for the next connection.
                try:
                    with self.lock:
                        if client.handler_thread_registered:
                            client.handler_thread_registered = False
                            self.handler_thread_count = max(
                                0,
                                self.handler_thread_count - 1,
                            )
                    self.disconnect_client(client)
                except Exception:
                    print(
                        "prism coordinator: failed to fully close rejected stratum client "
                        f"address={address}",
                        flush=True,
                    )
                    traceback.print_exc()
                with self.lock:
                    self.connection_setup_failure_count = int(
                        getattr(self, "connection_setup_failure_count", 0)
                    ) + 1
                    setup_failure_count = self.connection_setup_failure_count
                if isinstance(exc, OSError) and exc.errno in {errno.EMFILE, errno.ENFILE}:
                    self._record_stratum_resource_exhaustion(
                        listener_name=profile.name,
                        location="connection-setup",
                        error_number=exc.errno,
                    )
                if setup_failure_count == 1 or setup_failure_count % 100 == 0:
                    print(
                        "prism coordinator: stratum connection setup failed; backing off "
                        f"listener={profile.name} address={address} "
                        f"error={exc!r} count={setup_failure_count}",
                        flush=True,
                    )
                self._wait_after_stratum_resource_failure(profile.heartbeat_name)
                continue

    def _record_stratum_resource_exhaustion(
        self,
        *,
        listener_name: str,
        location: str,
        error_number: int | None,
    ) -> int:
        with self.lock:
            self.accept_resource_exhaustion_count = int(
                getattr(self, "accept_resource_exhaustion_count", 0)
            ) + 1
            exhaustion_count = self.accept_resource_exhaustion_count
        if exhaustion_count == 1 or exhaustion_count % 100 == 0:
            print(
                "prism coordinator: stratum resource exhaustion "
                f"listener={listener_name} location={location} errno={error_number} "
                f"count={exhaustion_count}",
                flush=True,
            )
        return exhaustion_count

    def _wait_after_stratum_resource_failure(self, heartbeat_name: str) -> None:
        backoff_seconds = getattr(
            self,
            "stratum_accept_resource_exhaustion_backoff_seconds",
            DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS,
        )
        remaining_seconds = max(0.0, float(backoff_seconds))
        watchdog_timeout_seconds = max(
            0.001,
            float(getattr(self, "watchdog_timeout_seconds", 120.0)),
        )
        heartbeat_interval_seconds = max(
            0.001,
            min(1.0, watchdog_timeout_seconds / 2.0),
        )
        deadline = time.monotonic() + remaining_seconds
        while not self.stop_event.is_set():
            self._record_heartbeat(heartbeat_name)
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                return
            if self.stop_event.wait(min(remaining_seconds, heartbeat_interval_seconds)):
                return

    def _ensure_connection_capacity_state(self) -> None:
        if not hasattr(self, "connection_limit_rejection_counts"):
            self.connection_limit_rejection_counts = {"global": 0, "username": 0}

    def _note_connection_limit_rejection_locked(self, scope: str) -> int:
        self._ensure_connection_capacity_state()
        count = int(self.connection_limit_rejection_counts.get(scope, 0)) + 1
        self.connection_limit_rejection_counts[scope] = count
        return count

    def reserve_client_username(self, client: ClientState, worker: WorkerIdentity) -> bool:
        """Atomically reserve an exact Stratum username for one connection."""
        with self.lock:
            self._ensure_connection_capacity_state()
            limit = int(
                getattr(
                    self,
                    "stratum_max_connections_per_username",
                    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME,
                )
            )
            active_for_username = sum(
                1
                for other in self.clients
                if (
                    other is not client
                    and other.worker is not None
                    and other.username == worker.username
                )
            )
            if limit > 0 and active_for_username >= limit:
                rejection_count = self._note_connection_limit_rejection_locked("username")
                if rejection_count == 1 or rejection_count % 100 == 0:
                    print(
                        "prism coordinator: rejected stratum authorization at username limit "
                        f"username={worker.username!r} limit={limit} count={rejection_count}",
                        flush=True,
                    )
                return False
            client.worker = worker
            client.username = worker.username
            return True

    def start_audit_server(self) -> None:
        self.start_health_snapshot_refresher()
        handler_cls = make_audit_handler(self)
        httpd = ThreadingHTTPServer((self.audit_bind or "127.0.0.1", self.audit_port), handler_cls)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
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
        timeout_seconds = getattr(
            self,
            "stratum_send_timeout_seconds",
            DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS,
        )
        if timeout_seconds <= 0:
            return
        seconds = int(timeout_seconds)
        microseconds = int((timeout_seconds - seconds) * 1_000_000)
        try:
            sock.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_SNDTIMEO,
                struct.pack("ll", seconds, microseconds),
            )
        except (AttributeError, OSError, struct.error):
            # Platform without SO_SNDTIMEO support: keep legacy blocking sends.
            return

    def _wait_for_blockpoll_trigger(self) -> bool:
        """Wait for the normal interval or an immediate coalesced retry."""
        remaining = float(self.blockpoll_seconds)
        while remaining > 0:
            if self.stop_event.is_set():
                return False
            wait_seconds = min(remaining, 0.25)
            if self._tip_refresh_retry.wait(wait_seconds):
                self._tip_refresh_retry.clear()
                return not self.stop_event.is_set()
            remaining -= wait_seconds
        return not self.stop_event.is_set()

    def blockpoll_loop(self) -> None:
        self._ensure_tip_refresh_state()
        while self._wait_for_blockpoll_trigger():
            # A superseding observation or post-fanout tip change wakes this
            # fallback loop immediately. The event coalesces repeated signals;
            # ordinary same-tip polling retains its configured interval.
            # Heartbeat at the top of each iteration: reaching here proves the
            # loop is alive. A transient qbit RPC error still loops and beats; a
            # hung RPC call never returns, so the beat goes stale and the
            # watchdog restarts the process.
            self._record_heartbeat("qbit_blockpoll")
            try:
                refreshed = self.poll_qbit_tip_template_once()
                if refreshed:
                    print(
                        f"prism coordinator: refreshed {refreshed} client job(s) after qbit tip/template change",
                        flush=True,
                    )
            except ShutdownInProgress:
                # Admission can close after the loop condition but before a
                # nested reconciliation enters the writer gate. That is an
                # intentional shutdown stop, not a template-health failure.
                return
            except (TemplateRefreshSuperseded, _PayoutStatePublicationBlocked) as exc:
                # Coordination-blocked attempts neither record into nor fire
                # the failure budget. A clock armed by an earlier budgeted
                # failure must wait for the next budgeted failure (or be
                # cleared by the next completed refresh): exiting here would
                # let a transient blip plus ordinary payout/tip churn restart
                # a process whose qbitd RPC is healthy. The retry is already
                # scheduled by the raise site.
                print(
                    f"prism coordinator: tip/template refresh superseded; retrying: {exc}",
                    flush=True,
                )
            except Exception:
                print("prism coordinator: qbit tip/template poll failed", flush=True)
                traceback.print_exc()
                if self.template_refresh_failure_expired(time.monotonic()):
                    print(
                        "prism coordinator: template refresh failure budget exhausted; "
                        "exiting non-zero so the restart policy recovers the process",
                        flush=True,
                    )
                    os._exit(1)

    def template_refresh_failure_expired(self, now: float) -> bool:
        budget = float(
            getattr(
                self,
                "template_refresh_failure_exit_seconds",
                DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
            )
        )
        if budget <= 0:
            return False
        failure_started = getattr(self, "template_refresh_failure_started_monotonic", None)
        return failure_started is not None and now - failure_started >= budget

    def _record_template_refresh_failure(self, now: float) -> None:
        budget = float(
            getattr(
                self,
                "template_refresh_failure_exit_seconds",
                DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
            )
        )
        if (
            budget > 0
            and getattr(self, "template_refresh_failure_started_monotonic", None) is None
        ):
            self.template_refresh_failure_started_monotonic = now

    def blockwait_once(self, known_tip: str) -> str:
        """One waitfornewblock round: returns the tip after the wait.

        qbitd returns as soon as its tip differs from ``known_tip`` (or after
        the server-side timeout, echoing the current tip), so a tip observed
        between our last poll and this call is reported immediately rather
        than being missed for a cycle.
        """
        timeout_seconds = getattr(
            self,
            "blockwait_timeout_seconds",
            DEFAULT_PRISM_BLOCKWAIT_TIMEOUT_SECONDS,
        )
        watchdog_timeout = float(getattr(self, "watchdog_timeout_seconds", 120.0))
        max_rpc_timeout = max(1.0, watchdog_timeout * 0.8)
        timeout_seconds = min(float(timeout_seconds), max(1.0, max_rpc_timeout - 1.0))
        result = self.rpc.call(
            "waitfornewblock",
            [max(1, int(timeout_seconds * 1000)), known_tip],
            timeout=timeout_seconds + 10.0,
        )
        if isinstance(result, dict):
            new_tip = str(result.get("hash", "") or "")
            if new_tip:
                return new_tip
        return known_tip

    def blockwait_loop(self) -> None:
        """Push-style tip detection alongside the interval poller.

        Stale rejects are dominated by the window between a block connecting
        and miners receiving fresh work; the poller alone leaves up to a full
        PRISM_BLOCKPOLL_SECONDS of that window. This loop parks inside
        waitfornewblock and triggers the same refresh path within milliseconds
        of a new tip. The poller stays on as the fallback and still owns
        same-tip template refreshes, which waitfornewblock does not signal.
        Disabled cleanly when qbitd does not support the RPC.
        """
        known_tip: str | None = None
        while not self.stop_event.is_set():
            self._record_heartbeat("qbit_blockwait")
            try:
                if known_tip is None:
                    known_tip = str(self.rpc.call("getbestblockhash"))
                    self.observe_tip_first_seen(known_tip)
                new_tip = self.blockwait_once(known_tip)
                if new_tip == known_tip:
                    if self.stop_event.wait(0.25):
                        return
                    continue
                self.observe_tip_first_seen(new_tip)
                refreshed = self.poll_qbit_tip_template_once(heartbeat_name="qbit_blockwait")
                known_tip = new_tip
                print(
                    f"prism coordinator: blockwait saw new tip {new_tip}; "
                    f"refreshed {refreshed} client job(s)",
                    flush=True,
                )
            except Exception as exc:
                if known_tip is not None and self._blockwait_unsupported(exc):
                    print(
                        "prism coordinator: waitfornewblock unavailable on this qbitd; "
                        "tip detection falls back to blockpoll only",
                        flush=True,
                    )
                    self._remove_watchdog_heartbeat("qbit_blockwait")
                    return
                print("prism coordinator: blockwait pass failed", flush=True)
                traceback.print_exc()
                if self.stop_event.wait(min(5.0, self.blockpoll_seconds)):
                    return

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
        if self.ctv_broadcaster_fee_sats > 0 and not self.ctv_broadcaster_wallet:
            raise ValueError(
                "ctv_broadcaster_wallet is required when ctv_broadcaster_fee_sats is positive"
            )
        broadcaster = CtvFanoutBroadcaster(
            self.rpc.call,
            funding_wallet=self.ctv_broadcaster_wallet,
        )
        return CtvFanoutBroadcastDaemon(
            self.ledger,
            broadcaster,
            fee_sats=self.ctv_broadcaster_fee_sats,
        )

    @ledger_writer_operation("ctv_broadcast_state")
    def run_ctv_fanout_broadcaster_once(
        self,
        *,
        progress_callback: Callable[[], None] | None = None,
    ) -> CtvFanoutDaemonResult:
        if self.ctv_fanout_broadcast_daemon is None:
            self.ctv_fanout_broadcast_daemon = self.make_ctv_fanout_broadcast_daemon()
        return self.ctv_fanout_broadcast_daemon.run_once(
            limit=self.ctv_broadcaster_limit,
            progress_callback=progress_callback,
            chunk_size=int(
                getattr(
                    self,
                    "ctv_broadcaster_chunk_size",
                    DEFAULT_PRISM_CTV_BROADCASTER_CHUNK_SIZE,
                )
            ),
            tip_refresh_pending=self.tip_refresh_is_pending,
            chunk_callback=self.observe_ctv_fanout_broadcaster_chunk,
        )

    def ctv_fanout_broadcaster_loop(self) -> None:
        while not self.stop_event.is_set():
            self._record_heartbeat("ctv_fanout_broadcaster")
            started = time.monotonic()
            shutdown_admission_closed = False
            try:
                try:
                    result = self.run_ctv_fanout_broadcaster_once(
                        progress_callback=self._record_ctv_fanout_broadcaster_progress,
                    )
                except ShutdownInProgress:
                    shutdown_admission_closed = True
                    return
                finally:
                    # Stamp completion before logging or entering the interval
                    # wait. A blocked row never reaches this finally clause, so
                    # the watchdog remains able to recover a wedged operation.
                    self._record_heartbeat("ctv_fanout_broadcaster")
                    if not shutdown_admission_closed:
                        self.observe_ctv_fanout_broadcaster_pass(
                            max(0.0, time.monotonic() - started)
                        )
            except Exception:
                print("prism coordinator: CTV fanout broadcaster pass failed", flush=True)
                traceback.print_exc()
            else:
                if result.yielded_to_tip_refresh:
                    self._record_ctv_fanout_broadcaster_yield()
                if result.scanned_count or result.submitted_count or result.failed_count:
                    print(
                        "prism coordinator: CTV fanout broadcaster "
                        f"scanned={result.scanned_count} "
                        f"submitted={result.submitted_count} "
                        f"updated={result.updated_count} "
                        f"failed={result.failed_count}",
                        flush=True,
                    )
            if self.stop_event.wait(self.ctv_broadcaster_interval_seconds):
                break

    def _tip_refresh_artifacts(
        self,
        snapshot: QbitTipTemplateSnapshot,
    ) -> CachedTemplateArtifacts:
        artifacts = snapshot.template_artifacts
        if (
            artifacts is None
            or artifacts.fingerprint != snapshot.template_fingerprint
            or artifacts.previousblockhash != snapshot.previousblockhash
            or artifacts.generation != snapshot.template_generation
            or snapshot.bestblockhash != snapshot.previousblockhash
            or qbit_template_fingerprint(artifacts.template) != artifacts.fingerprint
            or str(artifacts.template.get("previousblockhash", ""))
            != artifacts.previousblockhash
        ):
            raise TemplateRefreshBlocked(
                "tip/template snapshot does not own matching exact artifacts"
            )
        return artifacts

    def prepare_tip_refresh_bundle(
        self,
        snapshot: QbitTipTemplateSnapshot,
    ) -> CachedJobBundle:
        """Build ready-pool work from immutable shared inputs only.

        Client selection belongs exclusively to fanout. In particular, a
        connection disappearing before or during this build cannot affect the
        signed payout bundle or its cache lifetime.
        """
        artifacts = self._tip_refresh_artifacts(snapshot)
        build_started = time.monotonic()
        try:
            bundle = self.shared_job_bundle(artifacts, mode="ready")
        except TemplateRefreshBlocked:
            raise
        except Exception as exc:
            with self.lock:
                self.job_build_failure_count += 1
            raise TemplateRefreshBlocked("prepared refresh bundle build failed") from exc
        finally:
            self._observe_tip_refresh_seconds(
                "bundle_build",
                time.monotonic() - build_started,
            )
        # Revalidate only the snapshot-owned object. A concurrent cache fill is
        # unrelated to this refresh and cannot replace its exact artifacts.
        artifacts = self._tip_refresh_artifacts(snapshot)
        if (
            bundle.template is not artifacts.template
            or bundle.template_fingerprint != artifacts.fingerprint
            or bundle.template_generation != artifacts.generation
            or str(bundle.template.get("previousblockhash", ""))
            != artifacts.previousblockhash
        ):
            raise TemplateRefreshBlocked(
                "prepared refresh bundle does not match exact template artifacts"
            )
        if bundle.collection_only:
            raise TemplateRefreshBlocked(
                "ready-pool prepared refresh unexpectedly produced a collection bundle"
            )
        return bundle

    def prewarm_current_tip_ready_bundle(self) -> CachedJobBundle | None:
        """Publish one exact current-tip ready bundle before Stratum accepts."""
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            self.job_preparation_pending = True
        try:
            observation_sequence = self._reserve_tip_observation_sequence()
            snapshot = self.fetch_qbit_tip_template_snapshot()
            try:
                reconciled = self.ensure_reorg_reconciled_for_tip(
                    snapshot.bestblockhash
                )
            except Exception as exc:
                raise TemplateRefreshBlocked(
                    "startup reorg reconciliation failed before job preparation"
                ) from exc
            if not reconciled:
                raise TemplateRefreshBlocked(
                    "startup chain view remained untrusted during job preparation"
                )

            ready = self.pool_readiness_latched()
            bundle: CachedJobBundle | None = None
            if ready:
                bundle = self.shared_job_bundle(
                    self._tip_refresh_artifacts(snapshot),
                    None,
                )
                if bundle.collection_only:
                    raise TemplateRefreshBlocked(
                        "startup ready preparation produced collection work"
                    )
                if bundle.payout_state_generation != int(
                    getattr(self, "_payout_state_generation", 0)
                ):
                    raise TemplateRefreshSuperseded(
                        "payout state changed during startup job preparation"
                    )

            if str(self.rpc.call("getbestblockhash")) != snapshot.bestblockhash:
                raise TemplateRefreshSuperseded(
                    "qbit tip changed during startup job preparation"
                )
            if not self.observe_tip_first_seen(
                snapshot.bestblockhash,
                observation_sequence=observation_sequence,
                publish_refresh_observation=True,
            ):
                raise TemplateRefreshSuperseded(
                    "startup job preparation was superseded before publication"
                )
            with self.lock:
                self.tip_template_snapshot = snapshot
            with self._job_cache_lock:
                self._prepared_ready_snapshot = snapshot if bundle is not None else None
                self._prepared_ready_bundle = bundle
            self.last_successful_template_refresh_monotonic = time.monotonic()
            return bundle
        finally:
            with self._job_cache_lock:
                self.job_preparation_pending = False

    def prewarm_startup_jobs(self) -> CachedJobBundle | None:
        """Best-effort startup prewarm; transient blocking defers to blockpoll."""
        try:
            return self.prewarm_current_tip_ready_bundle()
        except TemplateRefreshBlocked as exc:
            # Startup prewarming is an optimization. A transient reconciliation,
            # payout-generation, or tip race must not prevent Stratum listeners
            # from opening; blockpoll and the bounded initial-job queue retry it.
            self._schedule_tip_refresh_retry()
            print(
                "prism coordinator: startup job preparation deferred "
                f"reason={exc}",
                flush=True,
            )
            return None

    def _tip_refresh_token_current_locked(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return bool(
            token.snapshot is snapshot
            and token.tip_hash == snapshot.bestblockhash
            and token.template_fingerprint == snapshot.template_fingerprint
            and token.template_generation == snapshot.template_generation
            and bundle.template_fingerprint == token.template_fingerprint
            and bundle.template_generation == token.template_generation
            and bundle.payout_state_generation == token.payout_state_generation
            and token.payout_state_generation
            == int(getattr(self, "_payout_state_generation", 0))
            and snapshot.template_artifacts is not None
            and bundle.template is snapshot.template_artifacts.template
            and self._tip_refresh_snapshot_current_locked(
                snapshot,
                token.observation_sequence,
            )
        )

    def _tip_refresh_snapshot_current_locked(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> bool:
        current_tip = getattr(self, "current_tip_first_seen", None)
        return bool(
            self.tip_template_snapshot is snapshot
            and current_tip is not None
            and current_tip[0] == snapshot.bestblockhash
            and int(getattr(self, "current_tip_observation_sequence", 0))
            == observation_sequence
        )

    def _validate_prepared_tip_refresh(
        self,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> TipRefreshValidationToken:
        """Perform the O(1) final chain/trust guard and mint its token."""
        artifacts = self._tip_refresh_artifacts(snapshot)
        if (
            bundle.template is not artifacts.template
            or bundle.template_fingerprint != artifacts.fingerprint
            or bundle.template_generation != artifacts.generation
        ):
            raise TemplateRefreshBlocked(
                "prepared refresh bundle changed before final validation"
            )
        try:
            current_tip = str(self.rpc.call("getbestblockhash"))
        except Exception as exc:
            self._schedule_tip_refresh_retry()
            raise TemplateRefreshBlocked(
                "qbit tip validation failed before prepared fanout"
            ) from exc
        if current_tip != snapshot.bestblockhash:
            self._schedule_tip_refresh_retry()
            raise TemplateRefreshSuperseded(
                "qbit tip changed before prepared fanout "
                f"expected={snapshot.bestblockhash} current={current_tip}"
            )
        try:
            chain_view_untrusted = bool(
                getattr(self, "reorg_reconciler_enabled", True)
                and self.qbit_chain_view_untrusted()
            )
        except Exception as exc:
            self._schedule_tip_refresh_retry()
            raise TemplateRefreshBlocked(
                "qbit chain trust check failed before prepared fanout"
            ) from exc
        if chain_view_untrusted:
            self._schedule_tip_refresh_retry()
            raise TemplateRefreshBlocked(
                "qbit chain view became untrusted before prepared fanout"
            )
        token = TipRefreshValidationToken(
            tip_hash=snapshot.bestblockhash,
            template_fingerprint=artifacts.fingerprint,
            template_generation=artifacts.generation,
            payout_state_generation=bundle.payout_state_generation,
            observation_sequence=observation_sequence,
            snapshot=snapshot,
        )
        with self.lock:
            if not self._tip_refresh_token_current_locked(token, bundle, snapshot):
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh was superseded before fanout submission"
                )
        return token

    def _activate_tip_refresh(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: _FanoutCancellation,
    ) -> None:
        with self.lock:
            if not self._tip_refresh_token_current_locked(token, bundle, snapshot):
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh was superseded before cancellation registration"
                )
            active = self._active_tip_refresh
            if active is not None:
                active[1].cancel()
            self._active_tip_refresh = (token, cancel_event)

    def _clear_active_tip_refresh(
        self,
        token: TipRefreshValidationToken,
        cancel_event: _FanoutCancellation,
    ) -> None:
        with self.lock:
            active = self._active_tip_refresh
            if active is not None and active[0] is token and active[1] is cancel_event:
                self._active_tip_refresh = None

    def _prepared_tip_refresh_obsolete(
        self,
        validation_token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: _FanoutCancellation | None,
    ) -> bool:
        if self.stop_event.is_set() or (
            cancel_event is not None and cancel_event.is_set()
        ):
            return True
        with self.lock:
            current = self._tip_refresh_token_current_locked(
                validation_token,
                bundle,
                snapshot,
            )
        if not current and cancel_event is not None:
            cancel_event.cancel()
        return not current

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
        worker_started = time.monotonic()
        started = worker_started if submitted_monotonic is None else submitted_monotonic
        phases = self._job_build_phases()
        phases.clear()
        cancelled = lambda: (
            self._prepared_tip_refresh_obsolete(
                validation_token,
                bundle,
                snapshot,
                cancel_event,
            )
            or getattr(client, "closing", False)
        )
        phases["executor_queue"] = max(0.0, worker_started - started)
        client_lock_started = worker_started
        client_lock_acquired = False
        client_lock_attempted = False
        try:
            while True:
                with self.lock:
                    if (
                        client not in self.clients
                        or client.connection_id != expected_connection_id
                        or getattr(client, "closing", False)
                    ):
                        return RefreshResult("disconnected")
                if cancelled():
                    phases["client_lock"] = max(
                        0.0,
                        time.monotonic() - client_lock_started,
                    )
                    self._record_tip_refresh_cancellation(
                        "client_lock" if client_lock_attempted else "executor_queue"
                    )
                    return RefreshResult("skipped")
                client_lock_attempted = True
                client_lock_acquired = client.job_update_lock.acquire(
                    timeout=PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS
                )
                if client_lock_acquired:
                    break
            phases["client_lock"] = max(
                0.0,
                time.monotonic() - client_lock_started,
            )
            if cancelled():
                self._record_tip_refresh_cancellation("client_lock")
                return RefreshResult("skipped")
            with self.lock:
                if (
                    client not in self.clients
                    or client.connection_id != expected_connection_id
                ):
                    return RefreshResult("disconnected")
                if (
                    not self.client_can_receive_jobs(client)
                    or self.intervening_job_supersedes_snapshot(
                        client.active_job,
                        expected_active_job,
                        snapshot,
                    )
                    or not self.client_needs_tip_template_refresh(client, snapshot)
                ):
                    return RefreshResult("skipped")
            self._ensure_job_cache_state()
            payout_gate_started = time.monotonic()
            with self._payout_state_delivery_gate.delivery_cancelable(
                cancelled,
                generation=bundle.payout_state_generation,
                priority=True,
            ) as payout_admitted:
                phases["payout_gate"] = max(
                    0.0,
                    time.monotonic() - payout_gate_started,
                )
                self._observe_payout_gate_admission(
                    payout_admitted,
                    generation=bundle.payout_state_generation,
                    fallback_wait_seconds=phases["payout_gate"],
                )
                if not payout_admitted or cancelled():
                    self._record_tip_refresh_cancellation("payout_gate")
                    return RefreshResult("skipped")
                fanout_admitted = (
                    cancel_event is None or cancel_event.begin_delivery()
                )
                if not fanout_admitted:
                    self._record_tip_refresh_cancellation("payout_gate")
                    return RefreshResult("skipped")
                try:
                    # The validation token binds this immutable bundle to the
                    # exact observed artifact object. Fanout tasks consult only
                    # in-memory publication/cancellation state, never RPC or
                    # the mutable cache.
                    with self.lock:
                        token_current = self._tip_refresh_token_current_locked(
                            validation_token,
                            bundle,
                            snapshot,
                        )
                        if not token_current:
                            if cancel_event is not None:
                                cancel_event.cancel()
                            return RefreshResult("skipped")
                        if (
                            client not in self.clients
                            or client.connection_id != expected_connection_id
                        ):
                            return RefreshResult("disconnected")
                        if (
                            not self.client_can_receive_jobs(client)
                            or self.intervening_job_supersedes_snapshot(
                                client.active_job,
                                expected_active_job,
                                snapshot,
                            )
                            or not self.client_needs_tip_template_refresh(
                                client,
                                snapshot,
                            )
                        ):
                            return RefreshResult("skipped")
                        clean_jobs = self.client_tip_changed_for_snapshot(
                            client,
                            snapshot,
                        )
                        stamp_started = time.monotonic()
                        context = self.stamp_job_for_client(
                            client,
                            bundle,
                            clean_jobs=clean_jobs,
                        )
                        phases["stamp"] = time.monotonic() - stamp_started
                        client.active_job = context
                        if clean_jobs:
                            for job_id in tuple(client.active_job_ids):
                                self.bury_evicted_job(client, job_id, prune=False)
                                self.jobs.pop(job_id, None)
                            client.active_job_ids.clear()
                            self.prune_evicted_job_graveyard(force=False)
                        self.jobs[context.job.job_id] = context
                        client.active_job_ids.add(context.job.job_id)
                        self.prune_client_active_jobs(client)

                    socket_send_started = time.monotonic()
                    try:
                        self.send_job_update(client, context.job)
                        payout_admitted.mark_delivered()
                    finally:
                        socket_send_finished = time.monotonic()
                        phases["socket_send"] = max(
                            0.0,
                            socket_send_finished - socket_send_started,
                        )
                    self.apply_job_difficulty(client, context.job)
                    self.note_tip_work_delivered(
                        client,
                        str(context.template["previousblockhash"]),
                    )
                    self.note_initial_job_delivered(client, validated_current=True)
                    delivered_monotonic = time.monotonic()
                    self._record_first_payout_delivery(
                        context.payout_state_generation,
                        delivered_monotonic,
                    )
                    if getattr(self, "hot_path_log_enabled", False):
                        print(
                            "prism coordinator: sent prepared job "
                            f"connection={client.connection_id} username={client.username} "
                            f"job={context.job.job_id} "
                            f"elapsed={delivered_monotonic - started:.3f}s",
                            flush=True,
                        )
                    return RefreshResult("sent", delivered_monotonic)
                finally:
                    if cancel_event is not None:
                        cancel_event.end_delivery()
        finally:
            if client_lock_acquired:
                client.job_update_lock.release()
            self.observe_job_build_elapsed(
                max(0.0, time.monotonic() - started),
                phases,
            )

    def _fanout_prepared_tip_refresh(
        self,
        clients: list[ClientState],
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        *,
        observation_sequence: int | None = None,
        expected_active_jobs: dict[ClientState, PrismJobContext | None] | None = None,
        heartbeat_name: str,
    ) -> tuple[int, float | None, float | None, int]:
        executor = self.tip_refresh_executor()
        cancel_event = _FanoutCancellation()
        if observation_sequence is None:
            with self.lock:
                observation_sequence = int(
                    getattr(self, "current_tip_observation_sequence", 0)
                )
        validation_token = self._validate_prepared_tip_refresh(
            bundle,
            snapshot,
            observation_sequence,
        )
        self._activate_tip_refresh(
            validation_token,
            bundle,
            snapshot,
            cancel_event,
        )
        futures: dict[Future[RefreshResult], ClientState] = {}
        submitted_at: dict[Future[RefreshResult], float] = {}
        queued_cancellations: set[Future[RefreshResult]] = set()
        if expected_active_jobs is None:
            with self.lock:
                expected_active_jobs = {
                    client: client.active_job
                    for client in clients
                }
        clients_iter = iter(clients)
        max_inflight = max(1, int(self.tip_refresh_max_workers))

        def record_queued_cancellation(future: Future[RefreshResult]) -> None:
            if future in queued_cancellations:
                return
            queued_cancellations.add(future)
            elapsed = max(0.0, time.monotonic() - submitted_at[future])
            self.observe_job_build_elapsed(elapsed, {"executor_queue": elapsed})
            self._record_tip_refresh_cancellation("executor_queue")

        def cancel_pending_futures(pending: set[Future[RefreshResult]]) -> None:
            cancel_event.cancel()
            for future in pending:
                if future.cancel():
                    record_queued_cancellation(future)

        def submit_available(pending: set[Future[RefreshResult]]) -> None:
            while (
                len(pending) < max_inflight
                and not self.stop_event.is_set()
                and not cancel_event.is_set()
            ):
                with self.lock:
                    token_current = self._tip_refresh_token_current_locked(
                        validation_token,
                        bundle,
                        snapshot,
                    )
                if not token_current:
                    cancel_event.cancel()
                    return
                try:
                    client = next(clients_iter)
                except StopIteration:
                    return
                submitted = time.monotonic()
                active_job = expected_active_jobs.get(client)
                if active_job is None:
                    priority = PRISM_DELIVERY_PRIORITY_INITIAL
                elif self.client_tip_changed_for_snapshot(client, snapshot):
                    priority = PRISM_DELIVERY_PRIORITY_NEW_TIP
                else:
                    priority = PRISM_DELIVERY_PRIORITY_SAME_TIP
                future = self._submit_delivery_task(
                    executor,
                    self.send_prepared_job,
                    client,
                    bundle,
                    snapshot,
                    validation_token,
                    client.connection_id,
                    expected_active_jobs.get(client),
                    cancel_event,
                    submitted,
                    priority=priority,
                )
                self._tip_refresh_future_started()
                future.add_done_callback(self._tip_refresh_future_finished)
                futures[future] = client
                submitted_at[future] = submitted
                pending.add(future)

        pending: set[Future[RefreshResult]] = set()
        try:
            sent = 0
            failed = 0
            first_delivery: float | None = None
            last_delivery: float | None = None
            invalidation: TemplateRefreshBlocked | None = None
            last_live_trust_check = time.monotonic()
            try:
                submit_available(pending)
            except RuntimeError:
                cancel_pending_futures(pending)
                cancel_event.set()
                if pending:
                    wait(pending)
                if not self.stop_event.is_set():
                    self._schedule_tip_refresh_retry()
                    raise
            while pending:
                self._record_heartbeat(heartbeat_name)
                if self.stop_event.is_set() or cancel_event.is_set():
                    cancel_pending_futures(pending)
                done, pending = wait(
                    pending,
                    timeout=PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS,
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    client = futures[future]
                    if future.cancelled():
                        if future not in queued_cancellations:
                            record_queued_cancellation(future)
                        self._record_tip_refresh_client_result("skipped")
                        continue
                    try:
                        result = future.result()
                    except OSError:
                        self._record_tip_refresh_client_result("disconnected")
                        self.disconnect_client(client)
                        continue
                    except TemplateRefreshBlocked as exc:
                        self._record_tip_refresh_client_result("skipped")
                        invalidation = exc
                        cancel_pending_futures(pending)
                        continue
                    except Exception:
                        failed += 1
                        self._record_tip_refresh_client_result("failed")
                        with self.lock:
                            self.job_build_failure_count += 1
                        print(
                            "prism coordinator: prepared job fanout failed "
                            f"connection={client.connection_id} username={client.username}",
                            flush=True,
                        )
                        traceback.print_exc()
                        continue
                    self._record_tip_refresh_client_result(result.result)
                    if result.result == "sent":
                        sent += 1
                        delivered = result.delivered_monotonic
                        if delivered is not None:
                            first_delivery = (
                                delivered
                                if first_delivery is None
                                else min(first_delivery, delivered)
                            )
                            last_delivery = (
                                delivered
                                if last_delivery is None
                                else max(last_delivery, delivered)
                            )
                if (
                    pending
                    and invalidation is None
                    and not self.stop_event.is_set()
                    and time.monotonic() - last_live_trust_check >= 1.0
                ):
                    # Validation tokens keep queued per-client deliveries
                    # RPC-free, but they cannot observe headers advancing
                    # ahead of blocks while the best-block hash stays fixed.
                    # Recheck the live chain view from the fanout driver about
                    # once per second and cancel every delivery still queued
                    # if the view becomes untrusted.
                    try:
                        trusted = self.ensure_reorg_reconciled_for_current_tip(
                            expected_tip_hash=snapshot.bestblockhash,
                        )
                        if not trusted:
                            raise TemplateRefreshBlocked(
                                "qbit chain view became untrusted during prepared fanout"
                            )
                        last_live_trust_check = time.monotonic()
                    except ShutdownInProgress:
                        # Admission can close after the stop-event check above
                        # but before reconciliation enters its writer scope.
                        # Preserve the intentional shutdown signal so the
                        # poller cannot consume its template-failure budget.
                        cancel_pending_futures(pending)
                        raise
                    except TemplateRefreshBlocked as exc:
                        invalidation = exc
                    except Exception as exc:
                        invalidation = TemplateRefreshBlocked(
                            "qbit chain trust check failed during prepared fanout"
                        )
                        invalidation.__cause__ = exc
                    if invalidation is not None:
                        cancel_pending_futures(pending)
                if invalidation is None:
                    try:
                        submit_available(pending)
                    except RuntimeError:
                        cancel_pending_futures(pending)
                        cancel_event.set()
                        if pending:
                            wait(pending)
                        if not self.stop_event.is_set():
                            self._schedule_tip_refresh_retry()
                            raise
            if invalidation is not None:
                cancel_event.set()
                self._schedule_tip_refresh_retry()
                raise invalidation
            with self.lock:
                token_current = self._tip_refresh_token_current_locked(
                    validation_token,
                    bundle,
                    snapshot,
                )
            if not token_current:
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh was superseded during fanout; immediate retry scheduled"
                )
            try:
                post_fanout_tip = str(self.rpc.call("getbestblockhash"))
            except Exception as exc:
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshBlocked(
                    "qbit tip validation failed after prepared fanout; "
                    "immediate retry scheduled"
                ) from exc
            if post_fanout_tip != snapshot.bestblockhash:
                cancel_event.set()
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "qbit tip changed during prepared fanout; immediate retry scheduled "
                    f"expected={snapshot.bestblockhash} current={post_fanout_tip}"
                )
            try:
                post_fanout_untrusted = bool(
                    getattr(self, "reorg_reconciler_enabled", True)
                    and self.qbit_chain_view_untrusted()
                )
            except Exception as exc:
                cancel_event.set()
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshBlocked(
                    "qbit chain trust check failed after prepared fanout; "
                    "immediate retry scheduled"
                ) from exc
            if post_fanout_untrusted:
                cancel_event.set()
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshBlocked(
                    "qbit chain view became untrusted during prepared fanout; "
                    "immediate retry scheduled"
                )
            with self.lock:
                token_current = self._tip_refresh_token_current_locked(
                    validation_token,
                    bundle,
                    snapshot,
                )
            if not token_current:
                cancel_event.set()
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh payout state changed during post-fanout "
                    "validation; immediate retry scheduled"
                )
            return sent, first_delivery, last_delivery, failed
        finally:
            self._clear_active_tip_refresh(validation_token, cancel_event)

    def poll_qbit_tip_template_once(self, *, heartbeat_name: str = "qbit_blockpoll") -> int:
        self._ensure_tip_refresh_state()
        refresh_started = time.monotonic()
        while not self._tip_refresh_lock.acquire(timeout=1.0):
            self._record_heartbeat(heartbeat_name)
            if self.stop_event.is_set():
                return 0
            self._probe_tip_while_refresh_waiting()
        observation_sequence = 0
        pending_signal_token: int | None = None
        try:
            observation_sequence = self._reserve_tip_observation_sequence()
            pending_signal_token = self._claim_tip_refresh_pending()
            # The interval poller has no push notification to mark priority for
            # it. Probe the cheap best-tip RPC before fetching and deriving the
            # template so CTV maintenance can yield as soon as a changed tip is
            # observed, rather than after reconciliation or bundle preparation.
            observed_best_tip = str(self.rpc.call("getbestblockhash"))
            with self.lock:
                current_tip = getattr(self, "current_tip_first_seen", None)
            if current_tip is not None and current_tip[0] != observed_best_tip:
                pending_signal_token = self._mark_tip_refresh_pending_for_poll(
                    pending_signal_token,
                    observation_sequence,
                )
            snapshot = self.fetch_qbit_tip_template_snapshot()
            self.pool_readiness_latched()
            payout_generation_before_reconciliation = int(
                getattr(self, "_payout_state_generation", 0)
            )
            with self.lock:
                previous_snapshot = self.tip_template_snapshot
                # Generation orders concurrent observations but is not itself
                # a template change. Repeated observations of identical work
                # must not trigger a clean fanout on every poll.
                snapshot_changed = previous_snapshot is not None and (
                    previous_snapshot.bestblockhash != snapshot.bestblockhash
                    or previous_snapshot.previousblockhash != snapshot.previousblockhash
                    or previous_snapshot.template_fingerprint
                    != snapshot.template_fingerprint
                )
                if snapshot_changed:
                    clients = [
                        client
                        for client in self.clients
                        if self.client_can_receive_jobs(client)
                    ]
                else:
                    clients = [
                        client
                        for client in self.clients
                        if self.client_can_receive_jobs(client)
                        and self.client_needs_tip_template_refresh(client, snapshot)
                    ]
                # Capture the exact job each client had when this refresh pass
                # selected it. A Vardiff/authorize path may install intervening
                # work while the shared bundle is prepared or while its task
                # waits in the executor queue. Artifact generations let the
                # task replace stale intervening work while preserving work
                # produced from a template stored after this snapshot.
                expected_active_jobs = {
                    client: client.active_job
                    for client in clients
                }

            if clients and snapshot_changed:
                pending_signal_token = self._mark_tip_refresh_pending_for_poll(
                    pending_signal_token,
                    observation_sequence,
                )

            refreshed = 0
            build_failures = 0
            first_delivery: float | None = None
            last_delivery: float | None = None
            self._raise_if_tip_refresh_superseded(
                snapshot,
                observation_sequence,
            )
            try:
                reorg_reconciled = self.ensure_reorg_reconciled_for_tip(
                    snapshot.bestblockhash
                )
            except ShutdownInProgress:
                # Shutdown may close writer admission after this refresh has
                # fetched a snapshot. Leave the refresh incomplete and let
                # the controlled shutdown proceed without consuming the
                # template failure budget or taking the hard-exit path.
                return 0
            except Exception as exc:
                raise TemplateRefreshBlocked(
                    "qbit reorg reconciliation failed before refresh preparation"
                ) from exc
            if not reorg_reconciled:
                raise TemplateRefreshBlocked(
                    "qbit chain view remained untrusted after reorg reconciliation"
                )
            payout_generation_after_reconciliation = int(
                getattr(self, "_payout_state_generation", 0)
            )
            if (
                payout_generation_after_reconciliation
                != payout_generation_before_reconciliation
            ):
                # A same-tip reconciliation can invalidate signed payout state
                # even when no client needed template work at initial
                # selection. Reselect after the ledger mutation so every old-
                # generation job is replaced from the post-reorg snapshot.
                with self.lock:
                    clients = [
                        client
                        for client in self.clients
                        if self.client_can_receive_jobs(client)
                        and self.client_needs_tip_template_refresh(client, snapshot)
                    ]
                    expected_active_jobs = {
                        client: client.active_job
                        for client in clients
                    }
                if clients:
                    pending_signal_token = self._mark_tip_refresh_pending_for_poll(
                        pending_signal_token,
                        observation_sequence,
                    )
            use_prepared_fanout = bool(
                clients
                and getattr(self, "_pool_ready_latched", False)
            )
            ready_mode = bool(getattr(self, "_pool_ready_latched", False))
            bundle: CachedJobBundle | None = None
            if use_prepared_fanout:
                self._raise_if_tip_refresh_superseded(
                    snapshot,
                    observation_sequence,
                )
                try:
                    bundle = self.prepare_tip_refresh_bundle(snapshot)
                except _PayoutStatePublicationBlocked:
                    for _client in clients:
                        self._record_tip_refresh_client_result("skipped")
                    self._schedule_tip_refresh_retry()
                    raise
                except TemplateRefreshBlocked:
                    for _client in clients:
                        self._record_tip_refresh_client_result("failed")
                    raise
                if (
                    bundle.payout_state_generation
                    != payout_generation_after_reconciliation
                ):
                    # Client selection was made against the reconciled
                    # generation above. A later mutation may produce a valid
                    # newer bundle for only that old subset; retry so selection
                    # and the signed payout snapshot advance together.
                    self._schedule_tip_refresh_retry()
                    raise TemplateRefreshSuperseded(
                        "payout state changed after refresh client selection; "
                        "immediate retry scheduled"
                    )

            # A ready-pool pass must validate and build its immutable shared
            # bundle before committing the observed tip. Otherwise a cache or
            # derivation failure can prune retained work without any replacement
            # job ready to fan out. Sequential/collection work has no shared
            # preparation stage, so it commits here immediately before builds.
            if not self.observe_tip_first_seen(
                snapshot.bestblockhash,
                observation_sequence=observation_sequence,
                publish_refresh_observation=True,
            ):
                raise TemplateRefreshSuperseded(
                    "tip/template poll was superseded by a newer tip observation"
                )
            self.prune_evicted_job_graveyard(force=False)
            with self.lock:
                current_tip = getattr(self, "current_tip_first_seen", None)
                if (
                    current_tip is None
                    or current_tip[0] != snapshot.bestblockhash
                    or int(getattr(self, "current_tip_observation_sequence", 0))
                    != observation_sequence
                ):
                    raise TemplateRefreshSuperseded(
                        "tip/template poll was superseded before snapshot publication"
                    )
                self.tip_template_snapshot = snapshot
            if bundle is not None and not bundle.collection_only:
                with self._job_cache_lock:
                    if (
                        bundle.payout_state_generation
                        == self._payout_state_generation
                    ):
                        self._prepared_ready_snapshot = snapshot
                        self._prepared_ready_bundle = bundle

            if not ready_mode:
                with self.lock:
                    eligible_collection_client = any(
                        self.client_can_receive_jobs(client)
                        for client in self.clients
                    )
                if not eligible_collection_client:
                    self._retain_collection_refresh(
                        snapshot,
                        observation_sequence,
                        payout_generation_after_reconciliation,
                    )

            if use_prepared_fanout:
                assert bundle is not None
                (
                    refreshed,
                    first_delivery,
                    last_delivery,
                    build_failures,
                ) = self._fanout_prepared_tip_refresh(
                    clients,
                    bundle,
                    snapshot,
                    observation_sequence=observation_sequence,
                    expected_active_jobs=expected_active_jobs,
                    heartbeat_name=heartbeat_name,
                )
            else:
                for client in clients:
                    if self.stop_event.is_set():
                        break
                    # Collection bundles are worker-specific, so build and
                    # validate each selected target independently.
                    self._record_heartbeat(heartbeat_name)
                    with self.lock:
                        target_connected = client in self.clients
                        target_eligible = (
                            target_connected
                            and self.client_can_receive_jobs(client)
                        )
                    if not target_connected:
                        self._record_tip_refresh_client_result("disconnected")
                        continue
                    if not target_eligible:
                        self._record_tip_refresh_client_result("skipped")
                        continue
                    try:
                        if self.maybe_send_job(
                            client,
                            clean_jobs=self.client_tip_changed_for_snapshot(client, snapshot),
                            raise_on_reorg_failure=True,
                            raise_on_build_failure=True,
                            tip_refresh_snapshot=snapshot,
                            tip_refresh_observation_sequence=observation_sequence,
                        ):
                            delivered = time.monotonic()
                            refreshed += 1
                            first_delivery = (
                                delivered
                                if first_delivery is None
                                else min(first_delivery, delivered)
                            )
                            last_delivery = (
                                delivered
                                if last_delivery is None
                                else max(last_delivery, delivered)
                            )
                            self._record_tip_refresh_client_result("sent")
                        else:
                            self._record_tip_refresh_client_result("skipped")
                    except _JobBuildFailed:
                        build_failures += 1
                        self._record_tip_refresh_client_result("failed")
                    except OSError:
                        self._record_tip_refresh_client_result("disconnected")
                        self.disconnect_client(client)

                if not ready_mode:
                    with self.lock:
                        eligible_collection_client = any(
                            self.client_can_receive_jobs(client)
                            for client in self.clients
                        )
                    if not eligible_collection_client:
                        self._retain_collection_refresh(
                            snapshot,
                            observation_sequence,
                            payout_generation_after_reconciliation,
                        )

                if clients:
                    try:
                        post_fanout_tip = str(self.rpc.call("getbestblockhash"))
                    except Exception as exc:
                        self._schedule_tip_refresh_retry()
                        raise TemplateRefreshBlocked(
                            "qbit tip validation failed after sequential refresh; "
                            "immediate retry scheduled"
                        ) from exc
                    if post_fanout_tip != snapshot.bestblockhash:
                        self._schedule_tip_refresh_retry()
                        raise TemplateRefreshSuperseded(
                            "qbit tip changed during sequential refresh; "
                            "immediate retry scheduled "
                            f"expected={snapshot.bestblockhash} current={post_fanout_tip}"
                        )
                    if int(getattr(self, "_payout_state_generation", 0)) != (
                        payout_generation_after_reconciliation
                    ):
                        self._schedule_tip_refresh_retry()
                        raise TemplateRefreshSuperseded(
                            "payout state changed during sequential refresh; "
                            "immediate retry scheduled"
                        )

            if refreshed == 0 and build_failures:
                raise TemplateRefreshBlocked(
                    f"job builds failed for {build_failures} client(s); no refreshed work was issued"
                )
            if refreshed:
                with self.lock:
                    self.tip_refresh_job_count += refreshed
                assert first_delivery is not None and last_delivery is not None
                self._observe_tip_refresh_seconds(
                    "first_delivery",
                    first_delivery - refresh_started,
                )
                self._observe_tip_refresh_seconds(
                    "last_delivery",
                    last_delivery - refresh_started,
                )
            if not self._clear_tip_refresh_pending_for_completed_refresh(
                snapshot,
                observation_sequence,
                payout_generation_after_reconciliation,
            ):
                # A newer tip or payout mutation won after the last delivery
                # guard. Preserve its pending token and retry immediately.
                pending_signal_token = None
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "tip or payout state changed before refresh completion; "
                    "immediate retry scheduled"
                )
            self.last_successful_template_refresh_monotonic = time.monotonic()
            self.template_refresh_failure_started_monotonic = None
            return refreshed
        except (TemplateRefreshSuperseded, _PayoutStatePublicationBlocked):
            # Coordination-blocked refreshes -- a superseded tip, a pending
            # payout publication fence, a refresh raced by payout mutation --
            # are churn between healthy components, not qbitd unhealthiness.
            # They must not arm the restart budget: sustained payout churn
            # would otherwise self-terminate a process whose RPC is fine, and
            # each restart re-triggers the same churn. Re-raise so callers
            # still schedule their immediate retry. Plain TemplateRefreshBlocked
            # stays budgeted below: it also wraps genuine failures (job builds
            # failing, malformed template artifacts, untrusted chain views)
            # whose persistence must still take the budgeted restart path.
            raise
        except Exception:
            self._record_template_refresh_failure(time.monotonic())
            raise
        finally:
            self._tip_refresh_lock.release()
            self._observe_tip_refresh_seconds(
                "refresh",
                time.monotonic() - refresh_started,
            )

    def _probe_tip_while_refresh_waiting(self) -> None:
        """Publish a changed live tip without entering the heavy refresh lane."""
        observation_sequence = self._reserve_tip_observation_sequence()
        try:
            observed_tip = str(self.rpc.call("getbestblockhash"))
        except Exception:
            # The owning refresh still has to unwind or complete. Preserve its
            # pending state and let the next bounded lock wait probe again.
            return
        with self.lock:
            current_tip = getattr(self, "current_tip_first_seen", None)
        if current_tip is None or current_tip[0] == observed_tip:
            return
        self.observe_tip_first_seen(
            observed_tip,
            observation_sequence=observation_sequence,
            publish_refresh_observation=False,
        )

    def _raise_if_tip_refresh_superseded(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> None:
        """Stop obsolete work before entering another expensive phase."""
        with self.lock:
            current_tip = getattr(self, "current_tip_first_seen", None)
            current_sequence = int(
                getattr(self, "current_tip_observation_sequence", 0)
            )
        if (
            current_tip is not None
            and current_tip[0] != snapshot.bestblockhash
            and current_sequence > observation_sequence
        ):
            self._schedule_tip_refresh_retry()
            raise TemplateRefreshSuperseded(
                "tip/template poll was superseded by a newer tip observation "
                "before refresh preparation"
            )

    def _reserve_tip_observation_sequence(self) -> int:
        with self.lock:
            sequence = int(getattr(self, "tip_observation_sequence", 0)) + 1
            self.tip_observation_sequence = sequence
            return sequence

    def observe_tip_first_seen(
        self,
        tip_hash: str,
        *,
        observation_sequence: int | None = None,
        publish_refresh_observation: bool = False,
    ) -> bool:
        if observation_sequence is None:
            observation_sequence = self._reserve_tip_observation_sequence()
        now = time.monotonic()
        active_to_cancel: _FanoutCancellation | None = None
        tip_changed = False
        with self.lock:
            current_sequence = int(
                getattr(self, "current_tip_observation_sequence", 0)
            )
            if observation_sequence < current_sequence:
                return False
            first_seen = getattr(self, "current_tip_first_seen", None)
            if first_seen is not None and first_seen[0] == tip_hash:
                same_tip = True
                # A same-tip re-observation proves the tip view is live; the
                # freshness stamp bounds submit_stale_check_tip reuse.
                self.current_tip_observed_monotonic = now
                active = getattr(self, "_active_tip_refresh", None)
                # A routine blockwait/poll observation of the same hash carries
                # no newer template. While that hash is actively fanning out,
                # do not invalidate its token merely by advancing the global
                # observation sequence. The next real refresh observation can
                # advance it after the active fanout clears.
                if publish_refresh_observation and (
                    active is None or active[0].tip_hash != tip_hash
                ):
                    self.current_tip_observation_sequence = observation_sequence
            else:
                same_tip = False
                tip_changed = first_seen is not None
                active = getattr(self, "_active_tip_refresh", None)
                if (
                    active is not None
                    and active[0].tip_hash != tip_hash
                    and active[0].observation_sequence < observation_sequence
                ):
                    active_to_cancel = active[1]
                # The first tip this process observes is a startup baseline,
                # not a tip flip: a None stamp keeps stale grace closed.
                self.current_tip_first_seen = (
                    tip_hash,
                    now if first_seen is not None else None,
                )
                # A retained collection snapshot is useful only for its exact
                # tip. The new observation will install a replacement after
                # reconciliation and publication if identity is still absent.
                self._retained_collection_refresh = None
                self.current_tip_observation_sequence = observation_sequence
                self.current_tip_observed_monotonic = now
                self.current_tip_parent = None

        if active_to_cancel is not None:
            active_to_cancel.cancel()
            self._mark_tip_refresh_pending(observation_sequence)
            self._schedule_tip_refresh_retry()
        if same_tip:
            return True
        if tip_changed:
            self._cancel_obsolete_job_bundle_builds(current_tip=tip_hash)
            # Supersede payout preparation immediately. The preparer will
            # discard its old immutable candidate before publication; this
            # marker does not wait for its ledger/RPC work to finish.
            self._ensure_job_cache_state()
            with self.lock:
                current = getattr(self, "current_tip_first_seen", None)
                current_sequence = int(
                    getattr(self, "current_tip_observation_sequence", 0)
                )
                if (
                    current is not None
                    and current[0] == tip_hash
                    and current_sequence == observation_sequence
                    and self._payout_state_source[1] != tip_hash
                ):
                    source_generation = self._payout_state_source[0] + 1
                    self._payout_state_source = (
                        source_generation,
                        tip_hash,
                        "external_tip",
                        now,
                    )
            self._ensure_job_cache_state()
            with self._job_cache_lock:
                self._prepared_ready_bundle = None
                self._prepared_ready_snapshot = None
            self._mark_tip_refresh_pending(observation_sequence)
            self._schedule_tip_refresh_retry()

        # Parent lookup is best-effort cleanup metadata, so never hold the
        # coordinator lock across RPC or fail tip observation when it is
        # temporarily unavailable. Submit classification independently fetches
        # and requires the parent before granting stale grace.
        try:
            parent_hash = self._fetch_tip_parent_hash(tip_hash)
        except Exception:
            parent_hash = None

        with self.lock:
            current = getattr(self, "current_tip_first_seen", None)
            if (
                current is None
                or current[0] != tip_hash
                or int(getattr(self, "current_tip_observation_sequence", 0))
                != observation_sequence
            ):
                return False
            if parent_hash is not None:
                self.current_tip_parent = (tip_hash, parent_hash)
            # Reclassify formerly same-tip entries immediately. On mainnet the
            # zero stale-grace TTL removes them in this pass; on other chains
            # the actual chain parent removes multi-tip-behind entries while
            # the independently configured grace lifetime protects one-back.
            self.prune_evicted_job_graveyard(now=now, force=True)
        return True

    def _fetch_tip_parent_hash(self, tip_hash: str) -> str | None:
        block = self.rpc.call("getblock", [tip_hash])
        if not isinstance(block, dict):
            return None
        parent = str(block.get("previousblockhash", "") or "")
        if not parent:
            return None
        return parent

    def current_tip_parent_hash(self, tip_hash: str) -> str | None:
        with self.lock:
            cached = getattr(self, "current_tip_parent", None)
            if cached is not None and cached[0] == tip_hash:
                return cached[1]
            first_seen = getattr(self, "current_tip_first_seen", None)
            observed_sequence = (
                int(getattr(self, "current_tip_observation_sequence", 0))
                if first_seen is not None and first_seen[0] == tip_hash
                else None
            )
        parent = self._fetch_tip_parent_hash(tip_hash)
        if parent is None:
            return None
        with self.lock:
            current = getattr(self, "current_tip_first_seen", None)
            if (
                observed_sequence is not None
                and current is not None
                and current[0] == tip_hash
                and int(getattr(self, "current_tip_observation_sequence", 0))
                == observed_sequence
            ):
                self.current_tip_parent = (tip_hash, parent)
        return parent

    def submit_stale_check_tip(self) -> str:
        """Best-known chain tip for per-share submit classification.

        Prefers the tip the blockpoll/blockwait observers already confirmed
        (refreshed at least every PRISM_BLOCKPOLL_SECONDS while healthy) so
        mining.submit never blocks on a getbestblockhash RPC per share. This
        also removes the submit-races-ahead-of-the-poller failure mode: a
        submit-path RPC can observe a new tip seconds before jobs refresh, and
        with PRISM_STRATUM_STALE_GRACE_SECONDS=0 (mainnet-forced) that
        rejected every in-flight share on the old tip. Classifying against the
        observed tip keeps shares valid exactly until the coordinator itself
        sees the flip and refreshes work, and it is the same tip source the
        stale-grace window and evicted-job classification are anchored to.

        Fail-safe bound: the observed tip is only trusted while its freshness
        stamp is younger than PRISM_SUBMIT_TIP_MAX_AGE_SECONDS. If tip
        observation stalls (poller failing after a tip change, reconciliation
        refusing a new tip), submits fall back to the live RPC read instead of
        accepting shares against a frozen snapshot indefinitely.
        """
        max_age = float(
            getattr(
                self,
                "submit_tip_max_age_seconds",
                DEFAULT_PRISM_SUBMIT_TIP_MAX_AGE_SECONDS,
            )
        )
        if max_age > 0:
            with self.lock:
                observed = getattr(self, "current_tip_first_seen", None)
                observed_at = getattr(self, "current_tip_observed_monotonic", None)
                # Keep the freshness decision and selected hash in the same
                # critical section as tip observation. Otherwise a poller can
                # publish a newer tip after these fields are copied but before
                # this method returns the superseded hash.
                if (
                    observed is not None
                    and observed_at is not None
                    and time.monotonic() - observed_at <= max_age
                ):
                    return observed[0]
        return str(self.rpc.call("getbestblockhash"))

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
        # Only blockpoll/blockwait anchor current_tip_first_seen. If the refresh
        # path has not observed this tip yet, the window is not open: self-healing
        # from a lagging submit's tip read would extend grace arbitrarily past the
        # real tip change. Fall through to a plain stale-job reject instead.
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
        """Record the first time this connection was sent work for a tip.

        First delivery wins per tip: same-tip template refreshes must not slide
        the connection's stale-grace anchor forward.
        """
        now = time.monotonic()
        with self.lock:
            delivered = client.tip_work_delivered
            if delivered is None or delivered[0] != job_parent_hash:
                client.tip_work_delivered = (job_parent_hash, now)

    def _ensure_evicted_job_state(self) -> None:
        graveyard = getattr(self, "evicted_job_graveyard", None)
        rebuild_indexes = False
        if not isinstance(graveyard, OrderedDict):
            converted: OrderedDict[str, EvictedJobEntry] = OrderedDict()
            for job_id, entry in (graveyard or {}).items():
                if isinstance(entry, EvictedJobEntry):
                    converted[job_id] = entry
                    continue
                context, connection_id, evicted_monotonic = entry
                client = next(
                    (
                        candidate
                        for candidate in getattr(self, "clients", ())
                        if candidate.connection_id == connection_id
                    ),
                    None,
                )
                converted[job_id] = EvictedJobEntry(
                    context=context,
                    connection_id=connection_id,
                    evicted_monotonic=evicted_monotonic,
                    previousblockhash=str(context.template["previousblockhash"]),
                    client=client,
                )
            self.evicted_job_graveyard = converted
            rebuild_indexes = True
        if not hasattr(self, "evicted_jobs_by_connection"):
            self.evicted_jobs_by_connection = {}
            rebuild_indexes = True
        if not hasattr(self, "evicted_same_tip_by_connection"):
            self.evicted_same_tip_by_connection = {}
            rebuild_indexes = True
        if not hasattr(self, "evicted_same_tip_job_ids"):
            self.evicted_same_tip_job_ids = OrderedDict()
            rebuild_indexes = True
        if not hasattr(self, "evicted_job_index_tip_hash"):
            self.evicted_job_index_tip_hash = None
            rebuild_indexes = True
        if not hasattr(self, "evicted_job_next_prune_monotonic"):
            self.evicted_job_next_prune_monotonic = 0.0
        if not hasattr(self, "evicted_job_expiration_counts"):
            self.evicted_job_expiration_counts = {
                job_class: 0 for job_class in PRISM_EVICTED_JOB_CLASSES
            }
        if not hasattr(self, "evicted_job_capacity_eviction_counts"):
            self.evicted_job_capacity_eviction_counts = {
                scope: 0 for scope in PRISM_EVICTED_JOB_CAPACITY_SCOPES
            }
        if not hasattr(self, "evicted_job_submit_counts"):
            self.evicted_job_submit_counts = {
                outcome: 0 for outcome in PRISM_EVICTED_JOB_SUBMIT_OUTCOMES
            }
        if not hasattr(self, "same_tip_job_retention_seconds"):
            self.same_tip_job_retention_seconds = DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS
        if not hasattr(self, "same_tip_job_retention_per_connection"):
            self.same_tip_job_retention_per_connection = (
                DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION
            )
        current_tip = self._current_observed_tip_hash_locked()
        if self.evicted_job_index_tip_hash != current_tip:
            rebuild_indexes = True
        if rebuild_indexes:
            self._rebuild_evicted_job_indexes_locked()

    def _current_observed_tip_hash_locked(self) -> str | None:
        first_seen = getattr(self, "current_tip_first_seen", None)
        if first_seen is not None:
            return str(first_seen[0])
        snapshot = getattr(self, "tip_template_snapshot", None)
        if snapshot is not None:
            return str(snapshot.bestblockhash)
        return None

    def _evicted_job_class_locked(self, entry: EvictedJobEntry) -> str:
        current_tip = self._current_observed_tip_hash_locked()
        if current_tip is None or entry.previousblockhash == current_tip:
            return "same_tip"
        return "stale_grace"

    def _remove_evicted_job_locked(self, job_id: str) -> EvictedJobEntry | None:
        entry = self.evicted_job_graveyard.pop(job_id, None)
        if entry is None:
            return None
        connection_jobs = self.evicted_jobs_by_connection.get(entry.connection_id)
        if connection_jobs is not None:
            connection_jobs.pop(job_id, None)
            if not connection_jobs:
                self.evicted_jobs_by_connection.pop(entry.connection_id, None)
        connection_jobs = self.evicted_same_tip_by_connection.get(entry.connection_id)
        if connection_jobs is not None:
            connection_jobs.pop(job_id, None)
            if not connection_jobs:
                self.evicted_same_tip_by_connection.pop(entry.connection_id, None)
        self.evicted_same_tip_job_ids.pop(job_id, None)
        return entry

    def _index_evicted_job_locked(self, job_id: str, entry: EvictedJobEntry) -> None:
        self.evicted_jobs_by_connection.setdefault(
            entry.connection_id,
            OrderedDict(),
        )[job_id] = None
        if self._evicted_job_class_locked(entry) != "same_tip":
            return
        self.evicted_same_tip_by_connection.setdefault(
            entry.connection_id,
            OrderedDict(),
        )[job_id] = None
        self.evicted_same_tip_job_ids[job_id] = None

    def _rebuild_evicted_job_indexes_locked(self) -> None:
        self.evicted_jobs_by_connection = {}
        self.evicted_same_tip_by_connection = {}
        self.evicted_same_tip_job_ids = OrderedDict()
        for job_id, entry in self.evicted_job_graveyard.items():
            self._index_evicted_job_locked(job_id, entry)
        self.evicted_job_index_tip_hash = self._current_observed_tip_hash_locked()
        self._enforce_evicted_same_tip_capacity_locked()

    def _enforce_evicted_same_tip_capacity_locked(
        self,
        connection_id: int | None = None,
    ) -> None:
        connection_ids = (
            (connection_id,)
            if connection_id is not None
            else tuple(self.evicted_same_tip_by_connection)
        )
        per_connection_cap = int(self.same_tip_job_retention_per_connection)
        for candidate_connection_id in connection_ids:
            job_ids = self.evicted_same_tip_by_connection.get(candidate_connection_id)
            while job_ids is not None and len(job_ids) > per_connection_cap:
                oldest_job_id = next(iter(job_ids))
                self._remove_evicted_job_locked(oldest_job_id)
                self.evicted_job_capacity_eviction_counts["connection"] += 1
                job_ids = self.evicted_same_tip_by_connection.get(candidate_connection_id)

    def _stale_grace_entry_expired_locked(
        self,
        entry: EvictedJobEntry,
        *,
        now: float,
        ttl: float,
    ) -> bool:
        current_tip = self._current_observed_tip_hash_locked()
        first_seen = getattr(self, "current_tip_first_seen", None)
        if (
            ttl <= 0
            or current_tip is None
            or first_seen is None
            or str(first_seen[0]) != current_tip
            or first_seen[1] is None
        ):
            return True

        # Submit eligibility is exactly one chain parent behind, so pruning
        # must use that same relationship. The prior poll observation can lag
        # (for example when authorize/vardiff issued work on an intermediate
        # tip), and using it here would drop work submit would still credit.
        # Until the parent RPC has populated the cache, retain conservatively;
        # submit classification fetches it before granting stale grace.
        cached_parent = getattr(self, "current_tip_parent", None)
        if (
            cached_parent is not None
            and cached_parent[0] == current_tip
            and entry.previousblockhash != cached_parent[1]
        ):
            return True

        client = entry.client
        if client is not None:
            delivered = client.tip_work_delivered
            if delivered is None or delivered[0] != current_tip:
                # Match stale_grace_deadline_open: prior-tip shares stay in
                # flight until this connection receives replacement work.
                return False
            anchor = delivered[1]
        else:
            # Disconnect normally removes these entries. Keep legacy/test
            # orphan state bounded from the refresh path's tip-flip anchor.
            anchor = float(first_seen[1])
        return now - anchor > ttl

    def bury_evicted_job(
        self,
        client: ClientState,
        job_id: str,
        *,
        now: float | None = None,
        prune: bool = True,
    ) -> None:
        with self.lock:
            self._ensure_evicted_job_state()
            context = self.jobs.get(job_id)
            if context is None:
                return
            self._remove_evicted_job_locked(job_id)
            self.evicted_job_graveyard[job_id] = EvictedJobEntry(
                context=context,
                connection_id=client.connection_id,
                evicted_monotonic=time.monotonic() if now is None else now,
                previousblockhash=str(context.template["previousblockhash"]),
                client=client,
            )
            self._index_evicted_job_locked(job_id, self.evicted_job_graveyard[job_id])
            self._enforce_evicted_same_tip_capacity_locked(client.connection_id)
            if prune:
                self.prune_evicted_job_graveyard(now=now, force=False)

    def _evicted_job_expired_locked(
        self,
        entry: EvictedJobEntry,
        *,
        now: float,
    ) -> tuple[str, bool]:
        job_class = self._evicted_job_class_locked(entry)
        if job_class == "same_tip":
            ttl = float(self.same_tip_job_retention_seconds)
            return job_class, ttl <= 0 or now - entry.evicted_monotonic > ttl
        return job_class, self._stale_grace_entry_expired_locked(
            entry,
            now=now,
            ttl=float(
                getattr(
                    self,
                    "stale_grace_seconds",
                    DEFAULT_PRISM_STALE_GRACE_SECONDS,
                )
            ),
        )

    def prune_evicted_job_graveyard(
        self,
        *,
        now: float | None = None,
        force: bool = True,
    ) -> None:
        with self.lock:
            self._ensure_evicted_job_state()
            if not self.evicted_job_graveyard:
                return
            now = time.monotonic() if now is None else now
            if not force and now < self.evicted_job_next_prune_monotonic:
                return
            self.evicted_job_next_prune_monotonic = (
                now + DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS
            )
            for job_id, entry in tuple(self.evicted_job_graveyard.items()):
                job_class, expired = self._evicted_job_expired_locked(entry, now=now)
                if expired:
                    self._remove_evicted_job_locked(job_id)
                    self.evicted_job_expiration_counts[job_class] += 1

    def evicted_job_entry(
        self,
        client: ClientState,
        job_id: str,
    ) -> EvictedJobEntry | None:
        with self.lock:
            self._ensure_evicted_job_state()
            entry = getattr(self, "evicted_job_graveyard", {}).get(job_id)
            if entry is None or entry.connection_id != client.connection_id:
                return None
            job_class, expired = self._evicted_job_expired_locked(
                entry,
                now=time.monotonic(),
            )
            if expired:
                self._remove_evicted_job_locked(job_id)
                self.evicted_job_expiration_counts[job_class] += 1
                return None
            return entry

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
        outcome = (
            "credited_stale_grace"
            if credit_policy == PRISM_CREDIT_POLICY_STALE_GRACE
            else "accepted_same_tip"
        )
        with self.lock:
            self._ensure_evicted_job_state()
            self.evicted_job_submit_counts[outcome] += 1

    def refresh_jobs_after_pending_accepted_block(
        self,
        client: ClientState,
        *,
        heartbeat_name: str = "qbit_blockpoll",
    ) -> int:
        with self.lock:
            block = client.post_accept_refresh_block
            client.post_accept_refresh_block = None
        if block is None:
            return 0
        block_height, block_hash = block
        return self.refresh_jobs_after_accepted_block(
            block_height=block_height,
            block_hash=block_hash,
            heartbeat_name=heartbeat_name,
        )

    def refresh_jobs_after_accepted_block(
        self, *, block_height: int, block_hash: str, heartbeat_name: str = "qbit_blockpoll"
    ) -> int:
        try:
            refreshed = self.poll_qbit_tip_template_once(heartbeat_name=heartbeat_name)
        except Exception:
            with self.lock:
                self.post_accept_refresh_failure_count += 1
            print(
                "prism coordinator: post-accept clean job refresh failed after direct PRISM block "
                f"height={block_height} hash={block_hash}",
                flush=True,
            )
            traceback.print_exc()
            return 0
        if refreshed:
            print(
                "prism coordinator: refreshed "
                f"{refreshed} client job(s) after direct PRISM block "
                f"height={block_height} hash={block_hash}",
                flush=True,
            )
        return refreshed

    def fetch_qbit_tip_template_snapshot(self) -> QbitTipTemplateSnapshot:
        # Reserve ordering before either RPC: a fetch that started on an older
        # view must not become "newer" merely because its template arrived last.
        generation = self._reserve_template_artifact_generation()
        template = self.rpc.call(
            "getblocktemplate",
            [{"rules": qbit_gbt_rules(getattr(self, "qbit_chain", "regtest"))}],
        )
        if not isinstance(template, dict):
            raise RuntimeError("getblocktemplate returned non-object")
        previousblockhash = str(template.get("previousblockhash", "") or "")
        if not previousblockhash:
            raise RuntimeError("getblocktemplate omitted previousblockhash")
        # The template parent is the tip this work actually extends. Validate
        # it after fetching the template so a tip transition between these RPCs
        # cannot produce an old bestblockhash paired with newer work. Reject a
        # template that was superseded before it can enter the shared cache or
        # drive tip observation/graveyard pruning.
        bestblockhash = str(self.rpc.call("getbestblockhash"))
        if bestblockhash != previousblockhash:
            self._schedule_tip_refresh_retry()
            raise TemplateRefreshSuperseded(
                "qbit tip changed while fetching block template "
                f"template_parent={previousblockhash} current={bestblockhash}"
            )
        # The poll already paid for this template; seed the job-build cache so
        # client job builds triggered by the refresh below reuse it instead of
        # refetching one template per client.
        artifacts = self.store_template_artifacts(
            template,
            generation=generation,
        )
        if artifacts is not None:
            return QbitTipTemplateSnapshot(
                bestblockhash=bestblockhash,
                previousblockhash=artifacts.previousblockhash,
                template_fingerprint=artifacts.fingerprint,
                template_generation=artifacts.generation,
                template_artifacts=artifacts,
            )
        raise TemplateRefreshBlocked(
            "unable to derive exact artifacts for observed qbit template"
        )

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
        # A trusted reconciliation for this same tip within the cache window is
        # reused: the blockpoll loop re-reconciles every poll anyway, so
        # per-client job builds do not each need a full ledger reconcile pass.
        # The chain-view trust check is NOT cached: headers can run ahead of
        # the validated tip without the best block hash changing (an arriving
        # reorg), and job issuance must pause immediately, not a TTL later.
        ttl = getattr(
            self,
            "reorg_reconcile_cache_seconds",
            DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
        )
        if ttl > 0:
            with self.lock:
                last_hash = self.last_reorg_reconciled_tip_hash
                trusted = self.last_reorg_reconciled_trusted
                last_monotonic = getattr(self, "last_reorg_reconciled_monotonic", None)
            if (
                trusted
                and last_hash == current_tip
                and last_monotonic is not None
                and time.monotonic() - last_monotonic <= ttl
                and not self.qbit_chain_view_untrusted()
            ):
                return True
        return self.ensure_reorg_reconciled_for_tip(current_tip)

    def ensure_reorg_reconciled_for_tip(self, tip_hash: str) -> bool:
        if not getattr(self, "reorg_reconciler_enabled", True):
            return True
        summary = self.reconcile_prism_pool_blocks_once(tip_hash=tip_hash)
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

    @ledger_writer_operation("payout_reconciliation")
    def reconcile_prism_pool_blocks_once(
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

        def finish(*, trusted: bool) -> dict[str, object]:
            with self.lock:
                self.reorg_inactive_block_count += inactive_blocks_total
                self.reorg_reactivated_block_count += reactivated_blocks_total
                self.matured_payout_count += matured_payouts_total
                self.last_reorg_reconciled_tip_hash = tip_hash
                self.last_reorg_reconciled_trusted = trusted
                self.last_reorg_reconciled_monotonic = time.monotonic()
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
            try:
                with self._payout_state_prepare_lock:
                    prepared_started = time.monotonic()
                    captured_source = self._capture_payout_state_source()
                    payout_changed = False
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
                                candidate = self._prepared_payout_state_candidate(
                                    captured_source
                                )
                                if (
                                    _force_publish
                                    or self._payout_source_requires_publication(
                                        candidate
                                    )
                                ):
                                    candidate_to_publish = candidate
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
                                        reactivated = (
                                            self.ledger.reactivate_pool_block(
                                                block_hash=block_hash,
                                                active_tip_height=active_tip_height,
                                            )
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
                                candidate = (
                                    self._prepared_payout_state_candidate(
                                        captured_source
                                    )
                                )
                                if (
                                    payout_changed
                                    or _force_publish
                                    or self._payout_source_requires_publication(
                                        candidate
                                    )
                                ):
                                    candidate_to_publish = candidate
                    except Exception:
                        inactive_blocks_total += inactive_blocks
                        reactivated_blocks_total += reactivated_blocks
                        matured_payouts_total += matured_payouts
                        # Durable partial mutations close admission before the
                        # preparation lock is released. Publication drains old
                        # socket sends afterward without blocking new ledger
                        # preparation or snapshot acquisition.
                        if payout_changed:
                            error_candidate = (
                                self._prepared_payout_state_candidate(
                                    captured_source
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
                            self.last_reorg_reconciled_tip_hash = tip_hash
                            self.last_reorg_reconciled_trusted = False
                            self.last_reorg_reconciled_monotonic = (
                                time.monotonic()
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
        return (
            not getattr(client, "closing", False)
            and client.subscribed
            and client.authorized
            and client.worker is not None
        )

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
            with self.lock:
                self._pool_ready_latched = True
                self._retained_collection_refresh = None
            return True
        return False

    def client_needs_tip_template_refresh(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        context = client.active_job
        if context is None:
            return True
        if getattr(context, "collection_only", False) and getattr(
            self, "_pool_ready_latched", False
        ):
            # The pool crossed min_ready_miners after this job was issued.
            # A collection job keeps settling solved blocks solver-pays-all,
            # so replace it with windowed work on the next poller pass.
            return True
        template = context.template
        previousblockhash = str(template.get("previousblockhash", ""))
        context_fingerprint = getattr(context, "template_fingerprint", None)
        if context_fingerprint is None:
            context_fingerprint = qbit_template_fingerprint(template)
        context_payout_generation = int(
            getattr(context, "payout_state_generation", 0)
        )
        return (
            previousblockhash != snapshot.bestblockhash
            or previousblockhash != snapshot.previousblockhash
            or context_fingerprint != snapshot.template_fingerprint
            or context_payout_generation
            != int(getattr(self, "_payout_state_generation", 0))
        )

    def intervening_job_supersedes_snapshot(
        self,
        active_job: PrismJobContext | None,
        expected_active_job: PrismJobContext | None,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        if active_job is expected_active_job or active_job is None:
            return False
        active_payout_generation = int(
            getattr(active_job, "payout_state_generation", 0)
        )
        if active_payout_generation < int(
            getattr(self, "_payout_state_generation", 0)
        ):
            # Template ordering cannot make a payout-stale intervening job
            # authoritative. Let the reconciled refresh replace it.
            return False
        active_parent_hash = str(
            getattr(active_job, "template", {}).get("previousblockhash", "")
        )
        if (
            active_parent_hash != snapshot.bestblockhash
            or active_parent_hash != snapshot.previousblockhash
        ):
            # Artifact generations order fetch starts, not chain tips. A fetch
            # for the old tip can start after this exact new-tip observation
            # and therefore carry a larger generation; it must not prevent
            # the new-tip snapshot from replacing that stale work.
            return False
        active_generation = int(getattr(active_job, "template_generation", 0))
        snapshot_generation = int(getattr(snapshot, "template_generation", 0))
        if active_generation <= 0 or snapshot_generation <= 0:
            # Legacy/test contexts without ordering metadata retain the safe
            # behavior: never overwrite an unclassified intervening job.
            return True
        return active_generation >= snapshot_generation

    def client_tip_changed_for_snapshot(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        context = client.active_job
        if context is None:
            return True
        previousblockhash = str(context.template.get("previousblockhash", ""))
        return (
            previousblockhash != snapshot.bestblockhash
            or previousblockhash != snapshot.previousblockhash
            or int(getattr(context, "payout_state_generation", 0))
            != int(getattr(self, "_payout_state_generation", 0))
        )

    def handle_client(self, client: ClientState) -> None:
        reader = None
        try:
            reader = client.sock.makefile("r", encoding="utf-8", newline="\n")
            for line in reader:
                if self.stop_event.is_set():
                    break
                line = line.strip()
                if not line:
                    continue
                request_id: object = None
                try:
                    request = json.loads(line)
                    if not isinstance(request, dict):
                        raise StratumError(20, "request must be an object")
                    request_id = request.get("id")
                    self.handle_request(client, request)
                except json.JSONDecodeError as exc:
                    self.send_error(client, request_id, 20, f"invalid JSON: {exc.msg}")
                except StratumError as exc:
                    self.send_error(client, request_id, exc.code, exc.message, reason=exc.reason)
                    if exc.disconnect:
                        break
                except Exception:
                    print(
                        f"prism coordinator: client thread failed address={client.address}",
                        flush=True,
                    )
                    traceback.print_exc()
                    break
        except (OSError, ValueError) as exc:
            if isinstance(exc, OSError) and exc.errno in {errno.EMFILE, errno.ENFILE}:
                self._record_stratum_resource_exhaustion(
                    listener_name=client.listener_name,
                    location="client-reader",
                    error_number=exc.errno,
                )
            print(
                "prism coordinator: stratum client socket failed "
                f"address={client.address} error={exc!r}",
                flush=True,
            )
        finally:
            try:
                if reader is not None:
                    reader.close()
            except (OSError, ValueError):
                pass
            finally:
                self.disconnect_client(client)
                with self.lock:
                    self._ensure_initial_job_state()
                    if getattr(client, "handler_thread_registered", False):
                        client.handler_thread_registered = False
                        self.handler_thread_count = max(
                            0,
                            self.handler_thread_count - 1,
                        )

    def disconnect_client(self, client: ClientState) -> None:
        # Retire admission and fanout eligibility without waiting behind job
        # delivery. Only the first caller owns socket close and final cleanup.
        with self.lock:
            # The timeout sweeper marks closing while leaving membership in
            # place as its atomic handoff token. Whichever disconnect caller
            # first removes that membership owns socket close and cleanup.
            if getattr(client, "closing", False) and client not in self.clients:
                return
            client.closing = True
            self.clients.discard(client)
            self._cancel_pending_initial_job_locked(client, count=True)

        # Do not take send_lock here: shutdown must interrupt an in-flight
        # sendall as well as the handler's blocking reader.
        try:
            client.close()
        finally:
            # Every mixed lock path uses job_update_lock -> coordinator lock.
            # Retirement above holds neither while this potentially waits.
            with client.job_update_lock:
                with self.lock:
                    for job_id in tuple(client.active_job_ids):
                        self.jobs.pop(job_id, None)
                    client.active_job_ids.clear()
                    client.active_job = None
                    self._ensure_evicted_job_state()
                    for job_id in tuple(
                        self.evicted_jobs_by_connection.get(client.connection_id, ())
                    ):
                        self._remove_evicted_job_locked(job_id)
                    client.authorized = False
                    client.worker = None
                    client.username = ""
            self._retain_current_collection_refresh_if_unrepresented()

    def handle_request(self, client: ClientState, request: dict[str, object]) -> None:
        """Dispatch one request, translating shutdown races to Stratum errors."""
        try:
            self._handle_request(client, request)
        except ShutdownInProgress as exc:
            # A request can pass the initial shutdown check immediately before
            # writer admission closes. Preserve the normal protocol response
            # instead of surfacing a generic client-thread failure.
            raise StratumError(
                20,
                "coordinator is shutting down",
                reason=PRISM_REJECTION_POOL_CLOSED,
                disconnect=True,
            ) from exc

    def _handle_request(self, client: ClientState, request: dict[str, object]) -> None:
        if self.stop_event.is_set() or self._ensure_shutdown_controller().phase != "running":
            raise StratumError(
                20,
                "coordinator is shutting down",
                reason=PRISM_REJECTION_POOL_CLOSED,
                disconnect=True,
            )
        method = request.get("method")
        params = request.get("params", [])
        request_id = request.get("id")
        if not isinstance(method, str):
            raise StratumError(20, "missing method")
        if not isinstance(params, list):
            raise StratumError(20, "params must be an array")

        if method == "mining.configure":
            self.handle_configure(client, request_id, params)
            return
        if method == "mining.subscribe":
            with client.job_update_lock:
                client.subscribed = True
                self.send_result(
                    client,
                    request_id,
                    [[], client.extranonce1_hex, self.extranonce2_size],
                )
                self._note_collection_identity_available(client)
                needs_initial_job = client.authorized
            if needs_initial_job:
                self.request_initial_job_delivery(client)
            return
        if method == "mining.authorize":
            username = str(params[0]) if params else ""
            password = str(params[1]) if len(params) > 1 and params[1] is not None else ""
            # Address validation may use RPC; it is unrelated to client job
            # state and therefore stays outside the job-update lock.
            worker = self.resolve_worker(username)
            with client.job_update_lock:
                was_authorized = client.authorized
                if not self.reserve_client_username(client, worker):
                    raise StratumError(
                        20,
                        "too many connections for username",
                        # A new connection has no useful session to preserve. A
                        # live miner re-authorizing to a full username does: keep
                        # its prior worker/session active after returning the
                        # capacity error.
                        disconnect=not client.authorized,
                    )
                # The password is authoritative for password-derived options: a
                # re-authorize without d=/md= clears any prior override (a stored
                # suggest_difficulty still applies via the request resolution).
                client.requested_difficulty, client.requested_min_difficulty = (
                    parse_stratum_password_options(password)
                )
                target = self.apply_client_difficulty_requests(client)
                if target is not None:
                    current = client.pending_share_difficulty or client.share_difficulty
                    if target != current:
                        if not was_authorized:
                            client.share_difficulty = target
                            client.pending_share_difficulty = None
                        else:
                            client.pending_share_difficulty = target
                        client.difficulty_generation = int(
                            getattr(client, "difficulty_generation", 0)
                        ) + 1
                client.authorization_generation = int(
                    getattr(client, "authorization_generation", 0)
                ) + 1
                client.authorized = True
                client.authorized_monotonic = time.monotonic()
                self.send_result(client, request_id, True)
                self._note_collection_identity_available(client)
            # Exactly one coalesced delivery represents this authorization,
            # including a password-derived difficulty change.
            self.request_initial_job_delivery(client)
            return
        if method == "mining.extranonce.subscribe":
            self.send_result(client, request_id, True)
            return
        if method == "mining.suggest_difficulty":
            self.handle_suggest_difficulty(client, request_id, params)
            return
        if method == "mining.submit":
            accepted_and_closed = self.handle_submit(client, params)
            try:
                self.send_result(client, request_id, True)
            finally:
                self.refresh_jobs_after_pending_accepted_block(client)
            if accepted_and_closed:
                client.close()
            return
        raise StratumError(20, f"unsupported method {method}")

    def handle_suggest_difficulty(self, client: ClientState, request_id: object, params: list[object]) -> None:
        with client.job_update_lock:
            suggested: Decimal | None = None
            if params:
                try:
                    suggested = Decimal(str(params[0]))
                except Exception:
                    suggested = None
                if suggested is not None and (not suggested.is_finite() or suggested <= 0):
                    suggested = None
            if suggested is not None:
                client.suggested_difficulty = suggested
                target = self.apply_client_difficulty_requests(client)
                if target is not None:
                    self.advertise_client_difficulty(client, target)
            self.send_result(client, request_id, True)

    def handle_configure(self, client: ClientState, request_id: object, params: list[object]) -> None:
        extensions = params[0] if params else []
        extension_params = params[1] if len(params) > 1 and isinstance(params[1], dict) else {}
        result: dict[str, object] = {}
        if isinstance(extensions, list):
            for extension in extensions:
                if extension == "version-rolling":
                    miner_mask = 0xFFFFFFFF
                    if "version-rolling.mask" in extension_params:
                        miner_mask = stratum_codec.parse_mask_hex(
                            extension_params["version-rolling.mask"],
                            field_name="version-rolling.mask",
                        )
                    client.version_mask = self.version_mask & miner_mask
                    result["version-rolling"] = client.version_mask != 0
                    result["version-rolling.mask"] = stratum_codec.format_mask_hex(client.version_mask)
                else:
                    result[str(extension)] = False
        self.send_result(client, request_id, result)

    def send_result(self, client: ClientState, request_id: object, result: object) -> None:
        client.send({"id": request_id, "result": result, "error": None})

    def send_error(self, client: ClientState, request_id: object, code: int, message: str, *, reason: str | None = None) -> None:
        data = {"reason_id": reason} if reason is not None else None
        client.send({"id": request_id, "result": None, "error": [code, message, data]})

    def resolve_worker(self, username: str) -> WorkerIdentity:
        payout_address, worker_name = split_worker_username(username)
        try:
            if not payout_address:
                raise StratumError(20, "username base is empty")
            script, p2mr_program_hex = self.validate_p2mr_address(payout_address, label="username base")
        except StratumError as username_error:
            fallback_address = getattr(self, "username_fallback_address", default_prism_username_fallback_address())
            if fallback_address is None:
                raise username_error
            print(
                f"prism coordinator: username {username!r} cannot be used as a payout "
                f"({username_error.message}); using fallback payout {fallback_address}",
                flush=True,
            )
            payout_address = fallback_address
            script, p2mr_program_hex = self.validate_p2mr_address(
                fallback_address,
                label="PRISM_USERNAME_FALLBACK_ADDRESS",
            )
        return WorkerIdentity(
            username=username,
            payout_address=payout_address,
            worker_name=worker_name,
            script_pubkey_hex=script,
            p2mr_program_hex=p2mr_program_hex,
        )

    def validate_p2mr_address(self, address: str, *, label: str) -> tuple[str, str]:
        self._ensure_p2mr_address_cache_state()
        with self._p2mr_address_cache_lock:
            cached = self._p2mr_address_cache.get(address)
            if cached is not None:
                expires_monotonic, cached_result = cached
                if expires_monotonic > time.monotonic():
                    self._p2mr_address_cache.move_to_end(address)
                    return cached_result
                self._p2mr_address_cache.pop(address, None)
            pending = self._p2mr_address_validation_inflight.get(address)
            is_leader = pending is None
            if pending is None:
                pending = _P2mrAddressValidationFlight()
                self._p2mr_address_validation_inflight[address] = pending
            else:
                pending.waiters += 1

        if not is_leader:
            pending.event.wait()
            if pending.result is not None:
                return pending.result
            if pending.error is not None:
                self._raise_shared_p2mr_address_validation_error(pending.error)
            raise RuntimeError("payout address validation completed without a result")

        try:
            validation = self.rpc.call("validateaddress", [address])
            if not isinstance(validation, dict) or not validation.get("isvalid"):
                raise StratumError(20, f"{label} is not a valid qbit address: {address}")
            script = str(validation.get("scriptPubKey") or "")
            if not script.startswith("5220") or len(script) != 68:
                raise StratumError(20, f"{label} does not resolve to a P2MR script: {address}")
            result = (script, script[4:])
            with self._p2mr_address_cache_lock:
                max_entries = int(
                    getattr(
                        self,
                        "payout_address_cache_max_entries",
                        DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES,
                    )
                )
                ttl_seconds = float(
                    getattr(
                        self,
                        "payout_address_cache_ttl_seconds",
                        DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS,
                    )
                )
                if max_entries > 0 and ttl_seconds > 0:
                    self._p2mr_address_cache[address] = (
                        time.monotonic() + ttl_seconds,
                        result,
                    )
                    self._p2mr_address_cache.move_to_end(address)
                    while len(self._p2mr_address_cache) > max_entries:
                        self._p2mr_address_cache.popitem(last=False)
                pending.result = result
            return result
        except BaseException as exc:
            with self._p2mr_address_cache_lock:
                pending.error = exc
            raise
        finally:
            with self._p2mr_address_cache_lock:
                if self._p2mr_address_validation_inflight.get(address) is pending:
                    self._p2mr_address_validation_inflight.pop(address, None)
                pending.event.set()

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

    def _ensure_p2mr_address_cache_state(self) -> None:
        if not hasattr(self, "_p2mr_address_cache_lock"):
            self._p2mr_address_cache_lock = threading.Lock()
        if not hasattr(self, "_p2mr_address_cache"):
            self._p2mr_address_cache = OrderedDict()
        if not hasattr(self, "_p2mr_address_validation_inflight"):
            self._p2mr_address_validation_inflight = {}

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
        with client.job_update_lock:
            return self._maybe_send_job_locked(
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
    ) -> bool:
        if not client.subscribed or not client.authorized or client.worker is None:
            return False
        self._ensure_job_cache_state()
        started = time.monotonic()
        phases = self._job_build_phases()
        phases.clear()
        if getattr(self, "hot_path_log_enabled", False):
            print(
                f"prism coordinator: building job connection={client.connection_id} username={client.username}",
                flush=True,
            )
        phase_started = time.monotonic()
        guarded_refresh = tip_refresh_snapshot is not None
        if guarded_refresh != (tip_refresh_observation_sequence is not None):
            raise ValueError("tip refresh snapshot and observation sequence must be paired")
        if guarded_refresh:
            assert tip_refresh_snapshot is not None
            assert tip_refresh_observation_sequence is not None
            with self.lock:
                refresh_current = self._tip_refresh_snapshot_current_locked(
                    tip_refresh_snapshot,
                    tip_refresh_observation_sequence,
                )
            if not refresh_current:
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "tip refresh snapshot was superseded before client job build"
                )
            try:
                chain_view_untrusted = bool(
                    getattr(self, "reorg_reconciler_enabled", True)
                    and self.qbit_chain_view_untrusted()
                )
            except Exception as exc:
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshBlocked(
                    "qbit chain trust check failed before sequential client job build"
                ) from exc
            if chain_view_untrusted:
                self._schedule_tip_refresh_retry()
                raise TemplateRefreshBlocked(
                    "qbit chain view became untrusted before sequential client job build"
                )
        else:
            try:
                if not self.ensure_reorg_reconciled_for_current_tip():
                    if raise_on_reorg_failure:
                        raise TemplateRefreshBlocked(
                            "qbit chain view became untrusted before client job build"
                        )
                    return False
            except TemplateRefreshBlocked:
                raise
            except Exception as exc:
                print(
                    f"prism coordinator: reorg reconciliation failed before job build "
                    f"connection={client.connection_id} username={client.username}; skipping this job",
                    flush=True,
                )
                traceback.print_exc()
                if raise_on_reorg_failure:
                    raise TemplateRefreshBlocked(
                        "reorg reconciliation failed before client job build"
                    ) from exc
                return False
        phases["reorg"] = time.monotonic() - phase_started
        built_from_guarded_artifacts = bool(
            guarded_refresh
            and tip_refresh_snapshot.template_artifacts is not None
            and "build_job_for_client" not in self.__dict__
        )
        try:
            if built_from_guarded_artifacts:
                assert tip_refresh_snapshot is not None
                assert tip_refresh_snapshot.template_artifacts is not None
                context = self.build_job_for_client_from_artifacts(
                    client,
                    tip_refresh_snapshot.template_artifacts,
                    clean_jobs=clean_jobs,
                )
            else:
                context = self.build_job_for_client(client, clean_jobs=clean_jobs)
        except TemplateRefreshBlocked:
            self._schedule_tip_refresh_retry()
            if guarded_refresh or raise_on_reorg_failure or raise_on_build_failure:
                raise
            return False
        except Exception as exc:
            # A single bad template (e.g. a coinbase whose bytes collide with the
            # extranonce placeholder, or a transient getblocktemplate failure) must
            # never tear down the miner's connection. Log it, count it, and skip
            # this job; the next share/retarget or block change rebuilds a fresh one.
            # Only the build is isolated: nothing has been registered or sent yet, so
            # there is no stale job state. Downstream send failures still surface to
            # handle_client, which disconnects the (now dead) socket and cleans up.
            with self.lock:
                self.job_build_failure_count += 1
            print(
                f"prism coordinator: job build failed connection={client.connection_id} "
                f"username={client.username}; keeping client connected and skipping this template",
                flush=True,
            )
            traceback.print_exc()
            if raise_on_build_failure:
                raise _JobBuildFailed(
                    f"job build failed for connection {client.connection_id}"
                ) from exc
            return False
        # Linearize direct delivery against the immutable publication pointer.
        # Expensive build and ledger reads happened under the preparation lock,
        # outside this admission boundary.
        with self._job_cache_lock:
            current_payout_generation = self._payout_state_generation
            published_tip = self._published_payout_state.source_tip_hash
            publication_blocked = self._payout_state_publication_blocked
            context_payout_generation = int(
                getattr(
                    context,
                    "payout_state_generation",
                    current_payout_generation,
                )
            )
        priority_delivery = (
            not publication_blocked
            and context_payout_generation == current_payout_generation
            and (
                published_tip is None
                or str(context.template.get("previousblockhash", ""))
                == published_tip
            )
        )
        payout_gate_started = time.monotonic()
        with self._payout_state_delivery_gate.delivery_cancelable(
            lambda: context_payout_generation != self._payout_state_generation,
            generation=context_payout_generation,
            priority=priority_delivery,
        ) as payout_admitted:
            payout_gate_wait = max(0.0, time.monotonic() - payout_gate_started)
            phases["payout_gate"] = phases.get("payout_gate", 0.0) + payout_gate_wait
            self._observe_payout_gate_admission(
                payout_admitted,
                generation=context_payout_generation,
                fallback_wait_seconds=payout_gate_wait,
            )
            if not payout_admitted:
                self._schedule_tip_refresh_retry()
                if guarded_refresh:
                    raise TemplateRefreshSuperseded(
                        "payout state changed during client job build"
                    )
                return False
            with self.lock:
                if getattr(client, "closing", False):
                    return False
                if guarded_refresh:
                    assert tip_refresh_snapshot is not None
                    assert tip_refresh_observation_sequence is not None
                    if not self._tip_refresh_snapshot_current_locked(
                        tip_refresh_snapshot,
                        tip_refresh_observation_sequence,
                    ):
                        self._schedule_tip_refresh_retry()
                        raise TemplateRefreshSuperseded(
                            "tip refresh snapshot was superseded during client job build"
                        )
                    artifacts = tip_refresh_snapshot.template_artifacts
                    if built_from_guarded_artifacts and artifacts is not None and (
                        context.template is not artifacts.template
                        or context.template_fingerprint != artifacts.fingerprint
                        or context.template_generation != artifacts.generation
                    ):
                        raise TemplateRefreshBlocked(
                            "client job build did not use the guarded refresh artifacts"
                        )
                client.active_job = context
                if clean_jobs:
                    for job_id in client.active_job_ids:
                        self.bury_evicted_job(client, job_id, prune=False)
                        self.jobs.pop(job_id, None)
                    client.active_job_ids.clear()
                    self.prune_evicted_job_graveyard(force=False)
                self.jobs[context.job.job_id] = context
                client.active_job_ids.add(context.job.job_id)
                self.prune_client_active_jobs(client)
            phase_started = time.monotonic()
            self.send_job_update(client, context.job)
            payout_admitted.mark_delivered()
            self.apply_job_difficulty(client, context.job)
            self.note_tip_work_delivered(client, str(context.template["previousblockhash"]))
            delivered_monotonic = time.monotonic()
            self._record_first_payout_delivery(
                context_payout_generation,
                delivered_monotonic,
            )
            self._consume_retained_collection_refresh(context)
            self.note_initial_job_delivered(
                client,
                validated_current=guarded_refresh,
            )
            phases["send"] = delivered_monotonic - phase_started
            elapsed = time.monotonic() - started
            self.observe_job_build_elapsed(elapsed, phases)
            if getattr(self, "hot_path_log_enabled", False):
                phase_report = ",".join(
                    f"{phase}:{phases[phase]:.3f}"
                    for phase in PRISM_JOB_BUILD_PHASES
                    if phase in phases
                )
                print(
                    f"prism coordinator: sent job connection={client.connection_id} username={client.username} "
                    f"job={context.job.job_id} collection={context.collection_only} elapsed={elapsed:.3f}s "
                    f"phases={phase_report}",
                    flush=True,
                )
            return True

    def prune_client_active_jobs(self, client: ClientState) -> None:
        for job_id in tuple(client.active_job_ids):
            if job_id not in self.jobs:
                client.active_job_ids.discard(job_id)
        ordered_active_job_ids = [
            job_id for job_id in self.jobs if job_id in client.active_job_ids
        ]
        while len(ordered_active_job_ids) > MAX_ACTIVE_PRISM_JOBS_PER_CLIENT:
            oldest_job_id = ordered_active_job_ids.pop(0)
            client.active_job_ids.remove(oldest_job_id)
            self.bury_evicted_job(client, oldest_job_id)
            self.jobs.pop(oldest_job_id, None)

    def send_difficulty(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        self.send_difficulty_value(client, job.share_difficulty)

    def send_difficulty_value(self, client: ClientState, difficulty: Decimal) -> None:
        client.send(self.difficulty_payload(difficulty))

    @staticmethod
    def difficulty_payload(difficulty: Decimal) -> dict[str, object]:
        return {
            "id": None,
            "method": "mining.set_difficulty",
            "params": [float(difficulty)],
        }

    def client_vardiff_config(self, client: ClientState) -> vardiff.VardiffConfig:
        """The difficulty policy for one client: its per-client specialization
        if any, else its listener profile, else the default listener's config
        (clients created without one: tests, legacy callers)."""
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
        if client.minimum_advertised_difficulty <= 0:
            return Decimal("0")
        return max(
            client.minimum_advertised_difficulty,
            self.client_vardiff_config(client).min_difficulty,
        )

    def apply_job_difficulty(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        if not self.client_vardiff_config(client).enabled:
            client.share_difficulty = job.share_difficulty
            client.pending_share_difficulty = None
            return
        pending = client.pending_share_difficulty
        client.share_difficulty = job.share_difficulty
        if pending is not None and job.share_difficulty == pending:
            client.pending_share_difficulty = None

    def apply_client_difficulty_requests(self, client: ClientState) -> Decimal | None:
        """Specialize the client's difficulty policy from its recorded requests
        (password ``d=``/``md=`` and ``mining.suggest_difficulty``), clamped to
        the pristine listener bounds. The listener floor always wins: on a
        high-diff listener no request can drop a client below the configured
        minimum. Explicit ``d=`` outranks a suggestion. Returns the resolved
        target difficulty, or None when the client requested nothing."""
        base = client.listener_vardiff_config or self.vardiff_config
        requested = (
            client.requested_difficulty
            if client.requested_difficulty is not None
            else client.suggested_difficulty
        )
        if requested is None and client.requested_min_difficulty is None:
            # No live requests: drop any stale specialization so the client
            # falls back to the pristine listener policy.
            client.vardiff_config = None
            return None
        floor = base.min_difficulty
        if client.requested_min_difficulty is not None:
            floor = vardiff.clamp(
                client.requested_min_difficulty,
                base.min_difficulty,
                base.max_difficulty,
            )
        if requested is None:
            requested = client.share_difficulty
        target = vardiff.clamp(requested, floor, base.max_difficulty)
        client.vardiff_config = dataclass_replace(
            base,
            min_difficulty=floor,
            startup_difficulty=target,
        )
        return target

    def advertise_client_difficulty(self, client: ClientState, target: Decimal) -> bool:
        """Move a client to an explicitly requested difficulty.

        Before the client can receive jobs the value is applied directly (the
        first set_difficulty/notify pair picks it up). Afterwards it uses the
        same job-gated pending mechanism as vardiff retargets: the difficulty
        is advertised together with the job it applies to, or not at all.
        Returns True only when a fresh set_difficulty/notify pair went out, so
        callers about to send their own job can skip a duplicate pair."""
        with client.job_update_lock:
            return self._advertise_client_difficulty_locked(client, target)

    def _advertise_client_difficulty_locked(
        self,
        client: ClientState,
        target: Decimal,
    ) -> bool:
        applied_directly = False
        schedule_initial = False
        with self.lock:
            current = client.pending_share_difficulty or client.share_difficulty
            if target == current:
                return False
            if not (client.subscribed and client.authorized) or (
                client.active_job is None and "maybe_send_job" not in self.__dict__
            ):
                client.share_difficulty = target
                client.pending_share_difficulty = None
                client.difficulty_generation = int(
                    getattr(client, "difficulty_generation", 0)
                ) + 1
                applied_directly = True
                schedule_initial = bool(
                    client.subscribed
                    and client.authorized
                    and client.worker is not None
                )
            else:
                prior_pending = client.pending_share_difficulty
                prior_generation = int(
                    getattr(client, "difficulty_generation", 0)
                )
                advertised_generation = prior_generation + 1
                client.pending_share_difficulty = target
                client.difficulty_generation = advertised_generation
        if applied_directly:
            if schedule_initial:
                # A pending first-job request captured the previous difficulty
                # generation. Replace it atomically so its cancellation callback
                # hands the client slot to current work instead of disconnecting.
                self.request_initial_job_delivery(client)
            return False
        with self.lock:
            self._ensure_initial_job_state()
            initial_pending = client in self.pending_initial_jobs
        if initial_pending:
            self.request_initial_job_delivery(client)
            return False
        if not self.stop_event.is_set() and self.maybe_send_job(client, clean_jobs=True):
            return True
        with self.lock:
            if (
                client.pending_share_difficulty == target
                and int(getattr(client, "difficulty_generation", 0))
                == advertised_generation
            ):
                client.pending_share_difficulty = prior_pending
                client.difficulty_generation = prior_generation
        return False

    def normalized_prior_balances(self, balances: list[dict[str, object]]) -> list[dict[str, object]]:
        rows = [
            {
                "recipient_id": str(balance.get("recipient_id", "")),
                "order_key": str(balance.get("order_key", "")),
                "p2mr_program_hex": str(balance.get("p2mr_program_hex", "")),
                "balance_sats": int(balance.get("balance_sats", 0)),
            }
            for balance in balances
        ]
        rows.sort(
            key=lambda row: (
                row["order_key"],
                row["recipient_id"],
                row["p2mr_program_hex"],
                row["balance_sats"],
            )
        )
        return rows

    def prior_balances_match_current(self, prior_balances: list[dict[str, object]]) -> bool:
        return self.normalized_prior_balances(prior_balances) == self.normalized_prior_balances(
            self.ledger.current_prior_balances()
        )

    def send_job(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        client.send(self.job_payload(job))

    @staticmethod
    def job_payload(job: direct_stratum.DirectQbitStratumJob) -> dict[str, object]:
        return {
            "id": None,
            "method": "mining.notify",
            "params": [
                job.job_id,
                job.prevhash,
                job.coinb1,
                job.coinb2,
                list(job.merkle_branch),
                job.version,
                job.nbits,
                job.ntime,
                job.clean_jobs,
            ],
        }

    def send_job_update(
        self,
        client: ClientState,
        job: direct_stratum.DirectQbitStratumJob,
    ) -> None:
        # Preserve instance-level send method replacements used by focused
        # tests; normal coordinators use the atomic socket batch below.
        if "send_difficulty" in self.__dict__ or "send_job" in self.__dict__:
            self.send_difficulty(client, job)
            self.send_job(client, job)
            return
        client.send_batch(
            [
                self.difficulty_payload(job.share_difficulty),
                self.job_payload(job),
            ]
        )

    def build_job_for_client(self, client: ClientState, *, clean_jobs: bool) -> PrismJobContext:
        if client.worker is None:
            raise StratumError(20, "client is not authorized")
        self._ensure_job_cache_state()
        artifacts = (
            self._retained_collection_artifacts()
            or self.current_template_artifacts()
        )
        return self.build_job_for_client_from_artifacts(
            client,
            artifacts,
            clean_jobs=clean_jobs,
        )

    def build_job_for_client_from_artifacts(
        self,
        client: ClientState,
        artifacts: CachedTemplateArtifacts,
        *,
        clean_jobs: bool,
    ) -> PrismJobContext:
        self._ensure_job_cache_state()
        phases = self._job_build_phases()
        while True:
            worker = client.worker
            if worker is None:
                raise StratumError(20, "client is not authorized")
            cached_bundle = self.shared_job_bundle(artifacts, worker)
            current_worker = client.worker
            if not cached_bundle.collection_only or current_worker == worker:
                break
            # Reauthorization changed a genuine collection input while the
            # worker-specific bundle was being built. Re-select the latest
            # identity without refetching or discarding the exact artifacts.
        stamp_started = time.monotonic()
        context = self.stamp_job_for_client(client, cached_bundle, clean_jobs=clean_jobs)
        phases["stamp"] = phases.get("stamp", 0.0) + (time.monotonic() - stamp_started)
        return context

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
    ) -> dict[str, Any]:
        share = {
            "share_seq": 1,
            "share_id": "bootstrap-share",
            "miner_id": worker.payout_address,
            "order_key": worker.payout_address,
            "p2mr_program_hex": worker.p2mr_program_hex,
            "share_difficulty": network_difficulty,
            "network_difficulty": network_difficulty,
            "template_height": int(template["height"]) - 1,
            "job_id": "bootstrap-job",
            "job_issued_at_ms": issued_at_ms,
            "accepted_at_ms": issued_at_ms,
            "ntime": int(template["curtime"]),
        }
        return self.build_audit_bundle(
            shares=[share],
            found_block={
                "block_height": int(template["height"]),
                "coinbase_value_sats": int(template["coinbasevalue"]),
                "network_difficulty": network_difficulty,
                "anchor_job_issued_at_ms": issued_at_ms,
            },
            prior_balances=[],
            coinbase_script_sig_suffix_hex=suffix_hex,
            witness_merkle_leaves_hex=direct_stratum.witness_merkle_leaves_hex(transaction_hexes),
            ctv_fee_parent_hash=str(template["previousblockhash"]),
            summary_only=summary_only,
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
        summary_only: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, object] = {
            "found_block": found_block,
            "prior_balances": prior_balances,
            "payout_policy": self.prism_payout_policy(),
            "coinbase_script_sig_suffix_hex": coinbase_script_sig_suffix_hex,
            "witness_merkle_leaves_hex": witness_merkle_leaves_hex or [],
        }
        job_build_phase_local = getattr(self, "_job_build_phase_local", None)
        record_phase_metrics = bool(
            getattr(job_build_phase_local, "tip_refresh_metrics", False)
        )
        if summary_only:
            artifact_started = time.monotonic()
            identity_indexes: dict[tuple[str, str, str], int] = {}
            identities: list[tuple[str, str, str]] = []
            compact_shares: list[tuple[object, ...]] = []
            for share in shares:
                identity = (
                    str(share["miner_id"]),
                    str(share["order_key"]),
                    str(share["p2mr_program_hex"]),
                )
                identity_index = identity_indexes.get(identity)
                if identity_index is None:
                    identity_index = len(identities)
                    identity_indexes[identity] = identity_index
                    identities.append(identity)
                compact_shares.append(
                    (
                        share["share_seq"],
                        share["share_id"],
                        identity_index,
                        share["share_difficulty"],
                        share["job_issued_at_ms"],
                        share["accepted_at_ms"],
                        share.get("credit_policy"),
                    )
                )
            payload["compact_share_identities"] = identities
            payload["compact_shares"] = compact_shares
            if record_phase_metrics:
                self._observe_tip_refresh_build_phase(
                    "serialization_copy",
                    time.monotonic() - artifact_started,
                )
        else:
            payload["shares"] = shares
        ctv_settlement = self.prism_ctv_settlement_config(
            block_height=int(found_block["block_height"]),
            parent_hash=ctv_fee_parent_hash,
        )
        if ctv_settlement is not None:
            payload["ctv_settlement"] = ctv_settlement
        if canonical_output_path is not None and summary_only:
            raise ValueError("canonical output and job summary output are mutually exclusive")
        command = prism_tool_command("qbit-prism-build-audit-bundle") + [
            "--input",
            "-",
            "--signing-key-seed-hex",
            self.signing_seed_hex,
            "--ledger-signing-key-seed-hex",
            self.ledger_attestation_signing_seed_hex,
        ]
        command.append("--job-summary-output" if summary_only else "--canonical-output")
        if record_phase_metrics:
            command.append("--phase-metrics")
        if canonical_output_path is not None:
            canonical_output_path.parent.mkdir(parents=True, exist_ok=True)
        succeeded = False
        created_output = False
        try:
            with ExitStack() as stack:
                if canonical_output_path is None:
                    output = stack.enter_context(
                        tempfile.TemporaryFile(mode="w+", encoding="utf-8")
                    )
                else:
                    output = stack.enter_context(
                        canonical_output_path.open("x+", encoding="utf-8")
                    )
                    created_output = True
                stderr = stack.enter_context(
                    tempfile.TemporaryFile(mode="w+", encoding="utf-8")
                )
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=output,
                    stderr=stderr,
                    text=True,
                    encoding="utf-8",
                    close_fds=True,
                )
                build_control = getattr(
                    job_build_phase_local,
                    "bundle_build_control",
                    None,
                )
                if isinstance(build_control, _JobBundleBuildControl):
                    self._register_job_bundle_process(build_control, process)
                assert process.stdin is not None
                input_byte_count = 0
                serialization_started = time.monotonic()

                class CountingWriter:
                    def write(_self, fragment: str) -> int:
                        nonlocal input_byte_count
                        # json.dump's ensure_ascii default guarantees one output
                        # byte per Python character for this UTF-8 text pipe.
                        input_byte_count += len(fragment)
                        return process.stdin.write(fragment)

                try:
                    # iterencode writes bounded fragments to the child instead
                    # of allocating a second full JSON representation in Python.
                    json.dump(payload, CountingWriter(), separators=(",", ":"))
                except BrokenPipeError:
                    # Prefer the builder's diagnostic below.
                    pass
                except BaseException:
                    process.kill()
                    process.wait()
                    raise
                finally:
                    try:
                        process.stdin.close()
                    except BrokenPipeError:
                        pass
                if record_phase_metrics:
                    self._observe_tip_refresh_build_phase(
                        "serialization_copy",
                        time.monotonic() - serialization_started,
                    )
                    self._record_tip_refresh_ipc_bytes("input", input_byte_count)
                try:
                    returncode = process.wait(
                        timeout=float(
                            getattr(
                                self,
                                "bundle_build_timeout_seconds",
                                DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
                            )
                        )
                    )
                except subprocess.TimeoutExpired as exc:
                    process.kill()
                    process.wait()
                    if record_phase_metrics:
                        with self._tip_refresh_metrics_lock:
                            self.tip_refresh_worker_failures += 1
                    raise RuntimeError(
                        "qbit-prism-build-audit-bundle timed out"
                    ) from exc
                stderr.seek(0)
                error_text = stderr.read()
                if returncode != 0:
                    if (
                        isinstance(build_control, _JobBundleBuildControl)
                        and build_control.cancel_event.is_set()
                    ):
                        raise _JobBundleBuildSuperseded(
                            "audit-builder subprocess was canceled after supersession"
                        )
                    if record_phase_metrics:
                        with self._tip_refresh_metrics_lock:
                            self.tip_refresh_worker_failures += 1
                    raise RuntimeError(
                        f"qbit-prism-build-audit-bundle failed: {error_text}"
                    )
                if (
                    isinstance(build_control, _JobBundleBuildControl)
                    and build_control.cancel_event.is_set()
                ):
                    raise _JobBundleBuildSuperseded(
                        "audit-builder result completed after supersession"
                    )
                output.flush()
                output_size = os.fstat(output.fileno()).st_size
                if record_phase_metrics:
                    self._record_tip_refresh_ipc_bytes("output", output_size)
                    for line in error_text.splitlines():
                        if not line.startswith(PRISM_BUILDER_PHASE_METRICS_PREFIX):
                            continue
                        raw_metrics = line.removeprefix(
                            PRISM_BUILDER_PHASE_METRICS_PREFIX
                        )
                        try:
                            metrics = json.loads(raw_metrics)
                            phase_seconds = metrics.get("phases_seconds", {})
                            if isinstance(phase_seconds, dict):
                                for phase in (
                                    "payout_state_derivation",
                                    "ctv_manifest_construction",
                                    "coinbase_bundle_construction",
                                    "signing_verification",
                                ):
                                    elapsed = phase_seconds.get(phase)
                                    if isinstance(elapsed, (int, float)):
                                        self._observe_tip_refresh_build_phase(
                                            phase,
                                            float(elapsed),
                                        )
                            rust_serialization = sum(
                                float(metrics.get(name, 0.0))
                                for name in (
                                    "input_deserialization_seconds",
                                    "output_serialization_seconds",
                                )
                            )
                            self._observe_tip_refresh_build_phase(
                                "serialization_copy",
                                rust_serialization,
                            )
                        except (TypeError, ValueError, json.JSONDecodeError):
                            # Metrics are diagnostic only. A malformed timing
                            # line must never invalidate an otherwise valid
                            # signed bundle.
                            pass
                if canonical_output_path is not None:
                    os.fsync(output.fileno())
                output.seek(0)
                bundle = json.load(output)
            succeeded = True
            return bundle
        finally:
            if canonical_output_path is not None and created_output and not succeeded:
                try:
                    canonical_output_path.unlink()
                except FileNotFoundError:
                    pass

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
        # A closed pool rejects before any share accounting: post-close submits
        # must not inflate global/per-worker submitted totals (the stale-percent
        # denominator) or vardiff windows they can never contribute to.
        with self.lock:
            if self.accepted_block_count >= self.max_blocks:
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
        with self.lock:
            context = self.jobs.get(job_id)
            if context is not None and job_id not in client.active_job_ids:
                context = None
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
        current_tip = self.submit_stale_check_tip()
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

        try:
            submission = direct_stratum.assemble_submission(
                context.job,
                extranonce2_hex=extranonce2_hex,
                ntime_hex=ntime_hex,
                nonce_hex=nonce_hex,
                version_bits_hex=version_bits_hex,
                version_mask=client.version_mask,
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
        with self.lock:
            if share_key in self.recent_share_keys:
                self.reject_stratum(
                    22,
                    PRISM_REJECTION_DUPLICATE_SHARE,
                    "duplicate share",
                    worker=worker_name,
                )
            if len(self.recent_share_keys) > 50_000:
                self.recent_share_keys.clear()
            self.recent_share_keys.add(share_key)
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
        if block_worthy and context.collection_only:
            with self.lock:
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
                    self.note_evicted_job_submit(credit_policy)
            except BaseException:
                with self.lock:
                    self.recent_share_keys.discard(share_key)
                raise
            return False
        candidate = PrismBlockCandidate(
            context=context,
            submission=submission,
            extranonce1_hex=client.extranonce1_hex,
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
            candidate_intent = self.block_candidate_intent(candidate)
            persist_intent = getattr(self.ledger, "persist_block_candidate_intent", None)
            try:
                if callable(persist_intent):
                    persist_intent(candidate_intent)
                block_landed = self.submit_block_candidate(candidate)
            except BaseException:
                with self.lock:
                    self.recent_share_keys.discard(share_key)
                raise
            if not block_landed:
                outcome = getattr(self, "_block_candidate_outcome", None)
                reason = getattr(outcome, "reason", None) if outcome is not None else None
                retryable_reasons = {None, *PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS}
                if reason in retryable_reasons:
                    # The durable outbox may still land and credit this block.
                    # Close without a Stratum result instead of issuing a false
                    # definitive rejection for an uncertain outcome.
                    with self.lock:
                        self.recent_share_keys.discard(share_key)
                    raise RuntimeError(
                        "block candidate outcome is pending durable retry"
                    )
                if reason not in retryable_reasons:
                    finish = getattr(self.ledger, "mark_block_candidate_abandoned", None)
                    if callable(finish):
                        finish(block_hash=submission.block_hash_hex, error=reason)
                with self.lock:
                    self.recent_share_keys.discard(share_key)
                self.reject_stratum(
                    23,
                    PRISM_REJECTION_LOW_DIFFICULTY,
                    "low difficulty share",
                    worker=worker_name,
                )
            else:
                finish = getattr(self.ledger, "mark_block_candidate_submitted", None)
                if callable(finish):
                    finish(block_hash=submission.block_hash_hex)
                if evicted_entry is not None:
                    self.note_evicted_job_submit(credit_policy)
            return False
        # A block-worthy submission that met the share target is a valid share
        # regardless of the block's fate: credit it now, acknowledge the miner
        # immediately, and land the block from the dedicated submitter thread
        # (ckpool/btcpool/StratumV2 semantics). An orphaned candidate keeps its
        # share credit.
        try:
            candidate_intent = self.block_candidate_intent(candidate)
            self.append_accepted_share(
                client,
                context,
                submission,
                pending_share,
                credit_policy=credit_policy,
                candidate_intent=candidate_intent,
            )
            if evicted_entry is not None:
                self.note_evicted_job_submit(credit_policy)
        except BaseException:
            with self.lock:
                self.recent_share_keys.discard(share_key)
            raise
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

    def pending_share_from_submission(
        self,
        *,
        context: PrismJobContext,
        submission: direct_stratum.DirectQbitSubmission,
        ntime_hex: str,
        credit_policy: str | None = None,
    ) -> PendingShare:
        return PendingShare(
            share_id=f"{context.worker.username}:{submission.block_hash_hex}",
            miner_id=context.worker.payout_address,
            order_key=context.worker.payout_address,
            p2mr_program_hex=context.worker.p2mr_program_hex,
            share_difficulty=self.accepted_share_difficulty(context),
            network_difficulty=max(1, int(context.found_block["network_difficulty"])),
            template_height=int(context.template["height"]) - 1,
            job_id=context.job.job_id,
            job_issued_at_ms=context.issued_at_ms,
            accepted_at_ms=now_ms(),
            ntime=int(ntime_hex, 16),
            credit_policy=credit_policy,
        )

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
        if getattr(self, "share_writer_active", False):
            self.enqueue_share_append(entry, wait=True)
        else:
            self._append_share_entry(entry)
        # Only committed shares affect public accounting, vardiff, and the
        # response that handle_request sends immediately after this returns.
        self.note_worker_accepted_share(context.worker.username, credit_policy)
        self.note_vardiff_accepted_share(client, context.job)

    def enqueue_share_append(self, entry: PendingShareAppend, *, wait: bool = False) -> None:
        queue_obj = getattr(self, "share_append_queue", None)
        if queue_obj is None:
            queue_obj = queue.Queue(maxsize=MAX_PENDING_SHARE_APPENDS)
            self.share_append_queue = queue_obj
        if entry.writer_token is None:
            entry.writer_token = self._ensure_shutdown_controller().reserve_writer(
                "share_persistence"
            )
        try:
            if wait:
                queue_obj.put(
                    entry,
                    timeout=getattr(self, "share_commit_timeout_seconds", 15.0),
                )
            else:
                queue_obj.put_nowait(entry)
        except queue.Full:
            entry.writer_token.finish()
            entry.writer_token = None
            raise StratumError(
                20,
                "share ledger commit queue is full",
                reason=PRISM_REJECTION_INTERNAL_ERROR,
            )
        if not wait:
            return
        # Once admitted, wait for a definite transaction outcome. A local
        # timeout is ambiguous because Postgres may commit immediately after
        # it; the liveness watchdog owns recovery from a wedged writer.
        entry.committed.wait()
        if entry.error is not None:
            raise StratumError(
                20,
                f"share ledger commit failed: {entry.error}",
                reason=PRISM_REJECTION_INTERNAL_ERROR,
            )

    def share_append_loop(self) -> None:
        while True:
            self._record_heartbeat("share_writer")
            queue_obj = getattr(self, "share_append_queue", None)
            if queue_obj is None:
                queue_obj = queue.Queue(maxsize=MAX_PENDING_SHARE_APPENDS)
                self.share_append_queue = queue_obj
            stopping = self.stop_event.is_set()
            try:
                entry = queue_obj.get(timeout=0.2 if stopping else 1.0)
            except queue.Empty:
                controller = self._ensure_shutdown_controller()
                if (
                    stopping
                    and controller.writer_admission_closed()
                    and not controller.has_active_writer(
                        {
                            "share_submission",
                            "share_persistence",
                            "accepted_block_handling",
                        }
                    )
                ):
                    return
                continue
            batch = [entry]
            batch_size = max(1, int(getattr(self, "share_commit_batch_size", 64)))
            deadline = time.monotonic() + max(
                0.0, float(getattr(self, "share_commit_linger_seconds", 0.005))
            )
            if entry.candidate_intent is not None:
                deadline = time.monotonic()
            while len(batch) < batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    next_entry = queue_obj.get(timeout=remaining)
                    batch.append(next_entry)
                    if next_entry.candidate_intent is not None:
                        break
                except queue.Empty:
                    break
            self._append_share_batch(batch)

    def _append_share_batch(self, batch: list[PendingShareAppend]) -> bool:
        """Commit a writer batch, then release every waiting submitter."""
        try:
            append_batch = getattr(self.ledger, "append_batch", None)
            if callable(append_batch):
                records = append_batch(
                    [(entry.pending_share, entry.candidate_intent) for entry in batch]
                )
            else:
                # Compatibility for lightweight test/tool ledgers. Production's
                # Postgres ledger always supplies the atomic batch method.
                records = [self.ledger.append(entry.pending_share) for entry in batch]
            if len(records) != len(batch):
                raise RuntimeError("share ledger returned an incomplete commit batch")
            hot_path_log = getattr(self, "hot_path_log_enabled", False)
            for entry, record in zip(batch, records, strict=True):
                entry.record = record
                if hot_path_log:
                    print(
                        "prism coordinator: accepted share "
                        f"seq={record.share_seq} miner={entry.username} job={entry.job_id} "
                        f"hash={entry.block_hash_hex} collection={entry.collection_only} "
                        f"credit_policy={entry.credit_policy or 'normal'}",
                        flush=True,
                    )
            return True
        except Exception as exc:
            with self.lock:
                self.share_append_failure_count = (
                    int(getattr(self, "share_append_failure_count", 0)) + len(batch)
                )
            for entry in batch:
                entry.error = exc
            print(
                f"prism coordinator: share ledger group commit failed count={len(batch)}",
                flush=True,
            )
            traceback.print_exc()
            return False
        finally:
            for entry in batch:
                entry.committed.set()
                if entry.writer_token is not None:
                    entry.writer_token.finish()
                    entry.writer_token = None

    def _recover_share_to_disk(self, entry: PendingShareAppend, reason: str) -> None:
        """Durably capture an acked share the writer could not persist.

        Appends the canonical pending-share JSON to the recovery file (fsynced)
        so a ledger outage or shutdown never silently loses a share the miner
        was told was accepted; replayed on the next start. Best-effort: if even
        the recovery write fails, log loudly rather than raise on the writer.
        """
        path = getattr(self, "share_recovery_path", None)
        if path is None:
            print(
                "prism coordinator: WOULD LOSE acked share (no recovery path) "
                f"share_id={entry.pending_share.share_id} reason={reason}",
                flush=True,
            )
            return
        try:
            payload = json.dumps(dataclasses.asdict(entry.pending_share), separators=(",", ":"))
        except Exception:
            payload = None
        with getattr(self, "share_recovery_lock", threading.Lock()):
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                if payload is None:
                    raise ValueError("pending share is not serializable")
                with open(path, "a", encoding="utf-8") as handle:
                    handle.write(payload + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                self.shares_recovered_to_disk = (
                    int(getattr(self, "shares_recovered_to_disk", 0)) + 1
                )
                print(
                    "prism coordinator: recovered unpersisted acked share to disk "
                    f"share_id={entry.pending_share.share_id} reason={reason}",
                    flush=True,
                )
            except Exception:
                print(
                    "prism coordinator: FAILED to recover acked share to disk; "
                    f"share may be lost share_id={entry.pending_share.share_id} reason={reason}",
                    flush=True,
                )
                traceback.print_exc()

    @ledger_writer_operation("share_recovery_replay")
    def replay_recovered_shares(self) -> int:
        """Replay any recovery-file shares into the ledger at startup.

        Idempotent: both ledgers raise on a duplicate share_id, so a row already
        committed by an earlier partial replay is skipped (not double-counted)
        and does not stop the pass. The file is cleared only after a clean pass,
        so a transient failure here never drops shares.
        """
        path = getattr(self, "share_recovery_path", None)
        if path is None or not path.exists():
            return 0
        try:
            lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        except Exception:
            print("prism coordinator: could not read share recovery file", flush=True)
            traceback.print_exc()
            return 0
        # Parse line-by-line and skip any single unparseable line rather than
        # aborting the whole replay: a crash mid-append can leave the last line
        # torn, and one torn line must not block the intact shares before it.
        pendings: list[PendingShare] = []
        parse_failed = False
        for line in lines:
            try:
                pendings.append(PendingShare(**json.loads(line)))
            except Exception:
                parse_failed = True
                print("prism coordinator: skipping an unparseable recovered share line", flush=True)
                traceback.print_exc()
        # Replay in acceptance order. A share recovered out of FIFO order (a
        # ledger flap during the shutdown drain, or an overflow-recovered newest
        # share) otherwise sorts by file order; ordering by accepted_at_ms lands
        # each share with a share_seq consistent with when it was accepted, so
        # the reward window stays correctly ordered.
        pendings.sort(key=lambda pending: pending.accepted_at_ms)
        replayed = 0
        skipped_duplicates = 0
        for pending in pendings:
            try:
                self.ledger.append(pending)
                replayed += 1
            except Exception as exc:
                if "duplicate share_id" in str(exc):
                    # Already committed by an earlier (partial) replay. Both
                    # ledgers raise on a duplicate share_id; treat it as done and
                    # keep going so replay is idempotent -- otherwise a retry
                    # after a partial pass would stop on the first committed row
                    # and strand every share after it.
                    skipped_duplicates += 1
                    continue
                print("prism coordinator: failed to replay a recovered share; keeping the file", flush=True)
                traceback.print_exc()
                self.shares_replayed = int(getattr(self, "shares_replayed", 0)) + replayed
                return replayed
        if skipped_duplicates:
            print(
                f"prism coordinator: skipped {skipped_duplicates} already-committed "
                "recovered share(s) during replay",
                flush=True,
            )
        if parse_failed:
            # Keep the file (with its intact-but-already-replayed lines, which
            # the ledger dedups on a re-run) so the torn line is preserved for
            # inspection rather than silently discarded.
            self.shares_replayed = int(getattr(self, "shares_replayed", 0)) + replayed
            if replayed:
                print(f"prism coordinator: replayed {replayed} recovered share(s) into the ledger", flush=True)
            return replayed
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        self.shares_replayed = int(getattr(self, "shares_replayed", 0)) + replayed
        if replayed:
            print(f"prism coordinator: replayed {replayed} recovered share(s) into the ledger", flush=True)
        return replayed

    def _append_share_entry(self, entry: PendingShareAppend, *, retry_until_stopped: bool = False) -> bool:
        """Synchronously append one accepted share.

        On the writer thread a transient ledger failure retries with capped
        backoff so ordering is preserved and nothing is silently lost; the
        synchronous path (no writer) propagates the exception exactly as the
        pre-async code did.

        Returns True when the share was persisted to the ledger, or False when
        it was recovered to disk instead (ledger still down at shutdown). The
        caller uses that to keep the shutdown drain in order.
        """
        backoff_seconds = 0.5
        while True:
            try:
                append_batch = getattr(self.ledger, "append_batch", None)
                if callable(append_batch):
                    record = append_batch(
                        [(entry.pending_share, entry.candidate_intent)]
                    )[0]
                else:
                    record = self.ledger.append(entry.pending_share)
                entry.record = record
                break
            except Exception:
                if not retry_until_stopped:
                    raise
                with self.lock:
                    self.share_append_failure_count = (
                        int(getattr(self, "share_append_failure_count", 0)) + 1
                    )
                print(
                    "prism coordinator: ledger share append failed; retrying "
                    f"share_id={entry.pending_share.share_id}",
                    flush=True,
                )
                traceback.print_exc()
                if self.stop_event.wait(backoff_seconds):
                    # Shutting down mid-outage: do not silently drop this
                    # already-acked, already-counted share -- recover it to
                    # disk for replay on the next start.
                    self._recover_share_to_disk(entry, "ledger unavailable at shutdown")
                    return False
                backoff_seconds = min(backoff_seconds * 2, 5.0)
                self._record_heartbeat("share_writer")
        if getattr(self, "hot_path_log_enabled", False):
            print(
                "prism coordinator: accepted share "
                f"seq={record.share_seq} miner={entry.username} job={entry.job_id} "
                f"hash={entry.block_hash_hex} collection={entry.collection_only} "
                f"credit_policy={entry.credit_policy or 'normal'}",
                flush=True,
            )
        entry.committed.set()
        return True

    def accepted_share_difficulty(self, context: PrismJobContext) -> int:
        override = self.share_weights_by_username.get(
            context.worker.username,
            self.share_weights_by_username.get(context.worker.payout_address),
        )
        if override is not None:
            return max(1, int(override))
        return scaled_target_difficulty(context.job.share_target)

    def note_vardiff_submitted_share(self, client: ClientState) -> None:
        self.submitted_share_count += 1
        if not self.client_vardiff_config(client).enabled:
            return
        with self.lock:
            client.vardiff_window_submitted += 1

    def note_vardiff_accepted_share(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        config = self.client_vardiff_config(client)
        if not config.enabled:
            return
        now = time.monotonic()
        with self.lock:
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

    def vardiff_idle_sweep_loop(self) -> None:
        while not self.stop_event.wait(self.vardiff_idle_sweep_seconds):
            self._record_heartbeat("vardiff_idle_sweep")
            try:
                retargeted = self.vardiff_idle_sweep_once()
                if retargeted:
                    print(
                        f"prism coordinator: idle vardiff sweep retargeted {retargeted} client(s)",
                        flush=True,
                    )
            except Exception:
                print("prism coordinator: idle vardiff sweep failed", flush=True)
                traceback.print_exc()

    def vardiff_idle_sweep_once(self) -> int:
        now = time.monotonic()
        with self.lock:
            clients = [
                client
                for client in self.clients
                if self.client_can_receive_jobs(client)
                and self.client_vardiff_config(client).enabled
                and client.active_job is not None
            ]
        retargeted = 0
        for client in clients:
            self._record_heartbeat("vardiff_idle_sweep")
            config = self.client_vardiff_config(client)
            with self.lock:
                elapsed = Decimal(str(max(0.001, now - client.vardiff_window_started_monotonic)))
                if elapsed < config.retarget_interval_seconds:
                    continue
                if client.vardiff_window_accepted != 0:
                    continue
                if client.vardiff_window_submitted != 0:
                    continue
                current_difficulty = client.pending_share_difficulty or client.share_difficulty
            # Do not reset the window here. retarget_client(require_idle=True)
            # resets it atomically with the step-down commit only if the client
            # is still idle at that point; if a share is accepted meanwhile, the
            # accept path owns the window and the speculative step-down aborts.
            try:
                if self.retarget_client(
                    client,
                    current_difficulty=current_difficulty,
                    accepted_shares=0,
                    submitted_shares=0,
                    accepted_difficulty=Decimal("0"),
                    elapsed_seconds=elapsed,
                    require_idle=True,
                ):
                    retargeted += 1
            except OSError:
                self.disconnect_client(client)
        if retargeted:
            with self.lock:
                self.idle_retarget_count += retargeted
        return retargeted

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
    ) -> bool:
        with client.job_update_lock:
            return self._retarget_client_locked(
                client,
                current_difficulty=current_difficulty,
                accepted_shares=accepted_shares,
                submitted_shares=submitted_shares,
                accepted_difficulty=accepted_difficulty,
                elapsed_seconds=elapsed_seconds,
                require_idle=require_idle,
            )

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
    ) -> bool:
        config = self.client_vardiff_config(client)
        if not config.enabled:
            return False
        observed_difficulty = vardiff.observed_difficulty(
            accepted_difficulty=accepted_difficulty,
            elapsed_seconds=elapsed_seconds,
            target_share_interval_seconds=config.target_share_interval_seconds,
        )
        with self.lock:
            previous_estimate = client.vardiff_difficulty_estimate
        if observed_difficulty is None:
            difficulty_estimate = None
            with self.lock:
                client.vardiff_difficulty_estimate = None
        else:
            difficulty_estimate = vardiff.smooth_difficulty_estimate(
                observed=observed_difficulty,
                previous=previous_estimate,
                config=config,
            )
            with self.lock:
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
        idle_window_started: float | None = None
        idle_window_reset_at: float | None = None
        with self.lock:
            previous_difficulty = client.pending_share_difficulty or client.share_difficulty
            if previous_difficulty != current_difficulty:
                return False
            if require_idle and (
                client.vardiff_window_accepted != 0 or client.vardiff_window_submitted != 0
            ):
                # A share landed since the idle snapshot; the accept path owns
                # this window. Abort the speculative step-down rather than
                # overriding a client that just resumed submitting.
                return False
            if require_idle:
                # Restart the window atomically with the step-down so a genuinely
                # idle client only retargets once per interval, and only when the
                # commit actually fires. The pre-reset clock is kept so a failed
                # send below can un-restart it (counters are provably zero here,
                # so restoring the clock alone reconstructs the window).
                idle_window_started = client.vardiff_window_started_monotonic
                idle_window_reset_at = time.monotonic()
                client.vardiff_window_started_monotonic = idle_window_reset_at
                client.vardiff_window_accepted = 0
                client.vardiff_window_submitted = 0
                client.vardiff_window_work = Decimal("0")
            prior_pending = client.pending_share_difficulty
            client.pending_share_difficulty = next_difficulty
        # Advertise the new difficulty together with the new job, gated on a
        # successful build: maybe_send_job sends mining.set_difficulty and
        # mining.notify together, or nothing if the build is skipped. A skipped
        # build then leaves the client on its existing job at its existing
        # difficulty (a consistent pair) so it keeps producing accepted shares and
        # the next one retargets again -- rather than advertising a difficulty for
        # a job it never received, which (since retargets only fire on accepted
        # shares) could wedge a client whose easier shares now miss the old target.
        try:
            if (
                client.authorized
                and client.subscribed
                and not self.stop_event.is_set()
                and self.maybe_send_job(client, clean_jobs=True)
            ):
                return True
        except OSError:
            with self.lock:
                if client.pending_share_difficulty == next_difficulty:
                    client.pending_share_difficulty = prior_pending
                self._restore_idle_window_clock(client, idle_window_started, idle_window_reset_at)
            raise
        with self.lock:
            if client.pending_share_difficulty == next_difficulty:
                client.pending_share_difficulty = prior_pending
            self._restore_idle_window_clock(client, idle_window_started, idle_window_reset_at)
        return False

    @staticmethod
    def _restore_idle_window_clock(
        client: ClientState,
        idle_window_started: float | None,
        idle_window_reset_at: float | None,
    ) -> None:
        """Un-restart the idle vardiff window after a step-down that never
        reached the miner (skipped build/send), so the next sweep can retry
        immediately instead of waiting out another full retarget interval.
        Caller must hold self.lock. No-op unless this retarget did the reset
        and nothing else has restarted the window since."""
        if idle_window_reset_at is None or idle_window_started is None:
            return
        if client.vardiff_window_started_monotonic == idle_window_reset_at:
            client.vardiff_window_started_monotonic = idle_window_started

    def enqueue_block_candidate(self, candidate: PrismBlockCandidate) -> bool:
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

    @ledger_writer_operation("accepted_block_handling")
    def replay_pending_block_candidates(self) -> int:
        """Queue durable candidate intents not completed by an earlier process."""
        pending_rows = getattr(self.ledger, "pending_block_candidate_rows", None)
        if callable(pending_rows):
            durable_rows = pending_rows(limit=MAX_PENDING_BLOCK_CANDIDATES)
        else:
            pending = getattr(self.ledger, "pending_block_candidates", None)
            if not callable(pending):
                return 0
            durable_rows = [
                {
                    "block_hash": (
                        intent.get("block_hash_hex", "")
                        if isinstance(intent, dict)
                        else ""
                    ),
                    "candidate": intent,
                }
                for intent in pending(limit=MAX_PENDING_BLOCK_CANDIDATES)
            ]
        queue_obj = getattr(self, "block_candidate_queue", None)
        if queue_obj is not None and not queue_obj.empty():
            return 0
        queued = 0
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
                if self.enqueue_block_candidate(self.block_candidate_from_intent(intent)):
                    queued += 1
            except Exception:
                print("prism coordinator: invalid durable block candidate intent", flush=True)
                traceback.print_exc()
                quarantine = getattr(self.ledger, "mark_block_candidate_abandoned", None)
                if durable_block_hash and callable(quarantine):
                    try:
                        quarantined = quarantine(
                            block_hash=durable_block_hash,
                            error="invalid durable candidate intent",
                        )
                        if quarantined:
                            self._clear_block_candidate_retry_state(durable_block_hash)
                            with self.lock:
                                self.block_candidate_poisoned_count = int(
                                    getattr(self, "block_candidate_poisoned_count", 0)
                                ) + 1
                    except Exception:
                        traceback.print_exc()
        if queued:
            print(
                f"prism coordinator: replayed {queued} pending block candidate(s)",
                flush=True,
            )
        return queued

    def block_submit_loop(self) -> None:
        while not self.stop_event.is_set():
            self._record_heartbeat("block_submitter")
            try:
                self.replay_pending_block_candidates()
                self.submit_next_block_candidate(timeout=1.0)
            except ShutdownInProgress:
                # Admission can close after the loop condition. Durable block
                # candidates remain in the outbox for the replacement writer.
                return

    def submit_next_block_candidate(self, timeout: float | None = None) -> bool:
        queue_obj = getattr(self, "block_candidate_queue", None)
        if queue_obj is None:
            return False
        try:
            if timeout is None:
                candidate = queue_obj.get_nowait()
            else:
                candidate = queue_obj.get(timeout=timeout)
        except queue.Empty:
            return False

        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.refresh_client = None
        try:
            with self._writer_operation("accepted_block_handling"):
                ran = self._submit_next_block_candidate_writer(candidate)
                refresh_client = getattr(outcome, "refresh_client", None)
                outcome.refresh_client = None
        except ShutdownInProgress:
            # The durable outbox remains pending and the replacement process
            # will replay it. Dequeuing the in-memory wakeup during the
            # admission-close race cannot lose candidate work.
            return False
        # Fresh-job fanout is deliberately outside the writer admission. Once
        # the candidate outbox is finalized it cannot mutate the ledger, so a
        # blocked client send must not hold the writer lease during shutdown.
        if refresh_client is not None and not self.stop_event.is_set():
            self.refresh_jobs_after_pending_accepted_block(
                refresh_client,
                heartbeat_name="block_submitter",
            )
        return ran

    def _submit_next_block_candidate_writer(self, candidate: PrismBlockCandidate) -> bool:
        """Land one dequeued block candidate; returns True when one ran.

        The block-submitter loop calls this continuously; tests call it
        directly to drain the queue deterministically.
        """
        accepted = False
        error = "candidate became stale or submission failed"
        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.reason = None
        try:
            accepted = self.submit_block_candidate(candidate)
        except Exception:
            error = "candidate submission raised an exception"
            print(
                "prism coordinator: block candidate submission failed "
                f"hash={candidate.submission.block_hash_hex}",
                flush=True,
            )
            traceback.print_exc()
        block_hash = str(candidate.submission.block_hash_hex).lower()
        abandon_reason = getattr(outcome, "reason", None) if outcome is not None else None
        retryable = not accepted and (
            abandon_reason is None
            or abandon_reason in PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS
        )
        if retryable:
            # Leave the outbox row pending. It will replay after a short pause
            # or on process restart; turning an infrastructure outage into a
            # terminal abandonment would recreate the loss this outbox avoids.
            print(
                "prism coordinator: retained block candidate for retry "
                f"hash={block_hash} reason={abandon_reason or 'exception'}",
                flush=True,
            )
            with self.lock:
                self.block_candidate_retry_count = int(
                    getattr(self, "block_candidate_retry_count", 0)
                ) + 1
            self.stop_event.wait(self._next_block_candidate_retry_delay(block_hash))
            return True
        self._clear_block_candidate_retry_state(block_hash)
        finish_name = (
            "mark_block_candidate_submitted"
            if accepted
            else "mark_block_candidate_abandoned"
        )
        finish = getattr(self.ledger, finish_name, None)
        if callable(finish):
            try:
                if accepted:
                    finish(block_hash=block_hash)
                else:
                    finish(block_hash=block_hash, error=error)
            except Exception:
                # Keep the coordinator alive. If the terminal-state update
                # failed, restart replay is exact-idempotent and will retry.
                print(
                    "prism coordinator: could not finalize durable block candidate "
                    f"hash={block_hash}",
                    flush=True,
                )
                traceback.print_exc()
        if accepted:
            outcome.refresh_client = candidate.client
        return True

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

    def _clear_block_candidate_retry_state(self, block_hash: str) -> None:
        with self.lock:
            delays = getattr(self, "block_candidate_retry_delays", None)
            if delays is not None:
                delays.pop(block_hash, None)

    def _defer_block_candidate(self, reason: str, message: str, *, worker: str | None) -> None:
        """Record a retryable outcome without counting a terminal abandonment."""
        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.reason = reason
        print(
            f"prism coordinator: block candidate deferred reason={reason}: {message}",
            flush=True,
        )

    def _abandon_block_candidate(self, reason: str, message: str, *, worker: str | None) -> None:
        """Record a lost/failed block candidate as a BLOCK-path event.

        The share that produced the candidate was acknowledged and, when it met
        the share target, credited at submit time; the block losing its race
        afterwards does not un-earn it and is NOT a share rejection. It is
        counted under a dedicated block-abandonment counter (by reason, so a
        benign 'tip moved' race is distinguishable from a real
        submitblock-rejected/ledger failure) rather than the share-reject
        counters, which stay a true measure of shares refused to miners.
        """
        if reason in PRISM_RETRYABLE_BLOCK_CANDIDATE_REASONS:
            self._defer_block_candidate(reason, message, worker=worker)
            return
        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.reason = reason
        with self.lock:
            counts = getattr(self, "block_candidate_abandoned_counts", None)
            if counts is None:
                counts = {}
                self.block_candidate_abandoned_counts = counts
            counts[reason] = int(counts.get(reason, 0)) + 1
        print(
            f"prism coordinator: block candidate abandoned reason={reason}: {message}",
            flush=True,
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

    @ledger_writer_operation("accepted_block_handling")
    def submit_block_candidate(self, candidate: PrismBlockCandidate) -> bool:
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
        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.reason = None
        context = candidate.context
        submission = candidate.submission
        worker = candidate.client.username or None
        with self.lock:
            pool_closed = self.accepted_block_count >= self.max_blocks
        if pool_closed:
            self._abandon_block_candidate(
                PRISM_REJECTION_POOL_CLOSED,
                "pool is no longer accepting blocks",
                worker=worker,
            )
            return False
        expected_height = int(context.template["height"])
        block_hash = str(submission.block_hash_hex).lower()
        parent_hash = str(context.template["previousblockhash"])
        current_tip = str(self.rpc.call("getbestblockhash"))
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
                    worker=worker,
                )
                return False
        already_active = landed_height == expected_height
        if landed_height is not None and not already_active:
            self._abandon_block_candidate(
                PRISM_REJECTION_BLOCK_STALE,
                f"candidate active at unexpected height {landed_height}",
                worker=worker,
            )
            return False
        if already_active:
            print(
                "prism coordinator: resuming finalization for active block candidate "
                f"height={landed_height} hash={submission.block_hash_hex}",
                flush=True,
            )
        elif parent_hash != current_tip:
            self._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                f"tip moved before submit: {current_tip}",
                worker=worker,
            )
            return False
        try:
            reorg_reconciled = self.ensure_reorg_reconciled_for_tip(current_tip)
        except Exception:
            traceback.print_exc()
            self._abandon_block_candidate(
                PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                "reorg reconciliation failed before block submit",
                worker=worker,
            )
            return False
        if not reorg_reconciled and not already_active:
            self._abandon_block_candidate(
                PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                "reorg reconciliation reported an untrusted chain view",
                worker=worker,
            )
            return False
        if not already_active and not self.prior_balances_match_current(context.prior_balances):
            self._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                "prior balances changed since the job was issued",
                worker=worker,
            )
            return False
        if not already_active:
            before_height = int(self.rpc.call("getblockcount"))
            if before_height + 1 != expected_height:
                self._abandon_block_candidate(
                    PRISM_REJECTION_BLOCK_STALE,
                    f"stale block height: template={expected_height} tip={before_height}",
                    worker=worker,
                )
                return False
            # The accepted share and complete candidate are already durable.
            # Submit before rebuilding/verifying the final audit bundle so disk,
            # subprocess, and large-ledger latency cannot lose the tip race.
            self._record_heartbeat("block_submitter")
            result = self.rpc.call("submitblock", [submission.block_hex])
            self._record_heartbeat("block_submitter")
            if result not in (None, "duplicate"):
                self._abandon_block_candidate(
                    PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                    f"submitblock rejected candidate: {result}",
                    worker=worker,
                )
                return False
            active_hash = str(self.rpc.call("getblockhash", [expected_height])).lower()
            if active_hash != block_hash:
                self._abandon_block_candidate(
                    PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                    f"submitted block is not active at height {expected_height}",
                    worker=worker,
                )
                return False
        # Capture the current source as soon as the candidate is known active.
        # The direct-block source itself is reserved only after durable ledger
        # confirmation: a failed audit/RPC preparation therefore cannot leave
        # an orphaned publishable source. A newer source arriving during this
        # expensive phase makes the conditional reservation below fail and
        # explicitly supersedes this prepared result.
        payout_source_tip = current_tip if already_active else block_hash
        payout_preparation_started = time.monotonic()
        direct_source_preparation_token = self._capture_payout_state_source()[1]
        active_tip_height = int(self.rpc.call("getblockcount"))
        self._record_heartbeat("block_submitter")
        candidate_bundle_path = self.temporary_audit_bundle_path(
            block_hash=submission.block_hash_hex
        )
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
            ctv_fee_parent_hash=str(context.template["previousblockhash"]),
            canonical_output_path=candidate_bundle_path,
        )
        # Tests and alternate builders may not implement the optional canonical
        # output yet. Their Python-serialized fallback is valid verifier input,
        # but it must not be labeled canonical during persistence: the ledger
        # will canonicalize final_bundle itself on this compatibility path.
        if not candidate_bundle_path.exists():
            candidate_bundle_path = self.write_temporary_audit_bundle(
                final_bundle,
                block_hash=submission.block_hash_hex,
            )
        try:
            final_manifest = final_bundle["signed_coinbase_manifest"]["manifest"]
            final_coinbase_tx_hex_raw = final_manifest["coinbase_tx_hex"]
            if not isinstance(final_coinbase_tx_hex_raw, str):
                raise ValueError("final audit bundle coinbase_tx_hex is not a string")
            final_coinbase_tx_hex = final_coinbase_tx_hex_raw.lower()
        except BaseException:
            try:
                candidate_bundle_path.unlink()
            except FileNotFoundError:
                pass
            raise
        if final_coinbase_tx_hex != submission.coinbase_tx_hex.lower():
            try:
                candidate_bundle_path.unlink()
            except FileNotFoundError:
                pass
            self.request_shutdown()
            self._abandon_block_candidate(
                PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
                "final audit bundle coinbase does not match submitted coinbase",
                worker=worker,
            )
            return False
        try:
            report = self.verify_bundle(
                candidate_bundle_path,
                submission.coinbase_tx_hex,
                self.trusted_ledger_writer_public_key_hex(final_bundle),
                expected_coinbase_value_sats=int(context.template["coinbasevalue"]),
            )
            # Existence alone does not prove that an alternate/older builder
            # emitted canonical bytes. Only forward the verifier candidate to
            # persistence when its exact bytes match the verifier's canonical
            # digest; otherwise the ledger canonicalizes final_bundle itself.
            persistence_canonical_bundle_path = self.verified_canonical_bundle_path(
                candidate_bundle_path,
                report,
            )
            self._record_heartbeat("block_submitter")
            # Finalization is exact-idempotent so an active-tip/active-ancestor
            # replay can safely repeat any step after a crash.
            self._record_heartbeat("block_submitter")
            self._ensure_job_cache_state()
            payout_candidate: PayoutStateCandidate | None = None
            direct_payout_source: (
                tuple[int, int, str | None, str, float] | None
            ) = None
            with self._payout_state_prepare_lock:
                try:
                    persistence = self.ledger.persist_accepted_block(
                        block_hash=submission.block_hash_hex,
                        block_height=expected_height,
                        parent_hash=str(context.template["previousblockhash"]),
                        final_bundle=final_bundle,
                        audit_report=report,
                        canonical_bundle_path=persistence_canonical_bundle_path,
                    )
                    self._record_heartbeat("block_submitter")
                    confirmation = self.ledger.confirm_accepted_block(
                        block_hash=block_hash,
                        active_tip_height=active_tip_height,
                    )
                    confirmed_count = int(confirmation.get("confirmed_count", 0))
                except Exception:
                    # A database error may be reported after a durable partial
                    # commit. Reserve the direct source if it is still current,
                    # then fence delivery until reconciliation proves and
                    # publishes the resulting ledger state. Audit/RPC failures
                    # above this transaction never create such a source.
                    self._block_payout_state_publication(
                        supersede_with=(
                            direct_source_preparation_token,
                            payout_source_tip,
                            "direct_block_uncertain",
                            payout_preparation_started,
                        )
                    )
                    raise
                finally:
                    self._observe_payout_state_seconds(
                        "preparation",
                        max(
                            0.0,
                            time.monotonic() - payout_preparation_started,
                        ),
                    )
                if confirmed_count in {0, 1}:
                    direct_payout_source = (
                        self._reserve_payout_state_source_if_current(
                            direct_source_preparation_token,
                            "direct_block",
                            tip_hash=payout_source_tip,
                            invalidated_monotonic=payout_preparation_started,
                        )
                    )
                    # Confirmation activates carry-forward rows. A zero count
                    # is an idempotent replay, but the direct-tip source still
                    # has to cross publication unless a newer source superseded
                    # it. Persistence and confirmation stayed outside the
                    # delivery barrier in either case.
                    if direct_payout_source is None:
                        self._record_discarded_payout_candidate()
                    else:
                        payout_candidate = self._prepared_payout_state_candidate(
                            direct_payout_source
                        )
                    # Confirmation may have changed ledger-backed payout state
                    # even when a newer tip superseded the direct source. Close
                    # cache construction and delivery admission before releasing
                    # the snapshot lock; publication and its old-send drain run
                    # below, outside this preparation section.
                    self._block_payout_state_publication(force=True)
                else:
                    # An unexpected confirmation result is just as uncertain
                    # as an exception after persistence: keep all delivery
                    # fenced until reconciliation establishes the ledger state.
                    self._block_payout_state_publication(
                        supersede_with=(
                            direct_source_preparation_token,
                            payout_source_tip,
                            "direct_block_uncertain",
                            payout_preparation_started,
                        )
                    )
            if confirmed_count not in {0, 1}:
                self.request_shutdown()
                self._abandon_block_candidate(
                    PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                    f"ledger did not confirm accepted block {block_hash}",
                    worker=worker,
                )
                return False
            published = (
                self._publish_payout_state_candidate(payout_candidate)
                if payout_candidate is not None
                else None
            )
            if published is None and getattr(
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
            if published is None and not getattr(
                self,
                "reorg_reconciler_enabled",
                True,
            ):
                # Collection-only tests disable reorg reconciliation; publish
                # their current in-memory source without the production
                # reconciler. In production, a bounded supersession result
                # leaves the source unpublished so job builds remain fenced
                # until the scheduled retry.
                published = self._publish_current_payout_state_with_retry_budget(
                    initial_attempted=direct_payout_source is not None,
                )
            ctv_persistence = None
            ctv_manifest_set = final_bundle.get("ctv_fanout_manifest_set")
            if isinstance(ctv_manifest_set, dict):
                ctv_persistence = self.ledger.persist_ctv_fanout_manifest_set(
                    block_hash=block_hash,
                    manifest_set=ctv_manifest_set,
                    manifest_set_sha256=sha256_json_hex(ctv_manifest_set),
                )
            final_bundle_path = self.audit_dir / f"prism-live-audit-bundle-{expected_height}-{block_hash}.json"
            self.write_audit_bundle_envelope(
                final_bundle_path,
                block_hash=block_hash,
                block_height=expected_height,
                report=report,
                persistence=persistence,
            )
            self.prune_audit_artifacts(keep_live_path=final_bundle_path)
            bundle_path = final_bundle_path
            if candidate.credit_share_on_accept:
                self.append_accepted_share(
                    candidate.client,
                    context,
                    submission,
                    candidate.pending_share,
                    candidate_intent=self.block_candidate_intent(candidate),
                )
            # Aggregate counts only: materializing the whole share history
            # (all_shares) here would scan the full ledger twice per block,
            # and would grow without bound as the ledger grows.
            evidence_share_count, evidence_distinct_miners = self.accepted_share_stats()
            evidence = {
                "schema": "qbit.prism.live-stratum-evidence.v1",
                "block_hash": block_hash,
                "block_height": expected_height,
                "coinbase_tx_hex": submission.coinbase_tx_hex,
                "audit_bundle_path": str(bundle_path),
                "audit_report": report,
                "ledger_backend": self.ledger.backend_name,
                "persistence": persistence,
                "confirmation": confirmation,
                "ctv_persistence": ctv_persistence,
                "accepted_share_count": evidence_share_count,
                "distinct_miner_count": evidence_distinct_miners,
                "job_share_count": len(context.shares_json),
            }
            self.evidence_path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
            with self.lock:
                self.accepted_block_count += 1
                self.latest_coinbase_size_bytes = len(
                    str(final_manifest["coinbase_tx_hex"])
                ) // 2
                self.latest_evidence = evidence
                should_stop = self.stop_after_block or self.accepted_block_count >= self.max_blocks
            print(
                "prism coordinator: qbit accepted direct PRISM block "
                f"height={expected_height} hash={block_hash}",
                flush=True,
            )
            if should_stop:
                self.request_shutdown()
            else:
                # The public submitter wrapper performs this fanout only after
                # its writer scope (including outbox finalization) exits. The
                # synchronous rare-share path consumes the same marker from
                # handle_request after the submit result is sent.
                candidate.client.post_accept_refresh_block = (
                    expected_height,
                    block_hash,
                )
            return True
        finally:
            try:
                candidate_bundle_path.unlink()
            except FileNotFoundError:
                pass

    @ledger_writer_operation("accepted_block_handling")
    def reject_prepared_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        reject = getattr(self.ledger, "reject_prepared_block", None)
        if callable(reject):
            return reject(block_hash=block_hash, active_tip_height=active_tip_height)
        return self.ledger.reverse_immature_block(
            block_hash=block_hash,
            active_tip_height=active_tip_height,
        )

    def temporary_audit_bundle_path(self, *, block_hash: str) -> Path:
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        return self.audit_dir / (
            f".prism-live-audit-bundle-candidate-{block_hash}-{uuid.uuid4().hex}.json.tmp"
        )

    @staticmethod
    def verified_canonical_bundle_path(
        candidate_bundle_path: Path,
        report: dict[str, Any],
    ) -> Path | None:
        expected_sha256 = str(report["audit_bundle_sha256_hex"]).lower()
        digest = hashlib.sha256()
        with candidate_bundle_path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        if digest.hexdigest() != expected_sha256:
            return None
        return candidate_bundle_path

    def write_temporary_audit_bundle(self, bundle: dict[str, Any], *, block_hash: str) -> Path:
        path = self.temporary_audit_bundle_path(block_hash=block_hash)
        with path.open("x", encoding="utf-8") as handle:
            json.dump(bundle, handle, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        return path

    def write_audit_bundle_envelope(
        self,
        path: Path,
        *,
        block_hash: str,
        block_height: int,
        report: dict[str, Any],
        persistence: dict[str, Any],
    ) -> None:
        audit_bundle_sha256 = str(
            persistence.get("audit_bundle_sha256")
            or report.get("audit_bundle_sha256_hex")
            or ""
        ).lower()
        body_uri = str(persistence.get("body_uri") or "")
        envelope = {
            "schema": "qbit.prism.live-audit-bundle-envelope.v1",
            "block_hash": block_hash,
            "block_height": block_height,
            "audit_bundle_sha256": audit_bundle_sha256,
            "body_uri": body_uri,
            "body_filename": Path(body_uri).name if body_uri else None,
            "coinbase_txid": report.get("coinbase_txid"),
            "coinbase_manifest_sha256": report.get("coinbase_manifest_sha256_hex"),
            "coinbase_tx_hex": report.get("coinbase_tx_hex"),
            "created_at": public_api.utc_now_iso(),
        }
        self.write_json_atomically(path, envelope)

    def write_json_atomically(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        tmp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with tmp_path.open("xb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            tmp_path.replace(path)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass

    def prune_audit_artifacts(self, *, keep_live_path: Path | None = None) -> None:
        self.prune_live_audit_envelopes(keep_path=keep_live_path)
        self.prune_candidate_audit_bundles()

    def prune_live_audit_envelopes(self, *, keep_path: Path | None = None) -> None:
        retention = int(getattr(self, "audit_live_bundle_retention", 5))
        if retention < 0:
            return
        keep_resolved = keep_path.resolve() if keep_path is not None else None
        retained_non_keep = max(retention - 1, 0) if keep_resolved is not None else retention
        paths = sorted(
            self.audit_dir.glob("prism-live-audit-bundle-[0-9]*.json"),
            key=lambda path: path.stat().st_mtime if path.exists() else 0,
            reverse=True,
        )
        retained_count = 0
        for path in paths:
            if keep_resolved is not None and path.resolve() == keep_resolved:
                continue
            if retained_count < retained_non_keep:
                retained_count += 1
                continue
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    def prune_candidate_audit_bundles(self) -> None:
        retention_seconds = int(getattr(self, "audit_candidate_retention_seconds", 24 * 60 * 60))
        now = time.time()
        for pattern in (
            "prism-live-audit-bundle-candidate-*.json",
            ".prism-live-audit-bundle-candidate-*.json.tmp",
        ):
            for path in self.audit_dir.glob(pattern):
                try:
                    if retention_seconds == 0 or now - path.stat().st_mtime > retention_seconds:
                        path.unlink()
                except FileNotFoundError:
                    pass

    def verify_bundle(
        self,
        bundle_path: Path,
        coinbase_tx_hex: str,
        ledger_writer_public_key_hex: str,
        *,
        expected_coinbase_value_sats: int,
    ) -> dict[str, Any]:
        completed = subprocess.run(
            prism_tool_command("qbit-prism-audit-verify")
            + [
                str(bundle_path),
                "--coinbase-tx-hex",
                coinbase_tx_hex,
                "--ledger-writer-public-key-hex",
                ledger_writer_public_key_hex,
                "--expected-coinbase-value-sats",
                str(expected_coinbase_value_sats),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"qbit-prism-audit-verify failed: {completed.stderr}")
        return json.loads(completed.stdout)

    def trusted_ledger_writer_public_key_hex(self, bundle: dict[str, Any]) -> str:
        if self.ledger_writer_public_key_hex is not None:
            return self.ledger_writer_public_key_hex
        return validate_hex(
            str(bundle["ledger_window_attestation"]["signature"]["public_key_hex"]),
            name="bundle ledger public key",
            expected_bytes=32,
        )

    def ready_miner_count(self) -> int:
        return self.accepted_share_stats()[1]

    def mining_delivery_snapshot(self, *, now: float | None = None) -> dict[str, object]:
        now = time.monotonic() if now is None else now
        with self.lock:
            self._ensure_initial_job_state()
            active = len(self.clients)
            current_tip = self._current_observed_tip_hash_locked()
            published_snapshot = getattr(self, "tip_template_snapshot", None)
            subscribed = sum(1 for client in self.clients if client.subscribed)
            authorized_clients = [
                client
                for client in self.clients
                if client.subscribed and client.authorized and client.worker is not None
            ]
            authorized = len(authorized_clients)
            current = sum(
                1
                for client in authorized_clients
                if self._client_has_current_tip_job_locked(client)
            )
            oldest_missing_age = max(
                (
                    now - client.authorized_monotonic
                    for client in authorized_clients
                    if not self._client_has_current_tip_job_locked(client)
                    and client.authorized_monotonic is not None
                ),
                default=0.0,
            )
            pending_requests = list(self.pending_initial_jobs.values())
            pending = len(pending_requests)
            oldest_age = max(
                (now - request.requested_monotonic for request in pending_requests),
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
            last_delivery = self.last_initial_job_delivery_monotonic
            timeout = float(self.stratum_initial_job_timeout_seconds)
            timeout_disconnects = self.initial_job_timeout_count
            queue_rejections = self.initial_job_queue_rejection_count
            cancelled = self.initial_job_cancelled_count
            coalesced = self.initial_job_coalesced_count
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
        no_delivery_progress = bool(
            deadline is not None
            and poor_coverage
            and (
                delivery_failure_age >= deadline
                or oldest_missing_age >= deadline
                or (
                    pending > 0
                    and (
                        (last_delivery is None and oldest_age >= deadline)
                        or (
                            last_delivery is not None
                            and now - last_delivery >= deadline
                        )
                    )
                )
            )
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
            if poor_coverage and no_delivery_progress:
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
            "clients_with_current_tip_jobs": current,
            "current_tip_job_coverage": round(coverage, 6),
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
            "oldest_initial_job_pending_seconds": round(
                max(oldest_age, oldest_missing_age), 3
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
        return {
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

    def refresh_health_snapshot(self) -> dict[str, object]:
        payload = self.health_payload()
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            self._health_snapshot = payload
            self._health_snapshot_monotonic = time.monotonic()
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
            return 503, {
                "ok": False,
                "schema": "qbit.prism.audit-health.v1",
                "error": "health snapshot is not available yet",
            }
        age_seconds = time.monotonic() - snapshot_monotonic
        stale_after = max(3 * refresh_seconds, 15.0)
        if age_seconds > stale_after:
            return 503, {
                "ok": False,
                "schema": "qbit.prism.audit-health.v1",
                "error": "health snapshot is stale",
                "snapshot_age_seconds": round(age_seconds, 3),
            }
        payload = dict(snapshot)
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
        try:
            self.refresh_health_snapshot()
        except Exception:
            print("prism coordinator: initial health snapshot refresh failed", flush=True)
            traceback.print_exc()
        threading.Thread(target=self.health_snapshot_loop, daemon=True).start()

    def latest_evidence_payload(self) -> dict[str, object] | None:
        with self.lock:
            if self.latest_evidence is not None:
                return dict(self.latest_evidence)
        if self.evidence_path.exists():
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        return None

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

    def metrics_payload(self) -> str:
        ledger_metrics = self.ledger.metrics()
        audit_metrics = self.audit_artifact_metrics()
        mining_metrics = self.mining_delivery_snapshot()
        process_rss_bytes, process_open_fds = self.process_resource_metrics()
        accepted_share_count = self.accepted_share_stats()[0]
        elapsed = max(0.001, time.monotonic() - self.started_monotonic)
        shares_per_second = accepted_share_count / elapsed
        stale_percent = 0.0
        if self.submitted_share_count > 0:
            stale_percent = (self.stale_share_count / self.submitted_share_count) * 100.0
        rejection_counts = getattr(self, "rejection_counts_by_reason", {})
        grace_credited_share_count = int(getattr(self, "grace_credited_share_count", 0))
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
            "# HELP qbit_prism_submitted_shares_total Stratum share submissions seen by the PRISM coordinator.",
            "# TYPE qbit_prism_submitted_shares_total counter",
            f"qbit_prism_submitted_shares_total {self.submitted_share_count}",
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
            "# HELP qbit_prism_stratum_clients_with_current_tip_jobs Authorized clients holding usable current-tip work.",
            "# TYPE qbit_prism_stratum_clients_with_current_tip_jobs gauge",
            f"qbit_prism_stratum_clients_with_current_tip_jobs {mining_metrics['clients_with_current_tip_jobs']}",
            "# HELP qbit_prism_stratum_current_tip_job_coverage Ratio of authorized clients holding current-tip work.",
            "# TYPE qbit_prism_stratum_current_tip_job_coverage gauge",
            f"qbit_prism_stratum_current_tip_job_coverage {mining_metrics['current_tip_job_coverage']}",
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
            f"qbit_prism_stale_shares_total {self.stale_share_count}",
            "# HELP qbit_prism_duplicate_shares_total Duplicate Stratum shares rejected.",
            "# TYPE qbit_prism_duplicate_shares_total counter",
            f"qbit_prism_duplicate_shares_total {self.duplicate_share_count}",
            "# HELP qbit_prism_low_difficulty_shares_total Low-difficulty Stratum shares rejected.",
            "# TYPE qbit_prism_low_difficulty_shares_total counter",
            f"qbit_prism_low_difficulty_shares_total {self.low_difficulty_share_count}",
            "# HELP qbit_prism_collection_block_submissions_total Solver-pays-all block candidates submitted from collection-mode jobs.",
            "# TYPE qbit_prism_collection_block_submissions_total counter",
            f"qbit_prism_collection_block_submissions_total {getattr(self, 'collection_block_submission_count', 0)}",
            "# HELP qbit_prism_grace_credited_shares_total Accepted shares credited by the stale-grace policy.",
            "# TYPE qbit_prism_grace_credited_shares_total counter",
            f"qbit_prism_grace_credited_shares_total {grace_credited_share_count}",
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
            "# HELP qbit_prism_block_candidate_poisoned_total Invalid durable candidate intents quarantined from replay.",
            "# TYPE qbit_prism_block_candidate_poisoned_total counter",
            f"qbit_prism_block_candidate_poisoned_total {int(getattr(self, 'block_candidate_poisoned_count', 0))}",
            "# HELP qbit_prism_block_candidates_abandoned_total Block candidates that did not land (lost tip race or failed submit), by reason. Not share rejections: the underlying share was accepted.",
            "# TYPE qbit_prism_block_candidates_abandoned_total counter",
            *[
                f'qbit_prism_block_candidates_abandoned_total{{reason_id="{reason}"}} {int(count)}'
                for reason, count in sorted(getattr(self, "block_candidate_abandoned_counts", {}).items())
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
        lines.extend(self.ctv_fanout_broadcaster_metrics_lines())
        lines.extend(self.job_build_metrics_lines())
        lines.extend(self.tip_refresh_metrics_lines())
        lines.extend(self.payout_state_metrics_lines())
        lines.extend(self.initial_delivery_metrics_lines())
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
        metrics: dict[str, dict[str, int] | int] = {
            kind: {"files": 0, "bytes": 0}
            for kind in ("body", "share_segment", "live_bundle", "candidate", "other")
        }
        metrics["scan_error"] = 0
        audit_dir = getattr(self, "audit_dir", None)
        if audit_dir is None:
            metrics["scan_error"] = 1
            return metrics
        try:
            paths = list(Path(audit_dir).iterdir())
        except OSError:
            metrics["scan_error"] = 1
            return metrics
        for path in paths:
            try:
                if not path.is_file():
                    continue
                size = path.stat().st_size
            except OSError:
                metrics["scan_error"] = 1
                continue
            kind = self.audit_artifact_kind(path.name)
            bucket = metrics[kind]
            assert isinstance(bucket, dict)
            bucket["files"] += 1
            bucket["bytes"] += size
        return metrics

    @staticmethod
    def audit_artifact_kind(name: str) -> str:
        if name.startswith("prism-audit-bundle-body-") and name.endswith(".json"):
            return "body"
        if name.startswith("prism-audit-share-segment-") and name.endswith(".json"):
            return "share_segment"
        if name.startswith("prism-live-audit-bundle-candidate-") or name.startswith(
            ".prism-live-audit-bundle-candidate-"
        ):
            return "candidate"
        if name.startswith("prism-live-audit-bundle-") and name.endswith(".json"):
            return "live_bundle"
        return "other"

    def ctv_fanout_broadcaster_metrics_lines(self) -> list[str]:
        self._ensure_ctv_broadcaster_metrics_state()
        with self._ctv_broadcaster_metrics_lock:
            bucket_counts = dict(self.ctv_broadcaster_pass_seconds_bucket_counts)
            pass_sum = self.ctv_broadcaster_pass_seconds_sum
            pass_count = self.ctv_broadcaster_pass_count
            processed_rows_total = self.ctv_broadcaster_processed_rows_total
            yielded_total = self.ctv_broadcaster_yielded_total
            chunk_seconds_buckets = dict(
                self.ctv_broadcaster_chunk_seconds_bucket_counts
            )
            chunk_rows_buckets = dict(self.ctv_broadcaster_chunk_rows_bucket_counts)
            chunk_seconds_sum = self.ctv_broadcaster_chunk_seconds_sum
            chunk_rows_sum = self.ctv_broadcaster_chunk_rows_sum
            chunk_count = self.ctv_broadcaster_chunk_count
        metric_name = "qbit_prism_ctv_fanout_broadcaster_pass_seconds"
        chunk_seconds_name = "qbit_prism_ctv_fanout_broadcaster_chunk_seconds"
        chunk_rows_name = "qbit_prism_ctv_fanout_broadcaster_chunk_rows"
        return [
            "# HELP qbit_prism_ctv_fanout_broadcaster_processed_rows_total CTV fanout rows completed by the broadcaster loop.",
            "# TYPE qbit_prism_ctv_fanout_broadcaster_processed_rows_total counter",
            f"qbit_prism_ctv_fanout_broadcaster_processed_rows_total {processed_rows_total}",
            "# HELP qbit_prism_ctv_fanout_broadcaster_pass_seconds CTV fanout broadcaster pass wall time.",
            "# TYPE qbit_prism_ctv_fanout_broadcaster_pass_seconds histogram",
            *[
                f'{metric_name}_bucket{{le="{bucket:g}"}} {bucket_counts.get(bucket, 0)}'
                for bucket in PRISM_CTV_BROADCASTER_SECONDS_BUCKETS
            ],
            f'{metric_name}_bucket{{le="+Inf"}} {pass_count}',
            f"{metric_name}_sum {pass_sum:.6f}",
            f"{metric_name}_count {pass_count}",
            "# HELP qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total CTV broadcaster passes yielding between committed chunks for a pending tip refresh.",
            "# TYPE qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total counter",
            f"qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total {yielded_total}",
            "# HELP qbit_prism_ctv_fanout_broadcaster_chunk_seconds CTV broadcaster committed chunk wall time.",
            "# TYPE qbit_prism_ctv_fanout_broadcaster_chunk_seconds histogram",
            *[
                f'{chunk_seconds_name}_bucket{{le="{bucket:g}"}} {chunk_seconds_buckets.get(bucket, 0)}'
                for bucket in PRISM_CTV_BROADCASTER_CHUNK_SECONDS_BUCKETS
            ],
            f'{chunk_seconds_name}_bucket{{le="+Inf"}} {chunk_count}',
            f"{chunk_seconds_name}_sum {chunk_seconds_sum:.6f}",
            f"{chunk_seconds_name}_count {chunk_count}",
            "# HELP qbit_prism_ctv_fanout_broadcaster_chunk_rows Rows processed per committed CTV broadcaster chunk.",
            "# TYPE qbit_prism_ctv_fanout_broadcaster_chunk_rows histogram",
            *[
                f'{chunk_rows_name}_bucket{{le="{bucket}"}} {chunk_rows_buckets.get(bucket, 0)}'
                for bucket in PRISM_CTV_BROADCASTER_CHUNK_ROWS_BUCKETS
            ],
            f'{chunk_rows_name}_bucket{{le="+Inf"}} {chunk_count}',
            f"{chunk_rows_name}_sum {chunk_rows_sum}",
            f"{chunk_rows_name}_count {chunk_count}",
        ]

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
        executor = getattr(self, "_tip_refresh_executor", None)
        _queued, slots = executor.stats() if executor is not None else (0, 0)
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

    def tip_refresh_metrics_lines(self) -> list[str]:
        self._ensure_tip_refresh_state()
        with self._tip_refresh_executor_lock:
            executor_workers = (
                self.tip_refresh_max_workers
                if self._tip_refresh_executor is not None
                else 0
            )
        with self._tip_refresh_metrics_lock:
            histograms = {
                name: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for name, histogram in self.tip_refresh_histograms.items()
            }
            phase_histograms = {
                phase: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for phase, histogram in self.tip_refresh_build_phase_histograms.items()
            }
            client_counts = dict(self.tip_refresh_client_counts)
            cancellation_counts = dict(self.tip_refresh_cancellation_counts)
            inflight = self.tip_refresh_inflight
            build_inflight = self.tip_refresh_build_inflight
            build_queue_depth = self.tip_refresh_build_queue_depth
            singleflight_hits = self.tip_refresh_singleflight_hits
            superseded_results = self.tip_refresh_superseded_results
            worker_failures = self.tip_refresh_worker_failures
            worker_restarts = self.tip_refresh_worker_restarts
            ipc_bytes = dict(self.tip_refresh_ipc_bytes)

        metric_names = {
            "refresh": "qbit_prism_tip_refresh_seconds",
            "bundle_build": "qbit_prism_tip_refresh_bundle_build_seconds",
            "first_delivery": "qbit_prism_tip_refresh_first_delivery_seconds",
            "last_delivery": "qbit_prism_tip_refresh_last_delivery_seconds",
        }
        descriptions = {
            "refresh": "Full qbit tip/template refresh pass wall time.",
            "bundle_build": "Shared ready-pool refresh bundle preparation wall time.",
            "first_delivery": "Tip observation to first successful client delivery.",
            "last_delivery": "Tip observation to last successful client delivery.",
        }
        lines: list[str] = []
        for name, metric_name in metric_names.items():
            histogram = histograms[name]
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    f"# HELP {metric_name} {descriptions[name]}",
                    f"# TYPE {metric_name} histogram",
                    *[
                        f'{metric_name}_bucket{{le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
                        for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                    ],
                    f'{metric_name}_bucket{{le="+Inf"}} {histogram["count"]}',
                    f'{metric_name}_sum {float(histogram["sum"]):.6f}',
                    f'{metric_name}_count {histogram["count"]}',
                ]
            )
        phase_metric_name = "qbit_prism_tip_refresh_bundle_phase_seconds"
        lines.extend(
            [
                "# HELP qbit_prism_tip_refresh_bundle_phase_seconds Shared bundle-build phase wall time.",
                "# TYPE qbit_prism_tip_refresh_bundle_phase_seconds histogram",
            ]
        )
        for phase in PRISM_TIP_REFRESH_BUILD_PHASES:
            histogram = phase_histograms[phase]
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    *[
                        f'{phase_metric_name}_bucket{{phase="{phase}",le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
                        for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                    ],
                    f'{phase_metric_name}_bucket{{phase="{phase}",le="+Inf"}} {histogram["count"]}',
                    f'{phase_metric_name}_sum{{phase="{phase}"}} {float(histogram["sum"]):.6f}',
                    f'{phase_metric_name}_count{{phase="{phase}"}} {histogram["count"]}',
                ]
            )
        lines.extend(
            [
                "# HELP qbit_prism_tip_refresh_clients_total Client outcomes from tip/template refresh passes.",
                "# TYPE qbit_prism_tip_refresh_clients_total counter",
                *[
                    f'qbit_prism_tip_refresh_clients_total{{result="{result}"}} {int(client_counts.get(result, 0))}'
                    for result in PRISM_TIP_REFRESH_RESULTS
                ],
                "# HELP qbit_prism_tip_refresh_cancellations_total Obsolete prepared refresh tasks canceled before delivery admission.",
                "# TYPE qbit_prism_tip_refresh_cancellations_total counter",
                *[
                    f'qbit_prism_tip_refresh_cancellations_total{{stage="{stage}"}} {int(cancellation_counts.get(stage, 0))}'
                    for stage in PRISM_TIP_REFRESH_CANCELLATION_STAGES
                ],
                "# HELP qbit_prism_tip_refresh_inflight Prepared refresh client tasks currently queued or running.",
                "# TYPE qbit_prism_tip_refresh_inflight gauge",
                f"qbit_prism_tip_refresh_inflight {inflight}",
                "# HELP qbit_prism_tip_refresh_executor_workers Configured persistent refresh executor workers, or zero before creation.",
                "# TYPE qbit_prism_tip_refresh_executor_workers gauge",
                f"qbit_prism_tip_refresh_executor_workers {executor_workers}",
                "# HELP qbit_prism_tip_refresh_bundle_inflight Shared bundle builds currently running.",
                "# TYPE qbit_prism_tip_refresh_bundle_inflight gauge",
                f"qbit_prism_tip_refresh_bundle_inflight {build_inflight}",
                "# HELP qbit_prism_tip_refresh_bundle_queue_depth Shared bundle callers waiting on bounded build admission or an identical single-flight.",
                "# TYPE qbit_prism_tip_refresh_bundle_queue_depth gauge",
                f"qbit_prism_tip_refresh_bundle_queue_depth {build_queue_depth}",
                "# HELP qbit_prism_tip_refresh_bundle_singleflight_hits_total Shared bundle callers coalesced behind an identical build.",
                "# TYPE qbit_prism_tip_refresh_bundle_singleflight_hits_total counter",
                f"qbit_prism_tip_refresh_bundle_singleflight_hits_total {singleflight_hits}",
                "# HELP qbit_prism_tip_refresh_bundle_superseded_results_total Completed or canceled shared bundles discarded after supersession.",
                "# TYPE qbit_prism_tip_refresh_bundle_superseded_results_total counter",
                f"qbit_prism_tip_refresh_bundle_superseded_results_total {superseded_results}",
                "# HELP qbit_prism_tip_refresh_builder_worker_failures_total Audit-builder subprocess failures.",
                "# TYPE qbit_prism_tip_refresh_builder_worker_failures_total counter",
                f"qbit_prism_tip_refresh_builder_worker_failures_total {worker_failures}",
                "# HELP qbit_prism_tip_refresh_builder_worker_restarts_total Long-lived builder worker restarts; zero for the inline subprocess design.",
                "# TYPE qbit_prism_tip_refresh_builder_worker_restarts_total counter",
                f"qbit_prism_tip_refresh_builder_worker_restarts_total {worker_restarts}",
                "# HELP qbit_prism_tip_refresh_builder_ipc_bytes_total Bytes copied across audit-builder subprocess IPC.",
                "# TYPE qbit_prism_tip_refresh_builder_ipc_bytes_total counter",
                *[
                    f'qbit_prism_tip_refresh_builder_ipc_bytes_total{{direction="{direction}"}} {int(ipc_bytes.get(direction, 0))}'
                    for direction in ("input", "output")
                ],
            ]
        )
        return lines

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
            ]
        )
        return lines

    def payout_state_metrics_lines(self) -> list[str]:
        self._ensure_job_cache_state()
        with self._payout_state_metrics_lock:
            state_histograms = {
                name: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for name, histogram in self.payout_state_histograms.items()
            }
            gate_histograms = {
                relation: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for relation, histogram in self.payout_gate_wait_histograms.items()
            }
            discarded = self.payout_state_candidates_discarded

        metric_names = {
            "preparation": "qbit_prism_payout_preparation_seconds",
            "publish": "qbit_prism_payout_publish_seconds",
            "first_delivery": "qbit_prism_payout_invalidation_first_delivery_seconds",
        }
        descriptions = {
            "preparation": "Payout reconciliation and candidate preparation outside delivery publication.",
            "publish": "Atomic payout generation/cache publication gate-hold time.",
            "first_delivery": "Payout invalidation to first delivery of the published generation.",
        }
        lines: list[str] = []
        for name, metric_name in metric_names.items():
            histogram = state_histograms[name]
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    f"# HELP {metric_name} {descriptions[name]}",
                    f"# TYPE {metric_name} histogram",
                    *[
                        f'{metric_name}_bucket{{le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
                        for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                    ],
                    f'{metric_name}_bucket{{le="+Inf"}} {histogram["count"]}',
                    f'{metric_name}_sum {float(histogram["sum"]):.6f}',
                    f'{metric_name}_count {histogram["count"]}',
                ]
            )

        gate_name = "qbit_prism_payout_gate_wait_seconds"
        lines.extend(
            [
                "# HELP qbit_prism_payout_gate_wait_seconds Delivery admission wait by generation relationship to the published payout state.",
                "# TYPE qbit_prism_payout_gate_wait_seconds histogram",
            ]
        )
        for relation in PRISM_PAYOUT_DELIVERY_GENERATIONS:
            histogram = gate_histograms[relation]
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    *[
                        f'{gate_name}_bucket{{generation="{relation}",le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
                        for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                    ],
                    f'{gate_name}_bucket{{generation="{relation}",le="+Inf"}} {histogram["count"]}',
                    f'{gate_name}_sum{{generation="{relation}"}} {float(histogram["sum"]):.6f}',
                    f'{gate_name}_count{{generation="{relation}"}} {histogram["count"]}',
                ]
            )
        lines.extend(
            [
                "# HELP qbit_prism_payout_candidates_discarded_total Prepared payout candidates discarded after source supersession.",
                "# TYPE qbit_prism_payout_candidates_discarded_total counter",
                f"qbit_prism_payout_candidates_discarded_total {discarded}",
            ]
        )
        return lines


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
