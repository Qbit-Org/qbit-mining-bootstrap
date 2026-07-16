#!/usr/bin/env python3
"""Minimal live direct qbit Stratum coordinator for PRISM regtest proof."""

from __future__ import annotations

import base64
from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import ExitStack, contextmanager
import dataclasses
import errno
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
from lab.prism.ctv_broadcaster_daemon import CtvFanoutBroadcastDaemon, CtvFanoutDaemonResult
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
DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS = 5.0
DEFAULT_PRISM_HEALTH_REFRESH_SECONDS = 5.0
DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS = 20.0
DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS = 0
DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME = 0
DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS = 1.0
DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES = 4_096
DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS = 3_600.0
DEFAULT_PRISM_STALE_GRACE_SECONDS = 3.0
DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS = 30.0
DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION = 64
DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS = 1.0
DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS = 16
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
PRISM_TIP_REFRESH_SECONDS_BUCKETS = PRISM_JOB_BUILD_SECONDS_BUCKETS
PRISM_JOB_BUILD_PHASES = ("reorg", "template", "merkle", "ledger", "bundle", "stamp", "send")
PRISM_JOB_CACHE_KINDS = ("template", "bundle")
PRISM_TIP_REFRESH_RESULTS = ("sent", "skipped", "disconnected", "failed")
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
    bundle: dict[str, Any]
    shares_json: list[dict[str, object]]
    prior_balances: list[dict[str, object]]
    found_block: dict[str, object]
    share_weight: int
    collection_only: bool
    worker: WorkerIdentity
    issued_at_ms: int
    template_fingerprint: str | None = None
    template_generation: int = 0


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
class QbitTipTemplateSnapshot:
    bestblockhash: str
    previousblockhash: str
    template_fingerprint: str
    template_generation: int = 0


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
class CachedJobBundle:
    """One heavy job build (ledger snapshot + signed audit bundle + base job)
    shared across every client on the same template.

    The base job is built with the extranonce1 placeholder; per-client jobs
    are stamped from it by swapping job_id, extranonce1, difficulty, and the
    clean_jobs flag. All other fields are byte-identical across clients
    because the stratum coinbase split excludes the extranonce window.
    """

    key: tuple[object, ...]
    template: dict[str, Any]
    template_fingerprint: str
    bundle: dict[str, Any]
    shares_json: list[dict[str, object]]
    prior_balances: list[dict[str, object]]
    found_block: dict[str, object]
    collection_only: bool
    issued_at_ms: int
    base_job: direct_stratum.DirectQbitStratumJob
    built_monotonic: float
    template_generation: int = 0


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


@dataclass(eq=False)
class ClientState:
    sock: socket.socket
    address: tuple[str, int]
    connection_id: int
    extranonce1_hex: str
    subscribed: bool = False
    authorized: bool = False
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
    # Serializes every job build/register/send transition for this connection.
    # The coordinator lock may be acquired while this lock is held, never in
    # the reverse order. RLock permits authorize/retarget helpers to call the
    # common maybe_send_job path while retaining the same serialization scope.
    job_update_lock: threading.RLock = field(default_factory=threading.RLock)
    send_lock: threading.Lock = field(default_factory=threading.Lock)

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


class TemplateRefreshBlocked(RuntimeError):
    """A live template was fetched, but safe work could not be issued."""


class _JobBuildFailed(RuntimeError):
    """Internal signal used to distinguish a skipped build from a no-op."""


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
        # Zero preserves the historical unlimited behavior. Operators can set
        # positive admission limits after sizing them for their miner/proxy
        # topology instead of inheriting an arbitrary coordinator default.
        self.stratum_max_connections = env_nonnegative_int(
            "PRISM_STRATUM_MAX_CONNECTIONS",
            DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
        )
        self.stratum_max_connections_per_username = env_nonnegative_int(
            "PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME",
            DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME,
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
        self.accept_resource_exhaustion_count = 0
        self.connection_setup_failure_count = 0
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
        self.latest_bundle: dict[str, Any] | None = None
        self.tip_template_snapshot: QbitTipTemplateSnapshot | None = None
        self._tip_refresh_lock = threading.Lock()
        self._tip_refresh_executor_lock = threading.Lock()
        self._tip_refresh_executor: ThreadPoolExecutor | None = None
        self._tip_refresh_executor_shutdown = False
        self.last_reorg_reconciled_tip_hash: str | None = None
        self.last_reorg_reconciled_trusted = False
        self.last_reorg_reconciled_monotonic: float | None = None
        self._prism_payout_policy_cache: dict[str, object] | None = None
        self._ensure_job_cache_state()
        self.stop_event = threading.Event()
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
            writer_id=env("PRISM_LEDGER_WRITER_ID", "prism-coordinator"),
            writer_epoch=env_int("PRISM_LEDGER_WRITER_EPOCH", 1),
            writer_session_token=writer_session_token,
            initialize_schema=env("PRISM_POSTGRES_INIT_SCHEMA", "0") in {"1", "true", "yes"},
            lease_ttl_seconds=env_positive_float("PRISM_LEDGER_LEASE_TTL_SECONDS", 60.0),
            read_concurrency=env_positive_int("PRISM_POSTGRES_READ_CONCURRENCY", 4),
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
        if not hasattr(self, "_job_build_lock"):
            self._job_build_lock = threading.Lock()
        if not hasattr(self, "_template_artifacts"):
            self._template_artifacts: CachedTemplateArtifacts | None = None
        if not hasattr(self, "_template_artifact_generation"):
            self._template_artifact_generation = int(
                getattr(self._template_artifacts, "generation", 0)
            )
        if not hasattr(self, "_job_bundle_cache"):
            self._job_bundle_cache: dict[tuple[object, ...], CachedJobBundle] = {}
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

    def _job_build_phases(self) -> dict[str, float]:
        """Per-thread scratch dict of phase timings for the current build."""
        self._ensure_job_cache_state()
        phases = getattr(self._job_build_phase_local, "phases", None)
        if phases is None:
            phases = {}
            self._job_build_phase_local.phases = phases
        return phases

    def _record_job_cache_event(self, kind: str, *, hit: bool) -> None:
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            counts = self.job_cache_hit_counts if hit else self.job_cache_miss_counts
            counts[kind] = int(counts.get(kind, 0)) + 1

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
                self._job_bundle_cache = {
                    key: entry
                    for key, entry in self._job_bundle_cache.items()
                    if entry.template_fingerprint == artifacts.fingerprint
                }
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

    def _lookup_job_bundle(self, fingerprint: str, worker: WorkerIdentity) -> CachedJobBundle | None:
        ttl = getattr(self, "job_bundle_cache_seconds", DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS)
        if ttl <= 0:
            return None
        now = time.monotonic()
        with self._job_cache_lock:
            ready = self._job_bundle_cache.get((fingerprint, "ready"))
            if ready is not None and now - ready.built_monotonic <= ttl:
                return ready
            collection = self._job_bundle_cache.get(
                (fingerprint, "collection", worker.payout_address, worker.p2mr_program_hex)
            )
            if collection is not None and now - collection.built_monotonic <= ttl:
                return collection
        return None

    def _job_bundle_entry_usable(self, cached: CachedJobBundle | None) -> bool:
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
        if not cached.collection_only:
            return True
        try:
            _, ready_miner_count = self.accepted_share_stats()
        except Exception:
            # If readiness cannot be proven, force the normal build path. That
            # path will either build an up-to-date bundle or surface the ledger
            # failure instead of continuing to issue no-submit collection jobs.
            return False
        return ready_miner_count < self.min_ready_miners

    def shared_job_bundle(self, artifacts: CachedTemplateArtifacts, worker: WorkerIdentity) -> CachedJobBundle:
        """Return the cached heavy build for this template, building at most once.

        Concurrent callers needing the same missing entry single-flight behind
        the build lock: the first one pays for the ledger snapshot and the
        signed audit bundle, the rest reuse it.
        """
        self._ensure_job_cache_state()
        cached = self._lookup_job_bundle(artifacts.fingerprint, worker)
        if self._job_bundle_entry_usable(cached):
            self._record_job_cache_event("bundle", hit=True)
            assert cached is not None
            return dataclass_replace(
                cached,
                template_generation=artifacts.generation,
            )
        with self._job_build_lock:
            cached = self._lookup_job_bundle(artifacts.fingerprint, worker)
            if self._job_bundle_entry_usable(cached):
                self._record_job_cache_event("bundle", hit=True)
                assert cached is not None
                return dataclass_replace(
                    cached,
                    template_generation=artifacts.generation,
                )
            self._record_job_cache_event("bundle", hit=False)
            built = self.build_shared_job_bundle(artifacts, worker)
            with self._job_cache_lock:
                self._job_bundle_cache[built.key] = built
            return built

    def build_shared_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentity,
    ) -> CachedJobBundle:
        phases = self._job_build_phases()
        template = artifacts.template
        issued_at_ms = now_ms()
        started = time.monotonic()
        _, ready_miner_count = self.accepted_share_stats()
        ready = ready_miner_count >= self.min_ready_miners
        # Bound the snapshot to a superset of the 8x reward window rather than
        # the whole accepted history: same audit bundle and digest, but the
        # ledger phase no longer scales with total ledger size.
        snapshot_window_weight = (
            PRISM_REWARD_WINDOW_MULTIPLIER
            * PRISM_SNAPSHOT_WINDOW_MARGIN
            * int(artifacts.network_difficulty)
        )
        shares = (
            [
                record.to_prism_json()
                for record in self.ledger.snapshot_at_job_issue(
                    issued_at_ms, window_weight=snapshot_window_weight
                )
            ]
            if ready
            else []
        )
        prior_balances = self.ledger.current_prior_balances()
        phases["ledger"] = phases.get("ledger", 0.0) + (time.monotonic() - started)
        started = time.monotonic()
        placeholder_suffix_hex = self.coinbase_script_sig_suffix_hex(
            PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
            "00" * self.extranonce2_size,
        )
        if ready and shares:
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
            )
            collection_only = False
            key: tuple[object, ...] = (artifacts.fingerprint, "ready")
        else:
            bundle = self.build_collection_bundle(
                template=template,
                transaction_hexes=artifacts.transaction_hexes,
                worker=worker,
                network_difficulty=artifacts.network_difficulty,
                issued_at_ms=issued_at_ms,
                suffix_hex=placeholder_suffix_hex,
            )
            shares = []
            collection_only = True
            key = (
                artifacts.fingerprint,
                "collection",
                worker.payout_address,
                worker.p2mr_program_hex,
            )
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
            bundle=bundle,
            shares_json=shares,
            prior_balances=prior_balances,
            found_block=bundle["found_block"],
            collection_only=collection_only,
            issued_at_ms=issued_at_ms,
            base_job=base_job,
            built_monotonic=time.monotonic(),
            template_generation=artifacts.generation,
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
            bundle=cached.bundle,
            shares_json=cached.shares_json,
            prior_balances=cached.prior_balances,
            found_block=cached.found_block,
            share_weight=self.share_weight_for_worker(client.worker),
            collection_only=cached.collection_only,
            worker=client.worker,
            issued_at_ms=cached.issued_at_ms,
            template_fingerprint=cached.template_fingerprint,
            template_generation=cached.template_generation,
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

    def _ensure_worker_metrics_state(self) -> None:
        if not hasattr(self, "worker_metrics_lock"):
            self.worker_metrics_lock = threading.Lock()
        if not hasattr(self, "worker_share_counts"):
            self.worker_share_counts = {}
        if not hasattr(self, "worker_rejection_counts"):
            self.worker_rejection_counts = {}

    def _ensure_tip_refresh_state(self) -> None:
        if not hasattr(self, "_tip_refresh_lock"):
            self._tip_refresh_lock = threading.Lock()
        if not hasattr(self, "_tip_refresh_executor_lock"):
            self._tip_refresh_executor_lock = threading.Lock()
        if not hasattr(self, "_tip_refresh_executor"):
            self._tip_refresh_executor: ThreadPoolExecutor | None = None
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
        if not hasattr(self, "tip_refresh_client_counts"):
            self.tip_refresh_client_counts = {
                result: 0 for result in PRISM_TIP_REFRESH_RESULTS
            }
        if not hasattr(self, "tip_refresh_inflight"):
            self.tip_refresh_inflight = 0

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

    def _record_tip_refresh_client_result(self, result: str) -> None:
        if result not in PRISM_TIP_REFRESH_RESULTS:
            raise ValueError(f"unknown tip refresh result: {result}")
        self._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_client_counts[result] += 1

    def _tip_refresh_future_started(self) -> None:
        self._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_inflight += 1

    def _tip_refresh_future_finished(self, _future: Future[RefreshResult]) -> None:
        self._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_inflight = max(0, self.tip_refresh_inflight - 1)

    def tip_refresh_executor(self) -> ThreadPoolExecutor:
        self._ensure_tip_refresh_state()
        with self._tip_refresh_executor_lock:
            if self._tip_refresh_executor_shutdown:
                raise RuntimeError("tip refresh executor is shut down")
            executor = self._tip_refresh_executor
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=self.tip_refresh_max_workers,
                    thread_name_prefix="prism-tip-refresh",
                )
                self._tip_refresh_executor = executor
            return executor

    def shutdown_tip_refresh_executor(self) -> None:
        self._ensure_tip_refresh_state()
        with self._tip_refresh_executor_lock:
            executor = self._tip_refresh_executor
            self._tip_refresh_executor = None
            self._tip_refresh_executor_shutdown = True
        if executor is not None:
            # Running workers may already hold client/job state or be inside a
            # socket send. Drain them before serve returns and the writer lease
            # is released; queued workers are cancelled without starting.
            executor.shutdown(wait=True, cancel_futures=True)

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

    def release_ledger_lease(self) -> None:
        """Best-effort writer-lease release for graceful shutdown so a restart
        reacquires immediately instead of waiting out the lease TTL. No-op for
        ledgers without a lease (the in-memory regtest ledger)."""
        release = getattr(self.ledger, "release_writer_lease", None)
        if release is None:
            return
        try:
            released = release()
        except Exception:
            print("prism coordinator: writer lease release failed during shutdown", flush=True)
            traceback.print_exc()
            return
        if released:
            print("prism coordinator: released writer lease on shutdown", flush=True)

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
            f"ledger={self.ledger.backend_name}",
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
        self.replay_pending_block_candidates()
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
            self._record_heartbeat("block_submitter")
            block_submitter_thread = threading.Thread(
                target=self.block_submit_loop,
                daemon=True,
            )
            block_submitter_thread.start()
            # Replay any shares stranded on disk by a prior ledger-outage
            # shutdown before serving, so no acked share is lost across restart.
            self.replay_recovered_shares()
            self._record_heartbeat("share_writer")
            self.share_writer_active = True
            share_writer_thread = threading.Thread(
                target=self.share_append_loop,
                daemon=True,
            )
            share_writer_thread.start()
            ctv_broadcaster_thread: threading.Thread | None = None
            if self.ctv_broadcaster_enabled:
                self._record_heartbeat("ctv_fanout_broadcaster")
                ctv_broadcaster_thread = threading.Thread(
                    target=self.ctv_fanout_broadcaster_loop,
                    daemon=True,
                )
                ctv_broadcaster_thread.start()
                print(
                    "prism coordinator: CTV fanout broadcaster enabled "
                    f"mode={'cpfp' if self.ctv_broadcaster_fee_sats > 0 else 'direct'} "
                    f"fee_bits={self.ctv_broadcaster_fee_sats} "
                    f"wallet={'configured' if self.ctv_broadcaster_wallet else 'none'} "
                    f"interval={self.ctv_broadcaster_interval_seconds:g}s "
                    f"limit={self.ctv_broadcaster_limit}",
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
            self.accept_loop(*listeners[0])
            blockpoll_thread.join(timeout=1)
            if blockwait_thread is not None:
                blockwait_thread.join(timeout=1)
            if vardiff_idle_sweep_thread is not None:
                vardiff_idle_sweep_thread.join(timeout=1)
            block_submitter_thread.join(timeout=1)
            # Give the share writer a real drain window on shutdown: acked
            # shares still queued are payouts.
            share_writer_thread.join(timeout=5)
            if ctv_broadcaster_thread is not None:
                ctv_broadcaster_thread.join(timeout=1)
            self.shutdown_tip_refresh_executor()

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

            with self.lock:
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
                thread.start()
            except (OSError, RuntimeError) as exc:
                # Admission is atomic with the global count. Undo it if socket
                # setup or thread creation fails before a handler owns cleanup,
                # then keep this listener alive for the next connection.
                try:
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

    def blockpoll_loop(self) -> None:
        while not self.stop_event.wait(self.blockpoll_seconds):
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
        last_success = getattr(self, "last_successful_template_refresh_monotonic", None)
        return last_success is not None and now - last_success >= budget

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
                known_tip = new_tip
                self.observe_tip_first_seen(new_tip)
                refreshed = self.poll_qbit_tip_template_once(heartbeat_name="qbit_blockwait")
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

    def run_ctv_fanout_broadcaster_once(
        self,
        *,
        progress_callback: Callable[[], None] | None = None,
    ) -> CtvFanoutDaemonResult:
        if self.ctv_fanout_broadcast_daemon is None:
            self.ctv_fanout_broadcast_daemon = self.make_ctv_fanout_broadcast_daemon()
        if progress_callback is None:
            return self.ctv_fanout_broadcast_daemon.run_once(limit=self.ctv_broadcaster_limit)
        return self.ctv_fanout_broadcast_daemon.run_once(
            limit=self.ctv_broadcaster_limit,
            progress_callback=progress_callback,
        )

    def ctv_fanout_broadcaster_loop(self) -> None:
        while not self.stop_event.is_set():
            self._record_heartbeat("ctv_fanout_broadcaster")
            started = time.monotonic()
            try:
                try:
                    result = self.run_ctv_fanout_broadcaster_once(
                        progress_callback=self._record_ctv_fanout_broadcaster_progress,
                    )
                finally:
                    # Stamp completion before logging or entering the interval
                    # wait. A blocked row never reaches this finally clause, so
                    # the watchdog remains able to recover a wedged operation.
                    self._record_heartbeat("ctv_fanout_broadcaster")
                    self.observe_ctv_fanout_broadcaster_pass(
                        max(0.0, time.monotonic() - started)
                    )
            except Exception:
                print("prism coordinator: CTV fanout broadcaster pass failed", flush=True)
                traceback.print_exc()
            else:
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
        self._ensure_job_cache_state()
        with self._job_cache_lock:
            artifacts = self._template_artifacts
        if (
            artifacts is None
            or artifacts.fingerprint != snapshot.template_fingerprint
            or artifacts.previousblockhash != snapshot.previousblockhash
        ):
            raise TemplateRefreshBlocked(
                "tip/template cache changed while preparing refreshed work"
            )
        return artifacts

    def prepare_tip_refresh_bundle(
        self,
        snapshot: QbitTipTemplateSnapshot,
        clients: list[ClientState],
    ) -> CachedJobBundle:
        artifacts = self._tip_refresh_artifacts(snapshot)
        with self.lock:
            representative = next(
                (
                    client
                    for client in clients
                    if client in self.clients and self.client_can_receive_jobs(client)
                ),
                None,
            )
            worker = representative.worker if representative is not None else None
        if worker is None:
            raise TemplateRefreshBlocked(
                "no authorized representative remained for prepared refresh"
            )
        build_started = time.monotonic()
        try:
            bundle = self.shared_job_bundle(artifacts, worker)
        except Exception as exc:
            with self.lock:
                self.job_build_failure_count += 1
            raise TemplateRefreshBlocked("prepared refresh bundle build failed") from exc
        finally:
            self._observe_tip_refresh_seconds(
                "bundle_build",
                time.monotonic() - build_started,
            )
        # Another path may have refreshed the shared artifact cache while the
        # heavy ledger/bundle build was in flight. Fail before submitting any
        # fanout task rather than issuing work from the superseded snapshot.
        self._tip_refresh_artifacts(snapshot)
        if bundle.collection_only:
            raise TemplateRefreshBlocked(
                "ready-pool prepared refresh unexpectedly produced a collection bundle"
            )
        return bundle

    def send_prepared_job(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        expected_connection_id: int,
        expected_active_job: PrismJobContext | None,
        cancel_event: _FanoutCancellation | None = None,
    ) -> RefreshResult:
        started = time.monotonic()
        phases = self._job_build_phases()
        phases.clear()
        with client.job_update_lock:
            if self.stop_event.is_set() or (cancel_event is not None and cancel_event.is_set()):
                return RefreshResult("skipped")
            with self.lock:
                if (
                    client not in self.clients
                    or client.connection_id != expected_connection_id
                    or not self.client_can_receive_jobs(client)
                    or self.intervening_job_supersedes_snapshot(
                        client.active_job,
                        expected_active_job,
                        snapshot,
                    )
                    or not self.client_needs_tip_template_refresh(client, snapshot)
                ):
                    return RefreshResult("skipped")
            phase_started = time.monotonic()
            try:
                if not self.ensure_reorg_reconciled_for_current_tip(
                    expected_tip_hash=snapshot.bestblockhash,
                ):
                    raise TemplateRefreshBlocked(
                        "qbit chain view became untrusted before prepared job delivery"
                    )
            except TemplateRefreshBlocked:
                if cancel_event is not None:
                    cancel_event.cancel()
                raise
            except Exception as exc:
                if cancel_event is not None:
                    cancel_event.cancel()
                raise TemplateRefreshBlocked(
                    "reorg reconciliation failed before prepared job delivery"
                ) from exc
            phases["reorg"] = time.monotonic() - phase_started
            if self.stop_event.is_set() or (cancel_event is not None and cancel_event.is_set()):
                return RefreshResult("skipped")
            delivery_admitted = cancel_event is None or cancel_event.begin_delivery()
            if not delivery_admitted:
                return RefreshResult("skipped")
            try:
                # prepare_tip_refresh_bundle validates the exact cache fingerprint
                # before any fanout task is submitted. From that point onward the
                # immutable bundle is the refresh snapshot: consulting the mutable
                # global cache here would let an unrelated same-tip job build abort
                # a partially delivered pass after replacing _template_artifacts.
                with self.lock:
                    if (
                        client not in self.clients
                        or client.connection_id != expected_connection_id
                        or not self.client_can_receive_jobs(client)
                        or self.intervening_job_supersedes_snapshot(
                            client.active_job,
                            expected_active_job,
                            snapshot,
                        )
                        or not self.client_needs_tip_template_refresh(client, snapshot)
                    ):
                        return RefreshResult("skipped")
                    clean_jobs = self.client_tip_changed_for_snapshot(client, snapshot)
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

                phase_started = time.monotonic()
                self.send_job_update(client, context.job)
                self.apply_job_difficulty(client, context.job)
                self.note_tip_work_delivered(
                    client,
                    str(context.template["previousblockhash"]),
                )
                delivered_monotonic = time.monotonic()
                phases["send"] = delivered_monotonic - phase_started
                elapsed = delivered_monotonic - started
                self.observe_job_build_elapsed(elapsed, phases)
                print(
                    "prism coordinator: sent prepared job "
                    f"connection={client.connection_id} username={client.username} "
                    f"job={context.job.job_id} elapsed={elapsed:.3f}s",
                    flush=True,
                )
                return RefreshResult("sent", delivered_monotonic)
            finally:
                if cancel_event is not None:
                    cancel_event.end_delivery()

    def _fanout_prepared_tip_refresh(
        self,
        clients: list[ClientState],
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        *,
        expected_active_jobs: dict[ClientState, PrismJobContext | None] | None = None,
        heartbeat_name: str,
    ) -> tuple[int, float | None, float | None, int]:
        executor = self.tip_refresh_executor()
        cancel_event = _FanoutCancellation()
        futures: dict[Future[RefreshResult], ClientState] = {}
        if expected_active_jobs is None:
            with self.lock:
                expected_active_jobs = {
                    client: client.active_job
                    for client in clients
                }
        for client in clients:
            if self.stop_event.is_set():
                break
            try:
                future = executor.submit(
                    self.send_prepared_job,
                    client,
                    bundle,
                    snapshot,
                    client.connection_id,
                    expected_active_jobs.get(client),
                    cancel_event,
                )
            except RuntimeError:
                if self.stop_event.is_set():
                    break
                raise
            self._tip_refresh_future_started()
            future.add_done_callback(self._tip_refresh_future_finished)
            futures[future] = client

        pending = set(futures)
        sent = 0
        failed = 0
        first_delivery: float | None = None
        last_delivery: float | None = None
        invalidation: TemplateRefreshBlocked | None = None
        while pending:
            self._record_heartbeat(heartbeat_name)
            if self.stop_event.is_set():
                cancel_event.set()
                for future in pending:
                    future.cancel()
                break
            done, pending = wait(
                pending,
                timeout=1.0,
                return_when=FIRST_COMPLETED,
            )
            for future in done:
                client = futures[future]
                if future.cancelled():
                    self._record_tip_refresh_client_result("skipped")
                    continue
                try:
                    result = future.result()
                except OSError:
                    self._record_tip_refresh_client_result("disconnected")
                    self.disconnect_client(client)
                    continue
                except TemplateRefreshBlocked as exc:
                    failed += 1
                    self._record_tip_refresh_client_result("failed")
                    invalidation = exc
                    cancel_event.set()
                    for pending_future in pending:
                        pending_future.cancel()
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
        if invalidation is not None:
            raise invalidation
        return sent, first_delivery, last_delivery, failed

    def poll_qbit_tip_template_once(self, *, heartbeat_name: str = "qbit_blockpoll") -> int:
        self._ensure_tip_refresh_state()
        refresh_started = time.monotonic()
        while not self._tip_refresh_lock.acquire(timeout=1.0):
            self._record_heartbeat(heartbeat_name)
            if self.stop_event.is_set():
                return 0
        try:
            observation_sequence = self._reserve_tip_observation_sequence()
            snapshot = self.fetch_qbit_tip_template_snapshot()
            self.pool_readiness_latched()
            if not self.ensure_reorg_reconciled_for_tip(snapshot.bestblockhash):
                raise TemplateRefreshBlocked(
                    "qbit chain view remained untrusted after reorg reconciliation"
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

            refreshed = 0
            build_failures = 0
            first_delivery: float | None = None
            last_delivery: float | None = None
            use_prepared_fanout = bool(
                clients
                and getattr(self, "_pool_ready_latched", False)
            )
            bundle: CachedJobBundle | None = None
            if use_prepared_fanout:
                try:
                    bundle = self.prepare_tip_refresh_bundle(snapshot, clients)
                except TemplateRefreshBlocked:
                    for _client in clients:
                        self._record_tip_refresh_client_result("failed")
                    raise

            # A ready-pool pass must validate and build its immutable shared
            # bundle before committing the observed tip. Otherwise a cache or
            # derivation failure can prune retained work without any replacement
            # job ready to fan out. Sequential/collection work has no shared
            # preparation stage, so it commits here immediately before builds.
            if not self.observe_tip_first_seen(
                snapshot.bestblockhash,
                observation_sequence=observation_sequence,
            ):
                raise TemplateRefreshBlocked(
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
                    raise TemplateRefreshBlocked(
                        "tip/template poll was superseded before snapshot publication"
                    )
                self.tip_template_snapshot = snapshot

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
                    expected_active_jobs=expected_active_jobs,
                    heartbeat_name=heartbeat_name,
                )
            else:
                for client in clients:
                    if self.stop_event.is_set():
                        break
                    # Collection bundles are worker-specific, so this first
                    # implementation retains their sequential build/send path.
                    self._record_heartbeat(heartbeat_name)
                    try:
                        if self.maybe_send_job(
                            client,
                            clean_jobs=self.client_tip_changed_for_snapshot(client, snapshot),
                            raise_on_reorg_failure=True,
                            raise_on_build_failure=True,
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
            self.last_successful_template_refresh_monotonic = time.monotonic()
            return refreshed
        finally:
            self._tip_refresh_lock.release()
            self._observe_tip_refresh_seconds(
                "refresh",
                time.monotonic() - refresh_started,
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
    ) -> bool:
        if observation_sequence is None:
            observation_sequence = self._reserve_tip_observation_sequence()
        now = time.monotonic()
        with self.lock:
            current_sequence = int(
                getattr(self, "current_tip_observation_sequence", 0)
            )
            if observation_sequence < current_sequence:
                return False
            first_seen = getattr(self, "current_tip_first_seen", None)
            if first_seen is not None and first_seen[0] == tip_hash:
                self.current_tip_observation_sequence = observation_sequence
                return True
            # The first tip this process observes is a startup baseline, not a
            # tip flip: a None stamp keeps the stale-grace window closed. Only
            # a change away from a previously observed tip records a flip time.
            self.current_tip_first_seen = (tip_hash, now if first_seen is not None else None)
            self.current_tip_observation_sequence = observation_sequence
            self.current_tip_parent = None

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

    def refresh_jobs_after_pending_accepted_block(self, client: ClientState) -> int:
        with self.lock:
            block = client.post_accept_refresh_block
            client.post_accept_refresh_block = None
        if block is None:
            return 0
        block_height, block_hash = block
        return self.refresh_jobs_after_accepted_block(
            block_height=block_height,
            block_hash=block_hash,
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
            raise TemplateRefreshBlocked(
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
            )
        return QbitTipTemplateSnapshot(
            bestblockhash=bestblockhash,
            previousblockhash=previousblockhash,
            template_fingerprint=qbit_template_fingerprint(template),
            template_generation=generation,
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
            raise TemplateRefreshBlocked(
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
        return not bool(summary.get("untrusted"))

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

    def reconcile_prism_pool_blocks_once(self, *, tip_hash: str | None = None) -> dict[str, object]:
        summary: dict[str, object] = {
            "enabled": bool(getattr(self, "reorg_reconciler_enabled", True)),
            "untrusted": False,
            "watched_blocks": 0,
            "inactive_blocks": 0,
            "reactivated_blocks": 0,
            "matured_payouts": 0,
        }
        if not getattr(self, "reorg_reconciler_enabled", True):
            return summary
        try:
            if self.qbit_chain_view_untrusted():
                with self.lock:
                    self.reorg_reconcile_skip_count += 1
                    self.last_reorg_reconciled_tip_hash = tip_hash
                    self.last_reorg_reconciled_trusted = False
                    self.last_reorg_reconciled_monotonic = time.monotonic()
                summary["untrusted"] = True
                return summary

            active_tip_height = int(self.rpc.call("getblockcount"))
            watch_blocks = getattr(self.ledger, "reorg_watch_blocks", None)
            if not callable(watch_blocks):
                return summary
            rows = watch_blocks(active_tip_height=active_tip_height)
            summary["watched_blocks"] = len(rows)

            inactive_blocks = 0
            reactivated_blocks = 0
            for row in rows:
                block_height = int(row["block_height"])
                block_hash = str(row["block_hash"]).lower()
                chain_state = str(row.get("chain_state", ""))
                if block_height > active_tip_height:
                    if chain_state == "confirmed":
                        inactive = self.ledger.mark_pool_block_inactive(
                            block_hash=block_hash,
                            active_tip_height=active_tip_height,
                        )
                        inactive_blocks += int(inactive.get("inactive_count", 0))
                    continue
                active_hash = str(self.rpc.call("getblockhash", [block_height])).lower()
                on_active_chain = active_hash == block_hash
                if on_active_chain and chain_state == "inactive":
                    reactivated = self.ledger.reactivate_pool_block(
                        block_hash=block_hash,
                        active_tip_height=active_tip_height,
                    )
                    reactivated_blocks += int(reactivated.get("reactivated_count", 0))
                elif not on_active_chain and chain_state == "confirmed":
                    inactive = self.ledger.mark_pool_block_inactive(
                        block_hash=block_hash,
                        active_tip_height=active_tip_height,
                    )
                    inactive_blocks += int(inactive.get("inactive_count", 0))

            matured_payouts = 0
            mark_mature = getattr(self.ledger, "mark_mature_pool_payouts", None)
            if callable(mark_mature):
                matured = mark_mature(active_tip_height=active_tip_height)
                matured_payouts = int(matured.get("matured_count", 0))

            with self.lock:
                self.reorg_inactive_block_count += inactive_blocks
                self.reorg_reactivated_block_count += reactivated_blocks
                self.matured_payout_count += matured_payouts
                self.last_reorg_reconciled_tip_hash = tip_hash
                self.last_reorg_reconciled_trusted = True
                self.last_reorg_reconciled_monotonic = time.monotonic()
            summary["inactive_blocks"] = inactive_blocks
            summary["reactivated_blocks"] = reactivated_blocks
            summary["matured_payouts"] = matured_payouts
            return summary
        except Exception:
            with self.lock:
                self.reorg_reconcile_error_count += 1
                self.last_reorg_reconciled_trusted = False
                self.last_reorg_reconciled_monotonic = time.monotonic()
            raise

    def client_can_receive_jobs(self, client: ClientState) -> bool:
        return client.subscribed and client.authorized and client.worker is not None

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
            self._pool_ready_latched = True
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
        return (
            previousblockhash != snapshot.bestblockhash
            or previousblockhash != snapshot.previousblockhash
            or context_fingerprint != snapshot.template_fingerprint
        )

    @staticmethod
    def intervening_job_supersedes_snapshot(
        active_job: PrismJobContext | None,
        expected_active_job: PrismJobContext | None,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        if active_job is expected_active_job or active_job is None:
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

    def disconnect_client(self, client: ClientState) -> None:
        with client.job_update_lock:
            with self.lock:
                self.clients.discard(client)
                for job_id in client.active_job_ids:
                    self.jobs.pop(job_id, None)
                client.active_job_ids.clear()
                self._ensure_evicted_job_state()
                for job_id in tuple(
                    self.evicted_jobs_by_connection.get(client.connection_id, ())
                ):
                    self._remove_evicted_job_locked(job_id)
            client.close()

    def handle_request(self, client: ClientState, request: dict[str, object]) -> None:
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
                self.maybe_send_job(client, clean_jobs=True)
            return
        if method == "mining.authorize":
            with client.job_update_lock:
                username = str(params[0]) if params else ""
                password = str(params[1]) if len(params) > 1 and params[1] is not None else ""
                worker = self.resolve_worker(username)
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
                difficulty_job_delivered = False
                if target is not None:
                    difficulty_job_delivered = self.advertise_client_difficulty(client, target)
                client.authorized = True
                self.send_result(client, request_id, True)
                # On a re-authorize whose new options already advertised a fresh
                # difficulty/job pair, do not send a second back-to-back pair.
                if not difficulty_job_delivered:
                    self.maybe_send_job(client, clean_jobs=True)
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
    ) -> bool:
        with client.job_update_lock:
            return self._maybe_send_job_locked(
                client,
                clean_jobs=clean_jobs,
                raise_on_reorg_failure=raise_on_reorg_failure,
                raise_on_build_failure=raise_on_build_failure,
            )

    def _maybe_send_job_locked(
        self,
        client: ClientState,
        *,
        clean_jobs: bool,
        raise_on_reorg_failure: bool = False,
        raise_on_build_failure: bool = False,
    ) -> bool:
        if not client.subscribed or not client.authorized or client.worker is None:
            return False
        self._ensure_job_cache_state()
        started = time.monotonic()
        phases = self._job_build_phases()
        phases.clear()
        print(
            f"prism coordinator: building job connection={client.connection_id} username={client.username}",
            flush=True,
        )
        phase_started = time.monotonic()
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
        try:
            context = self.build_job_for_client(client, clean_jobs=clean_jobs)
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
        client.active_job = context
        with self.lock:
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
        self.apply_job_difficulty(client, context.job)
        self.note_tip_work_delivered(client, str(context.template["previousblockhash"]))
        phases["send"] = time.monotonic() - phase_started
        elapsed = time.monotonic() - started
        self.observe_job_build_elapsed(elapsed, phases)
        phase_report = ",".join(
            f"{phase}:{phases[phase]:.3f}" for phase in PRISM_JOB_BUILD_PHASES if phase in phases
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
        with self.lock:
            current = client.pending_share_difficulty or client.share_difficulty
            if target == current:
                return False
            if not (client.subscribed and client.authorized):
                client.share_difficulty = target
                return False
            prior_pending = client.pending_share_difficulty
            client.pending_share_difficulty = target
        if not self.stop_event.is_set() and self.maybe_send_job(client, clean_jobs=True):
            return True
        with self.lock:
            if client.pending_share_difficulty == target:
                client.pending_share_difficulty = prior_pending
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
        phases = self._job_build_phases()
        artifacts = self.current_template_artifacts()
        cached_bundle = self.shared_job_bundle(artifacts, client.worker)
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
    ) -> dict[str, Any]:
        payload = {
            "shares": shares,
            "found_block": found_block,
            "prior_balances": prior_balances,
            "payout_policy": self.prism_payout_policy(),
            "coinbase_script_sig_suffix_hex": coinbase_script_sig_suffix_hex,
            "witness_merkle_leaves_hex": witness_merkle_leaves_hex or [],
        }
        ctv_settlement = self.prism_ctv_settlement_config(
            block_height=int(found_block["block_height"]),
            parent_hash=ctv_fee_parent_hash,
        )
        if ctv_settlement is not None:
            payload["ctv_settlement"] = ctv_settlement
        completed = subprocess.run(
            prism_tool_command("qbit-prism-build-audit-bundle")
            + [
                "--input",
                "-",
                "--signing-key-seed-hex",
                self.signing_seed_hex,
                "--ledger-signing-key-seed-hex",
                self.ledger_attestation_signing_seed_hex,
            ],
            input=json.dumps(payload),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"qbit-prism-build-audit-bundle failed: {completed.stderr}")
        return json.loads(completed.stdout)

    def coinbase_script_sig_suffix_hex(self, extranonce1_hex: str, extranonce2_hex: str) -> str:
        extranonce1_hex = validate_hex(extranonce1_hex, name="extranonce1")
        extranonce2_hex = validate_hex(extranonce2_hex, name="extranonce2")
        return self.coinbase_tag_hex + extranonce1_hex + extranonce2_hex

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
        current_tip = str(self.rpc.call("getbestblockhash"))
        # Do not anchor the stale-grace window from this submit-path tip read.
        # Only blockpoll/blockwait may open the window (see
        # stale_grace_deadline_open): a submit's getbestblockhash can observe a
        # new tip while job refresh still lags, and anchoring here would start
        # the grace clock late and credit prior-tip shares long after the real
        # tip change.
        #
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
            bundle={},
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
        try:
            if wait:
                queue_obj.put(
                    entry,
                    timeout=getattr(self, "share_commit_timeout_seconds", 15.0),
                )
            else:
                queue_obj.put_nowait(entry)
        except queue.Full:
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
                if stopping:
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
            for entry, record in zip(batch, records, strict=True):
                entry.record = record
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
            self.replay_pending_block_candidates()
            self.submit_next_block_candidate(timeout=1.0)

    def submit_next_block_candidate(self, timeout: float | None = None) -> bool:
        """Dequeue and land one block candidate; returns True when one ran.

        The block-submitter loop calls this continuously; tests call it
        directly to drain the queue deterministically.
        """
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
        active_tip_height = int(self.rpc.call("getblockcount"))
        self._record_heartbeat("block_submitter")
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
        )
        final_manifest = final_bundle["signed_coinbase_manifest"]["manifest"]
        if final_manifest["coinbase_tx_hex"].lower() != submission.coinbase_tx_hex.lower():
            self.stop_event.set()
            self._abandon_block_candidate(
                PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
                "final audit bundle coinbase does not match submitted coinbase",
                worker=worker,
            )
            return False
        candidate_bundle_path = self.write_temporary_audit_bundle(
            final_bundle,
            block_hash=submission.block_hash_hex,
        )
        try:
            report = self.verify_bundle(
                candidate_bundle_path,
                submission.coinbase_tx_hex,
                self.trusted_ledger_writer_public_key_hex(final_bundle),
                expected_coinbase_value_sats=int(context.template["coinbasevalue"]),
            )
            self._record_heartbeat("block_submitter")
            # Finalization is exact-idempotent so an active-tip/active-ancestor
            # replay can safely repeat any step after a crash.
            self._record_heartbeat("block_submitter")
            persistence = self.ledger.persist_accepted_block(
                block_hash=submission.block_hash_hex,
                block_height=expected_height,
                parent_hash=str(context.template["previousblockhash"]),
                final_bundle=final_bundle,
                audit_report=report,
            )
            self._record_heartbeat("block_submitter")
            confirmation = self.ledger.confirm_accepted_block(
                block_hash=block_hash,
                active_tip_height=active_tip_height,
            )
            if int(confirmation.get("confirmed_count", 0)) not in {0, 1}:
                self.stop_event.set()
                self._abandon_block_candidate(
                    PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                    f"ledger did not confirm accepted block {block_hash}",
                    worker=worker,
                )
                return False
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
                self.latest_bundle = final_bundle
                self.latest_evidence = evidence
                should_stop = self.stop_after_block or self.accepted_block_count >= self.max_blocks
            print(
                "prism coordinator: qbit accepted direct PRISM block "
                f"height={expected_height} hash={block_hash}",
                flush=True,
            )
            if should_stop:
                self.stop_event.set()
            else:
                # Fresh work is the most urgent post-block action: push it
                # directly from the submitter instead of waiting for the
                # winning client's next message. Stamp the block_submitter
                # heartbeat (not the poller's) through the refresh so a long
                # multi-client push on this thread is not mistaken for a hang
                # and does not trip a false liveness-watchdog exit.
                self.refresh_jobs_after_accepted_block(
                    block_height=expected_height,
                    block_hash=block_hash,
                    heartbeat_name="block_submitter",
                )
            return True
        finally:
            try:
                candidate_bundle_path.unlink()
            except FileNotFoundError:
                pass

    def reject_prepared_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        reject = getattr(self.ledger, "reject_prepared_block", None)
        if callable(reject):
            return reject(block_hash=block_hash, active_tip_height=active_tip_height)
        return self.ledger.reverse_immature_block(
            block_hash=block_hash,
            active_tip_height=active_tip_height,
        )

    def write_temporary_audit_bundle(self, bundle: dict[str, Any], *, block_hash: str) -> Path:
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=self.audit_dir,
            prefix=f".prism-live-audit-bundle-candidate-{block_hash}-",
            suffix=".json.tmp",
            delete=False,
        ) as handle:
            json.dump(bundle, handle, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
            return Path(handle.name)

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

    def health_payload(self) -> dict[str, object]:
        accepted_share_count, ready_miner_count = self.accepted_share_stats()
        return {
            "ok": True,
            "schema": "qbit.prism.audit-health.v1",
            "ledger_backend": self.ledger.backend_name,
            "accepted_share_count": accepted_share_count,
            "ready_miner_count": ready_miner_count,
            "accepted_block": self.accepted_block_count > 0,
            "accepted_block_count": self.accepted_block_count,
            "max_blocks": self.max_blocks,
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
                return 200, self.refresh_health_snapshot()
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
        return 200, payload

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

    def metrics_payload(self) -> str:
        ledger_metrics = self.ledger.metrics()
        audit_metrics = self.audit_artifact_metrics()
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
        if self.latest_bundle is not None:
            coinbase_hex = self.latest_bundle["signed_coinbase_manifest"]["manifest"]["coinbase_tx_hex"]
            coinbase_weight_headroom = 2_000_000 - (len(coinbase_hex) // 2)
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
        lines.extend(self.ctv_fanout_broadcaster_metrics_lines())
        lines.extend(self.job_build_metrics_lines())
        lines.extend(self.tip_refresh_metrics_lines())
        return "\n".join(lines) + "\n"

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
        metric_name = "qbit_prism_ctv_fanout_broadcaster_pass_seconds"
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
            client_counts = dict(self.tip_refresh_client_counts)
            inflight = self.tip_refresh_inflight

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
        lines.extend(
            [
                "# HELP qbit_prism_tip_refresh_clients_total Client outcomes from tip/template refresh passes.",
                "# TYPE qbit_prism_tip_refresh_clients_total counter",
                *[
                    f'qbit_prism_tip_refresh_clients_total{{result="{result}"}} {int(client_counts.get(result, 0))}'
                    for result in PRISM_TIP_REFRESH_RESULTS
                ],
                "# HELP qbit_prism_tip_refresh_inflight Prepared refresh client tasks currently queued or running.",
                "# TYPE qbit_prism_tip_refresh_inflight gauge",
                f"qbit_prism_tip_refresh_inflight {inflight}",
                "# HELP qbit_prism_tip_refresh_executor_workers Configured persistent refresh executor workers, or zero before creation.",
                "# TYPE qbit_prism_tip_refresh_executor_workers gauge",
                f"qbit_prism_tip_refresh_executor_workers {executor_workers}",
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
            "# HELP qbit_prism_job_build_seconds Wall time from job build start to job sent, per client job.",
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
        print(f"prism coordinator: received signal {signum}; shutting down", flush=True)
        coordinator.stop_event.set()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)
    try:
        coordinator.serve()
    finally:
        coordinator.shutdown_tip_refresh_executor()
        coordinator.release_ledger_lease()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
