#!/usr/bin/env python3
"""Minimal live direct qbit Stratum coordinator for PRISM regtest proof."""

from __future__ import annotations

import base64
from contextlib import contextmanager
import hashlib
import json
import math
import os
import shlex
import signal
import socket
import struct
import subprocess
import threading
import time
import traceback
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, replace as dataclass_replace
from decimal import Decimal, ROUND_CEILING
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator

import sys

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.auxpow import stratum_codec, vardiff
from lab.prism import direct_stratum, public_api
from lab.prism.prism_tools import prism_tool_command
from lab.prism.ctv_broadcaster import CtvFanoutBroadcaster
from lab.prism.ctv_broadcaster_daemon import CtvFanoutBroadcastDaemon, CtvFanoutDaemonResult
from lab.prism.share_ledger import (
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
DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS = 10.0
DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS = 5.0
DEFAULT_PRISM_HEALTH_REFRESH_SECONDS = 5.0
DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS = 20.0
MAX_ACTIVE_PRISM_JOBS_PER_CLIENT = 16
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
PRISM_JOB_BUILD_PHASES = ("reorg", "template", "merkle", "ledger", "bundle", "stamp", "send")
PRISM_JOB_CACHE_KINDS = ("template", "bundle")
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
    return env_bool("QBIT_PRODUCTION", "0") or env_bool("QBIT_TOOLS_PRODUCTION", "0")


def require_production_env(name: str) -> str:
    value = env_optional(name)
    if value is None:
        raise SystemExit(f"QBIT_PRODUCTION=1 requires {name}")
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
            raise SystemExit(f"QBIT_PRODUCTION=1 rejects {name}=1")

    prism_database_url = env_optional("PRISM_DATABASE_URL")
    if prism_database_url is None and env_optional("PRISM_POSTGRES_PSQL_COMMAND") is None:
        raise SystemExit("QBIT_PRODUCTION=1 requires PRISM_DATABASE_URL or PRISM_POSTGRES_PSQL_COMMAND")
    if env_optional("PRISM_POSTGRES_PASSWORD") == "change-this":
        raise SystemExit("QBIT_PRODUCTION=1 requires a non-default PRISM_POSTGRES_PASSWORD")
    if prism_database_url is not None and "change-this" in prism_database_url:
        raise SystemExit("QBIT_PRODUCTION=1 requires a non-default PRISM_DATABASE_URL")

    require_production_env("PRISM_MANIFEST_SIGNING_SEED_HEX")
    require_production_env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX")
    require_production_env("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX")
    require_production_env("PRISM_LEDGER_WRITER_ID")
    require_production_env("PRISM_LEDGER_WRITER_EPOCH")
    require_production_env("PRISM_AUDIT_DIR")
    require_production_env("PRISM_EVIDENCE_PATH")

    if env_optional("PRISM_LEDGER_WRITER_SESSION_TOKEN") is not None:
        raise SystemExit("QBIT_PRODUCTION=1 requires managed ledger session tokens; unset PRISM_LEDGER_WRITER_SESSION_TOKEN")

    require_production_env("QBIT_RPC_USER")
    qbit_rpc_password = require_production_env("QBIT_RPC_PASSWORD")
    if qbit_rpc_password == "change-this":
        raise SystemExit("QBIT_PRODUCTION=1 requires a non-default QBIT_RPC_PASSWORD")


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
        self.url = f"http://{host}:{port}"
        credentials = f"{user}:{password}".encode()
        self.auth = f"Basic {base64.b64encode(credentials).decode()}"

    def call(self, method: str, params: list[object] | None = None, *, wallet: str | None = None) -> Any:
        body = json.dumps(
            {
                "jsonrpc": "1.0",
                "id": method,
                "method": method,
                "params": params or [],
            }
        ).encode()
        url = self.url
        if wallet is not None:
            url = f"{self.url}/wallet/{urllib.parse.quote(wallet, safe='')}"
        request = urllib.request.Request(
            url,
            data=body,
            headers={
                "Authorization": self.auth,
                "Content-Type": "application/json",
                "User-Agent": "qbit-prism-coordinator/0.1",
            },
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read())
        if payload["error"] is not None:
            raise RuntimeError(f"qbit RPC {method} failed: {payload['error']}")
        return payload["result"]


@dataclass(frozen=True)
class WorkerIdentity:
    username: str
    payout_address: str
    worker_name: str | None
    script_pubkey_hex: str
    p2mr_program_hex: str


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


@dataclass(frozen=True)
class QbitTipTemplateSnapshot:
    bestblockhash: str
    previousblockhash: str
    template_fingerprint: str


@dataclass(frozen=True)
class CachedTemplateArtifacts:
    """Template plus everything derivable from it alone, shared by all clients.

    Derived fields are keyed by the template fingerprint: a refetch whose
    fingerprint matches (only clock fields moved) reuses the previously
    computed transaction hexes and witness merkle leaves instead of re-hashing
    the full template.
    """

    template: dict[str, Any]
    fingerprint: str
    previousblockhash: str
    transaction_hexes: tuple[str, ...]
    witness_merkle_leaves_hex: tuple[str, ...]
    network_difficulty: int
    fetched_monotonic: float


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
    share_difficulty: Decimal = Decimal("1")
    pending_share_difficulty: Decimal | None = None
    vardiff_window_started_monotonic: float = field(default_factory=time.monotonic)
    vardiff_window_accepted: int = 0
    vardiff_window_submitted: int = 0
    vardiff_window_work: Decimal = Decimal("0")
    vardiff_difficulty_estimate: Decimal | None = None
    active_job_ids: set[str] = field(default_factory=set)
    post_accept_refresh_block: tuple[int, str] | None = None
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    def send(self, payload: dict[str, object]) -> None:
        data = json.dumps(payload).encode() + b"\n"
        with self.send_lock:
            self.sock.sendall(data)

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()


class StratumError(RuntimeError):
    def __init__(self, code: int, message: str, *, reason: str | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.reason = reason


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
        self.coinbase_tag_hex = default_prism_coinbase_tag_hex()
        self.share_difficulty = env_decimal("PRISM_STRATUM_SHARE_DIFF", "0.000000001")
        self.vardiff_config = load_prism_vardiff_config(self.share_difficulty)
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
        self._ctv_fanout_market_fee_rate_cache: dict[tuple[int | None, str | None], int] = {}
        self.ctv_fanout_broadcast_daemon: CtvFanoutBroadcastDaemon | None = None
        self.lock = threading.RLock()
        self.clients: set[ClientState] = set()
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
        self.rejection_counts_by_reason = {reason: 0 for reason in PRISM_REJECTION_REASON_IDS}
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

    def record_rejection(self, reason: str) -> None:
        if reason not in PRISM_REJECTION_REASON_IDS:
            raise ValueError(f"unknown PRISM rejection reason: {reason}")
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

    def reject_stratum(self, code: int, reason: str, message: str) -> None:
        self.record_rejection(reason)
        raise StratumError(code, message, reason=reason)

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
            return SingleWriterShareLedger()
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

    def _derive_template_artifacts(self, template: dict[str, Any]) -> CachedTemplateArtifacts:
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
        )

    def _store_template_artifacts(self, artifacts: CachedTemplateArtifacts) -> None:
        with self._job_cache_lock:
            previous = self._template_artifacts
            self._template_artifacts = artifacts
            if previous is not None and previous.fingerprint != artifacts.fingerprint:
                self._job_bundle_cache = {
                    key: entry
                    for key, entry in self._job_bundle_cache.items()
                    if entry.template_fingerprint == artifacts.fingerprint
                }

    def store_template_artifacts(self, template: dict[str, Any]) -> CachedTemplateArtifacts | None:
        """Best-effort cache fill from an already-fetched template (blockpoll).

        Returns None instead of raising so a template the derivation cannot
        digest degrades to the legacy per-build fetch path rather than failing
        the poll.
        """
        self._ensure_job_cache_state()
        try:
            artifacts = self._derive_template_artifacts(template)
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
        phases = self._job_build_phases()
        started = time.monotonic()
        template = self.rpc.call(
            "getblocktemplate",
            [{"rules": qbit_gbt_rules(getattr(self, "qbit_chain", "regtest"))}],
        )
        if not isinstance(template, dict):
            raise RuntimeError("getblocktemplate returned non-object")
        phases["template"] = phases.get("template", 0.0) + (time.monotonic() - started)
        artifacts = self._derive_template_artifacts(template)
        self._store_template_artifacts(artifacts)
        return artifacts

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
            return cached
        with self._job_build_lock:
            cached = self._lookup_job_bundle(artifacts.fingerprint, worker)
            if self._job_bundle_entry_usable(cached):
                self._record_job_cache_event("bundle", hit=True)
                return cached
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
        shares = (
            [record.to_prism_json() for record in self.ledger.snapshot_at_job_issue(issued_at_ms)]
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
        if self.audit_bind and self.audit_port:
            self.start_audit_server()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.bind, self.port))
            server.listen()
            server.settimeout(1)
            # Seed liveness before starting monitored loops so the watchdog
            # never fires during startup.
            self._record_heartbeat("stratum_accept")
            self._record_heartbeat("qbit_blockpoll")
            blockpoll_thread = threading.Thread(target=self.blockpoll_loop, daemon=True)
            blockpoll_thread.start()
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
            while not self.stop_event.is_set():
                self._record_heartbeat("stratum_accept")
                try:
                    sock, address = server.accept()
                except socket.timeout:
                    continue
                sock.settimeout(None)
                self.apply_stratum_send_timeout(sock)
                with self.lock:
                    self.connection_counter += 1
                    connection_id = self.connection_counter
                client = ClientState(
                    sock=sock,
                    address=address,
                    connection_id=connection_id,
                    extranonce1_hex=f"{connection_id & 0xFFFFFFFF:08x}",
                    share_difficulty=self.client_startup_difficulty(),
                )
                with self.lock:
                    self.clients.add(client)
                thread = threading.Thread(target=self.handle_client, args=(client,), daemon=True)
                thread.start()
            blockpoll_thread.join(timeout=1)
            if ctv_broadcaster_thread is not None:
                ctv_broadcaster_thread.join(timeout=1)

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

        Job refreshes iterate clients sequentially; an unresponsive peer whose
        TCP buffer is full would otherwise block ``sendall`` indefinitely and
        stall job delivery for every other miner. SO_SNDTIMEO turns that into
        an OSError, which the existing failure paths treat as a dead client.
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

    def run_ctv_fanout_broadcaster_once(self) -> CtvFanoutDaemonResult:
        if self.ctv_fanout_broadcast_daemon is None:
            self.ctv_fanout_broadcast_daemon = self.make_ctv_fanout_broadcast_daemon()
        return self.ctv_fanout_broadcast_daemon.run_once(limit=self.ctv_broadcaster_limit)

    def ctv_fanout_broadcaster_loop(self) -> None:
        while not self.stop_event.is_set():
            self._record_heartbeat("ctv_fanout_broadcaster")
            try:
                result = self.run_ctv_fanout_broadcaster_once()
                if result.scanned_count or result.submitted_count or result.failed_count:
                    print(
                        "prism coordinator: CTV fanout broadcaster "
                        f"scanned={result.scanned_count} "
                        f"submitted={result.submitted_count} "
                        f"updated={result.updated_count} "
                        f"failed={result.failed_count}",
                        flush=True,
                    )
            except Exception:
                print("prism coordinator: CTV fanout broadcaster pass failed", flush=True)
                traceback.print_exc()
            if self.stop_event.wait(self.ctv_broadcaster_interval_seconds):
                break

    def poll_qbit_tip_template_once(self) -> int:
        snapshot = self.fetch_qbit_tip_template_snapshot()
        if not self.ensure_reorg_reconciled_for_tip(snapshot.bestblockhash):
            return 0
        with self.lock:
            previous_snapshot = self.tip_template_snapshot
            snapshot_changed = previous_snapshot is not None and previous_snapshot != snapshot
            self.tip_template_snapshot = snapshot
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

        refreshed = 0
        for client in clients:
            if self.stop_event.is_set():
                break
            # The refresh loop runs on the blockpoll thread; keep its liveness
            # heartbeat fresh per client so a long refresh pass (many clients,
            # or one blocked send) is never mistaken for a hung poller.
            self._record_heartbeat("qbit_blockpoll")
            try:
                if self.maybe_send_job(
                    client,
                    clean_jobs=self.client_tip_changed_for_snapshot(client, snapshot),
                ):
                    refreshed += 1
            except OSError:
                self.disconnect_client(client)

        if refreshed:
            with self.lock:
                self.tip_refresh_job_count += refreshed
        return refreshed

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

    def refresh_jobs_after_accepted_block(self, *, block_height: int, block_hash: str) -> int:
        try:
            refreshed = self.poll_qbit_tip_template_once()
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
        bestblockhash = str(self.rpc.call("getbestblockhash"))
        template = self.rpc.call(
            "getblocktemplate",
            [{"rules": qbit_gbt_rules(getattr(self, "qbit_chain", "regtest"))}],
        )
        if not isinstance(template, dict):
            raise RuntimeError("getblocktemplate returned non-object")
        # The poll already paid for this template; seed the job-build cache so
        # client job builds triggered by the refresh below reuse it instead of
        # refetching one template per client.
        artifacts = self.store_template_artifacts(template)
        if artifacts is not None:
            return QbitTipTemplateSnapshot(
                bestblockhash=bestblockhash,
                previousblockhash=artifacts.previousblockhash,
                template_fingerprint=artifacts.fingerprint,
            )
        return QbitTipTemplateSnapshot(
            bestblockhash=bestblockhash,
            previousblockhash=str(template.get("previousblockhash", "")),
            template_fingerprint=qbit_template_fingerprint(template),
        )

    def ensure_reorg_reconciled_for_current_tip(self) -> bool:
        if not getattr(self, "reorg_reconciler_enabled", True):
            return True
        current_tip = str(self.rpc.call("getbestblockhash"))
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
        if blockchain_info.get("initialblockdownload"):
            return True
        blocks_raw = blockchain_info.get("blocks")
        headers_raw = blockchain_info.get("headers")
        if blocks_raw is not None and headers_raw is not None:
            try:
                if int(headers_raw) > int(blocks_raw):
                    return True
            except (TypeError, ValueError) as exc:
                raise RuntimeError("getblockchaininfo blocks/headers are not integers") from exc
        return False

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

    def client_needs_tip_template_refresh(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        context = client.active_job
        if context is None:
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
        reader = client.sock.makefile("r", encoding="utf-8", newline="\n")
        try:
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
                except Exception:
                    print(
                        f"prism coordinator: client thread failed address={client.address}",
                        flush=True,
                    )
                    traceback.print_exc()
                    break
        finally:
            reader.close()
            self.disconnect_client(client)

    def disconnect_client(self, client: ClientState) -> None:
        with self.lock:
            self.clients.discard(client)
            for job_id in client.active_job_ids:
                self.jobs.pop(job_id, None)
            client.active_job_ids.clear()
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
            client.subscribed = True
            self.send_result(client, request_id, [[], client.extranonce1_hex, self.extranonce2_size])
            self.maybe_send_job(client, clean_jobs=True)
            return
        if method == "mining.authorize":
            username = str(params[0]) if params else ""
            client.worker = self.resolve_worker(username)
            client.username = username
            client.authorized = True
            self.send_result(client, request_id, True)
            self.maybe_send_job(client, clean_jobs=True)
            return
        if method == "mining.extranonce.subscribe":
            self.send_result(client, request_id, True)
            return
        if method == "mining.suggest_difficulty":
            self.send_result(client, request_id, True)
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
        validation = self.rpc.call("validateaddress", [address])
        if not isinstance(validation, dict) or not validation.get("isvalid"):
            raise StratumError(20, f"{label} is not a valid qbit address: {address}")
        script = str(validation.get("scriptPubKey") or "")
        if not script.startswith("5220") or len(script) != 68:
            raise StratumError(20, f"{label} does not resolve to a P2MR script: {address}")
        return script, script[4:]

    def maybe_send_job(self, client: ClientState, *, clean_jobs: bool) -> bool:
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
                return False
        except Exception:
            print(
                f"prism coordinator: reorg reconciliation failed before job build "
                f"connection={client.connection_id} username={client.username}; skipping this job",
                flush=True,
            )
            traceback.print_exc()
            return False
        phases["reorg"] = time.monotonic() - phase_started
        try:
            context = self.build_job_for_client(client, clean_jobs=clean_jobs)
        except Exception:
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
            return False
        client.active_job = context
        with self.lock:
            if clean_jobs:
                for job_id in client.active_job_ids:
                    self.jobs.pop(job_id, None)
                client.active_job_ids.clear()
            self.jobs[context.job.job_id] = context
            client.active_job_ids.add(context.job.job_id)
            self.prune_client_active_jobs(client)
        phase_started = time.monotonic()
        self.send_difficulty(client, context.job)
        self.send_job(client, context.job)
        self.apply_job_difficulty(client, context.job)
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
            self.jobs.pop(oldest_job_id, None)

    def send_difficulty(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        self.send_difficulty_value(client, job.share_difficulty)

    def send_difficulty_value(self, client: ClientState, difficulty: Decimal) -> None:
        client.send(
            {
                "id": None,
                "method": "mining.set_difficulty",
                "params": [float(difficulty)],
            }
        )

    def client_startup_difficulty(self) -> Decimal:
        if not self.vardiff_config.enabled:
            return self.share_difficulty
        return vardiff.clamp(
            self.vardiff_config.startup_difficulty,
            self.vardiff_config.min_difficulty,
            self.vardiff_config.max_difficulty,
        )

    def desired_client_share_difficulty(self, client: ClientState) -> Decimal:
        if not self.vardiff_config.enabled:
            return self.share_difficulty
        return client.pending_share_difficulty or client.share_difficulty

    def apply_job_difficulty(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        if not self.vardiff_config.enabled:
            client.share_difficulty = job.share_difficulty
            client.pending_share_difficulty = None
            return
        pending = client.pending_share_difficulty
        client.share_difficulty = job.share_difficulty
        if pending is not None and job.share_difficulty == pending:
            client.pending_share_difficulty = None

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
        client.send(
            {
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
            self.reject_stratum(20, PRISM_REJECTION_MALFORMED_SUBMIT, "submit params are incomplete")
        worker_name, job_id, extranonce2_hex, ntime_hex, nonce_hex = [str(item) for item in params[:5]]
        version_bits_hex = str(params[5]) if len(params) > 5 else None
        if worker_name != client.username:
            self.reject_stratum(20, PRISM_REJECTION_UNAUTHORIZED_WORKER, "submit username does not match authorized username")
        if len(extranonce2_hex) != self.extranonce2_size * 2:
            self.reject_stratum(20, PRISM_REJECTION_INVALID_EXTRANONCE, "unexpected extranonce2 size")
        if len(ntime_hex) != 8 or len(nonce_hex) != 8:
            self.reject_stratum(20, PRISM_REJECTION_INVALID_NTIME_OR_NONCE, "ntime and nonce must be 4-byte hex strings")
        self.note_vardiff_submitted_share(client)
        with self.lock:
            context = self.jobs.get(job_id)
            if context is not None and job_id not in client.active_job_ids:
                context = None
        if context is None:
            self.reject_stratum(21, PRISM_REJECTION_UNKNOWN_JOB, "stale job")
        with self.lock:
            if self.accepted_block_count >= self.max_blocks:
                self.reject_stratum(21, PRISM_REJECTION_POOL_CLOSED, "pool is no longer accepting shares")

        current_tip = str(self.rpc.call("getbestblockhash"))
        if str(context.template["previousblockhash"]) != current_tip:
            self.reject_stratum(21, PRISM_REJECTION_STALE_JOB, "stale job")

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
            self.reject_stratum(20, PRISM_REJECTION_MALFORMED_SUBMIT, f"malformed submit: {exc}")
        share_key = (client.username, submission.header_hex)
        with self.lock:
            if share_key in self.recent_share_keys:
                self.reject_stratum(22, PRISM_REJECTION_DUPLICATE_SHARE, "duplicate share")
            if len(self.recent_share_keys) > 50_000:
                self.recent_share_keys.clear()
            self.recent_share_keys.add(share_key)
        if not submission.share_pass:
            self.reject_stratum(23, PRISM_REJECTION_LOW_DIFFICULTY, "low difficulty share")

        pending_share = self.pending_share_from_submission(
            client=client,
            context=context,
            submission=submission,
            ntime_hex=ntime_hex,
        )
        if context.collection_only or not submission.block_pass:
            self.append_accepted_share(client, context, submission, pending_share)
            return False
        return self.submit_block_candidate(
            context,
            submission,
            client.extranonce1_hex,
            extranonce2_hex,
            pending_share=pending_share,
            client=client,
        )

    def pending_share_from_submission(
        self,
        *,
        client: ClientState,
        context: PrismJobContext,
        submission: direct_stratum.DirectQbitSubmission,
        ntime_hex: str,
    ) -> PendingShare:
        return PendingShare(
            share_id=f"{client.username}:{submission.block_hash_hex}",
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
        )

    def append_accepted_share(
        self,
        client: ClientState,
        context: PrismJobContext,
        submission: direct_stratum.DirectQbitSubmission,
        pending_share: PendingShare,
    ) -> None:
        record = self.ledger.append(pending_share)
        print(
            "prism coordinator: accepted share "
            f"seq={record.share_seq} miner={client.username} job={context.job.job_id} "
            f"hash={submission.block_hash_hex} collection={context.collection_only}",
            flush=True,
        )
        self.note_vardiff_accepted_share(client, context.job)

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
        if not self.vardiff_config.enabled:
            return
        with self.lock:
            client.vardiff_window_submitted += 1

    def note_vardiff_accepted_share(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        if not self.vardiff_config.enabled:
            return
        now = time.monotonic()
        with self.lock:
            client.vardiff_window_accepted += 1
            client.vardiff_window_work += job.share_difficulty
            elapsed_seconds = Decimal(str(max(0.001, now - client.vardiff_window_started_monotonic)))
            if elapsed_seconds < self.vardiff_config.retarget_interval_seconds:
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

    def retarget_client(
        self,
        client: ClientState,
        *,
        current_difficulty: Decimal,
        accepted_shares: int,
        submitted_shares: int,
        accepted_difficulty: Decimal,
        elapsed_seconds: Decimal,
    ) -> None:
        if not self.vardiff_config.enabled:
            return
        observed_difficulty = vardiff.observed_difficulty(
            accepted_difficulty=accepted_difficulty,
            elapsed_seconds=elapsed_seconds,
            target_share_interval_seconds=self.vardiff_config.target_share_interval_seconds,
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
                config=self.vardiff_config,
            )
            with self.lock:
                client.vardiff_difficulty_estimate = difficulty_estimate
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=current_difficulty,
            accepted_shares=accepted_shares,
            elapsed_seconds=elapsed_seconds,
            config=self.vardiff_config,
            accepted_difficulty=accepted_difficulty,
            difficulty_estimate=difficulty_estimate,
        )
        if not vardiff.should_retarget(
            current_difficulty,
            next_difficulty,
            self.vardiff_config.retarget_tolerance,
        ):
            return
        with self.lock:
            previous_difficulty = client.pending_share_difficulty or client.share_difficulty
            if previous_difficulty != current_difficulty:
                return
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
        if (
            client.authorized
            and client.subscribed
            and not self.stop_event.is_set()
            and self.maybe_send_job(client, clean_jobs=True)
        ):
            return
        with self.lock:
            if client.pending_share_difficulty == next_difficulty:
                client.pending_share_difficulty = prior_pending

    def submit_block_candidate(
        self,
        context: PrismJobContext,
        submission: direct_stratum.DirectQbitSubmission,
        extranonce1_hex: str,
        extranonce2_hex: str,
        *,
        pending_share: PendingShare,
        client: ClientState,
    ) -> bool:
        with self._watchdog_paused("qbit_blockpoll", "stratum_accept"), self.lock:
            if self.accepted_block_count >= self.max_blocks:
                self.reject_stratum(21, PRISM_REJECTION_POOL_CLOSED, "pool is no longer accepting shares")
            current_tip = str(self.rpc.call("getbestblockhash"))
            if str(context.template["previousblockhash"]) != current_tip:
                self.reject_stratum(21, PRISM_REJECTION_STALE_JOB, "stale job")
            try:
                reorg_reconciled = self.ensure_reorg_reconciled_for_tip(current_tip)
            except Exception:
                print("prism coordinator: reorg reconciliation failed before block submit", flush=True)
                traceback.print_exc()
                self.reject_stratum(
                    20,
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "reorg reconciliation failed before block submit",
                )
            if not reorg_reconciled:
                self.reject_stratum(21, PRISM_REJECTION_STALE_JOB, "stale job")
            if not self.prior_balances_match_current(context.prior_balances):
                self.reject_stratum(21, PRISM_REJECTION_STALE_JOB, "stale job")
            final_bundle = self.build_audit_bundle(
                shares=context.shares_json,
                found_block=context.found_block,
                prior_balances=context.prior_balances,
                coinbase_script_sig_suffix_hex=self.coinbase_script_sig_suffix_hex(
                    extranonce1_hex,
                    extranonce2_hex,
                ),
                witness_merkle_leaves_hex=direct_stratum.witness_merkle_leaves_hex(
                    getattr(context.job, "transaction_hexes", ())
                ),
                ctv_fee_parent_hash=str(context.template["previousblockhash"]),
            )
            final_manifest = final_bundle["signed_coinbase_manifest"]["manifest"]
            if final_manifest["coinbase_tx_hex"].lower() != submission.coinbase_tx_hex.lower():
                self.reject_stratum(
                    20,
                    PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
                    "final audit bundle coinbase does not match submitted coinbase",
                )
            bundle_path = self.audit_dir / f"prism-live-audit-bundle-candidate-{submission.block_hash_hex}.json"
            bundle_path.write_text(json.dumps(final_bundle, indent=2), encoding="utf-8")
            report = self.verify_bundle(
                bundle_path,
                submission.coinbase_tx_hex,
                self.trusted_ledger_writer_public_key_hex(final_bundle),
                expected_coinbase_value_sats=int(context.template["coinbasevalue"]),
            )
            current_tip = str(self.rpc.call("getbestblockhash"))
            if str(context.template["previousblockhash"]) != current_tip:
                self.reject_stratum(21, PRISM_REJECTION_STALE_JOB, "stale job")
            before_height = int(self.rpc.call("getblockcount"))
            expected_height = int(context.template["height"])
            if before_height + 1 != expected_height:
                self.reject_stratum(
                    21,
                    PRISM_REJECTION_BLOCK_STALE,
                    f"stale block height: template={expected_height} tip={before_height}",
                )
            persistence = self.ledger.persist_accepted_block(
                block_hash=submission.block_hash_hex,
                block_height=expected_height,
                parent_hash=str(context.template["previousblockhash"]),
                final_bundle=final_bundle,
                audit_report=report,
            )
            result = self.rpc.call("submitblock", [submission.block_hex])
            after_height = int(self.rpc.call("getblockcount"))
            if result not in (None, "duplicate"):
                self.reject_prepared_block(
                    block_hash=submission.block_hash_hex,
                    active_tip_height=after_height,
                )
                self.reject_stratum(20, PRISM_REJECTION_SUBMITBLOCK_REJECTED, f"submitblock rejected candidate: {result}")
            if after_height != before_height + 1:
                self.reject_prepared_block(
                    block_hash=submission.block_hash_hex,
                    active_tip_height=after_height,
                )
                self.reject_stratum(
                    20,
                    PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                    f"submitblock did not advance height: {before_height}->{after_height}",
                )
            block_hash = str(self.rpc.call("getblockhash", [after_height]))
            if block_hash.lower() != submission.block_hash_hex.lower():
                self.stop_event.set()
                self.reject_prepared_block(
                    block_hash=submission.block_hash_hex,
                    active_tip_height=after_height,
                )
                self.reject_stratum(
                    20,
                    PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                    f"submitted block hash mismatch: expected {submission.block_hash_hex} got {block_hash}",
                )
            confirmation = self.ledger.confirm_accepted_block(
                block_hash=block_hash,
                active_tip_height=after_height,
            )
            if int(confirmation.get("confirmed_count", 0)) != 1:
                self.stop_event.set()
                self.reject_stratum(
                    20,
                    PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                    f"ledger did not confirm accepted block {block_hash}",
                )
            ctv_persistence = None
            ctv_manifest_set = final_bundle.get("ctv_fanout_manifest_set")
            if isinstance(ctv_manifest_set, dict):
                ctv_persistence = self.ledger.persist_ctv_fanout_manifest_set(
                    block_hash=block_hash,
                    manifest_set=ctv_manifest_set,
                    manifest_set_sha256=sha256_json_hex(ctv_manifest_set),
                )
            final_bundle_path = self.audit_dir / f"prism-live-audit-bundle-{after_height}-{block_hash}.json"
            bundle_path.replace(final_bundle_path)
            bundle_path = final_bundle_path
            self.append_accepted_share(client, context, submission, pending_share)
            evidence = {
                "schema": "qbit.prism.live-stratum-evidence.v1",
                "block_hash": block_hash,
                "block_height": after_height,
                "coinbase_tx_hex": submission.coinbase_tx_hex,
                "audit_bundle_path": str(bundle_path),
                "audit_report": report,
                "ledger_backend": self.ledger.backend_name,
                "persistence": persistence,
                "confirmation": confirmation,
                "ctv_persistence": ctv_persistence,
                "accepted_share_count": len(self.ledger.all_shares()),
                "distinct_miners": sorted({share.miner_id for share in self.ledger.all_shares()}),
                "job_share_count": len(context.shares_json),
            }
            self.evidence_path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
            self.accepted_block_count += 1
            self.latest_bundle = final_bundle
            self.latest_evidence = evidence
            print(
                "prism coordinator: qbit accepted direct PRISM block "
                f"height={after_height} hash={block_hash}",
                flush=True,
            )
            should_stop = self.stop_after_block or self.accepted_block_count >= self.max_blocks
            if not should_stop:
                client.post_accept_refresh_block = (after_height, block_hash)
            if should_stop:
                self.stop_event.set()
            return False

    def reject_prepared_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        reject = getattr(self.ledger, "reject_prepared_block", None)
        if callable(reject):
            return reject(block_hash=block_hash, active_tip_height=active_tip_height)
        return self.ledger.reverse_immature_block(
            block_hash=block_hash,
            active_tip_height=active_tip_height,
        )

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
        accepted_share_count = self.accepted_share_stats()[0]
        elapsed = max(0.001, time.monotonic() - self.started_monotonic)
        shares_per_second = accepted_share_count / elapsed
        stale_percent = 0.0
        if self.submitted_share_count > 0:
            stale_percent = (self.stale_share_count / self.submitted_share_count) * 100.0
        rejection_counts = getattr(self, "rejection_counts_by_reason", {})
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
            "# HELP qbit_prism_stale_shares_total Stratum shares rejected or ignored as stale.",
            "# TYPE qbit_prism_stale_shares_total counter",
            f"qbit_prism_stale_shares_total {self.stale_share_count}",
            "# HELP qbit_prism_duplicate_shares_total Duplicate Stratum shares rejected.",
            "# TYPE qbit_prism_duplicate_shares_total counter",
            f"qbit_prism_duplicate_shares_total {self.duplicate_share_count}",
            "# HELP qbit_prism_low_difficulty_shares_total Low-difficulty Stratum shares rejected.",
            "# TYPE qbit_prism_low_difficulty_shares_total counter",
            f"qbit_prism_low_difficulty_shares_total {self.low_difficulty_share_count}",
            "# HELP qbit_prism_rejections_total PRISM share or block rejections by canonical reason ID.",
            "# TYPE qbit_prism_rejections_total counter",
            *[
                f'qbit_prism_rejections_total{{reason_id="{reason}"}} {int(rejection_counts.get(reason, 0))}'
                for reason in PRISM_REJECTION_REASON_IDS
            ],
            "# HELP qbit_prism_job_build_failures_total Job builds skipped after a template/coinbase error without dropping the client.",
            "# TYPE qbit_prism_job_build_failures_total counter",
            f"qbit_prism_job_build_failures_total {self.job_build_failure_count}",
            "# HELP qbit_prism_tip_refresh_jobs_total Client jobs refreshed after qbit tip/template changes.",
            "# TYPE qbit_prism_tip_refresh_jobs_total counter",
            f"qbit_prism_tip_refresh_jobs_total {self.tip_refresh_job_count}",
            "# HELP qbit_prism_active_job_contexts Current retained PRISM job contexts.",
            "# TYPE qbit_prism_active_job_contexts gauge",
            f"qbit_prism_active_job_contexts {len(getattr(self, 'jobs', {}))}",
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
        ]
        lines.extend(self.job_build_metrics_lines())
        return "\n".join(lines) + "\n"

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
        coordinator.release_ledger_lease()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
