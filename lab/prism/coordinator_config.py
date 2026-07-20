"""Immutable PRISM coordinator configuration and environment loading."""

from __future__ import annotations

import json
import math
import shlex
from os import environ as _PROCESS_ENVIRON
from dataclasses import dataclass, replace as dataclass_replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism.ctv_broadcaster_daemon import MAX_CTV_FANOUT_BROADCASTER_CHUNK_SIZE
from lab.prism.share_ledger import (
    DEFAULT_AUDIT_SHARE_SEGMENT_SIZE,
    DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
    DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
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
DEFAULT_PRISM_TIP_REFRESH_FAILURE_HOLDOFF_SECONDS = 1.0
DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS = 10.0
DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS = 60.0
DEFAULT_PRISM_CTV_BROADCASTER_CHUNK_SIZE = 5
DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS = 5.0
DEFAULT_PRISM_HEALTH_REFRESH_SECONDS = 5.0
DEFAULT_PRISM_METRICS_REFRESH_SECONDS = 5.0
DEFAULT_PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS = 15.0
DEFAULT_PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS = 15.0
DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS = 20.0
DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS = 384
DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME = 0
DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS = 128
DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS = 30.0
DEFAULT_PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS = 30.0
DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS = 1.0
DEFAULT_PRISM_STRATUM_LISTEN_BACKLOG = 1024
DEFAULT_PRISM_STRATUM_BIND_RETRY_SECONDS = 10.0
DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES = 4_096
DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS = 3_600.0
DEFAULT_PRISM_STALE_GRACE_SECONDS = 3.0
DEFAULT_PRISM_SUBMIT_TIP_MAX_AGE_SECONDS = 10.0
DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS = 30.0
DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION = 64
DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS = 16
DEFAULT_PRISM_JOB_BUILD_TIMEOUT_SECONDS = 60.0
DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS = 0.25
DEFAULT_PRISM_VARDIFF_IDLE_SWEEP_SECONDS = 15.0
DEFAULT_PRISM_WORKER_METRICS_LIMIT = 100
DEFAULT_SHARE_COMMIT_BATCH_SIZE = 64
DEFAULT_SHARE_COMMIT_LINGER_MILLISECONDS = 5.0
DEFAULT_SHARE_COMMIT_TIMEOUT_SECONDS = 15.0
DEFAULT_PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS = 15.0
DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS = 120
DEFAULT_HIGHDIFF_DIFFICULTY = "500000"
DEFAULT_HIGHDIFF_MAX_DIFFICULTY = "4294967296"


Env = Mapping[str, str]


def _current_environ(environ: Env | None) -> Env:
    return _PROCESS_ENVIRON if environ is None else environ


def env(name: str, default: str | None = None, *, environ: Env | None = None) -> str:
    value = _current_environ(environ).get(name, default)
    if value is None or value == "":
        raise SystemExit(f"{name} is required")
    return value


def env_int(name: str, default: int, *, environ: Env | None = None) -> int:
    return int(env(name, str(default), environ=environ))


def env_positive_int(name: str, default: int, *, environ: Env | None = None) -> int:
    try:
        value = env_int(name, default, environ=environ)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer") from exc
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


def env_positive_int_with_legacy(
    primary_name: str,
    legacy_name: str,
    default: int,
    *,
    environ: Env | None = None,
) -> int:
    if env_optional(primary_name, environ=environ) is not None:
        return env_positive_int(primary_name, default, environ=environ)
    return env_positive_int(legacy_name, default, environ=environ)


def env_nonnegative_int(name: str, default: int, *, environ: Env | None = None) -> int:
    try:
        value = env_int(name, default, environ=environ)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer") from exc
    if value < 0:
        raise SystemExit(f"{name} must be non-negative")
    return value


def env_nonnegative_int_with_legacy(
    primary_name: str,
    legacy_name: str,
    default: int,
    *,
    environ: Env | None = None,
) -> int:
    if env_optional(primary_name, environ=environ) is not None:
        return env_nonnegative_int(primary_name, default, environ=environ)
    return env_nonnegative_int(legacy_name, default, environ=environ)


def env_positive_float(name: str, default: float, *, environ: Env | None = None) -> float:
    try:
        value = float(env(name, str(default), environ=environ))
    except ValueError as exc:
        raise SystemExit(f"{name} must be a number") from exc
    if not math.isfinite(value):
        raise SystemExit(f"{name} must be finite")
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


def env_nonnegative_float(name: str, default: float, *, environ: Env | None = None) -> float:
    try:
        value = float(env(name, str(default), environ=environ))
    except ValueError as exc:
        raise SystemExit(f"{name} must be a number") from exc
    if not math.isfinite(value):
        raise SystemExit(f"{name} must be finite")
    if value < 0:
        raise SystemExit(f"{name} must be non-negative")
    return value


def env_optional_positive_int(name: str, *, environ: Env | None = None) -> int | None:
    raw = env_optional(name, environ=environ)
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer") from exc
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


def env_optional_positive_int_with_legacy(
    primary_name: str,
    legacy_name: str,
    *,
    environ: Env | None = None,
) -> int | None:
    value = env_optional_positive_int(primary_name, environ=environ)
    if value is not None:
        return value
    return env_optional_positive_int(legacy_name, environ=environ)


def env_decimal(name: str, default: str, *, environ: Env | None = None) -> Decimal:
    try:
        value = Decimal(env(name, default, environ=environ))
    except InvalidOperation as exc:
        raise SystemExit(f"{name} must be a decimal number") from exc
    if not value.is_finite():
        raise SystemExit(f"{name} must be finite")
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


def env_bool(name: str, default: str, *, environ: Env | None = None) -> bool:
    return env(name, default, environ=environ).lower() in {"1", "true", "yes", "on"}


def env_optional_bool(name: str, *, environ: Env | None = None) -> bool | None:
    raw = env_optional(name, environ=environ)
    if raw is None:
        return None
    return raw.lower() in {"1", "true", "yes", "on"}


def env_optional(name: str, *, environ: Env | None = None) -> str | None:
    value = _current_environ(environ).get(name)
    if value is None or value == "":
        return None
    return value


def production_mode(*, environ: Env | None = None) -> bool:
    return (
        env_bool("QBIT_PRODUCTION", "0", environ=environ)
        or env_bool("QBIT_TOOLS_PRODUCTION", "0", environ=environ)
        or env("QBIT_CHAIN", "regtest", environ=environ).lower() in {"main", "mainnet"}
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


def require_production_env(name: str, *, environ: Env | None = None) -> str:
    value = env_optional(name, environ=environ)
    if value is None:
        raise SystemExit(f"production mode requires {name}")
    return value


def validate_prism_production_gate(*, environ: Env | None = None) -> None:
    if not production_mode(environ=environ):
        return

    for name in (
        "PRISM_ALLOW_MEMORY_LEDGER",
        "PRISM_ALLOW_TEST_SIGNING_SEEDS",
        "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY",
        "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN",
    ):
        if env_bool(name, "0", environ=environ):
            raise SystemExit(f"production mode rejects {name}=1")

    if env("QBIT_CHAIN", "regtest", environ=environ).lower() in {"main", "mainnet"} and env_nonnegative_float(
        "PRISM_STRATUM_STALE_GRACE_SECONDS",
        DEFAULT_PRISM_STALE_GRACE_SECONDS,
        environ=environ,
    ) != 0:
        raise SystemExit("mainnet requires PRISM_STRATUM_STALE_GRACE_SECONDS=0")

    production_difficulties: dict[str, Decimal] = {}
    for name in (
        "PRISM_STRATUM_SHARE_DIFF",
        "PRISM_STRATUM_VARDIFF_MIN_DIFF",
        "PRISM_STRATUM_VARDIFF_START_DIFF",
        "PRISM_STRATUM_VARDIFF_MAX_DIFF",
    ):
        raw_value = require_production_env(name, environ=environ)
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
    if production_difficulties["PRISM_STRATUM_VARDIFF_MIN_DIFF"] > production_difficulties[
        "PRISM_STRATUM_VARDIFF_START_DIFF"
    ]:
        raise SystemExit("production vardiff minimum exceeds its start difficulty")
    if production_difficulties["PRISM_STRATUM_VARDIFF_START_DIFF"] > production_difficulties[
        "PRISM_STRATUM_VARDIFF_MAX_DIFF"
    ]:
        raise SystemExit("production vardiff start exceeds its maximum difficulty")

    prism_database_url = env_optional("PRISM_DATABASE_URL", environ=environ)
    if prism_database_url is None and env_optional("PRISM_POSTGRES_PSQL_COMMAND", environ=environ) is None:
        raise SystemExit("production mode requires PRISM_DATABASE_URL or PRISM_POSTGRES_PSQL_COMMAND")
    if env_optional("PRISM_POSTGRES_PASSWORD", environ=environ) == "change-this":
        raise SystemExit("production mode requires a non-default PRISM_POSTGRES_PASSWORD")
    if prism_database_url is not None and "change-this" in prism_database_url:
        raise SystemExit("production mode requires a non-default PRISM_DATABASE_URL")

    require_production_env("PRISM_MANIFEST_SIGNING_SEED_HEX", environ=environ)
    require_production_env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX", environ=environ)
    require_production_env("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX", environ=environ)
    require_production_env("PRISM_LEDGER_WRITER_ID", environ=environ)
    require_production_env("PRISM_LEDGER_WRITER_EPOCH", environ=environ)
    require_production_env("PRISM_AUDIT_DIR", environ=environ)
    require_production_env("PRISM_EVIDENCE_PATH", environ=environ)

    if env_optional("PRISM_LEDGER_WRITER_SESSION_TOKEN", environ=environ) is not None:
        raise SystemExit(
            "production mode requires managed ledger session tokens; unset "
            "PRISM_LEDGER_WRITER_SESSION_TOKEN"
        )
    if env_nonnegative_int(
        "PRISM_STRATUM_MAX_CONNECTIONS", DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS, environ=environ
    ) <= 0:
        raise SystemExit("production mode requires a positive PRISM_STRATUM_MAX_CONNECTIONS")
    env_positive_int(
        "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS",
        DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS,
        environ=environ,
    )
    if env_nonnegative_float(
        "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS",
        DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS,
        environ=environ,
    ) <= 0:
        raise SystemExit("production mode requires a positive PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS")

    require_production_env("QBIT_RPC_USER", environ=environ)
    qbit_rpc_password = require_production_env("QBIT_RPC_PASSWORD", environ=environ)
    if qbit_rpc_password == "change-this":
        raise SystemExit("production mode requires a non-default QBIT_RPC_PASSWORD")

    if env("QBIT_CHAIN", "regtest", environ=environ).lower() in {"main", "mainnet"} and env_bool(
        "PRISM_CTV_SETTLEMENT_ENABLED", "0", environ=environ
    ):
        require_production_env(
            "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT", environ=environ
        )
        env_positive_int(
            "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT", 0, environ=environ
        )

    validate_same_tip_job_retention_limits(
        retention_seconds=env_nonnegative_float(
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS",
            DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
            environ=environ,
        ),
        per_connection=env_nonnegative_int(
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION",
            DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION,
            environ=environ,
        ),
        max_connections=env_nonnegative_int(
            "PRISM_STRATUM_MAX_CONNECTIONS", DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS, environ=environ
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


def env_seed_hex(name: str, *, test_default: str, environ: Env | None = None) -> str:
    value = env_optional(name, environ=environ)
    if value is None:
        if env_bool("PRISM_ALLOW_TEST_SIGNING_SEEDS", "0", environ=environ):
            value = test_default
        else:
            raise SystemExit(f"{name} is required")
    return validate_hex(value, name=name, expected_bytes=32)


def load_prism_vardiff_config(
    startup_difficulty: Decimal, *, environ: Env | None = None
) -> vardiff.VardiffConfig:
    return vardiff.VardiffConfig(
        enabled=env_bool("PRISM_STRATUM_VARDIFF", "1", environ=environ),
        target_share_interval_seconds=env_decimal(
            "PRISM_STRATUM_VARDIFF_TARGET_SECONDS", "15", environ=environ
        ),
        min_difficulty=env_decimal(
            "PRISM_STRATUM_VARDIFF_MIN_DIFF", str(startup_difficulty), environ=environ
        ),
        max_difficulty=env_decimal("PRISM_STRATUM_VARDIFF_MAX_DIFF", "1024", environ=environ),
        retarget_interval_seconds=env_decimal(
            "PRISM_STRATUM_VARDIFF_RETARGET_SECONDS", "90", environ=environ
        ),
        max_step_factor=env_decimal("PRISM_STRATUM_VARDIFF_MAX_STEP_UP", "4", environ=environ),
        startup_difficulty=env_decimal(
            "PRISM_STRATUM_VARDIFF_START_DIFF", str(startup_difficulty), environ=environ
        ),
        max_step_down_factor=env_decimal(
            "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN", "4", environ=environ
        ),
        ewma_alpha=env_decimal("PRISM_STRATUM_VARDIFF_EWMA_ALPHA", "0.4", environ=environ),
        retarget_tolerance=env_decimal(
            "PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE", "0.25", environ=environ
        ),
    )


@dataclass(frozen=True)
class StratumListenerProfile:
    name: str
    bind: str
    port: int
    share_difficulty: Decimal
    vardiff_config: vardiff.VardiffConfig
    heartbeat_name: str
    minimum_advertised_difficulty: Decimal = Decimal("0")


def load_prism_highdiff_listener(
    base_bind: str,
    base_vardiff_config: vardiff.VardiffConfig,
    *,
    environ: Env | None = None,
) -> StratumListenerProfile | None:
    port_value = env_optional("PRISM_STRATUM_HIGHDIFF_PORT", environ=environ)
    if port_value is None:
        return None
    try:
        port = int(port_value)
    except ValueError as exc:
        raise SystemExit("PRISM_STRATUM_HIGHDIFF_PORT must be an integer") from exc
    if not 0 < port < 65536:
        raise SystemExit("PRISM_STRATUM_HIGHDIFF_PORT must be a valid TCP port")
    min_difficulty = env_decimal(
        "PRISM_STRATUM_HIGHDIFF_MIN_DIFF", DEFAULT_HIGHDIFF_DIFFICULTY, environ=environ
    )
    start_difficulty = env_decimal(
        "PRISM_STRATUM_HIGHDIFF_START_DIFF", DEFAULT_HIGHDIFF_DIFFICULTY, environ=environ
    )
    max_difficulty = env_decimal(
        "PRISM_STRATUM_HIGHDIFF_MAX_DIFF", DEFAULT_HIGHDIFF_MAX_DIFFICULTY, environ=environ
    )
    if min_difficulty > start_difficulty:
        raise SystemExit("PRISM_STRATUM_HIGHDIFF_MIN_DIFF exceeds PRISM_STRATUM_HIGHDIFF_START_DIFF")
    if start_difficulty > max_difficulty:
        raise SystemExit("PRISM_STRATUM_HIGHDIFF_START_DIFF exceeds PRISM_STRATUM_HIGHDIFF_MAX_DIFF")
    share_value = env_optional("PRISM_STRATUM_HIGHDIFF_SHARE_DIFF", environ=environ)
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
            raise SystemExit(
                "PRISM_STRATUM_HIGHDIFF_SHARE_DIFF is below PRISM_STRATUM_HIGHDIFF_MIN_DIFF"
            )
        if share_difficulty > max_difficulty:
            raise SystemExit(
                "PRISM_STRATUM_HIGHDIFF_SHARE_DIFF exceeds PRISM_STRATUM_HIGHDIFF_MAX_DIFF"
            )
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
        bind=env_optional("PRISM_STRATUM_HIGHDIFF_BIND", environ=environ) or base_bind,
        port=port,
        share_difficulty=share_difficulty,
        vardiff_config=config,
        heartbeat_name="stratum_accept_highdiff",
        minimum_advertised_difficulty=min_difficulty,
    )


def default_prism_payout_policy(*, environ: Env | None = None) -> dict[str, object]:
    policy: dict[str, object] = {
        "p2mr_spend_input_bytes": env_positive_int(
            "PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES", DEFAULT_P2MR_SPEND_INPUT_BYTES, environ=environ
        ),
        "target_feerate_sats_per_byte": env_positive_int_with_legacy(
            "PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE",
            "PRISM_PAYOUT_TARGET_FEERATE_SATS_PER_BYTE",
            DEFAULT_MIN_OUTPUT_FEERATE_SATS_PER_BYTE,
            environ=environ,
        ),
        "safety_multiplier": env_positive_int(
            "PRISM_PAYOUT_SAFETY_MULTIPLIER",
            DEFAULT_MIN_OUTPUT_SAFETY_MULTIPLIER,
            environ=environ,
        ),
    }
    min_output_sats = env_optional_positive_int_with_legacy(
        "PRISM_PAYOUT_MIN_OUTPUT_BITS", "PRISM_PAYOUT_MIN_OUTPUT_SATS", environ=environ
    )
    if min_output_sats is not None:
        policy["min_output_sats"] = min_output_sats
    return policy


def default_prism_coinbase_tag_hex(*, environ: Env | None = None) -> str:
    tag = _current_environ(environ).get("PRISM_COINBASE_TAG", DEFAULT_PRISM_COINBASE_TAG)
    try:
        tag_bytes = tag.encode("ascii")
    except UnicodeEncodeError as exc:
        raise SystemExit("PRISM_COINBASE_TAG must be ASCII") from exc
    if len(tag_bytes) > MAX_PRISM_COINBASE_TAG_BYTES:
        raise SystemExit(f"PRISM_COINBASE_TAG must be at most {MAX_PRISM_COINBASE_TAG_BYTES} bytes")
    if any(byte < 0x20 or byte > 0x7E for byte in tag_bytes):
        raise SystemExit("PRISM_COINBASE_TAG must contain printable ASCII only")
    return tag_bytes.hex()


def default_prism_username_fallback_address(*, environ: Env | None = None) -> str | None:
    configured = env_optional("PRISM_USERNAME_FALLBACK_ADDRESS", environ=environ)
    if configured is not None:
        return configured
    if (_current_environ(environ).get("QBIT_CHAIN") or "regtest").lower() in TESTNET_QBIT_CHAINS:
        return DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS
    return None


def _parse_share_weights(raw: str) -> tuple[tuple[str, int], ...]:
    if not raw:
        return ()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"PRISM_STRATUM_SHARE_WEIGHTS_JSON is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise SystemExit("PRISM_STRATUM_SHARE_WEIGHTS_JSON must be an object")
    weights: list[tuple[str, int]] = []
    for username, weight in parsed.items():
        parsed_weight = int(weight)
        if parsed_weight <= 0:
            raise SystemExit(f"share weight for {username} must be positive")
        weights.append((str(username), parsed_weight))
    return tuple(weights)


def load_share_weights(*, environ: Env | None = None) -> dict[str, int]:
    """Load the legacy username-weight mapping for compatibility callers."""

    source = _current_environ(environ)
    return dict(_parse_share_weights(source.get("PRISM_STRATUM_SHARE_WEIGHTS_JSON", "")))


@dataclass(frozen=True)
class RpcConfig:
    host: str
    port: int
    user: str
    password: str
    chain: str
    expected_genesis_hash: str | None
    minimum_peers_raw: str | None


@dataclass(frozen=True)
class StratumConfig:
    bind: str
    port: int
    extranonce2_size: int
    stale_grace_seconds: float
    send_timeout_seconds: float
    max_connections: int
    max_connections_per_username: int
    max_pending_initial_jobs: int
    initial_job_timeout_seconds: float
    accept_resource_exhaustion_backoff_seconds: float
    listen_backlog: int
    bind_retry_seconds: float
    share_difficulty: Decimal
    vardiff_config: vardiff.VardiffConfig
    vardiff_idle_sweep_seconds: float
    listener_profiles: tuple[StratumListenerProfile, ...]
    default_share_weight: int
    share_weights_by_username: tuple[tuple[str, int], ...]
    username_fallback_address: str | None
    fallback_version_mask: int
    same_tip_job_retention_seconds: float
    same_tip_job_retention_per_connection: int
    payout_address_cache_max_entries: int
    payout_address_cache_ttl_seconds: float


@dataclass(frozen=True)
class JobPipelineConfig:
    blockpoll_seconds: float
    blockwait_enabled: bool
    blockwait_timeout_seconds: float
    tip_refresh_failure_holdoff_seconds: float
    submit_tip_max_age_seconds: float
    tip_refresh_max_workers: int
    job_build_timeout_seconds: float
    job_build_cancel_grace_seconds: float
    worker_metrics_limit: int
    reorg_reconciler_enabled: bool
    job_bundle_cache_seconds: float
    bundle_build_timeout_seconds: float
    template_cache_seconds: float
    template_refresh_failure_exit_seconds: float
    reorg_reconcile_cache_seconds: float
    min_ready_miners: int
    payout_environment: tuple[tuple[str, str], ...]
    pool_fee_enabled_raw: str | None
    pool_fee_bps_raw: str | None
    pool_fee_address: str | None
    pool_fee_program_hex: str | None
    pool_fee_recipient_id: str | None
    pool_fee_order_key: str | None
    template_max_age_raw: str | None


@dataclass(frozen=True)
class LedgerConfig:
    psql_command: str
    database_url: str | None
    allow_memory_ledger: bool
    native_client_mode: str
    writer_id: str
    writer_epoch: int
    writer_session_token: str | None
    initialize_schema: bool
    lease_ttl_seconds: float
    read_concurrency: int
    accepted_stats_cache_seconds: float
    reward_window_cache_seconds: float
    signing_seed_hex: str
    attestation_signing_seed_hex: str
    writer_public_key_hex: str | None
    share_commit_batch_size: int
    share_commit_linger_seconds: float
    share_commit_timeout_seconds: float
    share_recovery_path: Path


@dataclass(frozen=True)
class AuditConfig:
    evidence_path: Path
    directory: Path
    share_segment_size: int
    live_bundle_retention: int
    candidate_retention_seconds: int
    bind: str | None
    port: int


@dataclass(frozen=True)
class CtvConfig:
    settlement_enabled_raw: str | None
    broadcaster_enabled: bool
    broadcaster_wallet: str | None
    broadcaster_fee_sats: int
    broadcaster_limit: int
    broadcaster_chunk_size: int
    broadcaster_interval_seconds: float
    broadcast_attempt_detail_limit: int
    broadcast_retry_backoff_seconds: int
    settlement_environment: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class LifecycleConfig:
    health_refresh_seconds: float
    metrics_refresh_seconds: float
    pending_refresh_health_deadline_seconds: float
    coherent_tip_poll_health_deadline_seconds: float
    mining_health_startup_grace_seconds: float
    writer_quiescence_timeout_seconds: float
    watchdog_enabled: bool
    watchdog_timeout_seconds: float
    watchdog_interval_seconds: float


@dataclass(frozen=True)
class CoordinatorConfig:
    rpc: RpcConfig
    stratum: StratumConfig
    jobs: JobPipelineConfig
    ledger: LedgerConfig
    audit: AuditConfig
    ctv: CtvConfig
    lifecycle: LifecycleConfig
    production: bool
    hot_path_log_enabled: bool
    coinbase_tag_hex: str
    stop_after_block: bool
    max_blocks: int


def _selected_environment(source: Env, names: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    return tuple((name, source[name]) for name in names if name in source)


def load_coordinator_config(environ: Env | None = None) -> CoordinatorConfig:
    """Load and validate one immutable configuration snapshot.

    Passing a mapping gives tests and embedding callers a no-global-environment
    construction path. The zero-argument production path snapshots ``os.environ``.
    """

    source: Env = dict(_PROCESS_ENVIRON) if environ is None else dict(environ)
    validate_prism_production_gate(environ=source)
    production = production_mode(environ=source)

    rpc = RpcConfig(
        host=env("QBIT_RPC_HOST", environ=source),
        port=env_int("QBIT_RPC_PORT", 18452, environ=source),
        user=env("QBIT_RPC_USER", environ=source),
        password=env("QBIT_RPC_PASSWORD", environ=source),
        chain=env("QBIT_CHAIN", "regtest", environ=source),
        expected_genesis_hash=env_optional("QBIT_EXPECTED_GENESIS_HASH", environ=source),
        minimum_peers_raw=source.get("PRISM_MIN_PEERS"),
    )

    blockpoll_seconds = env_positive_float(
        "PRISM_BLOCKPOLL_SECONDS", DEFAULT_PRISM_BLOCKPOLL_SECONDS, environ=source
    )
    same_tip_seconds = env_nonnegative_float(
        "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS",
        DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
        environ=source,
    )
    same_tip_per_connection = env_nonnegative_int(
        "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION",
        DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION,
        environ=source,
    )
    max_connections = env_nonnegative_int(
        "PRISM_STRATUM_MAX_CONNECTIONS", DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS, environ=source
    )
    max_pending_initial_jobs = env_positive_int(
        "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS",
        DEFAULT_PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS,
        environ=source,
    )
    if max_connections > 0 and max_pending_initial_jobs > max_connections:
        raise SystemExit(
            "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS cannot exceed PRISM_STRATUM_MAX_CONNECTIONS"
        )
    validate_same_tip_job_retention_limits(
        retention_seconds=same_tip_seconds,
        per_connection=same_tip_per_connection,
        max_connections=max_connections,
        production=production,
    )
    tip_refresh_max_workers = env_positive_int(
        "PRISM_TIP_REFRESH_MAX_WORKERS", DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS, environ=source
    )
    if tip_refresh_max_workers > DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS:
        raise SystemExit(
            "PRISM_TIP_REFRESH_MAX_WORKERS cannot exceed "
            f"{DEFAULT_PRISM_TIP_REFRESH_MAX_WORKERS}"
        )
    template_refresh_failure_exit_seconds = env_nonnegative_float(
        "PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS",
        DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
        environ=source,
    )
    if production and template_refresh_failure_exit_seconds <= 0:
        raise SystemExit(
            "production mode requires a positive PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS"
        )

    bind = env("PRISM_STRATUM_BIND", "127.0.0.1", environ=source)
    port = env_int("PRISM_STRATUM_PORT", 3340, environ=source)
    share_difficulty = env_decimal("PRISM_STRATUM_SHARE_DIFF", "0.000000001", environ=source)
    vardiff_config = load_prism_vardiff_config(share_difficulty, environ=source)
    listener_profiles = [
        StratumListenerProfile(
            name="default",
            bind=bind,
            port=port,
            share_difficulty=share_difficulty,
            vardiff_config=vardiff_config,
            heartbeat_name="stratum_accept",
        )
    ]
    highdiff_profile = load_prism_highdiff_listener(bind, vardiff_config, environ=source)
    if highdiff_profile is not None:
        if highdiff_profile.port == port and highdiff_profile.bind == bind:
            raise SystemExit("PRISM_STRATUM_HIGHDIFF_PORT must differ from PRISM_STRATUM_PORT")
        listener_profiles.append(highdiff_profile)
    default_share_weight = env_int("PRISM_STRATUM_SHARE_WEIGHT", 1, environ=source)
    if default_share_weight <= 0:
        raise SystemExit("PRISM_STRATUM_SHARE_WEIGHT must be positive")
    try:
        fallback_version_mask = direct_stratum.normalize_version_rolling_mask(
            env(
                "PRISM_VERSION_ROLLING_MASK",
                direct_stratum.QBIT_VERSION_ROLLING_MASK_HEX,
                environ=source,
            ),
            field_name="PRISM_VERSION_ROLLING_MASK",
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    stratum = StratumConfig(
        bind=bind,
        port=port,
        extranonce2_size=env_int("PRISM_STRATUM_EXTRANONCE2_SIZE", 8, environ=source),
        stale_grace_seconds=env_nonnegative_float(
            "PRISM_STRATUM_STALE_GRACE_SECONDS", DEFAULT_PRISM_STALE_GRACE_SECONDS, environ=source
        ),
        send_timeout_seconds=env_nonnegative_float(
            "PRISM_STRATUM_SEND_TIMEOUT_SECONDS",
            DEFAULT_PRISM_STRATUM_SEND_TIMEOUT_SECONDS,
            environ=source,
        ),
        max_connections=max_connections,
        max_connections_per_username=env_nonnegative_int(
            "PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME",
            DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME,
            environ=source,
        ),
        max_pending_initial_jobs=max_pending_initial_jobs,
        initial_job_timeout_seconds=env_nonnegative_float(
            "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS",
            DEFAULT_PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS,
            environ=source,
        ),
        accept_resource_exhaustion_backoff_seconds=env_positive_float(
            "PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS",
            DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS,
            environ=source,
        ),
        listen_backlog=env_positive_int(
            "PRISM_STRATUM_LISTEN_BACKLOG", DEFAULT_PRISM_STRATUM_LISTEN_BACKLOG, environ=source
        ),
        bind_retry_seconds=env_nonnegative_float(
            "PRISM_STRATUM_BIND_RETRY_SECONDS",
            DEFAULT_PRISM_STRATUM_BIND_RETRY_SECONDS,
            environ=source,
        ),
        share_difficulty=share_difficulty,
        vardiff_config=vardiff_config,
        vardiff_idle_sweep_seconds=env_nonnegative_float(
            "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS",
            DEFAULT_PRISM_VARDIFF_IDLE_SWEEP_SECONDS,
            environ=source,
        ),
        listener_profiles=tuple(listener_profiles),
        default_share_weight=default_share_weight,
        share_weights_by_username=_parse_share_weights(source.get("PRISM_STRATUM_SHARE_WEIGHTS_JSON", "")),
        username_fallback_address=default_prism_username_fallback_address(environ=source),
        fallback_version_mask=fallback_version_mask,
        same_tip_job_retention_seconds=same_tip_seconds,
        same_tip_job_retention_per_connection=same_tip_per_connection,
        payout_address_cache_max_entries=env_nonnegative_int(
            "PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES",
            DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES,
            environ=source,
        ),
        payout_address_cache_ttl_seconds=env_nonnegative_float(
            "PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS",
            DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS,
            environ=source,
        ),
    )

    jobs = JobPipelineConfig(
        blockpoll_seconds=blockpoll_seconds,
        blockwait_enabled=env_bool("PRISM_BLOCKWAIT_ENABLED", "1", environ=source),
        blockwait_timeout_seconds=env_positive_float(
            "PRISM_BLOCKWAIT_TIMEOUT_SECONDS",
            DEFAULT_PRISM_BLOCKWAIT_TIMEOUT_SECONDS,
            environ=source,
        ),
        tip_refresh_failure_holdoff_seconds=env_nonnegative_float(
            "PRISM_TIP_REFRESH_FAILURE_HOLDOFF_SECONDS",
            DEFAULT_PRISM_TIP_REFRESH_FAILURE_HOLDOFF_SECONDS,
            environ=source,
        ),
        submit_tip_max_age_seconds=env_nonnegative_float(
            "PRISM_SUBMIT_TIP_MAX_AGE_SECONDS",
            DEFAULT_PRISM_SUBMIT_TIP_MAX_AGE_SECONDS,
            environ=source,
        ),
        tip_refresh_max_workers=tip_refresh_max_workers,
        job_build_timeout_seconds=env_positive_float(
            "PRISM_JOB_BUILD_TIMEOUT_SECONDS", DEFAULT_PRISM_JOB_BUILD_TIMEOUT_SECONDS, environ=source
        ),
        job_build_cancel_grace_seconds=env_nonnegative_float(
            "PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS",
            DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS,
            environ=source,
        ),
        worker_metrics_limit=env_nonnegative_int(
            "PRISM_WORKER_METRICS_LIMIT", DEFAULT_PRISM_WORKER_METRICS_LIMIT, environ=source
        ),
        reorg_reconciler_enabled=env_bool("PRISM_REORG_RECONCILER_ENABLED", "1", environ=source),
        job_bundle_cache_seconds=env_nonnegative_float(
            "PRISM_JOB_BUNDLE_CACHE_SECONDS",
            DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS,
            environ=source,
        ),
        bundle_build_timeout_seconds=env_positive_float(
            "PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS",
            DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
            environ=source,
        ),
        template_cache_seconds=env_nonnegative_float(
            "PRISM_TEMPLATE_CACHE_SECONDS", blockpoll_seconds, environ=source
        ),
        template_refresh_failure_exit_seconds=template_refresh_failure_exit_seconds,
        reorg_reconcile_cache_seconds=env_nonnegative_float(
            "PRISM_REORG_RECONCILE_CACHE_SECONDS",
            DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
            environ=source,
        ),
        min_ready_miners=env_int("PRISM_MIN_READY_MINERS", 3, environ=source),
        payout_environment=_selected_environment(
            source,
            (
                "PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES",
                "PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE",
                "PRISM_PAYOUT_TARGET_FEERATE_SATS_PER_BYTE",
                "PRISM_PAYOUT_SAFETY_MULTIPLIER",
                "PRISM_PAYOUT_MIN_OUTPUT_BITS",
                "PRISM_PAYOUT_MIN_OUTPUT_SATS",
            ),
        ),
        pool_fee_enabled_raw=source.get("PRISM_POOL_FEE_ENABLED"),
        pool_fee_bps_raw=env_optional("PRISM_POOL_FEE_BPS", environ=source),
        pool_fee_address=env_optional("PRISM_POOL_FEE_ADDRESS", environ=source),
        pool_fee_program_hex=env_optional("PRISM_POOL_FEE_P2MR_PROGRAM_HEX", environ=source),
        pool_fee_recipient_id=env_optional("PRISM_POOL_FEE_RECIPIENT_ID", environ=source),
        pool_fee_order_key=env_optional("PRISM_POOL_FEE_ORDER_KEY", environ=source),
        template_max_age_raw=source.get("PRISM_TEMPLATE_MAX_AGE_SECONDS"),
    )

    evidence_path = Path(env("PRISM_EVIDENCE_PATH", "prism-live-evidence.json", environ=source))
    audit_dir = Path(env("PRISM_AUDIT_DIR", str(evidence_path.parent), environ=source))
    audit = AuditConfig(
        evidence_path=evidence_path,
        directory=audit_dir,
        share_segment_size=env_nonnegative_int(
            "PRISM_AUDIT_SHARE_SEGMENT_SIZE", DEFAULT_AUDIT_SHARE_SEGMENT_SIZE, environ=source
        ),
        live_bundle_retention=env_nonnegative_int(
            "PRISM_AUDIT_LIVE_BUNDLE_RETENTION", 5, environ=source
        ),
        candidate_retention_seconds=env_nonnegative_int(
            "PRISM_AUDIT_CANDIDATE_RETENTION_SECONDS", 24 * 60 * 60, environ=source
        ),
        bind=source.get("PRISM_AUDIT_BIND"),
        port=int(source.get("PRISM_AUDIT_PORT", "0") or "0"),
    )

    configured_writer_public_key = env_optional(
        "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX", environ=source
    )
    if configured_writer_public_key is not None:
        writer_public_key = validate_hex(
            configured_writer_public_key,
            name="PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX",
            expected_bytes=32,
        )
    elif env_bool("PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY", "0", environ=source):
        writer_public_key = None
    else:
        raise SystemExit(
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX is required; "
            "set PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY=1 only for local tests"
        )
    psql_command = source.get("PRISM_POSTGRES_PSQL_COMMAND", "")
    database_url = source.get("PRISM_DATABASE_URL", "")
    if not psql_command and database_url:
        psql_command = f"psql {shlex.quote(database_url)}"
    writer_session_token = env_optional("PRISM_LEDGER_WRITER_SESSION_TOKEN", environ=source)
    if writer_session_token is not None and not env_bool(
        "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN", "0", environ=source
    ):
        raise SystemExit(
            "PRISM_LEDGER_WRITER_SESSION_TOKEN requires "
            "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN=1 for local tests"
        )
    ledger = LedgerConfig(
        psql_command=psql_command,
        database_url=database_url or None,
        allow_memory_ledger=env_bool("PRISM_ALLOW_MEMORY_LEDGER", "0", environ=source),
        native_client_mode=env("PRISM_POSTGRES_NATIVE_CLIENT", "auto", environ=source),
        writer_id=env("PRISM_LEDGER_WRITER_ID", "prism-coordinator", environ=source),
        writer_epoch=env_int("PRISM_LEDGER_WRITER_EPOCH", 1, environ=source),
        writer_session_token=writer_session_token,
        initialize_schema=env("PRISM_POSTGRES_INIT_SCHEMA", "0", environ=source)
        in {"1", "true", "yes"},
        lease_ttl_seconds=env_positive_float(
            "PRISM_LEDGER_LEASE_TTL_SECONDS", 60.0, environ=source
        ),
        read_concurrency=env_positive_int("PRISM_POSTGRES_READ_CONCURRENCY", 4, environ=source),
        accepted_stats_cache_seconds=env_nonnegative_float(
            "PRISM_ACCEPTED_STATS_CACHE_SECONDS", 60.0, environ=source
        ),
        reward_window_cache_seconds=env_nonnegative_float(
            "PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS", 30.0, environ=source
        ),
        signing_seed_hex=env_seed_hex(
            "PRISM_MANIFEST_SIGNING_SEED_HEX", test_default="42" * 32, environ=source
        ),
        attestation_signing_seed_hex=env_seed_hex(
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX",
            test_default="43" * 32,
            environ=source,
        ),
        writer_public_key_hex=writer_public_key,
        share_commit_batch_size=env_positive_int(
            "PRISM_SHARE_COMMIT_BATCH_SIZE", DEFAULT_SHARE_COMMIT_BATCH_SIZE, environ=source
        ),
        share_commit_linger_seconds=env_nonnegative_float(
            "PRISM_SHARE_COMMIT_LINGER_MILLISECONDS",
            DEFAULT_SHARE_COMMIT_LINGER_MILLISECONDS,
            environ=source,
        )
        / 1000.0,
        share_commit_timeout_seconds=env_positive_float(
            "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS", DEFAULT_SHARE_COMMIT_TIMEOUT_SECONDS, environ=source
        ),
        share_recovery_path=Path(
            env(
                "PRISM_SHARE_RECOVERY_PATH",
                str(audit_dir / "prism-unpersisted-shares.jsonl"),
                environ=source,
            )
        ),
    )

    configured_broadcaster_enabled = env_optional_bool(
        "PRISM_CTV_BROADCASTER_ENABLED", environ=source
    )
    broadcaster_enabled = (
        configured_broadcaster_enabled
        if configured_broadcaster_enabled is not None
        else env_bool("PRISM_CTV_SETTLEMENT_ENABLED", "0", environ=source)
    )
    broadcaster_wallet = env_optional("PRISM_CTV_BROADCASTER_WALLET", environ=source)
    broadcaster_fee_sats = env_nonnegative_int_with_legacy(
        "PRISM_CTV_BROADCASTER_FEE_BITS",
        "PRISM_CTV_BROADCASTER_FEE_SATS",
        0,
        environ=source,
    )
    if broadcaster_enabled and broadcaster_fee_sats > 0 and not broadcaster_wallet:
        raise SystemExit(
            "PRISM_CTV_BROADCASTER_WALLET is required when "
            "PRISM_CTV_BROADCASTER_FEE_BITS is positive"
        )
    broadcaster_chunk_size = env_positive_int(
        "PRISM_CTV_BROADCASTER_CHUNK_SIZE",
        DEFAULT_PRISM_CTV_BROADCASTER_CHUNK_SIZE,
        environ=source,
    )
    if broadcaster_chunk_size > MAX_CTV_FANOUT_BROADCASTER_CHUNK_SIZE:
        raise SystemExit(
            "PRISM_CTV_BROADCASTER_CHUNK_SIZE must be at most "
            f"{MAX_CTV_FANOUT_BROADCASTER_CHUNK_SIZE}"
        )
    ctv = CtvConfig(
        settlement_enabled_raw=source.get("PRISM_CTV_SETTLEMENT_ENABLED"),
        broadcaster_enabled=broadcaster_enabled,
        broadcaster_wallet=broadcaster_wallet,
        broadcaster_fee_sats=broadcaster_fee_sats,
        broadcaster_limit=env_positive_int("PRISM_CTV_BROADCASTER_LIMIT", 100, environ=source),
        broadcaster_chunk_size=broadcaster_chunk_size,
        broadcaster_interval_seconds=env_positive_float(
            "PRISM_CTV_BROADCASTER_INTERVAL_SECONDS", 30.0, environ=source
        ),
        broadcast_attempt_detail_limit=env_nonnegative_int(
            "PRISM_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT",
            DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
            environ=source,
        ),
        broadcast_retry_backoff_seconds=env_nonnegative_int(
            "PRISM_CTV_BROADCAST_RETRY_BACKOFF_SECONDS",
            DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
            environ=source,
        ),
        settlement_environment=_selected_environment(
            source,
            (
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_SATS",
                "PRISM_RESERVED_COINBASE_OUTPUTS",
                "PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS",
                "PRISM_MAX_DIRECT_COINBASE_OUTPUTS",
                "PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_SATS_PER_1000_WEIGHT",
                "PRISM_CTV_FANOUT_FEE_ESTIMATE_TARGET_BLOCKS",
                "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS",
            ),
        ),
    )

    lifecycle = LifecycleConfig(
        health_refresh_seconds=env_positive_float(
            "PRISM_HEALTH_REFRESH_SECONDS", DEFAULT_PRISM_HEALTH_REFRESH_SECONDS, environ=source
        ),
        metrics_refresh_seconds=env_positive_float(
            "PRISM_METRICS_REFRESH_SECONDS",
            DEFAULT_PRISM_METRICS_REFRESH_SECONDS,
            environ=source,
        ),
        pending_refresh_health_deadline_seconds=env_positive_float(
            "PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS",
            DEFAULT_PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS,
            environ=source,
        ),
        coherent_tip_poll_health_deadline_seconds=env_positive_float(
            "PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS",
            DEFAULT_PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS,
            environ=source,
        ),
        mining_health_startup_grace_seconds=env_nonnegative_float(
            "PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS",
            DEFAULT_PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS,
            environ=source,
        ),
        writer_quiescence_timeout_seconds=env_positive_float(
            "PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS",
            DEFAULT_PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS,
            environ=source,
        ),
        watchdog_enabled=env_bool("PRISM_WATCHDOG_ENABLED", "1", environ=source),
        watchdog_timeout_seconds=env_positive_float(
            "PRISM_WATCHDOG_TIMEOUT_SECONDS", 120.0, environ=source
        ),
        watchdog_interval_seconds=env_positive_float(
            "PRISM_WATCHDOG_INTERVAL_SECONDS", 15.0, environ=source
        ),
    )
    max_blocks = env_int("PRISM_MAX_BLOCKS", 1, environ=source)
    if max_blocks <= 0:
        raise SystemExit("PRISM_MAX_BLOCKS must be positive")
    return CoordinatorConfig(
        rpc=rpc,
        stratum=stratum,
        jobs=jobs,
        ledger=ledger,
        audit=audit,
        ctv=ctv,
        lifecycle=lifecycle,
        production=production,
        hot_path_log_enabled=env_bool("PRISM_HOT_PATH_LOG", "0", environ=source),
        coinbase_tag_hex=default_prism_coinbase_tag_hex(environ=source),
        stop_after_block=env("PRISM_STOP_AFTER_BLOCK", "1", environ=source)
        in {"1", "true", "yes"},
        max_blocks=max_blocks,
    )
